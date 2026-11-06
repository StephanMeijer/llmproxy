use std::{env, error::Error, path::PathBuf, process::ExitCode, sync::Arc};

use llmproxy::{
    config::Config,
    proxy::{Proxy, metrics_router, router},
    storage,
};
use tokio::{net::TcpListener, signal, sync::watch};

const CONFIG_ENV: &str = "LLMPROXY_CONFIG";
const DEFAULT_CONFIG: &str = "/etc/llmproxy/config.toml";
const CHECK_FLAG: &str = "--check-config";
/// Startup address lines get their own target so `RUST_LOG=warn,llmproxy::listen=info` keeps them.
const LISTEN_TARGET: &str = "llmproxy::listen";

mod help;
mod operator;

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), Box<dyn Error + Send + Sync>> {
    let args: Vec<String> = env::args().skip(1).collect();
    if matches!(args.as_slice(), [flag] if flag == "--help" || flag == "-h") {
        print!("{}", help::USAGE);
        return Ok(());
    }
    if args.first().is_some_and(|arg| arg == "auth") {
        return operator::run(&args).await;
    }
    if args.first().is_some_and(|arg| arg == "db") {
        return migrate(&args).await;
    }
    let (check_only, path) = match args.as_slice() {
        [] => (false, None),
        [flag] if flag == CHECK_FLAG => (true, None),
        [path] if !path.starts_with('-') => (false, Some(path.clone())),
        [flag, path] if flag == CHECK_FLAG && !path.starts_with('-') => (true, Some(path.clone())),
        [path, flag] if flag == CHECK_FLAG && !path.starts_with('-') => (true, Some(path.clone())),
        _ => return Err("invalid arguments; use llmproxy --help".into()),
    };
    let path = path
        .or_else(|| env::var(CONFIG_ENV).ok())
        .unwrap_or_else(|| DEFAULT_CONFIG.to_owned());

    let config = Config::load(&PathBuf::from(&path))?;
    let (listen, metrics_listen) = (config.listen, config.metrics_listen);
    let shutdown_timeout = config.shutdown_timeout;
    let proxy = Arc::new(Proxy::new(config)?);
    if check_only {
        println!("{path}: ok");
        return Ok(());
    }

    // RUST_LOG picks the level (e.g. "warn" or "info,llmproxy=debug"); unset, empty or invalid means info.
    let filter = tracing_subscriber::EnvFilter::builder()
        .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
        .try_from_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(filter)
        .init();
    let listener = TcpListener::bind(listen)
        .await
        .map_err(|e| format!("bind {listen}: {e}"))?;
    // The bound address, not the configured one, so port 0 reports the port actually chosen.
    tracing::info!(target: LISTEN_TARGET, listen = %listener.local_addr()?, "listening");
    let metrics_listener = match metrics_listen {
        Some(addr) => {
            let listener = TcpListener::bind(addr)
                .await
                .map_err(|e| format!("bind {addr}: {e}"))?;
            tracing::info!(target: LISTEN_TARGET, listen = %listener.local_addr()?, "serving metrics");
            Some(listener)
        }
        None => None,
    };

    // One signal starts the drain on both listeners and the deadline that ends it.
    let (stop, stopped) = watch::channel(false);
    tokio::spawn(async move {
        shutdown().await;
        stop.send_replace(true);
    });
    let clients = axum::serve(listener, router(proxy.clone()))
        .with_graceful_shutdown(signalled(stopped.clone()));
    let metrics_stopped = stopped.clone();
    let served = async move {
        match metrics_listener {
            Some(listener) => {
                let metrics = axum::serve(listener, metrics_router(proxy))
                    .with_graceful_shutdown(signalled(metrics_stopped));
                tokio::try_join!(clients.into_future(), metrics.into_future()).map(|_| ())
            }
            None => clients.await,
        }
    };
    tokio::select! {
        served = served => served?,
        () = async {
            signalled(stopped).await;
            tokio::time::sleep(shutdown_timeout).await;
        } => {
            // Returning drops the runtime and with it every connection still open.
            tracing::warn!(
                timeout_secs = shutdown_timeout.as_secs(),
                "shutdown deadline passed; dropping remaining connections"
            );
        }
    }
    Ok(())
}

/// Resolves once a shutdown signal arrived, or at once if the signal task is gone.
async fn signalled(mut stopped: watch::Receiver<bool>) {
    // An error means the sender dropped without signalling; stopping is the safe reading.
    let _ = stopped.wait_for(|stop| *stop).await;
}

async fn migrate(args: &[String]) -> Result<(), Box<dyn Error + Send + Sync>> {
    let path = match args {
        [_, command] if command == "migrate" => {
            env::var(CONFIG_ENV).unwrap_or_else(|_| DEFAULT_CONFIG.to_owned())
        }
        [_, command, path] if command == "migrate" && !path.starts_with('-') => path.clone(),
        _ => return Err("usage: llmproxy db migrate [config]".into()),
    };
    let config = Config::load(&PathBuf::from(path))?;
    let postgres = config
        .postgres
        .as_ref()
        .ok_or("db migrate requires [postgres] configuration")?;
    storage::migrate(postgres).await?;
    println!("database migrations: ok");
    Ok(())
}

/// Resolves on SIGINT or SIGTERM; in-flight and held requests then get `shutdown_timeout` to finish.
async fn shutdown() {
    #[cfg(unix)]
    {
        use signal::unix::{SignalKind, signal};
        let Ok(mut term) = signal(SignalKind::terminate()) else {
            return signal::ctrl_c().await.unwrap_or_default();
        };
        tokio::select! {
            _ = signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    signal::ctrl_c().await.unwrap_or_default();
}
