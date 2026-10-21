use std::{env, error::Error, path::PathBuf, process::ExitCode, sync::Arc};

use llmproxy::{
    config::Config,
    proxy::{Proxy, metrics_router, router},
    storage,
};
use tokio::{net::TcpListener, signal};

const CONFIG_ENV: &str = "LLMPROXY_CONFIG";
const DEFAULT_CONFIG: &str = "/etc/llmproxy/config.toml";
const CHECK_FLAG: &str = "--check-config";

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
    if args.first().is_some_and(|arg| arg == "credentials") {
        return reencrypt(&args).await;
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
    let proxy = Arc::new(Proxy::new(config)?);
    if check_only {
        println!("{path}: ok");
        return Ok(());
    }

    tracing_subscriber::fmt().json().init();
    let listener = TcpListener::bind(listen)
        .await
        .map_err(|e| format!("bind {listen}: {e}"))?;
    tracing::info!(%listen, "listening");
    let clients = axum::serve(listener, router(proxy.clone())).with_graceful_shutdown(shutdown());
    match metrics_listen {
        Some(addr) => {
            let listener = TcpListener::bind(addr)
                .await
                .map_err(|e| format!("bind {addr}: {e}"))?;
            tracing::info!(listen = %addr, "serving metrics");
            let metrics =
                axum::serve(listener, metrics_router(proxy)).with_graceful_shutdown(shutdown());
            tokio::try_join!(clients.into_future(), metrics.into_future())?;
        }
        None => clients.await?,
    }
    Ok(())
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

async fn reencrypt(args: &[String]) -> Result<(), Box<dyn Error + Send + Sync>> {
    let target = match args {
        [_, command, flag, target] if command == "reencrypt" && flag == "--to" => target,
        _ => return Err("usage: llmproxy credentials reencrypt --to <key-id>".into()),
    };
    let path = env::var(CONFIG_ENV).unwrap_or_else(|_| DEFAULT_CONFIG.to_owned());
    let config = Config::load(&PathBuf::from(path))?;
    let postgres = config
        .postgres
        .as_ref()
        .ok_or("credential storage unavailable")?;
    let encryption = config
        .credential_encryption
        .as_ref()
        .ok_or("credential encryption unavailable")?;
    let store = storage::Storage::new(postgres)?;
    let result = store.reencrypt_credentials(encryption, target).await;
    store.close().await;
    let counts = result?;
    println!(
        "rewritten={} remaining={}",
        counts.rewritten, counts.remaining
    );
    Ok(())
}

/// Resolves on SIGINT or SIGTERM; in-flight and held requests then finish before exit.
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
