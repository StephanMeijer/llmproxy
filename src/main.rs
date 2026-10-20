use std::{env, error::Error, path::PathBuf, process::ExitCode, sync::Arc};

use llmproxy::{
    config::Config,
    proxy::{Proxy, metrics_router, router},
};
use tokio::{net::TcpListener, signal};

const CONFIG_ENV: &str = "LLMPROXY_CONFIG";
const DEFAULT_CONFIG: &str = "/etc/llmproxy/config.toml";
const CHECK_FLAG: &str = "--check-config";

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

async fn run() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().skip(1).collect();
    let check_only = args.iter().any(|a| a == CHECK_FLAG);
    let path = args
        .iter()
        .find(|a| !a.starts_with("--"))
        .cloned()
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
