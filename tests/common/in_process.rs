use llmproxy::{
    config::Config,
    proxy::{Proxy, metrics_router, router},
};
use std::sync::Arc;
use tokio::runtime::Runtime;

/// A `Proxy` and its metrics served on their own runtime. Dropping it stops every task the proxy
/// spawned, as a process exit would; on the test's runtime its background tasks would outlive it
/// and stall mid-handshake while the blocking database drop holds that runtime.
pub struct InProcess {
    pub base: String,
    pub metrics: String,
    runtime: Option<Runtime>,
}

impl InProcess {
    pub fn start(text: &str) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap();
        // The pool spawns its maintenance task at construction, so build the proxy inside.
        let proxy = {
            let _context = runtime.enter();
            Arc::new(Proxy::new(Config::parse(text, |_| None).unwrap()).unwrap())
        };
        let base = serve(&runtime, router(Arc::clone(&proxy)));
        let metrics = format!("{}/metrics", serve(&runtime, metrics_router(proxy)));
        Self {
            base,
            metrics,
            runtime: Some(runtime),
        }
    }
}

impl Drop for InProcess {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}

fn serve(runtime: &Runtime, app: axum::Router) -> String {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let addr = listener.local_addr().unwrap();
    runtime.spawn(async move {
        let listener = tokio::net::TcpListener::from_std(listener).unwrap();
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}
