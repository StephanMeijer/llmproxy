use llmproxy::{
    config::Config,
    proxy::{Proxy, metrics_router, router},
};
use std::sync::{Arc, Mutex};
use tokio::runtime::Runtime;

/// A `Proxy` and its metrics served on their own runtime. Dropping it stops every task the proxy
/// spawned, as a process exit would; on the test's runtime its background tasks would outlive it
/// and stall mid-handshake while the blocking database drop holds that runtime.
pub struct InProcess {
    pub base: String,
    pub metrics: String,
    log: Arc<Mutex<Vec<u8>>>,
    runtime: Option<Runtime>,
}

impl InProcess {
    pub fn start(text: &str) -> Self {
        // The binary's JSON log at its default INFO level, captured from every runtime thread.
        let log = Arc::new(Mutex::new(Vec::new()));
        let dispatch = tracing::Dispatch::new(
            tracing_subscriber::fmt()
                .json()
                .with_max_level(tracing::Level::INFO)
                .with_writer({
                    let log = Arc::clone(&log);
                    move || Captured(Arc::clone(&log))
                })
                .finish(),
        );
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .on_thread_start(move || {
                // Kept for the thread's life; the thread-local default ends with the thread.
                std::mem::forget(tracing::dispatcher::set_default(&dispatch));
            })
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
            log,
            runtime: Some(runtime),
        }
    }

    /// Everything the proxy logged so far, as the binary would have written it.
    pub fn log(&self) -> Vec<u8> {
        self.log.lock().unwrap().clone()
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

/// Appends to the replica's shared log buffer.
struct Captured(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Captured {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().write(bytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
