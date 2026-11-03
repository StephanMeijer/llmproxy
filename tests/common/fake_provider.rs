use axum::{
    body::{Body, Bytes, to_bytes},
    extract::Request,
    http::{HeaderMap, StatusCode},
    response::Response,
};
use std::sync::Arc;
use tokio::{
    net::TcpListener,
    sync::{Notify, mpsc, oneshot},
    task::JoinSet,
};

pub struct Hit {
    pub path: String,
    pub headers: HeaderMap,
    pub body: Bytes,
    pub reply: oneshot::Sender<Response>,
}

pub struct Provider {
    pub url: String,
    pub hits: mpsc::Receiver<Hit>,
    tasks: JoinSet<()>,
}

impl Provider {
    /// With `static_ok`, paths other than `/v1/messages` answer "static-ok" without a hit.
    pub async fn new(static_ok: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (send, hits) = mpsc::channel(32);
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            let router = axum::Router::new().fallback(move |request: Request| {
                let send = send.clone();
                async move {
                    let (parts, body) = request.into_parts();
                    let (reply, response) = oneshot::channel();
                    let hit = Hit {
                        path: parts.uri.to_string(),
                        headers: parts.headers,
                        body: to_bytes(body, 1024 * 1024).await.unwrap(),
                        reply,
                    };
                    if static_ok && hit.path.split('?').next() != Some("/v1/messages") {
                        return Response::new(Body::from("static-ok"));
                    }
                    if send.send(hit).await.is_err() {
                        return Response::new(Body::empty());
                    }
                    response
                        .await
                        .unwrap_or_else(|_| Response::new(Body::empty()))
                }
            });
            axum::serve(listener, router).await.unwrap();
        });
        Self { url, hits, tasks }
    }

    pub async fn hit(&mut self) -> Hit {
        tokio::time::timeout(super::WAIT, self.hits.recv())
            .await
            .expect("provider hit deadline")
            .expect("provider stopped")
    }

    pub async fn stop(&mut self) {
        self.tasks.shutdown().await;
    }
}

/// Answers with `request-id` (asserted by the Claude suite) and `retry-after: 0` (so a 429 retries at once).
pub fn reply(hit: Hit, status: StatusCode, body: &'static str) {
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    let headers = response.headers_mut();
    headers.insert("request-id", "provider-request".parse().unwrap());
    headers.insert("retry-after", "0".parse().unwrap());
    assert!(hit.reply.send(response).is_ok());
}

struct Dropped(Arc<Notify>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

/// Streams the chunks sent on the returned channel; the `Notify` fires once the body is dropped.
pub fn stream(hit: Hit) -> (mpsc::Sender<Bytes>, Arc<Notify>) {
    let (send, recv) = mpsc::channel(1);
    let dropped = Arc::new(Notify::new());
    let guard = Dropped(Arc::clone(&dropped));
    let body = futures_util::stream::unfold((recv, guard), |(mut recv, guard)| async move {
        recv.recv()
            .await
            .map(|bytes| (Ok::<_, std::io::Error>(bytes), (recv, guard)))
    });
    let mut response = Response::new(Body::from_stream(body));
    response
        .headers_mut()
        .insert("content-type", "text/event-stream".parse().unwrap());
    assert!(hit.reply.send(response).is_ok());
    (send, dropped)
}
