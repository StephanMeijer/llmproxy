use axum::{
    body::{Body, Bytes, to_bytes},
    extract::Request,
    http::StatusCode,
    response::Response,
};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot},
    task::JoinSet,
};

pub struct Hit {
    pub body: Bytes,
    pub key: Vec<u8>,
    pub reply: oneshot::Sender<Response>,
}

pub struct Provider {
    pub url: String,
    pub hits: mpsc::Receiver<Hit>,
    tasks: JoinSet<()>,
}

impl Provider {
    pub async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let (send, hits) = mpsc::channel(32);
        let mut tasks = JoinSet::new();
        tasks.spawn(async move {
            let router = axum::Router::new().fallback(move |request: Request| {
                let send = send.clone();
                async move {
                    if request.uri().path() != "/v1/messages" {
                        return Response::new(Body::from("static-ok"));
                    }
                    let (parts, body) = request.into_parts();
                    let (reply, response) = oneshot::channel();
                    let hit = Hit {
                        body: to_bytes(body, 1024 * 1024).await.unwrap(),
                        key: parts.headers.get("x-api-key").unwrap().as_bytes().to_vec(),
                        reply,
                    };
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

pub fn reply(hit: Hit, status: StatusCode, body: &'static str) {
    let mut response = Response::new(Body::from(body));
    *response.status_mut() = status;
    response
        .headers_mut()
        .insert("retry-after", "0".parse().unwrap());
    assert!(hit.reply.send(response).is_ok());
}

pub fn stream(hit: Hit) -> mpsc::Sender<Bytes> {
    let (send, recv) = mpsc::channel(1);
    let body = futures_util::stream::unfold(recv, |mut recv| async move {
        recv.recv()
            .await
            .map(|bytes| (Ok::<_, std::io::Error>(bytes), recv))
    });
    let mut response = Response::new(Body::from_stream(body));
    response
        .headers_mut()
        .insert("content-type", "text/event-stream".parse().unwrap());
    assert!(hit.reply.send(response).is_ok());
    send
}
