#![allow(clippy::unwrap_used, clippy::expect_used)]
#![allow(
    clippy::indexing_slicing,
    reason = "HeaderMap and Value indexing in assertions; a missing key should fail the test"
)]

use std::{
    convert::Infallible,
    net::SocketAddr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

use axum::{
    Router,
    body::{Body, Bytes, to_bytes},
    extract::{Request, State},
    http::header::CONTENT_TYPE,
    response::{IntoResponse, Response},
};
use futures_util::stream;
use llmproxy::{
    config::Config,
    proxy::{Proxy, metrics_router, router},
};
use serde_json::{Value, json};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::mpsc,
};

const TOKEN: &str = "proxy-token";
const UPSTREAM_KEY: &str = "upstream-key";
const STREAM_PATH: &str = "/v1/stream";

type Chunks = mpsc::Receiver<Result<Bytes, Infallible>>;

#[derive(Default)]
struct Upstream {
    hits: AtomicUsize,
    stream: Mutex<Option<Chunks>>,
}

/// Echoes what it received as JSON; `/v1/stream` instead relays chunks the test feeds in.
async fn upstream_handler(State(up): State<Arc<Upstream>>, req: Request) -> Response {
    up.hits.fetch_add(1, Ordering::SeqCst);
    if req.uri().path() == STREAM_PATH {
        let mut rx = up.stream.lock().unwrap().take().unwrap();
        let chunks = stream::poll_fn(move |cx| rx.poll_recv(cx));
        return Body::from_stream(chunks).into_response();
    }
    let (parts, body) = req.into_parts();
    let headers: serde_json::Map<String, Value> = parts
        .headers
        .iter()
        .map(|(k, v)| (k.to_string(), json!(v.to_str().unwrap())))
        .collect();
    let body = to_bytes(body, usize::MAX).await.unwrap();
    let echo = json!({
        "method": parts.method.as_str(),
        "target": parts.uri.path_and_query().unwrap().as_str(),
        "headers": headers,
        "body": String::from_utf8(body.to_vec()).unwrap(),
    });
    ([(CONTENT_TYPE, "application/json")], echo.to_string()).into_response()
}

struct Harness {
    base: String,
    addr: SocketAddr,
    upstream: Arc<Upstream>,
    metrics_base: String,
    client: reqwest::Client,
}

async fn serve(app: Router) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr
}

async fn start() -> Harness {
    start_with("1s", "").await
}

/// `extra_toml` is appended to the config, so it can add more `[[limit]]` tables.
async fn start_with(max_wait: &str, extra_toml: &str) -> Harness {
    let upstream = Arc::new(Upstream::default());
    let up_addr = serve(
        Router::new()
            .fallback(upstream_handler)
            .with_state(upstream.clone()),
    )
    .await;
    let text = format!(
        r#"
listen = "127.0.0.1:0"
proxy_token = "{TOKEN}"
max_wait = "{max_wait}"
max_body = 1024
tool_choice_prefix = "/albert/"

[[route]]
name = "minimax"
upstream = "http://{up_addr}/v1"
headers = {{ Authorization = "Bearer {UPSTREAM_KEY}", X-Api-Key = "" }}

[[route]]
name = "albert"
upstream = "http://{up_addr}/v1/"
headers = {{ Authorization = "Bearer {UPSTREAM_KEY}" }}

[[limit]]
name = "slow"
model = "slow-model"
rpm = 1
{extra_toml}"#
    );
    let proxy = Arc::new(Proxy::new(Config::parse(&text, |_| None).unwrap()).unwrap());
    let addr = serve(router(proxy.clone())).await;
    let metrics_addr = serve(metrics_router(proxy)).await;
    Harness {
        base: format!("http://{addr}"),
        addr,
        upstream,
        metrics_base: format!("http://{metrics_addr}"),
        client: reqwest::Client::new(),
    }
}

impl Harness {
    fn post(&self, path: &str, body: &str) -> reqwest::RequestBuilder {
        self.client
            .post(format!("{}{path}", self.base))
            .bearer_auth(TOKEN)
            .body(body.to_owned())
    }

    fn hits(&self) -> usize {
        self.upstream.hits.load(Ordering::SeqCst)
    }
}

async fn json_body(response: reqwest::Response) -> Value {
    serde_json::from_str(&response.text().await.unwrap()).unwrap()
}

async fn echo(response: reqwest::Response) -> Value {
    assert_eq!(response.status(), 200);
    json_body(response).await
}

#[tokio::test]
async fn rejects_missing_or_wrong_token_without_touching_upstream() {
    let h = start().await;
    let url = format!("{}/minimax/chat/completions", h.base);
    for request in [
        h.client.post(&url),
        h.client.post(&url).bearer_auth("wrong"),
        h.client.post(&url).header("x-api-key", "wrong"),
        h.client.post(&url).header("authorization", TOKEN), // missing the Bearer scheme
    ] {
        let response = request.send().await.unwrap();
        assert_eq!(response.status(), 401);
        let body: Value = json_body(response).await;
        assert_eq!(body["error"]["type"], "unauthorized");
    }
    // Auth comes before routing, so an unknown path is also a 401, not a 404.
    assert_eq!(
        h.client
            .get(format!("{}/nope/x", h.base))
            .send()
            .await
            .unwrap()
            .status(),
        401
    );
    assert_eq!(h.hits(), 0);
}

#[tokio::test]
async fn accepts_either_header_and_serves_healthz_without_a_token() {
    let h = start().await;
    let url = format!("{}/minimax/models", h.base);
    assert_eq!(
        h.client
            .get(&url)
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        h.client
            .get(&url)
            .header("x-api-key", TOKEN)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    assert_eq!(
        h.client
            .get(format!("{}/healthz", h.base))
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
}

#[tokio::test]
async fn swaps_credentials_and_hides_the_client() {
    let h = start().await;
    let response = h
        .post("/minimax/chat/completions?a=1", "{}")
        .header("x-api-key", TOKEN)
        .header("x-forwarded-for", "203.0.113.9")
        .header("x-real-ip", "203.0.113.9")
        .header("forwarded", "for=203.0.113.9")
        .header("x-custom", "kept")
        .send()
        .await
        .unwrap();
    assert_eq!(response.headers()["x-proxy-route"], "minimax");
    assert!(response.headers().contains_key("x-proxy-queue-wait-ms"));
    let seen = echo(response).await;
    assert_eq!(seen["method"], "POST");
    assert_eq!(seen["target"], "/v1/chat/completions?a=1");
    let headers = &seen["headers"];
    assert_eq!(headers["authorization"], format!("Bearer {UPSTREAM_KEY}"));
    assert_eq!(headers["x-custom"], "kept");
    for gone in ["x-api-key", "x-forwarded-for", "x-real-ip", "forwarded"] {
        assert!(
            headers.get(gone).is_none(),
            "{gone} leaked upstream: {headers}"
        );
    }
}

#[tokio::test]
async fn unknown_or_bare_route_is_a_404() {
    let h = start().await;
    for path in ["/nope/x", "/minimax"] {
        let response = h
            .client
            .get(format!("{}{path}", h.base))
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 404, "{path}");
        let body: Value = json_body(response).await;
        assert_eq!(body["error"]["message"], "no route matches this path");
    }
    assert_eq!(h.hits(), 0);
}

#[tokio::test]
async fn adds_tool_choice_only_when_tools_are_offered_without_one() {
    let h = start().await;
    let tools = r#"{"model":"m","tools":[{"type":"function"}],"stream":true}"#;
    let cases = [
        (
            "/albert/chat/completions",
            tools,
            r#"{"model":"m","tools":[{"type":"function"}],"stream":true,"tool_choice":"auto"}"#,
        ),
        (
            "/albert/chat/completions",
            r#"{"tools":[{"a":1}],"tool_choice":null}"#,
            r#"{"tools":[{"a":1}],"tool_choice":null}"#,
        ),
        (
            "/albert/chat/completions",
            r#"{"tools":[{"a":1}],"tool_choice":"none"}"#,
            r#"{"tools":[{"a":1}],"tool_choice":"none"}"#,
        ),
        (
            "/albert/chat/completions",
            r#"{"tools":[]}"#,
            r#"{"tools":[]}"#,
        ),
        (
            "/albert/chat/completions",
            r#"{"model":"m"}"#,
            r#"{"model":"m"}"#,
        ),
        ("/albert/chat/completions", "not json", "not json"),
        ("/albert/chat/completions", "[1,2]", "[1,2]"), // valid JSON, but not an object
        ("/albert/embeddings", tools, tools),
        ("/minimax/chat/completions", tools, tools),
    ];
    for (path, sent, expected) in cases {
        let seen = echo(h.post(path, sent).send().await.unwrap()).await;
        assert_eq!(seen["body"], expected, "{path} {sent}");
    }
}

#[tokio::test]
async fn holds_a_limited_request_then_answers_429() {
    let h = start().await;
    let slow = r#"{"model":"slow-model"}"#;
    assert_eq!(
        h.post("/minimax/chat/completions", slow)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );

    let start = Instant::now();
    let response = h
        .post("/minimax/chat/completions", slow)
        .send()
        .await
        .unwrap();
    assert!(
        start.elapsed() >= Duration::from_secs(1),
        "answered after {:?}",
        start.elapsed()
    );
    assert_eq!(response.status(), 429);
    assert_eq!(response.headers()["retry-after"], "1");
    let body: Value = json_body(response).await;
    assert_eq!(body["error"]["type"], "queue_timeout");
    assert_eq!(
        h.hits(),
        1,
        "a timed-out request must send nothing upstream"
    );

    // Other models are not affected.
    assert_eq!(
        h.post("/minimax/chat/completions", r#"{"model":"fast"}"#)
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
}

#[tokio::test]
async fn rejects_bodies_over_max_body() {
    let h = start().await;
    let response = h
        .post("/minimax/x", &"a".repeat(2048))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 413);
    assert_eq!(h.hits(), 0);
}

#[tokio::test]
async fn refuses_paths_that_climb_out_of_the_upstream() {
    let h = start().await;
    // reqwest would normalise these client-side, so speak HTTP directly.
    for path in [
        "/minimax/../admin",
        "/minimax/%2e%2e/admin",
        "/minimax/a/%2E%2E/%2E%2E/admin",
        "/minimax/a/.%2e/.%2E/admin",
    ] {
        let mut socket = tokio::net::TcpStream::connect(h.addr).await.unwrap();
        let request = format!(
            "GET {path} HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer {TOKEN}\r\nConnection: close\r\n\r\n"
        );
        socket.write_all(request.as_bytes()).await.unwrap();
        let mut reply = String::new();
        socket.read_to_string(&mut reply).await.unwrap();
        assert!(reply.starts_with("HTTP/1.1 400"), "{path}: {reply}");
    }
    assert_eq!(h.hits(), 0);
}

#[tokio::test]
async fn streams_without_buffering() {
    let h = start().await;
    let (tx, rx) = mpsc::channel(4);
    *h.upstream.stream.lock().unwrap() = Some(rx);
    tx.send(Ok(Bytes::from_static(b"data: one\n\n")))
        .await
        .unwrap();

    let mut response = h
        .client
        .get(format!("{}/minimax/stream", h.base))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    // The upstream has not finished, yet the first chunk must already reach the client.
    let first = tokio::time::timeout(Duration::from_secs(5), response.chunk())
        .await
        .expect("first chunk was buffered");
    assert_eq!(first.unwrap().unwrap(), "data: one\n\n");

    tx.send(Ok(Bytes::from_static(b"data: two\n\n")))
        .await
        .unwrap();
    drop(tx);
    assert_eq!(response.text().await.unwrap(), "data: two\n\n");
}

#[tokio::test]
async fn reports_upstream_failure_as_502() {
    let h = start().await;
    let dead = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap(); // closed again at once
    let text = format!(
        "listen = \"127.0.0.1:0\"\nproxy_token = \"{TOKEN}\"\n[[route]]\nname = \"dead\"\nupstream = \"http://{dead}\"\n"
    );
    let addr = serve(router(Arc::new(
        Proxy::new(Config::parse(&text, |_| None).unwrap()).unwrap(),
    )))
    .await;
    let response = h
        .client
        .get(format!("http://{addr}/dead/x"))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 502);
    let body: Value = json_body(response).await;
    assert_eq!(body["error"]["type"], "upstream_error");
}

#[tokio::test]
async fn exposes_request_counters() {
    let h = start().await;
    h.post("/minimax/x", "{}").send().await.unwrap();
    h.client
        .get(format!("{}/nope", h.base))
        .send()
        .await
        .unwrap();
    let text = h
        .client
        .get(format!("{}/metrics", h.metrics_base))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(
        text.contains(r#"llm3_http_requests_total{route="minimax",code="200"} 1"#),
        "{text}"
    );
    assert!(
        text.contains(r#"llm3_http_requests_total{route="other",code="401"} 1"#),
        "{text}"
    );
    assert!(text.contains("llm3_http_requests_in_flight 0"), "{text}");
    assert!(text.contains("llm3_http_requests_held 0"), "{text}");
}

#[tokio::test]
async fn allows_dot_segments_that_stay_inside_the_upstream() {
    let h = start().await;
    let response = h
        .client
        .get(format!("{}/minimax/a/%2E%2E/models", h.base))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    assert_eq!(echo(response).await["target"], "/v1/models");
}

/// Matches both the streaming request and the plain one below.
const ONE_AT_A_TIME: &str = r#"
[[limit]]
name = "one-at-a-time"
prefixes = ["/minimax/"]
max_inflight = 1
"#;
/// How long a second request must stay held while the first is still streaming.
const HELD_FOR: Duration = Duration::from_millis(200);
const PATIENCE: Duration = Duration::from_secs(5);

type Feed = mpsc::Sender<Result<Bytes, Infallible>>;

/// Starts a streaming request that is provably mid-stream (headers and a first chunk delivered),
/// so it holds its slot. Dropping the returned sender ends the upstream body.
async fn open_stream(h: &Harness) -> (Feed, reqwest::Response) {
    let (tx, rx) = mpsc::channel(4);
    *h.upstream.stream.lock().unwrap() = Some(rx);
    tx.send(Ok(Bytes::from_static(b"data: one\n\n")))
        .await
        .unwrap();
    let mut response = h
        .client
        .get(format!("{}/minimax/stream", h.base))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    let first = tokio::time::timeout(PATIENCE, response.chunk())
        .await
        .expect("first chunk was buffered");
    assert_eq!(first.unwrap().unwrap(), "data: one\n\n");
    (tx, response)
}

/// A second request that the one-at-a-time limit also matches, sent in the background so the test
/// can look at it while it waits.
fn spawn_second_request(h: &Harness) -> tokio::task::JoinHandle<reqwest::Response> {
    let (client, url) = (h.client.clone(), format!("{}/minimax/models", h.base));
    tokio::spawn(async move { client.get(url).bearer_auth(TOKEN).send().await.unwrap() })
}

#[tokio::test]
async fn inflight_slot_is_held_until_the_stream_ends() {
    let h = start_with("5s", ONE_AT_A_TIME).await;
    let (tx, response) = open_stream(&h).await;

    // The first response already has its headers and a chunk, yet it still owns the only slot.
    let second = spawn_second_request(&h);
    tokio::time::sleep(HELD_FOR).await;
    assert!(
        !second.is_finished(),
        "the second request ran while the first was still streaming"
    );
    assert_eq!(h.hits(), 1, "the second request reached the upstream");

    drop(tx);
    assert_eq!(response.text().await.unwrap(), "");
    let second = tokio::time::timeout(PATIENCE, second)
        .await
        .expect("the second request stayed held after the stream ended")
        .unwrap();
    assert_eq!(second.status(), 200);
    assert_eq!(h.hits(), 2);
}

#[tokio::test]
async fn inflight_slot_is_released_when_the_client_disconnects_mid_stream() {
    let h = start_with("5s", ONE_AT_A_TIME).await;
    let (tx, response) = open_stream(&h).await;

    let second = spawn_second_request(&h);
    tokio::time::sleep(HELD_FOR).await;
    assert!(
        !second.is_finished(),
        "the second request was not held, so this test would prove nothing"
    );

    drop(response); // the client goes away mid-stream
    // Give the proxy a write to fail on in case it only notices on its next write. Once it has
    // noticed, the send fails because the upstream body is gone, which is fine.
    tx.send(Ok(Bytes::from_static(b"data: two\n\n"))).await.ok();
    let second = tokio::time::timeout(PATIENCE, second)
        .await
        .expect("the slot leaked after the client disconnected")
        .unwrap();
    assert_eq!(second.status(), 200);
}

#[tokio::test]
async fn a_request_held_on_inflight_past_max_wait_gets_429_and_sends_nothing_upstream() {
    let h = start_with("1s", ONE_AT_A_TIME).await;
    let (tx, response) = open_stream(&h).await;

    let started = Instant::now();
    let second = h
        .client
        .get(format!("{}/minimax/models", h.base))
        .bearer_auth(TOKEN)
        .send()
        .await
        .unwrap();
    assert!(
        started.elapsed() >= Duration::from_secs(1),
        "answered after {:?}",
        started.elapsed()
    );
    assert_eq!(second.status(), 429);
    assert_eq!(second.headers()["retry-after"], "1");
    assert_eq!(json_body(second).await["error"]["type"], "queue_timeout");
    assert_eq!(h.hits(), 1, "a held request must send nothing upstream");

    drop(tx);
    assert_eq!(response.text().await.unwrap(), "");
}

const IN_FLIGHT: &str = "llm3_http_requests_in_flight";
const HELD: &str = "llm3_http_requests_held";
const GAUGE_POLL: Duration = Duration::from_millis(20);

/// Reads one gauge from the metrics endpoint.
async fn gauge(h: &Harness, name: &str) -> i64 {
    let text = h
        .client
        .get(format!("{}/metrics", h.metrics_base))
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let value = text
        .lines()
        .find_map(|line| line.strip_prefix(name)?.strip_prefix(' '));
    assert!(value.is_some(), "{name} is not exposed:\n{text}");
    value.unwrap().parse().unwrap()
}

/// A gauge falls a moment after the client sees the end of a body, so poll instead of reading once.
async fn wait_for_gauge(h: &Harness, name: &str, want: i64) {
    let deadline = Instant::now() + PATIENCE;
    let mut seen = gauge(h, name).await;
    while seen != want && Instant::now() < deadline {
        tokio::time::sleep(GAUGE_POLL).await;
        seen = gauge(h, name).await;
    }
    assert_eq!(seen, want, "{name} never reached {want}");
}

#[tokio::test]
async fn in_flight_counts_a_streaming_request_until_its_body_ends() {
    let h = start().await;
    let (tx, response) = open_stream(&h).await;

    // Headers and a chunk are already out, which is where the gauge used to fall back to 0.
    assert_eq!(gauge(&h, IN_FLIGHT).await, 1);
    assert_eq!(gauge(&h, HELD).await, 0);

    drop(tx);
    assert_eq!(response.text().await.unwrap(), "");
    wait_for_gauge(&h, IN_FLIGHT, 0).await;
}

#[tokio::test]
async fn in_flight_returns_to_zero_when_the_client_disconnects_mid_stream() {
    let h = start().await;
    let (tx, response) = open_stream(&h).await;
    assert_eq!(gauge(&h, IN_FLIGHT).await, 1);

    drop(response); // the client goes away mid-stream
    // Give the proxy a write to fail on in case it only notices on its next write.
    tx.send(Ok(Bytes::from_static(b"data: two\n\n"))).await.ok();
    wait_for_gauge(&h, IN_FLIGHT, 0).await;
}

#[tokio::test]
async fn held_counts_requests_waiting_on_a_limit() {
    let h = start_with("5s", ONE_AT_A_TIME).await;
    let (tx, response) = open_stream(&h).await;
    assert_eq!(gauge(&h, HELD).await, 0);

    let second = spawn_second_request(&h);
    wait_for_gauge(&h, HELD, 1).await;
    assert_eq!(gauge(&h, IN_FLIGHT).await, 2);

    drop(tx);
    assert_eq!(response.text().await.unwrap(), "");
    let second = tokio::time::timeout(PATIENCE, second)
        .await
        .expect("the second request stayed held after the stream ended")
        .unwrap();
    assert_eq!(second.status(), 200);
    second.text().await.unwrap();
    wait_for_gauge(&h, HELD, 0).await;
    wait_for_gauge(&h, IN_FLIGHT, 0).await;
}
