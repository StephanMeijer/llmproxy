#![allow(clippy::unwrap_used, clippy::expect_used)]
#![allow(
    clippy::indexing_slicing,
    reason = "Value indexing in assertions; a missing key should fail the test"
)]

use std::{
    collections::{BTreeMap, VecDeque},
    fmt::Write,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::{
        StatusCode,
        header::{ACCEPT_ENCODING, AUTHORIZATION, CONTENT_TYPE, RETRY_AFTER},
    },
    response::{IntoResponse, Response},
};
use llmproxy::{
    config::Config,
    proxy::{Proxy, metrics_router, router},
};
use serde_json::{Value, json};
use tokio::net::TcpListener;

const TOKEN: &str = "proxy-token";
const PROVIDER_KEY: &str = "Bearer provider-key";
const POLL_FOR: Duration = Duration::from_secs(5);
const MODEL: &str = "free-model";
const OPENROUTER_FREE: &str =
    r#"{"data":[{"id":"free-model","pricing":{"prompt":"0","completion":"0"}}]}"#;
const OPENROUTER_PAID: &str =
    r#"{"data":[{"id":"free-model","pricing":{"prompt":"0.000001","completion":"0"}}]}"#;
const OPENCODE_CHAT_ONLY: &str = r#"{"data":[{"id":"free-model","api":{"chat-completions":{}},
    "cost":{"text":{"input":{"price":0},"output":{"price":0}}}}]}"#;

const STREAM: [&str; 2] = ["data: {\"n\":1}\n\n", "data: [DONE]\n\n"];
const CHARGED: &str = r#"{"usage":{"cost":0.0002}}"#;
const FREE: &str = r#"{"usage":{"cost":0}}"#;
/// The cost sits in a `data:` line that two chunks split.
const CHARGED_STREAM: [&str; 3] = [
    "data: {\"n\":1}\n\ndata: {\"usage\":{\"co",
    "st\":0.0002}}\n\n",
    "data: [DONE]\n\n",
];

/// Every request the fake provider saw: path and `Authorization`.
type Seen = Arc<Mutex<Vec<(String, Option<String>)>>>;

#[derive(Clone, Default)]
struct Fake {
    seen: Seen,
    /// Statuses the next completions of a kind answer with before plain `200`s.
    script: Arc<Mutex<BTreeMap<String, VecDeque<u16>>>>,
    /// The `Accept-Encoding` of every request other than catalog fetches.
    encodings: Arc<Mutex<Vec<Option<String>>>>,
}

/// Serves a catalog per path prefix, `/<kind>/models`, and completions that echo what arrived.
async fn provider(State(fake): State<Fake>, req: Request) -> Response {
    let path = req.uri().path().to_owned();
    let auth = req
        .headers()
        .get(AUTHORIZATION)
        .map(|v| v.to_str().unwrap().to_owned());
    fake.seen.lock().unwrap().push((path.clone(), auth.clone()));
    let kind = path.split('/').nth(1).unwrap_or_default().to_owned();
    if !path.ends_with("/models") {
        let encoding = req.headers().get(ACCEPT_ENCODING);
        let encoding = encoding.map(|v| v.to_str().unwrap().to_owned());
        fake.encodings.lock().unwrap().push(encoding);
        let next = fake
            .script
            .lock()
            .unwrap()
            .get_mut(&kind)
            .and_then(VecDeque::pop_front);
        let body = to_bytes(req.into_body(), usize::MAX).await.unwrap();
        let body: Value = serde_json::from_slice(&body).unwrap();
        return match (kind.as_str(), next) {
            (_, Some(429)) => {
                (StatusCode::TOO_MANY_REQUESTS, [(RETRY_AFTER, "30")]).into_response()
            }
            (_, Some(code)) => StatusCode::from_u16(code).unwrap().into_response(),
            ("slow", None) => {
                tokio::time::sleep(Duration::from_secs(30)).await;
                StatusCode::OK.into_response()
            }
            ("stream", None) => sse(&STREAM),
            ("charged-stream", None) => sse(&CHARGED_STREAM),
            ("charged", None) => CHARGED.into_response(),
            ("zero", None) => FREE.into_response(),
            _ => json!({ "path": path, "auth": auth, "body": body })
                .to_string()
                .into_response(),
        };
    }
    match path.as_str() {
        "/paid/models" => OPENROUTER_PAID.into_response(),
        "/opencode/models" => OPENCODE_CHAT_ONLY.into_response(),
        "/free/models"
        | "/fail/models"
        | "/stream/models"
        | "/slow/models"
        | "/next/models"
        | "/charged/models"
        | "/charged-stream/models"
        | "/zero/models" => OPENROUTER_FREE.into_response(),
        "/garbage/models" => "not json".into_response(),
        "/huge/models" => vec![b' '; 9 << 20].into_response(),
        _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

fn sse(chunks: &[&'static str]) -> Response {
    let chunks = chunks
        .iter()
        .map(|c| Ok::<_, std::io::Error>(c.as_bytes()))
        .collect::<Vec<_>>();
    (
        [(CONTENT_TYPE, "text/event-stream")],
        Body::from_stream(futures_util::stream::iter(chunks)),
    )
        .into_response()
}

async fn serve(app: Router) -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    addr
}

struct Harness {
    base: String,
    metrics: String,
    fake: Fake,
    client: reqwest::Client,
}

/// One provider and route per `(path kind, catalog, endpoint, model)`, all in pool `free`,
/// in priority order; kind `dead` points at a port nothing listens on. Each route has its own
/// quota domain of 2 requests per minute.
async fn start(routes: &[(&str, &str, &str, &str)]) -> Harness {
    let fake = Fake::default();
    let addr = serve(Router::new().fallback(provider).with_state(fake.clone())).await;
    // Dropped at once, so nothing listens there.
    let dead = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap();
    let mut text = format!(
        "listen = \"127.0.0.1:0\"\nproxy_token = \"{TOKEN}\"\nmax_body = 4096\n\
         upstream_idle_timeout = \"1s\"\n"
    );
    for (n, (kind, catalog, _, _)) in routes.iter().enumerate() {
        let host = if *kind == "dead" { dead } else { addr };
        write!(
            text,
            r#"
[[pool_provider]]
name = "p{n}"
upstream = "http://{host}/{kind}"
headers = {{ Authorization = "{PROVIDER_KEY}" }}
catalog = "{catalog}"

[[quota_domain]]
name = "q{n}"
provider = "p{n}"
rpm = 2
"#
        )
        .unwrap();
    }
    text += "\n[[pool]]\nname = \"free\"\n";
    for (n, (_, _, endpoint, model)) in routes.iter().enumerate() {
        write!(
            text,
            "[[pool.route]]\npriority = {n}\nprovider = \"p{n}\"\nmodel = \"{model}\"\n\
             context = 8192\nmax_output = 1024\nendpoints = [\"{endpoint}\"]\nquota = \"q{n}\"\n"
        )
        .unwrap();
    }
    let proxy = Arc::new(Proxy::new(Config::parse(&text, |_| None).unwrap()).unwrap());
    let metrics = format!(
        "http://{}/metrics",
        serve(metrics_router(proxy.clone())).await
    );
    Harness {
        base: format!("http://{}", serve(router(proxy)).await),
        metrics,
        fake,
        // A bug must fail a test, not hang it.
        client: reqwest::Client::builder()
            .timeout(Duration::from_secs(20))
            .build()
            .unwrap(),
    }
}

impl Harness {
    async fn get(&self, path: &str) -> reqwest::Response {
        self.client
            .get(format!("{}{path}", self.base))
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap()
    }

    /// Polls the pool's status until it matches `want`, or fails after `POLL_FOR`.
    async fn wait_for(&self, want: &[&str]) -> Value {
        let deadline = Instant::now() + POLL_FOR;
        loop {
            let body = self.get("/pools/free/models").await.text().await.unwrap();
            let body: Value = serde_json::from_str(&body).unwrap();
            let statuses: Vec<&str> = body["routes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|r| r["status"].as_str().unwrap())
                .collect();
            if statuses == want {
                return body;
            }
            assert!(
                Instant::now() < deadline,
                "still {statuses:?}, want {want:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    fn seen(&self) -> Vec<(String, Option<String>)> {
        self.fake.seen.lock().unwrap().clone()
    }

    /// The paths of every request other than catalog fetches.
    fn calls(&self) -> Vec<String> {
        self.seen()
            .into_iter()
            .map(|(path, _)| path)
            .filter(|path| !path.ends_with("/models"))
            .collect()
    }

    /// The next completions of `kind` answer with `codes` first.
    fn script(&self, kind: &str, codes: &[u16]) {
        self.fake
            .script
            .lock()
            .unwrap()
            .entry(kind.to_owned())
            .or_default()
            .extend(codes);
    }

    async fn post(&self, path: &str, body: &str, session: Option<&str>) -> reqwest::Response {
        let mut req = self
            .client
            .post(format!("{}{path}", self.base))
            .bearer_auth(TOKEN)
            .header(CONTENT_TYPE, "application/json")
            .body(body.to_owned());
        if let Some(session) = session {
            req = req.header("x-llmproxy-session", session);
        }
        req.send().await.unwrap()
    }

    async fn metrics(&self) -> String {
        let response = self.client.get(&self.metrics).send().await.unwrap();
        response.text().await.unwrap()
    }

    /// Polls the metrics until each `series value` line is there, or fails after `POLL_FOR`;
    /// a charge is counted as the body ends, which can be just after the client has it all.
    async fn counts(&self, want: &[&str]) {
        let deadline = Instant::now() + POLL_FOR;
        loop {
            let metrics = self.metrics().await;
            let missing = want
                .iter()
                .find(|line| !metrics.contains(&format!("{line}\n")));
            let Some(line) = missing else { return };
            assert!(Instant::now() < deadline, "{line} not in\n{metrics}");
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    async fn chat(&self, session: Option<&str>) -> reqwest::Response {
        self.post("/pools/free/chat/completions", CHAT_BODY, session)
            .await
    }
}

const CHAT_BODY: &str = r#"{"model":"anything","messages":[{"role":"user","content":"hi"}]}"#;

async fn json(response: reqwest::Response) -> Value {
    serde_json::from_str(&response.text().await.unwrap()).unwrap()
}

/// The route that served, and the fake's echo of what it received.
async fn served(response: reqwest::Response) -> (String, String, Value) {
    assert_eq!(response.status(), StatusCode::OK);
    let header = |name| response.headers()[name].to_str().unwrap().to_owned();
    let (route, priority) = (
        header("x-llmproxy-pool-route"),
        header("x-llmproxy-pool-priority"),
    );
    (route, priority, json(response).await)
}

#[tokio::test]
async fn a_free_catalog_makes_the_route_eligible_with_provider_headers() {
    let h = start(&[("free", "openrouter", "chat", MODEL)]).await;
    let body = h.wait_for(&["eligible"]).await;
    let route = &body["routes"][0];
    assert_eq!(body["pool"], "free");
    assert_eq!(route["priority"], 0);
    assert_eq!(route["provider"], "p0");
    assert_eq!(route["model"], MODEL);
    assert_eq!(route["endpoints"], json!(["chat"]));
    let left = route["proof_valid_for_secs"].as_u64().unwrap();
    assert!((1790..=1800).contains(&left), "{left}");
    // The provider's key replaces the client's token on the catalog fetch.
    assert_eq!(
        h.seen(),
        [("/free/models".to_owned(), Some(PROVIDER_KEY.to_owned()))]
    );
}

#[tokio::test]
async fn every_route_closes_unless_its_catalog_proves_it_free() {
    let h = start(&[
        ("paid", "openrouter", "chat", MODEL),
        ("free", "openrouter", "chat", "gone-model"),
        ("opencode", "opencode", "responses", MODEL),
        ("broken", "openrouter", "chat", MODEL),
        ("garbage", "openrouter", "chat", MODEL),
        ("huge", "openrouter", "chat", MODEL),
        ("dead", "openrouter", "chat", MODEL),
    ])
    .await;
    // Every live provider was asked, so the failing fetches really happened.
    let deadline = Instant::now() + POLL_FOR;
    while h.seen().len() < 6 {
        assert!(Instant::now() < deadline, "fetched only {:?}", h.seen());
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let body = h
        .wait_for(&[
            "closed: not free",
            "closed: missing from catalog",
            "closed: endpoint not in catalog",
            "closed: no proof",
            "closed: no proof",
            "closed: no proof",
            "closed: no proof",
        ])
        .await;
    assert!(body["routes"][3]["proof_valid_for_secs"].is_null());
}

#[tokio::test]
async fn pool_paths_need_the_token_and_unknown_ones_send_nothing_upstream() {
    let h = start(&[("free", "openrouter", "chat", MODEL)]).await;
    h.wait_for(&["eligible"]).await;
    let anonymous = h
        .client
        .get(format!("{}/pools/free/models", h.base))
        .send()
        .await
        .unwrap();
    assert_eq!(anonymous.status(), StatusCode::UNAUTHORIZED);
    for path in [
        "/pools/nope/models",
        "/pools",
        "/pools/free",
        "/pools/free/other",
    ] {
        assert_eq!(h.get(path).await.status(), StatusCode::NOT_FOUND, "{path}");
    }
    for (method, path) in [
        (reqwest::Method::GET, "/pools/free/chat/completions"),
        (reqwest::Method::PUT, "/pools/free/responses"),
    ] {
        let response = h
            .client
            .request(method, format!("{}{path}", h.base))
            .bearer_auth(TOKEN)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
    }
    let post = h
        .client
        .post(format!("{}/pools/free/other", h.base))
        .bearer_auth(TOKEN)
        .body(r#"{"model":"free-model"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(post.status(), StatusCode::NOT_FOUND);
    let body: Value = serde_json::from_str(&post.text().await.unwrap()).unwrap();
    assert_eq!(body["error"]["type"], "not_found");
    // Only the catalog was ever fetched.
    assert!(h.seen().iter().all(|(path, _)| path == "/free/models"));
    // Known pools count under their own label; anything else stays "other".
    let metrics = h
        .client
        .get(&h.metrics)
        .send()
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(metrics.contains(r#"llm3_http_requests_total{route="pool:free",code="200"}"#));
    assert!(metrics.contains(r#"llm3_http_requests_total{route="pool:free",code="404"}"#));
    assert!(metrics.contains(r#"llm3_http_requests_total{route="other",code="404"}"#));
}

#[tokio::test]
async fn the_first_route_serves_with_its_model_and_the_provider_key() {
    let h = start(&[
        ("free", "openrouter", "chat", MODEL),
        ("next", "openrouter", "chat", MODEL),
    ])
    .await;
    h.wait_for(&["eligible", "eligible"]).await;
    let (route, priority, echo) = served(h.chat(None).await).await;
    assert_eq!(route, "p0/free-model");
    assert_eq!(priority, "0");
    assert_eq!(echo["path"], "/free/chat/completions");
    // The provider's key replaces the client's token, and the route's model replaces the body's.
    assert_eq!(echo["auth"], PROVIDER_KEY);
    assert_eq!(echo["body"]["model"], MODEL);
    assert_eq!(echo["body"]["messages"][0]["content"], "hi");
    assert_eq!(h.calls(), ["/free/chat/completions"]);
    h.counts(&[
        r#"llm3_pool_requests_total{pool="free",outcome="served"} 1"#,
        r#"llm3_pool_attempts_total{pool="free",route="p0/free-model",result="ok"} 1"#,
        r#"llm3_pool_charged_responses_total{pool="free",route="p0/free-model"} 0"#,
        r#"llm3_pool_auth_failures_total{pool="free",route="p1/free-model"} 0"#,
    ])
    .await;
}

#[tokio::test]
async fn a_server_error_or_timeout_fails_over_to_the_next_route() {
    let h = start(&[
        ("slow", "openrouter", "chat", MODEL),
        ("fail", "openrouter", "chat", MODEL),
        ("next", "openrouter", "chat", MODEL),
    ])
    .await;
    h.wait_for(&["eligible", "eligible", "eligible"]).await;
    h.script("fail", &[500]);
    let (route, priority, _) = served(h.chat(None).await).await;
    assert_eq!((route.as_str(), priority.as_str()), ("p2/free-model", "2"));
    assert_eq!(
        h.calls(),
        [
            "/slow/chat/completions",
            "/fail/chat/completions",
            "/next/chat/completions"
        ]
    );
    h.counts(&[
        r#"llm3_pool_attempts_total{pool="free",route="p0/free-model",result="failover_error"} 1"#,
        r#"llm3_pool_attempts_total{pool="free",route="p1/free-model",result="failover_5xx"} 1"#,
        r#"llm3_pool_attempts_total{pool="free",route="p2/free-model",result="ok"} 1"#,
        r#"llm3_pool_requests_total{pool="free",outcome="served"} 1"#,
    ])
    .await;
}

#[tokio::test]
async fn a_429_skips_the_quota_domain_until_retry_after() {
    let h = start(&[
        ("fail", "openrouter", "chat", MODEL),
        ("next", "openrouter", "chat", MODEL),
    ])
    .await;
    h.wait_for(&["eligible", "eligible"]).await;
    h.script("fail", &[429]);
    assert_eq!(served(h.chat(None).await).await.0, "p1/free-model");
    // The domain cools down, so the next request goes straight to the second route.
    assert_eq!(served(h.chat(None).await).await.0, "p1/free-model");
    assert_eq!(
        h.calls(),
        [
            "/fail/chat/completions",
            "/next/chat/completions",
            "/next/chat/completions"
        ]
    );
    h.counts(&[
        r#"llm3_pool_attempts_total{pool="free",route="p0/free-model",result="failover_429"} 1"#,
        r#"llm3_pool_attempts_total{pool="free",route="p1/free-model",result="ok"} 2"#,
    ])
    .await;
}

#[tokio::test]
async fn a_rejected_provider_key_cools_the_route_down() {
    let h = start(&[
        ("fail", "openrouter", "chat", MODEL),
        ("free", "openrouter", "chat", MODEL),
        ("next", "openrouter", "chat", MODEL),
    ])
    .await;
    h.wait_for(&["eligible", "eligible", "eligible"]).await;
    h.script("fail", &[401]);
    h.script("free", &[403]);
    assert_eq!(served(h.chat(None).await).await.0, "p2/free-model");
    assert_eq!(served(h.chat(None).await).await.0, "p2/free-model");
    assert_eq!(
        h.calls(),
        [
            "/fail/chat/completions",
            "/free/chat/completions",
            "/next/chat/completions",
            "/next/chat/completions"
        ]
    );
    h.counts(&[
        r#"llm3_pool_attempts_total{pool="free",route="p0/free-model",result="auth_failure"} 1"#,
        r#"llm3_pool_attempts_total{pool="free",route="p1/free-model",result="auth_failure"} 1"#,
        r#"llm3_pool_auth_failures_total{pool="free",route="p0/free-model"} 1"#,
        r#"llm3_pool_auth_failures_total{pool="free",route="p1/free-model"} 1"#,
        r#"llm3_pool_auth_failures_total{pool="free",route="p2/free-model"} 0"#,
    ])
    .await;
}

#[tokio::test]
async fn a_session_stays_on_its_route_after_the_first_recovers() {
    let h = start(&[
        ("fail", "openrouter", "chat", MODEL),
        ("next", "openrouter", "chat", MODEL),
    ])
    .await;
    h.wait_for(&["eligible", "eligible"]).await;
    h.script("fail", &[500]);
    assert_eq!(served(h.chat(Some("abc")).await).await.0, "p1/free-model");
    // The first route works again, but the session keeps its route.
    assert_eq!(served(h.chat(Some("abc")).await).await.0, "p1/free-model");
    // An invalid header is ignored, so priority order applies.
    assert_eq!(
        served(h.chat(Some("has space")).await).await.0,
        "p0/free-model"
    );
}

#[tokio::test]
async fn a_pinned_route_that_stops_being_a_candidate_falls_through() {
    let h = start(&[
        ("fail", "openrouter", "chat", MODEL),
        ("next", "openrouter", "chat", MODEL),
    ])
    .await;
    h.wait_for(&["eligible", "eligible"]).await;
    h.script("fail", &[500]);
    assert_eq!(served(h.chat(Some("abc")).await).await.0, "p1/free-model");
    assert_eq!(served(h.chat(Some("abc")).await).await.0, "p1/free-model");
    // Its quota of 2 per minute is spent, so the session moves on and stays there.
    assert_eq!(served(h.chat(Some("abc")).await).await.0, "p0/free-model");
}

#[tokio::test]
async fn requests_that_could_cost_money_are_refused_before_any_upstream_call() {
    let h = start(&[("free", "openrouter", "chat", MODEL)]).await;
    h.wait_for(&["eligible"]).await;
    for body in [
        r#"{"plugins":[]}"#,
        r#"{"web_search_options":{}}"#,
        r#"{"model":"free-model:online"}"#,
        r#"{"provider":{"order":["x"]}}"#,
        r#"{"models":["a","b"]}"#,
        r#"{"route":"fallback"}"#,
        "[]",
        "not json",
    ] {
        let response = h.post("/pools/free/chat/completions", body, None).await;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{body}");
        let error: Value = json(response).await;
        assert_eq!(error["error"]["type"], "bad_request", "{body}");
    }
    let huge = format!(r#"{{"x":"{}"}}"#, "a".repeat(5000));
    let response = h.post("/pools/free/chat/completions", &huge, None).await;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    assert!(h.calls().is_empty(), "{:?}", h.calls());
    h.counts(&[r#"llm3_pool_requests_total{pool="free",outcome="rejected"} 9"#])
        .await;
}

#[tokio::test]
async fn a_route_without_free_proof_is_never_called() {
    let h = start(&[
        ("paid", "openrouter", "chat", MODEL),
        ("free", "openrouter", "chat", MODEL),
    ])
    .await;
    h.wait_for(&["closed: not free", "eligible"]).await;
    assert_eq!(served(h.chat(None).await).await.0, "p1/free-model");
    assert_eq!(h.calls(), ["/free/chat/completions"]);
}

#[tokio::test]
async fn a_stream_is_relayed_as_it_arrives() {
    let h = start(&[("stream", "openrouter", "chat", MODEL)]).await;
    h.wait_for(&["eligible"]).await;
    let response = h.chat(None).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[CONTENT_TYPE], "text/event-stream");
    assert_eq!(response.headers()["x-llmproxy-pool-route"], "p0/free-model");
    assert_eq!(response.text().await.unwrap(), STREAM.concat());
}

#[tokio::test]
async fn nothing_left_to_try_is_pool_exhausted_with_retry_after() {
    let h = start(&[
        ("paid", "openrouter", "chat", MODEL),
        ("fail", "openrouter", "chat", MODEL),
    ])
    .await;
    h.wait_for(&["closed: not free", "eligible"]).await;
    // A 5xx leaves no wait to report, a spent quota does.
    for codes in [[500], [500]] {
        h.script("fail", &codes);
        let response = h.chat(None).await;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        let retry: u64 = response.headers()[RETRY_AFTER]
            .to_str()
            .unwrap()
            .parse()
            .unwrap();
        assert!(retry >= 1, "{retry}");
        let error: Value = json(response).await;
        assert_eq!(error["error"]["type"], "pool_exhausted");
    }
    let response = h.chat(None).await;
    assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    let retry: u64 = response.headers()[RETRY_AFTER]
        .to_str()
        .unwrap()
        .parse()
        .unwrap();
    assert!((1..=30).contains(&retry), "{retry}");
    assert_eq!(h.calls().len(), 2);
    h.counts(&[r#"llm3_pool_requests_total{pool="free",outcome="exhausted"} 3"#])
        .await;
}

#[tokio::test]
async fn responses_go_only_to_routes_that_declare_them() {
    let h = start(&[
        ("free", "openrouter", "chat", MODEL),
        ("opencode", "opencode", "responses", MODEL),
        ("next", "openrouter", "responses", MODEL),
    ])
    .await;
    h.wait_for(&["eligible", "closed: endpoint not in catalog", "eligible"])
        .await;
    let response = h
        .post("/pools/free/responses", r#"{"input":"hi"}"#, None)
        .await;
    let (route, priority, echo) = served(response).await;
    assert_eq!((route.as_str(), priority.as_str()), ("p2/free-model", "2"));
    assert_eq!(echo["path"], "/next/responses");
    assert_eq!(h.calls(), ["/next/responses"]);
}

#[tokio::test]
async fn all_routes_closed_counts_an_exhausted_request() {
    let h = start(&[("paid", "openrouter", "chat", MODEL)]).await;
    h.wait_for(&["closed: not free"]).await;
    assert_eq!(h.chat(None).await.status(), StatusCode::SERVICE_UNAVAILABLE);
    h.counts(&[r#"llm3_pool_requests_total{pool="free",outcome="exhausted"} 1"#])
        .await;
    let metrics = h.metrics().await;
    for name in [
        "llm3_pool_requests_total",
        "llm3_pool_attempts_total",
        "llm3_pool_auth_failures_total",
        "llm3_pool_charged_responses_total",
    ] {
        assert!(
            metrics.contains(&format!("# TYPE {name} counter\n")),
            "{name}"
        );
    }
}

#[tokio::test]
async fn a_reported_charge_is_counted_and_the_bytes_pass_unchanged() {
    let h = start(&[
        ("charged", "openrouter", "chat", MODEL),
        ("charged-stream", "openrouter", "chat", MODEL),
        ("zero", "openrouter", "chat", MODEL),
    ])
    .await;
    h.wait_for(&["eligible", "eligible", "eligible"]).await;
    let route = |n: usize| format!(r#"{{pool="free",route="p{n}/free-model"}}"#);
    let charged =
        |n: usize, count: u8| format!("llm3_pool_charged_responses_total{} {count}", route(n));
    // Each route has 2 requests a minute; spending the earlier ones moves on to the next route.
    for (n, body) in [
        (0, CHARGED.to_owned()),
        (0, CHARGED.to_owned()),
        (1, CHARGED_STREAM.concat()),
        (1, CHARGED_STREAM.concat()),
        (2, FREE.to_owned()),
    ] {
        let response = h.chat(None).await;
        assert_eq!(
            response.headers()["x-llmproxy-pool-route"],
            format!("p{n}/free-model")
        );
        assert_eq!(response.bytes().await.unwrap(), body.as_bytes());
    }
    let want = [charged(0, 2), charged(1, 2), charged(2, 0)];
    h.counts(&want.iter().map(String::as_str).collect::<Vec<_>>())
        .await;
}

#[tokio::test]
async fn pool_attempts_ask_for_identity_so_a_charge_stays_readable() {
    let h = start(&[("charged", "openrouter", "chat", MODEL)]).await;
    h.wait_for(&["eligible"]).await;
    let response = h
        .client
        .post(format!("{}/pools/free/chat/completions", h.base))
        .bearer_auth(TOKEN)
        .header(CONTENT_TYPE, "application/json")
        .header(ACCEPT_ENCODING, "gzip")
        .body(CHAT_BODY)
        .send()
        .await
        .unwrap();
    assert_eq!(response.bytes().await.unwrap(), CHARGED.as_bytes());
    assert_eq!(
        *h.fake.encodings.lock().unwrap(),
        [Some("identity".to_owned())]
    );
    h.counts(&[r#"llm3_pool_charged_responses_total{pool="free",route="p0/free-model"} 1"#])
        .await;
}
