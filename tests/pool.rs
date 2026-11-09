#![allow(clippy::unwrap_used, clippy::expect_used)]
#![allow(
    clippy::indexing_slicing,
    reason = "Value indexing in assertions; a missing key should fail the test"
)]

use std::{
    fmt::Write,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use axum::{
    Router,
    extract::{Request, State},
    http::{StatusCode, header::AUTHORIZATION},
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

/// Every request the fake provider saw: path and `Authorization`.
type Seen = Arc<Mutex<Vec<(String, Option<String>)>>>;

/// Serves a catalog per path prefix: `/<kind>/models`.
async fn provider(State(seen): State<Seen>, req: Request) -> Response {
    let path = req.uri().path().to_owned();
    let auth = req
        .headers()
        .get(AUTHORIZATION)
        .map(|v| v.to_str().unwrap().to_owned());
    seen.lock().unwrap().push((path.clone(), auth));
    match path.as_str() {
        "/free/models" => OPENROUTER_FREE.into_response(),
        "/paid/models" => OPENROUTER_PAID.into_response(),
        "/opencode/models" => OPENCODE_CHAT_ONLY.into_response(),
        "/garbage/models" => "not json".into_response(),
        "/huge/models" => vec![b' '; 9 << 20].into_response(),
        _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
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
    seen: Seen,
    client: reqwest::Client,
}

/// One provider and route per `(path kind, catalog, endpoint, model)`, all in pool `free`,
/// in priority order; kind `dead` points at a port nothing listens on.
async fn start(routes: &[(&str, &str, &str, &str)]) -> Harness {
    let seen = Seen::default();
    let addr = serve(Router::new().fallback(provider).with_state(seen.clone())).await;
    // Dropped at once, so nothing listens there.
    let dead = TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap();
    let mut text = format!("listen = \"127.0.0.1:0\"\nproxy_token = \"{TOKEN}\"\n");
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
rpm = 10
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
        seen,
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
        self.seen.lock().unwrap().clone()
    }
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
async fn pool_paths_need_the_token_and_send_nothing_upstream() {
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
    let post = h
        .client
        .post(format!("{}/pools/free/chat/completions", h.base))
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
