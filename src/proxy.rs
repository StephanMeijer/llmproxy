//! The request path: authenticate, route, hold on limits, rewrite, forward, stream back.

use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicI64, Ordering},
    },
    time::Instant,
};

use axum::{
    Router,
    body::{Body, to_bytes},
    extract::{Request, State},
    http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, header, request::Parts},
    response::{IntoResponse, Response},
    routing::get,
};
use futures_util::StreamExt;
use reqwest::{Url, redirect};
use serde_json::{Value, json};
use tokio::time::timeout;

use crate::{
    config::{Config, ConfigError},
    limit::Limits,
};

const HEALTH_PATH: &str = "/healthz";
const METRICS_PATH: &str = "/metrics";
const CHAT_COMPLETIONS_SUFFIX: &str = "/chat/completions";
const BEARER_PREFIX: &str = "Bearer ";
const API_KEY_HEADER: &str = "x-api-key";
/// Token estimate for limits: body bytes / 4.
const BYTES_PER_TOKEN: usize = 4;
/// Metric `route` labels for requests that matched no configured route.
const LABEL_HEALTHZ: &str = "healthz";
const LABEL_OTHER: &str = "other";

const HEADER_ROUTE: HeaderName = HeaderName::from_static("x-proxy-route");
const HEADER_QUEUE_WAIT: HeaderName = HeaderName::from_static("x-proxy-queue-wait-ms");

const HOP_BY_HOP: [&str; 8] = [
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];
/// Never forwarded: the client's credentials and anything that reveals the client.
const NOT_FORWARDED: [&str; 6] = [
    "host",
    "content-length",
    "authorization",
    API_KEY_HEADER,
    "forwarded",
    "x-real-ip",
];
const X_FORWARDED_PREFIX: &str = "x-forwarded-";

struct Route {
    name: String,
    name_header: HeaderValue,
    /// Normalised upstream URL with a trailing slash; every forwarded URL must stay under it.
    base: String,
    prefix: String,
    headers: Vec<(HeaderName, Option<HeaderValue>)>,
}

impl Route {
    fn new(cfg: &crate::config::RouteConfig) -> Result<Self, ConfigError> {
        let name = &cfg.name;
        if name.is_empty()
            || !name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
        {
            return Err(ConfigError(format!(
                "route {name:?}: name must be non-empty and use only [A-Za-z0-9_-]"
            )));
        }
        let upstream = Url::parse(&cfg.upstream)
            .ok()
            .filter(|u| matches!(u.scheme(), "http" | "https") && u.host().is_some())
            .ok_or_else(|| ConfigError(format!("route {name}: upstream must be an http(s) URL")))?;
        let mut base = upstream.to_string();
        if !base.ends_with('/') {
            base.push('/');
        }
        let headers = cfg
            .headers
            .iter()
            .map(|(k, v)| {
                let bad =
                    |what: &str| ConfigError(format!("route {name}: header {k}: invalid {what}"));
                let key = HeaderName::from_bytes(k.as_bytes()).map_err(|_| bad("name"))?;
                let value = if v.is_empty() {
                    None
                } else {
                    Some(HeaderValue::from_str(v).map_err(|_| bad("value"))?)
                };
                Ok((key, value))
            })
            .collect::<Result<_, ConfigError>>()?;
        Ok(Self {
            name: name.clone(),
            name_header: HeaderValue::from_str(name)
                .map_err(|e| ConfigError(format!("route {name}: {e}")))?,
            base,
            prefix: format!("/{name}/"),
            headers,
        })
    }
}

/// Request counters and the in-flight gauge, rendered as Prometheus text.
#[derive(Default)]
pub struct Metrics {
    in_flight: AtomicI64,
    requests: Mutex<BTreeMap<(String, u16), u64>>,
}

impl Metrics {
    fn record(&self, route: &str, code: u16) {
        let mut requests = self.requests.lock().unwrap_or_else(PoisonError::into_inner);
        *requests.entry((route.to_owned(), code)).or_default() += 1;
    }

    pub fn render(&self) -> String {
        let mut out = String::from("# TYPE llm3_http_requests_total counter\n");
        let requests = self.requests.lock().unwrap_or_else(PoisonError::into_inner);
        for ((route, code), n) in requests.iter() {
            out.push_str(&format!(
                "llm3_http_requests_total{{route=\"{route}\",code=\"{code}\"}} {n}\n"
            ));
        }
        out.push_str(&format!(
            "# TYPE llm3_http_requests_in_flight gauge\nllm3_http_requests_in_flight {}\n",
            self.in_flight.load(Ordering::Relaxed)
        ));
        out
    }
}

/// Counts a request as in flight for as long as its handler future lives, including when dropped.
struct InFlight<'a>(&'a AtomicI64);

impl<'a> InFlight<'a> {
    fn new(counter: &'a AtomicI64) -> Self {
        counter.fetch_add(1, Ordering::Relaxed);
        Self(counter)
    }
}

impl Drop for InFlight<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

pub struct Proxy {
    token: String,
    max_wait: std::time::Duration,
    max_body: usize,
    tool_choice_prefix: Option<String>,
    routes: Vec<Route>,
    limits: Limits,
    client: reqwest::Client,
    metrics: Metrics,
}

impl Proxy {
    /// Validates everything that is not plain syntax: route names, URLs, headers and limits.
    pub fn new(config: Config) -> Result<Self, ConfigError> {
        let routes = config
            .routes
            .iter()
            .map(Route::new)
            .collect::<Result<Vec<_>, _>>()?;
        for (i, route) in routes.iter().enumerate() {
            if routes[..i].iter().any(|r| r.name == route.name) {
                return Err(ConfigError(format!("route {}: defined twice", route.name)));
            }
        }
        // No total timeout and no redirects: streams may run long, and a redirect would carry our credential elsewhere.
        let client = reqwest::Client::builder()
            .connect_timeout(config.connect_timeout)
            .redirect(redirect::Policy::none())
            .build()
            .map_err(|e| ConfigError(format!("build http client: {e}")))?;
        Ok(Self {
            token: config.proxy_token,
            max_wait: config.max_wait,
            max_body: config.max_body,
            tool_choice_prefix: config.tool_choice_prefix,
            limits: Limits::new(&config.limits)?,
            routes,
            client,
            metrics: Metrics::default(),
        })
    }

    pub fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    fn authorized(&self, headers: &HeaderMap) -> bool {
        let bearer = headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix(BEARER_PREFIX));
        let api_key = headers.get(API_KEY_HEADER).and_then(|v| v.to_str().ok());
        [bearer, api_key]
            .into_iter()
            .flatten()
            .any(|t| constant_time_eq(t.as_bytes(), self.token.as_bytes()))
    }

    /// Returns the metric label and the response.
    async fn serve(&self, req: Request) -> (&str, Response) {
        let (parts, body) = req.into_parts();
        let path = parts.uri.path();
        if path == HEALTH_PATH {
            return (LABEL_HEALTHZ, (StatusCode::OK, "ok").into_response());
        }
        if !self.authorized(&parts.headers) {
            return (
                LABEL_OTHER,
                error(
                    StatusCode::UNAUTHORIZED,
                    "unauthorized",
                    "missing or invalid proxy token",
                ),
            );
        }
        let Some((route, rest)) = self
            .routes
            .iter()
            .find_map(|r| Some((r, path.strip_prefix(&r.prefix)?)))
        else {
            return (
                LABEL_OTHER,
                error(
                    StatusCode::NOT_FOUND,
                    "not_found",
                    "no route matches this path",
                ),
            );
        };
        (&route.name, self.forward(route, rest, &parts, body).await)
    }

    async fn forward(&self, route: &Route, rest: &str, parts: &Parts, body: Body) -> Response {
        let path = parts.uri.path();
        let Ok(mut body) = to_bytes(body, self.max_body).await else {
            return error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "body_too_large",
                "request body exceeds max_body or could not be read",
            );
        };

        let rewrite_tools = parts.method == Method::POST
            && path.ends_with(CHAT_COMPLETIONS_SUFFIX)
            && self
                .tool_choice_prefix
                .as_deref()
                .is_some_and(|p| path.starts_with(p));
        let mut json = (rewrite_tools || self.limits.by_model())
            .then(|| serde_json::from_slice::<Value>(&body).ok())
            .flatten();
        if rewrite_tools
            && let Some(value) = json.as_mut()
            && insert_tool_choice(value)
        {
            match serde_json::to_vec(value) {
                Ok(bytes) => body = bytes.into(),
                Err(_) => {
                    return error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "internal",
                        "could not re-encode request body",
                    );
                }
            }
        }

        let model = json
            .as_ref()
            .and_then(|v| v.get("model"))
            .and_then(Value::as_str);
        let queued = Instant::now();
        let Ok(permits) = timeout(
            self.max_wait,
            self.limits
                .acquire(path, model, body.len() / BYTES_PER_TOKEN),
        )
        .await
        else {
            let mut response = error(
                StatusCode::TOO_MANY_REQUESTS,
                "queue_timeout",
                "no capacity became available within max_wait",
            );
            response.headers_mut().insert(
                header::RETRY_AFTER,
                HeaderValue::from(self.max_wait.as_secs()),
            );
            return response;
        };
        let queue_wait_ms = millis(queued.elapsed());

        let query = parts
            .uri
            .query()
            .map(|q| format!("?{q}"))
            .unwrap_or_default();
        let url = Url::parse(&format!("{}{rest}{query}", route.base))
            .ok()
            // `url` resolves `..`; refuse anything that climbs out of the route's upstream.
            .filter(|u| u.as_str().starts_with(&route.base));
        let Some(url) = url else {
            return error(
                StatusCode::BAD_REQUEST,
                "bad_request",
                "path escapes the route's upstream",
            );
        };

        let mut headers = HeaderMap::new();
        for (name, value) in &parts.headers {
            let n = name.as_str();
            if !HOP_BY_HOP.contains(&n)
                && !NOT_FORWARDED.contains(&n)
                && !n.starts_with(X_FORWARDED_PREFIX)
            {
                headers.append(name.clone(), value.clone());
            }
        }
        for (name, value) in &route.headers {
            match value {
                Some(v) => headers.insert(name.clone(), v.clone()),
                None => headers.remove(name),
            };
        }

        let upstream = match self
            .client
            .request(parts.method.clone(), url)
            .headers(headers)
            .body(body)
            .send()
            .await
        {
            Ok(upstream) => upstream,
            Err(e) => {
                tracing::warn!(route = route.name, error = %e, "upstream request failed");
                return error(
                    StatusCode::BAD_GATEWAY,
                    "upstream_error",
                    "upstream request failed",
                );
            }
        };

        let status = upstream.status();
        let mut headers = HeaderMap::new();
        for (name, value) in upstream.headers() {
            if !HOP_BY_HOP.contains(&name.as_str()) {
                headers.append(name.clone(), value.clone());
            }
        }
        headers.insert(HEADER_ROUTE, route.name_header.clone());
        headers.insert(HEADER_QUEUE_WAIT, HeaderValue::from(queue_wait_ms));

        // The permits ride along in the stream, so a concurrency cap lasts until the body ends.
        let stream = upstream.bytes_stream().map(move |chunk| {
            let _held = &permits;
            chunk
        });
        let mut response = Response::new(Body::from_stream(stream));
        *response.status_mut() = status;
        *response.headers_mut() = headers;
        response
    }
}

/// Adds `tool_choice: "auto"` when the request offers tools and does not say how to use them.
/// A present key, even `null`, counts as the caller's choice.
fn insert_tool_choice(body: &mut Value) -> bool {
    let Some(object) = body.as_object_mut() else {
        return false;
    };
    let has_tools = object
        .get("tools")
        .and_then(Value::as_array)
        .is_some_and(|t| !t.is_empty());
    if !has_tools || object.contains_key("tool_choice") {
        return false;
    }
    object.insert("tool_choice".to_owned(), json!("auto"));
    true
}

/// Whole milliseconds as `u64`, which logs and headers render as a number.
fn millis(elapsed: std::time::Duration) -> u64 {
    u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0, |diff, (x, y)| diff | (x ^ y)) == 0
}

fn error(status: StatusCode, kind: &str, message: &str) -> Response {
    let body = json!({ "error": { "type": kind, "message": message } });
    (
        status,
        [(header::CONTENT_TYPE, "application/json")],
        body.to_string(),
    )
        .into_response()
}

async fn handle(State(proxy): State<Arc<Proxy>>, req: Request) -> Response {
    let started = Instant::now();
    let (method, path) = (req.method().clone(), req.uri().path().to_owned());
    let _in_flight = InFlight::new(&proxy.metrics.in_flight);
    let (route, response) = proxy.serve(req).await;
    let status = response.status().as_u16();
    proxy.metrics.record(route, status);
    tracing::info!(route, status, %method, path, ms = millis(started.elapsed()), "request");
    response
}

/// The client-facing app. Everything is a fallback so routes stay data, not code.
pub fn router(proxy: Arc<Proxy>) -> Router {
    Router::new().fallback(handle).with_state(proxy)
}

pub fn metrics_router(proxy: Arc<Proxy>) -> Router {
    Router::new()
        .route(
            METRICS_PATH,
            get(|State(proxy): State<Arc<Proxy>>| async move { proxy.metrics.render() }),
        )
        .with_state(proxy)
}
