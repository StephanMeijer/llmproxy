//! The request path: authenticate, route, hold on limits, rewrite, forward, stream back.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex, PoisonError,
        atomic::{AtomicI64, Ordering},
    },
    time::{Duration, Instant},
};

use axum::{
    Router,
    body::{Body, Bytes, HttpBody, to_bytes},
    extract::{Request, State},
    http::{HeaderMap, HeaderName, HeaderValue, Method, StatusCode, header, request::Parts},
    response::{IntoResponse, Response},
    routing::get,
};
use futures_util::StreamExt;
use reqwest::{Url, redirect};
use serde_json::{Value, json};
use tokio::{
    sync::OwnedSemaphorePermit,
    time::{sleep, timeout},
};

use crate::{
    auth::{AuthError, Authenticator, RouteKind},
    config::{Config, ConfigError},
    limit::Limits,
    metrics::CredentialMetrics,
};

#[path = "claude.rs"]
mod claude;

const HEALTH_PATH: &str = "/healthz";
const METRICS_PATH: &str = "/metrics";
const CHAT_COMPLETIONS_SUFFIX: &str = "/chat/completions";
const API_KEY_HEADER: &str = "x-api-key";
/// Token estimate for limits: body bytes / 4.
const BYTES_PER_TOKEN: usize = 4;
/// Metric `route` labels for requests that matched no configured route.
const LABEL_HEALTHZ: &str = "healthz";
const LABEL_OTHER: &str = "other";

const HEADER_ROUTE: HeaderName = HeaderName::from_static("x-proxy-route");
const HEADER_QUEUE_WAIT: HeaderName = HeaderName::from_static("x-proxy-queue-wait-ms");
const HEADER_RETRIES: HeaderName = HeaderName::from_static("x-proxy-retries");

/// First wait before resending after an upstream 429 with no `Retry-After`; it doubles up to the cap.
const RETRY_BACKOFF_START: Duration = Duration::from_secs(1);
const RETRY_BACKOFF_CAP: Duration = Duration::from_secs(10);
/// Floor for any retry wait, so `Retry-After: 0` cannot make the proxy spin.
const MIN_RETRY_WAIT: Duration = Duration::from_millis(250);

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

/// Upper bounds of the duration histograms, from quick replies to long generations.
///
/// shortcut: fixed, not configurable; upgrade if a deployment needs other boundaries.
const BUCKETS: [Duration; 12] = [
    Duration::from_millis(50),
    Duration::from_millis(100),
    Duration::from_millis(250),
    Duration::from_millis(500),
    Duration::from_secs(1),
    Duration::from_millis(2500),
    Duration::from_secs(5),
    Duration::from_secs(10),
    Duration::from_secs(30),
    Duration::from_mins(1),
    Duration::from_mins(2),
    Duration::from_mins(5),
];

struct Route {
    name: String,
    name_header: HeaderValue,
    /// Normalised upstream URL with a trailing slash; every forwarded URL must stay under it.
    base: String,
    prefix: String,
    headers: Vec<(HeaderName, Option<HeaderValue>)>,
    retry_429: bool,
}

/// Everything needed to send, and resend, one upstream request.
struct Outgoing {
    method: Method,
    url: Url,
    headers: HeaderMap,
    body: Bytes,
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
            retry_429: cfg.retry_429,
        })
    }
}

const DURATION_METRIC: &str = "llm3_http_request_duration_seconds";
const HEADER_DURATION_METRIC: &str = "llm3_http_request_header_duration_seconds";

/// One Prometheus histogram series. Each bucket counts the observations at or below its bound.
#[derive(Default)]
struct Histogram {
    le: [u64; BUCKETS.len()],
    count: u64,
    sum: Duration,
}

impl Histogram {
    fn observe(&mut self, elapsed: Duration) {
        for (n, bound) in self.le.iter_mut().zip(BUCKETS) {
            if elapsed <= bound {
                *n += 1;
            }
        }
        self.count += 1;
        self.sum += elapsed;
    }

    fn render(&self, name: &str, route: &str) -> String {
        let buckets = self
            .le
            .iter()
            .zip(BUCKETS)
            .map(|(n, bound)| {
                let bound = bound.as_secs_f64();
                format!("{name}_bucket{{route=\"{route}\",le=\"{bound}\"}} {n}\n")
            })
            .collect::<Vec<_>>()
            .concat();
        let (count, sum) = (self.count, self.sum.as_secs_f64());
        format!(
            "{buckets}{name}_bucket{{route=\"{route}\",le=\"+Inf\"}} {count}\n\
             {name}_sum{{route=\"{route}\"}} {sum}\n\
             {name}_count{{route=\"{route}\"}} {count}\n"
        )
    }
}

type Durations = Mutex<BTreeMap<String, Histogram>>;

fn observe(durations: &Durations, route: &str, elapsed: Duration) {
    let mut durations = durations.lock().unwrap_or_else(PoisonError::into_inner);
    durations
        .entry(route.to_owned())
        .or_default()
        .observe(elapsed);
}

fn render_durations(name: &str, help: &str, durations: &Durations) -> String {
    let rows = {
        let durations = durations.lock().unwrap_or_else(PoisonError::into_inner);
        durations
            .iter()
            .map(|(route, histogram)| histogram.render(name, route))
            .collect::<String>()
    };
    format!("# HELP {name} {help}\n# TYPE {name} histogram\n{rows}")
}

/// Request counters, two gauges and two duration histograms, rendered as Prometheus text.
#[derive(Default)]
pub struct Metrics {
    credentials: Arc<CredentialMetrics>,
    /// Requests from arrival until their response body ends.
    in_flight: Arc<AtomicI64>,
    /// Requests currently waiting on a limit.
    held: Arc<AtomicI64>,
    requests: Mutex<BTreeMap<(String, u16), u64>>,
    /// Seconds from arrival until the response headers are ready, by route.
    to_headers: Durations,
    /// Seconds from arrival until the response body ends, by route.
    to_end: Durations,
}

impl Metrics {
    /// Bounded telemetry for database and credential lifecycle owners.
    pub fn credentials(&self) -> &CredentialMetrics {
        &self.credentials
    }

    fn record(&self, route: &str, code: u16) {
        let mut requests = self.requests.lock().unwrap_or_else(PoisonError::into_inner);
        *requests.entry((route.to_owned(), code)).or_default() += 1;
    }

    fn observe_headers(&self, route: &str, elapsed: Duration) {
        observe(&self.to_headers, route, elapsed);
    }

    fn observe_end(&self, route: &str, elapsed: Duration) {
        observe(&self.to_end, route, elapsed);
    }

    pub fn render(&self) -> String {
        let rows = {
            let requests = self.requests.lock().unwrap_or_else(PoisonError::into_inner);
            requests
                .iter()
                .map(|((route, code), n)| {
                    format!("llm3_http_requests_total{{route=\"{route}\",code=\"{code}\"}} {n}\n")
                })
                .collect::<Vec<_>>()
                .concat()
        };
        let headers = render_durations(
            HEADER_DURATION_METRIC,
            "Seconds from arrival until the response headers are ready.",
            &self.to_headers,
        );
        let end = render_durations(
            DURATION_METRIC,
            "Seconds from arrival until the response body ends.",
            &self.to_end,
        );
        format!(
            "# HELP llm3_http_requests_total Requests by route and status code.\n\
             # TYPE llm3_http_requests_total counter\n{rows}\
             # HELP llm3_http_requests_in_flight Requests from arrival until their response body ends.\n\
             # TYPE llm3_http_requests_in_flight gauge\nllm3_http_requests_in_flight {}\n\
             # HELP llm3_http_requests_held Requests currently waiting on a limit.\n\
             # TYPE llm3_http_requests_held gauge\nllm3_http_requests_held {}\n\
             {headers}{end}{}",
            self.in_flight.load(Ordering::Relaxed),
            self.held.load(Ordering::Relaxed),
            self.credentials.render()
        )
    }
}

/// Counts one request on a gauge for as long as it lives, including when the owning future is
/// dropped. It owns an `Arc` so it can ride inside a `'static` response body.
struct Counted(Arc<AtomicI64>);

impl Counted {
    fn new(gauge: &Arc<AtomicI64>) -> Self {
        gauge.fetch_add(1, Ordering::Relaxed);
        Self(Arc::clone(gauge))
    }
}

impl Drop for Counted {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Records how long a request lived when it drops, including when the client leaves mid-stream.
/// It owns an `Arc` so it can ride inside a `'static` response body, like `Counted`.
struct Timed {
    proxy: Arc<Proxy>,
    route: String,
    started: Instant,
}

impl Drop for Timed {
    fn drop(&mut self) {
        self.proxy
            .metrics
            .observe_end(&self.route, self.started.elapsed());
    }
}

pub struct Proxy {
    auth: Authenticator,
    claude: Option<claude::Claude>,
    max_wait: Duration,
    max_body: usize,
    tool_choice_prefix: Option<String>,
    routes: Vec<Route>,
    limits: Limits,
    client: reqwest::Client,
    metrics: Metrics,
}

impl Proxy {
    /// Shared metrics rendered by this proxy's metrics listener.
    pub const fn metrics(&self) -> &Metrics {
        &self.metrics
    }

    /// Validates everything that is not plain syntax: route names, URLs, headers and limits.
    ///
    /// # Errors
    /// Returns a [`ConfigError`] naming the first invalid route or limit, or if the HTTP client
    /// cannot be built.
    pub fn new(mut config: Config) -> Result<Self, ConfigError> {
        config.validate_adapters()?;
        let routes = config
            .routes
            .iter()
            .map(Route::new)
            .collect::<Result<Vec<_>, _>>()?;
        let mut seen = BTreeSet::new();
        if let Some(twice) = routes.iter().find(|r| !seen.insert(&r.name)) {
            return Err(ConfigError(format!("route {}: defined twice", twice.name)));
        }
        // No total timeout and no redirects: streams may run long, and a redirect would carry our credential elsewhere.
        let client = reqwest::Client::builder()
            .connect_timeout(config.connect_timeout)
            .redirect(redirect::Policy::none())
            .build()
            .map_err(|e| ConfigError(format!("build http client: {e}")))?;
        let (claude, limits) = claude::Claude::configure(&config)?;
        let auth = Authenticator::new(config.proxy_token, config.postgres.as_ref());
        let metrics = Metrics {
            credentials: Arc::clone(&auth.metrics),
            ..Metrics::default()
        };
        Ok(Self {
            auth,
            claude,
            max_wait: config.max_wait,
            max_body: config.max_body,
            tool_choice_prefix: config.tool_choice_prefix,
            limits,
            routes,
            client,
            metrics,
        })
    }

    /// Returns the metric label and the response.
    async fn serve(&self, req: Request) -> (&str, Response) {
        let (mut parts, body) = req.into_parts();
        let path = parts.uri.path();
        if path == HEALTH_PATH {
            return (LABEL_HEALTHZ, (StatusCode::OK, "ok").into_response());
        }
        let matched = self
            .routes
            .iter()
            .find_map(|r| Some((r, path.strip_prefix(&r.prefix)?)));
        let kind = if self.claude.is_some() && (path == "/claude" || path.starts_with("/claude/")) {
            RouteKind::Claude
        } else if matched.is_some() {
            RouteKind::Static
        } else {
            RouteKind::Unknown
        };
        match self.auth.authenticate(kind, &parts.headers).await {
            Ok(Some(principal)) => {
                parts.extensions.insert(principal);
            }
            Ok(None) => {}
            Err(failure) => {
                let kind = match failure {
                    AuthError::Unauthorized => "unauthorized",
                    AuthError::Forbidden => "forbidden",
                    AuthError::Unavailable => "authentication_unavailable",
                };
                return (
                    LABEL_OTHER,
                    error(failure.status(), kind, "proxy authentication failed"),
                );
            }
        }
        if kind == RouteKind::Claude {
            if parts.uri.path() != claude::MESSAGES_PATH || parts.method != Method::POST {
                return (
                    "claude",
                    error(
                        StatusCode::NOT_FOUND,
                        "not_found",
                        "no route matches this path",
                    ),
                );
            }
            if let Some(claude) = &self.claude {
                return ("claude", claude.forward(self, &parts, body).await);
            }
        }
        let Some((route, rest)) = matched else {
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
        let Ok(body) = to_bytes(body, self.max_body).await else {
            return error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "body_too_large",
                "request body exceeds max_body or could not be read",
            );
        };
        let (body, model) = self.inspect_body(parts, body);

        let queued = Instant::now();
        let deadline = queued + self.max_wait;
        let held = self.limits.acquire(
            parts.uri.path(),
            model.as_deref(),
            body.len() / BYTES_PER_TOKEN,
        );
        let waiting = Counted::new(&self.metrics.held);
        let Ok(permits) = timeout(self.max_wait, held).await else {
            return queue_timeout(self.max_wait);
        };
        drop(waiting);
        let queue_wait_ms = millis(queued.elapsed());

        let Some(url) = upstream_url(route, rest, parts.uri.query()) else {
            return error(
                StatusCode::BAD_REQUEST,
                "bad_request",
                "path escapes the route's upstream",
            );
        };
        let outgoing = Outgoing {
            method: parts.method.clone(),
            url,
            headers: request_headers(parts, route),
            body,
        };
        match self.send_upstream(route, &outgoing, deadline).await {
            Ok((upstream, retries)) => relay(route, upstream, queue_wait_ms, retries, permits),
            Err(e) => {
                tracing::warn!(route = route.name, error = %e, "upstream request failed");
                error(
                    StatusCode::BAD_GATEWAY,
                    "upstream_error",
                    "upstream request failed",
                )
            }
        }
    }

    /// Sends `outgoing`. On an upstream `429` it waits and resends until `deadline`, then hands back
    /// the last response. Returns the response and how many times the request was resent.
    ///
    /// shortcut: `Retry-After` HTTP-dates are ignored (the backoff is used instead), there is no
    /// jitter, and a resend does not draw on the local rpm/tpm budget again; upgrade if a provider
    /// needs any of them.
    async fn send_upstream(
        &self,
        route: &Route,
        outgoing: &Outgoing,
        deadline: Instant,
    ) -> Result<(reqwest::Response, u32), reqwest::Error> {
        let mut backoff = RETRY_BACKOFF_START;
        let mut retries = 0;
        loop {
            let response = self
                .client
                .request(outgoing.method.clone(), outgoing.url.clone())
                .headers(outgoing.headers.clone())
                .body(outgoing.body.clone())
                .send()
                .await?;
            if !route.retry_429 || response.status() != StatusCode::TOO_MANY_REQUESTS {
                return Ok((response, retries));
            }
            let wait = retry_after(response.headers())
                .unwrap_or(backoff)
                .max(MIN_RETRY_WAIT);
            if Instant::now() + wait >= deadline {
                // Waiting would outlast the budget: pass this 429 on now, with the provider's own headers.
                return Ok((response, retries));
            }
            drop(response);
            let waiting = Counted::new(&self.metrics.held);
            sleep(wait).await;
            drop(waiting);
            retries += 1;
            backoff = (backoff * 2).min(RETRY_BACKOFF_CAP);
        }
    }

    /// Applies the `tool_choice` shim and reads the `model` the limits match on. The body is only
    /// parsed when one of those needs it.
    fn inspect_body(&self, parts: &Parts, body: Bytes) -> (Bytes, Option<String>) {
        let path = parts.uri.path();
        let rewrite_tools = parts.method == Method::POST
            && path.ends_with(CHAT_COMPLETIONS_SUFFIX)
            && self
                .tool_choice_prefix
                .as_deref()
                .is_some_and(|p| path.starts_with(p));
        let parsed = (rewrite_tools || self.limits.by_model())
            .then(|| serde_json::from_slice::<Value>(&body).ok())
            .flatten();
        let Some(mut json) = parsed else {
            return (body, None);
        };
        let body = if rewrite_tools && insert_tool_choice(&mut json) {
            json.to_string().into()
        } else {
            body
        };
        let model = json.get("model").and_then(Value::as_str).map(str::to_owned);
        (body, model)
    }
}

/// The upstream URL for `rest`, or `None` if it resolves outside the route's upstream.
/// `url` collapses `..` (also as `%2e%2e`), which would let a client climb out of it.
fn upstream_url(route: &Route, rest: &str, query: Option<&str>) -> Option<Url> {
    let query = query.map(|q| format!("?{q}")).unwrap_or_default();
    Url::parse(&format!("{}{rest}{query}", route.base))
        .ok()
        .filter(|u| u.as_str().starts_with(&route.base))
}

/// The client's headers minus credentials, client identity and hop-by-hop headers, then the
/// route's own headers applied on top.
fn request_headers(parts: &Parts, route: &Route) -> HeaderMap {
    let mut headers = HeaderMap::new();
    for (name, value) in &parts.headers {
        let n = name.as_str();
        if !hop_by_hop(&parts.headers, name)
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
    headers
}

/// Streams the upstream response back. The permits ride along in the stream, so a concurrency
/// cap lasts until the body ends.
fn relay(
    route: &Route,
    upstream: reqwest::Response,
    queue_wait_ms: u64,
    retries: u32,
    permits: Vec<OwnedSemaphorePermit>,
) -> Response {
    relay_stream(
        route,
        upstream.status(),
        upstream.headers().clone(),
        queue_wait_ms,
        retries,
        upstream.bytes_stream().map(move |chunk| {
            let _held = &permits;
            chunk
        }),
    )
}

fn hop_by_hop(headers: &HeaderMap, name: &HeaderName) -> bool {
    HOP_BY_HOP.contains(&name.as_str())
        || headers
            .get_all(header::CONNECTION)
            .iter()
            .filter_map(|value| value.to_str().ok())
            .flat_map(|value| value.split(','))
            .any(|token| token.trim().eq_ignore_ascii_case(name.as_str()))
}

fn relay_stream<S, E>(
    route: &Route,
    status: StatusCode,
    upstream_headers: HeaderMap,
    queue_wait_ms: u64,
    retries: u32,
    stream: S,
) -> Response
where
    S: futures_util::Stream<Item = Result<Bytes, E>> + Send + 'static,
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let blocked: Vec<_> = upstream_headers
        .keys()
        .filter(|name| hop_by_hop(&upstream_headers, name))
        .cloned()
        .collect();
    let mut headers = upstream_headers;
    for name in blocked {
        headers.remove(name);
    }
    headers.insert(HEADER_ROUTE, route.name_header.clone());
    headers.insert(HEADER_QUEUE_WAIT, HeaderValue::from(queue_wait_ms));
    headers.insert(HEADER_RETRIES, HeaderValue::from(retries));

    let mut response = Response::new(Body::from_stream(stream));
    *response.status_mut() = status;
    *response.headers_mut() = headers;
    response
}

/// `Retry-After` as whole seconds; the HTTP-date form is not understood.
fn retry_after(headers: &HeaderMap) -> Option<Duration> {
    let seconds = headers
        .get(header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()?;
    Some(Duration::from_secs(seconds))
}

fn queue_timeout(max_wait: Duration) -> Response {
    let mut response = error(
        StatusCode::TOO_MANY_REQUESTS,
        "queue_timeout",
        "no capacity became available within max_wait",
    );
    response
        .headers_mut()
        .insert(header::RETRY_AFTER, HeaderValue::from(max_wait.as_secs()));
    response
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
fn millis(elapsed: Duration) -> u64 {
    u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
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
    let in_flight = Counted::new(&proxy.metrics.in_flight);
    let (route, response) = proxy.serve(req).await;
    let status = response.status().as_u16();
    let to_headers = started.elapsed();
    proxy.metrics.record(route, status);
    proxy.metrics.observe_headers(route, to_headers);
    tracing::info!(route, status, %method, path, ms = millis(to_headers), "request");
    let timed = Timed {
        proxy: Arc::clone(&proxy),
        route: route.to_owned(),
        started,
    };
    count_until_body_ends(response, (in_flight, timed))
}

/// A streamed body (an upstream relay) keeps `guard` alive until the stream is dropped, which is
/// when it ends or the client leaves. Bodies of a known size, the proxy's own small replies, are
/// done by now, and are returned untouched so their framing does not change.
fn count_until_body_ends(response: Response, guard: impl Send + 'static) -> Response {
    if response.body().size_hint().exact().is_some() {
        return response;
    }
    response.map(|body| {
        Body::from_stream(body.into_data_stream().map(move |chunk| {
            let _guard = &guard;
            chunk
        }))
    })
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
