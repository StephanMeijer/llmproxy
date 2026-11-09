//! Free-model pools: one endpoint that fails over across models proven free by their catalog.

use std::{
    collections::{BTreeMap, HashMap},
    sync::{Arc, Mutex, MutexGuard, PoisonError, Weak},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    body::{Body, to_bytes},
    http::{HeaderName, HeaderValue, StatusCode, header, request::Parts},
    response::{IntoResponse, Response},
};
use futures_util::StreamExt;
use serde_json::{Map, Value, json};
use tokio::time::sleep;

use super::{LABEL_OTHER, Proxy, Route, error, not_found, relay, request_headers, retry_after};
use crate::{
    config::{Catalog, Config, ConfigError, Endpoint, RouteConfig},
    limit::Bucket,
};

mod charge;
mod proof;

use charge::Charge;
use proof::ModelProof;

const POOLS_PREFIX: &str = "/pools/";
const REFRESH_EVERY: Duration = Duration::from_mins(10);
/// Spreads replicas so they do not fetch in step.
const MAX_JITTER_MS: u32 = 30_000;
const PROOF_VALID_FOR: Duration = Duration::from_mins(30);
const FETCH_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_CATALOG_BYTES: usize = 8 << 20;
const ELIGIBLE: &str = "eligible";
const SESSION_HEADER: &str = "x-llmproxy-session";
const MAX_SESSION_LEN: usize = 128;
const MAX_SESSIONS: usize = 10_000;
const SESSION_TTL: Duration = Duration::from_hours(1);
const ROUTE_HEADER: HeaderName = HeaderName::from_static("x-llmproxy-pool-route");
const PRIORITY_HEADER: HeaderName = HeaderName::from_static("x-llmproxy-pool-priority");
/// How long a quota domain rests after a `429` without `Retry-After`.
const QUOTA_COOLDOWN: Duration = Duration::from_mins(1);
/// The longest a provider's `Retry-After` may rest a quota domain.
const MAX_QUOTA_COOLDOWN: Duration = Duration::from_hours(1);
/// How long a route rests after the provider refused its key.
const AUTH_COOLDOWN: Duration = Duration::from_mins(5);
/// Fields a zero price list does not cover, or that would let the body pick the route.
const REFUSED_FIELDS: [&str; 5] = [
    "plugins",
    "web_search_options",
    "provider",
    "models",
    "route",
];
/// A model suffix that adds paid web search.
const ONLINE_SUFFIX: &str = ":online";
/// Tool types the caller runs itself; any other (`openrouter:web_search`, `web_search_preview`,
/// `file_search`, ...) runs at the provider and may be billed.
const CHAT_TOOLS: &[&str] = &["function"];
const RESPONSES_TOOLS: &[&str] = &["function", "custom"];
/// `tool_choice` strings that name no tool.
const TOOL_CHOICE_MODES: [&str; 3] = ["auto", "none", "required"];

const REQUESTS: &str = "llm3_pool_requests_total";
const ATTEMPTS: &str = "llm3_pool_attempts_total";
const AUTH_FAILURES: &str = "llm3_pool_auth_failures_total";
const CHARGED: &str = "llm3_pool_charged_responses_total";
const HELP: [(&str, &str); 4] = [
    (
        REQUESTS,
        "Pool requests by outcome: served, exhausted or rejected.",
    ),
    (ATTEMPTS, "Pool upstream attempts by route and result."),
    (
        AUTH_FAILURES,
        "Pool attempts whose provider refused the key (401 or 403).",
    ),
    (
        CHARGED,
        "Relayed pool responses that reported a cost above zero; alert on any increase.",
    ),
];

/// Pool counters by metric and rendered labels; every label value comes from config.
#[derive(Default)]
pub struct Counters(Mutex<BTreeMap<(&'static str, String), u64>>);

impl Counters {
    fn add(&self, metric: &'static str, labels: &[(&str, &str)], n: u64) {
        let labels = labels
            .iter()
            .map(|(k, v)| format!("{k}=\"{}\"", escape(v)))
            .collect::<Vec<_>>()
            .join(",");
        let mut counts = lock(&self.0);
        let count = counts.entry((metric, labels)).or_default();
        *count = count.saturating_add(n);
        drop(counts);
    }

    pub fn render(&self) -> String {
        let counts = lock(&self.0);
        HELP.iter()
            .map(|(name, help)| {
                let rows = counts
                    .iter()
                    .filter(|((metric, _), _)| metric == name)
                    .map(|((_, labels), n)| format!("{name}{{{labels}}} {n}\n"))
                    .collect::<Vec<_>>()
                    .concat();
                format!("# HELP {name} {help}\n# TYPE {name} counter\n{rows}")
            })
            .collect::<Vec<_>>()
            .concat()
    }
}

/// A Prometheus label value: backslash, double quote and newline escaped.
fn escape(value: &str) -> String {
    value
        .replace('\\', r"\\")
        .replace('"', r#"\""#)
        .replace('\n', r"\n")
}

/// Watches one relayed response for a charge the provider reports.
struct Meter {
    charge: Charge,
    counters: Arc<Counters>,
    pool: String,
    route: String,
}

impl Meter {
    /// Reads a chunk, or what is left at the end of the body when `None`.
    fn read(&mut self, chunk: Option<&[u8]>) {
        let charged = match chunk {
            Some(chunk) => self.charge.feed(chunk),
            None => self.charge.finish(),
        };
        if charged {
            let labels = [("pool", self.pool.as_str()), ("route", &self.route)];
            self.counters.add(CHARGED, &labels, 1);
            tracing::error!(
                pool = self.pool,
                route = self.route,
                "pool response reported a non-zero charge"
            );
        }
    }
}

/// The body ends, or the client leaves; hyper may drop a body of known length without polling
/// it to the end, so the rest is read here.
impl Drop for Meter {
    fn drop(&mut self) {
        self.read(None);
    }
}

/// `(pool index, session)` to `(route index, when it last served)`.
type Sessions = HashMap<(usize, String), (usize, Instant)>;

/// The last catalog that parsed, and when it was fetched.
struct Proof {
    fetched: Instant,
    models: BTreeMap<String, ModelProof>,
}

struct Provider {
    route: Route,
    catalog: Catalog,
    proof: Mutex<Option<Proof>>,
}

/// A quota domain's budget and `429` cooldown, shared by every route that draws on it.
struct Quota {
    bucket: Bucket,
    cooldown: Option<Instant>,
}

struct PoolRoute {
    priority: u32,
    provider: Arc<Provider>,
    model: String,
    /// `<provider>/<model>`: the route header and metric label.
    label: String,
    endpoints: Vec<Endpoint>,
    quota: Arc<Mutex<Quota>>,
    /// Set when the provider refused its key.
    cooldown: Mutex<Option<Instant>>,
}

struct Pool {
    name: String,
    /// The metric label, `pool:<name>`; bounded because names come from config.
    label: String,
    routes: Vec<PoolRoute>,
}

pub struct Pools {
    providers: Vec<Arc<Provider>>,
    configured: Vec<Pool>,
    counters: Arc<Counters>,
    // shortcut: per-process affinity, upgrade when sticky routing isn't available
    sessions: Mutex<Sessions>,
    /// When the refresh task fetches catalogs next; now until it has started.
    next_refresh: Mutex<Instant>,
}

impl Pools {
    /// Expects a validated config, so every route names a known provider.
    pub fn new(config: &Config) -> Result<Self, ConfigError> {
        let providers = config
            .pool_providers
            .iter()
            .map(|p| {
                let route = Route::new(&RouteConfig {
                    name: p.name.clone(),
                    upstream: p.upstream.clone(),
                    headers: p.headers.clone(),
                    retry_429: false,
                })?;
                Ok(Arc::new(Provider {
                    route,
                    catalog: p.catalog,
                    proof: Mutex::new(None),
                }))
            })
            .collect::<Result<Vec<_>, ConfigError>>()?;
        let quotas: BTreeMap<&str, Arc<Mutex<Quota>>> = config
            .quota_domains
            .iter()
            .map(|q| {
                let quota = Quota {
                    bucket: Bucket::new(q.rpm, 0),
                    cooldown: None,
                };
                (q.name.as_str(), Arc::new(Mutex::new(quota)))
            })
            .collect();
        let pools = config
            .pools
            .iter()
            .map(|pool| {
                let mut routes = pool
                    .routes
                    .iter()
                    .filter_map(|r| {
                        let provider = providers.iter().find(|p| p.route.name == r.provider)?;
                        Some(PoolRoute {
                            priority: r.priority,
                            provider: Arc::clone(provider),
                            model: r.model.clone(),
                            label: format!("{}/{}", r.provider, r.model),
                            endpoints: r.endpoints.clone(),
                            quota: Arc::clone(quotas.get(r.quota.as_str())?),
                            cooldown: Mutex::new(None),
                        })
                    })
                    .collect::<Vec<_>>();
                routes.sort_by_key(|r| r.priority);
                Pool {
                    name: pool.name.clone(),
                    label: format!("pool:{}", pool.name),
                    routes,
                }
            })
            .collect::<Vec<Pool>>();
        let counters = Arc::<Counters>::default();
        // Zero from the start, so the first charge or refused key is an increase.
        for pool in &pools {
            for route in &pool.routes {
                let labels = [("pool", pool.name.as_str()), ("route", &route.label)];
                counters.add(CHARGED, &labels, 0);
                counters.add(AUTH_FAILURES, &labels, 0);
            }
        }
        Ok(Self {
            providers,
            configured: pools,
            counters,
            sessions: Mutex::default(),
            next_refresh: Mutex::new(Instant::now()),
        })
    }

    /// The pool counters, rendered by the metrics listener.
    pub const fn counters(&self) -> &Arc<Counters> {
        &self.counters
    }

    fn outcome(&self, pool: &Pool, outcome: &str) {
        let labels = [("pool", pool.name.as_str()), ("outcome", outcome)];
        self.counters.add(REQUESTS, &labels, 1);
    }

    fn attempted(&self, pool: &Pool, route: &PoolRoute, result: &str) {
        let labels = [("pool", pool.name.as_str()), ("route", &route.label)];
        if result == "auth_failure" {
            self.counters.add(AUTH_FAILURES, &labels, 1);
        }
        let labels = [labels[0], labels[1], ("result", result)];
        self.counters.add(ATTEMPTS, &labels, 1);
    }

    /// Whether `path` is a pool path; only when pools are configured.
    pub fn owns(&self, path: &str) -> bool {
        !self.configured.is_empty() && (path == "/pools" || path.starts_with(POOLS_PREFIX))
    }

    /// Returns the metric label and the response for an authenticated pool path.
    pub async fn serve(&self, proxy: &Proxy, parts: &Parts, body: Body) -> (&str, Response) {
        let rest = parts
            .uri
            .path()
            .strip_prefix(POOLS_PREFIX)
            .unwrap_or_default();
        let (name, rest) = rest.split_once('/').unwrap_or((rest, ""));
        let Some((index, pool)) = self
            .configured
            .iter()
            .enumerate()
            .find(|(_, p)| p.name == name)
        else {
            return (LABEL_OTHER, not_found());
        };
        let endpoint = match (parts.method.as_str(), rest) {
            ("GET", "models") => return (&pool.label, models(pool)),
            ("POST", "chat/completions") => Endpoint::Chat,
            ("POST", "responses") => Endpoint::Responses,
            _ => return (&pool.label, not_found()),
        };
        let Ok(body) = to_bytes(body, proxy.max_body).await else {
            self.outcome(pool, "rejected");
            return (
                &pool.label,
                error(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    "body_too_large",
                    "request body exceeds max_body or could not be read",
                ),
            );
        };
        let response = match refused(&body, endpoint) {
            Err(message) => {
                self.outcome(pool, "rejected");
                error(StatusCode::BAD_REQUEST, "bad_request", message)
            }
            Ok(body) => {
                let session = parts
                    .headers
                    .get(SESSION_HEADER)
                    .and_then(|v| v.to_str().ok())
                    .filter(|s| {
                        (1..=MAX_SESSION_LEN).contains(&s.len())
                            && s.bytes().all(|b| b.is_ascii_graphic())
                    })
                    .map(|s| (index, s.to_owned()));
                self.attempt(proxy, pool, endpoint, rest, parts, body, session)
                    .await
            }
        };
        (&pool.label, response)
    }

    /// Tries the candidate routes in order, the session's route first, and relays the first
    /// answer that is not a reason to fail over.
    #[allow(
        clippy::too_many_arguments,
        reason = "one request's parts, passed through once"
    )]
    async fn attempt(
        &self,
        proxy: &Proxy,
        pool: &Pool,
        endpoint: Endpoint,
        rest: &str,
        parts: &Parts,
        mut body: Map<String, Value>,
        session: Option<(usize, String)>,
    ) -> Response {
        let pinned = session
            .as_ref()
            .and_then(|key| pinned(&lock(&self.sessions), key, Instant::now()));
        let order = pinned
            .and_then(|i| Some((i, pool.routes.get(i)?)))
            .into_iter()
            .chain(
                pool.routes
                    .iter()
                    .enumerate()
                    .filter(|(i, _)| Some(*i) != pinned),
            );
        // The soonest any skipped route might take a request again.
        let mut soonest: Option<Duration> = None;
        let mut wait = |d: Duration| soonest = Some(soonest.map_or(d, |s| s.min(d)));
        for (index, route) in order {
            if !route.endpoints.contains(&endpoint) {
                continue;
            }
            if let Err(blocked) = self.admit(route, endpoint) {
                wait(blocked);
                continue;
            }
            let provider = &route.provider.route;
            body.insert("model".to_owned(), Value::String(route.model.clone()));
            let mut headers = request_headers(parts, provider);
            headers.remove(SESSION_HEADER);
            headers.insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            );
            // The charge meter reads plain bytes; reqwest decompresses nothing.
            headers.insert(
                header::ACCEPT_ENCODING,
                HeaderValue::from_static("identity"),
            );
            let sent = proxy
                .client
                .post(format!("{}{rest}", provider.base))
                .headers(headers)
                .body(Value::Object(body.clone()).to_string())
                .send()
                .await;
            let upstream = match sent {
                Ok(upstream) => upstream,
                Err(e) => {
                    self.attempted(pool, route, "failover_error");
                    tracing::warn!(pool = pool.name, provider = provider.name, model = route.model, error = ?e.without_url(), "pool route failed, trying the next");
                    continue;
                }
            };
            let status = upstream.status();
            if status == StatusCode::TOO_MANY_REQUESTS {
                self.attempted(pool, route, "failover_429");
                let (rest_for, until) = quota_rest(retry_after(upstream.headers()), Instant::now());
                lock(&route.quota).cooldown = Some(until);
                wait(rest_for);
            } else if matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN) {
                self.attempted(pool, route, "auth_failure");
                *lock(&route.cooldown) = Some(Instant::now() + AUTH_COOLDOWN);
                wait(AUTH_COOLDOWN);
            } else if !status.is_server_error() {
                self.attempted(pool, route, "ok");
                self.outcome(pool, "served");
                if let Some(key) = session {
                    remember(&mut lock(&self.sessions), key, index, Instant::now());
                }
                return self.relayed(pool, route, upstream);
            }
            if status.is_server_error() {
                self.attempted(pool, route, "failover_5xx");
            }
            tracing::warn!(
                pool = pool.name,
                provider = provider.name,
                model = route.model,
                status = status.as_u16(),
                "pool route failed, trying the next"
            );
        }
        self.outcome(pool, "exhausted");
        let soonest = soonest.unwrap_or_default();
        let secs = (soonest.as_secs() + u64::from(soonest.subsec_nanos() > 0)).max(1);
        let mut response = error(
            StatusCode::SERVICE_UNAVAILABLE,
            "pool_exhausted",
            "no route of this pool can take the request now",
        );
        response
            .headers_mut()
            .insert(header::RETRY_AFTER, HeaderValue::from(secs));
        response
    }

    /// Relays the upstream answer with the route headers, counting any charge it reports.
    fn relayed(&self, pool: &Pool, route: &PoolRoute, upstream: reqwest::Response) -> Response {
        let status = upstream.status();
        let upstream_headers = upstream.headers().clone();
        let sse = upstream_headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.starts_with("text/event-stream"));
        let mut meter = Meter {
            charge: Charge::new(sse),
            counters: Arc::clone(&self.counters),
            pool: pool.name.clone(),
            route: route.label.clone(),
        };
        // The bytes pass unchanged; the meter only reads them.
        let chunks = upstream.bytes_stream().map(move |chunk| {
            if let Ok(bytes) = &chunk {
                meter.read(Some(bytes));
            }
            chunk
        });
        let body = Body::from_stream(chunks);
        let mut response = relay(&route.provider.route, status, upstream_headers, 0, 0, body);
        let headers = response.headers_mut();
        if let Ok(value) = HeaderValue::try_from(route.label.as_str()) {
            headers.insert(ROUTE_HEADER, value);
        }
        headers.insert(PRIORITY_HEADER, HeaderValue::from(route.priority));
        response
    }

    /// Takes one request from the route's quota when it may serve `endpoint` now; otherwise
    /// returns how long until it might.
    fn admit(&self, route: &PoolRoute, endpoint: Endpoint) -> Result<(), Duration> {
        let now = Instant::now();
        if route_status(route, &[endpoint], now).0 != ELIGIBLE {
            return Err(lock(&self.next_refresh).saturating_duration_since(now));
        }
        let until = |at| remaining(at, now);
        let cooling = until(*lock(&route.cooldown));
        if !cooling.is_zero() {
            return Err(cooling);
        }
        let mut quota = lock(&route.quota);
        let blocked = until(quota.cooldown).max(quota.bucket.wait(1.0));
        if !blocked.is_zero() {
            return Err(blocked);
        }
        quota.bucket.take(1.0);
        drop(quota);
        Ok(())
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// How long a quota domain rests after a `429` with `retry_after`, capped, and until when.
fn quota_rest(retry_after: Option<Duration>, now: Instant) -> (Duration, Instant) {
    let rest = retry_after
        .unwrap_or(QUOTA_COOLDOWN)
        .min(MAX_QUOTA_COOLDOWN);
    (rest, now.checked_add(rest).unwrap_or(now))
}

/// Time left until `at` from `now`; zero once it has passed or when unset.
fn remaining(at: Option<Instant>, now: Instant) -> Duration {
    at.map_or(Duration::ZERO, |at| at.saturating_duration_since(now))
}

/// The body as a JSON object, or why it is refused before any upstream call.
fn refused(body: &[u8], endpoint: Endpoint) -> Result<Map<String, Value>, &'static str> {
    let Ok(Value::Object(body)) = serde_json::from_slice(body) else {
        return Err("request body must be a JSON object");
    };
    if REFUSED_FIELDS.iter().any(|f| body.contains_key(*f)) {
        return Err(
            "plugins, web_search_options, provider, models and route are not allowed in a pool request",
        );
    }
    if body
        .get("model")
        .and_then(Value::as_str)
        .is_some_and(|m| m.ends_with(ONLINE_SUFFIX))
    {
        return Err("a model ending in :online is not allowed in a pool request");
    }
    let allowed = match endpoint {
        Endpoint::Chat => CHAT_TOOLS,
        Endpoint::Responses => RESPONSES_TOOLS,
    };
    let caller_run = |tool: &Value| {
        tool.get("type")
            .and_then(Value::as_str)
            .is_some_and(|t| allowed.contains(&t))
    };
    let tools_ok = body
        .get("tools")
        .is_none_or(|t| t.as_array().is_some_and(|t| t.iter().all(caller_run)));
    let choice_ok = body.get("tool_choice").is_none_or(|c| {
        c.as_str()
            .map_or_else(|| caller_run(c), |c| TOOL_CHOICE_MODES.contains(&c))
    });
    if !tools_ok || !choice_ok {
        return Err(
            "only function tools (and custom tools on responses) are allowed in a pool request",
        );
    }
    Ok(body)
}

/// The session's route, while its entry is younger than `SESSION_TTL`.
fn pinned(sessions: &Sessions, key: &(usize, String), now: Instant) -> Option<usize> {
    let (route, at) = sessions.get(key)?;
    (now.saturating_duration_since(*at) < SESSION_TTL).then_some(*route)
}

/// Records the route that served the session. When full, expired entries go first, then the
/// oldest.
fn remember(sessions: &mut Sessions, key: (usize, String), route: usize, now: Instant) {
    if sessions.len() >= MAX_SESSIONS && !sessions.contains_key(&key) {
        sessions.retain(|_, (_, at)| now.saturating_duration_since(*at) < SESSION_TTL);
        let oldest = sessions
            .iter()
            .min_by_key(|(_, (_, at))| *at)
            .map(|(k, _)| k.clone());
        if let Some(oldest) = oldest.filter(|_| sessions.len() >= MAX_SESSIONS) {
            sessions.remove(&oldest);
        }
    }
    sessions.insert(key, (route, now));
}

/// `GET /pools/<pool>/models`: each route's status.
fn models(pool: &Pool) -> Response {
    let now = Instant::now();
    let routes: Vec<Value> = pool
        .routes
        .iter()
        .map(|r| {
            let (status, left) = route_status(r, &r.endpoints, now);
            json!({
                "priority": r.priority,
                "provider": r.provider.route.name,
                "model": r.model,
                "endpoints": r.endpoints,
                "status": status,
                "proof_valid_for_secs": left.map(|d| d.as_secs()),
            })
        })
        .collect();
    let body = json!({ "pool": pool.name, "routes": routes });
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "application/json")],
        body.to_string(),
    )
        .into_response()
}

/// [`status`] of `route` from its provider's last good fetch.
fn route_status(
    route: &PoolRoute,
    endpoints: &[Endpoint],
    now: Instant,
) -> (&'static str, Option<Duration>) {
    status(
        lock(&route.provider.proof)
            .as_ref()
            .map(|p| (p.fetched, p.models.get(&route.model))),
        endpoints,
        now,
    )
}

/// A route's status and how long its proof stays valid, from the provider's last good fetch
/// (`fetched`, with the route model's entry) at `now`.
fn status(
    fetched: Option<(Instant, Option<&ModelProof>)>,
    endpoints: &[Endpoint],
    now: Instant,
) -> (&'static str, Option<Duration>) {
    let Some((at, model)) = fetched else {
        return ("closed: no proof", None);
    };
    let left = PROOF_VALID_FOR.saturating_sub(now.saturating_duration_since(at));
    if left.is_zero() {
        return ("closed: expired", None);
    }
    let status = match model {
        None => "closed: missing from catalog",
        Some(model) if !model.free => "closed: not free",
        Some(ModelProof {
            kinds: Some(kinds), ..
        }) if !endpoints.iter().any(|e| kinds.contains(e)) => "closed: endpoint not in catalog",
        Some(_) => ELIGIBLE,
    };
    (status, Some(left))
}

/// Refreshes every provider's catalog now, then every `REFRESH_EVERY` plus jitter, until the
/// proxy is dropped.
pub fn spawn_refresh(proxy: &Arc<Proxy>) {
    if proxy.pools.providers.is_empty() {
        return;
    }
    let proxy: Weak<Proxy> = Arc::downgrade(proxy);
    tokio::spawn(async move {
        while let Some(live) = proxy.upgrade() {
            for provider in &live.pools.providers {
                match fetch(&live.client, provider).await {
                    Ok(models) => {
                        let fetched = Instant::now();
                        *lock(&provider.proof) = Some(Proof { fetched, models });
                    }
                    Err(reason) => {
                        tracing::warn!(
                            provider = provider.route.name,
                            reason,
                            "pool catalog refresh failed"
                        );
                    }
                }
            }
            let pause = REFRESH_EVERY + jitter();
            *lock(&live.pools.next_refresh) = Instant::now() + pause;
            drop(live);
            sleep(pause).await;
        }
    });
}

/// 0–30 s from the clock's nanoseconds; no random source is needed to spread replicas.
fn jitter() -> Duration {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.subsec_nanos());
    Duration::from_millis(u64::from(nanos % (MAX_JITTER_MS + 1)))
}

/// `GET <upstream>models` with only the provider's headers; the reason never carries the body.
async fn fetch(
    client: &reqwest::Client,
    provider: &Provider,
) -> Result<BTreeMap<String, ModelProof>, &'static str> {
    let (parts, ()) = axum::http::Request::new(()).into_parts();
    let mut response = client
        .get(format!("{}models", provider.route.base))
        .headers(request_headers(&parts, &provider.route))
        .timeout(FETCH_TIMEOUT)
        .send()
        .await
        .map_err(|_| "request failed")?;
    if !response.status().is_success() {
        return Err("non-success status");
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| "body failed")? {
        if body.len() + chunk.len() > MAX_CATALOG_BYTES {
            return Err("catalog exceeds 8 MiB");
        }
        body.extend_from_slice(&chunk);
    }
    proof::parse(provider.catalog, &body).map_err(|e| e.0)
}

#[cfg(test)]
mod tests {
    use super::{
        ATTEMPTS, Counters, Duration, Endpoint, Instant, MAX_QUOTA_COOLDOWN, MAX_SESSIONS,
        ModelProof, PROOF_VALID_FOR, QUOTA_COOLDOWN, SESSION_TTL, Sessions, jitter, pinned,
        quota_rest, remaining, remember, status,
    };

    const CHAT: &[Endpoint] = &[Endpoint::Chat];

    fn at(
        fetched: Instant,
        model: Option<&ModelProof>,
        now: Instant,
    ) -> (&'static str, Option<u64>) {
        let (status, left) = status(Some((fetched, model)), CHAT, now);
        (status, left.map(|d| d.as_secs()))
    }

    #[test]
    fn status_follows_the_last_good_fetch() {
        let fetched = Instant::now();
        let later = fetched + Duration::from_mins(10);
        let free = ModelProof {
            free: true,
            kinds: None,
        };
        let paid = ModelProof {
            free: false,
            kinds: None,
        };
        let responses_only = ModelProof {
            free: true,
            kinds: Some(vec![Endpoint::Responses]),
        };
        let chat_listed = ModelProof {
            free: true,
            kinds: Some(vec![Endpoint::Responses, Endpoint::Chat]),
        };
        assert_eq!(status(None, CHAT, later), ("closed: no proof", None));
        assert_eq!(at(fetched, Some(&free), later), ("eligible", Some(1200)));
        assert_eq!(
            at(fetched, Some(&chat_listed), later),
            ("eligible", Some(1200))
        );
        assert_eq!(
            at(fetched, Some(&free), fetched + PROOF_VALID_FOR),
            ("closed: expired", None)
        );
        assert_eq!(
            at(fetched, Some(&paid), later),
            ("closed: not free", Some(1200))
        );
        assert_eq!(
            at(fetched, None, later),
            ("closed: missing from catalog", Some(1200))
        );
        assert_eq!(
            at(fetched, Some(&responses_only), later),
            ("closed: endpoint not in catalog", Some(1200))
        );
    }

    #[test]
    fn sessions_expire_and_evict_the_oldest_when_full() {
        let start = Instant::now();
        let key = |n: usize| (0, n.to_string());
        let mut sessions = Sessions::new();
        remember(&mut sessions, key(0), 1, start);
        assert_eq!(pinned(&sessions, &key(0), start + SESSION_TTL / 2), Some(1));
        assert_eq!(pinned(&sessions, &key(0), start + SESSION_TTL), None);
        assert_eq!(pinned(&sessions, &key(1), start), None);
        let mut at = start;
        for n in 1..=MAX_SESSIONS {
            at += Duration::from_millis(1);
            remember(&mut sessions, key(n), 0, at);
        }
        // Full: the oldest entry, 0, made room for the newest.
        assert_eq!(sessions.len(), MAX_SESSIONS);
        assert_eq!(pinned(&sessions, &key(0), at), None);
        assert_eq!(pinned(&sessions, &key(1), at), Some(0));
        // A known session updates in place; once all expire, they all make room.
        remember(&mut sessions, key(1), 1, at);
        assert_eq!(sessions.len(), MAX_SESSIONS);
        remember(&mut sessions, key(0), 1, start + SESSION_TTL * 2);
        assert_eq!(sessions.len(), 1);
    }

    #[test]
    fn a_quota_cooldown_follows_retry_after_within_a_cap_and_ends() {
        let now = Instant::now();
        let secs = |s| Some(Duration::from_secs(s));
        assert_eq!(
            quota_rest(secs(30), now),
            (Duration::from_secs(30), now + Duration::from_secs(30))
        );
        assert_eq!(
            quota_rest(None, now),
            (QUOTA_COOLDOWN, now + QUOTA_COOLDOWN)
        );
        // An absurd provider value neither panics nor rests the domain past the cap.
        let (rest, until) = quota_rest(secs(u64::MAX), now);
        assert_eq!(
            (rest, until),
            (MAX_QUOTA_COOLDOWN, now + MAX_QUOTA_COOLDOWN)
        );
        let (_, until) = quota_rest(secs(1), now);
        assert_eq!(remaining(Some(until), now), Duration::from_secs(1));
        assert_eq!(
            remaining(Some(until), now + Duration::from_millis(400)),
            Duration::from_millis(600)
        );
        assert_eq!(
            remaining(Some(until), now + Duration::from_secs(2)),
            Duration::ZERO
        );
        assert_eq!(remaining(None, now), Duration::ZERO);
    }

    #[test]
    fn jitter_stays_within_thirty_seconds() {
        assert!(jitter() <= Duration::from_secs(30));
    }

    #[test]
    fn label_values_cannot_break_the_text_format() {
        let counters = Counters::default();
        counters.add(ATTEMPTS, &[("route", "p/a:free\"\\\n")], 1);
        assert!(
            counters
                .render()
                .contains("llm3_pool_attempts_total{route=\"p/a:free\\\"\\\\\\n\"} 1\n")
        );
    }
}
