//! Free-model pools: one endpoint that fails over across models proven free by their catalog.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, PoisonError, Weak},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use axum::{
    http::{Method, StatusCode, header},
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use tokio::time::sleep;

use super::{LABEL_OTHER, Proxy, Route, not_found, request_headers};
use crate::config::{Catalog, Config, ConfigError, Endpoint, RouteConfig};

mod proof;

use proof::ModelProof;

const POOLS_PREFIX: &str = "/pools/";
const REFRESH_EVERY: Duration = Duration::from_mins(10);
/// Spreads replicas so they do not fetch in step.
const MAX_JITTER_MS: u32 = 30_000;
const PROOF_VALID_FOR: Duration = Duration::from_mins(30);
const FETCH_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_CATALOG_BYTES: usize = 8 << 20;
const ELIGIBLE: &str = "eligible";

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

struct PoolRoute {
    priority: u32,
    provider: Arc<Provider>,
    model: String,
    endpoints: Vec<Endpoint>,
}

struct Pool {
    name: String,
    /// The metric label, `pool:<name>`; bounded because names come from config.
    label: String,
    routes: Vec<PoolRoute>,
}

pub struct Pools {
    providers: Vec<Arc<Provider>>,
    pools: Vec<Pool>,
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
                            endpoints: r.endpoints.clone(),
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
            .collect();
        Ok(Self { providers, pools })
    }

    /// Whether `path` is a pool path; only when pools are configured.
    pub fn owns(&self, path: &str) -> bool {
        !self.pools.is_empty() && (path == "/pools" || path.starts_with(POOLS_PREFIX))
    }

    /// Returns the metric label and the response for an authenticated pool path.
    pub fn serve(&self, method: &Method, path: &str) -> (&str, Response) {
        let rest = path.strip_prefix(POOLS_PREFIX).unwrap_or_default();
        let (name, rest) = rest.split_once('/').unwrap_or((rest, ""));
        let Some(pool) = self.pools.iter().find(|p| p.name == name) else {
            return (LABEL_OTHER, not_found());
        };
        // Nothing is sent upstream yet (#28).
        if method != Method::GET || rest != "models" {
            return (&pool.label, not_found());
        }
        let now = Instant::now();
        let routes: Vec<Value> = pool
            .routes
            .iter()
            .map(|r| {
                let proof = r
                    .provider
                    .proof
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                let fetched = proof.as_ref().map(|p| (p.fetched, p.models.get(&r.model)));
                let (status, left) = status(fetched, &r.endpoints, now);
                drop(proof);
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
            &pool.label,
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json")],
                body.to_string(),
            )
                .into_response(),
        )
    }
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
                        *provider
                            .proof
                            .lock()
                            .unwrap_or_else(PoisonError::into_inner) =
                            Some(Proof { fetched, models });
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
            drop(live);
            sleep(REFRESH_EVERY + jitter()).await;
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
    use super::{Duration, Endpoint, Instant, ModelProof, PROOF_VALID_FOR, jitter, status};

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
    fn jitter_stays_within_thirty_seconds() {
        assert!(jitter() <= Duration::from_secs(30));
    }
}
