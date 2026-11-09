//! Free-model pools: schema and offline validation (see `docs/pools.md`). Not served yet.

use std::collections::{BTreeMap, BTreeSet};

use reqwest::header::{HeaderName, HeaderValue};
use serde::{Deserialize, Serialize};

use super::{Config, ConfigError, http_upstream, valid_identifier};

const POOLS_ROUTE: &str = "pools";
/// `OpenRouter`'s own routers pick another model themselves, so a zero price proves nothing.
const OPENROUTER_ROUTER_PREFIX: &str = "openrouter/";

/// An upstream account whose live catalog proves which of its models are free.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolProviderConfig {
    pub name: String,
    pub upstream: String,
    /// Set on every request to this provider, including catalog fetches; "" removes a header.
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    pub catalog: Catalog,
}

/// How a provider's `<upstream>/models` states prices.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Catalog {
    Openrouter,
    Opencode,
}

/// A requests-per-minute budget shared by the routes drawing on one provider account.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QuotaDomainConfig {
    pub name: String,
    pub provider: String,
    pub rpm: u32,
}

/// `/pools/<name>/…` fails over across its routes in ascending priority.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolConfig {
    pub name: String,
    #[serde(default, rename = "route")]
    pub routes: Vec<PoolRouteConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolRouteConfig {
    /// Unique within the pool; the lowest is tried first.
    pub priority: u32,
    pub provider: String,
    pub model: String,
    /// Context window in tokens.
    pub context: u32,
    pub max_output: u32,
    #[serde(default)]
    pub capabilities: Vec<Capability>,
    pub endpoints: Vec<Endpoint>,
    /// A quota domain of the same provider.
    pub quota: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Capability {
    Tools,
    Vision,
    Schema,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Endpoint {
    Chat,
    Responses,
}

const fn err(message: String) -> Result<(), ConfigError> {
    Err(ConfigError(message))
}

/// Names must be unique identifiers; `kind` prefixes the message.
fn check_names<'a>(kind: &str, names: impl Iterator<Item = &'a str>) -> Result<(), ConfigError> {
    let mut seen = BTreeSet::new();
    for name in names {
        if !valid_identifier(name) {
            return err(format!(
                "{kind} {name:?}: name must contain 1–63 ASCII letters, digits, underscores or hyphens"
            ));
        }
        if !seen.insert(name) {
            return err(format!("{kind} {name}: defined twice"));
        }
    }
    Ok(())
}

pub(super) fn validate(config: &Config) -> Result<(), ConfigError> {
    if config.pools.is_empty()
        && config.pool_providers.is_empty()
        && config.quota_domains.is_empty()
    {
        return Ok(());
    }
    if config.routes.iter().any(|route| route.name == POOLS_ROUTE) {
        return err("route pools: prefix /pools is reserved when pools are configured".into());
    }
    check_names(
        "pool_provider",
        config.pool_providers.iter().map(|p| p.name.as_str()),
    )?;
    check_names(
        "quota_domain",
        config.quota_domains.iter().map(|q| q.name.as_str()),
    )?;
    check_names("pool", config.pools.iter().map(|p| p.name.as_str()))?;

    let mut providers = BTreeMap::new();
    for provider in &config.pool_providers {
        let name = &provider.name;
        if http_upstream(&provider.upstream).is_none() {
            return err(format!(
                "pool_provider {name}: upstream must be an http(s) URL without credentials, query or fragment"
            ));
        }
        for (key, value) in &provider.headers {
            if HeaderName::from_bytes(key.as_bytes()).is_err() {
                return err(format!("pool_provider {name}: header {key}: invalid name"));
            }
            if HeaderValue::from_str(value).is_err() {
                return err(format!("pool_provider {name}: header {key}: invalid value"));
            }
        }
        providers.insert(name.as_str(), provider.catalog);
    }

    let mut quotas = BTreeMap::new();
    for quota in &config.quota_domains {
        let name = &quota.name;
        if !providers.contains_key(quota.provider.as_str()) {
            return err(format!(
                "quota_domain {name}: unknown provider {}",
                quota.provider
            ));
        }
        if quota.rpm == 0 {
            return err(format!("quota_domain {name}: rpm must be positive"));
        }
        quotas.insert(name.as_str(), quota.provider.as_str());
    }

    for pool in &config.pools {
        let mut priorities = BTreeSet::new();
        if pool.routes.is_empty() {
            return err(format!("pool {}: needs at least one route", pool.name));
        }
        for route in &pool.routes {
            let at = format!("pool {} route {}", pool.name, route.priority);
            if !priorities.insert(route.priority) {
                return err(format!("{at}: priority used twice"));
            }
            let Some(&catalog) = providers.get(route.provider.as_str()) else {
                return err(format!("{at}: unknown provider {}", route.provider));
            };
            match quotas.get(route.quota.as_str()) {
                None => return err(format!("{at}: unknown quota_domain {}", route.quota)),
                Some(&owner) if owner != route.provider => {
                    return err(format!(
                        "{at}: quota_domain {} belongs to provider {owner}",
                        route.quota
                    ));
                }
                Some(_) => {}
            }
            if route.model.trim().is_empty() {
                return err(format!("{at}: model must not be empty"));
            }
            if catalog == Catalog::Openrouter && route.model.starts_with(OPENROUTER_ROUTER_PREFIX) {
                return err(format!(
                    "{at}: {} is an OpenRouter router that picks its own model; name the model instead",
                    route.model
                ));
            }
            if route.context == 0 || route.max_output == 0 {
                return err(format!("{at}: context and max_output must be positive"));
            }
            if route.endpoints.is_empty() {
                return err(format!("{at}: endpoints must not be empty"));
            }
        }
    }
    Ok(())
}
