//! TOML configuration: schema, `${VAR}` expansion and loading.

use std::{collections::BTreeMap, env, fmt, fs, net::SocketAddr, path::Path, time::Duration};

use serde::{Deserialize, Deserializer, de::Error as _};

const DEFAULT_MAX_WAIT: Duration = Duration::from_mins(2);
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_MAX_BODY: usize = 16 * 1024 * 1024;

/// Anything wrong with the configuration; the message says what and where.
#[derive(Debug)]
pub struct ConfigError(pub String);

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for ConfigError {}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub listen: SocketAddr,
    /// Prometheus listener; no metrics are served when unset.
    pub metrics_listen: Option<SocketAddr>,
    pub proxy_token: String,
    /// How long a request may be held by a limit before the client gets 429.
    #[serde(default = "default_max_wait", deserialize_with = "seconds")]
    pub max_wait: Duration,
    #[serde(default = "default_connect_timeout", deserialize_with = "seconds")]
    pub connect_timeout: Duration,
    #[serde(default = "default_max_body")]
    pub max_body: usize,
    /// POST `<prefix>…/chat/completions` bodies with tools but no `tool_choice` get `"auto"`.
    pub tool_choice_prefix: Option<String>,
    #[serde(default, rename = "route")]
    pub routes: Vec<RouteConfig>,
    #[serde(default, rename = "limit")]
    pub limits: Vec<LimitConfig>,
}

/// `/<name>/<rest>` is forwarded to `<upstream>/<rest>`.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RouteConfig {
    pub name: String,
    pub upstream: String,
    /// Set on every forwarded request; an empty value removes the header.
    #[serde(default)]
    pub headers: BTreeMap<String, String>,
    /// Hold and retry an upstream `429` until `max_wait` runs out. Turn it off where a 429 means the
    /// quota is spent, because retrying then only delays the same answer.
    #[serde(default = "default_retry_429")]
    pub retry_429: bool,
}

/// Matches on `model` (JSON body) and/or `prefixes` (request path); both must hold when both are set.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LimitConfig {
    pub name: String,
    pub model: Option<String>,
    #[serde(default)]
    pub prefixes: Vec<String>,
    /// Sustained requests per minute; the bucket holds `rpm + burst`.
    pub rpm: Option<u32>,
    #[serde(default)]
    pub burst: u32,
    /// Sustained estimated tokens per minute; the bucket holds `tpm + tpm_burst`.
    pub tpm: Option<u32>,
    #[serde(default)]
    pub tpm_burst: u32,
    /// Concurrent requests, held until the response body ends.
    pub max_inflight: Option<usize>,
}

const fn default_max_wait() -> Duration {
    DEFAULT_MAX_WAIT
}

const fn default_connect_timeout() -> Duration {
    DEFAULT_CONNECT_TIMEOUT
}

const fn default_retry_429() -> bool {
    true
}

const fn default_max_body() -> usize {
    DEFAULT_MAX_BODY
}

/// Whole seconds written as `"120s"`.
fn seconds<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
    let text = String::deserialize(deserializer)?;
    text.strip_suffix('s')
        .and_then(|n| n.parse().ok())
        .map(Duration::from_secs)
        .ok_or_else(|| {
            D::Error::custom(format!(
                "expected whole seconds like \"120s\", got {text:?}"
            ))
        })
}

impl Config {
    /// Reads `path`, expanding `${VAR}` from the process environment.
    ///
    /// # Errors
    /// Returns a [`ConfigError`] if the file cannot be read, or for any error of [`Config::parse`].
    pub fn load(path: &Path) -> Result<Self, ConfigError> {
        let text = fs::read_to_string(path)
            .map_err(|e| ConfigError(format!("read {}: {e}", path.display())))?;
        Self::parse(&text, |name| env::var(name).ok())
    }

    /// Parses `text`; `lookup` resolves `${VAR}` references, and `$${` is a literal `${`.
    ///
    /// # Errors
    /// Returns a [`ConfigError`] for invalid TOML, unknown keys, an unresolved or unterminated
    /// `${VAR}`, or an empty `proxy_token`.
    pub fn parse(text: &str, lookup: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let mut value: toml::Value =
            toml::from_str(text).map_err(|e| ConfigError(format!("parse config: {e}")))?;
        expand_value(&mut value, &lookup)?;
        let config: Self = value
            .try_into()
            .map_err(|e| ConfigError(format!("invalid config: {e}")))?;
        if config.proxy_token.is_empty() {
            return Err(ConfigError("proxy_token must not be empty".into()));
        }
        Ok(config)
    }
}

/// Expands only string values, so a secret containing quotes cannot break the TOML.
fn expand_value(
    value: &mut toml::Value,
    lookup: &impl Fn(&str) -> Option<String>,
) -> Result<(), ConfigError> {
    match value {
        toml::Value::String(s) => *s = expand(s, lookup)?,
        toml::Value::Array(items) => items.iter_mut().try_for_each(|v| expand_value(v, lookup))?,
        toml::Value::Table(table) => table
            .iter_mut()
            .try_for_each(|(_, v)| expand_value(v, lookup))?,
        _ => {}
    }
    Ok(())
}

fn expand(template: &str, lookup: &impl Fn(&str) -> Option<String>) -> Result<String, ConfigError> {
    let mut out = String::new();
    let mut rest = template;
    while let Some(start) = rest.find("${") {
        // "$${" is an escape for a literal "${": it looks nothing up and needs no closing brace.
        // shortcut: a "$" directly before a reference cannot be written, so a lone "$" or "$$" keeps
        // working as before; upgrade by making "$$" mean "$" everywhere, which breaks values with "$$".
        if let Some(before) = rest[..start].strip_suffix('$') {
            out.push_str(before);
            out.push_str("${");
            rest = &rest[start + 2..];
            continue;
        }
        out.push_str(&rest[..start]);
        let tail = &rest[start + 2..];
        let end = tail
            .find('}')
            .ok_or_else(|| ConfigError(format!("unterminated ${{ in {template:?}")))?;
        let name = &tail[..end];
        out.push_str(
            &lookup(name)
                .ok_or_else(|| ConfigError(format!("environment variable {name} is not set")))?,
        );
        rest = &tail[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}
