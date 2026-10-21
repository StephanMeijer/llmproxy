//! TOML configuration: schema, `${VAR}` expansion and loading.

use std::{
    collections::BTreeMap,
    env, fmt, fs,
    net::SocketAddr,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

use reqwest::Url;
use serde::{Deserialize, Deserializer, de::Error as _};

const DEFAULT_MAX_WAIT: Duration = Duration::from_mins(2);
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const DEFAULT_MAX_BODY: usize = 16 * 1024 * 1024;
const DEFAULT_MAX_CONNECTIONS: u32 = 10;
const MAX_IDENTIFIER_LENGTH: usize = 63;
const DEFAULT_CLAUDE_UPSTREAM: &str = "https://api.anthropic.com";
const CLAUDE_ROUTE: &str = "claude";

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
    pub postgres: Option<PostgresConfig>,
    pub credential_encryption: Option<CredentialEncryptionConfig>,
    pub claude: Option<ClaudeConfig>,
    #[serde(default, rename = "route")]
    pub routes: Vec<RouteConfig>,
    #[serde(default, rename = "limit")]
    pub limits: Vec<LimitConfig>,
}

/// Shared state settings; parsing validates syntax without connecting to `PostgreSQL`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresConfig {
    pub url: String,
    /// Deployment isolation key: 1–63 ASCII letters, digits, underscores or hyphens.
    pub namespace: String,
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    #[serde(default = "default_connect_timeout", deserialize_with = "seconds")]
    pub connect_timeout: Duration,
    #[serde(default = "default_connect_timeout", deserialize_with = "seconds")]
    pub acquire_timeout: Duration,
}

impl fmt::Debug for PostgresConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresConfig")
            .field("url", &"[redacted]")
            .field("namespace", &self.namespace)
            .field("max_connections", &self.max_connections)
            .field("connect_timeout", &self.connect_timeout)
            .field("acquire_timeout", &self.acquire_timeout)
            .finish()
    }
}

/// Mounted master-key paths, never key bytes. Files are only read by runtime key-ring loading.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialEncryptionConfig {
    pub active_key_id: String,
    pub fingerprint_key_file: PathBuf,
    pub keys: BTreeMap<String, PathBuf>,
}

/// Direct Anthropic API-key adapter; its prefix is reserved when this section is present.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaudeConfig {
    #[serde(default = "default_claude_upstream")]
    pub upstream: String,
    #[serde(default = "default_retry_429")]
    pub retry_429: bool,
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

const fn default_max_connections() -> u32 {
    DEFAULT_MAX_CONNECTIONS
}

fn default_claude_upstream() -> String {
    DEFAULT_CLAUDE_UPSTREAM.to_owned()
}

/// Whole seconds written as `"120s"`.
fn seconds<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
    let text = String::deserialize(deserializer)?;
    text.strip_suffix('s')
        .and_then(|n| n.parse().ok())
        .map(Duration::from_secs)
        .ok_or_else(|| D::Error::custom("expected whole seconds like \"120s\""))
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
    /// `${VAR}`, an empty `proxy_token`, or invalid optional adapter/storage settings.
    pub fn parse(text: &str, lookup: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let mut value: toml::Value =
            toml::from_str(text).map_err(|_| ConfigError("parse config: invalid TOML".into()))?;
        expand_value(&mut value, &lookup)?;
        // Serde errors may contain string values, including database passwords.
        let mut config: Self = value.try_into().map_err(|_| {
            ConfigError(
                "invalid config: check field names and types; durations require whole seconds like \"120s\""
                    .into(),
            )
        })?;
        if config.proxy_token.is_empty() {
            return Err(ConfigError("proxy_token must not be empty".into()));
        }
        config.validate_adapters()?;
        Ok(config)
    }

    pub(crate) fn validate_adapters(&mut self) -> Result<(), ConfigError> {
        if let Some(postgres) = &self.postgres {
            postgres.validate()?;
        }
        if let Some(encryption) = &self.credential_encryption {
            encryption.validate()?;
        }
        if let Some(claude) = &mut self.claude {
            if self.postgres.is_none() || self.credential_encryption.is_none() {
                return Err(ConfigError(
                    "claude requires [postgres] and [credential_encryption]".into(),
                ));
            }
            if self.routes.iter().any(|route| route.name == CLAUDE_ROUTE) {
                return Err(ConfigError(
                    "route claude: prefix /claude is reserved when [claude] is enabled".into(),
                ));
            }
            let upstream = Url::parse(&claude.upstream)
                .ok()
                .filter(|url| {
                    matches!(url.scheme(), "http" | "https")
                        && url.host().is_some()
                        && url.username().is_empty()
                        && url.password().is_none()
                        && url.query().is_none()
                        && url.fragment().is_none()
                })
                .ok_or_else(|| {
                    ConfigError(
                        "claude.upstream must be an http(s) URL without credentials, query or fragment"
                            .into(),
                    )
                })?;
            claude.upstream = upstream.to_string();
            if !claude.upstream.ends_with('/') {
                claude.upstream.push('/');
            }
        }
        Ok(())
    }
}

impl PostgresConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if !valid_identifier(&self.namespace) {
            return Err(ConfigError(
                "postgres.namespace must contain 1–63 ASCII letters, digits, underscores or hyphens"
                    .into(),
            ));
        }
        if !Url::parse(&self.url).is_ok_and(|url| {
            matches!(url.scheme(), "postgres" | "postgresql")
                && url.host().is_some()
                && url.fragment().is_none()
        }) {
            return Err(ConfigError(
                "postgres.url must be a postgres:// or postgresql:// URL with a host and no fragment"
                    .into(),
            ));
        }
        if self.max_connections == 0 {
            return Err(ConfigError(
                "postgres.max_connections must be positive".into(),
            ));
        }
        for (field, duration) in [
            ("connect_timeout", self.connect_timeout),
            ("acquire_timeout", self.acquire_timeout),
        ] {
            if duration.is_zero() || Instant::now().checked_add(duration).is_none() {
                return Err(ConfigError(format!(
                    "postgres.{field} must be a positive, representable duration"
                )));
            }
        }
        Ok(())
    }
}

impl CredentialEncryptionConfig {
    fn validate(&self) -> Result<(), ConfigError> {
        if self.fingerprint_key_file.as_os_str().is_empty() {
            return Err(ConfigError(
                "credential_encryption.fingerprint_key_file must not be empty".into(),
            ));
        }
        if !valid_identifier(&self.active_key_id)
            || self.keys.keys().any(|id| !valid_identifier(id))
        {
            return Err(ConfigError(
                "credential_encryption key IDs must contain 1–63 ASCII letters, digits, underscores or hyphens"
                    .into(),
            ));
        }
        if !self.keys.contains_key(&self.active_key_id) {
            return Err(ConfigError(
                "credential_encryption.active_key_id must name an entry in keys".into(),
            ));
        }
        if self.keys.values().any(|path| path.as_os_str().is_empty()) {
            return Err(ConfigError(
                "credential_encryption.keys paths must not be empty".into(),
            ));
        }
        Ok(())
    }
}

fn valid_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_IDENTIFIER_LENGTH
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
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
            .ok_or_else(|| ConfigError("unterminated ${ reference".into()))?;
        let name = &tail[..end];
        out.push_str(
            &lookup(name).ok_or_else(|| ConfigError("environment variable is not set".into()))?,
        );
        rest = &tail[end + 1..];
    }
    out.push_str(rest);
    Ok(out)
}
