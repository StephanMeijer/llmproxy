use super::{
    API_KEY_HEADER, Body, Config, ConfigError, Counted, HeaderValue, Instant, Limits, Method,
    Outgoing, Parts, Proxy, Response, Route, StatusCode, error, millis, queue_timeout,
    relay_stream, request_headers, to_bytes, upstream_url,
};
use crate::{
    auth::Principal,
    config::CredentialEncryptionConfig,
    limit::shared::{
        CompiledLimits, LeaseCancellations, PendingAdmission, SharedAdmissionError,
        SharedDefinition, SharedRateStore, SharedRequest,
    },
    metrics::{CredentialMetrics, CredentialOperation, CredentialOutcome, CredentialProvider},
    secret::{self, KeyRing},
    storage::{CredentialVersion, Credentials, Storage, StorageError},
};
use std::sync::OnceLock;

pub(super) const MESSAGES_PATH: &str = "/claude/v1/messages";

pub(super) struct Claude {
    route: Route,
    definitions: Vec<SharedDefinition>,
    storage: Storage,
    encryption: CredentialEncryptionConfig,
    runtime: OnceLock<(Credentials, SharedRateStore)>,
    refresh: tokio::sync::Mutex<()>,
}

impl Claude {
    pub(super) fn configure(
        config: &Config,
        storage: Option<Storage>,
    ) -> Result<(Option<Self>, Limits), ConfigError> {
        let Some(claude) = &config.claude else {
            return Ok((None, Limits::new(&config.limits)?));
        };
        let (postgres, storage) = config
            .postgres
            .as_ref()
            .zip(storage)
            .ok_or_else(|| ConfigError("Claude requires PostgreSQL".into()))?;
        let compiled = CompiledLimits::new(&config.limits, &postgres.namespace)?;
        let route = Route {
            name: "claude".into(),
            name_header: HeaderValue::from_static("claude"),
            base: format!("{}/", claude.upstream.trim_end_matches('/')),
            prefix: "/claude/".into(),
            headers: Vec::new(),
            retry_429: claude.retry_429,
        };
        let encryption = config
            .credential_encryption
            .as_ref()
            .ok_or_else(|| ConfigError("Claude requires credential encryption".into()))?;
        Ok((
            Some(Self {
                route,
                definitions: compiled.shared,
                storage,
                encryption: encryption.clone(),
                runtime: OnceLock::new(),
                refresh: tokio::sync::Mutex::new(()),
            }),
            compiled.local,
        ))
    }

    /// Key files are read on first use and retried on every request until they load,
    /// so a mount that is late or briefly unreadable does not require a restart.
    fn runtime(&self, max_wait: std::time::Duration) -> Option<&(Credentials, SharedRateStore)> {
        if let Some(runtime) = self.runtime.get() {
            return Some(runtime);
        }
        let loaded = self.load_runtime(max_wait)?;
        Some(self.runtime.get_or_init(|| loaded))
    }

    fn load_runtime(
        &self,
        max_wait: std::time::Duration,
    ) -> Option<(Credentials, SharedRateStore)> {
        let encryption = &self.encryption;
        let keys = KeyRing::load(&encryption.active_key_id, &encryption.keys).ok()?;
        let fingerprint = secret::key_file(&encryption.fingerprint_key_file).ok()?;
        let store = SharedRateStore::new(self.storage.clone(), max_wait);
        Some((
            Credentials::new(self.storage.clone(), keys, fingerprint),
            store,
        ))
    }

    pub(super) async fn forward(&self, proxy: &Proxy, parts: &Parts, body: Body) -> Response {
        let metrics = proxy.metrics.credentials();
        let Ok(body) = to_bytes(body, proxy.max_body).await else {
            return error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "body_too_large",
                "request body exceeds max_body or could not be read",
            );
        };
        let Some(Principal::Person(person)) = parts.extensions.get::<Principal>() else {
            return unavailable();
        };
        let Some((credentials, store)) = self.runtime(proxy.max_wait) else {
            return unavailable();
        };
        let snapshot = credentials.snapshot(*person).await;
        self.refresh_counts(credentials, metrics).await;
        let Ok(snapshot) = snapshot else {
            return unavailable();
        };
        let Ok(mut key) = HeaderValue::from_bytes(snapshot.secret.expose()) else {
            return unavailable();
        };
        key.set_sensitive(true);
        let Some(url) = upstream_url(&self.route, "v1/messages", parts.uri.query()) else {
            return error(
                StatusCode::BAD_REQUEST,
                "bad_request",
                "invalid upstream path",
            );
        };
        let model = request_model(&body);
        let queued = Instant::now();
        let waiting = Counted::new(&proxy.metrics.held);
        let admission = PendingAdmission::new(
            &self.definitions,
            SharedRequest {
                person: person.0,
                path: MESSAGES_PATH,
                model: model.as_deref(),
                body_bytes: body.len(),
            },
        )
        .acquire(store)
        .await;
        drop(waiting);
        let admission = match admission {
            Ok(admission) => admission,
            Err(SharedAdmissionError::QueueTimeout(_)) => return queue_timeout(proxy.max_wait),
            Err(SharedAdmissionError::Storage(storage_error)) => {
                self.refresh_counts(credentials, metrics).await;
                return admission_unavailable(storage_error, metrics);
            }
        };
        let queue_wait_ms = millis(queued.elapsed());
        let mut headers = request_headers(parts, &self.route);
        headers.insert(API_KEY_HEADER, key);
        let outgoing = Outgoing {
            method: Method::POST,
            url,
            headers,
            body,
        };
        let mut cancellation = LeaseCancellations::new(admission.leases());
        let result = cancellation
            .run(proxy.send_upstream(&self.route, &outgoing, queued + proxy.max_wait))
            .await;
        let (upstream, retries) = match result {
            Ok(Ok(response)) => response,
            Ok(Err(e)) => {
                // Debug keeps the source chain (refused, DNS); without_url drops the client's query string.
                tracing::warn!(route = "claude", error = ?e.without_url(), "upstream request failed");
                return error(
                    StatusCode::BAD_GATEWAY,
                    "upstream_error",
                    "upstream request failed",
                );
            }
            Err(_) => return lease_lost(),
        };
        if upstream.status() == StatusCode::UNAUTHORIZED {
            let result = cancellation
                .run(invalidate(credentials, snapshot.version, metrics))
                .await;
            self.refresh_counts(credentials, metrics).await;
            if result.is_err() {
                return unavailable();
            }
        }
        relay_stream(
            &self.route,
            upstream.status(),
            upstream.headers().clone(),
            queue_wait_ms,
            retries,
            admission.stream(upstream.bytes_stream()),
        )
    }

    async fn refresh_counts(&self, credentials: &Credentials, metrics: &CredentialMetrics) {
        let _refresh = self.refresh.lock().await;
        if let Ok(counts) = credentials.counts().await {
            metrics.set_credentials(CredentialProvider::Anthropic, counts);
        }
    }
}

fn request_model(body: &[u8]) -> Option<String> {
    serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .and_then(|value| {
            value
                .get("model")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned)
        })
}

async fn invalidate(
    credentials: &Credentials,
    version: CredentialVersion,
    metrics: &CredentialMetrics,
) {
    let outcome = match credentials.invalidate(version).await {
        Ok(()) => CredentialOutcome::Success,
        Err(StorageError::Conflict) => CredentialOutcome::Conflict,
        Err(_) => {
            tracing::warn!("Claude credential invalidation unavailable");
            CredentialOutcome::Unavailable
        }
    };
    metrics.record_operation(
        CredentialProvider::Anthropic,
        CredentialOperation::Invalidate,
        outcome,
    );
}

fn lease_lost() -> Response {
    tracing::warn!(
        route = "claude",
        "concurrency lease lost during upstream request"
    );
    unavailable()
}

fn unavailable() -> Response {
    error(
        StatusCode::SERVICE_UNAVAILABLE,
        "adapter_unavailable",
        "Claude adapter unavailable",
    )
}

fn admission_unavailable(storage_error: StorageError, metrics: &CredentialMetrics) -> Response {
    if storage_error == StorageError::Conflict {
        metrics.record_shared_limit_conflict();
    }
    unavailable()
}
