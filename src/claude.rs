use super::{
    API_KEY_HEADER, Body, Config, ConfigError, Counted, HeaderValue, Instant, Limits, Method,
    Outgoing, Parts, Proxy, Response, Route, StatusCode, error, millis, queue_timeout,
    relay_stream, request_headers, to_bytes, upstream_url,
};
use crate::{
    auth::Principal,
    config::{CredentialEncryptionConfig, PostgresConfig},
    limit::shared::{
        CompiledLimits, LeaseCancellations, PendingAdmission, SharedAdmissionError,
        SharedDefinition, SharedRateStore, SharedRequest,
    },
    secret::{KeyRing, SecretBytes},
    storage::{Credentials, Storage, StorageError},
};
use std::io::Read;
use std::sync::OnceLock;

pub(super) const MESSAGES_PATH: &str = "/claude/v1/messages";
const KEY_BYTES: u64 = 32;

pub(super) struct Claude {
    route: Route,
    definitions: Vec<SharedDefinition>,
    postgres: PostgresConfig,
    encryption: CredentialEncryptionConfig,
    runtime: OnceLock<Result<(Credentials, SharedRateStore), StorageError>>,
}

impl Claude {
    pub(super) fn configure(config: &Config) -> Result<(Option<Self>, Limits), ConfigError> {
        let Some(claude) = &config.claude else {
            return Ok((None, Limits::new(&config.limits)?));
        };
        let postgres = config
            .postgres
            .as_ref()
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
                postgres: PostgresConfig {
                    url: postgres.url.clone(),
                    namespace: postgres.namespace.clone(),
                    max_connections: postgres.max_connections,
                    connect_timeout: postgres.connect_timeout,
                    acquire_timeout: postgres.acquire_timeout,
                },
                encryption: CredentialEncryptionConfig {
                    active_key_id: encryption.active_key_id.clone(),
                    fingerprint_key_file: encryption.fingerprint_key_file.clone(),
                    keys: encryption.keys.clone(),
                },
                runtime: OnceLock::new(),
            }),
            compiled.local,
        ))
    }

    fn runtime(
        &self,
        max_wait: std::time::Duration,
    ) -> &Result<(Credentials, SharedRateStore), StorageError> {
        self.runtime.get_or_init(|| {
            let encryption = &self.encryption;
            let storage = Storage::new(&self.postgres)?;
            let keys = KeyRing::load(&encryption.active_key_id, &encryption.keys)
                .map_err(|_| StorageError::Unavailable)?;
            let mut fingerprint = Vec::new();
            std::fs::File::open(&encryption.fingerprint_key_file)
                .map_err(|_| StorageError::Unavailable)?
                .take(KEY_BYTES + 1)
                .read_to_end(&mut fingerprint)
                .map_err(|_| StorageError::Unavailable)?;
            let fingerprint = SecretBytes::new(fingerprint);
            if fingerprint.expose().len()
                != usize::try_from(KEY_BYTES).map_err(|_| StorageError::Unavailable)?
            {
                return Err(StorageError::Unavailable);
            }
            let store = SharedRateStore::new(storage.clone(), max_wait);
            Ok((Credentials::new(storage, keys, fingerprint), store))
        })
    }

    pub(super) async fn forward(&self, proxy: &Proxy, parts: &Parts, body: Body) -> Response {
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
        let Ok((credentials, store)) = self.runtime(proxy.max_wait) else {
            return unavailable();
        };
        let Ok(snapshot) = credentials.snapshot(*person).await else {
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
        let model = serde_json::from_slice::<serde_json::Value>(&body)
            .ok()
            .and_then(|value| {
                value
                    .get("model")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
            });
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
            Err(SharedAdmissionError::Storage(_)) => return unavailable(),
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
            Ok(Err(_)) => {
                return error(
                    StatusCode::BAD_GATEWAY,
                    "upstream_error",
                    "upstream request failed",
                );
            }
            Err(_) => return unavailable(),
        };
        if upstream.status() == StatusCode::UNAUTHORIZED {
            match cancellation
                .run(credentials.invalidate(snapshot.version))
                .await
            {
                Ok(Ok(()) | Err(StorageError::Conflict)) => {}
                Ok(Err(_)) => tracing::warn!("Claude credential invalidation unavailable"),
                Err(_) => return unavailable(),
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
}

fn unavailable() -> Response {
    error(
        StatusCode::SERVICE_UNAVAILABLE,
        "adapter_unavailable",
        "Claude adapter unavailable",
    )
}
