//! What the per-person Claude route adds to the shared forwarding pipeline in `proxy`: the
//! person's own provider key, admission through shared `PostgreSQL` limits, and invalidating a key
//! the provider rejects.

use std::{
    sync::{Arc, OnceLock},
    time::Duration,
};

use axum::http::HeaderValue;
use tokio::sync::mpsc;

use crate::{
    auth::Principal,
    config::{Config, ConfigError, CredentialEncryptionConfig},
    limit::{
        Limits,
        shared::{
            Admission, CompiledLimits, LeaseCancellations, LeaseGuard, PendingAdmission,
            SharedAdmissionError, SharedDefinition, SharedRateStore, SharedRequest,
        },
    },
    metrics::{CredentialOperation, CredentialOutcome, CredentialProvider, StorageMetrics},
    storage::{CredentialVersion, Credentials, Storage, StorageError},
};

pub const MESSAGES_PATH: &str = "/claude/v1/messages";
const COUNTS_RETRY: Duration = Duration::from_millis(250);

/// The adapter cannot serve this request: no usable credential, database or key ring.
pub struct Unavailable;

/// Why shared admission did not admit a request.
pub enum Refused {
    QueueTimeout,
    Unavailable,
}

pub struct Claude {
    definitions: Vec<SharedDefinition>,
    storage: Storage,
    encryption: CredentialEncryptionConfig,
    runtime: OnceLock<(Arc<Credentials>, SharedRateStore)>,
    /// Wakes the single background task that owns credential count refreshes.
    counts: OnceLock<mpsc::Sender<()>>,
}

/// One authenticated person's request, with their credential captured for its whole lifetime.
pub struct Person<'a> {
    claude: &'a Claude,
    id: uuid::Uuid,
    credentials: &'a Arc<Credentials>,
    store: &'a SharedRateStore,
    version: CredentialVersion,
    key: HeaderValue,
}

impl Claude {
    /// Splits the configured limits into shared (Claude) and local (static) ones.
    ///
    /// # Errors
    /// Returns a [`ConfigError`] for a missing database or encryption section, or an invalid limit.
    pub fn configure(
        config: &Config,
        storage: Option<Storage>,
    ) -> Result<(Option<Self>, Limits), ConfigError> {
        if config.claude.is_none() {
            return Ok((None, Limits::new(&config.limits)?));
        }
        let (postgres, storage) = config
            .postgres
            .as_ref()
            .zip(storage)
            .ok_or_else(|| ConfigError("Claude requires PostgreSQL".into()))?;
        let compiled = CompiledLimits::new(&config.limits, &postgres.namespace)?;
        let encryption = config
            .credential_encryption
            .as_ref()
            .ok_or_else(|| ConfigError("Claude requires credential encryption".into()))?;
        Ok((
            Some(Self {
                definitions: compiled.shared,
                storage,
                encryption: encryption.clone(),
                runtime: OnceLock::new(),
                counts: OnceLock::new(),
            }),
            compiled.local,
        ))
    }

    /// Captures the authenticated person's active credential.
    ///
    /// # Errors
    /// [`Unavailable`] when the caller is not a person, or the key ring, database or credential
    /// cannot be used.
    pub async fn person(
        &self,
        principal: Option<&Principal>,
        max_wait: Duration,
    ) -> Result<Person<'_>, Unavailable> {
        let Some(Principal::Person(person)) = principal else {
            return Err(Unavailable);
        };
        let (credentials, store) = self.runtime(max_wait).ok_or(Unavailable)?;
        let snapshot = credentials
            .snapshot(*person)
            .await
            .map_err(|_| Unavailable)?;
        let mut key = HeaderValue::from_bytes(snapshot.secret.expose()).map_err(|_| Unavailable)?;
        key.set_sensitive(true);
        Ok(Person {
            claude: self,
            id: person.0,
            credentials,
            store,
            version: snapshot.version,
            key,
        })
    }

    /// Key files are read on first use and retried on every request until they load,
    /// so a mount that is late or briefly unreadable does not require a restart.
    fn runtime(&self, max_wait: Duration) -> Option<&(Arc<Credentials>, SharedRateStore)> {
        if let Some(runtime) = self.runtime.get() {
            return Some(runtime);
        }
        let credentials = Credentials::open(self.storage.clone(), &self.encryption).ok()?;
        let store = SharedRateStore::new(self.storage.clone(), max_wait);
        Some(self.runtime.get_or_init(|| {
            let credentials = Arc::new(credentials);
            self.counts.get_or_init(|| {
                spawn_counts(
                    Arc::clone(&credentials),
                    Arc::clone(self.storage.metrics()),
                    self.storage.credential_changes(),
                )
            });
            (credentials, store)
        }))
    }

    /// Queues at most one refresh behind the one in flight; extra requests coalesce.
    fn refresh_counts(&self) {
        if let Some(wake) = self.counts.get() {
            // A full channel already holds a pending refresh.
            let _ = wake.try_send(());
        }
    }
}

impl Person<'_> {
    /// The person's provider key, marked sensitive so it is never logged.
    pub fn api_key(&self) -> HeaderValue {
        self.key.clone()
    }

    /// Waits on every shared limit this request matches, in configuration order.
    ///
    /// # Errors
    /// [`Refused::QueueTimeout`] when `max_wait` ran out, [`Refused::Unavailable`] when the
    /// database could not decide (a conflicting shared definition is also counted).
    pub async fn admit(
        &self,
        model: Option<&str>,
        body_bytes: usize,
    ) -> Result<Admission<Option<LeaseGuard>>, Refused> {
        let admission = PendingAdmission::new(
            &self.claude.definitions,
            SharedRequest {
                person: self.id,
                path: MESSAGES_PATH,
                model,
                body_bytes,
            },
        )
        .acquire(self.store)
        .await;
        match admission {
            Ok(admission) => Ok(admission),
            Err(SharedAdmissionError::QueueTimeout(_)) => Err(Refused::QueueTimeout),
            Err(SharedAdmissionError::Storage(storage_error)) => {
                self.claude.refresh_counts();
                if storage_error == StorageError::Conflict {
                    self.claude.storage.metrics().record_shared_limit_conflict();
                }
                Err(Refused::Unavailable)
            }
        }
    }

    /// The provider answered `401`: invalidate exactly this credential generation, unless a
    /// lease is lost first.
    ///
    /// # Errors
    /// [`Unavailable`] when a lease was lost while invalidating.
    pub async fn rejected(&self, cancellation: &mut LeaseCancellations) -> Result<(), Unavailable> {
        let result = cancellation
            .run(invalidate(
                self.credentials,
                self.version,
                self.claude.storage.metrics(),
            ))
            .await;
        self.claude.refresh_counts();
        result.map_err(|_| Unavailable)
    }
}

/// Refreshes credential counts once now, then on every credential change or request,
/// retrying a failed scan every `COUNTS_RETRY` until one succeeds.
/// Ends when the adapter (the only sender) is dropped or storage closes the listener.
fn spawn_counts(
    credentials: Arc<Credentials>,
    metrics: Arc<StorageMetrics>,
    mut changes: tokio::sync::watch::Receiver<()>,
) -> mpsc::Sender<()> {
    let (wake, mut woken) = mpsc::channel(1);
    tokio::spawn(async move {
        loop {
            changes.borrow_and_update();
            let scan = credentials.counts().await;
            let failed = scan.is_err();
            if let Ok(counts) = scan {
                metrics.set_credentials(CredentialProvider::Anthropic, counts);
            }
            tokio::select! {
                result = changes.changed() => if result.is_err() { break },
                request = woken.recv() => if request.is_none() { break },
                // A failed scan retries on its own; the next change may never come.
                () = tokio::time::sleep(COUNTS_RETRY), if failed => {},
            }
        }
    });
    wake
}

async fn invalidate(
    credentials: &Credentials,
    version: CredentialVersion,
    metrics: &StorageMetrics,
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
