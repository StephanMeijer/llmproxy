use super::{StorageError, check_connection_schema};
use crate::{config::PostgresConfig, metrics::CredentialMetrics};
use sqlx::{
    ConnectOptions, PgPool, Postgres, Transaction,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::{str::FromStr, sync::Arc, time::Duration};

/// Lazy namespace-scoped database runtime. Construction never contacts `PostgreSQL`.
#[derive(Clone)]
pub struct Storage {
    metrics: Arc<CredentialMetrics>,
    pub(super) pool: PgPool,
    pub(super) namespace: String,
    pub(super) timeout: Duration,
    pub(super) notifications: Arc<super::listener::Notifications>,
}

impl Storage {
    /// Creates a bounded pool with statement logging disabled.
    /// A shared notification listener lazily uses one additional connection per runtime.
    /// # Errors
    /// Returns `InvalidData` for invalid settings or connection syntax.
    pub fn new(config: &PostgresConfig) -> Result<Self, StorageError> {
        if config.max_connections == 0
            || config.acquire_timeout.is_zero()
            || config.connect_timeout.is_zero()
        {
            return Err(StorageError::InvalidData);
        }
        let options = PgConnectOptions::from_str(&config.url)
            .map_err(|_| StorageError::InvalidData)?
            .disable_statement_logging();
        let pool = PgPoolOptions::new()
            .min_connections(0)
            .max_connections(config.max_connections)
            .acquire_timeout(config.acquire_timeout.min(config.connect_timeout))
            .connect_lazy_with(options.clone());
        Ok(Self {
            notifications: Arc::new(super::listener::Notifications::new(
                options,
                config.acquire_timeout.min(config.connect_timeout),
            )),
            metrics: Arc::default(),
            pool,
            namespace: config.namespace.clone(),
            timeout: config.acquire_timeout,
        })
    }

    /// The metrics this runtime reports database availability to; share it with the renderer.
    #[must_use]
    pub const fn metrics(&self) -> &Arc<CredentialMetrics> {
        &self.metrics
    }

    /// Opens a schema-checked transaction; dropping it rolls back its mutations.
    /// # Errors
    /// Returns a sanitized availability or compatibility error.
    pub async fn begin(&self) -> Result<StorageTransaction, StorageError> {
        observe(&self.metrics, self.open().await)
    }

    async fn open(&self) -> Result<StorageTransaction, StorageError> {
        let mut tx = self.pool.begin().await?;
        let milliseconds = self.timeout.as_millis().to_string();
        sqlx::query("SELECT set_config('statement_timeout', $1, true), set_config('lock_timeout', $1, true)")
            .bind(milliseconds).execute(&mut *tx).await?;
        check_connection_schema(&mut tx).await?;
        Ok(StorageTransaction {
            tx,
            namespace: self.namespace.clone(),
            metrics: Arc::clone(&self.metrics),
        })
    }

    /// Closes this runtime's pool and waits for checked-out operations.
    pub async fn close(&self) {
        self.notifications.close().await;
        self.pool.close().await;
    }
}

/// Concrete transaction-scoped repositories; compose operations before committing.
pub struct StorageTransaction {
    pub(super) tx: Transaction<'static, Postgres>,
    pub(super) namespace: String,
    metrics: Arc<CredentialMetrics>,
}

impl StorageTransaction {
    /// Commits changes and delivers a payload-free wakeup hint after commit.
    /// # Errors
    /// Returns a sanitized database failure. The caller must reread after ambiguity.
    pub async fn commit(mut self) -> Result<(), StorageError> {
        let result = async {
            sqlx::query("SELECT pg_notify('llmproxy_storage', '')")
                .execute(&mut *self.tx)
                .await?;
            self.tx.commit().await.map_err(Into::into)
        }
        .await;
        observe(&self.metrics, result)
    }

    /// Explicitly rolls back an operation without producing a notification.
    /// # Errors
    /// Returns `Unavailable` when the database cannot acknowledge rollback.
    pub async fn rollback(self) -> Result<(), StorageError> {
        let result = self.tx.rollback().await.map_err(Into::into);
        observe(&self.metrics, result)
    }
}

/// A completed round trip proves availability; only an outage disproves it.
fn observe<T>(
    metrics: &CredentialMetrics,
    result: Result<T, StorageError>,
) -> Result<T, StorageError> {
    match &result {
        Ok(_) => metrics.set_postgres_available(true),
        Err(error) if error.is_outage() => metrics.set_postgres_available(false),
        Err(_) => {}
    }
    result
}
