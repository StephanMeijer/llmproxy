use super::{StorageError, check_connection_schema};
use crate::config::PostgresConfig;
use sqlx::{
    ConnectOptions, PgPool, Postgres, Transaction,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::{str::FromStr, sync::Arc, time::Duration};

/// Lazy namespace-scoped database runtime. Construction never contacts `PostgreSQL`.
#[derive(Clone)]
pub struct Storage {
    metrics: Option<Arc<crate::metrics::CredentialMetrics>>,
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
            metrics: None,
            pool,
            namespace: config.namespace.clone(),
            timeout: config.acquire_timeout,
        })
    }

    pub(crate) fn with_metrics(mut self, metrics: Arc<crate::metrics::CredentialMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    pub(crate) fn observe_availability(&self, available: bool) {
        if let Some(metrics) = &self.metrics {
            metrics.set_postgres_available(available);
        }
    }

    /// Opens a schema-checked transaction; dropping it rolls back its mutations.
    /// # Errors
    /// Returns a sanitized availability or compatibility error.
    pub async fn begin(&self) -> Result<StorageTransaction, StorageError> {
        let mut tx = self.pool.begin().await?;
        let milliseconds = self.timeout.as_millis().to_string();
        sqlx::query("SELECT set_config('statement_timeout', $1, true), set_config('lock_timeout', $1, true)")
            .bind(milliseconds).execute(&mut *tx).await?;
        check_connection_schema(&mut tx).await?;
        Ok(StorageTransaction {
            tx,
            namespace: self.namespace.clone(),
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
}

impl StorageTransaction {
    /// Commits changes and delivers a payload-free wakeup hint after commit.
    /// # Errors
    /// Returns a sanitized database failure. The caller must reread after ambiguity.
    pub async fn commit(mut self) -> Result<(), StorageError> {
        sqlx::query("SELECT pg_notify('llmproxy_storage', '')")
            .execute(&mut *self.tx)
            .await?;
        self.tx.commit().await.map_err(Into::into)
    }

    /// Explicitly rolls back an operation without producing a notification.
    /// # Errors
    /// Returns `Unavailable` when the database cannot acknowledge rollback.
    pub async fn rollback(self) -> Result<(), StorageError> {
        self.tx.rollback().await.map_err(Into::into)
    }
}
