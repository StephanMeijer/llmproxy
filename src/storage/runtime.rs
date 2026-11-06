use super::{StorageError, check_connection_schema};
use crate::{config::PostgresConfig, metrics::StorageMetrics};
use sqlx::{
    ConnectOptions, PgPool, Postgres, Transaction,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::{str::FromStr, sync::Arc};

/// Lazy namespace-scoped database runtime. Construction never contacts `PostgreSQL`.
#[derive(Clone)]
pub struct Storage {
    metrics: Arc<StorageMetrics>,
    pub(super) pool: PgPool,
    pub(super) namespace: String,
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
            // Session settings bound every statement, including the schema check `begin()` runs first.
            .connect_lazy_with(options.clone().options([
                ("statement_timeout", config.acquire_timeout.as_millis()),
                ("lock_timeout", config.acquire_timeout.as_millis()),
            ]));
        Ok(Self {
            notifications: Arc::new(super::listener::Notifications::new(
                options,
                config.acquire_timeout.min(config.connect_timeout),
            )),
            metrics: Arc::default(),
            pool,
            namespace: config.namespace.clone(),
        })
    }

    /// The metrics this runtime reports database availability to; share it with the renderer.
    #[must_use]
    pub const fn metrics(&self) -> &Arc<StorageMetrics> {
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
        check_connection_schema(&mut tx).await?;
        Ok(StorageTransaction {
            tx,
            namespace: self.namespace.clone(),
            metrics: Arc::clone(&self.metrics),
            locked: None,
            mixed: false,
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
    metrics: Arc<StorageMetrics>,
    /// The limit definition row this transaction already holds `FOR UPDATE`; row locks last until
    /// commit or rollback, so locking it again would only cost a round trip.
    pub(super) locked: Option<super::LimitKey>,
    /// Set once a second limit was locked, so commit cannot name a single key.
    pub(super) mixed: bool,
}

impl StorageTransaction {
    /// Commits changes and delivers a wakeup hint after commit, scoped to the one limit it locked.
    /// An empty payload wakes every waiter; payloads never carry state.
    /// # Errors
    /// Returns a sanitized database failure. The caller must reread after ambiguity.
    pub async fn commit(mut self) -> Result<(), StorageError> {
        let topic = match &self.locked {
            Some(key) if !self.mixed => super::listener::limit_topic(&self.namespace, key),
            _ => String::new(),
        };
        // pg_notify rejects payloads of 8000 bytes or more; an empty one wakes everyone instead.
        let topic = if topic.len() < 8000 {
            topic
        } else {
            String::new()
        };
        let result = async {
            sqlx::query("SELECT pg_notify('llmproxy_storage', $1)")
                .bind(topic)
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
    metrics: &StorageMetrics,
    result: Result<T, StorageError>,
) -> Result<T, StorageError> {
    match &result {
        Ok(_) => metrics.set_postgres_available(true),
        Err(error) if error.is_outage() => metrics.set_postgres_available(false),
        Err(_) => {}
    }
    result
}
