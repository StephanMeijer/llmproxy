use super::{CredentialRecord, LimitKey, LimitSnapshot, PersonId, Storage, StorageError};
use sqlx::postgres::{PgListener, PgPoolOptions};
use std::time::Duration;

/// Notifications only prompt a fresh authoritative read; payloads are never trusted.
pub struct ChangeListener {
    storage: Storage,
    listener: Option<PgListener>,
}

impl ChangeListener {
    #[must_use]
    pub const fn new(storage: Storage) -> Self {
        Self {
            storage,
            listener: None,
        }
    }

    async fn wake(&mut self, wait: Duration) -> Result<(), StorageError> {
        match self.listener.as_mut() {
            None => self.connect().await,
            Some(listener) => {
                match tokio::time::timeout(wait, listener.try_recv()).await {
                    Ok(Ok(Some(_))) | Err(_) => Ok(()),
                    Ok(Ok(None)) => {
                        // SQLx reports reconnect gaps as None. Re-LISTEN before the full reload.
                        listener
                            .listen_all(["llmproxy_storage", "llmproxy_credentials"])
                            .await?;
                        Ok(())
                    }
                    Ok(Err(error)) => {
                        self.listener = None;
                        Err(error.into())
                    }
                }
            }
        }
    }

    async fn connect(&mut self) -> Result<(), StorageError> {
        let listener = tokio::time::timeout(self.storage.timeout, async {
            let pool = PgPoolOptions::new()
                .max_connections(1)
                .min_connections(0)
                .acquire_timeout(self.storage.timeout)
                .connect_lazy_with((*self.storage.pool.connect_options()).clone());
            let mut listener = PgListener::connect_with(&pool).await?;
            listener
                .listen_all(["llmproxy_storage", "llmproxy_credentials"])
                .await?;
            Ok::<_, sqlx::Error>(listener)
        })
        .await
        .map_err(|_| StorageError::Unavailable)??;
        self.listener = Some(listener);
        Ok(())
    }

    /// Waits for a hint or timeout, then reads the current credential generation.
    /// The first call subscribes and immediately reloads, closing the subscribe race.
    /// # Errors
    /// Returns a sanitized availability/schema error; a later call retries connection.
    pub async fn credential(
        &mut self,
        person: PersonId,
        wait: Duration,
    ) -> Result<Option<CredentialRecord>, StorageError> {
        self.wake(wait).await?;
        let mut tx = self.storage.begin().await?;
        let result = tx.credential(person).await?;
        tx.rollback().await?;
        Ok(result)
    }

    /// Waits for a hint, timeout, or reconnect and reloads the complete limit state.
    /// # Errors
    /// Returns a sanitized availability/schema error; subsequent calls retry.
    pub async fn limit(
        &mut self,
        key: &LimitKey,
        wait: Duration,
    ) -> Result<LimitSnapshot, StorageError> {
        self.wake(wait).await?;
        let mut tx = self.storage.begin().await?;
        let result = tx.lock_limit(key).await?;
        tx.rollback().await?;
        Ok(result)
    }
}
