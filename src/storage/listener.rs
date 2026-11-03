use super::{CredentialRecord, LimitKey, LimitSnapshot, PersonId, Storage, StorageError};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgListener, PgPoolOptions},
};
use std::{sync::OnceLock, time::Duration};
use tokio::{
    sync::{Mutex, watch},
    task::{AbortHandle, JoinHandle},
};

const RETRY_INTERVAL: Duration = Duration::from_millis(250);

pub(super) struct Notifications {
    pool: PgPool,
    task: OnceLock<Running>,
}

struct Running {
    changes: watch::Receiver<()>,
    abort: AbortHandle,
    join: Mutex<Option<JoinHandle<()>>>,
}

impl Notifications {
    pub(super) fn new(options: PgConnectOptions, timeout: Duration) -> Self {
        Self {
            pool: PgPoolOptions::new()
                .max_connections(1)
                .min_connections(0)
                .acquire_timeout(timeout)
                .connect_lazy_with(options),
            task: OnceLock::new(),
        }
    }

    fn subscribe(&self) -> watch::Receiver<()> {
        self.task
            .get_or_init(|| {
                let (sender, changes) = watch::channel(());
                let pool = self.pool.clone();
                let join = tokio::spawn(async move {
                    while !pool.is_closed() {
                        if receive(&pool, &sender).await.is_err() {
                            sender.send_replace(());
                            tokio::select! {
                                () = tokio::time::sleep(RETRY_INTERVAL) => {},
                                () = pool.close_event() => break,
                            }
                        }
                    }
                });
                Running {
                    changes,
                    abort: join.abort_handle(),
                    join: Mutex::new(Some(join)),
                }
            })
            .changes
            .clone()
    }

    pub(super) async fn close(&self) {
        if let Some(task) = self.task.get() {
            task.abort.abort();
            if let Some(join) = task.join.lock().await.take()
                && let Err(error) = join.await
                && !error.is_cancelled()
            {
                tracing::error!(%error, "storage listener task failed");
            }
        }
        self.pool.close().await;
    }
}

async fn receive(pool: &PgPool, sender: &watch::Sender<()>) -> Result<(), sqlx::Error> {
    let mut listener = PgListener::connect_with(pool).await?;
    tokio::time::timeout(
        pool.options().get_acquire_timeout(),
        listener.listen_all(["llmproxy_storage", "llmproxy_credentials"]),
    )
    .await
    .map_err(|_| sqlx::Error::PoolTimedOut)??;
    sender.send_replace(());
    loop {
        // None reports a reconnect gap and must wake readers just like a notification.
        listener.try_recv().await?;
        sender.send_replace(());
    }
}

impl Drop for Notifications {
    fn drop(&mut self) {
        if let Some(task) = self.task.get() {
            task.abort.abort();
        }
    }
}

/// Notifications only prompt a fresh authoritative read; payloads are never trusted.
pub struct ChangeListener {
    storage: Storage,
    listener: Option<watch::Receiver<()>>,
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
        let listener = self
            .listener
            .get_or_insert_with(|| self.storage.notifications.subscribe());
        match tokio::time::timeout(wait, listener.changed()).await {
            Ok(Ok(())) | Err(_) => Ok(()),
            Ok(Err(_)) => Err(StorageError::Unavailable),
        }
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
