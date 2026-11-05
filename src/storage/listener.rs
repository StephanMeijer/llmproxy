use super::{LimitKey, LimitSnapshot, Storage, StorageError};
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgListener, PgPoolOptions},
};
use std::{
    sync::{
        Arc, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    sync::{
        Mutex,
        broadcast::{self, error::RecvError},
    },
    task::{AbortHandle, JoinHandle},
};

const RETRY_INTERVAL: Duration = Duration::from_millis(250);
/// Hints buffered per subscriber; a subscriber that falls further behind is woken as lagged.
const BUFFERED_HINTS: usize = 64;

/// A change hint: the limit topic a commit touched, or `None` to wake every subscriber.
type Hint = Option<String>;

const LIMIT_TOPIC: &str = "limit:";

/// The `llmproxy_storage` payload a commit locking `key` publishes; it is only ever compared.
pub(super) fn limit_topic(namespace: &str, key: &LimitKey) -> String {
    format!("{LIMIT_TOPIC}{namespace}/{}/{}", key.person.0, key.name)
}

pub(super) struct Notifications {
    pool: PgPool,
    task: OnceLock<Running>,
}

struct Running {
    changes: broadcast::Receiver<Hint>,
    listening: Arc<AtomicBool>,
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

    /// Returns a receiver and whether LISTEN is already active, so no later hint can be missed.
    fn subscribe(&self) -> (broadcast::Receiver<Hint>, bool) {
        let running = self.task.get_or_init(|| {
            let (sender, changes) = broadcast::channel(BUFFERED_HINTS);
            let listening = Arc::new(AtomicBool::new(false));
            let active = Arc::clone(&listening);
            let pool = self.pool.clone();
            let join = tokio::spawn(async move {
                while !pool.is_closed() {
                    if receive(&pool, &sender, &active).await.is_err() {
                        active.store(false, Ordering::SeqCst);
                        // A reconnect gap may hide any change: wake everyone.
                        sender.send(None).ok();
                        tokio::select! {
                            () = tokio::time::sleep(RETRY_INTERVAL) => {},
                            () = pool.close_event() => break,
                        }
                    }
                }
            });
            Running {
                changes,
                listening,
                abort: join.abort_handle(),
                join: Mutex::new(Some(join)),
            }
        });
        // Subscribe before reading the flag: a LISTEN completing in between still sends its hint.
        let changes = running.changes.resubscribe();
        (changes, running.listening.load(Ordering::SeqCst))
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

async fn receive(
    pool: &PgPool,
    sender: &broadcast::Sender<Hint>,
    listening: &AtomicBool,
) -> Result<(), sqlx::Error> {
    let mut listener = PgListener::connect_with(pool).await?;
    tokio::time::timeout(
        pool.options().get_acquire_timeout(),
        listener.listen_all(["llmproxy_storage", "llmproxy_credentials"]),
    )
    .await
    .map_err(|_| sqlx::Error::PoolTimedOut)??;
    listening.store(true, Ordering::SeqCst);
    sender.send(None).ok();
    loop {
        // None reports a reconnect gap and must wake readers just like a notification.
        let hint = listener.try_recv().await?.and_then(|notification| {
            (notification.channel() == "llmproxy_storage" && !notification.payload().is_empty())
                .then(|| notification.payload().to_owned())
        });
        sender.send(hint).ok();
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
    listener: Option<broadcast::Receiver<Hint>>,
    /// When set, keyed hints for other limits are ignored; unkeyed hints still wake.
    topic: Option<String>,
}

impl ChangeListener {
    #[must_use]
    pub const fn new(storage: Storage) -> Self {
        Self {
            storage,
            listener: None,
            topic: None,
        }
    }

    /// A listener that sleeps through commits scoped to other limits.
    #[must_use]
    pub fn for_limit(storage: Storage, key: &LimitKey) -> Self {
        let topic = Some(limit_topic(&storage.namespace, key));
        Self {
            storage,
            listener: None,
            topic,
        }
    }

    /// Waits for a relevant change hint, reconnect, lag, or timeout without reading any state.
    /// The first call subscribes and returns once LISTEN is active, closing the subscribe race.
    /// # Errors
    /// Returns `Unavailable` when the listener task has stopped.
    pub async fn changed(&mut self, wait: Duration) -> Result<(), StorageError> {
        let listener = if let Some(listener) = &mut self.listener {
            listener
        } else {
            let (listener, listening) = self.storage.notifications.subscribe();
            let listener = self.listener.insert(listener);
            if listening {
                return Ok(());
            }
            listener
        };
        let topic = self.topic.as_deref();
        let relevant = async {
            loop {
                match listener.recv().await {
                    // Only another limit's topic is skipped; any other payload wakes (fail open).
                    Ok(Some(payload))
                        if payload.starts_with(LIMIT_TOPIC)
                            && topic.is_some_and(|topic| topic != payload) => {}
                    Ok(_) | Err(RecvError::Lagged(_)) => return Ok(()),
                    Err(RecvError::Closed) => return Err(StorageError::Unavailable),
                }
            }
        };
        tokio::time::timeout(wait, relevant).await.unwrap_or(Ok(()))
    }

    /// Waits for a hint, timeout, or reconnect and reloads the complete limit state.
    /// # Errors
    /// Returns a sanitized availability/schema error; subsequent calls retry.
    pub async fn limit(
        &mut self,
        key: &LimitKey,
        wait: Duration,
    ) -> Result<LimitSnapshot, StorageError> {
        self.changed(wait).await?;
        let mut tx = self.storage.begin().await?;
        let result = tx.lock_limit(key).await?;
        tx.rollback().await?;
        Ok(result)
    }
}
