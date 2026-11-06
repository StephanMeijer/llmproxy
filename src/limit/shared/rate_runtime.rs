use super::{AdmissionStore, SharedCharge};
use crate::{
    limit::shared::{LeaseGuard, SharedDefinition},
    storage::{
        ChangeListener, FencedLease, Lease, LimitKey, OwnerId, PersonId, RateState, Storage,
        StorageError, StorageTransaction,
    },
};
use std::{
    collections::HashSet,
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::Instant;
use uuid::Uuid;

const POLL_INTERVAL: Duration = Duration::from_millis(250);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SharedAdmissionError {
    QueueTimeout(Duration),
    Storage(StorageError),
}

impl fmt::Display for SharedAdmissionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::QueueTimeout(_) => f.write_str("queue_timeout"),
            Self::Storage(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for SharedAdmissionError {}
impl From<StorageError> for SharedAdmissionError {
    fn from(error: StorageError) -> Self {
        Self::Storage(error)
    }
}

#[derive(Clone)]
pub struct SharedRateStore {
    storage: Storage,
    owner: OwnerId,
    max_wait: Duration,
    /// Keys whose definition this process already stored; only a successful `put_limit` adds one.
    defined: Arc<Mutex<HashSet<LimitKey>>>,
}

impl SharedRateStore {
    #[must_use]
    pub fn new(storage: Storage, max_wait: Duration) -> Self {
        Self {
            storage,
            owner: OwnerId(Uuid::new_v4()),
            max_wait,
            defined: Arc::default(),
        }
    }

    fn defined(&self, key: &LimitKey) -> bool {
        self.defined.lock().is_ok_and(|keys| keys.contains(key))
    }

    fn set_defined(&self, key: &LimitKey, defined: bool) {
        if let Ok(mut keys) = self.defined.lock() {
            if defined {
                keys.insert(key.clone());
            } else {
                keys.remove(key);
            }
        }
    }

    /// Enqueues, storing the definition only on this process's first admission for the key.
    async fn enqueue(
        &self,
        key: &LimitKey,
        definition: &SharedDefinition,
        reservation: &Lease,
        ttl: Duration,
    ) -> Result<(), StorageError> {
        let mut cached = self.defined(key);
        loop {
            let mut tx = self.storage.begin().await?;
            if !cached {
                tx.put_limit(key, definition).await?;
            }
            match tx.enqueue(key, reservation, ttl).await {
                // The row vanished since it was cached: forget it and put it once more.
                Err(StorageError::NotFound) if cached => {
                    self.set_defined(key, false);
                    cached = false;
                    tx.rollback().await?;
                }
                result => {
                    result?;
                    tx.commit().await?;
                    self.set_defined(key, true);
                    return Ok(());
                }
            }
        }
    }
}

struct Queued {
    storage: Storage,
    key: LimitKey,
    reservation: Lease,
    active: bool,
}

impl Drop for Queued {
    fn drop(&mut self) {
        if !self.active {
            return;
        }
        let storage = self.storage.clone();
        let key = self.key.clone();
        let reservation = Lease {
            id: self.reservation.id,
            owner: self.reservation.owner,
        };
        tokio::spawn(async move {
            let result = async {
                let mut tx = storage.begin().await?;
                tx.remove_waiter(&key, &reservation).await?;
                tx.commit().await
            }
            .await;
            if let Err(error) = result {
                tracing::warn!(%error, "shared queue cancellation cleanup failed");
            }
        });
    }
}

impl AdmissionStore for SharedRateStore {
    type Lease = Option<LeaseGuard>;
    type Error = SharedAdmissionError;

    async fn lease_lost(&self, leases: &[Self::Lease]) -> Self::Error {
        super::super::LeaseCancellations::new(leases)
            .cancelled()
            .await;
        SharedAdmissionError::Storage(StorageError::Unavailable)
    }

    fn max_wait(&self) -> Option<Duration> {
        Some(self.max_wait)
    }

    async fn acquire(&self, charge: SharedCharge<'_>) -> Result<Self::Lease, Self::Error> {
        self.acquire_with_remaining(charge, Some(self.max_wait))
            .await
    }

    async fn acquire_with_remaining(
        &self,
        charge: SharedCharge<'_>,
        remaining: Option<Duration>,
    ) -> Result<Self::Lease, Self::Error> {
        let enqueue_started = Instant::now();
        let max_wait = remaining.unwrap_or(self.max_wait);
        let deadline = enqueue_started + max_wait;
        tokio::time::timeout_at(deadline, async {
            let key = LimitKey {
                person: PersonId(charge.person),
                name: charge.definition.name().to_owned(),
            };
            let fenced = FencedLease::new(self.owner);
            let mut queued = Queued {
                storage: self.storage.clone(),
                key,
                reservation: Lease {
                    id: fenced.id,
                    owner: fenced.owner,
                },
                active: true,
            };
            self.enqueue(
                &queued.key,
                charge.definition,
                &queued.reservation,
                max_wait.saturating_sub(enqueue_started.elapsed()),
            )
            .await?;
            let mut listener = ChangeListener::for_limit(self.storage.clone(), &queued.key);
            loop {
                match self.poll_once(&mut queued, charge, fenced).await? {
                    Poll::Admitted(guard) => return Ok(guard),
                    Poll::Expired => {
                        return Err(SharedAdmissionError::QueueTimeout(self.max_wait));
                    }
                    Poll::Retry => {}
                    Poll::Wait => match listener.changed(POLL_INTERVAL).await {
                        Ok(()) => {}
                        Err(StorageError::Unavailable) => tokio::time::sleep(POLL_INTERVAL).await,
                        Err(error) => return Err(error.into()),
                    },
                }
            }
        })
        .await
        .map_err(|_| SharedAdmissionError::QueueTimeout(self.max_wait))?
    }
}

/// One admission attempt; every variant names the transaction state it left behind.
enum Poll {
    /// Rates charged, waiter removed, committed.
    Admitted(Option<LeaseGuard>),
    /// The waiter row expired: removed and committed; the caller reports the queue timeout.
    Expired,
    /// Not admitted; reaps committed or rolled back. The caller waits for a hint first.
    Wait,
    /// Liveness lost between the head check and commit: rolled back, poll again at once.
    Retry,
}

/// Commits only when a reap changed rows; otherwise rolls back without a notification.
async fn settle(tx: StorageTransaction, reaped: bool) -> Result<(), StorageError> {
    if reaped {
        tx.commit().await
    } else {
        tx.rollback().await
    }
}

impl SharedRateStore {
    /// One transaction: reap, check liveness, and admit when this waiter is the head and
    /// every budget and the concurrency cap allow it.
    async fn poll_once(
        &self,
        queued: &mut Queued,
        charge: SharedCharge<'_>,
        fenced: FencedLease,
    ) -> Result<Poll, SharedAdmissionError> {
        let started = Instant::now();
        let mut tx = self.storage.begin().await?;
        let reaped = tx.reclaim(&queued.key).await?;
        if !tx.waiter_live(&queued.key, &queued.reservation).await? {
            tx.remove_waiter(&queued.key, &queued.reservation).await?;
            tx.commit().await?;
            queued.active = false;
            return Ok(Poll::Expired);
        }
        let is_head = tx
            .waiter_head(&queued.key)
            .await?
            .is_some_and(|head| head.id == fenced.id && head.owner == fenced.owner);
        let states = if is_head {
            tx.prepare_rates(&queued.key, charge).await?
        } else {
            None
        };
        let Some(states) = states else {
            settle(tx, reaped).await?;
            return Ok(Poll::Wait);
        };
        let concurrency = match charge.definition.max_inflight() {
            Some(max) => tx.acquire_concurrency(&queued.key, &fenced, max).await?,
            None => true,
        };
        if !concurrency {
            settle(tx, reaped).await?;
            return Ok(Poll::Wait);
        }
        self.admit(tx, queued, charge, fenced, &states, started)
            .await
    }

    /// Spends the prepared budgets and leaves the queue, unless the waiter expired meanwhile.
    async fn admit(
        &self,
        mut tx: StorageTransaction,
        queued: &mut Queued,
        charge: SharedCharge<'_>,
        fenced: FencedLease,
        states: &[RateState],
        started: Instant,
    ) -> Result<Poll, SharedAdmissionError> {
        if !tx.waiter_live(&queued.key, &queued.reservation).await? {
            tx.rollback().await?;
            return Ok(Poll::Retry);
        }
        tx.commit_rates(&queued.key, states).await?;
        tx.remove_waiter(&queued.key, &queued.reservation).await?;
        let guard = charge
            .definition
            .max_inflight()
            .map(|_| LeaseGuard::start(self.storage.clone(), queued.key.clone(), fenced, started));
        tx.commit().await?;
        queued.active = false;
        Ok(Poll::Admitted(guard))
    }
}
