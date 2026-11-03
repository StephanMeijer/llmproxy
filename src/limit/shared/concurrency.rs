use std::{fmt, future::Future, time::Duration};
use tokio::{
    sync::{oneshot, watch},
    time::Instant,
};

use crate::storage::{FencedLease, LEASE_LIFETIME, LimitKey, Storage, StorageError};

#[path = "lease_stream.rs"]
mod lease_stream;
pub use lease_stream::LeaseStreamError;

const RENEW_EVERY: Duration = Duration::from_secs(10);
const RETRY_EVERY: Duration = Duration::from_secs(1);
const OPERATION_TIMEOUT: Duration = Duration::from_secs(2);
const EXPIRY_MARGIN: Duration = Duration::from_secs(1);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LeaseLost;

impl fmt::Display for LeaseLost {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("shared concurrency lease lost")
    }
}
impl std::error::Error for LeaseLost {}

#[derive(Clone)]
pub struct LeaseCancellation(watch::Receiver<LeaseStatus>);

#[derive(Clone, Copy)]
enum LeaseStatus {
    Active(Instant),
    Lost,
    Finished,
}

impl LeaseCancellation {
    pub async fn cancelled(&mut self) {
        loop {
            let status = *self.0.borrow_and_update();
            match status {
                LeaseStatus::Lost => return,
                LeaseStatus::Finished => std::future::pending::<()>().await,
                LeaseStatus::Active(deadline) => {
                    tokio::select! { biased;
                        () = tokio::time::sleep_until(deadline) => return,
                        result = self.0.changed() => if result.is_err() { return; },
                    }
                }
            }
        }
    }

    /// Cancels and drops the local upstream future when ownership is lost.
    /// # Errors
    /// Returns `LeaseLost` when continued work cannot be authorized.
    pub async fn run<F: Future>(&mut self, work: F) -> Result<F::Output, LeaseLost> {
        tokio::select! { biased;
            () = self.cancelled() => Err(LeaseLost),
            value = work => Ok(value),
        }
    }
}

/// Cancellation of any lease held by a single admitted request.
#[derive(Clone)]
pub struct LeaseCancellations(Vec<LeaseCancellation>);

impl LeaseCancellations {
    #[must_use]
    pub fn new(leases: &[Option<LeaseGuard>]) -> Self {
        Self(
            leases
                .iter()
                .flatten()
                .map(LeaseGuard::cancellation)
                .collect(),
        )
    }

    pub async fn cancelled(&mut self) {
        use futures_util::{StreamExt, stream::FuturesUnordered};
        let mut waiting: FuturesUnordered<_> = self
            .0
            .iter_mut()
            .map(LeaseCancellation::cancelled)
            .collect();
        if waiting.next().await.is_none() {
            std::future::pending::<()>().await;
        }
    }

    /// # Errors
    /// Returns `LeaseLost` if any owned lease loses authority.
    pub async fn run<F: Future>(&mut self, work: F) -> Result<F::Output, LeaseLost> {
        tokio::select! { biased;
            () = self.cancelled() => Err(LeaseLost),
            output = work => Ok(output),
        }
    }
}

/// Hold through retries and body completion; dropping signals conditional release.
#[must_use]
pub struct LeaseGuard {
    cancellation: LeaseCancellation,
    _finished: oneshot::Sender<()>,
}

impl LeaseGuard {
    /// Starts alongside acquisition. `operation_started` must precede the acquisition
    /// transaction, so network/lock/commit latency consumes the confirmed lifetime.
    pub fn start(
        storage: Storage,
        reservation: (LimitKey, FencedLease),
        operation_started: Instant,
    ) -> Self {
        let (cancel, cancellation) = watch::channel(LeaseStatus::Active(
            operation_started + LEASE_LIFETIME - EXPIRY_MARGIN,
        ));
        let (finished, completion) = oneshot::channel();
        let controller = Controller {
            storage,
            reservation,
            cancel,
            completion,
        };
        // The task is owned by the completion channel: guard drop ends renewal and
        // performs bounded cleanup even when a response is abandoned.
        tokio::spawn(controller.run(operation_started));
        Self {
            cancellation: LeaseCancellation(cancellation),
            _finished: finished,
        }
    }

    #[must_use]
    pub fn cancellation(&self) -> LeaseCancellation {
        self.cancellation.clone()
    }
}

struct Controller {
    storage: Storage,
    reservation: (LimitKey, FencedLease),
    cancel: watch::Sender<LeaseStatus>,
    completion: oneshot::Receiver<()>,
}

impl Controller {
    async fn run(mut self, started: Instant) {
        let mut deadline = started + LEASE_LIFETIME - EXPIRY_MARGIN;
        let mut next = started + RENEW_EVERY;
        loop {
            tokio::select! { biased;
                _ = &mut self.completion => break,
                () = tokio::time::sleep_until(deadline) => {
                    self.cancel.send_replace(LeaseStatus::Lost);
                    // Capacity is retained until the stream owner acknowledges cancellation
                    // by dropping its guard, or the authoritative row expires.
                    match (&mut self.completion).await { Ok(()) | Err(_) => {} }
                    break;
                }
                () = tokio::time::sleep_until(next) => {}
            }
            let renewal_started = Instant::now();
            let attempt_deadline = deadline.min(renewal_started + OPERATION_TIMEOUT);
            let result = tokio::select! { biased;
                _ = &mut self.completion => break,
                result = tokio::time::timeout_at(attempt_deadline, renew(&self.storage, &self.reservation)) => result,
            };
            match result {
                Ok(Ok(true)) => {
                    deadline = renewal_started + LEASE_LIFETIME - EXPIRY_MARGIN;
                    self.cancel.send_replace(LeaseStatus::Active(deadline));
                    next = renewal_started + RENEW_EVERY;
                }
                Ok(Ok(false)) => {
                    self.cancel.send_replace(LeaseStatus::Lost);
                    match (&mut self.completion).await {
                        Ok(()) | Err(_) => {}
                    }
                    break;
                }
                Ok(Err(_)) | Err(_) => next = Instant::now() + RETRY_EVERY,
            }
        }
        if matches!(*self.cancel.borrow(), LeaseStatus::Active(_)) {
            let status = if Instant::now() >= deadline {
                LeaseStatus::Lost
            } else {
                LeaseStatus::Finished
            };
            self.cancel.send_replace(status);
        }
        match tokio::time::timeout(OPERATION_TIMEOUT, self.release()).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) | Err(_) => {
                tracing::warn!("shared concurrency release unconfirmed; awaiting database expiry");
            }
        }
    }

    async fn release(&self) -> Result<(), StorageError> {
        let mut tx = self.storage.begin().await?;
        tx.release_concurrency(&self.reservation.0, &self.reservation.1)
            .await?;
        tx.commit().await
    }
}

async fn renew(
    storage: &Storage,
    reservation: &(LimitKey, FencedLease),
) -> Result<bool, StorageError> {
    let mut tx = storage.begin().await?;
    let renewed = tx.renew_concurrency(&reservation.0, &reservation.1).await?;
    tx.commit().await?;
    Ok(renewed)
}
