//! Ordered, single-charge admission independent of the persistence implementation.

use std::future::Future;
use std::time::Duration;
use tokio::time::Instant;
use uuid::Uuid;

use super::SharedDefinition;

#[path = "rate_runtime.rs"]
mod rate_runtime;
pub use rate_runtime::{SharedAdmissionError, SharedRateStore};

const BYTES_PER_TOKEN: usize = 4;

/// Store implementations atomically acquire both budgets and a concurrency lease.
/// The lease must release concurrency on Drop, including cancellation, but never refund rates.
pub trait AdmissionStore: Sync {
    type Lease: Send + Sync;
    type Error;

    fn lease_lost(&self, _leases: &[Self::Lease]) -> impl Future<Output = Self::Error> + Send {
        std::future::pending()
    }

    fn max_wait(&self) -> Option<Duration> {
        None
    }

    fn acquire_with_remaining(
        &self,
        charge: SharedCharge<'_>,
        _remaining: Option<Duration>,
    ) -> impl Future<Output = Result<Self::Lease, Self::Error>> + Send {
        self.acquire(charge)
    }

    fn acquire(
        &self,
        charge: SharedCharge<'_>,
    ) -> impl Future<Output = Result<Self::Lease, Self::Error>> + Send;
}

#[derive(Clone, Copy)]
pub struct SharedCharge<'a> {
    pub definition: &'a SharedDefinition,
    pub person: Uuid,
    pub tokens: usize,
}

/// A single logical request; consuming it acquires each selected definition once.
pub struct PendingAdmission<'a> {
    charges: Vec<SharedCharge<'a>>,
}

impl<'a> PendingAdmission<'a> {
    #[must_use]
    pub fn new(definitions: &'a [SharedDefinition], request: SharedRequest<'_>) -> Self {
        let charges = definitions
            .iter()
            .filter(|definition| definition.matches(request.path, request.model))
            .map(|definition| SharedCharge {
                definition,
                person: request.person,
                tokens: request.body_bytes / BYTES_PER_TOKEN,
            })
            .collect();
        Self { charges }
    }

    /// Acquire in configuration order; a later error drops earlier concurrency leases.
    /// Earlier committed rate charges remain spent. Hold the result across all upstream retries.
    /// # Errors
    /// Returns the first store failure, including timeout or configuration conflict.
    pub async fn acquire<S: AdmissionStore>(
        self,
        store: &S,
    ) -> Result<Admission<S::Lease>, S::Error> {
        let mut leases = Vec::with_capacity(self.charges.len());
        let started = Instant::now();
        for charge in self.charges {
            let remaining = store
                .max_wait()
                .map(|wait| wait.saturating_sub(started.elapsed()));
            let lease = tokio::select! { biased;
                error = store.lease_lost(&leases) => return Err(error),
                lease = store.acquire_with_remaining(charge, remaining) => lease?,
            };
            leases.push(lease);
        }
        Ok(Admission { leases })
    }
}

#[derive(Clone, Copy)]
pub struct SharedRequest<'a> {
    pub person: Uuid,
    pub path: &'a str,
    pub model: Option<&'a str>,
    pub body_bytes: usize,
}

/// Keep alive through the response stream and every retry; no recharging API exists.
#[must_use]
pub struct Admission<L> {
    leases: Vec<L>,
}

impl<L> Admission<L> {
    #[must_use]
    pub fn leases(&self) -> &[L] {
        &self.leases
    }
}
