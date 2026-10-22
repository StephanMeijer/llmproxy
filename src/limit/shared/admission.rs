//! Ordered, single-charge admission independent of the persistence implementation.

use std::future::Future;
use uuid::Uuid;

use super::SharedDefinition;

const BYTES_PER_TOKEN: usize = 4;

/// Store implementations atomically acquire both budgets and a concurrency lease.
/// The lease must release concurrency on Drop, including cancellation, but never refund rates.
pub trait AdmissionStore: Sync {
    type Lease: Send;
    type Error;

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
        for charge in self.charges {
            leases.push(store.acquire(charge).await?);
        }
        Ok(Admission { _leases: leases })
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
    _leases: Vec<L>,
}
