//! The whole shared-admission decision for one waiter, in the one order that is correct.

use std::time::Duration;

use super::{FencedLease, Lease, LimitKey, PreparedRates, StorageError, StorageTransaction};
use crate::limit::shared::SharedCharge;

/// What one admission attempt decided. Each variant says how the transaction was finished.
pub enum Admit {
    /// Budgets charged, concurrency reserved when capped, waiter removed, but **not committed**:
    /// the caller starts its lease guard first and then commits, so a commit that fails or is
    /// cancelled, even one that applied, still releases the reserved slot.
    Admitted(StorageTransaction),
    /// The waiter's own row expired; it is removed and the transaction committed.
    Expired,
    /// Another waiter is ahead. Committed only when expired rows were reaped.
    NotHead,
    /// A budget refills enough only after this long. Committed only when expired rows were reaped.
    Budget(Duration),
    /// The concurrency cap is full. Committed only when expired rows were reaped.
    AtCapacity,
    /// The waiter expired after the head check but before the charge; rolled back.
    Retry,
}

impl StorageTransaction {
    /// Decides one admission attempt for `fenced`'s waiter and finishes this transaction.
    ///
    /// The steps run in the only safe order, all under the limit's definition lock (taken by the
    /// first step and kept): reap expired rows, check the waiter is still live, check it heads the
    /// FIFO queue, check every rate budget, reserve concurrency when capped, check liveness once
    /// more (database time keeps moving within the transaction), then charge the budgets and leave
    /// the queue. Callers no longer compose these steps themselves; they only commit an admission.
    ///
    /// # Errors
    /// Returns a sanitized database error; the transaction is then dropped and rolls back.
    pub async fn try_admit(
        mut self,
        key: &LimitKey,
        fenced: &FencedLease,
        charge: SharedCharge<'_>,
    ) -> Result<Admit, StorageError> {
        let waiter = Lease {
            id: fenced.id,
            owner: fenced.owner,
        };
        let reaped = self.reclaim(key).await?;
        if !self.waiter_live(key, &waiter).await? {
            self.remove_waiter(key, &waiter).await?;
            self.commit().await?;
            return Ok(Admit::Expired);
        }
        let is_head = self
            .waiter_head(key)
            .await?
            .is_some_and(|head| head.id == fenced.id && head.owner == fenced.owner);
        if !is_head {
            return self.settle(reaped, Admit::NotHead).await;
        }
        let states = match self.prepare_rates(key, charge).await? {
            PreparedRates::Ready(states) => states,
            PreparedRates::Wait(wait) => return self.settle(reaped, Admit::Budget(wait)).await,
        };
        if let Some(max) = charge.definition.max_inflight()
            && !self.acquire_concurrency(key, fenced, max).await?
        {
            return self.settle(reaped, Admit::AtCapacity).await;
        }
        if !self.waiter_live(key, &waiter).await? {
            self.rollback().await?;
            return Ok(Admit::Retry);
        }
        self.commit_rates(key, &states).await?;
        self.remove_waiter(key, &waiter).await?;
        Ok(Admit::Admitted(self))
    }

    /// Keeps a reap (and its wake-up notification) when one happened; otherwise changes nothing.
    async fn settle(self, reaped: bool, outcome: Admit) -> Result<Admit, StorageError> {
        if reaped {
            self.commit().await?;
        } else {
            self.rollback().await?;
        }
        Ok(outcome)
    }
}
