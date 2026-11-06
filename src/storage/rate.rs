use super::{LimitKey, RateKind, RateState, StorageError, StorageTransaction};
use crate::limit::shared::SharedCharge;
use std::time::Duration;

/// What the FIFO head may do with the rate budgets it just locked.
#[derive(Debug)]
pub enum PreparedRates {
    /// Every bucket affords its cost; persist with `commit_rates`.
    Ready(Vec<RateState>),
    /// Longest wait until every bucket affords its cost; nothing was charged.
    Wait(Duration),
}

impl StorageTransaction {
    /// Computes charges under row locks without spending either budget, or how long the
    /// slowest bucket needs. A building block of [`StorageTransaction::try_admit`].
    /// # Errors
    /// Returns invalid accounting state or a sanitized database error.
    pub async fn prepare_rates(
        &mut self,
        key: &LimitKey,
        charge: SharedCharge<'_>,
    ) -> Result<PreparedRates, StorageError> {
        let rates = self.lock_rates(key).await?;
        let (requests, tokens) = charge
            .definition
            .buckets()
            .map_err(|_| StorageError::InvalidData)?;
        let mut states = Vec::with_capacity(2);
        let mut wait = Duration::ZERO;
        for (kind, bucket, cost) in [
            (RateKind::Requests, requests, 1),
            (RateKind::Tokens, tokens, charge.tokens),
        ] {
            if let Some(mut bucket) = bucket {
                if let Some(state) = rates.iter().find(|state| state.kind == kind) {
                    bucket
                        .restore(state.available_units)
                        .map_err(|_| StorageError::InvalidData)?;
                    let elapsed = u64::try_from(state.elapsed_microseconds)
                        .map_err(|_| StorageError::InvalidData)?;
                    bucket.refill(Duration::from_micros(elapsed));
                }
                let needed = bucket.wait(cost);
                if !needed.is_zero() {
                    wait = wait.max(needed);
                    continue;
                }
                bucket.charge(cost).map_err(|_| StorageError::InvalidData)?;
                states.push(RateState {
                    kind,
                    available_units: i64::try_from(bucket.available_units())
                        .map_err(|_| StorageError::InvalidData)?,
                    elapsed_microseconds: 0,
                });
            }
        }
        Ok(if wait.is_zero() {
            PreparedRates::Ready(states)
        } else {
            PreparedRates::Wait(wait)
        })
    }

    /// Persists prepared budgets in the caller's admission transaction.
    /// # Errors
    /// Returns a sanitized database error.
    pub async fn commit_rates(
        &mut self,
        key: &LimitKey,
        states: &[RateState],
    ) -> Result<(), StorageError> {
        for state in states {
            self.put_rate(key, state).await?;
        }
        Ok(())
    }
}
