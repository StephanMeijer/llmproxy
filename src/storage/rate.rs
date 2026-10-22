use super::{LimitKey, RateKind, RateState, StorageError, StorageTransaction};
use crate::limit::shared::SharedCharge;
use std::time::Duration;

impl StorageTransaction {
    /// Computes charges under row locks without spending either budget.
    /// Commit only when the FIFO head and concurrency reservation both admit.
    /// # Errors
    /// Returns invalid accounting state or a sanitized database error.
    pub async fn prepare_rates(
        &mut self,
        key: &LimitKey,
        charge: SharedCharge<'_>,
    ) -> Result<Option<Vec<RateState>>, StorageError> {
        let snapshot = self.lock_limit(key).await?;
        let (requests, tokens) = charge
            .definition
            .buckets()
            .map_err(|_| StorageError::InvalidData)?;
        let mut states = Vec::with_capacity(2);
        for (kind, bucket, cost) in [
            (RateKind::Requests, requests, 1),
            (RateKind::Tokens, tokens, charge.tokens),
        ] {
            if let Some(mut bucket) = bucket {
                if let Some(state) = snapshot.rates.iter().find(|state| state.kind == kind) {
                    bucket
                        .restore(state.available_units)
                        .map_err(|_| StorageError::InvalidData)?;
                    let elapsed = u64::try_from(state.elapsed_microseconds)
                        .map_err(|_| StorageError::InvalidData)?;
                    bucket.refill(Duration::from_micros(elapsed));
                }
                if !bucket.wait(cost).is_zero() {
                    return Ok(None);
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
        Ok(Some(states))
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
