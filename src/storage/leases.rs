//! Database-time concurrency capacity and fenced ownership mutations.

use std::time::Duration;
use uuid::Uuid;

use super::{LimitKey, OwnerId, ReservationId, StorageError, StorageTransaction};

pub const LEASE_LIFETIME: Duration = Duration::from_secs(30);

/// Lease ownership is `(id, owner)`: another owner cannot renew or release it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FencedLease {
    pub id: ReservationId,
    pub owner: OwnerId,
}

impl FencedLease {
    #[must_use]
    pub fn new(owner: OwnerId) -> Self {
        Self {
            id: ReservationId(Uuid::new_v4()),
            owner,
        }
    }
}

impl StorageTransaction {
    /// Reserves one concurrency slot when fewer than `maximum` live leases exist.
    /// A building block of [`StorageTransaction::try_admit`], which calls it in the safe order.
    /// # Errors
    /// Returns a sanitized constraint, invalid maximum, or database failure.
    pub async fn acquire_concurrency(
        &mut self,
        key: &LimitKey,
        lease: &FencedLease,
        maximum: usize,
    ) -> Result<bool, StorageError> {
        let maximum = i64::try_from(maximum).map_err(|_| StorageError::InvalidData)?;
        if maximum == 0 {
            return Err(StorageError::InvalidData);
        }
        self.lock_definition(key).await?;
        Ok(sqlx::query("INSERT INTO concurrency_leases(namespace,person_id,provider,limit_name,id,owner,expires_at) SELECT $1,$2,'anthropic',$3,$4,$5,clock_timestamp()+$6::bigint*interval '1 second' WHERE (SELECT count(*) FROM concurrency_leases WHERE namespace=$1 AND person_id=$2 AND provider='anthropic' AND limit_name=$3 AND expires_at>clock_timestamp())<$7")
            .bind(&self.namespace).bind(key.person).bind(&key.name).bind(lease.id).bind(lease.owner)
            .bind(lifetime_seconds()?).bind(maximum).execute(&mut *self.tx).await?.rows_affected() == 1)
    }

    /// Renews only this owner's unexpired lease using server time.
    /// # Errors
    /// Returns a sanitized database failure.
    pub async fn renew_concurrency(
        &mut self,
        key: &LimitKey,
        lease: &FencedLease,
    ) -> Result<bool, StorageError> {
        self.lock_definition(key).await?;
        Ok(sqlx::query("UPDATE concurrency_leases SET expires_at=clock_timestamp()+$4::bigint*interval '1 second' WHERE namespace=$1 AND id=$2 AND owner=$3 AND person_id=$5 AND provider='anthropic' AND limit_name=$6 AND expires_at>clock_timestamp()")
            .bind(&self.namespace).bind(lease.id).bind(lease.owner).bind(lifetime_seconds()?).bind(key.person).bind(&key.name)
            .execute(&mut *self.tx).await?.rows_affected() == 1)
    }

    /// Releases only this owner's lease and serializes availability with FIFO admission.
    /// # Errors
    /// Returns a sanitized database failure.
    pub async fn release_concurrency(
        &mut self,
        key: &LimitKey,
        lease: &FencedLease,
    ) -> Result<bool, StorageError> {
        self.lock_definition(key).await?;
        Ok(sqlx::query("DELETE FROM concurrency_leases WHERE namespace=$1 AND id=$2 AND owner=$3 AND person_id=$4 AND provider='anthropic' AND limit_name=$5")
            .bind(&self.namespace).bind(lease.id).bind(lease.owner).bind(key.person).bind(&key.name)
            .execute(&mut *self.tx).await?.rows_affected() == 1)
    }
}

fn lifetime_seconds() -> Result<i64, StorageError> {
    i64::try_from(LEASE_LIFETIME.as_secs()).map_err(|_| StorageError::InvalidData)
}
