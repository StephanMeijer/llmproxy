use super::{LimitKey, StorageError, StorageTransaction};
use std::time::Duration;
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, PartialEq, sqlx::Type)]
#[sqlx(transparent)]
pub struct ReservationId(pub Uuid);
#[derive(Clone, Copy, Debug, Eq, PartialEq, sqlx::Type)]
#[sqlx(transparent)]
pub struct OwnerId(pub Uuid);

#[derive(Debug, sqlx::FromRow)]
pub struct Waiter {
    pub id: ReservationId,
    pub owner: OwnerId,
    pub enqueue_order: i64,
}
#[derive(Debug, sqlx::FromRow)]
pub struct Lease {
    pub id: ReservationId,
    pub owner: OwnerId,
}

impl StorageTransaction {
    /// Adds a waiter in database-serialized FIFO order; TTL uses server time.
    /// # Errors
    /// Returns a sanitized constraint or database error.
    pub async fn enqueue(
        &mut self,
        key: &LimitKey,
        reservation: (&Lease, Duration),
    ) -> Result<i64, StorageError> {
        self.lock_definition(key).await?;
        let (lease, ttl) = reservation;
        Ok(sqlx::query_scalar("INSERT INTO fifo_waiters(namespace,person_id,provider,limit_name,id,owner,expires_at) VALUES ($1,$2,'anthropic',$3,$4,$5,clock_timestamp()+$6::bigint*interval '1 microsecond') RETURNING enqueue_order")
            .bind(&self.namespace).bind(key.person).bind(&key.name).bind(lease.id).bind(lease.owner).bind(microseconds(ttl)?)
            .fetch_one(&mut *self.tx).await?)
    }

    /// Adds an in-flight lease under the definition lock.
    /// # Errors
    /// Returns a sanitized constraint or database error.
    pub async fn insert_lease(
        &mut self,
        key: &LimitKey,
        reservation: (&Lease, Duration),
    ) -> Result<(), StorageError> {
        self.lock_definition(key).await?;
        let (lease, ttl) = reservation;
        sqlx::query("INSERT INTO concurrency_leases(namespace,person_id,provider,limit_name,id,owner,expires_at) VALUES ($1,$2,'anthropic',$3,$4,$5,clock_timestamp()+$6::bigint*interval '1 microsecond')")
            .bind(&self.namespace).bind(key.person).bind(&key.name).bind(lease.id).bind(lease.owner).bind(microseconds(ttl)?)
            .execute(&mut *self.tx).await?;
        Ok(())
    }

    /// Refreshes a live lease owned by this replica; expired leases cannot revive.
    /// # Errors
    /// Returns a sanitized database error.
    pub async fn renew_lease(
        &mut self,
        lease: &Lease,
        ttl: Duration,
    ) -> Result<bool, StorageError> {
        Ok(sqlx::query("UPDATE concurrency_leases SET expires_at=clock_timestamp()+$4::bigint*interval '1 microsecond' WHERE namespace=$1 AND id=$2 AND owner=$3 AND expires_at>clock_timestamp()")
            .bind(&self.namespace).bind(lease.id).bind(lease.owner).bind(microseconds(ttl)?).execute(&mut *self.tx).await?.rows_affected() == 1)
    }

    /// Removes only a reservation owned by the caller, from both queue and leases.
    /// # Errors
    /// Returns a sanitized database error.
    pub async fn release(&mut self, key: &LimitKey, lease: &Lease) -> Result<(), StorageError> {
        self.lock_definition(key).await?;
        for query in [
            "DELETE FROM fifo_waiters WHERE namespace=$1 AND id=$2 AND owner=$3 AND person_id=$4 AND limit_name=$5 AND provider='anthropic'",
            "DELETE FROM concurrency_leases WHERE namespace=$1 AND id=$2 AND owner=$3 AND person_id=$4 AND limit_name=$5 AND provider='anthropic'",
        ] {
            sqlx::query(query)
                .bind(&self.namespace)
                .bind(lease.id)
                .bind(lease.owner)
                .bind(key.person)
                .bind(&key.name)
                .execute(&mut *self.tx)
                .await?;
        }
        Ok(())
    }

    /// Reclaims expired records according to `PostgreSQL` time under the limit lock.
    /// # Errors
    /// Returns a sanitized database error.
    pub async fn reclaim(&mut self, key: &LimitKey) -> Result<(), StorageError> {
        self.lock_definition(key).await?;
        for query in [
            "DELETE FROM fifo_waiters WHERE namespace=$1 AND person_id=$2 AND provider='anthropic' AND limit_name=$3 AND expires_at<=clock_timestamp()",
            "DELETE FROM concurrency_leases WHERE namespace=$1 AND person_id=$2 AND provider='anthropic' AND limit_name=$3 AND expires_at<=clock_timestamp()",
        ] {
            sqlx::query(query)
                .bind(&self.namespace)
                .bind(key.person)
                .bind(&key.name)
                .execute(&mut *self.tx)
                .await?;
        }
        Ok(())
    }

    pub(super) async fn waiters(&mut self, key: &LimitKey) -> Result<Vec<Waiter>, StorageError> {
        Ok(sqlx::query_as("SELECT id,owner,enqueue_order FROM fifo_waiters WHERE namespace=$1 AND person_id=$2 AND provider='anthropic' AND limit_name=$3 AND expires_at>clock_timestamp() ORDER BY enqueue_order")
            .bind(&self.namespace).bind(key.person).bind(&key.name).fetch_all(&mut *self.tx).await?)
    }

    pub(super) async fn leases(&mut self, key: &LimitKey) -> Result<Vec<Lease>, StorageError> {
        Ok(sqlx::query_as("SELECT id,owner FROM concurrency_leases WHERE namespace=$1 AND person_id=$2 AND provider='anthropic' AND limit_name=$3 AND expires_at>clock_timestamp() ORDER BY id")
            .bind(&self.namespace).bind(key.person).bind(&key.name).fetch_all(&mut *self.tx).await?)
    }
}

fn microseconds(duration: Duration) -> Result<i64, StorageError> {
    i64::try_from(duration.as_micros()).map_err(|_| StorageError::InvalidData)
}
