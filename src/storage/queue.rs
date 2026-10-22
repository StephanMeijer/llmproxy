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
    /// Reads the live queue head while retaining the definition and waiter locks.
    /// # Errors
    /// Returns a sanitized database error.
    pub async fn waiter_head(&mut self, key: &LimitKey) -> Result<Option<Waiter>, StorageError> {
        self.lock_definition(key).await?;
        Ok(sqlx::query_as("SELECT id,owner,enqueue_order FROM fifo_waiters WHERE namespace=$1 AND person_id=$2 AND provider='anthropic' AND limit_name=$3 AND expires_at>clock_timestamp() ORDER BY enqueue_order LIMIT 1 FOR UPDATE")
            .bind(&self.namespace).bind(key.person).bind(&key.name).fetch_optional(&mut *self.tx).await?)
    }

    /// Checks the caller's database-time deadline under the definition lock.
    /// # Errors
    /// Returns a sanitized database error.
    pub async fn waiter_live(
        &mut self,
        key: &LimitKey,
        lease: &Lease,
    ) -> Result<bool, StorageError> {
        self.lock_definition(key).await?;
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM fifo_waiters WHERE namespace=$1 AND person_id=$2 AND provider='anthropic' AND limit_name=$3 AND id=$4 AND owner=$5 AND expires_at>clock_timestamp())")
            .bind(&self.namespace).bind(key.person).bind(&key.name).bind(lease.id).bind(lease.owner).fetch_one(&mut *self.tx).await?)
    }

    /// Deletes an owned waiter; commit publishes the queue advancement notification.
    /// # Errors
    /// Returns a sanitized database error.
    pub async fn remove_waiter(
        &mut self,
        key: &LimitKey,
        lease: &Lease,
    ) -> Result<(), StorageError> {
        self.lock_definition(key).await?;
        sqlx::query("DELETE FROM fifo_waiters WHERE namespace=$1 AND person_id=$2 AND provider='anthropic' AND limit_name=$3 AND id=$4 AND owner=$5")
            .bind(&self.namespace).bind(key.person).bind(&key.name).bind(lease.id).bind(lease.owner).execute(&mut *self.tx).await?;
        Ok(())
    }
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

    /// Removes only a waiter owned by the caller; leases require fenced release.
    /// # Errors
    /// Returns a sanitized database error.
    pub async fn release(&mut self, key: &LimitKey, lease: &Lease) -> Result<(), StorageError> {
        self.remove_waiter(key, lease).await
    }

    /// Reclaims expired records according to `PostgreSQL` time under the limit lock.
    /// # Errors
    /// Returns a sanitized database error.
    pub async fn reclaim(&mut self, key: &LimitKey) -> Result<bool, StorageError> {
        self.lock_definition(key).await?;
        let mut changed = false;
        for query in [
            "DELETE FROM fifo_waiters WHERE namespace=$1 AND person_id=$2 AND provider='anthropic' AND limit_name=$3 AND expires_at<=clock_timestamp()",
            "DELETE FROM concurrency_leases WHERE namespace=$1 AND person_id=$2 AND provider='anthropic' AND limit_name=$3 AND expires_at<=clock_timestamp()",
        ] {
            changed |= sqlx::query(query)
                .bind(&self.namespace)
                .bind(key.person)
                .bind(&key.name)
                .execute(&mut *self.tx)
                .await?
                .rows_affected()
                > 0;
        }
        Ok(changed)
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
