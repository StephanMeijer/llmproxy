use super::{Lease, PersonId, StorageError, StorageTransaction, Waiter};
use crate::limit::shared::SharedDefinition;

#[path = "rate.rs"]
mod rate;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LimitKey {
    pub person: PersonId,
    pub name: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, sqlx::Type)]
#[sqlx(type_name = "text", rename_all = "lowercase")]
pub enum RateKind {
    Requests,
    Tokens,
}

#[derive(Debug, sqlx::FromRow)]
pub struct RateState {
    pub kind: RateKind,
    pub available_units: i64,
    pub elapsed_microseconds: i64,
}

pub struct LimitSnapshot {
    pub definition: Vec<u8>,
    pub version: i64,
    pub rates: Vec<RateState>,
    pub waiters: Vec<Waiter>,
    pub leases: Vec<Lease>,
}

impl StorageTransaction {
    /// Stores a typed shared limit definition, serialized by `PostgreSQL` as JSONB.
    /// # Errors
    /// Returns a sanitized database error.
    pub async fn put_limit(
        &mut self,
        key: &LimitKey,
        definition: &SharedDefinition,
    ) -> Result<i64, StorageError> {
        if definition.namespace() != self.namespace || definition.name() != key.name {
            return Err(StorageError::InvalidData);
        }
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1 || '/' || $2, 0))")
            .bind(&self.namespace)
            .bind(&key.name)
            .execute(&mut *self.tx)
            .await?;
        let conflict: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM limit_definitions WHERE namespace=$1 AND provider='anthropic' AND name=$2 AND config<>$3)")
            .bind(&self.namespace).bind(&key.name).bind(sqlx::types::Json(definition)).fetch_one(&mut *self.tx).await?;
        if conflict {
            return Err(StorageError::Conflict);
        }
        Ok(sqlx::query_scalar("INSERT INTO limit_definitions(namespace,person_id,provider,name,config) VALUES ($1,$2,'anthropic',$3,$4) ON CONFLICT(namespace,person_id,provider,name) DO UPDATE SET config=EXCLUDED.config RETURNING version")
            .bind(&self.namespace).bind(key.person).bind(&key.name).bind(sqlx::types::Json(definition))
            .fetch_one(&mut *self.tx).await?)
    }

    /// Locks the definition before reading its rates and live reservations.
    /// # Errors
    /// Returns `NotFound`, malformed data, or a sanitized database error.
    pub async fn lock_limit(&mut self, key: &LimitKey) -> Result<LimitSnapshot, StorageError> {
        let (definition, version): (String, i64) = sqlx::query_as("SELECT config::text,version FROM limit_definitions WHERE namespace=$1 AND person_id=$2 AND provider='anthropic' AND name=$3 FOR UPDATE")
            .bind(&self.namespace).bind(key.person).bind(&key.name).fetch_one(&mut *self.tx).await?;
        let rates = sqlx::query_as("SELECT kind,available_units,GREATEST(0,(extract(epoch FROM (clock_timestamp()-updated_at))*1000000)::bigint) AS elapsed_microseconds FROM rate_state WHERE namespace=$1 AND person_id=$2 AND provider='anthropic' AND limit_name=$3 ORDER BY kind FOR UPDATE")
            .bind(&self.namespace).bind(key.person).bind(&key.name).fetch_all(&mut *self.tx).await?;
        let waiters = self.waiters(key).await?;
        let leases = self.leases(key).await?;
        Ok(LimitSnapshot {
            definition: definition.into_bytes(),
            version,
            rates,
            waiters,
            leases,
        })
    }

    /// Writes fixed-point state using database time, under the definition lock.
    /// # Errors
    /// Returns a sanitized constraint or database error.
    pub async fn put_rate(
        &mut self,
        key: &LimitKey,
        state: &RateState,
    ) -> Result<(), StorageError> {
        self.lock_definition(key).await?;
        sqlx::query("INSERT INTO rate_state(namespace,person_id,provider,limit_name,kind,available_units) VALUES ($1,$2,'anthropic',$3,$4,$5) ON CONFLICT(namespace,person_id,provider,limit_name,kind) DO UPDATE SET available_units=EXCLUDED.available_units,updated_at=clock_timestamp()")
            .bind(&self.namespace).bind(key.person).bind(&key.name).bind(state.kind).bind(state.available_units)
            .execute(&mut *self.tx).await?;
        Ok(())
    }

    pub(super) async fn lock_definition(&mut self, key: &LimitKey) -> Result<(), StorageError> {
        sqlx::query("SELECT version FROM limit_definitions WHERE namespace=$1 AND person_id=$2 AND provider='anthropic' AND name=$3 FOR UPDATE")
            .bind(&self.namespace).bind(key.person).bind(&key.name).fetch_one(&mut *self.tx).await?;
        Ok(())
    }
}
