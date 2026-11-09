use super::{PersonId, Provider, StorageError, StorageTransaction};
use crate::limit::shared::SharedDefinition;

#[path = "rate.rs"]
mod rate;
pub use rate::PreparedRates;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct LimitKey {
    pub person: PersonId,
    pub provider: Provider,
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

impl StorageTransaction {
    /// Stores a typed shared limit definition, serialized by `PostgreSQL` as JSONB.
    /// # Errors
    /// Returns a sanitized database error.
    pub async fn put_limit(
        &mut self,
        key: &LimitKey,
        definition: &SharedDefinition,
    ) -> Result<i64, StorageError> {
        if definition.namespace() != self.namespace
            || definition.provider() != key.provider
            || definition.name() != key.name
        {
            return Err(StorageError::InvalidData);
        }
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1 || '/' || $2, 0))")
            .bind(&self.namespace)
            .bind(&key.name)
            .execute(&mut *self.tx)
            .await?;
        let conflict: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM limit_definitions WHERE namespace=$1 AND provider=$4 AND name=$2 AND config<>$3)")
            .bind(&self.namespace).bind(&key.name).bind(sqlx::types::Json(definition)).bind(key.provider).fetch_one(&mut *self.tx).await?;
        if conflict {
            return Err(StorageError::Conflict);
        }
        // The conflict check above proves an existing row already holds this config, so it is never
        // rewritten. The advisory lock makes a concurrent insert visible to the fallback SELECT.
        Ok(sqlx::query_scalar("WITH inserted AS (INSERT INTO limit_definitions(namespace,person_id,provider,name,config) VALUES ($1,$2,$5,$3,$4) ON CONFLICT(namespace,person_id,provider,name) DO NOTHING RETURNING version) SELECT version FROM inserted UNION ALL SELECT version FROM limit_definitions WHERE namespace=$1 AND person_id=$2 AND provider=$5 AND name=$3 LIMIT 1")
            .bind(&self.namespace).bind(key.person).bind(&key.name).bind(sqlx::types::Json(definition)).bind(key.provider)
            .fetch_one(&mut *self.tx).await?)
    }

    /// Locks the definition and its rate rows, reading only the rates admission needs.
    /// # Errors
    /// Returns `NotFound` or a sanitized database error.
    pub async fn lock_rates(&mut self, key: &LimitKey) -> Result<Vec<RateState>, StorageError> {
        self.lock_definition(key).await?;
        Ok(sqlx::query_as("SELECT kind,available_units,GREATEST(0,(extract(epoch FROM (clock_timestamp()-updated_at))*1000000)::bigint) AS elapsed_microseconds FROM rate_state WHERE namespace=$1 AND person_id=$2 AND provider=$4 AND limit_name=$3 ORDER BY kind FOR UPDATE")
            .bind(&self.namespace).bind(key.person).bind(&key.name).bind(key.provider).fetch_all(&mut *self.tx).await?)
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
        sqlx::query("INSERT INTO rate_state(namespace,person_id,provider,limit_name,kind,available_units) VALUES ($1,$2,$6,$3,$4,$5) ON CONFLICT(namespace,person_id,provider,limit_name,kind) DO UPDATE SET available_units=EXCLUDED.available_units,updated_at=clock_timestamp()")
            .bind(&self.namespace).bind(key.person).bind(&key.name).bind(state.kind).bind(state.available_units).bind(key.provider)
            .execute(&mut *self.tx).await?;
        Ok(())
    }

    pub(super) async fn lock_definition(&mut self, key: &LimitKey) -> Result<(), StorageError> {
        if self.locked.as_ref() == Some(key) {
            return Ok(());
        }
        sqlx::query("SELECT version FROM limit_definitions WHERE namespace=$1 AND person_id=$2 AND provider=$4 AND name=$3 FOR UPDATE")
            .bind(&self.namespace).bind(key.person).bind(&key.name).bind(key.provider).fetch_one(&mut *self.tx).await?;
        self.mark_locked(key);
        Ok(())
    }

    fn mark_locked(&mut self, key: &LimitKey) {
        self.mixed |= self.locked.as_ref().is_some_and(|locked| locked != key);
        self.locked = Some(key.clone());
    }
}
