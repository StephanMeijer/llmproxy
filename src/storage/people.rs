use super::{StorageError, StorageTransaction};
use uuid::Uuid;

#[derive(Clone, Copy, Debug, Eq, PartialEq, sqlx::Type)]
#[sqlx(transparent)]
pub struct PersonId(pub Uuid);
#[derive(Clone, Copy, Debug, Eq, PartialEq, sqlx::Type)]
#[sqlx(transparent)]
pub struct TokenId(pub Uuid);

#[derive(Debug, sqlx::FromRow)]
pub struct Person {
    pub id: PersonId,
    pub display_name: String,
    pub enabled: bool,
    pub version: i64,
}

/// Hashes are returned only for explicit authentication; this type cannot be formatted.
#[derive(sqlx::FromRow)]
pub struct TokenRecord {
    pub id: TokenId,
    pub person_id: PersonId,
    pub secret_hash: String,
    pub revoked: bool,
    pub person_enabled: bool,
}

impl StorageTransaction {
    /// Creates a person in this deployment namespace.
    /// # Errors
    /// Returns a sanitized constraint or database error.
    pub async fn create_person(
        &mut self,
        id: PersonId,
        name: &str,
    ) -> Result<Person, StorageError> {
        Ok(sqlx::query_as("INSERT INTO people(namespace,id,display_name) VALUES ($1,$2,$3) RETURNING id,display_name,enabled,version")
            .bind(&self.namespace).bind(id).bind(name).fetch_one(&mut *self.tx).await?)
    }

    /// Reads current person state and locks it until transaction end.
    /// # Errors
    /// Returns `NotFound` or a sanitized database failure.
    pub async fn person(&mut self, id: PersonId) -> Result<Person, StorageError> {
        Ok(sqlx::query_as("SELECT id,display_name,enabled,version FROM people WHERE namespace=$1 AND id=$2 FOR UPDATE")
            .bind(&self.namespace).bind(id).fetch_one(&mut *self.tx).await?)
    }

    /// Updates person state; caller supplies the version observed under lock.
    /// # Errors
    /// Returns `Conflict` on concurrent modification, otherwise a database error.
    pub async fn update_person(&mut self, person: &Person) -> Result<Person, StorageError> {
        sqlx::query_as("UPDATE people SET display_name=$3,enabled=$4,version=version+1 WHERE namespace=$1 AND id=$2 AND version=$5 RETURNING id,display_name,enabled,version")
            .bind(&self.namespace).bind(person.id).bind(&person.display_name).bind(person.enabled).bind(person.version)
            .fetch_optional(&mut *self.tx).await?.ok_or(StorageError::Conflict)
    }

    /// Stores a verifier, never a plaintext proxy token.
    /// # Errors
    /// Returns a sanitized constraint or database error.
    pub async fn create_token(&mut self, token: &TokenRecord) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO proxy_tokens(namespace,id,person_id,secret_hash) VALUES ($1,$2,$3,$4)",
        )
        .bind(&self.namespace)
        .bind(token.id)
        .bind(token.person_id)
        .bind(&token.secret_hash)
        .execute(&mut *self.tx)
        .await?;
        Ok(())
    }

    /// Rereads both token revocation and person enablement on each authentication.
    /// # Errors
    /// Returns `NotFound` or a sanitized database failure.
    pub async fn token(&mut self, id: TokenId) -> Result<TokenRecord, StorageError> {
        Ok(sqlx::query_as("SELECT t.id,t.person_id,t.secret_hash,t.revoked_at IS NOT NULL AS revoked,p.enabled AS person_enabled FROM proxy_tokens t JOIN people p ON p.namespace=t.namespace AND p.id=t.person_id WHERE t.namespace=$1 AND t.id=$2")
            .bind(&self.namespace).bind(id).fetch_one(&mut *self.tx).await?)
    }

    /// Revokes a token using database time.
    /// # Errors
    /// Returns a sanitized database failure.
    pub async fn revoke_token(&mut self, id: TokenId) -> Result<bool, StorageError> {
        Ok(sqlx::query("UPDATE proxy_tokens SET revoked_at=clock_timestamp() WHERE namespace=$1 AND id=$2 AND revoked_at IS NULL")
            .bind(&self.namespace).bind(id).execute(&mut *self.tx).await?.rows_affected() == 1)
    }
}
