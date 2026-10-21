use crate::{
    config::CredentialEncryptionConfig,
    secret::{CredentialContext, KeyRing},
    storage::{PersonId, Storage, StorageError},
};

/// Rotation emits only aggregate counts, never identities or credential material.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct RotationCounts {
    pub rewritten: u64,
    pub remaining: i64,
}

impl Storage {
    /// Rewrites one current row per transaction. Restarting skips committed rows.
    /// Load the old and new keys on every replica before rotating; switch writers to
    /// the target key before removing old files. A zero reference count is required.
    /// # Errors
    /// Wrong/missing keys return `Unavailable`; the failing row remains untouched.
    pub async fn reencrypt_credentials(
        &self,
        config: &CredentialEncryptionConfig,
        target: &str,
    ) -> Result<RotationCounts, StorageError> {
        let keys = KeyRing::load(target, &config.keys).map_err(|_| StorageError::Unavailable)?;
        let mut counts = RotationCounts::default();
        loop {
            let mut tx = self.begin().await?;
            let person: Option<PersonId> = sqlx::query_scalar("SELECT person_id FROM provider_credentials WHERE namespace=$1 AND provider='anthropic' AND key_id<>$2 ORDER BY person_id LIMIT 1 FOR UPDATE")
                .bind(&self.namespace).bind(target).fetch_optional(&mut *tx.tx).await?;
            let Some(person) = person else {
                counts.remaining = sqlx::query_scalar(
                    "SELECT count(*) FROM provider_credentials WHERE namespace=$1 AND key_id<>$2",
                )
                .bind(&self.namespace)
                .bind(target)
                .fetch_one(&mut *tx.tx)
                .await?;
                tx.rollback().await?;
                return Ok(counts);
            };
            let record = tx
                .credential(person)
                .await?
                .ok_or(StorageError::Unavailable)?;
            let source = record.envelope.ok_or(StorageError::Unavailable)?;
            let context = CredentialContext {
                namespace: &self.namespace,
                person: person.0,
                provider: "anthropic",
            };
            let envelope = keys
                .reencrypt(&context, &source)
                .map_err(|_| StorageError::Unavailable)?;
            sqlx::query("UPDATE provider_credentials SET format_version=$3,key_id=$4,nonce=$5,ciphertext=$6 WHERE namespace=$1 AND person_id=$2 AND provider='anthropic'")
                .bind(&self.namespace).bind(person).bind(i32::from(envelope.version)).bind(&envelope.key_id)
                .bind(envelope.nonce.as_slice()).bind(&envelope.ciphertext).execute(&mut *tx.tx).await?;
            tx.commit().await?;
            counts.rewritten = counts.rewritten.saturating_add(1);
        }
    }

    /// Checks whether an old key still backs any current row, including invalid rows.
    /// Only retire a key after this returns zero and all writers use the new key.
    /// # Errors
    /// Returns a sanitized schema or database availability error.
    pub async fn credential_key_references(&self, key_id: &str) -> Result<i64, StorageError> {
        let mut tx = self.begin().await?;
        let count = sqlx::query_scalar(
            "SELECT count(*) FROM provider_credentials WHERE namespace=$1 AND key_id=$2",
        )
        .bind(&self.namespace)
        .bind(key_id)
        .fetch_one(&mut *tx.tx)
        .await?;
        tx.rollback().await?;
        Ok(count)
    }
}
