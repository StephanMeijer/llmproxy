use super::Credentials;
use crate::storage::{PersonId, StorageError};

/// Rotation emits only aggregate counts, never identities or credential material.
#[derive(Debug, Default, Eq, PartialEq)]
pub struct RotationCounts {
    pub rewritten: u64,
    pub remaining: i64,
}

impl Credentials {
    /// Rewrites every row not under the active key, one row per transaction. Restarting skips
    /// committed rows. Load the old and new keys on every replica before rotating; switch
    /// writers to the target key before removing old files.
    /// # Errors
    /// Wrong/missing keys return `Encryption`; the failing row remains untouched.
    pub async fn reencrypt(&self) -> Result<RotationCounts, StorageError> {
        let target = self.keys.active();
        let mut counts = RotationCounts::default();
        loop {
            let mut tx = self.storage.begin().await?;
            let person: Option<PersonId> = sqlx::query_scalar("SELECT person_id FROM provider_credentials WHERE namespace=$1 AND provider='anthropic' AND key_id<>$2 ORDER BY person_id LIMIT 1 FOR UPDATE")
                .bind(&self.storage.namespace).bind(target).fetch_optional(&mut *tx.tx).await?;
            let Some(person) = person else {
                counts.remaining = sqlx::query_scalar(
                    "SELECT count(*) FROM provider_credentials WHERE namespace=$1 AND key_id<>$2",
                )
                .bind(&self.storage.namespace)
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
            let envelope = self
                .keys
                .reencrypt(&self.context(person), &source)
                .map_err(|_| StorageError::Encryption)?;
            sqlx::query("UPDATE provider_credentials SET format_version=$3,key_id=$4,nonce=$5,ciphertext=$6 WHERE namespace=$1 AND person_id=$2 AND provider='anthropic'")
                .bind(&self.storage.namespace).bind(person).bind(i32::from(envelope.version)).bind(&envelope.key_id)
                .bind(envelope.nonce.as_slice()).bind(&envelope.ciphertext).execute(&mut *tx.tx).await?;
            tx.commit().await?;
            counts.rewritten = counts.rewritten.saturating_add(1);
        }
    }
}
