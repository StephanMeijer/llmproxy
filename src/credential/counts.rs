use super::Credentials;
use crate::{
    metrics::CredentialCounts,
    secret::CredentialEnvelope,
    storage::{PersonId, StorageError},
};
use sqlx::Row;

impl Credentials {
    /// Counts a consistent namespace snapshot, checking active envelopes with the loaded ring.
    /// Decrypted values are immediately zeroized; no identity or envelope enters telemetry.
    /// # Errors
    /// Returns a sanitized database error, preserving the caller's previous snapshot.
    pub async fn counts(&self) -> Result<CredentialCounts, StorageError> {
        let mut tx = self.storage.begin().await?;
        let rows = sqlx::query("SELECT person_id,state,format_version,key_id,nonce,ciphertext FROM provider_credentials WHERE namespace=$1 AND provider='anthropic'")
            .bind(&self.storage.namespace).fetch_all(&mut *tx.tx).await?;
        tx.rollback().await?;
        let mut counts = CredentialCounts::default();
        for row in rows {
            match row.try_get::<&str, _>("state")? {
                "invalid" => counts.invalid += 1,
                "removed" => counts.removed += 1,
                "active" => {
                    let envelope = (|| -> Result<CredentialEnvelope, StorageError> {
                        Ok(CredentialEnvelope {
                            version: u8::try_from(row.try_get::<i32, _>("format_version")?)
                                .map_err(|_| StorageError::InvalidData)?,
                            key_id: row.try_get("key_id")?,
                            nonce: row
                                .try_get::<Vec<u8>, _>("nonce")?
                                .try_into()
                                .map_err(|_| StorageError::InvalidData)?,
                            ciphertext: row.try_get("ciphertext")?,
                        })
                    })();
                    let person = PersonId(row.try_get("person_id")?);
                    let usable = envelope.is_ok_and(|envelope| {
                        self.keys.decrypt(&self.context(person), &envelope).is_ok()
                    });
                    if usable {
                        counts.active += 1;
                    } else {
                        counts.unavailable += 1;
                    }
                }
                _ => return Err(StorageError::InvalidData),
            }
        }
        Ok(counts)
    }
}
