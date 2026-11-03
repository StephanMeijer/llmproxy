use super::{super::envelope, Credentials};
use crate::{
    metrics::CredentialCounts,
    storage::{CredentialState, PersonId, StorageError},
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
            match row.try_get("state")? {
                CredentialState::Invalid => counts.invalid += 1,
                CredentialState::Removed => counts.removed += 1,
                CredentialState::Active => {
                    let person = PersonId(row.try_get("person_id")?);
                    // A malformed active envelope counts as unavailable, not as a read error.
                    let usable = envelope(&row).is_ok_and(|envelope| {
                        self.keys.decrypt(&self.context(person), &envelope).is_ok()
                    });
                    if usable {
                        counts.active += 1;
                    } else {
                        counts.unavailable += 1;
                    }
                }
            }
        }
        Ok(counts)
    }
}
