use super::{PersonId, Provider, StorageError, StorageTransaction};
use crate::secret::CredentialEnvelope;
use sqlx::{Row, postgres::PgRow};

#[path = "../credential/service.rs"]
mod service;
pub use service::{
    CredentialSnapshot, CredentialVersion, Credentials, KeyLoadError, RotationCounts,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, sqlx::Type, serde::Serialize)]
#[sqlx(type_name = "text", rename_all = "lowercase")]
#[serde(rename_all = "lowercase")]
pub enum CredentialState {
    Active,
    Invalid,
    Removed,
}

#[derive(Debug)]
pub struct CredentialRecord {
    pub person: PersonId,
    pub provider: Provider,
    pub generation: i64,
    pub state: CredentialState,
    pub envelope: Option<CredentialEnvelope>,
}

impl StorageTransaction {
    /// Reads the encrypted current generation under a row lock.
    /// # Errors
    /// Returns a sanitized missing, malformed, or unavailable record error.
    pub async fn credential(
        &mut self,
        provider: Provider,
        person: PersonId,
    ) -> Result<Option<CredentialRecord>, StorageError> {
        let row = sqlx::query("SELECT generation,state,format_version,key_id,nonce,ciphertext FROM provider_credentials WHERE namespace=$1 AND person_id=$2 AND provider=$3 FOR UPDATE")
            .bind(&self.namespace).bind(person).bind(provider).fetch_optional(&mut *self.tx).await?;
        row.map(|row| {
            let state = row.try_get("state")?;
            let envelope = match state {
                CredentialState::Active | CredentialState::Invalid => Some(envelope(&row)?),
                CredentialState::Removed => None,
            };
            Ok(CredentialRecord {
                person,
                provider,
                generation: row.try_get("generation")?,
                state,
                envelope,
            })
        })
        .transpose()
    }

    /// Inserts ciphertext; plaintext never enters this repository.
    /// # Errors
    /// Returns `Conflict` for duplicate ownership, otherwise a sanitized database error.
    pub async fn insert_credential(
        &mut self,
        record: &CredentialRecord,
        fingerprint: &[u8; 32],
    ) -> Result<(), StorageError> {
        let envelope = record.envelope.as_ref().ok_or(StorageError::InvalidData)?;
        sqlx::query("INSERT INTO provider_credentials(namespace,person_id,provider,state,format_version,key_id,nonce,ciphertext,fingerprint) VALUES ($1,$2,$8,'active',$3,$4,$5,$6,$7)")
            .bind(&self.namespace).bind(record.person).bind(i32::from(envelope.version)).bind(&envelope.key_id)
            .bind(envelope.nonce.as_slice()).bind(&envelope.ciphertext).bind(fingerprint.as_slice()).bind(record.provider)
            .execute(&mut *self.tx).await?;
        Ok(())
    }

    /// Replaces ciphertext only if the caller's generation remains current.
    /// # Errors
    /// Returns `Conflict` for a stale generation or duplicate fingerprint.
    pub async fn replace_credential(
        &mut self,
        record: &CredentialRecord,
        fingerprint: &[u8; 32],
    ) -> Result<i64, StorageError> {
        let envelope = record.envelope.as_ref().ok_or(StorageError::InvalidData)?;
        sqlx::query_scalar("UPDATE provider_credentials SET state='active',format_version=$4,key_id=$5,nonce=$6,ciphertext=$7,fingerprint=$8 WHERE namespace=$1 AND person_id=$2 AND provider=$9 AND generation=$3 RETURNING generation")
            .bind(&self.namespace).bind(record.person).bind(record.generation).bind(i32::from(envelope.version)).bind(&envelope.key_id)
            .bind(envelope.nonce.as_slice()).bind(&envelope.ciphertext).bind(fingerprint.as_slice()).bind(record.provider)
            .fetch_optional(&mut *self.tx).await?.ok_or(StorageError::Conflict)
    }

    /// Invalidates or removes exactly the generation observed by the caller.
    /// # Errors
    /// Returns `InvalidData` for activation without ciphertext or `Conflict` for stale state.
    pub async fn set_credential_state(
        &mut self,
        record: &CredentialRecord,
    ) -> Result<i64, StorageError> {
        let query = match record.state {
            CredentialState::Active => return Err(StorageError::InvalidData),
            CredentialState::Invalid => {
                "UPDATE provider_credentials SET state='invalid' WHERE namespace=$1 AND person_id=$2 AND provider=$4 AND generation=$3 AND state <> 'removed' RETURNING generation"
            }
            CredentialState::Removed => {
                "UPDATE provider_credentials SET state='removed',format_version=NULL,key_id=NULL,nonce=NULL,ciphertext=NULL,fingerprint=NULL WHERE namespace=$1 AND person_id=$2 AND provider=$4 AND generation=$3 RETURNING generation"
            }
        };
        sqlx::query_scalar(query)
            .bind(&self.namespace)
            .bind(record.person)
            .bind(record.generation)
            .bind(record.provider)
            .fetch_optional(&mut *self.tx)
            .await?
            .ok_or(StorageError::Conflict)
    }
}

/// Decodes the envelope columns of a `provider_credentials` row.
pub(super) fn envelope(row: &PgRow) -> Result<CredentialEnvelope, StorageError> {
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
}
