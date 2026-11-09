use super::{CredentialRecord, CredentialState};
use crate::{
    config::CredentialEncryptionConfig,
    secret::{self, CredentialContext, KeyRing, SecretBytes, SecretError, credential_fingerprint},
    storage::{PersonId, Provider, Storage, StorageError},
};
use std::fmt;

#[path = "counts.rs"]
mod counts;
#[path = "rotation.rs"]
mod rotation;
pub use rotation::RotationCounts;

/// Which key file could not be loaded; the message names the file, never its path or bytes.
#[derive(Debug)]
pub enum KeyLoadError {
    Ring(SecretError),
    Fingerprint(SecretError),
}

impl fmt::Display for KeyLoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Ring(error) => error.fmt(f),
            Self::Fingerprint(SecretError::InvalidKey) => {
                f.write_str("fingerprint key must contain exactly 32 bytes")
            }
            Self::Fingerprint(_) => f.write_str("fingerprint key unavailable"),
        }
    }
}

impl std::error::Error for KeyLoadError {}

/// Identity of a credential generation observed by an operator or admitted request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CredentialVersion {
    pub person: PersonId,
    pub generation: i64,
}

/// An admitted request owns its zeroizing secret independently of later mutations.
#[derive(Debug)]
pub struct CredentialSnapshot {
    pub version: CredentialVersion,
    pub secret: SecretBytes,
}

/// Namespace-scoped encrypted credential operations with a stable fingerprint pepper.
pub struct Credentials {
    storage: Storage,
    provider: Provider,
    keys: KeyRing,
    fingerprint_key: SecretBytes,
}

impl Credentials {
    /// Invalidates only the generation rejected by the provider.
    /// # Errors
    /// Returns a storage error, including `Conflict` if replacement won the race.
    pub async fn invalidate(&self, version: CredentialVersion) -> Result<(), StorageError> {
        let mut tx = self.storage.begin().await?;
        tx.set_credential_state(&CredentialRecord {
            person: version.person,
            provider: self.provider,
            generation: version.generation,
            state: CredentialState::Invalid,
            envelope: None,
        })
        .await?;
        tx.commit().await
    }

    /// Reads the configured key ring and fingerprint key files once.
    /// # Errors
    /// Returns which key file is missing, unreadable or malformed.
    pub fn open(
        storage: Storage,
        provider: Provider,
        config: &CredentialEncryptionConfig,
    ) -> Result<Self, KeyLoadError> {
        let keys =
            KeyRing::load(&config.active_key_id, &config.keys).map_err(KeyLoadError::Ring)?;
        let fingerprint_key =
            secret::key_file(&config.fingerprint_key_file).map_err(KeyLoadError::Fingerprint)?;
        Ok(Self::new(storage, provider, keys, fingerprint_key))
    }

    /// Builds from already loaded keys; production code uses [`Credentials::open`].
    /// Keep the fingerprint key stable across encryption-key rotations and replicas.
    #[must_use]
    pub const fn new(
        storage: Storage,
        provider: Provider,
        keys: KeyRing,
        fingerprint_key: SecretBytes,
    ) -> Self {
        Self {
            storage,
            provider,
            keys,
            fingerprint_key,
        }
    }

    /// Installs a person's first credential; an existing row requires explicit replacement.
    /// # Errors
    /// Returns a sanitized conflict, storage error, or unavailable encryption state.
    pub async fn install(
        &self,
        person: PersonId,
        secret: &SecretBytes,
    ) -> Result<CredentialVersion, StorageError> {
        let (record, fingerprint) = self.encrypt(
            CredentialVersion {
                person,
                generation: 1,
            },
            secret,
        )?;
        let mut tx = self.storage.begin().await?;
        if !tx.person(person).await?.enabled {
            return Err(StorageError::Conflict);
        }
        tx.insert_credential(&record, &fingerprint).await?;
        tx.commit().await?;
        Ok(CredentialVersion {
            person,
            generation: 1,
        })
    }

    /// Replaces exactly the observed generation, including a removed tombstone.
    /// # Errors
    /// Returns `Conflict` for stale generations or duplicate ownership.
    pub async fn replace(
        &self,
        version: CredentialVersion,
        secret: &SecretBytes,
    ) -> Result<CredentialVersion, StorageError> {
        let (record, fingerprint) = self.encrypt(version, secret)?;
        let mut tx = self.storage.begin().await?;
        if !tx.person(version.person).await?.enabled {
            return Err(StorageError::Conflict);
        }
        let generation = tx.replace_credential(&record, &fingerprint).await?;
        tx.commit().await?;
        Ok(CredentialVersion {
            generation,
            ..version
        })
    }

    /// Removes the current ciphertext without affecting already admitted snapshots.
    /// # Errors
    /// Returns a sanitized storage error or `Conflict` for a stale generation.
    pub async fn remove(
        &self,
        version: CredentialVersion,
    ) -> Result<CredentialVersion, StorageError> {
        let mut tx = self.storage.begin().await?;
        let generation = tx
            .set_credential_state(&CredentialRecord {
                person: version.person,
                provider: self.provider,
                generation: version.generation,
                state: CredentialState::Removed,
                envelope: None,
            })
            .await?;
        tx.commit().await?;
        Ok(CredentialVersion {
            generation,
            ..version
        })
    }

    /// Captures the current active generation for one admitted request.
    /// # Errors
    /// Missing, invalid and removed credentials are `Unavailable`; wrong or missing keys are
    /// `Encryption`.
    /// Decryption failure never mutates the durable row.
    pub async fn snapshot(&self, person: PersonId) -> Result<CredentialSnapshot, StorageError> {
        let mut tx = self.storage.begin().await?;
        let record = tx
            .credential(self.provider, person)
            .await?
            .ok_or(StorageError::Unavailable)?;
        match record.state {
            CredentialState::Active => {}
            CredentialState::Invalid | CredentialState::Removed => {
                return Err(StorageError::Unavailable);
            }
        }
        let envelope = record.envelope.ok_or(StorageError::Unavailable)?;
        let secret = self
            .keys
            .decrypt(&self.context(person), &envelope)
            .map_err(|_| StorageError::Encryption)?;
        tx.rollback().await?;
        Ok(CredentialSnapshot {
            version: CredentialVersion {
                person,
                generation: record.generation,
            },
            secret,
        })
    }

    fn encrypt(
        &self,
        version: CredentialVersion,
        secret: &SecretBytes,
    ) -> Result<(CredentialRecord, [u8; 32]), StorageError> {
        let envelope = self
            .keys
            .encrypt(&self.context(version.person), secret)
            .map_err(|_| StorageError::Encryption)?;
        let fingerprint = credential_fingerprint(&self.fingerprint_key, secret)
            .map_err(|_| StorageError::Encryption)?;
        Ok((
            CredentialRecord {
                person: version.person,
                provider: self.provider,
                generation: version.generation,
                state: CredentialState::Active,
                envelope: Some(envelope),
            },
            fingerprint,
        ))
    }

    fn context(&self, person: PersonId) -> CredentialContext<'_> {
        CredentialContext {
            namespace: &self.storage.namespace,
            person: person.0,
            provider: self.provider.as_str(),
        }
    }
}
