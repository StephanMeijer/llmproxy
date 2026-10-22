use super::{KEY_BYTES, SecretBytes, SecretError, random};
use aes_gcm::{
    Aes256Gcm, KeyInit, Nonce,
    aead::{Aead, Payload},
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt, io::Read, path::PathBuf};
use uuid::Uuid;
use zeroize::Zeroizing;

const ENVELOPE_VERSION: u8 = 1;
const NONCE_BYTES: usize = 12;
const MAX_KEY_ID_BYTES: usize = 63;

/// Authenticated ownership context; changes to any field invalidate decryption.
pub struct CredentialContext<'a> {
    pub namespace: &'a str,
    pub person: Uuid,
    pub provider: &'a str,
}

/// Persistable ciphertext envelope. Its `Debug` never includes ciphertext or IDs.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialEnvelope {
    pub version: u8,
    pub key_id: String,
    pub nonce: [u8; NONCE_BYTES],
    pub ciphertext: Vec<u8>,
}

impl fmt::Debug for CredentialEnvelope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CredentialEnvelope([REDACTED])")
    }
}

/// Immutable key-ring snapshot; publish a replacement snapshot for online rotation.
pub struct KeyRing {
    active: String,
    keys: BTreeMap<String, SecretBytes>,
}

impl fmt::Debug for KeyRing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("KeyRing([REDACTED])")
    }
}

impl KeyRing {
    /// Validates the active key and every 32-byte key in a snapshot.
    /// # Errors
    /// Returns `InvalidKey` for malformed IDs/keys, or `UnknownKey` if active is absent.
    pub fn new(active: &str, keys: BTreeMap<String, SecretBytes>) -> Result<Self, SecretError> {
        for (id, key) in &keys {
            if id.is_empty()
                || id.len() > MAX_KEY_ID_BYTES
                || !id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
                || key.expose().len() != KEY_BYTES
            {
                return Err(SecretError::InvalidKey);
            }
        }
        if !keys.contains_key(active) {
            return Err(SecretError::UnknownKey);
        }
        Ok(Self {
            active: active.to_owned(),
            keys,
        })
    }

    /// Reads raw 32-byte master keys from mounted files into zeroizing memory.
    /// Restrict files to the service account (for example Unix mode 0600).
    /// # Errors
    /// Returns redacted `KeyFile`, `InvalidKey`, or `UnknownKey` without paths.
    pub fn load(active: &str, paths: &BTreeMap<String, PathBuf>) -> Result<Self, SecretError> {
        let mut keys = BTreeMap::new();
        for (id, path) in paths {
            let file = std::fs::File::open(path).map_err(|_| SecretError::KeyFile)?;
            let mut bytes = Zeroizing::new(Vec::with_capacity(KEY_BYTES + 1));
            file.take(u64::try_from(KEY_BYTES + 1).map_err(|_| SecretError::InvalidKey)?)
                .read_to_end(&mut bytes)
                .map_err(|_| SecretError::KeyFile)?;
            keys.insert(id.clone(), SecretBytes(bytes));
        }
        Self::new(active, keys)
    }

    /// Encrypts using the active key and a fresh random 96-bit nonce.
    /// # Errors
    /// Returns redacted randomness, key, or authentication errors.
    pub fn encrypt(
        &self,
        context: &CredentialContext<'_>,
        plaintext: &SecretBytes,
    ) -> Result<CredentialEnvelope, SecretError> {
        let nonce = random()?;
        let mut envelope = CredentialEnvelope {
            version: ENVELOPE_VERSION,
            key_id: self.active.clone(),
            nonce,
            ciphertext: Vec::new(),
        };
        let aad = associated_data(context, &envelope)?;
        envelope.ciphertext = self
            .cipher(&envelope.key_id)?
            .encrypt(
                &Nonce::from(nonce),
                Payload {
                    msg: plaintext.expose(),
                    aad: &aad,
                },
            )
            .map_err(|_| SecretError::Authentication)?;
        Ok(envelope)
    }

    /// Decrypts only when version, key and authenticated ownership context match.
    /// # Errors
    /// Returns `UnsupportedVersion`, `UnknownKey`, or `Authentication` on tampering.
    pub fn decrypt(
        &self,
        context: &CredentialContext<'_>,
        envelope: &CredentialEnvelope,
    ) -> Result<SecretBytes, SecretError> {
        let aad = associated_data(context, envelope)?;
        self.cipher(&envelope.key_id)?
            .decrypt(
                &Nonce::from(envelope.nonce),
                Payload {
                    msg: &envelope.ciphertext,
                    aad: &aad,
                },
            )
            .map(SecretBytes::new)
            .map_err(|_| SecretError::Authentication)
    }

    /// Re-encrypts an authenticated envelope under this snapshot's active key.
    /// Keep old keys installed until every old envelope has been migrated.
    /// # Errors
    /// Propagates decryption/encryption failures without altering the source.
    pub fn reencrypt(
        &self,
        context: &CredentialContext<'_>,
        envelope: &CredentialEnvelope,
    ) -> Result<CredentialEnvelope, SecretError> {
        self.encrypt(context, &self.decrypt(context, envelope)?)
    }

    fn cipher(&self, id: &str) -> Result<Aes256Gcm, SecretError> {
        let key = self.keys.get(id).ok_or(SecretError::UnknownKey)?;
        Aes256Gcm::new_from_slice(key.expose()).map_err(|_| SecretError::InvalidKey)
    }
}

fn associated_data(
    context: &CredentialContext<'_>,
    envelope: &CredentialEnvelope,
) -> Result<Vec<u8>, SecretError> {
    if envelope.version != ENVELOPE_VERSION {
        return Err(SecretError::UnsupportedVersion);
    }
    serde_json::to_vec(&(
        "llmproxy/credential",
        context.namespace,
        context.person,
        context.provider,
        envelope.version,
        &envelope.key_id,
    ))
    .map_err(|_| SecretError::Authentication)
}
