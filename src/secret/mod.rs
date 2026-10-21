//! Redacted, zeroizing secrets and authenticated token/credential primitives.

mod encryption;
mod token;

pub use encryption::{CredentialContext, CredentialEnvelope, KeyRing};
pub use token::{ProxyToken, TokenHasher};

use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use std::fmt;
use zeroize::Zeroizing;

const KEY_BYTES: usize = 32;

/// Owned bytes which are wiped on drop and never formatted or serialized.
pub struct SecretBytes(Zeroizing<Vec<u8>>);

impl SecretBytes {
    /// Takes ownership of sensitive bytes without copying them.
    #[must_use]
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(Zeroizing::new(bytes))
    }

    /// Explicitly borrows plaintext for a cryptographic or transport operation.
    #[must_use]
    pub fn expose(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Debug for SecretBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SecretBytes([REDACTED])")
    }
}

/// Redacted failures with no paths, identifiers, key material or inner errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretError {
    Randomness,
    InvalidToken,
    InvalidHash,
    Busy,
    Worker,
    InvalidKey,
    KeyFile,
    UnknownKey,
    UnsupportedVersion,
    Authentication,
}

impl fmt::Display for SecretError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Randomness => "secure randomness unavailable",
            Self::InvalidToken => "invalid proxy token",
            Self::InvalidHash => "invalid token hash",
            Self::Busy => "token hashing capacity exhausted",
            Self::Worker => "token hashing worker failed",
            Self::InvalidKey => "invalid encryption key or key ring",
            Self::KeyFile => {
                "cannot read key file; supply a readable 32-byte file with restricted permissions"
            }
            Self::UnknownKey => "credential encryption key unavailable",
            Self::UnsupportedVersion => "unsupported credential envelope version",
            Self::Authentication => "credential authentication failed",
        })
    }
}
impl std::error::Error for SecretError {}

fn random<const N: usize>() -> Result<[u8; N], SecretError> {
    let mut bytes = [0; N];
    getrandom::fill(&mut bytes).map_err(|_| SecretError::Randomness)?;
    Ok(bytes)
}

/// Computes an indexed token lookup tag using a server-held 256-bit pepper.
/// # Errors
/// Returns `InvalidKey` unless the pepper has exactly 32 bytes.
pub fn lookup_tag(
    pepper: &SecretBytes,
    token: &ProxyToken,
) -> Result<[u8; KEY_BYTES], SecretError> {
    keyed_tag(
        pepper,
        b"llmproxy/token-lookup/v1\0",
        token.expose().as_bytes(),
    )
}

/// Computes a stable keyed fingerprint for identical provider-key detection.
/// Keep this pepper stable across encryption-key rotation.
/// # Errors
/// Returns `InvalidKey` unless the pepper has exactly 32 bytes.
pub fn credential_fingerprint(
    pepper: &SecretBytes,
    secret: &SecretBytes,
) -> Result<[u8; KEY_BYTES], SecretError> {
    keyed_tag(
        pepper,
        b"llmproxy/credential-fingerprint/v1\0",
        secret.expose(),
    )
}

fn keyed_tag(
    pepper: &SecretBytes,
    domain: &[u8],
    value: &[u8],
) -> Result<[u8; KEY_BYTES], SecretError> {
    if pepper.expose().len() != KEY_BYTES {
        return Err(SecretError::InvalidKey);
    }
    let mut mac =
        Hmac::<Sha256>::new_from_slice(pepper.expose()).map_err(|_| SecretError::InvalidKey)?;
    mac.update(domain);
    mac.update(value);
    Ok(mac.finalize().into_bytes().into())
}
