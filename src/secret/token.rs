use super::{SecretError, random};
use argon2::{
    Argon2, Params,
    password_hash::{PasswordHasher, PasswordVerifier, phc::PasswordHash},
};
use std::{
    fmt::{self, Write as _},
    num::NonZeroUsize,
    sync::Arc,
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;
use zeroize::Zeroizing;

const TOKEN_BYTES: usize = 32;
const SALT_BYTES: usize = 16;
const TOKEN_HEX_LEN: usize = TOKEN_BYTES * 2;
const MAX_HASH_MEMORY_KIB: u32 = 65_536;
const MAX_HASH_ITERATIONS: u32 = 4;
const MAX_HASH_LANES: u32 = 4;

/// Public UUID identifier plus a 256-bit random secret, wiped on drop.
pub struct ProxyToken {
    id: Uuid,
    encoded: Zeroizing<String>,
}

impl ProxyToken {
    /// Generates a fresh token. The UUID is public; the suffix is secret.
    /// # Errors
    /// Returns `Randomness` if the operating system RNG fails.
    pub fn generate() -> Result<Self, SecretError> {
        let id = Uuid::from_bytes(random()?);
        let bytes = Zeroizing::new(random::<TOKEN_BYTES>()?);
        // Full capacity up front: a reallocation would leave an unwiped copy of the secret behind.
        let mut encoded = Zeroizing::new(String::with_capacity(
            uuid::fmt::Hyphenated::LENGTH + 1 + TOKEN_HEX_LEN,
        ));
        write!(encoded, "{id}.").map_err(|_| SecretError::InvalidToken)?;
        for byte in bytes.iter() {
            write!(encoded, "{byte:02x}").map_err(|_| SecretError::InvalidToken)?;
        }
        Ok(Self { id, encoded })
    }

    /// Parses the canonical token wire format, rejecting unbounded input.
    /// # Errors
    /// Returns `InvalidToken` for malformed identifiers or secret suffixes.
    pub fn parse(value: &str) -> Result<Self, SecretError> {
        let (public, secret) = value.split_once('.').ok_or(SecretError::InvalidToken)?;
        let id = Uuid::parse_str(public).map_err(|_| SecretError::InvalidToken)?;
        if public != id.to_string()
            || secret.len() != TOKEN_HEX_LEN
            || !secret
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(SecretError::InvalidToken);
        }
        Ok(Self {
            id,
            encoded: Zeroizing::new(value.to_owned()),
        })
    }

    /// The non-secret indexed identifier; avoid using it as a telemetry label.
    #[must_use]
    pub const fn id(&self) -> Uuid {
        self.id
    }

    /// Explicitly borrows the complete bearer token for one-time delivery.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.encoded
    }
}

impl fmt::Debug for ProxyToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ProxyToken([REDACTED])")
    }
}

/// Shared bound for CPU/memory-heavy Argon2id operations. Only running workers hold memory;
/// callers waiting for a slot hold none. A cancelled caller does not release capacity until its
/// worker finishes.
#[derive(Clone, Debug)]
pub struct TokenHasher {
    slots: Arc<Semaphore>,
    wait: Duration,
}

impl TokenHasher {
    /// Sets the maximum number of concurrently running hashing workers. Fails fast with `Busy`
    /// when all are running; see [`TokenHasher::waiting`].
    #[must_use]
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self {
            slots: Arc::new(Semaphore::new(capacity.get())),
            wait: Duration::ZERO,
        }
    }

    /// Waits up to `wait` for a free worker before returning `Busy`, in arrival order.
    #[must_use]
    pub const fn waiting(mut self, wait: Duration) -> Self {
        self.wait = wait;
        self
    }

    async fn slot(&self) -> Result<OwnedSemaphorePermit, SecretError> {
        let slots = Arc::clone(&self.slots);
        if self.wait.is_zero() {
            return slots.try_acquire_owned().map_err(|_| SecretError::Busy);
        }
        tokio::time::timeout(self.wait, slots.acquire_owned())
            .await
            .map_err(|_| SecretError::Busy)?
            .map_err(|_| SecretError::Busy)
    }

    /// Hashes a token with a fresh salt and stores Argon2id parameters in PHC format.
    /// # Errors
    /// Returns `Busy` when no worker frees up in time, or a redacted randomness/hash/worker error.
    pub async fn hash(&self, token: &ProxyToken) -> Result<String, SecretError> {
        let permit = self.slot().await?;
        let token = Zeroizing::new(token.expose().to_owned());
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            let salt = random::<SALT_BYTES>()?;
            Argon2::default()
                .hash_password_with_salt(token.as_bytes(), &salt)
                .map(|hash| hash.to_string())
                .map_err(|_| SecretError::InvalidHash)
        })
        .await
        .map_err(|_| SecretError::Worker)?
    }

    /// Verifies in constant time using the stored salt and bounded PHC parameters.
    /// # Errors
    /// Returns `Busy`, `InvalidHash`, or `Worker`; a wrong token returns `false`.
    pub async fn verify(&self, token: &ProxyToken, encoded: &str) -> Result<bool, SecretError> {
        let hash = PasswordHash::new(encoded).map_err(|_| SecretError::InvalidHash)?;
        let params = Params::try_from(&hash).map_err(|_| SecretError::InvalidHash)?;
        if hash.algorithm.as_str() != "argon2id"
            || hash.version != Some(19)
            || params.m_cost() > MAX_HASH_MEMORY_KIB
            || params.t_cost() > MAX_HASH_ITERATIONS
            || params.p_cost() > MAX_HASH_LANES
        {
            return Err(SecretError::InvalidHash);
        }
        let permit = self.slot().await?;
        let token = Zeroizing::new(token.expose().to_owned());
        let encoded = encoded.to_owned();
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            match Argon2::default().verify_password(token.as_bytes(), encoded.as_str()) {
                Ok(()) => Ok(true),
                Err(argon2::password_hash::Error::PasswordInvalid) => Ok(false),
                Err(_) => Err(SecretError::InvalidHash),
            }
        })
        .await
        .map_err(|_| SecretError::Worker)?
    }
}
