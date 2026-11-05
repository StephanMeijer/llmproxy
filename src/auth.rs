//! Route-aware authentication with local legacy authority and database person identity.

use std::{num::NonZeroUsize, thread, time::Duration};

use axum::http::{HeaderMap, StatusCode, header};

use crate::{
    secret::{ProxyToken, SecretError, TokenHasher},
    storage::{PersonId, Storage, StorageError, TokenId},
};

const BEARER_PREFIX: &str = "Bearer ";
const API_KEY_HEADER: &str = "x-api-key";
/// Caps verifier memory at 4 x 64 MiB (`MAX_HASH_MEMORY_KIB`) whatever the host's core count.
const MAX_VERIFIERS: usize = 4;
/// Longest a request queues for a verifier; it is not taken from the limits' `max_wait`.
pub const MAX_VERIFY_WAIT: Duration = Duration::from_secs(10);

/// Authority derived exclusively from the supplied proxy token.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Principal {
    LegacyStatic,
    Person(PersonId),
}

/// Route classification precedes authentication, including unknown paths.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteKind {
    Health,
    Static,
    Claude,
    Unknown,
}

/// Sanitized authentication failures without token or database details.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AuthError {
    Unauthorized,
    Forbidden,
    Unavailable,
    /// Every verifier stayed busy for the whole verifier wait.
    Busy,
}

impl AuthError {
    #[must_use]
    pub const fn status(self) -> StatusCode {
        match self {
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::Busy => StatusCode::TOO_MANY_REQUESTS,
        }
    }
}

/// Database connections are lazy; legacy requests never enter the database path.
pub struct Authenticator {
    legacy: String,
    storage: Option<Storage>,
    hasher: TokenHasher,
}

impl Authenticator {
    /// Builds local authority and, given the process's shared storage, a personal-token verifier.
    /// Verifications run one per CPU, at most `MAX_VERIFIERS`; a request waits for one up to
    /// `max_wait` or `MAX_VERIFY_WAIT`, whichever is shorter.
    #[must_use]
    pub fn new(legacy: String, storage: Option<Storage>, max_wait: Duration) -> Self {
        let workers = thread::available_parallelism().map_or(1, |n| n.get().min(MAX_VERIFIERS));
        let workers = NonZeroUsize::new(workers).unwrap_or(NonZeroUsize::MIN);
        Self {
            legacy,
            storage,
            hasher: TokenHasher::new(workers).waiting(max_wait.min(MAX_VERIFY_WAIT)),
        }
    }

    /// Health is public; every other route requires one unambiguous token.
    /// # Errors
    /// Returns unauthorized for invalid tokens, forbidden for legacy Claude access,
    /// unavailable when the database or verifier cannot establish authority, or busy when no
    /// verifier frees up in time.
    pub async fn authenticate(
        &self,
        route: RouteKind,
        headers: &HeaderMap,
    ) -> Result<Option<Principal>, AuthError> {
        if route == RouteKind::Health {
            return Ok(None);
        }
        let token = request_token(headers)?;
        if constant_time_eq(token.as_bytes(), self.legacy.as_bytes()) {
            return match route {
                RouteKind::Claude => Err(AuthError::Forbidden),
                RouteKind::Static | RouteKind::Unknown => Ok(Some(Principal::LegacyStatic)),
                RouteKind::Health => Ok(None),
            };
        }
        let token = ProxyToken::parse(token).map_err(|_| AuthError::Unauthorized)?;
        let storage = self.storage.as_ref().ok_or(AuthError::Unavailable)?;
        let mut transaction = storage.begin().await.map_err(|_| AuthError::Unavailable)?;
        let record = transaction
            .token(TokenId(token.id()))
            .await
            .map_err(|error| match error {
                StorageError::NotFound => AuthError::Unauthorized,
                _ => AuthError::Unavailable,
            })?;
        transaction
            .rollback()
            .await
            .map_err(|_| AuthError::Unavailable)?;
        if record.revoked || !record.person_enabled {
            return Err(AuthError::Unauthorized);
        }
        if !self
            .hasher
            .verify(&token, &record.secret_hash)
            .await
            .map_err(|error| match error {
                SecretError::Busy => AuthError::Busy,
                _ => AuthError::Unavailable,
            })?
        {
            return Err(AuthError::Unauthorized);
        }
        Ok(Some(Principal::Person(record.person_id)))
    }
}

fn request_token(headers: &HeaderMap) -> Result<&str, AuthError> {
    let single = |name| {
        let mut values = headers.get_all(name).iter();
        let value = values.next();
        if values.next().is_some() {
            return Err(AuthError::Unauthorized);
        }
        value
            .map(|value| value.to_str().map_err(|_| AuthError::Unauthorized))
            .transpose()
    };
    let bearer = single(header::AUTHORIZATION.as_str())?
        .map(|value| {
            value
                .strip_prefix(BEARER_PREFIX)
                .ok_or(AuthError::Unauthorized)
        })
        .transpose()?;
    let api_key = single(API_KEY_HEADER)?;
    let token = match (bearer, api_key) {
        (Some(bearer), Some(key)) if constant_time_eq(bearer.as_bytes(), key.as_bytes()) => bearer,
        (Some(token), None) | (None, Some(token)) => token,
        (Some(_), Some(_)) | (None, None) => return Err(AuthError::Unauthorized),
    };
    if token.is_empty() || token.bytes().any(|byte| byte.is_ascii_whitespace()) {
        return Err(AuthError::Unauthorized);
    }
    Ok(token)
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0, |diff, (x, y)| diff | (x ^ y)) == 0
}
