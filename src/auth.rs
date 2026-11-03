//! Route-aware authentication with local legacy authority and database person identity.

use std::{num::NonZeroUsize, sync::OnceLock};

use axum::http::{HeaderMap, StatusCode, header};

use crate::{
    config::PostgresConfig,
    secret::{ProxyToken, TokenHasher},
    storage::{PersonId, Storage, StorageError, TokenId},
};

const BEARER_PREFIX: &str = "Bearer ";
const API_KEY_HEADER: &str = "x-api-key";

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
}

impl AuthError {
    #[must_use]
    pub const fn status(self) -> StatusCode {
        match self {
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        }
    }
}

/// Database connections are lazy; legacy requests never enter the database path.
pub struct Authenticator {
    legacy: String,
    postgres: Option<PostgresConfig>,
    storage: OnceLock<Result<Storage, StorageError>>,
    hasher: TokenHasher,
}

impl Authenticator {
    /// Builds local authority and an optional lazy personal-token verifier.
    #[must_use]
    pub fn new(legacy: String, postgres: Option<&PostgresConfig>) -> Self {
        Self {
            legacy,
            postgres: postgres.map(|config| PostgresConfig {
                url: config.url.clone(),
                namespace: config.namespace.clone(),
                max_connections: config.max_connections,
                connect_timeout: config.connect_timeout,
                acquire_timeout: config.acquire_timeout,
            }),
            storage: OnceLock::new(),
            hasher: TokenHasher::new(NonZeroUsize::MIN),
        }
    }

    /// Health is public; every other route requires one unambiguous token.
    /// # Errors
    /// Returns unauthorized for invalid tokens, forbidden for legacy Claude access,
    /// or unavailable when the database or verifier cannot establish authority.
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
        let postgres = self.postgres.as_ref().ok_or(AuthError::Unavailable)?;
        let storage = self
            .storage
            .get_or_init(|| Storage::new(postgres))
            .as_ref()
            .map_err(|_| AuthError::Unavailable)?;
        let mut transaction = storage.begin().await.map_err(|_| AuthError::Unavailable)?;
        let record = transaction
            .token(TokenId(token.id()))
            .await
            .map_err(|error| match error {
                StorageError::NotFound => AuthError::Unauthorized,
                StorageError::Unavailable
                | StorageError::IncompatibleSchema
                | StorageError::UnsupportedServer
                | StorageError::MigrationFailed
                | StorageError::Conflict
                | StorageError::InvalidData => AuthError::Unavailable,
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
            .map_err(|_| AuthError::Unavailable)?
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
