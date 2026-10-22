//! Explicit `PostgreSQL` migration lifecycle and fail-closed schema compatibility boundary.

use std::{fmt, str::FromStr};

use sqlx::{
    ConnectOptions, Connection, PgConnection, PgPool, migrate::Migrator, postgres::PgConnectOptions,
};

use crate::config::PostgresConfig;

mod credentials;
mod leases;
mod limits;
mod listener;
mod people;
mod queue;
mod runtime;
pub use credentials::{
    CredentialRecord, CredentialSnapshot, CredentialState, CredentialVersion, Credentials,
    RotationCounts,
};
pub use leases::{FencedLease, LEASE_LIFETIME};
pub use limits::{LimitKey, LimitSnapshot, RateKind, RateState};
pub use listener::ChangeListener;
pub use people::{Person, PersonId, TokenId, TokenRecord};
pub use queue::{Lease, OwnerId, ReservationId, Waiter};
pub use runtime::{Storage, StorageTransaction};

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");
const MIGRATION_LOCK: i64 = 0x006c_6c6d_7072_6f78;
const MIN_POSTGRES_VERSION: i32 = 160_000;

/// Bounded, non-secret failure categories; underlying driver errors never cross this boundary.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StorageError {
    Unavailable,
    IncompatibleSchema,
    UnsupportedServer,
    MigrationFailed,
    Conflict,
    InvalidData,
    NotFound,
}

impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Unavailable => "PostgreSQL unavailable; check connectivity and access",
            Self::IncompatibleSchema => {
                "PostgreSQL schema incompatible; run llmproxy db migrate with a compatible binary"
            }
            Self::UnsupportedServer => "PostgreSQL 16 or newer is required",
            Self::MigrationFailed => {
                "PostgreSQL migration failed; check schema ownership and migration privileges"
            }
            Self::Conflict => "database mutation conflicts with current state",
            Self::InvalidData => "invalid database record",
            Self::NotFound => "database record not found",
        })
    }
}

impl std::error::Error for StorageError {}

impl From<sqlx::Error> for StorageError {
    fn from(error: sqlx::Error) -> Self {
        match error {
            sqlx::Error::RowNotFound => Self::NotFound,
            sqlx::Error::Database(ref error) => match error.code().as_deref() {
                Some("23505" | "40001" | "40P01") => Self::Conflict,
                Some("23503" | "23514" | "23502" | "22003") => Self::InvalidData,
                Some("42P01" | "42703" | "42883") => Self::IncompatibleSchema,
                _ => Self::Unavailable,
            },
            sqlx::Error::ColumnDecode { .. } | sqlx::Error::Decode(_) => Self::InvalidData,
            _ => Self::Unavailable,
        }
    }
}

/// Apply embedded transactional migrations on a dedicated, advisory-locked session.
/// No connection is returned to a pool with a session lock still held, including on cancellation.
///
/// # Errors
/// Returns a sanitized connectivity, server-version, or migration failure.
pub async fn migrate(config: &PostgresConfig) -> Result<(), StorageError> {
    let options = PgConnectOptions::from_str(&config.url)
        .map_err(|_| StorageError::Unavailable)?
        .disable_statement_logging();
    let mut connection = tokio::time::timeout(config.connect_timeout, options.connect())
        .await
        .map_err(|_| StorageError::Unavailable)?
        .map_err(|_| StorageError::Unavailable)?;
    check_server(&mut connection).await?;
    sqlx::query("SELECT pg_advisory_lock($1)")
        .bind(MIGRATION_LOCK)
        .execute(&mut connection)
        .await
        .map_err(|_| StorageError::MigrationFailed)?;
    let result = MIGRATOR
        .run(&mut connection)
        .await
        .map_err(|_| StorageError::MigrationFailed);
    // Closing the dedicated session releases the advisory lock on both success and failure.
    connection
        .close()
        .await
        .map_err(|_| StorageError::Unavailable)?;
    result
}

/// Check compatibility when entering a database-backed operation, never during offline startup.
/// Missing, modified, partial, or newer schemas fail closed. Static routes need not call this.
///
/// # Errors
/// Returns [`StorageError::Unavailable`] for failed database access, or a compatibility error.
pub async fn check_schema(pool: &PgPool) -> Result<(), StorageError> {
    let mut connection = pool
        .acquire()
        .await
        .map_err(|_| StorageError::Unavailable)?;
    check_connection_schema(&mut connection).await
}

async fn check_connection_schema(connection: &mut PgConnection) -> Result<(), StorageError> {
    check_server(connection).await?;
    let exists: bool = sqlx::query_scalar("SELECT to_regclass('_sqlx_migrations') IS NOT NULL")
        .fetch_one(&mut *connection)
        .await
        .map_err(|_| StorageError::Unavailable)?;
    if !exists {
        return Err(StorageError::IncompatibleSchema);
    }
    let applied: Vec<(i64, bool, Vec<u8>)> =
        sqlx::query_as("SELECT version, success, checksum FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&mut *connection)
            .await
            .map_err(|_| StorageError::Unavailable)?;
    if applied.len() != MIGRATOR.iter().count()
        || applied
            .iter()
            .zip(MIGRATOR.iter())
            .any(|((version, success, checksum), migration)| {
                !success
                    || *version != migration.version
                    || checksum.as_slice() != migration.checksum.as_ref()
            })
    {
        return Err(StorageError::IncompatibleSchema);
    }
    Ok(())
}

async fn check_server(connection: &mut PgConnection) -> Result<(), StorageError> {
    let version: i32 = sqlx::query_scalar("SELECT current_setting('server_version_num')::integer")
        .fetch_one(connection)
        .await
        .map_err(|_| StorageError::Unavailable)?;
    if version < MIN_POSTGRES_VERSION {
        return Err(StorageError::UnsupportedServer);
    }
    Ok(())
}
