use llmproxy::{
    config::PostgresConfig,
    storage::{Storage, StorageError},
};
use std::time::Duration;

#[tokio::test]
async fn invalid_pool_settings_are_rejected_before_connecting() {
    let valid = || PostgresConfig {
        url: "postgres://localhost:1/unreachable".into(),
        namespace: "test".into(),
        max_connections: 1,
        connect_timeout: Duration::from_secs(1),
        acquire_timeout: Duration::from_secs(1),
    };
    for invalid in [
        PostgresConfig {
            max_connections: 0,
            ..valid()
        },
        PostgresConfig {
            connect_timeout: Duration::ZERO,
            ..valid()
        },
        PostgresConfig {
            acquire_timeout: Duration::ZERO,
            ..valid()
        },
        PostgresConfig {
            url: "postgres://[invalid".into(),
            ..valid()
        },
    ] {
        assert!(matches!(
            Storage::new(&invalid),
            Err(StorageError::InvalidData)
        ));
    }
}

#[test]
fn decoding_failures_discard_secret_driver_details() {
    let canary = "credential-canary-must-not-escape";
    for error in [
        sqlx::Error::Decode(std::io::Error::other(canary).into()),
        sqlx::Error::ColumnDecode {
            index: canary.into(),
            source: std::io::Error::other(canary).into(),
        },
    ] {
        let safe = StorageError::from(error);
        assert_eq!(safe, StorageError::InvalidData);
        assert!(!format!("{safe} {safe:?}").contains(canary));
    }
}
