use llmproxy::{
    config::{Config, PostgresConfig},
    storage::{Storage, StorageError},
};
use std::time::Duration;

#[test]
fn empty_fingerprint_path_is_rejected_offline() {
    let text = "listen='127.0.0.1:0'\nproxy_token='test'\n[credential_encryption]\nactive_key_id='test'\nfingerprint_key_file=''\nkeys={test='/missing'}";
    let result = Config::parse(text, |_| None);
    assert!(result.is_err());
    assert!(
        result
            .err()
            .is_some_and(|error| error.to_string().contains("fingerprint_key_file"))
    );
}

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
