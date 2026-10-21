use llmproxy::{
    limit::shared::{AccountingError, LeaseLost, LeaseStreamError, SharedAdmissionError},
    secret::SecretError,
    storage::StorageError,
};
use std::{collections::HashSet, error::Error, time::Duration};

#[test]
fn public_failures_have_distinct_bounded_diagnostics_without_inner_errors() {
    // Given every public redacted failure category.
    let errors: Vec<Box<dyn Error>> = vec![
        Box::new(SecretError::Randomness),
        Box::new(SecretError::InvalidToken),
        Box::new(SecretError::InvalidHash),
        Box::new(SecretError::Busy),
        Box::new(SecretError::Worker),
        Box::new(SecretError::InvalidKey),
        Box::new(SecretError::KeyFile),
        Box::new(SecretError::UnknownKey),
        Box::new(SecretError::UnsupportedVersion),
        Box::new(SecretError::Authentication),
        Box::new(StorageError::Unavailable),
        Box::new(StorageError::IncompatibleSchema),
        Box::new(StorageError::UnsupportedServer),
        Box::new(StorageError::MigrationFailed),
        Box::new(StorageError::Conflict),
        Box::new(StorageError::InvalidData),
        Box::new(StorageError::NotFound),
        Box::new(AccountingError::ZeroRate),
        Box::new(AccountingError::InvalidBalance),
        Box::new(AccountingError::InsufficientBudget),
        Box::new(LeaseLost),
    ];
    // When callers render these failures for an operator.
    let messages: HashSet<_> = errors.iter().map(ToString::to_string).collect();
    // Then each category remains actionable without leaking an inner driver error.
    assert_eq!(messages.len(), errors.len());
    assert!(
        messages
            .iter()
            .all(|text| !text.is_empty() && text.len() < 128)
    );
    assert!(errors.iter().all(|error| error.source().is_none()));
}

#[test]
fn admission_retry_metadata_distinguishes_timeout_from_storage_failure() {
    // Given a queue deadline and an unavailable database.
    let wait = Duration::from_secs(3);
    let timeout = SharedAdmissionError::QueueTimeout(wait);
    let unavailable = SharedAdmissionError::from(StorageError::Unavailable);
    // When a caller classifies the failures for an HTTP retry response.
    assert_eq!(timeout.retry_after(), Some(wait));
    assert_eq!(unavailable.retry_after(), None);
    // Then safe formatting preserves the failure category.
    assert_eq!(timeout.to_string(), "queue_timeout");
    assert_eq!(
        unavailable.to_string(),
        StorageError::Unavailable.to_string()
    );
}

#[test]
fn stream_error_preserves_upstream_failure_or_lease_loss() {
    // Given both possible stream termination causes.
    let upstream = LeaseStreamError::Upstream(std::io::Error::from(std::io::ErrorKind::TimedOut));
    let lost = LeaseStreamError::<std::io::Error>::LeaseLost(LeaseLost);
    // When the errors cross the stream's display boundary.
    assert_eq!(
        upstream.to_string(),
        std::io::Error::from(std::io::ErrorKind::TimedOut).to_string()
    );
    assert_eq!(lost.to_string(), LeaseLost.to_string());
}
