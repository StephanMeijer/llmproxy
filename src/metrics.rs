//! Bounded database and credential telemetry; no identity or secret enters this API.

use std::{
    collections::BTreeMap,
    sync::{Mutex, PoisonError},
};

/// Providers with credential storage, independent of configured route names.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum CredentialProvider {
    Anthropic,
}

impl CredentialProvider {
    const fn label(self) -> &'static str {
        match self {
            Self::Anthropic => "anthropic",
        }
    }
}

/// Current credential usability, including local decryption failures.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CredentialState {
    Active,
    Invalid,
    Removed,
    Unavailable,
}

impl CredentialState {
    const fn label(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Invalid => "invalid",
            Self::Removed => "removed",
            Self::Unavailable => "unavailable",
        }
    }
}

/// A complete authoritative count snapshot for one provider.
/// Each current credential belongs to exactly one state; undecryptable active
/// credentials count as unavailable, not both active and unavailable.
#[derive(Clone, Copy, Debug, Default)]
pub struct CredentialCounts {
    pub active: u64,
    pub invalid: u64,
    pub removed: u64,
    pub unavailable: u64,
}

/// Completed lifecycle operations; never inferred from an HTTP path or error.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum CredentialOperation {
    Install,
    Replace,
    Remove,
    Invalidate,
    Reencrypt,
}

impl CredentialOperation {
    const fn label(self) -> &'static str {
        match self {
            Self::Install => "install",
            Self::Replace => "replace",
            Self::Remove => "remove",
            Self::Invalidate => "invalidate",
            Self::Reencrypt => "reencrypt",
        }
    }
}

/// Redacted operation results, deliberately excluding error messages.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum CredentialOutcome {
    Success,
    Conflict,
    NotFound,
    Unavailable,
}

impl CredentialOutcome {
    const fn label(self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Conflict => "conflict",
            Self::NotFound => "not_found",
            Self::Unavailable => "unavailable",
        }
    }
}

#[derive(Default)]
struct Snapshot {
    postgres_available: bool,
    shared_limit_conflicts: u64,
    credentials: BTreeMap<CredentialProvider, CredentialCounts>,
    operations: BTreeMap<(CredentialProvider, CredentialOperation, CredentialOutcome), u64>,
}

/// Process-local telemetry updated by storage/credential lifecycle owners.
/// Labels are closed enums, so cardinality is bounded regardless of user data.
#[derive(Default)]
pub struct CredentialMetrics(Mutex<Snapshot>);

impl CredentialMetrics {
    /// Counts one Claude admission rejected by conflicting shared limit state.
    /// This process-local counter saturates at `u64::MAX`.
    pub fn record_shared_limit_conflict(&self) {
        let mut snapshot = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        snapshot.shared_limit_conflicts = snapshot.shared_limit_conflicts.saturating_add(1);
    }

    /// Records the latest database availability observation. Defaults to false
    /// until a database operation or health check succeeds. An outage preserves
    /// the last known credential counts; it does not report them as zero.
    pub fn set_postgres_available(&self, available: bool) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .postgres_available = available;
    }

    /// Replaces all counts for a provider from a consistent authoritative snapshot.
    /// Callers must serialize refreshes so an older query cannot overwrite newer
    /// counts. Repeated snapshots are idempotent. No counts are emitted before
    /// the first successful snapshot, and failed refreshes must leave it intact.
    pub fn set_credentials(&self, provider: CredentialProvider, counts: CredentialCounts) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .credentials
            .insert(provider, counts);
    }

    /// Counts one completed operation, after its durable result is known.
    /// The counter is local to this process, monotonic, and saturates at `u64::MAX`.
    pub fn record_operation(
        &self,
        provider: CredentialProvider,
        operation: CredentialOperation,
        outcome: CredentialOutcome,
    ) {
        let mut snapshot = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let count = snapshot
            .operations
            .entry((provider, operation, outcome))
            .or_default();
        *count = count.saturating_add(1);
        drop(snapshot);
    }

    pub(crate) fn render(&self) -> String {
        let snapshot = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let available = u8::from(snapshot.postgres_available);
        let shared_limit_conflicts = snapshot.shared_limit_conflicts;
        let credentials = snapshot.credentials.iter().flat_map(|(provider, counts)| {
            let provider = provider.label();
            [
                (CredentialState::Active, counts.active),
                (CredentialState::Invalid, counts.invalid),
                (CredentialState::Removed, counts.removed),
                (CredentialState::Unavailable, counts.unavailable),
            ].into_iter().map(move |(state, count)| {
                let state = state.label();
                format!(
                    "llm3_provider_credentials{{provider=\"{provider}\",state=\"{state}\"}} {count}\n"
                )
            })
        }).collect::<String>();
        let operations = snapshot.operations.iter().map(|((provider, operation, outcome), count)| {
            let (provider, operation, outcome) =
                (provider.label(), operation.label(), outcome.label());
            format!(
                "llm3_credential_operations_total{{provider=\"{provider}\",operation=\"{operation}\",outcome=\"{outcome}\"}} {count}\n"
            )
        }).collect::<Vec<_>>().concat();
        drop(snapshot);
        format!(
            "# HELP llm3_postgres_available Latest observed database availability (1 available, 0 unavailable or unobserved).\n\
             # TYPE llm3_postgres_available gauge\nllm3_postgres_available {available}\n\
             # HELP llm3_provider_credentials Last authoritative current credential counts by provider and state.\n\
             # TYPE llm3_provider_credentials gauge\n{credentials}\
             # HELP llm3_credential_operations_total Completed credential operations in this process.\n\
             # TYPE llm3_credential_operations_total counter\n{operations}\
             # HELP llm3_shared_limit_conflicts_total Claude admissions rejected by conflicting shared limit state in this process.\n\
             # TYPE llm3_shared_limit_conflicts_total counter\n\
             llm3_shared_limit_conflicts_total{{provider=\"anthropic\"}} {shared_limit_conflicts}\n"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{CredentialMetrics, Snapshot};
    use std::sync::Mutex;

    #[test]
    fn shared_limit_conflicts_saturate() {
        // Given a counter already at its representable maximum.
        let metrics = CredentialMetrics(Mutex::new(Snapshot {
            shared_limit_conflicts: u64::MAX,
            ..Snapshot::default()
        }));
        // When another conflict is observed.
        metrics.record_shared_limit_conflict();
        // Then the exposed counter remains monotonic.
        assert!(metrics.render().contains(&format!(
            "llm3_shared_limit_conflicts_total{{provider=\"anthropic\"}} {}\n",
            u64::MAX
        )));
    }
}
