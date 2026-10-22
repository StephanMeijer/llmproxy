//! Shared definition compilation and the repository admission boundary.

mod accounting;
mod admission;
mod concurrency;

pub use accounting::{AccountingError, FixedBucket, UNITS_PER_TOKEN};
pub use admission::{Admission, AdmissionStore, PendingAdmission, SharedCharge, SharedRequest};
pub use admission::{SharedAdmissionError, SharedRateStore};
pub use concurrency::{
    LeaseCancellation, LeaseCancellations, LeaseGuard, LeaseLost, LeaseStreamError,
};

use serde::Serialize;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use super::Limits;
use crate::config::{ConfigError, LimitConfig};

pub const CLAUDE_PREFIX: &str = "/claude/";
const PROVIDER: &str = "anthropic";
const ACCOUNTING_VERSION: u32 = 1;

/// Canonical behavior payload. Field order is fixed by this struct's serializer.
#[derive(Debug, Clone, Serialize)]
pub struct SharedDefinition {
    namespace: String,
    provider: &'static str,
    name: String,
    accounting_version: u32,
    order: usize,
    model: Option<String>,
    prefixes: Vec<String>,
    rpm: Option<u32>,
    burst: u32,
    tpm: Option<u32>,
    tpm_burst: u32,
    max_inflight: Option<usize>,
}

impl SharedDefinition {
    /// Stable, complete configuration bytes for storage and comparison.
    /// # Errors
    /// Returns a configuration error if serialization fails.
    pub fn canonical(&self) -> Result<Vec<u8>, ConfigError> {
        serde_json::to_vec(self)
            .map_err(|_| ConfigError("shared limit serialization failed".into()))
    }

    /// SHA-256 of the versioned canonical behavior payload.
    /// # Errors
    /// Returns a configuration error if serialization fails.
    pub fn fingerprint(&self) -> Result<[u8; 32], ConfigError> {
        Ok(Sha256::digest(self.canonical()?).into())
    }

    /// Reject a conflicting replica before shared admission.
    /// # Errors
    /// Returns a configuration conflict if the persisted fingerprint differs.
    pub fn verify(&self, persisted: &[u8]) -> Result<(), ConfigError> {
        if self.fingerprint()?.as_slice() != persisted {
            return Err(ConfigError("shared limit configuration conflict".into()));
        }
        Ok(())
    }

    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    #[must_use]
    pub const fn max_inflight(&self) -> Option<usize> {
        self.max_inflight
    }

    /// A budget identity deliberately excludes credential generation.
    #[must_use]
    pub fn key(&self, person: Uuid) -> SharedKey<'_> {
        SharedKey {
            namespace: &self.namespace,
            person,
            provider: self.provider,
            name: &self.name,
        }
    }

    #[must_use]
    pub fn matches(&self, path: &str, model: Option<&str>) -> bool {
        path.starts_with(CLAUDE_PREFIX)
            && self.model.as_deref().is_none_or(|m| model == Some(m))
            && (self.prefixes.is_empty() || self.prefixes.iter().any(|p| path.starts_with(p)))
    }

    /// Initial full request and token buckets, respectively.
    /// # Errors
    /// Returns an accounting error for an invalid rate.
    pub fn buckets(&self) -> Result<(Option<FixedBucket>, Option<FixedBucket>), AccountingError> {
        Ok((
            self.rpm
                .map(|n| FixedBucket::new(n, self.burst))
                .transpose()?,
            self.tpm
                .map(|n| FixedBucket::new(n, self.tpm_burst))
                .transpose()?,
        ))
    }
}

/// Namespace and person partition all durable budgets; provider/name identify the pool.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SharedKey<'a> {
    pub namespace: &'a str,
    pub person: Uuid,
    pub provider: &'a str,
    pub name: &'a str,
}

/// Local pools retain their static-path behavior; only Claude paths select shared definitions.
pub struct CompiledLimits {
    pub local: Limits,
    pub shared: Vec<SharedDefinition>,
}

impl CompiledLimits {
    /// Compile when the Claude adapter is enabled. Legacy callers keep [`Limits::new`].
    /// # Errors
    /// Returns existing limit validation errors or rejects duplicate shared names.
    pub fn new(configs: &[LimitConfig], namespace: &str) -> Result<Self, ConfigError> {
        let mut local = Limits::new(configs)?;
        local.exclude_claude = true;
        let mut shared: Vec<SharedDefinition> = Vec::new();
        for (order, cfg) in configs.iter().enumerate() {
            let overlaps = cfg.prefixes.is_empty()
                || cfg
                    .prefixes
                    .iter()
                    .any(|p| CLAUDE_PREFIX.starts_with(p) || p.starts_with(CLAUDE_PREFIX));
            if !overlaps {
                continue;
            }
            if shared.iter().any(|definition| definition.name == cfg.name) {
                return Err(ConfigError("duplicate shared limit name".into()));
            }
            shared.push(SharedDefinition {
                namespace: namespace.to_owned(),
                provider: PROVIDER,
                name: cfg.name.clone(),
                accounting_version: ACCOUNTING_VERSION,
                order,
                model: cfg.model.clone(),
                prefixes: cfg.prefixes.clone(),
                rpm: cfg.rpm,
                burst: cfg.burst,
                tpm: cfg.tpm,
                tpm_burst: cfg.tpm_burst,
                max_inflight: cfg.max_inflight,
            });
        }
        Ok(Self { local, shared })
    }
}
