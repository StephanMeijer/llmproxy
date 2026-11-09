//! Shared definition compilation and the repository admission boundary.

mod accounting;
mod admission;
mod concurrency;

pub use accounting::{AccountingError, FixedBucket, UNITS_PER_TOKEN};
pub use admission::{Admission, AdmissionStore, PendingAdmission, SharedCharge, SharedRequest};
pub use admission::{SharedAdmissionError, SharedRateStore};
pub use concurrency::{
    LeaseCancellation, LeaseCancellations, LeaseGuard, LeaseLost, LeaseStreamError, RENEW_EVERY,
};

use serde::Serialize;

use super::Limits;
use crate::config::{ConfigError, LimitConfig};
use crate::storage::Provider;

const ACCOUNTING_VERSION: u32 = 1;

/// Canonical behavior payload. Field order is fixed by this struct's serializer.
#[derive(Debug, Clone, Serialize)]
pub struct SharedDefinition {
    namespace: String,
    provider: Provider,
    /// The adapter's path prefix; not stored, so existing definitions keep matching.
    #[serde(skip)]
    prefix: &'static str,
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
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub const fn provider(&self) -> Provider {
        self.provider
    }

    #[must_use]
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    #[must_use]
    pub const fn max_inflight(&self) -> Option<usize> {
        self.max_inflight
    }

    #[must_use]
    pub fn matches(&self, path: &str, model: Option<&str>) -> bool {
        path.starts_with(self.prefix)
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

/// Local pools retain their static-path behavior; only Claude paths select shared definitions.
pub struct CompiledLimits {
    pub local: Limits,
    pub shared: Vec<SharedDefinition>,
}

impl CompiledLimits {
    /// Compile when an adapter is enabled; its `prefix` paths use shared definitions.
    /// Legacy callers keep [`Limits::new`].
    /// # Errors
    /// Returns existing limit validation errors or rejects duplicate shared names.
    pub fn new(
        configs: &[LimitConfig],
        namespace: &str,
        provider: Provider,
        prefix: &'static str,
    ) -> Result<Self, ConfigError> {
        let mut local = Limits::new(configs)?;
        local.shared_prefix = Some(prefix);
        let mut shared: Vec<SharedDefinition> = Vec::new();
        for (order, cfg) in configs.iter().enumerate() {
            let overlaps = cfg.prefixes.is_empty()
                || cfg
                    .prefixes
                    .iter()
                    .any(|p| prefix.starts_with(p) || p.starts_with(prefix));
            if !overlaps {
                continue;
            }
            if shared.iter().any(|definition| definition.name == cfg.name) {
                return Err(ConfigError("duplicate shared limit name".into()));
            }
            shared.push(SharedDefinition {
                namespace: namespace.to_owned(),
                provider,
                prefix,
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
