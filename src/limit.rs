//! Request holding: token buckets (requests/min, tokens/min) and concurrency caps.
//!
//! Waiters queue FIFO on a tokio `Mutex`; the holder sleeps until its budget exists, so a
//! request that is cancelled or times out while waiting takes nothing.

use std::{sync::Arc, time::Duration};

use tokio::{
    sync::{Mutex, OwnedSemaphorePermit, Semaphore},
    time::{Instant, sleep},
};

use crate::config::{ConfigError, LimitConfig};

const SECS_PER_MINUTE: f64 = 60.0;

struct Bucket {
    capacity: f64,
    per_sec: f64,
    level: f64,
    at: Instant,
}

impl Bucket {
    fn new(per_minute: u32, burst: u32) -> Self {
        let capacity = f64::from(per_minute) + f64::from(burst);
        Self {
            capacity,
            per_sec: f64::from(per_minute) / SECS_PER_MINUTE,
            level: capacity,
            at: Instant::now(),
        }
    }

    fn refill(&mut self) {
        let now = Instant::now();
        self.level = (now - self.at)
            .as_secs_f64()
            .mul_add(self.per_sec, self.level)
            .min(self.capacity);
        self.at = now;
    }

    /// Cost beyond capacity is clamped, so an oversized request waits for a full bucket instead of forever.
    fn wait(&mut self, cost: f64) -> Duration {
        self.refill();
        let missing = cost.min(self.capacity) - self.level;
        if missing <= 0.0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(missing / self.per_sec)
        }
    }

    fn take(&mut self, cost: f64) {
        self.refill();
        self.level -= cost.min(self.capacity);
    }
}

#[derive(Default)]
struct Rate {
    requests: Option<Bucket>,
    tokens: Option<Bucket>,
}

impl Rate {
    fn wait(&mut self, tokens: f64) -> Duration {
        let requests = self.requests.as_mut().map(|b| b.wait(1.0));
        let tokens = self.tokens.as_mut().map(|b| b.wait(tokens));
        requests.max(tokens).unwrap_or_default()
    }

    fn take(&mut self, tokens: f64) {
        if let Some(b) = &mut self.requests {
            b.take(1.0);
        }
        if let Some(b) = &mut self.tokens {
            b.take(tokens);
        }
    }
}

struct Limit {
    model: Option<String>,
    prefixes: Vec<String>,
    rate: Mutex<Rate>,
    inflight: Option<Arc<Semaphore>>,
}

impl Limit {
    fn new(cfg: &LimitConfig) -> Result<Self, ConfigError> {
        let name = &cfg.name;
        if cfg.model.is_none() && cfg.prefixes.is_empty() {
            return Err(ConfigError(format!(
                "limit {name}: set `model` or `prefixes`"
            )));
        }
        if cfg.rpm.is_none() && cfg.tpm.is_none() && cfg.max_inflight.is_none() {
            return Err(ConfigError(format!(
                "limit {name}: set `rpm`, `tpm` or `max_inflight`"
            )));
        }
        if cfg.rpm == Some(0) || cfg.tpm == Some(0) || cfg.max_inflight == Some(0) {
            return Err(ConfigError(format!(
                "limit {name}: rpm, tpm and max_inflight must be above 0"
            )));
        }
        Ok(Self {
            model: cfg.model.clone(),
            prefixes: cfg.prefixes.clone(),
            rate: Mutex::new(Rate {
                requests: cfg.rpm.map(|rpm| Bucket::new(rpm, cfg.burst)),
                tokens: cfg.tpm.map(|tpm| Bucket::new(tpm, cfg.tpm_burst)),
            }),
            inflight: cfg.max_inflight.map(|n| Arc::new(Semaphore::new(n))),
        })
    }

    fn matches(&self, path: &str, model: Option<&str>) -> bool {
        self.model.as_deref().is_none_or(|m| model == Some(m))
            && (self.prefixes.is_empty() || self.prefixes.iter().any(|p| path.starts_with(p)))
    }

    async fn acquire(&self, tokens: f64) -> Option<OwnedSemaphorePermit> {
        let permit = match &self.inflight {
            #[allow(
                clippy::expect_used,
                reason = "the semaphore is owned here and never closed"
            )]
            Some(s) => Some(
                s.clone()
                    .acquire_owned()
                    .await
                    .expect("semaphore is never closed"),
            ),
            None => None,
        };
        let mut rate = self.rate.lock().await;
        let wait = rate.wait(tokens);
        sleep(wait).await;
        rate.take(tokens);
        permit
    }
}

pub struct Limits {
    limits: Vec<Limit>,
    by_model: bool,
}

impl Limits {
    /// # Errors
    /// Returns a [`ConfigError`] naming the first limit that matches nothing, limits nothing, or
    /// has a zero rate or cap.
    pub fn new(configs: &[LimitConfig]) -> Result<Self, ConfigError> {
        Ok(Self {
            limits: configs.iter().map(Limit::new).collect::<Result<_, _>>()?,
            by_model: configs.iter().any(|c| c.model.is_some()),
        })
    }

    /// Whether any limit needs the request's `model`, i.e. the body must be parsed.
    #[must_use]
    pub const fn by_model(&self) -> bool {
        self.by_model
    }

    /// Waits, in config order, for every matching limit. The returned permits cap concurrency
    /// until dropped. Cancel-safe: dropping the future gives back anything not yet taken.
    pub async fn acquire(
        &self,
        path: &str,
        model: Option<&str>,
        tokens: usize,
    ) -> Vec<OwnedSemaphorePermit> {
        #[allow(
            clippy::cast_precision_loss,
            reason = "token estimates are far below 2^52"
        )]
        let tokens = tokens as f64;
        let mut permits = Vec::new();
        for limit in self.limits.iter().filter(|l| l.matches(path, model)) {
            permits.extend(limit.acquire(tokens).await);
        }
        permits
    }
}
