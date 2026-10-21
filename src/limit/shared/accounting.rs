//! Exact fixed-point arithmetic for database-time admission.

use std::{fmt, num::NonZeroU32, time::Duration};

/// One token per minute produces one unit per database microsecond.
pub const UNITS_PER_TOKEN: u64 = 60_000_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountingError {
    ZeroRate,
    InvalidBalance,
    InsufficientBudget,
}

impl fmt::Display for AccountingError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ZeroRate => "shared rate must be positive",
            Self::InvalidBalance => "shared balance outside configured capacity",
            Self::InsufficientBudget => "shared budget unavailable",
        })
    }
}
impl std::error::Error for AccountingError {}

#[derive(Debug, Clone, Copy)]
pub struct FixedBucket {
    rate: NonZeroU32,
    capacity: u64,
    level: u64,
}

impl FixedBucket {
    /// Create a full bucket. Capacity is rate plus burst, in fixed-point units.
    /// # Errors
    /// Rejects a zero rate.
    pub fn new(per_minute: u32, burst: u32) -> Result<Self, AccountingError> {
        let rate = NonZeroU32::new(per_minute).ok_or(AccountingError::ZeroRate)?;
        let capacity = (u64::from(per_minute) + u64::from(burst)) * UNITS_PER_TOKEN;
        Ok(Self {
            rate,
            capacity,
            level: capacity,
        })
    }

    /// Load the integer balance from durable state.
    /// # Errors
    /// Rejects balances above capacity or negative values.
    pub fn restore(&mut self, available_units: i64) -> Result<(), AccountingError> {
        self.level = u64::try_from(available_units)
            .ok()
            .filter(|level| *level <= self.capacity)
            .ok_or(AccountingError::InvalidBalance)?;
        Ok(())
    }

    #[must_use]
    pub const fn available_units(&self) -> u64 {
        self.level
    }

    /// Elapsed time must come from database timestamps under the bucket row lock.
    /// Sub-microsecond time is truncated to `PostgreSQL` timestamp precision.
    pub fn refill(&mut self, database_elapsed: Duration) {
        let added = database_elapsed
            .as_micros()
            .saturating_mul(u128::from(self.rate.get()));
        let level = (u128::from(self.level) + added).min(u128::from(self.capacity));
        // The clamp bounds level by two u32 values times UNITS_PER_TOKEN, below u64::MAX.
        self.level = u64::try_from(level).unwrap_or(self.capacity);
    }

    fn cost(&self, tokens: usize) -> u64 {
        u64::try_from(tokens)
            .unwrap_or(u64::MAX)
            .saturating_mul(UNITS_PER_TOKEN)
            .min(self.capacity)
    }

    /// Conservative microsecond wait; oversized costs clamp to full capacity.
    #[must_use]
    pub fn wait(&self, tokens: usize) -> Duration {
        let missing = self.cost(tokens).saturating_sub(self.level);
        Duration::from_micros(missing.div_ceil(u64::from(self.rate.get())))
    }

    /// Charge once at admission. Retry and cancellation never refund this charge.
    /// # Errors
    /// Returns insufficient budget without changing the balance.
    pub fn charge(&mut self, tokens: usize) -> Result<(), AccountingError> {
        self.level = self
            .level
            .checked_sub(self.cost(tokens))
            .ok_or(AccountingError::InsufficientBudget)?;
        Ok(())
    }
}
