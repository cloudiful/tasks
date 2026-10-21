//! Validated runtime configuration.

use std::time::Duration;

use crate::error::{Error, Result};

/// Default lease time to live.
pub const DEFAULT_LEASE_TTL: Duration = Duration::from_secs(90);

/// Default interval between lease renewals.
pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);

/// Default number of consecutive heartbeat failures that are tolerated.
pub const DEFAULT_HEARTBEAT_FAILURE_BUDGET: u32 = 3;

/// Default interval between cancellation checks.
pub const DEFAULT_CANCEL_POLL_INTERVAL: Duration = Duration::from_secs(3);

/// Default interval between recovery passes.
pub const DEFAULT_RECOVERY_INTERVAL: Duration = Duration::from_secs(15);

/// Default interval used when no wake up notification arrives.
pub const DEFAULT_IDLE_POLL_INTERVAL: Duration = Duration::from_secs(30);

/// Default grace period a worker waits for running handlers on shutdown.
pub const DEFAULT_SHUTDOWN_GRACE: Duration = Duration::from_secs(30);

/// Default number of tasks a worker runs concurrently.
pub const DEFAULT_CONCURRENCY: usize = 1;

/// Upper bound of the worker concurrency.
pub const MAX_CONCURRENCY: usize = 1024;

/// Configuration of the worker runtime.
///
/// The defaults are validated by [`WorkerConfig::validate`], which every
/// constructor of the runtime calls before it accepts work.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerConfig {
    /// Number of tasks a worker runs concurrently.
    pub concurrency: usize,
    /// Lifetime of a lease. A lease that expires makes the attempt
    /// unrecoverable and lets recovery seal it.
    pub lease_ttl: Duration,
    /// Interval between lease renewals. Must stay below the lease time to
    /// live.
    pub heartbeat_interval: Duration,
    /// Number of consecutive heartbeat failures that are tolerated before the
    /// handler is cancelled. The budget must leave a margin below the lease
    /// time to live.
    pub heartbeat_failure_budget: u32,
    /// Interval between cancellation checks.
    pub cancel_poll_interval: Duration,
    /// Interval between recovery passes over expired leases.
    pub recovery_interval: Duration,
    /// Interval used when no wake up notification arrives.
    pub idle_poll_interval: Duration,
    /// Time a worker waits for running handlers when it shuts down.
    pub shutdown_grace: Duration,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            concurrency: DEFAULT_CONCURRENCY,
            lease_ttl: DEFAULT_LEASE_TTL,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            heartbeat_failure_budget: DEFAULT_HEARTBEAT_FAILURE_BUDGET,
            cancel_poll_interval: DEFAULT_CANCEL_POLL_INTERVAL,
            recovery_interval: DEFAULT_RECOVERY_INTERVAL,
            idle_poll_interval: DEFAULT_IDLE_POLL_INTERVAL,
            shutdown_grace: DEFAULT_SHUTDOWN_GRACE,
        }
    }
}

impl WorkerConfig {
    /// Validates the configuration.
    pub fn validate(&self) -> Result<()> {
        if self.concurrency == 0 {
            return Err(Error::Validation {
                field: "concurrency",
                reason: "must be at least 1".to_owned(),
            });
        }
        if self.concurrency > MAX_CONCURRENCY {
            return Err(Error::Validation {
                field: "concurrency",
                reason: format!("must be at most {MAX_CONCURRENCY}"),
            });
        }
        if self.lease_ttl.is_zero() {
            return Err(Error::Validation {
                field: "lease_ttl",
                reason: "must be positive".to_owned(),
            });
        }
        if self.heartbeat_interval.is_zero() {
            return Err(Error::Validation {
                field: "heartbeat_interval",
                reason: "must be positive".to_owned(),
            });
        }
        if self.heartbeat_interval >= self.lease_ttl {
            return Err(Error::Validation {
                field: "heartbeat_interval",
                reason: "must be shorter than lease_ttl".to_owned(),
            });
        }
        if self.heartbeat_failure_budget == 0 {
            return Err(Error::Validation {
                field: "heartbeat_failure_budget",
                reason: "must be at least 1".to_owned(),
            });
        }
        let budget_window = self
            .heartbeat_interval
            .checked_mul(self.heartbeat_failure_budget)
            .ok_or(Error::Validation {
                field: "heartbeat_interval",
                reason: "the heartbeat budget overflows".to_owned(),
            })?;
        if budget_window >= self.lease_ttl {
            return Err(Error::Validation {
                field: "heartbeat_failure_budget",
                reason: "the heartbeat budget must leave a margin below lease_ttl".to_owned(),
            });
        }
        if self.cancel_poll_interval.is_zero() {
            return Err(Error::Validation {
                field: "cancel_poll_interval",
                reason: "must be positive".to_owned(),
            });
        }
        if self.cancel_poll_interval > self.lease_ttl {
            return Err(Error::Validation {
                field: "cancel_poll_interval",
                reason: "must not exceed lease_ttl".to_owned(),
            });
        }
        if self.recovery_interval.is_zero() {
            return Err(Error::Validation {
                field: "recovery_interval",
                reason: "must be positive".to_owned(),
            });
        }
        if self.recovery_interval >= self.lease_ttl {
            return Err(Error::Validation {
                field: "recovery_interval",
                reason: "must be shorter than lease_ttl".to_owned(),
            });
        }
        if self.idle_poll_interval.is_zero() {
            return Err(Error::Validation {
                field: "idle_poll_interval",
                reason: "must be positive".to_owned(),
            });
        }
        if self.shutdown_grace.is_zero() {
            return Err(Error::Validation {
                field: "shutdown_grace",
                reason: "must be positive".to_owned(),
            });
        }
        Ok(())
    }

    /// The smallest connection pool a worker should use.
    ///
    /// The pool must serve the running handlers plus the claim and heartbeat
    /// traffic.
    pub fn min_pool_capacity(&self) -> usize {
        self.concurrency + 2
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_valid() {
        let config = WorkerConfig::default();

        assert!(config.validate().is_ok());
        assert_eq!(config.min_pool_capacity(), 3);
    }

    #[test]
    fn heartbeat_must_stay_below_the_lease() {
        let lease_ttl = WorkerConfig::default().lease_ttl;
        let config = WorkerConfig {
            heartbeat_interval: lease_ttl,
            ..WorkerConfig::default()
        };

        assert!(config.validate().is_err());
    }

    #[test]
    fn heartbeat_budget_must_leave_a_margin() {
        let config = WorkerConfig {
            heartbeat_interval: Duration::from_secs(30),
            heartbeat_failure_budget: 3,
            ..WorkerConfig::default()
        };

        assert!(config.validate().is_err());

        let config = WorkerConfig {
            heartbeat_failure_budget: 2,
            ..config
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn zero_and_oversized_values_are_rejected() {
        let config = WorkerConfig {
            concurrency: 0,
            ..WorkerConfig::default()
        };
        assert!(config.validate().is_err());

        let config = WorkerConfig {
            concurrency: MAX_CONCURRENCY + 1,
            ..WorkerConfig::default()
        };
        assert!(config.validate().is_err());

        let config = WorkerConfig {
            lease_ttl: Duration::ZERO,
            ..WorkerConfig::default()
        };
        assert!(config.validate().is_err());

        let lease_ttl = WorkerConfig::default().lease_ttl;
        let config = WorkerConfig {
            recovery_interval: lease_ttl,
            ..WorkerConfig::default()
        };
        assert!(config.validate().is_err());

        let config = WorkerConfig {
            shutdown_grace: Duration::ZERO,
            ..WorkerConfig::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn pool_capacity_follows_the_concurrency() {
        let config = WorkerConfig {
            concurrency: 8,
            ..WorkerConfig::default()
        };

        assert_eq!(config.min_pool_capacity(), 10);
    }
}
