//! Explicit opt-in retention of terminal tasks.

use std::time::Duration;

use crate::error::{Error, Result};
use crate::model::TaskStatus;

/// The largest number of tasks one manual prune or one retention cycle deletes.
pub const MAX_RETENTION_LIMIT: u32 = 1000;

/// The terminal age rules of one prune statement, in microseconds.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct RetentionAges {
    pub(crate) succeeded: Option<i64>,
    pub(crate) failed: Option<i64>,
    pub(crate) cancelled: Option<i64>,
}

impl RetentionAges {
    pub(crate) fn from_status(status: TaskStatus, micros: i64) -> Self {
        let mut ages = Self::default();
        match status {
            TaskStatus::Succeeded => ages.succeeded = Some(micros),
            TaskStatus::Failed => ages.failed = Some(micros),
            TaskStatus::Cancelled => ages.cancelled = Some(micros),
            _ => {}
        }
        ages
    }
}

/// Explicit opt-in retention of terminal tasks.
///
/// The configuration has no default and enables no status on its own. An
/// omitted status is never deleted by [`crate::Queue::run_retention`], and no
/// business retention age is supplied by the library. Ages and the interval
/// use microsecond precision; a positive duration below one microsecond is
/// refused because it would round to zero.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionConfig {
    interval: Duration,
    max_per_cycle: u32,
    succeeded: Option<Duration>,
    failed: Option<Duration>,
    cancelled: Option<Duration>,
}

impl RetentionConfig {
    /// Creates a configuration without an enabled terminal status.
    ///
    /// The interval must be positive and `max_per_cycle` must be between 1 and
    /// [`MAX_RETENTION_LIMIT`].
    pub fn new(interval: Duration, max_per_cycle: u32) -> Result<Self> {
        let config = Self {
            interval,
            max_per_cycle,
            succeeded: None,
            failed: None,
            cancelled: None,
        };
        config.validate_schedule()?;
        Ok(config)
    }

    /// Enables the retention of one terminal task status.
    ///
    /// Only `succeeded`, `failed` and `cancelled` are accepted. `older_than`
    /// must be positive and representable with microsecond precision.
    pub fn with_status(mut self, status: TaskStatus, older_than: Duration) -> Result<Self> {
        let status = validate_terminal_status(status)?;
        retention_micros(older_than, "older_than")?;
        match status {
            TaskStatus::Succeeded => self.succeeded = Some(older_than),
            TaskStatus::Failed => self.failed = Some(older_than),
            TaskStatus::Cancelled => self.cancelled = Some(older_than),
            _ => {}
        }
        Ok(self)
    }

    /// Validates the complete configuration.
    ///
    /// At least one terminal status must be enabled.
    pub fn validate(&self) -> Result<()> {
        self.validate_schedule()?;
        if self.succeeded.is_none() && self.failed.is_none() && self.cancelled.is_none() {
            return Err(Error::Validation {
                field: "retention",
                reason: "must enable at least one terminal status".to_owned(),
            });
        }
        Ok(())
    }

    /// The delay between retention cycles.
    pub fn interval(&self) -> Duration {
        self.interval
    }

    /// The largest total number of tasks one cycle deletes.
    pub fn max_per_cycle(&self) -> u32 {
        self.max_per_cycle
    }

    /// The retention age of one terminal status.
    pub fn ttl(&self, status: TaskStatus) -> Option<Duration> {
        match status {
            TaskStatus::Succeeded => self.succeeded,
            TaskStatus::Failed => self.failed,
            TaskStatus::Cancelled => self.cancelled,
            _ => None,
        }
    }

    pub(crate) fn ages(&self) -> Result<RetentionAges> {
        Ok(RetentionAges {
            succeeded: self
                .succeeded
                .map(|age| retention_micros(age, "succeeded_ttl"))
                .transpose()?,
            failed: self
                .failed
                .map(|age| retention_micros(age, "failed_ttl"))
                .transpose()?,
            cancelled: self
                .cancelled
                .map(|age| retention_micros(age, "cancelled_ttl"))
                .transpose()?,
        })
    }

    fn validate_schedule(&self) -> Result<()> {
        if self.interval.is_zero() {
            return Err(Error::Validation {
                field: "interval",
                reason: "must be positive".to_owned(),
            });
        }
        if self.max_per_cycle == 0 || self.max_per_cycle > MAX_RETENTION_LIMIT {
            return Err(Error::Validation {
                field: "max_per_cycle",
                reason: format!("must be between 1 and {MAX_RETENTION_LIMIT}"),
            });
        }
        Ok(())
    }
}

pub(crate) fn validate_terminal_status(status: TaskStatus) -> Result<TaskStatus> {
    if status.is_terminal() {
        Ok(status)
    } else {
        Err(Error::Validation {
            field: "status",
            reason: format!("must be succeeded, failed or cancelled, not {status}"),
        })
    }
}

pub(crate) fn retention_micros(duration: Duration, field: &'static str) -> Result<i64> {
    let micros = duration.as_micros();
    if micros == 0 {
        return Err(Error::Validation {
            field,
            reason: "must be at least 1 microsecond".to_owned(),
        });
    }
    if micros > i64::MAX as u128 {
        return Err(Error::Validation {
            field,
            reason: "must be representable in microseconds".to_owned(),
        });
    }
    Ok(micros as i64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_configuration_needs_a_schedule_and_a_policy() {
        assert!(RetentionConfig::new(Duration::ZERO, 1).is_err());
        assert!(RetentionConfig::new(Duration::from_secs(1), 0).is_err());
        assert!(RetentionConfig::new(Duration::from_secs(1), MAX_RETENTION_LIMIT + 1).is_err());
        assert!(RetentionConfig::new(Duration::from_secs(1), MAX_RETENTION_LIMIT).is_ok());

        let empty = RetentionConfig::new(Duration::from_secs(60), 100).unwrap();
        assert!(empty.validate().is_err());
    }

    #[test]
    fn only_terminal_statuses_can_be_enabled() {
        let config = RetentionConfig::new(Duration::from_secs(60), 100).unwrap();
        for status in [
            TaskStatus::Queued,
            TaskStatus::Running,
            TaskStatus::CancelRequested,
        ] {
            assert!(
                config
                    .clone()
                    .with_status(status, Duration::from_secs(60))
                    .is_err()
            );
        }
        let config = config
            .with_status(TaskStatus::Succeeded, Duration::from_secs(7 * 24 * 60 * 60))
            .unwrap()
            .with_status(TaskStatus::Failed, Duration::from_secs(30 * 24 * 60 * 60))
            .unwrap();
        assert!(config.validate().is_ok());
        assert_eq!(
            config.ttl(TaskStatus::Succeeded),
            Some(Duration::from_secs(604_800))
        );
        assert_eq!(
            config.ttl(TaskStatus::Failed),
            Some(Duration::from_secs(2_592_000))
        );
        assert_eq!(config.ttl(TaskStatus::Cancelled), None);
    }

    #[test]
    fn ages_keep_microsecond_precision_and_reject_zero_rounding() {
        let config = RetentionConfig::new(Duration::from_secs(60), 100).unwrap();
        assert!(
            config
                .clone()
                .with_status(TaskStatus::Cancelled, Duration::ZERO)
                .is_err()
        );
        assert!(
            config
                .clone()
                .with_status(TaskStatus::Cancelled, Duration::from_nanos(1))
                .is_err()
        );
        assert!(
            config
                .clone()
                .with_status(TaskStatus::Cancelled, Duration::from_secs(u64::MAX))
                .is_err()
        );
        let config = config
            .with_status(TaskStatus::Cancelled, Duration::from_micros(1_500))
            .unwrap();
        assert_eq!(
            config.ages().unwrap().cancelled,
            Some(1_500),
            "a positive sub-second age keeps its microseconds"
        );
    }
}
