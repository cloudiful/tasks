//! Bounded exponential backoff for task retries.

use std::time::Duration;

use crate::model::RetryPolicy;

/// Returns the delay before the attempt that follows `attempt`.
///
/// `attempt` is the number of the attempt that just ended, starting at one.
/// The delay grows exponentially from the initial delay of `policy` and is
/// capped at its maximum delay.
pub fn retry_delay(policy: &RetryPolicy, attempt: u32) -> Duration {
    delay_for(policy.initial_delay(), policy.max_delay(), attempt)
}

/// Computes `initial * 2^(attempt - 1)` capped at `cap`.
///
/// The computation never overflows: the exponent is limited and the
/// multiplication saturates.
pub fn delay_for(initial: Duration, cap: Duration, attempt: u32) -> Duration {
    if attempt == 0 {
        return Duration::ZERO;
    }
    let exponent = attempt.saturating_sub(1).min(31);
    let factor = 1u32 << exponent;
    initial.saturating_mul(factor).min(cap)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delay_grows_exponentially_and_stays_capped() {
        let initial = Duration::from_secs(1);
        let cap = Duration::from_secs(10);

        assert_eq!(delay_for(initial, cap, 0), Duration::ZERO);
        assert_eq!(delay_for(initial, cap, 1), Duration::from_secs(1));
        assert_eq!(delay_for(initial, cap, 2), Duration::from_secs(2));
        assert_eq!(delay_for(initial, cap, 3), Duration::from_secs(4));
        assert_eq!(delay_for(initial, cap, 4), Duration::from_secs(8));
        assert_eq!(delay_for(initial, cap, 5), cap);
        assert_eq!(delay_for(initial, cap, 1000), cap);
    }

    #[test]
    fn delay_does_not_overflow() {
        let initial = Duration::from_secs(24 * 60 * 60);
        let cap = Duration::from_secs(24 * 60 * 60);

        assert_eq!(delay_for(initial, cap, u32::MAX), cap);
        assert_eq!(
            delay_for(Duration::MAX, Duration::MAX, u32::MAX),
            Duration::MAX
        );
    }

    #[test]
    fn policy_drives_the_delay() {
        let policy =
            RetryPolicy::new(4, Duration::from_millis(500), Duration::from_secs(4)).unwrap();

        assert_eq!(retry_delay(&policy, 1), Duration::from_millis(500));
        assert_eq!(retry_delay(&policy, 2), Duration::from_secs(1));
        assert_eq!(retry_delay(&policy, 3), Duration::from_secs(2));
        assert_eq!(retry_delay(&policy, 4), Duration::from_secs(4));
    }
}
