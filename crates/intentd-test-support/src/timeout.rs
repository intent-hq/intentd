//! The one test time-limit stretch every intentd test budget goes through.
//!
//! Coverage CI runs the suites under llvm-cov on oversubscribed runners and
//! exports `INTENTD_TEST_TIMEOUT_MULTIPLIER=3` so hang guards stretch with
//! the slower machine. A test that hard-codes `timeout(Duration::from_secs(5),
//! …)` ignores that setting and flakes there — on 2026-10-19 a fixed 5s note
//! page wait elapsed at 6.0–6.6s three times and held intentd#2512 out of the
//! merge queue until intentd#2514 scaled it. `fixed_deadline_lint` now fails
//! on a fixed test deadline that bypasses this helper.
//!
//! Six test modules used to carry private copies of this function with slightly
//! different rules; they now delegate here.

use std::time::Duration;

/// The environment variable coverage CI sets to stretch test budgets.
pub const TIMEOUT_MULTIPLIER_ENV: &str = "INTENTD_TEST_TIMEOUT_MULTIPLIER";

/// Highest stretch applied, so a typo such as `1e9` cannot turn a hang guard
/// into an effectively unbounded wait.
pub const MAX_TIMEOUT_MULTIPLIER: f64 = 20.0;

/// `base` stretched by `INTENTD_TEST_TIMEOUT_MULTIPLIER` (see
/// [`scale_test_timeout`] for the rules). Wrap every hang-guard budget in a
/// test — `tokio::time::timeout(test_timeout(Duration::from_secs(5)), …)`,
/// `Instant::now() + test_timeout(…)` — so slow coverage runs get more time
/// while ordinary runs keep the base.
#[must_use]
pub fn test_timeout(base: Duration) -> Duration {
    let multiplier = std::env::var(TIMEOUT_MULTIPLIER_ENV).ok();
    scale_test_timeout(base, multiplier.as_deref())
}

/// The stretch factor `INTENTD_TEST_TIMEOUT_MULTIPLIER` currently asks for,
/// under the rules of [`timeout_multiplier`] — for a test that scales a
/// budget itself (`base.mul_f64(test_timeout_multiplier())`).
#[must_use]
pub fn test_timeout_multiplier() -> f64 {
    timeout_multiplier(std::env::var(TIMEOUT_MULTIPLIER_ENV).ok().as_deref())
}

/// The stretch factor for a multiplier setting: unset, unparsable, or
/// non-finite means 1; values below 1 are raised to 1 (a budget is never
/// shortened); values above [`MAX_TIMEOUT_MULTIPLIER`] are capped.
#[must_use]
pub fn timeout_multiplier(setting: Option<&str>) -> f64 {
    setting
        .and_then(|value| value.trim().parse::<f64>().ok())
        .filter(|value| value.is_finite())
        .unwrap_or(1.0)
        .clamp(1.0, MAX_TIMEOUT_MULTIPLIER)
}

/// `base` stretched by [`timeout_multiplier`]`(multiplier)`; a product the
/// clock cannot represent saturates to [`Duration::MAX`] instead of
/// panicking.
#[must_use]
pub fn scale_test_timeout(base: Duration, multiplier: Option<&str>) -> Duration {
    Duration::try_from_secs_f64(base.as_secs_f64() * timeout_multiplier(multiplier))
        .unwrap_or(Duration::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    const FIVE: Duration = Duration::from_secs(5);

    #[test]
    fn default_and_malformed_settings_keep_the_base() {
        for multiplier in [None, Some(""), Some("bogus"), Some("NaN"), Some("inf")] {
            assert_eq!(scale_test_timeout(FIVE, multiplier), FIVE, "{multiplier:?}");
        }
    }

    #[test]
    fn coverage_multiplier_stretches_the_base() {
        assert_eq!(scale_test_timeout(FIVE, Some("3")), Duration::from_secs(15));
        assert_eq!(
            scale_test_timeout(FIVE, Some(" 3 ")),
            Duration::from_secs(15)
        );
        assert_eq!(
            scale_test_timeout(Duration::from_millis(500), Some("1.5")),
            Duration::from_millis(750)
        );
    }

    #[test]
    fn a_budget_is_never_shortened() {
        for multiplier in ["0", "0.1", "-1", "-inf"] {
            assert_eq!(
                scale_test_timeout(FIVE, Some(multiplier)),
                FIVE,
                "{multiplier}"
            );
        }
    }

    #[test]
    fn huge_multipliers_are_capped_and_huge_bases_saturate() {
        assert_eq!(
            scale_test_timeout(FIVE, Some("1e300")),
            Duration::from_secs(100)
        );
        assert_eq!(scale_test_timeout(Duration::MAX, Some("3")), Duration::MAX);
        assert_eq!(scale_test_timeout(Duration::MAX, None), Duration::MAX);
    }
}
