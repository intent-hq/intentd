//! Per-minute token-rate history behind `stats.getRateHistory` (§5.39):
//! UTC minute bucketing plus the zero-filled trailing-window read.
//!
//! Recording rides the existing turn-end usage-stats bookkeeping
//! (`agent_session.rs::record_turn_usage_stats`): the same clamped per-turn
//! token delta that feeds `usage_stats_hourly` (§5.36) is also folded into
//! the `usage_rate_minutely` store — never a raw cumulative snapshot, so the
//! two surfaces cannot disagree on what a "token" is. Everything here is
//! pure and unit-testable without a store.

use intent_core::{Error, Result};
use intent_store::UsageRateRow;
use serde_json::{json, Value};
use time::{Duration, OffsetDateTime, UtcOffset};

/// Default number of trailing minute samples returned by
/// `stats.getRateHistory` when the caller omits `limit`.
pub const DEFAULT_RATE_HISTORY_LIMIT: u32 = 60;

/// Upper bound on `limit`: the 24h retention window holds at most 1440
/// minute buckets, so larger windows could never be served.
pub const MAX_RATE_HISTORY_LIMIT: u32 = 1440;

/// Floor `t` to its UTC minute and render the bucket key used by
/// `usage_rate_minutely.bucket_utc`: `"YYYY-MM-DDTHH:MM:00Z"`. Keys sort
/// lexicographically in chronological order.
pub fn minute_bucket_utc(t: OffsetDateTime) -> String {
    let t = t.to_offset(UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:00Z",
        t.year(),
        u8::from(t.month()),
        t.day(),
        t.hour(),
        t.minute()
    )
}

/// Validate the wire `limit` param: absent → [`DEFAULT_RATE_HISTORY_LIMIT`];
/// out of range (< 1 or > [`MAX_RATE_HISTORY_LIMIT`]) is `InvalidParams`
/// (router surfaces `-32602`).
pub fn parse_limit(limit: Option<i64>) -> Result<u32> {
    match limit {
        None => Ok(DEFAULT_RATE_HISTORY_LIMIT),
        Some(n) if (1..=i64::from(MAX_RATE_HISTORY_LIMIT)).contains(&n) => Ok(n as u32),
        Some(n) => Err(Error::InvalidParams(format!(
            "limit must be between 1 and {MAX_RATE_HISTORY_LIMIT}, got {n}"
        ))),
    }
}

/// The inclusive window start for a `limit`-sample read ending at `now`'s
/// minute: `limit - 1` minutes before the current minute floor. Rows at or
/// after this key belong to the window.
pub fn window_start(now_utc: OffsetDateTime, limit: u32) -> String {
    minute_bucket_utc(now_utc - Duration::minutes(i64::from(limit) - 1))
}

/// Assemble the `stats.getRateHistory` result: exactly `limit` samples in
/// chronological order (oldest first), one per minute, ending at `now`'s
/// minute floor. Minutes without a persisted row are zero-filled; `rows`
/// outside the window are ignored (defensive — the store query already
/// filters). Empty stores return all-zero samples, never an error.
pub fn rate_history_json(rows: &[UsageRateRow], limit: u32, now_utc: OffsetDateTime) -> Value {
    let by_bucket: std::collections::BTreeMap<&str, &UsageRateRow> =
        rows.iter().map(|r| (r.bucket_utc.as_str(), r)).collect();
    let zero = UsageRateRow::default();
    let end = now_utc.to_offset(UtcOffset::UTC);
    let samples: Vec<Value> = (0..i64::from(limit))
        .map(|i| {
            let bucket = minute_bucket_utc(end - Duration::minutes(i64::from(limit) - 1 - i));
            let r = by_bucket.get(bucket.as_str()).copied().unwrap_or(&zero);
            json!({
                "bucketUtc": bucket,
                "inputTokens": r.input_tokens,
                "outputTokens": r.output_tokens,
                "cacheReadTokens": r.cache_read_tokens,
                "cacheCreationTokens": r.cache_creation_tokens,
            })
        })
        .collect();
    json!({ "samples": samples })
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::format_description::well_known::Rfc3339;

    fn parse(s: &str) -> OffsetDateTime {
        OffsetDateTime::parse(s, &Rfc3339).expect("parse test timestamp")
    }

    fn row(bucket: &str, input: u64, output: u64) -> UsageRateRow {
        UsageRateRow {
            bucket_utc: bucket.to_string(),
            input_tokens: input,
            output_tokens: output,
            ..Default::default()
        }
    }

    #[test]
    fn minute_bucket_floors_to_utc_minute() {
        assert_eq!(
            minute_bucket_utc(parse("2026-07-30T14:07:42.5Z")),
            "2026-07-30T14:07:00Z"
        );
        // Non-UTC inputs are converted before flooring.
        assert_eq!(
            minute_bucket_utc(parse("2026-07-30T07:07:59-07:00")),
            "2026-07-30T14:07:00Z"
        );
    }

    #[test]
    fn parse_limit_defaults_and_rejects_out_of_range() {
        assert_eq!(parse_limit(None).expect("default"), 60);
        assert_eq!(parse_limit(Some(1)).expect("min"), 1);
        assert_eq!(parse_limit(Some(1440)).expect("max"), 1440);
        for bad in [0, -1, 1441] {
            assert!(
                matches!(parse_limit(Some(bad)), Err(Error::InvalidParams(_))),
                "limit {bad} must be rejected"
            );
        }
    }

    #[test]
    fn window_start_is_limit_minus_one_minutes_back() {
        let now = parse("2026-07-30T14:07:30Z");
        assert_eq!(window_start(now, 60), "2026-07-30T13:08:00Z");
        assert_eq!(window_start(now, 1), "2026-07-30T14:07:00Z");
    }

    #[test]
    fn history_zero_fills_gaps_in_chronological_order() {
        let now = parse("2026-07-30T14:07:30Z");
        // Newest minute populated, one mid-window minute populated, the
        // rest missing; a row outside the window must be ignored.
        let rows = vec![
            row("2026-07-30T14:07:00Z", 100, 40),
            row("2026-07-30T14:05:00Z", 7, 2),
            row("2026-07-30T13:00:00Z", 999, 999),
        ];
        let v = rate_history_json(&rows, 5, now);
        let samples = v["samples"].as_array().expect("samples");
        assert_eq!(samples.len(), 5);
        assert_eq!(samples[0]["bucketUtc"], "2026-07-30T14:03:00Z");
        assert_eq!(samples[0]["inputTokens"], 0);
        assert_eq!(samples[2]["bucketUtc"], "2026-07-30T14:05:00Z");
        assert_eq!(samples[2]["inputTokens"], 7);
        assert_eq!(samples[2]["outputTokens"], 2);
        assert_eq!(samples[4]["bucketUtc"], "2026-07-30T14:07:00Z");
        assert_eq!(samples[4]["inputTokens"], 100);
        assert_eq!(samples[4]["outputTokens"], 40);
        assert_eq!(samples[4]["cacheReadTokens"], 0);
        assert_eq!(samples[4]["cacheCreationTokens"], 0);
        assert!(
            !samples.iter().any(|s| s["inputTokens"] == 999),
            "out-of-window row leaked into the history"
        );
    }

    #[test]
    fn history_on_empty_store_is_all_zeroed_samples() {
        let now = parse("2026-07-30T00:01:00Z");
        let v = rate_history_json(&[], 3, now);
        let samples = v["samples"].as_array().expect("samples");
        assert_eq!(samples.len(), 3);
        // The window crosses midnight backwards without issue.
        assert_eq!(samples[0]["bucketUtc"], "2026-07-29T23:59:00Z");
        assert_eq!(samples[2]["bucketUtc"], "2026-07-30T00:01:00Z");
        for s in samples {
            assert_eq!(s["inputTokens"], 0);
            assert_eq!(s["outputTokens"], 0);
            assert_eq!(s["cacheReadTokens"], 0);
            assert_eq!(s["cacheCreationTokens"], 0);
        }
    }
}
