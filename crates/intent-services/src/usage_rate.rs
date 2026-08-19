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
use intent_store::{UsageRateDelta, UsageRateRow};
use serde_json::{json, Value};
use time::{Duration, OffsetDateTime, UtcOffset};

/// Default number of trailing minute samples returned by
/// `stats.getRateHistory` when the caller omits `limit`.
pub(crate) const DEFAULT_RATE_HISTORY_LIMIT: u32 = 60;

/// Upper bound on `limit`: the 24h retention window holds at most 1440
/// minute buckets, so larger windows could never be served.
pub(crate) const MAX_RATE_HISTORY_LIMIT: u32 = 1440;

/// Floor `t` to its UTC minute and render the bucket key used by
/// `usage_rate_minutely.bucket_utc`: `"YYYY-MM-DDTHH:MM:00Z"`. Keys sort
/// lexicographically in chronological order.
pub(crate) fn minute_bucket_utc(t: OffsetDateTime) -> String {
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

/// Spread one turn's [`UsageRateDelta`] evenly across every UTC minute bucket
/// the turn spanned — `[turn_end − turn_duration, turn_end]`, inclusive, floored
/// to UTC minutes — instead of folding it all into the turn-end minute. Buckets
/// are returned oldest-first; the newest is always `turn_end`'s minute.
///
/// Each counter is split independently by integer division, and
/// the remainder is distributed one token per bucket to the EARLIEST buckets,
/// so the per-counter sum across the returned parts exactly equals the input
/// counter (no tokens lost or invented).
///
/// A sub-minute turn that stays within one minute (or a zero-duration turn)
/// yields a single bucket, identical to the pre-spread behaviour. The bucket
/// count is capped at [`MAX_RATE_HISTORY_LIMIT`] so a pathologically long
/// duration cannot explode the loop or outrun the 24h retention window.
///
/// Pure: callers skip all-zero parts, and the store accumulates additively when
/// two turns overlap the same minute.
pub(crate) fn split_delta_across_minutes(
    turn_end: OffsetDateTime,
    turn_duration: std::time::Duration,
    delta: &UsageRateDelta,
) -> Vec<(String, UsageRateDelta)> {
    let end = turn_end.to_offset(UtcOffset::UTC);
    // Cap the span up front so the bucket count can neither overflow nor exceed
    // retention, then subtract at full (sub-second) precision: a turn ending at
    // HH:MM:00.xxx after e.g. 60.2s genuinely touched the *previous* minute, so
    // truncating fractional seconds off either the duration or the end instant
    // (as whole-second arithmetic would) must not drop that minute. (A
    // std::time::Duration is never negative.)
    let max_span = std::time::Duration::from_secs(u64::from(MAX_RATE_HISTORY_LIMIT) * 60);
    let span = Duration::try_from(turn_duration.min(max_span)).unwrap_or(Duration::ZERO);
    let start = end - span;
    // Floor both instants to their UTC minute. `unix_timestamp()` drops the
    // always-non-negative fractional second, i.e. floors to the whole second.
    let end_min = end.unix_timestamp().div_euclid(60);
    let start_min = start.unix_timestamp().div_euclid(60);
    let m = ((end_min - start_min) + 1).clamp(1, i64::from(MAX_RATE_HISTORY_LIMIT)) as usize;

    let split = |total: u64| -> (u64, u64) { (total / m as u64, total % m as u64) };
    let (in_base, in_rem) = split(delta.input_tokens);
    let (out_base, out_rem) = split(delta.output_tokens);
    let (cr_base, cr_rem) = split(delta.cache_read_tokens);
    let (cc_base, cc_rem) = split(delta.cache_creation_tokens);
    let (th_base, th_rem) = split(delta.thought_tokens);

    (0..m)
        .map(|i| {
            // Earliest `rem` buckets each receive one extra token.
            let extra = |rem: u64| u64::from((i as u64) < rem);
            let bucket = minute_bucket_utc(end - Duration::minutes((m - 1 - i) as i64));
            let part = UsageRateDelta {
                input_tokens: in_base + extra(in_rem),
                output_tokens: out_base + extra(out_rem),
                cache_read_tokens: cr_base + extra(cr_rem),
                cache_creation_tokens: cc_base + extra(cc_rem),
                thought_tokens: th_base + extra(th_rem),
            };
            (bucket, part)
        })
        .collect()
}

/// Validate the wire `limit` param: absent → [`DEFAULT_RATE_HISTORY_LIMIT`];
/// out of range (< 1 or > [`MAX_RATE_HISTORY_LIMIT`]) is `InvalidParams`
/// (router surfaces `-32602`).
pub(crate) fn parse_limit(limit: Option<i64>) -> Result<u32> {
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
pub(crate) fn rate_history_json(
    rows: &[UsageRateRow],
    limit: u32,
    now_utc: OffsetDateTime,
) -> Value {
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
                "thoughtTokens": r.thought_tokens,
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

    fn delta(
        input: u64,
        output: u64,
        cache_read: u64,
        cache_creation: u64,
        thought: u64,
    ) -> UsageRateDelta {
        UsageRateDelta {
            input_tokens: input,
            output_tokens: output,
            cache_read_tokens: cache_read,
            cache_creation_tokens: cache_creation,
            thought_tokens: thought,
        }
    }

    #[test]
    fn split_single_minute_turn_yields_one_bucket() {
        let end = parse("2026-07-30T14:07:42Z");
        let d = delta(100, 40, 7, 3, 12);
        // Zero-duration and a sub-minute turn wholly inside 14:07 both collapse
        // to the single end-minute bucket carrying the full delta.
        for dur in [
            std::time::Duration::ZERO,
            std::time::Duration::from_secs(30),
        ] {
            let parts = split_delta_across_minutes(end, dur, &d);
            assert_eq!(parts.len(), 1, "dur {dur:?}");
            assert_eq!(parts[0].0, "2026-07-30T14:07:00Z");
            assert_eq!(parts[0].1, d);
        }
    }

    #[test]
    fn split_sub_minute_turn_crossing_a_boundary_spans_two_buckets() {
        // 14:06:50 → 14:07:10 touches both minutes despite lasting 20s.
        let end = parse("2026-07-30T14:07:10Z");
        let parts = split_delta_across_minutes(
            end,
            std::time::Duration::from_secs(20),
            &delta(10, 0, 0, 0, 0),
        );
        let buckets: Vec<&str> = parts.iter().map(|(b, _)| b.as_str()).collect();
        assert_eq!(buckets, ["2026-07-30T14:06:00Z", "2026-07-30T14:07:00Z"]);
    }

    #[test]
    fn split_counts_the_minute_touched_by_fractional_seconds() {
        // Ending at 14:08:00.100 after 60.2s means the turn started at
        // 14:06:59.900 — it genuinely touched 14:06. Whole-second arithmetic on
        // a truncated end/duration would compute only 14:07–14:08 and drop it.
        let end = parse("2026-07-30T14:08:00.100Z");
        let parts = split_delta_across_minutes(
            end,
            std::time::Duration::from_millis(60_200),
            &delta(9, 0, 0, 0, 0),
        );
        let buckets: Vec<&str> = parts.iter().map(|(b, _)| b.as_str()).collect();
        assert_eq!(
            buckets,
            [
                "2026-07-30T14:06:00Z",
                "2026-07-30T14:07:00Z",
                "2026-07-30T14:08:00Z",
            ]
        );
        // Conservation holds across the true span (9 / 3 = 3 each).
        let total: u64 = parts.iter().map(|(_, p)| p.input_tokens).sum();
        assert_eq!(total, 9);
    }

    #[test]
    fn split_multi_minute_even_and_chronological() {
        // 14:04:30 → 14:07:30 spans minutes 14:04..=14:07 → 4 buckets.
        let end = parse("2026-07-30T14:07:30Z");
        let parts = split_delta_across_minutes(
            end,
            std::time::Duration::from_secs(180),
            &delta(40, 0, 0, 0, 0),
        );
        let buckets: Vec<&str> = parts.iter().map(|(b, _)| b.as_str()).collect();
        assert_eq!(
            buckets,
            [
                "2026-07-30T14:04:00Z",
                "2026-07-30T14:05:00Z",
                "2026-07-30T14:06:00Z",
                "2026-07-30T14:07:00Z",
            ]
        );
        // 40 / 4 = 10 with no remainder → perfectly even.
        for (_, p) in &parts {
            assert_eq!(p.input_tokens, 10);
        }
    }

    #[test]
    fn split_remainder_goes_to_earliest_buckets_and_conserves_each_counter() {
        let end = parse("2026-07-30T14:07:30Z");
        // Non-divisible per counter: input 13 (base 3, rem 1), output 14
        // (base 3, rem 2), cache_read 4 (base 1, rem 0), cache_creation 5,
        // thought 6 (base 1, rem 2).
        let d = delta(13, 14, 4, 5, 6);
        let parts = split_delta_across_minutes(end, std::time::Duration::from_secs(180), &d);
        assert_eq!(parts.len(), 4);
        // Earliest bucket absorbs the +1 tokens.
        assert_eq!(parts[0].1, delta(4, 4, 1, 2, 2));
        assert_eq!(parts[3].1, delta(3, 3, 1, 1, 1));
        // Every counter's parts sum back to the original — nothing lost/invented.
        let sum = parts
            .iter()
            .fold(UsageRateDelta::default(), |mut acc, (_, p)| {
                acc.input_tokens += p.input_tokens;
                acc.output_tokens += p.output_tokens;
                acc.cache_read_tokens += p.cache_read_tokens;
                acc.cache_creation_tokens += p.cache_creation_tokens;
                acc.thought_tokens += p.thought_tokens;
                acc
            });
        assert_eq!(sum, d);
    }

    #[test]
    fn split_caps_bucket_count_at_retention_limit() {
        let end = parse("2026-07-30T14:07:30Z");
        // A pathologically long turn (100 days) must not explode the loop.
        let dur = std::time::Duration::from_secs(100 * 24 * 60 * 60);
        let parts = split_delta_across_minutes(end, dur, &delta(10_000, 0, 0, 0, 0));
        assert_eq!(parts.len(), MAX_RATE_HISTORY_LIMIT as usize);
        // Even capped, the counter is fully conserved across the buckets.
        let total: u64 = parts.iter().map(|(_, p)| p.input_tokens).sum();
        assert_eq!(total, 10_000);
        // Newest bucket is still the turn-end minute.
        assert_eq!(parts.last().expect("last").0, "2026-07-30T14:07:00Z");
    }

    fn row(bucket: &str, input: u64, output: u64, thought: u64) -> UsageRateRow {
        UsageRateRow {
            bucket_utc: bucket.to_string(),
            input_tokens: input,
            output_tokens: output,
            thought_tokens: thought,
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
            row("2026-07-30T14:07:00Z", 100, 40, 15),
            row("2026-07-30T14:05:00Z", 7, 2, 0),
            row("2026-07-30T13:00:00Z", 999, 999, 999),
        ];
        let v = rate_history_json(&rows, 5, now);
        let samples = v["samples"].as_array().expect("samples");
        assert_eq!(samples.len(), 5);
        assert_eq!(samples[0]["bucketUtc"], "2026-07-30T14:03:00Z");
        assert_eq!(samples[0]["inputTokens"], 0);
        assert_eq!(samples[0]["thoughtTokens"], 0);
        assert_eq!(samples[2]["bucketUtc"], "2026-07-30T14:05:00Z");
        assert_eq!(samples[2]["inputTokens"], 7);
        assert_eq!(samples[2]["outputTokens"], 2);
        // A minute whose provider never broke reasoning out still emits 0 —
        // samples are dense, the field is never omitted.
        assert_eq!(samples[2]["thoughtTokens"], 0);
        assert_eq!(samples[4]["bucketUtc"], "2026-07-30T14:07:00Z");
        assert_eq!(samples[4]["inputTokens"], 100);
        assert_eq!(samples[4]["outputTokens"], 40);
        assert_eq!(samples[4]["cacheReadTokens"], 0);
        assert_eq!(samples[4]["cacheCreationTokens"], 0);
        assert_eq!(samples[4]["thoughtTokens"], 15);
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
            assert_eq!(s["thoughtTokens"], 0);
        }
    }
}
