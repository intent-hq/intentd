//! `stats.getUsage` read aggregation over `usage_stats_hourly` rows (the
//! agentic usage-stats cards). Everything here is pure and unit-testable
//! without a store; the recording side lives in `usage_stats.rs`.
//!
//! Buckets are stored as UTC hour floors alongside a local wall-clock stamp
//! (`local_date` / `local_hour`) captured at record time (D12). For
//! month/year periods, filtering and hour-of-day / month grouping follow
//! that stamp, so "1pm" means 1pm on the daemon's machine when the activity
//! happened — immune to later DST transitions or timezone moves. Rows
//! lacking a stamp (pre-D12) defensively fall back to shifting `bucket_utc`
//! by the client's `tzOffsetMinutes` (minutes east of UTC). The `24h` period
//! (D11) is an absolute rolling window — the trailing 24 hourly UTC buckets
//! ending at the current hour — with only the per-bucket hour labels
//! rendered via `tzOffsetMinutes`.

use std::collections::{BTreeMap, BTreeSet};

use intent_core::{Error, Result};
use intent_store::UsageStatsRow;
use serde_json::{json, Value};
use time::format_description::well_known::Rfc3339;
use time::{Duration, OffsetDateTime};

/// One validated `stats.getUsage` period (`period` + `key` wire params).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UsagePeriod {
    /// One local-time calendar month (`period: "month"`, `key: "YYYY-MM"`).
    Month { year: i32, month: u8 },
    /// One local-time calendar year (`period: "year"`, `key: "YYYY"`).
    Year { year: i32 },
    /// The trailing 24 hourly UTC buckets ending at the current hour (D11).
    Last24h,
}

/// Parse the wire `period` / `key` pair into a [`UsagePeriod`]. `key` is
/// required for `"month"` / `"year"` and ignored for `"24h"`; anything
/// malformed is `InvalidParams` (router surfaces `-32602`).
pub(crate) fn parse_period(period: &str, key: Option<&str>) -> Result<UsagePeriod> {
    match period {
        "24h" => Ok(UsagePeriod::Last24h),
        "month" => {
            let key = key.ok_or_else(|| {
                Error::InvalidParams("key (\"YYYY-MM\") is required for period \"month\"".into())
            })?;
            key.split_once('-')
                .and_then(|(y, m)| {
                    let year = parse_digits(y, 4)? as i32;
                    let month = parse_digits(m, 2)?;
                    (1..=12).contains(&month).then_some(UsagePeriod::Month {
                        year,
                        month: month as u8,
                    })
                })
                .ok_or_else(|| {
                    Error::InvalidParams(format!("invalid month key {key:?}: expected \"YYYY-MM\""))
                })
        }
        "year" => {
            let key = key.ok_or_else(|| {
                Error::InvalidParams("key (\"YYYY\") is required for period \"year\"".into())
            })?;
            parse_digits(key, 4)
                .map(|y| UsagePeriod::Year { year: y as i32 })
                .ok_or_else(|| {
                    Error::InvalidParams(format!("invalid year key {key:?}: expected \"YYYY\""))
                })
        }
        other => Err(Error::InvalidParams(format!(
            "period must be \"24h\", \"month\" or \"year\" (got {other:?})"
        ))),
    }
}

/// Parse an exactly-`len`-digit decimal number (no signs, no spaces).
fn parse_digits(s: &str, len: usize) -> Option<u32> {
    (s.len() == len && s.bytes().all(|b| b.is_ascii_digit()))
        .then(|| s.parse().ok())
        .flatten()
}

/// Parse a stored `local_date` (`"YYYY-MM-DD"`) into `(year, month)`; `None`
/// for anything malformed, in which case the caller falls back to the UTC
/// shift. The day is range-checked only loosely (1–31, deliberately not
/// calendar-validated) since only `(year, month)` are consumed.
fn parse_local_date(s: &str) -> Option<(i32, u8)> {
    let mut it = s.split('-');
    let year = parse_digits(it.next()?, 4)? as i32;
    let month = parse_digits(it.next()?, 2)?;
    let day = parse_digits(it.next()?, 2)?;
    (it.next().is_none() && (1..=12).contains(&month) && (1..=31).contains(&day))
        .then_some((year, month as u8))
}

/// Per-row local wall-clock parts `(year, month, hour_of_day)` used for
/// month/year period filtering and grouping: the recorded stamp when present
/// and well-formed (D12), otherwise the UTC bucket shifted by the client
/// offset (defensive fallback for pre-D12 rows).
fn local_parts(row: &UsageStatsRow, utc: OffsetDateTime, tz: time::UtcOffset) -> (i32, u8, u8) {
    if let (Some(date), Some(hour)) = (row.local_date.as_deref(), row.local_hour) {
        if let Some((year, month)) = parse_local_date(date) {
            if hour < 24 {
                return (year, month, hour);
            }
        }
    }
    let local = utc.to_offset(tz);
    (local.year(), u8::from(local.month()), local.hour())
}

/// The separate token counters (D6) for one aggregation cell: the four
/// always-present counters plus reasoning ("thought") tokens, which follow
/// the §5.23 TokenUsageTotals convention — omitted from the wire when zero,
/// never `0`, never `null`.
#[derive(Debug, Default, Clone, Copy)]
struct TokenCell {
    input: u64,
    output: u64,
    cache_read: u64,
    cache_creation: u64,
    thought: u64,
}

impl TokenCell {
    fn add(&mut self, r: &UsageStatsRow) {
        self.input += r.input_tokens;
        self.output += r.output_tokens;
        self.cache_read += r.cache_read_tokens;
        self.cache_creation += r.cache_creation_tokens;
        self.thought += r.thought_tokens;
    }

    fn total(&self) -> u64 {
        self.input + self.output + self.cache_read + self.cache_creation + self.thought
    }

    fn json(&self) -> Value {
        let mut v = json!({
            "inputTokens": self.input,
            "outputTokens": self.output,
            "cacheReadTokens": self.cache_read,
            "cacheCreationTokens": self.cache_creation,
        });
        if self.thought > 0 {
            v["thoughtTokens"] = json!(self.thought);
        }
        v
    }
}

/// Floor a UTC datetime to its hour (the rolling 24h-window boundary).
fn floor_to_hour(t: OffsetDateTime) -> OffsetDateTime {
    t.replace_minute(0)
        .and_then(|t| t.replace_second(0))
        .and_then(|t| t.replace_nanosecond(0))
        .expect("hour floor components are in range")
}

/// Aggregate `usage_stats_hourly` rows into the `stats.getUsage` result for
/// one period. Month/year filtering and grouping follow each row's recorded
/// local wall-clock stamp (D12), falling back to the `tz_offset_minutes`
/// shift for unstamped rows; `tz_offset_minutes` is the client's offset east
/// of UTC (must be a plausible UTC offset, ±14h) and also labels the 24h
/// buckets; `now_utc` anchors the 24h rolling window (injected for
/// testability). Rows with malformed bucket keys are skipped.
///
/// Result shape (all periods): `totals` (4 counters plus `thoughtTokens`
/// when non-zero), `runs`, `sessions`, `longestRunMs`, `linesAdded`,
/// `linesDeleted`, `byModel` (sorted desc by
/// total tokens), `byProvider` (raw provider ids, same sorting), `byHourOfDay`
/// (24 entries), `byMonth` (12 entries, the period's local year; zeroed for
/// 24h), and `availablePeriods` computed over ALL rows regardless of the
/// requested period. Empty periods return zeroed shapes, never an error.
pub(crate) fn aggregate_usage(
    rows: &[UsageStatsRow],
    period: UsagePeriod,
    tz_offset_minutes: i64,
    now_utc: OffsetDateTime,
) -> Result<Value> {
    if !(-14 * 60..=14 * 60).contains(&tz_offset_minutes) {
        return Err(Error::InvalidParams(format!(
            "tzOffsetMinutes must be within ±840 (got {tz_offset_minutes})"
        )));
    }
    let tz = time::UtcOffset::from_whole_seconds(tz_offset_minutes as i32 * 60)
        .expect("validated offset is in range");

    // The 24h rolling window: the 24 hourly UTC buckets ending at now's hour.
    let window_end = floor_to_hour(now_utc.to_offset(time::UtcOffset::UTC));
    let window_start = window_end - Duration::hours(23);

    let mut totals = TokenCell::default();
    let mut runs = 0u64;
    let mut sessions = 0u64;
    let mut longest_run_ms = 0u64;
    let mut lines_added = 0u64;
    let mut lines_deleted = 0u64;
    let mut by_model: BTreeMap<String, (TokenCell, u64)> = BTreeMap::new();
    let mut by_provider: BTreeMap<String, (TokenCell, u64)> = BTreeMap::new();
    let mut by_hour = [TokenCell::default(); 24];
    let mut by_month = [TokenCell::default(); 12];
    let mut months: BTreeSet<String> = BTreeSet::new();
    let mut years: BTreeSet<String> = BTreeSet::new();

    for row in rows {
        let Ok(utc) = OffsetDateTime::parse(&row.bucket_utc, &Rfc3339) else {
            continue; // defensive: never fail the whole read on one bad key
        };
        let (local_year, local_month, local_hour) = local_parts(row, utc, tz);
        months.insert(format!("{local_year:04}-{local_month:02}"));
        years.insert(format!("{local_year:04}"));

        // byMonth covers the period's whole local year, independent of the
        // (possibly narrower) month filter; 24h has no month card.
        match period {
            UsagePeriod::Month { year, .. } | UsagePeriod::Year { year } => {
                if local_year == year {
                    by_month[usize::from(local_month) - 1].add(row);
                }
            }
            UsagePeriod::Last24h => {}
        }

        let (in_period, hour_slot) = match period {
            UsagePeriod::Month { year, month } => (
                local_year == year && local_month == month,
                usize::from(local_hour),
            ),
            UsagePeriod::Year { year } => (local_year == year, usize::from(local_hour)),
            UsagePeriod::Last24h => {
                if utc < window_start || utc > window_end {
                    (false, 0)
                } else {
                    (
                        true,
                        (utc - window_start).whole_hours().clamp(0, 23) as usize,
                    )
                }
            }
        };
        if !in_period {
            continue;
        }

        totals.add(row);
        runs += row.runs;
        sessions += row.sessions_started;
        longest_run_ms = longest_run_ms.max(row.longest_run_ms);
        lines_added += row.lines_added;
        lines_deleted += row.lines_deleted;
        by_hour[hour_slot].add(row);
        let entry = by_model.entry(row.model.clone()).or_default();
        entry.0.add(row);
        entry.1 += row.runs;
        let entry = by_provider.entry(row.provider.clone()).or_default();
        entry.0.add(row);
        entry.1 += row.runs;
    }

    // byModel sorted desc by total tokens; ties break on model name (asc, via
    // the BTreeMap ordering surviving the stable sort).
    let mut models: Vec<(String, (TokenCell, u64))> = by_model.into_iter().collect();
    models.sort_by_key(|(_, (cell, _))| std::cmp::Reverse(cell.total()));
    let by_model_json: Vec<Value> = models
        .into_iter()
        .map(|(model, (cell, runs))| {
            let mut v = cell.json();
            v["model"] = json!(model);
            v["runs"] = json!(runs);
            v
        })
        .collect();

    // byProvider mirrors byModel: raw provider ids on the wire ('unknown' for
    // pre-migration rows), sorted desc by total tokens, ties on provider id
    // asc (the BTreeMap ordering surviving the stable sort).
    let mut providers: Vec<(String, (TokenCell, u64))> = by_provider.into_iter().collect();
    providers.sort_by_key(|(_, (cell, _))| std::cmp::Reverse(cell.total()));
    let by_provider_json: Vec<Value> = providers
        .into_iter()
        .map(|(provider, (cell, runs))| {
            let mut v = cell.json();
            v["provider"] = json!(provider);
            v["runs"] = json!(runs);
            v
        })
        .collect();

    // byHourOfDay: for month/year the 24 local hours of day (0..23); for 24h
    // the 24 trailing hourly buckets in chronological order, each labelled
    // with its local-time hour.
    let by_hour_json: Vec<Value> = by_hour
        .iter()
        .enumerate()
        .map(|(i, cell)| {
            let hour = match period {
                UsagePeriod::Last24h => {
                    let bucket = window_start + Duration::hours(i as i64);
                    u64::from(bucket.to_offset(tz).hour())
                }
                _ => i as u64,
            };
            let mut v = cell.json();
            v["hour"] = json!(hour);
            v
        })
        .collect();

    let by_month_json: Vec<Value> = by_month
        .iter()
        .enumerate()
        .map(|(i, cell)| {
            let mut v = cell.json();
            v["month"] = json!(i as u64 + 1);
            v
        })
        .collect();

    Ok(json!({
        "totals": totals.json(),
        "runs": runs,
        "sessions": sessions,
        "longestRunMs": longest_run_ms,
        "linesAdded": lines_added,
        "linesDeleted": lines_deleted,
        "byModel": by_model_json,
        "byProvider": by_provider_json,
        "byHourOfDay": by_hour_json,
        "byMonth": by_month_json,
        "availablePeriods": {
            "months": months.into_iter().collect::<Vec<_>>(),
            "years": years.into_iter().collect::<Vec<_>>(),
        },
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> OffsetDateTime {
        OffsetDateTime::parse(s, &Rfc3339).expect("parse test timestamp")
    }

    fn row(bucket: &str, model: &str, input: u64, output: u64) -> UsageStatsRow {
        prow(bucket, model, "claude-code", input, output)
    }

    fn prow(bucket: &str, model: &str, provider: &str, input: u64, output: u64) -> UsageStatsRow {
        UsageStatsRow {
            bucket_utc: bucket.to_string(),
            model: model.to_string(),
            provider: provider.to_string(),
            input_tokens: input,
            output_tokens: output,
            ..Default::default()
        }
    }

    #[test]
    fn period_parsing_accepts_valid_and_rejects_malformed() {
        assert_eq!(parse_period("24h", None).unwrap(), UsagePeriod::Last24h);
        assert_eq!(
            parse_period("month", Some("2026-07")).unwrap(),
            UsagePeriod::Month {
                year: 2026,
                month: 7
            }
        );
        assert_eq!(
            parse_period("year", Some("2026")).unwrap(),
            UsagePeriod::Year { year: 2026 }
        );
        for (period, key) in [
            ("week", Some("2026-07")),
            ("month", None),
            ("month", Some("2026-13")),
            ("month", Some("2026-7")),
            ("month", Some("26-07")),
            ("year", None),
            ("year", Some("26")),
            ("year", Some("20x6")),
        ] {
            assert!(
                matches!(parse_period(period, key), Err(Error::InvalidParams(_))),
                "period {period:?} key {key:?} must be invalid params"
            );
        }
        // 24h ignores any stray key.
        assert_eq!(
            parse_period("24h", Some("2026-07")).unwrap(),
            UsagePeriod::Last24h
        );
    }

    #[test]
    fn month_period_filters_and_rolls_up() {
        let now = parse("2026-07-25T10:00:00Z");
        let mut in_july = row("2026-07-25T14:00:00Z", "Opus 4.8", 100, 40);
        in_july.runs = 2;
        in_july.sessions_started = 1;
        in_july.longest_run_ms = 5_000;
        in_july.lines_added = 10;
        in_july.lines_deleted = 3;
        let mut also_july = row("2026-07-10T14:00:00Z", "Sonnet 5", 30, 5);
        also_july.runs = 1;
        also_july.longest_run_ms = 9_000;
        let in_june = row("2026-06-30T12:00:00Z", "Opus 4.8", 999, 999);
        let rows = vec![in_july.clone(), also_july, in_june];

        let v = aggregate_usage(
            &rows,
            UsagePeriod::Month {
                year: 2026,
                month: 7,
            },
            0,
            now,
        )
        .unwrap();
        assert_eq!(v["totals"]["inputTokens"], 130);
        assert_eq!(v["totals"]["outputTokens"], 45);
        assert_eq!(v["runs"], 3);
        assert_eq!(v["sessions"], 1);
        assert_eq!(v["longestRunMs"], 9_000);
        assert_eq!(v["linesAdded"], 10);
        assert_eq!(v["linesDeleted"], 3);
        // byModel is sorted desc by total tokens.
        assert_eq!(v["byModel"][0]["model"], "Opus 4.8");
        assert_eq!(v["byModel"][0]["inputTokens"], 100);
        assert_eq!(v["byModel"][0]["runs"], 2);
        assert_eq!(v["byModel"][1]["model"], "Sonnet 5");
        // byMonth covers the local year, INCLUDING the June row.
        assert_eq!(v["byMonth"][5]["inputTokens"], 999);
        assert_eq!(v["byMonth"][6]["inputTokens"], 130);
        // byHourOfDay groups by local hour (both July rows are at 14:00).
        assert_eq!(v["byHourOfDay"][14]["inputTokens"], 130);
        assert_eq!(v["byHourOfDay"][13]["inputTokens"], 0);
        // availablePeriods spans ALL rows.
        assert_eq!(
            v["availablePeriods"]["months"],
            json!(["2026-06", "2026-07"])
        );
        assert_eq!(v["availablePeriods"]["years"], json!(["2026"]));
    }

    #[test]
    fn by_provider_aggregates_across_buckets_sorted_desc_with_ties_asc() {
        let now = parse("2026-07-25T10:00:00Z");
        // codex spans two buckets (60 total) and outranks claude-code (50);
        // droid and pi tie at 5 and must come out in provider-id order. The
        // June row is outside the period and must not leak into byProvider.
        let mut codex_a = prow("2026-07-10T14:00:00Z", "GPT-6", "codex", 40, 0);
        codex_a.runs = 1;
        let mut codex_b = prow("2026-07-11T09:00:00Z", "GPT-6 Mini", "codex", 20, 0);
        codex_b.runs = 2;
        let mut claude = prow("2026-07-12T08:00:00Z", "Opus 4.8", "claude-code", 30, 20);
        claude.runs = 3;
        let rows = vec![
            codex_a,
            codex_b,
            claude,
            prow("2026-07-13T10:00:00Z", "Pi 1", "pi", 5, 0),
            prow("2026-07-14T10:00:00Z", "Droid 1", "droid", 5, 0),
            prow("2026-06-30T12:00:00Z", "Opus 4.8", "opencode", 999, 0),
        ];
        let v = aggregate_usage(
            &rows,
            UsagePeriod::Month {
                year: 2026,
                month: 7,
            },
            0,
            now,
        )
        .unwrap();
        let by_provider = v["byProvider"].as_array().unwrap();
        assert_eq!(by_provider.len(), 4, "June opencode row excluded");
        assert_eq!(by_provider[0]["provider"], "codex");
        assert_eq!(by_provider[0]["inputTokens"], 60);
        assert_eq!(by_provider[0]["runs"], 3);
        assert_eq!(by_provider[1]["provider"], "claude-code");
        assert_eq!(by_provider[1]["inputTokens"], 30);
        assert_eq!(by_provider[1]["outputTokens"], 20);
        assert_eq!(by_provider[1]["runs"], 3);
        assert_eq!(by_provider[2]["provider"], "droid", "tie breaks id asc");
        assert_eq!(by_provider[3]["provider"], "pi");
    }

    #[test]
    fn by_provider_keeps_unknown_bucket_for_unattributed_rows() {
        // Pre-migration rows carry provider 'unknown'; the wire keeps the raw
        // id — no display-name mapping in the daemon.
        let now = parse("2026-07-25T10:00:00Z");
        let rows = vec![
            prow("2026-07-10T14:00:00Z", "Opus 4.8", "unknown", 10, 0),
            prow("2026-07-11T14:00:00Z", "Opus 4.8", "claude-code", 90, 0),
        ];
        let v = aggregate_usage(
            &rows,
            UsagePeriod::Month {
                year: 2026,
                month: 7,
            },
            0,
            now,
        )
        .unwrap();
        // One model, two providers: byProvider splits what byModel folds.
        assert_eq!(v["byModel"].as_array().unwrap().len(), 1);
        let by_provider = v["byProvider"].as_array().unwrap();
        assert_eq!(by_provider.len(), 2);
        assert_eq!(by_provider[0]["provider"], "claude-code");
        assert_eq!(by_provider[0]["inputTokens"], 90);
        assert_eq!(by_provider[1]["provider"], "unknown");
        assert_eq!(by_provider[1]["inputTokens"], 10);
    }

    #[test]
    fn tz_shift_moves_buckets_across_period_boundaries() {
        let now = parse("2026-07-25T10:00:00Z");
        // 23:30 UTC June 30 bucket floor is 23:00Z; at +120 minutes it is
        // 01:00 local July 1 — inside a July month period.
        let rows = vec![row("2026-06-30T23:00:00Z", "Opus 4.8", 50, 5)];
        let july = UsagePeriod::Month {
            year: 2026,
            month: 7,
        };
        let v = aggregate_usage(&rows, july, 120, now).unwrap();
        assert_eq!(v["totals"]["inputTokens"], 50);
        assert_eq!(v["byHourOfDay"][1]["inputTokens"], 50, "local hour is 01");
        assert_eq!(v["availablePeriods"]["months"], json!(["2026-07"]));

        // The same bucket at -60 minutes stays in June, so July is empty.
        let v = aggregate_usage(&rows, july, -60, now).unwrap();
        assert_eq!(v["totals"]["inputTokens"], 0);
        assert_eq!(v["availablePeriods"]["months"], json!(["2026-06"]));

        // Year boundary: Dec 31 23:00Z at +120 lands in the next local year.
        let rows = vec![row("2026-12-31T23:00:00Z", "Opus 4.8", 7, 0)];
        let v = aggregate_usage(&rows, UsagePeriod::Year { year: 2027 }, 120, now).unwrap();
        assert_eq!(v["totals"]["inputTokens"], 7);
        assert_eq!(v["byMonth"][0]["inputTokens"], 7);
        assert_eq!(v["availablePeriods"]["years"], json!(["2027"]));
    }

    #[test]
    fn last_24h_window_includes_trailing_buckets_in_order() {
        // now 15:37 → window is 24 buckets from yesterday 16:00Z .. today
        // 15:00Z inclusive.
        let now = parse("2026-07-25T15:37:12Z");
        let rows = vec![
            row("2026-07-24T15:00:00Z", "Opus 4.8", 111, 0), // 1h too old
            row("2026-07-24T16:00:00Z", "Opus 4.8", 1, 0),   // oldest slot 0
            row("2026-07-25T03:00:00Z", "Sonnet 5", 2, 0),   // slot 11
            row("2026-07-25T15:00:00Z", "Opus 4.8", 4, 0),   // newest slot 23
            row("2026-07-25T16:00:00Z", "Opus 4.8", 222, 0), // future bucket
        ];
        let v = aggregate_usage(&rows, UsagePeriod::Last24h, 0, now).unwrap();
        assert_eq!(v["totals"]["inputTokens"], 7);
        assert_eq!(v["byHourOfDay"].as_array().unwrap().len(), 24);
        assert_eq!(v["byHourOfDay"][0]["inputTokens"], 1);
        assert_eq!(v["byHourOfDay"][0]["hour"], 16);
        assert_eq!(v["byHourOfDay"][11]["inputTokens"], 2);
        assert_eq!(v["byHourOfDay"][11]["hour"], 3);
        assert_eq!(v["byHourOfDay"][23]["inputTokens"], 4);
        assert_eq!(v["byHourOfDay"][23]["hour"], 15);
        // byMonth is zeroed for 24h (FE hides the month card).
        assert!(v["byMonth"]
            .as_array()
            .unwrap()
            .iter()
            .all(|m| m["inputTokens"] == 0 && m["outputTokens"] == 0));
        // Local hour labels follow the tz offset.
        let v = aggregate_usage(&rows, UsagePeriod::Last24h, 60, now).unwrap();
        assert_eq!(v["byHourOfDay"][0]["hour"], 17);
        assert_eq!(v["byHourOfDay"][23]["hour"], 16);
    }

    #[test]
    fn empty_period_returns_zeroed_shape() {
        let now = parse("2026-07-25T10:00:00Z");
        let v = aggregate_usage(
            &[],
            UsagePeriod::Month {
                year: 2026,
                month: 7,
            },
            0,
            now,
        )
        .unwrap();
        assert_eq!(v["totals"]["inputTokens"], 0);
        assert_eq!(v["totals"]["cacheReadTokens"], 0);
        assert_eq!(v["runs"], 0);
        assert_eq!(v["sessions"], 0);
        assert_eq!(v["longestRunMs"], 0);
        assert_eq!(v["linesAdded"], 0);
        assert_eq!(v["linesDeleted"], 0);
        assert_eq!(v["byModel"], json!([]));
        assert_eq!(v["byProvider"], json!([]));
        assert_eq!(v["byHourOfDay"].as_array().unwrap().len(), 24);
        assert_eq!(v["byMonth"].as_array().unwrap().len(), 12);
        assert_eq!(v["availablePeriods"]["months"], json!([]));
        assert_eq!(v["availablePeriods"]["years"], json!([]));
    }

    #[test]
    fn implausible_tz_offset_is_invalid_params() {
        let now = parse("2026-07-25T10:00:00Z");
        for tz in [-15 * 60, 15 * 60] {
            assert!(matches!(
                aggregate_usage(&[], UsagePeriod::Last24h, tz, now),
                Err(Error::InvalidParams(_))
            ));
        }
    }

    #[test]
    fn malformed_bucket_keys_are_skipped() {
        let now = parse("2026-07-25T10:00:00Z");
        let rows = vec![
            row("not-a-timestamp", "Opus 4.8", 999, 0),
            row("2026-07-25T09:00:00Z", "Opus 4.8", 5, 0),
        ];
        let v = aggregate_usage(&rows, UsagePeriod::Last24h, 0, now).unwrap();
        assert_eq!(v["totals"]["inputTokens"], 5);
    }

    fn stamped(
        bucket: &str,
        model: &str,
        input: u64,
        local_date: &str,
        local_hour: u8,
    ) -> UsageStatsRow {
        UsageStatsRow {
            local_date: Some(local_date.to_string()),
            local_hour: Some(local_hour),
            ..row(bucket, model, input, 0)
        }
    }

    #[test]
    fn dst_fold_groups_rows_by_recorded_local_hour() {
        // US fall-back 2026-11-01: 01:xx EDT (UTC-4) is bucket 05:00Z, and
        // one hour later 01:xx EST (UTC-5) is bucket 06:00Z — two different
        // UTC buckets recorded at the SAME local wall-clock hour. Grouping
        // must follow the recorded stamp, not the client's current offset
        // (which would put the EDT row at local hour 0).
        let now = parse("2026-11-15T10:00:00Z");
        let rows = vec![
            stamped("2026-11-01T05:00:00Z", "Opus 4.8", 10, "2026-11-01", 1),
            stamped("2026-11-01T06:00:00Z", "Opus 4.8", 20, "2026-11-01", 1),
        ];
        let november = UsagePeriod::Month {
            year: 2026,
            month: 11,
        };
        let v = aggregate_usage(&rows, november, -300, now).unwrap();
        assert_eq!(v["totals"]["inputTokens"], 30);
        assert_eq!(v["byHourOfDay"][1]["inputTokens"], 30, "both at local 1am");
        assert_eq!(v["byHourOfDay"][0]["inputTokens"], 0);
        assert_eq!(v["availablePeriods"]["months"], json!(["2026-11"]));
    }

    #[test]
    fn recorded_stamp_drives_period_filter_by_month_and_available_periods() {
        // Recorded at 01:00 local Jan 1 2027 (+02:00 daemon), bucket still
        // Dec 31 2026 UTC: the stamp — not the client's tzOffsetMinutes of
        // 0 — must place the row in 2027 / January everywhere.
        let now = parse("2027-01-15T10:00:00Z");
        let rows = vec![stamped(
            "2026-12-31T23:00:00Z",
            "Opus 4.8",
            7,
            "2027-01-01",
            1,
        )];
        let v = aggregate_usage(&rows, UsagePeriod::Year { year: 2027 }, 0, now).unwrap();
        assert_eq!(v["totals"]["inputTokens"], 7);
        assert_eq!(v["byMonth"][0]["inputTokens"], 7, "January of local year");
        assert_eq!(v["byHourOfDay"][1]["inputTokens"], 7);
        assert_eq!(v["availablePeriods"]["months"], json!(["2027-01"]));
        assert_eq!(v["availablePeriods"]["years"], json!(["2027"]));

        // …and the UTC-keyed 2026 view no longer sees it.
        let v = aggregate_usage(&rows, UsagePeriod::Year { year: 2026 }, 0, now).unwrap();
        assert_eq!(v["totals"]["inputTokens"], 0);
    }

    #[test]
    fn rows_without_or_with_malformed_stamp_fall_back_to_tz_shift() {
        let now = parse("2026-07-25T10:00:00Z");
        let mut half_stamped = row("2026-07-20T10:00:00Z", "Opus 4.8", 3, 0);
        half_stamped.local_date = Some("2026-07-20".into()); // hour missing
        let rows = vec![
            // Stamped row: grouped by its stamp.
            stamped("2026-06-30T23:00:00Z", "Opus 4.8", 1, "2026-07-01", 1),
            // Pre-D12 row (NULL stamp): +120 shift → July 1, local hour 0.
            row("2026-06-30T22:00:00Z", "Opus 4.8", 2, 0),
            // Half/malformed stamps: full fallback → local hour 12.
            half_stamped,
            stamped("2026-07-15T10:00:00Z", "Opus 4.8", 4, "garbage", 9),
        ];
        let july = UsagePeriod::Month {
            year: 2026,
            month: 7,
        };
        let v = aggregate_usage(&rows, july, 120, now).unwrap();
        assert_eq!(v["totals"]["inputTokens"], 10, "all four rows in July");
        assert_eq!(v["byHourOfDay"][1]["inputTokens"], 1, "stamped");
        assert_eq!(v["byHourOfDay"][0]["inputTokens"], 2, "NULL fallback");
        assert_eq!(v["byHourOfDay"][12]["inputTokens"], 7, "fallback shift");
        assert_eq!(
            v["byHourOfDay"][9]["inputTokens"], 0,
            "garbage date ignored"
        );
        assert_eq!(v["availablePeriods"]["months"], json!(["2026-07"]));
    }

    #[test]
    fn thought_tokens_surface_on_totals_and_every_rollup() {
        let now = parse("2026-07-25T15:37:12Z");
        let mut with_thought = row("2026-07-25T14:00:00Z", "Opus 4.8", 100, 40);
        with_thought.thought_tokens = 25;
        let without_thought = row("2026-07-25T13:00:00Z", "Sonnet 5", 30, 5);
        let rows = vec![with_thought, without_thought];
        let july = UsagePeriod::Month {
            year: 2026,
            month: 7,
        };
        let v = aggregate_usage(&rows, july, 0, now).unwrap();
        assert_eq!(v["totals"]["thoughtTokens"], 25);
        assert_eq!(v["byMonth"][6]["thoughtTokens"], 25);
        assert_eq!(v["byHourOfDay"][14]["thoughtTokens"], 25);
        assert!(
            v["byHourOfDay"][13].get("thoughtTokens").is_none(),
            "zero-thought cell must omit the field: {v}"
        );
        let by_model = v["byModel"].as_array().unwrap();
        let opus = by_model.iter().find(|m| m["model"] == "Opus 4.8").unwrap();
        assert_eq!(opus["thoughtTokens"], 25);
        let sonnet = by_model.iter().find(|m| m["model"] == "Sonnet 5").unwrap();
        assert!(
            sonnet.get("thoughtTokens").is_none(),
            "zero-thought model must omit the field: {v}"
        );
        assert_eq!(v["byProvider"][0]["thoughtTokens"], 25);

        // 24h window carries it too.
        let v = aggregate_usage(&rows, UsagePeriod::Last24h, 0, now).unwrap();
        assert_eq!(v["totals"]["thoughtTokens"], 25);
    }

    #[test]
    fn thought_tokens_count_toward_by_model_and_by_provider_ranking() {
        let now = parse("2026-07-25T10:00:00Z");
        // thinker: 40 input + 30 thought = 70; plain: 60 input. The thought
        // counter must count toward the "total tokens" ranking, putting
        // thinker first.
        let mut thinker = prow("2026-07-10T14:00:00Z", "Opus 4.8", "claude-code", 40, 0);
        thinker.thought_tokens = 30;
        let plain = prow("2026-07-11T14:00:00Z", "GPT-6", "codex", 60, 0);
        let rows = vec![thinker, plain];
        let v = aggregate_usage(
            &rows,
            UsagePeriod::Month {
                year: 2026,
                month: 7,
            },
            0,
            now,
        )
        .unwrap();
        assert_eq!(v["byModel"][0]["model"], "Opus 4.8");
        assert_eq!(v["byModel"][1]["model"], "GPT-6");
        assert_eq!(v["byProvider"][0]["provider"], "claude-code");
        assert_eq!(v["byProvider"][1]["provider"], "codex");
    }

    #[test]
    fn zero_thought_rows_serialize_byte_identical_to_pre_change_shape() {
        // No thought tokens anywhere → every cell serializes without the
        // field, byte-compatible with the pre-0087 response shape.
        let now = parse("2026-07-25T10:00:00Z");
        let rows = vec![row("2026-07-25T09:00:00Z", "Opus 4.8", 100, 40)];
        let v = aggregate_usage(
            &rows,
            UsagePeriod::Month {
                year: 2026,
                month: 7,
            },
            0,
            now,
        )
        .unwrap();
        assert_eq!(
            serde_json::to_string(&v["totals"]).unwrap(),
            serde_json::to_string(&json!({
                "inputTokens": 100,
                "outputTokens": 40,
                "cacheReadTokens": 0,
                "cacheCreationTokens": 0,
            }))
            .unwrap()
        );
        let rendered = serde_json::to_string(&v).unwrap();
        assert!(
            !rendered.contains("thoughtTokens"),
            "zero-thought response must not mention the field: {rendered}"
        );
    }

    #[test]
    fn last_24h_window_ignores_local_stamps() {
        // The 24h rolling window stays UTC-keyed with tzOffsetMinutes-only
        // hour labels: adversarial stamps must change neither slotting,
        // labels, nor window membership.
        let now = parse("2026-07-25T15:37:12Z");
        let rows = vec![
            stamped("2026-07-24T15:00:00Z", "Opus 4.8", 111, "2026-07-25", 5), // still too old
            stamped("2026-07-24T16:00:00Z", "Opus 4.8", 1, "2026-01-01", 5),   // still slot 0
            stamped("2026-07-25T15:00:00Z", "Opus 4.8", 4, "2026-01-01", 5),   // still slot 23
        ];
        let v = aggregate_usage(&rows, UsagePeriod::Last24h, 60, now).unwrap();
        assert_eq!(v["totals"]["inputTokens"], 5);
        assert_eq!(v["byHourOfDay"][0]["inputTokens"], 1);
        assert_eq!(v["byHourOfDay"][0]["hour"], 17, "label from tz offset");
        assert_eq!(v["byHourOfDay"][23]["inputTokens"], 4);
        assert_eq!(v["byHourOfDay"][23]["hour"], 16);
        // byMonth stays zeroed for 24h regardless of stamps.
        assert!(v["byMonth"]
            .as_array()
            .unwrap()
            .iter()
            .all(|m| m["inputTokens"] == 0));
    }
}
