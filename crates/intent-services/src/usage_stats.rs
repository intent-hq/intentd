//! Global usage-stats recording helpers behind the agentic usage-stats cards:
//! UTC hour bucketing, per-turn token deltas, and model-name normalization,
//! plus the best-effort session-start / lines-changed recorders.
//!
//! The turn-end recording side effects live in `agent_session.rs`
//! (`record_turn_usage_stats`); the bucketing/delta/normalization helpers here
//! are pure and unit-testable without a store, while
//! [`record_session_started`] / [`record_lines_changed`] fold their one-shot
//! deltas straight into the store. Stats aggregate globally across workspaces
//! into the `usage_stats_hourly` table (one row per UTC hour bucket +
//! normalized model).

use intent_core::{AgentId, TokenUsageTotals, WorkspaceId};
use intent_store::{LocalStamp, Store, UsageStatsDelta};
use time::{OffsetDateTime, UtcOffset};

use crate::token_usage::UNKNOWN_MODEL;

/// Floor `t` to its UTC hour and render the bucket key used by
/// `usage_stats_hourly.bucket_utc`: `"YYYY-MM-DDTHH:00:00Z"`. Buckets are
/// stored in UTC; local wall-clock grouping uses the [`LocalStamp`] recorded
/// next to the bucket (D12).
pub fn hour_bucket_utc(t: OffsetDateTime) -> String {
    let t = t.to_offset(UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}T{:02}:00:00Z",
        t.year(),
        u8::from(t.month()),
        t.day(),
        t.hour()
    )
}

/// Render the local wall-clock stamp persisted next to a UTC bucket (D12):
/// `t` under `offset` as a calendar date (`"YYYY-MM-DD"`) plus hour-of-day.
pub fn local_stamp(t: OffsetDateTime, offset: UtcOffset) -> LocalStamp {
    let l = t.to_offset(offset);
    LocalStamp {
        date: format!("{:04}-{:02}-{:02}", l.year(), u8::from(l.month()), l.day()),
        hour: l.hour(),
    }
}

/// The daemon's current system UTC offset, used by the recorders to stamp
/// `local_date` / `local_hour` at record time. Falls back to UTC when the
/// local offset cannot be determined — notably the `time` crate's soundness
/// guard, which refuses to read the environment-derived timezone once a
/// Unix process is multi-threaded — in which case local stamps degrade to
/// UTC wall-clock (the pre-D12 grouping behavior).
pub fn recording_local_offset() -> UtcOffset {
    UtcOffset::current_local_offset().unwrap_or(UtcOffset::UTC)
}

/// Per-turn token delta between two cumulative end-of-turn snapshots: `next`
/// minus `prev` (`None` = session never reported → full `next`), clamped ≥ 0
/// per counter. The clamp absorbs snapshot regressions — e.g. the first
/// report of a recreated ACP session restarting from zero — at the cost of
/// under-counting that one turn, which is preferable to a huge bogus delta.
pub fn turn_token_delta(
    prev: Option<&TokenUsageTotals>,
    next: &TokenUsageTotals,
) -> TokenUsageTotals {
    let zero = TokenUsageTotals::default();
    let prev = prev.unwrap_or(&zero);
    TokenUsageTotals {
        input_tokens: next.input_tokens.saturating_sub(prev.input_tokens),
        output_tokens: next.output_tokens.saturating_sub(prev.output_tokens),
        cache_read_tokens: next
            .cache_read_tokens
            .saturating_sub(prev.cache_read_tokens),
        cache_creation_tokens: next
            .cache_creation_tokens
            .saturating_sub(prev.cache_creation_tokens),
    }
}

/// Known model families, matched as a whole hyphen/underscore-separated token
/// (first match in this order wins) and rendered with a canonical display
/// casing.
const FAMILIES: &[(&str, &str)] = &[
    ("opus", "Opus"),
    ("sonnet", "Sonnet"),
    ("haiku", "Haiku"),
    ("fable", "Fable"),
    ("gemini", "Gemini"),
    ("grok", "Grok"),
    ("gpt", "GPT"),
    ("deepseek", "DeepSeek"),
    ("qwen", "Qwen"),
    ("kimi", "Kimi"),
    ("glm", "GLM"),
];

/// Variant tokens that distinguish real sibling models within a family (so
/// `gemini-2.5-pro` and `gemini-2.5-flash` do NOT merge). Matched right after
/// the version and appended capitalized.
const VARIANTS: &[(&str, &str)] = &[
    ("pro", "Pro"),
    ("flash", "Flash"),
    ("mini", "Mini"),
    ("nano", "Nano"),
    ("turbo", "Turbo"),
    ("codex", "Codex"),
];

/// Normalize a host-specific model id to one canonical display name, so the
/// same model reached via different hosts (auggie / claude / pi / opencode)
/// lands in one `usage_stats_hourly` row: strip provider path prefixes, find
/// a known family token, and render `"{Family} {version}[ {Variant}]"` (e.g.
/// `claude-opus-4-8`, `anthropic/claude-opus-4.8-20260115`, and `Opus 4.8`
/// all → `"Opus 4.8"`). Unrecognized non-empty ids pass through unchanged
/// (minus any path prefix); empty/blank → `"unknown"`.
pub fn normalize_model_name(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return UNKNOWN_MODEL.to_string();
    }
    // Path-style provider prefixes ("anthropic/claude-...") never carry model
    // identity — keep only the final segment.
    let base = trimmed.rsplit('/').next().unwrap_or(trimmed);
    let lower = base.to_ascii_lowercase();
    let tokens: Vec<&str> = lower
        .split(['-', '_', ' ', ':', '@'])
        .filter(|t| !t.is_empty())
        .collect();
    for (family, display) in FAMILIES {
        if let Some(idx) = tokens.iter().position(|t| t == family) {
            let (version, rest) = extract_version(&tokens[idx + 1..]);
            let mut name = match version {
                Some(v) => format!("{display} {v}"),
                None => (*display).to_string(),
            };
            if let Some(first) = rest.first() {
                if let Some((_, vd)) = VARIANTS.iter().find(|(v, _)| v == first) {
                    name.push(' ');
                    name.push_str(vd);
                }
            }
            return name;
        }
    }
    base.to_string()
}

/// Record one agent-session start (D2: *sessions* = agent sessions) into the
/// current UTC hour bucket of `usage_stats_hourly`, keyed by the session's
/// normalized model (`"unknown"` when no model is resolved at creation time).
/// Best-effort: errors are logged, never propagated — stats bookkeeping must
/// not fail `agent.create`.
pub async fn record_session_started(store: &Store, raw_model: Option<&str>) {
    let now = OffsetDateTime::now_utc();
    let bucket = hour_bucket_utc(now);
    let local = local_stamp(now, recording_local_offset());
    let model = normalize_model_name(raw_model.unwrap_or(""));
    let delta = UsageStatsDelta {
        sessions_started: 1,
        ..Default::default()
    };
    if let Err(e) = store.add_usage_stats(&bucket, &model, &local, &delta).await {
        tracing::warn!(error = %e, "record session-start usage stats failed");
    }
}

/// Record one lines-changed delta (D5: period-scoped true totals, accrued as
/// attribution is recorded) into the current UTC hour bucket of
/// `usage_stats_hourly`, keyed by the acting agent's normalized session model
/// (`"unknown"` when the model cannot be resolved). Zero deltas and changes
/// with no attributed agent (manual/user edits are not agentic activity) are
/// skipped. Independent of `workspace_metrics` / `agent_metrics` by
/// construction — clearing those (e.g. `metrics.clearAgentStats`) never
/// touches `usage_stats_hourly`. Best-effort: errors are logged, never
/// propagated.
pub async fn record_lines_changed(
    store: &Store,
    workspace_id: &WorkspaceId,
    agent_id: Option<&str>,
    lines_added: u64,
    lines_deleted: u64,
) {
    if lines_added == 0 && lines_deleted == 0 {
        return;
    }
    let Some(agent_id) = agent_id else {
        return;
    };
    let raw_model = match store
        .get_agent_session_token_usage(workspace_id, &AgentId(agent_id.to_string()))
        .await
    {
        Ok((model, _)) => model,
        Err(e) => {
            tracing::warn!(agent = %agent_id, error = %e, "read agent model for lines-changed stats failed");
            None
        }
    };
    let now = OffsetDateTime::now_utc();
    let bucket = hour_bucket_utc(now);
    let local = local_stamp(now, recording_local_offset());
    let model = normalize_model_name(raw_model.as_deref().unwrap_or(""));
    let delta = UsageStatsDelta {
        lines_added,
        lines_deleted,
        ..Default::default()
    };
    if let Err(e) = store.add_usage_stats(&bucket, &model, &local, &delta).await {
        tracing::warn!(agent = %agent_id, error = %e, "record lines-changed usage stats failed");
    }
}

/// Pull a dotted version out of the tokens following a family keyword:
/// consumes up to two bare numeric tokens (`["4", "8"]` → `"4.8"`), a
/// single already-dotted token (`["4.8"]` → `"4.8"`), or a single digit-led
/// alphanumeric token (`["4o"]` → `"4o"`, so `gpt-4o` and `gpt-4o-mini` stay
/// distinct instead of both collapsing to the bare family). Date-like stamps
/// (all-digit tokens of 6+ chars, e.g. `20260115`) and any token not starting
/// with a digit end the version. Returns the version (if any) and the
/// unconsumed tail.
fn extract_version<'a>(tokens: &'a [&'a str]) -> (Option<String>, &'a [&'a str]) {
    let mut parts: Vec<&str> = Vec::new();
    let mut consumed = 0;
    for t in tokens {
        let numeric = !t.is_empty() && t.chars().all(|c| c.is_ascii_digit() || c == '.');
        let alnum_version = !numeric
            && t.starts_with(|c: char| c.is_ascii_digit())
            && t.chars().all(|c| c.is_ascii_alphanumeric() || c == '.');
        if (!numeric && !alnum_version) || (numeric && t.len() >= 6 && !t.contains('.')) {
            break;
        }
        parts.push(t);
        consumed += 1;
        if t.contains('.') || alnum_version || parts.len() == 2 {
            break;
        }
    }
    if parts.is_empty() {
        (None, tokens)
    } else {
        (Some(parts.join(".")), &tokens[consumed..])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::format_description::well_known::Rfc3339;

    fn parse(s: &str) -> OffsetDateTime {
        OffsetDateTime::parse(s, &Rfc3339).expect("parse test timestamp")
    }

    fn totals(i: u64, o: u64, cr: u64, cc: u64) -> TokenUsageTotals {
        TokenUsageTotals {
            input_tokens: i,
            output_tokens: o,
            cache_read_tokens: cr,
            cache_creation_tokens: cc,
        }
    }

    #[test]
    fn hour_bucket_floors_to_utc_hour() {
        let t = parse("2026-07-25T14:37:59.9Z");
        assert_eq!(hour_bucket_utc(t), "2026-07-25T14:00:00Z");
        // Non-UTC offsets convert to UTC before flooring.
        let t = parse("2026-01-01T00:30:00-05:00");
        assert_eq!(hour_bucket_utc(t), "2026-01-01T05:00:00Z");
    }

    #[test]
    fn local_stamp_renders_wall_clock_date_and_hour() {
        let t = parse("2026-12-31T23:30:00Z");
        // A positive offset rolls the date forward past midnight…
        let plus2 = UtcOffset::from_hms(2, 0, 0).unwrap();
        assert_eq!(
            local_stamp(t, plus2),
            LocalStamp {
                date: "2027-01-01".into(),
                hour: 1
            }
        );
        // …a negative offset stays on the previous local day…
        let minus5 = UtcOffset::from_hms(-5, 0, 0).unwrap();
        assert_eq!(
            local_stamp(t, minus5),
            LocalStamp {
                date: "2026-12-31".into(),
                hour: 18
            }
        );
        // …and the UTC fallback stamps UTC wall-clock.
        assert_eq!(
            local_stamp(t, UtcOffset::UTC),
            LocalStamp {
                date: "2026-12-31".into(),
                hour: 23
            }
        );
    }

    #[test]
    fn delta_subtracts_previous_snapshot_per_counter() {
        let prev = totals(100, 40, 20, 10);
        let next = totals(150, 70, 20, 25);
        assert_eq!(turn_token_delta(Some(&prev), &next), totals(50, 30, 0, 15));
    }

    #[test]
    fn delta_without_previous_snapshot_is_full_next() {
        let next = totals(70, 50, 30, 4);
        assert_eq!(turn_token_delta(None, &next), next);
    }

    #[test]
    fn delta_clamps_snapshot_regression_to_zero() {
        // A recreated ACP session restarts cumulative counters from zero:
        // every regressed counter clamps to 0 instead of underflowing.
        let prev = totals(1000, 500, 200, 100);
        let next = totals(10, 600, 5, 100);
        assert_eq!(turn_token_delta(Some(&prev), &next), totals(0, 100, 0, 0));
    }

    #[test]
    fn normalization_combines_hosts_into_one_display_name() {
        // The same model via different hosts (D3) → one canonical name.
        for raw in [
            "claude-opus-4-8",
            "claude-opus-4-8-20260115",
            "anthropic/claude-opus-4.8",
            "opus-4.8",
            "Opus 4.8",
        ] {
            assert_eq!(normalize_model_name(raw), "Opus 4.8", "raw: {raw}");
        }
    }

    #[test]
    fn normalization_keeps_variants_and_families_distinct() {
        assert_eq!(normalize_model_name("gemini-2.5-pro"), "Gemini 2.5 Pro");
        assert_eq!(normalize_model_name("gemini-2.5-flash"), "Gemini 2.5 Flash");
        assert_eq!(normalize_model_name("gpt-5.2-codex"), "GPT 5.2 Codex");
        assert_eq!(normalize_model_name("sonnet-5"), "Sonnet 5");
        assert_eq!(normalize_model_name("claude-haiku-4-5"), "Haiku 4.5");
    }

    #[test]
    fn normalization_keeps_alphanumeric_version_variants_distinct() {
        // Digit-led alphanumeric versions ("4o") are part of model identity:
        // gpt-4o and gpt-4o-mini must not both collapse to bare "GPT".
        assert_eq!(normalize_model_name("gpt-4o"), "GPT 4o");
        assert_eq!(normalize_model_name("gpt-4o-mini"), "GPT 4o Mini");
        assert_eq!(
            normalize_model_name("gpt-4o-mini-2024-07-18"),
            "GPT 4o Mini"
        );
        // ...while the same model via different hosts still combines.
        assert_eq!(normalize_model_name("openai/gpt-4o"), "GPT 4o");
        assert_eq!(normalize_model_name("GPT-4o"), "GPT 4o");
    }

    #[test]
    fn normalization_falls_back_to_raw_and_unknown() {
        // Unrecognized ids pass through (minus path prefix); blank → unknown.
        assert_eq!(normalize_model_name("my-custom-model"), "my-custom-model");
        assert_eq!(
            normalize_model_name("acme/my-custom-model"),
            "my-custom-model"
        );
        assert_eq!(normalize_model_name(""), UNKNOWN_MODEL);
        assert_eq!(normalize_model_name("   "), UNKNOWN_MODEL);
        // A family without any version renders the bare family name.
        assert_eq!(normalize_model_name("opus"), "Opus");
    }
}
