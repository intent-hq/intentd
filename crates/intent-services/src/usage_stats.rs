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
//! normalized model + resolved provider id).

use intent_core::{AgentId, TokenUsageTotals, WorkspaceId};
use intent_store::{LocalStamp, Store, UsageStatsDelta};
use time::{OffsetDateTime, UtcOffset};

use crate::token_usage::{UNKNOWN_MODEL, UNKNOWN_PROVIDER};

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
/// `local_date` / `local_hour` at record time. `None` when the offset cannot
/// be determined — notably the `time` crate's soundness guard, which on some
/// Unix platforms (e.g. Linux) refuses to read the environment-derived
/// timezone once the process is multi-threaded. Rows then persist NULL
/// stamps and the read side falls back to shifting `bucket_utc` by the
/// client's `tzOffsetMinutes` — exactly the pre-D12 grouping behavior. (A
/// UTC fallback stamp would be worse: readers prefer any well-formed stamp,
/// so it would silently pin those rows to UTC wall-clock.)
pub fn recording_local_offset() -> Option<UtcOffset> {
    UtcOffset::current_local_offset().ok()
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
        thought_tokens: next.thought_tokens.saturating_sub(prev.thought_tokens),
        // Usage stats (§5.36) track token counters only — cost has no bucket
        // there, so the delta carries none.
        cost: None,
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

/// Strip trailing bracketed context-variant suffixes from a model id (D14):
/// `claude-fable-5[1m]` → `claude-fable-5`, repeated groups peel one by one.
/// The suffix names a context window, not a model, so it must not block the
/// family/version tokenization. Only *trailing* groups are stripped —
/// brackets elsewhere stay put (a mid-string `[1m]` would otherwise leak a
/// digit-led `1m` token that reads as a version).
fn strip_bracket_suffix(base: &str) -> &str {
    let mut base = base.trim_end();
    while let Some(rest) = base.strip_suffix(']') {
        match rest.rfind('[') {
            Some(open) => base = base[..open].trim_end(),
            None => break,
        }
    }
    base
}

/// Split a token at its letter→digit boundaries so glued family+version ids
/// tokenize like their hyphenated forms (D14): `sonnet5` → `["sonnet", "5"]`.
/// Digit→letter boundaries are NOT split — digit-led alphanumeric versions
/// like `4o` are one identity token (see `extract_version`).
fn split_glued_digits(token: &str) -> impl Iterator<Item = &str> {
    let mut parts = Vec::new();
    let mut start = 0;
    let bytes = token.as_bytes();
    for i in 1..bytes.len() {
        if bytes[i - 1].is_ascii_alphabetic() && bytes[i].is_ascii_digit() {
            parts.push(&token[start..i]);
            start = i;
        }
    }
    parts.push(&token[start..]);
    parts.into_iter()
}

/// Find a known model family in a raw id or display string and render it as
/// `"{Family} {version}[ {Variant}]"`; `None` when no family token matches.
/// This is the family-matching core of [`normalize_model_name`], exposed
/// separately so the effective-model resolution (D13) can tell "resolved to a
/// known family" apart from the raw passthrough — display strings like
/// `"Opus 4.8 with 1M context · Best for everyday, complex tasks"` tokenize
/// on spaces and resolve to `"Opus 4.8"`.
pub fn known_family_model_name(raw: &str) -> Option<String> {
    // Path-style provider prefixes ("anthropic/claude-...") never carry model
    // identity — keep only the final segment. Trailing bracketed suffixes
    // ("[1m]") and glued family+digit tokens ("sonnet5") are defense-in-depth
    // for explicit option ids that bypass the configOptions resolution (D14).
    let base = raw.trim().rsplit('/').next().unwrap_or(raw);
    let base = strip_bracket_suffix(base);
    let lower = base.to_ascii_lowercase();
    let tokens: Vec<&str> = lower
        .split(['-', '_', ' ', ':', '@'])
        .filter(|t| !t.is_empty())
        .flat_map(split_glued_digits)
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
            return Some(name);
        }
    }
    None
}

/// First candidate that resolves to a known model family WITH a version
/// (bare "Opus" is rejected — it would merge sibling versions and, persisted,
/// is indistinguishable from a real option id in the post-session
/// model-application gate). Shared by the session-open effective-model
/// resolutions (D13/D14) and the ACP catalog's default-row resolution.
pub(crate) fn version_bearing_display<'a>(
    candidates: impl IntoIterator<Item = &'a str>,
) -> Option<String> {
    candidates
        .into_iter()
        .filter_map(known_family_model_name)
        .find(|name| name.chars().any(|c| c.is_ascii_digit()))
}

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
    known_family_model_name(trimmed)
        .unwrap_or_else(|| trimmed.rsplit('/').next().unwrap_or(trimmed).to_string())
}

/// Whether a stored session model is absent or a placeholder that names no
/// real model: `None`, blank, or a bare/compound `default` sentinel
/// (`"default"`, `"claude-code:default"`). Placeholder models trigger the
/// session-open effective-model resolution (D13) and fall back to the
/// provider id in usage-stats keys.
pub(crate) fn is_placeholder_model(model: Option<&str>) -> bool {
    let Some(m) = model.map(str::trim).filter(|m| !m.is_empty()) else {
        return true;
    };
    let bare = m.split_once(':').map(|(_, b)| b).unwrap_or(m).trim();
    bare.is_empty() || bare.eq_ignore_ascii_case("default")
}

/// The `usage_stats_hourly` model key for a session (D13/D14 ladder): the
/// session's `resolved_model` (the display identity resolved from the
/// provider's `configOptions[id="model"]` at session open — the effective
/// model for a placeholder session, D13, or an explicit pick's display name,
/// D14) wins when present, so both land in the same row as each other; a
/// real (non-placeholder) model without a resolution normalizes via
/// [`normalize_model_name`]; a placeholder/absent model without a resolution
/// falls back to `provider_id` — the session's *resolved* provider id
/// (callers compute it with [`crate::agent_session::resolve_provider_id`]
/// when the session row is readable, and pass `None` when even the provider
/// is unknowable, e.g. the row read failed). `"unknown"` only on that
/// unknowable tail.
pub fn stats_model_key(
    raw_model: Option<&str>,
    resolved_model: Option<&str>,
    provider_id: Option<&str>,
) -> String {
    let resolved = resolved_model.map(str::trim).filter(|r| !r.is_empty());
    if let Some(display) = resolved {
        return normalize_model_name(display);
    }
    if !is_placeholder_model(raw_model) {
        return normalize_model_name(raw_model.unwrap_or(""));
    }
    provider_id
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .map(|p| p.to_ascii_lowercase())
        .unwrap_or_else(|| UNKNOWN_MODEL.to_string())
}

/// The `usage_stats_hourly` provider key for a session: the resolved provider
/// id (callers compute it with [`crate::agent_session::resolve_provider_id`]
/// when the session row is readable), trimmed and lowercased; `"unknown"`
/// when the provider is unknowable — the session row read failed, or the row
/// predates provider attribution.
pub fn stats_provider_key(provider_id: Option<&str>) -> String {
    provider_id
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .map(|p| p.to_ascii_lowercase())
        .unwrap_or_else(|| UNKNOWN_PROVIDER.to_string())
}

/// Record one agent-session start (D2: *sessions* = agent sessions) into the
/// current UTC hour bucket of `usage_stats_hourly`, keyed by the session's
/// [`stats_model_key`] (normalized model, falling back to the resolved
/// provider id when no model is resolved at creation time — D13). `provider`
/// is the session's raw `provider` field; the resolved id follows the spawn
/// precedence (compound model prefix → provider field → configured default
/// (`providers.active`) → hardcoded default provider), though this call site
/// has no settings to offer and always passes `None` for the configured
/// default.
/// Best-effort: errors are logged, never propagated — stats bookkeeping must
/// not fail `agent.create`.
pub async fn record_session_started(
    store: &Store,
    raw_model: Option<&str>,
    provider: Option<&str>,
) {
    let now = OffsetDateTime::now_utc();
    let bucket = hour_bucket_utc(now);
    let local = recording_local_offset().map(|o| local_stamp(now, o));
    let provider_id = crate::agent_session::resolve_provider_id(raw_model, provider, None);
    // No resolved display model exists at creation time — the configOptions
    // resolution (D13/D14) only happens at session open.
    let model = stats_model_key(raw_model, None, Some(&provider_id));
    let provider_key = stats_provider_key(Some(&provider_id));
    let delta = UsageStatsDelta {
        sessions_started: 1,
        ..Default::default()
    };
    if let Err(e) = store
        .add_usage_stats(&bucket, &model, &provider_key, local.as_ref(), &delta)
        .await
    {
        tracing::warn!(error = %e, "record session-start usage stats failed");
    }
}

/// Record one lines-changed delta (D5: period-scoped true totals, accrued as
/// attribution is recorded) into the current UTC hour bucket of
/// `usage_stats_hourly`, keyed by the acting agent's [`stats_model_key`]
/// (normalized session model, falling back to the session's resolved
/// provider id when the model cannot be resolved; `"unknown"` only when the
/// session row is unreadable — D13). Zero deltas and changes with no
/// attributed agent (manual/user edits are not agentic activity) are
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
    let (raw_model, resolved_model, provider_id) = match store
        .get_agent_session_token_usage(workspace_id, &AgentId(agent_id.to_string()))
        .await
    {
        Ok((model, resolved, provider, _)) => {
            let provider_id = crate::agent_session::resolve_provider_id(
                model.as_deref(),
                provider.as_deref(),
                None,
            );
            (model, resolved, Some(provider_id))
        }
        Err(e) => {
            tracing::warn!(agent = %agent_id, error = %e, "read agent model for lines-changed stats failed");
            (None, None, None)
        }
    };
    let now = OffsetDateTime::now_utc();
    let bucket = hour_bucket_utc(now);
    let local = recording_local_offset().map(|o| local_stamp(now, o));
    let model = stats_model_key(
        raw_model.as_deref(),
        resolved_model.as_deref(),
        provider_id.as_deref(),
    );
    let provider_key = stats_provider_key(provider_id.as_deref());
    let delta = UsageStatsDelta {
        lines_added,
        lines_deleted,
        ..Default::default()
    };
    if let Err(e) = store
        .add_usage_stats(&bucket, &model, &provider_key, local.as_ref(), &delta)
        .await
    {
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
            thought_tokens: 0,
            cost: None,
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
    fn delta_subtracts_thought_tokens_too() {
        let with_thoughts = |t: u64| TokenUsageTotals {
            thought_tokens: t,
            ..totals(100, 40, 0, 0)
        };
        let delta = turn_token_delta(Some(&with_thoughts(30)), &with_thoughts(75));
        assert_eq!(delta.thought_tokens, 45);
        // A regression clamps like the other counters.
        let clamped = turn_token_delta(Some(&with_thoughts(75)), &with_thoughts(30));
        assert_eq!(clamped.thought_tokens, 0);
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
    fn known_family_resolves_display_and_description_strings() {
        // D13: the resolved display strings from a claude-agent-acp
        // configOptions payload contain the family inside prose — space
        // tokenization must find it.
        assert_eq!(
            known_family_model_name("Opus 4.8 with 1M context · Best for everyday, complex tasks"),
            Some("Opus 4.8".to_string())
        );
        assert_eq!(
            known_family_model_name("Sonnet 5 · Efficient for routine tasks"),
            Some("Sonnet 5".to_string())
        );
        // No family token → None (unlike normalize_model_name, no passthrough).
        assert_eq!(known_family_model_name("Default (recommended)"), None);
        assert_eq!(known_family_model_name("my-custom-model"), None);
    }

    #[test]
    fn placeholder_model_detection() {
        assert!(is_placeholder_model(None));
        assert!(is_placeholder_model(Some("")));
        assert!(is_placeholder_model(Some("   ")));
        assert!(is_placeholder_model(Some("default")));
        assert!(is_placeholder_model(Some("Default")));
        assert!(is_placeholder_model(Some("claude-code:default")));
        assert!(is_placeholder_model(Some("claude-code:")));
        assert!(!is_placeholder_model(Some("opus")));
        assert!(!is_placeholder_model(Some("claude-code:opus[1m]")));
        assert!(!is_placeholder_model(Some("claude-code:Opus 4.8")));
    }

    #[test]
    fn stats_model_key_ladder_model_then_provider_then_unknown() {
        // A real model normalizes as before.
        assert_eq!(
            stats_model_key(Some("claude-code:Opus 4.8"), None, Some("claude-code")),
            "Opus 4.8"
        );
        assert_eq!(stats_model_key(Some("opus-4.8"), None, None), "Opus 4.8");
        // Placeholder/absent model without a resolution → resolved provider id.
        assert_eq!(
            stats_model_key(Some("claude-code:default"), None, Some("claude-code")),
            "claude-code"
        );
        assert_eq!(
            stats_model_key(None, None, Some("Claude-Code")),
            "claude-code"
        );
        assert_eq!(
            stats_model_key(Some("default"), None, Some("codex")),
            "codex"
        );
        // "unknown" only when the provider is unknowable too.
        assert_eq!(stats_model_key(None, None, None), UNKNOWN_MODEL);
        assert_eq!(
            stats_model_key(Some("default"), None, Some("  ")),
            UNKNOWN_MODEL
        );
    }

    #[test]
    fn stats_provider_key_normalizes_and_falls_back_to_unknown() {
        assert_eq!(stats_provider_key(Some(" Claude-Code ")), "claude-code");
        assert_eq!(stats_provider_key(None), UNKNOWN_PROVIDER);
        assert_eq!(stats_provider_key(Some("  ")), UNKNOWN_PROVIDER);
        // Pin the wire value literally, independent of the constant chain
        // (migration 0059 / PROTOCOL.md §5.36).
        assert_eq!(stats_provider_key(None), "unknown");
    }

    #[test]
    fn stats_model_key_prefers_resolved_display_model() {
        // D14: an explicit pick with a persisted display resolution lands in
        // the resolved display's row, not a raw-id row.
        assert_eq!(
            stats_model_key(
                Some("claude-code:claude-fable-5[1m]"),
                Some("Fable 5"),
                Some("claude-code")
            ),
            "Fable 5"
        );
        // Blank/absent resolution → the raw id normalizes as before.
        assert_eq!(
            stats_model_key(Some("claude-code:sonnet-5"), None, Some("claude-code")),
            "Sonnet 5"
        );
        assert_eq!(
            stats_model_key(
                Some("claude-code:sonnet-5"),
                Some("  "),
                Some("claude-code")
            ),
            "Sonnet 5"
        );
        // D13: a placeholder model with a persisted resolution (the effective
        // model resolved at session open) attributes to the resolved display,
        // not the provider fallback (monorepo#1534 — `model` itself stays a
        // placeholder now).
        assert_eq!(
            stats_model_key(
                Some("claude-code:default"),
                Some("Opus 4.8"),
                Some("claude-code")
            ),
            "Opus 4.8"
        );
        assert_eq!(
            stats_model_key(None, Some("Opus 4.8"), Some("claude-code")),
            "Opus 4.8"
        );
        assert_eq!(
            stats_model_key(Some("default"), Some("Fable 5"), Some("claude-code")),
            "Fable 5"
        );
        // Blank resolution on a placeholder → provider fallback as before.
        assert_eq!(
            stats_model_key(Some("claude-code:default"), Some("  "), Some("claude-code")),
            "claude-code"
        );
    }

    #[test]
    fn normalization_strips_bracket_suffix_and_splits_glued_digits() {
        // D14 defense-in-depth for explicit option ids that bypass the
        // configOptions resolution: bracketed context-variant suffixes strip,
        // glued family+version tokens split.
        assert_eq!(normalize_model_name("claude-fable-5[1m]"), "Fable 5");
        assert_eq!(normalize_model_name("sonnet5"), "Sonnet 5");
        assert_eq!(normalize_model_name("claude-code:sonnet5[1m]"), "Sonnet 5");
        assert_eq!(normalize_model_name("opus-4.8[200k][1m]"), "Opus 4.8");
        // Digit→letter boundaries stay glued ("4o" is one identity token).
        assert_eq!(normalize_model_name("gpt-4o[128k]"), "GPT 4o");
        // Unrecognized ids still pass through with the raw suffix intact —
        // the strip only feeds family matching, not the passthrough.
        assert_eq!(
            normalize_model_name("my-custom-model[1m]"),
            "my-custom-model[1m]"
        );
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
