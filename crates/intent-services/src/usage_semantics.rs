//! Provider-keyed usage-report semantics — the quirk table behind the
//! turn-end usage accounting seam (intent-hq/intent#3794, #3795).
//!
//! The ACP `unstable_end_turn_token_usage` extension documents its counters
//! as cumulative totals "across all turns" of the ACP session, but adapters
//! diverge in practice: claude-agent-acp resets its tally every turn
//! (per-turn counters), while codex-acp, opencode and the opencode-based
//! unsloth report only the LAST request's counters. Treating those as
//! cumulative snapshots made REPLACE drop every earlier turn — the multi-turn
//! undercount of #3794/#3795. This module classifies each provider so the
//! accounting seam (`persist_turn_token_usage` / `record_turn_usage_stats`
//! in `agent_session.rs`) folds reports with the right operation; the quirk
//! knowledge lives here and nowhere else.
//!
//! grok never populates the standard `usage` field but attaches a complete
//! whole-prompt bill at `PromptResponse._meta.usage`; the seam synthesizes a
//! standard report from it via [`prompt_meta_usage_bill`]
//! (intent-hq/intent#3803) and the classification folds that bill as the
//! per-turn report it is ([`PerTurn`]).
//!
//! [`PerTurn`]: UsageReportSemantics::PerTurn

use intent_acp::session::{Meta, Usage};
use intent_core::UsageCost;

/// How one provider's end-of-turn `Usage` counters relate to the ACP
/// session's running totals, keying the replace-vs-sum choice at the
/// accounting seam.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UsageReportSemantics {
    /// Spec-compliant cumulative session totals: each report REPLACES the
    /// stored snapshot, and the per-turn stats delta is the saturating
    /// subtraction against the previous snapshot. The default for unknown
    /// (future) providers — the spec-faithful assumption.
    Cumulative,
    /// The report covers the just-finished turn only (claude-agent-acp
    /// resets its tally every turn): reports SUM into the stored snapshot,
    /// and the report itself IS the per-turn stats delta.
    PerTurn,
    /// The report covers only the last model request of the turn (codex-acp
    /// and opencode report the final request's counters): summed like
    /// [`PerTurn`](Self::PerTurn) as the accepted approximation — an
    /// undercount of multi-request turns, but strictly better than REPLACE
    /// dropping whole turns.
    LastRequest,
    /// The provider sends no end-of-turn usage report at all — sessions
    /// legitimately record zero and there is nothing to ingest. Distinct
    /// from [`Cumulative`](Self::Cumulative) so the classification is
    /// explicit; if a report ever does arrive it is folded with the
    /// spec-default REPLACE.
    NoReport,
}

impl UsageReportSemantics {
    /// Whether reports SUM into the stored session snapshot (and stand as
    /// their own per-turn stats delta) instead of replacing it.
    pub(crate) fn sums_reports(self) -> bool {
        matches!(self, Self::PerTurn | Self::LastRequest)
    }
}

/// Classify a resolved provider id (see
/// [`crate::agent_session::resolve_provider_id`]). Matching is trimmed and
/// case-insensitive; `None` and unknown ids take the spec-default
/// [`Cumulative`](UsageReportSemantics::Cumulative).
///
/// Classification per the ACP stats audit (intent-hq/intent#3794, #3795):
/// - `claude-code` resets its tally every turn → `PerTurn`
/// - `grok` bills per prompt via `_meta.usage` (#3803, synthesized by
///   [`prompt_meta_usage_bill`]) → `PerTurn`
/// - `codex`, `opencode`, `unsloth` report the last request → `LastRequest`
/// - `pi`, `auggie`, `droid`, `cortex` never report → `NoReport`
/// - everything else (incl. `mock`) → `Cumulative`
pub(crate) fn usage_report_semantics(provider_id: Option<&str>) -> UsageReportSemantics {
    let normalized = provider_id.map(|p| p.trim().to_ascii_lowercase());
    match normalized.as_deref() {
        Some("claude-code" | "grok") => UsageReportSemantics::PerTurn,
        Some("codex" | "opencode" | "unsloth") => UsageReportSemantics::LastRequest,
        Some("pi" | "auggie" | "droid" | "cortex") => UsageReportSemantics::NoReport,
        _ => UsageReportSemantics::Cumulative,
    }
}

/// Whether the provider reports usage via the `PromptResponse._meta.usage`
/// whole-prompt bill instead of the standard `usage` field — grok only
/// (intent-hq/intent#3803, audit §8). Gates the [`prompt_meta_usage_bill`]
/// synthesis at the turn-end seam, and — because that bill's cost covers the
/// just-finished prompt only — also keys the per-turn cost SUM in
/// `persist_turn_token_usage` (standard `usage_update` costs are cumulative
/// per ACP session and REPLACE instead). Matching is trimmed and
/// case-insensitive like [`usage_report_semantics`].
pub(crate) fn reads_prompt_meta_usage(provider_id: Option<&str>) -> bool {
    provider_id.is_some_and(|p| p.trim().eq_ignore_ascii_case("grok"))
}

/// grok's cost unit: `USD_TICKS_PER_USD` = 1e10 ticks per $1 (audit §8.2,
/// xai-grok-shell `extensions/notification.rs`).
const USD_TICKS_PER_USD: f64 = 1e10;

/// The whole-prompt bill totals grok serializes at
/// `PromptResponse._meta.usage` (`PromptUsage` with its `PromptUsageModel`
/// totals flattened in; camelCase; every counter `#[serde(default)]`).
/// Audited @ xai-org/grok-build `bc7f02e` (v1.0.12) — audit §8.2. Unknown
/// siblings (`modelUsage`, `numTurns`, `usageIsIncomplete`, `modelCalls`,
/// `apiDurationMs`, …) are ignored.
#[derive(Debug, Default, serde::Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct GrokPromptUsage {
    /// Full prompt input INCLUDING cache reads (and cache creation, which is
    /// "folded into `input_tokens` on the ACP wire" per the grok source doc).
    input_tokens: u64,
    /// Output including reasoning; `reasoningTokens` is an explicit subset.
    output_tokens: u64,
    cached_read_tokens: u64,
    cache_creation_tokens: u64,
    reasoning_tokens: u64,
    /// Server cost in USD ticks; grok scrubs it fail-closed (absent) when the
    /// bill is partial or incomplete, so absence means unknown — never free.
    cost_usd_ticks: Option<i64>,
    /// Belt-and-braces: grok stamps this when any shown cost is a partial
    /// sum; a partial figure is untrustworthy, drop it like an absent one.
    cost_is_partial: bool,
}

/// Parse grok's `_meta.usage` whole-prompt bill into a standard end-of-turn
/// report plus its optional cost (intent-hq/intent#3803, audit §8.4). The
/// caller gates on [`reads_prompt_meta_usage`]; the synthesized report then
/// flows through the ordinary accounting seam under grok's
/// [`PerTurn`](UsageReportSemantics::PerTurn) classification (SUM — it is a
/// whole-prompt bill).
///
/// Normalization to intentd's disjoint buckets: grok's `inputTokens` is the
/// FULL prompt input, so cache reads AND cache creation (both folded in on
/// grok's ACP wire) are subtracted out; `reasoningTokens` is a subset of
/// `outputTokens` and moves to the disjoint `thoughtTokens` bucket.
/// `costUsdTicks` ÷ 1e10 = USD; absent or partial cost yields tokens without
/// cost, never a zero cost. The sibling `_meta` last-call token fields and
/// `_meta.totalTokens` (a context-occupancy estimate) are deliberately not
/// read. `None` when there is no `usage` key, it does not parse, or the bill
/// is empty (no counters and no cost).
pub(crate) fn prompt_meta_usage_bill(meta: &Meta) -> Option<(Usage, Option<UsageCost>)> {
    let bill: GrokPromptUsage = serde_json::from_value(meta.get("usage")?.clone()).ok()?;
    // Trustworthy cost only: grok scrubs untrustworthy ticks to absent and
    // "absent when scrubbed, missing, or zero on the wire" — so non-positive
    // or partial figures count as no cost at all, never as $0.
    let cost_ticks = bill
        .cost_usd_ticks
        .filter(|&ticks| ticks > 0 && !bill.cost_is_partial);
    let empty = bill.input_tokens == 0
        && bill.output_tokens == 0
        && bill.cached_read_tokens == 0
        && bill.cache_creation_tokens == 0
        && bill.reasoning_tokens == 0;
    if empty && cost_ticks.is_none() {
        return None;
    }
    let usage = Usage::new(
        bill.input_tokens.saturating_add(bill.output_tokens),
        bill.input_tokens
            .saturating_sub(bill.cached_read_tokens)
            .saturating_sub(bill.cache_creation_tokens),
        bill.output_tokens.saturating_sub(bill.reasoning_tokens),
    )
    .thought_tokens(bill.reasoning_tokens)
    .cached_read_tokens(bill.cached_read_tokens)
    .cached_write_tokens(bill.cache_creation_tokens);
    let cost = cost_ticks.map(|ticks| UsageCost {
        #[allow(clippy::cast_precision_loss)]
        amount: ticks as f64 / USD_TICKS_PER_USD,
        currency: "USD".to_string(),
    });
    Some((usage, cost))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_every_known_provider() {
        // grok's _meta.usage bill is a whole-prompt (per-turn) report
        // (#3803), same SUM fold as claude-code's per-turn counters.
        for id in ["claude-code", "grok"] {
            assert_eq!(
                usage_report_semantics(Some(id)),
                UsageReportSemantics::PerTurn,
                "{id}"
            );
        }
        for id in ["codex", "opencode", "unsloth"] {
            assert_eq!(
                usage_report_semantics(Some(id)),
                UsageReportSemantics::LastRequest,
                "{id}"
            );
        }
        for id in ["pi", "auggie", "droid", "cortex"] {
            assert_eq!(
                usage_report_semantics(Some(id)),
                UsageReportSemantics::NoReport,
                "{id}"
            );
        }
        // mock and unknown/future providers take the spec-default.
        for id in ["mock", "some-future-provider"] {
            assert_eq!(
                usage_report_semantics(Some(id)),
                UsageReportSemantics::Cumulative,
                "{id}"
            );
        }
        assert_eq!(
            usage_report_semantics(None),
            UsageReportSemantics::Cumulative
        );
    }

    #[test]
    fn only_grok_reads_prompt_meta_usage() {
        assert!(reads_prompt_meta_usage(Some("grok")));
        assert!(reads_prompt_meta_usage(Some("  GroK ")));
        for id in [
            "claude-code",
            "codex",
            "opencode",
            "unsloth",
            "pi",
            "auggie",
            "droid",
            "cortex",
            "mock",
        ] {
            assert!(!reads_prompt_meta_usage(Some(id)), "{id}");
        }
        assert!(!reads_prompt_meta_usage(None));
    }

    #[test]
    fn matching_trims_and_ignores_case() {
        assert_eq!(
            usage_report_semantics(Some("  Claude-Code ")),
            UsageReportSemantics::PerTurn
        );
        assert_eq!(
            usage_report_semantics(Some("CODEX")),
            UsageReportSemantics::LastRequest
        );
    }

    #[test]
    fn only_per_turn_and_last_request_sum() {
        assert!(UsageReportSemantics::PerTurn.sums_reports());
        assert!(UsageReportSemantics::LastRequest.sums_reports());
        assert!(!UsageReportSemantics::Cumulative.sums_reports());
        assert!(!UsageReportSemantics::NoReport.sums_reports());
    }

    /// A `_meta` payload in the shape grok actually serializes (audit §8.2:
    /// `PromptResponseMeta` with the `PromptUsage` bill under `usage` —
    /// totals flattened, per-model map, `numTurns`; sibling last-call token
    /// fields and the context-occupancy `totalTokens` alongside).
    fn grok_meta(usage: serde_json::Value) -> Meta {
        let mut root = serde_json::json!({
            "sessionId": "sess-1",
            "requestId": "req-1",
            "promptId": "req-1",
            "totalTokens": 999_999,
            "modelId": "grok-code-1",
            "inputTokens": 11,
            "outputTokens": 7,
            "cachedReadTokens": 5,
            "reasoningTokens": 2,
        });
        root.as_object_mut().unwrap().insert("usage".into(), usage);
        root.as_object().unwrap().clone()
    }

    #[test]
    fn grok_bill_normalizes_to_disjoint_buckets_and_usd() {
        // inputTokens is the FULL prompt input: cache reads (3000) and cache
        // creation (500) folded in; reasoningTokens (400) ⊂ outputTokens.
        let meta = grok_meta(serde_json::json!({
            "inputTokens": 5000,
            "outputTokens": 1200,
            "totalTokens": 6200,
            "cachedReadTokens": 3000,
            "cacheCreationTokens": 500,
            "reasoningTokens": 400,
            "modelCalls": 3,
            "apiDurationMs": 8000,
            "costUsdTicks": 250_000_000i64,
            "modelUsage": {
                "grok-code-1": { "inputTokens": 5000, "outputTokens": 1200 }
            },
            "numTurns": 2
        }));
        let (usage, cost) = prompt_meta_usage_bill(&meta).expect("bill parsed");
        assert_eq!(usage.input_tokens, 1500, "5000 − 3000 read − 500 creation");
        assert_eq!(usage.output_tokens, 800, "1200 − 400 reasoning");
        assert_eq!(usage.thought_tokens, Some(400));
        assert_eq!(usage.cached_read_tokens, Some(3000));
        assert_eq!(usage.cached_write_tokens, Some(500));
        assert_eq!(usage.total_tokens, 6200);
        let cost = cost.expect("cost present");
        assert!((cost.amount - 0.025).abs() < 1e-12, "2.5e8 ticks = $0.025");
        assert_eq!(cost.currency, "USD");
    }

    #[test]
    fn grok_scrubbed_cost_yields_tokens_without_cost() {
        // grok scrubs costUsdTicks fail-closed on partial/incomplete bills —
        // absence means unknown, never free (§8.2).
        let meta = grok_meta(serde_json::json!({
            "inputTokens": 100,
            "outputTokens": 50,
            "totalTokens": 150,
            "usageIsIncomplete": true
        }));
        let (usage, cost) = prompt_meta_usage_bill(&meta).expect("tokens still parsed");
        assert_eq!(usage.input_tokens, 100);
        assert_eq!(usage.output_tokens, 50);
        assert_eq!(usage.thought_tokens, Some(0));
        assert!(cost.is_none(), "absent ticks → no cost, not $0");
    }

    #[test]
    fn grok_partial_cost_is_dropped() {
        let meta = grok_meta(serde_json::json!({
            "inputTokens": 100,
            "outputTokens": 50,
            "costUsdTicks": 100_000_000i64,
            "costIsPartial": true
        }));
        let (_, cost) = prompt_meta_usage_bill(&meta).expect("tokens parsed");
        assert!(cost.is_none(), "partial cost is untrustworthy → dropped");
    }

    #[test]
    fn grok_subset_overrun_saturates_at_zero() {
        // Defensive: a bill whose subsets exceed their supersets must not
        // underflow the disjoint buckets.
        let meta = grok_meta(serde_json::json!({
            "inputTokens": 10,
            "outputTokens": 5,
            "cachedReadTokens": 20,
            "reasoningTokens": 9
        }));
        let (usage, _) = prompt_meta_usage_bill(&meta).expect("parsed");
        assert_eq!(usage.input_tokens, 0);
        assert_eq!(usage.output_tokens, 0);
    }

    #[test]
    fn grok_empty_or_missing_bill_yields_none() {
        // No usage key at all.
        let mut meta = grok_meta(serde_json::json!({}));
        meta.remove("usage");
        assert!(prompt_meta_usage_bill(&meta).is_none(), "no usage key");
        // Empty bill: every counter zero/absent and no cost.
        let meta = grok_meta(serde_json::json!({ "numTurns": 1 }));
        assert!(prompt_meta_usage_bill(&meta).is_none(), "empty bill");
        // Malformed bill degrades to None, never an error.
        let meta = grok_meta(serde_json::json!("not-an-object"));
        assert!(prompt_meta_usage_bill(&meta).is_none(), "malformed bill");
        // Zero ticks alone is not a report either (grok omits zero costs).
        let meta = grok_meta(serde_json::json!({ "costUsdTicks": 0 }));
        assert!(prompt_meta_usage_bill(&meta).is_none(), "zero-tick bill");
    }
}
