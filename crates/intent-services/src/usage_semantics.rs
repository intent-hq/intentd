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
//! grok reports no end-of-turn counters either, but its per-turn bill is
//! recoverable from `PromptResponse._meta.usage` and is handled separately
//! (intent-hq/intent#3801) — it stays on the [`Cumulative`] default here.
//!
//! [`Cumulative`]: UsageReportSemantics::Cumulative

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
/// - `codex`, `opencode`, `unsloth` report the last request → `LastRequest`
/// - `pi`, `auggie`, `droid`, `cortex` never report → `NoReport`
/// - everything else (incl. `mock`) → `Cumulative`
pub(crate) fn usage_report_semantics(provider_id: Option<&str>) -> UsageReportSemantics {
    let normalized = provider_id.map(|p| p.trim().to_ascii_lowercase());
    match normalized.as_deref() {
        Some("claude-code") => UsageReportSemantics::PerTurn,
        Some("codex" | "opencode" | "unsloth") => UsageReportSemantics::LastRequest,
        Some("pi" | "auggie" | "droid" | "cortex") => UsageReportSemantics::NoReport,
        _ => UsageReportSemantics::Cumulative,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn classifies_every_known_provider() {
        assert_eq!(
            usage_report_semantics(Some("claude-code")),
            UsageReportSemantics::PerTurn
        );
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
        // grok is handled by the separate _meta.usage path (#3801); mock and
        // unknown/future providers take the spec-default.
        for id in ["grok", "mock", "some-future-provider"] {
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
}
