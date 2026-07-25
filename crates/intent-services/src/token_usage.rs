//! Token-usage tallying for the workspace `TokenUsage` snapshot (§5.23 / §19.1),
//! shared by the end-of-turn live update (primary) and the daemon-internal
//! periodic reconciliation scan.
//!
//! **Live-primary / scan-reconciliation split**: the end-of-turn live update
//! (`agent_session.rs`) persists each session's cumulative usage snapshot and
//! re-aggregates immediately, so the workspace tally is fresh at every turn
//! end. The 300 s scan loop (in `lib.rs`, alongside the PR refresh loop) is a
//! reconciliation pass: per-session tallies combine the recreate baseline and
//! the stored snapshot when either is present ([`agent_token_tally`]) and fall
//! back to legacy per-message usage summing only for sessions that never
//! reported end-of-turn usage. The
//! aggregation here is pure and side-effect free so it is unit-testable
//! without a store or bus; only the read + `workspace:tokenUsage-changed` event
//! cross the wire (the scan itself has no RPC, §6.8).

use intent_acp::session::Usage;
use intent_core::{AgentSession, TokenUsage, TokenUsageTotals};
use serde_json::Value;

/// Model key used when an agent session has no recorded model (§5.23 fallback).
pub const UNKNOWN_MODEL: &str = "unknown";

/// One agent's contribution to the workspace tally: its `agent-{uuid}` id, the
/// effective model name, and the summed per-turn counters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentTokenTally {
    pub agent_id: String,
    pub model: String,
    pub totals: TokenUsageTotals,
}

/// Add `rhs` into `lhs` counter-by-counter (saturating, so a pathological tally
/// can never panic on overflow).
fn add_totals(lhs: &mut TokenUsageTotals, rhs: &TokenUsageTotals) {
    lhs.input_tokens = lhs.input_tokens.saturating_add(rhs.input_tokens);
    lhs.output_tokens = lhs.output_tokens.saturating_add(rhs.output_tokens);
    lhs.cache_read_tokens = lhs.cache_read_tokens.saturating_add(rhs.cache_read_tokens);
    lhs.cache_creation_tokens = lhs
        .cache_creation_tokens
        .saturating_add(rhs.cache_creation_tokens);
}

/// Roll per-agent tallies up into the materialized `TokenUsage` snapshot:
/// `byAgentId` keyed by `agent-{uuid}`, `byModel` keyed by the effective model
/// name (`"unknown"` fallback), plus the workspace-wide `totals`. `lastScanAt`
/// is stamped by the caller (the scan job records the RFC-3339 scan time).
pub fn aggregate_token_usage(tallies: &[AgentTokenTally]) -> TokenUsage {
    let mut usage = TokenUsage::default();
    for tally in tallies {
        add_totals(&mut usage.totals, &tally.totals);
        add_totals(
            usage.by_agent_id.entry(tally.agent_id.clone()).or_default(),
            &tally.totals,
        );
        let model = if tally.model.is_empty() {
            UNKNOWN_MODEL.to_string()
        } else {
            tally.model.clone()
        };
        add_totals(usage.by_model.entry(model).or_default(), &tally.totals);
    }
    usage
}

/// Sum the per-turn token counters recorded on one agent session's transcript.
/// Each message MAY carry a `usage` object — checked at the content block's
/// top level and under `_meta` — whose `inputTokens`/`outputTokens`/
/// `cacheReadTokens`/`cacheCreationTokens` fields are summed; absent fields and
/// messages without usage contribute zero. The model key is the session's
/// `model` (empty → `"unknown"` is applied by [`aggregate_token_usage`]).
pub fn session_token_tally(session: &AgentSession) -> AgentTokenTally {
    let mut totals = TokenUsageTotals::default();
    for message in &session.messages {
        if let Some(usage) = extract_message_usage(&message.content) {
            add_totals(&mut totals, &usage);
        }
    }
    AgentTokenTally {
        agent_id: session.id.0.clone(),
        model: session.model.clone().unwrap_or_default(),
        totals,
    }
}

/// Interpret one end-of-turn ACP `Usage` report as the session's cumulative
/// [`TokenUsageTotals`] snapshot.
///
/// **Cumulative-replace interpretation** (isolated here on purpose): the ACP
/// `unstable_end_turn_token_usage` extension documents its counters as totals
/// "across all turns" of the ACP session, so each report REPLACES the
/// session's previous snapshot — reports are never summed. The ACP RFD is
/// still Draft; if the semantics flip to per-turn deltas later, this function
/// (and its callers' replace-vs-add choice) is the single place to change.
///
/// Field mapping: `cachedReadTokens` → `cacheReadTokens` and
/// `cachedWriteTokens` → `cacheCreationTokens`; absent optional counters map
/// to zero. `thoughtTokens` has no slot in [`TokenUsageTotals`] and is
/// intentionally dropped (as is `totalTokens`, which is derivable).
///
/// **Recreate baseline** (monorepo#737): counters are cumulative per *ACP*
/// session, so when the resume-impossible fallback recreates the ACP session,
/// the fresh session restarts from zero. The store banks the outgoing
/// session's snapshot into `token_usage_baseline` atomically with the id swap
/// (`Store::replace_acp_session_id`); the agent's effective total is then
/// `baseline + snapshot` (see [`agent_token_tally`]). The snapshot itself
/// stays replace-only — the baseline lives outside it.
pub fn snapshot_from_turn_usage(usage: &Usage) -> TokenUsageTotals {
    TokenUsageTotals {
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        cache_read_tokens: usage.cached_read_tokens.unwrap_or(0),
        cache_creation_tokens: usage.cached_write_tokens.unwrap_or(0),
    }
}

/// One agent's tally for the workspace roll-up (§5.23): the effective total
/// is `baseline + snapshot`, component-wise, where either part may be absent.
/// The `baseline` banks the cumulative totals of ACP sessions folded away by
/// a recreate (monorepo#737) and the `snapshot` is the current ACP session's
/// cumulative end-of-turn report (see [`snapshot_from_turn_usage`]), so their
/// sum never double-counts. A baseline WITHOUT a snapshot uses the baseline
/// alone — never baseline + message sums, since the pre-recreate snapshot
/// already superseded the message metadata. Only when BOTH are absent (the
/// session never reported end-of-turn usage and was never recreated) does the
/// tally fall back to summing per-message usage metadata via
/// [`agent_token_tally_from_contents`].
pub fn agent_token_tally(
    agent_id: &str,
    model: Option<&str>,
    baseline: Option<&TokenUsageTotals>,
    snapshot: Option<&TokenUsageTotals>,
    contents: &[serde_json::Value],
) -> AgentTokenTally {
    if baseline.is_none() && snapshot.is_none() {
        return agent_token_tally_from_contents(agent_id, model, contents);
    }
    let mut totals = baseline.cloned().unwrap_or_default();
    if let Some(snapshot) = snapshot {
        add_totals(&mut totals, snapshot);
    }
    AgentTokenTally {
        agent_id: agent_id.to_string(),
        model: model.unwrap_or("").to_string(),
        totals,
    }
}

/// Lightweight token tally from extracted usage data (finding F2: avoid full
/// AgentSession hydration). Takes an agent_id, model, and the message content
/// JSON list (usage metadata is embedded in each content block). This is the
/// incremental-scan counterpart to [`session_token_tally`].
pub fn agent_token_tally_from_contents(
    agent_id: &str,
    model: Option<&str>,
    contents: &[serde_json::Value],
) -> AgentTokenTally {
    let mut totals = TokenUsageTotals::default();
    for content in contents {
        if let Some(usage) = extract_message_usage(content) {
            add_totals(&mut totals, &usage);
        }
    }
    AgentTokenTally {
        agent_id: agent_id.to_string(),
        model: model.unwrap_or("").to_string(),
        totals,
    }
}

/// Extract a `TokenUsageTotals` from a message content block's optional `usage`
/// object (top-level or under `_meta`). Returns `None` when neither carries a
/// usage object, so messages without usage metadata contribute nothing.
fn extract_message_usage(content: &Value) -> Option<TokenUsageTotals> {
    let usage = content
        .get("usage")
        .or_else(|| content.get("_meta").and_then(|m| m.get("usage")))?;
    let field = |key: &str| usage.get(key).and_then(Value::as_u64).unwrap_or(0);
    Some(TokenUsageTotals {
        input_tokens: field("inputTokens"),
        output_tokens: field("outputTokens"),
        cache_read_tokens: field("cacheReadTokens"),
        cache_creation_tokens: field("cacheCreationTokens"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn totals(i: u64, o: u64, r: u64, c: u64) -> TokenUsageTotals {
        TokenUsageTotals {
            input_tokens: i,
            output_tokens: o,
            cache_read_tokens: r,
            cache_creation_tokens: c,
        }
    }

    #[test]
    fn aggregates_per_agent_per_model_and_totals() {
        let tallies = vec![
            AgentTokenTally {
                agent_id: "agent-1".into(),
                model: "opus-4.8".into(),
                totals: totals(100, 20, 8, 1),
            },
            AgentTokenTally {
                agent_id: "agent-2".into(),
                model: "opus-4.8".into(),
                totals: totals(50, 10, 4, 0),
            },
            AgentTokenTally {
                agent_id: "agent-3".into(),
                model: String::new(),
                totals: totals(5, 5, 0, 0),
            },
        ];
        let usage = aggregate_token_usage(&tallies);
        assert_eq!(usage.totals, totals(155, 35, 12, 1));
        assert_eq!(usage.by_agent_id["agent-1"], totals(100, 20, 8, 1));
        assert_eq!(usage.by_model["opus-4.8"], totals(150, 30, 12, 1));
        assert_eq!(usage.by_model[UNKNOWN_MODEL], totals(5, 5, 0, 0));
        assert!(usage.last_scan_at.is_none());
    }

    #[test]
    fn extracts_usage_from_top_level_and_meta() {
        let top = serde_json::json!({ "usage": { "inputTokens": 12, "outputTokens": 3 } });
        assert_eq!(extract_message_usage(&top), Some(totals(12, 3, 0, 0)));
        let meta = serde_json::json!({ "_meta": { "usage": { "cacheReadTokens": 7 } } });
        assert_eq!(extract_message_usage(&meta), Some(totals(0, 0, 7, 0)));
        assert_eq!(
            extract_message_usage(&serde_json::json!({ "content": "hi" })),
            None
        );
    }

    #[test]
    fn snapshot_from_turn_usage_maps_cached_fields() {
        // `Usage` is #[non_exhaustive]; construct via its camelCase wire form.
        let usage: Usage = serde_json::from_value(serde_json::json!({
            "totalTokens": 120,
            "inputTokens": 70,
            "outputTokens": 50,
            "thoughtTokens": 8,
            "cachedReadTokens": 30,
            "cachedWriteTokens": 4
        }))
        .expect("usage deserializes");
        assert_eq!(snapshot_from_turn_usage(&usage), totals(70, 50, 30, 4));

        // Absent optional counters map to zero.
        let sparse: Usage = serde_json::from_value(serde_json::json!({
            "totalTokens": 3,
            "inputTokens": 2,
            "outputTokens": 1
        }))
        .expect("sparse usage deserializes");
        assert_eq!(snapshot_from_turn_usage(&sparse), totals(2, 1, 0, 0));
    }

    #[test]
    fn agent_token_tally_prefers_snapshot_over_contents() {
        let contents = vec![serde_json::json!({ "usage": { "inputTokens": 999 } })];
        let snapshot = totals(70, 50, 30, 4);
        // Snapshot present → used as-is; message usage is NOT added on top.
        let tally = agent_token_tally(
            "agent-s",
            Some("opus-4.8"),
            None,
            Some(&snapshot),
            &contents,
        );
        assert_eq!(tally.totals, snapshot);
        assert_eq!(tally.model, "opus-4.8");
        // No baseline, no snapshot → falls back to summing message usage.
        let fallback = agent_token_tally("agent-s", None, None, None, &contents);
        assert_eq!(fallback.totals, totals(999, 0, 0, 0));
    }

    #[test]
    fn agent_token_tally_combines_baseline_and_snapshot() {
        let contents = vec![serde_json::json!({ "usage": { "inputTokens": 999 } })];
        let baseline = totals(100, 80, 10, 2);
        let snapshot = totals(5, 3, 1, 1);
        // Baseline + snapshot → component-wise sum; message usage ignored.
        let both = agent_token_tally(
            "agent-r",
            Some("opus-4.8"),
            Some(&baseline),
            Some(&snapshot),
            &contents,
        );
        assert_eq!(both.totals, totals(105, 83, 11, 3));
        // Baseline WITHOUT a snapshot → baseline alone, never baseline +
        // message sums (the pre-recreate snapshot superseded the messages).
        let baseline_only = agent_token_tally(
            "agent-r",
            Some("opus-4.8"),
            Some(&baseline),
            None,
            &contents,
        );
        assert_eq!(baseline_only.totals, baseline);
    }

    #[test]
    fn mixed_snapshot_and_fallback_sessions_aggregate_correctly() {
        // One session with a persisted end-of-turn snapshot (its message
        // usage must be ignored), one snapshot-less session that falls back
        // to per-message summing.
        let snap_contents = vec![serde_json::json!({ "usage": { "inputTokens": 500 } })];
        let snapshot = totals(40, 30, 20, 10);
        let legacy_contents = vec![
            serde_json::json!({ "usage": { "inputTokens": 7, "outputTokens": 3 } }),
            serde_json::json!({ "_meta": { "usage": { "cacheReadTokens": 2 } } }),
        ];
        let tallies = vec![
            agent_token_tally(
                "agent-snap",
                Some("opus-4.8"),
                None,
                Some(&snapshot),
                &snap_contents,
            ),
            agent_token_tally(
                "agent-legacy",
                Some("sonnet-5"),
                None,
                None,
                &legacy_contents,
            ),
        ];
        let usage = aggregate_token_usage(&tallies);
        assert_eq!(usage.by_agent_id["agent-snap"], totals(40, 30, 20, 10));
        assert_eq!(usage.by_agent_id["agent-legacy"], totals(7, 3, 2, 0));
        assert_eq!(usage.totals, totals(47, 33, 22, 10));
        assert_eq!(usage.by_model["opus-4.8"], totals(40, 30, 20, 10));
        assert_eq!(usage.by_model["sonnet-5"], totals(7, 3, 2, 0));
    }

    #[test]
    fn lightweight_tally_from_contents_matches_session_tally() {
        // Fixture: 3 message contents with usage metadata
        let contents = vec![
            serde_json::json!({ "usage": { "inputTokens": 10, "outputTokens": 2 } }),
            serde_json::json!({ "_meta": { "usage": { "cacheReadTokens": 5 } } }),
            serde_json::json!({ "text": "no usage here" }),
        ];
        let tally = agent_token_tally_from_contents("agent-test", Some("sonnet-5"), &contents);
        assert_eq!(tally.agent_id, "agent-test");
        assert_eq!(tally.model, "sonnet-5");
        assert_eq!(tally.totals, totals(10, 2, 5, 0));

        // Empty model fallback
        let tally2 = agent_token_tally_from_contents("agent-2", None, &contents);
        assert_eq!(tally2.model, "");
    }
}
