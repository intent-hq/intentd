//! Token-usage tallying for the daemon-internal periodic scan job (§5.23 / §19.1).
//!
//! The scan loop (in `lib.rs`, alongside the PR refresh loop) lists a workspace's
//! agent sessions and rolls each session's per-turn token counters up into the
//! durable `TokenUsage` workspace field surfaced by `workspace.getTokenUsage`.
//! The aggregation here is pure and side-effect free so it is unit-testable
//! without a store or bus; only the read + `workspace:tokenUsage-changed` event
//! cross the wire (the scan itself has no RPC, §6.8).

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
