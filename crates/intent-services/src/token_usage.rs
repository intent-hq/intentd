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

use std::collections::BTreeMap;

use intent_acp::session::Usage;
use intent_core::{token_usage_reported, TokenUsage, TokenUsageTotals, UsageCost};
use serde_json::Value;

/// Model key used when an agent session has no recorded model (§5.23 fallback).
pub(crate) const UNKNOWN_MODEL: &str = "unknown";

/// Provider key used when a session's provider is unknowable (§5.36 fallback).
/// Same `"unknown"` sentinel as [`UNKNOWN_MODEL`] — the two usage-stats
/// dimensions share the value, but the alias keeps provider-side call sites
/// readable.
pub(crate) const UNKNOWN_PROVIDER: &str = UNKNOWN_MODEL;

/// One agent's contribution to the workspace tally: its `agent-{uuid}` id, the
/// effective model name, and the summed per-turn counters.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentTokenTally {
    pub agent_id: String,
    pub model: String,
    pub totals: TokenUsageTotals,
}

/// Add `rhs` into `lhs` counter-by-counter (saturating, so a pathological tally
/// can never panic on overflow). The optional `cost` is NOT folded here:
/// bucket-level cost accumulation is currency-aware and lives in
/// [`CostBucket`] (the roll-up) / [`UsageCost::merge`] (the pairwise
/// baseline+snapshot fold).
fn add_totals(lhs: &mut TokenUsageTotals, rhs: &TokenUsageTotals) {
    lhs.input_tokens = lhs.input_tokens.saturating_add(rhs.input_tokens);
    lhs.output_tokens = lhs.output_tokens.saturating_add(rhs.output_tokens);
    lhs.cache_read_tokens = lhs.cache_read_tokens.saturating_add(rhs.cache_read_tokens);
    lhs.cache_creation_tokens = lhs
        .cache_creation_tokens
        .saturating_add(rhs.cache_creation_tokens);
    lhs.thought_tokens = lhs.thought_tokens.saturating_add(rhs.thought_tokens);
}

/// Currency-aware cost accumulator for one roll-up bucket (`totals`, one
/// `byAgentId` entry, one `byModel` entry): amounts sum per ISO 4217 code and
/// the bucket resolves to the currency with the largest sum. Mixing
/// currencies inside a bucket is pathological (an agent switching providers
/// mid-workspace) — picking the dominant currency keeps the wire shape a
/// single figure rather than inventing a conversion. A bucket with no
/// reported cost resolves to `None` (never a fabricated zero).
#[derive(Default)]
struct CostBucket(BTreeMap<String, f64>);

impl CostBucket {
    /// Non-finite amounts are ignored: a `NaN` would win `resolve`'s
    /// `total_cmp` (it sorts above every finite value) and then serialize as
    /// `null`, a shape the protocol does not describe.
    fn add(&mut self, cost: Option<&UsageCost>) {
        if let Some(cost) = cost.filter(|c| c.amount.is_finite()) {
            *self.0.entry(cost.currency.clone()).or_insert(0.0) += cost.amount;
        }
    }

    fn resolve(self) -> Option<UsageCost> {
        self.0
            .into_iter()
            .filter(|(_, amount)| amount.is_finite())
            .max_by(|a, b| a.1.total_cmp(&b.1))
            .map(|(currency, amount)| UsageCost { amount, currency })
    }
}

/// Roll per-agent tallies up into the materialized `TokenUsage` snapshot:
/// `byAgentId` keyed by `agent-{uuid}`, `byModel` keyed by the effective model
/// name (`"unknown"` fallback), plus the workspace-wide `totals`. `lastScanAt`
/// is stamped by the caller (the scan job records the RFC-3339 scan time).
/// Provider-reported costs (§5.23) accumulate per bucket via [`CostBucket`];
/// buckets no contributing session reported a cost for stay cost-less.
pub(crate) fn aggregate_token_usage(tallies: &[AgentTokenTally]) -> TokenUsage {
    let mut usage = TokenUsage::default();
    let mut totals_cost = CostBucket::default();
    let mut agent_costs: BTreeMap<String, CostBucket> = BTreeMap::new();
    let mut model_costs: BTreeMap<String, CostBucket> = BTreeMap::new();
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
        add_totals(
            usage.by_model.entry(model.clone()).or_default(),
            &tally.totals,
        );
        totals_cost.add(tally.totals.cost.as_ref());
        agent_costs
            .entry(tally.agent_id.clone())
            .or_default()
            .add(tally.totals.cost.as_ref());
        model_costs
            .entry(model)
            .or_default()
            .add(tally.totals.cost.as_ref());
    }
    usage.totals.cost = totals_cost.resolve();
    for (agent_id, bucket) in agent_costs {
        if let Some(entry) = usage.by_agent_id.get_mut(&agent_id) {
            entry.cost = bucket.resolve();
        }
    }
    for (model, bucket) in model_costs {
        if let Some(entry) = usage.by_model.get_mut(&model) {
            entry.cost = bucket.resolve();
        }
    }
    usage
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
/// Field mapping: `cachedReadTokens` → `cacheReadTokens`,
/// `cachedWriteTokens` → `cacheCreationTokens` and `thoughtTokens` →
/// `thoughtTokens`; absent optional counters map to zero. `totalTokens` is
/// dropped (it is derivable).
///
/// **Recreate baseline** (monorepo#737): counters are cumulative per *ACP*
/// session, so when the resume-impossible fallback recreates the ACP session,
/// the fresh session restarts from zero. The store banks the outgoing
/// session's snapshot into `token_usage_baseline` atomically with the id swap
/// (`Store::replace_acp_session_id`); the agent's effective total is then
/// `baseline + snapshot` (see [`agent_token_tally`]). The snapshot itself
/// stays replace-only — the baseline lives outside it.
pub(crate) fn snapshot_from_turn_usage(usage: &Usage) -> TokenUsageTotals {
    TokenUsageTotals {
        input_tokens: usage.input_tokens,
        output_tokens: usage.output_tokens,
        cache_read_tokens: usage.cached_read_tokens.unwrap_or(0),
        cache_creation_tokens: usage.cached_write_tokens.unwrap_or(0),
        thought_tokens: usage.thought_tokens.unwrap_or(0),
        // Cost rides on a separate ACP `usage_update` notification, not the
        // end-of-turn report; the caller stamps it onto the snapshot.
        cost: None,
    }
}

/// One agent's tally for the workspace roll-up (§5.23): the effective total
/// is `baseline + snapshot`, component-wise, where either part may be absent.
/// The `baseline` banks the cumulative totals of ACP sessions folded away by
/// a recreate (monorepo#737) and the `snapshot` is the current ACP session's
/// cumulative end-of-turn report (see [`snapshot_from_turn_usage`]), so their
/// sum never double-counts. A baseline WITHOUT a snapshot uses the baseline
/// alone — never baseline + message sums, since the pre-recreate snapshot
/// already superseded the message metadata. Only when neither part carries a
/// token report (both absent, or present with all-zero counters — the shape a
/// cost-only `usage_update` persist leaves behind for a provider that never
/// sends an end-of-turn token report) does the tally fall back to summing
/// per-message usage metadata via [`agent_token_tally_from_contents`]; any
/// cost reported on the absent-counter parts still rides along, so a
/// cost-only report never zeroes a fallback session's counters
/// ([`intent_core::token_usage_reported`] keeps the store-side hydration
/// decision in lockstep).
#[must_use]
pub fn agent_token_tally(
    agent_id: &str,
    model: Option<&str>,
    baseline: Option<&TokenUsageTotals>,
    snapshot: Option<&TokenUsageTotals>,
    contents: &[serde_json::Value],
) -> AgentTokenTally {
    // Cost combines with the same never-double-count rule as the counters
    // (§5.23): the baseline banks folded-away ACP sessions, the snapshot is
    // the current one.
    let cost = UsageCost::merge(
        baseline.and_then(|b| b.cost.as_ref()),
        snapshot.and_then(|s| s.cost.as_ref()),
    );
    if !token_usage_reported(baseline, snapshot) {
        let mut tally = agent_token_tally_from_contents(agent_id, model, contents);
        tally.totals.cost = cost;
        return tally;
    }
    let mut totals = baseline.cloned().unwrap_or_default();
    if let Some(snapshot) = snapshot {
        add_totals(&mut totals, snapshot);
    }
    totals.cost = cost;
    AgentTokenTally {
        agent_id: agent_id.to_string(),
        model: model.unwrap_or("").to_string(),
        totals,
    }
}

/// Lightweight token tally from extracted usage data (finding F2: avoid full
/// `AgentSession` hydration). Takes an `agent_id`, model, and the message content
/// JSON list (usage metadata is embedded in each content block).
pub(crate) fn agent_token_tally_from_contents(
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
        thought_tokens: field("thoughtTokens"),
        // Legacy per-message metadata never carried a cost figure.
        cost: None,
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
            thought_tokens: 0,
            cost: None,
        }
    }

    fn totals_with_thoughts(i: u64, o: u64, r: u64, c: u64, t: u64) -> TokenUsageTotals {
        TokenUsageTotals {
            thought_tokens: t,
            ..totals(i, o, r, c)
        }
    }

    fn cost(amount: f64, currency: &str) -> UsageCost {
        UsageCost {
            amount,
            currency: currency.to_string(),
        }
    }

    fn totals_with_cost(i: u64, o: u64, amount: f64, currency: &str) -> TokenUsageTotals {
        TokenUsageTotals {
            cost: Some(cost(amount, currency)),
            ..totals(i, o, 0, 0)
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

    /// Cost aggregation (§5.23): reporting sessions sum per bucket, sessions
    /// without a cost contribute nothing (never a fabricated 0), and a bucket
    /// no session reported for stays cost-less.
    #[test]
    fn aggregates_cost_only_from_reporting_sessions() {
        let tallies = vec![
            AgentTokenTally {
                agent_id: "agent-1".into(),
                model: "opus-4.8".into(),
                totals: totals_with_cost(100, 20, 1.25, "USD"),
            },
            AgentTokenTally {
                agent_id: "agent-2".into(),
                model: "opus-4.8".into(),
                totals: totals_with_cost(50, 10, 0.75, "USD"),
            },
            AgentTokenTally {
                agent_id: "agent-3".into(),
                model: "sonnet-5".into(),
                totals: totals(5, 5, 0, 0),
            },
        ];
        let usage = aggregate_token_usage(&tallies);
        assert_eq!(usage.totals.cost, Some(cost(2.0, "USD")));
        assert_eq!(usage.by_agent_id["agent-1"].cost, Some(cost(1.25, "USD")));
        assert_eq!(usage.by_model["opus-4.8"].cost, Some(cost(2.0, "USD")));
        assert_eq!(
            usage.by_agent_id["agent-3"].cost, None,
            "a session that reported no cost contributes none"
        );
        assert_eq!(usage.by_model["sonnet-5"].cost, None);
    }

    /// Mixed currencies inside one bucket are pathological; the bucket keeps
    /// the currency with the largest summed amount rather than inventing a
    /// conversion.
    #[test]
    fn mixed_currencies_keep_the_largest_sum() {
        let tallies = vec![
            AgentTokenTally {
                agent_id: "agent-1".into(),
                model: "opus-4.8".into(),
                totals: totals_with_cost(10, 1, 1.0, "USD"),
            },
            AgentTokenTally {
                agent_id: "agent-2".into(),
                model: "opus-4.8".into(),
                totals: totals_with_cost(10, 1, 4.0, "EUR"),
            },
            AgentTokenTally {
                agent_id: "agent-3".into(),
                model: "opus-4.8".into(),
                totals: totals_with_cost(10, 1, 2.0, "EUR"),
            },
        ];
        let usage = aggregate_token_usage(&tallies);
        assert_eq!(usage.totals.cost, Some(cost(6.0, "EUR")));
        assert_eq!(usage.by_model["opus-4.8"].cost, Some(cost(6.0, "EUR")));
    }

    /// The per-agent tally merges the recreate baseline's banked cost with
    /// the current session's snapshot cost, mirroring the counter rule.
    #[test]
    fn agent_token_tally_merges_baseline_and_snapshot_cost() {
        let baseline = totals_with_cost(100, 80, 3.0, "USD");
        let snapshot = totals_with_cost(5, 3, 1.5, "USD");
        let both = agent_token_tally(
            "agent-r",
            Some("opus-4.8"),
            Some(&baseline),
            Some(&snapshot),
            &[],
        );
        assert_eq!(both.totals.cost, Some(cost(4.5, "USD")));
        // Baseline alone keeps its banked cost.
        let baseline_only =
            agent_token_tally("agent-r", Some("opus-4.8"), Some(&baseline), None, &[]);
        assert_eq!(baseline_only.totals.cost, Some(cost(3.0, "USD")));
        // Neither reported cost → none.
        let cost_less = agent_token_tally(
            "agent-r",
            Some("opus-4.8"),
            Some(&totals(1, 1, 0, 0)),
            Some(&totals(1, 1, 0, 0)),
            &[],
        );
        assert_eq!(cost_less.totals.cost, None);
    }

    /// A zero-counter snapshot is what a cost-only `usage_update` persist
    /// leaves behind; it must not suppress the per-message fallback, and its
    /// cost still rides along.
    #[test]
    fn cost_only_snapshot_keeps_the_message_sum_fallback() {
        let snapshot = TokenUsageTotals {
            cost: Some(cost(0.4, "USD")),
            ..TokenUsageTotals::default()
        };
        let contents = vec![serde_json::json!({
            "usage": { "inputTokens": 7, "outputTokens": 3 }
        })];
        let tally = agent_token_tally(
            "agent-f",
            Some("sonnet-5"),
            None,
            Some(&snapshot),
            &contents,
        );
        assert_eq!(tally.totals.input_tokens, 7);
        assert_eq!(tally.totals.output_tokens, 3);
        assert_eq!(tally.totals.cost, Some(cost(0.4, "USD")));
    }

    /// A non-finite amount (a corrupt stored snapshot) is ignored rather than
    /// winning the bucket compare and serializing as `null`.
    #[test]
    fn non_finite_cost_is_ignored_by_the_bucket() {
        let tallies = vec![
            AgentTokenTally {
                agent_id: "agent-1".into(),
                model: "opus-4.8".into(),
                totals: totals_with_cost(10, 1, f64::NAN, "USD"),
            },
            AgentTokenTally {
                agent_id: "agent-2".into(),
                model: "opus-4.8".into(),
                totals: totals_with_cost(10, 1, 2.0, "EUR"),
            },
        ];
        let usage = aggregate_token_usage(&tallies);
        assert_eq!(usage.totals.cost, Some(cost(2.0, "EUR")));
        assert_eq!(usage.by_agent_id["agent-1"].cost, None);
    }

    #[test]
    fn extracts_usage_from_top_level_and_meta() {
        let top = serde_json::json!({ "usage": { "inputTokens": 12, "outputTokens": 3 } });
        assert_eq!(extract_message_usage(&top), Some(totals(12, 3, 0, 0)));
        let meta = serde_json::json!({ "_meta": { "usage": { "cacheReadTokens": 7 } } });
        assert_eq!(extract_message_usage(&meta), Some(totals(0, 0, 7, 0)));
        let thoughts = serde_json::json!({ "usage": { "thoughtTokens": 9 } });
        assert_eq!(
            extract_message_usage(&thoughts),
            Some(totals_with_thoughts(0, 0, 0, 0, 9))
        );
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
        assert_eq!(
            snapshot_from_turn_usage(&usage),
            totals_with_thoughts(70, 50, 30, 4, 8)
        );

        // Absent optional counters map to zero.
        let sparse: Usage = serde_json::from_value(serde_json::json!({
            "totalTokens": 3,
            "inputTokens": 2,
            "outputTokens": 1
        }))
        .expect("sparse usage deserializes");
        assert_eq!(snapshot_from_turn_usage(&sparse), totals(2, 1, 0, 0));
    }

    /// `thoughtTokens` folds through the same baseline+snapshot combine and
    /// per-agent/per-model/workspace roll-up as the other counters (§5.23).
    #[test]
    fn thought_tokens_fold_through_the_tally() {
        let baseline = totals_with_thoughts(100, 80, 10, 2, 40);
        let snapshot = totals_with_thoughts(5, 3, 1, 1, 7);
        let tally = agent_token_tally(
            "agent-t",
            Some("opus-4.8"),
            Some(&baseline),
            Some(&snapshot),
            &[],
        );
        assert_eq!(tally.totals, totals_with_thoughts(105, 83, 11, 3, 47));
        let usage = aggregate_token_usage(&[tally]);
        assert_eq!(usage.totals.thought_tokens, 47);
        assert_eq!(usage.by_agent_id["agent-t"].thought_tokens, 47);
        assert_eq!(usage.by_model["opus-4.8"].thought_tokens, 47);
    }

    /// A snapshot that only reports reasoning tokens is still a token report:
    /// it must not fall back to the per-message usage sum.
    #[test]
    fn thought_only_snapshot_is_a_report() {
        let snapshot = totals_with_thoughts(0, 0, 0, 0, 12);
        let contents = vec![serde_json::json!({ "usage": { "inputTokens": 999 } })];
        let tally = agent_token_tally(
            "agent-t",
            Some("opus-4.8"),
            None,
            Some(&snapshot),
            &contents,
        );
        assert_eq!(tally.totals, snapshot);
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
