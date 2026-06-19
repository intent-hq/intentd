//! `services::metrics` — the BE-internal change-metrics aggregator (§17.5,
//! §9.11). Ports the reference `line-changes` module: as agents edit files the
//! daemon recomputes the durable per-workspace / per-agent line-change totals
//! from the `tracked_changes` audit trail and persists them to
//! `workspace_metrics` / `agent_metrics`. There is no `metrics.calculate` RPC —
//! clients only **read** the aggregates (PROTOCOL §5.20) and subscribe to
//! `changes:metrics-changed`.
//!
//! Per §3.2 this depends only on `intent-store`; it never imports a sibling
//! service module. The wire-shape builders here produce the §5.20 `Metrics`
//! object (`{ additions, deletions, filesChanged, byAgent }`).

use std::collections::{HashMap, HashSet};

use intent_core::{Result, WorkspaceId};
use intent_store::{AgentMetricsRow, Store, WorkspaceMetricsRow};
use serde_json::{json, Map, Value};

/// Per-agent accumulator over the workspace's tracked-change rows.
#[derive(Default)]
struct Acc {
    additions: i64,
    deletions: i64,
    paths: HashSet<String>,
}

/// Recompute and persist the durable line-change aggregates for one workspace by
/// summing its `tracked_changes` rows (additions/deletions across every stage,
/// `filesChanged` = distinct paths), grouped by attributed agent. The per-agent
/// breakdown is rewritten from scratch so stale agents drop out. When a workspace
/// has no tracked changes the rows are deleted so reads return `null`.
pub async fn recompute(store: &Store, workspace_id: &WorkspaceId) -> Result<()> {
    let rows = store.list_tracked_changes(workspace_id).await?;
    if rows.is_empty() {
        store.delete_workspace_metrics(workspace_id).await?;
        store
            .delete_agent_metrics_for_workspace(workspace_id)
            .await?;
        return Ok(());
    }

    let mut ws_add = 0i64;
    let mut ws_del = 0i64;
    let mut ws_paths: HashSet<&str> = HashSet::new();
    let mut by_agent: HashMap<String, Acc> = HashMap::new();
    for row in &rows {
        ws_add += row.additions;
        ws_del += row.deletions;
        ws_paths.insert(row.path.as_str());
        if let Some(agent) = row.agent_id.as_ref().filter(|a| !a.is_empty()) {
            let acc = by_agent.entry(agent.clone()).or_default();
            acc.additions += row.additions;
            acc.deletions += row.deletions;
            acc.paths.insert(row.path.clone());
        }
    }

    store
        .upsert_workspace_metrics(workspace_id, ws_add, ws_del, ws_paths.len() as i64)
        .await?;
    store
        .delete_agent_metrics_for_workspace(workspace_id)
        .await?;
    for (agent_id, acc) in &by_agent {
        store
            .upsert_agent_metrics(
                workspace_id,
                agent_id,
                acc.additions,
                acc.deletions,
                acc.paths.len() as i64,
            )
            .await?;
    }
    Ok(())
}

/// Build the per-agent `Metrics` fragment (`{ additions, deletions, filesChanged }`).
fn agent_stats_value(additions: i64, deletions: i64, files_changed: i64) -> Value {
    json!({
        "additions": additions,
        "deletions": deletions,
        "filesChanged": files_changed,
    })
}

/// Build the workspace-level `Metrics` value (§5.20):
/// `{ additions, deletions, filesChanged, byAgent: { [agentId]: Metrics } }`.
pub fn workspace_metrics_value(ws: &WorkspaceMetricsRow, agents: &[AgentMetricsRow]) -> Value {
    let mut by_agent = Map::new();
    for a in agents {
        by_agent.insert(
            a.agent_id.clone(),
            agent_stats_value(a.additions, a.deletions, a.files_changed),
        );
    }
    json!({
        "additions": ws.additions,
        "deletions": ws.deletions,
        "filesChanged": ws.files_changed,
        "byAgent": Value::Object(by_agent),
    })
}

/// Sum one agent's rows across workspaces into a per-agent `Metrics` value (no
/// `byAgent`, per §5.20). Returns `null` when the agent has no recorded metrics.
pub fn agent_metrics_value(rows: &[AgentMetricsRow]) -> Value {
    if rows.is_empty() {
        return Value::Null;
    }
    let mut additions = 0i64;
    let mut deletions = 0i64;
    let mut files_changed = 0i64;
    for r in rows {
        additions += r.additions;
        deletions += r.deletions;
        files_changed += r.files_changed;
    }
    agent_stats_value(additions, deletions, files_changed)
}
