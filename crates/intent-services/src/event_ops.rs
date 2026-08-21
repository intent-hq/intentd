//! Pure mapping/aggregation helpers for the `event.*` query methods (§5.10).
//!
//! Ported wire-for-wire from `~/src/intent/src/features/events/main/
//! agent-event-tools.ts`: the `FileActivity` projection (`getAgentFiles`), the
//! `getAgentActivity` grouping, and the `getWorkspaceSummary` aggregation. The
//! store-backed queries that feed these live in `intent-store`; this module is
//! pure (no IO) so the aggregation math is unit-testable in isolation.

use std::collections::{HashMap, HashSet};

use intent_core::events::{AGENT_TOOL_CALL, FILE_CHANGED};
use intent_core::{
    ActorType, AgentActivity, Event, FileActivity, TopChangedFile, WorkspaceEventSummary,
};
use serde_json::Value;

/// The lowercase wire string for an [`ActorType`] (matches the serde form).
fn actor_type_str(actor_type: ActorType) -> &'static str {
    match actor_type {
        ActorType::User => "user",
        ActorType::Agent => "agent",
        ActorType::System => "system",
        ActorType::External => "external",
        ActorType::Tool => "tool",
    }
}

/// `"type:name"` actor label, reproducing the TS template literal
/// `` `${e.actor.type}:${e.actor.name}` `` (an absent name renders `undefined`).
fn combined_actor(ev: &Event) -> String {
    let name = ev.actor.name.as_deref().unwrap_or("undefined");
    format!("{}:{}", actor_type_str(ev.actor.actor_type), name)
}

/// Read a `data.<key>` string, defaulting to empty (the TS path/relativePath/
/// action fields are always present on `file:changed` events).
fn data_str(ev: &Event, key: &str) -> String {
    ev.data
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

/// Clone a `data.<key>` value, omitting it entirely when the key is absent
/// (mirrors `additions: e.data.additions` being dropped by `JSON.stringify`).
fn data_opt(ev: &Event, key: &str) -> Option<Value> {
    ev.data.get(key).cloned()
}

/// Project a `file:changed` event into a [`FileActivity`] with the combined
/// `"type:name"` actor (workspace summary `recentFiles`).
pub(crate) fn file_activity_combined(ev: &Event) -> FileActivity {
    FileActivity {
        path: data_str(ev, "path"),
        relative_path: data_str(ev, "relativePath"),
        action: data_str(ev, "action"),
        timestamp: ev.timestamp.clone(),
        actor: Some(combined_actor(ev)),
        additions: data_opt(ev, "additions"),
        deletions: data_opt(ev, "deletions"),
    }
}

/// Project a `file:changed` event with the bare actor name (`getAgentFiles`).
pub(crate) fn file_activity_named(ev: &Event) -> FileActivity {
    FileActivity {
        path: data_str(ev, "path"),
        relative_path: data_str(ev, "relativePath"),
        action: data_str(ev, "action"),
        timestamp: ev.timestamp.clone(),
        actor: ev.actor.name.clone(),
        additions: data_opt(ev, "additions"),
        deletions: data_opt(ev, "deletions"),
    }
}

/// Group events by `actor.id` into [`AgentActivity`] rows, preserving first-seen
/// order (`getAgentActivity`). Events lacking an `actor.id` are skipped;
/// `lastActive` ends at the last-iterated event's timestamp and `filesModified`
/// is de-duplicated in insertion order — both faithful to the TS.
pub(crate) fn aggregate_agent_activity(events: &[Event]) -> Vec<AgentActivity> {
    let mut order: Vec<String> = Vec::new();
    let mut map: HashMap<String, AgentActivity> = HashMap::new();
    for ev in events {
        let agent_id = match &ev.actor.id {
            Some(id) => id.clone(),
            None => continue,
        };
        let entry = map.entry(agent_id.clone()).or_insert_with(|| {
            order.push(agent_id.clone());
            AgentActivity {
                agent_id: agent_id.clone(),
                agent_name: ev.actor.name.clone(),
                event_count: 0,
                tool_calls: 0,
                files_modified: Vec::new(),
                last_active: ev.timestamp.clone(),
            }
        });
        entry.event_count += 1;
        entry.last_active.clone_from(&ev.timestamp);
        if ev.event_type == AGENT_TOOL_CALL {
            entry.tool_calls += 1;
            if let Some(files) = ev.data.get("filesModified").and_then(Value::as_array) {
                for f in files.iter().filter_map(Value::as_str) {
                    entry.files_modified.push(f.to_string());
                }
            }
        }
        if ev.event_type == FILE_CHANGED {
            entry.files_modified.push(data_str(ev, "path"));
        }
    }
    for activity in map.values_mut() {
        let mut seen = HashSet::new();
        activity.files_modified.retain(|f| seen.insert(f.clone()));
    }
    order
        .into_iter()
        .map(|id| map.remove(&id).expect("agent id present"))
        .collect()
}

/// Build the `event.workspaceSummary` aggregate from all events in the window
/// (`getWorkspaceSummary`): the first five `file:changed` rows, per-agent
/// activity (agent-typed subset), the events-per-minute rate, and the top five
/// most-changed files (count desc, ties by first appearance).
pub(crate) fn build_workspace_summary(
    all_events: &[Event],
    minutes_ago: i64,
) -> WorkspaceEventSummary {
    let file_events: Vec<&Event> = all_events
        .iter()
        .filter(|e| e.event_type == FILE_CHANGED)
        .collect();
    let recent_files = file_events
        .iter()
        .take(5)
        .map(|e| file_activity_combined(e))
        .collect();
    let agent_events: Vec<Event> = all_events
        .iter()
        .filter(|e| e.actor.actor_type == ActorType::Agent)
        .cloned()
        .collect();
    let active_agents = aggregate_agent_activity(&agent_events);
    let event_rate = all_events.len() as f64 / minutes_ago as f64;

    let mut order: Vec<String> = Vec::new();
    let mut counts: HashMap<String, i64> = HashMap::new();
    for e in &file_events {
        let path = data_str(e, "path");
        if !counts.contains_key(&path) {
            order.push(path.clone());
        }
        *counts.entry(path).or_insert(0) += 1;
    }
    let mut top_changed_files: Vec<TopChangedFile> = order
        .into_iter()
        .map(|p| {
            let change_count = counts[&p];
            TopChangedFile {
                path: p,
                change_count,
            }
        })
        .collect();
    top_changed_files.sort_by_key(|b| std::cmp::Reverse(b.change_count));
    top_changed_files.truncate(5);

    WorkspaceEventSummary {
        recent_files,
        active_agents,
        event_rate,
        top_changed_files,
    }
}

#[cfg(test)]
mod tests;
