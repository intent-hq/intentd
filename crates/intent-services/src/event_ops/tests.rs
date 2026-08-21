//! Unit tests for the pure `event.*` aggregation helpers.

use super::*;
use intent_core::{EventActor, WorkspaceId};
use serde_json::json;

fn ev(
    event_type: &str,
    actor_id: Option<&str>,
    actor_type: ActorType,
    ts: &str,
    data: Value,
) -> Event {
    Event {
        id: format!("id-{ts}"),
        workspace_id: WorkspaceId::from("ws-1"),
        timestamp: ts.to_string(),
        event_type: event_type.to_string(),
        actor: EventActor {
            actor_type,
            id: actor_id.map(std::string::ToString::to_string),
            name: actor_id.map(|s| format!("name-{s}")),
            ..Default::default()
        },
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data,
    }
}

#[test]
fn combined_actor_uses_type_and_name_with_undefined_fallback() {
    let e = ev(FILE_CHANGED, Some("a"), ActorType::Tool, "t1", json!({}));
    assert_eq!(combined_actor(&e), "tool:name-a");
    let mut anon = e.clone();
    anon.actor.name = None;
    anon.actor.actor_type = ActorType::System;
    assert_eq!(combined_actor(&anon), "system:undefined");
}

#[test]
fn file_activity_omits_absent_additions_and_keeps_present() {
    let with = ev(
        FILE_CHANGED,
        Some("a"),
        ActorType::Agent,
        "t1",
        json!({ "path": "/abs/a.rs", "relativePath": "a.rs", "action": "modify", "additions": 3 }),
    );
    let fa = file_activity_combined(&with);
    assert_eq!(fa.path, "/abs/a.rs");
    assert_eq!(fa.relative_path, "a.rs");
    assert_eq!(fa.action, "modify");
    assert_eq!(fa.actor.as_deref(), Some("agent:name-a"));
    assert_eq!(fa.additions, Some(json!(3)));
    assert_eq!(fa.deletions, None);

    // The per-agent projection uses the bare actor name.
    let named = file_activity_named(&with);
    assert_eq!(named.actor.as_deref(), Some("name-a"));
}

#[test]
fn aggregate_groups_by_agent_counts_tools_and_files() {
    // Newest-first order (as the store returns).
    let events = vec![
        ev(
            AGENT_TOOL_CALL,
            Some("a1"),
            ActorType::Agent,
            "t3",
            json!({ "filesModified": ["x.rs", "y.rs"] }),
        ),
        ev(
            FILE_CHANGED,
            Some("a1"),
            ActorType::Agent,
            "t2",
            json!({ "path": "x.rs" }),
        ),
        ev(
            "agent:message",
            Some("a2"),
            ActorType::Agent,
            "t1",
            json!({}),
        ),
    ];
    let out = aggregate_agent_activity(&events);
    assert_eq!(out.len(), 2);
    // First-seen order is preserved: a1 then a2.
    assert_eq!(out[0].agent_id, "a1");
    assert_eq!(out[0].agent_name.as_deref(), Some("name-a1"));
    assert_eq!(out[0].event_count, 2);
    assert_eq!(out[0].tool_calls, 1);
    // x.rs deduped across the tool-call + file:changed events; order preserved.
    assert_eq!(out[0].files_modified, vec!["x.rs", "y.rs"]);
    // lastActive ends at the last-iterated event for the agent (the older one).
    assert_eq!(out[0].last_active, "t2");
    assert_eq!(out[1].agent_id, "a2");
    assert_eq!(out[1].event_count, 1);
    assert_eq!(out[1].tool_calls, 0);
}

#[test]
fn aggregate_skips_events_without_actor_id() {
    let events = vec![ev(
        FILE_CHANGED,
        None,
        ActorType::System,
        "t1",
        json!({ "path": "z" }),
    )];
    assert!(aggregate_agent_activity(&events).is_empty());
}

#[test]
fn workspace_summary_rate_recent_and_top_changed() {
    let events = vec![
        ev(
            FILE_CHANGED,
            Some("a1"),
            ActorType::Agent,
            "t4",
            json!({ "path": "a.rs", "relativePath": "a.rs", "action": "modify" }),
        ),
        ev(
            FILE_CHANGED,
            Some("a1"),
            ActorType::Agent,
            "t3",
            json!({ "path": "a.rs", "relativePath": "a.rs", "action": "modify" }),
        ),
        ev(
            FILE_CHANGED,
            Some("u"),
            ActorType::User,
            "t2",
            json!({ "path": "b.rs", "relativePath": "b.rs", "action": "create" }),
        ),
        ev(
            "agent:message",
            Some("a1"),
            ActorType::Agent,
            "t1",
            json!({}),
        ),
    ];
    let summary = build_workspace_summary(&events, 2);
    // 4 events / 2 minutes = 2.0.
    assert_eq!(summary.event_rate, 2.0);
    // Three file:changed events → first five projected (here all three).
    assert_eq!(summary.recent_files.len(), 3);
    assert_eq!(
        summary.recent_files[0].actor.as_deref(),
        Some("agent:name-a1")
    );
    // Only agent-typed events contribute to activeAgents (a1, not the user).
    assert_eq!(summary.active_agents.len(), 1);
    assert_eq!(summary.active_agents[0].agent_id, "a1");
    // a.rs changed twice → top; b.rs once.
    assert_eq!(summary.top_changed_files.len(), 2);
    assert_eq!(summary.top_changed_files[0].path, "a.rs");
    assert_eq!(summary.top_changed_files[0].change_count, 2);
    assert_eq!(summary.top_changed_files[1].path, "b.rs");
    assert_eq!(summary.top_changed_files[1].change_count, 1);
}
