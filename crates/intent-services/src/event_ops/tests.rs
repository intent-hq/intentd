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
#[allow(clippy::float_cmp)] // asserting exact literals round-tripped through config parsing
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

// --- bound_event_rows (monorepo#3347) --------------------------------------

/// A wire-shape event row with a payload of roughly `data_bytes` bytes.
fn big_row(i: usize, data_bytes: usize) -> Value {
    serde_json::to_value(ev(
        "note:updated",
        Some("a1"),
        ActorType::Agent,
        &format!("2026-01-01T00:00:{i:02}Z"),
        json!({ "blob": "x".repeat(data_bytes) }),
    ))
    .expect("serialize event")
}

fn total_size(rows: &[Value]) -> usize {
    rows.iter().map(intent_core::slim_body_size).sum()
}

/// Under-budget responses are returned byte-identical: no markers, no trims.
#[test]
fn bound_event_rows_under_budget_untouched() {
    let mut rows: Vec<Value> = (0..50).map(|i| big_row(i, 1024)).collect();
    let before = rows.clone();
    assert_eq!(bound_event_rows(&mut rows), 0);
    assert_eq!(rows, before);
}

/// Regression (monorepo#3347): a busy-workspace row set that serializes past
/// the budget is bounded below it (modulo the small per-row identity floor),
/// every row survives, and trimmed rows carry observable markers.
#[test]
fn bound_event_rows_over_budget_trims_and_marks() {
    // 60 rows × ~30 KiB ≈ 1.8 MiB — well past the 700 KiB budget.
    let mut rows: Vec<Value> = (0..60).map(|i| big_row(i, 30 * 1024)).collect();
    assert!(total_size(&rows) > EVENT_QUERY_RESPONSE_BUDGET_BYTES);
    let trimmed = bound_event_rows(&mut rows);
    assert!(trimmed > 0, "over-budget set must trim rows");
    assert_eq!(rows.len(), 60, "no rows may be dropped");
    // The bound: budget + per-row identity/marker slack (identity fields are
    // small scalars; give each row a generous 1 KiB of slack).
    assert!(
        total_size(&rows) <= EVENT_QUERY_RESPONSE_BUDGET_BYTES + 60 * 1024,
        "post-trim size {} must be near the budget",
        total_size(&rows)
    );
    for row in &rows {
        // Identity fields always survive intact.
        assert!(row["id"].as_str().is_some(), "id survives: {row}");
        assert_eq!(row["type"], json!("note:updated"));
        assert!(row["timestamp"].as_str().is_some());
        assert_eq!(row["actor"]["type"], json!("agent"));
        if row["truncated"] == json!(true) {
            // Marker rows expose the original size and keep `data`'s shape.
            assert!(row["originalBytes"].as_u64().is_some(), "marker: {row}");
            assert!(row["data"].is_object(), "data stays an object: {row}");
        }
    }
    let marked = rows
        .iter()
        .filter(|r| r["truncated"] == json!(true))
        .count();
    assert_eq!(marked, trimmed, "return value counts marked rows");
}

/// Wire order is newest→oldest, and the fair-share walk spends the budget on
/// the earlier (newer) rows: the first row keeps more payload than the last.
#[test]
fn bound_event_rows_prefers_newer_rows() {
    let mut rows: Vec<Value> = (0..60).map(|i| big_row(i, 30 * 1024)).collect();
    bound_event_rows(&mut rows);
    let first = intent_core::slim_body_size(&rows[0]);
    let last = intent_core::slim_body_size(&rows[59]);
    assert!(
        first >= last,
        "newest row ({first}B) must keep at least as much as the oldest ({last}B)"
    );
    assert!(
        rows[0]["data"]["blob"].as_str().is_some(),
        "newest row keeps a bounded data preview: {}",
        rows[0]
    );
}

/// One pathological row does not starve its siblings: small rows before and
/// after a giant row survive untouched.
#[test]
fn bound_event_rows_giant_row_does_not_starve_siblings() {
    let mut rows = vec![
        big_row(0, 512),
        big_row(1, 2 * 1024 * 1024), // 2 MiB outlier
        big_row(2, 512),
    ];
    let trimmed = bound_event_rows(&mut rows);
    assert_eq!(trimmed, 1, "only the outlier is trimmed");
    assert_eq!(rows[0]["truncated"], Value::Null);
    assert_eq!(rows[1]["truncated"], json!(true));
    assert_eq!(rows[2]["truncated"], Value::Null);
    assert_eq!(rows[0]["data"]["blob"].as_str().map(str::len), Some(512));
    assert_eq!(rows[2]["data"]["blob"].as_str().map(str::len), Some(512));
    assert!(total_size(&rows) <= EVENT_QUERY_RESPONSE_BUDGET_BYTES + 3 * 1024);
}

/// Non-object rows (defensive: rows are always serialized `Event` objects)
/// are charged against the budget but never panic.
#[test]
fn bound_event_rows_non_object_rows_are_safe() {
    let mut rows = vec![
        Value::String("y".repeat(EVENT_QUERY_RESPONSE_BUDGET_BYTES + 1024)),
        big_row(1, 512),
    ];
    let trimmed = bound_event_rows(&mut rows);
    assert!(rows[0].is_string(), "non-object row is passed through");
    // The oversized string exhausted the budget, so the object row after it
    // is trimmed — and it is the only row that can carry a marker.
    assert_eq!(trimmed, 1);
    assert_eq!(rows[1]["truncated"], json!(true));
    assert_eq!(rows.len(), 2);
}
