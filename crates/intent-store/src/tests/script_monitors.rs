//! Monitor ledger isolation, lifecycle fencing and transactional outbox tests.
use super::*;
use intent_core::{ScriptMode, ScriptMonitor, ScriptMonitorTrigger};

fn monitor(ws: &WorkspaceId, agent: &AgentId, id: &str) -> ScriptMonitor {
    ScriptMonitor {
        monitor_id: id.into(),
        workspace_id: ws.clone(),
        agent_id: agent.clone(),
        script_id: "script-key".into(),
        run_id: "run-key".into(),
        script_name: "Check".into(),
        mode: ScriptMode::Command,
        state: "active".into(),
        created_at: "2026-01-01T00:00:00.000Z".into(),
        expires_at: "2026-01-02T00:00:00.000Z".into(),
        output_pattern: None,
        line_count: Some(1),
        settled_at: None,
        reason: None,
        result: None,
        trigger: None,
    }
}
async fn seed(store: &Store, name: &str) -> (WorkspaceId, AgentId) {
    let ws = WorkspaceId::from(name);
    let agent = AgentId::from(format!("agent-{name}"));
    store
        .insert_workspace(&sample_workspace(&ws, name, false))
        .await
        .unwrap();
    store
        .insert_agent_session(&sample_agent_session(&agent, &ws))
        .await
        .unwrap();
    (ws, agent)
}
fn triggered(row: &ScriptMonitor) -> ScriptMonitor {
    let mut row = row.clone();
    row.state = "triggered".into();
    row.reason = Some("line-count".into());
    row.settled_at = Some("2026-01-01T00:01:00.000Z".into());
    row.trigger = Some(ScriptMonitorTrigger {
        observed_line_count: 1,
        matched_line: None,
    });
    row
}
fn metadata(row: &ScriptMonitor) -> serde_json::Value {
    json!({"type":"script_monitor_wake","monitorId":row.monitor_id,"workspaceId":row.workspace_id})
}

#[tokio::test]
async fn monitor_ledger_namespace_is_composite_and_terminal_cas_is_once() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let (a, aa) = seed(&store, "a").await;
    let (b, bb) = seed(&store, "b").await;
    // Definition IDs remain globally unique. Ledger keys are independently
    // scoped, including retained rows whose original definition no longer exists.
    let one = monitor(&a, &aa, "one");
    let two = monitor(&b, &bb, "two");
    store.insert_script_monitor(&one).await.unwrap();
    store.insert_script_monitor(&two).await.unwrap();
    assert!(store
        .insert_script_monitor(&monitor(&a, &aa, "collision"))
        .await
        .is_err());
    assert_eq!(store.script_monitors(&a, None).await.unwrap().len(), 1);
    assert!(store.script_monitor(&b, "one").await.is_err());
    assert!(store
        .script_monitors(&a, Some(&bb))
        .await
        .unwrap()
        .is_empty());
    let terminal = triggered(&one);
    assert!(store.settle_script_monitor(&terminal).await.unwrap());
    assert!(!store.settle_script_monitor(&terminal).await.unwrap());
    assert!(store.script_monitor_wake_pending("one").await.unwrap());
    assert!(!store.script_monitor_wake_pending("two").await.unwrap());
    store.close().await;
    let reopened = Store::open(&tmp.path).await.unwrap();
    assert_eq!(
        reopened.script_monitor(&a, "one").await.unwrap().state,
        "triggered"
    );
    assert!(reopened.script_monitor_wake_pending("one").await.unwrap());
}

#[tokio::test]
async fn monitor_outbox_message_commit_and_retirement_are_durable_fences() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let (ws, agent) = seed(&store, "outbox").await;
    let first = monitor(&ws, &agent, "first");
    store.insert_script_monitor(&first).await.unwrap();
    store
        .settle_script_monitor(&triggered(&first))
        .await
        .unwrap();
    let md = metadata(&first);
    let blocks = json!([{"type":"text","text":"wake"}]);
    store
        .append_agent_message_with_provenance(
            &agent,
            "script-monitor:first",
            "user",
            &blocks,
            Some(&md),
            &now_iso(),
            crate::UsageMessageOrigin::Excluded,
        )
        .await
        .unwrap();
    assert!(!store.script_monitor_wake_pending("first").await.unwrap());
    let second = monitor(&ws, &agent, "second");
    store.insert_script_monitor(&second).await.unwrap();
    store
        .settle_script_monitor(&triggered(&second))
        .await
        .unwrap();
    store
        .set_agent_session_retired_at(&ws, &agent, Some(&now_iso()), &now_iso())
        .await
        .unwrap();
    assert!(!store.script_monitor_wake_allowed("second").await.unwrap());
    store
        .set_agent_session_retired_at(&ws, &agent, None, &now_iso())
        .await
        .unwrap();
    assert!(store
        .append_agent_message_with_provenance(
            &agent,
            "script-monitor:second",
            "user",
            &blocks,
            Some(&metadata(&second)),
            &now_iso(),
            crate::UsageMessageOrigin::Excluded
        )
        .await
        .is_err());
    // A stale queue snapshot arriving after cleanup/restore cannot resurrect
    // the wake; unrelated user messages in that snapshot must survive.
    let payload = json!({"messageMetadata":metadata(&second)}).to_string();
    sqlx::query("INSERT INTO agent_queue(id,agent_id,position,payload,created_at) VALUES('stale-wake',?,0,?,?)")
        .bind(agent.as_str()).bind(payload).bind(now_iso()).execute(store.write_pool()).await.unwrap();
    sqlx::query("INSERT INTO agent_queue(id,agent_id,position,payload,created_at) VALUES('user-message',?,1,'{}',?)")
        .bind(agent.as_str()).bind(now_iso()).execute(store.write_pool()).await.unwrap();
    let ids: Vec<String> = sqlx::query_scalar("SELECT id FROM agent_queue WHERE agent_id=?")
        .bind(agent.as_str())
        .fetch_all(store.read_pool())
        .await
        .unwrap();
    assert_eq!(ids, vec!["user-message"]);
    assert!(store
        .get_agent_message_by_id_with_pruned(&agent, "script-monitor:first")
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn monitor_cleanup_cancels_active_and_pruning_preserves_pending_wakes() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let (ws, agent) = seed(&store, "prune").await;
    let pending = monitor(&ws, &agent, "pending");
    store.insert_script_monitor(&pending).await.unwrap();
    store
        .settle_script_monitor(&triggered(&pending))
        .await
        .unwrap();
    let active = monitor(&ws, &agent, "active");
    store.insert_script_monitor(&active).await.unwrap();
    store
        .prune_script_monitors(&ws, "2026-02-01T00:00:00.000Z", false)
        .await
        .unwrap();
    assert_eq!(store.script_monitors(&ws, None).await.unwrap().len(), 2);
    store
        .set_agent_session_retired_at(&ws, &agent, Some(&now_iso()), &now_iso())
        .await
        .unwrap();
    assert_eq!(
        store
            .script_monitor(&ws, "active")
            .await
            .unwrap()
            .reason
            .as_deref(),
        Some("owner-retired")
    );
    assert!(store
        .insert_script_monitor(&monitor(&ws, &agent, "refused"))
        .await
        .is_err());
    store
        .prune_script_monitors(&ws, "2027-02-01T00:00:00.000Z", false)
        .await
        .unwrap();
    assert!(store.script_monitors(&ws, None).await.unwrap().is_empty());
}
