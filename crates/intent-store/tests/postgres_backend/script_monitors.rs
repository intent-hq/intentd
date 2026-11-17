//! Script monitors, notification jobs and their queue admission on both
//! backends, plus the monitor branch of the script summary page.
//!
//! Decoy rule: each body ends by giving the decoy rows with the ids the
//! subject reads, probes as absent or writes, in their pre-write state
//! (active monitors, pending jobs, a pending admission and its queue copy).
//! Script names and payloads carry the tenant, so a leaked row never looks
//! right.

use intent_core::script_lists::ScriptListOptions;
use intent_core::{
    AgentId, AgentSession, Error, ScriptMode, ScriptMonitor, ScriptMonitorTrigger, Workspace,
    WorkspaceId,
};
use intent_store::{AgentQueueRow, Store};
use serde_json::{json, Value};

use crate::harness::parity_test;

const T0: &str = "2026-01-01T00:00:00.000Z";
const T1: &str = "2026-01-02T00:00:00.000Z";
const SETTLED: &str = "2026-01-03T00:00:00.000Z";

fn wid(id: &str) -> WorkspaceId {
    WorkspaceId(id.into())
}

fn aid(id: &str) -> AgentId {
    AgentId(id.into())
}

/// Agents `agent-a` and `agent-b` in `ws-m`, `agent-c` in `ws-n`.
async fn seed(store: &Store, tenant: &str) {
    for ws in ["ws-m", "ws-n"] {
        store
            .insert_workspace(&Workspace {
                id: wid(ws),
                title: format!("{tenant} {ws}"),
                branch: "main".into(),
                created_at: T0.into(),
                updated_at: T0.into(),
                ..Default::default()
            })
            .await
            .unwrap();
    }
    for (agent, ws) in [
        ("agent-a", "ws-m"),
        ("agent-b", "ws-m"),
        ("agent-c", "ws-n"),
    ] {
        store
            .insert_agent_session(&AgentSession {
                id: aid(agent),
                workspace_id: wid(ws),
                name: format!("{agent} of {tenant}"),
                created_at: T0.into(),
                updated_at: T0.into(),
                ..AgentSession::default()
            })
            .await
            .unwrap();
    }
}

/// `m1`: agent-a/ws-m/s1 at T1; `m2`: agent-b/ws-m/s2 at T0; `m3`: agent-c/ws-n/s1 at T0.
fn monitor(id: &str, tenant: &str) -> ScriptMonitor {
    let (agent, ws, script, created_at) = match id {
        "m1" => ("agent-a", "ws-m", "s1", T1),
        "m2" => ("agent-b", "ws-m", "s2", T0),
        "m3" => ("agent-c", "ws-n", "s1", T0),
        _ => ("agent-b", "ws-m", "s-gone", T0),
    };
    ScriptMonitor {
        monitor_id: id.into(),
        workspace_id: wid(ws),
        agent_id: aid(agent),
        script_id: script.into(),
        run_id: format!("run-{id}"),
        script_name: format!("{tenant} {id}"),
        mode: ScriptMode::Command,
        state: "active".into(),
        created_at: created_at.into(),
        expires_at: format!("2026-02-01T00:00:00.000Z/{tenant}"),
        output_pattern: Some(format!("{tenant}-pattern")),
        line_count: Some(1),
        settled_at: None,
        reason: None,
        result: None,
        trigger: None,
    }
}

fn settled(row: &ScriptMonitor, state: &str) -> ScriptMonitor {
    ScriptMonitor {
        state: state.into(),
        reason: Some("line-count".into()),
        settled_at: Some(SETTLED.into()),
        trigger: Some(ScriptMonitorTrigger {
            observed_line_count: 1,
            matched_line: None,
        }),
        ..row.clone()
    }
}

fn queue(row: &ScriptMonitor, tenant: &str) -> AgentQueueRow {
    let id = format!("script-monitor:{}", row.monitor_id);
    AgentQueueRow {
        id: id.clone(),
        agent_id: row.agent_id.clone(),
        position: 0,
        payload: json!({
            "id": id,
            "turnId": id,
            "tenant": tenant,
            "messageMetadata": {
                "type": "script_monitor_wake",
                "monitorId": row.monitor_id,
                "workspaceId": row.workspace_id,
            },
        }),
        created_at: T1.into(),
        turn_id: id,
    }
}

async fn insert(store: &Store, ids: &[&str], tenant: &str) {
    for id in ids {
        store
            .insert_script_monitor(&monitor(id, tenant))
            .await
            .unwrap();
    }
}

async fn settle(store: &Store, ids: &[&str], tenant: &str) {
    for id in ids {
        assert!(store
            .settle_script_monitor(&settled(&monitor(id, tenant), "triggered"))
            .await
            .unwrap());
    }
}

/// Leave the decoy with active `m1`–`m3` and `m-gone`, pending jobs for
/// `m1`–`m3`, and pending admissions (with queue copies) for `m1` and `m2`.
async fn decoy_rows(store: &Store, tenant: &str) {
    if tenant != "decoy" {
        return;
    }
    store
        .suppress_script_notifications(&wid("ws-m"), None)
        .await
        .unwrap();
    store
        .suppress_script_notifications(&wid("ws-n"), None)
        .await
        .unwrap();
    for id in ["m1", "m2", "m3", "m-gone"] {
        let row = monitor(id, tenant);
        let _ = store
            .settle_script_monitor(&settled(&row, "cancelled"))
            .await;
    }
    insert(store, &["m1", "m2"], tenant).await;
    settle(store, &["m1", "m2"], tenant).await;
    for id in ["m1", "m2"] {
        assert!(store
            .admit_script_notification(&queue(&monitor(id, tenant), tenant))
            .await
            .unwrap());
    }
    insert(store, &["m1", "m2", "m3"], tenant).await;
    settle(store, &["m1", "m2", "m3"], tenant).await;
    insert(store, &["m1", "m2", "m3", "m-gone"], tenant).await;
}

fn ids(rows: &[ScriptMonitor]) -> Vec<&str> {
    rows.iter().map(|m| m.monitor_id.as_str()).collect()
}

fn invalid<T: std::fmt::Debug>(r: &intent_core::Result<T>) -> bool {
    matches!(r, Err(Error::InvalidParams(_)))
}

parity_test!(
    script_monitors_insert_read_and_cancel_intent,
    |store, tenant| {
        let (wm, wn) = (wid("ws-m"), wid("ws-n"));
        seed(&store, tenant).await;
        assert!(store.script_monitors(&wm, None).await.unwrap().is_empty());
        assert!(store
            .script_monitor_waiting(None, None)
            .await
            .unwrap()
            .is_empty());
        assert!(store.pending_script_monitors().await.unwrap().is_empty());
        assert_eq!(store.script_monitor_reservations(&wm).await.unwrap(), 0);
        assert!(invalid(&store.script_monitor(&wm, "m1").await));

        insert(&store, &["m1", "m2", "m3"], tenant).await;
        assert!(
            store
                .insert_script_monitor(&ScriptMonitor {
                    monitor_id: "m4".into(),
                    ..monitor("m1", tenant)
                })
                .await
                .is_err(),
            "one active monitor per script"
        );
        assert!(
            store
                .insert_script_monitor(&ScriptMonitor {
                    agent_id: aid("agent-c"),
                    ..monitor("m-gone", tenant)
                })
                .await
                .is_err(),
            "lifecycle fence: owner outside the workspace"
        );
        assert!(invalid(
            &store
                .insert_script_monitor(&settled(&monitor("m-gone", tenant), "triggered"))
                .await
        ));

        assert_eq!(
            ids(&store.script_monitors(&wm, None).await.unwrap()),
            ["m2", "m1"]
        );
        assert_eq!(
            ids(&store
                .script_monitors(&wm, Some(&aid("agent-a")))
                .await
                .unwrap()),
            ["m1"]
        );
        assert!(store
            .script_monitors(&wm, Some(&aid("agent-c")))
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            store.script_monitor(&wm, "m1").await.unwrap(),
            monitor("m1", tenant)
        );
        assert!(invalid(&store.script_monitor(&wn, "m1").await));
        assert!(invalid(&store.script_monitor(&wm, "m-gone").await));

        let waiting = store.script_monitor_waiting(None, None).await.unwrap();
        let expect = |agent: &str, id: &str, script: &str| {
            (
                agent.to_owned(),
                json!({
                    "monitorId": id, "scriptId": script, "runId": format!("run-{id}"),
                    "scriptName": format!("{tenant} {id}"),
                    "expiresAt": format!("2026-02-01T00:00:00.000Z/{tenant}"),
                }),
            )
        };
        assert_eq!(
            waiting,
            [
                expect("agent-b", "m2", "s2"),
                expect("agent-c", "m3", "s1"),
                expect("agent-a", "m1", "s1"),
            ]
        );
        assert_eq!(
            store.script_monitor_waiting(Some(&wn), None).await.unwrap(),
            [expect("agent-c", "m3", "s1")]
        );
        assert_eq!(
            store
                .script_monitor_waiting(Some(&wm), Some(&aid("agent-a")))
                .await
                .unwrap(),
            [expect("agent-a", "m1", "s1")]
        );

        store
            .script_monitor_cancel_intent(&wm, "m1", true)
            .await
            .unwrap();
        store
            .script_monitor_cancel_intent(&wn, "m2", true)
            .await
            .unwrap();
        let mut pending: Vec<(String, String, bool)> = store
            .pending_script_monitors()
            .await
            .unwrap()
            .into_iter()
            .map(|(m, intent)| (m.monitor_id, m.script_name, intent))
            .collect();
        pending.sort();
        assert_eq!(
            pending,
            [
                ("m1".to_owned(), format!("{tenant} m1"), true),
                ("m2".to_owned(), format!("{tenant} m2"), false),
                ("m3".to_owned(), format!("{tenant} m3"), false),
            ]
        );
        assert_eq!(store.script_monitor_reservations(&wm).await.unwrap(), 2);
        assert_eq!(store.script_monitor_reservations(&wn).await.unwrap(), 1);

        if tenant == "decoy" {
            store
                .script_monitor_cancel_intent(&wm, "m1", false)
                .await
                .unwrap();
            insert(&store, &["m-gone"], tenant).await;
        }
    }
);

parity_test!(
    script_monitor_summary_page_lists_active_monitors,
    |store, tenant| {
        let wm = wid("ws-m");
        let opts = |limit: usize, cursor: Option<String>| ScriptListOptions {
            summary: true,
            limit,
            cursor,
        };
        let page_ids = |page: &Value| -> Vec<String> {
            page["monitors"]
                .as_array()
                .unwrap()
                .iter()
                .map(|m| m["monitorId"].as_str().unwrap().to_owned())
                .collect()
        };
        seed(&store, tenant).await;
        let empty = store
            .script_summary_page(&wm, None, true, &opts(10, None))
            .await
            .unwrap();
        assert!(page_ids(&empty).is_empty());

        insert(&store, &["m1", "m2", "m3"], tenant).await;
        let first = store
            .script_summary_page(&wm, None, true, &opts(1, None))
            .await
            .unwrap();
        assert_eq!(page_ids(&first), ["m1"]);
        assert_eq!(
            first["monitors"][0]["scriptName"],
            json!(format!("{tenant} m1"))
        );
        let cursor = first["nextCursor"].as_str().unwrap().to_owned();
        let second = store
            .script_summary_page(&wm, None, true, &opts(1, Some(cursor)))
            .await
            .unwrap();
        assert_eq!(page_ids(&second), ["m2"]);
        assert!(second.get("nextCursor").is_none());
        let owned = store
            .script_summary_page(&wm, Some(&aid("agent-b")), true, &opts(10, None))
            .await
            .unwrap();
        assert_eq!(page_ids(&owned), ["m2"]);

        if tenant == "decoy" {
            insert(&store, &["m-gone"], tenant).await;
        }
    }
);

parity_test!(
    script_monitor_settle_jobs_and_wake_fences,
    |store, tenant| {
        let (wm, wn) = (wid("ws-m"), wid("ws-n"));
        let (a, b, c) = (aid("agent-a"), aid("agent-b"), aid("agent-c"));
        seed(&store, tenant).await;
        assert!(store.script_notification_jobs().await.unwrap().is_empty());
        assert!(!store.script_monitor_wake_pending("m1").await.unwrap());
        assert!(!store.script_monitor_wake_allowed("m1").await.unwrap());
        assert!(!store.script_monitor_pending_for_agent(&a).await.unwrap());
        assert!(store
            .script_notification_owners(&wm, None)
            .await
            .unwrap()
            .is_empty());

        insert(&store, &["m1", "m2", "m3"], tenant).await;
        assert_eq!(
            store.script_notification_owners(&wn, None).await.unwrap(),
            std::slice::from_ref(&c)
        );
        let m1 = settled(&monitor("m1", tenant), "triggered");
        assert!(invalid(
            &store.settle_script_monitor(&monitor("m1", tenant)).await
        ));
        assert!(!store
            .settle_script_monitor(&ScriptMonitor {
                run_id: "run-other".into(),
                ..settled(&monitor("m3", tenant), "triggered")
            })
            .await
            .unwrap());
        assert!(store.settle_script_monitor(&m1).await.unwrap());
        assert!(!store.settle_script_monitor(&m1).await.unwrap());
        assert!(store
            .settle_script_monitor(&settled(&monitor("m2", tenant), "cancelled"))
            .await
            .unwrap());

        let jobs = store.script_notification_jobs().await.unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].wake_id, "script-monitor:m1");
        assert_eq!(jobs[0].workspace_id, wm);
        assert_eq!(jobs[0].agent_id, a);
        assert_eq!(
            jobs[0].metadata,
            intent_core::script_monitor::script_monitor_wake_metadata(&m1)
        );
        assert_eq!(
            jobs[0].body,
            intent_core::script_monitor::script_monitor_wake_text(&m1)
        );
        assert!(store.script_monitor_wake_pending("m1").await.unwrap());
        assert!(!store.script_monitor_wake_pending("m2").await.unwrap());
        assert!(store.script_monitor_wake_allowed("m1").await.unwrap());
        assert!(!store.script_monitor_wake_allowed("m3").await.unwrap());
        assert!(store
            .script_monitor_wake_allowed_for_agent("m1", &a, &wm)
            .await
            .unwrap());
        assert!(!store
            .script_monitor_wake_allowed_for_agent("m1", &b, &wm)
            .await
            .unwrap());
        assert!(!store
            .script_monitor_wake_allowed_for_agent("m1", &a, &wn)
            .await
            .unwrap());
        assert!(store.script_monitor_pending_for_agent(&a).await.unwrap());
        assert!(!store.script_monitor_pending_for_agent(&b).await.unwrap());
        assert_eq!(
            store.script_notification_owners(&wm, None).await.unwrap(),
            std::slice::from_ref(&a)
        );
        assert!(store
            .script_notification_owners(&wm, Some(&b))
            .await
            .unwrap()
            .is_empty());
        assert_eq!(store.script_monitor_reservations(&wm).await.unwrap(), 1);

        store.finish_script_monitor_wake("m1", false).await.unwrap();
        assert!(store.script_monitor_wake_pending("m1").await.unwrap());
        store.finish_script_monitor_wake("m1", true).await.unwrap();
        assert!(!store.script_monitor_wake_pending("m1").await.unwrap());
        assert!(store.script_notification_jobs().await.unwrap().is_empty());
        assert_eq!(
            ids(&store.script_monitors(&wn, None).await.unwrap()),
            ["m3"]
        );

        decoy_rows(&store, tenant).await;
    }
);

parity_test!(script_notification_admission_lifecycle, |store, tenant| {
    let (wm, wn) = (wid("ws-m"), wid("ws-n"));
    let (a, b) = (aid("agent-a"), aid("agent-b"));
    seed(&store, tenant).await;
    assert!(store
        .script_notification_delivery_owners()
        .await
        .unwrap()
        .is_empty());
    assert!(store.load_all_agent_queues().await.unwrap().is_empty());
    insert(&store, &["m1", "m2", "m3"], tenant).await;
    settle(&store, &["m1", "m2", "m3"], tenant).await;

    let q1 = queue(&monitor("m1", tenant), tenant);
    let q2 = queue(&monitor("m2", tenant), tenant);
    assert!(
        !store
            .admit_script_notification(&AgentQueueRow {
                agent_id: a.clone(),
                ..q2.clone()
            })
            .await
            .unwrap(),
        "a job admits only to its owner"
    );
    assert!(store.admit_script_notification(&q1).await.unwrap());
    assert!(!store.admit_script_notification(&q1).await.unwrap());
    let queued = store.load_all_agent_queues().await.unwrap();
    assert_eq!(queued.len(), 1);
    assert_eq!(queued[0].id, "script-monitor:m1");
    assert_eq!(queued[0].payload, q1.payload);
    let wakes: Vec<String> = store
        .script_notification_jobs()
        .await
        .unwrap()
        .into_iter()
        .map(|j| j.wake_id)
        .collect();
    assert_eq!(wakes, ["script-monitor:m2", "script-monitor:m3"]);
    assert!(!store.script_monitor_wake_pending("m1").await.unwrap());
    assert!(store.script_monitor_wake_allowed("m1").await.unwrap());
    assert!(store
        .script_monitor_wake_allowed_for_agent("m1", &a, &wm)
        .await
        .unwrap());
    assert_eq!(
        store.script_notification_delivery_owners().await.unwrap(),
        [(a.clone(), wm.clone())]
    );
    assert_eq!(store.script_monitor_reservations(&wm).await.unwrap(), 1);

    assert!(!store.accept_script_notification(&b, "m1").await.unwrap());
    assert!(store.accept_script_notification(&a, "m1").await.unwrap());
    assert!(store.accept_script_notification(&a, "m1").await.unwrap());
    let content = format!("content of {tenant}");
    store
        .continue_script_notification(&b, "m1", "turn-x", "x")
        .await
        .unwrap();
    store
        .continue_script_notification(&a, "m1", "turn-2", &content)
        .await
        .unwrap();
    let queued = store.load_all_agent_queues().await.unwrap();
    assert_eq!(queued[0].turn_id, "turn-2");
    let mut expect = q1.payload.clone();
    expect["turnId"] = json!("turn-2");
    expect["content"] = json!(content);
    expect["persisted"] = json!(true);
    assert_eq!(queued[0].payload, expect);

    store.complete_script_notification(&a, "m1").await.unwrap();
    assert!(!store.script_monitor_wake_allowed("m1").await.unwrap());
    assert!(!store.accept_script_notification(&a, "m1").await.unwrap());
    assert!(store.load_all_agent_queues().await.unwrap().is_empty());
    assert!(store
        .script_notification_delivery_owners()
        .await
        .unwrap()
        .is_empty());
    store
        .cleanup_completed_script_notifications()
        .await
        .unwrap();

    assert!(store.admit_script_notification(&q2).await.unwrap());
    assert_eq!(
        store.script_notification_owners(&wm, None).await.unwrap(),
        std::slice::from_ref(&b)
    );
    store
        .suppress_script_notifications(&wm, Some(&a))
        .await
        .unwrap();
    assert!(store.script_monitor_wake_allowed("m2").await.unwrap());
    store
        .suppress_script_notifications(&wm, Some(&b))
        .await
        .unwrap();
    assert!(!store.script_monitor_wake_allowed("m2").await.unwrap());
    assert!(store.load_all_agent_queues().await.unwrap().is_empty());
    assert!(store.script_monitor_wake_pending("m3").await.unwrap());
    store
        .suppress_script_notifications(&wn, None)
        .await
        .unwrap();
    assert!(store.script_notification_jobs().await.unwrap().is_empty());
    assert!(store
        .script_notification_owners(&wm, None)
        .await
        .unwrap()
        .is_empty());

    decoy_rows(&store, tenant).await;
});
