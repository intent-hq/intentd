//! Script monitors, notification jobs and their queue admission on both
//! backends, plus the monitor branch of the script summary page.
//!
//! Decoy rule: each body ends by giving the decoy rows with the ids the
//! subject reads, probes as absent or writes, in their pre-write state
//! (active monitors, pending jobs, and admissions with their queue copies:
//! `m1` accepted, which every non-completed admission filter reaches, and
//! `m2` completed, which only the completed-notification cleanup reaches).
//! Script names and payloads carry the tenant, so a leaked row never looks
//! right.

use intent_core::script_lists::ScriptListOptions;
use std::collections::HashSet;

use intent_core::{
    AgentId, AgentSession, Error, Hook, HookId, HookState, ScriptMode, ScriptMonitor,
    ScriptMonitorTrigger, Workspace, WorkspaceId,
};
use intent_store::{AgentQueueRow, Store};
use serde_json::{json, Value};

use crate::harness::{parity_test, TestDb};

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
/// `m1`–`m3`, an accepted admission for `m1` (reached by an unscoped
/// `continue_script_notification`) and a completed admission for `m2`
/// (reached by an unscoped `cleanup_completed_script_notifications` while
/// the subject has `m2` queued), each with its queue copy.
async fn decoy_rows(store: &Store, tenant: &str, db: Option<&TestDb>) {
    if tenant != "decoy" {
        return;
    }
    let db = db.expect("the decoy tenant runs only on Postgres");
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
    assert!(store
        .accept_script_notification(&aid("agent-a"), "m1")
        .await
        .unwrap());
    // `complete_script_notification` sweeps completed rows straight away, so
    // only SQL can leave one behind; keep its queue copy for the sweep's
    // queue DELETE.
    let done = sqlx::query(
        "UPDATE agent_queue_admission SET phase = 'completed', payload = '{}' \
         WHERE tenant_id = 'decoy' AND id = 'script-monitor:m2'",
    )
    .execute(&db.admin)
    .await
    .unwrap();
    assert_eq!(done.rows_affected(), 1);
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
    |store, tenant, db| {
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

        decoy_rows(&store, tenant, db).await;
    }
);

parity_test!(
    script_notification_admission_lifecycle,
    |store, tenant, db| {
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
        store
            .cleanup_completed_script_notifications()
            .await
            .unwrap();
        assert_eq!(
            store.load_all_agent_queues().await.unwrap()[0].id,
            "script-monitor:m2",
            "cleanup leaves a pending admission's queue copy"
        );
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

        decoy_rows(&store, tenant, db).await;
    }
);

fn hook(id: &str, ws: &str, agent: &str, state: HookState, tenant: &str) -> Hook {
    Hook {
        hook_id: HookId(id.into()),
        workspace_id: wid(ws),
        agent_id: aid(agent),
        name: format!("{tenant} {id}"),
        code: format!("return '{tenant}'"),
        delay_ms: 1000,
        cron: None,
        run_at: None,
        state,
        created_at: T0.into(),
        last_run_at: None,
        next_run_at: Some(T1.into()),
        run_count: 0,
        last_error: None,
        last_logs: None,
        last_state: None,
        expires_at: None,
        perpetual: false,
        dispatch_count: 0,
    }
}

/// A workspace with one agent of the same suffix (`ws-h` / `agent-h`).
async fn seed_parent(store: &Store, tenant: &str, suffix: &str) {
    let ws = format!("ws-{suffix}");
    store
        .insert_workspace(&Workspace {
            id: wid(&ws),
            title: format!("{tenant} {ws}"),
            branch: "main".into(),
            created_at: T0.into(),
            updated_at: T0.into(),
            ..Default::default()
        })
        .await
        .unwrap();
    store
        .insert_agent_session(&AgentSession {
            id: aid(&format!("agent-{suffix}")),
            workspace_id: wid(&ws),
            name: format!("agent-{suffix} of {tenant}"),
            created_at: T0.into(),
            updated_at: T0.into(),
            ..AgentSession::default()
        })
        .await
        .unwrap();
}

parity_test!(
    workspaces_with_active_hooks_or_script_monitors_unions_both_signals,
    |store, tenant| {
        // `ws-m` is waiting through its monitor alone, `ws-n` through a
        // scheduled hook alone, `ws-h` through a running hook; `ws-x` holds
        // only settled rows and `ws-d` is never created by the subject.
        seed(&store, tenant).await;
        seed_parent(&store, tenant, "h").await;
        seed_parent(&store, tenant, "x").await;
        let requested: Vec<WorkspaceId> = ["ws-m", "ws-n", "ws-h", "ws-x", "ws-d", "ws-missing"]
            .into_iter()
            .map(wid)
            .collect();
        assert!(store
            .workspaces_with_active_hooks_or_script_monitors(&requested)
            .await
            .unwrap()
            .is_empty());

        insert(&store, &["m1", "m3"], tenant).await;
        settle(&store, &["m3"], tenant).await;
        for (id, ws, agent, state) in [
            ("hm", "ws-m", "agent-a", HookState::Cancelled),
            ("hn", "ws-n", "agent-c", HookState::Scheduled),
            ("hh", "ws-h", "agent-h", HookState::Running),
            ("hx", "ws-x", "agent-x", HookState::Dispatched),
        ] {
            store
                .insert_hook(&hook(id, ws, agent, state, tenant))
                .await
                .unwrap();
        }

        assert_eq!(
            store
                .workspaces_with_active_hooks_or_script_monitors(&requested)
                .await
                .unwrap(),
            HashSet::from([wid("ws-m"), wid("ws-n"), wid("ws-h")])
        );
        assert_eq!(
            store
                .workspaces_with_active_hooks_or_script_monitors(&[wid("ws-m"), wid("ws-x")])
                .await
                .unwrap(),
            HashSet::from([wid("ws-m")])
        );
        assert!(store
            .workspaces_with_active_hooks_or_script_monitors(&[wid("ws-x"), wid("ws-d")])
            .await
            .unwrap()
            .is_empty());
        assert!(store
            .workspaces_with_active_hooks_or_script_monitors(&[])
            .await
            .unwrap()
            .is_empty());

        if tenant == "decoy" {
            // Active rows where the subject has none, and inactive hooks
            // under the subject's active hook ids. Settling deletes a
            // monitor row, so the decoy's `m1` stays active.
            seed_parent(&store, tenant, "d").await;
            store
                .insert_hook(&hook("hd", "ws-d", "agent-d", HookState::Scheduled, tenant))
                .await
                .unwrap();
            store
                .insert_hook(&hook("hx2", "ws-x", "agent-x", HookState::Running, tenant))
                .await
                .unwrap();
            for (id, ws, agent, script) in [
                ("md", "ws-d", "agent-d", "s-d"),
                ("mx", "ws-x", "agent-x", "s-x"),
            ] {
                store
                    .insert_script_monitor(&ScriptMonitor {
                        monitor_id: id.into(),
                        workspace_id: wid(ws),
                        agent_id: aid(agent),
                        script_id: script.into(),
                        ..monitor(id, tenant)
                    })
                    .await
                    .unwrap();
            }
            for id in ["hn", "hh"] {
                store
                    .update_hook_state(&HookId(id.into()), HookState::Cancelled)
                    .await
                    .unwrap();
            }
        }
    }
);
