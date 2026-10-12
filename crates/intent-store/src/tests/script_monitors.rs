//! Active monitor isolation, lifecycle fencing and transactional delivery tests.
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
    assert!(reopened.script_monitor(&a, "one").await.is_err());
    assert!(reopened.script_monitors(&a, None).await.unwrap().is_empty());
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
    // Transcript persistence is not worker admission.
    assert!(store.script_monitor_wake_pending("first").await.unwrap());
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
async fn monitor_cleanup_removes_active_and_pending_without_terminal_rows() {
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
    assert_eq!(store.script_monitors(&ws, None).await.unwrap().len(), 1);
    store
        .set_agent_session_retired_at(&ws, &agent, Some(&now_iso()), &now_iso())
        .await
        .unwrap();
    assert!(store.script_monitor(&ws, "active").await.is_err());
    assert!(store
        .insert_script_monitor(&monitor(&ws, &agent, "refused"))
        .await
        .is_err());
    assert!(store.script_monitors(&ws, None).await.unwrap().is_empty());
}

fn queue(row: &ScriptMonitor) -> crate::AgentQueueRow {
    let id = format!("script-monitor:{}", row.monitor_id);
    crate::AgentQueueRow {
        id: id.clone(),
        agent_id: row.agent_id.clone(),
        position: 0,
        payload: json!({"id":id,"turnId":id,"messageMetadata":metadata(row)}),
        created_at: now_iso(),
        turn_id: id,
    }
}

#[tokio::test]
async fn monitor_retirement_and_queue_transfer_rollback_and_recover() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let (ws, agent) = seed(&store, "atomic").await;
    let row = monitor(&ws, &agent, "atomic");
    store.insert_script_monitor(&row).await.unwrap();
    sqlx::query("CREATE TRIGGER fail_job BEFORE INSERT ON script_notification_job BEGIN SELECT RAISE(ABORT,'injected'); END").execute(store.write_pool()).await.unwrap();
    assert!(store.settle_script_monitor(&triggered(&row)).await.is_err());
    assert_eq!(store.script_monitor(&ws, "atomic").await.unwrap(), row);
    assert!(store.script_notification_jobs().await.unwrap().is_empty());
    sqlx::query("DROP TRIGGER fail_job")
        .execute(store.write_pool())
        .await
        .unwrap();
    assert!(store.settle_script_monitor(&triggered(&row)).await.unwrap());
    let queued = queue(&row);
    sqlx::query("CREATE TRIGGER fail_admission BEFORE INSERT ON agent_queue_admission BEGIN SELECT RAISE(ABORT,'injected'); END").execute(store.write_pool()).await.unwrap();
    assert!(store.admit_script_notification(&queued).await.is_err());
    assert!(store.script_monitor_wake_pending("atomic").await.unwrap());
    assert!(store.load_all_agent_queues().await.unwrap().is_empty());
    sqlx::query("DROP TRIGGER fail_admission")
        .execute(store.write_pool())
        .await
        .unwrap();
    // A crash after transcript insertion must retain notification work.
    store
        .append_agent_message_with_provenance(
            &agent,
            &queued.id,
            "user",
            &json!([]),
            Some(&metadata(&row)),
            &now_iso(),
            crate::UsageMessageOrigin::Excluded,
        )
        .await
        .unwrap();
    assert!(store.script_monitor_wake_pending("atomic").await.unwrap());
    store.close().await;
    let store = Store::open(&tmp.path).await.unwrap();
    let (a, b) = tokio::join!(
        store.admit_script_notification(&queued),
        store.admit_script_notification(&queued)
    );
    assert_ne!(a.unwrap(), b.unwrap());
    assert!(!store.script_monitor_wake_pending("atomic").await.unwrap());
    assert!(store.script_monitor_wake_allowed("atomic").await.unwrap());
    assert_eq!(store.load_all_agent_queues().await.unwrap().len(), 1);
    // The ordinary snapshot can shrink before the worker starts; durable receipt survives.
    store.replace_agent_queue(&agent, &[]).await.unwrap();
    assert_eq!(
        store.load_all_agent_queues().await.unwrap()[0].turn_id,
        queued.id
    );
    assert!(store
        .accept_script_notification(&agent, "atomic")
        .await
        .unwrap());
    assert!(store
        .accept_script_notification(&agent, "atomic")
        .await
        .unwrap());
    store.close().await;
    let store = Store::open(&tmp.path).await.unwrap();
    assert_eq!(store.load_all_agent_queues().await.unwrap().len(), 1);
    assert!(store
        .accept_script_notification(&agent, "atomic")
        .await
        .unwrap());
    store
        .complete_script_notification(&agent, "atomic")
        .await
        .unwrap();
    assert!(store.load_all_agent_queues().await.unwrap().is_empty());
    assert!(!store
        .accept_script_notification(&agent, "atomic")
        .await
        .unwrap());
    store.replace_agent_queue(&agent, &[queued]).await.unwrap();
    assert!(store.load_all_agent_queues().await.unwrap().is_empty());
    assert!(store
        .get_agent_message_by_id_with_pruned(&agent, "script-monitor:atomic")
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
async fn monitor_retirement_and_queue_insert_failure_leave_no_partial_work() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let (ws, agent) = seed(&store, "queue-failure").await;
    let row = monitor(&ws, &agent, "queue-failure");
    store.insert_script_monitor(&row).await.unwrap();
    store.settle_script_monitor(&triggered(&row)).await.unwrap();
    sqlx::query("CREATE TRIGGER fail_queue BEFORE INSERT ON agent_queue BEGIN SELECT RAISE(ABORT,'injected'); END").execute(store.write_pool()).await.unwrap();
    assert!(store.admit_script_notification(&queue(&row)).await.is_err());
    assert!(store
        .script_monitor_wake_pending(&row.monitor_id)
        .await
        .unwrap());
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM agent_queue_admission")
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
    sqlx::query("DROP TRIGGER fail_queue")
        .execute(store.write_pool())
        .await
        .unwrap();
    // Failure after queue insertion must roll back the receipt and queue too.
    sqlx::query("CREATE TRIGGER fail_job_delete BEFORE DELETE ON script_notification_job BEGIN SELECT RAISE(ABORT,'injected'); END").execute(store.write_pool()).await.unwrap();
    assert!(store.admit_script_notification(&queue(&row)).await.is_err());
    assert!(store
        .script_monitor_wake_pending(&row.monitor_id)
        .await
        .unwrap());
    assert!(store.load_all_agent_queues().await.unwrap().is_empty());
    sqlx::query("DROP TRIGGER fail_job_delete")
        .execute(store.write_pool())
        .await
        .unwrap();
    store.admit_script_notification(&queue(&row)).await.unwrap();
    store
        .set_agent_session_retired_at(&ws, &agent, Some(&now_iso()), &now_iso())
        .await
        .unwrap();
    store
        .set_agent_session_retired_at(&ws, &agent, None, &now_iso())
        .await
        .unwrap();
    assert!(!store
        .accept_script_notification(&agent, &row.monitor_id)
        .await
        .unwrap());
    store
        .replace_agent_queue(&agent, &[queue(&row)])
        .await
        .unwrap();
    assert!(store.load_all_agent_queues().await.unwrap().is_empty());
}

#[tokio::test]
async fn monitor_upgrade_preserves_active_and_pending_but_never_replays_delivered_history() {
    let tmp = TempDb::new();
    let pool = crate::connect_write(&tmp.path).await.unwrap();
    let prefix = sqlx::migrate::Migrator {
        migrations: std::borrow::Cow::Owned(
            crate::MIGRATOR
                .iter()
                .filter(|m| m.version < 157)
                .cloned()
                .collect(),
        ),
        ..sqlx::migrate::Migrator::DEFAULT
    };
    prefix.run(&pool).await.unwrap();
    sqlx::raw_sql("INSERT INTO workspace(id,title,branch,created_at,updated_at) VALUES('upgrade','Upgrade','main','t','t'); INSERT INTO agent_session(id,workspace_id,name,status,created_at,updated_at) VALUES('owner','upgrade','Owner','idle','t','t');").execute(&pool).await.unwrap();
    let ws = WorkspaceId::from("upgrade");
    let agent = AgentId::from("owner");
    for (id, state, wake) in [
        ("active", "active", "none"),
        ("pending", "triggered", "pending"),
        ("queued", "triggered", "delivered"),
        ("history", "triggered", "delivered"),
        ("suppressed", "cancelled", "suppressed"),
    ] {
        let mut row = monitor(&ws, &agent, id);
        row.script_id = id.into();
        if state != "active" {
            row = triggered(&row);
            row.state = state.into();
        }
        sqlx::query("INSERT INTO script_monitor(id,workspace_id,agent_id,script_id,run_id,state,row_json,created_at,settled_at,cancel_intent,wake_state) VALUES(?,'upgrade','owner',?,'run-key',?,?,?, ?,1,?)").bind(id).bind(id).bind(state).bind(serde_json::to_string(&row).unwrap()).bind(&row.created_at).bind(&row.settled_at).bind(if wake=="delivered" {"pending"} else {wake}).execute(&pool).await.unwrap();
        if wake == "delivered" {
            sqlx::query("INSERT INTO agent_message(id,agent_id,seq,role,content,created_at,metadata) VALUES(?,'owner',?,'user','[]','t',?)").bind(format!("script-monitor:{id}")).bind(if id=="queued" {1_i64}else{2_i64}).bind(metadata(&row).to_string()).execute(&pool).await.unwrap();
        }
        if id == "queued" {
            let q = queue(&row);
            sqlx::query("INSERT INTO agent_queue(id,agent_id,position,payload,created_at,turn_id) VALUES(?,'owner',0,?,'t',?)").bind(&q.id).bind(q.payload.to_string()).bind(&q.turn_id).execute(&pool).await.unwrap();
        }
    }
    // A mid-migration DDL failure rolls back copied jobs and trigger changes.
    sqlx::query("CREATE TABLE agent_queue_admission (id TEXT)")
        .execute(&pool)
        .await
        .unwrap();
    assert!(crate::MIGRATOR.run(&pool).await.is_err());
    let old_count: i64 =
        sqlx::query_scalar("SELECT count(*) FROM script_monitor WHERE wake_state='pending'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(old_count, 1);
    let leaked_jobs: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM sqlite_master WHERE name='script_notification_job'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(leaked_jobs, 0);
    sqlx::query("DROP TABLE agent_queue_admission")
        .execute(&pool)
        .await
        .unwrap();
    crate::MIGRATOR.run(&pool).await.unwrap();
    crate::MIGRATOR.run(&pool).await.unwrap();
    pool.close().await;
    let store = Store::open(&tmp.path).await.unwrap();
    assert_eq!(store.script_monitors(&ws, None).await.unwrap().len(), 1);
    let active = store.pending_script_monitors().await.unwrap();
    assert!(active[0].1);
    assert_eq!(active[0].0.monitor_id, "active");
    let jobs = store.script_notification_jobs().await.unwrap();
    assert_eq!(jobs.len(), 2);
    assert!(jobs.iter().all(
        |job| job.wake_id == "script-monitor:pending" || job.wake_id == "script-monitor:queued"
    ));
    assert_eq!(store.load_all_agent_queues().await.unwrap().len(), 1);
    assert!(sqlx::query("UPDATE script_monitor SET state='completed'")
        .execute(store.write_pool())
        .await
        .is_err());
    assert!(!store.script_monitor_wake_allowed("history").await.unwrap());
    assert!(!store
        .script_monitor_wake_allowed("suppressed")
        .await
        .unwrap());
}

#[tokio::test]
async fn script_pages_project_after_active_key_selection_and_scope_cursors() {
    use intent_core::script_lists::ScriptListOptions;
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let (ws, owner) = seed(&store, "pages").await;
    for i in 0..5 {
        sqlx::query("INSERT INTO script(id,workspace_id,name,command,env,mode,source,created_at,purpose,archived_at) VALUES(?,?,?, ?,?,'command','user','same','saved',?)")
            .bind(format!("id-{i}")).bind(ws.as_str()).bind(format!("Name {i}")).bind("é😀".repeat(5000)).bind("{\"LARGE\":\"unneeded\"}")
            .bind((i==4).then_some("archived")).execute(store.write_pool()).await.unwrap();
    }
    let last_run =
        json!({"runId":"last","outcome":"failed","stoppedAt":"same","error":"é😀".repeat(1000)});
    sqlx::query("UPDATE script SET last_run=? WHERE id='id-3'")
        .bind(last_run.to_string())
        .execute(store.write_pool())
        .await
        .unwrap();
    let opts = ScriptListOptions::parse(&json!({"view":"summary","limit":2}), false).unwrap();
    let first = store
        .script_summary_page(&ws, None, false, &opts)
        .await
        .unwrap();
    assert_eq!(first["scripts"].as_array().unwrap().len(), 2);
    assert_eq!(first["scripts"][0]["id"], "id-3");
    let row = &first["scripts"][0];
    assert!(row.get("command").is_none());
    assert!(row.get("env").is_none());
    assert!(row["commandPreview"].as_str().unwrap().len() <= 160);
    assert_eq!(row["commandTruncated"], true);
    assert!(row["lastRun"]["error"].as_str().unwrap().len() <= 256);
    assert_eq!(row["lastRun"]["errorTruncated"], true);
    assert_eq!(row["lastRun"]["runId"], "last");
    let full = store.get_active_script(&ws, "id-3").await.unwrap().unwrap();
    let full_bytes = serde_json::to_vec(&full).unwrap().len();
    let summary_bytes = serde_json::to_vec(row).unwrap().len();
    assert!(summary_bytes * 10 < full_bytes);
    eprintln!(
        "production script projection: full={full_bytes} bytes summary={summary_bytes} bytes"
    );
    store.remove_script("id-2").await.unwrap();
    let opts = ScriptListOptions::parse(
        &json!({"view":"summary","limit":1,"cursor":first["nextCursor"]}),
        false,
    )
    .unwrap();
    let next = store
        .script_summary_page(&ws, None, false, &opts)
        .await
        .unwrap();
    assert_eq!(next["scripts"][0]["id"], "id-1");
    assert!(store
        .script_summary_page(&WorkspaceId::from("other"), None, false, &opts)
        .await
        .is_err());
    assert!(store
        .script_summary_page(&ws, Some(&owner), true, &opts)
        .await
        .is_err());
    // Poison the archived body. Selection must exclude it before decoding definitions.
    sqlx::query("UPDATE script SET env='invalid' WHERE id='id-4'")
        .execute(store.write_pool())
        .await
        .unwrap();
    assert!(store.list_unarchived_scripts().await.is_ok());
    assert!(store
        .get_active_script(&ws, "id-4")
        .await
        .unwrap()
        .is_none());
    assert!(store
        .script_summary_page(&ws, None, false, &opts)
        .await
        .is_ok());
    let mut watch = monitor(&ws, &owner, "paged-watch");
    watch.output_pattern = Some("x".repeat(1000));
    store.insert_script_monitor(&watch).await.unwrap();
    let opts = ScriptListOptions::parse(&json!({"view":"summary","limit":1}), true).unwrap();
    let page = store
        .script_summary_page(&ws, Some(&owner), true, &opts)
        .await
        .unwrap();
    assert_eq!(
        page["monitors"][0]["outputPatternPreview"]
            .as_str()
            .unwrap()
            .len(),
        160
    );
    assert!(page["monitors"][0].get("outputPattern").is_none());
    store
        .settle_script_monitor(&triggered(&watch))
        .await
        .unwrap();
    assert!(store
        .script_summary_page(&ws, None, true, &opts)
        .await
        .unwrap()["monitors"]
        .as_array()
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn monitor_lifecycle_cleanup_removes_active_pending_and_accepted_work() {
    for lifecycle in ["retire", "delete-agent", "archive", "delete-workspace"] {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.unwrap();
        let (ws, agent) = seed(&store, lifecycle).await;
        for (id, stage) in [("active", 0), ("pending", 1), ("accepted", 2)] {
            let mut row = monitor(&ws, &agent, id);
            row.script_id = id.into();
            store.insert_script_monitor(&row).await.unwrap();
            if stage > 0 {
                store.settle_script_monitor(&triggered(&row)).await.unwrap();
            }
            if stage > 1 {
                store.admit_script_notification(&queue(&row)).await.unwrap();
                store.accept_script_notification(&agent, id).await.unwrap();
            }
        }
        match lifecycle {
            "retire" => {
                store
                    .set_agent_session_retired_at(&ws, &agent, Some(&now_iso()), &now_iso())
                    .await
                    .unwrap();
            }
            "delete-agent" => {
                store.delete_agent_session(&ws, &agent).await.unwrap();
            }
            "archive" => {
                sqlx::query("UPDATE workspace SET archived=1 WHERE id=?")
                    .bind(ws.as_str())
                    .execute(store.write_pool())
                    .await
                    .unwrap();
            }
            _ => {
                store.delete_workspace(&ws).await.unwrap();
            }
        }
        assert!(
            store.script_monitors(&ws, None).await.unwrap().is_empty(),
            "{lifecycle}"
        );
        assert!(
            store.script_notification_jobs().await.unwrap().is_empty(),
            "{lifecycle}"
        );
        assert!(
            store.load_all_agent_queues().await.unwrap().is_empty(),
            "{lifecycle}"
        );
        assert!(
            !store.script_monitor_wake_allowed("accepted").await.unwrap(),
            "{lifecycle}"
        );
    }
}

#[tokio::test]
async fn monitor_transfer_preserves_delivery_and_reimport_never_revives_settled_work() {
    for phase in [
        "pending",
        "message-persisted",
        "admitted",
        "accepted",
        "completed",
    ] {
        let source_tmp = TempDb::new();
        let source = Store::open(&source_tmp.path).await.unwrap();
        let (ws, agent) = seed(&source, "transfer-delivery").await;
        let row = monitor(&ws, &agent, "delivery");
        source.insert_script_monitor(&row).await.unwrap();
        source
            .settle_script_monitor(&triggered(&row))
            .await
            .unwrap();
        let mut active = monitor(&ws, &agent, "active");
        active.script_id = "another-script".into();
        source.insert_script_monitor(&active).await.unwrap();
        if phase != "pending" {
            source
                .append_agent_message_with_provenance(
                    &agent,
                    "script-monitor:delivery",
                    "user",
                    &json!([{"type":"text","text":"wake"}]),
                    Some(&metadata(&row)),
                    &now_iso(),
                    crate::UsageMessageOrigin::Excluded,
                )
                .await
                .unwrap();
        }
        if matches!(phase, "admitted" | "accepted" | "completed") {
            assert!(source
                .admit_script_notification(&queue(&row))
                .await
                .unwrap());
        }
        if matches!(phase, "accepted" | "completed") {
            assert!(source
                .accept_script_notification(&agent, "delivery")
                .await
                .unwrap());
            source.replace_agent_queue(&agent, &[]).await.unwrap();
        }
        if phase == "completed" {
            source
                .complete_script_notification(&agent, "delivery")
                .await
                .unwrap();
        }
        let exported = source.transfer_export_rows(&ws).await.unwrap();
        for lifecycle in [
            "complete",
            "retire",
            "delete-agent",
            "archive",
            "delete-workspace",
        ] {
            let dest_tmp = TempDb::new();
            let dest = Store::open(&dest_tmp.path).await.unwrap();
            dest.transfer_import_rows(&exported).await.unwrap();
            // The public import path rejects an existing workspace; the store
            // transaction must leave its delivery obligations unchanged too.
            assert!(dest.transfer_import_rows(&exported).await.is_err());
            assert_eq!(
                dest.script_monitors(&ws, None).await.unwrap(),
                vec![active.clone()]
            );
            assert_eq!(
                dest.script_notification_jobs().await.unwrap().len(),
                usize::from(matches!(phase, "pending" | "message-persisted"))
            );
            assert_eq!(
                dest.load_all_agent_queues().await.unwrap().len(),
                usize::from(matches!(phase, "admitted" | "accepted"))
            );
            assert_eq!(
                dest.script_monitor_wake_allowed("delivery").await.unwrap(),
                phase != "completed"
            );
            if phase != "pending" {
                assert!(dest
                    .get_agent_message_by_id_with_pruned(&agent, "script-monitor:delivery")
                    .await
                    .unwrap()
                    .is_some());
            }
            match lifecycle {
                "complete" => {
                    let mut cancelled = active.clone();
                    cancelled.state = "cancelled".into();
                    dest.settle_script_monitor(&cancelled).await.unwrap();
                    if matches!(phase, "pending" | "message-persisted") {
                        assert!(dest.admit_script_notification(&queue(&row)).await.unwrap());
                    }
                    dest.complete_script_notification(&agent, "delivery")
                        .await
                        .unwrap();
                }
                "retire" => {
                    dest.set_agent_session_retired_at(&ws, &agent, Some(&now_iso()), &now_iso())
                        .await
                        .unwrap();
                }
                "delete-agent" => {
                    dest.delete_agent_session(&ws, &agent).await.unwrap();
                }
                "archive" => {
                    sqlx::query("UPDATE workspace SET archived=1 WHERE id=?")
                        .bind(ws.as_str())
                        .execute(dest.write_pool())
                        .await
                        .unwrap();
                }
                "delete-workspace" => {
                    dest.delete_workspace(&ws).await.unwrap();
                }
                _ => unreachable!(),
            }
            // Reimport into an existing workspace fails atomically. A deleted
            // workspace can be restored, but its old delivery cannot revive.
            if lifecycle == "delete-workspace" {
                dest.transfer_import_rows(&exported).await.unwrap();
            } else {
                assert!(dest.transfer_import_rows(&exported).await.is_err());
            }
            dest.close().await;
            let reopened = Store::open(&dest_tmp.path).await.unwrap();
            assert!(
                reopened
                    .script_monitors(&ws, None)
                    .await
                    .unwrap()
                    .is_empty(),
                "{phase}/{lifecycle}"
            );
            assert!(
                reopened
                    .script_notification_jobs()
                    .await
                    .unwrap()
                    .is_empty(),
                "{phase}/{lifecycle}"
            );
            assert!(
                reopened.load_all_agent_queues().await.unwrap().is_empty(),
                "{phase}/{lifecycle}"
            );
            assert!(
                !reopened
                    .accept_script_notification(&agent, "delivery")
                    .await
                    .unwrap(),
                "{phase}/{lifecycle}"
            );
        }
    }
}

#[tokio::test]
async fn completed_cleanup_receipt_never_replays_or_exports_pending_work() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let (ws, agent) = seed(&store, "completed-cleanup").await;
    let row = monitor(&ws, &agent, "cleanup");
    store.insert_script_monitor(&row).await.unwrap();
    store.settle_script_monitor(&triggered(&row)).await.unwrap();
    let queued = queue(&row);
    store.admit_script_notification(&queued).await.unwrap();
    store
        .accept_script_notification(&agent, "cleanup")
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER fail_cleanup BEFORE DELETE ON agent_queue_admission BEGIN SELECT RAISE(ABORT,'cleanup failure'); END").execute(store.write_pool()).await.unwrap();
    assert!(store
        .complete_script_notification(&agent, "cleanup")
        .await
        .is_err());
    let phase: (String, String) = sqlx::query_as("SELECT phase,payload FROM agent_queue_admission")
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    assert_eq!(phase, ("completed".into(), "{}".into()));
    let stale: i64 = sqlx::query_scalar("SELECT count(*) FROM agent_queue")
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    assert_eq!(
        stale, 1,
        "failed cleanup retains the stale queue branch too"
    );
    assert!(store.load_all_agent_queues().await.unwrap().is_empty());
    assert!(!store
        .script_monitor_pending_for_agent(&agent)
        .await
        .unwrap());
    assert!(store
        .script_notification_delivery_owners()
        .await
        .unwrap()
        .is_empty());
    assert!(!store.script_monitor_wake_allowed("cleanup").await.unwrap());
    assert!(!store
        .script_monitor_wake_allowed_for_agent("cleanup", &agent, &ws)
        .await
        .unwrap());
    assert!(!store
        .accept_script_notification(&agent, "cleanup")
        .await
        .unwrap());
    assert!(store
        .append_agent_message_with_provenance(
            &agent,
            &queued.id,
            "user",
            &json!([]),
            Some(&metadata(&row)),
            &now_iso(),
            crate::UsageMessageOrigin::Excluded
        )
        .await
        .is_err());
    let exported = store.transfer_export_rows(&ws).await.unwrap();
    for table in [
        "script_monitor",
        "script_notification_job",
        "agent_queue_admission",
        "agent_queue",
    ] {
        assert!(
            exported
                .iter()
                .find(|(name, _)| name == table)
                .unwrap()
                .1
                .is_empty(),
            "completed cleanup is not transferable work: {table}"
        );
    }
    let target = TempDb::new();
    let imported = Store::open(&target.path).await.unwrap();
    imported.transfer_import_rows(&exported).await.unwrap();
    assert!(imported.load_all_agent_queues().await.unwrap().is_empty());
    store.replace_agent_queue(&agent, &[queued]).await.unwrap();
    assert!(store.load_all_agent_queues().await.unwrap().is_empty());
    store.close().await;
    let reopened = Store::open(&tmp.path).await.unwrap();
    assert!(reopened.load_all_agent_queues().await.unwrap().is_empty());
    sqlx::query("DROP TRIGGER fail_cleanup")
        .execute(reopened.write_pool())
        .await
        .unwrap();
    reopened
        .cleanup_completed_script_notifications()
        .await
        .unwrap();
    let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM agent_queue_admission")
        .fetch_one(reopened.read_pool())
        .await
        .unwrap();
    assert_eq!(remaining, 0);
}
