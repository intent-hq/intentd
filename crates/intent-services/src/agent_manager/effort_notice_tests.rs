use super::thought_level_tests::{option, setup, setup_with_response};
use super::*;
use intent_core::AgentMessage;

fn resolved() -> ResolvedSpawn {
    ResolvedSpawn {
        provider: *intent_providers::provider_config("auggie"),
        model: None,
        reasoning_effort: None,
        cwd: std::env::temp_dir(),
        provider_binary: None,
        extra_env: std::collections::BTreeMap::default(),
        npx_fallback_binary: None,
        npx_fallback_package: None,
        unsloth_endpoint: None,
    }
}

async fn turn(mgr: &AgentManager, id: &AgentId, conn: &Connection, effort: Option<&str>) {
    mgr.apply_thought_level(conn, id, "sid-1", effort).await;
    mgr.maybe_persist_effort_change_notice(id, &WorkspaceId::from("ws-1"), &resolved())
        .await;
}

async fn notices(mgr: &AgentManager, id: &AgentId) -> Vec<AgentMessage> {
    mgr.services
        .store
        .get_agent_messages(id, None)
        .await
        .unwrap()
}

#[tokio::test]
async fn effort_notice_tracks_confirmed_turns_and_preserves_auto_and_none() {
    let mut selector = option("medium");
    selector.values.extend(["none".into(), "ultra".into()]);
    let (mgr, id, conn, _, _db, _task) = setup(Some(selector)).await;
    turn(&mgr, &id, &conn, None).await;
    assert!(notices(&mgr, &id).await.is_empty(), "silent first baseline");

    turn(&mgr, &id, &conn, Some("HIGH")).await;
    turn(&mgr, &id, &conn, Some("High")).await;
    turn(&mgr, &id, &conn, Some("none")).await;
    turn(&mgr, &id, &conn, Some("ultra")).await;
    turn(&mgr, &id, &conn, None).await;
    turn(&mgr, &id, &conn, Some("  ")).await;
    let rows = notices(&mgr, &id).await;
    let changes: Vec<_> = rows.iter().map(|m| m.metadata.clone().unwrap()).collect();
    assert_eq!(
        changes,
        vec![
            json!({"type": "effort_changed", "from": null, "to": "high"}),
            json!({"type": "effort_changed", "from": "high", "to": "none"}),
            json!({"type": "effort_changed", "from": "none", "to": "ultra"}),
            json!({"type": "effort_changed", "from": "ultra", "to": null}),
        ]
    );
    assert!(rows.iter().all(|m| m.role == "system"));
    assert_eq!(
        rows[0].content[0]["text"],
        "Effort changed from Auto to high."
    );

    let xml = crate::history_xml::format_history_as_xml(&rows, 10_000, 1_000);
    assert!(!xml.contains("Effort changed"), "not provider history");
}

#[tokio::test]
async fn effort_notice_never_confirms_rejected_or_unsupported_effort() {
    for response in [
        json!({"error": {"code": -32602, "message": "rejected effort"}}),
        json!({"result": {"configOptions": [{"id": "effort", "currentValue": "medium"}]}}),
    ] {
        let (mgr, id, conn, _, _db, _task) =
            setup_with_response(Some(option("medium")), response).await;
        turn(&mgr, &id, &conn, Some("medium")).await;
        turn(&mgr, &id, &conn, Some("high")).await;
        turn(&mgr, &id, &conn, Some("high")).await;
        turn(&mgr, &id, &conn, Some("unknown")).await;
        assert!(notices(&mgr, &id).await.is_empty());
        let baseline = mgr
            .services
            .store
            .get_agent_session_last_turn_effort(&WorkspaceId::from("ws-1"), &id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(baseline.effort.as_deref(), Some("medium"));
    }
    let (mgr, id, conn, calls, _db, _task) = setup(None).await;
    turn(&mgr, &id, &conn, Some("high")).await;
    turn(&mgr, &id, &conn, Some("low")).await;
    assert!(notices(&mgr, &id).await.is_empty());
    assert!(calls.lock().unwrap().is_empty());
    assert!(mgr
        .services
        .store
        .get_agent_session_last_turn_effort(&WorkspaceId::from("ws-1"), &id)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn effort_notice_retries_after_application_failure_without_advancing_baseline() {
    let (mgr, id, conn, _, _db, responder) = setup_with_response(
        Some(option("medium")),
        json!({"error": {"code": -32602, "message": "rejected effort"}}),
    )
    .await;
    turn(&mgr, &id, &conn, Some("medium")).await;
    turn(&mgr, &id, &conn, Some("high")).await;
    assert!(notices(&mgr, &id).await.is_empty());
    // Replace the failed adapter with an accepting session. This is also a
    // respawn: the committed baseline must live in the store, not the handle.
    responder.abort();
    let (read, write) = super::dead_child_respawn_tests::install_fake_handle(&mgr, &id, None);
    let (_task, _) =
        super::thought_level_tests::spawn_recording_responder(read, write, json!({"result": {}}));
    let conn = {
        let mut handles = mgr.handles.lock().unwrap();
        let handle = handles.get_mut(&id).unwrap();
        handle.thought_level = Some(option("medium"));
        handle.connection.clone()
    };
    turn(&mgr, &id, &conn, Some("high")).await;
    let rows = notices(&mgr, &id).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].metadata,
        Some(json!({"type":"effort_changed","from":"medium","to":"high"}))
    );
}

#[tokio::test]
async fn effort_notice_persistence_failure_does_not_block_application() {
    let (mgr, id, conn, calls, _db, _task) = setup(Some(option("medium"))).await;
    turn(&mgr, &id, &conn, None).await;
    mgr.services.store.close().await;
    turn(&mgr, &id, &conn, Some("high")).await;
    assert_eq!(calls.lock().unwrap()[0]["value"], "high");
}

#[tokio::test]
async fn effort_notice_uses_saved_default_only_for_matching_resumed_identity() {
    let (mgr, id, conn, calls, _db, _task) = setup(Some(option("medium"))).await;
    turn(&mgr, &id, &conn, Some("high")).await;
    let ws = WorkspaceId::from("ws-1");
    let default = mgr.resumed_effort_default(&id, &ws, "auggie").await;
    assert_eq!(default, "medium");
    assert!(mgr
        .resumed_effort_default(&id, &ws, "other")
        .await
        .is_empty());
    let record = mgr.services.store.get_agent_session(&id).await.unwrap();
    let opened = AcpSessionOpened {
        session_id: "sid-1".into(),
        modes: None,
        thought_level: Some(option("high")),
    };
    mgr.install_and_apply_thought_level(&conn, &record, &opened, None, None, Some(&default))
        .await;
    mgr.maybe_persist_effort_change_notice(&id, &ws, &resolved())
        .await;
    assert_eq!(calls.lock().unwrap().last().unwrap()["value"], "medium");
    assert_eq!(
        notices(&mgr, &id).await[0].metadata,
        Some(json!({"type":"effort_changed","from":"high","to":null}))
    );
    mgr.handles
        .lock()
        .unwrap()
        .get_mut(&id)
        .unwrap()
        .spawned_model = Some("other-model".into());
    assert!(mgr
        .resumed_effort_default(&id, &ws, "auggie")
        .await
        .is_empty());
}

#[tokio::test]
async fn effort_notice_does_not_claim_auto_without_a_known_resumed_default() {
    let (mgr, id, conn, calls, _db, _task) = setup(Some(option("high"))).await;
    turn(&mgr, &id, &conn, Some("high")).await;
    let record = mgr.services.store.get_agent_session(&id).await.unwrap();
    let mut opened = AcpSessionOpened {
        session_id: "sid-1".into(),
        modes: None,
        thought_level: Some(option("high")),
    };
    mgr.install_and_apply_thought_level(&conn, &record, &opened, None, None, Some(""))
        .await;
    mgr.maybe_persist_effort_change_notice(&id, &record.workspace_id, &resolved())
        .await;
    assert!(notices(&mgr, &id).await.is_empty());
    assert!(calls.lock().unwrap().is_empty());
    // A provider's explicit default sentinel does let us restore Auto.
    opened
        .thought_level
        .as_mut()
        .unwrap()
        .values
        .push("default".into());
    mgr.install_and_apply_thought_level(&conn, &record, &opened, None, None, Some(""))
        .await;
    mgr.maybe_persist_effort_change_notice(&id, &record.workspace_id, &resolved())
        .await;
    assert_eq!(calls.lock().unwrap()[0]["value"], "default");
    assert_eq!(
        notices(&mgr, &id).await[0].metadata,
        Some(json!({"type":"effort_changed","from":"high","to":null}))
    );
}

#[tokio::test]
async fn effort_notice_does_not_advance_on_failed_spawn() {
    use super::dead_child_respawn_tests::{mock_env, seed_mock_session};
    let script_dir = crate::tests::test_tempdir("effort-failed-spawn-");
    let script = script_dir.path().join("fail.mjs");
    std::fs::write(&script, "process.exit(1);\n").unwrap();
    let _env = mock_env(script.to_str().unwrap());
    let (mgr, _, _db) = super::role_reminder_tests::manager_with(None, None).await;
    let ws = WorkspaceId::from("ws-1");
    let id = AgentId::from("failed-effort-start");
    seed_mock_session(&mgr, &id, "sid-before").await;
    let baseline = intent_store::AgentTurnEffort {
        effort: Some("medium".into()),
        default_value: "low".into(),
        provider: "mock".into(),
        model: None,
    };
    mgr.services
        .store
        .set_agent_session_last_turn_effort(&ws, &id, &baseline)
        .await
        .unwrap();
    let mut record = mgr.services.store.get_agent_session(&id).await.unwrap();
    record.reasoning_effort = Some("high".into());
    mgr.services
        .store
        .update_agent_session(&ws, &record)
        .await
        .unwrap();
    assert!(mgr.ensure_started(&id, &ws).await.is_err());
    assert!(notices(&mgr, &id).await.is_empty());
    assert_eq!(
        mgr.services
            .store
            .get_agent_session_last_turn_effort(&ws, &id)
            .await
            .unwrap(),
        Some(baseline)
    );
    mgr.shutdown().await;
}
