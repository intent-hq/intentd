//! Regression for intent-hq/intent#7190: successful re-registration must
//! repair a pre-target-column row rather than return an invisible monitor.
use super::*;

async fn legacy_row(service: &Services, monitor: &PrMonitor) {
    sqlx::query(
        "UPDATE pr_monitor SET target_provider=NULL, target_instance_base_url=NULL,
         target_project_path=NULL, target_kind=NULL, target_provenance='unresolved',
         target_unresolved_reason='missing-provenance' WHERE monitor_id=?",
    )
    .bind(&monitor.monitor_id.0)
    .execute(service.store.write_pool())
    .await
    .unwrap();
}

async fn assert_visible(service: &Services, ws: &WorkspaceId, owner: &AgentId, id: &PrMonitorId) {
    for agent in [Some(owner), None] {
        let listed = service.pr_monitor_list_op(ws, agent).await.unwrap();
        assert_eq!(listed["monitors"].as_array().unwrap().len(), 1, "{listed}");
        assert_eq!(listed["monitors"][0]["monitorId"], id.0);
        assert_eq!(listed["monitors"][0]["agentId"], owner.0);
    }
    let snapshot = service
        .agent_snapshot_op(ws.clone(), owner.clone())
        .await
        .unwrap();
    assert_eq!(snapshot["prMonitors"], json!(["o/r#42"]));
}

#[tokio::test]
async fn reregister_legacy_monitor_restores_visibility_and_polling_after_restart() {
    let (db, root, service, forge, ws, owner) = setup().await;
    let first = register(&service, &ws, &owner).await;
    legacy_row(&service, &first).await;
    assert!(service.active_pr_monitor_labels(&owner).await.is_empty());

    let result = service
        .pr_monitor_start_op(&ws, &owner, 42, Some("o/r".into()))
        .await
        .unwrap();
    assert_eq!(result["ok"], true);
    assert_eq!(result["monitor"]["monitorId"], first.monitor_id.0);
    assert_visible(&service, &ws, &owner, &first.monitor_id).await;
    drop(service);

    let store = Store::open(&db.path).await.unwrap();
    let restarted = Services::new_with_file_secrets(
        store.clone(),
        intent_core::FileSecretStore::with_path(root.path().join("secrets.json")),
    )
    .with_event_bus(EventBus::new(store.clone()))
    .with_workspaces_root(root.path().to_path_buf())
    .with_source_control(Arc::new(forge.clone()));
    assert_eq!(restarted.rehydrate_pr_monitors().await.unwrap(), 1);
    assert_visible(&restarted, &ws, &owner, &first.monitor_id).await;
    let before = forge.fetches();
    forge.edit(|s| s.pr_state = PrState::Merged);
    restarted.poll_pr_monitors().await;
    assert!(forge.fetches() > before);
    assert_eq!(
        store.get_pr_monitor(&first.monitor_id).await.unwrap().state,
        PrMonitorState::Completed
    );
    assert!(
        !store
            .get_agent_session(&owner)
            .await
            .unwrap()
            .messages
            .is_empty(),
        "merged PR wakes its owner"
    );
}

#[tokio::test]
async fn legacy_monitor_repair_clears_old_pending_data_and_is_idempotent() {
    let (_db, _root, service, forge, ws, owner) = setup().await;
    let first = register(&service, &ws, &owner).await;
    forge.edit(|s| s.conversation_comments = 2);
    service.poll_pr_monitors().await;
    assert!(!service
        .store
        .get_pr_monitor(&first.monitor_id)
        .await
        .unwrap()
        .pending_changes
        .is_empty());
    legacy_row(&service, &first).await;
    for repo in ["O/R", "o/r"] {
        let result = service
            .pr_monitor_start_op(&ws, &owner, 42, Some(repo.into()))
            .await
            .unwrap();
        assert_eq!(result["monitor"]["monitorId"], first.monitor_id.0);
        assert_visible(&service, &ws, &owner, &first.monitor_id).await;
        let row = service
            .store
            .get_pr_monitor(&first.monitor_id)
            .await
            .unwrap();
        assert!(row.pending_changes.is_empty());
        assert!(row.pending_since.is_none());
        assert!(row.last_change_at.is_none());
        assert_eq!(row.last_snapshot, row.baseline_snapshot);
        assert_eq!(row.created_at, first.created_at);
    }
    assert!(service
        .store
        .get_agent_session(&owner)
        .await
        .unwrap()
        .messages
        .is_empty());
}

#[tokio::test]
async fn legacy_monitor_repair_preserves_live_ownership_and_repairs_an_adopted_row() {
    let (_db, _root, service, forge, ws, owner) = setup().await;
    let first = register(&service, &ws, &owner).await;
    legacy_row(&service, &first).await;
    let other = second_agent(&service, &ws, "agent-adopter").await;
    let reads = forge.fetches();
    assert!(matches!(
        service
            .pr_monitor_try_register(&ws, &other, "o", "r", 42)
            .await
            .unwrap(),
        PrMonitorRegistration::Refused(_)
    ));
    assert_eq!(forge.fetches(), reads);
    kill_owner(&service, &ws, &owner, OwnerDeath::Error).await;
    let result = service
        .pr_monitor_start_op(&ws, &other, 42, Some("o/r".into()))
        .await
        .unwrap();
    assert_eq!(result["adoptedFrom"], owner.0);
    assert_visible(&service, &ws, &other, &first.monitor_id).await;
    assert!(service.active_pr_monitor_labels(&owner).await.is_empty());
}

#[tokio::test]
async fn legacy_monitor_repair_handles_failed_and_deferred_baselines() {
    let (_db, _root, service, forge, ws, owner) = setup().await;
    let first = register(&service, &ws, &owner).await;
    legacy_row(&service, &first).await;
    forge.edit(|s| s.fail_get_pr = true);
    assert!(service
        .pr_monitor_register(&ws, &owner, "o", "r", 42)
        .await
        .is_err());
    assert_eq!(
        service
            .store
            .get_pr_monitor(&first.monitor_id)
            .await
            .unwrap(),
        first
    );
    assert!(service.active_pr_monitor_labels(&owner).await.is_empty());

    forge.edit(|s| {
        s.fail_get_pr = false;
        s.rate_limit_get_pr = true;
    });
    let result = service
        .pr_monitor_start_op(&ws, &owner, 42, Some("o/r".into()))
        .await
        .unwrap();
    assert_eq!(result["ok"], true);
    assert!(result["requirements"].is_null());
    assert!(result["pausedUntil"].is_string());
    assert_visible(&service, &ws, &owner, &first.monitor_id).await;
    let row = service
        .store
        .get_pr_monitor(&first.monitor_id)
        .await
        .unwrap();
    assert!(row.last_snapshot.is_none());
    assert!(row.baseline_snapshot.is_none());
    assert!(row.last_polled_at.is_none());
    assert!(row.last_error.is_some());

    elapse_pause(&service).await;
    forge.edit(|s| s.rate_limit_get_pr = false);
    service.poll_pr_monitors().await;
    assert!(service
        .store
        .get_pr_monitor(&first.monitor_id)
        .await
        .unwrap()
        .last_snapshot
        .is_some());
}

#[tokio::test]
async fn legacy_registration_refuses_to_reclassify_imported_or_qualified_monitors() {
    for provenance in [
        "unverified-import",
        "captured-request",
        "validated-transfer",
    ] {
        let (_db, _root, service, _forge, ws, owner) = setup().await;
        let first = register(&service, &ws, &owner).await;
        if provenance == "unverified-import" {
            legacy_row(&service, &first).await;
            sqlx::query("UPDATE pr_monitor SET target_unresolved_reason=? WHERE monitor_id=?")
                .bind(provenance)
                .bind(&first.monitor_id.0)
                .execute(service.store.write_pool())
                .await
                .unwrap();
        } else {
            sqlx::query("UPDATE pr_monitor SET target_provenance=? WHERE monitor_id=?")
                .bind(provenance)
                .bind(&first.monitor_id.0)
                .execute(service.store.write_pool())
                .await
                .unwrap();
        }
        let result = service.pr_monitor_register(&ws, &owner, "o", "r", 42).await;
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("incompatible target evidence"));
        assert_eq!(
            service
                .store
                .get_pr_monitor(&first.monitor_id)
                .await
                .unwrap(),
            first
        );
        assert!(service.active_pr_monitor_labels(&owner).await.is_empty());
    }
}
