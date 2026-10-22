//! Recovery must classify stored provenance before interpreting old snapshots.
use super::*;

async fn import_without_provenance(service: &Services, id: &PrMonitorId) {
    sqlx::query(
        "UPDATE pr_monitor SET target_provider=NULL, target_instance_base_url=NULL,
         target_project_path=NULL, target_kind=NULL, target_provenance='unresolved',
         target_unresolved_reason='missing-provenance' WHERE monitor_id=?",
    )
    .bind(&id.0)
    .execute(service.store.write_pool())
    .await
    .unwrap();
}

#[tokio::test]
async fn unresolved_monitor_summaries_withhold_output_without_losing_internal_waits() {
    let (_db, _root, service, forge, workspace, owner) = setup().await;
    let monitor = register(&service, &workspace, &owner).await;
    import_without_provenance(&service, &monitor.monitor_id).await;
    let before = service
        .store
        .get_pr_monitor(&monitor.monitor_id)
        .await
        .unwrap();
    let reads = forge.fetches();
    assert_eq!(service.active_pr_monitors_for_agent(&owner).await.len(), 1);
    assert!(service.active_pr_monitor_labels(&owner).await.is_empty());
    assert!(service
        .active_pr_monitors_by_agent(&workspace)
        .await
        .is_empty());
    let mut idle = json!({"agentId":owner});
    assert!(service
        .annotate_waiting_on_pr_monitors(&owner, &mut idle)
        .await
        .is_empty());
    assert!(idle.get("waitingOnPrMonitors").is_none());
    let mut replay = json!({"waitingOnPrMonitors":[waiting_on_pr_monitors_entry(&before)]});
    assert!(service
        .annotate_waiting_on_pr_monitors(&owner, &mut replay)
        .await
        .is_empty());
    assert!(replay.get("waitingOnPrMonitors").is_none());
    assert!(service
        .legacy_pr_monitor_wait_entries(&owner)
        .await
        .is_empty());
    assert!(service
        .agent_get_op(owner.clone(), Some(workspace.clone()))
        .await
        .unwrap()
        .waiting_on_pr_monitors
        .is_empty());
    assert_eq!(service.active_pr_monitors_for_agent(&owner).await.len(), 1);
    assert_eq!(
        service
            .store
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap(),
        before
    );
    assert_eq!(forge.fetches(), reads);
}

#[tokio::test]
async fn unresolved_owner_gone_recovery_cancels_without_legacy_event_disclosure() {
    let (_db, _root, service, forge, workspace, owner) = setup().await;
    let monitor = register(&service, &workspace, &owner).await;
    import_without_provenance(&service, &monitor.monitor_id).await;
    let reads = forge.fetches();
    let bus = service.event_bus.as_ref().unwrap();
    let mut events = bus.subscribe(crate::SubscriptionFilter {
        event_types: vec![
            PR_MONITOR_CANCELLED.into(),
            intent_core::events::CLIENT_UPDATED.into(),
        ],
        ..Default::default()
    });
    service
        .store
        .set_agent_session_status(
            &workspace,
            &owner,
            AgentStatus::Deleted,
            false,
            &now_iso(),
            None,
        )
        .await
        .unwrap();
    assert_eq!(service.rehydrate_pr_monitors().await.unwrap(), 0);
    assert_eq!(
        service
            .store
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap()
            .state,
        PrMonitorState::Cancelled
    );
    bus.publish(&NewEvent {
        workspace_id: workspace,
        timestamp: now_iso(),
        event_type: intent_core::events::CLIENT_UPDATED.into(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: json!({"monitorRecoveryTestBarrier": true}),
    })
    .await
    .unwrap();
    let batch = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(5)),
        events.recv(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        batch.iter().all(
            |event| event.event_type == intent_core::events::CLIENT_UPDATED
                && event.data["monitorRecoveryTestBarrier"] == true
        ),
        "qualified cancellation must not use the unqualified global bus: {batch:?}"
    );
    assert_eq!(forge.fetches(), reads);
}

#[tokio::test]
async fn restart_preserves_unresolved_imported_pending_bytes_without_legacy_emission() {
    let (db, root, service, forge, workspace, owner) = setup().await;
    let (monitor, _) = service
        .pr_monitor_register(&workspace, &owner, "o", "r", 42)
        .await
        .unwrap();
    // Importing an otherwise plausible old GitHub snapshot supplies no writer
    // provenance. Preserve the raw pending encoding, not just its parsed value.
    let pending = "[ \"Original imported change\" , \"PR is behind the base branch\" ]";
    sqlx::query(
        "UPDATE pr_monitor SET target_provider=NULL, target_instance_base_url=NULL,
         target_project_path=NULL, target_kind=NULL, target_provenance='unresolved',
         target_unresolved_reason='missing-provenance', pending_changes=?,
         pending_since='2026-09-20T12:00:01Z', last_change_at='2026-09-20T12:00:02Z'
         WHERE monitor_id=?",
    )
    .bind(pending)
    .bind(&monitor.monitor_id.0)
    .execute(service.store.write_pool())
    .await
    .unwrap();
    let before = service
        .store
        .get_pr_monitor(&monitor.monitor_id)
        .await
        .unwrap();
    let fetches = forge.fetches();
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
    restarted.poll_pr_monitors().await;
    assert_eq!(
        forge.fetches(),
        fetches,
        "unknown provenance must never read a forge"
    );
    assert_eq!(
        store.get_pr_monitor(&monitor.monitor_id).await.unwrap(),
        before
    );
    let raw: String =
        sqlx::query_scalar("SELECT pending_changes FROM pr_monitor WHERE monitor_id=?")
            .bind(&monitor.monitor_id.0)
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    assert_eq!(raw, pending);
    assert!(store
        .get_agent_session(&owner)
        .await
        .unwrap()
        .messages
        .is_empty());
}
