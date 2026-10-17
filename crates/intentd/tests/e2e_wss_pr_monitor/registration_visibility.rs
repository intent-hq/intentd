//! An explicit re-registration repairs a pre-target-column monitor for clients.
use super::*;

#[intent_test_macros::daemon_test]
async fn reregister_legacy_monitor_is_visible_and_delivers_updates_over_wss() {
    let fx = boot().await;
    let first = fx
        .services
        .pr_monitor_register(&fx.ws_id, &fx.agent_id, "o", "r", 42)
        .await
        .unwrap()
        .0;
    sqlx::query(
        "UPDATE pr_monitor SET target_provider=NULL, target_instance_base_url=NULL,
         target_project_path=NULL, target_kind=NULL, target_provenance='unresolved',
         target_unresolved_reason='missing-provenance' WHERE monitor_id=?",
    )
    .bind(&first.monitor_id.0)
    .execute(fx.services.store().write_pool())
    .await
    .unwrap();

    let mut rpc = connect(fx.port, fx.cfg.clone()).await;
    let params = json!({"workspaceId":fx.ws_id});
    assert_eq!(
        wss_rpc(&mut rpc, 1, "prMonitor.list", params.clone()).await["monitors"],
        json!([])
    );
    let mut sub = connect(fx.port, fx.cfg.clone()).await;
    wss_rpc(
        &mut sub,
        2,
        "events.subscribe",
        json!({"workspaceId":fx.ws_id,
        "eventTypes":["prMonitor:registered", "prMonitor:completed"]}),
    )
    .await;

    let second = fx
        .services
        .pr_monitor_register(&fx.ws_id, &fx.agent_id, "o", "r", 42)
        .await
        .unwrap()
        .0;
    assert_eq!(second.monitor_id, first.monitor_id);
    let registered = next_event(&mut sub, "prMonitor:registered").await;
    assert_eq!(registered["data"]["monitorId"], first.monitor_id.0);
    let listed = wss_rpc(&mut rpc, 3, "prMonitor.list", params.clone()).await;
    assert_eq!(listed["monitors"].as_array().unwrap().len(), 1, "{listed}");
    assert_eq!(listed["monitors"][0]["monitorId"], first.monitor_id.0);
    assert_eq!(listed["monitors"][0]["agentId"], fx.agent_id.0);

    fx.forge.edit(|s| s.merged = true);
    fx.services.poll_pr_monitors().await;
    let completed = next_event(&mut sub, "prMonitor:completed").await;
    assert_eq!(completed["data"]["monitorId"], first.monitor_id.0);
    let listed = wss_rpc(&mut rpc, 4, "prMonitor.list", params).await;
    assert_eq!(listed["monitors"][0]["state"], "completed");
    assert!(owner_messages(&fx).await.contains("merged"));
}
