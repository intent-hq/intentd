//! `agent:idle` opportunistic PR refresh: the idle worker reuses the
//! automatic admission clock, so it never adds forge calls inside the
//! shared 60 s gate, during a rate-limit pause, or for a burst.
use super::*;

fn age(svc: &Services, ws: &WorkspaceId, secs: u64) {
    svc.age_automatic_pr_refresh(ws, secs);
}

fn get_pr_calls(forge: &StubForge) -> usize {
    forge.seen_get_pr.lock().unwrap().len()
}

async fn publish_idle(bus: &crate::EventBus, ws: &WorkspaceId, agent: &str) {
    bus.publish(&intent_store::NewEvent {
        workspace_id: ws.clone(),
        timestamp: now_iso(),
        event_type: intent_core::events::AGENT_IDLE.to_string(),
        actor: intent_core::EventActor {
            actor_type: intent_core::ActorType::System,
            id: Some("test".into()),
            name: None,
            email: None,
            model: None,
            metadata: None,
        },
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: serde_json::json!({ "agentId": agent, "finishReason": "end_turn" }),
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn idle_refresh_links_a_newly_opened_pr_when_the_last_attempt_is_stale() {
    let forge = Arc::new(StubForge {
        discover: true,
        ..Default::default()
    });
    let (_t, svc, ws) = refresh_setup_shared(forge.clone(), "feature", None).await;
    svc.refresh_prs_on_agent_idle(&ws).await;
    assert_eq!(
        svc.store().get_workspace(&ws).await.unwrap().pr_number,
        Some(42)
    );
    let linked = svc
        .store()
        .events_by_type(&ws, "pr:linked", 10)
        .await
        .unwrap();
    assert_eq!(linked.len(), 1, "pr:linked without waiting for the sweep");
}

#[tokio::test]
async fn idle_refresh_inside_the_shared_gate_makes_no_forge_call() {
    let forge = Arc::new(StubForge::default());
    let (_t, svc, ws) = refresh_setup_shared(forge.clone(), "feature", Some(42)).await;
    let svc = svc.with_pr_monitor_poll_seconds(60);
    // The 60 s sweep (or any automatic command) just checked this workspace.
    svc.refresh_all_workspace_prs(0).await;
    assert_eq!(get_pr_calls(&forge), 1);
    svc.refresh_prs_on_agent_idle(&ws).await;
    assert_eq!(get_pr_calls(&forge), 1, "fresh: no forge call");
    age(&svc, &ws, 59);
    svc.refresh_prs_on_agent_idle(&ws).await;
    assert_eq!(get_pr_calls(&forge), 1, "59 s old: still fresh");
    age(&svc, &ws, 1);
    svc.refresh_prs_on_agent_idle(&ws).await;
    assert_eq!(get_pr_calls(&forge), 2, "60 s old: one refresh");
    // The idle attempt resets the shared clock for the sweep too.
    svc.refresh_all_workspace_prs(1).await;
    assert_eq!(get_pr_calls(&forge), 2);
}

#[tokio::test]
async fn idle_gate_uses_the_configured_active_interval_not_the_idle_tier() {
    let forge = Arc::new(StubForge::default());
    let (_t, svc, ws) = refresh_setup_shared(forge.clone(), "feature", Some(42)).await;
    // A day-old content clock would put the sweep on its 900 s tier; the
    // idle gate stays at max(60 s, active interval).
    sqlx::query("UPDATE workspace SET created_at = ?, last_content_activity = ? WHERE id = ?")
        .bind("2020-01-01T00:00:00Z")
        .bind("2020-01-01T00:00:00Z")
        .bind(&ws.0)
        .execute(svc.store().write_pool())
        .await
        .unwrap();
    let svc = svc.with_pr_monitor_poll_seconds(120);
    svc.refresh_prs_on_agent_idle(&ws).await;
    assert_eq!(get_pr_calls(&forge), 1);
    age(&svc, &ws, 119);
    svc.refresh_prs_on_agent_idle(&ws).await;
    assert_eq!(get_pr_calls(&forge), 1, "inside the 120 s active interval");
    age(&svc, &ws, 1);
    svc.refresh_prs_on_agent_idle(&ws).await;
    assert_eq!(get_pr_calls(&forge), 2, "not held to the 900 s idle tier");
}

#[tokio::test]
async fn idle_refresh_makes_no_forge_call_during_a_rate_limit_pause() {
    let forge = Arc::new(StubForge::default());
    let (_t, svc, ws) = refresh_setup_shared(forge.clone(), "feature", Some(42)).await;
    svc.sweep_rate_limit
        .pause_for(std::time::Duration::from_secs(3600), false);
    svc.refresh_prs_on_agent_idle(&ws).await;
    assert_eq!(get_pr_calls(&forge), 0);
    svc.sweep_rate_limit.lift();
    svc.refresh_prs_on_agent_idle(&ws).await;
    assert_eq!(get_pr_calls(&forge), 1, "a pause reserves no attempt");
}

#[tokio::test]
async fn idle_refresh_bursts_are_coalesced() {
    let park = Arc::new(GetPrPark::default());
    let forge = Arc::new(StubForge {
        get_pr_park: Some(park.clone()),
        ..Default::default()
    });
    let (_t, svc, ws) = refresh_setup_shared(forge.clone(), "feature", Some(42)).await;
    let first = {
        let svc = svc.clone();
        let ws = ws.clone();
        tokio::spawn(async move { svc.refresh_prs_on_agent_idle(&ws).await })
    };
    park.entered.notified().await;
    // Several agents in the workspace finish while the refresh is running.
    for _ in 0..4 {
        svc.refresh_prs_on_agent_idle(&ws).await;
    }
    park.release.notify_one();
    first.await.unwrap();
    for _ in 0..4 {
        svc.refresh_prs_on_agent_idle(&ws).await;
    }
    assert_eq!(get_pr_calls(&forge), 1);
    svc.shutdown_store_writers().await;
}

#[tokio::test]
async fn idle_loop_refreshes_once_for_a_burst_of_idle_events() {
    let forge = Arc::new(StubForge {
        discover: true,
        ..Default::default()
    });
    let (_t, svc, ws) = refresh_setup_shared(forge.clone(), "feature", None).await;
    let bus = svc.event_bus.clone().expect("bus");
    let task = svc.spawn_idle_pr_refresh_loop();
    tokio::time::timeout(
        intentd_test_support::test_timeout(std::time::Duration::from_secs(5)),
        async {
            while bus.subscriber_count() == 0 {
                tokio::task::yield_now().await;
            }
        },
    )
    .await
    .unwrap();
    for agent in ["agent-a", "agent-b", "agent-c"] {
        publish_idle(&bus, &ws, agent).await;
    }
    tokio::time::timeout(
        intentd_test_support::test_timeout(std::time::Duration::from_secs(10)),
        async {
            while svc
                .store()
                .get_workspace(&ws)
                .await
                .unwrap()
                .pr_number
                .is_none()
            {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        },
    )
    .await
    .expect("idle refresh linked the PR");
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(
        forge.seen_pr_queries.lock().unwrap().len(),
        1,
        "one discovery for the whole burst"
    );
    assert_eq!(
        svc.store()
            .events_by_type(&ws, "pr:linked", 10)
            .await
            .unwrap()
            .len(),
        1
    );
    task.abort();
}

#[tokio::test]
async fn idle_refresh_covers_git_roots_with_the_same_gate() {
    let primary = SweepRepo::init("main", None);
    let sub = SweepRepo::init("feature", Some("https://github.com/o/r.git"));
    sweep_commit(&sub.dir);
    let (_db, svc, ws) = sweep_setup(&primary.dir).await;
    let mut root = sweep_root(&ws.id, &sub.dir, Some(("o", "r")));
    root.pr_number = Some(42);
    svc.store().upsert_workspace_git_root(&root).await.unwrap();
    let forge = Arc::new(StubForge {
        discover: true,
        ..Default::default()
    });
    let svc = svc
        .with_source_control(forge.clone())
        .with_pr_monitor_poll_seconds(60);
    svc.refresh_prs_on_agent_idle(&ws.id).await;
    assert_eq!(get_pr_calls(&forge), 1, "the root's PR is refreshed");
    svc.refresh_prs_on_agent_idle(&ws.id).await;
    assert_eq!(get_pr_calls(&forge), 1, "fresh root: no forge call");
    age(&svc, &ws.id, 60);
    svc.refresh_prs_on_agent_idle(&ws.id).await;
    assert_eq!(get_pr_calls(&forge), 2, "stale root refreshed again");
    age(&svc, &ws.id, 60);
    svc.sweep_rate_limit
        .pause_for(std::time::Duration::from_secs(3600), false);
    svc.refresh_prs_on_agent_idle(&ws.id).await;
    assert_eq!(get_pr_calls(&forge), 2, "paused: no root forge call");
}

#[tokio::test]
async fn idle_refresh_skips_unconfigured_source_control_and_archived_workspaces() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws_id = WorkspaceId::new();
    let mut ws = workspace(&ws_id);
    ws.branch = "feature".into();
    ws.repository_owner = Some("o".into());
    ws.repository_name = Some("r".into());
    store.insert_workspace(&ws).await.unwrap();
    let unconfigured = Services::new(store.clone());
    unconfigured.refresh_prs_on_agent_idle(&ws_id).await;
    assert_eq!(
        unconfigured
            .store()
            .get_workspace(&ws_id)
            .await
            .unwrap()
            .pr_number,
        None
    );

    let forge = Arc::new(StubForge {
        discover: true,
        ..Default::default()
    });
    let svc = Services::new(store).with_source_control(forge.clone());
    sqlx::query("UPDATE workspace SET archived = 1 WHERE id = ?")
        .bind(&ws_id.0)
        .execute(svc.store().write_pool())
        .await
        .unwrap();
    assert!(svc.store().get_workspace(&ws_id).await.unwrap().archived);
    svc.refresh_prs_on_agent_idle(&ws_id).await;
    svc.refresh_prs_on_agent_idle(&WorkspaceId::new()).await;
    assert_eq!(get_pr_calls(&forge), 0);
    assert_eq!(forge.seen_pr_queries.lock().unwrap().len(), 0);
}

/// Shutdown drains an in-flight idle refresh, including the git-root tail
/// that runs after the workspace refresh: the worker's jobs are owned, not
/// detached.
#[tokio::test]
async fn shutdown_drains_an_in_flight_idle_refresh() {
    let primary = SweepRepo::init("main", None);
    let sub = SweepRepo::init("feature", Some("https://github.com/o/r.git"));
    sweep_commit(&sub.dir);
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws_id = WorkspaceId::new();
    let mut ws = workspace(&ws_id);
    ws.worktree_path = Some(primary.dir.to_string_lossy().into_owned());
    ws.branch = "feature".into();
    ws.repository_owner = Some("o".into());
    ws.repository_name = Some("r".into());
    ws.pr_number = Some(42);
    ws.updated_at = now_iso();
    store.insert_workspace(&ws).await.unwrap();
    let mut root = sweep_root(&ws_id, &sub.dir, Some(("o", "r")));
    root.pr_number = Some(43);
    store.upsert_workspace_git_root(&root).await.unwrap();
    let park = Arc::new(GetPrPark::default());
    let forge = Arc::new(StubForge {
        get_pr_park: Some(park.clone()),
        ..Default::default()
    });
    let bus = crate::EventBus::new(store.clone());
    let svc = Services::new(store)
        .with_event_bus(bus.clone())
        .with_source_control(forge.clone());
    let task = svc.spawn_idle_pr_refresh_loop();
    tokio::time::timeout(
        intentd_test_support::test_timeout(std::time::Duration::from_secs(5)),
        async {
            while bus.subscriber_count() == 0 {
                tokio::task::yield_now().await;
            }
        },
    )
    .await
    .unwrap();
    publish_idle(&bus, &ws_id, "agent-a").await;
    tokio::time::timeout(
        intentd_test_support::test_timeout(std::time::Duration::from_secs(10)),
        park.entered.notified(),
    )
    .await
    .expect("the workspace refresh reached the forge");
    let shutdown = {
        let svc = svc.clone();
        tokio::spawn(async move { svc.shutdown_store_writers().await })
    };
    assert!(
        tokio::time::timeout(
            intentd_test_support::test_timeout(std::time::Duration::from_millis(200)),
            async {
                while !shutdown.is_finished() {
                    tokio::task::yield_now().await;
                }
            },
        )
        .await
        .is_err(),
        "shutdown waits for the parked refresh"
    );
    park.release.notify_one();
    tokio::time::timeout(
        intentd_test_support::test_timeout(std::time::Duration::from_secs(10)),
        shutdown,
    )
    .await
    .expect("shutdown finished")
    .unwrap();
    assert_eq!(
        *forge.seen_get_pr.lock().unwrap(),
        vec![42, 43],
        "the root tail ran before shutdown returned"
    );
    task.await.unwrap();
}

async fn refresh_setup_shared(
    forge: Arc<StubForge>,
    branch: &str,
    pr_number: Option<u64>,
) -> (TempDb, Services, WorkspaceId) {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws_id = WorkspaceId::new();
    let mut ws = workspace(&ws_id);
    ws.branch = branch.to_string();
    ws.repository_owner = Some("o".into());
    ws.repository_name = Some("r".into());
    ws.pr_number = pr_number;
    ws.updated_at = now_iso();
    store.insert_workspace(&ws).await.expect("ws");
    let bus = crate::EventBus::new(store.clone());
    let svc = Services::new(store)
        .with_event_bus(bus)
        .with_source_control(forge);
    (tmp, svc, ws_id)
}
