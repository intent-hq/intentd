use super::*;
use crate::pr_ops;

pub(super) async fn owner_caller(svc: &Services) -> intent_core::Caller {
    intent_core::Caller::Wire {
        principal_id: svc.store().get_primary_principal().await.unwrap().id,
        host_role: intent_core::HostRole::Owner,
    }
}

async fn setup(forge: Arc<StubForge>, idle_secs: i64) -> (TempDb, Services, WorkspaceId) {
    let (db, svc, id) = setup_with_shared(forge, true).await;
    let mut ws = svc.store().get_workspace(&id).await.unwrap();
    ws.branch = "feature".into();
    svc.store()
        .update_workspace_with_branch(&ws, Some("feature"))
        .await
        .unwrap();
    assert_eq!(
        svc.store().get_workspace(&id).await.unwrap().branch,
        "feature"
    );
    let clock = (time::OffsetDateTime::now_utc() - time::Duration::seconds(idle_secs))
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap();
    sqlx::query("UPDATE workspace SET created_at = ?, last_content_activity = ? WHERE id = ?")
        .bind(&clock)
        .bind(&clock)
        .bind(&id.0)
        .execute(svc.store().write_pool())
        .await
        .unwrap();
    (db, svc.with_pr_monitor_poll_seconds(60), id)
}

#[tokio::test]
async fn automatic_commands_obey_all_tiers_and_keep_cached_linkage() {
    for (idle_secs, interval) in [(0, 60), (900, 120), (3600, 300), (21600, 600), (86400, 900)] {
        let forge = Arc::new(StubForge::default());
        let (_db, svc, ws) = setup(forge.clone(), idle_secs).await;
        let first = intent_core::with_caller(
            owner_caller(&svc).await,
            svc.pr_refresh_automatic(ws.clone()),
        )
        .await
        .unwrap();
        assert_eq!(first["prNumber"], 42);
        assert_eq!(forge.seen_get_pr.lock().unwrap().len(), 1);
        for _ in 0..3 {
            let cached = intent_core::with_caller(
                owner_caller(&svc).await,
                svc.pr_refresh_automatic(ws.clone()),
            )
            .await
            .unwrap();
            assert_eq!(cached["outcome"], "skipped");
            assert_eq!(cached["pullRequests"], first["pullRequests"]);
            assert_eq!(cached["prNumber"], first["prNumber"]);
        }
        svc.age_automatic_pr_refresh(&ws, interval - 1);
        intent_core::with_caller(
            owner_caller(&svc).await,
            svc.pr_refresh_automatic(ws.clone()),
        )
        .await
        .unwrap();
        assert_eq!(
            forge.seen_get_pr.lock().unwrap().len(),
            1,
            "before {interval}s"
        );
        svc.age_automatic_pr_refresh(&ws, 1);
        intent_core::with_caller(owner_caller(&svc).await, svc.pr_refresh_automatic(ws))
            .await
            .unwrap();
        assert_eq!(forge.seen_get_pr.lock().unwrap().len(), 2, "at {interval}s");
    }
}

#[tokio::test]
async fn automatic_failures_do_not_retry_early_but_manual_refresh_bypasses() {
    let forge = Arc::new(StubForge::with_get_pr_error(|| {
        ScError::Api("unavailable".into())
    }));
    let (_db, svc, ws) = setup(forge.clone(), 86400).await;
    assert!(intent_core::with_caller(
        owner_caller(&svc).await,
        svc.pr_refresh_automatic(ws.clone())
    )
    .await
    .is_err());
    let cached = intent_core::with_caller(
        owner_caller(&svc).await,
        svc.pr_refresh_automatic(ws.clone()),
    )
    .await
    .unwrap();
    assert_eq!(cached["outcome"], "skipped");
    assert_eq!(cached["prNumber"], 42);
    svc.refresh_all_workspace_prs(0).await;
    assert_eq!(forge.seen_get_pr.lock().unwrap().len(), 1);
    assert!(
        intent_core::with_caller(owner_caller(&svc).await, svc.pr_refresh(ws.clone()))
            .await
            .is_err()
    );
    assert_eq!(forge.seen_get_pr.lock().unwrap().len(), 2);
    svc.age_automatic_pr_refresh(&ws, 900);
    assert!(
        intent_core::with_caller(owner_caller(&svc).await, svc.pr_refresh_automatic(ws))
            .await
            .is_err()
    );
    assert_eq!(forge.seen_get_pr.lock().unwrap().len(), 3);
}

#[tokio::test]
async fn automatic_sweep_and_commands_share_admission_in_both_directions() {
    let forge = Arc::new(StubForge::default());
    let (_db, svc, ws) = setup(forge.clone(), 86400).await;
    intent_core::with_caller(
        owner_caller(&svc).await,
        svc.pr_refresh_automatic(ws.clone()),
    )
    .await
    .unwrap();
    svc.refresh_all_workspace_prs(0).await;
    assert_eq!(forge.seen_get_pr.lock().unwrap().len(), 1);
    svc.age_automatic_pr_refresh(&ws, 900);
    svc.refresh_all_workspace_prs(1).await;
    intent_core::with_caller(owner_caller(&svc).await, svc.pr_refresh_automatic(ws))
        .await
        .unwrap();
    assert_eq!(forge.seen_get_pr.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn automatic_activity_resumes_and_idle_workspaces_do_not_slow_active_ones() {
    let forge = Arc::new(StubForge::default());
    let (_db, svc, ws) = setup(forge.clone(), 86400).await;
    intent_core::with_caller(
        owner_caller(&svc).await,
        svc.pr_refresh_automatic(ws.clone()),
    )
    .await
    .unwrap();
    let id2 = WorkspaceId::new();
    let mut other = svc.store().get_workspace(&ws).await.unwrap();
    other.id = id2.clone();
    other.created_at = now_iso();
    other.last_content_activity = Some(now_iso());
    svc.store().insert_workspace(&other).await.unwrap();
    intent_core::with_caller(
        owner_caller(&svc).await,
        svc.pr_refresh_automatic(id2.clone()),
    )
    .await
    .unwrap();
    svc.age_automatic_pr_refresh(&id2, 60);
    intent_core::with_caller(owner_caller(&svc).await, svc.pr_refresh_automatic(id2))
        .await
        .unwrap();
    intent_core::with_caller(
        owner_caller(&svc).await,
        svc.pr_refresh_automatic(ws.clone()),
    )
    .await
    .unwrap();
    assert_eq!(forge.seen_get_pr.lock().unwrap().len(), 3);
    svc.age_automatic_pr_refresh(&ws, 60);
    sqlx::query("UPDATE workspace SET last_content_activity = ? WHERE id = ?")
        .bind(now_iso())
        .bind(&ws.0)
        .execute(svc.store().write_pool())
        .await
        .unwrap();
    intent_core::with_caller(owner_caller(&svc).await, svc.pr_refresh_automatic(ws))
        .await
        .unwrap();
    assert_eq!(forge.seen_get_pr.lock().unwrap().len(), 4);
}

#[tokio::test]
async fn automatic_concurrent_windows_and_reconnect_share_running_attempt() {
    let park = Arc::new(GetPrPark::default());
    let forge = Arc::new(StubForge {
        get_pr_park: Some(park.clone()),
        ..Default::default()
    });
    let (_db, svc, ws) = setup(forge.clone(), 86400).await;
    let first = {
        let svc = svc.clone();
        let ws = ws.clone();
        tokio::spawn(async move {
            intent_core::with_caller(owner_caller(&svc).await, svc.pr_refresh_automatic(ws)).await
        })
    };
    park.entered.notified().await;
    // Even after the interval passes, a slow running attempt owns admission.
    svc.age_automatic_pr_refresh(&ws, 900);
    assert_eq!(
        intent_core::with_caller(
            owner_caller(&svc).await,
            svc.pr_refresh_automatic(ws.clone())
        )
        .await
        .unwrap()["outcome"],
        "skipped"
    );
    first.abort(); // a dropped connection cannot release the retained writer
    assert_eq!(
        intent_core::with_caller(owner_caller(&svc).await, svc.pr_refresh_automatic(ws))
            .await
            .unwrap()["outcome"],
        "skipped"
    );
    assert_eq!(forge.seen_get_pr.lock().unwrap().len(), 1);
    park.release.notify_one();
    svc.shutdown_store_writers().await;
}

#[tokio::test]
async fn automatic_calls_honor_global_pause_and_restart_has_no_old_reservations() {
    let forge = Arc::new(StubForge::default());
    let (_db, svc, ws) = setup(forge.clone(), 86400).await;
    svc.sweep_rate_limit
        .pause_for(std::time::Duration::from_secs(3600), false);
    assert_eq!(
        intent_core::with_caller(
            owner_caller(&svc).await,
            svc.pr_refresh_automatic(ws.clone())
        )
        .await
        .unwrap()["outcome"],
        "skipped"
    );
    assert!(forge.seen_get_pr.lock().unwrap().is_empty());
    svc.sweep_rate_limit.lift();
    intent_core::with_caller(
        owner_caller(&svc).await,
        svc.pr_refresh_automatic(ws.clone()),
    )
    .await
    .unwrap();
    let restarted = Services::new(svc.store().clone()).with_source_control(forge.clone());
    intent_core::with_caller(owner_caller(&svc).await, restarted.pr_refresh_automatic(ws))
        .await
        .unwrap();
    assert_eq!(forge.seen_get_pr.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn automatic_root_backoff_keeps_local_maintenance_and_new_roots_available() {
    let primary = SweepRepo::init("main", None);
    let first = SweepRepo::init("feature", Some("https://github.com/o/r.git"));
    let second = SweepRepo::init("feature", Some("https://github.com/o/r.git"));
    sweep_commit(&first.dir);
    let (_db, svc, mut ws) = sweep_setup(&primary.dir).await;
    let now = time::OffsetDateTime::now_utc();
    ws.updated_at = (now - time::Duration::hours(1))
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap();
    ws.last_activity = None;
    let mut root = sweep_root(&ws.id, &first.dir, Some(("o", "r")));
    root.pr_number = Some(42);
    svc.store().upsert_workspace_git_root(&root).await.unwrap();
    let forge = Arc::new(StubForge {
        discover: true,
        ..Default::default()
    });
    let sc: Arc<dyn SourceControl> = forge.clone();
    svc.sweep_workspace_git_roots(&ws, Some(&sc), 900, true)
        .await;
    assert_eq!(forge.seen_get_pr.lock().unwrap().len(), 1);
    // Local root bookkeeping remains available while its forge read is idle.
    sqlx::query("UPDATE workspace_git_root SET registered_commit_sha = NULL WHERE id = ?")
        .bind(&root.id.0)
        .execute(svc.store().write_pool())
        .await
        .unwrap();
    for tick in 1..15 {
        svc.age_automatic_pr_refresh(&ws.id, 60);
        svc.sweep_workspace_git_roots(
            &ws,
            Some(&sc),
            900,
            pr_ops::local_root_maintenance_due(&ws, tick, now),
        )
        .await;
        assert!(svc
            .store()
            .get_workspace_git_root(&root.id)
            .await
            .unwrap()
            .registered_commit_sha
            .is_none());
        assert_eq!(forge.seen_get_pr.lock().unwrap().len(), 1);
    }
    svc.sweep_workspace_git_roots(&ws, Some(&sc), 900, true)
        .await;
    assert!(svc
        .store()
        .get_workspace_git_root(&root.id)
        .await
        .unwrap()
        .registered_commit_sha
        .is_some());
    assert_eq!(forge.seen_get_pr.lock().unwrap().len(), 1);
    let mut new_root = sweep_root(&ws.id, &second.dir, Some(("o", "r")));
    new_root.pr_number = Some(43);
    svc.store()
        .upsert_workspace_git_root(&new_root)
        .await
        .unwrap();
    svc.sweep_workspace_git_roots(&ws, Some(&sc), 900, false)
        .await;
    assert_eq!(
        forge.seen_get_pr.lock().unwrap().len(),
        2,
        "new root has independent admission"
    );
    svc.age_automatic_pr_refresh(&ws.id, 900);
    svc.sweep_workspace_git_roots(&ws, Some(&sc), 900, true)
        .await;
    assert_eq!(forge.seen_get_pr.lock().unwrap().len(), 4);
}

#[tokio::test]
async fn automatic_local_maintenance_preserves_scan_cadence_independently_of_forge() {
    let (_db, svc, id) = setup(Arc::new(StubForge::default()), 86400).await;
    let mut ws = svc.store().get_workspace(&id).await.unwrap();
    let now = time::OffsetDateTime::now_utc();
    ws.updated_at = now_iso();
    ws.last_activity = None;
    for tick in 0..60 {
        assert_eq!(
            pr_ops::local_root_maintenance_due(&ws, tick, now),
            tick % 3 == 0
        );
    }
    ws.updated_at = (now - time::Duration::hours(1))
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap();
    for tick in 0..60 {
        assert_eq!(
            pr_ops::local_root_maintenance_due(&ws, tick, now),
            tick % 30 == 0
        );
    }
    ws.last_activity = Some(now_iso());
    assert!(pr_ops::local_root_maintenance_due(&ws, 3, now));
    assert!(!pr_ops::local_root_maintenance_due(&ws, 1, now));
}
