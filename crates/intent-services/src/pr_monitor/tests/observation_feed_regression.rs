//! Regression: a monitored PR merged, the workspace PR refresh saw it and
//! the home pill read "Merged", but the monitor card kept "open; queued to
//! merge" — the monitor only learned on its own idle cadence (up to 15 min
//! for a settled, idle workspace). Every fresh PR read now feeds the
//! monitor sweep ([`ObservationFeed`]), so the next due-sweep polls the PR
//! at once.

use super::*;

async fn link_workspace_to_pr(svc: &Services, forge: &StubForge, ws: &WorkspaceId) {
    let pr = forge.get_pr(&RepoRef::new("o", "r"), 42).await.unwrap();
    let info = pr_ops::build_pr_info(&pr);
    let row = svc.store().get_workspace(ws).await.unwrap();
    svc.store()
        .update_workspace_with_branch(&row, Some(&pr.source_branch))
        .await
        .unwrap();
    let mut row = svc.store().get_workspace(ws).await.unwrap();
    row.pr_number = Some(42);
    row.pr_url = Some(info.url.clone());
    row.pr_status = Some(info.status);
    row.active_pull_request = Some(info.clone());
    row.pull_requests = Some(vec![info]);
    svc.store().update_workspace_pr_linkage(&row).await.unwrap();
}

async fn monitor_state(svc: &Services, monitor: &PrMonitor) -> PrMonitorState {
    svc.store()
        .get_pr_monitor(&monitor.monitor_id)
        .await
        .unwrap()
        .state
}

#[tokio::test]
async fn a_merge_the_workspace_refresh_sees_completes_the_monitor_on_the_next_due_sweep() {
    let (_db, _root, svc, forge, ws, owner) = setup().await;
    link_workspace_to_pr(&svc, &forge, &ws).await;
    let monitor = register(&svc, &ws, &owner).await;

    forge.edit(|s| s.pr_state = PrState::Merged);
    // The registration just polled: on cadence alone the PR is not due.
    svc.poll_due_pr_monitors().await;
    assert_eq!(monitor_state(&svc, &monitor).await, PrMonitorState::Active);

    // The workspace refresh reads the merge and moves the pill.
    svc.refresh_workspace_pr(&ws).await.unwrap();
    let row = svc.store().get_workspace(&ws).await.unwrap();
    assert_eq!(
        row.pull_requests.unwrap()[0].status,
        PullRequestStatus::Merged
    );

    // The monitor follows on the very next due-sweep, not its idle cadence.
    svc.poll_due_pr_monitors().await;
    assert_eq!(
        monitor_state(&svc, &monitor).await,
        PrMonitorState::Completed
    );
    assert!(owner_messages(&svc, &owner).await.contains("merged"));
}

#[tokio::test]
async fn a_merge_an_on_demand_read_sees_completes_the_monitor_on_the_next_due_sweep() {
    let (_db, _root, svc, forge, ws, owner) = setup().await;
    let monitor = register(&svc, &ws, &owner).await;

    forge.edit(|s| s.pr_state = PrState::Merged);
    svc.backdate_pr_cache(svc.pr_cache_max_age() + Duration::from_secs(1));
    let (entry, fetched) = svc.serve_pr(&RepoRef::new("o", "r"), 42).await.unwrap();
    assert!(fetched);
    assert_eq!(entry.pr.state, PrState::Merged);

    svc.poll_due_pr_monitors().await;
    assert_eq!(
        monitor_state(&svc, &monitor).await,
        PrMonitorState::Completed
    );
}

#[tokio::test]
async fn an_unchanged_read_does_not_pull_the_monitor_ahead_of_its_cadence() {
    let (_db, _root, svc, forge, ws, owner) = setup().await;
    let monitor = register(&svc, &ws, &owner).await;
    let polled = svc
        .store()
        .get_pr_monitor(&monitor.monitor_id)
        .await
        .unwrap()
        .last_polled_at;

    // A comment moves `updatedAt` only: not a lifecycle change.
    forge.edit(|s| s.conversation_comments = 3);
    svc.backdate_pr_cache(svc.pr_cache_max_age() + Duration::from_secs(1));
    svc.serve_pr(&RepoRef::new("o", "r"), 42).await.unwrap();
    let before = forge.fetches();
    svc.poll_due_pr_monitors().await;

    assert_eq!(forge.fetches(), before, "no extra monitor poll");
    assert_eq!(
        svc.store()
            .get_pr_monitor(&monitor.monitor_id)
            .await
            .unwrap()
            .last_polled_at,
        polled
    );
}

#[tokio::test]
async fn a_failed_poll_keeps_the_observation_for_the_next_tick() {
    let (_db, _root, svc, forge, ws, owner) = setup().await;
    let monitor = register(&svc, &ws, &owner).await;

    forge.edit(|s| s.pr_state = PrState::Merged);
    svc.backdate_pr_cache(svc.pr_cache_max_age() + Duration::from_secs(1));
    svc.serve_pr(&RepoRef::new("o", "r"), 42).await.unwrap();

    forge.edit_quiet(|s| s.fail_get_pr = true);
    svc.poll_due_pr_monitors().await;
    assert_eq!(monitor_state(&svc, &monitor).await, PrMonitorState::Active);

    forge.edit_quiet(|s| s.fail_get_pr = false);
    svc.poll_due_pr_monitors().await;
    assert_eq!(
        monitor_state(&svc, &monitor).await,
        PrMonitorState::Completed
    );
}

/// Review finding on intentd#2891: a check-now polls only its own monitor,
/// so it must not advance the feed baseline the sibling monitors on the same
/// PR (other workspaces) still rely on. A later fresh read must still pull
/// the sibling ahead of its cadence.
#[tokio::test]
async fn a_check_now_on_one_monitor_does_not_hide_the_merge_from_its_siblings() {
    let (_db, _root, svc, forge, ws, owner) = setup().await;
    let ws2 = WorkspaceId::new();
    svc.store()
        .insert_workspace(&workspace(&ws2))
        .await
        .unwrap();
    let owner2 = AgentId::from("agent-prmon-2");
    svc.store()
        .insert_agent_session(&agent(&ws2, "agent-prmon-2"))
        .await
        .unwrap();
    let checked = register(&svc, &ws, &owner).await;
    let sibling = register(&svc, &ws2, &owner2).await;
    // Both baselines come from a sweep poll of the open PR.
    svc.poll_pr_monitors().await;

    forge.edit(|s| s.pr_state = PrState::Merged);
    svc.pr_monitor_check_and_flush(&ws, &checked.monitor_id)
        .await
        .unwrap();
    assert_eq!(
        monitor_state(&svc, &checked).await,
        PrMonitorState::Completed
    );
    assert_eq!(monitor_state(&svc, &sibling).await, PrMonitorState::Active);

    // Another read sees the merge; the sibling follows on the next sweep.
    svc.backdate_pr_cache(svc.pr_cache_max_age() + Duration::from_secs(1));
    svc.serve_pr(&RepoRef::new("o", "r"), 42).await.unwrap();
    svc.poll_due_pr_monitors().await;
    assert_eq!(
        monitor_state(&svc, &sibling).await,
        PrMonitorState::Completed
    );
}
