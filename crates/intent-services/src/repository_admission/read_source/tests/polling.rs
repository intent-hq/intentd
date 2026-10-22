//! Scheduler probes use the real captured MCP registration and provider HTTP.
use super::*;

fn mr_revision(provider: &ReadServer, second: u8) {
    provider.replies.bodies.lock().unwrap().insert(MR.into(), json!({
        "iid":4,"web_url":"https://gitlab.test/forge/group/project/-/merge_requests/4",
        "title":"actual review","state":"opened","draft":false,"sha":"head-a",
        "source_branch":"feature","target_branch":"main","source_project_id":42,"target_project_id":42,
        "created_at":"2026-09-27T00:00:00Z","updated_at":format!("2026-09-27T00:00:{second:02}Z")
    }));
}

#[intent_test_macros::daemon_test]
async fn qualified_scheduler_quota_recovery_preserves_baseline_until_observed_change() {
    let provider = ReadServer::new().await;
    mr_revision(&provider, 0);
    let fixture = ActualRead::new(&provider).await;
    routing_options(&fixture);
    let registered = routing_json(&run(&fixture.server(), "return await ws.mr.monitor(4);").await);
    let id = intent_core::PrMonitorId(registered["monitor"]["monitorId"].as_str().unwrap().into());
    let service = &fixture.auth.service;
    let before = service.store.get_pr_monitor(&id).await.unwrap();
    // The provider's exhausted window has expired already. The scheduler still
    // observes its conservative fallback pause, driven here by explicit time.
    provider.replies.quota_headers.lock().unwrap().insert(
        MR.into(),
        "RateLimit-Remaining: 0\r\nRateLimit-Reset: 0\r\nRetry-After: 0\r\n".into(),
    );
    provider.status(MR, 429);
    let start = time::OffsetDateTime::now_utc();
    let poll = |at| {
        intent_core::with_caller(
            intent_core::Caller::Daemon,
            service.sweep_qualified_pr_monitors_at(false, at),
        )
    };
    poll(start).await;
    let paused = service.store.get_pr_monitor(&id).await.unwrap();
    let envelope: Value = serde_json::from_str(paused.last_snapshot.as_deref().unwrap()).unwrap();
    let deadline = intent_core::parse_iso(envelope["retryAt"].as_str().unwrap()).unwrap();
    assert_eq!(deadline, start + time::Duration::seconds(60));
    assert_eq!(paused.baseline_snapshot, before.baseline_snapshot);
    let calls = provider.count();
    provider.status(MR, 200);
    provider.replies.quota_headers.lock().unwrap().remove(MR);
    poll(deadline - time::Duration::seconds(1)).await;
    assert_eq!(provider.count(), calls, "no busy polling before deadline");
    poll(deadline).await;
    assert!(provider.count() > calls);
    let recovered = service.store.get_pr_monitor(&id).await.unwrap();
    assert!(recovered.last_error.is_none());
    assert!(
        recovered.pending_changes.is_empty(),
        "recovery alone cannot invent news"
    );
    assert_eq!(recovered.baseline_snapshot, before.baseline_snapshot);
    mr_revision(&provider, 1);
    provider.replies.bodies.lock().unwrap().get_mut(MR).unwrap()["sha"] = json!("head-b");
    poll(deadline + time::Duration::seconds(1)).await;
    let changed = service.store.get_pr_monitor(&id).await.unwrap();
    assert_eq!(
        changed.pending_changes,
        vec!["new commits pushed (head is now head-b)"]
    );
}

#[intent_test_macros::daemon_test]
async fn qualified_scheduler_clock_coalesces_changes_and_preserves_pending_without_delivery() {
    let provider = ReadServer::new().await;
    mr_revision(&provider, 0);
    let fixture = ActualRead::new(&provider).await;
    routing_options(&fixture);
    let registered = routing_json(&run(&fixture.server(), "return await ws.mr.monitor(4);").await);
    let id = intent_core::PrMonitorId(registered["monitor"]["monitorId"].as_str().unwrap().into());
    let service = &fixture.auth.service;
    let started = time::OffsetDateTime::now_utc();
    let poll = |now| {
        intent_core::with_caller(
            intent_core::Caller::Daemon,
            service.sweep_qualified_pr_monitors_at(false, now),
        )
    };
    mr_revision(&provider, 1);
    provider.replies.bodies.lock().unwrap().get_mut(MR).unwrap()["sha"] = json!("head-b");
    poll(started).await;
    let first = service.store.get_pr_monitor(&id).await.unwrap();
    assert_eq!(
        first.pending_changes,
        vec!["new commits pushed (head is now head-b)"]
    );
    mr_revision(&provider, 2);
    {
        let mut bodies = provider.replies.bodies.lock().unwrap();
        let mr = bodies.get_mut(MR).unwrap();
        mr["sha"] = json!("head-c");
        mr["draft"] = json!(true);
    }
    let changed = started + time::Duration::seconds(1);
    poll(changed).await;
    let consolidated = service.store.get_pr_monitor(&id).await.unwrap();
    assert_eq!(
        consolidated.pending_changes,
        vec![
            "state: open → draft",
            "marked as draft",
            "new commits pushed (head is now head-c)",
            "merge blocked: draft MRs cannot be merged"
        ]
    );
    assert_eq!(first.pending_since, consolidated.pending_since);
    let quiet =
        time::Duration::seconds(i64::try_from(service.pr_monitor_debounce().as_secs()).unwrap());
    assert!(!service.pr_monitor_debounce_elapsed_at(
        &consolidated,
        changed + quiet - time::Duration::seconds(1)
    ));
    assert!(service.pr_monitor_debounce_elapsed_at(&consolidated, changed + quiet));
    poll(changed + quiet).await;
    let retained = service.store.get_pr_monitor(&id).await.unwrap();
    assert_eq!(
        retained.pending_changes, consolidated.pending_changes,
        "no supported runtime delivery was made"
    );
    assert_eq!(retained.last_change_at, consolidated.last_change_at);
    assert_eq!(retained.baseline_snapshot, first.baseline_snapshot);
    mr_revision(&provider, 3);
    poll(changed + quiet + time::Duration::seconds(1)).await;
    let reverted = service.store.get_pr_monitor(&id).await.unwrap();
    assert!(reverted.pending_changes.is_empty());
    assert!(reverted.pending_since.is_none());
}

#[intent_test_macros::daemon_test]
async fn qualified_scheduler_restricted_approval_read_preserves_observed_pending_change() {
    let provider = ReadServer::new().await;
    mr_revision(&provider, 0);
    let fixture = ActualRead::new(&provider).await;
    routing_options(&fixture);
    let approvals = format!("{MR}/approvals");
    provider.replies.bodies.lock().unwrap().insert(
        approvals.clone(),
        json!({"approvals_required":1,"approvals_left":1,"approved_by":[]}),
    );
    let registered = routing_json(&run(&fixture.server(), "return await ws.mr.monitor(4);").await);
    assert_eq!(registered["ok"], true);
    let id = intent_core::PrMonitorId(registered["monitor"]["monitorId"].as_str().unwrap().into());
    mr_revision(&provider, 1);
    provider.replies.bodies.lock().unwrap().insert(approvals.clone(), json!({"approvals_required":1,"approvals_left":0,"approved_by":[{"user":{"id":7,"username":"reviewer","name":"Reviewer"}}]}));
    fixture.auth.service.poll_pr_monitors().await;
    let observed = fixture
        .auth
        .service
        .store
        .get_pr_monitor(&id)
        .await
        .unwrap();
    assert!(
        observed
            .pending_changes
            .iter()
            .any(|line| line == "new approval (0 → 1 approving)"),
        "pending={:?}; saved={:?}; error={:?}; calls={:?}",
        observed.pending_changes,
        observed.last_snapshot,
        observed.last_error,
        provider.replies.calls.lock().unwrap()
    );
    mr_revision(&provider, 2);
    provider.status(&approvals, 403);
    fixture.auth.service.poll_pr_monitors().await;
    let restricted = fixture
        .auth
        .service
        .store
        .get_pr_monitor(&id)
        .await
        .unwrap();
    let public: Value = serde_json::from_str(restricted.last_snapshot.as_ref().unwrap()).unwrap();
    assert!(public["qualifiedSnapshot"]["requirements"]["approvals"]["have"].is_null());
    assert!(
        restricted
            .pending_changes
            .iter()
            .any(|line| line == "new approval (0 → 1 approving)"),
        "an unreadable refresh cannot erase observed, undelivered news: {:?}",
        restricted.pending_changes
    );
    assert_eq!(observed.pending_since, restricted.pending_since);
}

#[intent_test_macros::daemon_test]
async fn qualified_scheduler_clock_preserves_adaptive_cadence() {
    let provider = ReadServer::new().await;
    let fixture = ActualRead::new(&provider).await;
    routing_options(&fixture);
    let registered = routing_json(&run(&fixture.server(), "return await ws.mr.monitor(4);").await);
    assert_eq!(registered["ok"], true);
    let service = &fixture.auth.service;
    let now = time::OffsetDateTime::now_utc();
    intent_core::with_caller(
        intent_core::Caller::Daemon,
        service.sweep_qualified_pr_monitors_at(false, now),
    )
    .await;
    let initial = provider.count();
    let intervals = service
        .workspace_automatic_check_intervals(std::slice::from_ref(&fixture.git.workspace.id), now)
        .await
        .unwrap();
    let seconds = service
        .pr_monitor_poll_interval()
        .as_secs()
        .max(intervals[&fixture.git.workspace.id]);
    assert!(seconds > 0);
    intent_core::with_caller(
        intent_core::Caller::Daemon,
        service.sweep_qualified_pr_monitors_at(
            true,
            now + time::Duration::seconds(i64::try_from(seconds).unwrap() - 1),
        ),
    )
    .await;
    assert_eq!(
        provider.count(),
        initial,
        "not due before the configured/adaptive floor"
    );
    intent_core::with_caller(
        intent_core::Caller::Daemon,
        service.sweep_qualified_pr_monitors_at(
            true,
            now + time::Duration::seconds(i64::try_from(seconds).unwrap()),
        ),
    )
    .await;
    assert!(provider.count() > initial, "due exactly at the floor");
}

#[intent_test_macros::daemon_test]
async fn qualified_scheduler_quota_pause_is_shared_by_connection() {
    let provider = ReadServer::new().await;
    let fixture = ActualRead::new(&provider).await;
    routing_options(&fixture);
    let other = format!("{PROJECT}/merge_requests/5");
    provider.replies.bodies.lock().unwrap().insert(other.clone(), json!({
        "iid":5,"web_url":"https://gitlab.test/forge/group/project/-/merge_requests/5",
        "title":"another review","state":"opened","draft":false,"source_branch":"feature5","target_branch":"main",
        "source_project_id":42,"target_project_id":42,"created_at":"2026-09-27T00:00:00Z","updated_at":"2026-09-27T00:00:00Z"
    }));
    for number in [4, 5] {
        let registered = routing_json(
            &run(
                &fixture.server(),
                &format!("return await ws.mr.monitor({number});"),
            )
            .await,
        );
        assert_eq!(registered["ok"], true, "{registered}");
    }
    provider.status(MR, 429);
    provider.status(&other, 429);
    let before = provider.count();
    fixture.auth.service.poll_pr_monitors().await;
    assert_eq!(
        provider.count() - before,
        1,
        "the first quota rejection must pause every monitor on that connection"
    );
    let paused = provider.count();
    fixture.auth.service.poll_pr_monitors().await;
    assert_eq!(provider.count(), paused);
}

#[intent_test_macros::daemon_test]
async fn qualified_scheduler_revocation_during_http_discards_late_response() {
    let provider = ReadServer::new().await;
    let fixture = ActualRead::new(&provider).await;
    routing_options(&fixture);
    let registered = routing_json(&run(&fixture.server(), "return await ws.mr.monitor(4);").await);
    let id = intent_core::PrMonitorId(registered["monitor"]["monitorId"].as_str().unwrap().into());
    let before = fixture
        .auth
        .service
        .store
        .get_pr_monitor(&id)
        .await
        .unwrap();
    provider.pause(MR);
    let service = fixture.auth.service.clone();
    let poll = tokio::spawn(async move { service.poll_pr_monitors().await });
    provider.entered().await;
    fixture.owner.retirement().retire();
    provider.resume();
    timeout(BUDGET, poll).await.unwrap().unwrap();
    let after = fixture
        .auth
        .service
        .store
        .get_pr_monitor(&id)
        .await
        .unwrap();
    assert_eq!(before.last_snapshot, after.last_snapshot);
    assert_eq!(before.baseline_snapshot, after.baseline_snapshot);
    assert_eq!(before.last_polled_at, after.last_polled_at);
    assert!(after.pending_changes.is_empty());
}

#[intent_test_macros::daemon_test]
async fn qualified_scheduler_distinguishes_authentication_and_project_restriction() {
    for status in [401, 403] {
        let provider = ReadServer::new().await;
        let fixture = ActualRead::new(&provider).await;
        routing_options(&fixture);
        let registered =
            routing_json(&run(&fixture.server(), "return await ws.mr.monitor(4);").await);
        let id =
            intent_core::PrMonitorId(registered["monitor"]["monitorId"].as_str().unwrap().into());
        let before = fixture
            .auth
            .service
            .store
            .get_pr_monitor(&id)
            .await
            .unwrap();
        provider.status(MR, status);
        fixture.auth.service.poll_pr_monitors().await;
        assert_eq!(
            fixture
                .auth
                .service
                .gitlab_repository_settled_connection()
                .is_err(),
            status == 401
        );
        let after = fixture
            .auth
            .service
            .store
            .get_pr_monitor(&id)
            .await
            .unwrap();
        assert_eq!(before.baseline_snapshot, after.baseline_snapshot);
        assert!(after.pending_changes.is_empty());
        if status == 401 {
            assert_eq!(before.last_snapshot, after.last_snapshot);
            assert_eq!(before.last_polled_at, after.last_polled_at);
        } else {
            assert!(after.last_error.is_some());
        }
    }
}

#[intent_test_macros::daemon_test]
async fn qualified_scheduler_github_pause_cannot_mutate_gitlab_row() {
    let provider = ReadServer::new().await;
    let fixture = ActualRead::new(&provider).await;
    routing_options(&fixture);
    let registered = routing_json(&run(&fixture.server(), "return await ws.mr.monitor(4);").await);
    let id = intent_core::PrMonitorId(registered["monitor"]["monitorId"].as_str().unwrap().into());
    let before = fixture
        .auth
        .service
        .store
        .get_pr_monitor(&id)
        .await
        .unwrap();
    let affected = fixture
        .auth
        .service
        .store
        .annotate_active_pr_monitors_pause(
            "GitHub rate limit: polling paused until 2090-01-01T00:00:00Z",
        )
        .await
        .unwrap();
    assert_eq!(
        affected, 0,
        "GitHub bulk pause cannot cross provider authority"
    );
    let after = fixture
        .auth
        .service
        .store
        .get_pr_monitor(&id)
        .await
        .unwrap();
    assert_eq!(before.last_error, after.last_error);
}

#[intent_test_macros::daemon_test]
async fn qualified_scheduler_revoked_original_cannot_fetch_or_change_row() {
    let provider = ReadServer::new().await;
    let fixture = ActualRead::new(&provider).await;
    routing_options(&fixture);
    let registered = routing_json(&run(&fixture.server(), "return await ws.mr.monitor(4);").await);
    let id = intent_core::PrMonitorId(registered["monitor"]["monitorId"].as_str().unwrap().into());
    let before = fixture
        .auth
        .service
        .store
        .get_pr_monitor(&id)
        .await
        .unwrap();
    let calls = provider.count();
    fixture.owner.retirement().retire();
    fixture.auth.service.poll_pr_monitors().await;
    assert_eq!(provider.count(), calls);
    let after = fixture
        .auth
        .service
        .store
        .get_pr_monitor(&id)
        .await
        .unwrap();
    assert_eq!(before.last_snapshot, after.last_snapshot);
    assert_eq!(before.last_polled_at, after.last_polled_at);
}

#[intent_test_macros::daemon_test]
async fn qualified_scheduler_quota_pause_does_not_busy_poll_or_pause_github() {
    let provider = ReadServer::new().await;
    let fixture = ActualRead::new(&provider).await;
    routing_options(&fixture);
    let registered = routing_json(&run(&fixture.server(), "return await ws.mr.monitor(4);").await);
    let id = intent_core::PrMonitorId(registered["monitor"]["monitorId"].as_str().unwrap().into());
    provider.status(MR, 429);
    let before = provider.count();
    fixture.auth.service.poll_pr_monitors().await;
    assert!(provider.count() > before);
    let paused = provider.count();
    let row = fixture
        .auth
        .service
        .store
        .get_pr_monitor(&id)
        .await
        .unwrap();
    let saved: Value = serde_json::from_str(row.last_snapshot.as_ref().unwrap()).unwrap();
    assert!(saved["retryAt"].is_string(), "{saved}");
    assert!(fixture
        .auth
        .service
        .sweep_rate_limit_paused_until()
        .is_none());
    fixture.auth.service.poll_pr_monitors().await;
    assert_eq!(provider.count(), paused);
}

#[intent_test_macros::daemon_test]
async fn qualified_scheduler_bare_wake_metadata_cannot_authorize_private_delivery() {
    let provider = ReadServer::new().await;
    let fixture = ActualRead::new(&provider).await;
    routing_options(&fixture);
    let registered = routing_json(&run(&fixture.server(), "return await ws.mr.monitor(4);").await);
    assert_eq!(registered["ok"], true, "{registered}");
    let metadata = json!({
        "type":"pr_monitor_wake", "reason":"changed",
        "monitorId":registered["monitor"]["monitorId"],
        "repo":"group/project", "prNumber":4,
        "resource":registered["monitor"]["resource"]
    });
    let delivered = fixture
        .auth
        .service
        .deliver_wake_message(
            &fixture.git.workspace.id,
            &fixture.agent,
            "Private MR content must require its original delivery obligation",
            Some(&metadata),
        )
        .await;
    assert!(
        delivered.is_err(),
        "plain metadata must never authorize qualified delivery: {delivered:?}"
    );
}

#[intent_test_macros::daemon_test]
async fn qualified_scheduler_reads_pinned_target_without_github_configuration() {
    let provider = ReadServer::new().await;
    let fixture = ActualRead::new(&provider).await;
    routing_options(&fixture);
    let registered = routing_json(&run(&fixture.server(), "return await ws.mr.monitor(4);").await);
    assert_eq!(registered["ok"], true, "{registered}");
    let id = intent_core::PrMonitorId(registered["monitor"]["monitorId"].as_str().unwrap().into());
    let before = provider.count();
    fixture.auth.service.poll_pr_monitors().await;
    assert!(
        provider.count() > before,
        "qualified monitor must poll without a GitHub provider"
    );
    let row = fixture
        .auth
        .service
        .store
        .get_pr_monitor(&id)
        .await
        .unwrap();
    assert!(
        row.pending_changes.is_empty(),
        "unchanged provider state must not wake"
    );
    assert!(row.last_error.is_none(), "{:?}", row.last_error);
}

#[intent_test_macros::daemon_test]
async fn qualified_scheduler_preserves_registered_identity_after_remote_change() {
    let provider = ReadServer::new().await;
    let fixture = ActualRead::new(&provider).await;
    routing_options(&fixture);
    let registered = routing_json(&run(&fixture.server(), "return await ws.pr.monitor(4);").await);
    assert_eq!(registered["ok"], true, "{registered}");
    fixture.git.git(
        &fixture.git.path,
        &[
            "remote",
            "set-url",
            "origin",
            "https://github.com/unrelated/repository.git",
        ],
    );
    let before = provider.count();
    fixture.auth.service.poll_pr_monitors().await;
    assert!(
        provider.count() > before,
        "poll the captured target, never re-resolve the workspace remote"
    );
    assert!(provider
        .replies
        .calls
        .lock()
        .unwrap()
        .iter()
        .all(|path| path.starts_with(PROJECT)));
}
