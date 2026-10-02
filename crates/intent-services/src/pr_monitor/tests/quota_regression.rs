//! Configurable PR-monitor capacity uses the production registry and scheduler.

use super::*;

/// The supported file-backed setting, not the test-only override, admits
/// an orchestrator's inventory across repositories through the real service.
#[tokio::test]
async fn configured_monitor_quota_scales_one_owner_and_bounds_sweep_reads() {
    let (_db, root, svc, forge, ws, owner) = setup().await;
    let path = root.path().join("monitor-quota.toml");
    std::fs::write(&path, "[prMonitor]\nmaxPerAgent = 55\n").unwrap();
    let registry = Arc::new(crate::settings_registry::SettingsRegistry::load(path).unwrap());
    let svc = svc.with_settings_registry(registry);
    for pr in 1..=55 {
        let repo = if pr % 2 == 0 { "backend" } else { "frontend" };
        let (m, _) = svc
            .pr_monitor_register(&ws, &owner, "o", repo, pr)
            .await
            .unwrap();
        backdate(&svc, &m.monitor_id, "2020-01-01T00:00:00Z").await;
    }
    assert_eq!(forge.fetches(), 55, "one baseline per registration");
    let (ws2, sibling) = sibling_workspace(&svc, "agent-sibling").await;
    let (shared, _) = svc
        .pr_monitor_register(&ws2, &sibling, "o", "frontend", 1)
        .await
        .unwrap();
    backdate(&svc, &shared.monitor_id, "2020-01-01T00:00:00Z").await;
    forge.take_fetched_numbers();
    let before = forge.fetches();
    let before_sub = sub_fetch_totals(&forge);
    let mut observed = Vec::new();
    for _ in 0..11 {
        svc.poll_due_pr_monitors().await;
        let reads = forge.take_fetched_numbers();
        assert_eq!(
            reads.len(),
            5,
            "existing scheduler caps each tick at five distinct reads"
        );
        observed.extend(reads);
    }
    observed.sort_unstable();
    assert_eq!(
        observed,
        (1..=55).collect::<Vec<_>>(),
        "no starvation or duplicate fetch for sibling monitor"
    );
    assert_eq!(forge.fetches() - before, 55);
    assert_eq!(
        sub_fetch_totals(&forge),
        before_sub,
        "quiet fingerprints reuse shared cached details"
    );
    svc.poll_due_pr_monitors().await;
    assert!(
        forge.take_fetched_numbers().is_empty(),
        "fresh monitors incur no further reads"
    );
    assert_eq!(svc.pr_monitors_for_agent(&owner).await.unwrap().len(), 55);
}

#[tokio::test]
async fn configured_monitor_quota_clamps_file_values() {
    let (_db, root, svc, _forge, _ws, _owner) = setup().await;
    let path = root.path().join("monitor-quota.toml");
    std::fs::write(&path, "").unwrap();
    let registry = Arc::new(crate::settings_registry::SettingsRegistry::load(path).unwrap());
    let svc = svc.with_settings_registry(registry.clone());
    assert_eq!(svc.pr_monitor_max_per_agent(), 5);
    for (configured, effective) in [(0, 1), (1, 1), (55, 55), (100, 100), (101, 100)] {
        registry
            .reload(&format!("[prMonitor]\nmaxPerAgent = {configured}\n"))
            .unwrap();
        assert_eq!(svc.pr_monitor_max_per_agent(), effective);
    }
}

#[tokio::test]
async fn configured_monitor_quota_is_live_and_preserves_ownership_and_adoption() {
    let (_db, root, svc, forge, ws, owner) = setup().await;
    let path = root.path().join("monitor-quota.toml");
    std::fs::write(&path, "[prMonitor]\nmaxPerAgent = 6\n").unwrap();
    let registry = Arc::new(crate::settings_registry::SettingsRegistry::load(path).unwrap());
    let svc = svc.with_settings_registry(registry.clone());
    for pr in 1..=6 {
        svc.pr_monitor_register(&ws, &owner, "o", "r", pr)
            .await
            .unwrap();
    }
    let before = forge.fetches();
    assert!(svc
        .pr_monitor_register(&ws, &owner, "o", "r", 7)
        .await
        .unwrap_err()
        .to_string()
        .contains("max 6"));
    assert_eq!(forge.fetches(), before, "quota rejects before forge access");
    let sibling = second_agent(&svc, &ws, "agent-sibling").await;
    assert!(matches!(
        svc.pr_monitor_try_register(&ws, &sibling, "o", "r", 1)
            .await
            .unwrap(),
        PrMonitorRegistration::Refused(_)
    ));
    assert_eq!(
        forge.fetches(),
        before,
        "live ownership refuses without a fetch"
    );
    let (orphan, _) = svc
        .pr_monitor_register(&ws, &sibling, "o", "r", 7)
        .await
        .unwrap();
    kill_owner(&svc, &ws, &sibling, OwnerDeath::Error).await;
    assert!(
        svc.pr_monitor_register(&ws, &owner, "o", "r", 7)
            .await
            .is_err(),
        "adoption counts against cap"
    );
    registry.reload("[prMonitor]\nmaxPerAgent = 7\n").unwrap();
    let (adopted, _) = svc
        .pr_monitor_register(&ws, &owner, "o", "r", 7)
        .await
        .unwrap();
    assert_eq!(adopted.monitor_id, orphan.monitor_id);
    assert_eq!(adopted.agent_id, owner);
    registry.reload("[prMonitor]\nmaxPerAgent = 1\n").unwrap();
    let (same, _) = svc
        .pr_monitor_register(&ws, &owner, "o", "r", 7)
        .await
        .unwrap();
    assert_eq!(
        same.monitor_id, adopted.monitor_id,
        "re-registration at lower cap remains idempotent"
    );
    assert_eq!(
        svc.pr_monitors_for_agent(&owner).await.unwrap().len(),
        7,
        "lowering quota never evicts watches"
    );
    assert!(svc
        .pr_monitor_register(&ws, &owner, "o", "r", 8)
        .await
        .is_err());
}

#[tokio::test]
async fn configured_monitor_quota_serializes_concurrent_admissions() {
    let (_db, root, svc, forge, ws, owner) = setup().await;
    let path = root.path().join("monitor-quota.toml");
    std::fs::write(&path, "[prMonitor]\nmaxPerAgent = 6\n").unwrap();
    let registry = Arc::new(crate::settings_registry::SettingsRegistry::load(path).unwrap());
    let svc = svc.with_settings_registry(registry);
    for pr in 1..=5 {
        svc.pr_monitor_register(&ws, &owner, "o", "r", pr)
            .await
            .unwrap();
    }
    let before = forge.fetches();
    let (left, right) = tokio::join!(
        svc.pr_monitor_register(&ws, &owner, "o", "backend", 6),
        svc.pr_monitor_register(&ws, &owner, "o", "frontend", 7),
    );
    assert_ne!(
        left.is_ok(),
        right.is_ok(),
        "only one caller can take the last slot"
    );
    assert_eq!(
        forge.fetches() - before,
        1,
        "loser is refused before baseline fetch"
    );
    assert_eq!(svc.pr_monitors_for_agent(&owner).await.unwrap().len(), 6);
    assert!(
        svc.pr_monitor_registration.is_empty(),
        "admission locks are reclaimed"
    );
    let winner = left.or(right).unwrap().0;
    let (left, right) = tokio::join!(
        svc.pr_monitor_register(
            &ws,
            &owner,
            "o",
            &winner.repo_name,
            winner.pr_number.cast_unsigned()
        ),
        svc.pr_monitor_register(
            &ws,
            &owner,
            "o",
            &winner.repo_name,
            winner.pr_number.cast_unsigned()
        ),
    );
    assert_eq!(left.unwrap().0.monitor_id, winner.monitor_id);
    assert_eq!(right.unwrap().0.monitor_id, winner.monitor_id);
    assert_eq!(svc.pr_monitors_for_agent(&owner).await.unwrap().len(), 6);
    assert!(svc.pr_monitor_registration.is_empty());
}

#[tokio::test]
async fn configured_monitor_quota_preserves_multi_repo_lifecycle_notifications() {
    let (_db, root, svc, forge, ws, owner) = setup().await;
    let path = root.path().join("monitor-quota.toml");
    std::fs::write(&path, "[prMonitor]\nmaxPerAgent = 6\n").unwrap();
    let registry = Arc::new(crate::settings_registry::SettingsRegistry::load(path).unwrap());
    let svc = svc.with_settings_registry(registry);
    forge.edit(|s| s.draft = true);
    let mut monitors = Vec::new();
    for repo in ["backend", "frontend"] {
        for pr in 1..=3 {
            monitors.push(
                svc.pr_monitor_register(&ws, &owner, "o", repo, pr)
                    .await
                    .unwrap()
                    .0,
            );
        }
    }
    forge.edit(|s| {
        s.draft = false;
        s.head_sha = "bbbbbbbb".into();
        s.checks[0].state = CheckState::Success;
    });
    svc.poll_pr_monitors().await;
    for m in &monitors {
        let row = svc.store().get_pr_monitor(&m.monitor_id).await.unwrap();
        assert!(
            row.pending_changes
                .iter()
                .any(|c| c.contains("new commits pushed")),
            "{:?}",
            row.pending_changes
        );
        assert!(
            row.pending_changes
                .iter()
                .any(|c| c.contains("marked ready for review")),
            "{:?}",
            row.pending_changes
        );
    }
    for m in &monitors {
        let row = svc.store().get_pr_monitor(&m.monitor_id).await.unwrap();
        assert!(svc
            .store()
            .update_pr_monitor_poll(
                &m.monitor_id,
                PrMonitorPollUpdate {
                    last_snapshot: row.last_snapshot.as_deref(),
                    baseline_snapshot: row.baseline_snapshot.as_deref(),
                    pending_changes: &row.pending_changes,
                    pending_since: Some("2020-01-01T00:00:00Z"),
                    last_change_at: Some("2020-01-01T00:00:00Z"),
                    last_polled_at: row.last_polled_at.as_deref(),
                    last_error: None,
                    updated_at: &now_iso(),
                    expected_updated_at: &row.updated_at,
                }
            )
            .await
            .unwrap());
    }
    svc.poll_pr_monitors().await;
    let changed = owner_messages(&svc, &owner).await;
    assert_eq!(changed.matches("new commits pushed").count(), 6);
    assert_eq!(changed.matches("marked ready for review").count(), 6);
    // Terminal states bypass debounce and each yields one final wake.
    forge.edit(|s| s.pr_state = PrState::Merged);
    svc.poll_pr_monitors().await;
    svc.poll_pr_monitors().await;
    let messages = owner_messages(&svc, &owner).await;
    for m in &monitors {
        let label = format!("[PR monitor o/{}#{}]", m.repo_name, m.pr_number);
        assert_eq!(messages.matches(&label).count(), 2, "{messages}");
        assert_eq!(
            svc.store()
                .get_pr_monitor(&m.monitor_id)
                .await
                .unwrap()
                .state,
            PrMonitorState::Completed
        );
    }
    assert!(messages.contains("merged"));
    // Explicitly re-register reopened PRs: terminal monitors are not
    // automatic discovery watches; closure also sends one terminal wake.
    forge.edit(|s| s.pr_state = PrState::Open);
    for m in &monitors {
        svc.pr_monitor_register(&ws, &owner, "o", &m.repo_name, m.pr_number.cast_unsigned())
            .await
            .unwrap();
    }
    forge.edit(|s| s.pr_state = PrState::Closed);
    svc.poll_pr_monitors().await;
    svc.poll_pr_monitors().await;
    let messages = owner_messages(&svc, &owner).await;
    for m in &monitors {
        assert_eq!(
            messages
                .matches(&format!("[PR monitor o/{}#{}]", m.repo_name, m.pr_number))
                .count(),
            3
        );
    }
    assert!(messages.contains("closed"));
}
