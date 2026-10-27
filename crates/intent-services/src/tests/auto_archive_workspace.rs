//! Automatic archive (PROTOCOL §5.1 "Automatic archive", §5.12
//! `workspace.autoArchiveIdleAfterDays`) at the service layer: idle
//! workspaces are archived through `workspace.archive`; anything in flight,
//! shared, remote or already archived is left alone.

use intent_core::{WorkspaceApi, WorkspaceStatus};

use super::retire_workspace::{
    create_workspace, days_from_now, drain_changes, fixture, subscribe_updates,
};

/// On by default: with no setting, a workspace idle past 7 days is archived
/// on the next sweep with the ordinary archive delta; one idle 6 days is
/// not; a second sweep is a no-op.
#[intent_test_macros::daemon_test]
async fn auto_archive_is_on_by_default_after_seven_idle_days() {
    let f = fixture().await;
    let ws = create_workspace(&f).await;

    let early = f.svc.sweep_auto_archive_at(days_from_now(6)).await;
    assert_eq!(early, crate::AutoArchiveReport::default());
    assert!(!f.store.get_workspace(&ws.id).await.unwrap().archived);

    let mut sub = subscribe_updates(&f, &ws.id);
    let report = f.svc.sweep_auto_archive_at(days_from_now(8)).await;
    assert_eq!(report.archived, 1, "{report:?}");
    assert_eq!(report.failed, 0, "{report:?}");
    let row = f.store.get_workspace(&ws.id).await.unwrap();
    assert!(row.archived);
    assert_eq!(row.status, WorkspaceStatus::Archived);
    assert!(
        std::path::Path::new(ws.worktree_path.as_deref().unwrap()).is_dir(),
        "archiving keeps the folder"
    );
    let changes = drain_changes(&mut sub).await;
    assert!(
        changes
            .iter()
            .any(|c| c["status"] == "Archived" && c["archived"] == true),
        "archive delta published: {changes:?}"
    );

    let again = f.svc.sweep_auto_archive_at(days_from_now(30)).await;
    assert_eq!(again, crate::AutoArchiveReport::default());
}

/// `0` turns the sweep off, read live from the settings registry.
#[intent_test_macros::daemon_test]
async fn auto_archive_zero_turns_it_off_live() {
    let f = fixture().await;
    let cfg = f.root.join("auto-archive.config.toml");
    let registry = std::sync::Arc::new(crate::SettingsRegistry::load(cfg).expect("registry"));
    let svc = f.svc.clone().with_settings_registry(registry.clone());
    let ws = create_workspace(&f).await;
    registry
        .apply(&[(
            "workspace.autoArchiveIdleAfterDays".to_string(),
            serde_json::json!(0),
        )])
        .expect("turn off");
    assert_eq!(
        svc.sweep_auto_archive_at(days_from_now(3650)).await,
        crate::AutoArchiveReport::default()
    );
    assert!(!f.store.get_workspace(&ws.id).await.unwrap().archived);

    registry
        .apply(&[(
            "workspace.autoArchiveIdleAfterDays".to_string(),
            serde_json::json!(30),
        )])
        .expect("30 days");
    assert_eq!(
        svc.sweep_auto_archive_at(days_from_now(20)).await.archived,
        0
    );
    assert_eq!(
        svc.sweep_auto_archive_at(days_from_now(31)).await.archived,
        1
    );
}

/// Recent activity resets the idle clock.
#[intent_test_macros::daemon_test]
async fn auto_archive_respects_recent_activity() {
    let f = fixture().await;
    let ws = create_workspace(&f).await;
    let recent = days_from_now(5)
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap();
    f.store
        .bump_workspace_last_activity(&ws.id, &recent)
        .await
        .expect("bump activity");
    let report = f
        .svc
        .sweep_auto_archive_after(Some(7), days_from_now(8))
        .await;
    assert_eq!(report.archived, 0, "{report:?}");
    assert!(!f.store.get_workspace(&ws.id).await.unwrap().archived);
}

/// A workspace with an active hook, an active PR monitor or an open invite is
/// skipped; the idle workspace next to them is still archived.
#[intent_test_macros::daemon_test]
async fn auto_archive_skips_hooks_monitors_and_guests() {
    let f = fixture().await;
    let hooked = create_workspace(&f).await;
    let monitored = create_workspace(&f).await;
    let shared = create_workspace(&f).await;
    let idle = create_workspace(&f).await;

    let agent = intent_core::AgentId::from(format!("agent-{}", hooked.id.0));
    f.store
        .insert_hook(&crate::test_support::fake_runtime::scheduled_hook(
            &hooked.id, &agent, "wait",
        ))
        .await
        .expect("hook");
    let monitor_agent = intent_core::AgentId::from(format!("agent-{}", monitored.id.0));
    f.store
        .insert_pr_monitor(&intent_core::PrMonitor {
            monitor_id: intent_core::PrMonitorId::new(),
            workspace_id: monitored.id.clone(),
            agent_id: monitor_agent,
            repo_owner: "o".to_string(),
            repo_name: "r".to_string(),
            pr_number: 1,
            state: intent_core::PrMonitorState::Active,
            last_snapshot: None,
            baseline_snapshot: None,
            pending_changes: vec![],
            pending_since: None,
            last_change_at: None,
            last_polled_at: None,
            last_error: None,
            created_at: intent_core::now_iso(),
            updated_at: intent_core::now_iso(),
        })
        .await
        .expect("monitor");
    let primary = f.store.get_primary_principal().await.expect("primary");
    f.store
        .insert_workspace_invite(&intent_core::WorkspaceInvite {
            id: uuid::Uuid::new_v4().to_string(),
            workspace_id: shared.id.clone(),
            secret_hash: crate::invite_ops::hash_secret("s"),
            secret: None,
            created_by_principal_id: primary.id,
            pin_github_user_id: None,
            pin_login: None,
            pin_identity: None,
            created_at: intent_core::now_iso(),
            expires_at: intent_core::iso_ms_from_now(3_600_000 * 24 * 365),
            redeemed_at: None,
            redeemed_by_principal_id: None,
            revoked_at: None,
            redemption_count: 0,
        })
        .await
        .expect("open invite");

    let report = f
        .svc
        .sweep_auto_archive_after(Some(7), days_from_now(8))
        .await;
    assert_eq!(report.archived, 1, "{report:?}");
    assert_eq!(report.skipped, 3, "{report:?}");
    assert_eq!(report.failed, 0, "{report:?}");
    for kept in [&hooked, &monitored, &shared] {
        assert!(
            !f.store.get_workspace(&kept.id).await.unwrap().archived,
            "{}",
            kept.id
        );
    }
    assert!(f.store.get_workspace(&idle.id).await.unwrap().archived);
}

/// Remote and Chief workspaces are never archived automatically.
#[intent_test_macros::daemon_test]
async fn auto_archive_skips_remote_and_chief() {
    let f = fixture().await;
    let id = intent_core::WorkspaceId::from("ws-auto-archive-remote");
    let mut remote = super::workspace(&id);
    remote.is_remote = true;
    remote.created_at = "2020-01-01T00:00:00Z".to_string();
    f.store.insert_workspace(&remote).await.expect("insert");
    let report = f
        .svc
        .sweep_auto_archive_after(Some(1), days_from_now(30))
        .await;
    assert_eq!(report.archived, 0, "{report:?}");
    assert!(!f.store.get_workspace(&id).await.unwrap().archived);
    let chief = f
        .store
        .get_workspace(&intent_core::WorkspaceId::from(
            intent_core::CHIEF_WORKSPACE_ID,
        ))
        .await;
    if let Ok(chief) = chief {
        assert!(!chief.archived);
    }
}

/// An archived workspace unarchived by a message keeps working: the
/// auto-archive delta is the ordinary archive delta, so the existing
/// unarchive path brings it back.
#[intent_test_macros::daemon_test]
async fn auto_archived_workspace_unarchives_normally() {
    let f = fixture().await;
    let ws = create_workspace(&f).await;
    assert_eq!(
        f.svc
            .sweep_auto_archive_after(Some(7), days_from_now(8))
            .await
            .archived,
        1
    );
    let back = f
        .svc
        .unarchive_workspace(ws.id.clone())
        .await
        .expect("unarchive");
    assert_eq!(back.status, WorkspaceStatus::Active);
    assert!(std::path::Path::new(ws.worktree_path.as_deref().unwrap()).is_dir());
}
