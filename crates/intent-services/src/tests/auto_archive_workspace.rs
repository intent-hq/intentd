//! Automatic archive (PROTOCOL §5.1 "Automatic archive", §5.12
//! `workspace.autoArchiveIdleAfterDays`) at the service layer: idle
//! workspaces are archived through `workspace.archive`; anything in flight,
//! shared, remote or already archived is left alone.

use intent_core::{WorkspaceApi, WorkspaceStatus};

use super::retire_workspace::{drain_changes, subscribe_updates};
use crate::test_support::retire_workspace::{create_workspace, days_from_now, fixture};

/// Off by default: with no setting, even a workspace idle for years is
/// left alone.
#[intent_test_macros::daemon_test]
async fn auto_archive_is_off_by_default() {
    let f = fixture().await;
    let ws = create_workspace(&f).await;
    assert_eq!(
        f.svc.sweep_auto_archive_at(days_from_now(3650)).await,
        crate::AutoArchiveReport::default()
    );
    assert!(!f.store.get_workspace(&ws.id).await.unwrap().archived);
}

/// With a 7-day window, a workspace idle past 7 days is archived on the next
/// sweep with the ordinary archive delta; one idle 6 days is not; a second
/// sweep is a no-op.
#[intent_test_macros::daemon_test]
async fn auto_archive_archives_after_the_configured_idle_days() {
    let f = fixture().await;
    let ws = create_workspace(&f).await;

    let early = f
        .svc
        .sweep_auto_archive_after(Some(7), days_from_now(6))
        .await;
    assert_eq!(early, crate::AutoArchiveReport::default());
    assert!(!f.store.get_workspace(&ws.id).await.unwrap().archived);

    let mut sub = subscribe_updates(&f, &ws.id);
    let report = f
        .svc
        .sweep_auto_archive_after(Some(7), days_from_now(8))
        .await;
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

    let again = f
        .svc
        .sweep_auto_archive_after(Some(7), days_from_now(30))
        .await;
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

/// An idle workspace with uncommitted changes is skipped (still Active) and
/// archived by the first sweep after the changes are gone.
#[intent_test_macros::daemon_test]
async fn auto_archive_skips_a_workspace_with_uncommitted_changes() {
    let f = fixture().await;
    let ws = create_workspace(&f).await;
    let wt = std::path::PathBuf::from(ws.worktree_path.clone().unwrap());
    std::fs::write(wt.join("draft.txt"), "unsaved work").unwrap();

    let report = f
        .svc
        .sweep_auto_archive_after(Some(7), days_from_now(8))
        .await;
    assert_eq!(report.archived, 0, "{report:?}");
    assert_eq!(report.skipped, 1, "{report:?}");
    assert_eq!(report.failed, 0, "{report:?}");
    assert!(!f.store.get_workspace(&ws.id).await.unwrap().archived);

    std::fs::remove_file(wt.join("draft.txt")).unwrap();
    let report = f
        .svc
        .sweep_auto_archive_after(Some(7), days_from_now(8))
        .await;
    assert_eq!(report.archived, 1, "{report:?}");
    assert!(f.store.get_workspace(&ws.id).await.unwrap().archived);
}

/// An idle workspace whose branch has commits on no remote-tracking ref is
/// skipped; once a remote ref covers them it is archived. (The fixture repo
/// starts with no remote refs, where history alone never blocks.)
#[intent_test_macros::daemon_test]
async fn auto_archive_skips_a_workspace_with_unpushed_commits() {
    let f = fixture().await;
    let ws = create_workspace(&f).await;
    let wt = std::path::PathBuf::from(ws.worktree_path.clone().unwrap());
    let repo = git2::Repository::open(&wt).expect("open worktree");
    let base = repo.head().unwrap().peel_to_commit().unwrap();
    repo.reference("refs/remotes/origin/main", base.id(), true, "test remote")
        .expect("remote ref");
    std::fs::write(wt.join("feature.txt"), "work\n").unwrap();
    let mut index = repo.index().unwrap();
    index.add_path(std::path::Path::new("feature.txt")).unwrap();
    index.write().unwrap();
    let tree = repo.find_tree(index.write_tree().unwrap()).unwrap();
    let sig = git2::Signature::now("Tester", "t@e.dev").unwrap();
    let head = repo
        .commit(
            Some("HEAD"),
            &sig,
            &sig,
            "feat: local only",
            &tree,
            &[&base],
        )
        .unwrap();

    let report = f
        .svc
        .sweep_auto_archive_after(Some(7), days_from_now(8))
        .await;
    assert_eq!(report.archived, 0, "{report:?}");
    assert_eq!(report.skipped, 1, "{report:?}");
    assert!(!f.store.get_workspace(&ws.id).await.unwrap().archived);

    repo.reference("refs/remotes/origin/feature", head, true, "pushed")
        .expect("pushed ref");
    let report = f
        .svc
        .sweep_auto_archive_after(Some(7), days_from_now(8))
        .await;
    assert_eq!(report.archived, 1, "{report:?}");
    assert!(f.store.get_workspace(&ws.id).await.unwrap().archived);
}

/// Activity that lands while the sweep runs its git scan wins: the final
/// step re-reads the row and leaves a workspace that is no longer idle
/// Active, then archives it once it is idle again.
#[intent_test_macros::daemon_test]
async fn auto_archive_rechecks_the_idle_clock_after_the_git_scan() {
    let f = fixture().await;
    let ws = create_workspace(&f).await;
    let cutoff = days_from_now(1);

    let during_scan = days_from_now(2)
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap();
    f.store
        .bump_workspace_last_activity(&ws.id, &during_scan)
        .await
        .expect("bump activity");
    assert_eq!(
        f.svc.archive_if_still_idle(&ws.id, cutoff).await,
        crate::auto_archive::IdleArchive::NoLongerDue
    );
    assert!(!f.store.get_workspace(&ws.id).await.unwrap().archived);

    assert_eq!(
        f.svc.archive_if_still_idle(&ws.id, days_from_now(3)).await,
        crate::auto_archive::IdleArchive::Archived
    );
    assert!(f.store.get_workspace(&ws.id).await.unwrap().archived);
}

/// Commit `name` on top of `HEAD` in `wt` and return the new commit.
fn commit_on_head(wt: &std::path::Path, name: &str) -> git2::Oid {
    let repo = git2::Repository::open(wt).expect("open worktree");
    let parent = repo.head().unwrap().peel_to_commit().unwrap();
    std::fs::write(wt.join(name), "work\n").unwrap();
    let mut index = repo.index().unwrap();
    index.add_path(std::path::Path::new(name)).unwrap();
    index.write().unwrap();
    let tree = repo.find_tree(index.write_tree().unwrap()).unwrap();
    let sig = git2::Signature::now("Tester", "t@e.dev").unwrap();
    repo.commit(Some("HEAD"), &sig, &sig, name, &tree, &[&parent])
        .unwrap()
}

/// A workspace whose branch was pushed, squash-merged, then deleted and
/// pruned on the remote: only `origin/main` (the base) remains, so its two
/// commits are on no remote ref. Returns the workspace and its `HEAD`.
async fn squash_merged_and_pruned(
    f: &crate::test_support::retire_workspace::Fixture,
) -> (intent_core::Workspace, git2::Oid) {
    let ws = create_workspace(f).await;
    let wt = std::path::PathBuf::from(ws.worktree_path.clone().unwrap());
    let repo = git2::Repository::open(&wt).expect("open worktree");
    let base = repo.head().unwrap().peel_to_commit().unwrap().id();
    repo.reference("refs/remotes/origin/main", base, true, "base")
        .expect("remote ref");
    commit_on_head(&wt, "one.txt");
    let head = commit_on_head(&wt, "two.txt");
    (ws, head)
}

/// Record a completed PR monitor whose final snapshot shows the PR merged at
/// `head_sha`.
async fn insert_merged_monitor(
    f: &crate::test_support::retire_workspace::Fixture,
    ws: &intent_core::Workspace,
    head_sha: &str,
) {
    let mut snapshot = crate::v1_goldens::pr_snapshot("merged");
    snapshot.requirements.state = "merged".to_string();
    snapshot.head_sha = Some(head_sha.to_string());
    let snapshot = serde_json::to_string(&snapshot).unwrap();
    f.store
        .insert_pr_monitor(&intent_core::PrMonitor {
            monitor_id: intent_core::PrMonitorId::new(),
            workspace_id: ws.id.clone(),
            agent_id: intent_core::AgentId::from(format!("agent-{}", ws.id.0)),
            repo_owner: "o".to_string(),
            repo_name: "r".to_string(),
            pr_number: 42,
            state: intent_core::PrMonitorState::Completed,
            last_snapshot: Some(snapshot.clone()),
            baseline_snapshot: Some(snapshot),
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
}

async fn sweep(f: &crate::test_support::retire_workspace::Fixture) -> crate::AutoArchiveReport {
    f.svc
        .sweep_auto_archive_after(Some(7), days_from_now(8))
        .await
}

/// A squash-merged workspace whose branch was pruned is skipped while no
/// merged PR is known (indistinguishable from a never-pushed branch), and
/// archived once a completed PR monitor shows the PR merged at its `HEAD`.
#[intent_test_macros::daemon_test]
async fn auto_archive_archives_a_merged_pr_workspace_after_the_branch_is_pruned() {
    let f = fixture().await;
    let (ws, head) = squash_merged_and_pruned(&f).await;

    let report = sweep(&f).await;
    assert_eq!((report.archived, report.skipped), (0, 1), "{report:?}");

    insert_merged_monitor(&f, &ws, &head.to_string()).await;
    let report = sweep(&f).await;
    assert_eq!(report.archived, 1, "{report:?}");
    assert!(f.store.get_workspace(&ws.id).await.unwrap().archived);
}

/// The workspace's stored PR record (no monitor) is enough when it says
/// merged and carries the head SHA; an open record is not.
#[intent_test_macros::daemon_test]
async fn auto_archive_uses_the_stored_merged_pr_record() {
    let f = fixture().await;
    let (ws, head) = squash_merged_and_pruned(&f).await;
    let pr = |status: &str| {
        serde_json::from_value::<intent_core::PullRequestInfo>(serde_json::json!({
            "id": "PR_1",
            "number": 42,
            "url": "https://github.com/o/r/pull/42",
            "title": "feat: work",
            "status": status,
            "createdAt": "2026-01-01T00:00:00Z",
            "updatedAt": "2026-01-02T00:00:00Z",
            "headSha": head.to_string(),
        }))
        .expect("pr record")
    };

    let mut row = f.store.get_workspace(&ws.id).await.unwrap();
    row.pull_requests = Some(vec![pr("Open")]);
    f.store.update_workspace(&row).await.expect("update");
    let report = sweep(&f).await;
    assert_eq!((report.archived, report.skipped), (0, 1), "{report:?}");

    let mut row = f.store.get_workspace(&ws.id).await.unwrap();
    row.pull_requests = Some(vec![pr("Merged")]);
    f.store.update_workspace(&row).await.expect("update");
    let report = sweep(&f).await;
    assert_eq!(report.archived, 1, "{report:?}");
    assert!(f.store.get_workspace(&ws.id).await.unwrap().archived);
}

/// A merged PR never lifts other work: a commit made after the merge, a
/// merged PR for some other commit, and uncommitted changes all keep the
/// workspace Active.
#[intent_test_macros::daemon_test]
async fn auto_archive_merged_pr_does_not_cover_new_or_uncommitted_work() {
    let f = fixture().await;

    let (after_merge, merged_head) = squash_merged_and_pruned(&f).await;
    insert_merged_monitor(&f, &after_merge, &merged_head.to_string()).await;
    commit_on_head(
        std::path::Path::new(after_merge.worktree_path.as_deref().unwrap()),
        "follow-up.txt",
    );

    let (other_pr, _) = squash_merged_and_pruned(&f).await;
    insert_merged_monitor(&f, &other_pr, "0123456789abcdef0123456789abcdef01234567").await;

    let (dirty, head) = squash_merged_and_pruned(&f).await;
    insert_merged_monitor(&f, &dirty, &head.to_string()).await;
    std::fs::write(
        std::path::Path::new(dirty.worktree_path.as_deref().unwrap()).join("draft.txt"),
        "unsaved",
    )
    .unwrap();

    let report = sweep(&f).await;
    assert_eq!((report.archived, report.skipped), (0, 3), "{report:?}");
    for kept in [&after_merge, &other_pr, &dirty] {
        assert!(
            !f.store.get_workspace(&kept.id).await.unwrap().archived,
            "{}",
            kept.id
        );
    }
}
