//! Tests for the stale tracked-change sweep (intent-hq/intent#7266).

use std::path::{Path, PathBuf};

use git2::{Repository, Signature};
use intent_core::{AgentStatus, WorkspaceId};
use intent_store::{NewTrackedChange, Store};

use super::CleanRows;
use crate::tests::{test_tempdir, workspace, TempDb};
use crate::Services;

struct Repo {
    dir: PathBuf,
    _guard: tempfile::TempDir,
}

fn init_repo() -> Repo {
    let guard = test_tempdir("intentd-sweep-");
    let dir = guard.path().to_path_buf();
    let repo = Repository::init(&dir).unwrap();
    let mut cfg = repo.config().unwrap();
    cfg.set_str("user.name", "Test").unwrap();
    cfg.set_str("user.email", "test@example.com").unwrap();
    commit(&dir, &[("seed.txt", "seed\n")]);
    Repo { dir, _guard: guard }
}

fn write(dir: &Path, rel: &str, contents: &str) {
    let full = dir.join(rel);
    std::fs::create_dir_all(full.parent().unwrap()).unwrap();
    std::fs::write(full, contents).unwrap();
}

/// Write and commit `files` (paths added through the index, like `git add -f`).
fn commit(dir: &Path, files: &[(&str, &str)]) {
    let repo = Repository::open(dir).unwrap();
    let mut index = repo.index().unwrap();
    for (rel, contents) in files {
        write(dir, rel, contents);
        index.add_path(Path::new(rel)).unwrap();
    }
    index.write().unwrap();
    let tree = repo.find_tree(index.write_tree().unwrap()).unwrap();
    let sig = Signature::now("Test", "test@example.com").unwrap();
    let parents: Vec<git2::Commit> = repo
        .head()
        .ok()
        .and_then(|h| h.target())
        .and_then(|oid| repo.find_commit(oid).ok())
        .into_iter()
        .collect();
    let refs: Vec<&git2::Commit> = parents.iter().collect();
    repo.commit(Some("HEAD"), &sig, &sig, "commit", &tree, &refs)
        .unwrap();
}

fn stage(dir: &Path, rel: &str) {
    let repo = Repository::open(dir).unwrap();
    let mut index = repo.index().unwrap();
    index.add_path(Path::new(rel)).unwrap();
    index.write().unwrap();
}

/// Register `child` as a populated submodule of `parent` at `sub_rel`.
fn add_submodule(parent: &Path, child: &Path, sub_rel: &str) {
    let repo = Repository::open(parent).unwrap();
    let url = child.to_string_lossy().to_string();
    let mut sm = repo.submodule(&url, Path::new(sub_rel), true).unwrap();
    let _ = std::fs::remove_dir_all(parent.join(sub_rel));
    Repository::clone(&url, parent.join(sub_rel)).unwrap();
    sm.add_to_index(true).unwrap();
    sm.add_finalize().unwrap();
    let mut index = repo.index().unwrap();
    index.read(true).unwrap();
    let tree = repo.find_tree(index.write_tree().unwrap()).unwrap();
    let sig = Signature::now("Test", "test@example.com").unwrap();
    let head = repo
        .find_commit(repo.head().unwrap().target().unwrap())
        .unwrap();
    repo.commit(Some("HEAD"), &sig, &sig, "add submodule", &tree, &[&head])
        .unwrap();
}

async fn svc_for(worktree: Option<&Path>) -> (TempDb, Services, WorkspaceId) {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let id = WorkspaceId::new();
    let mut ws = workspace(&id);
    ws.worktree_path = worktree.map(|p| p.to_string_lossy().to_string());
    store.insert_workspace(&ws).await.unwrap();
    (tmp, Services::new(store), id)
}

async fn track(svc: &Services, ws: &WorkspaceId, path: &str, stage: &str) {
    svc.store()
        .upsert_tracked_change(&NewTrackedChange {
            workspace_id: ws.clone(),
            path: path.to_string(),
            stage: stage.to_string(),
            status: "modified".to_string(),
            agent_id: Some("agent-1".to_string()),
            session_id: None,
            turn: None,
            commit_hash: None,
            old_blob_sha: None,
            new_blob_sha: None,
            additions: 1,
            deletions: 0,
        })
        .await
        .unwrap();
}

async fn paths(svc: &Services, ws: &WorkspaceId) -> Vec<(String, String)> {
    let mut rows: Vec<(String, String)> = svc
        .store()
        .list_tracked_changes(ws)
        .await
        .unwrap()
        .into_iter()
        .map(|r| (r.path, r.stage))
        .collect();
    rows.sort();
    rows
}

/// Mark then confirm: run two passes and return the rows the confirming pass
/// deleted (the marking pass never deletes).
async fn prune_confirmed(svc: &Services, ws: &WorkspaceId) -> u64 {
    let first = svc
        .prune_stale_tracked_changes(ws, &CleanRows::new())
        .await
        .unwrap();
    assert_eq!(first.deleted, 0, "the first pass only marks");
    svc.prune_stale_tracked_changes(ws, &first.clean)
        .await
        .unwrap()
        .deleted
}

fn owned(v: &[(&str, &str)]) -> Vec<(String, String)> {
    v.iter()
        .map(|(p, s)| ((*p).to_string(), (*s).to_string()))
        .collect()
}

/// Regression (intent-hq/intent#7266): rows whose path git reports clean —
/// ignored, deleted-untracked, reverted, committed outside the daemon — are
/// pruned; rows for real changes and non-working stages stay; the pruned
/// paths drop out of the agent-lock snapshot.
#[tokio::test]
async fn prune_drops_clean_paths_and_keeps_dirty_ones() {
    let repo = init_repo();
    let d = &repo.dir;
    commit(
        d,
        &[
            (".gitignore", ".dev/\n"),
            ("edited.rs", "a\n"),
            ("removed.rs", "a\n"),
            ("reverted.rs", "a\n"),
            ("old.rs", "fn old() {}\n"),
            ("history.rs", "h\n"),
        ],
    );
    // Committed from a terminal after the agent's row was recorded.
    commit(d, &[("terminal.rs", "t\n")]);
    write(d, "edited.rs", "b\n");
    std::fs::remove_file(d.join("removed.rs")).unwrap();
    write(d, "untracked.rs", "n\n");
    write(d, "staged.rs", "s\n");
    stage(d, "staged.rs");
    write(d, ".dev/scratch.json", "{}");
    // Unstaged rename: the old side is a tracked deletion, the new side untracked.
    std::fs::rename(d.join("old.rs"), d.join("new.rs")).unwrap();

    let (_t, svc, ws) = svc_for(Some(d)).await;
    let agent = crate::agent_locks::tests::session("agent-1", &ws, AgentStatus::Active, None);
    svc.store().insert_agent_session(&agent).await.unwrap();
    for p in [
        "edited.rs",
        "removed.rs",
        "untracked.rs",
        "new.rs",
        "old.rs",
        ".dev/scratch.json",
        ".dev/gone.json",
        "reverted.rs",
        "terminal.rs",
    ] {
        track(&svc, &ws, p, "unstaged").await;
    }
    track(&svc, &ws, "staged.rs", "staged").await;
    track(&svc, &ws, "history.rs", "committed").await;

    let deleted = prune_confirmed(&svc, &ws).await;
    assert_eq!(deleted, 4);
    assert_eq!(
        paths(&svc, &ws).await,
        owned(&[
            ("edited.rs", "unstaged"),
            ("history.rs", "committed"),
            ("new.rs", "unstaged"),
            ("old.rs", "unstaged"),
            ("removed.rs", "unstaged"),
            ("staged.rs", "staged"),
            ("untracked.rs", "unstaged"),
        ])
    );
    let after = svc.compute_agent_locks(&ws).await;
    assert_eq!(
        after.locked_file_paths,
        vec![
            "edited.rs".to_string(),
            "new.rs".to_string(),
            "old.rs".to_string(),
            "removed.rs".to_string(),
            "staged.rs".to_string(),
            "untracked.rs".to_string(),
        ]
    );
    // Another round finds nothing more.
    assert_eq!(prune_confirmed(&svc, &ws).await, 0);
}

/// A path that later becomes git-ignored is pruned too.
#[tokio::test]
async fn prune_drops_paths_that_became_ignored() {
    let repo = init_repo();
    write(&repo.dir, "logs/run.log", "x\n");
    let (_t, svc, ws) = svc_for(Some(&repo.dir)).await;
    track(&svc, &ws, "logs/run.log", "unstaged").await;
    assert_eq!(prune_confirmed(&svc, &ws).await, 0);
    commit(&repo.dir, &[(".gitignore", "logs/\n")]);
    assert_eq!(prune_confirmed(&svc, &ws).await, 1);
    assert!(paths(&svc, &ws).await.is_empty());
}

/// Rows inside a submodule follow the submodule's own status and
/// `.gitignore`; rows inside an unpopulated submodule are kept.
#[tokio::test]
async fn prune_judges_submodule_rows_by_the_submodule() {
    let child = init_repo();
    commit(
        &child.dir,
        &[
            (".gitignore", "target/\n"),
            ("lib.rs", "x\n"),
            ("clean.rs", "c\n"),
        ],
    );
    let other = init_repo();
    let repo = init_repo();
    add_submodule(&repo.dir, &child.dir, "pkg/sub");
    add_submodule(&repo.dir, &other.dir, "empty");
    std::fs::remove_dir_all(repo.dir.join("empty")).unwrap();
    std::fs::create_dir_all(repo.dir.join("empty")).unwrap();
    write(&repo.dir, "pkg/sub/lib.rs", "changed\n");
    write(&repo.dir, "pkg/sub/target/out.o", "bin");

    let (_t, svc, ws) = svc_for(Some(&repo.dir)).await;
    for p in [
        "pkg/sub/lib.rs",
        "pkg/sub/clean.rs",
        "pkg/sub/target/out.o",
        "empty/unknown.rs",
    ] {
        track(&svc, &ws, p, "unstaged").await;
    }
    assert_eq!(prune_confirmed(&svc, &ws).await, 2);
    assert_eq!(
        paths(&svc, &ws).await,
        owned(&[
            ("empty/unknown.rs", "unstaged"),
            ("pkg/sub/lib.rs", "unstaged")
        ])
    );
}

/// Race guard: a row re-recorded after the confirming pass listed it (an
/// agent write landing while the scan runs) survives, even though its path
/// looked clean in both passes.
#[tokio::test]
async fn prune_keeps_a_row_recorded_after_listing() {
    let repo = init_repo();
    let (_t, svc, ws) = svc_for(Some(&repo.dir)).await;
    track(&svc, &ws, "late.rs", "unstaged").await;
    track(&svc, &ws, "gone.rs", "unstaged").await;
    let first = svc
        .prune_stale_tracked_changes(&ws, &CleanRows::new())
        .await
        .unwrap();
    assert_eq!(first.clean.len(), 2);
    let hook_svc = svc.clone();
    let hook_ws = ws.clone();
    super::set_before_scan_hook(
        &ws,
        Box::new(move || {
            Box::pin(async move {
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
                track(&hook_svc, &hook_ws, "late.rs", "unstaged").await;
            })
        }),
    );
    let second = svc
        .prune_stale_tracked_changes(&ws, &first.clean)
        .await
        .unwrap();
    assert_eq!(second.deleted, 1);
    assert_eq!(paths(&svc, &ws).await, owned(&[("late.rs", "unstaged")]));
}

/// A path that is clean only momentarily — `git stash` then `stash pop`
/// between passes — keeps its row: the confirming pass sees it dirty again.
#[tokio::test]
async fn prune_keeps_a_row_whose_path_is_dirty_again_by_the_confirming_pass() {
    let repo = init_repo();
    commit(&repo.dir, &[("work.rs", "base\n")]);
    let (_t, svc, ws) = svc_for(Some(&repo.dir)).await;
    track(&svc, &ws, "work.rs", "unstaged").await;
    // Stashed: the worktree matches HEAD.
    let first = svc
        .prune_stale_tracked_changes(&ws, &CleanRows::new())
        .await
        .unwrap();
    assert_eq!(first.clean.len(), 1, "marked while stashed");
    // Popped: the agent's edit is back.
    write(&repo.dir, "work.rs", "agent edit\n");
    let second = svc
        .prune_stale_tracked_changes(&ws, &first.clean)
        .await
        .unwrap();
    assert_eq!(second.deleted, 0);
    assert!(second.clean.is_empty());
    assert_eq!(paths(&svc, &ws).await, owned(&[("work.rs", "unstaged")]));
}

/// A daemon commit advances HEAD before it moves its rows to `committed`; a
/// pass landing in that gap only marks the row, and the stage move keeps the
/// confirming pass from deleting it.
#[tokio::test]
async fn prune_keeps_a_row_committed_between_passes() {
    let repo = init_repo();
    let (_t, svc, ws) = svc_for(Some(&repo.dir)).await;
    commit(&repo.dir, &[("done.rs", "d\n")]);
    track(&svc, &ws, "done.rs", "unstaged").await;
    let first = svc
        .prune_stale_tracked_changes(&ws, &CleanRows::new())
        .await
        .unwrap();
    assert_eq!(first.clean.len(), 1, "marked in the commit gap");
    svc.store()
        .set_tracked_change_stage(&ws, "done.rs", "unstaged", "committed")
        .await
        .unwrap();
    let second = svc
        .prune_stale_tracked_changes(&ws, &first.clean)
        .await
        .unwrap();
    assert_eq!(second.deleted, 0);
    assert_eq!(paths(&svc, &ws).await, owned(&[("done.rs", "committed")]));
}

/// A sandboxed (copy-on-write) agent's rows describe files in its sandbox, which the
/// workspace worktree cannot judge: the sweep leaves them alone and the lock
/// filter keeps them.
#[tokio::test]
async fn sandboxed_agent_rows_are_left_alone() {
    let repo = init_repo();
    let (_t, svc, ws) = svc_for(Some(&repo.dir)).await;
    let mut agent = crate::agent_locks::tests::session("agent-1", &ws, AgentStatus::Active, None);
    agent.sandbox_path = Some(repo.dir.join("sandbox").to_string_lossy().to_string());
    svc.store().insert_agent_session(&agent).await.unwrap();
    track(&svc, &ws, "src/in-sandbox.rs", "unstaged").await;

    assert_eq!(prune_confirmed(&svc, &ws).await, 0);
    assert_eq!(
        paths(&svc, &ws).await,
        owned(&[("src/in-sandbox.rs", "unstaged")])
    );
    assert_eq!(
        svc.compute_agent_locks(&ws).await.locked_file_paths,
        vec!["src/in-sandbox.rs".to_string()]
    );
}

/// A workspace whose worktree cannot be scanned, or that has no local
/// worktree, deletes nothing; the multi-workspace pass prunes the rest.
#[tokio::test]
async fn sweep_skips_unscannable_workspaces() {
    let not_a_repo = test_tempdir("intentd-sweep-norepo-");
    let (_t, svc, broken) = svc_for(Some(not_a_repo.path())).await;
    track(&svc, &broken, "a.rs", "unstaged").await;

    let pathless = WorkspaceId::new();
    svc.store()
        .insert_workspace(&workspace(&pathless))
        .await
        .unwrap();
    track(&svc, &pathless, "b.rs", "unstaged").await;

    let mut remote_ws = workspace(&WorkspaceId::new());
    remote_ws.is_remote = true;
    let repo = init_repo();
    remote_ws.worktree_path = Some(repo.dir.to_string_lossy().to_string());
    svc.store().insert_workspace(&remote_ws).await.unwrap();
    track(&svc, &remote_ws.id, "c.rs", "unstaged").await;

    let live = WorkspaceId::new();
    let mut live_ws = workspace(&live);
    live_ws.worktree_path = Some(repo.dir.to_string_lossy().to_string());
    svc.store().insert_workspace(&live_ws).await.unwrap();
    track(&svc, &live, "d.rs", "unstaged").await;

    let mut memory = std::collections::HashMap::new();
    assert_eq!(svc.sweep_stale_tracked_changes(&mut memory).await, 0);
    assert_eq!(
        memory.keys().collect::<Vec<_>>(),
        vec![&live],
        "only the scannable workspace marks rows"
    );
    assert_eq!(svc.sweep_stale_tracked_changes(&mut memory).await, 1);
    assert!(memory.is_empty());
    assert_eq!(paths(&svc, &broken).await.len(), 1);
    assert_eq!(paths(&svc, &pathless).await.len(), 1);
    assert_eq!(paths(&svc, &remote_ws.id).await.len(), 1);
    assert!(paths(&svc, &live).await.is_empty());
}

/// Pruning recomputes the metrics aggregates and publishes
/// `changes:metrics-changed` for the workspace.
#[tokio::test]
async fn prune_recomputes_and_publishes_metrics() {
    let repo = init_repo();
    let (_t, svc, ws) = svc_for(Some(&repo.dir)).await;
    let bus = crate::EventBus::new(svc.store().clone());
    let svc = svc.with_event_bus(bus.clone());
    write(&repo.dir, "kept.rs", "k\n");
    track(&svc, &ws, "kept.rs", "unstaged").await;
    track(&svc, &ws, "gone.rs", "unstaged").await;
    crate::metrics::recompute(svc.store(), &ws).await.unwrap();
    let mut sub = bus.subscribe(crate::SubscriptionFilter {
        workspace_id: Some(ws.0.clone()),
        event_types: vec![intent_core::events::CHANGES_METRICS_CHANGED.to_string()],
        ..Default::default()
    });

    assert_eq!(prune_confirmed(&svc, &ws).await, 1);
    let events = tokio::time::timeout(
        intentd_test_support::test_timeout(std::time::Duration::from_secs(5)),
        sub.recv(),
    )
    .await
    .expect("metrics event")
    .expect("subscription open");
    let event = events.first().expect("one event");
    assert_eq!(event.data["metrics"]["filesChanged"], serde_json::json!(1));
    let stored = svc
        .store()
        .get_workspace_metrics(&ws)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(stored.files_changed, 1);
}

/// intent-hq/intent#7183: a workspace whose checkout folder is missing is
/// skipped (nothing marked or deleted) and recorded in the shared presence
/// map; once the folder returns the sweep resumes for it.
#[tokio::test]
async fn sweep_skips_missing_checkout_and_resumes_when_it_returns() {
    let repo = init_repo();
    let (_t, svc, ws) = svc_for(Some(&repo.dir)).await;
    track(&svc, &ws, "gone.rs", "unstaged").await;

    let away = repo.dir.with_extension("unmounted");
    std::fs::rename(&repo.dir, &away).unwrap();
    let outcome = svc
        .prune_stale_tracked_changes(&ws, &CleanRows::new())
        .await
        .unwrap();
    assert!(outcome.clean.is_empty() && outcome.deleted == 0);
    assert!(svc.worktree_presence().is_missing(&ws));

    std::fs::rename(&away, &repo.dir).unwrap();
    assert_eq!(prune_confirmed(&svc, &ws).await, 1);
    assert!(!svc.worktree_presence().is_missing(&ws));
    assert!(paths(&svc, &ws).await.is_empty());
}
