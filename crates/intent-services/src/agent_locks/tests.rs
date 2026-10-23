//! Tests for the daemon-owned agent-lock computation (§5.19).

use std::path::PathBuf;

use intent_core::{
    now_iso, AgentId, AgentSession, AgentStatus, ContentType, Note, NoteId, NoteMetadata,
    NoteVisibility, TaskMetadata, TaskStatus, Workspace, WorkspaceActivity, WorkspaceAttention,
    WorkspaceId, WorkspaceStatus,
};
use intent_store::{NewTrackedChange, Store};

use crate::Services;

struct TempDb {
    _dir: tempfile::TempDir,
    path: PathBuf,
}

impl TempDb {
    fn new() -> Self {
        let dir = crate::tests::test_tempdir("intentd-alocks-");
        let path = dir.path().join("store.db");
        Self { _dir: dir, path }
    }
}

fn workspace(id: &WorkspaceId) -> Workspace {
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "WS".to_string(),
        branch: "main".to_string(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        status_image_asset_id: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: ts.clone(),
        updated_at: ts,
        last_activity: None,
        last_content_activity: None,
        tags: vec![],
        path: None,
        repository_path: None,
        repository_owner: None,
        repository_name: None,
        worktree_path: None,
        scope: None,
        skip_worktree: false,
        setup_script: None,
        is_remote: false,
        default_model: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        active_pull_request: None,
        pull_requests: None,
        context_links: None,
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
        cow_supported: None,
        browser_client_id: None,
        pull_requests_total: None,
        display_status: None,
        attention_reminder: None,
        waiting: false,
        scripts_running: false,
        checkout_mode: None,
        disk_usage: None,
        pending_delete_at: None,
        membership: None,
    }
}

pub(crate) fn session(
    id: &str,
    ws: &WorkspaceId,
    status: AgentStatus,
    task: Option<&str>,
) -> AgentSession {
    let ts = now_iso();
    AgentSession {
        harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
        harness_features: None,
        id: AgentId::from(id),
        workspace_id: ws.clone(),
        parent_agent_id: None,
        backend_session_id: None,
        acp_session_id: None,
        name: format!("Agent {id}"),
        name_explicitly_set: false,
        model: None,
        reasoning_effort: None,
        effort_levels: None,
        provider: None,
        system_prompt: None,
        specialist: None,
        status,
        is_active: true,
        messages: vec![],
        stats: None,
        task_note_id: task.map(NoteId::from),
        skip_auto_commit: false,
        completion_report: None,
        completion_report_timestamp: None,
        attention_request_kind: None,
        attention_request_reason: None,
        attention_request_timestamp: None,
        delegation_depth: None,
        initial_message: None,
        context_references: None,
        image_blocks: None,
        file_blocks: None,
        is_background: false,
        metadata: None,
        created_at: ts.clone(),
        updated_at: ts,
        sandbox_id: None,
        sandbox_path: None,
        sandbox_branch: None,
        stop_reason: None,
        stop_reason_timestamp: None,
        session_corrupted: false,
        pending_delete_at: None,
        retired_at: None,
        notifications_muted: false,
    }
}

async fn setup() -> (TempDb, Services, WorkspaceId) {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws_id = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws_id)).await.unwrap();
    let services = Services::new(store);
    (tmp, services, ws_id)
}

async fn track(svc: &Services, ws: &WorkspaceId, agent: &str, path: &str, stage: &str) {
    let change = NewTrackedChange {
        workspace_id: ws.clone(),
        path: path.to_string(),
        stage: stage.to_string(),
        status: "modified".to_string(),
        agent_id: Some(agent.to_string()),
        session_id: None,
        turn: None,
        commit_hash: None,
        old_blob_sha: None,
        new_blob_sha: None,
        additions: 1,
        deletions: 0,
    };
    svc.store().upsert_tracked_change(&change).await.unwrap();
}

#[tokio::test]
async fn auto_commit_disabled_yields_empty_snapshot() {
    let (_tmp, svc, ws) = setup().await;
    svc.store()
        .set_workspace_auto_commit(&ws, false)
        .await
        .unwrap();
    let agent = session("agent-1", &ws, AgentStatus::Active, None);
    svc.store().insert_agent_session(&agent).await.unwrap();
    track(&svc, &ws, "agent-1", "src/a.rs", "unstaged").await;

    let snap = svc.compute_agent_locks(&ws).await;
    assert!(!snap.auto_commit_enabled);
    assert!(snap.locked_agent_ids.is_empty());
    assert!(snap.locked_file_paths.is_empty());
}

#[tokio::test]
async fn active_agent_with_working_changes_is_locked() {
    let (_tmp, svc, ws) = setup().await;
    let agent = session("agent-1", &ws, AgentStatus::Active, None);
    svc.store().insert_agent_session(&agent).await.unwrap();
    track(&svc, &ws, "agent-1", "src/b.rs", "unstaged").await;
    track(&svc, &ws, "agent-1", "src/a.rs", "staged").await;
    // Committed-stage rows never lock.
    track(&svc, &ws, "agent-1", "src/c.rs", "committed").await;

    let snap = svc.compute_agent_locks(&ws).await;
    assert!(snap.auto_commit_enabled, "schema default is enabled");
    assert_eq!(snap.locked_agent_ids, vec!["agent-1".to_string()]);
    assert_eq!(
        snap.locked_file_paths,
        vec!["src/a.rs".to_string(), "src/b.rs".to_string()],
        "sorted, committed stage excluded"
    );
}

#[tokio::test]
async fn idle_agent_without_task_is_not_locked() {
    let (_tmp, svc, ws) = setup().await;
    let agent = session("agent-1", &ws, AgentStatus::RuntimeIdle, None);
    svc.store().insert_agent_session(&agent).await.unwrap();
    track(&svc, &ws, "agent-1", "src/a.rs", "unstaged").await;

    let snap = svc.compute_agent_locks(&ws).await;
    assert!(snap.locked_agent_ids.is_empty());
    assert!(snap.locked_file_paths.is_empty());
}

#[tokio::test]
async fn idle_agent_with_open_task_is_locked() {
    let (_tmp, svc, ws) = setup().await;
    svc.store()
        .insert_note(&task_note(&ws, "note-1", TaskStatus::InProgress))
        .await
        .unwrap();
    let agent = session("agent-1", &ws, AgentStatus::RuntimeIdle, Some("note-1"));
    svc.store().insert_agent_session(&agent).await.unwrap();
    track(&svc, &ws, "agent-1", "src/a.rs", "unstaged").await;

    let snap = svc.compute_agent_locks(&ws).await;
    assert_eq!(snap.locked_agent_ids, vec!["agent-1".to_string()]);
    assert_eq!(snap.locked_file_paths, vec!["src/a.rs".to_string()]);
}

#[tokio::test]
async fn idle_agent_with_terminal_task_is_not_locked() {
    let (_tmp, svc, ws) = setup().await;
    for (note_id, status) in [
        ("note-c", TaskStatus::Complete),
        ("note-x", TaskStatus::Cancelled),
    ] {
        svc.store()
            .insert_note(&task_note(&ws, note_id, status))
            .await
            .unwrap();
    }
    let a1 = session("agent-1", &ws, AgentStatus::RuntimeIdle, Some("note-c"));
    let a2 = session("agent-2", &ws, AgentStatus::Idle, Some("note-x"));
    svc.store().insert_agent_session(&a1).await.unwrap();
    svc.store().insert_agent_session(&a2).await.unwrap();
    track(&svc, &ws, "agent-1", "src/a.rs", "unstaged").await;
    track(&svc, &ws, "agent-2", "src/b.rs", "staged").await;

    let snap = svc.compute_agent_locks(&ws).await;
    assert!(snap.locked_agent_ids.is_empty());
    assert!(snap.locked_file_paths.is_empty());
}

#[tokio::test]
async fn mixed_agents_lock_only_active_ones_paths() {
    let (_tmp, svc, ws) = setup().await;
    let a1 = session("agent-1", &ws, AgentStatus::Active, None);
    let a2 = session("agent-2", &ws, AgentStatus::Completed, None);
    svc.store().insert_agent_session(&a1).await.unwrap();
    svc.store().insert_agent_session(&a2).await.unwrap();
    track(&svc, &ws, "agent-1", "src/live.rs", "unstaged").await;
    track(&svc, &ws, "agent-2", "src/done.rs", "unstaged").await;
    // Unattributed row never locks.
    let unattributed = NewTrackedChange {
        workspace_id: ws.clone(),
        path: "src/user.rs".to_string(),
        stage: "unstaged".to_string(),
        status: "modified".to_string(),
        agent_id: None,
        session_id: None,
        turn: None,
        commit_hash: None,
        old_blob_sha: None,
        new_blob_sha: None,
        additions: 1,
        deletions: 0,
    };
    svc.store()
        .upsert_tracked_change(&unattributed)
        .await
        .unwrap();

    let snap = svc.compute_agent_locks(&ws).await;
    assert_eq!(snap.locked_agent_ids, vec!["agent-1".to_string()]);
    assert_eq!(snap.locked_file_paths, vec!["src/live.rs".to_string()]);
}

#[tokio::test]
async fn retired_and_deleted_agents_never_lock() {
    let (_tmp, svc, ws) = setup().await;
    let mut retired = session("agent-1", &ws, AgentStatus::Active, None);
    retired.retired_at = Some(now_iso());
    let deleted = session("agent-2", &ws, AgentStatus::Deleted, None);
    svc.store().insert_agent_session(&retired).await.unwrap();
    svc.store().insert_agent_session(&deleted).await.unwrap();
    track(&svc, &ws, "agent-1", "src/a.rs", "unstaged").await;
    track(&svc, &ws, "agent-2", "src/b.rs", "unstaged").await;

    let snap = svc.compute_agent_locks(&ws).await;
    assert!(snap.locked_agent_ids.is_empty());
    assert!(snap.locked_file_paths.is_empty());
}

#[tokio::test]
async fn get_agent_locks_wire_shape() {
    let (_tmp, svc, ws) = setup().await;
    let agent = session("agent-1", &ws, AgentStatus::Active, None);
    svc.store().insert_agent_session(&agent).await.unwrap();
    track(&svc, &ws, "agent-1", "src/a.rs", "unstaged").await;

    let snap = svc.compute_agent_locks(&ws).await;
    let v = snap.to_result_value();
    assert_eq!(v["autoCommitEnabled"], serde_json::json!(true));
    assert_eq!(v["lockedAgentIds"], serde_json::json!(["agent-1"]));
    assert_eq!(v["lockedFilePaths"], serde_json::json!(["src/a.rs"]));
}

/// The subscriber loop reacts to `agent:retired` / `agent:restored` — the
/// only events soft retire/restore emit (§5.5): retiring a locked agent
/// publishes the unlocked snapshot, restoring it publishes the relock.
#[tokio::test]
async fn loop_reacts_to_retire_and_restore() {
    let (_tmp, svc, ws) = setup().await;
    let bus = crate::EventBus::new(svc.store().clone());
    let svc = svc.with_event_bus(bus.clone());
    let agent = session("agent-1", &ws, AgentStatus::Active, None);
    svc.store().insert_agent_session(&agent).await.unwrap();
    track(&svc, &ws, "agent-1", "src/a.rs", "unstaged").await;

    let handle = svc.spawn_agent_locks_loop();
    let mut sub = bus.subscribe(crate::SubscriptionFilter {
        workspace_id: Some(ws.0.clone()),
        event_types: vec![intent_core::events::CHANGES_AGENT_LOCKS.to_string()],
        ..Default::default()
    });

    // Retire: flip retired_at in the store, then emit the lifecycle event —
    // exactly what retire_one_session does (no status-changed rides along).
    let now = now_iso();
    svc.store()
        .set_agent_session_retired_at(&ws, &AgentId::from("agent-1"), Some(&now), &now)
        .await
        .unwrap();
    publish_lifecycle(&bus, &ws, intent_core::events::AGENT_RETIRED, "agent-1").await;
    let ev = recv_locks_event(&mut sub).await;
    assert_eq!(ev["data"]["lockedAgentIds"], serde_json::json!([]));
    assert_eq!(ev["data"]["lockedFilePaths"], serde_json::json!([]));

    // Restore: clear the mark, emit agent:restored — the agent relocks.
    let now = now_iso();
    svc.store()
        .set_agent_session_retired_at(&ws, &AgentId::from("agent-1"), None, &now)
        .await
        .unwrap();
    publish_lifecycle(&bus, &ws, intent_core::events::AGENT_RESTORED, "agent-1").await;
    let ev = recv_locks_event(&mut sub).await;
    assert_eq!(ev["data"]["lockedAgentIds"], serde_json::json!(["agent-1"]));
    assert_eq!(
        ev["data"]["lockedFilePaths"],
        serde_json::json!(["src/a.rs"])
    );

    handle.abort();
}

async fn publish_lifecycle(bus: &crate::EventBus, ws: &WorkspaceId, event_type: &str, agent: &str) {
    bus.publish(&intent_store::NewEvent {
        workspace_id: ws.clone(),
        timestamp: now_iso(),
        event_type: event_type.to_string(),
        actor: crate::system_actor(),
        session_id: Some(agent.to_string()),
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: serde_json::json!({ "agentId": agent }),
    })
    .await
    .unwrap();
}

async fn recv_locks_event(sub: &mut crate::Subscription) -> serde_json::Value {
    let batch = tokio::time::timeout(
        intentd_test_support::test_timeout(std::time::Duration::from_secs(5)),
        sub.recv(),
    )
    .await
    .expect("changes:agent-locks delivered")
    .expect("subscription open");
    assert_eq!(batch.len(), 1, "expected exactly one lock event");
    serde_json::to_value(&batch[0]).expect("serialize event")
}

fn task_note(ws: &WorkspaceId, id: &str, status: TaskStatus) -> Note {
    let ts = now_iso();
    Note {
        id: NoteId::from(id),
        workspace_id: ws.clone(),
        title: format!("Task {id}"),
        content: "# Task\n".to_string(),
        content_type: ContentType::Markdown,
        tags: vec![],
        is_pinned: false,
        is_archived: false,
        is_default: false,
        parent_id: None,
        visibility: NoteVisibility::Workspace,
        metadata: NoteMetadata {
            task: Some(TaskMetadata {
                status,
                ..Default::default()
            }),
        },
        created_at: ts.clone(),
        updated_at: ts,
        rev: 0,
    }
}

/// A git worktree for the lock-filter tests, seeded with one commit of
/// `files` (added through the index, like `git add -f`).
fn git_worktree(files: &[(&str, &str)]) -> tempfile::TempDir {
    let dir = crate::tests::test_tempdir("intentd-alocks-git-");
    let repo = git2::Repository::init(dir.path()).unwrap();
    let mut index = repo.index().unwrap();
    for (rel, contents) in files {
        let full = dir.path().join(rel);
        std::fs::create_dir_all(full.parent().unwrap()).unwrap();
        std::fs::write(&full, contents).unwrap();
        index.add_path(std::path::Path::new(rel)).unwrap();
    }
    index.write().unwrap();
    let tree = repo.find_tree(index.write_tree().unwrap()).unwrap();
    let sig = git2::Signature::now("Test", "test@example.com").unwrap();
    repo.commit(Some("HEAD"), &sig, &sig, "seed", &tree, &[])
        .unwrap();
    dir
}

async fn setup_with_worktree(worktree: &std::path::Path) -> (TempDb, Services, WorkspaceId) {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws_id = WorkspaceId::new();
    let mut ws = workspace(&ws_id);
    ws.worktree_path = Some(worktree.to_string_lossy().to_string());
    store.insert_workspace(&ws).await.unwrap();
    (tmp, Services::new(store), ws_id)
}

/// Regression (intent-hq/intent#7266): only paths git still reports as
/// changes lock — committed/reverted, ignored, and never-existing paths do
/// not, and an agent whose rows are all such paths is not locked. The old
/// side of a rename (folded into the new path by the rename-detecting
/// status) still locks.
#[tokio::test]
async fn only_git_dirty_paths_lock() {
    let dir = git_worktree(&[
        (".gitignore", ".dev/\n"),
        ("src/edited.rs", "a\n"),
        ("src/clean.rs", "c\n"),
        (
            "src/old.rs",
            "fn old() { /* body long enough to match */ }\n",
        ),
    ]);
    std::fs::write(dir.path().join("src/edited.rs"), "b\n").unwrap();
    std::fs::write(dir.path().join("src/untracked.rs"), "u\n").unwrap();
    std::fs::create_dir_all(dir.path().join(".dev")).unwrap();
    std::fs::write(dir.path().join(".dev/scratch.json"), "{}").unwrap();
    std::fs::rename(dir.path().join("src/old.rs"), dir.path().join("src/new.rs")).unwrap();
    let (_tmp, svc, ws) = setup_with_worktree(dir.path()).await;
    let busy = session("agent-1", &ws, AgentStatus::Active, None);
    let stale_only = session("agent-2", &ws, AgentStatus::Active, None);
    svc.store().insert_agent_session(&busy).await.unwrap();
    svc.store().insert_agent_session(&stale_only).await.unwrap();
    for path in [
        "src/edited.rs",
        "src/untracked.rs",
        "src/clean.rs",
        "src/old.rs",
        "src/new.rs",
        ".dev/scratch.json",
    ] {
        track(&svc, &ws, "agent-1", path, "unstaged").await;
    }
    track(&svc, &ws, "agent-2", ".dev/other.json", "unstaged").await;
    track(&svc, &ws, "agent-2", "src/clean.rs", "staged").await;

    let snap = svc.compute_agent_locks(&ws).await;
    assert_eq!(snap.locked_agent_ids, vec!["agent-1".to_string()]);
    assert_eq!(
        snap.locked_file_paths,
        vec![
            "src/edited.rs".to_string(),
            "src/new.rs".to_string(),
            "src/old.rs".to_string(),
            "src/untracked.rs".to_string(),
        ]
    );
}

/// Paths inside a submodule are invisible to the superproject status (it
/// lists only the gitlink), so they keep locking.
#[tokio::test]
async fn submodule_internal_paths_still_lock() {
    let child = git_worktree(&[("lib.rs", "x\n")]);
    let parent = git_worktree(&[("README.md", "r\n")]);
    {
        let repo = git2::Repository::open(parent.path()).unwrap();
        let url = child.path().to_string_lossy().to_string();
        let mut sm = repo
            .submodule(&url, std::path::Path::new("sub"), true)
            .unwrap();
        let _ = std::fs::remove_dir_all(parent.path().join("sub"));
        git2::Repository::clone(&url, parent.path().join("sub")).unwrap();
        sm.add_to_index(true).unwrap();
        sm.add_finalize().unwrap();
    }
    std::fs::write(parent.path().join("sub/lib.rs"), "changed\n").unwrap();
    let (_tmp, svc, ws) = setup_with_worktree(parent.path()).await;
    let agent = session("agent-1", &ws, AgentStatus::Active, None);
    svc.store().insert_agent_session(&agent).await.unwrap();
    track(&svc, &ws, "agent-1", "sub/lib.rs", "unstaged").await;

    let snap = svc.compute_agent_locks(&ws).await;
    assert_eq!(snap.locked_file_paths, vec!["sub/lib.rs".to_string()]);
}

/// A worktree whose status cannot be read keeps the unfiltered behaviour.
#[tokio::test]
async fn unreadable_worktree_falls_back_to_unfiltered_locks() {
    let not_a_repo = crate::tests::test_tempdir("intentd-alocks-norepo-");
    let (_tmp, svc, ws) = setup_with_worktree(not_a_repo.path()).await;
    let agent = session("agent-1", &ws, AgentStatus::Active, None);
    svc.store().insert_agent_session(&agent).await.unwrap();
    track(&svc, &ws, "agent-1", "src/a.rs", "unstaged").await;

    let snap = svc.compute_agent_locks(&ws).await;
    assert_eq!(snap.locked_file_paths, vec!["src/a.rs".to_string()]);
}
