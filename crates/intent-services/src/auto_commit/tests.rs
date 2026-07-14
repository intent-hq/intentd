//! Tests for the LNI-1 auto-commit-on-idle subscriber.

use std::path::PathBuf;

use git2::{Repository, Signature};
use intent_core::events::AGENT_IDLE;
use intent_core::{
    now_iso, AgentId, AgentSession, AgentStatus, ContentType, Event, EventActor, Note, NoteId,
    NoteMetadata, NoteVisibility, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceId,
    WorkspaceStatus,
};
use intent_store::Store;
use serde_json::json;

use crate::auto_commit::{is_meaningful_agent_name, is_normal_finish_reason, normalize_subject};
use crate::Services;

struct TempDb {
    path: PathBuf,
}

impl TempDb {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("intentd-acommit-{}.db", uuid::Uuid::new_v4()));
        Self { path }
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(PathBuf::from(format!("{}{suffix}", self.path.display())));
        }
    }
}

struct GitRepo {
    dir: PathBuf,
}

impl Drop for GitRepo {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn init_git_repo() -> GitRepo {
    let dir = std::env::temp_dir().join(format!("intentd-ac-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    let repo = Repository::init(&dir).unwrap();
    let mut cfg = repo.config().unwrap();
    cfg.set_str("user.name", "Test").unwrap();
    cfg.set_str("user.email", "test@example.com").unwrap();
    // Seed commit so HEAD exists.
    std::fs::write(dir.join("seed.txt"), "seed\n").unwrap();
    let mut index = repo.index().unwrap();
    index.add_path(std::path::Path::new("seed.txt")).unwrap();
    index.write().unwrap();
    let tree = repo.find_tree(index.write_tree().unwrap()).unwrap();
    let sig = Signature::now("Test", "test@example.com").unwrap();
    repo.commit(Some("HEAD"), &sig, &sig, "seed", &tree, &[])
        .unwrap();
    GitRepo { dir }
}

fn workspace_with_repo(id: &WorkspaceId, repo: &GitRepo) -> Workspace {
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "WS".to_string(),
        branch: "main".to_string(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: ts.clone(),
        updated_at: ts,
        last_activity: None,
        tags: vec![],
        path: None,
        repository_path: None,
        repository_owner: None,
        repository_name: None,
        worktree_path: Some(repo.dir.to_string_lossy().to_string()),
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
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
    }
}

fn task_note(ws: &WorkspaceId, id: &str, title: &str) -> Note {
    let ts = now_iso();
    Note {
        id: NoteId::from(id),
        workspace_id: ws.clone(),
        title: title.to_string(),
        content: format!("# {title}\n"),
        content_type: ContentType::Markdown,
        tags: vec![],
        is_pinned: false,
        is_archived: false,
        is_default: false,
        parent_id: None,
        visibility: NoteVisibility::Workspace,
        metadata: NoteMetadata::default(),
        created_at: ts.clone(),
        updated_at: ts,
        rev: 0,
    }
}

fn session(
    id: &str,
    ws: &WorkspaceId,
    task_note_id: Option<&str>,
    skip_auto_commit: bool,
    name: &str,
    name_explicitly_set: bool,
) -> AgentSession {
    let ts = now_iso();
    AgentSession {
        id: AgentId::from(id),
        workspace_id: ws.clone(),
        parent_agent_id: None,
        backend_session_id: None,
        acp_session_id: None,
        name: name.to_string(),
        name_explicitly_set,
        model: None,
        provider: None,
        system_prompt: None,
        specialist: None,
        status: AgentStatus::Active,
        is_active: true,
        messages: vec![],
        stats: None,
        task_note_id: task_note_id.map(NoteId::from),
        skip_auto_commit,
        completion_report: None,
        completion_report_timestamp: None,
        delegation_depth: None,
        initial_message: None,
        context_references: None,
        image_blocks: None,
        is_background: false,
        metadata: None,
        created_at: ts.clone(),
        updated_at: ts,
        sandbox_id: None,
        sandbox_path: None,
        sandbox_branch: None,
    }
}

fn idle_event(ws: &WorkspaceId, agent: &str, finish_reason: &str) -> Event {
    Event {
        id: uuid::Uuid::new_v4().to_string(),
        workspace_id: ws.clone(),
        timestamp: now_iso(),
        event_type: AGENT_IDLE.to_string(),
        actor: EventActor {
            actor_type: intent_core::ActorType::System,
            id: Some("test".to_string()),
            name: None,
            email: None,
            model: None,
            metadata: None,
        },
        session_id: Some(agent.to_string()),
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: json!({
            "agentId": agent,
            "reason": "stream_complete",
            "finishReason": finish_reason,
            "status": "idle",
        }),
    }
}

async fn setup_dirty_workspace(repo: &GitRepo) -> (TempDb, Services, WorkspaceId) {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws_id = WorkspaceId::new();
    let ws = workspace_with_repo(&ws_id, repo);
    store.insert_workspace(&ws).await.expect("insert ws");
    // Make the worktree dirty so git_agent_commit has something to stage.
    std::fs::write(repo.dir.join("change.txt"), "agent edit\n").unwrap();
    let services = Services::new(store);
    (tmp, services, ws_id)
}

#[test]
fn normalize_subject_collapses_whitespace_and_clamps() {
    let raw = "  Fix\n the\tthing  ";
    assert_eq!(normalize_subject(raw), "Fix the thing");
    let long = "x".repeat(100);
    assert_eq!(normalize_subject(&long).chars().count(), 72);
}

#[test]
fn finish_reason_allowlist() {
    assert!(is_normal_finish_reason(Some("end_turn")));
    assert!(is_normal_finish_reason(Some("max_tokens")));
    assert!(is_normal_finish_reason(Some("max_turn_requests")));
    assert!(is_normal_finish_reason(Some("stream_complete")));
    assert!(!is_normal_finish_reason(Some("cancelled")));
    assert!(!is_normal_finish_reason(Some("refusal")));
    assert!(!is_normal_finish_reason(Some("error")));
    assert!(!is_normal_finish_reason(Some("provider_stopped")));
    assert!(!is_normal_finish_reason(None));
}

#[test]
fn meaningful_name_requires_explicit_set_and_nonempty() {
    let ws = WorkspaceId::from("ws-1");
    let auto = session("agent-a", &ws, None, false, "Agent abc123", false);
    assert!(!is_meaningful_agent_name(&auto));
    let named = session("agent-b", &ws, None, false, "Implementor", true);
    assert!(is_meaningful_agent_name(&named));
    let blank = session("agent-c", &ws, None, false, "   ", true);
    assert!(!is_meaningful_agent_name(&blank));
}

async fn last_commit_trailers(dir: &std::path::Path) -> (Option<String>, Option<String>, String) {
    let commits = intent_git::history::history(dir, 1).unwrap();
    let head = commits.into_iter().next().expect("at least one commit");
    (head.agent_id, head.linked_note_id, head.message)
}

#[tokio::test]
async fn task_linked_idle_commits_with_both_trailers() {
    let repo = init_git_repo();
    let (_tmp, svc, ws_id) = setup_dirty_workspace(&repo).await;
    let note = task_note(&ws_id, "task-1", "Port auto-commit");
    svc.store().insert_note(&note).await.unwrap();
    let agent = session("agent-a1", &ws_id, Some("task-1"), false, "Builder", true);
    svc.store().insert_agent_session(&agent).await.unwrap();
    let event = idle_event(&ws_id, "agent-a1", "end_turn");
    svc.handle_agent_idle_auto_commit(&event).await;

    let (agent_id, linked_note_id, message) = last_commit_trailers(&repo.dir).await;
    assert_eq!(agent_id.as_deref(), Some("agent-a1"));
    assert_eq!(linked_note_id.as_deref(), Some("task-1"));
    // Subject falls back to the task note title.
    assert!(
        message.starts_with("Port auto-commit"),
        "subject: {message}"
    );
}

#[tokio::test]
async fn auto_commit_disabled_setting_is_silent_skip() {
    let repo = init_git_repo();
    let (_tmp, svc, ws_id) = setup_dirty_workspace(&repo).await;
    svc.store()
        .set_setting("git.autoCommit", "false")
        .await
        .unwrap();
    let agent = session("agent-d1", &ws_id, None, false, "X", true);
    svc.store().insert_agent_session(&agent).await.unwrap();
    let event = idle_event(&ws_id, "agent-d1", "end_turn");
    svc.handle_agent_idle_auto_commit(&event).await;
    // No commit beyond the seed.
    let commits = intent_git::history::history(&repo.dir, 5).unwrap();
    assert_eq!(commits.len(), 1, "no new commit when auto-commit disabled");
}

#[tokio::test]
async fn session_skip_auto_commit_is_silent_skip() {
    let repo = init_git_repo();
    let (_tmp, svc, ws_id) = setup_dirty_workspace(&repo).await;
    let agent = session("agent-s1", &ws_id, Some("task-1"), true, "X", true);
    svc.store().insert_agent_session(&agent).await.unwrap();
    let event = idle_event(&ws_id, "agent-s1", "end_turn");
    svc.handle_agent_idle_auto_commit(&event).await;
    let commits = intent_git::history::history(&repo.dir, 5).unwrap();
    assert_eq!(commits.len(), 1);
}

#[tokio::test]
async fn clean_tree_is_silent_skip() {
    let repo = init_git_repo();
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws_id = WorkspaceId::new();
    let ws = workspace_with_repo(&ws_id, &repo);
    store.insert_workspace(&ws).await.unwrap();
    let svc = Services::new(store);
    let agent = session("agent-c1", &ws_id, None, false, "X", true);
    svc.store().insert_agent_session(&agent).await.unwrap();
    let event = idle_event(&ws_id, "agent-c1", "end_turn");
    svc.handle_agent_idle_auto_commit(&event).await;
    let commits = intent_git::history::history(&repo.dir, 5).unwrap();
    assert_eq!(commits.len(), 1);
}

#[tokio::test]
async fn non_task_agent_commits_with_agent_id_only() {
    let repo = init_git_repo();
    let (_tmp, svc, ws_id) = setup_dirty_workspace(&repo).await;
    let agent = session("agent-n1", &ws_id, None, false, "Custom Builder", true);
    svc.store().insert_agent_session(&agent).await.unwrap();
    let event = idle_event(&ws_id, "agent-n1", "end_turn");
    svc.handle_agent_idle_auto_commit(&event).await;
    let (agent_id, linked, message) = last_commit_trailers(&repo.dir).await;
    assert_eq!(agent_id.as_deref(), Some("agent-n1"));
    assert!(
        linked.is_none(),
        "non-task agent must not write Linked-Note-Id"
    );
    assert!(message.starts_with("Custom Builder"), "subject: {message}");
}

#[tokio::test]
async fn non_normal_finish_reason_skips() {
    let repo = init_git_repo();
    let (_tmp, svc, ws_id) = setup_dirty_workspace(&repo).await;
    let agent = session("agent-x1", &ws_id, None, false, "X", true);
    svc.store().insert_agent_session(&agent).await.unwrap();
    let event = idle_event(&ws_id, "agent-x1", "cancelled");
    svc.handle_agent_idle_auto_commit(&event).await;
    let commits = intent_git::history::history(&repo.dir, 5).unwrap();
    assert_eq!(commits.len(), 1);
}

#[tokio::test]
async fn missing_agent_id_event_is_a_no_op() {
    let repo = init_git_repo();
    let (_tmp, svc, ws_id) = setup_dirty_workspace(&repo).await;
    let mut event = idle_event(&ws_id, "agent-x", "end_turn");
    event.data = json!({ "finishReason": "end_turn" });
    svc.handle_agent_idle_auto_commit(&event).await;
    let commits = intent_git::history::history(&repo.dir, 5).unwrap();
    assert_eq!(commits.len(), 1);
}

#[tokio::test]
async fn fallback_subject_uses_default_for_auto_named_non_task_agent() {
    let repo = init_git_repo();
    let (_tmp, svc, ws_id) = setup_dirty_workspace(&repo).await;
    // Auto-generated name pattern → name_explicitly_set = false.
    let agent = session("agent-f1", &ws_id, None, false, "Agent abc123", false);
    svc.store().insert_agent_session(&agent).await.unwrap();
    let event = idle_event(&ws_id, "agent-f1", "end_turn");
    svc.handle_agent_idle_auto_commit(&event).await;
    let (_a, _l, message) = last_commit_trailers(&repo.dir).await;
    assert!(message.starts_with("Agent changes"), "subject: {message}");
}
