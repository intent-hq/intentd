//! Tests for the LNI-1 auto-commit-on-idle subscriber.

use std::path::PathBuf;

use git2::{Repository, Signature};
use intent_core::events::AGENT_IDLE;
use intent_core::{
    now_iso, AgentId, AgentSession, AgentStatus, ContentType, Event, EventActor, Note, NoteId,
    NoteMetadata, NoteVisibility, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceId,
    WorkspaceStatus,
};
use intent_store::{NewTrackedChange, Store};
use serde_json::json;

use crate::auto_commit::{
    is_meaningful_agent_name, is_normal_finish_reason, normalize_subject, parse_commit_message_json,
};
use crate::Services;

/// RAII temp `SQLite` store: the db (and its `-wal`/`-shm` sidecars) live in a
/// guarded temp dir removed on drop — including on panic — unless
/// `INTENTD_TEST_KEEP_TMP` (non-empty) is set.
struct TempDb {
    _dir: tempfile::TempDir,
    path: PathBuf,
}

impl TempDb {
    fn new() -> Self {
        let dir = crate::tests::test_tempdir("intentd-acommit-");
        let path = dir.path().join("store.db");
        Self { _dir: dir, path }
    }
}

/// RAII git repo in a guarded temp dir (removed on drop, including on panic,
/// unless `INTENTD_TEST_KEEP_TMP` is set). `dir` mirrors the guard's path so
/// call sites keep reading `repo.dir`.
struct GitRepo {
    _guard: tempfile::TempDir,
    dir: PathBuf,
}

fn init_git_repo() -> GitRepo {
    let guard = crate::tests::test_tempdir("intentd-ac-");
    let dir = guard.path().to_path_buf();
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
    GitRepo { _guard: guard, dir }
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
        status_image_asset_id: None,
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
        cow_supported: None,
        display_status: None,
        waiting: false,
        checkout_mode: None,
        execution_environment: None,
        disk_usage: None,
        pending_delete_at: None,
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
        harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
        harness_features: None,
        id: AgentId::from(id),
        workspace_id: ws.clone(),
        parent_agent_id: None,
        backend_session_id: None,
        acp_session_id: None,
        name: name.to_string(),
        name_explicitly_set,
        model: None,
        reasoning_effort: None,
        effort_levels: None,
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
    // Inject a missing auggie path so generation falls back to the deterministic
    // subject, preserving pre-LLM test semantics.
    let services =
        Services::new(store).with_auggie_bin(PathBuf::from("/nonexistent/intentd-test/auggie"));
    (tmp, services, ws_id)
}

/// Attribute `change.txt` (the dirty file from `setup_dirty_workspace`) to
/// `agent`: `git_agent_commit`'s no-files fallback commits only the paths the
/// tracked-changes pipeline attributes to the committing agent (monorepo#939),
/// so idle auto-commit tests that expect a commit must record attribution.
async fn attribute_dirty_change(svc: &Services, ws: &WorkspaceId, agent: &str) {
    let change = NewTrackedChange {
        workspace_id: ws.clone(),
        path: "change.txt".to_string(),
        stage: "unstaged".to_string(),
        status: "added".to_string(),
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

fn last_commit_trailers(dir: &std::path::Path) -> (Option<String>, Option<String>, String) {
    let commits = intent_git::history::history(dir, 1).unwrap();
    let head = commits.into_iter().next().expect("at least one commit");
    (head.agent_id, head.linked_note_id, head.message)
}

/// Registry with `providers.active = "auggie"` so the completeOnce provider
/// gate is open: unset settings resolve the gate CLOSED, so generation tests
/// that expect the CLI to be reached must opt in explicitly.
#[cfg(unix)]
fn auggie_active_registry() -> (tempfile::TempDir, std::sync::Arc<crate::SettingsRegistry>) {
    let config_dir = tempfile::tempdir().expect("temp config dir");
    let registry = std::sync::Arc::new(
        crate::SettingsRegistry::load(config_dir.path().join("config.toml"))
            .expect("load registry"),
    );
    registry
        .apply(&[("providers.active".to_string(), json!("auggie"))])
        .expect("set providers.active");
    (config_dir, registry)
}

/// Fake auggie CLI inside an RAII temp dir; keep the returned guard alive for
/// the duration of the test (dropping it removes the dir).
#[cfg(unix)]
fn fake_auggie(tag: &str, body: &str) -> (tempfile::TempDir, PathBuf) {
    use std::os::unix::fs::PermissionsExt;
    let dir = crate::tests::test_tempdir(&format!("intentd-acommit-{tag}-"));
    let bin = dir.path().join("auggie");
    std::fs::write(&bin, format!("#!/bin/sh\ncat > /dev/null\n{body}\n")).unwrap();
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    (dir, bin)
}

#[tokio::test]
async fn task_linked_idle_commits_with_both_trailers() {
    let repo = init_git_repo();
    let (_tmp, svc, ws_id) = setup_dirty_workspace(&repo).await;
    let note = task_note(&ws_id, "task-1", "Port auto-commit");
    svc.store().insert_note(&note).await.unwrap();
    let agent = session("agent-a1", &ws_id, Some("task-1"), false, "Builder", true);
    svc.store().insert_agent_session(&agent).await.unwrap();
    attribute_dirty_change(&svc, &ws_id, "agent-a1").await;
    let event = idle_event(&ws_id, "agent-a1", "end_turn");
    svc.handle_agent_idle_auto_commit(&event).await;

    let (agent_id, linked_note_id, message) = last_commit_trailers(&repo.dir);
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
    let config_dir = tempfile::tempdir().expect("temp config dir");
    let registry = std::sync::Arc::new(
        crate::SettingsRegistry::load(config_dir.path().join("config.toml"))
            .expect("load registry"),
    );
    registry
        .apply(&[("git.autoCommit".to_string(), serde_json::json!(false))])
        .unwrap();
    let svc = svc.with_settings_registry(registry);
    let agent = session("agent-d1", &ws_id, None, false, "X", true);
    svc.store().insert_agent_session(&agent).await.unwrap();
    let event = idle_event(&ws_id, "agent-d1", "end_turn");
    svc.handle_agent_idle_auto_commit(&event).await;
    // No commit beyond the seed.
    let commits = intent_git::history::history(&repo.dir, 5).unwrap();
    assert_eq!(commits.len(), 1, "no new commit when auto-commit disabled");
}

#[tokio::test]
async fn workspace_override_disabled_is_silent_skip() {
    // Global git.autoCommit stays at its default (true); the persisted
    // per-workspace override (false) must win at the idle-commit gate.
    let repo = init_git_repo();
    let (_tmp, svc, ws_id) = setup_dirty_workspace(&repo).await;
    svc.store()
        .set_workspace_auto_commit(&ws_id, false)
        .await
        .unwrap();
    let agent = session("agent-w1", &ws_id, None, false, "X", true);
    svc.store().insert_agent_session(&agent).await.unwrap();
    attribute_dirty_change(&svc, &ws_id, "agent-w1").await;
    let event = idle_event(&ws_id, "agent-w1", "end_turn");
    svc.handle_agent_idle_auto_commit(&event).await;
    let commits = intent_git::history::history(&repo.dir, 5).unwrap();
    assert_eq!(
        commits.len(),
        1,
        "workspace override=false blocks the commit"
    );
}

#[tokio::test]
async fn workspace_override_enabled_beats_global_disabled() {
    // Global git.autoCommit=false, workspace override=true → commit proceeds.
    let repo = init_git_repo();
    let (_tmp, svc, ws_id) = setup_dirty_workspace(&repo).await;
    let config_dir = tempfile::tempdir().expect("temp config dir");
    let registry = std::sync::Arc::new(
        crate::SettingsRegistry::load(config_dir.path().join("config.toml"))
            .expect("load registry"),
    );
    registry
        .apply(&[("git.autoCommit".to_string(), serde_json::json!(false))])
        .unwrap();
    let svc = svc.with_settings_registry(registry);
    svc.store()
        .set_workspace_auto_commit(&ws_id, true)
        .await
        .unwrap();
    let agent = session("agent-w2", &ws_id, None, false, "Override Agent", true);
    svc.store().insert_agent_session(&agent).await.unwrap();
    attribute_dirty_change(&svc, &ws_id, "agent-w2").await;
    let event = idle_event(&ws_id, "agent-w2", "end_turn");
    svc.handle_agent_idle_auto_commit(&event).await;
    let (agent_id, _, _) = last_commit_trailers(&repo.dir);
    assert_eq!(
        agent_id.as_deref(),
        Some("agent-w2"),
        "workspace override=true must beat global=false"
    );
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
    attribute_dirty_change(&svc, &ws_id, "agent-n1").await;
    let event = idle_event(&ws_id, "agent-n1", "end_turn");
    svc.handle_agent_idle_auto_commit(&event).await;
    let (agent_id, linked, message) = last_commit_trailers(&repo.dir);
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
    attribute_dirty_change(&svc, &ws_id, "agent-f1").await;
    let event = idle_event(&ws_id, "agent-f1", "end_turn");
    svc.handle_agent_idle_auto_commit(&event).await;
    let (_a, _l, message) = last_commit_trailers(&repo.dir);
    assert!(message.starts_with("Agent changes"), "subject: {message}");
}

#[tokio::test]
async fn idle_auto_commit_does_not_sweep_unattributed_changes() {
    // monorepo#939 regression: the idle auto-commit path must only commit the
    // paths attributed to the idle agent — another actor's dirty file stays
    // in the worktree.
    let repo = init_git_repo();
    let (_tmp, svc, ws_id) = setup_dirty_workspace(&repo).await;
    std::fs::write(repo.dir.join("unattributed.txt"), "someone else\n").unwrap();
    let agent = session("agent-u1", &ws_id, None, false, "Scoped Agent", true);
    svc.store().insert_agent_session(&agent).await.unwrap();
    attribute_dirty_change(&svc, &ws_id, "agent-u1").await;
    let event = idle_event(&ws_id, "agent-u1", "end_turn");
    svc.handle_agent_idle_auto_commit(&event).await;

    let commits = intent_git::history::history(&repo.dir, 5).unwrap();
    assert_eq!(commits.len(), 2, "exactly one auto-commit landed");
    let head = &commits[0];
    assert_eq!(
        head.files.as_deref(),
        Some(&["change.txt".to_string()][..]),
        "only the attributed path was committed"
    );
    assert!(
        repo.dir.join("unattributed.txt").exists(),
        "unattributed file still on disk"
    );
    let status = intent_git::status::status(&repo.dir).unwrap();
    let status = serde_json::to_value(&status).unwrap();
    let files = status["files"].as_array().unwrap();
    assert!(
        files.iter().any(|f| f["path"] == json!("unattributed.txt")),
        "unattributed file still dirty after auto-commit: {files:?}"
    );
}

#[tokio::test]
async fn idle_auto_commit_with_no_attributed_paths_is_silent_skip() {
    // Dirty worktree but zero tracked-change rows for the idle agent: the
    // attribution-filtered fallback yields an empty commit set, which the
    // subscriber treats as a silent skip (no commit, no sweep).
    let repo = init_git_repo();
    let (_tmp, svc, ws_id) = setup_dirty_workspace(&repo).await;
    let agent = session("agent-e1", &ws_id, None, false, "Empty Agent", true);
    svc.store().insert_agent_session(&agent).await.unwrap();
    // No attribute_dirty_change() call — nothing is attributed to agent-e1.
    let event = idle_event(&ws_id, "agent-e1", "end_turn");
    svc.handle_agent_idle_auto_commit(&event).await;
    let commits = intent_git::history::history(&repo.dir, 5).unwrap();
    assert_eq!(commits.len(), 1, "no commit beyond the seed");
    let status = intent_git::status::status(&repo.dir).unwrap();
    let status = serde_json::to_value(&status).unwrap();
    let files = status["files"].as_array().unwrap();
    assert!(
        files.iter().any(|f| f["path"] == json!("change.txt")),
        "dirty file untouched by the skip: {files:?}"
    );
}

#[tokio::test]
async fn idle_auto_commit_ignores_other_agents_attribution() {
    // Attribution rows exist, but for a different agent: the idle agent's
    // attributed set is still empty, so nothing is committed.
    let repo = init_git_repo();
    let (_tmp, svc, ws_id) = setup_dirty_workspace(&repo).await;
    let agent = session("agent-o1", &ws_id, None, false, "Other Agent", true);
    svc.store().insert_agent_session(&agent).await.unwrap();
    attribute_dirty_change(&svc, &ws_id, "agent-somebody-else").await;
    let event = idle_event(&ws_id, "agent-o1", "end_turn");
    svc.handle_agent_idle_auto_commit(&event).await;
    let commits = intent_git::history::history(&repo.dir, 5).unwrap();
    assert_eq!(commits.len(), 1, "no commit beyond the seed");
}

#[test]
fn parse_commit_message_accepts_clean_json() {
    let parsed = parse_commit_message_json(r#"{"subject": "feat: add feature"}"#).unwrap();
    assert_eq!(parsed, "feat: add feature");
}

#[test]
fn parse_commit_message_accepts_fenced_json() {
    let output = "```json\n{\"subject\": \"fix: repair parser\"}\n```";
    assert_eq!(
        parse_commit_message_json(output).unwrap(),
        "fix: repair parser"
    );
    let bare_fence = "```\n{\"subject\": \"fix: repair parser\"}\n```";
    assert_eq!(
        parse_commit_message_json(bare_fence).unwrap(),
        "fix: repair parser"
    );
}

#[test]
fn parse_commit_message_accepts_json_with_surrounding_prose() {
    let output = "Here is the commit message:\n{\"subject\": \"chore: tidy up\"}\nHope that helps!";
    assert_eq!(parse_commit_message_json(output).unwrap(), "chore: tidy up");
}

#[test]
fn parse_commit_message_skips_brace_blob_before_valid_object() {
    // A `{...}` blob in leading prose — whether malformed or a parseable
    // object with a blank subject — must not mask a later valid object.
    let malformed_first = r#"Sure {ok}: {"subject": "feat: x"}"#;
    assert_eq!(
        parse_commit_message_json(malformed_first).unwrap(),
        "feat: x"
    );
    let empty_subject_first = r#"{"subject": ""} then the answer {"subject": "feat: y"}"#;
    assert_eq!(
        parse_commit_message_json(empty_subject_first).unwrap(),
        "feat: y"
    );
}

#[test]
fn parse_commit_message_first_valid_object_wins() {
    let output = r#"{"subject": "feat: first"} {"subject": "feat: second"}"#;
    assert_eq!(parse_commit_message_json(output).unwrap(), "feat: first");
}

#[test]
fn parse_commit_message_composes_multiline_body() {
    // A body with escaped `\n` sequences composes into real newlines.
    let output = r#"{"subject": "feat: add retry", "body": "Adds retry.\n\n- backoff\n- jitter"}"#;
    assert_eq!(
        parse_commit_message_json(output).unwrap(),
        "feat: add retry\n\nAdds retry.\n\n- backoff\n- jitter"
    );
}

#[test]
fn parse_commit_message_composes_subject_and_body() {
    let output = r#"{"subject": "feat: add feature", "body": "Explains what and why."}"#;
    assert_eq!(
        parse_commit_message_json(output).unwrap(),
        "feat: add feature\n\nExplains what and why."
    );
    // Null / empty / whitespace-only bodies compose to the bare subject.
    assert_eq!(
        parse_commit_message_json(r#"{"subject": "feat: solo", "body": null}"#).unwrap(),
        "feat: solo"
    );
    assert_eq!(
        parse_commit_message_json(r#"{"subject": "feat: solo", "body": "   "}"#).unwrap(),
        "feat: solo"
    );
}

#[test]
fn parse_commit_message_rejects_missing_or_empty_subject() {
    assert!(parse_commit_message_json(r#"{"body": "no subject here"}"#).is_none());
    assert!(parse_commit_message_json(r#"{"subject": ""}"#).is_none());
    assert!(parse_commit_message_json(r#"{"subject": "   "}"#).is_none());
}

#[test]
fn parse_commit_message_rejects_non_json_output() {
    assert!(parse_commit_message_json("no json here").is_none());
    assert!(parse_commit_message_json("{\"subject\": \"feat: unterminated").is_none());
    assert!(parse_commit_message_json("feat: bare commit message").is_none());
}

#[test]
fn parse_commit_message_rejects_empty_output() {
    assert!(parse_commit_message_json("").is_none());
    assert!(parse_commit_message_json("   \n  ").is_none());
}

#[cfg(unix)]
#[tokio::test]
async fn generated_message_replaces_fallback_subject() {
    let repo = init_git_repo();
    let (_tmp, svc, ws_id) = setup_dirty_workspace(&repo).await;
    let (_bin_dir, bin) = fake_auggie(
        "gen-ok",
        r#"printf '{"subject": "feat: implement auto-commit"}'"#,
    );
    let (_config_dir, registry) = auggie_active_registry();
    let svc = svc.with_auggie_bin(bin).with_settings_registry(registry);
    let agent = session("agent-g1", &ws_id, None, false, "Builder", true);
    svc.store().insert_agent_session(&agent).await.unwrap();
    attribute_dirty_change(&svc, &ws_id, "agent-g1").await;
    let event = idle_event(&ws_id, "agent-g1", "end_turn");
    svc.handle_agent_idle_auto_commit(&event).await;
    let (_a, _l, message) = last_commit_trailers(&repo.dir);
    assert!(
        message.starts_with("feat: implement auto-commit"),
        "got: {message}"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn generation_uses_commit_quick_action_override() {
    // monorepo#1734: the auto-commit path calls agent.completeOnce with
    // `type: "commit"`, so the user's commit quick-action override reaches
    // the CLI. The fake auggie echoes its argv into the generated subject.
    let repo = init_git_repo();
    let (_tmp, svc, ws_id) = setup_dirty_workspace(&repo).await;
    // The argv includes `--mcp-config {"mcpServers":{}}`, so strip double
    // quotes before embedding it in the JSON string to keep the reply valid.
    let (_bin_dir, bin) = fake_auggie(
        "gen-quick-action",
        r#"args=$(printf '%s' "$*" | tr -d '"')
printf '{"subject": "feat: %s"}' "$args""#,
    );
    let (_config_dir, registry) = auggie_active_registry();
    registry
        .apply(&[(
            "quickActions.typeOverrides".to_string(),
            json!({ "commit": "haiku4.5" }),
        )])
        .expect("set commit override");
    let svc = svc.with_auggie_bin(bin).with_settings_registry(registry);
    let agent = session("agent-q1", &ws_id, None, false, "Builder", true);
    svc.store().insert_agent_session(&agent).await.unwrap();
    attribute_dirty_change(&svc, &ws_id, "agent-q1").await;
    let event = idle_event(&ws_id, "agent-q1", "end_turn");
    svc.handle_agent_idle_auto_commit(&event).await;
    let (_a, _l, message) = last_commit_trailers(&repo.dir);
    assert!(
        message.contains("--model haiku4.5"),
        "the commit quick-action override must reach the CLI, got: {message}"
    );
}

#[cfg(unix)]
#[tokio::test]
async fn generation_timeout_falls_back_to_subject() {
    let repo = init_git_repo();
    let (_tmp, svc, ws_id) = setup_dirty_workspace(&repo).await;
    let (_bin_dir, bin) = fake_auggie("timeout", "sleep 60");
    // Compressed generation budget so the timeout-fallback path runs in
    // milliseconds; the hung CLI is group-reaped when the budget elapses.
    let (_config_dir, registry) = auggie_active_registry();
    let svc = svc
        .with_auggie_bin(bin)
        .with_settings_registry(registry)
        .with_auto_commit_timeout_ms(250);
    let agent = session("agent-t1", &ws_id, None, false, "Timeout Agent", true);
    svc.store().insert_agent_session(&agent).await.unwrap();
    attribute_dirty_change(&svc, &ws_id, "agent-t1").await;
    let event = idle_event(&ws_id, "agent-t1", "end_turn");
    svc.handle_agent_idle_auto_commit(&event).await;
    let (_a, _l, message) = last_commit_trailers(&repo.dir);
    // Fell back to the agent name.
    assert!(message.starts_with("Timeout Agent"), "got: {message}");
}

#[cfg(unix)]
#[tokio::test]
async fn malformed_output_falls_back_to_subject() {
    let repo = init_git_repo();
    let (_tmp, svc, ws_id) = setup_dirty_workspace(&repo).await;
    let (_bin_dir, bin) = fake_auggie("malformed", "printf 'no json at all'");
    let (_config_dir, registry) = auggie_active_registry();
    let svc = svc.with_auggie_bin(bin).with_settings_registry(registry);
    let agent = session("agent-m1", &ws_id, None, false, "Malformed Agent", true);
    svc.store().insert_agent_session(&agent).await.unwrap();
    attribute_dirty_change(&svc, &ws_id, "agent-m1").await;
    let event = idle_event(&ws_id, "agent-m1", "end_turn");
    svc.handle_agent_idle_auto_commit(&event).await;
    let (_a, _l, message) = last_commit_trailers(&repo.dir);
    assert!(message.starts_with("Malformed Agent"), "got: {message}");
}

#[cfg(unix)]
#[tokio::test]
async fn no_changes_skips_generation_and_commit() {
    let repo = init_git_repo();
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws_id = WorkspaceId::new();
    let ws = workspace_with_repo(&ws_id, &repo);
    store.insert_workspace(&ws).await.unwrap();
    // No dirty files — the CLI should not be spawned.
    let (_bin_dir, bin) = fake_auggie("nochanges", "exit 99");
    let svc = Services::new(store).with_auggie_bin(bin);
    let agent = session("agent-n1", &ws_id, None, false, "X", true);
    svc.store().insert_agent_session(&agent).await.unwrap();
    let event = idle_event(&ws_id, "agent-n1", "end_turn");
    svc.handle_agent_idle_auto_commit(&event).await;
    let commits = intent_git::history::history(&repo.dir, 5).unwrap();
    // Only the seed commit exists; auggie exit 99 never ran.
    assert_eq!(commits.len(), 1);
}

#[cfg(unix)]
#[tokio::test]
async fn generated_message_preserves_trailers() {
    let repo = init_git_repo();
    let (_tmp, svc, ws_id) = setup_dirty_workspace(&repo).await;
    let note = task_note(&ws_id, "task-gen", "LLM commit task");
    svc.store().insert_note(&note).await.unwrap();
    let (_bin_dir, bin) = fake_auggie(
        "trailers",
        r#"printf '{"subject": "chore: generated commit"}'"#,
    );
    let (_config_dir, registry) = auggie_active_registry();
    let svc = svc.with_auggie_bin(bin).with_settings_registry(registry);
    let agent = session("agent-tr", &ws_id, Some("task-gen"), false, "Builder", true);
    svc.store().insert_agent_session(&agent).await.unwrap();
    attribute_dirty_change(&svc, &ws_id, "agent-tr").await;
    let event = idle_event(&ws_id, "agent-tr", "end_turn");
    svc.handle_agent_idle_auto_commit(&event).await;
    let (agent_id, linked_note_id, message) = last_commit_trailers(&repo.dir);
    assert_eq!(agent_id.as_deref(), Some("agent-tr"));
    assert_eq!(linked_note_id.as_deref(), Some("task-gen"));
    assert!(
        message.starts_with("chore: generated commit"),
        "got: {message}"
    );
}
