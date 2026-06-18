//! Unit tests: open a temp SQLite DB, run migrations, and round-trip
//! workspaces and notes including the `include_archived` filter.

use std::path::PathBuf;

use intent_core::{
    now_iso, AgentId, AuthorType, Comment, CommentAnchor, CommentAnchorType, CommentStatus,
    CommentType, ContentType, Note, NoteId, NoteVisibility, TaskMetadata, TaskStatus, Workspace,
    WorkspaceActivity, WorkspaceAttention, WorkspaceId, WorkspaceStatus,
};

use crate::Store;

/// A unique temp DB path that cleans up its `.db`/`-wal`/`-shm` files on drop.
struct TempDb {
    path: PathBuf,
}

impl TempDb {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("intentd-test-{}.db", uuid::Uuid::new_v4()));
        Self { path }
    }
}

impl Drop for TempDb {
    fn drop(&mut self) {
        for suffix in ["", "-wal", "-shm"] {
            let p = PathBuf::from(format!("{}{suffix}", self.path.display()));
            let _ = std::fs::remove_file(p);
        }
    }
}

fn sample_workspace(id: &WorkspaceId, title: &str, archived: bool) -> Workspace {
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: title.to_string(),
        branch: "feature/test".to_string(),
        base_ref: Some("main".to_string()),
        base_commit_sha: None,
        status: if archived {
            WorkspaceStatus::Archived
        } else {
            WorkspaceStatus::Active
        },
        status_message: Some("working".to_string()),
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::Unread,
        created_at: ts.clone(),
        updated_at: ts.clone(),
        last_activity: Some(ts),
        tags: vec!["alpha".to_string(), "beta".to_string()],
        path: None,
        repository_owner: Some("cloudlands-ai".to_string()),
        repository_name: Some("intentd".to_string()),
        worktree_path: None,
        scope: None,
        skip_worktree: false,
        setup_script: None,
        is_remote: false,
        default_model: Some("opus".to_string()),
        pr_number: Some(42),
        pr_url: None,
        archived,
        archived_at: if archived { Some(now_iso()) } else { None },
    }
}

#[tokio::test]
async fn workspace_round_trip_and_archive_filter() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    let active_id = WorkspaceId::new();
    let archived_id = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&active_id, "Active WS", false))
        .await
        .expect("insert active");
    store
        .insert_workspace(&sample_workspace(&archived_id, "Archived WS", true))
        .await
        .expect("insert archived");

    let all = store.list_workspaces(true).await.expect("list all");
    assert_eq!(all.len(), 2);

    let visible = store.list_workspaces(false).await.expect("list visible");
    assert_eq!(visible.len(), 1);
    assert_eq!(visible[0].id, active_id);

    let got = &visible[0];
    assert_eq!(got.title, "Active WS");
    assert_eq!(got.branch, "feature/test");
    assert_eq!(got.tags, vec!["alpha".to_string(), "beta".to_string()]);
    assert_eq!(got.status, WorkspaceStatus::Active);
    assert_eq!(got.attention, WorkspaceAttention::Unread);
    assert_eq!(got.pr_number, Some(42));
    assert_eq!(got.repository_name, Some("intentd".to_string()));
    assert!(!got.archived);
}

#[tokio::test]
async fn note_round_trip() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    let ws_id = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws_id, "WS", false))
        .await
        .expect("insert ws");

    let ts = now_iso();
    let note = Note {
        id: NoteId::new(),
        workspace_id: ws_id.clone(),
        title: "Spec".to_string(),
        content: "# Hello".to_string(),
        content_type: ContentType::Markdown,
        tags: vec!["spec".to_string()],
        is_pinned: true,
        is_archived: false,
        is_default: true,
        parent_id: None,
        visibility: NoteVisibility::Workspace,
        task: Some(TaskMetadata {
            status: TaskStatus::InProgress,
            ..Default::default()
        }),
        created_at: ts.clone(),
        updated_at: ts,
    };
    store.insert_note(&note).await.expect("insert note");

    let notes = store.list_notes(&ws_id).await.expect("list notes");
    assert_eq!(notes.len(), 1);
    let got = &notes[0];
    assert_eq!(got.id, note.id);
    assert_eq!(got.title, "Spec");
    assert_eq!(got.content, "# Hello");
    assert_eq!(got.content_type, ContentType::Markdown);
    assert_eq!(got.tags, vec!["spec".to_string()]);
    assert!(got.is_pinned);
    assert!(got.is_default);
    assert_eq!(
        got.task.as_ref().map(|t| t.status),
        Some(TaskStatus::InProgress)
    );

    let fetched = store.get_note(&note.id).await.expect("get note");
    assert_eq!(fetched.id, note.id);
}

fn task_note(ws_id: &WorkspaceId, title: &str, task: Option<TaskMetadata>) -> Note {
    let ts = now_iso();
    Note {
        id: NoteId::new(),
        workspace_id: ws_id.clone(),
        title: title.to_string(),
        content: String::new(),
        content_type: ContentType::Markdown,
        tags: vec![],
        is_pinned: false,
        is_archived: false,
        is_default: false,
        parent_id: None,
        visibility: NoteVisibility::Workspace,
        task,
        created_at: ts.clone(),
        updated_at: ts,
    }
}

#[tokio::test]
async fn task_metadata_round_trip_and_list_tasks() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws_id = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws_id, "WS", false))
        .await
        .expect("insert ws");

    let meta = TaskMetadata {
        status: TaskStatus::ReviewRequired,
        assigned_agent_ids: vec![AgentId::from("agent-1"), AgentId::from("agent-2")],
        acceptance_criteria: vec!["builds".to_string(), "tests pass".to_string()],
        estimated_effort: Some("M".to_string()),
        actual_effort: None,
        blocked_reason: None,
        started_at: Some(now_iso()),
        completed_at: None,
        peer_order: Some(100),
    };
    store
        .insert_note(&task_note(&ws_id, "Task A", Some(meta.clone())))
        .await
        .expect("insert task note");
    store
        .insert_note(&task_note(&ws_id, "Plain note", None))
        .await
        .expect("insert plain note");

    let tasks = store.list_tasks(&ws_id).await.expect("list tasks");
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].task, Some(meta));
}

fn sample_comment(note_id: &NoteId, thread_id: &str, id: &str) -> Comment {
    let ts = now_iso();
    Comment {
        id: id.to_string(),
        thread_id: thread_id.to_string(),
        note_id: Some(note_id.clone()),
        kind: CommentType::Suggestion,
        content: "please rename".to_string(),
        author: "alice".to_string(),
        author_type: AuthorType::User,
        status: CommentStatus::Open,
        parent_id: None,
        anchor: CommentAnchor {
            kind: CommentAnchorType::Range,
            start_id: Some("a1".to_string()),
            end_id: Some("a2".to_string()),
            point_id: None,
        },
        anchor_text: Some("foo".to_string()),
        anchor_before: Some("the ".to_string()),
        anchor_after: Some(" bar".to_string()),
        suggestion_original: Some("foo".to_string()),
        suggestion_proposed: Some("baz".to_string()),
        agent_id: Some(AgentId::from("agent-9")),
        created_at: ts.clone(),
        updated_at: ts,
    }
}

#[tokio::test]
async fn comment_round_trip_update_delete_and_thread() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws_id = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws_id, "WS", false))
        .await
        .expect("insert ws");
    let note = task_note(&ws_id, "Note", None);
    store.insert_note(&note).await.expect("insert note");

    let c1 = sample_comment(&note.id, "thread-1", "c1");
    let mut c2 = sample_comment(&note.id, "thread-1", "c2");
    c2.parent_id = Some("c1".to_string());
    c2.kind = CommentType::Comment;
    c2.anchor = CommentAnchor {
        kind: CommentAnchorType::Point,
        point_id: Some("p1".to_string()),
        ..Default::default()
    };
    c2.anchor_before = None;
    c2.anchor_after = None;
    c2.suggestion_original = None;
    c2.suggestion_proposed = None;
    c2.agent_id = None;
    store.insert_comment(&c1).await.expect("insert c1");
    store.insert_comment(&c2).await.expect("insert c2");

    let got = store.get_comment("c1").await.expect("get c1");
    assert_eq!(got, c1);

    let by_note = store.list_comments(&note.id).await.expect("list comments");
    assert_eq!(by_note.len(), 2);

    let thread = store.get_thread("thread-1").await.expect("get thread");
    assert_eq!(thread.thread_id, "thread-1");
    assert_eq!(thread.comments.len(), 2);

    let mut updated = c1.clone();
    updated.status = CommentStatus::Resolved;
    updated.content = "resolved now".to_string();
    store.update_comment(&updated).await.expect("update c1");
    let reread = store.get_comment("c1").await.expect("reget c1");
    assert_eq!(reread.status, CommentStatus::Resolved);
    assert_eq!(reread.content, "resolved now");

    store.delete_comment("c1").await.expect("delete c1");
    assert!(store.get_comment("c1").await.is_err());
    assert_eq!(
        store
            .list_comments(&note.id)
            .await
            .expect("list after del")
            .len(),
        1
    );
}
