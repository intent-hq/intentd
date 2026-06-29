//! Unit tests: open a temp SQLite DB, run migrations, and round-trip
//! workspaces and notes including the `include_archived` filter.

use std::path::PathBuf;

use intent_core::{
    events, now_iso, ActorType, AgentId, AgentSession, AgentStatus, AuthorType, ClientId, Comment,
    CommentAnchor, CommentAnchorType, CommentStatus, CommentType, ContentType, EventActor, Note,
    NoteId, NoteVisibility, TaskMetadata, TaskStatus, Workspace, WorkspaceActivity,
    WorkspaceAttention, WorkspaceId, WorkspaceStatus,
};
use serde_json::json;

use crate::{EventQuery, NewEvent, Store};

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
        path: Some("/tmp/ws-meta".to_string()),
        repository_path: Some("/tmp/repo".to_string()),
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
        pr_status: None,
        active_pull_request: None,
        archived,
        archived_at: if archived { Some(now_iso()) } else { None },
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
    }
}

#[tokio::test]
async fn migration_status_reports_current_after_open() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let status = store.migration_status().await.expect("migration status");
    assert!(status.is_current(), "fresh open must apply all migrations");
    assert_eq!(
        status.expected,
        vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
    );
    assert_eq!(
        status.applied,
        vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
    );
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
    // `path` and `repository_path` survive insert → list (§9.1 TS parity).
    assert_eq!(got.path, Some("/tmp/ws-meta".to_string()));
    assert_eq!(got.repository_path, Some("/tmp/repo".to_string()));
    assert!(!got.archived);
}

#[tokio::test]
async fn workspace_get_update_delete() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    let id = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&id, "Original", false))
        .await
        .expect("insert");

    // get
    let got = store.get_workspace(&id).await.expect("get");
    assert_eq!(got.title, "Original");

    // missing get → NotFound
    let missing = store.get_workspace(&WorkspaceId::from("nope")).await;
    assert!(matches!(missing, Err(intent_core::Error::NotFound(_))));

    // `path`/`repository_path` round-trip through insert → get.
    assert_eq!(got.path, Some("/tmp/ws-meta".to_string()));
    assert_eq!(got.repository_path, Some("/tmp/repo".to_string()));

    // update (full row replace)
    let mut updated = got.clone();
    updated.title = "Renamed".to_string();
    updated.attention = WorkspaceAttention::None;
    updated.tags = vec!["x".to_string()];
    updated.path = Some("/tmp/ws-meta2".to_string());
    updated.repository_path = Some("/tmp/repo2".to_string());
    store.update_workspace(&updated).await.expect("update");
    let reread = store.get_workspace(&id).await.expect("reget");
    assert_eq!(reread.title, "Renamed");
    assert_eq!(reread.attention, WorkspaceAttention::None);
    assert_eq!(reread.tags, vec!["x".to_string()]);
    assert_eq!(reread.path, Some("/tmp/ws-meta2".to_string()));
    assert_eq!(reread.repository_path, Some("/tmp/repo2".to_string()));

    // update missing → NotFound
    let mut ghost = updated.clone();
    ghost.id = WorkspaceId::from("nope");
    assert!(matches!(
        store.update_workspace(&ghost).await,
        Err(intent_core::Error::NotFound(_))
    ));

    // delete
    store.delete_workspace(&id).await.expect("delete");
    assert!(matches!(
        store.get_workspace(&id).await,
        Err(intent_core::Error::NotFound(_))
    ));
    // delete missing → NotFound
    assert!(matches!(
        store.delete_workspace(&id).await,
        Err(intent_core::Error::NotFound(_))
    ));
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
        rev: 0,
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

#[tokio::test]
async fn note_rev_increments_on_update() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    let ws_id = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws_id, "WS", false))
        .await
        .expect("insert ws");

    let ts = now_iso();
    let mut note = Note {
        id: NoteId::new(),
        workspace_id: ws_id.clone(),
        title: "Spec".to_string(),
        content: "# Hello".to_string(),
        content_type: ContentType::Markdown,
        tags: vec![],
        is_pinned: false,
        is_archived: false,
        is_default: false,
        parent_id: None,
        visibility: NoteVisibility::Workspace,
        task: None,
        created_at: ts.clone(),
        rev: 0,
        updated_at: ts,
    };
    store.insert_note(&note).await.expect("insert note");

    // Fresh insert starts at rev 0.
    let after_insert = store.get_note(&note.id).await.expect("get note");
    assert_eq!(after_insert.rev, 0);

    // First update bumps rev → 1 (the bump is store-owned, ignoring the stale
    // in-memory `rev` carried by the passed-in note).
    note.content = "# Hello v2".to_string();
    store.update_note(&note).await.expect("update note");
    let after_first = store.get_note(&note.id).await.expect("get note");
    assert_eq!(after_first.rev, 1);
    assert_eq!(after_first.content, "# Hello v2");

    // A second update bumps again → 2 (monotonic).
    note.content = "# Hello v3".to_string();
    store.update_note(&note).await.expect("update note");
    let after_second = store.get_note(&note.id).await.expect("get note");
    assert_eq!(after_second.rev, 2);
}

#[tokio::test]
async fn update_note_versioned_hit_miss_and_absent() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    let ws_id = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws_id, "WS", false))
        .await
        .expect("insert ws");

    let ts = now_iso();
    let mut note = Note {
        id: NoteId::new(),
        workspace_id: ws_id.clone(),
        title: "Spec".to_string(),
        content: "v0".to_string(),
        content_type: ContentType::Markdown,
        tags: vec![],
        is_pinned: false,
        is_archived: false,
        is_default: false,
        parent_id: None,
        visibility: NoteVisibility::Workspace,
        task: None,
        created_at: ts.clone(),
        rev: 0,
        updated_at: ts,
    };
    store.insert_note(&note).await.expect("insert note");

    // HIT: expected_version matches the stored rev (0) → write + bump to 1.
    note.content = "v1".to_string();
    store
        .update_note_versioned(&note, Some(0))
        .await
        .expect("versioned hit");
    let after_hit = store.get_note(&note.id).await.expect("get note");
    assert_eq!(after_hit.rev, 1);
    assert_eq!(after_hit.content, "v1");

    // MISS: stale expected_version (0, but stored rev is now 1) → Conflict
    // carrying the current entity under `current` (rev 1, unchanged content).
    note.content = "v2-should-not-persist".to_string();
    let conflict = store.update_note_versioned(&note, Some(0)).await;
    match conflict {
        Err(intent_core::Error::Conflict { current }) => {
            assert_eq!(current["rev"], 1);
            assert_eq!(current["content"], "v1");
            assert_eq!(current["id"], note.id.0);
        }
        other => panic!("expected Conflict, got {other:?}"),
    }
    // The failed conditional write must not have persisted or bumped.
    let after_miss = store.get_note(&note.id).await.expect("get note");
    assert_eq!(after_miss.rev, 1);
    assert_eq!(after_miss.content, "v1");

    // ABSENT: no expected_version → last-writer-wins (unconditional bump → 2).
    note.content = "v2".to_string();
    store
        .update_note_versioned(&note, None)
        .await
        .expect("absent degrades to last-writer-wins");
    let after_absent = store.get_note(&note.id).await.expect("get note");
    assert_eq!(after_absent.rev, 2);
    assert_eq!(after_absent.content, "v2");

    // A versioned write against a missing row is NotFound (not Conflict).
    let mut ghost = note.clone();
    ghost.id = NoteId::new();
    match store.update_note_versioned(&ghost, Some(5)).await {
        Err(intent_core::Error::NotFound(_)) => {}
        other => panic!("expected NotFound for absent row, got {other:?}"),
    }
}

#[tokio::test]
async fn delete_note_versioned_hit_miss_and_absent() {
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
        content: "v0".to_string(),
        content_type: ContentType::Markdown,
        tags: vec![],
        is_pinned: false,
        is_archived: false,
        is_default: false,
        parent_id: None,
        visibility: NoteVisibility::Workspace,
        task: None,
        created_at: ts.clone(),
        rev: 0,
        updated_at: ts,
    };
    store.insert_note(&note).await.expect("insert note");

    // MISS: stale expected_version (5, but stored rev is 0) → Conflict carrying
    // the current entity snapshot prior to deletion; the row survives.
    let conflict = store.delete_note_versioned(&note.id, Some(5)).await;
    match conflict {
        Err(intent_core::Error::Conflict { current }) => {
            assert_eq!(current["rev"], 0);
            assert_eq!(current["id"], note.id.0);
        }
        other => panic!("expected Conflict, got {other:?}"),
    }
    assert!(store.get_note(&note.id).await.is_ok());

    // HIT: expected_version matches the stored rev (0) → row deleted.
    store
        .delete_note_versioned(&note.id, Some(0))
        .await
        .expect("versioned delete hit");
    match store.get_note(&note.id).await {
        Err(intent_core::Error::NotFound(_)) => {}
        other => panic!("expected NotFound after delete, got {other:?}"),
    }

    // A versioned delete against a missing row is NotFound (not Conflict).
    match store.delete_note_versioned(&note.id, Some(0)).await {
        Err(intent_core::Error::NotFound(_)) => {}
        other => panic!("expected NotFound for absent row, got {other:?}"),
    }

    // ABSENT: a fresh row with no expected_version → unconditional delete.
    let ts2 = now_iso();
    let other = Note {
        id: NoteId::new(),
        created_at: ts2.clone(),
        updated_at: ts2,
        ..note.clone()
    };
    store.insert_note(&other).await.expect("insert note 2");
    store
        .delete_note_versioned(&other.id, None)
        .await
        .expect("unconditional delete");
    match store.get_note(&other.id).await {
        Err(intent_core::Error::NotFound(_)) => {}
        other => panic!("expected NotFound after unconditional delete, got {other:?}"),
    }
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
        rev: 0,
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

fn file_event(ws: &WorkspaceId, ts: &str, path: &str, actor: EventActor) -> NewEvent {
    NewEvent {
        workspace_id: ws.clone(),
        timestamp: ts.to_string(),
        event_type: events::FILE_CHANGED.to_string(),
        actor,
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: json!({ "path": path, "action": "modify" }),
    }
}

#[tokio::test]
async fn event_round_trip_and_queries() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    let other = WorkspaceId::new();

    let user = EventActor {
        actor_type: ActorType::User,
        id: Some("u1".to_string()),
        name: Some("Alice".to_string()),
        email: Some("alice@example.com".to_string()),
        ..Default::default()
    };
    let agent = EventActor {
        actor_type: ActorType::Agent,
        id: Some("agent-7".to_string()),
        model: Some("opus".to_string()),
        ..Default::default()
    };

    // Insert returns a UUIDv7 id and round-trips actor + data.
    let inserted = store
        .insert_event(&file_event(
            &ws,
            "2026-01-01T00:00:01Z",
            "src/a.rs",
            user.clone(),
        ))
        .await
        .expect("insert e1");
    assert_eq!(inserted.id.len(), 36, "UUIDv7 string id");
    assert_eq!(inserted.actor, user);
    assert_eq!(
        inserted.data,
        json!({ "path": "src/a.rs", "action": "modify" })
    );

    store
        .insert_event(&file_event(
            &ws,
            "2026-01-01T00:00:02Z",
            "docs/readme.md",
            agent.clone(),
        ))
        .await
        .expect("insert e2");
    store
        .insert_event(&NewEvent {
            workspace_id: ws.clone(),
            timestamp: "2026-01-01T00:00:03Z".to_string(),
            event_type: events::AGENT_TOOL_CALL.to_string(),
            actor: agent.clone(),
            session_id: Some("sess-1".to_string()),
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data: json!({ "tool": "edit" }),
        })
        .await
        .expect("insert e3");
    // A different workspace must never leak into ws queries.
    store
        .insert_event(&file_event(
            &other,
            "2026-01-01T00:00:09Z",
            "src/z.rs",
            user,
        ))
        .await
        .expect("insert other");

    // recent_files: newest first, scoped to workspace, only file:changed.
    let recent = store.recent_files(&ws, 10).await.expect("recent");
    assert_eq!(recent.len(), 2);
    assert_eq!(recent[0].timestamp, "2026-01-01T00:00:02Z");
    assert_eq!(recent[1].timestamp, "2026-01-01T00:00:01Z");

    // events_by_workspace: all three ws events, newest first; no leak.
    let all = store.events_by_workspace(&ws, 10).await.expect("by ws");
    assert_eq!(all.len(), 3);
    assert_eq!(all[0].timestamp, "2026-01-01T00:00:03Z");

    // events_by_type filters on the taxonomy string.
    let edits = store
        .events_by_type(&ws, events::FILE_CHANGED, 10)
        .await
        .expect("by type");
    assert_eq!(edits.len(), 2);
    let calls = store
        .events_by_type(&ws, events::AGENT_TOOL_CALL, 10)
        .await
        .expect("by type tool-call");
    assert_eq!(calls.len(), 1);

    // directory_changes: prefix filter on data.path.
    let in_src = store
        .directory_changes(&ws, "src/", 10)
        .await
        .expect("dir changes");
    assert_eq!(in_src.len(), 1);
    assert_eq!(in_src[0].data["path"], json!("src/a.rs"));

    // generic query: by actor type + session_id + time window.
    let by_agent = store
        .query_events(&EventQuery {
            workspace_id: Some(ws.clone()),
            actor_type: Some(ActorType::Agent),
            ..Default::default()
        })
        .await
        .expect("by actor");
    assert_eq!(by_agent.len(), 2);

    let by_session = store
        .query_events(&EventQuery {
            workspace_id: Some(ws.clone()),
            session_id: Some("sess-1".to_string()),
            ..Default::default()
        })
        .await
        .expect("by session");
    assert_eq!(by_session.len(), 1);
    assert_eq!(by_session[0].data, json!({ "tool": "edit" }));

    let windowed = store
        .query_events(&EventQuery {
            workspace_id: Some(ws.clone()),
            since: Some("2026-01-01T00:00:02Z".to_string()),
            until: Some("2026-01-01T00:00:02Z".to_string()),
            ..Default::default()
        })
        .await
        .expect("windowed");
    assert_eq!(windowed.len(), 1);
    assert_eq!(windowed[0].timestamp, "2026-01-01T00:00:02Z");

    // limit + offset paginate the newest-first stream.
    let page = store
        .query_events(&EventQuery {
            workspace_id: Some(ws.clone()),
            limit: Some(1),
            offset: Some(1),
            ..Default::default()
        })
        .await
        .expect("page");
    assert_eq!(page.len(), 1);
    assert_eq!(page[0].timestamp, "2026-01-01T00:00:02Z");
}

fn typed_event(ws: &WorkspaceId, ts: &str, event_type: &str, actor: EventActor) -> NewEvent {
    NewEvent {
        workspace_id: ws.clone(),
        timestamp: ts.to_string(),
        event_type: event_type.to_string(),
        actor,
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: json!({}),
    }
}

#[tokio::test]
async fn event_metadata_round_trips_through_store() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    let actor = EventActor {
        actor_type: ActorType::Agent,
        id: Some("agent-9".to_string()),
        ..Default::default()
    };

    // With metadata: persists and reads back the camelCase `metadata` object.
    let with_meta = NewEvent {
        workspace_id: ws.clone(),
        timestamp: "2026-01-01T00:00:01Z".to_string(),
        event_type: events::AGENT_MESSAGE.to_string(),
        actor: actor.clone(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: Some(json!({ "source": "test", "retryCount": 2 })),
        data: json!({}),
    };
    let inserted = store.insert_event(&with_meta).await.expect("insert");
    assert_eq!(
        inserted.metadata,
        Some(json!({ "source": "test", "retryCount": 2 }))
    );

    // Without metadata: stays `None` through the round-trip (column is NULL).
    let without_meta = NewEvent {
        timestamp: "2026-01-01T00:00:02Z".to_string(),
        metadata: None,
        ..with_meta.clone()
    };
    store.insert_event(&without_meta).await.expect("insert");

    let all = store.events_by_workspace(&ws, 10).await.expect("query");
    assert_eq!(all.len(), 2);
    let newest = &all[0];
    assert_eq!(newest.metadata, None);
    let oldest = &all[1];
    assert_eq!(
        oldest.metadata,
        Some(json!({ "source": "test", "retryCount": 2 }))
    );
}

#[tokio::test]
async fn stream_retention_sweep_trims_only_old_stream_events() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    let agent = EventActor {
        actor_type: ActorType::Agent,
        id: Some("agent-1".to_string()),
        ..Default::default()
    };

    // Mixed families across ages. Old = 2026-01-01, new = 2026-06-01.
    let old = "2026-01-01T00:00:00Z";
    let new = "2026-06-01T00:00:00Z";
    let seed = vec![
        // Old stream chunks (should be deleted by the sweep).
        typed_event(&ws, old, events::AGENT_STREAM_START, agent.clone()),
        typed_event(&ws, old, events::AGENT_STREAM_CHUNK, agent.clone()),
        typed_event(&ws, old, events::AGENT_STREAM_END, agent.clone()),
        // New stream chunk (within TTL — must survive).
        typed_event(&ws, new, events::AGENT_STREAM_CHUNK, agent.clone()),
        // Old non-stream families (must NEVER be deleted regardless of age).
        typed_event(&ws, old, events::AGENT_STARTED, agent.clone()),
        typed_event(&ws, old, events::AGENT_TOOL_CALL, agent.clone()),
        typed_event(&ws, old, events::FILE_CHANGED, agent.clone()),
        typed_event(&ws, old, events::NOTE_UPDATED, agent.clone()),
        typed_event(&ws, old, events::TASK_STATUS_CHANGED, agent.clone()),
    ];
    for ev in &seed {
        store.insert_event(ev).await.expect("insert seed event");
    }

    // Cutoff between old and new: only old stream chunks are eligible.
    let cutoff = "2026-03-01T00:00:00Z";
    let removed = store
        .delete_stream_events_before(cutoff)
        .await
        .expect("sweep");
    assert_eq!(removed, 3, "exactly the three old stream events removed");

    let remaining = store
        .events_by_workspace(&ws, 100)
        .await
        .expect("remaining");
    assert_eq!(remaining.len(), 6);
    // The surviving stream event is the new one; no old stream events remain.
    let stream_types: Vec<&str> = remaining
        .iter()
        .filter(|e| e.event_type.starts_with(events::AGENT_STREAM_PREFIX))
        .map(|e| e.timestamp.as_str())
        .collect();
    assert_eq!(stream_types, vec![new]);
    // Every non-stream family survives, including the old ones.
    for t in [
        events::AGENT_STARTED,
        events::AGENT_TOOL_CALL,
        events::FILE_CHANGED,
        events::NOTE_UPDATED,
        events::TASK_STATUS_CHANGED,
    ] {
        assert!(
            remaining.iter().any(|e| e.event_type == t),
            "preserved family {t} missing"
        );
    }

    // Idempotent: a re-run with the same cutoff removes nothing more.
    let removed_again = store
        .delete_stream_events_before(cutoff)
        .await
        .expect("sweep re-run");
    assert_eq!(removed_again, 0);
}

#[tokio::test]
async fn stream_retention_sweep_disabled_is_noop_in_practice() {
    // The daemon disables the sweep at TTL=0 by never calling it; at the store
    // layer a cutoff older than every row is a safe no-op (nothing eligible).
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    let agent = EventActor {
        actor_type: ActorType::Agent,
        id: Some("agent-1".to_string()),
        ..Default::default()
    };
    store
        .insert_event(&typed_event(
            &ws,
            "2026-06-01T00:00:00Z",
            events::AGENT_STREAM_CHUNK,
            agent,
        ))
        .await
        .expect("insert");

    let removed = store
        .delete_stream_events_before("2020-01-01T00:00:00Z")
        .await
        .expect("sweep");
    assert_eq!(removed, 0);
    assert_eq!(store.events_by_workspace(&ws, 10).await.unwrap().len(), 1);
}

fn sample_agent_session(id: &AgentId, ws: &WorkspaceId) -> AgentSession {
    let ts = now_iso();
    AgentSession {
        id: id.clone(),
        workspace_id: ws.clone(),
        parent_agent_id: None,
        backend_session_id: None,
        acp_session_id: None,
        name: "Builder".to_string(),
        name_explicitly_set: false,
        model: Some("opus".to_string()),
        provider: None,
        system_prompt: Some("be helpful".to_string()),
        specialist: None,
        status: AgentStatus::Pending,
        is_active: false,
        messages: Vec::new(),
        stats: None,
        task_note_id: None,
        skip_auto_commit: false,
        created_at: ts.clone(),
        updated_at: ts,
    }
}

#[tokio::test]
async fn agent_session_round_trip_and_append_only_log() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");

    let agent_id = AgentId::from("agent-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
    store
        .insert_agent_session(&sample_agent_session(&agent_id, &ws))
        .await
        .expect("insert session");

    // Append-only log: seq is monotonic per agent, starting at 0.
    let m0 = store
        .append_agent_message(
            &agent_id,
            "user",
            &json!([{ "type": "text", "text": "hi" }]),
            "t0",
        )
        .await
        .expect("append m0");
    let m1 = store
        .append_agent_message(
            &agent_id,
            "assistant",
            &json!([{ "type": "text", "text": "yo" }]),
            "t1",
        )
        .await
        .expect("append m1");
    assert_eq!(m0.seq, 0);
    assert_eq!(m1.seq, 1);

    // Round-trip the session with its full message log (chronological order).
    let loaded = store.get_agent_session(&agent_id).await.expect("get");
    assert_eq!(loaded.name, "Builder");
    assert_eq!(loaded.model.as_deref(), Some("opus"));
    assert_eq!(loaded.status, AgentStatus::Pending);
    assert_eq!(loaded.messages.len(), 2);
    assert_eq!(loaded.messages[0].seq, 0);
    assert_eq!(loaded.messages[0].role, "user");
    assert_eq!(
        loaded.messages[0].content,
        json!([{ "type": "text", "text": "hi" }])
    );
    assert_eq!(loaded.messages[1].seq, 1);

    // getConversation cap: most-recent N, still oldest→newest.
    assert_eq!(
        store.count_agent_messages(&agent_id).await.expect("count"),
        2
    );
    let recent = store
        .get_agent_messages(&agent_id, Some(1))
        .await
        .expect("recent");
    assert_eq!(recent.len(), 1);
    assert_eq!(recent[0].seq, 1);

    let listed = store.list_agent_sessions(&ws).await.expect("list");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, agent_id);
    assert_eq!(listed[0].messages.len(), 2);
}

#[tokio::test]
async fn agent_session_parent_agent_id_round_trips() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");

    // Default: no parent linkage persists as None.
    let orphan = AgentId::from("agent-11111111-2222-3333-4444-555555555555");
    store
        .insert_agent_session(&sample_agent_session(&orphan, &ws))
        .await
        .expect("insert orphan");
    assert_eq!(
        store
            .get_agent_session(&orphan)
            .await
            .expect("get")
            .parent_agent_id,
        None
    );

    // Inserted parent linkage round-trips on get and list.
    let parent = AgentId::from("agent-aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa");
    let child = AgentId::from("agent-bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb");
    let mut session = sample_agent_session(&child, &ws);
    session.parent_agent_id = Some(parent.clone());
    store
        .insert_agent_session(&session)
        .await
        .expect("insert child");
    assert_eq!(
        store
            .get_agent_session(&child)
            .await
            .expect("get")
            .parent_agent_id,
        Some(parent.clone())
    );

    // Update clears the linkage back to None.
    let mut cleared = store.get_agent_session(&child).await.expect("get");
    cleared.parent_agent_id = None;
    store.update_agent_session(&cleared).await.expect("update");
    assert_eq!(
        store
            .get_agent_session(&child)
            .await
            .expect("get")
            .parent_agent_id,
        None
    );
}

#[tokio::test]
async fn agent_session_specialist_round_trips() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");

    // Default: no specialist persists as None.
    let plain = AgentId::from("agent-cccccccc-cccc-cccc-cccc-cccccccccccc");
    store
        .insert_agent_session(&sample_agent_session(&plain, &ws))
        .await
        .expect("insert plain");
    assert_eq!(
        store
            .get_agent_session(&plain)
            .await
            .expect("get")
            .specialist,
        None
    );

    // Inserted specialist round-trips on get and survives an update.
    let spec_agent = AgentId::from("agent-dddddddd-dddd-dddd-dddd-dddddddddddd");
    let mut session = sample_agent_session(&spec_agent, &ws);
    session.specialist = Some("implementor".to_string());
    store
        .insert_agent_session(&session)
        .await
        .expect("insert specialist");
    assert_eq!(
        store
            .get_agent_session(&spec_agent)
            .await
            .expect("get")
            .specialist,
        Some("implementor".to_string())
    );

    let mut updated = store.get_agent_session(&spec_agent).await.expect("get");
    updated.name = "Renamed".to_string();
    store.update_agent_session(&updated).await.expect("update");
    assert_eq!(
        store
            .get_agent_session(&spec_agent)
            .await
            .expect("get")
            .specialist,
        Some("implementor".to_string())
    );
}

#[tokio::test]
async fn agent_acp_session_id_is_write_once() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");
    let agent_id = AgentId::new();
    store
        .insert_agent_session(&sample_agent_session(&agent_id, &ws))
        .await
        .expect("insert session");

    // First write succeeds; re-setting the same value is idempotent.
    store
        .set_acp_session_id(&agent_id, "acp-1")
        .await
        .expect("first set");
    store
        .set_acp_session_id(&agent_id, "acp-1")
        .await
        .expect("idempotent set");
    // Changing it to a different value is rejected.
    assert!(store.set_acp_session_id(&agent_id, "acp-2").await.is_err());

    // update_agent_session also refuses to overwrite a set acpSessionId.
    let mut s = store.get_agent_session(&agent_id).await.expect("get");
    s.acp_session_id = Some("acp-3".to_string());
    assert!(store.update_agent_session(&s).await.is_err());
}

#[tokio::test]
async fn replace_acp_session_id_is_no_clobber_cas() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");
    let agent_id = AgentId::new();
    store
        .insert_agent_session(&sample_agent_session(&agent_id, &ws))
        .await
        .expect("insert session");

    store
        .set_acp_session_id(&agent_id, "acp-1")
        .await
        .expect("first set");

    // CAS no-clobber: a stale expected-old does NOT overwrite the canonical id;
    // the stored value is returned for the caller to reuse.
    let kept = store
        .replace_acp_session_id(&agent_id, "wrong-old", "acp-2")
        .await
        .expect("cas returns canonical");
    assert_eq!(kept, "acp-1", "diverged expected-old reuses the stored id");
    let stored = store.get_agent_session(&agent_id).await.expect("get");
    assert_eq!(stored.acp_session_id.as_deref(), Some("acp-1"));

    // CAS swap: a matching expected-old replaces with the fresh id (the
    // resume-impossible fallback, where `set_acp_session_id` would reject).
    let swapped = store
        .replace_acp_session_id(&agent_id, "acp-1", "acp-2")
        .await
        .expect("cas swaps on match");
    assert_eq!(swapped, "acp-2");
    let stored = store.get_agent_session(&agent_id).await.expect("get");
    assert_eq!(stored.acp_session_id.as_deref(), Some("acp-2"));

    // A missing session surfaces NotFound rather than a silent no-op.
    assert!(store
        .replace_acp_session_id(&AgentId::new(), "acp-2", "acp-x")
        .await
        .is_err());
}

#[tokio::test]
async fn agent_provider_is_immutable_once_set() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");
    let agent_id = AgentId::new();
    store
        .insert_agent_session(&sample_agent_session(&agent_id, &ws))
        .await
        .expect("insert session");

    // Provider starts unset, so first set (via update) is allowed.
    let mut s = store.get_agent_session(&agent_id).await.expect("get");
    s.provider = Some("auggie".to_string());
    s.status = AgentStatus::Active;
    store.update_agent_session(&s).await.expect("set provider");

    // Changing the provider afterwards is rejected.
    let mut s = store.get_agent_session(&agent_id).await.expect("get");
    s.provider = Some("claude-code".to_string());
    assert!(store.update_agent_session(&s).await.is_err());
}

#[tokio::test]
async fn client_upsert_sets_first_seen_once_and_touches_last_seen() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let id = ClientId::from_string("cli-abc");

    store
        .upsert_client(&id, Some("Laptop"), Some(&json!({ "forward": true })))
        .await
        .expect("insert client");
    let first = store.get_client(&id).await.expect("get").expect("present");
    assert_eq!(first.name, Some("Laptop".to_string()));
    assert_eq!(first.capabilities, json!({ "forward": true }));

    // Re-hello updates name/capabilities and touches last_seen; first_seen stays.
    store
        .upsert_client(&id, Some("Desktop"), Some(&json!({ "forward": false })))
        .await
        .expect("re-upsert");
    let again = store.get_client(&id).await.expect("get").expect("present");
    assert_eq!(again.name, Some("Desktop".to_string()));
    assert_eq!(again.capabilities, json!({ "forward": false }));
    assert_eq!(
        again.first_seen, first.first_seen,
        "first_seen is preserved"
    );
    assert!(store
        .get_client(&ClientId::from_string("missing"))
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn draft_round_trip_upsert_get_delete() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");
    let client = ClientId::from_string("cli-1");
    store
        .upsert_client(&client, None, None)
        .await
        .expect("client");
    let agent = AgentId::from_string("agent-1");

    assert!(store
        .get_draft(&ws, &agent, &client)
        .await
        .unwrap()
        .is_none());
    store
        .upsert_draft(&ws, &agent, &client, "hello")
        .await
        .expect("set");
    let got = store
        .get_draft(&ws, &agent, &client)
        .await
        .unwrap()
        .expect("present");
    assert_eq!(got.text, "hello");

    // Upsert overwrites text in place.
    store
        .upsert_draft(&ws, &agent, &client, "world")
        .await
        .expect("set2");
    assert_eq!(
        store
            .get_draft(&ws, &agent, &client)
            .await
            .unwrap()
            .unwrap()
            .text,
        "world"
    );

    assert!(
        store.delete_draft(&ws, &agent, &client).await.unwrap(),
        "row removed"
    );
    assert!(
        !store.delete_draft(&ws, &agent, &client).await.unwrap(),
        "idempotent no-op"
    );
    assert!(store
        .get_draft(&ws, &agent, &client)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn drafts_are_isolated_by_client_and_cascade_on_workspace_delete() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .expect("insert ws");
    let agent = AgentId::from_string("agent-1");
    let a = ClientId::from_string("cli-a");
    let b = ClientId::from_string("cli-b");
    store.upsert_client(&a, None, None).await.unwrap();
    store.upsert_client(&b, None, None).await.unwrap();

    store.upsert_draft(&ws, &agent, &a, "from-a").await.unwrap();
    store.upsert_draft(&ws, &agent, &b, "from-b").await.unwrap();
    assert_eq!(
        store
            .get_draft(&ws, &agent, &a)
            .await
            .unwrap()
            .unwrap()
            .text,
        "from-a"
    );
    assert_eq!(
        store
            .get_draft(&ws, &agent, &b)
            .await
            .unwrap()
            .unwrap()
            .text,
        "from-b"
    );

    store.delete_workspace(&ws).await.expect("delete ws");
    assert!(
        store.get_draft(&ws, &agent, &a).await.unwrap().is_none(),
        "ON DELETE CASCADE removes drafts with their workspace"
    );
}

#[tokio::test]
async fn known_repo_upsert_idempotent_on_path() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    store
        .upsert_known_repo("/src/a", "a", Some("owner-a"))
        .await
        .expect("insert");
    // Re-upserting the same path must not create a duplicate row.
    store
        .upsert_known_repo("/src/a", "a", Some("owner-a"))
        .await
        .expect("upsert");
    let repos = store.list_known_repos().await.expect("list");
    assert_eq!(repos.len(), 1, "path is unique");
    let added_at = repos[0].added_at.clone();

    // Conflict updates name/owner when provided and keeps the original addedAt.
    store
        .upsert_known_repo("/src/a", "a-renamed", Some("owner-b"))
        .await
        .expect("update");
    let repos = store.list_known_repos().await.expect("list");
    assert_eq!(repos.len(), 1);
    assert_eq!(repos[0].name, "a-renamed");
    assert_eq!(repos[0].owner.as_deref(), Some("owner-b"));
    assert_eq!(repos[0].added_at, added_at, "addedAt is preserved");

    // An absent owner preserves the existing one (TS `?? existing`).
    store
        .upsert_known_repo("/src/a", "a-renamed", None)
        .await
        .expect("update no owner");
    let repos = store.list_known_repos().await.expect("list");
    assert_eq!(repos[0].owner.as_deref(), Some("owner-b"));
}

#[tokio::test]
async fn known_repo_list_orders_by_last_used_desc() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    // Insert three repos; each upsert stamps last_used_at = now, so the most
    // recently upserted sorts first. Small sleeps keep the ISO timestamps
    // strictly ordered regardless of clock resolution.
    let gap = std::time::Duration::from_millis(5);
    store.upsert_known_repo("/src/a", "a", None).await.unwrap();
    tokio::time::sleep(gap).await;
    store.upsert_known_repo("/src/b", "b", None).await.unwrap();
    tokio::time::sleep(gap).await;
    store.upsert_known_repo("/src/c", "c", None).await.unwrap();
    tokio::time::sleep(gap).await;
    // Bump /src/a so it becomes the most-recently-used.
    store.upsert_known_repo("/src/a", "a", None).await.unwrap();

    let repos = store.list_known_repos().await.expect("list");
    let paths: Vec<&str> = repos.iter().map(|r| r.path.as_str()).collect();
    assert_eq!(paths, vec!["/src/a", "/src/c", "/src/b"]);
}

#[tokio::test]
async fn idempotency_get_put_round_trips_and_dedupes() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    assert!(
        store.get_idempotent("ws-1", "k1").await.unwrap().is_none(),
        "missing key reads as None"
    );
    let inserted = store
        .put_idempotent("ws-1", "k1", "note.create", "{\"id\":\"n1\"}")
        .await
        .unwrap();
    assert!(inserted, "first put inserts the row");
    assert_eq!(
        store.get_idempotent("ws-1", "k1").await.unwrap().as_deref(),
        Some("{\"id\":\"n1\"}")
    );
    // A second put under the same (ws,key) is an INSERT OR IGNORE no-op that
    // keeps the original stored result.
    let inserted_again = store
        .put_idempotent("ws-1", "k1", "note.create", "{\"id\":\"n2\"}")
        .await
        .unwrap();
    assert!(!inserted_again, "duplicate put is a no-op");
    assert_eq!(
        store.get_idempotent("ws-1", "k1").await.unwrap().as_deref(),
        Some("{\"id\":\"n1\"}"),
        "duplicate put must not overwrite the original result"
    );
    // The same key is independent across workspaces (and the "" global sentinel).
    assert!(store.get_idempotent("ws-2", "k1").await.unwrap().is_none());
    assert!(store.get_idempotent("", "k1").await.unwrap().is_none());
}

#[tokio::test]
async fn idempotency_reaper_deletes_only_rows_older_than_cutoff() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    // One row well in the past, one fresh row (put stamps created_at = now).
    let old_ts = intent_core::iso_minutes_ago(48 * 60);
    sqlx::query(
        "INSERT INTO idempotency_key \
         (workspace_id, idempotency_key, method, result_json, created_at) \
         VALUES (?,?,?,?,?)",
    )
    .bind("ws-1")
    .bind("old")
    .bind("note.create")
    .bind("{\"id\":\"old\"}")
    .bind(&old_ts)
    .execute(store.pool())
    .await
    .expect("seed old row");
    store
        .put_idempotent("ws-1", "fresh", "note.create", "{\"id\":\"fresh\"}")
        .await
        .unwrap();

    let cutoff = intent_core::iso_minutes_ago(24 * 60);
    let removed = store.reap_idempotent(&cutoff).await.expect("reap");
    assert_eq!(removed, 1, "only the >24h row is reaped");
    assert!(store.get_idempotent("ws-1", "old").await.unwrap().is_none());
    assert!(store
        .get_idempotent("ws-1", "fresh")
        .await
        .unwrap()
        .is_some());

    // Re-running the sweep removes nothing more (idempotent).
    assert_eq!(store.reap_idempotent(&cutoff).await.expect("reap"), 0);
}
