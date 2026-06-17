//! Unit tests: open a temp SQLite DB, run migrations, and round-trip
//! workspaces and notes including the `include_archived` filter.

use std::path::PathBuf;

use intent_core::{
    now_iso, ContentType, Note, NoteId, NoteVisibility, Workspace, WorkspaceActivity,
    WorkspaceAttention, WorkspaceId, WorkspaceStatus,
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
        task: Some(serde_json::json!({ "status": "in_progress" })),
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
        got.task,
        Some(serde_json::json!({ "status": "in_progress" }))
    );
}
