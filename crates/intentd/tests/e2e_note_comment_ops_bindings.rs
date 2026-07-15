//! E2E coverage follow-up for note_ops.rs + comment operations.
//!
//! Exercises note.add, note.edit, note.editLines, comment.add (anchoring), comment.respond,
//! and comment.list via in-process Services calls. Hermetic tests asserting BE state changes.

#![cfg(unix)]

use std::path::PathBuf;
use std::sync::Arc;

use intent_core::{
    now_iso, NoteCreate, Workspace, WorkspaceActivity, WorkspaceApi, WorkspaceAttention,
    WorkspaceId, WorkspaceStatus,
};
use intent_services::{EventBus, Services};
use intent_store::Store;

fn workspace(id: &WorkspaceId, path: Option<std::path::PathBuf>) -> Workspace {
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "Test".into(),
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
        path: path.as_ref().map(|p| p.display().to_string()),
        repository_path: None,
        repository_owner: None,
        repository_name: None,
        worktree_path: path.map(|p| p.display().to_string()),
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
    }
}

fn cleanup_db(db: &PathBuf) {
    std::fs::remove_file(db).ok();
    std::fs::remove_file(format!("{}-shm", db.display())).ok();
    std::fs::remove_file(format!("{}-wal", db.display())).ok();
}

async fn setup() -> (Arc<Services>, WorkspaceId, PathBuf, PathBuf) {
    let db = std::env::temp_dir().join(format!("itd-e2e-note-comment-{}.db", uuid::Uuid::new_v4()));
    let ws_root =
        std::env::temp_dir().join(format!("itd-e2e-note-comment-ws-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&ws_root).expect("create ws root");

    let store = Store::open(&db).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store.clone())
        .with_workspaces_root(ws_root.parent().unwrap().to_path_buf())
        .with_event_bus(bus.clone());

    let ws = WorkspaceId::new();
    store
        .insert_workspace(&workspace(&ws, Some(ws_root.clone())))
        .await
        .expect("insert ws");

    (Arc::new(services), ws, ws_root, db)
}

#[tokio::test]
async fn note_add_appends_content_at_end() {
    let (services, ws, ws_root, db) = setup().await;

    let note = services
        .create_note(
            ws.clone(),
            NoteCreate {
                title: "Test Note".to_string(),
                content: Some("Initial content".to_string()),
                tags: None,
                parent_id: None,
            },
            None,
            None,
        )
        .await
        .expect("create note");

    // Call note.add with position="end"
    let result = services
        .add_to_note(
            ws.clone(),
            note.id.clone(),
            intent_core::NoteAddInput {
                content: "Added section".to_string(),
                heading: None,
                position: Some("end".to_string()),
            },
            None,
        )
        .await
        .expect("add to note");

    // Stable shape: { ok: true, noteId, newContent, position }
    assert_eq!(result.ok, true);
    assert_eq!(result.note_id, note.id);
    assert_eq!(result.position, "at end");
    assert!(result.new_content.contains("Initial content"));
    assert!(result.new_content.contains("Added section"));

    drop(services);
    cleanup_db(&db);
    std::fs::remove_dir_all(&ws_root).ok();
}

#[tokio::test]
async fn note_edit_replaces_first_exact_match() {
    let (services, ws, ws_root, db) = setup().await;

    let note = services
        .create_note(
            ws.clone(),
            NoteCreate {
                title: "Edit Test".to_string(),
                content: Some("Line one\nLine two\nLine three".to_string()),
                tags: None,
                parent_id: None,
            },
            None,
            None,
        )
        .await
        .expect("create note");

    // Call note.edit - exact match replacement
    let result = services
        .edit_note(
            ws.clone(),
            note.id.clone(),
            intent_core::NoteEditInput {
                old: "Line two".to_string(),
                new: "Modified line".to_string(),
            },
            None,
        )
        .await
        .expect("edit note");

    // Stable shape: { ok: true, noteId, newContent, matchPosition, wasEmpty }
    assert_eq!(result.ok, true);
    assert_eq!(result.note_id, note.id);
    assert!(result.match_position >= 0);
    assert!(result.new_content.contains("Modified line"));
    assert!(!result.new_content.contains("Line two"));

    drop(services);
    cleanup_db(&db);
    std::fs::remove_dir_all(&ws_root).ok();
}

#[tokio::test]
async fn note_edit_lines_replaces_line_range() {
    let (services, ws, ws_root, db) = setup().await;

    let note = services
        .create_note(
            ws.clone(),
            NoteCreate {
                title: "Lines Test".to_string(),
                content: Some("Line 1\nLine 2\nLine 3\nLine 4".to_string()),
                tags: None,
                parent_id: None,
            },
            None,
            None,
        )
        .await
        .expect("create note");

    // Call note.editLines - replace lines 2-3 (1-based, inclusive)
    let result = services
        .edit_note_lines(
            ws.clone(),
            note.id.clone(),
            intent_core::NoteEditLinesInput {
                start: 2,
                end: 3,
                content: "Replaced lines 2-3".to_string(),
            },
            None,
        )
        .await
        .expect("edit lines");

    // Stable shape: { ok: true, noteId, newContent }
    assert_eq!(result.ok, true);
    assert_eq!(result.note_id, note.id);
    assert!(result.new_content.contains("Line 1"));
    assert!(result.new_content.contains("Replaced lines 2-3"));
    assert!(result.new_content.contains("Line 4"));
    assert!(!result.new_content.contains("Line 2"));
    assert!(!result.new_content.contains("Line 3"));

    drop(services);
    cleanup_db(&db);
    std::fs::remove_dir_all(&ws_root).ok();
}

#[tokio::test]
async fn comment_add_anchors_to_text() {
    let (services, ws, ws_root, db) = setup().await;

    let note = services
        .create_note(
            ws.clone(),
            NoteCreate {
                title: "Comment Test".to_string(),
                content: Some(
                    "# Heading\n\nThis is a unique paragraph that we can anchor to.".to_string(),
                ),
                tags: None,
                parent_id: None,
            },
            None,
            None,
        )
        .await
        .expect("create note");

    // Call comment.add with search-context anchoring
    let result = services
        .comment_add(
            ws.clone(),
            note.id.clone(),
            "This is a unique paragraph that we can anchor to.".to_string(),
            "unique paragraph".to_string(),
            "This needs clarification".to_string(),
            None,
            None,
            None,
        )
        .await
        .expect("add comment");

    // Stable shape: { success: true, commentId, anchored: true, location: { line, anchoredText } }
    assert_eq!(result.success, true);
    assert!(!result.comment_id.is_empty());
    assert_eq!(result.anchored, true);
    assert_eq!(result.location.anchored_text, "unique paragraph");
    assert!(result.location.line > 0);

    drop(services);
    cleanup_db(&db);
    std::fs::remove_dir_all(&ws_root).ok();
}

#[tokio::test]
async fn comment_list_returns_threads() {
    let (services, ws, ws_root, db) = setup().await;

    let note = services
        .create_note(
            ws.clone(),
            NoteCreate {
                title: "List Test".to_string(),
                content: Some("Content for commenting".to_string()),
                tags: None,
                parent_id: None,
            },
            None,
            None,
        )
        .await
        .expect("create note");

    // Add a comment first
    services
        .comment_add(
            ws.clone(),
            note.id.clone(),
            "Content for commenting".to_string(),
            "Content".to_string(),
            "Test comment".to_string(),
            None,
            None,
            None,
        )
        .await
        .expect("add comment");

    // Call comment.list
    let result = services
        .comment_list(ws.clone(), note.id.clone(), None, None, None, false)
        .await
        .expect("list comments");

    // Stable shape: { threads: [...] }
    assert!(result.threads.len() > 0);
    assert!(!result.threads[0].thread_id.is_empty());

    drop(services);
    cleanup_db(&db);
    std::fs::remove_dir_all(&ws_root).ok();
}

#[tokio::test]
async fn comment_respond_adds_reply_to_thread() {
    let (services, ws, ws_root, db) = setup().await;

    let note = services
        .create_note(
            ws.clone(),
            NoteCreate {
                title: "Respond Test".to_string(),
                content: Some("Reply test content".to_string()),
                tags: None,
                parent_id: None,
            },
            None,
            None,
        )
        .await
        .expect("create note");

    // Add initial comment
    let add_result = services
        .comment_add(
            ws.clone(),
            note.id.clone(),
            "Reply test content".to_string(),
            "test content".to_string(),
            "Original comment".to_string(),
            None,
            None,
            None,
        )
        .await
        .expect("add comment");

    // Call comment.respond
    let result = services
        .comment_respond(
            ws.clone(),
            note.id.clone(),
            None,
            Some(add_result.comment_id.clone()),
            "Reply to comment".to_string(),
            None,
            None,
            None,
            None,
        )
        .await
        .expect("respond to comment");

    // Stable shape: { success: true, comment: { id, ... } }
    assert_eq!(result.success, true);
    assert!(!result.comment.id.is_empty());

    drop(services);
    cleanup_db(&db);
    std::fs::remove_dir_all(&ws_root).ok();
}
