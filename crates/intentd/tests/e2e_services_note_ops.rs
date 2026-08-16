//! E2E coverage for note.* operations (intent-services note_ops.rs coverage boost).
//!
//! Tests call intent_services::Services directly (not via WSS transport) for hermetic
//! in-process coverage. Tests note.add, note.edit, note.editLines, note.updateMetadata,
//! note.listTasks paths.

#![cfg(unix)]

mod common;

use std::path::PathBuf;
use std::sync::Arc;

use intent_core::{
    now_iso, NoteAddInput, NoteCreate, NoteEditInput, NoteEditLinesInput, Workspace,
    WorkspaceActivity, WorkspaceApi, WorkspaceAttention, WorkspaceId, WorkspaceStatus,
};
use intent_services::{EventBus, Services};
use intent_store::Store;

/// Clean up SQLite database including -wal and -shm sidecars.
fn cleanup_db(db: &PathBuf) {
    std::fs::remove_file(db).ok();
    std::fs::remove_file(db.with_extension("db-wal")).ok();
    std::fs::remove_file(db.with_extension("db-shm")).ok();
}

fn workspace(id: &WorkspaceId, path: PathBuf) -> Workspace {
    let ts = now_iso();
    Workspace {
        id: id.clone(),
        title: "E2E-note-ops".to_string(),
        branch: "main".to_string(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        status_image_asset_id: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: ts.clone(),
        updated_at: ts.clone(),
        last_activity: None,
        tags: vec![],
        path: Some(path.display().to_string()),
        repository_path: None,
        repository_owner: None,
        repository_name: None,
        worktree_path: Some(path.display().to_string()),
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

async fn setup() -> (Arc<Services>, WorkspaceId, PathBuf, PathBuf) {
    let db = std::env::temp_dir().join(format!("intentd-e2e-note-ops-{}.db", uuid::Uuid::new_v4()));
    let ws_root =
        std::env::temp_dir().join(format!("itd-e2e-note-ops-ws-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&ws_root).expect("create ws root");

    let store = Store::open(&db).await.expect("open store");
    let bus = EventBus::new(store.clone());
    let services = Services::new(store.clone())
        .with_workspaces_root(ws_root.parent().unwrap().to_path_buf())
        .with_event_bus(bus.clone());

    let ws = WorkspaceId::new();
    store
        .insert_workspace(&workspace(&ws, ws_root.clone()))
        .await
        .expect("insert ws");

    (Arc::new(services), ws, ws_root, db)
}

#[tokio::test]
async fn note_add_edit_edit_lines() {
    let (services, ws, ws_root, db) = setup().await;

    // Create a note using the proper API
    let create_input = NoteCreate {
        title: "Test Note".to_string(),
        content: Some("Initial content".to_string()),
        tags: None,
        parent_id: None,
    };
    let note = services
        .create_note(ws.clone(), create_input, None, None)
        .await
        .expect("create note")
        .note;
    let note_id = note.id.clone();

    // Test note.add
    let add_input = NoteAddInput {
        content: "Added section".to_string(),
        heading: None,
        position: Some("end".to_string()),
    };
    let add_result = services
        .add_to_note(ws.clone(), note_id.clone(), add_input, None)
        .await
        .expect("add to note");
    assert_eq!(add_result.note_id, note_id);

    // Read back and verify
    let note = services
        .get_note(ws.clone(), note_id.clone())
        .await
        .expect("get note");
    assert!(note.content.contains("Initial content"));
    assert!(note.content.contains("Added section"));

    // Test note.edit
    let edit_input = NoteEditInput {
        old: "Initial".to_string(),
        new: "Modified".to_string(),
    };
    let edit_result = services
        .edit_note(ws.clone(), note_id.clone(), edit_input, None)
        .await
        .expect("edit note");
    assert!(edit_result.match_position >= 0);
    // Verify the edit actually occurred
    assert!(!edit_result.new_content.contains("Initial"));
    assert!(edit_result.new_content.contains("Modified"));

    // Test note.editLines - always invoke it
    let note_after_edit = services
        .get_note(ws.clone(), note_id.clone())
        .await
        .expect("get note");
    assert!(
        !note_after_edit.content.is_empty(),
        "note content must be non-empty"
    );
    let edit_lines_input = NoteEditLinesInput {
        start: 1,
        end: 1,
        content: "Line replaced".to_string(),
    };
    let edit_lines_result = services
        .edit_note_lines(ws.clone(), note_id.clone(), edit_lines_input, None)
        .await
        .expect("edit note lines");
    assert_eq!(edit_lines_result.note_id, note_id);
    // Verify the line edit actually occurred in the content
    let note_after_line_edit = services
        .get_note(ws.clone(), note_id.clone())
        .await
        .expect("get note after line edit");
    assert!(note_after_line_edit.content.contains("Line replaced"));

    // Cleanup
    drop(services); // Drop store handles before DB cleanup
    cleanup_db(&db);
    std::fs::remove_dir_all(&ws_root).ok();
}

#[tokio::test]
async fn note_list_tasks() {
    let (services, ws, ws_root, db) = setup().await;

    // Create a note with tasks
    let content = "# Tasks\n\n- [ ] Task 1\n- [x] Task 2\n- [ ] Task 3";
    let create_input = NoteCreate {
        title: "Task Note".to_string(),
        content: Some(content.to_string()),
        tags: None,
        parent_id: None,
    };
    let note = services
        .create_note(ws.clone(), create_input, None, None)
        .await
        .expect("create note")
        .note;

    // List tasks
    let tasks = services
        .list_note_tasks(ws.clone(), note.id.clone())
        .await
        .expect("list tasks");

    assert_eq!(tasks.len(), 3);
    assert_eq!(tasks[0].text, "Task 1");
    assert_eq!(tasks[0].status, "todo");
    assert_eq!(tasks[1].text, "Task 2");
    assert_eq!(tasks[1].status, "done");
    assert_eq!(tasks[2].text, "Task 3");
    assert_eq!(tasks[2].status, "todo");

    // Cleanup
    drop(services); // Drop store handles before DB cleanup
    cleanup_db(&db);
    std::fs::remove_dir_all(&ws_root).ok();
}

#[tokio::test]
async fn note_update_metadata() {
    let (services, ws, ws_root, db) = setup().await;

    // Create a note
    let create_input = NoteCreate {
        title: "Original Title".to_string(),
        content: Some("Content".to_string()),
        tags: Some(vec!["tag1".to_string()]),
        parent_id: None,
    };
    let note = services
        .create_note(ws.clone(), create_input, None, None)
        .await
        .expect("create note")
        .note;

    // Update metadata (title and tags)
    let update_result = services
        .update_note_metadata(
            ws.clone(),
            note.id.clone(),
            Some("New Title".to_string()),
            Some(vec!["tag2".to_string()]),
            None,
            None,
        )
        .await
        .expect("update metadata");

    assert!(update_result.ok);
    assert_eq!(update_result.note_id, note.id);

    // Verify the changes
    let updated_note = services
        .get_note(ws.clone(), note.id.clone())
        .await
        .expect("get note");
    assert_eq!(updated_note.title, "New Title");
    assert_eq!(updated_note.tags, vec!["tag2".to_string()]);
    assert_eq!(updated_note.content, "Content"); // Content unchanged

    // Cleanup
    drop(services); // Drop store handles before DB cleanup
    cleanup_db(&db);
    std::fs::remove_dir_all(&ws_root).ok();
}
