//! End-to-end `note.*` service tests over a temp SQLite store. Pure content
//! math is covered in `note_ops`; these assert persistence, the setContent
//! reduction guard, spec-title skip, workspace scoping, and error mapping.

use std::path::PathBuf;

use intent_core::{
    now_iso, ContentType, Error, Note, NoteAddInput, NoteEditInput, NoteEditLinesInput, NoteId,
    NoteVisibility, Workspace, WorkspaceActivity, WorkspaceApi, WorkspaceAttention, WorkspaceId,
    WorkspaceStatus,
};
use intent_store::Store;

use crate::Services;

struct TempDb {
    path: PathBuf,
}

impl TempDb {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!("intentd-svc-{}.db", uuid::Uuid::new_v4()));
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
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: ts.clone(),
        updated_at: ts,
        last_activity: None,
        tags: vec![],
        path: None,
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
        archived: false,
        archived_at: None,
    }
}

fn note(ws: &WorkspaceId, id: &str, content: &str) -> Note {
    let ts = now_iso();
    Note {
        id: NoteId::from(id),
        workspace_id: ws.clone(),
        title: "Title".to_string(),
        content: content.to_string(),
        content_type: ContentType::Markdown,
        tags: vec![],
        is_pinned: false,
        is_archived: false,
        is_default: false,
        parent_id: None,
        visibility: NoteVisibility::Workspace,
        task: None,
        created_at: ts.clone(),
        updated_at: ts,
    }
}

async fn setup(content: &str) -> (TempDb, Services, WorkspaceId, NoteId) {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");
    let id = NoteId::from("n1");
    store
        .insert_note(&note(&ws, "n1", content))
        .await
        .expect("note");
    let services = Services::new(store);
    (tmp, services, ws, id)
}

#[tokio::test]
async fn add_persists_and_reports_position() {
    let (_tmp, svc, ws, id) = setup("# A\nbody").await;
    let r = svc
        .add_to_note(
            ws.clone(),
            id.clone(),
            NoteAddInput {
                content: "more".into(),
                heading: None,
                position: Some("end".into()),
            },
        )
        .await
        .expect("add");
    assert_eq!(r.new_content, "# A\nbody\n\nmore");
    assert_eq!(r.position, "at end");
    let persisted = svc.get_note(ws, id).await.expect("get");
    assert_eq!(persisted.content, "# A\nbody\n\nmore");
}

#[tokio::test]
async fn edit_no_match_maps_to_internal() {
    let (_tmp, svc, ws, id) = setup("hello").await;
    let err = svc
        .edit_note(
            ws,
            id,
            NoteEditInput {
                old: "zzz".into(),
                new: "x".into(),
            },
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Internal(_)));
}

#[tokio::test]
async fn edit_lines_deletes_range() {
    let (_tmp, svc, ws, id) = setup("a\nb\nc").await;
    let r = svc
        .edit_note_lines(
            ws,
            id,
            NoteEditLinesInput {
                start: 2,
                end: 2,
                content: String::new(),
            },
        )
        .await
        .expect("editLines");
    assert_eq!(r.new_content, "a\nc");
    assert_eq!(r.total_lines_after, 2);
}

#[tokio::test]
async fn set_content_reduction_guard_requires_confirmation() {
    let (_tmp, svc, ws, id) = setup("0123456789ABCDEFGHIJ").await;
    let denied = svc
        .set_note_content(ws.clone(), id.clone(), "x".into(), false)
        .await;
    assert!(matches!(denied, Err(Error::Internal(_))));
    let ok = svc
        .set_note_content(ws, id, "x".into(), true)
        .await
        .expect("confirmed");
    assert_eq!(ok.new_content, "x");
}

#[tokio::test]
async fn update_metadata_skips_spec_title_but_applies_tags() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");
    store
        .insert_note(&note(&ws, "spec", "body"))
        .await
        .expect("note");
    let svc = Services::new(store);
    let spec = NoteId::from("spec");

    // Title-only on spec → skipped, title unchanged.
    let skipped = svc
        .update_note_metadata(ws.clone(), spec.clone(), Some("New".into()), None)
        .await
        .expect("meta");
    assert_eq!(skipped.skipped, Some(true));
    assert_eq!(
        svc.get_note(ws.clone(), spec.clone()).await.unwrap().title,
        "Title"
    );

    // Tags still apply on spec.
    let applied = svc
        .update_note_metadata(
            ws.clone(),
            spec.clone(),
            Some("New".into()),
            Some(vec!["a".into()]),
        )
        .await
        .expect("meta2");
    assert_eq!(applied.tags, Some(vec!["a".to_string()]));
    assert_eq!(svc.get_note(ws, spec).await.unwrap().title, "Title");
}

#[tokio::test]
async fn list_tasks_and_delete_and_scoping() {
    let (_tmp, svc, ws, id) = setup("- [ ] one\n- [x] two").await;
    let tasks = svc
        .list_note_tasks(ws.clone(), id.clone())
        .await
        .expect("tasks");
    assert_eq!(tasks.len(), 2);
    assert_eq!(tasks[1].status, "done");

    // Wrong workspace → not found (peer message → Internal).
    let other = WorkspaceId::new();
    assert!(matches!(
        svc.list_note_tasks(other, id.clone()).await,
        Err(Error::Internal(_))
    ));

    let del = svc
        .delete_note(ws.clone(), id.clone())
        .await
        .expect("delete");
    assert!(del.deleted);
    assert!(matches!(
        svc.get_note(ws, id).await,
        Err(Error::NotFound(_))
    ));
}

#[tokio::test]
async fn read_asset_reads_base64_from_assets_root() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    let root = std::env::temp_dir().join(format!("intentd-assets-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(root.join(&ws.0)).expect("mkdir");
    std::fs::write(root.join(&ws.0).join("img.png"), b"hello").expect("write asset");
    let svc = Services::new(store).with_assets_root(root.clone());

    let r = svc
        .read_asset(ws, "workspace-asset://host/img.png".into())
        .await
        .expect("read asset");
    assert_eq!(r.asset_id, "img.png");
    assert_eq!(r.mime_type, "image/png");
    assert_eq!(r.data, "aGVsbG8="); // base64("hello")
    let _ = std::fs::remove_dir_all(root);
}
