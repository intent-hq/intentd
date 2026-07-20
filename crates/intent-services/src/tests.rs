//! End-to-end `note.*` service tests over a temp SQLite store. Pure content
//! math is covered in `note_ops`; these assert persistence, the setContent
//! reduction guard, spec-title skip, workspace scoping, and error mapping.

use std::path::PathBuf;
use std::sync::Mutex;

use intent_core::{
    now_iso, AgentId, AgentSession, AgentStatus, ContentType, Error, Note, NoteAddInput,
    NoteCreate, NoteEditInput, NoteEditLinesInput, NoteId, NoteMetadata, NoteUpdateInput,
    NoteVisibility, Workspace, WorkspaceActivity, WorkspaceApi, WorkspaceAttention, WorkspaceId,
    WorkspaceStatus,
};
use intent_store::Store;

use crate::Services;

/// Guard for tests that mutate debounce env vars to prevent parallel test
/// races (env::set_var is process-global). Supports both LAST_ACTIVITY and
/// WORKSPACE_IDLE debounce vars.
static ENV_DEBOUNCE_LOCK: Mutex<()> = Mutex::new(());

/// RAII guard for debounce env vars: holds the lock, sets the vars, and restores
/// on drop to prevent leakage into other tests.
struct DebounceEnvGuard {
    _lock: std::sync::MutexGuard<'static, ()>,
    prior_last_activity: Option<std::ffi::OsString>,
    prior_workspace_idle: Option<std::ffi::OsString>,
}

impl DebounceEnvGuard {
    fn new(millis: &str) -> Self {
        let _lock = ENV_DEBOUNCE_LOCK.lock().unwrap();
        let prior_last_activity = std::env::var_os("LAST_ACTIVITY_DEBOUNCE_TEST_MS");
        let prior_workspace_idle = std::env::var_os("WORKSPACE_IDLE_DEBOUNCE_TEST_MS");
        std::env::set_var("LAST_ACTIVITY_DEBOUNCE_TEST_MS", millis);
        std::env::set_var("WORKSPACE_IDLE_DEBOUNCE_TEST_MS", millis);
        Self {
            _lock,
            prior_last_activity,
            prior_workspace_idle,
        }
    }
}

impl Drop for DebounceEnvGuard {
    fn drop(&mut self) {
        if let Some(val) = &self.prior_last_activity {
            std::env::set_var("LAST_ACTIVITY_DEBOUNCE_TEST_MS", val);
        } else {
            std::env::remove_var("LAST_ACTIVITY_DEBOUNCE_TEST_MS");
        }
        if let Some(val) = &self.prior_workspace_idle {
            std::env::set_var("WORKSPACE_IDLE_DEBOUNCE_TEST_MS", val);
        } else {
            std::env::remove_var("WORKSPACE_IDLE_DEBOUNCE_TEST_MS");
        }
    }
}

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

/// Drop-cleanup wrapper around a per-test workspaces root, paired with the
/// intent-services hermetic-tests guard (see `default_workspaces_root`). Every
/// test that constructs a `Services` reachable from workspace provisioning
/// **must** attach one via `.with_workspaces_root(root.path().to_path_buf())`;
/// otherwise the guard panics rather than writing under `~/intent/workspaces`.
struct WorkspacesRoot(PathBuf);

impl WorkspacesRoot {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!("intentd-wss-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&p).expect("mkdir hermetic workspaces root");
        Self(p)
    }

    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for WorkspacesRoot {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
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
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
        cow_supported: None,
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
        metadata: NoteMetadata::default(),
        created_at: ts.clone(),
        rev: 0,
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

/// `workspace.list` / `workspace.get` populate the iOS card aggregates
/// (`taskStats` / `agentSummary` / `diffSummary`) computed from the workspace's
/// real notes, agents, and git state, with the nested wire shape iOS decodes.
#[tokio::test]
async fn workspace_list_and_get_populate_card_aggregates() {
    use intent_core::{AgentId, AgentSession, AgentStatus, TaskMetadata, TaskStatus};

    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");

    // Spec note links four task notes (the cancelled one is excluded from
    // `total`); a spec-linked filter is exercised since the spec body has links.
    let spec = note(
        &ws,
        "spec",
        "- [A](intent://local/task/task-a)\n- [B](intent://local/task/task-b)\n\
         - [C](intent://local/task/task-c)\n- [D](intent://local/task/task-d)",
    );
    store.insert_note(&spec).await.expect("spec");

    let mk_task = |id: &str, status: TaskStatus| {
        let mut tn = note(&ws, id, "body");
        tn.parent_id = Some(NoteId::from("spec"));
        tn.metadata.task = Some(TaskMetadata {
            status,
            ..Default::default()
        });
        tn
    };
    store
        .insert_note(&mk_task("task-a", TaskStatus::Complete))
        .await
        .unwrap();
    store
        .insert_note(&mk_task("task-b", TaskStatus::InProgress))
        .await
        .unwrap();
    store
        .insert_note(&mk_task("task-c", TaskStatus::ReviewRequired))
        .await
        .unwrap();
    store
        .insert_note(&mk_task("task-d", TaskStatus::Cancelled))
        .await
        .unwrap();

    let mk_agent = |id: &str, name: &str, specialist: Option<&str>| AgentSession {
        id: AgentId::from(id),
        workspace_id: ws.clone(),
        parent_agent_id: None,
        backend_session_id: None,
        acp_session_id: None,
        name: name.to_string(),
        name_explicitly_set: true,
        model: None,
        provider: None,
        system_prompt: None,
        specialist: specialist.map(str::to_string),
        status: AgentStatus::Active,
        is_active: true,
        messages: vec![],
        stats: None,
        task_note_id: None,
        skip_auto_commit: false,
        completion_report: None,
        completion_report_timestamp: None,
        delegation_depth: None,
        initial_message: None,
        context_references: None,
        image_blocks: None,
        is_background: false,
        metadata: None,
        created_at: now_iso(),
        updated_at: now_iso(),
        sandbox_id: None,
        sandbox_path: None,
        sandbox_branch: None,
        stop_reason: None,
    };
    store
        .insert_agent_session(&mk_agent("agent-1", "Builder", Some("implementor")))
        .await
        .unwrap();
    store
        .insert_agent_session(&mk_agent("agent-2", "Verifier", None))
        .await
        .unwrap();

    let svc = Services::new(store);

    // workspace.get
    let got = svc.get_workspace(ws.clone()).await.expect("get");
    let stats = got.task_stats.expect("task_stats present");
    assert_eq!(stats.total, 3); // cancelled excluded
    assert_eq!(stats.completed, 1);
    assert_eq!(stats.in_progress, 2); // in_progress + review_required
    let summary = got.agent_summary.expect("agent_summary present");
    assert_eq!(summary.count, 2);
    assert!(summary.agents.iter().any(|a| a.name == "Builder"
        && a.specialist.as_deref() == Some("implementor")
        && !a.is_streaming
        && !a.is_responding));
    // `agentIds` mirrors the agents used to build `agents` (forward-compat).
    let summary_ids: Vec<_> = summary.agent_ids.iter().map(|i| i.0.clone()).collect();
    let agent_ids: Vec<_> = summary.agents.iter().map(|a| a.id.0.clone()).collect();
    assert_eq!(summary_ids, agent_ids);
    assert!(summary_ids.contains(&"agent-1".to_string()));
    // No git worktree → diffSummary omitted.
    assert!(got.diff_summary.is_none());

    // workspace.list carries the same aggregates; assert the nested wire shape.
    let list = svc.list_workspaces(false).await.expect("list");
    let v = serde_json::to_value(&list[0]).unwrap();
    assert_eq!(v["taskStats"]["total"], 3);
    assert_eq!(v["taskStats"]["completed"], 1);
    assert_eq!(v["taskStats"]["inProgress"], 2);
    assert_eq!(v["agentSummary"]["count"], 2);
    assert_eq!(v["agentSummary"]["agents"][0]["id"], "agent-1");
    assert_eq!(v["agentSummary"]["agentIds"][0], "agent-1");
    assert_eq!(v["agentSummary"]["agentIds"].as_array().unwrap().len(), 2);
    assert!(v.get("diffSummary").is_none());
}

/// `crossWorkspace.listSiblings` returns only same-`repositoryPath` peers
/// (self filtered out, other-repo filtered out) with the PascalCase status.
#[tokio::test]
async fn cross_workspace_list_siblings_scopes_to_repository() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    let mk = |id: &WorkspaceId, title: &str, repo: Option<&str>| {
        let mut w = workspace(id);
        w.title = title.to_string();
        w.repository_path = repo.map(str::to_string);
        w
    };

    let caller = WorkspaceId::from("ws-caller");
    let sibling = WorkspaceId::from("ws-sibling");
    let other_repo = WorkspaceId::from("ws-other");
    let no_repo = WorkspaceId::from("ws-norepo");
    store
        .insert_workspace(&mk(&caller, "Caller", Some("/repo/a")))
        .await
        .unwrap();
    store
        .insert_workspace(&mk(&sibling, "", Some("/repo/a")))
        .await
        .unwrap();
    store
        .insert_workspace(&mk(&other_repo, "Other", Some("/repo/b")))
        .await
        .unwrap();
    store
        .insert_workspace(&mk(&no_repo, "NoRepo", None))
        .await
        .unwrap();

    let svc = Services::new(store);
    let v = svc
        .cross_workspace_list_siblings(caller.clone())
        .await
        .expect("siblings");
    let arr = v.as_array().expect("array");
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["id"], "ws-sibling");
    // Empty title falls back to "Untitled"; status is PascalCase.
    assert_eq!(arr[0]["title"], "Untitled");
    assert_eq!(arr[0]["status"], "Active");
    assert!(arr[0]["createdAt"].is_string());
}

/// A caller with no `repositoryPath` cannot list siblings (mirrors the TS
/// "not associated with a repository" error).
#[tokio::test]
async fn cross_workspace_list_siblings_requires_repository() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let caller = WorkspaceId::from("ws-norepo");
    store.insert_workspace(&workspace(&caller)).await.unwrap();
    let svc = Services::new(store);
    let err = svc
        .cross_workspace_list_siblings(caller)
        .await
        .expect_err("should error");
    match err {
        Error::Internal(m) => assert!(m.contains("not associated with a repository"), "{m}"),
        other => panic!("expected Internal, got {other:?}"),
    }
}

/// Cross-repo `readNote`/`listNotes` are access-denied; a same-repo sibling
/// note reads back with numbered content and source metadata.
#[tokio::test]
async fn cross_workspace_read_note_enforces_access_and_shape() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    let mk = |id: &WorkspaceId, repo: &str| {
        let mut w = workspace(id);
        w.repository_path = Some(repo.to_string());
        w
    };
    let caller = WorkspaceId::from("ws-caller");
    let sibling = WorkspaceId::from("ws-sibling");
    let other = WorkspaceId::from("ws-other");
    store
        .insert_workspace(&mk(&caller, "/repo/a"))
        .await
        .unwrap();
    store
        .insert_workspace(&mk(&sibling, "/repo/a"))
        .await
        .unwrap();
    store
        .insert_workspace(&mk(&other, "/repo/b"))
        .await
        .unwrap();
    store
        .insert_note(&note(&sibling, "sib-note", "l1\nl2"))
        .await
        .unwrap();

    let svc = Services::new(store);

    // Same-repo sibling read: full shape with numbered content + line count.
    let v = svc
        .cross_workspace_read_note(caller.clone(), sibling.clone(), NoteId::from("sib-note"))
        .await
        .expect("read");
    assert_eq!(v["id"], "sib-note");
    assert_eq!(v["content"], "l1\nl2");
    assert_eq!(v["numberedContent"], "   1 | l1\n   2 | l2");
    assert_eq!(v["sourceWorkspaceId"], "ws-sibling");
    assert_eq!(v["lineCount"], 2);

    // listNotes for the sibling returns the bare-array shape.
    let v = svc
        .cross_workspace_list_notes(caller.clone(), sibling.clone())
        .await
        .expect("list");
    let arr = v.as_array().expect("array");
    assert_eq!(arr[0]["id"], "sib-note");
    assert!(arr[0]["createdAt"].is_string());

    // Cross-repo access is denied.
    let err = svc
        .cross_workspace_list_notes(caller.clone(), other.clone())
        .await
        .expect_err("denied");
    match err {
        Error::Internal(m) => assert!(m.contains("Access denied"), "{m}"),
        other => panic!("expected Internal, got {other:?}"),
    }

    // A missing note in a valid sibling surfaces the "Note not found" message.
    let err = svc
        .cross_workspace_read_note(caller, sibling, NoteId::from("nope"))
        .await
        .expect_err("missing");
    match err {
        Error::Internal(m) => assert!(m.contains("Note not found"), "{m}"),
        other => panic!("expected Internal, got {other:?}"),
    }
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
            None,
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
            None,
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
            None,
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
        .set_note_content(ws.clone(), id.clone(), "x".into(), false, None, None)
        .await;
    assert!(matches!(denied, Err(Error::Internal(_))));
    let ok = svc
        .set_note_content(ws, id, "x".into(), true, None, None)
        .await
        .expect("confirmed");
    assert_eq!(ok.new_content, "x");
}

/// A5 (CRDT note-merge, PROTOCOL §5.2): two `note.setContent` calls whose new
/// content each observes the other's write survive in the merged result. The
/// second write's `oldContent` still points at the persisted state before it
/// ran, but the CRDT diff against the yrs doc's *current* text preserves the
/// first write's characters — the FE parity signal that the daemon no longer
/// last-write-wins on concurrent full-content writes.
#[tokio::test]
async fn set_content_merges_concurrent_writes() {
    let (_tmp, svc, ws, id) = setup("BODY").await;

    // Author A appends a line at the end.
    let a = svc
        .set_note_content(
            ws.clone(),
            id.clone(),
            "BODY\nA-line".into(),
            true,
            None,
            None,
        )
        .await
        .expect("A write");
    assert_eq!(a.new_content, "BODY\nA-line");

    // Author B prepends a line, having read the post-A content as baseline —
    // the yrs merge stitches both edits together.
    let b = svc
        .set_note_content(
            ws.clone(),
            id.clone(),
            "B-line\nBODY\nA-line".into(),
            true,
            None,
            None,
        )
        .await
        .expect("B write");
    assert_eq!(b.new_content, "B-line\nBODY\nA-line");

    // A surgical mutation invalidates the CRDT session so the next
    // `setContent` reseeds from the fresh persisted content.
    svc.edit_note(
        ws.clone(),
        id.clone(),
        NoteEditInput {
            old: "A-line".into(),
            new: "A-line (edited)".into(),
        },
        None,
    )
    .await
    .expect("edit");
    let c = svc
        .set_note_content(
            ws,
            id,
            "B-line\nBODY\nA-line (edited)\nC-line".into(),
            true,
            None,
            None,
        )
        .await
        .expect("C write");
    assert_eq!(c.new_content, "B-line\nBODY\nA-line (edited)\nC-line");
}

#[tokio::test]
async fn update_note_expected_version_gate_hit_miss_absent() {
    let (_tmp, svc, ws, id) = setup("v0").await;

    // HIT: the freshly-inserted note is at rev 0; the matching expectedVersion
    // writes successfully. (The returned note mirrors the pre-write in-memory
    // copy, matching the existing `update_note` contract; the persisted bump is
    // asserted via the conflict re-read below.)
    let hit = svc
        .update_note(
            ws.clone(),
            id.clone(),
            NoteUpdateInput {
                content: Some("v1".into()),
                expected_version: Some(0),
                ..Default::default()
            },
        )
        .await
        .expect("expectedVersion hit writes");
    assert_eq!(hit.content, "v1");

    // MISS: a stale expectedVersion (0) yields a Conflict carrying the current
    // entity, which proves the HIT persisted and bumped rev → 1.
    let miss = svc
        .update_note(
            ws.clone(),
            id.clone(),
            NoteUpdateInput {
                content: Some("v2-should-not-persist".into()),
                expected_version: Some(0),
                ..Default::default()
            },
        )
        .await;
    match miss {
        Err(Error::Conflict { current }) => {
            assert_eq!(current["rev"], 1);
            assert_eq!(current["content"], "v1");
        }
        other => panic!("expected Conflict, got {other:?}"),
    }

    // ABSENT: no expectedVersion → last-writer-wins (unconditional write).
    let absent = svc
        .update_note(
            ws.clone(),
            id.clone(),
            NoteUpdateInput {
                content: Some("v2".into()),
                expected_version: None,
                ..Default::default()
            },
        )
        .await
        .expect("absent degrades to last-writer-wins");
    assert_eq!(absent.content, "v2");

    // The unconditional write also bumps rev → 2: a stale expectedVersion (1)
    // now conflicts, carrying the current entity (rev 2, content "v2").
    let stale = svc
        .update_note(
            ws,
            id,
            NoteUpdateInput {
                content: Some("v3-should-not-persist".into()),
                expected_version: Some(1),
                ..Default::default()
            },
        )
        .await;
    match stale {
        Err(Error::Conflict { current }) => {
            assert_eq!(current["rev"], 2);
            assert_eq!(current["content"], "v2");
        }
        other => panic!("expected Conflict, got {other:?}"),
    }
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
        .update_note_metadata(
            ws.clone(),
            spec.clone(),
            Some("New".into()),
            None,
            None,
            None,
        )
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
            None,
            None,
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
        .delete_note(ws.clone(), id.clone(), None)
        .await
        .expect("delete");
    assert!(del.deleted);
    assert!(matches!(
        svc.get_note(ws, id).await,
        Err(Error::NotFound(_))
    ));
}

/// `note.delete` cascade parity with the reference `deleteNote` in
/// `src/features/notes/main/notes.service.ts` (which delegates to
/// `NotesRepository.delete`, unlinking the comments file alongside the note).
/// Migration 0030 encodes both cleanups at the schema layer: an `ON DELETE
/// CASCADE` composite FK on `comment(note_id, workspace_id)` and the
/// `note_parent_set_null_on_delete` trigger clearing children's `parent_id`
/// scoped to the same workspace. This regression pins that when a note is
/// deleted, (a) its comments are removed, (b) its children (e.g. linked task
/// notes) survive with `parent_id = NULL`, and (c) an unrelated note in the
/// same workspace is untouched.
#[tokio::test]
async fn delete_note_cascades_comments_and_unlinks_children() {
    use intent_core::{
        AuthorType, Comment, CommentAnchor, CommentAnchorType, CommentStatus, CommentType,
    };

    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");

    // Parent note that will be deleted.
    let parent_id = NoteId::from("parent");
    store
        .insert_note(&note(&ws, "parent", "parent body"))
        .await
        .expect("insert parent");

    // Child (task) note linked via `parent_id` — reference semantics leave
    // linked child task notes alone, so we expect the row to survive with a
    // cleared `parent_id`, not to be deleted.
    let child_id = NoteId::from("child");
    let mut child = note(&ws, "child", "child body");
    child.parent_id = Some(parent_id.clone());
    store.insert_note(&child).await.expect("insert child");

    // Unrelated note in the same workspace: no link, no comments; it must not
    // be touched by the parent's delete.
    let other_id = NoteId::from("other");
    store
        .insert_note(&note(&ws, "other", "other body"))
        .await
        .expect("insert other");

    // Two comments on the parent, exercising the composite FK cascade.
    let now = now_iso();
    let mk_comment = |id: &str, thread: &str| Comment {
        id: id.to_string(),
        thread_id: thread.to_string(),
        note_id: Some(parent_id.clone()),
        kind: CommentType::Comment,
        content: format!("{id} body"),
        author: "alice".to_string(),
        author_type: AuthorType::User,
        status: CommentStatus::Open,
        parent_id: None,
        anchor: CommentAnchor {
            kind: CommentAnchorType::Range,
            start_id: None,
            end_id: None,
            point_id: None,
        },
        anchor_text: None,
        anchor_before: None,
        anchor_after: None,
        suggestion_original: None,
        suggestion_proposed: None,
        agent_id: None,
        is_orphaned: None,
        created_at: now.clone(),
        updated_at: now.clone(),
    };
    let c1 = mk_comment("c1", "t1");
    let c2 = mk_comment("c2", "t1");
    store.insert_comment(&ws, &c1).await.expect("insert c1");
    store.insert_comment(&ws, &c2).await.expect("insert c2");

    let svc = Services::new(store);
    assert_eq!(
        svc.store()
            .list_comments(&parent_id)
            .await
            .expect("pre-delete comments")
            .len(),
        2,
    );

    let del = svc
        .delete_note(ws.clone(), parent_id.clone(), None)
        .await
        .expect("delete parent");
    assert!(del.deleted);

    // Parent row is gone.
    assert!(matches!(
        svc.get_note(ws.clone(), parent_id.clone()).await,
        Err(Error::NotFound(_))
    ));

    // Comments cascaded away via the 0030 composite FK.
    assert!(
        svc.store()
            .list_comments(&parent_id)
            .await
            .expect("post-delete comments")
            .is_empty(),
        "comments must cascade with the deleted note",
    );
    assert!(matches!(
        svc.store().get_comment("c1").await,
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        svc.store().get_comment("c2").await,
        Err(Error::NotFound(_))
    ));

    // Child (linked task) note survives with `parent_id` cleared by the 0030
    // trigger — the reference deleteNote leaves child task notes untouched.
    let child_after = svc
        .get_note(ws.clone(), child_id.clone())
        .await
        .expect("child survives parent delete");
    assert_eq!(child_after.parent_id, None);

    // Unrelated note in the same workspace is untouched.
    let other_after = svc
        .get_note(ws, other_id)
        .await
        .expect("unrelated note untouched");
    assert_eq!(other_after.content, "other body");
}

#[tokio::test]
async fn update_metadata_expected_version_gate_hit_and_miss() {
    let (_tmp, svc, ws, id) = setup("body").await;

    // HIT: the freshly-inserted note is at rev 0; a matching expectedVersion
    // applies the title and bumps rev → 1.
    let hit = svc
        .update_note_metadata(
            ws.clone(),
            id.clone(),
            Some("Renamed".into()),
            None,
            Some(0),
            None,
        )
        .await
        .expect("expectedVersion hit writes");
    assert_eq!(hit.title, Some("Renamed".to_string()));

    // MISS: a stale expectedVersion (0) now conflicts, carrying the current
    // entity (rev 1, title "Renamed") and leaving the note unchanged.
    let miss = svc
        .update_note_metadata(
            ws.clone(),
            id.clone(),
            Some("Nope".into()),
            None,
            Some(0),
            None,
        )
        .await;
    match miss {
        Err(Error::Conflict { current }) => {
            assert_eq!(current["rev"], 1);
            assert_eq!(current["title"], "Renamed");
        }
        other => panic!("expected Conflict, got {other:?}"),
    }
    assert_eq!(svc.get_note(ws, id).await.unwrap().title, "Renamed");
}

#[tokio::test]
async fn delete_expected_version_gate_miss_then_hit_and_absent() {
    let (_tmp, svc, ws, id) = setup("body").await;

    // MISS: a stale expectedVersion (5, but stored rev is 0) → Conflict carrying
    // the current entity snapshot; the note is NOT deleted.
    let miss = svc.delete_note(ws.clone(), id.clone(), Some(5)).await;
    match miss {
        Err(Error::Conflict { current }) => {
            assert_eq!(current["rev"], 0);
            assert_eq!(current["id"], id.0);
        }
        other => panic!("expected Conflict, got {other:?}"),
    }
    assert!(svc.get_note(ws.clone(), id.clone()).await.is_ok());

    // HIT: a matching expectedVersion (0) deletes the note.
    let del = svc
        .delete_note(ws.clone(), id.clone(), Some(0))
        .await
        .expect("expectedVersion hit deletes");
    assert!(del.deleted);
    assert!(matches!(
        svc.get_note(ws.clone(), id.clone()).await,
        Err(Error::NotFound(_))
    ));

    // ABSENT: deleting an already-absent note (no expectedVersion) is NotFound.
    assert!(matches!(
        svc.delete_note(ws, id, None).await,
        Err(Error::NotFound(_) | Error::Internal(_))
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

// ---------------------------------------------------------------------------
// task.* tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn task_update_atomic_and_conflict() {
    let (_tmp, svc, ws, id) = setup("intro\n- [ ] do the thing\ntail").await;
    // Atomic status-only update on line 2.
    let r = svc
        .task_update(
            ws.clone(),
            id.clone(),
            2,
            None,
            Some("done".into()),
            Some("do the thing".into()),
        )
        .await
        .expect("update");
    assert_eq!(r.status, "done");
    assert_eq!(r.previous_text, "do the thing");
    assert_eq!(
        svc.get_note(ws.clone(), id.clone()).await.unwrap().content,
        "intro\n- [x] do the thing\ntail"
    );
    // Conflict: the expected text no longer matches.
    let err = svc
        .task_update(
            ws,
            id,
            2,
            None,
            Some("todo".into()),
            Some("stale text".into()),
        )
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Internal(ref m) if m.contains("Conflict detected")));
}

#[tokio::test]
async fn task_update_status_and_not_found() {
    let (_tmp, svc, ws, id) = setup("- [ ] alpha\n- [ ] beta").await;
    let r = svc
        .task_update_status(ws.clone(), id.clone(), "beta".into(), "in-progress".into())
        .await
        .expect("updateStatus");
    assert_eq!(r.status, "in-progress");
    assert_eq!(
        svc.get_note(ws.clone(), id.clone()).await.unwrap().content,
        "- [ ] alpha\n- [/] beta"
    );
    assert!(svc
        .task_update_status(ws, id, "ghost".into(), "done".into())
        .await
        .is_err());
}

#[tokio::test]
async fn mark_as_task_then_update_note_status_and_get_my_task() {
    let (_tmp, svc, ws, id) = setup("# Parent task").await;
    let marked = svc
        .mark_as_task(
            ws.clone(),
            id.clone(),
            "not_started".into(),
            vec!["criterion one".into()],
            Some("M".into()),
        )
        .await
        .expect("markAsTask");
    assert_eq!(marked.status, intent_core::TaskStatus::NotStarted);

    // updateNoteStatus → in_progress sets startedAt.
    let upd = svc
        .task_update_note_status(ws.clone(), id.clone(), "in_progress".into(), None, None)
        .await
        .expect("updateNoteStatus");
    assert_eq!(upd.status, intent_core::TaskStatus::InProgress);
    assert!(upd
        .note
        .metadata
        .task
        .as_ref()
        .unwrap()
        .started_at
        .is_some());

    // Invalid status string rejected with the TS-style message.
    assert!(svc
        .task_update_note_status(ws.clone(), id.clone(), "bogus".into(), None, None)
        .await
        .is_err());

    // createPrerequisite makes a child; getMyTask reports it as a subtask.
    let prereq = svc
        .create_prerequisite(
            ws.clone(),
            id.clone(),
            "Child Step".into(),
            Some("details".into()),
            None,
        )
        .await
        .expect("createPrerequisite");
    assert_eq!(prereq.title, "Child Step");

    let mine = svc.get_my_task(ws, id).await.expect("getMyTask");
    assert_eq!(
        mine.task_metadata.acceptance_criteria,
        vec!["criterion one"]
    );
    assert_eq!(mine.subtasks.len(), 1);
    assert_eq!(mine.subtasks[0].title, "Child Step");
    assert_eq!(mine.subtasks[0].status, "not_started");
}

/// `task.list` returns the workspace's spec-linked task notes projected into the
/// `WorkspaceTask` shape (`{ id, title, status, updatedAt }`), including
/// cancelled, and excluding the spec, non-task notes, non-children, and
/// non-spec-linked task notes. `task.get` returns a single task note in the same
/// shape; unknown / cross-workspace ids surface `NotFound` and non-task notes
/// surface an `Internal` error.
#[tokio::test]
async fn task_list_and_get_project_workspace_tasks() {
    use intent_core::{TaskMetadata, TaskStatus};

    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");

    // Spec links three task notes; a fourth task note is unlinked (excluded).
    let spec = note(
        &ws,
        "spec",
        "- [A](intent://local/task/task-a)\n- [B](intent://local/task/task-b)\n\
         - [C](intent://local/task/task-c)",
    );
    store.insert_note(&spec).await.expect("spec");

    let mk_task = |id: &str, title: &str, status: TaskStatus| {
        let mut tn = note(&ws, id, "body");
        tn.title = title.to_string();
        tn.parent_id = Some(NoteId::from("spec"));
        tn.metadata.task = Some(TaskMetadata {
            status,
            ..Default::default()
        });
        tn
    };
    store
        .insert_note(&mk_task("task-a", "Alpha", TaskStatus::InProgress))
        .await
        .unwrap();
    store
        .insert_note(&mk_task("task-b", "Beta", TaskStatus::Complete))
        .await
        .unwrap();
    store
        .insert_note(&mk_task("task-c", "Gamma", TaskStatus::Cancelled))
        .await
        .unwrap();
    // Unlinked task note (spec has links, so excluded).
    store
        .insert_note(&mk_task("task-x", "Orphan", TaskStatus::NotStarted))
        .await
        .unwrap();
    // Non-task child of spec (excluded — no task metadata).
    let mut plain = note(&ws, "plain", "body");
    plain.parent_id = Some(NoteId::from("spec"));
    store.insert_note(&plain).await.unwrap();

    let svc = Services::new(store);

    // task.list returns the three spec-linked task notes (cancelled included)
    // plus the workspace-wide stats aggregate (full set, mirrors the FE
    // `computeTaskStats`: total excludes cancelled, completed counts complete,
    // inProgress counts in_progress + review_required).
    let result = svc.task_list(ws.clone(), None).await.expect("task.list");
    assert_eq!(result.tasks.len(), 3);
    let ids: Vec<&str> = result.tasks.iter().map(|t| t.id.as_str()).collect();
    assert_eq!(ids, vec!["task-a", "task-b", "task-c"]);
    let alpha = &result.tasks[0];
    assert_eq!(alpha.title, "Alpha");
    assert_eq!(alpha.status, TaskStatus::InProgress);
    assert!(!alpha.updated_at.is_empty());
    assert_eq!(result.stats.total, 2);
    assert_eq!(result.stats.completed, 1);
    assert_eq!(result.stats.in_progress, 1);

    // Status filter narrows `tasks` only; `stats` stays the full rollup so the
    // FE renders the progress bar verbatim regardless of the active filter.
    let done = svc
        .task_list(ws.clone(), Some("complete".into()))
        .await
        .expect("task.list filtered");
    assert_eq!(done.tasks.len(), 1);
    assert_eq!(done.tasks[0].id.as_str(), "task-b");
    assert_eq!(done.stats.total, 2);
    assert_eq!(done.stats.completed, 1);
    assert_eq!(done.stats.in_progress, 1);

    // Invalid status string is rejected.
    assert!(svc
        .task_list(ws.clone(), Some("bogus".into()))
        .await
        .is_err());

    // task.get returns a single task note in the WorkspaceTask shape.
    let got = svc
        .task_get(ws.clone(), NoteId::from("task-a"))
        .await
        .expect("task.get");
    assert_eq!(got.id.as_str(), "task-a");
    assert_eq!(got.title, "Alpha");
    assert_eq!(got.status, TaskStatus::InProgress);

    // Unknown id → NotFound; non-task note → Internal "Note is not a task".
    assert!(matches!(
        svc.task_get(ws.clone(), NoteId::from("missing")).await,
        Err(Error::NotFound(_))
    ));
    assert!(svc.task_get(ws, NoteId::from("plain")).await.is_err());
}

#[tokio::test]
async fn assign_agent_validates_and_starts_task() {
    let (_tmp, svc, ws, id) = setup("# Task").await;
    svc.mark_as_task(ws.clone(), id.clone(), "not_started".into(), vec![], None)
        .await
        .expect("markAsTask");
    // Bad agent id → error.
    assert!(svc
        .assign_agent(ws.clone(), id.clone(), "not-an-agent".into())
        .await
        .is_err());
    // Valid agent id → assigned and status flips to in_progress.
    let agent = "agent-b0a8044a-5eac-4b52-8456-15d3b784decb";
    let r = svc
        .assign_agent(ws.clone(), id.clone(), agent.into())
        .await
        .expect("assignAgent");
    assert_eq!(r.agent_id.0, agent);
    let note = svc.get_note(ws, id).await.unwrap();
    let task = note.metadata.task.unwrap();
    assert_eq!(task.status, intent_core::TaskStatus::InProgress);
    assert_eq!(task.assigned_agent_ids[0].0, agent);
}

#[tokio::test]
async fn remove_agent_from_all_tasks_strips_id_only_from_matching_tasks() {
    use intent_core::{AgentId, TaskMetadata, TaskStatus};

    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");

    let victim = AgentId::from("agent-victim");
    let other = AgentId::from("agent-other");

    // Task A: assigned to both `victim` and `other`.
    let mut a = note(&ws, "task-a", "a");
    a.metadata.task = Some(TaskMetadata {
        status: TaskStatus::InProgress,
        assigned_agent_ids: vec![victim.clone(), other.clone()],
        ..Default::default()
    });
    store.insert_note(&a).await.unwrap();

    // Task B: assigned only to `other` (must be left untouched).
    let mut b = note(&ws, "task-b", "b");
    b.metadata.task = Some(TaskMetadata {
        status: TaskStatus::NotStarted,
        assigned_agent_ids: vec![other.clone()],
        ..Default::default()
    });
    store.insert_note(&b).await.unwrap();

    // Task C: assigned only to `victim`.
    let mut c = note(&ws, "task-c", "c");
    c.metadata.task = Some(TaskMetadata {
        status: TaskStatus::NotStarted,
        assigned_agent_ids: vec![victim.clone()],
        ..Default::default()
    });
    store.insert_note(&c).await.unwrap();

    // Non-task note: must be skipped.
    store
        .insert_note(&note(&ws, "plain", "not a task"))
        .await
        .unwrap();

    let svc = Services::new(store);
    let r = svc
        .remove_agent_from_all_tasks(ws.clone(), victim.clone())
        .await
        .expect("removeAgentFromAllTasks");
    assert!(r.ok);
    assert_eq!(r.updated_count, 2);

    let a = svc
        .get_note(ws.clone(), NoteId::from("task-a"))
        .await
        .unwrap();
    assert_eq!(
        a.metadata.task.unwrap().assigned_agent_ids,
        vec![other.clone()]
    );
    let b = svc
        .get_note(ws.clone(), NoteId::from("task-b"))
        .await
        .unwrap();
    assert_eq!(b.metadata.task.unwrap().assigned_agent_ids, vec![other]);
    let c = svc
        .get_note(ws.clone(), NoteId::from("task-c"))
        .await
        .unwrap();
    assert!(c.metadata.task.unwrap().assigned_agent_ids.is_empty());

    // Idempotent: replaying with the now-absent id updates nothing.
    let r2 = svc
        .remove_agent_from_all_tasks(ws, victim)
        .await
        .expect("removeAgentFromAllTasks-replay");
    assert!(r2.ok);
    assert_eq!(r2.updated_count, 0);
}

#[tokio::test]
async fn convert_blocks_creates_children_idempotently() {
    let content = "intro\n@@@task\n# Build API\nBuild the thing.\n@@@\ntail";
    let (_tmp, svc, ws, id) = setup(content).await;
    let r = svc
        .convert_task_blocks(ws.clone(), id.clone(), None)
        .await
        .expect("convertBlocks");
    assert_eq!(r.converted_count, 1);
    assert_eq!(r.created_note_ids.len(), 1);
    let updated = svc.get_note(ws.clone(), id.clone()).await.unwrap();
    assert!(updated
        .content
        .contains("- [ ] [Build API](intent://local/task/"));
    assert!(!updated.content.contains("@@@task"));

    // The conversion write appends a version snapshot whose content matches
    // the converted (fence-free) parent content (TS parity: the reference
    // pushes a version as part of the conversion save).
    let versions = svc
        .store
        .list_note_versions(&ws, &id)
        .await
        .expect("versions");
    assert_eq!(versions.len(), 1);
    let v = svc
        .store
        .get_note_version(&ws, &id, versions[0].v)
        .await
        .expect("version");
    assert_eq!(v.content, updated.content);

    // Re-running is idempotent: the existing child is reused, none created.
    let r2 = svc
        .convert_task_blocks(ws.clone(), id.clone(), None)
        .await
        .expect("convertBlocks2");
    assert_eq!(r2.converted_count, 0);
    assert!(r2.created_note_ids.is_empty());

    // No-op re-run must not append another version.
    let versions = svc
        .store
        .list_note_versions(&ws, &id)
        .await
        .expect("versions");
    assert_eq!(versions.len(), 1);
}

// ---------------------------------------------------------------------------
// Version-author attribution: `capture_note_version` stamps a `NoteVersionAuthor`
// resolved from the caller. Reference parity with `notes.service.ts` L518-555:
// `Some(agent_id)` → `{id, name: session.name, type: "agent"}` (name falls
// back to the id string when the session lookup fails); `None` → the FE user
// author `{id: "user", name: "User", type: "user"}`. Only genuinely internal
// daemon writes (workspace-seed spec snapshot) keep the `system`/"intentd"
// stamp.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn note_add_stamps_user_author_when_caller_is_none() {
    let (_tmp, svc, ws, id) = setup("body").await;
    svc.add_to_note(
        ws.clone(),
        id.clone(),
        NoteAddInput {
            content: "more".into(),
            heading: None,
            position: None,
        },
        None,
    )
    .await
    .expect("add");
    let versions = svc
        .store
        .list_note_versions(&ws, &id)
        .await
        .expect("versions");
    let last = versions.last().expect("at least one version");
    assert_eq!(last.author.id, "user");
    assert_eq!(last.author.name, "User");
    assert_eq!(last.author.author_type, "user");
}

#[tokio::test]
async fn note_add_stamps_agent_author_with_session_name() {
    use intent_core::{AgentId, AgentSession, AgentStatus};
    let (_tmp, svc, ws, id) = setup("body").await;
    let agent_id = AgentId::from("agent-writer");
    let session = AgentSession {
        id: agent_id.clone(),
        workspace_id: ws.clone(),
        parent_agent_id: None,
        backend_session_id: None,
        acp_session_id: None,
        name: "Writer".to_string(),
        name_explicitly_set: true,
        model: None,
        provider: None,
        system_prompt: None,
        specialist: None,
        status: AgentStatus::Active,
        is_active: true,
        messages: vec![],
        stats: None,
        task_note_id: None,
        skip_auto_commit: false,
        completion_report: None,
        completion_report_timestamp: None,
        delegation_depth: None,
        initial_message: None,
        context_references: None,
        image_blocks: None,
        is_background: false,
        metadata: None,
        created_at: now_iso(),
        updated_at: now_iso(),
        sandbox_id: None,
        sandbox_path: None,
        sandbox_branch: None,
        stop_reason: None,
    };
    svc.store
        .insert_agent_session(&session)
        .await
        .expect("session");
    svc.add_to_note(
        ws.clone(),
        id.clone(),
        NoteAddInput {
            content: "more".into(),
            heading: None,
            position: None,
        },
        Some(agent_id.clone()),
    )
    .await
    .expect("add");
    let versions = svc
        .store
        .list_note_versions(&ws, &id)
        .await
        .expect("versions");
    let last = versions.last().expect("at least one version");
    assert_eq!(last.author.id, "agent-writer");
    assert_eq!(last.author.name, "Writer");
    assert_eq!(last.author.author_type, "agent");
}

#[tokio::test]
async fn note_add_falls_back_to_agent_id_when_session_missing() {
    use intent_core::AgentId;
    let (_tmp, svc, ws, id) = setup("body").await;
    let agent_id = AgentId::from("agent-ghost");
    svc.add_to_note(
        ws.clone(),
        id.clone(),
        NoteAddInput {
            content: "more".into(),
            heading: None,
            position: None,
        },
        Some(agent_id.clone()),
    )
    .await
    .expect("add");
    let versions = svc
        .store
        .list_note_versions(&ws, &id)
        .await
        .expect("versions");
    let last = versions.last().expect("at least one version");
    assert_eq!(last.author.id, "agent-ghost");
    assert_eq!(last.author.name, "agent-ghost");
    assert_eq!(last.author.author_type, "agent");
}

// ---------------------------------------------------------------------------
// TASKFLOW-1: auto-convert @@@task blocks on every note content-write path
// (reference parity with `notes.service.ts` update path L633-647). Each write
// method that mutates a note's content invokes `convert_task_blocks` when the
// resulting content contains a `@@@task` fence. `note.add` / `note.edit` /
// `note.editLines` / `note.setContent` surface the conversion counts +
// fence-free content in their result payloads; `note.create` / `note.update`
// return the refetched `Note` (fence-free content, fresh rev/updated_at)
// without count fields.
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_note_with_task_block_auto_converts() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");
    let svc = Services::new(store);

    let created = svc
        .create_note(
            ws.clone(),
            NoteCreate {
                title: "Parent".into(),
                content: Some("intro\n@@@task\n# Child One\nbody\n@@@\ntail".into()),
                tags: None,
                parent_id: None,
            },
            None,
            None,
        )
        .await
        .expect("create");
    assert!(!created.content.contains("@@@task"));
    assert!(created
        .content
        .contains("- [ ] [Child One](intent://local/task/"));
    let persisted = svc.get_note(ws, created.id.clone()).await.expect("get");
    assert!(!persisted.content.contains("@@@task"));
    // The conversion performs a second store write; the response must carry
    // the refetched note, so rev/updated_at match the stored note.
    assert_eq!(created.rev, persisted.rev);
    assert_eq!(created.updated_at, persisted.updated_at);
    assert_eq!(created.content, persisted.content);
}

#[tokio::test]
async fn update_note_full_content_with_task_block_auto_converts() {
    let (_tmp, svc, ws, id) = setup("original").await;
    let updated = svc
        .update_note(
            ws.clone(),
            id.clone(),
            NoteUpdateInput {
                content: Some("intro\n@@@task\n# From Update\nbody\n@@@".into()),
                title: None,
                tags: None,
                expected_version: None,
            },
        )
        .await
        .expect("update");
    assert!(!updated.content.contains("@@@task"));
    assert!(updated
        .content
        .contains("- [ ] [From Update](intent://local/task/"));
    // The conversion performs a second store write; the response must carry
    // the refetched note, so rev/updated_at match the stored note.
    let persisted = svc.get_note(ws, id).await.expect("get");
    assert_eq!(updated.rev, persisted.rev);
    assert_eq!(updated.updated_at, persisted.updated_at);
    assert_eq!(updated.content, persisted.content);
}

#[tokio::test]
async fn add_to_note_task_block_auto_converts() {
    let (_tmp, svc, ws, id) = setup("# Head\nintro").await;
    let r = svc
        .add_to_note(
            ws.clone(),
            id.clone(),
            NoteAddInput {
                content: "@@@task\n# From Add\nbody\n@@@".into(),
                heading: None,
                position: Some("end".into()),
            },
            None,
        )
        .await
        .expect("add");
    assert_eq!(r.converted_count, 1);
    assert_eq!(r.created_task_note_ids.len(), 1);
    assert!(!r.new_content.contains("@@@task"));
    assert!(r
        .new_content
        .contains("- [ ] [From Add](intent://local/task/"));
    let persisted = svc.get_note(ws, id).await.expect("get");
    assert_eq!(persisted.content, r.new_content);
}

#[tokio::test]
async fn edit_note_replacing_text_with_task_block_auto_converts() {
    let (_tmp, svc, ws, id) = setup("intro PLACEHOLDER tail").await;
    let r = svc
        .edit_note(
            ws.clone(),
            id.clone(),
            NoteEditInput {
                old: "PLACEHOLDER".into(),
                new: "\n@@@task\n# From Edit\nbody\n@@@\n".into(),
            },
            None,
        )
        .await
        .expect("edit");
    assert_eq!(r.converted_count, 1);
    assert_eq!(r.created_task_note_ids.len(), 1);
    assert!(!r.new_content.contains("@@@task"));
    assert!(r
        .new_content
        .contains("- [ ] [From Edit](intent://local/task/"));
    let persisted = svc.get_note(ws, id).await.expect("get");
    assert_eq!(persisted.content, r.new_content);
}

#[tokio::test]
async fn edit_note_lines_inserting_task_block_auto_converts() {
    let (_tmp, svc, ws, id) = setup("line1\nOLD\nline3").await;
    let r = svc
        .edit_note_lines(
            ws.clone(),
            id.clone(),
            NoteEditLinesInput {
                start: 2,
                end: 2,
                content: "@@@task\n# From EditLines\nbody\n@@@".into(),
            },
            None,
        )
        .await
        .expect("editLines");
    assert_eq!(r.converted_count, 1);
    assert_eq!(r.created_task_note_ids.len(), 1);
    assert!(!r.new_content.contains("@@@task"));
    assert!(r
        .new_content
        .contains("- [ ] [From EditLines](intent://local/task/"));
    // total_lines_after reflects the post-conversion content, not the raw insert.
    let expected_lines = r.new_content.split('\n').count();
    assert_eq!(r.total_lines_after, expected_lines);
    let persisted = svc.get_note(ws, id).await.expect("get");
    assert_eq!(persisted.content, r.new_content);
}

#[tokio::test]
async fn set_note_content_with_task_block_auto_converts() {
    let (_tmp, svc, ws, id) = setup("original body content that is long enough").await;
    let r = svc
        .set_note_content(
            ws.clone(),
            id.clone(),
            "intro\n@@@task\n# From SetContent\nbody\n@@@\ntail".into(),
            true,
            None,
            None,
        )
        .await
        .expect("setContent");
    assert_eq!(r.converted_count, 1);
    assert_eq!(r.created_task_note_ids.len(), 1);
    assert!(!r.new_content.contains("@@@task"));
    assert!(r
        .new_content
        .contains("- [ ] [From SetContent](intent://local/task/"));
    let persisted = svc.get_note(ws, id).await.expect("get");
    assert_eq!(persisted.content, r.new_content);
    // The conversion performs a second store write; the response must carry
    // the refetched note's timestamp, not the pre-conversion write time.
    assert_eq!(r.updated_at, persisted.updated_at);
}

#[tokio::test]
async fn write_without_task_block_reports_zero_conversions() {
    let (_tmp, svc, ws, id) = setup("body").await;
    let r = svc
        .add_to_note(
            ws,
            id,
            NoteAddInput {
                content: "no fences here".into(),
                heading: None,
                position: Some("end".into()),
            },
            None,
        )
        .await
        .expect("add");
    assert_eq!(r.converted_count, 0);
    assert!(r.created_task_note_ids.is_empty());
}

// ---------------------------------------------------------------------------
// comment.* tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn comment_add_unique_match_anchors_and_persists() {
    let (_tmp, svc, ws, id) = setup("Hello world, this is a test sentence.").await;
    let r = svc
        .comment_add(
            ws.clone(),
            id.clone(),
            "this is a test sentence".into(),
            "test".into(),
            "nice".into(),
            None,
            None,
            None,
        )
        .await
        .expect("comment.add");
    assert!(r.anchored);
    assert_eq!(r.location.anchored_text, "test");
    let note = svc.get_note(ws.clone(), id.clone()).await.unwrap();
    assert!(note.content.contains(&format!(
        "<!--anchor:{}:start-->test<!--anchor:{}:end-->",
        r.comment_id, r.comment_id
    )));

    // The thread is listed with the comment included.
    let list = svc
        .comment_list(ws, id, None, None, None, true)
        .await
        .expect("list");
    assert_eq!(list.total_threads, 1);
    assert_eq!(list.threads[0].targeted_text.as_deref(), Some("test"));
    assert_eq!(list.threads[0].comments.as_ref().unwrap().len(), 1);
}

#[tokio::test]
async fn comment_add_persists_anchor_context() {
    // M1: `comment.add` must persist `anchor_before` / `anchor_after` (the
    // ~50 chars of surrounding text) so a later note edit can relocate a
    // partial anchor without needing version history.
    let (_tmp, svc, ws, id) = setup("Hello world, this is a test sentence.").await;
    let r = svc
        .comment_add(
            ws.clone(),
            id.clone(),
            "this is a test sentence".into(),
            "test".into(),
            "nice".into(),
            None,
            None,
            None,
        )
        .await
        .expect("comment.add");
    let list = svc
        .comment_list(ws, id, None, None, None, true)
        .await
        .expect("list");
    let comment = list.threads[0].comments.as_ref().unwrap()[0].clone();
    assert_eq!(comment.id, r.comment_id);
    let ctx = comment.anchor_context.expect("anchor_context persisted");
    assert!(
        ctx.before.ends_with("this is a "),
        "unexpected before: {:?}",
        ctx.before
    );
    assert!(
        ctx.after.starts_with(" sentence"),
        "unexpected after: {:?}",
        ctx.after
    );
}

/// Fetch a single comment for a note by id from the service layer, matching
/// how the wire client sees it (including `is_orphaned`).
async fn fetch_comment_by_id(
    svc: &Services,
    ws: &WorkspaceId,
    note_id: &NoteId,
    comment_id: &str,
) -> intent_core::CommentWire {
    let list = svc
        .comment_list(ws.clone(), note_id.clone(), None, None, None, true)
        .await
        .expect("list");
    list.threads
        .into_iter()
        .flat_map(|t| t.comments.unwrap_or_default())
        .find(|c| c.id == comment_id)
        .expect("comment present")
}

#[tokio::test]
async fn edit_above_anchor_preserves_healthy_state() {
    // H1: an edit that touches text BEFORE the anchored range must leave both
    // markers intact and must not orphan the comment.
    let (_tmp, svc, ws, id) = setup("prefix line\ntarget word here\nsuffix line").await;
    let added = svc
        .comment_add(
            ws.clone(),
            id.clone(),
            "target word here".into(),
            "target word".into(),
            "c".into(),
            None,
            None,
            None,
        )
        .await
        .expect("add");
    // Rewrite the line above the anchored text.
    svc.edit_note(
        ws.clone(),
        id.clone(),
        NoteEditInput {
            old: "prefix line".into(),
            new: "PREFIX LINE".into(),
        },
        None,
    )
    .await
    .expect("edit");
    let note = svc.get_note(ws.clone(), id.clone()).await.unwrap();
    let cid = &added.comment_id;
    assert!(note.content.contains(&format!(
        "<!--anchor:{cid}:start-->target word<!--anchor:{cid}:end-->"
    )));
    let comment = fetch_comment_by_id(&svc, &ws, &id, cid).await;
    assert_ne!(comment.is_orphaned, Some(true));
}

#[tokio::test]
async fn edit_that_destroys_start_marker_recovers_via_context() {
    // H1: an edit that clobbers just the start marker (but leaves the
    // anchored text + neighbor intact) must be re-anchored using the
    // `anchor_before` context stored at add time.
    let (_tmp, svc, ws, id) = setup("prefix target here suffix").await;
    let added = svc
        .comment_add(
            ws.clone(),
            id.clone(),
            "prefix target here suffix".into(),
            "target".into(),
            "c".into(),
            None,
            None,
            None,
        )
        .await
        .expect("add");
    let cid = &added.comment_id;
    // Nuke the start marker via edit_note. The anchored text ("target") and
    // its `contextBefore` neighbor ("prefix") remain.
    let before = svc.get_note(ws.clone(), id.clone()).await.unwrap();
    let start_pat = format!("<!--anchor:{cid}:start-->");
    assert!(before.content.contains(&start_pat));
    svc.edit_note(
        ws.clone(),
        id.clone(),
        NoteEditInput {
            old: start_pat.clone(),
            new: String::new(),
        },
        None,
    )
    .await
    .expect("edit");
    let note = svc.get_note(ws.clone(), id.clone()).await.unwrap();
    assert!(
        note.content.contains(&format!("<!--anchor:{cid}:start-->")),
        "start marker not restored: {}",
        note.content
    );
    assert!(note.content.contains(&format!("<!--anchor:{cid}:end-->")));
    let comment = fetch_comment_by_id(&svc, &ws, &id, cid).await;
    assert_ne!(comment.is_orphaned, Some(true));
}

#[tokio::test]
async fn edit_that_destroys_both_markers_marks_orphaned() {
    // H1: an edit that wipes both anchor markers cannot be recovered — the
    // comment must be flipped to `is_orphaned = true` per reference
    // `updateNote` (failed recoveries).
    let (_tmp, svc, ws, id) = setup("prefix target here suffix").await;
    let added = svc
        .comment_add(
            ws.clone(),
            id.clone(),
            "prefix target here suffix".into(),
            "target".into(),
            "c".into(),
            None,
            None,
            None,
        )
        .await
        .expect("add");
    let cid = added.comment_id.clone();
    // Full-content replace that omits both anchor markers.
    svc.set_note_content(
        ws.clone(),
        id.clone(),
        "completely different content without markers".into(),
        true,
        None,
        None,
    )
    .await
    .expect("set_note_content");
    let note = svc.get_note(ws.clone(), id.clone()).await.unwrap();
    assert!(!note.content.contains(&format!("<!--anchor:{cid}:")));
    let comment = fetch_comment_by_id(&svc, &ws, &id, &cid).await;
    assert_eq!(comment.is_orphaned, Some(true));
}

#[tokio::test]
async fn comment_add_ambiguous_context_errors() {
    let (_tmp, svc, ws, id) = setup("repeat repeat").await;
    let err = svc
        .comment_add(
            ws,
            id,
            "repeat".into(),
            "repeat".into(),
            "c".into(),
            None,
            None,
            None,
        )
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::Internal(ref m) if m.contains("appears multiple times in the document"))
    );
}

#[tokio::test]
async fn comment_respond_suggestion_nests_diff_and_threads() {
    let (_tmp, svc, ws, id) = setup("alpha unique-target omega").await;
    let added = svc
        .comment_add(
            ws.clone(),
            id.clone(),
            "alpha unique-target omega".into(),
            "unique-target".into(),
            "root".into(),
            None,
            None,
            None,
        )
        .await
        .expect("add");

    // Suggestion requires both original + proposed.
    assert!(svc
        .comment_respond(
            ws.clone(),
            id.clone(),
            None,
            Some(added.comment_id.clone()),
            "please change".into(),
            Some("suggestion".into()),
            None,
            Some("only-original".into()),
            None,
        )
        .await
        .is_err());

    let resp = svc
        .comment_respond(
            ws.clone(),
            id.clone(),
            None,
            Some(added.comment_id.clone()),
            "please change".into(),
            Some("suggestion".into()),
            Some("Bob".into()),
            Some("old text".into()),
            Some("new text".into()),
        )
        .await
        .expect("respond");
    // The wire DTO nests suggestionDiff.
    let diff = resp.comment.suggestion_diff.expect("diff");
    assert_eq!(diff.original, "old text");
    assert_eq!(diff.proposed, "new text");
    assert_eq!(resp.thread.total_comments, 2);

    // getThread returns the root + one reply.
    let thread = svc
        .comment_get_thread(ws.clone(), id.clone(), Some(added.comment_id.clone()), None)
        .await
        .expect("getThread");
    assert_eq!(thread.total_comments, 2);
    assert_eq!(thread.replies.len(), 1);
    assert_eq!(thread.root_comment.id, added.comment_id);

    // delete removes the root comment.
    let del = svc
        .comment_delete(ws, id, added.comment_id)
        .await
        .expect("delete");
    assert!(del.success);
}

#[tokio::test]
async fn comment_resolve_thread_marks_and_reopens() {
    let (_tmp, svc, ws, id) = setup("alpha resolve-target omega").await;
    let added = svc
        .comment_add(
            ws.clone(),
            id.clone(),
            "alpha resolve-target omega".into(),
            "resolve-target".into(),
            "root".into(),
            None,
            None,
            None,
        )
        .await
        .expect("add");
    // A reply ensures the whole thread (not just the root) flips status.
    svc.comment_respond(
        ws.clone(),
        id.clone(),
        Some(added.comment_id.clone()),
        None,
        "a reply".into(),
        None,
        None,
        None,
        None,
    )
    .await
    .expect("respond");

    // Resolve (default-style explicit true) marks the thread resolved.
    let res = svc
        .comment_resolve_thread(
            ws.clone(),
            id.clone(),
            Some(added.comment_id.clone()),
            None,
            true,
        )
        .await
        .expect("resolve");
    assert!(res.success);
    assert!(res.resolved);
    assert_eq!(res.status, "resolved");
    assert_eq!(res.comment_count, 2);

    // getThread + list reflect the resolved state.
    let thread = svc
        .comment_get_thread(ws.clone(), id.clone(), Some(added.comment_id.clone()), None)
        .await
        .expect("getThread");
    assert_eq!(thread.status, "resolved");
    let list = svc
        .comment_list(ws.clone(), id.clone(), None, None, None, false)
        .await
        .expect("list");
    assert_eq!(list.threads[0].status, "resolved");

    // Unresolve via commentId reopens the thread.
    let res = svc
        .comment_resolve_thread(
            ws.clone(),
            id.clone(),
            None,
            Some(added.comment_id.clone()),
            false,
        )
        .await
        .expect("unresolve");
    assert!(!res.resolved);
    assert_eq!(res.status, "open");
    let thread = svc
        .comment_get_thread(ws.clone(), id.clone(), Some(added.comment_id.clone()), None)
        .await
        .expect("getThread2");
    assert_eq!(thread.status, "open");
    let list = svc
        .comment_list(ws, id, None, None, None, false)
        .await
        .expect("list2");
    assert_eq!(list.threads[0].status, "open");
}

#[tokio::test]
async fn comment_resolve_thread_requires_thread_or_comment_id() {
    let (_tmp, svc, ws, id) = setup("nothing here").await;
    let err = svc
        .comment_resolve_thread(ws, id, None, None, true)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Internal(ref m) if m.contains("Either threadId or commentId")));
}

/// Cross-workspace bare-id probes must not delete a comment that lives in a
/// different workspace: `comment_delete` scopes its DELETE by `workspace_id`
/// so a caller declaring workspace B cannot remove a row owned by workspace A
/// (the store's `set_thread_status` UPDATE is scoped the same way, so
/// `comment_resolve_thread` becomes a no-op across workspaces).
#[tokio::test]
async fn comment_ops_reject_cross_workspace_bare_id_writes() {
    let (_tmp, svc, ws_a, id_a) = setup("Hello world, this is a test sentence.").await;
    // Provision a second workspace on the same store/services handle.
    let ws_b = WorkspaceId::new();
    svc.store()
        .insert_workspace(&workspace(&ws_b))
        .await
        .expect("second workspace");

    let added = svc
        .comment_add(
            ws_a.clone(),
            id_a.clone(),
            "this is a test sentence".into(),
            "test".into(),
            "nice".into(),
            None,
            None,
            None,
        )
        .await
        .expect("comment.add");

    // Cross-workspace delete with the wrong workspaceId is rejected and the
    // row survives.
    let err = svc
        .comment_delete(ws_b.clone(), id_a.clone(), added.comment_id.clone())
        .await
        .expect_err("cross-ws delete must not remove");
    assert!(matches!(err, Error::Internal(_)), "delete: {err:?}");
    let thread = svc
        .comment_get_thread(
            ws_a.clone(),
            id_a.clone(),
            Some(added.comment_id.clone()),
            None,
        )
        .await
        .expect("thread still readable");
    assert_eq!(thread.total_comments, 1);

    // Cross-workspace resolve is rejected at the note-scope guard (the note
    // is not visible to ws_b) so the thread stays open when the owning
    // workspace re-reads it. Even if a caller bypassed the note guard, the
    // store's UPDATE is now scoped by workspace_id and would affect zero rows
    // (covered by the store-level regression tests in `agent_ops::tests`).
    let err = svc
        .comment_resolve_thread(
            ws_b.clone(),
            id_a.clone(),
            Some(added.comment_id.clone()),
            None,
            true,
        )
        .await
        .expect_err("cross-ws resolve must not observe");
    assert!(matches!(err, Error::Internal(_)), "resolve: {err:?}");
    let thread = svc
        .comment_get_thread(
            ws_a.clone(),
            id_a.clone(),
            Some(added.comment_id.clone()),
            None,
        )
        .await
        .expect("thread readable");
    assert_eq!(thread.status, "open");

    // The owner can still delete their own row.
    svc.comment_delete(ws_a, id_a, added.comment_id)
        .await
        .expect("owner delete succeeds");
}

/// A cross-workspace `commentId` probe on `comment.respond` must be rejected
/// with no side effects, even when both workspaces have a note that shares the
/// same `note_id` (a real scenario for well-known ids like `spec`). Without the
/// workspace-scoped `list_comments_in_workspace` lookup, the parent lookup
/// would leak a match from the other workspace and attach a reply whose
/// `parent_id` chains cross-workspace.
#[tokio::test]
async fn comment_respond_rejects_cross_workspace_comment_id_probe() {
    use intent_core::{
        AuthorType, Comment, CommentAnchor, CommentAnchorType, CommentStatus, CommentType,
    };

    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    // Two workspaces, each with a note carrying the same `note_id` string —
    // the same-`note_id`-across-workspaces case (e.g. the well-known `spec`
    // id) that made the un-scoped `list_comments(&note_id)` lookup leak a
    // cross-workspace parent match into `comment_respond`.
    let ws_a = WorkspaceId::new();
    let ws_b = WorkspaceId::new();
    store
        .insert_workspace(&workspace(&ws_a))
        .await
        .expect("ws_a");
    store
        .insert_workspace(&workspace(&ws_b))
        .await
        .expect("ws_b");
    let shared = NoteId::from("spec");
    store
        .insert_note(&note(&ws_a, "spec", "ws_a body"))
        .await
        .expect("note_a");
    store
        .insert_note(&note(&ws_b, "spec", "ws_b body"))
        .await
        .expect("note_b");

    // Seed a comment in ws_a directly via the store so we bypass any
    // note-mutating side effects of `comment_add` and keep the fixture focused
    // on the residual parent-lookup guard.
    let now = now_iso();
    let seeded_a = Comment {
        id: uuid::Uuid::new_v4().to_string(),
        thread_id: uuid::Uuid::new_v4().to_string(),
        note_id: Some(shared.clone()),
        kind: CommentType::Comment,
        content: "ws_a original".to_string(),
        author: "A".to_string(),
        author_type: AuthorType::User,
        status: CommentStatus::Open,
        parent_id: None,
        anchor: CommentAnchor {
            kind: CommentAnchorType::Range,
            start_id: None,
            end_id: None,
            point_id: None,
        },
        anchor_text: None,
        anchor_before: None,
        anchor_after: None,
        suggestion_original: None,
        suggestion_proposed: None,
        agent_id: None,
        is_orphaned: None,
        created_at: now.clone(),
        updated_at: now,
    };
    store
        .insert_comment(&ws_a, &seeded_a)
        .await
        .expect("seed ws_a comment");
    let svc = Services::new(store);

    // ws_b, using its own same-`note_id` note, probes with ws_a's commentId.
    // The reply must not be created and no cross-workspace parent must leak.
    let err = svc
        .comment_respond(
            ws_b.clone(),
            shared.clone(),
            None,
            Some(seeded_a.id.clone()),
            "leaked reply".into(),
            None,
            None,
            None,
            None,
        )
        .await
        .expect_err("cross-ws commentId probe must be rejected");
    assert!(matches!(err, Error::Internal(_)), "respond: {err:?}");

    // No side effects: neither workspace's comment set changed.
    let a_after = svc
        .store()
        .list_comments_in_workspace(&ws_a, &shared)
        .await
        .expect("list ws_a");
    assert_eq!(a_after.len(), 1, "ws_a still has just its seeded comment");
    assert_eq!(a_after[0].id, seeded_a.id);
    let b_after = svc
        .store()
        .list_comments_in_workspace(&ws_b, &shared)
        .await
        .expect("list ws_b");
    assert!(b_after.is_empty(), "ws_b has no comments after the probe");
}

// ---- event.* query/aggregation methods (M2.4) ----

use intent_core::{ActorType, EventActor};
use intent_store::NewEvent;

/// Insert a `file:changed` event for `agent` at `ts` (newest inserted last).
async fn insert_file_event(
    svc: &Services,
    ws: &WorkspaceId,
    actor_type: ActorType,
    actor_id: Option<&str>,
    ts: &str,
    path: &str,
) {
    svc.store()
        .insert_event(&NewEvent {
            workspace_id: ws.clone(),
            timestamp: ts.to_string(),
            event_type: "file:changed".to_string(),
            actor: EventActor {
                actor_type,
                id: actor_id.map(|s| s.to_string()),
                name: actor_id.map(|s| format!("name-{s}")),
                ..Default::default()
            },
            session_id: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data: serde_json::json!({ "path": path, "relativePath": path, "action": "modify" }),
        })
        .await
        .expect("insert event");
}

async fn event_setup() -> (TempDb, Services, WorkspaceId) {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");
    (tmp, Services::new(store), ws)
}

#[tokio::test]
async fn recent_files_limits_and_orders_newest_first() {
    let (_tmp, svc, ws) = event_setup().await;
    insert_file_event(
        &svc,
        &ws,
        ActorType::Agent,
        Some("a"),
        "2026-01-01T00:00:01Z",
        "a.rs",
    )
    .await;
    insert_file_event(
        &svc,
        &ws,
        ActorType::Tool,
        None,
        "2026-01-01T00:00:02Z",
        "b.rs",
    )
    .await;
    insert_file_event(
        &svc,
        &ws,
        ActorType::User,
        Some("u"),
        "2026-01-01T00:00:03Z",
        "c.rs",
    )
    .await;

    let files = svc
        .event_recent_files(ws.clone(), Some(2))
        .await
        .expect("recent");
    assert_eq!(files.len(), 2);
    // Newest first; combined "type:name" actor (absent name → undefined).
    assert_eq!(files[0].path, "c.rs");
    assert_eq!(files[0].actor.as_deref(), Some("user:name-u"));
    assert_eq!(files[1].path, "b.rs");
    assert_eq!(files[1].actor.as_deref(), Some("tool:undefined"));
}

#[tokio::test]
async fn agent_activity_files_branch_vs_aggregate_branch() {
    let (_tmp, svc, ws) = event_setup().await;
    let ts = intent_core::now_iso();
    insert_file_event(&svc, &ws, ActorType::Agent, Some("a1"), &ts, "x.rs").await;
    insert_file_event(&svc, &ws, ActorType::Agent, Some("a2"), &ts, "y.rs").await;

    // With agentId → FileActivity[] for that agent, bare actor name.
    let by_agent = svc
        .event_agent_activity(ws.clone(), Some("a1".to_string()), None)
        .await
        .expect("agent files");
    let arr = by_agent.as_array().expect("array");
    assert_eq!(arr.len(), 1);
    assert_eq!(arr[0]["path"], "x.rs");
    assert_eq!(arr[0]["actor"], "name-a1");

    // Without agentId → aggregated AgentActivity[] over the recent window.
    let agg = svc
        .event_agent_activity(ws.clone(), None, Some(60))
        .await
        .expect("agent activity");
    let agg_arr = agg.as_array().expect("array");
    assert_eq!(agg_arr.len(), 2);
    let ids: Vec<&str> = agg_arr
        .iter()
        .map(|a| a["agentId"].as_str().unwrap())
        .collect();
    assert!(ids.contains(&"a1") && ids.contains(&"a2"));
}

#[tokio::test]
async fn workspace_summary_aggregates_recent_window() {
    let (_tmp, svc, ws) = event_setup().await;
    let ts = intent_core::now_iso();
    insert_file_event(&svc, &ws, ActorType::Agent, Some("a1"), &ts, "a.rs").await;
    insert_file_event(&svc, &ws, ActorType::Agent, Some("a1"), &ts, "a.rs").await;
    insert_file_event(&svc, &ws, ActorType::User, Some("u"), &ts, "b.rs").await;

    let summary = svc
        .event_workspace_summary(ws.clone(), Some(60))
        .await
        .expect("summary");
    assert_eq!(summary.recent_files.len(), 3);
    // Only agent-typed events feed activeAgents.
    assert_eq!(summary.active_agents.len(), 1);
    assert_eq!(summary.active_agents[0].agent_id, "a1");
    assert_eq!(summary.top_changed_files[0].path, "a.rs");
    assert_eq!(summary.top_changed_files[0].change_count, 2);
}

#[tokio::test]
async fn directory_changes_filters_by_prefix_and_requires_dir() {
    let (_tmp, svc, ws) = event_setup().await;
    insert_file_event(
        &svc,
        &ws,
        ActorType::Agent,
        Some("a"),
        "2026-01-01T00:00:01Z",
        "src/a.rs",
    )
    .await;
    insert_file_event(
        &svc,
        &ws,
        ActorType::Agent,
        Some("a"),
        "2026-01-01T00:00:02Z",
        "docs/b.md",
    )
    .await;

    let changes = svc
        .event_directory_changes(ws.clone(), "src/".to_string(), None)
        .await
        .expect("dir");
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].path, "src/a.rs");

    let err = svc
        .event_directory_changes(ws.clone(), String::new(), None)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Internal(m) if m == "Directory path is required"));
}

#[tokio::test]
async fn query_filters_and_defaults_limit_50() {
    let (_tmp, svc, ws) = event_setup().await;
    insert_file_event(
        &svc,
        &ws,
        ActorType::Agent,
        Some("a"),
        "2026-01-01T00:00:01Z",
        "a.rs",
    )
    .await;
    insert_file_event(
        &svc,
        &ws,
        ActorType::User,
        Some("u"),
        "2026-01-01T00:00:02Z",
        "b.rs",
    )
    .await;

    // Filter by actorType=user → only the user event.
    let only_user = svc
        .event_query(
            ws.clone(),
            intent_core::EventQueryParams {
                actor_type: Some("user".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect("query");
    let only_user = only_user.as_array().expect("bare array (non-paginated)");
    assert_eq!(only_user.len(), 1);
    let ev: intent_core::Event = serde_json::from_value(only_user[0].clone()).expect("event");
    assert_eq!(ev.actor.actor_type, ActorType::User);

    // An unknown actorType matches nothing.
    let none = svc
        .event_query(
            ws.clone(),
            intent_core::EventQueryParams {
                actor_type: Some("nope".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect("query");
    assert!(none.as_array().expect("bare array").is_empty());
}

/// TA-2 / §5.5: `event.query` is opt-in paginated. Without `paginate`/`page_token`
/// it returns the legacy bare array; with it, it returns the `{ items, nextToken }`
/// envelope (newest→oldest, clamped limit) and an opaque token that walks
/// backward to exhaustion.
#[tokio::test]
async fn query_opt_in_pagination_envelope_and_token() {
    let (_tmp, svc, ws) = event_setup().await;
    for i in 0..3 {
        insert_file_event(
            &svc,
            &ws,
            ActorType::Agent,
            Some("a"),
            &format!("2026-01-01T00:00:0{i}Z"),
            &format!("f{i}.rs"),
        )
        .await;
    }

    // Page 1 (opt in via `paginate`): newest two, envelope shape, token present.
    let p1 = svc
        .event_query(
            ws.clone(),
            intent_core::EventQueryParams {
                limit: Some(2),
                paginate: Some(true),
                ..Default::default()
            },
        )
        .await
        .expect("p1");
    let items1 = p1["items"].as_array().expect("envelope items");
    assert_eq!(items1.len(), 2);
    // Newest→oldest: f2 then f1.
    assert_eq!(items1[0]["data"]["path"], "f2.rs");
    assert_eq!(items1[1]["data"]["path"], "f1.rs");
    let token = p1["nextToken"].as_str().expect("nextToken").to_string();
    assert!(token.parse::<u64>().is_err(), "token is opaque");

    // Page 2 follows the token: the oldest event, then exhaustion.
    let p2 = svc
        .event_query(
            ws.clone(),
            intent_core::EventQueryParams {
                limit: Some(2),
                page_token: Some(token),
                ..Default::default()
            },
        )
        .await
        .expect("p2");
    let items2 = p2["items"].as_array().unwrap();
    assert_eq!(items2.len(), 1);
    assert_eq!(items2[0]["data"]["path"], "f0.rs");
    assert!(p2["nextToken"].is_null());

    // An over-max limit clamps to 200; all three fit in one page, no token.
    let clamped = svc
        .event_query(
            ws.clone(),
            intent_core::EventQueryParams {
                limit: Some(10_000),
                paginate: Some(true),
                ..Default::default()
            },
        )
        .await
        .expect("clamped");
    assert_eq!(clamped["items"].as_array().unwrap().len(), 3);
    assert!(clamped["nextToken"].is_null());
}

#[tokio::test]
async fn subscribe_resolves_star_and_unsubscribe_roundtrips() {
    let (_tmp, svc, ws) = event_setup().await;
    // Empty eventTypes → error (TS resolveSubscriptionEventTypes guard).
    let err = svc
        .event_subscribe(ws.clone(), vec![], None, None)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Internal(m) if m.contains("eventTypes is required")));

    // Bare `*` expands to the category wildcards; `excludeSelf`/`batchWindow`
    // are accepted (TS shim forwards them) without changing the result shape.
    let sub = svc
        .event_subscribe(ws.clone(), vec!["*".to_string()], Some(true), Some(250))
        .await
        .expect("subscribe");
    assert!(sub.event_types.contains(&"agent:*".to_string()));
    assert!(sub.event_types.contains(&"file:*".to_string()));

    // Unsubscribe removes it; a second call reports not-found.
    let un = svc
        .event_unsubscribe(ws.clone(), sub.subscription_id.clone())
        .await
        .expect("unsub");
    assert!(un.ok);
    let again = svc
        .event_unsubscribe(ws.clone(), sub.subscription_id.clone())
        .await
        .unwrap_err();
    assert!(matches!(again, Error::Internal(m) if m == "Subscription not found"));

    // Empty subscriptionId → error.
    let empty = svc.event_unsubscribe(ws, String::new()).await.unwrap_err();
    assert!(matches!(empty, Error::Internal(m) if m == "subscriptionId is required"));
}

/// camelCase parity fixtures for the change-event envelopes published by CRUD
/// mutations (M2.6): the wire-serialized [`intent_core::Event`] must carry the
/// exact field names + payload shapes the iOS client expects (PROTOCOL §6.5).
mod change_event_parity {
    use std::time::Duration;

    use intent_core::{
        now_iso, AgentId, AgentSession, AgentStatus, NoteCreate, TaskMetadata, TaskStatus,
        WorkspaceApi, WorkspaceId,
    };
    use intent_store::Store;
    use serde_json::{json, Value};

    use super::{note, workspace, DebounceEnvGuard, TempDb, WorkspacesRoot};
    use crate::{EventBus, Services, Subscription, SubscriptionFilter};

    struct Harness {
        _tmp: TempDb,
        _ws_root: WorkspacesRoot,
        store: Store,
        services: Services,
        bus: EventBus,
        ws: WorkspaceId,
    }

    async fn harness() -> Harness {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws = WorkspaceId::new();
        store.insert_workspace(&workspace(&ws)).await.expect("ws");
        let bus = EventBus::new(store.clone());
        let ws_root = WorkspacesRoot::new();
        let services = Services::new(store.clone())
            .with_workspaces_root(ws_root.path().to_path_buf())
            .with_event_bus(bus.clone());
        Harness {
            _tmp: tmp,
            _ws_root: ws_root,
            store,
            services,
            bus,
            ws,
        }
    }

    /// Subscribe to this workspace with immediate (un-batched) delivery.
    fn subscribe(h: &Harness) -> Subscription {
        h.bus.subscribe(SubscriptionFilter {
            workspace_id: Some(h.ws.0.clone()),
            ..Default::default()
        })
    }

    /// Receive exactly one published event, serialized to its wire JSON.
    async fn recv_one(sub: &mut Subscription) -> Value {
        let batch = tokio::time::timeout(Duration::from_secs(2), sub.recv())
            .await
            .expect("event delivered")
            .expect("subscription open");
        assert_eq!(batch.len(), 1, "expected exactly one event");
        serde_json::to_value(&batch[0]).expect("serialize event")
    }

    fn assert_envelope(ev: &Value, ws: &str, event_type: &str) {
        assert_eq!(ev["type"], event_type);
        assert_eq!(ev["workspaceId"], ws);
        assert!(ev["id"].is_string());
        assert!(ev["timestamp"].is_string());
        assert_eq!(
            ev["actor"],
            json!({ "type": "system", "id": "system", "name": "System" })
        );
    }

    #[tokio::test]
    async fn note_created_payload() {
        let h = harness().await;
        let mut sub = subscribe(&h);
        let created = h
            .services
            .create_note(
                h.ws.clone(),
                NoteCreate {
                    title: "Note".to_string(),
                    content: None,
                    tags: None,
                    parent_id: None,
                },
                None,
                None,
            )
            .await
            .expect("create");
        let ev = recv_one(&mut sub).await;
        assert_envelope(&ev, &h.ws.0, "note:created");
        assert_eq!(
            ev["data"],
            json!({ "noteId": created.id.0, "title": "Note", "action": "create" })
        );
    }

    /// Idempotency replay (design note TB-0 §5.3): a second `note.create` with the
    /// same `(workspaceId, idempotencyKey)` returns the ORIGINAL note without
    /// re-executing — so no second `note:created` event is published.
    #[tokio::test]
    async fn idempotent_create_note_replay_returns_original_no_reexec() {
        let h = harness().await;
        let mut sub = subscribe(&h);
        let key = Some("idem-key-1".to_string());
        let first = h
            .services
            .create_note(
                h.ws.clone(),
                NoteCreate {
                    title: "Note".to_string(),
                    content: None,
                    tags: None,
                    parent_id: None,
                },
                key.clone(),
                None,
            )
            .await
            .expect("first create");
        let ev = recv_one(&mut sub).await;
        assert_envelope(&ev, &h.ws.0, "note:created");

        // Replay with the same key but different content still returns the
        // original note id (the body is never re-executed).
        let second = h
            .services
            .create_note(
                h.ws.clone(),
                NoteCreate {
                    title: "Different".to_string(),
                    content: Some("changed".to_string()),
                    tags: None,
                    parent_id: None,
                },
                key,
                None,
            )
            .await
            .expect("replay create");
        assert_eq!(second.id.0, first.id.0, "replay returns the original note");

        // No second event is published (the replay short-circuits before mutate).
        let none = tokio::time::timeout(Duration::from_millis(300), sub.recv()).await;
        assert!(none.is_err(), "replay must not publish a second event");
    }

    /// Soft-launch (R5): a missing `idempotencyKey` is accepted (never rejected)
    /// and each call executes normally, producing distinct notes.
    #[tokio::test]
    async fn missing_idempotency_key_executes_normally() {
        let h = harness().await;
        let mk = || NoteCreate {
            title: "Note".to_string(),
            content: None,
            tags: None,
            parent_id: None,
        };
        let a = h
            .services
            .create_note(h.ws.clone(), mk(), None, None)
            .await
            .expect("first create");
        let b = h
            .services
            .create_note(h.ws.clone(), mk(), None, None)
            .await
            .expect("second create");
        assert_ne!(
            a.id.0, b.id.0,
            "missing key must not dedupe — both creates run"
        );
    }

    #[tokio::test]
    async fn task_status_changed_payload() {
        let h = harness().await;
        // Pre-insert a task note directly (no event) so the only published event
        // is the status change.
        let mut tn = note(&h.ws, "task-1", "body");
        tn.metadata.task = Some(TaskMetadata {
            status: TaskStatus::NotStarted,
            ..Default::default()
        });
        h.store.insert_note(&tn).await.expect("insert task note");
        let mut sub = subscribe(&h);
        h.services
            .task_update_note_status(
                h.ws.clone(),
                tn.id.clone(),
                "in_progress".to_string(),
                None,
                None,
            )
            .await
            .expect("status");
        let ev = recv_one(&mut sub).await;
        assert_envelope(&ev, &h.ws.0, "task:status-changed");
        assert_eq!(ev["data"]["noteId"], "task-1");
        assert_eq!(ev["data"]["noteTitle"], "Title");
        assert_eq!(ev["data"]["previousStatus"], "not_started");
        assert_eq!(ev["data"]["newStatus"], "in_progress");
        assert!(ev["data"]["changedAt"].is_string());
        // System-attributed change: no agent provenance on the payload.
        assert!(ev["data"].get("agentId").is_none());
    }

    #[tokio::test]
    async fn task_status_changed_carries_agent_id_when_agent_attributed() {
        let h = harness().await;
        let mut tn = note(&h.ws, "task-agent", "Agent Task");
        tn.metadata.task = Some(TaskMetadata {
            status: TaskStatus::NotStarted,
            ..Default::default()
        });
        h.store.insert_note(&tn).await.expect("insert task note");
        // A live session so provenance resolves the display name.
        let session = AgentSession {
            id: AgentId::from("agent-prov"),
            workspace_id: h.ws.clone(),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: "Prov".to_string(),
            name_explicitly_set: true,
            model: None,
            provider: None,
            system_prompt: None,
            specialist: None,
            status: AgentStatus::Active,
            is_active: true,
            messages: vec![],
            stats: None,
            task_note_id: None,
            skip_auto_commit: false,
            completion_report: None,
            completion_report_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            is_background: false,
            metadata: None,
            created_at: now_iso(),
            updated_at: now_iso(),
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
        };
        h.store
            .insert_agent_session(&session)
            .await
            .expect("session");

        let mut sub = subscribe(&h);
        h.services
            .task_update_note_status(
                h.ws.clone(),
                tn.id.clone(),
                "in_progress".to_string(),
                None,
                Some(AgentId::from("agent-prov")),
            )
            .await
            .expect("status");
        let ev = recv_one(&mut sub).await;
        assert_eq!(ev["type"], "task:status-changed");
        // LC-1: agent-attributed changes surface `agentId` in the payload and
        // an agent actor on the envelope (TS `notes.service.ts` parity).
        assert_eq!(ev["data"]["agentId"], "agent-prov");
        assert_eq!(
            ev["actor"],
            json!({ "type": "agent", "id": "agent-prov", "name": "Prov" })
        );
    }

    #[tokio::test]
    async fn ready_tasks_changed_payload() {
        let h = harness().await;
        // Parent task with one child; both start not_started. The parent is
        // blocked by its incomplete child, so neither is ready yet.
        let mut parent = note(&h.ws, "parent", "p");
        parent.metadata.task = Some(TaskMetadata {
            status: TaskStatus::NotStarted,
            ..Default::default()
        });
        let mut child = note(&h.ws, "child", "c");
        child.parent_id = Some(parent.id.clone());
        child.metadata.task = Some(TaskMetadata {
            status: TaskStatus::NotStarted,
            ..Default::default()
        });
        h.store.insert_note(&parent).await.expect("insert parent");
        h.store.insert_note(&child).await.expect("insert child");

        let mut sub = subscribe(&h);
        // Completing the child recomputes the ready set: the child is now
        // terminal (excluded), and the parent's only child is complete, so the
        // parent becomes the sole ready task.
        h.services
            .task_update_note_status(
                h.ws.clone(),
                child.id.clone(),
                "complete".to_string(),
                None,
                None,
            )
            .await
            .expect("status");

        // First: task:status-changed for the child.
        let ev = recv_one(&mut sub).await;
        assert_envelope(&ev, &h.ws.0, "task:status-changed");
        assert_eq!(ev["data"]["noteId"], "child");

        // Then: task:ready-tasks-changed with the recomputed set + trigger.
        let ev = recv_one(&mut sub).await;
        assert_envelope(&ev, &h.ws.0, "task:ready-tasks-changed");
        assert_eq!(ev["data"]["readyTaskIds"], json!(["parent"]));
        assert_eq!(ev["data"]["triggeredBy"]["noteId"], "child");
        assert_eq!(ev["data"]["triggeredBy"]["previousStatus"], "not_started");
        assert_eq!(ev["data"]["triggeredBy"]["newStatus"], "complete");
        assert!(ev["data"]["computedAt"].is_string());
    }

    #[tokio::test]
    async fn comment_added_payload() {
        let h = harness().await;
        let tn = note(&h.ws, "n-1", "hello world");
        h.store.insert_note(&tn).await.expect("insert note");
        let mut sub = subscribe(&h);
        let added = h
            .services
            .comment_add(
                h.ws.clone(),
                tn.id.clone(),
                "hello world".to_string(),
                "hello".to_string(),
                "nice".to_string(),
                None,
                None,
                None,
            )
            .await
            .expect("comment");
        let ev = recv_one(&mut sub).await;
        assert_envelope(&ev, &h.ws.0, "comment:added");
        assert_eq!(
            ev["data"],
            json!({ "noteId": "n-1", "commentId": added.comment_id })
        );
    }

    /// Idempotency replay (Audit A F5, design note TB-0 §5.3): a second
    /// `comment.add` with the same `(workspaceId, idempotencyKey)` returns the
    /// ORIGINAL result without re-executing — no duplicate comment row, no
    /// second anchor in the note, and no second `comment:added` event.
    #[tokio::test]
    async fn idempotent_comment_add_replay_returns_original_no_reexec() {
        let h = harness().await;
        let tn = note(&h.ws, "n-1", "hello world");
        h.store.insert_note(&tn).await.expect("insert note");
        let mut sub = subscribe(&h);
        let key = Some("comment-idem-1".to_string());
        let first = h
            .services
            .comment_add(
                h.ws.clone(),
                tn.id.clone(),
                "hello world".to_string(),
                "hello".to_string(),
                "nice".to_string(),
                None,
                None,
                key.clone(),
            )
            .await
            .expect("first add");
        let ev = recv_one(&mut sub).await;
        assert_envelope(&ev, &h.ws.0, "comment:added");

        // Replay with the same key returns the original comment id and does
        // not insert a second comment.
        let second = h
            .services
            .comment_add(
                h.ws.clone(),
                tn.id.clone(),
                "hello world".to_string(),
                "hello".to_string(),
                "nice".to_string(),
                None,
                None,
                key,
            )
            .await
            .expect("replay add");
        assert_eq!(
            second.comment_id, first.comment_id,
            "replay returns the original comment"
        );
        let comments = h.store.list_comments(&tn.id).await.expect("list comments");
        assert_eq!(comments.len(), 1, "replay must not duplicate the comment");

        // No second event is published (the replay short-circuits before mutate).
        let none = tokio::time::timeout(Duration::from_millis(300), sub.recv()).await;
        assert!(none.is_err(), "replay must not publish a second event");
    }

    /// Reference parity: `comment.respond` publishes a single `comment:added`
    /// event carrying `{ noteId, commentId }` for the reply so subscribers see
    /// the new thread comment without a re-read (Audit D C2).
    #[tokio::test]
    async fn comment_respond_emits_comment_added_once() {
        let h = harness().await;
        let tn = note(&h.ws, "n-1", "hello world");
        h.store.insert_note(&tn).await.expect("insert note");
        let added = h
            .services
            .comment_add(
                h.ws.clone(),
                tn.id.clone(),
                "hello world".to_string(),
                "hello".to_string(),
                "root".to_string(),
                None,
                None,
                None,
            )
            .await
            .expect("comment");
        // Subscribe after the add so only the respond event is observed.
        let mut sub = subscribe(&h);
        let reply = h
            .services
            .comment_respond(
                h.ws.clone(),
                tn.id.clone(),
                None,
                Some(added.comment_id.clone()),
                "reply body".to_string(),
                None,
                None,
                None,
                None,
            )
            .await
            .expect("respond");
        let ev = recv_one(&mut sub).await;
        assert_envelope(&ev, &h.ws.0, "comment:added");
        assert_eq!(
            ev["data"],
            json!({ "noteId": "n-1", "commentId": reply.comment.id })
        );
        // Cardinality exactly 1: no additional publishes fire.
        let quiet = tokio::time::timeout(Duration::from_millis(200), sub.recv()).await;
        assert!(
            quiet.is_err(),
            "comment.respond must publish exactly one event, got extra: {quiet:?}"
        );
    }

    #[tokio::test]
    async fn comment_resolved_payload() {
        let h = harness().await;
        let tn = note(&h.ws, "n-1", "hello world");
        h.store.insert_note(&tn).await.expect("insert note");
        let added = h
            .services
            .comment_add(
                h.ws.clone(),
                tn.id.clone(),
                "hello world".to_string(),
                "hello".to_string(),
                "nice".to_string(),
                None,
                None,
                None,
            )
            .await
            .expect("comment");
        // Subscribe after the add so only the resolve event is observed.
        let mut sub = subscribe(&h);
        h.services
            .comment_resolve_thread(
                h.ws.clone(),
                tn.id.clone(),
                Some(added.comment_id.clone()),
                None,
                true,
            )
            .await
            .expect("resolve");
        let ev = recv_one(&mut sub).await;
        assert_envelope(&ev, &h.ws.0, "comment:resolved");
        assert_eq!(
            ev["data"],
            json!({ "noteId": "n-1", "threadId": added.comment_id, "resolved": true })
        );
    }

    #[tokio::test]
    async fn attention_changed_payload() {
        let h = harness().await;
        // Raise attention directly (no event), then dismiss it via the service.
        let mut ws = workspace(&h.ws);
        ws.attention = intent_core::WorkspaceAttention::Unread;
        h.store.update_workspace(&ws).await.expect("set attention");
        let mut sub = subscribe(&h);
        h.services
            .dismiss_attention(h.ws.clone())
            .await
            .expect("dismiss");
        let ev = recv_one(&mut sub).await;
        assert_envelope(&ev, &h.ws.0, "workspace:attention-changed");
        assert_eq!(
            ev["data"],
            json!({ "workspaceId": h.ws.0, "attention": "none" })
        );
    }

    /// §6.5 has no `workspace:archived`; the reference emitter publishes
    /// `workspace:updated` with the applied `{ archived }` delta. Verify
    /// `archive_workspace` fires exactly one such event (Audit D C3).
    #[tokio::test]
    async fn archive_workspace_emits_workspace_updated_once() {
        use intent_core::WorkspaceApi;
        let h = harness().await;
        let mut sub = subscribe(&h);
        h.services
            .archive_workspace(h.ws.clone())
            .await
            .expect("archive");
        let ev = recv_one(&mut sub).await;
        assert_envelope(&ev, &h.ws.0, "workspace:updated");
        assert_eq!(
            ev["data"],
            json!({ "workspaceId": h.ws.0, "changes": { "archived": true } })
        );
        let quiet = tokio::time::timeout(Duration::from_millis(200), sub.recv()).await;
        assert!(
            quiet.is_err(),
            "archive_workspace must publish exactly one event, got extra: {quiet:?}"
        );
    }

    /// Symmetric to archive: `unarchive_workspace` emits one `workspace:updated`
    /// carrying `{ archived: false }` (Audit D C3).
    #[tokio::test]
    async fn unarchive_workspace_emits_workspace_updated_once() {
        use intent_core::{WorkspaceApi, WorkspaceStatus};
        let h = harness().await;
        // Seed the row as archived so unarchive has a real state to flip.
        let mut ws = workspace(&h.ws);
        ws.status = WorkspaceStatus::Archived;
        ws.archived = true;
        ws.archived_at = Some(intent_core::now_iso());
        h.store.update_workspace(&ws).await.expect("archive row");
        let mut sub = subscribe(&h);
        h.services
            .unarchive_workspace(h.ws.clone())
            .await
            .expect("unarchive");
        let ev = recv_one(&mut sub).await;
        assert_envelope(&ev, &h.ws.0, "workspace:updated");
        assert_eq!(
            ev["data"],
            json!({ "workspaceId": h.ws.0, "changes": { "archived": false } })
        );
        let quiet = tokio::time::timeout(Duration::from_millis(200), sub.recv()).await;
        assert!(
            quiet.is_err(),
            "unarchive_workspace must publish exactly one event, got extra: {quiet:?}"
        );
    }

    /// `update_workspace` normalises the delta snapshot published as
    /// `workspace:updated { changes }` (§6.5) so subscribers can mirror the
    /// applied delta without a follow-up read: a raw `baseRef: "origin/main"`
    /// collapses to canonical `"main"`, and a whitespace-only `statusMessage`
    /// folds to the empty-string clear signal (preserving the "clear" vs
    /// "no change" distinction, which a `None` snapshot would erase via
    /// `skip_serializing_if`).
    #[tokio::test]
    async fn update_workspace_event_delta_matches_persisted_normalisation() {
        use intent_core::{WorkspaceApi, WorkspaceUpdate};
        let h = harness().await;
        let mut sub = subscribe(&h);
        let ws = h
            .services
            .update_workspace(
                h.ws.clone(),
                WorkspaceUpdate {
                    base_ref: Some("origin/main".to_string()),
                    status_message: Some("   \t ".to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("update");
        // Persisted / returned workspace carries the canonical values.
        assert_eq!(ws.base_ref.as_deref(), Some("main"));
        assert!(ws.status_message.is_none());

        // The emitted `changes` mirrors those canonical values (raw
        // `origin/main` and whitespace-only `statusMessage` would surface as
        // state divergence for subscribers). `statusMessage: ""` is the
        // explicit clear wire value; omitting it would collapse to
        // "no change" via `skip_serializing_if`.
        let ev = recv_one(&mut sub).await;
        assert_envelope(&ev, &h.ws.0, "workspace:updated");
        assert_eq!(ev["data"]["changes"]["baseRef"], json!("main"));
        assert_eq!(ev["data"]["changes"]["statusMessage"], json!(""));
    }

    /// Derived `activity` flips `Idle → AgentRunning → Idle` across in-flight
    /// session begin/end, reflected by `get_workspace`, and emits
    /// `workspace:activity-changed` ONLY on the zero/non-zero edges (§9.9/§10.1).
    #[tokio::test]
    async fn activity_changed_only_on_change_and_derived() {
        use intent_core::{WorkspaceActivity, WorkspaceApi};
        let _guard = DebounceEnvGuard::new("100");
        let h = harness().await;
        let mut sub = subscribe(&h);

        // First in-flight session: Idle → AgentRunning (emits agent_running).
        h.services.agent_activity_begin(&h.ws).await;
        let ev = recv_one(&mut sub).await;
        assert_envelope(&ev, &h.ws.0, "workspace:activity-changed");
        assert_eq!(
            ev["data"],
            json!({ "workspaceId": h.ws.0, "activity": "agent_running" })
        );
        assert_eq!(
            h.services.workspace_activity(&h.ws),
            WorkspaceActivity::AgentRunning
        );
        // get_workspace derives the live value (never persisted).
        let got = h.services.get_workspace(h.ws.clone()).await.expect("get");
        assert_eq!(got.activity, WorkspaceActivity::AgentRunning);

        // A nested begin/end pair stays non-zero → NO event is emitted.
        h.services.agent_activity_begin(&h.ws).await;
        h.services.agent_activity_end(&h.ws).await;
        assert_eq!(
            h.services.workspace_activity(&h.ws),
            WorkspaceActivity::AgentRunning
        );

        // Last session leaves flight: AgentRunning → Idle (emits idle after debounce).
        // If the nested pair had emitted, this would observe agent_running instead.
        h.services.agent_activity_end(&h.ws).await;
        // Wait for debounce window to expire.
        tokio::time::sleep(Duration::from_millis(150)).await;
        let ev = recv_one(&mut sub).await;
        assert_envelope(&ev, &h.ws.0, "workspace:activity-changed");
        assert_eq!(
            ev["data"],
            json!({ "workspaceId": h.ws.0, "activity": "idle" })
        );
        assert_eq!(
            h.services.workspace_activity(&h.ws),
            WorkspaceActivity::Idle
        );
        let got = h.services.get_workspace(h.ws.clone()).await.expect("get");
        assert_eq!(got.activity, WorkspaceActivity::Idle);

        // Decrementing past zero is a saturating no-op (no panic, stays Idle).
        h.services.agent_activity_end(&h.ws).await;
        assert_eq!(
            h.services.workspace_activity(&h.ws),
            WorkspaceActivity::Idle
        );
    }

    /// Regression: idle debounce must guard emission against races where
    /// `agent_activity_begin` happens after the sleep but before emission.
    /// The generation counter + count check at fire time prevents spurious idle
    /// events when agents come back in-flight during the grace window.
    #[tokio::test]
    async fn idle_debounce_guards_emission_against_race() {
        use intent_core::WorkspaceActivity;
        let h = harness().await;
        let _guard = DebounceEnvGuard::new("50");
        let mut sub = subscribe(&h);

        // Begin activity, then end it to schedule a debounce.
        h.services.agent_activity_begin(&h.ws).await;
        let ev = recv_one(&mut sub).await;
        assert_eq!(ev["data"]["activity"], "agent_running");

        h.services.agent_activity_end(&h.ws).await;

        // Quickly re-begin activity within the debounce window (race scenario).
        tokio::time::sleep(Duration::from_millis(10)).await;
        h.services.agent_activity_begin(&h.ws).await;

        // Wait past the debounce window.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Should receive agent_running event from re-begin, NOT an idle event.
        let ev = recv_one(&mut sub).await;
        assert_eq!(
            ev["data"]["activity"], "agent_running",
            "race guard prevents spurious idle emission when count is non-zero"
        );

        // Workspace should still be agent_running.
        assert_eq!(
            h.services.workspace_activity(&h.ws),
            WorkspaceActivity::AgentRunning
        );
    }

    /// Regression for STAB-N: workspace mutation paths must derive `activity`
    /// from live agent state before returning the `Workspace` on the wire (§9.9).
    /// When a workspace has agents in-flight, mutations that return a `Workspace`
    /// must set `ws.activity = workspace_activity(&id)` so the FE receives
    /// `agent_running`, not the stale/default `idle` from the persisted row.
    #[tokio::test]
    async fn update_workspace_derives_activity_from_live_agent_state() {
        use intent_core::{WorkspaceActivity, WorkspaceApi, WorkspaceUpdate};
        let h = harness().await;
        let _guard = DebounceEnvGuard::new("50");

        // Baseline: with no agents in-flight, update_workspace returns activity=idle.
        let updated = h
            .services
            .update_workspace(
                h.ws.clone(),
                WorkspaceUpdate {
                    title: Some("Renamed Without Agent".to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("update workspace");
        assert_eq!(
            updated.activity,
            WorkspaceActivity::Idle,
            "without agents in-flight, activity is idle"
        );

        // Start agent activity: the workspace now has 1 in-flight session.
        h.services.agent_activity_begin(&h.ws).await;
        assert_eq!(
            h.services.workspace_activity(&h.ws),
            WorkspaceActivity::AgentRunning,
            "workspace_activity() reports agent_running"
        );

        // Regression: update_workspace must return activity=agent_running,
        // not the stale default `idle` from the persisted row.
        let updated_with_agent = h
            .services
            .update_workspace(
                h.ws.clone(),
                WorkspaceUpdate {
                    title: Some("Renamed With Agent".to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("update workspace with agent");
        assert_eq!(
            updated_with_agent.activity,
            WorkspaceActivity::AgentRunning,
            "update_workspace MUST derive activity=agent_running when agents in-flight"
        );

        // End agent activity: the workspace returns to idle after debounce window.
        h.services.agent_activity_end(&h.ws).await;
        // During grace window, workspace_activity() still reports AgentRunning.
        assert_eq!(
            h.services.workspace_activity(&h.ws),
            WorkspaceActivity::AgentRunning,
            "during grace window, workspace_activity() still reports AgentRunning"
        );
        // Wait for debounce window to expire (50ms test window).
        tokio::time::sleep(Duration::from_millis(60)).await;
        assert_eq!(
            h.services.workspace_activity(&h.ws),
            WorkspaceActivity::Idle,
            "after debounce window, workspace_activity() reports idle"
        );

        // Confirm update_workspace returns activity=idle again.
        let updated_after_agent = h
            .services
            .update_workspace(
                h.ws.clone(),
                WorkspaceUpdate {
                    title: Some("Renamed After Agent".to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("update workspace after agent");
        assert_eq!(
            updated_after_agent.activity,
            WorkspaceActivity::Idle,
            "update_workspace reports idle after agents end and debounce expires"
        );
    }

    /// Regression for STAB-N: `dismiss_attention` must derive activity.
    #[tokio::test]
    async fn dismiss_attention_derives_activity_from_live_agent_state() {
        use intent_core::{WorkspaceActivity, WorkspaceAttention};
        let h = harness().await;

        // Set attention so there's something to dismiss.
        h.services
            .raise_attention(&h.ws, WorkspaceAttention::Unread)
            .await
            .expect("raise attention");

        // Start agent activity.
        h.services.agent_activity_begin(&h.ws).await;

        // Dismiss attention — must return activity=agent_running.
        let dismissed = h
            .services
            .dismiss_attention(h.ws.clone())
            .await
            .expect("dismiss attention");
        assert_eq!(
            dismissed.activity,
            WorkspaceActivity::AgentRunning,
            "dismiss_attention MUST derive activity=agent_running"
        );

        h.services.agent_activity_end(&h.ws).await;
    }

    /// Regression for STAB-N: `archive_workspace` must derive activity.
    #[tokio::test]
    async fn archive_workspace_derives_activity_from_live_agent_state() {
        use intent_core::WorkspaceActivity;
        let h = harness().await;

        // Start agent activity.
        h.services.agent_activity_begin(&h.ws).await;

        // Archive — must return activity=agent_running.
        let archived = h
            .services
            .archive_workspace(h.ws.clone())
            .await
            .expect("archive workspace");
        assert_eq!(
            archived.activity,
            WorkspaceActivity::AgentRunning,
            "archive_workspace MUST derive activity=agent_running"
        );

        h.services.agent_activity_end(&h.ws).await;
    }

    /// Regression for STAB-N: `unarchive_workspace` must derive activity.
    #[tokio::test]
    async fn unarchive_workspace_derives_activity_from_live_agent_state() {
        use intent_core::WorkspaceActivity;
        let h = harness().await;

        // Archive first.
        h.services
            .archive_workspace(h.ws.clone())
            .await
            .expect("archive workspace");

        // Start agent activity.
        h.services.agent_activity_begin(&h.ws).await;

        // Unarchive — must return activity=agent_running.
        let unarchived = h
            .services
            .unarchive_workspace(h.ws.clone())
            .await
            .expect("unarchive workspace");
        assert_eq!(
            unarchived.activity,
            WorkspaceActivity::AgentRunning,
            "unarchive_workspace MUST derive activity=agent_running"
        );

        h.services.agent_activity_end(&h.ws).await;
    }

    /// Regression for STAB-N: `mark_seen` must derive activity.
    #[tokio::test]
    async fn mark_seen_derives_activity_from_live_agent_state() {
        use intent_core::{WorkspaceActivity, WorkspaceAttention};
        let h = harness().await;

        // Set unread attention so there's something to mark seen.
        h.services
            .raise_attention(&h.ws, WorkspaceAttention::Unread)
            .await
            .expect("raise attention");

        // Start agent activity.
        h.services.agent_activity_begin(&h.ws).await;

        // Mark seen — must return activity=agent_running.
        let seen = h.services.mark_seen(h.ws.clone()).await.expect("mark seen");
        assert_eq!(
            seen.activity,
            WorkspaceActivity::AgentRunning,
            "mark_seen MUST derive activity=agent_running"
        );

        h.services.agent_activity_end(&h.ws).await;
    }

    /// The BE raises `attention`, it persists across a store reload, the raise is
    /// idempotent (no duplicate event), and dismissal clears it + emits
    /// `attention-changed` and is itself idempotent (§9.9).
    #[tokio::test]
    async fn attention_raise_dismiss_persists_and_idempotent() {
        use intent_core::{WorkspaceApi, WorkspaceAttention};
        let h = harness().await;
        let mut sub = subscribe(&h);

        // BE raises the blue dot → persisted + attention-changed { unread }.
        h.services
            .raise_attention(&h.ws, WorkspaceAttention::Unread)
            .await
            .expect("raise");
        let ev = recv_one(&mut sub).await;
        assert_envelope(&ev, &h.ws.0, "workspace:attention-changed");
        assert_eq!(
            ev["data"],
            json!({ "workspaceId": h.ws.0, "attention": "unread" })
        );
        // Survives a reload (persisted column, not derived state).
        let reloaded = h.store.get_workspace(&h.ws).await.expect("reload");
        assert_eq!(reloaded.attention, WorkspaceAttention::Unread);

        // Raising the same level again is idempotent (no second event).
        h.services
            .raise_attention(&h.ws, WorkspaceAttention::Unread)
            .await
            .expect("raise again");

        // Dismiss clears it for everyone → attention-changed { none }. If the
        // idempotent raise had emitted, this would observe unread instead.
        h.services
            .dismiss_attention(h.ws.clone())
            .await
            .expect("dismiss");
        let ev = recv_one(&mut sub).await;
        assert_envelope(&ev, &h.ws.0, "workspace:attention-changed");
        assert_eq!(
            ev["data"],
            json!({ "workspaceId": h.ws.0, "attention": "none" })
        );
        let reloaded = h.store.get_workspace(&h.ws).await.expect("reload");
        assert_eq!(reloaded.attention, WorkspaceAttention::None);

        // Dismiss is idempotent: a no-op on an already-clear workspace.
        let again = h
            .services
            .dismiss_attention(h.ws.clone())
            .await
            .expect("dismiss");
        assert_eq!(again.attention, WorkspaceAttention::None);
    }

    /// `markSeen` clears an `unread` flag (emitting attention-changed) and is a
    /// no-op when there is nothing unread (§9.9).
    #[tokio::test]
    async fn mark_seen_clears_unread_and_is_idempotent() {
        use intent_core::{WorkspaceApi, WorkspaceAttention};
        let h = harness().await;
        h.services
            .raise_attention(&h.ws, WorkspaceAttention::Unread)
            .await
            .expect("raise");
        let mut sub = subscribe(&h);

        // markSeen clears the unread flag and emits attention-changed { none }.
        let seen = h.services.mark_seen(h.ws.clone()).await.expect("seen");
        assert_eq!(seen.attention, WorkspaceAttention::None);
        let ev = recv_one(&mut sub).await;
        assert_envelope(&ev, &h.ws.0, "workspace:attention-changed");
        assert_eq!(
            ev["data"],
            json!({ "workspaceId": h.ws.0, "attention": "none" })
        );
        let reloaded = h.store.get_workspace(&h.ws).await.expect("reload");
        assert_eq!(reloaded.attention, WorkspaceAttention::None);

        // markSeen again is a no-op (already clear).
        let again = h
            .services
            .mark_seen(h.ws.clone())
            .await
            .expect("seen again");
        assert_eq!(again.attention, WorkspaceAttention::None);
    }

    /// `workspace.create` emits `workspace:created` after the row is inserted
    /// (§6.5), with the self-sufficient `{ workspaceId, workspace }` payload
    /// (§6.7). The new workspace mints its own id, so subscribe unfiltered.
    #[tokio::test]
    async fn workspace_created_payload() {
        use intent_core::WorkspaceCreate;
        let h = harness().await;
        let mut sub = h.bus.subscribe(SubscriptionFilter::default());
        let created = h
            .services
            .create_workspace(
                WorkspaceCreate {
                    title: Some("New workspace".to_string()),
                    branch: Some("feat/created-event".to_string()),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("create")
            .workspace;
        let ev = recv_one(&mut sub).await;
        assert_envelope(&ev, &created.id.0, "workspace:created");
        assert_eq!(ev["data"]["workspaceId"], created.id.0);
        assert_eq!(
            ev["data"]["workspace"],
            serde_json::to_value(&created).expect("workspace json")
        );
    }

    /// Idempotency replay (design note TB-0 §5.3): a second `workspace.create`
    /// with the same key returns the ORIGINAL workspace without re-executing —
    /// so no second row, and neither the `workspace:created` nor the seeded
    /// spec's `note:created` is republished on the replay.
    #[tokio::test]
    async fn idempotent_create_workspace_replay_no_second_event() {
        use intent_core::WorkspaceCreate;
        let h = harness().await;
        let mut sub = h.bus.subscribe(SubscriptionFilter::default());
        let key = Some("ws-idem-1".to_string());
        let first = h
            .services
            .create_workspace(
                WorkspaceCreate {
                    title: Some("First".to_string()),
                    branch: Some("feat/idem-a".to_string()),
                    ..Default::default()
                },
                key.clone(),
            )
            .await
            .expect("first create")
            .workspace;
        // The first create publishes `workspace:created` and the spec seed's
        // `note:created`; drain both before checking replay is silent.
        let ev = recv_one(&mut sub).await;
        assert_envelope(&ev, &first.id.0, "workspace:created");
        let ev = recv_one(&mut sub).await;
        assert_envelope(&ev, &first.id.0, "note:created");
        assert_eq!(ev["data"]["noteId"], "spec");

        // Replay with the same key but different input still returns the
        // original workspace (the body is never re-executed).
        let second = h
            .services
            .create_workspace(
                WorkspaceCreate {
                    title: Some("Second".to_string()),
                    branch: Some("feat/idem-b".to_string()),
                    ..Default::default()
                },
                key,
            )
            .await
            .expect("replay create")
            .workspace;
        assert_eq!(
            second.id.0, first.id.0,
            "replay returns the original workspace"
        );

        // No further events are published (the replay short-circuits the op).
        let none = tokio::time::timeout(Duration::from_millis(300), sub.recv()).await;
        assert!(none.is_err(), "replay must not publish a second event");
    }

    /// `workspace.create` seeds the well-known `spec` note (reference parity
    /// with `notes.service.ts ensureSpecExists`): default Spec — empty
    /// markdown, `spec` tag, pinned, default, workspace visibility — with a
    /// v1 version snapshot and a `note:created` event carrying `noteId=spec`.
    #[tokio::test]
    async fn workspace_create_seeds_spec_note() {
        use intent_core::{NoteId, NoteVisibility, WorkspaceCreate};
        let h = harness().await;
        let mut sub = h.bus.subscribe(SubscriptionFilter::default());
        let created = h
            .services
            .create_workspace(
                WorkspaceCreate {
                    title: Some("Seeded WS".to_string()),
                    branch: Some("feat/seed-spec".to_string()),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("create")
            .workspace;

        // Persisted spec matches the reference default.
        let spec = h
            .store
            .get_note(&created.id, &NoteId::from("spec"))
            .await
            .expect("spec exists");
        assert_eq!(spec.workspace_id, created.id);
        assert_eq!(spec.title, "Spec");
        assert_eq!(spec.content, "");
        assert_eq!(spec.tags, vec!["spec".to_string()]);
        assert!(spec.is_pinned);
        assert!(spec.is_default);
        assert!(!spec.is_archived);
        assert_eq!(spec.visibility, NoteVisibility::Workspace);
        assert!(spec.metadata.task.is_none());

        // Initial version snapshot captured.
        let versions = h
            .store
            .list_note_versions(&created.id, &NoteId::from("spec"))
            .await
            .expect("versions");
        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].v, 1);

        // `workspace:created` first, then the spec seed's `note:created`.
        let ev = recv_one(&mut sub).await;
        assert_envelope(&ev, &created.id.0, "workspace:created");
        let ev = recv_one(&mut sub).await;
        assert_envelope(&ev, &created.id.0, "note:created");
        assert_eq!(ev["data"]["noteId"], "spec");
        assert_eq!(ev["data"]["title"], "Spec");
    }

    /// Spec seeding is idempotent inside the create scope: a replay with the
    /// same `idempotencyKey` short-circuits and does not attempt to reinsert.
    #[tokio::test]
    async fn workspace_create_spec_seed_replay_no_duplicate() {
        use intent_core::{NoteId, WorkspaceCreate};
        let h = harness().await;
        let key = Some("spec-seed-idem".to_string());
        let first = h
            .services
            .create_workspace(
                WorkspaceCreate {
                    title: Some("First".to_string()),
                    branch: Some("feat/spec-seed-a".to_string()),
                    ..Default::default()
                },
                key.clone(),
            )
            .await
            .expect("first create")
            .workspace;
        let _ = h
            .services
            .create_workspace(
                WorkspaceCreate {
                    title: Some("Second".to_string()),
                    branch: Some("feat/spec-seed-b".to_string()),
                    ..Default::default()
                },
                key,
            )
            .await
            .expect("replay create");
        let spec = h
            .store
            .get_note(&first.id, &NoteId::from("spec"))
            .await
            .expect("spec exists");
        assert_eq!(spec.workspace_id, first.id);
        let versions = h
            .store
            .list_note_versions(&first.id, &NoteId::from("spec"))
            .await
            .expect("versions");
        assert_eq!(versions.len(), 1, "replay must not append a second version");
    }

    /// Note identity is composite (`(id, workspace_id)`, migration 0030): each
    /// workspace owns its own `spec` note. Two separate `workspace.create`
    /// calls each seed a fresh spec scoped to their workspace; there is no
    /// cross-workspace collision on the well-known `spec` id.
    #[tokio::test]
    async fn spec_seed_is_workspace_scoped_no_cross_workspace_collision() {
        use intent_core::{NoteId, WorkspaceCreate};
        let h = harness().await;
        let first = h
            .services
            .create_workspace(
                WorkspaceCreate {
                    title: Some("First".to_string()),
                    branch: Some("feat/spec-owner-a".to_string()),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("first create")
            .workspace;
        let second = h
            .services
            .create_workspace(
                WorkspaceCreate {
                    title: Some("Second".to_string()),
                    branch: Some("feat/spec-owner-b".to_string()),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("second create")
            .workspace;
        assert_ne!(second.id, first.id);
        let spec_a = h
            .store
            .get_note(&first.id, &NoteId::from("spec"))
            .await
            .expect("first spec exists");
        assert_eq!(spec_a.workspace_id, first.id);
        let spec_b = h
            .store
            .get_note(&second.id, &NoteId::from("spec"))
            .await
            .expect("second spec exists");
        assert_eq!(spec_b.workspace_id, second.id);
    }

    /// Reference parity with `notes.service.ts getNotes`: `note.list` for a
    /// workspace missing its `spec` note reseeds the default spec (empty
    /// markdown, `Spec` title, `spec` tag, pinned, default, workspace
    /// visibility), returns it in the response, and emits `note:created`
    /// exactly once. Heals workspaces that predate the composite-PK spec seed.
    #[tokio::test]
    async fn note_list_reseeds_missing_spec_note() {
        use intent_core::{NoteId, NoteVisibility};
        let h = harness().await;
        let mut sub = subscribe(&h);

        // Preconditions: workspace exists (harness inserted the row) but no
        // spec has been seeded — this mirrors a pre-#110 workspace.
        assert!(matches!(
            h.store.get_note(&h.ws, &NoteId::from("spec")).await,
            Err(intent_core::Error::NotFound(_))
        ));

        let notes = h.services.list_notes(&h.ws).await.expect("list");
        assert_eq!(notes.len(), 1);
        let spec = &notes[0];
        assert_eq!(spec.id, NoteId::from("spec"));
        assert_eq!(spec.workspace_id, h.ws);
        assert_eq!(spec.title, "Spec");
        assert_eq!(spec.content, "");
        assert_eq!(spec.tags, vec!["spec".to_string()]);
        assert!(spec.is_pinned);
        assert!(spec.is_default);
        assert!(!spec.is_archived);
        assert_eq!(spec.visibility, NoteVisibility::Workspace);

        // Reseed captures the initial version snapshot, same as create-time.
        let versions = h
            .store
            .list_note_versions(&h.ws, &NoteId::from("spec"))
            .await
            .expect("versions");
        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].v, 1);

        // Exactly one `note:created` fires for the reseed.
        let ev = recv_one(&mut sub).await;
        assert_envelope(&ev, &h.ws.0, "note:created");
        assert_eq!(ev["data"]["noteId"], "spec");
        assert_eq!(ev["data"]["title"], "Spec");

        // No further events fire — reseed publishes exactly once.
        let quiet = tokio::time::timeout(Duration::from_millis(300), sub.recv()).await;
        assert!(
            quiet.is_err(),
            "reseed must publish exactly one event, got extra: {quiet:?}"
        );
    }

    /// A workspace that already has a `spec` note is untouched by `note.list`:
    /// no spurious `note:created`, no extra version snapshot, no rev bump.
    #[tokio::test]
    async fn note_list_leaves_existing_spec_untouched() {
        use intent_core::{NoteId, WorkspaceCreate};
        let h = harness().await;

        // `create_workspace` seeds the spec; drain both events so the sub is
        // clean before the list call.
        let mut sub = h.bus.subscribe(SubscriptionFilter::default());
        let created = h
            .services
            .create_workspace(
                WorkspaceCreate {
                    title: Some("Seeded".to_string()),
                    branch: Some("feat/seeded-list".to_string()),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("create")
            .workspace;
        let _ = recv_one(&mut sub).await; // workspace:created
        let _ = recv_one(&mut sub).await; // note:created (seed)

        let spec_before = h
            .store
            .get_note(&created.id, &NoteId::from("spec"))
            .await
            .expect("spec exists");

        let notes = h.services.list_notes(&created.id).await.expect("list");
        let spec_row = notes
            .iter()
            .find(|n| n.id == NoteId::from("spec"))
            .expect("spec listed");
        // Same rev / updated_at / created_at as before — no write happened.
        assert_eq!(spec_row.rev, spec_before.rev);
        assert_eq!(spec_row.updated_at, spec_before.updated_at);
        assert_eq!(spec_row.created_at, spec_before.created_at);

        // No further events fire for the untouched-spec list call.
        let none = tokio::time::timeout(Duration::from_millis(300), sub.recv()).await;
        assert!(none.is_err(), "list must not publish for existing spec");

        // Version count unchanged.
        let versions = h
            .store
            .list_note_versions(&created.id, &NoteId::from("spec"))
            .await
            .expect("versions");
        assert_eq!(versions.len(), 1);
    }

    /// After a client deletes the `spec` note (§5 `note.delete`), the next
    /// `note.list` reseeds it and emits `note:created`. Regression guard for
    /// the self-healing behaviour the reference relies on.
    #[tokio::test]
    async fn note_list_reseeds_after_spec_deletion() {
        use intent_core::{NoteId, WorkspaceCreate};
        let h = harness().await;
        let mut sub = h.bus.subscribe(SubscriptionFilter::default());
        let created = h
            .services
            .create_workspace(
                WorkspaceCreate {
                    title: Some("Heal".to_string()),
                    branch: Some("feat/heal-spec".to_string()),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("create")
            .workspace;
        let _ = recv_one(&mut sub).await; // workspace:created
        let _ = recv_one(&mut sub).await; // note:created (initial seed)

        // Drop the spec straight through the store to simulate a corrupt /
        // manually-cleared workspace.
        h.store
            .delete_note(&created.id, &NoteId::from("spec"))
            .await
            .expect("delete spec");

        let notes = h.services.list_notes(&created.id).await.expect("list");
        assert!(notes.iter().any(|n| n.id == NoteId::from("spec")));

        let ev = recv_one(&mut sub).await;
        assert_envelope(&ev, &created.id.0, "note:created");
        assert_eq!(ev["data"]["noteId"], "spec");
    }

    /// A `note.list` for a workspace id that has no matching workspace row
    /// must not regress from `Ok([])` to `Err`: the reseed attempt trips the
    /// `note.workspace_id → workspace.id` FK, but the failure is swallowed
    /// (best-effort self-heal) and the empty listing is returned unchanged.
    /// No `note:created` fires.
    #[tokio::test]
    async fn note_list_tolerates_reseed_failure_for_unknown_workspace() {
        let h = harness().await;
        let mut sub = h.bus.subscribe(SubscriptionFilter::default());
        let unknown = intent_core::WorkspaceId::new();

        let notes = h.services.list_notes(&unknown).await.expect("list");
        assert!(notes.is_empty());

        let none = tokio::time::timeout(Duration::from_millis(300), sub.recv()).await;
        assert!(
            none.is_err(),
            "unknown-workspace list must not publish a reseed event"
        );
    }

    /// The Chief virtual workspace has no store row (synthesized via
    /// `chief_workspace()`), so `note.list` must not attempt to reseed a spec
    /// against a nonexistent FK target. Reseed is skipped; the list returns
    /// empty and no `note:created` fires.
    #[tokio::test]
    async fn note_list_skips_reseed_for_chief_virtual_workspace() {
        let h = harness().await;
        let mut sub = h.bus.subscribe(SubscriptionFilter::default());
        let chief = intent_core::WorkspaceId::chief();

        let notes = h.services.list_notes(&chief).await.expect("list");
        assert!(notes.is_empty());

        let none = tokio::time::timeout(Duration::from_millis(300), sub.recv()).await;
        assert!(none.is_err(), "chief list must not publish a reseed event");
    }

    /// Self-heal for workspaces damaged by the pre-#110 global-note-identity
    /// bug: on `note.list` with no `id='spec'` note but exactly one top-level,
    /// non-task note titled "Spec", the stray is *adopted* — its `note.id` is
    /// rewritten to `'spec'`, children are re-parented, version history / line
    /// attribution / comments follow, `is_pinned` / `is_default` / the `spec`
    /// tag are set, and delete+create events fire so live FE clients converge.
    #[tokio::test]
    async fn note_list_adopts_stray_spec_note() {
        use intent_core::{
            now_iso, AuthorType, Comment, CommentAnchor, CommentAnchorType, CommentStatus,
            CommentType, ContentType, LineAttributionData, Note, NoteId, NoteMetadata,
            NoteVersionAuthor, NoteVisibility, TaskMetadata, TaskStatus,
        };
        use std::collections::BTreeMap;

        let h = harness().await;

        // Seed a stray "Spec" note (random UUID id, pre-#110 shape): top-level,
        // non-task, existing tag list to verify merge preserves callers' tags.
        let stray_id = NoteId::new();
        let stray_ts = now_iso();
        let stray = Note {
            id: stray_id.clone(),
            workspace_id: h.ws.clone(),
            title: "Spec".to_string(),
            content: "# Real spec content\n\nkeep me".to_string(),
            content_type: ContentType::Markdown,
            tags: vec!["custom".to_string()],
            is_pinned: false,
            is_archived: false,
            is_default: false,
            parent_id: None,
            visibility: NoteVisibility::Workspace,
            metadata: NoteMetadata::default(),
            created_at: stray_ts.clone(),
            rev: 0,
            updated_at: stray_ts.clone(),
        };
        h.store.insert_note(&stray).await.expect("insert stray");

        // Two task-note children whose parent is the stray UUID id — after
        // adoption they must point at `spec` instead.
        let mk_child = |id: &str| Note {
            id: NoteId::from(id),
            workspace_id: h.ws.clone(),
            title: id.to_string(),
            content: String::new(),
            content_type: ContentType::Markdown,
            tags: vec![],
            is_pinned: false,
            is_archived: false,
            is_default: false,
            parent_id: Some(stray_id.clone()),
            visibility: NoteVisibility::Workspace,
            metadata: NoteMetadata {
                task: Some(TaskMetadata {
                    status: TaskStatus::NotStarted,
                    ..Default::default()
                }),
            },
            created_at: stray_ts.clone(),
            rev: 0,
            updated_at: stray_ts.clone(),
        };
        let child_a = mk_child("task-a");
        let child_b = mk_child("task-b");
        h.store.insert_note(&child_a).await.expect("insert child a");
        h.store.insert_note(&child_b).await.expect("insert child b");

        // Version history + line attribution + a comment, all keyed to the
        // stray id and expected to follow into `spec`.
        let author = NoteVersionAuthor {
            id: "user".to_string(),
            name: "User".to_string(),
            author_type: "user".to_string(),
        };
        h.store
            .append_note_version(&stray, &author, &stray_ts)
            .await
            .expect("v1");

        let mut attributions = BTreeMap::new();
        attributions.insert(
            "1".to_string(),
            intent_core::LineAttributionInfo {
                timestamp: 0,
                author: Some(intent_core::LineAttributionAuthor {
                    id: "user".to_string(),
                    name: "User".to_string(),
                    author_type: "user".to_string(),
                    turn_number: None,
                }),
            },
        );
        h.store
            .upsert_note_line_attribution(&LineAttributionData {
                note_id: stray_id.clone(),
                workspace_id: h.ws.clone(),
                computed_at: stray_ts.clone(),
                attributions,
            })
            .await
            .expect("upsert attribution");

        let comment = Comment {
            id: "c1".to_string(),
            thread_id: "t1".to_string(),
            note_id: Some(stray_id.clone()),
            kind: CommentType::Comment,
            content: "hi".to_string(),
            author: "user".to_string(),
            author_type: AuthorType::User,
            status: CommentStatus::Open,
            parent_id: None,
            anchor: CommentAnchor {
                kind: CommentAnchorType::Range,
                ..Default::default()
            },
            anchor_text: None,
            anchor_before: None,
            anchor_after: None,
            suggestion_original: None,
            suggestion_proposed: None,
            agent_id: None,
            is_orphaned: None,
            created_at: stray_ts.clone(),
            updated_at: stray_ts,
        };
        h.store
            .insert_comment(&h.ws, &comment)
            .await
            .expect("insert comment");

        let mut sub = subscribe(&h);

        // Trigger the self-heal via `note.list` — the public path §5 clients
        // use to converge a damaged workspace.
        let notes = h.services.list_notes(&h.ws).await.expect("list");

        // The stray UUID is gone; `spec` carries the adopted content, tags
        // merge (`spec` added, `custom` preserved), and the pinned/default
        // flags are on.
        assert!(
            !notes.iter().any(|n| n.id == stray_id),
            "old UUID note must be replaced"
        );
        let spec = notes
            .iter()
            .find(|n| n.id == NoteId::from("spec"))
            .expect("spec listed");
        assert_eq!(spec.title, "Spec");
        assert_eq!(spec.content, "# Real spec content\n\nkeep me");
        assert!(spec.tags.iter().any(|t| t == "spec"));
        assert!(spec.tags.iter().any(|t| t == "custom"));
        assert!(spec.is_pinned);
        assert!(spec.is_default);

        // Children re-parented to `spec`.
        let child_a_now = h
            .store
            .get_note(&h.ws, &NoteId::from("task-a"))
            .await
            .expect("child a");
        let child_b_now = h
            .store
            .get_note(&h.ws, &NoteId::from("task-b"))
            .await
            .expect("child b");
        assert_eq!(child_a_now.parent_id, Some(NoteId::from("spec")));
        assert_eq!(child_b_now.parent_id, Some(NoteId::from("spec")));

        // Version history moved.
        let versions_old = h
            .store
            .list_note_versions(&h.ws, &stray_id)
            .await
            .expect("versions old");
        assert!(versions_old.is_empty());
        let versions_new = h
            .store
            .list_note_versions(&h.ws, &NoteId::from("spec"))
            .await
            .expect("versions new");
        assert_eq!(versions_new.len(), 1);

        // Line attribution moved.
        let attr_old = h
            .store
            .get_note_line_attribution(&h.ws, &stray_id)
            .await
            .expect("attr old");
        assert!(attr_old.is_none());
        let attr_new = h
            .store
            .get_note_line_attribution(&h.ws, &NoteId::from("spec"))
            .await
            .expect("attr new");
        assert!(attr_new.is_some());

        // Comments moved.
        let comments_old = h
            .store
            .list_comments_in_workspace(&h.ws, &stray_id)
            .await
            .expect("comments old");
        assert!(comments_old.is_empty());
        let comments_new = h
            .store
            .list_comments_in_workspace(&h.ws, &NoteId::from("spec"))
            .await
            .expect("comments new");
        assert_eq!(comments_new.len(), 1);
        assert_eq!(comments_new[0].id, "c1");

        // Events: `note:deleted` for the old id, then `note:created` for
        // `spec` — both carry the adopted title so FE tree entries reconcile.
        let del = recv_one(&mut sub).await;
        assert_envelope(&del, &h.ws.0, "note:deleted");
        assert_eq!(del["data"]["noteId"], stray_id.0);
        assert_eq!(del["data"]["title"], "Spec");
        assert_eq!(del["data"]["action"], "delete");
        let created = recv_one(&mut sub).await;
        assert_envelope(&created, &h.ws.0, "note:created");
        assert_eq!(created["data"]["noteId"], "spec");
        assert_eq!(created["data"]["title"], "Spec");
        assert_eq!(created["data"]["action"], "create");

        // Idempotency: a second `note.list` is a no-op — no further events,
        // no additional version snapshots.
        let _ = h.services.list_notes(&h.ws).await.expect("list again");
        let quiet = tokio::time::timeout(Duration::from_millis(200), sub.recv()).await;
        assert!(
            quiet.is_err(),
            "second list must not republish after adoption"
        );
        let versions_after = h
            .store
            .list_note_versions(&h.ws, &NoteId::from("spec"))
            .await
            .expect("versions after");
        assert_eq!(versions_after.len(), 1);
    }

    /// Ambiguity (≥2 top-level "Spec" notes): the adoption bails out and
    /// falls through to the existing empty-seed path. Both strays stay put;
    /// the new `spec` is a fresh empty row.
    #[tokio::test]
    async fn note_list_falls_back_to_empty_seed_when_multiple_spec_candidates() {
        use intent_core::{now_iso, ContentType, Note, NoteId, NoteMetadata, NoteVisibility};
        let h = harness().await;
        let mk = |id: &str, title: &str, body: &str| Note {
            id: NoteId::from(id),
            workspace_id: h.ws.clone(),
            title: title.to_string(),
            content: body.to_string(),
            content_type: ContentType::Markdown,
            tags: vec![],
            is_pinned: false,
            is_archived: false,
            is_default: false,
            parent_id: None,
            visibility: NoteVisibility::Workspace,
            metadata: NoteMetadata::default(),
            created_at: now_iso(),
            rev: 0,
            updated_at: now_iso(),
        };
        // Two top-level candidates — case/whitespace variations still count.
        let s1 = mk("stray-1", "Spec", "one");
        let s2 = mk("stray-2", "  spec  ", "two");
        h.store.insert_note(&s1).await.expect("insert s1");
        h.store.insert_note(&s2).await.expect("insert s2");

        let mut sub = subscribe(&h);
        let notes = h.services.list_notes(&h.ws).await.expect("list");

        // Fresh empty seed, both strays untouched.
        let spec = notes
            .iter()
            .find(|n| n.id == NoteId::from("spec"))
            .expect("spec seeded");
        assert_eq!(spec.content, "");
        assert!(notes.iter().any(|n| n.id == NoteId::from("stray-1")));
        assert!(notes.iter().any(|n| n.id == NoteId::from("stray-2")));

        // Exactly one `note:created` (the empty seed) — no `note:deleted`.
        let ev = recv_one(&mut sub).await;
        assert_envelope(&ev, &h.ws.0, "note:created");
        assert_eq!(ev["data"]["noteId"], "spec");
        let quiet = tokio::time::timeout(Duration::from_millis(200), sub.recv()).await;
        assert!(quiet.is_err(), "no adoption events on ambiguous fallback");
    }

    /// Candidates must be top-level and non-task: a task-note titled "Spec"
    /// or a child note titled "Spec" is *not* adopted. With no other stray
    /// present the caller falls through to the empty-seed path.
    #[tokio::test]
    async fn note_list_ignores_task_or_child_spec_candidates() {
        use intent_core::{
            now_iso, ContentType, Note, NoteId, NoteMetadata, NoteVisibility, TaskMetadata,
            TaskStatus,
        };
        let h = harness().await;
        let base = |id: &str, title: &str| Note {
            id: NoteId::from(id),
            workspace_id: h.ws.clone(),
            title: title.to_string(),
            content: String::new(),
            content_type: ContentType::Markdown,
            tags: vec![],
            is_pinned: false,
            is_archived: false,
            is_default: false,
            parent_id: None,
            visibility: NoteVisibility::Workspace,
            metadata: NoteMetadata::default(),
            created_at: now_iso(),
            rev: 0,
            updated_at: now_iso(),
        };
        let parent = base("parent", "Parent");
        h.store.insert_note(&parent).await.expect("insert parent");
        let mut child = base("child-spec", "Spec");
        child.parent_id = Some(NoteId::from("parent"));
        h.store.insert_note(&child).await.expect("insert child");
        let mut task = base("task-spec", "Spec");
        task.metadata.task = Some(TaskMetadata {
            status: TaskStatus::NotStarted,
            ..Default::default()
        });
        h.store.insert_note(&task).await.expect("insert task");

        let notes = h.services.list_notes(&h.ws).await.expect("list");
        let spec = notes
            .iter()
            .find(|n| n.id == NoteId::from("spec"))
            .expect("spec seeded");
        assert_eq!(spec.content, "", "task/child matches must not be adopted");
        assert!(notes.iter().any(|n| n.id == NoteId::from("child-spec")));
        assert!(notes.iter().any(|n| n.id == NoteId::from("task-spec")));
    }

    /// A workspace that already has `id='spec'` is untouched even if a stray
    /// "Spec" UUID note sits beside it: `ensure_spec_note` returns early on
    /// the healthy path, no adoption runs, no adoption events fire.
    #[tokio::test]
    async fn note_list_does_not_adopt_when_spec_already_exists() {
        use intent_core::{
            now_iso, ContentType, Note, NoteId, NoteMetadata, NoteVisibility, WorkspaceCreate,
        };
        let h = harness().await;
        let mut sub = h.bus.subscribe(SubscriptionFilter::default());
        let created = h
            .services
            .create_workspace(
                WorkspaceCreate {
                    title: Some("Healthy".to_string()),
                    branch: Some("feat/adopt-noop".to_string()),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("create")
            .workspace;
        let _ = recv_one(&mut sub).await; // workspace:created
        let _ = recv_one(&mut sub).await; // note:created (seed)

        let stray_id = NoteId::new();
        let stray = Note {
            id: stray_id.clone(),
            workspace_id: created.id.clone(),
            title: "Spec".to_string(),
            content: "stray body".to_string(),
            content_type: ContentType::Markdown,
            tags: vec![],
            is_pinned: false,
            is_archived: false,
            is_default: false,
            parent_id: None,
            visibility: NoteVisibility::Workspace,
            metadata: NoteMetadata::default(),
            created_at: now_iso(),
            rev: 0,
            updated_at: now_iso(),
        };
        h.store.insert_note(&stray).await.expect("insert stray");

        let notes = h.services.list_notes(&created.id).await.expect("list");
        // Both notes present, stray untouched.
        assert!(notes.iter().any(|n| n.id == NoteId::from("spec")));
        let stray_now = h
            .store
            .get_note(&created.id, &stray_id)
            .await
            .expect("stray present");
        assert_eq!(stray_now.content, "stray body");

        let quiet = tokio::time::timeout(Duration::from_millis(200), sub.recv()).await;
        assert!(quiet.is_err(), "healthy workspace must not fire adoption");
    }
}

/// End-to-end §6.8 "one impl, two front doors": an agent (via the in-process MCP
/// callback server) calls a workspace tool against the SAME store-backed
/// `WorkspaceApi` the FE uses; the BE state changes and a change event fires.
mod mcp_callback {
    use std::sync::Arc;
    use std::time::Duration;

    use intent_acp::WorkspaceMcpServer;
    use intent_core::events::NOTE_UPDATED;
    use intent_core::{NoteId, WorkspaceApi, WorkspaceId};
    use intent_store::Store;
    use serde_json::json;

    use super::{note, workspace, TempDb};
    use crate::{EventBus, Services, SubscriptionFilter};

    #[tokio::test]
    async fn agent_note_add_through_mcp_changes_state_and_fires_event() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws = WorkspaceId::new();
        store.insert_workspace(&workspace(&ws)).await.expect("ws");
        let note_id = NoteId::from("n1");
        store
            .insert_note(&note(&ws, "n1", "# A\nbody"))
            .await
            .expect("note");
        let bus = EventBus::new(store.clone());
        let services = Services::new(store).with_event_bus(bus.clone());

        // Subscribe before the call; un-batched → immediate single-event batch.
        let mut sub = bus.subscribe(SubscriptionFilter {
            workspace_id: Some(ws.0.clone()),
            ..Default::default()
        });

        let api: Arc<dyn WorkspaceApi> = Arc::new(services);
        let server = WorkspaceMcpServer::new(api.clone(), ws.clone());

        // After the WSAPI-8 cutover the MCP front door only exposes
        // `workspace_api`; the discrete `add_to_note` tool is gone. Route
        // the equivalent state change through `ws.note.add`.
        let resp = server
            .handle_message(&json!({
                "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                "params": {
                    "name": "workspace_api",
                    "arguments": {
                        "code": "return await ws.note.add('n1', { content: 'more' });",
                        "summary": "test add_to_note via ws.note.add"
                    }
                }
            }))
            .await
            .expect("tools/call returns a response");
        assert_eq!(resp["result"]["isError"], json!(false));

        // BE state changed: the note content was persisted through the shared API.
        let persisted = api.get_note(ws.clone(), note_id).await.expect("get");
        assert_eq!(persisted.content, "# A\nbody\n\nmore");

        // Event fired on the same bus the FE transport pushes from.
        let batch = tokio::time::timeout(Duration::from_secs(2), sub.recv())
            .await
            .expect("event delivered")
            .expect("subscription open");
        assert!(batch.iter().any(|e| e.event_type == NOTE_UPDATED));
    }

    /// The MCP front door threads its bound `caller_agent_id` down into
    /// `WorkspaceApi::add_to_note`, so the version snapshot appended by the
    /// mutation carries the acting agent's session name (reference parity with
    /// `notes.service.ts` L518-555 — `currentActor?.type === 'agent'` branch).
    #[tokio::test]
    async fn agent_note_add_through_mcp_stamps_agent_version_author() {
        use intent_core::{now_iso, AgentId, AgentSession, AgentStatus};

        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws = WorkspaceId::new();
        store.insert_workspace(&workspace(&ws)).await.expect("ws");
        let note_id = NoteId::from("n1");
        store
            .insert_note(&note(&ws, "n1", "# A\nbody"))
            .await
            .expect("note");
        let agent_id = AgentId::from_string("agent-mcp-writer");
        let session = AgentSession {
            id: agent_id.clone(),
            workspace_id: ws.clone(),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: "McpWriter".to_string(),
            name_explicitly_set: true,
            model: None,
            provider: None,
            system_prompt: None,
            specialist: None,
            status: AgentStatus::Active,
            is_active: true,
            messages: vec![],
            stats: None,
            task_note_id: None,
            skip_auto_commit: false,
            completion_report: None,
            completion_report_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            is_background: false,
            metadata: None,
            created_at: now_iso(),
            updated_at: now_iso(),
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
        };
        store.insert_agent_session(&session).await.expect("session");

        let services = Services::new(store.clone());
        let api: Arc<dyn WorkspaceApi> = Arc::new(services);
        let server = WorkspaceMcpServer::new(api.clone(), ws.clone())
            .with_caller_agent_id(Some(agent_id.clone()));

        let resp = server
            .handle_message(&json!({
                "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                "params": {
                    "name": "workspace_api",
                    "arguments": {
                        "code": "return await ws.note.add('n1', { content: 'more' });",
                        "summary": "agent note.add via MCP"
                    }
                }
            }))
            .await
            .expect("tools/call returns a response");
        assert_eq!(resp["result"]["isError"], json!(false));

        let versions = store
            .list_note_versions(&ws, &note_id)
            .await
            .expect("versions");
        let last = versions.last().expect("version appended");
        assert_eq!(last.author.id, "agent-mcp-writer");
        assert_eq!(last.author.name, "McpWriter");
        assert_eq!(last.author.author_type, "agent");
    }
}

// ============================================================================
// drafts.* — BE-persisted per-client drafts emit `draft:changed` WITHOUT the
// draft text (no leakage, PROTOCOL §5.16/§6.5).
// ============================================================================
mod drafts_events {
    use std::time::Duration;

    use intent_core::events::DRAFT_CHANGED;
    use intent_core::{AgentId, ClientId, WorkspaceId};
    use intent_store::Store;
    use serde_json::json;

    use super::{workspace, TempDb};
    use crate::{EventBus, Services, SubscriptionFilter};

    #[tokio::test]
    async fn draft_set_then_clear_emit_change_events_without_text() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws = WorkspaceId::new();
        store.insert_workspace(&workspace(&ws)).await.expect("ws");
        let client = ClientId::from_string("cli-secret");
        store
            .upsert_client(&client, None, None)
            .await
            .expect("client");
        let agent = AgentId::from_string("agent-1");

        let bus = EventBus::new(store.clone());
        let services = Services::new(store).with_event_bus(bus.clone());
        let mut sub = bus.subscribe(SubscriptionFilter {
            workspace_id: Some(ws.0.clone()),
            ..Default::default()
        });

        let secret = "TOP SECRET DRAFT TEXT";
        let updated = services
            .drafts_set(
                ws.clone(),
                agent.clone(),
                client.clone(),
                secret.to_string(),
            )
            .await
            .expect("set draft");
        assert!(updated.is_some(), "a non-empty set stores a draft");

        let batch = tokio::time::timeout(Duration::from_secs(2), sub.recv())
            .await
            .expect("event delivered")
            .expect("subscription open");
        let ev = batch
            .iter()
            .find(|e| e.event_type == DRAFT_CHANGED)
            .expect("draft:changed fired");
        assert_eq!(ev.data["hasDraft"], json!(true));
        assert_eq!(ev.data["workspaceId"], json!(ws.0));
        assert_eq!(ev.data["agentId"], json!(agent.0));
        assert_eq!(ev.data["clientId"], json!(client.0));
        assert!(
            ev.data.get("text").is_none(),
            "draft:changed must NOT carry text"
        );
        assert!(
            !serde_json::to_string(&ev.data).unwrap().contains(secret),
            "the draft text never appears in the event payload"
        );

        // Clearing emits hasDraft=false (still no text).
        services
            .drafts_clear(ws.clone(), agent.clone(), client.clone())
            .await
            .expect("clear draft");
        let batch = tokio::time::timeout(Duration::from_secs(2), sub.recv())
            .await
            .expect("event delivered")
            .expect("subscription open");
        let ev = batch
            .iter()
            .find(|e| e.event_type == DRAFT_CHANGED)
            .expect("draft:changed fired on clear");
        assert_eq!(ev.data["hasDraft"], json!(false));
        assert!(ev.data.get("text").is_none());
    }
}

// ============================================================================
// pr.* read methods over a stubbed forge (no network). Asserts the parity-exact
// status/reviews/check-run shapes and the review-thread filtering/fallback.
// ============================================================================

mod pr {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    use async_trait::async_trait;
    use intent_core::{now_iso, Error, WorkspaceApi, WorkspaceId};
    use intent_sourcecontrol::{
        AuthStatus, Branch, CheckRun, CheckState, Comment, CommentAnchor, Error as ScError, Issue,
        IssueQuery, MergeMethod, MergeOptions, MergeOutcome, Mergeability, NewPullRequest, Page,
        PageParams, PrPatch, PrQuery, PrState, PullRequest, Repo, RepoRef, Result as ScResult,
        Review, ReviewComment, ReviewThread, ReviewThreadComment, ReviewVerdict, ScCapabilities,
        SourceControl, UserIdentity,
    };
    use intent_store::Store;
    use serde_json::json;

    use super::{workspace, TempDb};
    use crate::Services;

    #[derive(Default)]
    struct StubForge {
        fail_threads: bool,
        /// When set, each `get_pr` returns a fresh head SHA so the
        /// `pr.waitForChanges` poll detects a commit change.
        mutate_head: bool,
        /// When set, `list_prs` returns the sample PR so PR-refresh discovery can
        /// link an unlinked workspace by head ref.
        discover: bool,
        /// When set, `get_pr` returns the sample PR as merged, so refresh sees a
        /// linked PR whose lifecycle ended (relink-after-merge tests).
        merged_linked: bool,
        /// When set, `list_prs` also returns an *open* PR with this number (same
        /// head ref as the sample), simulating a newer PR on the same branch.
        open_pr_number: Option<u64>,
        /// When set, `list_prs` fails, simulating a transient forge error
        /// during relink discovery.
        fail_list_prs: bool,
        /// When set, `list_repos` emits a two-page sequence driven by the opaque
        /// cursor, exercising the §5.5 multi-page round-trip end to end.
        paginate: bool,
        head_seq: AtomicU64,
        /// Notified on the first `get_pr` call — the point at which
        /// `pr.waitForChanges` has finished its one SQLite read and is past it.
        /// The paused-clock poll tests use this as a barrier (see
        /// `run_wait_for_changes`).
        first_get_pr: Arc<tokio::sync::Notify>,
    }

    fn sample_pr() -> PullRequest {
        PullRequest {
            number: 42,
            url: "https://github.com/o/r/pull/42".into(),
            title: "Add thing".into(),
            body: None,
            state: PrState::Open,
            draft: false,
            source_branch: "feature".into(),
            target_branch: "main".into(),
            author: "octocat".into(),
            mergeable: Some(true),
            mergeable_state: Some("clean".into()),
            head_sha: Some("deadbeef".into()),
            created_at: String::new(),
            updated_at: String::new(),
        }
    }

    #[async_trait]
    impl SourceControl for StubForge {
        fn provider_id(&self) -> &'static str {
            "stub"
        }
        fn capabilities(&self) -> ScCapabilities {
            ScCapabilities {
                draft_prs: true,
                squash_merge: true,
                rebase_merge: true,
                review_required_changes: true,
                check_runs: true,
                issues: true,
            }
        }
        async fn check_auth(&self) -> ScResult<AuthStatus> {
            Ok(AuthStatus {
                authenticated: true,
                login: Some("octocat".into()),
                scopes: vec![],
            })
        }
        async fn get_user(&self) -> ScResult<UserIdentity> {
            Ok(UserIdentity {
                login: "octocat".into(),
                id: Some(583231),
                name: Some("The Octocat".into()),
                avatar_url: Some("https://avatars.example/u/1".into()),
                html_url: Some("https://github.com/octocat".into()),
            })
        }
        async fn list_repos(&self, page: PageParams) -> ScResult<Page<Repo>> {
            if self.paginate {
                // A two-page sequence driven by the opaque cursor that the
                // services layer round-trips through `nextToken`.
                let p = page
                    .cursor
                    .as_deref()
                    .and_then(|c| c.parse::<u64>().ok())
                    .unwrap_or(1);
                let repo = Repo {
                    owner: "octocat".into(),
                    name: format!("repo{p}"),
                    url: Some(format!("https://github.com/octocat/repo{p}")),
                    default_branch: Some("main".into()),
                    created_at: None,
                    updated_at: None,
                };
                let next_cursor = if p < 2 {
                    Some((p + 1).to_string())
                } else {
                    None
                };
                return Ok(Page {
                    items: vec![repo],
                    next_cursor,
                });
            }
            Ok(Page {
                items: vec![Repo {
                    owner: "octocat".into(),
                    name: "hello".into(),
                    url: Some("https://github.com/octocat/hello".into()),
                    default_branch: Some("main".into()),
                    created_at: None,
                    updated_at: None,
                }],
                next_cursor: None,
            })
        }
        async fn search_repos(&self, _: &str, page: PageParams) -> ScResult<Page<Repo>> {
            self.list_repos(page).await
        }
        async fn get_repo(&self, owner: &str, name: &str) -> ScResult<Repo> {
            if owner == "ghost" {
                return Err(ScError::NotFound("no such repo".into()));
            }
            Ok(Repo {
                owner: owner.into(),
                name: name.into(),
                url: Some(format!("https://github.com/{owner}/{name}")),
                default_branch: Some("main".into()),
                created_at: None,
                updated_at: None,
            })
        }
        async fn list_remote_branches(
            &self,
            _: &str,
            _: &str,
            _: PageParams,
        ) -> ScResult<Page<Branch>> {
            Ok(Page {
                items: vec![
                    Branch {
                        name: "main".into(),
                        commit_sha: None,
                        protected: false,
                    },
                    Branch {
                        name: "dev".into(),
                        commit_sha: None,
                        protected: false,
                    },
                ],
                next_cursor: None,
            })
        }
        async fn create_pr(&self, _: &RepoRef, input: NewPullRequest) -> ScResult<PullRequest> {
            Ok(PullRequest {
                number: 7,
                url: "https://github.com/o/r/pull/7".into(),
                title: input.title,
                body: input.body,
                state: PrState::Open,
                draft: input.draft,
                source_branch: input.source_branch,
                target_branch: input.target_branch,
                author: "octocat".into(),
                mergeable: Some(true),
                mergeable_state: Some("clean".into()),
                head_sha: Some("deadbeef".into()),
                created_at: String::new(),
                updated_at: String::new(),
            })
        }
        async fn get_pr(&self, _: &RepoRef, _: u64) -> ScResult<PullRequest> {
            // Signal that the one pre-loop SQLite read is done (see
            // `run_wait_for_changes`); harmless for non-poll callers.
            self.first_get_pr.notify_one();
            let mut pr = sample_pr();
            if self.merged_linked {
                pr.state = PrState::Merged;
            }
            if self.mutate_head {
                let n = self.head_seq.fetch_add(1, Ordering::SeqCst);
                pr.head_sha = Some(format!("sha{n}"));
            }
            Ok(pr)
        }
        async fn list_prs(&self, _: &RepoRef, _: PrQuery) -> ScResult<Page<PullRequest>> {
            if self.fail_list_prs {
                return Err(intent_sourcecontrol::Error::Api("list_prs down".into()));
            }
            let mut items = if self.discover {
                vec![sample_pr()]
            } else {
                vec![]
            };
            if let Some(n) = self.open_pr_number {
                let mut pr = sample_pr();
                pr.number = n;
                pr.url = format!("https://github.com/o/r/pull/{n}");
                items.push(pr);
            }
            Ok(Page {
                items,
                next_cursor: None,
            })
        }
        async fn update_pr(&self, _: &RepoRef, _: u64, _: PrPatch) -> ScResult<PullRequest> {
            unimplemented!()
        }
        async fn merge_pr(
            &self,
            _: &RepoRef,
            _: u64,
            method: MergeMethod,
            _: MergeOptions,
        ) -> ScResult<MergeOutcome> {
            Ok(MergeOutcome {
                merged: true,
                message: format!("Merged via {method:?}"),
                sha: Some("mergedsha".into()),
            })
        }
        async fn mergeability(&self, _: &RepoRef, _: u64) -> ScResult<Mergeability> {
            unimplemented!()
        }
        async fn update_branch(&self, _: &RepoRef, _: u64) -> ScResult<()> {
            Ok(())
        }
        async fn submit_review(
            &self,
            _: &RepoRef,
            _: u64,
            verdict: ReviewVerdict,
            body: Option<String>,
        ) -> ScResult<Review> {
            Ok(Review {
                author: "octocat".into(),
                verdict,
                body,
                submitted_at: "2026-06-17T05:00:00.000Z".into(),
            })
        }
        async fn list_reviews(&self, _: &RepoRef, _: u64) -> ScResult<Vec<Review>> {
            Ok(vec![
                Review {
                    author: "alice".into(),
                    verdict: ReviewVerdict::Approve,
                    body: None,
                    submitted_at: "2026-01-01".into(),
                },
                Review {
                    author: "bob".into(),
                    verdict: ReviewVerdict::Comment,
                    body: None,
                    submitted_at: "2026-01-02".into(),
                },
            ])
        }
        async fn list_comments(&self, _: &RepoRef, _: u64) -> ScResult<Vec<Comment>> {
            Ok(vec![Comment {
                id: "1".into(),
                author: "u".into(),
                body: "hi".into(),
                path: None,
                line: None,
                created_at: "2026".into(),
                url: None,
            }])
        }
        async fn add_comment(
            &self,
            _: &RepoRef,
            _: u64,
            body: &str,
            _: Option<CommentAnchor>,
        ) -> ScResult<Comment> {
            Ok(Comment {
                id: "777".into(),
                author: "octocat".into(),
                body: body.to_string(),
                path: None,
                line: None,
                created_at: "2026".into(),
                url: Some("https://github.com/o/r/pull/42#issuecomment-777".into()),
            })
        }
        async fn list_review_comments(
            &self,
            _: &RepoRef,
            _: u64,
            _: PageParams,
        ) -> ScResult<Page<ReviewComment>> {
            Ok(Page {
                items: vec![ReviewComment {
                    id: 5,
                    body: "nit".into(),
                    path: "a.rs".into(),
                    line: Some(1),
                    author: "rev".into(),
                    created_at: "2026".into(),
                    updated_at: "2026".into(),
                    in_reply_to_id: None,
                    url: "url".into(),
                }],
                next_cursor: None,
            })
        }
        async fn reply_to_review_comment(
            &self,
            _: &RepoRef,
            _: u64,
            comment_id: u64,
            body: &str,
        ) -> ScResult<ReviewComment> {
            Ok(ReviewComment {
                id: comment_id + 1,
                body: body.to_string(),
                path: "a.rs".into(),
                line: Some(1),
                author: "octocat".into(),
                created_at: "2026".into(),
                updated_at: "2026".into(),
                in_reply_to_id: Some(comment_id),
                url: "https://github.com/o/r/pull/42#discussion_r999".into(),
            })
        }
        async fn get_review_threads(
            &self,
            _: &RepoRef,
            _: u64,
            _: PageParams,
        ) -> ScResult<Page<ReviewThread>> {
            if self.fail_threads {
                return Err(ScError::Api("graphql down".into()));
            }
            Ok(Page {
                items: vec![
                    ReviewThread {
                        id: "RT1".into(),
                        is_resolved: false,
                        comments: vec![ReviewThreadComment {
                            id: "c1".into(),
                            body: "x".into(),
                            author: "rev".into(),
                            path: "a.rs".into(),
                            line: Some(1),
                            created_at: "2026".into(),
                        }],
                    },
                    ReviewThread {
                        id: "RT2".into(),
                        is_resolved: true,
                        comments: vec![ReviewThreadComment {
                            id: "c2".into(),
                            body: "y".into(),
                            author: "rev".into(),
                            path: "b.rs".into(),
                            line: Some(2),
                            created_at: "2026".into(),
                        }],
                    },
                ],
                next_cursor: None,
            })
        }
        async fn resolve_thread(&self, _: &str) -> ScResult<bool> {
            Ok(true)
        }
        async fn unresolve_thread(&self, _: &str) -> ScResult<bool> {
            Ok(false)
        }
        async fn check_runs(&self, _: &RepoRef, _: &str) -> ScResult<Vec<CheckRun>> {
            Ok(vec![
                CheckRun {
                    name: "build".into(),
                    state: CheckState::Success,
                    url: None,
                },
                CheckRun {
                    name: "test".into(),
                    state: CheckState::Failure,
                    url: None,
                },
                CheckRun {
                    name: "lint".into(),
                    state: CheckState::Pending,
                    url: None,
                },
            ])
        }
        async fn create_issue(&self, _: &RepoRef, _: &str, _: Option<&str>) -> ScResult<Issue> {
            unimplemented!()
        }
        async fn get_issue(&self, _: &RepoRef, _: u64) -> ScResult<Issue> {
            unimplemented!()
        }
        async fn list_issues(&self, _: &RepoRef, _: IssueQuery) -> ScResult<Page<Issue>> {
            Ok(Page {
                items: vec![Issue {
                    number: 11,
                    title: "Bug report".into(),
                    body: Some("something broke".into()),
                    state: "open".into(),
                    url: "https://github.com/o/r/issues/11".into(),
                }],
                next_cursor: None,
            })
        }
    }

    async fn setup(fail_threads: bool, with_pr: bool) -> (TempDb, Services, WorkspaceId) {
        setup_with(
            StubForge {
                fail_threads,
                ..Default::default()
            },
            with_pr,
        )
        .await
    }

    async fn setup_with(forge: StubForge, with_pr: bool) -> (TempDb, Services, WorkspaceId) {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws_id = WorkspaceId::new();
        let mut ws = workspace(&ws_id);
        ws.updated_at = now_iso();
        if with_pr {
            ws.repository_owner = Some("o".into());
            ws.repository_name = Some("r".into());
            ws.pr_number = Some(42);
        }
        store.insert_workspace(&ws).await.expect("ws");
        let services = Services::new(store).with_source_control(Arc::new(forge));
        (tmp, services, ws_id)
    }

    // ---- github.* browse / auth / identity (PROTOCOL §5.27) -------------

    async fn github_svc() -> (TempDb, Services) {
        let (tmp, svc, _ws) = setup_with(StubForge::default(), false).await;
        (tmp, svc)
    }

    #[tokio::test]
    async fn github_repos_list_projects_html_url_and_null_next_token() {
        let (_t, svc) = github_svc().await;
        let v = svc.github_repos_list(None, None).await.expect("list");
        assert_eq!(v["nextToken"], serde_json::Value::Null);
        let repo = &v["repos"][0];
        assert_eq!(repo["owner"], "octocat");
        assert_eq!(repo["name"], "hello");
        assert_eq!(repo["htmlUrl"], "https://github.com/octocat/hello");
        assert_eq!(repo["defaultBranch"], "main");
        // engine `url` is projected, never echoed verbatim.
        assert!(repo.get("url").is_none());
    }

    #[tokio::test]
    async fn github_repos_search_shapes_like_list() {
        let (_t, svc) = github_svc().await;
        let v = svc
            .github_repos_search("hello".into(), None, None)
            .await
            .expect("search");
        assert_eq!(v["repos"][0]["htmlUrl"], "https://github.com/octocat/hello");
        assert_eq!(v["nextToken"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn github_repos_list_paginates_via_opaque_next_token() {
        // §5.5: the first page returns a non-null opaque `nextToken`; echoing it
        // back fetches the next (last) page, which ends with a null `nextToken`.
        let (_t, svc, _ws) = setup_with(
            StubForge {
                paginate: true,
                ..Default::default()
            },
            false,
        )
        .await;

        let page1 = svc.github_repos_list(None, None).await.expect("page 1");
        assert_eq!(page1["repos"][0]["name"], "repo1");
        let token = page1["nextToken"]
            .as_str()
            .expect("page 1 has a next token");
        assert!(!token.is_empty());
        // The wire token is opaque — never the raw engine cursor ("2").
        assert_ne!(token, "2");

        let page2 = svc
            .github_repos_list(None, Some(token.to_string()))
            .await
            .expect("page 2");
        assert_eq!(page2["repos"][0]["name"], "repo2");
        assert_eq!(page2["nextToken"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn github_repos_get_returns_repo_or_null_when_missing() {
        let (_t, svc) = github_svc().await;
        let found = svc
            .github_repos_get("octocat".into(), "hello".into())
            .await
            .expect("get");
        assert_eq!(found["repo"]["htmlUrl"], "https://github.com/octocat/hello");

        // A NotFound from the engine surfaces as `{ repo: null }` (FE parity).
        let missing = svc
            .github_repos_get("ghost".into(), "nope".into())
            .await
            .expect("get missing");
        assert_eq!(missing["repo"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn github_branches_list_returns_names_and_null_next_token() {
        let (_t, svc) = github_svc().await;
        let v = svc
            .github_branches_list("octocat".into(), "hello".into(), None, None)
            .await
            .expect("branches");
        assert_eq!(v["branches"], serde_json::json!(["main", "dev"]));
        assert_eq!(v["nextToken"], serde_json::Value::Null);
    }

    #[tokio::test]
    async fn github_get_user_drops_id_name_and_never_leaks_token() {
        let (_t, svc) = github_svc().await;
        let v = svc.github_get_user().await.expect("user");
        let user = &v["user"];
        assert_eq!(user["login"], "octocat");
        assert_eq!(user["avatarUrl"], "https://avatars.example/u/1");
        assert_eq!(user["htmlUrl"], "https://github.com/octocat");
        assert!(user.get("id").is_none());
        assert!(user.get("name").is_none());
    }

    #[tokio::test]
    async fn github_auth_status_reports_configured_with_valid_token() {
        let (_t, svc) = github_svc().await;
        let v = svc.github_auth_status().await.expect("auth");
        assert_eq!(v["isConfigured"], true);
        assert_eq!(v["oauthUrl"], "");
        assert_eq!(v["configuredButNeedsUpdate"], false);
        assert_eq!(v["updatedScopes"], "");
    }

    #[tokio::test]
    async fn github_connect_and_revoke_are_noops_with_guidance() {
        let (_t, svc) = github_svc().await;
        let c = svc.github_connect().await.expect("connect");
        assert_eq!(c["ok"], false);
        assert!(c["guidance"].as_str().unwrap().contains("GITHUB_TOKEN"));
        let r = svc.github_revoke().await.expect("revoke");
        assert_eq!(r["ok"], false);
        assert!(!r["guidance"].as_str().unwrap().is_empty());
    }

    /// Drive `pr_wait_for_changes` deterministically under a paused clock.
    ///
    /// `pr_wait_for_changes` performs exactly one SQLite read (`load_ws_for_pr`)
    /// before it captures the `initial` PR snapshot (the stub's first `get_pr`)
    /// and enters its poll loop. That read crosses to sqlx's SQLite worker
    /// thread; if the tokio clock is already paused the runtime's idle
    /// auto-advance can fire the pool's `acquire_timeout` timer before the real
    /// worker-thread reply lands, intermittently surfacing a spurious
    /// `PoolTimedOut` (the pre-existing CI flake).
    ///
    /// So we run the call in a task while the clock still flows, wait on the
    /// stub's first-`get_pr` signal (which fires immediately after that DB read),
    /// and only then pause: by that point the read is provably done and the only
    /// remaining timers are the poll-loop sleeps, which auto-advance instantly as
    /// the tests intend. TEST-ONLY — no production change.
    async fn run_wait_for_changes(
        forge: StubForge,
        timeout: Option<i64>,
        poll: Option<i64>,
        watch: &str,
    ) -> serde_json::Value {
        let ready = forge.first_get_pr.clone();
        let (_t, svc, ws) = setup_with(forge, true).await;
        let watch = watch.to_string();
        let task = tokio::spawn(async move {
            svc.pr_wait_for_changes(ws, timeout, poll, Some(watch))
                .await
        });
        // The DB read finishes in real time; the stub signals once it is past it.
        ready.notified().await;
        tokio::time::pause();
        // `_t` (the temp DB) must outlive the task.
        let result = task.await.expect("join wait_for_changes task");
        drop(_t);
        result.expect("wait")
    }

    #[tokio::test]
    async fn status_shape_is_parity_exact() {
        let (_t, svc, ws) = setup(false, true).await;
        let v = svc.pr_status(ws).await.expect("status");
        assert_eq!(v["prNumber"], 42);
        assert_eq!(v["title"], "Add thing");
        assert_eq!(v["state"], "open");
        assert_eq!(v["mergeable"], true);
        assert_eq!(v["mergeableState"], "clean");
        assert_eq!(v["hasConflicts"], false);
        assert_eq!(v["isDraft"], false);
        assert_eq!(v["summary"], "✅ PR is mergeable with no conflicts.");
    }

    #[tokio::test]
    async fn get_reviews_aggregates_and_serializes_verdicts() {
        let (_t, svc, ws) = setup(false, true).await;
        let v = svc.pr_get_reviews(ws, None).await.expect("reviews");
        assert_eq!(v["reviewDecision"], "APPROVED");
        assert_eq!(v["approvalCount"], 1);
        assert_eq!(v["changesRequestedCount"], 0);
        assert_eq!(v["approvedBy"][0], "alice");
        assert_eq!(v["reviews"].as_array().unwrap().len(), 2);
        assert_eq!(v["reviews"][0]["verdict"], "approve");
        assert_eq!(v["reviews"][1]["verdict"], "comment");
    }

    #[tokio::test]
    async fn list_check_runs_tallies() {
        let (_t, svc, ws) = setup(false, true).await;
        let v = svc.pr_list_check_runs(ws, None).await.expect("checks");
        assert_eq!(v["total"], 3);
        assert_eq!(v["passed"], 1);
        assert_eq!(v["failed"], 1);
        assert_eq!(v["pending"], 1);
        assert_eq!(v["runs"].as_array().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn list_review_comments_filters_unresolved() {
        let (_t, svc, ws) = setup(false, true).await;
        let v = svc
            .pr_list_review_comments(ws, None, None)
            .await
            .expect("review comments");
        assert_eq!(v["usingFallback"], false);
        assert_eq!(v["threadCount"], 1);
        assert_eq!(v["threads"][0]["id"], "RT1");
        assert_eq!(v["threads"][0]["comments"][0]["author"]["login"], "rev");
        assert_eq!(v["filter"]["status"], "unresolved");
    }

    #[tokio::test]
    async fn list_review_comments_falls_back_to_rest() {
        let (_t, svc, ws) = setup(true, true).await;
        let v = svc
            .pr_list_review_comments(ws, None, None)
            .await
            .expect("review comments");
        assert_eq!(v["usingFallback"], true);
        assert_eq!(v["threadCount"], 1);
        assert_eq!(v["threads"][0]["id"], "rest-thread-5");
        assert!(v["note"].is_string());
    }

    #[tokio::test]
    async fn list_comments_returns_count() {
        let (_t, svc, ws) = setup(false, true).await;
        let v = svc.pr_list_comments(ws, Some(5)).await.expect("comments");
        assert_eq!(v["count"], 1);
        assert_eq!(v["comments"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn no_active_pr_is_internal_error() {
        let (_t, svc, ws) = setup(false, false).await;
        let err = svc.pr_status(ws).await.unwrap_err();
        assert!(matches!(err, Error::Internal(m) if m == "No active PR"));
    }

    #[tokio::test]
    async fn merge_returns_parity_shape() {
        let (_t, svc, ws) = setup(false, true).await;
        let v = svc
            .pr_merge(ws, Some("squash".into()), None, None, None)
            .await
            .expect("merge");
        assert_eq!(v["merged"], true);
        assert_eq!(v["sha"], "mergedsha");
        assert_eq!(v["mergeMethod"], "squash");
        assert_eq!(v["prNumber"], 42);
    }

    #[tokio::test]
    async fn merge_rejects_invalid_method() {
        let (_t, svc, ws) = setup(false, true).await;
        let err = svc
            .pr_merge(ws, Some("ff".into()), None, None, None)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Internal(m) if m.contains("mergeMethod must be one of")));
    }

    #[tokio::test]
    async fn merge_requires_active_pr() {
        let (_t, svc, ws) = setup(false, false).await;
        let err = svc.pr_merge(ws, None, None, None, None).await.unwrap_err();
        assert!(matches!(err, Error::Internal(m) if m == "No active PR"));
    }

    #[tokio::test]
    async fn post_comment_and_reply_surface_html_url() {
        let (_t, svc, ws) = setup(false, true).await;
        let v = svc
            .pr_post_comment(ws.clone(), "ship it".into())
            .await
            .expect("post");
        assert_eq!(v["id"], "777");
        assert!(v["htmlUrl"].as_str().unwrap().contains("issuecomment-777"));

        let r = svc
            .pr_reply_to_review_comment(ws, 5, "agreed".into())
            .await
            .expect("reply");
        assert_eq!(r["id"], 6);
        assert!(r["htmlUrl"].as_str().unwrap().contains("discussion_r999"));
    }

    #[tokio::test]
    async fn resolve_thread_reports_action() {
        let (_t, svc, ws) = setup(false, true).await;
        let v = svc
            .pr_resolve_thread(ws, "RT1".into(), Some("resolve".into()))
            .await
            .expect("resolve");
        assert_eq!(v["ok"], true);
        assert_eq!(v["threadId"], "RT1");
        assert_eq!(v["action"], "resolve");
    }

    #[tokio::test]
    async fn unresolve_thread_failure_is_internal_error() {
        // The stub `unresolve_thread` returns `false` (not resolved), which the
        // service treats as a silent failure.
        let (_t, svc, ws) = setup(false, true).await;
        let err = svc
            .pr_resolve_thread(ws, "RT1".into(), Some("unresolve".into()))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Internal(m) if m.contains("Failed to unresolve thread")));
    }

    #[tokio::test]
    async fn create_review_returns_review() {
        let (_t, svc, ws) = setup(false, true).await;
        let v = svc
            .pr_create_review(ws, "approve".into(), Some("LGTM".into()))
            .await
            .expect("review");
        assert_eq!(v["review"]["verdict"], "approve");
        assert_eq!(v["review"]["body"], "LGTM");
    }

    #[tokio::test]
    async fn update_branch_reports_success() {
        let (_t, svc, ws) = setup(false, true).await;
        let v = svc.pr_update_branch(ws).await.expect("update branch");
        assert_eq!(v["method"], "merge");
        assert_eq!(v["alreadyUpToDate"], false);
    }

    #[tokio::test]
    async fn wait_for_changes_detects_new_commit() {
        let v = run_wait_for_changes(
            StubForge {
                mutate_head: true,
                ..Default::default()
            },
            Some(30),
            Some(10),
            "commits",
        )
        .await;
        assert_eq!(v["changed"], true);
        assert!(v["changes"][0].as_str().unwrap().starts_with("New commit:"));
        assert!(v["iterations"].as_u64().unwrap() >= 1);
    }

    #[tokio::test]
    async fn wait_for_changes_times_out_without_changes() {
        let v = run_wait_for_changes(StubForge::default(), Some(30), Some(10), "any").await;
        assert_eq!(v["changed"], false);
        assert!(v["summary"].as_str().unwrap().contains("Timeout reached"));
    }

    #[tokio::test]
    async fn wait_for_changes_rejects_invalid_watch() {
        let (_t, svc, ws) = setup(false, true).await;
        let err = svc
            .pr_wait_for_changes(ws, None, None, Some("bogus".into()))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Internal(m) if m.contains("watch must be one of")));
    }

    // ------------------------------------------------------------------------
    // PR ↔ workspace refresh (§7.6): head.ref matching, persisted snapshot, and
    // `pr:*` event emission against the stubbed forge (no network).
    // ------------------------------------------------------------------------

    /// Build a bus-wired service plus a seeded workspace for refresh tests. The
    /// returned bus persists `pr:*` events to the durable log we assert on.
    async fn refresh_setup(
        forge: StubForge,
        branch: &str,
        pr_number: Option<u64>,
        is_remote: bool,
    ) -> (TempDb, Services, WorkspaceId) {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws_id = WorkspaceId::new();
        let mut ws = workspace(&ws_id);
        ws.branch = branch.to_string();
        ws.repository_owner = Some("o".into());
        ws.repository_name = Some("r".into());
        ws.pr_number = pr_number;
        ws.is_remote = is_remote;
        ws.updated_at = now_iso();
        store.insert_workspace(&ws).await.expect("ws");
        let bus = crate::EventBus::new(store.clone());
        let svc = Services::new(store)
            .with_event_bus(bus)
            .with_source_control(Arc::new(forge));
        (tmp, svc, ws_id)
    }

    #[tokio::test]
    async fn refresh_emits_pr_updated_and_is_idempotent() {
        // Linked PR #42; ws branch matches the PR head ("feature").
        let (_t, svc, ws_id) =
            refresh_setup(StubForge::default(), "feature", Some(42), false).await;
        let outcome = svc.refresh_workspace_pr(&ws_id).await.expect("refresh");
        assert_eq!(outcome, crate::PrRefreshOutcome::Updated);

        let after = svc.store().get_workspace(&ws_id).await.unwrap();
        assert_eq!(after.pr_status, Some(intent_core::PullRequestStatus::Open));
        let info = after.active_pull_request.as_ref().expect("snapshot");
        assert_eq!(info.number, 42);
        assert_eq!(info.head_ref.as_deref(), Some("feature"));

        // The daemon-owned PR list is upserted on every linked refresh.
        let list = after.pull_requests.as_ref().expect("pull_requests");
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].number, 42);

        let evs = svc
            .store()
            .events_by_type(&ws_id, "pr:updated", 10)
            .await
            .unwrap();
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].data["prNumber"], 42);
        assert_eq!(evs[0].data["prStatus"], "Open");
        assert_eq!(evs[0].data["pullRequests"][0]["number"], 42);

        // Re-running with the same forge state is a no-op (no new event).
        let again = svc.refresh_workspace_pr(&ws_id).await.expect("refresh2");
        assert_eq!(again, crate::PrRefreshOutcome::Unchanged);
        let evs2 = svc
            .store()
            .events_by_type(&ws_id, "pr:updated", 10)
            .await
            .unwrap();
        assert_eq!(evs2.len(), 1);
    }

    #[tokio::test]
    async fn refresh_unlinks_on_positive_branch_mismatch() {
        // Linked PR #42 but ws branch "main" differs from PR head "feature".
        let (_t, svc, ws_id) = refresh_setup(StubForge::default(), "main", Some(42), false).await;
        let outcome = svc.refresh_workspace_pr(&ws_id).await.expect("refresh");
        assert_eq!(outcome, crate::PrRefreshOutcome::Unlinked);

        let after = svc.store().get_workspace(&ws_id).await.unwrap();
        assert_eq!(after.pr_number, None);
        assert_eq!(after.pr_url, None);
        assert_eq!(after.pr_status, None);
        assert!(after.active_pull_request.is_none());

        let evs = svc
            .store()
            .events_by_type(&ws_id, "pr:unlinked", 10)
            .await
            .unwrap();
        assert_eq!(evs.len(), 1);
    }

    #[tokio::test]
    async fn refresh_links_via_head_ref_discovery() {
        // Unlinked ws; discovery returns a PR whose head ref equals the branch.
        let forge = StubForge {
            discover: true,
            ..Default::default()
        };
        let (_t, svc, ws_id) = refresh_setup(forge, "feature", None, false).await;
        let outcome = svc.refresh_workspace_pr(&ws_id).await.expect("refresh");
        assert_eq!(outcome, crate::PrRefreshOutcome::Linked);

        let after = svc.store().get_workspace(&ws_id).await.unwrap();
        assert_eq!(after.pr_number, Some(42));
        assert_eq!(after.pr_status, Some(intent_core::PullRequestStatus::Open));
        // Discovery also upserts into the daemon-owned PR list.
        let list = after.pull_requests.as_ref().expect("pull_requests");
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].number, 42);

        let evs = svc
            .store()
            .events_by_type(&ws_id, "pr:linked", 10)
            .await
            .unwrap();
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].data["prNumber"], 42);
        assert_eq!(evs[0].data["prUrl"], "https://github.com/o/r/pull/42");
        assert_eq!(evs[0].data["pullRequests"][0]["number"], 42);
    }

    #[tokio::test]
    async fn refresh_relinks_merged_pr_to_newer_open_pr() {
        // Regression: a linked PR fetched as merged (head ref still equals the
        // workspace branch) used to stay linked forever, so a newer open PR on
        // the same branch was never discovered. Now the merged PR is kept in
        // `pull_requests`, and the refresh relinks to the newer open PR.
        let forge = StubForge {
            merged_linked: true,
            open_pr_number: Some(300),
            ..Default::default()
        };
        let (_t, svc, ws_id) = refresh_setup(forge, "feature", Some(42), false).await;
        let outcome = svc.refresh_workspace_pr(&ws_id).await.expect("refresh");
        assert_eq!(outcome, crate::PrRefreshOutcome::Linked);

        let after = svc.store().get_workspace(&ws_id).await.unwrap();
        assert_eq!(after.pr_number, Some(300));
        assert_eq!(
            after.pr_url.as_deref(),
            Some("https://github.com/o/r/pull/300")
        );
        assert_eq!(after.pr_status, Some(intent_core::PullRequestStatus::Open));
        assert_eq!(after.active_pull_request.as_ref().unwrap().number, 300);

        // Both the merged historical PR and the new open PR are recorded.
        let list = after.pull_requests.as_ref().expect("pull_requests");
        assert_eq!(list.len(), 2);
        let merged = list.iter().find(|p| p.number == 42).expect("merged PR");
        assert_eq!(merged.status, intent_core::PullRequestStatus::Merged);
        let open = list.iter().find(|p| p.number == 300).expect("open PR");
        assert_eq!(open.status, intent_core::PullRequestStatus::Open);

        // `pr:linked` was emitted with the full list in the payload.
        let evs = svc
            .store()
            .events_by_type(&ws_id, "pr:linked", 10)
            .await
            .unwrap();
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].data["prNumber"], 300);
        assert_eq!(evs[0].data["pullRequests"].as_array().unwrap().len(), 2);

        // Re-running never relinks to the already-linked PR: the stub still
        // reports the linked PR as merged and offers only #300 in discovery,
        // and the self-exclusion guard (`p.number != number`) prevents a
        // relink loop back onto the same number.
        let again = svc.refresh_workspace_pr(&ws_id).await.expect("refresh2");
        assert_ne!(again, crate::PrRefreshOutcome::Linked);
    }

    #[tokio::test]
    async fn refresh_merged_pr_without_successor_stays_linked() {
        // A merged linked PR with no newer open PR on the branch: no relink,
        // the status delta persists (Merged) and the PR lands in the list.
        let forge = StubForge {
            merged_linked: true,
            ..Default::default()
        };
        let (_t, svc, ws_id) = refresh_setup(forge, "feature", Some(42), false).await;
        let outcome = svc.refresh_workspace_pr(&ws_id).await.expect("refresh");
        assert_eq!(outcome, crate::PrRefreshOutcome::Updated);

        let after = svc.store().get_workspace(&ws_id).await.unwrap();
        assert_eq!(after.pr_number, Some(42));
        assert_eq!(
            after.pr_status,
            Some(intent_core::PullRequestStatus::Merged)
        );
        let list = after.pull_requests.as_ref().expect("pull_requests");
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].number, 42);
        assert_eq!(list[0].status, intent_core::PullRequestStatus::Merged);

        let evs = svc
            .store()
            .events_by_type(&ws_id, "pr:updated", 10)
            .await
            .unwrap();
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].data["prStatus"], "Merged");
        assert_eq!(evs[0].data["pullRequests"][0]["status"], "Merged");
    }

    #[tokio::test]
    async fn refresh_merged_pr_persists_status_when_discovery_fails() {
        // A transient `list_prs` failure during relink discovery must not
        // discard the merged-status delta: the refresh degrades to the plain
        // update path, persists Merged, and the next sweep retries discovery.
        let forge = StubForge {
            merged_linked: true,
            fail_list_prs: true,
            ..Default::default()
        };
        let (_t, svc, ws_id) = refresh_setup(forge, "feature", Some(42), false).await;
        let outcome = svc.refresh_workspace_pr(&ws_id).await.expect("refresh");
        assert_eq!(outcome, crate::PrRefreshOutcome::Updated);

        let after = svc.store().get_workspace(&ws_id).await.unwrap();
        assert_eq!(after.pr_number, Some(42));
        assert_eq!(
            after.pr_status,
            Some(intent_core::PullRequestStatus::Merged)
        );
        let list = after.pull_requests.as_ref().expect("pull_requests");
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].status, intent_core::PullRequestStatus::Merged);
    }

    #[tokio::test]
    async fn refresh_skips_remote_workspace() {
        // Remote workspaces are not refreshed (no forge call, no event).
        let (_t, svc, ws_id) = refresh_setup(StubForge::default(), "feature", Some(42), true).await;
        let outcome = svc.refresh_workspace_pr(&ws_id).await.expect("refresh");
        assert_eq!(outcome, crate::PrRefreshOutcome::Skipped);
        let evs = svc.store().events_by_workspace(&ws_id, 10).await.unwrap();
        assert!(evs.is_empty());
    }

    // ------------------------------------------------------------------------
    // `pr.refresh` (PROTOCOL §5.7 extension): the on-demand RPC wraps
    // `refresh_workspace_pr` and reports the post-refresh linkage state; the
    // `pr:*` events flow through the existing refresh path only.
    // ------------------------------------------------------------------------

    #[tokio::test]
    async fn pr_refresh_reports_linked_state_after_discovery() {
        // Unlinked ws; discovery links PR #42 → outcome "linked" plus the
        // post-refresh linkage fields.
        let forge = StubForge {
            discover: true,
            ..Default::default()
        };
        let (_t, svc, ws_id) = refresh_setup(forge, "feature", None, false).await;
        let v = svc.pr_refresh(ws_id.clone()).await.expect("pr.refresh");
        assert_eq!(v["outcome"], "linked");
        assert_eq!(v["prNumber"], 42);
        assert_eq!(v["prUrl"], "https://github.com/o/r/pull/42");
        assert_eq!(v["prStatus"], "Open");
        assert_eq!(v["pullRequests"][0]["number"], 42);

        // Exactly one `pr:linked` event, emitted by the shared refresh path.
        let evs = svc
            .store()
            .events_by_type(&ws_id, "pr:linked", 10)
            .await
            .unwrap();
        assert_eq!(evs.len(), 1);
    }

    #[tokio::test]
    async fn pr_refresh_reports_unlinked_state_after_stale_link_cleared() {
        // Linked PR #42 but ws branch "main" differs from PR head "feature" →
        // the stale link is cleared and the RPC reports the cleared state.
        let (_t, svc, ws_id) = refresh_setup(StubForge::default(), "main", Some(42), false).await;
        let v = svc.pr_refresh(ws_id.clone()).await.expect("pr.refresh");
        assert_eq!(v["outcome"], "unlinked");
        assert!(v["prNumber"].is_null());
        assert!(v["prUrl"].is_null());
        assert!(v["prStatus"].is_null());
        // Always an array on the wire, even when empty.
        assert!(v["pullRequests"].as_array().is_some());

        let evs = svc
            .store()
            .events_by_type(&ws_id, "pr:unlinked", 10)
            .await
            .unwrap();
        assert_eq!(evs.len(), 1);
    }

    #[tokio::test]
    async fn pr_refresh_skips_ineligible_workspace_without_error() {
        // Remote workspace: outcome "skipped", no forge call, no event.
        let (_t, svc, ws_id) = refresh_setup(StubForge::default(), "feature", Some(42), true).await;
        let v = svc.pr_refresh(ws_id.clone()).await.expect("pr.refresh");
        assert_eq!(v["outcome"], "skipped");
        let evs = svc.store().events_by_workspace(&ws_id, 10).await.unwrap();
        assert!(evs.is_empty());
    }

    #[tokio::test]
    async fn refresh_all_discovers_unlinked_workspaces() {
        // STAB-3 regression test: `refresh_all_workspace_prs()` (the background
        // 60s loop) should discover and link PRs for unlinked workspaces, not just
        // refresh already-linked ones. Without the fix, the background loop
        // skipped workspaces with `pr_number.is_none()`, so discovery was
        // on-demand only. This test asserts that an unlinked workspace on branch X
        // with an open PR whose head ref is X gets linked when
        // `refresh_all_workspace_prs()` runs (simulating the background loop).
        let forge = StubForge {
            discover: true,
            ..Default::default()
        };
        let (_t, svc, ws_id) = refresh_setup(forge, "feature", None, false).await;

        // Before the sweep the workspace is unlinked (no pr_number).
        let before = svc.store().get_workspace(&ws_id).await.unwrap();
        assert_eq!(before.pr_number, None);

        // Run the same sweep the background loop runs.
        svc.refresh_all_workspace_prs().await;

        // After the sweep the workspace is linked to PR #42.
        let after = svc.store().get_workspace(&ws_id).await.unwrap();
        assert_eq!(after.pr_number, Some(42));
        assert_eq!(after.pr_status, Some(intent_core::PullRequestStatus::Open));

        // A `pr:linked` event was emitted.
        let evs = svc
            .store()
            .events_by_type(&ws_id, "pr:linked", 10)
            .await
            .unwrap();
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].data["prNumber"], 42);
    }

    // ------------------------------------------------------------------------
    // accept-changes.* orchestration (§5.18): the commit→push→create-PR
    // pipeline against a real worktree + local bare remote, then mergePR via the
    // stubbed forge.
    // ------------------------------------------------------------------------

    /// Drop guard removing a temp directory tree.
    struct TempDir(PathBuf);
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn unique_dir(prefix: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    /// Commit everything in the worktree on the current branch, returning the oid.
    fn commit_all(repo: &git2::Repository, msg: &str) -> git2::Oid {
        let mut index = repo.index().unwrap();
        index
            .add_all(["*"].iter(), git2::IndexAddOption::DEFAULT, None)
            .unwrap();
        index.write().unwrap();
        let tree = repo.find_tree(index.write_tree().unwrap()).unwrap();
        let sig = git2::Signature::now("Tester", "t@e.dev").unwrap();
        let parent = repo
            .head()
            .ok()
            .and_then(|h| h.target())
            .and_then(|o| repo.find_commit(o).ok());
        let parents: Vec<&git2::Commit> = parent.iter().collect();
        repo.commit(Some("HEAD"), &sig, &sig, msg, &tree, &parents)
            .unwrap()
    }

    /// Build a store + a workspace whose worktree is a real git repo on branch
    /// `feature` with an `origin` bare remote and a linked `o/r` repository.
    async fn ac_setup(
        forge: StubForge,
    ) -> (TempDb, TempDir, TempDir, Services, WorkspaceId, PathBuf) {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");

        let work = unique_dir("intentd-ac-work");
        let bare = unique_dir("intentd-ac-bare");
        let mut opts = git2::RepositoryInitOptions::new();
        opts.initial_head("feature");
        let repo = git2::Repository::init_opts(&work, &opts).unwrap();
        {
            let mut cfg = repo.config().unwrap();
            cfg.set_str("user.name", "Tester").unwrap();
            cfg.set_str("user.email", "t@e.dev").unwrap();
        }
        std::fs::write(work.join("README.md"), "init\n").unwrap();
        commit_all(&repo, "chore: init");
        git2::Repository::init_bare(&bare).unwrap();
        repo.remote("origin", bare.to_str().unwrap()).unwrap();

        let ws_id = WorkspaceId::new();
        let mut ws = workspace(&ws_id);
        ws.branch = "feature".into();
        ws.base_ref = Some("main".into());
        ws.repository_owner = Some("o".into());
        ws.repository_name = Some("r".into());
        ws.worktree_path = Some(work.to_string_lossy().to_string());
        store.insert_workspace(&ws).await.expect("ws");

        let svc = Services::new(store).with_source_control(Arc::new(forge));
        (tmp, TempDir(work.clone()), TempDir(bare), svc, ws_id, work)
    }

    #[tokio::test]
    async fn execute_runs_commit_push_create_pr_pipeline() {
        let (_t, _w, _b, svc, ws, work) = ac_setup(StubForge::default()).await;
        // An unstaged change for the commit step to capture.
        std::fs::write(work.join("feature.txt"), "hello\n").unwrap();

        let params = json!({
            "action": "commit",
            "commitMessage": "feat: add feature",
            "options": {
                "stageUnstaged": true,
                "pushAfterCommit": true,
                "createPRAfterPush": true,
            },
        });
        let res = svc
            .accept_changes_execute(ws.clone(), params)
            .await
            .expect("execute");

        assert_eq!(res["success"], true, "result: {res}");
        let steps = res["steps"].as_array().unwrap();
        assert_eq!(steps.len(), 3);
        assert_eq!(steps[0]["id"], "commit");
        assert_eq!(steps[0]["status"], "completed");
        assert_eq!(steps[1]["id"], "push");
        assert_eq!(steps[1]["status"], "completed");
        assert_eq!(steps[2]["id"], "create-pr");
        assert_eq!(steps[2]["status"], "completed");
        assert_eq!(res["result"]["commitHash"].as_str().unwrap().len(), 40);
        assert_eq!(res["result"]["prNumber"], 7);
        assert!(res["result"]["prUrl"].as_str().unwrap().contains("pull/7"));

        // Linkage persisted, including the daemon-owned PR list.
        let linked = svc.store().get_workspace(&ws).await.unwrap();
        assert_eq!(linked.pr_number, Some(7));
        let list = linked.pull_requests.as_ref().expect("pull_requests");
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].number, 7);

        // getStatus reflects the pushed branch + linked PR.
        let st = svc
            .accept_changes_get_status(ws.clone())
            .await
            .expect("status");
        assert_eq!(st["branch"], "feature");
        assert_eq!(st["hasRemote"], true);
        assert_eq!(st["isPushed"], true);
        assert_eq!(st["existingPR"]["number"], 7);

        // The bare remote now carries the feature branch.
        let bare_repo = git2::Repository::open_bare(_b.0.clone()).unwrap();
        assert!(bare_repo.find_reference("refs/heads/feature").is_ok());

        // mergePR via the stubbed forge.
        let mg = svc
            .accept_changes_merge_pr(ws.clone(), 7, Some("squash".into()), None, None)
            .await
            .expect("merge");
        assert_eq!(mg["success"], true);
        assert_eq!(mg["steps"][0]["id"], "merge");
        assert_eq!(mg["steps"][0]["status"], "completed");
        assert_eq!(mg["result"]["mergeCommitHash"], "mergedsha");
        let merged = svc.store().get_workspace(&ws).await.unwrap();
        assert_eq!(
            merged.pr_status,
            Some(intent_core::PullRequestStatus::Merged)
        );
        // The daemon-owned list mirrors the merged status so the pr:updated
        // payload is internally consistent (activePullRequest vs pullRequests).
        let list = merged.pull_requests.as_ref().expect("pull_requests");
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].number, 7);
        assert_eq!(list[0].status, intent_core::PullRequestStatus::Merged);
    }

    #[tokio::test]
    async fn execute_create_pr_without_remote_fails_step() {
        // A workspace with no git repo at all → push/create-pr cannot proceed.
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("store");
        let ws_id = WorkspaceId::new();
        let mut ws = workspace(&ws_id);
        ws.worktree_path = Some(unique_dir("intentd-ac-empty").to_string_lossy().to_string());
        store.insert_workspace(&ws).await.unwrap();
        let svc = Services::new(store).with_source_control(Arc::new(StubForge::default()));

        let res = svc
            .accept_changes_execute(ws_id, json!({ "action": "create-pr" }))
            .await
            .expect("execute");
        assert_eq!(res["success"], false);
        assert_eq!(res["steps"][0]["id"], "create-pr");
        assert_eq!(res["steps"][0]["status"], "failed");
    }

    #[tokio::test]
    async fn execute_rejects_unknown_action() {
        let (_t, _w, _b, svc, ws, _work) = ac_setup(StubForge::default()).await;
        let err = svc
            .accept_changes_execute(ws, json!({ "action": "teleport" }))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidParams(_)));
    }

    #[tokio::test]
    async fn prepare_lists_files_and_suggests_message() {
        let (_t, _w, _b, svc, ws, work) = ac_setup(StubForge::default()).await;
        std::fs::write(work.join("feature.txt"), "a\nb\n").unwrap();

        let p = svc
            .accept_changes_prepare(ws, "commit".into(), None)
            .await
            .expect("prepare");
        assert_eq!(p["valid"], true);
        assert!(p["filesCount"].as_u64().unwrap() >= 1);
        assert!(p["files"]
            .as_array()
            .unwrap()
            .iter()
            .any(|f| f["path"] == "feature.txt"));
        assert!(p["additions"].as_i64().unwrap() >= 2);
    }

    #[tokio::test]
    async fn add_remote_initializes_and_returns_status() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("store");
        let dir = TempDir(unique_dir("intentd-ac-addremote"));
        let ws_id = WorkspaceId::new();
        let mut ws = workspace(&ws_id);
        ws.branch = "feature".into();
        ws.worktree_path = Some(dir.0.to_string_lossy().to_string());
        store.insert_workspace(&ws).await.unwrap();
        let svc = Services::new(store).with_source_control(Arc::new(StubForge::default()));

        let st = svc
            .accept_changes_add_remote(ws_id, "https://github.com/o/r.git".into())
            .await
            .expect("add remote");
        assert_eq!(st["hasRemote"], true);
        assert_eq!(st["owner"], "o");
        assert_eq!(st["repo"], "r");

        let bad = svc
            .accept_changes_add_remote(WorkspaceId::new(), "not-a-url".into())
            .await;
        assert!(bad.is_err());
    }

    // -----------------------------------------------------------------------
    // `github.*` explicit-addressing surface (§5.27): owner/repo are passed
    // directly, so no workspace/PR linkage is needed (`setup_with(.., false)`).
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn github_pulls_create_preserves_head_verbatim_and_shapes_dto() {
        let (_t, svc, _ws) = setup_with(StubForge::default(), false).await;
        let v = svc
            .github_pulls_create(
                "octocat".into(),
                "hello".into(),
                "Add feature".into(),
                "desc".into(),
                "feature/x".into(),
                "main".into(),
                false,
            )
            .await
            .expect("create");
        let pull = &v["pull"];
        assert_eq!(pull["number"], 7);
        assert_eq!(pull["state"], "open");
        assert_eq!(pull["merged"], false);
        // `head` flows onto the PR head ref unmodified — no `owner:branch` prefix.
        assert_eq!(pull["headRef"], "feature/x");
        assert_eq!(pull["baseRef"], "main");
        assert_eq!(pull["user"]["login"], "octocat");
    }

    #[tokio::test]
    async fn github_pulls_get_list_search_shapes() {
        let (_t, svc, _ws) = setup_with(
            StubForge {
                discover: true,
                ..Default::default()
            },
            false,
        )
        .await;
        let g = svc
            .github_pulls_get("o".into(), "r".into(), 42)
            .await
            .unwrap();
        assert_eq!(g["pull"]["number"], 42);

        let l = svc
            .github_pulls_list(
                "o".into(),
                "r".into(),
                Some("open".into()),
                None,
                None,
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(l["pulls"][0]["number"], 42);
        assert!(l["nextToken"].is_null());

        let s = svc
            .github_pulls_search(
                "o".into(),
                "r".into(),
                Some("created".into()),
                Some("open".into()),
                Some(10),
                None,
            )
            .await
            .unwrap();
        assert_eq!(s["pulls"][0]["number"], 42);

        assert!(svc
            .github_pulls_search(
                "o".into(),
                "r".into(),
                Some("nope".into()),
                None,
                None,
                None
            )
            .await
            .is_err());
    }

    #[tokio::test]
    async fn github_pulls_merge_and_update_branch() {
        let (_t, svc, _ws) = setup_with(StubForge::default(), false).await;
        let m = svc
            .github_pulls_merge(
                "o".into(),
                "r".into(),
                42,
                Some("squash".into()),
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(m["merged"], true);
        assert_eq!(m["sha"], "mergedsha");

        let u = svc
            .github_pulls_update_branch("o".into(), "r".into(), 42, None)
            .await
            .unwrap();
        assert!(u["message"].is_string());
        assert!(u["url"].is_null());
    }

    #[tokio::test]
    async fn github_issues_list_and_search_shapes() {
        let (_t, svc, _ws) = setup_with(StubForge::default(), false).await;
        let l = svc
            .github_issues_list(
                "o".into(),
                "r".into(),
                Some("open".into()),
                None,
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(l["issues"][0]["number"], 11);
        assert_eq!(l["issues"][0]["owner"], "o");
        assert_eq!(l["issues"][0]["repo"], "r");
        assert!(l["nextToken"].is_null());

        let s = svc
            .github_issues_search(
                "o".into(),
                "r".into(),
                Some("assigned".into()),
                Some("open".into()),
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(s["issues"][0]["number"], 11);
    }

    #[tokio::test]
    async fn github_review_comments_threads_and_resolution() {
        let (_t, svc, _ws) = setup_with(StubForge::default(), false).await;
        let c = svc
            .github_list_review_comments("o".into(), "r".into(), 42, None, None)
            .await
            .unwrap();
        assert_eq!(c["comments"][0]["id"], 5);
        assert_eq!(c["comments"][0]["user"]["login"], "rev");

        let reply = svc
            .github_reply_review_comment("o".into(), "r".into(), 42, 5, "thanks".into())
            .await
            .unwrap();
        assert_eq!(reply["comment"]["inReplyToId"], 5);

        let t = svc
            .github_get_review_threads("o".into(), "r".into(), 42, None, None)
            .await
            .unwrap();
        assert_eq!(t["threads"][0]["id"], "RT1");
        assert_eq!(t["threads"][0]["comments"][0]["author"]["login"], "rev");

        let r = svc.github_resolve_thread("RT1".into()).await.unwrap();
        assert_eq!(r["isResolved"], true);
        let ur = svc.github_unresolve_thread("RT1".into()).await.unwrap();
        assert_eq!(ur["isResolved"], false);
    }
}

/// `file-tracking.*` reads + stage/unstage over the M4.7 `tracked_changes` table
/// and a real git worktree (M4.8).
mod file_tracking {
    use super::*;
    use git2::{Repository, Signature};
    use intent_store::NewTrackedChange;

    /// A self-cleaning git repository seeded with one commit.
    struct GitRepo {
        dir: PathBuf,
    }

    impl Drop for GitRepo {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn init_git_repo() -> GitRepo {
        let dir = std::env::temp_dir().join(format!("intentd-ft-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let repo = Repository::init(&dir).unwrap();
        let mut cfg = repo.config().unwrap();
        cfg.set_str("user.name", "Test").unwrap();
        cfg.set_str("user.email", "test@example.com").unwrap();
        commit_file(&dir, "seed.txt", "seed\n", "seed commit");
        GitRepo { dir }
    }

    fn commit_file(dir: &std::path::Path, rel: &str, contents: &str, message: &str) {
        std::fs::write(dir.join(rel), contents).unwrap();
        let repo = Repository::open(dir).unwrap();
        let mut index = repo.index().unwrap();
        index.add_path(std::path::Path::new(rel)).unwrap();
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
        repo.commit(Some("HEAD"), &sig, &sig, message, &tree, &refs)
            .unwrap();
    }

    fn tracked(ws: &WorkspaceId, path: &str, stage: &str, agent: Option<&str>) -> NewTrackedChange {
        NewTrackedChange {
            workspace_id: ws.clone(),
            path: path.to_string(),
            stage: stage.to_string(),
            status: "modified".to_string(),
            agent_id: agent.map(str::to_string),
            session_id: Some("sess-1".to_string()),
            turn: Some(3),
            commit_hash: None,
            old_blob_sha: None,
            new_blob_sha: None,
            additions: 5,
            deletions: 2,
        }
    }

    async fn ft_setup() -> (TempDb, Services, WorkspaceId) {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws = WorkspaceId::new();
        store.insert_workspace(&workspace(&ws)).await.expect("ws");
        (tmp, Services::new(store), ws)
    }

    #[tokio::test]
    async fn load_returns_tracked_changes_with_attribution() {
        let (_t, svc, ws) = ft_setup().await;
        svc.store()
            .upsert_tracked_change(&tracked(&ws, "src/a.ts", "unstaged", Some("agent-1")))
            .await
            .unwrap();
        svc.store()
            .upsert_tracked_change(&tracked(&ws, "src/b.ts", "staged", None))
            .await
            .unwrap();

        let result = svc.file_tracking_load(ws.clone()).await.unwrap();
        assert_eq!(result["totalCount"], serde_json::json!(2));
        assert_eq!(result["truncated"], serde_json::json!(false));
        let changes = result["changes"].as_array().unwrap();
        assert_eq!(changes.len(), 2);
        let a = changes
            .iter()
            .find(|c| c["relativePath"] == "src/a.ts")
            .unwrap();
        assert_eq!(a["stage"], serde_json::json!("unstaged"));
        assert_eq!(
            a["attribution"]["agent"]["agentId"],
            serde_json::json!("agent-1")
        );
        assert_eq!(
            a["attribution"]["agent"]["turnNumber"],
            serde_json::json!(3)
        );
        let b = changes
            .iter()
            .find(|c| c["relativePath"] == "src/b.ts")
            .unwrap();
        assert!(b["attribution"].get("agent").is_none());
    }

    #[tokio::test]
    async fn get_changes_filters_by_stage_and_agent() {
        let (_t, svc, ws) = ft_setup().await;
        svc.store()
            .upsert_tracked_change(&tracked(&ws, "a.ts", "unstaged", Some("agent-1")))
            .await
            .unwrap();
        svc.store()
            .upsert_tracked_change(&tracked(&ws, "b.ts", "staged", Some("agent-2")))
            .await
            .unwrap();

        let staged = svc
            .file_tracking_get_changes(ws.clone(), Some(serde_json::json!({ "stage": "staged" })))
            .await
            .unwrap();
        assert_eq!(staged["totalCount"], serde_json::json!(2));
        let arr = staged["changes"].as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["relativePath"], serde_json::json!("b.ts"));

        let by_agent = svc
            .file_tracking_get_changes(
                ws.clone(),
                Some(serde_json::json!({ "agentId": "agent-1" })),
            )
            .await
            .unwrap();
        let arr = by_agent["changes"].as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["relativePath"], serde_json::json!("a.ts"));
    }

    #[tokio::test]
    async fn get_line_stats_sums_additions_and_deletions() {
        let (_t, svc, ws) = ft_setup().await;
        svc.store()
            .upsert_tracked_change(&tracked(&ws, "a.ts", "unstaged", None))
            .await
            .unwrap();
        svc.store()
            .upsert_tracked_change(&tracked(&ws, "b.ts", "staged", None))
            .await
            .unwrap();
        let stats = svc.file_tracking_get_line_stats(ws.clone()).await.unwrap();
        assert_eq!(stats["additions"], serde_json::json!(10));
        assert_eq!(stats["deletions"], serde_json::json!(4));
    }

    #[tokio::test]
    async fn init_is_ok_and_load_empty_for_unknown_workspace() {
        let (_t, svc, ws) = ft_setup().await;
        assert_eq!(
            svc.file_tracking_init(ws.clone()).await.unwrap(),
            serde_json::json!({ "ok": true })
        );
        let missing = WorkspaceId::new();
        let result = svc.file_tracking_load(missing).await.unwrap();
        assert_eq!(result["totalCount"], serde_json::json!(0));
        assert_eq!(result["changes"], serde_json::json!([]));
    }

    #[tokio::test]
    async fn stage_then_unstage_preserves_attribution() {
        let repo = init_git_repo();
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws_id = WorkspaceId::new();
        let mut ws = workspace(&ws_id);
        ws.worktree_path = Some(repo.dir.to_string_lossy().to_string());
        store.insert_workspace(&ws).await.unwrap();
        let svc = Services::new(store);

        // A new agent-authored file with an unstaged audit row.
        std::fs::write(repo.dir.join("x.txt"), "hi\n").unwrap();
        svc.store()
            .upsert_tracked_change(&tracked(&ws_id, "x.txt", "unstaged", Some("agent-9")))
            .await
            .unwrap();

        // Stage → git index + audit row both move to staged, attribution kept.
        svc.file_tracking_stage(ws_id.clone(), serde_json::json!(["x.txt"]))
            .await
            .unwrap();
        let st = intent_git::status::status(&repo.dir).unwrap();
        assert!(st.files.iter().any(|f| f.path == "x.txt" && f.staged));
        let changes = svc.file_tracking_load(ws_id.clone()).await.unwrap();
        let row = changes["changes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["relativePath"] == "x.txt")
            .unwrap();
        assert_eq!(row["stage"], serde_json::json!("staged"));
        assert_eq!(
            row["attribution"]["agent"]["agentId"],
            serde_json::json!("agent-9")
        );

        // Unstage → both move back to unstaged, attribution still kept.
        svc.file_tracking_unstage(ws_id.clone(), serde_json::json!(["x.txt"]))
            .await
            .unwrap();
        let st = intent_git::status::status(&repo.dir).unwrap();
        assert!(st.files.iter().any(|f| f.path == "x.txt" && !f.staged));
        let changes = svc.file_tracking_load(ws_id.clone()).await.unwrap();
        let row = changes["changes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["relativePath"] == "x.txt")
            .unwrap();
        assert_eq!(row["stage"], serde_json::json!("unstaged"));
        assert_eq!(
            row["attribution"]["agent"]["agentId"],
            serde_json::json!("agent-9")
        );
    }

    #[tokio::test]
    async fn stage_rejects_empty_paths() {
        let (_t, svc, ws) = ft_setup().await;
        let err = svc
            .file_tracking_stage(ws, serde_json::json!([]))
            .await
            .unwrap_err();
        assert!(format!("{err}").contains("No file paths provided"));
    }

    #[tokio::test]
    async fn load_commits_returns_history_with_attribution() {
        let repo = init_git_repo();
        commit_file(
            &repo.dir,
            "feature.txt",
            "feature\n",
            "add feature\n\nAgent-Id: agent-7\nLinked-Note-Id: note-2\n",
        );
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws_id = WorkspaceId::new();
        let mut ws = workspace(&ws_id);
        ws.worktree_path = Some(repo.dir.to_string_lossy().to_string());
        store.insert_workspace(&ws).await.unwrap();
        let svc = Services::new(store);

        let result = svc
            .file_tracking_load_commits(ws_id, Some(10), None, None)
            .await
            .unwrap();
        let commits = result["commits"].as_array().unwrap();
        assert_eq!(commits.len(), 2);
        let head = &commits[0];
        assert_eq!(head["agentId"], serde_json::json!("agent-7"));
        assert_eq!(head["linkedNoteId"], serde_json::json!("note-2"));
        assert_eq!(head["filesChanged"], serde_json::json!(1));
        assert_eq!(head["isPushed"], serde_json::json!(false));
    }

    #[tokio::test]
    async fn sync_reconciles_unstaged_changes_preserving_attribution() {
        let repo = init_git_repo();
        // A pre-existing agent attribution row for a file we now modify.
        std::fs::write(repo.dir.join("seed.txt"), "seed\nmore\n").unwrap();
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws_id = WorkspaceId::new();
        let mut ws = workspace(&ws_id);
        ws.worktree_path = Some(repo.dir.to_string_lossy().to_string());
        store.insert_workspace(&ws).await.unwrap();
        store
            .upsert_tracked_change(&tracked(&ws_id, "seed.txt", "unstaged", Some("agent-3")))
            .await
            .unwrap();
        let svc = Services::new(store);

        let result = svc.file_tracking_sync(ws_id.clone(), false).await.unwrap();
        assert_eq!(result["success"], serde_json::json!(true));
        let changes = svc.file_tracking_load(ws_id).await.unwrap();
        let row = changes["changes"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["relativePath"] == "seed.txt")
            .unwrap();
        assert_eq!(
            row["attribution"]["agent"]["agentId"],
            serde_json::json!("agent-3")
        );
        assert!(row["stats"]["additions"].as_i64().unwrap() >= 1);
    }

    /// `git.stage` → `git.unstage` round-trip over the git.* RPC surface: a
    /// staged modification is reflected in the index, then dropped back to an
    /// unstaged modification, both calls echoing the validated path list.
    #[tokio::test]
    async fn git_unstage_round_trips_a_staged_modification() {
        let repo = init_git_repo();
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws_id = WorkspaceId::new();
        let mut ws = workspace(&ws_id);
        ws.worktree_path = Some(repo.dir.to_string_lossy().to_string());
        store.insert_workspace(&ws).await.unwrap();
        let svc = Services::new(store);

        std::fs::write(repo.dir.join("seed.txt"), "seed changed\n").unwrap();

        let staged = svc
            .git_stage(ws_id.clone(), serde_json::json!(["seed.txt"]))
            .await
            .unwrap();
        assert_eq!(staged, vec!["seed.txt".to_string()]);
        let st = intent_git::status::status(&repo.dir).unwrap();
        assert!(st.files.iter().any(|f| f.path == "seed.txt" && f.staged));

        let unstaged = svc
            .git_unstage(ws_id.clone(), serde_json::json!(["seed.txt"]))
            .await
            .unwrap();
        assert_eq!(unstaged, vec!["seed.txt".to_string()]);
        let st = intent_git::status::status(&repo.dir).unwrap();
        assert!(st.files.iter().any(|f| f.path == "seed.txt" && !f.staged));
    }

    /// `git.unstage` is idempotent: unstaging an already-unstaged path is a
    /// no-op that returns the path list rather than erroring.
    #[tokio::test]
    async fn git_unstage_is_idempotent_on_already_unstaged_paths() {
        let repo = init_git_repo();
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws_id = WorkspaceId::new();
        let mut ws = workspace(&ws_id);
        ws.worktree_path = Some(repo.dir.to_string_lossy().to_string());
        store.insert_workspace(&ws).await.unwrap();
        let svc = Services::new(store);

        std::fs::write(repo.dir.join("seed.txt"), "seed changed\n").unwrap();

        // Never staged → first unstage is already a no-op; a second confirms
        // idempotency (no error either time).
        let first = svc
            .git_unstage(ws_id.clone(), serde_json::json!(["seed.txt"]))
            .await
            .unwrap();
        assert_eq!(first, vec!["seed.txt".to_string()]);
        let second = svc
            .git_unstage(ws_id.clone(), serde_json::json!(["seed.txt"]))
            .await
            .unwrap();
        assert_eq!(second, vec!["seed.txt".to_string()]);
        let st = intent_git::status::status(&repo.dir).unwrap();
        assert!(st.files.iter().any(|f| f.path == "seed.txt" && !f.staged));
    }

    /// `git.discard` over the git.* RPC surface: an unstaged worktree change
    /// is restored from the index, echoing the validated path list.
    #[tokio::test]
    async fn git_discard_reverts_an_unstaged_modification() {
        let repo = init_git_repo();
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws_id = WorkspaceId::new();
        let mut ws = workspace(&ws_id);
        ws.worktree_path = Some(repo.dir.to_string_lossy().to_string());
        store.insert_workspace(&ws).await.unwrap();
        let svc = Services::new(store);

        std::fs::write(repo.dir.join("seed.txt"), "seed changed\n").unwrap();

        let discarded = svc
            .git_discard(ws_id.clone(), serde_json::json!(["seed.txt"]))
            .await
            .unwrap();
        assert_eq!(discarded, vec!["seed.txt".to_string()]);
        let on_disk = std::fs::read_to_string(repo.dir.join("seed.txt")).unwrap();
        assert_eq!(on_disk, "seed\n");
        let st = intent_git::status::status(&repo.dir).unwrap();
        assert!(st.files.iter().all(|f| f.path != "seed.txt"));
    }

    /// `git.discard` on an untracked file deletes it from disk (mirrors the
    /// reference's `fs.unlink` after `git ls-files --error-unmatch` fails).
    #[tokio::test]
    async fn git_discard_deletes_an_untracked_file() {
        let repo = init_git_repo();
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws_id = WorkspaceId::new();
        let mut ws = workspace(&ws_id);
        ws.worktree_path = Some(repo.dir.to_string_lossy().to_string());
        store.insert_workspace(&ws).await.unwrap();
        let svc = Services::new(store);

        std::fs::write(repo.dir.join("new.txt"), "hi\n").unwrap();
        assert!(repo.dir.join("new.txt").exists());

        let discarded = svc
            .git_discard(ws_id.clone(), serde_json::json!(["new.txt"]))
            .await
            .unwrap();
        assert_eq!(discarded, vec!["new.txt".to_string()]);
        assert!(!repo.dir.join("new.txt").exists());
    }

    /// `git.discard` is idempotent: discarding a clean tracked path or a
    /// missing untracked path returns the list rather than erroring.
    #[tokio::test]
    async fn git_discard_is_idempotent() {
        let repo = init_git_repo();
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws_id = WorkspaceId::new();
        let mut ws = workspace(&ws_id);
        ws.worktree_path = Some(repo.dir.to_string_lossy().to_string());
        store.insert_workspace(&ws).await.unwrap();
        let svc = Services::new(store);

        // Clean tracked path (seed.txt is committed, unchanged) + missing
        // untracked path (nope.txt was never created).
        let first = svc
            .git_discard(ws_id.clone(), serde_json::json!(["seed.txt", "nope.txt"]))
            .await
            .unwrap();
        assert_eq!(first, vec!["seed.txt".to_string(), "nope.txt".to_string()]);
        let second = svc
            .git_discard(ws_id.clone(), serde_json::json!(["seed.txt", "nope.txt"]))
            .await
            .unwrap();
        assert_eq!(second, vec!["seed.txt".to_string(), "nope.txt".to_string()]);
    }

    /// `git.discard` refuses the discard-all tokens (`.` / `*` / `--all`) in
    /// EVERY input shape: top-level string, CSV entry, and array element.
    /// Regression for the array-form bypass where the shared stage parser
    /// only inspected the top-level string, letting `["*"]` / `["--all"]`
    /// fall through to a silent no-op and `["."]` to the worktree-escape
    /// guard's `-32602`.
    #[tokio::test]
    async fn git_discard_refuses_discard_all_in_every_shape() {
        let repo = init_git_repo();
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws_id = WorkspaceId::new();
        let mut ws = workspace(&ws_id);
        ws.worktree_path = Some(repo.dir.to_string_lossy().to_string());
        store.insert_workspace(&ws).await.unwrap();
        let svc = Services::new(store);

        let expect_discard_all = |v: serde_json::Value| {
            let svc = &svc;
            let ws_id = ws_id.clone();
            async move {
                let err = svc.git_discard(ws_id, v).await.unwrap_err();
                let msg = format!("{err}");
                assert!(
                    matches!(err, intent_core::Error::Internal(_)),
                    "expected -32603 Internal, got: {msg}"
                );
                assert!(
                    msg.contains("Discarding all files is not allowed"),
                    "expected discard-all message, got: {msg}"
                );
            }
        };

        // Top-level string forms.
        expect_discard_all(serde_json::json!(".")).await;
        expect_discard_all(serde_json::json!("*")).await;
        expect_discard_all(serde_json::json!("--all")).await;
        // Array element forms (the bypass this test covers).
        expect_discard_all(serde_json::json!(["."])).await;
        expect_discard_all(serde_json::json!(["*"])).await;
        expect_discard_all(serde_json::json!(["--all"])).await;
        // Mixed array — a single tainted element still trips the guard.
        expect_discard_all(serde_json::json!(["seed.txt", "*"])).await;
    }

    /// Build a workspace whose worktree points at `repo`, returning the service.
    async fn svc_with_repo(repo: &GitRepo) -> (TempDb, Services, WorkspaceId) {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws_id = WorkspaceId::new();
        let mut ws = workspace(&ws_id);
        ws.worktree_path = Some(repo.dir.to_string_lossy().to_string());
        store.insert_workspace(&ws).await.unwrap();
        (tmp, Services::new(store), ws_id)
    }

    /// `git.commits` returns the §5.5 `{ items, nextToken }` envelope of
    /// `CommitInfo` (§8.9), walking older pages via the opaque continuation
    /// token; attribution trailers populate `agentId`/`linkedNoteId`.
    #[tokio::test]
    async fn git_commits_returns_commit_info_page() {
        let repo = init_git_repo();
        commit_file(
            &repo.dir,
            "feature.txt",
            "feature\n",
            "add feature\n\nAgent-Id: agent-7\nLinked-Note-Id: note-2\n",
        );
        let (_t, svc, ws_id) = svc_with_repo(&repo).await;

        // Page size 1 → newest commit only, with a token for the older page.
        let page1 = svc.git_commits(ws_id.clone(), Some(1), None).await.unwrap();
        let items = page1["items"].as_array().unwrap();
        assert_eq!(items.len(), 1);
        let head = &items[0];
        assert_eq!(head["agentId"], serde_json::json!("agent-7"));
        assert_eq!(head["linkedNoteId"], serde_json::json!("note-2"));
        assert_eq!(head["author"], serde_json::json!("Test"));
        assert_eq!(head["email"], serde_json::json!("test@example.com"));
        assert_eq!(head["files"], serde_json::json!(["feature.txt"]));
        let hash = head["hash"].as_str().unwrap();
        assert_eq!(head["sha"], serde_json::json!(hash[..7].to_string()));
        assert!(head["message"].as_str().unwrap().starts_with("add feature"));
        let token = page1["nextToken"].as_str().expect("token for older page");

        // Following the token yields the older (seed) commit, then no more.
        let page2 = svc
            .git_commits(ws_id, Some(1), Some(token.to_string()))
            .await
            .unwrap();
        let items2 = page2["items"].as_array().unwrap();
        assert_eq!(items2.len(), 1);
        assert!(items2[0]["message"]
            .as_str()
            .unwrap()
            .starts_with("seed commit"));
        assert_eq!(page2["nextToken"], serde_json::Value::Null);
    }

    /// `git.changes` returns the working-tree `FileStatus[]` (`path`/`status`/
    /// `staged`), including untracked files.
    #[tokio::test]
    async fn git_changes_returns_working_tree_files() {
        let repo = init_git_repo();
        std::fs::write(repo.dir.join("seed.txt"), "seed\nmore\n").unwrap();
        std::fs::write(repo.dir.join("new.txt"), "hi\n").unwrap();
        let (_t, svc, ws_id) = svc_with_repo(&repo).await;

        let changes = svc.git_changes(ws_id).await.unwrap();
        let arr = changes.as_array().unwrap();
        let seed = arr.iter().find(|c| c["path"] == "seed.txt").unwrap();
        assert_eq!(seed["status"], serde_json::json!("M"));
        assert_eq!(seed["staged"], serde_json::json!(false));
        let new = arr.iter().find(|c| c["path"] == "new.txt").unwrap();
        assert_eq!(new["status"], serde_json::json!("?"));
    }

    /// `git.diffs` (unstaged) returns `[{ path, hunks }]` with the FE hunk shape
    /// (`oldStart`/`oldLines`/`newStart`/`newLines`/`lines`) and `DiffLine`s
    /// tagged `Context`/`Addition`/`Deletion` with 1-based line numbers.
    #[tokio::test]
    async fn git_diffs_unstaged_returns_path_and_hunks() {
        let repo = init_git_repo();
        std::fs::write(repo.dir.join("seed.txt"), "seed\nadded\n").unwrap();
        let (_t, svc, ws_id) = svc_with_repo(&repo).await;

        let diffs = svc.git_diffs(ws_id, None, false, None).await.unwrap();
        let arr = diffs.as_array().unwrap();
        let f = arr.iter().find(|d| d["path"] == "seed.txt").unwrap();
        let hunks = f["hunks"].as_array().unwrap();
        assert!(!hunks.is_empty());
        let h = &hunks[0];
        assert!(h["newStart"].as_u64().is_some());
        assert!(h["oldStart"].as_u64().is_some());
        let lines = h["lines"].as_array().unwrap();
        let add = lines
            .iter()
            .find(|l| l["type"] == "Addition")
            .expect("an addition line");
        assert!(add["content"].as_str().unwrap().contains("added"));
        assert!(add["newNumber"].as_u64().is_some());
    }

    /// `git.diffs` (staged) hydrates hunks from blob SHAs and honors the `path`
    /// filter, returning only the requested file.
    #[tokio::test]
    async fn git_diffs_staged_filters_to_path() {
        let repo = init_git_repo();
        std::fs::write(repo.dir.join("seed.txt"), "seed\nstaged change\n").unwrap();
        std::fs::write(repo.dir.join("other.txt"), "other\n").unwrap();
        {
            let r = Repository::open(&repo.dir).unwrap();
            let mut idx = r.index().unwrap();
            idx.add_path(std::path::Path::new("seed.txt")).unwrap();
            idx.add_path(std::path::Path::new("other.txt")).unwrap();
            idx.write().unwrap();
        }
        let (_t, svc, ws_id) = svc_with_repo(&repo).await;

        let diffs = svc
            .git_diffs(ws_id, Some("seed.txt".to_string()), true, None)
            .await
            .unwrap();
        let arr = diffs.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["path"], serde_json::json!("seed.txt"));
        assert!(!arr[0]["hunks"].as_array().unwrap().is_empty());
    }

    /// The git reads degrade to empty results for a workspace with no worktree
    /// (mirrors the `git.status` empty fallbacks).
    #[tokio::test]
    async fn git_reads_empty_for_non_repo_workspace() {
        let (_t, svc, ws) = ft_setup().await;
        assert_eq!(
            svc.git_changes(ws.clone()).await.unwrap(),
            serde_json::json!([])
        );
        assert_eq!(
            svc.git_diffs(ws.clone(), None, false, None).await.unwrap(),
            serde_json::json!([])
        );
        let commits = svc.git_commits(ws.clone(), None, None).await.unwrap();
        assert_eq!(commits["items"], serde_json::json!([]));
        assert_eq!(commits["nextToken"], serde_json::Value::Null);
        // git.commitDetails degrades to an empty envelope (graceful) — same as
        // the other git reads — when the workspace has no worktree.
        let details = svc
            .git_commit_details(ws, "deadbeef".to_string())
            .await
            .unwrap();
        assert_eq!(details["commitHash"], "deadbeef");
        assert_eq!(details["files"], serde_json::json!([]));
        assert_eq!(details["fileDetails"], serde_json::json!([]));
    }

    /// `git.commitDetails` returns the commit's metadata + per-file additions/
    /// deletions for a real repo (PROTOCOL §5.6).
    #[tokio::test]
    async fn git_commit_details_returns_metadata_and_file_stats() {
        let repo = init_git_repo();
        // Land a second commit so HEAD has a non-empty parent diff.
        std::fs::write(repo.dir.join("seed.txt"), "seed\nadded\n").unwrap();
        std::fs::write(repo.dir.join("new.txt"), "hello\n").unwrap();
        {
            let r = Repository::open(&repo.dir).unwrap();
            let mut idx = r.index().unwrap();
            idx.add_path(std::path::Path::new("seed.txt")).unwrap();
            idx.add_path(std::path::Path::new("new.txt")).unwrap();
            idx.write().unwrap();
            let tree_oid = idx.write_tree().unwrap();
            let tree = r.find_tree(tree_oid).unwrap();
            let sig = git2::Signature::now("Test", "test@example.com").unwrap();
            let parent = r
                .head()
                .unwrap()
                .target()
                .and_then(|oid| r.find_commit(oid).ok())
                .unwrap();
            r.commit(Some("HEAD"), &sig, &sig, "second", &tree, &[&parent])
                .unwrap();
        }
        let head = Repository::open(&repo.dir)
            .unwrap()
            .head()
            .unwrap()
            .target()
            .unwrap()
            .to_string();
        let (_t, svc, ws_id) = svc_with_repo(&repo).await;

        let details = svc.git_commit_details(ws_id, head.clone()).await.unwrap();
        assert_eq!(details["commitHash"], head);
        assert_eq!(details["author"], "Test");
        assert_eq!(details["authorEmail"], "test@example.com");
        assert_eq!(details["message"], "second");
        let files = details["files"].as_array().unwrap();
        assert!(files.iter().any(|f| f == "seed.txt"));
        assert!(files.iter().any(|f| f == "new.txt"));
        let file_details = details["fileDetails"].as_array().unwrap();
        let seed = file_details
            .iter()
            .find(|f| f["path"] == "seed.txt")
            .unwrap();
        assert_eq!(seed["additions"], 1);
        assert_eq!(seed["deletions"], 0);
    }

    /// An unresolvable `commitHash` degrades to the empty envelope (graceful)
    /// rather than surfacing as a `-32603` so the FE can render an empty state.
    #[tokio::test]
    async fn git_commit_details_unknown_hash_is_empty_envelope() {
        let repo = init_git_repo();
        let (_t, svc, ws_id) = svc_with_repo(&repo).await;
        let details = svc
            .git_commit_details(
                ws_id,
                "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef".to_string(),
            )
            .await
            .unwrap();
        assert_eq!(
            details["commitHash"],
            "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
        );
        assert_eq!(details["files"], serde_json::json!([]));
        assert_eq!(details["fileDetails"], serde_json::json!([]));
    }

    /// `git.diffs` with `commitHash` returns the commit's per-file hunks vs
    /// its first parent (PROTOCOL §5.6 extension).
    #[tokio::test]
    async fn git_diffs_with_commit_hash_returns_per_file_hunks() {
        let repo = init_git_repo();
        std::fs::write(repo.dir.join("seed.txt"), "seed\nadded\n").unwrap();
        {
            let r = Repository::open(&repo.dir).unwrap();
            let mut idx = r.index().unwrap();
            idx.add_path(std::path::Path::new("seed.txt")).unwrap();
            idx.write().unwrap();
            let tree_oid = idx.write_tree().unwrap();
            let tree = r.find_tree(tree_oid).unwrap();
            let sig = git2::Signature::now("Test", "test@example.com").unwrap();
            let parent = r
                .head()
                .unwrap()
                .target()
                .and_then(|oid| r.find_commit(oid).ok())
                .unwrap();
            r.commit(Some("HEAD"), &sig, &sig, "second", &tree, &[&parent])
                .unwrap();
        }
        let head = Repository::open(&repo.dir)
            .unwrap()
            .head()
            .unwrap()
            .target()
            .unwrap()
            .to_string();
        let (_t, svc, ws_id) = svc_with_repo(&repo).await;

        let diffs = svc
            .git_diffs(ws_id, Some("seed.txt".to_string()), false, Some(head))
            .await
            .unwrap();
        let arr = diffs.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0]["path"], "seed.txt");
        let lines = arr[0]["hunks"][0]["lines"].as_array().unwrap();
        assert!(lines
            .iter()
            .any(|l| l["type"] == "Addition"
                && l["content"].as_str().unwrap_or("").contains("added")));
    }
}

/// `metrics.*` aggregation (§17.5) over the M4.7 `tracked_changes` table: the
/// internal recompute fills `workspace_metrics`/`agent_metrics`, and the four
/// read/clear wire methods project the §5.20 `Metrics` shape.
mod metrics {
    use super::*;
    use intent_store::NewTrackedChange;

    async fn setup() -> (TempDb, Services, WorkspaceId) {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws = WorkspaceId::new();
        store.insert_workspace(&workspace(&ws)).await.expect("ws");
        (tmp, Services::new(store), ws)
    }

    fn change(
        ws: &WorkspaceId,
        path: &str,
        agent: Option<&str>,
        additions: i64,
        deletions: i64,
    ) -> NewTrackedChange {
        NewTrackedChange {
            workspace_id: ws.clone(),
            path: path.to_string(),
            stage: "unstaged".to_string(),
            status: "modified".to_string(),
            agent_id: agent.map(str::to_string),
            session_id: None,
            turn: None,
            commit_hash: None,
            old_blob_sha: None,
            new_blob_sha: None,
            additions,
            deletions,
        }
    }

    #[tokio::test]
    async fn recompute_aggregates_by_workspace_and_agent() {
        let (_t, svc, ws) = setup().await;
        let store = svc.store();
        store
            .upsert_tracked_change(&change(&ws, "a.ts", Some("agent-1"), 100, 10))
            .await
            .unwrap();
        store
            .upsert_tracked_change(&change(&ws, "b.ts", Some("agent-1"), 40, 2))
            .await
            .unwrap();
        store
            .upsert_tracked_change(&change(&ws, "c.ts", Some("agent-2"), 7, 1))
            .await
            .unwrap();
        store
            .upsert_tracked_change(&change(&ws, "d.ts", None, 3, 0))
            .await
            .unwrap();

        crate::metrics::recompute(store, &ws).await.unwrap();

        let m = svc.metrics_get_workspace_stats(ws.clone()).await.unwrap();
        assert_eq!(m["additions"], serde_json::json!(150));
        assert_eq!(m["deletions"], serde_json::json!(13));
        assert_eq!(m["filesChanged"], serde_json::json!(4));
        assert_eq!(m["byAgent"]["agent-1"]["additions"], serde_json::json!(140));
        assert_eq!(m["byAgent"]["agent-1"]["deletions"], serde_json::json!(12));
        assert_eq!(
            m["byAgent"]["agent-1"]["filesChanged"],
            serde_json::json!(2)
        );
        assert_eq!(m["byAgent"]["agent-2"]["additions"], serde_json::json!(7));
        // Unattributed changes count toward the workspace total but not byAgent.
        assert!(m["byAgent"].get("").is_none());
    }

    #[tokio::test]
    async fn workspace_stats_null_when_unknown() {
        let (_t, svc, _ws) = setup().await;
        let missing = WorkspaceId::new();
        let m = svc.metrics_get_workspace_stats(missing).await.unwrap();
        assert_eq!(m, serde_json::Value::Null);
    }

    #[tokio::test]
    async fn agent_stats_sum_across_workspaces_and_clear() {
        let (_t, svc, ws1) = setup().await;
        let store = svc.store();
        let ws2 = WorkspaceId::new();
        store.insert_workspace(&workspace(&ws2)).await.unwrap();

        store
            .upsert_tracked_change(&change(&ws1, "a.ts", Some("agent-1"), 10, 1))
            .await
            .unwrap();
        store
            .upsert_tracked_change(&change(&ws2, "b.ts", Some("agent-1"), 5, 4))
            .await
            .unwrap();
        crate::metrics::recompute(store, &ws1).await.unwrap();
        crate::metrics::recompute(store, &ws2).await.unwrap();

        let a = svc
            .metrics_get_agent_stats("agent-1".to_string())
            .await
            .unwrap();
        assert_eq!(a["additions"], serde_json::json!(15));
        assert_eq!(a["deletions"], serde_json::json!(5));
        assert_eq!(a["filesChanged"], serde_json::json!(2));
        assert!(a.get("byAgent").is_none());

        // getAllWorkspaceStats returns one entry per workspace.
        let all = svc.metrics_get_all_workspace_stats().await.unwrap();
        assert_eq!(all[&ws1.0]["additions"], serde_json::json!(10));
        assert_eq!(all[&ws2.0]["additions"], serde_json::json!(5));

        // clearAgentStats resets the agent's counters across workspaces.
        let cleared = svc
            .metrics_clear_agent_stats("agent-1".to_string())
            .await
            .unwrap();
        assert_eq!(cleared, serde_json::json!({ "success": true }));
        let after = svc
            .metrics_get_agent_stats("agent-1".to_string())
            .await
            .unwrap();
        assert_eq!(after, serde_json::Value::Null);
    }

    #[tokio::test]
    async fn recompute_drops_metrics_when_no_changes() {
        let (_t, svc, ws) = setup().await;
        let store = svc.store();
        // Seed a stale aggregate, then recompute with no tracked changes: the
        // durable rows are cleared so the read returns `null`.
        store.upsert_workspace_metrics(&ws, 99, 9, 3).await.unwrap();
        store
            .upsert_agent_metrics(&ws, "agent-1", 99, 9, 3)
            .await
            .unwrap();
        assert!(store.get_workspace_metrics(&ws).await.unwrap().is_some());

        crate::metrics::recompute(store, &ws).await.unwrap();
        assert!(store.get_workspace_metrics(&ws).await.unwrap().is_none());
        assert!(store
            .list_agent_metrics_for_workspace(&ws)
            .await
            .unwrap()
            .is_empty());
        let m = svc.metrics_get_workspace_stats(ws).await.unwrap();
        assert_eq!(m, serde_json::Value::Null);
    }
}

/// `search.*` wire glue (§5.15): requestId mint/echo, the worktree-rooted
/// matches/files shape, and the idempotent no-op cancel. The ripgrep walk
/// itself is covered in `intent-search`.
mod search {
    use super::*;

    struct TempTree(PathBuf);
    impl Drop for TempTree {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn worktree() -> TempTree {
        let dir = std::env::temp_dir().join(format!("intentd-search-svc-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(dir.join("src")).unwrap();
        std::fs::write(dir.join("src/main.rs"), "fn main() {\n    // TODO: x\n}\n").unwrap();
        std::fs::write(dir.join("README.md"), "# readme\nTODO later\n").unwrap();
        TempTree(dir)
    }

    async fn services_with_worktree(dir: &std::path::Path) -> (TempDb, Services, WorkspaceId) {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws_id = WorkspaceId::new();
        let mut ws = workspace(&ws_id);
        // `path` is transient (never persisted); `worktree_path` round-trips.
        ws.worktree_path = Some(dir.to_string_lossy().to_string());
        store.insert_workspace(&ws).await.expect("ws");
        (tmp, Services::new(store), ws_id)
    }

    #[tokio::test]
    async fn in_files_echoes_request_id_and_returns_matches() {
        let tree = worktree();
        let (_tmp, svc, ws) = services_with_worktree(&tree.0).await;
        let r = svc
            .search_in_files(ws, "TODO".into(), None, Some("srch-xyz".into()))
            .await
            .unwrap();
        assert_eq!(r["requestId"], "srch-xyz");
        assert_eq!(r["truncated"], false);
        assert_eq!(r["matches"].as_array().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn in_files_mints_request_id_when_absent() {
        let tree = worktree();
        let (_tmp, svc, ws) = services_with_worktree(&tree.0).await;
        let r = svc
            .search_in_files(ws, "TODO".into(), None, None)
            .await
            .unwrap();
        assert!(r["requestId"].as_str().unwrap().starts_with("srch-"));
    }

    #[tokio::test]
    async fn file_names_glob_returns_relative_paths() {
        let tree = worktree();
        let (_tmp, svc, ws) = services_with_worktree(&tree.0).await;
        let r = svc
            .search_file_names(ws, "*.rs".into(), None, Some("srch-f".into()))
            .await
            .unwrap();
        assert_eq!(r["requestId"], "srch-f");
        let files = r["files"].as_array().unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0], "src/main.rs");
    }

    #[tokio::test]
    async fn no_worktree_path_returns_empty() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.unwrap();
        let ws_id = WorkspaceId::new();
        store.insert_workspace(&workspace(&ws_id)).await.unwrap();
        let svc = Services::new(store);
        let r = svc
            .search_in_files(ws_id, "x".into(), None, Some("srch-1".into()))
            .await
            .unwrap();
        assert_eq!(r["matches"].as_array().unwrap().len(), 0);
        assert_eq!(r["truncated"], false);
    }

    #[tokio::test]
    async fn cancel_unknown_is_noop_success() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.unwrap();
        let svc = Services::new(store);
        let r = svc.search_cancel("srch-unknown".into()).await.unwrap();
        assert_eq!(r, serde_json::json!({ "ok": true }));
    }
}

/// Store-backed `search.*` adapters (M4.12): per-namespace matching, the global
/// notes path, the store-backed memories path (empty + seeded), and the streaming
/// `search:result`/`search:done` delivery + mid-stream cancellation (§5.15/§6.5).
mod search_adapters {
    use std::time::Duration;

    use intent_core::{AgentId, AgentSession, AgentStatus, WorkspaceApi, WorkspaceId};
    use intent_store::{NewEvent, Store};
    use serde_json::{json, Value};

    use super::{note, workspace, TempDb};
    use crate::{EventBus, Services, Subscription, SubscriptionFilter};

    async fn store_with_ws() -> (TempDb, Store, WorkspaceId) {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws = WorkspaceId::new();
        store.insert_workspace(&workspace(&ws)).await.expect("ws");
        (tmp, store, ws)
    }

    async fn insert_session(
        store: &Store,
        ws: &WorkspaceId,
        agent: &str,
        messages: &[(&str, Value)],
    ) -> AgentId {
        let id = AgentId::from(agent);
        let ts = intent_core::now_iso();
        let session = AgentSession {
            id: id.clone(),
            workspace_id: ws.clone(),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: "A".to_string(),
            name_explicitly_set: false,
            model: None,
            provider: None,
            system_prompt: None,
            specialist: None,
            status: AgentStatus::default(),
            is_active: false,
            messages: vec![],
            stats: None,
            task_note_id: None,
            skip_auto_commit: false,
            completion_report: None,
            completion_report_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            is_background: false,
            metadata: None,
            created_at: ts.clone(),
            updated_at: ts.clone(),
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
        };
        store.insert_agent_session(&session).await.expect("session");
        for (role, content) in messages {
            store
                .append_agent_message(&id, role, content, &ts)
                .await
                .expect("message");
        }
        id
    }

    #[tokio::test]
    async fn messages_search_matches_and_previews() {
        let (_tmp, store, ws) = store_with_ws().await;
        insert_session(
            &store,
            &ws,
            "agent-1",
            &[
                ("user", json!("totally unrelated")),
                (
                    "assistant",
                    json!([{ "type": "text", "text": "here is the needle in text" }]),
                ),
            ],
        )
        .await;
        let svc = Services::new(store);
        let r = svc
            .search_messages(ws, "needle".into(), None, None, None, Some("srch-1".into()))
            .await
            .unwrap();
        assert_eq!(r["requestId"], "srch-1");
        let matches = r["matches"].as_array().unwrap();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0]["agentId"], "agent-1");
        assert!(matches[0]["messageId"].is_string());
        assert!(matches[0]["preview"].as_str().unwrap().contains("needle"));
    }

    #[tokio::test]
    async fn messages_search_filters_by_role() {
        let (_tmp, store, ws) = store_with_ws().await;
        insert_session(
            &store,
            &ws,
            "agent-1",
            &[
                ("user", json!("the needle here")),
                ("assistant", json!("another needle there")),
            ],
        )
        .await;
        let svc = Services::new(store);
        let r = svc
            .search_messages(ws, "needle".into(), None, Some("user".into()), None, None)
            .await
            .unwrap();
        let matches = r["matches"].as_array().unwrap();
        assert_eq!(matches.len(), 1);
    }

    #[tokio::test]
    async fn events_search_matches_data() {
        let (_tmp, store, ws) = store_with_ws().await;
        let ev = store
            .insert_event(&NewEvent {
                workspace_id: ws.clone(),
                timestamp: intent_core::now_iso(),
                event_type: "file:changed".to_string(),
                actor: intent_core::EventActor {
                    actor_type: intent_core::ActorType::System,
                    ..Default::default()
                },
                session_id: None,
                correlation_id: None,
                parent_event_id: None,
                metadata: None,
                data: json!({ "path": "src/alpha.rs", "action": "modify" }),
            })
            .await
            .expect("event");
        let svc = Services::new(store);
        let r = svc
            .search_events("alpha".into(), Some(ws), None, Some("srch-e".into()))
            .await
            .unwrap();
        let matches = r["matches"].as_array().unwrap();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0]["eventId"], ev.id);
    }

    #[tokio::test]
    async fn notes_search_is_global_across_workspaces() {
        let (_tmp, store, ws1) = store_with_ws().await;
        let ws2 = WorkspaceId::new();
        store.insert_workspace(&workspace(&ws2)).await.expect("ws2");
        store
            .insert_note(&note(&ws1, "n-1", "alpha content"))
            .await
            .expect("n1");
        store
            .insert_note(&note(&ws2, "n-2", "more alpha here"))
            .await
            .expect("n2");
        store
            .insert_note(&note(&ws2, "n-3", "no match"))
            .await
            .expect("n3");
        let svc = Services::new(store);
        // No workspaceId param — global search.
        let r = svc
            .search_notes("alpha".into(), Some("srch-n".into()))
            .await
            .unwrap();
        assert_eq!(r["requestId"], "srch-n");
        assert_eq!(r["matches"].as_array().unwrap().len(), 2);
    }

    /// A fake [`ContextEngine`] so the engine-available and graceful-degradation
    /// paths of `search.codebase` are exercised deterministically (§8.3),
    /// independent of whether `auggie` is on the host PATH.
    struct FakeEngine {
        availability: intent_core::EngineAvailability,
        result: std::result::Result<intent_core::RetrieveResult, ()>,
    }

    #[async_trait::async_trait]
    impl intent_core::ContextEngine for FakeEngine {
        async fn availability(&self) -> intent_core::EngineAvailability {
            self.availability.clone()
        }

        async fn retrieve(
            &self,
            _req: intent_core::RetrieveRequest,
        ) -> std::result::Result<intent_core::RetrieveResult, intent_core::ContextError> {
            self.result
                .clone()
                .map_err(|()| intent_core::ContextError::Unavailable {
                    reason: "needs login".to_string(),
                })
        }
    }

    /// (a) When the context engine is available it backs `search.codebase`: the
    /// returned matches are the engine's hits (carrying their `score`), not the
    /// ripgrep/symbol output (§5.15, §8).
    #[tokio::test]
    async fn codebase_search_uses_context_engine_when_available() {
        let dir = std::env::temp_dir().join(format!("intentd-search-eng-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(dir.join("src")).unwrap();
        std::fs::write(dir.join("src/main.rs"), "fn main() {\n    let x = 1;\n}\n").unwrap();
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws = WorkspaceId::new();
        let mut w = workspace(&ws);
        w.worktree_path = Some(dir.to_string_lossy().to_string());
        store.insert_workspace(&w).await.expect("ws");
        let engine = FakeEngine {
            availability: intent_core::EngineAvailability::Available {
                name: "fake".to_string(),
                version: Some("9.9.9".to_string()),
            },
            result: Ok(intent_core::RetrieveResult {
                items: vec![intent_core::RetrievedItem {
                    file: "src/engine_hit.rs".to_string(),
                    symbol: Some("Widget".to_string()),
                    line: Some(7),
                    preview: "struct Widget".to_string(),
                    score: Some(0.87),
                }],
            }),
        };
        let svc = Services::new(store).with_context_engine(std::sync::Arc::new(engine));
        let r = svc
            .search_codebase(ws, "widget".into(), Some("srch-eng".into()))
            .await
            .unwrap();
        assert_eq!(r["requestId"], "srch-eng");
        let matches = r["matches"].as_array().unwrap();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0]["file"], "src/engine_hit.rs");
        assert_eq!(matches[0]["symbol"], "Widget");
        assert_eq!(matches[0]["line"], 7);
        assert_eq!(matches[0]["score"], 0.87);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// (b) When the engine is `Unavailable`, `search.codebase` degrades to the
    /// ripgrep/symbol path without erroring (§8.3). An injected `Unavailable`
    /// engine makes this deterministic regardless of the host PATH.
    #[tokio::test]
    async fn codebase_search_returns_symbol_matches() {
        let dir = std::env::temp_dir().join(format!("intentd-search-cb-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(dir.join("src")).unwrap();
        std::fs::write(dir.join("src/main.rs"), "fn main() {\n    let x = 1;\n}\n").unwrap();
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws = WorkspaceId::new();
        let mut w = workspace(&ws);
        w.worktree_path = Some(dir.to_string_lossy().to_string());
        store.insert_workspace(&w).await.expect("ws");
        let engine = FakeEngine {
            availability: intent_core::EngineAvailability::Unavailable {
                reason: "auggie not found on PATH".to_string(),
            },
            result: Err(()),
        };
        let svc = Services::new(store).with_context_engine(std::sync::Arc::new(engine));
        let r = svc
            .search_codebase(ws, "main".into(), Some("srch-c".into()))
            .await
            .unwrap();
        let matches = r["matches"].as_array().unwrap();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0]["file"], "src/main.rs");
        assert_eq!(matches[0]["symbol"], "main");
        assert!(matches[0]["score"].is_number());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// (c) When the engine reports `Available` (binary present — e.g. auggie on
    /// PATH) but `retrieve()` returns `Unavailable`, `search.codebase` degrades to
    /// the ripgrep/symbol path without erroring. This is the Option-A reality
    /// (M10 CE-3): auggie exposes no structured codebase-retrieval CLI, so its
    /// `retrieve()` is always `Unavailable` even while `availability()` is
    /// `Available` for `intentd doctor` (§8.3).
    #[tokio::test]
    async fn codebase_search_degrades_when_available_engine_cannot_retrieve() {
        let dir = std::env::temp_dir().join(format!("intentd-search-deg-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(dir.join("src")).unwrap();
        std::fs::write(dir.join("src/main.rs"), "fn main() {\n    let x = 1;\n}\n").unwrap();
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws = WorkspaceId::new();
        let mut w = workspace(&ws);
        w.worktree_path = Some(dir.to_string_lossy().to_string());
        store.insert_workspace(&w).await.expect("ws");
        let engine = FakeEngine {
            availability: intent_core::EngineAvailability::Available {
                name: "auggie".to_string(),
                version: Some("0.29.0".to_string()),
            },
            result: Err(()),
        };
        let svc = Services::new(store).with_context_engine(std::sync::Arc::new(engine));
        let r = svc
            .search_codebase(ws, "main".into(), Some("srch-deg".into()))
            .await
            .unwrap();
        assert_eq!(r["requestId"], "srch-deg");
        let matches = r["matches"].as_array().unwrap();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0]["file"], "src/main.rs");
        assert_eq!(matches[0]["symbol"], "main");
        assert!(matches[0]["score"].is_number());
        let _ = std::fs::remove_dir_all(&dir);
    }

    struct StreamHarness {
        _tmp: TempDb,
        store: Store,
        services: Services,
        bus: EventBus,
        ws: WorkspaceId,
    }

    async fn stream_harness() -> StreamHarness {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws = WorkspaceId::new();
        store.insert_workspace(&workspace(&ws)).await.expect("ws");
        let bus = EventBus::new(store.clone());
        let services = Services::new(store.clone()).with_event_bus(bus.clone());
        StreamHarness {
            _tmp: tmp,
            store,
            services,
            bus,
            ws,
        }
    }

    fn subscribe(h: &StreamHarness) -> Subscription {
        h.bus.subscribe(SubscriptionFilter {
            event_types: vec!["search:*".to_string()],
            workspace_id: Some(h.ws.0.clone()),
            ..Default::default()
        })
    }

    async fn recv_event(sub: &mut Subscription) -> Value {
        let batch = tokio::time::timeout(Duration::from_secs(2), sub.recv())
            .await
            .expect("event delivered")
            .expect("subscription open");
        serde_json::to_value(&batch[0]).expect("serialize")
    }

    #[tokio::test]
    async fn streaming_emits_result_batches_and_done() {
        let h = stream_harness().await;
        // 60 matching messages → above the inline threshold → streamed.
        let msgs: Vec<(&str, Value)> = (0..60)
            .map(|i| ("assistant", json!(format!("needle {i}"))))
            .collect();
        insert_session(&h.store, &h.ws, "agent-1", &msgs).await;
        let mut sub = subscribe(&h);
        let ack = h
            .services
            .search_messages(
                h.ws.clone(),
                "needle".into(),
                None,
                None,
                None,
                Some("srch-stream".into()),
            )
            .await
            .unwrap();
        // Prompt ack: requestId + empty inline matches (the real data streams).
        assert_eq!(ack["requestId"], "srch-stream");
        assert_eq!(ack["matches"].as_array().unwrap().len(), 0);

        let mut streamed = 0usize;
        loop {
            let ev = recv_event(&mut sub).await;
            assert_eq!(ev["data"]["requestId"], "srch-stream");
            match ev["type"].as_str().unwrap() {
                "search:result" => {
                    streamed += ev["data"]["matches"].as_array().unwrap().len();
                }
                "search:done" => {
                    assert_eq!(ev["data"]["total"], 60);
                    assert_eq!(ev["data"]["truncated"], false);
                    break;
                }
                other => panic!("unexpected event {other}"),
            }
        }
        assert_eq!(streamed, 60);
    }

    #[tokio::test]
    async fn cancel_mid_stream_stops_further_batches() {
        let h = stream_harness().await;
        let msgs: Vec<(&str, Value)> = (0..60)
            .map(|i| ("assistant", json!(format!("needle {i}"))))
            .collect();
        insert_session(&h.store, &h.ws, "agent-1", &msgs).await;
        let mut sub = subscribe(&h);
        h.services
            .search_messages(
                h.ws.clone(),
                "needle".into(),
                None,
                None,
                None,
                Some("srch-cancel".into()),
            )
            .await
            .unwrap();
        // Receive the first batch, then cancel mid-stream by requestId.
        let first = recv_event(&mut sub).await;
        assert_eq!(first["type"], "search:result");
        h.services
            .search_cancel("srch-cancel".into())
            .await
            .unwrap();

        // Drain until the terminal done event: cancellation truncates the stream.
        let mut done = None;
        for _ in 0..10 {
            let ev = recv_event(&mut sub).await;
            if ev["type"] == "search:done" {
                done = Some(ev);
                break;
            }
        }
        let done = done.expect("search:done delivered");
        assert_eq!(done["data"]["truncated"], true);
        assert!(done["data"]["total"].as_u64().unwrap() < 60);
    }
}

/// `terminal.*` lifecycle over a real PTY host wired into [`Services`]: stream
/// `terminal:data`/`terminal:exit` onto the bus, multi-subscriber fan-out,
/// late-attach back-fill via `getBuffer`, write/resize/kill/list (§5.13, §12).
#[cfg(unix)]
mod terminal {
    use std::time::Duration;

    use base64::Engine as _;
    use intent_core::{WorkspaceApi, WorkspaceId};
    use intent_store::Store;
    use serde_json::Value;

    use super::{workspace, TempDb};
    use crate::events::{EventBus, Subscription, SubscriptionFilter};
    use crate::Services;

    struct Harness {
        _tmp: TempDb,
        services: Services,
        bus: EventBus,
        ws: WorkspaceId,
    }

    async fn harness() -> Harness {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws = WorkspaceId::new();
        store.insert_workspace(&workspace(&ws)).await.expect("ws");
        let bus = EventBus::new(store.clone());
        let services = Services::new(store).with_event_bus(bus.clone());
        Harness {
            _tmp: tmp,
            services,
            bus,
            ws,
        }
    }

    fn subscribe(h: &Harness) -> Subscription {
        h.bus.subscribe(SubscriptionFilter {
            event_types: vec!["terminal:*".to_string()],
            workspace_id: Some(h.ws.0.clone()),
            ..Default::default()
        })
    }

    fn b64(s: &str) -> String {
        base64::engine::general_purpose::STANDARD.encode(s.as_bytes())
    }

    fn decode(s: &str) -> Vec<u8> {
        base64::engine::general_purpose::STANDARD.decode(s).unwrap()
    }

    /// Drain terminal events until one of `event_type` arrives, accumulating the
    /// decoded `terminal:data` chunks seen along the way. Returns the matching
    /// event and the accumulated data bytes.
    async fn drain_until(
        sub: &mut Subscription,
        event_type: &str,
        timeout: Duration,
    ) -> (Value, Vec<u8>) {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut data = Vec::new();
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let batch = tokio::time::timeout(remaining, sub.recv())
                .await
                .expect("terminal event delivered")
                .expect("subscription open");
            for ev in &batch {
                let v = serde_json::to_value(ev).expect("serialize");
                if v["type"] == "terminal:data" {
                    if let Some(chunk) = v["data"]["chunk"].as_str() {
                        data.extend_from_slice(&decode(chunk));
                    }
                }
                if v["type"] == event_type {
                    return (v, data);
                }
            }
        }
    }

    fn contains(haystack: &[u8], needle: &[u8]) -> bool {
        haystack.windows(needle.len()).any(|w| w == needle)
    }

    /// create → write echoes back as `terminal:data`; two subscribers see the
    /// same output; kill emits `terminal:exit`.
    #[tokio::test]
    async fn create_streams_data_to_all_subscribers_then_exit() {
        let h = harness().await;
        let mut a = subscribe(&h);
        let mut b = subscribe(&h);
        let created = h
            .services
            .terminal_create(h.ws.clone(), 80, 24, None, Some("cat".into()), None)
            .await
            .expect("create");
        let terminal_id = created["terminalId"]
            .as_str()
            .expect("terminalId")
            .to_string();

        h.services
            .terminal_write(terminal_id.clone(), b64("ping-stream\n"))
            .await
            .expect("write");

        let (_, from_a) = drain_until(&mut a, "terminal:data", Duration::from_secs(5)).await;
        let (_, from_b) = drain_until(&mut b, "terminal:data", Duration::from_secs(5)).await;
        assert!(contains(&from_a, b"ping-stream"), "subscriber a sees echo");
        assert!(contains(&from_b, b"ping-stream"), "subscriber b sees echo");

        h.services
            .terminal_kill(terminal_id.clone())
            .await
            .expect("kill");
        let (exit, _) = drain_until(&mut a, "terminal:exit", Duration::from_secs(5)).await;
        assert_eq!(exit["data"]["terminalId"], terminal_id);
    }

    /// `terminal.getBuffer` back-fills scrollback for a late attach (the echoed
    /// input survives in the server-side buffer), then `terminal.list` reports it.
    #[tokio::test]
    async fn get_buffer_backfills_and_list_reports() {
        let h = harness().await;
        let mut sub = subscribe(&h);
        let created = h
            .services
            .terminal_create(h.ws.clone(), 80, 24, None, Some("cat".into()), None)
            .await
            .expect("create");
        let terminal_id = created["terminalId"].as_str().unwrap().to_string();

        h.services
            .terminal_write(terminal_id.clone(), b64("BUFFERED\n"))
            .await
            .expect("write");
        // Wait until the echo has streamed (so it is in scrollback too).
        drain_until(&mut sub, "terminal:data", Duration::from_secs(5)).await;

        let buf = h
            .services
            .terminal_get_buffer(terminal_id.clone(), None)
            .await
            .expect("getBuffer");
        assert_eq!(buf["terminalId"], terminal_id);
        let bytes = decode(buf["data"].as_str().expect("data"));
        assert!(contains(&bytes, b"BUFFERED"), "scrollback back-fill");

        h.services
            .terminal_resize(terminal_id.clone(), 120, 40)
            .await
            .expect("resize");

        let list = h.services.terminal_list(h.ws.clone()).await.expect("list");
        let terminals = list.as_array().expect("bare terminals array");
        let entry = terminals
            .iter()
            .find(|t| t["id"].as_str() == Some(terminal_id.as_str()))
            .expect("list contains terminal");
        // Bare-array shape: { id, name, cwd, isExecutingCommand }.
        assert_eq!(entry["name"], "Terminal");
        assert!(entry["cwd"].is_string(), "cwd is a string");
        assert!(
            entry["isExecutingCommand"].is_boolean(),
            "isExecutingCommand is a boolean"
        );

        h.services.terminal_kill(terminal_id).await.expect("kill");
    }

    /// `terminal.readOutput` returns a formatted, ANSI-stripped string: a header
    /// (`Terminal {id} (cwd: ...)`), a `─`×40 separator, then the trailing lines.
    /// A freshly created terminal with no output returns the empty sentinel.
    #[tokio::test]
    async fn read_output_formats_scrollback_and_empty_sentinel() {
        let h = harness().await;
        let mut sub = subscribe(&h);
        let created = h
            .services
            .terminal_create(h.ws.clone(), 80, 24, None, Some("cat".into()), None)
            .await
            .expect("create");
        let terminal_id = created["terminalId"].as_str().unwrap().to_string();

        // No output yet -> sentinel string.
        let empty = h
            .services
            .terminal_read_output(h.ws.clone(), terminal_id.clone(), None, None, None)
            .await
            .expect("readOutput empty");
        assert_eq!(empty, serde_json::json!("Terminal has no output yet."));

        h.services
            .terminal_write(terminal_id.clone(), b64("READOUT-MARK\n"))
            .await
            .expect("write");
        drain_until(&mut sub, "terminal:data", Duration::from_secs(5)).await;

        let out = h
            .services
            .terminal_read_output(h.ws.clone(), terminal_id.clone(), Some(50), None, None)
            .await
            .expect("readOutput");
        let text = out.as_str().expect("readOutput is a bare string");
        assert!(
            text.starts_with(&format!("Terminal {terminal_id} (cwd: ")),
            "header present: {text}"
        );
        assert!(text.contains(&"\u{2500}".repeat(40)), "separator present");
        assert!(text.contains("READOUT-MARK"), "echoed output present");

        // Unknown id and cross-workspace access map to internal errors.
        let unknown = h
            .services
            .terminal_read_output(h.ws.clone(), "pty-99999".into(), None, None, None)
            .await
            .unwrap_err();
        assert!(matches!(unknown, intent_core::Error::Internal(_)));
        let other_ws = WorkspaceId::new();
        let wrong = h
            .services
            .terminal_read_output(other_ws, terminal_id.clone(), None, None, None)
            .await
            .unwrap_err();
        assert!(matches!(wrong, intent_core::Error::Internal(_)));

        h.services.terminal_kill(terminal_id).await.expect("kill");
    }

    /// A process that exits on its own surfaces its exit code on `terminal:exit`.
    #[tokio::test]
    async fn natural_exit_reports_exit_code() {
        let h = harness().await;
        let mut sub = subscribe(&h);
        // `false` exits with code 1 immediately.
        let created = h
            .services
            .terminal_create(h.ws.clone(), 80, 24, None, Some("false".into()), None)
            .await
            .expect("create");
        let terminal_id = created["terminalId"].as_str().unwrap().to_string();

        let (exit, _) = drain_until(&mut sub, "terminal:exit", Duration::from_secs(5)).await;
        assert_eq!(exit["data"]["terminalId"], terminal_id);
        assert_eq!(exit["data"]["exitCode"], 1);
    }

    /// Unknown terminal ids map to `NotFound`; bad base64 input maps to
    /// `InvalidParams`.
    #[tokio::test]
    async fn unknown_id_and_bad_base64_error() {
        let h = harness().await;
        let err = h
            .services
            .terminal_get_buffer("pty-99999".into(), None)
            .await
            .unwrap_err();
        assert!(matches!(err, intent_core::Error::NotFound(_)));

        let created = h
            .services
            .terminal_create(h.ws.clone(), 80, 24, None, Some("cat".into()), None)
            .await
            .expect("create");
        let terminal_id = created["terminalId"].as_str().unwrap().to_string();
        let err = h
            .services
            .terminal_write(terminal_id.clone(), "!!not base64!!".into())
            .await
            .unwrap_err();
        assert!(matches!(err, intent_core::Error::InvalidParams(_)));
        h.services.terminal_kill(terminal_id).await.expect("kill");
    }
}

/// `script.*` reconciled onto the same PTY host (§5.8, §12.2): command one-shot
/// `script.run`, service auto-restart/backoff, dev-server URL detection on
/// `script:state`, and a terminal attaching to a running script's PTY.
#[cfg(unix)]
mod script {
    use std::time::Duration;

    use base64::Engine as _;
    use intent_core::{ScriptCreateParams, ScriptMode, WorkspaceApi, WorkspaceId};
    use intent_store::Store;
    use serde_json::Value;

    use super::{workspace, TempDb};
    use crate::events::{EventBus, Subscription, SubscriptionFilter};
    use crate::Services;

    struct Harness {
        _tmp: TempDb,
        services: Services,
        bus: EventBus,
        ws: WorkspaceId,
    }

    async fn harness() -> Harness {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws = WorkspaceId::new();
        store.insert_workspace(&workspace(&ws)).await.expect("ws");
        let bus = EventBus::new(store.clone());
        let services = Services::new(store).with_event_bus(bus.clone());
        Harness {
            _tmp: tmp,
            services,
            bus,
            ws,
        }
    }

    fn subscribe(h: &Harness) -> Subscription {
        h.bus.subscribe(SubscriptionFilter {
            event_types: vec!["script:*".to_string()],
            workspace_id: Some(h.ws.0.clone()),
            ..Default::default()
        })
    }

    fn decode(s: &str) -> Vec<u8> {
        base64::engine::general_purpose::STANDARD.decode(s).unwrap()
    }

    fn contains(haystack: &[u8], needle: &[u8]) -> bool {
        haystack.windows(needle.len()).any(|w| w == needle)
    }

    async fn create(h: &Harness, name: &str, command: &str, mode: ScriptMode) -> String {
        let params = ScriptCreateParams {
            name: name.to_string(),
            command: command.to_string(),
            mode,
            ..Default::default()
        };
        let v = h
            .services
            .script_create(h.ws.clone(), params)
            .await
            .expect("create");
        v["id"].as_str().expect("script id").to_string()
    }

    /// Drain script events until `pred` returns `Some`, accumulating decoded
    /// `script:output` chunks along the way.
    async fn drain_until<F, T>(
        sub: &mut Subscription,
        timeout: Duration,
        mut pred: F,
    ) -> (T, Vec<u8>)
    where
        F: FnMut(&Value) -> Option<T>,
    {
        let deadline = tokio::time::Instant::now() + timeout;
        let mut data = Vec::new();
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            let batch = tokio::time::timeout(remaining, sub.recv())
                .await
                .expect("script event delivered")
                .expect("subscription open");
            for ev in &batch {
                let v = serde_json::to_value(ev).expect("serialize");
                if v["type"] == "script:output" {
                    if let Some(chunk) = v["data"]["chunk"].as_str() {
                        data.extend_from_slice(&decode(chunk));
                    }
                }
                if let Some(t) = pred(&v) {
                    return (t, data);
                }
            }
        }
    }

    /// `script.run` runs a command-mode script once and captures its output.
    #[tokio::test]
    async fn command_runs_once_and_captures_output() {
        let h = harness().await;
        let id = create(&h, "echo", "echo run-once-output", ScriptMode::Command).await;
        let out = h
            .services
            .script_run(h.ws.clone(), id, None, Some(10))
            .await
            .expect("run");
        assert_eq!(out["timedOut"], false);
        assert_eq!(out["exitCode"], 0);
        assert!(
            out["output"]
                .as_str()
                .unwrap_or("")
                .contains("run-once-output"),
            "captured output: {:?}",
            out["output"]
        );
    }

    /// `script.output` returns the buffer as a plaintext string (a `[... lines]`
    /// header + text), not an object — the ancestor/§5.8 parity contract.
    #[tokio::test]
    async fn output_returns_plaintext_buffer_with_header() {
        let h = harness().await;
        let mut sub = subscribe(&h);
        let id = create(
            &h,
            "svc",
            "echo OUTPUT-PARITY-MARK ; sleep 5",
            ScriptMode::Service,
        )
        .await;
        h.services
            .script_start(h.ws.clone(), id.clone())
            .await
            .expect("start");
        drain_until(&mut sub, Duration::from_secs(5), |v| {
            if v["type"] == "script:output" {
                let bytes = v["data"]["chunk"].as_str().map(decode).unwrap_or_default();
                contains(&bytes, b"OUTPUT-PARITY-MARK").then_some(())
            } else {
                None
            }
        })
        .await;

        let out = h
            .services
            .script_output(h.ws.clone(), id.clone(), Some(10), None, None)
            .await
            .expect("output");
        let text = out
            .as_str()
            .unwrap_or_else(|| panic!("script.output must be a string, got: {out:?}"));
        assert!(text.starts_with('['), "header line present: {text:?}");
        assert!(
            text.contains("lines]"),
            "header is a [... lines] line: {text:?}"
        );
        assert!(
            text.contains("OUTPUT-PARITY-MARK"),
            "buffer text included: {text:?}"
        );
        h.services
            .script_stop(h.ws.clone(), id)
            .await
            .expect("stop");
    }

    /// `script.output` on a script that never produced output returns the bare
    /// `"No output yet."` string (ancestor empty-buffer case).
    #[tokio::test]
    async fn output_empty_buffer_returns_placeholder() {
        let h = harness().await;
        let id = create(&h, "idle", "echo never-started", ScriptMode::Command).await;
        let out = h
            .services
            .script_output(h.ws.clone(), id, None, None, None)
            .await
            .expect("output");
        assert_eq!(out, Value::String("No output yet.".to_string()));
    }

    /// A service that exits faster than the 2s floor is treated as a config error
    /// and is NOT auto-restarted (the ported backoff guard).
    ///
    /// Deterministic against scheduling jitter: after observing `exited`, keep
    /// draining `script:state` events across a window that comfortably exceeds
    /// `AUTO_RESTART_DELAY` and assert no second `running` arrives. A spurious
    /// restart shows up as an event (failure with a clear message) rather than
    /// hiding behind a wall-clock poll.
    #[tokio::test]
    async fn service_too_fast_exit_does_not_restart() {
        let h = harness().await;
        let mut sub = subscribe(&h);
        let id = create(&h, "boom", "echo boom", ScriptMode::Service).await;
        h.services
            .script_start(h.ws.clone(), id.clone())
            .await
            .expect("start");
        drain_until(&mut sub, Duration::from_secs(5), |v| {
            (v["type"] == "script:state" && v["data"]["status"] == "exited").then_some(())
        })
        .await;
        // Observation window > AUTO_RESTART_DELAY (1s) so any restart attempt
        // emits a `running` `script:state` we'd catch deterministically.
        let deadline = tokio::time::Instant::now() + Duration::from_millis(3000);
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, sub.recv()).await {
                Err(_) => break,
                Ok(None) => break,
                Ok(Some(batch)) => {
                    for ev in &batch {
                        let v = serde_json::to_value(ev).expect("serialize");
                        if v["type"] == "script:state" && v["data"]["status"] == "running" {
                            panic!(
                                "too-fast-exit service must NOT auto-restart; saw second `running` script:state: {v}",
                            );
                        }
                    }
                }
            }
        }
        let st = h
            .services
            .script_status(h.ws.clone(), id)
            .await
            .expect("status");
        assert_eq!(st["status"], "exited");
        assert_eq!(st["restartCount"], 0);
    }

    /// A service that runs past the 2s floor before exiting auto-restarts once,
    /// surfacing `restartCount: 1` on the next `running` `script:state`.
    #[tokio::test]
    async fn service_auto_restarts_after_long_enough_run() {
        let h = harness().await;
        let mut sub = subscribe(&h);
        let id = create(&h, "svc", "sleep 2.1", ScriptMode::Service).await;
        h.services
            .script_start(h.ws.clone(), id.clone())
            .await
            .expect("start");
        drain_until(&mut sub, Duration::from_secs(12), |v| {
            (v["type"] == "script:state"
                && v["data"]["status"] == "running"
                && v["data"]["restartCount"] == 1)
                .then_some(())
        })
        .await;
        let st = h
            .services
            .script_status(h.ws.clone(), id.clone())
            .await
            .expect("status");
        assert_eq!(st["restartCount"], 1);
        h.services
            .script_stop(h.ws.clone(), id)
            .await
            .expect("stop");
    }

    /// A service whose output prints a local dev-server URL surfaces it on
    /// `script:state` as `detectedUrl` (the `forward.*` hook).
    #[tokio::test]
    async fn service_url_detection_emits_state() {
        let h = harness().await;
        let mut sub = subscribe(&h);
        let id = create(
            &h,
            "dev",
            "echo listening on http://localhost:3000/ ; sleep 5",
            ScriptMode::Service,
        )
        .await;
        h.services
            .script_start(h.ws.clone(), id.clone())
            .await
            .expect("start");
        let (url, _) = drain_until(&mut sub, Duration::from_secs(5), |v| {
            if v["type"] == "script:state" {
                v["data"]["detectedUrl"].as_str().map(str::to_string)
            } else {
                None
            }
        })
        .await;
        assert!(url.contains("localhost:3000"), "detected url: {url}");
        h.services
            .script_stop(h.ws.clone(), id)
            .await
            .expect("stop");
    }

    /// The unified host: a running script's PTY is visible to `terminal.list` and
    /// its scrollback is readable via `terminal.getBuffer` — a terminal attaching
    /// to a live script (§12.2).
    #[tokio::test]
    async fn terminal_attaches_to_running_script_pty() {
        let h = harness().await;
        let mut sub = subscribe(&h);
        let id = create(
            &h,
            "svc",
            "echo SCRIPT-PTY-MARK ; sleep 5",
            ScriptMode::Service,
        )
        .await;
        h.services
            .script_start(h.ws.clone(), id.clone())
            .await
            .expect("start");
        // Wait until the marker has streamed (so it is in the PTY scrollback).
        drain_until(&mut sub, Duration::from_secs(5), |v| {
            if v["type"] == "script:output" {
                let bytes = v["data"]["chunk"].as_str().map(decode).unwrap_or_default();
                contains(&bytes, b"SCRIPT-PTY-MARK").then_some(())
            } else {
                None
            }
        })
        .await;

        // The script's PTY appears in the workspace's terminal list...
        let list = h.services.terminal_list(h.ws.clone()).await.expect("list");
        let term_id = list
            .as_array()
            .expect("bare terminals array")
            .iter()
            .filter_map(|t| t["id"].as_str())
            .next()
            .expect("script PTY listed as a terminal")
            .to_string();

        // ...and a terminal reads its scrollback (attach to a running script).
        let buf = h
            .services
            .terminal_get_buffer(term_id, None)
            .await
            .expect("getBuffer");
        let bytes = decode(buf["data"].as_str().expect("data"));
        assert!(
            contains(&bytes, b"SCRIPT-PTY-MARK"),
            "terminal reads the running script's PTY output"
        );
        h.services
            .script_stop(h.ws.clone(), id)
            .await
            .expect("stop");
    }
}

/// `rules.*` user-override CRUD over the `endUserRules` settings store + the
/// internal prompt-assembly/injection pipeline (§18.1, PROTOCOL §5.21).
mod rules {
    use std::time::Duration;

    use intent_core::{Error, WorkspaceApi, WorkspaceId};
    use intent_store::Store;
    use serde_json::Value;

    use super::{workspace, TempDb};
    use crate::{EventBus, Services, Subscription, SubscriptionFilter};

    struct TempTree(std::path::PathBuf);
    impl Drop for TempTree {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// A worktree containing a `CLAUDE.md` workspace rule file.
    fn worktree() -> TempTree {
        let dir = std::env::temp_dir().join(format!("intentd-rules-svc-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("CLAUDE.md"), "ALWAYS run the linter.").unwrap();
        TempTree(dir)
    }

    async fn setup(dir: &std::path::Path) -> (TempDb, Store, Services, WorkspaceId) {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws_id = WorkspaceId::new();
        let mut ws = workspace(&ws_id);
        ws.worktree_path = Some(dir.to_string_lossy().to_string());
        store.insert_workspace(&ws).await.expect("ws");
        let svc = Services::new(store.clone());
        (tmp, store, svc, ws_id)
    }

    /// Find the entry in a `{ rules: { rules: [...] } }` result by ruleType.
    fn entry<'a>(set: &'a Value, rule_type: &str) -> Option<&'a Value> {
        set["rules"]["rules"]
            .as_array()?
            .iter()
            .find(|e| e["ruleType"] == rule_type && e["source"] == "user-override")
    }

    #[tokio::test]
    async fn update_then_get_roundtrips() {
        let tree = worktree();
        let (_tmp, _store, svc, ws) = setup(&tree.0).await;
        svc.rules_update(
            ws.clone(),
            "base-system-prompt".into(),
            "Be concise.".into(),
            None,
        )
        .await
        .expect("update");

        let got = svc
            .rules_get(ws, "base-system-prompt".into())
            .await
            .expect("get");
        assert_eq!(got["content"], "Be concise.");
        assert_eq!(got["enabled"], true);
        assert!(got["updatedAt"].as_i64().unwrap() > 0);
    }

    #[tokio::test]
    async fn get_absent_type_reads_disabled_empty() {
        let tree = worktree();
        let (_tmp, _store, svc, ws) = setup(&tree.0).await;
        let got = svc.rules_get(ws, "workspace".into()).await.expect("get");
        assert_eq!(got["enabled"], false);
        assert_eq!(got["content"], "");
        assert_eq!(got["updatedAt"], 0);
    }

    #[tokio::test]
    async fn list_returns_editable_override_and_readonly_file() {
        let tree = worktree();
        let (_tmp, _store, svc, ws) = setup(&tree.0).await;
        // Upsert returns the re-read set directly (no extra fetch).
        let set = svc
            .rules_update(
                ws.clone(),
                "workspace".into(),
                "Prefer small PRs.".into(),
                None,
            )
            .await
            .expect("update");
        assert_eq!(set["rules"]["workspaceId"], ws.as_str());

        let listed = svc.rules_list(Some(ws)).await.expect("list");
        let user = entry(&listed, "workspace").expect("user-override entry");
        assert_eq!(user["editable"], true);
        assert_eq!(user["content"], "Prefer small PRs.");

        // The live CLAUDE.md surfaces as a read-only file-sourced entry.
        let file = listed["rules"]["rules"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["source"] == "CLAUDE.md")
            .expect("CLAUDE.md entry");
        assert_eq!(file["editable"], false);
        assert_eq!(file["ruleType"], "workspace");
        assert!(file["path"].as_str().unwrap().ends_with("CLAUDE.md"));
    }

    #[tokio::test]
    async fn over_long_content_rejected() {
        let tree = worktree();
        let (_tmp, _store, svc, ws) = setup(&tree.0).await;
        let huge = "x".repeat(50_001);
        let err = svc
            .rules_update(ws, "base-system-prompt".into(), huge, None)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidParams(_)));
    }

    #[tokio::test]
    async fn assembly_layers_all_sources_in_precedence() {
        let tree = worktree();
        let (_tmp, store, svc, ws) = setup(&tree.0).await;
        // base → specialization(agent type) → workspace → live workspace files.
        svc.rules_update(
            ws.clone(),
            "base-system-prompt".into(),
            "BASE_BODY".into(),
            None,
        )
        .await
        .unwrap();
        svc.rules_update(ws.clone(), "task-loop".into(), "SPEC_BODY".into(), None)
            .await
            .unwrap();
        svc.rules_update(
            ws.clone(),
            "workspace".into(),
            "WS_OVERRIDE_BODY".into(),
            None,
        )
        .await
        .unwrap();

        let prompt = crate::rules::assemble_system_prompt(
            &store,
            Some(&tree.0),
            "task-loop",
            None,
            false,
            false,
            None,
            None,
        )
        .await
        .expect("assembled prompt");

        let base = prompt.find("BASE_BODY").expect("base body");
        let spec = prompt.find("SPEC_BODY").expect("specialization body");
        let wsov = prompt
            .find("WS_OVERRIDE_BODY")
            .expect("workspace override body");
        let file = prompt
            .find("ALWAYS run the linter.")
            .expect("CLAUDE.md body");
        assert!(prompt.contains("## User Rules & Guidelines"));
        assert!(
            base < spec && spec < wsov && wsov < file,
            "precedence order"
        );
    }

    #[tokio::test]
    async fn disabled_override_excluded_from_assembly() {
        let tree = worktree();
        let (_tmp, store, svc, ws) = setup(&tree.0).await;
        svc.rules_update(
            ws.clone(),
            "base-system-prompt".into(),
            "HIDDEN_BODY".into(),
            Some(false),
        )
        .await
        .unwrap();
        let prompt = crate::rules::assemble_system_prompt(
            &store,
            Some(&tree.0),
            "task-loop",
            None,
            false,
            false,
            None,
            None,
        )
        .await
        .expect("assembled prompt (file still applies)");
        assert!(!prompt.contains("HIDDEN_BODY"));
        assert!(prompt.contains("ALWAYS run the linter."));
    }

    #[tokio::test]
    async fn specialization_tier3_bundled_when_no_override_or_file() {
        let tree = worktree();
        let (_tmp, store, _svc, _ws) = setup(&tree.0).await;
        // No override and no `.augment/agent-rules/task-loop.md` → bundled built-in,
        // composed as common + workspace + specific (task-loop is a workspace agent).
        let rules =
            crate::rules::get_specialization_rules(&store, Some(&tree.0), "task-loop").await;
        assert!(rules.contains("# Task Loop Agent"), "bundled specific body");
        assert!(rules.contains("## Delegating Tasks"), "common layer");
        assert!(rules.contains("# Space"), "workspace layer");
    }

    #[tokio::test]
    async fn specialization_tier2_workspace_file_overrides_bundled() {
        let tree = worktree();
        let (_tmp, store, _svc, _ws) = setup(&tree.0).await;
        let dir = tree.0.join(".augment").join("agent-rules");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("task-loop.md"), "WORKSPACE_FILE_RULES").unwrap();
        let rules =
            crate::rules::get_specialization_rules(&store, Some(&tree.0), "task-loop").await;
        assert_eq!(rules, "WORKSPACE_FILE_RULES");
    }

    #[tokio::test]
    async fn specialization_tier1_override_wins_over_file() {
        let tree = worktree();
        let (_tmp, store, svc, ws) = setup(&tree.0).await;
        let dir = tree.0.join(".augment").join("agent-rules");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("task-loop.md"), "WORKSPACE_FILE_RULES").unwrap();
        // Settings override wins over both the workspace file and the bundled default.
        svc.rules_update(ws, "task-loop".into(), "OVERRIDE_RULES".into(), None)
            .await
            .unwrap();
        let rules =
            crate::rules::get_specialization_rules(&store, Some(&tree.0), "task-loop").await;
        assert_eq!(rules, "OVERRIDE_RULES");
    }

    #[tokio::test]
    async fn specialization_unknown_type_falls_back_to_workspace_body() {
        let tree = worktree();
        let (_tmp, store, _svc, _ws) = setup(&tree.0).await;
        // The spawn default `interactive` is an unknown instruction id →
        // fallbackToWorkspace (common + workspace + workspace).
        let rules =
            crate::rules::get_specialization_rules(&store, Some(&tree.0), "interactive").await;
        assert!(rules.contains("# Space"), "workspace body present");
        assert!(rules.contains("## Delegating Tasks"), "common prepended");
    }

    #[tokio::test]
    async fn assembly_uses_bundled_specialization_when_no_override() {
        let tree = worktree();
        let (_tmp, store, _svc, _ws) = setup(&tree.0).await;
        // Assemble as a sub-agent so the SP-1 Suggested Next Steps directive
        // stays out and this test remains focused on specialization precedence.
        let prompt = crate::rules::assemble_system_prompt(
            &store,
            Some(&tree.0),
            "task-loop",
            None,
            true,
            false,
            None,
            None,
        )
        .await
        .expect("assembled prompt");
        // Bundled specialization is injected, and the live workspace file still applies.
        assert!(
            prompt.contains("# Task Loop Agent"),
            "bundled specialization"
        );
        assert!(
            prompt.contains("ALWAYS run the linter."),
            "live workspace file"
        );
        // Non-specialist assembly (no injection): no specialist artifacts.
        assert!(!prompt.contains("<specialist_role>"));
        assert!(!prompt.contains("## Role Reminder"));
    }

    #[tokio::test]
    async fn assembly_injects_specialist_role_section_and_footer() {
        let tree = worktree();
        let (_tmp, store, svc, ws) = setup(&tree.0).await;
        svc.rules_update(
            ws.clone(),
            "base-system-prompt".into(),
            "BASE_BODY".into(),
            None,
        )
        .await
        .unwrap();
        let injection = crate::rules::SpecialistPromptInjection {
            behavior_prompt: Some("Implement your assigned task.".into()),
            specialist_name: Some("Implementor".into()),
            role_reminder: Some("Stay in scope.".into()),
        };
        // Assemble as a sub-agent so this test asserts only the specialist
        // section + Role Reminder footer (no Suggested Next Steps directive).
        let prompt = crate::rules::assemble_system_prompt(
            &store,
            Some(&tree.0),
            "task-loop",
            Some(&injection),
            true,
            false,
            None,
            None,
        )
        .await
        .expect("assembled prompt");

        // Section wraps the behavior prompt in <specialist_role> tags, placed
        // after specialization and user rules (reference layer 4.8).
        let base = prompt.find("BASE_BODY").expect("base body");
        let spec = prompt.find("# Task Loop Agent").expect("specialization");
        let rules = prompt
            .find("ALWAYS run the linter.")
            .expect("workspace rules");
        let role = prompt
            .find("# Your Specialist Role")
            .expect("specialist role section");
        assert!(base < spec && spec < rules && rules < role, "section order");
        assert!(
            prompt.contains("<specialist_role>\nImplement your assigned task.\n</specialist_role>")
        );
        assert!(
            prompt.contains("The instructions in <specialist_role> define your primary function.")
        );
        // Role-reminder footer sits at the very end (recency) for sub-agents.
        assert!(
            prompt.ends_with("## Role Reminder\n\nYou are a Implementor. Stay in scope."),
            "footer at end: {:?}",
            &prompt[prompt.len().saturating_sub(120)..]
        );
    }

    #[tokio::test]
    async fn assembly_footer_falls_back_without_role_reminder() {
        let tree = worktree();
        let (_tmp, store, _svc, _ws) = setup(&tree.0).await;
        let injection = crate::rules::SpecialistPromptInjection {
            behavior_prompt: Some("Verify things.".into()),
            specialist_name: Some("Verifier".into()),
            role_reminder: None,
        };
        // Sub-agent so the fallback reminder is still last in the prompt.
        let prompt = crate::rules::assemble_system_prompt(
            &store,
            Some(&tree.0),
            "task-loop",
            Some(&injection),
            true,
            false,
            None,
            None,
        )
        .await
        .expect("assembled prompt");
        assert!(prompt.ends_with(
            "## Role Reminder\n\nYou are a Verifier. Follow the instructions in <specialist_role> above."
        ));
    }

    /// SP-1: top-level (non-sub-agent) interactive agents get the
    /// `## Suggested Next Steps` directive at the very end of the assembled
    /// prompt so the model reliably emits a `<!-- suggested-prompts ... -->`
    /// block. The default (`auto_commit_enabled = false`) variant uses the
    /// "Review changes before committing." example line and appends no
    /// auto-commit clause.
    #[tokio::test]
    async fn assembly_appends_suggested_prompts_for_top_level_agent() {
        let tree = worktree();
        let (_tmp, store, _svc, _ws) = setup(&tree.0).await;
        let prompt = crate::rules::assemble_system_prompt(
            &store,
            Some(&tree.0),
            "task-loop",
            None,
            false,
            false,
            None,
            None,
        )
        .await
        .expect("assembled prompt");
        assert!(
            prompt.contains("## Suggested Next Steps"),
            "SP-1 directive present for non-sub-agent"
        );
        assert!(
            prompt.contains("Review changes before committing."),
            "auto-commit-off example line"
        );
        assert!(
            !prompt.contains("Auto-commit is enabled;"),
            "no auto-commit clause when auto-commit is off"
        );
        // Recency: the directive sits at the very end of the prompt.
        // Debug tail walks forward to the next char boundary so the slice
        // never lands mid-multi-byte (the Suggested Next Steps block includes
        // non-ASCII like the en-dash in "2–4").
        let tail_from = prompt.len().saturating_sub(200);
        let tail_start = (tail_from..=prompt.len())
            .find(|i| prompt.is_char_boundary(*i))
            .unwrap_or(0);
        assert!(
            prompt
                .trim_end()
                .ends_with("something the user might say next."),
            "suggested-prompts block is the tail of the assembled prompt: {:?}",
            &prompt[tail_start..]
        );
    }

    /// SP-1: sub-agents (delegated children or background workers) skip the
    /// `## Suggested Next Steps` directive entirely — they report to a parent,
    /// not to a user-facing chat turn. This matches the reference `isSubAgent`
    /// gate in `getMandatoryActionsFooter`.
    #[tokio::test]
    async fn assembly_omits_suggested_prompts_for_sub_agent() {
        let tree = worktree();
        let (_tmp, store, _svc, _ws) = setup(&tree.0).await;
        let prompt = crate::rules::assemble_system_prompt(
            &store,
            Some(&tree.0),
            "task-loop",
            None,
            true,
            false,
            None,
            None,
        )
        .await
        .expect("assembled prompt");
        assert!(
            !prompt.contains("## Suggested Next Steps"),
            "SP-1 directive absent for sub-agent"
        );
        assert!(
            !prompt.contains("<!-- suggested-prompts"),
            "no suggested-prompts template for sub-agent"
        );
    }

    /// SP-1: when auto-commit is enabled the directive swaps the example
    /// second-line (`Check the changes in the diff view.`) and appends the
    /// auto-commit clause that tells the model not to propose commit-review
    /// prompts, matching the reference `autoCommitEnabled` branch.
    #[tokio::test]
    async fn assembly_suggested_prompts_toggles_auto_commit_clause() {
        let tree = worktree();
        let (_tmp, store, _svc, _ws) = setup(&tree.0).await;
        let prompt = crate::rules::assemble_system_prompt(
            &store,
            Some(&tree.0),
            "task-loop",
            None,
            false,
            true,
            None,
            None,
        )
        .await
        .expect("assembled prompt");
        assert!(prompt.contains("## Suggested Next Steps"));
        assert!(
            prompt.contains("Check the changes in the diff view."),
            "auto-commit-on example line"
        );
        assert!(
            !prompt.contains("Review changes before committing."),
            "auto-commit-off example line must not appear when auto-commit is on"
        );
        assert!(
            prompt.contains(
                "Auto-commit is enabled; do not include prompts about committing or reviewing changes before committing."
            ),
            "auto-commit clause appended"
        );
    }

    #[tokio::test]
    async fn update_emits_settings_changed() {
        let tree = worktree();
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws = WorkspaceId::new();
        let mut w = workspace(&ws);
        w.worktree_path = Some(tree.0.to_string_lossy().to_string());
        store.insert_workspace(&w).await.expect("ws");
        let bus = EventBus::new(store.clone());
        let svc = Services::new(store.clone()).with_event_bus(bus.clone());
        // Global event: subscribe without a workspace filter.
        let mut sub = bus.subscribe(SubscriptionFilter::default());

        svc.rules_update(ws, "base-system-prompt".into(), "Be concise.".into(), None)
            .await
            .expect("update");

        let batch = tokio::time::timeout(Duration::from_secs(2), recv(&mut sub))
            .await
            .expect("event delivered");
        let ev = serde_json::to_value(&batch[0]).unwrap();
        assert_eq!(ev["type"], "settings:changed");
        let changes = ev["data"]["changes"].as_array().expect("changes");
        assert_eq!(changes[0]["path"], "endUserRules");
        assert_eq!(
            changes[0]["value"]["base-system-prompt"]["content"],
            "Be concise."
        );
    }

    #[tokio::test]
    async fn assembly_omits_rtk_line_when_disabled() {
        let tree = worktree();
        let (_tmp, store, _svc, _ws) = setup(&tree.0).await;

        // rtk.enabled defaults to false, don't set it

        let prompt = crate::rules::assemble_system_prompt(
            &store,
            Some(&tree.0),
            "task-loop",
            None,
            false,
            false,
            None,
            None,
        )
        .await
        .expect("assembled prompt");

        // Assert RTK line is NOT present (regression guarantee)
        assert!(
            !prompt.contains("Prefix these commands with rtk"),
            "RTK line should not appear when disabled"
        );
    }

    async fn recv(sub: &mut Subscription) -> Vec<intent_core::Event> {
        sub.recv().await.expect("subscription open")
    }

    /// Task 6: sandboxed implementor sessions get an isolation context block
    /// with sandbox path, branch, and conflict-resolution instructions.
    #[tokio::test]
    async fn assembly_injects_sandbox_context_for_sandboxed_implementor() {
        let tree = worktree();
        let (_tmp, store, _svc, _ws) = setup(&tree.0).await;

        let injection = crate::rules::SpecialistPromptInjection {
            behavior_prompt: Some("Implement your task.".into()),
            specialist_name: Some("Implementor".into()),
            role_reminder: Some("Stay in scope.".into()),
        };

        // Create a mock workspace (direct mode + CoW supported)
        let workspace = intent_core::Workspace {
            id: intent_core::WorkspaceId::from("ws-1"),
            title: "Test".into(),
            branch: "main".into(),
            base_ref: None,
            base_commit_sha: None,
            status: intent_core::WorkspaceStatus::Active,
            status_message: None,
            activity: intent_core::WorkspaceActivity::Idle,
            attention: intent_core::WorkspaceAttention::None,
            created_at: "2026-01-01T00:00:00Z".into(),
            updated_at: "2026-01-01T00:00:00Z".into(),
            last_activity: None,
            tags: vec![],
            path: Some("/test/path".into()),
            repository_path: Some("/test/repo".into()),
            repository_owner: None,
            repository_name: Some("test-repo".into()),
            worktree_path: None,
            scope: None,
            skip_worktree: true,
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
            cow_supported: Some(true),
        };

        // Create a mock agent session with sandbox fields
        let agent_session = intent_core::AgentSession {
            id: intent_core::AgentId::from("agent-1"),
            workspace_id: intent_core::WorkspaceId::from("ws-1"),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: "Test Agent".into(),
            name_explicitly_set: false,
            model: None,
            provider: None,
            system_prompt: None,
            specialist: Some("implementor".into()),
            status: intent_core::AgentStatus::Active,
            is_active: false,
            messages: vec![],
            stats: None,
            task_note_id: None,
            skip_auto_commit: false,
            completion_report: None,
            completion_report_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            sandbox_id: Some("sandbox-123".into()),
            sandbox_path: Some("/test/sandboxes/agent-1/test-repo".into()),
            sandbox_branch: Some("sb/agent-1".into()),
            stop_reason: None,
            is_background: false,
            metadata: None,
            created_at: "2026-01-01T00:00:00Z".into(),
            updated_at: "2026-01-01T00:00:00Z".into(),
        };

        let prompt = crate::rules::assemble_system_prompt(
            &store,
            Some(&tree.0),
            "task-loop",
            Some(&injection),
            true,
            false,
            Some(&workspace),
            Some(&agent_session),
        )
        .await
        .expect("assembled prompt");

        assert!(
            prompt.contains("## Workspace Isolation"),
            "isolation header"
        );
        assert!(
            prompt.contains("isolated CoW (copy-on-write) sandbox"),
            "CoW mention"
        );
        assert!(
            prompt.contains("/test/sandboxes/agent-1/test-repo"),
            "sandbox path"
        );
        assert!(prompt.contains("sb/agent-1"), "sandbox branch");
        assert!(prompt.contains("Do NOT switch branches"), "branch warning");
        assert!(
            prompt.contains("woken with the conflicting paths"),
            "conflict bounce"
        );
        assert!(
            prompt.contains("resolve the conflicts **in your sandbox only**"),
            "sandbox-only fix"
        );
    }

    /// Task 6: coordinator (spec-writer specialist) in CoW-enabled direct-mode
    /// workspace gets parallel delegation safety guidance.
    #[tokio::test]
    async fn assembly_injects_parallel_delegation_hint_for_coordinator() {
        let tree = worktree();
        let (_tmp, store, _svc, _ws) = setup(&tree.0).await;

        let injection = crate::rules::SpecialistPromptInjection {
            behavior_prompt: Some("Plan and delegate.".into()),
            specialist_name: Some("Coordinator".into()),
            role_reminder: Some("Delegate to implementors.".into()),
        };

        // Create a mock workspace (direct mode + CoW supported)
        let workspace = intent_core::Workspace {
            id: intent_core::WorkspaceId::from("ws-1"),
            title: "Test".into(),
            branch: "main".into(),
            base_ref: None,
            base_commit_sha: None,
            status: intent_core::WorkspaceStatus::Active,
            status_message: None,
            activity: intent_core::WorkspaceActivity::Idle,
            attention: intent_core::WorkspaceAttention::None,
            created_at: "2026-01-01T00:00:00Z".into(),
            updated_at: "2026-01-01T00:00:00Z".into(),
            last_activity: None,
            tags: vec![],
            path: Some("/test/path".into()),
            repository_path: Some("/test/repo".into()),
            repository_owner: None,
            repository_name: Some("test-repo".into()),
            worktree_path: None,
            scope: None,
            skip_worktree: true,
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
            cow_supported: Some(true),
        };

        // Coordinator session (no sandbox fields — coordinators don't run in sandboxes)
        let agent_session = intent_core::AgentSession {
            id: intent_core::AgentId::from("agent-coordinator"),
            workspace_id: intent_core::WorkspaceId::from("ws-1"),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: "Coordinator Agent".into(),
            name_explicitly_set: false,
            model: None,
            provider: None,
            system_prompt: None,
            specialist: Some("spec-writer".into()),
            status: intent_core::AgentStatus::Active,
            is_active: false,
            messages: vec![],
            stats: None,
            task_note_id: None,
            skip_auto_commit: false,
            completion_report: None,
            completion_report_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
            is_background: false,
            metadata: None,
            created_at: "2026-01-01T00:00:00Z".into(),
            updated_at: "2026-01-01T00:00:00Z".into(),
        };

        let prompt = crate::rules::assemble_system_prompt(
            &store,
            Some(&tree.0),
            "task-loop",
            Some(&injection),
            true,
            false,
            Some(&workspace),
            Some(&agent_session),
        )
        .await
        .expect("assembled prompt");

        assert!(
            prompt.contains("## Agent Delegation & Isolation"),
            "delegation header"
        );
        assert!(prompt.contains("isolated CoW sandboxes"), "sandbox mention");
        assert!(
            prompt.contains("parallel delegation is safe"),
            "parallel safety"
        );
        assert!(prompt.contains("Merge-back is automatic"), "auto merge");
        assert!(
            prompt.contains("only handle `blocked` outcomes"),
            "coordinator scope"
        );
    }

    /// Task 6: worktree-mode workspaces get no isolation hints (behavior unchanged).
    #[tokio::test]
    async fn assembly_omits_hints_for_worktree_mode() {
        let tree = worktree();
        let (_tmp, store, _svc, _ws) = setup(&tree.0).await;

        let injection = crate::rules::SpecialistPromptInjection {
            behavior_prompt: Some("Implement your task.".into()),
            specialist_name: Some("Implementor".into()),
            role_reminder: Some("Stay in scope.".into()),
        };

        // Worktree-mode workspace (worktree_path present)
        let workspace = intent_core::Workspace {
            id: intent_core::WorkspaceId::from("ws-1"),
            title: "Test".into(),
            branch: "main".into(),
            base_ref: None,
            base_commit_sha: None,
            status: intent_core::WorkspaceStatus::Active,
            status_message: None,
            activity: intent_core::WorkspaceActivity::Idle,
            attention: intent_core::WorkspaceAttention::None,
            created_at: "2026-01-01T00:00:00Z".into(),
            updated_at: "2026-01-01T00:00:00Z".into(),
            last_activity: None,
            tags: vec![],
            path: Some("/test/worktree".into()),
            repository_path: Some("/test/repo".into()),
            repository_owner: None,
            repository_name: Some("test-repo".into()),
            worktree_path: Some("/test/worktree".into()), // Worktree mode!
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
            cow_supported: None, // Not computed for worktree mode
        };

        let agent_session = intent_core::AgentSession {
            id: intent_core::AgentId::from("agent-1"),
            workspace_id: intent_core::WorkspaceId::from("ws-1"),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: "Test Agent".into(),
            name_explicitly_set: false,
            model: None,
            provider: None,
            system_prompt: None,
            specialist: Some("implementor".into()),
            status: intent_core::AgentStatus::Active,
            is_active: false,
            messages: vec![],
            stats: None,
            task_note_id: None,
            skip_auto_commit: false,
            completion_report: None,
            completion_report_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
            is_background: false,
            metadata: None,
            created_at: "2026-01-01T00:00:00Z".into(),
            updated_at: "2026-01-01T00:00:00Z".into(),
        };

        let prompt = crate::rules::assemble_system_prompt(
            &store,
            Some(&tree.0),
            "task-loop",
            Some(&injection),
            true,
            false,
            Some(&workspace),
            Some(&agent_session),
        )
        .await
        .expect("assembled prompt");

        // No isolation hints in worktree mode
        assert!(
            !prompt.contains("## Workspace Isolation"),
            "no isolation header"
        );
        assert!(
            !prompt.contains("## Agent Delegation & Isolation"),
            "no delegation header"
        );
        assert!(!prompt.contains("CoW sandbox"), "no CoW mention");
    }

    /// Task 6: shared-mode direct workspace (CoW unsupported) gets no hints.
    #[tokio::test]
    async fn assembly_omits_hints_for_shared_mode_direct() {
        let tree = worktree();
        let (_tmp, store, _svc, _ws) = setup(&tree.0).await;

        let injection = crate::rules::SpecialistPromptInjection {
            behavior_prompt: Some("Implement your task.".into()),
            specialist_name: Some("Implementor".into()),
            role_reminder: Some("Stay in scope.".into()),
        };

        // Direct-mode workspace but CoW unsupported
        let workspace = intent_core::Workspace {
            id: intent_core::WorkspaceId::from("ws-1"),
            title: "Test".into(),
            branch: "main".into(),
            base_ref: None,
            base_commit_sha: None,
            status: intent_core::WorkspaceStatus::Active,
            status_message: None,
            activity: intent_core::WorkspaceActivity::Idle,
            attention: intent_core::WorkspaceAttention::None,
            created_at: "2026-01-01T00:00:00Z".into(),
            updated_at: "2026-01-01T00:00:00Z".into(),
            last_activity: None,
            tags: vec![],
            path: Some("/test/path".into()),
            repository_path: Some("/test/repo".into()),
            repository_owner: None,
            repository_name: Some("test-repo".into()),
            worktree_path: None,
            scope: None,
            skip_worktree: true,
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
            cow_supported: Some(false), // CoW not supported!
        };

        let agent_session = intent_core::AgentSession {
            id: intent_core::AgentId::from("agent-1"),
            workspace_id: intent_core::WorkspaceId::from("ws-1"),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: "Test Agent".into(),
            name_explicitly_set: false,
            model: None,
            provider: None,
            system_prompt: None,
            specialist: Some("implementor".into()),
            status: intent_core::AgentStatus::Active,
            is_active: false,
            messages: vec![],
            stats: None,
            task_note_id: None,
            skip_auto_commit: false,
            completion_report: None,
            completion_report_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
            is_background: false,
            metadata: None,
            created_at: "2026-01-01T00:00:00Z".into(),
            updated_at: "2026-01-01T00:00:00Z".into(),
        };

        let prompt = crate::rules::assemble_system_prompt(
            &store,
            Some(&tree.0),
            "task-loop",
            Some(&injection),
            true,
            false,
            Some(&workspace),
            Some(&agent_session),
        )
        .await
        .expect("assembled prompt");

        // No isolation hints when CoW is unsupported
        assert!(
            !prompt.contains("## Workspace Isolation"),
            "no isolation header"
        );
        assert!(!prompt.contains("CoW sandbox"), "no CoW mention");
    }

    /// Task 6 (verifier requirement): explicit isolation:"shared" override must prevent
    /// sandbox hint even when workspace has cow_supported=true and setting ON. This
    /// mutation test ensures build_isolation_hint keys off session.sandbox_path (actual
    /// effective isolation), not workspace.cow_supported (capability).
    #[tokio::test]
    async fn assembly_omits_sandbox_hint_when_explicit_shared_override() {
        let tree = worktree();
        let (_tmp, store, _svc, _ws) = setup(&tree.0).await;

        let injection = crate::rules::SpecialistPromptInjection {
            behavior_prompt: Some("Implement your task.".into()),
            specialist_name: Some("Implementor".into()),
            role_reminder: Some("Stay in scope.".into()),
        };

        // Workspace with cow_supported=true (capability present)
        let workspace = intent_core::Workspace {
            id: intent_core::WorkspaceId::from("ws-1"),
            title: "Test".into(),
            branch: "main".into(),
            base_ref: None,
            base_commit_sha: None,
            status: intent_core::WorkspaceStatus::Active,
            status_message: None,
            activity: intent_core::WorkspaceActivity::Idle,
            attention: intent_core::WorkspaceAttention::None,
            created_at: "2026-01-01T00:00:00Z".into(),
            updated_at: "2026-01-01T00:00:00Z".into(),
            last_activity: None,
            tags: vec![],
            path: Some("/test/path".into()),
            repository_path: Some("/test/repo".into()),
            repository_owner: None,
            repository_name: Some("test-repo".into()),
            worktree_path: None,
            scope: None,
            skip_worktree: true,
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
            cow_supported: Some(true), // CoW capable!
        };

        // Agent session WITHOUT sandbox fields (explicit isolation:"shared" override)
        let agent_session = intent_core::AgentSession {
            id: intent_core::AgentId::from("agent-1"),
            workspace_id: intent_core::WorkspaceId::from("ws-1"),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: "Test Agent".into(),
            name_explicitly_set: false,
            model: None,
            provider: None,
            system_prompt: None,
            specialist: Some("implementor".into()),
            status: intent_core::AgentStatus::Active,
            is_active: false,
            messages: vec![],
            stats: None,
            task_note_id: None,
            skip_auto_commit: false,
            completion_report: None,
            completion_report_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            sandbox_id: None,
            sandbox_path: None, // NO sandbox — explicit "shared" override!
            sandbox_branch: None,
            stop_reason: None,
            is_background: false,
            metadata: None,
            created_at: "2026-01-01T00:00:00Z".into(),
            updated_at: "2026-01-01T00:00:00Z".into(),
        };

        let prompt = crate::rules::assemble_system_prompt(
            &store,
            Some(&tree.0),
            "task-loop",
            Some(&injection),
            true,
            false,
            Some(&workspace),
            Some(&agent_session),
        )
        .await
        .expect("assembled prompt");

        // No sandbox hint when explicit shared override, even though workspace is CoW-capable
        assert!(
            !prompt.contains("## Workspace Isolation"),
            "no sandbox hint for explicit shared override"
        );
        assert!(
            !prompt.contains("isolated CoW"),
            "no CoW mention for shared mode"
        );
    }

    /// Task 6 (verifier requirement): explicit isolation:"cow" override in a setting-OFF
    /// workspace must still inject sandbox hint when session has sandbox_path. This
    /// mutation test proves build_isolation_hint respects actual session.sandbox_path
    /// (effective isolation), not just workspace.cow_supported + setting.
    #[tokio::test]
    async fn assembly_injects_sandbox_hint_when_explicit_cow_override() {
        let tree = worktree();
        let (_tmp, store, _svc, _ws) = setup(&tree.0).await;

        let injection = crate::rules::SpecialistPromptInjection {
            behavior_prompt: Some("Implement your task.".into()),
            specialist_name: Some("Implementor".into()),
            role_reminder: Some("Stay in scope.".into()),
        };

        // Workspace with cow_supported=true but hypothetically setting OFF
        // (doesn't matter — the agent session has sandbox_path, so it's sandboxed)
        let workspace = intent_core::Workspace {
            id: intent_core::WorkspaceId::from("ws-1"),
            title: "Test".into(),
            branch: "main".into(),
            base_ref: None,
            base_commit_sha: None,
            status: intent_core::WorkspaceStatus::Active,
            status_message: None,
            activity: intent_core::WorkspaceActivity::Idle,
            attention: intent_core::WorkspaceAttention::None,
            created_at: "2026-01-01T00:00:00Z".into(),
            updated_at: "2026-01-01T00:00:00Z".into(),
            last_activity: None,
            tags: vec![],
            path: Some("/test/path".into()),
            repository_path: Some("/test/repo".into()),
            repository_owner: None,
            repository_name: Some("test-repo".into()),
            worktree_path: None,
            scope: None,
            skip_worktree: true,
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
            cow_supported: Some(true), // Setting could be OFF, but session is sandboxed
        };

        // Agent session WITH sandbox fields (explicit isolation:"cow" override)
        let agent_session = intent_core::AgentSession {
            id: intent_core::AgentId::from("agent-1"),
            workspace_id: intent_core::WorkspaceId::from("ws-1"),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: "Test Agent".into(),
            name_explicitly_set: false,
            model: None,
            provider: None,
            system_prompt: None,
            specialist: Some("implementor".into()),
            status: intent_core::AgentStatus::Active,
            is_active: false,
            messages: vec![],
            stats: None,
            task_note_id: None,
            skip_auto_commit: false,
            completion_report: None,
            completion_report_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            sandbox_id: Some("sandbox-explicit".into()),
            sandbox_path: Some("/test/sandboxes/agent-1/test-repo".into()), // Sandboxed!
            sandbox_branch: Some("sb/agent-1".into()),
            stop_reason: None,
            is_background: false,
            metadata: None,
            created_at: "2026-01-01T00:00:00Z".into(),
            updated_at: "2026-01-01T00:00:00Z".into(),
        };

        let prompt = crate::rules::assemble_system_prompt(
            &store,
            Some(&tree.0),
            "task-loop",
            Some(&injection),
            true,
            false,
            Some(&workspace),
            Some(&agent_session),
        )
        .await
        .expect("assembled prompt");

        // Sandbox hint IS present when session has sandbox_path, regardless of setting
        assert!(
            prompt.contains("## Workspace Isolation"),
            "sandbox hint when explicit cow override"
        );
        assert!(
            prompt.contains("isolated CoW"),
            "CoW mention for sandboxed session"
        );
        assert!(
            prompt.contains("/test/sandboxes/agent-1/test-repo"),
            "sandbox path included"
        );
    }
}

mod known_repo {
    use super::*;
    use crate::sync_repos_from_workspaces;
    use intent_core::WorkspaceCreate;

    fn ws_with_repo(
        id: &WorkspaceId,
        repo_path: Option<&str>,
        repo_name: Option<&str>,
        repo_owner: Option<&str>,
    ) -> Workspace {
        Workspace {
            repository_path: repo_path.map(str::to_string),
            repository_name: repo_name.map(str::to_string),
            repository_owner: repo_owner.map(str::to_string),
            ..workspace(id)
        }
    }

    #[tokio::test]
    async fn one_time_sync_upserts_repos_from_workspaces() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        // Explicit name + owner.
        store
            .insert_workspace(&ws_with_repo(
                &WorkspaceId::new(),
                Some("/src/intent"),
                Some("intent"),
                Some("intent-hq"),
            ))
            .await
            .expect("ws1");
        // No name → basename of path; no owner.
        store
            .insert_workspace(&ws_with_repo(
                &WorkspaceId::new(),
                Some("/home/me/other-repo"),
                None,
                None,
            ))
            .await
            .expect("ws2");
        // No repository_path → skipped entirely.
        store
            .insert_workspace(&ws_with_repo(&WorkspaceId::new(), None, None, None))
            .await
            .expect("ws3");

        sync_repos_from_workspaces(&store).await.expect("sync");

        let repos = store.list_known_repos().await.expect("list");
        assert_eq!(repos.len(), 2, "only repos with a repository_path sync");
        let by_path: std::collections::HashMap<_, _> =
            repos.iter().map(|r| (r.path.as_str(), r)).collect();
        assert_eq!(by_path["/src/intent"].name, "intent");
        assert_eq!(by_path["/src/intent"].owner.as_deref(), Some("intent-hq"));
        assert_eq!(by_path["/home/me/other-repo"].name, "other-repo");
        assert_eq!(by_path["/home/me/other-repo"].owner, None);

        // Re-running the sync is idempotent on path (no duplicate rows).
        sync_repos_from_workspaces(&store).await.expect("resync");
        assert_eq!(store.list_known_repos().await.expect("list").len(), 2);
    }

    #[tokio::test]
    async fn create_workspace_registers_repo_visible_in_repo_list() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws_root = WorkspacesRoot::new();
        let svc = Services::new(store).with_workspaces_root(ws_root.path().to_path_buf());

        svc.create_workspace(
            WorkspaceCreate {
                repository_path: Some("/src/intent".to_string()),
                repository_name: Some("intent".to_string()),
                repository_owner: Some("intent-hq".to_string()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("create");

        let v = svc.repo_list().await.expect("repo_list");
        let repos = v["repos"].as_array().expect("repos array");
        assert_eq!(repos.len(), 1);
        assert_eq!(repos[0]["path"], "/src/intent");
        assert_eq!(repos[0]["name"], "intent");
        assert_eq!(repos[0]["owner"], "intent-hq");
        assert!(repos[0]["addedAt"].is_string());
        assert!(repos[0]["lastUsedAt"].is_string());
    }

    /// `workspace.create` derives `repository_name` from the local
    /// `repositoryPath` basename when the caller omits it; an explicit name
    /// always wins, and a create without a path leaves the name NULL.
    #[tokio::test]
    async fn create_workspace_derives_repository_name_from_path_basename() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws_root = WorkspacesRoot::new();
        let svc = Services::new(store).with_workspaces_root(ws_root.path().to_path_buf());

        let create = |path: Option<&str>, name: Option<&str>| WorkspaceCreate {
            repository_path: path.map(str::to_string),
            repository_name: name.map(str::to_string),
            ..Default::default()
        };

        // Local path, no name → basename derived and persisted.
        let derived = svc
            .create_workspace(create(Some("/Users/me/src/describe-workspace"), None), None)
            .await
            .expect("create derived");
        assert_eq!(
            derived.workspace.repository_name.as_deref(),
            Some("describe-workspace")
        );

        // Explicit name wins untouched.
        let explicit = svc
            .create_workspace(
                create(Some("/Users/me/src/describe-workspace"), Some("intent")),
                None,
            )
            .await
            .expect("create explicit");
        assert_eq!(
            explicit.workspace.repository_name.as_deref(),
            Some("intent")
        );

        // No repository path → name stays NULL.
        let pathless = svc
            .create_workspace(create(None, None), None)
            .await
            .expect("create pathless");
        assert_eq!(pathless.workspace.repository_name, None);

        // Windows-style `\` separators derive the basename too.
        let windows = svc
            .create_workspace(
                create(Some(r"C:\Users\me\src\describe-workspace"), None),
                None,
            )
            .await
            .expect("create windows");
        assert_eq!(
            windows.workspace.repository_name.as_deref(),
            Some("describe-workspace")
        );
    }

    /// `workspace.create` derives `repository_owner` and `repository_name` from
    /// the `origin` remote URL when the caller omits them (STAB-64). Caller-
    /// supplied values always win; non-github remotes leave owner unset; missing
    /// remotes fall back to basename for name. Strict host check rejects
    /// github.com.evil.com and similar substring attacks.
    #[tokio::test]
    async fn create_workspace_derives_owner_and_name_from_origin_remote() {
        use git2::{Repository, Signature};

        struct TempRepo(PathBuf);
        impl Drop for TempRepo {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }

        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws_root = WorkspacesRoot::new();
        let svc = Services::new(store.clone()).with_workspaces_root(ws_root.path().to_path_buf());

        // Helper: init a git repo with an origin remote and an initial commit.
        let make_repo = |remote_url: &str| -> TempRepo {
            let dir = std::env::temp_dir().join(format!("intentd-origin-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&dir).unwrap();
            let repo = Repository::init(&dir).unwrap();
            let mut cfg = repo.config().unwrap();
            cfg.set_str("user.name", "Test").unwrap();
            cfg.set_str("user.email", "test@example.com").unwrap();
            cfg.set_str("remote.origin.url", remote_url).unwrap();
            // Create an initial commit
            std::fs::write(dir.join("seed.txt"), "seed\n").unwrap();
            let mut index = repo.index().unwrap();
            index.add_path(std::path::Path::new("seed.txt")).unwrap();
            index.write().unwrap();
            let tree = repo.find_tree(index.write_tree().unwrap()).unwrap();
            let sig = Signature::now("Test", "test@example.com").unwrap();
            repo.commit(Some("HEAD"), &sig, &sig, "seed commit", &tree, &[])
                .unwrap();
            TempRepo(dir)
        };

        // GitHub https remote → owner and name derived.
        let https_repo = make_repo("https://github.com/intent-hq/intentd.git");
        let https_ws = svc
            .create_workspace(
                WorkspaceCreate {
                    repository_path: Some(https_repo.0.to_string_lossy().to_string()),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("create https");
        assert_eq!(
            https_ws.workspace.repository_owner.as_deref(),
            Some("intent-hq"),
            "https remote derives owner"
        );
        assert_eq!(
            https_ws.workspace.repository_name.as_deref(),
            Some("intentd"),
            "https remote derives name"
        );

        // GitHub ssh remote → owner and name derived.
        let ssh_repo = make_repo("git@github.com:intent-hq/intentd.git");
        let ssh_ws = svc
            .create_workspace(
                WorkspaceCreate {
                    repository_path: Some(ssh_repo.0.to_string_lossy().to_string()),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("create ssh");
        assert_eq!(
            ssh_ws.workspace.repository_owner.as_deref(),
            Some("intent-hq"),
            "ssh remote derives owner"
        );
        assert_eq!(
            ssh_ws.workspace.repository_name.as_deref(),
            Some("intentd"),
            "ssh remote derives name"
        );

        // Non-github remote → owner stays None, name falls back to basename.
        let gitlab_repo = make_repo("https://gitlab.com/myorg/myrepo.git");
        let gitlab_ws = svc
            .create_workspace(
                WorkspaceCreate {
                    repository_path: Some(gitlab_repo.0.to_string_lossy().to_string()),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("create gitlab");
        assert_eq!(
            gitlab_ws.workspace.repository_owner, None,
            "non-github remote leaves owner unset"
        );
        // The basename fallback still fires because the remote didn't parse.
        assert!(
            gitlab_ws
                .workspace
                .repository_name
                .as_deref()
                .unwrap()
                .starts_with("intentd-origin-"),
            "non-github remote falls back to basename for name"
        );

        // No origin remote → owner stays None, name falls back to basename.
        let no_remote = {
            let dir =
                std::env::temp_dir().join(format!("intentd-noremote-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir_all(&dir).unwrap();
            let repo = Repository::init(&dir).unwrap();
            let mut cfg = repo.config().unwrap();
            cfg.set_str("user.name", "Test").unwrap();
            cfg.set_str("user.email", "test@example.com").unwrap();
            // Create an initial commit
            std::fs::write(dir.join("seed.txt"), "seed\n").unwrap();
            let mut index = repo.index().unwrap();
            index.add_path(std::path::Path::new("seed.txt")).unwrap();
            index.write().unwrap();
            let tree = repo.find_tree(index.write_tree().unwrap()).unwrap();
            let sig = Signature::now("Test", "test@example.com").unwrap();
            repo.commit(Some("HEAD"), &sig, &sig, "seed commit", &tree, &[])
                .unwrap();
            TempRepo(dir)
        };
        let noremote_ws = svc
            .create_workspace(
                WorkspaceCreate {
                    repository_path: Some(no_remote.0.to_string_lossy().to_string()),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("create noremote");
        assert_eq!(
            noremote_ws.workspace.repository_owner, None,
            "no remote leaves owner unset"
        );
        assert!(
            noremote_ws
                .workspace
                .repository_name
                .as_deref()
                .unwrap()
                .starts_with("intentd-noremote-"),
            "no remote falls back to basename for name"
        );

        // Caller-supplied owner wins over remote derivation.
        let explicit_owner_repo = make_repo("https://github.com/intent-hq/intentd.git");
        let explicit_owner_ws = svc
            .create_workspace(
                WorkspaceCreate {
                    repository_path: Some(explicit_owner_repo.0.to_string_lossy().to_string()),
                    repository_owner: Some("my-override".to_string()),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("create explicit owner");
        assert_eq!(
            explicit_owner_ws.workspace.repository_owner.as_deref(),
            Some("my-override"),
            "caller-supplied owner wins"
        );
        assert_eq!(
            explicit_owner_ws.workspace.repository_name.as_deref(),
            Some("intentd"),
            "derived name still applies when only owner is explicit"
        );

        // Caller-supplied name wins over remote derivation.
        let explicit_name_repo = make_repo("https://github.com/intent-hq/intentd.git");
        let explicit_name_ws = svc
            .create_workspace(
                WorkspaceCreate {
                    repository_path: Some(explicit_name_repo.0.to_string_lossy().to_string()),
                    repository_name: Some("my-repo-name".to_string()),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("create explicit name");
        assert_eq!(
            explicit_name_ws.workspace.repository_owner.as_deref(),
            Some("intent-hq"),
            "derived owner still applies when only name is explicit"
        );
        assert_eq!(
            explicit_name_ws.workspace.repository_name.as_deref(),
            Some("my-repo-name"),
            "caller-supplied name wins"
        );

        // Verify known_repo registry also gets the derived owner.
        let repos = store.list_known_repos().await.expect("list repos");
        let https_entry = repos
            .iter()
            .find(|r| r.path == https_repo.0.to_string_lossy())
            .expect("https repo registered");
        assert_eq!(
            https_entry.owner.as_deref(),
            Some("intent-hq"),
            "known_repo entry carries derived owner"
        );
        assert_eq!(
            https_entry.name, "intentd",
            "known_repo entry carries derived name"
        );

        // Negative: host substring attack (github.com.evil.com) must NOT derive.
        let evil_https = make_repo("https://github.com.evil.com/owner/repo.git");
        let evil_https_ws = svc
            .create_workspace(
                WorkspaceCreate {
                    repository_path: Some(evil_https.0.to_string_lossy().to_string()),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("create evil https");
        assert_eq!(
            evil_https_ws.workspace.repository_owner, None,
            "github.com.evil.com must NOT derive owner (strict host check)"
        );

        // Negative: ssh host attack (git@github.com.evil:owner/repo.git) must NOT derive.
        let evil_ssh = make_repo("git@github.com.evil:owner/repo.git");
        let evil_ssh_ws = svc
            .create_workspace(
                WorkspaceCreate {
                    repository_path: Some(evil_ssh.0.to_string_lossy().to_string()),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("create evil ssh");
        assert_eq!(
            evil_ssh_ws.workspace.repository_owner, None,
            "git@github.com.evil:... must NOT derive owner (strict host check)"
        );
    }

    /// `workspace.list` backfills `repository_owner` and `repository_name` for
    /// existing workspaces with a `repositoryPath` and missing owner/name:
    /// derive from the `origin` remote URL (same helper as `workspace.create`),
    /// persist, and emit `workspace:updated` with the changed fields (STAB-64
    /// backfill).
    #[tokio::test]
    async fn list_workspaces_backfills_owner_and_name_from_origin_remote() {
        use git2::{Repository, Signature};

        struct TempRepo(PathBuf);
        impl Drop for TempRepo {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }

        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws_root = WorkspacesRoot::new();
        let bus = crate::events::bus::EventBus::new(store.clone());
        let svc = Services::new(store.clone())
            .with_workspaces_root(ws_root.path().to_path_buf())
            .with_event_bus(bus.clone());

        // Manually create a workspace row with repository_path but missing owner/name
        // (simulates old workspace created before the create derivation landed).
        let make_repo = |url: &str| -> TempRepo {
            let dir = std::env::temp_dir().join(format!(
                "intentd-backfill-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir_all(&dir).unwrap();
            let repo = Repository::init(&dir).unwrap();
            repo.remote("origin", url).unwrap();
            let mut index = repo.index().unwrap();
            let tree_id = index.write_tree().unwrap();
            let tree = repo.find_tree(tree_id).unwrap();
            let sig = Signature::now("Test", "test@example.com").unwrap();
            repo.commit(Some("HEAD"), &sig, &sig, "seed commit", &tree, &[])
                .unwrap();
            TempRepo(dir)
        };

        let repo_path = make_repo("https://github.com/octocat/hello-world.git");
        let id = WorkspaceId::from_string("backfill-test".to_string());
        let now = now_iso();
        let mut ws = Workspace {
            id: id.clone(),
            title: "Backfill Test".to_string(),
            branch: "main".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: WorkspaceStatus::Active,
            status_message: None,
            activity: WorkspaceActivity::Idle,
            attention: WorkspaceAttention::None,
            created_at: now.clone(),
            updated_at: now.clone(),
            last_activity: None,
            tags: vec![],
            path: None,
            repository_path: Some(repo_path.0.to_string_lossy().to_string()),
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
            archived: false,
            archived_at: None,
            token_usage: None,
            task_stats: None,
            agent_summary: None,
            diff_summary: None,
            cow_supported: None,
        };
        store.insert_workspace(&ws).await.expect("insert workspace");

        // Subscribe to events to capture workspace:updated
        let mut sub =
            svc.event_bus
                .as_ref()
                .unwrap()
                .subscribe(crate::events::filter::SubscriptionFilter {
                    event_types: vec!["workspace:updated".to_string()],
                    workspace_id: Some(id.0.clone()),
                    batch_window: None,
                    ..Default::default()
                });

        // Trigger workspace.list → spawns backfill
        let list = svc.list_workspaces(false).await.expect("list workspaces");
        assert!(list.iter().any(|w| w.id == id), "workspace appears in list");

        // Wait for the backfill to complete and emit workspace:updated
        let mut updated_event = None;
        for _ in 0..100 {
            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
            // Try to receive batched events (could be empty or timeout)
            match tokio::time::timeout(tokio::time::Duration::from_millis(10), sub.recv()).await {
                Ok(Some(batch)) => {
                    for evt in batch {
                        if evt.event_type == "workspace:updated"
                            && evt.workspace_id == id
                            && evt
                                .data
                                .get("changes")
                                .and_then(|c| c.get("repositoryOwner"))
                                .is_some()
                        {
                            updated_event = Some(evt);
                            break;
                        }
                    }
                    if updated_event.is_some() {
                        break;
                    }
                }
                _ => continue,
            }
        }

        assert!(
            updated_event.is_some(),
            "workspace:updated event emitted with repositoryOwner change"
        );
        let evt = updated_event.unwrap();
        assert_eq!(
            evt.data
                .get("changes")
                .and_then(|c| c.get("repositoryOwner"))
                .and_then(|v| v.as_str()),
            Some("octocat"),
            "event carries repositoryOwner change"
        );
        assert_eq!(
            evt.data
                .get("changes")
                .and_then(|c| c.get("repositoryName"))
                .and_then(|v| v.as_str()),
            Some("hello-world"),
            "event carries repositoryName change"
        );

        // Verify workspace row persisted the fields
        ws = store.get_workspace(&id).await.expect("get workspace");
        assert_eq!(
            ws.repository_owner.as_deref(),
            Some("octocat"),
            "repositoryOwner persisted"
        );
        assert_eq!(
            ws.repository_name.as_deref(),
            Some("hello-world"),
            "repositoryName persisted"
        );
    }
}

mod worktree_provisioning {
    use super::*;
    use intent_core::WorkspaceCreate;

    /// Drop guard removing a temp directory tree.
    struct TempDir(PathBuf);
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn unique_dir(prefix: &str) -> TempDir {
        let p = std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&p).unwrap();
        TempDir(p)
    }

    /// Commit everything in the worktree on the current branch, returning the oid.
    fn commit_all(repo: &git2::Repository, msg: &str) -> git2::Oid {
        let mut index = repo.index().unwrap();
        index
            .add_all(["*"].iter(), git2::IndexAddOption::DEFAULT, None)
            .unwrap();
        index.write().unwrap();
        let tree = repo.find_tree(index.write_tree().unwrap()).unwrap();
        let sig = git2::Signature::now("Tester", "t@e.dev").unwrap();
        let parent = repo
            .head()
            .ok()
            .and_then(|h| h.target())
            .and_then(|o| repo.find_commit(o).ok());
        let parents: Vec<&git2::Commit> = parent.iter().collect();
        repo.commit(Some("HEAD"), &sig, &sig, msg, &tree, &parents)
            .unwrap()
    }

    /// Init a git repo with one commit; returns (guard, head sha, head branch).
    fn seed_repo(prefix: &str) -> (TempDir, String, String) {
        let dir = unique_dir(prefix);
        let repo = git2::Repository::init(&dir.0).unwrap();
        {
            let mut cfg = repo.config().unwrap();
            cfg.set_str("user.name", "Tester").unwrap();
            cfg.set_str("user.email", "t@e.dev").unwrap();
        }
        std::fs::write(dir.0.join("README.md"), "init\n").unwrap();
        let sha = commit_all(&repo, "chore: init").to_string();
        let branch = repo.head().unwrap().shorthand().unwrap().to_string();
        (dir, sha, branch)
    }

    /// Regression for "agent spawns in a temp dir": `workspace.create` off a
    /// local git repo must provision a linked worktree at
    /// `<root>/<workspaceId>/<repo-slug>` on the workspace branch and record
    /// the base commit SHA.
    #[tokio::test]
    async fn create_provisions_worktree_from_local_repo() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let (repo_dir, head_sha, head_branch) = seed_repo("intentd-wtprov-repo");
        let root = unique_dir("intentd-wtprov-root");
        let svc = Services::new(store).with_workspaces_root(root.0.clone());

        let ws = svc
            .create_workspace(
                WorkspaceCreate {
                    repository_path: Some(repo_dir.0.to_string_lossy().to_string()),
                    repository_name: Some("My Repo".to_string()),
                    base_ref: Some(head_branch),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("create")
            .workspace;

        let wt = ws.worktree_path.as_deref().expect("worktree path set");
        assert_eq!(
            wt,
            root.0
                .join(&ws.id.0)
                .join("my-repo")
                .to_string_lossy()
                .as_ref(),
            "worktree lives at <root>/<workspaceId>/<repo-slug>"
        );
        let wt_repo = git2::Repository::open(wt).expect("worktree opens as a git repo");
        assert!(wt_repo.is_worktree());
        // Auto-generated branches are friendly `word-word` slugs (never the
        // raw workspace UUID) and are checked out.
        assert_slug_branch(&ws.branch);
        assert_ne!(ws.branch, ws.id.0, "branch must not be the raw UUID");
        assert_eq!(
            wt_repo.head().unwrap().shorthand().expect("branch name"),
            ws.branch.as_str()
        );
        assert_eq!(ws.base_commit_sha.as_deref(), Some(head_sha.as_str()));
    }

    /// Assert a `word-word` slug branch, optionally with a `-N` collision
    /// suffix (e.g. `auth-fix`, `amber-forest`, `auth-fix-2`).
    fn assert_slug_branch(branch: &str) {
        let parts: Vec<&str> = branch.split('-').collect();
        assert!(
            (2..=3).contains(&parts.len()),
            "branch '{branch}' must be word-word(-N)"
        );
        for part in &parts[..2] {
            assert!(
                (2..=15).contains(&part.len()) && part.bytes().all(|b| b.is_ascii_lowercase()),
                "branch '{branch}': segment '{part}' must be 2-15 lowercase letters"
            );
        }
        if let Some(suffix) = parts.get(2) {
            assert!(
                suffix.bytes().all(|b| b.is_ascii_digit()),
                "branch '{branch}': trailing segment must be numeric"
            );
        }
    }

    /// The initial-agent prompt seeds the branch slug, the
    /// `workspace.branchPrefix` setting is prepended, and a second workspace
    /// with the same prompt gets a `-2` collision suffix.
    #[tokio::test]
    async fn create_names_branch_from_prompt_with_prefix_and_suffix() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        store
            .set_setting("workspace.branchPrefix", "\"aw/\"")
            .await
            .expect("set prefix");
        let (repo_dir, _, head_branch) = seed_repo("intentd-wtslug-repo");
        let root = unique_dir("intentd-wtslug-root");
        let svc = Services::new(store).with_workspaces_root(root.0.clone());

        let create = |prompt: &str| WorkspaceCreate {
            repository_path: Some(repo_dir.0.to_string_lossy().to_string()),
            base_ref: Some(head_branch.clone()),
            initial_agent: Some(intent_core::WorkspaceCreateInitialAgent {
                prompt: Some(prompt.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let ws = svc
            .create_workspace(create("fix the auth flow"), None)
            .await
            .expect("create")
            .workspace;
        assert_eq!(ws.branch, "aw/auth-fix");

        // Same prompt again: the branch now exists, so a suffix is appended.
        let ws2 = svc
            .create_workspace(create("fix the auth flow"), None)
            .await
            .expect("create second")
            .workspace;
        assert_eq!(ws2.branch, "aw/auth-fix-2");
    }

    /// An explicit `branch` wins untouched — no slug, prefix, or suffix.
    #[tokio::test]
    async fn create_keeps_explicit_branch_untouched() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        store
            .set_setting("workspace.branchPrefix", "\"aw/\"")
            .await
            .expect("set prefix");
        let root = unique_dir("intentd-wtexpl-root");
        let svc = Services::new(store).with_workspaces_root(root.0.clone());

        let ws = svc
            .create_workspace(
                WorkspaceCreate {
                    branch: Some("exact/name".to_string()),
                    skip_worktree: Some(true),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("create")
            .workspace;
        assert_eq!(ws.branch, "exact/name");
    }

    /// `baseRef` selects the starting commit, and an explicit `branch` names
    /// the checked-out branch.
    #[tokio::test]
    async fn create_provisions_worktree_at_requested_base_ref() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let (repo_dir, first_sha, _) = seed_repo("intentd-wtbase-repo");
        let repo = git2::Repository::open(&repo_dir.0).unwrap();
        // Pin `base` at the first commit, then advance HEAD past it.
        let first = repo
            .find_commit(git2::Oid::from_str(&first_sha).unwrap())
            .unwrap();
        repo.branch("base", &first, false).unwrap();
        std::fs::write(repo_dir.0.join("b.txt"), "y\n").unwrap();
        commit_all(&repo, "feat: second");
        let root = unique_dir("intentd-wtbase-root");
        let svc = Services::new(store).with_workspaces_root(root.0.clone());

        let ws = svc
            .create_workspace(
                WorkspaceCreate {
                    repository_path: Some(repo_dir.0.to_string_lossy().to_string()),
                    branch: Some("my-feature".to_string()),
                    base_ref: Some("base".to_string()),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("create")
            .workspace;

        assert_eq!(ws.branch, "my-feature");
        assert_eq!(ws.base_commit_sha.as_deref(), Some(first_sha.as_str()));
        let wt_repo =
            git2::Repository::open(ws.worktree_path.as_deref().unwrap()).expect("worktree");
        let head = wt_repo.head().unwrap();
        assert_eq!(head.shorthand().expect("branch name"), "my-feature");
        assert_eq!(head.target().unwrap().to_string(), first_sha);
    }

    /// An unresolvable `baseRef` on a valid repo fails creation loudly instead
    /// of silently persisting a workspace without a checkout.
    #[tokio::test]
    async fn create_fails_on_unresolvable_base_ref() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let (repo_dir, _, _) = seed_repo("intentd-wtbad-repo");
        let root = unique_dir("intentd-wtbad-root");
        let svc = Services::new(store).with_workspaces_root(root.0.clone());

        let err = svc
            .create_workspace(
                WorkspaceCreate {
                    repository_path: Some(repo_dir.0.to_string_lossy().to_string()),
                    base_ref: Some("no-such-ref".to_string()),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect_err("create must fail");
        assert!(matches!(err, Error::InvalidParams(_)));
    }

    /// `skipWorktree`, non-git `repositoryPath`, and a caller-supplied
    /// `worktreePath` all skip provisioning (prior row-only behavior).
    #[tokio::test]
    async fn create_skips_provisioning_when_opted_out_or_not_a_repo() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let (repo_dir, _, _) = seed_repo("intentd-wtskip-repo");
        let root = unique_dir("intentd-wtskip-root");
        let svc = Services::new(store).with_workspaces_root(root.0.clone());

        let ws = svc
            .create_workspace(
                WorkspaceCreate {
                    repository_path: Some(repo_dir.0.to_string_lossy().to_string()),
                    skip_worktree: Some(true),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("create")
            .workspace;
        assert!(ws.worktree_path.is_none(), "skipWorktree opts out");

        let plain = unique_dir("intentd-wtskip-plain");
        let ws = svc
            .create_workspace(
                WorkspaceCreate {
                    repository_path: Some(plain.0.to_string_lossy().to_string()),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("create")
            .workspace;
        assert!(ws.worktree_path.is_none(), "non-git path stays row-only");

        let ws = svc
            .create_workspace(
                WorkspaceCreate {
                    repository_path: Some(repo_dir.0.to_string_lossy().to_string()),
                    worktree_path: Some("/tmp/custom-wt".to_string()),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("create")
            .workspace;
        assert_eq!(
            ws.worktree_path.as_deref(),
            Some("/tmp/custom-wt"),
            "caller-supplied worktreePath is respected untouched"
        );
    }

    /// `workspace.delete` cleans up the provisioned checkout (TS
    /// `removeGitWorktree` parity): the worktree directory and its
    /// `<root>/<workspaceId>` parent are removed, the registration is pruned,
    /// and the auto-generated workspace branch is deleted from the source repo.
    #[tokio::test]
    async fn delete_removes_worktree_and_auto_generated_branch() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let (repo_dir, _, head_branch) = seed_repo("intentd-wtdel-repo");
        let root = unique_dir("intentd-wtdel-root");
        let svc = Services::new(store.clone()).with_workspaces_root(root.0.clone());

        let ws = svc
            .create_workspace(
                WorkspaceCreate {
                    repository_path: Some(repo_dir.0.to_string_lossy().to_string()),
                    base_ref: Some(head_branch),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("create")
            .workspace;
        let wt = PathBuf::from(ws.worktree_path.as_deref().expect("worktree path"));
        assert!(wt.exists());
        assert!(
            store
                .workspace_branch_auto_generated(&ws.id)
                .await
                .expect("flag readable"),
            "auto-generated branch is recorded as workspace-owned"
        );

        svc.delete_workspace(ws.id.clone()).await.expect("delete");

        // Fast-ack: the response returns immediately while the worktree cleanup
        // runs in the background. Poll for the expected final state.
        for _ in 0..100 {
            let repo = git2::Repository::open(&repo_dir.0).unwrap();
            let branch_gone = repo
                .find_branch(&ws.branch, git2::BranchType::Local)
                .is_err();
            if !wt.exists() && !root.0.join(&ws.id.0).exists() && branch_gone {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
        assert!(!wt.exists(), "worktree directory removed");
        assert!(
            !root.0.join(&ws.id.0).exists(),
            "empty <root>/<workspaceId> parent removed"
        );
        let repo = git2::Repository::open(&repo_dir.0).unwrap();
        assert!(
            repo.find_branch(&ws.branch, git2::BranchType::Local)
                .is_err(),
            "auto-generated workspace branch deleted"
        );
        assert!(
            matches!(svc.get_workspace(ws.id).await, Err(Error::NotFound(_))),
            "workspace row deleted"
        );
    }

    /// The branch-deletion guard: a caller-supplied (explicit) branch is never
    /// deleted on `workspace.delete`, even though the worktree itself is
    /// cleaned up — mirroring the reference's "pre-existing branch" skip.
    #[tokio::test]
    async fn delete_preserves_explicit_branch() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let (repo_dir, _, head_branch) = seed_repo("intentd-wtdelex-repo");
        let root = unique_dir("intentd-wtdelex-root");
        let svc = Services::new(store.clone()).with_workspaces_root(root.0.clone());

        let ws = svc
            .create_workspace(
                WorkspaceCreate {
                    repository_path: Some(repo_dir.0.to_string_lossy().to_string()),
                    branch: Some("keep-me".to_string()),
                    base_ref: Some(head_branch),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("create")
            .workspace;
        let wt = PathBuf::from(ws.worktree_path.as_deref().expect("worktree path"));
        assert!(
            !store
                .workspace_branch_auto_generated(&ws.id)
                .await
                .expect("flag readable"),
            "explicit branch is not workspace-owned"
        );

        svc.delete_workspace(ws.id).await.expect("delete");

        // Fast-ack: poll for worktree removal.
        for _ in 0..100 {
            if !wt.exists() {
                break;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
        assert!(!wt.exists(), "worktree directory still removed");
        let repo = git2::Repository::open(&repo_dir.0).unwrap();
        assert!(
            repo.find_branch("keep-me", git2::BranchType::Local).is_ok(),
            "explicit branch preserved"
        );
    }

    /// A workspace without a provisioned checkout (`skipWorktree`) deletes its
    /// row without touching the repository.
    #[tokio::test]
    async fn delete_without_worktree_only_removes_row() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let (repo_dir, _, _) = seed_repo("intentd-wtdelskip-repo");
        let root = unique_dir("intentd-wtdelskip-root");
        let svc = Services::new(store).with_workspaces_root(root.0.clone());

        let ws = svc
            .create_workspace(
                WorkspaceCreate {
                    repository_path: Some(repo_dir.0.to_string_lossy().to_string()),
                    skip_worktree: Some(true),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("create")
            .workspace;
        let branch = ws.branch.clone();
        svc.delete_workspace(ws.id.clone()).await.expect("delete");
        assert!(matches!(
            svc.get_workspace(ws.id).await,
            Err(Error::NotFound(_))
        ));
        // No checkout was provisioned, so nothing in the repo changed.
        let repo = git2::Repository::open(&repo_dir.0).unwrap();
        assert!(repo.find_branch(&branch, git2::BranchType::Local).is_err());
    }

    /// `workspace.create` derives the workspace id from the initial-agent
    /// prompt via `extract_local_slug` (TS `generateLocalSlug` parity), so the
    /// on-disk directory is human-readable (e.g. `auth-fix`) instead of an
    /// opaque UUID. A second create with the same prompt collides and yields
    /// `<slug>-2`, mirroring the branch collision suffix.
    #[tokio::test]
    async fn create_derives_slug_id_from_prompt_and_uniquifies() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let root = unique_dir("intentd-idslug-root");
        let svc = Services::new(store).with_workspaces_root(root.0.clone());

        let make = |prompt: &str| WorkspaceCreate {
            skip_worktree: Some(true),
            initial_agent: Some(intent_core::WorkspaceCreateInitialAgent {
                prompt: Some(prompt.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let first = svc
            .create_workspace(make("fix the auth flow"), None)
            .await
            .expect("first create")
            .workspace;
        assert_eq!(first.id.0, "auth-fix");

        let second = svc
            .create_workspace(make("fix the auth flow"), None)
            .await
            .expect("second create")
            .workspace;
        assert_eq!(second.id.0, "auth-fix-2");
    }

    /// Workspace ids are never recycled across delete/recreate (LEAK-2): a
    /// deleted workspace leaves a tombstone, so re-creating with the same
    /// prompt yields a *different* (suffixed) id — reusing the id would
    /// collide the old workspace's agent streams and file paths with the new
    /// one's (FE `recentlyDeletedWorkspaces` parity).
    #[tokio::test]
    async fn create_never_recycles_deleted_workspace_id() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let root = unique_dir("intentd-idrecycle-root");
        let svc = Services::new(store).with_workspaces_root(root.0.clone());

        let make = |prompt: &str| WorkspaceCreate {
            skip_worktree: Some(true),
            initial_agent: Some(intent_core::WorkspaceCreateInitialAgent {
                prompt: Some(prompt.to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };

        let first = svc
            .create_workspace(make("fix the auth flow"), None)
            .await
            .expect("first create")
            .workspace;
        assert_eq!(first.id.0, "auth-fix");

        svc.delete_workspace(first.id.clone())
            .await
            .expect("delete");

        let second = svc
            .create_workspace(make("fix the auth flow"), None)
            .await
            .expect("recreate")
            .workspace;
        assert_ne!(second.id, first.id, "deleted id must not be recycled");
        assert_eq!(second.id.0, "auth-fix-2");

        // Delete the suffixed one too: the next create must skip *both*
        // tombstoned ids.
        svc.delete_workspace(second.id.clone())
            .await
            .expect("delete second");
        let third = svc
            .create_workspace(make("fix the auth flow"), None)
            .await
            .expect("third create")
            .workspace;
        assert_eq!(third.id.0, "auth-fix-3");
    }

    /// A leftover `<workspaces_root>/<id>` directory (orphaned/pre-tombstone
    /// state) also blocks id reuse: `workspace.create` uniquifies past it.
    #[tokio::test]
    async fn create_skips_workspace_id_with_leftover_directory() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let root = unique_dir("intentd-iddir-root");
        let svc = Services::new(store).with_workspaces_root(root.0.clone());

        std::fs::create_dir_all(root.0.join("auth-fix")).expect("seed leftover dir");

        let ws = svc
            .create_workspace(
                WorkspaceCreate {
                    skip_worktree: Some(true),
                    initial_agent: Some(intent_core::WorkspaceCreateInitialAgent {
                        prompt: Some("fix the auth flow".to_string()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("create")
            .workspace;
        assert_eq!(ws.id.0, "auth-fix-2");
    }

    /// When no initial-agent prompt is supplied, `workspace.create` falls back
    /// to a random adjective-animal slug (never a raw UUID). The resulting id
    /// must satisfy the FE `word-word` shape (`isValidWorkspaceId`).
    #[tokio::test]
    async fn create_falls_back_to_random_slug_id_without_prompt() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let root = unique_dir("intentd-idrand-root");
        let svc = Services::new(store).with_workspaces_root(root.0.clone());

        let ws = svc
            .create_workspace(
                WorkspaceCreate {
                    skip_worktree: Some(true),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("create")
            .workspace;

        let parts: Vec<&str> = ws.id.0.split('-').collect();
        assert_eq!(parts.len(), 2, "id '{}' must be word-word", ws.id.0);
        for part in &parts {
            assert!(
                (2..=15).contains(&part.len()) && part.bytes().all(|b| b.is_ascii_lowercase()),
                "id '{}': segment '{}' must be 2-15 lowercase letters",
                ws.id.0,
                part
            );
        }
    }

    /// `workspace.create` also writes the legacy
    /// `<root>/<id>/.workspace/workspace.json` metadata file so renderer paths
    /// (FE `FileSystemWorkspaceRepository.findById`) find it without ENOENT.
    /// The file matches the FE `WorkspaceSchema`: id/title/branch/status plus
    /// the empty `changesets`/`timeline`/`conversationInfo` arrays it requires.
    #[tokio::test]
    async fn create_writes_workspace_json_metadata_file() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let root = unique_dir("intentd-wsjson-root");
        let svc = Services::new(store).with_workspaces_root(root.0.clone());

        let ws = svc
            .create_workspace(
                WorkspaceCreate {
                    title: Some("My workspace".to_string()),
                    branch: Some("feat/wsjson".to_string()),
                    skip_worktree: Some(true),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("create")
            .workspace;

        let metadata_path = root
            .0
            .join(&ws.id.0)
            .join(".workspace")
            .join("workspace.json");
        assert!(
            metadata_path.exists(),
            "workspace.json must exist at {}",
            metadata_path.display()
        );
        let contents = std::fs::read_to_string(&metadata_path).expect("read workspace.json");
        let value: serde_json::Value =
            serde_json::from_str(&contents).expect("parse workspace.json");
        assert_eq!(value["id"], ws.id.0);
        assert_eq!(value["title"], "My workspace");
        assert_eq!(value["branch"], "feat/wsjson");
        assert_eq!(value["status"], "Active");
        // FE schema requires these three arrays; the daemon does not model
        // them so `write_workspace_metadata_file` fills empty arrays.
        assert!(value["changesets"].is_array());
        assert!(value["timeline"].is_array());
        assert!(value["conversationInfo"].is_array());
        assert_eq!(value["changesets"].as_array().unwrap().len(), 0);
    }

    /// When the caller passes `title: ""` (the JSON-RPC shape onboarding
    /// sends today), `workspace.create` stores `""` — reference parity with
    /// `workspace.service` (`title: request.title || ''`) so the FE renders
    /// "Untitled" until the initial agent's first-turn naming instruction
    /// calls `workspace.setTitle`.
    #[tokio::test]
    async fn create_stores_empty_title_when_title_is_empty_string() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let root = unique_dir("intentd-titleempty-root");
        let svc = Services::new(store).with_workspaces_root(root.0.clone());

        let ws = svc
            .create_workspace(
                WorkspaceCreate {
                    // Onboarding sends `title: ''` today; simulate that.
                    title: Some(String::new()),
                    skip_worktree: Some(true),
                    initial_agent: Some(intent_core::WorkspaceCreateInitialAgent {
                        prompt: Some("fix the auth flow".to_string()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("create")
            .workspace;

        assert_eq!(ws.id.0, "auth-fix");
        assert_eq!(
            ws.title, "",
            "empty-string title stored as empty for Untitled parity"
        );

        // The metadata file mirrors the stored empty title so FE reads see it too.
        let metadata_path = root
            .0
            .join(&ws.id.0)
            .join(".workspace")
            .join("workspace.json");
        let value: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&metadata_path).unwrap()).unwrap();
        assert_eq!(value["title"], "");
    }

    /// When the caller omits the `title` field entirely (JSON-RPC `null` /
    /// absent), `workspace.create` stores `""` (matching the empty-string
    /// case): the reference contract collapses missing and blank titles to
    /// the same Untitled shape.
    #[tokio::test]
    async fn create_stores_empty_title_when_title_is_none() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let root = unique_dir("intentd-titlenone-root");
        let svc = Services::new(store).with_workspaces_root(root.0.clone());

        let ws = svc
            .create_workspace(
                WorkspaceCreate {
                    // `title` field absent from the wire payload.
                    title: None,
                    skip_worktree: Some(true),
                    initial_agent: Some(intent_core::WorkspaceCreateInitialAgent {
                        prompt: Some("fix the auth flow".to_string()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("create")
            .workspace;

        assert_eq!(ws.title, "");
    }

    /// A whitespace-only title is normalized to `""` (same Untitled shape).
    #[tokio::test]
    async fn create_stores_empty_title_when_title_is_whitespace() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let root = unique_dir("intentd-titlewsp-root");
        let svc = Services::new(store).with_workspaces_root(root.0.clone());

        let ws = svc
            .create_workspace(
                WorkspaceCreate {
                    title: Some("   \t  ".to_string()),
                    skip_worktree: Some(true),
                    initial_agent: Some(intent_core::WorkspaceCreateInitialAgent {
                        prompt: Some("fix the auth flow".to_string()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("create")
            .workspace;

        assert_eq!(ws.title, "");
    }

    /// An explicit non-empty title from the caller wins over the slug
    /// fallback (the reference-app path still allows explicit titles).
    #[tokio::test]
    async fn create_preserves_explicit_title_over_slug_fallback() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let root = unique_dir("intentd-titleexpl-root");
        let svc = Services::new(store).with_workspaces_root(root.0.clone());

        let ws = svc
            .create_workspace(
                WorkspaceCreate {
                    title: Some("My Explicit Title".to_string()),
                    skip_worktree: Some(true),
                    initial_agent: Some(intent_core::WorkspaceCreateInitialAgent {
                        prompt: Some("fix the auth flow".to_string()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("create")
            .workspace;

        assert_eq!(ws.title, "My Explicit Title");
    }
}

mod file_ops_service {
    use super::*;

    /// `file.*` wired through `WorkspaceApi`: the workspace root resolves from
    /// `worktreePath`, writes/reads round-trip, and an out-of-workspace path
    /// surfaces as `Error::Internal` (→ `-32603`).
    #[tokio::test]
    async fn file_methods_resolve_root_and_enforce_workspace() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws = WorkspaceId::new();

        let dir = std::env::temp_dir().join(format!("intentd-fileapi-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let dir = std::fs::canonicalize(&dir).unwrap();
        let mut w = workspace(&ws);
        w.worktree_path = Some(dir.to_string_lossy().into_owned());
        store.insert_workspace(&w).await.expect("ws");
        let svc = Services::new(store);

        let written = svc
            .file_write(
                ws.clone(),
                "notes/x.txt".to_string(),
                "hi".to_string(),
                None,
            )
            .await
            .expect("write");
        assert_eq!(
            written,
            serde_json::json!({ "ok": true, "path": "notes/x.txt", "size": 2 })
        );

        let read = svc
            .file_read(ws.clone(), "notes/x.txt".to_string(), None)
            .await
            .expect("read");
        assert_eq!(read, serde_json::Value::String("hi".to_string()));

        let listed = svc
            .file_list(ws.clone(), "notes".to_string(), None)
            .await
            .expect("list");
        assert_eq!(
            listed,
            serde_json::json!([{ "name": "x.txt", "type": "file" }])
        );

        let denied = svc
            .file_read(ws.clone(), "../escape".to_string(), None)
            .await;
        assert!(matches!(denied, Err(Error::Internal(_))));

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Containment integration test: delegate an agent with isolation=cow, perform a
    /// file write through the agent-scoped ops path (caller_agent_id → resolve_root),
    /// and assert the write landed in the sandbox and the user's directory is untouched.
    #[tokio::test]
    async fn file_write_via_sandboxed_agent_is_contained() {
        use crate::sandbox_ops::{provision_sandbox, ProvisionConfig};
        use intent_core::{AgentId, AgentSession, AgentStatus};
        use intent_git::{cow_probe, CowSupport};
        use std::fs;

        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws_id = WorkspaceId::new();

        // Set up test directories under target/ (same volume)
        let workspace_root = std::env::current_dir()
            .unwrap()
            .ancestors()
            .nth(2) // packages/intentd
            .unwrap()
            .to_path_buf();
        let test_root = workspace_root
            .join("target")
            .join(format!("test-containment-{}", uuid::Uuid::new_v4()));
        let user_dir = test_root.join("user-workspace");
        let workspaces_root = test_root.join("workspaces");

        // Initialize a git repo in the user directory
        fs::create_dir_all(&user_dir).unwrap();
        let repo = git2::Repository::init(&user_dir).unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();
        let tree_id = {
            let mut index = repo.index().unwrap();
            index.write_tree().unwrap()
        };
        let tree = repo.find_tree(tree_id).unwrap();
        repo.commit(Some("HEAD"), &sig, &sig, "Initial commit", &tree, &[])
            .unwrap();

        // Early probe check - skip test if CoW not available
        fs::create_dir_all(&workspaces_root).unwrap();
        let probe = cow_probe(&user_dir, &workspaces_root).unwrap();
        if probe == CowSupport::Unsupported {
            eprintln!(
                "SKIP test (CoW not supported): {:?} → {:?}",
                user_dir, workspaces_root
            );
            let _ = fs::remove_dir_all(&test_root);
            return;
        }

        // Create workspace pointing at user directory
        let mut ws = workspace(&ws_id);
        ws.repository_path = Some(user_dir.to_string_lossy().to_string());
        ws.skip_worktree = true; // direct mode
        store.insert_workspace(&ws).await.expect("insert ws");

        // Create agent
        let agent_id = AgentId::new();
        let agent = AgentSession {
            id: agent_id.clone(),
            workspace_id: ws_id.clone(),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: "Sandboxed Agent".to_string(),
            name_explicitly_set: false,
            model: None,
            provider: None,
            system_prompt: None,
            specialist: None,
            status: AgentStatus::Active,
            is_active: true,
            messages: vec![],
            stats: None,
            task_note_id: None,
            skip_auto_commit: false,
            completion_report: None,
            completion_report_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            is_background: false,
            metadata: None,
            created_at: now_iso(),
            updated_at: now_iso(),
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
        };
        store
            .insert_agent_session(&agent)
            .await
            .expect("insert agent");

        // Create Services before provisioning so it has access to the store
        let svc = Services::new(store.clone());

        // Provision sandbox
        let config = ProvisionConfig { workspaces_root };
        let outcome = provision_sandbox(&store, &ws_id, &agent_id, &config)
            .await
            .expect("provision");
        let crate::sandbox_ops::ProvisionOutcome::Supported {
            path: sandbox_path, ..
        } = outcome
        else {
            panic!("Expected Supported after probe confirmed CoW available");
        };

        // Update agent session with sandbox path (simulates delegate flow)
        let mut updated_agent = store.get_agent_session(&agent_id).await.unwrap();
        updated_agent.sandbox_path = Some(sandbox_path.to_string_lossy().to_string());
        updated_agent.updated_at = now_iso();
        store
            .update_agent_session(&ws_id, &updated_agent)
            .await
            .unwrap();

        // Write a file via the sandboxed agent (caller_agent_id triggers resolve_root to use sandbox_path)
        let written = svc
            .file_write(
                ws_id.clone(),
                "contained.txt".to_string(),
                "sandboxed write".to_string(),
                Some(agent_id.clone()),
            )
            .await
            .expect("write via sandbox agent");

        assert_eq!(
            written,
            serde_json::json!({ "ok": true, "path": "contained.txt", "size": 15 })
        );

        // Assert the write landed in the sandbox
        let sandbox_file = sandbox_path.join("contained.txt");
        assert!(
            sandbox_file.exists(),
            "Write must land in sandbox: {:?}",
            sandbox_file
        );
        let sandbox_content = fs::read_to_string(&sandbox_file).unwrap();
        assert_eq!(sandbox_content, "sandboxed write");

        // Assert the user's directory is completely untouched
        let user_file = user_dir.join("contained.txt");
        assert!(
            !user_file.exists(),
            "User directory must remain untouched: {:?}",
            user_file
        );

        // Clean up
        let _ = fs::remove_dir_all(&test_root);
    }

    /// Wire-contract test: agent.delegate returns effectiveIsolation field when
    /// isolation is requested, reporting "cow" on successful provisioning or "direct"
    /// on unsupported fallback.
    #[tokio::test]
    async fn delegate_returns_effective_isolation_in_result() {
        use intent_core::AgentDelegateInput;
        use intent_git::{cow_probe, CowSupport};
        use std::fs;

        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws_id = WorkspaceId::new();

        // Set up test directories under target/ (same volume for CoW support)
        let workspace_root = std::env::current_dir()
            .unwrap()
            .ancestors()
            .nth(2)
            .unwrap()
            .to_path_buf();
        let test_root = workspace_root
            .join("target")
            .join(format!("test-delegate-iso-{}", uuid::Uuid::new_v4()));
        let user_dir = test_root.join("user-workspace");
        let workspaces_root = test_root.join("workspaces");

        // Initialize a git repo
        fs::create_dir_all(&user_dir).unwrap();
        let repo = git2::Repository::init(&user_dir).unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();
        let tree_id = {
            let mut index = repo.index().unwrap();
            index.write_tree().unwrap()
        };
        let tree = repo.find_tree(tree_id).unwrap();
        repo.commit(Some("HEAD"), &sig, &sig, "Initial commit", &tree, &[])
            .unwrap();

        // Create workspace
        let mut ws = workspace(&ws_id);
        ws.repository_path = Some(user_dir.to_string_lossy().to_string());
        ws.skip_worktree = true;
        store.insert_workspace(&ws).await.expect("insert ws");

        // Probe CoW support to determine expected outcome
        fs::create_dir_all(&workspaces_root).unwrap();
        let probe = cow_probe(&user_dir, &workspaces_root).unwrap();
        let expected_isolation = match probe {
            CowSupport::Supported => "cow",
            CowSupport::Unsupported => "direct",
        };

        // Create services with workspaces_root configured
        let mut svc = Services::new(store.clone());
        svc.workspaces_root = Some(workspaces_root);

        // Delegate with isolation=cow
        let delegate_input = AgentDelegateInput {
            task_text: Some("test task".to_string()),
            agent_instructions: Some("test instructions".to_string()),
            isolation: Some("cow".to_string()),
            ..Default::default()
        };
        let result = svc
            .agent_delegate_op(ws_id.clone(), delegate_input, None)
            .await
            .expect("delegate");

        // Assert effectiveIsolation field is present and correct
        let effective_iso = result
            .get("effectiveIsolation")
            .expect("effectiveIsolation field must be present when isolation is requested");
        assert_eq!(
            effective_iso.as_str().unwrap(),
            expected_isolation,
            "effectiveIsolation must report actual provisioning outcome"
        );

        // Also verify agentId and name are present (baseline delegate result shape)
        assert!(result.get("ok").unwrap().as_bool().unwrap());
        assert!(result.get("agentId").is_some());
        assert!(result.get("name").is_some());

        // Clean up
        let _ = fs::remove_dir_all(&test_root);
    }
}

mod primitive_ops_service {
    use super::*;
    use intent_core::WorkspaceApi;
    use serde_json::{json, Value};

    /// Parse the trailing fenced `ws-block:<type>` JSON block out of note content.
    fn last_block(content: &str, block_type: &str) -> Value {
        let fence = format!("```ws-block:{block_type}\n");
        let body = content
            .rsplit_once(&fence)
            .expect("ws-block fence present")
            .1
            .rsplit_once("\n```\n")
            .expect("closing fence")
            .0;
        serde_json::from_str(body).expect("block parses as JSON")
    }

    /// All four `primitive.*` methods append a parseable `ws-block:<type>` block
    /// and return `{ ok, primitiveId, noteId, content }` with the TS field shapes.
    #[tokio::test]
    async fn primitive_methods_append_blocks_and_match_response_shape() {
        let (_tmp, svc, ws, note_id) = setup("# Note").await;

        let r = svc
            .primitive_add_reference(
                ws.clone(),
                note_id.clone(),
                "src/a.ts#symbol:Foo".to_string(),
                "a ref".to_string(),
                Some("fn foo() {}".to_string()),
            )
            .await
            .expect("addReference");
        assert_eq!(r["ok"], json!(true));
        assert_eq!(r["noteId"], json!("n1"));
        assert!(r["primitiveId"].is_string());
        let block = last_block(r["content"].as_str().unwrap(), "reference");
        assert_eq!(block["type"], "reference");
        assert_eq!(block["version"], 1);
        assert_eq!(block["createdBy"], "agent");
        assert_eq!(block["target"]["kind"], "symbol");
        assert_eq!(block["snapshot"]["filePath"], "src/a.ts");
        assert_eq!(block["id"], r["primitiveId"]);

        let c = svc
            .primitive_add_cli(
                ws.clone(),
                note_id.clone(),
                "cargo test".to_string(),
                "run tests".to_string(),
                None,
            )
            .await
            .expect("addCli");
        let cblock = last_block(c["content"].as_str().unwrap(), "cli");
        assert_eq!(cblock["type"], "cli");
        assert_eq!(cblock["cwd"], "./");
        assert_eq!(cblock["display"]["showCommandPrefix"], "$");

        let p = svc
            .primitive_add_patch(
                ws.clone(),
                note_id.clone(),
                "src/a.ts".to_string(),
                "@@ -1 +1 @@".to_string(),
                "fix".to_string(),
            )
            .await
            .expect("addPatch");
        let pblock = last_block(p["content"].as_str().unwrap(), "patch");
        assert_eq!(pblock["type"], "patch");
        assert_eq!(pblock["patches"][0]["filePath"], "src/a.ts");

        let a = svc
            .primitive_add_agent_action(
                ws.clone(),
                note_id.clone(),
                "agent-1".to_string(),
                "do it".to_string(),
                "desc".to_string(),
            )
            .await
            .expect("addAgentAction");
        let ablock = last_block(a["content"].as_str().unwrap(), "agent_action");
        assert_eq!(ablock["type"], "agent_action");
        assert_eq!(ablock["agentId"], "agent-1");
        assert_eq!(ablock["inputs"], json!([]));

        // Blocks accumulate on the note (four appends, four fences).
        let persisted = svc.get_note(ws, note_id).await.expect("get_note");
        assert_eq!(persisted.content.matches("```ws-block:").count(), 4);
    }

    /// A missing note surfaces as `Error::Internal` (→ `-32603`), matching the TS
    /// builder throwing `Note <id> not found`.
    #[tokio::test]
    async fn primitive_on_missing_note_is_internal_error() {
        let (_tmp, svc, ws, _id) = setup("# Note").await;
        let res = svc
            .primitive_add_cli(
                ws,
                NoteId::from("missing"),
                "ls".to_string(),
                "d".to_string(),
                None,
            )
            .await;
        assert!(matches!(res, Err(Error::Internal(_))));
    }
}

/// `linear.*` P1 read handlers over an injected stub engine: not-configured
/// failures map to `Internal` (→ `-32603`) and successes serialize as bare
/// arrays / a bare object (no `{ items, nextToken }` envelope).
mod linear {
    use std::sync::Arc;

    use async_trait::async_trait;
    use intent_core::WorkspaceApi;
    use intent_linear::{
        CreateIssueRequest, Error as LinearError, IssueFilter, LinearEngine, LinearIssueResult,
        LinearLabel, LinearProject, LinearTeam, LinearUser, LinearWorkflowState,
        Result as LinearResult, UpdateIssueRequest,
    };

    use super::*;

    /// Injectable stub: when `fail` it reports `NotConfigured` for every call;
    /// otherwise it returns canned values for shape assertions.
    struct StubLinear {
        fail: bool,
    }

    impl StubLinear {
        fn not_configured<T>() -> LinearResult<T> {
            Err(LinearError::NotConfigured("no key".into()))
        }

        fn issue() -> LinearIssueResult {
            LinearIssueResult {
                id: "uuid-1".into(),
                identifier: "ENG-1".into(),
                title: "t".into(),
                description: None,
                url: None,
                team_name: None,
                team_key: None,
                state: None,
                priority: None,
                assignee: None,
                labels: None,
                project: None,
                creator: None,
                created_at: None,
                updated_at: None,
            }
        }
    }

    #[async_trait]
    impl LinearEngine for StubLinear {
        async fn auth_status(&self) -> LinearResult<intent_linear::AuthStatus> {
            Self::not_configured()
        }

        async fn list_issues(
            &self,
            _filter: IssueFilter,
            _limit: Option<u32>,
        ) -> LinearResult<Vec<LinearIssueResult>> {
            Self::not_configured()
        }

        async fn search_issues(
            &self,
            _query: &str,
            _limit: Option<u32>,
        ) -> LinearResult<Vec<LinearIssueResult>> {
            Self::not_configured()
        }

        async fn get_issue(&self, _id_or_identifier: &str) -> LinearResult<LinearIssueResult> {
            if self.fail {
                return Self::not_configured();
            }
            Ok(Self::issue())
        }

        async fn viewer(&self) -> LinearResult<LinearUser> {
            if self.fail {
                return Self::not_configured();
            }
            Ok(LinearUser {
                id: "u1".into(),
                name: "Ada".into(),
                display_name: None,
                email: None,
                avatar_url: None,
            })
        }

        async fn list_teams(&self, _limit: Option<u32>) -> LinearResult<Vec<LinearTeam>> {
            if self.fail {
                return Self::not_configured();
            }
            Ok(vec![LinearTeam {
                id: "t1".into(),
                key: "ENG".into(),
                name: "Engineering".into(),
                description: None,
            }])
        }

        async fn list_workflow_states(
            &self,
            _limit: Option<u32>,
        ) -> LinearResult<Vec<LinearWorkflowState>> {
            if self.fail {
                return Self::not_configured();
            }
            Ok(vec![LinearWorkflowState {
                id: "s1".into(),
                name: "Todo".into(),
                r#type: "unstarted".into(),
                description: None,
                color: None,
            }])
        }

        async fn list_projects(&self, _limit: Option<u32>) -> LinearResult<Vec<LinearProject>> {
            if self.fail {
                return Self::not_configured();
            }
            Ok(vec![LinearProject {
                id: "p1".into(),
                name: "Apollo".into(),
                description: None,
                state: "started".into(),
                url: None,
            }])
        }

        async fn list_labels(&self, _limit: Option<u32>) -> LinearResult<Vec<LinearLabel>> {
            if self.fail {
                return Self::not_configured();
            }
            Ok(vec![LinearLabel {
                id: "l1".into(),
                name: "bug".into(),
                description: None,
                color: None,
            }])
        }

        async fn create_issue(&self, _req: CreateIssueRequest) -> LinearResult<LinearIssueResult> {
            if self.fail {
                return Self::not_configured();
            }
            Ok(Self::issue())
        }

        async fn update_issue(&self, _req: UpdateIssueRequest) -> LinearResult<LinearIssueResult> {
            if self.fail {
                return Self::not_configured();
            }
            Ok(Self::issue())
        }
    }

    async fn svc(fail: bool) -> (TempDb, Services) {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let services = Services::new(store).with_linear_engine(Arc::new(StubLinear { fail }));
        (tmp, services)
    }

    #[tokio::test]
    async fn not_configured_maps_to_internal() {
        let (_tmp, s) = svc(true).await;
        assert!(matches!(
            s.linear_get_issue("ENG-1".into()).await,
            Err(Error::Internal(_))
        ));
        assert!(matches!(s.linear_viewer().await, Err(Error::Internal(_))));
        assert!(matches!(
            s.linear_list_teams(None).await,
            Err(Error::Internal(_))
        ));
        assert!(matches!(
            s.linear_list_workflow_states(None).await,
            Err(Error::Internal(_))
        ));
        assert!(matches!(
            s.linear_list_projects(None).await,
            Err(Error::Internal(_))
        ));
        assert!(matches!(
            s.linear_list_labels(None).await,
            Err(Error::Internal(_))
        ));
        assert!(matches!(
            s.linear_create_issue(serde_json::json!({"title":"t","teamId":"team-1"}))
                .await,
            Err(Error::Internal(_))
        ));
        assert!(matches!(
            s.linear_update_issue(serde_json::json!({"issueId":"uuid-1","title":"t"}))
                .await,
            Err(Error::Internal(_))
        ));
    }

    #[tokio::test]
    async fn success_serializes_as_bare_object_and_arrays() {
        let (_tmp, s) = svc(false).await;

        let issue = s.linear_get_issue("ENG-1".into()).await.unwrap();
        assert!(issue.is_object());
        assert_eq!(issue["identifier"], "ENG-1");

        let viewer = s.linear_viewer().await.unwrap();
        assert!(viewer.is_object());
        assert_eq!(viewer["name"], "Ada");

        for arr in [
            s.linear_list_teams(None).await.unwrap(),
            s.linear_list_workflow_states(None).await.unwrap(),
            s.linear_list_projects(None).await.unwrap(),
            s.linear_list_labels(None).await.unwrap(),
        ] {
            assert!(arr.is_array(), "expected bare array, got {arr}");
            assert!(arr.get("items").is_none(), "no envelope");
        }

        let created = s
            .linear_create_issue(serde_json::json!({"title":"New","teamId":"team-1"}))
            .await
            .unwrap();
        assert!(created.is_object());
        assert_eq!(created["identifier"], "ENG-1");
        assert!(created.get("items").is_none(), "no envelope");

        let updated = s
            .linear_update_issue(serde_json::json!({"issueId":"uuid-1","title":"Edit"}))
            .await
            .unwrap();
        assert!(updated.is_object());
        assert_eq!(updated["identifier"], "ENG-1");
        assert!(updated.get("items").is_none(), "no envelope");
    }

    #[tokio::test]
    async fn create_and_update_reject_invalid_params() {
        let (_tmp, s) = svc(false).await;

        // createIssue requires `title` and `teamId`.
        assert!(matches!(
            s.linear_create_issue(serde_json::json!({"teamId":"team-1"}))
                .await,
            Err(Error::InvalidParams(_))
        ));
        assert!(matches!(
            s.linear_create_issue(serde_json::json!({"title":"X"}))
                .await,
            Err(Error::InvalidParams(_))
        ));
        assert!(matches!(
            s.linear_create_issue(serde_json::json!({"title":"  ","teamId":"team-1"}))
                .await,
            Err(Error::InvalidParams(_))
        ));

        // updateIssue requires `issueId`.
        assert!(matches!(
            s.linear_update_issue(serde_json::json!({"title":"X"}))
                .await,
            Err(Error::InvalidParams(_))
        ));
        assert!(matches!(
            s.linear_update_issue(serde_json::json!({"issueId":""}))
                .await,
            Err(Error::InvalidParams(_))
        ));
    }
}

/// `sentry.*` P0 read handlers over an injected stub engine: not-configured
/// failures map to `Internal` (→ `-32603`) and successes serialize as a bare
/// array / a bare object (no `{ items, nextToken }` envelope).
mod sentry {
    use std::sync::Arc;

    use async_trait::async_trait;
    use intent_core::WorkspaceApi;
    use intent_sentry::{
        Error as SentryError, FetchIssuesRequest, Result as SentryResult, SentryAuthState,
        SentryEngine, SentryIssueLevel, SentryIssueResult, SentryIssueStatus, SentryProject,
    };

    use super::*;

    /// Injectable stub: when `fail` it reports `NotConfigured` for every call;
    /// otherwise it returns canned values for shape assertions.
    struct StubSentry {
        fail: bool,
    }

    impl StubSentry {
        fn not_configured<T>() -> SentryResult<T> {
            Err(SentryError::NotConfigured("no creds".into()))
        }

        fn issue() -> SentryIssueResult {
            SentryIssueResult {
                id: "1".into(),
                short_id: "PROJ-1".into(),
                title: "boom".into(),
                culprit: None,
                status: SentryIssueStatus::Unresolved,
                level: SentryIssueLevel::Error,
                count: "1".into(),
                user_count: 0,
                first_seen: "2026-01-01T00:00:00Z".into(),
                last_seen: "2026-01-02T00:00:00Z".into(),
                project_name: "Web".into(),
                project_slug: "web".into(),
                url: None,
                r#type: None,
                value: None,
                filename: None,
                function: None,
            }
        }
    }

    #[async_trait]
    impl SentryEngine for StubSentry {
        async fn auth_status(&self) -> SentryResult<SentryAuthState> {
            if self.fail {
                return Self::not_configured();
            }
            Ok(SentryAuthState {
                authenticated: true,
                organization: Some("acme".into()),
                error: None,
            })
        }

        async fn list_issues(
            &self,
            _request: FetchIssuesRequest,
        ) -> SentryResult<Vec<SentryIssueResult>> {
            if self.fail {
                return Self::not_configured();
            }
            Ok(vec![Self::issue()])
        }

        async fn search_issues(
            &self,
            _query: &str,
            _project: Option<&str>,
            _limit: Option<u32>,
        ) -> SentryResult<Vec<SentryIssueResult>> {
            if self.fail {
                return Self::not_configured();
            }
            Ok(vec![Self::issue()])
        }

        async fn list_projects(&self, _limit: Option<u32>) -> SentryResult<Vec<SentryProject>> {
            if self.fail {
                return Self::not_configured();
            }
            Ok(vec![SentryProject {
                id: "p1".into(),
                slug: "web".into(),
                name: "Web".into(),
                platform: Some("javascript".into()),
                is_member: Some(true),
            }])
        }

        async fn get_issue(&self, _id_or_short_id: &str) -> SentryResult<SentryIssueResult> {
            if self.fail {
                return Self::not_configured();
            }
            Ok(Self::issue())
        }

        async fn resolve_issue(&self, _id: &str) -> SentryResult<SentryIssueResult> {
            if self.fail {
                return Self::not_configured();
            }
            Ok(Self::issue())
        }

        async fn ignore_issue(&self, _id: &str) -> SentryResult<SentryIssueResult> {
            if self.fail {
                return Self::not_configured();
            }
            Ok(Self::issue())
        }

        async fn assign_issue(
            &self,
            _id: &str,
            _assigned_to: Option<&str>,
        ) -> SentryResult<SentryIssueResult> {
            if self.fail {
                return Self::not_configured();
            }
            Ok(Self::issue())
        }
    }

    async fn svc(fail: bool) -> (TempDb, Services) {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let services = Services::new(store).with_sentry_engine(Arc::new(StubSentry { fail }));
        (tmp, services)
    }

    #[tokio::test]
    async fn not_configured_maps_to_internal() {
        let (_tmp, s) = svc(true).await;
        assert!(matches!(
            s.sentry_auth_status().await,
            Err(Error::Internal(_))
        ));
        assert!(matches!(
            s.sentry_list_issues(None, None, None, None).await,
            Err(Error::Internal(_))
        ));
        assert!(matches!(
            s.sentry_search_issues("boom".into(), None, None).await,
            Err(Error::Internal(_))
        ));
        assert!(matches!(
            s.sentry_list_projects(None).await,
            Err(Error::Internal(_))
        ));
        assert!(matches!(
            s.sentry_get_issue("1".into()).await,
            Err(Error::Internal(_))
        ));
        assert!(matches!(
            s.sentry_resolve_issue("1".into()).await,
            Err(Error::Internal(_))
        ));
        assert!(matches!(
            s.sentry_ignore_issue("1".into()).await,
            Err(Error::Internal(_))
        ));
        assert!(matches!(
            s.sentry_assign_issue("1".into(), Some("u1".into())).await,
            Err(Error::Internal(_))
        ));
    }

    #[tokio::test]
    async fn invalid_status_is_invalid_params() {
        let (_tmp, s) = svc(false).await;
        assert!(matches!(
            s.sentry_list_issues(None, Some("bogus".into()), None, None)
                .await,
            Err(Error::InvalidParams(_))
        ));
    }

    #[tokio::test]
    async fn success_serializes_as_bare_object_and_arrays() {
        let (_tmp, s) = svc(false).await;

        let status = s.sentry_auth_status().await.unwrap();
        assert!(status.is_object());
        assert_eq!(status["authenticated"], true);
        assert_eq!(status["organization"], "acme");

        for arr in [
            s.sentry_list_issues(None, None, None, None).await.unwrap(),
            s.sentry_search_issues("boom".into(), None, None)
                .await
                .unwrap(),
        ] {
            assert!(arr.is_array(), "expected bare array, got {arr}");
            assert!(arr.get("items").is_none(), "no envelope");
            assert_eq!(arr[0]["shortId"], "PROJ-1");
        }
    }

    #[tokio::test]
    async fn list_projects_returns_bare_array() {
        let (_tmp, s) = svc(false).await;
        let v = s.sentry_list_projects(None).await.unwrap();
        assert!(v.is_array(), "expected bare array, got {v}");
        assert!(v.get("items").is_none(), "no envelope");
        assert_eq!(v[0]["slug"], "web");
        assert_eq!(v[0]["isMember"], true);
    }

    #[tokio::test]
    async fn p1_get_and_p2_writes_return_bare_object() {
        let (_tmp, s) = svc(false).await;
        for v in [
            s.sentry_get_issue("WEB-1".into()).await.unwrap(),
            s.sentry_resolve_issue("1".into()).await.unwrap(),
            s.sentry_ignore_issue("1".into()).await.unwrap(),
            s.sentry_assign_issue("1".into(), Some("user-1".into()))
                .await
                .unwrap(),
            s.sentry_assign_issue("1".into(), None).await.unwrap(),
        ] {
            assert!(v.is_object(), "expected bare object, got {v}");
            assert_eq!(v["shortId"], "PROJ-1");
            assert!(v.get("items").is_none(), "no envelope");
        }
    }
}

/// Daemon-startup heal sweep (iter#1c): sessions left non-terminal across a
/// crash must be rewritten to a non-active status so the FE does not surface a
/// phantom "Thinking" spinner the next time the chat is opened.
mod heal_stale_agent_sessions {
    use intent_core::{now_iso, AgentId, AgentSession, AgentStatus, WorkspaceId};
    use intent_store::Store;

    use super::{workspace, TempDb};
    use crate::Services;

    fn mk_session(ws: &WorkspaceId, id: &str, status: AgentStatus) -> AgentSession {
        let ts = now_iso();
        AgentSession {
            id: AgentId::from(id),
            workspace_id: ws.clone(),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: id.to_string(),
            name_explicitly_set: false,
            model: None,
            provider: None,
            system_prompt: None,
            specialist: None,
            status,
            is_active: matches!(
                status,
                AgentStatus::Active | AgentStatus::Processing | AgentStatus::Waiting
            ),
            messages: vec![],
            stats: None,
            task_note_id: None,
            skip_auto_commit: false,
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
            stop_reason: None,
        }
    }

    #[tokio::test]
    async fn rewrites_active_processing_waiting_to_runtime_idle() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws = WorkspaceId::new();
        store.insert_workspace(&workspace(&ws)).await.expect("ws");

        // Three stale shapes (the FE's `isActiveAgentThread` statuses) + four
        // shapes the heal MUST leave untouched (pending, both idle variants,
        // and the terminal completed/error/deleted family).
        store
            .insert_agent_session(&mk_session(&ws, "stale-active", AgentStatus::Active))
            .await
            .expect("stale-active");
        store
            .insert_agent_session(&mk_session(
                &ws,
                "stale-processing",
                AgentStatus::Processing,
            ))
            .await
            .expect("stale-processing");
        store
            .insert_agent_session(&mk_session(&ws, "stale-waiting", AgentStatus::Waiting))
            .await
            .expect("stale-waiting");
        store
            .insert_agent_session(&mk_session(&ws, "untouched-pending", AgentStatus::Pending))
            .await
            .expect("pending");
        store
            .insert_agent_session(&mk_session(
                &ws,
                "untouched-idle-lc",
                AgentStatus::RuntimeIdle,
            ))
            .await
            .expect("idle-lc");
        store
            .insert_agent_session(&mk_session(&ws, "untouched-idle-uc", AgentStatus::Idle))
            .await
            .expect("idle-uc");
        store
            .insert_agent_session(&mk_session(
                &ws,
                "untouched-completed",
                AgentStatus::Completed,
            ))
            .await
            .expect("completed");
        store
            .insert_agent_session(&mk_session(&ws, "untouched-error", AgentStatus::Error))
            .await
            .expect("error");

        let services = Services::new(store.clone());
        let healed = services
            .heal_stale_agent_sessions()
            .await
            .expect("heal sweep");
        assert_eq!(healed, 3, "exactly the three stale shapes were rewritten");

        // The three stale sessions are now non-active runtime-idle.
        for id in ["stale-active", "stale-processing", "stale-waiting"] {
            let s = store
                .get_agent_session(&AgentId::from(id))
                .await
                .expect("reload");
            assert_eq!(s.status, AgentStatus::RuntimeIdle, "{id} healed to idle");
            assert!(!s.is_active, "{id} is_active cleared");
        }

        // Every untouched session keeps its persisted status, including the
        // pending shape (waiting on a first turn) and the terminal family.
        for (id, want) in [
            ("untouched-pending", AgentStatus::Pending),
            ("untouched-idle-lc", AgentStatus::RuntimeIdle),
            ("untouched-idle-uc", AgentStatus::Idle),
            ("untouched-completed", AgentStatus::Completed),
            ("untouched-error", AgentStatus::Error),
        ] {
            let s = store
                .get_agent_session(&AgentId::from(id))
                .await
                .expect("reload");
            assert_eq!(s.status, want, "{id} status unchanged");
        }

        // A second sweep is a no-op: the heal is idempotent.
        let healed_again = services
            .heal_stale_agent_sessions()
            .await
            .expect("heal sweep (idempotent)");
        assert_eq!(healed_again, 0);
    }
}

/// Daemon-owned initial-agent orchestration inside `workspace.create`
/// (PROTOCOL §5.1): agent row + exactly-once prompt delivery inside the
/// idempotency scope. No `AgentManager` is attached here, so delivery takes
/// the store-only `agent_send_message_op` fallback (same persist the runtime
/// path starts from).
mod initial_agent_orchestration {
    use std::time::Duration;

    use intent_core::{AgentId, WorkspaceApi, WorkspaceCreate, WorkspaceCreateInitialAgent};
    use intent_store::Store;
    use serde_json::{json, Value};

    use super::{TempDb, WorkspacesRoot};
    use crate::{EventBus, Services, SubscriptionFilter};

    fn create_input(agent: Option<WorkspaceCreateInitialAgent>) -> WorkspaceCreate {
        WorkspaceCreate {
            title: Some("WS".to_string()),
            branch: Some("feat/initial-agent".to_string()),
            initial_agent: agent,
            ..Default::default()
        }
    }

    /// Drain published events (unfiltered subscription) until the bus goes
    /// quiet, flattening batches into one ordered list of event types.
    async fn drain_event_types(sub: &mut crate::Subscription) -> Vec<String> {
        let mut types = Vec::new();
        while let Ok(Some(batch)) =
            tokio::time::timeout(Duration::from_millis(400), sub.recv()).await
        {
            for ev in batch {
                types.push(ev.event_type);
            }
        }
        types
    }

    #[tokio::test]
    async fn creates_agent_and_delivers_prompt_once() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let bus = EventBus::new(store.clone());
        let ws_root = WorkspacesRoot::new();
        let services = Services::new(store.clone())
            .with_workspaces_root(ws_root.path().to_path_buf())
            .with_event_bus(bus.clone());
        let mut sub = bus.subscribe(SubscriptionFilter::default());

        let requested = format!("agent-{}", uuid::Uuid::new_v4());
        let res = services
            .create_workspace(
                create_input(Some(WorkspaceCreateInitialAgent {
                    agent_id: Some(requested.clone()),
                    prompt: Some("fix the auth flow".to_string()),
                    name: Some("Auth fixer".to_string()),
                    model: Some("opus".to_string()),
                    specialist: Some("implementor".to_string()),
                    ..Default::default()
                })),
                None,
            )
            .await
            .expect("create");

        // Result carries the AgentLite (client-supplied agentId honored).
        let agent = res.initial_agent.expect("initialAgent in result");
        assert_eq!(agent["id"], Value::from(requested.as_str()));
        assert_eq!(agent["name"], "Auth fixer");

        // Session row: parentless, non-background, specialist/model persisted,
        // prompt kept as `initialMessage` (resume source).
        let session = store
            .get_agent_session(&AgentId::from(requested.as_str()))
            .await
            .expect("session");
        assert_eq!(session.workspace_id, res.workspace.id);
        assert!(session.parent_agent_id.is_none());
        assert!(!session.is_background);
        assert_eq!(session.specialist.as_deref(), Some("implementor"));
        assert_eq!(session.model.as_deref(), Some("opus"));
        assert_eq!(
            session.initial_message.as_deref(),
            Some("fix the auth flow")
        );
        // Reference-parity flags stamped on the raw session metadata
        // (workspace.service.ts:1847/1859 — the FE surface reads these to
        // classify the workspace's coordinator).
        let meta = session.metadata.as_ref().and_then(Value::as_object);
        assert_eq!(
            meta.and_then(|m| m.get("isInitialAgent")),
            Some(&Value::Bool(true)),
            "isInitialAgent stamped: {:?}",
            session.metadata
        );
        assert_eq!(
            meta.and_then(|m| m.get("isFirstWorkspaceAgent")),
            Some(&Value::Bool(true)),
            "isFirstWorkspaceAgent stamped: {:?}",
            session.metadata
        );

        // Exactly one persisted message: the user prompt.
        let messages = store
            .get_agent_messages(&AgentId::from(requested.as_str()), None)
            .await
            .expect("messages");
        assert_eq!(messages.len(), 1, "exactly one delivered prompt");
        assert_eq!(messages[0].role, "user");
        assert_eq!(
            messages[0].content,
            json!([{ "type": "text", "text": "fix the auth flow" }])
        );

        // workspace:created precedes agent:created.
        let types = drain_event_types(&mut sub).await;
        let ws_pos = types.iter().position(|t| t == "workspace:created");
        let agent_pos = types.iter().position(|t| t == "agent:created");
        assert!(ws_pos.is_some(), "workspace:created published: {types:?}");
        assert!(agent_pos.is_some(), "agent:created published: {types:?}");
        assert!(ws_pos < agent_pos, "workspace:created first: {types:?}");
    }

    /// Idempotency replay: the stored result is returned without re-running
    /// the op — no duplicate agent row and no second prompt delivery.
    #[tokio::test]
    async fn idempotent_replay_no_duplicate_agent_or_message() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws_root = WorkspacesRoot::new();
        let services =
            Services::new(store.clone()).with_workspaces_root(ws_root.path().to_path_buf());
        let key = Some("ws-agent-idem-1".to_string());

        let input = || {
            create_input(Some(WorkspaceCreateInitialAgent {
                prompt: Some("fix the auth flow".to_string()),
                ..Default::default()
            }))
        };
        let first = services
            .create_workspace(input(), key.clone())
            .await
            .expect("first create");
        let agent_id = first.initial_agent.as_ref().unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string();

        let second = services
            .create_workspace(input(), key)
            .await
            .expect("replay create");
        assert_eq!(second.workspace.id, first.workspace.id);
        assert_eq!(
            second.initial_agent.as_ref().unwrap()["id"]
                .as_str()
                .unwrap(),
            agent_id,
            "replay returns the original agent"
        );

        let sessions = store
            .list_agent_sessions(&first.workspace.id)
            .await
            .expect("sessions");
        assert_eq!(sessions.len(), 1, "no duplicate agent row");
        let messages = store
            .get_agent_messages(&AgentId::from(agent_id.as_str()), None)
            .await
            .expect("messages");
        assert_eq!(messages.len(), 1, "no second prompt delivery");
    }

    /// No-prompt idempotency replay: the row-creation move stays inside the
    /// replay guard — a second `workspace.create` with the same key returns
    /// the stored result, produces no duplicate session row, and never
    /// persists a message even though the replayed request also carried no
    /// prompt.
    #[tokio::test]
    async fn no_prompt_idempotent_replay_no_duplicate_agent() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws_root = WorkspacesRoot::new();
        let services =
            Services::new(store.clone()).with_workspaces_root(ws_root.path().to_path_buf());
        let key = Some("ws-agent-no-prompt-idem-1".to_string());

        let input = || {
            create_input(Some(WorkspaceCreateInitialAgent {
                specialist: Some("implementor".to_string()),
                ..Default::default()
            }))
        };
        let first = services
            .create_workspace(input(), key.clone())
            .await
            .expect("first create");
        let agent_id = first.initial_agent.as_ref().unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string();

        let second = services
            .create_workspace(input(), key)
            .await
            .expect("replay create");
        assert_eq!(second.workspace.id, first.workspace.id);
        assert_eq!(
            second.initial_agent.as_ref().unwrap()["id"]
                .as_str()
                .unwrap(),
            agent_id,
            "replay returns the original agent"
        );

        let sessions = store
            .list_agent_sessions(&first.workspace.id)
            .await
            .expect("sessions");
        assert_eq!(sessions.len(), 1, "no duplicate agent row on replay");
        let messages = store
            .get_agent_messages(&AgentId::from(agent_id.as_str()), None)
            .await
            .expect("messages");
        assert!(
            messages.is_empty(),
            "no prompt delivered on replay either: {messages:?}"
        );
    }

    /// No (or blank) prompt → agent row is created without a first turn
    /// (reference parity with `workspace.service.ts`: the session persists
    /// whenever `initialAgent` is present; the FE's first send starts the
    /// turn). The result carries the `AgentLite`, the row is non-background
    /// with the requested specialist/model, `metadata.initialMessage` is
    /// absent, and no messages are persisted.
    #[tokio::test]
    async fn no_prompt_creates_agent_row_without_message() {
        for prompt in [None, Some("   ".to_string())] {
            let tmp = TempDb::new();
            let store = Store::open(&tmp.path).await.expect("open store");
            let ws_root = WorkspacesRoot::new();
            let services =
                Services::new(store.clone()).with_workspaces_root(ws_root.path().to_path_buf());

            let requested = format!("agent-{}", uuid::Uuid::new_v4());
            let res = services
                .create_workspace(
                    create_input(Some(WorkspaceCreateInitialAgent {
                        agent_id: Some(requested.clone()),
                        prompt,
                        name: Some("Coordinator".to_string()),
                        model: Some("opus".to_string()),
                        specialist: Some("implementor".to_string()),
                        ..Default::default()
                    })),
                    None,
                )
                .await
                .expect("create");

            let agent = res
                .initial_agent
                .as_ref()
                .expect("initialAgent in result even without a prompt");
            assert_eq!(agent["id"], Value::from(requested.as_str()));
            assert_eq!(agent["name"], "Coordinator");

            let sessions = store
                .list_agent_sessions(&res.workspace.id)
                .await
                .expect("sessions");
            assert_eq!(sessions.len(), 1, "one session row created");

            let session = store
                .get_agent_session(&AgentId::from(requested.as_str()))
                .await
                .expect("session");
            assert_eq!(session.workspace_id, res.workspace.id);
            assert!(session.parent_agent_id.is_none());
            assert!(!session.is_background);
            assert_eq!(session.specialist.as_deref(), Some("implementor"));
            assert_eq!(session.model.as_deref(), Some("opus"));
            assert!(
                session.initial_message.is_none(),
                "metadata.initialMessage omitted when no prompt supplied: {:?}",
                session.initial_message
            );
            // Reference-parity flags land even without a prompt (parity
            // with workspace.service.ts:1847/1859).
            let meta = session.metadata.as_ref().and_then(Value::as_object);
            assert_eq!(
                meta.and_then(|m| m.get("isInitialAgent")),
                Some(&Value::Bool(true)),
                "isInitialAgent stamped for no-prompt agent: {:?}",
                session.metadata
            );
            assert_eq!(
                meta.and_then(|m| m.get("isFirstWorkspaceAgent")),
                Some(&Value::Bool(true)),
                "isFirstWorkspaceAgent stamped for no-prompt agent: {:?}",
                session.metadata
            );

            let messages = store
                .get_agent_messages(&AgentId::from(requested.as_str()), None)
                .await
                .expect("messages");
            assert!(
                messages.is_empty(),
                "no messages persisted without a prompt: {messages:?}"
            );
        }
    }

    /// No-prompt path must not persist a caller-supplied
    /// `metadata.initialMessage`: the daemon owns the `initialMessage`
    /// invariant end-to-end, so a workspace created without a prompt has an
    /// empty transcript even if the caller stuffed a stray prompt into the
    /// initial-agent metadata. Otherwise `agent_create_op`'s metadata
    /// harvest would silently promote the caller value into
    /// `AgentSession.initial_message`.
    #[tokio::test]
    async fn no_prompt_drops_caller_supplied_initial_message_in_metadata() {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let ws_root = WorkspacesRoot::new();
        let services =
            Services::new(store.clone()).with_workspaces_root(ws_root.path().to_path_buf());

        let requested = format!("agent-{}", uuid::Uuid::new_v4());
        let res = services
            .create_workspace(
                create_input(Some(WorkspaceCreateInitialAgent {
                    agent_id: Some(requested.clone()),
                    prompt: None,
                    name: Some("Coordinator".to_string()),
                    model: Some("opus".to_string()),
                    specialist: Some("implementor".to_string()),
                    metadata: Some(json!({ "initialMessage": "stale prompt from caller" })),
                    ..Default::default()
                })),
                None,
            )
            .await
            .expect("create");

        assert!(res.initial_agent.is_some(), "initial agent row persisted");
        let session = store
            .get_agent_session(&AgentId::from(requested.as_str()))
            .await
            .expect("session");
        assert!(
            session.initial_message.is_none(),
            "caller-supplied metadata.initialMessage dropped on no-prompt create: {:?}",
            session.initial_message
        );
        let messages = store
            .get_agent_messages(&AgentId::from(requested.as_str()), None)
            .await
            .expect("messages");
        assert!(
            messages.is_empty(),
            "no messages persisted on no-prompt create: {messages:?}"
        );
    }
}

/// Daemon-owned clone orchestration inside `workspace.create` (PROTOCOL §5.1):
/// when `githubUrl` is set and no local repo is provided, the daemon clones
/// first, sets `repositoryPath` from the clone target, derives owner/name
/// from the URL, and streams `git:clone:*` under the new workspace id.
/// Failures fail the whole create pre-insert.
mod clone_orchestration {
    use std::path::PathBuf;
    use std::time::Duration;

    use intent_core::{WorkspaceApi, WorkspaceCreate};
    use intent_store::Store;

    use super::TempDb;
    use crate::{EventBus, Services, SubscriptionFilter};

    /// Drop guard removing a temp directory tree.
    struct TempDir(PathBuf);
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn unique_dir(prefix: &str) -> TempDir {
        let p = std::env::temp_dir().join(format!("{prefix}-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&p).unwrap();
        TempDir(p)
    }

    /// Init a small git repo with one commit; returns the guard.
    fn seed_repo(prefix: &str) -> TempDir {
        let dir = unique_dir(prefix);
        let repo = git2::Repository::init(&dir.0).unwrap();
        {
            let mut cfg = repo.config().unwrap();
            cfg.set_str("user.name", "Tester").unwrap();
            cfg.set_str("user.email", "t@e.dev").unwrap();
        }
        std::fs::write(dir.0.join("README.md"), "init\n").unwrap();
        let mut index = repo.index().unwrap();
        index
            .add_all(["*"].iter(), git2::IndexAddOption::DEFAULT, None)
            .unwrap();
        index.write().unwrap();
        let tree = repo.find_tree(index.write_tree().unwrap()).unwrap();
        let sig = git2::Signature::now("Tester", "t@e.dev").unwrap();
        repo.commit(Some("HEAD"), &sig, &sig, "chore: init", &tree, &[])
            .unwrap();
        dir
    }

    async fn drain_event_types(sub: &mut crate::Subscription) -> Vec<String> {
        let mut types = Vec::new();
        while let Ok(Some(batch)) =
            tokio::time::timeout(Duration::from_millis(400), sub.recv()).await
        {
            for ev in batch {
                types.push(ev.event_type);
            }
        }
        types
    }

    /// `githubUrl` → daemon clones via `file://` (fast, hermetic), sets
    /// `repositoryPath` to the clone target, and streams `git:clone:progress`
    /// + `git:clone:done` under the new workspace id before the row insert
    /// and `workspace:created`. Owner/name derivation from a real GitHub URL
    /// is covered by `clone_ops::tests::parse_owner_repo_handles_https_and_ssh`.
    #[tokio::test]
    async fn create_clones_github_url_before_worktree() {
        let source = seed_repo("intentd-clone-src");
        let root = unique_dir("intentd-clone-root");
        let clone_target = unique_dir("intentd-clone-target");
        let clone_dir: PathBuf = clone_target.0.join("checkout");

        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let bus = EventBus::new(store.clone());
        let svc = Services::new(store)
            .with_workspaces_root(root.0.clone())
            .with_event_bus(bus.clone());
        let mut sub = bus.subscribe(SubscriptionFilter::default());

        let url = format!("file://{}", source.0.to_string_lossy());
        let res = svc
            .create_workspace(
                WorkspaceCreate {
                    title: Some("Cloned WS".to_string()),
                    branch: Some("feat/clone-orch".to_string()),
                    github_url: Some(url),
                    clone_path: Some(clone_dir.to_string_lossy().to_string()),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("create");
        let ws = res.workspace;
        assert_eq!(
            ws.repository_path.as_deref(),
            Some(clone_dir.to_string_lossy().as_ref()),
            "repositoryPath set from clone target"
        );
        assert!(clone_dir.join(".git").exists(), "clone actually happened");

        // The clone streamed progress + a terminal ok done before the row
        // insert and `workspace:created`.
        let types = drain_event_types(&mut sub).await;
        let starting_pos = types.iter().position(|t| t == "git:clone:progress");
        let done_pos = types.iter().position(|t| t == "git:clone:done");
        let ws_pos = types.iter().position(|t| t == "workspace:created");
        assert!(
            starting_pos.is_some(),
            "git:clone:progress observed: {types:?}"
        );
        assert!(done_pos.is_some(), "git:clone:done observed: {types:?}");
        assert!(
            done_pos < ws_pos,
            "clone completes before workspace insert: {types:?}"
        );
    }

    /// A clone that cannot succeed (unreachable target under `file://`) fails
    /// the whole `workspace.create` — no workspace row is persisted.
    #[tokio::test]
    async fn clone_failure_fails_create_no_row_persisted() {
        let root = unique_dir("intentd-clone-fail-root");
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let bus = EventBus::new(store.clone());
        let svc = Services::new(store.clone())
            .with_workspaces_root(root.0.clone())
            .with_event_bus(bus.clone());

        let missing = format!("/does/not/exist/{}.git", uuid::Uuid::new_v4());
        let target = unique_dir("intentd-clone-fail-target");
        let clone_dir: PathBuf = target.0.join("checkout");
        let err = svc
            .create_workspace(
                WorkspaceCreate {
                    title: Some("Nope".to_string()),
                    branch: Some("feat/clone-fail".to_string()),
                    github_url: Some(format!("file://{missing}")),
                    clone_path: Some(clone_dir.to_string_lossy().to_string()),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect_err("create must fail on clone failure");
        assert!(
            format!("{err}").contains("clone"),
            "error mentions clone: {err}"
        );
        let list = store.list_workspaces(true).await.expect("list");
        assert!(list.is_empty(), "no row persisted on clone failure");
    }

    /// A `githubUrl` alongside an existing local `repositoryPath` (a real git
    /// repo on disk) is a no-op for clone: the daemon uses the local repo as
    /// the workspace's `repositoryPath` and does not re-clone.
    #[tokio::test]
    async fn github_url_skipped_when_local_repo_is_present() {
        let repo = seed_repo("intentd-clone-skip-src");
        let root = unique_dir("intentd-clone-skip-root");
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let bus = EventBus::new(store.clone());
        let svc = Services::new(store)
            .with_workspaces_root(root.0.clone())
            .with_event_bus(bus.clone());
        let mut sub = bus.subscribe(SubscriptionFilter::default());

        let res = svc
            .create_workspace(
                WorkspaceCreate {
                    title: Some("Skip clone".to_string()),
                    branch: Some("feat/clone-skip".to_string()),
                    repository_path: Some(repo.0.to_string_lossy().to_string()),
                    github_url: Some("file:///does-not-matter/owner/name.git".to_string()),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect("create");
        assert_eq!(
            res.workspace.repository_path.as_deref(),
            Some(repo.0.to_string_lossy().as_ref()),
            "existing local repo wins over githubUrl"
        );
        let types = drain_event_types(&mut sub).await;
        assert!(
            types.iter().all(|t| t != "git:clone:progress"),
            "no clone attempted: {types:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// A6 verifier hooks: surgical mutations schedule line-attribution recompute;
// composition-root sweeper spawns as an abortable task.
// ---------------------------------------------------------------------------
mod line_attribution_hooks {
    use super::*;

    fn assert_debouncer_scheduled(svc: &Services, ws: &WorkspaceId, id: &NoteId) {
        let map = svc
            .line_attribution_debouncers
            .lock()
            .expect("debouncer lock");
        assert!(
            map.contains_key(&(ws.clone(), id.clone())),
            "schedule_line_attribution_recompute must register a debounced timer",
        );
    }

    #[tokio::test]
    async fn task_update_status_schedules_recompute() {
        let (_tmp, svc, ws, id) = setup("- [ ] alpha\n- [ ] beta").await;
        svc.task_update_status(ws.clone(), id.clone(), "beta".into(), "in-progress".into())
            .await
            .expect("updateStatus");
        assert_debouncer_scheduled(&svc, &ws, &id);
    }

    #[tokio::test]
    async fn task_update_schedules_recompute() {
        let (_tmp, svc, ws, id) = setup("- [ ] alpha").await;
        svc.task_update(ws.clone(), id.clone(), 1, None, Some("done".into()), None)
            .await
            .expect("task.update");
        assert_debouncer_scheduled(&svc, &ws, &id);
    }

    #[tokio::test]
    async fn convert_task_blocks_schedules_recompute() {
        let content = "intro\n@@@task\n# Build API\nBuild the thing.\n@@@\ntail";
        let (_tmp, svc, ws, id) = setup(content).await;
        svc.convert_task_blocks(ws.clone(), id.clone(), None)
            .await
            .expect("convertBlocks");
        assert_debouncer_scheduled(&svc, &ws, &id);
    }

    #[tokio::test]
    async fn comment_add_schedules_recompute() {
        let (_tmp, svc, ws, id) = setup("Hello world, this is a test sentence.").await;
        svc.comment_add(
            ws.clone(),
            id.clone(),
            "this is a test sentence".into(),
            "test".into(),
            "nice".into(),
            None,
            None,
            None,
        )
        .await
        .expect("comment.add");
        assert_debouncer_scheduled(&svc, &ws, &id);
    }

    #[tokio::test]
    async fn spawn_crdt_session_sweep_loop_returns_abortable_handle() {
        let (_tmp, svc, _ws, _id) = setup("").await;
        let handle = svc.spawn_crdt_session_sweep_loop();
        assert!(!handle.is_finished(), "sweep loop should stay running");
        handle.abort();
        let joined = handle.await;
        assert!(
            matches!(&joined, Err(e) if e.is_cancelled()),
            "aborted sweep loop must join with a cancellation error: {joined:?}",
        );
    }
}

// ---------------------------------------------------------------------------
// REV-1: `browser.exec` routes through the injected `AgentReverseDispatch`.
// ---------------------------------------------------------------------------
mod browser_exec_reverse {
    use std::sync::{Arc, Mutex};

    use intent_core::{
        AgentReverseDispatch, BoxFuture, Error, ReverseDispatchError, WorkspaceApi, WorkspaceId,
    };
    use intent_store::Store;
    use serde_json::{json, Value};

    use super::{TempDb, WorkspacesRoot};
    use crate::Services;

    #[derive(Default)]
    struct RecordingDispatch {
        calls: Mutex<Vec<(String, Value)>>,
        reply: Mutex<Option<Value>>,
        err: Mutex<Option<ReverseDispatchError>>,
    }

    impl RecordingDispatch {
        fn with_reply(reply: Value) -> Arc<Self> {
            let d = Self::default();
            *d.reply.lock().unwrap() = Some(reply);
            Arc::new(d)
        }
        fn with_error(err: ReverseDispatchError) -> Arc<Self> {
            let d = Self::default();
            *d.err.lock().unwrap() = Some(err);
            Arc::new(d)
        }
    }

    impl AgentReverseDispatch for RecordingDispatch {
        fn is_connected(&self) -> bool {
            true
        }
        fn dispatch<'a>(
            &'a self,
            method: &'a str,
            params: Value,
        ) -> BoxFuture<'a, Result<Value, ReverseDispatchError>> {
            self.calls
                .lock()
                .unwrap()
                .push((method.to_string(), params.clone()));
            let reply = self.reply.lock().unwrap().clone();
            let err = self.err.lock().unwrap().clone();
            Box::pin(async move {
                if let Some(e) = err {
                    return Err(e);
                }
                Ok(reply.unwrap_or(json!({ "success": true, "results": [] })))
            })
        }
    }

    async fn services_with(
        dispatch: Arc<dyn AgentReverseDispatch>,
    ) -> (TempDb, WorkspacesRoot, Services) {
        let tmp = TempDb::new();
        let root = WorkspacesRoot::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let svc = Services::new(store)
            .with_workspaces_root(root.path().to_path_buf())
            .with_reverse_dispatch(dispatch);
        (tmp, root, svc)
    }

    #[tokio::test]
    async fn browser_exec_without_dispatch_returns_no_client_error() {
        let tmp = TempDb::new();
        let root = WorkspacesRoot::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        let svc = Services::new(store).with_workspaces_root(root.path().to_path_buf());
        let err = svc
            .browser_exec(
                WorkspaceId::from("ws-1"),
                vec![json!({"action":"listTabs"})],
                None,
                None,
            )
            .await
            .expect_err("no dispatch");
        assert!(matches!(err, Error::Internal(m) if m.contains("no client connected")));
    }

    #[tokio::test]
    async fn browser_exec_forwards_actions_and_returns_single_result_envelope() {
        let dispatch = RecordingDispatch::with_reply(json!({
            "success": true,
            "results": [{ "action": "listTabs", "success": true, "result": [] }],
        }));
        let (_tmp, _root, svc) = services_with(dispatch.clone()).await;
        let out = svc
            .browser_exec(
                WorkspaceId::from("ws-1"),
                vec![json!({ "action": "listTabs" })],
                Some("tab-1".to_string()),
                None,
            )
            .await
            .expect("ok");
        assert_eq!(out["action"], "listTabs");
        assert_eq!(out["success"], true);
        let calls = dispatch.calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "browser.exec");
        assert_eq!(calls[0].1["tabId"], "tab-1");
        assert_eq!(calls[0].1["actions"].as_array().unwrap().len(), 1);
        // REV-1: attribution — the `WorkspaceId` argument must be threaded
        // into the forwarded reverse-RPC params so the FE sees the same
        // envelope shape the client-triggered `browser.exec` path emits.
        assert_eq!(calls[0].1["workspaceId"], "ws-1");
    }

    #[tokio::test]
    async fn browser_exec_rejects_empty_actions_with_invalid_params() {
        let dispatch = RecordingDispatch::with_reply(json!({ "success": true, "results": [] }));
        let (_tmp, _root, svc) = services_with(dispatch.clone()).await;
        let err = svc
            .browser_exec(WorkspaceId::from("ws-1"), vec![], None, None)
            .await
            .expect_err("empty batch");
        assert!(matches!(err, Error::InvalidParams(m) if m.contains("non-empty")));
        // Guard runs before dispatch, so nothing is forwarded downstream.
        assert!(dispatch.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn browser_exec_surfaces_transport_error() {
        let dispatch = RecordingDispatch::with_error(ReverseDispatchError::Transport {
            code: 0,
            message: "timeout".into(),
        });
        let (_tmp, _root, svc) = services_with(dispatch).await;
        let err = svc
            .browser_exec(
                WorkspaceId::from("ws-1"),
                vec![json!({ "action": "listTabs" })],
                None,
                None,
            )
            .await
            .expect_err("transport");
        assert!(matches!(err, Error::Internal(m) if m.contains("timeout")));
    }
}

/// `scan_workspace_token_usage` tallies agent-session token counters into the
/// workspace's durable `tokenUsage` field, returning `true` when the materialized
/// tally changed and `false` when it matched the existing snapshot.
#[tokio::test]
async fn scan_workspace_token_usage_tallies_and_detects_change() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");
    let svc = Services::new(store);

    let sess1 = intent_core::AgentSession {
        id: intent_core::AgentId::from("agent-1"),
        workspace_id: ws.clone(),
        parent_agent_id: None,
        backend_session_id: None,
        acp_session_id: Some("s1".to_string()),
        name: "Agent One".to_string(),
        name_explicitly_set: false,
        model: Some("sonnet".to_string()),
        provider: Some("auggie".to_string()),
        system_prompt: None,
        specialist: None,
        status: intent_core::AgentStatus::Active,
        is_active: true,
        messages: vec![],
        stats: None,
        task_note_id: None,
        skip_auto_commit: false,
        completion_report: None,
        completion_report_timestamp: None,
        delegation_depth: None,
        created_at: now_iso(),
        updated_at: now_iso(),
        initial_message: None,
        context_references: None,
        image_blocks: None,
        sandbox_id: None,
        sandbox_path: None,
        sandbox_branch: None,
        stop_reason: None,
        is_background: false,
        metadata: None,
    };

    let sess2 = intent_core::AgentSession {
        id: intent_core::AgentId::from("agent-2"),
        workspace_id: ws.clone(),
        parent_agent_id: None,
        backend_session_id: None,
        acp_session_id: Some("s2".to_string()),
        name: "Agent Two".to_string(),
        name_explicitly_set: false,
        model: Some("gpt4".to_string()),
        provider: Some("openai".to_string()),
        system_prompt: None,
        specialist: None,
        status: intent_core::AgentStatus::Active,
        is_active: true,
        messages: vec![],
        stats: None,
        task_note_id: None,
        skip_auto_commit: false,
        completion_report: None,
        completion_report_timestamp: None,
        delegation_depth: None,
        created_at: now_iso(),
        updated_at: now_iso(),
        initial_message: None,
        context_references: None,
        image_blocks: None,
        sandbox_id: None,
        sandbox_path: None,
        sandbox_branch: None,
        stop_reason: None,
        is_background: false,
        metadata: None,
    };

    let ts = now_iso();
    svc.store.insert_agent_session(&sess1).await.expect("ins1");
    svc.store
        .append_agent_message(
            &intent_core::AgentId::from("agent-1"),
            "user",
            &serde_json::json!({}),
            &ts,
        )
        .await
        .expect("msg1");
    svc.store
        .append_agent_message(
            &intent_core::AgentId::from("agent-1"),
            "assistant",
            &serde_json::json!({
                "usage": {
                    "inputTokens": 10,
                    "outputTokens": 20,
                    "cacheReadTokens": 5,
                    "cacheCreationTokens": 3
                }
            }),
            &ts,
        )
        .await
        .expect("msg2");

    svc.store.insert_agent_session(&sess2).await.expect("ins2");
    svc.store
        .append_agent_message(
            &intent_core::AgentId::from("agent-2"),
            "assistant",
            &serde_json::json!({
                "usage": { "inputTokens": 15, "outputTokens": 25 }
            }),
            &ts,
        )
        .await
        .expect("msg3");

    let changed = svc.scan_workspace_token_usage(&ws).await.expect("scan ok");
    assert!(changed, "first scan writes new usage");

    let workspace = svc.get_workspace(ws.clone()).await.expect("get ws");
    let usage = workspace.token_usage.as_ref().expect("usage set");
    assert_eq!(usage.totals.input_tokens, 25);
    assert_eq!(usage.totals.output_tokens, 45);
    assert_eq!(usage.totals.cache_read_tokens, 5);
    assert_eq!(usage.totals.cache_creation_tokens, 3);
    assert_eq!(usage.by_agent_id.get("agent-1").unwrap().input_tokens, 10);
    assert_eq!(usage.by_agent_id.get("agent-2").unwrap().input_tokens, 15);
    assert_eq!(usage.by_model.get("sonnet").unwrap().input_tokens, 10);
    assert_eq!(usage.by_model.get("gpt4").unwrap().input_tokens, 15);
    assert!(usage.last_scan_at.is_some());

    let changed2 = svc.scan_workspace_token_usage(&ws).await.expect("scan2 ok");
    assert!(!changed2, "second scan finds no change");

    let ws2 = svc.get_workspace(ws.clone()).await.expect("get ws2");
    assert_eq!(ws2.token_usage.as_ref().unwrap().totals.input_tokens, 25);
}

/// `scan_all_token_usage` sweeps all non-archived workspaces and tallies each
/// one's token usage, logging errors per workspace and continuing the sweep.
#[tokio::test]
async fn scan_all_token_usage_sweeps_multiple_workspaces() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");

    let ws1 = WorkspaceId::new();
    let ws2 = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws1)).await.expect("ws1");
    store.insert_workspace(&workspace(&ws2)).await.expect("ws2");

    let svc = Services::new(store);
    let ts = now_iso();

    let sess = intent_core::AgentSession {
        id: intent_core::AgentId::from("agent-a"),
        workspace_id: ws1.clone(),
        parent_agent_id: None,
        backend_session_id: None,
        acp_session_id: None,
        name: "Agent A".to_string(),
        name_explicitly_set: false,
        model: Some("opus".to_string()),
        provider: Some("anthropic".to_string()),
        system_prompt: None,
        specialist: None,
        status: intent_core::AgentStatus::Active,
        is_active: true,
        messages: vec![],
        stats: None,
        task_note_id: None,
        skip_auto_commit: false,
        completion_report: None,
        completion_report_timestamp: None,
        delegation_depth: None,
        created_at: ts.clone(),
        updated_at: ts.clone(),
        initial_message: None,
        context_references: None,
        image_blocks: None,
        sandbox_id: None,
        sandbox_path: None,
        sandbox_branch: None,
        stop_reason: None,
        is_background: false,
        metadata: None,
    };
    svc.store.insert_agent_session(&sess).await.expect("sess");
    svc.store
        .append_agent_message(
            &intent_core::AgentId::from("agent-a"),
            "assistant",
            &serde_json::json!({"usage": {"inputTokens": 100}}),
            &ts,
        )
        .await
        .expect("msg");

    svc.scan_all_token_usage().await;

    let w1 = svc.get_workspace(ws1).await.expect("get ws1");
    assert!(w1.token_usage.is_some());
    assert_eq!(w1.token_usage.as_ref().unwrap().totals.input_tokens, 100);

    let w2 = svc.get_workspace(ws2).await.expect("get ws2");
    assert!(
        w2.token_usage.is_some(),
        "sweep writes snapshot even for zero sessions"
    );
    let usage = w2.token_usage.as_ref().unwrap();
    assert_eq!(usage.totals.input_tokens, 0);
    assert_eq!(usage.totals.output_tokens, 0);
    assert_eq!(usage.totals.cache_read_tokens, 0);
    assert_eq!(usage.totals.cache_creation_tokens, 0);
}

/// `parse_undo_metadata` extracts undo commit metadata from JSON, skipping
/// malformed entries and returning an empty vec when the input is absent/non-array.
#[test]
fn parse_undo_metadata_extracts_agent_and_file_attribution() {
    use crate::parse_undo_metadata;

    let valid = serde_json::json!([
        {
            "agentId": "agent-1",
            "linkedNoteId": "task-a",
            "files": ["src/foo.rs", "src/bar.rs"]
        },
        {
            "agentId": "agent-2",
            "files": ["test.rs"]
        },
        {
            "linkedNoteId": "task-b"
        }
    ]);
    let result = parse_undo_metadata(Some(&valid));
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].agent_id.as_deref(), Some("agent-1"));
    assert_eq!(result[0].linked_note_id.as_deref(), Some("task-a"));
    assert_eq!(result[0].files, vec!["src/foo.rs", "src/bar.rs"]);
    assert_eq!(result[1].agent_id.as_deref(), Some("agent-2"));
    assert_eq!(result[1].files, vec!["test.rs"]);
    assert!(result[2].agent_id.is_none());
    assert_eq!(result[2].linked_note_id.as_deref(), Some("task-b"));

    assert!(parse_undo_metadata(None).is_empty());
    assert!(parse_undo_metadata(Some(&serde_json::json!({}))).is_empty());

    let mixed = serde_json::json!([
        {"agentId": "valid"},
        "not an object",
        null,
        {"files": [123, "valid.rs", null]}
    ]);
    let mixed_result = parse_undo_metadata(Some(&mixed));
    assert_eq!(mixed_result.len(), 2);
    assert_eq!(mixed_result[0].agent_id.as_deref(), Some("valid"));
    assert_eq!(mixed_result[1].files, vec!["valid.rs"]);
}

/// `git_push_event` builds a git:push event with the correct schema and force flag.
#[test]
fn git_push_event_builds_correct_event_payload() {
    use crate::git_push_event;
    use intent_core::WorkspaceId;

    let ws = WorkspaceId::from("ws-123");
    let event = git_push_event(&ws, "main", "abc123", false);

    assert_eq!(event.workspace_id.0, "ws-123");
    assert_eq!(event.event_type, "git:push");
    assert_eq!(event.actor.actor_type, intent_core::ActorType::System);

    let data = event.data;
    assert_eq!(data["workspaceId"], "ws-123");
    assert_eq!(data["operation"], "push");
    assert_eq!(data["branch"], "main");
    assert_eq!(data["commit"], "abc123");
    assert_eq!(data["force"], false);

    let forced = git_push_event(&ws, "feat/test", "def456", true);
    assert_eq!(forced.data["force"], true);
    assert_eq!(forced.data["branch"], "feat/test");
    assert_eq!(forced.data["commit"], "def456");
}

/// Tests for `workspace:updated { lastActivity }` event emission (§10.1).
#[cfg(test)]
mod last_activity_events {
    use super::*;
    use crate::{EventBus, Subscription, SubscriptionFilter};
    use serde_json::Value;
    use std::time::Duration;
    use tokio::time::timeout;

    struct Harness {
        _tmp: TempDb,
        _ws_root: WorkspacesRoot,
        store: Store,
        services: Services,
        bus: EventBus,
        ws: WorkspaceId,
    }

    async fn harness() -> Harness {
        let tmp = TempDb::new();
        let ws_root = WorkspacesRoot::new();
        let store = Store::open(&tmp.path).await.expect("temp store");
        let ws = WorkspaceId::new();
        store
            .insert_workspace(&workspace(&ws))
            .await
            .expect("seed workspace");
        let bus = EventBus::new(store.clone());
        let services = Services::new(store.clone())
            .with_workspaces_root(ws_root.path().to_path_buf())
            .with_event_bus(bus.clone());
        Harness {
            _tmp: tmp,
            _ws_root: ws_root,
            store,
            services,
            bus,
            ws,
        }
    }

    fn subscribe(h: &Harness) -> Subscription {
        h.bus.subscribe(SubscriptionFilter {
            workspace_id: Some(h.ws.0.clone()),
            ..Default::default()
        })
    }

    async fn recv_one(sub: &mut Subscription) -> Value {
        let batch = timeout(Duration::from_secs(5), sub.recv())
            .await
            .expect("event delivered")
            .expect("subscription open");
        assert!(!batch.is_empty(), "expected at least one event");
        serde_json::to_value(&batch[0]).expect("serialize event")
    }

    fn assert_envelope(ev: &Value, expected_ws: &str, expected_type: &str) {
        assert_eq!(ev["workspaceId"], expected_ws);
        assert_eq!(ev["type"], expected_type);
    }

    /// After `raise_attention` (which bumps workspace.updated_at), a
    /// `workspace:updated { lastActivity }` event is emitted (after debounce).
    #[tokio::test]
    async fn raise_attention_emits_last_activity() {
        let _guard = DebounceEnvGuard::new("100");
        let h = harness().await;
        let mut sub = subscribe(&h);

        h.services
            .raise_attention(&h.ws, WorkspaceAttention::Unread)
            .await
            .expect("raise");

        // First event: workspace:attention-changed
        let ev1 = recv_one(&mut sub).await;
        assert_envelope(&ev1, &h.ws.0, "workspace:attention-changed");

        // Second event: workspace:updated { lastActivity }
        let ev2 = recv_one(&mut sub).await;
        assert_envelope(&ev2, &h.ws.0, "workspace:updated");
        assert!(ev2["data"]["changes"]["lastActivity"].is_string());

        let ws_after = h.store.get_workspace(&h.ws).await.expect("reload");
        assert_eq!(
            ev2["data"]["changes"]["lastActivity"].as_str().unwrap(),
            ws_after.updated_at.as_str()
        );
    }

    /// `dismiss_attention` only emits `workspace:updated { lastActivity }` when
    /// attention actually changed (idempotent no-op on already-clear).
    #[tokio::test]
    async fn dismiss_attention_idempotent() {
        let _guard = DebounceEnvGuard::new("100");
        let h = harness().await;

        // Raise first so we have something to dismiss.
        h.services
            .raise_attention(&h.ws, WorkspaceAttention::Unread)
            .await
            .expect("raise");
        tokio::time::sleep(Duration::from_millis(200)).await;

        let mut sub = subscribe(&h);

        // Dismiss (should emit both events).
        h.services
            .dismiss_attention(h.ws.clone())
            .await
            .expect("dismiss");

        let ev1 = recv_one(&mut sub).await;
        assert_envelope(&ev1, &h.ws.0, "workspace:attention-changed");

        let ev2 = recv_one(&mut sub).await;
        assert_envelope(&ev2, &h.ws.0, "workspace:updated");

        // Dismiss again (no-op, no events).
        h.services
            .dismiss_attention(h.ws.clone())
            .await
            .expect("dismiss again");

        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            timeout(Duration::from_millis(50), sub.recv())
                .await
                .is_err(),
            "no event on idempotent dismiss"
        );
    }

    /// Rapid bumps to the same workspace (e.g., multiple raise_attention calls)
    /// coalesce into one `workspace:updated { lastActivity }` event carrying
    /// the latest derived value.
    #[tokio::test]
    async fn burst_coalescing() {
        let _guard = DebounceEnvGuard::new("200");
        let h = harness().await;
        let mut sub = subscribe(&h);

        // Raise attention multiple times (each bumps workspace.updated_at).
        for i in 0..4 {
            h.services
                .raise_attention(
                    &h.ws,
                    if i % 2 == 0 {
                        WorkspaceAttention::Unread
                    } else {
                        WorkspaceAttention::ReviewRequired
                    },
                )
                .await
                .expect("raise");
        }

        // Drain all `workspace:attention-changed` events emitted during the burst.
        tokio::time::sleep(Duration::from_millis(50)).await;
        while timeout(Duration::from_millis(10), sub.recv()).await.is_ok() {}

        // Wait for the debounce window to fire.
        tokio::time::sleep(Duration::from_millis(300)).await;

        // Should see exactly one workspace:updated { lastActivity }.
        let ev = recv_one(&mut sub).await;
        assert_envelope(&ev, &h.ws.0, "workspace:updated");
        assert!(ev["data"]["changes"]["lastActivity"].is_string());

        // No second event (coalesced).
        assert!(
            timeout(Duration::from_millis(100), sub.recv())
                .await
                .is_err(),
            "burst coalesced into one event"
        );

        // The emitted lastActivity matches a fresh workspace.get.
        let ws_after = h.store.get_workspace(&h.ws).await.expect("reload");
        let mut ws_enriched = ws_after.clone();
        h.services.derive_last_activity(&mut ws_enriched).await;
        assert_eq!(
            ev["data"]["changes"]["lastActivity"].as_str().unwrap(),
            ws_enriched.last_activity.as_deref().unwrap()
        );
    }

    /// `scan_workspace_token_usage` only emits `workspace:updated { lastActivity }`
    /// when the token tallies actually changed (idempotent re-scan is silent).
    #[tokio::test]
    async fn token_usage_scan_only_on_change() {
        let _guard = DebounceEnvGuard::new("100");
        let h = harness().await;

        // First scan (no prior usage, should emit).
        let changed1 = h
            .services
            .scan_workspace_token_usage(&h.ws)
            .await
            .expect("scan 1");
        assert!(changed1, "first scan changed (none -> zero)");

        tokio::time::sleep(Duration::from_millis(200)).await;
        let mut sub = subscribe(&h);

        // Second scan (tallies unchanged, no emit).
        let changed2 = h
            .services
            .scan_workspace_token_usage(&h.ws)
            .await
            .expect("scan 2");
        assert!(!changed2, "second scan unchanged");

        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(
            timeout(Duration::from_millis(50), sub.recv())
                .await
                .is_err(),
            "no event on idempotent token scan"
        );
    }

    /// Finding F2: incremental token scan skips when watermark unchanged.
    #[tokio::test]
    async fn incremental_token_scan_skip_when_unchanged() {
        let h = harness().await;
        let agent_id = AgentId::new();
        h.store
            .insert_agent_session(&agent_session(&agent_id, &h.ws))
            .await
            .expect("insert session");

        let usage = serde_json::json!({ "usage": { "inputTokens": 10, "outputTokens": 5 } });
        h.store
            .append_agent_message(&agent_id, "user", &usage, &now_iso())
            .await
            .expect("append message");

        let changed1 = h
            .services
            .scan_workspace_token_usage(&h.ws)
            .await
            .expect("first scan");
        assert!(changed1, "first scan should detect change");

        let watermark1 = h
            .services
            .token_usage_watermarks
            .lock()
            .unwrap()
            .get(&h.ws)
            .copied()
            .expect("watermark set after first scan");
        assert_eq!(watermark1, 1, "watermark should be 1 after one message");

        let changed2 = h
            .services
            .scan_workspace_token_usage(&h.ws)
            .await
            .expect("second scan");
        assert!(!changed2, "second scan should skip (unchanged watermark)");
    }

    /// Finding F2: rescan when the agent_message watermark changes.
    #[tokio::test]
    async fn incremental_token_scan_rescan_on_append() {
        let h = harness().await;
        let agent_id = AgentId::new();
        h.store
            .insert_agent_session(&agent_session(&agent_id, &h.ws))
            .await
            .expect("insert session");

        let usage1 = serde_json::json!({ "usage": { "inputTokens": 10, "outputTokens": 5 } });
        h.store
            .append_agent_message(&agent_id, "user", &usage1, &now_iso())
            .await
            .expect("append message 1");

        let changed1 = h
            .services
            .scan_workspace_token_usage(&h.ws)
            .await
            .expect("first scan");
        assert!(changed1, "first scan should detect change");

        let watermark1 = h
            .services
            .token_usage_watermarks
            .lock()
            .unwrap()
            .get(&h.ws)
            .copied()
            .expect("watermark 1");
        assert_eq!(watermark1, 1, "one message");

        let usage2 = serde_json::json!({ "usage": { "inputTokens": 20, "outputTokens": 10 } });
        h.store
            .append_agent_message(&agent_id, "assistant", &usage2, &now_iso())
            .await
            .expect("append message 2");

        let changed2 = h
            .services
            .scan_workspace_token_usage(&h.ws)
            .await
            .expect("second scan");
        assert!(changed2, "second scan should detect change (new message)");

        let watermark2 = h
            .services
            .token_usage_watermarks
            .lock()
            .unwrap()
            .get(&h.ws)
            .copied()
            .expect("watermark 2");
        assert_eq!(watermark2, 2, "two messages");

        let ws = h.store.get_workspace(&h.ws).await.unwrap();
        let usage = ws.token_usage.expect("usage persisted");
        assert_eq!(usage.totals.input_tokens, 30, "10 + 20");
        assert_eq!(usage.totals.output_tokens, 15, "5 + 10");
    }

    /// Finding F2: lightweight tally matches expected aggregation.
    #[tokio::test]
    async fn incremental_token_scan_tally_parity() {
        let h = harness().await;
        let agent1 = AgentId::new();
        let agent2 = AgentId::new();
        h.store
            .insert_agent_session(&agent_session(&agent1, &h.ws))
            .await
            .expect("insert agent 1");
        h.store
            .insert_agent_session(&agent_session(&agent2, &h.ws))
            .await
            .expect("insert agent 2");

        let u1 = serde_json::json!({ "usage": { "inputTokens": 100, "outputTokens": 20 } });
        let u2 = serde_json::json!({ "_meta": { "usage": { "cacheReadTokens": 50 } } });
        h.store
            .append_agent_message(&agent1, "user", &u1, &now_iso())
            .await
            .expect("append 1");
        h.store
            .append_agent_message(&agent1, "assistant", &u2, &now_iso())
            .await
            .expect("append 2");
        h.store
            .append_agent_message(&agent2, "user", &u1, &now_iso())
            .await
            .expect("append 3");

        let changed = h
            .services
            .scan_workspace_token_usage(&h.ws)
            .await
            .expect("scan");
        assert!(changed, "scan should detect change");

        let ws = h.store.get_workspace(&h.ws).await.unwrap();
        let usage = ws.token_usage.expect("usage persisted");

        assert_eq!(usage.totals.input_tokens, 200, "100 + 100");
        assert_eq!(usage.totals.output_tokens, 40, "20 + 20");
        assert_eq!(usage.totals.cache_read_tokens, 50, "50 from agent 1");
        assert_eq!(usage.by_agent_id.len(), 2, "two agents");
        assert!(usage.last_scan_at.is_some(), "scan timestamp set");
    }

    fn agent_session(agent_id: &AgentId, ws: &WorkspaceId) -> AgentSession {
        AgentSession {
            id: agent_id.clone(),
            workspace_id: ws.clone(),
            backend_session_id: None,
            acp_session_id: None,
            name: format!("test-{}", agent_id.0),
            name_explicitly_set: false,
            model: Some("test-model".into()),
            provider: Some("test".into()),
            status: AgentStatus::Idle,
            is_active: false,
            system_prompt: None,
            messages: vec![],
            created_at: now_iso(),
            updated_at: now_iso(),
            parent_agent_id: None,
            specialist: None,
            task_note_id: None,
            skip_auto_commit: false,
            completion_report: None,
            completion_report_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            is_background: false,
            metadata: None,
            stats: None,
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
        }
    }

    /// Regression for STAB-N: busy→idle transition debounce (test a). An
    /// `agent_activity_end` followed by `agent_activity_begin` within the
    /// debounce window MUST NOT emit `workspace:activity-changed { idle }` — the
    /// pending idle flip is canceled and activity stays `AgentRunning` throughout.
    #[tokio::test]
    async fn idle_debounce_canceled_by_re_begin() {
        let _guard = DebounceEnvGuard::new("100");
        let h = harness().await;
        let mut sub = subscribe(&h);

        // Start agent activity → immediate AgentRunning event.
        h.services.agent_activity_begin(&h.ws).await;
        let ev = recv_one(&mut sub).await;
        assert_envelope(&ev, &h.ws.0, "workspace:activity-changed");
        assert_eq!(ev["data"]["activity"], "agent_running");

        // End agent activity → schedules idle flip after 100ms.
        h.services.agent_activity_end(&h.ws).await;

        // Re-begin within the window → cancels the pending idle flip and emits AgentRunning.
        tokio::time::sleep(Duration::from_millis(50)).await;
        h.services.agent_activity_begin(&h.ws).await;

        // Consume the AgentRunning event from the re-begin (0→1 transition).
        let ev_rebegin = recv_one(&mut sub).await;
        assert_envelope(&ev_rebegin, &h.ws.0, "workspace:activity-changed");
        assert_eq!(
            ev_rebegin["data"]["activity"], "agent_running",
            "re-begin emits AgentRunning on 0→1 transition"
        );

        // Wait beyond the original debounce window.
        tokio::time::sleep(Duration::from_millis(80)).await;

        // workspace_activity() MUST report AgentRunning (no idle event was emitted).
        assert_eq!(
            h.services.workspace_activity(&h.ws),
            WorkspaceActivity::AgentRunning,
            "activity stays AgentRunning when re-begin cancels debounce"
        );

        // No idle event should have been emitted.
        assert!(
            timeout(Duration::from_millis(50), sub.recv())
                .await
                .is_err(),
            "no idle event when re-begin cancels the debounce"
        );

        // Clean up.
        h.services.agent_activity_end(&h.ws).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
    }

    /// Regression for STAB-N: busy→idle transition debounce (test b). An
    /// `agent_activity_end` with no re-begin MUST emit exactly one
    /// `workspace:activity-changed { idle }` event after the debounce window.
    #[tokio::test]
    async fn idle_debounce_emits_after_window() {
        let _guard = DebounceEnvGuard::new("100");
        let h = harness().await;
        let mut sub = subscribe(&h);

        // Start agent activity → immediate AgentRunning event.
        h.services.agent_activity_begin(&h.ws).await;
        let ev = recv_one(&mut sub).await;
        assert_envelope(&ev, &h.ws.0, "workspace:activity-changed");
        assert_eq!(ev["data"]["activity"], "agent_running");

        // End agent activity → schedules idle flip after 100ms.
        h.services.agent_activity_end(&h.ws).await;

        // Before the window expires, no idle event yet.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(
            timeout(Duration::from_millis(20), sub.recv())
                .await
                .is_err(),
            "no idle event before debounce window expires"
        );

        // After the window expires, exactly one idle event.
        tokio::time::sleep(Duration::from_millis(80)).await;
        let ev_idle = recv_one(&mut sub).await;
        assert_envelope(&ev_idle, &h.ws.0, "workspace:activity-changed");
        assert_eq!(
            ev_idle["data"]["activity"], "idle",
            "idle event emitted after debounce window"
        );

        // workspace_activity() now reports Idle.
        assert_eq!(
            h.services.workspace_activity(&h.ws),
            WorkspaceActivity::Idle,
            "activity is Idle after debounce window expires"
        );

        // No duplicate events.
        assert!(
            timeout(Duration::from_millis(50), sub.recv())
                .await
                .is_err(),
            "no duplicate idle event"
        );
    }

    /// Regression for STAB-N: busy→idle transition debounce (test c).
    /// workspace_activity() MUST return AgentRunning during the grace window
    /// (before the debounce fires) so list/get/update responses and the event
    /// stream agree on the derived state.
    #[tokio::test]
    async fn idle_debounce_workspace_activity_during_grace() {
        let _guard = DebounceEnvGuard::new("100");
        let h = harness().await;

        // Start agent activity.
        h.services.agent_activity_begin(&h.ws).await;
        assert_eq!(
            h.services.workspace_activity(&h.ws),
            WorkspaceActivity::AgentRunning,
            "activity is AgentRunning while agent in-flight"
        );

        // End agent activity → schedules idle flip after 100ms.
        h.services.agent_activity_end(&h.ws).await;

        // During the grace window, workspace_activity() MUST still report AgentRunning.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            h.services.workspace_activity(&h.ws),
            WorkspaceActivity::AgentRunning,
            "workspace_activity() returns AgentRunning during grace window"
        );

        // After the window expires, workspace_activity() reports Idle.
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert_eq!(
            h.services.workspace_activity(&h.ws),
            WorkspaceActivity::Idle,
            "workspace_activity() returns Idle after grace window"
        );
    }
}
