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
        pr_status: None,
        active_pull_request: None,
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
        .task_update_note_status(ws.clone(), id.clone(), "in_progress".into())
        .await
        .expect("updateNoteStatus");
    assert_eq!(upd.status, intent_core::TaskStatus::InProgress);
    assert!(upd.note.task.as_ref().unwrap().started_at.is_some());

    // Invalid status string rejected with the TS-style message.
    assert!(svc
        .task_update_note_status(ws.clone(), id.clone(), "bogus".into())
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
    let task = note.task.unwrap();
    assert_eq!(task.status, intent_core::TaskStatus::InProgress);
    assert_eq!(task.assigned_agent_ids[0].0, agent);
}

#[tokio::test]
async fn convert_blocks_creates_children_idempotently() {
    let content = "intro\n@@@task\n# Build API\nBuild the thing.\n@@@\ntail";
    let (_tmp, svc, ws, id) = setup(content).await;
    let r = svc
        .convert_task_blocks(ws.clone(), id.clone())
        .await
        .expect("convertBlocks");
    assert_eq!(r.converted_count, 1);
    assert_eq!(r.created_note_ids.len(), 1);
    let updated = svc.get_note(ws.clone(), id.clone()).await.unwrap();
    assert!(updated
        .content
        .contains("- [ ] [Build API](intent://local/task/"));
    assert!(!updated.content.contains("@@@task"));

    // Re-running is idempotent: the existing child is reused, none created.
    let r2 = svc
        .convert_task_blocks(ws, id)
        .await
        .expect("convertBlocks2");
    assert_eq!(r2.converted_count, 0);
    assert!(r2.created_note_ids.is_empty());
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
    assert_eq!(only_user.len(), 1);
    assert_eq!(only_user[0].actor.actor_type, ActorType::User);

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
    assert!(none.is_empty());
}

#[tokio::test]
async fn subscribe_resolves_star_and_unsubscribe_roundtrips() {
    let (_tmp, svc, ws) = event_setup().await;
    // Empty eventTypes → error (TS resolveSubscriptionEventTypes guard).
    let err = svc.event_subscribe(ws.clone(), vec![]).await.unwrap_err();
    assert!(matches!(err, Error::Internal(m) if m.contains("eventTypes is required")));

    // Bare `*` expands to the category wildcards.
    let sub = svc
        .event_subscribe(ws.clone(), vec!["*".to_string()])
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

    use intent_core::{NoteCreate, TaskMetadata, TaskStatus, WorkspaceApi, WorkspaceId};
    use intent_store::Store;
    use serde_json::{json, Value};

    use super::{note, workspace, TempDb};
    use crate::{EventBus, Services, Subscription, SubscriptionFilter};

    struct Harness {
        _tmp: TempDb,
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
        let services = Services::new(store.clone()).with_event_bus(bus.clone());
        Harness {
            _tmp: tmp,
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

    #[tokio::test]
    async fn task_status_changed_payload() {
        let h = harness().await;
        // Pre-insert a task note directly (no event) so the only published event
        // is the status change.
        let mut tn = note(&h.ws, "task-1", "body");
        tn.task = Some(TaskMetadata {
            status: TaskStatus::NotStarted,
            ..Default::default()
        });
        h.store.insert_note(&tn).await.expect("insert task note");
        let mut sub = subscribe(&h);
        h.services
            .task_update_note_status(h.ws.clone(), tn.id.clone(), "in_progress".to_string())
            .await
            .expect("status");
        let ev = recv_one(&mut sub).await;
        assert_envelope(&ev, &h.ws.0, "task:status-changed");
        assert_eq!(ev["data"]["noteId"], "task-1");
        assert_eq!(ev["data"]["noteTitle"], "Title");
        assert_eq!(ev["data"]["previousStatus"], "not_started");
        assert_eq!(ev["data"]["newStatus"], "in_progress");
        assert!(ev["data"]["changedAt"].is_string());
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

        let resp = server
            .handle_message(&json!({
                "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                "params": {
                    "name": "add_to_note_workspace-mcp",
                    "arguments": { "noteId": "n1", "content": "more" }
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
}

// ============================================================================
// pr.* read methods over a stubbed forge (no network). Asserts the parity-exact
// status/reviews/check-run shapes and the review-thread filtering/fallback.
// ============================================================================

mod pr {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    use async_trait::async_trait;
    use intent_core::{now_iso, Error, WorkspaceApi, WorkspaceId};
    use intent_sourcecontrol::{
        AuthStatus, CheckRun, CheckState, Comment, CommentAnchor, Error as ScError, Issue,
        IssueQuery, MergeMethod, MergeOptions, MergeOutcome, Mergeability, NewPullRequest, PrPatch,
        PrQuery, PrState, PullRequest, RepoRef, Result as ScResult, Review, ReviewComment,
        ReviewThread, ReviewThreadComment, ReviewVerdict, ScCapabilities, SourceControl,
    };
    use intent_store::Store;

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
        head_seq: AtomicU64,
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
        async fn create_pr(&self, _: &RepoRef, _: NewPullRequest) -> ScResult<PullRequest> {
            unimplemented!()
        }
        async fn get_pr(&self, _: &RepoRef, _: u64) -> ScResult<PullRequest> {
            let mut pr = sample_pr();
            if self.mutate_head {
                let n = self.head_seq.fetch_add(1, Ordering::SeqCst);
                pr.head_sha = Some(format!("sha{n}"));
            }
            Ok(pr)
        }
        async fn list_prs(&self, _: &RepoRef, _: PrQuery) -> ScResult<Vec<PullRequest>> {
            if self.discover {
                Ok(vec![sample_pr()])
            } else {
                Ok(vec![])
            }
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
        async fn list_review_comments(&self, _: &RepoRef, _: u64) -> ScResult<Vec<ReviewComment>> {
            Ok(vec![ReviewComment {
                id: 5,
                body: "nit".into(),
                path: "a.rs".into(),
                line: Some(1),
                author: "rev".into(),
                created_at: "2026".into(),
                updated_at: "2026".into(),
                in_reply_to_id: None,
                url: "url".into(),
            }])
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
        async fn get_review_threads(&self, _: &RepoRef, _: u64) -> ScResult<Vec<ReviewThread>> {
            if self.fail_threads {
                return Err(ScError::Api("graphql down".into()));
            }
            Ok(vec![
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
            ])
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
        async fn list_issues(&self, _: &RepoRef, _: IssueQuery) -> ScResult<Vec<Issue>> {
            unimplemented!()
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
            .pr_merge(ws, Some("squash".into()), None, None)
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
            .pr_merge(ws, Some("ff".into()), None, None)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::Internal(m) if m.contains("mergeMethod must be one of")));
    }

    #[tokio::test]
    async fn merge_requires_active_pr() {
        let (_t, svc, ws) = setup(false, false).await;
        let err = svc.pr_merge(ws, None, None, None).await.unwrap_err();
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
        let (_t, svc, ws) = setup_with(
            StubForge {
                mutate_head: true,
                ..Default::default()
            },
            true,
        )
        .await;
        // Pause after the store is open so the SQLite pool keeps its real-time
        // connection; virtual time then makes the poll sleeps return instantly.
        tokio::time::pause();
        let v = svc
            .pr_wait_for_changes(ws, Some(30), Some(10), Some("commits".into()))
            .await
            .expect("wait");
        assert_eq!(v["changed"], true);
        assert!(v["changes"][0].as_str().unwrap().starts_with("New commit:"));
        assert!(v["iterations"].as_u64().unwrap() >= 1);
    }

    #[tokio::test]
    async fn wait_for_changes_times_out_without_changes() {
        let (_t, svc, ws) = setup(false, true).await;
        tokio::time::pause();
        let v = svc
            .pr_wait_for_changes(ws, Some(30), Some(10), Some("any".into()))
            .await
            .expect("wait");
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

        let evs = svc
            .store()
            .events_by_type(&ws_id, "pr:updated", 10)
            .await
            .unwrap();
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].data["prNumber"], 42);
        assert_eq!(evs[0].data["prStatus"], "Open");

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

        let evs = svc
            .store()
            .events_by_type(&ws_id, "pr:linked", 10)
            .await
            .unwrap();
        assert_eq!(evs.len(), 1);
        assert_eq!(evs[0].data["prNumber"], 42);
        assert_eq!(evs[0].data["prUrl"], "https://github.com/o/r/pull/42");
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
}
