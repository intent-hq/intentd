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

    /// Derived `activity` flips `Idle → AgentRunning → Idle` across in-flight
    /// session begin/end, reflected by `get_workspace`, and emits
    /// `workspace:activity-changed` ONLY on the zero/non-zero edges (§9.9/§10.1).
    #[tokio::test]
    async fn activity_changed_only_on_change_and_derived() {
        use intent_core::{WorkspaceActivity, WorkspaceApi};
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

        // Last session leaves flight: AgentRunning → Idle (emits idle). If the
        // nested pair had emitted, this would observe agent_running instead.
        h.services.agent_activity_end(&h.ws).await;
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
    use std::path::PathBuf;
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

        // Linkage persisted.
        let linked = svc.store().get_workspace(&ws).await.unwrap();
        assert_eq!(linked.pr_number, Some(7));

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
            .file_tracking_load_commits(ws_id, Some(10))
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
/// notes path, the memories deferral (empty, not error), and the streaming
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
            backend_session_id: None,
            acp_session_id: None,
            name: "A".to_string(),
            name_explicitly_set: false,
            model: None,
            provider: None,
            system_prompt: None,
            status: AgentStatus::default(),
            is_active: false,
            messages: vec![],
            stats: None,
            created_at: ts.clone(),
            updated_at: ts.clone(),
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

    #[tokio::test]
    async fn memories_search_returns_empty_not_error() {
        let (_tmp, store, ws) = store_with_ws().await;
        let svc = Services::new(store);
        let r = svc
            .search_memories("anything".into(), Some(ws), Some("srch-m".into()))
            .await
            .unwrap();
        assert_eq!(r["requestId"], "srch-m");
        assert_eq!(r["matches"].as_array().unwrap().len(), 0);
    }

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
        let svc = Services::new(store);
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
            .terminal_create(h.ws.clone(), 80, 24, None, Some("cat".into()))
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
            .terminal_create(h.ws.clone(), 80, 24, None, Some("cat".into()))
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
        let ids: Vec<&str> = list["terminals"]
            .as_array()
            .expect("terminals")
            .iter()
            .filter_map(|t| t["id"].as_str())
            .collect();
        assert!(
            ids.contains(&terminal_id.as_str()),
            "list contains terminal"
        );

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
            .terminal_create(h.ws.clone(), 80, 24, None, Some("false".into()))
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
            .terminal_create(h.ws.clone(), 80, 24, None, Some("cat".into()))
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
            .script_run(id, None, Some(10))
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

    /// A service that exits faster than the 2s floor is treated as a config error
    /// and is NOT auto-restarted (the ported backoff guard).
    #[tokio::test]
    async fn service_too_fast_exit_does_not_restart() {
        let h = harness().await;
        let mut sub = subscribe(&h);
        let id = create(&h, "boom", "echo boom", ScriptMode::Service).await;
        h.services.script_start(id.clone()).await.expect("start");
        drain_until(&mut sub, Duration::from_secs(5), |v| {
            (v["type"] == "script:state" && v["data"]["status"] == "exited").then_some(())
        })
        .await;
        // Past the restart delay it must stay exited, with no restart attempts.
        tokio::time::sleep(Duration::from_millis(1500)).await;
        let st = h.services.script_status(id).await.expect("status");
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
        h.services.script_start(id.clone()).await.expect("start");
        drain_until(&mut sub, Duration::from_secs(12), |v| {
            (v["type"] == "script:state"
                && v["data"]["status"] == "running"
                && v["data"]["restartCount"] == 1)
                .then_some(())
        })
        .await;
        let st = h.services.script_status(id.clone()).await.expect("status");
        assert_eq!(st["restartCount"], 1);
        h.services.script_stop(id).await.expect("stop");
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
        h.services.script_start(id.clone()).await.expect("start");
        let (url, _) = drain_until(&mut sub, Duration::from_secs(5), |v| {
            if v["type"] == "script:state" {
                v["data"]["detectedUrl"].as_str().map(str::to_string)
            } else {
                None
            }
        })
        .await;
        assert!(url.contains("localhost:3000"), "detected url: {url}");
        h.services.script_stop(id).await.expect("stop");
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
        h.services.script_start(id.clone()).await.expect("start");
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
        let term_id = list["terminals"]
            .as_array()
            .expect("terminals")
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
        h.services.script_stop(id).await.expect("stop");
    }
}
