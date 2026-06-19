//! Router error-matrix + dispatch unit tests using a fake `WorkspaceApi`.

use intent_core::{
    AuthorType, BoxFuture, Comment, CommentAddResult, CommentAnchor, CommentAnchorType,
    CommentLocation, CommentRespondResult, CommentRespondThread, CommentStatus, CommentType,
    CommentWire, ContentType, Error, Event, EventQueryParams, EventSubscribeResult,
    EventUnsubscribeResult, FileActivity, FileStatus, GitAgentCommitResult, GitBranches,
    GitCommitResult, GitFileStatus, GitMergeConflicts, GitStatus, Note, NoteAddInput,
    NoteAddResult, NoteCreate, NoteDeleteResult, NoteEditInput, NoteEditLinesInput,
    NoteEditLinesResult, NoteEditResult, NoteId, NoteSetContentResult, NoteTaskRow,
    NoteUpdateInput, NoteUpdateMetadataResult, NoteVisibility, ReadAssetResult, Result,
    TaskUpdateResult, Workspace, WorkspaceActivity, WorkspaceApi, WorkspaceAttention,
    WorkspaceCreate, WorkspaceEventSummary, WorkspaceId, WorkspaceStatus, WorkspaceUpdate,
};
use serde_json::Value;

use super::handle_message;

struct FakeApi;

fn sample_ws() -> Workspace {
    Workspace {
        id: WorkspaceId::from("ws-1"),
        title: "WS One".to_string(),
        branch: "main".to_string(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: "t0".to_string(),
        updated_at: "t0".to_string(),
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

fn sample_note(ws: &WorkspaceId) -> Note {
    Note {
        id: NoteId::from("note-1"),
        workspace_id: ws.clone(),
        title: "Spec".to_string(),
        content: "# Hi".to_string(),
        content_type: ContentType::Markdown,
        tags: vec![],
        is_pinned: false,
        is_archived: false,
        is_default: true,
        parent_id: None,
        visibility: NoteVisibility::Workspace,
        task: None,
        created_at: "t0".to_string(),
        updated_at: "t0".to_string(),
    }
}

fn ws_with(id: &WorkspaceId) -> Workspace {
    Workspace {
        id: id.clone(),
        ..sample_ws()
    }
}

impl WorkspaceApi for FakeApi {
    fn list_workspaces(&self, _include_archived: bool) -> BoxFuture<'_, Result<Vec<Workspace>>> {
        Box::pin(async { Ok(vec![sample_ws()]) })
    }
    fn get_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        Box::pin(async move {
            if id.as_str() == "missing" {
                return Err(Error::NotFound("workspace".to_string()));
            }
            Ok(ws_with(&id))
        })
    }
    fn create_workspace(&self, input: WorkspaceCreate) -> BoxFuture<'_, Result<Workspace>> {
        Box::pin(async move {
            let mut ws = sample_ws();
            if let Some(t) = input.title {
                ws.title = t;
            }
            Ok(ws)
        })
    }
    fn update_workspace(
        &self,
        id: WorkspaceId,
        update: WorkspaceUpdate,
    ) -> BoxFuture<'_, Result<Workspace>> {
        Box::pin(async move {
            if id.as_str() == "missing" {
                return Err(Error::NotFound("workspace".to_string()));
            }
            let mut ws = ws_with(&id);
            if let Some(t) = update.title {
                ws.title = t;
            }
            Ok(ws)
        })
    }
    fn delete_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            if id.as_str() == "missing" {
                return Err(Error::NotFound("workspace".to_string()));
            }
            Ok(())
        })
    }
    fn archive_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        Box::pin(async move {
            if id.as_str() == "missing" {
                return Err(Error::NotFound("workspace".to_string()));
            }
            let mut ws = ws_with(&id);
            ws.status = WorkspaceStatus::Archived;
            ws.archived = true;
            Ok(ws)
        })
    }
    fn unarchive_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        Box::pin(async move {
            if id.as_str() == "missing" {
                return Err(Error::NotFound("workspace".to_string()));
            }
            Ok(ws_with(&id))
        })
    }
    fn dismiss_attention(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        Box::pin(async move {
            if id.as_str() == "missing" {
                return Err(Error::NotFound("workspace".to_string()));
            }
            let mut ws = ws_with(&id);
            ws.attention = WorkspaceAttention::None;
            Ok(ws)
        })
    }
    fn mark_seen(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        Box::pin(async move {
            if id.as_str() == "missing" {
                return Err(Error::NotFound("workspace".to_string()));
            }
            let mut ws = ws_with(&id);
            ws.attention = WorkspaceAttention::None;
            Ok(ws)
        })
    }
    fn list_notes<'a>(&'a self, workspace_id: &'a WorkspaceId) -> BoxFuture<'a, Result<Vec<Note>>> {
        let id = workspace_id.clone();
        Box::pin(async move {
            if id.as_str() == "missing" {
                return Err(Error::NotFound("workspace".to_string()));
            }
            Ok(vec![sample_note(&id)])
        })
    }

    fn get_note(&self, workspace_id: WorkspaceId, note_id: NoteId) -> BoxFuture<'_, Result<Note>> {
        Box::pin(async move {
            if note_id.as_str() == "missing" {
                return Err(Error::NotFound("note".to_string()));
            }
            let mut note = sample_note(&workspace_id);
            note.id = note_id;
            Ok(note)
        })
    }

    fn create_note(
        &self,
        workspace_id: WorkspaceId,
        input: NoteCreate,
    ) -> BoxFuture<'_, Result<Note>> {
        Box::pin(async move {
            let mut note = sample_note(&workspace_id);
            note.id = NoteId::from("created");
            note.title = input.title;
            Ok(note)
        })
    }

    fn update_note(
        &self,
        workspace_id: WorkspaceId,
        note_id: NoteId,
        input: NoteUpdateInput,
    ) -> BoxFuture<'_, Result<Note>> {
        Box::pin(async move {
            if note_id.as_str() == "missing" {
                return Err(Error::NotFound("note".to_string()));
            }
            let mut note = sample_note(&workspace_id);
            note.id = note_id;
            if let Some(t) = input.title {
                note.title = t;
            }
            Ok(note)
        })
    }

    fn add_to_note(
        &self,
        _workspace_id: WorkspaceId,
        note_id: NoteId,
        input: NoteAddInput,
    ) -> BoxFuture<'_, Result<NoteAddResult>> {
        Box::pin(async move {
            Ok(NoteAddResult {
                ok: true,
                note_id,
                added_length: input.content.chars().count(),
                total_length: input.content.chars().count(),
                position: "at end".to_string(),
                old_content: String::new(),
                new_content: input.content,
                converted_count: 0,
                created_task_note_ids: vec![],
            })
        })
    }

    fn edit_note(
        &self,
        _workspace_id: WorkspaceId,
        note_id: NoteId,
        input: NoteEditInput,
    ) -> BoxFuture<'_, Result<NoteEditResult>> {
        Box::pin(async move {
            Ok(NoteEditResult {
                ok: true,
                note_id,
                old_text_length: input.old.chars().count(),
                new_text_length: input.new.chars().count(),
                match_position: 0,
                old_content: String::new(),
                new_content: input.new,
                converted_count: 0,
                created_task_note_ids: vec![],
            })
        })
    }

    fn edit_note_lines(
        &self,
        _workspace_id: WorkspaceId,
        note_id: NoteId,
        input: NoteEditLinesInput,
    ) -> BoxFuture<'_, Result<NoteEditLinesResult>> {
        Box::pin(async move {
            Ok(NoteEditLinesResult {
                ok: true,
                note_id,
                start_line: input.start,
                end_line: input.end,
                total_lines_before: 1,
                total_lines_after: 1,
                old_content: String::new(),
                new_content: input.content,
                converted_count: 0,
                created_task_note_ids: vec![],
            })
        })
    }

    fn set_note_content(
        &self,
        _workspace_id: WorkspaceId,
        note_id: NoteId,
        content: String,
        _confirm_replacement: bool,
    ) -> BoxFuture<'_, Result<NoteSetContentResult>> {
        Box::pin(async move {
            Ok(NoteSetContentResult {
                ok: true,
                note_id,
                title: "Title".to_string(),
                previous_title: Some("Title".to_string()),
                updated_at: "t1".to_string(),
                old_content: Some(String::new()),
                new_content: content,
                converted_count: 0,
                created_task_note_ids: vec![],
            })
        })
    }

    fn update_note_metadata(
        &self,
        _workspace_id: WorkspaceId,
        note_id: NoteId,
        title: Option<String>,
        tags: Option<Vec<String>>,
    ) -> BoxFuture<'_, Result<NoteUpdateMetadataResult>> {
        Box::pin(async move {
            Ok(NoteUpdateMetadataResult {
                ok: true,
                note_id,
                title,
                tags,
                updated_at: Some("t1".to_string()),
                skipped: None,
                reason: None,
            })
        })
    }

    fn delete_note(
        &self,
        _workspace_id: WorkspaceId,
        note_id: NoteId,
    ) -> BoxFuture<'_, Result<NoteDeleteResult>> {
        Box::pin(async move {
            Ok(NoteDeleteResult {
                ok: true,
                note_id,
                deleted: true,
            })
        })
    }

    fn list_note_tasks(
        &self,
        _workspace_id: WorkspaceId,
        _note_id: NoteId,
    ) -> BoxFuture<'_, Result<Vec<NoteTaskRow>>> {
        Box::pin(async move {
            Ok(vec![NoteTaskRow {
                line_number: 1,
                text: "task".to_string(),
                status: "todo".to_string(),
                task_note_id: None,
                linked_task_note_id: None,
            }])
        })
    }

    fn read_asset(
        &self,
        _workspace_id: WorkspaceId,
        asset: String,
    ) -> BoxFuture<'_, Result<ReadAssetResult>> {
        Box::pin(async move {
            Ok(ReadAssetResult {
                asset_id: asset,
                mime_type: "image/png".to_string(),
                data: "AAAA".to_string(),
                size_kb: 1,
            })
        })
    }

    fn task_update(
        &self,
        _workspace_id: WorkspaceId,
        note_id: NoteId,
        line: i64,
        _text: Option<String>,
        status: Option<String>,
        _expected: Option<String>,
    ) -> BoxFuture<'_, Result<TaskUpdateResult>> {
        Box::pin(async move {
            Ok(TaskUpdateResult {
                ok: true,
                note_id,
                line_number: line,
                previous_text: "old".to_string(),
                new_text: "new".to_string(),
                status: status.unwrap_or_else(|| "todo".to_string()),
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn comment_add(
        &self,
        _workspace_id: WorkspaceId,
        _note_id: NoteId,
        _search_context: String,
        comment_target: String,
        _comment: String,
        _kind: Option<String>,
        _author: Option<String>,
    ) -> BoxFuture<'_, Result<CommentAddResult>> {
        Box::pin(async move {
            Ok(CommentAddResult {
                success: true,
                message: format!("Comment successfully anchored to \"{comment_target}\""),
                comment_id: "c1".to_string(),
                anchored: true,
                location: CommentLocation {
                    line: 1,
                    anchored_text: comment_target,
                },
            })
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn comment_respond(
        &self,
        _workspace_id: WorkspaceId,
        note_id: NoteId,
        _thread_id: Option<String>,
        _comment_id: Option<String>,
        _comment: String,
        _kind: Option<String>,
        _author: Option<String>,
        suggestion_original: Option<String>,
        suggestion_proposed: Option<String>,
    ) -> BoxFuture<'_, Result<CommentRespondResult>> {
        Box::pin(async move {
            let now = "t0".to_string();
            let reply = Comment {
                id: "r1".to_string(),
                thread_id: "c1".to_string(),
                note_id: Some(note_id),
                kind: CommentType::Suggestion,
                content: "please change".to_string(),
                author: "Agent".to_string(),
                author_type: AuthorType::Agent,
                status: CommentStatus::Open,
                parent_id: Some("c1".to_string()),
                anchor: CommentAnchor {
                    kind: CommentAnchorType::Range,
                    start_id: Some("c1".to_string()),
                    end_id: Some("c1".to_string()),
                    point_id: None,
                },
                anchor_text: Some("target".to_string()),
                anchor_before: None,
                anchor_after: None,
                suggestion_original,
                suggestion_proposed,
                agent_id: None,
                created_at: now.clone(),
                updated_at: now,
            };
            Ok(CommentRespondResult {
                success: true,
                message: "Reply added successfully".to_string(),
                comment: CommentWire::from_comment(&reply),
                thread: CommentRespondThread {
                    thread_id: "c1".to_string(),
                    total_comments: 2,
                },
            })
        })
    }

    fn event_recent_files(
        &self,
        _workspace_id: WorkspaceId,
        limit: Option<i64>,
    ) -> BoxFuture<'_, Result<Vec<FileActivity>>> {
        Box::pin(async move {
            Ok(vec![FileActivity {
                path: format!("limit={}", limit.unwrap_or(-1)),
                relative_path: "r".to_string(),
                action: "modify".to_string(),
                timestamp: "t0".to_string(),
                actor: Some("agent:x".to_string()),
                additions: None,
                deletions: None,
            }])
        })
    }

    fn event_agent_activity(
        &self,
        _workspace_id: WorkspaceId,
        agent_id: Option<String>,
        minutes_ago: Option<i64>,
    ) -> BoxFuture<'_, Result<serde_json::Value>> {
        Box::pin(async move {
            Ok(serde_json::json!({ "agentId": agent_id, "minutesAgo": minutes_ago }))
        })
    }

    fn event_workspace_summary(
        &self,
        _workspace_id: WorkspaceId,
        minutes_ago: Option<i64>,
    ) -> BoxFuture<'_, Result<WorkspaceEventSummary>> {
        Box::pin(async move {
            Ok(WorkspaceEventSummary {
                recent_files: vec![],
                active_agents: vec![],
                event_rate: minutes_ago.unwrap_or(-1) as f64,
                top_changed_files: vec![],
            })
        })
    }

    fn event_directory_changes(
        &self,
        _workspace_id: WorkspaceId,
        dir: String,
        _limit: Option<i64>,
    ) -> BoxFuture<'_, Result<Vec<FileActivity>>> {
        Box::pin(async move {
            Ok(vec![FileActivity {
                path: dir,
                relative_path: "r".to_string(),
                action: "modify".to_string(),
                timestamp: "t0".to_string(),
                actor: Some("agent:x".to_string()),
                additions: None,
                deletions: None,
            }])
        })
    }

    fn event_query(
        &self,
        _workspace_id: WorkspaceId,
        params: EventQueryParams,
    ) -> BoxFuture<'_, Result<Vec<Event>>> {
        Box::pin(async move {
            // Echo the extracted eventType into a single event so the router
            // wiring of `EventQueryParams` is observable.
            Ok(vec![Event {
                id: "e1".to_string(),
                workspace_id: WorkspaceId::from("ws-1"),
                timestamp: "t0".to_string(),
                event_type: params.event_type.unwrap_or_default(),
                actor: Default::default(),
                session_id: None,
                correlation_id: None,
                parent_event_id: None,
                data: serde_json::json!({}),
            }])
        })
    }

    fn event_subscribe(
        &self,
        _workspace_id: WorkspaceId,
        event_types: Vec<String>,
    ) -> BoxFuture<'_, Result<EventSubscribeResult>> {
        Box::pin(async move {
            Ok(EventSubscribeResult {
                subscription_id: "sub-fake".to_string(),
                event_types,
            })
        })
    }

    fn event_unsubscribe(
        &self,
        _workspace_id: WorkspaceId,
        subscription_id: String,
    ) -> BoxFuture<'_, Result<EventUnsubscribeResult>> {
        Box::pin(async move {
            Ok(EventUnsubscribeResult {
                ok: true,
                subscription_id,
            })
        })
    }

    fn git_status(&self, workspace_id: WorkspaceId) -> BoxFuture<'_, Result<GitStatus>> {
        Box::pin(async move {
            if workspace_id.as_str() == "empty" {
                return Ok(GitStatus {
                    branch: String::new(),
                    ahead: 0,
                    behind: 0,
                    diverged: false,
                    files: vec![],
                    has_uncommitted_changes: false,
                    has_untracked_files: false,
                });
            }
            Ok(GitStatus {
                branch: "main".to_string(),
                ahead: 1,
                behind: 0,
                diverged: false,
                files: vec![FileStatus {
                    path: "src/a.ts".to_string(),
                    status: GitFileStatus::Modified,
                    staged: true,
                }],
                has_uncommitted_changes: true,
                has_untracked_files: false,
            })
        })
    }

    fn git_stage(
        &self,
        _workspace_id: WorkspaceId,
        paths: Value,
    ) -> BoxFuture<'_, Result<Vec<String>>> {
        Box::pin(async move {
            if let Value::String(s) = &paths {
                if s == "." || s == "*" || s.contains("--all") {
                    return Err(Error::Internal(
                        "Staging all files is not allowed.".to_string(),
                    ));
                }
            }
            let list = match paths {
                Value::Array(items) => items
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_string)
                    .collect(),
                Value::String(s) => s.split(',').map(|p| p.trim().to_string()).collect(),
                _ => vec![],
            };
            Ok(list)
        })
    }

    fn git_get_branches(
        &self,
        repo_path: String,
        include_remote: bool,
    ) -> BoxFuture<'_, Result<GitBranches>> {
        Box::pin(async move {
            if repo_path == "/unknown" {
                return Err(Error::InvalidParams(
                    "Unknown or unauthorized repository path".to_string(),
                ));
            }
            Ok(GitBranches {
                branches: vec!["main".to_string(), "feature".to_string()],
                remote_branches: if include_remote {
                    vec!["origin/main".to_string()]
                } else {
                    vec![]
                },
                current_branch: "feature".to_string(),
                default_branch: "main".to_string(),
            })
        })
    }

    fn git_commit(
        &self,
        _workspace_id: WorkspaceId,
        message: String,
    ) -> BoxFuture<'_, Result<GitCommitResult>> {
        Box::pin(async move {
            if message == "boom" {
                return Err(Error::Internal("nothing to commit".to_string()));
            }
            Ok(GitCommitResult {
                hash: "abc123".to_string(),
                files: vec!["src/a.ts".to_string()],
            })
        })
    }

    fn git_agent_commit(
        &self,
        _workspace_id: WorkspaceId,
        _message: String,
        files: Option<Vec<String>>,
        _user_requested: bool,
    ) -> BoxFuture<'_, Result<GitAgentCommitResult>> {
        Box::pin(async move {
            let files = files.unwrap_or_else(|| vec!["src/a.ts".to_string()]);
            let file_count = files.len() as i64;
            Ok(GitAgentCommitResult {
                hash: "def456".to_string(),
                files,
                file_count,
            })
        })
    }

    fn git_check_merge_conflicts(
        &self,
        _workspace_id: WorkspaceId,
        target_branch: Option<String>,
    ) -> BoxFuture<'_, Result<GitMergeConflicts>> {
        Box::pin(async move {
            Ok(GitMergeConflicts {
                has_conflicts: true,
                conflicted_files: vec!["src/a.ts".to_string()],
                cannot_determine: None,
                target_branch: target_branch.unwrap_or_else(|| "main".to_string()),
                current_branch: "feature".to_string(),
            })
        })
    }

    fn search_in_files(
        &self,
        _workspace_id: WorkspaceId,
        query: String,
        _opts: Option<Value>,
        request_id: Option<String>,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            if query == "bad(" {
                return Err(Error::InvalidParams("Invalid regex".to_string()));
            }
            let request_id = request_id.unwrap_or_else(|| "srch-minted".to_string());
            Ok(serde_json::json!({
                "requestId": request_id,
                "matches": [],
                "truncated": false,
            }))
        })
    }

    fn search_file_names(
        &self,
        _workspace_id: WorkspaceId,
        _pattern: String,
        _limit: Option<i64>,
        request_id: Option<String>,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            let request_id = request_id.unwrap_or_else(|| "srch-minted".to_string());
            Ok(serde_json::json!({
                "requestId": request_id,
                "files": [],
                "truncated": false,
            }))
        })
    }

    fn search_cancel(&self, _request_id: String) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async { Ok(serde_json::json!({ "ok": true })) })
    }

    fn search_messages(
        &self,
        _workspace_id: WorkspaceId,
        _query: String,
        _agent_id: Option<String>,
        _role: Option<String>,
        _limit: Option<i64>,
        request_id: Option<String>,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            let request_id = request_id.unwrap_or_else(|| "srch-minted".to_string());
            Ok(serde_json::json!({ "requestId": request_id, "matches": [] }))
        })
    }

    fn search_events(
        &self,
        _query: String,
        _workspace_id: Option<WorkspaceId>,
        _limit: Option<i64>,
        request_id: Option<String>,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            let request_id = request_id.unwrap_or_else(|| "srch-minted".to_string());
            Ok(serde_json::json!({ "requestId": request_id, "matches": [] }))
        })
    }

    fn search_memories(
        &self,
        _query: String,
        _workspace_id: Option<WorkspaceId>,
        request_id: Option<String>,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            let request_id = request_id.unwrap_or_else(|| "srch-minted".to_string());
            Ok(serde_json::json!({ "requestId": request_id, "matches": [] }))
        })
    }

    fn search_notes(
        &self,
        _query: String,
        request_id: Option<String>,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            let request_id = request_id.unwrap_or_else(|| "srch-minted".to_string());
            Ok(serde_json::json!({ "requestId": request_id, "matches": [] }))
        })
    }

    fn search_codebase(
        &self,
        _workspace_id: WorkspaceId,
        query: String,
        request_id: Option<String>,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            if query == "bad(" {
                return Err(Error::InvalidParams("Invalid regex".to_string()));
            }
            let request_id = request_id.unwrap_or_else(|| "srch-minted".to_string());
            Ok(serde_json::json!({ "requestId": request_id, "matches": [] }))
        })
    }
}

async fn call(msg: &str) -> Option<Value> {
    handle_message(&FakeApi, msg)
        .await
        .map(|s| serde_json::from_str(&s).expect("valid json response"))
}

fn err_code(v: &Value) -> i64 {
    v["error"]["code"].as_i64().expect("error code")
}

#[tokio::test]
async fn success_results_are_objects() {
    let ws = call(r#"{"jsonrpc":"2.0","id":1,"method":"workspace.list"}"#)
        .await
        .unwrap();
    assert!(ws["result"].is_object());
    assert!(ws["result"]["workspaces"].is_array());
    assert_eq!(ws["id"], serde_json::json!(1));

    let notes =
        call(r#"{"jsonrpc":"2.0","id":2,"method":"note.list","params":{"workspaceId":"ws-1"}}"#)
            .await
            .unwrap();
    assert!(notes["result"]["notes"].is_array());
}

#[tokio::test]
async fn parse_error_is_minus_32700() {
    let v = call("{not json").await.unwrap();
    assert_eq!(err_code(&v), -32700);
    assert_eq!(v["id"], Value::Null);
}

#[tokio::test]
async fn invalid_request_matrix() {
    for msg in [
        r#"[1,2,3]"#,
        r#"{"jsonrpc":"1.0","id":1,"method":"workspace.list"}"#,
        r#"{"jsonrpc":"2.0","id":1,"method":""}"#,
        r#"{"jsonrpc":"2.0","id":true,"method":"workspace.list"}"#,
    ] {
        let v = call(msg).await.unwrap();
        assert_eq!(err_code(&v), -32600, "msg={msg}");
    }
}

#[tokio::test]
async fn unknown_method_request_is_minus_32601() {
    let v = call(r#"{"jsonrpc":"2.0","id":9,"method":"nope.method"}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32601);
}

#[tokio::test]
async fn missing_workspace_id_is_minus_32602() {
    let v = call(r#"{"jsonrpc":"2.0","id":3,"method":"note.list","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
}

#[tokio::test]
async fn bad_params_type_is_minus_32602() {
    let v = call(r#"{"jsonrpc":"2.0","id":4,"method":"workspace.list","params":5}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
}

#[tokio::test]
async fn notifications_get_no_response() {
    assert!(
        handle_message(&FakeApi, r#"{"jsonrpc":"2.0","method":"workspace.list"}"#)
            .await
            .is_none()
    );
    assert!(
        handle_message(&FakeApi, r#"{"jsonrpc":"2.0","method":"nope"}"#)
            .await
            .is_none()
    );
    // id: null present IS a request needing a response.
    assert!(handle_message(
        &FakeApi,
        r#"{"jsonrpc":"2.0","id":null,"method":"workspace.list"}"#
    )
    .await
    .is_some());
}

#[tokio::test]
async fn domain_not_found_maps_to_minus_32602() {
    let v =
        call(r#"{"jsonrpc":"2.0","id":5,"method":"note.list","params":{"workspaceId":"missing"}}"#)
            .await
            .unwrap();
    assert_eq!(err_code(&v), -32602);
}

#[tokio::test]
async fn workspace_get_returns_workspace_object() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.get","params":{"workspaceId":"ws-1"}}"#,
    )
    .await
    .unwrap();
    assert!(v["result"]["workspace"].is_object());
    assert_eq!(v["result"]["workspace"]["id"], serde_json::json!("ws-1"));
}

#[tokio::test]
async fn workspace_get_missing_id_is_minus_32602_with_message() {
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"workspace.get","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Missing required parameter: workspaceId")
    );
}

#[tokio::test]
async fn workspace_get_not_found_is_minus_32602_with_message() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.get","params":{"workspaceId":"missing"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Workspace not found")
    );
}

#[tokio::test]
async fn workspace_create_returns_workspace_object() {
    let v =
        call(r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"New WS"}}"#)
            .await
            .unwrap();
    assert_eq!(
        v["result"]["workspace"]["title"],
        serde_json::json!("New WS")
    );
}

#[tokio::test]
async fn workspace_update_returns_workspace_object() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.update","params":{"workspaceId":"ws-1","title":"Renamed"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(
        v["result"]["workspace"]["title"],
        serde_json::json!("Renamed")
    );
}

#[tokio::test]
async fn workspace_lifecycle_methods_return_success_true() {
    for method in [
        "workspace.delete",
        "workspace.archive",
        "workspace.unarchive",
    ] {
        let msg = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"{method}","params":{{"workspaceId":"ws-1"}}}}"#
        );
        let v = call(&msg).await.unwrap();
        assert_eq!(v["result"]["success"], serde_json::json!(true), "{method}");
    }
}

#[tokio::test]
async fn workspace_attention_methods_clear_attention() {
    for method in ["workspace.dismissAttention", "workspace.markSeen"] {
        let msg = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"{method}","params":{{"workspaceId":"ws-1"}}}}"#
        );
        let v = call(&msg).await.unwrap();
        assert_eq!(
            v["result"]["workspace"]["attention"],
            serde_json::json!("none"),
            "{method}"
        );
    }
}

#[tokio::test]
async fn workspace_mutations_missing_id_is_minus_32602() {
    for method in [
        "workspace.update",
        "workspace.delete",
        "workspace.archive",
        "workspace.unarchive",
        "workspace.dismissAttention",
        "workspace.markSeen",
    ] {
        let msg = format!(r#"{{"jsonrpc":"2.0","id":1,"method":"{method}","params":{{}}}}"#);
        let v = call(&msg).await.unwrap();
        assert_eq!(err_code(&v), -32602, "{method}");
    }
}

#[tokio::test]
async fn note_get_returns_note_object() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"note.get","params":{"workspaceId":"ws-1","noteId":"n9"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["note"]["id"], serde_json::json!("n9"));
}

#[tokio::test]
async fn note_get_not_found_is_minus_32602_with_message() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"note.get","params":{"workspaceId":"ws-1","noteId":"missing"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(v["error"]["message"], serde_json::json!("Note not found"));
}

#[tokio::test]
async fn note_create_wraps_note_with_title() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"note.create","params":{"workspaceId":"ws-1","title":"Hi"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["note"]["title"], serde_json::json!("Hi"));
}

#[tokio::test]
async fn note_add_returns_bare_result_object() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"note.add","params":{"workspaceId":"ws-1","noteId":"n1","content":"hi"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["ok"], serde_json::json!(true));
    assert_eq!(v["result"]["noteId"], serde_json::json!("n1"));
    assert_eq!(v["result"]["newContent"], serde_json::json!("hi"));
}

#[tokio::test]
async fn note_list_tasks_returns_bare_array() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"note.listTasks","params":{"workspaceId":"ws-1","noteId":"n1"}}"#,
    )
    .await
    .unwrap();
    assert!(v["result"].is_array());
    assert_eq!(v["result"][0]["status"], serde_json::json!("todo"));
}

#[tokio::test]
async fn note_delete_returns_ok_shape() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"note.delete","params":{"workspaceId":"ws-1","noteId":"n1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["ok"], serde_json::json!(true));
    assert_eq!(v["result"]["deleted"], serde_json::json!(true));
}

#[tokio::test]
async fn note_read_asset_returns_flat_shape() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"note.readAsset","params":{"workspaceId":"ws-1","asset":"img.png"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["assetId"], serde_json::json!("img.png"));
    assert_eq!(v["result"]["mimeType"], serde_json::json!("image/png"));
    assert_eq!(v["result"]["sizeKb"], serde_json::json!(1));
}

#[tokio::test]
async fn note_methods_missing_note_id_is_minus_32602() {
    for method in [
        "note.get",
        "note.add",
        "note.edit",
        "note.editLines",
        "note.setContent",
        "note.updateMetadata",
        "note.delete",
        "note.listTasks",
    ] {
        let msg = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"{method}","params":{{"workspaceId":"ws-1"}}}}"#
        );
        let v = call(&msg).await.unwrap();
        assert_eq!(err_code(&v), -32602, "{method}");
        assert_eq!(
            v["error"]["message"],
            serde_json::json!("Missing required parameter: noteId"),
            "{method}"
        );
    }
}

#[tokio::test]
async fn note_methods_missing_workspace_id_is_minus_32602() {
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"note.get","params":{"noteId":"n1"}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("workspaceId is required")
    );
}

#[tokio::test]
async fn note_edit_missing_new_param_is_minus_32602() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"note.edit","params":{"workspaceId":"ws-1","noteId":"n1","old":"a"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Missing required parameter: new")
    );
}

#[tokio::test]
async fn task_update_returns_camel_case_result() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"task.update","params":{"workspaceId":"ws-1","noteId":"n1","line":3,"status":"done"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["ok"], serde_json::json!(true));
    assert_eq!(v["result"]["lineNumber"], serde_json::json!(3));
    assert_eq!(v["result"]["previousText"], serde_json::json!("old"));
    assert_eq!(v["result"]["status"], serde_json::json!("done"));
}

#[tokio::test]
async fn task_update_missing_line_is_minus_32602() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"task.update","params":{"workspaceId":"ws-1","noteId":"n1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Missing required parameter: line")
    );
}

#[tokio::test]
async fn comment_add_returns_location_shape() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"comment.add","params":{"workspaceId":"ws-1","noteId":"n1","searchContext":"a test sentence","commentTarget":"test","comment":"nice"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["anchored"], serde_json::json!(true));
    assert_eq!(v["result"]["commentId"], serde_json::json!("c1"));
    assert_eq!(
        v["result"]["location"]["anchoredText"],
        serde_json::json!("test")
    );
}

#[tokio::test]
async fn comment_add_missing_target_is_minus_32602() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"comment.add","params":{"workspaceId":"ws-1","noteId":"n1","searchContext":"x","comment":"nice"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Missing required parameter: commentTarget")
    );
}

#[tokio::test]
async fn comment_respond_nests_suggestion_diff_on_wire() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"comment.respond","params":{"workspaceId":"ws-1","noteId":"n1","commentId":"c1","comment":"please change","type":"suggestion","suggestionOriginal":"old text","suggestionProposed":"new text"}}"#,
    )
    .await
    .unwrap();
    // type alias + nested suggestionDiff confirm the wire DTO mapping.
    assert_eq!(
        v["result"]["comment"]["type"],
        serde_json::json!("suggestion")
    );
    assert_eq!(
        v["result"]["comment"]["suggestionDiff"]["original"],
        serde_json::json!("old text")
    );
    assert_eq!(
        v["result"]["comment"]["suggestionDiff"]["proposed"],
        serde_json::json!("new text")
    );
    // Flat storage fields must NOT leak onto the wire.
    assert!(v["result"]["comment"]["suggestionOriginal"].is_null());
    assert_eq!(v["result"]["thread"]["totalComments"], serde_json::json!(2));
}

#[tokio::test]
async fn task_comment_methods_missing_note_id_is_minus_32602() {
    for method in [
        "task.updateStatus",
        "task.updateNoteStatus",
        "task.update",
        "task.markAsTask",
        "task.convertBlocks",
        "task.assignAgent",
        "comment.add",
        "comment.list",
        "comment.getThread",
        "comment.respond",
        "comment.delete",
    ] {
        let msg = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"{method}","params":{{"workspaceId":"ws-1"}}}}"#
        );
        let v = call(&msg).await.unwrap();
        assert_eq!(err_code(&v), -32602, "{method}");
        assert_eq!(
            v["error"]["message"],
            serde_json::json!("Missing required parameter: noteId"),
            "{method}"
        );
    }
}

#[tokio::test]
async fn event_query_methods_route_and_pass_params() {
    // recentFiles passes `limit` through.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"event.recentFiles","params":{"workspaceId":"ws-1","limit":7}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"][0]["path"], serde_json::json!("limit=7"));

    // agentActivity echoes agentId + minutesAgo.
    let v = call(
        r#"{"jsonrpc":"2.0","id":2,"method":"event.agentActivity","params":{"workspaceId":"ws-1","agentId":"a1","minutesAgo":15}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["agentId"], serde_json::json!("a1"));
    assert_eq!(v["result"]["minutesAgo"], serde_json::json!(15));

    // workspaceSummary echoes minutesAgo into eventRate.
    let v = call(
        r#"{"jsonrpc":"2.0","id":3,"method":"event.workspaceSummary","params":{"workspaceId":"ws-1","minutesAgo":42}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["eventRate"], serde_json::json!(42.0));

    // directoryChanges requires `dir` and echoes it back.
    let v = call(
        r#"{"jsonrpc":"2.0","id":4,"method":"event.directoryChanges","params":{"workspaceId":"ws-1","dir":"src/"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"][0]["path"], serde_json::json!("src/"));

    // query passes EventQueryParams (eventType echoed into the result event).
    let v = call(
        r#"{"jsonrpc":"2.0","id":5,"method":"event.query","params":{"workspaceId":"ws-1","eventType":"file:changed"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"][0]["type"], serde_json::json!("file:changed"));
}

#[tokio::test]
async fn event_directory_changes_requires_dir() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"event.directoryChanges","params":{"workspaceId":"ws-1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Missing required parameter: dir")
    );
}

#[tokio::test]
async fn event_subscribe_validates_event_types() {
    // Missing eventTypes → -32602.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"event.subscribe","params":{"workspaceId":"ws-1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Missing required parameter: eventTypes")
    );

    // Present but not an array → -32602 with the array message.
    let v = call(
        r#"{"jsonrpc":"2.0","id":2,"method":"event.subscribe","params":{"workspaceId":"ws-1","eventTypes":"agent:*"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("eventTypes must be an array")
    );

    // A valid array routes through and echoes the resolved types.
    let v = call(
        r#"{"jsonrpc":"2.0","id":3,"method":"event.subscribe","params":{"workspaceId":"ws-1","eventTypes":["agent:*"]}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["subscriptionId"], serde_json::json!("sub-fake"));
    assert_eq!(v["result"]["eventTypes"], serde_json::json!(["agent:*"]));
}

#[tokio::test]
async fn event_unsubscribe_requires_subscription_id() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"event.unsubscribe","params":{"workspaceId":"ws-1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Missing required parameter: subscriptionId")
    );

    let v = call(
        r#"{"jsonrpc":"2.0","id":2,"method":"event.unsubscribe","params":{"workspaceId":"ws-1","subscriptionId":"s1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["ok"], serde_json::json!(true));
    assert_eq!(v["result"]["subscriptionId"], serde_json::json!("s1"));
}

#[tokio::test]
async fn agent_methods_validate_required_params() {
    // agent.list without workspaceId.
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"agent.list","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("workspaceId is required")
    );

    // agent.get without agentId.
    let v = call(r#"{"jsonrpc":"2.0","id":2,"method":"agent.get","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Missing required parameter: agentId")
    );

    // agent.sendMessage missing workspaceId (agentId + content present).
    let v = call(
        r#"{"jsonrpc":"2.0","id":3,"method":"agent.sendMessage","params":{"agentId":"agent-1","content":"hi"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("workspaceId is required")
    );

    // agent.rename with a blank name.
    let v = call(
        r#"{"jsonrpc":"2.0","id":4,"method":"agent.rename","params":{"agentId":"agent-1","name":"   "}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Name cannot be empty")
    );

    // agent.subscribe with a non-array eventTypes.
    let v = call(
        r#"{"jsonrpc":"2.0","id":5,"method":"agent.subscribe","params":{"workspaceId":"ws-1","eventTypes":"agent:*"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("eventTypes must be an array")
    );
}

#[tokio::test]
async fn agent_methods_are_routed_not_method_not_found() {
    // A fully-valid agent.list dispatches to the (default) impl → -32603, never
    // -32601, proving the method is registered in the dispatch table.
    let v =
        call(r#"{"jsonrpc":"2.0","id":1,"method":"agent.list","params":{"workspaceId":"ws-1"}}"#)
            .await
            .unwrap();
    assert_eq!(err_code(&v), -32603);

    // agent.getModels takes no params and must route too.
    let v = call(r#"{"jsonrpc":"2.0","id":2,"method":"agent.getModels"}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32603);
}

#[tokio::test]
async fn git_status_returns_status_object() {
    let v =
        call(r#"{"jsonrpc":"2.0","id":1,"method":"git.status","params":{"workspaceId":"ws-1"}}"#)
            .await
            .unwrap();
    assert_eq!(v["result"]["branch"], serde_json::json!("main"));
    assert_eq!(
        v["result"]["hasUncommittedChanges"],
        serde_json::json!(true)
    );
    assert_eq!(
        v["result"]["files"][0]["path"],
        serde_json::json!("src/a.ts")
    );
    assert_eq!(v["result"]["files"][0]["status"], serde_json::json!("M"));
    assert_eq!(v["result"]["files"][0]["staged"], serde_json::json!(true));
}

#[tokio::test]
async fn git_status_missing_workspace_id_is_minus_32602() {
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"git.status","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("workspaceId is required")
    );
}

#[tokio::test]
async fn git_stage_returns_ok_and_paths() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"git.stage","params":{"workspaceId":"ws-1","paths":["src/a.ts","src/b.ts"]}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["ok"], serde_json::json!(true));
    assert_eq!(
        v["result"]["paths"],
        serde_json::json!(["src/a.ts", "src/b.ts"])
    );
}

#[tokio::test]
async fn git_stage_missing_paths_is_minus_32602() {
    let v =
        call(r#"{"jsonrpc":"2.0","id":1,"method":"git.stage","params":{"workspaceId":"ws-1"}}"#)
            .await
            .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Missing required parameter: paths")
    );
}

#[tokio::test]
async fn git_stage_all_is_rejected_with_minus_32603() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"git.stage","params":{"workspaceId":"ws-1","paths":"."}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32603);
}

#[tokio::test]
async fn git_get_branches_returns_branch_shape() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"git.getBranches","params":{"repoPath":"/repo","includeRemote":true}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["currentBranch"], serde_json::json!("feature"));
    assert_eq!(v["result"]["defaultBranch"], serde_json::json!("main"));
    assert_eq!(
        v["result"]["branches"],
        serde_json::json!(["main", "feature"])
    );
    assert_eq!(
        v["result"]["remoteBranches"],
        serde_json::json!(["origin/main"])
    );
}

#[tokio::test]
async fn git_get_branches_missing_repo_path_is_minus_32602() {
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"git.getBranches","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Missing required parameter: repoPath")
    );
}

#[tokio::test]
async fn git_get_branches_unknown_repo_is_minus_32602_with_message() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"git.getBranches","params":{"repoPath":"/unknown"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Unknown or unauthorized repository path")
    );
}

#[tokio::test]
async fn git_commit_returns_ok_hash_and_files() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"git.commit","params":{"workspaceId":"ws-1","message":"msg"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["ok"], serde_json::json!(true));
    assert_eq!(v["result"]["hash"], serde_json::json!("abc123"));
    assert_eq!(v["result"]["files"], serde_json::json!(["src/a.ts"]));
}

#[tokio::test]
async fn git_commit_missing_message_is_minus_32602() {
    let v =
        call(r#"{"jsonrpc":"2.0","id":1,"method":"git.commit","params":{"workspaceId":"ws-1"}}"#)
            .await
            .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Missing required parameter: message")
    );
}

#[tokio::test]
async fn git_agent_commit_returns_ok_hash_files_and_count() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"git.agentCommit","params":{"workspaceId":"ws-1","message":"msg","files":["a.ts","b.ts"]}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["ok"], serde_json::json!(true));
    assert_eq!(v["result"]["hash"], serde_json::json!("def456"));
    assert_eq!(v["result"]["files"], serde_json::json!(["a.ts", "b.ts"]));
    assert_eq!(v["result"]["fileCount"], serde_json::json!(2));
}

#[tokio::test]
async fn git_agent_commit_missing_message_is_minus_32602() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"git.agentCommit","params":{"workspaceId":"ws-1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Missing required parameter: message")
    );
}

#[tokio::test]
async fn git_check_merge_conflicts_returns_conflict_shape() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"git.checkMergeConflicts","params":{"workspaceId":"ws-1","targetBranch":"develop"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["hasConflicts"], serde_json::json!(true));
    assert_eq!(
        v["result"]["conflictedFiles"],
        serde_json::json!(["src/a.ts"])
    );
    assert_eq!(v["result"]["targetBranch"], serde_json::json!("develop"));
    assert_eq!(v["result"]["currentBranch"], serde_json::json!("feature"));
    // `cannotDetermine` is omitted when not set (serde skip).
    assert!(v["result"].get("cannotDetermine").is_none());
}

#[tokio::test]
async fn git_check_merge_conflicts_missing_workspace_id_is_minus_32602() {
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"git.checkMergeConflicts","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("workspaceId is required")
    );
}

#[tokio::test]
async fn file_tracking_methods_are_routed_not_method_not_found() {
    // The default `WorkspaceApi` impl surfaces `-32603`; the point is that the
    // methods dispatch (never `-32601 Method not found`).
    for method in [
        "file-tracking.init",
        "file-tracking.sync",
        "file-tracking.load",
        "file-tracking.getChanges",
        "file-tracking.loadCommits",
        "file-tracking.getLineStats",
    ] {
        let msg = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"{method}","params":{{"workspaceId":"ws-1"}}}}"#
        );
        let v = call(&msg).await.unwrap();
        assert_ne!(err_code(&v), -32601, "{method} should be routed");
    }
}

#[tokio::test]
async fn file_tracking_reads_require_workspace_id() {
    for method in ["file-tracking.load", "file-tracking.getLineStats"] {
        let msg = format!(r#"{{"jsonrpc":"2.0","id":1,"method":"{method}","params":{{}}}}"#);
        let v = call(&msg).await.unwrap();
        assert_eq!(err_code(&v), -32602);
        assert_eq!(
            v["error"]["message"],
            serde_json::json!("workspaceId is required")
        );
    }
}

#[tokio::test]
async fn file_tracking_stage_requires_paths() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"file-tracking.stage","params":{"workspaceId":"ws-1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Missing required parameter: paths")
    );
}

#[tokio::test]
async fn metrics_methods_are_routed_not_method_not_found() {
    // `getAllWorkspaceStats` takes no params; the rest carry their required id.
    for (method, params) in [
        ("metrics.getWorkspaceStats", r#"{"workspaceId":"ws-1"}"#),
        ("metrics.getAgentStats", r#"{"agentId":"agent-1"}"#),
        ("metrics.getAllWorkspaceStats", r#"{}"#),
        ("metrics.clearAgentStats", r#"{"agentId":"agent-1"}"#),
    ] {
        let msg = format!(r#"{{"jsonrpc":"2.0","id":1,"method":"{method}","params":{params}}}"#);
        let v = call(&msg).await.unwrap();
        assert_ne!(err_code(&v), -32601, "{method} should be routed");
    }
}

#[tokio::test]
async fn metrics_workspace_stats_requires_workspace_id() {
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"metrics.getWorkspaceStats","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("workspaceId is required")
    );
}

#[tokio::test]
async fn metrics_agent_methods_require_agent_id() {
    for method in ["metrics.getAgentStats", "metrics.clearAgentStats"] {
        let msg = format!(r#"{{"jsonrpc":"2.0","id":1,"method":"{method}","params":{{}}}}"#);
        let v = call(&msg).await.unwrap();
        assert_eq!(err_code(&v), -32602);
        assert_eq!(
            v["error"]["message"],
            serde_json::json!("Missing required parameter: agentId")
        );
    }
}

#[tokio::test]
async fn search_in_files_requires_workspace_and_query() {
    // Missing workspaceId.
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"search.inFiles","params":{"query":"x"}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("workspaceId is required")
    );
    // Missing query.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"search.inFiles","params":{"workspaceId":"ws-1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Missing required parameter: query")
    );
}

#[tokio::test]
async fn search_in_files_echoes_request_id_and_maps_invalid_regex() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"search.inFiles","params":{"workspaceId":"ws-1","query":"x","requestId":"srch-7"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["requestId"], serde_json::json!("srch-7"));
    // Malformed regex → -32602 with the raw "Invalid regex" message.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"search.inFiles","params":{"workspaceId":"ws-1","query":"bad(","opts":{"regex":true}}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(v["error"]["message"], serde_json::json!("Invalid regex"));
}

#[tokio::test]
async fn search_file_names_requires_pattern() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"search.fileNames","params":{"workspaceId":"ws-1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Missing required parameter: pattern")
    );
}

#[tokio::test]
async fn search_cancel_requires_request_id_and_is_ok() {
    // Missing requestId → -32602.
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"search.cancel","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Missing required parameter: requestId")
    );
    // Known/unknown id alike → no-op success.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"search.cancel","params":{"requestId":"srch-1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"], serde_json::json!({ "ok": true }));
}

#[tokio::test]
async fn search_messages_requires_workspace_and_query() {
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"search.messages","params":{"query":"x"}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("workspaceId is required")
    );
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"search.messages","params":{"workspaceId":"ws-1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Missing required parameter: query")
    );
    // Routed + echoes the caller's requestId.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"search.messages","params":{"workspaceId":"ws-1","query":"x","requestId":"srch-9"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["requestId"], serde_json::json!("srch-9"));
}

#[tokio::test]
async fn search_events_requires_query_only() {
    // No workspaceId required; missing query → -32602.
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"search.events","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Missing required parameter: query")
    );
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"search.events","params":{"query":"x","requestId":"srch-e"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["requestId"], serde_json::json!("srch-e"));
}

#[tokio::test]
async fn search_memories_requires_query_only() {
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"search.memories","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"search.memories","params":{"query":"x","requestId":"srch-m"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["requestId"], serde_json::json!("srch-m"));
}

#[tokio::test]
async fn search_notes_requires_query_and_is_global() {
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"search.notes","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
    // No workspaceId → routed (global search).
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"search.notes","params":{"query":"x","requestId":"srch-n"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["requestId"], serde_json::json!("srch-n"));
}

#[tokio::test]
async fn search_codebase_requires_workspace_and_query_and_maps_regex() {
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"search.codebase","params":{"query":"x"}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("workspaceId is required")
    );
    // Malformed regex reuse surfaces the raw "Invalid regex" message.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"search.codebase","params":{"workspaceId":"ws-1","query":"bad("}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(v["error"]["message"], serde_json::json!("Invalid regex"));
}
