//! Router error-matrix + dispatch unit tests using a fake `WorkspaceApi`.

use intent_core::{
    AuthorType, BoxFuture, Comment, CommentAddResult, CommentAnchor, CommentAnchorType,
    CommentLocation, CommentResolveThreadResult, CommentRespondResult, CommentRespondThread,
    CommentStatus, CommentType, CommentWire, ContentType, Error, Event, EventQueryParams,
    EventSubscribeResult, EventUnsubscribeResult, FileActivity, FileStatus, GitAgentCommitResult,
    GitBranches, GitCommitResult, GitFileStatus, GitMergeConflicts, GitStatus, Note, NoteAddInput,
    NoteAddResult, NoteCreate, NoteDeleteResult, NoteEditInput, NoteEditLinesInput,
    NoteEditLinesResult, NoteEditResult, NoteId, NoteSetContentResult, NoteTaskRow,
    NoteUpdateInput, NoteUpdateMetadataResult, NoteVisibility, ReadAssetResult, Result,
    ScriptCreateParams, ScriptMode, TaskUpdateResult, Workspace, WorkspaceActivity, WorkspaceApi,
    WorkspaceAttention, WorkspaceCreate, WorkspaceEventSummary, WorkspaceId, WorkspaceStatus,
    WorkspaceUpdate,
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
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
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
        rev: 0,
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
    fn create_workspace(
        &self,
        input: WorkspaceCreate,
        _idempotency_key: Option<String>,
    ) -> BoxFuture<'_, Result<Workspace>> {
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
        _idempotency_key: Option<String>,
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
            // Sentinel: a stale `expectedVersion` on this id surfaces the
            // optimistic-concurrency conflict carrying the current entity.
            if note_id.as_str() == "conflict" {
                let mut current = sample_note(&workspace_id);
                current.id = note_id;
                current.rev = 7;
                return Err(Error::Conflict {
                    current: serde_json::to_value(&current).unwrap(),
                });
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
        _expected_version: Option<i64>,
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
        workspace_id: WorkspaceId,
        note_id: NoteId,
        title: Option<String>,
        tags: Option<Vec<String>>,
        _expected_version: Option<i64>,
    ) -> BoxFuture<'_, Result<NoteUpdateMetadataResult>> {
        Box::pin(async move {
            // Sentinel: a stale `expectedVersion` on this id surfaces the
            // optimistic-concurrency conflict carrying the current entity.
            if note_id.as_str() == "conflict" {
                let mut current = sample_note(&workspace_id);
                current.id = note_id;
                current.rev = 7;
                return Err(Error::Conflict {
                    current: serde_json::to_value(&current).unwrap(),
                });
            }
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
        workspace_id: WorkspaceId,
        note_id: NoteId,
        _expected_version: Option<i64>,
    ) -> BoxFuture<'_, Result<NoteDeleteResult>> {
        Box::pin(async move {
            // Sentinel: a stale `expectedVersion` on this id surfaces the
            // optimistic-concurrency conflict carrying the current snapshot.
            if note_id.as_str() == "conflict" {
                let mut current = sample_note(&workspace_id);
                current.id = note_id;
                current.rev = 7;
                return Err(Error::Conflict {
                    current: serde_json::to_value(&current).unwrap(),
                });
            }
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

    fn comment_resolve_thread(
        &self,
        _workspace_id: WorkspaceId,
        note_id: NoteId,
        thread_id: Option<String>,
        comment_id: Option<String>,
        resolved: bool,
    ) -> BoxFuture<'_, Result<CommentResolveThreadResult>> {
        Box::pin(async move {
            let target = thread_id.or(comment_id).unwrap_or_default();
            Ok(CommentResolveThreadResult {
                success: true,
                thread_id: target,
                note_id,
                resolved,
                status: if resolved { "resolved" } else { "open" }.to_string(),
                comment_count: 1,
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
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            // Echo the extracted eventType into a single event so the router
            // wiring of `EventQueryParams` is observable. The non-paginated
            // contract is a bare array (§5.10).
            let event = Event {
                id: "e1".to_string(),
                workspace_id: WorkspaceId::from("ws-1"),
                timestamp: "t0".to_string(),
                event_type: params.event_type.unwrap_or_default(),
                actor: Default::default(),
                session_id: None,
                correlation_id: None,
                parent_event_id: None,
                metadata: None,
                data: serde_json::json!({}),
            };
            Ok(serde_json::to_value(vec![event]).unwrap())
        })
    }

    fn event_subscribe(
        &self,
        _workspace_id: WorkspaceId,
        event_types: Vec<String>,
        _exclude_self: Option<bool>,
        _batch_window: Option<i64>,
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

    fn git_unstage(
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

    fn repo_list(&self) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(serde_json::json!({
                "repos": [
                    {
                        "path": "/src/intent",
                        "name": "intent",
                        "owner": "cloudlands-ai",
                        "addedAt": "t0",
                        "lastUsedAt": "t1"
                    }
                ]
            }))
        })
    }

    fn github_repos_list(
        &self,
        limit: Option<i64>,
        next_token: Option<String>,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(serde_json::json!({
                "repos": [],
                "nextToken": Value::Null,
                "echoLimit": limit,
                "echoToken": next_token,
            }))
        })
    }

    fn github_repos_search(
        &self,
        query: String,
        limit: Option<i64>,
        _next_token: Option<String>,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(serde_json::json!({
                "repos": [],
                "nextToken": Value::Null,
                "echoQuery": query,
                "echoLimit": limit,
            }))
        })
    }

    fn github_repos_get(&self, owner: String, repo: String) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move { Ok(serde_json::json!({ "repo": { "owner": owner, "name": repo } })) })
    }

    fn github_branches_list(
        &self,
        owner: String,
        repo: String,
        _limit: Option<i64>,
        _next_token: Option<String>,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(serde_json::json!({ "branches": [owner, repo], "nextToken": Value::Null }))
        })
    }

    fn github_auth_status(&self) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async {
            Ok(serde_json::json!({
                "isConfigured": true,
                "oauthUrl": "",
                "configuredButNeedsUpdate": false,
                "updatedScopes": "",
            }))
        })
    }

    fn github_connect(&self) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async { Ok(serde_json::json!({ "ok": false, "guidance": "set GITHUB_TOKEN" })) })
    }

    fn github_revoke(&self) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async { Ok(serde_json::json!({ "ok": false, "guidance": "nothing to revoke" })) })
    }

    fn github_get_user(&self) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async {
            Ok(serde_json::json!({
                "user": { "login": "octocat", "avatarUrl": "a", "htmlUrl": "h" }
            }))
        })
    }

    fn git_commit(
        &self,
        _workspace_id: WorkspaceId,
        message: String,
        _idempotency_key: Option<String>,
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

    fn terminal_create(
        &self,
        workspace_id: WorkspaceId,
        cols: u16,
        rows: u16,
        cwd: Option<String>,
        command: Option<String>,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(serde_json::json!({
                "terminalId": "pty-1",
                "workspaceId": workspace_id.as_str(),
                "cols": cols,
                "rows": rows,
                "cwd": cwd,
                "command": command,
            }))
        })
    }

    fn terminal_write(&self, terminal_id: String, data: String) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(serde_json::json!({ "ok": true, "terminalId": terminal_id, "data": data }))
        })
    }

    fn terminal_resize(
        &self,
        terminal_id: String,
        cols: u16,
        rows: u16,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(
                serde_json::json!({ "ok": true, "terminalId": terminal_id, "cols": cols, "rows": rows }),
            )
        })
    }

    fn terminal_kill(&self, terminal_id: String) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            if terminal_id == "pty-missing" {
                return Err(Error::NotFound(format!("terminal {terminal_id}")));
            }
            Ok(serde_json::json!({ "ok": true, "terminalId": terminal_id }))
        })
    }

    fn terminal_get_buffer(
        &self,
        terminal_id: String,
        max_bytes: Option<i64>,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(serde_json::json!({
                "terminalId": terminal_id,
                "data": "aGk=",
                "maxBytes": max_bytes,
            }))
        })
    }

    fn terminal_list(&self, workspace_id: WorkspaceId) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(serde_json::json!({
                "terminals": [{ "id": "pty-1", "alive": true }],
                "workspaceId": workspace_id.as_str(),
            }))
        })
    }

    fn file_read(&self, workspace_id: WorkspaceId, path: String) -> BoxFuture<'_, Result<Value>> {
        // Echo a bare string so the wire test can assert file.read is NOT
        // wrapped in an object.
        Box::pin(async move { Ok(Value::String(format!("{}:{path}", workspace_id.as_str()))) })
    }

    fn file_write(
        &self,
        _workspace_id: WorkspaceId,
        path: String,
        content: String,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(
                serde_json::json!({ "ok": true, "path": path, "size": content.encode_utf16().count() }),
            )
        })
    }

    fn file_list(&self, _workspace_id: WorkspaceId, path: String) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move { Ok(serde_json::json!([{ "name": path, "type": "file" }])) })
    }

    fn file_tree(&self, _workspace_id: WorkspaceId, path: String) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(serde_json::json!([{ "path": path, "name": path, "isDirectory": false }]))
        })
    }

    fn file_delete(
        &self,
        _workspace_id: WorkspaceId,
        path: String,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(
            async move { Ok(serde_json::json!({ "ok": true, "path": path, "deleted": true })) },
        )
    }

    fn file_mkdir(&self, _workspace_id: WorkspaceId, path: String) -> BoxFuture<'_, Result<Value>> {
        Box::pin(
            async move { Ok(serde_json::json!({ "ok": true, "path": path, "created": true })) },
        )
    }

    fn file_rename(
        &self,
        _workspace_id: WorkspaceId,
        old_path: String,
        new_path: String,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(serde_json::json!({
                "ok": true, "oldPath": old_path, "newPath": new_path,
                "renamed": true, "isDirectory": false
            }))
        })
    }

    fn primitive_add_reference(
        &self,
        _workspace_id: WorkspaceId,
        note_id: NoteId,
        semantic_id: String,
        description: String,
        snapshot: Option<String>,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(serde_json::json!({
                "ok": true, "primitiveId": "p-ref", "noteId": note_id.as_str(),
                "content": format!("{semantic_id}|{description}|{snapshot:?}"),
            }))
        })
    }

    fn primitive_add_cli(
        &self,
        _workspace_id: WorkspaceId,
        note_id: NoteId,
        command: String,
        description: String,
        working_directory: Option<String>,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(serde_json::json!({
                "ok": true, "primitiveId": "p-cli", "noteId": note_id.as_str(),
                "content": format!("{command}|{description}|{working_directory:?}"),
            }))
        })
    }

    fn primitive_add_patch(
        &self,
        _workspace_id: WorkspaceId,
        note_id: NoteId,
        file_path: String,
        diff: String,
        description: String,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(serde_json::json!({
                "ok": true, "primitiveId": "p-patch", "noteId": note_id.as_str(),
                "content": format!("{file_path}|{diff}|{description}"),
            }))
        })
    }

    fn primitive_add_agent_action(
        &self,
        _workspace_id: WorkspaceId,
        note_id: NoteId,
        agent_id: String,
        goal: String,
        description: String,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(serde_json::json!({
                "ok": true, "primitiveId": "p-action", "noteId": note_id.as_str(),
                "content": format!("{agent_id}|{goal}|{description}"),
            }))
        })
    }

    fn cross_workspace_list_siblings(
        &self,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(serde_json::json!([{
                "id": "sib-1", "title": "Untitled", "branch": "b", "status": "Active",
                "createdAt": "t0", "updatedAt": "t1", "caller": workspace_id.as_str(),
            }]))
        })
    }

    fn cross_workspace_list_notes(
        &self,
        _workspace_id: WorkspaceId,
        target_workspace_id: WorkspaceId,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(serde_json::json!([{
                "id": "n1", "title": "t", "createdAt": "t0", "updatedAt": "t1",
                "target": target_workspace_id.as_str(),
            }]))
        })
    }

    fn cross_workspace_read_note(
        &self,
        _workspace_id: WorkspaceId,
        target_workspace_id: WorkspaceId,
        note_id: NoteId,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(serde_json::json!({
                "id": note_id.as_str(), "title": "t", "content": "c",
                "numberedContent": "   1 | c", "sourceWorkspaceId": target_workspace_id.as_str(),
                "sourceWorkspaceTitle": "T", "branch": "b", "lineCount": 1,
            }))
        })
    }

    fn script_list(&self, workspace_id: WorkspaceId) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(serde_json::json!({ "scripts": [], "workspaceId": workspace_id.as_str() }))
        })
    }

    fn script_create(
        &self,
        workspace_id: WorkspaceId,
        params: ScriptCreateParams,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            let mode = match params.mode {
                ScriptMode::Service => "service",
                ScriptMode::Command => "command",
            };
            Ok(serde_json::json!({
                "workspaceId": workspace_id.as_str(),
                "name": params.name,
                "command": params.command,
                "mode": mode,
                "cwd": params.cwd,
                "env": params.env,
                "category": params.category,
                "autoStart": params.auto_start,
                "scriptId": params.script_id,
            }))
        })
    }

    fn script_remove(&self, script_id: String) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move { Ok(serde_json::json!({ "ok": true, "scriptId": script_id })) })
    }

    fn script_start(&self, script_id: String) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move { Ok(serde_json::json!({ "ok": true, "scriptId": script_id })) })
    }

    fn script_stop(&self, script_id: String) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move { Ok(serde_json::json!({ "ok": true, "scriptId": script_id })) })
    }

    fn script_restart(&self, script_id: String) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move { Ok(serde_json::json!({ "ok": true, "scriptId": script_id })) })
    }

    fn script_output(
        &self,
        script_id: String,
        max_lines: Option<i64>,
        _paginate: Option<bool>,
        _page_token: Option<String>,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            // `script.output` returns plaintext buffer text (a bare string), not
            // an object (§5.8). Echo `scriptId`/`maxLines` into the string so the
            // dispatch test can still assert they were threaded through.
            let _ = script_id;
            Ok(Value::String(format!(
                "[1 lines]\nmaxLines={}",
                max_lines.unwrap_or(-1)
            )))
        })
    }

    fn script_status(&self, script_id: String) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move { Ok(serde_json::json!({ "scriptId": script_id, "status": "idle" })) })
    }

    fn script_run(
        &self,
        script_id: String,
        max_lines: Option<i64>,
        timeout_seconds: Option<i64>,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(serde_json::json!({
                "scriptId": script_id,
                "maxLines": max_lines,
                "timeoutSeconds": timeout_seconds,
            }))
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
async fn expected_version_conflict_maps_to_minus_32005_with_data_current() {
    // A stale `expectedVersion` on `note.update` surfaces -32005 carrying the
    // current entity under `error.data.current` (PROTOCOL §4, §5.6).
    let v = call(
        r#"{"jsonrpc":"2.0","id":9,"method":"note.update","params":{"workspaceId":"ws-1","noteId":"conflict","content":"x","expectedVersion":1}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32005);
    assert_eq!(v["error"]["message"], serde_json::json!("Conflict"));
    assert_eq!(v["error"]["data"]["code"], serde_json::json!("conflict"));
    let current = &v["error"]["data"]["current"];
    assert!(current.is_object(), "data.current must be the entity");
    assert_eq!(current["id"], serde_json::json!("conflict"));
    assert_eq!(current["rev"], serde_json::json!(7));
}

#[tokio::test]
async fn update_metadata_expected_version_conflict_maps_to_minus_32005() {
    // A stale `expectedVersion` on `note.updateMetadata` surfaces -32005 carrying
    // the current entity under `error.data.current` (PROTOCOL §4, §5.6).
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"note.updateMetadata","params":{"workspaceId":"ws-1","noteId":"conflict","title":"x","expectedVersion":1}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32005);
    assert_eq!(v["error"]["message"], serde_json::json!("Conflict"));
    assert_eq!(v["error"]["data"]["code"], serde_json::json!("conflict"));
    assert_eq!(
        v["error"]["data"]["current"]["id"],
        serde_json::json!("conflict")
    );
    assert_eq!(v["error"]["data"]["current"]["rev"], serde_json::json!(7));
}

#[tokio::test]
async fn delete_expected_version_conflict_maps_to_minus_32005() {
    // A stale `expectedVersion` on `note.delete` surfaces -32005 carrying the
    // current entity snapshot under `error.data.current` (PROTOCOL §4, §5.6).
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"note.delete","params":{"workspaceId":"ws-1","noteId":"conflict","expectedVersion":1}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32005);
    assert_eq!(v["error"]["message"], serde_json::json!("Conflict"));
    assert_eq!(v["error"]["data"]["code"], serde_json::json!("conflict"));
    assert_eq!(
        v["error"]["data"]["current"]["id"],
        serde_json::json!("conflict")
    );
    assert_eq!(v["error"]["data"]["current"]["rev"], serde_json::json!(7));
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
async fn comment_resolve_thread_defaults_resolved_and_passes_params() {
    // Default resolved=true when the param is omitted.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"comment.resolveThread","params":{"workspaceId":"ws-1","noteId":"n1","threadId":"c1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["success"], serde_json::json!(true));
    assert_eq!(v["result"]["threadId"], serde_json::json!("c1"));
    assert_eq!(v["result"]["resolved"], serde_json::json!(true));
    assert_eq!(v["result"]["status"], serde_json::json!("resolved"));

    // resolved=false routes through for unresolve symmetry.
    let v = call(
        r#"{"jsonrpc":"2.0","id":2,"method":"comment.resolveThread","params":{"workspaceId":"ws-1","noteId":"n1","commentId":"x9","resolved":false}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["threadId"], serde_json::json!("x9"));
    assert_eq!(v["result"]["resolved"], serde_json::json!(false));
    assert_eq!(v["result"]["status"], serde_json::json!("open"));
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
        "comment.resolveThread",
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
async fn git_unstage_returns_ok_and_paths() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"git.unstage","params":{"workspaceId":"ws-1","paths":["src/a.ts","src/b.ts"]}}"#,
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
async fn git_unstage_missing_paths_is_minus_32602() {
    let v =
        call(r#"{"jsonrpc":"2.0","id":1,"method":"git.unstage","params":{"workspaceId":"ws-1"}}"#)
            .await
            .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Missing required parameter: paths")
    );
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
async fn repo_list_returns_repos_with_camelcase_keys() {
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"repo.list","params":{}}"#)
        .await
        .unwrap();
    let repo = &v["result"]["repos"][0];
    assert_eq!(repo["path"], serde_json::json!("/src/intent"));
    assert_eq!(repo["name"], serde_json::json!("intent"));
    assert_eq!(repo["owner"], serde_json::json!("cloudlands-ai"));
    assert_eq!(repo["addedAt"], serde_json::json!("t0"));
    assert_eq!(repo["lastUsedAt"], serde_json::json!("t1"));
}

// ---- github.* browse / auth / identity routing (PROTOCOL §5.27) --------

#[tokio::test]
async fn github_repos_list_routes_with_optional_pagination() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"github.repos.list","params":{"limit":25,"nextToken":"c"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["echoLimit"], serde_json::json!(25));
    assert_eq!(v["result"]["echoToken"], serde_json::json!("c"));
    assert_eq!(v["result"]["nextToken"], Value::Null);
}

#[tokio::test]
async fn github_repos_search_requires_query() {
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"github.repos.search","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
}

#[tokio::test]
async fn github_repos_search_routes_query() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"github.repos.search","params":{"query":"react"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["echoQuery"], serde_json::json!("react"));
}

#[tokio::test]
async fn github_repos_get_requires_owner_and_repo() {
    let missing_repo =
        call(r#"{"jsonrpc":"2.0","id":1,"method":"github.repos.get","params":{"owner":"o"}}"#)
            .await
            .unwrap();
    assert_eq!(err_code(&missing_repo), -32602);

    let ok = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"github.repos.get","params":{"owner":"o","repo":"r"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(ok["result"]["repo"]["owner"], serde_json::json!("o"));
    assert_eq!(ok["result"]["repo"]["name"], serde_json::json!("r"));
}

#[tokio::test]
async fn github_branches_list_requires_owner_and_repo() {
    let v =
        call(r#"{"jsonrpc":"2.0","id":1,"method":"github.branches.list","params":{"owner":"o"}}"#)
            .await
            .unwrap();
    assert_eq!(err_code(&v), -32602);

    let ok = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"github.branches.list","params":{"owner":"o","repo":"r"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(ok["result"]["branches"], serde_json::json!(["o", "r"]));
    assert_eq!(ok["result"]["nextToken"], Value::Null);
}

#[tokio::test]
async fn github_auth_status_connect_revoke_get_user_route_without_params() {
    let auth = call(r#"{"jsonrpc":"2.0","id":1,"method":"github.authStatus","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(auth["result"]["isConfigured"], serde_json::json!(true));

    let connect = call(r#"{"jsonrpc":"2.0","id":1,"method":"github.connect","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(connect["result"]["ok"], serde_json::json!(false));

    let revoke = call(r#"{"jsonrpc":"2.0","id":1,"method":"github.revoke","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(revoke["result"]["ok"], serde_json::json!(false));

    let user = call(r#"{"jsonrpc":"2.0","id":1,"method":"github.getUser","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(
        user["result"]["user"]["login"],
        serde_json::json!("octocat")
    );
    assert!(user["result"]["user"].get("id").is_none());
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

#[tokio::test]
async fn terminal_create_parses_dims_and_defaults() {
    // Explicit cols/rows/cwd/command flow through to the service call.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"terminal.create","params":{"workspaceId":"ws-1","cols":120,"rows":40,"cwd":"/tmp","command":"bash"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["terminalId"], serde_json::json!("pty-1"));
    assert_eq!(v["result"]["cols"], serde_json::json!(120));
    assert_eq!(v["result"]["rows"], serde_json::json!(40));
    assert_eq!(v["result"]["cwd"], serde_json::json!("/tmp"));
    assert_eq!(v["result"]["command"], serde_json::json!("bash"));

    // Defaults applied (80x24) when dims are absent.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"terminal.create","params":{"workspaceId":"ws-1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["cols"], serde_json::json!(80));
    assert_eq!(v["result"]["rows"], serde_json::json!(24));
    assert_eq!(v["result"]["command"], serde_json::json!(null));

    // workspaceId is required.
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"terminal.create","params":{"cols":80}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("workspaceId is required")
    );
}

#[tokio::test]
async fn terminal_write_resize_getbuffer_pass_params() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"terminal.write","params":{"terminalId":"pty-1","data":"aGk="}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["ok"], serde_json::json!(true));
    assert_eq!(v["result"]["data"], serde_json::json!("aGk="));

    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"terminal.resize","params":{"terminalId":"pty-1","cols":100,"rows":30}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["cols"], serde_json::json!(100));
    assert_eq!(v["result"]["rows"], serde_json::json!(30));

    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"terminal.getBuffer","params":{"terminalId":"pty-1","maxBytes":4096}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["terminalId"], serde_json::json!("pty-1"));
    assert_eq!(v["result"]["data"], serde_json::json!("aGk="));
    assert_eq!(v["result"]["maxBytes"], serde_json::json!(4096));

    // Missing terminalId → -32602.
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"terminal.write","params":{"data":"aGk="}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
}

#[tokio::test]
async fn terminal_kill_and_list_dispatch() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"terminal.kill","params":{"terminalId":"pty-1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["ok"], serde_json::json!(true));

    // Unknown terminal → NotFound maps to -32602.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"terminal.kill","params":{"terminalId":"pty-missing"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);

    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"terminal.list","params":{"workspaceId":"ws-1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(
        v["result"]["terminals"][0]["id"],
        serde_json::json!("pty-1")
    );
    assert_eq!(
        v["result"]["terminals"][0]["alive"],
        serde_json::json!(true)
    );
}

#[tokio::test]
async fn file_methods_dispatch_with_exact_wire_shapes() {
    // file.read returns a BARE string, not an object (the key parity gotcha).
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"file.read","params":{"workspaceId":"ws-1","path":"a.txt"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"], serde_json::json!("ws-1:a.txt"));
    assert!(v["result"].is_string());

    // file.write → { ok, path, size }.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"file.write","params":{"workspaceId":"ws-1","path":"a.txt","content":"hello"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(
        v["result"],
        serde_json::json!({ "ok": true, "path": "a.txt", "size": 5 })
    );

    // file.list → bare array; `path` defaults to "." when omitted.
    let v =
        call(r#"{"jsonrpc":"2.0","id":1,"method":"file.list","params":{"workspaceId":"ws-1"}}"#)
            .await
            .unwrap();
    assert!(v["result"].is_array());
    assert_eq!(v["result"][0]["name"], serde_json::json!("."));
    assert_eq!(v["result"][0]["type"], serde_json::json!("file"));

    // file.tree → bare array of { path, name, isDirectory }; `path` defaults to ".".
    let v =
        call(r#"{"jsonrpc":"2.0","id":1,"method":"file.tree","params":{"workspaceId":"ws-1"}}"#)
            .await
            .unwrap();
    assert!(v["result"].is_array());
    assert_eq!(v["result"][0]["path"], serde_json::json!("."));
    assert_eq!(v["result"][0]["name"], serde_json::json!("."));
    assert_eq!(v["result"][0]["isDirectory"], serde_json::json!(false));

    // file.delete → { ok, path, deleted: true }.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"file.delete","params":{"workspaceId":"ws-1","path":"a.txt"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(
        v["result"],
        serde_json::json!({ "ok": true, "path": "a.txt", "deleted": true })
    );

    // file.mkdir → { ok, path, created: true }.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"file.mkdir","params":{"workspaceId":"ws-1","path":"d"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(
        v["result"],
        serde_json::json!({ "ok": true, "path": "d", "created": true })
    );

    // file.rename → { ok, oldPath, newPath, renamed: true, isDirectory }.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"file.rename","params":{"workspaceId":"ws-1","oldPath":"a.txt","newPath":"b.txt"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(
        v["result"],
        serde_json::json!({
            "ok": true, "oldPath": "a.txt", "newPath": "b.txt",
            "renamed": true, "isDirectory": false
        })
    );
}

#[tokio::test]
async fn file_methods_require_params() {
    // Missing workspaceId → -32602 "workspaceId is required".
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"file.read","params":{"path":"a"}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);

    // Missing path → -32602 (router-level requireParam, outside the try block).
    let v =
        call(r#"{"jsonrpc":"2.0","id":1,"method":"file.read","params":{"workspaceId":"ws-1"}}"#)
            .await
            .unwrap();
    assert_eq!(err_code(&v), -32602);

    // file.write missing content → -32602.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"file.write","params":{"workspaceId":"ws-1","path":"a"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);

    // file.rename missing newPath → -32602.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"file.rename","params":{"workspaceId":"ws-1","oldPath":"a"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
}

#[tokio::test]
async fn primitive_methods_dispatch_and_pass_params() {
    // addReference: required semanticId/description flow through; snapshot optional.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"primitive.addReference","params":{"workspaceId":"ws-1","noteId":"n1","semanticId":"src/a.ts#L1","description":"d"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["ok"], serde_json::json!(true));
    assert_eq!(v["result"]["noteId"], serde_json::json!("n1"));
    assert_eq!(v["result"]["primitiveId"], serde_json::json!("p-ref"));
    assert_eq!(
        v["result"]["content"],
        serde_json::json!("src/a.ts#L1|d|None")
    );

    // addCli: workingDirectory optional, passed through as Some when present.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"primitive.addCli","params":{"workspaceId":"ws-1","noteId":"n1","command":"ls","description":"d","workingDirectory":"sub"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["primitiveId"], serde_json::json!("p-cli"));
    assert_eq!(
        v["result"]["content"],
        serde_json::json!("ls|d|Some(\"sub\")")
    );

    // addPatch and addAgentAction dispatch to their arms.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"primitive.addPatch","params":{"workspaceId":"ws-1","noteId":"n1","filePath":"a.ts","diff":"@@","description":"d"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["primitiveId"], serde_json::json!("p-patch"));
    assert_eq!(v["result"]["content"], serde_json::json!("a.ts|@@|d"));

    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"primitive.addAgentAction","params":{"workspaceId":"ws-1","noteId":"n1","agentId":"agent-1","goal":"g","description":"d"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["primitiveId"], serde_json::json!("p-action"));
    assert_eq!(v["result"]["content"], serde_json::json!("agent-1|g|d"));
}

#[tokio::test]
async fn primitive_methods_require_params() {
    // Missing workspaceId → -32602.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"primitive.addCli","params":{"noteId":"n1","command":"ls","description":"d"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);

    // Missing noteId → -32602.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"primitive.addReference","params":{"workspaceId":"ws-1","semanticId":"x","description":"d"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);

    // addReference missing semanticId → -32602.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"primitive.addReference","params":{"workspaceId":"ws-1","noteId":"n1","description":"d"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);

    // addPatch missing diff → -32602.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"primitive.addPatch","params":{"workspaceId":"ws-1","noteId":"n1","filePath":"a","description":"d"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);

    // addAgentAction missing goal → -32602.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"primitive.addAgentAction","params":{"workspaceId":"ws-1","noteId":"n1","agentId":"a","description":"d"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
}

#[tokio::test]
async fn cross_workspace_methods_dispatch_and_pass_params() {
    // listSiblings → bare array; the caller workspaceId flows to the service.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"crossWorkspace.listSiblings","params":{"workspaceId":"ws-1"}}"#,
    )
    .await
    .unwrap();
    assert!(v["result"].is_array());
    assert_eq!(v["result"][0]["caller"], serde_json::json!("ws-1"));
    // status is the PascalCase WorkspaceStatus from the workspace model.
    assert_eq!(v["result"][0]["status"], serde_json::json!("Active"));

    // listNotes → bare array; targetWorkspaceId flows through.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"crossWorkspace.listNotes","params":{"workspaceId":"ws-1","targetWorkspaceId":"ws-2"}}"#,
    )
    .await
    .unwrap();
    assert!(v["result"].is_array());
    assert_eq!(v["result"][0]["target"], serde_json::json!("ws-2"));

    // readNote → object; target + noteId flow through.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"crossWorkspace.readNote","params":{"workspaceId":"ws-1","targetWorkspaceId":"ws-2","noteId":"n9"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["id"], serde_json::json!("n9"));
    assert_eq!(v["result"]["sourceWorkspaceId"], serde_json::json!("ws-2"));
    assert_eq!(
        v["result"]["numberedContent"],
        serde_json::json!("   1 | c")
    );
}

#[tokio::test]
async fn cross_workspace_methods_require_params() {
    // Missing workspaceId → -32602.
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"crossWorkspace.listSiblings","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);

    // listNotes missing targetWorkspaceId → -32602.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"crossWorkspace.listNotes","params":{"workspaceId":"ws-1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);

    // readNote missing noteId → -32602.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"crossWorkspace.readNote","params":{"workspaceId":"ws-1","targetWorkspaceId":"ws-2"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
}

#[tokio::test]
async fn script_create_parses_fields_and_mode() {
    // All optional fields flow through; mode parses to the enum.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"script.create","params":{"workspaceId":"ws-1","name":"dev","command":"pnpm dev","mode":"service","cwd":"app","env":{"PORT":"3000"},"category":"dev","autoStart":true,"scriptId":"s-1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["name"], serde_json::json!("dev"));
    assert_eq!(v["result"]["command"], serde_json::json!("pnpm dev"));
    assert_eq!(v["result"]["mode"], serde_json::json!("service"));
    assert_eq!(v["result"]["cwd"], serde_json::json!("app"));
    assert_eq!(v["result"]["env"]["PORT"], serde_json::json!("3000"));
    assert_eq!(v["result"]["category"], serde_json::json!("dev"));
    assert_eq!(v["result"]["autoStart"], serde_json::json!(true));
    assert_eq!(v["result"]["scriptId"], serde_json::json!("s-1"));

    // Invalid mode → -32602.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"script.create","params":{"workspaceId":"ws-1","name":"x","command":"y","mode":"daemon"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);

    // Missing command → -32602.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"script.create","params":{"workspaceId":"ws-1","name":"x","mode":"command"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
}

#[tokio::test]
async fn script_lifecycle_and_run_dispatch() {
    for method in [
        "script.start",
        "script.stop",
        "script.restart",
        "script.remove",
    ] {
        let msg = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"{method}","params":{{"scriptId":"s-1"}}}}"#
        );
        let v = call(&msg).await.unwrap();
        assert_eq!(v["result"]["ok"], serde_json::json!(true), "{method}");
        assert_eq!(
            v["result"]["scriptId"],
            serde_json::json!("s-1"),
            "{method}"
        );
    }

    // script.run accepts the `timeout` alias for `timeoutSeconds`.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"script.run","params":{"scriptId":"s-1","maxLines":50,"timeout":30}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["maxLines"], serde_json::json!(50));
    assert_eq!(v["result"]["timeoutSeconds"], serde_json::json!(30));

    // script.output passes maxLines and its result is a bare plaintext string
    // (a header line + text), not an object (§5.8).
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"script.output","params":{"scriptId":"s-1","maxLines":10}}"#,
    )
    .await
    .unwrap();
    let out = v["result"]
        .as_str()
        .expect("script.output result is a string");
    assert!(out.starts_with("["), "header line present: {out:?}");
    assert!(out.contains("maxLines=10"), "maxLines threaded: {out:?}");

    // Missing scriptId → -32602.
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"script.start","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
}

#[tokio::test]
async fn rules_methods_route_and_validate_params() {
    // rules.list takes an optional workspaceId and must route to the (default)
    // impl → -32603, proving it is registered (never -32601).
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"rules.list","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32603);

    // rules.get requires workspaceId then ruleType.
    let v = call(r#"{"jsonrpc":"2.0","id":2,"method":"rules.get","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
    let v =
        call(r#"{"jsonrpc":"2.0","id":3,"method":"rules.get","params":{"workspaceId":"ws-1"}}"#)
            .await
            .unwrap();
    assert_eq!(err_code(&v), -32602);
    let v = call(
        r#"{"jsonrpc":"2.0","id":4,"method":"rules.get","params":{"workspaceId":"ws-1","ruleType":"workspace"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32603);

    // rules.update requires ruleType + content.
    let v = call(
        r#"{"jsonrpc":"2.0","id":5,"method":"rules.update","params":{"workspaceId":"ws-1","ruleType":"workspace"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    let v = call(
        r#"{"jsonrpc":"2.0","id":6,"method":"rules.update","params":{"workspaceId":"ws-1","ruleType":"workspace","content":"x"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32603);
}

// `github.*` explicit-addressing arms (§5.27). `FakeApi` uses the default
// trait impls (→ -32603 "not implemented"), so a fully-parametrized call
// proving the arm routes (not -32601) lands on -32603, while missing required
// params short-circuit to -32602 in the router before the trait is reached.

#[tokio::test]
async fn github_methods_are_routed_not_unknown() {
    for msg in [
        r#"{"jsonrpc":"2.0","id":1,"method":"github.pulls.create","params":{"owner":"o","repo":"r","title":"t","body":"b","head":"h","base":"main"}}"#,
        r#"{"jsonrpc":"2.0","id":2,"method":"github.pulls.get","params":{"owner":"o","repo":"r","number":1}}"#,
        r#"{"jsonrpc":"2.0","id":3,"method":"github.pulls.list","params":{"owner":"o","repo":"r"}}"#,
        r#"{"jsonrpc":"2.0","id":4,"method":"github.pulls.search","params":{"owner":"o","repo":"r"}}"#,
        r#"{"jsonrpc":"2.0","id":5,"method":"github.pulls.merge","params":{"owner":"o","repo":"r","number":1}}"#,
        r#"{"jsonrpc":"2.0","id":6,"method":"github.pulls.updateBranch","params":{"owner":"o","repo":"r","number":1}}"#,
        r#"{"jsonrpc":"2.0","id":7,"method":"github.issues.list","params":{"owner":"o","repo":"r"}}"#,
        r#"{"jsonrpc":"2.0","id":8,"method":"github.issues.search","params":{"owner":"o","repo":"r"}}"#,
        r#"{"jsonrpc":"2.0","id":9,"method":"github.listReviewComments","params":{"owner":"o","repo":"r","number":1}}"#,
        r#"{"jsonrpc":"2.0","id":10,"method":"github.replyReviewComment","params":{"owner":"o","repo":"r","number":1,"commentId":2,"body":"b"}}"#,
        r#"{"jsonrpc":"2.0","id":11,"method":"github.getReviewThreads","params":{"owner":"o","repo":"r","number":1}}"#,
        r#"{"jsonrpc":"2.0","id":12,"method":"github.resolveThread","params":{"threadId":"RT1"}}"#,
        r#"{"jsonrpc":"2.0","id":13,"method":"github.unresolveThread","params":{"threadId":"RT1"}}"#,
    ] {
        let v = call(msg).await.unwrap();
        assert_eq!(err_code(&v), -32603, "msg={msg}");
    }
}

#[tokio::test]
async fn github_missing_required_params_are_minus_32602() {
    for msg in [
        // missing owner/repo
        r#"{"jsonrpc":"2.0","id":1,"method":"github.pulls.list","params":{"repo":"r"}}"#,
        r#"{"jsonrpc":"2.0","id":2,"method":"github.pulls.get","params":{"owner":"o","repo":"r"}}"#,
        // missing number
        r#"{"jsonrpc":"2.0","id":3,"method":"github.pulls.merge","params":{"owner":"o","repo":"r"}}"#,
        // missing required create fields
        r#"{"jsonrpc":"2.0","id":4,"method":"github.pulls.create","params":{"owner":"o","repo":"r","title":"t"}}"#,
        // missing commentId / body
        r#"{"jsonrpc":"2.0","id":5,"method":"github.replyReviewComment","params":{"owner":"o","repo":"r","number":1}}"#,
        // missing threadId
        r#"{"jsonrpc":"2.0","id":6,"method":"github.resolveThread","params":{}}"#,
    ] {
        let v = call(msg).await.unwrap();
        assert_eq!(err_code(&v), -32602, "msg={msg}");
    }
}
