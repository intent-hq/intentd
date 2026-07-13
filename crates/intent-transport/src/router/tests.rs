//! Router error-matrix + dispatch unit tests using a fake `WorkspaceApi`.

use intent_core::{
    AgentId, AuthorType, BoxFuture, Comment, CommentAddResult, CommentAnchor, CommentAnchorType,
    CommentLocation, CommentResolveThreadResult, CommentRespondResult, CommentRespondThread,
    CommentStatus, CommentType, CommentWire, ContentType, Error, Event, EventQueryParams,
    FileActivity, FileStatus, GitAgentCommitResult, GitBranchStatus, GitBranches, GitCommitResult,
    GitFileStatus, GitMergeConflicts, GitStatus, Note, NoteAddInput, NoteAddResult, NoteCreate,
    NoteDeleteResult, NoteEditInput, NoteEditLinesInput, NoteEditLinesResult, NoteEditResult,
    NoteId, NoteMetadata, NoteSetContentResult, NoteTaskRow, NoteUpdateInput,
    NoteUpdateMetadataResult, NoteVisibility, ReadAssetResult, Result, ScriptCreateParams,
    ScriptMode, TaskUpdateResult, Workspace, WorkspaceActivity, WorkspaceApi, WorkspaceAttention,
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
        token_usage: None,
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
        metadata: NoteMetadata::default(),
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
    ) -> BoxFuture<'_, Result<intent_core::WorkspaceCreateResult>> {
        Box::pin(async move {
            let mut ws = sample_ws();
            if let Some(t) = input.title {
                ws.title = t;
            }
            Ok(intent_core::WorkspaceCreateResult {
                workspace: ws,
                initial_agent: None,
            })
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
        idempotency_key: Option<String>,
    ) -> BoxFuture<'_, Result<CommentAddResult>> {
        Box::pin(async move {
            Ok(CommentAddResult {
                success: true,
                message: format!("Comment successfully anchored to \"{comment_target}\""),
                // Echo the key so router tests can pin that the arm forwards
                // `params.idempotencyKey` instead of silently dropping it.
                comment_id: idempotency_key.unwrap_or_else(|| "c1".to_string()),
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

    fn git_discard(
        &self,
        _workspace_id: WorkspaceId,
        paths: Value,
    ) -> BoxFuture<'_, Result<Vec<String>>> {
        Box::pin(async move {
            // Mirrors `parse_discard_paths` in production: reject `.`/`*`/
            // `--all` in top-level string AND every parsed element (array
            // items + CSV entries), with a discard-oriented message.
            if let Value::String(s) = &paths {
                if s == "." || s == "*" || s.contains("--all") {
                    return Err(Error::Internal(
                        "Discarding all files is not allowed.".to_string(),
                    ));
                }
            }
            let list: Vec<String> = match paths {
                Value::Array(items) => items
                    .iter()
                    .filter_map(Value::as_str)
                    .map(|p| p.trim().to_string())
                    .filter(|p| !p.is_empty())
                    .collect(),
                Value::String(s) => s
                    .split(',')
                    .map(|p| p.trim().to_string())
                    .filter(|p| !p.is_empty())
                    .collect(),
                _ => vec![],
            };
            for p in &list {
                if p == "." || p == "*" || p.contains("--all") {
                    return Err(Error::Internal(
                        "Discarding all files is not allowed.".to_string(),
                    ));
                }
            }
            // Parity with production: an empty parsed list (e.g. `[]`,
            // `[null]`, `" , "`) is `-32603`, not a silent `ok: true`.
            if list.is_empty() {
                return Err(Error::Internal(
                    "No file paths provided. Please specify at least one file path to discard."
                        .to_string(),
                ));
            }
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

    fn git_branch_status(
        &self,
        repo_path: String,
        branch_name: String,
    ) -> BoxFuture<'_, Result<GitBranchStatus>> {
        Box::pin(async move {
            if repo_path == "/unknown" {
                return Err(Error::InvalidParams(
                    "Unknown or unauthorized repository path".to_string(),
                ));
            }
            Ok(GitBranchStatus {
                branch: branch_name.clone(),
                current_branch: "feature".to_string(),
                is_current_branch: branch_name == "feature",
                ahead: 1,
                behind: 2,
                has_uncommitted_changes: true,
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

    fn repo_remove(&self, path: String) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move { Ok(serde_json::json!({ "removed": path == "/src/intent" })) })
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
        _agent_id: Option<AgentId>,
        _linked_note_id: Option<NoteId>,
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
        env: Option<std::collections::BTreeMap<String, String>>,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(serde_json::json!({
                "terminalId": "pty-1",
                "workspaceId": workspace_id.as_str(),
                "cols": cols,
                "rows": rows,
                "cwd": cwd,
                "command": command,
                "env": env,
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

    fn file_exists(
        &self,
        _workspace_id: WorkspaceId,
        path: String,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(serde_json::json!({
                "exists": !path.is_empty(),
                "isFile": true,
                "isDirectory": false,
            }))
        })
    }

    fn file_stat(&self, _workspace_id: WorkspaceId, path: String) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(serde_json::json!({
                "size": path.len() as u64,
                "mtime": "1970-01-01T00:00:00.000Z",
                "isFile": true,
                "isDirectory": false,
                "isSymlink": false,
                "permissions": "0644",
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

    fn script_remove(
        &self,
        workspace_id: WorkspaceId,
        script_id: String,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(serde_json::json!({
                "ok": true,
                "scriptId": script_id,
                "workspaceId": workspace_id.as_str(),
            }))
        })
    }

    fn script_start(
        &self,
        workspace_id: WorkspaceId,
        script_id: String,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(serde_json::json!({
                "ok": true,
                "scriptId": script_id,
                "workspaceId": workspace_id.as_str(),
            }))
        })
    }

    fn script_stop(
        &self,
        workspace_id: WorkspaceId,
        script_id: String,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(serde_json::json!({
                "ok": true,
                "scriptId": script_id,
                "workspaceId": workspace_id.as_str(),
            }))
        })
    }

    fn script_restart(
        &self,
        workspace_id: WorkspaceId,
        script_id: String,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(serde_json::json!({
                "ok": true,
                "scriptId": script_id,
                "workspaceId": workspace_id.as_str(),
            }))
        })
    }

    fn script_output(
        &self,
        workspace_id: WorkspaceId,
        script_id: String,
        max_lines: Option<i64>,
        _paginate: Option<bool>,
        _page_token: Option<String>,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            // `script.output` returns plaintext buffer text (a bare string), not
            // an object (§5.8). Echo `scriptId`/`maxLines` into the string so the
            // dispatch test can still assert they were threaded through.
            let _ = (script_id, workspace_id);
            Ok(Value::String(format!(
                "[1 lines]\nmaxLines={}",
                max_lines.unwrap_or(-1)
            )))
        })
    }

    fn script_status(
        &self,
        workspace_id: WorkspaceId,
        script_id: String,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(serde_json::json!({
                "scriptId": script_id,
                "workspaceId": workspace_id.as_str(),
                "status": "idle",
            }))
        })
    }

    fn script_run(
        &self,
        workspace_id: WorkspaceId,
        script_id: String,
        max_lines: Option<i64>,
        timeout_seconds: Option<i64>,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(serde_json::json!({
                "scriptId": script_id,
                "workspaceId": workspace_id.as_str(),
                "maxLines": max_lines,
                "timeoutSeconds": timeout_seconds,
            }))
        })
    }

    // Echo the parsed rename params so the router tests can assert the
    // optional `skipIfExplicitlySet` flag is forwarded (P3-1.2b).
    fn agent_rename(
        &self,
        agent_id: AgentId,
        name: String,
        skip_if_explicitly_set: bool,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(serde_json::json!({
                "success": true,
                "name": name,
                "agentId": agent_id,
                "skipIfExplicitlySet": skip_if_explicitly_set,
            }))
        })
    }
}

async fn call(msg: &str) -> Option<Value> {
    handle_message(&FakeApi, msg)
        .await
        .map(|s| serde_json::from_str(&s).expect("valid json response"))
}

#[tokio::test]
async fn linear_get_issue_missing_id_and_identifier_is_minus_32602() {
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"linear.getIssue","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Missing required parameter: id")
    );
}

#[tokio::test]
async fn linear_get_issue_routes_with_id_or_identifier() {
    // The `FakeApi` uses the trait default (`Internal` → `-32603`), so a present
    // `id`/`identifier` means the arm routed past param validation.
    for params in [r#"{"id":"uuid-1"}"#, r#"{"identifier":"ENG-1"}"#] {
        let msg =
            format!(r#"{{"jsonrpc":"2.0","id":1,"method":"linear.getIssue","params":{params}}}"#);
        let v = call(&msg).await.unwrap();
        assert_eq!(err_code(&v), -32603, "params={params}");
    }
}

#[tokio::test]
async fn linear_p1_read_arms_route() {
    // Each arm reaches the trait default (`-32603`), i.e. it is dispatched
    // rather than reported as unknown method (`-32601`).
    for method in [
        "linear.viewer",
        "linear.listTeams",
        "linear.listWorkflowStates",
        "linear.listProjects",
        "linear.listLabels",
    ] {
        let msg = format!(r#"{{"jsonrpc":"2.0","id":1,"method":"{method}","params":{{}}}}"#);
        let v = call(&msg).await.unwrap();
        assert_eq!(err_code(&v), -32603, "{method}");
    }
}

#[tokio::test]
async fn linear_create_issue_missing_title_or_team_is_minus_32602() {
    // Empty params → `-32602` (title missing).
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"linear.createIssue","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Missing required parameter: title")
    );

    // Title present but `teamId` missing → `-32602`.
    let v =
        call(r#"{"jsonrpc":"2.0","id":1,"method":"linear.createIssue","params":{"title":"X"}}"#)
            .await
            .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Missing required parameter: teamId")
    );

    // Empty string title is also rejected.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"linear.createIssue","params":{"title":"","teamId":"t1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
}

#[tokio::test]
async fn linear_create_issue_with_required_routes_past_param_validation() {
    // Both required fields present → reaches the trait default (`-32603`).
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"linear.createIssue","params":{"title":"X","teamId":"t1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32603);
}

#[tokio::test]
async fn linear_update_issue_missing_issue_id_is_minus_32602() {
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"linear.updateIssue","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Missing required parameter: issueId")
    );

    // Empty string issueId is also rejected.
    let v =
        call(r#"{"jsonrpc":"2.0","id":1,"method":"linear.updateIssue","params":{"issueId":""}}"#)
            .await
            .unwrap();
    assert_eq!(err_code(&v), -32602);
}

#[tokio::test]
async fn linear_update_issue_with_issue_id_routes_past_param_validation() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"linear.updateIssue","params":{"issueId":"uuid-1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32603);
}

#[tokio::test]
async fn sentry_p1_read_arms_route() {
    // `FakeApi` uses the trait default (`-32603`), so a routed arm means the
    // method dispatched rather than being reported as unknown (`-32601`).
    for msg in [
        r#"{"jsonrpc":"2.0","id":1,"method":"sentry.listProjects","params":{}}"#,
        r#"{"jsonrpc":"2.0","id":1,"method":"sentry.listProjects","params":{"limit":50}}"#,
    ] {
        let v = call(msg).await.unwrap();
        assert_eq!(err_code(&v), -32603, "msg={msg}");
    }
}

#[tokio::test]
async fn sentry_get_issue_missing_id_and_short_id_is_minus_32602() {
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"sentry.getIssue","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Missing required parameter: id")
    );
}

#[tokio::test]
async fn sentry_get_issue_routes_with_id_or_short_id() {
    for params in [r#"{"id":"1"}"#, r#"{"shortId":"WEB-1"}"#] {
        let msg =
            format!(r#"{{"jsonrpc":"2.0","id":1,"method":"sentry.getIssue","params":{params}}}"#);
        let v = call(&msg).await.unwrap();
        assert_eq!(err_code(&v), -32603, "params={params}");
    }
}

#[tokio::test]
async fn sentry_write_arms_require_non_empty_id() {
    for method in [
        "sentry.resolveIssue",
        "sentry.ignoreIssue",
        "sentry.assignIssue",
    ] {
        // Missing `id` → `-32602`.
        let msg = format!(r#"{{"jsonrpc":"2.0","id":1,"method":"{method}","params":{{}}}}"#);
        let v = call(&msg).await.unwrap();
        assert_eq!(err_code(&v), -32602, "{method}");
        assert_eq!(
            v["error"]["message"],
            serde_json::json!("Missing required parameter: id"),
            "{method}"
        );

        // Empty `id` → `-32602`.
        let msg = format!(r#"{{"jsonrpc":"2.0","id":1,"method":"{method}","params":{{"id":""}}}}"#);
        let v = call(&msg).await.unwrap();
        assert_eq!(err_code(&v), -32602, "{method}");
    }
}

#[tokio::test]
async fn sentry_write_arms_route_with_id() {
    for method in ["sentry.resolveIssue", "sentry.ignoreIssue"] {
        let msg =
            format!(r#"{{"jsonrpc":"2.0","id":1,"method":"{method}","params":{{"id":"1"}}}}"#);
        let v = call(&msg).await.unwrap();
        assert_eq!(err_code(&v), -32603, "{method}");
    }
}

#[tokio::test]
async fn sentry_assign_issue_routes_with_or_without_assigned_to() {
    // assignedTo absent → unassign; both routes past param validation.
    for params in [
        r#"{"id":"1"}"#,
        r#"{"id":"1","assignedTo":"user-1"}"#,
        r#"{"id":"1","assignedTo":null}"#,
    ] {
        let msg = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"sentry.assignIssue","params":{params}}}"#
        );
        let v = call(&msg).await.unwrap();
        assert_eq!(err_code(&v), -32603, "params={params}");
    }
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

/// Audit A F5: the `comment.add` arm forwards `params.idempotencyKey` to the
/// service (the fake echoes it back as the comment id) instead of silently
/// dropping it like it used to.
#[tokio::test]
async fn comment_add_forwards_idempotency_key() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"comment.add","params":{"workspaceId":"ws-1","noteId":"n1","searchContext":"a test sentence","commentTarget":"test","comment":"nice","idempotencyKey":"idem-42"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["commentId"], serde_json::json!("idem-42"));

    // Present-but-empty (or whitespace-only) keys are treated as absent at the
    // router boundary, so distinct empty-key calls can never dedupe onto the
    // first cached result. The fake falls back to "c1" when the key is None.
    let v = call(
        r#"{"jsonrpc":"2.0","id":2,"method":"comment.add","params":{"workspaceId":"ws-1","noteId":"n1","searchContext":"a test sentence","commentTarget":"test","comment":"nice","idempotencyKey":"  "}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["commentId"], serde_json::json!("c1"));
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
async fn task_remove_agent_from_all_tasks_param_validation_and_routing() {
    // Missing workspaceId → -32602.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"task.removeAgentFromAllTasks","params":{"agentId":"a1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("workspaceId is required")
    );

    // Missing agentId → -32602.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"task.removeAgentFromAllTasks","params":{"workspaceId":"ws-1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Missing required parameter: agentId")
    );

    // Both present routes past param validation into the trait default
    // (`Internal` → `-32603`), proving the arm is wired.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"task.removeAgentFromAllTasks","params":{"workspaceId":"ws-1","agentId":"a1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32603);
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

/// Audit A F3: the singular `event.subscribe` / `event.unsubscribe` router
/// arms are gone — the only subscription surface is the connection-level
/// `events.subscribe` / `events.unsubscribe` fast-path (PROTOCOL §6), so the
/// dispatcher answers `-32601` for the singular spellings.
#[tokio::test]
async fn singular_event_subscribe_aliases_are_not_routable() {
    for frame in [
        r#"{"jsonrpc":"2.0","id":1,"method":"event.subscribe","params":{"workspaceId":"ws-1","eventTypes":["agent:*"]}}"#,
        r#"{"jsonrpc":"2.0","id":2,"method":"event.unsubscribe","params":{"workspaceId":"ws-1","subscriptionId":"s1"}}"#,
    ] {
        let v = call(frame).await.unwrap();
        assert_eq!(err_code(&v), -32601);
        assert_eq!(v["error"]["message"], serde_json::json!("Method not found"));
    }
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

    // agent.respondPermission without requestId.
    let v = call(
        r#"{"jsonrpc":"2.0","id":6,"method":"agent.respondPermission","params":{"outcome":{"outcome":"cancelled"}}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Missing required parameter: requestId")
    );

    // agent.respondPermission without outcome.
    let v = call(
        r#"{"jsonrpc":"2.0","id":7,"method":"agent.respondPermission","params":{"requestId":"perm_1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Missing required parameter: outcome")
    );

    // agent.enhancePrompt (§5.31) without prompt.
    let v = call(r#"{"jsonrpc":"2.0","id":8,"method":"agent.enhancePrompt","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Missing required parameter: prompt")
    );

    // agent.enhancePrompt with a blank prompt.
    let v = call(
        r#"{"jsonrpc":"2.0","id":9,"method":"agent.enhancePrompt","params":{"prompt":"   "}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("prompt cannot be empty")
    );

    // agent.enhancePrompt with an unknown mode.
    let v = call(
        r#"{"jsonrpc":"2.0","id":10,"method":"agent.enhancePrompt","params":{"prompt":"improve me","mode":"summarize"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("mode must be \"enhance\" or \"layout\"")
    );

    // agent.enhancePrompt with a non-positive timeoutMs.
    let v = call(
        r#"{"jsonrpc":"2.0","id":11,"method":"agent.enhancePrompt","params":{"prompt":"improve me","timeoutMs":0}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("timeoutMs must be a positive integer")
    );

    // agent.completeOnce (§5.32) without prompt.
    let v = call(r#"{"jsonrpc":"2.0","id":12,"method":"agent.completeOnce","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Missing required parameter: prompt")
    );

    // agent.completeOnce with a blank prompt.
    let v = call(
        r#"{"jsonrpc":"2.0","id":13,"method":"agent.completeOnce","params":{"prompt":"   "}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("prompt cannot be empty")
    );

    // agent.completeOnce with a non-positive timeoutMs.
    let v = call(
        r#"{"jsonrpc":"2.0","id":14,"method":"agent.completeOnce","params":{"prompt":"hi","timeoutMs":0}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("timeoutMs must be a positive integer")
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

    // models.list (§5.30) takes no params and must route too.
    let v = call(r#"{"jsonrpc":"2.0","id":8,"method":"models.list"}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32603);

    // agent.enhancePrompt (§5.31) with valid params routes past dispatch (the
    // default impl yields -32603, never -32601).
    let v = call(
        r#"{"jsonrpc":"2.0","id":9,"method":"agent.enhancePrompt","params":{"prompt":"improve me","mode":"layout","model":"haiku4.5","timeoutMs":5000}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32603);

    // agent.completeOnce (§5.32) with valid params routes past dispatch (the
    // default impl yields -32603, never -32601).
    let v = call(
        r#"{"jsonrpc":"2.0","id":10,"method":"agent.completeOnce","params":{"prompt":"pick a slug","systemPrompt":"be terse","model":"haiku4.5","timeoutMs":5000}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32603);

    // agent.pendingPermissions takes an optional agentId and must route.
    let v = call(r#"{"jsonrpc":"2.0","id":3,"method":"agent.pendingPermissions","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32603);

    // agent.respondPermission with valid params routes past dispatch (the
    // default impl yields -32603, never -32601).
    let v = call(
        r#"{"jsonrpc":"2.0","id":4,"method":"agent.respondPermission","params":{"requestId":"perm_1","outcome":{"outcome":"cancelled"}}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32603);
}

/// `agent.rename` forwards the optional `skipIfExplicitlySet` flag (defaulting
/// to `false` when omitted) and trims the name before dispatch (P3-1.2b).
#[tokio::test]
async fn agent_rename_forwards_skip_if_explicitly_set() {
    // Omitted → false.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"agent.rename","params":{"agentId":"agent-1","name":" Neo "}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["name"], "Neo");
    assert_eq!(v["result"]["skipIfExplicitlySet"], serde_json::json!(false));

    // Supplied → forwarded verbatim.
    let v = call(
        r#"{"jsonrpc":"2.0","id":2,"method":"agent.rename","params":{"agentId":"agent-1","name":"Neo","skipIfExplicitlySet":true}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["skipIfExplicitlySet"], serde_json::json!(true));
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
async fn git_discard_returns_ok_and_paths() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"git.discard","params":{"workspaceId":"ws-1","paths":["src/a.ts","src/b.ts"]}}"#,
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
async fn git_discard_missing_paths_is_minus_32602() {
    let v =
        call(r#"{"jsonrpc":"2.0","id":1,"method":"git.discard","params":{"workspaceId":"ws-1"}}"#)
            .await
            .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Missing required parameter: paths")
    );
}

#[tokio::test]
async fn git_discard_all_is_rejected_with_minus_32603() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"git.discard","params":{"workspaceId":"ws-1","paths":"."}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32603);
}

#[tokio::test]
async fn git_discard_all_array_form_is_rejected_with_minus_32603() {
    // Regression: the array-form discard-all bypass. `["*"]` / `["--all"]`
    // must be rejected exactly like the top-level string form.
    for paths in ["[\"*\"]", "[\"--all\"]", "[\".\"]"] {
        let frame = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"git.discard","params":{{"workspaceId":"ws-1","paths":{paths}}}}}"#
        );
        let v = call(&frame).await.unwrap();
        assert_eq!(err_code(&v), -32603, "expected -32603 for paths={paths}");
        // `Error::Internal` maps to `code=-32603` + generic message with the
        // detail string carried in `data` (router's `domain_to_rpc`).
        assert!(
            v["error"]["data"]
                .as_str()
                .unwrap_or("")
                .contains("Discarding all files is not allowed"),
            "expected discard-oriented message for paths={paths}: {v}",
        );
    }
}

#[tokio::test]
async fn git_discard_empty_parsed_list_is_minus_32603() {
    // Regression: an empty parsed list (`[]`, `[null]`, `" , "`) must be
    // `-32603` with the no-paths message, not a silent `ok: true`.
    for paths in ["[]", "[null]", "\" , \""] {
        let frame = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"git.discard","params":{{"workspaceId":"ws-1","paths":{paths}}}}}"#
        );
        let v = call(&frame).await.unwrap();
        assert_eq!(err_code(&v), -32603, "expected -32603 for paths={paths}");
        assert!(
            v["error"]["data"]
                .as_str()
                .unwrap_or("")
                .contains("No file paths provided"),
            "expected no-paths message for paths={paths}: {v}",
        );
    }
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
async fn git_branch_status_returns_status_shape() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"git.branchStatus","params":{"repoPath":"/repo","branchName":"feature"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["branch"], serde_json::json!("feature"));
    assert_eq!(v["result"]["currentBranch"], serde_json::json!("feature"));
    assert_eq!(v["result"]["isCurrentBranch"], serde_json::json!(true));
    assert_eq!(v["result"]["ahead"], serde_json::json!(1));
    assert_eq!(v["result"]["behind"], serde_json::json!(2));
    assert_eq!(
        v["result"]["hasUncommittedChanges"],
        serde_json::json!(true)
    );
}

#[tokio::test]
async fn git_branch_status_other_branch_marks_not_current() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"git.branchStatus","params":{"repoPath":"/repo","branchName":"main"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["branch"], serde_json::json!("main"));
    assert_eq!(v["result"]["currentBranch"], serde_json::json!("feature"));
    assert_eq!(v["result"]["isCurrentBranch"], serde_json::json!(false));
}

#[tokio::test]
async fn git_branch_status_missing_repo_path_is_minus_32602() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"git.branchStatus","params":{"branchName":"main"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Missing required parameter: repoPath")
    );
}

#[tokio::test]
async fn git_branch_status_missing_branch_name_is_minus_32602() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"git.branchStatus","params":{"repoPath":"/repo"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Missing required parameter: branchName")
    );
}

#[tokio::test]
async fn git_branch_status_unknown_repo_is_minus_32602_with_message() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"git.branchStatus","params":{"repoPath":"/unknown","branchName":"main"}}"#,
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

#[tokio::test]
async fn repo_remove_routes_path_and_returns_removed_flag() {
    let v =
        call(r#"{"jsonrpc":"2.0","id":1,"method":"repo.remove","params":{"path":"/src/intent"}}"#)
            .await
            .unwrap();
    assert_eq!(v["result"], serde_json::json!({ "removed": true }));

    let v = call(r#"{"jsonrpc":"2.0","id":2,"method":"repo.remove","params":{"path":"/other"}}"#)
        .await
        .unwrap();
    assert_eq!(v["result"], serde_json::json!({ "removed": false }));
}

#[tokio::test]
async fn repo_remove_requires_path_param() {
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"repo.remove","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
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
    // Explicit cols/rows/cwd/command/env flow through to the service call.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"terminal.create","params":{"workspaceId":"ws-1","cols":120,"rows":40,"cwd":"/tmp","command":"bash","env":{"FOO":"bar","BAZ":"qux"}}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["terminalId"], serde_json::json!("pty-1"));
    assert_eq!(v["result"]["cols"], serde_json::json!(120));
    assert_eq!(v["result"]["rows"], serde_json::json!(40));
    assert_eq!(v["result"]["cwd"], serde_json::json!("/tmp"));
    assert_eq!(v["result"]["command"], serde_json::json!("bash"));
    assert_eq!(
        v["result"]["env"],
        serde_json::json!({ "FOO": "bar", "BAZ": "qux" })
    );

    // Defaults applied (80x24) when dims are absent; env absent → null.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"terminal.create","params":{"workspaceId":"ws-1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["cols"], serde_json::json!(80));
    assert_eq!(v["result"]["rows"], serde_json::json!(24));
    assert_eq!(v["result"]["command"], serde_json::json!(null));
    assert_eq!(v["result"]["env"], serde_json::json!(null));

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

    // file.exists → { exists, isFile, isDirectory }.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"file.exists","params":{"workspaceId":"ws-1","path":"a.txt"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(
        v["result"],
        serde_json::json!({ "exists": true, "isFile": true, "isDirectory": false })
    );

    // file.stat → { size, mtime, isFile, isDirectory, isSymlink, permissions }.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"file.stat","params":{"workspaceId":"ws-1","path":"a.txt"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["size"], serde_json::json!(5u64));
    assert_eq!(
        v["result"]["mtime"],
        serde_json::json!("1970-01-01T00:00:00.000Z")
    );
    assert_eq!(v["result"]["isFile"], serde_json::json!(true));
    assert_eq!(v["result"]["isDirectory"], serde_json::json!(false));
    assert_eq!(v["result"]["isSymlink"], serde_json::json!(false));
    assert_eq!(v["result"]["permissions"], serde_json::json!("0644"));
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

    // file.exists / file.stat missing path → -32602.
    let v =
        call(r#"{"jsonrpc":"2.0","id":1,"method":"file.exists","params":{"workspaceId":"ws-1"}}"#)
            .await
            .unwrap();
    assert_eq!(err_code(&v), -32602);
    let v =
        call(r#"{"jsonrpc":"2.0","id":1,"method":"file.stat","params":{"workspaceId":"ws-1"}}"#)
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
            r#"{{"jsonrpc":"2.0","id":1,"method":"{method}","params":{{"workspaceId":"ws-1","scriptId":"s-1"}}}}"#
        );
        let v = call(&msg).await.unwrap();
        assert_eq!(v["result"]["ok"], serde_json::json!(true), "{method}");
        assert_eq!(
            v["result"]["scriptId"],
            serde_json::json!("s-1"),
            "{method}"
        );
        assert_eq!(
            v["result"]["workspaceId"],
            serde_json::json!("ws-1"),
            "{method} threads workspaceId"
        );
    }

    // script.run accepts the `timeout` alias for `timeoutSeconds`.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"script.run","params":{"workspaceId":"ws-1","scriptId":"s-1","maxLines":50,"timeout":30}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["maxLines"], serde_json::json!(50));
    assert_eq!(v["result"]["timeoutSeconds"], serde_json::json!(30));
    assert_eq!(v["result"]["workspaceId"], serde_json::json!("ws-1"));

    // script.output passes maxLines and its result is a bare plaintext string
    // (a header line + text), not an object (§5.8).
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"script.output","params":{"workspaceId":"ws-1","scriptId":"s-1","maxLines":10}}"#,
    )
    .await
    .unwrap();
    let out = v["result"]
        .as_str()
        .expect("script.output result is a string");
    assert!(out.starts_with("["), "header line present: {out:?}");
    assert!(out.contains("maxLines=10"), "maxLines threaded: {out:?}");

    // Missing scriptId → -32602.
    let v =
        call(r#"{"jsonrpc":"2.0","id":1,"method":"script.start","params":{"workspaceId":"ws-1"}}"#)
            .await
            .unwrap();
    assert_eq!(err_code(&v), -32602);

    // Missing workspaceId → -32602 (every mutating script.* op is workspace-scoped).
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"script.start","params":{"scriptId":"s-1"}}"#)
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

/// FIX 1 parity: `agent.sendMessage` / `agent.forceMessage` must forward the
/// FE-side per-turn prompt-assembly hints (`noteIds`, `stdinContext`,
/// `contextReferences`) verbatim to the [`WorkspaceApi`] call — the daemon
/// previously dropped them (see FE audit).
mod send_message_payload_forwarding {
    use std::sync::{Arc, Mutex};

    use intent_core::{AgentId, BoxFuture, Result, WorkspaceApi, WorkspaceId};
    use serde_json::{json, Value};

    use super::super::handle_message;

    /// Recorded snapshot of a single `agent_send_message` / `agent_force_message`
    /// call. Only the fields the FIX widens are asserted; the rest are captured
    /// so the tests document the full observed shape.
    #[derive(Default, Debug, Clone)]
    #[allow(dead_code)]
    struct Capture {
        workspace_id: Option<WorkspaceId>,
        agent_id: Option<AgentId>,
        content: Option<String>,
        message_id: Option<String>,
        image_blocks: Option<Value>,
        file_blocks: Option<Value>,
        priority: Option<String>,
        note_ids: Option<Value>,
        stdin_context: Option<String>,
        context_references: Option<Value>,
        message_metadata: Option<Value>,
    }

    #[derive(Default)]
    struct RecordingApi {
        send: Arc<Mutex<Capture>>,
        force: Arc<Mutex<Capture>>,
    }

    impl WorkspaceApi for RecordingApi {
        #[allow(clippy::too_many_arguments)]
        fn agent_send_message(
            &self,
            workspace_id: WorkspaceId,
            agent_id: AgentId,
            content: String,
            message_id: Option<String>,
            image_blocks: Option<Value>,
            file_blocks: Option<Value>,
            priority: Option<String>,
            note_ids: Option<Value>,
            stdin_context: Option<String>,
            context_references: Option<Value>,
            message_metadata: Option<Value>,
        ) -> BoxFuture<'_, Result<Value>> {
            let slot = self.send.clone();
            Box::pin(async move {
                *slot.lock().unwrap() = Capture {
                    workspace_id: Some(workspace_id),
                    agent_id: Some(agent_id),
                    content: Some(content),
                    message_id,
                    image_blocks,
                    file_blocks,
                    priority,
                    note_ids,
                    stdin_context,
                    context_references,
                    message_metadata,
                };
                Ok(json!({ "success": true, "queued": false, "messageId": "m-1" }))
            })
        }

        #[allow(clippy::too_many_arguments)]
        fn agent_force_message(
            &self,
            workspace_id: WorkspaceId,
            agent_id: AgentId,
            message_id: String,
            content: String,
            image_blocks: Option<Value>,
            file_blocks: Option<Value>,
            note_ids: Option<Value>,
            stdin_context: Option<String>,
            context_references: Option<Value>,
            message_metadata: Option<Value>,
        ) -> BoxFuture<'_, Result<Value>> {
            let slot = self.force.clone();
            Box::pin(async move {
                *slot.lock().unwrap() = Capture {
                    workspace_id: Some(workspace_id),
                    agent_id: Some(agent_id),
                    content: Some(content),
                    message_id: Some(message_id),
                    image_blocks,
                    file_blocks,
                    priority: None,
                    note_ids,
                    stdin_context,
                    context_references,
                    message_metadata,
                };
                Ok(json!({ "success": true, "queued": false, "messageId": "m-2" }))
            })
        }
    }

    #[tokio::test]
    async fn send_message_forwards_note_ids_stdin_context_and_context_references() {
        let api = RecordingApi::default();
        let msg = r#"{
            "jsonrpc":"2.0","id":1,"method":"agent.sendMessage",
            "params":{
                "workspaceId":"ws-1",
                "agentId":"agent-1",
                "content":"hi",
                "messageId":"m-1",
                "priority":"interrupt",
                "noteIds":["note-a","note-b"],
                "stdinContext":"ctx text",
                "contextReferences":[{"path":"src/a.rs"}]
            }
        }"#;
        let out = handle_message(&api, msg).await.expect("response");
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["result"]["success"], Value::Bool(true));

        let cap = api.send.lock().unwrap().clone();
        assert_eq!(cap.workspace_id.as_ref().map(|w| w.as_str()), Some("ws-1"));
        assert_eq!(cap.agent_id.as_ref().map(|a| a.as_str()), Some("agent-1"));
        assert_eq!(cap.content.as_deref(), Some("hi"));
        assert_eq!(cap.message_id.as_deref(), Some("m-1"));
        assert_eq!(cap.priority.as_deref(), Some("interrupt"));
        assert_eq!(cap.stdin_context.as_deref(), Some("ctx text"));
        assert_eq!(
            cap.note_ids,
            Some(json!(["note-a", "note-b"])),
            "noteIds must be forwarded verbatim"
        );
        assert_eq!(
            cap.context_references,
            Some(json!([{"path": "src/a.rs"}])),
            "contextReferences must be forwarded verbatim"
        );
    }

    #[tokio::test]
    async fn send_message_omitted_hints_are_none() {
        let api = RecordingApi::default();
        let msg = r#"{
            "jsonrpc":"2.0","id":2,"method":"agent.sendMessage",
            "params":{"workspaceId":"ws-1","agentId":"agent-1","content":"hi"}
        }"#;
        handle_message(&api, msg).await.expect("response");
        let cap = api.send.lock().unwrap().clone();
        assert!(cap.note_ids.is_none());
        assert!(cap.stdin_context.is_none());
        assert!(cap.context_references.is_none());
        assert!(cap.priority.is_none());
    }

    #[tokio::test]
    async fn force_message_forwards_stdin_context_and_context_references() {
        let api = RecordingApi::default();
        let msg = r#"{
            "jsonrpc":"2.0","id":3,"method":"agent.forceMessage",
            "params":{
                "workspaceId":"ws-1",
                "agentId":"agent-1",
                "messageId":"m-force",
                "content":"stop",
                "noteIds":["note-x"],
                "stdinContext":"forced ctx",
                "contextReferences":[{"symbol":"Foo"}]
            }
        }"#;
        handle_message(&api, msg).await.expect("response");
        let cap = api.force.lock().unwrap().clone();
        assert_eq!(cap.message_id.as_deref(), Some("m-force"));
        assert_eq!(cap.content.as_deref(), Some("stop"));
        assert_eq!(cap.stdin_context.as_deref(), Some("forced ctx"));
        assert_eq!(cap.note_ids, Some(json!(["note-x"])));
        assert_eq!(cap.context_references, Some(json!([{"symbol": "Foo"}])));
    }

    #[tokio::test]
    async fn send_message_forwards_image_and_file_blocks() {
        let api = RecordingApi::default();
        let msg = r#"{
            "jsonrpc":"2.0","id":4,"method":"agent.sendMessage",
            "params":{
                "workspaceId":"ws-1",
                "agentId":"agent-1",
                "content":"hi",
                "imageBlocks":[{"data":"aGVsbG8=","mimeType":"image/png"}],
                "fileBlocks":[{"data":"Zm9v","mimeType":"text/plain","fileName":"notes.txt"}]
            }
        }"#;
        handle_message(&api, msg).await.expect("response");
        let cap = api.send.lock().unwrap().clone();
        assert_eq!(
            cap.image_blocks,
            Some(json!([{"data": "aGVsbG8=", "mimeType": "image/png"}])),
            "imageBlocks must be forwarded verbatim"
        );
        assert_eq!(
            cap.file_blocks,
            Some(json!([{"data": "Zm9v", "mimeType": "text/plain", "fileName": "notes.txt"}])),
            "fileBlocks must be forwarded verbatim"
        );
    }

    /// `messageMetadata` (PROTOCOL §5.5) is the opaque per-message payload
    /// the FE attaches to distinguish daemon-initiated turns; both
    /// `agent.sendMessage` and `agent.forceMessage` must forward it
    /// verbatim to [`WorkspaceApi`] so the store can persist it on the
    /// user row (Fidelity B).
    #[tokio::test]
    async fn send_and_force_message_forward_message_metadata_verbatim() {
        let api = RecordingApi::default();
        let send = r#"{
            "jsonrpc":"2.0","id":10,"method":"agent.sendMessage",
            "params":{
                "workspaceId":"ws-1","agentId":"agent-1","content":"hi",
                "messageMetadata":{"source":"system","tag":"restart"}
            }
        }"#;
        handle_message(&api, send).await.expect("send response");
        let cap = api.send.lock().unwrap().clone();
        assert_eq!(
            cap.message_metadata,
            Some(json!({"source": "system", "tag": "restart"})),
            "sendMessage must forward messageMetadata verbatim"
        );

        let force = r#"{
            "jsonrpc":"2.0","id":11,"method":"agent.forceMessage",
            "params":{
                "workspaceId":"ws-1","agentId":"agent-1",
                "messageId":"m-force","content":"stop",
                "messageMetadata":{"kind":"queue-drain"}
            }
        }"#;
        handle_message(&api, force).await.expect("force response");
        let cap = api.force.lock().unwrap().clone();
        assert_eq!(
            cap.message_metadata,
            Some(json!({"kind": "queue-drain"})),
            "forceMessage must forward messageMetadata verbatim"
        );
    }

    /// Omitted `messageMetadata` collapses to `None` on both arms (same
    /// contract as the other opaque payloads).
    #[tokio::test]
    async fn omitted_message_metadata_is_none() {
        let api = RecordingApi::default();
        let send = r#"{
            "jsonrpc":"2.0","id":12,"method":"agent.sendMessage",
            "params":{"workspaceId":"ws-1","agentId":"agent-1","content":"hi"}
        }"#;
        handle_message(&api, send).await.expect("send response");
        assert!(api.send.lock().unwrap().message_metadata.is_none());

        let force = r#"{
            "jsonrpc":"2.0","id":13,"method":"agent.forceMessage",
            "params":{
                "workspaceId":"ws-1","agentId":"agent-1",
                "messageId":"m-x","content":"stop"
            }
        }"#;
        handle_message(&api, force).await.expect("force response");
        assert!(api.force.lock().unwrap().message_metadata.is_none());
    }

    #[tokio::test]
    async fn force_message_forwards_image_and_file_blocks() {
        let api = RecordingApi::default();
        let msg = r#"{
            "jsonrpc":"2.0","id":5,"method":"agent.forceMessage",
            "params":{
                "workspaceId":"ws-1",
                "agentId":"agent-1",
                "messageId":"m-force",
                "content":"stop",
                "imageBlocks":[{"data":"YWFh","mimeType":"image/jpeg"}],
                "fileBlocks":[{"data":"YmJi","mimeType":"application/pdf","fileName":"spec.pdf"}]
            }
        }"#;
        handle_message(&api, msg).await.expect("response");
        let cap = api.force.lock().unwrap().clone();
        assert_eq!(
            cap.image_blocks,
            Some(json!([{"data": "YWFh", "mimeType": "image/jpeg"}])),
            "imageBlocks must be forwarded verbatim"
        );
        assert_eq!(
            cap.file_blocks,
            Some(json!([{"data": "YmJi", "mimeType": "application/pdf", "fileName": "spec.pdf"}])),
            "fileBlocks must be forwarded verbatim"
        );
    }
}
