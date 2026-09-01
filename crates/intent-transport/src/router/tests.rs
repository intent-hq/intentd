//! Router error-matrix + dispatch unit tests using a fake `WorkspaceApi`.

use intent_core::{
    AgentId, AuthorType, BoxFuture, Comment, CommentAddResult, CommentLocation,
    CommentResolveThreadResult, CommentRespondResult, CommentRespondThread, CommentStatus,
    CommentType, CommentWire, ContentType, Error, Event, EventQueryParams, FileStatus,
    GitAgentCommitResult, GitBranchStatus, GitBranches, GitCommitResult, GitFileStatus,
    GitMergeConflicts, GitStatus, Note, NoteAddInput, NoteAddResult, NoteCreate, NoteCreateResult,
    NoteDeleteResult, NoteEditInput, NoteEditLinesInput, NoteEditLinesResult, NoteEditResult,
    NoteId, NoteMetadata, NoteSetContentResult, NoteTaskRow, NoteUpdateInput,
    NoteUpdateMetadataResult, NoteVisibility, ReadAssetResult, RepoConfig, Result,
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
        status_image_asset_id: None,
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
        pull_requests: None,
        context_links: None,
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
        disk_usage: None,
        pending_delete_at: None,
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
    fn agent_list_active(&self) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async {
            Ok(serde_json::json!({
                "streams": [{
                    "agentId": "agent-active",
                    "sessionId": "agent-active",
                    "workspaceId": "ws-active",
                    "startTime": 1_750_000_000_000_i64,
                }],
            }))
        })
    }

    fn debug_sample_stacks(
        &self,
        duration_ms: Option<i64>,
        frequency_hz: Option<i64>,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(serde_json::json!({
                "report": "fake stack report",
                "durationMs": duration_ms,
                "frequencyHz": frequency_hz,
                "sampleCount": 1,
                "distinctStacks": 1,
            }))
        })
    }

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
    fn workspace_disk_usage(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            if id.as_str() == "missing" {
                return Err(Error::NotFound("workspace".to_string()));
            }
            Ok(serde_json::json!({
                "diskUsage": {
                    "bytes": 4096,
                    "fileCount": 1,
                    "computedAt": "2026-01-01T00:00:00Z",
                    "breakdown": [],
                },
                "refreshing": true,
            }))
        })
    }
    fn workspace_transfer_plan(
        &self,
        id: WorkspaceId,
    ) -> BoxFuture<'_, Result<intent_core::transfer::TransferPlan>> {
        Box::pin(async move {
            if id.as_str() == "missing" {
                return Err(Error::NotFound("workspace".to_string()));
            }
            Ok(intent_core::transfer::TransferPlan {
                manifest: intent_core::transfer::TransferManifest {
                    format_version: intent_core::transfer::TRANSFER_FORMAT_VERSION,
                    creating_intentd_version: "0.0.0".to_string(),
                    workspace_id: id,
                    created_at: "2026-01-01T00:00:00Z".to_string(),
                    tables: vec![intent_core::transfer::TransferTableStat {
                        name: "note".to_string(),
                        row_count: 2,
                        approx_bytes: 100,
                    }],
                    assets: vec![],
                    attachments: vec![],
                    git: intent_core::transfer::TransferGitSummary {
                        has_repository: false,
                        branch: None,
                        dirty_files: vec![],
                        sandbox_branches: vec![],
                    },
                },
                total_size_bytes: 100,
                db_row_bytes: 100,
                asset_bytes: 0,
                attachment_bytes: 0,
                estimated_git_bundle_bytes: 0,
                warnings: vec![],
            })
        })
    }
    fn create_workspace(
        &self,
        input: WorkspaceCreate,
        _idempotency_key: Option<String>,
    ) -> BoxFuture<'_, Result<intent_core::WorkspaceCreateResult>> {
        Box::pin(async move {
            if let Some(base_ref) = input.base_ref.filter(|r| r == "no-such-ref") {
                return Err(Error::BaseRefUnresolvable { base_ref });
            }
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
    fn schedule_workspace_delete(
        &self,
        id: WorkspaceId,
        undo_delay_ms: u64,
    ) -> BoxFuture<'_, Result<String>> {
        Box::pin(async move {
            if id.as_str() == "missing" {
                return Err(Error::NotFound("workspace".to_string()));
            }
            Ok(format!("2026-01-01T00:00:{:02}Z", undo_delay_ms / 1000))
        })
    }
    fn cancel_workspace_delete(&self, id: WorkspaceId) -> BoxFuture<'_, Result<bool>> {
        Box::pin(async move {
            // "pending" has a scheduled deletion; anything else reports the
            // race-safe non-error `false`.
            Ok(id.as_str() == "pending")
        })
    }
    fn agent_delete(
        &self,
        agent_id: AgentId,
        _workspace_id: Option<WorkspaceId>,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            if agent_id.0 == "missing" {
                return Err(Error::NotFound("agent session".to_string()));
            }
            Ok(serde_json::json!({ "success": true }))
        })
    }
    fn agent_schedule_delete(
        &self,
        agent_id: AgentId,
        _workspace_id: Option<WorkspaceId>,
        undo_delay_ms: u64,
    ) -> BoxFuture<'_, Result<String>> {
        Box::pin(async move {
            if agent_id.0 == "missing" {
                return Err(Error::NotFound("agent session".to_string()));
            }
            Ok(format!("2026-01-01T00:00:{:02}Z", undo_delay_ms / 1000))
        })
    }
    fn agent_cancel_delete(
        &self,
        agent_id: AgentId,
        _workspace_id: Option<WorkspaceId>,
    ) -> BoxFuture<'_, Result<bool>> {
        Box::pin(async move {
            // "agent-pending" has a scheduled deletion; anything else reports
            // the race-safe non-error `false`.
            Ok(agent_id.0 == "agent-pending")
        })
    }
    fn archive_workspace(
        &self,
        id: WorkspaceId,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<Workspace>> {
        // The RPC front door has no calling agent: the sweep interrupts every
        // in-flight turn in the workspace.
        assert!(caller_agent_id.is_none(), "router passes no caller agent");
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
    fn duplicate_workspace(
        &self,
        id: WorkspaceId,
        new_title: Option<String>,
    ) -> BoxFuture<'_, Result<Workspace>> {
        Box::pin(async move {
            if id.as_str() == "missing" {
                return Err(Error::NotFound("workspace".to_string()));
            }
            let mut ws = ws_with(&WorkspaceId::from(format!("{}-copy", id.as_str())));
            ws.title = new_title.unwrap_or_else(|| format!("{} (Copy)", ws.title));
            Ok(ws)
        })
    }
    fn cleanup_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            if id.as_str() == "missing" {
                return Err(Error::NotFound("workspace".to_string()));
            }
            Ok(())
        })
    }
    fn find_repositories(&self, directory: String) -> BoxFuture<'_, Result<Vec<String>>> {
        Box::pin(async move {
            if directory == "fail" {
                return Err(Error::Internal("scan failed".to_string()));
            }
            Ok(vec![
                format!("{directory}/repo-a"),
                format!("{directory}/repo-b"),
            ])
        })
    }
    fn initialize_repository(&self, path: String) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            if path == "fail" {
                return Err(Error::Internal("init failed".to_string()));
            }
            Ok(())
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
        _caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<NoteCreateResult>> {
        Box::pin(async move {
            let mut note = sample_note(&workspace_id);
            note.id = NoteId::from("created");
            note.title = input.title;
            Ok(NoteCreateResult {
                note,
                converted_count: 0,
                created_task_note_ids: Vec::new(),
                created_tasks: Vec::new(),
                warnings: Vec::new(),
            })
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
        _caller_agent_id: Option<AgentId>,
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
                created_tasks: vec![],
                warnings: vec![],
            })
        })
    }

    fn edit_note(
        &self,
        _workspace_id: WorkspaceId,
        note_id: NoteId,
        input: NoteEditInput,
        _caller_agent_id: Option<AgentId>,
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
                created_tasks: vec![],
                warnings: vec![],
            })
        })
    }

    fn edit_note_lines(
        &self,
        _workspace_id: WorkspaceId,
        note_id: NoteId,
        input: NoteEditLinesInput,
        _caller_agent_id: Option<AgentId>,
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
                created_tasks: vec![],
                warnings: vec![],
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
        _caller_agent_id: Option<AgentId>,
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
                created_tasks: vec![],
                warnings: vec![],
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
        _caller_agent_id: Option<AgentId>,
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
                depends_on: Vec::new(),
                conflicts_with: Vec::new(),
                unmet_depends_on: Vec::new(),
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
        _author_type: Option<String>,
        idempotency_key: Option<String>,
        comment_id: Option<String>,
    ) -> BoxFuture<'_, Result<CommentAddResult>> {
        Box::pin(async move {
            Ok(CommentAddResult {
                success: true,
                message: format!("Comment successfully anchored to \"{comment_target}\""),
                // Echo the supplied `commentId` (then the idempotency key) so
                // router tests can pin that the arm forwards both params
                // instead of silently dropping them.
                comment_id: comment_id
                    .or(idempotency_key)
                    .unwrap_or_else(|| "c1".to_string()),
                anchored: true,
                note_rev: 1,
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
        _author_type: Option<String>,
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
                // Replies carry no anchor of their own (monorepo#729).
                anchor: None,
                anchor_text: None,
                anchor_before: None,
                anchor_after: None,
                suggestion_original,
                suggestion_proposed,
                agent_id: None,
                is_orphaned: None,
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

    // Small test values: loss-free in f64.
    #[allow(clippy::cast_precision_loss)]
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
                actor: intent_core::EventActor::default(),
                session_id: None,
                correlation_id: None,
                parent_event_id: None,
                metadata: None,
                data: serde_json::json!({}),
            };
            Ok(serde_json::to_value(vec![event]).unwrap())
        })
    }

    fn git_root_list(&self, workspace_id: WorkspaceId) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            if workspace_id.as_str() == "missing" {
                return Err(Error::NotFound(format!("workspace {workspace_id}")));
            }
            Ok(serde_json::json!({
                "gitRoots": [{
                    "id": "root-1",
                    "workspaceId": workspace_id.as_str(),
                    "path": "/tmp/clone-a",
                    "source": "agent",
                    "branch": "feature",
                }]
            }))
        })
    }

    fn git_root_path(
        &self,
        _workspace_id: WorkspaceId,
        git_root_id: intent_core::WorkspaceGitRootId,
    ) -> BoxFuture<'_, Result<String>> {
        Box::pin(async move {
            if git_root_id.as_str() == "root-1" {
                Ok("/repo".to_string())
            } else {
                Err(Error::InvalidParams(format!(
                    "Unknown git root: {git_root_id}"
                )))
            }
        })
    }

    fn git_status(
        &self,
        workspace_id: WorkspaceId,
        git_root_id: Option<intent_core::WorkspaceGitRootId>,
    ) -> BoxFuture<'_, Result<GitStatus>> {
        self.git_status_with_options(workspace_id, git_root_id, false)
    }

    fn git_status_with_options(
        &self,
        workspace_id: WorkspaceId,
        git_root_id: Option<intent_core::WorkspaceGitRootId>,
        force_refresh: bool,
    ) -> BoxFuture<'_, Result<GitStatus>> {
        Box::pin(async move {
            if let Some(id) = &git_root_id {
                if id.as_str() != "root-1" {
                    return Err(Error::InvalidParams(format!("Unknown git root: {id}")));
                }
                return Ok(GitStatus {
                    branch: "root-branch".to_string(),
                    ahead: 0,
                    behind: 0,
                    diverged: false,
                    files: vec![],
                    has_uncommitted_changes: false,
                    has_untracked_files: false,
                    files_truncated: false,
                    total_files: None,
                    has_upstream: false,
                    unpushed_count: None,
                });
            }
            if force_refresh {
                return Ok(GitStatus {
                    branch: "forced".to_string(),
                    ahead: 0,
                    behind: 0,
                    diverged: false,
                    files: vec![],
                    has_uncommitted_changes: false,
                    has_untracked_files: false,
                    files_truncated: false,
                    total_files: None,
                    has_upstream: false,
                    unpushed_count: None,
                });
            }
            if workspace_id.as_str() == "empty" {
                return Ok(GitStatus {
                    branch: String::new(),
                    ahead: 0,
                    behind: 0,
                    diverged: false,
                    files: vec![],
                    has_uncommitted_changes: false,
                    has_untracked_files: false,
                    files_truncated: false,
                    total_files: None,
                    has_upstream: false,
                    unpushed_count: None,
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
                    mode: None,
                    old_sha: None,
                    new_sha: None,
                }],
                has_uncommitted_changes: true,
                has_untracked_files: false,
                files_truncated: false,
                total_files: None,
                has_upstream: true,
                unpushed_count: Some(1),
            })
        })
    }

    fn git_commit_details(
        &self,
        _workspace_id: WorkspaceId,
        commit_hash: String,
        git_root_id: Option<intent_core::WorkspaceGitRootId>,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            if let Some(id) = &git_root_id {
                if id.as_str() != "root-1" {
                    return Err(Error::InvalidParams(format!("Unknown git root: {id}")));
                }
                return Ok(serde_json::json!({
                    "commitHash": commit_hash,
                    "message": "root-commit",
                    "files": ["root-only.txt"],
                    "fileDetails": [{ "path": "root-only.txt", "additions": 1, "deletions": 0 }],
                }));
            }
            Ok(serde_json::json!({
                "commitHash": commit_hash,
                "message": "primary-commit",
                "files": ["src/a.ts"],
                "fileDetails": [{ "path": "src/a.ts", "additions": 1, "deletions": 0 }],
            }))
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
                        "owner": "intent-hq",
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

    fn repo_warm_cache(&self, github_url: String) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            match github_url.as_str() {
                "https://github.com/intent-hq/busy" => Err(Error::WarmInFlight {
                    owner: "intent-hq".to_string(),
                    repo: "other".to_string(),
                }),
                "bad-url" => Err(Error::InvalidParams(format!(
                    "githubUrl carries no owner/repo pair: {github_url}"
                ))),
                _ => Ok(serde_json::json!({
                    "started": true,
                    "owner": "intent-hq",
                    "repo": "intentd"
                })),
            }
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

    fn github_repo_config_get(
        &self,
        owner: String,
        repo: String,
        git_ref: Option<String>,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(serde_json::json!({
                "config": { "branchPrefix": format!("{owner}/{repo}") },
                "echoRef": git_ref,
            }))
        })
    }

    fn github_branches_list(
        &self,
        owner: String,
        repo: String,
        prefix: Option<String>,
        _limit: Option<i64>,
        _next_token: Option<String>,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(serde_json::json!({
                "branches": [owner, repo],
                "nextToken": Value::Null,
                "echoPrefix": prefix,
            }))
        })
    }

    fn github_branches_list_cached(
        &self,
        owner: String,
        repo: String,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(serde_json::json!({
                "cached": true,
                "branches": [owner, repo],
                "defaultBranch": "main",
            }))
        })
    }

    fn github_auth_status(&self) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async {
            Ok(serde_json::json!({
                "isConfigured": true,
                "oauthUrl": "",
                "configuredButNeedsUpdate": false,
                "updatedScopes": "",
                "deviceFlow": Value::Null,
            }))
        })
    }

    fn github_connect(&self) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async {
            Ok(serde_json::json!({
                "ok": true,
                "userCode": "ABCD-1234",
                "verificationUri": "https://github.com/login/device",
                "expiresIn": 900,
                "interval": 5,
            }))
        })
    }

    fn github_cancel_auth(&self) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async { Ok(serde_json::json!({ "ok": true, "cancelled": true })) })
    }

    fn github_revoke(&self) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async { Ok(serde_json::json!({ "ok": true })) })
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

    #[allow(clippy::too_many_arguments)]
    fn git_agent_commit(
        &self,
        _workspace_id: WorkspaceId,
        _message: String,
        _agent_id: Option<AgentId>,
        _linked_note_id: Option<NoteId>,
        files: Option<Vec<String>>,
        _user_requested: bool,
        git_root_id: Option<intent_core::WorkspaceGitRootId>,
    ) -> BoxFuture<'_, Result<GitAgentCommitResult>> {
        Box::pin(async move {
            // Mirrors the services-level resolution: an unknown/foreign id
            // fails exactly like the six §5.6 root-scoped reads.
            let hash = match &git_root_id {
                Some(id) if id.as_str() == "root-1" => "root-def456",
                Some(id) => {
                    return Err(Error::InvalidParams(format!("Unknown git root: {id}")));
                }
                None => "def456",
            };
            let files = files.unwrap_or_else(|| vec!["src/a.ts".to_string()]);
            let file_count = i64::try_from(files.len()).expect("value fits in i64");
            Ok(GitAgentCommitResult {
                hash: hash.to_string(),
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
        workspace_id: Option<WorkspaceId>,
        _query: String,
        _agent_id: Option<String>,
        _role: Option<String>,
        _limit: Option<i64>,
        prefer_workspace_id: Option<WorkspaceId>,
        request_id: Option<String>,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            let request_id = request_id.unwrap_or_else(|| "srch-minted".to_string());
            Ok(serde_json::json!({
                "requestId": request_id,
                "matches": [],
                "workspaceId": workspace_id.map(|w| w.0),
                "preferWorkspaceId": prefer_workspace_id.map(|w| w.0),
            }))
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

    fn file_read(
        &self,
        workspace_id: WorkspaceId,
        path: String,
        _caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<Value>> {
        // Echo a bare string so the wire test can assert file.read is NOT
        // wrapped in an object.
        Box::pin(async move { Ok(Value::String(format!("{}:{path}", workspace_id.as_str()))) })
    }

    fn file_read_chunk(
        &self,
        _workspace_id: WorkspaceId,
        path: String,
        offset: u64,
        length: u64,
        _caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<Value>> {
        // Echo the window so the wire test can assert offset/length reach the
        // service, alongside the documented result shape.
        Box::pin(async move {
            Ok(serde_json::json!({
                "content": format!("b64:{path}:{offset}:{length}"),
                "bytesRead": length,
                "size": 1000u64,
            }))
        })
    }

    fn file_write(
        &self,
        _workspace_id: WorkspaceId,
        path: String,
        content: String,
        _caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(
                serde_json::json!({ "ok": true, "path": path, "size": content.encode_utf16().count() }),
            )
        })
    }

    fn file_list(
        &self,
        _workspace_id: WorkspaceId,
        path: String,
        _caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<Value>> {
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
        _caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(
            async move { Ok(serde_json::json!({ "ok": true, "path": path, "deleted": true })) },
        )
    }

    fn file_mkdir(
        &self,
        _workspace_id: WorkspaceId,
        path: String,
        _caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(
            async move { Ok(serde_json::json!({ "ok": true, "path": path, "created": true })) },
        )
    }

    fn file_rename(
        &self,
        _workspace_id: WorkspaceId,
        old_path: String,
        new_path: String,
        _caller_agent_id: Option<AgentId>,
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

    // Echo the parsed create params so the router tests can assert the
    // optional `nameExplicitlySet` flag threads into
    // `AgentCreateExtra.name_explicitly_set` (absent → null).
    fn agent_create(
        &self,
        workspace_id: WorkspaceId,
        name: Option<String>,
        _model: Option<String>,
        _specialist_id: Option<String>,
        _parent_agent_id: Option<AgentId>,
        _idempotency_key: Option<String>,
        extra: intent_core::AgentCreateExtra,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            Ok(serde_json::json!({
                "agent": { "id": "agent-fake", "name": name, "workspaceId": workspace_id.as_str() },
                "nameExplicitlySet": extra.name_explicitly_set,
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

    // `specialist.get`: unknown id → `NotFound`, empty id → `InvalidParams`,
    // so the router tests can assert the formerly collapsed
    // `InvalidParams | NotFound` arm carries per-origin discriminators
    // (monorepo#1320).
    fn specialist_get(
        &self,
        id: String,
        _workspace_path: Option<String>,
        _provider: Option<String>,
    ) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            if id.is_empty() {
                return Err(Error::InvalidParams("Invalid specialist id".to_string()));
            }
            Err(Error::NotFound(format!("Specialist not found: {id}")))
        })
    }

    fn get_repo_config(&self, id: WorkspaceId) -> BoxFuture<'_, Result<RepoConfig>> {
        Box::pin(async move {
            if id.as_str() == "missing" {
                return Err(Error::NotFound("workspace".to_string()));
            }
            // Return a simple config for testing
            Ok(RepoConfig {
                branch_prefix: Some("feature/".to_string()),
                ..Default::default()
            })
        })
    }

    fn save_repo_config(
        &self,
        id: WorkspaceId,
        config: serde_json::Map<String, serde_json::Value>,
    ) -> BoxFuture<'_, Result<RepoConfig>> {
        Box::pin(async move {
            if id.as_str() == "missing" {
                return Err(Error::NotFound("workspace".to_string()));
            }
            // Echo back the patch as a config (merge semantics live in
            // intent-services and are covered by its unit tests + WSS e2e).
            Ok(serde_json::from_value(serde_json::Value::Object(config)).unwrap_or_default())
        })
    }

    fn has_repo_config(&self, id: WorkspaceId) -> BoxFuture<'_, Result<bool>> {
        Box::pin(async move {
            if id.as_str() == "missing" {
                return Err(Error::NotFound("workspace".to_string()));
            }
            Ok(id.as_str() == "with-config")
        })
    }

    fn ensure_repo_intent_dir(&self, id: WorkspaceId) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            if id.as_str() == "missing" {
                return Err(Error::NotFound("workspace".to_string()));
            }
            Ok(())
        })
    }

    fn pr_refresh(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            if id.as_str() == "missing" {
                return Err(Error::NotFound("workspace".to_string()));
            }
            Ok(serde_json::json!({
                "outcome": "linked",
                "prNumber": 300,
                "prUrl": "https://github.com/o/r/pull/300",
                "prStatus": "Open",
                "pullRequests": [{ "number": 300 }],
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

/// `debug.sampleStacks` (PROTOCOL §5.43): both params optional — absent
/// params dispatch as `None` and the params object itself may be omitted;
/// numeric values are forwarded to the service layer.
#[tokio::test]
async fn debug_sample_stacks_dispatches_optional_params() {
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"debug.sampleStacks","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(v["result"]["report"], "fake stack report");
    assert!(v["result"]["durationMs"].is_null());
    assert!(v["result"]["frequencyHz"].is_null());

    let v = call(r#"{"jsonrpc":"2.0","id":2,"method":"debug.sampleStacks"}"#)
        .await
        .unwrap();
    assert_eq!(v["result"]["report"], "fake stack report");

    let v = call(
        r#"{"jsonrpc":"2.0","id":3,"method":"debug.sampleStacks","params":{"durationMs":500,"frequencyHz":50}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["durationMs"], 500);
    assert_eq!(v["result"]["frequencyHz"], 50);
}

/// `debug.sampleStacks`: a present but non-numeric `durationMs` /
/// `frequencyHz` is a caller error (`-32602`), not silently defaulted;
/// `null` is tolerated as absent (matching the `opt_int` convention).
#[tokio::test]
async fn debug_sample_stacks_non_numeric_params_are_invalid() {
    for params in [
        r#"{"durationMs":"1000"}"#,
        r#"{"frequencyHz":"99"}"#,
        r#"{"durationMs":true}"#,
        r#"{"frequencyHz":[]}"#,
    ] {
        let msg = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"debug.sampleStacks","params":{params}}}"#
        );
        let v = call(&msg).await.unwrap();
        assert_eq!(err_code(&v), -32602, "params={params}");
    }

    let v = call(
        r#"{"jsonrpc":"2.0","id":2,"method":"debug.sampleStacks","params":{"durationMs":null}}"#,
    )
    .await
    .unwrap();
    assert!(v["result"]["durationMs"].is_null(), "null tolerated: {v}");
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

/// The `note.list` `projection` param (§5.2, monorepo#3573): absent /
/// `null` / `"full"` serve identical full rows (structural `Value`
/// equality), `"slim"` serves rows with `content` replaced by
/// `contentPreview` + `contentLength` (every other field untouched), and
/// any other value is `-32602` naming the accepted values.
#[tokio::test]
async fn note_list_projection_param() {
    let full =
        call(r#"{"jsonrpc":"2.0","id":1,"method":"note.list","params":{"workspaceId":"ws-1"}}"#)
            .await
            .unwrap();
    for msg in [
        r#"{"jsonrpc":"2.0","id":1,"method":"note.list","params":{"workspaceId":"ws-1","projection":null}}"#,
        r#"{"jsonrpc":"2.0","id":1,"method":"note.list","params":{"workspaceId":"ws-1","projection":"full"}}"#,
    ] {
        let v = call(msg).await.unwrap();
        assert_eq!(v["result"], full["result"], "identical full rows: {msg}");
    }
    let full_row = &full["result"]["notes"][0];
    assert_eq!(full_row["content"], "# Hi");
    assert!(full_row.get("contentPreview").is_none());
    assert!(full_row.get("contentLength").is_none());

    let slim = call(
        r#"{"jsonrpc":"2.0","id":2,"method":"note.list","params":{"workspaceId":"ws-1","projection":"slim"}}"#,
    )
    .await
    .unwrap();
    let row = &slim["result"]["notes"][0];
    assert!(row.get("content").is_none(), "slim omits content: {row}");
    assert_eq!(row["contentPreview"], "# Hi");
    assert_eq!(row["contentLength"], 4);
    // Every other field matches the full row.
    for (k, v) in full_row.as_object().unwrap() {
        if k != "content" {
            assert_eq!(&row[k], v, "field {k} unchanged");
        }
    }

    for bad in [r#""compact""#, "5", "true", "{}"] {
        let msg = format!(
            r#"{{"jsonrpc":"2.0","id":3,"method":"note.list","params":{{"workspaceId":"ws-1","projection":{bad}}}}}"#
        );
        let v = call(&msg).await.unwrap();
        assert_eq!(err_code(&v), -32602, "projection={bad}");
        assert_eq!(
            v["error"]["message"],
            serde_json::json!("projection must be \"slim\" or \"full\"")
        );
    }
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
        r"[1,2,3]",
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
async fn workspace_create_base_ref_unresolvable_maps_to_minus_32602_with_data() {
    // An unresolvable base ref on `workspace.create` keeps the -32602 code and
    // the exact human message, and gains machine-readable `error.data`
    // (monorepo#761).
    let v = call(
        r#"{"jsonrpc":"2.0","id":3,"method":"workspace.create","params":{"repositoryPath":"/tmp/repo","baseRef":"no-such-ref"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("invalid params: cannot resolve base ref 'no-such-ref'")
    );
    assert_eq!(
        v["error"]["data"],
        serde_json::json!({ "code": "base-ref-unresolvable", "baseRef": "no-such-ref" })
    );
}

#[tokio::test]
async fn clone_failed_maps_to_structured_error_data() {
    // A classified clone failure (clone failure taxonomy, PROTOCOL §9.1;
    // monorepo#825/#826) carries the documented human message plus
    // machine-readable `error.data = { code, detail }` so clients stop
    // matching on prose. -32603 for remote failures, -32602 for the
    // user-fixable destination-exists shape.
    let rpc = super::domain_to_rpc(intent_core::Error::CloneFailed {
        category: intent_core::CloneErrorCategory::AuthRequired,
        detail: "git clone failed (exit status: 128): fatal: could not read Username \
                 for 'https://github.com': terminal prompts disabled"
            .to_string(),
    });
    assert_eq!(rpc.code, -32603);
    assert!(
        rpc.message
            .starts_with("workspace.create clone failed (auth-required):"),
        "documented message shape: {}",
        rpc.message
    );
    let data = rpc.data.expect("structured data");
    assert_eq!(data["code"], serde_json::json!("auth-required"));
    assert!(
        data["detail"]
            .as_str()
            .unwrap()
            .contains("terminal prompts disabled"),
        "detail carries the sanitized stderr tail: {data}"
    );

    let rpc = super::domain_to_rpc(intent_core::Error::CloneFailed {
        category: intent_core::CloneErrorCategory::DestinationExistsNonEmpty,
        detail: "fatal: destination path 'x' already exists and is not an empty directory."
            .to_string(),
    });
    assert_eq!(rpc.code, -32602, "user-fixable shape is -32602");
    assert_eq!(
        rpc.data.expect("structured data")["code"],
        serde_json::json!("destination-exists-non-empty")
    );

    // The additive monorepo#825 categories are environmental: -32603.
    let rpc = super::domain_to_rpc(intent_core::Error::CloneFailed {
        category: intent_core::CloneErrorCategory::RepoNotFound,
        detail: "remote: Repository not found.".to_string(),
    });
    assert_eq!(rpc.code, -32603);
    assert_eq!(
        rpc.data.expect("structured data")["code"],
        serde_json::json!("repo-not-found")
    );

    // The askpass exec-failure shape (monorepo#837) is environmental too:
    // -32603 with the documented wire spelling.
    let rpc = super::domain_to_rpc(intent_core::Error::CloneFailed {
        category: intent_core::CloneErrorCategory::AskpassMissing,
        detail: "fatal: cannot exec 'ssh-askpass-intent.sh': Not a directory".to_string(),
    });
    assert_eq!(rpc.code, -32603);
    assert_eq!(
        rpc.data.expect("structured data")["code"],
        serde_json::json!("askpass-missing")
    );
}

#[test]
fn voice_not_configured_maps_to_structured_error_data() {
    // The voice.transcribe no-API-key failure (PROTOCOL §5.41,
    // monorepo#1448) keeps the -32603 code and the generic "Internal error"
    // message, and carries machine-readable
    // `error.data = { code: "voice-no-api-key", detail }` with the
    // descriptive text unchanged so clients stop matching on prose.
    let detail = "voice not configured: voice: no API key found for elevenlabs \
                  (set voice.elevenlabs.apiKey or ELEVENLABS_API_KEY)";
    let rpc = super::domain_to_rpc(intent_core::Error::VoiceNotConfigured {
        detail: detail.to_string(),
    });
    assert_eq!(rpc.code, -32603);
    assert_eq!(rpc.message, "Internal error");
    assert_eq!(
        rpc.data.expect("structured data"),
        serde_json::json!({ "code": "voice-no-api-key", "detail": detail })
    );
}

#[test]
fn adapter_busy_maps_to_structured_error_data() {
    // An `agent.completeOnce` that queued past its own timeout at the
    // daemon-wide ephemeral-adapter bound (PROTOCOL §5.32, monorepo#2062)
    // keeps -32603 but carries `error.data = { code: "adapter-busy",
    // provider, waitedMs, limit }`, so a client tells daemon saturation apart
    // from a slow model — and from every other one-shot failure, which stay
    // bare `Internal` — without matching on prose.
    let rpc = super::domain_to_rpc(intent_core::Error::AdapterBusy {
        provider: "claude-code".to_string(),
        waited_ms: 30_000,
        limit: 6,
    });
    assert_eq!(rpc.code, -32603);
    assert_eq!(
        rpc.data.expect("structured data"),
        serde_json::json!({
            "code": "adapter-busy",
            "provider": "claude-code",
            "waitedMs": 30_000,
            "limit": 6,
        })
    );
    assert!(
        rpc.message.contains("claude-code") && rpc.message.contains("30000ms"),
        "human message names the provider and the wait: {}",
        rpc.message
    );
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

/// `workspace.diskUsage` returns the service payload verbatim:
/// `{ diskUsage?, refreshing }` with no extra envelope nesting.
#[tokio::test]
async fn workspace_disk_usage_returns_payload() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.diskUsage","params":{"workspaceId":"ws-1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["refreshing"], serde_json::json!(true));
    assert_eq!(v["result"]["diskUsage"]["bytes"], serde_json::json!(4096));
    assert_eq!(v["result"]["diskUsage"]["fileCount"], serde_json::json!(1));
}

#[tokio::test]
async fn workspace_disk_usage_missing_id_is_minus_32602() {
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"workspace.diskUsage","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Missing required parameter: workspaceId")
    );
}

#[tokio::test]
async fn workspace_disk_usage_not_found_maps_to_workspace_err() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.diskUsage","params":{"workspaceId":"missing"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Workspace not found")
    );
}

/// `workspace.transfer.plan` wraps the service payload as `{ plan }` with the
/// camelCase manifest/size fields (PROTOCOL §5.1).
#[tokio::test]
async fn workspace_transfer_plan_returns_plan_envelope() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.transfer.plan","params":{"workspaceId":"ws-1"}}"#,
    )
    .await
    .unwrap();
    let plan = &v["result"]["plan"];
    assert_eq!(plan["manifest"]["formatVersion"], serde_json::json!(1));
    assert_eq!(plan["manifest"]["workspaceId"], serde_json::json!("ws-1"));
    assert_eq!(
        plan["manifest"]["tables"][0]["rowCount"],
        serde_json::json!(2)
    );
    assert_eq!(
        plan["manifest"]["git"]["hasRepository"],
        serde_json::json!(false)
    );
    assert_eq!(plan["totalSizeBytes"], serde_json::json!(100));
    assert_eq!(plan["dbRowBytes"], serde_json::json!(100));
    assert_eq!(plan["assetBytes"], serde_json::json!(0));
    assert_eq!(plan["estimatedGitBundleBytes"], serde_json::json!(0));
}

#[tokio::test]
async fn workspace_transfer_plan_missing_id_is_minus_32602() {
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"workspace.transfer.plan","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Missing required parameter: workspaceId")
    );
}

#[tokio::test]
async fn workspace_transfer_plan_not_found_maps_to_workspace_err() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.transfer.plan","params":{"workspaceId":"missing"}}"#,
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

/// Agent ids are server-assigned: a stale client that still sends
/// `agentId` on `agent.create` is rejected up front with `-32602` before
/// the request ever reaches the service.
#[tokio::test]
async fn agent_create_rejects_client_supplied_agent_id() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"agent.create","params":{"workspaceId":"ws-1","name":"A","agentId":"agent-11111111-1111-1111-1111-111111111111"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert!(
        v["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("server-assigned"),
        "error message should say agent IDs are server-assigned: {v}"
    );
}

/// `agent.create` threads the optional `nameExplicitlySet` boolean into
/// `AgentCreateExtra.name_explicitly_set`: omitted/null stays `None` (the
/// service default `name.is_some()` holds) and a supplied bool is forwarded
/// verbatim, so a placeholder name (`false`) stays self-renameable.
#[tokio::test]
async fn agent_create_forwards_name_explicitly_set() {
    // Omitted → None (serialized as null by the echoing fake).
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"agent.create","params":{"workspaceId":"ws-1","name":"A"}}"#,
    )
    .await
    .unwrap();
    assert!(v["result"]["nameExplicitlySet"].is_null(), "omitted: {v}");

    // Explicit null → None as well.
    let v = call(
        r#"{"jsonrpc":"2.0","id":2,"method":"agent.create","params":{"workspaceId":"ws-1","name":"A","nameExplicitlySet":null}}"#,
    )
    .await
    .unwrap();
    assert!(v["result"]["nameExplicitlySet"].is_null(), "null: {v}");

    // Supplied booleans forwarded verbatim.
    let v = call(
        r#"{"jsonrpc":"2.0","id":3,"method":"agent.create","params":{"workspaceId":"ws-1","name":"A","nameExplicitlySet":false}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["nameExplicitlySet"], serde_json::json!(false));
    let v = call(
        r#"{"jsonrpc":"2.0","id":4,"method":"agent.create","params":{"workspaceId":"ws-1","name":"A","nameExplicitlySet":true}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["nameExplicitlySet"], serde_json::json!(true));
}

/// Non-boolean `nameExplicitlySet` values are rejected with `-32602` instead
/// of being silently dropped (a drop would flip the persisted flag back to
/// its `name.is_some()` default).
#[tokio::test]
async fn agent_create_rejects_non_boolean_name_explicitly_set() {
    for (id, params) in [
        (
            1,
            r#"{"workspaceId":"ws-1","name":"A","nameExplicitlySet":"false"}"#,
        ),
        (
            2,
            r#"{"workspaceId":"ws-1","name":"A","nameExplicitlySet":0}"#,
        ),
    ] {
        let v = call(&format!(
            r#"{{"jsonrpc":"2.0","id":{id},"method":"agent.create","params":{params}}}"#
        ))
        .await
        .unwrap();
        assert_eq!(err_code(&v), -32602, "non-bool rejected: {v}");
        assert_eq!(
            v["error"]["message"],
            serde_json::json!("nameExplicitlySet must be a boolean")
        );
    }
}

/// Same guard for `workspace.create`: `initialAgent.agentId` is no longer
/// accepted — reject with `-32602` before dispatching to the service.
#[tokio::test]
async fn workspace_create_rejects_initial_agent_agent_id() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"WS","initialAgent":{"agentId":"agent-11111111-1111-1111-1111-111111111111","prompt":"hi"}}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert!(
        v["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("server-assigned"),
        "error message should say agent IDs are server-assigned: {v}"
    );
}

/// `initialAgent` without an `agentId` still passes through the guard.
#[tokio::test]
async fn workspace_create_with_initial_agent_sans_agent_id_is_accepted() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"WS","initialAgent":{"prompt":"hi"}}}"#,
    )
    .await
    .unwrap();
    assert!(
        v["result"]["workspace"].is_object(),
        "create without agentId succeeds: {v}"
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
async fn workspace_delete_returns_success_true() {
    let msg =
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.delete","params":{"workspaceId":"ws-1"}}"#;
    let v = call(msg).await.unwrap();
    assert_eq!(v["result"]["success"], serde_json::json!(true));
}

/// `undoDelayMs: 0` (and explicit `null`) keep the immediate-delete result
/// byte-identical (§5.1): bare `{ success: true }`, no `scheduled`/`deleteAt`.
#[tokio::test]
async fn workspace_delete_zero_or_null_undo_delay_is_immediate() {
    for params in [
        r#"{"workspaceId":"ws-1","undoDelayMs":0}"#,
        r#"{"workspaceId":"ws-1","undoDelayMs":null}"#,
    ] {
        let msg =
            format!(r#"{{"jsonrpc":"2.0","id":1,"method":"workspace.delete","params":{params}}}"#);
        let v = call(&msg).await.unwrap();
        assert_eq!(v["result"], serde_json::json!({ "success": true }), "{v}");
    }
}

/// `undoDelayMs > 0` schedules the grace window (§5.1): the result carries
/// `scheduled: true` plus the ISO `deleteAt` deadline from the API.
#[tokio::test]
async fn workspace_delete_with_undo_delay_returns_scheduled_shape() {
    let msg = r#"{"jsonrpc":"2.0","id":1,"method":"workspace.delete","params":{"workspaceId":"ws-1","undoDelayMs":15000}}"#;
    let v = call(msg).await.unwrap();
    assert_eq!(
        v["result"],
        serde_json::json!({
            "success": true,
            "scheduled": true,
            "deleteAt": "2026-01-01T00:00:15Z",
        }),
        "{v}"
    );
}

/// A non-integer `undoDelayMs` is `-32602` (negative numbers and strings both
/// fail `as_u64`).
#[tokio::test]
async fn workspace_delete_invalid_undo_delay_is_minus_32602() {
    for bad in [r#""soon""#, "-1", "1.5", "true"] {
        let msg = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"workspace.delete","params":{{"workspaceId":"ws-1","undoDelayMs":{bad}}}}}"#
        );
        let v = call(&msg).await.unwrap();
        assert_eq!(err_code(&v), -32602, "undoDelayMs={bad}: {v}");
    }
}

/// `workspace.cancelDelete` returns `{ cancelled: bool }` (§5.1): `true` when
/// a pending deletion was cancelled, `false` otherwise — a non-error,
/// race-safe outcome (never `-32602`/`-32603` for "nothing pending").
#[tokio::test]
async fn workspace_cancel_delete_returns_cancelled_flag() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.cancelDelete","params":{"workspaceId":"pending"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"], serde_json::json!({ "cancelled": true }), "{v}");

    let v = call(
        r#"{"jsonrpc":"2.0","id":2,"method":"workspace.cancelDelete","params":{"workspaceId":"ws-1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(
        v["result"],
        serde_json::json!({ "cancelled": false }),
        "{v}"
    );
}

/// `undoDelayMs: 0` (and explicit `null`) keep the immediate agent-delete
/// result byte-identical (§5.5): bare `{ success: true }`, no
/// `scheduled`/`deleteAt`.
#[tokio::test]
async fn agent_delete_zero_or_null_undo_delay_is_immediate() {
    for params in [
        r#"{"agentId":"agent-1","undoDelayMs":0}"#,
        r#"{"agentId":"agent-1","undoDelayMs":null}"#,
        r#"{"agentId":"agent-1"}"#,
    ] {
        let msg =
            format!(r#"{{"jsonrpc":"2.0","id":1,"method":"agent.delete","params":{params}}}"#);
        let v = call(&msg).await.unwrap();
        assert_eq!(v["result"], serde_json::json!({ "success": true }), "{v}");
    }
}

/// `undoDelayMs > 0` schedules the agent delete grace window (§5.5): the
/// result carries `scheduled: true` plus the ISO `deleteAt` deadline from
/// the API.
#[tokio::test]
async fn agent_delete_with_undo_delay_returns_scheduled_shape() {
    let msg = r#"{"jsonrpc":"2.0","id":1,"method":"agent.delete","params":{"agentId":"agent-1","undoDelayMs":15000}}"#;
    let v = call(msg).await.unwrap();
    assert_eq!(
        v["result"],
        serde_json::json!({
            "success": true,
            "scheduled": true,
            "deleteAt": "2026-01-01T00:00:15Z",
        }),
        "{v}"
    );
}

/// A non-integer `undoDelayMs` on `agent.delete` is `-32602` (negative
/// numbers and strings both fail `as_u64`).
#[tokio::test]
async fn agent_delete_invalid_undo_delay_is_minus_32602() {
    for bad in [r#""soon""#, "-1", "1.5", "true"] {
        let msg = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"agent.delete","params":{{"agentId":"agent-1","undoDelayMs":{bad}}}}}"#
        );
        let v = call(&msg).await.unwrap();
        assert_eq!(err_code(&v), -32602, "undoDelayMs={bad}: {v}");
    }
}

/// `agent.cancelDelete` returns `{ cancelled: bool }` (§5.5): `true` when a
/// pending deletion was cancelled, `false` otherwise — a non-error, race-safe
/// outcome (never `-32602`/`-32603` for "nothing pending"). `agentId` is
/// required.
#[tokio::test]
async fn agent_cancel_delete_returns_cancelled_flag() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"agent.cancelDelete","params":{"agentId":"agent-pending"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"], serde_json::json!({ "cancelled": true }), "{v}");

    let v = call(
        r#"{"jsonrpc":"2.0","id":2,"method":"agent.cancelDelete","params":{"agentId":"agent-1","workspaceId":"ws-1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(
        v["result"],
        serde_json::json!({ "cancelled": false }),
        "{v}"
    );

    let v = call(r#"{"jsonrpc":"2.0","id":3,"method":"agent.cancelDelete","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602, "{v}");
}

/// `workspace.archive` and `workspace.unarchive` return the updated
/// `workspace` record (§5.1) — a `{success:true}` shape would force a
/// follow-up `workspace.get` on the FE. The `archived` flag flips through
/// the wire result on each call.
#[tokio::test]
async fn workspace_archive_and_unarchive_return_workspace() {
    let archived = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.archive","params":{"workspaceId":"ws-1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(
        archived["result"]["workspace"]["id"],
        serde_json::json!("ws-1")
    );
    assert_eq!(
        archived["result"]["workspace"]["archived"],
        serde_json::json!(true)
    );
    assert_eq!(
        archived["result"]["workspace"]["status"],
        serde_json::json!("Archived")
    );
    // The `success` shape must not leak.
    assert!(archived["result"].get("success").is_none());

    let unarchived = call(
        r#"{"jsonrpc":"2.0","id":2,"method":"workspace.unarchive","params":{"workspaceId":"ws-1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(
        unarchived["result"]["workspace"]["id"],
        serde_json::json!("ws-1")
    );
    assert_eq!(
        unarchived["result"]["workspace"]["archived"],
        serde_json::json!(false)
    );
    assert_eq!(
        unarchived["result"]["workspace"]["status"],
        serde_json::json!("Active")
    );
    assert!(unarchived["result"].get("success").is_none());
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
        "workspace.cancelDelete",
        "workspace.archive",
        "workspace.unarchive",
        "workspace.dismissAttention",
        "workspace.markSeen",
        "workspace.duplicate",
        "workspace.restore",
        "workspace.cleanup",
    ] {
        let msg = format!(r#"{{"jsonrpc":"2.0","id":1,"method":"{method}","params":{{}}}}"#);
        let v = call(&msg).await.unwrap();
        assert_eq!(err_code(&v), -32602, "{method}");
    }
}

#[tokio::test]
async fn workspace_duplicate_returns_workspace_with_new_title() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.duplicate","params":{"workspaceId":"ws-1","newTitle":"My Copy"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(
        v["result"]["workspace"]["title"],
        serde_json::json!("My Copy")
    );
    assert_eq!(
        v["result"]["workspace"]["id"],
        serde_json::json!("ws-1-copy")
    );
}

#[tokio::test]
async fn workspace_duplicate_defaults_to_copy_suffix() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.duplicate","params":{"workspaceId":"ws-1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(
        v["result"]["workspace"]["title"],
        serde_json::json!("WS One (Copy)")
    );
}

#[tokio::test]
async fn workspace_duplicate_not_found_is_minus_32602() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.duplicate","params":{"workspaceId":"missing"}}"#,
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
async fn workspace_restore_returns_workspace() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.restore","params":{"workspaceId":"ws-1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["workspace"]["id"], serde_json::json!("ws-1"));
}

#[tokio::test]
async fn workspace_cleanup_returns_success_true() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.cleanup","params":{"workspaceId":"ws-1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["success"], serde_json::json!(true));
}

#[tokio::test]
async fn workspace_find_repositories_returns_repositories_array() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.findRepositories","params":{"directory":"/tmp/scan"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(
        v["result"]["repositories"],
        serde_json::json!(["/tmp/scan/repo-a", "/tmp/scan/repo-b"])
    );
}

#[tokio::test]
async fn workspace_find_repositories_missing_directory_is_minus_32602() {
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"workspace.findRepositories","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
}

#[tokio::test]
async fn workspace_initialize_repository_returns_success_true() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.initializeRepository","params":{"path":"/tmp/new-repo"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["success"], serde_json::json!(true));
}

#[tokio::test]
async fn workspace_initialize_repository_missing_path_is_minus_32602() {
    let v =
        call(r#"{"jsonrpc":"2.0","id":1,"method":"workspace.initializeRepository","params":{}}"#)
            .await
            .unwrap();
    assert_eq!(err_code(&v), -32602);
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

/// -32602 discriminator (monorepo#1320): a lookup of a nonexistent entity
/// carries `error.data.code = "not-found"`; the message is unchanged.
#[tokio::test]
async fn not_found_minus_32602_carries_not_found_discriminator() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"note.get","params":{"workspaceId":"ws-1","noteId":"missing"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(v["error"]["message"], serde_json::json!("Note not found"));
    assert_eq!(v["error"]["data"]["code"], serde_json::json!("not-found"));

    // `workspace_err` path: missing workspace on `workspace.get`.
    let v = call(
        r#"{"jsonrpc":"2.0","id":2,"method":"workspace.get","params":{"workspaceId":"missing"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(v["error"]["data"]["code"], serde_json::json!("not-found"));
}

/// -32602 discriminator (monorepo#1320): param validation carries
/// `error.data.code = "invalid-params"`; the message is unchanged.
#[tokio::test]
async fn invalid_params_minus_32602_carries_invalid_params_discriminator() {
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"workspace.get","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Missing required parameter: workspaceId")
    );
    assert_eq!(
        v["error"]["data"]["code"],
        serde_json::json!("invalid-params")
    );
}

/// The formerly collapsed `InvalidParams | NotFound` arms (e.g.
/// `specialist.get`) are split so each origin carries its own discriminator
/// (monorepo#1320).
#[tokio::test]
async fn specialist_get_splits_not_found_from_invalid_params() {
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"specialist.get","params":{"id":"nope"}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Specialist not found: nope")
    );
    assert_eq!(v["error"]["data"]["code"], serde_json::json!("not-found"));

    let v = call(r#"{"jsonrpc":"2.0","id":2,"method":"specialist.get","params":{"id":""}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Invalid specialist id")
    );
    assert_eq!(
        v["error"]["data"]["code"],
        serde_json::json!("invalid-params")
    );
}

#[tokio::test]
async fn note_create_wraps_note_with_title() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"note.create","params":{"workspaceId":"ws-1","title":"Hi"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["note"]["title"], serde_json::json!("Hi"));
    // Additive conversion fields over the old `{note}` shape.
    assert_eq!(v["result"]["convertedCount"], serde_json::json!(0));
    assert_eq!(v["result"]["createdTaskNoteIds"], serde_json::json!([]));
    assert_eq!(v["result"]["createdTasks"], serde_json::json!([]));
    assert_eq!(v["result"]["warnings"], serde_json::json!([]));
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

/// Round 14 root cause A: the `comment.add` arm forwards `params.commentId`
/// to the service (the fake echoes it back as the comment id) so a
/// client-supplied id survives the router boundary.
#[tokio::test]
async fn comment_add_forwards_comment_id() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"comment.add","params":{"workspaceId":"ws-1","noteId":"n1","searchContext":"a test sentence","commentTarget":"test","comment":"nice","commentId":"0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(
        v["result"]["commentId"],
        serde_json::json!("0f1e2d3c-4b5a-6978-8796-a5b4c3d2e1f0")
    );

    // Absent commentId falls through to the fake's default, pinning that the
    // arm passes None rather than fabricating a value.
    let v = call(
        r#"{"jsonrpc":"2.0","id":2,"method":"comment.add","params":{"workspaceId":"ws-1","noteId":"n1","searchContext":"a test sentence","commentTarget":"test","comment":"nice"}}"#,
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
    // Reply-anchoring contract (monorepo#729): replies carry no anchor keys.
    assert!(v["result"]["comment"].get("anchor").is_none());
    assert!(v["result"]["comment"].get("anchorText").is_none());
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

    // query passes EventQueryParams (eventType echoed into the result event).
    let v = call(
        r#"{"jsonrpc":"2.0","id":5,"method":"event.query","params":{"workspaceId":"ws-1","eventType":"file:changed"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"][0]["type"], serde_json::json!("file:changed"));
}

/// `event.recentFiles` / `event.directoryChanges` were removed end-to-end: the
/// router arms and catalog entries are gone, so the dispatcher answers `-32601`.
#[tokio::test]
async fn removed_event_file_methods_are_not_routable() {
    for frame in [
        r#"{"jsonrpc":"2.0","id":1,"method":"event.recentFiles","params":{"workspaceId":"ws-1","limit":7}}"#,
        r#"{"jsonrpc":"2.0","id":2,"method":"event.directoryChanges","params":{"workspaceId":"ws-1","dir":"src/"}}"#,
    ] {
        let v = call(frame).await.unwrap();
        assert_eq!(err_code(&v), -32601);
        assert_eq!(v["error"]["message"], serde_json::json!("Method not found"));
    }
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

    // agent.list with the contradictory includeRetired + retiredOnly pair.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"agent.list","params":{"workspaceId":"ws-1","includeRetired":true,"retiredOnly":true}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("includeRetired and retiredOnly are mutually exclusive")
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

    // agent.listActive is daemon-global and accepts an empty params object.
    let v = call(r#"{"jsonrpc":"2.0","id":7,"method":"agent.listActive","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(
        v["result"],
        serde_json::json!({
            "streams": [{
                "agentId": "agent-active",
                "sessionId": "agent-active",
                "workspaceId": "ws-active",
                "startTime": 1_750_000_000_000_i64,
            }],
        })
    );

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

    // models.list with the optional providerId/forceRefresh params routes too.
    let v = call(
        r#"{"jsonrpc":"2.0","id":8,"method":"models.list","params":{"providerId":"auggie","forceRefresh":true}}"#,
    )
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

    // agent.completeOnce also takes the optional quick-action `type` hint
    // (monorepo#1734) — free-form, so it must route like any other param.
    let v = call(
        r#"{"jsonrpc":"2.0","id":10,"method":"agent.completeOnce","params":{"prompt":"pick a slug","type":"commit"}}"#,
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
async fn git_status_force_refresh_is_forwarded() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"git.status","params":{"workspaceId":"ws-1","forceRefresh":true}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["branch"], serde_json::json!("forced"));
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
async fn git_status_with_git_root_id_scopes_to_root() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"git.status","params":{"workspaceId":"ws-1","gitRootId":"root-1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["branch"], serde_json::json!("root-branch"));
}

#[tokio::test]
async fn git_status_unknown_git_root_id_is_minus_32602() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"git.status","params":{"workspaceId":"ws-1","gitRootId":"nope"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("invalid params: Unknown git root: nope")
    );
}

#[tokio::test]
async fn git_root_list_returns_git_roots_envelope() {
    let v =
        call(r#"{"jsonrpc":"2.0","id":1,"method":"gitRoot.list","params":{"workspaceId":"ws-1"}}"#)
            .await
            .unwrap();
    let roots = v["result"]["gitRoots"].as_array().unwrap();
    assert_eq!(roots.len(), 1);
    assert_eq!(roots[0]["id"], serde_json::json!("root-1"));
    assert_eq!(roots[0]["branch"], serde_json::json!("feature"));
}

#[tokio::test]
async fn git_root_list_missing_workspace_id_is_minus_32602() {
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"gitRoot.list","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("workspaceId is required")
    );
}

#[tokio::test]
async fn git_root_list_unknown_workspace_is_minus_32602() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"gitRoot.list","params":{"workspaceId":"missing"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
}

#[tokio::test]
async fn git_branch_status_git_root_id_resolves_repo_path() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"git.branchStatus","params":{"workspaceId":"ws-1","gitRootId":"root-1","branchName":"feature"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["branch"], serde_json::json!("feature"));
    assert_eq!(v["result"]["isCurrentBranch"], serde_json::json!(true));
}

#[tokio::test]
async fn git_branch_status_unknown_git_root_id_is_minus_32602() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"git.branchStatus","params":{"workspaceId":"ws-1","gitRootId":"nope","branchName":"feature"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    // Identical message to the other five gitRootId-scoped reads (§5.6):
    // the domain error maps through `domain_to_rpc`, which prefixes
    // `invalid params:` exactly like the `git.status` arm above.
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("invalid params: Unknown git root: nope")
    );
}

#[tokio::test]
async fn git_status_empty_git_root_id_is_treated_as_absent() {
    // §5.6: an empty/whitespace-only `gitRootId` reads as absent — the
    // primary-worktree behavior, not an unknown-root error.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"git.status","params":{"workspaceId":"ws-1","gitRootId":""}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["branch"], serde_json::json!("main"));
}

#[tokio::test]
async fn git_commit_details_with_git_root_id_scopes_to_root() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"git.commitDetails","params":{"workspaceId":"ws-1","commitHash":"abc123","gitRootId":"root-1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["commitHash"], serde_json::json!("abc123"));
    assert_eq!(v["result"]["message"], serde_json::json!("root-commit"));
    assert_eq!(v["result"]["files"], serde_json::json!(["root-only.txt"]));
}

#[tokio::test]
async fn git_commit_details_without_git_root_id_targets_primary() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"git.commitDetails","params":{"workspaceId":"ws-1","commitHash":"abc123"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["message"], serde_json::json!("primary-commit"));
}

#[tokio::test]
async fn git_commit_details_unknown_git_root_id_is_minus_32602() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"git.commitDetails","params":{"workspaceId":"ws-1","commitHash":"abc123","gitRootId":"nope"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("invalid params: Unknown git root: nope")
    );
}

#[tokio::test]
async fn git_commit_details_empty_git_root_id_is_treated_as_absent() {
    // §5.6: an empty/whitespace-only `gitRootId` reads as absent — the
    // primary-worktree behavior, not an unknown-root error.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"git.commitDetails","params":{"workspaceId":"ws-1","commitHash":"abc123","gitRootId":"  "}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["message"], serde_json::json!("primary-commit"));
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
    assert_eq!(repo["owner"], serde_json::json!("intent-hq"));
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

// ---- repo.warmCache (PROTOCOL §5.6) ------------------------------------

#[tokio::test]
async fn repo_warm_cache_routes_url_and_returns_started_shape() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"repo.warmCache","params":{"githubUrl":"https://github.com/intent-hq/intentd"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(
        v["result"],
        serde_json::json!({ "started": true, "owner": "intent-hq", "repo": "intentd" })
    );
}

#[tokio::test]
async fn repo_warm_cache_busy_maps_to_warm_in_flight_data() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"repo.warmCache","params":{"githubUrl":"https://github.com/intent-hq/busy"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32603);
    assert_eq!(
        v["error"]["data"],
        serde_json::json!({ "code": "warm-in-flight", "owner": "intent-hq", "repo": "other" })
    );
}

#[tokio::test]
async fn repo_warm_cache_requires_github_url_param() {
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"repo.warmCache","params":{}}"#)
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
async fn github_repo_config_get_requires_owner_and_repo_and_routes_ref() {
    let missing_repo =
        call(r#"{"jsonrpc":"2.0","id":1,"method":"github.repoConfig.get","params":{"owner":"o"}}"#)
            .await
            .unwrap();
    assert_eq!(err_code(&missing_repo), -32602);

    // `ref` is optional and omitted → forwarded as null.
    let ok = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"github.repoConfig.get","params":{"owner":"o","repo":"r"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(
        ok["result"]["config"]["branchPrefix"],
        serde_json::json!("o/r")
    );
    assert_eq!(ok["result"]["echoRef"], Value::Null);

    let with_ref = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"github.repoConfig.get","params":{"owner":"o","repo":"r","ref":"dev"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(with_ref["result"]["echoRef"], serde_json::json!("dev"));
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
async fn github_branches_list_threads_optional_prefix() {
    // Absent `prefix` reaches the API as `None` (old behavior preserved).
    let ok = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"github.branches.list","params":{"owner":"o","repo":"r"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(ok["result"]["echoPrefix"], Value::Null);

    // A wire `prefix` string is threaded through verbatim.
    let ok = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"github.branches.list","params":{"owner":"o","repo":"r","prefix":"feature/"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(ok["result"]["echoPrefix"], serde_json::json!("feature/"));
    assert_eq!(ok["result"]["branches"], serde_json::json!(["o", "r"]));
}

#[tokio::test]
async fn github_branches_list_cached_requires_owner_and_repo() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"github.branches.listCached","params":{"owner":"o"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);

    let ok = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"github.branches.listCached","params":{"owner":"o","repo":"r"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(ok["result"]["cached"], serde_json::json!(true));
    assert_eq!(ok["result"]["branches"], serde_json::json!(["o", "r"]));
    assert_eq!(ok["result"]["defaultBranch"], serde_json::json!("main"));
}

#[tokio::test]
async fn github_auth_status_connect_revoke_get_user_route_without_params() {
    let auth = call(r#"{"jsonrpc":"2.0","id":1,"method":"github.authStatus","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(auth["result"]["isConfigured"], serde_json::json!(true));
    assert_eq!(auth["result"]["deviceFlow"], Value::Null);

    let connect = call(r#"{"jsonrpc":"2.0","id":1,"method":"github.connect","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(connect["result"]["ok"], serde_json::json!(true));
    assert_eq!(
        connect["result"]["userCode"],
        serde_json::json!("ABCD-1234")
    );
    assert_eq!(
        connect["result"]["verificationUri"],
        serde_json::json!("https://github.com/login/device")
    );

    let cancel = call(r#"{"jsonrpc":"2.0","id":1,"method":"github.cancelAuth","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(cancel["result"]["ok"], serde_json::json!(true));
    assert_eq!(cancel["result"]["cancelled"], serde_json::json!(true));

    let revoke = call(r#"{"jsonrpc":"2.0","id":1,"method":"github.revoke","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(revoke["result"]["ok"], serde_json::json!(true));

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
async fn git_agent_commit_with_git_root_id_targets_root() {
    // §5.6 extension (monorepo#2053 follow-up): `gitRootId` targets the
    // commit at the registered root instead of the workspace worktree.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"git.agentCommit","params":{"workspaceId":"ws-1","message":"msg","files":["a.ts"],"gitRootId":"root-1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["ok"], serde_json::json!(true));
    assert_eq!(v["result"]["hash"], serde_json::json!("root-def456"));
    assert_eq!(v["result"]["files"], serde_json::json!(["a.ts"]));
    assert_eq!(v["result"]["fileCount"], serde_json::json!(1));
}

#[tokio::test]
async fn git_agent_commit_unknown_git_root_id_is_minus_32602() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"git.agentCommit","params":{"workspaceId":"ws-1","message":"msg","gitRootId":"nope"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    // Identical message to the six gitRootId-scoped reads (§5.6): the
    // domain error maps through `domain_to_rpc`, which prefixes
    // `invalid params:` exactly like the `git.status` arm above.
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("invalid params: Unknown git root: nope")
    );
}

#[tokio::test]
async fn git_agent_commit_empty_git_root_id_is_treated_as_absent() {
    // §5.6: an empty/whitespace-only `gitRootId` reads as absent — the
    // primary-worktree behavior, not an unknown-root error.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"git.agentCommit","params":{"workspaceId":"ws-1","message":"msg","files":["a.ts"],"gitRootId":"  "}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["ok"], serde_json::json!(true));
    assert_eq!(v["result"]["hash"], serde_json::json!("def456"));
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
        "file-tracking.getChanges",
        "file-tracking.loadCommits",
        "file-tracking.getLineStats",
        "file-tracking.getAgentLocks",
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
    for method in [
        "file-tracking.getChanges",
        "file-tracking.getLineStats",
        "file-tracking.getAgentLocks",
    ] {
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
        ("metrics.getAllWorkspaceStats", r"{}"),
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
async fn search_messages_requires_query_only() {
    // workspaceId is optional (absent → global search); missing query → -32602.
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"search.messages","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Missing required parameter: query")
    );
    // Global (no workspaceId) routes + echoes the caller's requestId.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"search.messages","params":{"query":"x","requestId":"srch-9"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["requestId"], serde_json::json!("srch-9"));
    assert_eq!(v["result"]["workspaceId"], serde_json::json!(null));
    // workspaceId and preferWorkspaceId both plumb through to the API.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"search.messages","params":{"workspaceId":"ws-1","preferWorkspaceId":"ws-2","query":"x"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["workspaceId"], serde_json::json!("ws-1"));
    assert_eq!(v["result"]["preferWorkspaceId"], serde_json::json!("ws-2"));
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

    // file.readChunk → { content, bytesRead, size } with offset/length routed.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"file.readChunk","params":{"workspaceId":"ws-1","path":"a.bin","offset":64,"length":32}}"#,
    )
    .await
    .unwrap();
    assert_eq!(
        v["result"],
        serde_json::json!({ "content": "b64:a.bin:64:32", "bytesRead": 32, "size": 1000u64 })
    );

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

    // file.readChunk missing offset / length → -32602.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"file.readChunk","params":{"workspaceId":"ws-1","path":"a","length":16}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"file.readChunk","params":{"workspaceId":"ws-1","path":"a","offset":0}}"#,
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
    assert!(out.starts_with('['), "header line present: {out:?}");
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

/// FIX 1 parity: `agent.sendMessage` must forward the FE-side per-turn
/// prompt-assembly hints (`noteIds`, `stdinContext`, `contextReferences`)
/// verbatim to the [`WorkspaceApi`] call — the daemon previously dropped
/// them (see FE audit). Also covers `agent.sendQueuedMessageNow`'s param
/// forwarding (the queued entry carries its own payload, so only the ids
/// cross the wire).
mod send_message_payload_forwarding {
    use std::sync::{Arc, Mutex};

    use intent_core::{AgentId, BoxFuture, Result, WorkspaceApi, WorkspaceId};
    use serde_json::{json, Value};

    use super::super::handle_message;

    /// Recorded snapshot of a single `agent_send_message` /
    /// `agent_send_queued_message_now` call. Only the fields the FIX widens
    /// are asserted; the rest are captured so the tests document the full
    /// observed shape.
    #[derive(Default, Debug, Clone)]
    // Unasserted fields are written but never read; kept to document the shape.
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
        origin: Option<intent_core::MessageOrigin>,
    }

    #[derive(Default)]
    struct RecordingApi {
        send: Arc<Mutex<Capture>>,
        send_now: Arc<Mutex<Capture>>,
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
            origin: intent_core::MessageOrigin,
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
                    origin: Some(origin),
                };
                Ok(json!({ "success": true, "queued": false, "messageId": "m-1" }))
            })
        }

        fn agent_send_queued_message_now(
            &self,
            workspace_id: WorkspaceId,
            agent_id: AgentId,
            message_id: String,
        ) -> BoxFuture<'_, Result<Value>> {
            let slot = self.send_now.clone();
            Box::pin(async move {
                *slot.lock().unwrap() = Capture {
                    workspace_id: Some(workspace_id),
                    agent_id: Some(agent_id),
                    message_id: Some(message_id),
                    ..Capture::default()
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
        assert_eq!(
            cap.workspace_id
                .as_ref()
                .map(intent_core::WorkspaceId::as_str),
            Some("ws-1")
        );
        assert_eq!(
            cap.agent_id.as_ref().map(intent_core::AgentId::as_str),
            Some("agent-1")
        );
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
    async fn send_queued_message_now_forwards_ids_verbatim() {
        let api = RecordingApi::default();
        let msg = r#"{
            "jsonrpc":"2.0","id":3,"method":"agent.sendQueuedMessageNow",
            "params":{
                "workspaceId":"ws-1",
                "agentId":"agent-1",
                "messageId":"user-msg-queued"
            }
        }"#;
        let out = handle_message(&api, msg).await.expect("response");
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["result"]["success"], Value::Bool(true));
        let cap = api.send_now.lock().unwrap().clone();
        assert_eq!(
            cap.workspace_id
                .as_ref()
                .map(intent_core::WorkspaceId::as_str),
            Some("ws-1")
        );
        assert_eq!(
            cap.agent_id.as_ref().map(intent_core::AgentId::as_str),
            Some("agent-1")
        );
        assert_eq!(cap.message_id.as_deref(), Some("user-msg-queued"));
    }

    /// `agent.sendQueuedMessageNow` requires `messageId` (the queued entry
    /// to dequeue) — missing params are `-32602` before any API call.
    #[tokio::test]
    async fn send_queued_message_now_missing_message_id_is_invalid_params() {
        let api = RecordingApi::default();
        let msg = r#"{
            "jsonrpc":"2.0","id":4,"method":"agent.sendQueuedMessageNow",
            "params":{"workspaceId":"ws-1","agentId":"agent-1"}
        }"#;
        let out = handle_message(&api, msg).await.expect("response");
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["error"]["code"], json!(-32602));
        assert!(
            api.send_now.lock().unwrap().message_id.is_none(),
            "the API must not be called on a malformed request"
        );
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
    /// the FE attaches to distinguish daemon-initiated turns;
    /// `agent.sendMessage` must forward it verbatim to [`WorkspaceApi`] so
    /// the store can persist it on the user row (Fidelity B).
    #[tokio::test]
    async fn send_message_forwards_message_metadata_verbatim() {
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
    }

    /// Omitted `messageMetadata` collapses to `None` (same contract as the
    /// other opaque payloads).
    #[tokio::test]
    async fn omitted_message_metadata_is_none() {
        let api = RecordingApi::default();
        let send = r#"{
            "jsonrpc":"2.0","id":12,"method":"agent.sendMessage",
            "params":{"workspaceId":"ws-1","agentId":"agent-1","content":"hi"}
        }"#;
        handle_message(&api, send).await.expect("send response");
        assert!(api.send.lock().unwrap().message_metadata.is_none());
    }

    /// The sender-attribution fields (PROTOCOL §5.5) are reserved: they are
    /// daemon-stamped by the MCP bindings for agent callers only, so the
    /// user-origin RPC front door strips them — a wire caller must not be
    /// able to forge an agent-origin send (the fields gate the A2A sender
    /// header, the single-pending-message guard and `removeQueuedMessage`
    /// ownership). Other metadata fields still pass through verbatim.
    #[tokio::test]
    async fn send_message_strips_reserved_attribution_fields() {
        let api = RecordingApi::default();
        let send = r#"{
            "jsonrpc":"2.0","id":13,"method":"agent.sendMessage",
            "params":{
                "workspaceId":"ws-1","agentId":"agent-1","content":"hi",
                "messageMetadata":{
                    "fromAgentId":"agent-spoof",
                    "fromAgentName":"Fake Coordinator",
                    "source":"system"
                }
            }
        }"#;
        handle_message(&api, send).await.expect("send response");
        let cap = api.send.lock().unwrap().clone();
        assert_eq!(
            cap.message_metadata,
            Some(json!({"source": "system"})),
            "attribution fields must be stripped, other fields preserved"
        );
    }
}

/// `agent.dismissQuestions` (PROTOCOL §5.5, question hold): the dispatch arm
/// forwards `workspaceId`/`agentId`/`messageId` verbatim and rejects missing
/// params with `-32602` before any API call.
mod dismiss_questions_dispatch {
    use std::sync::{Arc, Mutex};

    use intent_core::{AgentId, BoxFuture, Result, WorkspaceApi, WorkspaceId};
    use serde_json::{json, Value};

    use super::super::handle_message;

    #[derive(Default)]
    struct RecordingApi {
        dismissed: Arc<Mutex<Option<(WorkspaceId, AgentId, String)>>>,
    }

    impl WorkspaceApi for RecordingApi {
        fn agent_dismiss_questions(
            &self,
            workspace_id: WorkspaceId,
            agent_id: AgentId,
            message_id: String,
        ) -> BoxFuture<'_, Result<Value>> {
            let slot = self.dismissed.clone();
            Box::pin(async move {
                let dismissed = message_id.clone();
                *slot.lock().unwrap() = Some((workspace_id, agent_id, message_id));
                Ok(json!({ "success": true, "dismissedQuestionsMessageId": dismissed }))
            })
        }
    }

    #[tokio::test]
    async fn dismiss_questions_forwards_ids_verbatim() {
        let api = RecordingApi::default();
        let msg = r#"{
            "jsonrpc":"2.0","id":1,"method":"agent.dismissQuestions",
            "params":{"workspaceId":"ws-1","agentId":"agent-1","messageId":"msg-q1"}
        }"#;
        let out = handle_message(&api, msg).await.expect("response");
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["result"]["success"], Value::Bool(true));
        assert_eq!(v["result"]["dismissedQuestionsMessageId"], "msg-q1");
        let cap = api.dismissed.lock().unwrap().clone().expect("captured");
        assert_eq!(cap.0.as_str(), "ws-1");
        assert_eq!(cap.1.as_str(), "agent-1");
        assert_eq!(cap.2, "msg-q1");
    }

    #[tokio::test]
    async fn dismiss_questions_missing_params_are_invalid_params() {
        let api = RecordingApi::default();
        for params in [
            r#"{"agentId":"agent-1","messageId":"msg-q1"}"#,
            r#"{"workspaceId":"ws-1","messageId":"msg-q1"}"#,
            r#"{"workspaceId":"ws-1","agentId":"agent-1"}"#,
        ] {
            let msg = format!(
                r#"{{"jsonrpc":"2.0","id":2,"method":"agent.dismissQuestions","params":{params}}}"#
            );
            let out = handle_message(&api, &msg).await.expect("response");
            let v: Value = serde_json::from_str(&out).unwrap();
            assert_eq!(v["error"]["code"], json!(-32602), "params: {params}");
        }
        assert!(
            api.dismissed.lock().unwrap().is_none(),
            "the API must not be called on a malformed request"
        );
    }
}

/// `agent.markSeen` (PROTOCOL §5.5, seen marker): the dispatch arm forwards
/// `workspaceId`/`agentId`/`messageId` verbatim and rejects missing params
/// with `-32602` before any API call.
mod mark_seen_dispatch {
    use std::sync::{Arc, Mutex};

    use intent_core::{AgentId, BoxFuture, Result, WorkspaceApi, WorkspaceId};
    use serde_json::{json, Value};

    use super::super::handle_message;

    #[derive(Default)]
    struct RecordingApi {
        seen: Arc<Mutex<Option<(WorkspaceId, AgentId, String)>>>,
    }

    impl WorkspaceApi for RecordingApi {
        fn agent_mark_seen(
            &self,
            workspace_id: WorkspaceId,
            agent_id: AgentId,
            message_id: String,
        ) -> BoxFuture<'_, Result<Value>> {
            let slot = self.seen.clone();
            Box::pin(async move {
                let seen = message_id.clone();
                *slot.lock().unwrap() = Some((workspace_id, agent_id, message_id));
                Ok(json!({ "success": true, "lastSeenMessageId": seen }))
            })
        }
    }

    #[tokio::test]
    async fn mark_seen_forwards_ids_verbatim() {
        let api = RecordingApi::default();
        let msg = r#"{
            "jsonrpc":"2.0","id":1,"method":"agent.markSeen",
            "params":{"workspaceId":"ws-1","agentId":"agent-1","messageId":"msg-7"}
        }"#;
        let out = handle_message(&api, msg).await.expect("response");
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["result"]["success"], Value::Bool(true));
        assert_eq!(v["result"]["lastSeenMessageId"], "msg-7");
        let cap = api.seen.lock().unwrap().clone().expect("captured");
        assert_eq!(cap.0.as_str(), "ws-1");
        assert_eq!(cap.1.as_str(), "agent-1");
        assert_eq!(cap.2, "msg-7");
    }

    #[tokio::test]
    async fn mark_seen_missing_params_are_invalid_params() {
        let api = RecordingApi::default();
        for params in [
            r#"{"agentId":"agent-1","messageId":"msg-7"}"#,
            r#"{"workspaceId":"ws-1","messageId":"msg-7"}"#,
            r#"{"workspaceId":"ws-1","agentId":"agent-1"}"#,
        ] {
            let msg = format!(
                r#"{{"jsonrpc":"2.0","id":2,"method":"agent.markSeen","params":{params}}}"#
            );
            let out = handle_message(&api, &msg).await.expect("response");
            let v: Value = serde_json::from_str(&out).unwrap();
            assert_eq!(v["error"]["code"], json!(-32602), "params: {params}");
        }
        assert!(
            api.seen.lock().unwrap().is_none(),
            "the API must not be called on a malformed request"
        );
    }
}

/// `repoConfig.*` namespace tests (additive intentd-only surface, FE parity
/// with `packages/cloudlands-fe/src/features/workspace/main/repo-config.ipc.ts`).
#[tokio::test]
async fn repo_config_get_happy_path() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"repoConfig.get","params":{"workspaceId":"ws-1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["config"]["branchPrefix"], "feature/");
}

#[tokio::test]
async fn repo_config_get_unknown_workspace() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"repoConfig.get","params":{"workspaceId":"missing"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(v["error"]["message"], "Workspace not found");
}

#[tokio::test]
async fn repo_config_get_missing_workspace_id() {
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"repoConfig.get","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        "Missing required parameter: workspaceId"
    );
}

#[tokio::test]
async fn repo_config_save_happy_path() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"repoConfig.save","params":{"workspaceId":"ws-1","config":{"branchPrefix":"feat/"}}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["config"]["branchPrefix"], "feat/");
}

#[tokio::test]
async fn repo_config_save_unknown_workspace() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"repoConfig.save","params":{"workspaceId":"missing","config":{}}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(v["error"]["message"], "Workspace not found");
}

#[tokio::test]
async fn repo_config_save_missing_config() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"repoConfig.save","params":{"workspaceId":"ws-1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(v["error"]["message"], "Missing required parameter: config");
}

#[tokio::test]
async fn repo_config_save_invalid_config() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"repoConfig.save","params":{"workspaceId":"ws-1","config":"not-an-object"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert!(v["error"]["message"]
        .as_str()
        .unwrap()
        .contains("invalid config"));
}

#[tokio::test]
async fn repo_config_save_invalid_field_type() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"repoConfig.save","params":{"workspaceId":"ws-1","config":{"branchPrefix":42}}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert!(v["error"]["message"]
        .as_str()
        .unwrap()
        .contains("invalid config"));
}

#[tokio::test]
async fn repo_config_save_null_field_accepted() {
    // Explicit `null` is the "clear this field" signal — it must pass router
    // validation and reach the API as part of the raw patch.
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"repoConfig.save","params":{"workspaceId":"ws-1","config":{"branchPrefix":"x/","setupScript":null}}}"#,
    )
    .await
    .unwrap();
    assert!(v.get("error").is_none(), "null field should be accepted");
    assert_eq!(v["result"]["config"]["branchPrefix"], "x/");
}

#[tokio::test]
async fn repo_config_save_unknown_keys_forwarded() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"repoConfig.save","params":{"workspaceId":"ws-1","config":{"customKey":"customValue"}}}"#,
    )
    .await
    .unwrap();
    assert!(v.get("error").is_none(), "unknown keys should be accepted");
    assert_eq!(v["result"]["config"]["customKey"], "customValue");
}

#[tokio::test]
async fn repo_config_has_happy_path() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"repoConfig.has","params":{"workspaceId":"with-config"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["exists"], true);

    let v = call(
        r#"{"jsonrpc":"2.0","id":2,"method":"repoConfig.has","params":{"workspaceId":"ws-1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["exists"], false);
}

#[tokio::test]
async fn repo_config_has_unknown_workspace() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"repoConfig.has","params":{"workspaceId":"missing"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(v["error"]["message"], "Workspace not found");
}

#[tokio::test]
async fn repo_config_ensure_dir_happy_path() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"repoConfig.ensureDir","params":{"workspaceId":"ws-1"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(v["result"]["ok"], true);
}

#[tokio::test]
async fn repo_config_ensure_dir_unknown_workspace() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"repoConfig.ensureDir","params":{"workspaceId":"missing"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(v["error"]["message"], "Workspace not found");
}

// ---------------------------------------------------------------------------
// pr.refresh (PROTOCOL §5.7 extension)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn pr_refresh_dispatches_and_returns_service_result() {
    let v =
        call(r#"{"jsonrpc":"2.0","id":1,"method":"pr.refresh","params":{"workspaceId":"ws-1"}}"#)
            .await
            .unwrap();
    assert!(v.get("error").is_none(), "unexpected error: {v}");
    assert_eq!(v["result"]["outcome"], "linked");
    assert_eq!(v["result"]["prNumber"], 300);
    assert_eq!(v["result"]["prUrl"], "https://github.com/o/r/pull/300");
    assert_eq!(v["result"]["prStatus"], "Open");
    assert_eq!(v["result"]["pullRequests"][0]["number"], 300);
}

#[tokio::test]
async fn pr_refresh_missing_workspace_id_is_minus_32602() {
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"pr.refresh","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(v["error"]["message"], "workspaceId is required");
}

#[tokio::test]
async fn pr_refresh_unknown_workspace_is_minus_32602() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"pr.refresh","params":{"workspaceId":"missing"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(v["error"]["message"], "Workspace not found");
}

// ---------------------------------------------------------------------------
// agent.editAndRegenerate (PROTOCOL §5.5 extension)
// ---------------------------------------------------------------------------

mod edit_and_regenerate {
    use std::sync::{Arc, Mutex};

    use intent_core::{AgentId, BoxFuture, Result, WorkspaceApi, WorkspaceId};
    use serde_json::{json, Value};

    use super::super::handle_message;
    use super::err_code;

    /// Recorded snapshot of a single `agent_edit_and_regenerate` call.
    #[derive(Default, Debug, Clone)]
    struct Capture {
        workspace_id: Option<WorkspaceId>,
        agent_id: Option<AgentId>,
        message_id: Option<String>,
        content: Option<String>,
        image_blocks: Option<Value>,
        file_blocks: Option<Value>,
        model: Option<String>,
    }

    #[derive(Default)]
    struct RecordingApi {
        edit: Arc<Mutex<Capture>>,
    }

    impl WorkspaceApi for RecordingApi {
        #[allow(clippy::too_many_arguments)]
        fn agent_edit_and_regenerate(
            &self,
            workspace_id: WorkspaceId,
            agent_id: AgentId,
            message_id: String,
            content: String,
            image_blocks: Option<Value>,
            file_blocks: Option<Value>,
            model: Option<String>,
        ) -> BoxFuture<'_, Result<Value>> {
            let slot = self.edit.clone();
            Box::pin(async move {
                *slot.lock().unwrap() = Capture {
                    workspace_id: Some(workspace_id),
                    agent_id: Some(agent_id),
                    message_id: Some(message_id),
                    content: Some(content),
                    image_blocks,
                    file_blocks,
                    model,
                };
                Ok(json!({
                    "success": true,
                    "queued": false,
                    "messageId": "m-new",
                    "truncatedCount": 3,
                }))
            })
        }
    }

    #[tokio::test]
    async fn dispatches_and_forwards_all_params() {
        let api = RecordingApi::default();
        let msg = r#"{
            "jsonrpc":"2.0","id":1,"method":"agent.editAndRegenerate",
            "params":{
                "workspaceId":"ws-1",
                "agentId":"agent-1",
                "messageId":"msg-7",
                "content":"edited text",
                "imageBlocks":[{"data":"aGk=","mimeType":"image/png"}],
                "fileBlocks":[{"data":"aGk=","mimeType":"text/plain","fileName":"a.txt"}],
                "model":"auggie:sonnet4.5"
            }
        }"#;
        let out = handle_message(&api, msg).await.expect("response");
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["result"]["success"], Value::Bool(true));
        assert_eq!(v["result"]["truncatedCount"], json!(3));

        let cap = api.edit.lock().unwrap().clone();
        assert_eq!(
            cap.workspace_id
                .as_ref()
                .map(intent_core::WorkspaceId::as_str),
            Some("ws-1")
        );
        assert_eq!(
            cap.agent_id.as_ref().map(intent_core::AgentId::as_str),
            Some("agent-1")
        );
        assert_eq!(cap.message_id.as_deref(), Some("msg-7"));
        assert_eq!(cap.content.as_deref(), Some("edited text"));
        assert_eq!(
            cap.image_blocks,
            Some(json!([{"data":"aGk=","mimeType":"image/png"}]))
        );
        assert_eq!(
            cap.file_blocks,
            Some(json!([{"data":"aGk=","mimeType":"text/plain","fileName":"a.txt"}]))
        );
        assert_eq!(cap.model.as_deref(), Some("auggie:sonnet4.5"));
    }

    #[tokio::test]
    async fn optional_params_are_none_when_omitted() {
        let api = RecordingApi::default();
        let msg = r#"{
            "jsonrpc":"2.0","id":2,"method":"agent.editAndRegenerate",
            "params":{"workspaceId":"ws-1","agentId":"agent-1","messageId":"msg-7","content":"edited"}
        }"#;
        handle_message(&api, msg).await.expect("response");
        let cap = api.edit.lock().unwrap().clone();
        assert!(cap.image_blocks.is_none());
        assert!(cap.file_blocks.is_none());
        assert!(cap.model.is_none());
    }

    #[tokio::test]
    async fn missing_message_id_is_minus_32602() {
        let api = RecordingApi::default();
        let msg = r#"{
            "jsonrpc":"2.0","id":3,"method":"agent.editAndRegenerate",
            "params":{"workspaceId":"ws-1","agentId":"agent-1","content":"edited"}
        }"#;
        let out = handle_message(&api, msg).await.expect("response");
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(err_code(&v), -32602);
    }

    #[tokio::test]
    async fn missing_content_is_minus_32602() {
        let api = RecordingApi::default();
        let msg = r#"{
            "jsonrpc":"2.0","id":4,"method":"agent.editAndRegenerate",
            "params":{"workspaceId":"ws-1","agentId":"agent-1","messageId":"msg-7"}
        }"#;
        let out = handle_message(&api, msg).await.expect("response");
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(err_code(&v), -32602);
    }

    #[tokio::test]
    async fn missing_workspace_id_is_minus_32602() {
        let api = RecordingApi::default();
        let msg = r#"{
            "jsonrpc":"2.0","id":5,"method":"agent.editAndRegenerate",
            "params":{"agentId":"agent-1","messageId":"msg-7","content":"edited"}
        }"#;
        let out = handle_message(&api, msg).await.expect("response");
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(err_code(&v), -32602);
    }

    /// Domain `InvalidParams` from the service (unknown / non-user messageId)
    /// surfaces as `-32602` through `domain_to_rpc`.
    struct RejectingApi;

    impl WorkspaceApi for RejectingApi {
        #[allow(clippy::too_many_arguments)]
        fn agent_edit_and_regenerate(
            &self,
            _workspace_id: WorkspaceId,
            _agent_id: AgentId,
            message_id: String,
            _content: String,
            _image_blocks: Option<Value>,
            _file_blocks: Option<Value>,
            _model: Option<String>,
        ) -> BoxFuture<'_, Result<Value>> {
            Box::pin(async move {
                Err(intent_core::Error::InvalidParams(format!(
                    "agent.editAndRegenerate: messageId {message_id} not found in transcript"
                )))
            })
        }
    }

    #[tokio::test]
    async fn service_invalid_params_maps_to_minus_32602() {
        let msg = r#"{
            "jsonrpc":"2.0","id":6,"method":"agent.editAndRegenerate",
            "params":{"workspaceId":"ws-1","agentId":"agent-1","messageId":"nope","content":"edited"}
        }"#;
        let out = handle_message(&RejectingApi, msg).await.expect("response");
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(err_code(&v), -32602);
        assert!(
            v["error"]["message"]
                .as_str()
                .unwrap_or_default()
                .contains("not found in transcript"),
            "error message should surface the domain detail: {v}"
        );
    }
}

/// `merge_user_app_message_id` (PROTOCOL §5.5): the top-level
/// `userAppMessageId` param folds into `messageMetadata` under the shared
/// `USER_APP_MESSAGE_ID_KEY`, stays backward compatible when absent, and
/// rejects oversized ids / non-object metadata.
mod merge_user_app_message_id {
    use serde_json::{json, Map, Value};

    use super::super::merge_user_app_message_id;

    fn params(v: &Value) -> Map<String, Value> {
        v.as_object().unwrap().clone()
    }

    /// Unwrap the merge result, panicking with the `RpcErr` message on failure
    /// (`RpcErr` intentionally has no `Debug` impl).
    fn merge_ok(p: &Map<String, Value>, md: Option<Value>) -> Option<Value> {
        match merge_user_app_message_id(p, md) {
            Ok(v) => v,
            Err(e) => panic!("unexpected merge error: {}", e.message),
        }
    }

    #[test]
    fn absent_id_passes_metadata_through_unchanged() {
        let p = params(&json!({}));
        assert_eq!(merge_ok(&p, None), None);
        let md = json!({ "source": "system" });
        assert_eq!(merge_ok(&p, Some(md.clone())), Some(md));
    }

    #[test]
    fn empty_or_whitespace_id_is_ignored() {
        let p = params(&json!({ "userAppMessageId": "  " }));
        assert_eq!(merge_ok(&p, None), None);
    }

    #[test]
    fn padded_id_is_trimmed_before_fold() {
        let p = params(&json!({ "userAppMessageId": "  app-msg-1  " }));
        assert_eq!(
            merge_ok(&p, None),
            Some(json!({ "userAppMessageId": "app-msg-1" }))
        );
    }

    #[test]
    fn id_folds_into_fresh_metadata_object() {
        let p = params(&json!({ "userAppMessageId": "app-msg-1" }));
        assert_eq!(
            merge_ok(&p, None),
            Some(json!({ "userAppMessageId": "app-msg-1" }))
        );
    }

    #[test]
    fn id_folds_into_existing_metadata_and_top_level_wins() {
        let p = params(&json!({ "userAppMessageId": "app-msg-1" }));
        let md = json!({ "source": "system", "userAppMessageId": "stale" });
        assert_eq!(
            merge_ok(&p, Some(md)),
            Some(json!({ "source": "system", "userAppMessageId": "app-msg-1" }))
        );
    }

    #[test]
    fn oversized_id_is_invalid_params() {
        let p = params(&json!({ "userAppMessageId": "x".repeat(257) }));
        assert!(merge_user_app_message_id(&p, None).is_err());
    }

    #[test]
    fn non_object_metadata_with_id_is_invalid_params() {
        let p = params(&json!({ "userAppMessageId": "app-msg-1" }));
        assert!(merge_user_app_message_id(&p, Some(json!("opaque"))).is_err());
    }
}

/// `stats.getUsage`: routing, param validation, and verbatim forwarding of
/// `period` / `key` / `tzOffsetMinutes` to the [`WorkspaceApi`] call.
mod stats_get_usage {
    use std::sync::{Arc, Mutex};

    use intent_core::{BoxFuture, Result, WorkspaceApi};
    use serde_json::{json, Value};

    use super::super::handle_message;
    use super::{call, err_code};

    #[tokio::test]
    async fn routes_past_dispatch_not_method_not_found() {
        // The FakeApi default impl yields -32603, never -32601, proving the
        // method is registered in the dispatch table.
        let v = call(
            r#"{"jsonrpc":"2.0","id":1,"method":"stats.getUsage","params":{"period":"month","key":"2026-07","tzOffsetMinutes":120}}"#,
        )
        .await
        .unwrap();
        assert_eq!(err_code(&v), -32603);

        // period alone is enough for 24h (key + tzOffsetMinutes optional).
        let v =
            call(r#"{"jsonrpc":"2.0","id":2,"method":"stats.getUsage","params":{"period":"24h"}}"#)
                .await
                .unwrap();
        assert_eq!(err_code(&v), -32603);
    }

    #[tokio::test]
    async fn malformed_params_are_minus_32602() {
        for msg in [
            // missing period entirely
            r#"{"jsonrpc":"2.0","id":1,"method":"stats.getUsage","params":{}}"#,
            // non-string period
            r#"{"jsonrpc":"2.0","id":2,"method":"stats.getUsage","params":{"period":24}}"#,
            // non-integer tzOffsetMinutes
            r#"{"jsonrpc":"2.0","id":3,"method":"stats.getUsage","params":{"period":"24h","tzOffsetMinutes":"sixty"}}"#,
        ] {
            let v = call(msg).await.unwrap();
            assert_eq!(err_code(&v), -32602, "msg={msg}");
        }
    }

    /// The forwarded `(period, key, tz_offset_minutes)` triple.
    type SeenParams = (String, Option<String>, i64);

    /// Records the forwarded params of the last `stats_get_usage` call.
    #[derive(Default)]
    struct RecordingApi {
        seen: Arc<Mutex<Option<SeenParams>>>,
    }

    impl WorkspaceApi for RecordingApi {
        fn stats_get_usage(
            &self,
            period: String,
            key: Option<String>,
            tz_offset_minutes: i64,
        ) -> BoxFuture<'_, Result<Value>> {
            let slot = self.seen.clone();
            Box::pin(async move {
                *slot.lock().unwrap() = Some((period, key, tz_offset_minutes));
                Ok(json!({ "ok": true }))
            })
        }
    }

    #[tokio::test]
    async fn forwards_period_key_and_tz_offset() {
        let api = RecordingApi::default();
        let msg = r#"{
            "jsonrpc":"2.0","id":1,"method":"stats.getUsage",
            "params":{"period":"month","key":"2026-07","tzOffsetMinutes":-300}
        }"#;
        let out = handle_message(&api, msg).await.expect("response");
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["result"]["ok"], true);
        assert_eq!(
            api.seen.lock().unwrap().clone(),
            Some(("month".to_string(), Some("2026-07".to_string()), -300))
        );

        // Omitted key/tzOffsetMinutes default to None / 0.
        let msg = r#"{"jsonrpc":"2.0","id":2,"method":"stats.getUsage","params":{"period":"24h"}}"#;
        let out = handle_message(&api, msg).await.expect("response");
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["result"]["ok"], true);
        assert_eq!(
            api.seen.lock().unwrap().clone(),
            Some(("24h".to_string(), None, 0))
        );
    }

    /// Domain `InvalidParams` from the service (bad period/key/tz) surfaces as
    /// `-32602` through `domain_to_rpc`.
    struct RejectingApi;

    impl WorkspaceApi for RejectingApi {
        fn stats_get_usage(
            &self,
            period: String,
            _key: Option<String>,
            _tz_offset_minutes: i64,
        ) -> BoxFuture<'_, Result<Value>> {
            Box::pin(async move {
                Err(intent_core::Error::InvalidParams(format!(
                    "period must be \"24h\", \"month\" or \"year\" (got {period:?})"
                )))
            })
        }
    }

    #[tokio::test]
    async fn service_invalid_params_maps_to_minus_32602() {
        let msg =
            r#"{"jsonrpc":"2.0","id":3,"method":"stats.getUsage","params":{"period":"week"}}"#;
        let out = handle_message(&RejectingApi, msg).await.expect("response");
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(err_code(&v), -32602);
        assert!(
            v["error"]["message"]
                .as_str()
                .unwrap_or_default()
                .contains("period must be"),
            "error message should surface the domain detail: {v}"
        );
    }
}

/// A response whose serialized frame exceeds `MAX_OUTBOUND_MESSAGE_BYTES` is
/// replaced at serialization with a `-32010` error echoing the request id —
/// not dropped by the writer-task backstop (which would leave the client to
/// hit its RPC timeout).
mod oversized_response {
    use intent_core::{AgentId, BoxFuture, Result, WorkspaceApi, WorkspaceId};
    use serde_json::Value;

    use super::super::handle_message;

    struct HugeApi;

    impl WorkspaceApi for HugeApi {
        #[allow(clippy::too_many_arguments)]
        fn agent_edit_and_regenerate(
            &self,
            _workspace_id: WorkspaceId,
            _agent_id: AgentId,
            _message_id: String,
            _content: String,
            _image_blocks: Option<Value>,
            _file_blocks: Option<Value>,
            _model: Option<String>,
        ) -> BoxFuture<'_, Result<Value>> {
            Box::pin(async {
                Ok(Value::String(
                    "x".repeat(crate::MAX_OUTBOUND_MESSAGE_BYTES + 1),
                ))
            })
        }
    }

    #[tokio::test]
    async fn oversized_result_yields_error_response_with_same_id() {
        let msg = r#"{
            "jsonrpc":"2.0","id":42,"method":"agent.editAndRegenerate",
            "params":{"workspaceId":"ws-1","agentId":"agent-1","messageId":"m1","content":"c"}
        }"#;
        let out = handle_message(&HugeApi, msg)
            .await
            .expect("an error response frame, not a dropped frame");
        assert!(
            out.len() <= crate::MAX_OUTBOUND_MESSAGE_BYTES,
            "replacement error frame must itself fit under the cap"
        );
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["id"], 42);
        assert_eq!(v["error"]["code"], -32010);
        let message = v["error"]["message"].as_str().unwrap();
        assert!(
            message.contains("agent.editAndRegenerate"),
            "message must name the method: {message}"
        );
        assert!(
            message.contains("bytes"),
            "message must carry the serialized size: {message}"
        );
        assert_eq!(v["error"]["data"]["code"], "oversized-response");
        assert_eq!(v["error"]["data"]["method"], "agent.editAndRegenerate");
        assert!(
            v["error"]["data"]["responseBytes"].as_u64().unwrap()
                > crate::MAX_OUTBOUND_MESSAGE_BYTES as u64,
            "data.responseBytes must be the oversized serialized size"
        );
    }
}
