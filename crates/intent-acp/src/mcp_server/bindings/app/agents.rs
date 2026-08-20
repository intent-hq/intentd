//! `ws.app.agents.*` bindings (chief-gated).
//!
//! Exposes cross-workspace agent audit methods (`list`, `readConversation`)
//! and the completion-watch registration method (`waitFor`) exclusively to
//! Chief-of-Staff workspace agents. Non-chief agents receive a clear gating
//! error. Shape parity with the TS reference
//! `packages/cloudlands-fe/src/features/mcp/main/mcp/ws-app-agents-api.ts`.

use std::sync::Arc;

use intent_core::{AgentId, AgentStatus, WorkspaceApi, WorkspaceId};
use serde_json::{json, Value};

use crate::mcp_server::bindings::{map_err, opt_bool, opt_i64, opt_str};

pub(crate) const PRELUDE: &str = r#"
    globalThis.ws = globalThis.ws || {};
    ws.app = ws.app || {};
    ws.app.agents = {
        list: (options) => host({ method: 'app.agents.list', args: options || {} }),
        readConversation: (workspaceId, agentId, opts) =>
            host({ method: 'app.agents.readConversation', args: { workspaceId, agentId, ...(opts || {}) } }),
        waitFor: (options) => host({ method: 'app.agents.waitFor', args: options || {} }),
    };
"#;

const DEFAULT_LIST_LIMIT: i64 = 50;
const MAX_LIST_LIMIT: i64 = 200;
const DEFAULT_READ_LIMIT: i64 = 20;
const MAX_READ_LIMIT: i64 = 100;

pub(crate) async fn dispatch(
    api: &Arc<dyn WorkspaceApi>,
    workspace_id: &WorkspaceId,
    caller: Option<&AgentId>,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    // Chief-workspace gating: all ws.app.* methods require the caller to be
    // in the Chief workspace.
    if !workspace_id.is_chief() {
        return Err("ws.app.* is only available in the Chief of Staff workspace".to_string());
    }

    match method {
        "list" => list(api, args).await,
        "readConversation" => read_conversation(api, args).await,
        "waitFor" => wait_for(api, workspace_id, caller, args).await,
        other => Err(format!("host: unknown method `app.agents.{other}`")),
    }
}

async fn list(api: &Arc<dyn WorkspaceApi>, args: &Value) -> Result<Value, String> {
    // Extract optional filters
    let filter_workspace_id = opt_str(args, "workspaceId").map(WorkspaceId::from);
    let include_completed = opt_bool(args, "includeCompleted").unwrap_or(false);
    let cursor = normalize_offset(opt_i64(args, "cursor"))?;
    let limit = normalize_limit(opt_i64(args, "limit"), DEFAULT_LIST_LIMIT, MAX_LIST_LIMIT)?;

    // Fetch all workspaces (include archived to apply status filtering)
    let workspaces = if let Some(ws_id) = filter_workspace_id {
        // Single workspace request
        if ws_id.is_chief() {
            return Err("Chief workspace has no agent threads".to_string());
        }
        let ws = api.get_workspace(ws_id.clone()).await.map_err(map_err)?;
        vec![ws]
    } else {
        // All non-chief workspaces
        let all = api.list_workspaces(true).await.map_err(map_err)?;
        all.into_iter()
            .filter(|ws| {
                !ws.id.is_chief() && format!("{:?}", ws.status).to_lowercase() != "deleted"
            })
            .collect()
    };

    // Collect agent threads from all target workspaces
    let mut threads = Vec::new();
    for ws in workspaces {
        let agents = api.agent_list(ws.id.clone()).await.map_err(map_err)?;
        for agent in agents {
            // Filter by completion status if requested
            let is_terminal = matches!(
                agent.status,
                AgentStatus::Completed | AgentStatus::Error | AgentStatus::Deleted
            );
            if !include_completed && is_terminal {
                continue;
            }

            // Build thread info (metadata-only, no transcript)
            let task_note_id = agent.metadata.task_note_id.as_ref().map(|id| id.0.clone());
            threads.push(json!({
                "workspaceId": ws.id.0,
                "workspaceTitle": &ws.title,
                "agentId": agent.id.0,
                "agentName": agent.name,
                "status": agent.status,
                "sessionStatus": agent.status,
                "messageCount": agent.message_count,
                "taskNoteId": task_note_id,
                "createdAt": agent.created_at,
                "updatedAt": agent.updated_at,
                "lastActivity": agent.last_activity,
            }));
        }
    }

    // Sort by last activity timestamp, newest first
    threads.sort_by(|a, b| {
        let ts_a = thread_timestamp(a);
        let ts_b = thread_timestamp(b);
        ts_b.cmp(&ts_a)
    });

    // Paginate
    let total = threads.len();
    let page: Vec<_> = threads
        .into_iter()
        .skip(cursor as usize)
        .take(limit as usize)
        .collect();
    let returned = page.len();

    let mut result = json!({
        "threads": page,
        "total": total,
        "returned": returned,
    });
    if cursor + (returned as i64) < total as i64 {
        result
            .as_object_mut()
            .unwrap()
            .insert("nextCursor".to_string(), json!(cursor + returned as i64));
    }
    Ok(result)
}

async fn read_conversation(api: &Arc<dyn WorkspaceApi>, args: &Value) -> Result<Value, String> {
    let workspace_id_str =
        opt_str(args, "workspaceId").ok_or_else(|| "workspaceId is required".to_string())?;
    let agent_id_str = opt_str(args, "agentId").ok_or_else(|| "agentId is required".to_string())?;
    let workspace_id = WorkspaceId::from(workspace_id_str.as_str());
    let agent_id = AgentId::from(agent_id_str.as_str());

    // Gating: chief workspace itself has no agents
    if workspace_id.is_chief() {
        return Err(format!("Workspace not found: {workspace_id_str}"));
    }

    // Bounds handling options
    let last_n = opt_i64(args, "lastN");
    let start_turn = opt_i64(args, "startTurn");
    let end_turn = opt_i64(args, "endTurn");
    let include_tool_calls = opt_bool(args, "includeToolCalls").unwrap_or(false);

    // Validate workspace exists
    let workspace = api
        .get_workspace(workspace_id.clone())
        .await
        .map_err(map_err)?;

    // Fetch agent metadata
    let agent = api
        .agent_get(agent_id.clone(), Some(workspace_id.clone()))
        .await
        .map_err(map_err)?;

    // Fetch full conversation (use agent.getConversation which returns { messages, ... })
    let conversation_result = api
        .agent_get_conversation(
            agent_id.clone(),
            None,
            Some(workspace_id.clone()),
            None,
            None,
            None,
            None,
        )
        .await
        .map_err(map_err)?;
    let all_messages = conversation_result
        .get("messages")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let total_messages = all_messages.len();

    // Apply bounds: either turn-range or lastN
    let (selected_messages, selected_start_turn, selected_end_turn) =
        if start_turn.is_some() || end_turn.is_some() {
            // Turn-based slicing (1-based, inclusive)
            let start = start_turn.unwrap_or(1).max(1) as usize - 1; // convert to 0-based
            let end = end_turn
                .map(|e| (e as usize).min(total_messages)) // clamp to total_messages
                .unwrap_or(total_messages)
                .min(start + MAX_READ_LIMIT as usize);
            if end < start + 1 {
                return Err("endTurn must be greater than or equal to startTurn".to_string());
            }
            let slice = all_messages[start..end].to_vec();
            (slice, start + 1, end) // return 1-based
        } else {
            // lastN slicing (default 20, max 100)
            let limit = normalize_limit(last_n, DEFAULT_READ_LIMIT, MAX_READ_LIMIT)? as usize;
            let start = total_messages.saturating_sub(limit);
            let slice = all_messages[start..].to_vec();
            (slice, start + 1, total_messages)
        };

    // Filter tool-call blocks if requested
    let filtered_messages: Vec<Value> = selected_messages
        .into_iter()
        .map(|msg| {
            if include_tool_calls {
                msg
            } else {
                filter_tool_calls(msg)
            }
        })
        .filter(has_returned_content)
        .collect();

    // Extract task note ID from metadata
    let task_note_id = agent.metadata.task_note_id.as_ref().map(|id| id.0.clone());

    Ok(json!({
        "workspaceId": workspace_id.0,
        "workspaceTitle": &workspace.title,
        "agentId": agent_id.0,
        "agentName": agent.name,
        "status": agent.status,
        "sessionStatus": agent.status,
        "totalMessages": total_messages,
        "returnedMessages": filtered_messages.len(),
        "startTurn": selected_start_turn,
        "endTurn": selected_end_turn,
        "includeToolCalls": include_tool_calls,
        "taskNoteId": task_note_id,
        "createdAt": agent.created_at,
        "updatedAt": agent.updated_at,
        "lastActivity": agent.last_activity,
        "messages": filtered_messages,
    }))
}

/// `ws.app.agents.waitFor({ agentIds, waitMode? })`: register completion
/// watches for the calling agent on a set of existing target agents. Arg
/// validation happens here (helpful JS-visible messages); target resolution,
/// scope gating, and registration live in the service op, whose errors
/// (unknown agent id, self-wait, deleted targets) surface via `map_err`.
async fn wait_for(
    api: &Arc<dyn WorkspaceApi>,
    workspace_id: &WorkspaceId,
    caller: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    let caller_agent_id = caller.cloned().ok_or_else(|| {
        "No agent context available. This tool must be called by an agent.".to_string()
    })?;
    let agent_ids = match args.get("agentIds") {
        None | Some(Value::Null) => {
            return Err("agentIds is required (an array of agent ids)".to_string())
        }
        Some(Value::Array(items)) => {
            let mut ids = Vec::with_capacity(items.len());
            for item in items {
                match item.as_str() {
                    Some(s) => ids.push(s.to_string()),
                    None => return Err("agentIds must be an array of agent id strings".to_string()),
                }
            }
            ids
        }
        Some(_) => return Err("agentIds must be an array of agent id strings".to_string()),
    };
    if agent_ids.is_empty() {
        return Err("agentIds must contain at least one agent id".to_string());
    }
    let wait_mode = match args.get("waitMode") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) if s == "immediate" || s == "after_all" => Some(s.clone()),
        Some(other) => {
            return Err(format!(
                "invalid waitMode {other} (expected \"immediate\" or \"after_all\")"
            ))
        }
    };
    api.app_agents_wait(workspace_id.clone(), caller_agent_id, agent_ids, wait_mode)
        .await
        .map_err(map_err)
}

/// Normalize cursor offset: must be non-negative integer.
fn normalize_offset(value: Option<i64>) -> Result<i64, String> {
    match value {
        None => Ok(0),
        Some(v) if v >= 0 => Ok(v),
        Some(_) => Err("cursor must be a non-negative integer offset".to_string()),
    }
}

/// Normalize limit: must be positive integer, clamped to max.
fn normalize_limit(value: Option<i64>, default: i64, max: i64) -> Result<i64, String> {
    match value {
        None => Ok(default),
        Some(v) if v >= 1 => Ok(v.min(max)),
        Some(_) => Err("limit must be a positive integer".to_string()),
    }
}

/// Extract timestamp for sorting threads (newest first).
/// Returns a simple lexicographic comparison key (ISO 8601 timestamps sort correctly as strings).
fn thread_timestamp(thread: &Value) -> String {
    thread
        .get("lastActivity")
        .or_else(|| thread.get("updatedAt"))
        .or_else(|| thread.get("createdAt"))
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

/// Filter out tool_use and tool_result blocks from a message if includeToolCalls=false.
fn filter_tool_calls(mut message: Value) -> Value {
    if let Some(blocks) = message
        .get_mut("contentBlocks")
        .and_then(Value::as_array_mut)
    {
        blocks.retain(|block| {
            let type_str = block.get("type").and_then(Value::as_str);
            type_str != Some("tool_use") && type_str != Some("tool_result")
        });
    }
    message
}

/// Check if a message has any content after filtering tool calls.
fn has_returned_content(message: &Value) -> bool {
    message
        .get("contentBlocks")
        .and_then(Value::as_array)
        .map(|blocks| !blocks.is_empty())
        .unwrap_or(true) // If no contentBlocks field, assume it has content
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::{
        AgentLite, AgentMetadata, AgentStatus, BoxFuture, Error, Result, Workspace,
        WorkspaceActivity, WorkspaceAttention, WorkspaceStatus,
    };
    use serde_json::json;
    use std::sync::{Arc, Mutex};

    type WaitCall = (WorkspaceId, AgentId, Vec<String>, Option<String>);

    #[derive(Default)]
    struct FakeApi {
        workspaces: Mutex<Vec<Workspace>>,
        agents: Mutex<Vec<AgentLite>>,
        conversation_messages: Mutex<Vec<Value>>,
        wait_calls: Mutex<Vec<WaitCall>>,
        wait_error: Mutex<Option<String>>,
    }

    impl WorkspaceApi for FakeApi {
        fn list_workspaces(
            &self,
            _include_archived: bool,
        ) -> BoxFuture<'_, Result<Vec<Workspace>>> {
            let workspaces = self.workspaces.lock().unwrap().clone();
            Box::pin(async move { Ok(workspaces) })
        }

        fn get_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
            let workspaces = self.workspaces.lock().unwrap().clone();
            Box::pin(async move {
                workspaces
                    .into_iter()
                    .find(|w| w.id == id)
                    .ok_or_else(|| Error::NotFound(format!("Workspace not found: {}", id.as_str())))
            })
        }

        fn agent_list(&self, _workspace_id: WorkspaceId) -> BoxFuture<'_, Result<Vec<AgentLite>>> {
            let agents = self.agents.lock().unwrap().clone();
            Box::pin(async move { Ok(agents) })
        }

        fn agent_get(
            &self,
            agent_id: AgentId,
            _workspace_id: Option<WorkspaceId>,
        ) -> BoxFuture<'_, Result<AgentLite>> {
            let agents = self.agents.lock().unwrap().clone();
            Box::pin(async move {
                agents
                    .into_iter()
                    .find(|a| a.id == agent_id)
                    .ok_or_else(|| {
                        Error::NotFound(format!("Agent not found: {}", agent_id.as_str()))
                    })
            })
        }

        fn agent_get_conversation(
            &self,
            _agent_id: AgentId,
            _last_n: Option<i64>,
            _workspace_id: Option<WorkspaceId>,
            _include_tool_calls: Option<String>,
            _around_message_id: Option<String>,
            _around_index: Option<i64>,
            _projection: Option<intent_core::ConversationProjection>,
        ) -> BoxFuture<'_, Result<Value>> {
            let messages = self.conversation_messages.lock().unwrap().clone();
            Box::pin(async move { Ok(json!({ "messages": messages })) })
        }

        fn app_agents_wait(
            &self,
            workspace_id: WorkspaceId,
            caller_agent_id: AgentId,
            agent_ids: Vec<String>,
            wait_mode: Option<String>,
        ) -> BoxFuture<'_, Result<Value>> {
            let error = self.wait_error.lock().unwrap().clone();
            self.wait_calls.lock().unwrap().push((
                workspace_id,
                caller_agent_id,
                agent_ids.clone(),
                wait_mode.clone(),
            ));
            Box::pin(async move {
                if let Some(msg) = error {
                    return Err(Error::InvalidParams(msg));
                }
                let mode = wait_mode.unwrap_or_else(|| "immediate".to_string());
                let results: Vec<Value> = agent_ids
                    .iter()
                    .map(|id| {
                        json!({
                            "agentId": id,
                            "agentName": "Target",
                            "workspaceId": "ws-1",
                            "subscriptionId": format!("sub-{id}"),
                            "groupId": null,
                        })
                    })
                    .collect();
                Ok(json!({ "ok": true, "waitMode": mode, "results": results }))
            })
        }
    }

    fn make_workspace(id: &str, title: &str) -> Workspace {
        Workspace {
            id: WorkspaceId::from_string(id),
            title: title.to_string(),
            branch: "main".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: WorkspaceStatus::Active,
            status_message: None,
            status_image_asset_id: None,
            activity: WorkspaceActivity::Idle,
            attention: WorkspaceAttention::None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
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
            display_status: None,
            waiting: false,
            checkout_mode: None,
            disk_usage: None,
            pending_delete_at: None,
        }
    }

    fn make_agent(id: &str, name: &str, status: AgentStatus, ws_id: &WorkspaceId) -> AgentLite {
        AgentLite {
            harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
            harness_features: None,
            id: AgentId::from_string(id),
            workspace_id: ws_id.clone(),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: name.to_string(),
            name_explicitly_set: false,
            model: None,
            reasoning_effort: None,
            effort_levels: None,
            provider: None,
            status,
            is_active: false,
            is_streaming: false,
            is_processing: false,
            is_responding: false,
            is_waiting_on_tool: false,
            is_waiting_for_other_agents: false,
            waiting_for_agent_ids: vec![],
            waiting_on_hooks: vec![],
            waiting_on_pr_monitors: vec![],
            turn_in_flight: false,
            last_stream_activity_at: None,
            stats: None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            last_activity: Some("2026-01-01T00:00:00Z".to_string()),
            message_count: 10,
            digest: None,
            last_agent_response: None,
            last_user_message: None,
            last_message_role: None,
            last_message_id: None,
            last_tool_use: None,
            context_references: None,
            file_blocks: None,
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
            pending_delete_at: None,
            metadata: AgentMetadata {
                is_background: false,
                specialist: None,
                created_by_agent_id: None,
                task_note_id: None,
                completion_report: None,
                completion_report_timestamp: None,
                attention_request_kind: None,
                attention_request_reason: None,
                attention_request_timestamp: None,
                delegation_depth: None,
                initial_message: None,
                sandbox_id: None,
                sandbox_branch: None,
                sandbox_path: None,
                dismissed_questions_message_id: None,
                pending_questions_message_id: None,
                last_seen_message_id: None,
                is_initial_agent: None,
            },
        }
    }

    #[test]
    fn test_normalize_offset() {
        assert_eq!(normalize_offset(None).unwrap(), 0);
        assert_eq!(normalize_offset(Some(0)).unwrap(), 0);
        assert_eq!(normalize_offset(Some(10)).unwrap(), 10);
        assert!(normalize_offset(Some(-1)).is_err());
    }

    #[test]
    fn test_normalize_limit() {
        assert_eq!(normalize_limit(None, 50, 200).unwrap(), 50);
        assert_eq!(normalize_limit(Some(10), 50, 200).unwrap(), 10);
        assert_eq!(normalize_limit(Some(250), 50, 200).unwrap(), 200);
        assert!(normalize_limit(Some(0), 50, 200).is_err());
        assert!(normalize_limit(Some(-1), 50, 200).is_err());
    }

    #[test]
    fn test_filter_tool_calls() {
        let message = json!({
            "id": "msg-1",
            "role": "assistant",
            "contentBlocks": [
                { "type": "text", "text": "Hello" },
                { "type": "tool_use", "id": "call-1", "name": "fetch" },
                { "type": "tool_result", "tool_use_id": "call-1", "content": "data" },
                { "type": "text", "text": "World" }
            ]
        });

        let filtered = filter_tool_calls(message);
        let blocks = filtered.get("contentBlocks").unwrap().as_array().unwrap();
        assert_eq!(blocks.len(), 2);
        assert_eq!(blocks[0].get("text").unwrap(), "Hello");
        assert_eq!(blocks[1].get("text").unwrap(), "World");
    }

    #[test]
    fn test_has_returned_content() {
        let with_content = json!({ "contentBlocks": [{ "type": "text", "text": "hi" }] });
        let empty_blocks = json!({ "contentBlocks": [] });
        let no_blocks = json!({ "role": "user" });

        assert!(has_returned_content(&with_content));
        assert!(!has_returned_content(&empty_blocks));
        assert!(has_returned_content(&no_blocks)); // assumes content if field missing
    }

    #[test]
    fn test_thread_timestamp() {
        let thread = json!({ "lastActivity": "2026-01-15T12:00:00Z" });
        assert_eq!(thread_timestamp(&thread), "2026-01-15T12:00:00Z");

        let no_timestamp = json!({ "agentId": "agent-1" });
        assert_eq!(thread_timestamp(&no_timestamp), "");
    }

    #[tokio::test]
    async fn test_dispatch_rejects_non_chief_workspace() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let non_chief_id = WorkspaceId::from_string("amber-forest");
        let result = dispatch(&api, &non_chief_id, None, "list", &json!({})).await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "ws.app.* is only available in the Chief of Staff workspace"
        );
    }

    #[tokio::test]
    async fn test_list_default_limit_50() {
        let fake = Arc::new(FakeApi::default());
        let ws_id = WorkspaceId::from_string("ws-1");
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            workspaces.push(make_workspace("ws-1", "Workspace 1"));
            let mut agents = fake.agents.lock().unwrap();
            // Add 60 agents to test default limit
            for i in 0..60 {
                agents.push(make_agent(
                    &format!("agent-{}", i),
                    &format!("Agent {}", i),
                    AgentStatus::Active,
                    &ws_id,
                ));
            }
        }
        let api: Arc<dyn WorkspaceApi> = fake;

        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, None, "list", &json!({}))
            .await
            .unwrap();

        assert_eq!(result.get("total").unwrap().as_u64().unwrap(), 60);
        assert_eq!(result.get("returned").unwrap().as_u64().unwrap(), 50); // default limit
        let threads = result.get("threads").unwrap().as_array().unwrap();
        assert_eq!(threads.len(), 50);
    }

    #[tokio::test]
    async fn test_list_max_limit_200() {
        let fake = Arc::new(FakeApi::default());
        let ws_id = WorkspaceId::from_string("ws-1");
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            workspaces.push(make_workspace("ws-1", "Workspace 1"));
            let mut agents = fake.agents.lock().unwrap();
            // Add 250 agents to test max limit clamping
            for i in 0..250 {
                agents.push(make_agent(
                    &format!("agent-{}", i),
                    &format!("Agent {}", i),
                    AgentStatus::Active,
                    &ws_id,
                ));
            }
        }
        let api: Arc<dyn WorkspaceApi> = fake;

        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, None, "list", &json!({ "limit": 300 }))
            .await
            .unwrap();

        assert_eq!(result.get("total").unwrap().as_u64().unwrap(), 250);
        assert_eq!(result.get("returned").unwrap().as_u64().unwrap(), 200); // clamped to max
        let threads = result.get("threads").unwrap().as_array().unwrap();
        assert_eq!(threads.len(), 200);
    }

    #[tokio::test]
    async fn test_list_returns_expected_shape() {
        let fake = Arc::new(FakeApi::default());
        let ws_id = WorkspaceId::from_string("ws-1");
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            workspaces.push(make_workspace("ws-1", "Workspace 1"));
            let mut agents = fake.agents.lock().unwrap();
            agents.push(make_agent(
                "agent-1",
                "Test Agent",
                AgentStatus::Active,
                &ws_id,
            ));
        }
        let api: Arc<dyn WorkspaceApi> = fake;

        let chief_id = WorkspaceId::chief();
        let result = dispatch(&api, &chief_id, None, "list", &json!({}))
            .await
            .unwrap();

        let threads = result.get("threads").unwrap().as_array().unwrap();
        assert_eq!(threads.len(), 1);
        let thread = &threads[0];

        // Check expected fields are present
        assert!(thread.get("workspaceId").is_some());
        assert!(thread.get("workspaceTitle").is_some());
        assert!(thread.get("agentId").is_some());
        assert!(thread.get("agentName").is_some());
        assert!(thread.get("status").is_some());
        assert!(thread.get("messageCount").is_some());
        assert!(thread.get("createdAt").is_some());
        assert!(thread.get("updatedAt").is_some());
    }

    #[tokio::test]
    async fn test_read_conversation_default_last_n_20() {
        let fake = Arc::new(FakeApi::default());
        let ws_id = WorkspaceId::from_string("ws-1");
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            workspaces.push(make_workspace("ws-1", "Workspace 1"));
            let mut agents = fake.agents.lock().unwrap();
            agents.push(make_agent(
                "agent-1",
                "Test Agent",
                AgentStatus::Active,
                &ws_id,
            ));
            let mut messages = fake.conversation_messages.lock().unwrap();
            // Add 50 messages to test default lastN
            for i in 0..50 {
                messages.push(json!({
                    "id": format!("msg-{}", i),
                    "role": if i % 2 == 0 { "user" } else { "assistant" },
                    "contentBlocks": [{ "type": "text", "text": format!("Message {}", i) }]
                }));
            }
        }
        let api: Arc<dyn WorkspaceApi> = fake;

        let chief_id = WorkspaceId::chief();
        let result = dispatch(
            &api,
            &chief_id,
            None,
            "readConversation",
            &json!({ "workspaceId": "ws-1", "agentId": "agent-1" }),
        )
        .await
        .unwrap();

        assert_eq!(result.get("totalMessages").unwrap().as_u64().unwrap(), 50);
        assert_eq!(
            result.get("returnedMessages").unwrap().as_u64().unwrap(),
            20
        ); // default lastN
        let messages = result.get("messages").unwrap().as_array().unwrap();
        assert_eq!(messages.len(), 20);
    }

    #[tokio::test]
    async fn test_read_conversation_max_limit_100() {
        let fake = Arc::new(FakeApi::default());
        let ws_id = WorkspaceId::from_string("ws-1");
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            workspaces.push(make_workspace("ws-1", "Workspace 1"));
            let mut agents = fake.agents.lock().unwrap();
            agents.push(make_agent(
                "agent-1",
                "Test Agent",
                AgentStatus::Active,
                &ws_id,
            ));
            let mut messages = fake.conversation_messages.lock().unwrap();
            // Add 150 messages to test max clamping
            for i in 0..150 {
                messages.push(json!({
                    "id": format!("msg-{}", i),
                    "role": if i % 2 == 0 { "user" } else { "assistant" },
                    "contentBlocks": [{ "type": "text", "text": format!("Message {}", i) }]
                }));
            }
        }
        let api: Arc<dyn WorkspaceApi> = fake;

        let chief_id = WorkspaceId::chief();
        let result = dispatch(
            &api,
            &chief_id,
            None,
            "readConversation",
            &json!({ "workspaceId": "ws-1", "agentId": "agent-1", "lastN": 200 }),
        )
        .await
        .unwrap();

        assert_eq!(result.get("totalMessages").unwrap().as_u64().unwrap(), 150);
        assert_eq!(
            result.get("returnedMessages").unwrap().as_u64().unwrap(),
            100
        ); // clamped to max
        let messages = result.get("messages").unwrap().as_array().unwrap();
        assert_eq!(messages.len(), 100);
    }

    #[tokio::test]
    async fn test_read_conversation_start_turn_end_turn_validation() {
        let fake = Arc::new(FakeApi::default());
        let ws_id = WorkspaceId::from_string("ws-1");
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            workspaces.push(make_workspace("ws-1", "Workspace 1"));
            let mut agents = fake.agents.lock().unwrap();
            agents.push(make_agent(
                "agent-1",
                "Test Agent",
                AgentStatus::Active,
                &ws_id,
            ));
            let mut messages = fake.conversation_messages.lock().unwrap();
            for i in 0..10 {
                messages.push(json!({
                    "id": format!("msg-{}", i),
                    "role": if i % 2 == 0 { "user" } else { "assistant" },
                    "contentBlocks": [{ "type": "text", "text": format!("Message {}", i) }]
                }));
            }
        }
        let api: Arc<dyn WorkspaceApi> = fake.clone();

        let chief_id = WorkspaceId::chief();

        // Test valid range
        let result = dispatch(
            &api,
            &chief_id,
            None,
            "readConversation",
            &json!({ "workspaceId": "ws-1", "agentId": "agent-1", "startTurn": 2, "endTurn": 5 }),
        )
        .await
        .unwrap();
        assert_eq!(result.get("startTurn").unwrap().as_u64().unwrap(), 2);
        assert_eq!(result.get("endTurn").unwrap().as_u64().unwrap(), 5);
        let messages = result.get("messages").unwrap().as_array().unwrap();
        assert_eq!(messages.len(), 4); // turns 2-5 inclusive

        // Test invalid range (endTurn < startTurn)
        let result_err = dispatch(
            &api,
            &chief_id,
            None,
            "readConversation",
            &json!({ "workspaceId": "ws-1", "agentId": "agent-1", "startTurn": 5, "endTurn": 2 }),
        )
        .await;
        assert!(result_err.is_err());
        assert!(result_err
            .unwrap_err()
            .contains("endTurn must be greater than or equal to startTurn"));
    }

    #[tokio::test]
    async fn test_read_conversation_include_tool_calls_false_by_default() {
        let fake = Arc::new(FakeApi::default());
        let ws_id = WorkspaceId::from_string("ws-1");
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            workspaces.push(make_workspace("ws-1", "Workspace 1"));
            let mut agents = fake.agents.lock().unwrap();
            agents.push(make_agent(
                "agent-1",
                "Test Agent",
                AgentStatus::Active,
                &ws_id,
            ));
            let mut messages = fake.conversation_messages.lock().unwrap();
            messages.push(json!({
                "id": "msg-1",
                "role": "assistant",
                "contentBlocks": [
                    { "type": "text", "text": "Hello" },
                    { "type": "tool_use", "id": "call-1", "name": "fetch" },
                    { "type": "tool_result", "tool_use_id": "call-1", "content": "data" }
                ]
            }));
        }
        let api: Arc<dyn WorkspaceApi> = fake;

        let chief_id = WorkspaceId::chief();
        let result = dispatch(
            &api,
            &chief_id,
            None,
            "readConversation",
            &json!({ "workspaceId": "ws-1", "agentId": "agent-1" }),
        )
        .await
        .unwrap();

        assert!(!result.get("includeToolCalls").unwrap().as_bool().unwrap());
        let messages = result.get("messages").unwrap().as_array().unwrap();
        assert_eq!(messages.len(), 1);
        let blocks = messages[0]
            .get("contentBlocks")
            .unwrap()
            .as_array()
            .unwrap();
        // Tool blocks should be filtered out
        assert_eq!(blocks.len(), 1);
        assert_eq!(blocks[0].get("type").unwrap(), "text");
    }

    #[tokio::test]
    async fn test_read_conversation_include_tool_calls_true() {
        let fake = Arc::new(FakeApi::default());
        let ws_id = WorkspaceId::from_string("ws-1");
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            workspaces.push(make_workspace("ws-1", "Workspace 1"));
            let mut agents = fake.agents.lock().unwrap();
            agents.push(make_agent(
                "agent-1",
                "Test Agent",
                AgentStatus::Active,
                &ws_id,
            ));
            let mut messages = fake.conversation_messages.lock().unwrap();
            messages.push(json!({
                "id": "msg-1",
                "role": "assistant",
                "contentBlocks": [
                    { "type": "text", "text": "Hello" },
                    { "type": "tool_use", "id": "call-1", "name": "fetch" },
                    { "type": "tool_result", "tool_use_id": "call-1", "content": "data" }
                ]
            }));
        }
        let api: Arc<dyn WorkspaceApi> = fake;

        let chief_id = WorkspaceId::chief();
        let result = dispatch(
            &api,
            &chief_id,
            None,
            "readConversation",
            &json!({ "workspaceId": "ws-1", "agentId": "agent-1", "includeToolCalls": true }),
        )
        .await
        .unwrap();

        assert!(result.get("includeToolCalls").unwrap().as_bool().unwrap());
        let messages = result.get("messages").unwrap().as_array().unwrap();
        assert_eq!(messages.len(), 1);
        let blocks = messages[0]
            .get("contentBlocks")
            .unwrap()
            .as_array()
            .unwrap();
        // Tool blocks should be included
        assert_eq!(blocks.len(), 3);
    }

    #[tokio::test]
    async fn test_read_conversation_returns_expected_shape() {
        let fake = Arc::new(FakeApi::default());
        let ws_id = WorkspaceId::from_string("ws-1");
        {
            let mut workspaces = fake.workspaces.lock().unwrap();
            workspaces.push(make_workspace("ws-1", "Workspace 1"));
            let mut agents = fake.agents.lock().unwrap();
            agents.push(make_agent(
                "agent-1",
                "Test Agent",
                AgentStatus::Active,
                &ws_id,
            ));
            let mut messages = fake.conversation_messages.lock().unwrap();
            messages.push(json!({
                "id": "msg-1",
                "role": "user",
                "contentBlocks": [{ "type": "text", "text": "Hello" }]
            }));
        }
        let api: Arc<dyn WorkspaceApi> = fake;

        let chief_id = WorkspaceId::chief();
        let result = dispatch(
            &api,
            &chief_id,
            None,
            "readConversation",
            &json!({ "workspaceId": "ws-1", "agentId": "agent-1" }),
        )
        .await
        .unwrap();

        // Check expected fields are present
        assert!(result.get("workspaceId").is_some());
        assert!(result.get("workspaceTitle").is_some());
        assert!(result.get("agentId").is_some());
        assert!(result.get("agentName").is_some());
        assert!(result.get("status").is_some());
        assert!(result.get("totalMessages").is_some());
        assert!(result.get("returnedMessages").is_some());
        assert!(result.get("startTurn").is_some());
        assert!(result.get("endTurn").is_some());
        assert!(result.get("includeToolCalls").is_some());
        assert!(result.get("messages").is_some());
    }

    #[tokio::test]
    async fn test_wait_for_rejects_non_chief_workspace() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let non_chief_id = WorkspaceId::from_string("amber-forest");
        let caller = AgentId::from_string("agent-caller");
        let result = dispatch(
            &api,
            &non_chief_id,
            Some(&caller),
            "waitFor",
            &json!({ "agentIds": ["agent-1"] }),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "ws.app.* is only available in the Chief of Staff workspace"
        );
    }

    #[tokio::test]
    async fn test_wait_for_requires_agent_context() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let result = dispatch(
            &api,
            &chief_id,
            None,
            "waitFor",
            &json!({ "agentIds": ["agent-1"] }),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "No agent context available. This tool must be called by an agent."
        );
    }

    #[tokio::test]
    async fn test_wait_for_validates_agent_ids() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let caller = AgentId::from_string("agent-caller");

        // Missing agentIds
        let missing = dispatch(&api, &chief_id, Some(&caller), "waitFor", &json!({})).await;
        assert_eq!(
            missing.unwrap_err(),
            "agentIds is required (an array of agent ids)"
        );

        // Non-array agentIds
        let non_array = dispatch(
            &api,
            &chief_id,
            Some(&caller),
            "waitFor",
            &json!({ "agentIds": "agent-1" }),
        )
        .await;
        assert_eq!(
            non_array.unwrap_err(),
            "agentIds must be an array of agent id strings"
        );

        // Array with non-string entries
        let non_string = dispatch(
            &api,
            &chief_id,
            Some(&caller),
            "waitFor",
            &json!({ "agentIds": ["agent-1", 42] }),
        )
        .await;
        assert_eq!(
            non_string.unwrap_err(),
            "agentIds must be an array of agent id strings"
        );

        // Empty array
        let empty = dispatch(
            &api,
            &chief_id,
            Some(&caller),
            "waitFor",
            &json!({ "agentIds": [] }),
        )
        .await;
        assert_eq!(
            empty.unwrap_err(),
            "agentIds must contain at least one agent id"
        );
    }

    #[tokio::test]
    async fn test_wait_for_validates_wait_mode() {
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi::default());
        let chief_id = WorkspaceId::chief();
        let caller = AgentId::from_string("agent-caller");
        let result = dispatch(
            &api,
            &chief_id,
            Some(&caller),
            "waitFor",
            &json!({ "agentIds": ["agent-1"], "waitMode": "sometimes" }),
        )
        .await;
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "invalid waitMode \"sometimes\" (expected \"immediate\" or \"after_all\")"
        );
    }

    #[tokio::test]
    async fn test_wait_for_happy_path_forwards_args_to_service() {
        let fake = Arc::new(FakeApi::default());
        let api: Arc<dyn WorkspaceApi> = fake.clone();
        let chief_id = WorkspaceId::chief();
        let caller = AgentId::from_string("agent-caller");

        let result = dispatch(
            &api,
            &chief_id,
            Some(&caller),
            "waitFor",
            &json!({ "agentIds": ["agent-1", "agent-2"], "waitMode": "after_all" }),
        )
        .await
        .unwrap();

        assert!(result.get("ok").unwrap().as_bool().unwrap());
        assert_eq!(result.get("waitMode").unwrap(), "after_all");
        let results = result.get("results").unwrap().as_array().unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].get("agentId").unwrap(), "agent-1");
        assert_eq!(results[0].get("subscriptionId").unwrap(), "sub-agent-1");

        let calls = fake.wait_calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        let (ws, caller_id, ids, mode) = &calls[0];
        assert_eq!(ws, &chief_id);
        assert_eq!(caller_id, &caller);
        assert_eq!(ids, &vec!["agent-1".to_string(), "agent-2".to_string()]);
        assert_eq!(mode.as_deref(), Some("after_all"));
    }

    #[tokio::test]
    async fn test_wait_for_omits_wait_mode_by_default() {
        let fake = Arc::new(FakeApi::default());
        let api: Arc<dyn WorkspaceApi> = fake.clone();
        let chief_id = WorkspaceId::chief();
        let caller = AgentId::from_string("agent-caller");

        let result = dispatch(
            &api,
            &chief_id,
            Some(&caller),
            "waitFor",
            &json!({ "agentIds": ["agent-1"] }),
        )
        .await
        .unwrap();

        assert_eq!(result.get("waitMode").unwrap(), "immediate");
        let calls = fake.wait_calls.lock().unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].3, None);
    }

    #[tokio::test]
    async fn test_wait_for_surfaces_service_errors() {
        let fake = Arc::new(FakeApi::default());
        *fake.wait_error.lock().unwrap() = Some("unknown agent id: agent-ghost".to_string());
        let api: Arc<dyn WorkspaceApi> = fake;
        let chief_id = WorkspaceId::chief();
        let caller = AgentId::from_string("agent-caller");

        let result = dispatch(
            &api,
            &chief_id,
            Some(&caller),
            "waitFor",
            &json!({ "agentIds": ["agent-ghost"] }),
        )
        .await;
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("unknown agent id: agent-ghost"));
    }
}
