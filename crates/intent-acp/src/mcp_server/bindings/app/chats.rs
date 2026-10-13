//! Assistant-only management of chats in the chief workspace.

use std::sync::Arc;

use intent_core::{AgentId, AgentLite, AgentStatus, WorkspaceApi, WorkspaceId};
use serde_json::{json, Value};

use super::agents;
use crate::mcp_server::bindings::{map_err, req_str, strip_agent_hidden_fields};

pub(crate) const PRELUDE: &str = r"
    globalThis.ws = globalThis.ws || {};
    ws.app = ws.app || {};
    ws.app.chats = {
        list: (options) => host({ method: 'app.chats.list', args: options || {} }),
        readConversation: (agentId, opts) =>
            host({ method: 'app.chats.readConversation', args: { ...(opts || {}), agentId } }),
        getMessageBlock: (agentId, messageId, blockId) =>
            host({ method: 'app.chats.getMessageBlock', args: { agentId, messageId, blockId } }),
        rename: (agentId, name) => host({ method: 'app.chats.rename', args: { agentId, name } }),
        delete: (agentId) => host({ method: 'app.chats.delete', args: { agentId } }),
        cancelDelete: (agentId) => host({ method: 'app.chats.cancelDelete', args: { agentId } }),
    };
";

const DELETE_GRACE_MS: u64 = 15_000;

pub(crate) async fn dispatch(
    api: &Arc<dyn WorkspaceApi>,
    workspace_id: &WorkspaceId,
    caller_agent_id: Option<&AgentId>,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    // Trust only the caller's dispatch context, never an argument's workspaceId.
    if !workspace_id.is_chief() {
        return Err("ws.app.* is only available in the Assistant workspace".to_string());
    }
    let mut out = match method {
        "list" => list(api, caller_agent_id, args).await?,
        "readConversation" => {
            let agent = target_chat(api, args).await?;
            let workspace = api
                .get_workspace(WorkspaceId::chief())
                .await
                .map_err(map_err)?;
            agents::read_conversation_for_agent(api, &workspace, &agent, args).await?
        }
        "getMessageBlock" => {
            let message_id = req_str(args, "messageId")?;
            let block_id = req_str(args, "blockId")?;
            let agent = target_chat(api, args).await?;
            api.agent_get_message_block(agent.id, message_id, block_id, Some(WorkspaceId::chief()))
                .await
                .map_err(map_err)?
        }
        "rename" => {
            let name = req_str(args, "name")?;
            let name = name.trim();
            if name.is_empty() {
                return Err("name must not be empty".to_string());
            }
            let agent = target_chat(api, args).await?;
            api.agent_rename(agent.id, name.to_string(), false)
                .await
                .map_err(map_err)?
        }
        "delete" => {
            let caller = caller_agent_id.ok_or_else(|| {
                "No agent context available. This tool must be called by an agent.".to_string()
            })?;
            let agent_id = AgentId::from(req_str(args, "agentId")?);
            if &agent_id == caller {
                return Err("Cannot delete the current Assistant chat".to_string());
            }
            let agent = target_chat(api, args).await?;
            let delete_at = api
                .agent_schedule_delete(agent.id, Some(WorkspaceId::chief()), DELETE_GRACE_MS)
                .await
                .map_err(map_err)?;
            json!({ "success": true, "scheduled": true, "deleteAt": delete_at })
        }
        "cancelDelete" => {
            let agent = target_chat(api, args).await?;
            let cancelled = api
                .agent_cancel_delete(agent.id, Some(WorkspaceId::chief()))
                .await
                .map_err(map_err)?;
            json!({ "cancelled": cancelled })
        }
        other => return Err(format!("host: unknown method `app.chats.{other}`")),
    };
    strip_agent_hidden_fields(&mut out);
    Ok(out)
}

async fn target_chat(api: &Arc<dyn WorkspaceApi>, args: &Value) -> Result<AgentLite, String> {
    let agent_id = AgentId::from(req_str(args, "agentId")?);
    let agent = api
        .agent_get(agent_id, Some(WorkspaceId::chief()))
        .await
        .map_err(map_err)?;
    // Scope must hold on the resolved row even if an API implementation ignores
    // its optional workspace filter. No reads or mutations follow a mismatch.
    if !agent.workspace_id.is_chief() || agent.status == AgentStatus::Deleted {
        return Err("Assistant chat not found".to_string());
    }
    Ok(agent)
}

async fn list(
    api: &Arc<dyn WorkspaceApi>,
    caller_agent_id: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    let (cursor, limit) = agents::list_bounds(args)?;
    let chats = api
        .agent_list(WorkspaceId::chief())
        .await
        .map_err(map_err)?
        .into_iter()
        .filter(|agent| agent.workspace_id.is_chief() && agent.status != AgentStatus::Deleted)
        .map(|agent| {
            let mut row = json!({
                "agentId": agent.id,
                "agentName": agent.name,
                "status": agent.status,
                "messageCount": agent.message_count,
                "createdAt": agent.created_at,
                "updatedAt": agent.updated_at,
                "lastActivity": agent.last_activity.as_ref().unwrap_or(&agent.updated_at),
                "isCurrent": caller_agent_id == Some(&agent.id),
            });
            if let Some(delete_at) = agent.pending_delete_at {
                row["pendingDeleteAt"] = json!(delete_at);
            }
            row
        })
        .collect();
    Ok(agents::page_threads(chats, cursor, limit, "chats"))
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use intent_core::{BoxFuture, ConversationProjection, Error, Result, Workspace};

    use super::*;

    const DEADLINE: &str = "2026-10-11T00:00:15Z";
    const TARGET: &str = "chat-other";
    const CALLER: &str = "chat-current";

    #[derive(Default)]
    struct FakeApi {
        agents: Mutex<Vec<AgentLite>>,
        calls: Mutex<Vec<Value>>,
        messages: Vec<Value>,
        rename_error: bool,
        pending: Mutex<bool>,
    }

    impl WorkspaceApi for FakeApi {
        fn agent_list(&self, workspace_id: WorkspaceId) -> BoxFuture<'_, Result<Vec<AgentLite>>> {
            self.calls
                .lock()
                .unwrap()
                .push(json!({ "method": "list", "workspaceId": workspace_id }));
            let agents = self.agents.lock().unwrap().clone();
            Box::pin(async move { Ok(agents) })
        }

        fn agent_get(
            &self,
            agent_id: AgentId,
            workspace_id: Option<WorkspaceId>,
        ) -> BoxFuture<'_, Result<AgentLite>> {
            self.calls
                .lock()
                .unwrap()
                .push(json!({ "method": "get", "workspaceId": workspace_id }));
            // Deliberately ignore scope so the binding must check the actual row.
            let agent = self
                .agents
                .lock()
                .unwrap()
                .iter()
                .find(|a| a.id == agent_id)
                .cloned();
            Box::pin(
                async move { agent.ok_or_else(|| Error::NotFound("missing chat".to_string())) },
            )
        }

        fn get_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
            assert!(id.is_chief());
            self.calls
                .lock()
                .unwrap()
                .push(json!({ "method": "workspace" }));
            Box::pin(async move {
                Ok(serde_json::from_value(json!({
                    "id": id,
                    "title": "Assistant",
                    "branch": "main",
                    "status": "Active",
                    "activity": "idle",
                    "attention": "none",
                    "createdAt": "2026-10-11T00:00:00Z",
                    "updatedAt": "2026-10-11T00:00:00Z",
                    "tags": [],
                    "skipWorktree": true,
                    "isRemote": false,
                    "archived": false,
                }))
                .unwrap())
            })
        }

        fn agent_rename(
            &self,
            agent_id: AgentId,
            name: String,
            skip_if_explicitly_set: bool,
        ) -> BoxFuture<'_, Result<Value>> {
            self.calls.lock().unwrap().push(json!({
                "method": "rename", "agentId": agent_id, "name": name,
                "skipIfExplicitlySet": skip_if_explicitly_set,
            }));
            Box::pin(async move {
                if self.rename_error {
                    return Err(Error::InvalidParams("rename rejected".to_string()));
                }
                self.agents
                    .lock()
                    .unwrap()
                    .iter_mut()
                    .find(|a| a.id == agent_id)
                    .unwrap()
                    .name = name.clone();
                Ok(json!({ "success": true, "name": name, "notificationsMuted": true }))
            })
        }

        fn agent_schedule_delete(
            &self,
            agent_id: AgentId,
            workspace_id: Option<WorkspaceId>,
            undo_delay_ms: u64,
        ) -> BoxFuture<'_, Result<String>> {
            self.calls.lock().unwrap().push(json!({
                "method": "schedule", "agentId": agent_id,
                "workspaceId": workspace_id, "undoDelayMs": undo_delay_ms,
            }));
            *self.pending.lock().unwrap() = true;
            Box::pin(async { Ok(DEADLINE.to_string()) })
        }

        fn agent_cancel_delete(
            &self,
            agent_id: AgentId,
            workspace_id: Option<WorkspaceId>,
        ) -> BoxFuture<'_, Result<bool>> {
            self.calls.lock().unwrap().push(json!({
                "method": "cancel", "agentId": agent_id, "workspaceId": workspace_id,
            }));
            let cancelled = std::mem::take(&mut *self.pending.lock().unwrap());
            Box::pin(async move { Ok(cancelled) })
        }

        fn agent_get_conversation(
            &self,
            agent_id: AgentId,
            limit: Option<i64>,
            workspace_id: Option<WorkspaceId>,
            page_token: Option<String>,
            _around_message_id: Option<String>,
            _around_index: Option<i64>,
            projection: Option<ConversationProjection>,
            include_in_progress: bool,
        ) -> BoxFuture<'_, Result<Value>> {
            assert_eq!(projection, Some(ConversationProjection::Slim));
            assert!(!include_in_progress);
            self.calls.lock().unwrap().push(json!({
                "method": "conversation", "agentId": agent_id,
                "workspaceId": workspace_id, "limit": limit, "pageToken": page_token,
            }));
            let end = page_token.map_or(self.messages.len(), |token| token.parse().unwrap());
            // Simulate byte-budgeted pages smaller than their requested limit.
            let start = end.saturating_sub(usize::try_from(limit.unwrap()).unwrap().min(2));
            let mut out = json!({ "messages": self.messages[start..end], "totalMessages": self.messages.len() });
            if start > 0 {
                out["nextToken"] = json!(start.to_string());
            }
            Box::pin(async move { Ok(out) })
        }

        fn agent_get_message_block(
            &self,
            agent_id: AgentId,
            message_id: String,
            block_id: String,
            workspace_id: Option<WorkspaceId>,
        ) -> BoxFuture<'_, Result<Value>> {
            self.calls.lock().unwrap().push(json!({
                "method": "block", "agentId": agent_id, "workspaceId": workspace_id,
                "messageId": message_id, "blockId": block_id,
            }));
            Box::pin(async move {
                Ok(json!({
                    "messageId": message_id,
                    "block": { "id": block_id, "type": "text", "text": "full body", "notificationsMuted": true },
                }))
            })
        }
    }

    fn chat(id: &str, workspace_id: &WorkspaceId, status: AgentStatus) -> AgentLite {
        serde_json::from_value(json!({
            "id": id, "workspaceId": workspace_id, "name": id, "status": status,
            "createdAt": "2026-10-11T00:00:00Z", "updatedAt": "2026-10-11T00:00:00Z",
            "messageCount": 4, "metadata": { "isBackground": false }, "notificationsMuted": true,
        }))
        .unwrap()
    }

    fn fixture() -> Arc<FakeApi> {
        Arc::new(FakeApi {
            agents: Mutex::new(vec![
                chat(CALLER, &WorkspaceId::chief(), AgentStatus::Active),
                chat(TARGET, &WorkspaceId::chief(), AgentStatus::Idle),
            ]),
            messages: (0..4).map(|index| json!({
                "id": format!("m-{index}"), "role": "assistant",
                "messageMetadata": { "notificationsMuted": true },
                "contentBlocks": [
                    { "id": format!("b-{index}"), "type": "text", "text": format!("message {index}") },
                    { "type": "tool_use", "name": "tool", "input": "slim preview" },
                ],
            })).collect(),
            ..Default::default()
        })
    }

    async fn call(
        api: &Arc<FakeApi>,
        method: &str,
        args: Value,
    ) -> std::result::Result<Value, String> {
        let api: Arc<dyn WorkspaceApi> = api.clone();
        dispatch(
            &api,
            &WorkspaceId::chief(),
            Some(&AgentId::from(CALLER)),
            method,
            &args,
        )
        .await
    }

    fn target_args(id: &str) -> Value {
        json!({ "agentId": id, "workspaceId": "__chief__", "name": "Renamed", "messageId": "m-0", "blockId": "b-0" })
    }

    #[tokio::test]
    async fn ordinary_workspace_cannot_call_any_chat_method() {
        let fake = fixture();
        let api: Arc<dyn WorkspaceApi> = fake.clone();
        for method in [
            "list",
            "readConversation",
            "getMessageBlock",
            "rename",
            "delete",
            "cancelDelete",
        ] {
            let err = dispatch(
                &api,
                &WorkspaceId::from("ordinary"),
                Some(&AgentId::from(CALLER)),
                method,
                &target_args(TARGET),
            )
            .await
            .unwrap_err();
            assert!(
                err.contains("only available in the Assistant workspace"),
                "{method}: {err}"
            );
        }
        assert!(fake.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn resolved_target_must_be_a_live_assistant_chat_before_read_or_mutation() {
        let api = fixture();
        api.agents.lock().unwrap().extend([
            chat("foreign", &WorkspaceId::from("ordinary"), AgentStatus::Idle),
            chat("deleted", &WorkspaceId::chief(), AgentStatus::Deleted),
        ]);
        for id in ["foreign", "deleted", "missing"] {
            for method in [
                "readConversation",
                "getMessageBlock",
                "rename",
                "delete",
                "cancelDelete",
            ] {
                assert!(
                    call(&api, method, target_args(id)).await.is_err(),
                    "{method} accepted {id}"
                );
            }
        }
        assert!(api
            .calls
            .lock()
            .unwrap()
            .iter()
            .all(|c| c["method"] == "get" && c["workspaceId"] == "__chief__"));
    }

    #[tokio::test]
    async fn list_is_recent_first_bounded_and_metadata_only() {
        let api = fixture();
        {
            let mut agents = api.agents.lock().unwrap();
            agents[0].last_activity = Some("2026-10-11T00:00:03Z".to_string());
            agents[1].updated_at = "2026-10-11T00:00:02Z".to_string();
            agents[1].pending_delete_at = Some(DEADLINE.to_string());
            agents.extend([
                chat("deleted", &WorkspaceId::chief(), AgentStatus::Deleted),
                chat("foreign", &WorkspaceId::from("ordinary"), AgentStatus::Idle),
            ]);
        }
        let first = call(
            &api,
            "list",
            json!({ "limit": 1, "workspaceId": "ordinary" }),
        )
        .await
        .unwrap();
        assert_eq!(first["total"], 2);
        assert_eq!(first["returned"], 1);
        assert_eq!(first["nextCursor"], 1);
        assert_eq!(first["chats"][0]["agentId"], CALLER);
        assert_eq!(first["chats"][0]["isCurrent"], true);
        assert!(first["chats"][0].get("pendingDeleteAt").is_none());
        let second = call(
            &api,
            "list",
            json!({ "cursor": first["nextCursor"], "limit": 1 }),
        )
        .await
        .unwrap();
        assert_eq!(second["chats"][0]["agentId"], TARGET);
        assert_eq!(second["chats"][0]["isCurrent"], false);
        assert_eq!(second["chats"][0]["pendingDeleteAt"], DEADLINE);
        assert!(second.get("nextCursor").is_none());
        assert!(!second.to_string().contains("notificationsMuted"));
        assert!(api
            .calls
            .lock()
            .unwrap()
            .iter()
            .all(|c| c["method"] == "list" && c["workspaceId"] == "__chief__"));

        api.agents.lock().unwrap().extend((0..210).map(|i| {
            chat(
                &format!("chat-{i}"),
                &WorkspaceId::chief(),
                AgentStatus::Idle,
            )
        }));
        assert_eq!(call(&api, "list", json!({})).await.unwrap()["returned"], 50);
        assert_eq!(
            call(&api, "list", json!({ "limit": 999 })).await.unwrap()["returned"],
            200
        );
        assert!(call(&api, "list", json!({ "limit": 0 })).await.is_err());
        assert!(call(&api, "list", json!({ "cursor": -1 })).await.is_err());
        let end = call(&api, "list", json!({ "cursor": 999 })).await.unwrap();
        assert_eq!(end["returned"], 0);
        assert!(end.get("nextCursor").is_none());
    }

    #[tokio::test]
    async fn rename_other_chat_trims_persists_and_propagates_errors() {
        let api = fixture();
        let out = call(
            &api,
            "rename",
            json!({ "agentId": TARGET, "name": "  Research notes  " }),
        )
        .await
        .unwrap();
        assert_eq!(out, json!({ "success": true, "name": "Research notes" }));
        let agents = api.agents.lock().unwrap().clone();
        assert_eq!(agents[0].name, CALLER);
        assert_eq!(agents[1].name, "Research notes");
        let calls = api.calls.lock().unwrap().clone();
        assert_eq!(
            calls[1],
            json!({ "method": "rename", "agentId": TARGET, "name": "Research notes", "skipIfExplicitlySet": false })
        );
        for name in [json!(" \n\t "), Value::Null, json!(7)] {
            assert!(
                call(&api, "rename", json!({ "agentId": TARGET, "name": name }))
                    .await
                    .is_err()
            );
        }
        assert_eq!(api.calls.lock().unwrap().len(), 2);
        let error_api = Arc::new(FakeApi {
            agents: Mutex::new(agents),
            rename_error: true,
            ..Default::default()
        });
        let err = call(&error_api, "rename", target_args(TARGET))
            .await
            .unwrap_err();
        assert!(err.contains("rename rejected"), "{err}");
    }

    #[tokio::test]
    async fn delete_requires_caller_and_rejects_current_chat() {
        let fake = fixture();
        let api: Arc<dyn WorkspaceApi> = fake.clone();
        let err = dispatch(
            &api,
            &WorkspaceId::chief(),
            None,
            "delete",
            &target_args(TARGET),
        )
        .await
        .unwrap_err();
        assert!(err.contains("No agent context"), "{err}");
        let err = call(&fake, "delete", target_args(CALLER))
            .await
            .unwrap_err();
        assert!(err.contains("current Assistant chat"), "{err}");
        assert!(fake.calls.lock().unwrap().is_empty());
        let list = dispatch(&api, &WorkspaceId::chief(), None, "list", &json!({}))
            .await
            .unwrap();
        assert!(list["chats"]
            .as_array()
            .unwrap()
            .iter()
            .all(|chat| chat["isCurrent"] == false));
        assert!(dispatch(
            &api,
            &WorkspaceId::chief(),
            None,
            "readConversation",
            &target_args(TARGET)
        )
        .await
        .is_ok());
    }

    #[tokio::test]
    async fn delete_schedules_grace_and_cancel_reports_whether_pending() {
        let api = fixture();
        assert_eq!(
            call(&api, "cancelDelete", target_args(TARGET))
                .await
                .unwrap(),
            json!({ "cancelled": false })
        );
        let out = call(
            &api,
            "delete",
            json!({ "agentId": TARGET, "undoDelayMs": 0 }),
        )
        .await
        .unwrap();
        assert_eq!(
            out,
            json!({ "success": true, "scheduled": true, "deleteAt": DEADLINE })
        );
        let calls = api.calls.lock().unwrap().clone();
        assert_eq!(
            calls[3],
            json!({ "method": "schedule", "agentId": TARGET, "workspaceId": "__chief__", "undoDelayMs": 15_000 })
        );
        assert_eq!(
            call(&api, "cancelDelete", target_args(TARGET))
                .await
                .unwrap(),
            json!({ "cancelled": true })
        );
        assert_eq!(
            call(&api, "cancelDelete", target_args(TARGET))
                .await
                .unwrap(),
            json!({ "cancelled": false })
        );
    }

    #[tokio::test]
    async fn conversation_keeps_bounded_slim_paging_and_scrubs_hidden_fields() {
        let api = fixture();
        let out = call(
            &api,
            "readConversation",
            json!({ "agentId": TARGET, "workspaceId": "ordinary", "lastN": 300 }),
        )
        .await
        .unwrap();
        assert_eq!(out["workspaceId"], "__chief__");
        assert_eq!(out["totalMessages"], 4);
        assert_eq!(out["returnedMessages"], 4);
        assert_eq!(out["startTurn"], 1);
        assert_eq!(out["endTurn"], 4);
        assert_eq!(out["includeToolCalls"], false);
        assert_eq!(out["messages"][0]["id"], "m-0");
        assert_eq!(out["messages"][3]["id"], "m-3");
        assert_eq!(
            out["messages"][0]["contentBlocks"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert!(!out.to_string().contains("notificationsMuted"));
        let calls = api.calls.lock().unwrap().clone();
        let reads: Vec<_> = calls
            .iter()
            .filter(|c| c["method"] == "conversation")
            .collect();
        assert_eq!(reads.len(), 2);
        assert_eq!(reads[0]["limit"], 100);
        assert_eq!(reads[1]["pageToken"], "2");
        assert!(reads.iter().all(|c| c["workspaceId"] == "__chief__"));
        let range = call(
            &api,
            "readConversation",
            json!({ "agentId": TARGET, "startTurn": 1, "endTurn": 2, "includeToolCalls": true }),
        )
        .await
        .unwrap();
        assert_eq!(range["startTurn"], 1);
        assert_eq!(range["endTurn"], 2);
        assert_eq!(range["returnedMessages"], 2);
        assert_eq!(
            range["messages"][0]["contentBlocks"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
    }

    #[tokio::test]
    async fn message_block_reads_one_full_block_and_scrubs_hidden_fields() {
        let api = fixture();
        let out = call(&api, "getMessageBlock", target_args(TARGET))
            .await
            .unwrap();
        assert_eq!(
            out,
            json!({ "messageId": "m-0", "block": { "id": "b-0", "type": "text", "text": "full body" } })
        );
        assert_eq!(
            api.calls.lock().unwrap()[1],
            json!({ "method": "block", "agentId": TARGET, "workspaceId": "__chief__", "messageId": "m-0", "blockId": "b-0" })
        );
    }
}
