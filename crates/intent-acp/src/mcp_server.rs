//! In-process agent→BE MCP server (§6.8) — the key orchestration loop.
//!
//! Exposes the workspace tools over the SAME `Arc<dyn WorkspaceApi>` the FE's
//! JSON-RPC router uses ("one impl, two front doors"). Agents reach these tools
//! via MCP (`tools/list` / `tools/call`); the per-agent-type denylist (§18.4) is
//! enforced internally here while filtering the exposed set — there is no
//! `agent.getAvailableTools` wire method.

use std::collections::HashSet;
use std::sync::Arc;

use intent_core::{AgentId, WorkspaceApi, WorkspaceId};
use serde_json::{json, Value};

use crate::tool_restrictions::get_tool_denylist_for_agent_type;

mod dispatch;
mod tools;

pub use tools::ToolDef;

/// Protocol version advertised on `initialize` (matches the TS server).
pub const MCP_PROTOCOL_VERSION: &str = "2024-11-05";

/// The agent→BE MCP server: a fixed workspace context, the shared service
/// surface, and the set of tool names denied for this agent's type.
pub struct WorkspaceMcpServer {
    api: Arc<dyn WorkspaceApi>,
    workspace_id: WorkspaceId,
    denylist: HashSet<String>,
    name: String,
    version: String,
    /// The agent this server front-doors for. Caller-aware tools (e.g.
    /// `delegate_task`) attribute their actions to this id so children can be
    /// stamped with their spawning parent. `None` for the FE/RPC front door.
    caller_agent_id: Option<AgentId>,
}

impl WorkspaceMcpServer {
    /// Build a server with no tool restrictions (interactive/foreground agents).
    pub fn new(api: Arc<dyn WorkspaceApi>, workspace_id: WorkspaceId) -> Self {
        Self {
            api,
            workspace_id,
            denylist: HashSet::new(),
            name: "workspace-mcp".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            caller_agent_id: None,
        }
    }

    /// Build a server whose exposed tool set has the §18.4 denylist for
    /// `agent_type` removed (the spawn-time enforcement point).
    pub fn for_agent_type(
        api: Arc<dyn WorkspaceApi>,
        workspace_id: WorkspaceId,
        agent_type: &str,
    ) -> Self {
        let denylist = get_tool_denylist_for_agent_type(agent_type)
            .into_iter()
            .map(|s| s.to_string())
            .collect();
        Self {
            denylist,
            ..Self::new(api, workspace_id)
        }
    }

    /// Override the denied tool names directly (testing / custom policies).
    pub fn with_denylist<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.denylist = names.into_iter().map(Into::into).collect();
        self
    }

    /// Set the calling agent this server front-doors for (the spawn-time wiring
    /// point). Caller-aware tools attribute their actions to this id.
    pub fn with_caller_agent_id(mut self, caller: Option<AgentId>) -> Self {
        self.caller_agent_id = caller;
        self
    }

    /// Whether `name` is denied for this agent.
    pub fn is_denied(&self, name: &str) -> bool {
        self.denylist.contains(name)
    }

    /// The tool definitions exposed to this agent (full registry minus denylist).
    pub fn available_tools(&self) -> Vec<&'static ToolDef> {
        tools::all_tools()
            .iter()
            .filter(|t| !self.denylist.contains(t.name))
            .collect()
    }

    /// Handle one MCP JSON-RPC message. Returns `Some(response)` for requests and
    /// `None` for notifications (port of `MCPServer.handleMessage`).
    pub async fn handle_message(&self, message: &Value) -> Option<Value> {
        let method = message.get("method").and_then(Value::as_str)?;
        let id = message.get("id").cloned();
        match id {
            Some(id) => Some(self.handle_request(&id, method, message).await),
            None => None,
        }
    }

    async fn handle_request(&self, id: &Value, method: &str, message: &Value) -> Value {
        match method {
            "initialize" => ok(id, self.initialize_result()),
            "tools/list" => ok(id, self.list_tools_result()),
            "tools/call" => self.call_tool(id, message.get("params")).await,
            other => err(id, -32601, &format!("Unknown method: {other}")),
        }
    }

    fn initialize_result(&self) -> Value {
        json!({
            "protocolVersion": MCP_PROTOCOL_VERSION,
            "capabilities": { "tools": { "listChanged": false } },
            "serverInfo": { "name": self.name, "version": self.version },
        })
    }

    fn list_tools_result(&self) -> Value {
        let tools: Vec<Value> = self
            .available_tools()
            .iter()
            .map(|t| {
                json!({
                    "name": t.name,
                    "description": t.description,
                    "inputSchema": t.schema(),
                })
            })
            .collect();
        json!({ "tools": tools })
    }

    async fn call_tool(&self, id: &Value, params: Option<&Value>) -> Value {
        let params = params.cloned().unwrap_or_else(|| json!({}));
        let Some(name) = params.get("name").and_then(Value::as_str) else {
            return err(id, -32602, "Missing tool name");
        };
        if self.denylist.contains(name) {
            return err(id, -32602, &format!("Tool not available: {name}"));
        }
        if !tools::all_tools().iter().any(|t| t.name == name) {
            return err(id, -32602, &format!("Tool not found: {name}"));
        }
        let args = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        match self.dispatch(name, &args).await {
            Ok(value) => ok(id, tool_content(&value)),
            Err(e) => err(id, e.code(), &e.to_string()),
        }
    }
}

fn ok(id: &Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

fn err(id: &Value, code: i32, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

fn tool_content(value: &Value) -> Value {
    let text = serde_json::to_string(value).unwrap_or_else(|_| "null".to_string());
    json!({ "content": [{ "type": "text", "text": text }], "isError": false })
}
