//! `ws.mcp.*` bindings — forward tool requests to the user-configured
//! external MCP servers (the §18.3 hub).
//!
//! Thin wrappers over the [`WorkspaceApi`] agent-facing MCP surface: server
//! discovery (non-sensitive projection only — `env`/`headers`/`command`
//! never appear), `tools/list` and `tools/call` forwarding. The service
//! layer enforces the settings gates live per call (`agentFeatures.mcpTools`,
//! `mcp.enableUserServers`, per-server disabled state); the prelude/dispatch
//! gating in [`super`] and `super::super::tools` is defense in depth on top.

use std::sync::Arc;

use intent_core::{WorkspaceApi, WorkspaceId};
use serde_json::{json, Value};

use super::{map_err, opt_i64, req_str};

pub(crate) const PRELUDE: &str = r"
    globalThis.ws = globalThis.ws || {};
    ws.mcp = {
        listServers: () => host({ method: 'mcp.listServers', args: {} }),
        listTools: (serverId) => host({ method: 'mcp.listTools', args: { serverId } }),
        callTool: (serverId, toolName, args, timeoutMs) =>
            host({ method: 'mcp.callTool', args: { serverId, toolName, args, timeoutMs } }),
    };
";

pub(crate) async fn dispatch(
    api: &Arc<dyn WorkspaceApi>,
    _ws: &WorkspaceId,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    match method {
        "listServers" => api.mcp_list_servers().await.map_err(map_err),
        "listTools" => {
            let server_id = req_str(args, "serverId")?;
            api.mcp_list_tools(server_id).await.map_err(map_err)
        }
        "callTool" => call_tool(api, args).await,
        other => Err(format!("host: unknown method `mcp.{other}`")),
    }
}

async fn call_tool(api: &Arc<dyn WorkspaceApi>, args: &Value) -> Result<Value, String> {
    let server_id = req_str(args, "serverId")?;
    let tool_name = req_str(args, "toolName")?;
    let tool_args = match args.get("args") {
        None | Some(Value::Null) => json!({}),
        Some(v) => v.clone(),
    };
    let timeout_ms = match opt_i64(args, "timeoutMs") {
        Some(ms) => Some(
            u64::try_from(ms)
                .map_err(|_| "timeoutMs must be a non-negative integer".to_string())?,
        ),
        None => None,
    };
    api.mcp_call_tool(server_id, tool_name, tool_args, timeout_ms)
        .await
        .map_err(map_err)
}
