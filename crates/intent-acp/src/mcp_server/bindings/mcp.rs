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
    ws: &WorkspaceId,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    match method {
        "listServers" => api
            .mcp_list_servers(Some(ws.clone()))
            .await
            .map_err(map_err),
        "listTools" => {
            let server_id = req_str(args, "serverId")?;
            api.mcp_list_tools(server_id, Some(ws.clone()))
                .await
                .map_err(map_err)
        }
        "callTool" => call_tool(api, ws, args).await,
        other => Err(format!("host: unknown method `mcp.{other}`")),
    }
}

async fn call_tool(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let server_id = req_str(args, "serverId")?;
    let tool_name = req_str(args, "toolName")?;
    let tool_args = match args.get("args") {
        None | Some(Value::Null) => json!({}),
        Some(v) => v.clone(),
    };
    // `opt_i64` returns `None` for fractional values, so the presence check
    // keeps a bad `timeoutMs` from silently falling back to the default.
    let timeout_ms = match args.get("timeoutMs") {
        None | Some(Value::Null) => None,
        Some(_) => match opt_i64(args, "timeoutMs") {
            Some(ms) if ms > 0 => Some(ms.unsigned_abs()),
            _ => return Err("timeoutMs must be a positive integer".to_string()),
        },
    };
    api.mcp_call_tool(
        server_id,
        tool_name,
        tool_args,
        timeout_ms,
        Some(ws.clone()),
    )
    .await
    .map_err(map_err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::{BoxFuture, Result};
    use std::sync::Mutex;

    /// `WorkspaceApi` whose `mcp_call_tool` records the timeouts it received,
    /// so the tests can pin the binding's `timeoutMs` validation.
    #[derive(Default)]
    struct FakeApi {
        seen_timeouts: Mutex<Vec<Option<u64>>>,
    }

    impl WorkspaceApi for FakeApi {
        fn mcp_call_tool(
            &self,
            _server_id: String,
            _tool_name: String,
            _args: Value,
            timeout_ms: Option<u64>,
            _workspace_id: Option<WorkspaceId>,
        ) -> BoxFuture<'_, Result<Value>> {
            self.seen_timeouts.lock().unwrap().push(timeout_ms);
            Box::pin(async move { Ok(json!({ "content": [] })) })
        }
    }

    async fn call(
        api: &Arc<dyn WorkspaceApi>,
        timeout: Option<Value>,
    ) -> std::result::Result<Value, String> {
        let mut args = json!({ "serverId": "s1", "toolName": "t1" });
        if let Some(t) = timeout {
            args["timeoutMs"] = t;
        }
        let ws = WorkspaceId("ws-test".to_string());
        call_tool(api, &ws, &args).await
    }

    #[tokio::test]
    async fn timeout_omitted_forwards_none() {
        let fake = Arc::new(FakeApi::default());
        let api: Arc<dyn WorkspaceApi> = fake.clone();
        call(&api, None).await.unwrap();
        assert_eq!(*fake.seen_timeouts.lock().unwrap(), vec![None]);
    }

    #[tokio::test]
    async fn timeout_positive_integer_forwards() {
        let fake = Arc::new(FakeApi::default());
        let api: Arc<dyn WorkspaceApi> = fake.clone();
        call(&api, Some(json!(1500))).await.unwrap();
        assert_eq!(*fake.seen_timeouts.lock().unwrap(), vec![Some(1500)]);
    }

    #[tokio::test]
    async fn timeout_zero_negative_and_fractional_rejected() {
        let fake = Arc::new(FakeApi::default());
        let api: Arc<dyn WorkspaceApi> = fake.clone();
        for bad in [json!(0), json!(-5), json!(1.5), json!("nope")] {
            let err = call(&api, Some(bad.clone())).await.unwrap_err();
            assert!(err.contains("positive integer"), "{bad}: {err}");
        }
        assert!(fake.seen_timeouts.lock().unwrap().is_empty());
    }
}
