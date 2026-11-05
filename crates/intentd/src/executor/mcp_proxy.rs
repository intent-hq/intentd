//! The user MCP servers a remote agent's provider sees (nodeProtocol 11).
//!
//! Head names each server the agent would get locally (`LaunchCommand`'s
//! `mcpProxies`: name and server id only). For each one the node serves a
//! small MCP server on its own loopback listener, which the provider reaches
//! through `intentd mcp-bridge` as it reaches `workspace-mcp`. It answers
//! `initialize` and `ping` itself and sends `tools/list` and `tools/call` to
//! head, whose MCP hub runs them; the server's configuration and secrets stay
//! on head. Only tools are proxied.

use std::sync::Arc;

use serde_json::{json, Value};

use intent_core::node_link::McpProxy;
use intent_core::remote_launch::workspace_api::{
    MCP_PROXY_CALL_TOOL_METHOD, MCP_PROXY_LIST_TOOLS_METHOD,
};
use intent_core::BoxFuture;

/// Sends one internal `mcp.proxy.*` frame to head for the agent.
pub(super) type Forward =
    Arc<dyn Fn(&'static str, Value) -> BoxFuture<'static, Result<Value, String>> + Send + Sync>;

/// The MCP protocol version the proxy answers when the provider names none.
const DEFAULT_PROTOCOL_VERSION: &str = "2025-06-18";

const METHOD_NOT_FOUND: i64 = -32601;
const INVALID_PARAMS: i64 = -32602;
const INTERNAL_ERROR: i64 = -32603;

/// Answer one MCP message the provider sent to `proxy`'s server; `None` for
/// a notification.
pub(super) async fn handle(proxy: &McpProxy, forward: &Forward, message: Value) -> Option<Value> {
    let id = message.get("id").filter(|id| !id.is_null())?.clone();
    let method = message.get("method").and_then(Value::as_str).unwrap_or("");
    let params = message.get("params").cloned().unwrap_or(Value::Null);
    let outcome = match method {
        "initialize" => Ok(json!({
            "protocolVersion": params
                .get("protocolVersion")
                .and_then(Value::as_str)
                .unwrap_or(DEFAULT_PROTOCOL_VERSION),
            "capabilities": { "tools": {} },
            "serverInfo": { "name": proxy.name, "version": env!("CARGO_PKG_VERSION") },
        })),
        "ping" => Ok(json!({})),
        "tools/list" => forward(
            MCP_PROXY_LIST_TOOLS_METHOD,
            json!({ "serverId": proxy.server_id }),
        )
        .await
        .map_err(|message| (INTERNAL_ERROR, message)),
        "tools/call" => match params.get("name").and_then(Value::as_str) {
            None => Err((
                INVALID_PARAMS,
                "tools/call needs a string `name`".to_owned(),
            )),
            Some(name) => {
                let arguments = match params.get("arguments") {
                    None | Some(Value::Null) => json!({}),
                    Some(arguments) => arguments.clone(),
                };
                let called = forward(
                    MCP_PROXY_CALL_TOOL_METHOD,
                    json!({ "serverId": proxy.server_id, "name": name, "arguments": arguments }),
                )
                .await;
                // A failed call is a tool result the agent can read, as a
                // server reports its own tool errors.
                Ok(called.unwrap_or_else(|message| {
                    json!({ "content": [{ "type": "text", "text": message }], "isError": true })
                }))
            }
        },
        other => Err((
            METHOD_NOT_FOUND,
            format!("`{other}` is not available: this server proxies the backend's MCP tools only"),
        )),
    };
    Some(match outcome {
        Ok(result) => json!({ "jsonrpc": "2.0", "id": id, "result": result }),
        Err((code, message)) => json!({
            "jsonrpc": "2.0",
            "id": id,
            "error": { "code": code, "message": message },
        }),
    })
}

/// `proxy`'s MCP server as a bridge handler.
pub(super) fn handler(proxy: McpProxy, forward: Forward) -> intent_acp::McpHandler {
    let proxy = Arc::new(proxy);
    Arc::new(move |message| {
        let proxy = proxy.clone();
        let forward = forward.clone();
        Box::pin(async move { handle(&proxy, &forward, message).await })
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use super::*;

    fn proxy() -> McpProxy {
        McpProxy {
            name: "Linear".into(),
            server_id: "srv-1".into(),
        }
    }

    /// A forward that records its frames and answers with `answer`.
    fn recording(
        answer: Result<Value, String>,
    ) -> (Forward, Arc<Mutex<Vec<(&'static str, Value)>>>) {
        let sent = Arc::new(Mutex::new(Vec::new()));
        let log = sent.clone();
        let forward: Forward = Arc::new(move |method, args| {
            log.lock().unwrap().push((method, args));
            let answer = answer.clone();
            Box::pin(async move { answer })
        });
        (forward, sent)
    }

    #[tokio::test]
    async fn initialize_and_ping_are_answered_on_the_node_with_tools_only() {
        let (forward, sent) = recording(Ok(json!({})));
        let init = handle(
            &proxy(),
            &forward,
            json!({ "jsonrpc": "2.0", "id": 1, "method": "initialize",
                    "params": { "protocolVersion": "2024-11-05" } }),
        )
        .await
        .unwrap();
        assert_eq!(init["result"]["protocolVersion"], "2024-11-05");
        assert_eq!(init["result"]["capabilities"], json!({ "tools": {} }));
        assert_eq!(init["result"]["serverInfo"]["name"], "Linear");
        let ping = handle(&proxy(), &forward, json!({ "id": "p", "method": "ping" }))
            .await
            .unwrap();
        assert_eq!(ping, json!({ "jsonrpc": "2.0", "id": "p", "result": {} }));
        assert!(handle(
            &proxy(),
            &forward,
            json!({ "method": "notifications/initialized" })
        )
        .await
        .is_none());
        assert!(sent.lock().unwrap().is_empty(), "nothing reached head");
    }

    #[tokio::test]
    async fn tools_go_to_head_by_server_id() {
        let (forward, sent) = recording(Ok(json!({ "tools": [{ "name": "echo" }] })));
        let listed = handle(
            &proxy(),
            &forward,
            json!({ "id": 2, "method": "tools/list" }),
        )
        .await
        .unwrap();
        assert_eq!(listed["result"]["tools"][0]["name"], "echo");
        handle(
            &proxy(),
            &forward,
            json!({ "id": 3, "method": "tools/call",
                    "params": { "name": "echo", "arguments": { "input": "x" } } }),
        )
        .await
        .unwrap();
        handle(
            &proxy(),
            &forward,
            json!({ "id": 4, "method": "tools/call", "params": { "name": "echo" } }),
        )
        .await
        .unwrap();
        assert_eq!(
            *sent.lock().unwrap(),
            vec![
                (MCP_PROXY_LIST_TOOLS_METHOD, json!({ "serverId": "srv-1" })),
                (
                    MCP_PROXY_CALL_TOOL_METHOD,
                    json!({ "serverId": "srv-1", "name": "echo", "arguments": { "input": "x" } })
                ),
                (
                    MCP_PROXY_CALL_TOOL_METHOD,
                    json!({ "serverId": "srv-1", "name": "echo", "arguments": {} })
                ),
            ]
        );
    }

    /// A refused or failed call reaches the agent as a readable tool error;
    /// a failed listing is a JSON-RPC error; anything but tools is refused.
    #[tokio::test]
    async fn failures_and_other_methods() {
        let message = "mcp-proxy-unavailable: MCP server Linear is not running on the backend.";
        let (forward, sent) = recording(Err(message.to_owned()));
        let called = handle(
            &proxy(),
            &forward,
            json!({ "id": 1, "method": "tools/call", "params": { "name": "echo" } }),
        )
        .await
        .unwrap();
        assert_eq!(called["result"]["isError"], true);
        assert_eq!(called["result"]["content"][0]["text"], message);
        let listed = handle(
            &proxy(),
            &forward,
            json!({ "id": 2, "method": "tools/list" }),
        )
        .await
        .unwrap();
        assert_eq!(listed["error"]["code"], INTERNAL_ERROR);
        assert_eq!(listed["error"]["message"], message);
        let nameless = handle(
            &proxy(),
            &forward,
            json!({ "id": 3, "method": "tools/call", "params": {} }),
        )
        .await
        .unwrap();
        assert_eq!(nameless["error"]["code"], INVALID_PARAMS);
        for method in ["resources/list", "prompts/list", "sampling/createMessage"] {
            let refused = handle(&proxy(), &forward, json!({ "id": 9, "method": method }))
                .await
                .unwrap();
            assert_eq!(refused["error"]["code"], METHOD_NOT_FOUND, "{method}");
        }
        assert_eq!(sent.lock().unwrap().len(), 2, "only the two tool requests");
    }
}
