//! End-to-end tests for self-correcting `ws.*` errors (brief M4): real
//! invented names from history and a bad-argument call go through the full
//! `workspace_api` tool path (prelude, `QuickJS`, host dispatch).

use std::sync::Arc;

use intent_core::settings_file::AgentFeaturesSettings;
use intent_core::{AgentId, BoxFuture, Result, WorkspaceApi, WorkspaceId};
use serde_json::{json, Value};

use crate::WorkspaceMcpServer;

struct FakeApi;

impl WorkspaceApi for FakeApi {
    fn settings_get(&self, path: String) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            let value = match path.as_str() {
                "workspaceApi.toonOutput" => json!(false),
                "workspaceApi.maxOutputChars" => json!(0),
                _ => Value::Null,
            };
            Ok(json!({ "path": path, "value": value }))
        })
    }

    fn git_root_list(&self, _ws: WorkspaceId) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async { Ok(json!([])) })
    }
}

fn server(features: AgentFeaturesSettings) -> WorkspaceMcpServer {
    WorkspaceMcpServer::new(Arc::new(FakeApi), WorkspaceId::from_string("ws-suggest"))
        .with_caller_agent_id(Some(AgentId::from("agent-1")))
        .with_agent_features(features)
}

async fn call(server: &WorkspaceMcpServer, code: &str) -> Value {
    let response = server
        .handle_message(&json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": { "name": "workspace_api", "arguments": {
                "code": code, "summary": "self-correcting error test",
            } },
        }))
        .await
        .unwrap();
    response["result"].clone()
}

async fn error_text(code: &str) -> String {
    let result = call(&server(AgentFeaturesSettings::default()), code).await;
    assert_eq!(result["isError"], true, "{code}: {result}");
    result["content"][0]["text"].as_str().unwrap().to_string()
}

async fn ok_text(server: &WorkspaceMcpServer, code: &str) -> String {
    let result = call(server, code).await;
    assert_eq!(result["isError"], false, "{code}: {result}");
    result["content"][0]["text"].as_str().unwrap().to_string()
}

#[tokio::test]
async fn invented_names_from_history_point_at_the_real_method_and_signature() {
    for (code, wrong, signature, help) in [
        (
            "return await ws.agent.sendMessage('agent-2', 'hi')",
            "ws.agent.sendMessage",
            "ws.agent.send(agentId, message, priority?) →",
            "ws.help(\"agent\")",
        ),
        (
            "return await ws.git.roots()",
            "ws.git.roots",
            "ws.git.listRoots() →",
            "ws.help(\"git\")",
        ),
        (
            "return await ws.note.get('spec')",
            "ws.note.get",
            "ws.note.read(id, { startLine?, maxLines? }?) →",
            "ws.help(\"note\")",
        ),
        (
            "return await ws.workspace.getDetails()",
            "ws.workspace.getDetails",
            "ws.workspace.details() →",
            "ws.help(\"workspace\")",
        ),
    ] {
        let text = error_text(code).await;
        assert!(
            text.contains(&format!("{wrong} is not a function")),
            "{text}"
        );
        assert!(text.contains(signature), "{code}: {text}");
        assert!(text.contains(help), "{code}: {text}");
        assert!(!text.contains("host: unknown method"), "{text}");
    }
}

#[tokio::test]
async fn unknown_namespace_points_at_the_closest_namespace() {
    let text = error_text("return await ws.notes.read('spec')").await;
    assert!(text.contains("ws.notes"), "{text}");
    assert!(text.contains("ws.note.read(id"), "{text}");
}

#[tokio::test]
async fn method_in_another_namespace_is_found() {
    let text = error_text("return await ws.workspace.commit('feat: x')").await;
    assert!(text.contains("ws.git.commit(message"), "{text}");
}

#[tokio::test]
async fn no_close_match_lists_the_namespace_methods() {
    let text = error_text("return await ws.git.frobnicateEverything()").await;
    assert!(text.contains("listRoots"), "{text}");
    assert!(text.contains("ws.help(\"git\")"), "{text}");
}

#[tokio::test]
async fn missing_argument_returns_the_signature() {
    let text = error_text("return await ws.note.read()").await;
    assert!(text.contains("Note ID is required"), "{text}");
    assert!(
        text.contains("ws.note.read(id, { startLine?, maxLines? }?) →"),
        "{text}"
    );
}

#[tokio::test]
async fn non_argument_errors_get_no_signature() {
    let text = error_text("return await ws.note.read('spec')").await;
    assert!(!text.contains("ws.note.read(id"), "{text}");
}

#[tokio::test]
async fn valid_calls_and_feature_detection_are_unchanged() {
    let features = AgentFeaturesSettings {
        browser_automation: false,
        ..AgentFeaturesSettings::default()
    };
    let server = server(features);
    assert_eq!(
        ok_text(&server, "return await ws.git.listRoots()").await,
        "[]"
    );
    let probe = ok_text(
        &server,
        "return [ws.browser === undefined, typeof ws.note.read, Object.keys(ws.git).join(','), \
         JSON.stringify(ws.git) !== undefined, (await Promise.resolve(ws.git)) === ws.git, \
         'nope' in ws.git, ws.note.then === undefined]",
    )
    .await;
    let probe: Value = serde_json::from_str(&probe).unwrap();
    assert_eq!(
        probe,
        json!([
            true,
            "function",
            "commit,registerRoot,unregisterRoot,listRoots",
            true,
            true,
            false,
            true
        ])
    );
}
