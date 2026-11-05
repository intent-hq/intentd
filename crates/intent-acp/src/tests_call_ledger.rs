//! Regression coverage for intent-hq/intent#8592: a `workspace_api` script
//! that runs an external command and then fails to record its result must
//! still report what the command did. Before the fix the script's rejection
//! replaced the whole tool result, so an agent saw only the database error
//! and could not tell that its `ws.host.exec` POST had already succeeded.

#![cfg(test)]

use std::sync::Arc;

use intent_core::{
    AgentId, BoxFuture, Error, NoteAddInput, NoteAddResult, NoteId, Result, Workspace,
    WorkspaceApi, WorkspaceId,
};
use serde_json::{json, Value};

use crate::WorkspaceMcpServer;

const CORRUPT: &str =
    "update note failed: error returned from database: (code: 267) database disk image is malformed";

/// `host_exec` runs the real one-shot exec under a temp workspace root;
/// `add_to_note` fails the way a corrupted store does.
struct FakeApi {
    root: tempfile::TempDir,
}

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

    fn get_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        let ws = Workspace {
            id,
            title: "Ledger".to_string(),
            branch: "main".to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            worktree_path: Some(self.root.path().to_string_lossy().into_owned()),
            ..Default::default()
        };
        Box::pin(async move { Ok(ws) })
    }

    fn host_exec(&self, workspace_id: WorkspaceId, params: Value) -> BoxFuture<'_, Result<Value>> {
        Box::pin(async move {
            intent_services::host_exec::run_for_workspace(self, workspace_id, params).await
        })
    }

    fn add_to_note(
        &self,
        _workspace_id: WorkspaceId,
        _note_id: NoteId,
        _input: NoteAddInput,
        _caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<NoteAddResult>> {
        Box::pin(async { Err(Error::Internal(CORRUPT.to_string())) })
    }
}

fn server() -> WorkspaceMcpServer {
    let root = tempfile::Builder::new()
        .prefix("intent-acp-ledger-")
        .tempdir()
        .unwrap();
    WorkspaceMcpServer::new(Arc::new(FakeApi { root }), WorkspaceId::from_string("ws-1"))
}

async fn call(srv: &WorkspaceMcpServer, code: &str) -> (bool, String) {
    let resp = srv
        .handle_message(&json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {
                "name": "workspace_api",
                "arguments": { "code": code, "summary": "call ledger test" }
            }
        }))
        .await
        .expect("tools/call must produce a response");
    let result = &resp["result"];
    (
        result["isError"].as_bool().unwrap(),
        result["content"][0]["text"].as_str().unwrap().to_string(),
    )
}

#[cfg(unix)]
#[tokio::test]
async fn failed_note_write_still_reports_the_completed_host_command() {
    let srv = server();
    let (is_error, text) = call(
        &srv,
        r#"const r = await ws.host.exec({ command: "echo", args: ["review-5771962102"] });
           await ws.note.add("task-1", { content: "receipt " + r.stdout });
           return r;"#,
    )
    .await;
    assert!(is_error, "{text}");
    assert!(text.contains(CORRUPT), "the original error stays: {text}");
    assert!(
        text.contains("1. ws.host.exec — succeeded (exitCode 0)"),
        "the command's outcome is reported: {text}"
    );
    assert!(
        text.contains("review-5771962102"),
        "the command's output is not lost: {text}"
    );
    assert!(
        text.contains("2. ws.note.add — failed: internal error: update note failed"),
        "the failing call is named: {text}"
    );
    assert!(
        text.contains("already took effect"),
        "the agent is warned not to repeat side effects: {text}"
    );
    assert!(
        text.contains("intentd doctor"),
        "a corrupted store gets an actionable diagnostic: {text}"
    );
}

/// The engine waits for in-flight calls before a rejection ends the script,
/// so a call is only left without a reply when the eval budget runs out.
#[cfg(unix)]
#[tokio::test]
async fn call_cut_off_by_the_script_timeout_is_reported_as_unknown() {
    let srv = server().with_workspace_api_timeout(std::time::Duration::from_secs(10));
    let (is_error, text) = call(
        &srv,
        r#"await ws.host.exec({ command: "echo", args: ["first"] });
           await ws.host.exec({ command: "sleep", args: ["30"] });"#,
    )
    .await;
    assert!(is_error, "{text}");
    assert!(text.contains("timed out"), "{text}");
    assert!(
        text.contains("1. ws.host.exec — succeeded (exitCode 0)"),
        "{text}"
    );
    assert!(
        text.contains("2. ws.host.exec — no reply before the script ended; outcome unknown"),
        "{text}"
    );
    assert!(!text.contains("intentd doctor"), "{text}");
}

#[cfg(unix)]
#[tokio::test]
async fn successful_or_caught_scripts_carry_no_ledger() {
    let srv = server();
    let (is_error, text) = call(
        &srv,
        r#"const r = await ws.host.exec({ command: "echo", args: ["ok"] });
           try { await ws.note.add("task-1", { content: "x" }); } catch (e) { return "caught"; }"#,
    )
    .await;
    assert!(!is_error, "{text}");
    assert!(!text.contains("ws.host.exec"), "{text}");
}

#[tokio::test]
async fn non_corruption_failures_get_no_database_hint() {
    let srv = server();
    let (is_error, text) = call(&srv, r#"throw new Error("plain failure");"#).await;
    assert!(is_error, "{text}");
    assert!(!text.contains("intentd doctor"), "{text}");
    assert!(!text.contains("ws.* calls"), "no calls, no ledger: {text}");
}
