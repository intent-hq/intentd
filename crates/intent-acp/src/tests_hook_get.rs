//! `ws.hook.get` through the real `workspace_api` JS engine: a hook whose
//! `code` is over the read budget arrives from the host in parts
//! (`codeOffset` / `nextCodeOffset`) and the binding joins them.

#![cfg(test)]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use intent_core::{BoxFuture, HookId, Result, WorkspaceApi, WorkspaceId};
use serde_json::{json, Value};

use crate::WorkspaceMcpServer;

/// One hook whose code a test can change between host calls.
struct HookApi {
    code: Mutex<String>,
    /// Swap in this code after the given number of `hook_get` calls.
    swap: Mutex<Option<(usize, String)>>,
    reads: AtomicUsize,
}

impl WorkspaceApi for HookApi {
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

    fn hook_get(&self, _ws: WorkspaceId, hook_id: HookId) -> BoxFuture<'_, Result<Value>> {
        let n = self.reads.fetch_add(1, Ordering::SeqCst) + 1;
        if let Some((after, next)) = self.swap.lock().unwrap().clone() {
            if n > after {
                *self.code.lock().unwrap() = next;
            }
        }
        let code = self.code.lock().unwrap().clone();
        Box::pin(async move {
            Ok(json!({ "hookId": hook_id.0, "name": "big", "status": "scheduled", "code": code }))
        })
    }
}

fn server(code: String, swap: Option<(usize, String)>) -> (WorkspaceMcpServer, Arc<HookApi>) {
    let api = Arc::new(HookApi {
        code: Mutex::new(code),
        swap: Mutex::new(swap),
        reads: AtomicUsize::new(0),
    });
    let srv = WorkspaceMcpServer::new(api.clone(), WorkspaceId::from_string("ws"));
    (srv, api)
}

async fn call(srv: &WorkspaceMcpServer, code: &str) -> (bool, String) {
    let resp = srv
        .handle_message(&json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {
                "name": "workspace_api",
                "arguments": { "code": code, "summary": "hook get test" }
            }
        }))
        .await
        .expect("tools/call must produce a response");
    let is_error = resp["result"]["isError"].as_bool().unwrap_or(false);
    let text = resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    (is_error, text)
}

fn big_code() -> String {
    format!(
        "return {{ dispatch: false }}; // {}é😀{}",
        "\"".repeat(300_001),
        "\\é".repeat(150_000)
    )
}

#[tokio::test]
async fn hook_get_joins_an_oversized_code_exactly() {
    let code = big_code();
    let (srv, api) = server(code.clone(), None);
    let (is_error, text) = call(
        &srv,
        "const h = await ws.hook.get('h1'); \
         return { code: h.code, keys: Object.keys(h).filter(k => k.startsWith('code') || k.startsWith('next')) };",
    )
    .await;
    assert!(!is_error, "{}", &text[..text.len().min(300)]);
    let body: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(body["code"].as_str().unwrap(), code);
    assert_eq!(body["keys"], json!(["code"]), "paging markers removed");
    assert!(api.reads.load(Ordering::SeqCst) >= 3, "read in parts");
}

#[tokio::test]
async fn hook_get_restarts_when_the_code_changes_between_parts() {
    let first = big_code();
    let second = format!("{first}// edited");
    let (srv, _api) = server(first, Some((1, second.clone())));
    let (is_error, text) = call(&srv, "return (await ws.hook.get('h1')).code;").await;
    assert!(!is_error, "{}", &text[..text.len().min(300)]);
    let body: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(body.as_str().unwrap(), second);
}

#[tokio::test]
async fn hook_get_host_part_pages_and_rejects_bad_code_offsets() {
    let api: Arc<dyn WorkspaceApi> = Arc::new(HookApi {
        code: Mutex::new("aé".into()),
        swap: Mutex::new(None),
        reads: AtomicUsize::new(0),
    });
    let ws = WorkspaceId::from_string("ws");
    let get = |args: Value| {
        let api = api.clone();
        let ws = ws.clone();
        async move { crate::mcp_server::bindings::hook::dispatch(&api, &ws, None, "get", &args).await }
    };
    for bad in [json!(2), json!(3), json!(9), json!(-1), json!("1")] {
        let err = get(json!({ "hookId": "h1", "codeOffset": bad }))
            .await
            .unwrap_err();
        assert!(err.contains("codeOffset"), "{bad}: {err}");
    }
    let part = get(json!({ "hookId": "h1", "codeOffset": 1 }))
        .await
        .unwrap();
    assert_eq!(part["code"], "é");
    assert_eq!(part["codeTruncated"], true);
    assert_eq!(part["codeBytes"], 3);
    assert_eq!(part["nextCodeOffset"], Value::Null);
    let whole = get(json!({ "hookId": "h1" })).await.unwrap();
    assert_eq!(whole["code"], "aé");
    assert!(whole.get("codeTruncated").is_none());
}
