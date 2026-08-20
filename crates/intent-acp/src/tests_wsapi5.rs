//! WSAPI-5 per-namespace bindings tests: `ws.workspace.*`, `ws.git.*`,
//! `ws.script.*`, `ws.terminal.*`, `ws.file.*`. Same shape as
//! `tests::wsapi3_bindings_tests` — each namespace is driven through the
//! real JS engine via the `workspace_api` MCP tool against a `FakeApi` that
//! stubs the trait methods the bindings touch. A happy-path and one error
//! path per namespace prove the JS→Rust dispatch and argument peel.

#![cfg(test)]

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use intent_core::{
    AgentId, AgentLite, AgentMetadata, AgentStatus, BoxFuture, Error, GitAgentCommitResult, NoteId,
    Result, SaveAssetResult, ScriptCreateParams, Workspace, WorkspaceActivity, WorkspaceApi,
    WorkspaceAttention, WorkspaceId, WorkspaceStatus, WorkspaceUpdate, CHIEF_WORKSPACE_ID,
};
use serde_json::{json, Value};

use crate::WorkspaceMcpServer;

#[derive(Default)]
struct FakeApi {
    /// Recorded `git_agent_commit` calls: (message, agent_id, user_requested).
    agent_commit_calls: Mutex<Vec<(String, Option<String>, bool)>>,
    script_list_calls: Mutex<u32>,
    script_create_calls: Mutex<Vec<ScriptCreateParams>>,
    script_start_calls: Mutex<Vec<String>>,
    script_output_calls: Mutex<Vec<(String, Option<i64>)>>,
    script_run_calls: Mutex<Vec<(String, Option<i64>)>>,
    terminal_list_calls: Mutex<u32>,
    terminal_read_calls: Mutex<Vec<(String, Option<i64>)>>,
    file_read_calls: Mutex<Vec<String>>,
    file_write_calls: Mutex<Vec<(String, String)>>,
    file_list_calls: Mutex<Vec<String>>,
    file_delete_calls: Mutex<Vec<String>>,
    file_mkdir_calls: Mutex<Vec<String>>,
    file_rename_calls: Mutex<Vec<(String, String)>>,
    update_calls: Mutex<Vec<WorkspaceUpdate>>,
    rename_calls: Mutex<Vec<(String, String, bool)>>,
    /// `(workspaceId, callerAgentId)` per `archive_workspace` call — the
    /// caller must ride along so the service-layer sweep can skip it.
    archive_calls: Mutex<Vec<(String, Option<String>)>>,
    unarchive_calls: Mutex<Vec<String>>,
    /// Agents returned by `agent_list` — seeds for the archive guardrail.
    agents: Mutex<Vec<AgentLite>>,
    workspace_variant: Mutex<WorkspaceVariant>,
    // `None` = no override (use the `make_workspace` default `Some("hi")`);
    // `Some(Some(x))` or `Some(None)` = the value the last `update_workspace`
    // call landed on after empty/whitespace-clear normalization.
    status_message_state: Mutex<Option<Option<String>>>,
    /// Recorded `save_asset` calls: (data, mime_type, original_name).
    save_asset_calls: Mutex<Vec<(String, String, Option<String>)>>,
    /// Recorded `git_root_register` calls: (path, agent_id).
    git_root_register_calls: Mutex<Vec<(String, String)>>,
    /// Recorded `git_root_unregister` calls: path.
    git_root_unregister_calls: Mutex<Vec<String>>,
    git_root_list_calls: Mutex<u32>,
}

#[derive(Clone, Copy, Default)]
enum WorkspaceVariant {
    #[default]
    Titled,
    AutoTitled,
    NotFound,
}

fn make_workspace(id: &str, variant: WorkspaceVariant) -> Workspace {
    let (title, worktree) = match variant {
        WorkspaceVariant::Titled => ("Hello".to_string(), Some("/tmp/hello".to_string())),
        WorkspaceVariant::AutoTitled => (id.to_string(), None),
        WorkspaceVariant::NotFound => unreachable!(),
    };
    Workspace {
        id: WorkspaceId::from_string(id),
        title,
        branch: "main".to_string(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: Some("hi".to_string()),
        status_image_asset_id: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: "2026-01-01T00:00:00Z".to_string(),
        updated_at: "2026-01-01T00:00:00Z".to_string(),
        last_activity: None,
        tags: vec!["red".to_string()],
        path: None,
        repository_path: None,
        repository_owner: None,
        repository_name: Some("intentd".to_string()),
        worktree_path: worktree,
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

impl WorkspaceApi for FakeApi {
    // Pin the `workspaceApi.*` output knobs to the legacy behavior (plain
    // pretty JSON, no size limit) so this fixture keeps asserting raw JSON
    // bodies; the TOON/limit paths are covered by
    // `workspace_api_output_limit_tests`.
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
        let variant = *self.workspace_variant.lock().unwrap();
        let override_sm = self.status_message_state.lock().unwrap().clone();
        Box::pin(async move {
            match variant {
                WorkspaceVariant::NotFound => {
                    Err(Error::NotFound(format!("workspace {}", id.as_str())))
                }
                v => {
                    let mut w = make_workspace(id.as_str(), v);
                    if let Some(sm) = override_sm {
                        w.status_message = sm;
                    }
                    Ok(w)
                }
            }
        })
    }

    fn update_workspace(
        &self,
        id: WorkspaceId,
        update: WorkspaceUpdate,
    ) -> BoxFuture<'_, Result<Workspace>> {
        self.update_calls.lock().unwrap().push(update.clone());
        if let Some(ref s) = update.status_message {
            // Mirror `intent-services::update_workspace`: an empty or
            // whitespace-only `status_message` clears to `None`. Persist the
            // normalized value so a follow-up `get_workspace` (i.e.
            // `ws.workspace.details()`) reflects the clear.
            let normalized = if s.trim().is_empty() {
                None
            } else {
                Some(s.clone())
            };
            *self.status_message_state.lock().unwrap() = Some(normalized);
        }
        let override_sm = self.status_message_state.lock().unwrap().clone();
        Box::pin(async move {
            let mut w = make_workspace(id.as_str(), WorkspaceVariant::Titled);
            if let Some(t) = update.title {
                w.title = t;
            }
            if let Some(sm) = override_sm {
                w.status_message = sm;
            }
            Ok(w)
        })
    }

    fn save_asset(
        &self,
        _workspace_id: WorkspaceId,
        data: String,
        mime_type: String,
        original_name: Option<String>,
    ) -> BoxFuture<'_, Result<SaveAssetResult>> {
        self.save_asset_calls
            .lock()
            .unwrap()
            .push((data, mime_type, original_name));
        Box::pin(async {
            Ok(SaveAssetResult {
                asset_id: "asset-123".to_string(),
                path: "/tmp/assets/asset-123.png".to_string(),
                url: "workspace-asset://asset-123".to_string(),
            })
        })
    }

    fn agent_rename(
        &self,
        agent_id: AgentId,
        name: String,
        skip_if_explicitly_set: bool,
    ) -> BoxFuture<'_, Result<Value>> {
        self.rename_calls.lock().unwrap().push((
            agent_id.as_str().to_string(),
            name.clone(),
            skip_if_explicitly_set,
        ));
        Box::pin(async move { Ok(json!({ "success": true, "name": name })) })
    }

    fn agent_list(&self, _workspace_id: WorkspaceId) -> BoxFuture<'_, Result<Vec<AgentLite>>> {
        let agents = self.agents.lock().unwrap().clone();
        Box::pin(async move { Ok(agents) })
    }

    fn archive_workspace(
        &self,
        id: WorkspaceId,
        caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<Workspace>> {
        self.archive_calls.lock().unwrap().push((
            id.as_str().to_string(),
            caller_agent_id.map(|a| a.as_str().to_string()),
        ));
        let variant = *self.workspace_variant.lock().unwrap();
        Box::pin(async move {
            if matches!(variant, WorkspaceVariant::NotFound) {
                return Err(Error::NotFound(format!("workspace {}", id.as_str())));
            }
            let mut w = make_workspace(id.as_str(), variant);
            w.status = WorkspaceStatus::Archived;
            w.archived = true;
            w.archived_at = Some("2026-02-02T00:00:00Z".to_string());
            Ok(w)
        })
    }

    fn unarchive_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        self.unarchive_calls
            .lock()
            .unwrap()
            .push(id.as_str().to_string());
        let variant = *self.workspace_variant.lock().unwrap();
        Box::pin(async move {
            if matches!(variant, WorkspaceVariant::NotFound) {
                return Err(Error::NotFound(format!("workspace {}", id.as_str())));
            }
            let mut w = make_workspace(id.as_str(), variant);
            w.status = WorkspaceStatus::Active;
            w.archived = false;
            w.archived_at = None;
            Ok(w)
        })
    }

    fn git_agent_commit(
        &self,
        _id: WorkspaceId,
        message: String,
        agent_id: Option<AgentId>,
        _linked_note_id: Option<NoteId>,
        _files: Option<Vec<String>>,
        user_requested: bool,
    ) -> BoxFuture<'_, Result<GitAgentCommitResult>> {
        self.agent_commit_calls.lock().unwrap().push((
            message,
            agent_id.as_ref().map(|a| a.as_str().to_string()),
            user_requested,
        ));
        Box::pin(async {
            Ok(GitAgentCommitResult {
                hash: "def456".to_string(),
                files: vec!["a.txt".to_string(), "b.txt".to_string()],
                file_count: 2,
            })
        })
    }

    fn git_root_register(
        &self,
        _id: WorkspaceId,
        path: String,
        agent_id: AgentId,
    ) -> BoxFuture<'_, Result<Value>> {
        self.git_root_register_calls
            .lock()
            .unwrap()
            .push((path.clone(), agent_id.as_str().to_string()));
        Box::pin(async move {
            Ok(json!({
                "id": "gitroot-1",
                "workspaceId": "ws-1",
                "path": path,
                "source": "agent",
                "branch": "main",
            }))
        })
    }

    fn git_root_unregister(&self, _id: WorkspaceId, path: String) -> BoxFuture<'_, Result<Value>> {
        self.git_root_unregister_calls.lock().unwrap().push(path);
        Box::pin(async { Ok(json!({ "ok": true, "gitRootId": "gitroot-1", "path": "/tmp/sub" })) })
    }

    fn git_root_list(&self, _id: WorkspaceId) -> BoxFuture<'_, Result<Value>> {
        *self.git_root_list_calls.lock().unwrap() += 1;
        // Mirror the production envelope — the binding unwraps `gitRoots`
        // into the bare array the reference docs promise.
        Box::pin(async {
            Ok(
                json!({ "gitRoots": [{ "id": "gitroot-1", "path": "/tmp/sub", "source": "agent" }] }),
            )
        })
    }

    fn script_list(&self, _id: WorkspaceId) -> BoxFuture<'_, Result<Value>> {
        *self.script_list_calls.lock().unwrap() += 1;
        // Mirror the production `WorkspaceApi::script_list` shape from
        // `intent-services::ScriptManager::list`, which wraps the bare
        // array in `{ "scripts": [...] }`. The binding is responsible for
        // reshaping it to the reference bare-array contract before it
        // reaches JS callers — returning the wrapped shape here keeps the
        // test exercising that reshape.
        Box::pin(async { Ok(json!({ "scripts": [{ "id": "s-1", "name": "dev" }] })) })
    }

    fn script_create(
        &self,
        _id: WorkspaceId,
        params: ScriptCreateParams,
    ) -> BoxFuture<'_, Result<Value>> {
        self.script_create_calls
            .lock()
            .unwrap()
            .push(params.clone());
        Box::pin(async move { Ok(json!({ "id": "s-1" })) })
    }

    fn script_start(&self, _id: WorkspaceId, script_id: String) -> BoxFuture<'_, Result<Value>> {
        self.script_start_calls.lock().unwrap().push(script_id);
        Box::pin(async { Ok(json!({ "ok": true, "scriptId": "s-1" })) })
    }

    fn script_output(
        &self,
        _id: WorkspaceId,
        script_id: String,
        max_lines: Option<i64>,
        _paginate: Option<bool>,
        _page_token: Option<String>,
    ) -> BoxFuture<'_, Result<Value>> {
        self.script_output_calls
            .lock()
            .unwrap()
            .push((script_id, max_lines));
        Box::pin(async { Ok(json!("output body")) })
    }

    fn script_run(
        &self,
        _id: WorkspaceId,
        script_id: String,
        _max_lines: Option<i64>,
        timeout_seconds: Option<i64>,
    ) -> BoxFuture<'_, Result<Value>> {
        self.script_run_calls
            .lock()
            .unwrap()
            .push((script_id, timeout_seconds));
        Box::pin(async { Ok(json!({ "exitCode": 0, "output": "" })) })
    }

    fn terminal_list(&self, _id: WorkspaceId) -> BoxFuture<'_, Result<Value>> {
        *self.terminal_list_calls.lock().unwrap() += 1;
        Box::pin(async {
            Ok(json!({
                "terminals": [{ "id": "t-1", "name": "Terminal" }],
                "daemonBootId": "boot-fixed",
            }))
        })
    }

    fn terminal_read_output(
        &self,
        _id: WorkspaceId,
        terminal_id: String,
        max_lines: Option<i64>,
        _paginate: Option<bool>,
        _page_token: Option<String>,
    ) -> BoxFuture<'_, Result<Value>> {
        self.terminal_read_calls
            .lock()
            .unwrap()
            .push((terminal_id, max_lines));
        Box::pin(async { Ok(json!("terminal output")) })
    }

    fn file_read(
        &self,
        _id: WorkspaceId,
        path: String,
        _caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<Value>> {
        self.file_read_calls.lock().unwrap().push(path.clone());
        Box::pin(async move {
            if path == "outside/../oob" {
                return Err(Error::InvalidParams(
                    "Access denied: path outside workspace".to_string(),
                ));
            }
            Ok(json!(format!("contents of {path}")))
        })
    }

    fn file_write(
        &self,
        _id: WorkspaceId,
        path: String,
        content: String,
        _caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<Value>> {
        self.file_write_calls
            .lock()
            .unwrap()
            .push((path.clone(), content.clone()));
        Box::pin(async move { Ok(json!({ "ok": true, "path": path, "size": content.len() })) })
    }

    fn file_list(
        &self,
        _id: WorkspaceId,
        path: String,
        _caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<Value>> {
        self.file_list_calls.lock().unwrap().push(path);
        Box::pin(async { Ok(json!([{ "name": "a.txt", "type": "file" }])) })
    }

    fn file_delete(
        &self,
        _id: WorkspaceId,
        path: String,
        _caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<Value>> {
        self.file_delete_calls.lock().unwrap().push(path.clone());
        Box::pin(async move { Ok(json!({ "ok": true, "path": path, "deleted": true })) })
    }

    fn file_mkdir(
        &self,
        _id: WorkspaceId,
        path: String,
        _caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<Value>> {
        self.file_mkdir_calls.lock().unwrap().push(path.clone());
        Box::pin(async move { Ok(json!({ "ok": true, "path": path, "created": true })) })
    }

    fn file_rename(
        &self,
        _id: WorkspaceId,
        old_path: String,
        new_path: String,
        _caller_agent_id: Option<AgentId>,
    ) -> BoxFuture<'_, Result<Value>> {
        self.file_rename_calls
            .lock()
            .unwrap()
            .push((old_path.clone(), new_path.clone()));
        Box::pin(async move {
            Ok(json!({
                "ok": true, "oldPath": old_path, "newPath": new_path,
                "renamed": true, "isDirectory": false,
            }))
        })
    }
}

fn server() -> (WorkspaceMcpServer, Arc<FakeApi>) {
    let api = Arc::new(FakeApi::default());
    let srv = WorkspaceMcpServer::new(api.clone(), WorkspaceId::from_string("ws-1"));
    (srv, api)
}

fn server_with_caller(caller: &str) -> (WorkspaceMcpServer, Arc<FakeApi>) {
    let api = Arc::new(FakeApi::default());
    let srv = WorkspaceMcpServer::new(api.clone(), WorkspaceId::from_string("ws-1"))
        .with_caller_agent_id(Some(AgentId::from_string(caller)));
    (srv, api)
}

async fn call(srv: &WorkspaceMcpServer, code: &str) -> Value {
    srv.handle_message(&json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {
            "name": "workspace_api",
            "arguments": { "code": code, "summary": "wsapi5 unit test" }
        }
    }))
    .await
    .expect("tools/call must produce a response")
}

fn body(resp: &Value) -> Value {
    let text = resp["result"]["content"][0]["text"].as_str().unwrap();
    serde_json::from_str(text).expect("workspace_api body must be JSON")
}

fn text(resp: &Value) -> String {
    resp["result"]["content"][0]["text"]
        .as_str()
        .unwrap()
        .to_string()
}

// ============================================================================
// workspace.*
// ============================================================================

#[tokio::test]
async fn workspace_details_returns_reference_shape() {
    let (srv, _api) = server();
    let resp = call(&srv, "return await ws.workspace.details();").await;
    assert_eq!(resp["result"]["isError"], json!(false));
    let v = body(&resp);
    assert_eq!(v["id"], json!("ws-1"));
    assert_eq!(v["title"], json!("Hello"));
    assert_eq!(v["hasTitle"], json!(true));
    assert_eq!(v["branch"], json!("main"));
    assert_eq!(v["repositoryName"], json!("intentd"));
    assert_eq!(v["tags"], json!(["red"]));
}

#[tokio::test]
async fn workspace_details_not_found_returns_defaults() {
    let (srv, api) = server();
    *api.workspace_variant.lock().unwrap() = WorkspaceVariant::NotFound;
    let resp = call(&srv, "return await ws.workspace.details();").await;
    assert_eq!(resp["result"]["isError"], json!(false));
    let v = body(&resp);
    assert_eq!(v["hasTitle"], json!(false));
    assert_eq!(v["title"], json!("(untitled)"));
}

#[tokio::test]
async fn workspace_set_title_skips_when_already_titled() {
    let (srv, api) = server();
    let resp = call(&srv, "return await ws.workspace.setTitle('Whatever');").await;
    let v = body(&resp);
    assert_eq!(v["skipped"], json!(true));
    assert_eq!(v["title"], json!("Hello"));
    assert!(api.update_calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn workspace_set_title_updates_when_auto_titled() {
    let (srv, api) = server();
    *api.workspace_variant.lock().unwrap() = WorkspaceVariant::AutoTitled;
    let resp = call(&srv, "return await ws.workspace.setTitle(' New Name ');").await;
    let v = body(&resp);
    assert_eq!(v["ok"], json!(true));
    assert_eq!(v["title"], json!("New Name"));
    let calls = api.update_calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].title.as_deref(), Some("New Name"));
}

#[tokio::test]
async fn workspace_set_title_empty_errors() {
    let (srv, _api) = server();
    let resp = call(&srv, "return await ws.workspace.setTitle('   ');").await;
    assert_eq!(resp["result"]["isError"], json!(true));
    assert!(text(&resp).contains("title is required"));
}

#[tokio::test]
async fn workspace_set_status_message_clears_on_empty() {
    let (srv, api) = server();
    let resp = call(&srv, "return await ws.workspace.setStatusMessage('');").await;
    let v = body(&resp);
    assert_eq!(v["ok"], json!(true));
    assert_eq!(v["statusMessage"], Value::Null);
    assert_eq!(api.update_calls.lock().unwrap().len(), 1);
    // `ws.workspace.details()` after the clear must surface `null`, not
    // `""` — the empty-string-vs-null representation only shows up on the
    // read-back path, so pin it here so regressions in either services'
    // update normalization or the binding's `details` shaping get caught.
    let after = call(&srv, "return await ws.workspace.details();").await;
    let d = body(&after);
    assert_eq!(d["statusMessage"], Value::Null);
}

#[tokio::test]
async fn workspace_details_normalizes_legacy_empty_status_message() {
    // Rows persisted before the services-layer clear normalization (or by
    // any other writer that still emits `""` / whitespace) can carry an
    // empty string on read. The `details` binding must normalize on read
    // so the documented clear contract (`empty/null ⇒ null`) still holds
    // for legacy data. Seed the fake with an empty override and assert
    // the read-back is `null`; repeat for a whitespace-only value.
    let (srv, api) = server();
    *api.status_message_state.lock().unwrap() = Some(Some(String::new()));
    let resp = call(&srv, "return await ws.workspace.details();").await;
    let d = body(&resp);
    assert_eq!(d["statusMessage"], Value::Null);

    *api.status_message_state.lock().unwrap() = Some(Some("   \t".to_string()));
    let resp = call(&srv, "return await ws.workspace.details();").await;
    let d = body(&resp);
    assert_eq!(d["statusMessage"], Value::Null);
}

#[tokio::test]
async fn workspace_set_status_message_over_length_errors() {
    let (srv, _api) = server();
    let code = "return await ws.workspace.setStatusMessage('a'.repeat(1000));";
    let resp = call(&srv, code).await;
    assert_eq!(resp["result"]["isError"], json!(true));
    assert!(text(&resp).contains("500 characters or fewer"));
}

#[tokio::test]
async fn workspace_set_status_message_counts_chars_not_bytes() {
    // The 500 cap is a *character* limit per the FE contract
    // (`WORKSPACE_STATUS_MESSAGE_MAX_LENGTH` in `src/shared/types.ts`).
    // A string with 500 multi-byte characters (here 4-byte per char) must
    // pass — earlier byte-length code would reject at ~125 chars.
    let (srv, _api) = server();
    let code = "return await ws.workspace.setStatusMessage('🙂'.repeat(500));";
    let resp = call(&srv, code).await;
    assert_eq!(resp["result"]["isError"], json!(false), "{}", text(&resp));
}

#[tokio::test]
async fn workspace_set_status_image_saves_asset_and_updates_workspace() {
    let (srv, api) = server();
    let code = "return await ws.workspace.setStatusImage({ data: 'aGVsbG8=', mimeType: 'image/png', originalName: 'shot.png' });";
    let resp = call(&srv, code).await;
    let v = body(&resp);
    assert_eq!(v["ok"], json!(true));
    assert_eq!(v["statusImageAssetId"], json!("asset-123"));
    assert_eq!(v["url"], json!("workspace-asset://asset-123"));
    let saves = api.save_asset_calls.lock().unwrap();
    assert_eq!(saves.len(), 1);
    assert_eq!(saves[0].0, "aGVsbG8=");
    assert_eq!(saves[0].1, "image/png");
    assert_eq!(saves[0].2.as_deref(), Some("shot.png"));
    let updates = api.update_calls.lock().unwrap();
    assert_eq!(updates.len(), 1);
    assert_eq!(
        updates[0].status_image_asset_id,
        Some(Some("asset-123".to_string()))
    );
}

#[tokio::test]
async fn workspace_set_status_image_null_clears() {
    let (srv, api) = server();
    let resp = call(&srv, "return await ws.workspace.setStatusImage(null);").await;
    let v = body(&resp);
    assert_eq!(v["ok"], json!(true));
    assert_eq!(v["statusImageAssetId"], Value::Null);
    assert!(api.save_asset_calls.lock().unwrap().is_empty());
    let updates = api.update_calls.lock().unwrap();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].status_image_asset_id, Some(None));
}

#[tokio::test]
async fn workspace_set_status_image_no_arg_errors_instead_of_clearing() {
    // A clear is destructive: the prelude's JSON.stringify drops `undefined`
    // keys, so a bare `setStatusImage()` must error — only explicit `null`
    // clears.
    let (srv, api) = server();
    let resp = call(&srv, "return await ws.workspace.setStatusImage();").await;
    assert_eq!(resp["result"]["isError"], json!(true));
    assert!(text(&resp).contains("image is required"));
    assert!(api.save_asset_calls.lock().unwrap().is_empty());
    assert!(api.update_calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn workspace_set_status_image_requires_data() {
    let (srv, api) = server();
    let code = "return await ws.workspace.setStatusImage({ mimeType: 'image/png' });";
    let resp = call(&srv, code).await;
    assert_eq!(resp["result"]["isError"], json!(true));
    assert!(text(&resp).contains("image.data (base64) is required"));
    assert!(api.save_asset_calls.lock().unwrap().is_empty());
    assert!(api.update_calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn workspace_set_status_image_rejects_non_image_mime() {
    let (srv, api) = server();
    let code =
        "return await ws.workspace.setStatusImage({ data: 'aGVsbG8=', mimeType: 'text/plain' });";
    let resp = call(&srv, code).await;
    assert_eq!(resp["result"]["isError"], json!(true));
    assert!(text(&resp).contains("must be an image/* type"));
    assert!(api.save_asset_calls.lock().unwrap().is_empty());
    assert!(api.update_calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn workspace_set_agent_name_requires_caller_context() {
    let (srv, _api) = server();
    let resp = call(&srv, "return await ws.workspace.setAgentName('Fresh');").await;
    assert_eq!(resp["result"]["isError"], json!(true));
    assert!(text(&resp).contains("Could not determine agent ID"));
}

#[tokio::test]
async fn workspace_set_agent_name_forwards_caller_id() {
    let (srv, api) = server_with_caller("agent-42");
    let resp = call(&srv, "return await ws.workspace.setAgentName('Fresh');").await;
    assert_eq!(resp["result"]["isError"], json!(false));
    let calls = api.rename_calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, "agent-42");
    assert_eq!(calls[0].1, "Fresh");
    assert!(calls[0].2);
}

#[tokio::test]
async fn workspace_context_reports_unavailable_in_port() {
    let (srv, _api) = server();
    let resp = call(&srv, "return await ws.workspace.context();").await;
    assert_eq!(resp["result"]["isError"], json!(true));
    assert!(text(&resp).contains("not yet available in this daemon port"));
}

#[tokio::test]
async fn workspace_reference_docs_reports_unavailable_in_port() {
    let (srv, _api) = server();
    let resp = call(&srv, "return await ws.workspace.referenceDocs('x');").await;
    assert_eq!(resp["result"]["isError"], json!(true));
    assert!(text(&resp).contains("not yet available in this daemon port"));
}

#[tokio::test]
async fn workspace_timeline_reports_unavailable_in_port() {
    let (srv, _api) = server();
    let resp = call(&srv, "return await ws.workspace.timeline();").await;
    assert_eq!(resp["result"]["isError"], json!(true));
    assert!(text(&resp).contains("not yet available in this daemon port"));
}

#[tokio::test]
async fn workspace_emit_notification_reports_unavailable_in_port() {
    let (srv, _api) = server();
    let resp = call(&srv, "return await ws.workspace.emitNotification('t','m');").await;
    assert_eq!(resp["result"]["isError"], json!(true));
    assert!(text(&resp).contains("not yet available in this daemon port"));
}

/// Minimal [`AgentLite`] for seeding the fake's `agent_list` — only `id`,
/// `name`, `status`, and `is_responding` matter to the archive guardrail.
fn agent_lite(id: &str, name: &str, status: AgentStatus, is_responding: bool) -> AgentLite {
    AgentLite {
        harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
        harness_features: None,
        id: AgentId::from_string(id),
        workspace_id: WorkspaceId::from_string("ws-1"),
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
        is_responding,
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
        last_activity: None,
        message_count: 0,
        last_agent_response: None,
        last_user_message: None,
        last_message_role: None,
        last_message_id: None,
        last_tool_use: None,
        digest: None,
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
            sandbox_path: None,
            sandbox_branch: None,
            dismissed_questions_message_id: None,
            pending_questions_message_id: None,
            last_seen_message_id: None,
            is_initial_agent: None,
        },
    }
}

fn chief_server() -> (WorkspaceMcpServer, Arc<FakeApi>) {
    let api = Arc::new(FakeApi::default());
    let srv = WorkspaceMcpServer::new(api.clone(), WorkspaceId::from_string(CHIEF_WORKSPACE_ID));
    (srv, api)
}

#[tokio::test]
async fn workspace_archive_happy_path() {
    // The calling agent itself is mid-turn (isResponding) but must be
    // excluded from the guardrail.
    let (srv, api) = server_with_caller("agent-self");
    *api.agents.lock().unwrap() = vec![
        agent_lite("agent-self", "Me", AgentStatus::Active, true),
        agent_lite("agent-done", "Done", AgentStatus::Completed, false),
        agent_lite("agent-idle", "Idle", AgentStatus::RuntimeIdle, false),
    ];
    let resp = call(&srv, "return await ws.workspace.archive();").await;
    assert_eq!(resp["result"]["isError"], json!(false));
    let v = body(&resp);
    assert_eq!(v["ok"], json!(true));
    assert_eq!(v["status"], json!("Archived"));
    assert_eq!(v["archivedAt"], json!("2026-02-02T00:00:00Z"));
    assert_eq!(
        *api.archive_calls.lock().unwrap(),
        vec![("ws-1".to_string(), Some("agent-self".to_string()))],
        "the calling agent rides along so the sweep does not interrupt it"
    );
}

#[tokio::test]
async fn workspace_unarchive_happy_path() {
    let (srv, api) = server();
    let resp = call(&srv, "return await ws.workspace.unarchive();").await;
    assert_eq!(resp["result"]["isError"], json!(false));
    let v = body(&resp);
    assert_eq!(v["ok"], json!(true));
    assert_eq!(v["status"], json!("Active"));
    assert!(v.get("archivedAt").is_none());
    assert_eq!(
        *api.unarchive_calls.lock().unwrap(),
        vec!["ws-1".to_string()]
    );
}

#[tokio::test]
async fn workspace_archive_refuses_when_other_agents_running() {
    let (srv, api) = server_with_caller("agent-self");
    *api.agents.lock().unwrap() = vec![
        agent_lite("agent-self", "Me", AgentStatus::Active, true),
        agent_lite("agent-busy", "Busy Worker", AgentStatus::Active, true),
        agent_lite("agent-queued", "Queued One", AgentStatus::Pending, false),
    ];
    let resp = call(&srv, "return await ws.workspace.archive();").await;
    assert_eq!(resp["result"]["isError"], json!(true));
    let msg = text(&resp);
    assert!(msg.contains("Cannot archive"), "message: {msg}");
    assert!(msg.contains("Busy Worker (agent-busy)"), "message: {msg}");
    assert!(msg.contains("Queued One (agent-queued)"), "message: {msg}");
    assert!(
        !msg.contains("agent-self"),
        "caller must be excluded: {msg}"
    );
    assert!(api.archive_calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn workspace_archive_refuses_in_chief_workspace() {
    let (srv, api) = chief_server();
    let resp = call(&srv, "return await ws.workspace.archive();").await;
    assert_eq!(resp["result"]["isError"], json!(true));
    assert!(text(&resp).contains("chief-of-staff"));
    assert!(api.archive_calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn workspace_unarchive_refuses_in_chief_workspace() {
    let (srv, api) = chief_server();
    let resp = call(&srv, "return await ws.workspace.unarchive();").await;
    assert_eq!(resp["result"]["isError"], json!(true));
    assert!(text(&resp).contains("chief-of-staff"));
    assert!(api.unarchive_calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn workspace_set_status_image_refuses_in_chief_workspace() {
    let (srv, api) = chief_server();
    let code =
        "return await ws.workspace.setStatusImage({ data: 'aGVsbG8=', mimeType: 'image/png' });";
    let resp = call(&srv, code).await;
    assert_eq!(resp["result"]["isError"], json!(true));
    assert!(text(&resp).contains("chief-of-staff"));
    assert!(api.save_asset_calls.lock().unwrap().is_empty());
    assert!(api.update_calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn workspace_archive_not_found_errors() {
    let (srv, api) = server();
    *api.workspace_variant.lock().unwrap() = WorkspaceVariant::NotFound;
    let resp = call(&srv, "return await ws.workspace.archive();").await;
    assert_eq!(resp["result"]["isError"], json!(true));
    assert!(text(&resp).contains("not found"));
}

// ============================================================================
// git.*
// ============================================================================

#[tokio::test]
async fn git_commit_requires_caller_context() {
    let (srv, api) = server();
    let resp = call(&srv, "return await ws.git.commit('feat: x');").await;
    assert_eq!(resp["result"]["isError"], json!(true));
    assert!(text(&resp).contains("No agent context available"));
    assert!(api.agent_commit_calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn git_commit_returns_file_count_shape() {
    let (srv, api) = server_with_caller("agent-9");
    let resp = call(
        &srv,
        "return await ws.git.commit('feat: x', { userRequested: true });",
    )
    .await;
    assert_eq!(resp["result"]["isError"], json!(false));
    let v = body(&resp);
    assert_eq!(v["hash"], json!("def456"));
    assert_eq!(v["fileCount"], json!(2));
    let calls = api.agent_commit_calls.lock().unwrap();
    assert_eq!(calls[0].1.as_deref(), Some("agent-9"));
    assert!(calls[0].2);
}

#[tokio::test]
async fn git_commit_defaults_user_requested_to_false() {
    let (srv, api) = server_with_caller("agent-9");
    let resp = call(&srv, "return await ws.git.commit('feat: x');").await;
    assert_eq!(resp["result"]["isError"], json!(false));
    let calls = api.agent_commit_calls.lock().unwrap();
    assert_eq!(calls[0].0, "feat: x");
    assert!(!calls[0].2);
}

#[tokio::test]
async fn git_removed_methods_error_as_unknown() {
    // The read/stage/merge-check `ws.git.*` surface (and the old staged-only
    // commit's `agentCommit` spelling) was removed in favor of the plain
    // `git` CLI; raw `host({...})` frames for the old methods must fail with
    // the standard unknown-binding error.
    let (srv, api) = server_with_caller("agent-9");
    for method in ["status", "stage", "agentCommit", "checkMergeConflicts"] {
        let code =
            format!("return await host({{ method: 'git.{method}', args: {{ message: 'm' }} }});");
        let resp = call(&srv, &code).await;
        assert_eq!(resp["result"]["isError"], json!(true), "git.{method}");
        assert!(
            text(&resp).contains(&format!("unknown method `git.{method}`")),
            "git.{method} must surface the unknown-binding error"
        );
    }
    assert!(api.agent_commit_calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn git_register_root_requires_caller_context() {
    let (srv, api) = server();
    let resp = call(&srv, "return await ws.git.registerRoot('/tmp/sub');").await;
    assert_eq!(resp["result"]["isError"], json!(true));
    assert!(text(&resp).contains("No agent context available"));
    assert!(api.git_root_register_calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn git_register_root_passes_path_and_caller() {
    let (srv, api) = server_with_caller("agent-9");
    let resp = call(&srv, "return await ws.git.registerRoot('/tmp/sub');").await;
    assert_eq!(resp["result"]["isError"], json!(false));
    let v = body(&resp);
    assert_eq!(v["id"], json!("gitroot-1"));
    assert_eq!(v["path"], json!("/tmp/sub"));
    assert_eq!(v["source"], json!("agent"));
    let calls = api.git_root_register_calls.lock().unwrap();
    assert_eq!(calls[0], ("/tmp/sub".to_string(), "agent-9".to_string()));
}

#[tokio::test]
async fn git_register_root_requires_path() {
    let (srv, api) = server_with_caller("agent-9");
    let resp = call(&srv, "return await ws.git.registerRoot();").await;
    assert_eq!(resp["result"]["isError"], json!(true));
    assert!(text(&resp).contains("path is required"));
    assert!(api.git_root_register_calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn git_unregister_root_passes_path() {
    let (srv, api) = server();
    let resp = call(&srv, "return await ws.git.unregisterRoot('/tmp/sub');").await;
    assert_eq!(resp["result"]["isError"], json!(false));
    let v = body(&resp);
    assert_eq!(v["ok"], json!(true));
    assert_eq!(v["gitRootId"], json!("gitroot-1"));
    assert_eq!(
        *api.git_root_unregister_calls.lock().unwrap(),
        vec!["/tmp/sub".to_string()]
    );
}

#[tokio::test]
async fn git_list_roots_unwraps_envelope() {
    let (srv, api) = server();
    let resp = call(&srv, "return await ws.git.listRoots();").await;
    assert_eq!(resp["result"]["isError"], json!(false));
    let v = body(&resp);
    let roots = v.as_array().expect("bare array, envelope unwrapped");
    assert_eq!(roots.len(), 1);
    assert_eq!(roots[0]["id"], json!("gitroot-1"));
    assert_eq!(*api.git_root_list_calls.lock().unwrap(), 1);
}

// ============================================================================
// script.*
// ============================================================================

#[tokio::test]
async fn script_list_returns_array() {
    let (srv, api) = server();
    let resp = call(&srv, "return await ws.script.list();").await;
    assert_eq!(resp["result"]["isError"], json!(false));
    let arr = body(&resp);
    assert_eq!(arr.as_array().unwrap().len(), 1);
    assert_eq!(*api.script_list_calls.lock().unwrap(), 1);
}

#[tokio::test]
async fn script_create_forwards_positional_signature() {
    let (srv, api) = server();
    let code = r#"
        return await ws.script.create('dev', 'pnpm dev', 'service', {
            cwd: 'app',
            env: { PORT: '3000' },
            category: 'dev',
            autoStart: true,
            scriptId: 's-1',
        });
    "#;
    let resp = call(&srv, code).await;
    assert_eq!(resp["result"]["isError"], json!(false));
    let calls = api.script_create_calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    let p = &calls[0];
    assert_eq!(p.name, "dev");
    assert_eq!(p.command, "pnpm dev");
    assert_eq!(p.cwd.as_deref(), Some("app"));
    let env: BTreeMap<String, String> = [("PORT".to_string(), "3000".to_string())]
        .into_iter()
        .collect();
    assert_eq!(p.env.as_ref(), Some(&env));
    assert_eq!(p.category.as_deref(), Some("dev"));
    assert_eq!(p.auto_start, Some(true));
    assert_eq!(p.script_id.as_deref(), Some("s-1"));
}

#[tokio::test]
async fn script_create_rejects_non_string_env_value() {
    // `ScriptCreateParams.env` is `BTreeMap<String, String>` — a numeric
    // value must surface an input error rather than being silently
    // dropped (previously `filter_map(Value::as_str)` turned
    // `{ PORT: 3000 }` into `{}`).
    let (srv, api) = server();
    let code = "return await ws.script.create('n', 'c', 'service', { env: { PORT: 3000 } });";
    let resp = call(&srv, code).await;
    assert_eq!(resp["result"]["isError"], json!(true));
    let t = text(&resp);
    assert!(
        t.contains("env.PORT") && t.contains("string"),
        "unexpected: {t}"
    );
    assert!(api.script_create_calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn script_create_rejects_non_object_env() {
    // A non-object `env` (previously coerced to `None` via
    // `and_then(Value::as_object)`) must be an input error.
    let (srv, api) = server();
    let code = "return await ws.script.create('n', 'c', 'service', { env: 'PORT=3000' });";
    let resp = call(&srv, code).await;
    assert_eq!(resp["result"]["isError"], json!(true));
    assert!(text(&resp).contains("env must be an object"));
    assert!(api.script_create_calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn script_create_rejects_invalid_mode() {
    let (srv, api) = server();
    let resp = call(&srv, "return await ws.script.create('n', 'c', 'daemon');").await;
    assert_eq!(resp["result"]["isError"], json!(true));
    // The error string contains the reference's `"service"`/`"command"`
    // quoted literals; check the stable prose fragment either side of them.
    let t = text(&resp);
    assert!(t.contains("mode must be"), "unexpected error: {t}");
    assert!(
        t.contains("service") && t.contains("command"),
        "unexpected: {t}"
    );
    assert!(api.script_create_calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn script_start_forwards_script_id() {
    let (srv, api) = server();
    let resp = call(&srv, "return await ws.script.start('s-1');").await;
    assert_eq!(resp["result"]["isError"], json!(false));
    assert_eq!(api.script_start_calls.lock().unwrap()[0], "s-1");
}

#[tokio::test]
async fn script_output_forwards_max_lines() {
    let (srv, api) = server();
    let resp = call(&srv, "return await ws.script.output('s-1', 42);").await;
    assert_eq!(resp["result"]["isError"], json!(false));
    assert_eq!(api.script_output_calls.lock().unwrap()[0].1, Some(42));
}

#[tokio::test]
async fn script_run_accepts_timeout_alias() {
    let (srv, api) = server();
    let resp = call(&srv, "return await ws.script.run('s-1', { timeout: 7 });").await;
    assert_eq!(resp["result"]["isError"], json!(false));
    assert_eq!(api.script_run_calls.lock().unwrap()[0].1, Some(7));
}

// ============================================================================
// terminal.*
// ============================================================================

#[tokio::test]
async fn terminal_list_returns_array() {
    let (srv, api) = server();
    let resp = call(&srv, "return await ws.terminal.list();").await;
    assert_eq!(resp["result"]["isError"], json!(false));
    // The binding unwraps the wire `{ terminals, daemonBootId }` envelope so
    // agents keep seeing the bare terminals array (monorepo#1334).
    let arr = body(&resp);
    assert_eq!(arr.as_array().unwrap().len(), 1);
    assert_eq!(arr[0]["id"], json!("t-1"));
    assert_eq!(*api.terminal_list_calls.lock().unwrap(), 1);
}

#[tokio::test]
async fn terminal_read_output_requires_terminal_id() {
    let (srv, _api) = server();
    let resp = call(&srv, "return await ws.terminal.readOutput();").await;
    assert_eq!(resp["result"]["isError"], json!(true));
    assert!(text(&resp).contains("terminalId is required"));
}

#[tokio::test]
async fn terminal_read_output_forwards_max_lines() {
    let (srv, api) = server();
    let resp = call(&srv, "return await ws.terminal.readOutput('t-1', 200);").await;
    assert_eq!(resp["result"]["isError"], json!(false));
    let calls = api.terminal_read_calls.lock().unwrap();
    assert_eq!(calls[0].0, "t-1");
    assert_eq!(calls[0].1, Some(200));
}

// ============================================================================
// file.*
// ============================================================================

#[tokio::test]
async fn file_read_forwards_path() {
    let (srv, api) = server();
    let resp = call(&srv, "return await ws.file.read('a.txt');").await;
    assert_eq!(resp["result"]["isError"], json!(false));
    assert_eq!(api.file_read_calls.lock().unwrap()[0], "a.txt");
}

#[tokio::test]
async fn file_read_surfaces_daemon_access_denied() {
    let (srv, _api) = server();
    let resp = call(&srv, "return await ws.file.read('outside/../oob');").await;
    assert_eq!(resp["result"]["isError"], json!(true));
    assert!(text(&resp).contains("Access denied"));
}

#[tokio::test]
async fn file_read_requires_path() {
    let (srv, _api) = server();
    let resp = call(&srv, "return await ws.file.read();").await;
    assert_eq!(resp["result"]["isError"], json!(true));
    assert!(text(&resp).contains("path is required"));
}

#[tokio::test]
async fn file_write_requires_content() {
    let (srv, _api) = server();
    let resp = call(&srv, "return await ws.file.write('a.txt');").await;
    assert_eq!(resp["result"]["isError"], json!(true));
    assert!(text(&resp).contains("path and content are required"));
}

#[tokio::test]
async fn file_write_forwards_body() {
    let (srv, api) = server();
    let resp = call(&srv, "return await ws.file.write('a.txt', 'hi');").await;
    assert_eq!(resp["result"]["isError"], json!(false));
    assert_eq!(api.file_write_calls.lock().unwrap()[0].1, "hi");
}

#[tokio::test]
async fn file_list_defaults_to_dot() {
    let (srv, api) = server();
    let resp = call(&srv, "return await ws.file.list();").await;
    assert_eq!(resp["result"]["isError"], json!(false));
    assert_eq!(api.file_list_calls.lock().unwrap()[0], ".");
}

#[tokio::test]
async fn file_delete_forwards_path() {
    let (srv, api) = server();
    let resp = call(&srv, "return await ws.file.delete('a.txt');").await;
    assert_eq!(resp["result"]["isError"], json!(false));
    assert_eq!(api.file_delete_calls.lock().unwrap()[0], "a.txt");
}

#[tokio::test]
async fn file_mkdir_forwards_path() {
    let (srv, api) = server();
    let resp = call(&srv, "return await ws.file.mkdir('sub');").await;
    assert_eq!(resp["result"]["isError"], json!(false));
    assert_eq!(api.file_mkdir_calls.lock().unwrap()[0], "sub");
}

#[tokio::test]
async fn file_rename_requires_both_paths() {
    let (srv, _api) = server();
    let resp = call(&srv, "return await ws.file.rename('a.txt');").await;
    assert_eq!(resp["result"]["isError"], json!(true));
    assert!(text(&resp).contains("Both oldPath and newPath are required"));
}

#[tokio::test]
async fn file_rename_forwards_pair() {
    let (srv, api) = server();
    let resp = call(&srv, "return await ws.file.rename('a.txt', 'b.txt');").await;
    assert_eq!(resp["result"]["isError"], json!(false));
    assert_eq!(
        api.file_rename_calls.lock().unwrap()[0],
        ("a.txt".to_string(), "b.txt".to_string())
    );
}
