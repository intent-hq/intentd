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
    AgentId, BoxFuture, Error, FileStatus, GitAgentCommitResult, GitCommitResult, GitFileStatus,
    GitMergeConflicts, GitStatus, NoteId, Result, ScriptCreateParams, Workspace, WorkspaceActivity,
    WorkspaceApi, WorkspaceAttention, WorkspaceId, WorkspaceStatus, WorkspaceUpdate,
};
use serde_json::{json, Value};

use crate::WorkspaceMcpServer;

#[derive(Default)]
struct FakeApi {
    git_status_calls: Mutex<u32>,
    stage_calls: Mutex<Vec<Value>>,
    commit_calls: Mutex<Vec<String>>,
    agent_commit_calls: Mutex<Vec<(String, Option<String>, bool)>>,
    merge_calls: Mutex<Vec<Option<String>>>,
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
    workspace_variant: Mutex<WorkspaceVariant>,
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
        archived: false,
        archived_at: None,
        task_stats: None,
        agent_summary: None,
        diff_summary: None,
        token_usage: None,
    }
}

impl WorkspaceApi for FakeApi {
    fn get_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        let variant = *self.workspace_variant.lock().unwrap();
        Box::pin(async move {
            match variant {
                WorkspaceVariant::NotFound => {
                    Err(Error::NotFound(format!("workspace {}", id.as_str())))
                }
                v => Ok(make_workspace(id.as_str(), v)),
            }
        })
    }

    fn update_workspace(
        &self,
        id: WorkspaceId,
        update: WorkspaceUpdate,
    ) -> BoxFuture<'_, Result<Workspace>> {
        self.update_calls.lock().unwrap().push(update.clone());
        Box::pin(async move {
            let mut w = make_workspace(id.as_str(), WorkspaceVariant::Titled);
            if let Some(t) = update.title {
                w.title = t;
            }
            if let Some(s) = update.status_message {
                w.status_message = if s.is_empty() { None } else { Some(s) };
            }
            Ok(w)
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

    fn git_status(&self, _id: WorkspaceId) -> BoxFuture<'_, Result<GitStatus>> {
        *self.git_status_calls.lock().unwrap() += 1;
        Box::pin(async {
            Ok(GitStatus {
                branch: "main".to_string(),
                ahead: 0,
                behind: 0,
                diverged: false,
                files: vec![FileStatus {
                    path: "a.txt".to_string(),
                    status: GitFileStatus::Modified,
                    staged: false,
                }],
                has_uncommitted_changes: true,
                has_untracked_files: false,
            })
        })
    }

    fn git_stage(&self, _id: WorkspaceId, paths: Value) -> BoxFuture<'_, Result<Vec<String>>> {
        self.stage_calls.lock().unwrap().push(paths.clone());
        Box::pin(async move {
            let out = paths
                .as_array()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect();
            Ok(out)
        })
    }

    fn git_commit(
        &self,
        _id: WorkspaceId,
        message: String,
        _idempotency_key: Option<String>,
    ) -> BoxFuture<'_, Result<GitCommitResult>> {
        self.commit_calls.lock().unwrap().push(message);
        Box::pin(async {
            Ok(GitCommitResult {
                hash: "abc123".to_string(),
                files: vec!["a.txt".to_string()],
            })
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

    fn git_check_merge_conflicts(
        &self,
        _id: WorkspaceId,
        target: Option<String>,
    ) -> BoxFuture<'_, Result<GitMergeConflicts>> {
        self.merge_calls.lock().unwrap().push(target.clone());
        Box::pin(async move {
            Ok(GitMergeConflicts {
                has_conflicts: false,
                conflicted_files: Vec::new(),
                cannot_determine: None,
                target_branch: target.unwrap_or_else(|| "main".to_string()),
                current_branch: "feat".to_string(),
            })
        })
    }

    fn script_list(&self, _id: WorkspaceId) -> BoxFuture<'_, Result<Value>> {
        *self.script_list_calls.lock().unwrap() += 1;
        Box::pin(async { Ok(json!([{ "id": "s-1", "name": "dev" }])) })
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
        Box::pin(async { Ok(json!([{ "id": "t-1", "name": "Terminal" }])) })
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

    fn file_read(&self, _id: WorkspaceId, path: String) -> BoxFuture<'_, Result<Value>> {
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
    ) -> BoxFuture<'_, Result<Value>> {
        self.file_write_calls
            .lock()
            .unwrap()
            .push((path.clone(), content.clone()));
        Box::pin(async move { Ok(json!({ "ok": true, "path": path, "size": content.len() })) })
    }

    fn file_list(&self, _id: WorkspaceId, path: String) -> BoxFuture<'_, Result<Value>> {
        self.file_list_calls.lock().unwrap().push(path);
        Box::pin(async { Ok(json!([{ "name": "a.txt", "type": "file" }])) })
    }

    fn file_delete(&self, _id: WorkspaceId, path: String) -> BoxFuture<'_, Result<Value>> {
        self.file_delete_calls.lock().unwrap().push(path.clone());
        Box::pin(async move { Ok(json!({ "ok": true, "path": path, "deleted": true })) })
    }

    fn file_mkdir(&self, _id: WorkspaceId, path: String) -> BoxFuture<'_, Result<Value>> {
        self.file_mkdir_calls.lock().unwrap().push(path.clone());
        Box::pin(async move { Ok(json!({ "ok": true, "path": path, "created": true })) })
    }

    fn file_rename(
        &self,
        _id: WorkspaceId,
        old_path: String,
        new_path: String,
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

// ============================================================================
// git.*
// ============================================================================

#[tokio::test]
async fn git_status_returns_shaped_body() {
    let (srv, api) = server();
    let resp = call(&srv, "return await ws.git.status();").await;
    assert_eq!(resp["result"]["isError"], json!(false));
    let v = body(&resp);
    assert_eq!(v["branch"], json!("main"));
    assert_eq!(v["files"][0]["path"], json!("a.txt"));
    assert_eq!(*api.git_status_calls.lock().unwrap(), 1);
}

#[tokio::test]
async fn git_stage_blocks_stage_all_dot() {
    let (srv, api) = server();
    let resp = call(&srv, "return await ws.git.stage('.');").await;
    assert_eq!(resp["result"]["isError"], json!(true));
    assert!(text(&resp).contains("Staging all files is not allowed"));
    assert!(api.stage_calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn git_stage_blocks_stage_all_star() {
    let (srv, _api) = server();
    let resp = call(&srv, "return await ws.git.stage('*');").await;
    assert_eq!(resp["result"]["isError"], json!(true));
    assert!(text(&resp).contains("Staging all files is not allowed"));
}

#[tokio::test]
async fn git_stage_blocks_dash_dash_all() {
    let (srv, _api) = server();
    let resp = call(&srv, "return await ws.git.stage('--all');").await;
    assert_eq!(resp["result"]["isError"], json!(true));
    assert!(text(&resp).contains("Staging all files is not allowed"));
}

#[tokio::test]
async fn git_stage_accepts_array_of_paths() {
    let (srv, api) = server();
    let resp = call(&srv, "return await ws.git.stage(['a.txt', 'b.txt']);").await;
    assert_eq!(resp["result"]["isError"], json!(false));
    let v = body(&resp);
    assert_eq!(v["ok"], json!(true));
    assert_eq!(v["paths"], json!(["a.txt", "b.txt"]));
    assert_eq!(api.stage_calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn git_stage_csv_string_splits_on_commas() {
    let (srv, _api) = server();
    let resp = call(&srv, "return await ws.git.stage('a.txt, b.txt');").await;
    assert_eq!(resp["result"]["isError"], json!(false));
    let v = body(&resp);
    assert_eq!(v["paths"], json!(["a.txt", "b.txt"]));
}

#[tokio::test]
async fn git_commit_appends_agent_id_when_caller_present() {
    let (srv, api) = server_with_caller("agent-9");
    let resp = call(&srv, "return await ws.git.commit('feat: x');").await;
    assert_eq!(resp["result"]["isError"], json!(false));
    let v = body(&resp);
    assert_eq!(v["hash"], json!("abc123"));
    let msgs = api.commit_calls.lock().unwrap();
    assert!(msgs[0].contains("Agent-Id: agent-9"));
}

#[tokio::test]
async fn git_agent_commit_requires_caller_context() {
    let (srv, _api) = server();
    let resp = call(&srv, "return await ws.git.agentCommit('feat: x');").await;
    assert_eq!(resp["result"]["isError"], json!(true));
    assert!(text(&resp).contains("No agent context available"));
}

#[tokio::test]
async fn git_agent_commit_returns_file_count_shape() {
    let (srv, api) = server_with_caller("agent-9");
    let resp = call(
        &srv,
        "return await ws.git.agentCommit('feat: x', { userRequested: true });",
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
async fn git_check_merge_conflicts_returns_shape() {
    let (srv, api) = server();
    let resp = call(&srv, "return await ws.git.checkMergeConflicts('main');").await;
    assert_eq!(resp["result"]["isError"], json!(false));
    let v = body(&resp);
    assert_eq!(v["hasConflicts"], json!(false));
    assert_eq!(v["targetBranch"], json!("main"));
    assert_eq!(api.merge_calls.lock().unwrap()[0], Some("main".to_string()));
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
    let arr = body(&resp);
    assert_eq!(arr.as_array().unwrap().len(), 1);
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
