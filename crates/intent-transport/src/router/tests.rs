//! Router error-matrix + dispatch unit tests using a fake `WorkspaceApi`.

use intent_core::{
    BoxFuture, ContentType, Error, Note, NoteId, NoteVisibility, Result, Workspace,
    WorkspaceActivity, WorkspaceApi, WorkspaceAttention, WorkspaceCreate, WorkspaceId,
    WorkspaceStatus, WorkspaceUpdate,
};
use serde_json::Value;

use super::handle_message;

struct FakeApi;

fn sample_ws() -> Workspace {
    Workspace {
        id: WorkspaceId::from("ws-1"),
        title: "WS One".to_string(),
        branch: "main".to_string(),
        base_ref: None,
        base_commit_sha: None,
        status: WorkspaceStatus::Active,
        status_message: None,
        activity: WorkspaceActivity::Idle,
        attention: WorkspaceAttention::None,
        created_at: "t0".to_string(),
        updated_at: "t0".to_string(),
        last_activity: None,
        tags: vec![],
        path: None,
        repository_owner: None,
        repository_name: None,
        worktree_path: None,
        scope: None,
        skip_worktree: false,
        setup_script: None,
        is_remote: false,
        default_model: None,
        pr_number: None,
        pr_url: None,
        archived: false,
        archived_at: None,
    }
}

fn sample_note(ws: &WorkspaceId) -> Note {
    Note {
        id: NoteId::from("note-1"),
        workspace_id: ws.clone(),
        title: "Spec".to_string(),
        content: "# Hi".to_string(),
        content_type: ContentType::Markdown,
        tags: vec![],
        is_pinned: false,
        is_archived: false,
        is_default: true,
        parent_id: None,
        visibility: NoteVisibility::Workspace,
        task: None,
        created_at: "t0".to_string(),
        updated_at: "t0".to_string(),
    }
}

fn ws_with(id: &WorkspaceId) -> Workspace {
    Workspace {
        id: id.clone(),
        ..sample_ws()
    }
}

impl WorkspaceApi for FakeApi {
    fn list_workspaces(&self, _include_archived: bool) -> BoxFuture<'_, Result<Vec<Workspace>>> {
        Box::pin(async { Ok(vec![sample_ws()]) })
    }
    fn get_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        Box::pin(async move {
            if id.as_str() == "missing" {
                return Err(Error::NotFound("workspace".to_string()));
            }
            Ok(ws_with(&id))
        })
    }
    fn create_workspace(&self, input: WorkspaceCreate) -> BoxFuture<'_, Result<Workspace>> {
        Box::pin(async move {
            let mut ws = sample_ws();
            if let Some(t) = input.title {
                ws.title = t;
            }
            Ok(ws)
        })
    }
    fn update_workspace(
        &self,
        id: WorkspaceId,
        update: WorkspaceUpdate,
    ) -> BoxFuture<'_, Result<Workspace>> {
        Box::pin(async move {
            if id.as_str() == "missing" {
                return Err(Error::NotFound("workspace".to_string()));
            }
            let mut ws = ws_with(&id);
            if let Some(t) = update.title {
                ws.title = t;
            }
            Ok(ws)
        })
    }
    fn delete_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<()>> {
        Box::pin(async move {
            if id.as_str() == "missing" {
                return Err(Error::NotFound("workspace".to_string()));
            }
            Ok(())
        })
    }
    fn archive_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        Box::pin(async move {
            if id.as_str() == "missing" {
                return Err(Error::NotFound("workspace".to_string()));
            }
            let mut ws = ws_with(&id);
            ws.status = WorkspaceStatus::Archived;
            ws.archived = true;
            Ok(ws)
        })
    }
    fn unarchive_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        Box::pin(async move {
            if id.as_str() == "missing" {
                return Err(Error::NotFound("workspace".to_string()));
            }
            Ok(ws_with(&id))
        })
    }
    fn dismiss_attention(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        Box::pin(async move {
            if id.as_str() == "missing" {
                return Err(Error::NotFound("workspace".to_string()));
            }
            let mut ws = ws_with(&id);
            ws.attention = WorkspaceAttention::None;
            Ok(ws)
        })
    }
    fn mark_seen(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        Box::pin(async move {
            if id.as_str() == "missing" {
                return Err(Error::NotFound("workspace".to_string()));
            }
            let mut ws = ws_with(&id);
            ws.attention = WorkspaceAttention::None;
            Ok(ws)
        })
    }
    fn list_notes<'a>(&'a self, workspace_id: &'a WorkspaceId) -> BoxFuture<'a, Result<Vec<Note>>> {
        let id = workspace_id.clone();
        Box::pin(async move {
            if id.as_str() == "missing" {
                return Err(Error::NotFound("workspace".to_string()));
            }
            Ok(vec![sample_note(&id)])
        })
    }
}

async fn call(msg: &str) -> Option<Value> {
    handle_message(&FakeApi, msg)
        .await
        .map(|s| serde_json::from_str(&s).expect("valid json response"))
}

fn err_code(v: &Value) -> i64 {
    v["error"]["code"].as_i64().expect("error code")
}

#[tokio::test]
async fn success_results_are_objects() {
    let ws = call(r#"{"jsonrpc":"2.0","id":1,"method":"workspace.list"}"#)
        .await
        .unwrap();
    assert!(ws["result"].is_object());
    assert!(ws["result"]["workspaces"].is_array());
    assert_eq!(ws["id"], serde_json::json!(1));

    let notes =
        call(r#"{"jsonrpc":"2.0","id":2,"method":"note.list","params":{"workspaceId":"ws-1"}}"#)
            .await
            .unwrap();
    assert!(notes["result"]["notes"].is_array());
}

#[tokio::test]
async fn parse_error_is_minus_32700() {
    let v = call("{not json").await.unwrap();
    assert_eq!(err_code(&v), -32700);
    assert_eq!(v["id"], Value::Null);
}

#[tokio::test]
async fn invalid_request_matrix() {
    for msg in [
        r#"[1,2,3]"#,
        r#"{"jsonrpc":"1.0","id":1,"method":"workspace.list"}"#,
        r#"{"jsonrpc":"2.0","id":1,"method":""}"#,
        r#"{"jsonrpc":"2.0","id":true,"method":"workspace.list"}"#,
    ] {
        let v = call(msg).await.unwrap();
        assert_eq!(err_code(&v), -32600, "msg={msg}");
    }
}

#[tokio::test]
async fn unknown_method_request_is_minus_32601() {
    let v = call(r#"{"jsonrpc":"2.0","id":9,"method":"nope.method"}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32601);
}

#[tokio::test]
async fn missing_workspace_id_is_minus_32602() {
    let v = call(r#"{"jsonrpc":"2.0","id":3,"method":"note.list","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
}

#[tokio::test]
async fn bad_params_type_is_minus_32602() {
    let v = call(r#"{"jsonrpc":"2.0","id":4,"method":"workspace.list","params":5}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
}

#[tokio::test]
async fn notifications_get_no_response() {
    assert!(
        handle_message(&FakeApi, r#"{"jsonrpc":"2.0","method":"workspace.list"}"#)
            .await
            .is_none()
    );
    assert!(
        handle_message(&FakeApi, r#"{"jsonrpc":"2.0","method":"nope"}"#)
            .await
            .is_none()
    );
    // id: null present IS a request needing a response.
    assert!(handle_message(
        &FakeApi,
        r#"{"jsonrpc":"2.0","id":null,"method":"workspace.list"}"#
    )
    .await
    .is_some());
}

#[tokio::test]
async fn domain_not_found_maps_to_minus_32602() {
    let v =
        call(r#"{"jsonrpc":"2.0","id":5,"method":"note.list","params":{"workspaceId":"missing"}}"#)
            .await
            .unwrap();
    assert_eq!(err_code(&v), -32602);
}

#[tokio::test]
async fn workspace_get_returns_workspace_object() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.get","params":{"workspaceId":"ws-1"}}"#,
    )
    .await
    .unwrap();
    assert!(v["result"]["workspace"].is_object());
    assert_eq!(v["result"]["workspace"]["id"], serde_json::json!("ws-1"));
}

#[tokio::test]
async fn workspace_get_missing_id_is_minus_32602_with_message() {
    let v = call(r#"{"jsonrpc":"2.0","id":1,"method":"workspace.get","params":{}}"#)
        .await
        .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Missing required parameter: workspaceId")
    );
}

#[tokio::test]
async fn workspace_get_not_found_is_minus_32602_with_message() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.get","params":{"workspaceId":"missing"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(err_code(&v), -32602);
    assert_eq!(
        v["error"]["message"],
        serde_json::json!("Workspace not found")
    );
}

#[tokio::test]
async fn workspace_create_returns_workspace_object() {
    let v =
        call(r#"{"jsonrpc":"2.0","id":1,"method":"workspace.create","params":{"title":"New WS"}}"#)
            .await
            .unwrap();
    assert_eq!(
        v["result"]["workspace"]["title"],
        serde_json::json!("New WS")
    );
}

#[tokio::test]
async fn workspace_update_returns_workspace_object() {
    let v = call(
        r#"{"jsonrpc":"2.0","id":1,"method":"workspace.update","params":{"workspaceId":"ws-1","title":"Renamed"}}"#,
    )
    .await
    .unwrap();
    assert_eq!(
        v["result"]["workspace"]["title"],
        serde_json::json!("Renamed")
    );
}

#[tokio::test]
async fn workspace_lifecycle_methods_return_success_true() {
    for method in [
        "workspace.delete",
        "workspace.archive",
        "workspace.unarchive",
    ] {
        let msg = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"{method}","params":{{"workspaceId":"ws-1"}}}}"#
        );
        let v = call(&msg).await.unwrap();
        assert_eq!(v["result"]["success"], serde_json::json!(true), "{method}");
    }
}

#[tokio::test]
async fn workspace_attention_methods_clear_attention() {
    for method in ["workspace.dismissAttention", "workspace.markSeen"] {
        let msg = format!(
            r#"{{"jsonrpc":"2.0","id":1,"method":"{method}","params":{{"workspaceId":"ws-1"}}}}"#
        );
        let v = call(&msg).await.unwrap();
        assert_eq!(
            v["result"]["workspace"]["attention"],
            serde_json::json!("none"),
            "{method}"
        );
    }
}

#[tokio::test]
async fn workspace_mutations_missing_id_is_minus_32602() {
    for method in [
        "workspace.update",
        "workspace.delete",
        "workspace.archive",
        "workspace.unarchive",
        "workspace.dismissAttention",
        "workspace.markSeen",
    ] {
        let msg = format!(r#"{{"jsonrpc":"2.0","id":1,"method":"{method}","params":{{}}}}"#);
        let v = call(&msg).await.unwrap();
        assert_eq!(err_code(&v), -32602, "{method}");
    }
}
