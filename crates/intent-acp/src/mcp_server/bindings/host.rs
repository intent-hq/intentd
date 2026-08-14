//! `ws.host.*` bindings.
//!
//! `host.exec` exposes the daemon's one-shot exec primitive (PROTOCOL §5.14)
//! to agent JavaScript with the wire `host.exec` semantics: argv-only (no
//! shell interpolation), optional `timeoutMs` that reaps the whole process
//! group, workspace-cwd containment, and secret-safe env (values never logged
//! or echoed). The binding forwards the raw args object to
//! [`WorkspaceApi::host_exec`]; the concrete impl pins the containment root to
//! the calling workspace and delegates to `intent-services::host_exec::run`.

use std::sync::Arc;

use intent_core::{WorkspaceApi, WorkspaceId};
use serde_json::Value;

use super::map_err;

pub(crate) const PRELUDE: &str = r#"
    globalThis.ws = globalThis.ws || {};
    ws.host = {
        exec: (opts) => host({ method: 'host.exec', args: opts || {} }),
    };
"#;

pub(crate) async fn dispatch(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    match method {
        "exec" => exec(api, ws, args).await,
        other => Err(format!("host: unknown method `host.{other}`")),
    }
}

async fn exec(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    api.host_exec(ws.clone(), args.clone())
        .await
        .map_err(map_err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::{
        BoxFuture, Result, Workspace, WorkspaceActivity, WorkspaceAttention, WorkspaceStatus,
    };
    use serde_json::json;

    /// `WorkspaceApi` whose `host_exec` delegates to the real
    /// `intent-services` seam so the tests exercise the wire `host.exec`
    /// semantics (spawn, timeout reap, cwd containment) end-to-end;
    /// `get_workspace` roots the workspace at a temp dir so the containment
    /// guard has a real absolute root to check against. Owning the `TempDir`
    /// guard removes the dir when the test's api drops (monorepo#1302 —
    /// nothing else ever reclaimed these).
    struct FakeApi {
        root: tempfile::TempDir,
    }

    impl WorkspaceApi for FakeApi {
        fn get_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
            let ws = make_workspace(id.as_str(), &self.root.path().to_string_lossy());
            Box::pin(async move { Ok(ws) })
        }

        fn host_exec(
            &self,
            workspace_id: WorkspaceId,
            params: Value,
        ) -> BoxFuture<'_, Result<Value>> {
            Box::pin(async move {
                intent_services::host_exec::run_for_workspace(self, workspace_id, params).await
            })
        }
    }

    fn make_workspace(id: &str, root: &str) -> Workspace {
        Workspace {
            id: WorkspaceId::from_string(id),
            title: "Host".to_string(),
            branch: "main".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: WorkspaceStatus::Active,
            status_message: None,
            status_image_asset_id: None,
            activity: WorkspaceActivity::Idle,
            attention: WorkspaceAttention::None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            last_activity: None,
            tags: vec![],
            path: None,
            repository_path: None,
            repository_owner: None,
            repository_name: None,
            worktree_path: Some(root.to_string()),
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

    fn api(tag: &str) -> (Arc<dyn WorkspaceApi>, WorkspaceId) {
        let root = tempfile::Builder::new()
            .prefix(&format!("intent-acp-hostexec-{tag}-"))
            .tempdir()
            .unwrap();
        let api: Arc<dyn WorkspaceApi> = Arc::new(FakeApi { root });
        (api, WorkspaceId::from_string("ws-host"))
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn exec_success_captures_stdout_and_exit_code() {
        let (api, ws) = api("ok");
        let r = dispatch(
            &api,
            &ws,
            "exec",
            &json!({ "command": "echo", "args": ["hi"] }),
        )
        .await
        .unwrap();
        assert_eq!(r["stdout"], "hi\n");
        assert_eq!(r["exitCode"], 0);
        assert!(r.get("timedOut").is_none());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn exec_reports_non_zero_exit_code() {
        let (api, ws) = api("exit");
        let r = dispatch(
            &api,
            &ws,
            "exec",
            &json!({ "command": "sh", "args": ["-c", "exit 7"] }),
        )
        .await
        .unwrap();
        assert_eq!(r["exitCode"], 7);
        assert!(r.get("timedOut").is_none());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn exec_timeout_reaps_and_flags_timed_out() {
        let (api, ws) = api("timeout");
        let r = dispatch(
            &api,
            &ws,
            "exec",
            &json!({ "command": "sleep", "args": ["30"], "timeoutMs": 100 }),
        )
        .await
        .unwrap();
        assert_eq!(r["timedOut"], true);
    }

    #[tokio::test]
    async fn exec_rejects_cwd_outside_workspace() {
        let (api, ws) = api("cwd");
        let err = dispatch(
            &api,
            &ws,
            "exec",
            &json!({ "command": "echo", "cwd": "../../outside" }),
        )
        .await
        .unwrap_err();
        assert!(
            err.contains("cwd outside workspace"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn dispatch_rejects_unknown_method() {
        let (api, ws) = api("unknown");
        let err = dispatch(&api, &ws, "nope", &json!({})).await.unwrap_err();
        assert!(err.contains("unknown method"));
    }
}
