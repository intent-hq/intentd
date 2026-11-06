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

use intent_core::{AgentId, WorkspaceApi, WorkspaceId};
use serde_json::Value;

use super::map_err;

pub(crate) const PRELUDE: &str = r"
    globalThis.ws = globalThis.ws || {};
    ws.host = {
        exec: (opts) => host({ method: 'host.exec', args: opts || {} }),
    };
";

/// `secrets` is whether agent secret requests are effective for this caller
/// (see [`super::secret_requests_effective`]); only then does `secretEnv`
/// reach the agent-scoped exec, which resolves it from the caller's own
/// memory-held secrets (PROTOCOL §5.56).
pub(crate) async fn dispatch(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller: Option<&AgentId>,
    secrets: bool,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    match method {
        "exec" => exec(api, ws, caller, secrets, args).await,
        other => Err(format!("host: unknown method `host.{other}`")),
    }
}

async fn exec(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller: Option<&AgentId>,
    secrets: bool,
    args: &Value,
) -> Result<Value, String> {
    if args.get("secretEnv").is_some_and(|spec| !spec.is_null()) {
        let Some(caller) = caller.filter(|_| secrets) else {
            return Err(format!(
                "host: secretEnv is unavailable — {}",
                super::secret::SECRET_REQUESTS_UNAVAILABLE
            ));
        };
        return api
            .agent_host_exec(ws.clone(), caller.clone(), args.clone())
            .await
            .map_err(map_err);
    }
    // `secretEnv: null` is the same as omitting it: drop the key so the
    // plain exec's parser (which rejects any present `secretEnv`) accepts it.
    let mut args = args.clone();
    if let Some(fields) = args.as_object_mut() {
        fields.remove("secretEnv");
    }
    api.host_exec(ws.clone(), args).await.map_err(map_err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::{BoxFuture, Result, Workspace};
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
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            worktree_path: Some(root.to_string()),
            ..Default::default()
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
    async fn null_secret_env_runs_a_plain_exec() {
        let (api, ws) = api("nullsecret");
        for secrets in [false, true] {
            let r = dispatch(
                &api,
                &ws,
                None,
                secrets,
                "exec",
                &json!({ "command": "echo", "args": ["ok"], "secretEnv": null }),
            )
            .await
            .unwrap();
            assert_eq!(r["exitCode"], json!(0), "{r}");
            assert_eq!(r["stdout"], json!("ok\n"), "{r}");
        }
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn exec_success_captures_stdout_and_exit_code() {
        let (api, ws) = api("ok");
        let r = dispatch(
            &api,
            &ws,
            None,
            false,
            "exec",
            &json!({ "command": "echo", "args": ["hi"] }),
        )
        .await
        .unwrap();
        assert_eq!(r["stdout"], "hi\n");
        assert_eq!(r["exitCode"], 0);
        assert!(r.get("timedOut").is_none());
    }

    /// Regression for intent-hq/monorepo#3231: an omitted `cwd` must default
    /// to the workspace root, not the daemon's own process cwd — in-hook
    /// `git fetch` calls silently ran from the daemon's cwd and failed.
    #[cfg(unix)]
    #[tokio::test]
    async fn exec_defaults_cwd_to_workspace_root() {
        let (api, ws) = api("default-cwd");
        let root = api
            .get_workspace(ws.clone())
            .await
            .unwrap()
            .worktree_path
            .unwrap();
        let r = dispatch(&api, &ws, None, false, "exec", &json!({ "command": "pwd" }))
            .await
            .unwrap();
        assert_eq!(r["exitCode"], 0);
        let printed = r["stdout"].as_str().unwrap().trim();
        // macOS `/tmp` resolves through a `/private` symlink; accept either.
        let canonical = std::fs::canonicalize(&root)
            .map_or_else(|_| root.clone(), |p| p.to_string_lossy().into_owned());
        assert!(
            printed == root || printed == canonical,
            "pwd must print the workspace root ({root} or {canonical}), got {printed}"
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn exec_reports_non_zero_exit_code() {
        let (api, ws) = api("exit");
        let r = dispatch(
            &api,
            &ws,
            None,
            false,
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
            None,
            false,
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
            None,
            false,
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
        let err = dispatch(&api, &ws, None, false, "nope", &json!({}))
            .await
            .unwrap_err();
        assert!(err.contains("unknown method"));
    }
}
