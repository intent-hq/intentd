//! `ws.pr.*` bindings (WSAPI-6).
//!
//! The namespace exposes the read-only `pr.snapshot` (compact, diff-friendly
//! PR state) plus the centralized PR-monitor surface — `pr.monitor` /
//! `pr.unmonitor` / `pr.monitors`, gated by `agentFeatures.prMonitor`. Every
//! other PR operation (create, view, comment, review threads, branch update,
//! merge) is intentionally unbound — agents use the `gh` CLI instead. The
//! bindings only peel arguments and forward the trait's `serde_json::Value`
//! result unchanged.
//!
//! Monitors are agent-owned, so `pr.monitor` / `pr.unmonitor` / `pr.monitors`
//! require an agent caller context (mirroring `ws.hook.schedule`): the FE
//! front door manages monitors through the `prMonitor.*` wire methods.

use std::sync::Arc;

use intent_core::{AgentId, WorkspaceApi, WorkspaceId};
use serde_json::Value;

use super::{map_err, req_i64};

pub(crate) const PRELUDE: &str = r"
    globalThis.ws = globalThis.ws || {};
    ws.pr = {
        snapshot: (prNumber, options) =>
            host({ method: 'pr.snapshot', args: { prNumber, ...(options || {}) } }),
    };
";

/// The `agentFeatures.prMonitor` segment of the `ws.pr` prelude: the three
/// monitor installers, appended to [`PRELUDE`] only when the toggle is on. A
/// unit test guards that the segment stays syntactically attachable.
pub(crate) const MONITOR_PRELUDE_SEGMENT: &str = r"
    ws.pr.monitor = (prNumber, options) =>
        host({ method: 'pr.monitor', args: { prNumber, ...(options || {}) } });
    ws.pr.unmonitor = (prNumber, options) =>
        host({ method: 'pr.unmonitor', args: { prNumber, ...(options || {}) } });
    ws.pr.monitors = () => host({ method: 'pr.monitors' });
";

/// Feature-aware `ws.pr` prelude: the monitor installers are omitted when
/// `agentFeatures.prMonitor` is off, so agent code touching them fails with a
/// clear `ws.pr.monitor is not a function` `TypeError`.
pub(crate) fn prelude_for(features: &intent_core::settings_file::AgentFeaturesSettings) -> String {
    let mut out = PRELUDE.to_string();
    if features.pr_monitor {
        out.push_str(MONITOR_PRELUDE_SEGMENT);
    }
    out
}

pub(crate) async fn dispatch(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller: Option<&AgentId>,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    match method {
        "snapshot" => snapshot(api, ws, args).await,
        "monitor" => monitor(api, ws, caller, args).await,
        "unmonitor" => unmonitor(api, ws, caller, args).await,
        "monitors" => monitors(api, ws, caller).await,
        other => Err(format!("host: unknown method `pr.{other}`")),
    }
}

/// The `prNumber` every `ws.pr.*` binding requires, as a positive number.
fn req_pr_number(args: &Value) -> Result<u64, String> {
    let pr_number =
        req_i64(args, "prNumber").map_err(|_| "prNumber is required and must be a number")?;
    if pr_number <= 0 {
        return Err("prNumber is required and must be a number".to_string());
    }
    Ok(pr_number.cast_unsigned())
}

/// The optional cross-repo override; slug validation lives in the engine, but
/// a present-yet-non-string value fails fast rather than silently falling
/// back to the workspace repo.
fn opt_repo(args: &Value) -> Result<Option<String>, String> {
    match args.get("repo") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) => Ok(Some(s.clone())),
        Some(_) => Err("repo must be an \"owner/name\" string".to_string()),
    }
}

async fn snapshot(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let pr_number = req_pr_number(args)?;
    let repo = opt_repo(args)?;
    api.pr_state(ws.clone(), pr_number, repo)
        .await
        .map_err(map_err)
}

async fn monitor(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    let Some(owner) = caller else {
        return Err(
            "pr.monitor requires an agent caller context to attribute ownership".to_string(),
        );
    };
    let pr_number = req_pr_number(args)?;
    let repo = opt_repo(args)?;
    api.pr_monitor_start(ws.clone(), owner.clone(), pr_number, repo)
        .await
        .map_err(map_err)
}

async fn unmonitor(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    let Some(owner) = caller else {
        return Err(
            "pr.unmonitor requires an agent caller context to verify monitor ownership".to_string(),
        );
    };
    let pr_number = req_pr_number(args)?;
    let repo = opt_repo(args)?;
    api.pr_monitor_stop(ws.clone(), owner.clone(), pr_number, repo)
        .await
        .map_err(map_err)
}

async fn monitors(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller: Option<&AgentId>,
) -> Result<Value, String> {
    let Some(owner) = caller else {
        return Err(
            "pr.monitors requires an agent caller context to scope the listing".to_string(),
        );
    };
    let raw = api
        .pr_monitor_list(ws.clone(), Some(owner.clone()))
        .await
        .map_err(map_err)?;
    // The service returns `{ monitors: [...] }` (the wire shape); JS callers
    // get the bare array, mirroring `ws.hook.list`.
    if let Some(inner) = raw.get("monitors") {
        return Ok(inner.clone());
    }
    Ok(raw)
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::settings_file::AgentFeaturesSettings;
    use intent_core::{BoxFuture, Result};
    use serde_json::json;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// `WorkspaceApi` recording whether the ownership-scoped monitor methods
    /// were reached — the caller-context guards must reject before the
    /// service layer sees the call.
    #[derive(Default)]
    #[allow(clippy::struct_field_names)] // fields mirror the spied method names
    struct SpyApi {
        start_called: AtomicBool,
        stop_called: AtomicBool,
        list_called: AtomicBool,
    }

    impl WorkspaceApi for SpyApi {
        fn pr_monitor_start(
            &self,
            _workspace_id: WorkspaceId,
            _agent_id: AgentId,
            _pr_number: u64,
            _repo: Option<String>,
        ) -> BoxFuture<'_, Result<Value>> {
            self.start_called.store(true, Ordering::SeqCst);
            Box::pin(async { Ok(json!({ "ok": true })) })
        }

        fn pr_monitor_stop(
            &self,
            _workspace_id: WorkspaceId,
            _agent_id: AgentId,
            _pr_number: u64,
            _repo: Option<String>,
        ) -> BoxFuture<'_, Result<Value>> {
            self.stop_called.store(true, Ordering::SeqCst);
            Box::pin(async { Ok(json!({ "ok": true })) })
        }

        fn pr_monitor_list(
            &self,
            _workspace_id: WorkspaceId,
            _agent_id: Option<AgentId>,
        ) -> BoxFuture<'_, Result<Value>> {
            self.list_called.store(true, Ordering::SeqCst);
            Box::pin(async { Ok(json!({ "monitors": [{ "prNumber": 7 }] })) })
        }
    }

    fn spy() -> (Arc<SpyApi>, Arc<dyn WorkspaceApi>, WorkspaceId) {
        let spy = Arc::new(SpyApi::default());
        let api: Arc<dyn WorkspaceApi> = spy.clone();
        (spy, api, WorkspaceId::from_string("ws-pr"))
    }

    #[tokio::test]
    async fn monitor_methods_without_caller_context_never_reach_the_service() {
        for (method, args) in [
            ("monitor", json!({ "prNumber": 7 })),
            ("unmonitor", json!({ "prNumber": 7 })),
            ("monitors", json!({})),
        ] {
            let (spy, api, ws) = spy();
            let err = dispatch(&api, &ws, None, method, &args).await.unwrap_err();
            assert!(
                err.contains("requires an agent caller context"),
                "unexpected error for `{method}`: {err}"
            );
            assert!(
                !spy.start_called.load(Ordering::SeqCst)
                    && !spy.stop_called.load(Ordering::SeqCst)
                    && !spy.list_called.load(Ordering::SeqCst),
                "service must not be reached for `{method}`"
            );
        }
    }

    #[tokio::test]
    async fn monitor_and_unmonitor_reach_the_service_with_a_caller() {
        let (spy, api, ws) = spy();
        let caller = AgentId::from("agent-caller");
        dispatch(
            &api,
            &ws,
            Some(&caller),
            "monitor",
            &json!({ "prNumber": 7, "repo": "o/n" }),
        )
        .await
        .expect("monitor dispatched");
        dispatch(
            &api,
            &ws,
            Some(&caller),
            "unmonitor",
            &json!({ "prNumber": 7 }),
        )
        .await
        .expect("unmonitor dispatched");
        assert!(spy.start_called.load(Ordering::SeqCst));
        assert!(spy.stop_called.load(Ordering::SeqCst));
    }

    /// `ws.pr.monitors()` unwraps the wire envelope to the bare array, like
    /// `ws.hook.list()`.
    #[tokio::test]
    async fn monitors_unwraps_the_envelope_to_a_bare_array() {
        let (_spy, api, ws) = spy();
        let caller = AgentId::from("agent-caller");
        let out = dispatch(&api, &ws, Some(&caller), "monitors", &json!({}))
            .await
            .expect("monitors dispatched");
        assert_eq!(out, json!([{ "prNumber": 7 }]));
    }

    /// `prNumber` validation is shared by every `ws.pr.*` binding.
    #[tokio::test]
    async fn monitor_rejects_a_missing_or_non_positive_pr_number() {
        let (_spy, api, ws) = spy();
        let caller = AgentId::from("agent-caller");
        for args in [json!({}), json!({ "prNumber": 0 })] {
            let err = dispatch(&api, &ws, Some(&caller), "monitor", &args)
                .await
                .unwrap_err();
            assert!(err.contains("prNumber is required"), "unexpected: {err}");
        }
    }

    /// `agentFeatures.prMonitor` off omits the three monitor installers while
    /// keeping `ws.pr.snapshot` intact.
    #[test]
    fn prelude_gates_only_the_monitor_installers() {
        let on = prelude_for(&AgentFeaturesSettings::default());
        for marker in ["ws.pr.monitor =", "ws.pr.unmonitor =", "ws.pr.monitors ="] {
            assert!(on.contains(marker), "`{marker}` missing when enabled");
        }
        let features = AgentFeaturesSettings {
            pr_monitor: false,
            ..AgentFeaturesSettings::default()
        };
        let off = prelude_for(&features);
        for marker in ["ws.pr.monitor =", "ws.pr.unmonitor =", "ws.pr.monitors ="] {
            assert!(!off.contains(marker), "`{marker}` still installed when off");
        }
        assert!(
            off.contains("snapshot:"),
            "ws.pr.snapshot was wrongly dropped"
        );
    }
}
