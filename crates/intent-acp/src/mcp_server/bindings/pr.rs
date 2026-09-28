//! Shared `ws.pr.*` / `ws.mr.*` observation bindings.
//!
//! The namespace exposes the read-only `pr.snapshot` (compact, diff-friendly
//! PR state) plus the centralized PR-monitor surface — `pr.monitor` /
//! `pr.unmonitor` / `pr.monitors`, gated by `agentFeatures.prMonitor`. Every
//! other PR operation (create, view, comment, review threads, branch update,
//! merge) is intentionally unbound — agents use the `gh` CLI instead. The
//! bindings only peel arguments and forward the trait's `serde_json::Value`
//! result unchanged.
//!
//! `ws.mr` aliases the same object, arguments and results as `ws.pr`; the
//! spelling never selects a provider. Both exist independently of remotes,
//! and both currently use the same GitHub observation backend.
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
    ws.mr = ws.pr;
";

/// The `agentFeatures.prMonitor` segment of the shared prelude: the three
/// monitor installers, appended to [`PRELUDE`] only when the toggle is on. A
/// unit test guards that the segment stays syntactically attachable.
pub(crate) const MONITOR_PRELUDE_SEGMENT: &str = r"
    ws.pr.monitor = (prNumber, options) =>
        host({ method: 'pr.monitor', args: { prNumber, ...(options || {}) } });
    ws.pr.unmonitor = (prNumber, options) =>
        host({ method: 'pr.unmonitor', args: { prNumber, ...(options || {}) } });
    ws.pr.monitors = () => host({ method: 'pr.monitors' });
";

/// Feature-aware `ws.pr` / `ws.mr` prelude: the monitor installers are omitted when
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
    use std::sync::Mutex;

    use crate::WorkspaceMcpServer;

    struct RecordingApi {
        calls: Mutex<Vec<Value>>,
        response: Value,
        error: Option<&'static str>,
        retired: bool,
    }

    impl RecordingApi {
        fn record(
            &self,
            method: &str,
            ws: &WorkspaceId,
            owner: Option<AgentId>,
            number: Option<u64>,
            repo: Option<&str>,
        ) -> BoxFuture<'_, Result<Value>> {
            self.calls.lock().unwrap().push(json!({
                "method": method, "workspace": ws.as_str(),
                "owner": owner.map(|id| id.to_string()), "number": number, "repo": repo,
            }));
            Box::pin(async {
                if let Some(message) = self.error {
                    return Err(intent_core::Error::Internal(message.to_string()));
                }
                Ok(self.response.clone())
            })
        }
    }

    impl WorkspaceApi for RecordingApi {
        fn settings_get(&self, path: String) -> BoxFuture<'_, Result<Value>> {
            Box::pin(async move {
                // Keep result comparisons independent of TOON and spill thresholds.
                let value = match path.as_str() {
                    "workspaceApi.toonOutput" => json!(false),
                    "workspaceApi.maxOutputChars" => json!(0),
                    _ => Value::Null,
                };
                Ok(json!({ "path": path, "value": value }))
            })
        }

        fn agent_is_retired(&self, _agent_id: AgentId) -> BoxFuture<'_, bool> {
            Box::pin(async { self.retired })
        }

        fn pr_state(
            &self,
            ws: WorkspaceId,
            number: u64,
            repo: Option<String>,
        ) -> BoxFuture<'_, Result<Value>> {
            self.record("snapshot", &ws, None, Some(number), repo.as_deref())
        }

        fn pr_monitor_start(
            &self,
            ws: WorkspaceId,
            owner: AgentId,
            number: u64,
            repo: Option<String>,
        ) -> BoxFuture<'_, Result<Value>> {
            self.record("monitor", &ws, Some(owner), Some(number), repo.as_deref())
        }

        fn pr_monitor_stop(
            &self,
            ws: WorkspaceId,
            owner: AgentId,
            number: u64,
            repo: Option<String>,
        ) -> BoxFuture<'_, Result<Value>> {
            self.record("unmonitor", &ws, Some(owner), Some(number), repo.as_deref())
        }

        fn pr_monitor_list(
            &self,
            ws: WorkspaceId,
            owner: Option<AgentId>,
        ) -> BoxFuture<'_, Result<Value>> {
            self.record("monitors", &ws, owner, None, None)
        }
    }

    fn bridge(api: Arc<RecordingApi>, enabled: bool, caller: bool) -> WorkspaceMcpServer {
        WorkspaceMcpServer::new(api, WorkspaceId::from_string("ws-observation"))
            .with_caller_agent_id(caller.then(|| AgentId::from("agent-owner")))
            .with_agent_features(AgentFeaturesSettings {
                pr_monitor: enabled,
                ..AgentFeaturesSettings::default()
            })
    }

    async fn call(server: &WorkspaceMcpServer, code: &str) -> Value {
        let response = server
            .handle_message(&json!({
                "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                "params": { "name": "workspace_api", "arguments": {
                    "code": code, "summary": "Observation alias regression test",
                } },
            }))
            .await
            .unwrap();
        assert_eq!(response["jsonrpc"], "2.0");
        assert_eq!(response["id"], 1);
        response["result"].clone()
    }

    fn body(result: &Value) -> Value {
        assert_eq!(result["isError"], false, "{result}");
        serde_json::from_str(result["content"][0]["text"].as_str().unwrap()).unwrap()
    }

    fn recording_api(response: Value) -> Arc<RecordingApi> {
        Arc::new(RecordingApi {
            calls: Mutex::new(Vec::new()),
            response,
            error: None,
            retired: false,
        })
    }

    #[tokio::test]
    async fn aliases_forward_the_same_arguments_owners_and_results_through_js_and_raw_host() {
        for (method, response) in [
            (
                "snapshot",
                json!({ "repo": "o/r", "prNumber": 7, "requirements": { "threads": {} }, "extension": null }),
            ),
            ("snapshot", Value::Null),
            (
                "monitor",
                json!({ "ok": true, "monitor": { "monitorId": "m", "prNumber": 7 }, "requirements": null, "pausedUntil": "2026-09-27T13:00:00Z" }),
            ),
            (
                "monitor",
                json!({ "ok": false, "refused": true, "reason": "already-monitored", "ownerAgentId": "another-agent", "monitorId": "m", "prNumber": 7 }),
            ),
            (
                "unmonitor",
                json!({ "ok": true, "monitor": { "monitorId": "m", "state": "completed" } }),
            ),
            (
                "monitors",
                json!({ "monitors": [{ "monitorId": "m", "prNumber": 7, "pendingChanges": { "checks": true } }] }),
            ),
            ("monitors", json!([])),
        ] {
            for repo in [None, Some("o/r")] {
                let api = recording_api(response.clone());
                let server = bridge(api.clone(), true, true);
                let args = if method == "monitors" {
                    json!({})
                } else {
                    json!({ "prNumber": 7, "repo": repo })
                };
                let js_args = if method == "monitors" {
                    String::new()
                } else {
                    format!("7, {{ repo: {} }}", json!(repo))
                };
                let expected = if method == "monitors" {
                    response.get("monitors").unwrap_or(&response)
                } else {
                    &response
                };
                let mut results = Vec::new();
                for ns in ["pr", "mr"] {
                    for code in [
                        format!("return await ws.{ns}.{method}({js_args});"),
                        format!("return await host({{ method: '{ns}.{method}', args: {args} }});"),
                    ] {
                        let result = call(&server, &code).await;
                        assert_eq!(&body(&result), expected, "{code}");
                        results.push(result);
                    }
                }
                assert!(results.windows(2).all(|pair| pair[0] == pair[1]));
                let expected_call = json!({
                    "method": method, "workspace": "ws-observation",
                    "owner": if method == "snapshot" { None } else { Some("agent-owner") },
                    "number": if method == "monitors" { None } else { Some(7) },
                    "repo": if method == "monitors" { None } else { repo },
                });
                assert_eq!(*api.calls.lock().unwrap(), vec![expected_call; 4]);
            }
        }
    }

    #[tokio::test]
    async fn aliases_share_validation_errors_before_service_dispatch() {
        let api = recording_api(Value::Null);
        let server = bridge(api.clone(), true, true);
        for method in ["snapshot", "monitor", "unmonitor"] {
            for (args, expected) in [
                ("", "prNumber is required"),
                ("0", "prNumber is required"),
                ("-1", "prNumber is required"),
                ("'abc'", "prNumber is required"),
                ("7, { repo: 123 }", "repo must be an"),
            ] {
                let mut results = Vec::new();
                for ns in ["pr", "mr"] {
                    let result =
                        call(&server, &format!("return await ws.{ns}.{method}({args});")).await;
                    assert_eq!(result["isError"], true);
                    assert!(result["content"][0]["text"]
                        .as_str()
                        .unwrap()
                        .contains(expected));
                    results.push(result);
                }
                assert_eq!(results[0], results[1]);
            }
        }
        assert!(api.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn aliases_preserve_backend_failures_without_selecting_a_different_provider() {
        let mut api = recording_api(Value::Null);
        Arc::get_mut(&mut api).unwrap().error = Some("No repository is configured");
        let server = bridge(api.clone(), true, false);
        let pr = call(&server, "return await ws.pr.snapshot(7);").await;
        let mr = call(&server, "return await ws.mr.snapshot(7);").await;
        assert_eq!(pr, mr);
        assert_eq!(mr["isError"], true);
        assert!(mr["content"][0]["text"]
            .as_str()
            .unwrap()
            .contains("No repository is configured"));
        let calls = api.calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0], calls[1]);
    }

    #[tokio::test]
    async fn aliases_require_the_same_monitor_owner_but_snapshot_needs_no_caller() {
        let api = recording_api(json!({ "repo": "o/r", "prNumber": 7 }));
        let server = bridge(api.clone(), true, false);
        for ns in ["pr", "mr"] {
            for method in ["monitor", "unmonitor", "monitors"] {
                let result = call(&server, &format!("return await ws.{ns}.{method}(7);")).await;
                assert_eq!(result["isError"], true);
                assert!(result["content"][0]["text"]
                    .as_str()
                    .unwrap()
                    .contains("requires an agent caller context"));
            }
            assert_eq!(
                body(&call(&server, &format!("return await ws.{ns}.snapshot(7);")).await),
                api.response
            );
        }
        let calls = api.calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert!(calls
            .iter()
            .all(|call| call["method"] == "snapshot" && call["owner"].is_null()));
    }

    #[tokio::test]
    async fn aliases_obey_captured_gate_in_prelude_help_and_raw_dispatch() {
        let api = recording_api(json!({ "prNumber": 7 }));
        let disabled = bridge(api.clone(), false, true);
        let enabled = bridge(api.clone(), true, true);
        for (enabled, server) in [(false, &disabled), (true, &enabled), (false, &disabled)] {
            for ns in ["pr", "mr"] {
                let observed =
                    body(&call(server, &format!("return Object.keys(ws.{ns}).sort();")).await);
                let expected = if enabled {
                    json!(["monitor", "monitors", "snapshot", "unmonitor"])
                } else {
                    json!(["snapshot"])
                };
                assert_eq!(observed, expected);
                let help = body(&call(server, &format!("return await ws.help('{ns}');")).await);
                assert!(help
                    .as_str()
                    .unwrap()
                    .contains(&format!("ws.{ns}.snapshot(")));
                assert_eq!(
                    help.as_str()
                        .unwrap()
                        .contains(&format!("ws.{ns}.monitor(")),
                    enabled
                );
                for method in ["monitor", "unmonitor", "monitors"] {
                    let result = call(server, &format!("return await host({{ method: '{ns}.{method}', args: {{ prNumber: 7 }} }});")).await;
                    assert_eq!(result["isError"], !enabled, "{result}");
                    if !enabled {
                        assert!(result["content"][0]["text"]
                            .as_str()
                            .unwrap()
                            .contains("agentFeatures.prMonitor = false"));
                    }
                }
            }
        }
        assert_eq!(
            api.calls.lock().unwrap().len(),
            6,
            "only the enabled bridge may reach the service"
        );
        for ns in ["pr", "mr"] {
            assert_eq!(
                body(&call(&disabled, &format!("return await ws.{ns}.snapshot(7);")).await),
                api.response
            );
        }
    }

    #[tokio::test]
    async fn aliases_never_add_create_or_bypass_retired_caller_guard() {
        let mut api = recording_api(Value::Null);
        for retired in [false, true] {
            Arc::get_mut(&mut api).unwrap().retired = retired;
            let server = bridge(api.clone(), true, true);
            for ns in ["pr", "mr"] {
                assert_eq!(
                    body(&call(&server, &format!("return typeof ws.{ns}.create;")).await),
                    "undefined"
                );
                let result = call(
                    &server,
                    &format!("return await host({{ method: '{ns}.create', args: {{}} }});"),
                )
                .await;
                assert_eq!(result["isError"], true);
                if retired {
                    let result = call(&server, &format!("return await ws.{ns}.snapshot(7);")).await;
                    assert_eq!(result["isError"], true);
                    assert!(result["content"][0]["text"]
                        .as_str()
                        .unwrap()
                        .contains("retired"));
                } else {
                    assert!(result["content"][0]["text"]
                        .as_str()
                        .unwrap()
                        .contains("unknown method"));
                }
            }
        }
        assert!(api.calls.lock().unwrap().is_empty());
    }

    /// `WorkspaceApi` recording whether the ownership-scoped monitor methods
    /// were reached — the caller-context guards must reject before the
    /// service layer sees the call.
    #[derive(Default)]
    #[expect(clippy::struct_field_names)] // fields mirror the spied method names
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
