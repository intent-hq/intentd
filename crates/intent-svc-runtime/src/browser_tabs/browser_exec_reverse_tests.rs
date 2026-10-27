// ---------------------------------------------------------------------------
// REV-1: `browser.exec` routes through the injected `AgentReverseDispatch`.
// ---------------------------------------------------------------------------
use std::sync::{Arc, Mutex};

use intent_core::{
    AgentReverseDispatch, BoxFuture, ClientHostInfo, ClientId, Error, ReverseDispatchError,
    ReverseTarget, WorkspaceApi, WorkspaceId,
};
use intent_store::Store;
use serde_json::{json, Value};

use intent_services::feature_api::test_support::fixtures::{workspace, TempDb, WorkspacesRoot};
use intent_services::feature_api::Services;

#[derive(Default)]
struct RecordingDispatch {
    calls: Mutex<Vec<(String, Value, ReverseTarget)>>,
    reply: Mutex<Option<Value>>,
    err: Mutex<Option<ReverseDispatchError>>,
}

impl RecordingDispatch {
    fn with_reply(reply: Value) -> Arc<Self> {
        let d = Self::default();
        *d.reply.lock().unwrap() = Some(reply);
        Arc::new(d)
    }
    fn with_error(err: ReverseDispatchError) -> Arc<Self> {
        let d = Self::default();
        *d.err.lock().unwrap() = Some(err);
        Arc::new(d)
    }
}

impl AgentReverseDispatch for RecordingDispatch {
    fn is_connected(&self) -> bool {
        true
    }
    fn dispatch<'a>(
        &'a self,
        method: &'a str,
        params: Value,
        target: ReverseTarget,
    ) -> BoxFuture<'a, Result<Value, ReverseDispatchError>> {
        self.calls
            .lock()
            .unwrap()
            .push((method.to_string(), params.clone(), target));
        let reply = self.reply.lock().unwrap().clone();
        let err = self.err.lock().unwrap().clone();
        Box::pin(async move {
            if let Some(e) = err {
                return Err(e);
            }
            Ok(reply.unwrap_or(json!({ "success": true, "results": [] })))
        })
    }
}

/// Services over a store that knows workspace `ws-1` and the hello'd
/// client `desktop-a` (both are foreign keys of a registry tab row).
async fn services_with(
    dispatch: Arc<dyn AgentReverseDispatch>,
) -> (TempDb, WorkspacesRoot, Services) {
    let tmp = TempDb::new();
    let root = WorkspacesRoot::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    store
        .insert_workspace(&workspace(&WorkspaceId::from("ws-1")))
        .await
        .expect("ws");
    store
        .upsert_client(
            &ClientId::from_string("desktop-a"),
            Some("Desktop A"),
            Some(&json!({ "browserExec": true })),
            &ClientHostInfo::default(),
        )
        .await
        .expect("client");
    let svc = Services::new(store)
        .with_workspaces_root(root.path().to_path_buf())
        .with_reverse_dispatch(dispatch);
    (tmp, root, svc)
}

/// Register an open, unclaimed `tab_id` of `ws-1` hosted by `host`, the
/// way the host's own `browser.upsertTab` report would.
async fn register_tab(svc: &Services, tab_id: &str, host: &str) {
    svc.browser_upsert_tab(
        ClientId::from_string(host),
        serde_json::from_value(json!({
            "tabId": tab_id,
            "workspaceId": "ws-1",
            "url": "https://a.test/",
        }))
        .unwrap(),
    )
    .await
    .expect("tab registered");
}

#[tokio::test]
async fn browser_exec_without_dispatch_returns_no_client_error() {
    let tmp = TempDb::new();
    let root = WorkspacesRoot::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let svc = Services::new(store).with_workspaces_root(root.path().to_path_buf());
    let err = svc
        .browser_exec(
            WorkspaceId::from("ws-1"),
            vec![json!({"action":"screenshot"})],
            None,
            None,
        )
        .await
        .expect_err("no dispatch");
    assert!(matches!(err, Error::Internal(m) if m.contains("no client connected")));
}

#[tokio::test]
async fn browser_exec_forwards_actions_and_returns_single_result_envelope() {
    let dispatch = RecordingDispatch::with_reply(json!({
        "success": true,
        "results": [{ "action": "screenshot", "success": true, "result": {} }],
    }));
    let (_tmp, _root, svc) = services_with(dispatch.clone()).await;
    register_tab(&svc, "tab-1", "desktop-a").await;
    let out = svc
        .browser_exec(
            WorkspaceId::from("ws-1"),
            vec![json!({ "action": "screenshot" })],
            Some("tab-1".to_string()),
            None,
        )
        .await
        .expect("ok");
    assert_eq!(out["action"], "screenshot");
    assert_eq!(out["success"], true);
    let calls = dispatch.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].0, "browser.exec");
    assert_eq!(calls[0].1["tabId"], "tab-1");
    assert_eq!(calls[0].1["actions"].as_array().unwrap().len(), 1);
    // REV-1: attribution — the `WorkspaceId` argument must be threaded
    // into the forwarded reverse-RPC params so the FE sees the same
    // envelope shape the client-triggered `browser.exec` path emits.
    assert_eq!(calls[0].1["workspaceId"], "ws-1");
    // REV-2 Model 10: an unpinned workspace whose only tab is unclaimed
    // has no driving client yet — the first-connected eligible client
    // is asked (the pin / claimed-host cases live in `browser_routing`).
    assert_eq!(calls[0].2, ReverseTarget::Default);
}

/// REV-2 Model 5: every `tabId` an agent batch names must be an open
/// registry tab of the calling workspace; nothing is forwarded otherwise.
#[tokio::test]
async fn browser_exec_rejects_unknown_or_foreign_tab_ids_before_forwarding() {
    let dispatch = RecordingDispatch::with_reply(json!({ "success": true, "results": [] }));
    let (_tmp, _root, svc) = services_with(dispatch.clone()).await;
    register_tab(&svc, "tab-1", "desktop-a").await;
    for (actions, tab_id, missing) in [
        (
            vec![json!({ "action": "screenshot" })],
            Some("ghost"),
            "ghost",
        ),
        (
            vec![json!({ "action": "closeTab", "tabId": "ghost" })],
            None,
            "ghost",
        ),
        (
            vec![
                json!({ "action": "screenshot", "tabId": "tab-1" }),
                json!({ "action": "closeTab", "tabId": "ghost" }),
            ],
            None,
            "ghost",
        ),
    ] {
        let err = svc
            .browser_exec(
                WorkspaceId::from("ws-1"),
                actions,
                tab_id.map(str::to_string),
                None,
            )
            .await
            .expect_err("unknown tab");
        assert!(
            matches!(&err, Error::InvalidParams(m) if m == &format!("browser.exec: tab not found: {missing}")),
            "{err:?}"
        );
    }
    // A registered tab of ANOTHER workspace is equally unknown here.
    let err = svc
        .browser_exec(
            WorkspaceId::from("ws-2"),
            vec![json!({ "action": "screenshot" })],
            Some("tab-1".to_string()),
            None,
        )
        .await
        .expect_err("foreign tab");
    assert!(matches!(err, Error::InvalidParams(m) if m.contains("tab not found: tab-1")));
    assert!(dispatch.calls.lock().unwrap().is_empty());
}

/// REV-2 Model 5: `listTabs` is answered from the registry, so it never
/// shares a batch with FE-executed actions.
#[tokio::test]
async fn browser_exec_rejects_list_tabs_mixed_with_other_actions() {
    let dispatch = RecordingDispatch::with_reply(json!({ "success": true, "results": [] }));
    let (_tmp, _root, svc) = services_with(dispatch.clone()).await;
    let err = svc
        .browser_exec(
            WorkspaceId::from("ws-1"),
            vec![
                json!({ "action": "listTabs" }),
                json!({ "action": "screenshot" }),
            ],
            None,
            None,
        )
        .await
        .expect_err("mixed batch");
    assert!(matches!(err, Error::InvalidParams(m) if m.contains("listTabs")));
    assert!(dispatch.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn browser_exec_rejects_empty_actions_with_invalid_params() {
    let dispatch = RecordingDispatch::with_reply(json!({ "success": true, "results": [] }));
    let (_tmp, _root, svc) = services_with(dispatch.clone()).await;
    let err = svc
        .browser_exec(WorkspaceId::from("ws-1"), vec![], None, None)
        .await
        .expect_err("empty batch");
    assert!(matches!(err, Error::InvalidParams(m) if m.contains("non-empty")));
    // Guard runs before dispatch, so nothing is forwarded downstream.
    assert!(dispatch.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn browser_exec_surfaces_transport_error() {
    let dispatch = RecordingDispatch::with_error(ReverseDispatchError::Transport {
        code: 0,
        message: "timeout".into(),
    });
    let (_tmp, _root, svc) = services_with(dispatch).await;
    let err = svc
        .browser_exec(
            WorkspaceId::from("ws-1"),
            vec![json!({ "action": "screenshot" })],
            None,
            None,
        )
        .await
        .expect_err("transport");
    assert!(matches!(err, Error::Internal(m) if m.contains("timeout")));
}

// Regression (monorepo#3042): a `success: false` FE envelope that carries
// structured per-action errors (not-owner / already-claimed) must come
// back as data on the agent seam — not flattened into `Error::Internal`.
#[tokio::test]
async fn browser_exec_preserves_structured_single_action_ownership_failure() {
    let dispatch = RecordingDispatch::with_reply(json!({
        "success": false,
        "error": "Tab tab-9 is not owned by you",
        "results": [{
            "action": "resizeTab",
            "success": false,
            "errorCode": "not-owner",
            "ownerAgentId": null,
            "error": "Tab tab-9 is not owned by you (owner: none). Claim it with claimTab first."
        }],
    }));
    let (_tmp, _root, svc) = services_with(dispatch).await;
    register_tab(&svc, "tab-9", "desktop-a").await;
    let out = svc
        .browser_exec(
            WorkspaceId::from("ws-1"),
            vec![json!({ "action": "resizeTab", "tabId": "tab-9", "width": 375 })],
            None,
            None,
        )
        .await
        .expect("structured failure is data, not Err");
    assert_eq!(out["action"], "resizeTab");
    assert_eq!(out["success"], json!(false));
    assert_eq!(out["errorCode"], "not-owner");
    assert_eq!(out["ownerAgentId"], Value::Null);
}

#[tokio::test]
async fn browser_exec_preserves_structured_multi_action_partial_failure() {
    let dispatch = RecordingDispatch::with_reply(json!({
        "success": false,
        "error": "1 of 2 actions failed",
        "results": [
            { "action": "screenshot", "success": true, "result": {} },
            {
                "action": "claimTab",
                "success": false,
                "errorCode": "already-claimed",
                "ownerAgentId": "agent-42",
                "error": "Tab tab-3 is owned by agent agent-42"
            }
        ],
    }));
    let (_tmp, _root, svc) = services_with(dispatch).await;
    register_tab(&svc, "tab-3", "desktop-a").await;
    let out = svc
        .browser_exec(
            WorkspaceId::from("ws-1"),
            vec![
                json!({ "action": "screenshot" }),
                json!({ "action": "claimTab", "tabId": "tab-3", "width": 1280 }),
            ],
            None,
            None,
        )
        .await
        .expect("partial failure is data, not Err");
    assert_eq!(out["success"], json!(false));
    let results = out["results"].as_array().expect("results preserved");
    assert_eq!(results.len(), 2);
    assert_eq!(results[1]["errorCode"], "already-claimed");
    assert_eq!(results[1]["ownerAgentId"], "agent-42");
}

// The FE aborts a batch on the first failing action and returns the
// partial results collected so far: a 3-action batch failing at action 1
// replies with a single result. The shape must key off the request
// arity, so multi-action callers still get the envelope form.
#[tokio::test]
async fn browser_exec_multi_action_first_failure_keeps_envelope_shape() {
    let dispatch = RecordingDispatch::with_reply(json!({
        "success": false,
        "error": "Tab tab-9 is not owned by you",
        "results": [{
            "action": "resizeTab",
            "success": false,
            "errorCode": "not-owner",
            "ownerAgentId": "agent-7",
            "error": "Tab tab-9 is owned by agent agent-7"
        }],
    }));
    let (_tmp, _root, svc) = services_with(dispatch).await;
    register_tab(&svc, "tab-9", "desktop-a").await;
    let out = svc
        .browser_exec(
            WorkspaceId::from("ws-1"),
            vec![
                json!({ "action": "resizeTab", "tabId": "tab-9", "width": 375 }),
                json!({ "action": "screenshot" }),
                json!({ "action": "getAccessibilityTree" }),
            ],
            None,
            None,
        )
        .await
        .expect("aborted batch is data, not Err");
    assert_eq!(out["success"], json!(false));
    let results = out["results"].as_array().expect("results key present");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["errorCode"], "not-owner");
    assert_eq!(results[0]["ownerAgentId"], "agent-7");
}

#[tokio::test]
async fn browser_exec_failure_without_results_still_errors() {
    let dispatch = RecordingDispatch::with_reply(json!({
        "success": false,
        "error": "CDP not attached",
    }));
    let (_tmp, _root, svc) = services_with(dispatch).await;
    let err = svc
        .browser_exec(
            WorkspaceId::from("ws-1"),
            vec![json!({ "action": "screenshot" })],
            None,
            None,
        )
        .await
        .expect_err("no per-action detail to preserve");
    assert!(matches!(err, Error::Internal(m) if m.contains("CDP not attached")));
}
