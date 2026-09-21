//! Unit tests for the transport-side `browser.exec` classifier + handler.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::Duration;

use intent_core::{
    BoxFuture, BrowserTab, BrowserTabInput, BrowserTabVisibility, ClientHostInfo, ClientId, Error,
    Result, WorkspaceApi, WorkspaceId,
};
use serde_json::{json, Value};
use tokio::sync::mpsc;

use super::*;
use crate::reverse::{
    PrimaryReverseRegistry, ReverseChannel, ReverseClientIdentity, ReverseTransport,
};

/// Spawn a mock FE that reads one reverse-RPC frame off `out_rx` and replies
/// with `reply` (already carrying the right `id`). Returns the join handle so
/// the test can assert on the request the daemon actually forwarded. The
/// `out_rx.recv()` is wrapped in a fail-safe `tokio::time::timeout` (repo
/// convention) so a bug that never forwards the frame surfaces as a clear
/// test failure instead of hanging the runtime.
fn mock_fe_replies_with(
    mut out_rx: mpsc::Receiver<String>,
    reverse: ReverseChannel,
    reply: Value,
) -> tokio::task::JoinHandle<Value> {
    tokio::spawn(async move {
        let frame = tokio::time::timeout(Duration::from_secs(2), out_rx.recv())
            .await
            .expect("daemon did not forward a reverse RPC within 2s")
            .expect("daemon forwarded a reverse RPC");
        let req: Value = serde_json::from_str(&frame).expect("valid JSON frame");
        let id = req["id"]
            .as_str()
            .expect("reverse id is a string")
            .to_string();
        let mut response = reply;
        if let Some(obj) = response.as_object_mut() {
            obj.insert("jsonrpc".to_string(), Value::String("2.0".to_string()));
            obj.insert("id".to_string(), Value::String(id));
        }
        assert!(reverse.route_response(&response));
        req
    })
}

#[tokio::test]
async fn classify_recognizes_browser_exec() {
    let value = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "browser.exec",
        "params": { "actions": [{ "action": "listTabs" }] }
    });
    let req = classify(&value).expect("browser.exec is classified");
    assert!(req.id_present);
    assert!(matches!(req.method, BrowserMethod::Exec));
    assert!(req.params.contains_key("actions"));
}

#[tokio::test]
async fn classify_ignores_other_methods() {
    let value = json!({
        "jsonrpc": "2.0", "id": 1, "method": "host.status"
    });
    assert!(classify(&value).is_none());
}

#[tokio::test]
async fn exec_forwards_and_reshapes_single_action() {
    let (out_tx, out_rx) = mpsc::channel(8);
    let reverse = ReverseChannel::new(out_tx);
    let reply = json!({
        "result": { "success": true, "results": [
            { "action": "listTabs", "success": true, "result": [{ "id": "tab-1" }] }
        ]}
    });
    let mock = mock_fe_replies_with(out_rx, reverse.clone(), reply);
    let params = serde_json::from_value::<serde_json::Map<String, Value>>(json!({
        "actions": [{ "action": "listTabs" }],
        "tabId": "tab-1",
        "agentId": "agent-1",
        "workspaceId": "ws-1"
    }))
    .unwrap();
    let shaped = exec(&params, &reverse).await.expect("exec succeeds");
    let forwarded = mock.await.unwrap();
    assert_eq!(forwarded["method"], "browser.exec");
    assert_eq!(forwarded["params"]["actions"][0]["action"], "listTabs");
    assert_eq!(forwarded["params"]["agentId"], "agent-1");
    assert_eq!(forwarded["params"]["workspaceId"], "ws-1");
    assert_eq!(shaped["action"], "listTabs");
    assert_eq!(shaped["result"][0]["id"], "tab-1");
}

#[tokio::test]
async fn exec_forwards_and_reshapes_multi_action() {
    let (out_tx, out_rx) = mpsc::channel(8);
    let reverse = ReverseChannel::new(out_tx);
    let reply = json!({
        "result": { "success": true, "results": [
            { "action": "listTabs", "success": true, "result": [] },
            { "action": "screenshot", "success": true, "result": { "base64": "..." } }
        ]}
    });
    let _mock = mock_fe_replies_with(out_rx, reverse.clone(), reply);
    let params = serde_json::from_value::<serde_json::Map<String, Value>>(json!({
        "actions": [{ "action": "listTabs" }, { "action": "screenshot" }]
    }))
    .unwrap();
    let shaped = exec(&params, &reverse).await.expect("exec succeeds");
    let arr = shaped["results"].as_array().expect("results[] for multi");
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[1]["action"], "screenshot");
}

#[tokio::test]
async fn exec_rejects_missing_actions_before_forwarding() {
    let (out_tx, mut out_rx) = mpsc::channel(8);
    let reverse = ReverseChannel::new(out_tx);
    let params = serde_json::Map::new();
    let err = exec(&params, &reverse).await.expect_err("must reject");
    assert_eq!(err.code(), browser_ops::INVALID_PARAMS);
    assert!(err.to_string().contains("actions"));
    // Nothing was written to the outbound queue — validation short-circuits.
    assert!(matches!(
        tokio::time::timeout(Duration::from_millis(20), out_rx.recv()).await,
        Err(_) | Ok(None)
    ));
}

#[tokio::test]
async fn exec_rejects_empty_actions_before_forwarding() {
    let (out_tx, mut out_rx) = mpsc::channel(8);
    let reverse = ReverseChannel::new(out_tx);
    let params = serde_json::from_value::<serde_json::Map<String, Value>>(json!({
        "actions": []
    }))
    .unwrap();
    let err = exec(&params, &reverse).await.expect_err("must reject");
    assert_eq!(err.code(), browser_ops::INVALID_PARAMS);
    assert!(err.to_string().contains("empty"));
    assert!(matches!(
        tokio::time::timeout(Duration::from_millis(20), out_rx.recv()).await,
        Err(_) | Ok(None)
    ));
}

#[tokio::test]
async fn handle_frames_invalid_params_with_data_code() {
    let (out_tx, _out_rx) = mpsc::channel(8);
    let reverse = ReverseChannel::new(out_tx);
    let req = classify(&json!({
        "jsonrpc": "2.0", "id": 5, "method": "browser.exec", "params": {}
    }))
    .unwrap();
    let frame = handle(req, &reverse, no_tabs()).await.expect("error frame");
    let parsed: Value = serde_json::from_str(&frame).unwrap();
    assert_eq!(parsed["id"], 5);
    assert_eq!(parsed["error"]["code"], -32602);
    assert_eq!(parsed["error"]["data"]["code"], "invalid-params");
}

// ---------------------------------------------------------------------------
// Browser tab registry methods (REV-2 Model 2 & 6)
// ---------------------------------------------------------------------------

/// A `TabContext` for the `browser.exec` tests: no persistence, no identity.
fn no_tabs() -> TabContext<'static> {
    static NOOP: NoopApi = NoopApi;
    TabContext {
        api: &NOOP,
        client_id: None,
        registry: None,
    }
}

struct NoopApi;
impl WorkspaceApi for NoopApi {}

/// In-memory `WorkspaceApi` over a `tabId → BrowserTab` map that records
/// every registry call so the tests can assert which host the transport
/// bound to a report. Host-conflict semantics mirror the store: a report
/// about a tab hosted elsewhere is `InvalidParams`.
#[derive(Default)]
struct MemTabs {
    tabs: Mutex<BTreeMap<String, BrowserTab>>,
    calls: Mutex<Vec<String>>,
}

impl MemTabs {
    fn with_tab(tab_id: &str, host: &str) -> Self {
        let api = Self::default();
        api.tabs.lock().unwrap().insert(
            tab_id.to_string(),
            BrowserTab {
                tab_id: tab_id.to_string(),
                workspace_id: WorkspaceId("ws-1".to_string()),
                host_client_id: ClientId(host.to_string()),
                url: "https://a.test/".to_string(),
                requested_url: None,
                title: Some("A".to_string()),
                owner_agent_id: None,
                owner_agent_name: None,
                visibility: BrowserTabVisibility::Visible,
                emulated_size: None,
                displayed: Some(true),
                created_at: "2026-09-06T00:00:00Z".to_string(),
                updated_at: "2026-09-06T00:00:00Z".to_string(),
            },
        );
        api
    }
}

impl WorkspaceApi for MemTabs {
    fn browser_list_tabs(
        &self,
        workspace_id: WorkspaceId,
    ) -> BoxFuture<'_, Result<Vec<BrowserTab>>> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("list:{}", workspace_id.0));
        let tabs: Vec<BrowserTab> = self
            .tabs
            .lock()
            .unwrap()
            .values()
            .filter(|t| t.workspace_id == workspace_id)
            .cloned()
            .collect();
        Box::pin(async move { Ok(tabs) })
    }

    fn browser_upsert_tab(
        &self,
        host: ClientId,
        input: BrowserTabInput,
    ) -> BoxFuture<'_, Result<BrowserTab>> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("upsert:{}:{}", host.0, input.tab_id));
        let mut tabs = self.tabs.lock().unwrap();
        let result = match tabs.get_mut(&input.tab_id) {
            Some(existing) if existing.host_client_id != host => {
                Err(Error::InvalidParams(format!(
                    "browser tab {} is hosted by client {}",
                    input.tab_id, existing.host_client_id
                )))
            }
            Some(existing) => {
                existing.apply_input(input);
                Ok(existing.clone())
            }
            None => {
                let tab = BrowserTab {
                    tab_id: input.tab_id.clone(),
                    workspace_id: input.workspace_id.clone(),
                    host_client_id: host,
                    url: input.url.clone(),
                    requested_url: input.requested_url.clone(),
                    title: input.title.clone(),
                    owner_agent_id: input.owner_agent_id.clone(),
                    owner_agent_name: input.owner_agent_name.clone(),
                    visibility: input.visibility,
                    emulated_size: input.emulated_size,
                    displayed: input.displayed,
                    created_at: "now".to_string(),
                    updated_at: "now".to_string(),
                };
                tabs.insert(tab.tab_id.clone(), tab.clone());
                Ok(tab)
            }
        };
        Box::pin(async move { result })
    }

    fn browser_remove_tab(&self, host: ClientId, tab_id: String) -> BoxFuture<'_, Result<()>> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("remove:{}:{tab_id}", host.0));
        let mut tabs = self.tabs.lock().unwrap();
        let result = match tabs.get(&tab_id) {
            Some(existing) if existing.host_client_id != host => {
                Err(Error::InvalidParams(format!(
                    "browser tab {tab_id} is hosted by client {}",
                    existing.host_client_id
                )))
            }
            _ => {
                tabs.remove(&tab_id);
                Ok(())
            }
        };
        Box::pin(async move { result })
    }

    fn browser_sync_tabs(
        &self,
        host: ClientId,
        inputs: Vec<BrowserTabInput>,
    ) -> BoxFuture<'_, Result<Vec<String>>> {
        self.calls.lock().unwrap().push(format!(
            "sync:{}:{}",
            host.0,
            inputs
                .iter()
                .map(|t| t.tab_id.as_str())
                .collect::<Vec<_>>()
                .join(",")
        ));
        let tabs = self.tabs.lock().unwrap();
        let drop: Vec<String> = inputs
            .iter()
            .filter(|i| {
                tabs.get(&i.tab_id)
                    .is_some_and(|t| t.host_client_id != host)
            })
            .map(|i| i.tab_id.clone())
            .collect();
        Box::pin(async move { Ok(drop) })
    }

    fn browser_navigate_tab(&self, tab_id: String, url: String) -> BoxFuture<'_, Result<Value>> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("navigate:{tab_id}:{url}"));
        let known = self.tabs.lock().unwrap().contains_key(&tab_id);
        Box::pin(async move {
            if known {
                Ok(json!({ "action": "navigate", "success": true, "result": { "url": url } }))
            } else {
                Err(Error::InvalidParams(format!(
                    "browser.navigateTab: tab not found: {tab_id}"
                )))
            }
        })
    }

    fn browser_close_tab(&self, tab_id: String, force: bool) -> BoxFuture<'_, Result<Value>> {
        self.calls
            .lock()
            .unwrap()
            .push(format!("close:{tab_id}:{force}"));
        let known = self.tabs.lock().unwrap().contains_key(&tab_id);
        Box::pin(async move {
            if known {
                Ok(json!({ "ok": true }))
            } else {
                Err(Error::InvalidParams(format!(
                    "browser.closeTab: tab not found: {tab_id}"
                )))
            }
        })
    }
}

fn client(id: &str) -> ClientId {
    ClientId(id.to_string())
}

/// A registry with one live, hello'd connection for `client_id` named `name`.
/// The guard is returned so the registration outlives the test body.
fn registry_with_live(
    client_id: &str,
    name: &str,
) -> (PrimaryReverseRegistry, crate::reverse::PrimaryReverseGuard) {
    let registry = PrimaryReverseRegistry::new();
    let (out_tx, _out_rx) = mpsc::channel(8);
    let guard = registry.register(ReverseChannel::new(out_tx), ReverseTransport::Wss);
    guard.bind(ReverseClientIdentity {
        client_id: client(client_id),
        name: Some(name.to_string()),
        capabilities: json!({ "browserExec": true }),
        host: ClientHostInfo::default(),
    });
    (registry, guard)
}

async fn call(
    method: &str,
    params: Value,
    api: &dyn WorkspaceApi,
    client_id: Option<&ClientId>,
    registry: Option<&PrimaryReverseRegistry>,
) -> Value {
    let (out_tx, _out_rx) = mpsc::channel(8);
    let reverse = ReverseChannel::new(out_tx);
    let req = classify(&json!({
        "jsonrpc": "2.0", "id": 7, "method": method, "params": params
    }))
    .expect("classified");
    let ctx = TabContext {
        api,
        client_id,
        registry,
    };
    let frame = handle(req, &reverse, ctx).await.expect("response frame");
    serde_json::from_str(&frame).unwrap()
}

#[tokio::test]
async fn classify_recognizes_registry_methods() {
    for (method, expected) in [
        ("browser.listTabs", "ListTabs"),
        ("browser.upsertTab", "UpsertTab"),
        ("browser.removeTab", "RemoveTab"),
        ("browser.syncTabs", "SyncTabs"),
        ("browser.navigateTab", "NavigateTab"),
        ("browser.closeTab", "CloseTab"),
    ] {
        let req = classify(&json!({ "jsonrpc": "2.0", "id": 1, "method": method }))
            .unwrap_or_else(|| panic!("{method} is classified"));
        let got = match req.method {
            BrowserMethod::Exec => "Exec",
            BrowserMethod::ListTabs => "ListTabs",
            BrowserMethod::UpsertTab => "UpsertTab",
            BrowserMethod::RemoveTab => "RemoveTab",
            BrowserMethod::SyncTabs => "SyncTabs",
            BrowserMethod::NavigateTab => "NavigateTab",
            BrowserMethod::CloseTab => "CloseTab",
        };
        assert_eq!(got, expected, "{method}");
    }
}

#[test]
fn only_host_reports_need_the_bound_identity() {
    for (method, expected) in [
        ("browser.exec", false),
        ("browser.listTabs", false),
        ("browser.upsertTab", true),
        ("browser.removeTab", true),
        ("browser.syncTabs", true),
        ("browser.navigateTab", false),
        ("browser.closeTab", false),
    ] {
        let req = classify(&json!({ "jsonrpc": "2.0", "id": 1, "method": method }))
            .unwrap_or_else(|| panic!("{method} is classified"));
        assert_eq!(req.method.reports_as_host(), expected, "{method}");
    }
}

#[tokio::test]
async fn navigate_tab_routes_through_the_service_and_echoes_the_envelope() {
    let api = MemTabs::with_tab("tab-1", "client-a");
    let resp = call(
        "browser.navigateTab",
        json!({ "tabId": "tab-1", "url": "https://b.test/" }),
        &api,
        None,
        None,
    )
    .await;
    assert_eq!(resp["result"]["action"], "navigate");
    assert_eq!(resp["result"]["success"], true);
    assert_eq!(
        api.calls.lock().unwrap().as_slice(),
        ["navigate:tab-1:https://b.test/"]
    );
}

#[tokio::test]
async fn navigate_tab_validates_params_before_the_service() {
    let api = MemTabs::with_tab("tab-1", "client-a");
    for params in [
        json!({ "url": "https://b.test/" }),
        json!({ "tabId": "tab-1" }),
        json!({ "tabId": "", "url": "https://b.test/" }),
    ] {
        let resp = call("browser.navigateTab", params.clone(), &api, None, None).await;
        assert_eq!(resp["error"]["code"], -32602, "{params}");
    }
    assert!(api.calls.lock().unwrap().is_empty());
    let resp = call(
        "browser.navigateTab",
        json!({ "tabId": "nope", "url": "https://b.test/" }),
        &api,
        None,
        None,
    )
    .await;
    assert_eq!(resp["error"]["code"], -32602);
    assert!(resp["error"]["message"]
        .as_str()
        .unwrap()
        .contains("tab not found: nope"));
}

#[tokio::test]
async fn close_tab_parses_force_and_answers_ok() {
    let api = MemTabs::with_tab("tab-1", "client-a");
    let resp = call(
        "browser.closeTab",
        json!({ "tabId": "tab-1" }),
        &api,
        None,
        None,
    )
    .await;
    assert_eq!(resp["result"], json!({ "ok": true }));
    let resp = call(
        "browser.closeTab",
        json!({ "tabId": "tab-1", "force": true }),
        &api,
        None,
        None,
    )
    .await;
    assert_eq!(resp["result"], json!({ "ok": true }));
    assert_eq!(
        api.calls.lock().unwrap().as_slice(),
        ["close:tab-1:false", "close:tab-1:true"]
    );
    let resp = call(
        "browser.closeTab",
        json!({ "tabId": "tab-1", "force": "yes" }),
        &api,
        None,
        None,
    )
    .await;
    assert_eq!(resp["error"]["code"], -32602);
    let resp = call(
        "browser.closeTab",
        json!({ "tabId": "nope" }),
        &api,
        None,
        None,
    )
    .await;
    assert_eq!(resp["error"]["code"], -32602);
}

#[tokio::test]
async fn list_tabs_decorates_host_presence() {
    let api = MemTabs::with_tab("tab-1", "client-a");
    let template = api.tabs.lock().unwrap()["tab-1"].clone();
    api.tabs.lock().unwrap().insert(
        "tab-2".to_string(),
        BrowserTab {
            tab_id: "tab-2".to_string(),
            host_client_id: client("client-offline"),
            displayed: None,
            ..template
        },
    );
    let (registry, _guard) = registry_with_live("client-a", "Desktop A");
    let res = call(
        "browser.listTabs",
        json!({ "workspaceId": "ws-1" }),
        &api,
        None,
        Some(&registry),
    )
    .await;
    let tabs = res["result"]["tabs"].as_array().expect("tabs array");
    assert_eq!(tabs.len(), 2);
    let by_id = |id: &str| tabs.iter().find(|t| t["tabId"] == id).unwrap().clone();
    let live = by_id("tab-1");
    assert_eq!(live["hostClientId"], "client-a");
    assert_eq!(live["hostConnected"], true);
    assert_eq!(live["hostName"], "Desktop A");
    assert_eq!(live["url"], "https://a.test/");
    assert_eq!(live["visibility"], "visible");
    assert_eq!(live["displayed"], true);
    assert!(
        live.get("requestedUrl").is_none(),
        "unset optionals are omitted"
    );
    let offline = by_id("tab-2");
    assert_eq!(offline["hostConnected"], false);
    assert!(
        offline.get("hostName").is_none(),
        "offline host has no name"
    );
    assert!(
        offline.get("displayed").is_none(),
        "an unreported displayed is omitted, never false"
    );
    // Listing needs no hello (viewers and un-hello'd clients may read).
    assert_eq!(api.calls.lock().unwrap().as_slice(), ["list:ws-1"]);
}

#[tokio::test]
async fn list_tabs_without_registry_reports_everyone_offline() {
    let api = MemTabs::with_tab("tab-1", "client-a");
    let res = call(
        "browser.listTabs",
        json!({ "workspaceId": "ws-1" }),
        &api,
        None,
        None,
    )
    .await;
    assert_eq!(res["result"]["tabs"][0]["hostConnected"], false);
    let res = call("browser.listTabs", json!({}), &api, None, None).await;
    assert_eq!(res["error"]["code"], -32602);
    assert_eq!(res["error"]["data"]["code"], "invalid-params");
}

#[tokio::test]
async fn upsert_tab_binds_caller_as_host() {
    let api = MemTabs::default();
    let me = client("client-a");
    let res = call(
        "browser.upsertTab",
        json!({
            "workspaceId": "ws-1",
            "tab": {
                "tabId": "tab-new",
                "url": "https://a.test/",
                "title": "A",
                "ownerAgentId": "agent-1",
                "visibility": "hidden",
                "emulatedSize": { "width": 1280, "height": 800 }
            }
        }),
        &api,
        Some(&me),
        None,
    )
    .await;
    let tab = &res["result"]["tab"];
    assert_eq!(tab["tabId"], "tab-new");
    assert_eq!(tab["workspaceId"], "ws-1");
    assert_eq!(tab["hostClientId"], "client-a");
    assert_eq!(tab["ownerAgentId"], "agent-1");
    assert_eq!(tab["visibility"], "hidden");
    assert_eq!(tab["emulatedSize"], json!({ "width": 1280, "height": 800 }));
    assert_eq!(
        api.calls.lock().unwrap().as_slice(),
        ["upsert:client-a:tab-new"]
    );
}

#[tokio::test]
async fn upsert_tab_rejects_foreign_host_and_bad_params() {
    let api = MemTabs::with_tab("tab-1", "client-a");
    let other = client("client-b");
    let res = call(
        "browser.upsertTab",
        json!({ "workspaceId": "ws-1", "tab": { "tabId": "tab-1", "url": "https://b.test/" } }),
        &api,
        Some(&other),
        None,
    )
    .await;
    assert_eq!(res["error"]["code"], -32602);
    assert!(res["error"]["message"]
        .as_str()
        .unwrap()
        .contains("hosted by client client-a"));
    // Missing `tab`, non-object `tab`, and a tab without `url` are all -32602.
    for params in [
        json!({ "workspaceId": "ws-1" }),
        json!({ "workspaceId": "ws-1", "tab": "nope" }),
        json!({ "workspaceId": "ws-1", "tab": { "tabId": "t" } }),
        json!({ "workspaceId": "ws-1", "tab": { "tabId": "", "url": "https://x/" } }),
        json!({ "tab": { "tabId": "t", "url": "https://x/" } }),
    ] {
        let res = call(
            "browser.upsertTab",
            params.clone(),
            &api,
            Some(&other),
            None,
        )
        .await;
        assert_eq!(res["error"]["code"], -32602, "{params}");
        assert_eq!(res["error"]["data"]["code"], "invalid-params", "{params}");
    }
}

#[tokio::test]
async fn host_methods_require_hello() {
    let api = MemTabs::with_tab("tab-1", "client-a");
    for (method, params) in [
        (
            "browser.upsertTab",
            json!({ "workspaceId": "ws-1", "tab": { "tabId": "tab-1", "url": "https://a.test/" } }),
        ),
        ("browser.removeTab", json!({ "tabId": "tab-1" })),
        ("browser.syncTabs", json!({ "tabs": [] })),
    ] {
        let res = call(method, params, &api, None, None).await;
        assert_eq!(res["error"]["code"], -32602, "{method}");
        assert!(
            res["error"]["message"]
                .as_str()
                .unwrap()
                .contains("client.hello is required"),
            "{method}: {}",
            res["error"]["message"]
        );
    }
    assert!(
        api.calls.lock().unwrap().is_empty(),
        "nothing reached the api"
    );
}

#[tokio::test]
async fn remove_tab_is_host_scoped() {
    let api = MemTabs::with_tab("tab-1", "client-a");
    let other = client("client-b");
    let res = call(
        "browser.removeTab",
        json!({ "tabId": "tab-1" }),
        &api,
        Some(&other),
        None,
    )
    .await;
    assert_eq!(res["error"]["code"], -32602);
    let me = client("client-a");
    let res = call(
        "browser.removeTab",
        json!({ "tabId": "tab-1" }),
        &api,
        Some(&me),
        None,
    )
    .await;
    assert_eq!(res["result"], json!({ "ok": true }));
    assert!(api.tabs.lock().unwrap().is_empty());
    let res = call("browser.removeTab", json!({}), &api, Some(&me), None).await;
    assert_eq!(res["error"]["code"], -32602);
}

#[tokio::test]
async fn sync_tabs_returns_drop_list() {
    let api = MemTabs::with_tab("tab-b", "client-b");
    let me = client("client-a");
    let res = call(
        "browser.syncTabs",
        json!({ "tabs": [
            { "tabId": "tab-a", "workspaceId": "ws-1", "url": "https://a.test/" },
            { "tabId": "tab-b", "workspaceId": "ws-1", "url": "https://b.test/" }
        ] }),
        &api,
        Some(&me),
        None,
    )
    .await;
    assert_eq!(res["result"], json!({ "drop": ["tab-b"] }));
    assert_eq!(
        api.calls.lock().unwrap().as_slice(),
        ["sync:client-a:tab-a,tab-b"]
    );
    // Each entry needs its own workspaceId; `tabs` must be an array.
    let res = call(
        "browser.syncTabs",
        json!({ "tabs": [{ "tabId": "tab-a", "url": "https://a.test/" }] }),
        &api,
        Some(&me),
        None,
    )
    .await;
    assert_eq!(res["error"]["code"], -32602);
    let res = call(
        "browser.syncTabs",
        json!({ "tabs": {} }),
        &api,
        Some(&me),
        None,
    )
    .await;
    assert_eq!(res["error"]["code"], -32602);
}

#[tokio::test]
async fn exec_surfaces_no_frontend_connected_as_proxy_error() {
    let (out_tx, out_rx) = mpsc::channel(8);
    // Drop the receiver immediately: mirrors "no frontend connected" — the
    // outbound queue has no reader, so the reverse channel's send fails.
    drop(out_rx);
    let reverse = ReverseChannel::new(out_tx);
    let params = serde_json::from_value::<serde_json::Map<String, Value>>(json!({
        "actions": [{ "action": "listTabs" }]
    }))
    .unwrap();
    let err = exec(&params, &reverse)
        .await
        .expect_err("no frontend connected");
    assert_eq!(err.code(), browser_ops::INTERNAL_ERROR);
    assert!(err.to_string().contains("closed") || err.to_string().contains("browser.exec"));
}

#[tokio::test]
async fn exec_propagates_fe_error_as_proxy_error() {
    let (out_tx, out_rx) = mpsc::channel(8);
    let reverse = ReverseChannel::new(out_tx);
    let reply = json!({
        "error": { "code": -32603, "message": "CDP not attached" }
    });
    let _mock = mock_fe_replies_with(out_rx, reverse.clone(), reply);
    let params = serde_json::from_value::<serde_json::Map<String, Value>>(json!({
        "actions": [{ "action": "listTabs" }]
    }))
    .unwrap();
    let err = exec(&params, &reverse).await.expect_err("FE errored");
    assert_eq!(err.code(), browser_ops::INTERNAL_ERROR);
    assert!(err.to_string().contains("CDP not attached"));
}

#[tokio::test]
async fn exec_surfaces_fe_failure_envelope() {
    let (out_tx, out_rx) = mpsc::channel(8);
    let reverse = ReverseChannel::new(out_tx);
    let reply = json!({
        "result": { "success": false, "error": "no tab focused", "results": [] }
    });
    let _mock = mock_fe_replies_with(out_rx, reverse.clone(), reply);
    let params = serde_json::from_value::<serde_json::Map<String, Value>>(json!({
        "actions": [{ "action": "screenshot" }]
    }))
    .unwrap();
    let err = exec(&params, &reverse).await.expect_err("failure envelope");
    assert_eq!(err.code(), browser_ops::INTERNAL_ERROR);
    assert!(err.to_string().contains("no tab focused"));
}
