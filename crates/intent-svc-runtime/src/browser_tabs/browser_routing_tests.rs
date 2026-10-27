// ---------------------------------------------------------------------------
// REV-2 routing (Model 3–6 & 10): the driving client (pin → desktop session
// → claimed-tab host → first-connected; every rule also runs as a shared
// case in browser_routing_fixture_tests.rs), registry-answered `listTabs`, claim / pin migration of
// tab rows, and the tab-addressed `browser.navigateTab` / `browser.closeTab`.
// ---------------------------------------------------------------------------
use crate::browser_tabs::BrowserTabRoutingExt;
use std::time::Duration;

use intent_core::{AgentId, BrowserTab, ClientId, Error, ReverseTarget, WorkspaceApi, WorkspaceId};
use serde_json::{json, Value};

use super::browser_client_pin_tests::{setup, FakeRegistry};
use intent_services::feature_api::{EventBus, Services, SubscriptionFilter};

fn agent(id: &str) -> AgentId {
    AgentId::from_string(id)
}

/// Register an open tab of `ws` on `host`, owned by `owner` when given.
/// Both the workspace and the host client are foreign keys of the row,
/// so they are created on demand (`setup` already knows `desktop-a` /
/// `desktop-b` and the primary workspace). User tabs report
/// `displayed: true`, agent tabs `false` (hidden).
pub(super) async fn register_tab(
    svc: &Services,
    ws: &WorkspaceId,
    tab_id: &str,
    host: &str,
    owner: Option<&str>,
) -> BrowserTab {
    if svc.store().get_workspace(ws).await.is_err() {
        svc.store()
            .insert_workspace(&intent_services::feature_api::test_support::fixtures::workspace(ws))
            .await
            .unwrap();
    }
    let host_id = ClientId::from_string(host);
    if svc.store().get_client(&host_id).await.unwrap().is_none() {
        svc.store()
            .upsert_client(
                &host_id,
                None,
                Some(&json!({ "browserExec": true })),
                &intent_core::ClientHostInfo::default(),
            )
            .await
            .unwrap();
    }
    svc.browser_upsert_tab(
        ClientId::from_string(host),
        serde_json::from_value(json!({
            "tabId": tab_id,
            "workspaceId": ws.0,
            "url": format!("https://{tab_id}.test/"),
            "title": tab_id,
            "ownerAgentId": owner,
            "visibility": "hidden",
            "emulatedSize": owner.map(|_| json!({ "width": 1280, "height": 800 })),
            "displayed": owner.is_none(),
        }))
        .unwrap(),
    )
    .await
    .expect("tab registered")
}

async fn tab_events(bus: &EventBus, ws: &WorkspaceId, want: usize) -> Vec<Value> {
    let mut sub = bus.subscribe(SubscriptionFilter {
        workspace_id: Some(ws.0.clone()),
        ..Default::default()
    });
    let mut out = Vec::new();
    while out.len() < want {
        let batch = tokio::time::timeout(
            intentd_test_support::test_timeout(Duration::from_secs(2)),
            sub.recv(),
        )
        .await
        .expect("event delivered")
        .expect("subscription open");
        for ev in &batch {
            let ev = serde_json::to_value(ev).unwrap();
            if ev["type"]
                .as_str()
                .is_some_and(|t| t.starts_with("browser:"))
            {
                out.push(ev);
            }
        }
    }
    out
}

fn two_clients() -> std::sync::Arc<FakeRegistry> {
    FakeRegistry::new(&[
        ("desktop-a", Some("Desktop A"), true),
        ("desktop-b", Some("Desktop B"), true),
    ])
}

/// Model 10: without a pin, the host of the workspace's claimed tabs is
/// the driving client — even when another eligible client connected
/// first — and `workspace.getBrowserClient` reports it as `resolved`.
#[intent_test_macros::daemon_test]
async fn claimed_tab_host_is_the_driving_client_when_unpinned() {
    let reg = two_clients();
    let (_t, _r, svc, _bus, ws) = setup(reg.clone()).await;
    register_tab(&svc, &ws, "user-tab", "desktop-a", None).await;
    register_tab(&svc, &ws, "agent-tab", "desktop-b", Some("agent-1")).await;

    let state = svc.get_workspace_browser_client(ws.clone()).await.unwrap();
    assert_eq!(
        state,
        json!({
            "source": "default",
            "resolved": { "clientId": "desktop-b", "name": "Desktop B" }
        })
    );
    let out = svc
        .browser_exec(
            ws.clone(),
            vec![json!({ "action": "openTab", "url": "https://c.test/" })],
            None,
            Some(agent("agent-2")),
        )
        .await
        .expect("dispatch");
    assert_eq!(out["result"]["host"], "desktop-b");
    assert_eq!(
        reg.calls.lock().unwrap()[0].2,
        ReverseTarget::Client(ClientId::from_string("desktop-b"))
    );
}

/// Model 5: an agent `listTabs` is answered from the registry — every
/// host aggregated, no reverse call — with the FE field names plus the
/// host fields, filtered by `scope`.
#[intent_test_macros::daemon_test]
async fn list_tabs_is_answered_from_the_registry_across_hosts() {
    let reg = FakeRegistry::new(&[("desktop-a", Some("Desktop A"), true)]);
    let (_t, _r, svc, _bus, ws) = setup(reg.clone()).await;
    register_tab(&svc, &ws, "t-user", "desktop-a", None).await;
    register_tab(&svc, &ws, "t-mine", "desktop-b", Some("agent-1")).await;
    register_tab(&svc, &ws, "t-other", "desktop-b", Some("agent-2")).await;
    let other_ws = WorkspaceId::new();
    register_tab(&svc, &other_ws, "t-foreign", "desktop-a", None).await;

    let out = svc
        .browser_exec(
            ws.clone(),
            vec![json!({ "action": "listTabs" })],
            None,
            Some(agent("agent-1")),
        )
        .await
        .expect("list");
    assert_eq!(out["action"], "listTabs");
    assert_eq!(out["success"], true);
    let tabs = out["result"].as_array().expect("array");
    let ids: Vec<&str> = tabs.iter().map(|t| t["tabId"].as_str().unwrap()).collect();
    assert_eq!(ids, ["t-user", "t-mine", "t-other"]);
    assert_eq!(
        tabs[0],
        json!({
            "tabId": "t-user", "workspaceId": ws.0, "url": "https://t-user.test/",
            "title": "t-user", "ownerAgentId": null, "mode": "native",
            "visibility": "hidden", "displayed": true, "hostClientId": "desktop-a",
            "hostName": "Desktop A", "hostConnected": true
        })
    );
    assert_eq!(tabs[1]["ownerAgentId"], "agent-1");
    assert_eq!(tabs[1]["mode"], "emulated");
    assert_eq!(tabs[1]["width"], 1280);
    assert_eq!(tabs[1]["displayed"], false);
    assert_eq!(tabs[1]["hostClientId"], "desktop-b");
    assert_eq!(tabs[1]["hostConnected"], false, "desktop-b is not live");
    assert!(tabs[1].get("hostName").is_none());
    assert!(reg.calls.lock().unwrap().is_empty(), "no reverse call");

    let mine = svc
        .browser_exec(
            ws.clone(),
            vec![json!({ "action": "listTabs", "scope": "mine" })],
            None,
            Some(agent("agent-1")),
        )
        .await
        .unwrap();
    assert_eq!(mine["result"].as_array().unwrap().len(), 1);
    assert_eq!(mine["result"][0]["tabId"], "t-mine");
    let unclaimed = svc
        .browser_exec(
            ws.clone(),
            vec![json!({ "action": "listTabs", "scope": "unclaimed" })],
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(unclaimed["result"][0]["tabId"], "t-user");
    assert_eq!(unclaimed["result"].as_array().unwrap().len(), 1);

    let no_agent = svc
        .browser_exec(
            ws.clone(),
            vec![json!({ "action": "listTabs", "scope": "mine" })],
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(no_agent["success"], false, "mine needs an agent caller");
    let err = svc
        .browser_exec(
            ws.clone(),
            vec![json!({ "action": "listTabs", "scope": "yours" })],
            None,
            None,
        )
        .await
        .expect_err("bad scope");
    assert!(matches!(err, Error::InvalidParams(m) if m.contains("scope")));
    // A present non-string scope is rejected, not read as the default.
    for bad in [json!(42), json!({}), json!([])] {
        let err = svc
            .browser_exec(
                ws.clone(),
                vec![json!({ "action": "listTabs", "scope": bad })],
                None,
                None,
            )
            .await
            .expect_err("non-string scope");
        assert!(matches!(err, Error::InvalidParams(m) if m.contains("scope")));
    }
    // The registry answer still honours the tab-id contract: an unknown
    // tab named by the envelope or the action is `-32602`.
    for (action, envelope) in [
        (json!({ "action": "listTabs" }), Some("ghost".to_string())),
        (json!({ "action": "listTabs", "tabId": "ghost" }), None),
    ] {
        let err = svc
            .browser_exec(ws.clone(), vec![action], envelope, None)
            .await
            .expect_err("unknown tab");
        assert!(matches!(err, Error::InvalidParams(m) if m.contains("tab not found: ghost")));
    }
    let scoped = svc
        .browser_exec(
            ws,
            vec![json!({ "action": "listTabs" })],
            Some("t-user".to_string()),
            None,
        )
        .await
        .expect("known envelope tab");
    assert_eq!(scoped["success"], true);
}

/// intent-hq/intent#4835: `displayed` rides the registry-answered
/// `listTabs` entry exactly as the host last reported it — a host that
/// never reported it yields no key (never a default `false`), and a
/// later report flipping it is diffed into `browser:tab-updated {
/// changes: { displayed } }` and reflected on the next list.
#[tokio::test]
async fn list_tabs_carries_displayed_as_reported() {
    let reg = FakeRegistry::new(&[("desktop-a", Some("Desktop A"), true)]);
    let (_t, _r, svc, bus, ws) = setup(reg).await;
    let host = ClientId::from_string("desktop-a");
    let report = |displayed: Option<bool>| -> intent_core::BrowserTabInput {
        serde_json::from_value(json!({
            "tabId": "t-legacy",
            "workspaceId": ws.0,
            "url": "https://t-legacy.test/",
            "visibility": "visible",
            "displayed": displayed,
        }))
        .unwrap()
    };
    svc.browser_upsert_tab(host.clone(), report(None))
        .await
        .expect("registered without displayed");
    let list = || {
        svc.browser_exec(
            ws.clone(),
            vec![json!({ "action": "listTabs" })],
            None,
            None,
        )
    };
    let out = list().await.expect("list");
    assert_eq!(out["result"][0]["tabId"], "t-legacy");
    assert!(
        out["result"][0].get("displayed").is_none(),
        "never reported ⇒ omitted, not false: {}",
        out["result"][0]
    );

    let events = tokio::spawn({
        let bus = bus.clone();
        let ws = ws.clone();
        async move { tab_events(&bus, &ws, 1).await }
    });
    tokio::task::yield_now().await;
    let updated = svc
        .browser_upsert_tab(host.clone(), report(Some(true)))
        .await
        .expect("re-reported with displayed");
    assert_eq!(updated.displayed, Some(true));
    let evs = events.await.unwrap();
    assert_eq!(evs[0]["type"], "browser:tab-updated");
    assert_eq!(evs[0]["data"]["changes"], json!({ "displayed": true }));
    assert_eq!(evs[0]["data"]["tab"]["displayed"], true);
    assert_eq!(list().await.unwrap()["result"][0]["displayed"], true);

    svc.browser_upsert_tab(host, report(Some(false)))
        .await
        .expect("flipped displayed");
    assert_eq!(list().await.unwrap()["result"][0]["displayed"], false);
}

/// Model 5: a `claimTab` the driving client executed on a tab another
/// client hosted re-homes the row (host + owner) with one
/// `browser:tab-updated`; a failed claim moves nothing. The move clears
/// the previous host's `displayed` fact (`changes.displayed: null`, absent
/// on the event `tab` and the next list) until the new host reports it;
/// a same-host owner-only claim keeps it (intent-hq/intent#4835).
#[intent_test_macros::daemon_test]
async fn successful_claim_migrates_the_tab_to_the_driving_client() {
    let reg = two_clients();
    let (_t, _r, svc, bus, ws) = setup(reg.clone()).await;
    register_tab(&svc, &ws, "user-tab", "desktop-b", None).await;

    let out = svc
        .browser_exec(
            ws.clone(),
            vec![json!({ "action": "claimTab", "tabId": "user-tab", "width": 1024 })],
            None,
            Some(agent("agent-1")),
        )
        .await
        .expect("claim");
    assert_eq!(out["action"], "claimTab");
    assert_eq!(reg.calls.lock().unwrap()[0].2, ReverseTarget::Default);
    // The fake's canned reply carries no `result.tabId` — the FE's
    // success marker — so nothing counts as a confirmed claim and the
    // row is untouched.
    let row = svc
        .store()
        .get_browser_tab("user-tab")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.host_client_id, ClientId::from_string("desktop-b"));
    assert!(row.owner_agent_id.is_none());
    assert_eq!(row.displayed, Some(true), "desktop-b reported the fact");

    // Now drive the migration the way a confirmed `claimTab` does.
    let events = tokio::spawn({
        let bus = bus.clone();
        let ws = ws.clone();
        async move { tab_events(&bus, &ws, 1).await }
    });
    tokio::task::yield_now().await;
    svc.browser_tab_claim_migrate(
        "user-tab",
        &ClientId::from_string("desktop-a"),
        Some(&agent("agent-1")),
    )
    .await
    .expect("migrate");
    let row = svc
        .store()
        .get_browser_tab("user-tab")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.host_client_id, ClientId::from_string("desktop-a"));
    assert_eq!(row.owner_agent_id, Some(agent("agent-1")));
    assert_eq!(
        row.displayed, None,
        "desktop-a has not reported the layout fact yet"
    );
    let evs = events.await.unwrap();
    assert_eq!(evs[0]["type"], "browser:tab-updated");
    assert_eq!(
        evs[0]["data"]["changes"],
        json!({ "hostClientId": "desktop-a", "ownerAgentId": "agent-1", "displayed": null })
    );
    assert!(evs[0]["data"]["tab"].get("displayed").is_none());
    let listed = svc
        .browser_exec(
            ws.clone(),
            vec![json!({ "action": "listTabs" })],
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(listed["result"][0]["tabId"], "user-tab");
    assert!(listed["result"][0].get("displayed").is_none());
    // Re-homing the same tab to its current host and owner is a no-op
    // (no event).
    svc.browser_tab_claim_migrate(
        "user-tab",
        &ClientId::from_string("desktop-a"),
        Some(&agent("agent-1")),
    )
    .await
    .unwrap();
    assert!(svc
        .store()
        .list_browser_tabs(&ws)
        .await
        .unwrap()
        .iter()
        .all(|t| t.host_client_id == ClientId::from_string("desktop-a")));

    // A claim of a tab the driving client already hosts records the
    // owner immediately (no wait for the host's own report) with
    // `changes: { ownerAgentId }` only — the host's `displayed` stays.
    register_tab(&svc, &ws, "local-tab", "desktop-a", None).await;
    let events = tokio::spawn({
        let bus = bus.clone();
        let ws = ws.clone();
        async move { tab_events(&bus, &ws, 1).await }
    });
    tokio::task::yield_now().await;
    svc.browser_tab_claim_migrate(
        "local-tab",
        &ClientId::from_string("desktop-a"),
        Some(&agent("agent-1")),
    )
    .await
    .expect("same-host claim");
    let row = svc
        .store()
        .get_browser_tab("local-tab")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.host_client_id, ClientId::from_string("desktop-a"));
    assert_eq!(row.owner_agent_id, Some(agent("agent-1")));
    assert_eq!(row.displayed, Some(true), "same-host claim keeps the fact");
    let evs = events.await.unwrap();
    assert_eq!(evs[0]["type"], "browser:tab-updated");
    assert_eq!(evs[0]["data"]["tab"]["tabId"], "local-tab");
    assert_eq!(evs[0]["data"]["tab"]["displayed"], true);
    assert_eq!(
        evs[0]["data"]["changes"],
        json!({ "ownerAgentId": "agent-1" })
    );
}

/// Model 5 (end to end through `browser_exec`): the FE's real success
/// envelope for `claimTab` carries `result.tabId`, which is what triggers
/// the migration.
#[intent_test_macros::daemon_test]
async fn claim_reply_with_tab_id_migrates_through_browser_exec() {
    use std::sync::{Arc, Mutex};

    use intent_core::{AgentReverseDispatch, BoxFuture, ResolvedClient, ReverseDispatchError};

    struct ClaimOk(Mutex<Vec<ReverseTarget>>);
    impl AgentReverseDispatch for ClaimOk {
        fn is_connected(&self) -> bool {
            true
        }
        fn resolve(&self, target: &ReverseTarget) -> Result<ResolvedClient, ReverseDispatchError> {
            Ok(ResolvedClient {
                client_id: match target {
                    ReverseTarget::Default => ClientId::from_string("desktop-a"),
                    ReverseTarget::Client(c) | ReverseTarget::Pinned(c) => c.clone(),
                },
                name: None,
            })
        }
        fn dispatch<'a>(
            &'a self,
            _method: &'a str,
            params: Value,
            target: ReverseTarget,
        ) -> BoxFuture<'a, Result<Value, ReverseDispatchError>> {
            self.0.lock().unwrap().push(target);
            let tab_id = params["actions"][0]["tabId"].clone();
            Box::pin(async move {
                Ok(json!({
                    "success": true,
                    "results": [{
                        "action": "claimTab", "success": true,
                        "result": { "tabId": tab_id, "ownerAgentId": "agent-1", "width": 1024, "height": 800 }
                    }],
                }))
            })
        }
    }

    let tmp = intent_services::feature_api::test_support::fixtures::TempDb::new();
    let root = intent_services::feature_api::test_support::fixtures::WorkspacesRoot::new();
    let store = intent_store::Store::open(&tmp.path).await.unwrap();
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&intent_services::feature_api::test_support::fixtures::workspace(&ws))
        .await
        .unwrap();
    // The driving client has hello'd (its `client` row exists).
    store
        .upsert_client(
            &ClientId::from_string("desktop-a"),
            Some("Desktop A"),
            Some(&json!({ "browserExec": true })),
            &intent_core::ClientHostInfo::default(),
        )
        .await
        .unwrap();
    let bus = EventBus::new(store.clone());
    let dispatch = Arc::new(ClaimOk(Mutex::new(Vec::new())));
    let svc = Services::new(store)
        .with_workspaces_root(root.path().to_path_buf())
        .with_event_bus(bus.clone())
        .with_reverse_dispatch(dispatch.clone());
    register_tab(&svc, &ws, "user-tab", "desktop-b", None).await;

    let out = svc
        .browser_exec(
            ws.clone(),
            vec![json!({ "action": "claimTab", "tabId": "user-tab", "width": 1024 })],
            None,
            Some(agent("agent-1")),
        )
        .await
        .expect("claim");
    assert_eq!(out["success"], true);
    assert_eq!(
        dispatch.0.lock().unwrap().as_slice(),
        [ReverseTarget::Default]
    );
    let row = svc
        .store()
        .get_browser_tab("user-tab")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(row.host_client_id, ClientId::from_string("desktop-a"));
    assert_eq!(row.owner_agent_id, Some(agent("agent-1")));
    // desktop-a now hosts a claimed tab, so it is the driving client by
    // its own right — a later call resolves `Client(desktop-a)`.
    svc.browser_exec(
        ws.clone(),
        vec![json!({ "action": "claimTab", "tabId": "user-tab", "width": 800 })],
        None,
        Some(agent("agent-1")),
    )
    .await
    .unwrap();
    assert_eq!(
        dispatch.0.lock().unwrap()[1],
        ReverseTarget::Client(ClientId::from_string("desktop-a"))
    );
}

/// Model 3 & 10 (one host per workspace): a claim whose dispatch to the
/// pinned client is still in flight when `workspace.setBrowserClient`
/// switches the pin commits against the driving client *at commit
/// time* — the claimed rows land on the new pin, never on the client
/// that executed the claim — and the tab events stay gated.
#[intent_test_macros::daemon_test]
async fn claim_completing_after_a_pin_switch_commits_to_the_new_driving_client() {
    use std::sync::{Arc, Mutex};

    use intent_core::{AgentReverseDispatch, BoxFuture, ResolvedClient, ReverseDispatchError};
    use tokio::sync::Notify;

    struct HeldClaim {
        started: Arc<Notify>,
        release: Arc<Notify>,
        targets: Mutex<Vec<ReverseTarget>>,
    }
    impl AgentReverseDispatch for HeldClaim {
        fn is_connected(&self) -> bool {
            true
        }
        fn resolve(&self, target: &ReverseTarget) -> Result<ResolvedClient, ReverseDispatchError> {
            Ok(ResolvedClient {
                client_id: match target {
                    ReverseTarget::Default => ClientId::from_string("desktop-a"),
                    ReverseTarget::Client(c) | ReverseTarget::Pinned(c) => c.clone(),
                },
                name: None,
            })
        }
        fn dispatch<'a>(
            &'a self,
            _method: &'a str,
            params: Value,
            target: ReverseTarget,
        ) -> BoxFuture<'a, Result<Value, ReverseDispatchError>> {
            self.targets.lock().unwrap().push(target);
            let results: Vec<Value> = params["actions"]
                .as_array()
                .unwrap()
                .iter()
                .map(|a| {
                    json!({
                        "action": "claimTab", "success": true,
                        "result": { "tabId": a["tabId"], "ownerAgentId": "agent-1", "width": 1024, "height": 800 }
                    })
                })
                .collect();
            Box::pin(async move {
                self.started.notify_one();
                self.release.notified().await;
                Ok(json!({ "success": true, "results": results }))
            })
        }
    }

    let tmp = intent_services::feature_api::test_support::fixtures::TempDb::new();
    let root = intent_services::feature_api::test_support::fixtures::WorkspacesRoot::new();
    let store = intent_store::Store::open(&tmp.path).await.unwrap();
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&intent_services::feature_api::test_support::fixtures::workspace(&ws))
        .await
        .unwrap();
    for (id, name) in [("desktop-a", "Desktop A"), ("desktop-b", "Desktop B")] {
        store
            .upsert_client(
                &ClientId::from_string(id),
                Some(name),
                Some(&json!({ "browserExec": true })),
                &intent_core::ClientHostInfo::default(),
            )
            .await
            .unwrap();
    }
    let bus = EventBus::new(store.clone());
    let dispatch = Arc::new(HeldClaim {
        started: Arc::new(Notify::new()),
        release: Arc::new(Notify::new()),
        targets: Mutex::new(Vec::new()),
    });
    let svc = Arc::new(
        Services::new(store)
            .with_workspaces_root(root.path().to_path_buf())
            .with_event_bus(bus.clone())
            .with_reverse_dispatch(dispatch.clone()),
    );
    // Unclaimed tabs on both clients; the pin is desktop-a.
    register_tab(&svc, &ws, "tab-on-a", "desktop-a", None).await;
    register_tab(&svc, &ws, "tab-on-b", "desktop-b", None).await;
    svc.set_workspace_browser_client(ws.clone(), Some(ClientId::from_string("desktop-a")))
        .await
        .expect("pin a");

    let events = intent_core::spawn_daemon({
        let bus = bus.clone();
        let ws = ws.clone();
        async move { tab_events(&bus, &ws, 2).await }
    });
    tokio::task::yield_now().await;

    // The claim dispatches to the pinned client and its reply is held.
    let claim = intent_core::spawn_daemon({
        let svc = svc.clone();
        let ws = ws.clone();
        async move {
            svc.browser_exec(
                ws,
                vec![
                    json!({ "action": "claimTab", "tabId": "tab-on-a", "width": 1024 }),
                    json!({ "action": "claimTab", "tabId": "tab-on-b", "width": 1024 }),
                ],
                None,
                Some(agent("agent-1")),
            )
            .await
        }
    });
    tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(2)),
        dispatch.started.notified(),
    )
    .await
    .expect("claim dispatched");
    assert_eq!(
        dispatch.targets.lock().unwrap().as_slice(),
        [ReverseTarget::Pinned(ClientId::from_string("desktop-a"))]
    );

    // The pin switches while the claim is in flight; nothing is claimed
    // yet, so the setter migrates no row.
    svc.set_workspace_browser_client(ws.clone(), Some(ClientId::from_string("desktop-b")))
        .await
        .expect("pin b");
    for (id, host) in [("tab-on-a", "desktop-a"), ("tab-on-b", "desktop-b")] {
        let row = svc.store().get_browser_tab(id).await.unwrap().unwrap();
        assert_eq!(row.host_client_id, ClientId::from_string(host));
        assert!(row.owner_agent_id.is_none());
    }

    // The held reply lands: both rows commit to the *current* driving
    // client (desktop-b), not the executor (desktop-a).
    dispatch.release.notify_one();
    let out = claim.await.unwrap().expect("claim");
    assert_eq!(out["results"].as_array().map(Vec::len), Some(2));
    assert_eq!(out["results"][0]["success"], true);
    assert_eq!(out["results"][1]["success"], true);
    for id in ["tab-on-a", "tab-on-b"] {
        let row = svc.store().get_browser_tab(id).await.unwrap().unwrap();
        assert_eq!(
            row.host_client_id,
            ClientId::from_string("desktop-b"),
            "{id} must live on the driving client"
        );
        assert_eq!(row.owner_agent_id, Some(agent("agent-1")));
    }
    let evs = events.await.unwrap();
    assert_eq!(evs[0]["type"], "browser:tab-updated");
    assert_eq!(evs[0]["data"]["tab"]["tabId"], "tab-on-a");
    // The re-homed row drops desktop-a's `displayed` fact; the same-host
    // row keeps desktop-b's.
    assert_eq!(
        evs[0]["data"]["changes"],
        json!({ "hostClientId": "desktop-b", "ownerAgentId": "agent-1", "displayed": null })
    );
    assert_eq!(evs[1]["type"], "browser:tab-updated");
    assert_eq!(evs[1]["data"]["tab"]["tabId"], "tab-on-b");
    assert_eq!(
        evs[1]["data"]["changes"],
        json!({ "ownerAgentId": "agent-1" })
    );
    // desktop-b is the one host of the workspace: a later agent call
    // resolves the pin, and clearing it keeps the claimed-tab host.
    assert_eq!(
        svc.driving_client_target(&ws).await.unwrap(),
        ReverseTarget::Pinned(ClientId::from_string("desktop-b"))
    );
    svc.set_workspace_browser_client(ws.clone(), None)
        .await
        .expect("clear pin");
    assert_eq!(
        svc.driving_client_target(&ws).await.unwrap(),
        ReverseTarget::Client(ClientId::from_string("desktop-b"))
    );
}

/// Model 10: setting the pin moves every claimed tab of the workspace to
/// the new driving client (one `browser:tab-updated` each, clearing the
/// old host's `displayed` fact: `changes.displayed: null`); unclaimed
/// tabs and other workspaces stay put (fact retained); clearing the pin
/// moves nothing.
#[intent_test_macros::daemon_test]
async fn set_browser_client_migrates_claimed_tabs() {
    let reg = two_clients();
    let (_t, _r, svc, bus, ws) = setup(reg).await;
    register_tab(&svc, &ws, "user-tab", "desktop-a", None).await;
    register_tab(&svc, &ws, "mine-1", "desktop-a", Some("agent-1")).await;
    register_tab(&svc, &ws, "mine-2", "desktop-a", Some("agent-2")).await;
    let other_ws = WorkspaceId::new();
    svc.store()
        .insert_workspace(
            &intent_services::feature_api::test_support::fixtures::workspace(&other_ws),
        )
        .await
        .unwrap();
    register_tab(&svc, &other_ws, "elsewhere", "desktop-a", Some("agent-1")).await;

    let events = tokio::spawn({
        let bus = bus.clone();
        let ws = ws.clone();
        async move { tab_events(&bus, &ws, 2).await }
    });
    tokio::task::yield_now().await;
    svc.set_workspace_browser_client(ws.clone(), Some(ClientId::from_string("desktop-b")))
        .await
        .expect("pin");
    let evs = events.await.unwrap();
    let mut moved: Vec<&str> = evs
        .iter()
        .map(|e| e["data"]["tab"]["tabId"].as_str().unwrap())
        .collect();
    moved.sort_unstable();
    assert_eq!(moved, ["mine-1", "mine-2"]);
    for ev in &evs {
        assert_eq!(ev["type"], "browser:tab-updated");
        assert_eq!(
            ev["data"]["changes"],
            json!({ "hostClientId": "desktop-b", "displayed": null })
        );
        assert!(ev["data"]["tab"].get("displayed").is_none());
    }
    let row_of = |id: &str| {
        let svc = svc.clone();
        let id = id.to_string();
        async move { svc.store().get_browser_tab(&id).await.unwrap().unwrap() }
    };
    let host_of = |id: &str| {
        let row = row_of(id);
        async move { row.await.host_client_id.0 }
    };
    assert_eq!(host_of("user-tab").await, "desktop-a");
    assert_eq!(host_of("mine-1").await, "desktop-b");
    assert_eq!(host_of("mine-2").await, "desktop-b");
    assert_eq!(host_of("elsewhere").await, "desktop-a");
    assert_eq!(row_of("user-tab").await.displayed, Some(true), "not moved");
    assert_eq!(row_of("mine-1").await.displayed, None, "moved: unknown");
    assert_eq!(
        row_of("elsewhere").await.displayed,
        Some(false),
        "not moved"
    );

    svc.set_workspace_browser_client(ws.clone(), None)
        .await
        .expect("clear");
    assert_eq!(
        host_of("mine-1").await,
        "desktop-b",
        "clearing moves nothing"
    );
    // …and the driving client falls back to the claimed tabs' host.
    assert_eq!(
        svc.get_workspace_browser_client(ws).await.unwrap()["resolved"]["clientId"],
        "desktop-b"
    );
}

/// Model 4: `browser.navigateTab` goes to the driving client for a
/// claimed tab and to the physical host for an unclaimed one; an unknown
/// tab is `-32602`, an offline target `-32603`.
#[intent_test_macros::daemon_test]
async fn navigate_tab_routes_by_claim_state() {
    let reg = two_clients();
    let (_t, _r, svc, _bus, ws) = setup(reg.clone()).await;
    svc.set_workspace_browser_client(ws.clone(), Some(ClientId::from_string("desktop-a")))
        .await
        .unwrap();
    register_tab(&svc, &ws, "user-tab", "desktop-b", None).await;
    register_tab(&svc, &ws, "agent-tab", "desktop-a", Some("agent-1")).await;
    register_tab(&svc, &ws, "orphan", "desktop-gone", None).await;

    let out = svc
        .browser_navigate_tab("agent-tab".into(), "https://x.test/".into())
        .await
        .expect("claimed");
    assert_eq!(out["action"], "navigate");
    assert_eq!(out["result"]["host"], "desktop-a");
    let out = svc
        .browser_navigate_tab("user-tab".into(), "https://x.test/".into())
        .await
        .expect("unclaimed");
    assert_eq!(out["result"]["host"], "desktop-b");
    {
        let calls = reg.calls.lock().unwrap();
        assert_eq!(
            calls[0].2,
            ReverseTarget::Pinned(ClientId::from_string("desktop-a"))
        );
        assert_eq!(
            calls[1].2,
            ReverseTarget::Client(ClientId::from_string("desktop-b"))
        );
        assert_eq!(calls[1].1["tabId"], "user-tab");
        assert_eq!(calls[1].1["workspaceId"], ws.0);
        assert_eq!(
            calls[1].1["actions"],
            json!([{ "action": "navigate", "tabId": "user-tab", "url": "https://x.test/" }])
        );
    }
    let err = svc
        .browser_navigate_tab("ghost".into(), "https://x.test/".into())
        .await
        .expect_err("unknown");
    assert!(matches!(err, Error::InvalidParams(m) if m.contains("tab not found: ghost")));
    let err = svc
        .browser_navigate_tab("orphan".into(), "https://x.test/".into())
        .await
        .expect_err("offline host");
    assert!(
        matches!(&err, Error::Internal(m) if m == "browser.navigateTab: browser client desktop-gone for this workspace is not connected"),
        "{err:?}"
    );
}

/// Model 6: `browser.closeTab` routes the close to an online target; an
/// offline target is an error without `force` and a daemon-side
/// tombstone with it — the tab leaves `listTabs`, `browser:tab-closed`
/// is published, and the host's stale sync row is dropped.
#[tokio::test]
async fn close_tab_routes_online_and_force_tombstones_offline() {
    let reg = FakeRegistry::new(&[("desktop-a", Some("Desktop A"), true)]);
    let (_t, _r, svc, bus, ws) = setup(reg.clone()).await;
    register_tab(&svc, &ws, "live", "desktop-a", None).await;
    register_tab(&svc, &ws, "stale", "desktop-b", None).await;

    let out = svc
        .browser_close_tab("live".into(), false)
        .await
        .expect("routed close");
    assert_eq!(out, json!({ "ok": true }));
    assert_eq!(
        reg.calls.lock().unwrap()[0].1["actions"],
        json!([{ "action": "closeTab", "tabId": "live" }])
    );
    // The routed close leaves the row to the host's own report.
    assert!(svc.store().get_browser_tab("live").await.unwrap().is_some());

    let err = svc
        .browser_close_tab("stale".into(), false)
        .await
        .expect_err("offline host");
    assert!(
        matches!(&err, Error::Internal(m) if m.contains("\"Desktop B\" (desktop-b) for this workspace is not connected")),
        "{err:?}"
    );
    // The physical host was asked (and found offline) — never the
    // first-connected client.
    assert_eq!(
        reg.calls.lock().unwrap()[1].2,
        ReverseTarget::Client(ClientId::from_string("desktop-b"))
    );

    let events = tokio::spawn({
        let bus = bus.clone();
        let ws = ws.clone();
        async move { tab_events(&bus, &ws, 1).await }
    });
    tokio::task::yield_now().await;
    let out = svc
        .browser_close_tab("stale".into(), true)
        .await
        .expect("forced");
    assert_eq!(out, json!({ "ok": true }));
    let evs = events.await.unwrap();
    assert_eq!(evs[0]["type"], "browser:tab-closed");
    assert_eq!(evs[0]["data"]["tab"]["tabId"], "stale");
    assert!(svc
        .store()
        .get_browser_tab("stale")
        .await
        .unwrap()
        .is_none());
    let listed = svc
        .browser_exec(
            ws.clone(),
            vec![json!({ "action": "listTabs" })],
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(listed["result"].as_array().unwrap().len(), 1);
    assert_eq!(listed["result"][0]["tabId"], "live");
    // desktop-b comes back and still reports the tab: told to drop it.
    let drop = svc
        .browser_sync_tabs(
            ClientId::from_string("desktop-b"),
            vec![serde_json::from_value(json!({
                "tabId": "stale", "workspaceId": ws.0, "url": "https://stale.test/"
            }))
            .unwrap()],
        )
        .await
        .unwrap();
    assert_eq!(drop, ["stale"]);
    let err = svc
        .browser_close_tab("ghost".into(), true)
        .await
        .expect_err("unknown");
    assert!(matches!(err, Error::InvalidParams(m) if m.contains("tab not found: ghost")));
}
