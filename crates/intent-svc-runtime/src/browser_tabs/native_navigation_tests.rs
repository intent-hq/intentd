use std::sync::Arc;

use intent_core::{
    AgentReverseDispatch, BoxFuture, ClientId, ResolvedClient, ReverseDispatchError,
    ReverseLiveClient, ReverseTarget, WorkspaceApi, WorkspaceId,
};
use serde_json::{json, Value};

use super::browser_client_pin_tests::{setup, FakeRegistry};
use super::browser_routing_tests::register_tab;

struct NavigationRegistry {
    desktop: Arc<FakeRegistry>,
    navigation: Arc<FakeRegistry>,
}

impl NavigationRegistry {
    fn new(desktop_live: bool) -> Arc<Self> {
        Arc::new(Self {
            desktop: FakeRegistry::new(&[("desktop-a", None, desktop_live)]),
            navigation: FakeRegistry::new(&[
                ("desktop-a", None, desktop_live),
                ("phone", None, true),
                ("unsupported", None, false),
            ]),
        })
    }
}

impl AgentReverseDispatch for NavigationRegistry {
    fn is_connected(&self) -> bool {
        self.desktop.is_connected()
    }
    fn resolve(&self, target: &ReverseTarget) -> Result<ResolvedClient, ReverseDispatchError> {
        self.desktop.resolve(target)
    }
    fn resolve_navigation(
        &self,
        target: &ReverseTarget,
        _: &WorkspaceId,
    ) -> Result<ResolvedClient, ReverseDispatchError> {
        self.navigation.resolve(target)
    }
    fn live_clients(&self) -> Vec<ReverseLiveClient> {
        let mut clients = self.navigation.live_clients();
        for client in &mut clients {
            if client.client_id.as_str() == "phone" {
                client.capabilities = json!({"browserNavigation":true,"browserExec":false});
            }
        }
        clients
    }
    fn dispatch<'a>(
        &'a self,
        method: &'a str,
        params: Value,
        target: ReverseTarget,
    ) -> BoxFuture<'a, Result<Value, ReverseDispatchError>> {
        if intent_core::browser_navigation_actions(params["actions"].as_array().unwrap()) {
            self.navigation.dispatch(method, params, target)
        } else {
            self.desktop.dispatch(method, params, target)
        }
    }
}

#[intent_test_macros::daemon_test]
async fn native_navigation_new_tabs_fall_back_without_moving_existing_tabs() {
    for desktop_live in [false, true] {
        let registry = NavigationRegistry::new(desktop_live);
        let (_tmp, _root, svc, _bus, ws) = setup(registry.desktop.clone()).await;
        let svc = svc.with_reverse_dispatch(registry.clone());
        register_tab(&svc, &ws, "existing", "desktop-a", Some("agent-1")).await;
        for active_session in [false, true] {
            if active_session {
                svc.feature_desktop()
                    .activate_for_test(&ws, ClientId::from("desktop-a"));
            }
            let opened = svc
                .browser_exec(
                    ws.clone(),
                    vec![json!({"action":"openTab","url":"https://preview.test"})],
                    None,
                    None,
                )
                .await
                .unwrap();
            assert_eq!(
                opened["result"]["host"],
                if desktop_live { "desktop-a" } else { "phone" }
            );
            assert_eq!(
                svc.store()
                    .get_browser_tab("existing")
                    .await
                    .unwrap()
                    .unwrap()
                    .host_client_id
                    .as_str(),
                "desktop-a"
            );
        }
        svc.set_workspace_browser_client(ws.clone(), Some(ClientId::from("desktop-a")))
            .await
            .unwrap();
        let pinned = svc
            .browser_exec(
                ws.clone(),
                vec![json!({"action":"openTab","url":"https://preview.test"})],
                None,
                None,
            )
            .await;
        assert_eq!(
            pinned.is_ok(),
            desktop_live,
            "explicit offline pin never falls back"
        );
    }
}

#[intent_test_macros::daemon_test]
async fn native_navigation_tabs_keep_host_and_reject_automation_or_mixed_batches() {
    let registry = NavigationRegistry::new(true);
    let (_tmp, _root, svc, _bus, ws) = setup(registry.desktop.clone()).await;
    let svc = svc.with_reverse_dispatch(registry.clone());
    register_tab(&svc, &ws, "native", "phone", Some("agent-1")).await;
    assert!(
        svc.store()
            .get_browser_tab("native")
            .await
            .unwrap()
            .unwrap()
            .navigation_only,
        "hello capability stamps native reports"
    );
    register_tab(&svc, &ws, "desktop", "desktop-a", Some("agent-1")).await;
    svc.set_workspace_browser_client(ws.clone(), Some(ClientId::from("desktop-a")))
        .await
        .unwrap();
    assert_eq!(
        svc.store()
            .get_browser_tab("native")
            .await
            .unwrap()
            .unwrap()
            .host_client_id
            .as_str(),
        "phone"
    );
    let shown = svc
        .browser_exec(
            ws.clone(),
            vec![json!({"action":"showTab","tabId":"native"})],
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(shown["result"]["host"], "phone");
    let navigated = svc
        .browser_navigate_tab("native".into(), "https://updated.test".into())
        .await
        .unwrap();
    assert_eq!(navigated["result"]["host"], "phone");
    svc.browser_close_tab("native".into(), false).await.unwrap();
    let count = registry.navigation.calls.lock().unwrap().len();
    for action in ["screenshot", "evaluate", "claimTab", "resizeTab"] {
        let rejected = svc
            .browser_exec(
                ws.clone(),
                vec![json!({"action":action,"tabId":"native"})],
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(rejected["errorCode"], "browser-automation-unavailable");
    }
    let rejected = svc
        .browser_exec(
            ws.clone(),
            vec![
                json!({"action":"showTab","tabId":"native"}),
                json!({"action":"focusTab","tabId":"desktop"}),
            ],
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(rejected["success"], false);
    assert_eq!(rejected["results"][0]["errorCode"], "browser-mixed-hosts");
    assert_eq!(registry.navigation.calls.lock().unwrap().len(), count);
    assert!(registry.desktop.calls.lock().unwrap().is_empty());
    svc.set_workspace_browser_client(ws.clone(), Some(ClientId::from("phone")))
        .await
        .unwrap();
    assert_eq!(
        svc.store()
            .get_browser_tab("desktop")
            .await
            .unwrap()
            .unwrap()
            .host_client_id
            .as_str(),
        "desktop-a"
    );
    let navigated = svc
        .browser_navigate_tab("desktop".into(), "https://updated.test".into())
        .await
        .unwrap();
    assert_eq!(navigated["result"]["host"], "desktop-a");
}

#[intent_test_macros::daemon_test]
async fn native_navigation_only_connection_explains_automation_unavailability() {
    let registry = NavigationRegistry::new(false);
    let (_tmp, _root, svc, _bus, ws) = setup(registry.desktop.clone()).await;
    let svc = svc.with_reverse_dispatch(registry.clone());
    let result = svc
        .browser_exec(ws, vec![json!({"action":"screenshot"})], None, None)
        .await
        .unwrap();
    assert_eq!(result["errorCode"], "browser-automation-unavailable");
    assert!(registry.navigation.calls.lock().unwrap().is_empty());
}

#[intent_test_macros::daemon_test]
async fn native_navigation_prefers_latest_sender_only_for_new_unpinned_opens() {
    use intent_core::{AgentId, ClientHostInfo};
    use intent_services::feature_api::test_support::fixtures::session;
    let registry = NavigationRegistry::new(true);
    let (_tmp, _root, svc, _bus, ws) = setup(registry.desktop.clone()).await;
    let svc = svc.with_reverse_dispatch(registry.clone());
    let agent = AgentId::new();
    svc.store()
        .insert_agent_session(&session(&agent, &ws))
        .await
        .unwrap();
    for device in ["phone", "offline", "unsupported"] {
        svc.store()
            .upsert_client(
                &ClientId::from(device),
                None,
                None,
                &ClientHostInfo::default(),
            )
            .await
            .unwrap();
    }
    for (order, device, expected) in [
        ("1", "phone", "phone"),
        ("2", "desktop-a", "desktop-a"),
        ("3", "offline", "desktop-a"),
        ("4", "unsupported", "desktop-a"),
        ("5", "phone", "phone"),
    ] {
        svc.store()
            .record_browser_sender(&agent, order, order, Some(&ClientId::from(device)))
            .await
            .unwrap();
        let result = svc
            .browser_exec(
                ws.clone(),
                vec![json!({"action":"openTab","url":"https://preview.test"})],
                None,
                Some(agent.clone()),
            )
            .await
            .unwrap();
        assert_eq!(result["result"]["host"], expected);
    }
    // A navigation-only sender cannot receive any automation in a batch.
    svc.browser_exec(
        ws.clone(),
        vec![
            json!({"action":"openTab","url":"https://preview.test"}),
            json!({"action":"evaluate","script":"1"}),
        ],
        None,
        Some(agent.clone()),
    )
    .await
    .unwrap();
    assert_eq!(
        registry.desktop.calls.lock().unwrap().last().unwrap().2,
        ReverseTarget::Default
    );
    register_tab(
        &svc,
        &ws,
        "existing-desktop",
        "desktop-a",
        Some(agent.as_str()),
    )
    .await;
    let result = svc
        .browser_exec(
            ws.clone(),
            vec![json!({"action":"showTab","tabId":"existing-desktop"})],
            None,
            Some(agent.clone()),
        )
        .await
        .unwrap();
    assert_eq!(result["result"]["host"], "desktop-a");
    svc.set_workspace_browser_client(ws.clone(), Some(ClientId::from("desktop-a")))
        .await
        .unwrap();
    let result = svc
        .browser_exec(
            ws,
            vec![json!({"action":"openTab","url":"https://preview.test"})],
            None,
            Some(agent),
        )
        .await
        .unwrap();
    assert_eq!(result["result"]["host"], "desktop-a");
}

#[intent_test_macros::daemon_test]
async fn browser_sender_delegation_inherits_only_nearest_same_workspace_receipt() {
    use intent_core::{AgentId, ClientHostInfo};
    use intent_services::feature_api::test_support::fixtures::{session, workspace};
    let registry = NavigationRegistry::new(true);
    let (_tmp, _root, svc, _bus, ws) = setup(registry.desktop.clone()).await;
    let svc = svc.with_reverse_dispatch(registry);
    svc.store()
        .upsert_client(
            &ClientId::from("phone"),
            None,
            None,
            &ClientHostInfo::default(),
        )
        .await
        .unwrap();
    let parent = AgentId::new();
    let child = AgentId::new();
    let unrelated = AgentId::new();
    let foreign = AgentId::new();
    let foreign_ws = WorkspaceId::new();
    svc.store()
        .insert_workspace(&workspace(&foreign_ws))
        .await
        .unwrap();
    for (agent, home, ancestor) in [
        (&parent, &ws, None),
        (&child, &ws, Some(parent.clone())),
        (&unrelated, &ws, None),
        (&foreign, &foreign_ws, Some(parent.clone())),
    ] {
        let mut row = session(agent, home);
        row.parent_agent_id = ancestor;
        if agent == &unrelated {
            row.metadata = Some(json!({"sponsorAgentId": parent}));
        }
        svc.store().insert_agent_session(&row).await.unwrap();
    }
    svc.store()
        .record_browser_sender(&parent, "parent", "1", Some(&ClientId::from("phone")))
        .await
        .unwrap();
    assert_eq!(
        svc.store()
            .preferred_browser_sender(&child, &ws)
            .await
            .unwrap(),
        Some(ClientId::from("phone"))
    );
    let opened = svc
        .browser_exec(
            ws.clone(),
            vec![json!({"action":"openTab","url":"https://preview.test"})],
            None,
            Some(child.clone()),
        )
        .await
        .unwrap();
    assert_eq!(opened["result"]["host"], "phone");
    assert_eq!(
        svc.store()
            .preferred_browser_sender(&unrelated, &ws)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        svc.store()
            .preferred_browser_sender(&foreign, &foreign_ws)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        svc.store()
            .preferred_browser_sender(&child, &foreign_ws)
            .await
            .unwrap(),
        None
    );
    svc.store()
        .record_browser_sender(&child, "child", "2", Some(&ClientId::from("desktop-b")))
        .await
        .unwrap();
    assert_eq!(
        svc.store()
            .preferred_browser_sender(&child, &ws)
            .await
            .unwrap(),
        Some(ClientId::from("desktop-b"))
    );
    // Offline nearest receipt falls back ordinarily, never to the live ancestor.
    let opened = svc
        .browser_exec(
            ws.clone(),
            vec![json!({"action":"openTab","url":"https://preview.test"})],
            None,
            Some(child.clone()),
        )
        .await
        .unwrap();
    assert_eq!(opened["result"]["host"], "desktop-a");
    svc.store()
        .record_browser_sender(&child, "clear", "3", None)
        .await
        .unwrap();
    assert_eq!(
        svc.store()
            .preferred_browser_sender(&child, &ws)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        svc.store().latest_browser_sender(&parent).await.unwrap(),
        Some(ClientId::from("phone"))
    );
    // Corrupt parent cycles terminate even without any receipt.
    let cycle_a = AgentId::new();
    let cycle_b = AgentId::new();
    for (agent, ancestor) in [(&cycle_a, &cycle_b), (&cycle_b, &cycle_a)] {
        let mut row = session(agent, &ws);
        row.parent_agent_id = Some(ancestor.clone());
        svc.store().insert_agent_session(&row).await.unwrap();
    }
    assert_eq!(
        svc.store()
            .preferred_browser_sender(&cycle_a, &ws)
            .await
            .unwrap(),
        None
    );
    let mut ancestor = parent;
    for _ in 0..16 {
        let id = AgentId::new();
        let mut row = session(&id, &ws);
        row.parent_agent_id = Some(ancestor);
        svc.store().insert_agent_session(&row).await.unwrap();
        ancestor = id;
    }
    assert_eq!(
        svc.store()
            .preferred_browser_sender(&ancestor, &ws)
            .await
            .unwrap(),
        None,
        "lookup is bounded even on an excessively deep chain"
    );
}

#[intent_test_macros::daemon_test]
async fn browser_sender_precedes_focus_and_native_tabs_preserve_focus_fallback() {
    use intent_core::{AgentId, ClientHostInfo};
    use intent_services::feature_api::test_support::fixtures::session;
    let registry = Arc::new(NavigationRegistry {
        desktop: FakeRegistry::new(&[("desktop-a", None, true), ("desktop-b", None, true)]),
        navigation: FakeRegistry::new(&[
            ("desktop-a", None, true),
            ("desktop-b", None, true),
            ("phone", None, true),
        ]),
    });
    let (_tmp, _root, svc, _bus, ws) = setup(registry.desktop.clone()).await;
    let svc = svc.with_reverse_dispatch(registry.clone());
    let agent = AgentId::new();
    svc.store()
        .insert_agent_session(&session(&agent, &ws))
        .await
        .unwrap();
    for device in ["phone", "offline"] {
        svc.store()
            .upsert_client(
                &ClientId::from(device),
                None,
                None,
                &ClientHostInfo::default(),
            )
            .await
            .unwrap();
    }
    svc.presence_record_focus_for_test(&ws, &[ClientId::from("desktop-b")]);
    // A claimed native tab must not select the automation host or suppress focus.
    register_tab(&svc, &ws, "native-focused", "phone", Some(agent.as_str())).await;
    for (order, sender, expected) in [
        ("1", None, "desktop-b"),
        ("2", Some("phone"), "phone"),
        ("3", Some("desktop-a"), "desktop-a"),
        ("4", Some("offline"), "desktop-b"),
        ("5", Some("phone"), "phone"),
    ] {
        svc.store()
            .record_browser_sender(&agent, order, order, sender.map(ClientId::from).as_ref())
            .await
            .unwrap();
        let result = svc
            .browser_exec(
                ws.clone(),
                vec![json!({"action":"openTab","url":"https://preview.test"})],
                None,
                Some(agent.clone()),
            )
            .await
            .unwrap();
        assert_eq!(result["result"]["host"], expected);
    }
    // Native sender cannot take a mixed automation batch: focus remains the fallback.
    svc.browser_exec(
        ws.clone(),
        vec![
            json!({"action":"openTab"}),
            json!({"action":"evaluate","script":"1"}),
        ],
        None,
        Some(agent.clone()),
    )
    .await
    .unwrap();
    assert_eq!(
        registry.desktop.calls.lock().unwrap().last().unwrap().2,
        ReverseTarget::Preferred(vec![ClientId::from("desktop-b")])
    );
    svc.store()
        .record_browser_sender(&agent, "6", "6", Some(&ClientId::from("desktop-a")))
        .await
        .unwrap();
    let named = svc
        .browser_exec(
            ws.clone(),
            vec![json!({"action":"showTab","tabId":"native-focused"})],
            None,
            Some(agent.clone()),
        )
        .await
        .unwrap();
    assert_eq!(
        named["result"]["host"], "phone",
        "sender and focus never move named native tabs"
    );
    svc.set_workspace_browser_client(ws.clone(), Some(ClientId::from("desktop-b")))
        .await
        .unwrap();
    let pinned = svc
        .browser_exec(
            ws.clone(),
            vec![json!({"action":"openTab"})],
            None,
            Some(agent.clone()),
        )
        .await
        .unwrap();
    assert_eq!(pinned["result"]["host"], "desktop-b");
    svc.set_workspace_browser_client(ws.clone(), Some(ClientId::from("offline")))
        .await
        .unwrap();
    assert!(
        svc.browser_exec(ws, vec![json!({"action":"openTab"})], None, Some(agent))
            .await
            .is_err(),
        "offline pin cannot use sender or focus fallback"
    );
}
