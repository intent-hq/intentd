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
