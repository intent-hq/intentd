// ---------------------------------------------------------------------------
// `ws.browser.clients()` (`WorkspaceApi::browser_clients`): the connection
// snapshot plus the unpinned and workspace routes, with the rule that chose
// the workspace route (`workspace` / `tabs` / `default`).
// ---------------------------------------------------------------------------
use std::sync::{Arc, Mutex};

use intent_core::{
    AgentReverseDispatch, BoxFuture, ClientId, ResolvedClient, ReverseClientConnections,
    ReverseConnectionInfo, ReverseConnectionSnapshot, ReverseDispatchError, ReverseRoute,
    ReverseTarget, WorkspaceApi, WorkspaceId,
};
use intent_store::Store;
use serde_json::{json, Value};

use super::browser_client_pin_tests::FakeRegistry;
use super::browser_routing_tests::register_tab;
use intent_services::feature_api::test_support::fixtures::{workspace, TempDb, WorkspacesRoot};
use intent_services::feature_api::Services;

/// [`FakeRegistry`] plus a connection snapshot: each client has one
/// connection whose id is its position + 1, and a route names that
/// connection when the fake resolves the target. Records the scope and
/// targets it was asked about.
struct SnapshotRegistry {
    inner: Arc<FakeRegistry>,
    clients: Vec<(&'static str, bool)>,
    asked: Mutex<Vec<(WorkspaceId, Vec<ReverseTarget>)>>,
}

impl SnapshotRegistry {
    fn new(clients: &[(&'static str, Option<&str>, bool)]) -> Arc<Self> {
        Arc::new(Self {
            inner: FakeRegistry::new(clients),
            clients: clients.iter().map(|(id, _, ok)| (*id, *ok)).collect(),
            asked: Mutex::new(Vec::new()),
        })
    }

    fn connection_of(&self, client_id: &ClientId) -> u64 {
        let pos = self
            .clients
            .iter()
            .position(|(id, _)| *id == client_id.as_str())
            .expect("known client");
        u64::try_from(pos).unwrap() + 1
    }
}

impl AgentReverseDispatch for SnapshotRegistry {
    fn is_connected(&self) -> bool {
        self.inner.is_connected()
    }
    fn resolve(&self, target: &ReverseTarget) -> Result<ResolvedClient, ReverseDispatchError> {
        self.inner.resolve(target)
    }
    fn connection_snapshot(
        &self,
        workspace_id: &WorkspaceId,
        targets: &[ReverseTarget],
    ) -> ReverseConnectionSnapshot {
        self.asked
            .lock()
            .unwrap()
            .push((workspace_id.clone(), targets.to_vec()));
        ReverseConnectionSnapshot {
            clients: self
                .clients
                .iter()
                .map(|(id, eligible)| ReverseClientConnections {
                    client_id: ClientId::from_string(*id),
                    name: None,
                    can_host_browser: *eligible,
                    connections: vec![ReverseConnectionInfo {
                        connection_id: self.connection_of(&ClientId::from_string(*id)),
                        transport: "wss".to_string(),
                        connected_at: "2026-10-27T09:00:00Z".to_string(),
                        can_host_browser: *eligible,
                    }],
                })
                .collect(),
            routes: targets
                .iter()
                .map(|target| match self.inner.resolve(target) {
                    Ok(resolved) => ReverseRoute {
                        connection_id: Some(self.connection_of(&resolved.client_id)),
                        client_id: Some(resolved.client_id),
                        name: resolved.name,
                        error: None,
                    },
                    Err(error) => ReverseRoute::failed(&error),
                })
                .collect(),
        }
    }
    fn dispatch<'a>(
        &'a self,
        _method: &'a str,
        _params: Value,
        _target: ReverseTarget,
    ) -> BoxFuture<'a, Result<Value, ReverseDispatchError>> {
        panic!("browser.clients must never dispatch to a client")
    }
}

async fn setup(registry: Arc<SnapshotRegistry>) -> (TempDb, WorkspacesRoot, Services, WorkspaceId) {
    let tmp = TempDb::new();
    let root = WorkspacesRoot::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");
    let svc = Services::new(store)
        .with_workspaces_root(root.path().to_path_buf())
        .with_reverse_dispatch(registry);
    (tmp, root, svc, ws)
}

fn desk_client(id: &str, connection_id: u64, eligible: bool) -> Value {
    json!({
        "clientId": id,
        "canHostBrowser": eligible,
        "connections": [{
            "connectionId": connection_id,
            "transport": "wss",
            "connectedAt": "2026-10-27T09:00:00Z",
            "canHostBrowser": eligible,
        }],
    })
}

/// The one-desktop acceptance case: one client with one browser-capable
/// connection, and both routes name it with `source: "default"`.
#[intent_test_macros::daemon_test]
async fn browser_clients_unpinned_names_the_default_connection() {
    let reg = SnapshotRegistry::new(&[("desktop-a", Some("Desktop A"), true)]);
    let (_t, _r, svc, ws) = setup(reg.clone()).await;
    let out = svc
        .browser_clients(ws.clone(), None)
        .await
        .expect("clients");
    assert_eq!(
        out,
        json!({
            "clients": [desk_client("desktop-a", 1, true)],
            "unpinned": { "clientId": "desktop-a", "name": "Desktop A", "connectionId": 1 },
            "workspace": {
                "clientId": "desktop-a", "name": "Desktop A", "connectionId": 1,
                "source": "default",
            },
        })
    );
    assert_eq!(
        *reg.asked.lock().unwrap(),
        vec![(ws, vec![ReverseTarget::Default, ReverseTarget::Default])],
        "one snapshot, scoped to the calling workspace"
    );
}

/// A pin routes the workspace to the pinned client while the unpinned
/// route stays on the first-connected one; a pin that is offline reports
/// no connection and the error `browser.exec` would give (with the stored
/// client name, since no live connection supplies one).
#[intent_test_macros::daemon_test]
async fn browser_clients_reports_the_pin_and_an_offline_pin() {
    let reg = SnapshotRegistry::new(&[
        ("desktop-a", Some("Desktop A"), true),
        ("desktop-b", Some("Desktop B"), true),
    ]);
    let (_t, _r, svc, ws) = setup(reg).await;
    for (id, name) in [("desktop-b", "Desktop B"), ("gone", "Gone")] {
        svc.store()
            .upsert_client(
                &ClientId::from_string(id),
                Some(name),
                Some(&json!({ "browserExec": true })),
                &intent_core::ClientHostInfo::default(),
            )
            .await
            .expect("client");
    }

    svc.set_workspace_browser_client(ws.clone(), Some(ClientId::from_string("desktop-b")))
        .await
        .expect("pin");
    let out = svc
        .browser_clients(ws.clone(), None)
        .await
        .expect("clients");
    assert_eq!(out["unpinned"]["connectionId"], 1);
    assert_eq!(
        out["workspace"],
        json!({
            "clientId": "desktop-b", "name": "Desktop B", "connectionId": 2,
            "source": "workspace",
        })
    );

    svc.set_workspace_browser_client(ws.clone(), Some(ClientId::from_string("gone")))
        .await
        .expect("pin");
    let out = svc.browser_clients(ws, None).await.expect("clients");
    assert_eq!(
        out["workspace"],
        json!({
            "clientId": "gone", "connectionId": null,
            "error": "browser client \"Gone\" (gone) for this workspace is not connected",
            "source": "workspace",
        })
    );
}

/// Without a pin, the host of the workspace's claimed tabs drives it.
#[intent_test_macros::daemon_test]
async fn browser_clients_reports_the_claimed_tab_host() {
    let reg = SnapshotRegistry::new(&[
        ("desktop-a", Some("Desktop A"), true),
        ("desktop-b", Some("Desktop B"), true),
    ]);
    let (_t, _r, svc, ws) = setup(reg).await;
    register_tab(&svc, &ws, "agent-tab", "desktop-b", Some("agent-1")).await;
    let out = svc.browser_clients(ws, None).await.expect("clients");
    assert_eq!(out["unpinned"]["clientId"], "desktop-a");
    assert_eq!(out["workspace"]["clientId"], "desktop-b");
    assert_eq!(out["workspace"]["connectionId"], 2);
    assert_eq!(out["workspace"]["source"], "tabs");
}

/// Nothing can receive the request: a client that cannot host a browser is
/// still listed, and both routes report the dispatch error.
#[intent_test_macros::daemon_test]
async fn browser_clients_without_an_eligible_client_reports_no_client() {
    let reg = SnapshotRegistry::new(&[("cli", None, false)]);
    let (_t, _r, svc, ws) = setup(reg).await;
    let out = svc.browser_clients(ws, None).await.expect("clients");
    let no_client =
        json!({ "clientId": null, "connectionId": null, "error": "no client connected" });
    let mut workspace_route = no_client.clone();
    workspace_route["source"] = "default".into();
    assert_eq!(
        out,
        json!({
            "clients": [desk_client("cli", 1, false)],
            "unpinned": no_client,
            "workspace": workspace_route,
        })
    );
}

/// Without a dispatcher (tests, read-only wiring) the call still answers.
#[intent_test_macros::daemon_test]
async fn browser_clients_without_a_dispatcher_lists_nothing() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");
    let svc = Services::new(store);
    let out = svc.browser_clients(ws, None).await.expect("clients");
    assert_eq!(out["clients"], json!([]));
    assert_eq!(out["unpinned"]["error"], "no client connected");
    assert_eq!(out["workspace"]["error"], "no client connected");
    assert_eq!(out["workspace"]["source"], "default");
}

/// A dispatcher that answers one route for two targets breaks the contract;
/// that is an internal error, never a silent "no client connected".
#[intent_test_macros::daemon_test]
async fn browser_clients_rejects_a_short_route_list() {
    struct ShortRegistry;
    impl AgentReverseDispatch for ShortRegistry {
        fn is_connected(&self) -> bool {
            false
        }
        fn connection_snapshot(
            &self,
            _workspace_id: &WorkspaceId,
            _targets: &[ReverseTarget],
        ) -> ReverseConnectionSnapshot {
            ReverseConnectionSnapshot {
                clients: Vec::new(),
                routes: vec![ReverseRoute::failed(&ReverseDispatchError::NoClient)],
            }
        }
        fn dispatch<'a>(
            &'a self,
            _method: &'a str,
            _params: Value,
            _target: ReverseTarget,
        ) -> BoxFuture<'a, Result<Value, ReverseDispatchError>> {
            panic!("browser.clients must never dispatch to a client")
        }
    }
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");
    let svc = Services::new(store).with_reverse_dispatch(Arc::new(ShortRegistry));
    let err = svc
        .browser_clients(ws, None)
        .await
        .expect_err("short route list");
    assert!(
        matches!(&err, intent_core::Error::Internal(m) if m.contains("one route per target")),
        "{err:?}"
    );
}
