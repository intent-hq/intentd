// ---------------------------------------------------------------------------
// REV-2: per-workspace browser-client pin — `client.list`,
// `workspace.getBrowserClient` / `setBrowserClient`, and the target
// `browser_exec` dispatches with (unpinned / pinned live / pinned offline /
// no eligible client).
// ---------------------------------------------------------------------------
use std::sync::{Arc, Mutex};
use std::time::Duration;

use intent_core::{
    AgentReverseDispatch, BoxFuture, ClientHostInfo, ClientId, Error, ResolvedClient,
    ReverseDispatchError, ReverseLiveClient, ReverseTarget, WorkspaceApi, WorkspaceId,
    CHIEF_WORKSPACE_ID,
};
use intent_store::Store;
use serde_json::{json, Value};

use super::{workspace, TempDb, WorkspacesRoot};
use crate::{EventBus, Services, SubscriptionFilter};

/// A dispatcher with a fixed set of live clients: `browser_exec == true`
/// entries are eligible. `Default` resolves to the first eligible
/// client; `Client` / `Pinned` to the named client when eligible. Every
/// dispatch is answered with a success envelope echoing the first
/// action's name and the resolved host, so tests can see who was asked.
pub(super) struct FakeRegistry {
    clients: Vec<(ClientId, Option<String>, bool)>,
    pub(super) calls: Mutex<Vec<(String, Value, ReverseTarget)>>,
}

impl FakeRegistry {
    pub(super) fn new(clients: &[(&str, Option<&str>, bool)]) -> Arc<Self> {
        Arc::new(Self {
            clients: clients
                .iter()
                .map(|(id, name, eligible)| {
                    (
                        ClientId::from_string(*id),
                        name.map(str::to_string),
                        *eligible,
                    )
                })
                .collect(),
            calls: Mutex::new(Vec::new()),
        })
    }
}

impl AgentReverseDispatch for FakeRegistry {
    fn is_connected(&self) -> bool {
        self.clients.iter().any(|c| c.2)
    }
    fn resolve(&self, target: &ReverseTarget) -> Result<ResolvedClient, ReverseDispatchError> {
        let found = match target {
            ReverseTarget::Default => self.clients.iter().find(|c| c.2),
            ReverseTarget::Client(id) | ReverseTarget::Pinned(id) => {
                self.clients.iter().find(|c| &c.0 == id && c.2)
            }
        };
        match (found, target) {
            (Some(c), _) => Ok(ResolvedClient {
                client_id: c.0.clone(),
                name: c.1.clone(),
            }),
            (None, ReverseTarget::Default) => Err(ReverseDispatchError::NoClient),
            (None, ReverseTarget::Client(id) | ReverseTarget::Pinned(id)) => {
                Err(ReverseDispatchError::ClientOffline {
                    client_id: id.clone(),
                    name: self
                        .clients
                        .iter()
                        .find(|c| &c.0 == id)
                        .and_then(|c| c.1.clone()),
                    pinned: matches!(target, ReverseTarget::Pinned(_)),
                })
            }
        }
    }
    fn live_clients(&self) -> Vec<ReverseLiveClient> {
        self.clients
            .iter()
            .map(|(id, name, eligible)| ReverseLiveClient {
                principal_id: None,
                host_role: None,
                login: None,
                display_name: None,
                avatar_url: None,
                identity: None,
                client_id: id.clone(),
                name: name.clone(),
                capabilities: json!({ "browserExec": eligible }),
                host: ClientHostInfo::default(),
                connections: 1,
                transports: vec!["wss".to_string()],
                connected_at: "2026-09-06T00:00:00Z".to_string(),
            })
            .collect()
    }
    fn dispatch<'a>(
        &'a self,
        method: &'a str,
        params: Value,
        target: ReverseTarget,
    ) -> BoxFuture<'a, Result<Value, ReverseDispatchError>> {
        let action = params["actions"][0]["action"].clone();
        self.calls
            .lock()
            .unwrap()
            .push((method.to_string(), params, target.clone()));
        let resolved = self.resolve(&target).map(|r| {
            json!({
                "success": true,
                "results": [{ "action": action, "success": true, "result": { "host": r.client_id } }],
            })
        });
        Box::pin(async move { resolved })
    }
}

pub(super) async fn setup(
    registry: Arc<FakeRegistry>,
) -> (TempDb, WorkspacesRoot, Services, EventBus, WorkspaceId) {
    let tmp = TempDb::new();
    let root = WorkspacesRoot::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");
    // Both clients have hello'd at some point (persisted `client` rows).
    for (id, name) in [("desktop-a", "Desktop A"), ("desktop-b", "Desktop B")] {
        store
            .upsert_client(
                &ClientId::from_string(id),
                Some(name),
                Some(&json!({ "browserExec": true })),
                &ClientHostInfo::default(),
            )
            .await
            .expect("client");
    }
    let bus = EventBus::new(store.clone());
    let svc = Services::new(store)
        .with_workspaces_root(root.path().to_path_buf())
        .with_event_bus(bus.clone())
        .with_reverse_dispatch(registry);
    (tmp, root, svc, bus, ws)
}

fn actions() -> Vec<Value> {
    vec![json!({ "action": "screenshot" })]
}

#[tokio::test]
async fn client_list_projects_live_clients() {
    let reg =
        FakeRegistry::new(&[("desktop-a", Some("Desktop A"), true), ("aux", None, false)]);
    let (_t, _r, svc, _bus, _ws) = setup(reg).await;
    let clients = svc.client_list().await.expect("list");
    let wire = serde_json::to_value(&clients).unwrap();
    assert_eq!(
        wire,
        json!([
            {
                "clientId": "desktop-a", "name": "Desktop A",
                "login": null, "displayName": null, "avatarUrl": null,
                "capabilities": { "browserExec": true }, "connections": 1,
                "transports": ["wss"], "connectedAt": "2026-09-06T00:00:00Z"
            },
            {
                "clientId": "aux",
                "login": null, "displayName": null, "avatarUrl": null,
                "capabilities": { "browserExec": false }, "connections": 1,
                "transports": ["wss"], "connectedAt": "2026-09-06T00:00:00Z"
            }
        ]),
        "name is omitted (not null) when the hello carried none"
    );
}

#[tokio::test]
async fn client_list_is_empty_without_a_dispatcher() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let svc = Services::new(store);
    assert!(svc.client_list().await.expect("list").is_empty());
}

#[intent_test_macros::daemon_test]
async fn unpinned_workspace_reports_default_and_dispatches_default() {
    let reg = FakeRegistry::new(&[
        ("desktop-a", Some("Desktop A"), true),
        ("desktop-b", Some("Desktop B"), true),
    ]);
    let (_t, _r, svc, _bus, ws) = setup(reg.clone()).await;
    let state = svc.get_workspace_browser_client(ws.clone()).await.unwrap();
    assert_eq!(
        state,
        json!({
            "source": "default",
            "resolved": { "clientId": "desktop-a", "name": "Desktop A" }
        }),
        "clientId is omitted when unpinned"
    );
    let out = svc
        .browser_exec(ws.clone(), actions(), None, None)
        .await
        .expect("dispatch");
    assert_eq!(out["result"]["host"], "desktop-a");
    assert_eq!(reg.calls.lock().unwrap()[0].2, ReverseTarget::Default);
}

#[intent_test_macros::daemon_test]
async fn no_eligible_client_resolves_null_and_dispatch_says_no_client() {
    let reg = FakeRegistry::new(&[("aux", None, false)]);
    let (_t, _r, svc, _bus, ws) = setup(reg).await;
    let state = svc.get_workspace_browser_client(ws.clone()).await.unwrap();
    assert_eq!(state, json!({ "source": "default", "resolved": null }));
    let err = svc
        .browser_exec(ws, actions(), None, None)
        .await
        .expect_err("nothing eligible");
    assert!(matches!(err, Error::Internal(m) if m == "browser.exec: no client connected"));
}

#[intent_test_macros::daemon_test]
async fn pinned_live_client_is_reported_and_dispatched_to() {
    let reg = FakeRegistry::new(&[
        ("desktop-a", Some("Desktop A"), true),
        ("desktop-b", Some("Desktop B"), true),
    ]);
    let (_t, _r, svc, bus, ws) = setup(reg.clone()).await;
    let mut sub = bus.subscribe(SubscriptionFilter {
        workspace_id: Some(ws.0.clone()),
        ..Default::default()
    });

    let echoed = svc
        .set_workspace_browser_client(ws.clone(), Some(ClientId::from_string("desktop-b")))
        .await
        .expect("pin");
    let expected = json!({
        "clientId": "desktop-b",
        "source": "workspace",
        "resolved": { "clientId": "desktop-b", "name": "Desktop B" }
    });
    assert_eq!(echoed, expected, "set echoes the get shape");
    assert_eq!(
        svc.get_workspace_browser_client(ws.clone()).await.unwrap(),
        expected
    );
    assert_eq!(
        svc.get_workspace(ws.clone())
            .await
            .unwrap()
            .browser_client_id,
        Some(ClientId::from_string("desktop-b")),
        "the pin rides the Workspace payload"
    );

    // `workspace:updated` carries the self-sufficient delta.
    let batch = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(2)),
        sub.recv(),
    )
    .await
    .expect("event delivered")
    .expect("subscription open");
    let ev = serde_json::to_value(&batch[0]).unwrap();
    assert_eq!(ev["type"], "workspace:updated");
    assert_eq!(
        ev["data"]["changes"],
        json!({ "browserClientId": "desktop-b" })
    );

    // Dispatch goes to the pin, not the first-connected client.
    let out = svc
        .browser_exec(ws.clone(), actions(), None, None)
        .await
        .expect("dispatch");
    assert_eq!(out["result"]["host"], "desktop-b");
    assert_eq!(
        reg.calls.lock().unwrap()[0].2,
        ReverseTarget::Pinned(ClientId::from_string("desktop-b"))
    );

    // Clearing with `null` returns to the default and echoes `null` in
    // the change delta.
    let cleared = svc
        .set_workspace_browser_client(ws.clone(), None)
        .await
        .expect("clear");
    assert_eq!(
        cleared,
        json!({
            "source": "default",
            "resolved": { "clientId": "desktop-a", "name": "Desktop A" }
        })
    );
    let batch = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(2)),
        sub.recv(),
    )
    .await
    .expect("event delivered")
    .expect("subscription open");
    let ev = serde_json::to_value(&batch[0]).unwrap();
    assert_eq!(ev["data"]["changes"], json!({ "browserClientId": null }));
    assert_eq!(svc.get_workspace(ws).await.unwrap().browser_client_id, None);
}

/// Regression (#1760 review): racing setters must not publish
/// `workspace:updated` deltas out of order relative to the durable pin.
/// The last delta a subscriber sees always equals what the store holds,
/// and each reply echoes the pin *that call* committed — never a value a
/// racing setter committed between this call's publish and its read-back.
#[intent_test_macros::daemon_test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_setters_publish_deltas_in_commit_order() {
    const ROUNDS: usize = 24;
    let reg = FakeRegistry::new(&[
        ("desktop-a", Some("Desktop A"), true),
        ("desktop-b", Some("Desktop B"), true),
    ]);
    let (_t, _r, svc, bus, ws) = setup(reg).await;
    let svc = Arc::new(svc);
    let mut sub = bus.subscribe(SubscriptionFilter {
        workspace_id: Some(ws.0.clone()),
        ..Default::default()
    });

    let mut tasks = tokio::task::JoinSet::new();
    for i in 0..ROUNDS {
        let svc = svc.clone();
        let ws = ws.clone();
        let target = match i % 3 {
            0 => Some(ClientId::from_string("desktop-a")),
            1 => Some(ClientId::from_string("desktop-b")),
            _ => None,
        };
        tasks.spawn(intent_core::with_caller(
            intent_core::Caller::Daemon,
            async move {
                let requested = target.as_ref().map_or(Value::Null, |c| c.as_str().into());
                let reply = svc
                    .set_workspace_browser_client(ws, target)
                    .await
                    .expect("set");
                (requested, reply)
            },
        ));
    }
    while let Some(joined) = tasks.join_next().await {
        let (requested, reply) = joined.expect("join");
        let echoed = reply.get("clientId").cloned().unwrap_or(Value::Null);
        assert_eq!(
            echoed, requested,
            "a reply echoes its own committed pin, not a racing setter's: {reply}"
        );
    }

    let mut deltas: Vec<Value> = Vec::new();
    while deltas.len() < ROUNDS {
        let batch = tokio::time::timeout(
            intentd_test_support::test_timeout(Duration::from_secs(5)),
            sub.recv(),
        )
        .await
        .expect("all deltas delivered")
        .expect("subscription open");
        for ev in &batch {
            let ev = serde_json::to_value(ev).unwrap();
            assert_eq!(ev["type"], "workspace:updated");
            deltas.push(ev["data"]["changes"]["browserClientId"].clone());
        }
    }
    assert_eq!(deltas.len(), ROUNDS);

    let durable = svc
        .get_workspace(ws)
        .await
        .unwrap()
        .browser_client_id
        .map_or(Value::Null, |c| c.as_str().into());
    assert_eq!(
        deltas.last().unwrap(),
        &durable,
        "the last published delta is the committed pin: {deltas:?}"
    );
}

#[intent_test_macros::daemon_test]
async fn pinned_offline_client_is_a_hard_error_not_a_fallback() {
    // desktop-b hello'd before (client row exists) but is not live now.
    let reg = FakeRegistry::new(&[("desktop-a", Some("Desktop A"), true)]);
    let (_t, _r, svc, _bus, ws) = setup(reg.clone()).await;
    let echoed = svc
        .set_workspace_browser_client(ws.clone(), Some(ClientId::from_string("desktop-b")))
        .await
        .expect("an offline-but-known client can be pinned");
    assert_eq!(
        echoed,
        json!({ "clientId": "desktop-b", "source": "workspace", "resolved": null })
    );
    let err = svc
        .browser_exec(ws, actions(), None, None)
        .await
        .expect_err("pinned offline");
    assert!(
        matches!(
            &err,
            Error::Internal(m)
                if m == "browser.exec: browser client \"Desktop B\" (desktop-b) for this workspace is not connected"
        ),
        "the offline name comes from the persisted client row: {err:?}"
    );
    // The first-connected client was never consulted.
    assert_eq!(
        reg.calls.lock().unwrap()[0].2,
        ReverseTarget::Pinned(ClientId::from_string("desktop-b"))
    );
}

#[intent_test_macros::daemon_test]
async fn set_rejects_never_seen_client_chief_and_unknown_workspace() {
    let reg = FakeRegistry::new(&[("desktop-a", Some("Desktop A"), true)]);
    let (_t, _r, svc, _bus, ws) = setup(reg).await;
    let err = svc
        .set_workspace_browser_client(ws.clone(), Some(ClientId::from_string("ghost")))
        .await
        .expect_err("never hello'd");
    assert!(matches!(err, Error::InvalidParams(m) if m.contains("ghost")));
    assert_eq!(
        svc.get_workspace(ws.clone())
            .await
            .unwrap()
            .browser_client_id,
        None,
        "a rejected set leaves the pin untouched"
    );

    // A row minted only to key an anonymous connection's drafts is not a
    // hello'd client: it has a `client` row but is still rejected.
    let anon = ClientId::from_string("anon-draft");
    svc.ensure_client(anon.clone()).await.expect("placeholder");
    assert!(svc.store.get_client(&anon).await.unwrap().is_some());
    let err = svc
        .set_workspace_browser_client(ws.clone(), Some(anon))
        .await
        .expect_err("draft-only client");
    assert!(matches!(err, Error::InvalidParams(m) if m.contains("anon-draft")));
    assert_eq!(
        svc.get_workspace(ws.clone())
            .await
            .unwrap()
            .browser_client_id,
        None
    );

    let chief = WorkspaceId::from(CHIEF_WORKSPACE_ID);
    let err = svc
        .set_workspace_browser_client(chief.clone(), Some(ClientId::from_string("desktop-a")))
        .await
        .expect_err("chief");
    assert!(matches!(err, Error::InvalidParams(_)));
    assert_eq!(
        svc.get_workspace_browser_client(chief).await.unwrap(),
        json!({
            "source": "default",
            "resolved": { "clientId": "desktop-a", "name": "Desktop A" }
        }),
        "chief always reads as the default"
    );

    let missing = WorkspaceId::from("ws-none");
    assert!(matches!(
        svc.get_workspace_browser_client(missing.clone()).await,
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        svc.set_workspace_browser_client(missing, None).await,
        Err(Error::NotFound(_))
    ));
}

/// Upgrade path: pre-0117 `client` rows have no `last_hello_at`. The
/// migration backfills it from `last_seen` only for rows with a `name`
/// (the hello upsert always carried one; the anonymous-draft placeholder
/// never did), so a previously connected, currently offline client stays
/// pinnable after upgrade while a pre-upgrade draft-only placeholder is
/// rejected until it hellos. Pre-upgrade rows are shaped by nulling the
/// stamp directly, and the backfill statement is re-run from 0117.
#[intent_test_macros::daemon_test]
async fn pre_upgrade_named_row_is_pinnable_and_nameless_row_is_not() {
    let reg = FakeRegistry::new(&[]);
    let (_t, _r, svc, _bus, ws) = setup(reg).await;
    let named = ClientId::from_string("desktop-a");
    let nameless = ClientId::from_string("legacy-anon");
    svc.ensure_client(nameless.clone())
        .await
        .expect("placeholder");
    sqlx::query("UPDATE client SET last_hello_at = NULL")
        .execute(svc.store.write_pool())
        .await
        .expect("shape pre-upgrade rows");
    for id in [&named, &nameless] {
        assert_eq!(
            svc.store
                .get_client(id)
                .await
                .unwrap()
                .unwrap()
                .last_hello_at,
            None
        );
    }
    let backfill = include_str!("../../intent-store/migrations/0117_client_host_identity.sql")
        .lines()
        .find(|l| l.starts_with("UPDATE client SET last_hello_at"))
        .expect("0117 backfill statement");
    sqlx::raw_sql(backfill)
        .execute(svc.store.write_pool())
        .await
        .expect("re-run backfill");

    let state = svc
        .set_workspace_browser_client(ws.clone(), Some(named.clone()))
        .await
        .expect("pre-upgrade named row is pinnable");
    assert_eq!(state["clientId"], "desktop-a");
    assert_eq!(state["source"], "workspace");
    assert_eq!(
        state["resolved"],
        Value::Null,
        "pinned but not connected resolves to null"
    );

    let err = svc
        .set_workspace_browser_client(ws.clone(), Some(nameless))
        .await
        .expect_err("pre-upgrade nameless row fails closed");
    assert!(matches!(err, Error::InvalidParams(m) if m.contains("legacy-anon")));
    assert_eq!(
        svc.get_workspace(ws).await.unwrap().browser_client_id,
        Some(named),
        "the rejected set leaves the existing pin untouched"
    );
}
