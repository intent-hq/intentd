//! Unit tests for [`PrimaryReverseRegistry`] (REV-2): capability-gated
//! eligibility, `ReverseTarget` resolution (`Default` = first-connected
//! eligible, `Client`/`Pinned` = newest eligible connection of that client),
//! typed offline errors, logical-client transitions (`bind` / drop, queued in
//! mutation order), and RAII deregistration. Channel identity is checked
//! functionally — a
//! `dispatch` call sends the outbound frame to the resolved channel's queue
//! and no other, so the receivers themselves witness the routing decision.

use std::time::Duration;

use intent_core::{
    AgentReverseDispatch, ClientHostInfo, ClientId, ReverseDispatchError, ReverseTarget,
};
use serde_json::json;
use tokio::sync::mpsc;

use super::{ClientTransition, PrimaryReverseRegistry, ReverseClientIdentity, ReverseTransport};
use crate::reverse::ReverseChannel;

/// Build a `ReverseChannel` whose outbound queue is deep enough that
/// `request()` succeeds up to the timeout.
fn idle_channel() -> (ReverseChannel, mpsc::Receiver<String>) {
    let (tx, rx) = mpsc::channel::<String>(4);
    (ReverseChannel::new(tx), rx)
}

/// A `client.hello` identity for logical client `id`, advertising (or not)
/// the `browserExec` capability, with a `hostname` derived from `id`.
fn identity(id: &str, browser_exec: bool) -> ReverseClientIdentity {
    ReverseClientIdentity {
        client_id: ClientId::from_string(id),
        name: Some(format!("client {id}")),
        capabilities: json!({ "browserExec": browser_exec }),
        host: ClientHostInfo {
            hostname: Some(format!("{id}.local")),
            pretty_hostname: None,
            device_kind: None,
        },
    }
}

fn client(id: &str) -> ReverseTarget {
    ReverseTarget::Client(ClientId::from_string(id))
}

fn pinned(id: &str) -> ReverseTarget {
    ReverseTarget::Pinned(ClientId::from_string(id))
}

/// Drive one `dispatch` for `target` and answer with `result` via
/// `route_response` on the receiver whose channel is expected to be
/// resolved. Returns the observed reply.
async fn dispatch_and_reply(
    reg: &PrimaryReverseRegistry,
    target: ReverseTarget,
    channel: &ReverseChannel,
    rx: &mut mpsc::Receiver<String>,
    result: serde_json::Value,
) -> Result<serde_json::Value, ReverseDispatchError> {
    let dispatch = tokio::spawn({
        let reg = reg.clone();
        async move {
            reg.dispatch("browser.exec", json!({ "actions": [] }), target)
                .await
        }
    });
    let frame = tokio::time::timeout(Duration::from_millis(500), rx.recv())
        .await
        .expect("frame arrives")
        .expect("outbound frame");
    let value: serde_json::Value = serde_json::from_str(&frame).expect("json");
    let id = value
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap()
        .to_string();
    channel.route_response(&json!({
        "jsonrpc": "2.0",
        "id": id,
        "result": result,
    }));
    dispatch.await.expect("join")
}

#[test]
fn empty_registry_reports_no_primary() {
    let reg = PrimaryReverseRegistry::new();
    assert!(reg.primary().is_none());
    assert!(!reg.is_connected());
    assert!(reg.live_clients().is_empty());
    assert_eq!(
        reg.resolve(&ReverseTarget::Default),
        Err(ReverseDispatchError::NoClient)
    );
}

/// Regression for the iOS / auxiliary-connection misrouting (REV-1): a
/// connection that never sends `client.hello` (iOS) and one that hellos
/// without `browserExec` (an FE auxiliary `JsonRpcClient`) both arrive before
/// the desktop main connection. Under REV-1 the first arrival was primary and
/// the agent's `browser.exec` hung on iOS; under REV-2 both are ineligible and
/// the dispatch lands on the desktop regardless of arrival order.
#[tokio::test]
async fn ineligible_and_unhelloed_connections_never_receive_default_dispatch() {
    let reg = PrimaryReverseRegistry::new();
    let (ios, mut rx_ios) = idle_channel();
    let (aux, mut rx_aux) = idle_channel();
    let (desktop, mut rx_desktop) = idle_channel();
    let _g_ios = reg.register(ios, ReverseTransport::Wss);
    let g_aux = reg.register(aux, ReverseTransport::Uds);
    g_aux.bind(identity("fe-aux", false));
    assert!(
        !reg.is_connected(),
        "no eligible client yet: un-hello'd and capability-less connections do not count"
    );
    let g_desktop = reg.register(desktop.clone(), ReverseTransport::Wss);
    g_desktop.bind(identity("fe-desktop", true));
    assert_eq!(reg.len(), 3);
    assert!(reg.is_connected());

    let out = dispatch_and_reply(
        &reg,
        ReverseTarget::Default,
        &desktop,
        &mut rx_desktop,
        json!({ "primary": "desktop" }),
    )
    .await
    .expect("ok");
    assert_eq!(out, json!({ "primary": "desktop" }));
    assert!(
        rx_ios.try_recv().is_err(),
        "iOS must never see browser.exec"
    );
    assert!(
        rx_aux.try_recv().is_err(),
        "auxiliary connection must never see browser.exec"
    );
}

#[tokio::test]
async fn default_dispatch_routes_to_the_first_connected_eligible_channel() {
    let reg = PrimaryReverseRegistry::new();
    let (a, mut rx_a) = idle_channel();
    let (b, mut rx_b) = idle_channel();
    let g_a = reg.register(a.clone(), ReverseTransport::Wss);
    let g_b = reg.register(b, ReverseTransport::Wss);
    // Hello order is the reverse of arrival order: arrival still wins.
    g_b.bind(identity("b", true));
    g_a.bind(identity("a", true));

    let out = dispatch_and_reply(
        &reg,
        ReverseTarget::Default,
        &a,
        &mut rx_a,
        json!({ "primary": "a" }),
    )
    .await
    .expect("ok");
    assert_eq!(out, json!({ "primary": "a" }));
    assert!(rx_b.try_recv().is_err());
    assert_eq!(
        reg.resolve(&ReverseTarget::Default)
            .expect("resolves")
            .client_id
            .as_str(),
        "a"
    );
}

#[tokio::test]
async fn dropping_the_primary_promotes_the_next_eligible_registration() {
    let reg = PrimaryReverseRegistry::new();
    let (a, mut _rx_a) = idle_channel();
    let (aux, mut rx_aux) = idle_channel();
    let (b, mut rx_b) = idle_channel();
    let g_a = reg.register(a, ReverseTransport::Wss);
    g_a.bind(identity("a", true));
    let g_aux = reg.register(aux, ReverseTransport::Uds);
    g_aux.bind(identity("aux", false));
    let g_b = reg.register(b.clone(), ReverseTransport::Wss);
    g_b.bind(identity("b", true));
    drop(g_a);
    assert_eq!(reg.len(), 2);

    let out = dispatch_and_reply(
        &reg,
        ReverseTarget::Default,
        &b,
        &mut rx_b,
        json!({ "primary": "b" }),
    )
    .await
    .expect("ok");
    assert_eq!(out, json!({ "primary": "b" }));
    assert!(rx_aux.try_recv().is_err());
}

#[tokio::test]
async fn dropping_a_non_primary_leaves_the_head_unchanged() {
    let reg = PrimaryReverseRegistry::new();
    let (a, mut rx_a) = idle_channel();
    let (b, mut _rx_b) = idle_channel();
    let g_a = reg.register(a.clone(), ReverseTransport::Wss);
    g_a.bind(identity("a", true));
    let g_b = reg.register(b, ReverseTransport::Wss);
    g_b.bind(identity("b", true));
    drop(g_b);
    assert_eq!(reg.len(), 1);

    let out = dispatch_and_reply(
        &reg,
        ReverseTarget::Default,
        &a,
        &mut rx_a,
        json!({ "primary": "a" }),
    )
    .await
    .expect("ok");
    assert_eq!(out, json!({ "primary": "a" }));
}

#[tokio::test]
async fn client_target_routes_to_the_newest_eligible_connection_of_that_client() {
    let reg = PrimaryReverseRegistry::new();
    let (other, mut rx_other) = idle_channel();
    let (old, mut rx_old) = idle_channel();
    let (new, mut rx_new) = idle_channel();
    let (aux, mut rx_aux) = idle_channel();
    let g_other = reg.register(other, ReverseTransport::Wss);
    g_other.bind(identity("other", true));
    let g_old = reg.register(old, ReverseTransport::Wss);
    g_old.bind(identity("x", true));
    let g_new = reg.register(new.clone(), ReverseTransport::Uds);
    g_new.bind(identity("x", true));
    // A newer connection of the same logical client WITHOUT the capability
    // (an FE auxiliary socket sharing the clientId) is skipped.
    let g_aux = reg.register(aux, ReverseTransport::Uds);
    g_aux.bind(identity("x", false));

    for target in [client("x"), pinned("x")] {
        let out = dispatch_and_reply(&reg, target, &new, &mut rx_new, json!({ "host": "new" }))
            .await
            .expect("ok");
        assert_eq!(out, json!({ "host": "new" }));
    }
    assert!(rx_other.try_recv().is_err());
    assert!(rx_old.try_recv().is_err());
    assert!(rx_aux.try_recv().is_err());
    // `Default` is unaffected by the explicit target: still first-connected.
    assert_eq!(
        reg.resolve(&ReverseTarget::Default)
            .expect("resolves")
            .client_id
            .as_str(),
        "other"
    );
    let resolved = reg.resolve(&client("x")).expect("resolves");
    assert_eq!(resolved.client_id.as_str(), "x");
    assert_eq!(resolved.name.as_deref(), Some("client x"));
}

#[tokio::test]
async fn client_and_pinned_targets_report_typed_offline_errors() {
    let reg = PrimaryReverseRegistry::new();
    let (a, _rx_a) = idle_channel();
    let g_a = reg.register(a, ReverseTransport::Wss);
    g_a.bind(identity("a", true));

    // Never-seen client: no name is known.
    let err = reg
        .dispatch("browser.exec", json!({}), client("ghost"))
        .await
        .expect_err("offline");
    assert_eq!(
        err,
        ReverseDispatchError::ClientOffline {
            client_id: ClientId::from_string("ghost"),
            name: None,
            pinned: false,
        }
    );
    assert_eq!(err.to_string(), "browser client ghost is not connected");
    let err = reg
        .dispatch("browser.exec", json!({}), pinned("ghost"))
        .await
        .expect_err("offline");
    assert_eq!(
        err,
        ReverseDispatchError::ClientOffline {
            client_id: ClientId::from_string("ghost"),
            name: None,
            pinned: true,
        }
    );
    assert_eq!(
        err.to_string(),
        "pinned browser client ghost is not connected"
    );

    // A client that is connected but only through ineligible connections is
    // offline for browser purposes — and its name is known.
    let (aux, _rx_aux) = idle_channel();
    let g_aux = reg.register(aux, ReverseTransport::Uds);
    g_aux.bind(identity("b", false));
    let err = reg.resolve(&pinned("b")).expect_err("offline");
    assert_eq!(
        err,
        ReverseDispatchError::ClientOffline {
            client_id: ClientId::from_string("b"),
            name: Some("client b".to_string()),
            pinned: true,
        }
    );
    assert_eq!(
        err.to_string(),
        "pinned browser client \"client b\" (b) is not connected"
    );

    // A previously eligible client whose connection dropped.
    drop(g_a);
    assert_eq!(
        reg.resolve(&client("a")),
        Err(ReverseDispatchError::ClientOffline {
            client_id: ClientId::from_string("a"),
            name: None,
            pinned: false,
        })
    );
}

#[tokio::test]
async fn default_without_an_eligible_client_reports_no_client() {
    let reg = PrimaryReverseRegistry::new();
    let (aux, _rx_aux) = idle_channel();
    let g_aux = reg.register(aux, ReverseTransport::Uds);
    g_aux.bind(identity("aux", false));
    let err = reg
        .dispatch("browser.exec", json!({}), ReverseTarget::Default)
        .await
        .expect_err("no client");
    assert_eq!(err, ReverseDispatchError::NoClient);
    assert!(!reg.is_connected());
}

/// Drain every transition queued so far (no await: the queue is fed
/// synchronously under the registry lock).
fn drain(rx: &mut mpsc::UnboundedReceiver<ClientTransition>) -> Vec<ClientTransition> {
    let mut out = Vec::new();
    while let Ok(t) = rx.try_recv() {
        out.push(t);
    }
    out
}

#[test]
fn bind_and_drop_queue_logical_client_transitions_in_mutation_order() {
    let reg = PrimaryReverseRegistry::new();
    let mut rx = reg.take_transitions().expect("first claim gets the queue");
    assert!(
        reg.take_transitions().is_none(),
        "the queue has exactly one consumer"
    );
    let (c1, _rx1) = idle_channel();
    let (c2, _rx2) = idle_channel();
    let g1 = reg.register(c1, ReverseTransport::Wss);
    let g2 = reg.register(c2, ReverseTransport::Uds);
    assert!(drain(&mut rx).is_empty(), "registration alone is silent");

    // First hello of a client ⇒ it becomes live.
    g1.bind(identity("x", true));
    assert_eq!(
        drain(&mut rx),
        vec![ClientTransition::Connected(identity("x", true))]
    );
    // A second connection of the same client ⇒ no transition.
    g2.bind(identity("x", false));
    // Re-hello on the same connection ⇒ no transition (entry updated).
    g1.bind(identity("x", true));
    assert!(drain(&mut rx).is_empty());
    // Re-hello with a different clientId ⇒ the old client stays live through
    // g2, the new one comes up.
    g1.bind(identity("y", true));
    assert_eq!(
        drain(&mut rx),
        vec![ClientTransition::Connected(identity("y", true))]
    );
    // Dropping the only connection of `y` announces it gone; `x` is still
    // live through g2 until that drops too. Plain `drop` is the same path
    // the WSS heartbeat abort and panic-unwind take.
    drop(g1);
    assert_eq!(reg.len(), 1);
    assert_eq!(
        drain(&mut rx),
        vec![ClientTransition::Disconnected(identity("y", true))]
    );
    drop(g2);
    assert!(reg.is_empty());
    assert_eq!(
        drain(&mut rx),
        vec![ClientTransition::Disconnected(identity("x", false))]
    );

    // An un-hello'd connection drops silently.
    let (c3, _rx3) = idle_channel();
    let g3 = reg.register(c3, ReverseTransport::Uds);
    drop(g3);
    assert!(reg.is_empty());
    assert!(drain(&mut rx).is_empty());
}

/// A re-hello that moves a connection off the last live connection of one
/// client onto a brand-new one queues `Disconnected(old)` before
/// `Connected(new)`; a same-client reconnect after a drop queues
/// `Disconnected` before `Connected` — the order subscribers must observe.
#[test]
fn transitions_keep_disconnect_before_reconnect_order() {
    let reg = PrimaryReverseRegistry::new();
    let mut rx = reg.take_transitions().expect("queue");
    let (c1, _rx1) = idle_channel();
    let g1 = reg.register(c1, ReverseTransport::Wss);
    g1.bind(identity("x", true));
    g1.bind(identity("y", true));
    assert_eq!(
        drain(&mut rx),
        vec![
            ClientTransition::Connected(identity("x", true)),
            ClientTransition::Disconnected(identity("x", true)),
            ClientTransition::Connected(identity("y", true)),
        ]
    );

    // Same-client reconnect: the stale connection drops, the replacement
    // hellos — whichever task runs first, the queue reflects the registry
    // mutation order, so the disconnect can never trail the reconnect.
    drop(g1);
    let (c2, _rx2) = idle_channel();
    let g2 = reg.register(c2, ReverseTransport::Wss);
    g2.bind(identity("y", true));
    assert_eq!(
        drain(&mut rx),
        vec![
            ClientTransition::Disconnected(identity("y", true)),
            ClientTransition::Connected(identity("y", true)),
        ]
    );
    // The reverse interleaving (replacement hellos while the stale
    // connection is still registered) is a silent hand-over: the client
    // never went offline.
    let (c3, _rx3) = idle_channel();
    let g3 = reg.register(c3, ReverseTransport::Wss);
    g3.bind(identity("y", true));
    drop(g2);
    assert!(drain(&mut rx).is_empty());
    drop(g3);
    assert_eq!(
        drain(&mut rx),
        vec![ClientTransition::Disconnected(identity("y", true))]
    );
}

/// Transitions recorded before a publisher claims the queue are retained,
/// and the event types map one-to-one onto the `client:*` names.
#[test]
fn transitions_queued_before_the_claim_are_retained() {
    let reg = PrimaryReverseRegistry::new();
    let (c1, _rx1) = idle_channel();
    let g1 = reg.register(c1, ReverseTransport::Uds);
    g1.bind(identity("x", true));
    drop(g1);
    let mut rx = reg.take_transitions().expect("queue");
    let transitions = drain(&mut rx);
    assert_eq!(
        transitions
            .iter()
            .map(ClientTransition::event_type)
            .collect::<Vec<_>>(),
        vec!["client:connected", "client:disconnected"]
    );
    assert!(transitions
        .iter()
        .all(|t| t.identity() == &identity("x", true)));
}

#[test]
fn live_clients_groups_hellod_connections_by_client() {
    let reg = PrimaryReverseRegistry::new();
    let (silent, _rx0) = idle_channel();
    let (a1, _rx1) = idle_channel();
    let (b1, _rx2) = idle_channel();
    let (a2, _rx3) = idle_channel();
    let _g_silent = reg.register(silent, ReverseTransport::Uds);
    let guard_a_first = reg.register(a1, ReverseTransport::Wss);
    let guard_b = reg.register(b1, ReverseTransport::Uds);
    let guard_a_second = reg.register(a2, ReverseTransport::Uds);
    guard_b.bind(identity("b", false));
    guard_a_first.bind(identity("a", true));
    guard_a_second.bind(identity("a", true));

    let clients = reg.live_clients();
    assert_eq!(clients.len(), 2, "un-hello'd connections are not clients");
    let a = &clients[0];
    assert_eq!(a.client_id.as_str(), "a", "ordered by first connection");
    assert_eq!(a.name.as_deref(), Some("client a"));
    assert_eq!(a.capabilities, json!({ "browserExec": true }));
    assert_eq!(a.host.hostname.as_deref(), Some("a.local"));
    assert_eq!(a.connections, 2);
    assert_eq!(
        a.transports,
        vec![ReverseTransport::Wss, ReverseTransport::Uds]
    );
    assert!(!a.connected_at.is_empty());
    let b = &clients[1];
    assert_eq!(b.client_id.as_str(), "b");
    assert_eq!(b.connections, 1);
    assert_eq!(b.transports, vec![ReverseTransport::Uds]);
    assert_eq!(b.capabilities, json!({ "browserExec": false }));

    // Dropping a's oldest connection re-orders by the surviving connections
    // (b's connection now predates a's) and shrinks a's group.
    drop(guard_a_first);
    let clients = reg.live_clients();
    assert_eq!(clients[0].client_id.as_str(), "b");
    assert_eq!(clients[1].client_id.as_str(), "a");
    assert_eq!(clients[1].connections, 1);
    assert_eq!(clients[1].transports, vec![ReverseTransport::Uds]);
}

/// Regression (#1756 review): a client is `browserExec`-eligible when ANY of
/// its live connections advertises the capability. A later auxiliary socket
/// sharing the `clientId` without `browserExec` must not mask an earlier
/// eligible connection in the `client.list` projection — and the aggregate
/// must agree with what `resolve` / `dispatch` actually route to.
#[test]
fn live_clients_aggregate_browser_exec_across_connections() {
    let reg = PrimaryReverseRegistry::new();
    let (main, _rx_main) = idle_channel();
    let (aux, _rx_aux) = idle_channel();
    let g_main = reg.register(main, ReverseTransport::Wss);
    g_main.bind(identity("x", true));
    // Newest hello lacks the capability (e.g. an FE auxiliary JsonRpcClient).
    let g_aux = reg.register(aux, ReverseTransport::Uds);
    g_aux.bind(identity("x", false));

    let clients = reg.live_clients();
    assert_eq!(clients.len(), 1);
    let x = &clients[0];
    assert_eq!(x.connections, 2);
    assert!(
        x.browser_exec,
        "any eligible connection makes the client eligible"
    );
    assert_eq!(
        x.capabilities["browserExec"],
        json!(true),
        "the projected capability bag reports the aggregate, not the newest hello"
    );
    assert!(reg.resolve(&pinned("x")).is_ok());
    assert_eq!(
        reg.resolve(&ReverseTarget::Default)
            .unwrap()
            .client_id
            .as_str(),
        "x"
    );

    // Same aggregate when the eligible connection is the NEWER one.
    let reg = PrimaryReverseRegistry::new();
    let (aux, _rx_aux) = idle_channel();
    let (main, _rx_main) = idle_channel();
    let g_aux = reg.register(aux, ReverseTransport::Uds);
    g_aux.bind(identity("y", false));
    let g_main = reg.register(main, ReverseTransport::Wss);
    g_main.bind(identity("y", true));
    let clients = reg.live_clients();
    assert!(clients[0].browser_exec);
    assert_eq!(clients[0].capabilities["browserExec"], json!(true));

    // Dropping the only eligible connection flips the aggregate back.
    drop(g_main);
    let clients = reg.live_clients();
    assert_eq!(clients[0].connections, 1);
    assert!(!clients[0].browser_exec);
    assert_eq!(clients[0].capabilities["browserExec"], json!(false));
    assert!(matches!(
        reg.resolve(&pinned("y")),
        Err(ReverseDispatchError::ClientOffline { pinned: true, .. })
    ));
}

/// Regression (#1760 review): "newest hello wins" for the display fields
/// keys on hello order, not connection-registration order. An older
/// connection that re-hellos later carries the client's current identity.
#[test]
fn live_clients_project_the_most_recent_hello_regardless_of_registration_order() {
    let reg = PrimaryReverseRegistry::new();
    let (first, _rx_first) = idle_channel();
    let (second, _rx_second) = idle_channel();
    let g_first = reg.register(first, ReverseTransport::Wss);
    let g_second = reg.register(second, ReverseTransport::Uds);
    // The later-registered connection hellos first ...
    g_second.bind(ReverseClientIdentity {
        name: Some("stale".into()),
        capabilities: json!({ "browserExec": false, "rev": 1 }),
        ..identity("x", false)
    });
    // ... then the earlier-registered connection hellos with newer data.
    g_first.bind(ReverseClientIdentity {
        name: Some("fresh".into()),
        capabilities: json!({ "browserExec": true, "rev": 2 }),
        host: ClientHostInfo {
            hostname: Some("fresh.local".into()),
            pretty_hostname: Some("Fresh".into()),
            device_kind: Some("laptop".into()),
        },
        ..identity("x", true)
    });

    let clients = reg.live_clients();
    assert_eq!(clients.len(), 1);
    let x = &clients[0];
    assert_eq!(x.connections, 2);
    assert_eq!(x.name.as_deref(), Some("fresh"));
    assert_eq!(x.capabilities["rev"], json!(2));
    assert_eq!(x.host.hostname.as_deref(), Some("fresh.local"));
    assert_eq!(x.host.pretty_hostname.as_deref(), Some("Fresh"));
    assert_eq!(x.host.device_kind.as_deref(), Some("laptop"));

    // A re-hello on the older-registered connection supersedes again.
    g_second.bind(ReverseClientIdentity {
        name: Some("freshest".into()),
        capabilities: json!({ "browserExec": false, "rev": 3 }),
        ..identity("x", false)
    });
    let clients = reg.live_clients();
    let x = &clients[0];
    assert_eq!(x.name.as_deref(), Some("freshest"));
    assert_eq!(x.capabilities["rev"], json!(3));
    assert_eq!(x.host.hostname.as_deref(), Some("x.local"));
    assert_eq!(
        x.capabilities["browserExec"],
        json!(true),
        "the aggregate still reflects the eligible connection"
    );
}

/// The presence index behind `browser.listTabs` decoration tracks every
/// registry mutation: bind, multi-connection clients, re-hello under another
/// id, and drop — and only answers for the ids asked about.
#[test]
fn host_presence_follows_bind_rehello_and_drop() {
    let reg = PrimaryReverseRegistry::new();
    let (a1, _rx1) = idle_channel();
    let (a2, _rx2) = idle_channel();
    let (b1, _rx3) = idle_channel();
    let guard_a_first = reg.register(a1, ReverseTransport::Wss);
    let guard_a_second = reg.register(a2, ReverseTransport::Uds);
    let guard_b = reg.register(b1, ReverseTransport::Uds);
    let a_id = ClientId::from_string("a");
    let b_id = ClientId::from_string("b");
    let c_id = ClientId::from_string("c");
    let ask = |ids: &[&ClientId]| reg.host_presence(&ids.iter().copied().collect());

    assert!(
        ask(&[&a_id, &b_id]).is_empty(),
        "un-hello'd connections are not present"
    );

    guard_a_first.bind(identity("a", true));
    guard_a_second.bind(identity("a", false));
    guard_b.bind(identity("b", false));
    let presence = ask(&[&a_id, &b_id, &c_id]);
    assert_eq!(presence.len(), 2, "unknown ids are simply absent");
    assert_eq!(presence[&a_id].connections, 2);
    assert_eq!(presence[&a_id].name.as_deref(), Some("client a"));
    assert_eq!(presence[&b_id].connections, 1);
    assert_eq!(ask(&[&b_id]).len(), 1, "only the asked ids are looked up");

    // Re-hello under the same id refreshes the display name only.
    guard_a_second.bind(ReverseClientIdentity {
        name: Some("renamed a".to_string()),
        ..identity("a", false)
    });
    let presence = ask(&[&a_id]);
    assert_eq!(presence[&a_id].connections, 2);
    assert_eq!(presence[&a_id].name.as_deref(), Some("renamed a"));

    // Re-hello under another id moves the connection between clients; a's
    // name falls back to its remaining (older) hello instead of keeping the
    // departed connection's "renamed a".
    guard_a_second.bind(identity("c", false));
    let presence = ask(&[&a_id, &c_id]);
    assert_eq!(presence[&a_id].connections, 1);
    assert_eq!(presence[&a_id].name.as_deref(), Some("client a"));
    assert_eq!(presence[&c_id].connections, 1);
    assert_eq!(presence[&c_id].name.as_deref(), Some("client c"));

    // Dropping the newest connection of a multi-connection client reverts
    // the name to the newest surviving hello — the same rule `live_clients`
    // applies — rather than keeping the dropped connection's name.
    let (a3, _rx4) = idle_channel();
    let guard_a_third = reg.register(a3, ReverseTransport::Wss);
    guard_a_third.bind(ReverseClientIdentity {
        name: Some("third a".to_string()),
        ..identity("a", false)
    });
    assert_eq!(ask(&[&a_id])[&a_id].name.as_deref(), Some("third a"));
    drop(guard_a_third);
    let presence = ask(&[&a_id]);
    assert_eq!(presence[&a_id].connections, 1);
    assert_eq!(presence[&a_id].name.as_deref(), Some("client a"));
    assert_eq!(
        presence[&a_id].name,
        reg.live_clients()
            .iter()
            .find(|c| c.client_id == a_id)
            .and_then(|c| c.name.clone()),
        "presence name agrees with live_clients"
    );

    // Dropping the last connection removes the client.
    drop(guard_a_first);
    let presence = ask(&[&a_id, &b_id, &c_id]);
    assert!(!presence.contains_key(&a_id));
    assert_eq!(presence[&b_id].connections, 1);
    assert_eq!(presence[&c_id].connections, 1);
    drop(guard_b);
    drop(guard_a_second);
    assert!(ask(&[&a_id, &b_id, &c_id]).is_empty());
}

/// The presence name keys on hello order, not registration order, so it
/// agrees with `live_clients` when an earlier-registered connection hellos
/// (or re-hellos) after a later-registered one — including after the
/// newest-hello'd connection drops.
#[test]
fn host_presence_name_follows_highest_hello_seq_not_registration_order() {
    let reg = PrimaryReverseRegistry::new();
    let (first, _rx_first) = idle_channel();
    let (second, _rx_second) = idle_channel();
    let g_first = reg.register(first, ReverseTransport::Wss);
    let g_second = reg.register(second, ReverseTransport::Uds);
    let x_id = ClientId::from_string("x");
    let ask = || reg.host_presence(&[&x_id].into_iter().collect());
    let live_name = || {
        reg.live_clients()
            .iter()
            .find(|c| c.client_id == x_id)
            .and_then(|c| c.name.clone())
    };

    // The later-registered connection hellos first ...
    g_second.bind(ReverseClientIdentity {
        name: Some("stale".into()),
        ..identity("x", false)
    });
    // ... then the earlier-registered one hellos with the newer name.
    g_first.bind(ReverseClientIdentity {
        name: Some("fresh".into()),
        ..identity("x", true)
    });
    let presence = ask();
    assert_eq!(presence[&x_id].connections, 2);
    assert_eq!(presence[&x_id].name.as_deref(), Some("fresh"));
    assert_eq!(presence[&x_id].name, live_name());

    // A re-hello on the later-registered connection supersedes again.
    g_second.bind(ReverseClientIdentity {
        name: Some("freshest".into()),
        ..identity("x", false)
    });
    assert_eq!(ask()[&x_id].name.as_deref(), Some("freshest"));
    assert_eq!(ask()[&x_id].name, live_name());

    // Dropping the newest-hello'd connection falls back to the surviving
    // hello even though it sits earlier in registration order.
    drop(g_second);
    let presence = ask();
    assert_eq!(presence[&x_id].connections, 1);
    assert_eq!(presence[&x_id].name.as_deref(), Some("fresh"));
    assert_eq!(presence[&x_id].name, live_name());
    drop(g_first);
    assert!(ask().is_empty());
}

/// `bound_client_id` reports only what `client.hello` bound onto this
/// connection's entry: nothing before a hello, the latest id after a
/// re-hello, and nothing once the entry is gone — per guard, not per registry.
#[test]
fn bound_client_id_tracks_this_connections_hello() {
    let reg = PrimaryReverseRegistry::new();
    let (a, _rx_a) = idle_channel();
    let (b, _rx_b) = idle_channel();
    let guard_a = reg.register(a, ReverseTransport::Wss);
    let guard_b = reg.register(b, ReverseTransport::Uds);

    assert_eq!(guard_a.bound_client_id(), None);
    guard_b.bind(identity("b", false));
    assert_eq!(
        guard_a.bound_client_id(),
        None,
        "another connection's hello does not qualify this one"
    );
    assert_eq!(guard_b.bound_client_id(), Some(ClientId::from_string("b")));

    guard_a.bind(identity("a", true));
    assert_eq!(guard_a.bound_client_id(), Some(ClientId::from_string("a")));
    guard_a.bind(identity("c", true));
    assert_eq!(guard_a.bound_client_id(), Some(ClientId::from_string("c")));
    drop(guard_b);
    assert_eq!(guard_a.bound_client_id(), Some(ClientId::from_string("c")));
    assert_eq!(
        reg.inner.lock().bound.len(),
        1,
        "the index forgets a dropped connection"
    );
    drop(guard_a);
    assert!(reg.inner.lock().bound.is_empty());
}

#[tokio::test]
async fn dropping_guard_closes_an_accepted_request() {
    let reg = PrimaryReverseRegistry::new();
    let (channel, mut rx) = idle_channel();
    let guard = reg.register(channel, ReverseTransport::Wss);
    guard.bind(identity("a", true));
    let dispatch = tokio::spawn({
        let reg = reg.clone();
        async move {
            reg.dispatch(
                "browser.exec",
                json!({ "actions": [{ "action": "screenshot" }] }),
                ReverseTarget::Default,
            )
            .await
        }
    });
    let frame = rx.recv().await.expect("accepted frame");
    assert!(frame.contains("screenshot"));

    drop(guard);
    let err = dispatch
        .await
        .expect("join")
        .expect_err("guard drop closes request");
    assert!(matches!(err, ReverseDispatchError::Transport { .. }));
    assert!(reg.is_empty());
}

#[tokio::test]
async fn stale_primary_clone_cannot_request_after_guard_drop() {
    let reg = PrimaryReverseRegistry::new();
    let (channel, mut rx) = idle_channel();
    let guard = reg.register(channel, ReverseTransport::Wss);
    guard.bind(identity("a", true));
    let stale = reg.primary().expect("primary clone");

    drop(guard);
    let err = stale
        .request(
            "browser.exec",
            json!({ "actions": [{ "action": "screenshot" }] }),
            Duration::from_secs(5),
        )
        .await
        .expect_err("closed state rejects stale clone");
    assert!(err.message.contains("closed"));
    assert!(
        rx.try_recv().is_err(),
        "closed clone cannot enqueue a frame"
    );
    assert!(reg.is_empty());
}

#[tokio::test]
async fn dispatch_without_clients_reports_no_client() {
    let reg = PrimaryReverseRegistry::new();
    let err = reg
        .dispatch("browser.exec", json!({}), ReverseTarget::Default)
        .await
        .expect_err("no client");
    assert_eq!(err, ReverseDispatchError::NoClient);
}

#[tokio::test]
async fn dispatch_reports_transport_error_when_channel_is_closed() {
    let reg = PrimaryReverseRegistry::new();
    let (a, rx_a) = idle_channel();
    let g_a = reg.register(a, ReverseTransport::Wss);
    g_a.bind(identity("a", true));
    // Drop the receiver so the outbound queue is closed; `request` should
    // surface a transport error rather than `NoClient`.
    drop(rx_a);
    let err = reg
        .dispatch("browser.exec", json!({}), ReverseTarget::Default)
        .await
        .expect_err("transport error");
    assert!(
        matches!(err, ReverseDispatchError::Transport { .. }),
        "unexpected error: {err:?}"
    );
}
