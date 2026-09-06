//! Unit tests for [`PrimaryReverseRegistry`] (REV-2): capability-gated
//! eligibility, `ReverseTarget` resolution (`Default` = first-connected
//! eligible, `Client`/`Pinned` = newest eligible connection of that client),
//! typed offline errors, logical-client transitions (`bind` / `release`), and
//! RAII deregistration. Channel identity is checked functionally — a
//! `dispatch` call sends the outbound frame to the resolved channel's queue
//! and no other, so the receivers themselves witness the routing decision.

use std::time::Duration;

use intent_core::{AgentReverseDispatch, ClientId, ReverseDispatchError, ReverseTarget};
use serde_json::json;
use tokio::sync::mpsc;

use super::{PrimaryReverseRegistry, ReverseClientIdentity, ReverseTransport};
use crate::reverse::ReverseChannel;

/// Build a `ReverseChannel` whose outbound queue is deep enough that
/// `request()` succeeds up to the timeout.
fn idle_channel() -> (ReverseChannel, mpsc::Receiver<String>) {
    let (tx, rx) = mpsc::channel::<String>(4);
    (ReverseChannel::new(tx), rx)
}

/// A `client.hello` identity for logical client `id`, advertising (or not)
/// the `browserExec` capability.
fn identity(id: &str, browser_exec: bool) -> ReverseClientIdentity {
    ReverseClientIdentity {
        client_id: ClientId::from_string(id),
        name: Some(format!("client {id}")),
        capabilities: json!({ "browserExec": browser_exec }),
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
    let _ = g_aux.bind(identity("fe-aux", false));
    assert!(
        !reg.is_connected(),
        "no eligible client yet: un-hello'd and capability-less connections do not count"
    );
    let g_desktop = reg.register(desktop.clone(), ReverseTransport::Wss);
    let _ = g_desktop.bind(identity("fe-desktop", true));
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
    let _ = g_b.bind(identity("b", true));
    let _ = g_a.bind(identity("a", true));

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
    let _ = g_a.bind(identity("a", true));
    let g_aux = reg.register(aux, ReverseTransport::Uds);
    let _ = g_aux.bind(identity("aux", false));
    let g_b = reg.register(b.clone(), ReverseTransport::Wss);
    let _ = g_b.bind(identity("b", true));
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
    let _ = g_a.bind(identity("a", true));
    let g_b = reg.register(b, ReverseTransport::Wss);
    let _ = g_b.bind(identity("b", true));
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
    let _ = g_other.bind(identity("other", true));
    let g_old = reg.register(old, ReverseTransport::Wss);
    let _ = g_old.bind(identity("x", true));
    let g_new = reg.register(new.clone(), ReverseTransport::Uds);
    let _ = g_new.bind(identity("x", true));
    // A newer connection of the same logical client WITHOUT the capability
    // (an FE auxiliary socket sharing the clientId) is skipped.
    let g_aux = reg.register(aux, ReverseTransport::Uds);
    let _ = g_aux.bind(identity("x", false));

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
    let _ = g_a.bind(identity("a", true));

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
    let _ = g_aux.bind(identity("b", false));
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
    let _ = g_aux.bind(identity("aux", false));
    let err = reg
        .dispatch("browser.exec", json!({}), ReverseTarget::Default)
        .await
        .expect_err("no client");
    assert_eq!(err, ReverseDispatchError::NoClient);
    assert!(!reg.is_connected());
}

#[test]
fn bind_and_release_report_logical_client_transitions() {
    let reg = PrimaryReverseRegistry::new();
    let (c1, _rx1) = idle_channel();
    let (c2, _rx2) = idle_channel();
    let g1 = reg.register(c1, ReverseTransport::Wss);
    let g2 = reg.register(c2, ReverseTransport::Uds);

    // First hello of a client ⇒ it becomes live.
    let outcome = g1.bind(identity("x", true));
    assert_eq!(outcome.connected, Some(identity("x", true)));
    assert_eq!(outcome.disconnected, None);
    // A second connection of the same client ⇒ no transition.
    let outcome = g2.bind(identity("x", false));
    assert_eq!(outcome.connected, None);
    assert_eq!(outcome.disconnected, None);
    // Re-hello on the same connection ⇒ no transition (entry updated).
    let outcome = g1.bind(identity("x", true));
    assert_eq!(outcome.connected, None);
    assert_eq!(outcome.disconnected, None);
    // Re-hello with a different clientId ⇒ the old client stays live through
    // g2, the new one comes up.
    let outcome = g1.bind(identity("y", true));
    assert_eq!(outcome.connected, Some(identity("y", true)));
    assert_eq!(outcome.disconnected, None);
    // Releasing the only connection of `y` reports it gone; `x` is still
    // live through g2 until that is released too.
    assert_eq!(g1.release(), Some(identity("y", true)));
    assert_eq!(reg.len(), 1);
    assert_eq!(g2.release(), Some(identity("x", false)));
    assert!(reg.is_empty());

    // An un-hello'd connection releases silently.
    let (c3, _rx3) = idle_channel();
    let g3 = reg.register(c3, ReverseTransport::Uds);
    assert_eq!(g3.release(), None);
    assert!(reg.is_empty());
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
    let _ = guard_b.bind(identity("b", false));
    let _ = guard_a_first.bind(identity("a", true));
    let _ = guard_a_second.bind(identity("a", true));

    let clients = reg.live_clients();
    assert_eq!(clients.len(), 2, "un-hello'd connections are not clients");
    let a = &clients[0];
    assert_eq!(a.client_id.as_str(), "a", "ordered by first connection");
    assert_eq!(a.name.as_deref(), Some("client a"));
    assert_eq!(a.capabilities, json!({ "browserExec": true }));
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

#[tokio::test]
async fn dropping_guard_closes_an_accepted_request() {
    let reg = PrimaryReverseRegistry::new();
    let (channel, mut rx) = idle_channel();
    let guard = reg.register(channel, ReverseTransport::Wss);
    let _ = guard.bind(identity("a", true));
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
    let _ = guard.bind(identity("a", true));
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
    let _ = g_a.bind(identity("a", true));
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
