//! WSS end-to-end for `agent.subscribe` / `agent.unsubscribe` real delivery
//! (monorepo#937): a subscription registered over the wire with a subscriber
//! `agentId` must actually deliver — a matching workspace event published by
//! another actor wakes the subscriber with a batched `[WORKSPACE EVENTS]`
//! message (visible in `agent.getConversation`), and `agent.unsubscribe`
//! stops delivery. Drives a real [`WsApiServer`] over plain `ws://`
//! (insecure dev mode) so the WebSocket-upgrade → JSON-RPC → router →
//! services → store round-trip is exercised end-to-end.

#![cfg(unix)]

mod common;

use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use intent_core::WorkspaceApi;
use intent_services::{EventBus, Services};
use intent_store::Store;
use intent_transport::{WsApiServer, WsOptions};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type PlainWs = WebSocketStream<MaybeTlsStream<TcpStream>>;

struct TempDir(PathBuf);
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

struct Fixture {
    _ws: WsApiServer,
    port: u16,
    _dir: TempDir,
}

async fn boot() -> Fixture {
    let short = uuid::Uuid::new_v4().simple().to_string();
    let dir = std::env::temp_dir().join(format!("intentd-evsub-{}", &short[..8]));
    std::fs::create_dir_all(&dir).unwrap();
    let store = Store::open(&dir.join("intentd.db")).await.expect("store");
    let bus = EventBus::new(store.clone());
    let workspaces_root = dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_root).expect("mkdir hermetic root");
    let services = Services::new(store)
        .with_workspaces_root(workspaces_root)
        .with_settings_registry(common::registry_with_default_provider(&dir))
        .with_event_bus(bus.clone());
    let api: Arc<dyn WorkspaceApi> = Arc::new(services);
    let opts = WsOptions {
        base_port: 0,
        bind_address: Ipv4Addr::LOCALHOST.into(),
        ..Default::default()
    };
    let ws = WsApiServer::new_insecure(api, bus, opts, None);
    let port = ws.start().await.expect("start");
    Fixture {
        _ws: ws,
        port,
        _dir: TempDir(dir),
    }
}

async fn connect(port: u16) -> PlainWs {
    let url = format!("ws://127.0.0.1:{port}/ws");
    let (sock, _resp) = tokio_tungstenite::connect_async(&url)
        .await
        .expect("plain ws handshake");
    sock
}

async fn wss_rpc(ws: &mut PlainWs, id: i64, method: &str, params: Value) -> Value {
    let v = wss_rpc_raw(ws, id, method, params).await;
    assert!(v.get("error").is_none(), "rpc {method} errored: {v}");
    v["result"].clone()
}

async fn wss_rpc_raw(ws: &mut PlainWs, id: i64, method: &str, params: Value) -> Value {
    let req = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(req.to_string().into()))
        .await
        .unwrap();
    timeout(common::rpc_read_timeout(), async {
        loop {
            match ws.next().await.unwrap().unwrap() {
                Message::Text(text) => {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    if v.get("id") == Some(&json!(id)) {
                        return v;
                    }
                }
                Message::Ping(_) | Message::Pong(_) => {}
                _ => panic!("unexpected message"),
            }
        }
    })
    .await
    .expect("response timeout")
}

async fn create_agent(rpc: &mut PlainWs, id: i64, ws_id: &str, name: &str) -> String {
    let created = wss_rpc(
        rpc,
        id,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": name }),
    )
    .await;
    created["agent"]["id"]
        .as_str()
        .expect("agent id")
        .to_string()
}

/// Serialized conversation text for an agent (empty string when no messages).
async fn conversation_text(rpc: &mut PlainWs, id: i64, ws_id: &str, agent_id: &str) -> String {
    let convo = wss_rpc(
        rpc,
        id,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    convo.to_string()
}

/// Subscribe over the wire with a subscriber `agentId`, drive a matching
/// `note:*` event from another actor (a front-door `note.create` emits
/// `note:created`), and assert the subscriber is woken with the batched
/// `[WORKSPACE EVENTS]` message. Then unsubscribe and assert a further
/// matching event delivers nothing. Agent subscribers use non-agent
/// categories here because `agent:*` is rejected for them (monorepo#1229),
/// which is asserted separately below.
#[tokio::test]
async fn agent_subscribe_delivers_batched_wake_over_wss() {
    let fx = boot().await;
    let mut rpc = connect(fx.port).await;

    let created = wss_rpc(
        &mut rpc,
        1,
        "workspace.create",
        json!({ "title": "Event sub e2e", "path": "." }),
    )
    .await;
    let ws_id = created["workspace"]["id"].as_str().unwrap().to_string();

    let subscriber = create_agent(&mut rpc, 2, &ws_id, "Watcher").await;

    // The guard over the wire (monorepo#1229): an agent-owned subscription to
    // the `agent:*` wildcard or an exact agent event type is rejected with an
    // error pointing at ws.agent.watch.
    for (i, event_type) in ["agent:*", "agent:message", "agent:tool:call"]
        .iter()
        .enumerate()
    {
        let err = wss_rpc_raw(
            &mut rpc,
            200 + i64::try_from(i).expect("value fits in i64"),
            "agent.subscribe",
            json!({
                "workspaceId": ws_id,
                "agentId": subscriber,
                "eventTypes": [event_type],
            }),
        )
        .await;
        let msg = err["error"]["message"].as_str().unwrap_or_default();
        assert!(
            msg.contains("ws.agent.watch"),
            "{event_type} must be rejected for agent subscribers: {err}"
        );
    }

    // Subscribe with the subscriber identity and a short batch window
    // (PROTOCOL §5.5 request shape + monorepo#937 `agentId`).
    let sub = wss_rpc(
        &mut rpc,
        4,
        "agent.subscribe",
        json!({
            "workspaceId": ws_id,
            "agentId": subscriber,
            "eventTypes": ["note:*"],
            "batchWindow": 50,
        }),
    )
    .await;
    let sub_id = sub["subscriptionId"].as_str().expect("subscriptionId");
    assert_eq!(sub["eventTypes"], json!(["note:*"]));

    // Another actor's event: a front-door `note.create` publishes
    // `note:created` in this workspace, which matches `note:*` and is not
    // the subscriber's own actor id.
    wss_rpc(
        &mut rpc,
        5,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "N1", "content": "one" }),
    )
    .await;

    // Await the batched wake landing in the subscriber's conversation.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let mut req_id = 6;
    loop {
        let text = conversation_text(&mut rpc, req_id, &ws_id, &subscriber).await;
        req_id += 1;
        if text.contains("WORKSPACE EVENTS") {
            assert!(text.contains("note:created"), "wake names the event type");
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "subscriber never received the batched wake; conversation: {text}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Unsubscribe (PROTOCOL §5.5 response shape) …
    let un = wss_rpc(
        &mut rpc,
        100,
        "agent.unsubscribe",
        json!({ "workspaceId": ws_id, "subscriptionId": sub_id }),
    )
    .await;
    assert_eq!(un["success"], json!(true));
    assert_eq!(un["subscriptionId"], json!(sub_id));

    // … and a further matching event no longer delivers. Settle first so any
    // batch already in flight when the delivery task was aborted lands
    // before the baseline read (review: flake window).
    tokio::time::sleep(Duration::from_millis(400)).await;
    let baseline = conversation_text(&mut rpc, 101, &ws_id, &subscriber).await;
    wss_rpc(
        &mut rpc,
        102,
        "note.create",
        json!({ "workspaceId": ws_id, "title": "N2", "content": "two" }),
    )
    .await;
    tokio::time::sleep(Duration::from_millis(400)).await;
    let after = conversation_text(&mut rpc, 103, &ws_id, &subscriber).await;
    assert_eq!(
        baseline, after,
        "no wake may be delivered after unsubscribe"
    );

    // Unknown id → error (kept response contract).
    let err = wss_rpc_raw(
        &mut rpc,
        104,
        "agent.unsubscribe",
        json!({ "workspaceId": ws_id, "subscriptionId": "missing" }),
    )
    .await;
    assert!(
        err.get("error").is_some(),
        "unknown subscription must error"
    );
}

/// Bare `*` expansion over the wire (monorepo#1229): an agent-owned
/// subscription silently narrows to the non-agent categories (no `agent:*`,
/// no `chat:stream:delta`), while a front-door subscription (no `agentId`)
/// keeps the full category expansion including `agent:*`.
#[tokio::test]
async fn bare_star_expansion_narrows_for_agent_subscribers_over_wss() {
    let fx = boot().await;
    let mut rpc = connect(fx.port).await;

    let created = wss_rpc(
        &mut rpc,
        1,
        "workspace.create",
        json!({ "title": "Bare-star narrowing e2e", "path": "." }),
    )
    .await;
    let ws_id = created["workspace"]["id"].as_str().unwrap().to_string();
    let subscriber = create_agent(&mut rpc, 2, &ws_id, "Watcher").await;

    let sub = wss_rpc(
        &mut rpc,
        3,
        "agent.subscribe",
        json!({
            "workspaceId": ws_id,
            "agentId": subscriber,
            "eventTypes": ["*"],
        }),
    )
    .await;
    let types: Vec<&str> = sub["eventTypes"]
        .as_array()
        .expect("eventTypes array")
        .iter()
        .filter_map(Value::as_str)
        .collect();
    assert!(
        !types.contains(&"agent:*") && !types.contains(&"chat:stream:delta"),
        "agent subscriber's bare * must exclude agent events: {types:?}"
    );
    for expected in ["file:*", "task:*", "note:*", "workspace:*"] {
        assert!(
            types.contains(&expected),
            "agent subscriber's bare * keeps {expected}: {types:?}"
        );
    }

    // Front door (no agentId): the full expansion, `agent:*` included.
    let sub = wss_rpc(
        &mut rpc,
        4,
        "agent.subscribe",
        json!({ "workspaceId": ws_id, "eventTypes": ["*"] }),
    )
    .await;
    let types: Vec<&str> = sub["eventTypes"]
        .as_array()
        .expect("eventTypes array")
        .iter()
        .filter_map(Value::as_str)
        .collect();
    assert!(
        types.contains(&"agent:*"),
        "front-door bare * keeps agent:*: {types:?}"
    );
}

/// The subscriber's event subscriptions of `agent.getSubscriptions` for the
/// given agent, over the wire.
async fn event_subscriptions_view(
    rpc: &mut PlainWs,
    id: i64,
    ws_id: &str,
    agent_id: &str,
) -> Vec<Value> {
    let subs = wss_rpc(
        rpc,
        id,
        "agent.getSubscriptions",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    subs["eventSubscriptions"]
        .as_array()
        .expect("eventSubscriptions array")
        .clone()
}

/// monorepo#947 introspection + workspace-delete cleanup over the wire:
/// `agent.subscribe` → `agent.getSubscriptions` lists the event subscription
/// (additive `eventSubscriptions` field; existing fields intact) and
/// `agent.diagnostics` reports it; `agent.unsubscribe` removes it from the
/// view; a second subscription then disappears when its workspace is deleted
/// (`workspace.delete` sweep).
#[tokio::test]
async fn event_subscriptions_introspection_and_workspace_delete_cleanup_over_wss() {
    let fx = boot().await;
    let mut rpc = connect(fx.port).await;

    let created = wss_rpc(
        &mut rpc,
        1,
        "workspace.create",
        json!({ "title": "Event sub introspection e2e", "path": "." }),
    )
    .await;
    let ws_id = created["workspace"]["id"].as_str().unwrap().to_string();
    let subscriber = create_agent(&mut rpc, 2, &ws_id, "Watcher").await;

    // Baseline: existing fields intact, additive field present and empty.
    let baseline = wss_rpc(
        &mut rpc,
        3,
        "agent.getSubscriptions",
        json!({ "workspaceId": ws_id, "agentId": subscriber }),
    )
    .await;
    assert!(baseline["subscriptions"].is_array());
    assert!(baseline["delegationGroups"].is_array());
    assert!(baseline["agentStatuses"].is_object());
    assert_eq!(baseline["eventSubscriptions"], json!([]));

    let sub = wss_rpc(
        &mut rpc,
        4,
        "agent.subscribe",
        json!({
            "workspaceId": ws_id,
            "agentId": subscriber,
            "eventTypes": ["note:*"],
            "excludeSelf": false,
            "batchWindow": 75,
        }),
    )
    .await;
    let sub_id = sub["subscriptionId"].as_str().expect("subscriptionId");

    // getSubscriptions now lists it with the documented wire fields.
    let subs = event_subscriptions_view(&mut rpc, 5, &ws_id, &subscriber).await;
    assert_eq!(subs.len(), 1);
    assert_eq!(subs[0]["id"], json!(sub_id));
    assert_eq!(subs[0]["workspaceId"], json!(ws_id));
    assert_eq!(subs[0]["subscriberAgentId"], json!(subscriber));
    assert_eq!(subs[0]["eventTypes"], json!(["note:*"]));
    assert_eq!(subs[0]["excludeSelf"], json!(false));
    assert_eq!(subs[0]["batchWindow"], json!(75));
    assert!(subs[0]["createdAt"].is_string());

    // agent.diagnostics reports it: snapshot array, summary count, and the
    // per-agent eventSubscriptionCount.
    let diag = wss_rpc(
        &mut rpc,
        6,
        "agent.diagnostics",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    let d = &diag["diagnostics"];
    assert_eq!(d["summary"]["eventSubscriptions"], json!(1));
    assert_eq!(d["eventSubscriptions"][0]["id"], json!(sub_id));
    assert_eq!(d["eventSubscriptions"][0]["orphaned"], json!(false));
    let row = d["agents"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["id"] == json!(subscriber))
        .expect("subscriber row");
    assert_eq!(row["eventSubscriptionCount"], json!(1));
    assert!(diag["text"]
        .as_str()
        .expect("text")
        .contains("Event subscriptions: 1"));

    // Unsubscribe removes it from the introspection view.
    wss_rpc(
        &mut rpc,
        7,
        "agent.unsubscribe",
        json!({ "workspaceId": ws_id, "subscriptionId": sub_id }),
    )
    .await;
    let subs = event_subscriptions_view(&mut rpc, 8, &ws_id, &subscriber).await;
    assert_eq!(subs, Vec::<Value>::new());

    // Re-subscribe, then delete the workspace: the sweep drops the
    // subscription (registry + row), so a fresh daemon-side view is empty.
    wss_rpc(
        &mut rpc,
        9,
        "agent.subscribe",
        json!({
            "workspaceId": ws_id,
            "agentId": subscriber,
            "eventTypes": ["note:*"],
        }),
    )
    .await;
    assert_eq!(
        event_subscriptions_view(&mut rpc, 10, &ws_id, &subscriber)
            .await
            .len(),
        1
    );
    let deleted = wss_rpc(
        &mut rpc,
        11,
        "workspace.delete",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(deleted["success"], json!(true));
    // The registry sweep is observable over the wire: `agent.getSubscriptions`
    // reads the daemon-global registry per subscriber (the workspace row is
    // not consulted), so the deleted workspace's subscription must be gone.
    let subs = event_subscriptions_view(&mut rpc, 12, &ws_id, &subscriber).await;
    assert_eq!(
        subs,
        Vec::<Value>::new(),
        "workspace.delete must sweep the workspace's event subscriptions"
    );
    // And re-subscribing fails closed: the workspace's agents were deleted
    // with it, so the dead subscriber is rejected.
    let err = wss_rpc_raw(
        &mut rpc,
        13,
        "agent.subscribe",
        json!({
            "workspaceId": ws_id,
            "agentId": subscriber,
            "eventTypes": ["note:*"],
        }),
    )
    .await;
    assert!(
        err.get("error").is_some(),
        "subscribing for a deleted workspace's agent must fail: {err}"
    );
}
