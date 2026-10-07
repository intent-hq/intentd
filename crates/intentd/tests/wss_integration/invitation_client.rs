//! Invitation clients must remain live while proof work owns the test coroutine.

use std::time::Instant;

use futures_util::FutureExt;
use intent_core::{events::WORKSPACE_UPDATED, ActorType, EventActor};
use intent_store::NewEvent;
use serde_json::json;
use tokio::sync::watch;

use super::*;

#[path = "../common/invitation_client.rs"]
mod fixture_client;
use fixture_client::{await_workspace_updated, from_raw, wss_rpc, RawWs};

const INTERVAL: Duration = Duration::from_millis(25);
const HEARTBEAT_TIMEOUT: Duration = Duration::from_millis(75);
const OBSERVE_TIMEOUT: Duration = Duration::from_secs(5);

async fn start_gated() -> (Server, watch::Sender<bool>) {
    let (gate, receiver) = watch::channel(false);
    let srv = start(WsOptions {
        heartbeat_interval: INTERVAL,
        heartbeat_timeout: HEARTBEAT_TIMEOUT,
        heartbeat_gate: Some(receiver),
        ..WsOptions::default()
    })
    .await;
    eprintln!("INVITE6041 regression fixture={}", srv.dir.path().display());
    (srv, gate)
}

async fn wait_registered(srv: &Server, count: usize) {
    tokio::time::timeout(OBSERVE_TIMEOUT, async {
        while srv.ws.client_count() != count {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("clients register before enabling the reaper");
}

async fn pass_deadline(registered: Instant) {
    tokio::time::timeout(OBSERVE_TIMEOUT, async {
        while registered.elapsed() <= HEARTBEAT_TIMEOUT {
            // timing-guard: observe the real monotonic deadline while reaping is gated
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("the registered client passes its heartbeat deadline");
    eprintln!(
        "INVITE6041 release age_ms={}",
        registered.elapsed().as_millis()
    );
}

async fn wait_reaped(srv: &Server, initial_count: usize) {
    tokio::time::timeout(OBSERVE_TIMEOUT, async {
        while srv.ws.client_count() >= initial_count {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("an enabled heartbeat sweep removes the silent sentinel");
    eprintln!(
        "INVITE6041 post-sweep client_count={}",
        srv.ws.client_count()
    );
}

async fn assert_abrupt_eof(ws: &mut RawWs) {
    // The server has already reaped this socket. Reading WebSocket frames here
    // would automatically flush queued Pong replies and may report BrokenPipe
    // before the TLS read can observe the missing close_notify. Observe only
    // the existing TLS read side, without polling/sending any WebSocket frame.
    let mut pending = Vec::new();
    let result = tokio::time::timeout(
        OBSERVE_TIMEOUT,
        ws.get_mut().take(64 * 1024).read_to_end(&mut pending),
    )
    .await
    .expect("heartbeat abort is a socket EOF, not an outer timeout");
    let error = result.expect_err("reaped TLS stream must not end cleanly or fill the read cap");
    eprintln!("INVITE6041 reaped TLS read error={error:?}");
    assert_eq!(error.kind(), std::io::ErrorKind::UnexpectedEof);
    assert!(error.to_string().contains("close_notify"), "{error}");
}

#[intent_test_macros::daemon_test]
async fn invitation_unpolled_client_is_reaped_after_gate_release() {
    let (srv, gate) = start_gated().await;
    let mut client = connect_ws(srv.port, srv.cfg.clone()).await;
    client.send(Message::Text(json!({"jsonrpc":"2.0","id":1,"method":"events.subscribe","params":{"eventTypes":["workspace:updated"]}}).to_string().into())).await.expect("raw subscribe");
    tokio::time::timeout(OBSERVE_TIMEOUT, async {
        loop {
            match client.next().await {
                Some(Ok(Message::Text(text))) => {
                    let ack: Value = serde_json::from_str(&text).expect("raw response");
                    if ack["id"] == 1 {
                        assert!(ack.get("error").is_none(), "{ack}");
                        break;
                    }
                }
                Some(Ok(Message::Ping(payload))) => client
                    .send(Message::Pong(payload))
                    .await
                    .expect("raw setup pong"),
                other => panic!("raw subscribe response: {other:?}"),
            }
        }
    })
    .await
    .expect("raw subscription ready");
    wait_registered(&srv, 1).await;
    let registered = Instant::now();
    pass_deadline(registered).await;
    assert_eq!(
        srv.ws.client_count(),
        1,
        "closed gate retains the silent subscriber"
    );
    gate.send(true).expect("release reaper");
    wait_reaped(&srv, 1).await;
    assert_abrupt_eof(&mut client).await;
    srv.ws.stop().await;
}

#[intent_test_macros::daemon_test]
async fn invitation_client_retains_workspace_event_across_reaper_sweep() {
    let (srv, gate) = start_gated().await;
    let ws_id = WorkspaceId::new();
    srv.store
        .insert_workspace(&fixture_workspace(&ws_id))
        .await
        .expect("workspace");
    let mut client = from_raw(connect_ws(srv.port, srv.cfg.clone()).await);
    let ack = wss_rpc(
        &mut client,
        1,
        "events.subscribe",
        json!({"eventTypes": ["workspace:updated"]}),
    )
    .await;
    assert!(ack.get("error").is_none(), "{ack}");
    // A separate raw socket proves that the actual reaper ran. It is registered
    // after the target, so its expired deadline also bounds the target's idle gap.
    let mut sentinel = connect_ws(srv.port, srv.cfg.clone()).await;
    wait_registered(&srv, 2).await;
    let registered = Instant::now();
    srv.bus.publish(&NewEvent {
        workspace_id: ws_id.clone(),
        timestamp: now_iso(),
        event_type: WORKSPACE_UPDATED.to_string(),
        actor: EventActor { actor_type: ActorType::User, id: Some("owner".into()), ..Default::default() },
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: json!({"workspaceId": ws_id.0, "changes": {"members": true, "memberCount": 2, "addedPrincipalId": "invited"}}),
    }).await.expect("publish real retained workspace event");
    pass_deadline(registered).await;
    assert_eq!(srv.ws.client_count(), 2, "closed gate retains both clients");
    gate.send(true).expect("release reaper");
    wait_reaped(&srv, 2).await;
    assert_abrupt_eof(&mut sentinel).await;

    // Preserve cleanup even when the current invitation client fails. A buffered
    // event alone is insufficient: the following RPC must prove ongoing liveness.
    let outcome = std::panic::AssertUnwindSafe(async {
        let event = await_workspace_updated(&mut client, "retained across heartbeat", |changes| {
            changes["addedPrincipalId"] == "invited" && changes["memberCount"] == 2
        })
        .await;
        assert_eq!(event["data"]["workspaceId"], ws_id.0);
        assert_eq!(event["data"]["changes"]["members"], true);
        eprintln!("INVITE6041 retained event delivered; checking subsequent RPC");
        let principal = wss_rpc(&mut client, 2, "principal.me", json!({})).await;
        assert!(principal.get("error").is_none(), "{principal}");
        assert_eq!(
            srv.ws.client_count(),
            1,
            "responsive invitation client survives real sweep"
        );
    })
    .catch_unwind()
    .await;
    drop(client);
    drop(sentinel);
    srv.ws.stop().await;
    if let Err(panic) = outcome {
        std::panic::resume_unwind(panic);
    }
}

#[intent_test_macros::daemon_test]
async fn invitation_client_drop_releases_socket_without_reaping() {
    let (srv, gate) = start_gated().await;
    let mut client = from_raw(connect_ws(srv.port, srv.cfg.clone()).await);
    let principal = wss_rpc(&mut client, 1, "principal.me", json!({})).await;
    assert!(principal.get("error").is_none(), "{principal}");
    wait_registered(&srv, 1).await;
    assert!(!*gate.borrow(), "heartbeat reaping remains disabled");
    drop(client);
    tokio::time::timeout(OBSERVE_TIMEOUT, async {
        while srv.ws.client_count() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("dropping the owned reader closes its peer without a heartbeat reap");
    assert!(
        !*gate.borrow(),
        "cleanup cannot be credited to heartbeat reaping"
    );
    assert_eq!(
        srv.ws.bound_port().await,
        Some(srv.port),
        "listener still runs"
    );
    srv.ws.stop().await;
}
