//! Local code pairing (§5.58, §2.7) through a real daemon: local-only owner
//! administration over UDS, the local pairing credential admitted on `/ws`
//! from loopback with owner authority but no owner-token surfaces, refused on
//! `/tunnel`, revocation closing the live connection with 1008, and the two
//! events reaching UDS subscribers only.
//!
//! The credential is seeded in the store (hash only, as redemption would)
//! before the daemon starts; `local_pair_endpoint` covers redemption itself.
use super::*;
use intent_store::Store;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::UnixStream;

const LOCAL_TOKEN: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

async fn send(ws: &mut common::TlsWs, id: u64, method: &str, params: Value) -> Value {
    ws.send(Message::Text(
        json!({"jsonrpc":"2.0","id":id,"method":method,"params":params})
            .to_string()
            .into(),
    ))
    .await
    .unwrap();
    timeout(common::rpc_read_timeout(), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(frame))) => {
                    let response: Value = serde_json::from_str(&frame).unwrap();
                    if response["id"] == id {
                        return response;
                    }
                }
                Some(Ok(Message::Ping(bytes))) => ws.send(Message::Pong(bytes)).await.unwrap(),
                other => panic!("expected an RPC response, got {other:?}"),
            }
        }
    })
    .await
    .expect("RPC deadline")
}

/// A long-lived UDS connection subscribed to the two local pairing events.
struct UdsEvents {
    reader: BufReader<tokio::net::unix::OwnedReadHalf>,
    _write: tokio::net::unix::OwnedWriteHalf,
}

impl UdsEvents {
    async fn subscribe(socket: &Path) -> Self {
        let stream = UnixStream::connect(socket).await.unwrap();
        let (read, mut write) = stream.into_split();
        let mut line = json!({"jsonrpc":"2.0","id":1,"method":"events.subscribe","params":{
            "eventTypes":["pairing:localCode-changed","pairing:localCredentials-changed"]}})
        .to_string();
        line.push('\n');
        write.write_all(line.as_bytes()).await.unwrap();
        let mut reader = BufReader::new(read);
        let reply = timeout(
            common::rpc_read_timeout(),
            common::uds_exchange::read_reply(&mut reader, 1, "events.subscribe"),
        )
        .await
        .unwrap();
        assert!(reply["result"].is_object(), "{reply}");
        Self {
            reader,
            _write: write,
        }
    }

    async fn next_event(&mut self, event_type: &str) -> Value {
        timeout(common::rpc_read_timeout(), async {
            loop {
                let mut buf = String::new();
                assert!(self.reader.read_line(&mut buf).await.unwrap() > 0);
                let frame: Value = serde_json::from_str(buf.trim_end()).unwrap();
                let event = &frame["params"]["event"];
                if event["type"] == event_type {
                    return event["data"].clone();
                }
            }
        })
        .await
        .unwrap_or_else(|_| panic!("no {event_type} event over UDS"))
    }
}

async fn refused_status(port: u16, fingerprint: &str, path: &str) -> Option<u16> {
    let tls = common::tls_connect_with_retry(port, client_config(fingerprint)).await;
    let url = format!("wss://localhost:{port}{path}?token={LOCAL_TOKEN}");
    match tokio_tungstenite::client_async(url, tls).await {
        Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
            Some(response.status().as_u16())
        }
        _ => None,
    }
}

#[tokio::test]
async fn local_pairing_credential_lifecycle() {
    let dir = temp_data_dir();
    let token_hash = intent_transport::hash_token(LOCAL_TOKEN);
    {
        let store = Store::open(&dir.path().join("intentd.db")).await.unwrap();
        store
            .insert_local_pairing_credential("lp-1", &token_hash, "Intent Alpha")
            .await
            .unwrap();
    }
    let _daemon = spawn_serve(dir.path());
    let socket = dir.path().join("intentd.sock");
    let (port, fingerprint) = boot(dir.path()).await;
    let mut uds_events = UdsEvents::subscribe(&socket).await;

    let listed = uds_rpc(&socket, 2, "pairing.listLocalCredentials", json!({})).await;
    let credentials = listed["result"]["credentials"].as_array().unwrap();
    assert_eq!(credentials.len(), 1, "{listed}");
    assert_eq!(credentials[0]["credentialId"], "lp-1");
    assert_eq!(credentials[0]["appName"], "Intent Alpha");
    assert_eq!(credentials[0]["connected"], false);
    assert!(credentials[0]["lastUsedAt"].is_null());

    // Owner connection over WSS: subscribes to the pairing events, which must
    // never reach it.
    let mut owner = connect_ws(port, client_config(&fingerprint)).await;
    let subscribed = send(
        &mut owner,
        1,
        "events.subscribe",
        json!({"eventTypes":["pairing:localCode-changed","pairing:localCredentials-changed"]}),
    )
    .await;
    assert!(subscribed["result"].is_object(), "{subscribed}");

    // Admitted from loopback on /ws.
    let url = format!("wss://localhost:{port}/ws?token={LOCAL_TOKEN}");
    let mut paired = common::wss_connect_with_retry(port, client_config(&fingerprint), &url).await;
    let connected = uds_events
        .next_event("pairing:localCredentials-changed")
        .await;
    assert_eq!(connected["credential"]["credentialId"], "lp-1");
    assert_eq!(connected["credential"]["connected"], true);

    // Owner authority for ordinary methods…
    let workspaces = send(&mut paired, 2, "workspace.list", json!({})).await;
    assert!(workspaces.get("error").is_none(), "{workspaces}");
    // …but every local-only method and owner-token surface is refused.
    for (id, method) in [
        (3, "pairing.listLocalCredentials"),
        (4, "pairing.createLocalCode"),
        (5, "pairing.getInfo"),
        (6, "server.pairingInfo"),
    ] {
        let refused = send(&mut paired, id, method, json!({})).await;
        assert_eq!(refused["error"]["code"], -32001, "{method}: {refused}");
    }
    let self_info = send(&mut paired, 7, "pairing.getSelfInfo", json!({})).await;
    assert_eq!(self_info["error"]["code"], -32003, "{self_info}");
    assert_eq!(
        self_info["error"]["data"]["code"],
        "local-pairing-credential"
    );
    let redeem = send(
        &mut paired,
        8,
        "pairing.redeemLocalCode",
        json!({"code":"ABC-DEF","appName":"x"}),
    )
    .await;
    assert_eq!(redeem["error"]["code"], -32001);
    let mint = send(
        &mut paired,
        9,
        "executor.mint",
        json!({"backendId":"b","backendName":"B"}),
    )
    .await;
    assert_eq!(mint["error"]["code"], -32003, "{mint}");

    // Refused on /tunnel.
    assert_eq!(
        refused_status(port, &fingerprint, "/tunnel").await,
        Some(401)
    );

    let listed = uds_rpc(&socket, 10, "pairing.listLocalCredentials", json!({})).await;
    assert_eq!(listed["result"]["credentials"][0]["connected"], true);
    assert!(listed["result"]["credentials"][0]["lastUsedAt"].is_string());

    // Codes over UDS: create, replace, cancel; events reach UDS only.
    let created = uds_rpc(&socket, 11, "pairing.createLocalCode", json!({})).await;
    let code = created["result"]["code"].as_str().unwrap();
    assert_eq!(code.len(), 7, "{created}");
    let code_id = created["result"]["localCode"]["codeId"].clone();
    let event = uds_events.next_event("pairing:localCode-changed").await;
    assert_eq!(event["localCode"]["codeId"], code_id);
    assert_eq!(event["localCode"]["state"], "active");
    assert!(!event.to_string().contains(code));
    let cancelled = uds_rpc(
        &socket,
        12,
        "pairing.cancelLocalCode",
        json!({"codeId": code_id}),
    )
    .await;
    assert_eq!(cancelled["result"], json!({"cancelled": true}));
    let event = uds_events.next_event("pairing:localCode-changed").await;
    assert_eq!(event["localCode"]["state"], "cancelled");
    assert_eq!(event["localCode"]["cancelReason"], "user");

    // Revoke while connected: 1008, then 401 on replay.
    let revoked = uds_rpc(
        &socket,
        13,
        "pairing.revokeLocalCredential",
        json!({"credentialId":"lp-1"}),
    )
    .await;
    assert_eq!(revoked["result"], json!({"revoked": true}));
    let close = timeout(common::rpc_read_timeout(), async {
        loop {
            match paired.next().await {
                Some(Ok(Message::Close(frame))) => return frame,
                Some(Ok(_)) => {}
                other => panic!("expected a close frame, got {other:?}"),
            }
        }
    })
    .await
    .expect("close deadline")
    .expect("close frame");
    assert_eq!(u16::from(close.code), 1008);
    assert_eq!(close.reason, "credential revoked");
    let event = uds_events
        .next_event("pairing:localCredentials-changed")
        .await;
    assert_eq!(event, json!({"credentialId":"lp-1","revoked":true}));
    assert_eq!(refused_status(port, &fingerprint, "/ws").await, Some(401));
    let listed = uds_rpc(&socket, 14, "pairing.listLocalCredentials", json!({})).await;
    assert_eq!(listed["result"]["credentials"], json!([]));

    // The owner's WSS subscription saw none of the local pairing events.
    let leaked = timeout(Duration::from_millis(300), async {
        loop {
            match owner.next().await {
                Some(Ok(Message::Text(frame))) => {
                    let frame: Value = serde_json::from_str(&frame).unwrap();
                    if frame["params"]["event"]["type"]
                        .as_str()
                        .is_some_and(|t| t.starts_with("pairing:"))
                    {
                        return frame;
                    }
                }
                Some(Ok(Message::Ping(bytes))) => owner.send(Message::Pong(bytes)).await.unwrap(),
                Some(Ok(_)) => {}
                other => panic!("owner connection ended: {other:?}"),
            }
        }
    })
    .await;
    assert!(leaked.is_err(), "pairing event reached WSS: {leaked:?}");
    let still_owner = send(&mut owner, 2, "workspace.list", json!({})).await;
    assert!(still_owner.get("error").is_none());
}

#[tokio::test]
async fn local_pairing_methods_are_refused_over_wss() {
    let dir = temp_data_dir();
    let _daemon = spawn_serve(dir.path());
    let (port, fingerprint) = boot(dir.path()).await;
    let mut owner = connect_ws(port, client_config(&fingerprint)).await;
    for (id, method, params) in [
        (1, "pairing.createLocalCode", json!({})),
        (2, "pairing.cancelLocalCode", json!({})),
        (3, "pairing.listLocalCredentials", json!({})),
        (
            4,
            "pairing.revokeLocalCredential",
            json!({"credentialId":"x"}),
        ),
    ] {
        let refused = send(&mut owner, id, method, params).await;
        assert_eq!(refused["error"]["code"], -32001, "{method}: {refused}");
    }
    let redeem = send(
        &mut owner,
        5,
        "pairing.redeemLocalCode",
        json!({"code":"ABC-DEF","appName":"x"}),
    )
    .await;
    assert_eq!(redeem["error"]["code"], -32001);
    assert_eq!(
        redeem["error"]["message"],
        "pairing.redeemLocalCode is served only on /local-pair"
    );
}
