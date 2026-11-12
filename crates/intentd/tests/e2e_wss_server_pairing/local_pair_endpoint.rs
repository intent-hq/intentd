//! Local code pairing end to end (§5.58) between two real daemons with
//! separate data folders on one computer, sharing one discovery folder.
//!
//! The test plays the requester's desktop app (A's main process): it reads
//! the discovery folder, skips A's own record, dials B's `/local-pair` with
//! the record's fingerprint pinned, redeems the code B created over UDS, and
//! connects to B's `/ws` with the returned credential. Wrong, expired and
//! used codes fail with the contract errors. B runs with a shortened code
//! lifetime so expiry is observable in seconds.
use super::*;
use tokio_tungstenite::tungstenite::client::IntoClientRequest as _;

/// B's shortened code lifetime (the seam only ever shortens the 10 minutes).
const CODE_LIFETIME_MS: u64 = 3_000;

fn spawn_named(data_dir: &Path, discovery: &Path, product: &str, lifetime: bool) -> GuardedChild {
    common::enable_ws_api(data_dir);
    let discovery = discovery.to_string_lossy().to_string();
    let lifetime_ms = CODE_LIFETIME_MS.to_string();
    let mut env = vec![
        ("INTENT_LOCAL_DAEMONS_DIR", discovery.as_str()),
        ("INTENTD_PRODUCT_NAME", product),
    ];
    if lifetime {
        env.push((
            "INTENTD_LOCAL_PAIRING_CODE_LIFETIME_MS",
            lifetime_ms.as_str(),
        ));
    }
    spawn_serve_inner(data_dir, &env)
}

/// The requester's list: every record in the folder that advertises
/// `localCodePairing: 1`, minus its own daemon (matched by fingerprint).
fn discover(discovery: &Path, own_fingerprint: &str) -> Vec<Value> {
    let Ok(entries) = std::fs::read_dir(discovery) else {
        return Vec::new();
    };
    entries
        .filter_map(Result::ok)
        .filter(|e| e.path().extension().is_some_and(|x| x == "json"))
        .filter_map(|e| serde_json::from_slice::<Value>(&std::fs::read(e.path()).ok()?).ok())
        .filter(|r| r["version"] == 1 && r["capabilities"]["localCodePairing"] == 1)
        .filter(|r| r["fingerprint"] != own_fingerprint)
        .collect()
}

async fn await_peer(discovery: &Path, own_fingerprint: &str, port: u16) -> Value {
    let budget = common::daemon_startup_timeout();
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        let found = discover(discovery, own_fingerprint);
        if let [record] = found.as_slice() {
            if record["port"] == port {
                return record.clone();
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "B never appeared alone in A's list within {budget:?}: {found:?}"
        );
        // timing-guard: poll interval for the discovery folder, bounded by the startup budget
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn redeem(port: u16, fingerprint: &str, id: u64, code: &str, app: &str) -> Value {
    let url = format!("wss://localhost:{port}/local-pair");
    let mut ws = common::wss_connect_with_retry(port, client_config(fingerprint), &url).await;
    ws.send(Message::Text(
        json!({"jsonrpc":"2.0","id":id,"method":"pairing.redeemLocalCode",
            "params":{"code":code,"appName":app}})
        .to_string()
        .into(),
    ))
    .await
    .unwrap();
    timeout(common::rpc_read_timeout(), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(frame))) => {
                    return serde_json::from_str::<Value>(&frame).unwrap();
                }
                Some(Ok(Message::Ping(bytes))) => ws.send(Message::Pong(bytes)).await.unwrap(),
                other => panic!("expected a redeem response, got {other:?}"),
            }
        }
    })
    .await
    .expect("redeem deadline")
}

async fn create_code(socket: &Path, id: i64) -> (String, Value) {
    let created = uds_rpc(socket, id, "pairing.createLocalCode", json!({})).await;
    let code = created["result"]["code"]
        .as_str()
        .unwrap_or_else(|| panic!("createLocalCode: {created}"))
        .to_string();
    (code, created["result"]["localCode"].clone())
}

/// A wrong code with the same shape as `code`.
fn wrong_code(code: &str) -> String {
    if code == "AAA-AAA" {
        "BBB-BBB".into()
    } else {
        "AAA-AAA".into()
    }
}

async fn upgrade_status(port: u16, fingerprint: &str, path_and_query: &str) -> Option<u16> {
    let tls = common::tls_connect_with_retry(port, client_config(fingerprint)).await;
    let url = format!("wss://localhost:{port}{path_and_query}");
    match tokio_tungstenite::client_async(url, tls).await {
        Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
            Some(response.status().as_u16())
        }
        Ok(_) => Some(101),
        Err(_) => None,
    }
}

#[tokio::test]
async fn two_daemons_pair_with_a_short_code() {
    let root = common::test_tempdir_in("/tmp", "itd-local-pair-");
    let discovery = root.path().join("local-daemons");
    let dir_a = root.path().join("a");
    let dir_b = root.path().join("b");
    std::fs::create_dir_all(&dir_a).unwrap();
    std::fs::create_dir_all(&dir_b).unwrap();
    let _a = spawn_named(&dir_a, &discovery, "Intent A", false);
    let _b = spawn_named(&dir_b, &discovery, "Intent B", true);
    let (port_a, fingerprint_a) = boot(&dir_a).await;
    let (port_b, fingerprint_b) = boot(&dir_b).await;
    let socket_b = dir_b.join("intentd.sock");

    // B advertises the capability.
    let caps = uds_rpc(&socket_b, 1, "system.capabilities", json!({})).await;
    assert_eq!(caps["result"]["localCodePairing"], 1, "{caps}");

    // A's list shows B only, with B's live port and fingerprint.
    let record = await_peer(&discovery, &fingerprint_a, port_b).await;
    assert_eq!(record["displayName"], "Intent B");
    assert_eq!(record["fingerprint"], fingerprint_b);
    let pinned = record["fingerprint"].as_str().unwrap().to_string();

    // No active code yet: expired.
    let none = redeem(port_b, &pinned, 2, "KTP-82M", "Intent A").await;
    assert_eq!(none["error"]["code"], -32602, "{none}");
    assert_eq!(none["error"]["data"]["code"], "local-code-expired");

    // An expired code fails.
    let (stale, _) = create_code(&socket_b, 3).await;
    // timing-guard: outlast B's shortened code lifetime; expiry is the daemon's own timer
    tokio::time::sleep(Duration::from_millis(CODE_LIFETIME_MS + 1_500)).await;
    let expired = redeem(port_b, &pinned, 4, &stale, "Intent A").await;
    assert_eq!(
        expired["error"]["data"]["code"], "local-code-expired",
        "{expired}"
    );

    // A wrong code counts a try; the right one, typed loosely, grants.
    let (code, local_code) = create_code(&socket_b, 5).await;
    let wrong = redeem(port_b, &pinned, 6, &wrong_code(&code), "Intent A").await;
    assert_eq!(wrong["error"]["code"], -32602, "{wrong}");
    assert_eq!(wrong["error"]["data"]["code"], "local-code-invalid");
    assert_eq!(wrong["error"]["data"]["attemptsRemaining"], 4);
    let typed = code.to_lowercase().replace('-', "");
    let grant = redeem(port_b, &pinned, 7, &typed, "Intent A").await;
    let grant = grant["result"].clone();
    assert_eq!(grant["version"], 1, "{grant}");
    assert_eq!(grant["host"], "127.0.0.1");
    assert_eq!(grant["port"], port_b, "the port A dialed");
    assert_eq!(grant["fingerprint"], pinned, "the fingerprint A pinned");
    assert_eq!(grant["displayName"], "Intent B");
    let token = grant["token"].as_str().unwrap().to_string();
    assert_eq!(token.len(), 64);

    // The code is spent.
    let used = redeem(port_b, &pinned, 8, &code, "Intent A").await;
    assert_eq!(
        used["error"]["data"]["code"], "local-code-expired",
        "{used}"
    );

    // A connects to B's /ws with the credential: owner authority on B.
    let url = format!("wss://localhost:{port_b}/ws?token={token}");
    let mut paired = common::wss_connect_with_retry(port_b, client_config(&pinned), &url).await;
    paired
        .send(Message::Text(
            json!({"jsonrpc":"2.0","id":1,"method":"workspace.list","params":{}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
    let listed = timeout(common::rpc_read_timeout(), async {
        loop {
            if let Some(Ok(Message::Text(frame))) = paired.next().await {
                let reply: Value = serde_json::from_str(&frame).unwrap();
                if reply["id"] == 1 {
                    return reply;
                }
            }
        }
    })
    .await
    .unwrap();
    assert!(listed.get("error").is_none(), "{listed}");

    // B lists A by name, connected; the code ended redeemed for it.
    let credentials = uds_rpc(&socket_b, 9, "pairing.listLocalCredentials", json!({})).await;
    let credentials = credentials["result"]["credentials"]
        .as_array()
        .unwrap()
        .clone();
    assert_eq!(credentials.len(), 1, "{credentials:?}");
    assert_eq!(credentials[0]["appName"], "Intent A");
    assert_eq!(credentials[0]["credentialId"], grant["credentialId"]);
    assert!(local_code["codeId"].is_string());

    // The credential is B's alone: A's daemon refuses it.
    assert_eq!(
        upgrade_status(port_a, &fingerprint_a, &format!("/ws?token={token}")).await,
        Some(401)
    );
    // /local-pair refuses a browser origin at upgrade.
    let tls = common::tls_connect_with_retry(port_b, client_config(&pinned)).await;
    let mut request = format!("wss://localhost:{port_b}/local-pair")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("Origin", "https://evil.example".parse().unwrap());
    match tokio_tungstenite::client_async(request, tls).await {
        Err(tokio_tungstenite::tungstenite::Error::Http(response)) => {
            assert_eq!(response.status().as_u16(), 403);
        }
        other => panic!("expected 403, got {:?}", other.map(|_| ())),
    }
}
