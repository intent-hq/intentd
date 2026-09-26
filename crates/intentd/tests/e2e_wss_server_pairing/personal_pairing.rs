//! Real daemon process restarts, UDS parity and hash-only reusable credentials.
use super::*;
use intent_core::{now_iso, Principal, PrincipalId};
use intent_store::Store;
use intentd_test_support::GuardedChild;

async fn call(ws: &mut common::TlsWs, id: u64, method: &str) -> Value {
    ws.send(Message::Text(
        json!({"jsonrpc":"2.0","id":id,"method":method})
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
                _ => panic!("expected a pairing RPC response"),
            }
        }
    })
    .await
    .expect("pairing RPC deadline")
}

fn spawn_personal(dir: &Path, sidecar: &Path, run: u8) -> GuardedChild {
    common::enable_ws_api(dir);
    let workspaces = dir.join("workspaces");
    std::fs::create_dir_all(&workspaces).unwrap();
    let log = std::fs::File::create(dir.join(format!("personal-{run}.log"))).unwrap();
    GuardedChild::spawn(
        common::serve_command()
            .env_remove("GITHUB_TOKEN")
            .env_remove("GH_TOKEN")
            .env_remove("GITLAB_TOKEN")
            .env("INTENTD_DATA_DIR", dir)
            .env("INTENTD_WORKSPACES_DIR", workspaces)
            .env("INTENTD_AUTH_TOKEN", TOKEN)
            .env("INTENTD_TAILCAT_BIN", sidecar)
            .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
            .env("MOCK_ACP_HOST", "localhost:0")
            .stdout(Stdio::null())
            .stderr(Stdio::from(log)),
    )
    .unwrap()
}

#[tokio::test]
async fn personal_pairing_reuses_uds_owner_member_guest_credentials_after_daemon_restart() {
    use std::os::unix::fs::PermissionsExt;
    let dir = common::test_tempdir_in("/tmp", "personal-pairing-restart-");
    std::fs::write(
        dir.path().join("config.toml"),
        "[server.tunnel]\nenabled = true\n",
    )
    .unwrap();
    // An owned, non-networked sidecar supplies a stable tunnel route. It blocks
    // on a signal rather than spawning a sleeping child, and exits on teardown.
    let sidecar = dir.path().join("tailcat-fixture.py");
    std::fs::write(&sidecar, "#!/usr/bin/env python3\nimport sys,pathlib,signal,json\nkey=next(x[6:] for x in sys.argv if x.startswith('--key='))\nif sys.argv[1]=='genkey': pathlib.Path(key).write_text('pairing-fixture')\nelse:\n print(json.dumps({'listenAddr':'tc-personal-fixture'}),flush=True)\n signal.pause()\n").unwrap();
    std::fs::set_permissions(&sidecar, std::fs::Permissions::from_mode(0o755)).unwrap();
    let store = Store::open(&dir.path().join("intentd.db")).await.unwrap();
    let owner = store.get_primary_principal().await.unwrap();
    let mut people = Vec::new();
    for (token, member) in [("b4".repeat(32), true), ("c4".repeat(32), false)] {
        let person = Principal {
            id: PrincipalId::new(),
            identity: None,
            github_user_id: None,
            login: None,
            display_name: None,
            avatar_url: None,
            is_primary: false,
            created_at: now_iso(),
            updated_at: now_iso(),
        };
        store.upsert_principal(&person).await.unwrap();
        store
            .insert_principal_credential(&person.id, &intent_transport::hash_token(&token))
            .await
            .unwrap();
        if member {
            sqlx::query("INSERT INTO host_member (principal_id, added_at) VALUES (?, ?)")
                .bind(&person.id.0)
                .bind(now_iso())
                .execute(store.write_pool())
                .await
                .unwrap();
        }
        people.push((person, token, if member { "member" } else { "guest" }));
    }
    let socket = dir.path().join("intentd.sock");
    let mut fingerprint = None;
    for run in 0..2 {
        let mut daemon = spawn_personal(dir.path(), &sidecar, run);
        common::await_daemon_listening(
            &mut daemon,
            &socket,
            &dir.path().join(format!("personal-{run}.log")),
        )
        .await;
        let status = common::await_wss_status(&socket).await;
        let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
        let info = uds_rpc(&socket, 1, "pairing.getSelfInfo", json!({})).await;
        assert!(
            info.get("error").is_none(),
            "UDS owner pairing must succeed"
        );
        assert!(info["result"]["token"].as_str() == Some(TOKEN));
        assert_eq!(info["result"]["principal"]["id"], owner.id.0);
        assert_eq!(info["result"]["principal"]["hostRole"], "owner");
        let fp = info["result"]["fingerprint"].as_str().unwrap().to_string();
        if let Some(previous) = &fingerprint {
            assert_eq!(previous, &fp);
        }
        fingerprint = Some(fp.clone());
        let legacy = uds_rpc(&socket, 2, "pairing.getInfo", json!({})).await;
        assert!(legacy["result"]["uri"] == info["result"]["uri"]);
        let refused = uds_rpc(
            &socket,
            3,
            "pairing.getSelfInfo",
            json!({"principalId":people[0].0.id.0}),
        )
        .await;
        assert_eq!(refused["error"]["data"]["code"], "invalid-params");
        for (person, token, role) in &people {
            let url = format!("wss://localhost:{port}/ws?token={token}");
            let (mut first, mut second) = tokio::join!(
                common::wss_connect_with_retry(port, client_config(&fp), &url),
                common::wss_connect_with_retry(port, client_config(&fp), &url)
            );
            for device in [&mut first, &mut second] {
                let reply = call(device, 4, "pairing.getSelfInfo").await;
                assert!(reply["result"]["token"].as_str() == Some(token));
                assert_eq!(reply["result"]["principal"]["id"], person.id.0);
                assert_eq!(reply["result"]["principal"]["hostRole"], *role);
                assert!(!reply.to_string().contains(TOKEN));
                assert_eq!(reply["result"]["fingerprint"], fp);
            }
        }
        let log = std::fs::read_to_string(dir.path().join(format!("personal-{run}.log"))).unwrap();
        for token in std::iter::once(TOKEN).chain(people.iter().map(|(_, t, _)| t.as_str())) {
            assert!(!log.contains(token), "daemon must not log credentials");
        }
        let shutdown = uds_rpc(&socket, 5, "system.shutdown", json!({})).await;
        assert!(shutdown.get("error").is_none());
        assert!(
            daemon
                .wait_with_timeout(common::rpc_read_timeout())
                .unwrap()
                .is_some(),
            "daemon and sidecar must exit before restart"
        );
        eprintln!("personal pairing restart round {run}: build={:?}, pid={}, TLS fingerprint preserved, same owner/member/guest credentials", intent_transport::BUILD_COMMIT, daemon.id());
    }
    let rows: Vec<(String,)> = sqlx::query_as("SELECT token_hash FROM principal_credential")
        .fetch_all(store.read_pool())
        .await
        .unwrap();
    assert_eq!(
        rows.len(),
        2,
        "pairing never mints another durable credential"
    );
    for (_, token, _) in &people {
        assert!(rows
            .iter()
            .any(|(hash,)| hash == &intent_transport::hash_token(token)));
    }
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM workspace WHERE id != '__chief__'")
            .fetch_one(store.read_pool())
            .await
            .unwrap(),
        0
    );
}

#[tokio::test]
async fn member_removal_real_uds_operation_rejects_old_devices_after_restart() {
    use std::os::unix::fs::PermissionsExt;
    let dir = common::test_tempdir_in("/tmp", "member-removal-restart-");
    std::fs::write(
        dir.path().join("config.toml"),
        "[server.tunnel]\nenabled = true\n",
    )
    .unwrap();
    let sidecar = dir.path().join("tailcat-fixture.py");
    std::fs::write(&sidecar,"#!/usr/bin/env python3\nimport sys,pathlib,signal,json\nkey=next(x[6:] for x in sys.argv if x.startswith('--key='))\nif sys.argv[1]=='genkey': pathlib.Path(key).write_text('removal-fixture')\nelse:\n print(json.dumps({'listenAddr':'tc-removal-fixture'}),flush=True)\n signal.pause()\n").unwrap();
    std::fs::set_permissions(&sidecar, std::fs::Permissions::from_mode(0o755)).unwrap();
    let store = Store::open(&dir.path().join("intentd.db")).await.unwrap();
    let mut member = store.get_primary_principal().await.unwrap();
    member.id = PrincipalId::new();
    member.is_primary = false;
    store.upsert_principal(&member).await.unwrap();
    sqlx::query("INSERT INTO host_member (principal_id,added_at) VALUES (?,?)")
        .bind(&member.id.0)
        .bind(now_iso())
        .execute(store.write_pool())
        .await
        .unwrap();
    let tokens = ["restart-device-one", "restart-device-two"];
    for token in tokens {
        store
            .insert_principal_credential(&member.id, &intent_transport::hash_token(token))
            .await
            .unwrap();
    }
    let mut guest = member.clone();
    guest.id = PrincipalId::new();
    store.upsert_principal(&guest).await.unwrap();
    let guest_token = "restart-unaffected";
    store
        .insert_principal_credential(&guest.id, &intent_transport::hash_token(guest_token))
        .await
        .unwrap();
    let socket = dir.path().join("intentd.sock");
    let mut fingerprint = None;
    for run in 0..2 {
        let mut daemon = spawn_personal(dir.path(), &sidecar, run);
        common::await_daemon_listening(
            &mut daemon,
            &socket,
            &dir.path().join(format!("personal-{run}.log")),
        )
        .await;
        let status = common::await_wss_status(&socket).await;
        let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
        let hello=uds_rpc(&socket,0,"client.hello",json!({"capabilities":{"hostMembership":false,"personalPairing":999,"authenticatedDevices":1}})).await;
        assert_eq!(
            hello["result"]["server"]["capabilities"]["hostMembership"],
            1
        );
        assert_eq!(
            hello["result"]["server"]["capabilities"]["personalPairing"],
            1
        );
        assert!(hello["result"]["server"]["capabilities"]
            .get("authenticatedDevices")
            .is_none());
        assert_eq!(
            hello["result"]["server"]["buildCommit"].as_str(),
            intent_transport::BUILD_COMMIT
        );
        let info = uds_rpc(&socket, 1, "pairing.getSelfInfo", json!({})).await;
        let fp = info["result"]["fingerprint"].as_str().unwrap().to_owned();
        if let Some(previous) = &fingerprint {
            assert_eq!(previous, &fp);
        }
        fingerprint = Some(fp.clone());
        assert!(info["result"]["token"].as_str() == Some(TOKEN));
        let url = format!("wss://localhost:{port}/ws?token={guest_token}");
        let mut unaffected = common::wss_connect_with_retry(port, client_config(&fp), &url).await;
        if run == 0 {
            let mut phones = Vec::new();
            for token in tokens {
                let url = format!("wss://localhost:{port}/ws?token={token}");
                let mut phone =
                    common::wss_connect_with_retry(port, client_config(&fp), &url).await;
                let pairing = call(&mut phone, 1, "pairing.getSelfInfo").await;
                assert!(pairing["result"]["token"].as_str() == Some(token));
                assert_eq!(pairing["result"]["principal"]["id"], member.id.0);
                phones.push(phone);
            }
            let removed = uds_rpc(
                &socket,
                2,
                "host.members.remove",
                json!({"principalId":member.id}),
            )
            .await;
            assert_eq!(removed["result"], json!({"removed":true}));
            for phone in &mut phones {
                let frame = timeout(common::rpc_read_timeout(), phone.next())
                    .await
                    .unwrap()
                    .unwrap()
                    .unwrap();
                assert!(
                    matches!(frame,Message::Close(Some(f)) if u16::from(f.code)==1008 && f.reason=="credential revoked")
                );
            }
        }
        for token in tokens {
            for path in ["/ws", "/tunnel"] {
                let tls = common::tls_connect_with_retry(port, client_config(&fp)).await;
                let url = format!("wss://localhost:{port}{path}?token={token}");
                let refused = tokio_tungstenite::client_async(url, tls).await;
                assert!(
                    matches!(refused,Err(tokio_tungstenite::tungstenite::Error::Http(r)) if r.status().as_u16()==401)
                );
            }
        }
        let kept = call(&mut unaffected, 3, "pairing.getSelfInfo").await;
        assert!(kept["result"]["token"].as_str() == Some(guest_token));
        assert_eq!(kept["result"]["principal"]["hostRole"], "guest");
        assert_eq!(
            uds_rpc(&socket, 4, "host.members.list", json!({})).await["result"]["members"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert!(uds_rpc(&socket, 5, "system.shutdown", json!({}))
            .await
            .get("error")
            .is_none());
        assert!(daemon
            .wait_with_timeout(common::rpc_read_timeout())
            .unwrap()
            .is_some());
        let log = std::fs::read_to_string(dir.path().join(format!("personal-{run}.log"))).unwrap();
        for token in tokens.into_iter().chain([TOKEN, guest_token]) {
            assert!(!log.contains(token), "daemon must not log credentials");
        }
        eprintln!("member removal restart round {run}: build={:?}, pid={}, durable revocation rejects old WSS and tunnel bearers",intent_transport::BUILD_COMMIT,daemon.id());
    }
}
