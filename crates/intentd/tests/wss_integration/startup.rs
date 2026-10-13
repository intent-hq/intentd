//! Full daemon startup against a temporary database, with transaction barriers.
use super::*;
use intentd_test_support::GuardedChild;
use serde_json::json;
use std::os::unix::fs::PermissionsExt;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::time::timeout;

type Ws = tokio_tungstenite::WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>;

async fn barrier(listener: &UnixListener, stage: u8) -> UnixStream {
    let (mut gate, _) = timeout(common::daemon_startup_timeout(), listener.accept())
        .await
        .expect("startup reached barrier")
        .unwrap();
    assert_eq!(gate.read_u8().await.unwrap(), stage);
    gate
}

async fn call(ws: &mut Ws, method: &str, params: Value) -> Value {
    timeout(Duration::from_secs(3), async {
        ws.send(Message::Text(
            json!({"jsonrpc":"2.0","id":41,"method":method,"params":params})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => return serde_json::from_str(&text).unwrap(),
                Some(Ok(Message::Ping(data))) => ws.send(Message::Pong(data)).await.unwrap(),
                other => panic!("RPC response expected: {other:?}"),
            }
        }
    })
    .await
    .expect("startup RPC responds while transaction is blocked")
}

async fn uds_call(path: &Path, method: &str) -> Value {
    timeout(Duration::from_secs(3), async {
        let mut stream = UnixStream::connect(path).await.unwrap();
        stream
            .write_all(format!("{}\n", json!({"jsonrpc":"2.0","id":42,"method":method})).as_bytes())
            .await
            .unwrap();
        let mut line = String::new();
        BufReader::new(stream).read_line(&mut line).await.unwrap();
        serde_json::from_str(&line).unwrap()
    })
    .await
    .expect("UDS status responds during indexing")
}

async fn seed(dir: &Path) -> usize {
    let store = Store::open(&dir.join("intentd.db")).await.unwrap();
    sqlx::query("INSERT INTO workspace(id,title,branch,created_at,updated_at) VALUES ('startup','private title','main','now','now')").execute(store.write_pool()).await.unwrap();
    let content = "A😀\r\n旧".repeat(3000);
    sqlx::query("INSERT INTO note(id,workspace_id,title,content,rev,created_at,updated_at) VALUES ('spec','startup','secret note title',?,17,'now','now')").bind(&content).execute(store.write_pool()).await.unwrap();
    let principal = intent_core::Principal {
        id: intent_core::PrincipalId::from("startup-guest"),
        identity: None,
        github_user_id: None,
        login: None,
        display_name: None,
        avatar_url: None,
        is_primary: false,
        created_at: "now".into(),
        updated_at: "now".into(),
    };
    store.upsert_principal(&principal).await.unwrap();
    store
        .insert_principal_credential(
            &principal.id,
            &intent_transport::hash_token("valid-guest-token"),
        )
        .await
        .unwrap();
    store
        .add_workspace_member(
            &WorkspaceId::from("startup"),
            &principal.id,
            intent_core::WorkspaceRole::Collaborator,
        )
        .await
        .unwrap();
    store.close().await;
    content.len()
}

fn launch(dir: &Path, gate: &Path) -> (GuardedChild, u16) {
    common::enable_ws_api(dir);
    let registry = intent_services::SettingsRegistry::load(dir.join("config.toml")).unwrap();
    let port = registry.snapshot().effective.server.ws_api.port;
    let mut command = common::hermetic_serve_command_fixed_port(dir);
    command
        .env("INTENTD_TCP_PORT", port.to_string())
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .env("INTENTD_WORKSPACES_DIR", dir.join("workspaces"))
        .env("INTENTD_TEST_INDEX_STARTUP_GATE", gate)
        .env("TOKIO_WORKER_THREADS", "1")
        .stdout(std::process::Stdio::null())
        .stderr(std::fs::File::create(dir.join("startup.log")).unwrap());
    if let Ok(proxy_port) = std::fs::read_to_string(dir.join("proxy-port")) {
        command
            .env("INTENTD_TAILCAT_BIN", dir.join("tailcat-fixture"))
            .env("INTENTD_TEST_PROXY_PORT", proxy_port.trim());
    }
    let service_gate = dir.join("service-gate");
    if service_gate.exists() {
        command.env("INTENTD_TEST_SETTINGS_WS_START_GATE", service_gate);
    }
    (GuardedChild::spawn(&mut command).unwrap(), port)
}

fn startup_client_config(dir: &Path) -> Arc<ClientConfig> {
    let intent_transport::CertStatus::Valid { fingerprint } = intent_transport::inspect_cert(dir)
    else {
        panic!("child daemon must persist its TLS certificate before indexing");
    };
    client_config(&fingerprint)
}

async fn database(dir: &Path) -> sqlx::SqlitePool {
    sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(dir.join("intentd.db"))
                .read_only(true),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn protected_startup_transaction_progress_and_fresh_handoff() {
    let dir = common::test_tempdir_in("/tmp", "startup-e2e-");
    let bytes = seed(dir.path()).await;
    // A real local byte-forwarding sidecar verifies the saved route carries
    // the same TLS/owner-token endpoint during indexing and after handoff.
    let proxy_port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    std::fs::write(dir.path().join("proxy-port"), proxy_port.to_string()).unwrap();
    let fixture = dir.path().join("tailcat-fixture");
    std::fs::write(
        &fixture,
        include_str!("../fixtures/startup-tailcat-proxy.py"),
    )
    .unwrap();
    std::fs::set_permissions(&fixture, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::create_dir(dir.path().join("tunnel")).unwrap();
    std::fs::write(
        dir.path().join("tunnel/tailcat.private.json"),
        "saved-test-key",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("config.toml"),
        "[server]\nbindAddress = \"0.0.0.0\"\n[server.tunnel]\nenabled = true\nonly = true\n",
    )
    .unwrap();
    let gate_path = dir.path().join("gate");
    let listener = UnixListener::bind(&gate_path).unwrap();
    let (mut child, port) = launch(dir.path(), &gate_path);
    let mut inventory = barrier(&listener, b'i').await;
    let cfg = startup_client_config(dir.path());
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_intentd"))
        .arg("status")
        .env("INTENTD_DATA_DIR", dir.path())
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("not ready (indexing)"));
    let socket = dir.path().join("intentd.sock");
    assert_eq!(
        std::fs::metadata(&socket).unwrap().permissions().mode() & 0o777,
        0o600
    );
    let mut ws = connect_ws(proxy_port, cfg.clone()).await;
    let hello = call(
        &mut ws,
        "client.hello",
        json!({"clientId":"must-not-persist","name":"pending client"}),
    )
    .await;
    assert_eq!(hello["error"]["data"]["code"], "daemon-not-ready");
    let attempt = hello["error"]["data"]["startup"]["attemptId"].clone();
    let status = call(&mut ws, "system.startupStatus", json!({})).await;
    assert_eq!(status["result"]["phase"], "indexing");
    assert_eq!(
        status["result"]["progress"],
        json!({"completedNotes":0,"totalNotes":1,"processedSourceBytes":0,"totalSourceBytes":bytes})
    );
    assert!(status["result"].get("estimatedRemainingMs").is_none());
    assert!(!status.to_string().contains("secret note title"));
    assert_eq!(
        uds_call(&socket, "system.startupStatus").await["result"]["attemptId"],
        attempt
    );
    for method in [
        "workspace.list",
        "note.add",
        "events.subscribe",
        "principal.me",
        "system.shutdown",
        "unknown",
    ] {
        let response = call(
            &mut ws,
            method,
            json!({"workspaceId":"startup","noteId":"spec","content":"MUST NOT WRITE"}),
        )
        .await;
        assert_eq!(response["error"]["code"], -32012, "{method}");
    }
    assert_eq!(
        uds_call(&socket, "workspace.list").await["error"]["code"],
        -32012
    );
    // Notification followed by a request proves it is neither answered nor run.
    ws.send(Message::Text(json!({"jsonrpc":"2.0","method":"client.hello","params":{"clientId":"notification-client"}}).to_string().into())).await.unwrap();
    assert_eq!(
        call(&mut ws, "system.startupStatus", json!({})).await["id"],
        41
    );
    for (path, origin, token, code) in [
        ("/ws", None, None, 401),
        ("/ws", None, Some("guest-or-invalid"), 503),
        ("/ws", None, Some("valid-guest-token"), 503),
        ("/ws", Some("https://evil.example"), Some(TOKEN), 403),
        ("/invite", None, None, 503),
        ("/tunnel", None, Some(TOKEN), 503),
        ("/unknown", None, Some(TOKEN), 404),
    ] {
        let response = https_request(port, cfg.clone(), &upgrade_req(path, origin, token)).await;
        assert_eq!(status_code(&response), code, "{path}: {response}");
        assert!(!response.contains("attemptId"));
        if code == 503 {
            assert!(response.contains("Retry-After: 1"));
        }
    }
    let health = https_request(
        port,
        cfg.clone(),
        "GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n",
    )
    .await;
    assert_eq!(status_code(&health), 503);
    assert!(health.ends_with("{\"status\":\"starting\"}"));
    let db = database(dir.path()).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM client")
            .fetch_one(&db)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT rev FROM note WHERE id='spec'")
            .fetch_one(&db)
            .await
            .unwrap(),
        17
    );
    inventory.write_u8(b'c').await.unwrap();
    // The actual projection worker is blocked, while the sole async runtime
    // worker must continue serving both protected transports.
    let mut note = barrier(&listener, b'n').await;
    assert_eq!(
        uds_call(&socket, "system.startupStatus").await["result"]["phase"],
        "indexing"
    );
    assert_eq!(
        call(&mut ws, "system.startupStatus", json!({})).await["result"]["progress"]
            ["completedNotes"],
        0
    );
    note.write_u8(b'c').await.unwrap();
    let mut committing = barrier(&listener, b'c').await;
    let status = call(&mut ws, "system.startupStatus", json!({})).await;
    assert_eq!(status["result"]["phase"], "finalizing");
    assert_eq!(status["result"]["finalizingStep"], "committing");
    assert_eq!(status["result"]["progress"]["completedNotes"], 1);
    assert_eq!(status["result"]["progress"]["processedSourceBytes"], bytes);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT indexed_rev FROM note_page_head")
            .fetch_one(&db)
            .await
            .unwrap(),
        -1,
        "rebuild is still uncommitted"
    );
    let mut old_uds = UnixStream::connect(&socket).await.unwrap();
    // Force admission before handoff, rather than merely connecting into backlog.
    old_uds
        .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"system.startupStatus\"}\n")
        .await
        .unwrap();
    let mut old_uds = BufReader::new(old_uds);
    old_uds.read_line(&mut String::new()).await.unwrap();
    committing.write_u8(b'c').await.unwrap();
    timeout(common::daemon_startup_timeout(), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Close(Some(frame)))) => {
                    assert_eq!(
                        frame.code,
                        tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Away
                    );
                    assert_eq!(frame.reason, "Startup complete; reconnect");
                    break;
                }
                Some(Ok(Message::Ping(data))) => ws.send(Message::Pong(data)).await.unwrap(),
                other => panic!("handoff closes bootstrap seat: {other:?}"),
            }
        }
    })
    .await
    .expect("ready handoff");
    assert_eq!(
        timeout(Duration::from_secs(3), old_uds.read_u8())
            .await
            .unwrap()
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::UnexpectedEof
    );
    let mut ready = connect_ws(proxy_port, cfg.clone()).await;
    let status = call(&mut ready, "system.startupStatus", json!({})).await;
    let live = call(&mut ready, "system.status", json!({})).await;
    assert_eq!(live["result"]["tcAddress"], "tc-saved-startup-test:443");
    assert_eq!(
        std::fs::read_to_string(dir.path().join("tunnel/tailcat.private.json")).unwrap(),
        "saved-test-key"
    );
    assert_eq!(status["result"]["phase"], "ready");
    assert_eq!(status["result"]["attemptId"], attempt);
    assert!(status["result"].get("retryAfterMs").is_none());
    let hello = call(
        &mut ready,
        "client.hello",
        json!({"clientId":"now-ready","name":"ready client"}),
    )
    .await;
    assert_eq!(
        hello["result"]["server"]["capabilities"]["startupStatus"], 1,
        "{hello}"
    );
    assert_eq!(
        call(&mut ready, "unknown", json!({})).await["error"]["code"],
        -32601
    );
    let invalid = https_request(
        port,
        cfg,
        &upgrade_req("/ws", None, Some("guest-or-invalid")),
    )
    .await;
    assert_eq!(status_code(&invalid), 401);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT rev FROM note WHERE id='spec'")
            .fetch_one(&db)
            .await
            .unwrap(),
        17
    );
    db.close().await;
    child.signal(nix::sys::signal::Signal::SIGTERM).unwrap();
    assert!(child
        .wait_with_timeout(Duration::from_secs(20))
        .unwrap()
        .unwrap()
        .success());
    assert!(!socket.exists());
}

#[tokio::test]
async fn failed_commit_and_shutdown_roll_back_and_restart_resets_attempt() {
    let dir = common::test_tempdir_in("/tmp", "startup-rollback-");
    seed(dir.path()).await;
    let gate_path = dir.path().join("gate");
    let listener = UnixListener::bind(&gate_path).unwrap();
    let mut previous = Value::Null;
    for fail_commit in [true, false] {
        let (mut child, port) = launch(dir.path(), &gate_path);
        let mut inventory = barrier(&listener, b'i').await;
        let cfg = startup_client_config(dir.path());
        let mut ws = connect_ws(port, cfg).await;
        let status = call(&mut ws, "system.startupStatus", json!({})).await;
        let attempt = status["result"]["attemptId"].clone();
        assert_ne!(attempt, previous);
        previous = attempt;
        assert_eq!(status["result"]["progress"]["completedNotes"], 0);
        inventory.write_u8(b'c').await.unwrap();
        let mut projection = barrier(&listener, b'n').await;
        if fail_commit {
            projection.write_u8(b'c').await.unwrap();
            barrier(&listener, b'c').await.write_u8(b'f').await.unwrap();
        } else {
            child.signal(nix::sys::signal::Signal::SIGTERM).unwrap();
        }
        assert_eq!(
            child
                .wait_with_timeout(Duration::from_secs(20))
                .unwrap()
                .unwrap()
                .success(),
            !fail_commit
        );
        assert!(!dir.path().join("intentd.sock").exists());
        let db = database(dir.path()).await;
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT indexed_rev FROM note_page_head")
                .fetch_one(&db)
                .await
                .unwrap(),
            -1
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT rev FROM note WHERE id='spec'")
                .fetch_one(&db)
                .await
                .unwrap(),
            17
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM client")
                .fetch_one(&db)
                .await
                .unwrap(),
            0
        );
        db.close().await;
        // Reacquiring proves physical database ownership was released at exit.
        drop(Store::prepare_for_daemon(&dir.path().join("intentd.db")).unwrap());
    }
}

#[tokio::test]
async fn services_stop_uses_canonical_teardown() {
    service_startup_exit(false).await;
}

#[tokio::test]
async fn services_admission_failure_uses_canonical_teardown() {
    service_startup_exit(true).await;
}

async fn service_startup_exit(invalidate_policy: bool) {
    let dir = common::test_tempdir_in("/tmp", "startup-services-");
    seed(dir.path()).await;
    let gate_path = dir.path().join("gate");
    let indexing = UnixListener::bind(&gate_path).unwrap();
    let services = UnixListener::bind(dir.path().join("service-gate")).unwrap();
    let (mut child, _) = launch(dir.path(), &gate_path);
    for stage in [b'i', b'n', b'c'] {
        barrier(&indexing, stage)
            .await
            .write_u8(b'c')
            .await
            .unwrap();
    }
    let (mut service_gate, _) = timeout(common::daemon_startup_timeout(), services.accept())
        .await
        .unwrap()
        .unwrap();
    let socket = dir.path().join("intentd.sock");
    let status = uds_call(&socket, "system.startupStatus").await;
    assert_eq!(status["result"]["phase"], "finalizing");
    assert_eq!(status["result"]["finalizingStep"], "services");
    if invalidate_policy {
        std::fs::write(dir.path().join("config.toml"), "invalid = [").unwrap();
        service_gate.write_u8(1).await.unwrap();
    } else {
        // Keep setup blocked: shutdown must cancel this unadmitted listener
        // check while preserving every already-created service cleanup owner.
        child.signal(nix::sys::signal::Signal::SIGTERM).unwrap();
    }
    assert_eq!(
        child
            .wait_with_timeout(Duration::from_secs(20))
            .unwrap()
            .unwrap()
            .success(),
        !invalidate_policy
    );
    let log = std::fs::read_to_string(dir.path().join("startup.log")).unwrap();
    assert!(log.contains("agent manager ready"), "{log}");
    assert!(
        !log.contains("startup completion reconciliation complete"),
        "{log}"
    );
    for phase in [
        "cleanup",
        "request_drain",
        "startup_recovery_join",
        "agent_shutdown",
        "writer_drain",
        "store_close",
    ] {
        assert!(
            log.lines()
                .any(|line| line.contains(&format!("phase=\"{phase}\" state=\"completed\""))),
            "missing completed {phase}: {log}"
        );
    }
    assert!(!socket.exists());
    drop(Store::prepare_for_daemon(&dir.path().join("intentd.db")).unwrap());
    let db = database(dir.path()).await;
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT rev FROM note WHERE id='spec'")
            .fetch_one(&db)
            .await
            .unwrap(),
        17
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT indexed_rev FROM note_page_head")
            .fetch_one(&db)
            .await
            .unwrap(),
        17
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM client")
            .fetch_one(&db)
            .await
            .unwrap(),
        0
    );
    db.close().await;
}

#[tokio::test]
async fn bootstrap_disable_failure_and_owner_token_rotation() {
    use intent_core::startup::StartupStatus;
    use intent_transport::startup::StartupDispatch;
    let dir = common::test_tempdir_in("/tmp", "startup-policy-");
    let tls = ensure_tls_certificate(dir.path()).unwrap();
    let cfg = client_config(&tls.fingerprint256);
    let tokens = Arc::new(AsyncTokenStore::new(Arc::new(MemTokenStore(Mutex::new(
        Some(TOKEN.into()),
    )))));
    let dispatch = StartupDispatch::new(StartupStatus::new());
    let disabled = WsApiServer::new_startup(
        dispatch.clone(),
        Some(&tls),
        Some(&tokens),
        WsOptions {
            enabled: false,
            base_port: 0,
            ..Default::default()
        },
    )
    .unwrap();
    let port = disabled.start().await.unwrap();
    let rejected = https_request(port, cfg.clone(), &upgrade_req("/ws", None, Some(TOKEN))).await;
    assert_eq!(status_code(&rejected), 403);
    disabled.stop().await;
    let server = WsApiServer::new_startup(
        dispatch.clone(),
        Some(&tls),
        Some(&tokens),
        WsOptions {
            base_port: 0,
            ..Default::default()
        },
    )
    .unwrap();
    let port = server.start().await.unwrap();
    let mut ws = connect_ws(port, cfg.clone()).await;
    dispatch.status().fail();
    let failed = call(&mut ws, "system.startupStatus", json!({})).await;
    assert_eq!(failed["result"]["phase"], "failed");
    assert_eq!(
        call(&mut ws, "note.add", json!({})).await["error"]["data"]["retryable"],
        false
    );
    let health = https_request(
        port,
        cfg.clone(),
        "GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n",
    )
    .await;
    assert!(health.ends_with("{\"status\":\"failed\"}"));
    tokens.store_token(&"cd".repeat(32)).await.unwrap();
    timeout(Duration::from_secs(3), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Close(Some(close)))) => {
                    assert_eq!(close.reason, "Credential changed; reconnect");
                    break;
                }
                Some(Ok(Message::Ping(data))) => ws.send(Message::Pong(data)).await.unwrap(),
                other => panic!("rotation closes restricted owner seat: {other:?}"),
            }
        }
    })
    .await
    .unwrap();
    let rejected = https_request(port, cfg, &upgrade_req("/ws", None, Some(TOKEN))).await;
    assert_eq!(status_code(&rejected), 503);
    server.stop().await;
}
