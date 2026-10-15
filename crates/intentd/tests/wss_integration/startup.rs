//! Protected preparation and lazy note demand against a temporary database.
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
    .expect("startup RPC responds while preparation is blocked")
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
    .expect("UDS status responds during preparation")
}

async fn seed(dir: &Path) -> usize {
    let store = Store::open(&dir.join("intentd.db")).await.unwrap();
    sqlx::query("INSERT INTO workspace(id,title,branch,created_at,updated_at) VALUES ('startup','private title','main','now','now')").execute(store.write_pool()).await.unwrap();
    let content = "A😀\r\n旧".repeat(3000);
    sqlx::query("INSERT INTO note(id,workspace_id,title,content,rev,created_at,updated_at) VALUES ('spec','startup','secret note title',?,17,'now','now')").bind(&content).execute(store.write_pool()).await.unwrap();
    sqlx::query("INSERT INTO note(id,workspace_id,title,content,rev,created_at,updated_at) VALUES ('unopened','startup','unopened fixture',?,17,'now','now')").bind(&content).execute(store.write_pool()).await.unwrap();
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
    let denied = intent_core::Principal {
        id: intent_core::PrincipalId::from("startup-denied"),
        ..principal
    };
    store.upsert_principal(&denied).await.unwrap();
    store
        .insert_principal_credential(
            &denied.id,
            &intent_transport::hash_token("valid-denied-token"),
        )
        .await
        .unwrap();
    store.close().await;
    content.len()
}

fn launch(dir: &Path, gate: &Path) -> (GuardedChild, u16) {
    launch_with_handshake(dir, gate, false)
}

fn launch_with_handshake(dir: &Path, gate: &Path, advertised: bool) -> (GuardedChild, u16) {
    common::enable_ws_api(dir);
    let registry = intent_services::SettingsRegistry::load(dir.join("config.toml")).unwrap();
    let port = registry.snapshot().effective.server.ws_api.port;
    let mut command = common::hermetic_serve_command_fixed_port(dir);
    command
        .env("INTENTD_TCP_PORT", port.to_string())
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .env("INTENTD_WORKSPACES_DIR", dir.join("workspaces"))
        .env("INTENTD_TEST_PREPARING_STARTUP_GATE", gate)
        .env("TOKIO_WORKER_THREADS", "1")
        .stdout(std::process::Stdio::null())
        .stderr(std::fs::File::create(dir.join("startup.log")).unwrap());
    command.env_remove("INTENTD_SITTER_IDLE_RESTART");
    if advertised {
        command.env("INTENTD_SITTER_IDLE_RESTART", "1");
    }
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
        panic!("child daemon must persist its TLS certificate before preparation");
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
async fn protected_preparation_and_fresh_handoff_preserve_lazy_backlog() {
    let dir = common::test_tempdir_in("/tmp", "startup-e2e-");
    seed(dir.path()).await;
    // A real local byte-forwarding sidecar verifies the saved route carries
    // the same TLS/owner-token endpoint during preparation and after handoff.
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
    let mut preparation = barrier(&listener, b'p').await;
    assert!(!common::daemon_is_ready(&dir.path().join("intentd.sock")).await);
    let cfg = startup_client_config(dir.path());
    let output = std::process::Command::new(intentd_test_support::runtime_paths::binary_path(
        "intentd",
        env!("CARGO_BIN_EXE_intentd"),
    ))
    .arg("status")
    .env("INTENTD_DATA_DIR", dir.path())
    .output()
    .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stdout).contains("not ready (preparing)"));
    // Old installed sitters execute this NEW binary for their liveness probe.
    // The CLI may confirm liveness, but the protected wire stays not-ready.
    let output = std::process::Command::new(intentd_test_support::runtime_paths::binary_path(
        "intentd",
        env!("CARGO_BIN_EXE_intentd"),
    ))
    .args(["call", "system.status"])
    .env("INTENTD_DATA_DIR", dir.path())
    .output()
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let cli: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(cli["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(cli["ready"], false);
    assert_eq!(cli["startup"]["phase"], "preparing");
    let socket = dir.path().join("intentd.sock");
    assert_eq!(
        uds_call(&socket, "system.status").await["error"]["code"],
        -32012
    );
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
    // The private monitor handshake is application admission too. A bootstrap
    // seat cannot acquire or remember monitor authority before the fresh hello.
    let monitor_params = json!({"clientId":"must-not-persist-monitor", "connectionRole":"review-monitor", "capabilities":{"reviewMonitoring":1}});
    let monitor = call(&mut ws, "client.hello", monitor_params.clone()).await;
    assert_eq!(monitor["error"]["code"], -32012);
    assert_eq!(monitor["error"]["data"]["code"], "daemon-not-ready");
    let status = call(&mut ws, "system.startupStatus", json!({})).await;
    assert_eq!(status["result"]["phase"], "preparing");
    assert!(status["result"].get("progress").is_none());
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
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM note_page_head WHERE indexed_rev <> -1")
            .fetch_one(&db)
            .await
            .unwrap(),
        0,
        "preparation must not convert unopened notes"
    );
    let mut old_uds = UnixStream::connect(&socket).await.unwrap();
    // Force admission before handoff, rather than merely connecting into backlog.
    old_uds
        .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"system.startupStatus\"}\n")
        .await
        .unwrap();
    let mut old_uds = BufReader::new(old_uds);
    old_uds.read_line(&mut String::new()).await.unwrap();
    preparation.write_u8(b'c').await.unwrap();
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
    assert!(common::daemon_is_ready(&dir.path().join("intentd.sock")).await);
    assert_eq!(status["result"]["attemptId"], attempt);
    assert!(status["result"].get("retryAfterMs").is_none());
    assert!(status["result"].get("progress").is_none());
    assert!(status["result"].get("estimatedRemainingMs").is_none());
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM note_page_head WHERE indexed_rev <> -1")
            .fetch_one(&db)
            .await
            .unwrap(),
        0,
        "ready must not convert the stale backlog"
    );
    let monitor = call(&mut ready, "client.hello", monitor_params).await;
    assert_eq!(monitor["error"]["code"], -32602, "{monitor}");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM client WHERE id='must-not-persist-monitor'"
        )
        .fetch_one(&db)
        .await
        .unwrap(),
        0,
        "fresh handoff must not turn a bootstrap claim into private authority"
    );
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
    // A ready daemon must retain lazy demand semantics. Authentication without
    // workspace authorization and legacy full reads cannot activate conversion.
    let cfg = startup_client_config(dir.path());
    let mut denied = common::wss_connect_with_retry(
        port,
        cfg.clone(),
        &format!("wss://localhost:{port}/ws?token=valid-denied-token"),
    )
    .await;
    let page = json!({"workspaceId":"startup","noteId":"spec","page":{"kind":"source"}});
    let refused = call(&mut denied, "note.get", page.clone()).await;
    assert_eq!(refused["error"]["data"]["code"], "not-found", "{refused}");
    let mut guest = common::wss_connect_with_retry(
        port,
        cfg,
        &format!("wss://localhost:{port}/ws?token=valid-guest-token"),
    )
    .await;
    let legacy = call(
        &mut guest,
        "note.get",
        json!({"workspaceId":"startup","noteId":"spec"}),
    )
    .await;
    assert!(legacy.get("error").is_none(), "{legacy}");
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM note_page_head WHERE indexed_rev <> -1")
            .fetch_one(&db)
            .await
            .unwrap(),
        0
    );

    // Hold the real database writer, forcing the owned demand job to remain
    // pending while ordinary authenticated status stays ready and responsive.
    let writer = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new().filename(dir.path().join("intentd.db")),
        )
        .await
        .unwrap();
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&writer)
        .await
        .unwrap();
    let pending = call(&mut guest, "note.get", page.clone()).await;
    assert_eq!(pending["error"]["code"], -32006, "{pending}");
    assert_eq!(
        pending["error"]["data"],
        json!({"code":"note-index-preparing","retryAfterMs":1000})
    );
    let during = call(&mut ready, "system.startupStatus", json!({})).await;
    assert_eq!(during["result"]["phase"], "ready");
    assert_eq!(during["result"]["attemptId"], attempt);
    assert!(during["result"].get("progress").is_none());
    assert!(during["result"].get("estimatedRemainingMs").is_none());
    assert!(common::daemon_is_ready(&socket).await);
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM note_page_head WHERE indexed_rev <> -1")
            .fetch_one(&db)
            .await
            .unwrap(),
        0
    );
    guest.close(None).await.unwrap();
    sqlx::query("ROLLBACK").execute(&writer).await.unwrap();
    writer.close().await;
    let mut guest = common::wss_connect_with_retry(
        port,
        startup_client_config(dir.path()),
        &format!("wss://localhost:{port}/ws?token=valid-guest-token"),
    )
    .await;
    let prepared = timeout(Duration::from_secs(15), async {
        loop {
            let response = call(&mut guest, "note.get", page.clone()).await;
            if response["error"]["data"]["code"] != "note-index-preparing" {
                break response;
            }
            // timing-guard: retry only the explicit read-preparation response.
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(prepared["result"]["kind"], "noteSourcePage", "{prepared}");
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT count(*) FROM note_page_head WHERE indexed_rev=current_rev"
        )
        .fetch_one(&db)
        .await
        .unwrap(),
        1
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>(
            "SELECT indexed_rev FROM note_page_head WHERE note_id='unopened'"
        )
        .fetch_one(&db)
        .await
        .unwrap(),
        -1
    );
    let canonical: Vec<(String, i64)> = sqlx::query_as("SELECT content,rev FROM note ORDER BY id")
        .fetch_all(&db)
        .await
        .unwrap();
    assert_eq!(canonical, vec![("A😀\r\n旧".repeat(3000), 17); 2]);
    assert_eq!(
        call(&mut ready, "system.startupStatus", json!({})).await["result"]["phase"],
        "ready"
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
async fn failed_preparation_and_shutdown_preserve_backlog_and_reset_attempt() {
    let dir = common::test_tempdir_in("/tmp", "startup-rollback-");
    seed(dir.path()).await;
    let gate_path = dir.path().join("gate");
    let listener = UnixListener::bind(&gate_path).unwrap();
    let mut previous = Value::Null;
    for fail_preparation in [true, false] {
        let (mut child, port) = launch(dir.path(), &gate_path);
        let mut preparation = barrier(&listener, b'p').await;
        let cfg = startup_client_config(dir.path());
        let mut ws = connect_ws(port, cfg).await;
        let status = call(&mut ws, "system.startupStatus", json!({})).await;
        let attempt = status["result"]["attemptId"].clone();
        assert_ne!(attempt, previous);
        previous = attempt;
        assert!(status["result"].get("progress").is_none());
        if fail_preparation {
            preparation.write_u8(b'f').await.unwrap();
        } else {
            child.signal(nix::sys::signal::Signal::SIGTERM).unwrap();
        }
        assert_eq!(
            child
                .wait_with_timeout(Duration::from_secs(20))
                .unwrap()
                .unwrap()
                .success(),
            !fail_preparation
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
async fn queue_restore_failure_never_activates_operational_work() {
    let dir = common::test_tempdir_in("/tmp", "startup-queue-failure-");
    seed(dir.path()).await;
    let db = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new().filename(dir.path().join("intentd.db")),
        )
        .await
        .unwrap();
    sqlx::query("INSERT INTO agent_session(id,workspace_id,name,status,created_at,updated_at) VALUES ('review-agent','startup','review','idle','2026-10-13T00:00:00Z','2026-10-13T00:00:00Z')").execute(&db).await.unwrap();
    sqlx::query("INSERT INTO hook(hook_id,workspace_id,agent_id,name,code,delay_ms,state,created_at,expires_at) VALUES ('review-hook','startup','review-agent','review hook','return {dispatch:false}',1000,'scheduled','2000-01-01T00:00:00Z','2000-01-01T00:00:00Z')").execute(&db).await.unwrap();
    sqlx::query("ALTER TABLE agent_queue RENAME TO saved_agent_queue")
        .execute(&db)
        .await
        .unwrap();
    sqlx::query("INSERT INTO saved_agent_queue(id,agent_id,position,payload,created_at) VALUES ('retained','review-agent',0,'{}','now')").execute(&db).await.unwrap();
    db.close().await;
    let marker = dir.path().join("mcp-started");
    intent_core::FileSecretStore::with_path(dir.path().join("secrets.json"))
        .store(
            "mcp.servers",
            &json!({"review-mcp": {
                "id":"review-mcp", "name":"review MCP", "transport":"stdio",
                "command":"/bin/sh", "args":["-c", "touch \"$1\"", "fixture", marker],
                "enabled":true
            }})
            .to_string(),
        )
        .unwrap();
    let gate_path = dir.path().join("gate");
    let indexing = UnixListener::bind(&gate_path).unwrap();
    let (mut child, _) = launch(dir.path(), &gate_path);
    barrier(&indexing, b'p').await.write_u8(b'c').await.unwrap();
    assert!(!child
        .wait_with_timeout(Duration::from_secs(20))
        .unwrap()
        .unwrap()
        .success());
    let log = std::fs::read_to_string(dir.path().join("startup.log")).unwrap();
    for phase in ["cleanup", "agent_shutdown", "writer_drain", "store_close"] {
        assert!(
            log.contains(&format!("phase=\"{phase}\" state=\"completed\"")),
            "{log}"
        );
    }
    let db = database(dir.path()).await;
    let hook: (String, i64) =
        sqlx::query_as("SELECT state,run_count FROM hook WHERE hook_id='review-hook'")
            .fetch_one(&db)
            .await
            .unwrap();
    assert_eq!(
        hook,
        ("scheduled".into(), 0),
        "fatal startup must not consume a hook wake: {log}"
    );
    let agent: (String, i64) =
        sqlx::query_as("SELECT status,message_count FROM agent_session WHERE id='review-agent'")
            .fetch_one(&db)
            .await
            .unwrap();
    assert_eq!(
        agent,
        ("idle".into(), 0),
        "no wake/provider activation: {log}"
    );
    let queue: (String, String) = sqlx::query_as("SELECT id,payload FROM saved_agent_queue")
        .fetch_one(&db)
        .await
        .unwrap();
    assert_eq!(queue, ("retained".into(), "{}".into()));
    assert!(
        !marker.exists(),
        "MCP must not start after fatal prerequisites: {log}"
    );
    db.close().await;
    drop(Store::prepare_for_daemon(&dir.path().join("intentd.db")).unwrap());
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
    barrier(&indexing, b'p').await.write_u8(b'c').await.unwrap();
    let (mut service_gate, _) = timeout(common::daemon_startup_timeout(), services.accept())
        .await
        .unwrap()
        .unwrap();
    let socket = dir.path().join("intentd.sock");
    let status = uds_call(&socket, "system.startupStatus").await;
    assert_eq!(status["result"]["phase"], "finalizing");
    assert_eq!(status["result"]["finalizingStep"], "services");
    assert!(status["result"].get("progress").is_none());
    assert!(status["result"].get("estimatedRemainingMs").is_none());
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
        -1
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

/// Opt-in because the historical binary must be built from frozen b6fc9f881,
/// not rebuilt from this PR. The fixture drives its actual start/serve/update
/// entrypoints against the current daemon, without mocking its readiness code.
#[tokio::test]
#[ignore = "requires INTENTD_TEST_LEGACY_SITTER pointing to the frozen released sitter"]
async fn frozen_sitter_preserves_new_daemon_startup() {
    let seed_dir = common::test_tempdir_in("/tmp", "legacy-sitter-seed-");
    seed(seed_dir.path()).await;
    let sitter =
        std::env::var("INTENTD_TEST_LEGACY_SITTER").expect("frozen sitter binary required");
    let budget = std::env::var("INTENTD_TEST_LEGACY_BUDGET_MS").unwrap_or_else(|_| "2000".into());
    let millis: u64 = budget.parse().unwrap();
    let daemon = std::env::var("INTENTD_TEST_LEGACY_DAEMON").unwrap_or_else(|_| {
        intentd_test_support::runtime_paths::binary_path("intentd", env!("CARGO_BIN_EXE_intentd"))
            .to_string_lossy()
            .into_owned()
    });
    let mut command = std::process::Command::new("python3");
    command.arg(
        intentd_test_support::runtime_paths::manifest_dir(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/legacy-sitter-startup.py"),
    );
    command.args([
        &sitter,
        &daemon,
        intentd_sitter::manifest::TARGET_TRIPLE,
        &budget,
        seed_dir.path().join("intentd.db").to_str().unwrap(),
    ]);
    let mut child = GuardedChild::spawn(&mut command).unwrap();
    assert!(child
        .wait_with_timeout(Duration::from_millis(millis * 4) + Duration::from_secs(90))
        .unwrap()
        .unwrap()
        .success());
}

#[tokio::test]
async fn cli_startup_liveness_uses_server_identity_and_rejects_invalid_probes() {
    for (method, params, kind, success) in [
        ("system.status", "{}", "starting", true),
        ("system.status", "{}", "missing-version", false),
        ("system.status", "{}", "bad-envelope", false),
        ("system.status", "{}", "result-and-error", false),
        ("system.status", "{}", "missing-message", false),
        ("system.status", "{}", "failed", false),
        ("system.status", "{\"extra\":true}", "starting", false),
        ("system.status", "null", "starting", false),
        ("client.hello", "{}", "starting", false),
        ("note.add", "{}", "starting", false),
    ] {
        let dir = common::test_tempdir_in("/tmp", "cli-startup-identity-");
        let listener = UnixListener::bind(dir.path().join("intentd.sock")).unwrap();
        let status = intent_core::startup::StartupStatus::with_daemon_version("0.0.7");
        if kind == "failed" {
            status.fail();
        }
        let mut error = json!({"code":-32012,"message":"Daemon not ready","data":{
            "code":if kind == "failed" {"daemon-startup-failed"} else {"daemon-not-ready"},
            "retryable":kind != "failed","retryAfterMs":1000,"startup":status.snapshot()}});
        if kind == "missing-version" {
            error["data"]["startup"]
                .as_object_mut()
                .unwrap()
                .remove("daemonVersion");
        }
        let server = async {
            let (peer, _) = listener.accept().await.unwrap();
            let mut peer = BufReader::new(peer);
            let mut line = String::new();
            peer.read_line(&mut line).await.unwrap();
            let request: Value = serde_json::from_str(&line).unwrap();
            assert_eq!(request["method"], method);
            let mut response = json!({"jsonrpc":"2.0","id":request["id"],"error":error});
            match kind {
                "bad-envelope" => response["jsonrpc"] = json!("1.0"),
                "result-and-error" => response["result"] = json!({"version":"0.0.7"}),
                "missing-message" => {
                    response["error"].as_object_mut().unwrap().remove("message");
                }
                _ => {}
            }
            peer.get_mut()
                .write_all(format!("{response}\n").as_bytes())
                .await
                .unwrap();
        };
        let mut command =
            tokio::process::Command::new(intentd_test_support::runtime_paths::binary_path(
                "intentd",
                env!("CARGO_BIN_EXE_intentd"),
            ));
        command
            .args(["call", method, "--params", params])
            .env("INTENTD_DATA_DIR", dir.path())
            .kill_on_drop(true);
        let ((), output) = timeout(Duration::from_secs(5), async {
            tokio::join!(server, command.output())
        })
        .await
        .unwrap();
        let output = output.unwrap();
        assert_eq!(
            output.status.success(),
            success,
            "{method} {params} {kind}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        if success {
            let actual: Value = serde_json::from_slice(&output.stdout).unwrap();
            assert_ne!(env!("CARGO_PKG_VERSION"), "0.0.7");
            assert_eq!(
                actual["version"], "0.0.7",
                "never use this CLI's build version"
            );
            assert_eq!(actual["ready"], false);
        }
    }
}

#[tokio::test]
async fn sigusr2_during_preparation_is_retained_until_ready_and_stop_wins() {
    for advertised in [false, true] {
        for stop_early in [false, true] {
            let dir = common::test_tempdir_in("/tmp", "startup-usr2-");
            seed(dir.path()).await;
            let gate_path = dir.path().join("gate");
            let listener = UnixListener::bind(&gate_path).unwrap();
            let (mut child, _) = launch_with_handshake(dir.path(), &gate_path, advertised);
            let mut preparation = barrier(&listener, b'p').await;
            child.signal(nix::sys::signal::Signal::SIGUSR2).unwrap();
            let socket = dir.path().join("intentd.sock");
            assert_eq!(
                uds_call(&socket, "system.startupStatus").await["result"]["phase"],
                "preparing"
            );
            assert_eq!(
                uds_call(&socket, "system.status").await["error"]["code"],
                -32012
            );
            if stop_early {
                child.signal(nix::sys::signal::Signal::SIGTERM).unwrap();
            } else {
                preparation.write_u8(b'c').await.unwrap();
                if !advertised {
                    timeout(common::daemon_startup_timeout(), async {
                        while !common::daemon_is_ready(&socket).await {
                            // timing-guard: poll application readiness after the preparation barrier.
                            tokio::time::sleep(Duration::from_millis(25)).await;
                        }
                    })
                    .await
                    .expect("unsupervised signal must not stop startup");
                    child.signal(nix::sys::signal::Signal::SIGTERM).unwrap();
                }
            }
            let exit = child
                .wait_with_timeout(Duration::from_secs(20))
                .unwrap()
                .expect("bounded exit");
            assert_eq!(
                exit.code(),
                Some(if advertised && !stop_early { 75 } else { 0 })
            );
            assert!(!socket.exists());
            let db = database(dir.path()).await;
            let indexed: i64 = sqlx::query_scalar("SELECT indexed_rev FROM note_page_head")
                .fetch_one(&db)
                .await
                .unwrap();
            assert_eq!(indexed, -1);
            db.close().await;
            if !stop_early {
                let log = std::fs::read_to_string(dir.path().join("startup.log")).unwrap();
                assert!(
                    log.contains("phase=\"store_close\" state=\"completed\""),
                    "{log}"
                );
            }
        }
    }
}
