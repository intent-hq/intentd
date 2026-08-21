// WSS e2e test for RTK detection + prompt injection.
//
// Verifies that when `rtk.enabled` is true and a fake `rtk` shim is on PATH,
// the assembled system prompt includes the RTK instruction line with the
// filtered subcommand list. Also tests the negative path: with flag off or
// rtk missing, the prompt must not contain the RTK line (regression guarantee).

#![cfg(unix)]

mod common;

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::net::{TcpStream, UnixStream};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;
use uuid::Uuid;

const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";

struct Daemon {
    child: Child,
    data_dir: PathBuf,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let log_path = self.data_dir.join("daemon.log");
        if let Ok(log) = std::fs::read_to_string(&log_path) {
            if !log.is_empty() {
                eprintln!("=== DAEMON LOG ===\n{log}\n=== END LOG ===");
            }
        }
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-rtk-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

fn spawn_serve(data_dir: &Path, listen: &str, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    if listen != "uds" {
        common::enable_ws_api(data_dir);
    }
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    for (k, v) in env {
        cmd.env(k, v);
    }
    cmd.spawn().expect("spawn intentd serve")
}

async fn await_uds(socket: &Path) -> bool {
    timeout(common::daemon_startup_timeout(), async {
        loop {
            if UnixStream::connect(socket).await.is_ok() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .is_ok()
}

#[derive(Debug)]
struct PinnedVerifier {
    fingerprint: String,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for PinnedVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let fp = Sha256::digest(end_entity.as_ref())
            .iter()
            .map(|b| format!("{b:02X}"))
            .collect::<Vec<_>>()
            .join(":");
        if fp == self.fingerprint {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General("fingerprint mismatch".into()))
        }
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

fn client_config(fingerprint: &str) -> Arc<ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .unwrap()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedVerifier {
            fingerprint: fingerprint.to_string(),
            provider,
        }))
        .with_no_client_auth();
    Arc::new(config)
}

async fn wss_connect(
    port: u16,
    cfg: Arc<ClientConfig>,
) -> WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>> {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

async fn wss_rpc(
    ws: &mut WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>,
    id: i64,
    method: &str,
    params: Value,
) -> Value {
    let req = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(req.to_string().into()))
        .await
        .expect("send");

    // Loop until we receive the response matching our request id, skipping Ping/Pong
    loop {
        let msg = timeout(Duration::from_secs(10), ws.next())
            .await
            .expect("ws rpc timeout")
            .expect("ws closed")
            .expect("ws error");
        match msg {
            Message::Text(text) => {
                let v: Value = serde_json::from_str(&text).expect("invalid json");
                if v["id"] == id {
                    return v;
                }
                // Skip responses for other requests
            }
            Message::Ping(_) | Message::Pong(_) => {
                // Skip ping/pong frames
            }
            _ => {
                // Skip other frame types
            }
        }
    }
}

async fn wss_event(
    ws: &mut WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>,
    secs: u64,
) -> Value {
    loop {
        let next = timeout(Duration::from_secs(secs), ws.next())
            .await
            .expect("wss event timed out");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] == "events.event" {
                    return v;
                }
                // Skip non-event messages (e.g., RPC responses)
            }
            Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => {
                // Skip ping/pong frames
            }
            Some(Ok(Message::Close(_))) => {
                panic!("websocket closed while waiting for event");
            }
            Some(Err(e)) => panic!("websocket error: {e}"),
            None => panic!("websocket stream ended"),
            _ => {
                // Skip other frame types
            }
        }
    }
}

fn gate(name: &str) -> Option<String> {
    let key = "MOCK_AGENT_SCRIPT_PATH";
    match std::env::var(key) {
        Ok(path) if !path.is_empty() => Some(path),
        _ => {
            eprintln!("skipping {name}: {key} not set");
            None
        }
    }
}

#[tokio::test]
async fn rtk_settings_integration() {
    //Test that rtk.enabled setting round-trips correctly over WSS
    let data_dir = temp_data_dir();
    let socket = data_dir.join("intentd.sock");

    let mut _daemon = Daemon {
        child: spawn_serve(
            &data_dir,
            "both",
            &[("INTENTD_AUTH_TOKEN", TOKEN), ("INTENTD_TCP_PORT", "0")],
        ),
        data_dir: data_dir.clone(),
    };

    assert!(await_uds(&socket).await, "daemon did not start");

    // Discover port + fingerprint
    let status_resp = common::await_wss_status(&socket).await;
    let fp = status_resp["result"]["fingerprint"]
        .as_str()
        .expect("no fingerprint");
    let bound_port = u16::try_from(status_resp["result"]["port"].as_u64().expect("no port"))
        .expect("value fits in u16");
    assert_ne!(bound_port, 0, "bound port should be non-zero");

    let cfg = client_config(fp);
    let mut ws = wss_connect(bound_port, cfg).await;

    // 1. Verify rtk.enabled defaults to false
    let get_resp = wss_rpc(
        &mut ws,
        10,
        "settings.get",
        json!({ "path": "rtk.enabled" }),
    )
    .await;
    assert_eq!(
        get_resp["result"]["value"],
        json!(false),
        "rtk.enabled should default to false"
    );

    // 2. Update rtk.enabled to true
    let update_resp = wss_rpc(
        &mut ws,
        20,
        "settings.update",
        json!({ "changes": [{ "path": "rtk.enabled", "value": true }] }),
    )
    .await;
    assert!(update_resp["result"]["applied"].is_array());
    assert_eq!(update_resp["result"]["applied"][0]["path"], "rtk.enabled");
    assert_eq!(update_resp["result"]["applied"][0]["value"], json!(true));

    // 3. Read back rtk.enabled to verify it was persisted
    let get_resp2 = wss_rpc(
        &mut ws,
        30,
        "settings.get",
        json!({ "path": "rtk.enabled" }),
    )
    .await;
    assert_eq!(
        get_resp2["result"]["value"],
        json!(true),
        "rtk.enabled should now be true"
    );

    // 4. Reset rtk.enabled back to default
    let reset_resp = wss_rpc(
        &mut ws,
        40,
        "settings.reset",
        json!({ "path": "rtk.enabled" }),
    )
    .await;
    assert_eq!(
        reset_resp["result"]["value"],
        json!(false),
        "reset should restore default false"
    );
}

/// WSS e2e: RTK prompt injection over the real wire transport.
///
/// Drives the full orchestration: `workspace.create` with `initialAgent.prompt`
/// (the daemon-owned initial-agent flow from PROTOCOL §5.1) starts a turn, which
/// triggers `assemble_system_prompt` → systemPrompt persistence. Then `agent.getSession`
/// returns the assembled prompt with (or without) the RTK layer depending on the flag.
///
/// Positive path: rtk.enabled=true, fake-rtk.sh on PATH → systemPrompt CONTAINS
/// "Prefix these commands with rtk for compressed, LLM-friendly output: ls, cat, grep"
/// (the filtered subcommand list from the fake shim, excluding `test` and `help`).
///
/// Negative path: rtk.enabled=false → systemPrompt DOES NOT contain "Prefix these commands with rtk".
#[tokio::test]
async fn rtk_prompt_injection_over_wss() {
    let Some(script) = gate("WSS RTK prompt injection E2E") else {
        return;
    };

    let data_dir = temp_data_dir();
    let socket = data_dir.join("intentd.sock");

    // Fake rtk shim (outputs `ls, cat, grep, test, help`; test + help excluded)
    // Copy to a temp directory and name it `rtk` so `which rtk` finds it
    let fake_rtk_src = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/fake-rtk.sh");
    assert!(fake_rtk_src.exists(), "fake-rtk.sh fixture missing");

    let bin_dir = data_dir.join("bin");
    std::fs::create_dir_all(&bin_dir).expect("mkdir bin");
    let fake_rtk_dest = bin_dir.join("rtk");
    std::fs::copy(&fake_rtk_src, &fake_rtk_dest).expect("copy fake-rtk.sh to bin/rtk");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(&fake_rtk_dest).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&fake_rtk_dest, perms).unwrap();
    }

    // Build PATH with the bin dir first so `which rtk` finds our shim
    let original_path = std::env::var("PATH").unwrap_or_default();
    let augmented_path = format!("{}:{}", bin_dir.display(), original_path);

    // Mock ACP behavior for deterministic test
    let behavior = json!({ "response": "build complete" }).to_string();

    let mut _daemon = Daemon {
        child: spawn_serve(
            &data_dir,
            "both",
            &[
                ("INTENTD_AUTH_TOKEN", TOKEN),
                ("INTENTD_TCP_PORT", "0"),
                ("MOCK_AGENT_SCRIPT_PATH", &script),
                ("MOCK_AGENT_BEHAVIOR", &behavior),
                ("PATH", &augmented_path),
            ],
        ),
        data_dir: data_dir.clone(),
    };

    assert!(await_uds(&socket).await, "daemon did not start");

    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);

    // ===== POSITIVE PATH: rtk.enabled = true =====
    // Enable RTK before creating workspace
    let mut settings_ws = wss_connect(port, cfg.clone()).await;
    let _ = wss_rpc(
        &mut settings_ws,
        10,
        "settings.update",
        json!({ "changes": [{ "path": "rtk.enabled", "value": true }] }),
    )
    .await;

    // SUBSCRIBER conn — subscribe to events before creating workspace
    let mut sub = wss_connect(port, cfg.clone()).await;
    let sub_resp = wss_rpc(
        &mut sub,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["workspace:*", "agent:*"] }),
    )
    .await;
    assert!(
        sub_resp["result"]["subscriptionId"].is_string(),
        "subscribed: {sub_resp}"
    );

    // RPC conn — workspace.create with initialAgent to trigger activation
    // (agent id is server-assigned and read back from the result)
    let mut rpc = wss_connect(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut rpc,
        20,
        "workspace.create",
        json!({
            "title": "RTK enabled WS",
            "branch": "feat/rtk-enabled-e2e",
            "idempotencyKey": "rtk-test-1",
            "initialAgent": {
                "prompt": "run the build",
                "name": "Test Agent (RTK on)",
                "model": "mock:default",
            },
        }),
    )
    .await;
    let _ws_id_1 = created["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id");
    let agent_id_1 = created["result"]["initialAgent"]["id"]
        .as_str()
        .expect("initial agent id")
        .to_string();

    // Wait for agent turn to complete by consuming events
    let mut saw_stream_end = false;
    for _ in 0..120 {
        let frame = wss_event(&mut sub, 30).await;
        let ev = &frame["params"]["event"];
        if ev["type"] == "agent:stream:end" {
            saw_stream_end = true;
            break;
        }
    }
    assert!(saw_stream_end, "agent:stream:end event observed");

    // Now get the session to verify systemPrompt was persisted
    let session_1 = wss_rpc(
        &mut rpc,
        30,
        "agent.getSession",
        json!({ "agentId": agent_id_1 }),
    )
    .await;
    let system_prompt_1 = session_1["result"]["session"]["systemPrompt"]
        .as_str()
        .expect("systemPrompt should be populated after turn completes");

    assert!(
        system_prompt_1
            .contains("Prefix these commands with rtk for compressed, LLM-friendly output:"),
        "systemPrompt (enabled) must contain the RTK header"
    );

    // Extract just the RTK line for clearer error messages
    let rtk_line = system_prompt_1
        .lines()
        .find(|line| line.starts_with("Prefix these commands with rtk"))
        .expect("RTK line should be present when enabled");

    assert!(
        rtk_line.contains("ls") && rtk_line.contains("cat") && rtk_line.contains("grep"),
        "RTK line must include filtered subcommands, got: {rtk_line}"
    );
    assert!(
        !rtk_line.contains(" test")
            && !rtk_line.contains("test,")
            && !rtk_line.contains(" help")
            && !rtk_line.contains("help,"),
        "RTK line must NOT include excluded commands (test, help), got: {rtk_line}"
    );

    // ===== NEGATIVE PATH: rtk.enabled = false =====
    // Disable RTK
    let _ = wss_rpc(
        &mut settings_ws,
        40,
        "settings.update",
        json!({ "changes": [{ "path": "rtk.enabled", "value": false }] }),
    )
    .await;

    // Create a second workspace + agent (flag off)
    let created_2 = wss_rpc(
        &mut rpc,
        50,
        "workspace.create",
        json!({
            "title": "RTK disabled WS",
            "branch": "feat/rtk-disabled-e2e",
            "idempotencyKey": "rtk-test-2",
            "initialAgent": {
                "prompt": "run the build",
                "name": "Test Agent (RTK off)",
                "model": "mock:default",
            },
        }),
    )
    .await;
    let _ws_id_2 = created_2["result"]["workspace"]["id"]
        .as_str()
        .expect("workspace id");
    let agent_id_2 = created_2["result"]["initialAgent"]["id"]
        .as_str()
        .expect("initial agent id")
        .to_string();

    // Wait for the second agent turn to complete
    let mut saw_stream_end_2 = false;
    for _ in 0..120 {
        let frame = wss_event(&mut sub, 30).await;
        let ev = &frame["params"]["event"];
        if ev["type"] == "agent:stream:end" && ev["data"]["agentId"] == agent_id_2 {
            saw_stream_end_2 = true;
            break;
        }
    }
    assert!(
        saw_stream_end_2,
        "agent:stream:end event observed for second agent"
    );

    // Get the session to verify systemPrompt was persisted
    let session_2 = wss_rpc(
        &mut rpc,
        60,
        "agent.getSession",
        json!({ "agentId": agent_id_2 }),
    )
    .await;
    let system_prompt_2 = session_2["result"]["session"]["systemPrompt"]
        .as_str()
        .expect("systemPrompt should be populated after turn completes");

    assert!(
        !system_prompt_2.contains("Prefix these commands with rtk"),
        "systemPrompt (disabled) must NOT contain the RTK instruction line"
    );
}
