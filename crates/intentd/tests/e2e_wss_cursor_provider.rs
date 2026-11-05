//! WSS e2e for the Cursor provider against a mock `cursor-agent`
//! (`fixtures/mock-cursor-agent.mjs`, built from captured cursor-agent
//! 2026.11.05-86047df traffic), wired through `providers.paths.cursor` so the
//! real registry entry, discovery, auth probe, model source, handshake,
//! client-request handler, tool mapping and teardown all run.
//!
//! Gated on `node`; skips cleanly otherwise.

#![cfg(unix)]

use crate::common;

use intentd_test_support::GuardedChild;
use std::path::Path;
use std::process::Stdio;
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

const TOKEN: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

struct Daemon {
    child: GuardedChild,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill_tree();
    }
}

/// Kills every mock `cursor-agent acp` process group (and its worker) the
/// test's daemons started. Declared before the daemon so it drops after it:
/// a SIGKILLed test daemon cannot reap agents spawned in their own process
/// groups, and the mock (like the real CLI) ignores stdin EOF.
struct MockCleanup(std::path::PathBuf);

impl Drop for MockCleanup {
    fn drop(&mut self) {
        use nix::sys::signal::{kill, killpg, Signal};
        use nix::unistd::Pid;
        let Ok(log) = std::fs::read_to_string(&self.0) else {
            return;
        };
        for pid in log.split_whitespace().filter_map(|p| p.parse::<i32>().ok()) {
            let _ = killpg(Pid::from_raw(pid), Signal::SIGKILL);
            let _ = kill(Pid::from_raw(pid), Signal::SIGKILL);
        }
    }
}

fn spawn_serve(data_dir: &Path, env: &[(&str, &str)]) -> GuardedChild {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    common::enable_ws_api(data_dir);
    let mut cmd = common::hermetic_serve_command(data_dir);
    cmd.env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .env("MOCK_CURSOR_PIDS_LOG", data_dir.join("mock-pids.log"))
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    for (k, v) in env {
        cmd.env(k, v);
    }
    common::hermetic_fixture_identity(&mut cmd, data_dir);
    GuardedChild::spawn(&mut cmd).expect("spawn intentd serve")
}

async fn await_uds(socket: &Path) -> bool {
    timeout(common::daemon_startup_timeout(), async {
        loop {
            if UnixStream::connect(socket).await.is_ok() {
                return;
            }
            // timing-guard: poll interval while the daemon socket comes up (bounded by the timeout).
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

type Ws = WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>>;

async fn connect_ws(port: u16, cfg: Arc<ClientConfig>) -> Ws {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

async fn wss_rpc(ws: &mut Ws, id: i64, method: &str, params: Value) -> Value {
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send rpc frame");
    loop {
        let next = timeout(
            intentd_test_support::test_timeout(Duration::from_secs(20)),
            ws.next(),
        )
        .await
        .expect("wss rpc timed out");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["id"] == json!(id) {
                    assert!(v.get("error").is_none(), "rpc {method} errored: {v}");
                    return v["result"].clone();
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => {}
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

fn row<'a>(rows: &'a Value, id: &str) -> &'a Value {
    rows.as_array()
        .expect("rows array")
        .iter()
        .find(|r| r["id"] == id)
        .unwrap_or_else(|| panic!("no {id} row in {rows}"))
}

fn rpc_log(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(|l| serde_json::from_str(l).expect("rpc log line"))
        .collect()
}

/// Whether `pid` still runs. A killed process that is not yet reaped is a
/// zombie (`Z` in `/proc/<pid>/stat` on Linux) and counts as gone.
fn pid_alive(pid: i32) -> bool {
    if nix::sys::signal::kill(nix::unistd::Pid::from_raw(pid), None).is_err() {
        return false;
    }
    std::fs::read_to_string(format!("/proc/{pid}/stat")).map_or(true, |stat| {
        stat.rsplit_once(") ")
            .is_none_or(|(_, rest)| !rest.starts_with('Z'))
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cursor_provider_discovery_auth_models_and_turn_over_wss() {
    use std::os::unix::fs::PermissionsExt;
    let Some(node) = intent_providers::resolve_on_path("node") else {
        eprintln!("skipping Cursor WSS E2E: node not on PATH");
        return;
    };
    let fixture = format!(
        "{}/tests/fixtures/mock-cursor-agent.mjs",
        env!("CARGO_MANIFEST_DIR")
    );
    let data_dir_guard = common::test_tempdir_in("/tmp", "itd-wss-cursor-");
    let data_dir = data_dir_guard.path().to_path_buf();
    let _mock_cleanup = MockCleanup(data_dir.join("mock-pids.log"));
    let wrapper = data_dir.join("cursor-agent");
    std::fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\nexec '{}' '{}' \"$@\"\n",
            node.display(),
            fixture
        ),
    )
    .unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::write(
        data_dir.join("config.toml"),
        format!("[providers.paths]\ncursor = \"{}\"\n", wrapper.display()),
    )
    .unwrap();
    let log = data_dir.join("cursor-rpc.jsonl");
    let pid_file = data_dir.join("cursor-acp.pid");
    let _daemon = Daemon {
        child: spawn_serve(
            &data_dir,
            &[
                ("MOCK_CURSOR_RPC_LOG", log.to_str().unwrap()),
                ("MOCK_CURSOR_PID_FILE", pid_file.to_str().unwrap()),
                // Mixed credentials: a stored login and an API key.
                ("CURSOR_API_KEY", "crsr-e2e-placeholder"),
                // Colored CLI output must not break the probes.
                ("FORCE_COLOR", "1"),
            ],
        ),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
    let mut rpc = connect_ws(port, cfg.clone()).await;

    // Catalog row: the FE contract for the Labs-gated Cursor entry.
    let catalog = wss_rpc(&mut rpc, 1, "providers.catalog", json!({})).await;
    let cursor = row(&catalog["providers"], "cursor");
    assert_eq!(cursor["displayName"], "Cursor");
    assert_eq!(cursor["shortName"], "Cursor");
    assert_eq!(cursor["command"], "cursor-agent");
    assert_eq!(cursor["canBeDisabled"], true);
    assert_eq!(cursor["supportsTestPrompt"], true);
    assert_eq!(cursor["visible"], true);
    assert_eq!(cursor["loginCommandHint"], "cursor-agent login");

    // Discovery honours the override and reports the CLI version.
    let discovery = wss_rpc(&mut rpc, 2, "host.providerDiscovery", json!({})).await;
    let cursor = row(&discovery["providers"], "cursor");
    assert_eq!(cursor["installed"], true, "{cursor}");
    assert_eq!(cursor["cliCommand"], "cursor-agent", "{cursor}");
    assert_eq!(cursor["cliVersion"], "2026.11.05-86047df", "{cursor}");
    assert!(cursor.get("unavailableReason").is_none(), "{cursor}");

    // Auth comes from the parsed `cursor-agent models` run.
    let auth = wss_rpc(
        &mut rpc,
        3,
        "host.providerAuthStatus",
        json!({ "providerId": "cursor", "force": true }),
    )
    .await;
    assert_eq!(auth["providers"][0]["id"], "cursor", "{auth}");
    assert_eq!(auth["providers"][0]["authenticated"], true, "{auth}");
    assert!(
        auth["providers"][0]["statusNote"]
            .as_str()
            .is_some_and(|n| n.contains("Using the stored Cursor login")),
        "mixed credentials name the stored login: {auth}"
    );

    let models = wss_rpc(
        &mut rpc,
        4,
        "models.list",
        json!({ "providerId": "cursor", "forceRefresh": true }),
    )
    .await;
    assert_eq!(models["source"], "cursor", "{models}");
    let rows = models["models"].as_array().unwrap();
    assert_eq!(rows.len(), 4, "{models}");
    assert_eq!(
        rows[0],
        json!({ "id": "default", "name": "Auto", "provider": "cursor", "isDefault": true })
    );
    assert_eq!(rows[1]["id"], "gpt-5.2", "{models}");
    assert_eq!(rows[2]["name"], "Grok 4.7 Low Fast", "{models}");

    // Every `models` probe ran with plain-output env despite FORCE_COLOR=1.
    let cli_models: Vec<Value> = rpc_log(&log)
        .into_iter()
        .filter(|f| f["dir"] == "cli" && f["msg"]["args"][0] == "models")
        .collect();
    assert!(!cli_models.is_empty());
    assert!(
        cli_models
            .iter()
            .all(|f| f["msg"]["FORCE_COLOR"] == "0" && f["msg"]["NO_COLOR"] == "1"),
        "{cli_models:?}"
    );

    // No ACP session was opened by discovery, auth or the model list.
    assert!(
        rpc_log(&log)
            .iter()
            .all(|f| f["msg"]["method"] != "session/new" && f["msg"]["method"] != "session/load"),
        "probes must not start an ACP session (it would appear in Cursor's history)"
    );

    // A full turn.
    let workspace = wss_rpc(
        &mut rpc,
        5,
        "workspace.create",
        json!({ "title": "Cursor E2E", "noPrompt": true }),
    )
    .await;
    let workspace_id = workspace["workspace"]["id"].as_str().unwrap().to_string();
    let mut events = connect_ws(port, cfg).await;
    wss_rpc(
        &mut events,
        1,
        "events.subscribe",
        json!({ "workspaceId": workspace_id, "eventTypes": ["agent:*"] }),
    )
    .await;
    let created = wss_rpc(
        &mut rpc,
        6,
        "agent.create",
        json!({ "workspaceId": workspace_id, "name": "Cursor fixture", "provider": "cursor", "model": "gpt-5.2" }),
    )
    .await;
    let agent_id = created["agent"]["id"].as_str().unwrap().to_string();
    wss_rpc(
        &mut rpc,
        7,
        "agent.sendMessage",
        json!({ "workspaceId": workspace_id, "agentId": agent_id, "content": "check git status" }),
    )
    .await;
    let mut tools: Vec<Value> = Vec::new();
    timeout(
        intentd_test_support::test_timeout(Duration::from_secs(60)),
        async {
            loop {
                let Some(Ok(Message::Text(text))) = events.next().await else {
                    continue;
                };
                let frame: Value = serde_json::from_str(&text).unwrap();
                let event = &frame["params"]["event"];
                if event["data"]["agentId"] != agent_id.as_str() {
                    continue;
                }
                match event["type"].as_str() {
                    Some("agent:tool:call") => tools.push(event["data"].clone()),
                    Some("agent:failed") => panic!("cursor turn failed: {event}"),
                    Some("agent:idle") => break,
                    _ => {}
                }
            }
        },
    )
    .await
    .expect("Cursor turn must finish");

    // Timeline mapping: shell → bash/terminal (newline ids kept opaque), the
    // workspace bridge MCP call → workspace_api.
    let shell_id =
        "call-d5f81713-ef9b-4563-81e1-9fccb289dc3b-0\nfc_4488f13b-7cc1-9ba7-9fe8-679b98c595e1_0";
    let shell: Vec<_> = tools
        .iter()
        .filter(|t| t["toolCallId"] == shell_id)
        .collect();
    assert!(!shell.is_empty(), "{tools:?}");
    assert!(
        shell
            .iter()
            .all(|t| t["toolName"] == "bash" && t["toolKind"] == "terminal"),
        "{shell:?}"
    );
    assert!(
        tools.iter().any(|t| t["toolName"] == "workspace_api"),
        "the workspace bridge call maps to workspace_api: {tools:?}"
    );

    let frames = rpc_log(&log);
    let acp: Vec<&Value> = frames.iter().filter(|f| f["dir"] != "cli").collect();
    let sent_by_daemon = |method: &str| -> Vec<&Value> {
        acp.iter()
            .filter(|f| f["dir"] == "in" && f["msg"]["method"] == method)
            .map(|f| &f["msg"])
            .collect()
    };
    // Handshake: no subagents, no parameterized model picker, never
    // `authenticate` (a browser login on a signed-out host).
    // Every initialize (the session-free model probe and the agent session).
    let init = sent_by_daemon("initialize");
    assert!(!init.is_empty(), "{acp:?}");
    for frame in &init {
        let caps = frame["params"]["clientCapabilities"].to_string();
        assert!(!caps.contains("subagents"), "{caps}");
        assert!(!caps.contains("parameterizedModelPicker"), "{caps}");
    }
    assert!(sent_by_daemon("authenticate").is_empty());
    // The workspace bridge rides session/new mcpServers as a stdio server.
    let new = sent_by_daemon("session/new");
    assert_eq!(new.len(), 1, "{acp:?}");
    let servers = new[0]["params"]["mcpServers"].as_array().unwrap();
    assert!(
        servers.iter().any(|s| s["command"].is_string()
            && s["args"]
                .as_array()
                .is_some_and(|a| a.iter().any(|x| x == "mcp-bridge"))),
        "stdio workspace bridge missing: {servers:?}"
    );
    // The picked base model is applied as this session's variant.
    let set = sent_by_daemon("session/set_config_option");
    assert!(
        set.iter().any(|f| f["params"]["configId"] == "model"
            && f["params"]["value"] == "gpt-5.2[reasoning=medium,fast=false]"),
        "{set:?}"
    );
    // The system prompt is prepended on the first fresh turn.
    let prompt = sent_by_daemon("session/prompt");
    assert_eq!(prompt.len(), 1);
    assert!(
        prompt[0]["params"]["prompt"]
            .to_string()
            .contains("<system>"),
        "first-turn prepend missing"
    );
    assert!(
        prompt[0]["params"]["prompt"]
            .to_string()
            .contains("Do not use Cursor's native Task (subagent) tool"),
        "the prompt-only subagent restriction must reach Cursor"
    );
    // Responses to Cursor's own requests: allow-once (never allow-always,
    // which writes the user's Cursor allowlist) and a skipped question.
    let response_to = |method: &str| -> Value {
        let id = acp
            .iter()
            .find(|f| f["dir"] == "out" && f["msg"]["method"] == method)
            .unwrap_or_else(|| panic!("mock never sent {method}"))["msg"]["id"]
            .clone();
        acp.iter()
            .find(|f| f["dir"] == "in" && f["msg"]["id"] == id && f["msg"].get("method").is_none())
            .unwrap_or_else(|| panic!("no response to {method}"))["msg"]
            .clone()
    };
    assert_eq!(
        response_to("session/request_permission")["result"]["outcome"],
        json!({ "outcome": "selected", "optionId": "allow-once" })
    );
    assert_eq!(
        response_to("cursor/ask_question")["result"]["outcome"]["outcome"],
        "skipped"
    );
    assert_eq!(response_to("cursor/task")["result"], json!({}));
    assert!(
        tools.iter().any(|t| t["toolName"] == "Subagent"),
        "the native Task call maps to Subagent: {tools:?}"
    );

    // `cursor-agent acp` ignores stdin EOF, and its worker survives a SIGTERM
    // to the main pid alone: deleting the agent must kill the whole group.
    let pids: Vec<i32> = std::fs::read_to_string(&pid_file)
        .unwrap()
        .split_whitespace()
        .map(|p| p.parse().unwrap())
        .collect();
    assert_eq!(pids.len(), 2, "main and worker pids");
    assert!(
        pids.iter().all(|p| pid_alive(*p)),
        "mock acp + worker should be running"
    );
    let deleted = wss_rpc(
        &mut rpc,
        8,
        "agent.delete",
        json!({ "workspaceId": workspace_id, "agentId": agent_id }),
    )
    .await;
    assert_eq!(deleted["success"], true, "{deleted}");
    timeout(
        intentd_test_support::test_timeout(Duration::from_secs(20)),
        async {
            while pids.iter().any(|p| pid_alive(*p)) {
                // timing-guard: poll interval for the killed processes to disappear (bounded by the timeout).
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        },
    )
    .await
    .expect("the cursor-agent acp process and its worker must be killed on teardown");
}

/// A signed-out Cursor CLI reports `authenticated: false` and no models.
#[tokio::test]
async fn cursor_signed_out_reports_unauthenticated_over_wss() {
    use std::os::unix::fs::PermissionsExt;
    let Some(node) = intent_providers::resolve_on_path("node") else {
        eprintln!("skipping Cursor WSS E2E: node not on PATH");
        return;
    };
    let fixture = format!(
        "{}/tests/fixtures/mock-cursor-agent.mjs",
        env!("CARGO_MANIFEST_DIR")
    );
    let data_dir_guard = common::test_tempdir_in("/tmp", "itd-wss-cursor-out-");
    let data_dir = data_dir_guard.path().to_path_buf();
    let _mock_cleanup = MockCleanup(data_dir.join("mock-pids.log"));
    let wrapper = data_dir.join("cursor-agent");
    std::fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\nexec '{}' '{}' \"$@\"\n",
            node.display(),
            fixture
        ),
    )
    .unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::write(
        data_dir.join("config.toml"),
        format!("[providers.paths]\ncursor = \"{}\"\n", wrapper.display()),
    )
    .unwrap();
    let _daemon = Daemon {
        child: spawn_serve(&data_dir, &[("MOCK_CURSOR_SIGNED_OUT", "1")]),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let mut rpc = connect_ws(
        port,
        client_config(status["result"]["fingerprint"].as_str().unwrap()),
    )
    .await;
    let auth = wss_rpc(
        &mut rpc,
        1,
        "host.providerAuthStatus",
        json!({ "providerId": "cursor", "force": true }),
    )
    .await;
    assert_eq!(
        auth["providers"],
        json!([{ "id": "cursor", "authenticated": false }])
    );
    let models = wss_rpc(
        &mut rpc,
        2,
        "models.list",
        json!({ "providerId": "cursor", "forceRefresh": true }),
    )
    .await;
    assert!(
        models["warning"]
            .as_str()
            .is_some_and(|w| w.contains("authentication required")),
        "{models}"
    );
}

/// A model the session does not offer, or one Cursor rejects, fails the
/// turn with an actionable error and never reaches `session/prompt`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cursor_unknown_or_rejected_model_fails_with_an_actionable_error() {
    use std::os::unix::fs::PermissionsExt;
    let Some(node) = intent_providers::resolve_on_path("node") else {
        eprintln!("skipping Cursor WSS E2E: node not on PATH");
        return;
    };
    let fixture = format!(
        "{}/tests/fixtures/mock-cursor-agent.mjs",
        env!("CARGO_MANIFEST_DIR")
    );
    let data_dir_guard = common::test_tempdir_in("/tmp", "itd-wss-cursor-model-");
    let data_dir = data_dir_guard.path().to_path_buf();
    let _mock_cleanup = MockCleanup(data_dir.join("mock-pids.log"));
    let wrapper = data_dir.join("cursor-agent");
    std::fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\nexec '{}' '{}' \"$@\"\n",
            node.display(),
            fixture
        ),
    )
    .unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::write(
        data_dir.join("config.toml"),
        format!("[providers.paths]\ncursor = \"{}\"\n", wrapper.display()),
    )
    .unwrap();
    let log = data_dir.join("cursor-rpc.jsonl");
    let _daemon = Daemon {
        child: spawn_serve(
            &data_dir,
            &[
                ("MOCK_CURSOR_RPC_LOG", log.to_str().unwrap()),
                (
                    "MOCK_CURSOR_REJECT_MODELS",
                    "grok-4.7[context=256k,reasoning_effort=high,fast=true]",
                ),
            ],
        ),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let workspace = wss_rpc(
        &mut rpc,
        1,
        "workspace.create",
        json!({ "title": "Cursor model errors", "noPrompt": true }),
    )
    .await;
    let workspace_id = workspace["workspace"]["id"].as_str().unwrap().to_string();
    let mut events = connect_ws(port, cfg).await;
    wss_rpc(
        &mut events,
        1,
        "events.subscribe",
        json!({ "workspaceId": workspace_id, "eventTypes": ["agent:*"] }),
    )
    .await;
    for (id, model, expected) in [
        (10, "grok-4.7", "Cursor rejected model 'grok-4.7'"),
        (20, "gpt-9", "Cursor does not offer model 'gpt-9'"),
    ] {
        let created = wss_rpc(
            &mut rpc,
            id,
            "agent.create",
            json!({ "workspaceId": workspace_id, "name": format!("Cursor {model}"), "provider": "cursor", "model": model }),
        )
        .await;
        let agent_id = created["agent"]["id"].as_str().unwrap().to_string();
        let sent = timeout(
            intentd_test_support::test_timeout(Duration::from_secs(15)),
            async {
                // The setup failure may surface as the RPC error itself.
                let frame = json!({ "jsonrpc": "2.0", "id": id + 1, "method": "agent.sendMessage",
                    "params": { "workspaceId": workspace_id, "agentId": agent_id, "content": "hi" } });
                rpc.send(Message::Text(frame.to_string().into())).await.unwrap();
                loop {
                    if let Some(Ok(Message::Text(text))) = rpc.next().await {
                        let v: Value = serde_json::from_str(&text).unwrap();
                        if v["id"] == json!(id + 1) {
                            return v;
                        }
                    }
                }
            },
        )
        .await
        .expect("sendMessage answers");
        let failure = if sent.get("error").is_some() {
            sent.to_string()
        } else {
            timeout(
                intentd_test_support::test_timeout(Duration::from_secs(40)),
                async {
                    loop {
                        let Some(Ok(Message::Text(text))) = events.next().await else {
                            continue;
                        };
                        let frame: Value = serde_json::from_str(&text).unwrap();
                        let event = &frame["params"]["event"];
                        if event["data"]["agentId"] != agent_id.as_str() {
                            continue;
                        }
                        match event["type"].as_str() {
                            Some("agent:failed") => return event.to_string(),
                            Some("agent:idle") => panic!("{model}: turn must not run: {event}"),
                            _ => {}
                        }
                    }
                },
            )
            .await
            .expect("the model failure must surface")
        };
        assert!(failure.contains(expected), "{model}: {failure}");
        assert!(
            failure.contains("Refresh the Cursor model list"),
            "{failure}"
        );
    }
    assert!(
        rpc_log(&log)
            .iter()
            .all(|f| f["msg"]["method"] != "session/prompt"),
        "a failed model setup never reaches session/prompt"
    );
}

/// Read agent events until `agent:stream:end` for `agent_id`, returning them.
async fn turn_events(events: &mut Ws, agent_id: &str) -> Vec<Value> {
    let mut seen = Vec::new();
    timeout(
        intentd_test_support::test_timeout(Duration::from_secs(60)),
        async {
            loop {
                let Some(Ok(Message::Text(text))) = events.next().await else {
                    continue;
                };
                let frame: Value = serde_json::from_str(&text).unwrap();
                let event = frame["params"]["event"].clone();
                if event["data"]["agentId"] != agent_id {
                    continue;
                }
                let kind = event["type"].as_str().unwrap_or_default().to_string();
                assert_ne!(kind, "agent:failed", "turn failed: {event}");
                seen.push(event);
                if kind == "agent:stream:end" {
                    return;
                }
            }
        },
    )
    .await
    .expect("the turn must end");
    seen
}

fn method_frames<'a>(frames: &'a [Value], method: &str) -> Vec<&'a Value> {
    frames
        .iter()
        .filter(|f| f["dir"] == "in" && f["msg"]["method"] == method)
        .map(|f| &f["msg"])
        .collect()
}

/// Stop sends `session/cancel` as a notification and the same process
/// takes the next turn; after a daemon restart the session resumes through
/// `session/load`, Auto is re-applied, and the replayed history is not
/// duplicated in the conversation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cursor_stop_then_resume_over_wss() {
    use std::os::unix::fs::PermissionsExt;
    let Some(node) = intent_providers::resolve_on_path("node") else {
        eprintln!("skipping Cursor WSS E2E: node not on PATH");
        return;
    };
    let fixture = format!(
        "{}/tests/fixtures/mock-cursor-agent.mjs",
        env!("CARGO_MANIFEST_DIR")
    );
    let data_dir_guard = common::test_tempdir_in("/tmp", "itd-wss-cursor-resume-");
    let data_dir = data_dir_guard.path().to_path_buf();
    let _mock_cleanup = MockCleanup(data_dir.join("mock-pids.log"));
    let wrapper = data_dir.join("cursor-agent");
    std::fs::write(
        &wrapper,
        format!(
            "#!/bin/sh\nexec '{}' '{}' \"$@\"\n",
            node.display(),
            fixture
        ),
    )
    .unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::write(
        data_dir.join("config.toml"),
        format!("[providers.paths]\ncursor = \"{}\"\n", wrapper.display()),
    )
    .unwrap();
    let log = data_dir.join("cursor-rpc.jsonl");
    let pid_file = data_dir.join("cursor-acp.pid");
    let env = [
        ("MOCK_CURSOR_RPC_LOG", log.to_str().unwrap()),
        ("MOCK_CURSOR_PID_FILE", pid_file.to_str().unwrap()),
    ];
    let socket = data_dir.join("intentd.sock");
    let read_pid = || -> i32 {
        std::fs::read_to_string(&pid_file)
            .unwrap()
            .split_whitespace()
            .next()
            .unwrap()
            .parse()
            .unwrap()
    };

    let mut daemon = Daemon {
        child: spawn_serve(&data_dir, &env),
    };
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let workspace = wss_rpc(
        &mut rpc,
        1,
        "workspace.create",
        json!({ "title": "Cursor stop and resume", "noPrompt": true }),
    )
    .await;
    let workspace_id = workspace["workspace"]["id"].as_str().unwrap().to_string();
    let mut events = connect_ws(port, cfg).await;
    wss_rpc(
        &mut events,
        1,
        "events.subscribe",
        json!({ "workspaceId": workspace_id, "eventTypes": ["agent:*"] }),
    )
    .await;
    // No model stored: Auto must still be applied explicitly.
    let created = wss_rpc(
        &mut rpc,
        2,
        "agent.create",
        json!({ "workspaceId": workspace_id, "name": "Cursor resume", "provider": "cursor" }),
    )
    .await;
    let agent_id = created["agent"]["id"].as_str().unwrap().to_string();

    // Turn 1 blocks until Stop.
    wss_rpc(
        &mut rpc,
        3,
        "agent.sendMessage",
        json!({ "workspaceId": workspace_id, "agentId": agent_id, "content": "WAIT_FOR_CANCEL please" }),
    )
    .await;
    timeout(
        intentd_test_support::test_timeout(Duration::from_secs(30)),
        async {
            while method_frames(&rpc_log(&log), "session/prompt").is_empty() {
                // timing-guard: poll interval until the mock logs the prompt (bounded by the timeout).
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        },
    )
    .await
    .expect("the first prompt reaches Cursor");
    let pid = read_pid();
    wss_rpc(
        &mut rpc,
        4,
        "agent.stop",
        json!({ "workspaceId": workspace_id, "agentId": agent_id }),
    )
    .await;
    turn_events(&mut events, &agent_id).await;
    let frames = rpc_log(&log);
    let cancels = method_frames(&frames, "session/cancel");
    assert!(!cancels.is_empty(), "Stop must send session/cancel");
    assert!(
        cancels.iter().all(|m| m.get("id").is_none()),
        "session/cancel is a notification: {cancels:?}"
    );
    assert!(pid_alive(pid), "Stop keeps the Cursor process alive");

    // Turn 2 continues on the same process and session.
    wss_rpc(
        &mut rpc,
        5,
        "agent.sendMessage",
        json!({ "workspaceId": workspace_id, "agentId": agent_id, "content": "check git status" }),
    )
    .await;
    turn_events(&mut events, &agent_id).await;
    assert_eq!(read_pid(), pid, "the next turn reuses the process");
    let frames = rpc_log(&log);
    assert_eq!(method_frames(&frames, "session/new").len(), 1);
    assert_eq!(method_frames(&frames, "session/prompt").len(), 2);
    let auto_applied = |frames: &[Value]| {
        method_frames(frames, "session/set_config_option")
            .iter()
            .filter(|m| m["params"]["configId"] == "model" && m["params"]["value"] == "default[]")
            .count()
    };
    assert_eq!(auto_applied(&frames), 1, "no stored model applies Auto");
    let before = wss_rpc(
        &mut rpc,
        6,
        "agent.getConversation",
        json!({ "workspaceId": workspace_id, "agentId": agent_id }),
    )
    .await;
    let before = common::conversation_fingerprint(&before);

    // Restart the daemon: the next message resumes via session/load.
    drop(rpc);
    drop(events);
    daemon.child.kill_tree().unwrap();
    daemon.child = spawn_serve(&data_dir, &env);
    assert!(await_uds(&socket).await, "daemon did not restart");
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
    let mut rpc = connect_ws(port, cfg.clone()).await;
    let mut events = connect_ws(port, cfg).await;
    wss_rpc(
        &mut events,
        1,
        "events.subscribe",
        json!({ "workspaceId": workspace_id, "eventTypes": ["agent:*"] }),
    )
    .await;
    let after_restart = wss_rpc(
        &mut rpc,
        7,
        "agent.getConversation",
        json!({ "workspaceId": workspace_id, "agentId": agent_id }),
    )
    .await;
    assert_eq!(
        common::conversation_fingerprint(&after_restart),
        before,
        "a restart alone changes no history"
    );
    wss_rpc(
        &mut rpc,
        8,
        "agent.sendMessage",
        json!({ "workspaceId": workspace_id, "agentId": agent_id, "content": "and again" }),
    )
    .await;
    turn_events(&mut events, &agent_id).await;
    let frames = rpc_log(&log);
    let loads = method_frames(&frames, "session/load");
    assert_eq!(loads.len(), 1, "the restart resumes through session/load");
    let session_id = method_frames(&frames, "session/prompt")[0]["params"]["sessionId"].clone();
    assert_eq!(loads[0]["params"]["sessionId"], session_id);
    assert_eq!(
        auto_applied(&frames),
        2,
        "Auto is re-applied after session/load"
    );
    let after = wss_rpc(
        &mut rpc,
        9,
        "agent.getConversation",
        json!({ "workspaceId": workspace_id, "agentId": agent_id }),
    )
    .await;
    let after = common::conversation_fingerprint(&after);
    assert!(
        after.starts_with(&before),
        "the resumed conversation keeps the earlier history as-is:\n{}",
        common::fingerprint_diff(&before, &after)
    );
    let new_rows = &after[before.len()..];
    let new_text = serde_json::to_string(new_rows).unwrap();
    assert_eq!(
        new_text.matches("check git status").count(),
        0,
        "replayed history must not be appended again: {new_text}"
    );
    assert!(new_text.contains("and again"), "{new_text}");
}
