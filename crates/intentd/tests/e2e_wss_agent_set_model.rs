//! WSS e2e test for STAB-115: `agent.setModel` triggering provider respawn.
//!
//! Boots a real `intentd serve` (WSS listener enabled via config) against the mock ACP provider and
//! verifies that calling `agent.setModel` while a provider child is live causes
//! the next turn to respawn the child with the new model.
//!
//! Gated on `node` + the mock script; skips cleanly otherwise.

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
        let _ = std::fs::remove_dir_all(&self.data_dir);
    }
}

fn temp_data_dir() -> PathBuf {
    let id = Uuid::new_v4().simple().to_string();
    let dir = PathBuf::from("/tmp").join(format!("itd-wss-setmodel-{}", &id[..8]));
    std::fs::create_dir_all(&dir).expect("mkdir data dir");
    dir
}

fn spawn_serve(data_dir: &Path, listen: &str, env: &[(&str, &str)]) -> Child {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    let secrets_file = data_dir.join("secrets.json");
    if listen != "uds" {
        common::enable_ws_api(data_dir);
    }
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_intentd"));
    cmd.arg("serve")
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_SECRETS_FILE", &secrets_file)
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

async fn connect_ws(
    port: u16,
    cfg: Arc<ClientConfig>,
) -> WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>> {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

async fn wss_rpc<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send rpc frame");
    loop {
        let next = timeout(Duration::from_secs(15), ws.next())
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

fn gate() -> Option<String> {
    let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping WSS setModel E2E: node not on PATH");
        return None;
    }
    if !std::path::Path::new(&script).exists() {
        eprintln!("skipping WSS setModel E2E: mock script missing at {script}");
        return None;
    }
    Some(script)
}

/// Exercise the real Antigravity registry/launch/session path with the existing
/// deterministic ACP fixture, including both cold load and recreation.
#[tokio::test]
async fn antigravity_exact_model_and_isolated_profile_survive_respawn_over_wss() {
    use std::os::unix::fs::PermissionsExt;
    let Some(script) = gate() else { return };
    let node = intent_providers::resolve_on_path("node").unwrap();
    for (load, reject_model) in [(true, false), (false, false), (true, true)] {
        let data_dir = temp_data_dir();
        let wrapper = data_dir.join("antigravity-fixture");
        std::fs::write(
            &wrapper,
            format!("#!/bin/sh\nexec '{}' '{}'\n", node.display(), script),
        )
        .unwrap();
        std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
        std::fs::write(
            data_dir.join("config.toml"),
            format!(
                "[providers.paths]\nantigravity = \"{}\"\n",
                wrapper.display()
            ),
        )
        .unwrap();
        let log = data_dir.join("acp-rpc.jsonl");
        let behavior =
            json!({"advertiseLoadSession":load,"rejectSetConfigOption":reject_model,"response":"antigravity fixture complete"})
                .to_string();
        let catalog = json!({"models":{"currentModelId":"gemini-3.7-flash-low","availableModels":[
            {"modelId":"gemini-3.7-flash-low","name":"Gemini 3.7 Flash (Low)"},
            {"modelId":"gemini-3.6-flash-medium","name":"Gemini 3.6 Flash (Medium)"}
        ]},"configOptions":[{"id":"model","category":"model","name":"Model","type":"select","currentValue":"gemini-3.7-flash-low","options":[
            {"value":"gemini-3.7-flash-low","name":"Gemini 3.7 Flash (Low)"},
            {"value":"gemini-3.6-flash-medium","name":"Gemini 3.6 Flash (Medium)"}
        ]}]})
        .to_string();
        let child = spawn_serve(
            &data_dir,
            "both",
            &[
                ("INTENTD_AUTH_TOKEN", TOKEN),
                ("INTENTD_TCP_PORT", "0"),
                ("MOCK_AGENT_BEHAVIOR", &behavior),
                ("MOCK_AGENT_SESSION_RESULT", &catalog),
                ("MOCK_AGENT_RPC_LOG", log.to_str().unwrap()),
            ],
        );
        let _daemon = Daemon {
            child,
            data_dir: data_dir.clone(),
        };
        let socket = data_dir.join("intentd.sock");
        assert!(await_uds(&socket).await, "daemon did not start");
        let status = common::await_wss_status(&socket).await;
        let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
        let cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
        let mut rpc = connect_ws(port, cfg.clone()).await;
        let discovered = wss_rpc(&mut rpc, 2, "host.providerDiscovery", json!({})).await;
        let provider = discovered["providers"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["id"] == "antigravity")
            .unwrap();
        assert_eq!(provider["installed"], true);
        // resolvedPath intentionally reports auto-detection, while installed
        // and the auth/model/session paths honor the configured override.
        assert_eq!(provider["command"], "antigravity-acp");
        let authenticated = wss_rpc(
            &mut rpc,
            3,
            "host.providerAuthStatus",
            json!({"providerId":"antigravity","force":true}),
        )
        .await;
        assert_eq!(authenticated["providers"][0]["authenticated"], true);
        let models = wss_rpc(
            &mut rpc,
            4,
            "models.list",
            json!({"providerId":"antigravity","forceRefresh":true}),
        )
        .await;
        assert_eq!(models["models"].as_array().unwrap().len(), 2);
        assert_eq!(models["models"][0]["id"], "gemini-3.7-flash-low");
        assert_eq!(models["models"][0]["isDefault"], true);
        assert!(models["models"][0].get("effortLevels").is_none());
        let workspace = wss_rpc(
            &mut rpc,
            10,
            "workspace.create",
            json!({"title":"Antigravity session test","noPrompt":true}),
        )
        .await;
        let workspace_id = workspace["workspace"]["id"].as_str().unwrap();
        let mut events = connect_ws(port, cfg).await;
        wss_rpc(
            &mut events,
            1,
            "events.subscribe",
            json!({"workspaceId":workspace_id,"eventTypes":["agent:*"]}),
        )
        .await;
        let created = wss_rpc(&mut rpc, 11, "agent.create", json!({"workspaceId":workspace_id,"name":"Antigravity fixture","provider":"antigravity","model":"gemini-3.7-flash-low"})).await;
        let agent = created["agent"]["id"].as_str().unwrap();
        for (turn, model) in [(0, "gemini-3.7-flash-low"), (1, "gemini-3.6-flash-medium")] {
            if turn == 1 && !reject_model {
                wss_rpc(&mut rpc, 12, "agent.setModel", json!({"workspaceId":workspace_id,"agentId":agent,"providerId":"antigravity","modelId":model})).await;
            }
            wss_rpc(&mut rpc, 20 + turn, "agent.sendMessage", json!({"workspaceId":workspace_id,"agentId":agent,"content":format!("test turn {turn}")})).await;
            timeout(common::test_timeout(Duration::from_secs(40)), async {
                loop {
                    if let Some(Ok(Message::Text(text))) = events.next().await {
                        let frame: Value = serde_json::from_str(&text).unwrap();
                        let event = &frame["params"]["event"];
                        if event["data"]["agentId"] == agent {
                            if event["type"] == "agent:failed" {
                                assert!(reject_model, "session error: {event}");
                                assert!(event.to_string().contains("rejected model"));
                                break;
                            }
                            if event["type"] == "agent:idle" {
                                assert!(!reject_model, "rejected model must not run a prompt");
                                break;
                            }
                        }
                    }
                }
            })
            .await
            .expect("Antigravity turn must finish");
            if reject_model {
                // Let the terminal failure's queue/status publication finish
                // before redriving the same model on the next user message.
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        let calls: Vec<Value> = std::fs::read_to_string(&log)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        let prompts: Vec<_> = calls
            .iter()
            .filter(|call| call["method"] == "session/prompt")
            .collect();
        if reject_model {
            assert!(
                prompts.is_empty(),
                "a failed setup never reaches session/prompt"
            );
            let attempts: Vec<_> = calls
                .iter()
                .filter(|call| call["method"] == "session/set_config_option")
                .collect();
            assert_eq!(
                attempts.len(),
                2,
                "each redrive reapplies the rejected model"
            );
            assert_ne!(
                attempts[0]["pid"], attempts[1]["pid"],
                "failed setup is reaped, not cached"
            );
            continue;
        }
        assert_eq!(prompts.len(), 2, "one prompt per turn: {calls:?}");
        assert_ne!(
            prompts[0]["pid"], prompts[1]["pid"],
            "model change respawns"
        );
        assert_eq!(
            prompts[0]["geminiHome"], prompts[1]["geminiHome"],
            "restore keeps private conversation state"
        );
        let home = Path::new(prompts[0]["geminiHome"].as_str().unwrap());
        assert!(home.starts_with(data_dir.canonicalize().unwrap().join("antigravity")));
        let hooks = std::fs::read_to_string(home.join("config/hooks.json")).unwrap();
        assert!(hooks.contains("antigravity-tool-guard"));
        assert!(!hooks.contains("--allow-tool 'start_subagent'"));
        assert_eq!(
            serde_json::from_slice::<Value>(
                &std::fs::read(home.join("config/mcp_config.json")).unwrap()
            )
            .unwrap(),
            json!({"mcpServers":{}})
        );
        for (prompt, model) in prompts
            .iter()
            .zip(["gemini-3.7-flash-low", "gemini-3.6-flash-medium"])
        {
            let process: Vec<_> = calls
                .iter()
                .filter(|call| call["pid"] == prompt["pid"])
                .collect();
            let mode = process
                .iter()
                .position(|call| call["method"] == "session/set_mode")
                .unwrap();
            let selected = process
                .iter()
                .position(|call| call["method"] == "session/set_config_option")
                .unwrap();
            let sent = process
                .iter()
                .position(|call| call["method"] == "session/prompt")
                .unwrap();
            assert!(mode < selected && selected < sent);
            assert_eq!(process[mode]["params"]["modeId"], "default");
            assert_eq!(process[selected]["params"]["value"], model);
            assert!(!process.iter().any(|call| call["method"] == "authenticate"));
            let opened = process
                .iter()
                .find(|call| call["method"] == "session/new" || call["method"] == "session/load")
                .unwrap();
            assert!(opened["params"]["mcpServers"]
                .as_array()
                .unwrap()
                .iter()
                .any(|server| server["name"] == "workspace-mcp"));
        }
        assert!(
            prompts[0]["params"]["prompt"]
                .to_string()
                .contains("<system>"),
            "first-turn instructions delivered"
        );
        assert_eq!(
            calls.iter().any(|call| call["method"] == "session/load"),
            load
        );
        let agent = wss_rpc(
            &mut rpc,
            40,
            "agent.get",
            json!({"workspaceId":workspace_id,"agentId":agent}),
        )
        .await;
        assert_eq!(agent["agent"]["provider"], "antigravity");
        assert_eq!(agent["agent"]["model"], "gemini-3.6-flash-medium");
    }
}

/// STAB-115: agent.setModel triggers respawn on next turn when provider child is live.
/// 1. Create agent with model "sonnet4.5" on provider "auggie"
/// 2. Send message (spawns child with sonnet4.5)
/// 3. Call agent.setModel to change to "haiku"
/// 4. Send another message (should respawn with haiku)
/// 5. Verify agent.get shows model="haiku"
#[tokio::test]
async fn agent_set_model_triggers_respawn_over_wss() {
    let Some(script) = gate() else {
        return;
    };

    let data_dir = temp_data_dir();
    let behavior = json!({
        "response": "mock response",
    })
    .to_string();
    let env: [(&str, &str); 4] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
    ];
    let child = spawn_serve(&data_dir, "both", &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir.clone(),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let fingerprint = status["result"]["fingerprint"]
        .as_str()
        .expect("fingerprint")
        .to_string();
    let cfg = client_config(&fingerprint);

    let mut ws = connect_ws(port, cfg).await;

    // Create workspace
    let ws_result = wss_rpc(
        &mut ws,
        10,
        "workspace.create",
        json!({ "title": "STAB-115 WSS E2E", "noPrompt": true }),
    )
    .await;
    let ws_id = ws_result["workspace"]["id"].as_str().expect("workspace id");

    // Create agent with initial model "sonnet4.5" on provider "auggie"
    let agent_result = wss_rpc(
        &mut ws,
        20,
        "agent.create",
        json!({
            "workspaceId": ws_id,
            "name": "SetModel Test",
            "model": "sonnet4.5", "provider": "auggie",
        }),
    )
    .await;
    let agent_id = agent_result["agent"]["id"].as_str().expect("agent id");

    // Send first message (spawns child with sonnet4.5)
    wss_rpc(
        &mut ws,
        30,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": agent_id,
            "content": "first message",
        }),
    )
    .await;

    // Change model to "haiku"
    wss_rpc(
        &mut ws,
        40,
        "agent.setModel",
        json!({
            "workspaceId": ws_id,
            "agentId": agent_id,
            "modelId": "haiku",
            "providerId": "auggie",
        }),
    )
    .await;

    // Send second message (should trigger respawn with haiku)
    wss_rpc(
        &mut ws,
        50,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": agent_id,
            "content": "second message",
        }),
    )
    .await;

    // Verify the model changed via agent.get. The respawn applies the new
    // model asynchronously after agent.sendMessage returns, so poll with a
    // bounded deadline instead of asserting immediately.
    let deadline = tokio::time::Instant::now() + common::test_timeout(Duration::from_secs(30));
    let mut rpc_id = 60;
    loop {
        let get_result = wss_rpc(
            &mut ws,
            rpc_id,
            "agent.get",
            json!({
                "agentId": agent_id,
                "workspaceId": ws_id,
            }),
        )
        .await;
        let model = get_result["agent"]["model"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        if model == "haiku" {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "model should have changed to haiku after setModel; last saw {model:?}"
        );
        rpc_id += 1;
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
