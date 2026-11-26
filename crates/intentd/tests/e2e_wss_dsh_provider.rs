//! WSS e2e for the DeepSeek Harness provider against a mock `dsh`
//! (`fixtures/mock-dsh.mjs`, built from captured dsh 0.2.2-rc.5 traffic), wired
//! through `providers.paths.dsh` so the real registry entry, launch overlay,
//! upload guard, sign-in check, model source and turn all run.
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

fn wss_frame(id: i64, method: &str, params: &Value) -> Message {
    Message::Text(
        json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params })
            .to_string()
            .into(),
    )
}

/// The full response frame (result or error) for `id`.
async fn wss_call(ws: &mut Ws, id: i64, method: &str, params: Value) -> Value {
    ws.send(wss_frame(id, method, &params))
        .await
        .expect("send rpc frame");
    timeout(
        intentd_test_support::test_timeout(Duration::from_secs(60)),
        async {
            loop {
                if let Some(Ok(Message::Text(text))) = ws.next().await {
                    let v: Value = serde_json::from_str(&text).expect("json frame");
                    if v["id"] == json!(id) {
                        return v;
                    }
                }
            }
        },
    )
    .await
    .expect("wss rpc timed out")
}

struct Fixture {
    _dir: tempfile::TempDir,
    data_dir: std::path::PathBuf,
    log: std::path::PathBuf,
    _daemon: Daemon,
    port: u16,
    cfg: Arc<ClientConfig>,
}

/// A daemon whose `providers.paths.dsh` is the mock, with a private HOME and
/// DSH home. `api_key` sets `DEEPSEEK_API_KEY` in the daemon's environment.
async fn start(prefix: &str, api_key: Option<&str>, extra: &[(&str, &str)]) -> Option<Fixture> {
    use std::os::unix::fs::PermissionsExt;
    let Some(node) = intent_providers::resolve_on_path("node") else {
        eprintln!("skipping DSH WSS E2E: node not on PATH");
        return None;
    };
    let mock = format!("{}/tests/fixtures/mock-dsh.mjs", env!("CARGO_MANIFEST_DIR"));
    let dump = format!(
        "{}/../intent-providers/tests/fixtures/dsh-dump-config-0.2.2-rc.5.txt",
        env!("CARGO_MANIFEST_DIR")
    );
    let dir = common::test_tempdir_in("/tmp", prefix);
    let data_dir = dir.path().to_path_buf();
    let home = data_dir.join("home");
    std::fs::create_dir_all(&home).unwrap();
    let wrapper = data_dir.join("dsh");
    std::fs::write(
        &wrapper,
        format!("#!/bin/sh\nexec '{}' '{mock}' \"$@\"\n", node.display()),
    )
    .unwrap();
    std::fs::set_permissions(&wrapper, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::write(
        data_dir.join("config.toml"),
        format!("[providers.paths]\ndsh = \"{}\"\n", wrapper.display()),
    )
    .unwrap();
    let log = data_dir.join("dsh.jsonl");
    let home_s = home.to_str().unwrap().to_string();
    let dsh_home = home.join(".dsh");
    let mut env: Vec<(&str, &str)> = vec![
        ("MOCK_DSH_LOG", log.to_str().unwrap()),
        ("MOCK_DSH_DUMP", &dump),
        ("HOME", &home_s),
        ("DSH_HOME", dsh_home.to_str().unwrap()),
    ];
    env.extend_from_slice(extra);
    if let Some(key) = api_key {
        env.push(("DEEPSEEK_API_KEY", key));
    }
    let mut child = spawn_serve_with(&data_dir, &env, api_key.is_none());
    let socket = data_dir.join("intentd.sock");
    if !await_uds(&socket).await {
        let _ = child.kill_tree();
        panic!("daemon did not start");
    }
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
    Some(Fixture {
        _dir: dir,
        data_dir,
        log,
        _daemon: Daemon { child },
        port,
        cfg,
    })
}

fn spawn_serve_with(data_dir: &Path, env: &[(&str, &str)], without_key: bool) -> GuardedChild {
    if !without_key {
        return spawn_serve(data_dir, env);
    }
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = data_dir.join("workspaces");
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    common::enable_ws_api(data_dir);
    let mut cmd = common::hermetic_serve_command(data_dir);
    cmd.env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .env_remove("DEEPSEEK_API_KEY")
        .stdout(Stdio::null())
        .stderr(Stdio::from(log));
    for (k, v) in env {
        cmd.env(k, v);
    }
    common::hermetic_fixture_identity(&mut cmd, data_dir);
    GuardedChild::spawn(&mut cmd).expect("spawn intentd serve")
}

fn mock_log(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .map(|l| serde_json::from_str(l).expect("mock log line"))
        .collect()
}

/// Discovery, sign-in, the grouped model list and a full turn, with the
/// upload overlay and telemetry opt-out on every DSH launch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dsh_provider_discovery_models_and_turn_over_wss() {
    let Some(fx) = start("itd-wss-dsh-", Some("sk-e2e-placeholder"), &[]).await else {
        return;
    };
    let mut rpc = connect_ws(fx.port, fx.cfg.clone()).await;

    let catalog = wss_rpc(&mut rpc, 1, "providers.catalog", json!({})).await;
    let dsh = row(&catalog["providers"], "dsh");
    assert_eq!(dsh["displayName"], "DeepSeek Harness");
    assert_eq!(dsh["shortName"], "DSH");
    assert_eq!(dsh["command"], "dsh");
    assert_eq!(dsh["canBeDisabled"], true);
    assert_eq!(dsh["loginCommandHint"], "dsh web");

    let discovery = wss_rpc(&mut rpc, 2, "host.providerDiscovery", json!({})).await;
    let dsh = row(&discovery["providers"], "dsh");
    assert_eq!(dsh["installed"], true, "{dsh}");
    assert_eq!(dsh["cliCommand"], "dsh", "{dsh}");
    assert_eq!(dsh["cliVersion"], "0.2.2-rc.5", "{dsh}");
    assert!(dsh.get("unavailableReason").is_none(), "{dsh}");

    let auth = wss_rpc(
        &mut rpc,
        3,
        "host.providerAuthStatus",
        json!({ "providerId": "dsh", "force": true }),
    )
    .await;
    assert_eq!(auth["providers"][0]["id"], "dsh", "{auth}");
    assert_eq!(auth["providers"][0]["authenticated"], true, "{auth}");

    let flash = r#"["deepseek-official","deepseek-v4-flash"]"#;
    let pro = r#"["deepseek-official","deepseek-v4-pro"]"#;
    let models = wss_rpc(
        &mut rpc,
        4,
        "models.list",
        json!({ "providerId": "dsh", "forceRefresh": true }),
    )
    .await;
    let rows = models["models"].as_array().expect("model rows");
    assert_eq!(
        rows.iter()
            .map(|r| r["id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        [flash, pro],
        "{models}"
    );
    assert_eq!(rows[1]["name"], "deepseek-v4-pro", "{models}");
    assert_eq!(
        rows[0]["effortLevels"],
        json!(["off", "low", "high", "max"])
    );

    let workspace = wss_rpc(
        &mut rpc,
        5,
        "workspace.create",
        json!({ "title": "DSH E2E", "noPrompt": true }),
    )
    .await;
    let workspace_id = workspace["workspace"]["id"].as_str().unwrap().to_string();
    let mut events = connect_ws(fx.port, fx.cfg.clone()).await;
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
        json!({ "workspaceId": workspace_id, "name": "DSH fixture", "provider": "dsh", "model": pro }),
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
                    Some("agent:failed") => panic!("dsh turn failed: {event}"),
                    Some("agent:idle") => break,
                    _ => {}
                }
            }
        },
    )
    .await
    .expect("DSH turn must finish");
    assert!(
        tools.iter().any(|t| t["toolCallId"] == "call_00_bash"),
        "{tools:?}"
    );

    let frames = mock_log(&fx.log);
    // Every DSH launch (version, guard, model probe, agent) ran with the
    // telemetry opt-out, and every profile launch with Intent's overlay.
    let launches: Vec<&Value> = frames.iter().filter(|f| f["dir"] == "launch").collect();
    assert!(launches.len() >= 4, "{launches:?}");
    let overlay_path = fx
        .data_dir
        .join("home/.local/share/intent/providers/dsh/intent-overlay.yml");
    for launch in &launches {
        assert_eq!(launch["msg"]["telemetryDisabled"], "1", "{launch}");
        let args: Vec<&str> = launch["msg"]["args"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a.as_str().unwrap())
            .collect();
        if args.first() == Some(&"--profile") {
            assert_eq!(
                &args[..4],
                [
                    "--profile",
                    "acp",
                    "--patch",
                    overlay_path.to_str().unwrap()
                ],
                "{launch}"
            );
        }
    }
    assert!(
        launches.iter().any(|l| l["msg"]["args"]
            .as_array()
            .unwrap()
            .iter()
            .any(|a| a == "--dump-config")),
        "the upload guard ran: {launches:?}"
    );
    assert_eq!(
        std::fs::read_to_string(&overlay_path).unwrap(),
        intent_providers::dsh::OVERLAY
    );

    let sent = |method: &str| -> Vec<&Value> {
        frames
            .iter()
            .filter(|f| f["dir"] == "in" && f["msg"]["method"] == method)
            .map(|f| &f["msg"])
            .collect()
    };
    assert!(sent("authenticate").is_empty(), "DSH has no ACP sign-in");
    let new = sent("session/new");
    let agent_session = new.last().expect("agent session/new");
    let servers = agent_session["params"]["mcpServers"].as_array().unwrap();
    assert!(
        servers.iter().any(|s| s["command"].is_string()
            && s["args"]
                .as_array()
                .is_some_and(|a| a.iter().any(|x| x == "mcp-bridge"))),
        "stdio workspace bridge missing: {servers:?}"
    );
    assert!(
        sent("session/set_config_option")
            .iter()
            .any(|f| f["params"]["configId"] == "model" && f["params"]["value"] == pro),
        "the picked model is applied: {frames:?}"
    );
    let prompt = sent("session/prompt");
    assert_eq!(prompt.len(), 1);
    assert!(
        prompt[0]["params"]["prompt"]
            .to_string()
            .contains("<system>"),
        "first-turn prepend missing"
    );
}

/// Without a DeepSeek key DSH reports signed out; a DSH whose upload row was
/// renamed is unavailable and refused at agent start.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dsh_signed_out_and_upload_guard_over_wss() {
    let Some(fx) = start(
        "itd-wss-dsh-guard-",
        None,
        &[("MOCK_DSH_RENAMED_UPLOAD", "1")],
    )
    .await
    else {
        return;
    };
    let mut rpc = connect_ws(fx.port, fx.cfg.clone()).await;

    let auth = wss_rpc(
        &mut rpc,
        1,
        "host.providerAuthStatus",
        json!({ "providerId": "dsh", "force": true }),
    )
    .await;
    assert_eq!(auth["providers"][0]["authenticated"], false, "{auth}");

    let discovery = wss_rpc(&mut rpc, 2, "host.providerDiscovery", json!({})).await;
    let dsh = row(&discovery["providers"], "dsh");
    assert_eq!(dsh["installed"], false, "{dsh}");
    assert_eq!(dsh["cliVersion"], "0.2.2-rc.5", "{dsh}");
    let reason = dsh["unavailableReason"]
        .as_str()
        .expect("unavailableReason");
    assert!(reason.contains("session-log-deepseek-v2"), "{reason}");

    // A key saved by `dsh web` (the versioned `refs` layout) counts as
    // signed in, so the refusal below is the upload guard, not sign-in.
    let dsh_home = fx.data_dir.join("home/.dsh");
    std::fs::create_dir_all(&dsh_home).unwrap();
    std::fs::write(
        dsh_home.join(".credentials.yaml"),
        "version: 1\nrefs:\n  DEEPSEEK_API_KEY: sk-saved\n",
    )
    .unwrap();
    let auth = wss_rpc(
        &mut rpc,
        3,
        "host.providerAuthStatus",
        json!({ "providerId": "dsh", "force": true }),
    )
    .await;
    assert_eq!(auth["providers"][0]["authenticated"], true, "{auth}");

    let workspace = wss_rpc(
        &mut rpc,
        4,
        "workspace.create",
        json!({ "title": "DSH guard", "noPrompt": true }),
    )
    .await;
    let workspace_id = workspace["workspace"]["id"].as_str().unwrap().to_string();
    let mut events = connect_ws(fx.port, fx.cfg.clone()).await;
    wss_rpc(
        &mut events,
        1,
        "events.subscribe",
        json!({ "workspaceId": workspace_id, "eventTypes": ["agent:*"] }),
    )
    .await;
    let created = wss_call(
        &mut rpc,
        5,
        "agent.create",
        json!({ "workspaceId": workspace_id, "name": "DSH guarded", "provider": "dsh" }),
    )
    .await;
    let refusal = if created.get("error").is_some() {
        created
    } else {
        let agent_id = created["result"]["agent"]["id"]
            .as_str()
            .unwrap()
            .to_string();
        let sent = wss_call(
            &mut rpc,
            6,
            "agent.sendMessage",
            json!({ "workspaceId": workspace_id, "agentId": agent_id, "content": "hi" }),
        )
        .await;
        if sent.get("error").is_some() {
            sent
        } else {
            // The launch runs after the send is accepted; its refusal fails the agent.
            timeout(
                intentd_test_support::test_timeout(Duration::from_secs(60)),
                async {
                    loop {
                        let Some(Ok(Message::Text(text))) = events.next().await else {
                            continue;
                        };
                        let frame: Value = serde_json::from_str(&text).unwrap();
                        let event = &frame["params"]["event"];
                        if event["data"]["agentId"] == agent_id.as_str()
                            && event["type"] == "agent:failed"
                        {
                            return event.clone();
                        }
                    }
                },
            )
            .await
            .expect("the guarded launch fails the agent")
        }
    };
    assert!(
        refusal
            .to_string()
            .contains("cannot start DeepSeek Harness agent"),
        "{refusal}"
    );
    assert!(
        mock_log(&fx.log)
            .iter()
            .all(|f| f["msg"]["method"] != "session/prompt"),
        "a guarded DSH never receives a prompt"
    );
}
