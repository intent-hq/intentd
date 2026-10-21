//! WSS e2e for provider worker observations (PROTOCOL §5.5b): a claude-code
//! agent whose adapter reports a background shell through the AIR
//! async-tasks extension shows up in `agent.workers.list` and on the
//! `agent.workers.subscribe` channel, can be stopped with
//! `agent.workers.stop` (forwarded to the provider as
//! `_session/async_task/stop`), and a between-turn completion updates the
//! projection without opening a turn. Envelope, ownership, generation-fence,
//! capacity and teardown errors are asserted on the wire.
//!
//! Hermetic setup (shared with the npx isolation suite): the daemon child's
//! `PATH` starts with a scratch `bin/` holding a fake `node`, a fake `npx`
//! that execs the deterministic mock ACP fixture, and a version-only `claude`
//! CLI. The mock answers `initialize` with the reviewed claude-agent-acp
//! identity (`agentInfo`), records the client capabilities it was offered
//! (`MOCK_AGENT_INIT_LOG`) and every stop request (`MOCK_AGENT_STOP_LOG`).
//! Nothing is downloaded. Gated on `node`, `git` and the mock script.

#![cfg(unix)]

mod common;

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use intentd_test_support::GuardedChild;
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

/// Fixed 64-hex token, adopted by the daemon via the `INTENTD_AUTH_TOKEN` seam.
const TOKEN: &str = "efefefefefefefefefefefefefefefefefefefefefefefefefefefefefefefef";

/// Live `intentd serve` process; killed (whole process group) on drop, with
/// the daemon log echoed for post-mortems.
struct Daemon {
    child: GuardedChild,
    data_dir: tempfile::TempDir,
}

impl Drop for Daemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        let log_path = self.data_dir.path().join("daemon.log");
        if let Ok(log) = std::fs::read_to_string(&log_path) {
            eprintln!("=== DAEMON LOG ===\n{log}\n=== END LOG ===");
        }
    }
}

fn temp_data_dir() -> tempfile::TempDir {
    common::test_tempdir_in("/tmp", "itd-wss-workers-")
}

/// The hermetic workspaces root — deliberately containing a space so the ACP
/// session cwd shape with whitespace is exercised end to end.
fn workspaces_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("work spaces")
}

fn spawn_serve(data_dir: &Path, env: &[(&str, &str)]) -> GuardedChild {
    let log = std::fs::File::create(data_dir.join("daemon.log")).expect("create daemon log");
    let workspaces_dir = workspaces_dir(data_dir);
    std::fs::create_dir_all(&workspaces_dir).expect("mkdir hermetic workspaces dir");
    common::enable_ws_api(data_dir);
    let mut cmd = common::hermetic_serve_command(data_dir);
    cmd.env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", &workspaces_dir)
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        // The mock is a Node child of the daemon; host-injected Node
        // instrumentation would slow every start (intent-hq/intent#5649).
        .env_remove("NODE_OPTIONS")
        .env("DD_TRACE_ENABLED", "false")
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
            // timing-guard: poll interval
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .is_ok()
}

/// Pin the server's SHA-256 fingerprint (colon-UPPER hex over the DER cert).
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

/// Open an authenticated WSS connection (token in the query string).
async fn connect_ws(
    port: u16,
    cfg: Arc<ClientConfig>,
) -> WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>> {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

/// Send one JSON-RPC frame and return the result whose id matches; any
/// out-of-band notifications (`events.event`) are ignored.
async fn wss_rpc<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send rpc frame");
    loop {
        let next = timeout(common::rpc_read_timeout(), ws.next())
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

/// Read one `events.event` notification from a subscriber connection (bounded).
async fn wss_event<S>(ws: &mut WebSocketStream<S>, secs: u64) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
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
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => {}
            other => panic!("expected text frame, got {other:?}"),
        }
    }
}

/// Mock-agent gate (parity with the WSS lifecycle suite).
fn gate(test: &str) -> Option<String> {
    let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").unwrap_or_else(|_| {
        format!(
            "{}/tests/fixtures/mock-acp-agent.mjs",
            env!("CARGO_MANIFEST_DIR")
        )
    });
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping {test}: node not on PATH");
        return None;
    }
    if !Path::new(&script).exists() {
        eprintln!("skipping {test}: mock script missing at {script}");
        return None;
    }
    if intent_providers::resolve_on_path("git").is_none() {
        eprintln!("skipping {test}: git not on PATH");
        return None;
    }
    Some(script)
}

fn run_git(args: &[&str], cwd: &Path) -> String {
    let out = std::process::Command::new("git")
        .args(args)
        .env("GIT_AUTHOR_NAME", "e2e")
        .env("GIT_AUTHOR_EMAIL", "e2e@example.com")
        .env("GIT_COMMITTER_NAME", "e2e")
        .env("GIT_COMMITTER_EMAIL", "e2e@example.com")
        .current_dir(cwd)
        .stderr(Stdio::null())
        .output()
        .expect("run git");
    assert!(out.status.success(), "git {args:?} failed");
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// A one-commit source repository the workspace is checked out from: a
/// workspace needs a real checkout (`path` / `worktreePath`) to be the
/// agent's cwd — a repository-less workspace spawns its agent in the temp dir.
fn make_source_repo(dir: &Path) -> PathBuf {
    let repo = dir.join("source-repo");
    std::fs::create_dir_all(&repo).expect("mkdir source repo");
    run_git(&["init", "-q", "-b", "main"], &repo);
    std::fs::write(repo.join("README.md"), "hello\n").unwrap();
    run_git(&["add", "README.md"], &repo);
    run_git(&["commit", "-q", "-m", "init"], &repo);
    repo
}

/// A fake `npx`: answers the spawn-time `--version` probe like npm ≥ 7 and
/// otherwise execs the mock fixture under the host's node.
const FAKE_NPX_SCRIPT: &str = "#!/bin/sh\nif [ \"$1\" = --version ]; then\n  echo '10.9.2'\n  exit 0\nfi\nexec '__NODE__' '__SCRIPT__'\n";

/// Write [`FAKE_NPX_SCRIPT`] as an executable `npx` into `bin_dir`, beside
/// a fake `node` that execs the host's node: `find_npx` follows the detected
/// `node` to its sibling `npx` (intent-hq/intent#5725), so a lone `npx` shim
/// on PATH would lose to the real node's own npx — the fake pair together is
/// the toolchain the daemon selects.
fn write_fake_npx(bin_dir: &Path, report: &Path, script: &str) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let node = intent_providers::resolve_on_path("node").expect("node on PATH (gated)");
    let cli = bin_dir.join("claude");
    std::fs::write(
        &cli,
        format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}.cli'\n[ \"$#\" = 1 ] && [ \"$1\" = --version ] || exit 91\nprintf '%s\\n' '{}'\n",
            report.display(),
            intent_providers::test_support::current_cli_version(
                intent_providers::installed_cli::InstalledCli::Claude
            )
        ),
    )
    .expect("write canonical Claude CLI");
    std::fs::set_permissions(&cli, std::fs::Permissions::from_mode(0o755)).expect("chmod CLI");
    let fake_node = bin_dir.join("node");
    std::fs::write(
        &fake_node,
        format!("#!/bin/sh\nexec '{}' \"$@\"\n", node.display()),
    )
    .expect("write fake node");
    std::fs::set_permissions(&fake_node, std::fs::Permissions::from_mode(0o755))
        .expect("chmod node");
    let npx = bin_dir.join("npx");
    std::fs::write(
        &npx,
        FAKE_NPX_SCRIPT
            .replace("__NODE__", &node.display().to_string())
            .replace("__SCRIPT__", script),
    )
    .expect("write fake npx");
    std::fs::set_permissions(&npx, std::fs::Permissions::from_mode(0o755)).expect("chmod npx");
    npx
}

/// Send one JSON-RPC frame and return the whole response frame (success or
/// error) whose id matches; notifications and pushes are skipped.
async fn wss_call<S>(ws: &mut WebSocketStream<S>, id: i64, method: &str, params: Value) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send rpc frame");
    loop {
        let next = timeout(common::rpc_read_timeout(), ws.next())
            .await
            .expect("wss call timed out");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["id"] == json!(id) {
                    return v;
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

/// Read `subscription.push` frames for `subscription_id` until `accept`
/// matches a snapshot; asserts every push is a complete snapshot with a
/// strictly increasing `seq`, starting at `*seq`.
async fn await_push<S>(
    ws: &mut WebSocketStream<S>,
    subscription_id: &str,
    seq: &mut i64,
    accept: impl Fn(&Value) -> bool,
) -> Value
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    for _ in 0..200 {
        let next = timeout(Duration::from_secs(60), ws.next())
            .await
            .expect("worker push timed out");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["method"] != "subscription.push"
                    || v["params"]["subscriptionId"] != json!(subscription_id)
                {
                    continue;
                }
                let params = &v["params"];
                assert_eq!(params["kind"], "snapshot", "complete replacement: {v}");
                let got = params["seq"].as_i64().expect("seq");
                assert!(got > *seq, "seq must increase: {got} after {seq}");
                *seq = got;
                if accept(&params["snapshot"]) {
                    return params["snapshot"].clone();
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => {}
            other => panic!("expected text frame, got {other:?}"),
        }
    }
    panic!("no matching worker snapshot for {subscription_id}");
}

/// Collect this agent's events until its turn settles.
async fn turn_events<S>(sub: &mut WebSocketStream<S>, agent_id: &str) -> Vec<Value>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let mut events = Vec::new();
    for _ in 0..400 {
        let frame = wss_event(sub, 60).await;
        let ev = frame["params"]["event"].clone();
        if ev["data"]["agentId"].as_str() != Some(agent_id) {
            continue;
        }
        let ty = ev["type"].as_str().unwrap_or_default().to_owned();
        events.push(ev);
        if ty == "agent:stream:end" || ty == "agent:failed" {
            assert_eq!(ty, "agent:stream:end", "turn failed: {events:?}");
            return events;
        }
    }
    panic!("turn never settled for {agent_id}");
}

/// Every `events.event` for `agent_id` that arrives within `window`.
async fn events_within<S>(
    sub: &mut WebSocketStream<S>,
    agent_id: &str,
    window: Duration,
) -> Vec<Value>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let deadline = tokio::time::Instant::now() + window;
    let mut events = Vec::new();
    while let Ok(next) = tokio::time::timeout_at(deadline, sub.next()).await {
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                let ev = &v["params"]["event"];
                if v["method"] == "events.event" && ev["data"]["agentId"].as_str() == Some(agent_id)
                {
                    events.push(ev.clone());
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = sub.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => {}
            other => panic!("expected text frame, got {other:?}"),
        }
    }
    events
}

fn read_jsonl(path: &Path) -> Vec<Value> {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("jsonl line"))
        .collect()
}

fn assert_error(frame: &Value, code: i64, data_code: &str) {
    assert!(frame.get("result").is_none(), "expected an error: {frame}");
    assert_eq!(frame["jsonrpc"], "2.0", "{frame}");
    assert_eq!(frame["error"]["code"], json!(code), "{frame}");
    assert_eq!(frame["error"]["data"]["code"], json!(data_code), "{frame}");
}

/// PROTOCOL §5.5b end to end over WSS: subscribe before the agent exists
/// (no-session), spawn a background shell through the AIR extension, see it
/// on the channel, the list and the firehose, stop it through the provider,
/// observe a between-turn completion that opens no turn, and exercise the
/// documented error and capacity envelopes.
#[tokio::test]
async fn provider_workers_are_listed_streamed_and_stopped_over_wss() {
    let Some(script) = gate("WSS agent workers E2E") else {
        return;
    };
    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();
    let bin_dir = data_dir.join("bin");
    let home_dir = data_dir.join("home");
    std::fs::create_dir_all(&bin_dir).unwrap();
    std::fs::create_dir_all(&home_dir).unwrap();
    write_fake_npx(&bin_dir, &data_dir.join("npx-report"), &script);
    let source_repo = make_source_repo(&data_dir);
    let init_log = data_dir.join("init.jsonl");
    let stop_log = data_dir.join("stop.jsonl");
    let path = format!("{}:/usr/bin:/bin", bin_dir.display());
    let behavior = json!({
        "response": "WORKERS_E2E_REPLY",
        "agentInfo": {
            "name": "@agentclientprotocol/claude-agent-acp",
            "version": intent_providers::CLAUDE_AGENT_ACP_VERSION,
        },
        "asyncTasks": {
            "spawnOn": "RUN_BG", "command": "npm run dev",
            "autoCompleteOn": "AUTO_DONE", "completeAfterMs": 1500,
        },
    })
    .to_string();
    let init_log_s = init_log.to_string_lossy().into_owned();
    let stop_log_s = stop_log.to_string_lossy().into_owned();
    let env: [(&str, &str); 8] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("PATH", path.as_str()),
        ("HOME", home_dir.to_str().unwrap()),
        ("SHELL", "/bin/sh"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        ("MOCK_AGENT_INIT_LOG", &init_log_s),
        ("MOCK_AGENT_STOP_LOG", &stop_log_s),
    ];
    let child = spawn_serve(&data_dir, &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir_guard,
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let cfg = client_config(
        status["result"]["fingerprint"]
            .as_str()
            .expect("fingerprint"),
    );

    let mut sub = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut sub,
        1,
        "workspace.create",
        json!({
            "title": "Agent Workers E2E",
            "repositoryPath": source_repo.to_string_lossy(),
            "repositoryName": "source-repo",
            "baseRef": "main",
            "noPrompt": true,
        }),
    )
    .await;
    let ws_id = created["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let subscribed = wss_rpc(
        &mut sub,
        2,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(subscribed["subscriptionId"].is_string(), "{subscribed}");

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let hello = wss_rpc(
        &mut rpc,
        9,
        "client.hello",
        json!({ "clientName": "workers-e2e" }),
    )
    .await;
    assert_eq!(
        hello["server"]["capabilities"]["agentWorkers"],
        json!(1),
        "{hello}"
    );
    let agent = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Workers E2E", "model": "mock-model", "provider": "claude-code" }),
    )
    .await;
    let agent_id = agent["agent"]["id"].as_str().expect("agent id").to_string();
    let target = json!({ "workspaceId": ws_id, "agentId": agent_id });

    // Subscribe before the agent has a provider process: ack, then seq-0.
    let mut workers = connect_ws(port, cfg.clone()).await;
    let ack = wss_call(&mut workers, 20, "agent.workers.subscribe", target.clone()).await;
    assert_eq!(ack["jsonrpc"], "2.0");
    let sub_id = ack["result"]["subscriptionId"]
        .as_str()
        .expect("subscriptionId")
        .to_string();
    let mut seq = -1;
    let first = await_push(&mut workers, &sub_id, &mut seq, |_| true).await;
    assert_eq!(seq, 0, "the first push is seq 0");
    assert_eq!(
        first["support"],
        json!({ "status": "unavailable", "reason": "no-session" })
    );
    assert_eq!(first["continuity"], "lost");
    assert_eq!(first["provider"], "claude-code");
    assert_eq!(first["workers"], json!([]));
    assert_eq!(first["acpSessionId"], Value::Null);
    let listed = wss_rpc(&mut rpc, 21, "agent.workers.list", target.clone()).await;
    assert_eq!(listed["support"], first["support"]);

    // Turn 1 backgrounds a shell. Its tool card stays completed: neither the
    // mixed nor the metadata-only AIR marker reopens it.
    let sent = wss_rpc(
        &mut rpc,
        22,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "please RUN_BG now" }),
    )
    .await;
    assert_eq!(sent["success"], true, "{sent}");
    let turn = turn_events(&mut sub, &agent_id).await;
    let tool_states: Vec<&str> = turn
        .iter()
        .filter(|e| e["type"] == "agent:tool:call" && e["data"]["toolCallId"] == "tool-bg-1")
        .filter_map(|e| e["data"]["status"].as_str())
        .collect();
    assert_eq!(tool_states.last(), Some(&"completed"), "{tool_states:?}");
    let first_done = tool_states
        .iter()
        .position(|s| *s == "completed")
        .expect("completed");
    assert!(
        tool_states[first_done..].iter().all(|s| *s == "completed"),
        "a marker must not reopen the tool: {tool_states:?}"
    );

    let running = await_push(&mut workers, &sub_id, &mut seq, |s| {
        s["workers"].as_array().is_some_and(|w| w.len() == 1)
    })
    .await;
    assert_eq!(running["support"], json!({ "status": "supported" }));
    assert_eq!(running["continuity"], "continuous");
    assert_eq!(running["workspaceId"], json!(ws_id));
    assert_eq!(running["agentId"], json!(agent_id));
    let generation = running["connectionGeneration"]
        .as_str()
        .expect("generation")
        .to_string();
    let session = running["acpSessionId"]
        .as_str()
        .expect("session")
        .to_string();
    let row = &running["workers"][0];
    assert_eq!(
        row["identity"],
        json!({ "workspaceId": ws_id, "agentId": agent_id, "provider": "claude-code",
                "acpSessionId": session, "connectionGeneration": generation, "asyncTaskId": "bg-1" })
    );
    for (field, value) in [
        ("toolCallId", json!("tool-bg-1")),
        ("name", json!("npm run dev")),
        ("taskType", json!("shell")),
        ("canStop", json!(true)),
        ("providerState", json!("running")),
        ("status", json!("running")),
        ("source", json!("claude-air")),
        ("cause", json!("unavailable")),
        ("exitEvidence", json!({ "kind": "unknown" })),
    ] {
        assert_eq!(row[field], value, "{field}: {row}");
    }
    assert!(row.get("unknownReason").is_none(), "{row}");
    assert_eq!(row["firstObservedAt"], row["observedAt"]);
    let init = read_jsonl(&init_log);
    assert_eq!(
        init[0]["clientCapabilities"]["_meta"]["jetbrains"]["air"],
        json!({ "version": 1, "capabilities": ["asyncTasks"] }),
        "the async-tasks extension was requested: {init:?}"
    );
    let listed = wss_rpc(&mut rpc, 23, "agent.workers.list", target.clone()).await;
    assert_eq!(listed["workers"], running["workers"]);
    assert_eq!(listed["connectionGeneration"], json!(generation));

    // Error envelopes (§9): the stop answers are decided before any provider call.
    let stop = |id: i64, generation: &str, task: &str| {
        (
            id,
            json!({ "workspaceId": ws_id, "agentId": agent_id,
                     "connectionGeneration": generation, "asyncTaskId": task }),
        )
    };
    let (id, params) = stop(30, "generation-stale", "bg-1");
    assert_error(
        &wss_call(&mut rpc, id, "agent.workers.stop", params).await,
        -32602,
        "not-found",
    );
    let (id, params) = stop(31, &generation, "missing");
    assert_error(
        &wss_call(&mut rpc, id, "agent.workers.stop", params).await,
        -32602,
        "not-found",
    );
    let (id, params) = stop(32, &generation, "");
    assert_error(
        &wss_call(&mut rpc, id, "agent.workers.stop", params).await,
        -32602,
        "invalid-params",
    );
    assert_error(
        &wss_call(
            &mut rpc,
            33,
            "agent.workers.list",
            json!({ "workspaceId": "ws-elsewhere", "agentId": agent_id }),
        )
        .await,
        -32602,
        "not-found",
    );
    assert_error(
        &wss_call(
            &mut rpc,
            34,
            "agent.workers.list",
            json!({ "workspaceId": ws_id }),
        )
        .await,
        -32602,
        "invalid-params",
    );
    assert!(
        read_jsonl(&stop_log).is_empty(),
        "no provider call for refused stops"
    );

    // Stop through the provider: exactly one `_session/async_task/stop`.
    let (id, params) = stop(35, &generation, "bg-1");
    let stopped = wss_call(&mut rpc, id, "agent.workers.stop", params).await;
    assert_eq!(
        stopped["result"],
        json!({ "asyncTaskId": "bg-1", "stopped": true }),
        "{stopped}"
    );
    assert_eq!(
        read_jsonl(&stop_log),
        vec![json!({ "sessionId": session, "asyncTaskId": "bg-1" })]
    );
    let ended = await_push(&mut workers, &sub_id, &mut seq, |s| {
        s["workers"][0]["status"] == "stopped"
    })
    .await;
    assert_eq!(ended["workers"][0]["providerState"], "stopped");
    assert!(ended["workers"][0]["observedAt"].as_u64() >= row["observedAt"].as_u64());
    let firehose = events_within(&mut sub, &agent_id, Duration::from_secs(2)).await;
    assert!(
        firehose.iter().any(|e| e["type"] == "agent:workers-changed"
            && e["data"]["workers"][0]["status"] == "stopped"),
        "the firehose carries the transient snapshot: {firehose:?}"
    );
    let (id, params) = stop(36, &generation, "bg-1");
    let again = wss_call(&mut rpc, id, "agent.workers.stop", params).await;
    assert_eq!(
        again["result"],
        json!({ "asyncTaskId": "bg-1", "stopped": false })
    );
    assert_eq!(
        read_jsonl(&stop_log).len(),
        1,
        "an ended worker is declined locally"
    );

    // Turn 2: the provider reports completion BETWEEN turns. The projection
    // updates and no implicit turn opens.
    let sent = wss_rpc(
        &mut rpc,
        40,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "RUN_BG AUTO_DONE" }),
    )
    .await;
    assert_eq!(sent["success"], true);
    turn_events(&mut sub, &agent_id).await;
    let completed = await_push(&mut workers, &sub_id, &mut seq, |s| {
        s["workers"].as_array().is_some_and(|w| {
            w.iter()
                .any(|r| r["identity"]["asyncTaskId"] == "bg-2" && r["status"] == "completed")
        })
    })
    .await;
    assert_eq!(completed["workers"].as_array().unwrap().len(), 2);
    let after = events_within(&mut sub, &agent_id, Duration::from_secs(3)).await;
    assert!(
        !after
            .iter()
            .any(|e| e["type"] == "agent:stream:start" || e["type"] == "agent:started"),
        "a worker report must not open a turn: {after:?}"
    );

    // Capacity is per connection; teardown is the shared events.unsubscribe.
    let mut many = connect_ws(port, cfg.clone()).await;
    let mut ids = Vec::new();
    for n in 0..8 {
        let ack = wss_call(
            &mut many,
            100 + n,
            "agent.workers.subscribe",
            target.clone(),
        )
        .await;
        ids.push(
            ack["result"]["subscriptionId"]
                .as_str()
                .expect("admitted")
                .to_string(),
        );
    }
    assert_error(
        &wss_call(&mut many, 120, "agent.workers.subscribe", target.clone()).await,
        -32602,
        "capacity",
    );
    let removed = wss_rpc(
        &mut many,
        121,
        "events.unsubscribe",
        json!({ "subscriptionId": ids[0], "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(removed["success"], true, "{removed}");
    let readmitted = wss_call(&mut many, 122, "agent.workers.subscribe", target.clone()).await;
    assert!(
        readmitted["result"]["subscriptionId"].is_string(),
        "{readmitted}"
    );
    assert_error(
        &wss_call(
            &mut many,
            123,
            "agent.workers.subscribe",
            json!({ "workspaceId": ws_id, "agentId": "" }),
        )
        .await,
        -32602,
        "invalid-params",
    );
}

/// The idle reaper keeps an agent whose provider still runs a background
/// worker: with a sub-second `INTENTD_IDLE_REAP_MS` sweep, an idle claude-code
/// agent with a running shell survives several sweeps (its process, and so
/// the worker, stays alive and the row stays `running` on a continuous
/// connection). Once the worker is stopped the next sweep reaps it with the
/// usual `agent:process:evicted` / `idle-ttl` event.
#[tokio::test]
async fn idle_reap_keeps_an_agent_with_a_running_worker_over_wss() {
    let Some(script) = gate("WSS idle reap with running worker E2E") else {
        return;
    };
    let data_dir_guard = temp_data_dir();
    let data_dir = data_dir_guard.path().to_path_buf();
    let bin_dir = data_dir.join("bin");
    let home_dir = data_dir.join("home");
    std::fs::create_dir_all(&bin_dir).unwrap();
    std::fs::create_dir_all(&home_dir).unwrap();
    write_fake_npx(&bin_dir, &data_dir.join("npx-report"), &script);
    let source_repo = make_source_repo(&data_dir);
    let path = format!("{}:/usr/bin:/bin", bin_dir.display());
    let behavior = json!({
        "response": "KEEPALIVE_E2E_REPLY",
        "agentInfo": {
            "name": "@agentclientprotocol/claude-agent-acp",
            "version": intent_providers::CLAUDE_AGENT_ACP_VERSION,
        },
        "asyncTasks": { "spawnOn": "RUN_BG", "command": "npm run dev" },
    })
    .to_string();
    let env: [(&str, &str); 7] = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("PATH", path.as_str()),
        ("HOME", home_dir.to_str().unwrap()),
        ("SHELL", "/bin/sh"),
        ("MOCK_AGENT_SCRIPT_PATH", &script),
        ("MOCK_AGENT_BEHAVIOR", &behavior),
        // Sub-second TTL + sweep (test-only seam; §13.1).
        ("INTENTD_IDLE_REAP_MS", "500"),
    ];
    let child = spawn_serve(&data_dir, &env);
    let _daemon = Daemon {
        child,
        data_dir: data_dir_guard,
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon did not start");
    let status = common::await_wss_status(&socket).await;
    let port =
        u16::try_from(status["result"]["port"].as_u64().expect("port")).expect("value fits in u16");
    let cfg = client_config(
        status["result"]["fingerprint"]
            .as_str()
            .expect("fingerprint"),
    );

    let mut sub = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut sub,
        1,
        "workspace.create",
        json!({
            "title": "Idle Reap Keep-Alive E2E",
            "repositoryPath": source_repo.to_string_lossy(),
            "repositoryName": "source-repo",
            "baseRef": "main",
            "noPrompt": true,
        }),
    )
    .await;
    let ws_id = created["workspace"]["id"]
        .as_str()
        .expect("workspace id")
        .to_string();
    let subscribed = wss_rpc(
        &mut sub,
        2,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    assert!(subscribed["subscriptionId"].is_string(), "{subscribed}");

    let mut rpc = connect_ws(port, cfg.clone()).await;
    let agent = wss_rpc(
        &mut rpc,
        10,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Keep-Alive E2E", "model": "mock-model", "provider": "claude-code" }),
    )
    .await;
    let agent_id = agent["agent"]["id"].as_str().expect("agent id").to_string();
    let target = json!({ "workspaceId": ws_id, "agentId": agent_id });

    let sent = wss_rpc(
        &mut rpc,
        11,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "please RUN_BG now" }),
    )
    .await;
    assert_eq!(sent["success"], true, "{sent}");
    turn_events(&mut sub, &agent_id).await;

    // Idle past many TTLs with the worker running: never evicted.
    let idle = events_within(&mut sub, &agent_id, Duration::from_secs(4)).await;
    assert!(
        !idle.iter().any(|e| e["type"] == "agent:process:evicted"),
        "an agent with a running worker must not be TTL-reaped: {idle:?}"
    );
    let listed = wss_rpc(&mut rpc, 12, "agent.workers.list", target.clone()).await;
    assert_eq!(listed["continuity"], "continuous", "{listed}");
    assert_eq!(listed["workers"][0]["identity"]["asyncTaskId"], "bg-1");
    assert_eq!(listed["workers"][0]["status"], "running", "{listed}");
    let generation = listed["connectionGeneration"]
        .as_str()
        .expect("generation")
        .to_string();

    // The worker ends: the next sweep reaps the idle agent as usual.
    let stopped = wss_call(
        &mut rpc,
        13,
        "agent.workers.stop",
        json!({ "workspaceId": ws_id, "agentId": agent_id,
                "connectionGeneration": generation, "asyncTaskId": "bg-1" }),
    )
    .await;
    assert_eq!(
        stopped["result"],
        json!({ "asyncTaskId": "bg-1", "stopped": true }),
        "{stopped}"
    );
    let mut evicted = None;
    for _ in 0..30 {
        let batch = events_within(&mut sub, &agent_id, Duration::from_secs(1)).await;
        if let Some(e) = batch
            .into_iter()
            .find(|e| e["type"] == "agent:process:evicted")
        {
            evicted = Some(e);
            break;
        }
    }
    let evicted = evicted.expect("the agent is reaped once its worker ended");
    assert_eq!(evicted["data"]["reason"], "idle-ttl", "{evicted}");
}
