//! Remote launch end to end: a real `intentd serve` head (WSS, the
//! experimental switch on, `remote-node.json` configured) and a real
//! `intentd node` process on loopback. An agent created over WSS with a
//! remote placement is launched on the node on its first message: the node
//! fetches the workspace's last commit over `git-remote-intent` into its own
//! worktree, runs the provider (the `node-test-agent.mjs` fixture), streams
//! the turn back into head's transcript, and on stop pushes the agent's
//! work, which lands as its `wip` ref in the workspace repository.
//!
//! Also pins that no node capability is advertised to clients.
//!
//! Gated on `node` and `git` on PATH; skips cleanly otherwise.

#![cfg(unix)]

use std::io::BufRead as _;
use std::path::Path;
use std::process::{Command, Stdio};
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
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;

use crate::common;

const TOKEN: &str = "abababababababababababababababababababababababababababababababab";
const NODE_ID: &str = "node-e2e";
const NODE_TOKEN: &str = "node-e2e-lease-token-0123456789abcdef";

fn wait() -> Duration {
    common::test_timeout(Duration::from_secs(60))
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

async fn connect(port: u16, cfg: Arc<ClientConfig>) -> common::TlsWs {
    let url = format!("wss://localhost:{port}/ws?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

/// One JSON-RPC exchange; the whole response envelope.
async fn rpc(ws: &mut common::TlsWs, id: i64, method: &str, params: Value) -> Value {
    let frame = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });
    ws.send(Message::Text(frame.to_string().into()))
        .await
        .expect("send rpc frame");
    loop {
        let next = timeout(wait(), ws.next()).await.expect("rpc timed out");
        match next {
            Some(Ok(Message::Text(text))) => {
                let v: Value = serde_json::from_str(&text).expect("json frame");
                if v["id"] == json!(id) {
                    assert_eq!(v["jsonrpc"], "2.0", "{v}");
                    return v;
                }
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            Some(Ok(_)) => {}
            other => panic!("socket closed during {method}: {other:?}"),
        }
    }
}

async fn ok(ws: &mut common::TlsWs, id: i64, method: &str, params: Value) -> Value {
    let v = rpc(ws, id, method, params).await;
    assert!(v.get("error").is_none(), "{method} errored: {v}");
    v["result"].clone()
}

/// The next `events.event` whose type is `kind`.
async fn event(ws: &mut common::TlsWs, kind: &str) -> Value {
    timeout(wait(), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    if v["method"] == "events.event" && v["params"]["event"]["type"] == kind {
                        return v["params"]["event"].clone();
                    }
                }
                Some(Ok(Message::Ping(p))) => {
                    let _ = ws.send(Message::Pong(p)).await;
                }
                Some(Ok(_)) => {}
                other => panic!("event socket closed: {other:?}"),
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("no {kind} event"))
}

fn git(dir: &Path, args: &[&str]) -> String {
    let output = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["-c", "user.name=t", "-c", "user.email=t@t"])
        .args(args)
        .output()
        .unwrap();
    assert!(output.status.success(), "git {args:?}: {output:?}");
    String::from_utf8_lossy(&output.stdout).trim().to_owned()
}

fn try_rev(dir: &Path, name: &str) -> Option<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(["rev-parse", "--verify", "--quiet", name])
        .output()
        .unwrap();
    output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn write_private(path: &Path, value: &Value) {
    use std::os::unix::fs::PermissionsExt as _;
    std::fs::write(path, value.to_string()).unwrap();
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).unwrap();
}

/// Start `intentd node` with the fixture provider; return it with its
/// listen address and certificate pin (its first stdout line).
fn start_node(dir: &Path, node_bin: &Path) -> (GuardedChild, String, String) {
    let fixture = format!(
        "{}/tests/fixtures/node-test-agent.mjs",
        env!("CARGO_MANIFEST_DIR")
    );
    let config = dir.join("node.json");
    write_private(
        &config,
        &json!({
            "nodeId": NODE_ID,
            "listen": "127.0.0.1:0",
            "token": NODE_TOKEN,
            "maxAgents": 2,
            "permission": "allowAll",
            "providers": { "mock": { "binary": node_bin, "args": [fixture] } },
        }),
    );
    let log = std::fs::File::create(dir.join("node.log")).unwrap();
    let mut command = Command::new(env!("CARGO_BIN_EXE_intentd"));
    command
        .arg("node")
        .arg("--config")
        .arg(&config)
        .arg("--root")
        .arg(dir.join("root"))
        .env_remove("INTENTD_DATA_DIR")
        .stdout(Stdio::piped())
        .stderr(Stdio::from(log));
    let mut child = GuardedChild::spawn(&mut command).expect("spawn intentd node");
    let stdout = child.stdout.take().unwrap();
    let mut line = String::new();
    std::io::BufReader::new(stdout)
        .read_line(&mut line)
        .expect("node prints its address");
    let started: Value = serde_json::from_str(&line).expect("node start line is JSON");
    (
        child,
        started["address"].as_str().unwrap().to_owned(),
        started["certificateSha256"].as_str().unwrap().to_owned(),
    )
}

async fn seed_workspace(data_dir: &Path, repo: &Path) -> String {
    use intent_core::{now_iso, Workspace, WorkspaceId};
    let store = intent_store::Store::open(&data_dir.join("intentd.db"))
        .await
        .expect("open store");
    let ws = WorkspaceId::new();
    let ts = now_iso();
    store
        .insert_workspace(&Workspace {
            id: ws.clone(),
            title: "Remote E2E".to_string(),
            branch: "main".to_string(),
            repository_path: Some(repo.to_string_lossy().into_owned()),
            skip_worktree: true,
            created_at: ts.clone(),
            updated_at: ts,
            ..Default::default()
        })
        .await
        .expect("insert workspace");
    ws.0
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_agent_launches_streams_stops_and_leaves_its_wip_ref() {
    let Some(node_bin) = intent_providers::resolve_on_path("node") else {
        eprintln!("skipping remote launch e2e: node not on PATH");
        return;
    };
    if intent_providers::resolve_on_path("git").is_none() {
        eprintln!("skipping remote launch e2e: git not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-remote-");
    let repo = scratch.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    std::fs::write(repo.join("README.md"), "base\n").unwrap();
    git(&repo, &["add", "README.md"]);
    git(&repo, &["commit", "--quiet", "-m", "base"]);
    let base = git(&repo, &["rev-parse", "HEAD"]);
    // Uncommitted on head: never carried to the node.
    std::fs::write(repo.join("README.md"), "dirty on head\n").unwrap();

    let node_dir = scratch.path().join("node");
    std::fs::create_dir_all(&node_dir).unwrap();
    let (_node, address, pin) = start_node(&node_dir, &node_bin);

    let data_dir = scratch.path().join("head");
    std::fs::create_dir_all(data_dir.join("workspaces")).unwrap();
    let ws_id = seed_workspace(&data_dir, &repo).await;
    write_private(
        &data_dir.join("remote-node.json"),
        &json!({
            "nodeId": NODE_ID,
            "name": "E2E node",
            "endpoint": format!("wss://{address}"),
            "certificateSha256": pin,
            "token": NODE_TOKEN,
        }),
    );
    common::enable_ws_api(&data_dir);
    let mut serve = common::hermetic_serve_command(&data_dir);
    serve
        .env("INTENTD_DATA_DIR", &data_dir)
        .env("INTENTD_WORKSPACES_DIR", data_dir.join("workspaces"))
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .env("INTENTD_EXPERIMENTAL_REMOTE_LAUNCH", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::from(
            std::fs::File::create(scratch.path().join("head.log")).unwrap(),
        ));
    common::hermetic_fixture_identity(&mut serve, &data_dir);
    let _head = common::DaemonGuard::process_only(
        common::spawn_guarded(&mut serve).expect("spawn intentd serve"),
    );
    let socket = data_dir.join("intentd.sock");
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());

    let mut rpc_ws = connect(port, cfg.clone()).await;
    let hello = ok(
        &mut rpc_ws,
        1,
        "client.hello",
        json!({ "clientId": "remote-e2e", "name": "Remote E2E" }),
    )
    .await;
    let capabilities = &hello["server"]["capabilities"];
    for capability in ["agentNodes", "agentPlatformRouting", "localNodeIsolation"] {
        assert!(
            capabilities.get(capability).is_none(),
            "{capability} must not be advertised: {capabilities}"
        );
    }

    let mut events = connect(port, cfg.clone()).await;
    ok(
        &mut events,
        2,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;

    // The node binds asynchronously after head starts: until then the
    // placement is unavailable.
    let create = json!({
        "workspaceId": ws_id,
        "name": "Remote",
        "model": "default",
        "provider": "mock",
        "placement": { "target": "remote" },
    });
    let deadline = tokio::time::Instant::now() + wait();
    let mut id = 10;
    let created = loop {
        id += 1;
        let response = rpc(&mut rpc_ws, id, "agent.create", create.clone()).await;
        if response.get("error").is_none() {
            break response["result"].clone();
        }
        assert_eq!(
            response["error"]["data"]["code"], "placement-unavailable",
            "{response}"
        );
        assert!(
            tokio::time::Instant::now() < deadline,
            "node never bound: {response}"
        );
        // timing-guard: poll interval while the separate node process binds to head (no event exposes it)
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let agent = &created["agent"];
    let agent_id = agent["id"].as_str().unwrap().to_owned();
    assert_eq!(agent["nodeId"], NODE_ID);
    assert_eq!(agent["nodeName"], "E2E node");
    assert_eq!(agent["effectiveIsolation"], "pending");
    assert_eq!(agent["placement"]["target"], "remote");
    assert_eq!(agent["placement"]["nodeId"], NODE_ID);
    assert!(
        created["notice"]
            .as_str()
            .unwrap()
            .contains("Uncommitted changes in the workspace are not carried"),
        "{created}"
    );
    let refs = format!("refs/intent/ws/{ws_id}/agents/{agent_id}");
    assert_eq!(try_rev(&repo, &format!("{refs}/base")), Some(base.clone()));

    // `null` is not "omitted": the closed placement object is required.
    let null = rpc(
        &mut rpc_ws,
        89,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Null", "model": "default", "provider": "mock", "placement": null }),
    )
    .await;
    assert_eq!(null["error"]["code"], -32602, "{null}");

    // A placement no node matches is refused with the documented data.
    let refused = rpc(
        &mut rpc_ws,
        90,
        "agent.create",
        json!({
            "workspaceId": ws_id, "name": "Nowhere", "model": "default", "provider": "mock",
            "placement": { "target": "remote", "os": if cfg!(target_os = "linux") { "macos" } else { "linux" } },
        }),
    )
    .await;
    assert_eq!(refused["error"]["code"], -32602, "{refused}");
    assert_eq!(refused["error"]["data"]["reason"], "unsupported-platform");
    assert!(refused["error"]["data"]["supportedPlatforms"].is_array());

    // First message: launch on the node, stream the turn back.
    ok(
        &mut rpc_ws,
        91,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "hello remote" }),
    )
    .await;
    let end = event(&mut events, "agent:stream:end").await;
    assert_eq!(end["data"]["agentId"], agent_id, "{end}");
    let got = ok(
        &mut rpc_ws,
        92,
        "agent.get",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await["agent"]
        .clone();
    let reply = got["lastAgentResponse"].as_str().unwrap_or_default();
    assert!(reply.contains("echo:"), "{got}");
    assert!(reply.contains("hello remote"), "{got}");
    // The agent's instructions went with its first prompt, persisted as its
    // system prompt.
    let session = ok(
        &mut rpc_ws,
        94,
        "agent.getSession",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let system_prompt = session["systemPrompt"]
        .as_str()
        .or_else(|| session["session"]["systemPrompt"].as_str())
        .unwrap_or_default();
    assert!(
        !system_prompt.trim().is_empty(),
        "the remote agent's system prompt is assembled and stored: {session}"
    );
    assert_eq!(got["effectiveIsolation"], "isolated", "{got}");

    // Stop: the node pushes the agent's work before it exits.
    ok(
        &mut rpc_ws,
        93,
        "agent.stop",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let wip = timeout(wait(), async {
        loop {
            if let Some(wip) = try_rev(&repo, &format!("{refs}/wip")) {
                return wip;
            }
            // timing-guard: poll interval for the node process's git push to land the ref (no event exposes it)
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the wip ref lands on head");
    assert_eq!(try_rev(&repo, &format!("{refs}/head")), Some(base.clone()));
    assert_eq!(git(&repo, &["rev-parse", &format!("{wip}^")]), base);
    // The node's worktree started from the commit, not head's dirty tree:
    // its README is the committed one.
    assert_eq!(git(&repo, &["show", &format!("{wip}:README.md")]), "base");
    let wip_files = git(&repo, &["ls-tree", "--name-only", &wip]);
    assert!(wip_files.lines().any(|f| f == "agent.pid"), "{wip_files}");
    assert_eq!(
        std::fs::read_to_string(repo.join("README.md")).unwrap(),
        "dirty on head\n"
    );
}
