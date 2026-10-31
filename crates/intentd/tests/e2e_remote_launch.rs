//! Remote launch end to end: a real `intentd serve` backend (WSS,
//! `experimental.remoteAgents` on) and a second real `intentd serve` as the
//! machine (remote agents on), both on loopback. The machine mints an
//! executor credential for the backend's id (`remoteMachines.backendInfo`)
//! and the backend adds it with `remoteMachines.add`, as the frontend relays
//! it. An agent created over WSS with a
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

use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
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

/// The machine: a regular `intentd serve` running the fixture provider as
/// `mock`, with remote agents on and an executor credential minted for the
/// backend `backend_id`.
struct Machine {
    _daemon: common::DaemonGuard,
    port: u16,
    fingerprint: String,
    daemon_id: String,
    credential_id: String,
    token: String,
}

async fn start_machine(dir: &Path, backend_id: &str) -> Machine {
    let fixture = format!(
        "{}/tests/fixtures/node-test-agent.mjs",
        env!("CARGO_MANIFEST_DIR")
    );
    std::fs::create_dir_all(dir.join("workspaces")).unwrap();
    common::enable_ws_api(dir);
    let mut serve = common::hermetic_serve_command(dir);
    serve
        .env("INTENTD_DATA_DIR", dir)
        .env("INTENTD_WORKSPACES_DIR", dir.join("workspaces"))
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .env("MOCK_AGENT_SCRIPT_PATH", &fixture)
        .stdout(Stdio::null())
        .stderr(Stdio::from(
            std::fs::File::create(dir.join("machine.log")).unwrap(),
        ));
    common::hermetic_fixture_identity(&mut serve, dir);
    let daemon = common::DaemonGuard::process_only(
        common::spawn_guarded(&mut serve).expect("spawn the machine's intentd serve"),
    );
    let status = common::await_wss_status(&dir.join("intentd.sock")).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let fingerprint = status["result"]["fingerprint"].as_str().unwrap().to_owned();
    let mut ws = connect(port, client_config(&fingerprint)).await;
    ok(
        &mut ws,
        1,
        "settings.update",
        json!({ "changes": [{ "path": "remoteAgents.enabled", "value": true }] }),
    )
    .await;
    let minted = ok(
        &mut ws,
        2,
        "executor.mint",
        json!({ "backendId": backend_id, "backendName": "E2E head" }),
    )
    .await;
    Machine {
        _daemon: daemon,
        port,
        fingerprint,
        daemon_id: minted["machine"]["daemonId"].as_str().unwrap().to_owned(),
        credential_id: minted["credentialId"].as_str().unwrap().to_owned(),
        token: minted["token"].as_str().unwrap().to_owned(),
    }
}

/// Send `content` to the agent, wait for its turn to end, and return its
/// reply. `id` and `id + 1` are the RPC ids used.
async fn turn(
    rpc_ws: &mut common::TlsWs,
    events: &mut common::TlsWs,
    (ws_id, agent_id): (&str, &str),
    content: &str,
    id: i64,
) -> String {
    ok(
        rpc_ws,
        id,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": content }),
    )
    .await;
    let end = event(events, "agent:stream:end").await;
    assert_eq!(end["data"]["agentId"], agent_id, "{end}");
    let got = ok(
        rpc_ws,
        id + 1,
        "agent.get",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    got["agent"]["lastAgentResponse"]
        .as_str()
        .unwrap_or_default()
        .to_owned()
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
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping remote launch e2e: node not on PATH");
        return;
    }
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

    let data_dir = scratch.path().join("head");
    std::fs::create_dir_all(data_dir.join("workspaces")).unwrap();
    let ws_id = seed_workspace(&data_dir, &repo).await;
    common::enable_ws_api(&data_dir);
    let mut serve = common::hermetic_serve_command(&data_dir);
    serve
        .env("INTENTD_DATA_DIR", &data_dir)
        .env("INTENTD_WORKSPACES_DIR", data_dir.join("workspaces"))
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .env("INTENTD_AUTH_TOKEN", TOKEN)
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
    ok(
        &mut rpc_ws,
        3,
        "settings.update",
        json!({ "changes": [{ "path": "experimental.remoteAgents", "value": true }] }),
    )
    .await;
    let backend = ok(&mut rpc_ws, 4, "remoteMachines.backendInfo", json!({})).await;
    let backend_id = backend["backendId"].as_str().unwrap().to_owned();

    // Expose the machine to this backend, as the frontend relays it: mint on
    // the machine, then add on the backend, which probes before saving.
    let node_dir = scratch.path().join("node");
    std::fs::create_dir_all(&node_dir).unwrap();
    let machine = start_machine(&node_dir, &backend_id).await;
    let node_id = machine.daemon_id.clone();
    let added = ok(
        &mut rpc_ws,
        5,
        "remoteMachines.add",
        json!({
            "machineId": node_id,
            "name": "E2E node",
            "hosts": ["127.0.0.1"],
            "port": machine.port,
            "fingerprint": machine.fingerprint,
            "credentialId": machine.credential_id,
            "token": machine.token,
        }),
    )
    .await;
    assert_eq!(added["machine"]["id"], node_id.as_str(), "{added}");
    assert!(!added.to_string().contains(&machine.token));

    let mut events = connect(port, cfg.clone()).await;
    ok(
        &mut events,
        2,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;

    // The backend connects to the machine on demand at placement; retry
    // while that first connect is still in flight.
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
            "the machine never took the placement: {response}"
        );
        // timing-guard: poll interval while the machine daemon binds to head (no event exposes it)
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let agent = &created["agent"];
    let agent_id = agent["id"].as_str().unwrap().to_owned();
    assert_eq!(agent["nodeId"], node_id.as_str());
    assert_eq!(agent["nodeName"], "E2E node");
    assert_eq!(agent["effectiveIsolation"], "pending");
    assert_eq!(agent["placement"]["target"], "remote");
    assert_eq!(agent["placement"]["nodeId"], node_id.as_str());
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

    // workspace_api on the node: a note goes to head as the agent, a file
    // write lands in the node's worktree (and comes back in the wip ref),
    // and a namespace the node cannot serve fails by name.
    let wsapi = r#"wsapi const note = await ws.note.create("From the node", "remote body"); await ws.file.write("remote.txt", "node work"); return JSON.stringify({ note: note.id });"#;
    let reply = turn(&mut rpc_ws, &mut events, (&ws_id, &agent_id), wsapi, 95).await;
    assert!(reply.starts_with("wsapi: "), "{reply}");
    let notes = ok(
        &mut rpc_ws,
        97,
        "note.list",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert!(
        notes.to_string().contains("From the node"),
        "the forwarded note is on head: {notes}"
    );
    let refused = turn(
        &mut rpc_ws,
        &mut events,
        (&ws_id, &agent_id),
        "wsapi return await ws.script.list();",
        98,
    )
    .await;
    assert!(
        refused.starts_with("wsapi-error: ")
            && refused.contains("is not supported on remote agents yet"),
        "{refused}"
    );

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
            // timing-guard: poll interval for the machine's git push to land the ref (no event exposes it)
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
    assert_eq!(
        git(&repo, &["show", &format!("{wip}:remote.txt")]),
        "node work"
    );
    // The provider reached the node's workspace API the way a real one
    // does: through the node's own `intentd mcp-bridge`.
    let bridge: Value =
        serde_json::from_str(&git(&repo, &["show", &format!("{wip}:mcp-bridge.json")])).unwrap();
    assert!(
        bridge["command"].as_str().unwrap().ends_with("intentd"),
        "{bridge}"
    );
    assert_eq!(bridge["args"][0], "mcp-bridge", "{bridge}");
    assert_eq!(bridge["args"][1], "--connect", "{bridge}");
    let wip_files = git(&repo, &["ls-tree", "--name-only", &wip]);
    assert!(wip_files.lines().any(|f| f == "agent.pid"), "{wip_files}");
    assert_eq!(
        std::fs::read_to_string(repo.join("README.md")).unwrap(),
        "dirty on head\n"
    );
}

/// A backend `intentd serve` on `data_dir` with a seeded workspace on
/// `repo`; its guard, WSS port, pinned client config and workspace id.
async fn start_backend(
    data_dir: &Path,
    repo: &Path,
) -> (common::DaemonGuard, u16, Arc<ClientConfig>, String) {
    std::fs::create_dir_all(data_dir.join("workspaces")).unwrap();
    let ws_id = seed_workspace(data_dir, repo).await;
    common::enable_ws_api(data_dir);
    let mut serve = common::hermetic_serve_command(data_dir);
    serve
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", data_dir.join("workspaces"))
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .stdout(Stdio::null())
        .stderr(Stdio::from(
            std::fs::File::create(data_dir.join("head.log")).unwrap(),
        ));
    common::hermetic_fixture_identity(&mut serve, data_dir);
    let guard = common::DaemonGuard::process_only(
        common::spawn_guarded(&mut serve).expect("spawn intentd serve"),
    );
    let status = common::await_wss_status(&data_dir.join("intentd.sock")).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
    (guard, port, cfg, ws_id)
}

/// `remoteMachines.add` params for `machine`, named `name`.
fn add_params(machine: &Machine, name: &str) -> Value {
    json!({
        "machineId": machine.daemon_id,
        "name": name,
        "hosts": ["127.0.0.1"],
        "port": machine.port,
        "fingerprint": machine.fingerprint,
        "credentialId": machine.credential_id,
        "token": machine.token,
    })
}

/// Placement across two exposed machines over WSS: the `remoteMachines`
/// capability follows `experimental.remoteAgents`; a named machine takes
/// the agent; a model no machine lists is `model-unavailable` naming each
/// machine's models and sign-in status; an unknown `nodeId` is
/// `machine-unknown`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn placement_chooses_among_exposed_machines() {
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping placement e2e: node not on PATH");
        return;
    }
    if intent_providers::resolve_on_path("git").is_none() {
        eprintln!("skipping placement e2e: git not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-place-");
    let repo = scratch.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    std::fs::write(repo.join("README.md"), "base\n").unwrap();
    git(&repo, &["add", "README.md"]);
    git(&repo, &["commit", "--quiet", "-m", "base"]);
    let (_head, port, cfg, ws_id) = start_backend(&scratch.path().join("head"), &repo).await;

    let mut rpc_ws = connect(port, cfg.clone()).await;
    let hello = json!({ "clientId": "placement-e2e", "name": "Placement E2E" });
    let off = ok(&mut rpc_ws, 1, "client.hello", hello.clone()).await;
    assert!(
        off["server"]["capabilities"]
            .get("remoteMachines")
            .is_none(),
        "{off}"
    );
    ok(
        &mut rpc_ws,
        2,
        "settings.update",
        json!({ "changes": [{ "path": "experimental.remoteAgents", "value": true }] }),
    )
    .await;
    let mut fresh = connect(port, cfg.clone()).await;
    let on = ok(&mut fresh, 1, "client.hello", hello).await;
    assert_eq!(on["server"]["capabilities"]["remoteMachines"], 1, "{on}");

    let backend = ok(&mut rpc_ws, 3, "remoteMachines.backendInfo", json!({})).await;
    let backend_id = backend["backendId"].as_str().unwrap().to_owned();
    let mut machines = Vec::new();
    for (i, name) in ["Machine A", "Machine B"].into_iter().enumerate() {
        let dir = scratch.path().join(format!("node-{i}"));
        std::fs::create_dir_all(&dir).unwrap();
        let machine = start_machine(&dir, &backend_id).await;
        let id = 4 + i64::try_from(i).unwrap();
        ok(
            &mut rpc_ws,
            id,
            "remoteMachines.add",
            add_params(&machine, name),
        )
        .await;
        machines.push(machine);
    }
    let b_id = machines[1].daemon_id.clone();

    // A named machine takes the agent, never another one.
    let create = |name: &str, model: &str, placement: Value| {
        json!({
            "workspaceId": ws_id, "name": name, "model": model, "provider": "mock",
            "placement": placement,
        })
    };
    // A machine's first report can take a while after remote agents were
    // turned on; its refusal is retryable until then.
    let deadline = tokio::time::Instant::now() + wait();
    let mut id = 100;
    let created = loop {
        id += 1;
        let response = rpc(
            &mut rpc_ws,
            id,
            "agent.create",
            create(
                "On B",
                "default",
                json!({ "target": "remote", "nodeId": b_id }),
            ),
        )
        .await;
        if response.get("error").is_none() {
            break response["result"].clone();
        }
        assert_eq!(response["error"]["data"]["retryable"], true, "{response}");
        assert!(
            tokio::time::Instant::now() < deadline,
            "machine B never took the placement: {response}"
        );
        // timing-guard: poll interval while the machine observes its providers (no event exposes it)
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    assert_eq!(created["agent"]["nodeId"], b_id.as_str(), "{created}");
    assert_eq!(created["agent"]["nodeName"], "Machine B", "{created}");

    // Unplaced, either machine can take it.
    let any = ok(
        &mut rpc_ws,
        11,
        "agent.create",
        create("Anywhere", "default", json!({ "target": "remote" })),
    )
    .await;
    let chosen = any["agent"]["nodeId"].as_str().unwrap().to_owned();
    assert!(
        machines.iter().any(|machine| machine.daemon_id == chosen),
        "{any}"
    );

    // A model no machine lists: refused before anything is created.
    let missing = rpc(
        &mut rpc_ws,
        12,
        "agent.create",
        create("Missing", "no-such-model", json!({ "target": "remote" })),
    )
    .await;
    assert_eq!(missing["error"]["code"], -32602, "{missing}");
    let data = &missing["error"]["data"];
    assert_eq!(data["code"], "model-unavailable", "{missing}");
    assert_eq!(data["provider"], "mock");
    assert_eq!(data["model"], "no-such-model");
    let listed = data["machines"].as_array().unwrap();
    assert_eq!(listed.len(), 2, "{missing}");
    for machine in listed {
        assert!(machine["status"].is_string(), "{machine}");
        assert!(machine["models"].is_array(), "{machine}");
        assert!(machine["observedAt"].is_string(), "{machine}");
    }
    let message = missing["error"]["message"].as_str().unwrap();
    assert!(
        message.contains("isn't available on Machine A") && message.contains("Machine B"),
        "{message}"
    );

    // An unknown machine.
    let unknown = rpc(
        &mut rpc_ws,
        13,
        "agent.create",
        create(
            "Unknown",
            "default",
            json!({ "target": "remote", "nodeId": "no-such-machine" }),
        ),
    )
    .await;
    assert_eq!(
        unknown["error"]["data"]["code"], "placement-unavailable",
        "{unknown}"
    );
    assert_eq!(
        unknown["error"]["data"]["reason"], "machine-unknown",
        "{unknown}"
    );
    assert_eq!(
        unknown["error"]["data"]["nodeId"], "no-such-machine",
        "{unknown}"
    );

    for machine in &machines {
        assert!(!missing.to_string().contains(&machine.token));
    }
}
