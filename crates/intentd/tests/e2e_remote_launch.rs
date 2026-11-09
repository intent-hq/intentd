//! Remote launch end to end: a real `intentd serve` backend (WSS) and a
//! second real `intentd serve` as the machine, both on loopback and with no
//! remote-agent setting turned on (none exists). The machine mints an
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

mod cloud;
mod forge_push;
mod forge_token;
mod git_identity;
mod secrets;
mod sub_agents;

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

#[track_caller]
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
    daemon: common::DaemonGuard,
    port: u16,
    fingerprint: String,
    daemon_id: String,
    credential_id: String,
    token: String,
}

async fn start_machine(dir: &Path, backend_id: &str) -> Machine {
    start_machine_with(dir, backend_id, &[]).await
}

/// [`start_machine`] with extra environment for the machine process.
async fn start_machine_with(dir: &Path, backend_id: &str, env: &[(&str, &str)]) -> Machine {
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
        // An identity the test runner inherited must not reach the agents.
        .env_remove("GIT_AUTHOR_NAME")
        .env_remove("GIT_AUTHOR_EMAIL")
        .env_remove("GIT_COMMITTER_NAME")
        .env_remove("GIT_COMMITTER_EMAIL")
        .envs(env.iter().copied())
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
    let minted = ok(
        &mut ws,
        2,
        "executor.mint",
        json!({ "backendId": backend_id, "backendName": "E2E head" }),
    )
    .await;
    Machine {
        daemon,
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
    seed_workspace_on(data_dir, repo, false).await
}

/// [`seed_workspace`]; with `worktree`, `repo` is the workspace's own
/// worktree (so its setup script applies) instead of a shared checkout.
async fn seed_workspace_on(data_dir: &Path, repo: &Path, worktree: bool) -> String {
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
            worktree_path: worktree.then(|| repo.to_string_lossy().into_owned()),
            base_ref: worktree.then(|| "main".to_owned()),
            skip_worktree: !worktree,
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
    let first = ok(
        &mut rpc_ws,
        91,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "hello remote" }),
    )
    .await;
    let first_message = first["messageId"].as_str().unwrap().to_owned();
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
    // A message whose images cannot cross the link is refused up front, to
    // a running remote agent and to a new one; nothing is queued or created.
    let (text, images) = oversized_prompt_images();
    let too_large = |response: &Value| {
        assert_eq!(response["error"]["code"], -32602, "{response}");
        assert!(
            response["error"]["message"]
                .as_str()
                .is_some_and(|m| m.contains("attachments-too-large-for-machine")),
            "{response}"
        );
    };
    for (id, method) in [(1001, "agent.sendMessage"), (1002, "agent.queueMessage")] {
        let response = rpc(
            &mut rpc_ws,
            id,
            method,
            json!({ "workspaceId": ws_id, "agentId": agent_id, "content": text, "imageBlocks": images }),
        )
        .await;
        too_large(&response);
    }
    // Editing an earlier message is refused before the transcript is cut.
    let response = rpc(
        &mut rpc_ws,
        1007,
        "agent.editAndRegenerate",
        json!({
            "workspaceId": ws_id, "agentId": agent_id, "messageId": first_message,
            "content": text, "imageBlocks": images,
        }),
    )
    .await;
    too_large(&response);
    let kept = ok(
        &mut rpc_ws,
        1008,
        "agent.get",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert!(
        kept["agent"]["lastAgentResponse"]
            .as_str()
            .is_some_and(|reply| reply.contains("hello remote")),
        "{kept}"
    );
    let agents = |list: &Value| list["agents"].as_array().map(Vec::len);
    let listed = ok(
        &mut rpc_ws,
        1003,
        "agent.list",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    let response = rpc(
        &mut rpc_ws,
        1004,
        "agent.create",
        json!({
            "workspaceId": ws_id, "name": "Pictures", "model": "default", "provider": "mock",
            "placement": { "target": "remote" },
            "imageBlocks": images,
            "metadata": { "initialMessage": text },
        }),
    )
    .await;
    too_large(&response);
    let relisted = ok(
        &mut rpc_ws,
        1005,
        "agent.list",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(agents(&relisted), agents(&listed), "{relisted}");
    assert!(agents(&listed).is_some(), "{listed}");
    let queue = ok(
        &mut rpc_ws,
        1006,
        "agent.getQueue",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert!(!queue.to_string().contains(&text[..64]), "{queue}");
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
    remote_workspace_setters_and_proposal(&mut rpc_ws, &mut events, (&ws_id, &agent_id)).await;
    // Saved scripts (`remoteScripts: 1`): the agent's runs are machine runs.
    let service_id = machine_runs(
        &mut rpc_ws,
        &mut events,
        (&ws_id, &agent_id),
        &node_id,
        &repo,
    )
    .await;

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

    // Lost on disconnect: a live machine run of a machine that goes away
    // ends `exited`, `-1`, `remote-script-lost: …`, and is not reattached.
    // A fresh remote agent starts the saved service (any agent may start any
    // saved script), so the step does not depend on relaunching the agent
    // stopped above.
    let mut create_id = 290;
    let deadline = tokio::time::Instant::now() + wait();
    let second = loop {
        create_id += 1;
        let response = rpc(
            &mut rpc_ws,
            create_id,
            "agent.create",
            json!({
                "workspaceId": ws_id,
                "name": "Remote 2",
                "model": "default",
                "provider": "mock",
                "placement": { "target": "remote" },
            }),
        )
        .await;
        if response.get("error").is_none() {
            break response["result"]["agent"]["id"]
                .as_str()
                .unwrap()
                .to_owned();
        }
        assert_eq!(
            response["error"]["data"]["code"], "placement-unavailable",
            "{response}"
        );
        assert!(
            tokio::time::Instant::now() < deadline,
            "the machine never took the placement: {response}"
        );
        // timing-guard: poll interval while the backend redials the machine (no event exposes it)
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let started = wsapi_turn(
        &mut rpc_ws,
        &mut events,
        (&ws_id, &second),
        &format!(
            "wsapi const st = await ws.script.start({service_id:?}); return JSON.stringify({{ marker: \"turn-300\", st }});"
        ),
        "turn-300",
        300,
    )
    .await;
    assert!(started.starts_with("wsapi: "), "{started}");
    machine_run_until(&mut rpc_ws, (&ws_id, &service_id, &second), 310, |run| {
        run["runtime"]["status"] == "running"
    })
    .await;
    drop(machine);
    let lost = machine_run_until(&mut rpc_ws, (&ws_id, &service_id, &second), 400, |run| {
        run["runtime"]["status"] == "exited"
    })
    .await;
    assert_eq!(lost["runtime"]["exitCode"], -1, "{lost}");
    assert!(
        lost["runtime"]["error"]
            .as_str()
            .unwrap_or_default()
            .starts_with("remote-script-lost:"),
        "{lost}"
    );
}

/// intent-hq/intent#8593: a remote agent sets the workspace status, status
/// image and title and its own name through head (the explicit name and
/// the seeded title are kept, as for a local agent), and proposes a
/// sibling workspace. Its code discards the card, so only the batch the
/// machine publishes to head can put it on the agent's message in head's
/// history as a pending, Apply-able card.
async fn remote_workspace_setters_and_proposal(
    rpc_ws: &mut common::TlsWs,
    events: &mut common::TlsWs,
    (ws_id, agent_id): (&str, &str),
) {
    let setters = wsapi_turn(
        rpc_ws,
        events,
        (ws_id, agent_id),
        r#"wsapi const name = await ws.workspace.setAgentName("Remote namer"); const status = await ws.workspace.setStatusMessage("Working on the machine."); const image = await ws.workspace.setStatusImage({ data: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg==", mimeType: "image/png" }); const title = await ws.workspace.setTitle("Remote title"); return JSON.stringify({ marker: "turn-setters", name, status, image, title });"#,
        "turn-setters",
        600,
    )
    .await;
    let value = wsapi_value(&setters);
    assert_eq!(
        value["status"]["statusMessage"], "Working on the machine.",
        "{value}"
    );
    assert_eq!(
        value["title"]["skipped"], true,
        "the seeded title is kept: {value}"
    );
    assert_eq!(value["name"]["success"], true, "{value}");
    assert_eq!(
        value["name"]["skipped"], true,
        "the explicit name is kept: {value}"
    );
    let image = value["image"]["statusImageAssetId"]
        .as_str()
        .unwrap_or_default();
    assert!(!image.is_empty(), "{value}");
    let workspace = ok(
        rpc_ws,
        610,
        "workspace.get",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(
        workspace["workspace"]["statusMessage"], "Working on the machine.",
        "{workspace}"
    );
    assert_eq!(
        workspace["workspace"]["statusImageAssetId"], image,
        "{workspace}"
    );

    let proposed = wsapi_turn(
        rpc_ws,
        events,
        (ws_id, agent_id),
        r#"wsapi-tool const p = await ws.workspace.proposeSibling({ title: "Remote follow-up", initialPrompt: "Continue the remote work." }); return JSON.stringify({ marker: "turn-proposal", id: p.proposalId });"#,
        "turn-proposal",
        620,
    )
    .await;
    let returned = wsapi_value(&proposed);
    let pending = timeout(wait(), async {
        let mut id = 630;
        loop {
            let agent = ok(
                rpc_ws,
                id,
                "agent.get",
                json!({ "workspaceId": ws_id, "agentId": agent_id }),
            )
            .await;
            id += 1;
            let pending = &agent["agent"]["metadata"]["pendingProposals"];
            if pending.as_array().is_some_and(|p| !p.is_empty()) {
                return pending.clone();
            }
            // timing-guard: poll interval for the turn's message to be recorded with its card
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the remote proposal is pending on head");
    assert_eq!(pending[0]["proposalId"], returned["id"], "{pending}");
    assert!(pending[0]["messageId"].is_string(), "{pending}");
}

/// Remote parity S3: a remote agent's hook runs on head (its `host.exec`
/// sees head's worktree, not the machine's checkout) and its dispatch wakes
/// the agent on the machine; a structured question lands on the agent's
/// message on head and the answer reaches the agent; `ws.browser.*` and
/// `ws.desktop.*` reach the user's client connected to head.
async fn remote_hooks_questions_and_client_calls(
    (port, cfg): (u16, Arc<ClientConfig>),
    rpc_ws: &mut common::TlsWs,
    events: &mut common::TlsWs,
    (ws_id, agent_id): (&str, &str),
    (repo, node_dir): (&Path, &Path),
) {
    // A remote agent's schedule returns the hook row, as a local agent's
    // does, so it can cancel by `result.hook.hookId` (intent-hq/intent#8691).
    let kept = wsapi_turn(
        rpc_ws,
        events,
        (ws_id, agent_id),
        r#"wsapi const k = await ws.hook.schedule({ name: "remote quiet", delayMs: 600000, ttlMs: 60000, code: "return { dispatch: false };" }); const c = await ws.hook.cancel(k.hook.hookId); return JSON.stringify({ marker: "turn-hook-cancel", k, c });"#,
        "turn-hook-cancel",
        690,
    )
    .await;
    let kept = wsapi_value(&kept);
    assert_eq!(kept["k"]["dispatched"], false, "{kept}");
    assert_eq!(kept["k"]["hook"]["name"], "remote quiet", "{kept}");
    assert_eq!(kept["k"]["hook"]["agentId"], agent_id, "{kept}");
    assert_eq!(
        kept["c"]["hook"]["hookId"], kept["k"]["hook"]["hookId"],
        "{kept}"
    );
    assert_eq!(kept["c"]["hook"]["state"], "cancelled", "{kept}");

    let scheduled = wsapi_turn(
        rpc_ws,
        events,
        (ws_id, agent_id),
        r#"wsapi const h = await ws.hook.schedule({ name: "remote wake", delayMs: 10000, ttlMs: 60000, code: "const r = await ws.host.exec({ command: 'pwd' }); return { dispatch: true, message: 'hook-woke-remote ' + r.stdout.trim() };" }); return JSON.stringify({ marker: "turn-hook", h });"#,
        "turn-hook",
        700,
    )
    .await;
    assert!(scheduled.starts_with("wsapi: "), "{scheduled}");
    // The wake is the agent's next turn on the machine; the provider echoes it.
    event(events, "agent:stream:end").await;
    let woke = reply_containing(rpc_ws, (ws_id, agent_id), "hook-woke-remote", 710).await;
    assert!(woke.contains("echo: [Background hook"), "{woke}");
    let repo = std::fs::canonicalize(repo).unwrap();
    assert!(
        woke.contains(repo.to_str().unwrap()),
        "the hook's host.exec runs in head's worktree: {woke}"
    );
    assert!(!woke.contains(node_dir.to_str().unwrap()), "{woke}");

    let asked = wsapi_turn(
        rpc_ws,
        events,
        (ws_id, agent_id),
        r#"wsapi-tool const q = await ws.app.question.ask({ header: "Pick", question: "Which one?", options: [{ label: "Left" }, { label: "Right" }] }); return JSON.stringify({ marker: "turn-question", q });"#,
        "turn-question",
        800,
    )
    .await;
    let asked = wsapi_value(&asked);
    assert_eq!(asked["q"]["ok"], true, "{asked}");
    let marked = ok(
        rpc_ws,
        810,
        "agent.get",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert!(
        marked["agent"]["metadata"]["pendingQuestionsMessageId"].is_string(),
        "the question is pending on the agent's message on head: {marked}"
    );
    // The app sends the answers as a user message tagged with the
    // questions' message.
    let marked_id = marked["agent"]["metadata"]["pendingQuestionsMessageId"].clone();
    ok(
        rpc_ws,
        820,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": agent_id,
            "content": "Q: Which one?\nA: Left",
            "messageMetadata": { "type": "question_answers", "answeredQuestionsMessageId": marked_id },
        }),
    )
    .await;
    event(events, "agent:stream:end").await;
    let answered = reply_containing(rpc_ws, (ws_id, agent_id), "A: Left", 821).await;
    assert!(answered.contains("A: Left"), "{answered}");
    let cleared = ok(
        rpc_ws,
        830,
        "agent.get",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert!(
        cleared["agent"]["metadata"]["pendingQuestionsMessageId"]
            .as_str()
            .unwrap_or_default()
            .is_empty(),
        "{cleared}"
    );

    // The user's client: it serves the browser and the desktop.
    let mut client = connect(port, cfg).await;
    ok(
        &mut client,
        1,
        "client.hello",
        json!({
            "clientId": "remote-e2e-ui",
            "name": "Remote E2E UI",
            "capabilities": { "browserExec": true, "desktopControl": 1 },
        }),
    )
    .await;
    let (seen_tx, mut seen) = tokio::sync::mpsc::unbounded_channel::<Value>();
    let served = tokio::spawn(async move {
        while let Some(Ok(message)) = client.next().await {
            let request: Value = match message {
                Message::Text(text) => serde_json::from_str(&text).unwrap(),
                Message::Ping(p) => {
                    let _ = client.send(Message::Pong(p)).await;
                    continue;
                }
                _ => continue,
            };
            let result = match request["method"].as_str() {
                Some("browser.exec") => json!({ "results": [{
                    "action": "getUrl",
                    "success": true,
                    "result": "http://daemon.localhost:4173/",
                }] }),
                Some("desktop.control") if request["params"]["operation"] == "prepare" => json!({
                    "computerId": "remote-e2e-desktop",
                    "computerName": "E2E desktop",
                    "platform": "macos",
                }),
                _ => continue,
            };
            let _ = seen_tx.send(request.clone());
            let reply = json!({ "jsonrpc": "2.0", "id": request["id"], "result": result });
            if client
                .send(Message::Text(reply.to_string().into()))
                .await
                .is_err()
            {
                break;
            }
        }
    });
    let browsed = wsapi_turn(
        rpc_ws,
        events,
        (ws_id, agent_id),
        r#"wsapi const b = await ws.browser.exec([{ action: "getUrl" }]); return JSON.stringify({ marker: "turn-browser", b });"#,
        "turn-browser",
        900,
    )
    .await;
    let browsed = wsapi_value(&browsed);
    assert_eq!(
        browsed["b"]["result"], "http://daemon.localhost:4173/",
        "{browsed}"
    );
    let request = timeout(wait(), seen.recv()).await.unwrap().unwrap();
    assert_eq!(request["method"], "browser.exec", "{request}");
    assert_eq!(
        request["params"]["actions"][0]["action"], "getUrl",
        "{request}"
    );

    let controlled = wsapi_turn(
        rpc_ws,
        events,
        (ws_id, agent_id),
        r#"wsapi const d = await ws.desktop.startControl(); const e = await ws.desktop.endControl(); return JSON.stringify({ marker: "turn-desktop", d, e });"#,
        "turn-desktop",
        950,
    )
    .await;
    let controlled = wsapi_value(&controlled);
    assert_eq!(
        controlled["d"]["status"], "pending_permission",
        "the remote agent asks for the same consent: {controlled}"
    );
    let request = timeout(wait(), seen.recv()).await.unwrap().unwrap();
    assert_eq!(request["method"], "desktop.control", "{request}");
    assert_eq!(request["params"]["operation"], "prepare", "{request}");
    assert_eq!(controlled["e"]["withdrawn"], true, "{controlled}");
    // The withdrawn request wakes the agent once, as it would a local agent;
    // take that turn here so later steps see only their own turns.
    event(events, "agent:stream:end").await;
    let woke = reply_containing(rpc_ws, (ws_id, agent_id), "Desktop permission", 960).await;
    assert!(woke.contains("echo: "), "{woke}");
    served.abort();
}

/// The agent's latest assistant message containing `needle`, as JSON text,
/// polled from its conversation: a wake turn has no reply of its own to
/// wait on, and `lastAgentResponse` is only the reply's last line.
async fn reply_containing(
    rpc_ws: &mut common::TlsWs,
    (ws_id, agent_id): (&str, &str),
    needle: &str,
    id: i64,
) -> String {
    timeout(wait(), async {
        let mut id = id;
        loop {
            let convo = ok(
                rpc_ws,
                id,
                "agent.getConversation",
                json!({ "workspaceId": ws_id, "agentId": agent_id }),
            )
            .await;
            id += 1;
            let found = convo["messages"].as_array().and_then(|messages| {
                messages
                    .iter()
                    .rev()
                    .filter(|m| m["role"] == "assistant")
                    .map(Value::to_string)
                    .find(|text| text.contains(needle))
            });
            if let Some(text) = found {
                return text;
            }
            // timing-guard: poll interval for the wake turn's reply to be recorded
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("no assistant reply containing {needle:?}"))
}

/// [`turn`] for a `wsapi` prompt whose reply carries `marker`. Monitor
/// wakes, a stop or a relaunch also end turns, so the first
/// `agent:stream:end` after sending may belong to another turn, and the
/// agent's `lastAgentResponse` may still be the previous `wsapi:` reply:
/// only a reply carrying this prompt's own `marker` is accepted.
async fn wsapi_turn(
    rpc_ws: &mut common::TlsWs,
    events: &mut common::TlsWs,
    (ws_id, agent_id): (&str, &str),
    content: &str,
    marker: &str,
    id: i64,
) -> String {
    let mut reply = turn(rpc_ws, events, (ws_id, agent_id), content, id).await;
    let mut next = id + 2;
    let found = timeout(wait(), async {
        while !reply.contains(marker) {
            // timing-guard: poll interval for this turn's reply to be recorded (the stream-end event may have belonged to another turn)
            tokio::time::sleep(Duration::from_millis(100)).await;
            let got = ok(
                rpc_ws,
                next,
                "agent.get",
                json!({ "workspaceId": ws_id, "agentId": agent_id }),
            )
            .await;
            next += 1;
            got["agent"]["lastAgentResponse"]
                .as_str()
                .unwrap_or_default()
                .clone_into(&mut reply);
        }
    })
    .await;
    assert!(
        found.is_ok(),
        "no reply carrying {marker:?}; last reply: {reply}"
    );
    reply
}

/// A long text and two noise PNGs that image preparation keeps as they are
/// (each under 3 MiB and 2000 px): together they fit the backend's 8 MiB
/// image budget, but their prompt frame is over the node link's 8 MiB cap.
fn oversized_prompt_images() -> (String, Value) {
    use base64::Engine as _;
    let mut state: u32 = 0x9E37_79B9;
    let pixels: Vec<u8> = (0..1020 * 1020 * 3)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            state.to_le_bytes()[0]
        })
        .collect();
    let image = image::RgbImage::from_raw(1020, 1020, pixels).unwrap();
    let mut png = Vec::new();
    image
        .write_to(&mut std::io::Cursor::new(&mut png), image::ImageFormat::Png)
        .unwrap();
    assert!(png.len() <= 3 * 1024 * 1024, "{} bytes", png.len());
    let data = base64::engine::general_purpose::STANDARD.encode(&png);
    assert!(2 * data.len() <= 8 * 1024 * 1024, "{} bytes", data.len());
    let block = json!({ "data": data, "mimeType": "image/png" });
    ("x".repeat(60 * 1024), json!([block.clone(), block]))
}

/// `bytes` as lowercase hex.
fn lower_hex(bytes: &[u8]) -> String {
    bytes.iter().fold(String::new(), |mut hex, byte| {
        let _ = std::fmt::Write::write_fmt(&mut hex, format_args!("{byte:02x}"));
        hex
    })
}

/// A `wsapi:` reply's returned value: the agent returns `JSON.stringify(…)`,
/// which the reply carries as a JSON string.
fn wsapi_value(reply: &str) -> Value {
    let raw: Value = serde_json::from_str(
        reply
            .strip_prefix("wsapi: ")
            .unwrap_or_else(|| panic!("{reply}")),
    )
    .unwrap();
    match raw {
        Value::String(text) => serde_json::from_str(&text).unwrap(),
        other => other,
    }
}

/// The script row of `script_id`, from `script.get`.
async fn script_row(
    rpc_ws: &mut common::TlsWs,
    (ws_id, script_id): (&str, &str),
    id: i64,
) -> Value {
    let got = ok(
        rpc_ws,
        id,
        "script.get",
        json!({ "workspaceId": ws_id, "scriptId": script_id }),
    )
    .await;
    got.get("script").cloned().unwrap_or(got)
}

/// Poll the script's first machine run until `done` holds; returns it.
async fn machine_run_until(
    rpc_ws: &mut common::TlsWs,
    (ws_id, script_id, agent_id): (&str, &str, &str),
    first_id: i64,
    done: impl Fn(&Value) -> bool,
) -> Value {
    let mut id = first_id;
    let mut last = Value::Null;
    let reached = timeout(wait(), async {
        loop {
            id += 1;
            let row = script_row(rpc_ws, (ws_id, script_id), id).await;
            let run = row["machineRuns"]
                .as_array()
                .and_then(|runs| runs.iter().find(|run| run["remoteAgentId"] == agent_id))
                .cloned()
                .unwrap_or(Value::Null);
            if done(&run) {
                return run;
            }
            last = row;
            // timing-guard: poll interval for the machine's script frames to settle the run
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    reached.unwrap_or_else(|_| {
        panic!("the machine run never reached the expected state; last row: {last}")
    })
}

/// A remote agent's saved scripts run on its machine: the run, its output,
/// its monitor and its stop all reach the backend. Returns the id of a
/// saved service, stopped, for the caller's disconnect check.
async fn machine_runs(
    rpc_ws: &mut common::TlsWs,
    events: &mut common::TlsWs,
    (ws_id, agent_id): (&str, &str),
    node_id: &str,
    repo: &Path,
) -> String {
    // timing-guard: the saved script's own runtime on the machine, not a wait in the test
    let wsapi = r#"wsapi const c = await ws.script.create("mrun", "sleep 1; pwd; echo machine-run", "command", { purpose: "saved" }); const id = c.id ?? c.script.id; const st = await ws.script.start(id); const m = await ws.script.monitor(id, { ttlMs: 600000 }); return JSON.stringify({ id, st, m });"#;
    let reply = turn(rpc_ws, events, (ws_id, agent_id), wsapi, 200).await;
    let body = wsapi_value(&reply);
    let script_id = body["id"]
        .as_str()
        .unwrap_or_else(|| panic!("{body}"))
        .to_owned();
    assert_eq!(body["st"]["status"], "starting", "{body}");
    assert_eq!(body["m"]["monitor"]["remoteAgentId"], agent_id, "{body}");
    assert_eq!(body["m"]["monitor"]["nodeId"], node_id, "{body}");

    // The monitor wakes the agent when the run ends on the machine.
    let end = event(events, "agent:stream:end").await;
    assert_eq!(end["data"]["agentId"], agent_id, "{end}");
    let woken = ok(
        rpc_ws,
        202,
        "agent.get",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let woken = woken["agent"]["lastAgentResponse"]
        .as_str()
        .unwrap_or_default()
        .to_owned();
    assert!(woken.contains("exited with code 0"), "{woken}");

    // Definition reads run on head as the agent (intent#8630): head runs
    // with the bound-caller assertion armed, so an unbound gate aborts it.
    let wsapi = format!(
        r#"wsapi const l = await ws.script.list(); const p = await ws.script.listPage(); const g = await ws.script.get({script_id:?}); return JSON.stringify({{ marker: "turn-220", l, p, g }});"#
    );
    let reply = wsapi_turn(rpc_ws, events, (ws_id, agent_id), &wsapi, "turn-220", 220).await;
    assert!(reply.starts_with("wsapi: "), "{reply}");
    let reads = wsapi_value(&reply);
    for key in ["l", "p", "g"] {
        assert!(
            reads[key].to_string().contains(&script_id),
            "script.{key}: {reads}"
        );
    }

    let row = script_row(rpc_ws, (ws_id, &script_id), 203).await;
    assert_eq!(row["remoteAuthor"]["agentId"], agent_id, "{row}");
    assert_eq!(row["remoteAuthor"]["nodeId"], node_id, "{row}");
    assert_ne!(
        row["runtime"]["status"], "running",
        "the home run never started: {row}"
    );
    let run = &row["machineRuns"][0];
    assert_eq!(run["remoteAgentId"], agent_id, "{row}");
    assert_eq!(run["nodeId"], node_id, "{row}");
    assert_eq!(run["runtime"]["status"], "exited", "{row}");
    assert_eq!(run["runtime"]["exitCode"], 0, "{row}");
    assert_eq!(run["lastRun"]["outcome"], "succeeded", "{row}");

    // Its output came from the agent's checkout on the machine.
    let output = ok(
        rpc_ws,
        204,
        "script.output",
        json!({ "workspaceId": ws_id, "scriptId": script_id, "remoteAgentId": agent_id }),
    )
    .await;
    let text = output.to_string();
    assert!(text.contains("machine-run"), "{output}");
    assert!(
        !text.contains(&repo.to_string_lossy().into_owned()),
        "ran on head: {output}"
    );

    // Only the remote agent starts a run on its machine.
    let refused = rpc(
        rpc_ws,
        205,
        "script.start",
        json!({ "workspaceId": ws_id, "scriptId": script_id, "remoteAgentId": agent_id }),
    )
    .await;
    assert_eq!(refused["error"]["code"], -32602, "{refused}");
    assert!(
        refused
            .to_string()
            .contains("remote-script-start-remote-only"),
        "{refused}"
    );

    // Definition authority: a remote agent changes only scripts it created,
    // never sets autoStart, and never supplies remoteAuthor.
    let users = ok(
        rpc_ws,
        260,
        "script.create",
        json!({ "workspaceId": ws_id, "name": "user-saved", "command": "true", "mode": "command", "purpose": "saved" }),
    )
    .await;
    let users = users.get("script").cloned().unwrap_or(users);
    let user_script = users["id"].as_str().unwrap().to_owned();
    for (code, expected) in [
        (
            format!("wsapi return await ws.script.remove({user_script:?});"),
            "remote-script-not-owned",
        ),
        (
            r#"wsapi return await ws.script.create("auto", "true", "service", { autoStart: true });"#.to_owned(),
            "remote-script-autostart-unsupported",
        ),
        (
            r#"wsapi return await ws.script.create("forged", "true", "command", { remoteAuthor: { agentId: "a", nodeId: "n" } });"#.to_owned(),
            "remote-script-author-readonly",
        ),
    ] {
        let reply = wsapi_turn(rpc_ws, events, (ws_id, agent_id), &code, expected, 262).await;
        assert!(
            reply.starts_with("wsapi-error: ") && reply.contains(expected),
            "{code}: {reply}"
        );
    }

    // Remote parity S6: the agent copies a chat attachment into its machine
    // checkout; head serves the bytes as the bound agent (intent#8630).
    let placed = ok(
        rpc_ws,
        263,
        "file.placeAttachment",
        json!({ "workspaceId": ws_id, "fileName": "notes.txt", "data": "aGVsbG8=", "mimeType": "text/plain" }),
    )
    .await;
    let attachment_id = placed["attachmentId"].as_str().unwrap().to_owned();
    let wsapi = format!(
        r#"wsapi const a = await ws.file.getAttachment({attachment_id:?}); const t = await ws.file.read(a.path); return JSON.stringify({{ marker: "turn-264", a, t }});"#
    );
    let reply = wsapi_turn(rpc_ws, events, (ws_id, agent_id), &wsapi, "turn-264", 264).await;
    assert!(reply.starts_with("wsapi: "), "{reply}");
    let copied = wsapi_value(&reply);
    assert_eq!(
        copied["a"]["path"], ".intent/attachments/notes.txt",
        "{copied}"
    );
    assert_eq!(copied["t"], "hello", "{copied}");
    // A file larger than one link frame arrives in pieces, intact.
    let source = common::test_tempdir_in("/tmp", "itd-att-");
    let large: Vec<u8> = (0u32..20 * 1024 * 1024)
        .map(|i| i.wrapping_mul(2_654_435_761).to_le_bytes()[3])
        .collect();
    let large_path = source.path().join("large.bin");
    std::fs::write(&large_path, &large).unwrap();
    let placed = ok(
        rpc_ws,
        265,
        "file.placeAttachment",
        json!({ "workspaceId": ws_id, "fileName": "large.bin", "sourcePath": large_path, "mimeType": "application/octet-stream" }),
    )
    .await;
    let attachment_id = placed["attachmentId"].as_str().unwrap().to_owned();
    let wsapi = format!(
        r#"wsapi const a = await ws.file.getAttachment({attachment_id:?}); const r = await ws.host.exec({{ command: "sha256sum", args: [a.path] }}); return JSON.stringify({{ marker: "turn-266", a, hash: r.stdout.slice(0, 64) }});"#
    );
    let reply = wsapi_turn(rpc_ws, events, (ws_id, agent_id), &wsapi, "turn-266", 266).await;
    assert!(reply.starts_with("wsapi: "), "{reply}");
    let copied = wsapi_value(&reply);
    assert_eq!(copied["a"]["size"], large.len(), "{copied}");
    assert_eq!(
        copied["hash"],
        lower_hex(&Sha256::digest(&large)),
        "{copied}"
    );

    // A service started by the agent, stopped from the app.
    // timing-guard: the saved script's own runtime on the machine, not a wait in the test
    let wsapi = r#"wsapi const c = await ws.script.create("msrv", "echo up; sleep 300", "service"); const id = c.id ?? c.script.id; await ws.script.start(id); return JSON.stringify({ marker: "turn-206", id });"#;
    let reply = wsapi_turn(rpc_ws, events, (ws_id, agent_id), wsapi, "turn-206", 206).await;
    let body = wsapi_value(&reply);
    let service_id = body["id"].as_str().unwrap().to_owned();
    machine_run_until(rpc_ws, (ws_id, &service_id, agent_id), 210, |run| {
        run["runtime"]["status"] == "running"
    })
    .await;
    let stopped = ok(
        rpc_ws,
        250,
        "script.stop",
        json!({ "workspaceId": ws_id, "scriptId": service_id, "remoteAgentId": agent_id }),
    )
    .await;
    assert_eq!(stopped["status"], "idle", "{stopped}");
    service_id
}

/// A backend `intentd serve` on `data_dir` with a seeded workspace on
/// `repo`; its guard, WSS port, pinned client config and workspace id.
async fn start_backend(
    data_dir: &Path,
    repo: &Path,
) -> (common::DaemonGuard, u16, Arc<ClientConfig>, String) {
    start_backend_with(data_dir, repo, &[]).await
}

/// [`start_backend`] with extra environment for the backend process.
async fn start_backend_with(
    data_dir: &Path,
    repo: &Path,
    env: &[(&str, &str)],
) -> (common::DaemonGuard, u16, Arc<ClientConfig>, String) {
    start_backend_on(data_dir, repo, env, false).await
}

/// [`start_backend_with`]; see [`seed_workspace_on`] for `worktree`.
async fn start_backend_on(
    data_dir: &Path,
    repo: &Path,
    env: &[(&str, &str)],
    worktree: bool,
) -> (common::DaemonGuard, u16, Arc<ClientConfig>, String) {
    std::fs::create_dir_all(data_dir.join("workspaces")).unwrap();
    let ws_id = seed_workspace_on(data_dir, repo, worktree).await;
    let (guard, port, cfg) = serve_backend(data_dir, env).await;
    (guard, port, cfg, ws_id)
}

/// Start the backend's `intentd serve` on `data_dir` (seeded or from an
/// earlier run: a restart keeps its store and its fixed WSS port).
async fn serve_backend(
    data_dir: &Path,
    env: &[(&str, &str)],
) -> (common::DaemonGuard, u16, Arc<ClientConfig>) {
    common::enable_ws_api(data_dir);
    let mut serve = common::hermetic_serve_command(data_dir);
    serve
        .env("INTENTD_DATA_DIR", data_dir)
        .env("INTENTD_WORKSPACES_DIR", data_dir.join("workspaces"))
        .env("INTENTD_ASSERT_HERMETIC_ROOT", "1")
        .env("INTENTD_AUTH_TOKEN", TOKEN)
        .envs(env.iter().copied())
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
    (guard, port, cfg)
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
/// capability is advertised with no setting; a named machine takes
/// the agent; a model no machine lists is `model-unavailable` naming each
/// machine's models and sign-in status; an unknown `nodeId` is
/// `machine-unknown`; `workspace.create` places its initial agent the same
/// way (`remoteInitialAgent`).
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
    let on = ok(&mut rpc_ws, 1, "client.hello", hello).await;
    assert_eq!(on["server"]["capabilities"]["remoteMachines"], 1, "{on}");
    assert_eq!(
        on["server"]["capabilities"]["remoteInitialAgent"], 1,
        "{on}"
    );
    assert_eq!(on["server"]["capabilities"]["remoteLinkTraffic"], 1, "{on}");
    // Saved scripts for remote agents come only with exposed machines.
    assert_eq!(on["server"]["capabilities"]["remoteScripts"], 1, "{on}");

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
    // A machine's first report can take a while after its first mint; its
    // refusal is retryable until then.
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

    // `workspace.create` places its initial agent the same way: a named
    // machine takes it, and a refusal creates no workspace.
    let branch = std::process::Command::new("git")
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .current_dir(&repo)
        .output()
        .unwrap();
    let branch = String::from_utf8(branch.stdout).unwrap().trim().to_owned();
    let workspace = |model: &str, placement: Value| {
        json!({
            "repositoryPath": repo.to_string_lossy(),
            "baseRef": branch,
            "initialAgent": { "model": model, "provider": "mock", "placement": placement },
        })
    };
    let before = ok(&mut rpc_ws, 20, "workspace.list", json!({})).await;
    let refused = rpc(
        &mut rpc_ws,
        21,
        "workspace.create",
        workspace("no-such-model", json!({ "target": "remote" })),
    )
    .await;
    assert_eq!(refused["error"]["code"], -32602, "{refused}");
    assert_eq!(
        refused["error"]["data"]["code"], "model-unavailable",
        "{refused}"
    );
    let after = ok(&mut rpc_ws, 22, "workspace.list", json!({})).await;
    let count = |list: &Value| list["workspaces"].as_array().map(Vec::len);
    assert_eq!(
        count(&after),
        count(&before),
        "a refused placement creates no workspace: {after}"
    );
    assert!(count(&before).is_some(), "{before}");
    // Images whose prompt cannot cross the link to the machine are refused
    // before any workspace, worktree or agent exists.
    let (text, images) = oversized_prompt_images();
    let mut oversized = workspace("default", json!({ "target": "remote", "nodeId": b_id }));
    oversized["initialAgent"]["prompt"] = json!(text);
    oversized["initialAgent"]["imageBlocks"] = images;
    let workspaces_dir = scratch.path().join("head").join("workspaces");
    let entries = |dir: &Path| std::fs::read_dir(dir).map_or(0, Iterator::count);
    let dirs_before = entries(&workspaces_dir);
    let refused = rpc(&mut rpc_ws, 30, "workspace.create", oversized).await;
    assert_eq!(refused["error"]["code"], -32602, "{refused}");
    assert!(
        refused["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("attachments-too-large-for-machine")),
        "{refused}"
    );
    let after = ok(&mut rpc_ws, 31, "workspace.list", json!({})).await;
    assert_eq!(count(&after), count(&before), "{after}");
    assert_eq!(entries(&workspaces_dir), dirs_before, "no worktree is made");
    let placed = ok(
        &mut rpc_ws,
        23,
        "workspace.create",
        workspace("default", json!({ "target": "remote", "nodeId": b_id })),
    )
    .await;
    assert_eq!(placed["initialAgent"]["nodeId"], b_id.as_str(), "{placed}");
    assert_eq!(placed["initialAgent"]["nodeName"], "Machine B", "{placed}");
    assert!(
        placed["notice"]
            .as_str()
            .is_some_and(|notice| notice.contains("Machine B")),
        "{placed}"
    );
}

/// The `agent.pid` file the fixture provider wrote in `agent_id`'s worktree
/// under the machine's data dir `root`.
fn find_agent_pid_file(root: &Path, agent_id: &str) -> Option<std::path::PathBuf> {
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(kind) = entry.file_type() else {
                continue;
            };
            if kind.is_dir() && !kind.is_symlink() {
                stack.push(path);
            } else if path.file_name().is_some_and(|n| n == "agent.pid")
                && path.to_string_lossy().contains(agent_id)
            {
                return Some(path);
            }
        }
    }
    None
}

fn pid_alive(pid: i64) -> bool {
    Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}

/// intent-hq/intent#8426: `agent.delete` on a running remote agent stops
/// its runtime on the machine — the provider process exits and the agent
/// no longer counts against the machine's capacity, on head
/// (`remoteMachines.list`) and on the machine (`executor.list`).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deleting_a_running_remote_agent_stops_it_on_the_machine() {
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping remote delete e2e: node not on PATH");
        return;
    }
    if intent_providers::resolve_on_path("git").is_none() {
        eprintln!("skipping remote delete e2e: git not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-rdel-");
    let repo = scratch.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    std::fs::write(repo.join("README.md"), "base\n").unwrap();
    git(&repo, &["add", "README.md"]);
    git(&repo, &["commit", "--quiet", "-m", "base"]);

    let (_head, port, cfg, ws_id) = start_backend(&scratch.path().join("head"), &repo).await;
    let mut rpc_ws = connect(port, cfg.clone()).await;
    ok(
        &mut rpc_ws,
        1,
        "client.hello",
        json!({ "clientId": "remote-delete-e2e", "name": "Remote delete E2E" }),
    )
    .await;
    let backend = ok(&mut rpc_ws, 2, "remoteMachines.backendInfo", json!({})).await;
    let backend_id = backend["backendId"].as_str().unwrap().to_owned();
    let node_dir = scratch.path().join("node");
    std::fs::create_dir_all(&node_dir).unwrap();
    let machine = start_machine(&node_dir, &backend_id).await;
    ok(
        &mut rpc_ws,
        3,
        "remoteMachines.add",
        add_params(&machine, "E2E node"),
    )
    .await;
    let mut machine_ws = connect(machine.port, client_config(&machine.fingerprint)).await;

    let mut events = connect(port, cfg.clone()).await;
    ok(
        &mut events,
        4,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;

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
    let agent_id = created["agent"]["id"].as_str().unwrap().to_owned();

    let reply = turn(&mut rpc_ws, &mut events, (&ws_id, &agent_id), "hello", 100).await;
    assert!(reply.contains("echo:"), "{reply}");

    let head_running = |listed: &Value| listed["machines"][0]["runningAgents"].clone();
    let machine_running = |listed: &Value| listed["backends"][0]["runningAgents"].clone();
    let listed = ok(&mut rpc_ws, 110, "remoteMachines.list", json!({})).await;
    assert_eq!(head_running(&listed), 1, "{listed}");
    let listed = ok(&mut machine_ws, 111, "executor.list", json!({})).await;
    assert_eq!(machine_running(&listed), 1, "{listed}");
    let pid_file = find_agent_pid_file(&node_dir, &agent_id)
        .unwrap_or_else(|| panic!("the fixture's agent.pid for {agent_id} on the machine"));
    let pids: Value = serde_json::from_str(&std::fs::read_to_string(&pid_file).unwrap()).unwrap();
    let (provider_pid, child_pid) = (
        pids["agent"].as_i64().unwrap(),
        pids["child"].as_i64().unwrap(),
    );
    assert!(
        pid_alive(provider_pid),
        "the provider runs before the delete"
    );

    ok(
        &mut rpc_ws,
        120,
        "agent.delete",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;

    let mut id = 200;
    timeout(wait(), async {
        loop {
            id += 2;
            let head = ok(&mut rpc_ws, id, "remoteMachines.list", json!({})).await;
            let node = ok(&mut machine_ws, id + 1, "executor.list", json!({})).await;
            if head_running(&head) == 0
                && machine_running(&node) == 0
                && !pid_alive(provider_pid)
                && !pid_alive(child_pid)
            {
                return;
            }
            // timing-guard: poll interval for the machine to tear the agent down (no event exposes it)
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("agent.delete stops the remote agent and frees its slot on the machine");
}

/// intent-hq/intent#8426, local half: `agent.delete` on a running local
/// agent stops its provider process tree before the rows go.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deleting_a_running_local_agent_stops_its_provider() {
    if intent_providers::resolve_on_path("node").is_none() {
        eprintln!("skipping local delete e2e: node not on PATH");
        return;
    }
    if intent_providers::resolve_on_path("git").is_none() {
        eprintln!("skipping local delete e2e: git not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-ldel-");
    let repo = scratch.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    std::fs::write(repo.join("README.md"), "base\n").unwrap();
    git(&repo, &["add", "README.md"]);
    git(&repo, &["commit", "--quiet", "-m", "base"]);
    let fixture = format!(
        "{}/tests/fixtures/node-test-agent.mjs",
        env!("CARGO_MANIFEST_DIR")
    );
    let (_head, port, cfg, ws_id) = start_backend_with(
        &scratch.path().join("head"),
        &repo,
        &[("MOCK_AGENT_SCRIPT_PATH", fixture.as_str())],
    )
    .await;
    let mut rpc_ws = connect(port, cfg.clone()).await;
    let mut events = connect(port, cfg.clone()).await;
    ok(
        &mut events,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    let created = ok(
        &mut rpc_ws,
        2,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Local", "model": "default", "provider": "mock" }),
    )
    .await;
    let agent_id = created["agent"]["id"].as_str().unwrap().to_owned();
    turn(&mut rpc_ws, &mut events, (&ws_id, &agent_id), "hello", 10).await;

    let pid_file = repo.join("agent.pid");
    let pids: Value = serde_json::from_str(
        &std::fs::read_to_string(&pid_file)
            .unwrap_or_else(|e| panic!("the fixture's agent.pid at {}: {e}", pid_file.display())),
    )
    .unwrap();
    let (provider_pid, child_pid) = (
        pids["agent"].as_i64().unwrap(),
        pids["child"].as_i64().unwrap(),
    );
    assert!(
        pid_alive(provider_pid),
        "the provider runs before the delete"
    );

    ok(
        &mut rpc_ws,
        20,
        "agent.delete",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    timeout(wait(), async {
        while pid_alive(provider_pid) || pid_alive(child_pid) {
            // timing-guard: poll interval for the provider tree to exit (no event exposes it)
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("agent.delete stops the local agent's provider process tree");
}

/// Remote parity S3 over two real daemons: see
/// [`remote_hooks_questions_and_client_calls`].
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_agent_hooks_questions_browser_and_desktop() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping remote S3 e2e: node or git not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-s3-");
    let repo = scratch.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    std::fs::write(repo.join("README.md"), "base\n").unwrap();
    git(&repo, &["add", "README.md"]);
    git(&repo, &["commit", "--quiet", "-m", "base"]);
    let (_head, port, cfg, ws_id) = start_backend(&scratch.path().join("head"), &repo).await;
    let mut rpc_ws = connect(port, cfg.clone()).await;
    let backend = ok(&mut rpc_ws, 1, "remoteMachines.backendInfo", json!({})).await;
    let backend_id = backend["backendId"].as_str().unwrap().to_owned();
    let node_dir = scratch.path().join("node");
    std::fs::create_dir_all(&node_dir).unwrap();
    let machine = start_machine(&node_dir, &backend_id).await;
    ok(
        &mut rpc_ws,
        2,
        "remoteMachines.add",
        add_params(&machine, "E2E node"),
    )
    .await;
    let mut events = connect(port, cfg.clone()).await;
    ok(
        &mut events,
        3,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    let create = json!({
        "workspaceId": ws_id, "name": "Remote", "model": "default", "provider": "mock",
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
        assert!(
            tokio::time::Instant::now() < deadline,
            "the machine never took the placement: {response}"
        );
        // timing-guard: poll interval while the machine daemon binds to head (no event exposes it)
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let agent_id = created["agent"]["id"].as_str().unwrap().to_owned();
    remote_hooks_questions_and_client_calls(
        (port, cfg),
        &mut rpc_ws,
        &mut events,
        (&ws_id, &agent_id),
        (&repo, &node_dir),
    )
    .await;
}

/// Deleting a remote agent ends it for good: without a prior stop, its
/// machine pushes its work one last time (landing as its `wip` ref) and
/// then removes its checkout.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn deleting_a_remote_agent_pushes_its_work_then_removes_its_checkout() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping remote final stop e2e: node or git not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-final-");
    let repo = scratch.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    std::fs::write(repo.join("README.md"), "base\n").unwrap();
    git(&repo, &["add", "README.md"]);
    git(&repo, &["commit", "--quiet", "-m", "base"]);
    let (_head, port, cfg, ws_id) = start_backend(&scratch.path().join("head"), &repo).await;
    let mut rpc_ws = connect(port, cfg.clone()).await;
    let backend = ok(&mut rpc_ws, 1, "remoteMachines.backendInfo", json!({})).await;
    let backend_id = backend["backendId"].as_str().unwrap().to_owned();
    let node_dir = scratch.path().join("node");
    std::fs::create_dir_all(&node_dir).unwrap();
    let machine = start_machine(&node_dir, &backend_id).await;
    ok(
        &mut rpc_ws,
        2,
        "remoteMachines.add",
        add_params(&machine, "E2E node"),
    )
    .await;
    let mut events = connect(port, cfg.clone()).await;
    ok(
        &mut events,
        3,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;

    let create = json!({
        "workspaceId": ws_id, "name": "Remote", "model": "default", "provider": "mock",
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
        assert!(
            tokio::time::Instant::now() < deadline,
            "the machine never took the placement: {response}"
        );
        // timing-guard: poll interval while the machine daemon binds to head (no event exposes it)
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let agent_id = created["agent"]["id"].as_str().unwrap().to_owned();
    let refs = format!("refs/intent/ws/{ws_id}/agents/{agent_id}");
    let reply = turn(
        &mut rpc_ws,
        &mut events,
        (&ws_id, &agent_id),
        r#"wsapi await ws.file.write("turn.txt", "turn work"); return "ok";"#,
        50,
    )
    .await;
    assert!(reply.starts_with("wsapi: "), "{reply}");
    // The turn's end pushes `wip` on its own; wait for it, so only the
    // final stop can carry what is written next.
    let turn_wip = timeout(wait(), async {
        loop {
            if let Some(wip) = try_rev(&repo, &format!("{refs}/wip")) {
                if git(&repo, &["ls-tree", "--name-only", &wip])
                    .lines()
                    .any(|f| f == "turn.txt")
                {
                    return wip;
                }
            }
            // timing-guard: poll interval for the turn-end push to land the ref (no event exposes it)
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the turn-end wip lands on head");
    let checkout = node_dir
        .join("executor")
        .join(&machine.credential_id)
        .join("agents")
        .join(&agent_id);
    // Unsaved work after the last turn, written straight into the checkout.
    std::fs::write(checkout.join("work").join("final.txt"), "unsaved work").unwrap();

    ok(
        &mut rpc_ws,
        60,
        "agent.delete",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let wip = timeout(wait(), async {
        loop {
            if let Some(wip) = try_rev(&repo, &format!("{refs}/wip")) {
                if wip != turn_wip {
                    return wip;
                }
            }
            // timing-guard: poll interval for the machine's final push to land the ref (no event exposes it)
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the final push lands a new wip on head");
    assert_eq!(
        git(&repo, &["show", &format!("{wip}:final.txt")]),
        "unsaved work"
    );
    assert_eq!(
        git(&repo, &["show", &format!("{wip}:turn.txt")]),
        "turn work"
    );
    timeout(wait(), async {
        while checkout.exists() {
            // timing-guard: poll interval for the machine's checkout removal (no event exposes it)
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the machine removes the checkout after the push");
}

/// The machine's own graceful shutdown (a restart or an update) stops its
/// agents and pushes each one's work a last time, and the backend accepts
/// that push: a live agent's changes since its last turn-end push land in
/// its `wip` ref (intent-hq/intent#8437).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn machine_shutdown_pushes_live_agents_work() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping remote shutdown push e2e: node or git not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-shutdown-");
    let repo = scratch.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    std::fs::write(repo.join("README.md"), "base\n").unwrap();
    git(&repo, &["add", "README.md"]);
    git(&repo, &["commit", "--quiet", "-m", "base"]);
    let (_head, port, cfg, ws_id) = start_backend(&scratch.path().join("head"), &repo).await;
    let mut rpc_ws = connect(port, cfg.clone()).await;
    let backend = ok(&mut rpc_ws, 1, "remoteMachines.backendInfo", json!({})).await;
    let backend_id = backend["backendId"].as_str().unwrap().to_owned();
    let node_dir = scratch.path().join("node");
    std::fs::create_dir_all(&node_dir).unwrap();
    let mut machine = start_machine(&node_dir, &backend_id).await;
    ok(
        &mut rpc_ws,
        2,
        "remoteMachines.add",
        add_params(&machine, "E2E node"),
    )
    .await;
    let mut events = connect(port, cfg.clone()).await;
    ok(
        &mut events,
        3,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;

    let create = json!({
        "workspaceId": ws_id, "name": "Remote", "model": "default", "provider": "mock",
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
        assert!(
            tokio::time::Instant::now() < deadline,
            "the machine never took the placement: {response}"
        );
        // timing-guard: poll interval while the machine daemon binds to head (no event exposes it)
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let agent_id = created["agent"]["id"].as_str().unwrap().to_owned();
    let wip_ref = format!("refs/intent/ws/{ws_id}/agents/{agent_id}/wip");
    let reply = turn(
        &mut rpc_ws,
        &mut events,
        (&ws_id, &agent_id),
        r#"wsapi await ws.file.write("turn.txt", "pushed at turn end"); return "ok";"#,
        50,
    )
    .await;
    assert!(reply.starts_with("wsapi: "), "{reply}");
    // The turn-end push lands first.
    let pushed = timeout(wait(), async {
        loop {
            if let Some(wip) = try_rev(&repo, &wip_ref) {
                if git(&repo, &["ls-tree", "--name-only", &wip]).contains("turn.txt") {
                    return wip;
                }
            }
            // timing-guard: poll interval for the turn-end push to land the ref (no event exposes it)
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the turn-end push lands on head");

    // Work since that push, still only in the machine's checkout.
    let checkout = node_dir
        .join("executor")
        .join(&machine.credential_id)
        .join("agents")
        .join(&agent_id)
        .join("work");
    assert!(
        checkout.join("turn.txt").is_file(),
        "{}",
        checkout.display()
    );
    std::fs::write(checkout.join("unpushed.txt"), "since the last push").unwrap();

    // The machine's daemon shuts down gracefully, the agent still live.
    let daemon = machine.daemon.child_mut();
    daemon.signal(nix::sys::signal::Signal::SIGTERM).unwrap();
    daemon
        .wait_with_timeout(common::test_timeout(Duration::from_secs(120)))
        .unwrap()
        .expect("the machine's daemon exits gracefully");

    let wip = try_rev(&repo, &wip_ref).expect("the wip ref exists");
    let log = std::fs::read_to_string(node_dir.join("machine.log")).unwrap_or_default();
    assert_ne!(
        wip, pushed,
        "the shutdown push did not land on head; machine log:\n{log}"
    );
    assert_eq!(
        git(&repo, &["show", &format!("{wip}:unpushed.txt")]),
        "since the last push"
    );
}

/// The prepared checkout end to end (nodeProtocol 10, intent-hq/intent#8594):
/// a remote agent's checkout on the machine has every submodule populated on
/// the backend, nested ones included, at the commits the backend has checked
/// out (here `a` is at a backend-only commit ahead of its gitlink); and a
/// commit inside one submodule plus an uncommitted edit inside another come
/// back to the backend's submodule repositories with the agent's final
/// push, the superproject's `wip` naming the module `wip`s.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_remote_agents_submodules_are_prepared_and_their_work_comes_back() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping remote submodules e2e: node or git not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-subm-");
    let upstream = |name: &str| {
        let dir = scratch.path().join("upstream").join(name);
        std::fs::create_dir_all(&dir).unwrap();
        git(&dir, &["init", "--quiet"]);
        std::fs::write(dir.join("README.md"), format!("{name}\n")).unwrap();
        git(&dir, &["add", "README.md"]);
        git(&dir, &["commit", "--quiet", "-m", name]);
        dir
    };
    let add = |dir: &Path, url: &Path, path: &str| {
        git(
            dir,
            &[
                "-c",
                "protocol.file.allow=always",
                "submodule",
                "add",
                "--quiet",
                url.to_str().unwrap(),
                path,
            ],
        );
    };
    let b_url = upstream("b");
    let a_url = upstream("a");
    add(&a_url, &b_url, "b");
    git(&a_url, &["commit", "--quiet", "-m", "add b"]);
    let c_url = upstream("c");
    let repo = scratch.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    add(&repo, &a_url, "a");
    add(&repo, &c_url, "c");
    git(
        &repo,
        &[
            "-c",
            "protocol.file.allow=always",
            "submodule",
            "update",
            "--quiet",
            "--init",
            "--recursive",
        ],
    );
    git(&repo, &["commit", "--quiet", "-m", "submodules"]);
    std::fs::write(repo.join("a/ahead.txt"), "backend only\n").unwrap();
    git(&repo.join("a"), &["add", "ahead.txt"]);
    git(&repo.join("a"), &["commit", "--quiet", "-m", "ahead"]);
    let heads =
        |root: &Path| ["a", "a/b", "c"].map(|path| git(&root.join(path), &["rev-parse", "HEAD"]));
    let backend_heads = heads(&repo);
    let status = |root: &Path| -> Vec<String> {
        git(root, &["submodule", "status", "--recursive"])
            .lines()
            .map(|line| {
                line.split_whitespace()
                    .take(2)
                    .collect::<Vec<_>>()
                    .join(" ")
            })
            .collect()
    };

    let (_head, port, cfg, ws_id) = start_backend(&scratch.path().join("head"), &repo).await;
    let mut rpc_ws = connect(port, cfg.clone()).await;
    let backend = ok(&mut rpc_ws, 1, "remoteMachines.backendInfo", json!({})).await;
    let backend_id = backend["backendId"].as_str().unwrap().to_owned();
    let node_dir = scratch.path().join("node");
    std::fs::create_dir_all(&node_dir).unwrap();
    let machine = start_machine(&node_dir, &backend_id).await;
    ok(
        &mut rpc_ws,
        2,
        "remoteMachines.add",
        add_params(&machine, "E2E node"),
    )
    .await;
    let mut events = connect(port, cfg.clone()).await;
    ok(
        &mut events,
        3,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    let create = json!({
        "workspaceId": ws_id, "name": "Remote", "model": "default", "provider": "mock",
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
        assert!(
            tokio::time::Instant::now() < deadline,
            "the machine never took the placement: {response}"
        );
        // timing-guard: poll interval while the machine daemon binds to head (no event exposes it)
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let agent_id = created["agent"]["id"].as_str().unwrap().to_owned();
    assert!(
        created["notice"]
            .as_str()
            .unwrap_or_default()
            .contains("submodules"),
        "{created}"
    );
    let reply = turn(&mut rpc_ws, &mut events, (&ws_id, &agent_id), "hello", 50).await;
    assert!(reply.contains("echo:"), "{reply}");

    let work = node_dir
        .join("executor")
        .join(&machine.credential_id)
        .join("agents")
        .join(&agent_id)
        .join("work");
    assert_eq!(heads(&work), backend_heads);
    assert_eq!(status(&work), status(&repo));
    assert_eq!(
        std::fs::read_to_string(work.join("a/ahead.txt")).unwrap(),
        "backend only\n"
    );

    // The workspace's git roots on the backend: the remote agent lists only
    // its carried submodules, at their paths in its checkout, and not the
    // repository outside it (intent-hq/intent#8692).
    seed_git_roots(
        &scratch.path().join("head"),
        &ws_id,
        &[
            repo.join("a"),
            repo.join("a/b"),
            repo.join("c"),
            c_url.clone(),
        ],
    )
    .await;
    let listed = wsapi_turn(
        &mut rpc_ws,
        &mut events,
        (&ws_id, &agent_id),
        r#"wsapi const info = await ws.workspace.info(); const roots = await ws.git.listRoots(); return JSON.stringify({ marker: "turn-list-roots", info, paths: roots.map(r => r.path).sort() });"#,
        "turn-list-roots",
        300,
    )
    .await;
    let listed = wsapi_value(&listed);
    let checkout = listed["info"]["path"].as_str().unwrap().to_owned();
    assert_eq!(
        listed["paths"],
        json!(["a", "a/b", "c"].map(|path| format!("{checkout}/{path}"))),
        "{listed}"
    );

    // The agent commits in the nested `a/b` and leaves an edit in `c`.
    std::fs::write(work.join("a/b/agent.txt"), "agent commit\n").unwrap();
    git(&work.join("a/b"), &["add", "agent.txt"]);
    git(
        &work.join("a/b"),
        &["commit", "--quiet", "-m", "agent work"],
    );
    let b_commit = git(&work.join("a/b"), &["rev-parse", "HEAD"]);
    std::fs::write(work.join("c/edit.txt"), "agent edit\n").unwrap();

    ok(
        &mut rpc_ws,
        60,
        "agent.delete",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let refs = format!("refs/intent/ws/{ws_id}/agents/{agent_id}");
    let wip = timeout(wait(), async {
        loop {
            let c_wip = try_rev(&repo.join("c"), &format!("{refs}/wip"));
            let super_wip = try_rev(&repo, &format!("{refs}/wip"));
            if let (Some(c_wip), Some(super_wip)) = (c_wip, super_wip) {
                let edited = git(&repo.join("c"), &["ls-tree", "--name-only", &c_wip])
                    .lines()
                    .any(|file| file == "edit.txt");
                if edited && git(&repo, &["rev-parse", &format!("{super_wip}:c")]) == c_wip {
                    return super_wip;
                }
            }
            // timing-guard: poll interval for the machine's final push to land the refs (no event exposes it)
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the final push lands the modules and the superproject");
    assert_eq!(
        try_rev(&repo.join("a/b"), &format!("{refs}/head")),
        Some(b_commit.clone())
    );
    let c_wip = try_rev(&repo.join("c"), &format!("{refs}/wip")).unwrap();
    assert_eq!(
        git(&repo.join("c"), &["show", &format!("{c_wip}:edit.txt")]),
        "agent edit"
    );
    let a_wip = try_rev(&repo.join("a"), &format!("{refs}/wip")).unwrap();
    assert_eq!(
        git(&repo.join("a"), &["rev-parse", &format!("{a_wip}:b")]),
        b_commit
    );
    assert_eq!(git(&repo, &["rev-parse", &format!("{wip}:a")]), a_wip);
    // The backend's own checkouts are untouched.
    assert_eq!(heads(&repo), backend_heads);
    assert!(!repo.join("c/edit.txt").exists());
}

/// Register `paths` as `ws_id`'s secondary git roots in the backend's store.
async fn seed_git_roots(data_dir: &Path, ws_id: &str, paths: &[std::path::PathBuf]) {
    use intent_core::{now_iso, WorkspaceGitRoot, WorkspaceGitRootId, WorkspaceGitRootSource};
    let store = intent_store::Store::open(&data_dir.join("intentd.db"))
        .await
        .expect("open store");
    let ts = now_iso();
    for path in paths {
        store
            .upsert_workspace_git_root(&WorkspaceGitRoot {
                id: WorkspaceGitRootId::new(),
                workspace_id: intent_core::WorkspaceId::from(ws_id),
                path: std::fs::canonicalize(path)
                    .unwrap()
                    .to_string_lossy()
                    .into_owned(),
                source: WorkspaceGitRootSource::Auto,
                repo_owner: None,
                repo_name: None,
                registered_by_agent_ids: vec![],
                registered_commit_sha: None,
                pr_number: None,
                pr_url: None,
                pr_status: None,
                pull_requests: None,
                created_at: ts.clone(),
                updated_at: ts.clone(),
            })
            .await
            .expect("insert git root");
    }
}

/// Create a remote agent named `name` on the exposed machine, retrying
/// while the backend's first connect to it is still in flight.
async fn create_remote(rpc_ws: &mut common::TlsWs, ws_id: &str, name: &str, id: i64) -> Value {
    let create = json!({
        "workspaceId": ws_id, "name": name, "model": "default", "provider": "mock",
        "placement": { "target": "remote" },
    });
    let deadline = tokio::time::Instant::now() + wait();
    let mut id = id;
    loop {
        id += 1;
        let response = rpc(rpc_ws, id, "agent.create", create.clone()).await;
        if response.get("error").is_none() {
            return response["result"].clone();
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the machine never took the placement: {response}"
        );
        // timing-guard: poll interval while the machine daemon binds to head (no event exposes it)
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Poll `agent.get` until the agent's `setup` satisfies `done`; returns it.
async fn setup_until(
    rpc_ws: &mut common::TlsWs,
    (ws_id, agent_id): (&str, &str),
    id: i64,
    done: impl Fn(&Value) -> bool,
) -> Value {
    let mut id = id;
    let mut last = Value::Null;
    let found = timeout(wait(), async {
        loop {
            id += 1;
            let got = ok(
                rpc_ws,
                id,
                "agent.get",
                json!({ "workspaceId": ws_id, "agentId": agent_id }),
            )
            .await;
            last = got["agent"]["setup"].clone();
            if done(&last) {
                return last.clone();
            }
            // timing-guard: poll interval for the machine's setup records and output to reach head
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    found.unwrap_or_else(|_| panic!("setup never reached the expected state: {last}"))
}

/// The workspace setup script on the machine (nodeProtocol 10,
/// intent-hq/intent#8594): it runs in the remote agent's fresh checkout
/// after the submodules are populated and before the agent starts, with the
/// documented environment; its status and output reach the backend
/// (`agent.get`, list rows without output, the agent's own
/// `workspace.details`); the files it creates stay out of the agent's
/// `wip`. A failing script still starts the agent, whose first turn carries
/// the failure notice.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_remote_agents_setup_script_runs_on_the_machine_after_its_submodules() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping remote setup e2e: node or git not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-setup-");
    let module = scratch.path().join("upstream").join("a");
    std::fs::create_dir_all(&module).unwrap();
    git(&module, &["init", "--quiet"]);
    std::fs::write(module.join("README.md"), "a\n").unwrap();
    git(&module, &["add", "README.md"]);
    git(&module, &["commit", "--quiet", "-m", "a"]);
    let repo = scratch.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet", "--initial-branch", "main"]);
    git(
        &repo,
        &[
            "-c",
            "protocol.file.allow=always",
            "submodule",
            "add",
            "--quiet",
            module.to_str().unwrap(),
            "a",
        ],
    );
    git(&repo, &["commit", "--quiet", "-m", "submodule"]);
    // Read from the backend worktree at each launch, uncommitted.
    let set_script = |script: &str| {
        std::fs::create_dir_all(repo.join(".intent")).unwrap();
        std::fs::write(
            repo.join(".intent/config.json"),
            json!({ "setupScript": script }).to_string(),
        )
        .unwrap();
    };
    set_script(
        "test -e a/.git || { echo submodules-missing; exit 9; }\n\
         echo submodules-ready\n\
         printf '%s|%s|%s|%s|%s\\n' \"$BRANCH_NAME\" \"$SOURCE_BRANCH\" \"$INTENT_REMOTE_MACHINE\" \"$MAIN_CHECKOUT\" \"${INTENT_NODE_GIT_SOCKET-unset}\" > setup-env.txt\n\
         mkdir -p generated a/built && echo out > generated/out.txt && echo out > a/built/out.txt\n",
    );

    let (_head, port, cfg, ws_id) =
        start_backend_on(&scratch.path().join("head"), &repo, &[], true).await;
    let mut rpc_ws = connect(port, cfg.clone()).await;
    let backend = ok(&mut rpc_ws, 1, "remoteMachines.backendInfo", json!({})).await;
    let backend_id = backend["backendId"].as_str().unwrap().to_owned();
    let node_dir = scratch.path().join("node");
    std::fs::create_dir_all(&node_dir).unwrap();
    let machine = start_machine(&node_dir, &backend_id).await;
    ok(
        &mut rpc_ws,
        2,
        "remoteMachines.add",
        add_params(&machine, "E2E node"),
    )
    .await;
    let mut events = connect(port, cfg.clone()).await;
    ok(
        &mut events,
        3,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;

    let created = create_remote(&mut rpc_ws, &ws_id, "Remote", 10).await;
    let agent_id = created["agent"]["id"].as_str().unwrap().to_owned();
    assert!(
        created["notice"]
            .as_str()
            .unwrap_or_default()
            .contains("setup script runs on that machine"),
        "{created}"
    );
    let reply = turn(&mut rpc_ws, &mut events, (&ws_id, &agent_id), "hello", 50).await;
    assert!(reply.contains("echo:"), "{reply}");
    assert!(!reply.contains("workspace setup failed"), "{reply}");
    assert!(!reply.contains("populated on this machine"), "{reply}");

    let setup = setup_until(&mut rpc_ws, (&ws_id, &agent_id), 100, |setup| {
        setup["output"]
            .as_str()
            .is_some_and(|out| out.contains("Setup script completed in"))
    })
    .await;
    assert_eq!(setup["state"], "completed", "{setup}");
    assert_eq!(setup["exitCode"], 0, "{setup}");
    assert!(setup["startedAt"].is_string() && setup["finishedAt"].is_string());
    let output = setup["output"].as_str().unwrap();
    let fetched = output.find("Fetching a…").expect(output);
    let ready = output.find("submodules-ready").expect(output);
    assert!(fetched < ready, "the checkout comes before setup: {output}");
    assert_eq!(setup["outputTruncated"], false);

    let listed = ok(
        &mut rpc_ws,
        200,
        "agent.list",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    let row = listed["agents"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == agent_id.as_str())
        .unwrap()
        .clone();
    assert_eq!(row["setup"]["state"], "completed", "{row}");
    assert!(row["setup"].get("output").is_none(), "{row}");

    let details = wsapi_turn(
        &mut rpc_ws,
        &mut events,
        (&ws_id, &agent_id),
        r#"wsapi const d = await ws.workspace.details(); return JSON.stringify({ marker: "turn-details", setup: d.setupStatus });"#,
        "turn-details",
        300,
    )
    .await;
    let value = wsapi_value(&details);
    assert_eq!(value["setup"]["state"], "completed", "{value}");
    assert_eq!(value["setup"]["exitCode"], 0, "{value}");
    assert!(value["setup"].get("unsentSubmodules").is_none(), "{value}");

    let work = node_dir
        .join("executor")
        .join(&machine.credential_id)
        .join("agents")
        .join(&agent_id)
        .join("work");
    let env = std::fs::read_to_string(work.join("setup-env.txt")).unwrap();
    let fields: Vec<&str> = env.trim_end().split('|').collect();
    assert_eq!(fields[..3], ["main", "main", "1"], "{env}");
    let main_checkout = Path::new(fields[3]);
    assert!(main_checkout.is_dir(), "{env}");
    assert!(
        main_checkout.starts_with(
            node_dir
                .join("executor")
                .join(&machine.credential_id)
                .join("repos")
        ),
        "{env}"
    );
    assert_eq!(fields[4], "unset", "no agent launch wiring: {env}");
    assert!(
        !entry_is_script(&work) && !entry_is_script(work.parent().unwrap()),
        "the script file is removed and never in the checkout"
    );

    // A failing script: the second agent still starts, told on its first
    // turn.
    set_script("echo boom-from-setup\nexit 3\n");
    let failing = create_remote(&mut rpc_ws, &ws_id, "Failing", 400).await;
    let failing_id = failing["agent"]["id"].as_str().unwrap().to_owned();
    turn(
        &mut rpc_ws,
        &mut events,
        (&ws_id, &failing_id),
        "hello",
        450,
    )
    .await;
    let setup = setup_until(&mut rpc_ws, (&ws_id, &failing_id), 500, |setup| {
        setup["state"] == "failed"
    })
    .await;
    assert_eq!(setup["exitCode"], 3, "{setup}");
    turn(
        &mut rpc_ws,
        &mut events,
        (&ws_id, &failing_id),
        "again",
        600,
    )
    .await;
    // The fixture echoes each prompt; the notice rides the first one only.
    let replies = timeout(wait(), async {
        let mut id = 650;
        loop {
            id += 1;
            let session = ok(
                &mut rpc_ws,
                id,
                "agent.getSession",
                json!({ "workspaceId": ws_id, "agentId": failing_id }),
            )
            .await;
            let replies: Vec<String> = session["session"]["messages"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|message| message["role"] == "assistant")
                .map(Value::to_string)
                .collect();
            if replies.len() >= 2 {
                return replies;
            }
            // timing-guard: poll interval for both replies to be recorded
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("both turns are recorded");
    assert!(replies[0].contains("workspace setup failed"), "{replies:?}");
    assert!(replies[0].contains("exited with code 3"), "{replies:?}");
    assert!(replies[0].contains("boom-from-setup"), "{replies:?}");
    assert!(
        !replies[1].contains("workspace setup failed"),
        "notice once: {replies:?}"
    );

    // The first agent's work comes back without the setup's files.
    std::fs::write(work.join("agent.txt"), "agent edit\n").unwrap();
    std::fs::write(work.join("a/agent.txt"), "module edit\n").unwrap();
    ok(
        &mut rpc_ws,
        700,
        "agent.delete",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let refs = format!("refs/intent/ws/{ws_id}/agents/{agent_id}");
    let wip = timeout(wait(), async {
        loop {
            if let Some(wip) = try_rev(&repo, &format!("{refs}/wip")) {
                let files = git(&repo, &["ls-tree", "-r", "--name-only", &wip]);
                if files.lines().any(|f| f == "agent.txt") {
                    return wip;
                }
            }
            // timing-guard: poll interval for the machine's final push to land the refs (no event exposes it)
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the final push lands the agent's edit");
    let files = git(&repo, &["ls-tree", "-r", "--name-only", &wip]);
    assert!(files.lines().any(|f| f == "agent.txt"), "{files}");
    for setup_file in ["setup-env.txt", "generated/out.txt"] {
        assert!(!files.lines().any(|f| f == setup_file), "{files}");
    }
    let module_wip = git(&repo, &["rev-parse", &format!("{wip}:a")]);
    let module_files = git(
        &repo.join("a"),
        &["ls-tree", "-r", "--name-only", &module_wip],
    );
    assert!(
        module_files.lines().any(|f| f == "agent.txt"),
        "{module_files}"
    );
    assert!(
        !module_files.lines().any(|f| f == "built/out.txt"),
        "{module_files}"
    );
}

/// A submodule the setup script populates on the machine, not one of the
/// agent's pins, is not carried: the agent's first turn names it and its
/// `workspace.details().setupStatus` lists it (intent-hq/intent#8594).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_remote_agent_is_told_about_a_submodule_its_setup_script_added() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping remote unsent-submodule e2e: node or git not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-unsent-");
    let upstream = |name: &str| {
        let dir = scratch.path().join("upstream").join(name);
        std::fs::create_dir_all(&dir).unwrap();
        git(&dir, &["init", "--quiet"]);
        std::fs::write(dir.join("README.md"), format!("{name}\n")).unwrap();
        git(&dir, &["add", "README.md"]);
        git(&dir, &["commit", "--quiet", "-m", name]);
        dir
    };
    let repo = scratch.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet", "--initial-branch", "main"]);
    for name in ["a", "x"] {
        let url = upstream(name);
        git(
            &repo,
            &[
                "-c",
                "protocol.file.allow=always",
                "submodule",
                "add",
                "--quiet",
                url.to_str().unwrap(),
                name,
            ],
        );
    }
    git(&repo, &["commit", "--quiet", "-m", "submodules"]);
    // `x` is not populated on the backend, so it is not a pin.
    git(&repo, &["submodule", "deinit", "--quiet", "-f", "x"]);
    std::fs::create_dir_all(repo.join(".intent")).unwrap();
    std::fs::write(
        repo.join(".intent/config.json"),
        json!({ "setupScript": "git -c protocol.file.allow=always submodule update --quiet --init --checkout x\n" })
            .to_string(),
    )
    .unwrap();

    let (_head, port, cfg, ws_id) =
        start_backend_on(&scratch.path().join("head"), &repo, &[], true).await;
    let mut rpc_ws = connect(port, cfg.clone()).await;
    let backend = ok(&mut rpc_ws, 1, "remoteMachines.backendInfo", json!({})).await;
    let backend_id = backend["backendId"].as_str().unwrap().to_owned();
    let node_dir = scratch.path().join("node");
    std::fs::create_dir_all(&node_dir).unwrap();
    let machine = start_machine(&node_dir, &backend_id).await;
    ok(
        &mut rpc_ws,
        2,
        "remoteMachines.add",
        add_params(&machine, "E2E node"),
    )
    .await;
    let mut events = connect(port, cfg.clone()).await;
    ok(
        &mut events,
        3,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    let created = create_remote(&mut rpc_ws, &ws_id, "Remote", 10).await;
    let agent_id = created["agent"]["id"].as_str().unwrap().to_owned();
    turn(&mut rpc_ws, &mut events, (&ws_id, &agent_id), "hello", 50).await;
    // The fixture echoes the prompt with the notice prepended; the row's
    // `lastAgentResponse` does not carry the notice, so read the transcript.
    let reply = timeout(wait(), async {
        let mut id = 60;
        loop {
            id += 1;
            let session = ok(
                &mut rpc_ws,
                id,
                "agent.getSession",
                json!({ "workspaceId": ws_id, "agentId": agent_id }),
            )
            .await;
            if let Some(first) = session["session"]["messages"]
                .as_array()
                .into_iter()
                .flatten()
                .find(|message| message["role"] == "assistant")
            {
                return first.to_string();
            }
            // timing-guard: poll interval for the reply to be recorded
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the first turn is recorded");
    assert!(reply.contains("x was populated on this machine"), "{reply}");
    assert!(!reply.contains("a was populated"), "{reply}");
    let work = node_dir
        .join("executor")
        .join(&machine.credential_id)
        .join("agents")
        .join(&agent_id)
        .join("work");
    assert!(work.join("a/README.md").exists(), "the pin a is carried");
    assert!(work.join("x/README.md").exists(), "the script populated x");

    let details = wsapi_turn(
        &mut rpc_ws,
        &mut events,
        (&ws_id, &agent_id),
        r#"wsapi const d = await ws.workspace.details(); return JSON.stringify({ marker: "turn-unsent", setup: d.setupStatus });"#,
        "turn-unsent",
        100,
    )
    .await;
    let value = wsapi_value(&details);
    assert_eq!(value["setup"]["state"], "completed", "{value}");
    assert_eq!(value["setup"]["unsentSubmodules"], json!(["x"]), "{value}");
}

/// Whether `work` holds a setup script file (`setup-<uuid>.sh`).
fn entry_is_script(work: &Path) -> bool {
    std::fs::read_dir(work).unwrap().any(|entry| {
        let name = entry.unwrap().file_name().to_string_lossy().into_owned();
        name.starts_with("setup-") && Path::new(&name).extension().is_some_and(|ext| ext == "sh")
    })
}

/// The `name` file the fixture provider wrote in `agent_id`'s worktree under
/// the machine's data dir `root`.
fn find_agent_file(root: &Path, agent_id: &str, name: &str) -> Option<std::path::PathBuf> {
    find_agent_pid_file(root, agent_id)
        .map(|pid| pid.with_file_name(name))
        .filter(|path| path.exists())
}

/// Remote parity S4 (nodeProtocol 11): a remote agent's provider sees the
/// user's MCP servers as servers in its own session, and their tool calls
/// run in head's MCP hub, as the agent (head runs with the bound-caller
/// assertion armed). The machine gets names and ids only: no command,
/// environment or secret. A server a local agent would not get (disabled)
/// is not offered. `ws.mcp.*` is forwarded to head too.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_remote_agents_provider_calls_user_mcp_tools_in_heads_hub() {
    const SECRET: &str = "s4-mcp-secret-5d1e";
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping remote S4 e2e: node or git not on PATH");
        return;
    }
    let mcp_script = format!(
        "{}/tests/fixtures/mock-mcp-server.mjs",
        env!("CARGO_MANIFEST_DIR")
    );
    let scratch = common::test_tempdir_in("/tmp", "itd-s4-");
    let repo = scratch.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    std::fs::write(repo.join("README.md"), "base\n").unwrap();
    git(&repo, &["add", "README.md"]);
    git(&repo, &["commit", "--quiet", "-m", "base"]);
    let (_head, port, cfg, ws_id) = start_backend(&scratch.path().join("head"), &repo).await;
    let mut rpc_ws = connect(port, cfg.clone()).await;
    let created = ok(
        &mut rpc_ws,
        1,
        "mcp.servers.create",
        json!({ "config": {
            "name": "Echo",
            "transport": "stdio",
            "command": "node",
            "args": [mcp_script],
            "env": { "MOCK_MCP_SECRET": SECRET },
            "enabled": false,
        } }),
    )
    .await;
    let server_id = created["server"]["id"].as_str().unwrap().to_owned();
    ok(
        &mut rpc_ws,
        2,
        "mcp.servers.create",
        json!({ "config": {
            "name": "Off",
            "transport": "stdio",
            "command": "node",
            "args": [mcp_script],
            "enabled": false,
        } }),
    )
    .await;
    let toggled = ok(
        &mut rpc_ws,
        3,
        "mcp.servers.toggle",
        json!({ "serverId": server_id, "enabled": true }),
    )
    .await;
    assert_eq!(toggled["status"]["state"], "running", "{toggled}");
    let hub_pid = toggled["status"]["pid"].as_u64().expect("stdio pid");
    let backend = ok(&mut rpc_ws, 100, "remoteMachines.backendInfo", json!({})).await;
    let backend_id = backend["backendId"].as_str().unwrap().to_owned();
    let node_dir = scratch.path().join("node");
    std::fs::create_dir_all(&node_dir).unwrap();
    let machine = start_machine(&node_dir, &backend_id).await;
    ok(
        &mut rpc_ws,
        101,
        "remoteMachines.add",
        add_params(&machine, "E2E node"),
    )
    .await;
    let mut events = connect(port, cfg.clone()).await;
    ok(
        &mut events,
        102,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    let create = json!({
        "workspaceId": ws_id, "name": "Remote", "model": "default", "provider": "mock",
        "placement": { "target": "remote" },
    });
    let deadline = tokio::time::Instant::now() + wait();
    let mut id = 110;
    let created = loop {
        id += 1;
        let response = rpc(&mut rpc_ws, id, "agent.create", create.clone()).await;
        if response.get("error").is_none() {
            break response["result"].clone();
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the machine never took the placement: {response}"
        );
        // timing-guard: poll interval while the machine daemon binds to head (no event exposes it)
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let agent_id = created["agent"]["id"].as_str().unwrap().to_owned();

    // The provider starts the server named in its session and calls an
    // (unlisted) tool: it answers from head's hub process, with the secret
    // only head's config holds.
    let reply = turn(
        &mut rpc_ws,
        &mut events,
        (&ws_id, &agent_id),
        r#"mcp Echo whoami {"input":"from-provider"}"#,
        200,
    )
    .await;
    let body: Value = serde_json::from_str(
        reply
            .strip_prefix("mcp: ")
            .unwrap_or_else(|| panic!("{reply}")),
    )
    .unwrap();
    assert!(body["capabilities"]["tools"].is_object(), "{body}");
    assert_eq!(body["tools"], json!(["echo", "reverse"]), "{body}");
    assert!(body["error"].is_null(), "{body}");
    let who: Value =
        serde_json::from_str(body["result"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(
        who["pid"].as_u64(),
        Some(hub_pid),
        "ran in head's hub: {who}"
    );
    assert_eq!(who["secret"], SECRET, "{who}");
    assert_eq!(who["input"], "from-provider", "{who}");

    // The provider's session names the servers, nothing about them.
    let session = std::fs::read_to_string(
        find_agent_file(&node_dir, &agent_id, "session-mcp.json")
            .expect("the provider recorded its session servers"),
    )
    .unwrap();
    let servers: Value = serde_json::from_str(&session).unwrap();
    let names: Vec<&str> = servers
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|server| server["name"].as_str())
        .collect();
    assert_eq!(names, ["workspace-mcp", "Echo"], "{session}");
    assert!(!session.contains(SECRET), "{session}");
    assert!(!session.contains("mock-mcp-server.mjs"), "{session}");

    // ws.mcp.* is forwarded and runs on head's hub as the agent.
    let wsapi = format!(
        r#"wsapi const s = await ws.mcp.listServers(); const c = await ws.mcp.callTool({server_id:?}, "whoami", {{ input: "from-wsapi" }}); return JSON.stringify({{ marker: "turn-300", s, c }});"#
    );
    let reply = wsapi_turn(
        &mut rpc_ws,
        &mut events,
        (&ws_id, &agent_id),
        &wsapi,
        "turn-300",
        300,
    )
    .await;
    assert!(reply.starts_with("wsapi: "), "{reply}");
    let answer = wsapi_value(&reply);
    let ids: Vec<&str> = answer["s"]["servers"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|server| server["id"].as_str())
        .collect();
    assert!(ids.contains(&server_id.as_str()), "{answer}");
    let who: Value =
        serde_json::from_str(answer["c"]["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(who["pid"].as_u64(), Some(hub_pid), "{who}");
    assert_eq!(who["input"], "from-wsapi", "{who}");
    drop(machine);
}

fn write_skill(root: &Path, name: &str, description: &str) {
    let dir = root.join(name);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(
        dir.join("SKILL.md"),
        format!("---\nname: {name}\ndescription: {description}\n---\n\nUse it.\n"),
    )
    .unwrap();
}

fn skill_names(listed: &Value) -> Vec<String> {
    listed["skills"]
        .as_array()
        .unwrap_or_else(|| panic!("skill.list with agentId is the envelope: {listed}"))
        .iter()
        .map(|skill| skill["name"].as_str().unwrap().to_owned())
        .collect()
}

/// A remote agent's skills are its machine's (parity S7, node-link.md §
/// Skills on the machine): before its first start `skill.list` with its
/// `agentId` merges the machine's user skills with the workspace's project
/// skills; its first prompt carries the machine's catalog; once the machine
/// reports it, `skills:changed` names the agent and `skill.list` answers
/// with machine paths. The backend's own user skills never appear. A
/// message starting with a skill the machine lacks still runs, with a
/// `skill_unavailable` notice.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_remote_agents_skills_come_from_its_machine() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping remote skills e2e: node or git not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-skills-");
    let repo = scratch.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    write_skill(
        &repo.join(".agents/skills"),
        "proj-skill",
        "from the project",
    );
    git(&repo, &["add", "."]);
    git(&repo, &["commit", "--quiet", "-m", "base"]);
    let head_claude = scratch.path().join("head-claude");
    write_skill(
        &head_claude.join("skills"),
        "backend-only",
        "on the backend",
    );
    let head_claude_env = head_claude.to_string_lossy().into_owned();
    let (_head, port, cfg, ws_id) = start_backend_with(
        &scratch.path().join("head"),
        &repo,
        &[("CLAUDE_CONFIG_DIR", &head_claude_env)],
    )
    .await;
    let mut rpc_ws = connect(port, cfg.clone()).await;
    let caps = ok(&mut rpc_ws, 1, "system.capabilities", json!({})).await;
    assert_eq!(caps["remoteAgentSkills"], 1, "{caps}");
    let backend = ok(&mut rpc_ws, 2, "remoteMachines.backendInfo", json!({})).await;
    let backend_id = backend["backendId"].as_str().unwrap().to_owned();
    let node_dir = scratch.path().join("node");
    std::fs::create_dir_all(&node_dir).unwrap();
    let node_claude = scratch.path().join("node-claude");
    write_skill(
        &node_claude.join("skills"),
        "machine-skill",
        "on the machine",
    );
    let node_claude_env = node_claude.to_string_lossy().into_owned();
    let machine = start_machine_with(
        &node_dir,
        &backend_id,
        &[("CLAUDE_CONFIG_DIR", &node_claude_env)],
    )
    .await;
    ok(
        &mut rpc_ws,
        3,
        "remoteMachines.add",
        add_params(&machine, "E2E node"),
    )
    .await;
    let mut events = connect(port, cfg.clone()).await;
    ok(
        &mut events,
        4,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    let mut skill_events = connect(port, cfg.clone()).await;
    ok(
        &mut skill_events,
        5,
        "events.subscribe",
        json!({ "eventTypes": ["skills:changed"] }),
    )
    .await;
    let created = create_remote(&mut rpc_ws, &ws_id, "Remote", 10).await;
    let agent_id = created["agent"]["id"].as_str().unwrap().to_owned();

    let before = ok(
        &mut rpc_ws,
        40,
        "skill.list",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let names = skill_names(&before);
    assert!(names.contains(&"machine-skill".to_owned()), "{before}");
    assert!(names.contains(&"proj-skill".to_owned()), "{before}");
    assert!(!names.contains(&"backend-only".to_owned()), "{before}");
    assert!(before.get("unavailable").is_none(), "{before}");
    let local = ok(
        &mut rpc_ws,
        41,
        "skill.list",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert!(
        local
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["name"] == "backend-only"),
        "without agentId the backend's own skills: {local}"
    );

    let reply = turn(
        &mut rpc_ws,
        &mut events,
        (&ws_id, &agent_id),
        "echo-skills",
        50,
    )
    .await;
    assert!(reply.contains("machine-skill"), "{reply}");
    assert!(reply.contains("proj-skill"), "{reply}");
    assert!(!reply.contains("backend-only"), "{reply}");

    let changed = loop {
        let changed = event(&mut skill_events, "skills:changed").await;
        if changed["data"]["agentId"] == agent_id.as_str() {
            break changed;
        }
    };
    assert_eq!(changed["data"]["workspaceId"], ws_id.as_str(), "{changed}");
    let after = ok(
        &mut rpc_ws,
        60,
        "skill.list",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let rows = after["skills"].as_array().unwrap();
    let machine_row = rows
        .iter()
        .find(|s| s["name"] == "machine-skill")
        .unwrap_or_else(|| panic!("{after}"));
    assert!(
        machine_row["location"]
            .as_str()
            .unwrap()
            .contains("node-claude"),
        "{machine_row}"
    );
    let project_row = rows.iter().find(|s| s["name"] == "proj-skill").unwrap();
    assert_eq!(project_row["scope"], "project");
    assert!(
        !project_row["location"]
            .as_str()
            .unwrap()
            .starts_with(&*repo.to_string_lossy()),
        "the project skill is the one in the machine's checkout: {project_row}"
    );
    assert!(after.get("stale").is_none(), "{after}");

    turn(
        &mut rpc_ws,
        &mut events,
        (&ws_id, &agent_id),
        "/no-such-skill please",
        70,
    )
    .await;
    turn(
        &mut rpc_ws,
        &mut events,
        (&ws_id, &agent_id),
        "/machine-skill please",
        72,
    )
    .await;
    let convo = ok(
        &mut rpc_ws,
        80,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let notices: Vec<&Value> = convo["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|m| m["metadata"]["type"] == "skill_unavailable")
        .collect();
    assert_eq!(notices.len(), 1, "{convo}");
    assert_eq!(notices[0]["metadata"]["skill"], "no-such-skill");
    assert_eq!(notices[0]["metadata"]["machine"], "E2E node");
    let text = notices[0]["contentBlocks"].to_string();
    assert!(text.contains("/no-such-skill"), "{text}");
    assert!(text.contains("E2E node"), "{text}");
}
