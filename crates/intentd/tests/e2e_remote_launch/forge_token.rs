//! The backend's GitHub token reaches its remote agents (nodeProtocol 15,
//! monorepo `docs/protocol/node-link.md` § Forge token on the machine): the
//! backend sends it after the bind, every agent the machine spawns gets it
//! as `GH_TOKEN`, a backend restart (a new `headSession`, then a fresh bind)
//! hands it over again, and a protocol-14 machine gets no frame and starts
//! its agents as before. The value never reaches a file of either daemon
//! (logs and stores included), an event, an agent's journal, or the link
//! traffic capture.

use super::secrets::{agent_turn, create_agent, files_containing};
use super::*;

/// Distinctive, so the scans cannot match anything by accident.
const TOKEN: &str = "ghp_forgeE2eCanary7Rq2Lw9Zt4";

/// Where the backend resolves its GitHub token first.
const TOKEN_ACCOUNT: &str = "sourceControl.github.token";

fn sha_hex(value: &str) -> String {
    lower_hex(&Sha256::digest(value.as_bytes()))
}

/// A workspace repository with one commit, a backend whose secret store
/// holds [`TOKEN`], and the backend's id.
async fn backend_with_token(
    scratch: &Path,
) -> (common::DaemonGuard, u16, Arc<ClientConfig>, String, String) {
    let repo = scratch.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    std::fs::write(repo.join("README.md"), "base\n").unwrap();
    git(&repo, &["add", "README.md"]);
    git(&repo, &["commit", "--quiet", "-m", "base"]);
    let head_dir = scratch.join("head");
    std::fs::create_dir_all(&head_dir).unwrap();
    intent_core::FileSecretStore::with_path(head_dir.join("secrets.json"))
        .store(TOKEN_ACCOUNT, TOKEN)
        .unwrap();
    let (head, port, cfg, ws_id) = start_backend(&head_dir, &repo).await;
    let mut rpc_ws = connect(port, cfg.clone()).await;
    let backend = ok(&mut rpc_ws, 1, "remoteMachines.backendInfo", json!({})).await;
    let backend_id = backend["backendId"].as_str().unwrap().to_owned();
    (head, port, cfg, ws_id, backend_id)
}

/// The agent's `GH_TOKEN`, as the SHA-256 the test agent reports (or
/// `unset`).
async fn gh_token_digest(
    rpc_ws: &mut common::TlsWs,
    events: &mut common::TlsWs,
    (ws_id, agent_id): (&str, &str),
    id: i64,
) -> String {
    let tag = format!("t{id}");
    let reply = agent_turn(
        rpc_ws,
        events,
        (ws_id, agent_id),
        &format!("sha-env GH_TOKEN {tag}"),
        &format!("sha {tag} "),
        id,
    )
    .await;
    let at = reply.find(&format!("sha {tag} ")).unwrap() + tag.len() + 5;
    reply[at..]
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_owned()
}

/// Every text frame `socket` received so far.
async fn drain(socket: &mut common::TlsWs) -> String {
    let mut seen = String::new();
    // fixed-deadline: allow — a quiet window that ends the drain; a longer one only collects more
    while let Ok(Some(Ok(message))) = timeout(Duration::from_millis(500), socket.next()).await {
        if let Message::Text(text) = message {
            seen.push_str(&text);
            seen.push('\n');
        }
    }
    seen
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_remote_agent_gets_the_backends_github_token_and_nothing_records_it() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping forge token e2e: node or git not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-forge-");
    let head_dir = scratch.path().join("head");
    let (head, port, cfg, ws_id, backend_id) = backend_with_token(scratch.path()).await;
    let mut rpc_ws = connect(port, cfg.clone()).await;
    ok(
        &mut rpc_ws,
        2,
        "remoteLinkTraffic.start",
        json!({ "payloads": true }),
    )
    .await;
    let mut all_events = connect(port, cfg.clone()).await;
    ok(
        &mut all_events,
        3,
        "events.subscribe",
        json!({ "eventTypes": ["*"], "workspaceId": ws_id }),
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
    let node_dir = scratch.path().join("node");
    std::fs::create_dir_all(&node_dir).unwrap();
    let machine = start_machine(&node_dir, &backend_id).await;
    ok(
        &mut rpc_ws,
        5,
        "remoteMachines.add",
        add_params(&machine, "E2E node"),
    )
    .await;

    // Every agent on the machine runs with the backend's token.
    let first = create_agent(&mut rpc_ws, &ws_id, "First", 10).await;
    let second = create_agent(&mut rpc_ws, &ws_id, "Second", 40).await;
    for (agent, id) in [(&first, 100), (&second, 200)] {
        assert_eq!(
            gh_token_digest(&mut rpc_ws, &mut events, (&ws_id, agent), id).await,
            sha_hex(TOKEN),
            "{agent} has GH_TOKEN"
        );
    }

    // The capture shows the frame, never the value.
    let traffic = ok(&mut rpc_ws, 400, "remoteLinkTraffic.snapshot", json!({})).await;
    let captured = traffic.to_string();
    assert!(captured.contains("forgeToken"), "{captured}");
    assert!(!captured.contains(TOKEN), "the capture holds the token");

    // A backend restart: the machine drops the token with the old
    // headSession, and the new bind hands it over again.
    let seen_events = drain(&mut all_events).await;
    drop(rpc_ws);
    drop(events);
    drop(all_events);
    drop(head);
    let (head, port, cfg) = serve_backend(&head_dir, &[]).await;
    let mut rpc_ws = connect(port, cfg.clone()).await;
    let mut events = connect(port, cfg.clone()).await;
    ok(
        &mut events,
        500,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    let third = create_agent(&mut rpc_ws, &ws_id, "Third", 510).await;
    assert_eq!(
        gh_token_digest(&mut rpc_ws, &mut events, (&ws_id, &third), 600).await,
        sha_hex(TOKEN),
        "an agent spawned after the rebind has GH_TOKEN"
    );

    // Nothing recorded the value: not the head's store, WAL or log (its
    // secret store is where it comes from), not the machine's journal,
    // store or log, and no event.
    assert!(!seen_events.contains(TOKEN), "an event carried the token");
    assert_eq!(
        files_containing(&head_dir, TOKEN),
        vec![head_dir.join("secrets.json")]
    );
    assert_eq!(
        files_containing(&node_dir, TOKEN),
        Vec::<std::path::PathBuf>::new()
    );
    drop(head);
    drop(machine);
}

/// A protocol-14 machine gets no `forgeToken` frame and starts its agents
/// as before, without `GH_TOKEN`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_older_machine_starts_its_agents_without_the_token() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping forge token e2e: node or git not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-forge14-");
    let (_head, port, cfg, ws_id, backend_id) = backend_with_token(scratch.path()).await;
    let mut rpc_ws = connect(port, cfg.clone()).await;
    ok(
        &mut rpc_ws,
        2,
        "remoteLinkTraffic.start",
        json!({ "payloads": true }),
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
    let node_dir = scratch.path().join("node");
    std::fs::create_dir_all(&node_dir).unwrap();
    let machine = start_machine_with(
        &node_dir,
        &backend_id,
        &[("INTENTD_TEST_NODE_LINK_PROTOCOL", "14")],
    )
    .await;
    ok(
        &mut rpc_ws,
        4,
        "remoteMachines.add",
        add_params(&machine, "Older node"),
    )
    .await;
    let agent = create_agent(&mut rpc_ws, &ws_id, "Older", 10).await;
    assert_eq!(
        gh_token_digest(&mut rpc_ws, &mut events, (&ws_id, &agent), 100).await,
        "unset"
    );
    let traffic = ok(&mut rpc_ws, 200, "remoteLinkTraffic.snapshot", json!({})).await;
    let captured = traffic.to_string();
    assert!(captured.contains("launch"), "{captured}");
    assert!(!captured.contains("forgeToken"), "{captured}");
    drop(machine);
}
