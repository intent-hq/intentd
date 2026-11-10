//! Node invites (monorepo `docs/protocol/methods/nodes.md`, "Node invites and
//! claim"): `intentd pair --node` on a full-mode machine prints an
//! `intent://node` link with no owner token; the backend claims it once with
//! `remoteMachines.claim` over the machine's `/node/claim` and then runs an
//! agent there; the claimed executor credential is refused on the owner
//! socket; a second claim, a revoked, an expired, a tampered and a self invite
//! are refused without minting anything.

use super::*;

/// The `URL:` line of `intentd pair --node` output.
fn node_link(stdout: &str) -> String {
    stdout
        .lines()
        .find_map(|line| line.strip_prefix("URL:"))
        .map(|rest| rest.trim().to_owned())
        .unwrap_or_else(|| panic!("no URL line: {stdout}"))
}

/// Run `intentd pair --node` against the daemon in `data_dir`, retrying
/// while its listener is still starting.
async fn pair_node(data_dir: &Path, args: &[&str]) -> std::process::Output {
    let deadline = tokio::time::Instant::now() + wait();
    loop {
        let output = Command::new(env!("CARGO_BIN_EXE_intentd"))
            .args(["pair", "--node"])
            .args(args)
            .env("INTENTD_DATA_DIR", data_dir)
            .stdin(Stdio::null())
            .output()
            .expect("run intentd pair --node");
        if output.status.success() || tokio::time::Instant::now() >= deadline {
            return output;
        }
        // timing-guard: the WSS listener binds after the UDS socket accepts
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[track_caller]
fn claim_error(response: &Value, code: &str) {
    assert_eq!(response["error"]["data"]["code"], code, "{response}");
}

/// The secret-store value `account` on the daemon whose data dir is `dir`.
fn stored_secret(dir: &Path, account: &str) -> Option<String> {
    let bytes = std::fs::read(dir.join("secrets.json")).ok()?;
    let map: serde_json::Map<String, Value> = serde_json::from_slice(&bytes).ok()?;
    map.get(account)?.as_str().map(str::to_owned)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_invite_is_claimed_once_and_runs_agents() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping node invite e2e: node or git not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-ninv-");
    let repo = scratch.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    std::fs::write(repo.join("README.md"), "base\n").unwrap();
    git(&repo, &["add", "README.md"]);
    git(&repo, &["commit", "--quiet", "-m", "base"]);

    let head_dir = scratch.path().join("head");
    let (_head, port, cfg, ws_id) = start_backend(&head_dir, &repo).await;
    let mut rpc_ws = connect(port, cfg.clone()).await;
    let hello = ok(
        &mut rpc_ws,
        1,
        "client.hello",
        json!({ "clientId": "node-invite-e2e", "name": "Node invite E2E" }),
    )
    .await;
    assert_eq!(
        hello["server"]["capabilities"]["remoteMachineClaim"],
        json!(1),
        "{hello}"
    );

    // A full-mode machine: `pair --node` works without node mode.
    let node_dir = scratch.path().join("node");
    std::fs::create_dir_all(&node_dir).unwrap();
    let (_machine, machine_port, fingerprint) = spawn_machine(&node_dir, &[]).await;
    let output = pair_node(&node_dir, &["--yes", "--host", "127.0.0.1"]).await;
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    assert!(
        output.status.success(),
        "pair --node failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let uri = node_link(&stdout);
    assert!(
        uri.starts_with("intent://node?v=1&host=127.0.0.1&"),
        "{uri}"
    );
    assert!(uri.contains("&invite=") && uri.contains("&secret="), "{uri}");
    assert!(
        !stdout.contains(TOKEN) && !uri.contains("token="),
        "the node link must not carry the owner token: {stdout}"
    );
    assert!(stdout.contains("intentd pair --node"), "{stdout}");
    let secret = uri
        .split('&')
        .find_map(|pair| pair.strip_prefix("secret="))
        .unwrap()
        .to_owned();

    let mut machine_ws = connect(machine_port, client_config(&fingerprint)).await;
    let listed = ok(&mut machine_ws, 2, "executor.list", json!({})).await;
    assert_eq!(listed["invites"].as_array().unwrap().len(), 1, "{listed}");
    assert!(listed["backends"].as_array().unwrap().is_empty(), "{listed}");
    let invite_id = listed["invites"][0]["inviteId"].as_str().unwrap().to_owned();
    assert!(!listed.to_string().contains(&secret), "{listed}");

    let claimed = ok(&mut rpc_ws, 3, "remoteMachines.claim", json!({ "uri": uri })).await;
    let node_id = claimed["machine"]["id"].as_str().unwrap().to_owned();
    assert!(!claimed.to_string().contains(&secret), "{claimed}");
    let machines = ok(&mut rpc_ws, 4, "remoteMachines.list", json!({})).await;
    assert_eq!(machines["machines"][0]["id"], node_id.as_str(), "{machines}");

    // Spent: no open invite, one credential that names it.
    let listed = ok(&mut machine_ws, 5, "executor.list", json!({})).await;
    assert!(listed["invites"].as_array().unwrap().is_empty(), "{listed}");
    assert_eq!(listed["backends"].as_array().unwrap().len(), 1, "{listed}");
    assert_eq!(listed["backends"][0]["inviteId"], invite_id.as_str(), "{listed}");

    let again = rpc(&mut rpc_ws, 6, "remoteMachines.claim", json!({ "uri": uri })).await;
    claim_error(&again, "node-invite-used");
    assert!(!again.to_string().contains(&secret), "{again}");
    let listed = ok(&mut machine_ws, 7, "executor.list", json!({})).await;
    assert_eq!(listed["backends"].as_array().unwrap().len(), 1, "{listed}");

    // The claimed credential runs agents.
    let mut events = connect(port, cfg.clone()).await;
    ok(
        &mut events,
        2,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    let agent_id = super::secrets::create_agent(&mut rpc_ws, &ws_id, "Invited", 10).await;
    let reply = turn(&mut rpc_ws, &mut events, (&ws_id, &agent_id), "echo hello", 100).await;
    assert!(!reply.is_empty(), "the remote agent never answered");

    // ...but is no owner token: the machine's JSON-RPC socket refuses it.
    let token = stored_secret(&head_dir, &format!("remoteMachines.{node_id}.token"))
        .expect("the backend keeps the claimed executor token");
    let url = format!("wss://localhost:{machine_port}/ws?token={token}");
    let refused = common::try_wss_connect(machine_port, client_config(&fingerprint), &url).await;
    assert!(refused.is_err(), "an executor token opened the owner socket");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn revoked_expired_tampered_and_self_invites_are_refused() {
    let scratch = common::test_tempdir_in("/tmp", "itd-ninv-r-");
    let repo = scratch.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    let head_dir = scratch.path().join("head");
    let (_head, port, cfg, _ws_id) = start_backend(&head_dir, &repo).await;
    let mut rpc_ws = connect(port, cfg).await;

    let node_dir = scratch.path().join("node");
    std::fs::create_dir_all(&node_dir).unwrap();
    let (_machine, machine_port, fingerprint) = spawn_machine(&node_dir, &[]).await;
    let mut machine_ws = connect(machine_port, client_config(&fingerprint)).await;
    let mint = json!({ "host": "127.0.0.1", "label": "e2e" });
    let revoked = ok(&mut machine_ws, 2, "executor.invite.create", mint.clone()).await;
    let expired = ok(&mut machine_ws, 3, "executor.invite.create", mint.clone()).await;
    let tampered = ok(&mut machine_ws, 4, "executor.invite.create", mint.clone()).await;
    assert_eq!(revoked["invite"]["label"], "e2e", "{revoked}");
    assert!(!revoked["uri"].as_str().unwrap().contains("token="), "{revoked}");
    let listed = ok(&mut machine_ws, 5, "executor.list", json!({})).await;
    assert_eq!(listed["invites"].as_array().unwrap().len(), 3, "{listed}");

    ok(
        &mut machine_ws,
        6,
        "executor.revoke",
        json!({ "inviteId": revoked["invite"]["inviteId"] }),
    )
    .await;
    let response = rpc(
        &mut rpc_ws,
        7,
        "remoteMachines.claim",
        json!({ "uri": revoked["uri"] }),
    )
    .await;
    claim_error(&response, "node-invite-revoked");

    let pool = sqlx::SqlitePool::connect(&format!(
        "sqlite://{}",
        node_dir.join("intentd.db").display()
    ))
    .await
    .expect("open the machine's store");
    sqlx::query("UPDATE node_invite SET expires_at = '2000-01-01T00:00:00Z' WHERE id = ?")
        .bind(expired["invite"]["inviteId"].as_str().unwrap())
        .execute(&pool)
        .await
        .expect("expire the invite");
    pool.close().await;
    let response = rpc(
        &mut rpc_ws,
        8,
        "remoteMachines.claim",
        json!({ "uri": expired["uri"] }),
    )
    .await;
    claim_error(&response, "node-invite-expired");
    assert_eq!(
        response["error"]["data"]["expiresAt"], "2000-01-01T00:00:00Z",
        "{response}"
    );

    let secret = tampered["secret"].as_str().unwrap();
    let wrong = tampered["uri"]
        .as_str()
        .unwrap()
        .replace(secret, &"0".repeat(secret.len()));
    let response = rpc(&mut rpc_ws, 9, "remoteMachines.claim", json!({ "uri": wrong })).await;
    claim_error(&response, "node-invite-invalid");
    assert_eq!(response["error"]["data"]["inviteSpent"], Value::Null, "{response}");

    let response = rpc(
        &mut rpc_ws,
        10,
        "remoteMachines.claim",
        json!({ "uri": "intent://pair?v=1&host=127.0.0.1&port=1&fp=AA&token=x" }),
    )
    .await;
    claim_error(&response, "invalid-node-link");

    // Nothing was minted, and the tampered invite is still open.
    let listed = ok(&mut machine_ws, 11, "executor.list", json!({})).await;
    assert!(listed["backends"].as_array().unwrap().is_empty(), "{listed}");
    assert_eq!(listed["invites"].as_array().unwrap().len(), 1, "{listed}");

    // The backend's own invite.
    let own = ok(&mut rpc_ws, 12, "executor.invite.create", json!({ "host": "127.0.0.1" })).await;
    let response = rpc(&mut rpc_ws, 13, "remoteMachines.claim", json!({ "uri": own["uri"] })).await;
    claim_error(&response, "node-invite-self");
}
