//! A machine that becomes an orchestrator leaves the backends that used it
//! (monorepo `docs/protocol/methods/nodes.md`, "Orchestrator mode"): a real
//! backend adds a real machine; the machine restarts as an orchestrator; the
//! backend's next dial fails with `machine-orchestrator`, and the backend
//! removes the machine row, its order entry and its stored token, and
//! announces the removal. Switched back to a full daemon, the machine pairs
//! again with a new credential.

use super::orchestrator::orchestrator_config;
use super::*;

/// Rewrite the machine's `[daemon] mode` back to a full daemon.
fn regular_config(data_dir: &Path) {
    let path = data_dir.join("config.toml");
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(text.contains("mode = \"orchestrator\""), "{text}");
    std::fs::write(
        path,
        text.replace("mode = \"orchestrator\"", "mode = \"regular\""),
    )
    .unwrap();
}

fn holds_token(head_dir: &Path, machine_id: &str) -> bool {
    std::fs::read_to_string(head_dir.join("secrets.json"))
        .unwrap_or_default()
        .contains(&format!("remoteMachines.{machine_id}.token"))
}

fn order_keys(order: &Value) -> Vec<String> {
    order["order"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            row["nodeId"]
                .as_str()
                .map_or_else(|| row["kind"].as_str().unwrap().to_owned(), str::to_owned)
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_machine_that_becomes_an_orchestrator_leaves_its_backends() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping orchestrator switch e2e: node or git not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-orch-sw-");
    let repo = scratch.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    std::fs::write(repo.join("README.md"), "base\n").unwrap();
    git(&repo, &["add", "README.md"]);
    git(&repo, &["commit", "--quiet", "-m", "base"]);
    let head_dir = scratch.path().join("head");
    let (_head, port, cfg, _ws_id) = start_backend(&head_dir, &repo).await;
    let mut rpc_ws = connect(port, cfg.clone()).await;
    let mut events = connect(port, cfg.clone()).await;
    ok(
        &mut events,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["remoteMachine:changed"] }),
    )
    .await;

    let backend = ok(&mut rpc_ws, 2, "remoteMachines.backendInfo", json!({})).await;
    let backend_id = backend["backendId"].as_str().unwrap().to_owned();
    let node_dir = scratch.path().join("node-0");
    std::fs::create_dir_all(&node_dir).unwrap();
    let machine = start_machine(&node_dir, &backend_id).await;
    let machine_id = machine.daemon_id.clone();
    ok(
        &mut rpc_ws,
        3,
        "remoteMachines.add",
        add_params(&machine, "Machine A"),
    )
    .await;
    assert!(holds_token(&head_dir, &machine_id));
    let order = ok(&mut rpc_ws, 4, "remoteMachines.order", json!({})).await;
    assert!(order_keys(&order).contains(&machine_id), "{order}");

    // The machine restarts as an orchestrator, on the port its row names.
    let machine_port = machine.port;
    let (credential_id, token) = (machine.credential_id.clone(), machine.token.clone());
    drop(machine);
    orchestrator_config(&node_dir);
    let pinned = machine_port.to_string();
    let (orchestrator, again_port, fingerprint) =
        spawn_machine(&node_dir, &[("INTENTD_TCP_PORT", pinned.as_str())]).await;
    assert_eq!(again_port, machine_port);

    // The next dial is refused, and the backend forgets the machine.
    let refreshed = rpc(
        &mut rpc_ws,
        5,
        "remoteMachines.refresh",
        json!({ "machineId": machine_id }),
    )
    .await;
    assert_eq!(refreshed["error"]["code"], -32602, "{refreshed}");
    assert_eq!(
        refreshed["error"]["data"]["code"], "machine-orchestrator",
        "{refreshed}"
    );
    loop {
        let changed = event(&mut events, "remoteMachine:changed").await;
        if changed["data"]["removed"] == true {
            assert_eq!(changed["data"]["machineId"], machine_id.as_str());
            break;
        }
    }
    let listed = ok(&mut rpc_ws, 6, "remoteMachines.list", json!({})).await;
    assert_eq!(listed["machines"], json!([]), "{listed}");
    let order = ok(&mut rpc_ws, 7, "remoteMachines.order", json!({})).await;
    assert!(!order_keys(&order).contains(&machine_id), "{order}");
    assert!(
        !holds_token(&head_dir, &machine_id),
        "the stored token is gone"
    );

    // Adding it while it is an orchestrator fails and saves nothing.
    let orchestrator = Machine {
        daemon: orchestrator,
        port: machine_port,
        fingerprint: fingerprint.clone(),
        daemon_id: machine_id.clone(),
        credential_id,
        token,
    };
    let refused = rpc(
        &mut rpc_ws,
        10,
        "remoteMachines.add",
        add_params(&orchestrator, "Machine A"),
    )
    .await;
    assert_eq!(refused["error"]["code"], -32602, "{refused}");
    assert_eq!(
        refused["error"]["data"]["code"], "machine-orchestrator",
        "{refused}"
    );
    let listed = ok(&mut rpc_ws, 11, "remoteMachines.list", json!({})).await;
    assert_eq!(listed["machines"], json!([]), "{listed}");
    assert!(!holds_token(&head_dir, &machine_id));

    // Switched back to a full daemon, it pairs again.
    drop(orchestrator);
    regular_config(&node_dir);
    let (_machine, again_port, again_fingerprint) =
        spawn_machine(&node_dir, &[("INTENTD_TCP_PORT", pinned.as_str())]).await;
    assert_eq!(again_fingerprint, fingerprint);
    let mut machine_ws = connect(again_port, client_config(&again_fingerprint)).await;
    let minted = ok(
        &mut machine_ws,
        1,
        "executor.mint",
        json!({ "backendId": backend_id, "backendName": "E2E head" }),
    )
    .await;
    let readded = ok(
        &mut rpc_ws,
        8,
        "remoteMachines.add",
        json!({
            "machineId": machine_id,
            "name": "Machine A",
            "hosts": ["127.0.0.1"],
            "port": again_port,
            "fingerprint": again_fingerprint,
            "credentialId": minted["credentialId"],
            "token": minted["token"],
        }),
    )
    .await;
    assert_eq!(readded["machine"]["id"], machine_id.as_str(), "{readded}");
    let listed = ok(&mut rpc_ws, 9, "remoteMachines.list", json!({})).await;
    assert_eq!(listed["machines"][0]["id"], machine_id.as_str(), "{listed}");
    assert!(holds_token(&head_dir, &machine_id));
}
