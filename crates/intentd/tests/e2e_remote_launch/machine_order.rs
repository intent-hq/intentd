//! The ordered machine list (monorepo `docs/protocol/methods/nodes.md`,
//! "Ordered machine list") against a real backend and two real machines:
//! the default list keeps "This computer" first so an unplaced agent still
//! runs on the backend; after the owner moves machine B to the top, an
//! unplaced agent, an unplaced `workspace.create` initial agent and
//! `target: "remote"` without a `nodeId` all go to B; a reorder that does not
//! name the current rows is `order-stale`; the order survives a backend
//! restart; and with B gone the walk falls through to the backend.

use super::*;

#[track_caller]
fn keys(order: &Value) -> Vec<String> {
    let Some(rows) = order.as_array() else {
        panic!("no order array: {order}");
    };
    rows.iter()
        .map(|row| match row["kind"].as_str() {
            Some("local") => "local".to_owned(),
            Some("machine") => row["nodeId"].as_str().unwrap().to_owned(),
            Some("cloud") => row["cloudMachineId"].as_str().unwrap().to_owned(),
            other => panic!("unknown row kind {other:?}: {row}"),
        })
        .collect()
}

fn key(id: &str) -> Value {
    if id == "local" {
        json!({ "kind": "local" })
    } else {
        json!({ "kind": "machine", "nodeId": id })
    }
}

/// The list of the next `remoteMachine:changed { order }`, skipping the
/// machine-state variants.
async fn order_event(events: &mut common::TlsWs) -> Value {
    loop {
        let changed = event(events, "remoteMachine:changed").await;
        if let Some(order) = changed["data"].get("order") {
            return order.clone();
        }
    }
}

/// Create an unplaced agent, retrying while the machine's first report is
/// not in yet (its refusal is retryable until then).
async fn create_until_placed(
    rpc_ws: &mut common::TlsWs,
    ws_id: &str,
    name: &str,
    placement: Option<Value>,
    first_id: i64,
) -> Value {
    let deadline = tokio::time::Instant::now() + wait();
    let mut id = first_id;
    loop {
        id += 1;
        let mut params =
            json!({ "workspaceId": ws_id, "name": name, "model": "default", "provider": "mock" });
        if let Some(placement) = &placement {
            params["placement"] = placement.clone();
        }
        let response = rpc(rpc_ws, id, "agent.create", params).await;
        if response.get("error").is_none() {
            return response["result"]["agent"].clone();
        }
        assert_eq!(response["error"]["data"]["retryable"], true, "{response}");
        assert!(
            tokio::time::Instant::now() < deadline,
            "the agent was never placed: {response}"
        );
        // timing-guard: poll interval while the machine observes its providers (no event exposes it)
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unplaced_agents_walk_the_users_machine_order() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping machine order e2e: node or git not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-order-");
    let repo = scratch.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    std::fs::write(repo.join("README.md"), "base\n").unwrap();
    git(&repo, &["add", "README.md"]);
    git(&repo, &["commit", "--quiet", "-m", "base"]);
    let head_dir = scratch.path().join("head");
    let (head, port, cfg, ws_id) = start_backend(&head_dir, &repo).await;

    let mut rpc_ws = connect(port, cfg.clone()).await;
    let hello = ok(
        &mut rpc_ws,
        1,
        "client.hello",
        json!({ "clientId": "order-e2e", "name": "Order E2E" }),
    )
    .await;
    assert_eq!(
        hello["server"]["capabilities"]["machineOrder"], 1,
        "{hello}"
    );

    let mut events = connect(port, cfg.clone()).await;
    ok(
        &mut events,
        2,
        "events.subscribe",
        json!({ "eventTypes": ["remoteMachine:changed"] }),
    )
    .await;

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
    let a_id = machines[0].daemon_id.clone();
    let b_id = machines[1].daemon_id.clone();

    // New machines are appended; "This computer" stays first.
    let order = ok(&mut rpc_ws, 10, "remoteMachines.order", json!({})).await;
    assert_eq!(keys(&order["order"]), ["local", &a_id, &b_id], "{order}");
    assert_eq!(order["order"][1]["name"], "Machine A", "{order}");
    let announced = loop {
        let order = order_event(&mut events).await;
        if keys(&order).len() == 3 {
            break order;
        }
    };
    assert_eq!(keys(&announced), ["local", &a_id, &b_id]);

    // Unchanged default: an unplaced agent runs on the backend.
    let local = create_until_placed(&mut rpc_ws, &ws_id, "Local", None, 20).await;
    assert!(local.get("nodeId").is_none(), "{local}");
    // Wait for B's first report, so the walk below decides on it.
    let warm = create_until_placed(
        &mut rpc_ws,
        &ws_id,
        "Warm",
        Some(json!({ "target": "remote", "nodeId": b_id })),
        25,
    )
    .await;
    assert_eq!(warm["nodeId"], b_id.as_str(), "{warm}");

    // A reorder must name exactly the current rows.
    let stale = rpc(
        &mut rpc_ws,
        30,
        "remoteMachines.setOrder",
        json!({ "order": [key(&b_id), key("local")] }),
    )
    .await;
    assert_eq!(stale["error"]["code"], -32602, "{stale}");
    assert_eq!(stale["error"]["data"]["code"], "order-stale", "{stale}");
    assert_eq!(
        keys(&stale["error"]["data"]["order"]),
        ["local", &a_id, &b_id]
    );

    let reordered = ok(
        &mut rpc_ws,
        31,
        "remoteMachines.setOrder",
        json!({ "order": [key(&b_id), key("local"), key(&a_id)] }),
    )
    .await;
    assert_eq!(keys(&reordered["order"]), [&b_id, "local", &a_id]);
    let changed = order_event(&mut events).await;
    assert_eq!(keys(&changed), [&b_id, "local", &a_id]);

    // B is first now: unplaced and `target: "remote"` both go to B.
    let walked = create_until_placed(&mut rpc_ws, &ws_id, "Walked", None, 40).await;
    assert_eq!(walked["nodeId"], b_id.as_str(), "{walked}");
    let remote = create_until_placed(
        &mut rpc_ws,
        &ws_id,
        "Remote",
        Some(json!({ "target": "remote" })),
        50,
    )
    .await;
    assert_eq!(remote["nodeId"], b_id.as_str(), "{remote}");
    // An explicit local placement never walks.
    let pinned = create_until_placed(
        &mut rpc_ws,
        &ws_id,
        "Pinned",
        Some(json!({ "target": "local" })),
        60,
    )
    .await;
    assert!(pinned.get("nodeId").is_none(), "{pinned}");

    // `workspace.create`'s unplaced initial agent walks the list too.
    let branch = String::from_utf8(
        std::process::Command::new("git")
            .args(["rev-parse", "--abbrev-ref", "HEAD"])
            .current_dir(&repo)
            .output()
            .unwrap()
            .stdout,
    )
    .unwrap()
    .trim()
    .to_owned();
    let created = ok(
        &mut rpc_ws,
        70,
        "workspace.create",
        json!({
            "repositoryPath": repo.to_string_lossy(),
            "baseRef": branch,
            "initialAgent": { "model": "default", "provider": "mock" },
        }),
    )
    .await;
    assert_eq!(
        created["initialAgent"]["nodeId"],
        b_id.as_str(),
        "{created}"
    );

    // The order survives a backend restart.
    drop(rpc_ws);
    drop(events);
    drop(head);
    let (_head, port, cfg) = serve_backend(&head_dir, &[]).await;
    let mut rpc_ws = connect(port, cfg).await;
    let order = ok(&mut rpc_ws, 80, "remoteMachines.order", json!({})).await;
    assert_eq!(keys(&order["order"]), [&b_id, "local", &a_id], "{order}");

    // With B gone, the walk falls through to the next row: the backend.
    let b = machines.remove(1);
    drop(b);
    let fallback = create_until_placed(&mut rpc_ws, &ws_id, "Fallback", None, 90).await;
    assert!(fallback.get("nodeId").is_none(), "{fallback}");
}
