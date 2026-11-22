//! Runner load (Homes GA 16; monorepo `docs/protocol/methods/nodes.md`,
//! "Machine model lookup" 4, and `node-link.md` "Load pushed on change")
//! against a real orchestrator and a real machine: the machine pushes its
//! report when its running-agent count changes, so the orchestrator's
//! `remoteMachines.models` row shows `load` going 0 → 1 → 0 as an agent
//! starts there and is deleted, with the host's memory beside it, and the
//! orchestrator itself has no `local`.

use super::orchestrator::orchestrator_config;
use super::*;

/// `remoteMachines.models`' only row's `load.runningAgents`, once it is
/// `want` (an absent `load` reads as `None`).
async fn load_becomes(rpc_ws: &mut common::TlsWs, id: &mut i64, want: Option<u64>) -> Value {
    let mut last = Value::Null;
    let reached = timeout(wait(), async {
        loop {
            *id += 1;
            last = ok(rpc_ws, *id, "remoteMachines.models", json!({})).await;
            if last["machines"][0]["load"]["runningAgents"].as_u64() == want {
                return;
            }
            // timing-guard: poll interval for the machine's next pushed report (the change event is covered by the svc-remote tests)
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await;
    assert!(reached.is_ok(), "load never became {want:?}: {last}");
    last
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_runners_load_follows_its_agents_on_an_orchestrator() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping runner load e2e: node or git not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-load-");
    let repo = scratch.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    std::fs::write(repo.join("README.md"), "base\n").unwrap();
    git(&repo, &["add", "README.md"]);
    git(&repo, &["commit", "--quiet", "-m", "base"]);
    let head_dir = scratch.path().join("head");
    let (head, _, _, ws_id) = start_backend(&head_dir, &repo).await;
    drop(head);
    orchestrator_config(&head_dir);
    let (_head, port, cfg) = serve_backend(&head_dir, &[]).await;
    let mut rpc_ws = connect(port, cfg.clone()).await;
    ok(
        &mut rpc_ws,
        1,
        "client.hello",
        json!({ "clientId": "runner-load-e2e", "name": "Runner load E2E" }),
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
        add_params(&machine, "Runner"),
    )
    .await;
    let models = ok(&mut rpc_ws, 4, "remoteMachines.models", json!({})).await;
    assert!(
        models.get("local").is_none(),
        "an orchestrator runs nothing: {models}"
    );
    assert_ne!(
        models["machines"][0]["load"]["runningAgents"], 1,
        "no agent runs yet: {models}"
    );

    let mut events = connect(port, cfg.clone()).await;
    ok(
        &mut events,
        5,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    let create =
        json!({ "workspaceId": ws_id, "name": "Loaded", "model": "default", "provider": "mock" });
    let deadline = tokio::time::Instant::now() + wait();
    let mut id = 10;
    let agent_id = loop {
        id += 1;
        let response = rpc(&mut rpc_ws, id, "agent.create", create.clone()).await;
        if response.get("error").is_none() {
            break response["result"]["agent"]["id"]
                .as_str()
                .unwrap()
                .to_owned();
        }
        assert_eq!(response["error"]["data"]["retryable"], true, "{response}");
        assert!(
            tokio::time::Instant::now() < deadline,
            "the machine never took the agent: {response}"
        );
        // timing-guard: poll interval while the machine observes its providers (no event exposes it)
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    let reply = turn(&mut rpc_ws, &mut events, (&ws_id, &agent_id), "hello", 100).await;
    assert!(reply.contains("echo:"), "{reply}");

    let mut id = 200;
    let loaded = load_becomes(&mut rpc_ws, &mut id, Some(1)).await;
    let row = &loaded["machines"][0];
    assert_eq!(row["state"], "connected", "{loaded}");
    assert_eq!(row["runningAgents"], 1, "this backend's agent: {loaded}");
    assert!(
        row["load"]["agentCap"].as_u64().is_some_and(|cap| cap >= 1),
        "{loaded}"
    );
    // The machine samples its host: memory with every report.
    let total = row["load"]["memoryTotalBytes"].as_u64();
    if cfg!(any(target_os = "linux", target_os = "macos")) {
        assert!(total.is_some_and(|total| total > 0), "{loaded}");
        assert!(
            row["load"]["memoryUsedBytes"]
                .as_u64()
                .is_some_and(|used| used <= total.unwrap()),
            "{loaded}"
        );
    }
    let listed = ok(&mut rpc_ws, 300, "remoteMachines.list", json!({})).await;
    assert_eq!(
        listed["machines"][0]["capabilities"]["capacity"]["runningAgents"], 1,
        "{listed}"
    );

    ok(
        &mut rpc_ws,
        301,
        "agent.delete",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let mut id = 400;
    load_becomes(&mut rpc_ws, &mut id, Some(0)).await;
}
