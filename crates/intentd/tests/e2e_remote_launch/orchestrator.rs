//! Orchestrator mode (monorepo `docs/protocol/methods/nodes.md`,
//! "Orchestrator mode") against a real backend started with
//! `daemon.mode = "orchestrator"` and a real machine: the mode is reported;
//! the ordered list has no "This computer" row; an unplaced agent with no
//! machine is `no-agent-machine` and a local placement is refused with
//! `orchestrator-mode`, creating nothing; providers are only configured (no
//! test prompt, no sign-in or install probe, no local model catalog); being
//! exposed as a machine is refused; quick actions are never run here; once a
//! machine is added, unplaced agents run there.

use super::*;

/// Switch a backend data dir to orchestrator mode, with Codex as the default
/// provider of its quick actions. The daemon wrote every setting at its last start,
/// so the existing `[daemon]` and `[model]` keys are rewritten in place.
fn orchestrator_config(data_dir: &Path) {
    let path = data_dir.join("config.toml");
    let text = std::fs::read_to_string(&path).unwrap_or_default();
    let mut out = String::new();
    let mut section = String::new();
    let mut seen_mode = false;
    let mut seen_default = false;
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('[') {
            trimmed.clone_into(&mut section);
        }
        if section == "[daemon]" && trimmed.starts_with("mode") {
            out.push_str("mode = \"orchestrator\"\n");
            seen_mode = true;
            continue;
        }
        if section == "[model]" && trimmed.starts_with("defaultProvider") {
            out.push_str("defaultProvider = \"codex\"\n");
            seen_default = true;
            continue;
        }
        out.push_str(line);
        out.push('\n');
    }
    if !seen_mode {
        out.push_str("\n[daemon]\nmode = \"orchestrator\"\n");
    }
    if !seen_default {
        if text.contains("\n[model]\n") || text.starts_with("[model]\n") {
            out = out.replacen("[model]\n", "[model]\ndefaultProvider = \"codex\"\n", 1);
        } else {
            out.push_str("\n[model]\ndefaultProvider = \"codex\"\n");
        }
    }
    std::fs::write(path, out).unwrap();
}

fn agent_count(list: &Value) -> usize {
    list["agents"].as_array().map_or(0, Vec::len)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_orchestrator_runs_nothing_itself() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping orchestrator e2e: node or git not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-orch-");
    let repo = scratch.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    std::fs::write(repo.join("README.md"), "base\n").unwrap();
    git(&repo, &["add", "README.md"]);
    git(&repo, &["commit", "--quiet", "-m", "base"]);
    let head_dir = scratch.path().join("head");
    // A local agent made while the daemon ran as a full daemon.
    let (head, port, cfg, ws_id) = start_backend(&head_dir, &repo).await;
    let mut rpc_ws = connect(port, cfg.clone()).await;
    let before_switch = ok(
        &mut rpc_ws,
        90,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Before", "model": "default", "provider": "mock" }),
    )
    .await;
    let local_id = before_switch["agent"]["id"].as_str().unwrap().to_owned();
    drop(rpc_ws);
    drop(head);
    orchestrator_config(&head_dir);
    let (_head, port, cfg) = serve_backend(&head_dir, &[]).await;
    let mut rpc_ws = connect(port, cfg.clone()).await;

    let hello = ok(
        &mut rpc_ws,
        1,
        "client.hello",
        json!({ "clientId": "orchestrator-e2e", "name": "Orchestrator E2E" }),
    )
    .await;
    assert_eq!(hello["server"]["daemonMode"], "orchestrator", "{hello}");
    assert_eq!(
        hello["server"]["capabilities"]["machineOrder"], 1,
        "{hello}"
    );
    let status = ok(&mut rpc_ws, 2, "host.status", json!({})).await;
    assert_eq!(status["daemonMode"], "orchestrator", "{status}");

    // No "This computer" row, and it cannot be added.
    let order = ok(&mut rpc_ws, 3, "remoteMachines.order", json!({})).await;
    assert_eq!(order, json!({ "order": [] }));
    let stale = rpc(
        &mut rpc_ws,
        4,
        "remoteMachines.setOrder",
        json!({ "order": [{ "kind": "local" }] }),
    )
    .await;
    assert_eq!(stale["error"]["data"]["code"], "order-stale", "{stale}");

    // Nowhere to run an agent: refused before anything is written.
    let before = ok(
        &mut rpc_ws,
        5,
        "agent.list",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    let create = |placement: Option<Value>| {
        let mut params = json!({ "workspaceId": ws_id, "name": "Agent", "model": "default", "provider": "mock" });
        if let Some(placement) = placement {
            params["placement"] = placement;
        }
        params
    };
    let unplaced = rpc(&mut rpc_ws, 6, "agent.create", create(None)).await;
    assert_eq!(unplaced["error"]["code"], -32602, "{unplaced}");
    assert_eq!(unplaced["error"]["data"]["code"], "placement-unavailable");
    assert_eq!(unplaced["error"]["data"]["reason"], "no-agent-machine");
    assert!(unplaced["error"]["message"]
        .as_str()
        .is_some_and(|m| m.contains("Settings → Machines")));
    let local = rpc(
        &mut rpc_ws,
        7,
        "agent.create",
        create(Some(json!({ "target": "local" }))),
    )
    .await;
    assert_eq!(local["error"]["code"], -32003, "{local}");
    assert_eq!(
        local["error"]["data"]["code"], "orchestrator-mode",
        "{local}"
    );
    let after = ok(
        &mut rpc_ws,
        8,
        "agent.list",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(
        agent_count(&after),
        agent_count(&before),
        "nothing was created"
    );

    // The local agent from before the switch is refused, and stays listed.
    let prompt = rpc(
        &mut rpc_ws,
        91,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": local_id, "content": "hello" }),
    )
    .await;
    assert_eq!(prompt["error"]["code"], -32003, "{prompt}");
    assert_eq!(
        prompt["error"]["data"]["code"], "orchestrator-mode",
        "{prompt}"
    );
    let listed = ok(
        &mut rpc_ws,
        92,
        "agent.list",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert!(listed["agents"]
        .as_array()
        .unwrap()
        .iter()
        .any(|agent| agent["id"] == local_id.as_str()));

    // Every other way to run something here is refused too.
    let refused = |response: &Value| response["error"]["data"]["code"] == "orchestrator-mode";
    let isolated = rpc(
        &mut rpc_ws,
        93,
        "agent.delegate",
        json!({ "workspaceId": ws_id, "taskText": "Do it", "isolation": "direct" }),
    )
    .await;
    assert!(refused(&isolated), "{isolated}");
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
    let initial = rpc(
        &mut rpc_ws,
        94,
        "workspace.create",
        json!({
            "repositoryPath": repo.to_string_lossy(),
            "baseRef": branch,
            "initialAgent": { "model": "default", "provider": "mock", "placement": { "target": "local" } },
        }),
    )
    .await;
    assert!(refused(&initial), "{initial}");
    for (id, method) in [
        (95, "executor.invite.create"),
        (96, "agent.importableSessions.list"),
    ] {
        let response = rpc(&mut rpc_ws, id, method, json!({})).await;
        assert!(refused(&response), "{method}: {response}");
    }

    // Providers are only configured here.
    let test_prompt = rpc(
        &mut rpc_ws,
        10,
        "host.providerTestPrompt",
        json!({ "providerId": "codex" }),
    )
    .await;
    assert_eq!(test_prompt["error"]["code"], -32003, "{test_prompt}");
    assert_eq!(test_prompt["error"]["data"]["code"], "orchestrator-mode");
    let auth = ok(
        &mut rpc_ws,
        11,
        "host.providerAuthStatus",
        json!({ "force": true }),
    )
    .await;
    let rows = auth["providers"].as_array().unwrap();
    assert!(!rows.is_empty(), "{auth}");
    assert!(
        rows.iter().all(|row| row["authenticated"].is_null()),
        "{auth}"
    );
    let discovery = ok(&mut rpc_ws, 12, "host.providerDiscovery", json!({})).await;
    assert_eq!(discovery["providers"], json!([]), "{discovery}");
    let catalog = ok(&mut rpc_ws, 13, "providers.catalog", json!({})).await;
    assert!(catalog["providers"]
        .as_array()
        .unwrap()
        .iter()
        .all(|row| row["supportsTestPrompt"] == false));
    let models = ok(
        &mut rpc_ws,
        14,
        "models.list",
        json!({ "providerId": "codex" }),
    )
    .await;
    assert_eq!(models["models"], json!([]), "{models}");

    // An orchestrator is never exposed as a machine.
    let mint = rpc(
        &mut rpc_ws,
        15,
        "executor.mint",
        json!({ "backendId": "other-backend", "backendName": "Other" }),
    )
    .await;
    assert_eq!(mint["error"]["data"]["code"], "orchestrator-mode", "{mint}");

    // A quick action with no machine is unavailable, never local.
    let quick = ok(
        &mut rpc_ws,
        16,
        "agent.completeOnce",
        json!({ "prompt": "hi" }),
    )
    .await;
    assert_eq!(quick["available"], false, "{quick}");
    assert!(quick["reason"]
        .as_str()
        .is_some_and(|reason| reason.contains("Settings → Machines")));

    // Add a machine: unplaced agents run there, and quick actions reach it.
    let backend = ok(&mut rpc_ws, 20, "remoteMachines.backendInfo", json!({})).await;
    let backend_id = backend["backendId"].as_str().unwrap().to_owned();
    let dir = scratch.path().join("node-0");
    std::fs::create_dir_all(&dir).unwrap();
    let machine = start_machine(&dir, &backend_id).await;
    ok(
        &mut rpc_ws,
        21,
        "remoteMachines.add",
        add_params(&machine, "Machine A"),
    )
    .await;
    let deadline = tokio::time::Instant::now() + wait();
    let mut id = 100;
    let placed = loop {
        id += 1;
        let response = rpc(&mut rpc_ws, id, "agent.create", create(None)).await;
        if response.get("error").is_none() {
            break response["result"]["agent"].clone();
        }
        assert_eq!(response["error"]["data"]["retryable"], true, "{response}");
        assert!(
            tokio::time::Instant::now() < deadline,
            "the machine never took the agent: {response}"
        );
        // timing-guard: poll interval while the machine observes its providers (no event exposes it)
        tokio::time::sleep(Duration::from_millis(500)).await;
    };
    assert_eq!(placed["nodeId"], machine.daemon_id.as_str(), "{placed}");

    // With the machine added the call goes to the ordered list. On a host
    // without a Codex CLI no machine offers it and the answer is the
    // unavailable shape; with one, the machine runs it and its own failure
    // (no sign-in in this hermetic home) comes back once-prefixed, as the
    // local route's. Either way nothing ran on the orchestrator.
    let quick = rpc(
        &mut rpc_ws,
        30,
        "agent.completeOnce",
        json!({ "prompt": "hi" }),
    )
    .await;
    if let Some(error) = quick.get("error") {
        assert_eq!(error["code"], -32603, "{quick}");
        let data = error["data"].as_str().unwrap_or_default();
        assert!(data.starts_with("codex: "), "{quick}");
        assert_eq!(data.matches("codex: ").count(), 1, "{quick}");
    } else {
        assert_eq!(quick["result"]["available"], false, "{quick}");
        assert!(
            quick["result"]["reason"]
                .as_str()
                .is_some_and(|r| !r.is_empty()),
            "{quick}"
        );
    }
}
