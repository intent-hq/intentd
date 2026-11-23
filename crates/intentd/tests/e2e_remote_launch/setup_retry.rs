//! A remote agent's failed setup runs again in its kept checkout (monorepo
//! `docs/protocol/methods/nodes.md` "Retrying a remote agent's setup",
//! `node-link.md` § Setup retry), over two real daemons: the user's
//! `agent.retrySetup` on the backend's WSS socket, and the agent's own
//! `ws.workspace.retrySetup()` in a turn. Refusals carry their documented
//! `data.code`.

use super::*;

/// The `data.code` of a refused call.
fn refused_code(response: &Value) -> &str {
    assert_eq!(response["error"]["code"], -32602, "{response}");
    response["error"]["data"]["code"]
        .as_str()
        .unwrap_or_default()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_remote_agents_failed_setup_runs_again_for_the_user_and_the_agent() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping setup retry e2e: node or git not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-retry-");
    let repo = scratch.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet", "--initial-branch", "main"]);
    std::fs::write(repo.join("README.md"), "r\n").unwrap();
    git(&repo, &["add", "README.md"]);
    git(&repo, &["commit", "--quiet", "-m", "init"]);
    // Read from the backend worktree at each launch and each retry.
    let set_script = |script: &str| {
        std::fs::create_dir_all(repo.join(".intent")).unwrap();
        std::fs::write(
            repo.join(".intent/config.json"),
            json!({ "setupScript": script }).to_string(),
        )
        .unwrap();
    };
    let needs_fix = "echo ran >> setup-runs.txt\n\
                     [ -f fixed ] || { echo not-fixed-yet; exit 3; }\n\
                     echo fixed-now\n";
    set_script(needs_fix);

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

    // The user's retry: the script fixed on the backend since the launch
    // runs in the agent's kept checkout, and its output starts over.
    let created = create_remote(&mut rpc_ws, &ws_id, "User retry", 10).await;
    let agent_id = created["agent"]["id"].as_str().unwrap().to_owned();
    turn(&mut rpc_ws, &mut events, (&ws_id, &agent_id), "hello", 50).await;
    let setup = setup_until(&mut rpc_ws, (&ws_id, &agent_id), 100, |setup| {
        setup["state"] == "failed"
    })
    .await;
    assert_eq!(setup["exitCode"], 3, "{setup}");
    let first = timeout(wait(), async {
        let mut id = 150;
        loop {
            id += 1;
            let session = ok(
                &mut rpc_ws,
                id,
                "agent.getSession",
                json!({ "workspaceId": ws_id, "agentId": agent_id }),
            )
            .await;
            if let Some(reply) = session["session"]["messages"]
                .as_array()
                .into_iter()
                .flatten()
                .find(|message| message["role"] == "assistant")
            {
                return reply.to_string();
            }
            // timing-guard: poll interval for the first reply to be recorded
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the first reply is recorded");
    assert!(first.contains("workspace setup failed"), "{first}");
    assert!(first.contains("ws.workspace.retrySetup()"), "{first}");

    set_script("echo ran >> setup-runs.txt\necho fixed-by-user\n");
    let started = rpc(
        &mut rpc_ws,
        200,
        "agent.retrySetup",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert_eq!(started["id"], 200, "{started}");
    assert!(started.get("error").is_none(), "{started}");
    let result = &started["result"];
    assert_eq!(
        result.as_object().unwrap().keys().collect::<Vec<_>>(),
        ["agentId", "setup"],
        "{result}"
    );
    assert_eq!(result["agentId"], agent_id.as_str(), "{result}");
    assert_eq!(result["setup"]["state"], "running", "{result}");
    assert!(result["setup"]["startedAt"].is_string(), "{result}");
    let setup = setup_until(&mut rpc_ws, (&ws_id, &agent_id), 300, |setup| {
        setup["state"] == "completed"
            && setup["output"]
                .as_str()
                .is_some_and(|out| out.contains("Setup script completed in"))
    })
    .await;
    assert_eq!(setup["exitCode"], 0, "{setup}");
    let output = setup["output"].as_str().unwrap();
    assert!(output.contains("fixed-by-user"), "{output}");
    assert!(
        !output.contains("not-fixed-yet"),
        "the output starts over: {output}"
    );
    let work = agent_home(&node_dir, &machine.credential_id, &agent_id).join("work");
    assert_eq!(
        std::fs::read_to_string(work.join("setup-runs.txt")).unwrap(),
        "ran\nran\n",
        "the same checkout"
    );
    // The agent keeps working after the retry.
    let reply = turn(&mut rpc_ws, &mut events, (&ws_id, &agent_id), "after", 400).await;
    assert!(reply.contains("echo:"), "{reply}");

    // Refusals, with their documented codes.
    let again = rpc(
        &mut rpc_ws,
        500,
        "agent.retrySetup",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert_eq!(refused_code(&again), "setup-retry-not-failed", "{again}");
    let local = ok(
        &mut rpc_ws,
        501,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Local", "model": "default", "provider": "mock" }),
    )
    .await;
    let local_id = local["agent"]["id"].as_str().unwrap().to_owned();
    let not_remote = rpc(
        &mut rpc_ws,
        502,
        "agent.retrySetup",
        json!({ "workspaceId": ws_id, "agentId": local_id }),
    )
    .await;
    assert_eq!(
        refused_code(&not_remote),
        "setup-retry-not-remote",
        "{not_remote}"
    );
    let missing = rpc(
        &mut rpc_ws,
        503,
        "agent.retrySetup",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(missing["error"]["code"], -32602, "{missing}");

    // The agent's own retry: it fixes the cause in its checkout, retries
    // in its turn and gets the result; its setup status follows.
    set_script(needs_fix);
    let created = create_remote(&mut rpc_ws, &ws_id, "Self retry", 600).await;
    let self_id = created["agent"]["id"].as_str().unwrap().to_owned();
    turn(&mut rpc_ws, &mut events, (&ws_id, &self_id), "hello", 650).await;
    setup_until(&mut rpc_ws, (&ws_id, &self_id), 700, |setup| {
        setup["state"] == "failed"
    })
    .await;
    let retried = wsapi_turn(
        &mut rpc_ws,
        &mut events,
        (&ws_id, &self_id),
        r#"wsapi await ws.file.write("fixed", ""); const r = await ws.workspace.retrySetup(); const d = await ws.workspace.details(); return JSON.stringify({ marker: "turn-retry", r, setup: d.setupStatus });"#,
        "turn-retry",
        800,
    )
    .await;
    let value = wsapi_value(&retried);
    assert_eq!(value["r"]["state"], "completed", "{value}");
    assert_eq!(value["r"]["exitCode"], 0, "{value}");
    assert!(value["r"]["startedAt"].is_string(), "{value}");
    assert!(value["r"]["finishedAt"].is_string(), "{value}");
    assert!(
        value["r"]["output"]
            .as_str()
            .is_some_and(|out| out.contains("fixed-now")),
        "{value}"
    );
    assert_eq!(value["setup"]["state"], "completed", "{value}");
    let setup = setup_until(&mut rpc_ws, (&ws_id, &self_id), 900, |setup| {
        setup["state"] == "completed"
    })
    .await;
    assert_eq!(setup["exitCode"], 0, "{setup}");
    // A second call has nothing to retry.
    let refused = wsapi_turn(
        &mut rpc_ws,
        &mut events,
        (&ws_id, &self_id),
        r#"wsapi let m = "none"; try { await ws.workspace.retrySetup(); } catch (e) { m = String(e); } return JSON.stringify({ marker: "turn-again", m });"#,
        "turn-again",
        1000,
    )
    .await;
    let value = wsapi_value(&refused);
    assert!(
        value["m"]
            .as_str()
            .is_some_and(|m| m.contains("setup-retry-not-failed")),
        "{value}"
    );
}
