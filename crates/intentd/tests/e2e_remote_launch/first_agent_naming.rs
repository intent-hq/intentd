//! The first agent of a new workspace on an orchestrator names itself and
//! the workspace, as on a full daemon: its first prompt carries the
//! fire-once naming instruction, `ws.workspace.details()` reports no title,
//! and `setTitle` / `setAgentName` from the home's machine land on the
//! orchestrator's rows.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_first_agent_on_an_orchestrator_names_itself_and_the_workspace() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping first-agent naming e2e: node or git not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-naming-");
    let repo = scratch.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    std::fs::write(repo.join("README.md"), "base\n").unwrap();
    git(&repo, &["add", "README.md"]);
    git(&repo, &["commit", "--quiet", "-m", "base"]);
    let branch = git(&repo, &["rev-parse", "--abbrev-ref", "HEAD"]);
    let head_dir = scratch.path().join("head");
    let (head, _, _, _) = start_backend(&head_dir, &repo).await;
    drop(head);
    super::orchestrator::orchestrator_config(&head_dir);
    let (_head, port, cfg) = serve_backend(&head_dir, &[]).await;
    let mut rpc_ws = connect(port, cfg).await;

    let backend = ok(&mut rpc_ws, 1, "remoteMachines.backendInfo", json!({})).await;
    let backend_id = backend["backendId"].as_str().unwrap().to_owned();
    let node_dir = scratch.path().join("node-0");
    std::fs::create_dir_all(&node_dir).unwrap();
    let machine = start_machine_with(&node_dir, &backend_id, &[]).await;
    ok(
        &mut rpc_ws,
        2,
        "remoteMachines.add",
        add_params(&machine, "Machine A"),
    )
    .await;

    // The app's two first-agent requests, both untitled with the
    // specialist's placeholder name (`nameExplicitlySet: false`): the
    // first-run wizard's, which makes an orchestrator's first workspace, and
    // the new workspace form's.
    let wizard = json!({
        "name": "Developer",
        "nameExplicitlySet": false,
        "model": "default",
        "provider": "mock",
        "prompt": "echo-raw",
        "agentType": "workspace",
        "specialist": "developer",
        "metadata": { "source": "onboarding", "isInitialAgent": true, "specialist": "developer" },
    });
    let form = json!({
        "name": "Developer",
        "nameExplicitlySet": false,
        "rememberSpecialist": true,
        "model": "default",
        "provider": "mock",
        "prompt": "echo-raw",
        "agentType": "workspace",
        "specialist": "developer",
    });
    let mut id = 10;
    let mut first = None;
    for (label, initial_agent) in [("wizard", wizard), ("form", form)] {
        let deadline = tokio::time::Instant::now() + wait();
        let created = loop {
            id += 1;
            let response = rpc(
                &mut rpc_ws,
                id,
                "workspace.create",
                json!({
                    "title": "",
                    "repositoryPath": repo.to_string_lossy(),
                    "baseRef": branch,
                    "initialAgent": initial_agent,
                }),
            )
            .await;
            if response.get("error").is_none() {
                break response["result"].clone();
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "the machine never became a home: {response}"
            );
            // timing-guard: retry while the new machine finishes its first bind
            tokio::time::sleep(Duration::from_millis(200)).await;
        };
        let ws_id = created["workspace"]["id"].as_str().unwrap().to_owned();
        assert_eq!(
            created["workspace"]["home"]["name"], "Machine A",
            "{label}: {created}"
        );
        let agent_id = created["initialAgent"]["id"].as_str().unwrap().to_owned();
        assert_eq!(
            created["initialAgent"]["name"], "Developer",
            "{label}: {created}"
        );

        id += 100;
        let raw = reply_containing(&mut rpc_ws, (&ws_id, &agent_id), "raw: ", id).await;
        assert!(
            raw.contains("ws.workspace.setAgentName"),
            "{label}: the first prompt asks the agent to name itself: {raw}"
        );
        assert!(
            raw.contains("This workspace needs a title"),
            "{label}: the first prompt asks for a workspace title: {raw}"
        );
        // The title tool was retired from the instructions in harness 3.10:
        // the instruction names the call the agent actually has.
        assert!(
            raw.contains("call `ws.workspace.setTitle` through"),
            "{label}: the title instruction names a call the agent has: {raw}"
        );
        assert!(!raw.contains("set_workspace_title"), "{label}: {raw}");
        first.get_or_insert((ws_id, agent_id));
    }
    let (ws_id, agent_id) = first.unwrap();

    ok(
        &mut rpc_ws,
        900,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": agent_id,
            "content": r#"wsapi const details = await ws.workspace.details(); const title = await ws.workspace.setTitle("Named on the home"); const name = await ws.workspace.setAgentName("Home namer"); return JSON.stringify({ marker: "turn-naming", details, title, name });"#,
        }),
    )
    .await;
    reply_containing(&mut rpc_ws, (&ws_id, &agent_id), "turn-naming", 910).await;
    let got = ok(
        &mut rpc_ws,
        390,
        "agent.get",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let value = wsapi_value(
        got["agent"]["lastAgentResponse"]
            .as_str()
            .unwrap_or_default(),
    );
    assert_eq!(value["details"]["hasTitle"], false, "{value}");
    assert_eq!(value["title"]["title"], "Named on the home", "{value}");
    assert!(value["title"].get("skipped").is_none(), "{value}");
    assert_eq!(value["name"]["success"], true, "{value}");
    assert!(value["name"].get("skipped").is_none(), "{value}");

    let workspace = ok(
        &mut rpc_ws,
        400,
        "workspace.get",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(
        workspace["workspace"]["title"], "Named on the home",
        "{workspace}"
    );
    let agent = ok(
        &mut rpc_ws,
        401,
        "agent.get",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    assert_eq!(agent["agent"]["name"], "Home namer", "{agent}");
}
