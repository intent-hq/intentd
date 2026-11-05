//! Sub-agents of remote agents start from the parent's work
//! (nodeProtocol 13): on the parent's machine they share its checkout, on
//! another machine they start from its head and wip, pushed at creation.
//! Head runs with the bound-caller assertion armed, so every frame the
//! machines forward runs as its agent.

use super::*;

/// The id of the workspace's agent named `name`.
async fn agent_named(rpc_ws: &mut common::TlsWs, ws_id: &str, name: &str, id: i64) -> String {
    let deadline = tokio::time::Instant::now() + wait();
    let mut id = id;
    loop {
        id += 1;
        let listed = ok(rpc_ws, id, "agent.list", json!({ "workspaceId": ws_id })).await;
        let found = listed["agents"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|agent| agent["name"] == name)
            .and_then(|agent| agent["id"].as_str())
            .map(str::to_owned);
        if let Some(found) = found {
            return found;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no agent named {name}: {listed}"
        );
        // timing-guard: poll interval for the forwarded create to be listed
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Poll `git show <rev>` in `repo` until it prints `want`.
async fn shows(repo: &Path, rev: &str, want: &str) {
    let found = timeout(wait(), async {
        loop {
            let output = Command::new("git")
                .arg("-C")
                .arg(repo)
                .args(["show", rev])
                .output()
                .unwrap();
            if output.status.success() && String::from_utf8_lossy(&output.stdout).trim() == want {
                return;
            }
            // timing-guard: poll interval for the machine's turn-end push to land
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(found.is_ok(), "{rev} never showed {want:?}");
}

fn repo_with_commit(root: &Path) -> std::path::PathBuf {
    let repo = root.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    std::fs::write(repo.join("README.md"), "base\n").unwrap();
    git(&repo, &["add", "README.md"]);
    git(&repo, &["commit", "--quiet", "-m", "base"]);
    repo
}

/// A remote parent and its sub-agent on the same machine see each other's
/// uncommitted edits at once; the child's work reaches head in the
/// parent's refs, and the child has no refs of its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_remote_agents_sub_agent_on_its_machine_shares_its_checkout() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping remote sub-agent e2e: node or git not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-share-");
    let repo = repo_with_commit(scratch.path());
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
    let parent = create_remote(&mut rpc_ws, &ws_id, "Parent", 10).await;
    let parent_id = parent["agent"]["id"].as_str().unwrap().to_owned();

    let child_prompt = r#"wsapi const p = await ws.file.read("parent.txt"); await ws.file.write("child.txt", "from child"); const i = await ws.workspace.info(); return JSON.stringify({ marker: "child-1", p, path: i.path });"#;
    let code = format!(
        r#"wsapi await ws.file.write("parent.txt", "from parent"); const i = await ws.workspace.info(); const c = await ws.agent.create("Child", {child_prompt:?}, {{ provider: "mock", model: "default" }}); return JSON.stringify({{ marker: "parent-1", path: i.path, c }});"#
    );
    let reply = wsapi_turn(
        &mut rpc_ws,
        &mut events,
        (&ws_id, &parent_id),
        &code,
        "parent-1",
        40,
    )
    .await;
    let made = wsapi_value(&reply);
    let parent_path = made["path"].as_str().unwrap().to_owned();
    assert!(
        made["c"].to_string().contains("parent's checkout"),
        "the notice says where the child works: {made}"
    );
    let child_id = agent_named(&mut rpc_ws, &ws_id, "Child", 60).await;

    // The child's turn end comes first, so the parent's next turn waits
    // for its own.
    loop {
        let end = event(&mut events, "agent:stream:end").await;
        if end["data"]["agentId"] == child_id.as_str() {
            break;
        }
    }
    let seen = reply_containing(&mut rpc_ws, (&ws_id, &child_id), "child-1", 100).await;
    let seen: Value = serde_json::from_str(&seen).unwrap();
    let text = seen.to_string();
    assert!(text.contains("from parent"), "the child sees the parent's edit: {text}");
    assert!(
        text.contains(&parent_path),
        "the child works in the parent's checkout {parent_path}: {text}"
    );
    let child = ok(
        &mut rpc_ws,
        200,
        "agent.get",
        json!({ "workspaceId": ws_id, "agentId": child_id }),
    )
    .await;
    assert_eq!(child["agent"]["nodeId"], machine.daemon_id.as_str(), "{child}");

    // The child's edit is in the parent's checkout on the machine.
    assert_eq!(
        std::fs::read_to_string(Path::new(&parent_path).join("child.txt")).unwrap(),
        "from child",
        "the parent sees the child's edit"
    );

    let parent_refs = format!("refs/intent/ws/{ws_id}/agents/{parent_id}");
    shows(&repo, &format!("{parent_refs}/wip:child.txt"), "from child").await;
    let child_refs = format!("refs/intent/ws/{ws_id}/agents/{child_id}");
    assert_eq!(try_rev(&repo, &format!("{child_refs}/head")), None);
    assert_eq!(try_rev(&repo, &format!("{child_refs}/wip")), None);
}

/// A remote parent's sub-agent on another machine starts from the
/// parent's commit and uncommitted changes, pushed at its creation
/// (mid-turn, before the parent's own turn-end push), in its own checkout.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_remote_agents_sub_agent_on_another_machine_starts_from_its_work() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping remote sub-agent e2e: node or git not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-copy-");
    let repo = repo_with_commit(scratch.path());
    let (_head, port, cfg, ws_id) = start_backend(&scratch.path().join("head"), &repo).await;
    let mut rpc_ws = connect(port, cfg.clone()).await;
    let backend = ok(&mut rpc_ws, 1, "remoteMachines.backendInfo", json!({})).await;
    let backend_id = backend["backendId"].as_str().unwrap().to_owned();
    let mut machines = Vec::new();
    for (index, name) in ["Near", "Far"].into_iter().enumerate() {
        let dir = scratch.path().join(name);
        std::fs::create_dir_all(&dir).unwrap();
        let machine = start_machine(&dir, &backend_id).await;
        ok(
            &mut rpc_ws,
            2 + i64::try_from(index).unwrap(),
            "remoteMachines.add",
            add_params(&machine, name),
        )
        .await;
        machines.push(machine);
    }
    let (near, far) = (&machines[0], &machines[1]);
    let mut events = connect(port, cfg.clone()).await;
    ok(
        &mut events,
        5,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    let create = json!({
        "workspaceId": ws_id, "name": "Parent", "model": "default", "provider": "mock",
        "placement": { "target": "remote", "nodeId": near.daemon_id },
    });
    let deadline = tokio::time::Instant::now() + wait();
    let mut id = 10;
    let parent = loop {
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
    let parent_id = parent["agent"]["id"].as_str().unwrap().to_owned();

    let far_prompt = r#"wsapi const p = await ws.file.read("parent.txt"); const i = await ws.workspace.info(); return JSON.stringify({ marker: "far-1", p, path: i.path });"#;
    let code = format!(
        r#"wsapi await ws.file.write("parent.txt", "from parent"); const i = await ws.workspace.info(); const c = await ws.agent.create("Far", {far_prompt:?}, {{ provider: "mock", model: "default", placement: {{ target: "remote", nodeId: {far_id:?} }} }}); return JSON.stringify({{ marker: "parent-1", path: i.path, c }});"#,
        far_id = far.daemon_id,
    );
    let reply = wsapi_turn(
        &mut rpc_ws,
        &mut events,
        (&ws_id, &parent_id),
        &code,
        "parent-1",
        40,
    )
    .await;
    let made = wsapi_value(&reply);
    let parent_path = made["path"].as_str().unwrap().to_owned();
    assert!(
        made["c"].to_string().contains("parent's work"),
        "the notice says what the child starts from: {made}"
    );
    let child_id = agent_named(&mut rpc_ws, &ws_id, "Far", 60).await;

    let seen = reply_containing(&mut rpc_ws, (&ws_id, &child_id), "far-1", 100).await;
    let seen: Value = serde_json::from_str(&seen).unwrap();
    let text = seen.to_string();
    assert!(
        text.contains("from parent"),
        "the child starts with the parent's uncommitted edit: {text}"
    );
    assert!(
        !text.contains(&parent_path),
        "the child works in its own checkout: {text}"
    );
    let child = ok(
        &mut rpc_ws,
        200,
        "agent.get",
        json!({ "workspaceId": ws_id, "agentId": child_id }),
    )
    .await;
    assert_eq!(child["agent"]["nodeId"], far.daemon_id.as_str(), "{child}");

    let parent_refs = format!("refs/intent/ws/{ws_id}/agents/{parent_id}");
    let child_refs = format!("refs/intent/ws/{ws_id}/agents/{child_id}");
    assert_eq!(
        try_rev(&repo, &format!("{child_refs}/base")),
        try_rev(&repo, &format!("{parent_refs}/head")),
        "the child's base is the parent's head"
    );
    shows(&repo, &format!("{child_refs}/wip:parent.txt"), "from parent").await;
}
