//! A remote agent's commits reach head mid-turn: `ws.git.commit` pushes
//! the checkout before it answers (`pushed: true`), and
//! `ws.agent.reportToParent` pushes before it delivers, so a commit hash in
//! a report is fetchable on head while the agent's turn is still running.
//! Each agent blocks in `ws.host.exec` until the test has checked head.

use super::*;

/// Writes `$1` to the file `$2`, then waits for the file `$3`.
const GATE: &str =
    r#"printf %s "$1" > "$2.tmp" && mv "$2.tmp" "$2"; while [ ! -f "$3" ]; do sleep 0.1; done"#;

/// The text of `path` once it exists.
async fn written(path: &Path) -> String {
    timeout(wait(), async {
        loop {
            if let Ok(text) = std::fs::read_to_string(path) {
                return text;
            }
            // timing-guard: poll interval for the agent's gate file (the agent is blocked mid-turn)
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{} was never written", path.display()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_remote_agents_commits_reach_head_before_its_turn_ends() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping push-on-commit e2e: node or git not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-pushc-");
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
    let parent = create_remote(&mut rpc_ws, &ws_id, "Parent", 10).await;
    let parent_id = parent["agent"]["id"].as_str().unwrap().to_owned();
    let parent_head = format!("refs/intent/ws/{ws_id}/agents/{parent_id}/head");
    let gates = scratch.path().join("gates");
    std::fs::create_dir_all(&gates).unwrap();

    // `ws.git.commit` pushes before it answers: head has the commit while
    // the turn is still running.
    let (hash_1, go_1) = (gates.join("hash-1"), gates.join("go-1"));
    let code = format!(
        r#"wsapi await ws.file.write("a.txt", "a"); const c = await ws.git.commit("first", {{ userRequested: true }}); await ws.host.exec({{ command: "sh", args: ["-c", {GATE:?}, "sh", c.hash, {hash:?}, {go:?}] }}); return JSON.stringify({{ marker: "commit-1", c }});"#,
        hash = hash_1.display().to_string(),
        go = go_1.display().to_string(),
    );
    ok(
        &mut rpc_ws,
        40,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": parent_id, "content": code }),
    )
    .await;
    let hash = written(&hash_1).await;
    assert_eq!(
        try_rev(&repo, &parent_head).as_deref(),
        Some(hash.as_str()),
        "the commit is on head before the turn ends"
    );
    std::fs::write(&go_1, "").unwrap();
    let reply = reply_containing(&mut rpc_ws, (&ws_id, &parent_id), "commit-1", 50).await;
    assert!(
        reply.replace('\\', "").contains(r#""pushed":true"#),
        "{reply}"
    );

    // A plain `git commit` reaches head when the agent reports it: the
    // child (in the parent's checkout) waits for the parent's turn to end,
    // commits with git, reports, and blocks.
    let start = gates.join("start");
    let (hash_2, go_2) = (gates.join("hash-2"), gates.join("go-2"));
    let commit_script = r#"while [ ! -f "$1" ]; do sleep 0.1; done; echo b > b.txt && git add -A && git -c user.name=t -c user.email=t@t.invalid commit -qm child && git rev-parse HEAD"#;
    let child_prompt = format!(
        r#"wsapi const x = await ws.host.exec({{ command: "sh", args: ["-c", {commit_script:?}, "sh", {start:?}] }}); const h = x.stdout.trim(); let r; try {{ r = await ws.agent.reportToParent("committed " + h); }} catch (e) {{ r = "ERR:" + String(e); }} await ws.host.exec({{ command: "sh", args: ["-c", {GATE:?}, "sh", typeof r === "string" ? r : h, {hash:?}, {go:?}] }}); return JSON.stringify({{ marker: "child-1", r }});"#,
        start = start.display().to_string(),
        hash = hash_2.display().to_string(),
        go = go_2.display().to_string(),
    );
    let code = format!(
        r#"wsapi const c = await ws.agent.create("Child", {child_prompt:?}, {{ provider: "mock", model: "default" }}); return JSON.stringify({{ marker: "parent-2", c }});"#
    );
    wsapi_turn(
        &mut rpc_ws,
        &mut events,
        (&ws_id, &parent_id),
        &code,
        "parent-2",
        100,
    )
    .await;
    std::fs::write(&start, "").unwrap();
    let hash = written(&hash_2).await;
    assert!(!hash.starts_with("ERR:"), "{hash}");
    assert!(
        Command::new("git")
            .arg("-C")
            .arg(&repo)
            .args(["cat-file", "-e", &format!("{hash}^{{commit}}")])
            .status()
            .unwrap()
            .success(),
        "the reported commit {hash} is on head before the child's turn ends"
    );
    assert_eq!(try_rev(&repo, &parent_head).as_deref(), Some(hash.as_str()));
    std::fs::write(&go_2, "").unwrap();
}
