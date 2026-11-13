//! Workspace homes on an orchestrator (monorepo
//! `docs/protocol/methods/nodes.md`, "Workspace homes"; `node-link.md`,
//! "Workspace homes (nodeProtocol 19)") against a real orchestrator and a
//! real machine, both with homes enabled: `workspace.create` chooses the
//! machine as the workspace's home and the row carries `home`; the machine
//! makes the home checkout from `home/head`; a commit there reaches the
//! hub's `home/head`, moves `lastBackupAt` and the orchestrator's mirror
//! worktree; a rewrite keeps the old tip pinned at `home/rewritten/<sha>`,
//! and a fast-forward pins nothing; an agent placed on the home's machine
//! works in the home checkout; a terminal opened from the app runs in the
//! home checkout; after an orchestrator restart the
//! home's machine is dialed again and backups keep arriving.

use super::*;
use base64::Engine as _;

const HOMES: (&str, &str) = ("INTENTD_UNSTABLE_HOMES", "1");

/// The home checkout `home_id` on the machine whose data dir is `node_dir`.
fn home_checkout(node_dir: &Path, home_id: &str) -> Option<PathBuf> {
    std::fs::read_dir(node_dir.join("w"))
        .ok()?
        .filter_map(Result::ok)
        .map(|entry| entry.path().join("homes").join(home_id).join("work"))
        .find(|work| work.join(".git").exists())
}

/// Poll `check` until it gives a value; `what` names the wait.
async fn until<T>(what: &str, mut check: impl FnMut() -> Option<T>) -> T {
    let deadline = tokio::time::Instant::now() + wait();
    loop {
        if let Some(found) = check() {
            return found;
        }
        assert!(tokio::time::Instant::now() < deadline, "timed out: {what}");
        // timing-guard: poll interval while the machine's watcher pushes (no event exposes the ref)
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

fn commit_on(work: &Path, file: &str, text: &str, message: &str) -> String {
    std::fs::write(work.join(file), text).unwrap();
    git(work, &["add", file]);
    git(
        work,
        &[
            "-c",
            "user.name=Home",
            "-c",
            "user.email=home@example.invalid",
            "commit",
            "--quiet",
            "-m",
            message,
        ],
    );
    git(work, &["rev-parse", "HEAD"])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_new_workspace_gets_a_home_that_is_backed_up_and_mirrored() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping homes e2e: node or git not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-homes-");
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
    let (head, port, cfg) = serve_backend(&head_dir, &[HOMES]).await;
    let mut rpc_ws = connect(port, cfg).await;

    let backend = ok(&mut rpc_ws, 1, "remoteMachines.backendInfo", json!({})).await;
    let backend_id = backend["backendId"].as_str().unwrap().to_owned();
    let node_dir = scratch.path().join("node-0");
    std::fs::create_dir_all(&node_dir).unwrap();
    let machine = start_machine_with(&node_dir, &backend_id, &[HOMES]).await;
    ok(
        &mut rpc_ws,
        2,
        "remoteMachines.add",
        add_params(&machine, "Machine A"),
    )
    .await;

    // The new workspace's home is the machine.
    let deadline = tokio::time::Instant::now() + wait();
    let mut id = 10;
    let created = loop {
        id += 1;
        let response = rpc(
            &mut rpc_ws,
            id,
            "workspace.create",
            json!({ "repositoryPath": repo.to_string_lossy(), "baseRef": branch }),
        )
        .await;
        if response.get("error").is_none() {
            break response["result"]["workspace"].clone();
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the machine never became a home: {response}"
        );
        // timing-guard: retry while the new machine finishes its first bind
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let ws_id = created["id"].as_str().unwrap().to_owned();
    let home = &created["home"];
    assert_eq!(home["name"], "Machine A", "{created}");
    assert_eq!(home["nodeId"], machine.daemon_id.as_str(), "{created}");
    assert_eq!(home["state"], "attached", "{created}");
    let home_id = home["homeId"].as_str().unwrap().to_owned();
    let mirror = PathBuf::from(created["worktreePath"].as_str().unwrap());
    let base = git(&mirror, &["rev-parse", "HEAD"]);
    let home_ref = |name: &str| format!("refs/intent/ws/{ws_id}/home/{name}");
    // The home opens in the background: home/head is seeded first.
    until("home/head at the base", || {
        (try_rev(&mirror, &home_ref("head")).as_deref() == Some(base.as_str())).then_some(())
    })
    .await;

    // The machine made the home checkout at the workspace's base.
    // `.git` appears before `git worktree add` has checked anything out.
    let work = until("the home checkout at the base", || {
        home_checkout(&node_dir, &home_id)
            .filter(|work| try_rev(work, "HEAD").as_deref() == Some(base.as_str()))
    })
    .await;
    assert!(work.join("README.md").is_file());

    // A terminal opened from the app runs in the home checkout on the
    // machine: typing reaches it, its output is served by the orchestrator,
    // it is listed, and a kill ends it there.
    let b64 = base64::engine::general_purpose::STANDARD;
    id += 1;
    let terminal = ok(
        &mut rpc_ws,
        id,
        "terminal.create",
        json!({ "workspaceId": ws_id, "cols": 80, "rows": 24, "command": "/bin/sh" }),
    )
    .await;
    let terminal_id = terminal["terminalId"].as_str().unwrap().to_owned();
    id += 1;
    ok(
        &mut rpc_ws,
        id,
        "terminal.resize",
        json!({ "terminalId": terminal_id, "cols": 132, "rows": 50 }),
    )
    .await;
    id += 1;
    ok(
        &mut rpc_ws,
        id,
        "terminal.write",
        json!({ "terminalId": terminal_id, "data": b64.encode("echo $$ > .term-pid; echo at:$(pwd -P):$(stty size)\n") }),
    )
    .await;
    let expected = format!(
        "at:{}:50 132",
        std::fs::canonicalize(&work).unwrap().display()
    );
    let deadline = tokio::time::Instant::now() + wait();
    loop {
        id += 1;
        let buffer = ok(
            &mut rpc_ws,
            id,
            "terminal.getBuffer",
            json!({ "terminalId": terminal_id }),
        )
        .await;
        let text = String::from_utf8_lossy(&b64.decode(buffer["data"].as_str().unwrap()).unwrap())
            .into_owned();
        if text.contains(&expected) {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the home terminal never answered {expected:?}: {text:?}"
        );
        // timing-guard: poll while the machine streams the terminal's output
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    id += 1;
    let listed = ok(
        &mut rpc_ws,
        id,
        "terminal.list",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert!(
        listed["terminals"]
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row["id"] == terminal_id.as_str()),
        "{listed}"
    );
    id += 1;
    ok(
        &mut rpc_ws,
        id,
        "terminal.kill",
        json!({ "terminalId": terminal_id }),
    )
    .await;
    // The kill reached the machine: the shell there is gone.
    let pid = std::fs::read_to_string(work.join(".term-pid"))
        .unwrap()
        .trim()
        .to_owned();
    until("the home shell ended", || {
        (!std::process::Command::new("kill")
            .args(["-0", &pid])
            .status()
            .unwrap()
            .success())
        .then_some(())
    })
    .await;
    std::fs::remove_file(work.join(".term-pid")).unwrap();

    // A commit there reaches home/head, the mirror and lastBackupAt.
    let first = commit_on(&work, "notes.txt", "one\n", "first");
    until("home/head at the first commit", || {
        (try_rev(&mirror, &home_ref("head")).as_deref() == Some(first.as_str())).then_some(())
    })
    .await;
    until("the mirror at the first commit", || {
        (git(&mirror, &["rev-parse", "HEAD"]) == first).then_some(())
    })
    .await;
    assert_eq!(
        std::fs::read_to_string(mirror.join("notes.txt")).unwrap(),
        "one\n"
    );
    let deadline = tokio::time::Instant::now() + wait();
    loop {
        id += 1;
        let got = ok(
            &mut rpc_ws,
            id,
            "workspace.get",
            json!({ "workspaceId": ws_id }),
        )
        .await;
        if got["workspace"]["home"]["lastBackupAt"].is_string()
            || got["home"]["lastBackupAt"].is_string()
        {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no lastBackupAt: {got}"
        );
        // timing-guard: poll while the backup record is ingested
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // A fast-forward pins nothing; a rewrite pins the old tip.
    let second = commit_on(&work, "notes.txt", "two\n", "second");
    until("home/head at the second commit", || {
        (try_rev(&mirror, &home_ref("head")).as_deref() == Some(second.as_str())).then_some(())
    })
    .await;
    // The pin is settled before the mirror moves: wait for the mirror.
    until("the mirror at the second commit", || {
        (git(&mirror, &["rev-parse", "HEAD"]) == second).then_some(())
    })
    .await;
    assert_eq!(
        try_rev(&mirror, &home_ref(&format!("rewritten/{first}"))),
        None
    );
    git(
        &work,
        &[
            "-c",
            "user.name=Home",
            "-c",
            "user.email=home@example.invalid",
            "commit",
            "--quiet",
            "--amend",
            "-m",
            "second, amended",
        ],
    );
    let amended = git(&work, &["rev-parse", "HEAD"]);
    until("home/head at the amended commit", || {
        (try_rev(&mirror, &home_ref("head")).as_deref() == Some(amended.as_str())).then_some(())
    })
    .await;
    assert_eq!(
        try_rev(&mirror, &home_ref(&format!("rewritten/{second}"))).as_deref(),
        Some(second.as_str()),
        "the rewritten tip is pinned"
    );
    until("the mirror at the amended commit", || {
        (git(&mirror, &["rev-parse", "HEAD"]) == amended).then_some(())
    })
    .await;

    // The list row carries the home too.
    id += 1;
    let list = ok(&mut rpc_ws, id, "workspace.list", json!({})).await;
    let row = list["workspaces"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == ws_id.as_str())
        .unwrap_or_else(|| panic!("{list}"))
        .clone();
    assert_eq!(row["home"]["homeId"], home_id.as_str(), "{row}");

    // An agent placed on the home's machine with no checkout of its own
    // works in the home checkout: the fixture's start file lands there.
    id += 1;
    let agent = ok(
        &mut rpc_ws,
        id,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Sharer", "model": "default", "provider": "mock" }),
    )
    .await;
    let agent_id = agent["agent"]["id"].as_str().unwrap().to_owned();
    // A remote agent starts on its machine with its first message.
    id += 1;
    ok(
        &mut rpc_ws,
        id,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "hello" }),
    )
    .await;
    until("the sharing agent started in the home checkout", || {
        work.join("agent.pid").is_file().then_some(())
    })
    .await;

    // After an orchestrator restart, the home's machine is dialed again on
    // its own: a commit there still reaches home/head.
    drop(rpc_ws);
    drop(head);
    let (_head, _, _) = serve_backend(&head_dir, &[HOMES]).await;
    let third = commit_on(&work, "notes.txt", "three\n", "third");
    until("home/head at the third commit after a restart", || {
        (try_rev(&mirror, &home_ref("head")).as_deref() == Some(third.as_str())).then_some(())
    })
    .await;
}
