//! Files and git on the home (monorepo `docs/protocol/node-link.md`,
//! "Files and git on the home", nodeProtocol 19) against a real
//! orchestrator and a real machine, both with homes enabled: a file the app
//! writes lands in the home checkout (also when it names the orchestrator's
//! worktree path), `git.status` reads the home, `git.agentCommit` commits
//! there and reaches the hub's `home/head`, `git.push` from the hub carries
//! a commit the home made just before, attachments are placed on the home
//! (in pieces when large), and a lost home answers `home-unavailable`.

use super::homes::{commit_on, home_checkout, until, HOMES};
use super::*;
use base64::Engine as _;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn files_and_git_of_a_workspace_with_a_home_run_on_the_home() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping home calls e2e: node or git not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-home-calls-");
    let repo = scratch.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    std::fs::write(repo.join("README.md"), "base\n").unwrap();
    git(&repo, &["add", "README.md"]);
    git(&repo, &["commit", "--quiet", "-m", "base"]);
    let branch = git(&repo, &["rev-parse", "--abbrev-ref", "HEAD"]);
    // The forge the hub pushes to.
    let origin = scratch.path().join("origin.git");
    git(
        scratch.path(),
        &["init", "--quiet", "--bare", &origin.to_string_lossy()],
    );
    git(
        &repo,
        &["remote", "add", "origin", &origin.to_string_lossy()],
    );
    git(&repo, &["push", "--quiet", "origin", &branch]);

    let head_dir = scratch.path().join("head");
    let (head, _, _, _) = start_backend(&head_dir, &repo).await;
    drop(head);
    super::orchestrator::orchestrator_config(&head_dir);
    let (_head, port, cfg) = serve_backend(&head_dir, &[HOMES]).await;
    let mut rpc_ws = connect(port, cfg.clone()).await;
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
    let home_id = created["home"]["homeId"].as_str().unwrap().to_owned();
    let mirror = PathBuf::from(created["worktreePath"].as_str().unwrap());
    let base = git(&mirror, &["rev-parse", "HEAD"]);
    let work = until("the home checkout at the base", || {
        home_checkout(&node_dir, &home_id)
            .filter(|work| try_rev(work, "HEAD").as_deref() == Some(base.as_str()))
    })
    .await;

    // A file the app writes lands in the home checkout, not the mirror.
    id += 1;
    ok(
        &mut rpc_ws,
        id,
        "file.write",
        json!({ "workspaceId": ws_id, "path": "app.txt", "content": "from the app\n" }),
    )
    .await;
    assert_eq!(
        std::fs::read_to_string(work.join("app.txt")).unwrap(),
        "from the app\n"
    );
    assert!(!mirror.join("app.txt").exists());
    // A path under the orchestrator's worktree names the same file there.
    id += 1;
    ok(
        &mut rpc_ws,
        id,
        "file.write",
        json!({
            "workspaceId": ws_id,
            "path": mirror.join("dir").join("abs.txt").to_string_lossy(),
            "content": "absolute\n",
        }),
    )
    .await;
    assert_eq!(
        std::fs::read_to_string(work.join("dir").join("abs.txt")).unwrap(),
        "absolute\n"
    );
    id += 1;
    let read = ok(
        &mut rpc_ws,
        id,
        "file.read",
        json!({ "workspaceId": ws_id, "path": mirror.join("app.txt").to_string_lossy() }),
    )
    .await;
    assert_eq!(read, "from the app\n", "{read}");
    // Any other absolute path is refused.
    id += 1;
    let refused = rpc(
        &mut rpc_ws,
        id,
        "file.read",
        json!({ "workspaceId": ws_id, "path": "/etc/hostname" }),
    )
    .await;
    assert_eq!(refused["error"]["code"], -32602, "{refused}");
    // Results carry checkout paths, never the machine's.
    id += 1;
    let listed = ok(
        &mut rpc_ws,
        id,
        "file.list",
        json!({ "workspaceId": ws_id, "path": "." }),
    )
    .await;
    let text = listed.to_string();
    assert!(text.contains("app.txt"), "{listed}");
    assert!(
        !text.contains(&*node_dir.to_string_lossy()),
        "the machine's path leaked: {listed}"
    );

    // git.status reflects the home.
    id += 1;
    let status = ok(
        &mut rpc_ws,
        id,
        "git.status",
        json!({ "workspaceId": ws_id, "forceRefresh": true }),
    )
    .await;
    assert!(status.to_string().contains("app.txt"), "{status}");

    // A commit lands on the home and its head reaches the hub.
    id += 1;
    let committed = ok(
        &mut rpc_ws,
        id,
        "git.agentCommit",
        json!({
            "workspaceId": ws_id,
            "message": "app file",
            "files": ["app.txt"],
            "userRequested": true,
        }),
    )
    .await;
    let hash = committed["hash"].as_str().unwrap().to_owned();
    assert_eq!(git(&work, &["rev-parse", "HEAD"]), hash, "{committed}");
    let home_head = format!("refs/intent/ws/{ws_id}/home/head");
    until("home/head at the app commit", || {
        (try_rev(&mirror, &home_head).as_deref() == Some(hash.as_str())).then_some(())
    })
    .await;
    // The workspace's commit policy still applies: an agent-initiated
    // commit from the app with nothing to attribute is refused.
    id += 1;
    let refused = rpc(
        &mut rpc_ws,
        id,
        "git.agentCommit",
        json!({ "workspaceId": ws_id, "message": "sweep" }),
    )
    .await;
    assert!(refused.get("error").is_some(), "{refused}");

    // git.push from the hub carries what the home committed just before.
    let late = commit_on(&work, "late.txt", "late\n", "late");
    id += 1;
    let pushed = ok(&mut rpc_ws, id, "git.push", json!({ "workspaceId": ws_id })).await;
    assert_eq!(pushed["pushedSha"], late.as_str(), "{pushed}");
    let pushed_branch = pushed["branch"].as_str().unwrap().to_owned();
    assert_eq!(
        git(
            &origin,
            &["rev-parse", &format!("refs/heads/{pushed_branch}")]
        ),
        late
    );

    // Attachments are placed on the home: one piece, and several.
    let b64 = base64::engine::general_purpose::STANDARD;
    id += 1;
    let small = ok(
        &mut rpc_ws,
        id,
        "file.placeAttachment",
        json!({ "workspaceId": ws_id, "fileName": "note.txt", "data": b64.encode("hello") }),
    )
    .await;
    let small_path = small["path"].as_str().unwrap();
    assert_eq!(
        std::fs::read_to_string(work.join(small_path)).unwrap(),
        "hello",
        "{small}"
    );
    let large: Vec<u8> = (0..(4 * 1024 * 1024 + 4099))
        .map(|i: usize| u8::try_from(i % 253).unwrap())
        .collect();
    id += 1;
    let placed = ok(
        &mut rpc_ws,
        id,
        "file.placeAttachment",
        json!({ "workspaceId": ws_id, "fileName": "big.bin", "data": b64.encode(&large) }),
    )
    .await;
    assert_eq!(placed["size"], large.len(), "{placed}");
    let large_path = placed["path"].as_str().unwrap();
    assert_eq!(std::fs::read(work.join(large_path)).unwrap(), large);
    // The orchestrator keeps a copy under the same id, so its own lookups
    // (chat images, ws.file.getAttachment) find it.
    assert_eq!(std::fs::read(mirror.join(large_path)).unwrap(), large);
    // A retried keyed placement over one piece replays the first.
    let mut keyed = Value::Null;
    for _ in 0..2 {
        id += 1;
        keyed = ok(
            &mut rpc_ws,
            id,
            "file.placeAttachment",
            json!({
                "workspaceId": ws_id,
                "fileName": "keyed.bin",
                "data": b64.encode(&large),
                "idempotencyKey": "home-calls-e2e",
            }),
        )
        .await;
    }
    assert_eq!(keyed["replayed"], true, "{keyed}");
    assert_eq!(keyed["size"], large.len(), "{keyed}");
    // The home's registry answers for it.
    id += 1;
    let info = ok(
        &mut rpc_ws,
        id,
        "file.getAttachmentInfo",
        json!({ "workspaceId": ws_id, "attachmentId": placed["attachmentId"] }),
    )
    .await;
    assert_eq!(info["exists"], true, "{info}");

    // A registered git root is not known on the home: refused clearly.
    id += 1;
    let refused = rpc(
        &mut rpc_ws,
        id,
        "git.status",
        json!({ "workspaceId": ws_id, "gitRootId": "root-1" }),
    )
    .await;
    assert_eq!(refused["error"]["code"], -32602, "{refused}");
    assert!(
        refused["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("home-root-unsupported")),
        "{refused}"
    );

    // A code search with more matches than a reply carries streams from
    // the orchestrator, as a local one does.
    let mut events = connect(port, cfg.clone()).await;
    id += 1;
    ok(
        &mut events,
        id,
        "events.subscribe",
        json!({ "eventTypes": ["search:*"], "workspaceId": ws_id }),
    )
    .await;
    for n in 0..30 {
        id += 1;
        ok(
            &mut rpc_ws,
            id,
            "file.write",
            json!({
                "workspaceId": ws_id,
                "path": format!("needles/n{n}.txt"),
                "content": "zqxneedle\n",
            }),
        )
        .await;
    }
    id += 1;
    let searched = ok(
        &mut rpc_ws,
        id,
        "search.codebase",
        json!({ "workspaceId": ws_id, "query": "zqxneedle" }),
    )
    .await;
    assert_eq!(searched["matches"], json!([]), "{searched}");
    let done = event(&mut events, "search:done").await;
    assert_eq!(done["data"]["requestId"], searched["requestId"], "{done}");
    assert!(done["data"]["total"].as_u64().unwrap() >= 30, "{done}");

    // A home whose machine is gone answers home-unavailable.
    drop(machine);
    id += 1;
    let lost = rpc(
        &mut rpc_ws,
        id,
        "file.read",
        json!({ "workspaceId": ws_id, "path": "app.txt" }),
    )
    .await;
    assert_eq!(lost["error"]["data"]["code"], "home-unavailable", "{lost}");
    id += 1;
    let lost = rpc(
        &mut rpc_ws,
        id,
        "git.agentCommit",
        json!({
            "workspaceId": ws_id,
            "message": "lost",
            "files": ["app.txt"],
            "userRequested": true,
        }),
    )
    .await;
    assert_eq!(lost["error"]["data"]["code"], "home-unavailable", "{lost}");
    assert_eq!(lost["error"]["data"]["retryable"], false, "{lost}");
}
