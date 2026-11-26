//! Git roots on the home (monorepo `docs/protocol/node-link.md`, "Files
//! and git on the home", point 7) against a real orchestrator and a real
//! machine (the orchestrator serves homes): a call with the `gitRootId` of a
//! submodule reads, shows git status and commits in that submodule of the
//! home checkout; a root outside
//! the checkout, a root the home checkout lacks and an unknown id each fail
//! with a clear `-32602`.

use super::homes::{home_checkout, until};
use super::*;

fn seeded_root(workspace_id: &str, path: &Path) -> intent_core::WorkspaceGitRoot {
    let now = intent_core::now_iso();
    intent_core::WorkspaceGitRoot {
        id: intent_core::WorkspaceGitRootId::new(),
        workspace_id: intent_core::WorkspaceId::from(workspace_id),
        path: path.to_string_lossy().into_owned(),
        source: intent_core::WorkspaceGitRootSource::Agent,
        repo_owner: None,
        repo_name: None,
        registered_by_agent_ids: vec![],
        registered_commit_sha: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        pull_requests: None,
        created_at: now.clone(),
        updated_at: now,
    }
}

#[track_caller]
fn refused_with(response: &Value, code: &str) {
    assert_eq!(response["error"]["code"], -32602, "{response}");
    assert!(
        response["error"]["message"]
            .as_str()
            .is_some_and(|message| message.contains(code)),
        "{response}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_submodule_root_of_a_workspace_with_a_home_runs_on_the_home() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping home roots e2e: node or git not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-home-roots-");
    let lib = scratch.path().join("upstream").join("lib");
    std::fs::create_dir_all(&lib).unwrap();
    git(&lib, &["init", "--quiet"]);
    std::fs::write(lib.join("lib.txt"), "lib\n").unwrap();
    git(&lib, &["add", "lib.txt"]);
    git(&lib, &["commit", "--quiet", "-m", "lib"]);
    let repo = scratch.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet", "--initial-branch=main"]);
    std::fs::write(repo.join("README.md"), "base\n").unwrap();
    git(&repo, &["add", "README.md"]);
    git(
        &repo,
        &[
            "-c",
            "protocol.file.allow=always",
            "submodule",
            "add",
            "--quiet",
            lib.to_str().unwrap(),
            "lib",
        ],
    );
    git(&repo, &["commit", "--quiet", "-m", "base with lib"]);
    // A repository that is no part of the workspace checkout.
    let other = scratch.path().join("other");
    std::fs::create_dir_all(&other).unwrap();
    git(&other, &["init", "--quiet"]);

    let head_dir = scratch.path().join("head");
    let (head, _, _, _) = start_backend(&head_dir, &repo).await;
    drop(head);
    super::orchestrator::orchestrator_config(&head_dir);
    // A create from a URL clones with the submodule populated (a create
    // from a local path is a plain worktree, which leaves it empty on any
    // daemon); `file://` submodules need this on modern git.
    let file_submodules = ("GIT_CONFIG_PARAMETERS", "'protocol.file.allow=always'");
    let (head, port, cfg) = serve_backend(&head_dir, &[file_submodules]).await;
    let mut rpc_ws = connect(port, cfg.clone()).await;
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

    let deadline = tokio::time::Instant::now() + wait();
    let mut id = 10;
    let created = loop {
        id += 1;
        let response = rpc(
            &mut rpc_ws,
            id,
            "workspace.create",
            json!({ "githubUrl": format!("file://{}", repo.display()), "baseRef": "main" }),
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
    // The backend's identity for the workspace; the machine has none of its
    // own when its global config is empty (as on CI).
    git(&mirror, &["config", "user.name", "Backend User"]);
    git(
        &mirror,
        &["config", "user.email", "backend@example.invalid"],
    );
    let work = until("the home checkout with its submodule", || {
        home_checkout(&node_dir, &home_id).filter(|work| work.join("lib").join("lib.txt").exists())
    })
    .await;

    // The workspace's roots, as an agent's `ws.git.registerRoot` or the
    // submodule sweep would record them on the orchestrator, written while
    // it is stopped: a second writer racing the live daemon fails at random
    // with "database is locked".
    drop(rpc_ws);
    drop(head);
    // live-store-write: allow — the orchestrator owning this store is stopped; only the machine daemon, on its own data dir, runs
    let store = intent_store::Store::open(&head_dir.join("intentd.db"))
        .await
        .unwrap();
    let lib_path = std::fs::canonicalize(mirror.join("lib")).unwrap_or_else(|_| mirror.join("lib"));
    let lib_root = store
        .upsert_workspace_git_root(&seeded_root(&ws_id, &lib_path))
        .await
        .unwrap()
        .0;
    let outside_root = store
        .upsert_workspace_git_root(&seeded_root(&ws_id, &other))
        .await
        .unwrap()
        .0;
    let missing_root = store
        .upsert_workspace_git_root(&seeded_root(&ws_id, &mirror.join("nested")))
        .await
        .unwrap()
        .0;
    store.close().await;
    let lib_id = lib_root.id.as_str().to_owned();
    let (_head, port, cfg) = serve_backend(&head_dir, &[file_submodules]).await;
    let mut rpc_ws = connect(port, cfg).await;

    // A file written in the submodule lands in the home's, not the mirror's,
    // once the machine binds to the restarted orchestrator.
    let deadline = tokio::time::Instant::now() + wait();
    loop {
        id += 1;
        let response = rpc(
            &mut rpc_ws,
            id,
            "file.write",
            json!({ "workspaceId": ws_id, "path": "lib/added.txt", "content": "in lib\n" }),
        )
        .await;
        if response.get("error").is_none() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the home never came back: {response}"
        );
        // timing-guard: retry while the machine binds to the restarted orchestrator
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(
        std::fs::read_to_string(work.join("lib").join("added.txt")).unwrap(),
        "in lib\n"
    );
    assert!(!mirror.join("lib").join("added.txt").exists());

    // A read in the root reads the home's submodule.
    id += 1;
    let read = ok(
        &mut rpc_ws,
        id,
        "file.read",
        json!({ "workspaceId": ws_id, "gitRootId": lib_id, "path": "added.txt" }),
    )
    .await;
    assert_eq!(read, "in lib\n", "{read}");

    // An absolute path names the file in the root, as locally.
    id += 1;
    let read = ok(
        &mut rpc_ws,
        id,
        "file.read",
        json!({
            "workspaceId": ws_id,
            "gitRootId": lib_id,
            "path": lib_path.join("added.txt").to_string_lossy(),
        }),
    )
    .await;
    assert_eq!(read, "in lib\n", "{read}");
    // A client cannot name a root path itself: without a gitRootId the call
    // reads the checkout root.
    id += 1;
    let read = ok(
        &mut rpc_ws,
        id,
        "file.read",
        json!({ "workspaceId": ws_id, "gitRootPath": "lib", "path": "README.md" }),
    )
    .await;
    assert_eq!(read, "base\n", "{read}");

    // git.status of the root is the home submodule's status.
    id += 1;
    let status = ok(
        &mut rpc_ws,
        id,
        "git.status",
        json!({ "workspaceId": ws_id, "gitRootId": lib_id, "forceRefresh": true }),
    )
    .await;
    let text = status.to_string();
    assert!(text.contains("added.txt"), "{status}");
    assert!(
        !text.contains(&*node_dir.to_string_lossy()),
        "the machine's path leaked: {status}"
    );
    // The root's status is not the checkout root's.
    id += 1;
    let primary = ok(
        &mut rpc_ws,
        id,
        "git.status",
        json!({ "workspaceId": ws_id, "forceRefresh": true }),
    )
    .await;
    assert!(!primary.to_string().contains("added.txt"), "{primary}");

    // A commit in the root commits in the home's submodule.
    let lib_work = work.join("lib");
    let lib_before = git(&lib_work, &["rev-parse", "HEAD"]);
    let primary_before = git(&work, &["rev-parse", "HEAD"]);
    id += 1;
    let committed = ok(
        &mut rpc_ws,
        id,
        "git.agentCommit",
        json!({
            "workspaceId": ws_id,
            "gitRootId": lib_id,
            "message": "lib file",
            "files": ["added.txt"],
            "userRequested": true,
        }),
    )
    .await;
    let hash = committed["hash"].as_str().unwrap().to_owned();
    assert_eq!(git(&lib_work, &["rev-parse", "HEAD"]), hash, "{committed}");
    assert_ne!(hash, lib_before);
    assert_eq!(
        git(&lib_work, &["log", "-1", "--format=%an <%ae>"]),
        "Backend User <backend@example.invalid>"
    );
    assert_eq!(git(&work, &["rev-parse", "HEAD"]), primary_before);
    id += 1;
    let commits = ok(
        &mut rpc_ws,
        id,
        "git.commits",
        json!({ "workspaceId": ws_id, "gitRootId": lib_id, "limit": 5 }),
    )
    .await;
    assert!(commits.to_string().contains("lib file"), "{commits}");

    // A root outside the checkout is not on the home.
    id += 1;
    let refused = rpc(
        &mut rpc_ws,
        id,
        "file.read",
        json!({ "workspaceId": ws_id, "gitRootId": outside_root.id.as_str(), "path": "x" }),
    )
    .await;
    refused_with(&refused, "home-root-outside-checkout");
    // A root inside the checkout that the home checkout lacks.
    id += 1;
    let refused = rpc(
        &mut rpc_ws,
        id,
        "git.status",
        json!({ "workspaceId": ws_id, "gitRootId": missing_root.id.as_str() }),
    )
    .await;
    refused_with(&refused, "home-root-missing");
    // An id the workspace never registered.
    id += 1;
    let refused = rpc(
        &mut rpc_ws,
        id,
        "git.status",
        json!({ "workspaceId": ws_id, "gitRootId": "root-unknown" }),
    )
    .await;
    refused_with(&refused, "Unknown git root");
    drop(machine);
}
