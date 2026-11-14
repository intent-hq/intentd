//! File and git events of a home (monorepo `docs/protocol/node-link.md`,
//! "File and git events of a home", nodeProtocol 19) against a real
//! orchestrator and a real machine: a file the app writes on the home
//! reaches the orchestrator's clients as `file:*`, a commit through
//! `git.agentCommit` as `git:commit`, and a commit made in the home checkout
//! itself (a terminal's) as `changes:git-status`. No event shows the
//! machine's paths.

use super::homes::{home_checkout, until, HOMES};
use super::*;

/// The next `events.event` of the workspace that `accept` takes; every
/// event read on the way is kept in `seen`.
async fn event_where(
    ws: &mut common::TlsWs,
    seen: &mut Vec<Value>,
    what: &str,
    accept: impl Fn(&Value) -> bool,
) -> Value {
    timeout(wait(), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    if v["method"] != "events.event" {
                        continue;
                    }
                    let event = v["params"]["event"].clone();
                    seen.push(event.clone());
                    if accept(&event) {
                        return event;
                    }
                }
                Some(Ok(Message::Ping(p))) => {
                    let _ = ws.send(Message::Pong(p)).await;
                }
                Some(Ok(_)) => {}
                other => panic!("event socket closed: {other:?}"),
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("no {what}; saw {seen:#?}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn file_and_git_events_of_a_home_reach_the_orchestrators_clients() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping home events e2e: node or git not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-home-events-");
    let repo = scratch.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    std::fs::write(repo.join("README.md"), "base\n").unwrap();
    git(&repo, &["add", "README.md"]);
    git(&repo, &["commit", "--quiet", "-m", "base"]);
    let branch = git(&repo, &["rev-parse", "--abbrev-ref", "HEAD"]);
    git(&repo, &["config", "user.name", "Backend User"]);
    git(&repo, &["config", "user.email", "backend@example.invalid"]);

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

    let mut events = connect(port, cfg.clone()).await;
    id += 1;
    ok(
        &mut events,
        id,
        "events.subscribe",
        json!({
            "eventTypes": ["file:*", "git:*", "changes:git-status"],
            "workspaceId": ws_id,
        }),
    )
    .await;
    let mut seen = Vec::new();

    // A file the app writes on the home is announced to the orchestrator's
    // clients. The machine's watch of a new checkout may still be arming:
    // write again until one is announced.
    let mut written = 0;
    let announced = timeout(wait(), async {
        loop {
            written += 1;
            assert!(
                written <= 10,
                "no file event for a write on the home; saw {seen:#?}"
            );
            let name = format!("app-{written}.txt");
            id += 1;
            ok(
                &mut rpc_ws,
                id,
                "file.write",
                json!({ "workspaceId": ws_id, "path": name, "content": "from the app\n" }),
            )
            .await;
            let got = timeout(
                common::test_timeout(Duration::from_secs(5)),
                event_where(&mut events, &mut seen, "a file event", |event| {
                    event["type"]
                        .as_str()
                        .is_some_and(|kind| kind.starts_with("file:"))
                        && event["data"]["path"]
                            .as_str()
                            .is_some_and(|path| path.starts_with("app-"))
                }),
            )
            .await;
            if let Ok(event) = got {
                return event;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("no file event for a write on the home; saw {seen:#?}"));
    assert_eq!(announced["workspaceId"], ws_id, "{announced}");
    let path = announced["data"]["path"].as_str().unwrap_or_default();
    assert!(
        path.starts_with("app-") && path.ends_with("txt"),
        "a checkout-relative path: {announced}"
    );

    // A commit on the home through the app is announced as `git:commit`.
    std::fs::write(work.join("dirty.txt"), "left uncommitted\n").unwrap();
    id += 1;
    let committed = ok(
        &mut rpc_ws,
        id,
        "git.agentCommit",
        json!({
            "workspaceId": ws_id,
            "message": "app files",
            "files": ["app-1.txt"],
            "userRequested": true,
        }),
    )
    .await;
    let hash = committed["hash"].as_str().unwrap().to_owned();
    let commit = event_where(&mut events, &mut seen, "git:commit", |event| {
        event["type"] == "git:commit"
    })
    .await;
    assert_eq!(commit["workspaceId"], ws_id, "{commit}");
    assert_eq!(commit["data"]["workspaceId"], ws_id, "{commit}");
    assert_eq!(commit["data"]["commit"], hash, "{commit}");

    // A commit made in the home checkout itself (as from a home terminal)
    // refreshes the git status the clients see.
    git(&work, &["add", "-A"]);
    git(
        &work,
        &[
            "-c",
            "user.name=Terminal",
            "-c",
            "user.email=terminal@example.invalid",
            "commit",
            "--quiet",
            "-m",
            "from the terminal",
        ],
    );
    let status = event_where(
        &mut events,
        &mut seen,
        "a clean changes:git-status",
        |event| {
            event["type"] == "changes:git-status"
                && event["data"]["status"]["uncommittedCount"] == 0
        },
    )
    .await;
    assert_eq!(status["data"]["workspaceId"], ws_id, "{status}");

    // No event shows the machine's paths or its hidden record.
    let node_path = node_dir.to_string_lossy().into_owned();
    let canonical = std::fs::canonicalize(&node_dir)
        .unwrap()
        .to_string_lossy()
        .into_owned();
    for event in &seen {
        let text = event.to_string();
        assert!(
            !text.contains(&node_path) && !text.contains(&canonical),
            "the machine's path leaked: {event}"
        );
        assert!(
            !text.contains("__home__"),
            "the hidden record leaked: {event}"
        );
    }
}
