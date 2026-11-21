//! Create progress on an orchestrator (PROTOCOL §5.1 / §6.5): a
//! `workspace.create` that carries a `progressId` streams its clone and
//! provisioning steps to the app as `git:clone:progress` frames, the same as
//! on a full daemon, while the orchestrator clones the repository and its
//! submodules before recording the home, and reporting the start of the
//! first agent on its machine as its own `agent` step.

use super::*;
use crate::common::submodule_repo::SubmoduleRepo;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_orchestrator_create_streams_its_clone_progress() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping orchestrator create progress e2e: node or git not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-orch-progress-");
    let source = SubmoduleRepo::create(scratch.path());
    let seed_repo = scratch.path().join("seed");
    std::fs::create_dir_all(&seed_repo).unwrap();
    git(&seed_repo, &["init", "--quiet"]);
    std::fs::write(seed_repo.join("README.md"), "seed\n").unwrap();
    git(&seed_repo, &["add", "README.md"]);
    git(&seed_repo, &["commit", "--quiet", "-m", "seed"]);
    let head_dir = scratch.path().join("head");
    let (head, _, _, _) = start_backend(&head_dir, &seed_repo).await;
    drop(head);
    super::orchestrator::orchestrator_config(&head_dir);
    let (_head, port, cfg) = serve_backend(&head_dir, &[SubmoduleRepo::DAEMON_ENV]).await;
    let mut rpc_ws = connect(port, cfg).await;

    let backend = ok(&mut rpc_ws, 1, "remoteMachines.backendInfo", json!({})).await;
    let backend_id = backend["backendId"].as_str().unwrap().to_owned();
    let node_dir = scratch.path().join("node-0");
    std::fs::create_dir_all(&node_dir).unwrap();
    let machine = start_machine_with(&node_dir, &backend_id, &[SubmoduleRepo::DAEMON_ENV]).await;
    ok(
        &mut rpc_ws,
        2,
        "remoteMachines.add",
        add_params(&machine, "Machine A"),
    )
    .await;
    ok(
        &mut rpc_ws,
        3,
        "events.subscribe",
        json!({ "eventTypes": ["git:clone:progress", "git:clone:done"] }),
    )
    .await;

    // Retry until the new machine can be a home; each attempt has its own
    // progress id so frames of a refused attempt never count.
    let deadline = tokio::time::Instant::now() + wait();
    let mut id = 10;
    let (progress_id, frames) = loop {
        id += 1;
        let progress_id = format!("prog-orch-{id}");
        let frame = json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": "workspace.create",
            "params": {
                "githubUrl": source.url(),
                "progressId": progress_id,
                "initialAgent": {
                    "name": "Developer",
                    "model": "default",
                    "provider": "mock",
                    "agentType": "workspace",
                },
            },
        });
        rpc_ws
            .send(Message::Text(frame.to_string().into()))
            .await
            .unwrap();
        let mut frames = Vec::new();
        let response = timeout(wait(), async {
            loop {
                match rpc_ws.next().await {
                    Some(Ok(Message::Text(text))) => {
                        let v: Value = serde_json::from_str(&text).unwrap();
                        if v["id"] == json!(id) {
                            return v;
                        }
                        if v["method"] == "events.event"
                            && v["params"]["event"]["data"]["progressId"] == progress_id.as_str()
                        {
                            frames.push(v["params"]["event"].clone());
                        }
                    }
                    Some(Ok(Message::Ping(p))) => {
                        let _ = rpc_ws.send(Message::Pong(p)).await;
                    }
                    Some(Ok(_)) => {}
                    other => panic!("socket closed during workspace.create: {other:?}"),
                }
            }
        })
        .await
        .expect("workspace.create timed out");
        if response.get("error").is_none() {
            assert!(
                response["result"]["workspace"]["home"].is_object(),
                "the orchestrator records a home: {response}"
            );
            break (progress_id, frames);
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the machine never became a home: {response}"
        );
        // timing-guard: retry while the new machine finishes its first bind
        tokio::time::sleep(Duration::from_millis(200)).await;
    };

    // The terminal done can trail the response by a frame.
    let mut frames = frames;
    if !frames.iter().any(|f| f["type"] == "git:clone:done") {
        loop {
            let done = event(&mut rpc_ws, "git:clone:done").await;
            if done["data"]["progressId"] == progress_id.as_str() {
                frames.push(done);
                break;
            }
        }
    }
    let phases: Vec<&str> = frames
        .iter()
        .filter(|f| f["type"] == "git:clone:progress")
        .filter_map(|f| f["data"]["phase"].as_str())
        .collect();
    assert!(
        phases.contains(&"submodules"),
        "the submodule clone is reported: {phases:?}"
    );
    assert!(
        phases
            .iter()
            .any(|p| matches!(*p, "receiving" | "resolving" | "counting" | "checkout")),
        "the clone's own steps are reported: {phases:?}"
    );
    // The first agent starts on its machine after the workspace is
    // finalized; that step is reported with the machine's name.
    let tail: Vec<(&str, &str)> = frames
        .iter()
        .filter(|f| f["type"] == "git:clone:progress")
        .map(|f| {
            (
                f["data"]["phase"].as_str().unwrap(),
                f["data"]["message"].as_str().unwrap(),
            )
        })
        .skip_while(|(phase, _)| *phase != "finalizing")
        .collect();
    assert_eq!(
        tail,
        [
            ("finalizing", "Finalizing workspace..."),
            ("agent", "Starting agent on Machine A..."),
            ("complete", "Workspace ready"),
        ],
        "{frames:?}"
    );
    let agent_frame = frames
        .iter()
        .find(|f| f["data"]["phase"] == "agent")
        .unwrap();
    assert_eq!(agent_frame["data"]["percent"], json!(97), "{agent_frame}");
    let percents: Vec<i64> = frames
        .iter()
        .filter(|f| f["type"] == "git:clone:progress")
        .map(|f| f["data"]["percent"].as_i64().unwrap())
        .collect();
    assert!(
        percents.windows(2).all(|w| w[0] <= w[1]),
        "percent never goes back: {percents:?}"
    );
    let dones: Vec<&Value> = frames
        .iter()
        .filter(|f| f["type"] == "git:clone:done")
        .collect();
    assert_eq!(dones.len(), 1, "exactly one done: {frames:?}");
    assert_eq!(dones[0]["data"]["ok"], json!(true), "{frames:?}");
}
