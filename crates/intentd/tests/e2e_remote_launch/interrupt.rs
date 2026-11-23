//! Interrupting a remote agent mid-turn on an orchestrator: an
//! interrupt-priority `agent.sendMessage` cuts the running turn on the
//! runner and starts the new message at once, and `agent.stop` ends the
//! running turn, the same as for a local agent.

use super::orchestrator::orchestrator_config;
use super::*;

/// The agent's last reply, once it contains `want`.
async fn reply_becomes(
    rpc_ws: &mut common::TlsWs,
    (ws_id, agent_id): (&str, &str),
    id: &mut i64,
    want: &str,
) -> String {
    let mut last = String::new();
    let reached = timeout(Duration::from_secs(10), async {
        loop {
            *id += 1;
            let got = ok(
                rpc_ws,
                *id,
                "agent.get",
                json!({ "workspaceId": ws_id, "agentId": agent_id }),
            )
            .await;
            last = got["agent"]["lastAgentResponse"]
                .as_str()
                .unwrap_or_default()
                .to_owned();
            if last.contains(want) {
                return;
            }
            // timing-guard: poll interval for the reply row (stream events are asserted separately)
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(
        reached.is_ok(),
        "the reply never contained {want:?}: {last}"
    );
    last
}

/// Wait for a streamed chunk containing `text` (the turn is running).
async fn delta(events: &mut common::TlsWs, text: &str) {
    timeout(wait(), async {
        loop {
            let d = event(events, "chat:stream:delta").await;
            if d["data"]["content"]
                .as_str()
                .unwrap_or_default()
                .contains(text)
            {
                return;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("no streamed chunk containing {text:?}"));
}

/// Send `content` with no wait for the turn's end.
async fn send(
    rpc_ws: &mut common::TlsWs,
    (ws_id, agent_id): (&str, &str),
    id: i64,
    content: &str,
    priority: Option<&str>,
) -> Value {
    let mut params = json!({ "workspaceId": ws_id, "agentId": agent_id, "content": content });
    if let Some(priority) = priority {
        params["priority"] = json!(priority);
    }
    ok(rpc_ws, id, "agent.sendMessage", params).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn interrupt_and_stop_reach_a_remote_agent_mid_turn() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping remote interrupt e2e: node or git not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-rint-");
    let repo = scratch.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    std::fs::write(repo.join("README.md"), "base\n").unwrap();
    git(&repo, &["add", "README.md"]);
    git(&repo, &["commit", "--quiet", "-m", "base"]);
    let head_dir = scratch.path().join("head");
    let (head, _, _, ws_id) = start_backend(&head_dir, &repo).await;
    drop(head);
    orchestrator_config(&head_dir);
    let (_head, port, cfg) = serve_backend(&head_dir, &[]).await;
    let mut rpc_ws = connect(port, cfg.clone()).await;
    ok(
        &mut rpc_ws,
        1,
        "client.hello",
        json!({ "clientId": "remote-interrupt-e2e", "name": "Remote interrupt E2E" }),
    )
    .await;
    let backend = ok(&mut rpc_ws, 2, "remoteMachines.backendInfo", json!({})).await;
    let backend_id = backend["backendId"].as_str().unwrap().to_owned();
    let node_dir = scratch.path().join("node");
    std::fs::create_dir_all(&node_dir).unwrap();
    let machine = start_machine(&node_dir, &backend_id).await;
    ok(
        &mut rpc_ws,
        3,
        "remoteMachines.add",
        add_params(&machine, "Runner"),
    )
    .await;
    let mut events = connect(port, cfg.clone()).await;
    ok(
        &mut events,
        4,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*", "chat:*"], "workspaceId": ws_id }),
    )
    .await;

    let create = json!({
        "workspaceId": ws_id,
        "name": "Remote",
        "model": "default",
        "provider": "mock",
        "placement": { "target": "remote" },
    });
    let deadline = tokio::time::Instant::now() + wait();
    let mut id = 10;
    let created = loop {
        id += 1;
        let response = rpc(&mut rpc_ws, id, "agent.create", create.clone()).await;
        if response.get("error").is_none() {
            break response["result"].clone();
        }
        assert_eq!(
            response["error"]["data"]["code"], "placement-unavailable",
            "{response}"
        );
        assert!(
            tokio::time::Instant::now() < deadline,
            "the runner never took the placement: {response}"
        );
        // timing-guard: poll interval while the runner binds to the orchestrator (no event exposes it)
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let agent_id = created["agent"]["id"].as_str().unwrap().to_owned();
    let agent = (ws_id.as_str(), agent_id.as_str());

    // 1. An interrupt-priority message cuts a running turn and starts at once.
    send(&mut rpc_ws, agent, 100, "first block", None).await;
    let mut id = 200;
    delta(&mut events, "echo: first block").await;
    let sent = send(&mut rpc_ws, agent, 300, "second", Some("interrupt")).await;
    assert_eq!(sent["queued"], false, "the interrupt is not queued: {sent}");
    reply_becomes(&mut rpc_ws, agent, &mut id, "echo: second").await;
    let end = stream_end(&mut events, &agent_id).await;
    assert_eq!(end["data"]["stopReason"], "interrupted", "{end}");

    // 2. Stop ends a running turn.
    send(&mut rpc_ws, agent, 400, "third block", None).await;
    delta(&mut events, "echo: third block").await;
    ok(
        &mut rpc_ws,
        500,
        "agent.stop",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let stopped = timeout(Duration::from_secs(10), async {
        loop {
            let end = stream_end(&mut events, &agent_id).await;
            if end["data"]["stopReason"] == "interrupted" {
                return end;
            }
        }
    })
    .await
    .expect("agent.stop ends the remote turn");
    assert_eq!(stopped["data"]["agentId"], agent_id, "{stopped}");
    // The agent takes the next message after the stop.
    send(&mut rpc_ws, agent, 600, "fourth", None).await;
    reply_becomes(&mut rpc_ws, agent, &mut id, "echo: fourth").await;
}
