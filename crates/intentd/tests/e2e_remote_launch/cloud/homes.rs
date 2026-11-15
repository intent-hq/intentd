//! Cloud homes against the fake E2B (monorepo `docs/protocol/methods/nodes.md`,
//! "Choosing the home" and "Cloud homes"), on an orchestrator serving homes
//! with only a cloud entry on its machine list:
//!
//! 1. A workspace created with no agent gets the entry as its home and no
//!    sandbox. Its first file call starts a home-only sandbox: the home is
//!    `starting` meanwhile (a call that waits too long fails
//!    `home-unavailable` with `starting: true`), then `attached` on that
//!    sandbox, and the call succeeds.
//! 2. A workspace whose initial agent is placed on the entry: that agent's
//!    sandbox becomes the home; a terminal runs in the home checkout there
//!    and file calls work.
//! 3. Both sandboxes' `cloud.startedBy` is the workspace owner.
//! 4. A home whose sandbox fails to start is detached at once, and calls
//!    fail `home-unavailable` without `starting`.

use base64::Engine as _;

use super::super::homes::HOMES;
use super::*;

/// The workspace's home, from `workspace.get`.
async fn home_of(rpc_ws: &mut common::TlsWs, id: i64, ws_id: &str) -> Value {
    let got = ok(rpc_ws, id, "workspace.get", json!({ "workspaceId": ws_id })).await;
    let home = &got["workspace"]["home"];
    if home.is_object() {
        home.clone()
    } else {
        got["home"].clone()
    }
}

/// `workspace.get` until the home matches `check`.
async fn home_until(
    rpc_ws: &mut common::TlsWs,
    id: &mut i64,
    ws_id: &str,
    what: &str,
    check: impl Fn(&Value) -> bool,
) -> Value {
    let deadline = tokio::time::Instant::now() + wait();
    loop {
        *id += 1;
        let home = home_of(rpc_ws, *id, ws_id).await;
        if check(&home) {
            return home;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out: {what}: {home}"
        );
        // timing-guard: poll while the home's sandbox starts
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Call `method` until it succeeds; every failure meanwhile must be
/// `home-unavailable` with `starting: true`. Returns the result and how
/// many calls failed that way.
async fn call_while_starting(
    rpc_ws: &mut common::TlsWs,
    id: &mut i64,
    method: &str,
    params: &Value,
) -> (Value, usize) {
    let deadline = tokio::time::Instant::now() + wait();
    let mut starting = 0;
    loop {
        *id += 1;
        let reply = rpc(rpc_ws, *id, method, params.clone()).await;
        if reply.get("error").is_none() {
            return (reply["result"].clone(), starting);
        }
        let data = &reply["error"]["data"];
        assert_eq!(data["code"], "home-unavailable", "{reply}");
        assert_eq!(data["starting"], true, "{reply}");
        assert_eq!(data["retryable"], true, "{reply}");
        starting += 1;
        assert!(
            tokio::time::Instant::now() < deadline,
            "the home never started: {reply}"
        );
    }
}

/// The cloud machine row whose `nodeId` is `node_id`.
async fn machine_of(rpc_ws: &mut common::TlsWs, id: i64, node_id: &str) -> Value {
    let listed = ok(rpc_ws, id, "cloudMachines.list", json!({})).await;
    listed["machines"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|machine| machine["id"] == node_id)
        .cloned()
        .unwrap_or_else(|| panic!("no cloud machine {node_id}: {listed}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cloud_entry_is_a_home_that_starts_lazily() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping cloud homes e2e: node or git not on PATH");
        return;
    }
    let fixture = format!(
        "{}/tests/fixtures/node-test-agent.mjs",
        env!("CARGO_MANIFEST_DIR")
    );
    let scratch = common::test_tempdir_in("/tmp", "itd-e2b-homes-");
    let repo = scratch.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    std::fs::write(repo.join("README.md"), "base\n").unwrap();
    git(&repo, &["add", "README.md"]);
    git(&repo, &["commit", "--quiet", "-m", "base"]);
    let branch = git(&repo, &["rev-parse", "--abbrev-ref", "HEAD"]);
    let fake = FakeE2b::start(scratch, &fixture).await;
    fake.state()
        .sandbox_env
        .push((HOMES.0.to_owned(), HOMES.1.to_owned()));

    let head_dir = fake.scratch().join("head");
    let (head, _, _, _) = start_backend(&head_dir, &repo).await;
    drop(head);
    super::super::orchestrator::orchestrator_config(&head_dir);
    let manifest_base = fake.manifest_base();
    let (_head, port, cfg) = serve_backend(
        &head_dir,
        &[
            ("INTENTD_E2B_API_URL", &fake.url),
            ("INTENTD_E2B_ENVD_URL", &fake.url),
            ("INTENTD_E2B_DIRECT_HOST", "127.0.0.1"),
            ("INTENTD_E2B_DAEMON_WAIT_SECS", "30"),
            ("INTENTD_E2B_MANIFEST_BASE_URL", &manifest_base),
            HOMES,
        ],
    )
    .await;
    let mut rpc_ws = connect(port, cfg.clone()).await;
    ok(
        &mut rpc_ws,
        1,
        "client.hello",
        json!({ "clientId": "e2b-homes", "name": "E2B homes" }),
    )
    .await;
    let mut events = connect(port, cfg.clone()).await;
    ok(
        &mut events,
        2,
        "events.subscribe",
        json!({ "eventTypes": ["workspace:updated"] }),
    )
    .await;
    let keyed = ok(
        &mut rpc_ws,
        3,
        "providers.profiles.create",
        json!({ "baseProviderId": "codex", "name": "Cloud", "env": [
            { "key": "OPENAI_API_KEY", "kind": "set", "value": PROFILE_SECRET },
        ] }),
    )
    .await;
    let keyed_ref = format!("codex@{}", keyed["profile"]["id"].as_str().unwrap());
    let configured = ok(
        &mut rpc_ws,
        4,
        "cloudMachines.configure",
        configure_params(json!({
            "apiKey": E2B_KEY, "size": "4x8", "idlePauseMinutes": 1440, "pausedDeleteDays": 30,
        })),
    )
    .await;
    let entry = configured["provider"]["id"].as_str().unwrap().to_owned();
    let template = configured["provider"]["template"]
        .as_str()
        .unwrap()
        .to_owned();
    let mut id = 10;

    // 1. No agent: the entry is the home, and nothing starts yet.
    id += 1;
    let created = ok(
        &mut rpc_ws,
        id,
        "workspace.create",
        json!({ "repositoryPath": repo.to_string_lossy(), "baseRef": branch }),
    )
    .await;
    let lazy = created["workspace"]["id"].as_str().unwrap().to_owned();
    let owner = created["workspace"]["ownerPrincipalId"]
        .as_str()
        .unwrap()
        .to_owned();
    let home = &created["workspace"]["home"];
    assert_eq!(home["cloudMachineId"], entry.as_str(), "{created}");
    assert!(home.get("nodeId").is_none(), "{created}");
    assert_eq!(home["state"], "attached", "{created}");
    let lazy_home = home["homeId"].as_str().unwrap().to_owned();
    assert!(fake.state().creates.is_empty(), "no sandbox before a use");

    // The first file call starts a home-only sandbox and succeeds once it
    // runs; each call waits up to 10 s for it meanwhile.
    let write = json!({ "workspaceId": lazy, "path": "lazy.txt", "content": "lazy home\n" });
    let (_, waited) = call_while_starting(&mut rpc_ws, &mut id, "file.write", &write).await;
    eprintln!("calls that failed while the home started: {waited}");
    assert_eq!(fake.state().creates.len(), 1, "one home-only sandbox");
    assert_eq!(
        fake.state().metadata["sbx0"]["agentId"],
        format!("home-{lazy}"),
        "the sandbox is the home's, not an agent's"
    );
    id += 1;
    let read = ok(
        &mut rpc_ws,
        id,
        "file.read",
        json!({ "workspaceId": lazy, "path": "lazy.txt" }),
    )
    .await;
    assert_eq!(read, "lazy home\n", "{read}");
    let home = home_until(&mut rpc_ws, &mut id, &lazy, "the lazy home attached", |home| {
        home["state"] == "attached" && home["nodeId"].is_string()
    })
    .await;
    assert_eq!(home["homeId"], lazy_home.as_str(), "{home}");
    let lazy_node = home["nodeId"].as_str().unwrap().to_owned();
    id += 1;
    let machine = machine_of(&mut rpc_ws, id, &lazy_node).await;
    assert_eq!(machine["cloud"]["sandboxId"], "sbx0", "{machine}");
    assert_eq!(
        machine["cloud"]["startedBy"]["principalId"],
        owner.as_str(),
        "{machine}"
    );
    // It was announced as starting, then attached.
    let mut seen = Vec::new();
    drain_events(&mut events, &mut seen).await;
    let states: Vec<String> = seen
        .iter()
        .filter_map(|text| serde_json::from_str::<Value>(text).ok())
        .map(|frame| frame["params"]["event"].clone())
        .filter(|event| event["data"]["workspaceId"] == lazy.as_str())
        .filter_map(|event| {
            event["data"]["changes"]["home"]["state"]
                .as_str()
                .map(str::to_owned)
        })
        .collect();
    let starting = states.iter().position(|state| state == "starting");
    let attached = states.iter().rposition(|state| state == "attached");
    assert!(
        matches!((starting, attached), (Some(s), Some(a)) if s < a),
        "home states announced: {states:?}"
    );

    // 2. An initial agent on the entry: its sandbox becomes the home.
    id += 1;
    let created = ok(
        &mut rpc_ws,
        id,
        "workspace.create",
        json!({
            "repositoryPath": repo.to_string_lossy(),
            "baseRef": branch,
            "initialAgent": {
                "model": "default", "provider": keyed_ref, "placement": { "target": "e2b" },
            },
        }),
    )
    .await;
    let with_agent = created["workspace"]["id"].as_str().unwrap().to_owned();
    let agent_id = created["initialAgent"]["id"]
        .as_str()
        .or_else(|| created["initialAgent"]["agentId"].as_str())
        .unwrap()
        .to_owned();
    let home = &created["workspace"]["home"];
    assert_eq!(home["cloudMachineId"], entry.as_str(), "{created}");
    let agent_home = home["homeId"].as_str().unwrap().to_owned();
    let home = home_until(
        &mut rpc_ws,
        &mut id,
        &with_agent,
        "the agent's sandbox became the home",
        |home| home["state"] == "attached" && home["nodeId"].is_string(),
    )
    .await;
    let agent_node = home["nodeId"].as_str().unwrap().to_owned();
    assert_ne!(agent_node, lazy_node);
    assert_eq!(
        fake.state().creates.len(),
        2,
        "no home-only sandbox next to the agent's"
    );
    id += 1;
    let machine = machine_of(&mut rpc_ws, id, &agent_node).await;
    assert_eq!(machine["cloud"]["agentId"], agent_id.as_str(), "{machine}");
    assert_eq!(
        machine["cloud"]["startedBy"]["principalId"],
        owner.as_str(),
        "{machine}"
    );
    // A terminal runs in the home checkout on the agent's sandbox.
    let b64 = base64::engine::general_purpose::STANDARD;
    id += 1;
    let terminal = ok(
        &mut rpc_ws,
        id,
        "terminal.create",
        json!({ "workspaceId": with_agent, "cols": 80, "rows": 24, "command": "/bin/sh" }),
    )
    .await;
    let terminal_id = terminal["terminalId"].as_str().unwrap().to_owned();
    id += 1;
    ok(
        &mut rpc_ws,
        id,
        "terminal.write",
        json!({ "terminalId": terminal_id, "data": b64.encode("echo at:$(pwd -P)\n") }),
    )
    .await;
    let expected = format!("/homes/{agent_home}/work");
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
        // timing-guard: poll while the sandbox streams the terminal's output
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    id += 1;
    ok(
        &mut rpc_ws,
        id,
        "terminal.kill",
        json!({ "terminalId": terminal_id }),
    )
    .await;
    id += 1;
    ok(
        &mut rpc_ws,
        id,
        "file.write",
        json!({ "workspaceId": with_agent, "path": "agent-home.txt", "content": "on the agent's sandbox\n" }),
    )
    .await;
    id += 1;
    let listed = ok(
        &mut rpc_ws,
        id,
        "file.list",
        json!({ "workspaceId": with_agent, "path": "." }),
    )
    .await;
    assert!(listed.to_string().contains("agent-home.txt"), "{listed}");

    // 4. A home whose sandbox fails to start is detached at once.
    fake.state().failed_mint_templates.push(template);
    id += 1;
    let created = ok(
        &mut rpc_ws,
        id,
        "workspace.create",
        json!({ "repositoryPath": repo.to_string_lossy(), "baseRef": branch }),
    )
    .await;
    let failing = created["workspace"]["id"].as_str().unwrap().to_owned();
    id += 1;
    let reply = rpc(
        &mut rpc_ws,
        id,
        "file.read",
        json!({ "workspaceId": failing, "path": "README.md" }),
    )
    .await;
    assert_eq!(reply["error"]["data"]["code"], "home-unavailable", "{reply}");
    let home = home_until(
        &mut rpc_ws,
        &mut id,
        &failing,
        "the failed home detached",
        |home| home["state"] == "detached",
    )
    .await;
    assert!(home.get("nodeId").is_none(), "{home}");
    id += 1;
    let reply = rpc(
        &mut rpc_ws,
        id,
        "file.read",
        json!({ "workspaceId": failing, "path": "README.md" }),
    )
    .await;
    assert_eq!(reply["error"]["code"], -32603, "{reply}");
    assert_eq!(reply["error"]["data"]["code"], "home-unavailable", "{reply}");
    assert!(
        reply["error"]["data"].get("starting").is_none(),
        "a detached home is not starting: {reply}"
    );
    assert_no_secret_logged(&head_dir, &fake);
}
