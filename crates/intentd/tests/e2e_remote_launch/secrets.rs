//! A remote agent's secrets stay on its machine (nodeProtocol 14, monorepo
//! `docs/protocol/methods/secrets.md` § Remote agents): the agent's
//! `ws.secret.request` is forwarded to head, `secret.provide` sends the
//! value to the agent's machine only, its `ws.host.exec` `secretEnv` uses it
//! there with the output redacted, another agent on the same machine is
//! refused, and a head restart drops it. Head never writes the value: every
//! file under its data dir is scanned. Head runs with the bound-caller
//! assertion armed, so every frame the machine forwards runs as its agent.

use super::*;

/// Distinctive, so the data-dir scans cannot match anything by accident.
const SECRET_VALUE: &str = "s5-remote-secret-Qx7m2Lk9";

/// Every file under `dir` that contains `needle`.
fn files_containing(dir: &Path, needle: &str) -> Vec<std::path::PathBuf> {
    let mut found = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(next) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&next) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            match entry.file_type() {
                Ok(kind) if kind.is_dir() => stack.push(path),
                Ok(kind) if kind.is_file() => {
                    let bytes = std::fs::read(&path).unwrap_or_default();
                    if bytes
                        .windows(needle.len())
                        .any(|window| window == needle.as_bytes())
                    {
                        found.push(path);
                    }
                }
                _ => {}
            }
        }
    }
    found
}

/// The `ws.host.exec` code that prints the SHA-256 of `$API_KEY` and the
/// value itself (which the machine must redact), as a `wsapi` prompt whose
/// reply carries `marker`. Failures come back as `{ marker, error }`.
fn exec_prompt(marker: &str) -> String {
    format!(
        r#"wsapi try {{ const r = await ws.host.exec({{ command: "sh", args: ["-c", "printf '%s' \"$API_KEY\" | sha256sum | cut -c1-64; printf 'raw=%s' \"$API_KEY\""], secretEnv: ["API_KEY"] }}); return JSON.stringify({{ marker: "{marker}", r }}); }} catch (e) {{ return JSON.stringify({{ marker: "{marker}", error: String(e) }}); }}"#
    )
}

async fn create_agent(rpc_ws: &mut common::TlsWs, ws_id: &str, name: &str, id: i64) -> String {
    let create = json!({
        "workspaceId": ws_id, "name": name, "model": "default", "provider": "mock",
        "placement": { "target": "remote" },
    });
    let deadline = tokio::time::Instant::now() + wait();
    let mut id = id;
    loop {
        id += 1;
        let response = rpc(rpc_ws, id, "agent.create", create.clone()).await;
        if response.get("error").is_none() {
            return response["result"]["agent"]["id"]
                .as_str()
                .unwrap()
                .to_owned();
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the machine never took the placement: {response}"
        );
        // timing-guard: poll interval while the machine daemon binds to head (no event exposes it)
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// [`wsapi_turn`] for one of several agents in the workspace: other
/// agents' turn ends (the provided notice wakes the owner) are skipped, and
/// the reply is polled until it carries `marker`.
async fn agent_turn(
    rpc_ws: &mut common::TlsWs,
    events: &mut common::TlsWs,
    (ws_id, agent_id): (&str, &str),
    content: &str,
    marker: &str,
    id: i64,
) -> String {
    ok(
        rpc_ws,
        id,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": content }),
    )
    .await;
    let mut next = id + 1;
    let found = timeout(wait(), async {
        loop {
            let end = event(events, "agent:stream:end").await;
            if end["data"]["agentId"] != agent_id {
                continue;
            }
            // The turn's reply may be recorded just after its end event.
            for _ in 0..20 {
                let got = ok(
                    rpc_ws,
                    next,
                    "agent.get",
                    json!({ "workspaceId": ws_id, "agentId": agent_id }),
                )
                .await;
                next += 1;
                let reply = got["agent"]["lastAgentResponse"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned();
                if reply.contains(marker) {
                    return reply;
                }
                // timing-guard: poll interval for this turn's reply to be recorded
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    })
    .await;
    found.unwrap_or_else(|_| panic!("no reply of {agent_id} carrying {marker:?}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_remote_agents_secret_stays_on_its_machine() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
        || intent_providers::resolve_on_path("sha256sum").is_none()
    {
        eprintln!("skipping remote S5 e2e: node, git or sha256sum not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-s5-");
    let repo = scratch.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    std::fs::write(repo.join("README.md"), "base\n").unwrap();
    git(&repo, &["add", "README.md"]);
    git(&repo, &["commit", "--quiet", "-m", "base"]);
    let head_dir = scratch.path().join("head");
    std::fs::create_dir_all(&head_dir).unwrap();
    std::fs::write(
        head_dir.join("config.toml"),
        "[agentFeatures]\nsecretRequests = true\n",
    )
    .unwrap();
    let (head, port, cfg, ws_id) = start_backend(&head_dir, &repo).await;
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
    let owner = create_agent(&mut rpc_ws, &ws_id, "Owner", 10).await;
    let other = create_agent(&mut rpc_ws, &ws_id, "Other", 40).await;

    // The owner's request is forwarded and recorded on head.
    let reply = agent_turn(
        &mut rpc_ws,
        &mut events,
        (&ws_id, &owner),
        r#"wsapi const r = await ws.secret.request({ name: "API_KEY", reason: "e2e" }); return JSON.stringify({ marker: "turn-100", r });"#,
        "turn-100",
        100,
    )
    .await;
    let requested = wsapi_value(&reply);
    let request_id = requested["r"]["requestId"]
        .as_str()
        .unwrap_or_else(|| panic!("{requested}"))
        .to_owned();

    // The value goes to the machine; head lists the name from there.
    let provided = ok(
        &mut rpc_ws,
        200,
        "secret.provide",
        json!({ "workspaceId": ws_id, "requestId": request_id, "value": SECRET_VALUE }),
    )
    .await;
    assert_eq!(provided["name"], "API_KEY", "{provided}");
    let listed = ok(
        &mut rpc_ws,
        201,
        "secret.list",
        json!({ "workspaceId": ws_id, "agentId": owner }),
    )
    .await;
    assert_eq!(
        listed["secrets"],
        json!([{ "name": "API_KEY" }]),
        "{listed}"
    );
    assert!(listed.get("secretsUnavailable").is_none(), "{listed}");

    // The owner's host.exec on the machine gets the value, redacted.
    let want =
        Sha256::digest(SECRET_VALUE.as_bytes())
            .iter()
            .fold(String::new(), |mut hex, byte| {
                use std::fmt::Write as _;
                let _ = write!(hex, "{byte:02x}");
                hex
            });
    let reply = agent_turn(
        &mut rpc_ws,
        &mut events,
        (&ws_id, &owner),
        &exec_prompt("turn-300"),
        "turn-300",
        300,
    )
    .await;
    let ran = wsapi_value(&reply);
    let stdout = ran["r"]["stdout"]
        .as_str()
        .unwrap_or_else(|| panic!("{ran}"));
    assert!(
        stdout.starts_with(&want),
        "the value reached the command: {ran}"
    );
    assert!(stdout.ends_with("raw=[secret:API_KEY]"), "{ran}");

    // Another agent on the same machine never reaches it.
    let reply = agent_turn(
        &mut rpc_ws,
        &mut events,
        (&ws_id, &other),
        &exec_prompt("turn-400"),
        "turn-400",
        400,
    )
    .await;
    let refused = wsapi_value(&reply);
    let error = refused["error"]
        .as_str()
        .unwrap_or_else(|| panic!("{refused}"));
    assert!(error.contains("secret-unavailable"), "{refused}");
    let listed = ok(
        &mut rpc_ws,
        401,
        "secret.list",
        json!({ "workspaceId": ws_id, "agentId": other }),
    )
    .await;
    assert_eq!(listed["secrets"], json!([]), "{listed}");

    // Head never wrote the value: not in its store, WAL, logs or secrets
    // file. The machine keeps it in memory only.
    assert_eq!(
        files_containing(&head_dir, SECRET_VALUE),
        Vec::<std::path::PathBuf>::new()
    );
    assert_eq!(
        files_containing(&node_dir, SECRET_VALUE),
        Vec::<std::path::PathBuf>::new()
    );

    // A head restart drops it on the machine (a new headSession).
    drop(rpc_ws);
    drop(events);
    drop(head);
    let (_head, port, cfg) = serve_backend(&head_dir, &[]).await;
    let mut rpc_ws = connect(port, cfg.clone()).await;
    let mut events = connect(port, cfg.clone()).await;
    ok(
        &mut events,
        500,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    let reply = agent_turn(
        &mut rpc_ws,
        &mut events,
        (&ws_id, &owner),
        &exec_prompt("turn-600"),
        "turn-600",
        600,
    )
    .await;
    let after = wsapi_value(&reply);
    let error = after["error"].as_str().unwrap_or_else(|| panic!("{after}"));
    assert!(error.contains("secret-unavailable"), "{after}");
    assert!(!after.to_string().contains(SECRET_VALUE), "{after}");
    assert_eq!(
        files_containing(&head_dir, SECRET_VALUE),
        Vec::<std::path::PathBuf>::new()
    );
    drop(machine);
}
