//! Named E2B entries against the fake E2B, with two entries in two E2B
//! projects (two keys): each placement starts its sandbox with its own
//! entry's key, a placement without an entry falls back to `"e2b"`,
//! `apiKeyFrom` copies a key, a rename reaches the machine row and the
//! agent, and removing one entry deletes only its sandbox and clears its
//! agents' entry while the other entry's sandbox keeps running.

use super::*;

fn placed(ws_id: &str, provider: &str, name: &str, entry: Option<&str>) -> Value {
    let mut placement = json!({ "target": "e2b" });
    if let Some(entry) = entry {
        placement["cloudMachineId"] = json!(entry);
    }
    json!({
        "workspaceId": ws_id, "name": name, "model": "default", "provider": provider,
        "placement": placement,
    })
}

/// The machine `cloudMachines.list` shows for agent `agent_id`.
async fn machine_of(rpc_ws: &mut common::TlsWs, id: i64, agent_id: &str) -> Option<Value> {
    let listed = ok(rpc_ws, id, "cloudMachines.list", json!({})).await;
    listed["machines"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|machine| machine["cloud"]["agentId"] == agent_id)
        .cloned()
}

/// The backend's and every sandbox's log lines about links and stops, for a
/// remove that left a stop unacknowledged.
fn stop_logs(head_dir: &Path, scratch: &Path) -> String {
    let mut logs = vec![head_dir.join("head.log")];
    let mut dirs = vec![scratch.join("e2b")];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(&dir).into_iter().flatten().flatten() {
            let path = entry.path();
            if path.is_dir() {
                dirs.push(path);
            } else if path.file_name().is_some_and(|name| name == "serve.log") {
                logs.push(path);
            }
        }
    }
    let mut out = String::new();
    for log in logs {
        out.push_str("--- ");
        out.push_str(&log.display().to_string());
        out.push('\n');
        let text = std::fs::read_to_string(&log).unwrap_or_default();
        for line in text.lines().filter(|line| {
            ["stop", "link", "lease", "WARN", "ERROR"]
                .iter()
                .any(|word| line.contains(word))
        }) {
            out.push_str(line);
            out.push('\n');
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_named_entries_start_and_remove_their_own_sandboxes() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping E2B entries e2e: node or git not on PATH");
        return;
    }
    let fixture = format!(
        "{}/tests/fixtures/node-test-agent.mjs",
        env!("CARGO_MANIFEST_DIR")
    );
    let scratch = common::test_tempdir_in("/tmp", "itd-e2b-entries-");
    let repo = scratch.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    std::fs::write(repo.join("README.md"), "base\n").unwrap();
    git(&repo, &["add", "README.md"]);
    git(&repo, &["commit", "--quiet", "-m", "base"]);
    let fake = FakeE2b::start(scratch, &fixture).await;
    let head_dir = fake.scratch().join("head");
    let (_head, port, cfg, ws_id) = start_backend_with(
        &head_dir,
        &repo,
        &[
            ("INTENTD_E2B_API_URL", &fake.url),
            ("INTENTD_E2B_ENVD_URL", &fake.url),
            ("INTENTD_E2B_DIRECT_HOST", "127.0.0.1"),
        ],
    )
    .await;
    let mut rpc_ws = connect(port, cfg.clone()).await;
    let mut transcript = Vec::new();
    let began = std::time::Instant::now();
    let step = |name: &str| eprintln!("[entries {:>6.1}s] {name}", began.elapsed().as_secs_f64());
    let hello = ok(
        &mut rpc_ws,
        1,
        "client.hello",
        json!({ "clientId": "e2b-entries", "name": "E2B entries" }),
    )
    .await;
    assert_eq!(
        hello["server"]["capabilities"]["cloudMachineEntries"], 1,
        "{hello}"
    );
    let keyed = ok(
        &mut rpc_ws,
        2,
        "providers.profiles.create",
        json!({ "baseProviderId": "codex", "name": "Cloud", "env": [
            { "key": "OPENAI_API_KEY", "kind": "set", "value": PROFILE_SECRET },
        ] }),
    )
    .await;
    let keyed_ref = format!("codex@{}", keyed["profile"]["id"].as_str().unwrap());

    // Two entries, two projects. The first takes the id "e2b".
    let alpha = ok(
        &mut rpc_ws,
        3,
        "cloudMachines.configure",
        json!({ "name": "Alpha", "apiKey": E2B_KEY, "size": "4x8" }),
    )
    .await;
    transcript.push(alpha.to_string());
    assert_eq!(alpha["provider"]["id"], "e2b", "{alpha}");
    assert_eq!(alpha["provider"]["provider"], "e2b");
    assert_eq!(alpha["provider"]["name"], "Alpha");
    let beta = ok(
        &mut rpc_ws,
        4,
        "cloudMachines.configure",
        json!({ "name": "Beta", "apiKey": E2B_KEY_2, "size": "4x8" }),
    )
    .await;
    transcript.push(beta.to_string());
    let beta_id = beta["provider"]["id"].as_str().unwrap().to_owned();
    assert!(
        beta_id.len() == 16
            && beta_id.starts_with("e2b-")
            && beta_id[4..]
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
        "{beta}"
    );

    // A copied key works (it is tested before saving), and removing the
    // copy leaves the source's key in place.
    let copy = ok(
        &mut rpc_ws,
        5,
        "cloudMachines.configure",
        json!({ "name": "Copy", "apiKeyFrom": beta_id }),
    )
    .await;
    transcript.push(copy.to_string());
    let copy_id = copy["provider"]["id"].as_str().unwrap().to_owned();
    let listed = ok(&mut rpc_ws, 6, "cloudMachines.list", json!({})).await;
    let ids: Vec<&str> = listed["providers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["id"].as_str().unwrap())
        .collect();
    assert_eq!(
        ids,
        vec!["e2b", beta_id.as_str(), copy_id.as_str()],
        "{listed}"
    );
    let gone = ok(
        &mut rpc_ws,
        7,
        "cloudMachines.remove",
        json!({ "cloudMachineId": copy_id }),
    )
    .await;
    assert_eq!(gone, json!({ "ok": true, "unconfirmedStops": [] }));

    // A validateOnly create checks everything and saves nothing.
    let probe = ok(
        &mut rpc_ws,
        8,
        "cloudMachines.configure",
        json!({ "name": "Probe", "apiKey": E2B_KEY_2, "validateOnly": true }),
    )
    .await;
    assert_eq!(probe["provider"]["id"], "", "{probe}");
    assert_eq!(probe["provider"]["name"], "Probe");
    let listed = ok(&mut rpc_ws, 9, "cloudMachines.list", json!({})).await;
    assert_eq!(listed["providers"].as_array().unwrap().len(), 2, "{listed}");

    // One agent per entry: named, and the "e2b" fallback of a placement
    // that names none while several entries exist.
    let on_beta = ok(
        &mut rpc_ws,
        10,
        "agent.create",
        placed(&ws_id, &keyed_ref, "Bee", Some(&beta_id)),
    )
    .await;
    transcript.push(on_beta.to_string());
    let bee = on_beta["agent"]["id"].as_str().unwrap().to_owned();
    assert_eq!(on_beta["agent"]["nodeName"], "Beta", "{on_beta}");
    assert_eq!(
        on_beta["agent"]["placement"]["cloudMachineId"],
        beta_id.as_str()
    );
    let on_alpha = ok(
        &mut rpc_ws,
        11,
        "agent.create",
        placed(&ws_id, &keyed_ref, "Ant", None),
    )
    .await;
    transcript.push(on_alpha.to_string());
    let ant = on_alpha["agent"]["id"].as_str().unwrap().to_owned();
    assert_eq!(on_alpha["agent"]["nodeName"], "Alpha", "{on_alpha}");
    assert_eq!(on_alpha["agent"]["placement"]["cloudMachineId"], "e2b");
    let unknown = rpc(
        &mut rpc_ws,
        12,
        "agent.create",
        placed(&ws_id, &keyed_ref, "Nobody", Some("e2b-ffffffffffff")),
    )
    .await;
    assert_eq!(
        unknown["error"]["data"]["reason"], "not-configured",
        "{unknown}"
    );
    assert_eq!(
        unknown["error"]["data"]["cloudMachineId"],
        "e2b-ffffffffffff"
    );

    let bee_linked = agent_until(&mut rpc_ws, (&ws_id, &bee), 20, |a| {
        a.get("nodeId").is_some()
    })
    .await;
    let ant_linked = agent_until(&mut rpc_ws, (&ws_id, &ant), 40, |a| {
        a.get("nodeId").is_some()
    })
    .await;
    transcript.push(bee_linked.to_string());
    transcript.push(ant_linked.to_string());
    let sandbox_of = |agent: &str| {
        let state = fake.state();
        let (id, _) = state
            .metadata
            .iter()
            .find(|(_, metadata)| metadata["agentId"] == agent)
            .expect("a sandbox for the agent");
        (
            id.clone(),
            state.projects[id].clone(),
            state.metadata[id].clone(),
        )
    };
    let (bee_sandbox, bee_project, bee_metadata) = sandbox_of(&bee);
    let (ant_sandbox, ant_project, ant_metadata) = sandbox_of(&ant);
    assert_eq!(
        bee_project, E2B_KEY_2,
        "Beta's sandbox is in Beta's project"
    );
    assert_eq!(
        ant_project, E2B_KEY,
        "Alpha's sandbox is in Alpha's project"
    );
    assert_eq!(bee_metadata["intentEntryId"], beta_id.as_str());
    assert_eq!(ant_metadata["intentEntryId"], "e2b");
    let bee_machine = machine_of(&mut rpc_ws, 60, &bee).await.unwrap();
    let ant_machine = machine_of(&mut rpc_ws, 61, &ant).await.unwrap();
    assert_eq!(bee_machine["name"], "Beta · Bee", "{bee_machine}");
    assert_eq!(bee_machine["cloud"]["cloudMachineId"], beta_id.as_str());
    assert_eq!(ant_machine["name"], "Alpha · Ant", "{ant_machine}");
    assert_eq!(ant_machine["cloud"]["cloudMachineId"], "e2b");

    step("both agents linked");
    // A sub-agent placed on E2B without an entry, while several exist,
    // starts with its parent's entry.
    let mut events = connect(port, cfg.clone()).await;
    ok(
        &mut events,
        100,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    let kid_prompt = r#"wsapi return JSON.stringify({ marker: "kid-1" });"#;
    let code = format!(
        r#"wsapi await ws.agent.create("Kid", {kid_prompt:?}, {{ provider: {keyed_ref:?}, model: "default", placement: {{ target: "e2b" }} }}); return JSON.stringify({{ marker: "bee-1" }});"#
    );
    wsapi_turn(
        &mut rpc_ws,
        &mut events,
        (&ws_id, &bee),
        &code,
        "bee-1",
        110,
    )
    .await;
    drop(events);
    let kid = timeout(wait(), async {
        let mut id = 150;
        loop {
            id += 1;
            let listed = ok(
                &mut rpc_ws,
                id,
                "agent.list",
                json!({ "workspaceId": ws_id }),
            )
            .await;
            if let Some(kid) = listed["agents"]
                .as_array()
                .into_iter()
                .flatten()
                .find(|a| a["name"] == "Kid")
            {
                return kid["id"].as_str().unwrap().to_owned();
            }
            // timing-guard: poll interval for the forwarded create to be listed
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("the sub-agent was created");
    let kid_linked = agent_until(&mut rpc_ws, (&ws_id, &kid), 200, |a| {
        a.get("nodeId").is_some()
    })
    .await;
    transcript.push(kid_linked.to_string());
    assert_eq!(
        kid_linked["placement"]["cloudMachineId"],
        beta_id.as_str(),
        "{kid_linked}"
    );
    assert_eq!(kid_linked["nodeName"], "Beta");
    let (kid_sandbox, kid_project, _) = sandbox_of(&kid);
    assert_eq!(
        kid_project, E2B_KEY_2,
        "the sub-agent's sandbox is in Beta's project"
    );

    // Bee watches the sub-agent it created: Bee's wake turn ends before
    // the remove, or the wake would start Bee again on the entry left.
    // The reply is streamed while the turn runs, hence also idle.
    agent_until(&mut rpc_ws, (&ws_id, &bee), 300, |a| {
        a["status"] == "idle"
            && a["lastAgentResponse"]
                .as_str()
                .is_some_and(|reply| reply.contains("Child agent Kid"))
    })
    .await;
    step("sub-agent linked");
    // A rename reaches the machine row and the agent.
    let renamed = ok(
        &mut rpc_ws,
        70,
        "cloudMachines.configure",
        json!({ "cloudMachineId": beta_id, "name": "Gamma" }),
    )
    .await;
    assert_eq!(renamed["provider"]["name"], "Gamma", "{renamed}");
    let bee_machine = machine_of(&mut rpc_ws, 71, &bee).await.unwrap();
    assert_eq!(bee_machine["name"], "Gamma · Bee", "{bee_machine}");
    let bee_now = agent_until(&mut rpc_ws, (&ws_id, &bee), 72, |a| {
        a["nodeName"] == "Gamma"
    })
    .await;
    assert_eq!(bee_now["placement"]["cloudMachineId"], beta_id.as_str());

    step("renamed");
    // The sub-agent is mid-turn at the remove: its final stop ends the turn
    // as stopped, never failed or redriven against the removed entry.
    let streamed = |a: &Value| a["lastStreamActivityAt"].clone();
    let idle_kid = ok(
        &mut rpc_ws,
        73,
        "agent.get",
        json!({ "workspaceId": ws_id, "agentId": kid }),
    )
    .await;
    let streamed_before = streamed(&idle_kid["agent"]);
    ok(
        &mut rpc_ws,
        74,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": kid, "content": "block here" }),
    )
    .await;
    // A remote turn's text is not served mid-turn: its first streamed
    // chunk ("echo: block here") moves the stream stamp, and then it parks.
    agent_until(&mut rpc_ws, (&ws_id, &kid), 75, |a| {
        a["turnInFlight"] == true
            && a["lastUserMessage"] == "block here"
            && streamed(a) != streamed_before
    })
    .await;
    let creates_before = fake.state().creates.len();
    // Removing one entry: its live sandbox needs stopAgents; then only its
    // sandbox goes, and its agent no longer names it.
    let busy = rpc(
        &mut rpc_ws,
        80,
        "cloudMachines.remove",
        json!({ "cloudMachineId": beta_id }),
    )
    .await;
    assert_eq!(busy["error"]["data"]["code"], "machine-in-use", "{busy}");
    let removed = ok(
        &mut rpc_ws,
        81,
        "cloudMachines.remove",
        json!({ "cloudMachineId": beta_id, "stopAgents": true }),
    )
    .await;
    step("entry removed");
    transcript.push(removed.to_string());
    assert_eq!(
        removed,
        json!({ "ok": true, "unconfirmedStops": [] }),
        "every stop is acknowledged\n{}",
        stop_logs(&head_dir, fake.scratch())
    );
    {
        let state = fake.state();
        assert!(
            state.deletes.contains(&bee_sandbox),
            "Gamma's sandbox is deleted"
        );
        assert!(
            state.deletes.contains(&kid_sandbox),
            "so is its sub-agent's"
        );
        assert!(
            !state.deletes.contains(&ant_sandbox),
            "Alpha's sandbox is kept"
        );
        assert!(state.sandboxes.contains_key(&ant_sandbox));
    }
    let after = ok(&mut rpc_ws, 82, "cloudMachines.list", json!({})).await;
    transcript.push(after.to_string());
    let providers: Vec<&str> = after["providers"]
        .as_array()
        .unwrap()
        .iter()
        .map(|p| p["id"].as_str().unwrap())
        .collect();
    assert_eq!(providers, vec!["e2b"], "{after}");
    let machines = after["machines"].as_array().unwrap();
    assert_eq!(machines.len(), 1, "{after}");
    assert_eq!(machines[0]["cloud"]["agentId"], ant.as_str());
    let bee_left = agent_until(&mut rpc_ws, (&ws_id, &bee), 83, |a| {
        a.get("nodeId").is_none() && a["placement"].get("cloudMachineId").is_none()
    })
    .await;
    assert_eq!(bee_left["placement"]["target"], "e2b", "{bee_left}");
    let kid_left = agent_until(&mut rpc_ws, (&ws_id, &kid), 85, |a| {
        a.get("nodeId").is_none() && !matches!(a["status"].as_str(), Some("pending" | "active"))
    })
    .await;
    assert_eq!(kid_left["status"], "idle", "{kid_left}");
    assert!(kid_left.get("placementError").is_none(), "{kid_left}");
    let log = std::fs::read_to_string(head_dir.join("head.log")).unwrap_or_default();
    assert!(
        !log.contains("redriving the prompt"),
        "a stopped turn was redriven"
    );
    assert_eq!(
        fake.state().creates.len(),
        creates_before,
        "nothing started again on a sandbox"
    );
    let again = ok(
        &mut rpc_ws,
        90,
        "cloudMachines.remove",
        json!({ "cloudMachineId": beta_id }),
    )
    .await;
    assert_eq!(
        again,
        json!({ "ok": true, "unconfirmedStops": [] }),
        "a repeated remove is a no-op"
    );

    // Neither key reaches the backend's log or a reply.
    let log = std::fs::read_to_string(head_dir.join("head.log")).unwrap_or_default();
    for secret in [E2B_KEY, E2B_KEY_2, PROFILE_SECRET] {
        assert!(!log.contains(secret), "a key reached the backend's log");
        for text in &transcript {
            assert!(!text.contains(secret), "a key reached a reply: {text}");
        }
    }
}

/// The backend's saved entry rows (removed entries' kept keys included).
async fn provider_rows(head_dir: &Path) -> Vec<String> {
    let store = intent_store::Store::open(&head_dir.join("intentd.db"))
        .await
        .expect("open store");
    store
        .list_cloud_machine_providers()
        .await
        .unwrap()
        .into_iter()
        .map(|(id, _)| id)
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_removed_entrys_key_stays_until_the_reaper_deletes_its_sandbox() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping E2B entries e2e: node or git not on PATH");
        return;
    }
    let fixture = format!(
        "{}/tests/fixtures/node-test-agent.mjs",
        env!("CARGO_MANIFEST_DIR")
    );
    let scratch = common::test_tempdir_in("/tmp", "itd-e2b-retired-");
    let repo = scratch.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    std::fs::write(repo.join("README.md"), "base\n").unwrap();
    git(&repo, &["add", "README.md"]);
    git(&repo, &["commit", "--quiet", "-m", "base"]);
    let fake = FakeE2b::start(scratch, &fixture).await;
    let head_dir = fake.scratch().join("head");
    // A settings minute of 100 ms: the reaper lists every second.
    let (_head, port, cfg, ws_id) = start_backend_with(
        &head_dir,
        &repo,
        &[
            ("INTENTD_E2B_API_URL", &fake.url),
            ("INTENTD_E2B_ENVD_URL", &fake.url),
            ("INTENTD_E2B_DIRECT_HOST", "127.0.0.1"),
            ("INTENTD_E2B_MINUTE_MS", "100"),
        ],
    )
    .await;
    let mut rpc_ws = connect(port, cfg.clone()).await;
    let keyed = ok(
        &mut rpc_ws,
        1,
        "providers.profiles.create",
        json!({ "baseProviderId": "codex", "name": "Cloud", "env": [
            { "key": "OPENAI_API_KEY", "kind": "set", "value": PROFILE_SECRET },
        ] }),
    )
    .await;
    let keyed_ref = format!("codex@{}", keyed["profile"]["id"].as_str().unwrap());
    ok(
        &mut rpc_ws,
        2,
        "cloudMachines.configure",
        json!({ "name": "Alpha", "apiKey": E2B_KEY, "size": "4x8", "idlePauseMinutes": 1440 }),
    )
    .await;
    let created = ok(
        &mut rpc_ws,
        3,
        "agent.create",
        placed(&ws_id, &keyed_ref, "Ant", None),
    )
    .await;
    let ant = created["agent"]["id"].as_str().unwrap().to_owned();
    agent_until(&mut rpc_ws, (&ws_id, &ant), 10, |a| {
        a.get("nodeId").is_some()
    })
    .await;
    let sandbox = fake.state().sandboxes.keys().next().cloned().unwrap();

    // E2B will not delete: the entry goes, its key stays hidden.
    fake.state().refuse_deletes = true;
    let removed = ok(
        &mut rpc_ws,
        40,
        "cloudMachines.remove",
        json!({ "cloudMachineId": "e2b", "stopAgents": true }),
    )
    .await;
    assert_eq!(removed["ok"], true, "{removed}");
    assert!(fake.state().sandboxes.contains_key(&sandbox), "E2B kept it");
    let listed = ok(&mut rpc_ws, 41, "cloudMachines.list", json!({})).await;
    assert_eq!(
        listed,
        json!({ "providers": [], "machines": [], "providerKinds": ["e2b", "northflank", "agent37", "vercel"] }),
        "{listed}"
    );
    let rows = provider_rows(&head_dir).await;
    assert!(
        rows.len() == 1 && rows[0].starts_with("retired-"),
        "the key is kept for the reaper: {rows:?}"
    );
    let ant_now = agent_until(&mut rpc_ws, (&ws_id, &ant), 42, |a| {
        a["placement"].get("cloudMachineId").is_none()
    })
    .await;
    assert_eq!(ant_now["placement"]["target"], "e2b", "{ant_now}");

    // A new entry does not take the id the kept key still belongs to.
    let next = ok(
        &mut rpc_ws,
        50,
        "cloudMachines.configure",
        json!({ "name": "Next", "apiKey": E2B_KEY_2, "size": "4x8", "idlePauseMinutes": 1440 }),
    )
    .await;
    let next_id = next["provider"]["id"].as_str().unwrap().to_owned();
    assert_ne!(next_id, "e2b", "{next}");

    // Once E2B deletes again, the reaper deletes the sandbox, then forgets
    // the kept key.
    fake.state().refuse_deletes = false;
    timeout(wait(), async {
        loop {
            let gone = !fake.state().sandboxes.contains_key(&sandbox);
            if gone && provider_rows(&head_dir).await == vec![next_id.clone()] {
                return;
            }
            // timing-guard: poll interval for the reaper's next pass (every second here)
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })
    .await
    .expect("the reaper deletes the sandbox and the kept key");
    let log = std::fs::read_to_string(head_dir.join("head.log")).unwrap_or_default();
    for secret in [E2B_KEY, E2B_KEY_2, PROFILE_SECRET] {
        assert!(!log.contains(secret), "a key reached the backend's log");
    }
}
