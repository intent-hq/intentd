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
    let sandboxes = scratch.path().join("e2b");
    std::fs::create_dir_all(&sandboxes).unwrap();
    let fake = FakeE2b::start(&sandboxes, &fixture).await;
    let head_dir = scratch.path().join("head");
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
    transcript.push(removed.to_string());
    assert_eq!(removed["ok"], true, "{removed}");
    {
        let state = fake.state();
        assert!(
            state.deletes.contains(&bee_sandbox),
            "Gamma's sandbox is deleted"
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
