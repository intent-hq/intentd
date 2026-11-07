//! Sub-agents of an agent on an E2B sandbox, against the fake E2B: with no
//! placement a sub-agent shares the parent's sandbox; with `target: "e2b"`
//! it gets a new sandbox of its own, started from the parent's work and
//! credited to the member behind the agent chain, who may then pause it;
//! retiring the parent ends both sandboxes.

use sha2::{Digest as _, Sha256};

use super::*;

const MEMBER_TOKEN: &str = "f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1";

/// Add a host member who connects with [`MEMBER_TOKEN`]; returns its id.
async fn seed_member(data_dir: &Path) -> String {
    use intent_core::{now_iso, Principal, PrincipalId};
    let store = intent_store::Store::open(&data_dir.join("intentd.db"))
        .await
        .expect("open store");
    let principal = Principal {
        id: PrincipalId::new(),
        identity: None,
        github_user_id: None,
        login: Some("member".into()),
        display_name: Some("Member".into()),
        avatar_url: None,
        is_primary: false,
        created_at: now_iso(),
        updated_at: now_iso(),
    };
    store.upsert_principal(&principal).await.unwrap();
    let hash = lower_hex(&Sha256::digest(MEMBER_TOKEN.as_bytes()));
    store
        .insert_principal_credential(&principal.id, &hash)
        .await
        .unwrap();
    sqlx::query("INSERT INTO host_member(principal_id,added_at) VALUES (?,?)")
        .bind(principal.id.as_str())
        .bind(now_iso())
        .execute(store.write_pool())
        .await
        .unwrap();
    principal.id.0
}

/// The id of the workspace's agent named `name`, once it is listed.
async fn agent_named(rpc_ws: &mut common::TlsWs, ws_id: &str, name: &str, id: i64) -> String {
    timeout(wait(), async {
        let mut id = id;
        loop {
            id += 1;
            let listed = ok(rpc_ws, id, "agent.list", json!({ "workspaceId": ws_id })).await;
            if let Some(found) = listed["agents"]
                .as_array()
                .into_iter()
                .flatten()
                .find(|agent| agent["name"] == name)
                .and_then(|agent| agent["id"].as_str())
            {
                return found.to_owned();
            }
            // timing-guard: poll interval for the forwarded create to be listed
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("no agent named {name}"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sandbox_agents_sub_agents_share_its_sandbox_or_start_their_own() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping E2B sub-agent e2e: node or git not on PATH");
        return;
    }
    let fixture = format!(
        "{}/tests/fixtures/node-test-agent.mjs",
        env!("CARGO_MANIFEST_DIR")
    );
    let scratch = common::test_tempdir_in("/tmp", "itd-e2b-sub-");
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
    std::fs::create_dir_all(head_dir.join("workspaces")).unwrap();
    let ws_id = seed_workspace(&head_dir, &repo).await;
    let member_id = seed_member(&head_dir).await;
    let (_head, port, cfg) = serve_backend(
        &head_dir,
        &[
            ("INTENTD_E2B_API_URL", &fake.url),
            ("INTENTD_E2B_ENVD_URL", &fake.url),
            ("INTENTD_E2B_DIRECT_HOST", "127.0.0.1"),
        ],
    )
    .await;
    let mut rpc_ws = connect(port, cfg.clone()).await;
    let mut member = common::wss_connect_with_retry(
        port,
        cfg.clone(),
        &format!("wss://localhost:{port}/ws?token={MEMBER_TOKEN}"),
    )
    .await;
    let mut events = connect(port, cfg.clone()).await;
    ok(
        &mut events,
        1,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    ok(
        &mut rpc_ws,
        2,
        "cloudMachines.configure",
        json!({ "provider": "e2b", "apiKey": E2B_KEY, "size": "4x8" }),
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

    // The member places the parent on a new sandbox.
    let parent = ok(
        &mut member,
        4,
        "agent.create",
        json!({
            "workspaceId": ws_id, "name": "Parent", "model": "default", "provider": keyed_ref,
            "placement": { "target": "e2b" },
        }),
    )
    .await;
    let parent_id = parent["agent"]["id"].as_str().unwrap().to_owned();
    let reply = turn(
        &mut rpc_ws,
        &mut events,
        (&ws_id, &parent_id),
        "hello cloud",
        10,
    )
    .await;
    assert!(reply.contains("hello cloud"), "{reply}");
    let parent_node = agent_until(&mut rpc_ws, (&ws_id, &parent_id), 20, |a| {
        a.get("nodeId").is_some()
    })
    .await["nodeId"]
        .as_str()
        .unwrap()
        .to_owned();
    let parent_machine = cloud_of(&mut rpc_ws, 40, "sbx1").await.expect("sbx1");
    assert_eq!(
        parent_machine["cloud"]["startedBy"]["principalId"],
        member_id.as_str(),
        "{parent_machine}"
    );

    // One sub-agent with no placement, one with `target: "e2b"`, created
    // mid-turn after an uncommitted edit.
    let read =
        r#"const p = await ws.file.read("parent.txt"); const i = await ws.workspace.info();"#;
    let shared_prompt = format!(
        r#"wsapi {read} return JSON.stringify({{ marker: "shared-1", p, path: i.path }});"#
    );
    let fresh_prompt =
        format!(r#"wsapi {read} return JSON.stringify({{ marker: "fresh-1", p, path: i.path }});"#);
    let code = format!(
        r#"wsapi await ws.file.write("parent.txt", "from parent"); const i = await ws.workspace.info(); const s = await ws.agent.create("Shared", {shared_prompt:?}, {{ provider: {keyed_ref:?}, model: "default" }}); const f = await ws.agent.create("Fresh", {fresh_prompt:?}, {{ provider: {keyed_ref:?}, model: "default", placement: {{ target: "e2b" }} }}); return JSON.stringify({{ marker: "parent-1", path: i.path }});"#
    );
    let made = wsapi_turn(
        &mut rpc_ws,
        &mut events,
        (&ws_id, &parent_id),
        &code,
        "parent-1",
        50,
    )
    .await;
    let parent_path = wsapi_value(&made)["path"].as_str().unwrap().to_owned();
    let shared_id = agent_named(&mut rpc_ws, &ws_id, "Shared", 100).await;
    let fresh_id = agent_named(&mut rpc_ws, &ws_id, "Fresh", 200).await;

    // No placement: the parent's sandbox and checkout.
    let shared = agent_until(&mut rpc_ws, (&ws_id, &shared_id), 300, |a| {
        a.get("nodeId").is_some()
    })
    .await;
    assert_eq!(shared["nodeId"], parent_node.as_str(), "{shared}");
    let seen = reply_containing(&mut rpc_ws, (&ws_id, &shared_id), "shared-1", 400).await;
    assert!(seen.contains(&parent_path), "shares the checkout: {seen}");

    // `target: "e2b"`: a new sandbox the child owns, credited to the member,
    // starting from the parent's uncommitted work in its own checkout.
    let fresh = agent_until(&mut rpc_ws, (&ws_id, &fresh_id), 500, |a| {
        a.get("nodeId").is_some()
    })
    .await;
    let fresh_node = fresh["nodeId"].as_str().unwrap().to_owned();
    assert_ne!(fresh_node, parent_node, "{fresh}");
    assert_eq!(fresh["placement"]["target"], "e2b", "{fresh}");
    assert_eq!(fake.state().creates.len(), 2, "one new sandbox");
    let fresh_machine = cloud_of(&mut rpc_ws, 600, "sbx2").await.expect("sbx2");
    assert_eq!(fresh_machine["id"], fresh_node.as_str(), "{fresh_machine}");
    assert_eq!(fresh_machine["cloud"]["agentId"], fresh_id.as_str());
    assert_eq!(
        fresh_machine["cloud"]["startedBy"]["principalId"],
        member_id.as_str(),
        "credited to the member behind the chain: {fresh_machine}"
    );
    let seen = reply_containing(&mut rpc_ws, (&ws_id, &fresh_id), "fresh-1", 700).await;
    assert!(
        seen.contains("from parent"),
        "starts from the parent's work: {seen}"
    );
    assert!(!seen.contains(&parent_path), "its own checkout: {seen}");

    // The member may pause the sandbox its agents started.
    let paused = ok(
        &mut member,
        800,
        "cloudMachines.pause",
        json!({ "machineId": fresh_node }),
    )
    .await;
    assert_eq!(paused["machine"]["cloud"]["state"], "paused", "{paused}");

    // Retiring the parent retires its sub-agents: their final stops end
    // both sandboxes. (A user may not retire the last top-level agent.)
    ok(
        &mut rpc_ws,
        899,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Keeper", "model": "default", "provider": keyed_ref }),
    )
    .await;
    ok(
        &mut rpc_ws,
        900,
        "agent.retire",
        json!({ "workspaceId": ws_id, "agentId": parent_id }),
    )
    .await;
    for sandbox in ["sbx1", "sbx2"] {
        machine_until(&mut rpc_ws, 1000, sandbox, |machine| machine.is_none()).await;
        // The E2B delete follows the machine row's removal.
        call_until(&fake, sandbox, |calls| {
            calls.iter().any(|call| call.0 == "DELETE")
        })
        .await;
    }
}
