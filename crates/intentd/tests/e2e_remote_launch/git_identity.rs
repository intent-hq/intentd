//! Remote agents commit with the backend's git identity (monorepo
//! `docs/protocol/node-link.md` § Commit identity): the backend resolves
//! `user.name` / `user.email` from the workspace checkout's config chain at
//! every start, so a repository-local identity beats the global one and a
//! config change reaches the next start; a top-level agent and its sub-agent
//! both commit as that identity, over the machine's inherited one; and with
//! no identity on the backend the machine sets nothing. The machine never writes it into git config.

use super::forge_push::{child_named, sh_ok};
use super::secrets::{agent_turn, create_agent};
use super::*;

/// Author and committer of a fresh commit in the agent's checkout.
const COMMIT_AND_SHOW: &str =
    "git commit --quiet --allow-empty -m by-agent && git log -1 --format='%an|%ae|%cn|%ce'";

/// The agent's four identity variables, `unset` for a missing one.
const SHOW_ENV: &str = "echo \"${GIT_AUTHOR_NAME-unset}|${GIT_AUTHOR_EMAIL-unset}|${GIT_COMMITTER_NAME-unset}|${GIT_COMMITTER_EMAIL-unset}\"";

/// What git config in the checkout says (empty when nothing is set there).
const SHOW_CONFIG: &str = "git config --local --get-regexp '^user\\.' || true";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_agents_commit_with_the_backends_identity_for_the_workspace() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping git identity e2e: node or git not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-ident-");
    let repo = scratch.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet", "--initial-branch=main"]);
    std::fs::write(repo.join("README.md"), "base\n").unwrap();
    git(&repo, &["add", "README.md"]);
    git(&repo, &["commit", "--quiet", "-m", "base"]);
    git(&repo, &["config", "user.name", "Repo Person"]);
    git(&repo, &["config", "user.email", "repo@example.invalid"]);
    // The backend's own global config, away from the host's.
    let home = scratch.path().join("home");
    std::fs::create_dir_all(&home).unwrap();
    let global = home.join(".gitconfig");
    std::fs::write(
        &global,
        "[user]\n\tname = Global Person\n\temail = global@example.invalid\n",
    )
    .unwrap();
    let xdg = scratch.path().join("xdg");
    std::fs::create_dir_all(&xdg).unwrap();
    let head_dir = scratch.path().join("head");
    std::fs::create_dir_all(&head_dir).unwrap();
    let home_s = home.to_string_lossy().into_owned();
    let xdg_s = xdg.to_string_lossy().into_owned();
    let (_head, port, cfg, ws_id) = start_backend_with(
        &head_dir,
        &repo,
        &[("HOME", &home_s), ("XDG_CONFIG_HOME", &xdg_s)],
    )
    .await;
    let mut rpc_ws = connect(port, cfg.clone()).await;
    let backend = ok(&mut rpc_ws, 1, "remoteMachines.backendInfo", json!({})).await;
    let backend_id = backend["backendId"].as_str().unwrap().to_owned();
    let mut events = connect(port, cfg.clone()).await;
    ok(
        &mut events,
        2,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    let node_dir = scratch.path().join("node");
    std::fs::create_dir_all(&node_dir).unwrap();
    // The machine's own inherited identity: the backend's beats it.
    let machine = start_machine_with(
        &node_dir,
        &backend_id,
        &[("GIT_AUTHOR_NAME", "Machine Person")],
    )
    .await;
    ok(
        &mut rpc_ws,
        3,
        "remoteMachines.add",
        add_params(&machine, "E2E node"),
    )
    .await;

    // The repository's identity beats the global one, for the top-level
    // agent and its sub-agent alike, as author and committer.
    let repo_ident = "Repo Person|repo@example.invalid|Repo Person|repo@example.invalid";
    let parent = create_agent(&mut rpc_ws, &ws_id, "Committer", 10).await;
    let top = (ws_id.as_str(), parent.as_str());
    assert_eq!(
        sh_ok(&mut rpc_ws, &mut events, top, COMMIT_AND_SHOW, 100)
            .await
            .trim(),
        repo_ident
    );
    let code = r#"wsapi const c = await ws.agent.create("Helper", "echo-raw", { provider: "mock", model: "default" }); return JSON.stringify({ marker: "made-1", c });"#;
    agent_turn(&mut rpc_ws, &mut events, top, code, "made-1", 200).await;
    let child = child_named(&mut rpc_ws, &ws_id, "Helper", 300).await;
    let sub = (ws_id.as_str(), child.as_str());
    assert_eq!(
        sh_ok(&mut rpc_ws, &mut events, sub, COMMIT_AND_SHOW, 400)
            .await
            .trim(),
        repo_ident
    );
    // Only the environment carries it: the checkout's config has no `user.*`.
    assert_eq!(
        sh_ok(&mut rpc_ws, &mut events, top, SHOW_CONFIG, 500)
            .await
            .trim(),
        ""
    );

    // Resolved at each start: without the repository's identity, the next
    // agent gets the global one.
    git(&repo, &["config", "--unset", "user.name"]);
    git(&repo, &["config", "--unset", "user.email"]);
    let second = create_agent(&mut rpc_ws, &ws_id, "Global", 600).await;
    assert_eq!(
        sh_ok(
            &mut rpc_ws,
            &mut events,
            (&ws_id, &second),
            COMMIT_AND_SHOW,
            700
        )
        .await
        .trim(),
        "Global Person|global@example.invalid|Global Person|global@example.invalid"
    );

    // No identity on the backend: the machine sets nothing, and its own
    // inherited value stays.
    std::fs::remove_file(&global).unwrap();
    let third = create_agent(&mut rpc_ws, &ws_id, "Nobody", 800).await;
    assert_eq!(
        sh_ok(&mut rpc_ws, &mut events, (&ws_id, &third), SHOW_ENV, 900)
            .await
            .trim(),
        "Machine Person|unset|unset|unset"
    );
    drop(machine);
}
