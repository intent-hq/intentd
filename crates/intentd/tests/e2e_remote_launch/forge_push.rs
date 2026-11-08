//! A top-level remote agent of a GitHub workspace pushes the workspace
//! branch (nodeProtocol 15, monorepo `docs/protocol/node-link.md` § Forge
//! token on the machine, rules 8 and 9): its checkout is on
//! `intent/<agentId>` with `main` on `origin` as upstream, `git push` lands
//! there, and its git credential helper answers with the machine's live
//! token, also after the backend's token rotates (no respawn). A sub-agent
//! sharing its checkout sees `origin` and has `GH_TOKEN`, but no helper.
//! "GitHub" is a local bare repository that `origin` is redirected to.

use super::forge_token::{backend_with_token, gh_token_digest, sha_hex, TOKEN, TOKEN_ACCOUNT};
use super::secrets::{agent_turn, create_agent, files_containing};
use super::*;

const ORIGIN: &str = "https://github.com/acme/widget.git";

/// The rotated token.
const ROTATED: &str = "ghp_forgeE2eRotated3Kx8Vb5Nm1";

/// The SHA-256 of the password `git credential fill` gets for github.com in
/// the agent's checkout (that of the empty string without a helper). The
/// host's own git configuration is left out, so only the agent's
/// environment can supply a helper.
const CREDENTIAL_DIGEST: &str = "printf 'protocol=https\\nhost=github.com\\n\\n' | env -u XDG_CONFIG_HOME HOME=/nonexistent GIT_CONFIG_NOSYSTEM=1 git credential fill 2>/dev/null | sed -n 's/^password=//p' | tr -d '\\n' | sha256sum | cut -c1-64";

/// Run `command` in the agent's environment; its `{ code, out }`.
async fn sh(
    rpc_ws: &mut common::TlsWs,
    events: &mut common::TlsWs,
    agent: (&str, &str),
    command: &str,
    id: i64,
) -> Value {
    let tag = format!("s{id}");
    let reply = agent_turn(
        rpc_ws,
        events,
        agent,
        &format!("sh {tag} {command}"),
        &format!("sh-out {tag} "),
        id,
    )
    .await;
    let marker = format!("sh-out {tag} ");
    let at = reply.find(&marker).unwrap() + marker.len();
    serde_json::from_str(&reply[at..]).unwrap_or_else(|e| panic!("{e}: {reply}"))
}

/// The output of a command that must succeed.
async fn sh_ok(
    rpc_ws: &mut common::TlsWs,
    events: &mut common::TlsWs,
    agent: (&str, &str),
    command: &str,
    id: i64,
) -> String {
    let ran = sh(rpc_ws, events, agent, command, id).await;
    assert_eq!(ran["code"], 0, "{command}: {ran}");
    ran["out"].as_str().unwrap().to_owned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_top_level_remote_agent_pushes_the_workspace_branch_and_its_sub_agent_has_no_helper() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
        || intent_providers::resolve_on_path("sha256sum").is_none()
    {
        eprintln!("skipping forge push e2e: node, git or sha256sum not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-push-");
    let github = scratch.path().join("github.git");
    git(scratch.path(), &["init", "--quiet", "--bare", "github.git"]);
    let head_dir = scratch.path().join("head");
    let (_head, port, cfg, ws_id, backend_id) =
        backend_with_token(scratch.path(), Some("git@github.com:acme/widget.git")).await;
    let mut rpc_ws = connect(port, cfg.clone()).await;
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
    // The machine's own git configuration has a `store` helper (and none
    // from the test's environment): the agents' git must never hand it the
    // backend's token.
    let store = format!(
        "'credential.helper=store --file={}'",
        node_dir.join("git-credentials").display()
    );
    let machine =
        start_machine_with(&node_dir, &backend_id, &[("GIT_CONFIG_PARAMETERS", &store)]).await;
    ok(
        &mut rpc_ws,
        3,
        "remoteMachines.add",
        add_params(&machine, "E2E node"),
    )
    .await;
    let parent = create_agent(&mut rpc_ws, &ws_id, "Pusher", 10).await;
    let pusher = (ws_id.as_str(), parent.as_str());

    // The first turn carries the notice: the branch and how to open a PR.
    // Read from the conversation: the agent's last-response preview is
    // capped below the raw prompt's length.
    ok(
        &mut rpc_ws,
        100,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": parent, "content": "echo-raw" }),
    )
    .await;
    let raw = reply_containing(&mut rpc_ws, pusher, "raw: ", 101).await;
    let local = format!("intent/{parent}");
    assert!(raw.contains(&format!("`{local}`")), "{raw}");
    assert!(raw.contains("gh pr create --head main"), "{raw}");

    // On its own branch, with `main` on origin as upstream (not fetched
    // yet, so only the configuration names it).
    let out = sh_ok(
        &mut rpc_ws,
        &mut events,
        pusher,
        "b=$(git rev-parse --abbrev-ref HEAD) && echo $b && git config branch.$b.remote && git config branch.$b.merge && git config remote.origin.url && git config push.default",
        200,
    )
    .await;
    assert_eq!(
        out.lines().collect::<Vec<_>>(),
        [
            local.as_str(),
            "origin",
            "refs/heads/main",
            ORIGIN,
            "upstream"
        ],
        "{out}"
    );

    // A plain `git push` lands on `main` at "GitHub".
    let redirect = format!("url.file://{}.insteadOf={ORIGIN}", github.display());
    sh_ok(
        &mut rpc_ws,
        &mut events,
        pusher,
        &format!(
            "git -c user.name=E2E -c user.email=e2e@example.invalid commit --quiet --allow-empty -m pushed-by-agent && git -c '{redirect}' push --quiet"
        ),
        300,
    )
    .await;
    assert_eq!(
        git(&github, &["log", "-1", "--format=%s", "main"]).trim(),
        "pushed-by-agent"
    );

    // Its credential helper answers with the machine's token.
    let digest = sh_ok(&mut rpc_ws, &mut events, pusher, CREDENTIAL_DIGEST, 400).await;
    assert_eq!(digest, sha_hex(TOKEN));
    // As after a successful HTTPS push: git offers the credential to
    // every helper it consults. Only Intent's (which stores nothing) is.
    sh_ok(
        &mut rpc_ws,
        &mut events,
        pusher,
        "printf 'protocol=https\\nhost=github.com\\n\\n' | git credential fill | git credential approve",
        450,
    )
    .await;
    assert!(
        !node_dir.join("git-credentials").exists(),
        "the machine's store helper got the token"
    );

    // A sub-agent in the same checkout: origin and GH_TOKEN, no helper,
    // and its first turn tells it to read only.
    let code = r#"wsapi const c = await ws.agent.create("Reader", "echo-raw", { provider: "mock", model: "default" }); return JSON.stringify({ marker: "made-1", c });"#;
    agent_turn(&mut rpc_ws, &mut events, pusher, code, "made-1", 500).await;
    let child = child_named(&mut rpc_ws, &ws_id, "Reader", 600).await;
    let reader = (ws_id.as_str(), child.as_str());
    let raw = reply_containing(&mut rpc_ws, reader, "raw: ", 700).await;
    assert!(raw.contains("Use it only to read"), "{raw}");
    assert!(!raw.contains("gh pr create --head"), "{raw}");
    let out = sh_ok(
        &mut rpc_ws,
        &mut events,
        reader,
        &format!("git config remote.origin.url && {CREDENTIAL_DIGEST}"),
        800,
    )
    .await;
    assert_eq!(
        out.lines().collect::<Vec<_>>(),
        [ORIGIN, sha_hex("").as_str()],
        "the sub-agent has no credential helper: {out}"
    );
    assert_eq!(
        gh_token_digest(&mut rpc_ws, &mut events, reader, 900).await,
        sha_hex(TOKEN),
        "the sub-agent has GH_TOKEN"
    );

    // The backend's token rotates: the running agent's helper gets the new
    // value without a respawn, while its GH_TOKEN stays the one it started
    // with.
    intent_core::FileSecretStore::with_path(head_dir.join("secrets.json"))
        .store(TOKEN_ACCOUNT, ROTATED)
        .unwrap();
    ok(
        &mut rpc_ws,
        1000,
        "settings.update",
        json!({ "changes": [{ "path": "sourceControl.github.tokenSource", "value": "explicit" }] }),
    )
    .await;
    let deadline = tokio::time::Instant::now() + wait();
    let mut id = 1100;
    loop {
        let digest = sh_ok(&mut rpc_ws, &mut events, pusher, CREDENTIAL_DIGEST, id).await;
        if digest == sha_hex(ROTATED) {
            break;
        }
        assert_eq!(digest, sha_hex(TOKEN));
        assert!(
            tokio::time::Instant::now() < deadline,
            "the helper never got the rotated token"
        );
        id += 10;
    }
    assert_eq!(
        gh_token_digest(&mut rpc_ws, &mut events, pusher, 2000).await,
        sha_hex(TOKEN),
        "no respawn: GH_TOKEN is the token it started with"
    );

    // Neither value reached a file on the machine.
    for token in [TOKEN, ROTATED] {
        assert_eq!(
            files_containing(&node_dir, token),
            Vec::<std::path::PathBuf>::new()
        );
    }
    drop(machine);
}

/// The id of the workspace's agent named `name`.
async fn child_named(rpc_ws: &mut common::TlsWs, ws_id: &str, name: &str, id: i64) -> String {
    let deadline = tokio::time::Instant::now() + wait();
    let mut id = id;
    loop {
        id += 1;
        let listed = ok(rpc_ws, id, "agent.list", json!({ "workspaceId": ws_id })).await;
        let found = listed["agents"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|agent| agent["name"] == name)
            .and_then(|agent| agent["id"].as_str())
            .map(str::to_owned);
        if let Some(found) = found {
            return found;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "no agent named {name}: {listed}"
        );
        // timing-guard: poll interval for the forwarded create to be listed
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
