//! Sub-agents fetch the workspace's GitHub repository through the backend
//! (nodeProtocol 17, monorepo `docs/protocol/node-link.md` § Sub-agent fetch
//! through the backend). A sub-agent with its own checkout on another
//! machine, and one that shares its parent's checkout, both `git fetch
//! origin` a commit that reached "GitHub" after they started. Their
//! `git push` to the workspace repository is refused with a clear message,
//! their work still goes back through their refs, and the top-level agent
//! still pushes to GitHub directly. When GitHub cannot be reached, a fetch
//! gets the last refs the backend fetched, with a warning. "GitHub" is a
//! local bare repository the backend's refresh is pointed at.

use super::forge_push::{child_named, sh, sh_ok};
use super::forge_token::{backend_with_repo, backend_with_token_env};
use super::*;

const ORIGIN: &str = "https://github.com/acme/widget.git";

const REFUSED: &str = "sub-agents hand work back through intent1; ask your parent to push";

const WARNING: &str = "could not refresh from GitHub";

/// Commit `subject` on `main` of the GitHub stand-in, through a scratch
/// clone.
#[track_caller]
fn push_to_github(github: &Path, scratch: &Path, subject: &str) {
    let clone = scratch.join(format!("clone-{subject}"));
    let output = Command::new("git")
        .args(["clone", "--quiet"])
        .arg(github)
        .arg(&clone)
        .output()
        .unwrap();
    assert!(output.status.success(), "{output:?}");
    git(
        &clone,
        &["commit", "--quiet", "--allow-empty", "-m", subject],
    );
    git(&clone, &["push", "--quiet", "origin", "HEAD:main"]);
}

/// Poll the subject of `rev` in `repo` until it is `want`.
async fn subject_becomes(repo: &Path, rev: &str, want: &str) {
    let found = timeout(wait(), async {
        loop {
            let output = Command::new("git")
                .arg("-C")
                .arg(repo)
                .args(["log", "-1", "--format=%s", rev])
                .output()
                .unwrap();
            if output.status.success() && String::from_utf8_lossy(&output.stdout).trim() == want {
                return;
            }
            // timing-guard: poll interval for the machine's turn-end push to land
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(found.is_ok(), "{rev} never had subject {want:?}");
}

/// The agents of a forge fetch e2e: a parent on "Near", a child that
/// shares its checkout, and a child with its own checkout on "Far". Each
/// has finished its first turn.
struct Family {
    machines: Vec<Machine>,
    parent: String,
    sharer: String,
    far: String,
}

async fn family(
    rpc_ws: &mut common::TlsWs,
    events: &mut common::TlsWs,
    scratch: &Path,
    ws_id: &str,
    backend_id: &str,
) -> Family {
    ok(
        events,
        2,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;
    let mut machines = Vec::new();
    for (index, name) in ["Near", "Far"].into_iter().enumerate() {
        let dir = scratch.join(name);
        std::fs::create_dir_all(&dir).unwrap();
        let machine = start_machine(&dir, backend_id).await;
        ok(
            rpc_ws,
            3 + i64::try_from(index).unwrap(),
            "remoteMachines.add",
            add_params(&machine, name),
        )
        .await;
        machines.push(machine);
    }
    let (near, far) = (&machines[0], &machines[1]);
    let create = json!({
        "workspaceId": ws_id, "name": "Parent", "model": "default", "provider": "mock",
        "placement": { "target": "remote", "nodeId": near.daemon_id },
    });
    let deadline = tokio::time::Instant::now() + wait();
    let mut id = 10;
    let parent = loop {
        id += 1;
        let response = rpc(rpc_ws, id, "agent.create", create.clone()).await;
        if response.get("error").is_none() {
            break response["result"]["agent"]["id"]
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
    };
    // The first turn carries the forge notice; take it before the wsapi turn.
    ok(
        rpc_ws,
        50,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": parent, "content": "echo-raw" }),
    )
    .await;
    reply_containing(rpc_ws, (ws_id, &parent), "raw: ", 51).await;

    // One child shares the parent's checkout; one has its own on Far.
    let code = format!(
        r#"wsapi const s = await ws.agent.create("Sharer", "echo-raw", {{ provider: "mock", model: "default" }}); const f = await ws.agent.create("Far", "echo-raw", {{ provider: "mock", model: "default", placement: {{ target: "remote", nodeId: {far_id:?} }} }}); return JSON.stringify({{ marker: "made-1", s, f }});"#,
        far_id = far.daemon_id,
    );
    // The children's completions wake the parent, so read this turn's reply
    // from the conversation.
    ok(
        rpc_ws,
        100,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": parent, "content": code }),
    )
    .await;
    reply_containing(rpc_ws, (ws_id, &parent), "made-1", 101).await;
    let sharer = child_named(rpc_ws, ws_id, "Sharer", 200).await;
    let far_child = child_named(rpc_ws, ws_id, "Far", 300).await;
    reply_containing(rpc_ws, (ws_id, &sharer), "raw: ", 400).await;
    reply_containing(rpc_ws, (ws_id, &far_child), "raw: ", 500).await;
    Family {
        machines,
        parent,
        sharer,
        far: far_child,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sub_agents_fetch_github_through_the_backend_and_cannot_push() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping forge fetch e2e: node or git not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-fetch-");
    let forge_root = scratch.path().join("github");
    let github = forge_root.join("acme").join("widget.git");
    std::fs::create_dir_all(&github).unwrap();
    git(
        &github,
        &["init", "--quiet", "--bare", "--initial-branch=main"],
    );
    let base = format!("file://{}/", forge_root.display());
    let (_head, port, cfg, ws_id, backend_id) = backend_with_token_env(
        scratch.path(),
        Some(ORIGIN),
        &[("INTENTD_GITHUB_GIT_BASE_URL", &base)],
    )
    .await;
    let repo = scratch.path().join("repo");
    git(
        &repo,
        &["push", "--quiet", github.to_str().unwrap(), "main"],
    );

    let mut rpc_ws = connect(port, cfg.clone()).await;
    let mut events = connect(port, cfg.clone()).await;
    let family = family(&mut rpc_ws, &mut events, scratch.path(), &ws_id, &backend_id).await;
    let machines = family.machines;
    let parent = (ws_id.as_str(), family.parent.as_str());
    let sharer = (ws_id.as_str(), family.sharer.as_str());
    let far_child = family.far;
    let far_agent = (ws_id.as_str(), far_child.as_str());

    // GitHub moves on after both children started.
    push_to_github(&github, scratch.path(), "after-launch");

    // The child with its own checkout has an `origin` and fetches it.
    let out = sh_ok(
        &mut rpc_ws,
        &mut events,
        far_agent,
        "git config remote.origin.url && git fetch --quiet origin && git log -1 --format=%s origin/main",
        1000,
    )
    .await;
    assert_eq!(
        out.lines().collect::<Vec<_>>(),
        [ORIGIN, "after-launch"],
        "{out}"
    );

    // The child in the parent's checkout fetches through the same origin.
    let out = sh_ok(
        &mut rpc_ws,
        &mut events,
        sharer,
        "git fetch --quiet origin && git log -1 --format=%s origin/main",
        1100,
    )
    .await;
    assert_eq!(out, "after-launch");

    // A sub-agent's push is refused with the message, and nothing moves.
    for (agent, id) in [(far_agent, 1200), (sharer, 1300)] {
        let ran = sh(
            &mut rpc_ws,
            &mut events,
            agent,
            "git -c user.name=E2E -c user.email=e2e@example.invalid commit --quiet --allow-empty -m sub-push && git push origin HEAD:refs/heads/sub-push",
            id,
        )
        .await;
        assert_ne!(ran["code"], 0, "{ran}");
        assert!(ran["out"].as_str().unwrap().contains(REFUSED), "{ran}");
    }
    assert_eq!(git(&github, &["branch", "--list", "sub-push"]), "");

    // The hand-back still works: the child's commit reaches its head ref.
    sh_ok(
        &mut rpc_ws,
        &mut events,
        far_agent,
        "git -c user.name=E2E -c user.email=e2e@example.invalid commit --quiet --allow-empty -m child-work",
        1400,
    )
    .await;
    subject_becomes(
        &repo,
        &format!("refs/intent/ws/{ws_id}/agents/{far_child}/head"),
        "child-work",
    )
    .await;

    // The top-level agent still pushes to GitHub itself.
    let redirect = format!("url.file://{}.insteadOf={ORIGIN}", github.display());
    sh_ok(
        &mut rpc_ws,
        &mut events,
        parent,
        &format!(
            "git -c user.name=E2E -c user.email=e2e@example.invalid commit --quiet --allow-empty -m parent-push && git -c '{redirect}' push --quiet origin HEAD:refs/heads/parent-push"
        ),
        1500,
    )
    .await;
    assert_eq!(
        git(&github, &["log", "-1", "--format=%s", "parent-push"]),
        "parent-push"
    );

    // GitHub is unreachable: the fetch succeeds on the last refs and warns.
    std::fs::rename(&forge_root, scratch.path().join("github-away")).unwrap();
    let deadline = tokio::time::Instant::now() + wait();
    let mut id = 1600;
    let out = loop {
        let out = sh_ok(
            &mut rpc_ws,
            &mut events,
            far_agent,
            "git fetch origin 2>&1 && git log -1 --format=%s origin/main",
            id,
        )
        .await;
        if out.contains(WARNING) {
            break out;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the fetch never warned: {out}"
        );
        id += 10;
        // timing-guard: the backend reuses a refresh for 5 s after it succeeds
        tokio::time::sleep(Duration::from_secs(1)).await;
    };
    assert_eq!(out.lines().last(), Some("after-launch"), "{out}");
    drop(machines);
}

const GADGET: &str = "https://github.com/acme/gadget.git";

/// A workspace whose submodule `gadget` has its own GitHub repository: both
/// sub-agents fetch a commit pushed to it after they started, from inside
/// the submodule; their push there is refused and reaches nothing; the
/// workspace repository's fetch still works.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sub_agents_fetch_a_github_submodule_through_the_backend_and_cannot_push() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping submodule forge fetch e2e: node or git not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-subfetch-");
    let forge_root = scratch.path().join("github");
    let github = forge_root.join("acme").join("widget.git");
    let gadget = forge_root.join("acme").join("gadget.git");
    for bare in [&github, &gadget] {
        std::fs::create_dir_all(bare).unwrap();
        git(bare, &["init", "--quiet", "--bare", "--initial-branch=main"]);
    }
    push_to_github(&gadget, scratch.path(), "gadget-base");
    let base = format!("file://{}/", forge_root.display());
    let (_head, port, cfg, ws_id, backend_id) = backend_with_repo(
        scratch.path(),
        Some(ORIGIN),
        &[("INTENTD_GITHUB_GIT_BASE_URL", &base)],
        |repo| {
            git(
                repo,
                &[
                    "-c",
                    "protocol.file.allow=always",
                    "submodule",
                    "add",
                    "--quiet",
                    gadget.to_str().unwrap(),
                    "gadget",
                ],
            );
            // The submodule's own `origin` names its GitHub repository.
            git(
                repo,
                &["config", "-f", ".gitmodules", "submodule.gadget.url", GADGET],
            );
            git(&repo.join("gadget"), &["remote", "set-url", "origin", GADGET]);
            git(repo, &["add", ".gitmodules"]);
            git(repo, &["commit", "--quiet", "-m", "gadget"]);
        },
    )
    .await;
    let repo = scratch.path().join("repo");
    git(
        &repo,
        &["push", "--quiet", github.to_str().unwrap(), "main"],
    );

    let mut rpc_ws = connect(port, cfg.clone()).await;
    let mut events = connect(port, cfg.clone()).await;
    let family = family(&mut rpc_ws, &mut events, scratch.path(), &ws_id, &backend_id).await;
    let sharer = (ws_id.as_str(), family.sharer.as_str());
    let far_agent = (ws_id.as_str(), family.far.as_str());

    // The submodule's GitHub repository moves on after both started.
    push_to_github(&gadget, scratch.path(), "gadget-after-launch");

    // Each has an `origin` in the submodule and fetches the new commit.
    for (agent, id) in [(far_agent, 1000), (sharer, 1100)] {
        let out = sh_ok(
            &mut rpc_ws,
            &mut events,
            agent,
            "cd gadget && git config remote.origin.url && git fetch --quiet origin && git log -1 --format=%s origin/main",
            id,
        )
        .await;
        assert_eq!(
            out.lines().collect::<Vec<_>>(),
            [GADGET, "gadget-after-launch"],
            "{out}"
        );
    }

    // A push from the submodule is refused with the message, and nothing
    // reaches its GitHub repository.
    for (agent, id) in [(far_agent, 1200), (sharer, 1300)] {
        let ran = sh(
            &mut rpc_ws,
            &mut events,
            agent,
            "cd gadget && git -c user.name=E2E -c user.email=e2e@example.invalid commit --quiet --allow-empty -m sub-push && git push origin HEAD:refs/heads/sub-push",
            id,
        )
        .await;
        assert_ne!(ran["code"], 0, "{ran}");
        assert!(ran["out"].as_str().unwrap().contains(REFUSED), "{ran}");
    }
    assert_eq!(git(&gadget, &["branch", "--list", "sub-push"]), "");

    // The workspace repository's own fetch still works.
    push_to_github(&github, scratch.path(), "after-launch");
    let out = sh_ok(
        &mut rpc_ws,
        &mut events,
        far_agent,
        "git fetch --quiet origin && git log -1 --format=%s origin/main",
        1400,
    )
    .await;
    assert_eq!(out, "after-launch");
    drop(family.machines);
}
