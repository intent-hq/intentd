//! A remote agent opens a PR from a branch it made in a submodule of its
//! checkout (intent-hq/intent: a runner agent's PR missing from the sidebar).
//! The backend's copy of the submodule stays detached at its pin and never
//! sees the agent's branch, so the submodule's git root learns the PR from
//! the agent's own `gh pr create`: its `pullRequests` list the PR, also after
//! the agent resets the submodule to its pin.
//! "GitHub" is a local bare repository for the push, a fake `gh` the agent
//! runs by its path, and a stub REST API on loopback for the backend.

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use super::forge_token::backend_with_repo_seeded;
use super::secrets::{agent_turn, create_agent};
use super::*;

const PR_URL: &str = "https://github.com/acme/lib/pull/7";

/// How long the root may take to list the PR after `gh pr create`: the link
/// runs right after the tool call.
// fixed-deadline: allow — stays under nextest's unscaled 180 s slow-timeout kill (.config/nextest.toml) so a stall panics naming its step
const LINK_BUDGET: Duration = Duration::from_secs(30);

const BRANCH: &str = "fix/remote-pr";

/// A GitHub REST API that knows one open PR, `acme/lib#7`.
struct StubGithub {
    port: u16,
}

fn pull_json() -> Value {
    json!({
        "number": 7,
        "html_url": PR_URL,
        "title": "Fix the remote PR",
        "body": "",
        "state": "open",
        "draft": false,
        "merged": false,
        "user": { "login": "agent" },
        "head": { "ref": BRANCH, "sha": "1111111111111111111111111111111111111111" },
        "base": { "ref": "main", "sha": "2222222222222222222222222222222222222222" },
        "created_at": "2026-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
    })
}

impl StubGithub {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut head = Vec::new();
                    let mut byte = [0u8; 1];
                    while !head.ends_with(b"\r\n\r\n") {
                        match stream.read(&mut byte).await {
                            Ok(1) => head.push(byte[0]),
                            _ => return,
                        }
                    }
                    let head = String::from_utf8_lossy(&head).into_owned();
                    let target = head.split_whitespace().nth(1).unwrap_or("/").to_owned();
                    let path = target.split('?').next().unwrap_or("/").to_owned();
                    let (status, body) = match path.trim_end_matches('/') {
                        "/repos/acme/lib/pulls/7" => ("200 OK", pull_json()),
                        "/repos/acme/lib/pulls" => ("200 OK", json!([pull_json()])),
                        _ => ("404 Not Found", json!({ "message": "Not Found" })),
                    };
                    let body = body.to_string();
                    let reply = format!(
                        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = stream.write_all(reply.as_bytes()).await;
                });
            }
        });
        Self { port }
    }
}

/// Run `command` as a `Bash` tool call of the agent; its output.
async fn bash_ok(
    rpc_ws: &mut common::TlsWs,
    events: &mut common::TlsWs,
    agent: (&str, &str),
    command: &str,
    id: i64,
) -> String {
    let tag = format!("b{id}");
    let reply = agent_turn(
        rpc_ws,
        events,
        agent,
        &format!("bash {tag} {command}"),
        &format!("sh-out {tag} "),
        id,
    )
    .await;
    let marker = format!("sh-out {tag} ");
    let at = reply.find(&marker).unwrap() + marker.len();
    let ran: Value = serde_json::from_str(&reply[at..]).unwrap_or_else(|e| panic!("{e}: {reply}"));
    assert_eq!(ran["code"], 0, "{command}: {ran}");
    ran["out"].as_str().unwrap().to_owned()
}

/// The PR numbers in the `lib` root's `pullRequests`.
async fn lib_root_prs(rpc_ws: &mut common::TlsWs, ws_id: &str, id: i64) -> Vec<u64> {
    let listed = ok(rpc_ws, id, "gitRoot.list", json!({ "workspaceId": ws_id })).await;
    listed["gitRoots"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|root| root["repoName"] == "lib")
        .flat_map(|root| root["pullRequests"].as_array().cloned().unwrap_or_default())
        .filter_map(|pr| pr["number"].as_u64())
        .collect()
}

async fn until_listed(rpc_ws: &mut common::TlsWs, ws_id: &str, id: &mut i64, what: &str) {
    let deadline = tokio::time::Instant::now() + LINK_BUDGET;
    loop {
        *id += 1;
        if lib_root_prs(rpc_ws, ws_id, *id).await.contains(&7) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{what}: the lib root never listed {PR_URL}"
        );
        // timing-guard: poll interval; the root's PR list has no wire wait
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pr_a_remote_agent_opens_from_a_submodule_branch_is_listed_on_its_root() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping created PR e2e: node or git not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-created-pr-");
    let upstream = scratch.path().join("upstream-lib");
    std::fs::create_dir_all(&upstream).unwrap();
    git(&upstream, &["init", "--quiet", "--initial-branch=main"]);
    std::fs::write(upstream.join("lib.txt"), "lib\n").unwrap();
    git(&upstream, &["add", "lib.txt"]);
    git(&upstream, &["commit", "--quiet", "-m", "lib"]);
    let github = scratch.path().join("github-lib.git");
    git(
        scratch.path(),
        &[
            "clone",
            "--quiet",
            "--bare",
            upstream.to_str().unwrap(),
            github.to_str().unwrap(),
        ],
    );

    let stub = StubGithub::start().await;
    let api = format!("http://127.0.0.1:{}", stub.port);
    let repo = scratch.path().join("repo");
    // The submodule's root, as the submodule sweep records it, seeded before
    // the backend starts: a second writer racing the live daemon fails at
    // random with "database is locked".
    let seed_root = async |ws_id: &str| {
        let lib = std::fs::canonicalize(repo.join("lib")).unwrap();
        let pin = git(&lib, &["rev-parse", "HEAD"]);
        let store = intent_store::Store::open(&scratch.path().join("head").join("intentd.db"))
            .await
            .unwrap();
        let now = intent_core::now_iso();
        store
            .upsert_workspace_git_root(&intent_core::WorkspaceGitRoot {
                id: intent_core::WorkspaceGitRootId::new(),
                workspace_id: intent_core::WorkspaceId::from(ws_id),
                path: lib.to_string_lossy().into_owned(),
                source: intent_core::WorkspaceGitRootSource::Auto,
                repo_owner: Some("acme".into()),
                repo_name: Some("lib".into()),
                registered_by_agent_ids: vec![],
                registered_commit_sha: Some(pin),
                pr_number: None,
                pr_url: None,
                pr_status: None,
                pull_requests: None,
                created_at: now.clone(),
                updated_at: now,
            })
            .await
            .unwrap();
        store.close().await;
    };
    let (_head, port, cfg, ws_id, backend_id) = backend_with_repo_seeded(
        scratch.path(),
        None,
        &[("INTENTD_GITHUB_API_BASE_URI", &api)],
        |repo| {
            git(
                repo,
                &[
                    "-c",
                    "protocol.file.allow=always",
                    "submodule",
                    "add",
                    "--quiet",
                    upstream.to_str().unwrap(),
                    "lib",
                ],
            );
            git(repo, &["commit", "--quiet", "-m", "base with lib"]);
            git(
                &repo.join("lib"),
                &[
                    "remote",
                    "set-url",
                    "origin",
                    "https://github.com/acme/lib.git",
                ],
            );
        },
        seed_root,
    )
    .await;
    let lib = std::fs::canonicalize(repo.join("lib")).unwrap();
    let pin = git(&lib, &["rev-parse", "HEAD"]);

    let mut rpc_ws = connect(port, cfg.clone()).await;
    let mut events = connect(port, cfg.clone()).await;
    ok(
        &mut events,
        2,
        "events.subscribe",
        json!({ "eventTypes": ["agent:*"], "workspaceId": ws_id }),
    )
    .await;

    // A `gh` that prints the URL of the PR it "opened".
    let bin = scratch.path().join("bin");
    std::fs::create_dir_all(&bin).unwrap();
    let gh = bin.join("gh");
    std::fs::write(
        &gh,
        format!("#!/bin/sh\n[ \"$1 $2\" = \"pr create\" ] || exit 1\necho {PR_URL}\n"),
    )
    .unwrap();
    {
        use std::os::unix::fs::PermissionsExt as _;
        std::fs::set_permissions(&gh, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let node_dir = scratch.path().join("node");
    std::fs::create_dir_all(&node_dir).unwrap();
    let machine = start_machine_with(&node_dir, &backend_id, &[]).await;
    ok(
        &mut rpc_ws,
        3,
        "remoteMachines.add",
        add_params(&machine, "E2E node"),
    )
    .await;
    let agent_id = create_agent(&mut rpc_ws, &ws_id, "Opener", 10).await;
    let agent = (ws_id.as_str(), agent_id.as_str());

    // Nothing links the root before the agent opens its PR.
    assert!(lib_root_prs(&mut rpc_ws, &ws_id, 90).await.is_empty());

    let ident = "-c user.name=Agent -c user.email=agent@example.invalid";
    bash_ok(
        &mut rpc_ws,
        &mut events,
        agent,
        &format!(
            "cd lib && git checkout --quiet -b {BRANCH} && echo fix > fix.txt && git add fix.txt && git {ident} commit --quiet -m fix && git remote add github {} && git push --quiet github {BRANCH}",
            github.display()
        ),
        100,
    )
    .await;
    assert_eq!(
        git(&github, &["rev-parse", "--verify", "--quiet", BRANCH]).len(),
        40
    );
    let created = bash_ok(
        &mut rpc_ws,
        &mut events,
        agent,
        &format!(
            "cd lib && {} pr create --head {BRANCH} --title fix --body fix",
            gh.display()
        ),
        200,
    )
    .await;
    assert_eq!(created, PR_URL);

    let mut id = 300;
    until_listed(&mut rpc_ws, &ws_id, &mut id, "after gh pr create").await;
    // The backend's submodule never left its pin.
    assert_eq!(git(&lib, &["rev-parse", "HEAD"]), pin);

    // The agent resets its submodule to the pin, as the agent in the report
    // did. The listing never depended on the agent's branch, so it stays;
    // that later sweeps keep pool entries is covered by the intent-services
    // git-root refresh tests.
    bash_ok(
        &mut rpc_ws,
        &mut events,
        agent,
        &format!("cd lib && git checkout --quiet --detach {pin}"),
        400,
    )
    .await;
    id += 1;
    assert!(
        lib_root_prs(&mut rpc_ws, &ws_id, id).await.contains(&7),
        "the PR left the lib root after the agent reset its submodule"
    );
}
