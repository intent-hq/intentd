//! Agents pull workspace files from each other (`ws.file.pull`, monorepo
//! `docs/protocol/node-link.md` § Checkout files) over two real daemons: a
//! remote agent pulls a file and a folder another remote agent staged in its
//! checkout on the machine (relayed by head with `checkoutRead`), and a
//! remote agent pulls from a local agent's checkout on head. A local caller
//! of a remote source is covered at the backend (`intent-svc-remote`
//! `a_local_agent_pulls_from_a_remote_agents_machine`): the mock provider
//! gets no workspace MCP server when it runs locally.
//! Paths outside the source or destination checkout, agents of another
//! workspace, unknown agents and files over the 200 MB cap are refused with
//! their `file-pull-*` codes, and nothing lands outside the checkouts. A
//! machine on an older link is covered at the backend
//! (`head_serves_pull_pieces_from_local_and_remote_checkouts`).

use super::secrets::{agent_turn, create_agent};
use super::*;

/// The JS that awaits `call` and returns `"ok"` or its error text.
const CATCH: &str =
    "const e = async (p) => { try { await p; return 'ok'; } catch (x) { return String(x); } };";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn agents_pull_files_between_local_and_remote_checkouts() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping file pull e2e: node or git not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-pull-");
    let repo = scratch.path().join("repo");
    let other_repo = scratch.path().join("other");
    for dir in [&repo, &other_repo] {
        std::fs::create_dir_all(dir).unwrap();
        git(dir, &["init", "--quiet"]);
        std::fs::write(dir.join("README.md"), "base\n").unwrap();
        git(dir, &["add", "README.md"]);
        git(dir, &["commit", "--quiet", "-m", "base"]);
    }
    let fixture = format!(
        "{}/tests/fixtures/node-test-agent.mjs",
        env!("CARGO_MANIFEST_DIR")
    );
    let head_dir = scratch.path().join("head");
    std::fs::create_dir_all(head_dir.join("workspaces")).unwrap();
    let ws_id = seed_workspace(&head_dir, &repo).await;
    let other_ws = seed_workspace(&head_dir, &other_repo).await;
    let (_head, port, cfg) =
        serve_backend(&head_dir, &[("MOCK_AGENT_SCRIPT_PATH", fixture.as_str())]).await;
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
    let local = ok(
        &mut rpc_ws,
        4,
        "agent.create",
        json!({ "workspaceId": ws_id, "name": "Local", "model": "default", "provider": "mock" }),
    )
    .await["agent"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let stranger = ok(
        &mut rpc_ws,
        5,
        "agent.create",
        json!({ "workspaceId": other_ws, "name": "Stranger", "model": "default", "provider": "mock" }),
    )
    .await["agent"]["id"]
        .as_str()
        .unwrap()
        .to_owned();
    let remote_a = create_agent(&mut rpc_ws, &ws_id, "Remote A", 10).await;
    let remote_b = create_agent(&mut rpc_ws, &ws_id, "Remote B", 40).await;

    // Remote A stages a file and a folder in its checkout on the machine.
    agent_turn(
        &mut rpc_ws,
        &mut events,
        (&ws_id, &remote_a),
        r#"wsapi await ws.file.write("out/report.txt", "from A"); await ws.file.write("out/bundle/one.txt", "1"); await ws.file.write("out/bundle/deep/two.txt", "2"); return JSON.stringify({ marker: "turn-100" });"#,
        "turn-100",
        100,
    )
    .await;

    // Remote B pulls both from Remote A: remote to remote, relayed by head,
    // which writes nothing.
    let reply = agent_turn(
        &mut rpc_ws,
        &mut events,
        (&ws_id, &remote_b),
        &format!(
            r#"wsapi const f = await ws.file.pull({{ fromAgentId: "{remote_a}", path: "out/report.txt", to: "pulled/report.txt" }}); const d = await ws.file.pull({{ fromAgentId: "{remote_a}", path: "out/bundle", to: "got" }}); return JSON.stringify({{ marker: "turn-200", f, d, report: await ws.file.read("pulled/report.txt"), one: await ws.file.read("got/one.txt"), two: await ws.file.read("got/deep/two.txt") }});"#
        ),
        "turn-200",
        200,
    )
    .await;
    let pulled = wsapi_value(&reply);
    assert_eq!(pulled["f"]["path"], "pulled/report.txt", "{pulled}");
    assert_eq!(pulled["f"]["kind"], "file", "{pulled}");
    assert_eq!(pulled["f"]["size"], 6, "{pulled}");
    assert_eq!(
        pulled["f"]["sha256"],
        lower_hex(&Sha256::digest(b"from A")),
        "{pulled}"
    );
    assert_eq!(pulled["d"]["path"], "got", "{pulled}");
    assert_eq!(pulled["d"]["kind"], "directory", "{pulled}");
    assert_eq!(pulled["d"]["files"], 2, "{pulled}");
    assert_eq!(pulled["d"]["skipped"], 0, "{pulled}");
    assert_eq!(pulled["report"], "from A", "{pulled}");
    assert_eq!(pulled["one"], "1", "{pulled}");
    assert_eq!(pulled["two"], "2", "{pulled}");
    assert!(
        !repo.join("got").exists() && !repo.join("pulled").exists(),
        "a remote destination is written on the machine, never on head"
    );

    // Remote A pulls from the local agent's checkout; the refusals name
    // their codes.
    let outside = scratch.path().join("outside.txt");
    std::fs::write(&outside, "outside").unwrap();
    std::fs::create_dir_all(repo.join("notes")).unwrap();
    std::fs::write(repo.join("notes/local.txt"), "from local").unwrap();
    std::os::unix::fs::symlink(&outside, repo.join("leak.txt")).unwrap();
    std::fs::File::create(repo.join("big.bin"))
        .unwrap()
        .set_len(200 * 1024 * 1024 + 1)
        .unwrap();
    let reply = agent_turn(
        &mut rpc_ws,
        &mut events,
        (&ws_id, &remote_a),
        &format!(
            r#"wsapi {CATCH} const p = (path, extra) => ws.file.pull(Object.assign({{ fromAgentId: "{local}", path }}, extra || {{}})); const got = await p("notes/local.txt"); return JSON.stringify({{ marker: "turn-300", got, text: await ws.file.read("notes/local.txt"), up: await e(p("../outside.txt")), abs: await e(p("{outside}")), leak: await e(p("leak.txt")), git: await e(p(".git/config")), big: await e(p("big.bin")), escape: await e(p("notes/local.txt", {{ to: "../escaped.txt" }})), stranger: await e(ws.file.pull({{ fromAgentId: "{stranger}", path: "README.md" }})), unknown: await e(ws.file.pull({{ fromAgentId: "agent-nope", path: "README.md" }})) }});"#,
            outside = outside.display(),
        ),
        "turn-300",
        300,
    )
    .await;
    let from_local = wsapi_value(&reply);
    assert_eq!(from_local["got"]["path"], "notes/local.txt", "{from_local}");
    assert_eq!(from_local["got"]["size"], 10, "{from_local}");
    assert_eq!(from_local["got"]["files"], 1, "{from_local}");
    assert_eq!(from_local["text"], "from local", "{from_local}");
    for (key, code) in [
        ("up", "file-pull-outside-checkout"),
        ("abs", "file-pull-outside-checkout"),
        ("leak", "file-pull-outside-checkout"),
        ("git", "file-pull-outside-checkout"),
        ("big", "file-pull-too-large"),
        ("escape", "file-pull-outside-checkout"),
        ("stranger", "file-pull-unknown-agent"),
        ("unknown", "file-pull-unknown-agent"),
    ] {
        let error = from_local[key].as_str().unwrap_or_default();
        assert!(error.contains(code), "{key}: {from_local}");
    }
    assert!(
        !node_dir.join("escaped.txt").exists() && !scratch.path().join("escaped.txt").exists(),
        "nothing lands outside the checkout"
    );
}
