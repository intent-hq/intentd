//! `host.exec` and `host.execStream` on the home (monorepo
//! `docs/protocol/node-link.md`, "Files and git on the home", point 4)
//! against a real orchestrator and a real machine, both with homes enabled:
//! `host.exec` for a workspace whose home is on the machine runs in the
//! home checkout (its `cwd` mapped like `terminal.create`'s), and
//! `host.execStream` is an exec-mode `ptyOpen` there: stdout and stderr
//! stream apart, stdin reaches the process, a cancel ends the process on
//! the machine, and the exit code comes back. A lost home answers
//! `home-unavailable`.

use super::homes::{home_checkout, process_gone, until, HOMES};
use super::*;
use base64::Engine as _;

/// What the `host:exec:*` events of `request_id` carried so far.
#[derive(Default, Debug)]
struct Streamed {
    stdout: String,
    stderr: String,
    exit: Option<Value>,
}

/// Read `host:exec:*` events of `request_id` until `done` holds.
async fn stream_until(
    events: &mut common::TlsWs,
    request_id: &str,
    streamed: &mut Streamed,
    done: impl Fn(&Streamed) -> bool,
) {
    let b64 = base64::engine::general_purpose::STANDARD;
    let reached = timeout(wait(), async {
        while !done(streamed) {
            match events.next().await {
                Some(Ok(Message::Text(text))) => {
                    let v: Value = serde_json::from_str(&text).unwrap();
                    if v["method"] != "events.event" {
                        continue;
                    }
                    let event = &v["params"]["event"];
                    if event["data"]["requestId"] != request_id {
                        continue;
                    }
                    let chunk = || {
                        String::from_utf8_lossy(
                            &b64.decode(event["data"]["chunk"].as_str().unwrap())
                                .unwrap(),
                        )
                        .into_owned()
                    };
                    match event["type"].as_str() {
                        Some("host:exec:stdout") => streamed.stdout.push_str(&chunk()),
                        Some("host:exec:stderr") => streamed.stderr.push_str(&chunk()),
                        Some("host:exec:exit") => streamed.exit = Some(event["data"].clone()),
                        _ => {}
                    }
                }
                Some(Ok(Message::Ping(p))) => {
                    let _ = events.send(Message::Pong(p)).await;
                }
                Some(Ok(_)) => {}
                other => panic!("event socket closed: {other:?}"),
            }
        }
    })
    .await;
    assert!(reached.is_ok(), "the stream never got there: {streamed:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_exec_and_exec_stream_of_a_workspace_with_a_home_run_on_the_home() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping home exec e2e: node or git not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-home-exec-");
    let repo = scratch.path().join("repo");
    std::fs::create_dir_all(repo.join("sub")).unwrap();
    git(&repo, &["init", "--quiet"]);
    std::fs::write(repo.join("README.md"), "base\n").unwrap();
    std::fs::write(repo.join("sub").join("keep.txt"), "sub\n").unwrap();
    git(&repo, &["add", "README.md", "sub/keep.txt"]);
    git(&repo, &["commit", "--quiet", "-m", "base"]);
    let branch = git(&repo, &["rev-parse", "--abbrev-ref", "HEAD"]);

    let head_dir = scratch.path().join("head");
    let (head, _, _, _) = start_backend(&head_dir, &repo).await;
    drop(head);
    super::orchestrator::orchestrator_config(&head_dir);
    let (_head, port, cfg) = serve_backend(&head_dir, &[HOMES]).await;
    let mut rpc_ws = connect(port, cfg.clone()).await;
    let backend = ok(&mut rpc_ws, 1, "remoteMachines.backendInfo", json!({})).await;
    let backend_id = backend["backendId"].as_str().unwrap().to_owned();
    let node_dir = scratch.path().join("node-0");
    std::fs::create_dir_all(&node_dir).unwrap();
    let machine = start_machine_with(&node_dir, &backend_id, &[HOMES]).await;
    ok(
        &mut rpc_ws,
        2,
        "remoteMachines.add",
        add_params(&machine, "Machine A"),
    )
    .await;

    let deadline = tokio::time::Instant::now() + wait();
    let mut id = 10;
    let created = loop {
        id += 1;
        let response = rpc(
            &mut rpc_ws,
            id,
            "workspace.create",
            json!({ "repositoryPath": repo.to_string_lossy(), "baseRef": branch }),
        )
        .await;
        if response.get("error").is_none() {
            break response["result"]["workspace"].clone();
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the machine never became a home: {response}"
        );
        // timing-guard: retry while the new machine finishes its first bind
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let ws_id = created["id"].as_str().unwrap().to_owned();
    let home_id = created["home"]["homeId"].as_str().unwrap().to_owned();
    let mirror = PathBuf::from(created["worktreePath"].as_str().unwrap());
    let base = git(&mirror, &["rev-parse", "HEAD"]);
    let work = until("the home checkout at the base", || {
        home_checkout(&node_dir, &home_id)
            .filter(|work| try_rev(work, "HEAD").as_deref() == Some(base.as_str()))
    })
    .await;
    let real_work = std::fs::canonicalize(&work).unwrap();

    // host.exec runs in the home checkout: its pwd is the home's.
    id += 1;
    let ran = ok(
        &mut rpc_ws,
        id,
        "host.exec",
        json!({ "workspaceId": ws_id, "command": "pwd", "args": ["-P"] }),
    )
    .await;
    assert_eq!(ran["exitCode"], 0, "{ran}");
    assert_eq!(
        ran["stdout"].as_str().unwrap().trim(),
        real_work.to_string_lossy(),
        "{ran}"
    );
    // What it writes lands on the home, not in the mirror.
    id += 1;
    ok(
        &mut rpc_ws,
        id,
        "host.exec",
        json!({ "workspaceId": ws_id, "command": "sh", "args": ["-c", "echo made > exec.txt"] }),
    )
    .await;
    assert_eq!(
        std::fs::read_to_string(work.join("exec.txt")).unwrap(),
        "made\n"
    );
    assert!(!mirror.join("exec.txt").exists());
    // A cwd under the orchestrator's worktree names the same folder there,
    // and a relative one is relative to the home checkout.
    for cwd in [
        mirror.join("sub").to_string_lossy().into_owned(),
        "sub".to_owned(),
    ] {
        id += 1;
        let ran = ok(
            &mut rpc_ws,
            id,
            "host.exec",
            json!({ "workspaceId": ws_id, "command": "pwd", "args": ["-P"], "cwd": cwd }),
        )
        .await;
        assert_eq!(
            ran["stdout"].as_str().unwrap().trim(),
            real_work.join("sub").to_string_lossy(),
            "{cwd}: {ran}"
        );
    }
    // Any other absolute cwd is refused.
    id += 1;
    let refused = rpc(
        &mut rpc_ws,
        id,
        "host.exec",
        json!({ "workspaceId": ws_id, "command": "pwd", "cwd": "/etc" }),
    )
    .await;
    assert_eq!(refused["error"]["code"], -32602, "{refused}");
    // The command's own exit code and stderr come back unchanged.
    id += 1;
    let failed = ok(
        &mut rpc_ws,
        id,
        "host.exec",
        json!({ "workspaceId": ws_id, "command": "sh", "args": ["-c", "echo oops >&2; exit 3"] }),
    )
    .await;
    assert_eq!(failed["exitCode"], 3, "{failed}");
    assert_eq!(failed["stderr"], "oops\n", "{failed}");

    // host.execStream runs on the home too: stdout and stderr stream
    // apart, stdin reaches it, and a cancel ends its process there.
    let mut events = connect(port, cfg.clone()).await;
    id += 1;
    ok(
        &mut events,
        id,
        "events.subscribe",
        json!({ "eventTypes": ["host:exec:*"], "workspaceId": ws_id }),
    )
    .await;
    id += 1;
    let started = ok(
        &mut rpc_ws,
        id,
        "host.execStream",
        json!({
            "workspaceId": ws_id,
            "command": "sh",
            // timing-guard: the process only waits to be cancelled; the test ends it
            "args": ["-c", "echo $$ > stream.pid; echo at:$(pwd -P); echo warn >&2; read line; echo got:$line; exec sleep 60"],
            "requestId": "home-stream-1",
        }),
    )
    .await;
    assert_eq!(started["requestId"], "home-stream-1", "{started}");
    let mut streamed = Streamed::default();
    let at = format!("at:{}\n", real_work.display());
    stream_until(&mut events, "home-stream-1", &mut streamed, |s| {
        s.stdout.contains(&at) && s.stderr.contains("warn\n")
    })
    .await;
    assert!(!streamed.stdout.contains("warn"), "{streamed:?}");
    id += 1;
    ok(
        &mut rpc_ws,
        id,
        "host.execStream.write",
        json!({ "requestId": "home-stream-1", "stdin": "hello\n" }),
    )
    .await;
    stream_until(&mut events, "home-stream-1", &mut streamed, |s| {
        s.stdout.contains("got:hello\n")
    })
    .await;
    // A home process's stdin cannot be closed over the link: refused, and
    // nothing is written.
    id += 1;
    let eof = rpc(
        &mut rpc_ws,
        id,
        "host.execStream.write",
        json!({ "requestId": "home-stream-1", "stdin": "more\n", "eof": true }),
    )
    .await;
    assert_eq!(eof["error"]["code"], -32602, "{eof}");
    assert!(
        eof["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("home-exec-eof-unsupported")),
        "{eof}"
    );
    let pid = std::fs::read_to_string(work.join("stream.pid"))
        .unwrap()
        .trim()
        .to_owned();
    assert!(!process_gone(&pid), "the stream's process ended early");
    id += 1;
    let cancelled = ok(
        &mut rpc_ws,
        id,
        "host.execStream.cancel",
        json!({ "requestId": "home-stream-1" }),
    )
    .await;
    assert_eq!(cancelled["cancelled"], true, "{cancelled}");
    stream_until(&mut events, "home-stream-1", &mut streamed, |s| {
        s.exit.is_some()
    })
    .await;
    let exit = streamed.exit.clone().unwrap();
    assert_eq!(exit["cancelled"], true, "{exit}");
    assert_eq!(exit["ok"], false, "{exit}");
    until("the stream's process ended on the machine", || {
        process_gone(&pid).then_some(())
    })
    .await;
    assert!(!streamed.stdout.contains("more"), "{streamed:?}");

    // A command that ends on its own reports its exit code.
    id += 1;
    ok(
        &mut rpc_ws,
        id,
        "host.execStream",
        json!({
            "workspaceId": ws_id,
            "command": "sh",
            "args": ["-c", "printf 'it'\"'\"'s here'; exit 4"],
            "requestId": "home-stream-2",
        }),
    )
    .await;
    let mut streamed = Streamed::default();
    stream_until(&mut events, "home-stream-2", &mut streamed, |s| {
        s.exit.is_some()
    })
    .await;
    let exit = streamed.exit.clone().unwrap();
    assert_eq!(exit["exitCode"], 4, "{exit}");
    assert_eq!(exit["ok"], false, "{exit}");
    assert_eq!(streamed.stdout, "it's here", "{streamed:?}");

    // A stream past its timeoutMs ends on the machine too.
    id += 1;
    ok(
        &mut rpc_ws,
        id,
        "host.execStream",
        json!({
            "workspaceId": ws_id,
            "command": "sh",
            // timing-guard: the process only waits for its timeoutMs to end it
            "args": ["-c", "echo $$ > timed.pid; exec sleep 60"],
            "requestId": "home-stream-3",
            "timeoutMs": 1500,
        }),
    )
    .await;
    let mut streamed = Streamed::default();
    stream_until(&mut events, "home-stream-3", &mut streamed, |s| {
        s.exit.is_some()
    })
    .await;
    let exit = streamed.exit.clone().unwrap();
    assert_eq!(exit["timedOut"], true, "{exit}");
    let pid = std::fs::read_to_string(work.join("timed.pid"))
        .unwrap()
        .trim()
        .to_owned();
    until("the timed-out process ended on the machine", || {
        process_gone(&pid).then_some(())
    })
    .await;

    // A stream whose machine goes away: each write fails as itself with
    // home-unavailable, and does not close the stream's stdin for later
    // writes.
    id += 1;
    ok(
        &mut rpc_ws,
        id,
        "host.execStream",
        json!({
            "workspaceId": ws_id,
            "command": "sh",
            // timing-guard: the process only waits while its machine goes away
            "args": ["-c", "exec sleep 60"],
            "requestId": "home-stream-4",
        }),
    )
    .await;

    // A home whose machine is gone answers home-unavailable.
    drop(machine);
    let deadline = tokio::time::Instant::now() + wait();
    let refused = loop {
        id += 1;
        let written = rpc(
            &mut rpc_ws,
            id,
            "host.execStream.write",
            json!({ "requestId": "home-stream-4", "stdin": "x\n" }),
        )
        .await;
        if written.get("error").is_some() {
            break written;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "writes kept succeeding with the machine gone: {written}"
        );
        // timing-guard: poll until the orchestrator sees the link close
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    for written in [refused, {
        id += 1;
        rpc(
            &mut rpc_ws,
            id,
            "host.execStream.write",
            json!({ "requestId": "home-stream-4", "stdin": "y\n" }),
        )
        .await
    }] {
        assert!(
            written["error"]["message"]
                .as_str()
                .is_some_and(|m| m.contains("home-unavailable")),
            "{written}"
        );
    }
    id += 1;
    let lost = rpc(
        &mut rpc_ws,
        id,
        "host.exec",
        json!({ "workspaceId": ws_id, "command": "pwd" }),
    )
    .await;
    assert_eq!(lost["error"]["data"]["code"], "home-unavailable", "{lost}");
    id += 1;
    let lost = rpc(
        &mut rpc_ws,
        id,
        "host.execStream",
        json!({ "workspaceId": ws_id, "command": "pwd" }),
    )
    .await;
    assert_eq!(lost["error"]["data"]["code"], "home-unavailable", "{lost}");
}
