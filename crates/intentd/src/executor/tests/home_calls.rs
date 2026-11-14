//! Files and git on the home (nodeProtocol 19) over a real link: a
//! backend's `homeCall` runs the wire method against the home checkout
//! through the machine's hidden record of it, with real services. The home
//! checkout is a test checkout made a git repository here.

use intent_core::node_link::{
    HomeCall, HomeCallCancel, HomeCallOutcome, HomeCallReply, LinkId, HOME_REPLY_TOO_LARGE,
};
use serde_json::{json, Value};

use super::*;

/// A machine whose home calls run through real services on their own
/// store.
struct Machine {
    node: Running,
    services: intent_services::Services,
    _store: tempfile::TempDir,
}

async fn machine(dir: &std::path::Path) -> Machine {
    let store_dir = tempfile::tempdir().unwrap();
    let store = intent_store::Store::open(&store_dir.path().join("machine.db"))
        .await
        .unwrap();
    let services = intent_services::Services::new(store);
    let records: Arc<dyn runner::HomeRecords> =
        Arc::new(crate::executor::ServicesHomeRecords(services.clone()));
    let node = Running::start_with_records(
        dir,
        MachineApi::new(),
        CountAdmission::new(4),
        Arc::new(HomesLaunch(true)),
        |storage, _cache| checkout::PlainCheckouts::new(storage),
        Some(records),
    )
    .await;
    Machine {
        node,
        services,
        _store: store_dir,
    }
}

fn git(worktree: &std::path::Path, args: &[&str]) -> String {
    let output = std::process::Command::new("git")
        .arg("-C")
        .arg(worktree)
        .args([
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@example.com",
            "-c",
            "commit.gpgsign=false",
        ])
        .args(args)
        .output()
        .unwrap();
    assert!(output.status.success(), "git {args:?}: {output:?}");
    String::from_utf8_lossy(&output.stdout).into_owned()
}

/// Dial, open `h1` and wait for its setup and first backup; the checkout
/// is a git repository with one commit.
async fn opened(m: &Machine, dir: &std::path::Path) -> (DialedLink, HomeRef, std::path::PathBuf) {
    let h1 = home("h1");
    let mut dialed = dial_link(&m.node.endpoint, leasing(1, Some(Vec::new())))
        .await
        .unwrap();
    replay_complete(&mut dialed).await;
    dialed.link.send(&open(&h1)).await.unwrap();
    let reply = next_of(&mut dialed, |frame| match frame {
        LinkFrame::HomeOpenReply(reply) => Some(reply),
        _ => None,
    })
    .await;
    assert_eq!(reply.outcome, HomeOpenOutcome::Ready);
    next_pushed(&mut dialed, &h1).await;
    let work = worktree(dir, &h1);
    git(&work, &["init", "-q", "-b", "main"]);
    std::fs::write(work.join("README.md"), "hello\n").unwrap();
    git(&work, &["add", "-A"]);
    git(&work, &["commit", "-q", "-m", "init"]);
    (dialed, h1, work)
}

fn call_frame(home: &HomeRef, method: &str, params: Value, timeout_ms: u64) -> HomeCall {
    HomeCall {
        request_id: LinkId::new_v4(),
        workspace_id: home.workspace_id.clone(),
        home_id: home.home_id.clone(),
        method: method.into(),
        params: match params {
            Value::Object(params) => params,
            other => panic!("params must be an object: {other}"),
        },
        timeout_ms,
    }
}

/// Send one home call and wait for its reply.
async fn call(
    dialed: &mut DialedLink,
    home: &HomeRef,
    method: &str,
    params: Value,
) -> HomeCallReply {
    let frame = call_frame(home, method, params, 30_000);
    let id = frame.request_id;
    dialed.link.send(&LinkFrame::HomeCall(frame)).await.unwrap();
    next_of(dialed, |frame| match frame {
        LinkFrame::HomeCallReply(reply) if reply.request_id == id => Some(reply),
        _ => None,
    })
    .await
}

fn ok(reply: HomeCallReply) -> Value {
    assert_eq!(reply.outcome, HomeCallOutcome::Ok, "{:?}", reply.error);
    reply.result.unwrap()
}

fn err(reply: &HomeCallReply) -> (i64, Option<Value>) {
    assert_eq!(reply.outcome, HomeCallOutcome::Err);
    let error = reply.error.clone().unwrap();
    (error.code, error.data)
}

/// How many times the test checkouts pushed `home`.
fn pushes(dir: &std::path::Path, home: &HomeRef) -> usize {
    let file = worktree(dir, home).with_file_name(checkout::TEST_HOME_PUSHED);
    std::fs::read_to_string(file).unwrap_or_default().len()
}

/// Files and git calls answer as the wire methods do, with paths relative
/// to the checkout and never its absolute path; a write pushes `home/wip`
/// soon after; the hidden record is never listed.
#[tokio::test]
async fn home_files_and_git_answer_as_the_wire_methods() {
    let dir = tempfile::tempdir().unwrap();
    let m = machine(dir.path()).await;
    let (mut dialed, h1, work) = opened(&m, dir.path()).await;
    let shown = |value: &Value| {
        let text = value.to_string();
        let root = work.to_string_lossy().into_owned();
        assert!(!text.contains(&root), "{text}");
        text
    };

    let before = pushes(dir.path(), &h1);
    ok(call(
        &mut dialed,
        &h1,
        "file.write",
        json!({ "workspaceId": "ws-1", "path": "notes.txt", "content": "from the app" }),
    )
    .await);
    assert_eq!(
        std::fs::read_to_string(work.join("notes.txt")).unwrap(),
        "from the app"
    );
    until("the write's home/wip push", || {
        pushes(dir.path(), &h1) > before
    })
    .await;

    let read = ok(call(
        &mut dialed,
        &h1,
        "file.read",
        json!({ "workspaceId": "ws-1", "path": "notes.txt" }),
    )
    .await);
    assert!(shown(&read).contains("from the app"), "{read}");

    // Contents are never rewritten, even when they name the checkout.
    std::fs::write(
        work.join("paths.txt"),
        format!("built in {}/out", work.display()),
    )
    .unwrap();
    let raw = ok(call(
        &mut dialed,
        &h1,
        "file.read",
        json!({ "path": "paths.txt" }),
    )
    .await);
    assert!(
        raw.to_string().contains(&format!("{}/out", work.display())),
        "{raw}"
    );

    let listed = ok(call(
        &mut dialed,
        &h1,
        "file.list",
        json!({ "workspaceId": "ws-1", "path": "" }),
    )
    .await);
    let text = shown(&listed);
    assert!(
        text.contains("notes.txt") && text.contains("README.md"),
        "{text}"
    );

    let status = ok(call(
        &mut dialed,
        &h1,
        "git.status",
        json!({ "workspaceId": "ws-1", "forceRefresh": true }),
    )
    .await);
    assert!(shown(&status).contains("notes.txt"), "{status}");

    // The record is the machine's own: never listed, and named by nothing
    // the backend sent.
    let records = m.services.store().home_record_ids().await.unwrap();
    assert_eq!(records.len(), 1);
    let listed = m
        .services
        .store()
        .list_workspaces(intent_core::WorkspaceListFilter {
            include_archived: true,
            include_retired: true,
        })
        .await
        .unwrap();
    assert!(listed.is_empty(), "{listed:?}");
}

/// `git.agentCommit` pushes `home/head` before it answers; when that push
/// fails the commit still succeeds, with `backupPending: true`.
#[tokio::test]
async fn a_home_commit_pushes_its_head_or_reports_the_backup_pending() {
    let dir = tempfile::tempdir().unwrap();
    let m = machine(dir.path()).await;
    let (mut dialed, h1, work) = opened(&m, dir.path()).await;

    std::fs::write(work.join("a.txt"), "a").unwrap();
    let before = pushes(dir.path(), &h1);
    let committed = ok(call(
        &mut dialed,
        &h1,
        "git.agentCommit",
        json!({ "workspaceId": "ws-1", "message": "Add a", "files": ["a.txt"] }),
    )
    .await);
    assert!(committed["hash"].is_string(), "{committed}");
    assert_eq!(committed.get("backupPending"), None, "{committed}");
    assert!(
        pushes(dir.path(), &h1) > before,
        "home/head was pushed before the reply"
    );

    std::fs::write(work.with_file_name(checkout::TEST_HOME_PUSH_FAILS), "").unwrap();
    std::fs::write(work.join("b.txt"), "b").unwrap();
    let pending = ok(call(
        &mut dialed,
        &h1,
        "git.agentCommit",
        json!({ "workspaceId": "ws-1", "message": "Add b", "files": ["b.txt"] }),
    )
    .await);
    assert_eq!(pending["backupPending"], true, "{pending}");
    let log = git(&work, &["log", "--format=%s"]);
    assert!(log.starts_with("Add b\nAdd a\n"), "{log}");
}

/// Paths leaving the checkout or naming `.git` are `-32602`; a method off
/// the list is `-32601`; a home the machine does not hold is refused; a
/// result over the frame limit is `-32603` `home-reply-too-large`.
#[tokio::test]
async fn home_calls_refuse_escapes_unknown_methods_and_oversized_replies() {
    let dir = tempfile::tempdir().unwrap();
    let m = machine(dir.path()).await;
    let (mut dialed, h1, work) = opened(&m, dir.path()).await;
    std::fs::write(dir.path().join("secret.txt"), "outside").unwrap();

    for (method, params) in [
        ("file.read", json!({ "path": "../../../../secret.txt" })),
        ("file.read", json!({ "path": ".git/config" })),
        ("file.read", json!({ "path": "/etc/hostname" })),
        (
            "file.write",
            json!({ "path": "sub/.git/hooks/x", "content": "" }),
        ),
        ("git.stage", json!({ "paths": ["README.md", "../x"] })),
        (
            "file.rename",
            json!({ "oldPath": "README.md", "newPath": "../moved" }),
        ),
    ] {
        let reply = call(&mut dialed, &h1, method, params.clone()).await;
        assert_eq!(err(&reply).0, -32602, "{method} {params}: {reply:?}");
    }
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(dir.path(), work.join("escape")).unwrap();
        let reply = call(
            &mut dialed,
            &h1,
            "file.read",
            json!({ "path": "escape/secret.txt" }),
        )
        .await;
        assert_eq!(err(&reply).0, -32602, "{reply:?}");
    }

    let reply = call(&mut dialed, &h1, "agent.list", json!({})).await;
    assert_eq!(err(&reply).0, -32601, "{reply:?}");

    let reply = call(&mut dialed, &home("h9"), "file.list", json!({ "path": "" })).await;
    let (_, data) = err(&reply);
    assert_eq!(
        data,
        Some(json!({ "code": NODE_HOME_UNKNOWN })),
        "{reply:?}"
    );

    let big = 9 * 1024 * 1024;
    std::fs::write(work.join("big.bin"), vec![b'x'; big]).unwrap();
    let reply = call(
        &mut dialed,
        &h1,
        "file.readChunk",
        json!({ "path": "big.bin", "offset": 0, "length": big }),
    )
    .await;
    let (code, data) = err(&reply);
    assert_eq!(code, -32603, "{reply:?}");
    assert_eq!(data, Some(json!({ "code": HOME_REPLY_TOO_LARGE })));

    // The link still serves calls afterwards.
    ok(call(
        &mut dialed,
        &h1,
        "file.exists",
        json!({ "path": "README.md" }),
    )
    .await);
}

/// `homeCallCancel` ends a call, and a call past its `timeoutMs` ends too:
/// neither is answered, and later calls are.
#[tokio::test]
async fn a_cancelled_or_timed_out_home_call_ends_without_a_reply() {
    let dir = tempfile::tempdir().unwrap();
    let m = machine(dir.path()).await;
    let (mut dialed, h1, work) = opened(&m, dir.path()).await;

    let sleeping = call_frame(
        &h1,
        "host.exec",
        json!({ "command": "sh", "args": ["-c", "echo $$ > cancelled.pid; exec sleep 30"], "timeoutMs": 60_000 }),
        60_000,
    );
    let cancelled = sleeping.request_id;
    let mut again = sleeping.clone();
    dialed
        .link
        .send(&LinkFrame::HomeCall(sleeping))
        .await
        .unwrap();
    // A second call with the running call's id is refused at once.
    again.method = "file.exists".into();
    again.params = json!({ "path": "README.md" }).as_object().cloned().unwrap();
    dialed.link.send(&LinkFrame::HomeCall(again)).await.unwrap();
    let refused = next_of(&mut dialed, |frame| match frame {
        LinkFrame::HomeCallReply(reply) => Some(reply),
        _ => None,
    })
    .await;
    assert_eq!(refused.request_id, cancelled);
    assert_eq!(err(&refused).0, -32602, "{refused:?}");
    let late = call_frame(
        &h1,
        "host.exec",
        json!({ "command": "sh", "args": ["-c", "echo $$ > late.pid; exec sleep 30"], "timeoutMs": 60_000 }),
        200,
    );
    let timed_out = late.request_id;
    dialed.link.send(&LinkFrame::HomeCall(late)).await.unwrap();
    until("both commands started", || {
        work.join("cancelled.pid").exists() && work.join("late.pid").exists()
    })
    .await;
    dialed
        .link
        .send(&LinkFrame::HomeCallCancel(HomeCallCancel {
            request_id: cancelled,
        }))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;

    let probe = call_frame(&h1, "file.exists", json!({ "path": "README.md" }), 30_000);
    let probe_id = probe.request_id;
    dialed.link.send(&LinkFrame::HomeCall(probe)).await.unwrap();
    let answered = next_of(&mut dialed, |frame| match frame {
        LinkFrame::HomeCallReply(reply) => Some(reply),
        _ => None,
    })
    .await;
    assert_eq!(answered.request_id, probe_id, "{answered:?}");
    assert!(![cancelled, timed_out].contains(&answered.request_id));
    let runner = m.node.service.runners_for_test()[0].clone();
    until("no call is left running", || {
        runner.home_calls_for_test() == 0
    })
    .await;
    // Neither command outlives its call.
    #[cfg(unix)]
    for file in ["cancelled.pid", "late.pid"] {
        let pid = std::fs::read_to_string(work.join(file)).unwrap();
        let pid = pid.trim().to_owned();
        until(&format!("{file} reaped"), || {
            !std::process::Command::new("kill")
                .args(["-0", &pid])
                .stderr(std::process::Stdio::null())
                .status()
                .unwrap()
                .success()
        })
        .await;
    }
}

/// What calls stored under a home's record (an attachment and its
/// `idempotencyKey`) survives the machine's restart: the home reopens with
/// the same record.
#[tokio::test]
async fn home_attachments_survive_a_machine_restart() {
    use base64::Engine as _;
    let dir = tempfile::tempdir().unwrap();
    let m = machine(dir.path()).await;
    let (mut dialed, h1, _work) = opened(&m, dir.path()).await;
    let place = json!({
        "workspaceId": "ws-1",
        "fileName": "note.txt",
        "data": base64::engine::general_purpose::STANDARD.encode(b"kept bytes"),
        "idempotencyKey": "place-1",
    });
    let placed = ok(call(&mut dialed, &h1, "file.placeAttachment", place.clone()).await);
    let attachment = placed["attachmentId"].clone();
    assert!(attachment.is_string(), "{placed}");
    let records = m.services.store().home_record_ids().await.unwrap();
    drop(dialed);

    // Restart: the daemon purges records whose home is gone, and this one
    // is not.
    let Machine {
        node,
        services,
        _store,
    } = m;
    let api = node.stop().await;
    crate::executor::ServicesHomeRecords(services.clone())
        .purge()
        .await;
    assert_eq!(services.store().home_record_ids().await.unwrap(), records);
    let node = Running::start_with_records(
        dir.path(),
        api,
        CountAdmission::new(4),
        Arc::new(HomesLaunch(true)),
        |storage, _cache| checkout::PlainCheckouts::new(storage),
        Some(Arc::new(crate::executor::ServicesHomeRecords(
            services.clone(),
        ))),
    )
    .await;
    let mut dialed = dial_link(&node.endpoint, leasing(2, Some(vec![h1.clone()])))
        .await
        .unwrap();
    replayed(&mut dialed).await;

    let info = ok(call(
        &mut dialed,
        &h1,
        "file.getAttachmentInfo",
        json!({ "workspaceId": "ws-1", "attachmentId": attachment }),
    )
    .await);
    assert_eq!(info["attachmentId"], attachment, "{info}");
    assert_eq!(info["exists"], true, "{info}");
    let replayed_place = ok(call(&mut dialed, &h1, "file.placeAttachment", place).await);
    assert_eq!(
        replayed_place["attachmentId"], attachment,
        "{replayed_place}"
    );
    assert_eq!(services.store().home_record_ids().await.unwrap(), records);

    // A swept home takes its record with it (open homes are never swept,
    // so it is closed first, as at a shutdown).
    drop(dialed);
    node.service.runners_for_test()[0].close_homes();
    let mut dialed = dial_link(&node.endpoint, leasing(3, Some(Vec::new())))
        .await
        .unwrap();
    replayed(&mut dialed).await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while !services.store().home_record_ids().await.unwrap().is_empty() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the swept home's record stays"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    node.stop().await;
}
