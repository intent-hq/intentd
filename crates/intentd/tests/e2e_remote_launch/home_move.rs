//! Moving a workspace's home (monorepo `docs/protocol/methods/nodes.md`,
//! "Moving the home") against a real orchestrator and two real machines:
//! `workspace.setHome` answers `moving` at once; while the old home's last
//! backup is under way a second move is `home-moving` and terminals fail
//! `home-unavailable { moving: true }`; the new home is opened from the
//! old one's committed and uncommitted work, the home switches, and the
//! old machine gets its `moved` stop. A new home that cannot open rolls
//! the move back; an orchestrator restart mid-move completes it. Each
//! refusal carries its documented code.

use std::os::unix::fs::PermissionsExt as _;

use nix::sys::signal::Signal;

use super::homes::{commit_on, home_checkout, opened_home, until};
use super::*;

/// An orchestrator whose workspace's home is on machine A, with machine B
/// exposed too.
struct TwoMachines {
    scratch: tempfile::TempDir,
    head_dir: PathBuf,
    head: common::DaemonGuard,
    rpc_ws: common::TlsWs,
    a_dir: PathBuf,
    a: Machine,
    b_dir: PathBuf,
    b: Machine,
    ws_id: String,
    home_id: String,
    mirror: PathBuf,
    work: PathBuf,
    next_id: i64,
}

/// `error.code` and `error.data.code` of a refused call.
#[track_caller]
fn refusal(response: &Value) -> (i64, String) {
    let error = &response["error"];
    assert!(error.is_object(), "expected an error: {response}");
    (
        error["code"].as_i64().unwrap(),
        error["data"]["code"]
            .as_str()
            .unwrap_or_default()
            .to_owned(),
    )
}

async fn two_machines(prefix: &str) -> Option<TwoMachines> {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping home move e2e: node or git not on PATH");
        return None;
    }
    let scratch = common::test_tempdir_in("/tmp", prefix);
    let repo = scratch.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    std::fs::write(repo.join("README.md"), "base\n").unwrap();
    git(&repo, &["add", "README.md"]);
    git(&repo, &["commit", "--quiet", "-m", "base"]);
    let branch = git(&repo, &["rev-parse", "--abbrev-ref", "HEAD"]);
    let head_dir = scratch.path().join("head");
    let (head, port, cfg, seeded) = start_backend(&head_dir, &repo).await;
    // A regular daemon keeps no homes.
    let mut rpc_ws = connect(port, cfg).await;
    let response = rpc(
        &mut rpc_ws,
        1,
        "workspace.setHome",
        json!({ "workspaceId": seeded, "to": { "kind": "machine", "nodeId": "n-1" } }),
    )
    .await;
    assert_eq!(refusal(&response), (-32602, "homes-unsupported".into()));
    drop(rpc_ws);
    drop(head);
    super::orchestrator::orchestrator_config(&head_dir);
    let (head, port, cfg) = serve_backend(&head_dir, &[]).await;
    let mut rpc_ws = connect(port, cfg).await;
    let backend = ok(&mut rpc_ws, 2, "remoteMachines.backendInfo", json!({})).await;
    let backend_id = backend["backendId"].as_str().unwrap().to_owned();
    let a_dir = scratch.path().join("node-a");
    std::fs::create_dir_all(&a_dir).unwrap();
    let a = start_machine_with(&a_dir, &backend_id, &[]).await;
    ok(
        &mut rpc_ws,
        3,
        "remoteMachines.add",
        add_params(&a, "Machine A"),
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
            "machine A never became a home: {response}"
        );
        // timing-guard: retry while the new machine finishes its first bind
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let ws_id = created["id"].as_str().unwrap().to_owned();
    let home_id = created["home"]["homeId"].as_str().unwrap().to_owned();
    assert_eq!(created["home"]["nodeId"], json!(a.daemon_id));
    let mirror = PathBuf::from(created["worktreePath"].as_str().unwrap());
    let base = git(&mirror, &["rev-parse", "HEAD"]);
    let work = opened_home(&mut rpc_ws, &mut id, &ws_id, &a_dir, &home_id).await;
    assert_eq!(try_rev(&work, "HEAD").as_deref(), Some(base.as_str()));
    let b_dir = scratch.path().join("node-b");
    std::fs::create_dir_all(&b_dir).unwrap();
    let b = start_machine_with(&b_dir, &backend_id, &[]).await;
    id += 1;
    ok(
        &mut rpc_ws,
        id,
        "remoteMachines.add",
        add_params(&b, "Machine B"),
    )
    .await;
    Some(TwoMachines {
        scratch,
        head_dir,
        head,
        rpc_ws,
        a_dir,
        a,
        b_dir,
        b,
        ws_id,
        home_id,
        mirror,
        work,
        next_id: id,
    })
}

impl TwoMachines {
    fn id(&mut self) -> i64 {
        self.next_id += 1;
        self.next_id
    }

    async fn set_home(&mut self, to: Value) -> Value {
        let id = self.id();
        let ws_id = self.ws_id.clone();
        rpc(
            &mut self.rpc_ws,
            id,
            "workspace.setHome",
            json!({ "workspaceId": ws_id, "to": to }),
        )
        .await
    }

    async fn home(&mut self) -> Value {
        let id = self.id();
        let ws_id = self.ws_id.clone();
        let got = ok(
            &mut self.rpc_ws,
            id,
            "workspace.get",
            json!({ "workspaceId": ws_id }),
        )
        .await;
        got["workspace"]["home"].clone()
    }

    /// Poll `workspace.get` until the home satisfies `done`.
    async fn home_until(&mut self, what: &str, done: impl Fn(&Value) -> bool) -> Value {
        let deadline = tokio::time::Instant::now() + wait();
        loop {
            let home = self.home().await;
            if done(&home) {
                return home;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out: {what}: {home}"
            );
            // timing-guard: the move runs in the background; poll its outcome
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }
}

/// The orphaned `head` refs the hub holds for `ws_id`.
fn orphaned_heads(mirror: &Path, ws_id: &str) -> Vec<String> {
    git(
        mirror,
        &[
            "for-each-ref",
            "--format=%(refname)",
            &format!("refs/intent/ws/{ws_id}/home/orphaned/"),
        ],
    )
    .lines()
    .filter(|name| name.ends_with("/head"))
    .map(str::to_owned)
    .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn moving_the_home_carries_its_work_to_another_machine_and_stops_the_old_one() {
    let Some(mut s) = two_machines("itd-home-setmove-").await else {
        return;
    };
    // Targets that cannot be a home.
    for to in [
        json!({ "kind": "machine", "nodeId": "no-such-machine" }),
        json!({ "kind": "cloud", "cloudMachineId": "e2b" }),
        json!({ "agentId": "no-such-agent" }),
    ] {
        let response = s.set_home(to).await;
        assert_eq!(
            refusal(&response),
            (-32602, "home-target-unavailable".into())
        );
    }
    let response = s.set_home(json!({ "kind": "boat" })).await;
    assert_eq!(refusal(&response).0, -32602);

    // Committed and uncommitted work on A.
    let kept = commit_on(&s.work, "kept.txt", "kept\n", "kept");
    std::fs::write(s.work.join("dirty.txt"), "left uncommitted\n").unwrap();

    // A is frozen: the move waits on its last backup.
    s.a.daemon.child_mut().signal(Signal::SIGSTOP).unwrap();
    let b_id = s.b.daemon_id.clone();
    let started = s
        .set_home(json!({ "kind": "machine", "nodeId": b_id }))
        .await;
    assert!(started.get("error").is_none(), "{started}");
    let moving = &started["result"]["home"];
    assert_eq!(moving["state"], "moving", "{started}");
    assert_eq!(moving["homeId"], json!(s.home_id));
    let again = s
        .set_home(json!({ "kind": "machine", "nodeId": b_id }))
        .await;
    assert_eq!(refusal(&again), (-32602, "home-moving".into()));
    let id = s.id();
    let ws_id = s.ws_id.clone();
    let terminal = rpc(
        &mut s.rpc_ws,
        id,
        "terminal.create",
        json!({ "workspaceId": ws_id, "cols": 80, "rows": 24, "command": "/bin/sh" }),
    )
    .await;
    assert_eq!(refusal(&terminal), (-32603, "home-unavailable".into()));
    assert_eq!(
        terminal["error"]["data"]["moving"],
        json!(true),
        "{terminal}"
    );
    s.a.daemon.child_mut().signal(Signal::SIGCONT).unwrap();

    let old_home = s.home_id.clone();
    let home = s
        .home_until("the home is on B", |home| {
            home["nodeId"] == json!(b_id) && home["state"] == "attached"
        })
        .await;
    let new_home = home["homeId"].as_str().unwrap().to_owned();
    assert_ne!(new_home, old_home);
    assert_eq!(home["name"], "Machine B");
    // The new home has A's commit and its uncommitted work. `HEAD` is set
    // before `git worktree add` writes the files and the wip is restored,
    // so wait for the files too.
    let b_dir = s.b_dir.clone();
    let read = |work: &Path, file: &str| std::fs::read_to_string(work.join(file)).ok();
    until("the home checkout on B with A's work", || {
        home_checkout(&b_dir, &new_home).filter(|work| {
            try_rev(work, "HEAD").as_deref() == Some(kept.as_str())
                && read(work, "kept.txt").as_deref() == Some("kept\n")
                && read(work, "dirty.txt").as_deref() == Some("left uncommitted\n")
        })
    })
    .await;
    // A got its moved stop: its last push lands on orphaned refs, then its
    // checkout goes.
    let (a_dir, mirror, ws_id) = (s.a_dir.clone(), s.mirror.clone(), s.ws_id.clone());
    until("the old home checkout is removed", || {
        home_checkout(&a_dir, &old_home).is_none().then_some(())
    })
    .await;
    let orphaned = orphaned_heads(&mirror, &ws_id);
    assert_eq!(orphaned.len(), 1, "{orphaned:?}");
    assert_eq!(git(&mirror, &["rev-parse", &orphaned[0]]), kept);
    // The notice is written once the stop's ack is recorded.
    let deadline = tokio::time::Instant::now() + wait();
    loop {
        let id = s.id();
        let notes = ok(
            &mut s.rpc_ws,
            id,
            "note.list",
            json!({ "workspaceId": ws_id }),
        )
        .await;
        let named = notes["notes"].as_array().is_some_and(|notes| {
            notes.iter().any(|note| {
                note["title"] == "Work left on the old home"
                    && note["content"]
                        .as_str()
                        .is_some_and(|text| text.contains(&orphaned[0]))
            })
        });
        if named {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "no notice: {notes}");
        // timing-guard: poll until the ack's notice is written
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    drop(s.rpc_ws);
    drop(s.head);
    let store = intent_store::Store::open(&s.head_dir.join("intentd.db"))
        .await
        .unwrap();
    assert!(store
        .list_home_stops(&s.a.daemon_id)
        .await
        .unwrap()
        .is_empty());
    assert!(store.list_home_moves().await.unwrap().is_empty());
    drop(s.scratch);
}

/// B's work dir for this backend: the machine names it by its credential.
fn work_dir(node_dir: &Path, credential_id: &str) -> PathBuf {
    use sha2::Digest as _;
    let digest = sha2::Sha256::digest(credential_id.as_bytes());
    let name = digest[..4].iter().fold(String::new(), |mut name, b| {
        use std::fmt::Write as _;
        let _ = write!(name, "{b:02x}");
        name
    });
    node_dir.join("w").join(name)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_move_whose_new_home_cannot_open_rolls_back() {
    let Some(mut s) = two_machines("itd-home-rollback-").await else {
        return;
    };
    // B cannot make a home directory: its homeOpen fails.
    let homes = work_dir(&s.b_dir, &s.b.credential_id).join("homes");
    std::fs::create_dir_all(&homes).unwrap();
    std::fs::set_permissions(&homes, std::fs::Permissions::from_mode(0o500)).unwrap();
    let before = s.home().await;
    let b_id = s.b.daemon_id.clone();
    let started = s
        .set_home(json!({ "kind": "machine", "nodeId": b_id }))
        .await;
    assert!(started.get("error").is_none(), "{started}");
    let home = s
        .home_until("the move rolls back", |home| home["state"] != "moving")
        .await;
    assert_eq!(home["homeId"], before["homeId"]);
    assert_eq!(home["nodeId"], json!(s.a.daemon_id));
    assert_eq!(home["state"], "attached");
    assert_eq!(home["since"], before["since"]);
    // The old home still works: a commit there is backed up.
    let commit = commit_on(&s.work, "after.txt", "after\n", "after the rollback");
    let (mirror, head_ref) = (
        s.mirror.clone(),
        format!("refs/intent/ws/{}/home/head", s.ws_id),
    );
    until("the old home is backed up", || {
        (try_rev(&mirror, &head_ref).as_deref() == Some(commit.as_str())).then_some(())
    })
    .await;
    std::fs::set_permissions(&homes, std::fs::Permissions::from_mode(0o700)).unwrap();
    drop(s.rpc_ws);
    drop(s.head);
    let store = intent_store::Store::open(&s.head_dir.join("intentd.db"))
        .await
        .unwrap();
    assert!(store.list_home_moves().await.unwrap().is_empty());
    assert!(store
        .list_home_stops(&s.a.daemon_id)
        .await
        .unwrap()
        .is_empty());
    drop(s.scratch);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_orchestrator_restart_mid_move_completes_the_move() {
    let Some(mut s) = two_machines("itd-home-resume-").await else {
        return;
    };
    let kept = commit_on(&s.work, "kept.txt", "kept\n", "kept");
    // The move stops at its first step: A is frozen.
    s.a.daemon.child_mut().signal(Signal::SIGSTOP).unwrap();
    let b_id = s.b.daemon_id.clone();
    let started = s
        .set_home(json!({ "kind": "machine", "nodeId": b_id }))
        .await;
    assert_eq!(started["result"]["home"]["state"], "moving", "{started}");
    let TwoMachines {
        scratch,
        head_dir,
        head,
        rpc_ws,
        a_dir,
        mut a,
        b_dir,
        b,
        ws_id,
        home_id,
        ..
    } = s;
    drop(rpc_ws);
    drop(head);
    let store = intent_store::Store::open(&head_dir.join("intentd.db"))
        .await
        .unwrap();
    let pending = store.list_home_moves().await.unwrap();
    assert_eq!(pending.len(), 1, "the move is kept: {pending:?}");
    assert!(!pending[0].pushed);
    drop(store);
    a.daemon.child_mut().signal(Signal::SIGCONT).unwrap();

    let (head, port, cfg) = serve_backend(&head_dir, &[]).await;
    let rpc_ws = connect(port, cfg).await;
    let mut s = TwoMachines {
        scratch,
        head_dir,
        head,
        rpc_ws,
        a_dir,
        a,
        b_dir,
        b,
        ws_id,
        home_id,
        mirror: PathBuf::new(),
        work: PathBuf::new(),
        next_id: 1000,
    };
    let home = s
        .home_until("the move completes after the restart", |home| {
            home["nodeId"] == json!(b_id) && home["state"] == "attached"
        })
        .await;
    let new_home = home["homeId"].as_str().unwrap().to_owned();
    let b_dir = s.b_dir.clone();
    until("the home checkout on B", || {
        home_checkout(&b_dir, &new_home)
            .filter(|work| try_rev(work, "HEAD").as_deref() == Some(kept.as_str()))
    })
    .await;
    let (a_dir, old_home) = (s.a_dir.clone(), s.home_id.clone());
    until("the old home checkout is removed", || {
        home_checkout(&a_dir, &old_home).is_none().then_some(())
    })
    .await;
    drop(s.rpc_ws);
    drop(s.head);
    drop(s.scratch);
}

/// nodes.md "Moving the home" step 4: an agent sharing the home checkout is
/// stopped by the old home's `moved` stop, stays on its machine, and at its
/// next start runs in a checkout of its own made from the home's last
/// committed and uncommitted work, with a notice in its prompt and its
/// transcript.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_sharing_agent_relaunches_isolated_after_the_home_moves() {
    let Some(mut s) = two_machines("itd-home-sharer-").await else {
        return;
    };
    let (ws_id, a_id) = (s.ws_id.clone(), s.a.daemon_id.clone());
    let id = s.id();
    let agent = ok(
        &mut s.rpc_ws,
        id,
        "agent.create",
        json!({
            "workspaceId": ws_id,
            "name": "Sharer",
            "model": "default",
            "provider": "mock",
            "placement": { "nodeId": a_id },
        }),
    )
    .await;
    let agent_id = agent["agent"]["id"].as_str().unwrap().to_owned();
    let id = s.id();
    ok(
        &mut s.rpc_ws,
        id,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "hello" }),
    )
    .await;
    let work = s.work.clone();
    let pids = until("the agent started in the home checkout", || {
        std::fs::read_to_string(work.join("agent.pid"))
            .ok()
            .and_then(|text| serde_json::from_str::<Value>(&text).ok())
    })
    .await;
    let pid = pids["agent"].to_string();
    std::fs::remove_file(work.join("agent.pid")).unwrap();

    // The home's last work: a commit and an uncommitted file.
    let kept = commit_on(&work, "kept.txt", "kept\n", "kept");
    std::fs::write(work.join("dirty.txt"), "left uncommitted\n").unwrap();

    let b_id = s.b.daemon_id.clone();
    let started = s
        .set_home(json!({ "kind": "machine", "nodeId": b_id }))
        .await;
    assert!(started.get("error").is_none(), "{started}");
    s.home_until("the home is on B", |home| {
        home["nodeId"] == json!(b_id) && home["state"] == "attached"
    })
    .await;
    // The old home's stop ends the sharing agent before its checkout goes.
    let (a_dir, old_home) = (s.a_dir.clone(), s.home_id.clone());
    until("the old home checkout is removed", || {
        home_checkout(&a_dir, &old_home).is_none().then_some(())
    })
    .await;
    until("the sharing agent is stopped", || {
        super::homes::process_gone(&pid).then_some(())
    })
    .await;

    // Its next start: its own checkout on A, from the home's last work.
    let id = s.id();
    ok(
        &mut s.rpc_ws,
        id,
        "agent.sendMessage",
        json!({ "workspaceId": ws_id, "agentId": agent_id, "content": "echo-raw" }),
    )
    .await;
    let id = s.id();
    let raw = reply_containing(&mut s.rpc_ws, (&ws_id, &agent_id), "raw: ", id).await;
    s.next_id += 1000;
    let notice = "The workspace's home moved. This agent stays on";
    assert!(raw.contains(notice), "the turn carries the notice: {raw}");
    let id = s.id();
    ok(
        &mut s.rpc_ws,
        id,
        "agent.sendMessage",
        json!({
            "workspaceId": ws_id,
            "agentId": agent_id,
            "content": "sh t1 pwd && git rev-parse HEAD && cat kept.txt dirty.txt",
        }),
    )
    .await;
    let reply = reply_containing(&mut s.rpc_ws, (&ws_id, &agent_id), "sh-out t1 ", id + 1).await;
    s.next_id += 1000;
    assert!(reply.contains("\\\"code\\\":0"), "{reply}");
    assert!(
        reply.contains(&kept),
        "it starts at the home's head: {reply}"
    );
    assert!(reply.contains("kept\\\\n"), "{reply}");
    assert!(
        reply.contains("left uncommitted"),
        "it has the home's wip: {reply}"
    );
    assert!(
        reply.contains(&a_dir.to_string_lossy().into_owned()),
        "it stays on A: {reply}"
    );
    assert!(
        !reply.contains("/homes/"),
        "not in a home checkout: {reply}"
    );

    // The user sees the notice once in the transcript.
    let id = s.id();
    let convo = ok(
        &mut s.rpc_ws,
        id,
        "agent.getConversation",
        json!({ "workspaceId": ws_id, "agentId": agent_id }),
    )
    .await;
    let notices = convo["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|m| m["role"] == "system" && m.to_string().contains(notice))
        .count();
    assert_eq!(notices, 1, "{convo}");
    drop(s.rpc_ws);
    drop(s.head);
    drop(s.scratch);
}
