//! Home final stops (monorepo `docs/protocol/node-link.md`, "Home final
//! stop") against a real orchestrator and a real machine: archiving the
//! workspace pushes its home's `home/head` and `home/wip` one last time
//! and removes the home checkout; a move recorded while the orchestrator
//! is down (the switch a home move makes, written straight to its store)
//! is carried out when the machine binds next: the leftover checkout is
//! pushed to `home/orphaned/<orphanedAt>/{head,wip}` only, then removed,
//! and the workspace gets a notice naming those refs.

use super::homes::{commit_on, home_checkout, opened_home, until};
use super::*;

/// A workspace whose home is a fresh machine: the orchestrator, its
/// connection, the machine, the workspace id, its home id and the home
/// checkout on the machine.
struct Homed {
    scratch: tempfile::TempDir,
    head_dir: PathBuf,
    head: common::DaemonGuard,
    rpc_ws: common::TlsWs,
    node_dir: PathBuf,
    machine: Machine,
    ws_id: String,
    home_id: String,
    mirror: PathBuf,
    work: PathBuf,
    next_id: i64,
}

async fn homed(prefix: &str) -> Option<Homed> {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping home stop e2e: node or git not on PATH");
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
    let (head, _, _, _) = start_backend(&head_dir, &repo).await;
    drop(head);
    super::orchestrator::orchestrator_config(&head_dir);
    let (head, port, cfg) = serve_backend(&head_dir, &[]).await;
    let mut rpc_ws = connect(port, cfg).await;
    let backend = ok(&mut rpc_ws, 1, "remoteMachines.backendInfo", json!({})).await;
    let backend_id = backend["backendId"].as_str().unwrap().to_owned();
    let node_dir = scratch.path().join("node-0");
    std::fs::create_dir_all(&node_dir).unwrap();
    let machine = start_machine_with(&node_dir, &backend_id, &[]).await;
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
    let work = opened_home(&mut rpc_ws, &mut id, &ws_id, &node_dir, &home_id).await;
    assert_eq!(try_rev(&work, "HEAD").as_deref(), Some(base.as_str()));
    Some(Homed {
        scratch,
        head_dir,
        head,
        rpc_ws,
        node_dir,
        machine,
        ws_id,
        home_id,
        mirror,
        work,
        next_id: id,
    })
}

impl Homed {
    fn home_ref(&self, name: &str) -> String {
        format!("refs/intent/ws/{}/home/{name}", self.ws_id)
    }

    fn id(&mut self) -> i64 {
        self.next_id += 1;
        self.next_id
    }
}

/// The files of `commit`'s tree in `repo`.
fn tree_files(repo: &Path, commit: &str) -> Vec<String> {
    git(repo, &["ls-tree", "--name-only", commit])
        .lines()
        .map(str::to_owned)
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archiving_a_workspace_pushes_its_home_once_more_and_removes_it() {
    let Some(mut homed) = homed("itd-home-stop-").await else {
        return;
    };
    // A commit the watcher backs up, then uncommitted work it would not
    // push for minutes: only the archive's last push takes it.
    let commit = commit_on(&homed.work, "kept.txt", "kept\n", "kept");
    let (mirror, head_ref) = (homed.mirror.clone(), homed.home_ref("head"));
    until("the commit is backed up", || {
        (try_rev(&mirror, &head_ref).as_deref() == Some(commit.as_str())).then_some(())
    })
    .await;
    std::fs::write(homed.work.join("dirty.txt"), "left uncommitted\n").unwrap();
    let id = homed.id();
    let ws_id = homed.ws_id.clone();
    ok(
        &mut homed.rpc_ws,
        id,
        "workspace.archive",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    let mirror = homed.mirror.clone();
    let (head_ref, wip_ref) = (homed.home_ref("head"), homed.home_ref("wip"));
    let wip = until("the last push of the home", || {
        let head = try_rev(&mirror, &head_ref)?;
        let wip = try_rev(&mirror, &wip_ref)?;
        (head == commit && tree_files(&mirror, &wip).contains(&"dirty.txt".to_owned()))
            .then_some(wip)
    })
    .await;
    assert_eq!(git(&mirror, &["rev-parse", &format!("{wip}^")]), commit);
    let (node_dir, home_id) = (homed.node_dir.clone(), homed.home_id.clone());
    until("the home checkout is removed", || {
        home_checkout(&node_dir, &home_id).is_none().then_some(())
    })
    .await;
    assert!(!homed.work.exists());
    // The stop is acknowledged: the machine acks after it removes the
    // checkout, and the backend then drops the stop. The backend stays up
    // until it has, or the ack is lost with it.
    let db = intent_store::DiagnosticStore::open(&homed.head_dir.join("intentd.db"))
        .await
        .unwrap();
    let deadline = tokio::time::Instant::now() + wait();
    loop {
        let (left,): (i64,) = sqlx::query_as("SELECT COUNT(*) FROM home_stop WHERE node_id = ?")
            .bind(&homed.machine.daemon_id)
            .fetch_one(db.read_pool())
            .await
            .unwrap();
        if left == 0 {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "the home stop was never acknowledged"
        );
        // timing-guard: poll while the backend records the machine's ack
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    drop(db);
    let id = homed.id();
    let got = ok(
        &mut homed.rpc_ws,
        id,
        "workspace.get",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert!(
        got["workspace"].get("home").is_none_or(Value::is_null),
        "{got}"
    );
    drop(homed.rpc_ws);
    drop(homed.head);
    let store = intent_store::Store::open(&homed.head_dir.join("intentd.db"))
        .await
        .unwrap();
    assert!(store
        .list_home_stops(&homed.machine.daemon_id)
        .await
        .unwrap()
        .is_empty());
    drop(homed.scratch);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_moved_home_pushes_its_leftover_work_to_orphaned_refs_when_its_machine_binds() {
    let Some(homed) = homed("itd-home-move-").await else {
        return;
    };
    let Homed {
        scratch,
        head_dir,
        head,
        rpc_ws,
        node_dir,
        machine,
        ws_id,
        home_id,
        mirror,
        work,
        ..
    } = homed;
    let home_ref = |name: &str| format!("refs/intent/ws/{ws_id}/home/{name}");
    // The orchestrator goes away; the home gets committed and uncommitted
    // work it does not have yet.
    drop(rpc_ws);
    drop(head);
    let left = commit_on(&work, "left.txt", "left\n", "left on the old home");
    std::fs::write(work.join("dirty.txt"), "left uncommitted\n").unwrap();

    // The move's switch, recorded while the orchestrator is down: a new
    // home on the same machine, and a stop of the old one.
    let store = intent_store::Store::open(&head_dir.join("intentd.db"))
        .await
        .unwrap();
    let workspace_id = intent_core::WorkspaceId::from(ws_id.as_str());
    let old = store
        .get_workspace_home(&workspace_id)
        .await
        .unwrap()
        .unwrap();
    let new_home = intent_core::WorkspaceHome {
        home_id: intent_core::HomeId::new(),
        since: intent_core::now_iso(),
        last_backup_at: None,
        ..old.clone()
    };
    let stop = store
        .switch_workspace_home(&workspace_id, &new_home, &intent_core::now_iso())
        .await
        .unwrap()
        .expect("the old home gets a stop");
    drop(store);
    let at = stop.orphaned_at.clone().unwrap();

    // The machine binds again when the orchestrator is back.
    let (head, port, cfg) = serve_backend(&head_dir, &[]).await;
    let orphaned_head = format!("{}orphaned/{at}/head", home_ref(""));
    let orphaned_wip = format!("{}orphaned/{at}/wip", home_ref(""));
    let wip = until("the orphaned refs", || {
        let head = try_rev(&mirror, &orphaned_head)?;
        let wip = try_rev(&mirror, &orphaned_wip)?;
        (head == left).then_some(wip)
    })
    .await;
    assert!(tree_files(&mirror, &wip).contains(&"dirty.txt".to_owned()));
    until("the old home checkout is removed", || {
        home_checkout(&node_dir, &home_id).is_none().then_some(())
    })
    .await;
    // The old home never wrote the new home's refs.
    assert_ne!(
        try_rev(&mirror, &home_ref("head")).as_deref(),
        Some(left.as_str())
    );
    // The workspace's notice names the refs.
    let mut rpc_ws = connect(port, cfg).await;
    let deadline = tokio::time::Instant::now() + wait();
    let mut id = 100;
    loop {
        id += 1;
        let notes = ok(
            &mut rpc_ws,
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
                        .is_some_and(|text| text.contains(&orphaned_head))
            })
        });
        if named {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "no notice: {notes}");
        // timing-guard: poll until the ack's notice is written
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    drop(rpc_ws);
    drop(head);
    drop(machine);
    drop(scratch);
}
