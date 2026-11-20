//! Detached workspaces (monorepo `docs/protocol/methods/nodes.md`,
//! "Detached workspaces") against a real orchestrator and a real machine,
//! both with homes enabled: once the home's link has been down for the
//! link-loss grace, `workspace:updated` carries `home.state: "detached"`
//! with a new `since`, and a file call fails with `home-unavailable` and
//! its documented data; when the machine binds again the home is
//! `"attached"` and files work again. The failing backup streak the loss
//! began keeps `backupWarning` on until the home backs up again
//! ("Continuous backup").

use super::homes::{commit_on, opened_home};
use super::*;

/// The orchestrator's link-loss grace in this test (60 s in production).
const GRACE: (&str, &str) = ("INTENTD_LINK_LOSS_GRACE_MS", "1500");

/// How long backups fail before `backupWarning` shows in this test (15
/// minutes in production): shorter than the grace, so the warning is on
/// by the time the home is detached.
const WARNING: (&str, &str) = ("INTENTD_BACKUP_WARNING_MS", "1000");

/// The next `workspace:updated` of `ws_id` whose home matches `wanted`.
async fn home_update(
    events: &mut common::TlsWs,
    ws_id: &str,
    wanted: impl Fn(&Value) -> bool,
) -> Value {
    loop {
        let updated = event(events, "workspace:updated").await;
        if updated["data"]["workspaceId"] != ws_id {
            continue;
        }
        let home = &updated["data"]["changes"]["home"];
        if home.is_object() && wanted(home) {
            return home.clone();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_home_whose_link_is_lost_is_detached_and_attached_again_at_the_next_bind() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping detached home e2e: node or git not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-home-detached-");
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
    let (_head, port, cfg) = serve_backend(&head_dir, &[GRACE, WARNING]).await;
    let mut rpc_ws = connect(port, cfg.clone()).await;
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
    assert_eq!(created["home"]["state"], "attached", "{created}");
    let attached_since = created["home"]["since"].as_str().unwrap().to_owned();

    let mut events = connect(port, cfg.clone()).await;
    id += 1;
    ok(
        &mut events,
        id,
        "events.subscribe",
        json!({ "eventTypes": ["workspace:updated"], "workspaceId": ws_id }),
    )
    .await;

    // A backup, so the error names when the last one was.
    let work = opened_home(&mut rpc_ws, &mut id, &ws_id, &node_dir, &home_id).await;
    commit_on(&work, "a.txt", "a\n", "a");
    home_update(&mut events, &ws_id, |home| home["lastBackupAt"].is_string()).await;

    // The machine goes away: past the grace the home is detached.
    let machine_port = machine.port;
    let machine_id = machine.daemon_id.clone();
    drop(machine);
    let detached = home_update(&mut events, &ws_id, |home| home["state"] == "detached").await;
    assert_eq!(detached["homeId"], home_id.as_str(), "{detached}");
    assert_ne!(detached["since"], attached_since.as_str(), "{detached}");
    // Backups have failed since the link was lost, past the warning.
    assert_eq!(detached["backupWarning"], true, "{detached}");
    id += 1;
    let row = ok(
        &mut rpc_ws,
        id,
        "workspace.get",
        json!({ "workspaceId": ws_id }),
    )
    .await;
    assert_eq!(row["workspace"]["home"]["state"], "detached", "{row}");
    // The home may have backed up more than once before the loss (at open
    // and for the commit); the error names the last one the row holds.
    let last_backup = row["workspace"]["home"]["lastBackupAt"]
        .as_str()
        .unwrap()
        .to_owned();

    id += 1;
    let lost = rpc(
        &mut rpc_ws,
        id,
        "file.read",
        json!({ "workspaceId": ws_id, "path": "a.txt" }),
    )
    .await;
    let error = &lost["error"];
    assert_eq!(error["code"], -32603, "{lost}");
    assert_eq!(
        error["message"],
        "Home Machine A is unavailable. Chats and notes still work; files, terminals and scripts don't.",
        "{lost}"
    );
    let data = &error["data"];
    assert_eq!(data["code"], "home-unavailable", "{lost}");
    assert_eq!(data["nodeId"], machine_id.as_str(), "{lost}");
    assert_eq!(data["name"], "Machine A", "{lost}");
    assert_eq!(data["lastBackupAt"], last_backup.as_str(), "{lost}");
    assert_eq!(data["retryable"], true, "{lost}");

    // The machine comes back on the port its row names: the home is
    // attached again.
    let pinned = machine_port.to_string();
    let (_machine, again_port, _) =
        spawn_machine(&node_dir, &[("INTENTD_TCP_PORT", pinned.as_str())]).await;
    assert_eq!(again_port, machine_port, "the machine keeps its WSS port");
    let attached = home_update(&mut events, &ws_id, |home| home["state"] == "attached").await;
    assert_ne!(attached["since"], detached["since"], "{attached}");
    // The bind alone does not end the streak the loss began: only a backup
    // the orchestrator holds does, and it moves `lastBackupAt`.
    let detached_since = detached["since"].as_str().unwrap();
    let backed_up_since_loss = |home: &Value| {
        home["lastBackupAt"]
            .as_str()
            .is_some_and(|at| at > detached_since)
    };
    assert!(
        attached["backupWarning"] == true || backed_up_since_loss(&attached),
        "the warning stays on until a backup: {attached}"
    );
    // The orchestrator asks the home it attached again for a backup, which
    // ends the streak.
    let cleared = if attached["backupWarning"] == true {
        home_update(&mut events, &ws_id, |home| home["backupWarning"] != true).await
    } else {
        attached.clone()
    };
    assert!(backed_up_since_loss(&cleared), "{cleared}");
    id += 1;
    let read = ok(
        &mut rpc_ws,
        id,
        "file.read",
        json!({ "workspaceId": ws_id, "path": "a.txt" }),
    )
    .await;
    assert_eq!(read, "a\n", "{read}");
}
