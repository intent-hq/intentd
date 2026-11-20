//! Archive stops machine runs (monorepo `docs/protocol/methods/scripts.md`,
//! "autoStart: runs while the workspace is active"): a service a remote
//! agent started on its machine ends there when the workspace is archived,
//! as `script.stop` with `remoteAgentId` ends it, and its machine run reads
//! idle on head.

use super::homes::process_gone;
use super::secrets::{agent_turn, create_agent};
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn archiving_a_workspace_stops_its_remote_agents_machine_runs() {
    if intent_providers::resolve_on_path("node").is_none()
        || intent_providers::resolve_on_path("git").is_none()
    {
        eprintln!("skipping machine-run archive e2e: node or git not on PATH");
        return;
    }
    let scratch = common::test_tempdir_in("/tmp", "itd-mrun-archive-");
    let repo = scratch.path().join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    git(&repo, &["init", "--quiet"]);
    std::fs::write(repo.join("README.md"), "base\n").unwrap();
    git(&repo, &["add", "README.md"]);
    git(&repo, &["commit", "--quiet", "-m", "base"]);
    let head_dir = scratch.path().join("head");
    let (_head, port, cfg, ws_id) = start_backend(&head_dir, &repo).await;
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
    let agent_id = create_agent(&mut rpc_ws, &ws_id, "Remote", 10).await;

    // A service on the machine that records its process id.
    let pid_file = scratch.path().join("service.pid");
    // timing-guard: the saved service's own runtime on the machine, not a wait in the test
    let command = format!("printf '%s' $$ > {}; exec sleep 300", pid_file.display());
    let wsapi = format!(
        r#"wsapi const c = await ws.script.create("msrv", {command:?}, "service"); const id = c.id ?? c.script.id; await ws.script.start(id); return JSON.stringify({{ marker: "turn-100", id }});"#
    );
    let reply = agent_turn(
        &mut rpc_ws,
        &mut events,
        (&ws_id, &agent_id),
        &wsapi,
        "turn-100",
        100,
    )
    .await;
    let script_id = wsapi_value(&reply)["id"]
        .as_str()
        .unwrap_or_else(|| panic!("{reply}"))
        .to_owned();
    machine_run_until(&mut rpc_ws, (&ws_id, &script_id, &agent_id), 200, |run| {
        run["runtime"]["status"] == "running"
    })
    .await;
    let pid = super::homes::until("the service's pid file", || {
        std::fs::read_to_string(&pid_file)
            .ok()
            .filter(|pid| !pid.is_empty())
    })
    .await;
    assert!(!process_gone(&pid), "the service is not running: {pid}");

    ok(
        &mut rpc_ws,
        400,
        "workspace.archive",
        json!({ "workspaceId": ws_id }),
    )
    .await;

    // The archive reply comes after the stop: the run is already idle.
    let row = script_row(&mut rpc_ws, (&ws_id, &script_id), 401).await;
    let run = row["machineRuns"]
        .as_array()
        .and_then(|runs| {
            runs.iter()
                .find(|run| run["remoteAgentId"] == agent_id.as_str())
        })
        .unwrap_or_else(|| panic!("the machine run is gone: {row}"));
    assert_eq!(run["runtime"]["status"], "idle", "{row}");
    super::homes::until("the service ends on the machine", || {
        process_gone(&pid).then_some(())
    })
    .await;
    drop(machine);
}
