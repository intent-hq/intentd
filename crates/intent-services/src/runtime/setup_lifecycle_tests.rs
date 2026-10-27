//! Setup-script lifecycle events from a real script run (moved from
//! `tests.rs` `setup_lifecycle_events`).

use intent_core::{WorkspaceApi, WorkspaceCreate, WorkspaceSetupState};
use serde_json::json;

#[cfg(unix)]
use intentd_test_support::Barrier;

use crate::tests::setup_lifecycle_events::*;

/// Script exits non-zero: `completed` still fires once, carrying the
/// script's real exit code — a failing setup script never fails the
/// create and never suppresses the lifecycle terminal event.
#[cfg(unix)]
#[intent_test_macros::daemon_test]
async fn script_failure_emits_completed_with_nonzero_exit_code() {
    let (repo_dir, head_branch) = seed_repo("intentd-setupev-fail-repo");
    let root = unique_dir("intentd-setupev-fail-root");
    let (svc, bus, _tmp) = services(root.0.clone()).await;
    let mut sub = subscribe_setup(&bus);

    let ws = svc
        .create_workspace(
            WorkspaceCreate {
                repository_path: Some(repo_dir.0.to_string_lossy().to_string()),
                base_ref: Some(head_branch),
                setup_script: Some("exit 7".to_string()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("create")
        .workspace;

    let ev = recv_setup(&mut sub).await;
    assert_eq!(ev["type"], "workspace:setup:started");
    assert_eq!(ev["workspaceId"], ws.id.0);

    let ev = recv_setup(&mut sub).await;
    assert_eq!(ev["type"], "workspace:setup:completed");
    assert_eq!(ev["data"]["ranScript"], json!(true));
    assert_eq!(ev["data"]["exitCode"], json!(7));
    let failed = svc.workspace_setup_status(&ws.id);
    assert_eq!(failed.state, WorkspaceSetupState::Failed);
    assert_eq!(failed.exit_code, Some(7));
    assert!(failed.finished_at.is_some());

    assert_quiet(&mut sub).await;
}

/// Script runs to a zero exit: `started` then `completed` with
/// `ranScript: true` and `exitCode: 0`. The script parks on a barrier
/// so the `running` snapshot (state + `terminalId` in one write) can be
/// asserted without racing the script's exit.
#[cfg(unix)]
#[intent_test_macros::daemon_test]
async fn script_success_emits_started_then_completed_exit_zero() {
    let (repo_dir, head_branch) = seed_repo("intentd-setupev-ok-repo");
    let root = unique_dir("intentd-setupev-ok-root");
    let (svc, bus, _tmp) = services(root.0.clone()).await;
    let mut sub = subscribe_setup(&bus);
    let barrier = Barrier::new(&root.0, "setup");

    let ws = svc
        .create_workspace(
            WorkspaceCreate {
                repository_path: Some(repo_dir.0.to_string_lossy().to_string()),
                base_ref: Some(head_branch),
                setup_script: Some(format!(
                    "{}\n{}\nexit 0",
                    barrier.sh_arrive(),
                    barrier.sh_wait()
                )),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("create")
        .workspace;

    let ev = recv_setup(&mut sub).await;
    assert_eq!(ev["type"], "workspace:setup:started");
    assert_eq!(ev["workspaceId"], ws.id.0);
    assert_eq!(ev["data"], json!({ "workspaceId": ws.id.0 }));
    // `started` precedes the spawn attempt, so the record is still
    // `pending` or already `running`; once the script has reached the
    // barrier the spawn succeeded and the snapshot is `running` with
    // its terminal attached.
    let early = svc.workspace_setup_status(&ws.id);
    assert!(
        matches!(
            early.state,
            WorkspaceSetupState::Pending | WorkspaceSetupState::Running
        ),
        "after started: {early:?}"
    );
    wait_for_barrier(&barrier).await;
    let running = svc.workspace_setup_status(&ws.id);
    assert_eq!(running.state, WorkspaceSetupState::Running);
    assert!(running.terminal_id.is_some(), "running carries terminalId");
    assert!(running.started_at.is_some(), "running carries startedAt");
    assert_eq!(running.exit_code, None);
    assert_eq!(running.finished_at, None);
    barrier.release();

    let ev = recv_setup(&mut sub).await;
    assert_eq!(ev["type"], "workspace:setup:completed");
    assert_eq!(ev["workspaceId"], ws.id.0);
    assert_eq!(ev["data"]["workspaceId"], ws.id.0);
    assert_eq!(ev["data"]["ranScript"], json!(true));
    assert_eq!(ev["data"]["exitCode"], json!(0));
    let done = svc.workspace_setup_status(&ws.id);
    assert_eq!(done.state, WorkspaceSetupState::Completed);
    assert_eq!(done.exit_code, Some(0));
    assert!(done.terminal_id.is_some(), "completed keeps the terminalId");
    assert_eq!(done.started_at, running.started_at);
    assert!(done.finished_at.is_some(), "completed carries finishedAt");

    assert_quiet(&mut sub).await;
}

/// Wait until the setup script has reached its barrier (proving the
/// spawn succeeded and the script is parked there).
#[cfg(unix)]
pub(crate) async fn wait_for_barrier(barrier: &Barrier) {
    tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(20)),
        async {
            while !barrier.entered() {
                // timing-guard: poll interval for the barrier's arrival file
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        },
    )
    .await
    .expect("setup script reached the barrier");
}
