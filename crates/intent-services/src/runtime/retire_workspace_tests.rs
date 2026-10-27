//! `workspace.retire` against running scripts and terminals (moved from
//! `tests/retire_workspace.rs`).

use std::time::Duration;

use intent_core::{Error, WorkspaceApi, WorkspaceStatus};

use crate::tests::retire_workspace::*;

/// Review regression (intentd#2605): the automatic sweep's retire skips
/// (logs and counts a failure) a workspace whose script close cannot settle
/// a finished run, leaving it Archived for the next sweep.
#[intent_test_macros::daemon_test]
async fn auto_retire_skips_a_workspace_whose_script_result_cannot_be_settled() {
    let f = fixture().await;
    let (ws, id) = archived_with_unsettled_run(&f).await;

    let out = f
        .svc
        .auto_retire_workspace_op(ws.id.clone(), days_from_now(8))
        .await;
    assert!(matches!(out, Err(Error::Internal(_))), "{out:?}");
    assert_still_archived_and_unsettled(&f, &ws, &id).await;
}

/// Review regression (intentd#2605): a manual `workspace.retire` whose
/// script close cannot settle a finished run (the result write fails)
/// aborts before the Retired flip with a typed error, so a Retired workspace
/// never holds a live or admitted script. Once the store recovers, retire
/// succeeds and settles the run.
#[intent_test_macros::daemon_test]
async fn manual_retire_aborts_while_a_script_result_cannot_be_settled() {
    let f = fixture().await;
    let (ws, id) = archived_with_unsettled_run(&f).await;

    let err = f
        .svc
        .retire_workspace(ws.id.clone(), None)
        .await
        .unwrap_err();
    assert!(matches!(err, Error::Internal(_)), "{err:?}");
    assert_still_archived_and_unsettled(&f, &ws, &id).await;

    sqlx::query("DROP TRIGGER refuse_result")
        .execute(f.store.write_pool())
        .await
        .unwrap();
    let retired = f
        .svc
        .retire_workspace(ws.id.clone(), None)
        .await
        .expect("retire after recovery");
    assert!(retired.is_retired());
    let def = f
        .store
        .get_script_in_workspace(&ws.id, &id)
        .await
        .unwrap()
        .expect("script row");
    assert!(
        def.last_run.is_some(),
        "run settled before the flip: {def:?}"
    );
}

/// Retire kills the workspace's terminals: their shells ran in the deleted
/// folder, and a write addressed only by `terminalId` must have nothing left
/// to reach.
#[intent_test_macros::daemon_test]
async fn retire_kills_workspace_terminals() {
    let f = fixture().await;
    let ws = create_workspace(&f).await;
    f.svc
        .terminal_create(ws.id.clone(), 80, 24, None, None, None)
        .await
        .expect("terminal");
    assert_eq!(f.svc.pty().list_scope(ws.id.as_str()).len(), 1);
    f.svc
        .retire_workspace(ws.id.clone(), None)
        .await
        .expect("retire");
    assert!(
        f.svc.pty().list_scope(ws.id.as_str()).is_empty(),
        "terminals killed on retire"
    );
}

/// Disk-backed mutations on a retired workspace are refused with
/// `workspace-retired`, as is archive; `workspace.delete` still works.
/// (Unarchive revives instead — see the revive tests below.)
#[intent_test_macros::daemon_test]
async fn retired_workspace_refuses_disk_mutations_and_still_deletes() {
    let f = fixture().await;
    let ws = create_workspace(&f).await;
    f.svc
        .retire_workspace(ws.id.clone(), None)
        .await
        .expect("retire");
    let id = || ws.id.clone();
    let is_retired = |r: &Error| matches!(r, Error::WorkspaceRetired { workspace_id } if *workspace_id == ws.id.0);

    let err = f
        .svc
        .file_write(id(), "a.txt".into(), "x".into(), None)
        .await
        .unwrap_err();
    assert!(is_retired(&err), "file.write: {err:?}");
    let err = f.svc.file_mkdir(id(), "d".into(), None).await.unwrap_err();
    assert!(is_retired(&err), "file.mkdir: {err:?}");
    let err = f
        .svc
        .file_delete(id(), "README.md".into(), None)
        .await
        .unwrap_err();
    assert!(is_retired(&err), "file.delete: {err:?}");
    // script.start/restart/run are refused at the entry gates (RPC
    // `retired_gate`, ws.* dispatcher), not in the service: an extra await
    // before the launch reservation would reorder the start/stop race.
    let err = f
        .svc
        .git_stage(id(), serde_json::json!(["README.md"]))
        .await
        .unwrap_err();
    assert!(is_retired(&err), "git.stage: {err:?}");
    let err = f
        .svc
        .git_agent_commit(id(), "msg".into(), None, None, None, true, None)
        .await
        .unwrap_err();
    assert!(is_retired(&err), "git.agentCommit: {err:?}");
    let err = f
        .svc
        .terminal_create(id(), 80, 24, None, None, None)
        .await
        .unwrap_err();
    assert!(is_retired(&err), "terminal.create: {err:?}");
    let err = f.svc.archive_workspace(id(), None).await.unwrap_err();
    assert!(is_retired(&err), "workspace.archive: {err:?}");
    assert_eq!(
        f.store.get_workspace(&ws.id).await.unwrap().status,
        WorkspaceStatus::Retired,
        "refusals leave the row retired"
    );

    f.svc.delete_workspace(id()).await.expect("delete retired");
    assert!(matches!(
        f.store.get_workspace(&ws.id).await,
        Err(Error::NotFound(_))
    ));
}

/// Review regression (intentd#2605): a `script.create` of a NEW id that
/// passed its retired check while the workspace was still Archived is
/// waited for by retire's script close (it is not yet in the registry
/// snapshot), so its definition commits strictly before the Retired flip and
/// never after `workspace.retire` returned.
#[intent_test_macros::daemon_test]
async fn script_create_admitted_before_retire_commits_before_the_flip() {
    let f = fixture().await;
    let park = std::sync::Arc::new(crate::runtime_config::SupervisePark::default());
    let svc = f.svc.clone().with_script_create_admitted_park(park.clone());
    let ws = create_workspace(&f).await;
    svc.archive_workspace(ws.id.clone(), None)
        .await
        .expect("archive");

    let create = intent_core::spawn_daemon({
        let svc = svc.clone();
        let id = ws.id.clone();
        async move {
            svc.script_create(
                id,
                intent_core::ScriptCreateParams {
                    name: "racing".to_string(),
                    command: "echo racing".to_string(),
                    mode: intent_core::ScriptMode::Command,
                    purpose: Some(intent_core::ScriptPurpose::Saved),
                    script_id: Some("racing-script".to_string()),
                    ..Default::default()
                },
            )
            .await
        }
    });
    tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(10)),
        park.entered.notified(),
    )
    .await
    .expect("create parks after its checks");
    let retire = intent_core::spawn_daemon({
        let svc = svc.clone();
        let id = ws.id.clone();
        async move { svc.retire_workspace(id, None).await }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(
        !retire.is_finished(),
        "retire must wait for the admitted create before flipping"
    );
    assert!(!f.store.get_workspace(&ws.id).await.unwrap().is_retired());
    park.release.notify_one();

    tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(10)),
        create,
    )
    .await
    .expect("create finishes")
    .expect("join")
    .expect("create admitted before retirement");
    let retired = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(10)),
        retire,
    )
    .await
    .expect("retire finishes")
    .expect("join")
    .expect("retire");
    assert!(retired.is_retired());
    assert!(f
        .store
        .get_script_in_workspace(&ws.id, "racing-script")
        .await
        .unwrap()
        .is_some());
}

/// Review regression (intentd#2605): a script mutation that passed the RPC
/// retired check while the workspace was still Archived, and resumes only
/// after `workspace.retire` returned (its closing mark dropped), is refused
/// under the definition lock BEFORE any durable write. Driven through the
/// script manager directly — the code behind the front-door gate — for
/// every gated script mutation: `run` of an archived command (no
/// `archivedAt` restoration, no `script:changed`) and of a service (the
/// "use script.start" warning must not bypass the refusal), `start`,
/// `restart`, `stop`, `remove`, restore and `create`. Nothing is written:
/// no run row, no definition change, no event.
#[intent_test_macros::daemon_test]
async fn script_mutations_after_retire_returns_are_refused_and_write_nothing() {
    let f = fixture().await;
    let ws = create_workspace(&f).await;
    let mut ids = Vec::new();
    for (name, mode) in [
        ("svc", intent_core::ScriptMode::Service),
        ("cmd", intent_core::ScriptMode::Command),
    ] {
        let v = f
            .svc
            .script_create(
                ws.id.clone(),
                intent_core::ScriptCreateParams {
                    name: name.to_string(),
                    command: "echo hi".to_string(),
                    mode,
                    purpose: Some(intent_core::ScriptPurpose::Saved),
                    ..Default::default()
                },
            )
            .await
            .expect("create");
        ids.push(v["id"].as_str().expect("script id").to_string());
    }
    let (svc_id, cmd_id) = (ids[0].clone(), ids[1].clone());
    let mgr = f.svc.script_manager();
    mgr.archive(&ws.id, vec![cmd_id.clone()], true)
        .await
        .expect("archive the command definition");
    let archived_at = f
        .store
        .get_script_in_workspace(&ws.id, &cmd_id)
        .await
        .unwrap()
        .expect("cmd row")
        .archived_at;
    assert!(archived_at.is_some(), "command definition archived");
    f.svc
        .retire_workspace(ws.id.clone(), None)
        .await
        .expect("retire");
    let mut sub = f.bus.subscribe(crate::SubscriptionFilter {
        event_types: vec![intent_core::events::SCRIPT_CHANGED.to_string()],
        workspace_id: Some(ws.id.0.clone()),
        ..Default::default()
    });

    let retired = |r: &intent_core::Result<serde_json::Value>| {
        matches!(r, Err(Error::WorkspaceRetired { .. }))
    };
    let run_cmd = mgr.run(&ws.id, &cmd_id, None, Some(5)).await;
    assert!(retired(&run_cmd), "run archived command: {run_cmd:?}");
    let run_svc = mgr.run(&ws.id, &svc_id, None, Some(5)).await;
    assert!(retired(&run_svc), "run service: {run_svc:?}");
    let start = mgr.start(&ws.id, &svc_id).await;
    assert!(retired(&start), "start: {start:?}");
    let restart = mgr.restart(&ws.id, &svc_id).await;
    assert!(retired(&restart), "restart: {restart:?}");
    let stop = mgr.stop(&ws.id, &svc_id).await;
    assert!(retired(&stop), "stop: {stop:?}");
    let remove = mgr.remove(&ws.id, &cmd_id).await;
    assert!(retired(&remove), "remove: {remove:?}");
    let restore = mgr.archive(&ws.id, vec![cmd_id.clone()], false).await;
    assert!(retired(&restore), "restore: {restore:?}");
    let created = mgr
        .create(
            ws.id.clone(),
            intent_core::ScriptCreateParams {
                name: "late".to_string(),
                command: "echo late".to_string(),
                mode: intent_core::ScriptMode::Command,
                purpose: Some(intent_core::ScriptPurpose::Saved),
                script_id: Some("late-script".to_string()),
                ..Default::default()
            },
        )
        .await;
    assert!(retired(&created), "create: {created:?}");

    let cmd_row = f
        .store
        .get_script_in_workspace(&ws.id, &cmd_id)
        .await
        .unwrap()
        .expect("cmd definition kept");
    assert_eq!(cmd_row.archived_at, archived_at, "archivedAt unchanged");
    assert!(f
        .store
        .get_script_in_workspace(&ws.id, &svc_id)
        .await
        .unwrap()
        .is_some());
    assert!(f
        .store
        .get_script_in_workspace(&ws.id, "late-script")
        .await
        .unwrap()
        .is_none());
    for id in &ids {
        assert!(
            f.store
                .latest_script_run(&ws.id, id)
                .await
                .unwrap()
                .is_none(),
            "no run row written for {id}"
        );
    }
    assert!(
        tokio::time::timeout(
            intentd_test_support::test_timeout(Duration::from_millis(300)),
            sub.recv(),
        )
        .await
        .is_err(),
        "no script:changed event"
    );
}

/// A `terminal.create` that passes its first checks while retire holds the
/// archive fence waits for it and is then refused: no PTY is spawned for a
/// workspace that retired in the meantime.
#[intent_test_macros::daemon_test]
async fn terminal_create_racing_retire_spawns_nothing() {
    let f = fixture().await;
    let ws = create_workspace(&f).await;
    let guard = f.svc.feature_archive_fence().acquire(&ws.id).await;
    let create = intent_core::spawn_daemon({
        let svc = f.svc.clone();
        let id = ws.id.clone();
        async move { svc.terminal_create(id, 80, 24, None, None, None).await }
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    flip_retired(&f, &ws.id).await;
    drop(guard);

    let err = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(10)),
        create,
    )
    .await
    .expect("create finishes")
    .expect("join")
    .unwrap_err();
    assert!(matches!(err, Error::WorkspaceRetired { .. }), "{err:?}");
    assert!(f.svc.pty().list_scope(ws.id.as_str()).is_empty());
}

/// `terminal.write` / `resize` / `kill`, addressed only by `terminalId`,
/// are refused with `workspace-retired` when the terminal's workspace is
/// retired, and the terminal is not touched.
#[intent_test_macros::daemon_test]
async fn terminal_id_mutations_refused_on_a_retired_workspace() {
    let f = fixture().await;
    let ws = create_workspace(&f).await;
    let created = f
        .svc
        .terminal_create(ws.id.clone(), 80, 24, None, None, None)
        .await
        .expect("terminal");
    let terminal_id = created["terminalId"]
        .as_str()
        .expect("terminal id")
        .to_string();
    flip_retired(&f, &ws.id).await;
    let is_retired = |e: &Error| matches!(e, Error::WorkspaceRetired { workspace_id } if *workspace_id == ws.id.0);

    let err = f
        .svc
        .terminal_write(terminal_id.clone(), "echo hi\n".into())
        .await
        .unwrap_err();
    assert!(is_retired(&err), "write: {err:?}");
    let err = f
        .svc
        .terminal_resize(terminal_id.clone(), 100, 30)
        .await
        .unwrap_err();
    assert!(is_retired(&err), "resize: {err:?}");
    let err = f.svc.terminal_kill(terminal_id.clone()).await.unwrap_err();
    assert!(is_retired(&err), "kill: {err:?}");
    assert_eq!(f.svc.pty().list_scope(ws.id.as_str()).len(), 1);
}

/// An archived workspace whose command finished but whose result could not
/// be persisted (a trigger refuses the `last_run` write), so the script still
/// holds an unsettled admitted run.
pub(crate) async fn archived_with_unsettled_run(f: &Fixture) -> (intent_core::Workspace, String) {
    let ws = create_workspace(f).await;
    let v = f
        .svc
        .script_create(
            ws.id.clone(),
            intent_core::ScriptCreateParams {
                name: "done".to_string(),
                command: "printf done".to_string(),
                mode: intent_core::ScriptMode::Command,
                purpose: Some(intent_core::ScriptPurpose::Saved),
                ..Default::default()
            },
        )
        .await
        .expect("create");
    let id = v["id"].as_str().expect("script id").to_string();
    sqlx::query(
        "CREATE TRIGGER refuse_result BEFORE UPDATE OF last_run ON script \
         BEGIN SELECT RAISE(FAIL,'result refused'); END",
    )
    .execute(f.store.write_pool())
    .await
    .unwrap();
    let out = f
        .svc
        .script_run(ws.id.clone(), id.clone(), None, Some(10))
        .await
        .expect("run");
    assert_eq!(out["exitCode"], 0, "{out}");
    f.svc
        .archive_workspace(ws.id.clone(), None)
        .await
        .expect("archive");
    (ws, id)
}

/// The retire flip waits for the scripts to be fully stopped AND settled:
/// asserts the row stays Archived (not Retired), the checkout is kept and
/// the run's result is still unsettled.
pub(crate) async fn assert_still_archived_and_unsettled(
    f: &Fixture,
    ws: &intent_core::Workspace,
    id: &str,
) {
    let row = f.store.get_workspace(&ws.id).await.unwrap();
    assert!(row.archived && !row.is_retired(), "{row:?}");
    assert!(std::path::Path::new(ws.worktree_path.as_deref().unwrap()).is_dir());
    let def = f
        .store
        .get_script_in_workspace(&ws.id, id)
        .await
        .unwrap()
        .expect("script row");
    assert!(def.last_run.is_none(), "result still unsettled: {def:?}");
}
