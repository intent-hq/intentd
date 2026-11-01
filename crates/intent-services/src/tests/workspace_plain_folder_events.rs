//! Plain-folder workspaces emit no git events and run no git or PR
//! background work (PROTOCOL §5.1 "Plain-folder workspaces": no
//! `changes:*`, `git:*`, `gitRoot:*`, `pr:*` or `prMonitor:*`). `file:*` and
//! `task:*` still reach agent subscriptions. Each git-only path is checked
//! against a git workspace control so a test cannot pass vacuously. The
//! watcher-driven half (file changes, `.git` metadata) lives in
//! `events::registry` tests; auto-commit lives in `auto_commit::tests`.

use std::time::Duration;

use intent_core::{
    now_iso, ActorType, AgentCreateExtra, AgentId, Error, EventActor, WorkspaceApi,
    WorkspaceGitRoot, WorkspaceGitRootId, WorkspaceGitRootSource, WorkspaceId,
};
use intent_store::{NewEvent, Store};
use serde_json::json;

use super::{test_registry_with_default_provider, test_tempdir, workspace, TempDb};
use crate::events::EventBus;
use crate::test_support::submodule_fixture::superproject_with_submodule;
use crate::Services;

struct Harness {
    _tmp: TempDb,
    svc: Services,
    bus: EventBus,
}

async fn harness() -> Harness {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let registry = test_registry_with_default_provider(&tmp);
    let svc = Services::new_isolated(store).with_settings_registry(registry);
    let bus = EventBus::new(svc.store().clone());
    let svc = svc.with_event_bus(bus.clone());
    Harness {
        _tmp: tmp,
        svc,
        bus,
    }
}

/// Insert a workspace row rooted at `path`, plain-folder or git. A
/// plain-folder row matches what `workspace.create { plainFolder: true }`
/// persists: the folder as `repositoryPath` only, no branch.
async fn insert_row(svc: &Services, path: &std::path::Path, plain: bool) -> WorkspaceId {
    let id = WorkspaceId::new();
    let mut ws = workspace(&id);
    let path = path.to_string_lossy().into_owned();
    if plain {
        ws.plain_folder = true;
        ws.branch = String::new();
        ws.repository_path = Some(path);
    } else {
        ws.worktree_path = Some(path.clone());
        ws.repository_path = Some(path);
        ws.repository_owner = Some("o".into());
        ws.repository_name = Some("r".into());
    }
    svc.store().insert_workspace(&ws).await.expect("insert ws");
    id
}

async fn events_of(svc: &Services, ws: &WorkspaceId, event_type: &str) -> usize {
    svc.store()
        .events_by_type(ws, event_type, 100)
        .await
        .expect("events")
        .len()
}

async fn create_agent(svc: &Services, ws: &WorkspaceId, name: &str) -> AgentId {
    let created = svc
        .agent_create_op(
            ws.clone(),
            Some(name.to_string()),
            Some("sonnet4.5".into()),
            None,
            None,
            None,
            false,
            AgentCreateExtra {
                provider: Some("auggie".into()),
                ..Default::default()
            },
        )
        .await
        .expect("create agent");
    AgentId::from(created["agent"]["id"].as_str().unwrap())
}

fn agent_event(ws: &WorkspaceId, actor: &AgentId, event_type: &str) -> NewEvent {
    NewEvent {
        workspace_id: ws.clone(),
        timestamp: now_iso(),
        event_type: event_type.to_string(),
        actor: EventActor {
            actor_type: ActorType::Agent,
            id: Some(actor.0.clone()),
            ..Default::default()
        },
        session_id: Some(actor.0.clone()),
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: json!({ "path": "notes.md", "noteId": "n1" }),
    }
}

/// `file:*` and `task:*` keep reaching agent subscriptions in a plain
/// folder: suppression is per event family, never workspace-wide.
#[intent_test_macros::daemon_test]
async fn agent_subscriptions_receive_file_and_task_events() {
    let h = harness().await;
    let dir = test_tempdir("intentd-plain-events-sub-");
    let ws = insert_row(&h.svc, dir.path(), true).await;
    let subscriber = create_agent(&h.svc, &ws, "Watcher").await;
    let worker = create_agent(&h.svc, &ws, "Worker").await;
    h.svc
        .agent_subscribe(
            ws.clone(),
            Some(subscriber.clone()),
            vec!["file:*".into(), "task:*".into()],
            None,
            Some(2000),
        )
        .await
        .expect("subscribe");

    for event_type in ["file:changed", "task:status-changed"] {
        h.bus
            .publish(&agent_event(&ws, &worker, event_type))
            .await
            .expect("publish");
    }

    let deadline =
        tokio::time::Instant::now() + intentd_test_support::test_timeout(Duration::from_secs(30));
    loop {
        let session = h
            .svc
            .store()
            .get_agent_session(&subscriber)
            .await
            .expect("subscriber session");
        let text = serde_json::to_string(&session.messages).unwrap();
        if text.contains("file:changed") && text.contains("task:status-changed") {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "subscriber never received file and task events: {text}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// `ws.git.registerRoot` (and any other caller) cannot add a git root to a
/// plain folder, so no `gitRoot:*` event is emitted.
#[intent_test_macros::daemon_test]
async fn git_root_registration_is_refused() {
    let h = harness().await;
    let dir = test_tempdir("intentd-plain-events-root-");
    let ws = insert_row(&h.svc, dir.path(), true).await;
    let git_ws = insert_row(&h.svc, dir.path(), false).await;
    let root_for = |ws: &WorkspaceId| WorkspaceGitRoot {
        id: WorkspaceGitRootId::new(),
        workspace_id: ws.clone(),
        path: dir.path().join("nested").to_string_lossy().into_owned(),
        source: WorkspaceGitRootSource::Agent,
        repo_owner: None,
        repo_name: None,
        registered_by_agent_ids: vec![],
        registered_commit_sha: None,
        pr_number: None,
        pr_url: None,
        pr_status: None,
        pull_requests: None,
        created_at: now_iso(),
        updated_at: now_iso(),
    };

    let err = h
        .svc
        .register_git_root(&root_for(&ws))
        .await
        .expect_err("plain folder refuses git roots");
    assert!(
        matches!(&err, Error::InvalidParams(m) if m.contains("plain folder")),
        "unexpected error: {err:?}"
    );
    assert!(h
        .svc
        .store()
        .list_workspace_git_roots(&ws)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(events_of(&h.svc, &ws, "gitRoot:registered").await, 0);

    h.svc
        .register_git_root(&root_for(&git_ws))
        .await
        .expect("git workspace registers roots");
    assert_eq!(events_of(&h.svc, &git_ws, "gitRoot:registered").await, 1);
}

/// The background git-root sweep does not auto-detect submodules (or touch
/// roots at all) in a plain folder that happens to contain them.
#[intent_test_macros::daemon_test]
async fn git_root_sweep_skips_plain_folder() {
    let h = harness().await;
    let dir = test_tempdir("intentd-plain-events-sweep-");
    let (sup, _origin) = superproject_with_submodule(dir.path());
    let ws_id = insert_row(&h.svc, &sup, true).await;
    let git_id = insert_row(&h.svc, &sup, false).await;

    for id in [&ws_id, &git_id] {
        let ws = h.svc.store().get_workspace(id).await.unwrap();
        h.svc.sweep_workspace_git_roots(&ws, None, 60, true).await;
    }

    assert!(h
        .svc
        .store()
        .list_workspace_git_roots(&ws_id)
        .await
        .unwrap()
        .is_empty());
    assert_eq!(events_of(&h.svc, &ws_id, "gitRoot:registered").await, 0);
    assert_eq!(
        events_of(&h.svc, &git_id, "gitRoot:registered").await,
        1,
        "control: a git workspace auto-detects the submodule"
    );
}

/// PR monitors cannot be registered in a plain folder; the refusal comes
/// before any forge call and persists nothing.
#[intent_test_macros::daemon_test]
async fn pr_monitor_registration_is_refused() {
    let h = harness().await;
    let dir = test_tempdir("intentd-plain-events-monitor-");
    let ws = insert_row(&h.svc, dir.path(), true).await;
    let agent = create_agent(&h.svc, &ws, "Builder").await;

    let err = h
        .svc
        .pr_monitor_try_register(&ws, &agent, "o", "r", 7)
        .await
        .expect_err("plain folder refuses PR monitors");
    assert!(
        matches!(&err, Error::InvalidParams(m) if m.contains("plain folder")),
        "unexpected error: {err:?}"
    );
    assert_eq!(events_of(&h.svc, &ws, "prMonitor:registered").await, 0);
}

/// Automatic and explicit PR refreshes skip a plain folder before resolving
/// a forge, even when its row carries PR fields (without the gate the
/// refresh reaches the forge and fails on the unknown `o/r` PR).
#[intent_test_macros::daemon_test]
async fn pr_refresh_skips_plain_folder() {
    let h = harness().await;
    let dir = test_tempdir("intentd-plain-events-pr-");
    let ws = insert_row(&h.svc, dir.path(), true).await;
    let mut row = h.svc.store().get_workspace(&ws).await.unwrap();
    row.repository_owner = Some("o".into());
    row.repository_name = Some("r".into());
    row.pr_number = Some(3);
    h.svc.store().update_workspace(&row).await.unwrap();

    let outcome = h
        .svc
        .refresh_workspace_pr(&ws)
        .await
        .expect("plain folder refresh is a skip, not a forge error");
    assert_eq!(outcome, crate::pr_ops::PrRefreshOutcome::Skipped);
    h.svc.refresh_prs_on_agent_idle(&ws).await;
    for event_type in ["pr:linked", "pr:updated", "pr:unlinked"] {
        assert_eq!(events_of(&h.svc, &ws, event_type).await, 0, "{event_type}");
    }
}

/// The agent-lock and line-metrics publishers stay silent for a plain
/// folder; a git workspace still gets both `changes:*` events.
#[intent_test_macros::daemon_test]
async fn agent_locks_and_metrics_publish_no_changes_events() {
    let h = harness().await;
    let dir = test_tempdir("intentd-plain-events-changes-");
    let ws = insert_row(&h.svc, dir.path(), true).await;
    let git_ws = insert_row(&h.svc, dir.path(), false).await;

    for id in [&ws, &git_ws] {
        h.svc.refresh_agent_locks(id, None).await;
        h.svc.recompute_and_publish_metrics(id).await;
    }

    assert_eq!(events_of(&h.svc, &ws, "changes:agent-locks").await, 0);
    assert_eq!(events_of(&h.svc, &ws, "changes:metrics-changed").await, 0);
    assert_eq!(events_of(&h.svc, &git_ws, "changes:agent-locks").await, 1);
    assert_eq!(
        events_of(&h.svc, &git_ws, "changes:metrics-changed").await,
        1
    );
}
