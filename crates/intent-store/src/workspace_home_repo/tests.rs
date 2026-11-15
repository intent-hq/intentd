use super::*;
use intent_core::{now_iso, Workspace};

async fn open() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("homes.db")).await.unwrap();
    for id in ["ws-1", "ws-2"] {
        store
            .insert_workspace(&Workspace {
                id: WorkspaceId::from(id),
                title: id.into(),
                ..Default::default()
            })
            .await
            .unwrap();
    }
    (dir, store)
}

fn home(id: &str, node: &str) -> WorkspaceHome {
    WorkspaceHome {
        home_id: HomeId::from(id),
        node_id: Some(node.into()),
        cloud_machine_id: None,
        name: format!("{node} name"),
        state: HomeState::Attached,
        since: now_iso(),
        last_backup_at: None,
        backup_warning: false,
    }
}

async fn failing_since(store: &Store, workspace: &str) -> Option<String> {
    sqlx::query_scalar("SELECT backup_failing_since FROM workspace_home WHERE workspace_id = ?")
        .bind(workspace)
        .fetch_one(store.read_pool())
        .await
        .unwrap()
}

/// A home is stored per workspace, listed in one query and by machine; a
/// state or backup update names the home it is about, so one for an older
/// `homeId` changes nothing.
#[tokio::test]
async fn homes_are_kept_per_workspace_and_updated_by_home_id() {
    let (_dir, store) = open().await;
    let ws1 = WorkspaceId::from("ws-1");
    let ws2 = WorkspaceId::from("ws-2");
    store
        .set_workspace_home(&ws1, &home("h1", "node-a"))
        .await
        .unwrap();
    store
        .set_workspace_home(&ws2, &home("h2", "node-b"))
        .await
        .unwrap();
    assert_eq!(store.list_workspace_homes().await.unwrap().len(), 2);
    let on_a = store.homes_on_machine("node-a").await.unwrap();
    assert_eq!(on_a.len(), 1);
    assert_eq!(
        (&on_a[0].0, &on_a[0].1.home_id),
        (&ws1, &HomeId::from("h1"))
    );

    let stale = HomeId::from("h0");
    assert!(!store
        .set_workspace_home_state(&ws1, &stale, HomeState::Detached, &now_iso())
        .await
        .unwrap());
    assert!(!store
        .record_workspace_home_backup(&ws1, &stale, "node-a", &now_iso())
        .await
        .unwrap());
    let h1 = HomeId::from("h1");
    assert!(store
        .set_workspace_home_state(&ws1, &h1, HomeState::Detached, &now_iso())
        .await
        .unwrap());
    assert!(
        !store
            .set_workspace_home_state(&ws1, &h1, HomeState::Detached, &now_iso())
            .await
            .unwrap(),
        "an unchanged state is not a change"
    );
    let at = now_iso();
    assert!(
        !store
            .record_workspace_home_backup(&ws1, &h1, "node-b", &at)
            .await
            .unwrap(),
        "only the home's machine reports its backups"
    );
    assert!(store
        .record_workspace_home_backup(&ws1, &h1, "node-a", &at)
        .await
        .unwrap());
    let got = store.get_workspace_home(&ws1).await.unwrap().unwrap();
    assert_eq!(got.state, HomeState::Detached);
    assert_eq!(got.last_backup_at.as_deref(), Some(at.as_str()));
}

/// Backups failing for 15 minutes raise `backupWarning`; the next backup
/// clears it, and a new `homeId` starts without a failing streak.
#[tokio::test]
async fn backup_warning_follows_the_failing_streak() {
    let (_dir, store) = open().await;
    let ws = WorkspaceId::from("ws-1");
    let h1 = HomeId::from("h1");
    store
        .set_workspace_home(&ws, &home("h1", "node-a"))
        .await
        .unwrap();
    let long_ago = "2026-01-01T00:00:00Z";
    store
        .record_workspace_home_backup_failure(&ws, &h1, long_ago)
        .await
        .unwrap();
    store
        .record_workspace_home_backup_failure(&ws, &h1, &now_iso())
        .await
        .unwrap();
    assert_eq!(
        failing_since(&store, "ws-1").await.as_deref(),
        Some(long_ago)
    );
    assert!(
        store
            .get_workspace_home(&ws)
            .await
            .unwrap()
            .unwrap()
            .backup_warning
    );

    store
        .set_workspace_home(&ws, &home("h1", "node-a"))
        .await
        .unwrap();
    assert!(
        store
            .get_workspace_home(&ws)
            .await
            .unwrap()
            .unwrap()
            .backup_warning,
        "rewriting the same home keeps its streak"
    );
    store
        .record_workspace_home_backup(&ws, &h1, "node-a", &now_iso())
        .await
        .unwrap();
    assert!(
        !store
            .get_workspace_home(&ws)
            .await
            .unwrap()
            .unwrap()
            .backup_warning
    );

    store
        .record_workspace_home_backup_failure(&ws, &h1, long_ago)
        .await
        .unwrap();
    store
        .set_workspace_home(&ws, &home("h2", "node-b"))
        .await
        .unwrap();
    assert_eq!(failing_since(&store, "ws-1").await, None);

    store
        .record_workspace_home_backup_failure(&ws, &HomeId::from("h2"), &now_iso())
        .await
        .unwrap();
    assert!(
        !store
            .get_workspace_home(&ws)
            .await
            .unwrap()
            .unwrap()
            .backup_warning,
        "a fresh failure is not yet a warning"
    );
}

/// Deleting the workspace removes its home.
#[tokio::test]
async fn a_deleted_workspace_has_no_home() {
    let (_dir, store) = open().await;
    let ws = WorkspaceId::from("ws-1");
    store
        .set_workspace_home(&ws, &home("h1", "node-a"))
        .await
        .unwrap();
    store.delete_workspace(&ws).await.unwrap();
    assert!(store.list_workspace_homes().await.unwrap().is_empty());
}

/// `backupWarning` shows exactly 15 minutes into a failing streak: the
/// time left counts down from the streak's start, is zero from then on,
/// and an unreadable start never warns.
#[test]
fn backup_warning_shows_fifteen_minutes_into_a_streak() {
    let since = "2026-11-12T09:00:00Z";
    let at = |text: &str| parse_iso(text).unwrap();
    assert_eq!(
        backup_warning_in(since, at("2026-11-12T09:00:00Z")),
        Some(time::Duration::minutes(15))
    );
    assert_eq!(
        backup_warning_in(since, at("2026-11-12T09:14:59Z")),
        Some(time::Duration::seconds(1))
    );
    assert_eq!(
        backup_warning_in(since, at("2026-11-12T09:15:00Z")),
        Some(time::Duration::ZERO)
    );
    assert_eq!(
        backup_warning_in(since, at("2026-11-13T09:00:00Z")),
        Some(time::Duration::ZERO)
    );
    assert!(!warning(Some(since), at("2026-11-12T09:14:59Z")));
    assert!(warning(Some(since), at("2026-11-12T09:15:00Z")));
    assert!(!warning(None, at("2026-11-13T09:00:00Z")));
    assert_eq!(backup_warning_in("not a time", at(since)), None);
    assert!(!warning(Some("not a time"), at(since)));
}

/// A failure answers the streak's first failure, and nothing for a home
/// that is not the workspace's; a state move happens only from the state
/// named, so a detach never ends a move.
#[tokio::test]
async fn failures_answer_their_streak_and_state_moves_name_their_start() {
    let (_dir, store) = open().await;
    let ws = WorkspaceId::from("ws-1");
    let h1 = HomeId::from("h1");
    store
        .set_workspace_home(&ws, &home("h1", "node-a"))
        .await
        .unwrap();
    let first = "2026-11-12T09:00:00Z";
    assert_eq!(
        store
            .record_workspace_home_backup_failure(&ws, &h1, first)
            .await
            .unwrap()
            .as_deref(),
        Some(first)
    );
    assert_eq!(
        store
            .record_workspace_home_backup_failure(&ws, &h1, "2026-11-12T09:05:00Z")
            .await
            .unwrap()
            .as_deref(),
        Some(first)
    );
    assert_eq!(
        store
            .record_workspace_home_backup_failure(&ws, &HomeId::from("h0"), first)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        store
            .workspace_home_backup_failing_since(&ws, &h1)
            .await
            .unwrap()
            .as_deref(),
        Some(first)
    );

    let since = "2026-11-12T10:00:00Z";
    assert!(store
        .move_workspace_home_state(&ws, &h1, HomeState::Attached, HomeState::Detached, since)
        .await
        .unwrap());
    assert!(!store
        .move_workspace_home_state(&ws, &h1, HomeState::Attached, HomeState::Detached, since)
        .await
        .unwrap());
    let got = store.get_workspace_home(&ws).await.unwrap().unwrap();
    assert_eq!(
        (got.state, got.since.as_str()),
        (HomeState::Detached, since)
    );
    store
        .set_workspace_home_state(&ws, &h1, HomeState::Moving, since)
        .await
        .unwrap();
    assert!(!store
        .move_workspace_home_state(&ws, &h1, HomeState::Attached, HomeState::Detached, since)
        .await
        .unwrap());
    assert!(!store
        .move_workspace_home_state(
            &ws,
            &HomeId::from("h0"),
            HomeState::Moving,
            HomeState::Attached,
            since
        )
        .await
        .unwrap());
}

/// `autoStart` is claimed once per new checkout, by the `setupFinished`
/// record that reported it (its lease's journal and sequence), and the
/// claim is durable: after a restart (the store reopened) a replay of the
/// same record claims nothing. A later checkout's record (a higher
/// sequence on the lease, or a fresh lease's journal) claims again; a
/// record of another home, or from another machine, claims nothing.
#[tokio::test]
async fn auto_start_is_claimed_once_per_setup_record_across_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("homes.db");
    let ws1 = WorkspaceId::from("ws-1");
    let h1 = HomeId::from("h1");
    {
        let store = Store::open(&path).await.unwrap();
        store
            .insert_workspace(&Workspace {
                id: ws1.clone(),
                title: "ws-1".into(),
                ..Default::default()
            })
            .await
            .unwrap();
        store
            .set_workspace_home(&ws1, &home("h1", "node-a"))
            .await
            .unwrap();
        assert!(store
            .claim_workspace_home_auto_start(&ws1, &h1, "node-a", "lease-1", 10)
            .await
            .unwrap());
        assert!(!store
            .claim_workspace_home_auto_start(&ws1, &h1, "node-a", "lease-1", 10)
            .await
            .unwrap());
    }
    // The orchestrator restarts: the record, ingested but not acked yet,
    // is replayed.
    let store = Store::open(&path).await.unwrap();
    let claim = |lease: &'static str, seq: u64| {
        let store = &store;
        let ws1 = &ws1;
        let h1 = &h1;
        async move {
            store
                .claim_workspace_home_auto_start(ws1, h1, "node-a", lease, seq)
                .await
                .unwrap()
        }
    };
    assert!(!claim("lease-1", 10).await, "a replay starts nothing");
    assert!(!claim("lease-1", 9).await, "an older record starts nothing");
    assert!(claim("lease-1", 20).await, "a new checkout on the lease");
    assert!(!claim("lease-1", 20).await);
    assert!(claim("lease-2", 3).await, "a fresh lease's journal");
    assert!(!claim("lease-2", 3).await);
    assert!(!store
        .claim_workspace_home_auto_start(&ws1, &HomeId::from("h0"), "node-a", "lease-2", 50)
        .await
        .unwrap());
    assert!(!store
        .claim_workspace_home_auto_start(&ws1, &h1, "node-b", "lease-2", 50)
        .await
        .unwrap());
    // A new home starts unclaimed, even on the same lease.
    store
        .set_workspace_home(&ws1, &home("h2", "node-a"))
        .await
        .unwrap();
    assert!(store
        .claim_workspace_home_auto_start(&ws1, &HomeId::from("h2"), "node-a", "lease-2", 3)
        .await
        .unwrap());
}

fn cloud_home(id: &str) -> WorkspaceHome {
    WorkspaceHome {
        node_id: None,
        cloud_machine_id: Some("e2b".into()),
        ..home(id, "unused")
    }
}

async fn state_of(store: &Store, workspace: &WorkspaceId) -> (HomeState, Option<String>) {
    let home = store.get_workspace_home(workspace).await.unwrap().unwrap();
    (home.state, home.node_id)
}

/// A cloud home is claimed by one sandbox only; a start marks it starting
/// only while no sandbox holds it (a start that lost the race never marks
/// a running home starting); a failed start detaches it unless another
/// sandbox holds it; a restart resets a home left starting with no sandbox,
/// and leaves one a sandbox holds for its reopen.
#[tokio::test]
async fn a_cloud_home_is_claimed_once_and_only_an_unheld_one_starts() {
    let (_dir, store) = open().await;
    let ws1 = WorkspaceId::from("ws-1");
    let ws2 = WorkspaceId::from("ws-2");
    let h1 = HomeId::from("h1");
    let h2 = HomeId::from("h2");
    store
        .set_workspace_home(&ws1, &cloud_home("h1"))
        .await
        .unwrap();
    store
        .set_workspace_home(&ws2, &cloud_home("h2"))
        .await
        .unwrap();

    assert!(store
        .mark_cloud_home_starting(&ws1, &h1, &now_iso())
        .await
        .unwrap());
    assert!(!store
        .claim_cloud_home(&ws1, &h1, "other-entry", "m1")
        .await
        .unwrap());
    assert!(store
        .claim_cloud_home(&ws1, &h1, "e2b", "m1")
        .await
        .unwrap());
    assert!(!store
        .claim_cloud_home(&ws1, &h1, "e2b", "m2")
        .await
        .unwrap());
    // The losing start fails: the home keeps the winner's sandbox.
    assert!(!store
        .detach_cloud_home(&ws1, &h1, Some("m2"), &now_iso())
        .await
        .unwrap());
    assert!(!store
        .detach_cloud_home(&ws1, &h1, None, &now_iso())
        .await
        .unwrap());
    assert!(store
        .move_workspace_home_state(
            &ws1,
            &h1,
            HomeState::Starting,
            HomeState::Attached,
            &now_iso()
        )
        .await
        .unwrap());
    assert!(!store
        .mark_cloud_home_starting(&ws1, &h1, &now_iso())
        .await
        .unwrap());
    assert_eq!(
        state_of(&store, &ws1).await,
        (HomeState::Attached, Some("m1".into()))
    );

    // The winner's own failure detaches the home and frees it.
    assert!(store
        .detach_cloud_home(&ws1, &h1, Some("m1"), &now_iso())
        .await
        .unwrap());
    assert_eq!(state_of(&store, &ws1).await, (HomeState::Detached, None));
    assert!(store
        .mark_cloud_home_starting(&ws1, &h1, &now_iso())
        .await
        .unwrap());
    assert!(store
        .claim_cloud_home(&ws1, &h1, "e2b", "m3")
        .await
        .unwrap());

    // ws-1 is starting on m3, ws-2 starting with no sandbox: a restart
    // resets only ws-2.
    assert!(store
        .mark_cloud_home_starting(&ws2, &h2, &now_iso())
        .await
        .unwrap());
    let reset = store.reset_unstarted_cloud_homes(&now_iso()).await.unwrap();
    assert_eq!(reset, vec![ws2.clone()]);
    assert_eq!(state_of(&store, &ws2).await, (HomeState::Attached, None));
    assert_eq!(
        state_of(&store, &ws1).await,
        (HomeState::Starting, Some("m3".into()))
    );
}
