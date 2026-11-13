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
