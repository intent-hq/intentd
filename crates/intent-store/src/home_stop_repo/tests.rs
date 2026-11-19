use super::*;
use intent_core::{now_iso, HomeState, Workspace, WorkspaceHome};

async fn open() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("stops.db")).await.unwrap();
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
        setup: None,
    }
}

/// A switch replaces the workspace's home and records a `moved` stop of the
/// old one in the same write; the stop is listed for the old machine and
/// goes only with its own ack.
#[tokio::test]
async fn a_switch_records_a_moved_stop_that_goes_with_its_ack() {
    let (_dir, store) = open().await;
    let ws = WorkspaceId::from("ws-1");
    store
        .set_workspace_home(&ws, &home("h1", "node-a"))
        .await
        .unwrap();
    let stop = store
        .switch_workspace_home(&ws, &home("h2", "node-b"), "2026-11-14T14:01:07Z")
        .await
        .unwrap()
        .expect("the old home gets a stop");
    assert_eq!(stop.home_id, HomeId::from("h1"));
    assert_eq!(stop.node_id, "node-a");
    assert_eq!(stop.reason, HomeStopReason::Moved);
    assert_eq!(stop.orphaned_at.as_deref(), Some("20261114T140107Z"));
    let current = store.get_workspace_home(&ws).await.unwrap().unwrap();
    assert_eq!(current.home_id, HomeId::from("h2"));
    assert_eq!(
        store.list_home_stops("node-a").await.unwrap(),
        vec![stop.clone()]
    );
    assert!(store.list_home_stops("node-b").await.unwrap().is_empty());
    // Another stop id (an older resend's ack) does not remove it.
    assert!(store
        .finish_home_stop(&stop.home_id, "another")
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        store
            .finish_home_stop(&stop.home_id, &stop.stop_id)
            .await
            .unwrap(),
        Some(stop.clone())
    );
    assert!(store.get_home_stop(&stop.home_id).await.unwrap().is_none());
    // A lost ack's resend is answered again: a second ack is a no-op.
    assert!(store
        .finish_home_stop(&stop.home_id, &stop.stop_id)
        .await
        .unwrap()
        .is_none());
}

/// A switch to the same `homeId`, or from no home, records nothing.
#[tokio::test]
async fn a_switch_without_an_old_home_records_no_stop() {
    let (_dir, store) = open().await;
    let ws = WorkspaceId::from("ws-1");
    assert!(store
        .switch_workspace_home(&ws, &home("h1", "node-a"), &now_iso())
        .await
        .unwrap()
        .is_none());
    assert!(store
        .switch_workspace_home(&ws, &home("h1", "node-a"), &now_iso())
        .await
        .unwrap()
        .is_none());
    assert!(store.list_home_stops("node-a").await.unwrap().is_empty());
}

/// Archiving a workspace retires its home in the archive's own write: the
/// home row goes and a `retired` stop takes its place.
#[tokio::test]
async fn archiving_retires_the_home() {
    let (_dir, store) = open().await;
    let ws = WorkspaceId::from("ws-1");
    store
        .set_workspace_home(&ws, &home("h1", "node-a"))
        .await
        .unwrap();
    store
        .archive_workspace_detaching_guests(&ws, &now_iso())
        .await
        .unwrap();
    assert!(store.get_workspace_home(&ws).await.unwrap().is_none());
    let stops = store.list_home_stops("node-a").await.unwrap();
    assert_eq!(stops.len(), 1);
    assert_eq!(stops[0].home_id, HomeId::from("h1"));
    assert_eq!(stops[0].workspace_id, ws);
    assert_eq!(stops[0].reason, HomeStopReason::Retired);
    assert_eq!(stops[0].orphaned_at, None);
    // The other workspace keeps its home.
    let other = WorkspaceId::from("ws-2");
    store
        .set_workspace_home(&other, &home("h2", "node-a"))
        .await
        .unwrap();
    assert!(store.get_workspace_home(&other).await.unwrap().is_some());
}

/// Deleting a workspace retires its home, and turns a pending `moved` stop
/// of an older home into a `retired` one: nothing of a deleted workspace is
/// pushed.
#[tokio::test]
async fn deleting_retires_the_home_and_its_moved_stops() {
    let (_dir, store) = open().await;
    let ws = WorkspaceId::from("ws-1");
    store
        .set_workspace_home(&ws, &home("h1", "node-a"))
        .await
        .unwrap();
    let moved = store
        .switch_workspace_home(&ws, &home("h2", "node-b"), &now_iso())
        .await
        .unwrap()
        .unwrap();
    store.delete_workspace(&ws).await.unwrap();
    let old = store.get_home_stop(&moved.home_id).await.unwrap().unwrap();
    assert_eq!(old.reason, HomeStopReason::Retired);
    assert_eq!(old.orphaned_at, None);
    assert_eq!(old.stop_id, moved.stop_id);
    let current = store
        .get_home_stop(&HomeId::from("h2"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.reason, HomeStopReason::Retired);
    assert_eq!(current.node_id, "node-b");
}

/// A recorded stop keeps the first record of a home; removing the machine
/// drops its stops only.
#[tokio::test]
async fn recording_keeps_the_first_stop_and_machine_removal_drops_them() {
    let (_dir, store) = open().await;
    let first = HomeStopRecord::new(
        WorkspaceId::from("ws-1"),
        HomeId::from("h1"),
        "node-a".into(),
        HomeStopReason::Moved,
        &now_iso(),
    );
    assert_eq!(store.record_home_stop(&first).await.unwrap(), first);
    let again = HomeStopRecord::new(
        WorkspaceId::from("ws-1"),
        HomeId::from("h1"),
        "node-a".into(),
        HomeStopReason::Retired,
        &now_iso(),
    );
    assert_eq!(store.record_home_stop(&again).await.unwrap(), first);
    let other = HomeStopRecord::new(
        WorkspaceId::from("ws-2"),
        HomeId::from("h2"),
        "node-b".into(),
        HomeStopReason::Retired,
        &now_iso(),
    );
    store.record_home_stop(&other).await.unwrap();
    assert_eq!(
        store.delete_home_stops_on_machine("node-a").await.unwrap(),
        1
    );
    assert!(store.list_home_stops("node-a").await.unwrap().is_empty());
    assert_eq!(store.list_home_stops("node-b").await.unwrap(), vec![other]);
}
