use super::*;
use intent_core::node_link::{HomeStopReason, HomeStopRecord};
use intent_core::{now_iso, HomeState, Workspace, WorkspaceHome};

async fn open() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("rebuild.db")).await.unwrap();
    for id in ["ws-1", "ws-2", "ws-3"] {
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

fn home(id: &str, node: &str, cloud: bool, state: HomeState) -> WorkspaceHome {
    WorkspaceHome {
        home_id: HomeId::from(id),
        node_id: Some(node.into()),
        cloud_machine_id: cloud.then(|| "e2b".to_owned()),
        name: "E2B".into(),
        state,
        since: now_iso(),
        last_backup_at: Some("2026-11-15T10:00:00Z".into()),
        backup_warning: false,
    }
}

/// A deleted sandbox's attached cloud home gets a new `homeId` with no
/// machine (still attached, its last backup kept), and the stops the
/// machine owed go; a starting cloud home is left to its start, and homes
/// on other machines are untouched. The rebuild names only the new home,
/// and finishes once.
#[tokio::test]
async fn a_deleted_sandbox_s_home_is_rebuilt_under_a_new_home_id() {
    let (_dir, store) = open().await;
    let (ws1, ws2, ws3) = (
        WorkspaceId::from("ws-1"),
        WorkspaceId::from("ws-2"),
        WorkspaceId::from("ws-3"),
    );
    store
        .set_workspace_home(&ws1, &home("h1", "m-gone", true, HomeState::Attached))
        .await
        .unwrap();
    store
        .set_workspace_home(&ws2, &home("h2", "m-gone", true, HomeState::Starting))
        .await
        .unwrap();
    store
        .set_workspace_home(&ws3, &home("h3", "m-other", true, HomeState::Attached))
        .await
        .unwrap();
    let now = now_iso();
    store
        .record_home_stop(&HomeStopRecord::new(
            ws1.clone(),
            HomeId::from("h0"),
            "m-gone".into(),
            HomeStopReason::Moved,
            &now,
        ))
        .await
        .unwrap();

    let rebuilt = store
        .rebuild_cloud_homes_on_machine("m-gone", &now)
        .await
        .unwrap();
    assert_eq!(rebuilt.len(), 1, "{rebuilt:?}");
    let renewed = &rebuilt[0];
    assert_eq!(renewed.workspace_id, ws1);
    assert_eq!(renewed.from_home_id, HomeId::from("h1"));
    let current = store.get_workspace_home(&ws1).await.unwrap().unwrap();
    assert_eq!(current.home_id, renewed.home_id);
    assert_ne!(current.home_id, HomeId::from("h1"));
    assert_eq!(current.node_id, None);
    assert_eq!(current.state, HomeState::Attached);
    assert_eq!(current.cloud_machine_id.as_deref(), Some("e2b"));
    assert_eq!(
        current.last_backup_at.as_deref(),
        Some("2026-11-15T10:00:00Z")
    );
    assert!(store.list_home_stops("m-gone").await.unwrap().is_empty());
    let starting = store.get_workspace_home(&ws2).await.unwrap().unwrap();
    assert_eq!(starting.home_id, HomeId::from("h2"));
    assert_eq!(starting.node_id.as_deref(), Some("m-gone"));
    let other = store.get_workspace_home(&ws3).await.unwrap().unwrap();
    assert_eq!(other.node_id.as_deref(), Some("m-other"));

    assert_eq!(
        store.home_rebuild_of(&ws1, &current.home_id).await.unwrap(),
        Some(HomeId::from("h1"))
    );
    assert_eq!(
        store
            .home_rebuild_of(&ws1, &HomeId::from("h1"))
            .await
            .unwrap(),
        None
    );
    assert!(!store
        .finish_home_rebuild(&ws1, &HomeId::from("h1"))
        .await
        .unwrap());
    assert!(store
        .finish_home_rebuild(&ws1, &current.home_id)
        .await
        .unwrap());
    assert!(!store
        .finish_home_rebuild(&ws1, &current.home_id)
        .await
        .unwrap());
    assert_eq!(
        store.home_rebuild_of(&ws1, &current.home_id).await.unwrap(),
        None
    );
}
