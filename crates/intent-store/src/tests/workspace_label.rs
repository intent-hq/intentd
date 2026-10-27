//! Workspace label column (`0175_workspace_label`, PROTOCOL §9.1
//! `Workspace.label`): nullable, written by insert and the full-row update,
//! read back on `get_workspace` and list rows.

use super::*;
use intent_core::WorkspaceListFilter;

/// A fresh database carries the nullable `label` column, defaulting to NULL.
#[tokio::test]
async fn migration_adds_nullable_label_column() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let id = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&id, "ws", false))
        .await
        .expect("insert");
    let raw: Option<String> = sqlx::query_scalar("SELECT label FROM workspace WHERE id = ?")
        .bind(&id.0)
        .fetch_one(store.read_pool())
        .await
        .expect("label column exists");
    assert_eq!(raw, None);
    assert_eq!(store.get_workspace(&id).await.expect("get").label, None);
}

/// Insert, full-row update (set / replace / clear) and list rows all
/// round-trip the label.
#[tokio::test]
async fn label_round_trips_through_insert_update_and_list() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let id = WorkspaceId::new();
    let mut ws = sample_workspace(&id, "ws", false);
    ws.label = Some("pinned".to_string());
    store.insert_workspace(&ws).await.expect("insert");
    assert_eq!(
        store
            .get_workspace(&id)
            .await
            .expect("get")
            .label
            .as_deref(),
        Some("pinned")
    );

    ws.label = Some("For Later".to_string());
    store.update_workspace(&ws).await.expect("replace");
    assert_eq!(
        store
            .get_workspace(&id)
            .await
            .expect("get")
            .label
            .as_deref(),
        Some("For Later")
    );
    let listed = store
        .list_workspaces(WorkspaceListFilter::WITH_ARCHIVED)
        .await
        .expect("list");
    let row = listed.iter().find(|w| w.id == id).expect("listed");
    assert_eq!(row.label.as_deref(), Some("For Later"));

    ws.label = None;
    store.update_workspace(&ws).await.expect("clear");
    assert_eq!(store.get_workspace(&id).await.expect("get").label, None);
}
