//! Workspace plain-folder column (`0177_workspace_plain_folder`, PROTOCOL
//! §5.1 "Plain-folder workspaces", `Workspace.plainFolder`): defaults to off,
//! written by insert only, read back on `get_workspace` and list rows, and
//! kept across a store reopen (daemon restart).

use super::*;
use intent_core::WorkspaceListFilter;

/// A fresh database carries the column, defaulting to 0 (off).
#[tokio::test]
async fn migration_adds_plain_folder_column_defaulting_off() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let id = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&id, "ws", false))
        .await
        .expect("insert");
    let raw: i64 = sqlx::query_scalar("SELECT plain_folder FROM workspace WHERE id = ?")
        .bind(&id.0)
        .fetch_one(store.read_pool())
        .await
        .expect("plain_folder column exists");
    assert_eq!(raw, 0);
    assert!(!store.get_workspace(&id).await.expect("get").plain_folder);
}

/// Insert persists the flag; get, list and a reopened store read it back,
/// and a full-row update — even from a snapshot saying `false` — never
/// changes it.
#[tokio::test]
async fn plain_folder_is_insert_only_and_survives_reopen() {
    let tmp = TempDb::new();
    let id = WorkspaceId::new();
    {
        let store = Store::open(&tmp.path).await.expect("open store");
        let mut ws = sample_workspace(&id, "ws", false);
        ws.plain_folder = true;
        store.insert_workspace(&ws).await.expect("insert");
        assert!(store.get_workspace(&id).await.expect("get").plain_folder);

        ws.plain_folder = false;
        ws.title = "renamed".to_string();
        store.update_workspace(&ws).await.expect("full-row write");
        let row = store.get_workspace(&id).await.expect("get");
        assert_eq!(row.title, "renamed", "the update itself applied");
        assert!(row.plain_folder, "a full-row write never clears the flag");
    }
    let reopened = Store::open(&tmp.path).await.expect("reopen store");
    assert!(
        reopened.get_workspace(&id).await.expect("get").plain_folder,
        "the flag survives a restart"
    );
    let listed = reopened
        .list_workspaces(WorkspaceListFilter::WITH_ARCHIVED)
        .await
        .expect("list");
    let row = listed.iter().find(|w| w.id == id).expect("listed");
    assert!(row.plain_folder, "kept on list rows");
}
