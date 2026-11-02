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

    // The schema itself: NOT NULL with DEFAULT 0, so rows written by an older
    // daemon (which never names the column) read back as off.
    let (notnull, default): (i64, Option<String>) = sqlx::query_as(
        "SELECT \"notnull\", dflt_value FROM pragma_table_info('workspace') WHERE name = 'plain_folder'",
    )
    .fetch_one(store.read_pool())
    .await
    .expect("plain_folder in table_info");
    assert_eq!(notnull, 1, "plain_folder is NOT NULL");
    assert_eq!(default.as_deref(), Some("0"), "plain_folder defaults to 0");
    let legacy = WorkspaceId::new();
    sqlx::query(
        "INSERT INTO workspace (id, title, branch, status, created_at, updated_at) \
         SELECT ?, title, branch, status, created_at, updated_at FROM workspace WHERE id = ?",
    )
    .bind(&legacy.0)
    .bind(&id.0)
    .execute(store.write_pool())
    .await
    .expect("insert without naming plain_folder");
    assert!(
        !store
            .get_workspace(&legacy)
            .await
            .expect("get legacy")
            .plain_folder,
        "an insert that omits the column reads back as off"
    );
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

/// `plain_folder_paths` lists live and archived plain folders and keeps a
/// deleted plain folder via its tombstone (`0178`); git rows and their
/// tombstones contribute nothing.
#[tokio::test]
async fn plain_folder_paths_include_deleted_plain_folders() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let plain_row = |id: &WorkspaceId, path: &str| {
        let mut ws = sample_workspace(id, "plain", false);
        ws.plain_folder = true;
        ws.branch = String::new();
        ws.worktree_path = None;
        ws.repository_path = Some(path.to_string());
        ws
    };
    let live = WorkspaceId::new();
    let archived = WorkspaceId::new();
    let deleted = WorkspaceId::new();
    let git = WorkspaceId::new();
    store
        .insert_workspace(&plain_row(&live, "/u/live"))
        .await
        .expect("live");
    store
        .insert_workspace(&plain_row(&archived, "/u/archived"))
        .await
        .expect("archived");
    store
        .archive_workspace_detaching_guests(&archived, &intent_core::now_iso())
        .await
        .expect("archive");
    store
        .insert_workspace(&plain_row(&deleted, "/u/deleted"))
        .await
        .expect("deleted");
    let mut git_row = sample_workspace(&git, "git", false);
    git_row.repository_path = Some("/u/repo".to_string());
    store.insert_workspace(&git_row).await.expect("git");
    store
        .delete_workspace(&deleted)
        .await
        .expect("delete plain");
    store.delete_workspace(&git).await.expect("delete git");

    let mut paths = store.plain_folder_paths().await.expect("paths");
    paths.sort();
    assert_eq!(paths, vec!["/u/archived", "/u/deleted", "/u/live"]);
    let tombstone: Option<String> =
        sqlx::query_scalar("SELECT plain_folder_path FROM deleted_workspace_id WHERE id = ?")
            .bind(&git.0)
            .fetch_one(store.read_pool())
            .await
            .expect("git tombstone");
    assert_eq!(tombstone, None, "a git tombstone records no folder");
}
