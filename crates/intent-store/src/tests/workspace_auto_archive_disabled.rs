//! Workspace auto-archive opt-out column (`0176_workspace_auto_archive_disabled`,
//! PROTOCOL §5.1 `Workspace.autoArchiveDisabled`): defaults to off, written
//! by insert and the scoped setter only, read back on `get_workspace` and
//! list rows.

use super::*;
use intent_core::WorkspaceListFilter;

/// A fresh database carries the column, defaulting to 0 (off).
#[tokio::test]
async fn migration_adds_auto_archive_disabled_column_defaulting_off() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let id = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&id, "ws", false))
        .await
        .expect("insert");
    let raw: i64 = sqlx::query_scalar("SELECT auto_archive_disabled FROM workspace WHERE id = ?")
        .bind(&id.0)
        .fetch_one(store.read_pool())
        .await
        .expect("auto_archive_disabled column exists");
    assert_eq!(raw, 0);
    assert!(
        !store
            .get_workspace(&id)
            .await
            .expect("get")
            .auto_archive_disabled
    );
}

/// Insert and the scoped setter round-trip the flag on get and list rows; a
/// stale full-row write never reverts it; an unknown workspace is `NotFound`.
#[tokio::test]
async fn auto_archive_disabled_round_trips_and_survives_stale_full_row_write() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let id = WorkspaceId::new();
    let mut ws = sample_workspace(&id, "ws", false);
    ws.auto_archive_disabled = true;
    store.insert_workspace(&ws).await.expect("insert");
    assert!(
        store
            .get_workspace(&id)
            .await
            .expect("get")
            .auto_archive_disabled
    );

    store
        .set_workspace_auto_archive_disabled(&id, false)
        .await
        .expect("turn off");
    assert!(
        !store
            .get_workspace(&id)
            .await
            .expect("get")
            .auto_archive_disabled
    );

    // The stale `ws` snapshot still says `true`; the full-row write must not
    // touch the column.
    store
        .update_workspace(&ws)
        .await
        .expect("stale full-row write");
    assert!(
        !store
            .get_workspace(&id)
            .await
            .expect("get")
            .auto_archive_disabled
    );

    store
        .set_workspace_auto_archive_disabled(&id, true)
        .await
        .expect("turn on");
    let stale = {
        let mut s = store.get_workspace(&id).await.expect("get");
        s.auto_archive_disabled = false;
        s
    };
    store
        .update_workspace(&stale)
        .await
        .expect("stale full-row write");
    let listed = store
        .list_workspaces(WorkspaceListFilter::WITH_ARCHIVED)
        .await
        .expect("list");
    let row = listed.iter().find(|w| w.id == id).expect("listed");
    assert!(row.auto_archive_disabled, "kept on list rows");

    let missing = store
        .set_workspace_auto_archive_disabled(&WorkspaceId::new(), true)
        .await
        .expect_err("unknown workspace");
    assert!(
        matches!(missing, intent_core::Error::NotFound(_)),
        "{missing:?}"
    );
}

/// The automatic-sweep archive reads the opt-out inside its transaction:
/// an opted-out row is left untouched (`Ok(None)`), while the manual archive
/// still archives it and an opted-in row is archived automatically.
#[tokio::test]
async fn automatic_archive_refuses_an_opted_out_row_inside_the_transaction() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let id = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&id, "ws", false))
        .await
        .expect("insert");
    store
        .set_workspace_auto_archive_disabled(&id, true)
        .await
        .expect("opt out");

    let refused = store
        .archive_workspace_detaching_guests_unless_auto_archive_disabled(
            &id,
            "2026-10-30T00:00:00Z",
        )
        .await
        .expect("automatic archive");
    assert!(refused.is_none(), "opted-out row is not archived");
    let row = store.get_workspace(&id).await.expect("get");
    assert!(!row.archived);
    assert_eq!(row.archived_at, None);

    store
        .set_workspace_auto_archive_disabled(&id, false)
        .await
        .expect("opt back in");
    let swept = store
        .archive_workspace_detaching_guests_unless_auto_archive_disabled(
            &id,
            "2026-10-30T00:00:01Z",
        )
        .await
        .expect("automatic archive");
    assert!(swept.is_some());
    assert!(store.get_workspace(&id).await.expect("get").archived);

    let manual = WorkspaceId::new();
    let mut ws = sample_workspace(&manual, "manual", false);
    ws.auto_archive_disabled = true;
    store.insert_workspace(&ws).await.expect("insert");
    store
        .archive_workspace_detaching_guests(&manual, "2026-10-30T00:00:02Z")
        .await
        .expect("manual archive ignores the opt-out");
    assert!(store.get_workspace(&manual).await.expect("get").archived);
}
