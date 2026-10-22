//! Retired workspace lifecycle at the store layer (`workspace.retire`,
//! PROTOCOL §5.1): retired rows are opt-in on every list, the retire flip is
//! scoped and conditional, and neither unarchive nor a full-row update can
//! flip a retired row back.

use super::*;
use intent_core::WorkspaceListFilter;

async fn seeded() -> (TempDb, Store, WorkspaceId, WorkspaceId, WorkspaceId) {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let active = WorkspaceId::new();
    let archived = WorkspaceId::new();
    let retired = WorkspaceId::new();
    for (id, is_archived) in [(&active, false), (&archived, true), (&retired, true)] {
        store
            .insert_workspace(&sample_workspace(id, "ws", is_archived))
            .await
            .expect("insert");
    }
    assert!(store
        .retire_workspace_if_archived(&retired, &now_iso())
        .await
        .expect("retire"));
    (tmp, store, active, archived, retired)
}

fn ids(list: &[Workspace]) -> Vec<WorkspaceId> {
    let mut ids: Vec<_> = list.iter().map(|w| w.id.clone()).collect();
    ids.sort_by(|a, b| a.0.cmp(&b.0));
    ids
}

fn sorted(mut v: Vec<WorkspaceId>) -> Vec<WorkspaceId> {
    v.sort_by(|a, b| a.0.cmp(&b.0));
    v
}

/// Retired rows never appear without `include_retired` — not even with
/// `include_archived` — and appear with it.
#[tokio::test]
async fn list_workspaces_excludes_retired_unless_opted_in() {
    let (_tmp, store, active, archived, retired) = seeded().await;

    let live = store
        .list_workspaces(WorkspaceListFilter::ACTIVE)
        .await
        .unwrap();
    assert_eq!(ids(&live), vec![active.clone()]);

    let with_archived = store
        .list_workspaces(WorkspaceListFilter::WITH_ARCHIVED)
        .await
        .unwrap();
    assert_eq!(
        ids(&with_archived),
        sorted(vec![active.clone(), archived.clone()]),
        "includeArchived alone must never include retired rows"
    );

    let all = store
        .list_workspaces(WorkspaceListFilter::ALL)
        .await
        .unwrap();
    assert_eq!(
        ids(&all),
        sorted(vec![active.clone(), archived.clone(), retired.clone()])
    );

    let retired_only = store
        .list_workspaces(WorkspaceListFilter::new(false, true))
        .await
        .unwrap();
    assert_eq!(ids(&retired_only), sorted(vec![active, retired]));
}

/// The retire flip sets `Retired` + `retiredAt`, keeps `archived` /
/// `archivedAt` and every other column, declines a second call, refuses an
/// Active row (the caller archives first), and reports a missing row.
#[tokio::test]
async fn retire_flip_is_scoped_and_conditional() {
    let (_tmp, store, active, _archived, retired) = seeded().await;

    let row = store.get_workspace(&retired).await.unwrap();
    assert_eq!(row.status, WorkspaceStatus::Retired);
    assert!(row.is_retired());
    assert!(row.archived);
    assert!(row.archived_at.is_some());
    assert!(row.retired_at.is_some());
    assert_eq!(row.branch, "feature/test", "the branch is kept");

    assert!(!store
        .retire_workspace_if_archived(&retired, &now_iso())
        .await
        .unwrap());
    assert!(!store
        .retire_workspace_if_archived(&active, &now_iso())
        .await
        .unwrap());
    assert_eq!(
        store.get_workspace(&active).await.unwrap().status,
        WorkspaceStatus::Active
    );
    assert!(matches!(
        store
            .retire_workspace_if_archived(&WorkspaceId::new(), &now_iso())
            .await,
        Err(Error::NotFound(_))
    ));
}

/// Neither the conditional unarchive nor a full-row update resurrects a
/// retired row: reviving is a separate, explicit operation.
#[tokio::test]
async fn retired_row_survives_unarchive_and_full_row_update() {
    let (_tmp, store, _active, _archived, retired) = seeded().await;

    assert!(!store
        .unarchive_workspace_if_archived(&retired, &now_iso())
        .await
        .unwrap());
    let mut row = store.get_workspace(&retired).await.unwrap();
    assert_eq!(row.status, WorkspaceStatus::Retired);

    row.status = WorkspaceStatus::Active;
    row.retired_at = None;
    row.title = "renamed".into();
    store.update_workspace(&row).await.unwrap();
    let row = store.get_workspace(&retired).await.unwrap();
    assert_eq!(row.title, "renamed");
    assert_eq!(row.status, WorkspaceStatus::Retired);
    assert!(row.retired_at.is_some());
}

/// The archive flip reads the status inside its IMMEDIATE transaction: a
/// retired row is refused with `WorkspaceRetired` and never flipped back to
/// Archived (closes the archive-vs-retire race).
#[tokio::test]
async fn archive_flip_refuses_a_retired_row() {
    let (_tmp, store, _active, _archived, retired) = seeded().await;
    let err = store
        .archive_workspace_detaching_guests(&retired, &now_iso())
        .await
        .unwrap_err();
    assert!(
        matches!(&err, Error::WorkspaceRetired { workspace_id } if *workspace_id == retired.0),
        "{err:?}"
    );
    assert_eq!(
        store.get_workspace(&retired).await.unwrap().status,
        WorkspaceStatus::Retired
    );
}
