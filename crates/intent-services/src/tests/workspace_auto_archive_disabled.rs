//! `workspace.update { autoArchiveDisabled }` (PROTOCOL §5.1 "Auto-archive
//! opt-out"): off by default and omitted while off, owner-only, carried on
//! `workspace:updated { changes }`, persisted across archive / unarchive, and
//! honoured by the automatic archive sweep.

use std::time::Duration;

use intent_core::{Error, WorkspaceApi, WorkspaceId, WorkspaceListFilter, WorkspaceUpdate};
use intent_store::Store;
use serde_json::{json, Value};

use super::{workspace, TempDb, WorkspacesRoot};
use crate::test_support::capability_fixtures::{fixture, Role};
use crate::test_support::retire_workspace as retire;
use crate::{EventBus, Services, Subscription, SubscriptionFilter};

struct Harness {
    _tmp: TempDb,
    _ws_root: WorkspacesRoot,
    services: Services,
    bus: EventBus,
    ws: WorkspaceId,
}

async fn harness() -> Harness {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");
    let bus = EventBus::new(store.clone());
    let ws_root = WorkspacesRoot::new();
    let services = Services::new_isolated(store)
        .with_workspaces_root(ws_root.path().to_path_buf())
        .with_event_bus(bus.clone());
    Harness {
        _tmp: tmp,
        _ws_root: ws_root,
        services,
        bus,
        ws,
    }
}

fn subscribe(h: &Harness) -> Subscription {
    h.bus.subscribe(SubscriptionFilter {
        event_types: vec!["workspace:updated".to_string()],
        workspace_id: Some(h.ws.0.clone()),
        ..Default::default()
    })
}

async fn next_changes(sub: &mut Subscription) -> Value {
    let batch = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(2)),
        sub.recv(),
    )
    .await
    .expect("event delivered")
    .expect("subscription open");
    assert_eq!(batch.len(), 1, "expected exactly one event");
    let ev = serde_json::to_value(&batch[0]).expect("serialize event");
    assert_eq!(ev["type"], "workspace:updated");
    ev["data"]["changes"].clone()
}

fn opt_out_update(value: &Value) -> WorkspaceUpdate {
    serde_json::from_value(json!({ "autoArchiveDisabled": value })).expect("deserialize update")
}

async fn set_opt_out(h: &Harness, disabled: bool) -> intent_core::Workspace {
    h.services
        .update_workspace(h.ws.clone(), opt_out_update(&json!(disabled)))
        .await
        .expect("update autoArchiveDisabled")
}

/// Missing leaves the flag untouched, a boolean sets it, and any other JSON
/// type is a deserialize error (`-32602` at the router).
#[test]
fn update_param_deserializes_as_optional_bool() {
    let missing: WorkspaceUpdate = serde_json::from_value(json!({})).unwrap();
    assert_eq!(missing.auto_archive_disabled, None);
    assert_eq!(
        opt_out_update(&json!(true)).auto_archive_disabled,
        Some(true)
    );
    assert_eq!(
        opt_out_update(&json!(false)).auto_archive_disabled,
        Some(false)
    );
    assert!(
        serde_json::from_value::<WorkspaceUpdate>(json!({ "autoArchiveDisabled": "yes" })).is_err()
    );
}

/// Off by default and omitted from the payload; on/off persist, show on get
/// and list rows, travel on the `workspace:updated` delta, and an unrelated
/// update leaves the flag alone.
#[intent_test_macros::daemon_test]
async fn opt_out_round_trips_and_emits_delta() {
    let h = harness().await;
    let fresh = h.services.get_workspace(h.ws.clone()).await.expect("get");
    assert!(!fresh.auto_archive_disabled, "off by default");
    assert!(
        serde_json::to_value(&fresh)
            .unwrap()
            .get("autoArchiveDisabled")
            .is_none(),
        "omitted while off"
    );

    let mut sub = subscribe(&h);
    let ws = set_opt_out(&h, true).await;
    assert!(ws.auto_archive_disabled);
    assert_eq!(
        serde_json::to_value(&ws).unwrap()["autoArchiveDisabled"],
        true
    );
    assert_eq!(
        next_changes(&mut sub).await,
        json!({ "autoArchiveDisabled": true })
    );

    let ws = h
        .services
        .update_workspace(
            h.ws.clone(),
            WorkspaceUpdate {
                title: Some("Renamed".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect("title");
    assert!(ws.auto_archive_disabled, "missing = untouched");
    let changes = next_changes(&mut sub).await;
    assert!(changes.get("autoArchiveDisabled").is_none(), "{changes}");

    let rows = h
        .services
        .list_workspaces(WorkspaceListFilter::WITH_ARCHIVED)
        .await
        .expect("list");
    let row = rows.iter().find(|w| w.id == h.ws).expect("listed");
    assert!(row.auto_archive_disabled, "kept on list rows");

    let ws = set_opt_out(&h, false).await;
    assert!(!ws.auto_archive_disabled);
    assert_eq!(
        next_changes(&mut sub).await,
        json!({ "autoArchiveDisabled": false })
    );
    let got = h.services.get_workspace(h.ws.clone()).await.expect("get");
    assert!(!got.auto_archive_disabled);
}

/// The flag rides the row through archive and unarchive.
#[intent_test_macros::daemon_test]
async fn opt_out_survives_archive_and_unarchive() {
    let h = harness().await;
    set_opt_out(&h, true).await;
    let archived = h
        .services
        .archive_workspace(h.ws.clone(), None)
        .await
        .expect("archive");
    assert!(archived.archived);
    assert!(archived.auto_archive_disabled);
    let unarchived = h
        .services
        .unarchive_workspace(h.ws.clone())
        .await
        .expect("unarchive");
    assert!(!unarchived.archived);
    assert!(unarchived.auto_archive_disabled);
}

/// Owner-only: a collaborator's write is `Forbidden` (-32003) and leaves the
/// row alone.
#[tokio::test]
async fn collaborator_cannot_change_opt_out() {
    let tmp = TempDb::new();
    let f = fixture(&tmp).await;
    let svc = f.services.clone();
    let ws = f.ws.clone();

    let owned = f
        .run(
            Role::Owner,
            svc.update_workspace(ws.clone(), opt_out_update(&json!(true))),
        )
        .await
        .expect("owner sets the opt-out");
    assert!(owned.auto_archive_disabled);

    let err = f
        .run(
            Role::Collaborator,
            svc.update_workspace(ws.clone(), opt_out_update(&json!(false))),
        )
        .await
        .expect_err("collaborator refused");
    assert!(matches!(err, Error::Forbidden(_)), "{err:?}");
    let got = svc.store.get_workspace(&ws).await.expect("get");
    assert!(got.auto_archive_disabled);
}

/// The sweep leaves an opted-out idle workspace active (not even counted as
/// skipped); once the opt-out is cleared, the next sweep archives it.
#[intent_test_macros::daemon_test]
async fn sweep_skips_opted_out_workspace_until_cleared() {
    let f = retire::fixture().await;
    let ws = retire::create_workspace(&f).await;
    f.store
        .set_workspace_auto_archive_disabled(&ws.id, true)
        .await
        .expect("opt out");

    let report = f
        .svc
        .sweep_auto_archive_after(Some(7), retire::days_from_now(30))
        .await;
    assert_eq!(report, crate::AutoArchiveReport::default(), "{report:?}");
    assert!(!f.store.get_workspace(&ws.id).await.unwrap().archived);

    f.store
        .set_workspace_auto_archive_disabled(&ws.id, false)
        .await
        .expect("opt back in");
    let report = f
        .svc
        .sweep_auto_archive_after(Some(7), retire::days_from_now(30))
        .await;
    assert_eq!(report.archived, 1, "{report:?}");
    assert!(f.store.get_workspace(&ws.id).await.unwrap().archived);
}
