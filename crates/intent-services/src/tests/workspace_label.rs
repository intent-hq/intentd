//! `workspace.update { label }` (PROTOCOL §5.1 "Workspace labels"): the
//! clearable tri-state, trim + 1–64 character validation, owner-only
//! authorization, the `workspace:updated { changes: { label } }` delta, and
//! persistence across archive / unarchive.

use std::time::Duration;

use intent_core::{Error, WorkspaceApi, WorkspaceId, WorkspaceListFilter, WorkspaceUpdate};
use intent_store::Store;
use serde_json::{json, Value};

use super::{workspace, TempDb, WorkspacesRoot};
use crate::test_support::capability_fixtures::{fixture, Role};
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
    let services = Services::new(store)
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

async fn assert_no_event(sub: &mut Subscription) {
    let got = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_millis(200)),
        sub.recv(),
    )
    .await;
    assert!(got.is_err(), "no workspace:updated expected: {got:?}");
}

fn label_update(value: &Value) -> WorkspaceUpdate {
    serde_json::from_value(json!({ "label": value })).expect("deserialize update")
}

async fn update_label(h: &Harness, value: &Value) -> intent_core::Result<intent_core::Workspace> {
    h.services
        .update_workspace(h.ws.clone(), label_update(value))
        .await
}

/// The wire tri-state: missing leaves the label untouched, `null` is an
/// explicit clear, a string is a set.
#[test]
fn update_label_deserializes_as_clearable_tri_state() {
    let missing: WorkspaceUpdate = serde_json::from_value(json!({})).unwrap();
    assert_eq!(missing.label, None);
    assert_eq!(label_update(&Value::Null).label, Some(None));
    assert_eq!(
        label_update(&json!("For Later")).label,
        Some(Some("For Later".to_string()))
    );
}

/// Set (trimmed), replace, untouched-by-other-fields, and clear all persist
/// and travel on `workspace:updated { changes: { label } }`, with an
/// explicit `null` on the clear.
#[intent_test_macros::daemon_test]
async fn label_set_replace_clear_round_trip_and_event_delta() {
    let h = harness().await;
    let mut sub = subscribe(&h);

    let ws = update_label(&h, &json!("  For Later \t"))
        .await
        .expect("set");
    assert_eq!(ws.label.as_deref(), Some("For Later"));
    assert_eq!(serde_json::to_value(&ws).unwrap()["label"], "For Later");
    assert_eq!(
        next_changes(&mut sub).await,
        json!({ "label": "For Later" })
    );

    let ws = update_label(&h, &json!("pinned")).await.expect("replace");
    assert_eq!(ws.label.as_deref(), Some("pinned"));
    assert_eq!(next_changes(&mut sub).await, json!({ "label": "pinned" }));

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
    assert_eq!(ws.label.as_deref(), Some("pinned"), "missing = untouched");
    let changes = next_changes(&mut sub).await;
    assert!(changes.get("label").is_none(), "{changes}");

    let got = h.services.get_workspace(h.ws.clone()).await.expect("get");
    assert_eq!(got.label.as_deref(), Some("pinned"));
    let rows = h
        .services
        .list_workspaces(WorkspaceListFilter::WITH_ARCHIVED)
        .await
        .expect("list");
    let row = rows.iter().find(|w| w.id == h.ws).expect("listed");
    assert_eq!(row.label.as_deref(), Some("pinned"), "kept on list rows");

    let ws = update_label(&h, &Value::Null).await.expect("clear");
    assert_eq!(ws.label, None);
    assert!(
        serde_json::to_value(&ws).unwrap().get("label").is_none(),
        "an unset label is omitted, not null"
    );
    assert_eq!(next_changes(&mut sub).await, json!({ "label": null }));
    let got = h.services.get_workspace(h.ws.clone()).await.expect("get");
    assert_eq!(got.label, None);
}

/// Empty / whitespace-only and over-64-character labels are
/// `InvalidParams` (-32602) and write nothing; 64 multi-byte characters is
/// within the limit (counted in characters, not bytes).
#[intent_test_macros::daemon_test]
async fn invalid_labels_are_rejected_without_writing() {
    let h = harness().await;
    update_label(&h, &json!("Keep")).await.expect("seed");
    let mut sub = subscribe(&h);

    for bad in [json!(""), json!("   "), json!("x".repeat(65))] {
        let err = update_label(&h, &bad).await.expect_err("rejected");
        assert!(matches!(err, Error::InvalidParams(_)), "{bad}: {err:?}");
    }
    let mixed = serde_json::from_value::<WorkspaceUpdate>(
        json!({ "title": "Should not land", "label": "" }),
    )
    .unwrap();
    assert!(matches!(
        h.services.update_workspace(h.ws.clone(), mixed).await,
        Err(Error::InvalidParams(_))
    ));
    assert_no_event(&mut sub).await;
    let got = h.services.get_workspace(h.ws.clone()).await.expect("get");
    assert_eq!(got.label.as_deref(), Some("Keep"));
    assert_ne!(got.title, "Should not land");

    let wide = "é".repeat(64);
    let ws = update_label(&h, &json!(wide)).await.expect("64 chars ok");
    assert_eq!(ws.label.as_deref(), Some(wide.as_str()));
}

/// The label rides the row through archive and unarchive.
#[intent_test_macros::daemon_test]
async fn label_survives_archive_and_unarchive() {
    let h = harness().await;
    update_label(&h, &json!("For Later")).await.expect("set");
    let archived = h
        .services
        .archive_workspace(h.ws.clone(), None)
        .await
        .expect("archive");
    assert!(archived.archived);
    assert_eq!(archived.label.as_deref(), Some("For Later"));
    let unarchived = h
        .services
        .unarchive_workspace(h.ws.clone())
        .await
        .expect("unarchive");
    assert!(!unarchived.archived);
    assert_eq!(unarchived.label.as_deref(), Some("For Later"));
    let got = h.services.get_workspace(h.ws.clone()).await.expect("get");
    assert_eq!(got.label.as_deref(), Some("For Later"));
}

/// A full-row write from a snapshot read before a label change (e.g. PR
/// creation finishing, or a concurrent title-only update) must not revert
/// the newer label: only the scoped label write touches the column.
#[intent_test_macros::daemon_test]
async fn stale_full_row_write_keeps_newer_label() {
    let h = harness().await;
    let stale = h.services.store.get_workspace(&h.ws).await.expect("read");
    assert_eq!(stale.label, None);
    update_label(&h, &json!("For Later")).await.expect("set");
    h.services
        .store
        .update_workspace(&stale)
        .await
        .expect("stale unrelated write");
    let got = h.services.get_workspace(h.ws.clone()).await.expect("get");
    assert_eq!(got.label.as_deref(), Some("For Later"), "set survives");

    let stale = h.services.store.get_workspace(&h.ws).await.expect("read");
    update_label(&h, &Value::Null).await.expect("clear");
    h.services
        .store
        .update_workspace(&stale)
        .await
        .expect("stale unrelated write");
    let got = h.services.get_workspace(h.ws.clone()).await.expect("get");
    assert_eq!(got.label, None, "clear survives");

    update_label(&h, &json!("Keep")).await.expect("set");
    let title_only: WorkspaceUpdate =
        serde_json::from_value(json!({ "title": "Renamed" })).expect("update");
    let ws = h
        .services
        .update_workspace(h.ws.clone(), title_only)
        .await
        .expect("title-only update");
    assert_eq!(ws.label.as_deref(), Some("Keep"));
    let got = h.services.get_workspace(h.ws.clone()).await.expect("get");
    assert_eq!(
        got.label.as_deref(),
        Some("Keep"),
        "missing label untouched"
    );
}

/// Label writes are owner-only: a collaborator's set or clear is
/// `Forbidden` (-32003) and leaves the row alone, while the owner may set
/// it and the collaborator keeps its card-metadata rights.
#[tokio::test]
async fn collaborator_cannot_change_label() {
    let tmp = TempDb::new();
    let f = fixture(&tmp).await;
    let svc = f.services.clone();
    let ws = f.ws.clone();

    let owned = f
        .run(
            Role::Owner,
            svc.update_workspace(ws.clone(), label_update(&json!("Mine"))),
        )
        .await
        .expect("owner sets label");
    assert_eq!(owned.label.as_deref(), Some("Mine"));

    for value in [json!("Theirs"), Value::Null] {
        let err = f
            .run(
                Role::Collaborator,
                svc.update_workspace(ws.clone(), label_update(&value)),
            )
            .await
            .expect_err("collaborator refused");
        assert!(matches!(err, Error::Forbidden(_)), "{value}: {err:?}");
    }
    f.run(
        Role::Collaborator,
        svc.update_workspace(
            ws.clone(),
            WorkspaceUpdate {
                title: Some("Collaborator title".to_string()),
                ..Default::default()
            },
        ),
    )
    .await
    .expect("collaborator may still edit the title");
    let got = svc.store.get_workspace(&ws).await.expect("get");
    assert_eq!(got.label.as_deref(), Some("Mine"));
}
