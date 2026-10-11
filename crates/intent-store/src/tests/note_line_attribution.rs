//! Attribution refreshes must survive transient saturation of the sole writer.
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use intent_core::{LineAttributionData, LineAttributionInfo, NoteId, WorkspaceId};

use super::{sample_workspace, stray_note, TempDb};
use crate::{Store, POOL_TIMEOUT_OBSERVER};

#[tokio::test]
async fn attribution_recovers_after_write_pool_timeout() {
    let tmp = TempDb::new();
    let mut store = Store::open(&tmp.path).await.unwrap();
    let ws = WorkspaceId::new();
    let other_ws = WorkspaceId::new();
    for workspace_id in [&ws, &other_ws] {
        store
            .insert_workspace(&sample_workspace(workspace_id, "WS", false))
            .await
            .unwrap();
        store
            .insert_note(&stray_note(workspace_id, "same-note", "Note"))
            .await
            .unwrap();
    }
    let snapshot = |workspace_id: WorkspaceId, timestamp| LineAttributionData {
        note_id: NoteId::from("same-note"),
        workspace_id,
        computed_at: "2026-10-01T00:00:00Z".into(),
        attributions: [(
            "1".into(),
            LineAttributionInfo {
                timestamp,
                author: None,
            },
        )]
        .into(),
    };
    let other = snapshot(other_ws.clone(), 100);
    store.upsert_note_line_attribution(&other).await.unwrap();
    store.write_pool.close().await;
    store.write_pool = crate::connect_write_with_acquire_timeout(&tmp.path, Duration::from_secs(1))
        .await
        .unwrap()
        .into();
    let held = store.write_pool().acquire().await.unwrap();
    let observed = Arc::new(AtomicUsize::new(0));
    let latest = snapshot(ws.clone(), 200);
    let write = POOL_TIMEOUT_OBSERVER.scope(
        observed.clone(),
        store.upsert_note_line_attribution(&latest),
    );
    tokio::pin!(write);
    let result = tokio::select! {
        result = &mut write => { drop(held); result }
        () = async {
            tokio::time::timeout(Duration::from_secs(15), async {
                while observed.load(Ordering::SeqCst) == 0 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }).await.expect("upsert must observe an actual pool timeout");
        } => {
            drop(held);
            tokio::time::timeout(Duration::from_secs(15), &mut write).await.expect("bounded recovery")
        }
    };
    result.expect("attribution upsert must recover without another edit");
    assert!(observed.load(Ordering::SeqCst) > 0);
    for expected in [&latest, &other] {
        let actual = store
            .get_note_line_attribution(&expected.workspace_id, &expected.note_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::to_value(actual).unwrap(),
            serde_json::to_value(expected).unwrap()
        );
    }
}

#[tokio::test]
async fn attribution_does_not_retry_a_closed_pool() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    store.write_pool.close().await;
    let data = LineAttributionData {
        note_id: NoteId::from("note"),
        workspace_id: WorkspaceId::new(),
        computed_at: "2026-10-01T00:00:00Z".into(),
        attributions: BTreeMap::default(),
    };
    let error = tokio::time::timeout(
        Duration::from_secs(1),
        store.upsert_note_line_attribution(&data),
    )
    .await
    .expect("closed pool must not spend the transient retry window")
    .unwrap_err();
    assert!(error.to_string().contains("closed pool"), "{error}");
}

async fn after_attribution_pool_timeout<T>(
    store: &Store,
    operation: impl std::future::Future<Output = intent_core::Result<T>>,
) -> T {
    let held = store.write_pool().acquire().await.unwrap();
    let observed = Arc::new(AtomicUsize::new(0));
    let write = POOL_TIMEOUT_OBSERVER.scope(observed.clone(), operation);
    tokio::pin!(write);
    let result = tokio::select! {
        result = &mut write => { drop(held); result }
        () = async {
            tokio::time::timeout(Duration::from_secs(10), async {
                while observed.load(Ordering::SeqCst) == 0 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }).await.expect("observe this operation's actual pool timeout");
        } => {
            drop(held);
            tokio::time::timeout(Duration::from_secs(10), &mut write).await.expect("recovery deadline")
        }
    };
    let value = result.expect("normalized attribution must retry transient pool contention");
    assert!(observed.load(Ordering::SeqCst) > 0);
    value
}

#[tokio::test]
async fn attribution_ticket_and_atomic_publication_recover_after_pool_timeouts() {
    let tmp = TempDb::new();
    let mut store = Store::open(&tmp.path).await.unwrap();
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "WS", false))
        .await
        .unwrap();
    let mut note = stray_note(&ws, "attribution", "Note");
    note.content = "first\nsecond".into();
    store.insert_note(&note).await.unwrap();
    let before = store
        .read_note_page_state(&ws, &note.id, None)
        .await
        .unwrap();
    store.write_pool.close().await;
    store.write_pool = crate::connect_write_with_acquire_timeout(&tmp.path, Duration::from_secs(1))
        .await
        .unwrap()
        .into();
    let job = after_attribution_pool_timeout(
        &store,
        store.begin_note_attribution(&ws, &note.id, note.rev),
    )
    .await;
    let pending = store
        .read_note_page_state(&ws, &note.id, None)
        .await
        .unwrap();
    assert_eq!(pending["attributionState"], "pending");
    let data = LineAttributionData {
        note_id: note.id.clone(),
        workspace_id: ws.clone(),
        computed_at: "2026-10-05T00:00:00Z".into(),
        attributions: [(
            "1".into(),
            LineAttributionInfo {
                timestamp: 1,
                author: None,
            },
        )]
        .into(),
    };
    after_attribution_pool_timeout(
        &store,
        store.publish_note_attribution(&job, &note.content, &data),
    )
    .await;
    let ready = store
        .read_note_page_state(&ws, &note.id, None)
        .await
        .unwrap();
    assert_eq!(ready["attributionState"], "ready");
    assert_eq!(
        ready["attributionGeneration"],
        pending["attributionGeneration"]
    );
    assert_eq!(ready["sourceRevision"], before["sourceRevision"]);
    assert_eq!(ready["commentRevision"], before["commentRevision"]);
    let actual = store
        .get_note_line_attribution(&ws, &note.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::to_value(actual).unwrap(),
        serde_json::to_value(&data).unwrap()
    );
    let rows = store
        .read_attribution_rows(
            &ws,
            &note.id,
            &store.note_annotation_epochs(&ws, &note.id).await.unwrap(),
            &[crate::note_annotation_repo::SourceRange { start: 0, end: 12 }],
            None,
            8,
        )
        .await
        .unwrap();
    assert_eq!(rows.items.len(), 1);
    assert_eq!(rows.items[0].line, 1);
    // A committed/superseded ticket is a permanent stale result, not a retry.
    assert!(matches!(
        tokio::time::timeout(
            Duration::from_secs(1),
            store.publish_note_attribution(&job, &note.content, &data)
        )
        .await
        .unwrap(),
        Err(intent_core::Error::NotePage(
            intent_core::note_page::NotePageError::Stale
        ))
    ));
}
