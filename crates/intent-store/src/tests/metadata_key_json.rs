//! The proposal metadata setter must retry only transient contention, and
//! replay only its idempotent, workspace-scoped single-key SQL statement.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::{sample_agent_session, sample_workspace, TempDb};
use crate::{Store, POOL_TIMEOUT_OBSERVER};
use intent_core::{AgentId, Error, WorkspaceId};
use serde_json::{json, Value};
use tokio::time::timeout;

const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(1);
const WATCHDOG: Duration = Duration::from_secs(10);
const UPDATED_AT: &str = "2026-09-27T00:00:00Z";

async fn setup() -> (TempDb, Store, WorkspaceId, AgentId) {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "metadata", false))
        .await
        .expect("insert workspace");
    let id = AgentId::new();
    let mut session = sample_agent_session(&id, &ws);
    session.metadata = Some(json!({
        "sibling": {"keep": [true, 7, null]},
        "proposalResolutions": {"previous": "dismissed"},
    }));
    store
        .insert_agent_session(&session)
        .await
        .expect("insert agent");
    (tmp, store, ws, id)
}

async fn shorten_acquire_timeout(store: &mut Store, tmp: &TempDb) {
    // Migrations and fixture inserts use the ordinary pool. Only the real
    // contention control replaces it with the existing short-timeout seam.
    store.write_pool.close().await;
    store.write_pool = crate::connect_write_with_acquire_timeout(&tmp.path, ACQUIRE_TIMEOUT)
        .await
        .expect("open short-timeout writer");
}

async fn wait_for_timeout(observed: &AtomicUsize) {
    timeout(WATCHDOG, async {
        while observed.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the setter must reach a classified pool acquire timeout");
}

#[tokio::test]
async fn recovers_after_real_pool_timeout_preserving_newer_sibling_metadata() {
    let (tmp, mut store, ws, id) = setup().await;
    shorten_acquire_timeout(&mut store, &tmp).await;
    let mut held = store
        .write_pool()
        .acquire()
        .await
        .expect("hold sole writer");
    let observed = Arc::new(AtomicUsize::new(0));
    let value = json!({"previous": "dismissed", "current": "applied"});
    let serialized = value.to_string();
    let write = POOL_TIMEOUT_OBSERVER.scope(
        Arc::clone(&observed),
        store.set_agent_session_metadata_key_json(
            &ws,
            &id,
            "proposalResolutions",
            &serialized,
            UPDATED_AT,
        ),
    );
    tokio::pin!(write);

    // A bypass of the retry helper returns its real error here instead of
    // hanging behind the observer. The successful path releases the writer
    // only after this exact setter has observed PoolTimedOut, not a sleep.
    let result = tokio::select! {
        result = &mut write => {
            drop(held);
            result
        }
        () = wait_for_timeout(&observed) => {
            // A sibling changes while the setter is waiting. Retrying must
            // evaluate json_set against the current row, not an old snapshot.
            sqlx::query("UPDATE agent_session SET metadata = json_set(metadata, '$.sibling', json(?)) WHERE id = ?")
                .bind(r#"{"newer":[false,42,null]}"#)
                .bind(&id.0)
                .execute(&mut *held)
                .await
                .expect("write newer sibling using held connection");
            drop(held);
            timeout(WATCHDOG, &mut write).await.expect("bounded recovery")
        }
    };
    result.expect("JSON metadata setter must recover from the real pool timeout");
    assert!(
        observed.load(Ordering::SeqCst) >= 1,
        "no actual timeout observed"
    );
    let session = store.get_agent_session(&id).await.expect("read agent");
    assert_eq!(
        session.metadata,
        Some(json!({
            "sibling": {"newer": [false, 42, null]},
            "proposalResolutions": value,
        }))
    );
    assert_eq!(session.updated_at, UPDATED_AT);
}

#[tokio::test]
async fn permanent_pool_contention_returns_last_error_after_existing_deadline() {
    let (tmp, mut store, ws, id) = setup().await;
    let before = store.get_agent_session(&id).await.expect("read before");
    shorten_acquire_timeout(&mut store, &tmp).await;
    let held = store
        .write_pool()
        .acquire()
        .await
        .expect("hold sole writer");
    let observed = Arc::new(AtomicUsize::new(0));
    let start = Instant::now();
    // Exercise the actual production retry window. Its last acquire may
    // extend past the retry deadline by ACQUIRE_TIMEOUT; the outer guard
    // detects an unbounded retry without shortening the production policy.
    let error = timeout(
        crate::BUSY_RETRY_DEADLINE + WATCHDOG,
        POOL_TIMEOUT_OBSERVER.scope(
            Arc::clone(&observed),
            store.set_agent_session_metadata_key_json(
                &ws,
                &id,
                "pendingProposals",
                "[]",
                UPDATED_AT,
            ),
        ),
    )
    .await
    .expect("permanent contention must terminate")
    .expect_err("held writer cannot succeed");
    let elapsed = start.elapsed();
    drop(held);
    assert!(
        matches!(&error, Error::Internal(message)
        if message == &format!("set agent session metadata key json failed: {}", crate::POOL_TIMED_OUT_MESSAGE)),
        "last acquire failure must be preserved: {error:?}"
    );
    assert!(
        observed.load(Ordering::SeqCst) > 1,
        "transient errors must be retried"
    );
    assert!(
        elapsed >= crate::BUSY_RETRY_DEADLINE,
        "returned before the retry deadline: {elapsed:?}"
    );
    let after = store.get_agent_session(&id).await.expect("read after");
    assert_eq!(after.metadata, before.metadata);
    assert_eq!(after.updated_at, before.updated_at);
}

#[tokio::test]
async fn preserves_json_value_types_and_siblings_on_repeated_writes() {
    let (_tmp, store, ws, id) = setup().await;
    for value in [
        json!({"id": 7}),
        json!([1, false, null]),
        json!("quoted"),
        json!(42),
        json!(1.5),
        json!(true),
        Value::Null,
    ] {
        for _ in 0..2 {
            store
                .set_agent_session_metadata_key_json(
                    &ws,
                    &id,
                    "pendingProposals",
                    &value.to_string(),
                    UPDATED_AT,
                )
                .await
                .expect("set JSON value");
            let session = store.get_agent_session(&id).await.expect("read agent");
            assert_eq!(
                session.metadata,
                Some(json!({
                    "sibling": {"keep": [true, 7, null]},
                    "proposalResolutions": {"previous": "dismissed"},
                    "pendingProposals": value,
                }))
            );
            assert_eq!(session.updated_at, UPDATED_AT);
        }
    }
}

#[tokio::test]
async fn preserves_null_and_nonobject_metadata_defenses() {
    let (_tmp, store, ws, id) = setup().await;
    for prior in [
        None,
        Some("null"),
        Some("[1,true]"),
        Some("42"),
        Some(r#""legacy""#),
    ] {
        sqlx::query("UPDATE agent_session SET metadata = ? WHERE id = ?")
            .bind(prior)
            .bind(&id.0)
            .execute(store.write_pool())
            .await
            .expect("seed prior metadata");
        store
            .set_agent_session_metadata_key_json(&ws, &id, "pendingProposals", "[]", UPDATED_AT)
            .await
            .expect("set JSON on legacy metadata");
        let expected = match prior {
            None => json!({"pendingProposals": []}),
            Some(raw) => {
                json!({"priorNonObjectMetadata": serde_json::from_str::<Value>(raw).unwrap(), "pendingProposals": []})
            }
        };
        assert_eq!(
            store
                .get_agent_session(&id)
                .await
                .expect("read agent")
                .metadata,
            Some(expected)
        );
    }
}

#[tokio::test]
async fn missing_agent_and_wrong_workspace_remain_not_found_without_mutation() {
    let (_tmp, store, ws, id) = setup().await;
    let other_ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&other_ws, "other", false))
        .await
        .expect("insert other workspace");
    let other_id = AgentId::new();
    let before = store.get_agent_session(&id).await.expect("read before");
    for (workspace, agent) in [(&other_ws, &id), (&ws, &other_id)] {
        let error = timeout(
            WATCHDOG,
            store.set_agent_session_metadata_key_json(
                workspace,
                agent,
                "pendingProposals",
                "[]",
                UPDATED_AT,
            ),
        )
        .await
        .expect("NotFound is not retried")
        .expect_err("missing scoped agent");
        assert!(matches!(error, Error::NotFound(_)), "{error:?}");
    }
    let after = store.get_agent_session(&id).await.expect("read after");
    assert_eq!(after.metadata, before.metadata);
    assert_eq!(after.updated_at, before.updated_at);
}

#[tokio::test]
async fn malformed_json_and_closed_pool_fail_without_retry_or_mutation() {
    let (_tmp, store, ws, id) = setup().await;
    let before = store.get_agent_session(&id).await.expect("read before");
    let error = timeout(
        WATCHDOG,
        store.set_agent_session_metadata_key_json(
            &ws,
            &id,
            "pendingProposals",
            "[broken",
            UPDATED_AT,
        ),
    )
    .await
    .expect("malformed JSON is not retried")
    .expect_err("bad value must fail");
    assert!(
        matches!(&error, Error::Internal(message) if message.contains("malformed JSON")),
        "{error:?}"
    );
    let after = store
        .get_agent_session(&id)
        .await
        .expect("read after bad value");
    assert_eq!(after.metadata, before.metadata);
    assert_eq!(after.updated_at, before.updated_at);

    sqlx::query("UPDATE agent_session SET metadata = 'broken' WHERE id = ?")
        .bind(&id.0)
        .execute(store.write_pool())
        .await
        .expect("seed malformed column");
    let error = timeout(
        WATCHDOG,
        store.set_agent_session_metadata_key_json(&ws, &id, "pendingProposals", "[]", UPDATED_AT),
    )
    .await
    .expect("malformed column is not retried")
    .expect_err("bad column must fail");
    assert!(
        matches!(&error, Error::Internal(message) if message.contains("malformed JSON")),
        "{error:?}"
    );
    let (metadata, updated_at): (String, String) =
        sqlx::query_as("SELECT metadata, updated_at FROM agent_session WHERE id = ?")
            .bind(&id.0)
            .fetch_one(store.read_pool())
            .await
            .expect("read raw row");
    assert_eq!(metadata, "broken");
    assert_eq!(updated_at, before.updated_at);

    store.write_pool().close().await;
    let error = timeout(
        WATCHDOG,
        store.set_agent_session_metadata_key_json(&ws, &id, "pendingProposals", "[]", UPDATED_AT),
    )
    .await
    .expect("closed pool is not retried")
    .expect_err("closed pool must fail");
    assert!(
        matches!(&error, Error::Internal(message) if message.contains("attempted to acquire a connection on a closed pool")),
        "{error:?}"
    );
}
