//! intent-hq/intent#7070: `client.hello` (`upsert_client`) and note writes
//! (`NoteWriteConnection::begin`) must wait out a transient single-writer
//! acquire timeout instead of failing on the first `PoolTimedOut`, the same
//! contract `append_agent_message_survives_write_pool_acquire_timeout` pins
//! for agent-message appends.
use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use super::*;

/// Short enough to keep the tests fast.
const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(1);
/// Upper bound on how long the holder keeps the connection if the operation
/// never reports a timeout; the test then fails on the observer assertion
/// instead of hanging. Also bounds the pool-open retries.
const WATCHDOG: Duration = Duration::from_secs(15);

/// Open a migrated store whose write pool uses [`ACQUIRE_TIMEOUT`]. sqlx opens
/// the first connection eagerly under that timeout, so retry the open on a
/// loaded host until [`WATCHDOG`].
async fn short_acquire_store(tmp: &TempDb) -> Store {
    drop(Store::open(&tmp.path).await.expect("open store"));
    let open_deadline = tokio::time::Instant::now() + WATCHDOG;
    let write_pool = loop {
        match crate::connect_write_with_acquire_timeout(&tmp.path, ACQUIRE_TIMEOUT).await {
            Ok(pool) => break pool,
            Err(Error::Internal(msg))
                if msg.contains("acquire timeout exceeded")
                    && tokio::time::Instant::now() < open_deadline =>
            {
                eprintln!("retrying short-timeout write pool open: {msg}");
            }
            Err(e) => panic!("open short-timeout write pool: {e}"),
        }
    };
    Store {
        task_event_baselines: Arc::default(),
        task_events_enabled: Arc::default(),
        note_pages: Arc::default(),
        note_index_jobs: Arc::default(),
        note_index_owner: Arc::default(),
        _daemon_owner: None,
        write_pool: write_pool.into(),
        read_pool: crate::connect_read(&tmp.path)
            .await
            .expect("open read pool")
            .into(),
        browser_tab_displayed: crate::browser_tab_repo::DisplayedOverlay::default(),
        export_author_barrier: Arc::default(),
        repository_lifecycle: crate::repository_lifecycle::domain_for(&tmp.path).unwrap(),
    }
}

/// Hold the only write connection until `op` has observed at least one real
/// `PoolTimedOut` (so a slow pre-acquire step cannot consume the hold window
/// and turn the test into a false pass), then release it and return `op`'s
/// result.
async fn run_while_writer_held<T>(
    store: &Store,
    op: impl Future<Output = Result<T, Error>>,
) -> Result<T, Error> {
    let held = store
        .write_pool()
        .acquire()
        .await
        .expect("hold write connection");
    let observed = Arc::new(AtomicUsize::new(0));
    let holder = {
        let observed = Arc::clone(&observed);
        tokio::spawn(async move {
            let deadline = tokio::time::Instant::now() + WATCHDOG;
            while observed.load(Ordering::SeqCst) == 0 && tokio::time::Instant::now() < deadline {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            drop(held);
        })
    };
    let result = crate::POOL_TIMEOUT_OBSERVER
        .scope(Arc::clone(&observed), op)
        .await;
    holder.await.expect("holder task");
    assert!(
        observed.load(Ordering::SeqCst) >= 1,
        "the operation must have hit at least one real PoolTimedOut before the connection was released (result: {:?})",
        result.as_ref().err()
    );
    result
}

#[tokio::test]
async fn upsert_client_survives_write_pool_acquire_timeout() {
    let tmp = TempDb::new();
    let store = short_acquire_store(&tmp).await;
    let client = ClientId("client-7070".to_string());
    let host = ClientHostInfo {
        hostname: Some("host-7070".to_string()),
        ..ClientHostInfo::default()
    };

    run_while_writer_held(
        &store,
        store.upsert_client(&client, Some("Desk"), None, &host),
    )
    .await
    .expect("hello upsert must retry through the write-pool acquire timeout");

    let row = store
        .get_client(&client)
        .await
        .expect("read client")
        .expect("client row persisted");
    assert_eq!(row.name.as_deref(), Some("Desk"));
    assert_eq!(row.host.hostname.as_deref(), Some("host-7070"));
}

#[tokio::test]
async fn note_write_survives_write_pool_acquire_timeout() {
    let tmp = TempDb::new();
    let store = short_acquire_store(&tmp).await;
    let ws_id = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws_id, "WS", false))
        .await
        .expect("insert ws");
    let mut note = task_note(&ws_id, "Note", None);
    note.content = "base".to_string();
    store.insert_note(&note).await.expect("insert note");
    let author = version_author();
    let ts = now_iso();

    note.content = "edited".to_string();
    let (rev, v) = run_while_writer_held(
        &store,
        store.update_note_with_version(&note, Some(0), &author, &ts),
    )
    .await
    .expect("note edit must retry through the write-pool acquire timeout");
    assert_eq!(
        (rev, v),
        (1, 1),
        "exactly one write and one snapshot applied"
    );

    let stored = store.get_note(&ws_id, &note.id).await.expect("get note");
    assert_eq!(stored.content, "edited");
    assert_eq!(stored.rev, 1);
}

/// Under continuous saturation the pre-BEGIN acquire must give up within its
/// hard budget (twice the pool acquire timeout: 20s in production, under the
/// 30s desktop/browser RPC default) with nothing applied, so a caller that
/// has already timed out never sees the edit commit later. The writer is
/// held well past the budget and the note must be untouched afterwards.
#[tokio::test]
async fn note_write_gives_up_within_budget_under_continuous_saturation() {
    const HOLD: Duration = Duration::from_secs(8);
    // Budget (2 x ACQUIRE_TIMEOUT) plus generous scheduling slack, still
    // well below HOLD so a late success after release cannot pass.
    const BOUND: Duration = Duration::from_millis(4_500);
    let tmp = TempDb::new();
    let store = short_acquire_store(&tmp).await;
    let ws_id = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws_id, "WS", false))
        .await
        .expect("insert ws");
    let mut note = task_note(&ws_id, "Note", None);
    note.content = "base".to_string();
    store.insert_note(&note).await.expect("insert note");
    let author = version_author();
    let ts = now_iso();

    let held = store
        .write_pool()
        .acquire()
        .await
        .expect("hold write connection");
    let holder = tokio::spawn(async move {
        tokio::time::sleep(HOLD).await;
        drop(held);
    });

    note.content = "edited".to_string();
    let started = std::time::Instant::now();
    let result = store
        .update_note_with_version(&note, Some(0), &author, &ts)
        .await;
    let elapsed = started.elapsed();
    let err = result.expect_err("a saturated writer must fail the edit, not commit it late");
    assert!(
        elapsed < BOUND,
        "the acquire must give up within its budget, took {elapsed:?}"
    );
    assert!(
        matches!(&err, Error::Internal(msg) if msg.contains("nothing was applied")),
        "exhaustion must report a clean no-op failure: {err:?}"
    );

    holder.await.expect("holder task");
    let stored = store.get_note(&ws_id, &note.id).await.expect("get note");
    assert_eq!(
        stored.content, "base",
        "nothing may commit after the budget"
    );
    assert_eq!(stored.rev, 0);
}
