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

/// intentd#2588 review: a failed retired-note-paging cleanup step (here a
/// write-pool acquire timeout) is retried with backoff instead of ending the
/// cleanup until the next daemon restart.
#[tokio::test]
async fn retired_note_paging_cleanup_retries_a_failed_step() {
    use crate::note_paging_cleanup::NotePagingCleanupPolicy;
    let tmp = TempDb::new();
    let store = short_acquire_store(&tmp).await;
    sqlx::raw_sql(
        r"
        INSERT INTO workspace(id,title,branch,created_at,updated_at) VALUES ('ws','W','main','c','u');
        INSERT INTO note(id,workspace_id,title,content,rev,created_at,updated_at)
          VALUES ('n','ws','Spec','body',1,'c','u');
        INSERT INTO note_page_head(workspace_id,note_id,instance_id,current_rev,generation)
          VALUES ('ws','n','instance',1,'generation');
        WITH RECURSIVE seq(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM seq WHERE i+1 < 50)
        INSERT INTO note_page_entry(workspace_id,note_id,collection,position,value)
          SELECT 'ws','n','blocks',i,'entry' FROM seq;
        ",
    )
    .execute(store.write_pool())
    .await
    .expect("seed retired index");

    // Hold the only writer past one acquire timeout so the first step fails.
    let held = store
        .write_pool()
        .acquire()
        .await
        .expect("hold write connection");
    let cleaner = store.clone();
    let cleanup = tokio::spawn(async move {
        cleaner
            .run_retired_note_paging_cleanup(NotePagingCleanupPolicy {
                min_pause: Duration::ZERO,
                initial_backoff: Duration::from_millis(10),
                max_backoff: Duration::from_millis(50),
                ..NotePagingCleanupPolicy::default()
            })
            .await
    });
    tokio::time::sleep(ACQUIRE_TIMEOUT * 2 + Duration::from_millis(500)).await;
    drop(held);
    let outcome = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(10)),
        cleanup,
    )
    .await
    .expect("cleanup finishes after the writer is released")
    .expect("cleanup task");
    assert!(outcome.finished, "{outcome:?}");
    assert!(outcome.failed_steps >= 1, "{outcome:?}");
    assert_eq!(outcome.rows_removed, 51);
    assert_eq!(outcome.tables_dropped, 2);
}
