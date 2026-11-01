//! Removal of the unused note paging system (intent-hq/intent#7515).
//!
//! Migrations 0149-0156 stay because released databases applied them; 0164
//! drops every trigger and every small table they created, and the post-startup
//! cleanup ([`crate::note_paging_cleanup`]) removes the two large tables left
//! behind. These tests pin that notes and comments stay intact, that comments
//! keep working afterwards, that the cleanup is resumable and does not block
//! writes, and that a large note save no longer holds the single writer long
//! enough to block an unrelated write.
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::*;

/// Every object 0149-0156 created starts with one of these prefixes.
const PAGING_PREFIXES: &[&str] = &[
    "note_page_",
    "note_artifact_",
    "note_operation",
    "note_stage",
    "note_annotation_",
    "note_comment_",
    "note_attribution_",
];

async fn paging_objects<'e>(pool: impl sqlx::Executor<'e, Database = sqlx::Sqlite>) -> Vec<String> {
    let names: Vec<String> = sqlx::query_scalar("SELECT name FROM sqlite_master")
        .fetch_all(pool)
        .await
        .unwrap();
    names
        .into_iter()
        .filter(|name| PAGING_PREFIXES.iter().any(|p| name.starts_with(p)))
        .collect()
}

/// Tables (and their indexes) of paging objects still in the schema.
async fn paging_object_tables<'e>(
    pool: impl sqlx::Executor<'e, Database = sqlx::Sqlite>,
) -> std::collections::BTreeSet<String> {
    let rows: Vec<(String, String)> = sqlx::query_as("SELECT name, tbl_name FROM sqlite_master")
        .fetch_all(pool)
        .await
        .unwrap();
    rows.into_iter()
        .filter(|(name, _)| PAGING_PREFIXES.iter().any(|p| name.starts_with(p)))
        .map(|(_, table)| table)
        .collect()
}

fn fast_cleanup() -> crate::note_paging_cleanup::NotePagingCleanupPolicy {
    crate::note_paging_cleanup::NotePagingCleanupPolicy {
        min_pause: Duration::ZERO,
        ..Default::default()
    }
}

async fn comment_rows<'e>(pool: impl sqlx::Executor<'e, Database = sqlx::Sqlite>) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT json_array(id,thread_id,note_id,workspace_id,kind,content,author,author_type,\
         status,parent_id,anchor_json,anchor_text,extra_json,created_at,updated_at) \
         FROM comment ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .unwrap()
}

#[tokio::test]
async fn drop_migration_keeps_notes_and_comments_and_comments_still_work() {
    let tmp = TempDb::new();
    let pool = crate::connect_write(&tmp.path).await.unwrap();
    let prefix = sqlx::migrate::Migrator {
        migrations: std::borrow::Cow::Owned(
            crate::MIGRATOR
                .iter()
                .filter(|m| m.version <= 163)
                .cloned()
                .collect(),
        ),
        ..sqlx::migrate::Migrator::DEFAULT
    };
    prefix.run(&pool).await.unwrap();
    // Seed through the paging triggers, as a released daemon would have.
    sqlx::raw_sql(
        r#"
        INSERT INTO workspace(id,title,branch,created_at,updated_at)
          VALUES ('ws','W','main','c','u');
        INSERT INTO note(id,workspace_id,title,content,rev,created_at,updated_at)
          VALUES ('n','ws','Spec','body 😀 with <!-- c:root --> marker',4,'c','u');
        INSERT INTO comment(id,thread_id,note_id,workspace_id,kind,content,author,author_type,
                            anchor_json,extra_json,created_at,updated_at)
          VALUES ('root','thread','n','ws','comment','root body','alice','user',
                  '{"type":"range","startId":"a1","endId":"a2"}',
                  '{"isOrphaned":true}','c','u');
        INSERT INTO comment(id,thread_id,note_id,workspace_id,kind,content,author,author_type,
                            parent_id,anchor_json,created_at,updated_at)
          VALUES ('reply','thread','n','ws','comment','reply 😀','bob','user','root','null','c','u');
        UPDATE note SET content=content||' edited', rev=rev+1 WHERE id='n';
        "#,
    )
    .execute(&pool)
    .await
    .unwrap();
    assert!(
        !paging_objects(&pool).await.is_empty(),
        "the 0163 prefix still carries the paging schema"
    );
    let note_before: (String, i64) =
        sqlx::query_as("SELECT content,rev FROM note WHERE workspace_id='ws' AND id='n'")
            .fetch_one(&pool)
            .await
            .unwrap();
    let comments_before = comment_rows(&pool).await;
    pool.close().await;

    let store = Store::open(&tmp.path).await.unwrap();
    // 0164 leaves only the two large tables for the post-startup cleanup.
    assert_eq!(
        paging_object_tables(store.read_pool()).await,
        ["note_page_entry".to_string(), "note_page_head".to_string()].into()
    );
    let cleanup = store.run_retired_note_paging_cleanup(fast_cleanup()).await;
    assert!(cleanup.finished, "{cleanup:?}");
    assert_eq!(cleanup.tables_dropped, 2);
    assert_eq!(
        paging_objects(store.read_pool()).await,
        Vec::<String>::new()
    );
    let note_after: (String, i64) =
        sqlx::query_as("SELECT content,rev FROM note WHERE workspace_id='ws' AND id='n'")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    assert_eq!(note_after, note_before);
    assert_eq!(comment_rows(store.read_pool()).await, comments_before);

    // A comment created after the drop goes through the comment.add path
    // (note rewrite + insert in one transaction) and reads back normally.
    let ws = WorkspaceId::from("ws");
    let mut note = store.get_note(&ws, &NoteId::from("n")).await.unwrap();
    let rev = note.rev;
    note.content.push_str(" <!-- c:new -->");
    let added = sample_comment(&note.id, "new-thread", "new");
    let new_rev = store
        .update_note_with_comment(
            &store.lock_note_content(&note.workspace_id, &note.id).await,
            &note,
            Some(rev),
            &added,
            &version_author(),
        )
        .await
        .unwrap();
    assert_eq!(new_rev, rev + 1);
    let mut reply = sample_comment(&note.id, "new-thread", "new-reply");
    reply.parent_id = Some("new".into());
    reply.anchor = None;
    store.insert_comment(&ws, &reply).await.unwrap();

    let listed = store.list_comments(&note.id).await.unwrap();
    let mut ids: Vec<&str> = listed.iter().map(|c| c.id.as_str()).collect();
    ids.sort_unstable();
    assert_eq!(ids, ["new", "new-reply", "reply", "root"]);
    let created = listed.iter().find(|c| c.id == "new").unwrap();
    assert_eq!(created.content, added.content);
    assert_eq!(created.suggestion_proposed, added.suggestion_proposed);
    let legacy_root = listed.iter().find(|c| c.id == "root").unwrap();
    assert_eq!(legacy_root.is_orphaned, Some(true));

    let mut edited = created.clone();
    edited.content = "edited after the drop".into();
    store.update_comment(&ws, &edited).await.unwrap();
    assert_eq!(
        store.get_comment("new").await.unwrap().content,
        "edited after the drop"
    );
    let resolved = store
        .set_thread_status(&ws, "new-thread", CommentStatus::Resolved, &now_iso())
        .await
        .unwrap();
    assert_eq!(resolved, 2);
    store.delete_comment(&ws, "new-reply").await.unwrap();
    assert_eq!(
        store
            .delete_comment_in_note(&ws, &note.id, "reply")
            .await
            .unwrap(),
        "thread"
    );
    let mut remaining: Vec<String> = store
        .list_comments(&note.id)
        .await
        .unwrap()
        .into_iter()
        .map(|c| c.id)
        .collect();
    remaining.sort_unstable();
    assert_eq!(remaining, ["new", "root"]);
}

/// Rows inserted, updated or deleted on the single write connection since it
/// opened, including rows changed by triggers (`SQLite`'s `total_changes()`).
async fn writer_changes(store: &Store) -> i64 {
    sqlx::query_scalar("SELECT total_changes()")
        .fetch_one(store.write_pool())
        .await
        .unwrap()
}

/// Rows changed per table on the single write connection, triggers included,
/// from `SQLite`'s update hook (which skips `WITHOUT ROWID` tables).
#[derive(Clone, Default)]
struct WriterRowLog(Arc<Mutex<BTreeMap<String, i64>>>);

impl WriterRowLog {
    async fn install(store: &Store) -> Self {
        let log = Self::default();
        let sink = Arc::clone(&log.0);
        let mut conn = store.write_pool().acquire().await.unwrap();
        conn.lock_handle()
            .await
            .unwrap()
            .set_update_hook(move |change| {
                *sink
                    .lock()
                    .unwrap()
                    .entry(change.table.to_string())
                    .or_default() += 1;
            });
        log
    }

    /// The rows logged since the last call, leaving out the full-text index's
    /// own tables (`note_fts*`), whose size follows the note's text.
    fn take_outside_search(&self) -> BTreeMap<String, i64> {
        let mut rows = std::mem::take(&mut *self.0.lock().unwrap());
        rows.retain(|table, _| !table.starts_with("note_fts"));
        rows
    }
}

/// What a save and an unrelated note insert queued behind it changed
/// together: every row (`total_changes()`), and the rows outside the
/// full-text index, per table.
struct SaveRows {
    total: i64,
    outside_search: BTreeMap<String, i64>,
}

/// Save `note` as revision `rev + 1` while an unrelated note insert queues
/// behind it.
async fn save_beside_an_unrelated_write(
    store: &Store,
    log: &WriterRowLog,
    note: &Note,
    rev: i64,
) -> SaveRows {
    let before = writer_changes(store).await;
    log.take_outside_search();
    let saver = store.clone();
    let snapshot = note.clone();
    let save = tokio::spawn(async move {
        let lock = saver
            .lock_note_content(&snapshot.workspace_id, &snapshot.id)
            .await;
        saver
            .update_note_with_version(&lock, &snapshot, Some(rev), &version_author(), &now_iso())
            .await
    });
    tokio::task::yield_now().await;
    store
        .insert_note(&task_note(
            &note.workspace_id,
            &format!("unrelated {rev}"),
            None,
        ))
        .await
        .unwrap();
    let (saved_rev, _) = save.await.unwrap().unwrap();
    assert_eq!(saved_rev, rev + 1);
    SaveRows {
        total: writer_changes(store).await - before,
        outside_search: log.take_outside_search(),
    }
}

/// intent-hq/intent#7515: saving a large note rebuilt its page index while
/// holding the only write connection, so an unrelated write queued behind it
/// for seconds. A save now writes the note row and its history only: outside
/// the full-text index it changes the same rows as a one-line save. Measured
/// in rows, never in wall-clock time: an overloaded runner stretches even a
/// small save past any fixed wait (this test's 6 s budget failed in intentd
/// merge-group run 41295819142).
#[tokio::test]
async fn large_note_save_does_not_block_an_unrelated_write() {
    const LARGE: usize = 5 * 1024 * 1024;
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws_id = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws_id, "WS", false))
        .await
        .unwrap();
    let line = "- [ ] a task line with **markdown** and `code` 😀\n";
    let author = version_author();
    let mut small = task_note(&ws_id, "Small", None);
    small.content = line.to_string();
    let mut large = task_note(&ws_id, "Large", None);
    large.content = line.repeat(LARGE / line.len() + 1);
    let lines = i64::try_from(large.content.lines().count()).unwrap();
    let log = WriterRowLog::install(&store).await;
    for note in [&small, &large] {
        store
            .insert_note_with_version(note, &author, &now_iso())
            .await
            .unwrap();
    }

    for round in 0..3_i64 {
        for note in [&mut small, &mut large] {
            note.content.push_str("round ");
            note.content.push_str(&round.to_string());
            note.content.push('\n');
        }
        let small_rows = save_beside_an_unrelated_write(&store, &log, &small, round).await;
        let large_rows = save_beside_an_unrelated_write(&store, &log, &large, round).await;
        assert!(
            small_rows.outside_search.contains_key("note"),
            "round {round}: the update hook saw no note write: {:?}",
            small_rows.outside_search
        );
        assert_eq!(
            large_rows.outside_search, small_rows.outside_search,
            "round {round}: outside the search index, saving a {lines}-line note \
             changed other rows than saving a one-line note"
        );
        // Backstop for tables the update hook skips: full-text indexing grows
        // with the note (about 750 rows here), a per-line index would not fit.
        let budget = small_rows.total + lines / 20;
        assert!(
            large_rows.total <= budget,
            "round {round}: saving a {lines}-line note changed {} rows \
             (a one-line note {}); budget {budget}",
            large_rows.total,
            small_rows.total
        );
    }
    let stored = store.get_note(&ws_id, &large.id).await.unwrap();
    assert_eq!(stored.content.len(), large.content.len());
}

/// Seed `rows` retired page-index rows under one head for note `n`, as a
/// released daemon would have left them.
async fn seed_retired_index(store: &Store, rows: u32) {
    sqlx::raw_sql(&format!(
        r"
        INSERT INTO workspace(id,title,branch,created_at,updated_at) VALUES ('ws','W','main','c','u');
        INSERT INTO note(id,workspace_id,title,content,rev,created_at,updated_at)
          VALUES ('n','ws','Spec','canonical body',3,'c','u');
        INSERT INTO note_page_head(workspace_id,note_id,instance_id,current_rev,generation)
          VALUES ('ws','n','instance',3,'generation');
        WITH RECURSIVE seq(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM seq WHERE i+1 < {rows})
        INSERT INTO note_page_entry(workspace_id,note_id,collection,position,value,source_start,source_end)
          SELECT 'ws','n','blocks',i,printf('entry %d with some indexed text',i),i,i+1 FROM seq;
        "
    ))
    .execute(store.write_pool())
    .await
    .unwrap();
}

async fn table_exists(store: &Store, table: &str) -> bool {
    sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?)")
        .bind(table)
        .fetch_one(store.read_pool())
        .await
        .unwrap()
}

#[tokio::test]
async fn retired_paging_cleanup_resumes_after_a_restart_and_is_idempotent() {
    use crate::note_paging_cleanup::RetiredNotePagingStep;
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    seed_retired_index(&store, 12_000).await;

    // A daemon that stops after one batch leaves a partial, harmless state.
    assert_eq!(
        store.retired_note_paging_step(5_000).await.unwrap(),
        RetiredNotePagingStep::Deleted {
            table: "note_page_entry",
            rows: 5_000
        }
    );
    let ws = WorkspaceId::from("ws");
    let mut note = store.get_note(&ws, &NoteId::from("n")).await.unwrap();
    note.content = "edited while partially cleared".into();
    store
        .update_note_with_version(
            &store.lock_note_content(&note.workspace_id, &note.id).await,
            &note,
            Some(3),
            &version_author(),
            &now_iso(),
        )
        .await
        .unwrap();
    store.close().await;
    drop(store);

    let store = Store::open(&tmp.path).await.unwrap();
    let outcome = store.run_retired_note_paging_cleanup(fast_cleanup()).await;
    assert!(outcome.finished, "{outcome:?}");
    assert_eq!(
        outcome.rows_removed,
        7_000 + 1,
        "remaining entries plus the head row"
    );
    assert_eq!(outcome.tables_dropped, 2);
    assert!(!table_exists(&store, "note_page_entry").await);
    assert!(!table_exists(&store, "note_page_head").await);
    let stored = store.get_note(&ws, &NoteId::from("n")).await.unwrap();
    assert_eq!(stored.content, "edited while partially cleared");
    assert_eq!(stored.rev, 4);

    // Once both tables are gone every later start is a no-op.
    assert_eq!(
        store.retired_note_paging_step(5_000).await.unwrap(),
        RetiredNotePagingStep::Done
    );
    let again = store.run_retired_note_paging_cleanup(fast_cleanup()).await;
    assert_eq!(
        again,
        crate::note_paging_cleanup::NotePagingCleanupOutcome {
            finished: true,
            max_step: again.max_step,
            ..Default::default()
        }
    );
}

/// Retired entry rows a read sees now; 0 once the table is dropped.
async fn remaining_retired_rows(store: &Store) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM note_page_entry")
        .fetch_one(store.read_pool())
        .await
        .unwrap_or(0)
}

/// Insert `note` the way [`Store::insert_note`] does (one `BEGIN IMMEDIATE`
/// transaction on the single write connection) and return how many retired
/// entry rows remained at the moment this write held the writer.
async fn insert_note_counting_retired_rows(store: &Store, note: &Note) -> i64 {
    let mut conn = store.write_pool().acquire().await.unwrap();
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut *conn)
        .await
        .unwrap();
    let present: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='note_page_entry')",
    )
    .fetch_one(&mut *conn)
    .await
    .unwrap();
    let remaining: i64 = if present {
        sqlx::query_scalar("SELECT count(*) FROM note_page_entry")
            .fetch_one(&mut *conn)
            .await
            .unwrap()
    } else {
        0
    };
    let inserted = crate::note_repo::exec_insert_note(&mut conn, note).await;
    crate::commit_with_rollback_guard(conn, inserted, "commit unrelated note insert")
        .await
        .unwrap();
    remaining
}

/// The cleanup hands the single writer back between short batches, so an
/// unrelated write gets in while it runs instead of queueing behind all of
/// it. Measured in cleanup progress (the retired rows each write sees once
/// it holds the writer), never in wall-clock time: a busy runner stretches
/// any step arbitrarily (one 500-row batch took 8.8 s in intentd
/// merge-group run 41295819142, failing a 3 s step budget).
#[tokio::test]
async fn unrelated_writes_complete_promptly_while_the_cleanup_runs() {
    const SEEDED: i64 = 200_000;
    let batch = i64::from(crate::note_paging_cleanup::NOTE_PAGING_CLEANUP_BATCH_ROWS);
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    seed_retired_index(&store, 200_000).await;
    let cleaner = store.clone();
    let cleanup = tokio::spawn(async move {
        cleaner
            .run_retired_note_paging_cleanup(
                crate::note_paging_cleanup::NotePagingCleanupPolicy::default(),
            )
            .await
    });
    let ws = WorkspaceId::from("ws");
    // (rows a read saw just before the write, rows left once it held the writer)
    let mut writes: Vec<(i64, i64)> = Vec::new();
    let part_way = |writes: &[(i64, i64)]| {
        writes
            .iter()
            .filter(|&&(_, at_write)| at_write > 0 && at_write < SEEDED)
            .count()
    };
    // A few part-way writes prove the point; more would only slow the
    // cleanup, whose batches shrink while it waits for the writer.
    while !cleanup.is_finished() && part_way(&writes) < 5 {
        let before = remaining_retired_rows(&store).await;
        let note = task_note(&ws, &format!("during cleanup {}", writes.len()), None);
        let at_write = insert_note_counting_retired_rows(&store, &note).await;
        writes.push((before, at_write));
        // Pacing only: lets the cleanup run a few batches between writes.
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let outcome = cleanup.await.unwrap();
    assert!(outcome.finished, "{outcome:?}");
    assert_eq!(outcome.rows_removed, 200_001);
    assert_eq!(outcome.tables_dropped, 2);
    assert!(
        part_way(&writes) > 0,
        "no write got the writer while the cleanup was part-way; (rows before, rows at write): {writes:?}"
    );
    // A write waits for at most the batch in flight (plus any that commit
    // during the read before it), never for the rest of the cleanup.
    for (round, &(before, at_write)) in writes.iter().enumerate() {
        if before >= 10 * batch {
            assert!(
                at_write > 0,
                "round {round}: a write issued with {before} rows left waited for the whole cleanup"
            );
        }
    }
    let notes: i64 = sqlx::query_scalar("SELECT count(*) FROM note WHERE workspace_id='ws'")
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    assert_eq!(notes, 1 + i64::try_from(writes.len()).unwrap());
}

async fn retired_rows(store: &Store, workspace: &str) -> (i64, i64) {
    let entries = sqlx::query_scalar("SELECT count(*) FROM note_page_entry WHERE workspace_id=?")
        .bind(workspace)
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    let heads = sqlx::query_scalar("SELECT count(*) FROM note_page_head WHERE workspace_id=?")
        .bind(workspace)
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    (entries, heads)
}

async fn seed_other_workspace_index(store: &Store) {
    sqlx::raw_sql(
        r"
        INSERT INTO workspace(id,title,branch,created_at,updated_at) VALUES ('keep','K','main','c','u');
        INSERT INTO note(id,workspace_id,title,content,rev,created_at,updated_at)
          VALUES ('k','keep','Kept','kept body',1,'c','u');
        INSERT INTO note_page_head(workspace_id,note_id,instance_id,current_rev,generation)
          VALUES ('keep','k','instance',1,'generation');
        WITH RECURSIVE seq(i) AS (SELECT 0 UNION ALL SELECT i+1 FROM seq WHERE i+1 < 10)
        INSERT INTO note_page_entry(workspace_id,note_id,collection,position,value)
          SELECT 'keep','k','blocks',i,'kept entry' FROM seq;
        ",
    )
    .execute(store.write_pool())
    .await
    .unwrap();
}

/// intentd#2588 review: a workspace's retired page rows are removed in
/// statements of at most one batch each (entries before heads), scoped to
/// that workspace, before its notes are deleted.
#[tokio::test]
async fn workspace_retired_page_sweep_is_bounded_per_statement() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    seed_retired_index(&store, 350).await;
    seed_other_workspace_index(&store).await;
    let statements = store
        .delete_workspace_retired_note_paging_in_batches("ws", 100)
        .await
        .unwrap();
    assert_eq!(statements, [100, 100, 100, 50, 1]);
    assert_eq!(retired_rows(&store, "ws").await, (0, 0));
    assert_eq!(retired_rows(&store, "keep").await, (10, 1));
}

/// intentd#2588 review: `delete_workspace` with leftover page-index rows
/// (more than two production batches) completes and leaves no orphans,
/// while another workspace's rows stay for the background cleanup.
#[tokio::test]
async fn workspace_delete_leaves_no_retired_page_orphans() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let rows = crate::note_paging_cleanup::NOTE_PAGING_CLEANUP_BATCH_ROWS * 2 + 2_000;
    seed_retired_index(&store, rows).await;
    seed_other_workspace_index(&store).await;
    store
        .delete_workspace(&WorkspaceId::from("ws"))
        .await
        .unwrap();
    assert_eq!(retired_rows(&store, "ws").await, (0, 0));
    assert_eq!(retired_rows(&store, "keep").await, (10, 1));
    let notes: i64 = sqlx::query_scalar("SELECT count(*) FROM note WHERE workspace_id='ws'")
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    assert_eq!(notes, 0);
}

/// The sweep tolerates the post-startup cleanup having dropped the tables.
#[tokio::test]
async fn workspace_delete_after_the_retired_tables_are_gone() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    seed_retired_index(&store, 10).await;
    let outcome = store.run_retired_note_paging_cleanup(fast_cleanup()).await;
    assert!(outcome.finished, "{outcome:?}");
    assert_eq!(
        store
            .delete_workspace_retired_note_paging_in_batches("ws", 100)
            .await
            .unwrap(),
        Vec::<u64>::new(),
        "missing tables are skipped"
    );
    store
        .delete_workspace(&WorkspaceId::from("ws"))
        .await
        .unwrap();
    let notes: i64 = sqlx::query_scalar("SELECT count(*) FROM note WHERE workspace_id='ws'")
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    assert_eq!(notes, 0);
}
