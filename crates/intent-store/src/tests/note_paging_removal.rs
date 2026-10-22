//! Removal of the unused note paging system (intent-hq/intent#7515).
//!
//! Migrations 0149-0156 stay because released databases applied them; 0164
//! drops every trigger and every small table they created, and the post-startup
//! cleanup ([`crate::note_paging_cleanup`]) removes the two large tables left
//! behind. These tests pin that notes and comments stay intact, that comments
//! keep working afterwards, that the cleanup is resumable and does not block
//! writes, and that a large note save no longer holds the single writer long
//! enough to block an unrelated write.
use std::time::{Duration, Instant};

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
        .update_note_with_comment(&note, Some(rev), &added, &version_author())
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

/// intent-hq/intent#7515: saving a large note rebuilt its page index while
/// holding the only write connection, so an unrelated write queued behind it
/// for seconds. A save now writes the note row and its history only.
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
    let mut note = task_note(&ws_id, "Large", None);
    note.content = line.repeat(LARGE / line.len() + 1);
    let author = version_author();
    store
        .insert_note_with_version(&note, &author, &now_iso())
        .await
        .unwrap();

    // Bounds the wait only; the save itself is a few row writes.
    let bound = intentd_test_support::test_timeout(Duration::from_secs(2));
    for round in 0..3_i64 {
        note.content.push_str("round ");
        note.content.push_str(&round.to_string());
        note.content.push('\n');
        let saver = store.clone();
        let snapshot = note.clone();
        let author = author.clone();
        let save = tokio::spawn(async move {
            saver
                .update_note_with_version(&snapshot, Some(round), &author, &now_iso())
                .await
        });
        tokio::task::yield_now().await;
        let started = Instant::now();
        store
            .insert_note(&task_note(&ws_id, &format!("unrelated {round}"), None))
            .await
            .unwrap();
        let waited = started.elapsed();
        assert!(
            waited < bound,
            "round {round}: unrelated write waited {waited:?} behind a large note save"
        );
        let (rev, _) = save.await.unwrap().unwrap();
        assert_eq!(rev, round + 1);
    }
    let stored = store.get_note(&ws_id, &note.id).await.unwrap();
    assert_eq!(stored.content.len(), note.content.len());
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
        .update_note_with_version(&note, Some(3), &version_author(), &now_iso())
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

#[tokio::test]
async fn unrelated_writes_complete_promptly_while_the_cleanup_runs() {
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
    // Bounds the wait only; one batch holds the writer for about 100 ms.
    let bound = intentd_test_support::test_timeout(Duration::from_secs(1));
    let ws = WorkspaceId::from("ws");
    let mut overlapped = 0;
    for round in 0..10 {
        let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM note_page_entry")
            .fetch_one(store.read_pool())
            .await
            .unwrap_or(0);
        if remaining > 0 && remaining < 200_000 {
            overlapped += 1;
        }
        let started = Instant::now();
        store
            .insert_note(&task_note(&ws, &format!("during cleanup {round}"), None))
            .await
            .unwrap();
        let waited = started.elapsed();
        assert!(
            waited < bound,
            "round {round}: unrelated write waited {waited:?} behind the cleanup"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(overlapped > 0, "writes never overlapped a running cleanup");
    let outcome = cleanup.await.unwrap();
    assert!(outcome.finished, "{outcome:?}");
    assert_eq!(outcome.rows_removed, 200_001);
    assert!(
        outcome.max_step < intentd_test_support::test_timeout(Duration::from_secs(1)),
        "{outcome:?}"
    );
}
