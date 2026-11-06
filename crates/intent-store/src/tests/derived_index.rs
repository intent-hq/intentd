//! Regression coverage for intent-hq/intent#8592: a damaged derived search
//! index makes note writes fail with `(code: 267) database disk image is
//! malformed`. Detection must name the index without writing, and the
//! offline repair must back up the files, rebuild the index from canonical
//! rows, and leave every note intact and writable.

use super::{sample_agent_session, sample_workspace, stray_note, TempDb};
use crate::{
    repair_derived_indexes, DerivedIndex, DiagnosticStore, IntegrityReport, NoteFtsOptions,
    RepairAction, RepairReport, Store,
};
use intent_core::{AgentId, Note, WorkspaceId};
use serde_json::json;
use std::fmt::Write as _;
use std::path::PathBuf;

const NOTES: usize = 12;

/// A store with one workspace, `NOTES` notes whose bodies carry a unique
/// word each, and one agent with two searchable messages.
async fn seeded(tmp: &TempDb) -> (Store, WorkspaceId, Vec<Note>) {
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = WorkspaceId::from("repair");
    store
        .insert_workspace(&sample_workspace(&ws, "Repair", false))
        .await
        .unwrap();
    let mut notes = Vec::new();
    for i in 0..NOTES {
        let mut note = stray_note(&ws, &format!("note-{i}"), &format!("Receipt {i}"));
        note.content = format!("checkpoint body{i} sourceGateProgress{i}\n").repeat(40);
        note.tags = vec![format!("tag{i}")];
        store.insert_note(&note).await.unwrap();
        notes.push(note);
    }
    let agent = AgentId::from("agent-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
    store
        .insert_agent_session(&sample_agent_session(&agent, &ws))
        .await
        .unwrap();
    for (role, text) in [
        ("user", "please review zebrafinch"),
        ("assistant", "approved zebrafinch"),
    ] {
        store
            .append_agent_message(
                &agent,
                role,
                &json!([{ "type": "text", "text": text }]),
                "t",
            )
            .await
            .unwrap();
    }
    (store, ws, notes)
}

/// Overwrite the FTS5 segment pages of `table` with zeroes, the way lost or
/// torn WAL frames leave an index whose structure points at garbage.
async fn damage(store: &Store, table: &str) {
    let damaged = sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {table}_data SET block = zeroblob(length(block)) WHERE id > 10"
    )))
    .execute(store.write_pool())
    .await
    .unwrap()
    .rows_affected();
    assert!(damaged > 0, "{table} has segment rows to damage");
}

async fn note_hits(store: &Store, word: &str) -> crate::Result<usize> {
    store
        .search_notes_fts(word, &NoteFtsOptions::default())
        .await
        .map(|hits| hits.len())
}

async fn message_hits(store: &Store, word: &str) -> usize {
    sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM agent_message_fts WHERE agent_message_fts MATCH ?",
    )
    .bind(word)
    .fetch_one(store.read_pool())
    .await
    .map(|n| usize::try_from(n).unwrap())
    .unwrap()
}

/// Append to each note in turn until one write fails; returns its error.
async fn first_failed_append(store: &Store, notes: &mut [Note]) -> Option<String> {
    for round in 0..4 {
        for note in notes.iter_mut() {
            let mut next = note.clone();
            writeln!(next.content, "receipt index{round} appended").unwrap();
            let lock = store.lock_note_content(&next.workspace_id, &next.id).await;
            match store.update_note(&lock, &next).await {
                Ok(rev) => {
                    next.rev = rev;
                    *note = next;
                }
                Err(e) => return Some(e.to_string()),
            }
        }
    }
    None
}

/// The backup directory, rebuilt indexes and post-repair check of a repair
/// that rebuilt something; panics on any other outcome.
#[track_caller]
fn rebuilt(report: RepairReport) -> (PathBuf, Vec<DerivedIndex>, IntegrityReport) {
    match report.action {
        RepairAction::Rebuilt {
            backup_dir,
            rebuilt,
            after,
        } => (backup_dir, rebuilt, after),
        other => panic!(
            "expected a rebuild, got {other:?} after {:?}",
            report.before
        ),
    }
}

#[tokio::test]
async fn damaged_note_index_is_reported_read_only_and_repaired_without_losing_notes() {
    let tmp = TempDb::new();
    let (store, ws, mut notes) = seeded(&tmp).await;
    damage(&store, "note_fts").await;

    // The symptom from the issue: a note write fails with code 267
    // (SQLITE_CORRUPT_VTAB), at the UPDATE or when FTS5 flushes at COMMIT.
    let err = first_failed_append(&store, &mut notes)
        .await
        .expect("a write through the damaged index fails");
    assert!(
        err.contains("(code: 267) database disk image is malformed"),
        "{err}"
    );

    // Doctor's read-only view names the damaged index while the daemon's
    // store is still open, and blames nothing else.
    let diagnostic = DiagnosticStore::open(&tmp.path).await.unwrap();
    let report = diagnostic.integrity_report().await.unwrap();
    diagnostic.close().await;
    assert_eq!(
        report.damaged_indexes(),
        vec![DerivedIndex::NoteSearch],
        "{report:?}"
    );
    assert!(!report.has_unrepairable(), "{report:?}");
    assert!(report
        .problems
        .iter()
        .all(|p| p.table.as_deref() == Some("note_fts")));
    store.close().await;
    drop(store);

    let backups = tempfile::tempdir().unwrap();
    let repair = repair_derived_indexes(&tmp.path, backups.path(), false)
        .await
        .unwrap();
    assert_eq!(
        repair.before.damaged_indexes(),
        vec![DerivedIndex::NoteSearch]
    );
    let (backup_dir, indexes, after) = rebuilt(repair);
    assert_eq!(indexes, vec![DerivedIndex::NoteSearch]);
    assert!(after.is_ok(), "{after:?}");
    let backup_db = backup_dir.join(tmp.path.file_name().unwrap());
    assert!(backup_dir.starts_with(backups.path()));
    assert!(
        std::fs::metadata(&backup_db).unwrap().len() > 0,
        "the pre-repair database was copied"
    );

    // Every note survived with the content of its last successful write,
    // search covers bodies and tags again, and writes work.
    let store = Store::open(&tmp.path).await.unwrap();
    for (i, note) in notes.iter_mut().enumerate() {
        let stored = store.get_note(&ws, &note.id).await.unwrap();
        assert_eq!(stored.content, note.content, "note {i} content preserved");
        assert_eq!(note_hits(&store, &format!("body{i}")).await.unwrap(), 1);
        assert_eq!(note_hits(&store, &format!("tag{i}")).await.unwrap(), 1);
    }
    assert!(first_failed_append(&store, &mut notes).await.is_none());
    assert_eq!(note_hits(&store, "index3").await.unwrap(), NOTES);
    assert_eq!(
        message_hits(&store, "zebrafinch").await,
        2,
        "untouched index kept"
    );
}

#[tokio::test]
async fn damaged_message_index_is_attributed_and_rebuilt() {
    let tmp = TempDb::new();
    let (store, _, _) = seeded(&tmp).await;
    damage(&store, "agent_message_fts").await;
    store.close().await;
    drop(store);

    let backups = tempfile::tempdir().unwrap();
    let repair = repair_derived_indexes(&tmp.path, backups.path(), false)
        .await
        .unwrap();
    let (_, indexes, after) = rebuilt(repair);
    assert_eq!(indexes, vec![DerivedIndex::MessageSearch]);
    assert!(after.is_ok(), "{after:?}");
    let store = Store::open(&tmp.path).await.unwrap();
    assert_eq!(message_hits(&store, "zebrafinch").await, 2);
}

#[tokio::test]
async fn forced_rebuild_of_a_healthy_store_matches_the_trigger_maintained_index() {
    let tmp = TempDb::new();
    let (store, _, _) = seeded(&tmp).await;
    let words = ["checkpoint", "body3", "tag7", "receipt"];
    let mut before = Vec::new();
    for word in words {
        before.push(note_hits(&store, word).await.unwrap());
    }
    store.close().await;
    drop(store);

    let backups = tempfile::tempdir().unwrap();
    let unforced = repair_derived_indexes(&tmp.path, backups.path(), false)
        .await
        .unwrap();
    assert!(unforced.before.is_ok());
    assert!(matches!(unforced.action, RepairAction::NothingToDo));
    assert_eq!(
        std::fs::read_dir(backups.path()).unwrap().count(),
        0,
        "a healthy database is neither copied nor changed"
    );

    let (first_backup, indexes, after) = rebuilt(
        repair_derived_indexes(&tmp.path, backups.path(), true)
            .await
            .unwrap(),
    );
    assert_eq!(indexes, DerivedIndex::ALL.to_vec());
    assert!(after.is_ok());
    let (second_backup, _, _) = rebuilt(
        repair_derived_indexes(&tmp.path, backups.path(), true)
            .await
            .unwrap(),
    );
    assert_ne!(first_backup, second_backup, "each run has its own backup");

    let store = Store::open(&tmp.path).await.unwrap();
    let mut after = Vec::new();
    for word in words {
        after.push(note_hits(&store, word).await.unwrap());
    }
    assert_eq!(after, before);
    assert_eq!(message_hits(&store, "zebrafinch").await, 2);
}

#[tokio::test]
async fn repair_refuses_while_a_daemon_owns_the_database() {
    let tmp = TempDb::new();
    Store::open(&tmp.path).await.unwrap().close().await;
    let daemon = Store::open_for_daemon(&tmp.path).await.unwrap();
    let backups = tempfile::tempdir().unwrap();
    let err = repair_derived_indexes(&tmp.path, backups.path(), true)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("stop intentd"), "{err}");
    assert_eq!(
        std::fs::read_dir(backups.path()).unwrap().count(),
        0,
        "no backup is taken before ownership is held"
    );
    daemon.close().await;
}

#[tokio::test]
async fn repair_refuses_a_schema_from_another_build() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    sqlx::query(
        "INSERT INTO _sqlx_migrations (version, description, installed_on, success, checksum, execution_time) \
         VALUES (999999, 'future', CURRENT_TIMESTAMP, 1, x'00', 0)",
    )
    .execute(store.write_pool())
    .await
    .unwrap();
    store.close().await;
    drop(store);
    let backups = tempfile::tempdir().unwrap();
    let err = repair_derived_indexes(&tmp.path, backups.path(), true)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("does not match this intentd build"), "{err}");
}

#[tokio::test]
async fn repair_of_a_missing_database_creates_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("absent.db");
    let err = repair_derived_indexes(&path, &dir.path().join("backups"), false)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("no database"), "{err}");
    assert!(!path.exists());
}

#[tokio::test]
async fn read_pool_rejects_writes() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let err = sqlx::query("CREATE TABLE stray_write (x)")
        .execute(store.read_pool())
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("readonly"), "{err}");
    // Reads still work on the same pool.
    let one: i64 = sqlx::query_scalar("SELECT 1")
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    assert_eq!(one, 1);
}

#[tokio::test]
async fn owned_read_pool_rejects_writes() {
    let tmp = TempDb::new();
    let store = Store::open_for_daemon(&tmp.path).await.unwrap();
    let err = sqlx::query("CREATE TABLE stray_write (x)")
        .execute(store.read_pool())
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("readonly"), "{err}");
    store.close().await;
}

/// Review finding: a rebuild allocates and frees pages, so with page-level
/// damage in a canonical table it could overwrite note pages. The repair
/// must refuse before any copy or write, even with `force`.
#[tokio::test]
async fn repair_changes_nothing_when_a_canonical_table_is_damaged_too() {
    use std::io::{Seek, SeekFrom, Write};
    let tmp = TempDb::new();
    let (store, _, _) = seeded(&tmp).await;
    damage(&store, "note_fts").await;
    let root: i64 = sqlx::query_scalar(
        "SELECT rootpage FROM sqlite_schema WHERE type = 'table' AND name = 'note'",
    )
    .fetch_one(store.read_pool())
    .await
    .unwrap();
    let page_size: i64 = sqlx::query_scalar("PRAGMA page_size")
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    store.close().await;
    drop(store);
    // Overwrite the B-tree page header of `note`'s root page.
    {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(&tmp.path)
            .unwrap();
        file.seek(SeekFrom::Start(
            u64::try_from((root - 1) * page_size).unwrap(),
        ))
        .unwrap();
        file.write_all(&[0xFF; 8]).unwrap();
        file.sync_all().unwrap();
    }
    let bytes_before = std::fs::read(&tmp.path).unwrap();

    let backups = tempfile::tempdir().unwrap();
    match repair_derived_indexes(&tmp.path, backups.path(), true).await {
        Ok(report) => {
            assert!(
                matches!(report.action, RepairAction::RefusedOtherDamage),
                "{report:?}"
            );
            assert!(report.before.has_unrepairable());
            assert!(
                report
                    .before
                    .problems
                    .iter()
                    .any(|p| p.repairable.is_none() && p.table.as_deref() == Some("note")),
                "{:?}",
                report.before
            );
        }
        Err(e) => assert!(e.to_string().contains("nothing was changed"), "{e}"),
    }
    assert_eq!(
        std::fs::read(&tmp.path).unwrap(),
        bytes_before,
        "the database file is untouched"
    );
    assert_eq!(std::fs::read_dir(backups.path()).unwrap().count(), 0);
}

/// Review finding: the backup must be a usable snapshot, including commits
/// that only exist in the WAL when the repair starts.
#[tokio::test]
async fn backup_includes_commits_still_in_the_wal() {
    use sqlx::Connection;
    let tmp = TempDb::new();
    let (store, ws, notes) = seeded(&tmp).await;
    damage(&store, "note_fts").await;
    store.close().await;
    drop(store);
    // An idle connection with autocheckpoint off keeps a commit in the WAL.
    let mut other = sqlx::SqliteConnection::connect_with(
        &sqlx::sqlite::SqliteConnectOptions::new().filename(&tmp.path),
    )
    .await
    .unwrap();
    sqlx::query("PRAGMA wal_autocheckpoint = 0")
        .execute(&mut other)
        .await
        .unwrap();
    sqlx::query("UPDATE workspace SET title = 'only-in-wal' WHERE id = ?")
        .bind(&ws.0)
        .execute(&mut other)
        .await
        .unwrap();
    let mut wal = tmp.path.clone().into_os_string();
    wal.push("-wal");
    let wal = PathBuf::from(wal);
    assert!(
        std::fs::metadata(&wal).unwrap().len() > 0,
        "commit is in the WAL"
    );

    let backups = tempfile::tempdir().unwrap();
    let (backup_dir, _, after) = rebuilt(
        repair_derived_indexes(&tmp.path, backups.path(), false)
            .await
            .unwrap(),
    );
    assert!(after.is_ok(), "{after:?}");
    assert!(backup_dir.join(wal.file_name().unwrap()).is_file());
    other.close().await.unwrap();

    let copy = Store::open(&backup_dir.join(tmp.path.file_name().unwrap()))
        .await
        .unwrap();
    let title: String = sqlx::query_scalar("SELECT title FROM workspace WHERE id = ?")
        .bind(&ws.0)
        .fetch_one(copy.read_pool())
        .await
        .unwrap();
    assert_eq!(title, "only-in-wal");
    for note in &notes {
        assert_eq!(
            copy.get_note(&ws, &note.id).await.unwrap().content,
            note.content
        );
    }
    copy.close().await;
}

/// Review finding: the note rebuild re-syncs `note_search_ctx` — orphaned
/// rows are dropped, missing rows added and drifted filter columns fixed.
#[tokio::test]
async fn note_rebuild_resyncs_search_context() {
    let tmp = TempDb::new();
    let (store, ws, _) = seeded(&tmp).await;
    {
        let mut conn = store.write_pool().acquire().await.unwrap();
        sqlx::query("PRAGMA foreign_keys = OFF")
            .execute(&mut *conn)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO note_search_ctx(note_id, workspace_id, is_archived, updated_at) \
             VALUES ('gone', ?, 0, 't')",
        )
        .bind(&ws.0)
        .execute(&mut *conn)
        .await
        .unwrap();
        sqlx::query("PRAGMA foreign_keys = ON")
            .execute(&mut *conn)
            .await
            .unwrap();
        sqlx::query("DELETE FROM note_search_ctx WHERE note_id = 'note-1'")
            .execute(&mut *conn)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE note_search_ctx SET is_archived = 1, updated_at = 'stale' \
             WHERE note_id = 'note-2'",
        )
        .execute(&mut *conn)
        .await
        .unwrap();
    }
    assert_eq!(note_hits(&store, "body1").await.unwrap(), 0);
    store.close().await;
    drop(store);

    let backups = tempfile::tempdir().unwrap();
    let (_, _, after) = rebuilt(
        repair_derived_indexes(&tmp.path, backups.path(), true)
            .await
            .unwrap(),
    );
    assert!(after.is_ok(), "{after:?}");

    let store = Store::open(&tmp.path).await.unwrap();
    assert_eq!(note_hits(&store, "body1").await.unwrap(), 1);
    let orphans: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM note_search_ctx WHERE note_id = 'gone'")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    assert_eq!(orphans, 0);
    let drifted: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM note_search_ctx c JOIN note n \
           ON n.id = c.note_id AND n.workspace_id = c.workspace_id \
         WHERE c.is_archived IS NOT n.is_archived OR c.updated_at IS NOT n.updated_at",
    )
    .fetch_one(store.read_pool())
    .await
    .unwrap();
    assert_eq!(drifted, 0);
    store.close().await;
}

/// Review finding: `SQLite` joins all B-tree errors into one newline-separated
/// row, so each line is attributed on its own.
#[tokio::test]
async fn integrity_lines_are_split_and_attributed() {
    use crate::derived_index::{attribute, problem_lines, word_after};
    let tmp = TempDb::new();
    Store::open(&tmp.path).await.unwrap().close().await;
    let diagnostic = DiagnosticStore::open(&tmp.path).await.unwrap();
    let pool = diagnostic.read_pool();
    let root = |table: &'static str| async move {
        sqlx::query_scalar::<_, i64>(
            "SELECT rootpage FROM sqlite_schema WHERE type = 'table' AND name = ?",
        )
        .bind(table)
        .fetch_one(pool)
        .await
        .unwrap()
    };
    let (note_root, workspace_root) = (root("note").await, root("workspace").await);
    let index: String = sqlx::query_scalar(
        "SELECT name FROM sqlite_schema WHERE type = 'index' AND tbl_name = 'note_search_ctx' \
         ORDER BY name LIMIT 1",
    )
    .fetch_one(pool)
    .await
    .unwrap();
    let lines = problem_lines(&[
        format!(
            "*** in database main ***\nTree {note_root} page {note_root} cell 0: invalid page number\n\
             Tree {workspace_root} page 9: btreeInitPage() returns error code 11"
        ),
        "malformed inverted index for FTS5 table main.note_fts".to_string(),
        "unable to validate the inverted index for FTS5 table main.agent_message_fts: \
         database disk image is malformed"
            .to_string(),
        format!("row 5 missing from index {index}"),
        format!("wrong # of entries in index {index}"),
    ]);
    assert_eq!(lines.len(), 6, "{lines:?}");
    let mut attributed = Vec::new();
    for line in &lines {
        let (table, index) = attribute(pool, line).await;
        attributed.push((table, index));
    }
    let table = |name: &str| Some(name.to_string());
    assert_eq!(
        attributed,
        vec![
            (table("note"), None),
            (table("workspace"), None),
            (table("note_fts"), Some(DerivedIndex::NoteSearch)),
            (
                table("agent_message_fts"),
                Some(DerivedIndex::MessageSearch)
            ),
            (table("note_search_ctx"), None),
            (table("note_search_ctx"), None),
        ]
    );
    assert_eq!(
        word_after("Tree 12 page 3", "Tree "),
        Some("12".to_string())
    );
    assert_eq!(word_after("Tree ", "Tree "), None);
    assert_eq!(word_after("no tree here", "Tree "), None);
    diagnostic.close().await;
}
