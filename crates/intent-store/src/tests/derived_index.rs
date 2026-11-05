//! Regression coverage for intent-hq/intent#8592: a damaged derived search
//! index makes note writes fail with `(code: 267) database disk image is
//! malformed`. Detection must name the index without writing, and the
//! offline repair must back up the files, rebuild the index from canonical
//! rows, and leave every note intact and writable.

use super::{sample_agent_session, sample_workspace, stray_note, TempDb};
use crate::{repair_derived_indexes, DerivedIndex, DiagnosticStore, NoteFtsOptions, Store};
use intent_core::{AgentId, Note, WorkspaceId};
use serde_json::json;
use std::fmt::Write as _;

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
    let damaged = sqlx::query(&format!(
        "UPDATE {table}_data SET block = zeroblob(length(block)) WHERE id > 10"
    ))
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
    assert_eq!(repair.rebuilt, vec![DerivedIndex::NoteSearch]);
    assert!(repair.after.is_ok(), "{:?}", repair.after);
    let backup_db = repair.backup_dir.join(tmp.path.file_name().unwrap());
    assert!(repair.backup_dir.starts_with(backups.path()));
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
    assert_eq!(
        repair.rebuilt,
        vec![DerivedIndex::MessageSearch],
        "{:?}",
        repair.before
    );
    assert!(repair.after.is_ok(), "{:?}", repair.after);
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
    assert!(unforced.before.is_ok() && unforced.rebuilt.is_empty());

    let forced = repair_derived_indexes(&tmp.path, backups.path(), true)
        .await
        .unwrap();
    assert_eq!(forced.rebuilt, DerivedIndex::ALL.to_vec());
    assert!(forced.after.is_ok());
    assert_ne!(
        forced.backup_dir, unforced.backup_dir,
        "each run has its own backup"
    );

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
