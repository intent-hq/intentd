//! Startup preserves projections; demand rebuilds each retired note exactly once.
use super::{assert_error, page, request, setup, Store};
use intent_core::note_page::NotePageError;
use serde_json::{json, Value};

const DERIVED: [&str; 4] = [
    "note_page_head",
    "note_page_piece",
    "note_page_entry",
    "note_artifact_source",
];

// Persistent triggers survive closing both pools, and catch delete/reinsert even
// if a rebuild happens to reproduce identical rows. No timing or WAL-size proxy.
async fn audit_writes(store: &Store) {
    sqlx::query("CREATE TABLE reopen_audit (table_name TEXT, operation TEXT)")
        .execute(store.write_pool())
        .await
        .unwrap();
    for table in DERIVED.into_iter().chain(["note"]) {
        for operation in ["INSERT", "UPDATE", "DELETE"] {
            sqlx::query(&format!(
                "CREATE TRIGGER audit_{table}_{operation} AFTER {operation} ON {table} BEGIN \
                 INSERT INTO reopen_audit VALUES ('{table}', '{operation}'); END"
            ))
            .execute(store.write_pool())
            .await
            .unwrap();
        }
    }
}

async fn writes(store: &Store) -> Vec<(String, String)> {
    sqlx::query_as("SELECT table_name,operation FROM reopen_audit ORDER BY rowid")
        .fetch_all(store.read_pool())
        .await
        .unwrap()
}

// Snapshot every column, including raw revision, generation and derived payloads.
async fn rows(store: &Store, table: &str) -> Vec<String> {
    let columns: Vec<String> = sqlx::query_scalar("SELECT name FROM pragma_table_info(?)")
        .bind(table)
        .fetch_all(store.read_pool())
        .await
        .unwrap();
    assert!(!columns.is_empty());
    let columns = columns
        .iter()
        .map(|name| format!("\"{name}\""))
        .collect::<Vec<_>>()
        .join(",");
    sqlx::query_scalar(&format!(
        "SELECT json_array({columns}) AS snapshot FROM {table} ORDER BY snapshot"
    ))
    .fetch_all(store.read_pool())
    .await
    .unwrap()
}

async fn derived_rows(store: &Store) -> Vec<Vec<String>> {
    let mut snapshot = Vec::new();
    for table in DERIVED {
        snapshot.push(rows(store, table).await);
    }
    snapshot
}

async fn fixture() -> (Store, super::TempDb) {
    let source = format!(
        "<table><tr><td>one &amp; two 😀</td></tr></table>\n\n\
         <div data-type=\"mermaid-block\" data-mermaid-code=\"graph TD\nA-->B\"></div>\n\n\
         [Task](intent://local/task/example)\n\n{}",
        "e\u{301} text\r\n".repeat(700)
    );
    let (store, tmp, mut note) = setup(&source).await;
    note.id = "empty".into();
    note.content.clear();
    store.insert_note(&note).await.unwrap();
    for table in DERIVED {
        assert!(!rows(&store, table).await.is_empty(), "fixture: {table}");
    }
    (store, tmp)
}

async fn assert_expired(store: &Store, original: &Value) {
    assert_error(
        store
            .read_note_page(
                "pages",
                "spec",
                "alice",
                request(json!({"kind":"context","contextRef":original["contextRef"]})),
                &json!(1),
            )
            .await,
        NotePageError::Expired,
    );
}

#[tokio::test]
async fn equivalent_profile_reopen_preserves_rows_and_performs_no_index_writes() {
    let (store, tmp) = fixture().await;
    let original = page(&store, json!({"kind":"source"})).await;
    let canonical = rows(&store, "note").await;
    let derived = derived_rows(&store).await;
    audit_writes(&store).await;
    store.close().await;
    drop(store);

    // Use the daemon-owned startup entry point too: both delegate to the same
    // migration path, with independent freshly opened pools and no conversion.
    let reopened = Store::open_for_daemon(&tmp.path).await.unwrap();
    assert!(writes(&reopened).await.is_empty());
    assert_eq!(rows(&reopened, "note").await, canonical);
    assert_eq!(derived_rows(&reopened).await, derived);
    let current = page(&reopened, json!({"kind":"source"})).await;
    assert_eq!(current["sourceRevision"], original["sourceRevision"]);
    assert_eq!(current["text"], original["text"]);
    assert_eq!(current["scope"], original["scope"]);
    reopened.close().await;
}

async fn retired_profile_rebuilds_once(retired: &str) {
    let (store, tmp) = fixture().await;
    let original = page(&store, json!({"kind":"source"})).await;
    let canonical = rows(&store, "note").await;
    let entries = rows(&store, "note_page_entry").await;
    let artifacts = rows(&store, "note_artifact_source").await;
    let generations: Vec<String> =
        sqlx::query_scalar("SELECT content_generation FROM note_page_head ORDER BY note_id")
            .fetch_all(store.read_pool())
            .await
            .unwrap();
    assert_ne!(retired, crate::note_page_index::profile_revision());
    sqlx::query("UPDATE note_page_head SET profile_revision=?")
        .bind(retired)
        .execute(store.write_pool())
        .await
        .unwrap();
    assert_expired(&store, &original).await;
    let retired_rows = derived_rows(&store).await;
    audit_writes(&store).await;
    store.close().await;
    drop(store);

    let reopened = Store::open(&tmp.path).await.unwrap();
    assert!(writes(&reopened).await.is_empty());
    assert_eq!(derived_rows(&reopened).await, retired_rows);
    assert_eq!(rows(&reopened, "note").await, canonical);
    assert_expired(&reopened, &original).await;
    assert!(
        writes(&reopened).await.is_empty(),
        "expired references create no demand"
    );

    super::demand_rebuild::settled(&reopened).await.unwrap();
    let current = page(&reopened, json!({"kind":"source"})).await;
    assert_eq!(current["sourceRevision"], original["sourceRevision"]);
    assert_eq!(current["text"], original["text"]);
    let mutations = writes(&reopened).await;
    assert!(!mutations.iter().any(|(table, _)| table == "note"));
    for table in ["note_page_piece", "note_page_entry", "note_artifact_source"] {
        for operation in ["DELETE", "INSERT"] {
            assert!(
                mutations.contains(&(table.into(), operation.into())),
                "missing rebuild evidence: {table} {operation}"
            );
        }
    }
    let heads: Vec<(String, String, String, i64, i64)> = sqlx::query_as(
        "SELECT note_id,content_generation,profile_revision,current_rev,indexed_rev \
         FROM note_page_head ORDER BY note_id",
    )
    .fetch_all(reopened.read_pool())
    .await
    .unwrap();
    assert_eq!(heads.len(), generations.len());
    for ((id, generation, profile, current, indexed), previous) in heads.iter().zip(generations) {
        if id == "spec" {
            assert_ne!(*generation, previous);
            assert_eq!(profile, crate::note_page_index::profile_revision());
        } else {
            assert_eq!(id, "empty");
            assert_eq!(
                *generation, previous,
                "unopened note retains its generation"
            );
            assert_eq!(profile, retired, "unopened note remains retired");
        }
        assert_eq!(current, indexed);
    }
    assert_eq!(rows(&reopened, "note").await, canonical);
    assert_eq!(rows(&reopened, "note_page_entry").await, entries);
    assert_eq!(rows(&reopened, "note_artifact_source").await, artifacts);
    assert_expired(&reopened, &original).await;
    let rebuilt = derived_rows(&reopened).await;
    sqlx::query("DELETE FROM reopen_audit")
        .execute(reopened.write_pool())
        .await
        .unwrap();
    reopened.close().await;
    drop(reopened);

    let reused = Store::open(&tmp.path).await.unwrap();
    assert!(writes(&reused).await.is_empty());
    assert_eq!(rows(&reused, "note").await, canonical);
    assert_eq!(derived_rows(&reused).await, rebuilt);
    assert_expired(&reused, &original).await;
    assert_eq!(
        page(&reused, json!({"kind":"source"})).await["sourceRevision"],
        original["sourceRevision"]
    );
    assert!(
        writes(&reused).await.is_empty(),
        "ready note is reused after restart"
    );

    // Even across restart, the unopened empty note stays retired until accessed.
    let empty_profile: String =
        sqlx::query_scalar("SELECT profile_revision FROM note_page_head WHERE note_id='empty'")
            .fetch_one(reused.read_pool())
            .await
            .unwrap();
    assert_eq!(empty_profile, retired);
    let empty = reused
        .read_note_page(
            "pages",
            "empty",
            "alice",
            request(json!({"kind":"source"})),
            &json!(1),
        )
        .await
        .unwrap();
    assert_eq!(empty["text"], "");
    assert_eq!(rows(&reused, "note").await, canonical);
    let after_empty = derived_rows(&reused).await;
    sqlx::query("DELETE FROM reopen_audit")
        .execute(reused.write_pool())
        .await
        .unwrap();
    reused
        .ensure_note_index("pages", "empty", true)
        .await
        .unwrap();
    assert!(writes(&reused).await.is_empty());
    assert_eq!(derived_rows(&reused).await, after_empty);
    reused.close().await;
}

#[tokio::test]
async fn changed_profile_reopen_defers_each_rebuild_until_demand_then_reuses() {
    retired_profile_rebuilds_once("retired-parser-profile").await;
}

#[tokio::test]
async fn legacy_profile_reopen_defers_each_rebuild_until_demand_then_reuses() {
    // Actual whole-lock profile shipped in v0.10.46 (b6fc9f881). There is no
    // compatibility relabel: each note transitions once, only on demand.
    retired_profile_rebuilds_once(
        "c79f8abc1ce46156d4a032355bf4946cec1ea4813c1e111e403260a6f8949476",
    )
    .await;
}
