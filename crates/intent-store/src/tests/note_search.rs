use super::{sample_workspace, stray_note, TempDb};
use crate::{NoteFtsMatch, NoteFtsOptions, Store};
use intent_core::{Note, WorkspaceId};
use sqlx::Row;

async fn workspace(store: &Store, id: &str, archived: bool) -> WorkspaceId {
    let id = WorkspaceId::from(id);
    store
        .insert_workspace(&sample_workspace(&id, "Search", archived))
        .await
        .unwrap();
    id
}

async fn note(store: &Store, ws: &WorkspaceId, id: &str, title: &str, body: &str) -> Note {
    let mut note = stray_note(ws, id, title);
    note.content = body.into();
    note.updated_at = "2026-01-01T00:00:00Z".into();
    store.insert_note(&note).await.unwrap();
    note
}

async fn hits(store: &Store, query: &str) -> Vec<NoteFtsMatch> {
    store
        .search_notes_fts(query, &NoteFtsOptions::default())
        .await
        .unwrap()
}

async fn drop_index(store: &Store) {
    sqlx::raw_sql(
        "DROP TRIGGER note_fts_after_insert; DROP TRIGGER note_fts_after_delete;
        DROP TRIGGER note_search_ctx_after_update; DROP TRIGGER note_fts_after_update;
        DROP TABLE note_fts; DROP TABLE note_search_ctx;",
    )
    .execute(store.write_pool())
    .await
    .unwrap();
}

#[tokio::test]
async fn note_fts_body_only_match_is_indexed() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = WorkspaceId::from("indexed");
    store
        .insert_workspace(&sample_workspace(&ws, "Indexed", false))
        .await
        .unwrap();
    let mut note = stray_note(&ws, "spec", "Roadmap");
    note.content = "A quokka appears only in the body".into();
    store.insert_note(&note).await.unwrap();
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM note_fts WHERE note_fts MATCH 'quokka'")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    assert_eq!(count, 1);
}

#[tokio::test]
async fn note_fts_lifecycle_and_composite_identity() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let a = workspace(&store, "a", false).await;
    let b = workspace(&store, "b", false).await;
    let mut first = note(&store, &a, "spec", "Oldtitle", "shared oldbody").await;
    note(&store, &b, "spec", "Other", "shared oldbody").await;
    let both = hits(&store, "oldbody").await;
    assert_eq!(
        both.iter()
            .map(|h| (h.workspace_id.as_str(), h.note_id.as_str()))
            .collect::<Vec<_>>(),
        vec![("a", "spec"), ("b", "spec")]
    );
    first.title = "Newtitle".into();
    first.content = "shared freshbody".into();
    first.tags = vec!["unicorn".into(), "café".into()];
    store.update_note(&first).await.unwrap();
    assert!(hits(&store, "oldtitle").await.is_empty());
    assert_eq!(hits(&store, "oldbody").await[0].workspace_id, "b");
    for query in ["newtitle", "freshbody", "unicorn", "cafe"] {
        let got = hits(&store, query).await;
        assert_eq!(got.len(), 1, "{query}");
        assert_eq!(got[0].workspace_id, "a");
        assert_eq!(got[0].content, first.content);
        assert_eq!(got[0].tags, first.tags);
    }
    first.tags = vec!["narwhal".into()];
    // Metadata-only writes must not reindex an old caller's body.
    first.content = "stale caller body".into();
    store.update_note_metadata(&first).await.unwrap();
    assert!(hits(&store, "unicorn").await.is_empty());
    assert_eq!(hits(&store, "narwhal").await.len(), 1);
    assert_eq!(hits(&store, "freshbody").await.len(), 1);
    store.delete_note(&a, &first.id).await.unwrap();
    assert!(hits(&store, "narwhal").await.is_empty());
    assert_eq!(hits(&store, "oldbody").await.len(), 1);
    store.delete_workspace(&b).await.unwrap();
    assert!(hits(&store, "shared").await.is_empty());
    for table in ["note_fts", "note_search_ctx"] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(store.read_pool())
            .await
            .unwrap();
        assert_eq!(count, 0);
    }
}

#[tokio::test]
async fn note_fts_migration_backfills_and_rolls_back_atomically() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    drop_index(&store).await;
    let a = workspace(&store, "a", false).await;
    let b = workspace(&store, "b", false).await;
    let mut n = note(&store, &a, "spec", "Backfilltitle", "backfillbody").await;
    n.tags = vec!["backfilltag".into()];
    n.is_archived = true;
    store.update_note(&n).await.unwrap();
    note(&store, &b, "spec", "Other", "backfillbody").await;
    let sql = include_str!("../../migrations/0141_note_fts.sql");
    // A failed migration transaction must leave original notes intact and no
    // half-populated derived tables. Then apply the same SQL successfully.
    let mut tx = store.write_pool().begin().await.unwrap();
    sqlx::raw_sql(sql).execute(&mut *tx).await.unwrap();
    assert!(sqlx::query("INSERT INTO no_such_table VALUES (1)")
        .execute(&mut *tx)
        .await
        .is_err());
    tx.rollback().await.unwrap();
    let exists: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE name='note_fts'")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    assert_eq!(exists, 0);
    let mut tx = store.write_pool().begin().await.unwrap();
    sqlx::raw_sql(sql).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!(hits(&store, "backfillbody").await.len(), 2);
    assert_eq!(hits(&store, "backfilltitle").await.len(), 1);
    assert!(hits(&store, "backfilltag").await[0].is_archived);
    assert_eq!(
        store.get_note(&a, &n.id).await.unwrap().content,
        "backfillbody"
    );
}

#[tokio::test]
async fn note_fts_permissions_scope_and_archive_filter_before_top_n() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let denied = workspace(&store, "denied", false).await;
    let allowed = workspace(&store, "allowed", true).await;
    // More high-ranking inaccessible rows than the requested result count.
    for i in 0..20 {
        note(&store, &denied, &format!("denied-{i}"), "needle", "needle").await;
    }
    let mut archived = note(&store, &allowed, "archived", "needle", "needle").await;
    archived.is_archived = true;
    store.update_note_metadata(&archived).await.unwrap();
    note(&store, &allowed, "visible", "Other", "needle").await;
    let permit = [allowed.clone()];
    let mut options = NoteFtsOptions {
        allowed_workspace_ids: Some(&permit),
        prefer_workspace_id: Some(&denied),
        include_archived: false,
        limit: Some(1),
        ..Default::default()
    };
    let got = store.search_notes_fts("needle", &options).await.unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].note_id, "visible");
    assert!(got[0].workspace_archived);
    assert!(!got[0].is_archived);
    options.include_archived = true;
    options.limit = None;
    assert_eq!(
        store
            .search_notes_fts("needle", &options)
            .await
            .unwrap()
            .len(),
        2
    );
    options.workspace_id = Some(&denied);
    assert!(store
        .search_notes_fts("needle", &options)
        .await
        .unwrap()
        .is_empty());
    options.workspace_id = Some(&allowed);
    options.include_archived = false;
    archived.is_archived = false;
    store.update_note_metadata(&archived).await.unwrap();
    assert_eq!(
        store
            .search_notes_fts("needle", &options)
            .await
            .unwrap()
            .len(),
        2
    );
    options.workspace_id = None;
    options.allowed_workspace_ids = Some(&[]);
    assert!(store
        .search_notes_fts("needle", &options)
        .await
        .unwrap()
        .is_empty());
    options.allowed_workspace_ids = None;
    assert_eq!(
        store
            .search_notes_fts("needle", &options)
            .await
            .unwrap()
            .len(),
        22
    );
    options.limit = Some(0);
    assert!(store
        .search_notes_fts("needle", &options)
        .await
        .unwrap()
        .is_empty());
    options.limit = Some(-1);
    assert!(store.search_notes_fts("needle", &options).await.is_err());
}

#[tokio::test]
async fn note_fts_ranking_weights_and_stable_ties() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let a = workspace(&store, "a", false).await;
    let b = workspace(&store, "b", false).await;
    // Same length/frequency: field weights alone decide title > tag > body.
    note(&store, &a, "title", "needle", "padding").await;
    note(&store, &a, "body", "padding", "needle").await;
    let mut tag = note(&store, &a, "tag", "padding", "").await;
    tag.tags = vec!["needle".into()];
    store.update_note_metadata(&tag).await.unwrap();
    let ranked = hits(&store, "needle").await;
    assert_eq!(
        ranked
            .iter()
            .map(|h| h.note_id.as_str())
            .collect::<Vec<_>>(),
        ["title", "tag", "body"]
    );
    assert!(ranked[0].rank < ranked[1].rank && ranked[1].rank < ranked[2].rank);
    // Reverse insertion order and timestamps: cutoff obeys all tie-breaks.
    note(&store, &b, "a", "same", "tie").await;
    note(&store, &a, "z", "same", "tie").await;
    note(&store, &a, "a", "same", "tie").await;
    let mut newest = note(&store, &b, "new", "same", "tie").await;
    newest.updated_at = "2026-02-01T00:00:00Z".into();
    store.update_note_metadata(&newest).await.unwrap();
    let options = NoteFtsOptions {
        limit: Some(2),
        ..Default::default()
    };
    let got = store.search_notes_fts("tie", &options).await.unwrap();
    assert_eq!(
        got.iter()
            .map(|h| (h.workspace_id.as_str(), h.note_id.as_str()))
            .collect::<Vec<_>>(),
        [("b", "new"), ("a", "a")]
    );
}

#[tokio::test]
async fn note_fts_workspace_adjustments_are_soft() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let active = workspace(&store, "active", false).await;
    let preferred = workspace(&store, "preferred", false).await;
    let archived = workspace(&store, "archived", true).await;
    for ws in [&active, &preferred, &archived] {
        note(&store, ws, "equal", "equal", "needle").await;
    }
    let options = NoteFtsOptions {
        prefer_workspace_id: Some(&preferred),
        ..Default::default()
    };
    let got = store.search_notes_fts("needle", &options).await.unwrap();
    assert_eq!(
        got.iter()
            .map(|h| h.workspace_id.as_str())
            .collect::<Vec<_>>(),
        ["preferred", "active", "archived"]
    );
    assert!((got[1].rank - got[0].rank - 1.0).abs() < 1e-9);
    assert!((got[2].rank - got[1].rank - 1.0).abs() < 1e-9);
    // Rare-term IDF makes the repeated archived match decisively better,
    // even than a preferred-workspace match: these are not hard tiers.
    for i in 0..200 {
        note(
            &store,
            &active,
            &format!("filler-{i}"),
            "filler",
            "ordinary padding words",
        )
        .await;
    }
    note(
        &store,
        &preferred,
        "weak",
        "Other",
        &format!("rareword {}", "padding ".repeat(500)),
    )
    .await;
    note(
        &store,
        &archived,
        "strong",
        "rareword",
        &"rareword ".repeat(10),
    )
    .await;
    let got = store.search_notes_fts("rareword", &options).await.unwrap();
    assert_eq!(got[0].note_id, "strong");
}

#[tokio::test]
async fn note_fts_transcript_tokenization_and_empty_expression() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = workspace(&store, "tokens", false).await;
    note(
        &store,
        &ws,
        "one",
        "Roadmap",
        "Deploying café staging environment",
    )
    .await;
    // These expressions have the exact shape produced by fts_match_expr.
    for query in ["\"STAGING\" \"envir\"*", "\"deploy\"*", "\"cafe\"*"] {
        assert_eq!(hits(&store, query).await.len(), 1, "{query}");
    }
    assert!(hits(&store, "\"staging\" \"absent\"*").await.is_empty());
    assert!(hits(&store, "  ").await.is_empty());
}

#[tokio::test]
async fn note_fts_adoption_and_rollback_preserve_index_identity() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = workspace(&store, "adopt", false).await;
    let stray = note(&store, &ws, "random-id", "Spec", "adoptedbody").await;
    store.adopt_stray_spec_note(&ws).await.unwrap().unwrap();
    let got = hits(&store, "adoptedbody").await;
    assert_eq!(got[0].note_id, "spec");
    assert!(store.get_note(&ws, &stray.id).await.is_err());
    assert_eq!(hits(&store, "spec").await.len(), 1);
    let mut tx = store.write_pool().begin().await.unwrap();
    sqlx::query("UPDATE note SET content='rolledback' WHERE workspace_id=?")
        .bind(&ws.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    assert!(hits(&store, "rolledback").await.is_empty());
    assert_eq!(hits(&store, "adoptedbody").await.len(), 1);
}

#[tokio::test]
async fn note_fts_survives_compaction_and_note_rowid_reassignment() {
    let tmp = TempDb::new();
    // Force the real legacy auto_vacuum=NONE activation path.
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(&tmp.path)
                .create_if_missing(true),
        )
        .await
        .unwrap();
    sqlx::query("CREATE TABLE filler (id INTEGER PRIMARY KEY)")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = workspace(&store, "compact", false).await;
    let doomed = note(&store, &ws, "doomed", "Old", "deletedword").await;
    let mut survivor = note(&store, &ws, "spec", "Survivor", "survivorword").await;
    store.delete_note(&ws, &doomed.id).await.unwrap();
    let search_id: i64 =
        sqlx::query_scalar("SELECT search_id FROM note_search_ctx WHERE note_id='spec'")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    // SQLite versions may preserve note rowids during VACUUM. Explicitly
    // renumber as well, making independence from implicit rowids mandatory.
    sqlx::query("UPDATE note SET rowid = rowid + 1000")
        .execute(store.write_pool())
        .await
        .unwrap();
    assert!(matches!(
        store.activate_incremental_vacuum().await.unwrap(),
        crate::AutoVacuumActivation::Activated { .. }
    ));
    let after: i64 =
        sqlx::query_scalar("SELECT search_id FROM note_search_ctx WHERE note_id='spec'")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    assert_eq!(after, search_id);
    assert_eq!(hits(&store, "survivorword").await[0].note_id, "spec");
    assert!(hits(&store, "deletedword").await.is_empty());
    survivor.content = "aftercompaction".into();
    store.update_note(&survivor).await.unwrap();
    assert!(hits(&store, "survivorword").await.is_empty());
    assert_eq!(hits(&store, "aftercompaction").await.len(), 1);
    note(&store, &ws, "later", "Later", "laterword").await;
    assert_eq!(hits(&store, "laterword").await.len(), 1);
    store.delete_note(&ws, &survivor.id).await.unwrap();
    assert!(hits(&store, "aftercompaction").await.is_empty());
}

#[tokio::test]
async fn note_fts_query_plan_hydrates_only_winners() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = workspace(&store, "plan", false).await;
    let allowed = [ws.clone()];
    for filtered in [false, true] {
        let options = NoteFtsOptions {
            workspace_id: filtered.then_some(&ws),
            allowed_workspace_ids: filtered.then_some(&allowed[..]),
            include_archived: !filtered,
            limit: Some(10),
            ..Default::default()
        };
        let sql = format!(
            "EXPLAIN QUERY PLAN {}",
            crate::note_search_repo::search_notes_fts_sql(&options)
        );
        let mut query = sqlx::query(&sql).bind("plan").bind("needle");
        if filtered {
            query = query.bind("plan").bind("[\"plan\"]");
        }
        let rows = query
            .bind(10_i64)
            .fetch_all(store.read_pool())
            .await
            .unwrap();
        let plan: Vec<String> = rows.iter().map(|row| row.get("detail")).collect();
        let joined = plan.join("\n");
        println!("filtered={filtered}\n{joined}");
        assert!(
            plan.iter()
                .any(|d| d.contains("note_fts VIRTUAL TABLE INDEX") && d.contains('M')),
            "{joined}"
        );
        assert!(
            plan.iter()
                .any(|d| d.starts_with("SEARCH c USING INTEGER PRIMARY KEY")),
            "{joined}"
        );
        assert_eq!(
            plan.iter().filter(|d| d.starts_with("SEARCH n ")).count(),
            1,
            "{joined}"
        );
        assert!(
            !plan
                .iter()
                .any(|d| d.starts_with("SCAN n ") || d == "SCAN n"),
            "{joined}"
        );
        let top_scan = plan.iter().position(|d| d == "SCAN top").unwrap();
        let note_lookup = plan
            .iter()
            .position(|d| d.starts_with("SEARCH n "))
            .unwrap();
        assert!(
            note_lookup > top_scan,
            "body table must be joined after top-N: {joined}"
        );
    }
}

/// Disposable, representative backfill/query benchmark. No timing threshold:
/// the production-query plan guard above is the deterministic cost contract.
/// Run with `cargo test -p intent-store bench_note_fts -- --ignored --nocapture`.
#[tokio::test]
#[ignore = "records representative backfill and query timings"]
async fn bench_note_fts() {
    use std::time::Instant;
    for (count, padding_words) in [(2_000, 128), (10_000, 2_048)] {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.unwrap();
        let ws = workspace(&store, "bench", false).await;
        drop_index(&store).await;
        let body = format!("common {}", "padding ".repeat(padding_words));
        let mut tx = store.write_pool().begin().await.unwrap();
        for i in 0..count {
            sqlx::query(
                "INSERT INTO note(id,workspace_id,title,content,tags,created_at,updated_at)
                VALUES (?,?,?,?,'[\"fixture\"]','2026-01-01','2026-01-01')",
            )
            .bind(format!("note-{i:05}"))
            .bind(&ws.0)
            .bind(format!("Note {i}"))
            .bind(if i % 100 == 0 {
                format!("{body} raremarker")
            } else {
                body.clone()
            })
            .execute(&mut *tx)
            .await
            .unwrap();
        }
        tx.commit().await.unwrap();
        let started = Instant::now();
        let mut tx = store.write_pool().begin().await.unwrap();
        sqlx::raw_sql(include_str!("../../migrations/0141_note_fts.sql"))
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        println!(
            "notes={count} body_bytes={} backfill_ms={:.2}",
            body.len(),
            started.elapsed().as_secs_f64() * 1000.0
        );
        let allowed = [ws];
        let options = NoteFtsOptions {
            allowed_workspace_ids: Some(&allowed),
            include_archived: false,
            limit: Some(10),
            ..Default::default()
        };
        for query in ["common", "raremarker"] {
            let mut timings = Vec::new();
            for _ in 0..20 {
                let started = Instant::now();
                let hits = store.search_notes_fts(query, &options).await.unwrap();
                timings.push(started.elapsed().as_secs_f64() * 1000.0);
                assert_eq!(hits.len(), 10);
                assert!(hits.iter().all(|hit| hit.content.contains(query)));
            }
            timings.sort_by(f64::total_cmp);
            println!(
                "notes={count} query={query} limit=10 median_ms={:.2} p95_ms={:.2}",
                timings[10], timings[18]
            );
        }
    }
}
