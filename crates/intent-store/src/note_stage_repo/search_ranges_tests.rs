use super::*;
use serde_json::json;
use sqlx::{Connection, Row};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

const SCHEMA:&str="
CREATE TABLE note_stage_root(root_key TEXT PRIMARY KEY,workspace_id TEXT,note_id TEXT,content_generation TEXT,source_length INTEGER);
CREATE TABLE note_stage(operation_key TEXT PRIMARY KEY,root_key TEXT);
CREATE TABLE note_stage_view(operation_key TEXT,generation INTEGER,length INTEGER,PRIMARY KEY(operation_key,generation));
CREATE TABLE note_stage_view_piece(operation_key TEXT,generation INTEGER,start INTEGER,end INTEGER,origin_kind TEXT,origin_id TEXT,origin_start INTEGER,PRIMARY KEY(operation_key,generation,start));
CREATE TABLE note_stage_base_piece(root_key TEXT,start INTEGER,end INTEGER,text TEXT,PRIMARY KEY(root_key,start));
CREATE TABLE note_page_piece(workspace_id TEXT,note_id TEXT,content_generation TEXT,start INTEGER,end INTEGER,text TEXT,PRIMARY KEY(workspace_id,note_id,content_generation,start));
CREATE TABLE note_stage_text(operation_key TEXT,text_id TEXT,length INTEGER,PRIMARY KEY(operation_key,text_id));
CREATE TABLE note_stage_text_piece(operation_key TEXT,text_id TEXT,start INTEGER,end INTEGER,text TEXT,PRIMARY KEY(operation_key,text_id,start));
CREATE TABLE note_stage_record(operation_key TEXT,stream TEXT,chunk_sequence INTEGER,ordinal INTEGER,value TEXT,PRIMARY KEY(operation_key,stream,chunk_sequence,ordinal));
INSERT INTO note_stage_root VALUES('root','w','n','old',8);
INSERT INTO note_stage VALUES('op','root');
INSERT INTO note_stage_view VALUES('op',0,8);
INSERT INTO note_page_piece VALUES('w','n','old',0,3,'A😀'),('w','n','old',3,8,'BCDEF');
INSERT INTO note_stage_view_piece VALUES('op',0,0,8,'root','root',0);
";
// Owner registers these proposed tables with cleanup compatibility in0154.
const TABLES: &str = r#"-- Proposed tables only; owner also widens/rebuilds the 0153 queue step check
-- preserving existing (due_ms,mode,step). Append cleanup phases15/16, terminal17.
-- mode0 skips receipt phases8..14 to15; mode1 visits all phases. No renumbering.
-- Both tables must be excluded from transfer and independently drained in64-row
-- batches before note_stage deletion. No view FK: view cleanup precedes these.
CREATE TABLE note_stage_search_input (
 operation_key TEXT NOT NULL REFERENCES note_stage(operation_key) ON DELETE CASCADE,
 generation INTEGER NOT NULL CHECK(generation BETWEEN 0 AND 9007199254740991),
 start INTEGER NOT NULL CHECK(start BETWEEN 0 AND 9007199254740991),
 end INTEGER NOT NULL CHECK(end>start AND end<=9007199254740991),
 ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 0 AND 9007199254740991),
 PRIMARY KEY(operation_key,generation,start,end,ordinal)
);
CREATE TABLE note_stage_search_range (
 operation_key TEXT NOT NULL REFERENCES note_stage(operation_key) ON DELETE CASCADE,
 generation INTEGER NOT NULL CHECK(generation BETWEEN 0 AND 9007199254740991),
 start INTEGER NOT NULL CHECK(start BETWEEN 0 AND 9007199254740991),
 end INTEGER NOT NULL CHECK(end>start AND end<=9007199254740991),
 PRIMARY KEY(operation_key,generation,start)
);
"#;

fn header(selection: &str) -> NoteStageHeader {
    serde_json::from_value(json!({"baseRevision":"base","editorSessionId":"editor","localEditSequence":0,"liveGeneration":0,"selectionGeneration":0,"action":"read","output":"search","selection":selection,"query":{"text":"B","caseSensitive":false,"mode":"source"}})).unwrap()
}
fn view() -> PreparedView {
    PreparedView {
        view_id: "view".into(),
        generation: 0,
        length: 8,
    }
}
async fn fixture() -> SqliteConnection {
    let mut conn = SqliteConnection::connect("sqlite::memory:").await.unwrap();
    sqlx::raw_sql("PRAGMA foreign_keys=ON;")
        .execute(&mut conn)
        .await
        .unwrap();
    sqlx::raw_sql(SCHEMA).execute(&mut conn).await.unwrap();
    sqlx::raw_sql(TABLES).execute(&mut conn).await.unwrap();
    conn
}
async fn range(conn: &mut SqliteConnection, ordinal: u64, start: u64, end: u64) {
    let value = json!({"kind":"range","ordinal":ordinal,"start":start,"end":end,"anchorAffinity":"before","headAffinity":"after","direction":"backward"});
    sqlx::query("INSERT INTO note_stage_record VALUES('op','selection',?,?,?)")
        .bind(integer(ordinal / 128).unwrap())
        .bind(integer(ordinal % 128).unwrap())
        .bind(value.to_string())
        .execute(conn)
        .await
        .unwrap();
}
async fn intervals(conn: &mut SqliteConnection, length: u64) -> Vec<SearchInterval> {
    let mut rows = Vec::new();
    let mut after = None;
    while let Some(row) = next_interval(conn, "op", 0, length, after).await.unwrap() {
        after = Some(row.start);
        rows.push(row);
    }
    rows
}

#[tokio::test]
async fn unsorted_duplicates_overlap_touch_and_points_preserve_uploads() {
    let mut conn = fixture().await;
    for (i, (start, end)) in [(6, 8), (1, 3), (3, 5), (1, 3), (0, 1), (5, 5), (5, 6)]
        .into_iter()
        .enumerate()
    {
        range(&mut conn, u64::try_from(i).unwrap(), start, end).await;
    }
    let before: Vec<String> =
        sqlx::query_scalar("SELECT value FROM note_stage_record ORDER BY chunk_sequence,ordinal")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    let mut tx = conn.begin().await.unwrap();
    let summary = normalize(&mut tx, "op", &header("ranges"), &view())
        .await
        .unwrap();
    assert_eq!((summary.intervals, summary.selected_units), (1, 8));
    tx.commit().await.unwrap();
    assert_eq!(
        intervals(&mut conn, 8).await,
        vec![SearchInterval { start: 0, end: 8 }]
    );
    let after: Vec<String> =
        sqlx::query_scalar("SELECT value FROM note_stage_record ORDER BY chunk_sequence,ordinal")
            .fetch_all(&mut conn)
            .await
            .unwrap();
    assert_eq!(after, before);
    let input: i64 = sqlx::query_scalar("SELECT count(*) FROM note_stage_search_input")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        input, 6,
        "validated point is discarded, duplicate remains in sort input"
    );
    assert!(normalize(&mut conn, "op", &header("ranges"), &view())
        .await
        .is_err());
}

#[tokio::test]
async fn only_true_gaps_split_source_search_spans() {
    let mut conn = fixture().await;
    for (i, (start, end)) in [(6, 8), (3, 5), (0, 1)].into_iter().enumerate() {
        range(&mut conn, u64::try_from(i).unwrap(), start, end).await;
    }
    let summary = normalize(&mut conn, "op", &header("ranges"), &view())
        .await
        .unwrap();
    assert_eq!(summary.selected_units, 5);
    assert_eq!(
        intervals(&mut conn, 8).await,
        vec![
            SearchInterval { start: 0, end: 1 },
            SearchInterval { start: 3, end: 5 },
            SearchInterval { start: 6, end: 8 }
        ]
    );
    // A consumer must reset per returned span, never join these into "ABC...".
    let (_, left) = view_read::read_piece(&mut conn, "op", 0, 8, 0, 4)
        .await
        .unwrap();
    let (_, right) = view_read::read_piece(&mut conn, "op", 0, 8, 3, 4)
        .await
        .unwrap();
    assert!(left.starts_with('A') && right.starts_with('B'));
    assert!(next_interval(&mut conn, "op", 0, 8, Some(9)).await.is_err());
}

#[tokio::test]
async fn all_empty_selection_and_empty_source_are_distinct() {
    let mut conn = fixture().await;
    assert_eq!(
        normalize(&mut conn, "op", &header("ranges"), &view())
            .await
            .unwrap()
            .intervals,
        0
    );
    assert!(intervals(&mut conn, 8).await.is_empty());
    assert_eq!(
        normalize(&mut conn, "op", &header("all"), &view())
            .await
            .unwrap()
            .selected_units,
        8
    );
    assert_eq!(
        intervals(&mut conn, 8).await,
        vec![SearchInterval { start: 0, end: 8 }]
    );
    let mut empty = fixture().await;
    sqlx::query("UPDATE note_stage_view SET length=0")
        .execute(&mut empty)
        .await
        .unwrap();
    sqlx::query("DELETE FROM note_stage_view_piece")
        .execute(&mut empty)
        .await
        .unwrap();
    sqlx::query("DELETE FROM note_page_piece")
        .execute(&mut empty)
        .await
        .unwrap();
    sqlx::query("UPDATE note_stage_root SET source_length=0")
        .execute(&mut empty)
        .await
        .unwrap();
    let mut zero = view();
    zero.length = 0;
    assert_eq!(
        normalize(&mut empty, "op", &header("all"), &zero)
            .await
            .unwrap()
            .intervals,
        0
    );
    assert!(intervals(&mut empty, 0).await.is_empty());
    let mut wrong = fixture().await;
    range(&mut wrong, 0, 0, 0).await;
    assert!(normalize(&mut wrong, "op", &header("all"), &view())
        .await
        .is_err());
}

#[tokio::test]
async fn boundaries_scope_generation_and_header_fail_closed() {
    for (start, end) in [(2, 2), (0, 2), (2, 3), (3, 9), (5, 4)] {
        let mut conn = fixture().await;
        range(&mut conn, 0, start, end).await;
        assert!(
            normalize(&mut conn, "op", &header("ranges"), &view())
                .await
                .is_err(),
            "{start}..{end}"
        );
    }
    let mut conn = fixture().await;
    assert!(normalize(&mut conn, "foreign", &header("ranges"), &view())
        .await
        .is_err());
    let mut wrong = view();
    wrong.length = 7;
    assert!(normalize(&mut conn, "op", &header("ranges"), &wrong)
        .await
        .is_err());
    let mut case_sensitive = header("ranges");
    case_sensitive.query.as_mut().unwrap().case_sensitive = true;
    assert!(normalize(&mut conn, "op", &case_sensitive, &view())
        .await
        .is_err());
    let mut rendered = header("ranges");
    rendered.query.as_mut().unwrap().mode = NoteStageSearchMode::RenderedText;
    assert!(normalize(&mut conn, "op", &rendered, &view())
        .await
        .is_err());
    sqlx::query("INSERT INTO note_stage_view VALUES('op',1,8)")
        .execute(&mut conn)
        .await
        .unwrap();
    assert!(
        normalize(&mut conn, "op", &header("ranges"), &view())
            .await
            .is_err(),
        "old same-length generation rejected"
    );
}

#[tokio::test]
async fn late_failure_rolls_back_both_external_indexes_and_retries_exactly() {
    let mut conn = fixture().await;
    range(&mut conn, 0, 0, 1).await;
    range(&mut conn, 1, 3, 4).await;
    sqlx::raw_sql("CREATE TRIGGER fail_second BEFORE INSERT ON note_stage_search_range WHEN new.start=3 BEGIN SELECT RAISE(ABORT,'late union write'); END;").execute(&mut conn).await.unwrap();
    let mut tx = conn.begin().await.unwrap();
    assert!(normalize(&mut tx, "op", &header("ranges"), &view())
        .await
        .is_err());
    tx.rollback().await.unwrap();
    for table in ["note_stage_search_input", "note_stage_search_range"] {
        let n: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(n, 0);
    }
    sqlx::query("DROP TRIGGER fail_second")
        .execute(&mut conn)
        .await
        .unwrap();
    let mut tx = conn.begin().await.unwrap();
    assert_eq!(
        normalize(&mut tx, "op", &header("ranges"), &view())
            .await
            .unwrap()
            .intervals,
        2
    );
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn malformed_oversized_or_wrong_ordinal_uploads_are_not_normalized() {
    for value in ["{}".to_owned()," ".repeat(65537),json!({"kind":"range","ordinal":1,"start":0,"end":1,"anchorAffinity":"before","headAffinity":"after","direction":"forward"}).to_string()] {
        let mut conn=fixture().await;
        sqlx::query("INSERT INTO note_stage_record VALUES('op','selection',0,0,?)").bind(value).execute(&mut conn).await.unwrap();
        assert!(normalize(&mut conn,"op",&header("ranges"),&view()).await.is_err());
    }
}

#[tokio::test]
async fn thousand_unsorted_ranges_cross_chunks_and_late_seek_skips_prefix() {
    let mut conn = fixture().await;
    sqlx::query("DELETE FROM note_page_piece")
        .execute(&mut conn)
        .await
        .unwrap();
    sqlx::query("UPDATE note_stage_root SET source_length=4000")
        .execute(&mut conn)
        .await
        .unwrap();
    sqlx::query("UPDATE note_stage_view SET length=4000")
        .execute(&mut conn)
        .await
        .unwrap();
    sqlx::query("UPDATE note_stage_view_piece SET end=4000")
        .execute(&mut conn)
        .await
        .unwrap();
    sqlx::query("INSERT INTO note_page_piece VALUES('w','n','old',0,4000,?)")
        .bind("a".repeat(4000))
        .execute(&mut conn)
        .await
        .unwrap();
    for i in 0..1000 {
        let start = (999 - i) * 4;
        range(&mut conn, i, start, start + 2).await;
    }
    let mut retained = view();
    retained.length = 4000;
    let summary = normalize(&mut conn, "op", &header("ranges"), &retained)
        .await
        .unwrap();
    assert_eq!((summary.intervals, summary.selected_units), (1000, 2000));
    let mut costs = Vec::new();
    for after in [0, 1996, 3992] {
        let steps = Arc::new(AtomicU64::new(0));
        let counter = steps.clone();
        conn.lock_handle()
            .await
            .unwrap()
            .set_progress_handler(1, move || {
                counter.fetch_add(1, Ordering::Relaxed);
                true
            });
        assert_eq!(
            next_interval(&mut conn, "op", 0, 4000, Some(after))
                .await
                .unwrap(),
            Some(SearchInterval {
                start: after + 4,
                end: after + 6
            })
        );
        conn.lock_handle().await.unwrap().remove_progress_handler();
        costs.push(steps.load(Ordering::Relaxed));
    }
    eprintln!("normalized interval first/middle/late seek VM: {costs:?}");
    assert!(costs[2] <= costs[0] + 500);
    let plan = sqlx::query(&format!("EXPLAIN QUERY PLAN {SORTED}"))
        .bind("op")
        .bind(0)
        .bind(0)
        .bind(0)
        .bind(0)
        .fetch_all(&mut conn)
        .await
        .unwrap();
    assert!(plan
        .iter()
        .any(|r| r.get::<String, _>("detail").contains("SEARCH")));
    assert!(plan
        .iter()
        .all(|r| !r.get::<String, _>("detail").contains("TEMP B-TREE")));
}

#[tokio::test]
async fn largest_safe_endpoints_seek_only_the_retained_tail_piece() {
    let mut conn = fixture().await;
    sqlx::query("DELETE FROM note_page_piece")
        .execute(&mut conn)
        .await
        .unwrap();
    sqlx::query("UPDATE note_stage_root SET source_length=?")
        .bind(integer(SAFE).unwrap())
        .execute(&mut conn)
        .await
        .unwrap();
    sqlx::query("UPDATE note_stage_view SET length=?")
        .bind(integer(SAFE).unwrap())
        .execute(&mut conn)
        .await
        .unwrap();
    sqlx::query("UPDATE note_stage_view_piece SET end=?")
        .bind(integer(SAFE).unwrap())
        .execute(&mut conn)
        .await
        .unwrap();
    // Sparse fixture is only an arithmetic/seek control, not proof of a whole
    // SAFE-length source. The helper must touch only this two-unit tail piece.
    sqlx::query("INSERT INTO note_stage_base_piece VALUES('root',?,?,'😀')")
        .bind(integer(SAFE - 2).unwrap())
        .bind(integer(SAFE).unwrap())
        .execute(&mut conn)
        .await
        .unwrap();
    range(&mut conn, 0, SAFE - 2, SAFE).await;
    let mut retained = view();
    retained.length = SAFE;
    let summary = normalize(&mut conn, "op", &header("ranges"), &retained)
        .await
        .unwrap();
    assert_eq!(summary.selected_units, 2);
    assert_eq!(
        next_interval(&mut conn, "op", 0, SAFE, None).await.unwrap(),
        Some(SearchInterval {
            start: SAFE - 2,
            end: SAFE
        })
    );
    assert!(next_interval(&mut conn, "op", 0, SAFE, Some(SAFE + 1))
        .await
        .is_err());
}
