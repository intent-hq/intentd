use super::*;
use sqlx::Connection;

const SCHEMA:&str="
CREATE TABLE note_stage_root(root_key TEXT PRIMARY KEY,workspace_id TEXT,note_id TEXT,content_generation TEXT,source_length INTEGER);
CREATE TABLE note_stage(operation_key TEXT PRIMARY KEY,root_key TEXT);
CREATE TABLE note_stage_view(operation_key TEXT,generation INTEGER,length INTEGER,PRIMARY KEY(operation_key,generation));
CREATE TABLE note_stage_view_piece(operation_key TEXT,generation INTEGER,start INTEGER,end INTEGER,origin_kind TEXT,origin_id TEXT,origin_start INTEGER,PRIMARY KEY(operation_key,generation,start));
CREATE TABLE note_stage_base_piece(root_key TEXT,start INTEGER,end INTEGER,text TEXT,PRIMARY KEY(root_key,start));
CREATE TABLE note_page_piece(workspace_id TEXT,note_id TEXT,content_generation TEXT,start INTEGER,end INTEGER,text TEXT,PRIMARY KEY(workspace_id,note_id,content_generation,start));
CREATE TABLE note_stage_text(operation_key TEXT,text_id TEXT,length INTEGER,PRIMARY KEY(operation_key,text_id));
CREATE TABLE note_stage_text_piece(operation_key TEXT,text_id TEXT,start INTEGER,end INTEGER,text TEXT,PRIMARY KEY(operation_key,text_id,start));
INSERT INTO note_stage_root VALUES('root','ws','note','old',4);
INSERT INTO note_stage VALUES('op','root');
INSERT INTO note_stage_view VALUES('op',0,4),('op',1,9);
INSERT INTO note_page_piece VALUES('ws','note','old',0,3,'A😀'),('ws','note','old',3,4,'B');
INSERT INTO note_stage_text VALUES('op','insert',5);
INSERT INTO note_stage_text_piece VALUES('op','insert',0,2,'界\"'),('op','insert',2,5,'\n🙂');
INSERT INTO note_stage_view_piece VALUES('op',0,0,4,'root','root',0),('op',1,0,3,'root','root',0),('op',1,3,8,'text','insert',0),('op',1,8,9,'root','root',3);
";
async fn fixture() -> SqliteConnection {
    let mut conn = SqliteConnection::connect("sqlite::memory:").await.unwrap();
    sqlx::raw_sql(SCHEMA).execute(&mut conn).await.unwrap();
    conn
}

#[tokio::test]
async fn stage_view_read_reconstructs_scalars_across_origins_and_piece_seams() {
    let mut conn = fixture().await;
    let expected = "A😀界\"\n🙂B";
    for budget in [4, 5, 6, 7, 8, 9, 10, 16384] {
        let (mut offset, mut text) = (0, String::new());
        while offset < 9 {
            let (end, part) = read_piece(&mut conn, "op", 1, 9, offset, budget)
                .await
                .unwrap();
            assert!(!part.is_empty());
            assert!(part.len() <= budget);
            assert_eq!(
                end - offset,
                u64::try_from(part.encode_utf16().count()).unwrap()
            );
            assert!(end > offset);
            offset = end;
            text.push_str(&part);
        }
        assert_eq!(text, expected);
    }
    assert_eq!(
        read_piece(&mut conn, "op", 1, 9, 9, 4).await.unwrap(),
        (9, String::new())
    );
    assert_eq!(
        read_piece(&mut conn, "op", 0, 4, 0, 16384).await.unwrap(),
        (4, "A😀B".into()),
        "older retained generation remains immutable and readable"
    );
}

#[tokio::test]
async fn stage_view_read_checks_binding_budget_and_scalar_offsets() {
    let mut conn = fixture().await;
    for (op, generation, length, offset, budget) in [
        ("foreign", 1, 9, 0, 4),
        ("op", 2, 9, 0, 4),
        ("op", 1, 8, 0, 4),
        ("op", 1, 9, 10, 4),
        ("op", 1, 9, 2, 4),
        ("op", 1, 9, 7, 4),
        ("op", 1, 9, 0, 3),
        ("op", 1, 9, 0, 16385),
        ("op", u64::MAX, 9, 0, 4),
        ("op", 1, u64::MAX, u64::MAX, 4),
        ("foreign", 1, 0, 0, 4),
    ] {
        assert!(
            read_piece(&mut conn, op, generation, length, offset, budget)
                .await
                .is_err(),
            "{op}/{generation}/{length}/{offset}/{budget}"
        );
    }
    assert_eq!(
        read_piece(&mut conn, "op", 1, 9, 1, 4).await.unwrap(),
        (3, "😀".into())
    );
    assert_eq!(
        read_piece(&mut conn, "op", 1, 9, 6, 4).await.unwrap(),
        (8, "🙂".into())
    );
    sqlx::query("INSERT INTO note_stage_view VALUES('op',2,0)")
        .execute(&mut conn)
        .await
        .unwrap();
    assert_eq!(
        read_piece(&mut conn, "op", 2, 0, 0, 4).await.unwrap(),
        (0, String::new())
    );
}

#[tokio::test]
async fn stage_view_read_keeps_pinned_root_and_rejects_foreign_owners() {
    let mut conn = fixture().await;
    sqlx::raw_sql("INSERT INTO note_stage_base_piece VALUES('root',0,3,'A😀'),('root',3,4,'B'); DELETE FROM note_page_piece; INSERT INTO note_page_piece VALUES('ws','note','new',0,4,'XXXX');").execute(&mut conn).await.unwrap();
    assert_eq!(
        read_piece(&mut conn, "op", 0, 4, 0, 16384).await.unwrap().1,
        "A😀B"
    );
    for mutation in [
        "UPDATE note_stage_view_piece SET origin_id='foreign-root' WHERE generation=0",
        "UPDATE note_stage_text SET operation_key='foreign'",
        "DELETE FROM note_stage_text WHERE text_id='insert'",
    ] {
        let mut tx = conn.begin().await.unwrap();
        sqlx::query(mutation).execute(&mut *tx).await.unwrap();
        let generation = u64::from(!mutation.contains("foreign-root"));
        let length = if generation == 0 { 4 } else { 9 };
        assert!(read_piece(&mut tx, "op", generation, length, 0, 16384)
            .await
            .is_err());
        tx.rollback().await.unwrap();
    }
}

#[tokio::test]
async fn stage_view_read_rejects_corrupt_extents_and_oversized_source_rows() {
    let mut conn = fixture().await;
    for mutation in [
        "DELETE FROM note_stage_view_piece WHERE generation=1 AND start=3",
        "UPDATE note_stage_view_piece SET end=10 WHERE generation=1 AND start=8",
        "UPDATE note_stage_view_piece SET origin_start=2 WHERE generation=1 AND start=0",
        "UPDATE note_stage_view_piece SET end=2 WHERE generation=1 AND start=0",
        "UPDATE note_stage_view_piece SET origin_kind='unknown' WHERE generation=1 AND start=0",
        "UPDATE note_stage_text_piece SET end=100 WHERE start=0",
        "UPDATE note_stage_text_piece SET start=-1 WHERE start=0",
        "UPDATE note_stage_text_piece SET text=printf('%05000d',0) WHERE start=0",
        "UPDATE note_stage_view_piece SET origin_start=9007199254740991 WHERE generation=1 AND start=3",
        "DELETE FROM note_stage_text_piece WHERE start=2",
    ] {
        let mut tx=conn.begin().await.unwrap();sqlx::query(mutation).execute(&mut *tx).await.unwrap();
        assert!(read_piece(&mut tx,"op",1,9,0,16384).await.is_err(),"{mutation}");
        tx.rollback().await.unwrap();
    }
}

#[tokio::test]
async fn stage_view_read_caps_large_output_and_seeks_late_rows() {
    let mut conn = fixture().await;
    sqlx::raw_sql("INSERT INTO note_stage_view VALUES('op',3,100001); INSERT INTO note_stage_text VALUES('op','large',100001); INSERT INTO note_stage_view_piece VALUES('op',3,0,100001,'text','large',0);").execute(&mut conn).await.unwrap();
    for start in (0..100_001_i64).step_by(4096) {
        let end = (start + 4096).min(100_001);
        sqlx::query("INSERT INTO note_stage_text_piece VALUES('op','large',?,?,?)")
            .bind(start)
            .bind(end)
            .bind("a".repeat(usize::try_from(end - start).unwrap()))
            .execute(&mut conn)
            .await
            .unwrap();
    }
    let (end, text) = read_piece(&mut conn, "op", 3, 100_001, 0, 16384)
        .await
        .unwrap();
    assert_eq!(end, 16384);
    assert_eq!(text.len(), 16384);
    assert_eq!(
        read_piece(&mut conn, "op", 3, 100_001, 100_000, 4)
            .await
            .unwrap(),
        (100_001, "a".into())
    );
    for (sql, bindings) in [
        (VIEW_PIECE, vec!["op", "3", "100000"]),
        (TEXT_PIECE, vec!["op", "large", "100000"]),
    ] {
        let plan = format!("EXPLAIN QUERY PLAN {sql}");
        let mut query = sqlx::query(&plan);
        for value in bindings {
            query = query.bind(value);
        }
        let rows = query.fetch_all(&mut conn).await.unwrap();
        let plan = rows
            .iter()
            .map(|r| r.get::<String, _>("detail"))
            .collect::<Vec<_>>()
            .join(" ");
        assert!(plan.contains("SEARCH"), "{plan}");
        assert!(!plan.contains("SCAN") && !plan.contains("TEMP"), "{plan}");
    }
}
