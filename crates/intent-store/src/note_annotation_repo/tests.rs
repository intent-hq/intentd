use super::*;

#[test]
fn interval_admission_preserves_disjoint_ranges_and_empty_sets() {
    assert!(validate_ranges(&[]).is_ok());
    assert!(validate_ranges(&[
        SourceRange { start: 0, end: 2 },
        SourceRange { start: 3, end: 4 }
    ])
    .is_ok());
    for ranges in [
        vec![SourceRange { start: -1, end: 2 }],
        vec![SourceRange { start: 2, end: 2 }],
        vec![
            SourceRange { start: 0, end: 2 },
            SourceRange { start: 2, end: 4 },
        ],
        vec![
            SourceRange { start: 3, end: 4 },
            SourceRange { start: 0, end: 2 },
        ],
        vec![SourceRange {
            start: 0,
            end: MAX_OFFSET + 1,
        }],
        vec![SourceRange { start: 0, end: 1 }; MAX_RANGES + 1],
    ] {
        assert!(validate_ranges(&ranges).is_err());
    }
    assert!(validate_limit(0).is_err());
    assert!(validate_limit(MAX_ITEMS + 1).is_err());
}

#[tokio::test]
async fn annotation_range_query_plans_seek_indexes_on_large_collections() {
    use sqlx::{Connection, Execute};
    let mut conn = sqlx::SqliteConnection::connect("sqlite::memory:")
        .await
        .unwrap();
    sqlx::raw_sql("CREATE TABLE note_comment_anchor(id INTEGER PRIMARY KEY,head_id INTEGER,comment_id TEXT,start INTEGER,end INTEGER);
        CREATE VIRTUAL TABLE note_comment_anchor_extent USING rtree(id,scope_min,scope_max,start,end);
        CREATE TABLE note_comment_projection(comment_id TEXT PRIMARY KEY,thread_id TEXT);
        CREATE TABLE note_attribution_line(head_id INTEGER,line INTEGER,start INTEGER,end INTEGER,timestamp INTEGER,has_author INTEGER,PRIMARY KEY(head_id,line));
        CREATE INDEX note_attribution_extent ON note_attribution_line(head_id,end,start,line);
        CREATE INDEX note_attribution_start ON note_attribution_line(head_id,start,line);
        WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<10000)
        INSERT INTO note_comment_anchor SELECT x,1,CAST(x AS TEXT),x*10,x*10+7 FROM n;
        INSERT INTO note_comment_anchor_extent SELECT id,1,1,start,end FROM note_comment_anchor;
        INSERT INTO note_comment_projection SELECT comment_id,comment_id FROM note_comment_anchor;
        INSERT INTO note_attribution_line SELECT 1,id,start,end,0,0 FROM note_comment_anchor;")
        .execute(&mut conn).await.unwrap();
    let ranges = [SourceRange {
        start: 50003,
        end: 50006,
    }];
    let mut comments = super::comments::matching_threads(1, &ranges, CommentFilter::Anchored);
    comments.push("SELECT thread_id FROM matched");
    let mut query = comments.build();
    let explain = format!("EXPLAIN QUERY PLAN {}", query.sql());
    let args = query.take_arguments().unwrap().unwrap();
    let plan = sqlx::query_with(&explain, args)
        .fetch_all(&mut conn)
        .await
        .unwrap();
    let details = plan
        .iter()
        .map(|row| row.get::<String, _>("detail"))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(details.contains("VIRTUAL TABLE INDEX"), "{details}");
    assert!(
        details.contains("SEARCH a USING INTEGER PRIMARY KEY"),
        "{details}"
    );
    assert!(!details.contains("SCAN a"), "{details}");
    let mut comments = super::comments::matching_threads(1, &ranges, CommentFilter::Anchored);
    comments.push("SELECT thread_id FROM matched");
    let rows = comments.build().fetch_all(&mut conn).await.unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<String, _>("thread_id"), "5000");
    let mut attribution = super::attribution::range_query(1, &ranges, None, 2);
    let mut query = attribution.build();
    let explain = format!("EXPLAIN QUERY PLAN {}", query.sql());
    let args = query.take_arguments().unwrap().unwrap();
    let plan = sqlx::query_with(&explain, args)
        .fetch_all(&mut conn)
        .await
        .unwrap();
    let details = plan
        .iter()
        .map(|row| row.get::<String, _>("detail"))
        .collect::<Vec<_>>()
        .join("\n");
    assert!(details.contains("note_attribution_extent"), "{details}");
    assert!(details.contains("note_attribution_start"), "{details}");
    assert!(details.contains("line>? AND line<?"), "{details}");
    let rows = super::attribution::range_query(1, &ranges, None, 2)
        .build()
        .fetch_all(&mut conn)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<i64, _>("line"), 5000);
    // Even a viewport admitting every line returns only limit+one rows.
    let rows = super::attribution::range_query(
        1,
        &[SourceRange {
            start: 0,
            end: 100_010,
        }],
        None,
        2,
    )
    .build()
    .fetch_all(&mut conn)
    .await
    .unwrap();
    assert_eq!(rows.len(), 2);
}

#[tokio::test]
async fn annotation_full_summary_queries_seek_all_and_orphan_pages_in_dense_100k_notes() {
    use sqlx::{Connection, Execute};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };
    let mut conn = sqlx::SqliteConnection::connect("sqlite::memory:")
        .await
        .unwrap();
    sqlx::raw_sql("CREATE TABLE note_comment_thread(head_id INTEGER,thread_id TEXT,total_comments INTEGER,anchor_count INTEGER,PRIMARY KEY(head_id,thread_id));
        CREATE INDEX note_comment_orphan_order ON note_comment_thread(head_id,thread_id) WHERE anchor_count=0;
        CREATE TABLE note_comment_root(head_id INTEGER,thread_id TEXT,root_comment_id TEXT,root_present INTEGER,PRIMARY KEY(head_id,thread_id));
        CREATE TABLE note_comment_projection(comment_id TEXT PRIMARY KEY,head_id INTEGER,thread_id TEXT,parent_id TEXT,status TEXT,created_at TEXT,preview TEXT,truncated INTEGER);
        CREATE INDEX note_comment_reply_order ON note_comment_projection(head_id,thread_id,created_at,comment_id);
        CREATE INDEX note_comment_root_order ON note_comment_projection(head_id,thread_id,created_at,comment_id) WHERE parent_id IS NULL;
        WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<100000)
        INSERT INTO note_comment_thread SELECT 1,printf('%06d',x),1,x%2 FROM n;
        INSERT INTO note_comment_root SELECT head_id,thread_id,thread_id,1 FROM note_comment_thread;
        INSERT INTO note_comment_projection SELECT thread_id,head_id,thread_id,NULL,'open','date','bounded',0 FROM note_comment_thread;
        CREATE TABLE note_comment_anchor(id INTEGER PRIMARY KEY,head_id INTEGER,thread_id TEXT,start INTEGER,end INTEGER);
        CREATE INDEX note_comment_anchor_start_order ON note_comment_anchor(head_id,start,thread_id,id);
        CREATE INDEX note_comment_anchor_thread_order ON note_comment_anchor(head_id,thread_id,start,end,id);
        CREATE TABLE note_comment_anchor_cover(head_id INTEGER,level INTEGER,bucket INTEGER,thread_id TEXT,anchor_id INTEGER,start INTEGER,end INTEGER,PRIMARY KEY(head_id,level,bucket,thread_id,anchor_id)) WITHOUT ROWID;
        INSERT INTO note_comment_anchor SELECT CAST(thread_id AS INTEGER),head_id,thread_id,0,10 FROM note_comment_thread;
        INSERT INTO note_comment_anchor_cover SELECT head_id,3,0,thread_id,id,start,end FROM note_comment_anchor;
        INSERT INTO note_comment_anchor_cover SELECT head_id,1,4,thread_id,id,start,end FROM note_comment_anchor;")
        .execute(&mut conn).await.unwrap();
    for filter in [
        CommentFilter::All,
        CommentFilter::Orphaned,
        CommentFilter::Anchored,
    ] {
        let make_query = || {
            if filter == CommentFilter::Anchored {
                super::page::matches::match_summary_query(
                    1,
                    &[SourceRange { start: 1, end: 2 }],
                    0,
                    Some((1, "090000")),
                    3,
                )
            } else {
                super::comments::thread_summary_query(1, &[], filter, Some((0, "090000")), 3)
            }
        };
        let mut builder = make_query();
        let mut query = builder.build();
        let explain = format!("EXPLAIN QUERY PLAN {}", query.sql());
        let arguments = query.take_arguments().unwrap().unwrap();
        let plan = sqlx::query_with(&explain, arguments)
            .fetch_all(&mut conn)
            .await
            .unwrap();
        let plan = plan
            .iter()
            .map(|row| row.get::<String, _>("detail"))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(plan.contains("MATERIALIZE selected"), "{plan}");
        assert!(
            plan.contains("thread_id>?") || plan.contains("(position,thread_id)>(?,?)"),
            "{plan}"
        );
        if filter == CommentFilter::Orphaned {
            assert!(plan.contains("note_comment_orphan_order"), "{plan}");
        }
        let work = Arc::new(AtomicUsize::new(0));
        let counter = Arc::clone(&work);
        conn.lock_handle()
            .await
            .unwrap()
            .set_progress_handler(1, move || {
                counter.fetch_add(1, Ordering::Relaxed);
                true
            });
        let rows = make_query().build().fetch_all(&mut conn).await.unwrap();
        let steps = work.load(Ordering::Relaxed);
        conn.lock_handle().await.unwrap().remove_progress_handler();
        assert_eq!(rows.len(), 3);
        assert!(
            steps < 10000,
            "{filter:?} used {steps} VM instructions\n{plan}"
        );
        assert_eq!(
            rows[0].get::<String, _>("thread_id"),
            if filter == CommentFilter::Orphaned {
                "090002"
            } else {
                "090001"
            }
        );
    }
}
