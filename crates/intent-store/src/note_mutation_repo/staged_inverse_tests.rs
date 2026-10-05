use super::*;
use intent_core::note_mutation::{NoteSourceHistory, NoteSplice};
use serde_json::json;
use sqlx::Connection;

const SCHEMA: &str = "CREATE TABLE note_stage_view(operation_key TEXT,generation INTEGER,input_generation INTEGER,history_group TEXT,length INTEGER,PRIMARY KEY(operation_key,generation)); CREATE TABLE note_stage_record(operation_key TEXT,stream TEXT,chunk_sequence INTEGER,ordinal INTEGER,value TEXT,PRIMARY KEY(operation_key,stream,chunk_sequence,ordinal));";
async fn connection() -> SqliteConnection {
    let mut conn = SqliteConnection::connect("sqlite::memory:").await.unwrap();
    sqlx::raw_sql(SCHEMA).execute(&mut conn).await.unwrap();
    conn
}
async fn view(conn: &mut SqliteConnection, generation: i64, group: Option<&str>, length: i64) {
    sqlx::query("INSERT INTO note_stage_view VALUES('op',?,?,?,?)")
        .bind(generation)
        .bind((generation > 0).then_some(generation - 1))
        .bind(group)
        .bind(length)
        .execute(conn)
        .await
        .unwrap();
}
async fn splice(
    conn: &mut SqliteConnection,
    key: (i64, i64),
    sequence: u64,
    ordinal: u64,
    mapping: (u64, u64, u64),
) {
    let value = json!({"kind":"splice","localSequence":sequence,"ordinal":ordinal,"start":mapping.0,"end":mapping.1,
        "replacement":{"textId":"text","length":mapping.2,"utf8Bytes":mapping.2,"sha256":"0".repeat(64)}});
    sqlx::query("INSERT INTO note_stage_record VALUES('op','dirty',?,?,?)")
        .bind(key.0)
        .bind(key.1)
        .bind(value.to_string())
        .execute(conn)
        .await
        .unwrap();
}
fn byte(text: &str, offset: u64) -> usize {
    let mut units = 0;
    for (position, c) in text.char_indices() {
        if units == offset {
            return position;
        }
        units += u64::try_from(c.len_utf16()).unwrap();
    }
    assert_eq!(units, offset);
    text.len()
}
fn undo(source: &str, input: &str, ranges: &[InverseRange]) -> String {
    let mut result = source.to_owned();
    for range in ranges.iter().rev() {
        let old = &input[byte(input, range.replacement_start)..byte(input, range.replacement_end)];
        result.replace_range(byte(source, range.start)..byte(source, range.end), old);
    }
    result
}
async fn drain(conn: &mut SqliteConnection, group: &CapturedGroup) -> Vec<InverseRange> {
    let mut cursor = group.cursor().unwrap();
    let mut ranges = Vec::new();
    while let Some(range) = next_inverse(conn, "op", group, &mut cursor).await.unwrap() {
        ranges.push(range);
    }
    ranges
}
#[tokio::test]
async fn chronological_groups_cross_chunks_and_restore_unicode() {
    let mut conn = connection().await;
    view(&mut conn, 0, None, 5).await; // A😀BC
    view(&mut conn, 1, Some("2"), 6).await; // AxyB!C
    view(&mut conn, 2, Some("9"), 5).await; // AZB!C
    splice(&mut conn, (0, 0), 2, 0, (1, 3, 2)).await;
    splice(&mut conn, (1, 0), 2, 1, (4, 4, 1)).await;
    splice(&mut conn, (1, 1), 9, 0, (1, 3, 1)).await;
    let mut walk = GroupWalk::new(2).unwrap();
    let latest = previous_group(&mut conn, "op", &mut walk)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (
            latest.generation,
            latest.input_generation,
            latest.history_group.as_str()
        ),
        (2, 1, "9")
    );
    let ranges = drain(&mut conn, &latest).await;
    assert_eq!(undo("AZB!C", "AxyB!C", &ranges), "AxyB!C");
    let earlier = previous_group(&mut conn, "op", &mut walk)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(earlier.history_group, "2");
    let ranges = drain(&mut conn, &earlier).await;
    assert_eq!(
        ranges[1],
        InverseRange {
            ordinal: 1,
            start: 4,
            end: 5,
            replacement_start: 4,
            replacement_end: 4
        }
    );
    assert_eq!(undo("AxyB!C", "A😀BC", &ranges), "A😀BC");
    assert!(previous_group(&mut conn, "op", &mut walk)
        .await
        .unwrap()
        .is_none());
}
#[test]
fn newest_composed_history_restores_canonical_changes_outside_user_range() {
    let input = "a😀--tail";
    let mut history = NoteSourceHistory::new(input.to_owned());
    history
        .apply_phase(&[NoteSplice {
            start: 1,
            end: 3,
            text: "X".into(),
        }])
        .unwrap();
    history
        .apply_phase(&[NoteSplice {
            start: 4,
            end: 8,
            text: "LINK".into(),
        }])
        .unwrap();
    let mut cursor = MappingCursor::new(9).unwrap();
    let ranges: Vec<_> = history
        .mapping()
        .iter()
        .map(|item| cursor.push(item).unwrap())
        .collect();
    cursor
        .finish(u64::try_from(history.source().encode_utf16().count()).unwrap())
        .unwrap();
    assert_eq!(ranges.len(), 2);
    assert_eq!(undo(history.source(), input, &ranges), input);
}
#[test]
fn exact_edit_identity_empty_history_and_safe_arithmetic() {
    let mut same = NoteSourceHistory::new("same".into());
    same.apply_phase(&[NoteSplice {
        start: 0,
        end: 4,
        text: "same".into(),
    }])
    .unwrap();
    let mut cursor = MappingCursor::new(4).unwrap();
    let range = cursor.push(&same.mapping()[0]).unwrap();
    assert_eq!(range.replacement_end, 4);
    cursor.finish(4).unwrap();
    // No native group or operation identity is invented by this helper.
    MappingCursor::new(0).unwrap().finish(0).unwrap();
    MappingCursor::new(4).unwrap().finish(4).unwrap();
    let mut cursor = MappingCursor::new(SAFE).unwrap();
    let overflow = NoteSpliceMapping {
        start: SAFE,
        end: SAFE,
        inserted_length: 1,
    };
    assert!(cursor.push(&overflow).is_err());
    assert_eq!(cursor.ordinal, 0);
    cursor.finish(SAFE).unwrap();
    let mut cursor = MappingCursor::new(4).unwrap();
    cursor
        .push(&NoteSpliceMapping {
            start: 2,
            end: 3,
            inserted_length: 0,
        })
        .unwrap();
    assert!(cursor
        .push(&NoteSpliceMapping {
            start: 1,
            end: 2,
            inserted_length: 0
        })
        .is_err());
    assert_eq!(cursor.ordinal, 1);
    assert!(cursor.finish(4).is_err());
    cursor.finish(3).unwrap();
}
#[tokio::test]
async fn rejects_mismatched_groups_missing_ordinals_and_bad_final_length_without_advancing() {
    let mut conn = connection().await;
    view(&mut conn, 0, None, 3).await;
    view(&mut conn, 1, Some("7"), 3).await;
    splice(&mut conn, (0, 0), 7, 1, (0, 1, 1)).await;
    let mut walk = GroupWalk::new(1).unwrap();
    assert!(previous_group(&mut conn, "op", &mut walk).await.is_err());
    assert_eq!(walk.generation, 1);
    sqlx::query("UPDATE note_stage_record SET value=json_set(value,'$.ordinal',0)")
        .execute(&mut conn)
        .await
        .unwrap();
    sqlx::query("UPDATE note_stage_view SET history_group='07' WHERE generation=1")
        .execute(&mut conn)
        .await
        .unwrap();
    assert!(previous_group(&mut conn, "op", &mut walk).await.is_err());
    sqlx::query("UPDATE note_stage_view SET history_group='7',length=4 WHERE generation=1")
        .execute(&mut conn)
        .await
        .unwrap();
    let group = previous_group(&mut conn, "op", &mut walk)
        .await
        .unwrap()
        .unwrap();
    let mut cursor = group.cursor().unwrap();
    assert!(next_inverse(&mut conn, "op", &group, &mut cursor)
        .await
        .is_err());
    assert_eq!(cursor.mapping.ordinal, 0);
    assert!(!cursor.done);
    assert!(
        previous_group(&mut conn, "foreign", &mut GroupWalk::new(1).unwrap())
            .await
            .is_err()
    );
}
#[tokio::test]
async fn one_thousand_records_stream_with_indexed_bounds_and_transaction_rollback() {
    let mut conn = connection().await;
    view(&mut conn, 0, None, 0).await;
    view(&mut conn, 1, Some("42"), 1000).await;
    sqlx::query("BEGIN").execute(&mut conn).await.unwrap();
    for i in 0..1000 {
        splice(
            &mut conn,
            (i / 128, i % 128),
            42,
            u64::try_from(i).unwrap(),
            (0, 0, 1),
        )
        .await;
    }
    for query in [PREVIOUS_RECORD, NEXT_RECORD] {
        let sql = format!("EXPLAIN QUERY PLAN {query}");
        let mut q = sqlx::query(&sql).bind("op").bind(0_i64).bind(0_i64);
        if query == NEXT_RECORD {
            q = q.bind(i64::MAX).bind(i64::MAX);
        }
        let rows = q.fetch_all(&mut conn).await.unwrap();
        let details: Vec<String> = rows.iter().map(|row| row.get("detail")).collect();
        assert!(details.iter().any(|s| s.contains("SEARCH")));
        assert!(!details
            .iter()
            .any(|s| s.contains("SCAN") || s.contains("TEMP B-TREE")));
    }
    let mut walk = GroupWalk::new(1).unwrap();
    let group = previous_group(&mut conn, "op", &mut walk)
        .await
        .unwrap()
        .unwrap();
    let mut cursor = group.cursor().unwrap();
    for i in 0..1000 {
        let range = next_inverse(&mut conn, "op", &group, &mut cursor)
            .await
            .unwrap()
            .unwrap();
        assert_eq!((range.ordinal, range.start, range.end), (i, i, i + 1));
    }
    assert!(next_inverse(&mut conn, "op", &group, &mut cursor)
        .await
        .unwrap()
        .is_none());
    sqlx::query("ROLLBACK").execute(&mut conn).await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM note_stage_record")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(count, 0);
}
#[tokio::test]
async fn empty_captured_stream_is_valid_but_orphan_records_are_not() {
    let mut conn = connection().await;
    view(&mut conn, 0, None, 0).await;
    assert!(
        previous_group(&mut conn, "op", &mut GroupWalk::new(0).unwrap())
            .await
            .unwrap()
            .is_none()
    );
    splice(&mut conn, (0, 0), 1, 0, (0, 0, 0)).await;
    assert!(
        previous_group(&mut conn, "op", &mut GroupWalk::new(0).unwrap())
            .await
            .is_err()
    );
}

#[test]
fn canonical_only_operation_inverse_and_empty_source_roundtrip() {
    let mut history = NoteSourceHistory::new(String::new());
    history
        .apply_phase(&[NoteSplice {
            start: 0,
            end: 0,
            text: "😀canonical".into(),
        }])
        .unwrap();
    let mut cursor = MappingCursor::new(0).unwrap();
    let ranges: Vec<_> = history
        .mapping()
        .iter()
        .map(|m| cursor.push(m).unwrap())
        .collect();
    cursor
        .finish(u64::try_from(history.source().encode_utf16().count()).unwrap())
        .unwrap();
    assert_eq!(undo(history.source(), "", &ranges), "");
    assert_eq!(ranges.len(), 1);
}

#[tokio::test]
async fn malformed_retained_envelopes_and_broken_generation_chain_fail_closed() {
    let mut conn = connection().await;
    view(&mut conn, 0, None, 1).await;
    view(&mut conn, 1, Some("1"), 1).await;
    splice(&mut conn, (0, 0), 1, 0, (0, 1, 1)).await;
    sqlx::query("UPDATE note_stage_record SET value=?")
        .bind("x".repeat(65537))
        .execute(&mut conn)
        .await
        .unwrap();
    assert!(
        previous_group(&mut conn, "op", &mut GroupWalk::new(1).unwrap())
            .await
            .is_err()
    );
    sqlx::query("DELETE FROM note_stage_record")
        .execute(&mut conn)
        .await
        .unwrap();
    splice(&mut conn, (0, 0), 1, 0, (0, 1, 1)).await;
    sqlx::query("UPDATE note_stage_view SET input_generation=1 WHERE generation=1")
        .execute(&mut conn)
        .await
        .unwrap();
    assert!(
        previous_group(&mut conn, "op", &mut GroupWalk::new(1).unwrap())
            .await
            .is_err()
    );
}
