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
    view(&mut conn, 0, None, 1000).await;
    view(&mut conn, 1, Some("42"), 1000).await;
    sqlx::query("BEGIN").execute(&mut conn).await.unwrap();
    let mut tail = intent_core::note_stage::NoteStageTail::default();
    for i in 0..1000 {
        splice(
            &mut conn,
            (i / 128, i % 128),
            42,
            u64::try_from(i).unwrap(),
            (u64::try_from(i).unwrap(), u64::try_from(i + 1).unwrap(), 1),
        )
        .await;
        let encoded: String = sqlx::query_scalar("SELECT value FROM note_stage_record WHERE operation_key='op' AND stream='dirty' AND chunk_sequence=? AND ordinal=?")
            .bind(i/128).bind(i%128).fetch_one(&mut conn).await.unwrap();
        let admitted: NoteStageRecord = serde_json::from_str(&encoded).unwrap();
        tail = tail
            .advance(intent_core::note_stage::NoteStageStream::Dirty, &[admitted])
            .unwrap();
    }
    assert_eq!(tail.next_ordinal, 1000);
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

#[tokio::test]
async fn removed_spans_are_retained_sparsely_with_scalar_boundaries_and_bounded_pieces() {
    let mut conn = connection().await;
    sqlx::raw_sql("CREATE TABLE note_stage(operation_key TEXT PRIMARY KEY,root_key TEXT); CREATE TABLE note_stage_root(root_key TEXT PRIMARY KEY,source_length INTEGER); CREATE TABLE note_stage_view_piece(operation_key TEXT,generation INTEGER,start INTEGER,end INTEGER,origin_kind TEXT,origin_id TEXT,origin_start INTEGER,PRIMARY KEY(operation_key,generation,start)); CREATE TABLE note_stage_text(operation_key TEXT,text_id TEXT,length INTEGER,PRIMARY KEY(operation_key,text_id)); CREATE TABLE note_stage_text_piece(operation_key TEXT,text_id TEXT,start INTEGER,end INTEGER,text TEXT,PRIMARY KEY(operation_key,text_id,start)); CREATE TABLE note_operation_source(operation_key TEXT,phase TEXT,start INTEGER,end INTEGER,text TEXT,PRIMARY KEY(operation_key,phase,start)); INSERT INTO note_stage VALUES('op','root'); INSERT INTO note_stage_root VALUES('root',0);")
        .execute(&mut conn).await.unwrap();
    let text = format!("A{}Z", "😀".repeat(3000));
    view(&mut conn, 0, None, 0).await;
    view(&mut conn, 1, Some("1"), 6002).await;
    sqlx::query("INSERT INTO note_stage_text VALUES('op','text',6002)")
        .execute(&mut conn)
        .await
        .unwrap();
    let mut start = 0_i64;
    let mut from = 0;
    while from < text.len() {
        let mut to = (from + 4096).min(text.len());
        while !text.is_char_boundary(to) {
            to -= 1;
        }
        let fragment = &text[from..to];
        let end = start + i64::try_from(fragment.encode_utf16().count()).unwrap();
        sqlx::query("INSERT INTO note_stage_text_piece VALUES('op','text',?,?,?)")
            .bind(start)
            .bind(end)
            .bind(fragment)
            .execute(&mut conn)
            .await
            .unwrap();
        start = end;
        from = to;
    }
    sqlx::query("INSERT INTO note_stage_view_piece VALUES('op',1,0,6002,'text','text',0)")
        .execute(&mut conn)
        .await
        .unwrap();
    let group = ReceiptGroup {
        history_group: "1".into(),
        input_state: "after".into(),
        output_state: "before".into(),
        input_generation: 1,
        input_length: 6002,
    };
    let range = InverseRange {
        ordinal: 0,
        start: 0,
        end: 0,
        replacement_start: 1,
        replacement_end: 6001,
    };
    sqlx::query("BEGIN").execute(&mut conn).await.unwrap();
    let phase = retain_input_span(&mut conn, "op", &group, &range)
        .await
        .unwrap();
    let rows: Vec<(i64,i64,String)> = sqlx::query_as("SELECT start,end,text FROM note_operation_source WHERE operation_key='op' AND phase=? ORDER BY start").bind(&phase).fetch_all(&mut conn).await.unwrap();
    assert!(rows.iter().all(|row| row.2.len() <= 4096));
    assert_eq!(rows.first().unwrap().0, 1);
    assert_eq!(rows.last().unwrap().1, 6001);
    assert_eq!(
        rows.iter().map(|row| row.2.as_str()).collect::<String>(),
        "😀".repeat(3000)
    );
    sqlx::query("ROLLBACK").execute(&mut conn).await.unwrap();
    let invalid_end = InverseRange {
        replacement_end: 2,
        ..range
    };
    assert!(retain_input_span(&mut conn, "op", &group, &invalid_end)
        .await
        .is_err());
}

#[test]
fn adjacent_deletions_and_replacement_boundaries_undo_through_actual_batch_admission() {
    use intent_core::note_mutation::apply_note_splices;
    for (base, edits) in [
        (
            "ab",
            vec![
                NoteSplice {
                    start: 0,
                    end: 1,
                    text: String::new(),
                },
                NoteSplice {
                    start: 1,
                    end: 2,
                    text: String::new(),
                },
            ],
        ),
        (
            "ab",
            vec![
                NoteSplice {
                    start: 0,
                    end: 1,
                    text: String::new(),
                },
                NoteSplice {
                    start: 1,
                    end: 2,
                    text: "😀".into(),
                },
            ],
        ),
        (
            "ab",
            vec![
                NoteSplice {
                    start: 0,
                    end: 1,
                    text: "😀".into(),
                },
                NoteSplice {
                    start: 1,
                    end: 2,
                    text: String::new(),
                },
            ],
        ),
        (
            "a😀z",
            vec![
                NoteSplice {
                    start: 0,
                    end: 1,
                    text: String::new(),
                },
                NoteSplice {
                    start: 1,
                    end: 3,
                    text: String::new(),
                },
            ],
        ),
        (
            "axb",
            vec![
                NoteSplice {
                    start: 0,
                    end: 1,
                    text: String::new(),
                },
                NoteSplice {
                    start: 2,
                    end: 3,
                    text: String::new(),
                },
            ],
        ),
    ] {
        let changed = apply_note_splices(base, &edits).unwrap();
        let mut mapping =
            MappingCursor::new(u64::try_from(base.encode_utf16().count()).unwrap()).unwrap();
        let mut coalescer = InverseCoalescer::default();
        let mut raw = Vec::new();
        let mut merged = Vec::new();
        for edit in &edits {
            let range = mapping
                .push(&NoteSpliceMapping {
                    start: edit.start,
                    end: edit.end,
                    inserted_length: u64::try_from(edit.text.encode_utf16().count()).unwrap(),
                })
                .unwrap();
            raw.push(NoteSplice {
                start: range.start,
                end: range.end,
                text: base[byte(base, range.replacement_start)..byte(base, range.replacement_end)]
                    .into(),
            });
            if let Some(range) = coalescer.push(range).unwrap() {
                merged.push(range);
            }
        }
        if let Some(range) = coalescer.finish().unwrap() {
            merged.push(range);
        }
        assert_eq!(merged.len(), if base == "axb" { 2 } else { 1 });
        assert_eq!(merged[0].ordinal, 0);
        let inverse: Vec<_> = merged
            .iter()
            .map(|range| NoteSplice {
                start: range.start,
                end: range.end,
                text: base[byte(base, range.replacement_start)..byte(base, range.replacement_end)]
                    .into(),
            })
            .collect();
        let restored = apply_note_splices(&changed.source, &inverse).unwrap();
        assert_eq!(restored.source, base);
        if raw[0].start == raw[1].start {
            assert!(apply_note_splices(&changed.source, &raw).is_err());
        }
    }
}

#[tokio::test]
async fn base_byte_comparison_distinguishes_identity_edits_from_source_changes() {
    let mut conn = connection().await;
    sqlx::raw_sql("CREATE TABLE note_operation_source(operation_key TEXT,phase TEXT,start INTEGER,end INTEGER,text TEXT,PRIMARY KEY(operation_key,phase,start)); INSERT INTO note_operation_source VALUES('op','base',0,3,'A😀'),('op','base',3,4,'B');").execute(&mut conn).await.unwrap();
    assert!(source_matches_base(&mut conn, "op", "A😀B").await.unwrap());
    for changed in ["A😀", "A😀BB", "A😀C", "A😃B"] {
        assert!(!source_matches_base(&mut conn, "op", changed).await.unwrap());
    }
    sqlx::query("UPDATE note_operation_source SET start=4 WHERE start=3")
        .execute(&mut conn)
        .await
        .unwrap();
    assert!(source_matches_base(&mut conn, "op", "A😀B").await.is_err());
}
