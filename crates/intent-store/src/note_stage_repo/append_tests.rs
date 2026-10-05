use super::*;
use intent_core::note_stage::{
    NoteStageAction, NoteStageOutput, NoteStageSelection, NOTE_STAGE_STREAMS,
};
use sqlx::Connection;

// Isolated helper fixture mirrors the agreed append table contract. Production
// migration, operation lifecycle and service authorization have separate gates.
const SCHEMA:&str="
CREATE TABLE note_stage_chunk(operation_key TEXT,stream TEXT,sequence INTEGER,previous_digest TEXT,chunk_digest TEXT,record_count INTEGER,PRIMARY KEY(operation_key,stream,sequence));
CREATE TABLE note_stage_record(operation_key TEXT,stream TEXT,chunk_sequence INTEGER,ordinal INTEGER,value TEXT,PRIMARY KEY(operation_key,stream,chunk_sequence,ordinal),FOREIGN KEY(operation_key,stream,chunk_sequence) REFERENCES note_stage_chunk(operation_key,stream,sequence));
CREATE TABLE note_stage_stream(operation_key TEXT,stream TEXT,next_sequence INTEGER,last_digest TEXT,records INTEGER,tail TEXT,PRIMARY KEY(operation_key,stream));
CREATE TABLE note_stage_text(operation_key TEXT,text_id TEXT,length INTEGER,utf8_bytes INTEGER,sha256 TEXT,PRIMARY KEY(operation_key,text_id));
CREATE TABLE note_stage_text_piece(operation_key TEXT,text_id TEXT,start INTEGER,end INTEGER,text TEXT CHECK(length(CAST(text AS BLOB))<=4096),PRIMARY KEY(operation_key,text_id,start),FOREIGN KEY(operation_key,text_id) REFERENCES note_stage_text(operation_key,text_id));
";
fn header() -> NoteStageHeader {
    NoteStageHeader {
        base_revision: "before".into(),
        editor_session_id: "session".into(),
        local_edit_sequence: 8,
        live_generation: 1,
        selection_generation: 1,
        action: NoteStageAction::Mutate,
        output: NoteStageOutput::Source,
        selection: NoteStageSelection::Ranges,
        query: None,
    }
}
fn request(
    stream: NoteStageStream,
    sequence: u64,
    previous: Option<String>,
    records: Vec<Value>,
) -> NoteStageAppend {
    let mut q = NoteStageAppend {
        backend_id: "backend".into(),
        workspace_id: "workspace".into(),
        note_id: "note".into(),
        note_instance_id: "instance".into(),
        operation_id: "11111111-1111-4111-8111-111111111111".into(),
        header_digest: "a".repeat(64),
        stream,
        sequence,
        previous_digest: previous,
        records,
        chunk_digest: String::new(),
    };
    q.chunk_digest = q.computed_digest().unwrap();
    q
}
fn text(id: &str, offset: u64, value: &str) -> Value {
    json!({"kind":"text","id":id,"offset":offset,"text":value})
}
fn splice(group: u64, ordinal: u64, start: u64, end: u64) -> Value {
    json!({"kind":"splice","localSequence":group,"ordinal":ordinal,"start":start,"end":end,"replacement":{"textId":"later","length":1,"utf8Bytes":1,"sha256":"b".repeat(64)}})
}
async fn fixture() -> SqliteConnection {
    let mut conn = SqliteConnection::connect("sqlite::memory:").await.unwrap();
    sqlx::query("PRAGMA foreign_keys=ON")
        .execute(&mut conn)
        .await
        .unwrap();
    for statement in SCHEMA.split(';').filter(|s| !s.trim().is_empty()) {
        sqlx::query(statement).execute(&mut conn).await.unwrap();
    }
    for stream in NOTE_STAGE_STREAMS {
        sqlx::query("INSERT INTO note_stage_stream VALUES('op',?,0,NULL,0,?)")
            .bind(stream_name(stream))
            .bind(serde_json::to_string(&NoteStageTail::default()).unwrap())
            .execute(&mut conn)
            .await
            .unwrap();
    }
    conn
}
async fn committed(conn: &mut SqliteConnection, q: &NoteStageAppend) -> Value {
    let mut tx = conn.begin().await.unwrap();
    let out = append(&mut tx, "op", &header(), q).await.unwrap();
    tx.commit().await.unwrap();
    out
}
async fn rolled_back(conn: &mut SqliteConnection, q: &NoteStageAppend) -> Error {
    let mut tx = conn.begin().await.unwrap();
    let error = append(&mut tx, "op", &header(), q).await.unwrap_err();
    tx.rollback().await.unwrap();
    error
}
async fn totals(conn: &mut SqliteConnection) -> (i64, i64, i64, i64) {
    let chunks = sqlx::query_scalar("SELECT count(*) FROM note_stage_chunk")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    let records = sqlx::query_scalar("SELECT count(*) FROM note_stage_record")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    let texts = sqlx::query_scalar("SELECT count(*) FROM note_stage_text")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    let pieces = sqlx::query_scalar("SELECT count(*) FROM note_stage_text_piece")
        .fetch_one(&mut *conn)
        .await
        .unwrap();
    (chunks, records, texts, pieces)
}

#[tokio::test]
async fn stage_append_old_chunk_replay_returns_original_ack_without_writes() {
    let mut conn = fixture().await;
    let first = request(NoteStageStream::Text, 0, None, vec![text("id", 0, "😀")]);
    let ack = committed(&mut conn, &first).await;
    let next = request(
        NoteStageStream::Text,
        1,
        Some(first.chunk_digest.clone()),
        vec![text("id", 2, "x")],
    );
    committed(&mut conn, &next).await;
    let before = totals(&mut conn).await;
    assert_eq!(committed(&mut conn, &first).await, ack);
    assert_eq!(ack["nextSequence"], 1);
    assert_eq!(totals(&mut conn).await, before);
    let frame = json!({"jsonrpc":"2.0","id":"escaped\\\"\n","result":ack});
    assert!(frame.to_string().len() < 4096);
    let changed = request(NoteStageStream::Text, 0, None, vec![text("id", 0, "other")]);
    assert!(matches!(
        rolled_back(&mut conn, &changed).await,
        Error::NoteMutation(NoteMutationError::Mismatch)
    ));
    let mut forged = first.clone();
    forged.records[0]["text"] = json!("forged");
    assert!(matches!(
        rolled_back(&mut conn, &forged).await,
        Error::NoteMutation(NoteMutationError::Mismatch)
    ));
    assert_eq!(totals(&mut conn).await, before);
}
#[tokio::test]
async fn stage_append_text_pieces_keep_scalar_boundaries_and_indexed_per_id_offsets() {
    let mut conn = fixture().await;
    let content = "a".repeat(4095) + "😀\r\n" + &"漢".repeat(1800);
    let units = u64::try_from(content.encode_utf16().count()).unwrap();
    let first = request(
        NoteStageStream::Text,
        0,
        None,
        vec![
            text("empty", 0, ""),
            text("large", 0, &content),
            text("second", 0, "é"),
        ],
    );
    committed(&mut conn, &first).await;
    let next = request(
        NoteStageStream::Text,
        1,
        Some(first.chunk_digest.clone()),
        vec![text("large", units, "tail"), text("second", 1, "😀")],
    );
    committed(&mut conn, &next).await;
    let rows=sqlx::query("SELECT start,end,text FROM note_stage_text_piece WHERE operation_key='op' AND text_id='large' ORDER BY start").fetch_all(&mut conn).await.unwrap();
    let mut restored = String::new();
    let mut at = 0_i64;
    for row in rows {
        assert_eq!(row.get::<i64, _>("start"), at);
        let part: String = row.get("text");
        assert!(part.len() <= 4096);
        at += i64::try_from(part.encode_utf16().count()).unwrap();
        assert_eq!(row.get::<i64, _>("end"), at);
        restored.push_str(&part);
    }
    assert_eq!(restored, content + "tail");
    let metadata = sqlx::query(TEXT_SQL)
        .bind("op")
        .bind("large")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(metadata.get::<i64, _>("length"), at);
    assert_eq!(
        metadata.get::<i64, _>("utf8_bytes"),
        i64::try_from(restored.len()).unwrap()
    );
    let empty: i64 =
        sqlx::query_scalar("SELECT count(*) FROM note_stage_text_piece WHERE text_id='empty'")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(empty, 0);
    let gap = request(
        NoteStageStream::Text,
        2,
        Some(next.chunk_digest.clone()),
        vec![text("large", units + 3, "bad")],
    );
    assert!(matches!(
        rolled_back(&mut conn, &gap).await,
        Error::NoteMutation(NoteMutationError::Invalid)
    ));
}
#[tokio::test]
async fn stage_append_rejects_chain_gaps_and_preserves_history_tail_across_chunks() {
    let mut conn = fixture().await;
    let first = request(NoteStageStream::Dirty, 0, None, vec![splice(2, 0, 10, 12)]);
    committed(&mut conn, &first).await;
    let gap = request(
        NoteStageStream::Dirty,
        2,
        Some(first.chunk_digest.clone()),
        vec![splice(2, 1, 15, 16)],
    );
    assert!(matches!(
        rolled_back(&mut conn, &gap).await,
        Error::NoteMutation(NoteMutationError::Mismatch)
    ));
    let wrong = request(
        NoteStageStream::Dirty,
        1,
        Some("c".repeat(64)),
        vec![splice(2, 1, 15, 16)],
    );
    assert!(matches!(
        rolled_back(&mut conn, &wrong).await,
        Error::NoteMutation(NoteMutationError::Mismatch)
    ));
    let next = request(
        NoteStageStream::Dirty,
        1,
        Some(first.chunk_digest.clone()),
        vec![splice(2, 1, 15, 16), splice(8, 0, 0, 0)],
    );
    committed(&mut conn, &next).await;
    let before = totals(&mut conn).await;
    for record in [splice(2, 2, 20, 21), splice(8, 2, 4, 5), splice(8, 1, 0, 0)] {
        let invalid = request(
            NoteStageStream::Dirty,
            2,
            Some(next.chunk_digest.clone()),
            vec![record],
        );
        assert!(matches!(
            rolled_back(&mut conn, &invalid).await,
            Error::NoteMutation(NoteMutationError::Invalid)
        ));
    }
    assert_eq!(totals(&mut conn).await, before);
    let raw: String = sqlx::query_scalar(
        "SELECT tail FROM note_stage_stream WHERE operation_key='op' AND stream='dirty'",
    )
    .fetch_one(&mut conn)
    .await
    .unwrap();
    let tail: NoteStageTail = serde_json::from_str(&raw).unwrap();
    assert_eq!(tail.local_sequence, Some(8));
    assert_eq!(tail.next_ordinal, 1);
}
#[tokio::test]
async fn stage_append_partial_text_and_late_sql_errors_roll_back_every_table() {
    let mut conn = fixture().await;
    let bad = request(
        NoteStageStream::Text,
        0,
        None,
        vec![text("id", 0, "😀"), text("id", 1, "split")],
    );
    assert!(matches!(
        rolled_back(&mut conn, &bad).await,
        Error::NoteMutation(NoteMutationError::Invalid)
    ));
    assert_eq!(totals(&mut conn).await, (0, 0, 0, 0));
    sqlx::query("CREATE TRIGGER fail_stream BEFORE UPDATE ON note_stage_stream BEGIN SELECT RAISE(ABORT,'injected tail persistence failure'); END").execute(&mut conn).await.unwrap();
    let good = request(NoteStageStream::Text, 0, None, vec![text("id", 0, "😀")]);
    assert!(matches!(
        rolled_back(&mut conn, &good).await,
        Error::Internal(_)
    ));
    assert_eq!(totals(&mut conn).await, (0, 0, 0, 0));
    let sequence: i64 =
        sqlx::query_scalar("SELECT next_sequence FROM note_stage_stream WHERE stream='text'")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(sequence, 0);
    sqlx::query("DROP TRIGGER fail_stream")
        .execute(&mut conn)
        .await
        .unwrap();
    committed(&mut conn, &good).await;
    assert_eq!(totals(&mut conn).await, (1, 1, 1, 1));
}
#[tokio::test]
async fn stage_append_uses_indexed_single_key_queries_and_bounded_chunks() {
    let mut conn = fixture().await;
    let records = (0..128).map(|i| splice(8, i, i * 2, i * 2 + 1)).collect();
    let dense = request(NoteStageStream::Dirty, 0, None, records);
    committed(&mut conn, &dense).await;
    let mut excess = dense.clone();
    excess.sequence = 1;
    excess.previous_digest = Some(dense.chunk_digest.clone());
    excess.records.push(splice(8, 128, 256, 257));
    assert!(matches!(
        rolled_back(&mut conn, &excess).await,
        Error::NoteMutation(NoteMutationError::Budget)
    ));
    for sql in [REPLAY_SQL, STREAM_SQL, TEXT_SQL] {
        let sql = format!("EXPLAIN QUERY PLAN {sql}");
        let plan = sqlx::query(&sql).bind("op").bind("text");
        let rows = if sql.ends_with(REPLAY_SQL) {
            plan.bind(0).fetch_all(&mut conn).await.unwrap()
        } else {
            plan.fetch_all(&mut conn).await.unwrap()
        };
        let plan = rows
            .iter()
            .map(|r| r.get::<String, _>("detail"))
            .collect::<Vec<_>>()
            .join(" ");
        assert!(plan.contains("SEARCH"), "{plan}");
        assert!(!plan.contains("SCAN"), "{plan}");
    }
}
