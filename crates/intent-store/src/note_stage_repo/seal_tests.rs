use super::super::append;
use super::*;
use intent_core::note_stage::{
    NoteStageAction, NoteStageManifestEntry, NoteStageOutput, NoteStageSelection,
};
use serde_json::json;
use sqlx::Connection;

const SCHEMA:&str="
CREATE TABLE note_stage_chunk(operation_key TEXT,stream TEXT,sequence INTEGER,previous_digest TEXT,chunk_digest TEXT,record_count INTEGER,PRIMARY KEY(operation_key,stream,sequence));
CREATE TABLE note_stage_record(operation_key TEXT,stream TEXT,chunk_sequence INTEGER,ordinal INTEGER,value TEXT,PRIMARY KEY(operation_key,stream,chunk_sequence,ordinal));
CREATE TABLE note_stage_stream(operation_key TEXT,stream TEXT,next_sequence INTEGER,last_digest TEXT,records INTEGER,tail TEXT,PRIMARY KEY(operation_key,stream));
CREATE TABLE note_stage_text(operation_key TEXT,text_id TEXT,length INTEGER,utf8_bytes INTEGER,PRIMARY KEY(operation_key,text_id));
CREATE TABLE note_stage_text_piece(operation_key TEXT,text_id TEXT,start INTEGER,end INTEGER,text TEXT,PRIMARY KEY(operation_key,text_id,start));
";
fn header() -> NoteStageHeader {
    NoteStageHeader {
        base_revision: "base".into(),
        editor_session_id: "editor".into(),
        local_edit_sequence: 9,
        live_generation: 0,
        selection_generation: 0,
        action: NoteStageAction::Mutate,
        output: NoteStageOutput::Source,
        selection: NoteStageSelection::Ranges,
        query: None,
    }
}
async fn fixture() -> SqliteConnection {
    let mut conn = SqliteConnection::connect("sqlite::memory:").await.unwrap();
    for sql in SCHEMA.split(';').filter(|s| !s.trim().is_empty()) {
        sqlx::query(sql).execute(&mut conn).await.unwrap();
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
fn chunk(sequence: u64, previous: Option<String>, group: u64) -> NoteStageAppend {
    let mut request=NoteStageAppend{backend_id:"backend".into(),workspace_id:"workspace".into(),note_id:"note".into(),note_instance_id:"instance".into(),operation_id:"11111111-1111-4111-8111-111111111111".into(),header_digest:"a".repeat(64),stream:NoteStageStream::Dirty,sequence,previous_digest:previous,chunk_digest:String::new(),
        records:(0..128).map(|i|json!({"kind":"splice","localSequence":group,"ordinal":i,"start":i*2,"end":i*2+1,"replacement":{"textId":"later","length":0,"utf8Bytes":0,"sha256":"b".repeat(64)}})).collect()};
    request.chunk_digest = request.computed_digest().unwrap();
    request
}
async fn append(conn: &mut SqliteConnection, request: &NoteStageAppend) {
    let mut tx = conn.begin().await.unwrap();
    append::append(&mut tx, "op", &header(), request)
        .await
        .unwrap();
    tx.commit().await.unwrap();
}
async fn manifest(conn: &mut SqliteConnection) -> NoteStageSeal {
    let q = chunk(0, None, 9);
    let mut manifest = Vec::new();
    for stream in NOTE_STAGE_STREAMS {
        let row=sqlx::query("SELECT next_sequence,last_digest,records FROM note_stage_stream WHERE operation_key='op' AND stream=?").bind(stream_name(stream)).fetch_one(&mut *conn).await.unwrap();
        manifest.push(NoteStageManifestEntry {
            stream,
            chunks: u64::try_from(row.get::<i64, _>("next_sequence")).unwrap(),
            records: u64::try_from(row.get::<i64, _>("records")).unwrap(),
            last_digest: row.get("last_digest"),
        });
    }
    let mut q = NoteStageSeal {
        backend_id: q.backend_id,
        workspace_id: q.workspace_id,
        note_id: q.note_id,
        note_instance_id: q.note_instance_id,
        operation_id: q.operation_id,
        header_digest: q.header_digest,
        manifest,
        payload_digest: String::new(),
    };
    q.payload_digest = q.computed_digest().unwrap();
    q
}
#[tokio::test]
async fn stage_seal_manifest_checks_complete_dense_chains_without_operation_record_cap() {
    let mut conn = fixture().await;
    let one = chunk(0, None, 7);
    append(&mut conn, &one).await;
    let two = chunk(1, Some(one.chunk_digest), 9);
    append(&mut conn, &two).await;
    let request = manifest(&mut conn).await;
    verify_manifest(&mut conn, "op", &header(), &request)
        .await
        .unwrap();
    assert_eq!(request.manifest[1].records, 256);
    let extra = chunk(2, Some(two.chunk_digest), 9);
    sqlx::query("INSERT INTO note_stage_chunk VALUES('op','dirty',2,?,?,1)")
        .bind(extra.previous_digest)
        .bind(extra.chunk_digest)
        .execute(&mut conn)
        .await
        .unwrap();
    assert!(matches!(
        verify_manifest(&mut conn, "op", &header(), &request).await,
        Err(Error::NoteMutation(NoteMutationError::Mismatch))
    ));
}
#[tokio::test]
async fn stage_seal_manifest_rejects_changed_record_missing_ordinal_and_chain() {
    let mut conn = fixture().await;
    append(&mut conn, &chunk(0, None, 9)).await;
    let request = manifest(&mut conn).await;
    for mutation in [
        "UPDATE note_stage_record SET value=json_set(value,'$.start',1234) WHERE ordinal=0",
        "DELETE FROM note_stage_record WHERE ordinal=10",
        "UPDATE note_stage_chunk SET previous_digest='bad'",
    ] {
        let mut tx = conn.begin().await.unwrap();
        sqlx::query(mutation).execute(&mut *tx).await.unwrap();
        assert!(verify_manifest(&mut tx, "op", &header(), &request)
            .await
            .is_err());
        tx.rollback().await.unwrap();
        verify_manifest(&mut conn, "op", &header(), &request)
            .await
            .unwrap();
    }
}
#[tokio::test]
async fn stage_seal_manifest_rejects_tail_or_captured_fence_mismatch() {
    let mut conn = fixture().await;
    append(&mut conn, &chunk(0, None, 8)).await;
    let request = manifest(&mut conn).await;
    assert!(matches!(
        verify_manifest(&mut conn, "op", &header(), &request).await,
        Err(Error::NoteMutation(NoteMutationError::Invalid))
    ));
    let mut captured = header();
    captured.local_edit_sequence = 8;
    verify_manifest(&mut conn, "op", &captured, &request)
        .await
        .unwrap();
    sqlx::query(
        "UPDATE note_stage_stream SET tail=json_set(tail,'$.nextOrdinal',0) WHERE stream='dirty'",
    )
    .execute(&mut conn)
    .await
    .unwrap();
    assert!(matches!(
        verify_manifest(&mut conn, "op", &captured, &request).await,
        Err(Error::NoteMutation(NoteMutationError::Mismatch))
    ));
}
#[tokio::test]
async fn stage_seal_manifest_accepts_empty_dirty_prefix_and_rejects_incomplete_stream_set() {
    let mut conn = fixture().await;
    let request = manifest(&mut conn).await;
    verify_manifest(&mut conn, "op", &header(), &request)
        .await
        .unwrap();
    sqlx::query("DELETE FROM note_stage_stream WHERE stream='live'")
        .execute(&mut conn)
        .await
        .unwrap();
    assert!(verify_manifest(&mut conn, "op", &header(), &request)
        .await
        .is_err());
}

async fn append_text(conn: &mut SqliteConnection, id: &str, text: &str) {
    let mut request = chunk(0, None, 9);
    request.stream = NoteStageStream::Text;
    request.records = vec![json!({"kind":"text","id":id,"offset":0,"text":text})];
    request.chunk_digest = request.computed_digest().unwrap();
    append(conn, &request).await;
}

#[tokio::test]
async fn stage_seal_text_hashes_raw_unicode_bytes_across_bounded_pieces() {
    let mut conn = fixture().await;
    let text = "é😀\"\n".repeat(1500);
    append_text(&mut conn, "unicode", &text).await;
    let actual = verify_text(&mut conn, "op", "unicode").await.unwrap();
    let expected_hash: String = hex_digest(Sha256::digest(text.as_bytes()).as_ref());
    assert_eq!(
        actual,
        VerifiedText {
            length: u64::try_from(text.encode_utf16().count()).unwrap(),
            utf8_bytes: u64::try_from(text.len()).unwrap(),
            sha256: expected_hash
        }
    );
    let pieces: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM note_stage_text_piece")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert!(pieces > 1);
    assert!(verify_text(&mut conn, "other-operation", "unicode")
        .await
        .is_err());
    assert!(verify_text(&mut conn, "op", "missing").await.is_err());
}

#[tokio::test]
async fn stage_seal_text_accepts_owned_empty_and_rejects_gaps_overlaps_and_wrong_extents() {
    let mut conn = fixture().await;
    append_text(&mut conn, "empty", "").await;
    let actual = verify_text(&mut conn, "op", "empty").await.unwrap();
    assert_eq!(actual.length, 0);
    assert_eq!(actual.utf8_bytes, 0);
    assert_eq!(
        actual.sha256,
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
    sqlx::query("INSERT INTO note_stage_text VALUES('op','value',4,6)")
        .execute(&mut conn)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO note_stage_text_piece VALUES('op','value',0,2,'😀'),('op','value',2,4,'ab')",
    )
    .execute(&mut conn)
    .await
    .unwrap();
    verify_text(&mut conn, "op", "value").await.unwrap();
    for mutation in [
        "INSERT INTO note_stage_text_piece VALUES('op','value',-1,0,'x')",
        "DELETE FROM note_stage_text_piece WHERE text_id='value' AND start=0",
        "UPDATE note_stage_text_piece SET start=1 WHERE text_id='value' AND start=2",
        "UPDATE note_stage_text_piece SET end=1 WHERE text_id='value' AND start=0",
        "UPDATE note_stage_text SET utf8_bytes=7 WHERE text_id='value'",
        "UPDATE note_stage_text_piece SET text=printf('%05000d',0) WHERE text_id='value' AND start=0",
    ] {
        let mut tx=conn.begin().await.unwrap();
        sqlx::query(mutation).execute(&mut *tx).await.unwrap();
        assert!(verify_text(&mut tx,"op","value").await.is_err(),"{mutation}");
        tx.rollback().await.unwrap();
        verify_text(&mut conn,"op","value").await.unwrap();
    }
}

#[test]
fn stage_seal_projection_checks_shape_and_native_coordinates_without_source_unit_substitution() {
    let value = json!({"version":1,"nodeType":"paragraph","parentOrdinal":null,"nativeRange":{"from":7,"to":9_007_199_254_740_991_u64}});
    assert_eq!(
        projection_descriptor(&value, 0).unwrap(),
        ProjectionDescriptor {
            node_type: "paragraph",
            parent_ordinal: None,
            native_from: 7,
            native_to: 9_007_199_254_740_991,
            attributes_ref: None
        }
    );
    let mut child = value.clone();
    child["parentOrdinal"] = json!(0);
    child["attributesRef"] = json!("owned-attributes");
    assert_eq!(
        projection_descriptor(&child, 1).unwrap().attributes_ref,
        Some("owned-attributes")
    );
    for (key, bad) in [
        ("version", json!(2)),
        ("nodeType", json!("")),
        ("nodeType", json!("é".repeat(513))),
        ("parentOrdinal", json!(1)),
        ("parentOrdinal", json!(-1)),
        ("attributesRef", Value::Null),
        ("attributesRef", json!("")),
        ("attributesRef", json!("x".repeat(257))),
        ("nativeRange", json!({"from":2,"to":1})),
        (
            "nativeRange",
            json!({"from":0,"to":9_007_199_254_740_992_u64}),
        ),
        ("nativeRange", json!({"from":0,"to":1,"sourceStart":0})),
        ("extra", json!(true)),
    ] {
        let mut malformed = child.clone();
        malformed[key] = bad;
        assert!(projection_descriptor(&malformed, 1).is_err(), "{key}");
    }
    for key in ["version", "nodeType", "parentOrdinal", "nativeRange"] {
        let mut malformed = value.clone();
        malformed.as_object_mut().unwrap().remove(key);
        assert!(projection_descriptor(&malformed, 0).is_err(), "{key}");
    }
    assert!(projection_descriptor(&child, 0).is_err());
}

const VIEW_SCHEMA: &str = "
ALTER TABLE note_stage_text ADD COLUMN sha256 TEXT;
CREATE TABLE note_stage_root(root_key TEXT PRIMARY KEY,workspace_id TEXT,note_id TEXT,content_generation TEXT,source_length INTEGER);
CREATE TABLE note_stage_base_piece(root_key TEXT,start INTEGER,end INTEGER,text TEXT,PRIMARY KEY(root_key,start));
CREATE TABLE note_page_piece(workspace_id TEXT,note_id TEXT,content_generation TEXT,start INTEGER,end INTEGER,text TEXT,PRIMARY KEY(workspace_id,note_id,content_generation,start));
CREATE TABLE note_stage_view(operation_key TEXT,generation INTEGER,input_generation INTEGER,history_group TEXT,length INTEGER,PRIMARY KEY(operation_key,generation));
CREATE TABLE note_stage_view_piece(operation_key TEXT,generation INTEGER,start INTEGER,end INTEGER,origin_kind TEXT,origin_id TEXT,origin_start INTEGER,PRIMARY KEY(operation_key,generation,start),FOREIGN KEY(operation_key,generation) REFERENCES note_stage_view(operation_key,generation) ON DELETE CASCADE);
";
async fn view_fixture(base: &str) -> SqliteConnection {
    let mut conn = fixture().await;
    for sql in VIEW_SCHEMA.split(';').filter(|s| !s.trim().is_empty()) {
        sqlx::query(sql).execute(&mut conn).await.unwrap();
    }
    let length = i64::try_from(base.encode_utf16().count()).unwrap();
    sqlx::query("INSERT INTO note_stage_root VALUES('root','ws','note','gen',?)")
        .bind(length)
        .execute(&mut conn)
        .await
        .unwrap();
    let mut byte = 0;
    let mut position = 0_i64;
    while byte < base.len() {
        let mut end = (byte + 4096).min(base.len());
        while !base.is_char_boundary(end) {
            end -= 1;
        }
        let part = &base[byte..end];
        let next = position + i64::try_from(part.encode_utf16().count()).unwrap();
        sqlx::query("INSERT INTO note_page_piece VALUES('ws','note','gen',?,?,?)")
            .bind(position)
            .bind(next)
            .bind(part)
            .execute(&mut conn)
            .await
            .unwrap();
        position = next;
        byte = end;
    }
    conn
}
fn text_reference(id: &str, text: &str) -> Value {
    let digest: String = hex_digest(Sha256::digest(text.as_bytes()).as_ref());
    json!({"textId":id,"length":text.encode_utf16().count(),"utf8Bytes":text.len(),"sha256":digest})
}
async fn upload(conn: &mut SqliteConnection, stream: NoteStageStream, records: Vec<Value>) {
    let row=sqlx::query("SELECT next_sequence,last_digest FROM note_stage_stream WHERE operation_key='op' AND stream=?").bind(stream_name(stream)).fetch_one(&mut *conn).await.unwrap();
    let mut request = chunk(
        u64::try_from(row.get::<i64, _>("next_sequence")).unwrap(),
        row.get("last_digest"),
        9,
    );
    request.stream = stream;
    request.records = records;
    request.chunk_digest = request.computed_digest().unwrap();
    append(conn, &request).await;
}
async fn text_upload(conn: &mut SqliteConnection, id: &str, text: &str) {
    upload(
        conn,
        NoteStageStream::Text,
        vec![json!({"kind":"text","id":id,"offset":0,"text":text})],
    )
    .await;
}
async fn two_groups(conn: &mut SqliteConnection) {
    text_upload(conn, "first", "XY").await;
    text_upload(conn, "second", "🦀").await;
    upload(conn,NoteStageStream::Dirty,vec![
        json!({"kind":"splice","localSequence":7,"ordinal":0,"start":1,"end":3,"replacement":text_reference("first","XY")}),
        json!({"kind":"splice","localSequence":9,"ordinal":0,"start":2,"end":4,"replacement":text_reference("second","🦀")}),
    ]).await;
}
// Only tests reconstruct tiny expected views. Production copies descriptors.
async fn tiny_view(conn: &mut SqliteConnection, generation: u64, base: &str) -> String {
    let rows=sqlx::query("SELECT start,end,origin_kind,origin_id,origin_start FROM note_stage_view_piece WHERE operation_key='op' AND generation=? ORDER BY start")
        .bind(integer(generation).unwrap()).fetch_all(&mut *conn).await.unwrap();
    let mut result = String::new();
    for row in rows {
        let source = if row.get::<&str, _>("origin_kind") == "root" {
            base.to_owned()
        } else {
            let parts:Vec<String>=sqlx::query_scalar("SELECT text FROM note_stage_text_piece WHERE operation_key='op' AND text_id=? ORDER BY start")
                .bind(row.get::<&str,_>("origin_id")).fetch_all(&mut *conn).await.unwrap();
            parts.concat()
        };
        let units: Vec<u16> = source.encode_utf16().collect();
        let start = usize::try_from(row.get::<i64, _>("origin_start")).unwrap();
        let length =
            usize::try_from(row.get::<i64, _>("end") - row.get::<i64, _>("start")).unwrap();
        result.push_str(&String::from_utf16(&units[start..start + length]).unwrap());
    }
    result
}
#[tokio::test]
async fn stage_seal_frozen_views_retain_each_group_and_leave_final_mutation_unapplied() {
    let mut conn = view_fixture("a😀bc").await;
    two_groups(&mut conn).await;
    text_upload(&mut conn, "mutation", "!").await;
    upload(&mut conn,NoteStageStream::Mutation,vec![json!({"kind":"splice","ordinal":0,"start":0,"end":1,"replacement":text_reference("mutation","!")})]).await;
    upload(&mut conn,NoteStageStream::Selection,vec![json!({"kind":"range","ordinal":0,"start":1,"end":4,"anchorAffinity":"before","headAffinity":"after","direction":"forward"})]).await;
    let request = manifest(&mut conn).await;
    let mut tx = conn.begin().await.unwrap();
    let prepared = prepare_frozen_view(&mut tx, "op", &header(), &request, "root")
        .await
        .unwrap();
    assert_eq!(prepared.generation, 2);
    assert_eq!(prepared.length, 5);
    assert!(!prepared.view_id.is_empty());
    tx.commit().await.unwrap();
    assert_eq!(tiny_view(&mut conn, 0, "a😀bc").await, "a😀bc");
    assert_eq!(tiny_view(&mut conn, 1, "a😀bc").await, "aXYbc");
    assert_eq!(tiny_view(&mut conn, 2, "a😀bc").await, "aX🦀c");
    let groups:Vec<(i64,Option<i64>,Option<String>,i64)>=sqlx::query_as("SELECT generation,input_generation,history_group,length FROM note_stage_view ORDER BY generation").fetch_all(&mut conn).await.unwrap();
    assert_eq!(
        groups,
        vec![
            (0, None, None, 5),
            (1, Some(0), Some("7".into()), 5),
            (2, Some(1), Some("9".into()), 5)
        ]
    );
    let cached: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM note_stage_text WHERE length(sha256)=64")
            .fetch_one(&mut conn)
            .await
            .unwrap();
    assert_eq!(cached, 3);
}
#[tokio::test]
async fn stage_seal_frozen_view_empty_and_large_base_keep_only_root_descriptor() {
    for base in [String::new(), "x".repeat(100_001)] {
        let mut conn = view_fixture(&base).await;
        let request = manifest(&mut conn).await;
        let mut tx = conn.begin().await.unwrap();
        let prepared = prepare_frozen_view(&mut tx, "op", &header(), &request, "root")
            .await
            .unwrap();
        assert_eq!(prepared.length, u64::try_from(base.len()).unwrap());
        assert_eq!(prepared.generation, 0);
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM note_stage_view_piece")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
        assert_eq!(count, i64::from(!base.is_empty()));
        tx.rollback().await.unwrap();
    }
}
#[tokio::test]
async fn stage_seal_view_checks_surrogate_boundaries_and_every_reference_hash() {
    for (start, end, reference) in [
        (2, 3, text_reference("replacement", "x")),
        (0, 6, text_reference("replacement", "x")),
        (0, 1, text_reference("missing", "x")),
        (0, 1, text_reference("replacement", "y")),
        (0, 1, text_reference("replacement", "xx")),
    ] {
        let mut conn = view_fixture("a😀bc").await;
        text_upload(&mut conn, "replacement", "x").await;
        upload(&mut conn,NoteStageStream::Dirty,vec![json!({"kind":"splice","localSequence":9,"ordinal":0,"start":start,"end":end,"replacement":reference})]).await;
        let request = manifest(&mut conn).await;
        let mut tx = conn.begin().await.unwrap();
        assert!(
            prepare_frozen_view(&mut tx, "op", &header(), &request, "root")
                .await
                .is_err()
        );
        tx.rollback().await.unwrap();
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM note_stage_view")
            .fetch_one(&mut conn)
            .await
            .unwrap();
        assert_eq!(count, 0);
        let hash: Option<String> =
            sqlx::query_scalar("SELECT sha256 FROM note_stage_text WHERE text_id='replacement'")
                .fetch_one(&mut conn)
                .await
                .unwrap();
        assert!(hash.is_none());
    }
}
#[tokio::test]
async fn stage_seal_partial_generations_roll_back_after_real_sql_failure_then_retry() {
    let mut conn = view_fixture("a😀bc").await;
    two_groups(&mut conn).await;
    let request = manifest(&mut conn).await;
    sqlx::query("CREATE TRIGGER reject_second BEFORE INSERT ON note_stage_view_piece WHEN new.generation=2 BEGIN SELECT RAISE(ABORT,'injected second generation failure'); END").execute(&mut conn).await.unwrap();
    let mut tx = conn.begin().await.unwrap();
    assert!(
        prepare_frozen_view(&mut tx, "op", &header(), &request, "root")
            .await
            .is_err()
    );
    let partial: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM note_stage_view WHERE generation=1")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    assert_eq!(partial, 1);
    tx.rollback().await.unwrap();
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM note_stage_view")
        .fetch_one(&mut conn)
        .await
        .unwrap();
    assert_eq!(count, 0);
    sqlx::query("DROP TRIGGER reject_second")
        .execute(&mut conn)
        .await
        .unwrap();
    let mut tx = conn.begin().await.unwrap();
    prepare_frozen_view(&mut tx, "op", &header(), &request, "root")
        .await
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(tiny_view(&mut conn, 2, "a😀bc").await, "aX🦀c");
}

#[tokio::test]
async fn stage_seal_one_group_across_chunks_uses_original_input_coordinates() {
    let mut conn = view_fixture("abcdefgh").await;
    text_upload(&mut conn, "one", "XY").await;
    text_upload(&mut conn, "two", "😀").await;
    upload(&mut conn,NoteStageStream::Dirty,vec![json!({"kind":"splice","localSequence":9,"ordinal":0,"start":1,"end":2,"replacement":text_reference("one","XY")})]).await;
    upload(&mut conn,NoteStageStream::Dirty,vec![json!({"kind":"splice","localSequence":9,"ordinal":1,"start":5,"end":6,"replacement":text_reference("two","😀")})]).await;
    let request = manifest(&mut conn).await;
    let mut tx = conn.begin().await.unwrap();
    let prepared = prepare_frozen_view(&mut tx, "op", &header(), &request, "root")
        .await
        .unwrap();
    assert_eq!(prepared.generation, 1);
    assert_eq!(prepared.length, 10);
    assert!(view_boundary(&mut tx, "op", 1, 10, 7).await.is_err());
    view_boundary(&mut tx, "op", 1, 10, 6).await.unwrap();
    view_boundary(&mut tx, "op", 1, 10, 8).await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!(tiny_view(&mut conn, 1, "abcdefgh").await, "aXYcde😀gh");
    for query in [
        "EXPLAIN QUERY PLAN SELECT value FROM note_stage_record WHERE operation_key='op' AND stream='dirty' AND (chunk_sequence,ordinal)>(0,0) ORDER BY chunk_sequence,ordinal LIMIT 1",
        "EXPLAIN QUERY PLAN SELECT start,end,origin_kind,origin_id,origin_start FROM note_stage_view_piece WHERE operation_key='op' AND generation=1 AND start<=5 ORDER BY start DESC LIMIT 1",
        "EXPLAIN QUERY PLAN SELECT length,utf8_bytes,sha256 FROM note_stage_text WHERE operation_key='op' AND text_id='one'",
    ] {
        let rows=sqlx::query(query).fetch_all(&mut conn).await.unwrap();
        let plan=rows.iter().map(|r|r.get::<String,_>("detail")).collect::<Vec<_>>().join(" ");
        assert!(plan.contains("SEARCH"),"{plan}");assert!(!plan.contains("SCAN"),"{plan}");assert!(!plan.contains("TEMP B-TREE"),"{plan}");
    }
}
