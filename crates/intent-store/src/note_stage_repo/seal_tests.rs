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
    let expected_hash: String = Sha256::digest(text.as_bytes())
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
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
    let value = json!({"version":1,"nodeType":"paragraph","parentOrdinal":null,"nativeRange":{"from":7,"to":9007199254740991_u64}});
    assert_eq!(
        projection_descriptor(&value, 0).unwrap(),
        ProjectionDescriptor {
            node_type: "paragraph",
            parent_ordinal: None,
            native_from: 7,
            native_to: 9007199254740991,
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
        ("nativeRange", json!({"from":0,"to":9007199254740992_u64})),
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
