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
