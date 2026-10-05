use super::{
    count, request, seal_request, setup, Error, NoteMutationError, NoteStageAppend, NoteStageBegin,
    Store,
};
use intent_core::note_stage::{NoteStageOutput, NoteStageSelection};
use serde_json::{json, Value};

async fn begin_search(store: &Store) -> NoteStageBegin {
    let mut begin = request(store).await;
    begin.header.output = NoteStageOutput::Search;
    begin.header.selection = NoteStageSelection::Ranges;
    begin.header.query = Some(
        serde_json::from_value(json!({"text":"B","caseSensitive":false,"mode":"source"})).unwrap(),
    );
    begin.header_digest = begin.computed_digest().unwrap();
    store.begin_note_stage("alice", &begin).await.unwrap();
    begin
}

fn range(ordinal: u64, start: u64, end: u64) -> Value {
    json!({"kind":"range","ordinal":ordinal,"start":start,"end":end,"anchorAffinity":"before","headAffinity":"after","direction":"forward"})
}

fn chunk(
    begin: &NoteStageBegin,
    sequence: u64,
    previous: Option<&str>,
    records: &[Value],
) -> NoteStageAppend {
    let mut request:NoteStageAppend=serde_json::from_value(json!({"backendId":begin.backend_id,"workspaceId":begin.workspace_id,"noteId":begin.note_id,"noteInstanceId":begin.note_instance_id,"operationId":begin.operation_id,"headerDigest":begin.header_digest,"stream":"selection","sequence":sequence,"previousDigest":previous,"records":records,"chunkDigest":"0".repeat(64)})).unwrap();
    request.chunk_digest = request.computed_digest().unwrap();
    request
}

#[tokio::test]
async fn staged_search_upload_and_seal_union_preserve_manifest_and_rollback() {
    let (store, _tmp, mut note) = setup("A😀BCDEF").await;
    let begin = begin_search(&store).await;
    let first = chunk(&begin, 0, None, &[range(0, 6, 8), range(1, 0, 1)]);
    let ack = store.append_note_stage("alice", &first).await.unwrap();
    assert_eq!(store.append_note_stage("alice", &first).await.unwrap(), ack);
    let second = chunk(
        &begin,
        1,
        Some(first.chunk_digest.as_str()),
        &[range(2, 1, 3), range(3, 0, 1), range(4, 3, 4)],
    );
    store.append_note_stage("alice", &second).await.unwrap();
    let uploaded: Vec<String> =
        sqlx::query_scalar("SELECT value FROM note_stage_record ORDER BY chunk_sequence,ordinal")
            .fetch_all(store.read_pool())
            .await
            .unwrap();
    let expected: Vec<String> = first
        .records
        .iter()
        .chain(&second.records)
        .map(Value::to_string)
        .collect();
    assert_eq!(uploaded, expected);
    let seal = seal_request(&store, &begin).await;
    note.content = "remote replacement".into();
    store.update_note(&note).await.unwrap();
    sqlx::query("CREATE TRIGGER injected_search_union BEFORE INSERT ON note_stage_search_range BEGIN SELECT RAISE(ABORT,'injected union failure'); END")
        .execute(store.write_pool()).await.unwrap();
    assert!(matches!(
        store.seal_note_stage("alice", &seal).await,
        Err(Error::Internal(_))
    ));
    for table in [
        "note_stage_search_input",
        "note_stage_search_range",
        "note_stage_view",
    ] {
        assert_eq!(count(&store, table).await, 0);
    }
    assert_eq!(count(&store, "note_stage_chunk").await, 2);
    sqlx::query("DROP TRIGGER injected_search_union")
        .execute(store.write_pool())
        .await
        .unwrap();
    let sealed = store.seal_note_stage("alice", &seal).await.unwrap();
    assert_eq!(sealed["payloadDigest"], seal.payload_digest);
    assert_eq!(store.seal_note_stage("alice", &seal).await.unwrap(), sealed);
    let ranges: Vec<(i64, i64)> =
        sqlx::query_as("SELECT start,end FROM note_stage_search_range ORDER BY start")
            .fetch_all(store.read_pool())
            .await
            .unwrap();
    assert_eq!(ranges, [(0, 4), (6, 8)]);
    assert_eq!(count(&store, "note_stage_search_input").await, 5);
    let after: Vec<String> =
        sqlx::query_scalar("SELECT value FROM note_stage_record ORDER BY chunk_sequence,ordinal")
            .fetch_all(store.read_pool())
            .await
            .unwrap();
    assert_eq!(after, uploaded);
    let retained: String = sqlx::query_scalar("SELECT manifest FROM note_stage")
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    assert_eq!(retained, serde_json::to_string(&seal.manifest).unwrap());
}

#[tokio::test]
async fn staged_search_seal_rejects_surrogate_endpoint_without_partial_index() {
    let (store, _tmp, _note) = setup("A😀BCDEF").await;
    let begin = begin_search(&store).await;
    let request = chunk(&begin, 0, None, &[range(0, 2, 2)]);
    store.append_note_stage("alice", &request).await.unwrap();
    let seal = seal_request(&store, &begin).await;
    assert!(matches!(
        store.seal_note_stage("alice", &seal).await,
        Err(Error::NoteMutation(NoteMutationError::Invalid))
    ));
    for table in [
        "note_stage_search_input",
        "note_stage_search_range",
        "note_stage_view",
    ] {
        assert_eq!(count(&store, table).await, 0);
    }
    assert_eq!(count(&store, "note_stage_chunk").await, 1);
    let phase: String = sqlx::query_scalar("SELECT phase FROM note_stage")
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    assert_eq!(phase, "staging");
}
