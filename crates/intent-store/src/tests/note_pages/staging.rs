use super::{page, setup};
use crate::Store;
use intent_core::{
    note_mutation::NoteMutationError,
    note_stage::{NoteStageAppend, NoteStageBegin},
    Error,
};
use serde_json::json;

async fn request(store: &Store) -> NoteStageBegin {
    let first = page(store, json!({"kind":"source","maxSourceBytes":128})).await;
    let mut value = first["scope"].clone();
    value["operationId"] = json!(uuid::Uuid::new_v4().to_string());
    value["expiresAt"] = json!(format!(
        "{}.000Z",
        &intent_core::iso_ms_from_now(60_000)[..19]
    ));
    value["headerDigest"] = json!("0".repeat(64));
    value["header"] = json!({"baseRevision":first["sourceRevision"],"editorSessionId":"test-session","localEditSequence":0,"liveGeneration":0,"selectionGeneration":0,"action":"mutate","output":"source","selection":"all"});
    let mut request: NoteStageBegin = serde_json::from_value(value).unwrap();
    request.header_digest = request.computed_digest().unwrap();
    request
}

async fn count(store: &Store, table: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
        .fetch_one(store.read_pool())
        .await
        .unwrap()
}

#[tokio::test]
async fn stage_begin_pins_without_copy_and_source_generation_survives_reopen() {
    let source = "A😀\r\n".repeat(2048);
    let (store, tmp, mut note) = setup(&source).await;
    let request = request(&store).await;
    let state = store.begin_note_stage("alice", &request).await.unwrap();
    assert_eq!(state["phase"], "staging");
    assert_eq!(count(&store, "note_stage_base_piece").await, 0);
    assert_eq!(count(&store, "note_stage_root").await, 1);
    assert_eq!(count(&store, "note_stage_stream").await, 5);
    let generation: String =
        sqlx::query_scalar("SELECT content_generation FROM note_page_head WHERE note_id='spec'")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    note.title = "metadata-only".into();
    store.update_note_metadata(&note).await.unwrap();
    assert_eq!(count(&store, "note_stage_base_piece").await, 0);
    let after: String =
        sqlx::query_scalar("SELECT content_generation FROM note_page_head WHERE note_id='spec'")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    assert_eq!(generation, after);
    note.content = "replaced".into();
    store.update_note(&note).await.unwrap();
    assert!(count(&store, "note_stage_base_piece").await > 1);
    assert_eq!(
        store.begin_note_stage("alice", &request).await.unwrap(),
        state
    );
    drop(store);
    let store = Store::open(&tmp.path).await.unwrap();
    let query = intent_core::note_mutation::NoteOperationStatusQuery {
        backend_id: request.backend_id.clone(),
        workspace_id: request.workspace_id.clone(),
        note_id: request.note_id.clone(),
        note_instance_id: request.note_instance_id.clone(),
        operation_id: request.operation_id.clone(),
        header_digest: Some(request.header_digest.clone()),
        payload_digest: None,
    };
    assert!(matches!(
        store.read_note_stage_base_piece("bob", &query, 0).await,
        Err(Error::NoteMutation(NoteMutationError::Invalid))
    ));
    let mut rebuilt = String::new();
    let mut offset = 0;
    while offset < u64::try_from(source.encode_utf16().count()).unwrap() {
        let (start, end, text) = store
            .read_note_stage_base_piece("alice", &query, offset)
            .await
            .unwrap();
        assert_eq!(start, offset);
        assert!(text.len() <= 4096);
        rebuilt.push_str(&text);
        offset = end;
    }
    assert_eq!(rebuilt, source);
    assert_eq!(
        store.begin_note_stage("alice", &request).await.unwrap(),
        state
    );
    assert_eq!(
        count(&store, "note_version").await,
        0,
        "begin and append do not create note versions"
    );
}

fn append(begin: &NoteStageBegin, text: &str) -> NoteStageAppend {
    let mut request:NoteStageAppend=serde_json::from_value(json!({"backendId":begin.backend_id,"workspaceId":begin.workspace_id,"noteId":begin.note_id,"noteInstanceId":begin.note_instance_id,"operationId":begin.operation_id,"headerDigest":begin.header_digest,"stream":"text","sequence":0,"previousDigest":null,"records":[{"kind":"text","id":"insert","offset":0,"text":text}],"chunkDigest":"0".repeat(64)})).unwrap();
    request.chunk_digest = request.computed_digest().unwrap();
    request
}

#[tokio::test]
async fn stage_append_replay_and_transaction_failure_preserve_accepted_prefix() {
    let (store, _tmp, _note) = setup("unchanged").await;
    let request = request(&store).await;
    store.begin_note_stage("alice", &request).await.unwrap();
    let first = append(&request, "😀\r\n");
    let ack = store.append_note_stage("alice", &first).await.unwrap();
    assert_eq!(ack["nextSequence"], 1);
    assert_eq!(store.append_note_stage("alice", &first).await.unwrap(), ack);
    assert_eq!(count(&store, "note_stage_chunk").await, 1);
    let mut next = first.clone();
    next.sequence = 1;
    next.previous_digest = Some(first.chunk_digest.clone());
    next.records = vec![json!({"kind":"text","id":"insert","offset":4,"text":"next"})];
    next.chunk_digest = next.computed_digest().unwrap();
    sqlx::query("CREATE TRIGGER injected_stage_tail_failure BEFORE UPDATE ON note_stage_stream BEGIN SELECT RAISE(ABORT,'injected stage tail'); END").execute(store.write_pool()).await.unwrap();
    assert!(matches!(
        store.append_note_stage("alice", &next).await,
        Err(Error::Internal(_))
    ));
    assert_eq!(count(&store, "note_stage_chunk").await, 1);
    assert_eq!(count(&store, "note_stage_text_piece").await, 1);
    let text: String = sqlx::query_scalar("SELECT text FROM note_stage_text_piece")
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    assert_eq!(text, "😀\r\n");
    sqlx::query("DROP TRIGGER injected_stage_tail_failure")
        .execute(store.write_pool())
        .await
        .unwrap();
    assert_eq!(
        store.append_note_stage("alice", &next).await.unwrap()["nextSequence"],
        2
    );
    assert_eq!(store.append_note_stage("alice", &first).await.unwrap(), ack);
    assert!(matches!(
        store.append_note_stage("bob", &next).await,
        Err(Error::NoteMutation(NoteMutationError::Invalid))
    ));
    assert_eq!(
        page(&store, json!({"kind":"source"})).await["text"],
        "unchanged"
    );
}

#[tokio::test]
async fn stage_begin_rejects_profile_retirement_and_method_identity_mismatch() {
    let (store, _tmp, _note) = setup("source").await;
    let mut request = request(&store).await;
    let original = crate::note_page_index::profile_revision();
    sqlx::query("UPDATE note_page_head SET profile_revision='obsolete'")
        .execute(store.write_pool())
        .await
        .unwrap();
    assert!(matches!(
        store.begin_note_stage("alice", &request).await,
        Err(Error::NoteMutation(NoteMutationError::Conflict))
    ));
    assert_eq!(count(&store, "note_operation").await, 0);
    sqlx::query("UPDATE note_page_head SET profile_revision=?")
        .bind(original)
        .execute(store.write_pool())
        .await
        .unwrap();
    let state = store.begin_note_stage("alice", &request).await.unwrap();
    let original = request.clone();
    request.header.editor_session_id = "different".into();
    request.header_digest = request.computed_digest().unwrap();
    assert!(matches!(
        store.begin_note_stage("alice", &request).await,
        Err(Error::NoteMutation(NoteMutationError::Mismatch))
    ));
    assert_eq!(count(&store, "note_operation").await, 1);
    let mut collision:intent_core::note_mutation::NoteApplySplices=serde_json::from_value(json!({"backendId":request.backend_id,"workspaceId":request.workspace_id,"noteId":request.note_id,"noteInstanceId":request.note_instance_id,"operationId":request.operation_id,"baseRevision":request.header.base_revision,"expiresAt":request.expires_at,"payloadDigest":"0".repeat(64),"splices":[{"start":0,"end":0,"text":"x"}]})).unwrap();
    collision.payload_digest = collision.computed_digest().unwrap();
    assert!(matches!(
        store
            .begin_note_mutation("alice", collision, &intent_core::now_iso())
            .await,
        Err(Error::NoteMutation(NoteMutationError::Mismatch))
    ));
    assert_eq!(state["phase"], "staging");
    request.operation_id = uuid::Uuid::new_v4().to_string();
    request.header_digest = request.computed_digest().unwrap();
    sqlx::query("INSERT INTO note_annotation_workspace_retirement(workspace_id) VALUES('pages')")
        .execute(store.write_pool())
        .await
        .unwrap();
    assert!(matches!(
        store.begin_note_stage("alice", &request).await,
        Err(Error::NotFound(_))
    ));
    assert_eq!(count(&store, "note_operation").await, 1);
    assert!(matches!(
        store.begin_note_stage("alice", &original).await,
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        store
            .append_note_stage("alice", &append(&original, "no"))
            .await,
        Err(Error::NotFound(_))
    ));
}

#[tokio::test]
async fn stage_copy_on_write_rolls_back_with_source_and_retains_committed_roots() {
    let (store, _tmp, mut note) = setup("base😀\r\n".repeat(1000).as_str()).await;
    let request = request(&store).await;
    store.begin_note_stage("alice", &request).await.unwrap();
    let generation: String =
        sqlx::query_scalar("SELECT content_generation FROM note_page_head WHERE note_id='spec'")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    sqlx::query("CREATE TRIGGER injected_stage_source_failure AFTER UPDATE OF content_generation ON note_page_head BEGIN SELECT RAISE(ABORT,'injected generation'); END").execute(store.write_pool()).await.unwrap();
    note.content = "failed replacement".into();
    assert!(store.update_note(&note).await.is_err());
    assert_eq!(count(&store, "note_stage_base_piece").await, 0);
    let after: String =
        sqlx::query_scalar("SELECT content_generation FROM note_page_head WHERE note_id='spec'")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    assert_eq!(generation, after);
    sqlx::query("DROP TRIGGER injected_stage_source_failure")
        .execute(store.write_pool())
        .await
        .unwrap();
    // Explicitly exercise the retention branch of the COW trigger, not a staged
    // commit claim: this test seeds committed status to model later retention.
    sqlx::query("UPDATE note_stage SET phase='committed'")
        .execute(store.write_pool())
        .await
        .unwrap();
    sqlx::query("UPDATE note_operation SET admission_expires=0")
        .execute(store.write_pool())
        .await
        .unwrap();
    store.update_note(&note).await.unwrap();
    assert!(count(&store, "note_stage_base_piece").await > 1);
}
