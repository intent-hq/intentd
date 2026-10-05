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

fn status_query(request: &NoteStageBegin) -> intent_core::note_mutation::NoteOperationStatusQuery {
    intent_core::note_mutation::NoteOperationStatusQuery {
        backend_id: request.backend_id.clone(),
        workspace_id: request.workspace_id.clone(),
        note_id: request.note_id.clone(),
        note_instance_id: request.note_instance_id.clone(),
        operation_id: request.operation_id.clone(),
        header_digest: Some(request.header_digest.clone()),
        payload_digest: None,
    }
}
fn cancel_request(request: &NoteStageBegin) -> intent_core::note_stage::NoteStageCancel {
    intent_core::note_stage::NoteStageCancel {
        backend_id: request.backend_id.clone(),
        workspace_id: request.workspace_id.clone(),
        note_id: request.note_id.clone(),
        note_instance_id: request.note_instance_id.clone(),
        operation_id: request.operation_id.clone(),
        header_digest: request.header_digest.clone(),
    }
}

#[tokio::test]
async fn stage_status_cancel_are_owned_durable_and_do_not_delete_unbounded_payloads() {
    let (store, tmp, _note) = setup("base😀").await;
    let request = request(&store).await;
    let state = store.begin_note_stage("alice", &request).await.unwrap();
    let query = status_query(&request);
    assert_eq!(
        store.note_stage_status("alice", &query).await.unwrap(),
        state
    );
    assert_eq!(
        store.note_stage_status("bob", &query).await.unwrap()["outcome"],
        "unknown"
    );
    assert_eq!(
        store
            .cancel_note_stage("bob", &cancel_request(&request))
            .await
            .unwrap()["outcome"],
        "unknown"
    );
    let chunk = append(&request, "uploaded");
    store.append_note_stage("alice", &chunk).await.unwrap();
    let mut wrong = query.clone();
    wrong.header_digest = Some("f".repeat(64));
    assert!(matches!(
        store.note_stage_status("alice", &wrong).await,
        Err(Error::NoteMutation(NoteMutationError::Mismatch))
    ));
    wrong = query.clone();
    wrong.payload_digest = Some("f".repeat(64));
    assert!(matches!(
        store.note_stage_status("alice", &wrong).await,
        Err(Error::NoteMutation(NoteMutationError::Mismatch))
    ));
    let cancelled = store
        .cancel_note_stage("alice", &cancel_request(&request))
        .await
        .unwrap();
    assert_eq!(cancelled["kind"], "noteStageState");
    assert_eq!(cancelled["phase"], "cancelled");
    assert_eq!(cancelled["streams"][0]["nextSequence"], 1);
    assert_eq!(
        count(&store, "note_stage_chunk").await,
        1,
        "cancel only closes admission; bounded background cleanup is separate"
    );
    assert_eq!(
        store.begin_note_stage("alice", &request).await.unwrap(),
        cancelled
    );
    assert_eq!(
        store
            .cancel_note_stage("alice", &cancel_request(&request))
            .await
            .unwrap(),
        cancelled
    );
    assert!(matches!(
        store.append_note_stage("alice", &chunk).await,
        Err(Error::NoteMutation(NoteMutationError::Invalid))
    ));
    assert!(matches!(
        store.read_note_stage_base_piece("alice", &query, 0).await,
        Err(Error::NoteMutation(NoteMutationError::Expired))
    ));
    drop(store);
    let reopened = Store::open(&tmp.path).await.unwrap();
    assert_eq!(
        reopened.note_stage_status("alice", &query).await.unwrap(),
        cancelled
    );
    assert_eq!(
        reopened.begin_note_stage("alice", &request).await.unwrap(),
        cancelled
    );
}

#[tokio::test]
async fn stage_cancel_failure_rolls_back_phase_and_committed_outcome_is_never_cancelled() {
    let (store, _tmp, _note) = setup("base").await;
    let request = request(&store).await;
    let state = store.begin_note_stage("alice", &request).await.unwrap();
    sqlx::query("CREATE TRIGGER injected_stage_cancel_failure BEFORE UPDATE ON note_operation BEGIN SELECT RAISE(ABORT,'cancel failure'); END").execute(store.write_pool()).await.unwrap();
    assert!(matches!(
        store
            .cancel_note_stage("alice", &cancel_request(&request))
            .await,
        Err(Error::Internal(_))
    ));
    assert_eq!(
        store
            .note_stage_status("alice", &status_query(&request))
            .await
            .unwrap(),
        state
    );
    let phase: String = sqlx::query_scalar("SELECT phase FROM note_stage")
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    assert_eq!(phase, "staging");
    sqlx::query("DROP TRIGGER injected_stage_cancel_failure")
        .execute(store.write_pool())
        .await
        .unwrap();
    // Exercise cancellation's committed branch with a seeded retained outcome;
    // this does not claim a staged commit was executed.
    let receipt = json!({"kind":"noteCommitReceipt","outcome":"committed","scope":request.scope(),"operationId":request.operation_id,"headerDigest":request.header_digest,"payloadDigest":"f".repeat(64)});
    sqlx::query("UPDATE note_operation SET outcome=?")
        .bind(receipt.to_string())
        .execute(store.write_pool())
        .await
        .unwrap();
    sqlx::query("UPDATE note_stage SET phase='committed',payload_digest=?")
        .bind("f".repeat(64))
        .execute(store.write_pool())
        .await
        .unwrap();
    assert_eq!(
        store
            .cancel_note_stage("alice", &cancel_request(&request))
            .await
            .unwrap(),
        receipt
    );
    assert_eq!(
        store
            .note_stage_status("alice", &status_query(&request))
            .await
            .unwrap(),
        receipt
    );
    let phase: String = sqlx::query_scalar("SELECT phase FROM note_stage")
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    assert_eq!(phase, "committed");
}

async fn seal_request(
    store: &Store,
    begin: &NoteStageBegin,
) -> intent_core::note_stage::NoteStageSeal {
    use intent_core::note_stage::{NoteStageManifestEntry, NOTE_STAGE_STREAMS};
    let key: String =
        sqlx::query_scalar("SELECT operation_key FROM note_operation WHERE operation_id=?")
            .bind(&begin.operation_id)
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    let mut manifest = Vec::new();
    for stream in NOTE_STAGE_STREAMS {
        let name = serde_json::to_value(stream).unwrap();
        let (chunks,records,last_digest):(i64,i64,Option<String>)=sqlx::query_as("SELECT next_sequence,records,last_digest FROM note_stage_stream WHERE operation_key=? AND stream=?")
            .bind(&key).bind(name.as_str().unwrap()).fetch_one(store.read_pool()).await.unwrap();
        manifest.push(NoteStageManifestEntry {
            stream,
            chunks: u64::try_from(chunks).unwrap(),
            records: u64::try_from(records).unwrap(),
            last_digest,
        });
    }
    let mut seal = intent_core::note_stage::NoteStageSeal {
        backend_id: begin.backend_id.clone(),
        workspace_id: begin.workspace_id.clone(),
        note_id: begin.note_id.clone(),
        note_instance_id: begin.note_instance_id.clone(),
        operation_id: begin.operation_id.clone(),
        header_digest: begin.header_digest.clone(),
        manifest,
        payload_digest: String::new(),
    };
    seal.payload_digest = seal.computed_digest().unwrap();
    seal
}

#[tokio::test]
async fn stage_seal_is_atomic_replayable_after_remote_write_and_reopen() {
    let (store, tmp, mut note) = setup("base😀\r\n").await;
    let begin = request(&store).await;
    store.begin_note_stage("alice", &begin).await.unwrap();
    store
        .append_note_stage("alice", &append(&begin, "unused text"))
        .await
        .unwrap();
    let seal = seal_request(&store, &begin).await;
    note.content = "remote".into();
    store.update_note(&note).await.unwrap();
    let state = store.seal_note_stage("alice", &seal).await.unwrap();
    assert_eq!(state["phase"], "sealed");
    assert_eq!(state["viewLength"], 8);
    assert_eq!(state["payloadDigest"], seal.payload_digest);
    assert_eq!(state["expiresAt"], begin.expires_at);
    assert_eq!(state["streams"].as_array().unwrap().len(), 5);
    assert_eq!(count(&store, "note_stage_view").await, 1);
    assert_eq!(count(&store, "note_stage_view_piece").await, 1);
    assert_eq!(count(&store, "note_version").await, 0);
    assert!(matches!(
        store.seal_note_stage("bob", &seal).await,
        Err(Error::NoteMutation(NoteMutationError::Invalid))
    ));
    let mut changed = seal.clone();
    changed.manifest[0].records = 0;
    changed.payload_digest = changed.computed_digest().unwrap();
    assert!(matches!(
        store.seal_note_stage("alice", &changed).await,
        Err(Error::NoteMutation(NoteMutationError::Mismatch))
    ));
    assert!(matches!(
        store
            .append_note_stage("alice", &append(&begin, "unused text"))
            .await,
        Err(Error::NoteMutation(NoteMutationError::Invalid))
    ));
    drop(store);
    let store = Store::open(&tmp.path).await.unwrap();
    assert_eq!(store.seal_note_stage("alice", &seal).await.unwrap(), state);
    assert_eq!(count(&store, "note_stage_view").await, 1);
    assert_eq!(
        store
            .get_note(&note.workspace_id, &note.id)
            .await
            .unwrap()
            .content,
        "remote"
    );
    store
        .cancel_note_stage("alice", &cancel_request(&begin))
        .await
        .unwrap();
    assert!(matches!(
        store.seal_note_stage("alice", &seal).await,
        Err(Error::NoteMutation(NoteMutationError::Expired))
    ));
}

#[tokio::test]
async fn stage_seal_failed_publication_rolls_back_views_cache_and_phase() {
    let (store, _tmp, _note) = setup("source").await;
    let begin = request(&store).await;
    store.begin_note_stage("alice", &begin).await.unwrap();
    store
        .append_note_stage("alice", &append(&begin, "x"))
        .await
        .unwrap();
    let seal = seal_request(&store, &begin).await;
    sqlx::query("CREATE TRIGGER injected_seal_publication BEFORE UPDATE ON note_operation WHEN json_extract(new.outcome,'$.phase')='sealed' BEGIN SELECT RAISE(ABORT,'seal publication'); END").execute(store.write_pool()).await.unwrap();
    assert!(matches!(
        store.seal_note_stage("alice", &seal).await,
        Err(Error::Internal(_))
    ));
    assert_eq!(count(&store, "note_stage_view").await, 0);
    assert_eq!(count(&store, "note_stage_view_piece").await, 0);
    assert_eq!(count(&store, "note_stage_validation").await, 0);
    let digest: Option<String> = sqlx::query_scalar("SELECT sha256 FROM note_stage_text")
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    assert!(digest.is_none());
    assert_eq!(
        store
            .note_stage_status("alice", &status_query(&begin))
            .await
            .unwrap()["phase"],
        "staging"
    );
    sqlx::query("DROP TRIGGER injected_seal_publication")
        .execute(store.write_pool())
        .await
        .unwrap();
    assert_eq!(
        store.seal_note_stage("alice", &seal).await.unwrap()["phase"],
        "sealed"
    );
}

#[tokio::test]
async fn stage_seal_incomplete_reference_can_be_completed_without_partial_view() {
    use sha2::{Digest, Sha256};
    let (store, _tmp, _note) = setup("base").await;
    let mut begin = request(&store).await;
    begin.header.local_edit_sequence = 1;
    begin.header_digest = begin.computed_digest().unwrap();
    store.begin_note_stage("alice", &begin).await.unwrap();
    let mut dirty = append(&begin, "");
    dirty.stream = intent_core::note_stage::NoteStageStream::Dirty;
    dirty.records = vec![
        json!({"kind":"splice","localSequence":1,"ordinal":0,"start":1,"end":3,"replacement":{"textId":"insert","length":2,"utf8Bytes":4,"sha256":format!("{:x}",Sha256::digest("😀".as_bytes()))}}),
    ];
    dirty.chunk_digest = dirty.computed_digest().unwrap();
    store.append_note_stage("alice", &dirty).await.unwrap();
    let missing = seal_request(&store, &begin).await;
    assert!(matches!(
        store.seal_note_stage("alice", &missing).await,
        Err(Error::NoteMutation(NoteMutationError::Invalid))
    ));
    assert_eq!(count(&store, "note_stage_view").await, 0);
    store
        .append_note_stage("alice", &append(&begin, "😀"))
        .await
        .unwrap();
    let complete = seal_request(&store, &begin).await;
    assert_eq!(
        store.seal_note_stage("alice", &complete).await.unwrap()["viewLength"],
        4
    );
    let retained: Vec<(i64, Option<i64>, Option<String>)> = sqlx::query_as(
        "SELECT generation,input_generation,history_group FROM note_stage_view ORDER BY generation",
    )
    .fetch_all(store.read_pool())
    .await
    .unwrap();
    assert_eq!(retained.len(), 2);
    assert_eq!(retained[1].1, Some(0));
    assert!(retained[1].2.is_some());
}

fn source_request(begin: &NoteStageBegin) -> intent_core::note_stage_read::NoteStageRead {
    serde_json::from_value(json!({"backendId":begin.backend_id,"workspaceId":begin.workspace_id,"noteId":begin.note_id,"noteInstanceId":begin.note_instance_id,"operationId":begin.operation_id,"headerDigest":begin.header_digest,"kind":"source","maxSourceBytes":8192,"maxWireBytes":4096})).unwrap()
}

#[tokio::test]
async fn staged_source_output_is_exact_bounded_immutable_and_cursor_owned() {
    use intent_core::note_page::NotePageError;
    let source = "A😀\r\n\"\\\u{1}".repeat(2048);
    let (store, tmp, mut note) = setup(&source).await;
    let begin = request(&store).await;
    store.begin_note_stage("alice", &begin).await.unwrap();
    let seal = seal_request(&store, &begin).await;
    store.seal_note_stage("alice", &seal).await.unwrap();
    let mut query = source_request(&begin);
    let id = json!("\u{1}".repeat(64));
    let first = store
        .read_note_stage_source("alice", &query, &id)
        .await
        .unwrap();
    let cursor = first["nextCursor"].as_str().unwrap().to_owned();
    assert!(!cursor.is_empty());
    for change in 0..5 {
        let mut foreign = query.clone();
        foreign.cursor = Some(cursor.clone());
        match change {
            0 => foreign.max_source_bytes = Some(4096),
            1 => foreign.max_wire_bytes = Some(8192),
            2 => foreign.max_items = Some(2),
            3 => foreign.operation_id = uuid::Uuid::new_v4().to_string(),
            _ => foreign.header_digest = "f".repeat(64),
        }
        assert!(matches!(
            store.read_note_stage_source("alice", &foreign, &id).await,
            Err(Error::NotePage(NotePageError::CursorInvalid))
        ));
    }
    assert!(matches!(
        store.read_note_stage_source("bob", &query, &id).await,
        Err(Error::NotePage(NotePageError::CursorInvalid))
    ));
    note.content = "remote replacement".into();
    store.update_note(&note).await.unwrap();
    drop(store);
    let store = Store::open(&tmp.path).await.unwrap();
    let mut text = String::new();
    let mut offset = 0_u64;
    loop {
        let value = store
            .read_note_stage_source("alice", &query, &id)
            .await
            .unwrap();
        assert!(
            json!({"jsonrpc":"2.0","id":id,"result":value})
                .to_string()
                .len()
                <= 4096
        );
        assert_eq!(
            value["sourceLength"],
            u64::try_from(source.encode_utf16().count()).unwrap()
        );
        assert_eq!(value["headerDigest"], begin.header_digest);
        assert_eq!(value["payloadDigest"], seal.payload_digest);
        assert_eq!(value["expiresAt"], begin.expires_at);
        let items = value["items"].as_array().unwrap();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["offset"], offset);
        let part = items[0]["text"].as_str().unwrap();
        assert!(!part.is_empty());
        assert!(part.len() <= 8192);
        offset += u64::try_from(part.encode_utf16().count()).unwrap();
        text.push_str(part);
        query.cursor = value["nextCursor"].as_str().map(str::to_owned);
        if query.cursor.is_none() {
            break;
        }
    }
    assert_eq!(text, source);
    let cancel = serde_json::from_value(json!({"backendId":begin.backend_id,"workspaceId":begin.workspace_id,"noteId":begin.note_id,"noteInstanceId":begin.note_instance_id,"operationId":begin.operation_id,"headerDigest":begin.header_digest})).unwrap();
    store.cancel_note_stage("alice", &cancel).await.unwrap();
    query.cursor = Some(cursor);
    assert!(matches!(
        store.read_note_stage_source("alice", &query, &id).await,
        Err(Error::NotePage(NotePageError::Expired))
    ));
}

#[tokio::test]
async fn staged_source_output_empty_expired_and_missing_pieces_do_not_fake_completion() {
    use intent_core::note_page::NotePageError;
    for source in ["", "not empty"] {
        let (store, _tmp, _note) = setup(source).await;
        let begin = request(&store).await;
        store.begin_note_stage("alice", &begin).await.unwrap();
        let query = source_request(&begin);
        assert!(store
            .read_note_stage_source("alice", &query, &json!(1))
            .await
            .is_err());
        let seal = seal_request(&store, &begin).await;
        store.seal_note_stage("alice", &seal).await.unwrap();
        let page = store
            .read_note_stage_source("alice", &query, &json!(1))
            .await
            .unwrap();
        assert!(page["nextCursor"].is_null());
        assert_eq!(
            page["items"].as_array().unwrap().is_empty(),
            source.is_empty()
        );
        sqlx::query("DELETE FROM note_stage_view_piece")
            .execute(store.write_pool())
            .await
            .unwrap();
        if !source.is_empty() {
            assert!(store
                .read_note_stage_source("alice", &query, &json!(1))
                .await
                .is_err());
        }
        sqlx::query("UPDATE note_operation SET outcome=json_set(outcome,'$.expiresAt','2000-01-01T00:00:00.000Z')").execute(store.write_pool()).await.unwrap();
        assert!(matches!(
            store
                .read_note_stage_source("alice", &query, &json!(1))
                .await,
            Err(Error::NotePage(NotePageError::Expired))
        ));
    }
}

#[tokio::test]
async fn staged_source_output_requires_captured_output_before_hydration() {
    use intent_core::{
        note_page::NotePageError,
        note_stage::{NoteStageOutput, NoteStageSearch, NoteStageSearchMode},
    };
    for mode in [NoteStageOutput::SelectionMarkdown, NoteStageOutput::Search] {
        let (store, _tmp, _note) = setup("must not fall back to whole source").await;
        let mut begin = request(&store).await;
        begin.header.output = mode;
        if mode == NoteStageOutput::Search {
            begin.header.query = Some(NoteStageSearch {
                text: "source".into(),
                case_sensitive: false,
                mode: NoteStageSearchMode::Source,
            });
        }
        begin.header_digest = begin.computed_digest().unwrap();
        store.begin_note_stage("alice", &begin).await.unwrap();
        let seal = seal_request(&store, &begin).await;
        store.seal_note_stage("alice", &seal).await.unwrap();
        let query = source_request(&begin);
        assert!(matches!(
            store
                .read_note_stage_source("alice", &query, &json!(1))
                .await,
            Err(Error::NotePage(NotePageError::CursorInvalid))
        ));
        // Corrupt backing pieces to distinguish pre-hydration selector rejection
        // from reading source and only then deciding which output was requested.
        sqlx::query("DELETE FROM note_stage_view_piece")
            .execute(store.write_pool())
            .await
            .unwrap();
        assert!(matches!(
            store
                .read_note_stage_source("alice", &query, &json!(1))
                .await,
            Err(Error::NotePage(NotePageError::CursorInvalid))
        ));
    }
}

#[path = "staged_commit.rs"]
mod commit;

#[path = "staged_commit_groups.rs"]
mod commit_groups;
mod search_ranges;
