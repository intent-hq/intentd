use super::{append, count, page, request, seal_request, setup, status_query};
use crate::{NoteMutationWrite, StageCommitAdmission, Store};
use intent_core::{
    note_mutation::{apply_note_splices, NoteMutationError, NoteSplice},
    note_stage::{NoteStageBegin, NoteStageCommit, NoteStageStream},
    Error, NoteVersionAuthor,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

fn author() -> NoteVersionAuthor {
    NoteVersionAuthor {
        id: "alice".into(),
        name: "Alice".into(),
        author_type: "user".into(),
    }
}

async fn captured(store: &Store, groups: bool) -> (NoteStageBegin, NoteStageCommit) {
    let mut begin = request(store).await;
    begin.header.local_edit_sequence = if groups { 2 } else { 0 };
    begin.header_digest = begin.computed_digest().unwrap();
    store.begin_note_stage("alice", &begin).await.unwrap();
    if groups {
        let mut chunk = append(&begin, "");
        chunk.records = [("first", "B"), ("second", "C"), ("last", "!")]
            .into_iter()
            .map(|(id, text)| json!({"kind":"text","id":id,"offset":0,"text":text}))
            .collect();
        chunk.chunk_digest = chunk.computed_digest().unwrap();
        store.append_note_stage("alice", &chunk).await.unwrap();
        let reference = |id: &str, text: &str| json!({"textId":id,"length":text.encode_utf16().count(),"utf8Bytes":text.len(),"sha256":format!("{:x}",Sha256::digest(text.as_bytes()))});
        let mut dirty = append(&begin, "");
        dirty.stream = NoteStageStream::Dirty;
        dirty.records = vec![
            json!({"kind":"splice","localSequence":1,"ordinal":0,"start":1,"end":2,"replacement":reference("first","B")}),
            json!({"kind":"splice","localSequence":2,"ordinal":0,"start":4,"end":5,"replacement":reference("second","C")}),
        ];
        dirty.chunk_digest = dirty.computed_digest().unwrap();
        store.append_note_stage("alice", &dirty).await.unwrap();
        let mut mutation = append(&begin, "");
        mutation.stream = NoteStageStream::Mutation;
        mutation.records = vec![
            json!({"kind":"splice","ordinal":0,"start":6,"end":6,"replacement":reference("last","!")}),
        ];
        mutation.chunk_digest = mutation.computed_digest().unwrap();
        store.append_note_stage("alice", &mutation).await.unwrap();
    }
    let seal = seal_request(store, &begin).await;
    store.seal_note_stage("alice", &seal).await.unwrap();
    (
        begin,
        NoteStageCommit {
            backend_id: seal.backend_id,
            workspace_id: seal.workspace_id,
            note_id: seal.note_id,
            note_instance_id: seal.note_instance_id,
            operation_id: seal.operation_id,
            header_digest: seal.header_digest,
            payload_digest: seal.payload_digest,
        },
    )
}

async fn writer(store: &Store, request: &NoteStageCommit) -> Box<NoteMutationWrite> {
    let StageCommitAdmission::Reserved(reserved) = store
        .reserve_note_stage_commit("alice", request)
        .await
        .unwrap()
    else {
        panic!("new write expected")
    };
    reserved.into_mutation(|_| Ok(())).await.unwrap()
}

fn byte_at(text: &str, at: u64) -> usize {
    let mut offset = 0;
    for (byte, c) in text.char_indices() {
        if offset == at {
            return byte;
        }
        offset += c.len_utf16() as u64;
    }
    assert_eq!(offset, at);
    text.len()
}

async fn inverse_text(store: &Store, operation: &str, reference: &Value) -> String {
    let (phase, start, end): (String, i64, i64) = sqlx::query_as(
        "SELECT phase,start,end FROM note_operation_text WHERE operation_key=? AND text_id=?",
    )
    .bind(operation)
    .bind(reference["textId"].as_str().unwrap())
    .fetch_one(store.read_pool())
    .await
    .unwrap();
    let mut at = start;
    let mut text = String::new();
    while at < end {
        let (a,b,piece):(i64,i64,String) = sqlx::query_as("SELECT start,end,text FROM note_operation_source WHERE operation_key=? AND phase=? AND start<=? AND end>? ORDER BY start DESC LIMIT 1")
            .bind(operation).bind(&phase).bind(at).bind(at).fetch_one(store.read_pool()).await.unwrap();
        let next = end.min(b);
        text.push_str(
            &piece[byte_at(&piece, u64::try_from(at - a).unwrap())
                ..byte_at(&piece, u64::try_from(next - a).unwrap())],
        );
        at = next;
    }
    assert_eq!(
        reference["sha256"],
        format!("{:x}", Sha256::digest(text.as_bytes()))
    );
    text
}

#[tokio::test]
async fn staged_commit_preserves_groups_and_canonical_outside_range_then_replays() {
    let (store, _tmp, mut note) = setup("ab😀cd").await;
    let (begin, request) = captured(&store, true).await;
    let mut write = writer(&store, &request).await;
    assert_eq!(write.source(), "aB😀Cd!");
    write
        .apply_recorded_phase(
            "anchor-repair",
            &[NoteSplice {
                start: 0,
                end: 1,
                text: "A".into(),
            }],
        )
        .await
        .unwrap();
    write
        .persist_source(&author(), &intent_core::now_iso())
        .await
        .unwrap();
    let receipt = write.commit().await.unwrap();
    assert_eq!(receipt["headerDigest"], request.header_digest);
    assert!(receipt["viewId"].is_string());
    assert_eq!(
        page(&store, json!({"kind":"source"})).await["text"],
        "AB😀Cd!"
    );
    let operation: String =
        sqlx::query_scalar("SELECT operation_key FROM note_operation WHERE operation_id=?")
            .bind(&request.operation_id)
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    let rows:Vec<String>=sqlx::query_scalar("SELECT value FROM note_operation_item WHERE operation_key=? AND kind='inverse' ORDER BY sequence")
        .bind(&operation).fetch_all(store.read_pool()).await.unwrap();
    let rows: Vec<Value> = rows
        .iter()
        .map(|s| serde_json::from_str(s).unwrap())
        .collect();
    let mut source = String::from("AB😀Cd!");
    let mut state = receipt["afterRevision"].clone();
    let mut groups = Vec::new();
    let mut start = 0;
    while start < rows.len() {
        let group = rows[start]["historyGroup"].clone();
        assert!(!groups.contains(&group));
        groups.push(group.clone());
        let end = (start + 1..rows.len())
            .find(|&i| rows[i]["historyGroup"] != group)
            .unwrap_or(rows.len());
        let output = rows[start]["outputState"].clone();
        let mut edits = Vec::new();
        for row in &rows[start..end] {
            assert_eq!(row["inputState"], state);
            assert_eq!(row["outputState"], output);
            edits.push(NoteSplice {
                start: row["start"].as_u64().unwrap(),
                end: row["end"].as_u64().unwrap(),
                text: inverse_text(&store, &operation, &row["replacement"]).await,
            });
        }
        source = apply_note_splices(&source, &edits).unwrap().source;
        assert_eq!(source, ["aB😀Cd", "aB😀cd", "ab😀cd"][groups.len() - 1]);
        state = output;
        start = end;
    }
    assert_eq!(groups.len(), 3);
    assert_eq!(state, receipt["beforeRevision"]);
    let versions = count(&store, "note_version").await;
    note.content = "remote".into();
    store.update_note(&note).await.unwrap();
    let StageCommitAdmission::Replay(replayed) = store
        .reserve_note_stage_commit("alice", &request)
        .await
        .unwrap()
    else {
        panic!("receipt expected")
    };
    assert_eq!(replayed, receipt);
    assert_eq!(count(&store, "note_version").await, versions);
    assert_eq!(
        store
            .note_stage_status("alice", &status_query(&begin))
            .await
            .unwrap(),
        receipt
    );
}

#[tokio::test]
async fn staged_commit_conflict_cancel_and_publication_failure_never_mutate() {
    let (store, _tmp, mut note) = setup("ab😀cd").await;
    let (begin, request) = captured(&store, true).await;
    let original = page(&store, json!({"kind":"source"})).await;
    sqlx::query("CREATE TRIGGER fail_stage_commit BEFORE UPDATE OF phase ON note_stage WHEN new.phase='committed' BEGIN SELECT RAISE(ABORT,'commit publication'); END").execute(store.write_pool()).await.unwrap();
    let mut write = writer(&store, &request).await;
    write
        .persist_source(&author(), &intent_core::now_iso())
        .await
        .unwrap();
    assert!(matches!(write.commit().await, Err(Error::Internal(_))));
    let after = page(&store, json!({"kind":"source"})).await;
    assert_eq!(after["sourceRevision"], original["sourceRevision"]);
    assert_eq!(after["text"], original["text"]);
    assert_eq!(count(&store, "note_version").await, 0);
    assert_eq!(count(&store, "note_operation_item").await, 0);
    assert_eq!(
        store
            .note_stage_status("alice", &status_query(&begin))
            .await
            .unwrap()["phase"],
        "sealed"
    );
    note.content = "remote".into();
    store.update_note(&note).await.unwrap();
    assert!(matches!(
        store.reserve_note_stage_commit("alice", &request).await,
        Err(Error::NoteMutation(NoteMutationError::Conflict))
    ));
    let cancel = intent_core::note_stage::NoteStageCancel {
        backend_id: request.backend_id.clone(),
        workspace_id: request.workspace_id.clone(),
        note_id: request.note_id.clone(),
        note_instance_id: request.note_instance_id.clone(),
        operation_id: request.operation_id.clone(),
        header_digest: request.header_digest.clone(),
    };
    store.cancel_note_stage("alice", &cancel).await.unwrap();
    assert!(matches!(
        store.reserve_note_stage_commit("alice", &request).await,
        Err(Error::NoteMutation(NoteMutationError::Expired))
    ));
}

#[tokio::test]
async fn staged_commit_empty_capture_owns_empty_or_canonical_operation_inverse() {
    for changed in [false, true] {
        let (store, _tmp, _note) = setup("ab😀cd").await;
        let (_, request) = captured(&store, false).await;
        let mut write = writer(&store, &request).await;
        if changed {
            write
                .apply_recorded_phase(
                    "anchor-repair",
                    &[NoteSplice {
                        start: 0,
                        end: 1,
                        text: "A".into(),
                    }],
                )
                .await
                .unwrap();
        }
        write
            .persist_source(&author(), &intent_core::now_iso())
            .await
            .unwrap();
        let receipt = write.commit().await.unwrap();
        assert!(receipt["inverseRef"].is_string());
        let n: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM note_operation_item WHERE kind='inverse'")
                .fetch_one(store.read_pool())
                .await
                .unwrap();
        assert_eq!(n, i64::from(changed));
    }
}

#[tokio::test]
async fn staged_commit_original_deadline_rejects_after_all_publication_writes() {
    let (store, _tmp, _note) = setup("ab😀cd").await;
    let (begin, request) = captured(&store, true).await;
    let original = page(&store, json!({"kind":"source"})).await;
    let mut write = writer(&store, &request).await;
    write
        .persist_source(&author(), &intent_core::now_iso())
        .await
        .unwrap();
    let deadline = intent_core::parse_iso(&begin.expires_at).unwrap();
    let result = crate::note_mutation_repo::STAGED_COMMIT_NOW
        .scope(deadline.unix_timestamp_nanos(), write.commit())
        .await;
    assert!(matches!(
        result,
        Err(Error::NoteMutation(NoteMutationError::Expired))
    ));
    let after = page(&store, json!({"kind":"source"})).await;
    assert_eq!(after["sourceRevision"], original["sourceRevision"]);
    assert_eq!(after["text"], original["text"]);
    assert_eq!(count(&store, "note_version").await, 0);
    assert_eq!(count(&store, "note_operation_item").await, 0);
    assert_eq!(
        store
            .note_stage_status("alice", &status_query(&begin))
            .await
            .unwrap()["phase"],
        "sealed"
    );
    let mut retry = writer(&store, &request).await;
    retry
        .persist_source(&author(), &intent_core::now_iso())
        .await
        .unwrap();
    assert_eq!(retry.commit().await.unwrap()["kind"], "noteCommitReceipt");
}

#[tokio::test]
async fn staged_commit_receipt_reads_bind_header_and_survive_staging_reclamation() {
    let (store, _tmp, _note) = setup("ab😀cd").await;
    let (_, request) = captured(&store, true).await;
    let mut write = writer(&store, &request).await;
    write
        .persist_source(&author(), &intent_core::now_iso())
        .await
        .unwrap();
    let receipt = write.commit().await.unwrap();
    let raw = json!({"backendId":request.backend_id,"workspaceId":request.workspace_id,"noteId":request.note_id,"noteInstanceId":request.note_instance_id,"operationId":request.operation_id,"headerDigest":request.header_digest,"kind":"inverse","ref":receipt["inverseRef"],"maxItems":1,"maxWireBytes":4096});
    let query =
        serde_json::from_value::<intent_core::note_receipt_detail::NoteOperationReceiptRead>(
            raw.clone(),
        )
        .unwrap()
        .query()
        .unwrap();
    let first = store
        .read_note_receipt_detail("alice", &query, &json!(1))
        .await
        .unwrap();
    assert_eq!(first["headerDigest"], receipt["headerDigest"]);
    assert_eq!(first["viewId"], receipt["viewId"]);
    assert!(first.get("beforeRevision").is_none());
    assert!(first.get("afterRevision").is_none());
    assert!(first["nextCursor"].is_string());
    let mut wrong = query.clone();
    wrong.header_digest = Some("0".repeat(64));
    assert!(matches!(
        store
            .read_note_receipt_detail("alice", &wrong, &json!(1))
            .await,
        Err(Error::NoteMutation(NoteMutationError::Mismatch))
    ));
    wrong.header_digest = None;
    wrong.payload_digest = Some(request.payload_digest.clone());
    assert!(matches!(
        store
            .read_note_receipt_detail("alice", &wrong, &json!(1))
            .await,
        Err(Error::NoteMutation(NoteMutationError::Mismatch))
    ));
    assert!(matches!(
        store
            .read_note_receipt_detail("other", &query, &json!(1))
            .await,
        Err(Error::NotePage(
            intent_core::note_page::NotePageError::CursorInvalid
        ))
    ));
    // Receipt data must not rely on the expiring staged view/pin/upload rows.
    sqlx::query("DELETE FROM note_stage")
        .execute(store.write_pool())
        .await
        .unwrap();
    let mut next = query.clone();
    next.cursor = Some(first["nextCursor"].as_str().unwrap().into());
    let second = store
        .read_note_receipt_detail("alice", &next, &json!(1))
        .await
        .unwrap();
    assert_eq!(second["headerDigest"], receipt["headerDigest"]);
    let replacement = second["items"][0]["replacement"].clone();
    let mut text = raw;
    text["kind"] = json!("inverseText");
    text["textId"] = replacement["textId"].clone();
    let text =
        serde_json::from_value::<intent_core::note_receipt_detail::NoteOperationReceiptRead>(text)
            .unwrap()
            .query()
            .unwrap();
    let fragment = store
        .read_note_receipt_detail("alice", &text, &json!(2))
        .await
        .unwrap();
    let restored: String = fragment["items"]
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["text"].as_str().unwrap())
        .collect();
    assert_eq!(
        restored.encode_utf16().count() as u64,
        replacement["length"].as_u64().unwrap()
    );
    assert_eq!(
        format!("{:x}", Sha256::digest(restored.as_bytes())),
        replacement["sha256"]
    );
    assert_eq!(fragment["expiresAt"], receipt["receiptExpiresAt"]);
}
