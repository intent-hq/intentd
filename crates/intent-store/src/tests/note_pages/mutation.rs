use super::{page, setup};
use crate::{NoteMutationAdmission, NoteMutationWrite, Store};
use intent_core::{
    note_mutation::{NoteApplySplices, NoteMutationError, NoteSplice},
    Error, NoteId, NoteVersionAuthor, WorkspaceId,
};
use serde_json::{json, Value};

fn author() -> NoteVersionAuthor {
    NoteVersionAuthor {
        id: "alice".into(),
        name: "Alice".into(),
        author_type: "user".into(),
    }
}

async fn request(store: &Store, edits: Vec<NoteSplice>) -> NoteApplySplices {
    let head = page(store, json!({"kind":"source","maxSourceBytes":128})).await;
    let scope = &head["scope"];
    let mut request = NoteApplySplices {
        backend_id: scope["backendId"].as_str().unwrap().into(),
        workspace_id: "pages".into(),
        note_id: "spec".into(),
        note_instance_id: scope["noteInstanceId"].as_str().unwrap().into(),
        base_revision: head["sourceRevision"].as_str().unwrap().into(),
        operation_id: uuid::Uuid::new_v4().to_string(),
        expires_at: format!("{}.000Z", &intent_core::iso_ms_from_now(60_000)[..19]),
        payload_digest: String::new(),
        splices: edits,
    };
    request.payload_digest = request.computed_digest().unwrap();
    request
}

fn edit(start: u64, end: u64, text: &str) -> NoteSplice {
    NoteSplice {
        start,
        end,
        text: text.into(),
    }
}

async fn begin(store: &Store, request: NoteApplySplices) -> Box<NoteMutationWrite> {
    match store
        .begin_note_mutation("alice", request, &intent_core::now_iso())
        .await
        .unwrap()
    {
        NoteMutationAdmission::Write(write) => write,
        NoteMutationAdmission::Replay(_) => panic!("expected new write"),
    }
}

async fn commit(store: &Store, request: NoteApplySplices) -> Value {
    let mut write = begin(store, request).await;
    write
        .persist_source(&author(), &intent_core::now_iso())
        .await
        .unwrap();
    write.commit().await.unwrap()
}

#[tokio::test]
async fn note_mutation_receipt_replays_after_restart_unrelated_write_and_expiry() {
    let (store, tmp, _) = setup("same😀\r\nsame").await;
    let request = request(&store, vec![edit(8, 12, "second")]).await;
    let receipt = commit(&store, request.clone()).await;
    assert_eq!(receipt["kind"], "noteCommitReceipt");
    assert!(receipt.to_string().len() < 3584);
    let mut note = store
        .get_note(&WorkspaceId("pages".into()), &NoteId("spec".into()))
        .await
        .unwrap();
    assert_eq!(note.content, "same😀\r\nsecond");
    note.title = "later metadata".into();
    store.update_note_metadata(&note).await.unwrap();
    drop(store);
    let reopened = Store::open(&tmp.path).await.unwrap();
    let replay = reopened
        .begin_note_mutation(
            "alice",
            request.clone(),
            &intent_core::iso_from_unix_secs(request.deadline().unwrap().unix_timestamp() + 86_400),
        )
        .await
        .unwrap();
    assert!(matches!(replay, NoteMutationAdmission::Replay(value) if value == receipt));
    assert_eq!(
        reopened
            .note_mutation_status(
                "alice",
                &request.scope(),
                &request.operation_id,
                &request.payload_digest
            )
            .await
            .unwrap(),
        receipt
    );
    let versions: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM note_version WHERE note_id='spec'")
            .fetch_one(reopened.read_pool())
            .await
            .unwrap();
    assert_eq!(versions, 1);
    let foreign = reopened
        .note_mutation_status(
            "bob",
            &request.scope(),
            &request.operation_id,
            &request.payload_digest,
        )
        .await
        .unwrap();
    assert_eq!(foreign["outcome"], "unknown");
}

#[tokio::test]
async fn note_mutation_duplicate_identity_and_stale_base_never_repeat_a_write() {
    let (store, _tmp, _) = setup("abcd").await;
    let original = request(&store, vec![edit(1, 2, "X")]).await;
    commit(&store, original.clone()).await;
    let mut changed = original.clone();
    changed.splices[0].text = "Y".into();
    changed.payload_digest = changed.computed_digest().unwrap();
    assert!(matches!(
        store
            .begin_note_mutation("alice", changed, &intent_core::now_iso())
            .await,
        Err(Error::NoteMutation(NoteMutationError::Mismatch))
    ));
    let mut stale = original;
    stale.operation_id = uuid::Uuid::new_v4().to_string();
    stale.payload_digest = stale.computed_digest().unwrap();
    assert!(matches!(
        store
            .begin_note_mutation("alice", stale, &intent_core::now_iso())
            .await,
        Err(Error::NoteMutation(NoteMutationError::Conflict))
    ));
    assert_eq!(page(&store, json!({"kind":"source"})).await["text"], "aXcd");
}

#[tokio::test]
async fn note_mutation_drop_and_receipt_failure_roll_back_source_history_and_indexes() {
    let (store, _tmp, _) = setup("original😀").await;
    let original = page(&store, json!({"kind":"source"})).await;
    let request = request(&store, vec![edit(0, 8, "replaced")]).await;
    let mut write = begin(&store, request.clone()).await;
    write
        .persist_source(&author(), &intent_core::now_iso())
        .await
        .unwrap();
    drop(write);
    assert_eq!(
        page(&store, json!({"kind":"source"})).await["sourceRevision"],
        original["sourceRevision"]
    );
    sqlx::query("CREATE TRIGGER reject_receipt BEFORE UPDATE ON note_operation BEGIN SELECT RAISE(ABORT,'injected receipt failure'); END")
        .execute(store.write_pool()).await.unwrap();
    let mut write = begin(&store, request.clone()).await;
    write
        .persist_source(&author(), &intent_core::now_iso())
        .await
        .unwrap();
    assert!(write.commit().await.is_err());
    let after = page(&store, json!({"kind":"source"})).await;
    assert_eq!(after["sourceRevision"], original["sourceRevision"]);
    assert_eq!(after["text"], original["text"]);
    for table in [
        "note_operation",
        "note_operation_item",
        "note_operation_source",
        "note_version",
    ] {
        let count: i64 = sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table}"))
            .fetch_one(store.read_pool())
            .await
            .unwrap();
        assert_eq!(count, 0, "{table}");
    }
    assert_eq!(
        store
            .note_mutation_status(
                "alice",
                &request.scope(),
                &request.operation_id,
                &request.payload_digest
            )
            .await
            .unwrap()["outcome"],
        "unknown"
    );
}

#[tokio::test]
async fn note_mutation_conversion_savepoint_retains_initial_canonical_repair_only() {
    let (store, _tmp, _) = setup("aBADz").await;
    let request = request(&store, vec![edit(0, 1, "A")]).await;
    let mut write = begin(&store, request).await;
    write
        .apply_canonical_phase(
            &[edit(1, 4, "")],
            vec![json!({"kind":"warning","code":"repair"})],
        )
        .unwrap();
    write
        .persist_source(&author(), &intent_core::now_iso())
        .await
        .unwrap();
    write.begin_conversion().await.unwrap();
    write
        .apply_canonical_phase(
            &[edit(1, 2, "conversion")],
            vec![json!({"kind":"warning","code":"discarded"})],
        )
        .unwrap();
    write
        .persist_source(&author(), &intent_core::now_iso())
        .await
        .unwrap();
    write.rollback_conversion().await.unwrap();
    assert_eq!(write.source(), "Az");
    let receipt = write.commit().await.unwrap();
    assert_eq!(receipt["sourceLength"], 2);
    assert_eq!(page(&store, json!({"kind":"source"})).await["text"], "Az");
    let versions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM note_version")
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    assert_eq!(versions, 1);
    let effects: Vec<String> =
        sqlx::query_scalar("SELECT value FROM note_operation_item WHERE kind='effects'")
            .fetch_all(store.read_pool())
            .await
            .unwrap();
    assert_eq!(
        effects,
        vec![json!({"kind":"warning","code":"repair"}).to_string()]
    );
}

#[tokio::test]
async fn note_mutation_large_deletion_keeps_inverse_text_out_of_receipt_and_rows_bounded() {
    let (store, _tmp, _) = setup(&"😀x".repeat(30_000)).await;
    let request = request(&store, vec![edit(0, 90_000, "")]).await;
    let receipt = commit(&store, request).await;
    assert!(receipt.to_string().len() < 3584);
    assert_eq!(receipt["sourceLength"], 0);
    let maximum: i64 =
        sqlx::query_scalar("SELECT MAX(length(CAST(text AS BLOB))) FROM note_operation_source")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    assert!(maximum <= 4096);
    let inverse: String =
        sqlx::query_scalar("SELECT value FROM note_operation_item WHERE kind='inverse'")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    assert!(!inverse.contains('😀'));
    assert_eq!(
        serde_json::from_str::<Value>(&inverse).unwrap()["source"]["range"]["end"],
        90_000
    );
}

#[tokio::test]
async fn note_mutation_concurrent_exact_retries_commit_once() {
    let (store, _tmp, _) = setup("same same").await;
    let request = request(&store, vec![edit(5, 9, "different")]).await;
    let execute = || async {
        match store
            .begin_note_mutation("alice", request.clone(), &intent_core::now_iso())
            .await
            .unwrap()
        {
            NoteMutationAdmission::Replay(receipt) => receipt,
            NoteMutationAdmission::Write(mut write) => {
                write
                    .persist_source(&author(), &intent_core::now_iso())
                    .await
                    .unwrap();
                write.commit().await.unwrap()
            }
        }
    };
    let (first, second) = tokio::join!(execute(), execute());
    assert_eq!(first, second);
    let versions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM note_version")
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    assert_eq!(versions, 1);
    assert_eq!(
        page(&store, json!({"kind":"source"})).await["text"],
        "same different"
    );
}

#[tokio::test]
async fn note_mutation_deleted_incarnation_receipt_does_not_mutate_replacement() {
    let (store, _tmp, mut note) = setup("old source").await;
    let request = request(&store, vec![edit(0, 3, "new")]).await;
    let receipt = commit(&store, request.clone()).await;
    store
        .delete_note(&note.workspace_id, &note.id)
        .await
        .unwrap();
    note.content = "replacement incarnation".into();
    store.insert_note(&note).await.unwrap();
    let replay = store
        .begin_note_mutation("alice", request.clone(), &intent_core::now_iso())
        .await
        .unwrap();
    assert!(matches!(replay, NoteMutationAdmission::Replay(value) if value == receipt));
    let replacement = page(&store, json!({"kind":"source"})).await;
    assert_eq!(replacement["text"], "replacement incarnation");
    assert_ne!(
        replacement["scope"]["noteInstanceId"],
        request.note_instance_id
    );
    // Pruning ends historical outcome knowledge, never renews admission.
    sqlx::query("DELETE FROM note_operation")
        .execute(store.write_pool())
        .await
        .unwrap();
    let late =
        intent_core::iso_from_unix_secs(request.deadline().unwrap().unix_timestamp() + 86_400);
    assert!(matches!(
        store
            .begin_note_mutation("alice", request.clone(), &late)
            .await,
        Err(Error::NoteMutation(NoteMutationError::Expired))
    ));
    assert_eq!(
        store
            .note_mutation_status(
                "alice",
                &request.scope(),
                &request.operation_id,
                &request.payload_digest
            )
            .await
            .unwrap()["outcome"],
        "unknown"
    );
}
