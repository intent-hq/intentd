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
async fn note_mutation_child_failures_rollback_children_relations_and_conversion_versions() {
    // The second child's version failure occurs after its row/index insertion;
    // SQLite ABORT does not undo the first child or earlier conversion writes.
    for failing_table in ["note", "note_version"] {
        let (store, _tmp, original) = setup("base").await;
        let mut dependency = original.clone();
        dependency.id = NoteId("dependency".into());
        dependency.metadata.task = Some(intent_core::TaskMetadata::default());
        store.insert_note(&dependency).await.unwrap();
        let id_column = if failing_table == "note" {
            "id"
        } else {
            "note_id"
        };
        sqlx::query(&format!("CREATE TRIGGER fail_second_child BEFORE INSERT ON {failing_table} WHEN NEW.{id_column}='second-child' BEGIN SELECT RAISE(ABORT,'injected child failure'); END"))
            .execute(store.write_pool()).await.unwrap();
        let request = request(&store, vec![edit(0, 4, "caller")]).await;
        let mut write = begin(&store, request).await;
        write
            .persist_source(&author(), &intent_core::now_iso())
            .await
            .unwrap();
        write.begin_conversion().await.unwrap();
        write
            .apply_canonical_phase(&[edit(0, 6, "converted")], vec![])
            .unwrap();
        write
            .persist_source(&author(), &intent_core::now_iso())
            .await
            .unwrap();
        let mut child = write.note().clone();
        child.id = NoteId("first-child".into());
        child.parent_id = Some(original.id.clone());
        child.rev = 0;
        child.metadata.task = Some(intent_core::TaskMetadata {
            depends_on: vec![dependency.id.clone()],
            ..Default::default()
        });
        write
            .insert_conversion_child(&child, &author())
            .await
            .unwrap();
        child.id = NoteId("second-child".into());
        assert!(write
            .insert_conversion_child(&child, &author())
            .await
            .is_err());
        // Only a successful rollback allows the initial canonical write to commit.
        write.rollback_conversion().await.unwrap();
        assert_eq!(write.source(), "caller");
        write.commit().await.unwrap();
        assert_eq!(
            page(&store, json!({"kind":"source"})).await["text"],
            "caller"
        );
        for table in ["note", "note_version", "note_page_head"] {
            let id_column = if table == "note" { "id" } else { "note_id" };
            let count: i64 = sqlx::query_scalar(&format!(
                "SELECT COUNT(*) FROM {table} WHERE {id_column} IN ('first-child','second-child')"
            ))
            .fetch_one(store.read_pool())
            .await
            .unwrap();
            assert_eq!(count, 0, "{failing_table}: {table}");
        }
        let versions: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM note_version WHERE note_id='spec'")
                .fetch_one(store.read_pool())
                .await
                .unwrap();
        assert_eq!(versions, 1);
        let effects: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM note_operation_item WHERE kind='effects'")
                .fetch_one(store.read_pool())
                .await
                .unwrap();
        assert_eq!(effects, 0);
        assert_eq!(
            store
                .get_note(&dependency.workspace_id, &dependency.id)
                .await
                .unwrap(),
            dependency
        );
    }
}

#[tokio::test]
async fn note_mutation_failed_savepoint_rollback_drops_the_outer_write() {
    let (store, _tmp, original) = setup("base").await;
    sqlx::query("CREATE TRIGGER fail_child_transaction BEFORE INSERT ON note_version WHEN NEW.note_id='child' BEGIN SELECT RAISE(ROLLBACK,'injected transaction failure'); END")
        .execute(store.write_pool()).await.unwrap();
    let request = request(&store, vec![edit(0, 4, "caller")]).await;
    let mut write = begin(&store, request.clone()).await;
    write
        .persist_source(&author(), &intent_core::now_iso())
        .await
        .unwrap();
    write.begin_conversion().await.unwrap();
    let mut child = write.note().clone();
    child.id = NoteId("child".into());
    child.parent_id = Some(original.id.clone());
    child.rev = 0;
    assert!(write
        .insert_conversion_child(&child, &author())
        .await
        .is_err());
    assert!(write.rollback_conversion().await.is_err());
    drop(write); // Failed recovery is never a reason to commit the outer writer.
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM note_operation")
        .fetch_one(store.write_pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
    assert_eq!(page(&store, json!({"kind":"source"})).await["text"], "base");
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM note_version")
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
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
async fn note_mutation_relation_updates_preserve_source_and_stamp_each_changed_revision() {
    let (store, _tmp, original) = setup("base").await;
    let mut child = original.clone();
    child.id = NoteId("reused-child".into());
    child.parent_id = Some(original.id.clone());
    child.content = "unrelated😀\r\n".repeat(4096);
    child.metadata.task = Some(intent_core::TaskMetadata {
        estimated_effort: Some("2d".into()),
        ..Default::default()
    });
    store.insert_note(&child).await.unwrap();
    let mut target = child.clone();
    target.id = NoteId("target".into());
    store.insert_note(&target).await.unwrap();
    let request = request(&store, vec![edit(0, 4, "caller")]).await;
    let mut write = begin(&store, request).await;
    let snapshot = write.workspace_note_metadata().await.unwrap();
    assert_eq!(snapshot.len(), 3);
    assert!(snapshot.iter().all(|note| note.content.is_empty()));
    assert_eq!(
        snapshot
            .iter()
            .find(|note| note.id == child.id)
            .unwrap()
            .metadata,
        child.metadata
    );
    assert!(write
        .persist_conversion_relations(&child.id, &[], &[], "2026-01-01T00:00:00Z")
        .await
        .is_err());
    write
        .persist_source(&author(), &intent_core::now_iso())
        .await
        .unwrap();
    write.begin_conversion().await.unwrap();
    let first = write
        .persist_conversion_relations(
            &child.id,
            std::slice::from_ref(&target.id),
            &[],
            "2026-01-01T00:00:01Z",
        )
        .await
        .unwrap()
        .unwrap();
    assert!(first.content.is_empty());
    assert_eq!(first.updated_at, "2026-01-01T00:00:01Z");
    assert_eq!(first.rev, child.rev + 1);
    assert!(write
        .persist_conversion_relations(
            &child.id,
            std::slice::from_ref(&target.id),
            &[],
            "2026-01-01T00:00:02Z",
        )
        .await
        .unwrap()
        .is_none());
    let second = write
        .persist_conversion_relations(
            &child.id,
            std::slice::from_ref(&target.id),
            std::slice::from_ref(&target.id),
            "2026-01-01T00:00:03Z",
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second.rev, child.rev + 2);
    assert_eq!(second.updated_at, "2026-01-01T00:00:03Z");
    write.finish_conversion().await.unwrap();
    write.commit().await.unwrap();
    let actual = store
        .get_note(&child.workspace_id, &child.id)
        .await
        .unwrap();
    assert!(
        store
            .note_annotation_epochs(&child.workspace_id, &child.id)
            .await
            .unwrap()
            .anchors_ready
    );
    assert_eq!(actual.content, child.content);
    assert_eq!(actual.rev, second.rev);
    assert_eq!(actual.updated_at, second.updated_at);
    assert_eq!(
        actual
            .metadata
            .task
            .as_ref()
            .unwrap()
            .estimated_effort
            .as_deref(),
        Some("2d")
    );
    assert_eq!(
        actual.metadata.task.as_ref().unwrap().depends_on,
        vec![target.id.clone()]
    );
    assert_eq!(
        actual.metadata.task.as_ref().unwrap().conflicts_with,
        vec![target.id]
    );
    let source: String = sqlx::query_scalar("SELECT group_concat(text,'') FROM (SELECT text FROM note_page_piece WHERE workspace_id='pages' AND note_id='reused-child' ORDER BY start)")
        .fetch_one(store.read_pool()).await.unwrap();
    assert_eq!(source, child.content);
}

#[tokio::test]
async fn note_mutation_relation_failure_rolls_back_prior_relations_and_created_children() {
    let (store, _tmp, original) = setup("base").await;
    let mut first = original.clone();
    first.id = NoteId("first-child".into());
    first.parent_id = Some(original.id.clone());
    first.metadata.task = Some(intent_core::TaskMetadata::default());
    store.insert_note(&first).await.unwrap();
    let mut second = first.clone();
    second.id = NoteId("second-child".into());
    store.insert_note(&second).await.unwrap();
    sqlx::query("CREATE TRIGGER fail_second_relation BEFORE UPDATE OF task_json ON note WHEN NEW.id='second-child' BEGIN SELECT RAISE(ABORT,'injected relation failure'); END")
        .execute(store.write_pool()).await.unwrap();
    let request = request(&store, vec![edit(0, 4, "caller")]).await;
    let mut write = begin(&store, request).await;
    write
        .persist_source(&author(), &intent_core::now_iso())
        .await
        .unwrap();
    write.begin_conversion().await.unwrap();
    let mut created = first.clone();
    created.id = NoteId("created-child".into());
    write
        .insert_conversion_child(&created, &author())
        .await
        .unwrap();
    write
        .persist_conversion_relations(
            &first.id,
            std::slice::from_ref(&created.id),
            &[],
            "2026-01-01T00:00:01Z",
        )
        .await
        .unwrap();
    assert!(write
        .persist_conversion_relations(
            &second.id,
            std::slice::from_ref(&created.id),
            &[],
            "2026-01-01T00:00:02Z",
        )
        .await
        .is_err());
    // ABORT affects only the failing statement. Recovery must undo the first
    // child's metadata/index revision and the newly inserted child as well.
    write.rollback_conversion().await.unwrap();
    write.commit().await.unwrap();
    assert_eq!(
        store
            .get_note(&first.workspace_id, &first.id)
            .await
            .unwrap(),
        first
    );
    assert_eq!(
        store
            .get_note(&second.workspace_id, &second.id)
            .await
            .unwrap(),
        second
    );
    assert_eq!(
        page(&store, json!({"kind":"source"})).await["text"],
        "caller"
    );
    for table in ["note", "note_page_head", "note_version"] {
        let key = if table == "note" { "id" } else { "note_id" };
        let count: i64 = sqlx::query_scalar(&format!(
            "SELECT COUNT(*) FROM {table} WHERE {key}='created-child'"
        ))
        .fetch_one(store.read_pool())
        .await
        .unwrap();
        assert_eq!(count, 0, "{table}");
    }
    let effects: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM note_operation_item WHERE kind='effects'")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    assert_eq!(effects, 0);
    let revision: i64 = sqlx::query_scalar("SELECT current_rev FROM note_page_head WHERE workspace_id='pages' AND note_id='first-child'")
        .fetch_one(store.read_pool()).await.unwrap();
    assert_eq!(revision, first.rev);
}

#[tokio::test]
async fn note_mutation_uncaught_phase_version_failure_drops_all_prior_phases() {
    let (store, _tmp, _) = setup("base").await;
    sqlx::query("CREATE TRIGGER fail_second_version BEFORE INSERT ON note_version WHEN NEW.note_id='spec' AND NEW.v>1 BEGIN SELECT RAISE(ABORT,'injected version failure'); END")
        .execute(store.write_pool()).await.unwrap();
    let request = request(&store, vec![edit(0, 4, "caller")]).await;
    let mut write = begin(&store, request).await;
    write
        .persist_source(&author(), &intent_core::now_iso())
        .await
        .unwrap();
    // Same source is deliberate: a stale persisted-source equality check alone
    // cannot detect the row/revision written before version insertion failed.
    assert!(write
        .persist_source(&author(), &intent_core::now_iso())
        .await
        .is_err());
    drop(write);
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM note_operation")
        .fetch_one(store.write_pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
    assert_eq!(page(&store, json!({"kind":"source"})).await["text"], "base");
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM note_version")
        .fetch_one(store.read_pool())
        .await
        .unwrap();
    assert_eq!(count, 0);
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
        serde_json::from_str::<Value>(&inverse).unwrap()["replacement"]["length"],
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

#[tokio::test]
async fn note_operation_status_preserves_receipt_scope_without_reading_recreated_text() {
    use intent_core::note_mutation::NoteOperationStatusQuery;
    let (store, _tmp, _) = setup("original").await;
    let request = request(&store, vec![edit(0, 8, "saved")]).await;
    let receipt = commit(&store, request.clone()).await;
    let mut status = NoteOperationStatusQuery {
        backend_id: request.backend_id.clone(),
        workspace_id: request.workspace_id.clone(),
        note_id: request.note_id.clone(),
        note_instance_id: request.note_instance_id.clone(),
        operation_id: request.operation_id.clone(),
        payload_digest: Some(request.payload_digest.clone()),
        header_digest: None,
    };
    assert_eq!(
        store
            .read_note_operation_status("alice", &status)
            .await
            .unwrap(),
        receipt
    );
    assert_eq!(
        store
            .read_note_operation_status("bob", &status)
            .await
            .unwrap()["outcome"],
        "unknown"
    );
    let mut replacement = store
        .get_note(&WorkspaceId("pages".into()), &NoteId("spec".into()))
        .await
        .unwrap();
    sqlx::query("DELETE FROM note WHERE workspace_id='pages' AND id='spec'")
        .execute(store.write_pool())
        .await
        .unwrap();
    assert_eq!(
        store
            .read_note_operation_status("alice", &status)
            .await
            .unwrap(),
        receipt
    );
    replacement.content = "new incarnation private text".repeat(1000);
    store.insert_note(&replacement).await.unwrap();
    assert_eq!(
        store
            .read_note_operation_status("alice", &status)
            .await
            .unwrap(),
        receipt
    );
    let replacement_scope = page(&store, json!({"kind":"source","maxSourceBytes":32})).await;
    status.note_instance_id = replacement_scope["scope"]["noteInstanceId"]
        .as_str()
        .unwrap()
        .into();
    assert_eq!(
        store
            .read_note_operation_status("alice", &status)
            .await
            .unwrap()["outcome"],
        "unknown"
    );
    status
        .note_instance_id
        .clone_from(&request.note_instance_id);
    status.payload_digest = Some("b".repeat(64));
    assert!(matches!(
        store.read_note_operation_status("alice", &status).await,
        Err(Error::NoteMutation(NoteMutationError::Mismatch))
    ));
    status.payload_digest = Some(request.payload_digest);
    status.backend_id = "foreign database".into();
    assert!(matches!(
        store.read_note_operation_status("alice", &status).await,
        Err(Error::NoteMutation(NoteMutationError::Conflict))
    ));
    status.backend_id = request.backend_id;
    status.header_digest = Some("c".repeat(64));
    assert!(matches!(
        store.read_note_operation_status("alice", &status).await,
        Err(Error::Unsupported(_))
    ));
}
