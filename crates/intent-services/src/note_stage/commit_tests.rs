use super::Services;
use crate::tests::setup;
use intent_core::{
    note_mutation::NoteMutationError,
    note_stage::{
        NoteStageAppend, NoteStageBegin, NoteStageCommit, NoteStageManifestEntry, NoteStageSeal,
        NoteStageStream, NOTE_STAGE_STREAMS,
    },
    Error, NoteId, WorkspaceApi, WorkspaceId,
};
use serde_json::{json, Value};

async fn captured(
    services: &Services,
    workspace: &WorkspaceId,
    note: &NoteId,
    replacement: &str,
) -> NoteStageCommit {
    let state = services
        .store
        .read_note_page_state(workspace, note, None)
        .await
        .unwrap();
    let mut value = state["scope"].clone();
    value["operationId"] = json!(uuid::Uuid::new_v4().to_string());
    value["expiresAt"] = json!(format!(
        "{}.000Z",
        &intent_core::iso_ms_from_now(60_000)[..19]
    ));
    value["headerDigest"] = json!("0".repeat(64));
    value["header"] = json!({"baseRevision":state["sourceRevision"],"editorSessionId":"commit","localEditSequence":0,"liveGeneration":0,"selectionGeneration":0,"action":"mutate","output":"source","selection":"all"});
    let mut begin: NoteStageBegin = serde_json::from_value(value).unwrap();
    begin.header_digest = begin.computed_digest().unwrap();
    services.note_operation_begin(begin.clone()).await.unwrap();
    let mut manifest: Vec<_> = NOTE_STAGE_STREAMS
        .into_iter()
        .map(|stream| NoteStageManifestEntry {
            stream,
            chunks: 0,
            records: 0,
            last_digest: None,
        })
        .collect();
    for (stream, records) in [
        (
            NoteStageStream::Text,
            vec![json!({"kind":"text","id":"insert","offset":0,"text":replacement})],
        ),
        (
            NoteStageStream::Mutation,
            vec![
                json!({"kind":"splice","ordinal":0,"start":0,"end":0,"replacement":{"textId":"insert","length":replacement.encode_utf16().count(),"utf8Bytes":replacement.len(),"sha256":crate::attachment_upload::sha256_hex(replacement.as_bytes())}}),
            ],
        ),
    ] {
        let mut chunk = NoteStageAppend {
            backend_id: begin.backend_id.clone(),
            workspace_id: begin.workspace_id.clone(),
            note_id: begin.note_id.clone(),
            note_instance_id: begin.note_instance_id.clone(),
            operation_id: begin.operation_id.clone(),
            header_digest: begin.header_digest.clone(),
            stream,
            sequence: 0,
            previous_digest: None,
            chunk_digest: String::new(),
            records,
        };
        chunk.chunk_digest = chunk.computed_digest().unwrap();
        services.note_operation_append(chunk.clone()).await.unwrap();
        let entry = manifest
            .iter_mut()
            .find(|entry| entry.stream == stream)
            .unwrap();
        entry.chunks = 1;
        entry.records = chunk.records.len() as u64;
        entry.last_digest = Some(chunk.chunk_digest);
    }
    let mut seal = NoteStageSeal {
        backend_id: begin.backend_id,
        workspace_id: begin.workspace_id,
        note_id: begin.note_id,
        note_instance_id: begin.note_instance_id,
        operation_id: begin.operation_id,
        header_digest: begin.header_digest,
        payload_digest: String::new(),
        manifest,
    };
    seal.payload_digest = seal.computed_digest().unwrap();
    services.note_operation_seal(seal.clone()).await.unwrap();
    NoteStageCommit {
        backend_id: seal.backend_id,
        workspace_id: seal.workspace_id,
        note_id: seal.note_id,
        note_instance_id: seal.note_instance_id,
        operation_id: seal.operation_id,
        header_digest: seal.header_digest,
        payload_digest: seal.payload_digest,
    }
}

#[intent_test_macros::daemon_test]
async fn staged_commit_conversion_failure_keeps_only_canonical_initial_edit_and_group() {
    for trigger in [
        "CREATE TRIGGER fail_conversion BEFORE INSERT ON note WHEN NEW.parent_id IS NOT NULL BEGIN SELECT RAISE(ABORT,'child injected'); END",
        "CREATE TRIGGER fail_conversion BEFORE INSERT ON note_version WHEN NEW.note_id != 'n1' BEGIN SELECT RAISE(ABORT,'child version injected'); END",
        "CREATE TRIGGER fail_conversion BEFORE UPDATE OF task_json ON note WHEN NEW.parent_id IS NOT NULL BEGIN SELECT RAISE(ABORT,'relation injected'); END",
        "CREATE TRIGGER fail_conversion BEFORE INSERT ON note_version WHEN NEW.note_id = 'n1' AND (SELECT COUNT(*) FROM note_version WHERE note_id='n1') > 0 BEGIN SELECT RAISE(ABORT,'conversion version injected'); END",
    ] {
        let source="prefix😀\r\n@@@task key=a\n# A\nbody\n@@@\n@@@task key=b dependsOn=a\n# B\nbody\n@@@\n";
        let (_tmp,services,workspace,note)=setup(source).await;
        let input=captured(&services,&workspace,&note,"X").await;
        sqlx::query(trigger).execute(services.store.write_pool()).await.unwrap();
        let receipt=services.note_operation_commit(input.clone()).await.unwrap();
        assert_eq!(services.store.get_note(&workspace,&note).await.unwrap().content,format!("X{source}"),"{trigger}");
        assert_eq!(services.store.list_notes(&workspace).await.unwrap().len(),1);
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT COUNT(*) FROM note_version").fetch_one(services.store.read_pool()).await.unwrap(),1);
        assert_eq!(sqlx::query_scalar::<_,i64>("SELECT converted_count FROM note_operation").fetch_one(services.store.read_pool()).await.unwrap(),0);
        let inverse:String=sqlx::query_scalar("SELECT value FROM note_operation_item WHERE kind='inverse'").fetch_one(services.store.read_pool()).await.unwrap();
        let inverse:Value=serde_json::from_str(&inverse).unwrap();
        assert_eq!(inverse["start"],0);assert_eq!(inverse["end"],1);
        assert_eq!(inverse["inputState"],receipt["afterRevision"]);assert_eq!(inverse["outputState"],receipt["beforeRevision"]);
        assert_eq!(services.note_operation_commit(input).await.unwrap(),receipt);
    }
}

#[intent_test_macros::daemon_test]
async fn staged_commit_fatal_version_error_rolls_back_and_sealed_retry_succeeds() {
    let (_tmp, services, workspace, note) = setup("base😀").await;
    let input = captured(&services, &workspace, &note, "X").await;
    let before = services.store.get_note(&workspace, &note).await.unwrap();
    sqlx::query("CREATE TRIGGER fail_initial BEFORE INSERT ON note_version BEGIN SELECT RAISE(ABORT,'version injected'); END").execute(services.store.write_pool()).await.unwrap();
    assert!(matches!(
        services.note_operation_commit(input.clone()).await,
        Err(Error::Internal(_))
    ));
    let after = services.store.get_note(&workspace, &note).await.unwrap();
    assert_eq!(before.content, after.content);
    assert_eq!(before.rev, after.rev);
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT phase FROM note_stage")
            .fetch_one(services.store.read_pool())
            .await
            .unwrap(),
        "sealed"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM note_operation_item")
            .fetch_one(services.store.read_pool())
            .await
            .unwrap(),
        0
    );
    sqlx::query("DROP TRIGGER fail_initial")
        .execute(services.store.write_pool())
        .await
        .unwrap();
    let receipt = services.note_operation_commit(input.clone()).await.unwrap();
    assert_eq!(
        services
            .store
            .get_note(&workspace, &note)
            .await
            .unwrap()
            .content,
        "Xbase😀"
    );
    assert_eq!(
        services.note_operation_commit(input.clone()).await.unwrap(),
        receipt
    );
    let mut mismatched = input;
    mismatched.payload_digest = "0".repeat(64);
    assert!(matches!(
        services.note_operation_commit(mismatched).await,
        Err(Error::NoteMutation(NoteMutationError::Mismatch))
    ));
}

#[intent_test_macros::daemon_test]
async fn staged_commit_checks_complete_replacement_numbered_guard() {
    let (_tmp, services, workspace, note) = setup("original").await;
    let input = captured(
        &services,
        &workspace,
        &note,
        "   1 | line one\n   2 | line two",
    )
    .await;
    assert!(matches!(
        services.note_operation_commit(input).await,
        Err(Error::InvalidParams(_))
    ));
    assert_eq!(
        services
            .store
            .get_note(&workspace, &note)
            .await
            .unwrap()
            .content,
        "original"
    );
    assert_eq!(
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM note_version")
            .fetch_one(services.store.read_pool())
            .await
            .unwrap(),
        0
    );
}

struct Boundary {
    target: u8,
    observed: std::sync::atomic::AtomicU8,
    expiry: std::sync::Mutex<Option<time::OffsetDateTime>>,
    now: std::sync::Mutex<Option<time::OffsetDateTime>>,
    reached: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
tokio::task_local! { static COMMIT_BOUNDARY: std::sync::Arc<Boundary>; }
fn boundary(target: u8) -> std::sync::Arc<Boundary> {
    std::sync::Arc::new(Boundary {
        target,
        observed: std::sync::atomic::AtomicU8::new(0),
        expiry: std::sync::Mutex::new(None),
        now: std::sync::Mutex::new(None),
        reached: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    })
}
async fn pause(kind: u8, receipt: Option<&Value>) {
    if let Ok(boundary) = COMMIT_BOUNDARY.try_with(std::sync::Arc::clone) {
        if boundary.target != kind {
            return;
        }
        *boundary.expiry.lock().unwrap() = receipt
            .and_then(|v| v["receiptExpiresAt"].as_str())
            .and_then(intent_core::parse_iso);
        boundary
            .observed
            .store(kind, std::sync::atomic::Ordering::SeqCst);
        boundary.reached.notify_one();
        boundary.release.notified().await;
    }
}
pub(super) async fn after_reservation(
    result: &intent_core::Result<intent_store::StageCommitAdmission>,
) {
    match result {
        Ok(intent_store::StageCommitAdmission::Reserved(_)) => pause(1, None).await,
        Ok(intent_store::StageCommitAdmission::Replay(receipt)) => pause(2, Some(receipt)).await,
        Err(Error::NoteMutation(NoteMutationError::Mismatch)) => pause(3, None).await,
        Err(_) => pause(5, None).await,
    }
}
pub(super) async fn after_commit(result: &intent_core::Result<Value>) {
    if let Ok(receipt) = result {
        pause(4, Some(receipt)).await;
    }
}
pub(super) fn return_now(real: time::OffsetDateTime) -> time::OffsetDateTime {
    COMMIT_BOUNDARY
        .try_with(|b| *b.now.lock().unwrap())
        .ok()
        .flatten()
        .unwrap_or(real)
}

#[tokio::test]
async fn staged_commit_replay_and_error_reauthorize_after_store() {
    for mismatch in [false, true] {
        let (_tmp, services, workspace, note) = setup("base").await;
        let caller = super::boundary_tests::guest(&services, &workspace).await;
        let mut input =
            intent_core::with_caller(caller.clone(), captured(&services, &workspace, &note, "X"))
                .await;
        intent_core::with_caller(
            caller.clone(),
            services.note_operation_commit(input.clone()),
        )
        .await
        .unwrap();
        if mismatch {
            input.payload_digest = "0".repeat(64);
        }
        let boundary = boundary(if mismatch { 3 } else { 2 });
        let operation = COMMIT_BOUNDARY.scope(
            boundary.clone(),
            intent_core::with_caller(caller.clone(), services.note_operation_commit(input)),
        );
        let revoke = async {
            boundary.reached.notified().await;
            assert_eq!(
                boundary.observed.load(std::sync::atomic::Ordering::SeqCst),
                if mismatch { 3 } else { 2 }
            );
            let intent_core::Caller::Wire { principal_id, .. } = &caller else {
                panic!("wire expected")
            };
            services
                .store
                .remove_workspace_member(&workspace, principal_id)
                .await
                .unwrap();
            boundary.release.notify_one();
        };
        let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            tokio::join!(operation, revoke)
        })
        .await
        .unwrap();
        assert!(matches!(result, Err(Error::NotFound(_))));
        assert_eq!(
            services
                .store
                .get_note(&workspace, &note)
                .await
                .unwrap()
                .content,
            "Xbase"
        );
        assert!(services
            .stage_request_admission
            .0
            .lock()
            .unwrap()
            .is_empty());
    }
}

#[intent_test_macros::daemon_test]
async fn staged_commit_replay_and_fresh_receipt_check_original_retention_at_return() {
    for replay in [false, true] {
        let (_tmp, services, workspace, note) = setup("base").await;
        let input = captured(&services, &workspace, &note, "X").await;
        if replay {
            services.note_operation_commit(input.clone()).await.unwrap();
        }
        let boundary = boundary(if replay { 2 } else { 4 });
        let operation = COMMIT_BOUNDARY.scope(
            boundary.clone(),
            services.note_operation_commit(input.clone()),
        );
        let advance = async {
            boundary.reached.notified().await;
            assert_eq!(
                boundary.observed.load(std::sync::atomic::Ordering::SeqCst),
                if replay { 2 } else { 4 }
            );
            let expires = boundary.expiry.lock().unwrap().unwrap();
            assert!(expires > time::OffsetDateTime::now_utc());
            *boundary.now.lock().unwrap() = Some(expires);
            boundary.release.notify_one();
        };
        let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            tokio::join!(operation, advance)
        })
        .await
        .unwrap();
        assert!(matches!(
            result,
            Err(Error::NoteMutation(NoteMutationError::Expired))
        ));
        assert_eq!(
            services
                .store
                .get_note(&workspace, &note)
                .await
                .unwrap()
                .content,
            "Xbase"
        );
        assert_eq!(
            sqlx::query_scalar::<_, String>("SELECT phase FROM note_stage")
                .fetch_one(services.store.read_pool())
                .await
                .unwrap(),
            "committed"
        );
        assert_eq!(
            services.note_operation_commit(input).await.unwrap()["kind"],
            "noteCommitReceipt"
        );
    }
}

#[intent_test_macros::daemon_test]
async fn staged_commit_cancelled_reservation_releases_writer_and_admission() {
    let (_tmp, services, workspace, note) = setup("base").await;
    let input = captured(&services, &workspace, &note, "X").await;
    let boundary = boundary(1);
    let mut operation = Box::pin(COMMIT_BOUNDARY.scope(
        boundary.clone(),
        services.note_operation_commit(input.clone()),
    ));
    tokio::time::timeout(std::time::Duration::from_secs(15), async {
        tokio::select! {
            ()=boundary.reached.notified()=>{},
            _=&mut operation=>panic!("reservation must stay held"),
        }
    })
    .await
    .unwrap();
    assert_eq!(
        boundary.observed.load(std::sync::atomic::Ordering::SeqCst),
        1
    );
    drop(operation);
    let writer = tokio::time::timeout(
        std::time::Duration::from_secs(15),
        services.store.write_pool().acquire(),
    )
    .await
    .unwrap()
    .unwrap();
    drop(writer);
    assert!(services
        .stage_request_admission
        .0
        .lock()
        .unwrap()
        .is_empty());
    assert_eq!(
        services
            .store
            .get_note(&workspace, &note)
            .await
            .unwrap()
            .content,
        "base"
    );
    assert_eq!(
        sqlx::query_scalar::<_, String>("SELECT phase FROM note_stage")
            .fetch_one(services.store.read_pool())
            .await
            .unwrap(),
        "sealed"
    );
    assert_eq!(
        services.note_operation_commit(input).await.unwrap()["kind"],
        "noteCommitReceipt"
    );
}
