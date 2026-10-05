use super::Services;
use crate::tests::setup;
use intent_core::{
    note_mutation::{NoteMutationError, NoteOperationStatusQuery},
    note_stage::{
        NoteStageAppend, NoteStageBegin, NoteStageManifestEntry, NoteStageSeal, NOTE_STAGE_STREAMS,
    },
    with_caller, Caller, Error, HostRole, Principal, PrincipalId, Result, WorkspaceId,
    WorkspaceRole,
};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc, Mutex,
};

struct Boundary {
    now: Mutex<Option<time::OffsetDateTime>>,
    expiry: Mutex<Option<time::OffsetDateTime>>,
    observed: AtomicU8,
    reached: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
tokio::task_local! {
    static BOUNDARY: Arc<Boundary>;
}
pub(super) async fn after_store(result: &Result<Value>) {
    if let Ok(boundary) = BOUNDARY.try_with(Arc::clone) {
        let observation = match result {
            Ok(_) => 1,
            Err(Error::NoteMutation(NoteMutationError::Mismatch)) => 2,
            Err(_) => 3,
        };
        *boundary.expiry.lock().unwrap() = result
            .as_ref()
            .ok()
            .and_then(|v| v["expiresAt"].as_str())
            .and_then(intent_core::parse_iso);
        boundary.observed.store(observation, Ordering::SeqCst);
        boundary.reached.notify_one();
        boundary.release.notified().await;
    }
}
pub(super) async fn guest(service: &Services, workspace: &WorkspaceId) -> Caller {
    let principal = Principal {
        id: PrincipalId::new(),
        identity: None,
        github_user_id: None,
        login: None,
        display_name: None,
        avatar_url: None,
        is_primary: false,
        created_at: intent_core::now_iso(),
        updated_at: intent_core::now_iso(),
    };
    service.store.upsert_principal(&principal).await.unwrap();
    service
        .store
        .add_workspace_member(workspace, &principal.id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    Caller::Wire {
        principal_id: principal.id,
        host_role: HostRole::Guest,
    }
}
async fn begin_request(
    service: &Services,
    workspace: &WorkspaceId,
    note: &intent_core::NoteId,
) -> NoteStageBegin {
    let state = service
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
    value["header"] = json!({"baseRevision":state["sourceRevision"],"editorSessionId":"boundary","localEditSequence":0,"liveGeneration":0,"selectionGeneration":0,"action":"mutate","output":"source","selection":"all"});
    let mut request: NoteStageBegin = serde_json::from_value(value).unwrap();
    request.header_digest = request.computed_digest().unwrap();
    request
}
fn query(begin: &NoteStageBegin) -> NoteOperationStatusQuery {
    NoteOperationStatusQuery {
        backend_id: begin.backend_id.clone(),
        workspace_id: begin.workspace_id.clone(),
        note_id: begin.note_id.clone(),
        note_instance_id: begin.note_instance_id.clone(),
        operation_id: begin.operation_id.clone(),
        header_digest: Some(begin.header_digest.clone()),
        payload_digest: None,
    }
}
async fn call(service: &Services, method: u8, begin: NoteStageBegin) -> Result<Value> {
    let query = query(&begin);
    match method {
        0 => service.begin_note_stage(begin).await,
        1 => {
            let mut chunk = serde_json::to_value(query).unwrap();
            chunk["stream"] = json!("text");
            chunk["sequence"] = json!(0);
            chunk["previousDigest"] = Value::Null;
            chunk["records"] = json!([{"kind":"text","id":"text","offset":0,"text":"kept😀"}]);
            chunk["chunkDigest"] = json!("0".repeat(64));
            let mut chunk: NoteStageAppend = serde_json::from_value(chunk).unwrap();
            chunk.chunk_digest = chunk.computed_digest().unwrap();
            service.append_note_stage(chunk).await
        }
        2 => {
            service
                .cancel_note_stage(
                    serde_json::from_value(serde_json::to_value(query).unwrap()).unwrap(),
                )
                .await
        }
        4 => {
            let mut request = NoteStageSeal {
                backend_id: begin.backend_id,
                workspace_id: begin.workspace_id,
                note_id: begin.note_id,
                note_instance_id: begin.note_instance_id,
                operation_id: begin.operation_id,
                header_digest: begin.header_digest,
                payload_digest: String::new(),
                manifest: NOTE_STAGE_STREAMS
                    .into_iter()
                    .map(|stream| NoteStageManifestEntry {
                        stream,
                        chunks: 0,
                        records: 0,
                        last_digest: None,
                    })
                    .collect(),
            };
            request.payload_digest = request.computed_digest().unwrap();
            service.seal_note_stage(request).await
        }
        5 => {
            let mut request = serde_json::to_value(query).unwrap();
            request.as_object_mut().unwrap().remove("payloadDigest");
            request["kind"] = json!("source");
            service
                .read_stage_source(serde_json::from_value(request).unwrap(), json!(1))
                .await
        }
        6 => {
            let mut request = serde_json::to_value(query).unwrap();
            request["payloadDigest"] = json!("0".repeat(64));
            service
                .commit_note_stage(serde_json::from_value(request).unwrap())
                .await
        }
        _ => service.read_note_stage_status(query).await,
    }
}
#[tokio::test]
async fn staged_service_rechecks_authority_after_store_success_and_mismatch() {
    for method in 0..5 {
        for mismatch in [false, true] {
            let (_tmp, service, workspace, note) = setup("source😀").await;
            let caller = guest(&service, &workspace).await;
            let original = begin_request(&service, &workspace, &note).await;
            with_caller(caller.clone(), service.begin_note_stage(original.clone()))
                .await
                .unwrap();
            let mut request = original.clone();
            if mismatch {
                request.header.editor_session_id = "different".into();
                request.header_digest = request.computed_digest().unwrap();
            }
            let boundary = Arc::new(Boundary {
                now: Mutex::new(None),
                expiry: Mutex::new(None),
                observed: AtomicU8::new(0),
                reached: tokio::sync::Notify::new(),
                release: tokio::sync::Notify::new(),
            });
            let operation = BOUNDARY.scope(
                boundary.clone(),
                with_caller(caller.clone(), call(&service, method, request)),
            );
            let revoke = async {
                boundary.reached.notified().await;
                assert_eq!(
                    boundary.observed.load(Ordering::SeqCst),
                    if mismatch { 2 } else { 1 },
                    "actual Store outcome before final authorization"
                );
                let Caller::Wire { principal_id, .. } = &caller else {
                    panic!("guest required")
                };
                service
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
            assert!(service.stage_request_admission.0.lock().unwrap().is_empty());
            let Caller::Wire { principal_id, .. } = &caller else {
                panic!("guest required")
            };
            service
                .store
                .add_workspace_member(&workspace, principal_id, WorkspaceRole::Collaborator)
                .await
                .unwrap();
            let retained = with_caller(
                caller.clone(),
                service.read_note_stage_status(query(&original)),
            )
            .await
            .unwrap();
            assert_eq!(
                retained["phase"],
                if method == 2 && !mismatch {
                    "cancelled"
                } else if method == 4 && !mismatch {
                    "sealed"
                } else {
                    "staging"
                }
            );
            assert_eq!(
                retained["streams"][0]["nextSequence"],
                i32::from(method == 1 && !mismatch)
            );
            let stranger = guest(&service, &workspace).await;
            let unknown = with_caller(stranger, service.read_note_stage_status(query(&original)))
                .await
                .unwrap();
            assert_eq!(unknown["outcome"], "unknown");
        }
    }
}

struct AuthBoundary {
    reached: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
tokio::task_local! { static AUTH_BOUNDARY: Arc<AuthBoundary>; }
pub(super) async fn before_authorize() {
    if let Ok(boundary) = AUTH_BOUNDARY.try_with(Arc::clone) {
        boundary.reached.notify_one();
        boundary.release.notified().await;
    }
}
#[tokio::test]
async fn staged_service_admits_before_pending_authorization_and_releases_on_denial() {
    for method in 0..7 {
        let (_tmp, service, workspace, note) = setup("source").await;
        let caller = guest(&service, &workspace).await;
        let request = begin_request(&service, &workspace, &note).await;
        let first = Arc::new(AuthBoundary {
            reached: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let second = Arc::new(AuthBoundary {
            reached: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let one = AUTH_BOUNDARY.scope(
            first.clone(),
            with_caller(caller.clone(), call(&service, method, request.clone())),
        );
        let two = async {
            first.reached.notified().await;
            if method == 1 {
                assert!(matches!(
                    with_caller(caller.clone(), call(&service, 1, request.clone())).await,
                    Err(Error::NoteMutation(NoteMutationError::Budget))
                ));
            }
            AUTH_BOUNDARY
                .scope(
                    second.clone(),
                    with_caller(caller.clone(), call(&service, 3, request.clone())),
                )
                .await
        };
        let probe = async {
            second.reached.notified().await;
            assert!(matches!(
                with_caller(caller.clone(), call(&service, 3, request.clone())).await,
                Err(Error::NoteMutation(NoteMutationError::Budget))
            ));
            let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM note_stage")
                .fetch_one(service.store.read_pool())
                .await
                .unwrap();
            assert_eq!(total, 0, "no Store stage action reached while auth held");
            let Caller::Wire { principal_id, .. } = &caller else {
                panic!("guest required")
            };
            service
                .store
                .remove_workspace_member(&workspace, principal_id)
                .await
                .unwrap();
            first.release.notify_one();
            second.release.notify_one();
        };
        let (one, two, ()) = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            tokio::join!(one, two, probe)
        })
        .await
        .unwrap();
        assert!(matches!(one, Err(Error::NotFound(_))));
        assert!(matches!(two, Err(Error::NotFound(_))));
        assert!(service.stage_request_admission.0.lock().unwrap().is_empty());
    }
}

pub(super) fn return_now(real: time::OffsetDateTime) -> time::OffsetDateTime {
    BOUNDARY
        .try_with(|b| *b.now.lock().unwrap())
        .ok()
        .flatten()
        .unwrap_or(real)
}
#[tokio::test]
async fn staged_service_observes_original_expiry_after_store_result() {
    for method in [0, 3, 4] {
        let (_tmp, service, workspace, note) = setup("source").await;
        let caller = guest(&service, &workspace).await;
        let request = begin_request(&service, &workspace, &note).await;
        with_caller(caller.clone(), service.begin_note_stage(request.clone()))
            .await
            .unwrap();
        let boundary = Arc::new(Boundary {
            now: Mutex::new(None),
            expiry: Mutex::new(None),
            observed: AtomicU8::new(0),
            reached: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let operation = BOUNDARY.scope(
            boundary.clone(),
            with_caller(caller.clone(), call(&service, method, request.clone())),
        );
        let advance = async {
            boundary.reached.notified().await;
            assert_eq!(boundary.observed.load(Ordering::SeqCst), 1);
            let expiry = boundary.expiry.lock().unwrap().unwrap();
            assert_eq!(expiry, intent_core::parse_iso(&request.expires_at).unwrap());
            assert!(expiry > time::OffsetDateTime::now_utc());
            *boundary.now.lock().unwrap() = Some(expiry);
            boundary.release.notify_one();
        };
        let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            tokio::join!(operation, advance)
        })
        .await
        .unwrap();
        assert_eq!(result.unwrap()["phase"], "expired");
        let original = with_caller(caller, service.read_note_stage_status(query(&request)))
            .await
            .unwrap();
        assert_eq!(
            original["phase"],
            if method == 4 { "sealed" } else { "staging" },
            "only this response clock advanced, no global time or retained state mutation"
        );
    }
}

#[tokio::test]
async fn staged_source_service_rechecks_after_result_and_original_expiry() {
    for mode in 0..3 {
        let (_tmp, service, workspace, note) = setup("frozen😀").await;
        let caller = guest(&service, &workspace).await;
        let begin = begin_request(&service, &workspace, &note).await;
        with_caller(caller.clone(), service.begin_note_stage(begin.clone()))
            .await
            .unwrap();
        with_caller(caller.clone(), call(&service, 4, begin.clone()))
            .await
            .unwrap();
        let mut query = serde_json::to_value(query(&begin)).unwrap();
        query.as_object_mut().unwrap().remove("payloadDigest");
        query["kind"] = json!("source");
        if mode == 1 {
            query["headerDigest"] = json!("f".repeat(64));
        }
        let query = serde_json::from_value(query).unwrap();
        let boundary = Arc::new(Boundary {
            now: Mutex::new(None),
            expiry: Mutex::new(None),
            observed: AtomicU8::new(0),
            reached: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let read = BOUNDARY.scope(
            boundary.clone(),
            with_caller(caller.clone(), service.read_stage_source(query, json!(1))),
        );
        let change = async {
            boundary.reached.notified().await;
            assert_eq!(
                boundary.observed.load(Ordering::SeqCst),
                if mode == 1 { 3 } else { 1 },
                "Store result captured before authorization/expiry change"
            );
            if mode == 2 {
                let expiry = boundary.expiry.lock().unwrap().unwrap();
                assert_eq!(expiry, intent_core::parse_iso(&begin.expires_at).unwrap());
                *boundary.now.lock().unwrap() = Some(expiry);
            } else {
                let Caller::Wire { principal_id, .. } = &caller else {
                    panic!("wire required")
                };
                service
                    .store
                    .remove_workspace_member(&workspace, principal_id)
                    .await
                    .unwrap();
            }
            boundary.release.notify_one();
        };
        let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(15), async {
            tokio::join!(read, change)
        })
        .await
        .unwrap();
        if mode == 2 {
            assert!(matches!(
                result,
                Err(Error::NotePage(
                    intent_core::note_page::NotePageError::Expired
                ))
            ));
        } else {
            assert!(matches!(result, Err(Error::NotFound(_))));
        }
        assert!(service.stage_request_admission.0.lock().unwrap().is_empty());
    }
}

#[tokio::test]
async fn staged_search_service_rechecks_results_and_detail_original_expiry() {
    for detail in [false, true] {
        for mode in 0..3 {
            let (_tmp, service, workspace, note) = setup("frozen😀").await;
            let caller = guest(&service, &workspace).await;
            let mut begin = begin_request(&service, &workspace, &note).await;
            begin.header.output = intent_core::note_stage::NoteStageOutput::Search;
            begin.header.query = Some(
                serde_json::from_value(
                    json!({"text":"frozen", "caseSensitive":false, "mode":"source"}),
                )
                .unwrap(),
            );
            begin.header_digest = begin.computed_digest().unwrap();
            with_caller(caller.clone(), service.begin_note_stage(begin.clone()))
                .await
                .unwrap();
            with_caller(caller.clone(), call(&service, 4, begin.clone()))
                .await
                .unwrap();
            let mut query = serde_json::to_value(query(&begin)).unwrap();
            query.as_object_mut().unwrap().remove("payloadDigest");
            query["kind"] = json!("search");
            let first = with_caller(
                caller.clone(),
                service.read_stage_source(serde_json::from_value(query.clone()).unwrap(), json!(1)),
            )
            .await
            .unwrap();
            if detail {
                query["kind"] = json!("detail");
                query["ref"] = first["items"][0]["detailRef"].clone();
            }
            if mode == 1 {
                query["headerDigest"] = json!("f".repeat(64));
            }

            let boundary = Arc::new(Boundary {
                now: Mutex::new(None),
                expiry: Mutex::new(None),
                observed: AtomicU8::new(0),
                reached: tokio::sync::Notify::new(),
                release: tokio::sync::Notify::new(),
            });
            let read = BOUNDARY.scope(
                boundary.clone(),
                with_caller(caller.clone(), async {
                    if detail {
                        let read: intent_core::note_receipt_detail::NoteOperationReceiptRead =
                            serde_json::from_value(query).unwrap();
                        service
                            .read_note_receipt(read.query().unwrap(), json!(1))
                            .await
                    } else {
                        service
                            .read_stage_source(serde_json::from_value(query).unwrap(), json!(1))
                            .await
                    }
                }),
            );
            let change = async {
                boundary.reached.notified().await;
                assert_eq!(
                    boundary.observed.load(Ordering::SeqCst),
                    if mode == 1 { 3 } else { 1 },
                    "Store result captured before authorization/expiry change"
                );
                if mode == 2 {
                    let expiry = boundary.expiry.lock().unwrap().unwrap();
                    assert_eq!(expiry, intent_core::parse_iso(&begin.expires_at).unwrap());
                    *boundary.now.lock().unwrap() = Some(expiry);
                } else {
                    let Caller::Wire { principal_id, .. } = &caller else {
                        panic!("wire required")
                    };
                    service
                        .store
                        .remove_workspace_member(&workspace, principal_id)
                        .await
                        .unwrap();
                }
                boundary.release.notify_one();
            };
            let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(15), async {
                tokio::join!(read, change)
            })
            .await
            .unwrap();
            if mode == 2 {
                assert!(matches!(
                    result,
                    Err(Error::NotePage(
                        intent_core::note_page::NotePageError::Expired
                    ))
                ));
            } else {
                assert!(matches!(result, Err(Error::NotFound(_))));
            }
            assert!(service.stage_request_admission.0.lock().unwrap().is_empty());
        }
    }
}

mod selection;
