use super::*;
use crate::tests::setup;
use intent_core::{
    note_mutation::{NoteApplySplices, NoteSplice},
    note_page::NotePageError,
    note_receipt_detail::{NoteGetReceiptContextRequest, NoteOperationReceiptRead},
    with_caller, HostRole, Principal, PrincipalId, WorkspaceApi, WorkspaceRole,
};
use serde_json::json;
use std::sync::{
    atomic::{AtomicI64, Ordering},
    Arc,
};

struct ReadBoundary {
    expires: AtomicI64,
    now: AtomicI64,
    reached: tokio::sync::Notify,
    release: tokio::sync::Notify,
}
tokio::task_local! {
    static READ_BOUNDARY: Arc<ReadBoundary>;
}
pub(super) fn observed_now(real: i64) -> i64 {
    READ_BOUNDARY
        .try_with(|b| b.now.load(Ordering::SeqCst))
        .ok()
        .filter(|now| *now > 0)
        .unwrap_or(real)
}
pub(super) async fn pause_after_read(result: &Result<(Value, i64)>) {
    if let Ok(boundary) = READ_BOUNDARY.try_with(Arc::clone) {
        boundary.expires.store(
            match result {
                Ok((_, expiry)) => *expiry,
                Err(_) => -1,
            },
            Ordering::SeqCst,
        );
        boundary.reached.notify_one();
        boundary.release.notified().await;
    }
}

#[tokio::test]
async fn public_splices_receipt_expiry_and_revocation_after_store_result() {
    for context in [false, true] {
        for revoke in [false, true] {
            for missing in [false, true] {
                let (_tmp, services, workspace, note) = setup("source😀").await;
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
                services.store.upsert_principal(&principal).await.unwrap();
                services
                    .store
                    .add_workspace_member(&workspace, &principal.id, WorkspaceRole::Collaborator)
                    .await
                    .unwrap();
                let caller = Caller::Wire {
                    principal_id: principal.id.clone(),
                    host_role: HostRole::Guest,
                };
                let state = services
                    .store
                    .read_note_page_state(&workspace, &note, None)
                    .await
                    .unwrap();
                let mut request = NoteApplySplices {
                    backend_id: state["scope"]["backendId"].as_str().unwrap().into(),
                    workspace_id: workspace.0.clone(),
                    note_id: note.0.clone(),
                    note_instance_id: state["scope"]["noteInstanceId"].as_str().unwrap().into(),
                    base_revision: state["sourceRevision"].as_str().unwrap().into(),
                    operation_id: uuid::Uuid::new_v4().to_string(),
                    expires_at: format!("{}.000Z", &intent_core::iso_ms_from_now(60_000)[..19]),
                    payload_digest: String::new(),
                    splices: vec![NoteSplice {
                        start: 0,
                        end: 6,
                        text: "saved".into(),
                    }],
                };
                request.payload_digest = request.computed_digest().unwrap();
                let receipt = with_caller(caller.clone(), services.note_apply_splices(request))
                    .await
                    .unwrap();
                let inverse: String = sqlx::query_scalar(
                    "SELECT value FROM note_operation_item WHERE kind='inverse' AND sequence=0",
                )
                .fetch_one(services.store.read_pool())
                .await
                .unwrap();
                let inverse: Value = serde_json::from_str(&inverse).unwrap();
                let mut params = receipt["scope"].clone();
                params["operationId"] = receipt["operationId"].clone();
                params["payloadDigest"] = receipt["payloadDigest"].clone();
                params["kind"] = json!("inverse");
                params["ref"] = if missing {
                    json!("unknown")
                } else {
                    receipt["inverseRef"].clone()
                };
                let query = serde_json::from_value::<NoteOperationReceiptRead>(params)
                    .unwrap()
                    .query()
                    .unwrap();
                let mut params = receipt["scope"].clone();
                params["sourceRevision"] = receipt["afterRevision"].clone();
                let reference = inverse["provenanceRef"].as_str().unwrap();
                params["page"] = json!({"kind":"context","contextRef":if missing {format!("{}:unknown",reference.split_once(':').unwrap().0)}else{reference.into()}});
                let context_request: NoteGetReceiptContextRequest =
                    serde_json::from_value(params).unwrap();
                let boundary = Arc::new(ReadBoundary {
                    expires: AtomicI64::new(0),
                    now: AtomicI64::new(0),
                    reached: tokio::sync::Notify::new(),
                    release: tokio::sync::Notify::new(),
                });
                let read = READ_BOUNDARY.scope(
                    boundary.clone(),
                    with_caller(caller.clone(), async {
                        if context {
                            services
                                .read_note_receipt_context(context_request.clone(), json!(1))
                                .await
                        } else {
                            services.read_note_receipt(query.clone(), json!(1)).await
                        }
                    }),
                );
                let advance = async {
                    boundary.reached.notified().await;
                    let expiry = boundary.expires.load(Ordering::SeqCst);
                    if missing {
                        assert_eq!(expiry, -1, "Store actually rejected the reference");
                    } else {
                        assert!(
                            expiry > time::OffsetDateTime::now_utc().unix_timestamp(),
                            "Store actually returned a live page"
                        );
                    }
                    // Advance only this request's final observation clock; the
                    // retained receipt and global clocks remain unchanged.
                    boundary.now.store(expiry.max(1), Ordering::SeqCst);
                    if revoke {
                        services
                            .store
                            .remove_workspace_member(&workspace, &principal.id)
                            .await
                            .unwrap();
                    }
                    boundary.release.notify_one();
                };
                let (result, ()) =
                    tokio::time::timeout(std::time::Duration::from_secs(10), async {
                        tokio::join!(read, advance)
                    })
                    .await
                    .unwrap();
                if revoke {
                    assert!(
                        matches!(result, Err(Error::NotFound(_))),
                        "authorization dominates prior page/error"
                    );
                } else if missing {
                    assert!(matches!(
                        result,
                        Err(Error::NotePage(NotePageError::CursorInvalid))
                    ));
                } else {
                    assert!(matches!(
                        result,
                        Err(Error::NotePage(NotePageError::Expired))
                    ));
                }
                if !revoke && !missing {
                    let page = with_caller(caller, async {
                        if context {
                            services
                                .read_note_receipt_context(context_request, json!(1))
                                .await
                        } else {
                            services.read_note_receipt(query, json!(1)).await
                        }
                    })
                    .await
                    .unwrap();
                    assert!(
                        !page["items"].as_array().unwrap().is_empty(),
                        "no deadline renewal or retained-state mutation"
                    );
                }
            }
        }
    }
}
