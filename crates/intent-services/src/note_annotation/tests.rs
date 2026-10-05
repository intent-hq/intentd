use super::*;
use crate::tests::setup;
use intent_core::{
    note_page::NotePageError, now_iso, with_caller, HostRole, Principal, PrincipalId, WorkspaceRole,
};
use serde_json::json;

async fn guest(service: &Services, workspace: &WorkspaceId) -> Caller {
    let principal = Principal {
        id: PrincipalId::new(),
        identity: None,
        github_user_id: None,
        login: None,
        display_name: None,
        avatar_url: None,
        is_primary: false,
        created_at: now_iso(),
        updated_at: now_iso(),
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

#[tokio::test]
async fn annotation_service_authorizes_current_membership_and_context_identity() {
    let (_tmp, service, workspace, note) = setup("source").await;
    let alice = guest(&service, &workspace).await;
    let bob = guest(&service, &workspace).await;
    let body = "\"\\\n😀".repeat(5000);
    let comment:intent_core::Comment=serde_json::from_value(json!({"id":"root","threadId":"thread","noteId":note,"type":"comment","content":body,"author":"Alice","authorType":"user","status":"open","createdAt":"date","updatedAt":"date"})).unwrap();
    service
        .store
        .insert_comment(&workspace, &comment)
        .await
        .unwrap();
    let state = service
        .store
        .read_note_page_state(&workspace, &note, None)
        .await
        .unwrap();
    let mut params = state["scope"].clone();
    params["sourceRevision"] = state["sourceRevision"].clone();
    params["threadId"] = json!("thread");
    params["page"] = json!({"kind":"replies","maxItems":1,"maxWireBytes":4096});
    let request: AnnotationReadRequest = serde_json::from_value(params.clone()).unwrap();
    assert!(matches!(
        service
            .read_annotation_page(AnnotationMethod::Replies, request.clone(), json!(1))
            .await,
        Err(Error::Forbidden(_))
    ));
    let first = with_caller(
        alice.clone(),
        service.read_annotation_page(AnnotationMethod::Replies, request, json!(1)),
    )
    .await
    .unwrap();
    assert_eq!(first["rootCommentId"], "root");
    assert_eq!(first["rootState"], "present");
    assert!(
        json!({"jsonrpc":"2.0","id":1,"result":first})
            .to_string()
            .len()
            <= 4096
    );
    params.as_object_mut().unwrap().remove("threadId");
    params["commentRevision"] = first["commentRevision"].clone();
    params["page"] =
        json!({"kind":"context","contextRef":first["items"][0]["bodyRef"],"maxWireBytes":4096});
    let context: AnnotationReadRequest = serde_json::from_value(params).unwrap();
    let mut wrong_epoch_kind = context.clone();
    wrong_epoch_kind.attribution_generation = wrong_epoch_kind.comment_revision.take();
    assert!(matches!(
        with_caller(
            alice.clone(),
            service.read_annotation_page(AnnotationMethod::Context, wrong_epoch_kind, json!(1))
        )
        .await,
        Err(Error::InvalidParams(_))
    ));
    let crossed = with_caller(
        bob,
        service.read_annotation_page(AnnotationMethod::Context, context.clone(), json!(1)),
    )
    .await;
    assert!(matches!(
        crossed,
        Err(Error::NotePage(NotePageError::CursorInvalid))
    ));
    let current = with_caller(
        alice.clone(),
        service.read_annotation_page(AnnotationMethod::Context, context.clone(), json!(1)),
    )
    .await
    .unwrap();
    assert_eq!(current["items"][0]["field"], "body");
    assert!(
        json!({"jsonrpc":"2.0","id":1,"result":current})
            .to_string()
            .len()
            <= 4096
    );
    let Caller::Wire { principal_id, .. } = &alice else {
        panic!("wire fixture")
    };
    service
        .store
        .remove_workspace_member(&workspace, principal_id)
        .await
        .unwrap();
    assert!(matches!(
        with_caller(
            alice.clone(),
            service.read_annotation_page(AnnotationMethod::Context, context.clone(), json!(1))
        )
        .await,
        Err(Error::NotFound(_))
    ));
    service
        .store
        .add_workspace_member(&workspace, principal_id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    sqlx::query("UPDATE comment SET status='resolved' WHERE id='root'")
        .execute(service.store.write_pool())
        .await
        .unwrap();
    assert!(matches!(
        with_caller(
            alice,
            service.read_annotation_page(AnnotationMethod::Context, context, json!(1))
        )
        .await,
        Err(Error::NotePage(NotePageError::Stale))
    ));
}

#[tokio::test]
async fn annotation_service_pending_attribution_and_strict_shapes_do_not_fall_back_to_legacy() {
    let (_tmp, service, workspace, note) = setup("source").await;
    let caller = guest(&service, &workspace).await;
    let state = service
        .store
        .read_note_page_state(&workspace, &note, None)
        .await
        .unwrap();
    let mut params = state["scope"].clone();
    params["sourceRevision"] = state["sourceRevision"].clone();
    params["page"] =
        json!({"kind":"attribution","ranges":[{"start":0,"end":2}],"maxWireBytes":4096});
    let pending = with_caller(
        caller.clone(),
        service.read_annotation_page(
            AnnotationMethod::Attribution,
            serde_json::from_value(params.clone()).unwrap(),
            json!(1),
        ),
    )
    .await
    .unwrap();
    assert_eq!(pending["state"], "pending");
    assert_eq!(pending["items"], json!([]));
    assert!(pending["nextCursor"].is_null());
    params["page"]["includeComments"] = json!(true);
    assert!(with_caller(
        caller.clone(),
        service.read_annotation_page(
            AnnotationMethod::Attribution,
            serde_json::from_value(params.clone()).unwrap(),
            json!(1)
        )
    )
    .await
    .is_err());
    params["page"]
        .as_object_mut()
        .unwrap()
        .remove("includeComments");
    params["sourceRevision"] = json!("wrong-source");
    assert!(matches!(
        with_caller(
            caller,
            service.read_annotation_page(
                AnnotationMethod::Attribution,
                serde_json::from_value(params).unwrap(),
                json!(1)
            )
        )
        .await,
        Err(Error::NotePage(NotePageError::Stale))
    ));
}
