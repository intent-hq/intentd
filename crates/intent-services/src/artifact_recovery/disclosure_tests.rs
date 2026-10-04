//! Service unit model: controlled lookup + real `SQLite` membership, not artifact
//! arena/transport execution. Fixture is the unmodified retained Store capture
//! d2f43fb2/2a70fee6, SHA256 9ca87c8e323b760594d390f28438b67311d7a0ddde100905d7827857c83a3612.
use super::*;
use intent_core::note_artifact::response::{JobPhase, JobState};
use serde_json::json;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

fn terminal_fixture() -> (Receipt, Value) {
    let capture: Value =
        serde_json::from_str(include_str!("fixtures/store-recovery.json")).unwrap();
    let result = capture["result"]["afterAbort"].clone();
    assert_eq!(result["kind"], "artifactJobState");
    assert_eq!(result["state"], "aborted");
    assert!(result.get("privateArtifactRef").is_none());
    let text = |field: &str| result[field].as_str().unwrap().to_owned();
    let receipt = Receipt::Job(JobState {
        job_id: text("jobId"),
        job_ref: text("jobRef"),
        header_digest: text("headerDigest"),
        state: JobPhase::Aborted,
        next_sequence: result["nextSequence"].as_u64().unwrap(),
        accepted_bytes: result["acceptedBytes"].as_u64().unwrap(),
        current_digest: text("currentDigest"),
        expires_at: text("expiresAt"),
        status_until: text("statusUntil"),
        reservation: serde_json::from_value(result["reservation"].clone()).unwrap(),
        private_artifact_ref: None,
    });
    assert_eq!(serde_json::to_value(&receipt).unwrap(), result);
    (receipt, result)
}

async fn member(
    services: &Services,
    workspace: &WorkspaceId,
) -> (Caller, intent_core::PrincipalId) {
    let id = intent_core::PrincipalId::new();
    services
        .store
        .upsert_principal(&intent_core::Principal {
            id: id.clone(),
            github_user_id: None,
            login: Some("artifact-recovery-guest".into()),
            display_name: None,
            avatar_url: None,
            is_primary: false,
            created_at: intent_core::now_iso(),
            updated_at: intent_core::now_iso(),
            identity: None,
        })
        .await
        .unwrap();
    services
        .store
        .add_workspace_member(workspace, &id, intent_core::WorkspaceRole::Collaborator)
        .await
        .unwrap();
    (
        Caller::Wire {
            principal_id: id.clone(),
            host_role: intent_core::HostRole::Guest,
        },
        id,
    )
}

#[tokio::test]
async fn artifact_recovery_model_delivers_retained_terminal_dto_at_exact_frame_limit() {
    let (_temp, services, workspace, _) = crate::tests::setup("").await;
    let (caller, id) = member(&services, &workspace).await;
    let (receipt, expected) = terminal_fixture();
    // Expired source does not bar original-owner cleanup/status. This model
    // injects a retained terminal receipt, not a source grant or readable lease.
    assert!(
        intent_core::parse_iso(expected["expiresAt"].as_str().unwrap()).unwrap()
            < intent_core::parse_iso(&intent_core::now_iso()).unwrap()
    );
    let empty_frame = serde_json::to_vec(&json!({"jsonrpc":"2.0","id":"","result":expected}))
        .unwrap()
        .len();
    let padding = 4096 - empty_frame;
    let rpc_id = "\"".repeat(padding / 2) + if padding % 2 == 1 { "a" } else { "" };
    assert_eq!(
        serde_json::to_vec(&json!({"jsonrpc":"2.0","id":rpc_id,"result":expected}))
            .unwrap()
            .len(),
        4096
    );
    let actual = intent_core::with_caller(
        caller.clone(),
        services.prepared_artifact_disclose(
            workspace.clone(),
            json!(rpc_id),
            |principal| async move {
                assert_eq!(principal, format!("principal:{}", id.0));
                Ok(receipt)
            },
        ),
    )
    .await
    .unwrap();
    assert_eq!(actual, expected);
    let (receipt, _) = terminal_fixture();
    let refused = intent_core::with_caller(
        caller,
        services
            .prepared_artifact_disclose(workspace, json!(rpc_id + "a"), |_| async { Ok(receipt) }),
    )
    .await;
    assert!(matches!(
        refused,
        Err(Error::NotePage(
            intent_core::note_page::NotePageError::Budget
        ))
    ));
    services.store.close().await;
}

#[tokio::test]
async fn artifact_recovery_model_rechecks_original_caller_after_held_lookup() {
    let (_temp, services, workspace, _) = crate::tests::setup("").await;
    let (caller, id) = member(&services, &workspace).await;
    let (receipt, _) = terminal_fixture();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let settled = Arc::new(AtomicBool::new(false));
    let observed = settled.clone();
    let pending = services.prepared_artifact_disclose(
        workspace.clone(),
        json!("held"),
        |principal| async move {
            started_tx.send(principal).unwrap();
            release_rx.await.unwrap();
            observed.store(true, Ordering::SeqCst);
            Ok(receipt)
        },
    );
    tokio::pin!(pending);
    let principal = intent_core::with_caller(caller, async {
        tokio::select! {
            result = &mut pending => panic!("disclosed before held lookup settled: {result:?}"),
            started = started_rx => started.unwrap(),
        }
    })
    .await;
    assert_eq!(principal, format!("principal:{}", id.0));
    assert!(!settled.load(Ordering::SeqCst));
    services
        .store
        .remove_workspace_member(&workspace, &id)
        .await
        .unwrap();
    release_tx.send(()).unwrap();
    // Re-poll under a privileged ambient caller: the original guest still owns
    // this operation and its removed membership must govern disclosure.
    let result = intent_core::with_caller(Caller::Daemon, &mut pending).await;
    assert!(settled.load(Ordering::SeqCst));
    assert!(
        matches!(result, Err(Error::NotFound(_) | Error::Forbidden(_))),
        "revoked captured caller received a retained receipt: {result:?}"
    );
    assert!(intent_core::current_caller().is_none());
    services.store.close().await;
}

#[tokio::test]
async fn artifact_recovery_model_cancel_does_not_claim_lookup_settlement() {
    let (_temp, services, workspace, _) = crate::tests::setup("").await;
    let (caller, _) = member(&services, &workspace).await;
    let (receipt, _) = terminal_fixture();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let (settled_tx, settled_rx) = tokio::sync::oneshot::channel();
    // An independently owned test operation models physical lookup settlement;
    // dropping the caller future must not be mistaken for that event.
    let operation = tokio::spawn(async move {
        release_rx.await.unwrap();
        settled_tx.send(()).unwrap();
        receipt
    });
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let mut pending =
        Box::pin(
            services.prepared_artifact_disclose(workspace, json!("cancel"), |_| async move {
                started_tx.send(()).unwrap();
                Ok(operation.await.unwrap())
            }),
        );
    intent_core::with_caller(caller, async {
        tokio::select! {
            result = &mut pending => panic!("disclosed before model lookup settled: {result:?}"),
            started = started_rx => started.unwrap(),
        }
    })
    .await;
    drop(pending);
    let mut settled_rx = settled_rx;
    assert!(matches!(
        settled_rx.try_recv(),
        Err(tokio::sync::oneshot::error::TryRecvError::Empty)
    ));
    release_tx.send(()).unwrap();
    settled_rx.await.unwrap();
    services.store.close().await;
}

#[tokio::test]
async fn artifact_recovery_model_rechecks_before_lookup_error_disclosure() {
    let (_temp, services, workspace, _) = crate::tests::setup("").await;
    let (caller, id) = member(&services, &workspace).await;
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let settled = Arc::new(AtomicBool::new(false));
    let observed = settled.clone();
    let pending = services.prepared_artifact_disclose(
        workspace.clone(),
        json!("held-error"),
        |principal| async move {
            started_tx.send(principal).unwrap();
            release_rx.await.unwrap();
            observed.store(true, Ordering::SeqCst);
            Err(Error::Internal("held artifact identity mismatch".into()))
        },
    );
    tokio::pin!(pending);
    let principal = intent_core::with_caller(caller, async {
        tokio::select! {
            result = &mut pending => panic!("disclosed before held lookup settled: {result:?}"),
            started = started_rx => started.unwrap(),
        }
    })
    .await;
    assert_eq!(principal, format!("principal:{}", id.0));
    assert!(!settled.load(Ordering::SeqCst));
    services
        .store
        .remove_workspace_member(&workspace, &id)
        .await
        .unwrap();
    release_tx.send(()).unwrap();
    let result = intent_core::with_caller(Caller::Daemon, &mut pending).await;
    assert!(settled.load(Ordering::SeqCst));
    assert!(
        matches!(result, Err(Error::NotFound(_) | Error::Forbidden(_))),
        "revoked captured caller received lookup outcome: {result:?}"
    );
    services.store.close().await;
}

#[tokio::test]
async fn artifact_recovery_model_preserves_authorized_lookup_error() {
    let (_temp, services, workspace, _) = crate::tests::setup("").await;
    let (caller, id) = member(&services, &workspace).await;
    let result = intent_core::with_caller(
        caller,
        services.prepared_artifact_disclose(
            workspace,
            json!("authorized-error"),
            |principal| async move {
                assert_eq!(principal, format!("principal:{}", id.0));
                Err(Error::Internal("held artifact identity mismatch".into()))
            },
        ),
    )
    .await;
    assert!(
        matches!(result, Err(Error::Internal(message)) if message == "held artifact identity mismatch")
    );
    services.store.close().await;
}
