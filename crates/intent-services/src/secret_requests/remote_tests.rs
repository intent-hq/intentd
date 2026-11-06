//! A remote agent's `secret.provide`, `secret.list` and `secret.forget`
//! (monorepo `docs/protocol/methods/secrets.md` § Remote agents): the value
//! goes only to its machine, the request is answered only once the machine
//! holds it, and the backend never keeps a copy.
use std::sync::{Arc, Mutex};

use intent_core::remote_launch::{RemotePlacementView, REMOTE_PLACEMENT_METADATA_KEY};
use intent_core::{now_iso, with_caller, AgentId, BoxFuture, Caller, Error};
use zeroize::Zeroizing;

use super::{Outcome, CODE_DELIVERY_FAILED};
use crate::feature_ports::{MachineSecretAnswer, MachineSecretOp, RemoteLaunchPort};
use crate::remote_skills::tests::{session, view};
use crate::tests::setup;
use crate::Services;

const VALUE: &str = "canary-remote-provide";

/// A machine that answers each `Put` with the next scripted answer, each
/// `List` and `Forget` with a fixed one, and records what it was sent (names
/// only; a value is checked, not kept).
struct SecretsPort {
    puts: Mutex<Vec<MachineSecretAnswer>>,
    list: MachineSecretAnswer,
    forget: MachineSecretAnswer,
    sent: Mutex<Vec<String>>,
}

impl RemoteLaunchPort for SecretsPort {
    fn placements<'a>(
        &'a self,
        _svc: &'a Services,
    ) -> BoxFuture<'a, intent_core::Result<intent_core::placement::PlatformAvailability>> {
        Box::pin(async { Ok(intent_core::placement::PlatformAvailability::none(now_iso())) })
    }
    fn placement_help<'a>(&'a self, _svc: &'a Services) -> BoxFuture<'a, Option<String>> {
        Box::pin(async { None })
    }
    fn admit<'a>(
        &'a self,
        _svc: &'a Services,
        _workspace_id: &'a intent_core::WorkspaceId,
        _placement: &'a intent_core::placement::AgentPlacement,
        _needs: &'a intent_core::remote_launch::RemoteLaunchNeeds,
    ) -> BoxFuture<'a, intent_core::Result<RemotePlacementView>> {
        Box::pin(async { Ok(view(None)) })
    }
    fn launch<'a>(
        &'a self,
        _svc: &'a Services,
        _request: intent_core::remote_launch::RemoteLaunchRequest,
    ) -> BoxFuture<'a, intent_core::Result<()>> {
        Box::pin(async { Ok(()) })
    }
    fn runtime<'a>(
        &'a self,
        _svc: &'a Services,
        _agent_id: &'a AgentId,
    ) -> BoxFuture<'a, intent_core::Result<Arc<crate::feature_ports::RemoteRuntime>>> {
        Box::pin(async { Err(Error::InvalidParams("no runtime".into())) })
    }
    fn machine_secret<'a>(
        &'a self,
        _svc: &'a Services,
        _agent_id: &'a AgentId,
        op: MachineSecretOp,
    ) -> BoxFuture<'a, MachineSecretAnswer> {
        let (sent, answer) = match &op {
            MachineSecretOp::Put { name, value } => {
                assert_eq!(value.as_str(), VALUE);
                (format!("put {name}"), self.puts.lock().unwrap().remove(0))
            }
            MachineSecretOp::Forget { name } => (format!("forget {name:?}"), self.forget.clone()),
            MachineSecretOp::List => ("list".to_owned(), self.list.clone()),
        };
        self.sent.lock().unwrap().push(sent);
        Box::pin(async move { answer })
    }
}

async fn fixture(
    puts: Vec<MachineSecretAnswer>,
    list: MachineSecretAnswer,
    forget: MachineSecretAnswer,
) -> (
    crate::tests::TempDb,
    Services,
    intent_core::WorkspaceId,
    AgentId,
    Arc<SecretsPort>,
) {
    let (tmp, svc, ws, _) = setup("").await;
    let port = Arc::new(SecretsPort {
        puts: Mutex::new(puts),
        list,
        forget,
        sent: Mutex::new(Vec::new()),
    });
    let svc = svc.with_remote_launch_port(port.clone());
    let id = AgentId::from("remote");
    svc.store
        .insert_agent_session(&session(&ws, &id))
        .await
        .unwrap();
    svc.store
        .set_agent_session_metadata_key_json(
            &ws,
            &id,
            REMOTE_PLACEMENT_METADATA_KEY,
            &serde_json::to_string(&view(None)).unwrap(),
            &now_iso(),
        )
        .await
        .unwrap();
    (tmp, svc, ws, id, port)
}

async fn provide(
    svc: &Services,
    ws: &intent_core::WorkspaceId,
    request_id: &str,
) -> intent_core::Result<serde_json::Value> {
    with_caller(
        Caller::Daemon,
        svc.secret_answer_op(
            ws.clone(),
            request_id.to_owned(),
            Outcome::Provided,
            Some(Zeroizing::new(VALUE.to_owned())),
        ),
    )
    .await
}

/// An offline machine fails the provide with `secret-delivery-failed`, the
/// name is forgotten there (an unconfirmed value is not kept) and the request
/// stays pending for a retry; once the machine holds the value
/// the request is answered. The backend never holds the value.
#[tokio::test]
async fn provide_answers_only_once_the_machine_holds_the_value() {
    let (_tmp, svc, ws, id, port) = fixture(
        vec![MachineSecretAnswer::Offline, MachineSecretAnswer::Stored],
        MachineSecretAnswer::Names(vec!["API_KEY".into()]),
        MachineSecretAnswer::Queued,
    )
    .await;
    let ticket = svc.secret_vault.request(&ws, &id, "API_KEY").unwrap();

    let failed = provide(&svc, &ws, &ticket.request_id).await.unwrap_err();
    match &failed {
        Error::SecretRequest { code, message } => {
            assert_eq!(*code, CODE_DELIVERY_FAILED);
            assert!(!message.contains(VALUE), "{message}");
        }
        other => panic!("expected secret-delivery-failed, got {other:?}"),
    }
    assert!(
        svc.secret_vault
            .pending_owner(&ws, &ticket.request_id)
            .is_some(),
        "the request stays pending"
    );
    assert!(svc.secret_vault.resolve(&ws, &id, &["API_KEY"]).is_err());

    let answered = provide(&svc, &ws, &ticket.request_id).await.unwrap();
    assert_eq!(answered["name"], "API_KEY", "{answered}");
    assert!(svc
        .secret_vault
        .pending_owner(&ws, &ticket.request_id)
        .is_none());
    assert!(
        svc.secret_vault.resolve(&ws, &id, &["API_KEY"]).is_err(),
        "the backend keeps no copy"
    );

    let listed = with_caller(Caller::Daemon, svc.secret_list_op(ws.clone(), id.clone()))
        .await
        .unwrap();
    assert_eq!(
        listed["secrets"],
        serde_json::json!([{ "name": "API_KEY" }])
    );
    assert_eq!(
        listed["answeredRequests"][0]["outcome"], "provided",
        "{listed}"
    );
    assert!(!listed.to_string().contains(VALUE));
    let forgot = "forget Some(\"API_KEY\")".to_owned();
    for _ in 0..1000 {
        if port.sent.lock().unwrap().contains(&forgot) {
            break;
        }
        tokio::task::yield_now().await;
    }
    let mut sent = port.sent.lock().unwrap().clone();
    sent.sort();
    assert_eq!(
        sent,
        [forgot.as_str(), "list", "put API_KEY", "put API_KEY"]
    );
}

/// With the machine away, `secret.list` reports `secretsUnavailable` and
/// `secret.forget` is deferred.
#[tokio::test]
async fn list_and_forget_without_the_machine() {
    let (_tmp, svc, ws, id, _port) = fixture(
        Vec::new(),
        MachineSecretAnswer::Offline,
        MachineSecretAnswer::Queued,
    )
    .await;
    let listed = with_caller(Caller::Daemon, svc.secret_list_op(ws.clone(), id.clone()))
        .await
        .unwrap();
    assert_eq!(listed["secrets"], serde_json::json!([]));
    assert_eq!(listed["secretsUnavailable"], "machine-offline");
    let forgot = with_caller(
        Caller::Daemon,
        svc.secret_forget_op(ws.clone(), id.clone(), "API_KEY".into()),
    )
    .await
    .unwrap();
    assert_eq!(
        forgot,
        serde_json::json!({ "ok": true, "forgotten": false, "deferred": true })
    );
}
