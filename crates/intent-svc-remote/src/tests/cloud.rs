//! `cloudMachines.pause` and `kill` admit only the owner and the principal
//! that started the machine.

use intent_core::machines::{
    CloudMachine, CloudMachineSize, CloudSandboxState, CloudStartedBy, ExposedMachine,
    RemoteMachineState,
};
use intent_core::{Error, HostRole, PrincipalId};

use super::*;

/// A non-primary principal; a host member when `member`, else a guest.
async fn principal(backend: &Backend, member: bool) -> PrincipalId {
    let mut person = backend.store().get_primary_principal().await.unwrap();
    person.id = PrincipalId::new();
    person.is_primary = false;
    backend.store().upsert_principal(&person).await.unwrap();
    if member {
        sqlx::query("INSERT INTO host_member (principal_id,added_at) VALUES (?,?)")
            .bind(person.id.as_str())
            .bind(intent_core::now_iso())
            .execute(backend.store().write_pool())
            .await
            .unwrap();
    }
    person.id
}

/// A cloud machine row started by `starter`.
async fn cloud_row(backend: &Backend, id: &str, starter: &PrincipalId) {
    backend
        .store()
        .save_exposed_machine(&ExposedMachine {
            id: id.to_owned(),
            name: "E2B · Worker".into(),
            hosts: vec![],
            port: 7443,
            tc_address: Some(format!("tc-{id}")),
            fingerprint: "ab".repeat(32),
            credential_id: intent_core::ExecutorCredentialId::from(format!("cred-{id}")),
            platform: None,
            capabilities: None,
            observed_at: None,
            state: RemoteMachineState::Disconnected,
            added_at: intent_core::now_iso(),
            last_connected_at: None,
            node_protocol: None,
            cloud: Some(CloudMachine {
                provider: "e2b".into(),
                sandbox_id: format!("sbx-{id}"),
                state: CloudSandboxState::Running,
                template: "intent-hq/intent-node-4x8:0".into(),
                size: CloudMachineSize::S4x8,
                started_by: CloudStartedBy {
                    principal_id: starter.to_string(),
                    display_name: None,
                },
                agent_id: "agent-1".into(),
                workspace_id: "ws-1".into(),
                started_at: intent_core::now_iso(),
                running_seconds: 0,
                running_since: Some(intent_core::now_iso()),
                paused_at: None,
            }),
        })
        .await
        .unwrap();
}

async fn call_as(
    backend: &Backend,
    who: &PrincipalId,
    role: HostRole,
    method: &'static str,
    machine_id: &str,
) -> intent_core::Result<Value> {
    with_caller(
        Caller::Wire {
            principal_id: who.clone(),
            host_role: role,
        },
        backend
            .services
            .cloud_machines(method, json!({ "machineId": machine_id })),
    )
    .await
}

#[tokio::test]
async fn pause_and_kill_admit_only_the_owner_and_the_starter() {
    let backend = Backend::new().await;
    let starter = principal(&backend, true).await;
    let other = principal(&backend, true).await;
    let guest = principal(&backend, false).await;
    cloud_row(&backend, "m-1", &starter).await;

    for method in ["pause", "kill"] {
        let refused = call_as(&backend, &other, HostRole::Member, method, "m-1").await;
        assert!(
            matches!(refused, Err(Error::Forbidden(_))),
            "another member may not {method}: {refused:?}"
        );
        let refused = call_as(&backend, &guest, HostRole::Guest, method, "m-1").await;
        assert!(
            matches!(refused, Err(Error::Forbidden(_))),
            "a guest may not {method}: {refused:?}"
        );
    }
    assert!(
        backend
            .store()
            .get_exposed_machine("m-1")
            .await
            .unwrap()
            .is_some(),
        "a refused kill removes nothing"
    );

    // The starter passes the check: pause then fails only because E2B is
    // not configured here, and kill removes the machine.
    let paused = call_as(&backend, &starter, HostRole::Member, "pause", "m-1").await;
    match paused {
        Err(Error::RemoteMachine(failure)) => {
            assert_eq!(failure.data["reason"], "not-configured", "{failure:?}");
        }
        other => panic!("the starter's pause reaches E2B: {other:?}"),
    }
    let killed = call_as(&backend, &starter, HostRole::Member, "kill", "m-1")
        .await
        .expect("the starter may kill");
    assert_eq!(killed["ok"], true);
    assert!(backend
        .store()
        .get_exposed_machine("m-1")
        .await
        .unwrap()
        .is_none());

    // The owner may act on any machine; an unknown one is not-found.
    let owner = backend.store().get_primary_principal().await.unwrap().id;
    cloud_row(&backend, "m-2", &starter).await;
    match call_as(&backend, &owner, HostRole::Owner, "pause", "m-2").await {
        Err(Error::RemoteMachine(failure)) => {
            assert_eq!(failure.data["reason"], "not-configured", "{failure:?}");
        }
        other => panic!("the owner's pause reaches E2B: {other:?}"),
    }
    call_as(&backend, &owner, HostRole::Owner, "kill", "m-2")
        .await
        .expect("the owner may kill");
    let unknown = call_as(&backend, &owner, HostRole::Owner, "pause", "m-2").await;
    match unknown {
        Err(Error::RemoteMachine(failure)) => assert_eq!(failure.data["code"], "not-found"),
        other => panic!("an unknown machine is not-found: {other:?}"),
    }
}
