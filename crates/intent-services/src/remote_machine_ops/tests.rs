use std::sync::Arc;
use std::time::Duration;

use intent_core::{
    with_caller, AgentId, Caller, Error, ExecutorCredentialId, HostRole, PrincipalId,
};
use intent_store::Store;
use serde_json::json;

use crate::settings::InMemorySecretStore;
use crate::tests::TempDb;
use crate::{EventBus, Services, SubscriptionFilter};

struct Fixture {
    _tmp: TempDb,
    _config: tempfile::TempDir,
    store: Store,
    services: Services,
    bus: EventBus,
    member: PrincipalId,
}

async fn fixture() -> Fixture {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let member = intent_core::Principal {
        id: PrincipalId::new(),
        identity: None,
        github_user_id: Some(77),
        login: Some("member".into()),
        display_name: None,
        avatar_url: None,
        is_primary: false,
        created_at: intent_core::now_iso(),
        updated_at: intent_core::now_iso(),
    };
    store.upsert_principal(&member).await.expect("member");
    let config = tempfile::TempDir::new().unwrap();
    let registry = Arc::new(
        crate::SettingsRegistry::load(config.path().join("config.toml")).expect("registry"),
    );
    let bus = EventBus::new(store.clone());
    let services = Services::new(store.clone())
        .with_settings_registry(registry)
        .with_secret_store(Arc::new(InMemorySecretStore::default()))
        .with_event_bus(bus.clone());
    Fixture {
        _tmp: tmp,
        _config: config,
        store,
        services,
        bus,
        member: member.id,
    }
}

#[tokio::test]
async fn only_the_owner_reaches_remote_machines() {
    let f = fixture().await;
    let owner = Caller::Wire {
        principal_id: f.store.get_primary_principal().await.unwrap().id,
        host_role: HostRole::Owner,
    };
    // Without the remote feature the owner reaches the default port.
    for caller in [owner, Caller::Daemon] {
        let result = with_caller(caller, f.services.remote_machines_op("list", json!({}))).await;
        assert!(matches!(result, Err(Error::Unsupported(_))), "{result:?}");
    }
    let refused = [
        Caller::Wire {
            principal_id: f.member.clone(),
            host_role: HostRole::Member,
        },
        Caller::Wire {
            principal_id: f.member.clone(),
            host_role: HostRole::Guest,
        },
        Caller::Agent {
            agent_id: AgentId::new(),
        },
        Caller::Executor {
            credential_id: ExecutorCredentialId::from("c-1".to_owned()),
            backend_id: "backend-1".into(),
        },
        // A connection admitted as owner whose principal no longer is.
        Caller::Wire {
            principal_id: f.member.clone(),
            host_role: HostRole::Owner,
        },
    ];
    for caller in refused {
        for method in ["backendInfo", "add", "list", "remove", "refresh"] {
            let result = with_caller(
                caller.clone(),
                f.services.remote_machines_op(method, json!({})),
            )
            .await;
            assert!(
                matches!(result, Err(Error::Forbidden(_))),
                "{caller:?} {method}: {result:?}"
            );
        }
    }
}

#[tokio::test]
async fn the_token_lives_in_the_secret_store_under_the_machine_account() {
    let f = fixture().await;
    assert_eq!(
        super::remote_machine_token_account("m-1"),
        "remoteMachines.m-1.token"
    );
    assert_eq!(
        f.services.load_remote_machine_token("m-1").await.unwrap(),
        None
    );
    f.services
        .store_remote_machine_token("m-1", "secret-token")
        .await
        .unwrap();
    assert_eq!(
        f.services.load_remote_machine_token("m-1").await.unwrap(),
        Some("secret-token".to_owned())
    );
    f.services.delete_remote_machine_token("m-1").await.unwrap();
    f.services.delete_remote_machine_token("m-1").await.unwrap();
    assert_eq!(
        f.services.load_remote_machine_token("m-1").await.unwrap(),
        None
    );
}

#[tokio::test]
async fn the_experimental_switch_is_the_setting() {
    let f = fixture().await;
    assert!(!f.services.experimental_remote_agents_enabled());
    f.services
        .settings_registry()
        .unwrap()
        .apply(&[("experimental.remoteAgents".to_owned(), json!(true))])
        .unwrap();
    assert!(f.services.experimental_remote_agents_enabled());
}

#[tokio::test]
async fn remote_machine_changed_is_global_and_owner_only() {
    let f = fixture().await;
    let mut sub = f.bus.subscribe(SubscriptionFilter {
        event_types: vec![intent_core::events::REMOTE_MACHINE_CHANGED.to_owned()],
        ..SubscriptionFilter::default()
    });
    f.services
        .publish_remote_machine_changed(json!({ "machineId": "m-1", "removed": true }))
        .await;
    let batch = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(5)),
        sub.recv(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        batch[0].data,
        json!({ "machineId": "m-1", "removed": true })
    );
    assert_eq!(batch[0].workspace_id.as_str(), "", "a global event");
    assert!(!intent_core::events::is_collaborator_event_type(
        intent_core::events::REMOTE_MACHINE_CHANGED
    ));
    assert!(!intent_core::events::is_member_execution_event_type(
        intent_core::events::REMOTE_MACHINE_CHANGED
    ));
}
