use super::*;
use intent_core::{
    node_link::{MachineCapabilities, ProviderCapability, ProviderStatus},
    nodes::{NodeArch, NodeOs},
};

async fn open() -> (tempfile::TempDir, Store) {
    let dir = tempfile::tempdir().unwrap();
    let store = Store::open(&dir.path().join("executor.db")).await.unwrap();
    (dir, store)
}

fn credential(id: &str, backend: &str, created_at: &str) -> ExecutorCredential {
    ExecutorCredential {
        id: ExecutorCredentialId::from(id.to_string()),
        backend_id: backend.into(),
        backend_name: format!("{backend} name"),
        backend_fingerprint: Some(format!("{backend}-fp")),
        created_at: created_at.into(),
        last_used_at: None,
        revoked_at: None,
    }
}

fn binding(head: &str, node: &str, generation: u64) -> LeaseBinding {
    LeaseBinding {
        head_id: head.into(),
        node_id: node.into(),
        lease_id: format!("lease-{head}"),
        generation: NodeCounter(generation),
    }
}

fn id(value: &str) -> ExecutorCredentialId {
    ExecutorCredentialId::from(value.to_string())
}

#[tokio::test]
async fn executor_credentials_resolve_list_and_revoke_by_hash() {
    let (_dir, store) = open().await;
    let first = credential("cred-1", "backend-a", "2026-10-30T00:00:01Z");
    let again = credential("cred-2", "backend-a", "2026-10-30T00:00:02Z");
    let other = credential("cred-3", "backend-b", "2026-10-30T00:00:03Z");
    store
        .insert_executor_credential(&first, "hash-1")
        .await
        .unwrap();
    store
        .insert_executor_credential(&again, "hash-2")
        .await
        .unwrap();
    store
        .insert_executor_credential(&other, "hash-3")
        .await
        .unwrap();

    assert!(
        store
            .insert_executor_credential(&credential("cred-4", "b", "t"), "hash-1")
            .await
            .is_err(),
        "a token hash admits exactly one credential"
    );
    let mut empty = credential("cred-5", "", "t");
    assert!(matches!(
        store.insert_executor_credential(&empty, "hash-5").await,
        Err(Error::InvalidParams(_))
    ));
    empty.backend_id = "backend".into();
    assert!(matches!(
        store.insert_executor_credential(&empty, "").await,
        Err(Error::InvalidParams(_))
    ));

    let resolved = store
        .resolve_active_executor_credential("hash-1")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resolved.id, first.id);
    assert_eq!(resolved.backend_id, "backend-a");
    assert!(resolved.last_used_at.is_some(), "admission stamps last use");
    assert!(store
        .resolve_active_executor_credential("nope")
        .await
        .unwrap()
        .is_none());

    let listed: Vec<_> = store
        .list_active_executor_credentials()
        .await
        .unwrap()
        .into_iter()
        .map(|c| c.id)
        .collect();
    assert_eq!(
        listed,
        vec![first.id.clone(), again.id.clone(), other.id.clone()]
    );

    assert_eq!(
        store.revoke_executor_credential(&first.id).await.unwrap(),
        ExecutorRevocation::Revoked
    );
    assert_eq!(
        store.revoke_executor_credential(&first.id).await.unwrap(),
        ExecutorRevocation::AlreadyRevoked
    );
    assert_eq!(
        store
            .revoke_executor_credential(&id("unknown"))
            .await
            .unwrap(),
        ExecutorRevocation::NotFound
    );
    assert!(
        store
            .resolve_active_executor_credential("hash-1")
            .await
            .unwrap()
            .is_none(),
        "a revoked credential no longer admits"
    );
    let kept = store
        .get_executor_credential(&first.id)
        .await
        .unwrap()
        .unwrap();
    assert!(!kept.is_active());
    let listed: Vec<_> = store
        .list_active_executor_credentials()
        .await
        .unwrap()
        .into_iter()
        .map(|c| c.id)
        .collect();
    assert_eq!(
        listed,
        vec![again.id, other.id],
        "list shows unrevoked only"
    );
}

#[tokio::test]
async fn executor_binding_is_kept_per_credential_and_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("executor.db");
    let store = Store::open(&path).await.unwrap();
    let a = credential("cred-a", "backend-a", "2026-10-30T00:00:01Z");
    let b = credential("cred-b", "backend-b", "2026-10-30T00:00:02Z");
    store
        .insert_executor_credential(&a, "hash-a")
        .await
        .unwrap();
    store
        .insert_executor_credential(&b, "hash-b")
        .await
        .unwrap();
    assert_eq!(store.executor_binding(&a.id).await.unwrap(), None);

    let bind = |cred: &ExecutorCredentialId, offer: LeaseBinding| {
        let store = store.clone();
        let cred = cred.clone();
        async move { store.bind_executor(&cred, "machine", &offer).await.unwrap() }
    };
    assert_eq!(
        bind(&a.id, binding("backend-a", "machine", 1)).await,
        BindDecision::Accept
    );
    assert_eq!(
        bind(&a.id, binding("backend-a", "machine", 1)).await,
        BindDecision::Stale {
            current: NodeCounter(1)
        }
    );
    assert_eq!(
        bind(&a.id, binding("backend-a", "other-machine", 9)).await,
        BindDecision::WrongNode
    );
    assert_eq!(
        bind(&a.id, binding("backend-b", "machine", 9)).await,
        BindDecision::OtherHead,
        "a credential only binds the backend it was minted for"
    );
    assert_eq!(
        bind(&b.id, binding("backend-b", "machine", 1)).await,
        BindDecision::Accept,
        "another backend binds independently"
    );
    assert_eq!(
        bind(&a.id, binding("backend-a", "machine", 2)).await,
        BindDecision::Accept
    );
    assert_eq!(
        store.executor_binding(&a.id).await.unwrap(),
        Some(binding("backend-a", "machine", 2))
    );
    let huge = binding("backend-b", "machine", u64::MAX);
    assert_eq!(bind(&b.id, huge.clone()).await, BindDecision::Accept);
    drop(store);

    let store = Store::open(&path).await.unwrap();
    assert_eq!(
        store.executor_binding(&a.id).await.unwrap(),
        Some(binding("backend-a", "machine", 2)),
        "the binding is durable"
    );
    assert_eq!(store.executor_binding(&b.id).await.unwrap(), Some(huge));
    assert_eq!(
        store
            .bind_executor(&a.id, "machine", &binding("backend-a", "machine", 2))
            .await
            .unwrap(),
        BindDecision::Stale {
            current: NodeCounter(2)
        },
        "a restart never accepts an older or equal generation"
    );

    store.revoke_executor_credential(&a.id).await.unwrap();
    assert!(matches!(
        store
            .bind_executor(&a.id, "machine", &binding("backend-a", "machine", 3))
            .await,
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        store
            .bind_executor(
                &id("unknown"),
                "machine",
                &binding("backend-a", "machine", 3)
            )
            .await,
        Err(Error::NotFound(_))
    ));
    assert_eq!(
        store.executor_binding(&a.id).await.unwrap(),
        Some(binding("backend-a", "machine", 2)),
        "a refused bind changes nothing"
    );
}

fn machine(id: &str, credential: &str, added_at: &str) -> ExposedMachine {
    ExposedMachine {
        id: id.into(),
        name: format!("{id} name"),
        hosts: vec!["192.168.1.10".into(), "mac.local".into()],
        port: 7443,
        tc_address: Some("tc-abc".into()),
        fingerprint: "ab".repeat(32),
        credential_id: ExecutorCredentialId::from(credential.to_string()),
        platform: None,
        capabilities: None,
        observed_at: None,
        state: RemoteMachineState::Disconnected,
        added_at: added_at.into(),
        last_connected_at: None,
    }
}

#[tokio::test]
async fn exposed_machines_replace_by_id_and_report_the_replaced_credential() {
    let (_dir, store) = open().await;
    let mac = machine("mac", "cred-1", "2026-10-30T00:00:02Z");
    let linux = machine("linux", "cred-9", "2026-10-30T00:00:01Z");
    assert_eq!(store.save_exposed_machine(&mac).await.unwrap(), None);
    assert_eq!(store.save_exposed_machine(&linux).await.unwrap(), None);
    assert_eq!(
        store.get_exposed_machine("mac").await.unwrap(),
        Some(mac.clone())
    );
    assert_eq!(store.get_exposed_machine("absent").await.unwrap(), None);
    let ids: Vec<_> = store
        .list_exposed_machines()
        .await
        .unwrap()
        .into_iter()
        .map(|m| m.id)
        .collect();
    assert_eq!(ids, vec!["linux".to_string(), "mac".to_string()]);

    assert_eq!(
        store.save_exposed_machine(&mac).await.unwrap(),
        None,
        "same credential: nothing to revoke"
    );
    let mut re_added = machine("mac", "cred-2", "2026-10-30T00:00:03Z");
    re_added.tc_address = None;
    assert_eq!(
        store.save_exposed_machine(&re_added).await.unwrap(),
        Some(id("cred-1"))
    );
    assert_eq!(
        store.get_exposed_machine("mac").await.unwrap(),
        Some(re_added)
    );

    let mut bad = machine("bad", "cred-3", "t");
    bad.state = RemoteMachineState::Connected;
    assert!(matches!(
        store.save_exposed_machine(&bad).await,
        Err(Error::InvalidParams(_))
    ));
    bad.state = RemoteMachineState::Disconnected;
    bad.hosts.clear();
    bad.tc_address = None;
    assert!(matches!(
        store.save_exposed_machine(&bad).await,
        Err(Error::InvalidParams(_))
    ));
    // A loopback-only machine is reachable over its tailcat address alone.
    let mut tailcat_only = machine("tc-only", "cred-4", "t");
    tailcat_only.hosts.clear();
    assert_eq!(
        store.save_exposed_machine(&tailcat_only).await.unwrap(),
        None
    );
    assert_eq!(
        store.get_exposed_machine("tc-only").await.unwrap(),
        Some(tailcat_only)
    );
    assert_eq!(
        store.remove_exposed_machine("tc-only").await.unwrap(),
        Some(id("cred-4"))
    );
    bad.hosts = vec!["h".into()];
    bad.port = 0;
    assert!(matches!(
        store.save_exposed_machine(&bad).await,
        Err(Error::InvalidParams(_))
    ));
    assert_eq!(store.get_exposed_machine("bad").await.unwrap(), None);

    assert_eq!(
        store.remove_exposed_machine("mac").await.unwrap(),
        Some(id("cred-2"))
    );
    assert_eq!(store.remove_exposed_machine("mac").await.unwrap(), None);
    assert_eq!(store.list_exposed_machines().await.unwrap().len(), 1);
}

#[tokio::test]
async fn exposed_machine_state_routes_and_capabilities_are_cached() {
    let (_dir, store) = open().await;
    store
        .save_exposed_machine(&machine("mac", "cred-1", "2026-10-30T00:00:01Z"))
        .await
        .unwrap();

    assert!(store
        .set_exposed_machine_state("mac", RemoteMachineState::Revoked)
        .await
        .unwrap());
    assert!(!store
        .set_exposed_machine_state("absent", RemoteMachineState::Unreachable)
        .await
        .unwrap());
    assert!(matches!(
        store
            .set_exposed_machine_state("mac", RemoteMachineState::Connected)
            .await,
        Err(Error::InvalidParams(_))
    ));

    let hosts = vec!["10.0.0.5".to_string()];
    assert!(store
        .record_exposed_machine_connection("mac", &hosts, None, "2026-10-30T01:00:00Z")
        .await
        .unwrap());
    assert!(!store
        .record_exposed_machine_connection("absent", &hosts, None, "t")
        .await
        .unwrap());
    assert!(matches!(
        store
            .record_exposed_machine_connection("mac", &[], None, "t")
            .await,
        Err(Error::InvalidParams(_))
    ));
    assert!(store
        .record_exposed_machine_connection("mac", &[], Some("tc-abc"), "t")
        .await
        .unwrap());
    assert!(store
        .record_exposed_machine_connection("mac", &hosts, None, "2026-10-30T01:00:00Z")
        .await
        .unwrap());

    let platform = AgentPlatform {
        os: NodeOs::Macos,
        arch: NodeArch::Aarch64,
    };
    let mut report = MachineCapabilities::new(platform, "0.12.0", 4, "2026-10-30T01:00:01Z");
    report.providers.push(ProviderCapability {
        id: "claude".into(),
        visible: true,
        status: ProviderStatus::SignedIn,
        models: vec!["opus".into(), "sonnet".into()],
        models_stale: false,
    });
    assert!(store
        .record_exposed_machine_capabilities("mac", &report)
        .await
        .unwrap());
    assert!(!store
        .record_exposed_machine_capabilities("absent", &report)
        .await
        .unwrap());

    let saved = store.get_exposed_machine("mac").await.unwrap().unwrap();
    assert_eq!(saved.state, RemoteMachineState::Revoked);
    assert_eq!(saved.hosts, hosts);
    assert_eq!(saved.tc_address, None);
    assert_eq!(
        saved.last_connected_at.as_deref(),
        Some("2026-10-30T01:00:00Z")
    );
    assert_eq!(saved.platform, Some(platform));
    assert_eq!(saved.observed_at.as_deref(), Some("2026-10-30T01:00:01Z"));
    assert_eq!(saved.capabilities, Some(report));
    assert_eq!(
        saved.credential_id,
        id("cred-1"),
        "routes never move the credential"
    );
}

#[tokio::test]
async fn daemon_id_is_minted_once_and_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("executor.db");
    let store = Store::open(&path).await.unwrap();
    let (a, b) = tokio::join!(store.daemon_id(), store.daemon_id());
    let first = a.unwrap();
    assert_eq!(b.unwrap(), first, "concurrent first reads agree");
    assert!(!first.is_empty());
    assert_eq!(store.daemon_id().await.unwrap(), first);
    drop(store);
    let store = Store::open(&path).await.unwrap();
    assert_eq!(store.daemon_id().await.unwrap(), first);

    let (_other_dir, other) = open().await;
    assert_ne!(
        other.daemon_id().await.unwrap(),
        first,
        "each data dir has its own id"
    );
}
