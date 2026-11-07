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
                cloud_machine_id: "e2b".into(),
                sandbox_id: format!("sbx-{id}"),
                state: CloudSandboxState::Running,
                template: "intent/intent-node-4x8:0".into(),
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

mod entries {
    //! Named E2B entries: the migration of a single provider, which entry
    //! a placement picks, and the `configure` refusals that come before any
    //! E2B call.

    use intent_core::remote_launch::{RemotePlacementView, REMOTE_PLACEMENT_METADATA_KEY};
    use intent_core::{AgentId, Workspace};

    use super::*;
    use crate::cloud::config::{self, ProviderConfig};

    /// A cloud agent whose placement names `entry` (none when `None`).
    async fn cloud_agent(backend: &Backend, entry: Option<&str>) -> AgentId {
        let store = backend.store();
        let now = intent_core::now_iso();
        // Already there for the second agent.
        let _ = store
            .insert_workspace(&Workspace {
                id: workspace(),
                title: "Cloud".into(),
                branch: "main".into(),
                created_at: now.clone(),
                updated_at: now.clone(),
                ..Default::default()
            })
            .await;
        let agent = AgentId::new();
        sqlx::query("INSERT INTO agent_session (id, workspace_id, name, status, provider, created_at, updated_at) VALUES (?, ?, 'cloud', 'idle', 'codex', ?, ?)")
            .bind(agent.as_str())
            .bind(workspace().as_str())
            .bind(&now)
            .bind(&now)
            .execute(store.write_pool())
            .await
            .unwrap();
        let mut placement = json!({ "target": "e2b", "checkout": "isolated" });
        if let Some(entry) = entry {
            placement["cloudMachineId"] = json!(entry);
        }
        let view = json!({
            "nodeId": "", "leaseId": "", "nodeName": "E2B", "placement": placement,
            "effectiveIsolation": "pending", "baseCommit": "",
        });
        store
            .set_agent_session_metadata_key_json(
                &workspace(),
                &agent,
                REMOTE_PLACEMENT_METADATA_KEY,
                &view.to_string(),
                &now,
            )
            .await
            .unwrap();
        agent
    }

    async fn entry_of(backend: &Backend, agent: &AgentId) -> Option<String> {
        let session = backend
            .store()
            .get_agent_session_summary(agent)
            .await
            .unwrap();
        RemotePlacementView::from_metadata(session.metadata.as_ref())
            .unwrap()
            .placement
            .cloud_machine_id
    }

    async fn save_entry(backend: &Backend, id: &str, name: &str) {
        config::save(&backend.services, id, &ProviderConfig::new(name))
            .await
            .unwrap();
    }

    fn reason(result: intent_core::Result<impl std::fmt::Debug>) -> Value {
        match result {
            Err(Error::RemoteMachine(failure)) => failure.data,
            other => panic!("expected a cloud error: {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_saved_single_provider_becomes_the_entry_e2b() {
        let backend = Backend::new().await;
        let store = backend.store();
        // As saved before entries: no name on the settings, no entry on the
        // sandbox row or the agents.
        store
            .save_cloud_machine_provider(
                "e2b",
                &json!({
                    "templateProject": "intent", "size": "4x8", "idlePauseMinutes": 15,
                    "pausedDeleteDays": 7, "configuredAt": "2026-01-01T00:00:00Z",
                })
                .to_string(),
            )
            .await
            .unwrap();
        let starter = principal(&backend, true).await;
        cloud_row(&backend, "m-1", &starter).await;
        sqlx::query(
            "UPDATE exposed_machine SET cloud_json = json_remove(cloud_json, '$.cloudMachineId')",
        )
        .execute(store.write_pool())
        .await
        .unwrap();
        let old = cloud_agent(&backend, None).await;
        let other = cloud_agent(&backend, Some("e2b-0123456789ab")).await;

        config::migrate(&backend.services).await.unwrap();

        let entries = config::load_all(&backend.services).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, "e2b");
        assert_eq!(entries[0].1.name, "E2B");
        assert_eq!(entries[0].1.idle_pause_minutes, 15, "settings kept");
        let raw: String = sqlx::query_scalar("SELECT cloud_json FROM exposed_machine")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&raw).unwrap()["cloudMachineId"],
            "e2b"
        );
        assert_eq!(entry_of(&backend, &old).await.as_deref(), Some("e2b"));
        assert_eq!(
            entry_of(&backend, &other).await.as_deref(),
            Some("e2b-0123456789ab"),
            "an agent that names an entry keeps it"
        );

        // Once migrated, an agent without an entry (its entry was removed)
        // stays without one.
        let cleared = cloud_agent(&backend, None).await;
        config::migrate(&backend.services).await.unwrap();
        assert_eq!(entry_of(&backend, &cleared).await, None);
    }

    #[tokio::test]
    async fn a_placement_picks_its_entry_in_the_documented_order() {
        let backend = Backend::new().await;
        let svc = &backend.services;
        let none = config::resolve(svc, None, None).await;
        assert_eq!(reason(none)["reason"], "not-configured");
        let unknown = reason(config::resolve(svc, Some("e2b-ffffffffffff"), None).await);
        assert_eq!(unknown["reason"], "not-configured");
        assert_eq!(unknown["cloudMachineId"], "e2b-ffffffffffff");

        save_entry(&backend, "e2b-aaaaaaaaaaaa", "Alpha").await;
        let (only, _) = config::resolve(svc, None, None).await.unwrap();
        assert_eq!(only, "e2b-aaaaaaaaaaaa", "the only entry");

        save_entry(&backend, "e2b-bbbbbbbbbbbb", "Beta").await;
        let required = reason(config::resolve(svc, None, None).await);
        assert_eq!(required["reason"], "entry-required");
        assert_eq!(required["retryable"], false);
        assert_eq!(
            required["entries"],
            json!([
                { "cloudMachineId": "e2b-aaaaaaaaaaaa", "name": "Alpha" },
                { "cloudMachineId": "e2b-bbbbbbbbbbbb", "name": "Beta" },
            ])
        );
        let (chained, _) = config::resolve(svc, None, Some("e2b-bbbbbbbbbbbb"))
            .await
            .unwrap();
        assert_eq!(chained, "e2b-bbbbbbbbbbbb", "the creating chain's entry");
        let gone = reason(config::resolve(svc, None, Some("e2b-cccccccccccc")).await);
        assert_eq!(
            gone["reason"], "entry-required",
            "a removed chain entry is skipped"
        );

        save_entry(&backend, "e2b", "Legacy").await;
        let (legacy, _) = config::resolve(svc, None, None).await.unwrap();
        assert_eq!(
            legacy, "e2b",
            "an older client's placement falls back to e2b"
        );
        let (named, _) = config::resolve(svc, Some("e2b-aaaaaaaaaaaa"), Some("e2b"))
            .await
            .unwrap();
        assert_eq!(named, "e2b-aaaaaaaaaaaa", "a named entry wins");
    }

    async fn configure(backend: &Backend, params: Value) -> intent_core::Result<Value> {
        let owner = backend.store().get_primary_principal().await.unwrap().id;
        with_caller(
            Caller::Wire {
                principal_id: owner,
                host_role: HostRole::Owner,
            },
            backend.services.cloud_machines("configure", params),
        )
        .await
    }

    fn invalid(result: intent_core::Result<Value>) -> String {
        match result {
            Err(Error::InvalidParams(message)) => message,
            other => panic!("expected invalid-params: {other:?}"),
        }
    }

    #[tokio::test]
    async fn configure_refuses_before_any_e2b_call() {
        let backend = Backend::new().await;
        let unknown =
            reason(configure(&backend, json!({ "cloudMachineId": "e2b-ffffffffffff" })).await);
        assert_eq!(unknown["code"], "not-found");
        invalid(configure(&backend, json!({ "provider": "aws" })).await);
        invalid(configure(&backend, json!({ "name": "  " })).await);
        invalid(configure(&backend, json!({ "name": "x".repeat(65) })).await);
        invalid(configure(&backend, json!({ "name": "A\u{7}" })).await);
        invalid(
            configure(
                &backend,
                json!({ "name": "A", "apiKey": "k", "apiKeyFrom": "e2b" }),
            )
            .await,
        );
        // Creating needs a key.
        let keyless = reason(configure(&backend, json!({ "name": "Alpha" })).await);
        assert_eq!(keyless["reason"], "not-configured");
        let legacy = reason(configure(&backend, json!({ "provider": "e2b" })).await);
        assert_eq!(legacy["reason"], "not-configured");

        save_entry(&backend, "e2b", "E2B").await;
        save_entry(&backend, "e2b-aaaaaaaaaaaa", "Alpha").await;
        let taken = invalid(configure(&backend, json!({ "name": "alpha", "apiKey": "k" })).await);
        assert!(taken.contains("already exists"), "{taken}");
        let renamed = invalid(
            configure(
                &backend,
                json!({ "cloudMachineId": "e2b", "name": "ALPHA" }),
            )
            .await,
        );
        assert!(renamed.contains("already exists"), "{renamed}");
        let own = invalid(
            configure(
                &backend,
                json!({ "cloudMachineId": "e2b", "apiKeyFrom": "e2b" }),
            )
            .await,
        );
        assert!(own.contains("another"), "{own}");
        let missing = reason(
            configure(
                &backend,
                json!({ "name": "Copy", "apiKeyFrom": "e2b-ffffffffffff" }),
            )
            .await,
        );
        assert_eq!(missing["code"], "not-found");
        let keyless_source =
            reason(configure(&backend, json!({ "name": "Copy", "apiKeyFrom": "e2b" })).await);
        assert_eq!(keyless_source["code"], "not-found", "the source has no key");

        for n in 2..16 {
            save_entry(&backend, &format!("e2b-{n:012x}"), &format!("Entry {n}")).await;
        }
        let full =
            invalid(configure(&backend, json!({ "name": "Seventeenth", "apiKey": "k" })).await);
        assert!(full.contains("16"), "{full}");
        let names: Vec<(String, ProviderConfig)> =
            config::load_all(&backend.services).await.unwrap();
        assert_eq!(names.len(), 16);
    }

    #[test]
    fn a_new_legacy_entry_takes_the_first_free_name() {
        let entries = vec![
            ("e2b-aaaaaaaaaaaa".to_owned(), ProviderConfig::new("e2b")),
            ("e2b-bbbbbbbbbbbb".to_owned(), ProviderConfig::new("E2B 2")),
        ];
        assert_eq!(config::free_legacy_name(&entries, "e2b"), "E2B 3");
        assert_eq!(config::free_legacy_name(&[], "e2b"), "E2B");
    }
}
