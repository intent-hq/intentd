//! Host-member authority over the runtime's script, terminal and execution
//! context surfaces (moved from `capability/host_role_tests.rs`).

use intent_core::{
    with_caller, Caller, Error, HostRole, PrincipalId, WorkspaceApi, WorkspaceId, WorkspaceRole,
};
use intent_store::Store;
use serde_json::json;

use super::test_fixtures::host_role::{caller, fixture};
use crate::host_execution::HostExecutionExt;
use intent_services::feature_api::test_support::fixtures::{workspace, TempDb};
use intent_services::feature_api::Services;

#[tokio::test]
async fn member_execution_context_event_is_durable_and_survives_store_reopen() {
    use intent_core::events::HOST_EXECUTION_CONTEXT_CHANGED;
    use intent_services::feature_api::events::{EventBus, SubscriptionFilter};
    use intent_store::EventQuery;
    use std::{sync::Arc, time::Duration};

    let tmp = TempDb::new();
    let (svc, owner, member) = fixture(&tmp).await;
    let registry = Arc::new(
        intent_services::feature_api::SettingsRegistry::load(tmp.path.with_extension("toml"))
            .unwrap(),
    );
    registry
        .apply(&[
            ("sourceControl.github.tokenSource".into(), json!("explicit")),
            ("model.defaultProvider".into(), json!("codex")),
        ])
        .unwrap();
    let bus = EventBus::new(svc.store().clone());
    let svc = svc
        .with_settings_registry(registry)
        .with_event_bus(bus.clone());
    let worker = svc.spawn_execution_context_loop();
    let mut events = bus.subscribe(SubscriptionFilter {
        event_types: vec![HOST_EXECUTION_CONTEXT_CHANGED.into()],
        ..Default::default()
    });
    let expected = with_caller(caller(&member), svc.host_execution_context())
        .await
        .unwrap();
    with_caller(
        caller(&member),
        svc.observe_execution_readiness(json!({"providers":[{
            "id":"codex","authenticated":false,"secret":"private-readiness-body"
        }]})),
    )
    .await
    .unwrap();
    let live = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(5)),
        events.recv(),
    )
    .await
    .unwrap()
    .unwrap()
    .remove(0);
    worker.abort();
    let _ = worker.await;
    assert_eq!(live.data, expected);
    assert!(live.workspace_id.as_str().is_empty());
    let query = EventQuery {
        event_types: vec![HOST_EXECUTION_CONTEXT_CHANGED.into()],
        ..Default::default()
    };
    let persisted = svc.store().query_events(&query).await.unwrap();
    assert_eq!(persisted.len(), 1, "live delivery must follow persistence");
    assert_eq!(persisted[0].id, live.id);
    assert_eq!(persisted[0].data, expected);

    let owner_history = with_caller(
        Caller::Wire {
            principal_id: owner,
            host_role: intent_core::HostRole::Owner,
        },
        svc.event_query(
            WorkspaceId::from(""),
            intent_core::EventQueryParams {
                event_type: Some(HOST_EXECUTION_CONTEXT_CHANGED.into()),
                ..Default::default()
            },
        ),
    )
    .await
    .unwrap();
    assert_eq!(owner_history[0]["data"], expected);
    drop(events);
    drop(bus);
    drop(svc);
    let reopened = Store::open(&tmp.path).await.unwrap();
    let restored = reopened.query_events(&query).await.unwrap();
    assert_eq!(restored.len(), 1);
    assert_eq!(restored[0].id, live.id);
    assert_eq!(restored[0].data, expected);
    assert_eq!(
        restored[0]
            .data
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<std::collections::BTreeSet<_>>(),
        [
            "defaultModelId",
            "defaultProviderId",
            "enabledProviderIds",
            "gitCredentialPolicy",
            "repositoryConnections"
        ]
        .into_iter()
        .collect()
    );
    assert!(!restored[0]
        .data
        .to_string()
        .contains("private-readiness-body"));
}

#[tokio::test]
async fn member_execution_context_is_allowlisted_and_uses_host_policy() {
    use intent_services::feature_api::settings::{InMemorySecretStore, SecretStore};
    use std::sync::Arc;
    let tmp = TempDb::new();
    let (svc, _, member) = fixture(&tmp).await;
    let registry = Arc::new(
        intent_services::feature_api::SettingsRegistry::load(tmp.path.with_extension("toml"))
            .unwrap(),
    );
    registry
        .apply(&[
            ("sourceControl.github.tokenSource".into(), json!("explicit")),
            (
                "sourceControl.github.exposeGitCredentialToChildren".into(),
                json!(false),
            ),
            ("model.defaultProvider".into(), json!("codex")),
            ("model.default".into(), json!("host-model")),
            ("providers.paths".into(), json!({"codex":"/host/provider"})),
        ])
        .unwrap();
    let secrets = Arc::new(InMemorySecretStore::default());
    secrets
        .store("sourceControl.github.token", "must-never-leave-the-host")
        .unwrap();
    secrets
        .store("collaboration.github.token", "identity-only")
        .unwrap();
    let svc = svc
        .with_secret_store(secrets.clone())
        .with_settings_registry(registry.clone());
    with_caller(caller(&member), async {
        let context = svc.host_execution_context().await.unwrap();
        let keys: std::collections::BTreeSet<_> = context.as_object().unwrap().keys().map(String::as_str).collect();
        assert_eq!(keys, ["defaultModelId", "defaultProviderId", "enabledProviderIds", "gitCredentialPolicy", "repositoryConnections"].into_iter().collect());
        assert_eq!(context["defaultProviderId"], "codex");
        assert_eq!(context["defaultModelId"], "host-model");
        assert_eq!(context["gitCredentialPolicy"], json!({
            "provider":"github", "protocol":"https", "host":"github.com", "managedHelperEnabled":false,
            "setting":"sourceControl.github.exposeGitCredentialToChildren",
        }));
        assert_eq!(context["repositoryConnections"][0], json!({"provider":"github", "host":"github.com", "configured":true}));
        assert!(!context.to_string().contains("must-never-leave"));
        assert!(!context.to_string().contains("identity-only"));
        assert!(!context.to_string().contains("/host/provider"));
        assert_eq!(svc.execution_provider_paths().await.unwrap()["codex"], "/host/provider");
        assert!(matches!(svc.settings_get("providers.paths".into()).await, Err(Error::Forbidden(_))));
        svc.feature_secrets().delete_for_test("sourceControl.github.token").await.unwrap();
        assert_eq!(svc.host_execution_context().await.unwrap()["repositoryConnections"][0]["configured"], false,
            "collaboration credentials must never supply execution");
        registry.apply(&[("sourceControl.github.exposeGitCredentialToChildren".into(), json!(true))]).unwrap();
        assert_eq!(svc.host_execution_context().await.unwrap()["gitCredentialPolicy"]["managedHelperEnabled"], true);
    }).await;
    svc.store().remove_host_member(&member).await.unwrap();
    assert!(
        with_caller(caller(&member), svc.host_execution_context())
            .await
            .is_err(),
        "stale admitted member rejected"
    );
    assert!(
        with_caller(caller(&PrincipalId::new()), svc.host_execution_context())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn member_execution_enablement_events_follow_committed_changes_and_reset() {
    use intent_core::events::HOST_EXECUTION_CONTEXT_CHANGED;
    use intent_services::feature_api::events::{EventBus, SubscriptionFilter};
    use std::{sync::Arc, time::Duration};
    let tmp = TempDb::new();
    let (svc, _, member) = fixture(&tmp).await;
    let path = tmp.path.with_extension("toml");
    let registry =
        Arc::new(intent_services::feature_api::SettingsRegistry::load(path.clone()).unwrap());
    registry
        .apply(&[("sourceControl.github.tokenSource".into(), json!("explicit"))])
        .unwrap();
    let bus = EventBus::new(svc.store().clone());
    let svc = svc
        .with_settings_registry(registry)
        .with_event_bus(bus.clone());
    let worker = svc.spawn_execution_context_loop();
    let mut events = bus.subscribe(SubscriptionFilter {
        event_types: vec![HOST_EXECUTION_CONTEXT_CHANGED.into()],
        ..Default::default()
    });
    let mut all = intent_providers::all_provider_ids();
    all.sort_unstable();
    let disabled: Vec<_> = all.iter().copied().filter(|id| *id != "codex").collect();
    let before = with_caller(caller(&member), svc.host_execution_context())
        .await
        .unwrap();
    for (map, expected) in [
        (json!({"codex":false}), json!(disabled)),
        (json!({"codex":true}), json!(all)),
        (json!({"codex":false}), json!(disabled)),
        (json!({}), json!(all)),
        (json!({"codex":false}), json!(disabled)),
    ] {
        let update = with_caller(
            Caller::Daemon,
            svc.settings_update(json!([{"path":"providers.enabled","value":map}])),
        )
        .await
        .unwrap();
        let event = tokio::time::timeout(
            intentd_test_support::test_timeout(Duration::from_secs(5)),
            events.recv(),
        )
        .await
        .unwrap()
        .unwrap()
        .remove(0);
        assert_eq!(event.data["enabledProviderIds"], expected);
        assert_eq!(
            event.data["repositoryConnections"],
            before["repositoryConnections"]
        );
        assert_eq!(
            event.data,
            with_caller(caller(&member), svc.host_execution_context())
                .await
                .unwrap()
        );
        let rows = svc
            .store()
            .query_events(&intent_store::EventQuery {
                event_types: vec![HOST_EXECUTION_CONTEXT_CHANGED.into()],
                ..Default::default()
            })
            .await
            .unwrap();
        assert!(rows
            .iter()
            .any(|row| row.id == event.id && row.data == event.data));
        let reloaded = Services::new(svc.store().clone()).with_settings_registry(Arc::new(
            intent_services::feature_api::SettingsRegistry::load(path.clone()).unwrap(),
        ));
        assert_eq!(
            with_caller(caller(&member), reloaded.host_execution_context())
                .await
                .unwrap(),
            event.data
        );
        let noop = with_caller(
            Caller::Daemon,
            svc.settings_update(json!([{"path":"providers.enabled","value":map}])),
        )
        .await
        .unwrap();
        assert_eq!(noop["revision"], update["revision"]);
        assert_eq!(noop["applied"], json!([]));
        assert!(tokio::time::timeout(
            intentd_test_support::test_timeout(Duration::from_millis(25)),
            events.recv()
        )
        .await
        .is_err());
    }
    let reset = with_caller(
        Caller::Daemon,
        svc.settings_reset("providers.enabled".into()),
    )
    .await
    .unwrap();
    let event = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(5)),
        events.recv(),
    )
    .await
    .unwrap()
    .unwrap()
    .remove(0);
    assert_eq!(event.data["enabledProviderIds"], json!(all));
    // Resetting an already absent map and an empty update are true no-ops.
    let noop = with_caller(
        Caller::Daemon,
        svc.settings_reset("providers.enabled".into()),
    )
    .await
    .unwrap();
    assert_eq!(noop["revision"], reset["revision"]);
    let noop = with_caller(Caller::Daemon, svc.settings_update(json!([])))
        .await
        .unwrap();
    assert_eq!(noop["revision"], reset["revision"]);
    assert!(tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_millis(75)),
        events.recv()
    )
    .await
    .is_err());
    worker.abort();
    let _ = worker.await;
}

#[tokio::test]
async fn member_execution_enablement_projects_complete_canonical_policy() {
    use intent_services::feature_api::settings::{InMemorySecretStore, SecretStore};
    use std::sync::Arc;
    let tmp = TempDb::new();
    let (svc, owner, member) = fixture(&tmp).await;
    let registry = Arc::new(
        intent_services::feature_api::SettingsRegistry::load(tmp.path.with_extension("toml"))
            .unwrap(),
    );
    registry
        .apply(&[("sourceControl.github.tokenSource".into(), json!("explicit"))])
        .unwrap();
    let forge = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    registry
        .apply(&[(
            "sourceControl.github.apiBaseUrl".into(),
            json!(format!("http://{}", forge.local_addr().unwrap())),
        )])
        .unwrap();
    let secrets = Arc::new(InMemorySecretStore::default());
    secrets
        .store(
            "sourceControl.github.token",
            "private-unverified-repository-token",
        )
        .unwrap();
    let svc = svc
        .with_settings_registry(registry.clone())
        .with_secret_store(secrets);
    let mut all = intent_providers::all_provider_ids();
    all.sort_unstable();
    all.dedup();
    let without_codex: Vec<_> = all.iter().copied().filter(|id| *id != "codex").collect();
    // Case variants and legacy aliases must neither disable the canonical
    // provider nor manufacture extra IDs. Duplicate JSON keys collapse at parse.
    let duplicate = serde_json::from_str::<serde_json::Value>(
        r#"{"codex":true,"codex":false,"Codex":true,"augment":false,"acp":false,"default":false,"unknown":true}"#,
    )
    .unwrap();
    for (map, expected) in [
        (json!(null), json!(all)),
        (json!({}), json!(all)),
        (json!({"codex":false}), json!(without_codex)),
        (json!({"codex":true}), json!(all)),
        (duplicate, json!(without_codex)),
        (
            json!({"Codex":false,"augment":false,"unknown":true}),
            json!(all),
        ),
    ] {
        registry
            .apply(&[("providers.enabled".into(), map)])
            .unwrap();
        for identity in [
            Caller::Wire {
                principal_id: owner.clone(),
                host_role: intent_core::HostRole::Owner,
            },
            caller(&member),
        ] {
            let context = with_caller(identity, svc.host_execution_context())
                .await
                .unwrap();
            assert_eq!(context["enabledProviderIds"], expected);
            assert_eq!(context["repositoryConnections"][0]["configured"], true);
            assert!(!context
                .to_string()
                .contains("private-unverified-repository-token"));
        }
    }
    for provider in intent_providers::ACP_PROVIDERS {
        registry
            .apply(&[("providers.enabled".into(), json!({provider.id:false}))])
            .unwrap();
        let context = with_caller(caller(&member), svc.host_execution_context())
            .await
            .unwrap();
        let expected: Vec<_> = all
            .iter()
            .copied()
            .filter(|id| *id != provider.id || !provider.can_be_disabled)
            .collect();
        assert_eq!(
            context["enabledProviderIds"],
            json!(expected),
            "{}",
            provider.id
        );
    }
    let disabled: std::collections::BTreeMap<_, _> = all.iter().map(|id| (*id, false)).collect();
    registry
        .apply(&[("providers.enabled".into(), json!(disabled))])
        .unwrap();
    let context = with_caller(caller(&member), svc.host_execution_context())
        .await
        .unwrap();
    let always: Vec<_> = all
        .iter()
        .copied()
        .filter(|id| !intent_providers::find_provider(id).unwrap().can_be_disabled)
        .collect();
    assert_eq!(
        context["enabledProviderIds"],
        json!(always),
        "an empty effective list is still present"
    );
    assert!(
        tokio::time::timeout(
            intentd_test_support::test_timeout(std::time::Duration::from_millis(25)),
            forge.accept()
        )
        .await
        .is_err(),
        "policy/configured reads must not probe repository authorization"
    );
}

#[tokio::test]
async fn member_script_create_cannot_replace_definition_from_another_workspace() {
    let tmp = TempDb::new();
    let (svc, _, member) = fixture(&tmp).await;
    let target = WorkspaceId::new();
    svc.store()
        .insert_workspace(&workspace(&target))
        .await
        .unwrap();
    for source in [WorkspaceId::chief(), WorkspaceId::new()] {
        let original = with_caller(
            Caller::Daemon,
            svc.script_create(
                source.clone(),
                intent_core::ScriptCreateParams {
                    name: "Owner script".into(),
                    command: "original command".into(),
                    ..Default::default()
                },
            ),
        )
        .await
        .unwrap();
        let id = original["id"].as_str().unwrap();
        let replaced = with_caller(
            caller(&member),
            svc.script_create(
                target.clone(),
                intent_core::ScriptCreateParams {
                    name: "Replacement".into(),
                    command: "replacement command".into(),
                    script_id: Some(id.into()),
                    ..Default::default()
                },
            ),
        )
        .await;
        assert!(matches!(replaced, Err(Error::NotFound(_))), "{replaced:?}");
        let scripts = svc.store().list_all_scripts().await.unwrap();
        let saved = scripts.iter().find(|s| s.id == id).unwrap();
        assert_eq!(saved.workspace_id, source.as_str());
        assert_eq!(saved.command, "original command");

        // Exercise the atomic write guard independently of the service's
        // preflight, covering an id claimed between the lookup and insert.
        let mut moved = saved.clone();
        moved.workspace_id = target.to_string();
        moved.command = "replacement command".into();
        assert!(matches!(
            svc.store().upsert_script_in_workspace(&moved).await,
            Err(Error::NotFound(_))
        ));
        assert_eq!(
            svc.store().script_workspace(id).await.unwrap(),
            Some(source)
        );
        // Preserve the owner/internal store API; subsequent updates within
        // that workspace remain legal through the scoped entry point too.
        svc.store().upsert_script(&moved).await.unwrap();
        moved.name = "Scoped update".into();
        svc.store()
            .upsert_script_in_workspace(&moved)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn member_script_create_refuses_missing_workspace_without_persisting() {
    let tmp = TempDb::new();
    let (svc, _, member) = fixture(&tmp).await;
    let missing = WorkspaceId::new();
    let result = with_caller(
        caller(&member),
        svc.script_create(
            missing.clone(),
            intent_core::ScriptCreateParams {
                name: "Missing workspace".into(),
                command: "true".into(),
                mode: intent_core::ScriptMode::Command,
                ..Default::default()
            },
        ),
    )
    .await;
    assert!(matches!(result, Err(Error::NotFound(_))), "{result:?}");
    assert!(svc.store().list_all_scripts().await.unwrap().is_empty());
}

#[tokio::test]
async fn member_script_remove_preserves_a_protected_definition_and_runtime() {
    use intent_core::{events::SCRIPT_CHANGED, ScriptCreateParams};
    let tmp = TempDb::new();
    let (svc, _, member) = fixture(&tmp).await;
    let bus = intent_services::feature_api::events::EventBus::new(svc.store().clone());
    let svc = svc.with_event_bus(bus);
    let target = WorkspaceId::new();
    svc.store()
        .insert_workspace(&workspace(&target))
        .await
        .unwrap();
    for (ws, command) in [
        (target.clone(), "echo ordinary"),
        (WorkspaceId::chief(), "echo protected"),
    ] {
        with_caller(
            Caller::Daemon,
            svc.script_create(
                ws,
                ScriptCreateParams {
                    name: "dev".into(),
                    command: command.into(),
                    script_id: Some("dev".into()),
                    ..Default::default()
                },
            ),
        )
        .await
        .unwrap();
    }
    let query = intent_store::EventQuery {
        event_types: vec![SCRIPT_CHANGED.into()],
        ..Default::default()
    };
    let before = svc.store().query_events(&query).await.unwrap().len();
    let result = with_caller(
        caller(&member),
        svc.script_remove(target.clone(), "dev".into()),
    )
    .await;
    assert!(matches!(result, Err(Error::NotFound(_))), "{result:?}");
    assert_eq!(
        svc.store().query_events(&query).await.unwrap().len(),
        before,
        "refusal publishes no removal"
    );
    with_caller(
        caller(&member),
        svc.script_status(target.clone(), "dev".into()),
    )
    .await
    .unwrap();
    let saved = svc.store().list_all_scripts().await.unwrap();
    assert_eq!(saved.len(), 1);
    assert_eq!(saved[0].workspace_id, WorkspaceId::chief().as_str());
    assert_eq!(saved[0].command, "echo protected");

    let restarted = Services::new(Store::open(&tmp.path).await.unwrap());
    assert_eq!(restarted.hydrate_scripts().await.unwrap(), 1);
    let restored = with_caller(Caller::Daemon, restarted.script_list(WorkspaceId::chief()))
        .await
        .unwrap();
    assert_eq!(restored["scripts"][0]["command"], "echo protected");

    with_caller(caller(&member), async {
        let own = svc
            .script_create(
                target.clone(),
                ScriptCreateParams {
                    name: "valid".into(),
                    command: "echo valid".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        svc.script_remove(target.clone(), own["id"].as_str().unwrap().into())
            .await
            .unwrap();
    })
    .await;
    assert_eq!(svc.store().list_all_scripts().await.unwrap().len(), 1);
    // The unrestricted owner path retains its legacy id-only delete semantics.
    with_caller(Caller::Daemon, svc.script_remove(target, "dev".into()))
        .await
        .unwrap();
    assert!(svc.store().list_all_scripts().await.unwrap().is_empty());
}

#[cfg(unix)]
#[tokio::test]
async fn member_terminal_create_refuses_missing_workspace_without_spawning() {
    let tmp = TempDb::new();
    let (svc, _, member) = fixture(&tmp).await;
    let missing = WorkspaceId::new();
    let result = with_caller(
        caller(&member),
        svc.terminal_create(missing.clone(), 80, 24, None, Some("/bin/cat".into()), None),
    )
    .await;
    let spawned = svc.pty().list_scope(missing.as_str());
    // Clean up a mistakenly admitted process before the regression assertion.
    for id in &spawned {
        svc.pty().kill(*id).await;
    }
    assert!(matches!(result, Err(Error::NotFound(_))), "{result:?}");
    assert!(spawned.is_empty(), "missing workspace spawned {spawned:?}");
}

#[cfg(unix)]
#[tokio::test]
async fn member_terminal_ids_resolve_the_workspace_and_recheck_revocation() {
    let tmp = TempDb::new();
    let (svc, _, member) = fixture(&tmp).await;
    let ws = WorkspaceId::from("member-terminal");
    svc.store().insert_workspace(&workspace(&ws)).await.unwrap();
    let created = with_caller(
        caller(&member),
        svc.terminal_create(ws.clone(), 80, 24, None, Some("/bin/cat".into()), None),
    )
    .await
    .unwrap();
    let id = created["terminalId"].as_str().unwrap().to_owned();
    with_caller(caller(&member), async {
        svc.terminal_write(
            id.clone(),
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, "shared\n"),
        )
        .await
        .unwrap();
        svc.terminal_resize(id.clone(), 100, 30).await.unwrap();
        svc.terminal_get_buffer(id.clone(), None).await.unwrap();
        assert!(svc
            .terminal_get_buffer("unknown".into(), None)
            .await
            .is_err());
        assert!(svc
            .terminal_read_output(WorkspaceId::chief(), id.clone(), None, None, None)
            .await
            .is_err());
    })
    .await;
    svc.store().remove_host_member(&member).await.unwrap();
    assert!(with_caller(
        caller(&member),
        svc.terminal_write(id.clone(), "denied".into())
    )
    .await
    .is_err());
    with_caller(Caller::Daemon, svc.terminal_kill(id))
        .await
        .unwrap();
}

#[tokio::test]
async fn member_tools_script_and_terminal_lists_admit_host_members() {
    let tmp = TempDb::new();
    let (svc, _, member) = fixture(&tmp).await;
    let ws = WorkspaceId::from("member-execution");
    svc.store().insert_workspace(&workspace(&ws)).await.unwrap();
    with_caller(caller(&member), async {
        svc.terminal_list(ws.clone()).await.unwrap();
        svc.script_list(ws).await.unwrap();
    })
    .await;
}

#[tokio::test]
async fn script_archive_restore_enforce_workspace_scope_and_guest_authority() {
    use intent_core::{ScriptCreateParams, ScriptMode};
    let tmp = TempDb::new();
    let (svc, _, member) = fixture(&tmp).await;
    let a = WorkspaceId::new();
    let b = WorkspaceId::new();
    for ws in [&a, &b] {
        svc.store().insert_workspace(&workspace(ws)).await.unwrap();
    }
    let def = with_caller(
        Caller::Daemon,
        svc.script_create(
            a.clone(),
            ScriptCreateParams {
                name: "kept".into(),
                command: "true".into(),
                mode: ScriptMode::Command,
                ..Default::default()
            },
        ),
    )
    .await
    .unwrap();
    let id = def["id"].as_str().unwrap().to_owned();
    with_caller(caller(&member), async {
        assert_eq!(
            svc.script_archive(b.clone(), vec![id.clone()])
                .await
                .unwrap(),
            serde_json::json!({"archived":[],"skipped":[{"scriptId":id,"reason":"notFound"}]})
        );
        svc.script_archive(a.clone(), vec![id.clone()])
            .await
            .unwrap();
        svc.script_restore(a.clone(), vec![id.clone()])
            .await
            .unwrap();
    })
    .await;
    sqlx::query("DELETE FROM host_member WHERE principal_id=?")
        .bind(member.as_str())
        .execute(svc.store().write_pool())
        .await
        .unwrap();
    let guest = Caller::Wire {
        principal_id: member.clone(),
        host_role: HostRole::Guest,
    };
    with_caller(guest.clone(), async {
        assert!(matches!(
            svc.script_archive(a.clone(), vec![id.clone()]).await,
            Err(Error::NotFound(_))
        ));
        assert!(matches!(
            svc.script_restore(a.clone(), vec![id.clone()]).await,
            Err(Error::NotFound(_))
        ));
    })
    .await;
    svc.store()
        .add_workspace_member(&a, &member, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    with_caller(guest, async {
        assert!(matches!(
            svc.script_archive(a.clone(), vec![id.clone()]).await,
            Err(Error::Forbidden(_))
        ));
        assert!(matches!(
            svc.script_restore(a.clone(), vec![id.clone()]).await,
            Err(Error::Forbidden(_))
        ));
    })
    .await;
}

#[tokio::test]
async fn member_execution_context_events_cover_policy_setup_and_unchanged_configured_rejection() {
    use intent_core::events::HOST_EXECUTION_CONTEXT_CHANGED;
    use intent_services::feature_api::events::{EventBus, SubscriptionFilter};
    use std::{sync::Arc, time::Duration};
    let tmp = TempDb::new();
    let (svc, _, member) = fixture(&tmp).await;
    let registry = Arc::new(
        intent_services::feature_api::SettingsRegistry::load(tmp.path.with_extension("toml"))
            .unwrap(),
    );
    registry
        .apply(&[("sourceControl.github.tokenSource".into(), json!("explicit"))])
        .unwrap();
    let bus = EventBus::new(svc.store().clone());
    let svc = svc
        .with_settings_registry(registry)
        .with_event_bus(bus.clone());
    let worker = svc.spawn_execution_context_loop();
    let mut events = bus.subscribe(SubscriptionFilter {
        event_types: vec![HOST_EXECUTION_CONTEXT_CHANGED.into()],
        ..Default::default()
    });
    let before = svc.execution_context_snapshot().await.unwrap();
    with_caller(
        caller(&member),
        svc.execution_call_for_test(async {
            let _ = intent_services::feature_api::execution_call::forge_error_for_test(
                intent_sourcecontrol::Error::Auth("private response".into()),
            );
            Ok(())
        }),
    )
    .await
    .unwrap();
    let batch = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(5)),
        events.recv(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        batch[0].data,
        json!(before),
        "rejection invalidates despite unchanged configured flags"
    );
    with_caller(
        Caller::Daemon,
        svc.settings_update(
            json!([{"path":"sourceControl.github.exposeGitCredentialToChildren","value":false}]),
        ),
    )
    .await
    .unwrap();
    let batch = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(5)),
        events.recv(),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        batch.last().unwrap().data["gitCredentialPolicy"]["managedHelperEnabled"],
        false
    );
    with_caller(
        caller(&member),
        svc.observe_execution_readiness(
            json!({"providers":[{"id":"codex","authenticated":false,"secret":"no-leak"}]}),
        ),
    )
    .await
    .unwrap();
    let batch = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(5)),
        events.recv(),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(!batch[0].data.to_string().contains("no-leak"));
    worker.abort();
    let _ = worker.await;
}

#[tokio::test]
async fn member_pr_status_rejection_publishes_durable_safe_context() {
    use intent_core::events::HOST_EXECUTION_CONTEXT_CHANGED;
    use intent_services::feature_api::events::{EventBus, SubscriptionFilter};
    use intent_services::feature_api::test_support::stub_forge::StubForge;
    use std::{sync::Arc, time::Duration};
    let tmp = TempDb::new();
    let (svc, _, member) = fixture(&tmp).await;
    let ws = WorkspaceId::new();
    let mut row = workspace(&ws);
    row.repository_owner = Some("o".into());
    row.repository_name = Some("r".into());
    row.pr_number = Some(42);
    svc.store().insert_workspace(&row).await.unwrap();
    let bus = EventBus::new(svc.store().clone());
    let svc = svc
        .with_event_bus(bus.clone())
        .with_source_control(Arc::new(StubForge::with_get_pr_error(|| {
            intent_sourcecontrol::Error::Auth("private-response".into())
        })));
    let registry = Arc::new(
        intent_services::feature_api::SettingsRegistry::load(tmp.path.with_extension("toml"))
            .unwrap(),
    );
    registry
        .apply(&[("sourceControl.github.tokenSource".into(), json!("explicit"))])
        .unwrap();
    let svc = svc.with_settings_registry(registry);
    let mut events = bus.subscribe(SubscriptionFilter {
        event_types: vec![HOST_EXECUTION_CONTEXT_CHANGED.into()],
        ..Default::default()
    });
    let worker = svc.spawn_execution_context_loop();
    let before = svc.execution_context_snapshot().await.unwrap();
    let _error = with_caller(caller(&member), svc.pr_status(ws))
        .await
        .unwrap_err();
    let received = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_secs(2)),
        events.recv(),
    )
    .await;
    worker.abort();
    let _ = worker.await;
    let batch = received
        .expect("classified PR rejection must invalidate readiness")
        .unwrap();
    assert_eq!(batch[0].data, json!(before));
    assert_eq!(
        batch[0]
            .data
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<std::collections::BTreeSet<_>>(),
        [
            "defaultModelId",
            "defaultProviderId",
            "enabledProviderIds",
            "gitCredentialPolicy",
            "repositoryConnections"
        ]
        .into_iter()
        .collect()
    );
    assert!(!batch[0].data.to_string().contains("private-response"));
    let stored = svc
        .store()
        .query_events(&intent_store::EventQuery {
            event_types: vec![HOST_EXECUTION_CONTEXT_CHANGED.into()],
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(stored
        .iter()
        .any(|e| e.id == batch[0].id && e.data == batch[0].data));
}
