//! Manual specialist memory is optional, workspace-local, and atomic.
use super::tests::{setup, workspace};
use intent_core::{AgentCreateExtra, AgentId, WorkspaceApi, WorkspaceId};
use intent_store::Store;
use serde_json::{json, Value};

async fn create(
    svc: &crate::Services,
    ws: &WorkspaceId,
    specialist: Option<&str>,
    extra: AgentCreateExtra,
) -> intent_core::Result<Value> {
    svc.agent_create(
        ws.clone(),
        None,
        None,
        specialist.map(str::to_string),
        None,
        None,
        extra,
    )
    .await
}

#[intent_test_macros::daemon_test]
async fn creation_preferences_persist_only_specialist_and_isolate_workspaces() {
    let (tmp, svc, ws) = setup().await;
    let other = WorkspaceId::new();
    svc.store
        .insert_workspace(&workspace(&other))
        .await
        .unwrap();
    assert_eq!(
        svc.agent_get_creation_preferences(ws.clone())
            .await
            .unwrap(),
        json!({})
    );
    let result = create(
        &svc,
        &ws,
        Some("coordinator"),
        AgentCreateExtra {
            remember_specialist: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(result["agent"]["metadata"]["specialist"], "spec-writer");
    assert_eq!(
        svc.agent_get_creation_preferences(ws.clone())
            .await
            .unwrap(),
        json!({"specialistId":"spec-writer"})
    );
    assert_eq!(
        svc.agent_get_creation_preferences(other.clone())
            .await
            .unwrap(),
        json!({})
    );
    create(
        &svc,
        &other,
        None,
        AgentCreateExtra {
            remember_specialist: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        svc.agent_get_creation_preferences(other.clone())
            .await
            .unwrap(),
        json!({"specialistId":null})
    );
    drop(svc);
    let reopened = Store::open(&tmp.path).await.unwrap();
    assert_eq!(
        reopened.get_agent_creation_preferences(&ws).await.unwrap(),
        json!({"specialistId":"spec-writer"})
    );
    assert_eq!(
        reopened
            .get_agent_creation_preferences(&other)
            .await
            .unwrap(),
        json!({"specialistId":null})
    );
}

#[intent_test_macros::daemon_test]
async fn creation_preferences_ignore_failed_unmarked_background_and_child_creates() {
    let (_tmp, svc, ws) = setup().await;
    let remembered = AgentCreateExtra {
        remember_specialist: true,
        ..Default::default()
    };
    create(&svc, &ws, Some("missing-specialist"), remembered.clone())
        .await
        .unwrap_err();
    assert_eq!(
        svc.agent_get_creation_preferences(ws.clone())
            .await
            .unwrap(),
        json!({})
    );
    let first = create(&svc, &ws, Some("implementor"), remembered.clone())
        .await
        .unwrap();
    for extra in [
        AgentCreateExtra::default(),
        AgentCreateExtra {
            is_background: Some(true),
            ..remembered.clone()
        },
        AgentCreateExtra {
            metadata: Some(json!({"createdByAgentId":"parent"})),
            ..remembered.clone()
        },
    ] {
        create(&svc, &ws, None, extra).await.unwrap();
    }
    svc.agent_create(
        ws.clone(),
        None,
        None,
        None,
        Some(AgentId(first["agent"]["id"].as_str().unwrap().into())),
        None,
        remembered,
    )
    .await
    .unwrap();
    assert_eq!(
        svc.agent_get_creation_preferences(ws.clone())
            .await
            .unwrap(),
        json!({"specialistId":"implementor"})
    );
}

#[intent_test_macros::daemon_test]
async fn creation_preferences_remember_successful_welcome_selection_and_general() {
    let (_tmp, svc, ws) = setup().await;
    let agent = create(&svc, &ws, None, AgentCreateExtra::default())
        .await
        .unwrap();
    let id = AgentId(agent["agent"]["id"].as_str().unwrap().into());
    svc.agent_update(
        id.clone(),
        Some(ws.clone()),
        json!({"specialist":"coordinator","rememberSpecialist":true}),
    )
    .await
    .unwrap();
    assert_eq!(
        svc.agent_get_creation_preferences(ws.clone())
            .await
            .unwrap(),
        json!({"specialistId":"spec-writer"})
    );
    for changes in [
        json!({"specialist":"missing-specialist","rememberSpecialist":true}),
        json!({"specialist":null,"rememberSpecialist":"yes"}),
        json!({"rememberSpecialist":true}),
    ] {
        svc.agent_update(id.clone(), Some(ws.clone()), changes)
            .await
            .unwrap_err();
        assert_eq!(
            svc.agent_get_creation_preferences(ws.clone())
                .await
                .unwrap(),
            json!({"specialistId":"spec-writer"})
        );
    }
    svc.agent_update(
        id.clone(),
        Some(ws.clone()),
        json!({"specialist":"implementor"}),
    )
    .await
    .unwrap();
    assert_eq!(
        svc.agent_get_creation_preferences(ws.clone())
            .await
            .unwrap(),
        json!({"specialistId":"spec-writer"})
    );
    svc.agent_update(
        id,
        Some(ws.clone()),
        json!({"specialist":null,"rememberSpecialist":true}),
    )
    .await
    .unwrap();
    assert_eq!(
        svc.agent_get_creation_preferences(ws.clone())
            .await
            .unwrap(),
        json!({"specialistId":null})
    );
}

#[intent_test_macros::daemon_test]
async fn creation_preferences_roll_back_insert_and_update_when_memory_write_fails() {
    let (_tmp, svc, ws) = setup().await;
    let initial = create(
        &svc,
        &ws,
        Some("implementor"),
        AgentCreateExtra {
            remember_specialist: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let id = AgentId(initial["agent"]["id"].as_str().unwrap().into());
    sqlx::query("CREATE TRIGGER fail_preferences BEFORE UPDATE ON settings WHEN NEW.key LIKE 'workspace.agentCreationPreferences:%' BEGIN SELECT RAISE(ABORT, 'test failure'); END")
        .execute(svc.store.write_pool()).await.unwrap();
    create(
        &svc,
        &ws,
        None,
        AgentCreateExtra {
            remember_specialist: true,
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(svc.store.list_agent_sessions(&ws).await.unwrap().len(), 1);
    svc.agent_update(
        id.clone(),
        Some(ws.clone()),
        json!({"specialist":null,"rememberSpecialist":true,"notificationsMuted":true}),
    )
    .await
    .unwrap_err();
    assert_eq!(
        svc.store
            .get_agent_session(&id)
            .await
            .unwrap()
            .specialist
            .as_deref(),
        Some("implementor")
    );
    assert!(
        !svc.store
            .get_agent_session(&id)
            .await
            .unwrap()
            .notifications_muted
    );
    assert_eq!(
        svc.agent_get_creation_preferences(ws.clone())
            .await
            .unwrap(),
        json!({"specialistId":"implementor"})
    );
}

#[intent_test_macros::daemon_test]
async fn creation_preferences_do_not_replay_models_and_delete_with_workspace() {
    let (_tmp, svc, ws) = setup().await;
    let registry = svc.settings_registry().unwrap();
    registry
        .apply(&[("model.default".into(), json!("first-default"))])
        .unwrap();
    let first = create(
        &svc,
        &ws,
        Some("implementor"),
        AgentCreateExtra {
            remember_specialist: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(first["agent"]["model"], "first-default");
    registry
        .apply(&[("model.default".into(), json!("second-default"))])
        .unwrap();
    let second = create(
        &svc,
        &ws,
        Some("implementor"),
        AgentCreateExtra {
            remember_specialist: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(second["agent"]["model"], "second-default");
    assert_eq!(
        svc.agent_get_creation_preferences(ws.clone())
            .await
            .unwrap(),
        json!({"specialistId":"implementor"})
    );
    svc.store.delete_workspace(&ws).await.unwrap();
    assert_eq!(
        svc.store.get_agent_creation_preferences(&ws).await.unwrap(),
        json!({})
    );
    assert!(svc.agent_get_creation_preferences(ws).await.is_err());
}

#[intent_test_macros::daemon_test]
async fn creation_preferences_background_updates_never_replace_manual_selection() {
    let (_tmp, svc, ws) = setup().await;
    let agent = create(
        &svc,
        &ws,
        None,
        AgentCreateExtra {
            is_background: Some(true),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let id = AgentId(agent["agent"]["id"].as_str().unwrap().into());
    svc.agent_update(
        id,
        Some(ws.clone()),
        json!({"specialist":"implementor","rememberSpecialist":true,"isBackground":false}),
    )
    .await
    .unwrap();
    assert_eq!(
        svc.agent_get_creation_preferences(ws).await.unwrap(),
        json!({})
    );
}

#[intent_test_macros::daemon_test]
async fn creation_preferences_idempotent_replay_does_not_restore_old_choice() {
    let (_tmp, svc, ws) = setup().await;
    let first = svc
        .agent_create(
            ws.clone(),
            None,
            None,
            Some("implementor".into()),
            None,
            Some("manual-create".into()),
            AgentCreateExtra {
                remember_specialist: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    create(
        &svc,
        &ws,
        None,
        AgentCreateExtra {
            remember_specialist: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let replay = svc
        .agent_create(
            ws.clone(),
            None,
            None,
            Some("implementor".into()),
            None,
            Some("manual-create".into()),
            AgentCreateExtra {
                remember_specialist: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(replay, first);
    assert_eq!(
        svc.agent_get_creation_preferences(ws).await.unwrap(),
        json!({"specialistId":null})
    );
}

#[intent_test_macros::daemon_test]
async fn creation_preferences_welcome_renames_only_generated_placeholders() {
    let (_tmp, svc, ws) = setup().await;
    for (name, explicit, expected) in [
        ("Agent", false, "Implementor"),
        ("Agent 2", false, "Implementor"),
        ("My task", false, "My task"),
        ("Agent", true, "Agent"),
    ] {
        let created = svc
            .agent_create(
                ws.clone(),
                Some(name.into()),
                None,
                None,
                None,
                None,
                AgentCreateExtra {
                    name_explicitly_set: Some(explicit),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let id = AgentId(created["agent"]["id"].as_str().unwrap().into());
        let updated = svc
            .agent_update(
                id.clone(),
                Some(ws.clone()),
                json!({"specialist":"implementor","rememberSpecialist":true}),
            )
            .await
            .unwrap();
        assert_eq!(updated["agent"]["name"], expected);
        assert_eq!(updated["agent"]["nameExplicitlySet"], explicit);
        let cleared = svc
            .agent_update(
                id,
                Some(ws.clone()),
                json!({"specialist":null,"rememberSpecialist":true}),
            )
            .await
            .unwrap();
        assert_eq!(
            cleared["agent"]["name"],
            if expected == "Implementor" {
                "Agent"
            } else {
                expected
            }
        );
    }
}

#[intent_test_macros::daemon_test]
async fn creation_preferences_initial_agent_plan_preserves_opt_in_and_name_provenance() {
    let (tmp, svc, _ws) = setup().await;
    let svc = svc.with_workspaces_root(tmp.path.parent().unwrap().join("workspaces"));
    let created = svc
        .create_workspace(
            intent_core::WorkspaceCreate {
                title: Some("Initial choice".into()),
                initial_agent: Some(intent_core::WorkspaceCreateInitialAgent {
                    name: Some("Implementor".into()),
                    specialist: Some("implementor".into()),
                    remember_specialist: Some(true),
                    name_explicitly_set: Some(false),
                    ..Default::default()
                }),
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
    assert_eq!(
        created.initial_agent.as_ref().unwrap()["nameExplicitlySet"],
        false
    );
    assert_eq!(
        svc.agent_get_creation_preferences(created.workspace.id)
            .await
            .unwrap(),
        json!({"specialistId":"implementor"})
    );
}

#[intent_test_macros::daemon_test]
async fn creation_preferences_mixed_mute_failure_rolls_back_all_changes() {
    let (_tmp, svc, ws) = setup().await;
    let bus = crate::EventBus::new(svc.store.clone());
    let svc = svc.with_event_bus(bus.clone());
    let created = create(
        &svc,
        &ws,
        Some("implementor"),
        AgentCreateExtra {
            remember_specialist: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let id = AgentId(created["agent"]["id"].as_str().unwrap().into());
    let before = svc.store.get_agent_session(&id).await.unwrap();
    let mut sub = bus.subscribe(crate::SubscriptionFilter {
        event_types: vec!["agent:updated".into(), "agent:renamed".into()],
        ..Default::default()
    });
    sqlx::query("CREATE TRIGGER fail_preferences_mute BEFORE UPDATE OF notifications_muted ON agent_session BEGIN SELECT RAISE(ABORT, 'test mute failure'); END")
        .execute(svc.store.write_pool()).await.unwrap();
    let changes = json!({"specialist":null,"rememberSpecialist":true,"notificationsMuted":true});
    svc.agent_update(id.clone(), Some(ws.clone()), changes.clone())
        .await
        .unwrap_err();
    let after = svc.store.get_agent_session(&id).await.unwrap();
    assert_eq!(after.metadata, before.metadata);
    assert_eq!(after.specialist, before.specialist);
    assert_eq!(after.name, before.name);
    assert_eq!(after.name_explicitly_set, before.name_explicitly_set);
    assert_eq!(after.notifications_muted, before.notifications_muted);
    assert_eq!(after.updated_at, before.updated_at);
    assert_eq!(
        svc.agent_get_creation_preferences(ws.clone())
            .await
            .unwrap(),
        json!({"specialistId":"implementor"})
    );
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(50), sub.recv())
            .await
            .is_err(),
        "failed update must not publish a success event"
    );
    sqlx::query("DROP TRIGGER fail_preferences_mute")
        .execute(svc.store.write_pool())
        .await
        .unwrap();
    svc.agent_update(id.clone(), Some(ws.clone()), changes)
        .await
        .unwrap();
    let after = svc.store.get_agent_session(&id).await.unwrap();
    assert_eq!(after.specialist, None);
    assert_eq!(after.name, "Agent");
    assert!(after.notifications_muted);
    assert_eq!(
        svc.agent_get_creation_preferences(ws.clone())
            .await
            .unwrap(),
        json!({"specialistId":null})
    );
    let event = tokio::time::timeout(std::time::Duration::from_secs(2), sub.recv())
        .await
        .expect("successful mixed update event");
    assert!(event.is_some());
    svc.store.update_agent_session(&ws, &before).await.unwrap();
    assert!(
        svc.store
            .get_agent_session(&id)
            .await
            .unwrap()
            .notifications_muted,
        "a stale ordinary row write must preserve the mute toggle"
    );
}

#[intent_test_macros::daemon_test]
async fn creation_preferences_welcome_selection_freezes_complete_instructions() {
    let (_tmp, svc, ws) = setup().await;
    for (initial, custom) in [
        (None, false),
        (None, true),
        (Some("implementor"), false),
        (Some("implementor"), true),
    ] {
        let created = create(
            &svc,
            &ws,
            initial,
            AgentCreateExtra {
                metadata: custom.then(|| json!({"behaviorPrompt":"Caller behavior override.","specialistGeneratedBehaviorPrompt":"Caller behavior override."})),
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let id = AgentId::from(created["agent"]["id"].as_str().unwrap());
        for specialist in [
            Some("implementor"),
            Some("verifier"),
            None,
            Some("implementor"),
        ] {
            svc.agent_update(
                id.clone(),
                Some(ws.clone()),
                json!({"specialist":specialist,"rememberSpecialist":true}),
            )
            .await
            .unwrap();
            let injection = svc.agent_specialist_injection(&id, None).await;
            let reminder = svc.agent_role_reminder(&id).await;
            if let Some(specialist) = specialist {
                let (body, name, role) = svc
                    .specialists_service()
                    .resolve_prompt_injection(specialist, None)
                    .unwrap();
                let injection = injection.unwrap();
                assert_eq!(
                    injection.behavior_prompt,
                    if custom {
                        Some("Caller behavior override.".into())
                    } else {
                        body
                    }
                );
                assert_eq!(injection.specialist_name, Some(name.clone()));
                assert_eq!(injection.role_reminder, role.clone());
                assert_eq!(
                    reminder,
                    role.map(|r| crate::harness::latest().role_reminder_prefix(&name, &r))
                );
            } else {
                assert!(reminder.is_none());
                if custom {
                    assert_eq!(
                        injection.unwrap().behavior_prompt.as_deref(),
                        Some("Caller behavior override.")
                    );
                } else {
                    assert!(injection.is_none());
                }
            }
        }
    }
}

#[intent_test_macros::daemon_test]
async fn creation_preferences_preserve_unmarked_legacy_and_changed_override_bodies() {
    let (_tmp, svc, ws) = setup().await;
    for legacy in [true, false] {
        let created = create(&svc, &ws, Some("implementor"), AgentCreateExtra::default())
            .await
            .unwrap();
        let id = AgentId::from(created["agent"]["id"].as_str().unwrap());
        let mut session = svc.store.get_agent_session(&id).await.unwrap();
        let metadata = session.metadata.as_mut().unwrap().as_object_mut().unwrap();
        if legacy {
            // Old rows did not distinguish a caller override from a frozen body.
            metadata.remove("specialistGeneratedBehaviorPrompt");
        } else {
            metadata.insert("behaviorPrompt".into(), json!("Later explicit override."));
        }
        let preserved = metadata["behaviorPrompt"].as_str().unwrap().to_string();
        svc.store.update_agent_session(&ws, &session).await.unwrap();
        for specialist in [None, Some("verifier")] {
            svc.agent_update(
                id.clone(),
                Some(ws.clone()),
                json!({"specialist":specialist,"rememberSpecialist":true}),
            )
            .await
            .unwrap();
            let injection = svc.agent_specialist_injection(&id, None).await.unwrap();
            assert_eq!(
                injection.behavior_prompt.as_deref(),
                Some(preserved.as_str())
            );
            assert_eq!(injection.specialist_name.is_some(), specialist.is_some());
        }
    }
}
