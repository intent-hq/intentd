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

#[tokio::test]
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

#[tokio::test]
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

#[tokio::test]
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

#[tokio::test]
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
        json!({"specialist":null,"rememberSpecialist":true}),
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
    assert_eq!(
        svc.agent_get_creation_preferences(ws.clone())
            .await
            .unwrap(),
        json!({"specialistId":"implementor"})
    );
}

#[tokio::test]
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

#[tokio::test]
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

#[tokio::test]
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

#[tokio::test]
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
