//! A specialist's provider must be selected before resolving its model and effort.
//! Catalogs deliberately disagree with the settings default to detect substitution.

use std::{path::Path, sync::Arc};

use intent_acp::WorkspaceMcpServer;
use intent_core::{
    AgentCreateExtra, AgentDelegateInput, AgentId, AgentWakeCreateOptions, AgentWakeOrCreateInput,
    NoteCreate, NoteId, WorkspaceApi, WorkspaceId,
};
use intent_store::{EventQuery, Store};
use serde_json::{json, Value};

use super::tests::{workspace, TempDb};
use crate::Services;

fn write_specialist(dir: &Path, id: &str, frontmatter: &str) {
    std::fs::create_dir_all(dir).expect("specialists dir");
    std::fs::write(
        dir.join(format!("{id}.md")),
        format!("---\nname: {id}\ndescription: Test\n{frontmatter}---\nTest prompt."),
    )
    .expect("write specialist");
}

fn set(svc: &Services, path: &str, value: Value) {
    svc.settings_registry()
        .expect("registry")
        .apply(&[(path.into(), value)])
        .expect("setting");
}

async fn setup() -> (TempDb, Services, WorkspaceId) {
    let tmp = TempDb::new();
    let root = tmp.path.parent().unwrap();
    let store = Store::open(&tmp.path).await.expect("store");
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&workspace(&ws))
        .await
        .expect("workspace");
    let specialists = root.join("specialists");
    write_specialist(
        &specialists,
        "pinned",
        "codingAgent: auggie\nmodel: pinned-model\naliases: [\"pin-alias\"]\nreasoningEffort: low\nmodelOptions: [{\"provider\":\"auggie\",\"model\":\"pinned-model\",\"reasoningEffort\":\"high\"}]\n",
    );
    let svc = Services::new(store)
        .with_settings_registry(Arc::new(
            crate::SettingsRegistry::load(root.join("config.toml")).expect("registry"),
        ))
        .with_workspaces_root(root.join("workspaces"))
        .with_specialist_dirs(Some(specialists.clone()), Some(specialists));
    set(&svc, "model.defaultProvider", json!("grok"));
    set(&svc, "model.default", json!("default-model"));
    set(&svc, "model.defaultReasoningEffort", json!("low"));
    // Availability is deterministic; Services has no agent manager, so no
    // provider process is launched by these create/delegate/binding tests.
    set(
        &svc,
        "providers.paths",
        json!({"auggie": "/bin/sh", "grok": "/bin/sh"}),
    );
    for (provider, model) in [("auggie", "pinned-model"), ("grok", "default-model")] {
        svc.models_catalog.store_for_test(
            provider,
            &(crate::model_catalog::source_for(provider)
                .unwrap()
                .version_key)(),
            vec![json!({"id": model, "name": model, "provider": provider,
                        "effortLevels": ["low", "high"]})],
        );
    }
    (tmp, svc, ws)
}

async fn create(
    svc: &Services,
    ws: &WorkspaceId,
    model: Option<&str>,
    extra: AgentCreateExtra,
) -> Value {
    svc.agent_create_op(
        ws.clone(),
        None,
        model.map(str::to_string),
        Some("pin-alias".into()),
        None,
        None,
        false,
        extra,
    )
    .await
    .expect("create")
}

async fn assert_pinned(svc: &Services, id: &str) {
    let session = svc
        .store()
        .get_agent_session(&AgentId::from(id))
        .await
        .expect("persisted session");
    assert_eq!(session.provider.as_deref(), Some("auggie"));
    assert_eq!(session.model.as_deref(), Some("pinned-model"));
    assert_eq!(session.reasoning_effort.as_deref(), Some("high"));
    assert_eq!(session.specialist.as_deref(), Some("pinned"));
}

async fn seed_task(svc: &Services, ws: &WorkspaceId) -> NoteId {
    let note = svc
        .create_note(
            ws.clone(),
            NoteCreate {
                title: "Pinned task".into(),
                content: Some("Do work".into()),
                tags: None,
                parent_id: None,
            },
            None,
            None,
        )
        .await
        .expect("create note")
        .note;
    svc.mark_as_task(
        ws.clone(),
        note.id.clone(),
        "not_started".into(),
        vec![],
        None,
        None,
        None,
        None,
    )
    .await
    .expect("mark task");
    note.id
}

#[intent_test_macros::daemon_test]
async fn specialist_provider_wake_create_resolves_effort_after_provider() {
    let (_tmp, svc, ws) = setup().await;
    for default_provider in ["grok", ""] {
        set(&svc, "model.defaultProvider", json!(default_provider));
        let task = seed_task(&svc, &ws).await;
        let result = svc
            .agent_wake_or_create_op(
                ws.clone(),
                task,
                "Do work".into(),
                AgentWakeOrCreateInput {
                    create: Some(AgentWakeCreateOptions {
                        specialist: Some("pin-alias".into()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await
            .expect("wake creates pinned agent");
        assert_eq!(result["created"], true);
        assert_pinned(&svc, result["agentId"].as_str().unwrap()).await;
    }
}

#[intent_test_macros::daemon_test]
async fn specialist_provider_wake_create_ignores_unselected_scalar_effort() {
    let (_tmp, svc, ws) = setup().await;
    // The selected model option supports high; the specialist's fallback
    // scalar low must neither override it nor cause a validation failure.
    svc.models_catalog.store_for_test(
        "auggie",
        &(crate::model_catalog::source_for("auggie")
            .unwrap()
            .version_key)(),
        vec![
            json!({"id": "pinned-model", "name": "pinned-model", "provider": "auggie",
                    "effortLevels": ["high"]}),
        ],
    );
    let task = seed_task(&svc, &ws).await;
    let result = svc
        .agent_wake_or_create_op(
            ws,
            task,
            "Do work".into(),
            AgentWakeOrCreateInput {
                create: Some(AgentWakeCreateOptions {
                    specialist: Some("pin-alias".into()),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await
        .expect("only the selected effort is validated");
    assert_pinned(&svc, result["agentId"].as_str().unwrap()).await;
}

#[intent_test_macros::daemon_test]
async fn specialist_provider_wake_create_preserves_explicit_choices() {
    let (_tmp, svc, ws) = setup().await;
    for (wake_effort, create_effort, provider, model, expected_effort) in [
        (Some("low"), Some("high"), None, None, Some("low")),
        (Some(""), Some("high"), None, None, None),
        (None, Some("low"), None, None, Some("low")),
        (None, Some(""), None, None, None),
        (None, None, Some("grok"), None, Some("low")),
        (None, None, None, Some("default-model"), Some("low")),
    ] {
        let task = seed_task(&svc, &ws).await;
        let result = svc
            .agent_wake_or_create_op(
                ws.clone(),
                task,
                "Do work".into(),
                AgentWakeOrCreateInput {
                    model: model.map(str::to_string),
                    reasoning_effort: wake_effort.map(str::to_string),
                    create: Some(AgentWakeCreateOptions {
                        specialist: Some("pin-alias".into()),
                        provider: provider.map(str::to_string),
                        reasoning_effort: create_effort.map(str::to_string),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await
            .expect("wake creates with explicit choices");
        let session = svc
            .store()
            .get_agent_session(&AgentId::from(result["agentId"].as_str().unwrap()))
            .await
            .expect("persisted session");
        let use_pin = provider.is_none() && model.is_none();
        assert_eq!(
            session.provider.as_deref(),
            if use_pin { Some("auggie") } else { provider }
        );
        assert_eq!(
            session.model.as_deref(),
            Some(if use_pin {
                "pinned-model"
            } else {
                "default-model"
            })
        );
        assert_eq!(session.reasoning_effort.as_deref(), expected_effort);
    }
}

#[tokio::test]
async fn specialist_provider_create_persists_pin_before_model_resolution() {
    let (_tmp, svc, ws) = setup().await;
    let created = create(&svc, &ws, None, AgentCreateExtra::default()).await;
    assert_pinned(&svc, created["agent"]["id"].as_str().unwrap()).await;
}

#[tokio::test]
async fn specialist_provider_preview_prefers_pin_to_settings_default() {
    let (_tmp, svc, _ws) = setup().await;
    let got = svc
        .specialist_get("pin-alias".into(), None, None)
        .await
        .expect("get");
    let list = svc.specialist_list(None, None).await.expect("list");
    let listed = list["specialists"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["id"] == "pinned")
        .unwrap();
    for def in [&got["specialist"], listed] {
        assert_eq!(def["resolvedProvider"], "auggie");
        assert_eq!(def["resolvedModel"], "pinned-model");
        assert_eq!(def["resolvedReasoningEffort"], "high");
    }
    let overridden = svc
        .specialist_get("pinned".into(), None, Some("grok".into()))
        .await
        .expect("explicit preview");
    assert_eq!(overridden["specialist"]["resolvedProvider"], "grok");
    assert_eq!(overridden["specialist"]["resolvedModel"], "default-model");
}

#[intent_test_macros::daemon_test]
async fn specialist_provider_workspace_api_create_and_delegate_agree() {
    let (_tmp, svc, ws) = setup().await;
    set(&svc, "workspaceApi.toonOutput", json!(false));
    let server = WorkspaceMcpServer::new(Arc::new(svc.clone()), ws.clone());
    let response = server.handle_message(&json!({
        "jsonrpc": "2.0", "id": 1, "method": "tools/call",
        "params": {"name": "workspace_api", "arguments": {
            "code": "return await ws.agent.create('Pinned', 'Do work', {specialist: 'pin-alias'});",
            "summary": "create specialist"
        }}
    })).await.expect("binding response");
    assert_ne!(response["result"]["isError"], true, "{response}");
    let result: Value =
        serde_json::from_str(response["result"]["content"][0]["text"].as_str().unwrap())
            .expect("binding JSON");
    assert_pinned(&svc, result["id"].as_str().expect("created id")).await;
    let delegated = svc
        .agent_delegate_op(
            ws,
            AgentDelegateInput {
                specialist: Some("pin-alias".into()),
                agent_instructions: Some("Do work".into()),
                ..Default::default()
            },
            None,
        )
        .await
        .expect("delegate");
    assert_pinned(&svc, delegated["agentId"].as_str().unwrap()).await;
}

#[tokio::test]
async fn specialist_provider_pin_works_without_a_configured_default() {
    let (_tmp, svc, ws) = setup().await;
    set(&svc, "model.defaultProvider", json!(""));
    let created = create(&svc, &ws, None, AgentCreateExtra::default()).await;
    assert_pinned(&svc, created["agent"]["id"].as_str().unwrap()).await;
}

#[tokio::test]
async fn specialist_provider_explicit_choices_and_effort_stay_authoritative() {
    let (_tmp, svc, ws) = setup().await;
    for (provider, model) in [
        (Some("grok"), None),
        (Some("grok"), Some("default-model")),
        (None, Some("default-model")),
    ] {
        let created = create(
            &svc,
            &ws,
            model,
            AgentCreateExtra {
                provider: provider.map(str::to_string),
                reasoning_effort: Some("high".into()),
                ..Default::default()
            },
        )
        .await;
        let agent = &created["agent"];
        assert_eq!(
            agent["provider"].as_str(),
            provider,
            "explicit model alone retains default-provider behavior"
        );
        assert_eq!(agent["model"], "default-model");
        assert_eq!(agent["reasoningEffort"], "high");
    }
    for effort in ["low", ""] {
        let created = create(
            &svc,
            &ws,
            None,
            AgentCreateExtra {
                reasoning_effort: Some(effort.into()),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(created["agent"]["provider"], "auggie");
        assert_eq!(
            created["agent"]["reasoningEffort"].as_str(),
            (!effort.is_empty()).then_some(effort)
        );
    }
    let err = svc
        .agent_create_op(
            ws,
            None,
            Some("pinned-model".into()),
            Some("pinned".into()),
            None,
            None,
            false,
            AgentCreateExtra::default(),
        )
        .await
        .expect_err("explicit model alone still checks settings provider ownership");
    assert!(
        err.to_string().contains("does not belong to provider grok"),
        "{err}"
    );
}

#[tokio::test]
async fn specialist_provider_uses_merged_tiers_and_preserves_an_explicit_clear() {
    let (tmp, svc, ws) = setup().await;
    let root = tmp.path.parent().unwrap();
    let user = root.join("specialists");
    let bundled = root.join("bundled");
    write_specialist(
        &bundled,
        "pinned",
        "codingAgent: auggie\nmodel: pinned-model\naliases: [\"pin-alias\"]\n",
    );
    write_specialist(&user, "pinned", "");
    let svc = svc.with_specialist_dirs(Some(user.clone()), Some(bundled));
    let inherited = create(&svc, &ws, None, AgentCreateExtra::default()).await;
    assert_eq!(inherited["agent"]["provider"], "auggie");
    assert_eq!(inherited["agent"]["model"], "pinned-model");

    let project = root.join("project");
    write_specialist(
        &project.join(".intent/specialists"),
        "pinned",
        "codingAgent: grok\nmodel: default-model\n",
    );
    let project_ws = WorkspaceId::new();
    let mut row = workspace(&project_ws);
    row.worktree_path = Some(project.to_string_lossy().into_owned());
    svc.store().insert_workspace(&row).await.unwrap();
    let project_created = create(&svc, &project_ws, None, AgentCreateExtra::default()).await;
    assert_eq!(project_created["agent"]["provider"], "grok");
    assert_eq!(project_created["agent"]["model"], "default-model");
    let preview = svc
        .specialist_get(
            "pin-alias".into(),
            Some(project.to_string_lossy().into_owned()),
            None,
        )
        .await
        .unwrap();
    assert_eq!(preview["specialist"]["resolvedProvider"], "grok");

    write_specialist(&user, "pinned", "codingAgent: \"\"\nmodel: \"\"\n");
    let unpinned = create(&svc, &ws, None, AgentCreateExtra::default()).await;
    assert!(unpinned["agent"]["provider"].is_null());
    assert_eq!(unpinned["agent"]["model"], "default-model");
    assert_eq!(unpinned["agent"]["reasoningEffort"], "low");
    let preview = svc
        .specialist_get("pin-alias".into(), None, None)
        .await
        .unwrap();
    assert_eq!(preview["specialist"]["resolvedProvider"], "grok");
}

#[intent_test_macros::daemon_test]
async fn specialist_provider_invalid_pins_leave_no_creation_side_effects() {
    let (tmp, svc, ws) = setup().await;
    let svc = svc
        .clone()
        .with_event_bus(crate::EventBus::new(svc.store().clone()));
    let _env = crate::agent_manager::tests::EnvGuard::apply(&[("MOCK_AGENT_SCRIPT_PATH", None)]);
    set(&svc, "workspaceApi.toonOutput", json!(false));
    let server = WorkspaceMcpServer::new(Arc::new(svc.clone()), ws.clone());
    let specialists = tmp.path.parent().unwrap().join("specialists");
    for (pin, message) in [
        ("typo", "unknown provider"),
        ("mock", "not available"),
        ("auggie", "not enabled"),
    ] {
        write_specialist(
            &specialists,
            "pinned",
            &format!("codingAgent: {pin}\nmodel: pinned-model\naliases: [\"pin-alias\"]\n"),
        );
        set(&svc, "providers.enabled", json!({"auggie": false}));
        let sessions = svc.store().list_all_agent_sessions().await.unwrap().len();
        let workspaces = svc.store().list_workspaces(true).await.unwrap().len();
        let notes = svc.store().list_all_notes().await.unwrap().len();
        let events = svc
            .store()
            .query_events(&EventQuery::default())
            .await
            .unwrap()
            .len();

        let rejected = svc
            .agent_create_op(
                ws.clone(),
                None,
                None,
                Some("pin-alias".into()),
                None,
                None,
                false,
                AgentCreateExtra::default(),
            )
            .await
            .expect_err("invalid pin");
        assert!(matches!(rejected, intent_core::Error::InvalidParams(_)));
        assert!(rejected.to_string().contains(message), "{rejected}");
        let rejected = svc
            .create_workspace(
                intent_core::WorkspaceCreate {
                    title: Some("Rejected".into()),
                    skip_isolation: Some(true),
                    initial_agent: Some(intent_core::WorkspaceCreateInitialAgent {
                        specialist: Some("pin-alias".into()),
                        prompt: Some("Do work".into()),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
                None,
            )
            .await
            .expect_err("invalid workspace initial-agent pin");
        assert!(matches!(rejected, intent_core::Error::InvalidParams(_)));
        assert!(rejected.to_string().contains(message), "{rejected}");
        let response = server.handle_message(&json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": "workspace_api", "arguments": {
                "code": "return await ws.agent.create('Rejected', 'Do work', {specialist: 'pin-alias'});",
                "summary": "reject specialist"
            }}
        })).await.unwrap();
        assert_eq!(response["result"]["isError"], true, "{response}");
        assert!(
            response["result"]["content"][0]["text"]
                .as_str()
                .unwrap()
                .contains(message),
            "{response}"
        );
        assert_eq!(
            svc.store().list_all_agent_sessions().await.unwrap().len(),
            sessions
        );
        assert_eq!(
            svc.store().list_workspaces(true).await.unwrap().len(),
            workspaces
        );
        assert_eq!(svc.store().list_all_notes().await.unwrap().len(), notes);
        assert_eq!(
            svc.store()
                .query_events(&EventQuery::default())
                .await
                .unwrap()
                .len(),
            events
        );
        assert!(!tmp.path.parent().unwrap().join("workspaces").exists());

        // An explicit choice bypasses even an unusable specialist pin.
        let overridden = create(
            &svc,
            &ws,
            Some("default-model"),
            AgentCreateExtra {
                provider: Some("grok".into()),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(overridden["agent"]["provider"], "grok");
        let model_only = create(
            &svc,
            &ws,
            Some("default-model"),
            AgentCreateExtra::default(),
        )
        .await;
        assert!(model_only["agent"]["provider"].is_null());
    }
}
