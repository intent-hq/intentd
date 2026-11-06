//! intent-hq/intent#8621: `agent.create` over JSON-RPC with a blank or
//! whitespace-only `model` runs default-model resolution against a real
//! `Services`, so `session.model` is the settings default, never `""`. The
//! model catalog cache stays cold.

use std::sync::Arc;

use intent_core::{AgentId, WorkspaceApi, WorkspaceId};
use intent_services::{Services, SettingsRegistry};
use intent_store::Store;
use serde_json::{json, Value};

#[tokio::test]
async fn rpc_create_with_blank_model_resolves_the_default() {
    for blank in ["", "   "] {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(&dir.path().join("intent.db"))
            .await
            .expect("open store");
        let mut workspace = intent_core::chief_workspace();
        workspace.id = WorkspaceId::new();
        store.insert_workspace(&workspace).await.expect("ws");
        let registry = Arc::new(
            SettingsRegistry::load(dir.path().join("config.toml")).expect("load registry"),
        );
        registry
            .apply(&[
                ("model.defaultProvider".into(), json!("auggie")),
                ("model.default".into(), json!("fable-5")),
            ])
            .expect("seed defaults");
        let services = Services::new_isolated(store).with_settings_registry(registry);
        let request = json!({
            "jsonrpc": "2.0", "id": 1, "method": "agent.create",
            "params": {
                "workspaceId": workspace.id,
                "name": "Helper",
                "model": blank,
                "provider": blank,
                "specialistId": "chief-of-staff",
            }
        });
        let response = intent_core::with_caller(
            intent_core::Caller::Daemon,
            Box::pin(super::handle_message(&services, &request.to_string())),
        )
        .await
        .expect("response");
        let response: Value = serde_json::from_str(&response).expect("json");
        let id = response["result"]["agent"]["id"]
            .as_str()
            .unwrap_or_else(|| panic!("{blank:?}: create failed: {response}"));
        let agent = intent_core::with_caller(
            intent_core::Caller::Daemon,
            Box::pin(services.agent_get(AgentId::from(id), Some(workspace.id.clone()))),
        )
        .await
        .expect("agent.get");
        assert_eq!(
            agent.model.as_deref(),
            Some("fable-5"),
            "{blank:?}: a blank model must resolve the default, never persist \"\""
        );
    }
}
