//! intent-hq/intent#8621: a blank or whitespace-only `model` on the MCP
//! `ws.agent.create` binding is treated as absent, so default-model
//! resolution runs and `session.model` is never `""`. The cold catalog cache
//! keeps the settings chain as the resolving rung.

use std::sync::Arc;

use intent_core::{WorkspaceApi, WorkspaceId};
use intent_store::Store;
use serde_json::json;
use tempfile::TempDir;

use super::tests::{workspace, TempDb};
use crate::Services;

async fn setup() -> (TempDb, Services, WorkspaceId, TempDir) {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");
    let config_dir = TempDir::new().expect("temp config dir");
    let registry = Arc::new(
        crate::SettingsRegistry::load(config_dir.path().join("config.toml"))
            .expect("load registry"),
    );
    registry
        .apply(&[
            ("model.defaultProvider".into(), json!("auggie")),
            ("model.default".into(), json!("fable-5")),
        ])
        .expect("seed defaults");
    let services = Services::new_isolated(store).with_settings_registry(registry);
    (tmp, services, ws, config_dir)
}

#[intent_test_macros::daemon_test]
async fn mcp_create_with_blank_model_resolves_the_default() {
    for blank in ["", "   "] {
        let (_t, svc, ws, _cfg) = setup().await;
        let api: Arc<dyn WorkspaceApi> = Arc::new(svc.clone());
        let server = intent_acp::WorkspaceMcpServer::new(api, ws.clone());
        let code = format!(
            "return await ws.agent.create('Helper', 'hi', {{ model: {m}, provider: {m}, specialist: 'chief-of-staff' }});",
            m = json!(blank)
        );
        let resp = server
            .handle_message(&json!({
                "jsonrpc": "2.0", "id": 1, "method": "tools/call",
                "params": { "name": "workspace_api",
                            "arguments": { "code": code, "summary": "blank model" } }
            }))
            .await
            .expect("tools/call response");
        assert_eq!(resp["result"]["isError"], json!(false), "{blank:?}: {resp}");
        let sessions = svc.store.list_all_agent_sessions().await.expect("list");
        assert_eq!(sessions.len(), 1, "{blank:?}: one agent created");
        let session = &sessions[0];
        assert_eq!(
            session.model.as_deref(),
            Some("fable-5"),
            "{blank:?}: a blank model must resolve the default, never persist \"\""
        );
    }
}
