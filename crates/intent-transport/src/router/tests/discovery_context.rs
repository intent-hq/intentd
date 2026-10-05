//! Fixture API exposes exactly the semantic arguments the router forwarded.
use intent_core::{BoxFuture, Result, WorkspaceApi};
use serde_json::{json, Value};

struct DiscoveryFixture;

fn reply(value: Value) -> BoxFuture<'static, Result<Value>> {
    Box::pin(async move { Ok(value) })
}

impl WorkspaceApi for DiscoveryFixture {
    fn models_list(&self, provider: Option<String>, refresh: bool) -> BoxFuture<'_, Result<Value>> {
        reply(json!({"provider":provider,"refresh":refresh}))
    }
    fn agent_get_models(&self) -> BoxFuture<'_, Result<Value>> {
        reply(json!({"models":[{"id":"fixture-model"}]}))
    }
    fn providers_catalog(&self) -> BoxFuture<'_, Result<Value>> {
        reply(json!({"providers":[{"id":"fixture-provider","visible":false}]}))
    }
    fn settings_list(&self) -> BoxFuture<'_, Result<Value>> {
        reply(json!({"settings":[{"path":"fixture.setting","value":"redacted"}]}))
    }
    fn settings_get(&self, path: String) -> BoxFuture<'_, Result<Value>> {
        reply(json!({"path":path}))
    }
    fn mcp_servers_get_status(&self, server_id: String) -> BoxFuture<'_, Result<Value>> {
        reply(json!({"serverId":server_id}))
    }
    fn specialist_list(
        &self,
        path: Option<String>,
        provider: Option<String>,
    ) -> BoxFuture<'_, Result<Value>> {
        reply(json!({"path":path,"provider":provider}))
    }
    fn specialist_get(
        &self,
        id: String,
        path: Option<String>,
        provider: Option<String>,
    ) -> BoxFuture<'_, Result<Value>> {
        reply(json!({"id":id,"path":path,"provider":provider}))
    }
    fn specialist_create(
        &self,
        id: String,
        spec: Value,
        scope: Option<String>,
        path: Option<String>,
    ) -> BoxFuture<'_, Result<Value>> {
        reply(json!({"id":id,"spec":spec,"scope":scope,"path":path}))
    }
    fn specialist_edit(
        &self,
        id: String,
        spec: Value,
        scope: String,
        path: Option<String>,
    ) -> BoxFuture<'_, Result<Value>> {
        reply(json!({"id":id,"spec":spec,"scope":scope,"path":path}))
    }
    fn specialist_delete(
        &self,
        id: String,
        scope: String,
        path: Option<String>,
    ) -> BoxFuture<'_, Result<Value>> {
        reply(json!({"id":id,"scope":scope,"path":path}))
    }
}

#[tokio::test]
async fn discovery_context_preserves_semantic_arguments() {
    for (method, params, expected) in [
        (
            "models.list",
            json!({"providerId":"fixture","forceRefresh":true}),
            json!({"provider":"fixture","refresh":true}),
        ),
        (
            "models.list",
            json!({}),
            json!({"provider":null,"refresh":false}),
        ),
        (
            "agent.getModels",
            json!({}),
            json!({"models":[{"id":"fixture-model"}]}),
        ),
        (
            "providers.catalog",
            json!({}),
            json!({"providers":[{"id":"fixture-provider","visible":false}]}),
        ),
        (
            "settings.list",
            json!({}),
            json!({"settings":[{"path":"fixture.setting","value":"redacted"}]}),
        ),
        (
            "settings.get",
            json!({"path":"fixture.setting"}),
            json!({"path":"fixture.setting"}),
        ),
        (
            "mcp.servers.getStatus",
            json!({"serverId":"server"}),
            json!({"serverId":"server"}),
        ),
        (
            "specialist.list",
            json!({"provider":"fixture","workspacePath":"explicit-project"}),
            json!({"path":null,"provider":"fixture"}),
        ),
        (
            "specialist.get",
            json!({"id":"spec","provider":"fixture","workspacePath":"explicit-project"}),
            json!({"id":"spec","path":"explicit-project","provider":"fixture"}),
        ),
        (
            "specialist.create",
            json!({"id":"spec","scope":"project","workspacePath":"explicit-project","spec":{"name":"Create"}}),
            json!({"id":"spec","scope":"project","path":"explicit-project","spec":{"name":"Create"}}),
        ),
        (
            "specialist.edit",
            json!({"id":"spec","scope":"project","workspacePath":"explicit-project","spec":{"name":"Edit"}}),
            json!({"id":"spec","scope":"project","path":"explicit-project","spec":{"name":"Edit"}}),
        ),
        (
            "specialist.delete",
            json!({"id":"spec","scope":"project","workspacePath":"explicit-project"}),
            json!({"id":"spec","scope":"project","path":"explicit-project"}),
        ),
    ] {
        for context in [None, Some("workspace-a"), Some("workspace-b")] {
            let mut params = params.clone();
            if let Some(ws) = context {
                params["workspaceId"] = json!(ws);
            }
            let request = json!({"jsonrpc":"2.0","id":7,"method":method,"params":params});
            let response = super::handle_message(&DiscoveryFixture, &request.to_string())
                .await
                .unwrap();
            let response: Value = serde_json::from_str(&response).unwrap();
            assert_eq!(
                response,
                json!({"jsonrpc":"2.0","id":7,"result":expected}),
                "{method}: {context:?}"
            );
        }
    }
}
