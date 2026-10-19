//! Assistant-only, app-wide MCP server configuration.
//!
//! The existing service owns validation, secret redaction and lifecycle changes.
//! Updates replace the full configuration: callers should edit a configuration
//! returned by `listServers`, retaining redacted credential placeholders for
//! secrets they want to keep. OAuth credentials are never accessed here.

use std::sync::Arc;

use intent_core::{WorkspaceApi, WorkspaceId};
use serde_json::{json, Value};

use crate::mcp_server::bindings::{map_err, req_str};

pub(crate) const PRELUDE: &str = r"
    globalThis.ws = globalThis.ws || {};
    ws.app = ws.app || {};
    ws.app.mcp = {
        listServers: () => host({ method: 'app.mcp.listServers', args: {} }),
        createServer: (config) => host({ method: 'app.mcp.createServer', args: { config } }),
        updateServer: (serverId, config) => host({ method: 'app.mcp.updateServer', args: { serverId, config } }),
        setServerEnabled: (serverId, enabled) => host({ method: 'app.mcp.setServerEnabled', args: { serverId, enabled } }),
        getServerStatus: (serverId) => host({ method: 'app.mcp.getServerStatus', args: { serverId } }),
    };
";

pub(crate) async fn dispatch(
    api: &Arc<dyn WorkspaceApi>,
    workspace_id: &WorkspaceId,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    // Only the trusted caller context grants access, never an argument.
    if !workspace_id.is_chief() {
        return Err("ws.app.* is only available in the Assistant workspace".to_string());
    }

    match method {
        "listServers" => api.mcp_servers_list(None).await.map_err(map_err),
        "createServer" => create_server(api, args).await,
        "updateServer" => update_server(api, args).await,
        "setServerEnabled" => {
            let server_id = req_str(args, "serverId")?;
            let enabled = args
                .get("enabled")
                .and_then(Value::as_bool)
                .ok_or_else(|| "enabled is required and must be a boolean".to_string())?;
            api.mcp_servers_toggle(server_id, enabled, None)
                .await
                .map_err(map_err)
        }
        "getServerStatus" => api
            .mcp_servers_get_status(req_str(args, "serverId")?)
            .await
            .map_err(map_err),
        other => Err(format!("host: unknown method `app.mcp.{other}`")),
    }
}

fn config_arg(args: &Value) -> Result<&Value, String> {
    let config = args
        .get("config")
        .filter(|config| config.is_object())
        .ok_or_else(|| "config is required and must be an object".to_string())?;
    if config
        .get("enabled")
        .is_some_and(|enabled| !enabled.is_boolean())
    {
        return Err("config.enabled must be a boolean".to_string());
    }
    Ok(config)
}

async fn create_server(api: &Arc<dyn WorkspaceApi>, args: &Value) -> Result<Value, String> {
    let mut config = config_arg(args)?.clone();
    let enabled = config
        .get("enabled")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    config["enabled"] = json!(enabled);
    let created = api.mcp_servers_create(config).await.map_err(map_err)?;
    // Creation can generate an id; use the persisted definition, not the input.
    let server_id = created
        .get("server")
        .and_then(|server| server.get("id"))
        .and_then(Value::as_str)
        .ok_or_else(|| "mcp.servers.create returned no server id".to_string())?;
    let status = if enabled {
        api.mcp_servers_toggle(server_id.to_string(), true, None)
            .await
            .map_err(map_err)?
    } else {
        api.mcp_servers_get_status(server_id.to_string())
            .await
            .map_err(map_err)?
    };
    server_with_status(&created, &status)
}

async fn update_server(api: &Arc<dyn WorkspaceApi>, args: &Value) -> Result<Value, String> {
    let server_id = req_str(args, "serverId")?;
    // Full replacement, including the service's redacted-secret semantics.
    // The update API already reconciles the running server with the saved config.
    let updated = api
        .mcp_servers_update(server_id.clone(), config_arg(args)?.clone())
        .await
        .map_err(map_err)?;
    let status = api
        .mcp_servers_get_status(server_id)
        .await
        .map_err(map_err)?;
    server_with_status(&updated, &status)
}

fn server_with_status(server_result: &Value, status_result: &Value) -> Result<Value, String> {
    let server = server_result
        .get("server")
        .filter(|server| server.is_object())
        .ok_or_else(|| "mcp server API returned no server configuration".to_string())?;
    let status = status_result
        .get("status")
        .filter(|status| status.is_object())
        .ok_or_else(|| "mcp server API returned no status".to_string())?;
    Ok(json!({ "server": server, "status": status }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::{BoxFuture, Error};
    use std::sync::Mutex;

    #[derive(Debug, PartialEq)]
    enum Call {
        List(Option<WorkspaceId>),
        Create(Value),
        Update(String, Value),
        Toggle(String, bool, Option<WorkspaceId>),
        Status(String),
    }

    #[derive(Default)]
    struct FakeApi {
        calls: Mutex<Vec<Call>>,
        fail_on: Option<&'static str>,
    }

    fn api_error() -> Error {
        Error::InvalidParams("MCP service rejected the request".to_string())
    }

    fn redacted_server(enabled: bool) -> Value {
        json!({
            "id": "saved-server",
            "name": "Saved server",
            "transport": "http",
            "url": "https://mcp.example.test",
            "enabled": enabled,
            "env": { "TOKEN": "********" },
            "headers": { "Authorization": "********" },
        })
    }

    fn status_value(state: &str) -> Value {
        json!({ "serverId": "saved-server", "state": state })
    }

    impl FakeApi {
        fn response(&self, method: &str, call: Call, value: Value) -> intent_core::Result<Value> {
            self.calls.lock().unwrap().push(call);
            if self.fail_on == Some(method) {
                Err(api_error())
            } else {
                Ok(value)
            }
        }
    }

    impl WorkspaceApi for FakeApi {
        fn mcp_servers_list(
            &self,
            workspace_id: Option<WorkspaceId>,
        ) -> BoxFuture<'_, intent_core::Result<Value>> {
            Box::pin(async move {
                self.response(
                    "list",
                    Call::List(workspace_id),
                    json!({ "servers": [redacted_server(true)] }),
                )
            })
        }

        fn mcp_servers_create(&self, config: Value) -> BoxFuture<'_, intent_core::Result<Value>> {
            Box::pin(async move {
                let server = redacted_server(config["enabled"].as_bool().unwrap());
                self.response("create", Call::Create(config), json!({ "server": server }))
            })
        }

        fn mcp_servers_update(
            &self,
            server_id: String,
            config: Value,
        ) -> BoxFuture<'_, intent_core::Result<Value>> {
            Box::pin(async move {
                self.response(
                    "update",
                    Call::Update(server_id, config),
                    json!({ "server": redacted_server(true) }),
                )
            })
        }

        fn mcp_servers_toggle(
            &self,
            server_id: String,
            enabled: bool,
            workspace_id: Option<WorkspaceId>,
        ) -> BoxFuture<'_, intent_core::Result<Value>> {
            Box::pin(async move {
                self.response(
                    "toggle",
                    Call::Toggle(server_id, enabled, workspace_id),
                    json!({ "status": status_value(if enabled { "auth_required" } else { "stopped" }) }),
                )
            })
        }

        fn mcp_servers_get_status(
            &self,
            server_id: String,
        ) -> BoxFuture<'_, intent_core::Result<Value>> {
            Box::pin(async move {
                self.response(
                    "status",
                    Call::Status(server_id),
                    json!({ "status": status_value("stopped") }),
                )
            })
        }
    }

    fn input_config() -> Value {
        json!({
            "transport": "http",
            "url": "https://mcp.example.test",
            "headers": { "Authorization": "Bearer input-secret" },
        })
    }

    fn valid_calls() -> Vec<(&'static str, Value)> {
        vec![
            ("listServers", json!({})),
            ("createServer", json!({ "config": input_config() })),
            (
                "updateServer",
                json!({ "serverId": "saved-server", "config": input_config() }),
            ),
            (
                "setServerEnabled",
                json!({ "serverId": "saved-server", "enabled": true }),
            ),
            ("getServerStatus", json!({ "serverId": "saved-server" })),
        ]
    }

    #[tokio::test]
    async fn ordinary_workspace_cannot_read_or_mutate_even_with_spoofed_workspace_ids() {
        let fake = Arc::new(FakeApi::default());
        let api: Arc<dyn WorkspaceApi> = fake.clone();
        for (method, args) in valid_calls() {
            let mut spoofed = args.clone();
            spoofed["workspaceId"] = json!(WorkspaceId::chief());
            spoofed["workspace_id"] = json!(WorkspaceId::chief());
            spoofed["isChief"] = json!(true);
            for input in [args, spoofed, Value::Null] {
                let err = dispatch(
                    &api,
                    &WorkspaceId::from_string("ordinary-workspace"),
                    method,
                    &input,
                )
                .await
                .unwrap_err();
                assert_eq!(err, "ws.app.* is only available in the Assistant workspace");
            }
        }
        assert!(fake.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn list_returns_redacted_api_result_without_workspace_scope() {
        let fake = Arc::new(FakeApi::default());
        let api: Arc<dyn WorkspaceApi> = fake.clone();
        let result = dispatch(
            &api,
            &WorkspaceId::chief(),
            "listServers",
            &json!({ "workspaceId": "ordinary-workspace" }),
        )
        .await
        .unwrap();
        assert_eq!(result, json!({ "servers": [redacted_server(true)] }));
        assert_eq!(*fake.calls.lock().unwrap(), vec![Call::List(None)]);
    }

    #[tokio::test]
    async fn create_defaults_to_enabled_and_starts_the_persisted_server() {
        for enabled in [None, Some(true), Some(false)] {
            let fake = Arc::new(FakeApi::default());
            let api: Arc<dyn WorkspaceApi> = fake.clone();
            let mut config = input_config();
            if let Some(enabled) = enabled {
                config["enabled"] = json!(enabled);
            }
            let result = dispatch(
                &api,
                &WorkspaceId::chief(),
                "createServer",
                &json!({ "config": config }),
            )
            .await
            .unwrap();
            let enabled = enabled.unwrap_or(true);
            config["enabled"] = json!(enabled);
            let follow_up = if enabled {
                Call::Toggle("saved-server".to_string(), true, None)
            } else {
                Call::Status("saved-server".to_string())
            };
            assert_eq!(
                *fake.calls.lock().unwrap(),
                vec![Call::Create(config), follow_up]
            );
            assert_eq!(
                result,
                json!({
                    "server": redacted_server(enabled),
                    "status": status_value(if enabled { "auth_required" } else { "stopped" }),
                })
            );
            assert!(!result.to_string().contains("input-secret"));
        }
    }

    #[tokio::test]
    async fn update_forwards_full_configuration_without_merging_or_defaulting() {
        for config in [redacted_server(true), input_config()] {
            let fake = Arc::new(FakeApi::default());
            let api: Arc<dyn WorkspaceApi> = fake.clone();
            let result = dispatch(
                &api,
                &WorkspaceId::chief(),
                "updateServer",
                &json!({ "serverId": "saved-server", "config": config }),
            )
            .await
            .unwrap();
            assert_eq!(
                *fake.calls.lock().unwrap(),
                vec![
                    Call::Update("saved-server".to_string(), config),
                    Call::Status("saved-server".to_string()),
                ]
            );
            assert_eq!(
                result,
                json!({ "server": redacted_server(true), "status": status_value("stopped") })
            );
            assert!(!result.to_string().contains("input-secret"));
        }
    }

    #[tokio::test]
    async fn enable_and_disable_delegate_globally_and_return_api_status() {
        for enabled in [true, false] {
            let fake = Arc::new(FakeApi::default());
            let api: Arc<dyn WorkspaceApi> = fake.clone();
            let result = dispatch(
                &api,
                &WorkspaceId::chief(),
                "setServerEnabled",
                &json!({ "serverId": "saved-server", "enabled": enabled, "workspaceId": "other" }),
            )
            .await
            .unwrap();
            assert_eq!(
                *fake.calls.lock().unwrap(),
                vec![Call::Toggle("saved-server".to_string(), enabled, None)]
            );
            assert_eq!(
                result,
                json!({ "status": status_value(if enabled { "auth_required" } else { "stopped" }) })
            );
        }
    }

    #[tokio::test]
    async fn get_status_returns_api_envelope() {
        let fake = Arc::new(FakeApi::default());
        let api: Arc<dyn WorkspaceApi> = fake.clone();
        let result = dispatch(
            &api,
            &WorkspaceId::chief(),
            "getServerStatus",
            &json!({ "serverId": "saved-server" }),
        )
        .await
        .unwrap();
        assert_eq!(result, json!({ "status": status_value("stopped") }));
        assert_eq!(
            *fake.calls.lock().unwrap(),
            vec![Call::Status("saved-server".to_string())]
        );
    }

    #[tokio::test]
    async fn invalid_required_arguments_never_reach_the_api() {
        let fake = Arc::new(FakeApi::default());
        let api: Arc<dyn WorkspaceApi> = fake.clone();
        let invalid = [
            Value::Null,
            json!(false),
            json!(7),
            json!("invalid"),
            json!([]),
        ];
        for (method, mut args) in valid_calls().into_iter().skip(1) {
            assert!(dispatch(&api, &WorkspaceId::chief(), method, &json!({}))
                .await
                .is_err());
            let key = if method == "createServer" {
                "config"
            } else {
                "serverId"
            };
            for bad in &invalid {
                // A string is valid for serverId; all other types must fail.
                if key == "serverId" && bad.is_string() {
                    continue;
                }
                args[key] = bad.clone();
                assert!(dispatch(&api, &WorkspaceId::chief(), method, &args)
                    .await
                    .is_err());
            }
        }
        for method in ["setServerEnabled", "updateServer"] {
            assert!(dispatch(
                &api,
                &WorkspaceId::chief(),
                method,
                &json!({ "serverId": "saved-server" }),
            )
            .await
            .is_err());
        }
        for bad in [
            Value::Null,
            json!(7),
            json!("invalid"),
            json!([]),
            json!({}),
        ] {
            assert!(dispatch(
                &api,
                &WorkspaceId::chief(),
                "setServerEnabled",
                &json!({ "serverId": "saved-server", "enabled": bad }),
            )
            .await
            .is_err());
            for method in ["createServer", "updateServer"] {
                assert!(dispatch(
                    &api,
                    &WorkspaceId::chief(),
                    method,
                    &json!({ "serverId": "saved-server", "config": { "enabled": bad } }),
                )
                .await
                .is_err());
            }
        }
        for config in &invalid {
            assert!(dispatch(
                &api,
                &WorkspaceId::chief(),
                "updateServer",
                &json!({ "serverId": "saved-server", "config": config }),
            )
            .await
            .is_err());
        }
        assert!(fake.calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn service_errors_propagate_without_follow_up_calls() {
        for ((method, args), failing_api) in valid_calls()
            .into_iter()
            .zip(["list", "create", "update", "toggle", "status"])
        {
            let fake = Arc::new(FakeApi {
                fail_on: Some(failing_api),
                ..FakeApi::default()
            });
            let api: Arc<dyn WorkspaceApi> = fake.clone();
            assert_eq!(
                dispatch(&api, &WorkspaceId::chief(), method, &args)
                    .await
                    .unwrap_err(),
                api_error().to_string()
            );
            assert_eq!(fake.calls.lock().unwrap().len(), 1);
        }
    }

    #[tokio::test]
    async fn errors_after_persistence_are_not_reported_as_success() {
        for (method, args, failing_api) in [
            (
                "createServer",
                json!({ "config": input_config() }),
                "toggle",
            ),
            (
                "createServer",
                json!({ "config": { "command": "test", "enabled": false } }),
                "status",
            ),
            (
                "updateServer",
                json!({ "serverId": "saved-server", "config": input_config() }),
                "status",
            ),
        ] {
            let fake = Arc::new(FakeApi {
                fail_on: Some(failing_api),
                ..FakeApi::default()
            });
            let api: Arc<dyn WorkspaceApi> = fake.clone();
            assert_eq!(
                dispatch(&api, &WorkspaceId::chief(), method, &args)
                    .await
                    .unwrap_err(),
                api_error().to_string()
            );
            assert_eq!(fake.calls.lock().unwrap().len(), 2);
        }
    }

    #[tokio::test]
    async fn unknown_method_never_reaches_api() {
        let fake = Arc::new(FakeApi::default());
        let api: Arc<dyn WorkspaceApi> = fake.clone();
        assert_eq!(
            dispatch(&api, &WorkspaceId::chief(), "oauthToken", &json!({}))
                .await
                .unwrap_err(),
            "host: unknown method `app.mcp.oauthToken`"
        );
        assert!(fake.calls.lock().unwrap().is_empty());
    }
}
