//! Routing context is accepted at the transport boundary, without turning
//! daemon discovery/configuration into workspace-scoped service operations.
use super::*;
use serde_json::json;

async fn owner(srv: &Server) -> Guest {
    Guest {
        principal: srv.store.get_primary_principal().await.unwrap(),
        ws: connect_ws(srv.port, srv.cfg.clone()).await,
        next_id: 0,
    }
}

/// Compare full envelopes, normalizing only the request ID. No result fields
/// are discarded: even an added workspace-dependent field would fail this.
async fn equivalent(client: &mut Guest, method: &str, params: Value) -> Value {
    let direct = client.call(method, params.clone()).await;
    let mut contextual = params;
    contextual["workspaceId"] = json!("routing-only-workspace");
    let mut routed = client.call(method, contextual).await;
    routed["id"] = direct["id"].clone();
    assert_eq!(routed, direct, "{method} must retain its direct contract");
    direct
}

async fn success(client: &mut Guest, method: &str, params: Value) -> Value {
    let reply = equivalent(client, method, params).await;
    assert!(reply.get("error").is_none(), "{method}: {reply}");
    reply["result"].clone()
}

#[intent_test_macros::daemon_test]
async fn discovery_context_settings_mcp_and_host_reads() {
    let srv = start(WsOptions::default()).await;
    let mut client = owner(&srv).await;
    srv.set_setting("agents.acpNodeMaxOldSpaceMb", json!(4096));
    let before = std::fs::read(srv.dir.path().join("config.toml")).unwrap();
    let settings = success(&mut client, "settings.list", json!({})).await;
    assert!(settings.is_object());
    let setting = success(
        &mut client,
        "settings.get",
        json!({"path":"agents.acpNodeMaxOldSpaceMb"}),
    )
    .await;
    assert_eq!(setting["value"], 4096.0);
    assert_eq!(
        std::fs::read(srv.dir.path().join("config.toml")).unwrap(),
        before
    );

    // Seed configuration directly in the test service; no process or external
    // server is started by this disabled MCP fixture.
    srv.api
        .mcp_servers_create(json!({
            "id":"routing-fixture", "transport":"stdio", "command":"unused-fixture",
            "enabled":false, "env":{"FIXTURE_SECRET":"private-fixture-value"}
        }))
        .await
        .unwrap();
    let status = success(
        &mut client,
        "mcp.servers.getStatus",
        json!({"serverId":"routing-fixture"}),
    )
    .await;
    assert_eq!(status["status"]["state"], "stopped");
    assert!(!status.to_string().contains("private-fixture-value"));
    let redacted = success(&mut client, "settings.get", json!({"path":"mcp.servers"})).await;
    assert!(!redacted.to_string().contains("private-fixture-value"));
    assert!(redacted.to_string().contains("********"), "{redacted}");

    for (method, params) in [
        ("system.capabilities", json!({})),
        ("host.executionContext", json!({})),
        ("host.status", json!({})),
        ("host.env", json!({})),
        (
            "host.toolAvailability",
            json!({"tools":["sh", "intent-routing-missing-tool"]}),
        ),
        ("host.findBinary", json!({"name":"sh"})),
        ("host.checkGit", json!({})),
        ("host.checkNode", json!({})),
        ("host.checkGh", json!({})),
        ("host.checkAuggie", json!({})),
    ] {
        assert!(
            success(&mut client, method, params).await.is_object(),
            "{method}"
        );
    }
    for (method, params, code) in [
        ("settings.get", json!({"path":"not.a.setting"}), -32602),
        ("settings.get", json!({}), -32602),
        (
            "mcp.servers.getStatus",
            json!({"serverId":"missing"}),
            -32602,
        ),
        ("host.findBinary", json!({}), -32602),
        (
            "host.providerAuthStatus",
            json!({"providerId":"unknown-provider"}),
            -32602,
        ),
        ("host.providerAuthStatus", json!({"force":"yes"}), -32602),
    ] {
        assert_eq!(
            equivalent(&mut client, method, params).await["error"]["code"],
            code,
            "{method}"
        );
    }
    srv.ws.stop().await;
}

#[intent_test_macros::daemon_test]
async fn discovery_context_client_registry_is_daemon_wide() {
    let (srv, _) = super::authenticated_devices::start_roster().await;
    let mut a = owner(&srv).await;
    let mut b = owner(&srv).await;
    for (client, id) in [(&mut a, "routing-client-a"), (&mut b, "routing-client-b")] {
        let hello = client
            .call("client.hello", json!({"clientId":id,"name":id}))
            .await;
        assert!(hello.get("error").is_none(), "{hello}");
    }
    let listed = success(&mut a, "client.list", json!({})).await;
    let clients = listed["clients"].as_array().unwrap();
    assert_eq!(clients.len(), 2, "{listed}");
    for id in ["routing-client-a", "routing-client-b"] {
        assert!(clients.iter().any(|c| c["clientId"] == id), "{listed}");
    }
    srv.ws.stop().await;
}

#[cfg(unix)]
#[intent_test_macros::daemon_test]
async fn discovery_context_catalog_cache_visibility_and_readiness() {
    use std::os::unix::fs::PermissionsExt;
    let dir = test_tempdir("intentd-routing-models-");
    let bin = dir.path().join("auggie");
    let calls = dir.path().join("calls");
    std::fs::write(
        &bin,
        format!(
            r#"#!/bin/sh
if [ "$*" = "model list --json" ]; then
  printf 'probe\n' >> '{}'
  printf '%s\n' '{{"models":[{{"shortName":"routing-model","displayName":"Routing model"}}]}}'
elif [ "$*" = "model list" ]; then
  printf '%s\n' 'Available models:' '  - Routing model [routing-model]'
fi
exit 0
"#,
            calls.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&bin, std::fs::Permissions::from_mode(0o755)).unwrap();
    let srv = start_with_auggie(WsOptions::default(), Some(bin.clone())).await;
    srv.set_setting("providers.paths", json!({"auggie":bin}));
    let mut client = owner(&srv).await;
    for params in [json!({}), json!({"providerId":"auggie"})] {
        let catalog = success(&mut client, "models.list", params).await;
        assert_eq!(catalog["models"][0]["id"], "routing-model");
        assert_eq!(catalog["source"], "auggie");
    }
    assert_eq!(
        std::fs::read_to_string(&calls).unwrap().lines().count(),
        1,
        "context must share the daemon model cache"
    );
    success(
        &mut client,
        "models.list",
        json!({"providerId":"auggie","forceRefresh":true}),
    )
    .await;
    assert_eq!(
        std::fs::read_to_string(&calls).unwrap().lines().count(),
        3,
        "both forced calls must refresh"
    );
    let legacy = success(&mut client, "agent.getModels", json!({})).await;
    assert_eq!(legacy["models"][0]["id"], "routing-model");
    assert!(legacy.get("providerId").is_none());

    let providers = success(&mut client, "providers.catalog", json!({})).await;
    let cortex = providers["providers"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["id"] == "cortex")
        .unwrap();
    assert_eq!(cortex["visible"], false);
    let hidden = success(&mut client, "models.list", json!({"providerId":"cortex"})).await;
    assert_eq!(hidden["models"], json!([]));
    assert!(hidden["warning"]
        .as_str()
        .unwrap()
        .contains("INTENTD_ENABLE_CORTEX"));
    let auth = success(
        &mut client,
        "host.providerAuthStatus",
        json!({"providerId":"auggie","force":true}),
    )
    .await;
    assert_eq!(auth["providers"].as_array().unwrap().len(), 1);
    assert_eq!(auth["providers"][0]["authenticated"], true);

    // Discovery still heals missing daemon defaults for either request shape;
    // it must not create a workspace row or overwrite a user's existing choice.
    for context in [None, Some("routing-only-workspace")] {
        srv.set_setting("model.defaultProvider", json!(""));
        srv.set_setting("model.default", json!(""));
        let params = context.map_or(json!({}), |ws| json!({"workspaceId":ws}));
        let discovery = client.call("host.providerDiscovery", params).await;
        assert!(discovery.get("error").is_none(), "{discovery}");
        assert!(discovery["result"]["providers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["id"] == "auggie" && p["installed"] == true));
        let healed = srv
            .api
            .settings_get("model.defaultProvider".into())
            .await
            .unwrap();
        assert!(
            healed["value"].as_str().is_some_and(|p| !p.is_empty()),
            "{healed}"
        );
    }
    srv.set_setting("model.defaultProvider", json!("auggie"));
    srv.set_setting("model.default", json!("routing-model"));
    success(&mut client, "host.providerDiscovery", json!({})).await;
    assert_eq!(
        srv.api.settings_get("model.default".into()).await.unwrap()["value"],
        "routing-model"
    );
    srv.ws.stop().await;
}

#[intent_test_macros::daemon_test]
async fn discovery_context_specialist_tiers_and_explicit_project_writes() {
    let srv = start(WsOptions::default()).await;
    let mut client = owner(&srv).await;
    let project = srv.dir.path().join("project");
    let other_project = srv.dir.path().join("other-project");
    let mut workspace = fixture_workspace(&WorkspaceId::from("routing-only-workspace"));
    workspace.path = Some(other_project.to_string_lossy().into_owned());
    srv.store.insert_workspace(&workspace).await.unwrap();
    let project_specs = project.join(".intent/specialists");
    let user_specs = srv.dir.path().join("user-specialists");
    let bundled_specs = srv.dir.path().join("bundled-specialists");
    for (dir, name) in [
        (&project_specs, "Project"),
        (&user_specs, "User"),
        (&bundled_specs, "Bundled"),
    ] {
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(
            dir.join("routing-fixture.md"),
            format!("---\nname: {name}\nmodel: routing-model\n---\n{name} prompt\n"),
        )
        .unwrap();
    }
    let list = success(
        &mut client,
        "specialist.list",
        json!({"provider":"auggie", "workspacePath":project}),
    )
    .await;
    let row = list["specialists"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["id"] == "routing-fixture")
        .unwrap();
    assert_eq!(row["name"], "User", "list ignores project context");
    for (params, expected) in [
        (json!({"id":"routing-fixture","provider":"auggie"}), "User"),
        (
            json!({"id":"routing-fixture","provider":"auggie","workspacePath":project}),
            "Project",
        ),
    ] {
        let got = success(&mut client, "specialist.get", params).await;
        assert_eq!(got["specialist"]["name"], expected);
    }
    // Re-run the identical write lifecycle at the identical path. Comparing
    // bytes and responses proves routing context neither redirects writes nor
    // leaks into specialist frontmatter.
    let mut outcomes = Vec::new();
    for contextual in [false, true] {
        let mut results = Vec::new();
        for (method, spec) in [
            (
                "specialist.create",
                Some(json!({"name":"Created","behaviorPrompt":"First"})),
            ),
            (
                "specialist.edit",
                Some(json!({"name":"Edited","behaviorPrompt":"Second"})),
            ),
            ("specialist.delete", None),
        ] {
            let mut params = json!({"id":"written","scope":"project","workspacePath":project});
            if contextual {
                params["workspaceId"] = json!("routing-only-workspace");
            }
            if let Some(spec) = spec {
                params["spec"] = spec;
            }
            let mut reply = client.call(method, params).await;
            assert!(reply.get("error").is_none(), "{reply}");
            reply.as_object_mut().unwrap().remove("id");
            results.push((reply, std::fs::read(project_specs.join("written.md")).ok()));
        }
        assert!(!project_specs.join("written.md").exists());
        assert!(!user_specs.join("written.md").exists());
        outcomes.push(results);
    }
    assert_eq!(outcomes[0], outcomes[1]);
    assert!(!other_project.join(".intent/specialists").exists());
    for (method, params) in [
        (
            "specialist.create",
            json!({"id":"new","scope":"project","spec":{}}),
        ),
        (
            "specialist.edit",
            json!({"id":"routing-fixture","scope":"project","spec":{}}),
        ),
        (
            "specialist.delete",
            json!({"id":"routing-fixture","scope":"project"}),
        ),
        (
            "specialist.edit",
            json!({"id":"routing-fixture","workspacePath":project,"spec":{}}),
        ),
        (
            "specialist.delete",
            json!({"id":"routing-fixture","scope":"bundled"}),
        ),
        ("specialist.get", json!({"id":"missing"})),
        ("specialist.list", json!({"provider":"unknown-provider"})),
    ] {
        assert_eq!(
            equivalent(&mut client, method, params).await["error"]["code"],
            -32602,
            "{method}"
        );
    }
    srv.ws.stop().await;
}

#[intent_test_macros::daemon_test]
async fn discovery_context_does_not_grant_host_privileges() {
    let srv = start(WsOptions::default()).await;
    let mut guest = Guest::connect(&srv, &"e9".repeat(32)).await;
    let workspace = WorkspaceId::from("routing-only-workspace");
    srv.store
        .insert_workspace(&fixture_workspace(&workspace))
        .await
        .unwrap();
    let primary = srv.store.get_primary_principal().await.unwrap();
    srv.store
        .set_workspace_member_role(
            &workspace,
            &primary.id,
            intent_core::WorkspaceRole::Collaborator,
        )
        .await
        .unwrap();
    srv.store
        .add_workspace_member(
            &workspace,
            &guest.principal.id,
            intent_core::WorkspaceRole::Owner,
        )
        .await
        .unwrap();
    // Even owning the named workspace cannot grant daemon administration.
    for (method, params) in [
        ("settings.list", json!({})),
        ("settings.get", json!({"path":"model.default"})),
        ("mcp.servers.getStatus", json!({"serverId":"missing"})),
        ("host.providerDiscovery", json!({})),
        ("host.providerAuthStatus", json!({"providerId":"auggie"})),
        ("host.env", json!({})),
        (
            "specialist.create",
            json!({"id":"forbidden","scope":"project","workspacePath":srv.dir.path(),"spec":{}}),
        ),
    ] {
        assert_eq!(
            equivalent(&mut guest, method, params).await["error"]["code"],
            -32003,
            "{method}"
        );
    }
    assert!(!srv
        .dir
        .path()
        .join(".intent/specialists/forbidden.md")
        .exists());
    srv.ws.stop().await;
}
