//! Actual pinned Claude through the production daemon, authenticated WSS, and
//! a loopback model. The shared fixture owns synthetic auth and source sentinels.
use super::*;

#[tokio::test]
#[ignore = "requires Linux bwrap and pinned packages in INTENT_CLAUDE_FIXTURE_MODULES"]
async fn native_managed_catalog_over_wss() {
    if std::env::var_os("INTENT_MANAGED_SESSION_FIXTURE").is_none() {
        let modules = std::env::var("INTENT_CLAUDE_FIXTURE_MODULES").expect("pinned modules");
        let output =
            tokio::process::Command::new("node")
                .env_remove("NODE_OPTIONS")
                .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join(
                    "../intent-services/src/provider_profile/fixtures/acquired-claude-acp.mjs",
                ))
                .arg(modules)
                .arg(std::env::current_exe().unwrap())
                .arg("managed_provider::native_managed_catalog_over_wss")
                .arg("wss")
                .output()
                .await
                .unwrap();
        assert!(
            output.status.success(),
            "{}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        println!("{}", String::from_utf8_lossy(&output.stdout));
        return;
    }
    let cwd = std::env::current_dir().unwrap();
    let data = PathBuf::from(std::env::var_os("INTENT_ACP_FIXTURE_STATE").unwrap());
    let ws_id = seed_workspace_only(&data).await;
    let store = intent_store::Store::open(&data.join("intentd.db"))
        .await
        .unwrap();
    let mut workspace = store
        .get_workspace(&intent_core::WorkspaceId::from(ws_id.clone()))
        .await
        .unwrap();
    workspace.worktree_path = Some(cwd.to_string_lossy().into_owned());
    store.update_workspace(&workspace).await.unwrap();
    drop(store);
    // Intent's disabled stable-ID entry shadows the project default by name.
    std::fs::write(data.join("secrets.json"), json!({"mcp.servers": json!({"disabled-id":{"name":"ambient","enabled":false}}).to_string()}).to_string()).unwrap();
    let mut agent_id = None;
    let mut native_session = None;
    for (index, text) in [
        "MANAGED-FIRST-TURN",
        "MANAGED-RESUMED-TURN",
        "MANAGED-MUST-FAIL",
        "DEFERRED-EXTERNAL-TURN",
    ]
    .iter()
    .enumerate()
    {
        std::fs::write(cwd.parent().unwrap().join("wss-phase"), index.to_string()).unwrap();
        if index == 2 {
            // Applicable policy changes after selection must fail closed. The
            // original home/project MCP sentinels must still never start.
            std::fs::write(
                cwd.parent().unwrap().join("admin/managed-settings.json"),
                r#"{"deniedMcpServers":[{"serverName":"workspace-mcp"}]}"#,
            )
            .unwrap();
        }
        if index == 3 {
            for name in ["host", "ambient"] {
                assert!(
                    !cwd.parent().unwrap().join(name).exists(),
                    "managed launches must never start ambient MCP"
                );
            }
            std::fs::remove_file(cwd.parent().unwrap().join("admin/managed-settings.json"))
                .unwrap();
            let approved: Value =
                serde_json::from_slice(&std::fs::read(cwd.join("approved.json")).unwrap()).unwrap();
            let mut server = approved["approved"].clone();
            server["enabled"] = json!(true);
            server["name"] = json!("approved");
            std::fs::write(data.join("secrets.json"), json!({"mcp.servers":json!({"external-id":server,"disabled-id":{"name":"ambient","enabled":false}}).to_string()}).to_string()).unwrap();
            agent_id = None;
        }
        let mut daemon = Daemon {
            child: spawn_serve(&data, "both", &[("INTENTD_AUTH_TOKEN", TOKEN)]),
        };
        let socket = data.join("intentd.sock");
        assert!(
            await_uds(&socket).await,
            "{}",
            std::fs::read_to_string(data.join("daemon.log")).unwrap()
        );
        let status = common::await_wss_status(&socket).await;
        let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
        let cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
        let mut sub = connect_ws(port, cfg.clone()).await;
        let subscribed = wss_rpc(
            &mut sub,
            1,
            "events.subscribe",
            json!({"eventTypes":["agent:*"],"workspaceId":ws_id}),
        )
        .await;
        assert!(subscribed["subscriptionId"].is_string());
        let mut rpc = connect_ws(port, cfg).await;
        if agent_id.is_none() {
            let created = wss_rpc(&mut rpc, 2, "agent.create", json!({"workspaceId":ws_id,"name":"Managed WSS","model":"claude-sonnet-4-6","provider":"claude-code"})).await;
            agent_id = Some(created["agent"]["id"].as_str().unwrap().to_owned());
        }
        let id = agent_id.as_ref().unwrap();
        let sent = wss_rpc(
            &mut rpc,
            3,
            "agent.sendMessage",
            json!({"workspaceId":ws_id,"agentId":id,"content":text}),
        )
        .await;
        assert_eq!(sent["success"], true);
        let mut ended = false;
        let mut error = false;
        for _ in 0..150 {
            let frame = wss_event(&mut sub, 60).await;
            assert_eq!(frame["jsonrpc"], "2.0");
            let event = &frame["params"]["event"];
            if event["type"] == "agent:failed" {
                error = true;
                assert!(
                    event["data"]["error"]
                        .as_str()
                        .is_some_and(|reason| reason.contains("policy")
                            || reason.contains("Managed session configuration")),
                    "{event}"
                );
            }
            if event["type"] == "agent:stream:end" {
                ended = true;
                break;
            }
        }
        assert!(ended);
        let fetched = wss_rpc(&mut rpc, 4, "agent.get", json!({"agentId":id})).await;
        let session = &fetched["agent"];
        // Read the private marker without opening a second daemon Store/writer.
        let options = sqlx::sqlite::SqliteConnectOptions::new()
            .filename(data.join("intentd.db"))
            .read_only(true);
        let pool = sqlx::SqlitePool::connect_with(options).await.unwrap();
        let metadata: Option<String> =
            sqlx::query_scalar("SELECT metadata FROM agent_session WHERE id = ?")
                .bind(id)
                .fetch_one(&pool)
                .await
                .unwrap();
        pool.close().await;
        let persisted: Value = serde_json::from_str(metadata.as_deref().unwrap_or("{}")).unwrap();

        if index == 3 {
            assert!(!error);
            assert!(
                persisted.get("intentManagedClaudeProfile").is_none(),
                "enabled external catalog must preserve legacy behavior"
            );
        } else {
            assert!(
                persisted.get("intentManagedClaudeProfile").is_some(),
                "production route must select managed: {session:?}; persisted={:?}; daemon={}",
                persisted,
                std::fs::read_to_string(data.join("daemon.log")).unwrap()
            );
            if index < 2 {
                assert!(
                    !error,
                    "{}",
                    std::fs::read_to_string(data.join("daemon.log")).unwrap()
                );
                if let Some(first) = &native_session {
                    assert_eq!(&session["acpSessionId"], first);
                } else {
                    assert!(session["acpSessionId"].is_string(), "{session}");
                    native_session = Some(session["acpSessionId"].clone());
                }
            } else {
                assert!(error, "denied bridge must not silently fall back");
            }
        }
        drop(rpc);
        drop(sub);
        nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(daemon.child.id().cast_signed()),
            nix::sys::signal::Signal::SIGTERM,
        )
        .unwrap();
        timeout(Duration::from_secs(30), async {
            while daemon.child.try_wait().unwrap().is_none() {
                // timing-guard: poll observed daemon exit before restart, not a fixed readiness delay.
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .expect("daemon shutdown");
        drop(daemon);
    }
    println!("PASS actual WSS managed lifecycle: create, daemon restart/load, stable-ID tombstone, changed policy fails closed");
}
