//! Member execution through a disposable real daemon and mock ACP provider.
use super::*;
use intent_core::{now_iso, Principal, PrincipalId, WorkspaceId};
use intent_store::Store;
use std::fmt::Write as _;

const MEMBER_TOKEN: &str = "f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1f1";

async fn seed_member(data_dir: &Path, ws: &WorkspaceId) -> PrincipalId {
    let store = Store::open(&data_dir.join("intentd.db")).await.unwrap();
    let path = data_dir.join("workspaces").join(ws.as_str());
    std::fs::create_dir_all(&path).unwrap();
    let mut workspace = workspace_seed(ws);
    workspace.path = Some(path.to_string_lossy().into_owned());
    workspace.worktree_path = workspace.path.clone();
    store.insert_workspace(&workspace).await.unwrap();
    let member = Principal {
        id: PrincipalId::new(),
        identity: None,
        github_user_id: None,
        login: Some("member".into()),
        display_name: Some("Member".into()),
        avatar_url: None,
        is_primary: false,
        created_at: now_iso(),
        updated_at: now_iso(),
    };
    store.upsert_principal(&member).await.unwrap();
    let mut hash = String::with_capacity(64);
    for byte in Sha256::digest(MEMBER_TOKEN.as_bytes()) {
        write!(hash, "{byte:02x}").unwrap();
    }
    store
        .insert_principal_credential(&member.id, &hash)
        .await
        .unwrap();
    sqlx::query("INSERT INTO host_member(principal_id,added_at) VALUES (?,?)")
        .bind(member.id.as_str())
        .bind(now_iso())
        .execute(store.write_pool())
        .await
        .unwrap();
    member.id
}

async fn member_connection(
    port: u16,
    cfg: Arc<ClientConfig>,
) -> WebSocketStream<tokio_rustls::client::TlsStream<TcpStream>> {
    common::wss_connect_with_retry(
        port,
        cfg,
        &format!("wss://localhost:{port}/ws?token={MEMBER_TOKEN}"),
    )
    .await
}

#[tokio::test]
async fn member_provider_enablement_commits_safe_snapshots_over_wss_and_restart() {
    use std::os::unix::fs::PermissionsExt as _;
    let dir = temp_data_dir();
    let ws = WorkspaceId::new();
    seed_member(dir.path(), &ws).await;
    let store = Store::open(&dir.path().join("intentd.db")).await.unwrap();
    let guest_token = "ad".repeat(32);
    let guest = Principal {
        id: PrincipalId::new(),
        identity: None,
        github_user_id: None,
        login: None,
        display_name: None,
        avatar_url: None,
        is_primary: false,
        created_at: now_iso(),
        updated_at: now_iso(),
    };
    store.upsert_principal(&guest).await.unwrap();
    let mut hash = String::with_capacity(64);
    for byte in Sha256::digest(guest_token.as_bytes()) {
        write!(hash, "{byte:02x}").unwrap();
    }
    store
        .insert_principal_credential(&guest.id, &hash)
        .await
        .unwrap();
    store
        .add_workspace_member(&ws, &guest.id, intent_core::WorkspaceRole::Collaborator)
        .await
        .unwrap();
    let probe = dir.path().join("fake-auggie");
    let calls = dir.path().join("probe-calls");
    std::fs::write(
        &probe,
        format!(
            "#!/bin/sh\necho invoked >> '{}'\necho private-provider-body\nexit 1\n",
            calls.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&probe, std::fs::Permissions::from_mode(0o700)).unwrap();
    let config = dir.path().join("config.toml");
    std::fs::write(&config, format!(
        "[sourceControl.github]\ntokenSource = 'explicit'\napiBaseUrl = 'http://127.0.0.1:9'\n[providers.paths]\nauggie = '{}'\ncodex = '/private/uninstalled-provider'\n",
        probe.display()
    )).unwrap();
    std::fs::write(
        dir.path().join("secrets.json"),
        r#"{"collaboration.github.token":"private-identity-only-token"}"#,
    )
    .unwrap();
    let env = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("GITHUB_TOKEN", ""),
        ("GH_TOKEN", ""),
        ("GITLAB_TOKEN", ""),
        ("INTENTD_GITHUB_API_BASE_URI", "http://127.0.0.1:9"),
    ];
    let daemon = Daemon {
        child: spawn_serve(dir.path(), "both", &env),
    };
    let socket = dir.path().join("intentd.sock");
    assert!(await_uds(&socket).await);
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
    let mut owner = connect_ws(port, cfg.clone()).await;
    let mut member = member_connection(port, cfg.clone()).await;
    let mut observer = member_connection(port, cfg.clone()).await;
    let mut owner_observer = connect_ws(port, cfg.clone()).await;
    let mut guest_client = common::wss_connect_with_retry(
        port,
        cfg,
        &format!("wss://localhost:{port}/ws?token={guest_token}"),
    )
    .await;
    for client in [&mut observer, &mut owner_observer, &mut guest_client] {
        wss_rpc(
            client,
            1,
            "events.subscribe",
            json!({"eventTypes":["host:execution-context-changed","settings:changed"]}),
        )
        .await;
    }
    let before_calls = std::fs::read(&calls).unwrap_or_default();
    let mut all = intent_providers::all_provider_ids();
    all.sort_unstable();
    all.dedup();
    let without_auggie: Vec<_> = all.iter().copied().filter(|id| *id != "auggie").collect();
    let before = wss_rpc_envelope(&mut member, 2, "host.executionContext", json!({})).await;
    assert_eq!(before["jsonrpc"], "2.0");
    assert_eq!(before["id"], 2);
    assert!(before.get("error").is_none(), "{before}");
    assert_eq!(before["result"]["enabledProviderIds"], json!(all));
    let before = before["result"].clone();
    assert_eq!(
        before,
        wss_rpc(&mut owner, 2, "host.executionContext", json!({})).await
    );
    assert_eq!(
        before["repositoryConnections"][0]["configured"], false,
        "identity-only credentials do not configure execution"
    );
    for (method, params) in [
        ("host.executionContext", json!({})),
        ("settings.get", json!({"path":"providers.enabled"})),
    ] {
        let denied = wss_rpc_envelope(&mut guest_client, 3, method, params).await;
        assert_eq!(denied["error"]["code"], -32003, "{denied}");
    }
    for (method, params) in [
        ("settings.get", json!({"path":"providers.enabled"})),
        (
            "settings.update",
            json!({"changes":[{"path":"providers.enabled","value":{}}]}),
        ),
        ("settings.reset", json!({"path":"providers.enabled"})),
    ] {
        let denied = wss_rpc_envelope(&mut member, 3, method, params).await;
        assert_eq!(denied["error"]["code"], -32003, "{denied}");
    }
    let query = intent_store::EventQuery {
        event_types: vec!["host:execution-context-changed".into()],
        ..Default::default()
    };
    let mut delivered = Vec::new();
    for (method, params, expected) in [
        (
            "settings.update",
            json!({"changes":[{"path":"providers.enabled","value":{"auggie":false,"unknown-private":true,"augment":false}}]}),
            json!(without_auggie),
        ),
        (
            "settings.update",
            json!({"changes":[{"path":"providers.enabled","value":{"auggie":true}}]}),
            json!(all),
        ),
        (
            "settings.update",
            json!({"changes":[{"path":"providers.enabled","value":{"auggie":false}}]}),
            json!(without_auggie),
        ),
        (
            "settings.update",
            json!({"changes":[{"path":"providers.enabled","value":{}}]}),
            json!(all),
        ),
        (
            "settings.update",
            json!({"changes":[{"path":"providers.enabled","value":{"auggie":false}}]}),
            json!(without_auggie),
        ),
        (
            "settings.reset",
            json!({"path":"providers.enabled"}),
            json!(all),
        ),
        (
            "settings.update",
            json!({"changes":[{"path":"providers.enabled","value":{"auggie":false}}]}),
            json!(without_auggie),
        ),
    ] {
        wss_rpc(&mut owner, 4, method, params).await;
        let frame = wss_event(&mut observer, 10).await;
        assert_eq!(frame["jsonrpc"], "2.0");
        let event = &frame["params"]["event"];
        assert_eq!(event["type"], "host:execution-context-changed", "{frame}");
        let mut expected_context = before.clone();
        expected_context["enabledProviderIds"] = expected;
        assert_eq!(
            event["data"], expected_context,
            "complete allowlisted snapshot"
        );
        assert_eq!(
            wss_rpc(&mut member, 5, "host.executionContext", json!({})).await,
            expected_context
        );
        for secret in [
            "unknown-private",
            "private-identity-only-token",
            "/private/uninstalled-provider",
            "private-provider-body",
            probe.to_str().unwrap(),
        ] {
            assert!(!frame.to_string().contains(secret), "{frame}");
        }
        let owner_event = timeout(Duration::from_secs(10), async {
            loop {
                let frame = wss_event(&mut owner_observer, 10).await;
                if frame["params"]["event"]["type"] == "host:execution-context-changed" {
                    break frame;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(owner_event["params"]["event"], *event);
        let rows = store.query_events(&query).await.unwrap();
        assert!(
            rows.iter()
                .any(|row| row.id.as_str() == event["id"].as_str().unwrap()
                    && row.data == expected_context),
            "event committed before delivery"
        );
        delivered.push(event.clone());
    }
    assert!(
        try_wss_event(&mut guest_client, Duration::from_millis(75))
            .await
            .is_none(),
        "guest receives no host policy or settings events"
    );
    assert!(
        try_wss_event(&mut observer, Duration::from_millis(75))
            .await
            .is_none(),
        "member receives no raw settings events"
    );
    assert_eq!(
        std::fs::read(&calls).unwrap_or_default(),
        before_calls,
        "enablement must not spawn a provider or probe auth"
    );
    drop(owner);
    drop(owner_observer);
    drop(member);
    drop(observer);
    drop(guest_client);
    drop(daemon);
    drop(store);
    let daemon = Daemon {
        child: spawn_serve(dir.path(), "both", &env),
    };
    assert!(await_uds(&socket).await);
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
    let mut member = member_connection(port, cfg).await;
    assert_eq!(
        wss_rpc(&mut member, 6, "host.executionContext", json!({})).await,
        delivered.last().unwrap()["data"]
    );
    let reopened = Store::open(&dir.path().join("intentd.db")).await.unwrap();
    let rows = reopened.query_events(&query).await.unwrap();
    for event in delivered {
        assert!(rows.iter().any(
            |row| row.id.as_str() == event["id"].as_str().unwrap() && row.data == event["data"]
        ));
    }
    drop(member);
    drop(daemon);
}

#[tokio::test]
async fn member_ai_rejection_emits_safe_diagnostic_and_context_invalidation_over_wss() {
    let script = gate("member AI rejection").expect("mock provider prerequisite");
    let dir = temp_data_dir();
    let ws = WorkspaceId::new();
    seed_member(dir.path(), &ws).await;
    std::fs::write(
        dir.path().join("config.toml"),
        "[sourceControl.github]\ntokenSource = 'explicit'\n",
    )
    .unwrap();
    let behavior =
        json!({"promptRpcError":{"code":401,"message":"Unauthorized private-provider-response"}})
            .to_string();
    let env = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("MOCK_AGENT_SCRIPT_PATH", script.as_str()),
        ("MOCK_AGENT_BEHAVIOR", behavior.as_str()),
        ("GITHUB_TOKEN", ""),
        ("GH_TOKEN", ""),
        ("GITLAB_TOKEN", ""),
    ];
    let daemon = Daemon {
        child: spawn_serve(dir.path(), "both", &env),
    };
    let socket = dir.path().join("intentd.sock");
    assert!(await_uds(&socket).await);
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
    let mut owner = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut owner,
        1,
        "agent.create",
        json!({"workspaceId":ws,"provider":"mock","model":"default"}),
    )
    .await;
    let id = created["agent"]["id"].as_str().unwrap();
    let mut observer = member_connection(port, cfg.clone()).await;
    wss_rpc(
        &mut observer,
        2,
        "events.subscribe",
        json!({"eventTypes":["agent:failed","host:execution-context-changed","settings:changed"]}),
    )
    .await;
    let mut sender = member_connection(port, cfg).await;
    let before = wss_rpc(&mut sender, 3, "host.executionContext", json!({})).await;
    wss_rpc(
        &mut sender,
        4,
        "agent.sendMessage",
        json!({"agentId":id,"workspaceId":ws,"content":"exercise classified auth rejection"}),
    )
    .await;
    let invalidation = timeout(Duration::from_secs(30), async {
        let (mut failed, mut invalidated) = (false, false);
        let mut invalidation = Value::Null;
        while !failed || !invalidated {
            let frame = wss_event(&mut observer, 30).await;
            let event = &frame["params"]["event"];
            assert!(
                !frame.to_string().contains("private-provider-response"),
                "{frame}"
            );
            match event["type"].as_str().unwrap() {
                "agent:failed" => {
                    assert_eq!(event["workspaceId"], json!(ws));
                    let auth = &event["data"]["executionAuthorization"];
                    assert_eq!(auth["resource"], "ai");
                    assert_eq!(auth["reason"], "rejected");
                    assert_eq!(auth["providerId"], "mock");
                    assert_eq!(auth["host"], Value::Null);
                    assert_eq!(
                        auth["recovery"],
                        json!({"actor":"host-owner","action":"check-ai-authorization"})
                    );
                    assert!(event["data"]["error"]
                        .as_str()
                        .unwrap()
                        .contains("connected host"));
                    failed = true;
                }
                "host:execution-context-changed" => {
                    assert_eq!(
                        event["data"], before,
                        "rejection invalidates without changing configured flags"
                    );
                    invalidation = event.clone();
                    invalidated = true;
                }
                other => panic!("unexpected member event {other}"),
            }
        }
        invalidation
    })
    .await
    .expect("safe rejection and context invalidation");
    // Delivery must follow durable insertion, and killing the actual daemon
    // must not erase the sanitized invalidation needed for later history reads.
    let query = intent_store::EventQuery {
        event_types: vec!["host:execution-context-changed".into()],
        ..Default::default()
    };
    let db = dir.path().join("intentd.db");
    let store = Store::open(&db).await.unwrap();
    let rows = store.query_events(&query).await.unwrap();
    let delivered_id = invalidation["id"].as_str().unwrap();
    assert!(rows.iter().any(|event| event.id.as_str() == delivered_id));
    drop(store);
    drop(owner);
    drop(observer);
    drop(sender);
    drop(daemon);
    let reopened = Store::open(&db).await.unwrap();
    let rows = reopened.query_events(&query).await.unwrap();
    let restored = rows
        .iter()
        .find(|event| event.id.as_str() == delivered_id)
        .expect("the delivered invalidation survives daemon shutdown");
    assert_eq!(restored.data, before);
    assert!(restored.workspace_id.as_str().is_empty());
    assert_eq!(
        restored
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
    assert!(!restored
        .data
        .to_string()
        .contains("private-provider-response"));
}

#[tokio::test]
async fn member_prompt_survives_sender_disconnect_and_safe_context_events_over_wss() {
    let script =
        gate("member prompt and background execution").expect("mock provider prerequisite");
    let dir = temp_data_dir();
    let ws = WorkspaceId::new();
    let member_id = seed_member(dir.path(), &ws).await;
    let gh_dir = dir.path().join("empty-gh");
    std::fs::create_dir_all(&gh_dir).unwrap();
    std::fs::write(
        dir.path().join("config.toml"),
        "[sourceControl.github]\ntokenSource = 'explicit'\nexposeGitCredentialToChildren = false\n",
    )
    .unwrap();
    let behavior = json!({"clientCalls":[{"method":"session/request_permission","params":{
        "sessionId":"mock-session","toolCall":{"toolCallId":"write","title":"Write shared file"},
        "options":[{"optionId":"allow_once","name":"Allow","kind":"allow_once"}]},
        "assertResult":{"outcome":{"outcome":"selected","optionId":"allow_once"}}}],
        "response":"Member-approved background work completed."})
    .to_string();
    let gh = gh_dir.to_string_lossy();
    let env = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("MOCK_AGENT_SCRIPT_PATH", script.as_str()),
        ("MOCK_AGENT_BEHAVIOR", behavior.as_str()),
        ("INTENTD_PERMISSION_POLICY", "interactive"),
        ("GH_CONFIG_DIR", gh.as_ref()),
        ("GITHUB_TOKEN", ""),
        ("GH_TOKEN", ""),
        ("GITLAB_TOKEN", ""),
    ];
    let _daemon = Daemon {
        child: spawn_serve(dir.path(), "both", &env),
    };
    let socket = dir.path().join("intentd.sock");
    assert!(await_uds(&socket).await);
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
    let mut owner = connect_ws(port, cfg.clone()).await;
    let created = wss_rpc(
        &mut owner,
        1,
        "agent.create",
        json!({"workspaceId":ws,"provider":"mock","model":"default"}),
    )
    .await;
    let id = created["agent"]["id"].as_str().unwrap();
    let mut observer = member_connection(port, cfg.clone()).await;
    wss_rpc(&mut observer,1,"events.subscribe",json!({"eventTypes":["host:execution-context-changed","settings:changed","agent:permission:request","agent:failed","agent:idle"]})).await;
    let mut sender = member_connection(port, cfg.clone()).await;
    let context = wss_rpc(&mut sender, 2, "host.executionContext", json!({})).await;
    assert_eq!(
        context["gitCredentialPolicy"]["managedHelperEnabled"],
        false
    );
    assert_eq!(context["repositoryConnections"][0]["configured"], false);
    wss_rpc(&mut sender,3,"agent.sendMessage",json!({"agentId":id,"workspaceId":ws,"content":"run shared work","messageMetadata":{"fromPrincipalId":"forged-owner"}})).await;
    sender.close(None).await.unwrap();
    drop(sender);
    let permission = timeout(Duration::from_secs(30), async {
        loop {
            let event = wss_event(&mut observer, 30).await;
            assert_ne!(event["params"]["event"]["type"], "settings:changed");
            let e = &event["params"]["event"];
            if e["type"] == "agent:permission:request" {
                break e["data"].clone();
            }
        }
    })
    .await
    .expect("member receives prompt after sender disconnects");
    let request = permission["requestId"].as_str().unwrap();
    let mut answer = member_connection(port, cfg.clone()).await;
    for filter in [json!({}), json!({"agentId":id})] {
        let pending = wss_rpc(&mut answer, 4, "agent.pendingPermissions", filter).await;
        assert!(pending["requests"]
            .as_array()
            .unwrap()
            .iter()
            .any(|r| r["requestId"] == request));
    }
    let resolved = wss_rpc(
        &mut answer,
        5,
        "agent.respondPermission",
        json!({"requestId":request,"outcome":{"outcome":"selected","optionId":"allow_once"}}),
    )
    .await;
    assert_eq!(resolved["resolved"], true);
    timeout(Duration::from_secs(30), async {
        loop {
            let event = wss_event(&mut observer, 30).await;
            let e = &event["params"]["event"];
            assert_ne!(e["type"], "agent:failed", "{event}");
            if e["type"] == "agent:idle" {
                break;
            }
        }
    })
    .await
    .expect("shared work completes after initiating socket closes");
    let conversation = wss_rpc(
        &mut answer,
        6,
        "agent.getConversation",
        json!({"agentId":id,"workspaceId":ws}),
    )
    .await;
    assert!(conversation.to_string().contains(member_id.as_str()));
    assert!(!conversation.to_string().contains("forged-owner"));
    wss_rpc(
        &mut owner,
        7,
        "settings.update",
        json!({"changes":[{"path":"sourceControl.github.exposeGitCredentialToChildren","value":true}]}),
    )
    .await;
    let event = wss_event(&mut observer, 10).await;
    assert_eq!(
        event["params"]["event"]["type"], "host:execution-context-changed",
        "{event}"
    );
    let safe = &event["params"]["event"]["data"];
    assert_eq!(safe["gitCredentialPolicy"]["managedHelperEnabled"], true);
    assert_eq!(
        safe.as_object()
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
}

#[tokio::test]
async fn member_scripts_respect_managed_helper_policy_and_keep_alternative_helpers() {
    let dir = temp_data_dir();
    let ws = WorkspaceId::new();
    seed_member(dir.path(), &ws).await;
    let checkout = dir.path().join("workspaces").join(ws.as_str());
    assert!(std::process::Command::new("git")
        .args(["init", "--quiet"])
        .current_dir(&checkout)
        .status()
        .unwrap()
        .success());
    let gh_dir = dir.path().join("empty-gh");
    std::fs::create_dir_all(&gh_dir).unwrap();
    let git_config = dir.path().join("empty-gitconfig");
    std::fs::write(&git_config, "").unwrap();
    // Disposable credentials only; even incidental readiness cannot contact
    // a real forge or fall back to the user's CLI account.
    std::fs::write(dir.path().join("secrets.json"),r#"{"sourceControl.github.token":"fake-host-repository-token","collaboration.github.token":"fake-identity-only-token"}"#).unwrap();
    std::fs::write(
        dir.path().join("config.toml"),
        "[sourceControl.github]\ntokenSource = 'explicit'\napiBaseUrl = 'http://127.0.0.1:9'\n",
    )
    .unwrap();
    let secrets_file = dir.path().join("secrets.json");
    let secrets_file = secrets_file.to_string_lossy();
    let gh = gh_dir.to_string_lossy();
    let git_config = git_config.to_string_lossy();
    let env = [
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_SECRETS_FILE", secrets_file.as_ref()),
        ("GH_CONFIG_DIR", gh.as_ref()),
        ("GITHUB_TOKEN", ""),
        ("GH_TOKEN", ""),
        ("GITLAB_TOKEN", ""),
        ("GIT_CONFIG_GLOBAL", git_config.as_ref()),
        ("GIT_CONFIG_NOSYSTEM", "1"),
        ("GIT_CONFIG_COUNT", "0"),
        ("GIT_CONFIG_PARAMETERS", ""),
        ("GIT_TERMINAL_PROMPT", "0"),
        ("GIT_ASKPASS", "/bin/false"),
        ("SSH_ASKPASS", "/bin/false"),
        ("GIT_AUTHOR_NAME", "Shared Host Author"),
        ("GIT_AUTHOR_EMAIL", "host@example.invalid"),
        ("GIT_COMMITTER_NAME", "Shared Host Committer"),
        ("GIT_COMMITTER_EMAIL", "committer@example.invalid"),
    ];
    let _daemon = Daemon {
        child: spawn_serve(dir.path(), "both", &env),
    };
    let socket = dir.path().join("intentd.sock");
    assert!(await_uds(&socket).await);
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
    let mut owner = connect_ws(port, cfg.clone()).await;
    let mut member = member_connection(port, cfg).await;
    let command = "git var GIT_AUTHOR_IDENT; git var GIT_COMMITTER_IDENT; printf 'protocol=https\\nhost=github.com\\n\\n' | git credential fill";
    let script = wss_rpc(
        &mut member,
        1,
        "script.create",
        json!({"workspaceId":ws,"name":"Credential probe","mode":"command","command":command}),
    )
    .await;
    let script_id = script["id"]
        .as_str()
        .or_else(|| script["script"]["id"].as_str())
        .expect("script id");
    let override_script = wss_rpc(&mut member, 5, "script.create", json!({
        "workspaceId":ws,"name":"Explicit environment probe","mode":"command","command":command,
        "env":{"GIT_CONFIG_PARAMETERS":"","GIT_AUTHOR_NAME":"Script Author","GIT_AUTHOR_EMAIL":"script@example.invalid",
            "GIT_COMMITTER_NAME":"Script Committer","GIT_COMMITTER_EMAIL":"script-committer@example.invalid"}
    })).await;
    let override_id = override_script["id"].as_str().expect("override script id");
    for (enabled, alternative) in [(true, false), (false, false), (false, true), (true, true)] {
        if alternative {
            assert!(std::process::Command::new("git").args(["config","--local","credential.helper","!f() { printf 'username=host-alternative\\npassword=fake-alternative-token\\n'; }; f"]).current_dir(&checkout).status().unwrap().success());
        } else {
            let _ = std::process::Command::new("git")
                .args(["config", "--local", "--unset-all", "credential.helper"])
                .current_dir(&checkout)
                .status()
                .unwrap();
        }
        wss_rpc(&mut owner,2,"settings.update",json!({"changes":[{"path":"sourceControl.github.exposeGitCredentialToChildren","value":enabled}]})).await;
        let result = wss_rpc(
            &mut member,
            3,
            "script.run",
            json!({"workspaceId":ws,"scriptId":script_id,"timeoutSeconds":15}),
        )
        .await;
        let output = result["output"].as_str().unwrap_or("");
        assert!(
            output.contains("Shared Host Author <host@example.invalid>"),
            "{result}"
        );
        assert!(
            output.contains("Shared Host Committer <committer@example.invalid>"),
            "{result}"
        );
        assert!(!output.contains("fake-identity-only-token"));
        if enabled {
            assert!(
                output.contains("password=fake-host-repository-token"),
                "{result}"
            );
        } else if alternative {
            assert!(
                output.contains("password=fake-alternative-token"),
                "{result}"
            );
        } else {
            assert!(!output.contains("password="), "{result}");
            assert_ne!(result["exitCode"], 0);
        }
        let result = wss_rpc(
            &mut member,
            6,
            "script.run",
            json!({"workspaceId":ws,"scriptId":override_id,"timeoutSeconds":15}),
        )
        .await;
        let output = result["output"].as_str().unwrap();
        assert!(
            output.contains("Script Author <script@example.invalid>"),
            "{result}"
        );
        assert!(
            output.contains("Script Committer <script-committer@example.invalid>"),
            "{result}"
        );
        assert!(
            !output.contains("fake-host-repository-token"),
            "explicit script helper env wins even when managed injection is enabled: {result}"
        );
        assert!(!output.contains("fake-identity-only-token"));
        if alternative {
            assert!(
                output.contains("password=fake-alternative-token"),
                "configured alternative survives the explicit override: {result}"
            );
        } else {
            assert!(!output.contains("password="), "{result}");
            assert_ne!(result["exitCode"], 0);
        }
        let context = wss_rpc(&mut member, 4, "host.executionContext", json!({})).await;
        assert_eq!(
            context["gitCredentialPolicy"]["managedHelperEnabled"], enabled,
            "execution never changes host policy"
        );
    }
}

#[tokio::test]
async fn member_provider_safe_reads_use_host_cache_and_preserve_administration_over_wss() {
    use std::os::unix::fs::PermissionsExt as _;
    let dir = temp_data_dir();
    let ws = WorkspaceId::new();
    let member = seed_member(dir.path(), &ws).await;
    let gh = dir.path().join("empty-gh");
    std::fs::create_dir_all(&gh).unwrap();
    let probe = dir.path().join("fake-auggie");
    let calls = dir.path().join("probe-calls");
    let authorized = dir.path().join("authorized");
    std::fs::write(&probe, format!("#!/bin/sh\nif [ \"$1\" = token ]; then\n echo probe >> '{}'\n if [ -f '{}' ]; then echo private-probe-secret; exit 0; fi\n exit 1\nfi\nexit 0\n", calls.display(), authorized.display())).unwrap();
    std::fs::set_permissions(&probe, std::fs::Permissions::from_mode(0o700)).unwrap();
    std::fs::write(
        dir.path().join("config.toml"),
        format!(
            "[sourceControl.github]\ntokenSource = 'explicit'\n[providers.paths]\nauggie = '{}'\n",
            probe.display()
        ),
    )
    .unwrap();
    let gh = gh.to_string_lossy();
    let _daemon = Daemon {
        child: spawn_serve(
            dir.path(),
            "both",
            &[
                ("INTENTD_AUTH_TOKEN", TOKEN),
                ("GH_CONFIG_DIR", gh.as_ref()),
                ("GITHUB_TOKEN", ""),
                ("GH_TOKEN", ""),
                ("GITLAB_TOKEN", ""),
            ],
        ),
    };
    let socket = dir.path().join("intentd.sock");
    assert!(await_uds(&socket).await);
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
    let mut owner = connect_ws(port, cfg.clone()).await;
    let mut client = member_connection(port, cfg.clone()).await;
    let mut observer = member_connection(port, cfg.clone()).await;
    wss_rpc(
        &mut observer,
        1,
        "events.subscribe",
        json!({"eventTypes":["host:execution-context-changed","settings:changed"]}),
    )
    .await;
    let settings_before = std::fs::read(dir.path().join("config.toml")).unwrap();
    let context = wss_rpc(&mut client, 1, "host.executionContext", json!({})).await;
    let discovery = wss_rpc(&mut client, 2, "host.providerDiscovery", json!({})).await;
    assert!(discovery["providers"]
        .as_array()
        .unwrap()
        .iter()
        .any(|p| p["id"] == "auggie" && p["installed"] == true));
    assert!(!discovery.to_string().contains("private-probe-secret"));
    assert_eq!(
        std::fs::read(dir.path().join("config.toml")).unwrap(),
        settings_before,
        "member discovery must not heal host defaults"
    );
    let mut second = member_connection(port, cfg.clone()).await;
    let (first, concurrent) = tokio::join!(
        wss_rpc(
            &mut client,
            3,
            "host.providerAuthStatus",
            json!({"providerId":"auggie"})
        ),
        wss_rpc(
            &mut second,
            1,
            "host.providerAuthStatus",
            json!({"providerId":"auggie"})
        )
    );
    for result in [first, concurrent] {
        assert_eq!(
            result,
            json!({"providers":[{"id":"auggie","authenticated":false}]})
        );
    }
    for id in 4..7 {
        let result = wss_rpc(
            &mut client,
            id,
            "host.providerAuthStatus",
            json!({"providerId":"auggie"}),
        )
        .await;
        assert_eq!(result["providers"][0]["authenticated"], false);
    }
    assert_eq!(
        std::fs::read_to_string(&calls).unwrap().lines().count(),
        1,
        "concurrent and cached requests share a bounded probe"
    );
    let initial = wss_event(&mut observer, 10).await;
    assert_eq!(
        initial["params"]["event"]["type"],
        "host:execution-context-changed"
    );
    assert_eq!(initial["params"]["event"]["data"], context);
    std::fs::write(&authorized, "ready").unwrap();
    let cached = wss_rpc(
        &mut client,
        7,
        "host.providerAuthStatus",
        json!({"providerId":"auggie"}),
    )
    .await;
    assert_eq!(cached["providers"][0]["authenticated"], false);
    let refreshed = wss_rpc(
        &mut client,
        8,
        "host.providerAuthStatus",
        json!({"providerId":"auggie","force":true}),
    )
    .await;
    assert_eq!(
        refreshed,
        json!({"providers":[{"id":"auggie","authenticated":true}]})
    );
    assert_eq!(std::fs::read_to_string(&calls).unwrap().lines().count(), 2);
    let changed = wss_event(&mut observer, 10).await;
    let event = &changed["params"]["event"];
    assert_eq!(event["type"], "host:execution-context-changed");
    assert_eq!(
        event["data"], context,
        "readiness changes do not expose the probe output"
    );
    assert!(!changed.to_string().contains("private-probe-secret"));
    let store = Store::open(&dir.path().join("intentd.db")).await.unwrap();
    let events = store
        .query_events(&intent_store::EventQuery {
            event_types: vec!["host:execution-context-changed".into()],
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(events
        .iter()
        .any(|row| row.id.as_str() == event["id"].as_str().unwrap() && row.data == context));
    for method in [
        "settings.list",
        "host.env",
        "host.providerTestPrompt",
        "prMonitor.flush",
    ] {
        let denied = wss_rpc_envelope(&mut client, 9, method, json!({})).await;
        assert_eq!(denied["error"]["code"], -32003, "{method}: {denied}");
    }
    let owner_status = wss_rpc(
        &mut owner,
        1,
        "host.providerAuthStatus",
        json!({"providerId":"auggie"}),
    )
    .await;
    assert_eq!(owner_status, refreshed);
    let owner_discovery = wss_rpc(&mut owner, 2, "host.providerDiscovery", json!({})).await;
    assert_eq!(owner_discovery["providers"], discovery["providers"]);
    // Keep the credential and workspace row: the same socket's cached role
    // must not preserve host safe-read authority after durable removal.
    store
        .add_workspace_member(&ws, &member, intent_core::WorkspaceRole::Collaborator)
        .await
        .unwrap();
    sqlx::query("DELETE FROM host_member WHERE principal_id = ?")
        .bind(member.as_str())
        .execute(store.write_pool())
        .await
        .unwrap();
    for method in ["host.providerDiscovery", "host.providerAuthStatus"] {
        let denied =
            wss_rpc_envelope(&mut client, 10, method, json!({"providerId":"auggie"})).await;
        assert_eq!(
            denied["error"]["code"], -32003,
            "revoked member {method}: {denied}"
        );
    }
    assert_eq!(std::fs::read_to_string(&calls).unwrap().lines().count(), 2);
}

async fn seed_linked_member_pr(data_dir: &Path, ws: &WorkspaceId) {
    seed_member(data_dir, ws).await;
    let store = Store::open(&data_dir.join("intentd.db")).await.unwrap();
    sqlx::query("UPDATE workspace SET repository_owner = 'fake-org', repository_name = 'fake-repo', pr_number = 7 WHERE id = ?")
        .bind(ws.as_str()).execute(store.write_pool()).await.unwrap();
}

#[tokio::test]
async fn member_pr_status_missing_auth_does_not_consume_collaboration_credentials_over_wss() {
    let dir = temp_data_dir();
    let ws = WorkspaceId::new();
    seed_linked_member_pr(dir.path(), &ws).await;
    let gh = dir.path().join("empty-gh");
    std::fs::create_dir_all(&gh).unwrap();
    std::fs::write(
        dir.path().join("secrets.json"),
        json!({"collaboration.github.token":"private-identity-only-token"}).to_string(),
    )
    .unwrap();
    std::fs::write(
        dir.path().join("config.toml"),
        "[sourceControl.github]\ntokenSource = 'explicit'\nexposeGitCredentialToChildren = false\n",
    )
    .unwrap();
    let gh = gh.to_string_lossy();
    let _daemon = Daemon {
        child: spawn_serve(
            dir.path(),
            "both",
            &[
                ("INTENTD_AUTH_TOKEN", TOKEN),
                ("GH_CONFIG_DIR", gh.as_ref()),
                ("GITHUB_TOKEN", ""),
                ("GH_TOKEN", ""),
                ("GITLAB_TOKEN", ""),
            ],
        ),
    };
    let socket = dir.path().join("intentd.sock");
    assert!(await_uds(&socket).await);
    let status = common::await_wss_status(&socket).await;
    let port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    let cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
    let mut owner = connect_ws(port, cfg.clone()).await;
    let mut client = member_connection(port, cfg.clone()).await;
    let mut observer = member_connection(port, cfg).await;
    wss_rpc(
        &mut observer,
        1,
        "events.subscribe",
        json!({"eventTypes":["host:execution-context-changed"]}),
    )
    .await;
    let context = wss_rpc(&mut client, 1, "host.executionContext", json!({})).await;
    assert_eq!(context["repositoryConnections"][0]["configured"], false);
    let reply = wss_rpc_envelope(&mut client, 2, "pr.status", json!({"workspaceId":ws})).await;
    assert_eq!(reply["error"]["code"], -32603);
    assert_eq!(
        reply["error"]["data"],
        json!({"code":"host-execution-authorization","executionAuthorization":{
        "resource":"git","reason":"missing","providerId":"github","host":"github.com",
        "recovery":{"actor":"host-owner","action":"check-git-authorization","setting":"sourceControl.github.exposeGitCredentialToChildren"}}})
    );
    for forbidden in [
        "private-identity-only-token",
        "gh auth login",
        "sources tried",
    ] {
        assert!(!reply.to_string().contains(forbidden), "{reply}");
    }
    let event = wss_event(&mut observer, 10).await;
    assert_eq!(event["params"]["event"]["data"], context);
    let legacy = wss_rpc_envelope(&mut owner, 1, "pr.status", json!({"workspaceId":ws})).await;
    assert_eq!(legacy["error"]["code"], -32603);
    assert!(legacy["error"]["data"]["executionAuthorization"].is_null());
    assert_eq!(legacy["error"]["message"], "Internal error");
    assert!(legacy["error"]["data"]
        .as_str()
        .unwrap()
        .starts_with("source control not configured: github: no token found"));
    // A local Git operation still works on a host with no repository account.
    let repo = dir.path().join("workspaces").join(ws.as_str());
    assert!(Command::new("git")
        .args(["init", "--quiet"])
        .current_dir(&repo)
        .env("GIT_CONFIG_GLOBAL", "/dev/null")
        .env("GIT_CONFIG_NOSYSTEM", "1")
        .status()
        .unwrap()
        .success());
    let local = wss_rpc_envelope(&mut client, 3, "git.status", json!({"workspaceId":ws})).await;
    assert!(local.get("error").is_none(), "{local}");
    let secrets: Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("secrets.json")).unwrap()).unwrap();
    assert_eq!(
        secrets,
        json!({"collaboration.github.token":"private-identity-only-token"})
    );
}

struct PrAuthServer {
    task: tokio::task::JoinHandle<()>,
    status: Arc<std::sync::atomic::AtomicU16>,
    authorization: Arc<std::sync::Mutex<Vec<String>>>,
    url: String,
}

impl Drop for PrAuthServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl PrAuthServer {
    async fn start() -> Self {
        use std::sync::atomic::{AtomicU16, Ordering};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let status = Arc::new(AtomicU16::new(401));
        let authorization = Arc::new(std::sync::Mutex::new(Vec::new()));
        let (state, headers) = (status.clone(), authorization.clone());
        let task = tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let mut bytes = Vec::new();
                loop {
                    let mut chunk = [0; 2048];
                    let n = stream.read(&mut chunk).await.unwrap();
                    if n == 0 {
                        break;
                    }
                    bytes.extend_from_slice(&chunk[..n]);
                    if bytes.windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }
                let request = String::from_utf8_lossy(&bytes);
                if request.contains("/pulls/7 ") {
                    headers.lock().unwrap().push(
                        request
                            .lines()
                            .find(|line| line.to_ascii_lowercase().starts_with("authorization:"))
                            .unwrap_or("")
                            .to_string(),
                    );
                }
                let code = state.load(Ordering::SeqCst);
                let message = match code {
                    401 => "private-rejected-response",
                    403 => "insufficient_scope private-scope-response",
                    429 => "rate limit exceeded",
                    404 => "missing repository",
                    _ => "temporary network failure",
                };
                let body = json!({"message":message}).to_string();
                let response = format!("HTTP/1.1 {code} Error\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len());
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });
        Self {
            task,
            status,
            authorization,
            url,
        }
    }
}

#[intent_test_macros::daemon_test]
async fn member_pr_status_typed_rejections_are_safe_and_non_auth_errors_stay_legacy_over_wss() {
    use intent_services::{EventBus, InMemorySecretStore, SecretStore, Services, SettingsRegistry};
    use intent_transport::{AsyncTokenStore, TokenStore, WsApiServer, WsOptions};
    use std::sync::atomic::Ordering;
    struct OwnerToken;
    impl TokenStore for OwnerToken {
        fn load_token(&self) -> Option<String> {
            Some(TOKEN.into())
        }
        fn store_token(&self, _token: &str) -> intent_core::Result<()> {
            Ok(())
        }
    }
    let fake = PrAuthServer::start().await;
    let dir = temp_data_dir();
    let ws = WorkspaceId::new();
    seed_linked_member_pr(dir.path(), &ws).await;
    // Inject the production GitHub adapter with a local HTTP origin. The
    // binary's fallback PR registry ignores its settings-file API override;
    // this established composition seam guarantees no live forge traffic.
    let secrets = Arc::new(InMemorySecretStore::default());
    secrets
        .store("sourceControl.github.token", "fake-repository-token")
        .unwrap();
    secrets
        .store("collaboration.github.token", "private-identity-only-token")
        .unwrap();
    let registry = Arc::new(SettingsRegistry::load(dir.path().join("config.toml")).unwrap());
    registry
        .apply(&[
            ("sourceControl.github.tokenSource".into(), json!("explicit")),
            ("sourceControl.github.apiBaseUrl".into(), json!(fake.url)),
            (
                "sourceControl.github.exposeGitCredentialToChildren".into(),
                json!(false),
            ),
        ])
        .unwrap();
    let store = Store::open(&dir.path().join("intentd.db")).await.unwrap();
    let bus = EventBus::new(store.clone());
    let github =
        intent_sourcecontrol::GitHubSourceControl::new("fake-repository-token", Some(&fake.url))
            .unwrap();
    let services = Arc::new(
        Services::new(store)
            .with_workspaces_root(dir.path().join("workspaces"))
            .with_settings_registry(registry)
            .with_secret_store(secrets)
            .with_event_bus(bus.clone())
            .with_source_control(Arc::new(github)),
    );
    let worker = services.spawn_execution_context_loop();
    let tls = intent_transport::ensure_tls_certificate(dir.path()).unwrap();
    let tokens = Arc::new(AsyncTokenStore::new(Arc::new(OwnerToken)));
    let server = WsApiServer::new(
        services,
        bus,
        &tls,
        &tokens,
        WsOptions {
            base_port: 0,
            bind_addresses: vec![std::net::Ipv4Addr::LOCALHOST.into()],
            ..Default::default()
        },
        None,
    )
    .unwrap();
    let port = server.start().await.unwrap();
    let cfg = client_config(&tls.fingerprint256);
    let mut owner = connect_ws(port, cfg.clone()).await;
    let mut client = member_connection(port, cfg.clone()).await;
    let mut observer = member_connection(port, cfg).await;
    wss_rpc(
        &mut observer,
        1,
        "events.subscribe",
        json!({"eventTypes":["host:execution-context-changed"]}),
    )
    .await;
    let context = wss_rpc(&mut client, 1, "host.executionContext", json!({})).await;
    assert_eq!(context["repositoryConnections"][0]["configured"], true);
    for (code, reason) in [
        (401, Some("rejected")),
        (403, Some("insufficient-scope")),
        (429, None),
        (404, None),
        (500, None),
    ] {
        fake.status.store(code, Ordering::SeqCst);
        let before_owner = fake.authorization.lock().unwrap().len();
        let legacy = wss_rpc_envelope(&mut owner, 2, "pr.status", json!({"workspaceId":ws})).await;
        let before_member = fake.authorization.lock().unwrap().len();
        assert!(
            before_member > before_owner,
            "owner PR read must reach the local forge"
        );
        assert_eq!(legacy["error"]["code"], -32603, "{legacy}");
        assert!(legacy["error"]["data"]["executionAuthorization"].is_null());
        if reason.is_some() {
            assert_eq!(legacy["error"]["message"], "Internal error");
            assert_eq!(
                legacy["error"]["data"],
                match code {
                    401 => "source control auth error: private-rejected-response",
                    403 => "source control auth error: insufficient_scope private-scope-response",
                    _ => unreachable!(),
                }
            );
            let owner_event = wss_event(&mut observer, 10).await;
            assert_eq!(owner_event["params"]["event"]["data"], context);
        }
        let reply = wss_rpc_envelope(&mut client, 2, "pr.status", json!({"workspaceId":ws})).await;
        assert!(
            fake.authorization.lock().unwrap().len() > before_member,
            "member PR read must reach the local forge"
        );
        assert_eq!(reply["error"]["code"], -32603);
        if let Some(reason) = reason {
            assert_eq!(
                reply["error"]["data"],
                json!({"code":"host-execution-authorization","executionAuthorization":{
                "resource":"git","reason":reason,"providerId":"github","host":"127.0.0.1",
                "recovery":{"actor":"host-owner","action":"check-git-authorization"}}})
            );
            assert!(!reply.to_string().contains("private-"), "{reply}");
            let notification = wss_event(&mut observer, 10).await;
            let event = &notification["params"]["event"];
            assert_eq!(event["data"], context);
            let store = Store::open(&dir.path().join("intentd.db")).await.unwrap();
            let rows = store
                .query_events(&intent_store::EventQuery {
                    event_types: vec!["host:execution-context-changed".into()],
                    ..Default::default()
                })
                .await
                .unwrap();
            assert!(rows
                .iter()
                .any(|row| row.id.as_str() == event["id"].as_str().unwrap()));
        } else {
            assert_eq!(reply["error"], legacy["error"]);
            assert!(try_wss_event(&mut observer, Duration::from_millis(50))
                .await
                .is_none());
        }
    }
    server.stop().await;
    worker.abort();
    let _ = worker.await;
    let headers = fake.authorization.lock().unwrap();
    // The production adapter may retry transient HTTP responses.
    assert!(headers.len() >= 10);
    assert!(
        headers
            .iter()
            .all(|h| h.contains("fake-repository-token") && !h.contains("identity-only")),
        "repository clients must not consume collaboration credentials"
    );
}
