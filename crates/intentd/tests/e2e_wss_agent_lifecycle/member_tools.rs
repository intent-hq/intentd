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
    assert_eq!(restored.data.as_object().unwrap().len(), 4);
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
    assert_eq!(safe.as_object().unwrap().len(), 4);
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
