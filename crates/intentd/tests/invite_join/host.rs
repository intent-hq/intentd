//! Host invitations over the actual pinned TLS/WSS listener, using the shared
//! disposable daemon and public forge fixture from the workspace invite suite.
use super::*;

#[path = "sharing.rs"]
mod sharing;

fn result(frame: &Value, id: i64) -> Value {
    assert_eq!(frame["jsonrpc"], "2.0");
    assert_eq!(frame["id"], id);
    assert_eq!(frame.as_object().unwrap().len(), 3);
    assert!(frame.get("error").is_none(), "{frame}");
    frame["result"].clone()
}

async fn host_invite(owner: &mut Ws, provider: &str) -> Value {
    result(&wss_rpc(owner, 200, "host.invite.create", json!({
        "pinProvider":provider,"pinLogin":if provider=="github" {"gh-guest"} else {GUEST_GL_LOGIN}
    })).await, 200)
}

fn script_proof(mock: &MockForge, provider: &str, nonce: &str) -> Value {
    let now = chrono::Utc::now().to_rfc3339();
    if provider == "gitlab" {
        mock.script_snippet("901", GUEST_GL_PAT, &now, nonce);
        json!({"provider":"gitlab","host":HOST,"proofId":"901","login":GUEST_GL_LOGIN})
    } else {
        mock.snippets.lock().unwrap().push(("gh901".into(),json!({
            "owner":{"login":"gh-guest","id":9001},"created_at":now,
            "files":{"intent-join-proof.txt":{"filename":"intent-join-proof.txt","content":nonce,"truncated":false}}
        }),String::new()));
        json!({"provider":"github","host":"github.com","gistId":"gh901","login":"gh-guest"})
    }
}

async fn restart(host: &mut Host, mock: &MockForge, credentials: &[(&str, &str)]) {
    host.daemon.child.kill().unwrap();
    host.daemon.child.wait().unwrap();
    let data_dir = host.dir.path();
    let secrets = data_dir.join("secrets.json").to_string_lossy().to_string();
    let tailcat = data_dir
        .join("fake-tailcat.sh")
        .to_string_lossy()
        .to_string();
    let mut env = vec![
        ("INTENTD_AUTH_TOKEN", TOKEN),
        ("INTENTD_TCP_PORT", "0"),
        ("INTENTD_SECRETS_FILE", secrets.as_str()),
        ("INTENTD_GITHUB_LOGIN_BASE_URI", mock.base_uri.as_str()),
        ("INTENTD_GITHUB_API_BASE_URI", mock.base_uri.as_str()),
        ("INTENTD_GITLAB_API_BASE_URI", mock.base_uri.as_str()),
        ("INTENTD_TAILCAT_BIN", tailcat.as_str()),
    ];
    env.extend_from_slice(credentials);
    host.daemon = Daemon {
        child: spawn_serve(data_dir, &env),
    };
    let socket = data_dir.join("intentd.sock");
    assert!(await_uds(&socket).await, "daemon restart");
    let status = common::await_wss_status(&socket).await;
    host.port = u16::try_from(status["result"]["port"].as_u64().unwrap()).unwrap();
    host.cfg = client_config(status["result"]["fingerprint"].as_str().unwrap());
}

async fn exercise_host_join(provider: &str, credentials: &[(&str, &str)]) {
    let mock = spawn_mock_forge().await;
    let mut host = boot(&mock, credentials).await;
    let mut owner = connect_ws(host.port, host.cfg.clone(), TOKEN).await;
    let store = intent_store::Store::open(&host.dir.path().join("intentd.db"))
        .await
        .unwrap();
    // No principal.me refresh is needed to issue a link on an empty host.
    if credentials.is_empty() {
        assert!(store
            .get_primary_principal()
            .await
            .unwrap()
            .identity_key()
            .is_none());
    }
    let mut events = connect_ws(host.port, host.cfg.clone(), TOKEN).await;
    result(
        &wss_rpc(
            &mut events,
            201,
            "events.subscribe",
            json!({"eventTypes":["host:invites-changed","host:members-changed"]}),
        )
        .await,
        201,
    );
    let created = host_invite(&mut owner, provider).await;
    let invite = &created["invite"];
    let id = invite["id"].as_str().unwrap();
    let secret = created["secret"].as_str().unwrap();
    let identity = if provider == "github" {
        github_identity(9001)
    } else {
        gitlab_identity(GUEST_GL_ID)
    };
    assert_eq!(created["hosts"], json!([]));
    assert_eq!(created["version"], 1);
    assert_eq!(created["port"], host.port);
    assert_eq!(created["url"], invite["url"]);
    let url = created["url"].as_str().unwrap();
    assert!(url.ends_with("&scope=host"));
    assert!(!url.contains("host="));
    assert!(!url.contains(TOKEN));
    assert_eq!(invite["scope"], "host");
    assert_eq!(invite["pinIdentity"], identity);
    assert_eq!(invite["reusable"], false);
    for key in ["secret", "secretHash", "workspaceId"] {
        assert!(invite.get(key).is_none(), "{key}: {invite}");
    }
    let event = next_event(&mut events, "host:invites-changed", 10).await;
    assert_eq!(event["data"], json!({"inviteId":id,"action":"created"}));
    let listed = result(
        &wss_rpc(&mut owner, 202, "host.invite.list", json!({})).await,
        202,
    );
    assert_eq!(listed, json!({"invites":[invite]}));
    let before = result(
        &wss_rpc(&mut owner, 203, "host.members.list", json!({})).await,
        203,
    );
    assert_eq!(before["members"].as_array().unwrap().len(), 1);
    assert_eq!(before["revision"], 0);
    let mut join = connect_invite(host.port, host.cfg.clone()).await;
    let inspect = result(
        &admitted_rpc(
            &mut join,
            204,
            "invite.inspect",
            json!({"inviteId":id,"secret":secret,"scope":"host"}),
        )
        .await,
        204,
    );
    assert_eq!(
        inspect,
        json!({"scope":"host","role":"member","pinIdentity":identity,"hostname":inspect["hostname"],"prettyHostname":inspect["prettyHostname"]})
    );
    assert!(inspect["hostname"].is_string());
    let challenge = result(
        &admitted_rpc(
            &mut join,
            205,
            "invite.challenge",
            json!({"inviteId":id,"secret":secret,"scope":"host"}),
        )
        .await,
        205,
    );
    let mut expected = inspect;
    expected["nonce"] = challenge["nonce"].clone();
    expected["nonceExpiresAt"] = challenge["nonceExpiresAt"].clone();
    assert_eq!(challenge, expected);
    let mut proof = script_proof(&mock, provider, challenge["nonce"].as_str().unwrap());
    proof["inviteId"] = json!(id);
    proof["secret"] = json!(secret);
    proof["scope"] = json!("host");
    proof["nonce"] = challenge["nonce"].clone();
    let joined = result(
        &admitted_rpc(&mut join, 206, "invite.prove", proof).await,
        206,
    );
    assert_eq!(
        joined,
        json!({"scope":"host","status":"authorized","token":joined["token"],"principalId":joined["principalId"],"login":if provider=="github" {"gh-guest"} else {GUEST_GL_LOGIN},"identity":identity,"hostRole":"member"})
    );
    let token = joined["token"].as_str().unwrap();
    let person = joined["principalId"].as_str().unwrap();
    assert_ne!(token, TOKEN);
    let event = next_event(&mut events, "host:members-changed", 10).await;
    assert_eq!(
        event["data"],
        json!({"revision":1,"principalId":person,"hostRole":"member","action":"added"})
    );
    let event = next_event(&mut events, "host:invites-changed", 10).await;
    assert_eq!(event["data"], json!({"inviteId":id,"action":"redeemed"}));
    let mut member = connect_ws(host.port, host.cfg.clone(), token).await;
    let me = result(
        &wss_rpc(&mut member, 207, "principal.me", json!({})).await,
        207,
    );
    assert_eq!(me["hostRole"], "member");
    assert_eq!(me["isAdministrator"], false);
    assert_eq!(me["id"], person);
    let after = result(
        &wss_rpc(&mut owner, 208, "host.members.list", json!({})).await,
        208,
    );
    assert_eq!(after["revision"], 1);
    assert_eq!(after["members"][1]["principalId"], person);
    for method in [
        "host.members.list",
        "host.invite.list",
        "host.invite.create",
        "host.invite.revoke",
    ] {
        let refused = wss_rpc(
            &mut member,
            209,
            method,
            json!({"inviteId":id,"pinLogin":"gh-guest","pinProvider":"github"}),
        )
        .await;
        assert_eq!(refused["error"]["code"], -32003, "{method}: {refused}");
    }
    // Another invitation for the same account consumes its link but keeps
    // the host role, revision and bearer used by other paired devices.
    let again = host_invite(&mut owner, provider).await;
    let accepted=result(&admitted_rpc(&mut join,210,"invite.accept",json!({"inviteId":again["invite"]["id"],"secret":again["secret"],"scope":"host","credential":token})).await,210);
    assert_eq!(accepted, joined);
    assert_eq!(store.host_membership_state().await.unwrap().revision, 1);
    assert_eq!(
        store
            .list_principal_credentials(&intent_core::PrincipalId(person.into()))
            .await
            .unwrap()
            .len(),
        1
    );
    let revoked = host_invite(&mut owner, provider).await;
    assert_eq!(
        result(
            &wss_rpc(
                &mut owner,
                211,
                "host.invite.revoke",
                json!({"inviteId":revoked["invite"]["id"]})
            )
            .await,
            211
        ),
        json!({"revoked":true})
    );
    assert_eq!(
        result(
            &wss_rpc(
                &mut owner,
                212,
                "host.invite.revoke",
                json!({"inviteId":revoked["invite"]["id"]})
            )
            .await,
            212
        ),
        json!({"revoked":false})
    );
    drop(owner);
    drop(member);
    drop(events);
    drop(join);
    restart(&mut host, &mock, credentials).await;
    // Restart reuses durable membership and bearer without administrator authority.
    let mut reconnect = connect_ws(host.port, host.cfg.clone(), token).await;
    assert_eq!(
        result(
            &wss_rpc(&mut reconnect, 213, "principal.me", json!({})).await,
            213
        )["hostRole"],
        "member"
    );
}

#[tokio::test]
async fn host_invites_admit_github_without_repository_setup_over_wss() {
    exercise_host_join("github", &[]).await;
}
#[tokio::test]
async fn host_invites_admit_gitlab_without_repository_setup_over_wss() {
    exercise_host_join("gitlab", &[]).await;
}
#[tokio::test]
async fn github_repository_host_invites_gitlab_member_over_wss() {
    exercise_host_join("gitlab", &[("GITHUB_TOKEN", OWNER_GH_TOKEN)]).await;
}
#[tokio::test]
async fn gitlab_repository_host_invites_github_member_over_wss() {
    exercise_host_join("github", &[("GITLAB_TOKEN", HOST_GL_PAT)]).await;
}

#[tokio::test]
async fn host_invite_scope_secret_and_parameter_refusals_over_wss() {
    let mock = spawn_mock_forge().await;
    let host = boot(&mock, &[]).await;
    let mut owner = connect_ws(host.port, host.cfg.clone(), TOKEN).await;
    for params in [
        json!({}),
        json!({"pinLogin":" " ,"pinProvider":"github"}),
        json!({"pinLogin":"gh-guest"}),
        json!({"pinLogin":"gh-guest","pinProvider":"other"}),
        json!({"pinLogin":"gh-guest","pinProvider":"github","pinHost":"gitlab.com"}),
        json!({"pinLogin":"gh-guest","pinProvider":"gitlab","pinHost":"https://gitlab.com"}),
        json!({"pinLogin":"gh-guest","pinProvider":"github","workspaceId":"bogus"}),
        json!({"pinLogin":"gh-guest","pinProvider":"github","expiresInSecs":10}),
    ] {
        let frame = wss_rpc(&mut owner, 300, "host.invite.create", params).await;
        assert_eq!(frame["error"]["code"], -32602, "{frame}");
        assert_eq!(frame["error"]["data"]["code"], "invalid-params", "{frame}");
    }
    assert_eq!(
        result(
            &wss_rpc(&mut owner, 301, "host.invite.list", json!({})).await,
            301
        ),
        json!({"invites":[]})
    );
    let created = host_invite(&mut owner, "github").await;
    let mut join = connect_invite(host.port, host.cfg.clone()).await;
    for (scope, secret, expected) in [
        (
            Value::Null,
            created["secret"].clone(),
            "invite-scope-mismatch",
        ),
        (
            json!("workspace"),
            created["secret"].clone(),
            "invite-scope-mismatch",
        ),
        (json!("host"), json!("wrong"), "invite-not-found"),
    ] {
        let mut params = json!({"inviteId":created["invite"]["id"],"secret":secret});
        if !scope.is_null() {
            params["scope"] = scope;
        }
        let frame = admitted_rpc(&mut join, 302, "invite.inspect", params).await;
        assert_eq!(frame["error"]["data"], json!({"code":expected}));
        assert!(frame.get("result").is_none());
    }
    let frame = wss_rpc(
        &mut owner,
        303,
        "host.invite.revoke",
        json!({"inviteId":"not-present"}),
    )
    .await;
    assert_eq!(frame["error"]["code"], -32602);
    assert_eq!(frame["error"]["data"]["code"], "not-found");
}

#[tokio::test]
async fn host_proof_refuses_wrong_provider_instance_and_account_over_wss() {
    let mock = spawn_mock_forge().await;
    let host = boot(&mock, &[]).await;
    let mut owner = connect_ws(host.port, host.cfg.clone(), TOKEN).await;
    let created = host_invite(&mut owner, "gitlab").await;
    let mut join = connect_invite(host.port, host.cfg.clone()).await;
    for (provider, instance, login) in [
        ("github", "github.com", "gh-guest"),
        ("gitlab", "other.example", GUEST_GL_LOGIN),
        ("gitlab", HOST, INTRUDER_GL_LOGIN),
    ] {
        let challenge = result(
            &admitted_rpc(
                &mut join,
                350,
                "invite.challenge",
                json!({
                    "inviteId":created["invite"]["id"],"secret":created["secret"],"scope":"host"
                }),
            )
            .await,
            350,
        );
        mock.script_snippet(
            "902",
            INTRUDER_GL_PAT,
            &chrono::Utc::now().to_rfc3339(),
            challenge["nonce"].as_str().unwrap(),
        );
        let frame = admitted_rpc(&mut join, 351, "invite.prove", json!({
            "inviteId":created["invite"]["id"],"secret":created["secret"],"scope":"host",
            "nonce":challenge["nonce"],"provider":provider,"host":instance,"proofId":"902","login":login
        })).await;
        assert_eq!(
            frame["error"]["data"],
            json!({"code":"invite-pin-mismatch"})
        );
        assert!(frame.get("result").is_none());
    }
    let store = intent_store::Store::open(&host.dir.path().join("intentd.db"))
        .await
        .unwrap();
    assert_eq!(store.list_principals().await.unwrap().len(), 1);
    assert_eq!(store.host_membership_state().await.unwrap().member_count, 0);
    assert!(store
        .get_host_invite(created["invite"]["id"].as_str().unwrap())
        .await
        .unwrap()
        .unwrap()
        .redeemed_at
        .is_none());
}

#[tokio::test]
async fn host_pin_lookup_refusals_preserve_an_empty_invite_list_over_wss() {
    let mock = spawn_mock_forge().await;
    let host = boot(&mock, &[]).await;
    let mut owner = connect_ws(host.port, host.cfg.clone(), TOKEN).await;
    for status in [401, 429, 503] {
        mock.user_lookup_status.store(status, Ordering::SeqCst);
        let frame = wss_rpc(
            &mut owner,
            360,
            "host.invite.create",
            json!({
                "pinProvider":"gitlab","pinLogin":GUEST_GL_LOGIN
            }),
        )
        .await;
        assert_eq!(frame["error"]["code"], -32603, "{frame}");
        if status == 401 {
            assert_eq!(
                frame["error"]["data"],
                json!({"code":"identity-unverifiable","host":HOST})
            );
        }
        assert!(frame.get("result").is_none());
    }
    mock.user_lookup_status.store(0, Ordering::SeqCst);
    let frame = wss_rpc(
        &mut owner,
        361,
        "host.invite.create",
        json!({
            "pinProvider":"gitlab","pinLogin":"unknown-person"
        }),
    )
    .await;
    assert_eq!(frame["error"]["data"]["code"], "invite-pin-unknown");
    assert_eq!(
        result(
            &wss_rpc(&mut owner, 362, "host.invite.list", json!({})).await,
            362
        ),
        json!({"invites":[]})
    );

    mock.user_lookup_status.store(401, Ordering::SeqCst);
    let connected = boot(&mock, &[("GITLAB_TOKEN", HOST_GL_PAT)]).await;
    let mut connected_owner = connect_ws(connected.port, connected.cfg.clone(), TOKEN).await;
    let invite = host_invite(&mut connected_owner, "gitlab").await;
    assert_eq!(
        invite["invite"]["pinIdentity"],
        gitlab_identity(GUEST_GL_ID)
    );
}

#[tokio::test]
async fn guest_upgrades_to_host_member_with_same_principal_and_bearer_over_wss() {
    let mock = spawn_mock_forge().await;
    let host = boot(&mock, &[]).await;
    let mut owner = connect_ws(host.port, host.cfg.clone(), TOKEN).await;
    let workspace = create_workspace(&mut owner, 400, "Shared before membership").await;
    let (id, secret, _) = create_invite(&mut owner, 401, &workspace, json!({})).await;
    let mut join = connect_invite(host.port, host.cfg.clone()).await;
    let nonce = challenge_nonce(&mut join, 402, &id, &secret).await;
    script_proof(&mock, "gitlab", &nonce);
    let guest = result(
        &prove_gitlab(&mut join, 403, &id, &secret, &nonce, "901", GUEST_GL_LOGIN).await,
        403,
    );
    assert_eq!(guest["scope"], "workspace");
    assert_eq!(guest["hostRole"], "guest");
    assert_eq!(guest["workspaceId"], workspace);
    assert_eq!(guest["identity"], gitlab_identity(GUEST_GL_ID));
    let token = guest["token"].as_str().unwrap();
    let person = intent_core::PrincipalId(guest["principalId"].as_str().unwrap().into());
    let mut device = connect_ws(host.port, host.cfg.clone(), token).await;
    for method in [
        "host.members.list",
        "host.invite.list",
        "host.invite.create",
        "host.invite.revoke",
    ] {
        let frame = wss_rpc(
            &mut device,
            404,
            method,
            json!({"inviteId":id,"pinProvider":"gitlab","pinLogin":GUEST_GL_LOGIN}),
        )
        .await;
        assert_eq!(frame["error"]["code"], -32003, "{method}: {frame}");
    }
    let invitation = host_invite(&mut owner, "gitlab").await;
    // Legacy clients omit scope. None of the four entry points may silently
    // treat a host invitation as a workspace share or grant host membership.
    for method in [
        "invite.inspect",
        "invite.challenge",
        "invite.prove",
        "invite.accept",
    ] {
        let frame = admitted_rpc(&mut join, 410, method, json!({
            "inviteId":invitation["invite"]["id"],"secret":invitation["secret"],
            "credential":token,"nonce":"unused","provider":"gitlab","proofId":"901","login":GUEST_GL_LOGIN
        })).await;
        assert_eq!(
            frame["error"]["data"],
            json!({"code":"invite-scope-mismatch"})
        );
        assert!(frame.get("result").is_none());
    }
    let member = result(&admitted_rpc(&mut join, 405, "invite.accept", json!({
        "inviteId":invitation["invite"]["id"],"secret":invitation["secret"],"scope":"host","credential":token
    })).await, 405);
    assert_eq!(
        member,
        json!({"status":"authorized","scope":"host","hostRole":"member",
        "token":token,"principalId":person,"identity":gitlab_identity(GUEST_GL_ID),"login":GUEST_GL_LOGIN})
    );
    let store = intent_store::Store::open(&host.dir.path().join("intentd.db"))
        .await
        .unwrap();
    assert_eq!(
        store
            .get_workspace_member_role(&intent_core::WorkspaceId(workspace), &person)
            .await
            .unwrap(),
        Some(intent_core::WorkspaceRole::Collaborator)
    );
    let second = create_workspace(&mut owner, 406, "Inherited membership").await;
    let (id, secret, _) = create_invite(&mut owner, 407, &second, json!({})).await;
    let accepted = result(
        &admitted_rpc(
            &mut join,
            408,
            "invite.accept",
            json!({
                "inviteId":id,"secret":secret,"scope":"workspace","credential":token
            }),
        )
        .await,
        408,
    );
    let mut expected = member;
    expected["scope"] = json!("workspace");
    expected["workspaceId"] = json!(second);
    assert_eq!(accepted, expected);
    assert_eq!(
        store
            .get_workspace_member_role(&intent_core::WorkspaceId(second), &person)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        store
            .list_principal_credentials(&person)
            .await
            .unwrap()
            .len(),
        1
    );
    assert_eq!(store.list_host_members().await.unwrap().members.len(), 2);
    let mut reconnected = connect_ws(host.port, host.cfg.clone(), token).await;
    let me = result(
        &wss_rpc(&mut reconnected, 409, "principal.me", json!({})).await,
        409,
    );
    assert_eq!(me["hostRole"], "member");
    assert_eq!(me["isAdministrator"], false);
    assert_eq!(me["id"], json!(person));
}
