//! Collaborator labels through real pinned WSS, persistent settings and both joins.
use super::*;

async fn seed_person(store: &intent_store::Store, id: i64, token: &str) {
    let person = intent_core::Principal {
        id: intent_core::PrincipalId::new(),
        identity: Some(intent_core::PrincipalIdentity::github(id)),
        github_user_id: Some(id),
        login: Some(
            if id == 9001 {
                "gh-guest"
            } else {
                "workspace-guest"
            }
            .into(),
        ),
        display_name: None,
        avatar_url: None,
        is_primary: false,
        created_at: chrono::Utc::now().to_rfc3339(),
        updated_at: chrono::Utc::now().to_rfc3339(),
    };
    store.upsert_principal(&person).await.unwrap();
    store
        .insert_principal_credential(&person.id, &preview_secret_hash(token))
        .await
        .unwrap();
}

async fn status(socket: &mut Ws) -> Value {
    result(&wss_rpc(socket, 950, "system.status", json!({})).await, 950)
}

#[tokio::test]
async fn collaboration_machine_name_live_both_scopes_owner_only_and_restart() {
    let mock = spawn_mock_forge().await;
    let mut host = boot(&mock, &[]).await;
    let mut owner = connect_ws(host.port, host.cfg.clone(), TOKEN).await;
    let original = status(&mut owner).await;
    assert_eq!(original.get("collaborationName"), Some(&Value::Null));
    let store = intent_store::Store::open(&host.dir.path().join("intentd.db"))
        .await
        .unwrap();
    let member_token = "a8".repeat(32);
    let guest_token = "a9".repeat(32);
    seed_person(&store, 9001, &member_token).await;
    seed_person(&store, 9002, &guest_token).await;
    let host_link = host_invite(&mut owner, "github").await;
    let workspace = create_workspace(&mut owner, 951, "Shared machine name").await;
    let (workspace_id, workspace_secret, _) =
        create_invite(&mut owner, 952, &workspace, json!({})).await;
    let mut join = connect_invite(host.port, host.cfg.clone()).await;
    let links = [
        json!({"scope":"host","inviteId":host_link["invite"]["id"],"secret":host_link["secret"]}),
        json!({"scope":"workspace","inviteId":workspace_id,"secret":workspace_secret}),
    ];
    result(
        &wss_rpc(
            &mut owner,
            953,
            "settings.update",
            json!({"changes":[{"path":"sharing.machineName","value":"  Shared 🦀  "}]}),
        )
        .await,
        953,
    );
    for params in &links {
        for method in ["invite.inspect", "invite.challenge"] {
            let preview = result(
                &admitted_rpc(&mut join, 954, method, params.clone()).await,
                954,
            );
            assert_eq!(preview["collaborationName"], "Shared 🦀");
            assert_eq!(preview["hostname"], original["hostname"]);
            assert_eq!(preview["prettyHostname"], original["prettyHostname"]);
        }
    }
    for (mut params, token) in links.into_iter().zip([&member_token, &guest_token]) {
        params["credential"] = json!(token);
        result(
            &admitted_rpc(&mut join, 955, "invite.accept", params).await,
            955,
        );
    }
    let mut member = connect_ws(host.port, host.cfg.clone(), &member_token).await;
    let mut guest = connect_ws(host.port, host.cfg.clone(), &guest_token).await;
    for socket in [&mut member, &mut guest] {
        assert_eq!(status(socket).await["collaborationName"], "Shared 🦀");
        for (method, params) in [
            (
                "settings.update",
                json!({"changes":[{"path":"sharing.machineName","value":"Unauthorized"}]}),
            ),
            ("settings.reset", json!({"path":"sharing.machineName"})),
            ("settings.get", json!({"path":"sharing.machineName"})),
        ] {
            let refused = wss_rpc(socket, 956, method, params).await;
            assert_eq!(refused["jsonrpc"], "2.0");
            assert_eq!(refused["id"], 956);
            assert_eq!(refused["error"]["code"], -32003, "{refused}");
        }
    }
    for name in ["Live rename", ""] {
        result(
            &wss_rpc(
                &mut owner,
                957,
                "settings.update",
                json!({"changes":[{"path":"sharing.machineName","value":name}]}),
            )
            .await,
            957,
        );
        let expected = if name.is_empty() {
            Value::Null
        } else {
            json!(name)
        };
        for socket in [&mut owner, &mut member, &mut guest] {
            let current = status(socket).await;
            assert_eq!(current.get("collaborationName"), Some(&expected));
            assert_eq!(current["hostname"], original["hostname"]);
            assert_eq!(current["prettyHostname"], original["prettyHostname"]);
        }
    }
    result(
        &wss_rpc(
            &mut owner,
            958,
            "settings.update",
            json!({"changes":[{"path":"sharing.machineName","value":"Persistent name"}]}),
        )
        .await,
        958,
    );
    drop((owner, member, guest, join));
    restart(&mut host, &mock, &[]).await;
    let mut owner = connect_ws(host.port, host.cfg.clone(), TOKEN).await;
    let mut member = connect_ws(host.port, host.cfg.clone(), &member_token).await;
    let mut guest = connect_ws(host.port, host.cfg.clone(), &guest_token).await;
    for socket in [&mut owner, &mut member, &mut guest] {
        assert_eq!(status(socket).await["collaborationName"], "Persistent name");
    }
    result(
        &wss_rpc(
            &mut owner,
            959,
            "settings.reset",
            json!({"path":"sharing.machineName"}),
        )
        .await,
        959,
    );
    for socket in [&mut owner, &mut member, &mut guest] {
        assert_eq!(
            status(socket).await.get("collaborationName"),
            Some(&Value::Null)
        );
    }
}
