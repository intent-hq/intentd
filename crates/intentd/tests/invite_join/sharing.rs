//! Member-issued workspace invitations on a disposable daemon with real TLS,
//! pinned certificates, credential admission and the scoped invite endpoint.
use super::*;

#[tokio::test]
async fn sharing_member_issues_scoped_guest_invites_without_repository_setup() {
    let mock = spawn_mock_forge().await;
    let host = boot(&mock, &[]).await;
    let mut owner = connect_ws(host.port, host.cfg.clone(), TOKEN).await;
    let store = intent_store::Store::open(&host.dir.path().join("intentd.db"))
        .await
        .unwrap();
    let primary = store.get_primary_principal().await.unwrap();
    assert!(primary.identity_key().is_none());
    let link = host_invite(&mut owner, "gitlab").await;
    let mut join = connect_invite(host.port, host.cfg.clone()).await;
    let challenge = result(
        &admitted_rpc(
            &mut join,
            700,
            "invite.challenge",
            json!({
                "inviteId":link["invite"]["id"],"secret":link["secret"],"scope":"host"
            }),
        )
        .await,
        700,
    );
    let mut proof = script_proof(&mock, "gitlab", challenge["nonce"].as_str().unwrap());
    proof["inviteId"] = link["invite"]["id"].clone();
    proof["secret"] = link["secret"].clone();
    proof["scope"] = json!("host");
    proof["nonce"] = challenge["nonce"].clone();
    let admitted = result(
        &admitted_rpc(&mut join, 701, "invite.prove", proof).await,
        701,
    );
    let member_id = intent_core::PrincipalId(admitted["principalId"].as_str().unwrap().into());
    let mut member = connect_ws(
        host.port,
        host.cfg.clone(),
        admitted["token"].as_str().unwrap(),
    )
    .await;

    // A previously authenticated external guest can accept a member's link.
    // Same numeric identity as the GitLab member, different provider.
    let guest = intent_core::Principal {
        id: intent_core::PrincipalId::new(),
        identity: Some(intent_core::PrincipalIdentity::github(
            i64::try_from(GUEST_GL_ID).unwrap(),
        )),
        github_user_id: Some(i64::try_from(GUEST_GL_ID).unwrap()),
        login: Some(GUEST_GL_LOGIN.into()),
        display_name: Some("External person".into()),
        avatar_url: None,
        is_primary: false,
        created_at: chrono::Utc::now().to_rfc3339(),
        updated_at: chrono::Utc::now().to_rfc3339(),
    };
    let guest_token = "a7".repeat(32);
    store.upsert_principal(&guest).await.unwrap();
    store
        .insert_principal_credential(&guest.id, &preview_secret_hash(&guest_token))
        .await
        .unwrap();
    let mut outsider = connect_ws(host.port, host.cfg.clone(), &guest_token).await;
    let hidden = create_workspace(&mut owner, 702, "Other workspace").await;
    for retained in [false, true] {
        let ws = create_workspace(&mut owner, 703, "Shared workspace").await;
        let wid = intent_core::WorkspaceId(ws.clone());
        assert!(store
            .get_workspace_member_role(&wid, &member_id)
            .await
            .unwrap()
            .is_none());
        if retained {
            store
                .add_workspace_member(&wid, &member_id, intent_core::WorkspaceRole::Collaborator)
                .await
                .unwrap();
        }
        let before = store.list_workspace_members(&wid).await.unwrap();
        let workspace = result(
            &wss_rpc(&mut member, 704, "workspace.get", json!({"workspaceId":ws})).await,
            704,
        );
        assert_eq!(workspace["workspace"]["ownerPrincipalId"], primary.id.0);
        assert_eq!(workspace["workspace"]["myRole"], "collaborator");
        assert_eq!(workspace["workspace"]["canManage"], true);

        let (id, secret, invite) = create_invite(&mut member, 705, &ws, json!({})).await;
        assert_eq!(invite["scope"], "workspace");
        assert_eq!(invite["createdByPrincipalId"], member_id.0);
        assert_eq!(invite["reusable"], true);
        let listed = result(
            &wss_rpc(
                &mut member,
                706,
                "workspace.invite.list",
                json!({"workspaceId":ws}),
            )
            .await,
            706,
        );
        assert_eq!(listed["invites"].as_array().unwrap().len(), 1);
        assert_eq!(listed["invites"][0]["id"], id);
        assert!(listed["invites"][0].get("secret").is_none());
        for method in [
            "workspace.invite.create",
            "workspace.invite.list",
            "workspace.invite.revoke",
        ] {
            let refused = wss_rpc(
                &mut outsider,
                707,
                method,
                json!({"workspaceId":ws,"inviteId":id}),
            )
            .await;
            assert_eq!(refused["error"]["code"], -32003, "{method}: {refused}");
        }
        let wrong = wss_rpc(
            &mut member,
            708,
            "workspace.invite.revoke",
            json!({"workspaceId":hidden,"inviteId":id}),
        )
        .await;
        assert_eq!(wrong["error"]["data"]["code"], "not-found", "{wrong}");
        let wrong_scope = admitted_rpc(
            &mut join,
            709,
            "invite.accept",
            json!({"scope":"host","inviteId":id,"secret":secret,"credential":guest_token}),
        )
        .await;
        assert_eq!(
            wrong_scope["error"]["data"]["code"],
            "invite-scope-mismatch"
        );
        let accepted = result(
            &admitted_rpc(
                &mut join,
                710,
                "invite.accept",
                json!({"inviteId":id,"secret":secret,"credential":guest_token}),
            )
            .await,
            710,
        );
        assert_eq!(accepted["workspaceId"], ws);
        assert_eq!(accepted["hostRole"], "guest");
        assert_eq!(accepted["principalId"], guest.id.0);
        let roster = result(
            &wss_rpc(
                &mut member,
                711,
                "workspace.members.list",
                json!({"workspaceId":ws}),
            )
            .await,
            711,
        );
        assert_eq!(roster["members"].as_array().unwrap().len(), 3);
        assert_eq!(roster["guestCount"], 2); // guest plus reusable link
        assert_eq!(store.list_host_members().await.unwrap().members.len(), 2);
        let revoked = result(
            &wss_rpc(
                &mut member,
                712,
                "workspace.invite.revoke",
                json!({"workspaceId":ws,"inviteId":id}),
            )
            .await,
            712,
        );
        assert_eq!(revoked["revoked"], true);
        let removed = result(
            &wss_rpc(
                &mut member,
                713,
                "workspace.members.remove",
                json!({"workspaceId":ws,"principalId":guest.id}),
            )
            .await,
            713,
        );
        assert_eq!(removed["removed"], true);
        let after = result(
            &wss_rpc(&mut member, 716, "workspace.get", json!({"workspaceId":ws})).await,
            716,
        );
        assert_eq!(after["workspace"]["memberCount"], 2);
        assert_eq!(store.list_workspace_members(&wid).await.unwrap(), before);
        let hidden_again = wss_rpc(
            &mut outsider,
            714,
            "workspace.members.list",
            json!({"workspaceId":ws}),
        )
        .await;
        assert_eq!(hidden_again["error"]["data"]["code"], "not-found");
    }
    for client in [&mut owner, &mut member] {
        let directory = result(
            &wss_rpc(client, 715, "principal.list", json!({})).await,
            715,
        );
        assert_eq!(directory["principals"].as_array().unwrap().len(), 2);
        assert!(!directory["principals"]
            .as_array()
            .unwrap()
            .iter()
            .any(|p| p["principalId"] == primary.id.0));
    }
}
