use super::*;
use intent_core::WorkspaceRole;
use serde_json::json;

/// Narrow role/admission proof; full member method/event/reverse/tunnel
/// capabilities are exercised by the subsequent transport implementation.
#[tokio::test]
async fn host_roles_follow_durable_authority_over_wss_and_reconnect() {
    let srv = start(WsOptions::default()).await;
    let owner = srv.store.get_primary_principal().await.unwrap();
    assert!(owner.identity_key().is_none());
    let token = "cd".repeat(32);
    let mut member = Guest::connect(&srv, &token).await;
    let guest = member.call("principal.me", json!({})).await;
    assert_eq!(guest["result"]["hostRole"], "guest");
    assert_eq!(guest["result"]["hostMembershipRevision"], 0);
    sqlx::query("INSERT INTO host_member (principal_id, added_at) VALUES (?, ?)")
        .bind(&member.principal.id.0)
        .bind(now_iso())
        .execute(srv.store.write_pool())
        .await
        .unwrap();
    // The already-admitted guest caller must use the new durable grant.
    let me = member.call("principal.me", json!({})).await;
    assert_eq!(me["result"]["id"], member.principal.id.0);
    assert_eq!(me["result"]["hostRole"], "member");
    assert_eq!(me["result"]["isAdministrator"], false);
    assert_eq!(me["result"]["hostMembershipRevision"], 1);
    for (id, retained) in [("old-workspace", true), ("future-workspace", false)] {
        let ws = WorkspaceId::from(id);
        srv.store
            .insert_workspace(&fixture_workspace(&ws))
            .await
            .unwrap();
        if retained {
            srv.store
                .add_workspace_member(&ws, &member.principal.id, WorkspaceRole::Collaborator)
                .await
                .unwrap();
        }
        let got = member
            .call("workspace.get", json!({"workspaceId": id}))
            .await;
        let row = &got["result"]["workspace"];
        assert_eq!(row["ownerPrincipalId"], owner.id.0, "{got}");
        assert_eq!(row["myRole"], "collaborator", "{got}");
        assert_eq!(row["canManage"], true, "{got}");
    }
    let listed = member.call("workspace.list", json!({})).await;
    assert_eq!(listed["result"]["workspaces"].as_array().unwrap().len(), 2);
    let refused = member.call("settings.list", json!({})).await;
    assert_eq!(refused["error"]["code"], -32003);
    // Reconnect resolves the member role at credential admission too.
    let url = format!("wss://localhost:{}/ws?token={token}", srv.port);
    member.ws = common::wss_connect_with_retry(srv.port, srv.cfg.clone(), &url).await;
    assert_eq!(
        member.call("principal.me", json!({})).await["result"]["hostRole"],
        "member"
    );
    srv.store
        .remove_host_member(&member.principal.id)
        .await
        .unwrap();
    // Storage removal does not broadcast; this intentionally proves that
    // service gates revalidate even before the later socket-close integration.
    let me = member.call("principal.me", json!({})).await;
    assert_eq!(me["result"]["hostRole"], "guest");
    assert_eq!(me["result"]["hostMembershipRevision"], 2);
    let denied = member
        .call("workspace.get", json!({"workspaceId":"old-workspace"}))
        .await;
    assert_eq!(denied["error"]["data"]["code"], "not-found");
    let denied = https_request(
        srv.port,
        srv.cfg.clone(),
        &upgrade_req("/ws", None, Some(&token)),
    )
    .await;
    assert_eq!(status_code(&denied), 401);
    drop(member);
    srv.ws.stop().await;
}

#[tokio::test]
async fn owner_device_credential_keeps_owner_identity_and_administration() {
    let srv = start(WsOptions::default()).await;
    let owner = srv.store.get_primary_principal().await.unwrap();
    let token = "de".repeat(32);
    srv.store
        .insert_principal_credential(&owner.id, &sha256_hex(token.as_bytes()))
        .await
        .unwrap();
    let url = format!("wss://localhost:{}/ws?token={token}", srv.port);
    let mut device = Guest {
        principal: owner.clone(),
        ws: common::wss_connect_with_retry(srv.port, srv.cfg.clone(), &url).await,
        next_id: 0,
    };
    let me = device.call("principal.me", json!({})).await;
    assert_eq!(me["result"]["id"], owner.id.0);
    assert_eq!(me["result"]["hostRole"], "owner");
    assert_eq!(me["result"]["isAdministrator"], true);
    assert!(device
        .call("settings.list", json!({}))
        .await
        .get("error")
        .is_none());
    drop(device);
    srv.ws.stop().await;
}
