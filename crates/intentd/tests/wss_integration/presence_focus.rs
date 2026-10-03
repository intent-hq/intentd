//! Regression: B is already focused before a viewer with only A open attaches.
use super::*;
use intent_core::WorkspaceRole;
use serde_json::json;

#[intent_test_macros::daemon_test]
async fn wss_presence_focus_before_attach_filters_and_invalidates() {
    let srv = start(WsOptions::default()).await;
    let a = WorkspaceId::new();
    let b = WorkspaceId::new();
    let hidden = WorkspaceId::new();
    for id in [&a, &b, &hidden] {
        srv.store
            .insert_workspace(&fixture_workspace(id))
            .await
            .unwrap();
    }
    let person_token = "a4".repeat(32);
    let viewer_token = "b4".repeat(32);
    let limited_token = "c4".repeat(32);
    let person = seed_principal(&srv.store, "same-handle", &person_token).await;
    let viewer = seed_principal(&srv.store, "viewer", &viewer_token).await;
    let limited = seed_principal(&srv.store, "limited", &limited_token).await;
    for (who, scopes) in [
        (&person.id, vec![&a, &b, &hidden]),
        (&viewer.id, vec![&a, &b]),
        (&limited.id, vec![&a]),
    ] {
        for ws in scopes {
            srv.store
                .add_workspace_member(ws, who, WorkspaceRole::Collaborator)
                .await
                .unwrap();
        }
    }
    let mut person_c = PresenceClient::open(srv.port, srv.cfg.clone(), &person_token).await;
    person_c
        .call(
            1,
            "client.hello",
            json!({"clientId":"person", "name":"Person"}),
        )
        .await;
    let reply = person_c
        .call(2, "presence.update", json!({"focus":[{"workspaceId":b}]}))
        .await;
    assert_eq!(reply["result"]["ok"], true, "{reply}");
    let mut viewer_c = PresenceClient::open(srv.port, srv.cfg.clone(), &viewer_token).await;
    let params = json!({"workspaceId":a,"principalId":person.id,"replaceGroup":"focus"});
    let reply = viewer_c
        .call(1, "presence.focus.subscribe", params.clone())
        .await;
    assert_eq!(reply["jsonrpc"], "2.0");
    assert_eq!(reply["id"], 1);
    let sub = reply["result"]["subscriptionId"]
        .as_str()
        .expect("focus supported")
        .to_string();
    let push = viewer_c.push(&sub).await;
    assert_eq!(push["seq"], 0);
    assert_eq!(
        push["snapshot"],
        json!({"workspaceId":a,"principalId":person.id,"target":{"workspaceId":b}})
    );
    let roster = viewer_c
        .call(99, "presence.snapshot", json!({"workspaceId":a}))
        .await;
    let row = roster["result"]["members"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["principalId"] == json!(person.id))
        .unwrap();
    assert_eq!(row["focus"], json!([]), "legacy roster stays source-scoped");
    let mut limited_c = PresenceClient::open(srv.port, srv.cfg.clone(), &limited_token).await;
    let reply = limited_c
        .call(1, "presence.focus.subscribe", params.clone())
        .await;
    let limited_sub = reply["result"]["subscriptionId"]
        .as_str()
        .unwrap()
        .to_string();
    let push = limited_c.push(&limited_sub).await;
    assert!(push["snapshot"]["target"].is_null(), "{push}");
    assert!(!push.to_string().contains(b.as_str()));
    person_c
        .call(
            3,
            "presence.update",
            json!({"focus":[{"workspaceId":hidden}]}),
        )
        .await;
    let push = viewer_c.push(&sub).await;
    assert!(push["seq"].as_u64().unwrap() > 0);
    assert!(push["snapshot"]["target"].is_null(), "{push}");
    assert!(!push.to_string().contains(hidden.as_str()));
    // Restore B before reconnect: a replacement must discover it without any
    // new focus event after attach. The prior subscription is disposed.
    person_c
        .call(4, "presence.update", json!({"focus":[{"workspaceId":b}]}))
        .await;
    assert_eq!(
        viewer_c.push(&sub).await["snapshot"]["target"],
        json!({"workspaceId":b})
    );
    // Multiple clients are deterministic, prefer source, and retain other live
    // focus when one client closes. Last disconnect clears immediately.
    let mut second = PresenceClient::open(srv.port, srv.cfg.clone(), &person_token).await;
    second
        .call(
            1,
            "client.hello",
            json!({"clientId":"second", "name":"Second"}),
        )
        .await;
    second
        .call(2, "presence.update", json!({"focus":[{"workspaceId":a}]}))
        .await;
    assert_eq!(
        viewer_c.push(&sub).await["snapshot"]["target"],
        json!({"workspaceId":a})
    );
    second.close().await;
    assert_eq!(
        viewer_c.push(&sub).await["snapshot"]["target"],
        json!({"workspaceId":b})
    );
    person_c.close().await;
    assert!(viewer_c.push(&sub).await["snapshot"]["target"].is_null());
    let mut person_c = PresenceClient::open(srv.port, srv.cfg.clone(), &person_token).await;
    person_c
        .call(
            1,
            "client.hello",
            json!({"clientId":"person-again", "name":"Person"}),
        )
        .await;
    person_c
        .call(2, "presence.update", json!({"focus":[{"workspaceId":b}]}))
        .await;
    assert_eq!(
        viewer_c.push(&sub).await["snapshot"]["target"],
        json!({"workspaceId":b})
    );
    let replacement = viewer_c
        .call(10, "presence.focus.subscribe", params.clone())
        .await;
    let replacement_sub = replacement["result"]["subscriptionId"]
        .as_str()
        .unwrap()
        .to_string();
    assert_ne!(replacement_sub, sub);
    assert_eq!(
        viewer_c.push(&replacement_sub).await["snapshot"]["target"],
        json!({"workspaceId":b})
    );
    let old = viewer_c
        .call(
            11,
            "presence.focus.unsubscribe",
            json!({"subscriptionId":sub}),
        )
        .await;
    assert_eq!(old["result"]["success"], false);
    viewer_c.close().await;
    let mut viewer_c = PresenceClient::open(srv.port, srv.cfg.clone(), &viewer_token).await;
    let connected = viewer_c
        .call(12, "presence.focus.subscribe", params.clone())
        .await;
    let sub = connected["result"]["subscriptionId"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(
        viewer_c.push(&sub).await["snapshot"]["target"],
        json!({"workspaceId":b})
    );
    // Real membership removal triggers clearing without a focus update or poll.
    let mut owner = PresenceClient::open(srv.port, srv.cfg.clone(), TOKEN).await;
    let removed = owner
        .call(
            1,
            "workspace.members.remove",
            json!({"workspaceId":b,"principalId":viewer.id}),
        )
        .await;
    assert!(removed.get("error").is_none(), "{removed}");
    let cleared = viewer_c.push(&sub).await;
    assert!(cleared["snapshot"]["target"].is_null(), "{cleared}");
    assert!(!cleared.to_string().contains(b.as_str()));
    // Losing the caller's source access terminates even an otherwise valid,
    // still-connected self projection. No focus update is needed.
    let own = limited_c
        .call(
            20,
            "presence.focus.subscribe",
            json!({"workspaceId":a,"principalId":limited.id}),
        )
        .await;
    let own_sub = own["result"]["subscriptionId"]
        .as_str()
        .unwrap()
        .to_string();
    assert!(limited_c.push(&own_sub).await["snapshot"]["target"].is_null());
    let removed = owner
        .call(
            20,
            "workspace.members.remove",
            json!({"workspaceId":a,"principalId":limited.id}),
        )
        .await;
    assert!(removed.get("error").is_none(), "{removed}");
    let ended = limited_c.push(&own_sub).await;
    assert_eq!(ended["snapshot"]["closed"], true);
    assert!(ended["snapshot"]["target"].is_null());
    // Source-person removal clears and stops even while the person is online.
    let removed = owner
        .call(
            2,
            "workspace.members.remove",
            json!({"workspaceId":a,"principalId":person.id}),
        )
        .await;
    assert!(removed.get("error").is_none(), "{removed}");
    let ended = viewer_c.push(&sub).await;
    assert!(ended["snapshot"]["target"].is_null());
    assert_eq!(ended["snapshot"]["closed"], true);
    let missing = viewer_c
        .call(
            2,
            "presence.focus.subscribe",
            json!({"workspaceId":a,"principalId":"unknown"}),
        )
        .await;
    let forbidden = viewer_c
        .call(
            3,
            "presence.focus.subscribe",
            json!({"workspaceId":hidden,"principalId":person.id}),
        )
        .await;
    assert_eq!(missing["error"], forbidden["error"]);
    assert_eq!(missing["error"]["code"], -32602);
    assert_eq!(missing["error"]["data"]["code"], "not-found");
    let reply = viewer_c
        .call(
            4,
            "presence.focus.unsubscribe",
            json!({"subscriptionId":sub}),
        )
        .await;
    assert_eq!(reply["result"], json!({"success":true}));
    let reply = limited_c
        .call(
            2,
            "presence.focus.unsubscribe",
            json!({"subscriptionId":sub}),
        )
        .await;
    assert_eq!(reply["result"], json!({"success":false}));
}
