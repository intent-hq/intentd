//! Actual host-invite emission and remaining-guest delivery, with unchanged
//! online rosters. All identities, credentials, providers and state are local
//! disposable fixtures; no frontend action is injected.
use super::*;
use intent_core::{Principal, PrincipalId, PrincipalIdentity, WorkspaceId, WorkspaceRole};

async fn seed_guest(store: &intent_store::Store, login: &str, id: i64, token: &str) -> Principal {
    let person = Principal {
        id: PrincipalId::new(),
        identity: Some(PrincipalIdentity::github(id)),
        github_user_id: Some(id),
        login: Some(login.into()),
        display_name: Some(format!("{login} name")),
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
    person
}

/// A workspace update sent after the action is a matched FIFO marker on this
/// same event subscription. Reaching it bounds both positive and privacy
/// assertions without a sleep or an empty-receive timing assumption.
async fn events_through_marker(
    owner: &mut Ws,
    subscriber: &mut Ws,
    workspace: &str,
    title: &str,
) -> Vec<Value> {
    result(
        &wss_rpc(
            owner,
            880,
            "workspace.update",
            json!({"workspaceId":workspace,"title":title}),
        )
        .await,
        880,
    );
    timeout(Duration::from_secs(10), async {
        let mut events = Vec::new();
        loop {
            match subscriber.next().await {
                Some(Ok(Message::Text(text))) => {
                    let frame: Value = serde_json::from_str(&text).unwrap();
                    if frame["method"] != "events.event" {
                        continue;
                    }
                    let event = frame["params"]["event"].clone();
                    if event["type"] == "workspace:updated"
                        && event["data"]["changes"]["title"] == title
                    {
                        return events;
                    }
                    events.push(event);
                }
                Some(Ok(Message::Ping(payload))) => {
                    subscriber.send(Message::Pong(payload)).await.unwrap();
                }
                other => panic!("event delivery before marker: {other:?}"),
            }
        }
    })
    .await
    .expect("workspace event barrier")
}

fn assert_scoped(events: &[Value], workspace: &str) {
    for event in events {
        assert_eq!(event["workspaceId"], workspace, "{event}");
        assert!(
            !matches!(
                event["type"].as_str(),
                Some("host:members-changed" | "host:invites-changed")
            ),
            "remaining guests cannot receive global host information: {event}"
        );
    }
}

async fn offline_addition_notifies_guest(cached: bool) {
    let mock = spawn_mock_forge().await;
    let host = boot(&mock, &[]).await;
    let mut owner = connect_ws(host.port, host.cfg.clone(), TOKEN).await;
    let workspace = create_workspace(&mut owner, 881, "Shared").await;
    let private = create_workspace(&mut owner, 882, "Private").await;
    let wid = WorkspaceId(workspace.clone());
    let store = intent_store::Store::open(&host.dir.path().join("intentd.db"))
        .await
        .unwrap();
    let token = "81".repeat(32);
    let remaining_token = "82".repeat(32);
    let person = seed_guest(&store, "gh-guest", 9001, &token).await;
    let remaining = seed_guest(&store, "remaining-guest", 9002, &remaining_token).await;
    for principal in [&person.id, &remaining.id] {
        store
            .add_workspace_member(&wid, principal, WorkspaceRole::Collaborator)
            .await
            .unwrap();
    }
    let retained = store.list_workspace_members(&wid).await.unwrap();
    let mut guest_rpc = connect_ws(host.port, host.cfg.clone(), &remaining_token).await;
    result(
        &wss_rpc(
            &mut guest_rpc,
            883,
            "client.hello",
            json!({"clientId":"remaining-guest"}),
        )
        .await,
        883,
    );
    let mut subscriber = connect_ws(host.port, host.cfg.clone(), &remaining_token).await;
    result(
        &wss_rpc(&mut subscriber, 884, "events.subscribe", json!({
            "eventTypes":["presence:changed","workspace:updated","host:members-changed","host:invites-changed"]
        })).await,
        884,
    );
    if cached {
        let mut promoted = connect_ws(host.port, host.cfg.clone(), &token).await;
        result(
            &wss_rpc(
                &mut promoted,
                885,
                "client.hello",
                json!({"clientId":"promoted-guest"}),
            )
            .await,
            885,
        );
        promoted.close(None).await.unwrap();
        // Observe the actual last-connection close, not an assumed delay.
        timeout(Duration::from_secs(10), async {
            loop {
                let event = next_event(&mut subscriber, "presence:changed", 10).await;
                if event["data"]["members"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|row| row["principalId"] != person.id.0)
                {
                    break;
                }
            }
        })
        .await
        .expect("promoted guest is now offline");
    }
    events_through_marker(&mut owner, &mut subscriber, &workspace, "Ready").await;
    let before = result(
        &wss_rpc(
            &mut guest_rpc,
            886,
            "presence.snapshot",
            json!({"workspaceId":workspace}),
        )
        .await,
        886,
    );
    assert!(before["members"]
        .as_array()
        .unwrap()
        .iter()
        .all(|row| row["principalId"] != person.id.0));
    let count = result(
        &wss_rpc(
            &mut guest_rpc,
            887,
            "workspace.get",
            json!({"workspaceId":workspace}),
        )
        .await,
        887,
    )["workspace"]["memberCount"]
        .clone();
    let before_members = result(
        &wss_rpc(
            &mut guest_rpc,
            895,
            "workspace.members.list",
            json!({"workspaceId":workspace}),
        )
        .await,
        895,
    );
    assert_eq!(count, 3);
    assert_eq!(before_members["guestCount"], 2);
    assert_eq!(
        before_members["members"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["principalId"] == person.id.0)
            .unwrap()["hostRole"],
        "guest"
    );
    let link = host_invite(&mut owner, "github").await;
    let mut join = connect_invite(host.port, host.cfg.clone()).await;
    let joined = result(
        &admitted_rpc(&mut join, 888, "invite.accept", json!({
            "inviteId":link["invite"]["id"],"secret":link["secret"],"scope":"host","credential":token
        })).await,
        888,
    );
    assert_eq!(joined["principalId"], person.id.0);
    assert_eq!(joined["hostRole"], "member");
    result(
        &wss_rpc(
            &mut owner,
            889,
            "workspace.update",
            json!({"workspaceId":private,"title":"Private updated"}),
        )
        .await,
        889,
    );
    let events = events_through_marker(&mut owner, &mut subscriber, &workspace, "Added").await;
    assert_scoped(&events, &workspace);
    let presence: Vec<_> = events
        .iter()
        .filter(|event| event["type"] == "presence:changed")
        .collect();
    assert!(
        !presence.is_empty(),
        "offline host addition must reach the remaining guest: {events:?}"
    );
    assert!(presence.iter().all(|event| event["data"] == before));
    let members = result(
        &wss_rpc(
            &mut guest_rpc,
            890,
            "workspace.members.list",
            json!({"workspaceId":workspace}),
        )
        .await,
        890,
    );
    let promoted: Vec<_> = members["members"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|row| row["principalId"] == person.id.0)
        .collect();
    assert_eq!(promoted.len(), 1);
    assert_eq!(promoted[0]["hostRole"], "member");
    assert_eq!(members["guestCount"], 1);
    assert_eq!(store.list_workspace_members(&wid).await.unwrap(), retained);
    assert!(store
        .get_workspace_member_role(&WorkspaceId(private.clone()), &person.id)
        .await
        .unwrap()
        .is_none());
    assert_eq!(
        result(
            &wss_rpc(
                &mut guest_rpc,
                891,
                "workspace.get",
                json!({"workspaceId":workspace})
            )
            .await,
            891
        )["workspace"]["memberCount"],
        count
    );
    let refused = wss_rpc(
        &mut guest_rpc,
        892,
        "workspace.members.list",
        json!({"workspaceId":private}),
    )
    .await;
    assert_eq!(refused["error"]["data"]["code"], "not-found");

    let again = host_invite(&mut owner, "github").await;
    result(
        &admitted_rpc(&mut join, 893, "invite.accept", json!({
            "inviteId":again["invite"]["id"],"secret":again["secret"],"scope":"host","credential":token
        })).await,
        893,
    );
    let noop = events_through_marker(&mut owner, &mut subscriber, &workspace, "Unchanged").await;
    assert_scoped(&noop, &workspace);
    assert!(noop.iter().all(|event| event["type"] != "presence:changed"));

    result(
        &wss_rpc(
            &mut owner,
            894,
            "host.members.remove",
            json!({"principalId":person.id}),
        )
        .await,
        894,
    );
    let removed = events_through_marker(&mut owner, &mut subscriber, &workspace, "Removed").await;
    assert_scoped(&removed, &workspace);
    assert!(removed
        .iter()
        .any(|event| event["type"] == "presence:changed"));
    assert!(store
        .get_workspace_member_role(&wid, &person.id)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn host_membership_presence_reaches_guest_after_uncached_offline_promotion() {
    offline_addition_notifies_guest(false).await;
}

#[tokio::test]
async fn host_membership_presence_reaches_guest_after_cached_offline_promotion() {
    offline_addition_notifies_guest(true).await;
}
