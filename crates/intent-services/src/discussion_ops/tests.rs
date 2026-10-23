use super::*;
use crate::tests::{workspace, TempDb};
use intent_core::{with_caller, AgentId, EventQueryParams, HostRole, WorkspaceApi, WorkspaceRole};
use intent_store::Store;

fn human(id: &PrincipalId) -> Caller {
    Caller::Wire {
        principal_id: id.clone(),
        host_role: HostRole::Guest,
    }
}

#[tokio::test]
async fn discussion_history_is_filtered_from_agent_event_queries_before_paging() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.unwrap();
    // Seed the legacy event log directly: changing the publish path must not
    // expose messages already written by an older daemon.
    for (i, kind) in [
        "note:updated",
        "discussion:message",
        "agent:idle",
        "discussion:reaction",
        "discussion:future-type",
        "file:changed",
        "discussion:message",
    ]
    .iter()
    .enumerate()
    {
        store
            .insert_event(&NewEvent {
                workspace_id: ws.clone(),
                timestamp: format!("2026-01-01T00:00:0{i}Z"),
                event_type: (*kind).into(),
                actor: EventActor::default(),
                session_id: None,
                correlation_id: None,
                parent_event_id: None,
                metadata: None,
                data: json!({"body":"legacy"}),
            })
            .await
            .unwrap();
    }
    let svc = Services::new(store);
    let caller = Caller::Agent {
        agent_id: AgentId::new(),
    };
    for kind in [
        "discussion:message",
        "discussion:*",
        "discussion:reaction",
        "discussion:future-type",
    ] {
        for paginate in [false, true] {
            let result = with_caller(
                caller.clone(),
                svc.event_query(
                    ws.clone(),
                    EventQueryParams {
                        event_type: Some(kind.into()),
                        paginate: Some(paginate),
                        limit: Some(1),
                        ..Default::default()
                    },
                ),
            )
            .await
            .unwrap();
            assert_eq!(
                result,
                if paginate {
                    json!({"items":[],"nextToken":null})
                } else {
                    json!([])
                }
            );
        }
    }
    for event_type in [None, Some("*".into())] {
        let legacy = with_caller(
            caller.clone(),
            svc.event_query(
                ws.clone(),
                EventQueryParams {
                    event_type: event_type.clone(),
                    limit: Some(2),
                    ..Default::default()
                },
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            legacy
                .as_array()
                .unwrap()
                .iter()
                .map(|e| e["type"].as_str().unwrap())
                .collect::<Vec<_>>(),
            vec!["file:changed", "agent:idle"]
        );
        let page = with_caller(
            caller.clone(),
            svc.event_query(
                ws.clone(),
                EventQueryParams {
                    event_type: event_type.clone(),
                    limit: Some(2),
                    paginate: Some(true),
                    ..Default::default()
                },
            ),
        )
        .await
        .unwrap();
        assert_eq!(page["items"], legacy);
        let next = with_caller(
            caller.clone(),
            svc.event_query(
                ws.clone(),
                EventQueryParams {
                    event_type,
                    limit: Some(2),
                    page_token: Some(page["nextToken"].as_str().unwrap().into()),
                    ..Default::default()
                },
            ),
        )
        .await
        .unwrap();
        assert_eq!(next["items"].as_array().unwrap().len(), 1);
        assert_eq!(next["items"][0]["type"], "note:updated");
        assert!(next["nextToken"].is_null());
    }
    let unrelated = with_caller(
        caller,
        svc.event_query(
            ws.clone(),
            EventQueryParams {
                event_type: Some("agent:*".into()),
                ..Default::default()
            },
        ),
    )
    .await
    .unwrap();
    assert_eq!(unrelated.as_array().unwrap().len(), 1);
    assert_eq!(unrelated[0]["type"], "agent:idle");
    let primary = svc.store.get_primary_principal().await.unwrap();
    let human_history = with_caller(
        Caller::Wire {
            principal_id: primary.id,
            host_role: HostRole::Owner,
        },
        svc.event_query(
            ws,
            EventQueryParams {
                event_type: Some("discussion:*".into()),
                ..Default::default()
            },
        ),
    )
    .await
    .unwrap();
    assert_eq!(human_history.as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn discussion_agent_subscription_admission_rejects_exact_wildcard_and_mixed_lists() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.unwrap();
    let svc = Services::new(store);
    let agent = AgentId::new();
    for types in [
        vec!["discussion:message".into()],
        vec!["discussion:reaction".into()],
        vec!["discussion:*".into()],
        vec!["file:*".into(), "discussion:message".into()],
    ] {
        let caller = Caller::Agent {
            agent_id: agent.clone(),
        };
        let error = with_caller(
            caller.clone(),
            svc.event_subscribe(ws.clone(), Some(agent.clone()), types.clone(), None, None),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(&error, Error::InvalidParams(m) if m.contains("human-only")),
            "{error:?}"
        );
        let error = with_caller(
            caller,
            svc.agent_subscribe(ws.clone(), Some(agent.clone()), types, None, None),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(&error, Error::InvalidParams(m) if m.contains("human-only")),
            "{error:?}"
        );
    }
    assert!(svc
        .store
        .list_event_subscriptions()
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn discussion_requires_humans_and_current_membership() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.unwrap();
    let mut guest = store.get_primary_principal().await.unwrap();
    guest.id = PrincipalId::new();
    guest.is_primary = false;
    guest.login = Some("member".into());
    store.upsert_principal(&guest).await.unwrap();
    let svc = Services::new(store.clone());
    for caller in [
        Caller::Daemon,
        Caller::Agent {
            agent_id: AgentId::new(),
        },
    ] {
        assert!(matches!(
            with_caller(caller.clone(), svc.discussion_send_op(&ws, "id", "hello")).await,
            Err(Error::Forbidden(_) | Error::NotFound(_))
        ));
        assert!(matches!(
            with_caller(caller, svc.discussion_list_op(&ws, None, None, 50)).await,
            Err(Error::Forbidden(_) | Error::NotFound(_))
        ));
    }
    assert!(matches!(
        svc.discussion_send_op(&ws, "id", "hello").await,
        Err(Error::Forbidden(_) | Error::NotFound(_))
    ));
    assert!(matches!(
        with_caller(
            human(&guest.id),
            svc.discussion_list_op(&ws, None, None, 50)
        )
        .await,
        Err(Error::NotFound(_))
    ));
    store
        .add_workspace_member(&ws, &guest.id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    let first = with_caller(
        human(&guest.id),
        svc.discussion_send_op(&ws, "id", " hello\nthere "),
    )
    .await
    .unwrap();
    assert_eq!(first.author.principal_id, Some(guest.id.clone()));
    assert_eq!(first.author.login.as_deref(), Some("member"));
    assert_eq!(first.body, " hello\nthere ");
    for (id, body) in [
        ("", "hello"),
        ("id", " \n\t"),
        ("id", &"é".repeat(8193)),
        (&"x".repeat(129), "hello"),
    ] {
        assert!(matches!(
            with_caller(human(&guest.id), svc.discussion_send_op(&ws, id, body)).await,
            Err(Error::InvalidParams(_))
        ));
    }
    let full = "é".repeat(8192);
    assert!(
        with_caller(human(&guest.id), svc.discussion_send_op(&ws, "full", &full))
            .await
            .is_ok()
    );
    store.remove_workspace_member(&ws, &guest.id).await.unwrap();
    assert!(matches!(
        with_caller(human(&guest.id), svc.discussion_send_op(&ws, "id", "hello")).await,
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        with_caller(
            human(&guest.id),
            svc.discussion_list_op(&ws, None, None, 50)
        )
        .await,
        Err(Error::NotFound(_))
    ));
}

async fn conversation_fixture() -> (TempDb, Services, WorkspaceId, Vec<intent_core::Principal>) {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.unwrap();
    let primary = store.get_primary_principal().await.unwrap();
    let mut people = vec![primary.clone()];
    for name in ["alice", "bob", "carol", "outsider"] {
        let mut p = primary.clone();
        p.id = PrincipalId(name.into());
        p.is_primary = false;
        p.login = Some(name.into());
        p.display_name = Some(format!("Offline {name}"));
        store.upsert_principal(&p).await.unwrap();
        if name != "outsider" {
            store
                .add_workspace_member(&ws, &p.id, WorkspaceRole::Collaborator)
                .await
                .unwrap();
        }
        people.push(p);
    }
    (tmp, Services::new(store), ws, people)
}

fn recipients(ids: &[PrincipalId]) -> DiscussionTarget {
    DiscussionTarget {
        conversation_id: None,
        recipient_principal_ids: Some(ids.to_vec()),
        ..DiscussionTarget::default()
    }
}

#[tokio::test]
async fn discussion_conversations_atomic_creation_retry_and_bounded_discovery() {
    let (_tmp, svc, ws, p) = conversation_fixture().await;
    let caller = human(&p[1].id);
    let target = recipients(&[p[2].id.clone(), p[3].id.clone()]);
    let (a, b) = tokio::join!(
        with_caller(
            caller.clone(),
            svc.discussion_send_to_op(&ws, "first", "hello", &target)
        ),
        with_caller(
            caller.clone(),
            svc.discussion_send_to_op(&ws, "first", "hello", &target)
        )
    );
    let a = a.unwrap();
    assert_eq!(
        serde_json::to_value(a.clone()).unwrap(),
        serde_json::to_value(b.unwrap()).unwrap()
    );
    assert_eq!(a.conversation.kind, "group");
    assert_eq!(a.message.sequence, 1);
    assert_eq!(a.message.conversation_id, a.conversation.id);
    let mut renamed = p[1].clone();
    renamed.login = Some("renamed".into());
    svc.store.upsert_principal(&renamed).await.unwrap();
    let retry = with_caller(
        caller.clone(),
        svc.discussion_send_to_op(
            &ws,
            "first",
            "hello",
            &recipients(&[p[3].id.clone(), p[2].id.clone()]),
        ),
    )
    .await
    .unwrap();
    assert_eq!(
        serde_json::to_value(&retry).unwrap(),
        serde_json::to_value(&a).unwrap()
    );
    for (body, bad) in [
        ("changed", target.clone()),
        ("hello", recipients(&[p[2].id.clone()])),
        ("hello", DiscussionTarget::default()),
    ] {
        assert!(matches!(
            with_caller(
                caller.clone(),
                svc.discussion_send_to_op(&ws, "first", body, &bad)
            )
            .await,
            Err(Error::InvalidParams(_))
        ));
    }
    for bad in [
        recipients(&[]),
        recipients(&[p[1].id.clone()]),
        recipients(&[p[2].id.clone(), p[2].id.clone()]),
        DiscussionTarget {
            conversation_id: Some("default".into()),
            recipient_principal_ids: Some(vec![p[2].id.clone()]),
            ..DiscussionTarget::default()
        },
    ] {
        assert!(matches!(
            with_caller(
                caller.clone(),
                svc.discussion_send_to_op(&ws, "invalid", "hello", &bad)
            )
            .await,
            Err(Error::InvalidParams(_))
        ));
    }
    assert!(matches!(
        with_caller(
            caller.clone(),
            svc.discussion_send_to_op(&ws, "unknown", "hello", &recipients(&[p[4].id.clone()]))
        )
        .await,
        Err(Error::NotFound(_))
    ));
    let first = with_caller(
        caller.clone(),
        svc.discussion_conversations_op(&ws, None, 1),
    )
    .await
    .unwrap();
    assert_eq!(first["conversations"].as_array().unwrap().len(), 1);
    assert_eq!(first["conversations"][0]["id"], "default");
    assert_eq!(first["hasMore"], true);
    let private = with_caller(
        caller.clone(),
        svc.discussion_conversations_op(&ws, Some("default"), 1),
    )
    .await
    .unwrap();
    assert_eq!(private["conversations"][0]["id"], a.conversation.id);
    assert_eq!(
        private["hasMore"], false,
        "failed creates leave no empty conversation"
    );
    let direct = with_caller(
        caller.clone(),
        svc.discussion_send_to_op(&ws, "direct", "dm", &recipients(&[p[2].id.clone()])),
    )
    .await
    .unwrap();
    assert_eq!(direct.conversation.kind, "direct");
    assert_eq!(direct.message.sequence, 1);
    let second = with_caller(
        caller.clone(),
        svc.discussion_send_to_op(
            &ws,
            "next",
            "second",
            &DiscussionTarget {
                conversation_id: Some(a.conversation.id.clone()),
                recipient_principal_ids: None,
                ..Default::default()
            },
        ),
    )
    .await
    .unwrap();
    assert_eq!(second.message.sequence, 2);
    let page = with_caller(
        caller.clone(),
        svc.discussion_history_op(&ws, &a.conversation.id, None, Some(0), 1),
    )
    .await
    .unwrap();
    assert_eq!(page["conversation"]["id"], a.conversation.id);
    assert_eq!(page["messages"][0]["id"], a.message.id);
    assert_eq!(page["hasMore"], true);
    let page = with_caller(
        caller.clone(),
        svc.discussion_conversations_op(&ws, Some("default"), 1),
    )
    .await
    .unwrap();
    assert_eq!(page["conversations"][0]["id"], direct.conversation.id);
    assert_eq!(page["hasMore"], true);
    let next = with_caller(
        caller.clone(),
        svc.discussion_conversations_op(&ws, Some(&direct.conversation.id), 1),
    )
    .await
    .unwrap();
    assert_eq!(next["conversations"][0]["id"], a.conversation.id);
    assert_eq!(next["hasMore"], false);
    let default = with_caller(caller, svc.discussion_send_op(&ws, "default", "everyone"))
        .await
        .unwrap();
    assert_eq!(default.sequence, 1);
    assert_eq!(default.conversation_id, "default");
}

#[tokio::test]
async fn discussion_conversations_authorize_participants_even_for_owner_and_offline_picker() {
    let (_tmp, svc, ws, p) = conversation_fixture().await;
    let alice = human(&p[1].id);
    let created = with_caller(
        alice.clone(),
        svc.discussion_send_to_op(&ws, "first", "private", &recipients(&[p[2].id.clone()])),
    )
    .await
    .unwrap();
    let target = DiscussionTarget {
        conversation_id: Some(created.conversation.id.clone()),
        recipient_principal_ids: None,
        ..Default::default()
    };
    let mut all = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let page = with_caller(
            alice.clone(),
            svc.discussion_recipients_op(&ws, "", after.as_deref(), 1),
        )
        .await
        .unwrap();
        assert!(page["members"].as_array().unwrap().len() <= 1);
        for row in page["members"].as_array().unwrap() {
            all.push(row["principalId"].as_str().unwrap().to_string());
        }
        if page["hasMore"] == false {
            break;
        }
        after = all.last().cloned();
    }
    assert_eq!(all.len(), 3); // offline bob, carol and primary
    assert!(!all.contains(&p[1].id.0));
    assert!(!all.contains(&p[4].id.0));
    let matches = with_caller(
        alice.clone(),
        svc.discussion_recipients_op(&ws, "OFFLINE BOB", None, 50),
    )
    .await
    .unwrap();
    assert_eq!(matches["members"][0]["principalId"], p[2].id.0);
    assert_eq!(matches["members"].as_array().unwrap().len(), 1);
    assert!(matches!(
        with_caller(
            alice.clone(),
            svc.discussion_recipients_op(&ws, "", None, 101)
        )
        .await,
        Err(Error::InvalidParams(_))
    ));
    for caller in [
        Caller::Wire {
            principal_id: p[0].id.clone(),
            host_role: HostRole::Owner,
        },
        human(&p[3].id),
        human(&p[4].id),
        Caller::Agent {
            agent_id: AgentId::new(),
        },
        Caller::Daemon,
    ] {
        assert!(with_caller(
            caller.clone(),
            svc.discussion_history_op(&ws, &created.conversation.id, None, None, 50)
        )
        .await
        .is_err());
        assert!(with_caller(
            caller.clone(),
            svc.discussion_send_to_op(&ws, "bad", "bad", &target)
        )
        .await
        .is_err());
        let expected = match &caller {
            Caller::Agent { .. } => Some(0),
            Caller::Wire { principal_id, .. }
                if principal_id == &p[0].id || principal_id == &p[3].id =>
            {
                Some(1)
            }
            _ => None,
        };
        let list = with_caller(caller, svc.discussion_conversations_op(&ws, None, 100)).await;
        if let Some(expected) = expected {
            assert_eq!(
                list.unwrap()["conversations"].as_array().unwrap().len(),
                expected
            );
        } else {
            assert!(list.is_err());
        }
    }
    svc.store
        .remove_workspace_member(&ws, &p[2].id)
        .await
        .unwrap();
    assert!(with_caller(
        human(&p[2].id),
        svc.discussion_history_op(&ws, &created.conversation.id, None, None, 50)
    )
    .await
    .is_err());
    let remaining = with_caller(alice, svc.discussion_recipients_op(&ws, "bob", None, 50))
        .await
        .unwrap();
    assert_eq!(remaining["members"], json!([]));
}

#[tokio::test]
async fn discussion_private_typing_projection_and_target_validation_are_atomic() {
    let (_tmp, svc, ws, p) = conversation_fixture().await;
    let alice = human(&p[1].id);
    let made = with_caller(
        alice.clone(),
        svc.discussion_send_to_op(&ws, "dm", "private", &recipients(&[p[2].id.clone()])),
    )
    .await
    .unwrap();
    with_caller(alice.clone(), svc.presence_connect_op("alice".into()))
        .await
        .unwrap();
    let active = json!({"focus":[],"discussionTyping":{"workspaceId":ws,"conversationId":made.conversation.id}});
    with_caller(
        alice.clone(),
        svc.presence_update_op("alice".into(), active),
    )
    .await
    .unwrap();
    let snapshot = with_caller(human(&p[2].id), svc.presence_snapshot_op(ws.clone()))
        .await
        .unwrap();
    let entry = snapshot["members"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["principalId"] == p[1].id.0)
        .unwrap()["discussionTyping"][0]
        .clone();
    assert_eq!(entry["conversationId"], made.conversation.id);
    for caller in [
        human(&p[3].id),
        Caller::Wire {
            principal_id: p[0].id.clone(),
            host_role: HostRole::Owner,
        },
    ] {
        let snapshot = with_caller(caller.clone(), svc.presence_snapshot_op(ws.clone()))
            .await
            .unwrap();
        assert!(snapshot["members"]
            .as_array()
            .unwrap()
            .iter()
            .all(|m| m["discussionTyping"] == json!([])));
        let mut data =
            json!({"members":[{"principalId":p[1].id,"discussionTyping":[entry.clone()]}]});
        with_caller(caller, svc.project_discussion_typing(&ws, &mut data))
            .await
            .unwrap();
        assert_eq!(data["members"][0]["discussionTyping"], json!([]));
    }
    let denied =
        json!({"focus":[],"discussionTyping":{"workspaceId":ws,"conversationId":"missing"}});
    assert!(matches!(
        with_caller(
            alice.clone(),
            svc.presence_update_op("alice".into(), denied)
        )
        .await,
        Err(Error::NotFound(_))
    ));
    let unchanged = with_caller(alice.clone(), svc.presence_snapshot_op(ws.clone()))
        .await
        .unwrap();
    assert_eq!(unchanged["members"][0]["discussionTyping"][0], entry);
    with_caller(
        alice.clone(),
        svc.presence_update_op(
            "alice".into(),
            json!({"focus":[],"discussionTyping":{"workspaceId":ws}}),
        ),
    )
    .await
    .unwrap();
    let moved = with_caller(alice, svc.presence_snapshot_op(ws))
        .await
        .unwrap();
    assert_eq!(
        moved["members"][0]["discussionTyping"][0]["conversationId"],
        "default"
    );
    assert!(
        moved["members"][0]["discussionTyping"][0]["pulse"]
            .as_u64()
            .unwrap()
            > entry["pulse"].as_u64().unwrap()
    );
}

#[tokio::test]
async fn discussion_store_rechecks_sender_membership_but_retry_survives_recipient_removal() {
    let (_tmp, svc, ws, p) = conversation_fixture().await;
    let alice = human(&p[1].id);
    let recipients = recipients(&[p[2].id.clone()]);
    let created = with_caller(
        alice.clone(),
        svc.discussion_send_to_op(&ws, "private", "original", &recipients),
    )
    .await
    .unwrap();
    let public = with_caller(
        alice.clone(),
        svc.discussion_send_op(&ws, "public", "original"),
    )
    .await
    .unwrap();
    svc.store
        .remove_workspace_member(&ws, &p[2].id)
        .await
        .unwrap();
    let retry = with_caller(
        alice,
        svc.discussion_send_to_op(&ws, "private", "original", &recipients),
    )
    .await
    .unwrap();
    assert_eq!(
        retry.message, created.message,
        "recipient removal cannot turn uncertain commit recovery into another creation"
    );
    svc.store
        .remove_workspace_member(&ws, &p[1].id)
        .await
        .unwrap();
    // Calling the store directly models a caller whose service-level access
    // check passed before another transaction removed their membership.
    for (key, target) in [
        ("private", recipients),
        ("public", DiscussionTarget::default()),
        (
            "new-private",
            DiscussionTarget {
                conversation_id: Some(created.conversation.id.clone()),
                recipient_principal_ids: None,
                ..Default::default()
            },
        ),
        ("new-public", DiscussionTarget::default()),
    ] {
        assert!(matches!(
            svc.store
                .send_discussion_message(&ws, key, "original", &created.message.author, &target)
                .await,
            Err(Error::NotFound(_))
        ));
    }
    assert_eq!(
        svc.store
            .list_discussion_messages(&ws, None, None, 50)
            .await
            .unwrap()
            .messages,
        vec![public]
    );
    assert_eq!(
        svc.store
            .list_discussion_messages_in(&ws, &created.conversation.id, None, None, 50)
            .await
            .unwrap()
            .messages,
        vec![created.message]
    );
}

#[tokio::test]
async fn discussion_export_cannot_bypass_participant_authorization() {
    let (_tmp, svc, ws, p) = conversation_fixture().await;
    with_caller(
        human(&p[1].id),
        svc.discussion_send_to_op(
            &ws,
            "private",
            "private body",
            &recipients(&[p[2].id.clone()]),
        ),
    )
    .await
    .unwrap();
    for caller in [
        Caller::Wire {
            principal_id: p[0].id.clone(),
            host_role: HostRole::Owner,
        },
        Caller::Agent {
            agent_id: AgentId::new(),
        },
    ] {
        assert!(matches!(
            with_caller(caller, svc.workspace_export_start(ws.clone())).await,
            Err(Error::Unsupported(_))
        ));
    }
    assert!(
        svc.transfer_exports.lock().unwrap().is_empty(),
        "no readable export session"
    );
}

#[tokio::test]
async fn discussion_reactions_are_idempotent_projected_and_participant_scoped() {
    let (_tmp, svc, ws, p) = conversation_fixture().await;
    let made = with_caller(
        human(&p[1].id),
        svc.discussion_send_to_op(&ws, "dm", "private", &recipients(&[p[2].id.clone()])),
    )
    .await
    .unwrap();
    let id = &made.conversation.id;
    let message = &made.message.id;
    let (a, b) = tokio::join!(
        with_caller(
            human(&p[1].id),
            svc.discussion_react_op(&ws, id, message, "👍", true)
        ),
        with_caller(
            human(&p[1].id),
            svc.discussion_react_op(&ws, id, message, "👍", true)
        ),
    );
    let a = a.unwrap();
    assert_eq!(a, b.unwrap());
    assert_eq!(a.reaction_version, 1);
    assert_eq!(
        serde_json::to_value(&a.reactions).unwrap(),
        json!([{"reaction":"👍","count":1,"reacted":true,"reactors":[made.message.author]}])
    );
    let bob = with_caller(
        human(&p[2].id),
        svc.discussion_history_op(&ws, id, None, None, 50),
    )
    .await
    .unwrap();
    assert_eq!(bob["messages"][0]["reactions"][0]["reacted"], false);
    let both = with_caller(
        human(&p[2].id),
        svc.discussion_react_op(&ws, id, message, "👍", true),
    )
    .await
    .unwrap();
    assert_eq!(both.reaction_version, 2);
    assert_eq!(both.reactions[0].count, 2);
    let other = with_caller(
        human(&p[2].id),
        svc.discussion_react_op(&ws, id, message, "👀", true),
    )
    .await
    .unwrap();
    assert_eq!(other.reaction_version, 3);
    assert_eq!(other.reactions.len(), 2);
    let removed = with_caller(
        human(&p[1].id),
        svc.discussion_react_op(&ws, id, message, "👍", false),
    )
    .await
    .unwrap();
    assert_eq!(removed.reaction_version, 4);
    assert_eq!(removed.reactions[0].count, 1);
    assert!(!removed.reactions[0].reacted);
    assert_eq!(
        with_caller(
            human(&p[1].id),
            svc.discussion_react_op(&ws, id, message, "👍", false)
        )
        .await
        .unwrap(),
        removed
    );
    let event = intent_core::Event {
        id: "reaction".into(),
        workspace_id: ws.clone(),
        timestamp: intent_core::now_iso(),
        event_type: DISCUSSION_REACTION.into(),
        actor: EventActor::default(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        metadata: None,
        data: json!({"workspaceId":ws,"conversationId":id,"messageId":message,"reactionVersion":0}),
    };
    for (i, selected) in [(1, false), (2, true)] {
        let projected = with_caller(
            human(&p[i].id),
            svc.discussion_project_event_op(event.clone()),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            projected.data,
            json!({"workspaceId":ws,"conversationId":id,"messageId":message,"reactionVersion":4,"reactions":[{"reaction":"👍","count":1,"reacted":selected,"reactors":other.reactions[1].reactors},{"reaction":"👀","count":1,"reacted":selected,"reactors":other.reactions[1].reactors}]})
        );
    }
    for caller in [
        human(&p[0].id),
        human(&p[3].id),
        human(&p[4].id),
        Caller::Agent {
            agent_id: AgentId::new(),
        },
        Caller::Daemon,
    ] {
        assert!(with_caller(
            caller.clone(),
            svc.discussion_react_op(&ws, id, message, "👍", true)
        )
        .await
        .is_err());
        assert!(
            with_caller(caller, svc.discussion_project_event_op(event.clone()))
                .await
                .unwrap()
                .is_none()
        );
    }
    for invalid in ["", "❤️", "👍🏻", "thumbs-up"] {
        assert!(matches!(
            with_caller(
                human(&p[1].id),
                svc.discussion_react_op(&ws, id, message, invalid, true)
            )
            .await,
            Err(Error::InvalidParams(_))
        ));
    }
    assert!(with_caller(
        human(&p[1].id),
        svc.discussion_react_op(&ws, "default", message, "👍", true)
    )
    .await
    .is_err());
    svc.store
        .remove_workspace_member(&ws, &p[2].id)
        .await
        .unwrap();
    assert!(with_caller(
        human(&p[2].id),
        svc.discussion_react_op(&ws, id, message, "👍", false)
    )
    .await
    .is_err());
    assert!(
        with_caller(human(&p[2].id), svc.discussion_project_event_op(event))
            .await
            .unwrap()
            .is_none()
    );
    let history = with_caller(
        human(&p[1].id),
        svc.discussion_history_op(&ws, id, None, None, 50),
    )
    .await
    .unwrap();
    assert_eq!(history["messages"].as_array().unwrap().len(), 1);
    assert_eq!(history["messages"][0]["sequence"], 1);
    assert_eq!(history["messages"][0]["reactionVersion"], 4);
}

#[tokio::test]
async fn discussion_replies_bind_retry_and_snapshot_without_cross_conversation_leaks() {
    let (_tmp, svc, ws, p) = conversation_fixture().await;
    let caller = human(&p[1].id);
    let made = with_caller(
        caller.clone(),
        svc.discussion_send_to_op(
            &ws,
            "original",
            "Original quote",
            &recipients(&[p[2].id.clone()]),
        ),
    )
    .await
    .unwrap();
    let target = DiscussionTarget {
        conversation_id: Some(made.conversation.id.clone()),
        reply_to_message_id: Some(made.message.id.clone()),
        ..Default::default()
    };
    let reply = with_caller(
        human(&p[2].id),
        svc.discussion_send_to_op(&ws, "reply", "A reply", &target),
    )
    .await
    .unwrap();
    assert_eq!(
        reply.message.reply_to_message_id,
        Some(made.message.id.clone())
    );
    assert_eq!(
        serde_json::to_value(&reply.message.reply_to).unwrap(),
        json!({"id":made.message.id,"author":made.message.author,"body":"Original quote"})
    );
    let mut changed = p[1].clone();
    changed.display_name = Some("Changed after send".into());
    svc.store.upsert_principal(&changed).await.unwrap();
    let retry = with_caller(
        human(&p[2].id),
        svc.discussion_send_to_op(&ws, "reply", "A reply", &target),
    )
    .await
    .unwrap();
    assert_eq!(retry.message, reply.message);
    for reply_id in [None, Some(reply.message.id.clone())] {
        let different = DiscussionTarget {
            reply_to_message_id: reply_id,
            ..target.clone()
        };
        assert!(matches!(
            with_caller(
                human(&p[2].id),
                svc.discussion_send_to_op(&ws, "reply", "A reply", &different)
            )
            .await,
            Err(Error::InvalidParams(_))
        ));
    }
    for bad in [
        DiscussionTarget {
            reply_to_message_id: Some(made.message.id.clone()),
            ..Default::default()
        },
        DiscussionTarget {
            reply_to_message_id: Some(made.message.id.clone()),
            ..recipients(&[p[3].id.clone()])
        },
        DiscussionTarget {
            reply_to_message_id: Some("missing".into()),
            ..target.clone()
        },
    ] {
        assert!(matches!(
            with_caller(
                caller.clone(),
                svc.discussion_send_to_op(&ws, "bad", "Must not exist", &bad)
            )
            .await,
            Err(Error::NotFound(_))
        ));
    }
    let other = WorkspaceId::new();
    svc.store
        .insert_workspace(&workspace(&other))
        .await
        .unwrap();
    assert!(with_caller(
        human(&p[0].id),
        svc.discussion_send_to_op(
            &other,
            "bad",
            "Cross workspace",
            &DiscussionTarget {
                reply_to_message_id: Some(made.message.id.clone()),
                ..Default::default()
            }
        )
    )
    .await
    .is_err());
    let all = with_caller(
        caller.clone(),
        svc.discussion_conversations_op(&ws, None, 100),
    )
    .await
    .unwrap();
    assert_eq!(all["conversations"].as_array().unwrap().len(), 2);
    let nested = DiscussionTarget {
        reply_to_message_id: Some(reply.message.id.clone()),
        ..target
    };
    let next = with_caller(
        caller,
        svc.discussion_send_to_op(&ws, "next", "Reply to a reply", &nested),
    )
    .await
    .unwrap();
    assert_eq!(next.message.sequence, 3);
    assert_eq!(next.message.reply_to.unwrap().body, "A reply");
    let wire = serde_json::to_value(next.message.reply_to_message_id).unwrap();
    assert_eq!(wire, reply.message.id);
}

#[tokio::test]
async fn discussion_reaction_concurrency_and_send_retry_keep_viewer_state() {
    let (tmp, svc, ws, p) = conversation_fixture().await;
    let original = with_caller(
        human(&p[1].id),
        svc.discussion_send_op(&ws, "original", "Shared"),
    )
    .await
    .unwrap();
    let (alice, bob) = tokio::join!(
        with_caller(
            human(&p[1].id),
            svc.discussion_react_op(&ws, "default", &original.id, "👍", true)
        ),
        with_caller(
            human(&p[2].id),
            svc.discussion_react_op(&ws, "default", &original.id, "👍", true)
        ),
    );
    let mut versions = vec![
        alice.unwrap().reaction_version,
        bob.unwrap().reaction_version,
    ];
    versions.sort_unstable();
    assert_eq!(versions, vec![1, 2]);
    let retry = with_caller(
        human(&p[1].id),
        svc.discussion_send_op(&ws, "original", "Shared"),
    )
    .await
    .unwrap();
    assert_eq!(retry.id, original.id);
    assert_eq!(retry.reaction_version, 2);
    assert_eq!(retry.reactions[0].count, 2);
    assert!(retry.reactions[0].reacted);
    let (removed, added) = tokio::join!(
        with_caller(
            human(&p[1].id),
            svc.discussion_react_op(&ws, "default", &original.id, "👍", false)
        ),
        with_caller(
            human(&p[3].id),
            svc.discussion_react_op(&ws, "default", &original.id, "👍", true)
        ),
    );
    let mut versions = vec![
        removed.unwrap().reaction_version,
        added.unwrap().reaction_version,
    ];
    versions.sort_unstable();
    assert_eq!(versions, vec![3, 4]);
    let reopened = Services::new(Store::open(&tmp.path).await.unwrap());
    for (person, selected) in [(1, false), (2, true), (3, true)] {
        let history = with_caller(
            human(&p[person].id),
            reopened.discussion_history_op(&ws, "default", None, None, 50),
        )
        .await
        .unwrap();
        assert_eq!(
            history["messages"][0]["reactions"],
            json!([{"reaction":"👍","count":2,"reacted":selected,"reactors":history["messages"][0]["reactions"][0]["reactors"]}])
        );
        assert_eq!(history["messages"][0]["reactionVersion"], 4);
        assert_eq!(history["messages"][0]["sequence"], 1);
    }
}

mod agents;

mod principal_mentions;
