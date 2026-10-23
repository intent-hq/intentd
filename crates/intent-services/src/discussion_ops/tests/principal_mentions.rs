use super::*;
use intent_core::discussion::DiscussionPrincipalMention;

fn mentioned(id: &PrincipalId) -> DiscussionTarget {
    DiscussionTarget {
        principal_mentions: vec![DiscussionPrincipalMention {
            principal_id: id.clone(),
            name: "Bob".into(),
            start: 3,
            end: 7,
        }],
        ..Default::default()
    }
}

#[tokio::test]
async fn discussion_principal_mentions_persist_retry_and_never_infer_from_text() {
    let (tmp, svc, ws, people) = conversation_fixture().await;
    let target = mentioned(&people[2].id);
    let body = "😀 @Bob @Bob";
    let first = with_caller(
        human(&people[1].id),
        svc.discussion_send_to_op(&ws, "mention", body, &target),
    )
    .await
    .unwrap();
    assert_eq!(first.message.principal_mentions, target.principal_mentions);
    assert!(first.message.recipient_agent_ids.is_empty());
    assert!(svc.store.load_all_agent_queues().await.unwrap().is_empty());
    // Live profile changes cannot rewrite the selected name or span on retry.
    let mut renamed = people[2].clone();
    renamed.display_name = Some("Robert".into());
    svc.store.upsert_principal(&renamed).await.unwrap();
    let retry = with_caller(
        human(&people[1].id),
        svc.discussion_send_to_op(&ws, "mention", body, &target),
    )
    .await
    .unwrap();
    assert_eq!(retry.message, first.message);
    for changed in [
        DiscussionTarget::default(),
        mentioned(&people[3].id),
        DiscussionTarget {
            principal_mentions: vec![DiscussionPrincipalMention {
                name: "Bo".into(),
                end: 6,
                ..target.principal_mentions[0].clone()
            }],
            ..Default::default()
        },
        DiscussionTarget {
            principal_mentions: vec![DiscussionPrincipalMention {
                start: 8,
                end: 12,
                ..target.principal_mentions[0].clone()
            }],
            ..Default::default()
        },
    ] {
        assert!(matches!(
            with_caller(
                human(&people[1].id),
                svc.discussion_send_to_op(&ws, "mention", body, &changed)
            )
            .await,
            Err(Error::InvalidParams(_))
        ));
    }
    let plain = with_caller(
        human(&people[1].id),
        svc.discussion_send_op(&ws, "plain", body),
    )
    .await
    .unwrap();
    assert!(plain.principal_mentions.is_empty());
    let reopened = Services::new(Store::open(&tmp.path).await.unwrap());
    let history = with_caller(
        human(&people[1].id),
        reopened.discussion_history_op(&ws, "default", None, None, 50),
    )
    .await
    .unwrap();
    assert_eq!(
        history["messages"][0]["principalMentions"],
        json!([{"principalId":people[2].id,"name":"Bob","start":3,"end":7}])
    );
    svc.store
        .remove_workspace_member(&ws, &people[2].id)
        .await
        .unwrap();
    for key in ["mention", "after-removal"] {
        assert!(matches!(
            with_caller(
                human(&people[1].id),
                svc.discussion_send_to_op(&ws, key, body, &target)
            )
            .await,
            Err(Error::NotFound(_))
        ));
    }
}

#[tokio::test]
async fn discussion_principal_mentions_require_current_members_and_private_participation() {
    let (_tmp, svc, ws, people) = conversation_fixture().await;
    let body = "😀 @Bob";
    for id in [&people[4].id, &PrincipalId::from("absent")] {
        assert!(matches!(
            with_caller(
                human(&people[1].id),
                svc.discussion_send_to_op(&ws, "invalid", body, &mentioned(id))
            )
            .await,
            Err(Error::NotFound(_))
        ));
    }
    let mut target = mentioned(&people[2].id);
    target.recipient_principal_ids = Some(vec![people[2].id.clone()]);
    let created = with_caller(
        human(&people[1].id),
        svc.discussion_send_to_op(&ws, "private", body, &target),
    )
    .await
    .unwrap();
    assert_eq!(
        created.message.principal_mentions,
        target.principal_mentions
    );
    assert_eq!(
        with_caller(
            human(&people[1].id),
            svc.discussion_send_to_op(&ws, "private", body, &target)
        )
        .await
        .unwrap()
        .message,
        created.message
    );
    let mut outsider = mentioned(&people[3].id);
    outsider.conversation_id = Some(created.conversation.id.clone());
    assert!(matches!(
        with_caller(
            human(&people[1].id),
            svc.discussion_send_to_op(&ws, "not-participant", body, &outsider)
        )
        .await,
        Err(Error::NotFound(_))
    ));
    outsider.conversation_id = None;
    outsider.recipient_principal_ids = Some(vec![people[2].id.clone()]);
    assert!(matches!(
        with_caller(
            human(&people[1].id),
            svc.discussion_send_to_op(&ws, "bad-first-send", body, &outsider)
        )
        .await,
        Err(Error::NotFound(_))
    ));
    let conversations = with_caller(
        human(&people[1].id),
        svc.discussion_conversations_op(&ws, None, 50),
    )
    .await
    .unwrap();
    assert_eq!(
        conversations["conversations"].as_array().unwrap().len(),
        2,
        "failed first send must roll back its conversation"
    );
    let author = created.message.author;
    assert!(matches!(
        svc.store
            .send_discussion_message(&ws, "store-bypass", body, &author, &outsider)
            .await,
        Err(Error::NotFound(_))
    ));
    let agent_author = DiscussionAuthor {
        principal_id: None,
        agent_id: Some(AgentId::from("agent")),
        login: None,
        display_name: None,
        avatar_url: None,
    };
    assert!(matches!(
        svc.store
            .send_discussion_message(
                &ws,
                "agent-store-bypass",
                body,
                &agent_author,
                &mentioned(&people[2].id)
            )
            .await,
        Err(Error::Forbidden(_))
    ));
    // Direct service callers cannot bypass human-only notification creation.
    assert!(matches!(
        with_caller(
            Caller::Agent {
                agent_id: AgentId::from("agent")
            },
            svc.discussion_send_to_op(&ws, "agent", body, &target)
        )
        .await,
        Err(Error::Forbidden(_))
    ));
}
