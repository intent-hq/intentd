use super::*;

#[tokio::test]
async fn discussion_management_roles_deletion_and_reopen() {
    let (tmp, svc, ws, p) = conversation_fixture().await;
    svc.store
        .set_workspace_member_role(&ws, &p[2].id, WorkspaceRole::Manager)
        .await
        .unwrap();
    let sent = with_caller(
        human(&p[1].id),
        svc.discussion_send_to_op(&ws, "create", "Everyone", &new_conversation()),
    )
    .await
    .unwrap();
    let id = &sent.conversation.id;
    for index in [0, 1, 2, 3] {
        let history = with_caller(
            human(&p[index].id),
            svc.discussion_history_op(&ws, id, None, None, 50),
        )
        .await
        .unwrap();
        assert_eq!(history["messages"][0]["id"], sent.message.id);
    }
    for index in [1, 3] {
        assert!(matches!(
            with_caller(
                human(&p[index].id),
                svc.discussion_manage_op(&ws, id, Some("Denied"))
            )
            .await,
            Err(Error::Forbidden(_))
        ));
    }
    assert!(matches!(
        with_caller(
            human(&p[4].id),
            svc.discussion_manage_op(&ws, id, Some("Hidden"))
        )
        .await,
        Err(Error::NotFound(_))
    ));
    for name in ["", "   "] {
        assert!(matches!(
            with_caller(
                human(&p[2].id),
                svc.discussion_manage_op(&ws, id, Some(name))
            )
            .await,
            Err(Error::InvalidParams(_))
        ));
    }
    let renamed = with_caller(
        human(&p[2].id),
        svc.discussion_manage_op(&ws, id, Some("  Plans  ")),
    )
    .await
    .unwrap();
    assert_eq!(renamed["conversation"]["name"], "Plans");
    assert!(matches!(
        with_caller(human(&p[2].id), svc.discussion_manage_op(&ws, id, None)).await,
        Err(Error::Forbidden(_))
    ));
    svc.store
        .set_workspace_member_role(&ws, &p[2].id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    assert!(matches!(
        with_caller(
            human(&p[2].id),
            svc.discussion_manage_op(&ws, id, Some("Stale role"))
        )
        .await,
        Err(Error::Forbidden(_))
    ));
    let reopened = Services::new_isolated(Store::open(&tmp.path).await.unwrap());
    let history = with_caller(
        human(&p[3].id),
        reopened.discussion_history_op(&ws, id, None, None, 50),
    )
    .await
    .unwrap();
    assert_eq!(history["conversation"]["name"], "Plans");
    for conversation in [id.as_str(), "default"] {
        with_caller(
            human(&p[0].id),
            svc.discussion_manage_op(&ws, conversation, Some("Renamed")),
        )
        .await
        .unwrap();
        assert_eq!(
            with_caller(
                human(&p[0].id),
                svc.discussion_manage_op(&ws, conversation, None)
            )
            .await
            .unwrap(),
            json!({"deleted":true,"conversationId":conversation})
        );
        assert!(matches!(
            with_caller(
                human(&p[0].id),
                reopened.discussion_history_op(&ws, conversation, None, None, 50)
            )
            .await,
            Err(Error::NotFound(_))
        ));
        assert!(with_caller(
            human(&p[1].id),
            svc.discussion_send_to_op(
                &ws,
                "resurrect",
                "No",
                &DiscussionTarget {
                    conversation_id: Some(conversation.into()),
                    ..Default::default()
                }
            )
        )
        .await
        .is_err());
    }
    assert!(with_caller(
        human(&p[1].id),
        svc.discussion_send_to_op(&ws, "create", "Everyone", &new_conversation())
    )
    .await
    .is_err());
    let list = with_caller(
        human(&p[1].id),
        reopened.discussion_conversations_op(&ws, None, 50),
    )
    .await
    .unwrap();
    assert_eq!(list, json!({"conversations":[],"hasMore":false}));
    let after = with_caller(
        human(&p[1].id),
        reopened.discussion_send_to_op(
            &ws,
            "after-delete",
            "New conversation",
            &new_conversation(),
        ),
    )
    .await
    .unwrap();
    assert_ne!(after.conversation.id, *id);
}

#[tokio::test]
async fn discussion_legacy_private_history_is_read_only_and_never_widened() {
    let (_tmp, svc, ws, p) = conversation_fixture().await;
    let private = with_caller(
        human(&p[1].id),
        svc.seed_legacy_discussion(
            &ws,
            "legacy",
            "Private history",
            &recipients(&[p[2].id.clone()]),
        ),
    )
    .await
    .unwrap();
    let id = &private.conversation.id;
    let target = DiscussionTarget {
        conversation_id: Some(id.clone()),
        ..Default::default()
    };
    for index in [1, 2] {
        let history = with_caller(
            human(&p[index].id),
            svc.discussion_history_op(&ws, id, None, None, 50),
        )
        .await
        .unwrap();
        assert_eq!(history["messages"][0]["body"], "Private history");
        assert!(matches!(
            with_caller(
                human(&p[index].id),
                svc.discussion_send_to_op(&ws, "append", "No", &target)
            )
            .await,
            Err(Error::Forbidden(_))
        ));
        assert!(matches!(
            with_caller(
                human(&p[index].id),
                svc.discussion_react_op(&ws, id, &private.message.id, "👍", true)
            )
            .await,
            Err(Error::Forbidden(_))
        ));
    }
    for index in [0, 3, 4] {
        assert!(matches!(
            with_caller(
                human(&p[index].id),
                svc.discussion_history_op(&ws, id, None, None, 50)
            )
            .await,
            Err(Error::NotFound(_))
        ));
    }
    assert!(matches!(
        with_caller(human(&p[0].id), svc.discussion_manage_op(&ws, id, None)).await,
        Err(Error::NotFound(_))
    ));
    assert!(matches!(
        with_caller(
            human(&p[1].id),
            svc.discussion_send_to_op(&ws, "dm", "No", &recipients(&[p[2].id.clone()]))
        )
        .await,
        Err(Error::InvalidParams(_))
    ));
    assert_eq!(
        svc.store
            .discussion_conversation(&ws, id)
            .await
            .unwrap()
            .kind,
        "direct"
    );
}
