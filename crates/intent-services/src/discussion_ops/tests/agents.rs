use super::*;

async fn agent(svc: &Services, ws: &WorkspaceId, id: &str) -> AgentId {
    let id = AgentId::from(id);
    sqlx::query("INSERT INTO agent_session(id,workspace_id,name,status,created_at,updated_at) VALUES(?,?,'Discussion helper','idle',?,?)")
        .bind(id.as_str()).bind(ws.as_str()).bind(intent_core::now_iso()).bind(intent_core::now_iso())
        .execute(svc.store.write_pool()).await.unwrap();
    id
}
fn addressed(ids: &[AgentId]) -> DiscussionTarget {
    DiscussionTarget {
        recipient_agent_ids: ids.to_vec(),
        ..Default::default()
    }
}
fn as_agent(id: &AgentId) -> Caller {
    Caller::Agent {
        agent_id: id.clone(),
    }
}

#[tokio::test]
async fn discussion_agents_receive_only_explicit_messages_and_can_read_and_reply() {
    let (_tmp, svc, ws, people) = conversation_fixture().await;
    let a = agent(&svc, &ws, "agent-addressed-a").await;
    let b = agent(&svc, &ws, "agent-addressed-b").await;
    assert!(with_caller(
        as_agent(&a),
        svc.discussion_history_op(&ws, "default", None, None, 50)
    )
    .await
    .is_err());
    assert_eq!(
        with_caller(as_agent(&a), svc.discussion_conversations_op(&ws, None, 50))
            .await
            .unwrap()["conversations"],
        json!([])
    );
    with_caller(
        human(&people[1].id),
        svc.discussion_send_op(&ws, "ordinary", "human context"),
    )
    .await
    .unwrap();
    assert!(svc.store.load_all_agent_queues().await.unwrap().is_empty());
    let sent = with_caller(
        human(&people[1].id),
        svc.discussion_send_to_op(
            &ws,
            "address",
            "please review",
            &addressed(std::slice::from_ref(&a)),
        ),
    )
    .await
    .unwrap();
    assert_eq!(sent.message.recipient_agent_ids, vec![a.clone()]);
    assert_eq!(svc.queue_snapshot(&a).len(), 1);
    let durable = svc.store.load_all_agent_queues().await.unwrap();
    assert_eq!(
        durable[0].payload["messageMetadata"]["fromPrincipalId"],
        json!(people[1].id)
    );
    assert_eq!(durable[0].payload["userOrigin"], true);
    assert_eq!(
        durable[0].payload["queuedAt"],
        json!(sent.message.created_at)
    );
    assert!(durable[0].payload["submissionOrder"].as_u64().unwrap() > 0);
    assert_eq!(durable[0].payload["correlationOrderKnown"], true);
    assert!(svc.queue_snapshot(&b).is_empty());
    let content = svc.queue_snapshot(&a)[0]["content"]
        .as_str()
        .unwrap()
        .to_owned();
    assert!(content.contains("please review") && content.contains("ws.discussion.send"));
    assert!(!content.contains("human context"));
    let history = with_caller(
        as_agent(&a),
        svc.discussion_history_op(&ws, "default", None, None, 1),
    )
    .await
    .unwrap();
    assert_eq!(history["messages"].as_array().unwrap().len(), 1);
    assert_eq!(history["hasMore"], true);
    assert_eq!(
        with_caller(as_agent(&a), svc.discussion_conversations_op(&ws, None, 1))
            .await
            .unwrap()["conversations"][0]["id"],
        "default"
    );
    with_caller(
        human(&people[1].id),
        svc.discussion_send_op(&ws, "later", "not delivered"),
    )
    .await
    .unwrap();
    let reply = with_caller(
        as_agent(&a),
        svc.discussion_send_op(&ws, "answer", "review complete"),
    )
    .await
    .unwrap();
    assert_eq!(reply.author.agent_id, Some(a.clone()));
    assert_eq!(reply.author.principal_id, None);
    assert_eq!(
        reply.author.display_name.as_deref(),
        Some("Discussion helper")
    );
    assert_eq!(svc.queue_snapshot(&a).len(), 1);
    assert!(svc.queue_snapshot(&b).is_empty());
    with_caller(
        as_agent(&a),
        svc.discussion_send_to_op(
            &ws,
            "ask-b",
            "second opinion",
            &addressed(std::slice::from_ref(&b)),
        ),
    )
    .await
    .unwrap();
    assert_eq!(svc.queue_snapshot(&b).len(), 1);
    assert!(with_caller(
        as_agent(&b),
        svc.discussion_history_op(&ws, "default", None, None, 100)
    )
    .await
    .is_ok());
    assert!(with_caller(
        as_agent(&a),
        svc.discussion_send_to_op(&ws, "self", "loop", &addressed(std::slice::from_ref(&a)))
    )
    .await
    .is_err());
    assert!(with_caller(
        as_agent(&b),
        svc.event_subscribe(
            ws.clone(),
            Some(b.clone()),
            vec!["discussion:message".into()],
            None,
            None
        )
    )
    .await
    .is_err());
    assert!(svc
        .store
        .discussion_pending_deliveries(None)
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn discussion_agents_cannot_target_private_foreign_retired_or_forged_recipients() {
    let (_tmp, svc, ws, people) = conversation_fixture().await;
    let a = agent(&svc, &ws, "agent-private-a").await;
    let b = agent(&svc, &ws, "agent-private-b").await;
    let foreign_ws = WorkspaceId::new();
    svc.store
        .insert_workspace(&workspace(&foreign_ws))
        .await
        .unwrap();
    let foreign = agent(&svc, &foreign_ws, "agent-foreign").await;
    sqlx::query("UPDATE agent_session SET retired_at=? WHERE id=?")
        .bind(intent_core::now_iso())
        .bind(b.as_str())
        .execute(svc.store.write_pool())
        .await
        .unwrap();
    for (i, ids) in [
        vec![a.clone(), a.clone()],
        vec![foreign.clone()],
        vec![b],
        vec![AgentId::new()],
        vec![a.clone(); 32],
    ]
    .iter()
    .enumerate()
    {
        assert!(with_caller(
            human(&people[1].id),
            svc.discussion_send_to_op(
                &ws,
                &format!("invalid-{i}"),
                "must not persist",
                &addressed(ids)
            )
        )
        .await
        .is_err());
    }
    assert!(svc
        .store
        .list_discussion_messages(&ws, None, None, 50)
        .await
        .unwrap()
        .messages
        .is_empty());
    assert!(!svc
        .store
        .discussion_agent_access(&ws, "default", &a)
        .await
        .unwrap());
    with_caller(
        human(&people[1].id),
        svc.discussion_send_to_op(&ws, "grant", "public", &addressed(std::slice::from_ref(&a))),
    )
    .await
    .unwrap();
    let private = with_caller(
        human(&people[1].id),
        svc.discussion_send_to_op(
            &ws,
            "private",
            "secret",
            &recipients(&[people[2].id.clone()]),
        ),
    )
    .await
    .unwrap();
    let private_target = DiscussionTarget {
        conversation_id: Some(private.conversation.id.clone()),
        recipient_agent_ids: vec![a.clone()],
        ..Default::default()
    };
    assert!(with_caller(
        human(&people[1].id),
        svc.discussion_send_to_op(&ws, "private-address", "secret", &private_target)
    )
    .await
    .is_err());
    assert!(with_caller(
        as_agent(&a),
        svc.discussion_history_op(&ws, &private.conversation.id, None, None, 50)
    )
    .await
    .is_err());
    assert!(with_caller(
        as_agent(&a),
        svc.discussion_history_op(&foreign_ws, "default", None, None, 50)
    )
    .await
    .is_err());
    assert!(with_caller(
        as_agent(&a),
        svc.discussion_send_to_op(&ws, "private-reply", "secret", &private_target)
    )
    .await
    .is_err());
    assert!(with_caller(
        as_agent(&a),
        svc.discussion_react_op(&ws, "default", &private.message.id, "👍", true)
    )
    .await
    .is_err());
    let mut create = recipients(&[people[2].id.clone()]);
    create.recipient_agent_ids.push(a.clone());
    assert!(with_caller(
        human(&people[1].id),
        svc.discussion_send_to_op(&ws, "create-address", "secret", &create)
    )
    .await
    .is_err());
    sqlx::query("UPDATE agent_session SET retired_at=? WHERE id=?")
        .bind(intent_core::now_iso())
        .bind(a.as_str())
        .execute(svc.store.write_pool())
        .await
        .unwrap();
    assert!(with_caller(
        as_agent(&a),
        svc.discussion_history_op(&ws, "default", None, None, 50)
    )
    .await
    .is_err());
    assert!(
        with_caller(as_agent(&a), svc.discussion_send_op(&ws, "retired", "no"))
            .await
            .is_err()
    );
}

#[tokio::test]
async fn discussion_agent_retry_partial_handoff_and_restart_never_redispatch() {
    let (_tmp, svc, ws, people) = conversation_fixture().await;
    let a = agent(&svc, &ws, "agent-retry-a").await;
    let b = agent(&svc, &ws, "agent-retry-b").await;
    let target = addressed(&[b.clone(), a.clone()]);
    sqlx::query("CREATE TRIGGER fail_discussion_handoff BEFORE INSERT ON agent_queue WHEN NEW.agent_id='agent-retry-b' BEGIN SELECT RAISE(ABORT,'handoff unavailable'); END").execute(svc.store.write_pool()).await.unwrap();
    assert!(with_caller(
        human(&people[1].id),
        svc.discussion_send_to_op(&ws, "retry", "durable", &target)
    )
    .await
    .is_err());
    assert_eq!(
        svc.store
            .list_discussion_messages(&ws, None, None, 50)
            .await
            .unwrap()
            .messages
            .len(),
        1
    );
    assert_eq!(svc.queue_snapshot(&a).len(), 1);
    assert!(svc.queue_snapshot(&b).is_empty());
    assert_eq!(
        svc.store
            .discussion_pending_deliveries(None)
            .await
            .unwrap()
            .len(),
        1
    );
    sqlx::query("DROP TRIGGER fail_discussion_handoff")
        .execute(svc.store.write_pool())
        .await
        .unwrap();
    let restored = Services::new(svc.store.clone());
    assert_eq!(restored.rehydrate_agent_queues().await.unwrap(), 2);
    assert_eq!(restored.queue_snapshot(&a).len(), 1);
    assert_eq!(restored.queue_snapshot(&b).len(), 1);
    let canonical = addressed(&[a.clone(), b.clone()]);
    let (left, right) = tokio::join!(
        with_caller(
            human(&people[1].id),
            restored.discussion_send_to_op(&ws, "retry", "durable", &target)
        ),
        with_caller(
            human(&people[1].id),
            restored.discussion_send_to_op(&ws, "retry", "durable", &canonical)
        ),
    );
    assert_eq!(left.unwrap().message, right.unwrap().message);
    assert_eq!(restored.queue_snapshot(&a).len(), 1);
    assert_eq!(restored.queue_snapshot(&b).len(), 1);
    assert!(with_caller(
        human(&people[1].id),
        restored.discussion_send_to_op(
            &ws,
            "retry",
            "durable",
            &addressed(std::slice::from_ref(&a))
        )
    )
    .await
    .is_err());
    // Model a completed/removed ordinary queue. The delivery receipt has been
    // consumed; neither a request retry nor another restart can recreate it.
    restored.agent_queues.lock().unwrap().clear();
    restored.store.replace_agent_queue(&a, &[]).await.unwrap();
    restored.store.replace_agent_queue(&b, &[]).await.unwrap();
    let after = Services::new(restored.store.clone());
    assert_eq!(after.rehydrate_agent_queues().await.unwrap(), 0);
    with_caller(
        human(&people[1].id),
        after.discussion_send_to_op(&ws, "retry", "durable", &target),
    )
    .await
    .unwrap();
    assert!(after
        .store
        .load_all_agent_queues()
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn discussion_agent_handoff_survives_cancelled_request_and_does_not_transfer_authority() {
    let (_tmp, svc, ws, people) = conversation_fixture().await;
    let a = agent(&svc, &ws, "agent-cancel-handoff").await;
    let author = DiscussionAuthor {
        principal_id: Some(people[1].id.clone()),
        agent_id: None,
        login: Some("sender".into()),
        display_name: None,
        avatar_url: None,
    };
    // Crash boundary before dispatch: message, grant and outbox are committed.
    let (sent, _) = svc
        .store
        .send_discussion_message(
            &ws,
            "cancel",
            "committed",
            &author,
            &addressed(std::slice::from_ref(&a)),
        )
        .await
        .unwrap();
    let gate = svc.agent_queue_persist_gate.lock().await;
    {
        let request = svc.dispatch_discussion_deliveries(Some(sent.message.id), false);
        tokio::pin!(request);
        // One poll registers the retained handoff and waits on its join handle.
        tokio::select! {
            biased;
            result = &mut request => panic!("handoff must wait for the gate: {result:?}"),
            () = std::future::ready(()) => {}
        }
        // Dropping the caller future must not cancel the registered handoff.
    }
    drop(gate);
    svc.dispatch_discussion_deliveries(None, false)
        .await
        .unwrap();
    assert_eq!(svc.queue_snapshot(&a).len(), 1);
    assert_eq!(svc.store.load_all_agent_queues().await.unwrap().len(), 1);
    assert!(svc
        .store
        .discussion_pending_deliveries(None)
        .await
        .unwrap()
        .is_empty());
    let archive = svc.store.transfer_export_rows(&ws).await.unwrap();
    assert!(!archive.iter().any(
        |(table, _)| table == "discussion_agent_grant" || table == "discussion_agent_delivery"
    ));
    assert!(
        archive
            .iter()
            .find(|(table, _)| table == "agent_queue")
            .unwrap()
            .1
            .is_empty(),
        "local discussion references must not replay after transfer"
    );
}
