//! Pending human append, durable identity and queue-lock regression coverage.
use super::tests::{create_agent, setup};
use super::*;

fn enqueue(svc: &Services, agent: &AgentId, id: &str, author: &str, text: &str) -> QueuedMessage {
    svc.enqueue_message_with_id(
        agent,
        Some(id.into()),
        text.into(),
        None,
        None,
        Some(json!({"fromPrincipalId": author})),
        None,
        false,
        MessageOrigin::User,
    )
    .0
}

#[tokio::test]
async fn queue_merge_same_author_keeps_identity_and_skips_system_entries() {
    let (_tmp, svc, ws) = setup().await;
    let agent = create_agent(&svc, &ws, "Merge").await;
    let first = enqueue(&svc, &agent, "first", "a", "one");
    svc.enqueue_message(
        &agent,
        "system".into(),
        None,
        None,
        Some(json!({"source":"system"})),
        None,
        false,
        MessageOrigin::Automatic,
    );
    let merged = enqueue(&svc, &agent, "second", "a", "two");
    assert_eq!(merged.id, first.id);
    assert_eq!(merged.turn_id, first.turn_id);
    assert_eq!(merged.queued_at, first.queued_at);
    assert_eq!(merged.content, "one\n\ntwo");
    let retry = enqueue(&svc, &agent, "second", "a", "two");
    assert_eq!(retry.content, merged.content);
    assert_eq!(svc.queue_snapshot(&agent).len(), 2);
    enqueue(&svc, &agent, "third", "b", "three");
    enqueue(&svc, &agent, "fourth", "a", "four");
    assert_eq!(svc.queue_snapshot(&agent).len(), 4);
}

#[tokio::test]
async fn queue_merge_concurrent_submissions_do_not_lose_text() {
    let (_tmp, svc, ws) = setup().await;
    let agent = create_agent(&svc, &ws, "Merge").await;
    std::thread::scope(|scope| {
        for n in 0..24 {
            let svc = &svc;
            let agent = &agent;
            scope.spawn(move || enqueue(svc, agent, &format!("id-{n}"), "a", &format!("text-{n}")));
        }
    });
    assert_eq!(svc.queue_snapshot(&agent).len(), 1);
    let entry = svc.dequeue_message(&agent).unwrap();
    let texts: std::collections::HashSet<_> = entry.content.split("\n\n").collect();
    assert_eq!(texts.len(), 24);
    for n in 0..24 {
        assert!(texts.contains(format!("text-{n}").as_str()));
    }
    let fresh = enqueue(&svc, &agent, "after-drain", "a", "new");
    assert_ne!(fresh.id, entry.id);
    assert_eq!(fresh.content, "new");
}

#[tokio::test]
async fn queue_merge_preserves_edit_hold_and_appended_text_on_save() {
    let (_tmp, svc, ws) = setup().await;
    let agent = create_agent(&svc, &ws, "Merge").await;
    let first = enqueue(&svc, &agent, "first", "a", "one");
    svc.agent_edit_queued_message_op(agent.clone(), first.id.clone(), "one".into(), Some(true))
        .await
        .unwrap();
    let merged = enqueue(&svc, &agent, "second", "a", "two");
    assert!(merged.editing);
    assert!(svc.dequeue_message(&agent).is_none());
    let saved = svc
        .agent_edit_queued_message_op(agent.clone(), first.id, "edited".into(), Some(false))
        .await
        .unwrap();
    assert_eq!(saved["queuedMessage"]["content"], "edited\n\ntwo");
    assert_eq!(
        svc.dequeue_message(&agent).unwrap().content,
        "edited\n\ntwo"
    );
}

#[tokio::test]
async fn queue_merge_preserves_metadata_attachments_and_restart_deduplication() {
    let (_tmp, svc, ws) = setup().await;
    let agent = create_agent(&svc, &ws, "Merge").await;
    let first_metadata = json!({"fromPrincipalId":"a","type":"question_answers","answeredQuestionsMessageId":"q1","custom":1});
    let second_metadata = json!({"fromPrincipalId":"a","type":"question_answers","answeredQuestionsMessageId":"q2","custom":2});
    let (first, _) = svc.enqueue_message_with_id(
        &agent,
        Some("first".into()),
        "one".into(),
        Some(json!([{"imageRef":"one"}])),
        Some(json!([{"path":"one"}])),
        Some(first_metadata.clone()),
        None,
        false,
        MessageOrigin::User,
    );
    let (merged, position) = svc.enqueue_message_with_id(
        &agent,
        Some("second".into()),
        "two".into(),
        Some(json!([{"imageRef":"two"}])),
        Some(json!([{"path":"two"}])),
        Some(second_metadata.clone()),
        Some(QueuedPrepend {
            content: Some("preempted".into()),
            image_blocks: None,
            file_blocks: None,
        }),
        true,
        MessageOrigin::User,
    );
    assert_eq!(merged.id, first.id);
    assert_eq!(position, 0);
    assert_eq!(merged.prepend_content.as_deref(), Some("preempted"));
    assert_eq!(
        merged.image_blocks,
        Some(json!([{"imageRef":"one"},{"imageRef":"two"}]))
    );
    assert_eq!(
        merged.file_blocks,
        Some(json!([{"path":"one"},{"path":"two"}]))
    );
    assert_eq!(
        merged.message_metadata.as_ref().unwrap()[MERGED_MESSAGE_METADATA_KEY],
        json!([first_metadata, second_metadata])
    );
    for question in ["q1", "q2"] {
        svc.store
            .append_agent_message_with_id(
                &agent,
                question,
                "assistant",
                &json!([{"type":"text","text":"question"}]),
                None,
                &now_iso(),
            )
            .await
            .unwrap();
        svc.record_pending_questions_marker(&ws, &agent, question)
            .await;
        assert!(
            svc.resolve_pending_questions_for_answer(&ws, &agent, merged.message_metadata.as_ref())
                .await
        );
    }
    svc.persist_queue_snapshot(&agent).await;
    svc.agent_queues.lock().unwrap().clear();
    assert_eq!(svc.rehydrate_agent_queues().await.unwrap(), 1);
    let retry = enqueue(&svc, &agent, "second", "a", "two");
    assert_eq!(retry.content, "one\n\ntwo");
    assert_eq!(retry.message_metadata, merged.message_metadata);
}

#[tokio::test]
async fn queue_merge_unknown_and_delivered_humans_are_barriers() {
    let (_tmp, svc, ws) = setup().await;
    let agent = create_agent(&svc, &ws, "Merge").await;
    enqueue(&svc, &agent, "first", "a", "one");
    svc.enqueue_message(
        &agent,
        "unknown".into(),
        None,
        None,
        None,
        None,
        false,
        MessageOrigin::User,
    );
    let next = enqueue(&svc, &agent, "second", "a", "two");
    assert_eq!(next.id, "second");
    svc.agent_queues
        .lock()
        .unwrap()
        .get_mut(&agent)
        .unwrap()
        .last_mut()
        .unwrap()
        .persisted = true;
    let fresh = enqueue(&svc, &agent, "third", "a", "three");
    assert_eq!(fresh.id, "third");
    assert_eq!(
        svc.find_queued_message(&agent, "second").unwrap().content,
        "two"
    );
}

#[tokio::test]
async fn queue_merge_drain_and_append_race_preserves_each_submission_once() {
    let (_tmp, svc, ws) = setup().await;
    let agent = create_agent(&svc, &ws, "Merge").await;
    for n in 0..32 {
        let first_id = format!("first-{n}");
        let next_id = format!("next-{n}");
        enqueue(&svc, &agent, &first_id, "a", "one");
        let drained = std::thread::scope(|scope| {
            let drain = scope.spawn(|| svc.dequeue_message(&agent).unwrap());
            let append = scope.spawn(|| enqueue(&svc, &agent, &next_id, "a", "two"));
            append.join().unwrap();
            drain.join().unwrap()
        });
        let mut texts = vec![drained.content];
        if let Some(pending) = svc.dequeue_message(&agent) {
            texts.push(pending.content);
        }
        assert_eq!(texts.join("\n\n"), "one\n\ntwo");
    }
}

#[tokio::test]
async fn queue_workspace_owner_can_delete_but_cannot_edit_or_send_foreign_human() {
    use intent_core::{with_caller, Caller, HostRole};
    let (_tmp, svc, ws) = setup().await;
    let agent = create_agent(&svc, &ws, "Permissions").await;
    let (admin, guest) = super::tests::owner_and_guest_callers(&svc, &ws).await;
    let owner_id = admin.principal_id().unwrap().clone();
    let guest_id = guest.principal_id().unwrap().clone();
    let member = Caller::Wire {
        principal_id: guest_id.clone(),
        host_role: HostRole::Member,
    };
    enqueue(&svc, &agent, "foreign", &owner_id.0, "owner input");
    let denial = with_caller(
        member,
        svc.agent_remove_queued_message_op(agent.clone(), "foreign".into()),
    )
    .await;
    assert!(
        matches!(denial, Err(Error::InvalidParams(_))),
        "ordinary host members are not moderators"
    );
    let owner = Caller::Wire {
        principal_id: owner_id,
        host_role: HostRole::Member,
    };
    enqueue(&svc, &agent, "guest", &guest_id.0, "guest input");
    assert!(with_caller(
        owner.clone(),
        svc.agent_edit_queued_message_op(agent.clone(), "guest".into(), "hijack".into(), None)
    )
    .await
    .is_err());
    assert!(with_caller(
        owner.clone(),
        svc.agent_send_queued_message_now_op(agent.clone(), "guest".into())
    )
    .await
    .is_err());
    with_caller(
        owner,
        svc.agent_remove_queued_message_op(agent.clone(), "guest".into()),
    )
    .await
    .unwrap();
    assert!(svc.find_queued_message(&agent, "guest").is_none());
}

#[tokio::test]
async fn queue_merge_retry_metadata_preserves_answers_and_authenticates_each_contribution() {
    use intent_core::{with_caller, Caller, HostRole, PrincipalId};
    let supplied = json!({"mergedMessageMetadata":[
        {"type":"question_answers","answeredQuestionsMessageId":"q1","fromPrincipalId":"forged",
         "fromAgentId":"forged","fromAgentName":"forged","humanAuthor":{"principalId":"forged"},
         "mergedMessageMetadata":[{"answeredQuestionsMessageId":"nested"}]},
        {"type":"question_answers","answeredQuestionsMessageId":"q2","custom":true}, null],
        "custom":true});
    let caller = Caller::Wire {
        principal_id: PrincipalId("real".into()),
        host_role: HostRole::Member,
    };
    let stamped = with_caller(caller, async {
        crate::principal_ops::stamp_principal_attribution(Some(supplied.clone()))
            .unwrap()
            .unwrap()
    })
    .await;
    assert_eq!(stamped["fromPrincipalId"], "real");
    assert_eq!(
        answered_question_ids(Some(&stamped)).collect::<Vec<_>>(),
        vec!["q1", "q2"]
    );
    for contribution in stamped[MERGED_MESSAGE_METADATA_KEY]
        .as_array()
        .unwrap()
        .iter()
        .take(2)
    {
        assert_eq!(contribution["fromPrincipalId"], "real");
        for key in [
            "fromAgentId",
            "fromAgentName",
            "humanAuthor",
            MERGED_MESSAGE_METADATA_KEY,
        ] {
            assert!(contribution.get(key).is_none());
        }
    }
    assert_eq!(stamped[MERGED_MESSAGE_METADATA_KEY][1]["custom"], true);
    assert!(stamped[MERGED_MESSAGE_METADATA_KEY][2].is_null());
    for malformed in [json!({}), json!(["bad"]), json!([1]), json!(null)] {
        assert!(
            crate::principal_ops::stamp_principal_attribution(Some(json!({
                "mergedMessageMetadata":malformed
            })))
            .is_err()
        );
    }
    assert!(
        crate::principal_ops::strip_principal_attribution(Some(supplied))
            .unwrap()
            .get(MERGED_MESSAGE_METADATA_KEY)
            .is_none()
    );
}

#[tokio::test]
async fn queue_merge_edit_preserves_repeated_text_and_hold_acquisition_race() {
    let (_tmp, svc, ws) = setup().await;
    let agent = create_agent(&svc, &ws, "Merge").await;
    enqueue(&svc, &agent, "first", "a", "one\n\ntwo");
    // Append wins before the editor's hold reaches the daemon.
    enqueue(&svc, &agent, "second", "a", "two");
    svc.agent_edit_queued_message_op(
        agent.clone(),
        "first".into(),
        "one\n\ntwo".into(),
        Some(true),
    )
    .await
    .unwrap();
    let saved = svc
        .agent_edit_queued_message_op(
            agent.clone(),
            "first".into(),
            "one\n\ntwo".into(),
            Some(false),
        )
        .await
        .unwrap();
    assert_eq!(saved["queuedMessage"]["content"], "one\n\ntwo\n\ntwo");
    svc.agent_edit_queued_message_op(
        agent.clone(),
        "first".into(),
        "one\n\ntwo\n\ntwo".into(),
        Some(true),
    )
    .await
    .unwrap();
    let merged = enqueue(&svc, &agent, "third", "a", "three");
    let echo = svc
        .agent_edit_queued_message_op(agent, "first".into(), merged.content.clone(), Some(false))
        .await
        .unwrap();
    assert_eq!(echo["queuedMessage"]["content"], merged.content);
}

#[tokio::test]
async fn queue_merge_interrupt_retains_position_and_carryover() {
    let (_tmp, svc, ws) = setup().await;
    let agent = create_agent(&svc, &ws, "Merge").await;
    svc.enqueue_message(
        &agent,
        "system".into(),
        None,
        None,
        Some(json!({"source":"system"})),
        None,
        false,
        MessageOrigin::Automatic,
    );
    let first = enqueue(&svc, &agent, "first", "a", "one");
    let (merged, position) = svc.enqueue_message_with_id(
        &agent,
        Some("interrupt".into()),
        "two".into(),
        None,
        None,
        Some(json!({"fromPrincipalId":"a"})),
        Some(QueuedPrepend {
            content: Some("carryover".into()),
            image_blocks: Some(json!([{"imageRef":"carryover"}])),
            file_blocks: None,
        }),
        true,
        MessageOrigin::User,
    );
    assert_eq!(merged.id, first.id);
    assert_eq!(position, 1);
    assert!(!merged.interrupt_priority);
    assert_eq!(merged.content, "one\n\ntwo");
    assert_eq!(merged.prepend_content.as_deref(), Some("carryover"));
    assert_eq!(
        merged.prepend_image_blocks,
        Some(json!([{"imageRef":"carryover"}]))
    );
}

#[tokio::test]
async fn queue_merge_provisional_handback_coalesces_newer_held_input() {
    for batch in [false, true] {
        let (_tmp, svc, ws) = setup().await;
        let agent = create_agent(&svc, &ws, "Handback").await;
        let first = enqueue(&svc, &agent, "first", "a", "one");
        let (mut popped, draining) = svc.dequeue_message_draining_provisional(&agent).unwrap();
        assert_eq!(enqueue(&svc, &agent, "first", "a", "one").id, first.id);
        assert!(svc
            .agent_queues
            .lock()
            .unwrap()
            .get(&agent)
            .unwrap()
            .is_empty());
        popped.image_blocks = Some(json!([{"imageRef":"first"}]));
        let (newer, _) = svc.enqueue_message_with_id(
            &agent,
            Some("second".into()),
            "two".into(),
            Some(json!([{"imageRef":"second"}])),
            None,
            Some(json!({"fromPrincipalId":"a"})),
            None,
            false,
            MessageOrigin::User,
        );
        svc.mark_parked_recovery_send(&agent, newer.id.clone());
        svc.agent_edit_queued_message_op(agent.clone(), newer.id.clone(), "two".into(), Some(true))
            .await
            .unwrap();
        if batch {
            svc.requeue_front_batch(&agent, vec![popped]);
        } else {
            svc.requeue_front(&agent, popped);
        }
        drop(draining);
        let queue = svc.queue_snapshot(&agent);
        assert_eq!(queue.len(), 1, "undelivered handback must coalesce");
        assert_eq!(queue[0]["id"], first.id);
        assert_eq!(queue[0]["content"], "one\n\ntwo");
        assert_eq!(queue[0]["editing"], true);
        assert_eq!(queue[0]["editingMessageId"], newer.id);
        assert_eq!(
            queue[0]["imageBlocks"],
            json!([{"imageRef":"first"},{"imageRef":"second"}])
        );
        assert!(matches!(
            svc.claim_parked_recovery_send(&agent),
            RecoverySendClaim::Deferred
        ));
        let saved = svc
            .agent_edit_queued_message_op(agent.clone(), newer.id, "edited two".into(), Some(false))
            .await
            .unwrap();
        assert_eq!(saved["queuedMessage"]["content"], "one\n\nedited two");
        assert!(saved["queuedMessage"].get("editingMessageId").is_none());
        assert!(svc
            .agent_edit_queued_message_op(
                agent.clone(),
                "second".into(),
                "stale".into(),
                Some(false)
            )
            .await
            .is_err());
        assert_eq!(
            enqueue(&svc, &agent, "second", "a", "two").content,
            "one\n\nedited two"
        );
        let RecoverySendClaim::Drained(pair) = svc.claim_parked_recovery_send(&agent) else {
            panic!("absorbed recovery id must still authorize the survivor")
        };
        assert_eq!(pair.0.id, first.id);
    }
}

#[tokio::test]
async fn queue_merge_handback_keeps_persisted_and_foreign_author_barriers() {
    for persisted in [false, true] {
        let (_tmp, svc, ws) = setup().await;
        let agent = create_agent(&svc, &ws, "Handback barriers").await;
        enqueue(&svc, &agent, "first", "a", "one");
        let mut popped = svc.dequeue_message(&agent).unwrap();
        popped.persisted = persisted;
        if !persisted {
            enqueue(&svc, &agent, "other", "b", "barrier");
        }
        enqueue(&svc, &agent, "second", "a", "two");
        svc.requeue_front(&agent, popped);
        assert_eq!(
            svc.queue_snapshot(&agent).len(),
            if persisted { 2 } else { 3 }
        );
    }
}

#[tokio::test]
async fn queue_merge_reaffirmed_edit_hold_preserves_appends_on_save_and_cancel() {
    for (draft, reaffirm) in [
        ("one", Some(true)),
        ("edited", Some(true)),
        ("one", None),
        ("edited", None),
    ] {
        let (_tmp, svc, ws) = setup().await;
        let agent = create_agent(&svc, &ws, "Hold").await;
        enqueue(&svc, &agent, "first", "a", "one");
        svc.agent_edit_queued_message_op(agent.clone(), "first".into(), "one".into(), Some(true))
            .await
            .unwrap();
        enqueue(&svc, &agent, "second", "a", "two");
        svc.agent_edit_queued_message_op(agent.clone(), "first".into(), "one".into(), reaffirm)
            .await
            .unwrap();
        let saved = svc
            .agent_edit_queued_message_op(agent, "first".into(), draft.into(), Some(false))
            .await
            .unwrap();
        assert_eq!(saved["queuedMessage"]["content"], format!("{draft}\n\ntwo"));
    }
}

#[tokio::test]
async fn queue_merge_uses_arrival_order_across_priority_handback_and_restart() {
    for handback in [false, true] {
        let (_tmp, svc, ws) = setup().await;
        let agent = create_agent(&svc, &ws, "Arrival").await;
        enqueue(&svc, &agent, "a1", "a", "one");
        let popped = handback.then(|| svc.dequeue_message(&agent).unwrap());
        svc.enqueue_message_with_id(
            &agent,
            Some("b1".into()),
            "barrier".into(),
            None,
            None,
            Some(json!({"fromPrincipalId":"b","source":"system"})),
            None,
            true,
            MessageOrigin::User,
        );
        enqueue(&svc, &agent, "a2", "a", "two");
        if let Some(popped) = popped {
            svc.requeue_front(&agent, popped);
        }
        assert_eq!(svc.queue_snapshot(&agent).len(), 3);
        svc.persist_queue_snapshot(&agent).await;
        svc.agent_queues.lock().unwrap().clear();
        svc.rehydrate_agent_queues().await.unwrap();
        let latest = enqueue(&svc, &agent, "a3", "a", "three");
        assert_eq!(latest.id, "a2");
        assert_eq!(latest.content, "two\n\nthree");
        assert_eq!(svc.queue_snapshot(&agent).len(), 3);
    }
}

#[tokio::test]
async fn queue_merge_authenticated_authors_override_custom_metadata_labels() {
    let (_tmp, svc, ws) = setup().await;
    let agent = create_agent(&svc, &ws, "Identity").await;
    enqueue(&svc, &agent, "first", "a", "one");
    let (merged, _) = svc.enqueue_message_with_id(
        &agent,
        Some("second".into()),
        "two".into(),
        None,
        None,
        Some(json!({"fromPrincipalId":"a","type":"custom"})),
        None,
        false,
        MessageOrigin::User,
    );
    assert_eq!(merged.id, "first");
    assert_eq!(merged.content, "one\n\ntwo");
    let automatic = crate::principal_ops::strip_principal_attribution(Some(
        json!({"fromPrincipalId":"a","source":"system"}),
    ));
    svc.enqueue_message(
        &agent,
        "automatic".into(),
        None,
        None,
        automatic,
        None,
        false,
        MessageOrigin::Automatic,
    );
    assert_eq!(
        enqueue(&svc, &agent, "third", "a", "three").content,
        "one\n\ntwo\n\nthree"
    );
    assert_eq!(svc.queue_snapshot(&agent).len(), 2);
}

#[tokio::test]
async fn queue_merge_provisional_foreign_human_remains_a_barrier_until_committed() {
    for commit in [false, true] {
        let (_tmp, svc, ws) = setup().await;
        let agent = create_agent(&svc, &ws, "Provisional barrier").await;
        enqueue(&svc, &agent, "a1", "a", "one");
        enqueue(&svc, &agent, "b2", "b", "barrier");
        let (popped, draining) = svc
            .take_queued_message_draining_gated(&agent, "b2", None, None)
            .unwrap()
            .unwrap();
        if commit {
            svc.commit_provisional_queue_delivery(&agent, std::slice::from_ref(&popped));
            svc.commit_queue_history(&agent, &popped.id);
        }
        let appended = enqueue(&svc, &agent, "a3", "a", "three");
        if commit {
            assert_eq!(appended.id, "a1");
            assert_eq!(appended.content, "one\n\nthree");
        } else {
            assert_eq!(appended.id, "a3");
            svc.requeue_front(&agent, popped);
            assert_eq!(
                svc.find_queued_message(&agent, "a1").unwrap().content,
                "one"
            );
        }
        drop(draining);
        assert_eq!(svc.queue_snapshot(&agent).len(), if commit { 1 } else { 3 });
    }
}

#[tokio::test]
async fn queue_merge_two_held_sources_rejects_displaced_draft_without_changing_survivor() {
    let (_tmp, svc, ws) = setup().await;
    let agent = create_agent(&svc, &ws, "Held conflict").await;
    enqueue(&svc, &agent, "first", "a", "one");
    svc.agent_edit_queued_message_op(agent.clone(), "first".into(), "one".into(), Some(true))
        .await
        .unwrap();
    let (popped, draining) = svc
        .take_queued_message_draining_gated(&agent, "first", None, None)
        .unwrap()
        .unwrap();
    enqueue(&svc, &agent, "second", "a", "two");
    svc.agent_edit_queued_message_op(agent.clone(), "second".into(), "two".into(), Some(true))
        .await
        .unwrap();
    svc.requeue_front(&agent, popped);
    drop(draining);
    let before = svc.queue_snapshot(&agent);
    assert_eq!(before.len(), 1);
    assert_eq!(before[0]["editingMessageId"], "first");
    for editing in [Some(true), Some(false), None] {
        let error = svc
            .agent_edit_queued_message_op(
                agent.clone(),
                "second".into(),
                "unsaved second draft".into(),
                editing,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(error, Error::InvalidParams(ref message) if message.starts_with("queued edit conflict:"))
        );
        assert_eq!(svc.queue_snapshot(&agent), before);
    }
    let saved = svc
        .agent_edit_queued_message_op(agent, "first".into(), "edited first".into(), Some(false))
        .await
        .unwrap();
    assert_eq!(saved["queuedMessage"]["content"], "edited first\n\ntwo");
    assert!(saved["queuedMessage"].get("editingMessageId").is_none());
}

#[tokio::test]
async fn queue_merge_migrated_edit_alias_is_author_gated_and_expires_on_release() {
    use intent_core::with_caller;
    let (_tmp, svc, ws) = setup().await;
    let agent = create_agent(&svc, &ws, "Alias permissions").await;
    let (owner, author) = super::tests::owner_and_guest_callers(&svc, &ws).await;
    let principal = &author.principal_id().unwrap().0;
    enqueue(&svc, &agent, "first", principal, "one");
    let popped = svc.dequeue_message(&agent).unwrap();
    enqueue(&svc, &agent, "second", principal, "two");
    with_caller(
        author.clone(),
        svc.agent_edit_queued_message_op(agent.clone(), "second".into(), "two".into(), Some(true)),
    )
    .await
    .unwrap();
    svc.requeue_front(&agent, popped);
    assert!(with_caller(
        owner,
        svc.agent_edit_queued_message_op(
            agent.clone(),
            "second".into(),
            "hijack".into(),
            Some(false)
        )
    )
    .await
    .is_err());
    let saved = with_caller(
        author.clone(),
        svc.agent_edit_queued_message_op(
            agent.clone(),
            "second".into(),
            "edited".into(),
            Some(false),
        ),
    )
    .await
    .unwrap();
    assert!(saved["queuedMessage"]["content"]
        .as_str()
        .unwrap()
        .ends_with("edited"));
    assert!(with_caller(
        author,
        svc.agent_edit_queued_message_op(
            agent.clone(),
            "second".into(),
            "stale".into(),
            Some(false)
        )
    )
    .await
    .is_err());
    assert_eq!(
        svc.queue_snapshot(&agent)[0]["content"],
        saved["queuedMessage"]["content"]
    );
}

#[tokio::test]
async fn queue_merge_restored_alias_is_not_duplicated_by_frozen_draining_overlay() {
    let (_tmp, svc, ws) = setup().await;
    let agent = create_agent(&svc, &ws, "Frozen alias").await;
    enqueue(&svc, &agent, "a1", "a", "one");
    enqueue(&svc, &agent, "b", "b", "barrier");
    enqueue(&svc, &agent, "a2", "a", "two");
    svc.agent_remove_queued_message_op(agent.clone(), "b".into())
        .await
        .unwrap();
    let (popped, guard) = svc
        .take_queued_message_draining_gated(&agent, "a2", None, None)
        .unwrap()
        .unwrap();
    svc.requeue_front(&agent, popped);
    let snapshot = svc.queue_snapshot(&agent);
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0]["id"], "a1");
    assert_eq!(snapshot[0]["content"], "one\n\ntwo");
    svc.freeze_shutdown_drains();
    svc.persist_shutdown_drains().await;
    drop(guard);
    let restarted = Services::new(svc.store.clone());
    restarted.rehydrate_agent_queues().await.unwrap();
    let rows = restarted.queue_snapshot(&agent);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], "a1");
    assert_eq!(rows[0]["content"], "one\n\ntwo");
}

#[tokio::test]
async fn queue_merge_migrated_hold_updates_keep_prefix_and_suffix_until_release() {
    for reaffirm in [Some(true), None] {
        let (_tmp, svc, ws) = setup().await;
        let agent = create_agent(&svc, &ws, "Migrated hold").await;
        enqueue(&svc, &agent, "a1", "a", "one");
        let (popped, guard) = svc.dequeue_message_draining_provisional(&agent).unwrap();
        enqueue(&svc, &agent, "a2", "a", "two");
        svc.agent_edit_queued_message_op(agent.clone(), "a2".into(), "two".into(), Some(true))
            .await
            .unwrap();
        svc.requeue_front(&agent, popped);
        drop(guard);
        enqueue(&svc, &agent, "a3", "a", "three");
        svc.agent_edit_queued_message_op(agent.clone(), "a2".into(), "interim".into(), reaffirm)
            .await
            .unwrap();
        let saved = svc
            .agent_edit_queued_message_op(agent, "a2".into(), "final".into(), Some(false))
            .await
            .unwrap();
        assert_eq!(saved["queuedMessage"]["content"], "one\n\nfinal\n\nthree");
    }
}
