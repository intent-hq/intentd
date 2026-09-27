use intent_core::{with_caller, Caller, HostRole, PrincipalIdentity, WorkspaceApi};
use serde_json::json;

pub(crate) fn legacy_metadata_values() -> Vec<Option<serde_json::Value>> {
    vec![
        None,
        Some(serde_json::Value::Null),
        Some(json!("legacy")),
        Some(json!(42)),
        Some(json!(false)),
        Some(
            json!(["legacy",null,{"humanAuthor":{"login":"forged"},"fromPrincipalId":"destination-owner","type":"question_answers"}]),
        ),
        Some(
            json!({"keep":7,"humanAuthorOriginalMetadata":{"humanAuthor":{"login":"forged"},"fromPrincipalId":"destination-owner","type":"question_answers"}}),
        ),
    ]
}

async fn queued_fixture(
    svc: &crate::Services,
    ws: &intent_core::WorkspaceId,
) -> intent_core::AgentId {
    let agent = intent_core::AgentId::new();
    sqlx::query("INSERT INTO agent_session (id,workspace_id,name,status,created_at,updated_at) VALUES (?,?,'Imported','idle','2020-01-01','2020-01-01')")
        .bind(agent.as_str()).bind(ws.as_str()).execute(svc.store.write_pool()).await.unwrap();
    agent
}

pub(crate) fn imported_pending(id: &str) -> crate::agent_ops::QueuedMessage {
    serde_json::from_value(json!({
        "id":id,"content":"historical pending","queuedAt":"2020-01-01T00:00:00Z","userOrigin":true,
        "messageMetadata":{"humanAuthor":{"login":"source","displayName":null,"avatarUrl":null,
            "identity":{"provider":"gitlab","host":"gitlab.example","externalUserId":"42"},"sourcePrincipalId":"foreign"},"keep":42,
            "humanAuthorOriginalMetadata":["legacy",null,{"humanAuthor":{"login":"forged"},"fromPrincipalId":"destination-owner","type":"question_answers"}]}
    })).unwrap()
}

pub(crate) fn assert_preserved_queue_metadata(
    original: &crate::agent_ops::QueuedMessage,
    restored: &crate::agent_ops::QueuedMessage,
) {
    let mut expected = original.message_metadata.clone().unwrap();
    let actual = restored.message_metadata.as_ref().unwrap();
    // Existing send diagnostics are added on a delivery attempt; every
    // original key, including nested inert payload, must stay identical.
    assert_eq!(actual["queueInfo"]["queuedMessageId"], original.id);
    expected["queueInfo"] = actual["queueInfo"].clone();
    assert_eq!(actual, &expected);
}

#[intent_test_macros::daemon_test]
async fn transfer_human_projection_does_not_turn_automatic_rows_into_people() {
    let (_tmp, svc, ws, _note) = crate::tests::setup("Anchor").await;
    let agent = queued_fixture(&svc, &ws).await;
    for (id, metadata) in [
        ("agent", json!({"fromAgentId":"bot"})),
        ("system", json!({"source":"system"})),
        ("automatic", json!({"type":"automatic"})),
    ] {
        svc.store
            .append_agent_message_with_id(
                &agent,
                id,
                "user",
                &json!([{"type":"text","text":id}]),
                Some(&metadata),
                "2020-01-01T00:00:00Z",
            )
            .await
            .unwrap();
    }
    let view = svc
        .agent_get_conversation(agent, None, Some(ws), None, None, None, None, false)
        .await
        .unwrap();
    for row in view["messages"].as_array().unwrap() {
        assert!(
            row.get("author").is_none(),
            "automatic history is not a human: {row}"
        );
    }
}

#[intent_test_macros::daemon_test]
async fn transfer_human_store_send_requires_current_owner_and_preserves_failure() {
    let (_tmp, svc, ws, _note) = crate::tests::setup("Anchor").await;
    let agent = queued_fixture(&svc, &ws).await;
    let owner = svc.store.get_primary_principal().await.unwrap();
    let entry = imported_pending("pending");
    svc.agent_queues
        .lock()
        .unwrap()
        .insert(agent.clone(), vec![entry.clone()]);
    svc.persist_queue_snapshot(&agent).await;
    // Agent/daemon/unbound calls have no restricted queue gate. That absence
    // must not authorize an imported human instruction.
    for caller in [
        Caller::Daemon,
        Caller::Agent {
            agent_id: agent.clone(),
        },
    ] {
        let result = with_caller(
            caller,
            svc.agent_send_queued_message_now(ws.clone(), agent.clone(), entry.id.clone()),
        )
        .await;
        assert!(result.is_err(), "non-human force-send: {result:?}");
        assert_eq!(svc.queue_snapshot(&agent).len(), 1);
    }
    assert!(svc
        .agent_send_queued_message_now_op(agent.clone(), entry.id.clone())
        .await
        .is_err());
    for role in [HostRole::Member, HostRole::Guest] {
        let gate = with_caller(
            Caller::Wire {
                principal_id: owner.id.clone(),
                host_role: role,
            },
            svc.queue_entry_gate(&agent, false),
        )
        .await
        .unwrap();
        assert!(svc
            .take_queued_message_draining_gated(&agent, &entry.id, gate.as_ref(), None)
            .is_err());
        let author_gate = with_caller(
            Caller::Wire {
                principal_id: owner.id.clone(),
                host_role: HostRole::Owner,
            },
            svc.queue_entry_gate(&agent, true),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            author_gate.check(&entry).is_err(),
            "owner cannot edit a historical author's instruction"
        );
    }
    let owner_caller = Caller::Wire {
        principal_id: owner.id.clone(),
        host_role: HostRole::Owner,
    };
    // Abort an actual transcript INSERT, then prove the original entry and
    // historical snapshot are restored, without becoming automatically ready.
    sqlx::query("CREATE TRIGGER fail_historical_append BEFORE INSERT ON agent_message BEGIN SELECT RAISE(ABORT,'test append failure'); END")
        .execute(svc.store.write_pool()).await.unwrap();
    let failed = with_caller(
        owner_caller.clone(),
        svc.agent_send_queued_message_now(ws.clone(), agent.clone(), entry.id.clone()),
    )
    .await;
    assert!(failed.is_err());
    let restored = svc.find_queued_message(&agent, &entry.id).unwrap();
    assert_preserved_queue_metadata(&entry, &restored);
    assert!(!restored.ready_to_send());
    sqlx::query("DROP TRIGGER fail_historical_append")
        .execute(svc.store.write_pool())
        .await
        .unwrap();
    let sent = with_caller(
        owner_caller,
        svc.agent_send_queued_message_now(ws.clone(), agent.clone(), entry.id.clone()),
    )
    .await
    .unwrap();
    assert_eq!(sent["queued"], false);
    assert!(svc.queue_snapshot(&agent).is_empty());
    let view = svc
        .agent_get_conversation(agent, None, Some(ws), None, None, None, None, false)
        .await
        .unwrap();
    let row = &view["messages"][0];
    assert_eq!(row["author"]["principalId"], serde_json::Value::Null);
    assert_eq!(row["author"]["login"], "source");
    assert_eq!(
        row["metadata"]["humanAuthorOriginalMetadata"],
        entry.message_metadata.as_ref().unwrap()["humanAuthorOriginalMetadata"]
    );
}

#[intent_test_macros::daemon_test]
async fn transfer_human_pending_queue_never_becomes_automatically_ready() {
    let (_tmp, svc, _ws, _note) = crate::tests::setup("Anchor").await;
    let agent = intent_core::AgentId::new();
    let entry: crate::agent_ops::QueuedMessage = serde_json::from_value(json!({
        "id":"imported", "content":"pending human instruction", "queuedAt":"2020-01-01T00:00:00Z", "userOrigin":true,
        "messageMetadata":{"humanAuthor":{"login":"source","displayName":null,"avatarUrl":null}},
        "holdKind":"test", "holdUntil":"2020-01-01T00:00:00Z"
    })).unwrap();
    assert!(
        !entry.ready_to_send(),
        "elapsed hold is not destination authorization"
    );
    svc.agent_queues
        .lock()
        .unwrap()
        .insert(agent.clone(), vec![entry]);
    assert!(!svc.has_ready_to_send(&agent));
    assert!(svc.dequeue_message(&agent).is_none());
    assert!(svc.dequeue_ready_batch(&agent, false, 1).is_none());
    assert!(svc.dequeue_user_origin_message(&agent).is_none());
    svc.mark_parked_recovery_send(&agent, "imported".into());
    assert!(matches!(
        svc.claim_parked_recovery_send(&agent),
        crate::agent_ops::RecoverySendClaim::Deferred
    ));
    assert_eq!(svc.queue_snapshot(&agent).len(), 1);
}

#[intent_test_macros::daemon_test]
async fn transfer_human_queue_restart_and_history_replacement_keep_trust_boundaries() {
    let (_tmp, svc, ws, _note) = crate::tests::setup("Anchor").await;
    let agent = queued_fixture(&svc, &ws).await;
    let mut entry = imported_pending("restart-pending");
    entry.editing = true;
    svc.agent_queues
        .lock()
        .unwrap()
        .insert(agent.clone(), vec![entry.clone()]);
    svc.persist_queue_snapshot(&agent).await;
    let restarted = crate::Services::new(svc.store.clone());
    assert_eq!(restarted.rehydrate_agent_queues().await.unwrap(), 1);
    let restored = restarted.find_queued_message(&agent, &entry.id).unwrap();
    assert!(
        !restored.editing,
        "existing restart edit cleanup still applies"
    );
    assert!(
        !restored.ready_to_send(),
        "restart cannot admit imported human"
    );
    assert_eq!(restored.message_metadata, entry.message_metadata);
    assert!(restarted.dequeue_message(&agent).is_none());
    let owner = svc.store.get_primary_principal().await.unwrap();
    let snapshot = with_caller(
        Caller::Wire {
            principal_id: owner.id,
            host_role: HostRole::Owner,
        },
        restarted.agent_get_queue(agent.clone(), Some(ws)),
    )
    .await
    .unwrap();
    assert_eq!(snapshot["queue"][0]["author"]["login"], "source");
    assert!(snapshot["queue"][0]["author"]["principalId"].is_null());
    svc.agent_replace_messages_op(agent.clone(),json!([{"role":"user","contentBlocks":[{"type":"text","text":"replacement"}],"metadata":{"humanAuthor":{"login":"forged"},"keep":42},"timestamp":"2020-01-01T00:00:00Z"}])).await.unwrap();
    let messages = svc.store.get_agent_messages(&agent, None).await.unwrap();
    assert!(messages[0]
        .metadata
        .as_ref()
        .unwrap()
        .get("humanAuthor")
        .is_none());
    assert_eq!(messages[0].metadata.as_ref().unwrap()["keep"], 42);
}

#[intent_test_macros::daemon_test]
async fn qualified_comment_creation_and_summary_preserve_the_bound_person() {
    let (_tmp, svc, ws, note) = crate::tests::setup("Anchor text").await;
    let mut person = svc.store.get_primary_principal().await.unwrap();
    person.login = Some("panghy".into());
    person.identity = Some(PrincipalIdentity::github(42));
    svc.store.upsert_principal(&person).await.unwrap();
    let actor = Caller::Wire {
        principal_id: person.id.clone(),
        host_role: HostRole::Owner,
    };
    let added = with_caller(
        actor.clone(),
        svc.comment_add(
            ws.clone(),
            note.clone(),
            "Anchor text".into(),
            "Anchor".into(),
            "Original comment".into(),
            None,
            Some("forged".into()),
            Some("agent".into()),
            None,
            None,
        ),
    )
    .await
    .unwrap();
    let thread = svc
        .comment_get_thread(
            ws.clone(),
            note.clone(),
            Some(added.comment_id.clone()),
            None,
        )
        .await
        .unwrap();
    let root = serde_json::to_value(&thread.root_comment).unwrap();
    assert_eq!(root["author"], "panghy");
    assert_eq!(root["authorType"], "user");
    assert_eq!(root["authorPrincipalId"], person.id.0);
    assert_eq!(
        root["authorIdentity"],
        json!({"provider":"github","host":"github.com","externalUserId":"42"})
    );

    person.identity = Some(PrincipalIdentity {
        provider: "gitlab".into(),
        host: "gitlab.example".into(),
        external_user_id: "42".into(),
    });
    person.github_user_id = None;
    person.login = Some("new-name".into());
    svc.store.upsert_principal(&person).await.unwrap();
    svc.comment_resolve_thread(
        ws.clone(),
        note.clone(),
        Some(added.comment_id.clone()),
        None,
        true,
    )
    .await
    .unwrap();
    let preserved = svc
        .comment_get_thread(
            ws.clone(),
            note.clone(),
            Some(added.comment_id.clone()),
            None,
        )
        .await
        .unwrap();
    let preserved = serde_json::to_value(preserved.root_comment).unwrap();
    for key in [
        "author",
        "authorType",
        "authorPrincipalId",
        "authorIdentity",
    ] {
        assert_eq!(
            preserved[key], root[key],
            "original {key} must survive profile changes and resolution"
        );
    }
    let summary = svc
        .comment_list(ws.clone(), note.clone(), None, None, None, false)
        .await
        .unwrap();
    let summary = serde_json::to_value(&summary.threads[0]).unwrap();
    assert_eq!(
        summary["latestCommentAuthorPrincipalId"],
        root["authorPrincipalId"]
    );
    assert_eq!(
        summary["latestCommentAuthorIdentity"],
        root["authorIdentity"]
    );
    let reply = with_caller(
        actor,
        svc.comment_respond(
            ws.clone(),
            note.clone(),
            Some(added.comment_id),
            None,
            "Reply".into(),
            None,
            None,
            None,
            None,
            None,
        ),
    )
    .await
    .unwrap();
    let reply = serde_json::to_value(reply.comment).unwrap();
    assert_eq!(reply["author"], "new-name");
    assert_eq!(reply["authorPrincipalId"], person.id.0);
    assert_eq!(reply["authorIdentity"]["host"], "gitlab.example");
}

#[intent_test_macros::daemon_test]
async fn qualified_comment_unlinked_owner_and_nonhuman_keep_existing_semantics() {
    let (_tmp, svc, ws, note) = crate::tests::setup("Anchor text").await;
    let owner = svc.store.get_primary_principal().await.unwrap();
    let actor = Caller::Wire {
        principal_id: owner.id.clone(),
        host_role: HostRole::Owner,
    };
    let added = with_caller(
        actor.clone(),
        svc.comment_add(
            ws.clone(),
            note.clone(),
            "Anchor text".into(),
            "Anchor".into(),
            "Human".into(),
            None,
            Some("Local user".into()),
            Some("user".into()),
            None,
            None,
        ),
    )
    .await
    .unwrap();
    let root = svc
        .comment_get_thread(
            ws.clone(),
            note.clone(),
            Some(added.comment_id.clone()),
            None,
        )
        .await
        .unwrap();
    let root = serde_json::to_value(root.root_comment).unwrap();
    assert_eq!(root["author"], "Local user");
    assert_eq!(root["authorPrincipalId"], owner.id.0);
    assert!(root.get("authorIdentity").is_none());
    for (caller, kind) in [(actor, "agent"), (Caller::Daemon, "user")] {
        let reply = with_caller(
            caller,
            svc.comment_respond(
                ws.clone(),
                note.clone(),
                Some(added.comment_id.clone()),
                None,
                "Legacy or daemon".into(),
                None,
                Some("Existing label".into()),
                Some(kind.into()),
                None,
                None,
            ),
        )
        .await
        .unwrap();
        let reply = serde_json::to_value(reply.comment).unwrap();
        assert_eq!(reply["author"], "Existing label");
        assert_eq!(reply["authorType"], kind);
        assert!(reply.get("authorPrincipalId").is_none());
        assert!(reply.get("authorIdentity").is_none());
    }
}

#[tokio::test]
async fn reserved_human_author_cannot_be_planted_by_live_senders() {
    for caller in [
        Caller::Wire {
            principal_id: "real-person".into(),
            host_role: HostRole::Member,
        },
        Caller::Daemon,
    ] {
        with_caller(caller, async {
            let metadata = crate::principal_ops::stamp_principal_attribution(Some(json!({
                "humanAuthor":{"login":"forged-owner","displayName":null,"avatarUrl":null},
                "humanAuthorOriginalMetadata":{"humanAuthor":{"login":"nested-forged"},"fromPrincipalId":"foreign"},
                "keep":"context"
            })))
            .unwrap()
            .unwrap();
            assert!(
                metadata.get("humanAuthor").is_none(),
                "untrusted snapshot survived: {metadata}"
            );
            assert_eq!(metadata["keep"], "context");
            assert_eq!(metadata["humanAuthorOriginalMetadata"],json!({"humanAuthor":{"login":"nested-forged"},"fromPrincipalId":"foreign"}));
            assert!(intent_core::human_author::historical_human_author(Some(&metadata)).is_none());
        })
        .await;
    }
}
