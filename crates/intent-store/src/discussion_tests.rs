use super::*;
use intent_core::discussion::DiscussionAuthor;
use intent_core::PrincipalId;

#[tokio::test]
async fn discussion_upgrade_from_main_preserves_data_and_migration_checksums() {
    assert_discussion_upgrade_preserves_data_and_migration_checksums(172).await;
}

#[tokio::test]
async fn discussion_upgrade_from_pre_discussions_preserves_data_and_migration_checksums() {
    assert_discussion_upgrade_preserves_data_and_migration_checksums(164).await;
}

async fn assert_discussion_upgrade_preserves_data_and_migration_checksums(
    last_applied_version: i64,
) {
    let tmp = TempDb::new();
    let pool = crate::connect_write(&tmp.path).await.unwrap();
    let prefix = sqlx::migrate::Migrator {
        migrations: std::borrow::Cow::Owned(
            crate::MIGRATOR
                .iter()
                .filter(|m| m.version <= last_applied_version)
                .cloned()
                .collect(),
        ),
        ..sqlx::migrate::Migrator::DEFAULT
    };
    prefix.run(&pool).await.unwrap();
    sqlx::raw_sql(
        "INSERT INTO principal(id,created_at,updated_at) VALUES('existing-guest','original','original');
         INSERT INTO workspace(id,title,branch,status,created_at,updated_at,repository_path,repository_owner,repository_name)
         VALUES('existing-workspace','Existing workspace','main','Active','original','original','/owned/repository','Owner','Project');
         INSERT INTO workspace_member VALUES('existing-workspace','existing-guest','collaborator','original');",
    )
    .execute(&pool)
    .await
    .unwrap();
    let applied: Vec<(i64, Vec<u8>)> =
        sqlx::query_as("SELECT version, checksum FROM _sqlx_migrations ORDER BY version")
            .fetch_all(&pool)
            .await
            .unwrap();
    pool.close().await;

    let store = Store::open(&tmp.path).await.unwrap();
    let preserved: Vec<(i64, Vec<u8>)> = sqlx::query_as(
        "SELECT version, checksum FROM _sqlx_migrations WHERE version <= ? ORDER BY version",
    )
    .bind(last_applied_version)
    .fetch_all(store.read_pool())
    .await
    .unwrap();
    assert_eq!(preserved, applied);
    let ws = WorkspaceId::from("existing-workspace");
    let guest = PrincipalId::from("existing-guest");
    assert_eq!(
        store.get_workspace(&ws).await.unwrap().title,
        "Existing workspace"
    );
    assert_eq!(
        store.get_workspace_member_role(&ws, &guest).await.unwrap(),
        Some(WorkspaceRole::Collaborator)
    );
    let owner = store.get_primary_principal().await.unwrap();
    assert!(store
        .set_workspace_guest_role(&ws, &guest, WorkspaceRole::Manager, Some(&owner.id))
        .await
        .unwrap());
    let author = DiscussionAuthor {
        agent_id: None,
        principal_id: Some(guest),
        login: None,
        display_name: None,
        avatar_url: None,
    };
    let (message, inserted) = store
        .insert_discussion_message(&ws, "after-upgrade", "Still here", &author)
        .await
        .unwrap();
    assert!(inserted);
    store.close().await;
    let store = Store::open(&tmp.path).await.unwrap();
    let page = store
        .list_discussion_messages(&ws, None, None, 100)
        .await
        .unwrap();
    assert_eq!(page.messages, vec![message]);
}

#[tokio::test]
async fn discussion_atomic_retries_page_boundaries_and_reopen() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = WorkspaceId::new();
    let other = WorkspaceId::new();
    for id in [&ws, &other] {
        store
            .insert_workspace(&sample_workspace(id, "Discussion", false))
            .await
            .unwrap();
    }
    let author = DiscussionAuthor {
        agent_id: None,
        principal_id: Some(store.get_primary_principal().await.unwrap().id),
        login: Some("original".into()),
        display_name: None,
        avatar_url: None,
    };
    let (a, b) = tokio::join!(
        store.insert_discussion_message(&ws, "retry", "original body", &author),
        store.insert_discussion_message(&ws, "retry", "original body", &author),
    );
    let (first, inserted_a) = a.unwrap();
    let (retry, inserted_b) = b.unwrap();
    assert_ne!(inserted_a, inserted_b);
    assert_eq!(first, retry);
    assert_eq!(first.sequence, 1);
    let mut renamed = author.clone();
    renamed.login = Some("renamed".into());
    let (retry, inserted) = store
        .insert_discussion_message(&ws, "retry", "original body", &renamed)
        .await
        .unwrap();
    assert!(!inserted);
    assert_eq!(retry, first);
    assert!(matches!(
        store
            .insert_discussion_message(&ws, "retry", "changed body", &renamed)
            .await,
        Err(Error::InvalidParams(_))
    ));
    let mut second_person = store.get_primary_principal().await.unwrap();
    second_person.id = PrincipalId::new();
    second_person.is_primary = false;
    store.upsert_principal(&second_person).await.unwrap();
    store
        .add_workspace_member(
            &ws,
            &second_person.id,
            intent_core::WorkspaceRole::Collaborator,
        )
        .await
        .unwrap();
    renamed.principal_id = Some(second_person.id);
    let (second, _) = store
        .insert_discussion_message(&ws, "retry", "other human", &renamed)
        .await
        .unwrap();
    assert_ne!(second.id, first.id);
    assert_eq!(second.sequence, 2);
    let (third, _) = store
        .insert_discussion_message(&ws, "third", "newest", &author)
        .await
        .unwrap();
    let (isolated, _) = store
        .insert_discussion_message(&other, "retry", "private", &author)
        .await
        .unwrap();
    assert_eq!(isolated.sequence, 1);
    drop(store);
    let store = Store::open(&tmp.path).await.unwrap();
    let latest = store
        .list_discussion_messages(&ws, None, None, 2)
        .await
        .unwrap();
    assert!(latest.has_more);
    assert_eq!(latest.messages, vec![second, third]);
    let earlier = store
        .list_discussion_messages(&ws, Some(2), None, 2)
        .await
        .unwrap();
    assert!(!earlier.has_more);
    assert_eq!(earlier.messages, vec![first]);
    assert!(store
        .list_discussion_messages(&ws, Some(1), None, 2)
        .await
        .unwrap()
        .messages
        .is_empty());
    for (before, limit) in [(None, 0), (None, 101), (Some(0), 2)] {
        assert!(store
            .list_discussion_messages(&ws, before, None, limit)
            .await
            .is_err());
    }
    store.delete_workspace(&ws).await.unwrap();
    assert!(store
        .list_discussion_messages(&ws, None, None, 100)
        .await
        .unwrap()
        .messages
        .is_empty());
    assert_eq!(
        store
            .list_discussion_messages(&other, None, None, 100)
            .await
            .unwrap()
            .messages,
        vec![isolated]
    );
}

#[tokio::test]
async fn discussion_forward_pages_follow_sequences_and_skip_gaps() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "Discussion", false))
        .await
        .unwrap();
    let author = DiscussionAuthor {
        agent_id: None,
        principal_id: Some(store.get_primary_principal().await.unwrap().id),
        login: None,
        display_name: None,
        avatar_url: None,
    };
    for i in 1..=5 {
        store
            .insert_discussion_message(&ws, &i.to_string(), "body", &author)
            .await
            .unwrap();
    }
    sqlx::query(
        "DELETE FROM workspace_discussion_message WHERE workspace_id=? AND sequence IN (2,4)",
    )
    .bind(ws.as_str())
    .execute(store.write_pool())
    .await
    .unwrap();
    for (cursor, expected, more) in [
        (0, vec![1, 3], true),
        (1, vec![3, 5], false),
        (2, vec![3, 5], false),
        (3, vec![5], false),
        (5, vec![], false),
        (i64::MAX, vec![], false),
    ] {
        let page = store
            .list_discussion_messages(&ws, None, Some(cursor), 2)
            .await
            .unwrap();
        assert_eq!(
            page.messages.iter().map(|m| m.sequence).collect::<Vec<_>>(),
            expected
        );
        assert_eq!(page.has_more, more);
    }
    for (before, after, limit) in [
        (None, Some(-1), 2),
        (Some(3), Some(0), 2),
        (None, Some(0), 0),
        (None, Some(0), 101),
    ] {
        assert!(matches!(
            store
                .list_discussion_messages(&ws, before, after, limit)
                .await,
            Err(Error::InvalidParams(_))
        ));
    }
}

#[tokio::test]
async fn discussion_concurrent_mismatched_retries_insert_exactly_once() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "Discussion", false))
        .await
        .unwrap();
    let author = DiscussionAuthor {
        agent_id: None,
        principal_id: Some(store.get_primary_principal().await.unwrap().id),
        login: None,
        display_name: None,
        avatar_url: None,
    };
    let (a, b) = tokio::join!(
        store.insert_discussion_message(&ws, "same", "first", &author),
        store.insert_discussion_message(&ws, "same", "second", &author),
    );
    let (success, error) = if a.is_ok() { (a, b) } else { (b, a) };
    let (message, inserted) = success.unwrap();
    assert!(inserted);
    assert!(matches!(error, Err(Error::InvalidParams(_))));
    let page = store
        .list_discussion_messages(&ws, None, None, 100)
        .await
        .unwrap();
    assert_eq!(page.messages, vec![message]);
    let (next, _) = store
        .insert_discussion_message(&ws, "next", "next", &author)
        .await
        .unwrap();
    assert_eq!(next.sequence, 2);
}

#[tokio::test]
async fn discussion_conversation_migration_preserves_default_history_and_retry() {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "Existing discussion", false))
        .await
        .unwrap();
    sqlx::raw_sql("DROP TABLE discussion_agent_activity; DROP TABLE discussion_agent_delivery; DROP TABLE discussion_agent_grant; DROP TABLE discussion_reaction; DROP TABLE workspace_discussion_message; DROP TABLE discussion_participant; DROP TABLE discussion_conversation; DROP TABLE workspace_discussion_conversation;")
        .execute(store.write_pool()).await.unwrap();
    sqlx::raw_sql(include_str!("../migrations/0165_workspace_discussions.sql"))
        .execute(store.write_pool())
        .await
        .unwrap();
    let author = DiscussionAuthor {
        agent_id: None,
        principal_id: Some(store.get_primary_principal().await.unwrap().id),
        login: Some("original".into()),
        display_name: None,
        avatar_url: None,
    };
    sqlx::query("INSERT INTO workspace_discussion_message(id,workspace_id,sequence,principal_id,client_message_id,body,author_json,created_at) VALUES('old-message',?,7,?,'retry','preserved',?,'2026-01-01T00:00:00Z')")
        .bind(ws.as_str()).bind(author.principal_id.as_ref().unwrap().as_str()).bind(serde_json::to_string(&author).unwrap()).execute(store.write_pool()).await.unwrap();
    sqlx::raw_sql(include_str!(
        "../migrations/0166_discussion_conversations.sql"
    ))
    .execute(store.write_pool())
    .await
    .unwrap();
    sqlx::raw_sql(include_str!(
        "../migrations/0167_discussion_message_actions.sql"
    ))
    .execute(store.write_pool())
    .await
    .unwrap();
    sqlx::raw_sql(include_str!("../migrations/0168_discussion_agents.sql"))
        .execute(store.write_pool())
        .await
        .unwrap();
    sqlx::query("UPDATE workspace_discussion_message SET recipient_agent_ids_json='[\"historical-agent\"]' WHERE id='old-message'").execute(store.write_pool()).await.unwrap();
    sqlx::raw_sql(include_str!("../migrations/0169_discussion_activity.sql"))
        .execute(store.write_pool())
        .await
        .unwrap();
    sqlx::query("INSERT INTO discussion_reaction(id,workspace_id,message_id,principal_id,reaction) VALUES('old-reaction',?,'old-message',?,'🙂')")
        .bind(ws.as_str()).bind(author.principal_id.as_ref().unwrap().as_str()).execute(store.write_pool()).await.unwrap();
    sqlx::query("UPDATE workspace_discussion_message SET reactions_json='[{\"reaction\":\"🙂\",\"count\":1,\"reacted\":false}]',reaction_version=2 WHERE id='old-message'").execute(store.write_pool()).await.unwrap();
    sqlx::raw_sql(include_str!("../migrations/0170_discussion_reactors.sql"))
        .execute(store.write_pool())
        .await
        .unwrap();
    sqlx::raw_sql(include_str!(
        "../migrations/0173_discussion_principal_mentions.sql"
    ))
    .execute(store.write_pool())
    .await
    .unwrap();
    sqlx::raw_sql(include_str!(
        "../migrations/0174_workspace_discussion_management.sql"
    ))
    .execute(store.write_pool())
    .await
    .unwrap();
    let page = store
        .list_discussion_messages(&ws, None, None, 50)
        .await
        .unwrap();
    assert_eq!(
        page.messages[0].reactions[0].reactors[0].principal_id,
        author.principal_id
    );
    let principal = author.principal_id.as_ref().unwrap();
    let (same, changed) = store
        .set_discussion_reaction(&ws, "default", "old-message", principal, "🙂", true)
        .await
        .unwrap();
    assert!(!changed);
    assert_eq!(same.reaction_version, 2);
    assert_eq!(page.messages.len(), 1);
    let old = &page.messages[0];
    assert_eq!(old.id, "old-message");
    assert!(old.principal_mentions.is_empty());
    assert!(
        old.agent_activity.is_empty(),
        "migration must not invent activity for delivered historical work"
    );
    assert_eq!(old.conversation_id, "default");
    assert_eq!(old.sequence, 7);
    assert_eq!(old.author, author);
    let (retry, inserted) = store
        .send_discussion_message(
            &ws,
            "retry",
            "preserved",
            &author,
            &intent_core::discussion::DiscussionTarget {
                recipient_agent_ids: vec![intent_core::AgentId::from("historical-agent")],
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(!inserted);
    assert_eq!(&retry.message, old);
    let (removed, changed) = store
        .set_discussion_reaction(&ws, "default", "old-message", principal, "🙂", false)
        .await
        .unwrap();
    assert!(changed);
    assert!(removed.reactions.is_empty());
    assert_eq!(removed.reaction_version, 3);

    let (next, _) = store
        .insert_discussion_message(&ws, "next", "next", &author)
        .await
        .unwrap();
    assert_eq!(next.sequence, 8);
}

#[tokio::test]
async fn discussion_sender_removal_wins_while_append_or_retry_waits_for_writer() {
    use intent_core::discussion::DiscussionTarget;
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "Writer race", false))
        .await
        .unwrap();
    let owner = store.get_primary_principal().await.unwrap();
    let mut person = owner.clone();
    person.id = PrincipalId::new();
    person.is_primary = false;
    store.upsert_principal(&person).await.unwrap();
    store
        .add_workspace_member(&ws, &person.id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    let author = DiscussionAuthor {
        agent_id: None,
        principal_id: Some(person.id.clone()),
        login: None,
        display_name: None,
        avatar_url: None,
    };
    let create = DiscussionTarget {
        conversation_id: None,
        recipient_principal_ids: Some(vec![owner.id]),
        ..DiscussionTarget::default()
    };
    let (private, _) = store
        .send_discussion_message(&ws, "private", "body", &author, &create)
        .await
        .unwrap();
    store
        .insert_discussion_message(&ws, "public", "body", &author)
        .await
        .unwrap();
    for (key, target) in [
        ("public", DiscussionTarget::default()),
        ("new-public", DiscussionTarget::default()),
        ("private", create),
        (
            "new-private",
            DiscussionTarget {
                conversation_id: Some(private.conversation.id.clone()),
                recipient_principal_ids: None,
                ..Default::default()
            },
        ),
    ] {
        store
            .add_workspace_member(&ws, &person.id, WorkspaceRole::Collaborator)
            .await
            .unwrap();
        assert!(store
            .get_workspace_member_role(&ws, &person.id)
            .await
            .unwrap()
            .is_some());
        let mut removal = store.write_pool().begin().await.unwrap();
        let pending = store.send_discussion_message(&ws, key, "body", &author, &target);
        tokio::pin!(pending);
        tokio::select! {
            biased;
            result=&mut pending => panic!("send unexpectedly passed held writer: {result:?}"),
            ()=std::future::ready(()) => {}
        }
        sqlx::query("DELETE FROM workspace_member WHERE workspace_id=? AND principal_id=?")
            .bind(ws.as_str())
            .bind(person.id.as_str())
            .execute(&mut *removal)
            .await
            .unwrap();
        removal.commit().await.unwrap();
        assert!(matches!(pending.await, Err(Error::NotFound(_))));
    }
    for conversation in ["default", private.conversation.id.as_str()] {
        store
            .add_workspace_member(&ws, &person.id, WorkspaceRole::Collaborator)
            .await
            .unwrap();
        let page = store
            .list_discussion_messages_in(&ws, conversation, None, None, 1)
            .await
            .unwrap();
        let message = &page.messages[0].id;
        let mut removal = store.write_pool().begin().await.unwrap();
        let pending =
            store.set_discussion_reaction(&ws, conversation, message, &person.id, "👍", true);
        tokio::pin!(pending);
        tokio::select! { biased; result=&mut pending => panic!("reaction bypassed writer: {result:?}"), ()=std::future::ready(())=>{} }
        sqlx::query("DELETE FROM workspace_member WHERE workspace_id=? AND principal_id=?")
            .bind(ws.as_str())
            .bind(person.id.as_str())
            .execute(&mut *removal)
            .await
            .unwrap();
        removal.commit().await.unwrap();
        assert!(matches!(pending.await, Err(Error::NotFound(_))));
    }
    assert_eq!(
        store
            .list_discussion_messages(&ws, None, None, 50)
            .await
            .unwrap()
            .messages
            .len(),
        1
    );
    assert_eq!(
        store
            .list_discussion_messages_in(&ws, &private.conversation.id, None, None, 50)
            .await
            .unwrap()
            .messages
            .len(),
        1
    );
}

#[tokio::test]
async fn discussion_agent_grants_and_outbox_rollback_with_message_and_identity_is_distinct() {
    use intent_core::discussion::DiscussionTarget;
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "Agents", false))
        .await
        .unwrap();
    let owner = store.get_primary_principal().await.unwrap();
    // Even equal string IDs belong to different identity domains.
    let agent = AgentId::from(owner.id.as_str());
    store
        .insert_agent_session(&sample_agent_session(&agent, &ws))
        .await
        .unwrap();
    let human = DiscussionAuthor {
        principal_id: Some(owner.id),
        agent_id: None,
        login: None,
        display_name: None,
        avatar_url: None,
    };
    let target = DiscussionTarget {
        recipient_agent_ids: vec![agent.clone()],
        ..Default::default()
    };
    sqlx::query("CREATE TRIGGER fail_discussion_outbox BEFORE INSERT ON discussion_agent_delivery BEGIN SELECT RAISE(ABORT,'outbox unavailable'); END").execute(store.write_pool()).await.unwrap();
    assert!(store
        .send_discussion_message(&ws, "shared-key", "human", &human, &target)
        .await
        .is_err());
    assert!(!store
        .discussion_agent_access(&ws, "default", &agent)
        .await
        .unwrap());
    assert!(store
        .list_discussion_messages(&ws, None, None, 50)
        .await
        .unwrap()
        .messages
        .is_empty());
    sqlx::query("DROP TRIGGER fail_discussion_outbox")
        .execute(store.write_pool())
        .await
        .unwrap();
    let (original, _) = store
        .send_discussion_message(&ws, "shared-key", "human", &human, &target)
        .await
        .unwrap();
    let author = DiscussionAuthor {
        principal_id: None,
        agent_id: Some(agent.clone()),
        login: None,
        display_name: Some("Agent".into()),
        avatar_url: None,
    };
    let (reply, inserted) = store
        .send_discussion_message(
            &ws,
            "shared-key",
            "agent",
            &author,
            &DiscussionTarget::default(),
        )
        .await
        .unwrap();
    assert!(inserted);
    assert_ne!(reply.message.id, original.message.id);
    drop(store);
    let reopened = Store::open(&tmp.path).await.unwrap();
    assert!(reopened
        .discussion_agent_access(&ws, "default", &agent)
        .await
        .unwrap());
    assert_eq!(
        reopened
            .discussion_pending_deliveries(None)
            .await
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn discussion_reactor_samples_are_bounded_immutable_and_refilled() {
    use intent_core::discussion::DiscussionActor;
    use intent_core::AgentId;
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.unwrap();
    let ws = WorkspaceId::new();
    store
        .insert_workspace(&sample_workspace(&ws, "Reactions", false))
        .await
        .unwrap();
    let primary = store.get_primary_principal().await.unwrap();
    let author = DiscussionAuthor {
        principal_id: Some(primary.id.clone()),
        agent_id: None,
        login: primary.login,
        display_name: primary.display_name,
        avatar_url: primary.avatar_url,
    };
    let (message, _) = store
        .insert_discussion_message(&ws, "target", "body", &author)
        .await
        .unwrap();
    let mut actors = vec![];
    let mut expected = vec![];
    for n in 0..22 {
        let id = AgentId::from(format!("reactor-{n}"));
        sqlx::query("INSERT INTO agent_session(id,workspace_id,name,status,created_at,updated_at) VALUES(?,?,?,'idle','now','now')")
            .bind(id.as_str()).bind(ws.as_str()).bind(format!("Helper {n}")).execute(store.write_pool()).await.unwrap();
        sqlx::query("INSERT INTO discussion_agent_grant(workspace_id,conversation_id,agent_id) VALUES(?,'default',?)")
            .bind(ws.as_str()).bind(id.as_str()).execute(store.write_pool()).await.unwrap();
        let actor = DiscussionActor::Agent(id.clone());
        let (state, changed) = store
            .set_discussion_actor_reaction(&ws, "default", &message.id, &actor, "✅", true)
            .await
            .unwrap();
        assert!(changed);
        assert_eq!(state.reaction_version, n + 1);
        assert_eq!(state.reactions[0].count, n + 1);
        assert_eq!(
            state.reactions[0].reactors.len(),
            usize::try_from(n + 1).unwrap().min(20)
        );
        if n < 20 {
            expected.push(id);
        }
        assert_eq!(
            state.reactions[0]
                .reactors
                .iter()
                .map(|r| r.agent_id.clone().unwrap())
                .collect::<Vec<_>>(),
            expected
        );
        actors.push(actor);
    }
    sqlx::query("UPDATE agent_session SET name='Renamed' WHERE id='reactor-0'")
        .execute(store.write_pool())
        .await
        .unwrap();
    let (same, changed) = store
        .set_discussion_actor_reaction(&ws, "default", &message.id, &actors[0], "✅", true)
        .await
        .unwrap();
    assert!(!changed);
    assert_eq!(same.reaction_version, 22);
    assert_eq!(
        same.reactions[0].reactors[0].display_name.as_deref(),
        Some("Helper 0")
    );
    let (removed, changed) = store
        .set_discussion_actor_reaction(&ws, "default", &message.id, &actors[0], "✅", false)
        .await
        .unwrap();
    assert!(changed);
    assert_eq!(removed.reactions[0].count, 21);
    assert_eq!(removed.reactions[0].reactors.len(), 20);
    assert_eq!(
        removed.reactions[0].reactors[19].agent_id,
        Some(AgentId::from("reactor-20"))
    );
    assert!(!removed.reactions[0].reacted);
    // Exercise every current choice and verify fixed aggregate order, not insertion order.
    for emoji in ["🙈", "👀", "👎", "👍"] {
        store
            .set_discussion_actor_reaction(&ws, "default", &message.id, &actors[1], emoji, true)
            .await
            .unwrap();
    }
    let reopened = Store::open(&tmp.path).await.unwrap();
    let page = reopened
        .list_discussion_messages_for(&ws, "default", None, None, 50, Some(&actors[1]))
        .await
        .unwrap();
    assert_eq!(
        page.messages[0]
            .reactions
            .iter()
            .map(|r| r.reaction.as_str())
            .collect::<Vec<_>>(),
        vec!["👍", "👎", "👀", "✅", "🙈"]
    );
    assert!(page.messages[0].reactions.iter().all(|r| r.reacted));
    assert!(matches!(
        store
            .set_discussion_actor_reaction(&ws, "default", &message.id, &actors[1], "🙂", true)
            .await,
        Err(Error::InvalidParams(_))
    ));
}
