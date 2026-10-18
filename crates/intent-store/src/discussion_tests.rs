use super::*;
use intent_core::discussion::DiscussionAuthor;
use intent_core::PrincipalId;

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
        principal_id: store.get_primary_principal().await.unwrap().id,
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
    renamed.principal_id = second_person.id;
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
        principal_id: store.get_primary_principal().await.unwrap().id,
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
        principal_id: store.get_primary_principal().await.unwrap().id,
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
    sqlx::raw_sql("DROP TABLE discussion_reaction; DROP TABLE workspace_discussion_message; DROP TABLE discussion_participant; DROP TABLE discussion_conversation;")
        .execute(store.write_pool()).await.unwrap();
    sqlx::raw_sql(include_str!("../migrations/0162_workspace_discussions.sql"))
        .execute(store.write_pool())
        .await
        .unwrap();
    let author = DiscussionAuthor {
        principal_id: store.get_primary_principal().await.unwrap().id,
        login: Some("original".into()),
        display_name: None,
        avatar_url: None,
    };
    sqlx::query("INSERT INTO workspace_discussion_message(id,workspace_id,sequence,principal_id,client_message_id,body,author_json,created_at) VALUES('old-message',?,7,?,'retry','preserved',?,'2026-01-01T00:00:00Z')")
        .bind(ws.as_str()).bind(author.principal_id.as_str()).bind(serde_json::to_string(&author).unwrap()).execute(store.write_pool()).await.unwrap();
    sqlx::raw_sql(include_str!(
        "../migrations/0163_discussion_conversations.sql"
    ))
    .execute(store.write_pool())
    .await
    .unwrap();
    sqlx::raw_sql(include_str!(
        "../migrations/0164_discussion_message_actions.sql"
    ))
    .execute(store.write_pool())
    .await
    .unwrap();
    let page = store
        .list_discussion_messages(&ws, None, None, 50)
        .await
        .unwrap();
    assert_eq!(page.messages.len(), 1);
    let old = &page.messages[0];
    assert_eq!(old.id, "old-message");
    assert_eq!(old.conversation_id, "default");
    assert_eq!(old.sequence, 7);
    assert_eq!(old.author, author);
    let (retry, inserted) = store
        .insert_discussion_message(&ws, "retry", "preserved", &author)
        .await
        .unwrap();
    assert!(!inserted);
    assert_eq!(&retry, old);
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
        principal_id: person.id.clone(),
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
