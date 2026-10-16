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
        principal_id: PrincipalId::new(),
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
    renamed.principal_id = PrincipalId::new();
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
        principal_id: PrincipalId::new(),
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
        principal_id: PrincipalId::new(),
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
