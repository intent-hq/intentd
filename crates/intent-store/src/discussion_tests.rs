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
        .insert_discussion_message(&ws, "retry", "changed body", &renamed)
        .await
        .unwrap();
    assert!(!inserted);
    assert_eq!(retry, first);
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
    let latest = store.list_discussion_messages(&ws, None, 2).await.unwrap();
    assert!(latest.has_more);
    assert_eq!(latest.messages, vec![second, third]);
    let earlier = store
        .list_discussion_messages(&ws, Some(2), 2)
        .await
        .unwrap();
    assert!(!earlier.has_more);
    assert_eq!(earlier.messages, vec![first]);
    assert!(store
        .list_discussion_messages(&ws, Some(1), 2)
        .await
        .unwrap()
        .messages
        .is_empty());
    for (before, limit) in [(None, 0), (None, 101), (Some(0), 2)] {
        assert!(store
            .list_discussion_messages(&ws, before, limit)
            .await
            .is_err());
    }
    store.delete_workspace(&ws).await.unwrap();
    assert!(store
        .list_discussion_messages(&ws, None, 100)
        .await
        .unwrap()
        .messages
        .is_empty());
    assert_eq!(
        store
            .list_discussion_messages(&other, None, 100)
            .await
            .unwrap()
            .messages,
        vec![isolated]
    );
}
