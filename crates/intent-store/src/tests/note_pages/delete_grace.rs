//! Actual writer/derived-state controls for guarded grace deletion.
use super::setup;
use crate::note_delete_repo::{NoteDeleteAuthority, NoteDeleteCommitOutcome, NoteDeleteGuard};
use intent_core::{note_delete::*, Caller, Error, Note, NoteId};
use std::time::Duration;
fn authority() -> NoteDeleteAuthority {
    NoteDeleteAuthority {
        caller: Caller::Daemon,
        principal_token_hash: None,
    }
}
async fn prepare(store: &crate::Store, note: &Note) -> (NoteDeleteSchedule, NoteDeleteGuard) {
    let identity = store
        .note_delete_current(&authority(), &note.workspace_id, Some(&note.id))
        .await
        .unwrap()
        .unwrap();
    let request = NoteDeleteSchedule {
        workspace_id: note.workspace_id.clone(),
        note_id: note.id.clone(),
        note_instance_id: identity.note_instance_id,
        expected_version: identity.revision,
        source_revision: identity.source_revision,
        operation_key: NoteDeleteKey {
            epoch: uuid::Uuid::new_v4().to_string(),
            issued_tick_ms: 0,
            nonce: uuid::Uuid::new_v4().to_string(),
        },
        undo_delay_ms: 15000,
    };
    let guard = store
        .note_delete_prepare(&authority(), &request)
        .await
        .unwrap();
    (request, guard)
}
async fn commit(
    store: &crate::Store,
    request: &NoteDeleteSchedule,
    guard: &NoteDeleteGuard,
) -> NoteDeleteCommitOutcome {
    store
        .note_delete_guarded_commit(
            &authority(),
            request,
            guard,
            tokio::time::Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap()
}
async fn child(store: &crate::Store, parent: &Note, id: &str) -> Note {
    let mut child = parent.clone();
    child.id = NoteId::from(id);
    child.parent_id = Some(parent.id.clone());
    child.content = "😀 child <!--anchor:x:start-->body<!--anchor:x:end-->".into();
    store.insert_note(&child).await.unwrap();
    child
}
#[tokio::test]
async fn grace_exact_delete_detaches_children_with_consistent_read_index() {
    let (store, _tmp, note) = setup("literal @@@task\r\n😀").await;
    let child = child(&store, &note, "child").await;
    let before = store
        .note_delete_current(&authority(), &note.workspace_id, Some(&child.id))
        .await
        .unwrap();
    let (request, guard) = prepare(&store, &note).await;
    assert_eq!(
        commit(&store, &request, &guard).await,
        NoteDeleteCommitOutcome::Deleted
    );
    assert!(store.get_note(&note.workspace_id, &note.id).await.is_err());
    let after = store.get_note(&note.workspace_id, &child.id).await.unwrap();
    assert_eq!(after.content, child.content);
    assert_eq!(after.parent_id, None);
    let current = store
        .note_delete_current(&authority(), &note.workspace_id, Some(&child.id))
        .await
        .unwrap();
    assert_ne!(current, before);
    let annotations = store
        .note_annotation_epochs(&note.workspace_id, &child.id)
        .await
        .unwrap();
    assert!(annotations.anchors_ready);
    assert_eq!(annotations.source_revision, after.rev);
    let pending: i64 =
        sqlx::query_scalar("SELECT count(*) FROM note_page_head WHERE indexed_rev=-1")
            .fetch_one(store.read_pool())
            .await
            .unwrap();
    assert_eq!(pending, 0);
    store.close().await;
}
#[tokio::test]
async fn grace_rejects_parent_edit_and_same_id_replacement() {
    for replacement in [false, true] {
        let (store, _tmp, mut note) = setup("before").await;
        let (request, guard) = prepare(&store, &note).await;
        if replacement {
            store
                .delete_note(&note.workspace_id, &note.id)
                .await
                .unwrap();
            note.content = "replacement".into();
            store.insert_note(&note).await.unwrap();
        } else {
            note.content = "edited".into();
            store.update_note(&note).await.unwrap();
        }
        let expected = store.get_note(&note.workspace_id, &note.id).await.unwrap();
        assert_eq!(
            commit(&store, &request, &guard).await,
            NoteDeleteCommitOutcome::Rejected(NoteDeleteReason::NoteChanged)
        );
        assert_eq!(
            store.get_note(&note.workspace_id, &note.id).await.unwrap(),
            expected
        );
        store.close().await;
    }
}
#[tokio::test]
async fn grace_rejects_each_direct_child_graph_change() {
    for change in ["edit", "remove", "reparent", "add", "replace"] {
        let (store, _tmp, note) = setup("parent").await;
        let mut child = child(&store, &note, "child").await;
        let (request, guard) = prepare(&store, &note).await;
        match change {
            "edit" => {
                child.content = "child edit".into();
                store.update_note(&child).await.unwrap();
            }
            "remove" => {
                store
                    .delete_note(&note.workspace_id, &child.id)
                    .await
                    .unwrap();
            }
            "reparent" => {
                child.parent_id = None;
                store.update_note(&child).await.unwrap();
            }
            "add" => {
                let mut extra = child.clone();
                extra.id = NoteId::from("extra");
                store.insert_note(&extra).await.unwrap();
            }
            "replace" => {
                store
                    .delete_note(&note.workspace_id, &child.id)
                    .await
                    .unwrap();
                store.insert_note(&child).await.unwrap();
            }
            _ => unreachable!(),
        }
        let expected = store.list_notes(&note.workspace_id).await.unwrap();
        assert_eq!(
            commit(&store, &request, &guard).await,
            NoteDeleteCommitOutcome::Rejected(NoteDeleteReason::ChildChanged),
            "{change}"
        );
        assert_eq!(
            store.list_notes(&note.workspace_id).await.unwrap(),
            expected,
            "{change}"
        );
        store.close().await;
    }
}
#[tokio::test]
async fn grace_child_index_failure_rolls_back_parent_and_detachment() {
    let (store, _tmp, note) = setup("parent").await;
    let child = child(&store, &note, "child").await;
    let (request, guard) = prepare(&store, &note).await;
    let before = store
        .note_delete_current(&authority(), &note.workspace_id, Some(&child.id))
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER reject_grace BEFORE INSERT ON note_page_entry WHEN NEW.note_id='child' BEGIN SELECT RAISE(ABORT,'grace index failure'); END").execute(store.write_pool()).await.unwrap();
    assert_eq!(
        commit(&store, &request, &guard).await,
        NoteDeleteCommitOutcome::Failed
    );
    assert_eq!(
        store.get_note(&note.workspace_id, &note.id).await.unwrap(),
        note
    );
    assert_eq!(
        store.get_note(&note.workspace_id, &child.id).await.unwrap(),
        child
    );
    assert_eq!(
        store
            .note_delete_current(&authority(), &note.workspace_id, Some(&child.id))
            .await
            .unwrap(),
        before
    );
    sqlx::query("DROP TRIGGER reject_grace")
        .execute(store.write_pool())
        .await
        .unwrap();
    assert_eq!(
        commit(&store, &request, &guard).await,
        NoteDeleteCommitOutcome::Deleted
    );
    store.close().await;
}
#[tokio::test]
async fn grace_missing_and_foreign_workspace_are_indistinguishable() {
    let (store, _tmp, note) = setup("secret").await;
    let primary = store.get_primary_principal().await.unwrap();
    // A valid principal without primary/host/workspace authority cannot probe
    // workspace existence. Use the private fixture's durable primary downgrade.
    sqlx::query("UPDATE principal SET is_primary=0 WHERE id=?")
        .bind(primary.id.as_str())
        .execute(store.write_pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM workspace_member WHERE principal_id=?")
        .bind(primary.id.as_str())
        .execute(store.write_pool())
        .await
        .unwrap();
    sqlx::query("DELETE FROM host_member WHERE principal_id=?")
        .bind(primary.id.as_str())
        .execute(store.write_pool())
        .await
        .unwrap();
    let auth = NoteDeleteAuthority {
        caller: Caller::Wire {
            principal_id: primary.id,
            host_role: intent_core::HostRole::Guest,
        },
        principal_token_hash: None,
    };
    for ws in [
        note.workspace_id.clone(),
        intent_core::WorkspaceId::from("missing"),
    ] {
        let err = store
            .note_delete_current(&auth, &ws, Some(&note.id))
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::NoteDelete(NoteDeleteError::Unavailable)),
            "{err:?}"
        );
    }
    store.close().await;
}

#[tokio::test]
async fn grace_capacity_overflow_rejects_without_changing_graph() {
    let (store, _tmp, note) = setup("bounded parent").await;
    for i in 0..MAX_CHILDREN {
        child(&store, &note, &format!("child-{i:03}")).await;
    }
    let (request, guard) = prepare(&store, &note).await;
    assert_eq!(guard.children_count, MAX_CHILDREN);
    child(&store, &note, "overflow").await;
    assert!(matches!(
        store.note_delete_prepare(&authority(), &request).await,
        Err(Error::NoteDelete(NoteDeleteError::GraphLimit))
    ));
    assert_eq!(
        commit(&store, &request, &guard).await,
        NoteDeleteCommitOutcome::Rejected(NoteDeleteReason::ChildChanged)
    );
    assert_eq!(
        store.list_notes(&note.workspace_id).await.unwrap().len(),
        MAX_CHILDREN + 2
    );
    store.close().await;
}
#[tokio::test]
async fn grace_revalidates_bearer_and_durable_role_inside_writer() {
    for revoke in [true, false] {
        let (store, _tmp, note) = setup("authority guard").await;
        let principal = store.get_primary_principal().await.unwrap();
        let hash = "a".repeat(64);
        store
            .insert_principal_credential(&principal.id, &hash)
            .await
            .unwrap();
        let auth = NoteDeleteAuthority {
            caller: Caller::Wire {
                principal_id: principal.id.clone(),
                host_role: intent_core::HostRole::Owner,
            },
            principal_token_hash: Some(hash.clone()),
        };
        let (request, _) = prepare(&store, &note).await;
        let guard = store.note_delete_prepare(&auth, &request).await.unwrap();
        if revoke {
            store.revoke_principal_credential(&hash).await.unwrap();
        } else {
            sqlx::query("UPDATE principal SET is_primary=0 WHERE id=?")
                .bind(principal.id.as_str())
                .execute(store.write_pool())
                .await
                .unwrap();
            sqlx::query("DELETE FROM workspace_member WHERE principal_id=?")
                .bind(principal.id.as_str())
                .execute(store.write_pool())
                .await
                .unwrap();
            sqlx::query("DELETE FROM host_member WHERE principal_id=?")
                .bind(principal.id.as_str())
                .execute(store.write_pool())
                .await
                .unwrap();
        }
        let result = store
            .note_delete_guarded_commit(
                &auth,
                &request,
                &guard,
                tokio::time::Instant::now() + Duration::from_secs(5),
            )
            .await
            .unwrap();
        assert!(
            matches!(result, NoteDeleteCommitOutcome::Rejected(_)),
            "{result:?}"
        );
        assert_eq!(
            store.get_note(&note.workspace_id, &note.id).await.unwrap(),
            note
        );
        store.close().await;
    }
}
#[tokio::test]
async fn grace_annotation_failure_rolls_back_and_cancelled_writer_is_reusable() {
    use crate::note_annotation_repo::{FinalizerPause, FINALIZER_PAUSE};
    use std::sync::Arc;
    let (store, _tmp, note) = setup("parent").await;
    let mut child = child(&store, &note, "child").await;
    let author = intent_core::NoteVersionAuthor {
        id: "author".into(),
        name: "Author".into(),
        author_type: "user".into(),
    };
    let comment = crate::tests::sample_comment(&child.id, "x", "x");
    child.rev = store
        .update_note_with_comment(&child, Some(child.rev), &comment, &author)
        .await
        .unwrap();
    child = store.get_note(&note.workspace_id, &child.id).await.unwrap();
    let (request, guard) = prepare(&store, &note).await;
    let before = store
        .note_annotation_epochs(&note.workspace_id, &child.id)
        .await
        .unwrap();
    sqlx::query("CREATE TRIGGER reject_grace_anchor BEFORE INSERT ON note_comment_anchor BEGIN SELECT RAISE(ABORT,'grace anchor failure'); END").execute(store.write_pool()).await.unwrap();
    assert_eq!(
        commit(&store, &request, &guard).await,
        NoteDeleteCommitOutcome::Failed
    );
    assert_eq!(
        store.get_note(&note.workspace_id, &note.id).await.unwrap(),
        note
    );
    assert_eq!(
        store.get_note(&note.workspace_id, &child.id).await.unwrap(),
        child
    );
    assert_eq!(
        store
            .note_annotation_epochs(&note.workspace_id, &child.id)
            .await
            .unwrap(),
        before
    );
    sqlx::query("DROP TRIGGER reject_grace_anchor")
        .execute(store.write_pool())
        .await
        .unwrap();
    let pause = Arc::new(FinalizerPause::default());
    let auth = authority();
    let mut write = Box::pin(FINALIZER_PAUSE.scope(
        pause.clone(),
        store.note_delete_guarded_commit(
            &auth,
            &request,
            &guard,
            tokio::time::Instant::now() + Duration::from_secs(5),
        ),
    ));
    tokio::time::timeout(Duration::from_secs(5),async {
        tokio::select! {r=&mut write=>panic!("finalizer did not pause: {r:?}"),()=pause.entered.notified()=>{}}
    }).await.unwrap();
    assert_eq!(
        store.get_note(&note.workspace_id, &note.id).await.unwrap(),
        note
    );
    drop(write);
    tokio::time::timeout(
        Duration::from_secs(5),
        sqlx::query("BEGIN IMMEDIATE; ROLLBACK").execute(store.write_pool()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        store.get_note(&note.workspace_id, &note.id).await.unwrap(),
        note
    );
    assert_eq!(
        store.get_note(&note.workspace_id, &child.id).await.unwrap(),
        child
    );
    assert_eq!(
        store
            .note_annotation_epochs(&note.workspace_id, &child.id)
            .await
            .unwrap(),
        before
    );
    assert_eq!(
        commit(&store, &request, &guard).await,
        NoteDeleteCommitOutcome::Deleted
    );
    store.close().await;
}

#[tokio::test]
async fn grace_unavailable_index_is_not_authoritative_absence() {
    for damage in ["missing", "current", "indexed"] {
        let (store, _tmp, note) = setup("existing canonical source").await;
        let (request, guard) = prepare(&store, &note).await;
        let sql=match damage {
            "missing"=>"DELETE FROM note_page_head WHERE workspace_id=? AND note_id=?",
            "current"=>"UPDATE note_page_head SET current_rev=current_rev+1 WHERE workspace_id=? AND note_id=?",
            _=>"UPDATE note_page_head SET indexed_rev=-1 WHERE workspace_id=? AND note_id=?",
        };
        sqlx::query(sql)
            .bind(note.workspace_id.as_str())
            .bind(note.id.as_str())
            .execute(store.write_pool())
            .await
            .unwrap();
        assert!(
            matches!(
                store
                    .note_delete_current(&authority(), &note.workspace_id, Some(&note.id))
                    .await,
                Err(Error::NoteDelete(NoteDeleteError::Unavailable))
            ),
            "{damage}"
        );
        assert!(
            matches!(
                store.note_delete_prepare(&authority(), &request).await,
                Err(Error::NoteDelete(NoteDeleteError::Unavailable))
            ),
            "{damage}"
        );
        assert_eq!(
            commit(&store, &request, &guard).await,
            NoteDeleteCommitOutcome::Failed
        );
        assert_eq!(
            store.get_note(&note.workspace_id, &note.id).await.unwrap(),
            note
        );
        assert_eq!(
            store
                .note_delete_current(
                    &authority(),
                    &note.workspace_id,
                    Some(&NoteId::from("absent"))
                )
                .await
                .unwrap(),
            None
        );
        store.close().await;
    }
}
#[tokio::test]
async fn grace_unavailable_child_index_refuses_admission_and_commit() {
    for damage in ["missing", "current", "indexed"] {
        let (store, _tmp, note) = setup("parent").await;
        let child = child(&store, &note, "child").await;
        let (request, guard) = prepare(&store, &note).await;
        let sql=match damage {
            "missing"=>"DELETE FROM note_page_head WHERE workspace_id=? AND note_id=?",
            "current"=>"UPDATE note_page_head SET current_rev=current_rev+1 WHERE workspace_id=? AND note_id=?",
            _=>"UPDATE note_page_head SET indexed_rev=-1 WHERE workspace_id=? AND note_id=?",
        };
        sqlx::query(sql)
            .bind(note.workspace_id.as_str())
            .bind(child.id.as_str())
            .execute(store.write_pool())
            .await
            .unwrap();
        assert!(
            matches!(
                store.note_delete_prepare(&authority(), &request).await,
                Err(Error::NoteDelete(NoteDeleteError::GraphLimit))
            ),
            "{damage}"
        );
        assert_eq!(
            commit(&store, &request, &guard).await,
            NoteDeleteCommitOutcome::Rejected(NoteDeleteReason::ChildChanged)
        );
        assert_eq!(
            store.get_note(&note.workspace_id, &note.id).await.unwrap(),
            note
        );
        assert_eq!(
            store.get_note(&note.workspace_id, &child.id).await.unwrap(),
            child
        );
        store.close().await;
    }
}

#[tokio::test]
async fn grace_retired_agent_and_removed_workspace_fail_closed_at_commit() {
    let (store, _tmp, note) = setup("agent authority").await;
    let agent = intent_core::AgentId::from("agent-grace");
    let session = crate::tests::sample_agent_session(&agent, &note.workspace_id);
    store.insert_agent_session(&session).await.unwrap();
    let auth = NoteDeleteAuthority {
        caller: Caller::Agent {
            agent_id: agent.clone(),
        },
        principal_token_hash: None,
    };
    let (request, _) = prepare(&store, &note).await;
    let guard = store.note_delete_prepare(&auth, &request).await.unwrap();
    sqlx::query("UPDATE agent_session SET retired_at='2026-10-08T00:00:00Z' WHERE id=?")
        .bind(agent.as_str())
        .execute(store.write_pool())
        .await
        .unwrap();
    assert_eq!(
        store
            .note_delete_guarded_commit(
                &auth,
                &request,
                &guard,
                tokio::time::Instant::now() + Duration::from_secs(5)
            )
            .await
            .unwrap(),
        NoteDeleteCommitOutcome::Rejected(NoteDeleteReason::AuthorityLost)
    );
    assert_eq!(
        store.get_note(&note.workspace_id, &note.id).await.unwrap(),
        note
    );
    sqlx::query("DELETE FROM workspace WHERE id=?")
        .bind(note.workspace_id.as_str())
        .execute(store.write_pool())
        .await
        .unwrap();
    assert_eq!(
        commit(&store, &request, &guard).await,
        NoteDeleteCommitOutcome::Rejected(NoteDeleteReason::WorkspaceMissing)
    );
    assert!(store.get_note(&note.workspace_id, &note.id).await.is_err());
    store.close().await;
}
