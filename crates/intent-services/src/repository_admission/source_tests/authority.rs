//! Concrete engine composition over disposable Store/Git sources. Writer
//! retirement and real request-entry installation remain separate boundaries.

use std::sync::Arc;

use intent_core::caller::{with_caller, with_wire_credential};
use intent_core::{
    AgentId, HostRole, NativeReviewGitReceipt, NativeReviewOutcome, NativeReviewPreparation,
    NativeReviewPublication, Principal, PrincipalId, PrincipalIdentity, WorkspaceRole,
};
use tokio::sync::{oneshot, Notify};

use super::*;
use crate::repository_admission::{
    begin_repository_stage, classify_repository_completion, revalidate_repository_stage,
    RepositoryCompletion, RepositoryEntry,
};
use crate::repository_admission_source_tests::fixtures::{resolver, Fixture};
use crate::repository_context_reader::read_repository_context_with_resolver;

const HASH: &str = "disposable-original-personal-hash";

async fn wire(
    principal: &PrincipalId,
    role: HostRole,
    hash: Option<&str>,
) -> OriginalRepositoryCaller {
    let credential = hash.map(|hash| WireCredential::Principal {
        principal_id: principal.clone(),
        token_hash: hash.into(),
    });
    let entry = if credential.is_some() {
        RepositoryEntry::Bearer
    } else {
        RepositoryEntry::AdmittedLocal
    };
    with_caller(
        Caller::Wire {
            principal_id: principal.clone(),
            host_role: role,
        },
        with_wire_credential(credential, async {
            OriginalRepositoryCaller::capture(entry).unwrap()
        }),
    )
    .await
}

async fn owner(f: &Fixture, personal: bool) -> OriginalRepositoryCaller {
    let principal = f.store.get_primary_principal().await.unwrap();
    if personal {
        f.store
            .insert_principal_credential(&principal.id, HASH)
            .await
            .unwrap();
    }
    wire(&principal.id, HostRole::Owner, personal.then_some(HASH)).await
}

async fn guest(f: &Fixture) -> Principal {
    let person = Principal {
        id: PrincipalId::new(),
        identity: Some(PrincipalIdentity::github(8301)),
        github_user_id: Some(8301),
        login: Some("source-guest".into()),
        display_name: None,
        avatar_url: None,
        is_primary: false,
        created_at: "2026-09-27T00:00:00Z".into(),
        updated_at: "2026-09-27T00:00:00Z".into(),
    };
    f.store.upsert_principal(&person).await.unwrap();
    f.store
        .insert_principal_credential(&person.id, HASH)
        .await
        .unwrap();
    f.store
        .add_workspace_member(&f.workspace.id, &person.id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    person
}

fn input(f: &Fixture) -> RepositorySourceInput {
    let context = Fixture::input(f.root(), &f.path);
    let read =
        read_repository_context_with_resolver(&context, &resolver(), &f.environment()).unwrap();
    let fixture: serde_json::Value = serde_json::from_str(include_str!(
        "../../../../intent-core/tests/fixtures/native_review_v1.json"
    ))
    .unwrap();
    let mut preparation: NativeReviewPreparation =
        serde_json::from_value(fixture["prepare"]["reviewPreparation"].clone()).unwrap();
    preparation.root = f.root();
    preparation.scope = context.scope.clone();
    preparation.context_revision = context.revision.clone();
    preparation.local_head_sha = read.context.roots[0].head_sha.clone();
    preparation.source.repository = context.roots[0].targets[0].target.clone();
    preparation.source.connection = None;
    preparation.source.provider_project_id = None;
    preparation.source.branch = "main".into();
    preparation.target = preparation.source.clone();
    preparation.transport = None;
    RepositorySourceInput {
        facts: RepositoryOperationFacts {
            preparation,
            worktree_path: f.path.clone(),
            git_dir: read.change_inputs[0].git_dir.clone(),
            common_dir: read.change_inputs[0].common_dir.clone(),
            source_ref: "refs/heads/main".into(),
            staging_fingerprint: None,
            fetch_destinations: Vec::new(),
            push_destinations: Vec::new(),
            credential_requests: Vec::new(),
        },
        context,
        resolver: resolver(),
        environment: f.environment(),
        before_lock: None,
    }
}

#[tokio::test]
async fn explicit_local_source_uses_original_caller_and_retires_escaped_admission() {
    let f = Fixture::new().await;
    let services = Services::new(f.store.clone());
    let original = owner(&f, false).await;
    let escaped = with_caller(
        Caller::Daemon,
        with_repository_source(
            &services,
            original,
            "local".into(),
            vec![NativeReviewStage::Commit],
            input(&f),
            RepositoryRetirement::default(),
            |admission| async move {
                assert!(
                    revalidate_repository_stage(&admission, NativeReviewStage::Commit)
                        .await
                        .is_ok()
                );
                Ok(admission)
            },
        ),
    )
    .await
    .unwrap();
    assert!(matches!(
        revalidate_repository_stage(&escaped, NativeReviewStage::Commit).await,
        Err(AdmissionError::Retired)
    ));
}

#[tokio::test]
async fn original_personal_hash_never_uses_a_new_or_foreign_credential() {
    for mode in 0..3 {
        let f = Fixture::new().await;
        let services = Services::new(f.store.clone());
        let captured = owner(&f, true).await;
        match mode {
            0 => {
                f.store.revoke_principal_credential(HASH).await.unwrap();
            }
            1 => {
                sqlx::query("DELETE FROM principal_credential WHERE token_hash = ?")
                    .bind(HASH)
                    .execute(f.store.write_pool())
                    .await
                    .unwrap();
            }
            _ => {
                let person = PrincipalId::new();
                // Foreign-key enforcement refuses assigning the original hash
                // to an unknown actor, so remove it and add an unrelated hash.
                f.store.revoke_principal_credential(HASH).await.unwrap();
                let primary = f.store.get_primary_principal().await.unwrap();
                f.store
                    .insert_principal_credential(&primary.id, &person.to_string())
                    .await
                    .unwrap();
            }
        }
        assert!(matches!(
            with_repository_source(
                &services,
                captured,
                "invalid".into(),
                vec![NativeReviewStage::Commit],
                input(&f),
                RepositoryRetirement::default(),
                |_| async { Ok(()) }
            )
            .await,
            Err(AdmissionError::Denied)
        ));
    }
}

#[tokio::test]
async fn guest_source_preserves_git_permission_without_create_or_forge_credential_authority() {
    let f = Fixture::new().await;
    let person = guest(&f).await;
    let services = Services::new(f.store.clone());
    with_repository_source(
        &services,
        wire(&person.id, HostRole::Guest, Some(HASH)).await,
        "guest-push".into(),
        vec![NativeReviewStage::Push],
        input(&f),
        RepositoryRetirement::default(),
        |admission| async move {
            let stamp = begin_repository_stage(
                revalidate_repository_stage(&admission, NativeReviewStage::Push)
                    .await
                    .unwrap(),
            )
            .unwrap();
            assert!(matches!(
                stamp.credential_authority(),
                Err(AdmissionError::Denied)
            ));
            drop(stamp);
            assert!(matches!(
                admission.execution().unwrap().outcome,
                NativeReviewOutcome::Uncertain { .. }
            ));
            Ok(())
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        with_repository_source(
            &services,
            wire(&person.id, HostRole::Guest, Some(HASH)).await,
            "guest-create".into(),
            vec![NativeReviewStage::CreatePr],
            input(&f),
            RepositoryRetirement::default(),
            |_| async { Ok(()) }
        )
        .await,
        Err(AdmissionError::Denied)
    ));
}

#[tokio::test]
async fn direct_grant_remove_readd_retires_old_operation_but_fresh_request_can_enter() {
    let owned = Fixture::new().await;
    let f = &owned;
    let owned_person = guest(f).await;
    let person = &owned_person;
    let services = Services::new(f.store.clone());
    with_repository_source(
        &services,
        wire(&person.id, HostRole::Guest, Some(HASH)).await,
        "old-grant".into(),
        vec![NativeReviewStage::Commit],
        input(f),
        RepositoryRetirement::default(),
        |admission| async move {
            f.store
                .remove_workspace_member(&f.workspace.id, &person.id)
                .await
                .unwrap();
            f.store
                .add_workspace_member(&f.workspace.id, &person.id, WorkspaceRole::Collaborator)
                .await
                .unwrap();
            assert!(matches!(
                revalidate_repository_stage(&admission, NativeReviewStage::Commit).await,
                Err(AdmissionError::Retired)
            ));
            Ok(())
        },
    )
    .await
    .unwrap();
    with_repository_source(
        &services,
        wire(&person.id, HostRole::Guest, Some(HASH)).await,
        "new-grant".into(),
        vec![NativeReviewStage::Commit],
        input(f),
        RepositoryRetirement::default(),
        |_| async { Ok(()) },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn role_or_credential_aba_is_not_hidden_by_restoring_identical_current_values() {
    for credential in [false, true] {
        let f = Fixture::new().await;
        let person = guest(&f).await;
        if !credential {
            // Vacate the original owner before exercising a valid role change;
            // Store forbids two simultaneous workspace owners.
            let primary = f.store.get_primary_principal().await.unwrap();
            f.store
                .remove_workspace_member(&f.workspace.id, &primary.id)
                .await
                .unwrap();
        }
        let services = Services::new(f.store.clone());
        with_repository_source(
            &services,
            wire(&person.id, HostRole::Guest, Some(HASH)).await,
            "aba".into(),
            vec![NativeReviewStage::Commit],
            input(&f),
            RepositoryRetirement::default(),
            |admission| async move {
                if credential {
                    sqlx::query("DELETE FROM principal_credential WHERE token_hash = ?")
                        .bind(HASH)
                        .execute(f.store.write_pool())
                        .await
                        .unwrap();
                    f.store
                        .insert_principal_credential(&person.id, HASH)
                        .await
                        .unwrap();
                } else {
                    f.store
                        .set_workspace_member_role(
                            &f.workspace.id,
                            &person.id,
                            WorkspaceRole::Owner,
                        )
                        .await
                        .unwrap();
                    f.store
                        .set_workspace_member_role(
                            &f.workspace.id,
                            &person.id,
                            WorkspaceRole::Collaborator,
                        )
                        .await
                        .unwrap();
                }
                assert!(matches!(
                    revalidate_repository_stage(&admission, NativeReviewStage::Commit).await,
                    Err(AdmissionError::Retired)
                ));
                Ok(())
            },
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn changed_primary_or_deleted_original_principal_never_borrows_current_owner() {
    for remove in [false, true] {
        let f = Fixture::new().await;
        let services = Services::new(f.store.clone());
        let original = owner(&f, true).await;
        let primary = f.store.get_primary_principal().await.unwrap();
        with_repository_source(
            &services,
            original,
            "primary".into(),
            vec![NativeReviewStage::Commit],
            input(&f),
            RepositoryRetirement::default(),
            |admission| async move {
                if remove {
                    sqlx::query("DELETE FROM principal WHERE id = ?")
                        .bind(primary.id.as_str())
                        .execute(f.store.write_pool())
                        .await
                        .unwrap();
                } else {
                    // Profile upserts intentionally preserve is_primary. Change
                    // the actual fixture row to exercise the migration-owned flag.
                    sqlx::query("UPDATE principal SET is_primary = 0 WHERE id = ?")
                        .bind(primary.id.as_str())
                        .execute(f.store.write_pool())
                        .await
                        .unwrap();
                }
                assert!(matches!(
                    revalidate_repository_stage(&admission, NativeReviewStage::Commit).await,
                    Err(AdmissionError::Denied)
                ));
                Ok(())
            },
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn queued_permission_aba_is_checked_after_the_real_worktree_lock_wait() {
    let f = Fixture::new().await;
    let person = guest(&f).await;
    let services = Services::new(f.store.clone());
    let locks = services.worktree_locks.clone();
    let path = f.path.clone();
    let (ready_tx, ready_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let blocker = tokio::spawn(async move {
        locks
            .with_lock(&path, || async {
                ready_tx.send(()).unwrap();
                release_rx.await.unwrap();
            })
            .await;
    });
    ready_rx.await.unwrap();
    let reached = Arc::new(Notify::new());
    let mut parameters = input(&f);
    parameters.before_lock = Some(reached.clone());
    let session = with_repository_source(
        &services,
        wire(&person.id, HostRole::Guest, Some(HASH)).await,
        "queued".into(),
        vec![NativeReviewStage::Commit],
        parameters,
        RepositoryRetirement::default(),
        |_| async { Ok(()) },
    );
    tokio::pin!(session);
    tokio::select! { ()=reached.notified()=>{}, _=&mut session=>panic!("session did not wait") }
    f.store
        .remove_workspace_member(&f.workspace.id, &person.id)
        .await
        .unwrap();
    f.store
        .add_workspace_member(&f.workspace.id, &person.id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    release_tx.send(()).unwrap();
    blocker.await.unwrap();
    assert!(matches!(session.await, Err(AdmissionError::Retired)));
}

#[tokio::test]
async fn workspace_recreation_and_live_git_ref_changes_stop_the_bound_source() {
    for recreate in [false, true] {
        let f = Fixture::new().await;
        let services = Services::new(f.store.clone());
        with_repository_source(
            &services,
            owner(&f, false).await,
            "root-change".into(),
            vec![NativeReviewStage::Commit],
            input(&f),
            RepositoryRetirement::default(),
            |admission| async move {
                if recreate {
                    f.store.delete_workspace(&f.workspace.id).await.unwrap();
                    f.store.insert_workspace(&f.workspace).await.unwrap();
                } else {
                    f.git(&f.path, &["checkout", "-b", "changed/ref"]);
                }
                assert!(matches!(
                    revalidate_repository_stage(&admission, NativeReviewStage::Commit).await,
                    Err(AdmissionError::Retired | AdmissionError::BindingChanged)
                ));
                Ok(())
            },
        )
        .await
        .unwrap();
    }
}

#[tokio::test]
async fn no_op_credential_touch_and_profile_update_preserve_current_source() {
    let f = Fixture::new().await;
    let services = Services::new(f.store.clone());
    let original = owner(&f, true).await;
    with_repository_source(
        &services,
        original,
        "no-op".into(),
        vec![NativeReviewStage::Commit],
        input(&f),
        RepositoryRetirement::default(),
        |admission| async move {
            f.store.touch_principal_credential(HASH).await.unwrap();
            let mut person = f.store.get_primary_principal().await.unwrap();
            person.display_name = Some("New profile".into());
            f.store.upsert_principal(&person).await.unwrap();
            assert!(
                revalidate_repository_stage(&admission, NativeReviewStage::Commit)
                    .await
                    .is_ok()
            );
            Ok(())
        },
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn completed_commit_survives_real_revocation_without_claiming_publication() {
    let f = Fixture::new().await;
    let person = guest(&f).await;
    let services = Services::new(f.store.clone());
    with_repository_source(&services,wire(&person.id,HostRole::Guest,Some(HASH)).await,"commit-then-push".into(),
        vec![NativeReviewStage::Commit,NativeReviewStage::Push],input(&f),RepositoryRetirement::default(),|admission|async move {
            let stamp=begin_repository_stage(revalidate_repository_stage(&admission,NativeReviewStage::Commit).await.unwrap()).unwrap();
            let repo=git2::Repository::open(&f.path).unwrap();let parent=repo.head().unwrap().peel_to_commit().unwrap();
            let signature=git2::Signature::now("Fixture","fixture@example.invalid").unwrap();
            let next=repo.commit(Some("HEAD"),&signature,&signature,"local fixture commit",&parent.tree().unwrap(),&[&parent]).unwrap().to_string();
            classify_repository_completion(stamp,RepositoryCompletion::Committed{hash:next.clone(),staging_after:None}).unwrap();
            f.store.revoke_principal_credential(HASH).await.unwrap();
            assert!(matches!(revalidate_repository_stage(&admission,NativeReviewStage::Push).await,Err(AdmissionError::Denied)));
            let result=admission.fail_before_dispatch(NativeReviewStage::Push,AdmissionError::Denied).unwrap();
            assert!(matches!(result.git_receipts.as_slice(),[NativeReviewGitReceipt::Commit{commit_hash}] if commit_hash==&next));
            assert!(matches!(result.publication,NativeReviewPublication::Unknown{local_head_sha:Some(ref sha),remote_source_sha:None} if sha==&next));
            Ok(())
        }).await.unwrap();
}

#[tokio::test]
async fn closed_store_is_local_unavailable_without_a_provider_auth_failure() {
    let f = Fixture::new().await;
    let services = Services::new(f.store.clone());
    with_repository_source(
        &services,
        owner(&f, false).await,
        "closed".into(),
        vec![NativeReviewStage::Commit],
        input(&f),
        RepositoryRetirement::default(),
        |admission| async move {
            f.store.close().await;
            assert!(matches!(
                revalidate_repository_stage(&admission, NativeReviewStage::Commit).await,
                Err(AdmissionError::Unavailable)
            ));
            Ok(())
        },
    )
    .await
    .unwrap();
}

async fn internal(caller: Caller) -> OriginalRepositoryCaller {
    let entry = match caller {
        Caller::Agent { .. } => RepositoryEntry::AgentCallback,
        Caller::Daemon => RepositoryEntry::DaemonTask,
        Caller::Wire { .. } => panic!("internal fixture"),
    };
    with_caller(
        caller,
        with_wire_credential(None, async {
            OriginalRepositoryCaller::capture(entry).unwrap()
        }),
    )
    .await
}

async fn agent(f: &Fixture) -> AgentId {
    let id = AgentId::new();
    let session: intent_core::AgentSession = serde_json::from_value(serde_json::json!({
        "id":id,"workspaceId":f.workspace.id,"name":"source fixture","status":"active",
        "acpSessionId":"original-acp","createdAt":"2026-09-27T00:00:00Z","updatedAt":"2026-09-27T00:00:00Z"
    })).unwrap();
    f.store.insert_agent_session(&session).await.unwrap();
    id
}

#[tokio::test]
async fn internal_entries_use_workspace_only_facts_and_their_own_lifetime() {
    for is_agent in [false, true] {
        let f = Fixture::new().await;
        let services = Services::new(f.store.clone());
        let caller = if is_agent {
            Caller::Agent {
                agent_id: agent(&f).await,
            }
        } else {
            Caller::Daemon
        };
        // No human credential or primary can be borrowed by either branch.
        sqlx::query("DELETE FROM principal")
            .execute(f.store.write_pool())
            .await
            .unwrap();
        let original = internal(caller).await;
        let retirement = RepositoryRetirement::default();
        let facts = read_current_authority(
            &services,
            &original,
            &f.workspace.id,
            &[NativeReviewStage::Commit],
            &retirement,
        )
        .await
        .unwrap();
        assert!(facts.primary_principal_id.is_none() && facts.credential.is_none());
        assert_eq!(facts.internal_stages, vec![NativeReviewStage::Commit]);
        let escaped = with_repository_source(
            &services,
            original,
            "internal".into(),
            vec![NativeReviewStage::Commit],
            input(&f),
            retirement.clone(),
            |admission| async move {
                assert!(
                    revalidate_repository_stage(&admission, NativeReviewStage::Commit)
                        .await
                        .is_ok()
                );
                retirement.retire();
                assert!(matches!(
                    revalidate_repository_stage(&admission, NativeReviewStage::Commit).await,
                    Err(AdmissionError::Retired)
                ));
                Ok(admission)
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            revalidate_repository_stage(&escaped, NativeReviewStage::Commit).await,
            Err(AdmissionError::Retired)
        ));
    }
}

#[tokio::test]
async fn missing_agent_or_human_identity_cannot_take_a_different_source_branch() {
    let f = Fixture::new().await;
    let services = Services::new(f.store.clone());
    for original in [
        internal(Caller::Agent {
            agent_id: AgentId::new(),
        })
        .await,
        wire(&PrincipalId::new(), HostRole::Owner, None).await,
    ] {
        assert!(matches!(
            with_repository_source(
                &services,
                original,
                "missing".into(),
                vec![NativeReviewStage::Commit],
                input(&f),
                RepositoryRetirement::default(),
                |_| async { Ok(()) }
            )
            .await,
            Err(AdmissionError::Denied)
        ));
    }
}

#[tokio::test]
async fn agent_retirement_pending_delete_and_replacement_stop_the_original_request() {
    for change in 0..4 {
        let f = Fixture::new().await;
        let services = Services::new(f.store.clone());
        let id = agent(&f).await;
        let original = internal(Caller::Agent {
            agent_id: id.clone(),
        })
        .await;
        let services_ref = &services;
        with_repository_source(&services,original,"agent-lifetime".into(),vec![NativeReviewStage::Commit],input(&f),RepositoryRetirement::default(), |admission| async move {
            match change {
                0=>{ f.store.set_agent_session_retired_at(&f.workspace.id,&id,Some("2026-09-27T01:00:00Z"),"2026-09-27T01:00:00Z").await.unwrap(); }
                1=>{ services_ref.pending_agent_deletes.schedule(id.to_string(),"2026-09-27T01:00:00Z".into(), |_|tokio::spawn(async {})); }
                2=>{ sqlx::query("UPDATE agent_session SET acp_session_id = 'replacement' WHERE id = ?").bind(id.as_str()).execute(f.store.write_pool()).await.unwrap(); }
                _=>{ sqlx::query("UPDATE agent_session SET status = 'deleted' WHERE id = ?").bind(id.as_str()).execute(f.store.write_pool()).await.unwrap(); }
            }
            assert!(matches!(revalidate_repository_stage(&admission,NativeReviewStage::Commit).await,Err(AdmissionError::Denied | AdmissionError::Retired)));
            services_ref.pending_agent_deletes.cancel(id.as_str());
            // A denial already observed by this source is permanent, even if a
            // later restore returns the same row values. Unobserved ABA still
            // requires the explicit owner retirement hook before mutation.
            f.store.set_agent_session_retired_at(&f.workspace.id,&id,None,"2026-09-27T01:01:00Z").await.unwrap();
            sqlx::query("UPDATE agent_session SET acp_session_id = 'original-acp', status = 'active' WHERE id = ?").bind(id.as_str()).execute(f.store.write_pool()).await.unwrap();
            assert!(matches!(revalidate_repository_stage(&admission,NativeReviewStage::Commit).await,Err(AdmissionError::Retired)));
            Ok(())
        }).await.unwrap();
    }
}

#[tokio::test]
async fn internal_workspace_recreation_is_detected_without_a_human_generation() {
    let f = Fixture::new().await;
    let services = Services::new(f.store.clone());
    with_repository_source(
        &services,
        internal(Caller::Daemon).await,
        "daemon-workspace".into(),
        vec![NativeReviewStage::Commit],
        input(&f),
        RepositoryRetirement::default(),
        |admission| async move {
            f.store.delete_workspace(&f.workspace.id).await.unwrap();
            f.store.insert_workspace(&f.workspace).await.unwrap();
            assert!(matches!(
                revalidate_repository_stage(&admission, NativeReviewStage::Commit).await,
                Err(AdmissionError::Retired)
            ));
            Ok(())
        },
    )
    .await
    .unwrap();
}
