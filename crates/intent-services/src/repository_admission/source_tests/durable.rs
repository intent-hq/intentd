//! Existing Services gates over disposable real grants. The Store-owned atomic
//! provenance snapshot and final source integration have separate receipts.

use intent_core::caller::{with_caller, with_wire_credential, Caller, WireCredential};
use intent_core::{HostInvite, HostRole, Principal, PrincipalId, PrincipalIdentity, WorkspaceRole};
use intent_store::{HostJoinCredential, Store};

use super::*;
use crate::repository_admission::RepositoryEntry;

use crate::repository_admission_source_tests::fixtures;

fn person(number: i64) -> Principal {
    Principal {
        id: PrincipalId::new(),
        identity: Some(PrincipalIdentity::github(number)),
        github_user_id: Some(number),
        login: Some(format!("person-{number}")),
        display_name: None,
        avatar_url: None,
        is_primary: false,
        created_at: "2026-09-27T00:00:00Z".into(),
        updated_at: "2026-09-27T00:00:00Z".into(),
    }
}

async fn original(principal: &PrincipalId, role: HostRole) -> OriginalRepositoryCaller {
    with_caller(
        Caller::Wire {
            principal_id: principal.clone(),
            host_role: role,
        },
        with_wire_credential(
            Some(WireCredential::Principal {
                principal_id: principal.clone(),
                token_hash: "captured-disposable-hash".into(),
            }),
            async { OriginalRepositoryCaller::capture(RepositoryEntry::Bearer).unwrap() },
        ),
    )
    .await
}

async fn member(store: &Store) -> Principal {
    let person = person(902);
    let owner = store.get_primary_principal().await.unwrap();
    let invite = HostInvite::new(
        "source-member-invite".into(),
        owner.id,
        person.identity_key().unwrap(),
        person.login.clone().unwrap(),
        "invite-disposable-hash".into(),
        None,
    )
    .unwrap();
    store.insert_host_invite(&invite).await.unwrap();
    let generation = store
        .host_membership_state()
        .await
        .unwrap()
        .authorization_generation;
    store
        .join_host_by_invite(
            &invite.id,
            &person,
            HostJoinCredential::Proof {
                token_hash: "member-disposable-hash",
                authorization_generation: generation,
            },
        )
        .await
        .unwrap();
    person
}

#[tokio::test]
async fn original_guest_gates_preserve_git_rights_but_cannot_borrow_ambient_owner_for_create() {
    let f = fixtures::Fixture::new().await;
    let guest = person(901);
    f.store.upsert_principal(&guest).await.unwrap();
    f.store
        .add_workspace_member(&f.workspace.id, &guest.id, WorkspaceRole::Collaborator)
        .await
        .unwrap();
    let captured = original(&guest.id, HostRole::Guest).await;
    let services = Services::new(f.store.clone());
    let primary = f.store.get_primary_principal().await.unwrap();
    with_caller(
        Caller::Wire {
            principal_id: primary.id,
            host_role: HostRole::Owner,
        },
        async {
            assert!(check_stage_gates(
                &services,
                &captured,
                &f.workspace.id,
                &[NativeReviewStage::Commit, NativeReviewStage::Push]
            )
            .await
            .is_ok());
            assert_eq!(
                check_stage_gates(
                    &services,
                    &captured,
                    &f.workspace.id,
                    &[NativeReviewStage::CreatePr]
                )
                .await,
                Err(AdmissionError::Denied)
            );
        },
    )
    .await;
    f.store
        .remove_workspace_member(&f.workspace.id, &guest.id)
        .await
        .unwrap();
    assert_eq!(
        check_stage_gates(
            &services,
            &captured,
            &f.workspace.id,
            &[NativeReviewStage::Commit]
        )
        .await,
        Err(AdmissionError::Denied)
    );
}

#[tokio::test]
async fn actual_host_member_gate_rechecks_removal_and_excludes_chief() {
    let f = fixtures::Fixture::new().await;
    let person = member(&f.store).await;
    let captured = original(&person.id, HostRole::Member).await;
    let services = Services::new(f.store.clone());
    assert!(check_stage_gates(
        &services,
        &captured,
        &f.workspace.id,
        &[NativeReviewStage::CreatePr]
    )
    .await
    .is_ok());
    assert_eq!(
        check_stage_gates(
            &services,
            &captured,
            &WorkspaceId::chief(),
            &[NativeReviewStage::Commit]
        )
        .await,
        Err(AdmissionError::Denied)
    );
    f.store.remove_host_member(&person.id).await.unwrap();
    assert_eq!(
        check_stage_gates(
            &services,
            &captured,
            &f.workspace.id,
            &[NativeReviewStage::CreatePr]
        )
        .await,
        Err(AdmissionError::Denied)
    );
}

#[tokio::test]
async fn unrelated_workspace_and_closed_store_do_not_become_a_gate_success() {
    let f = fixtures::Fixture::new().await;
    let guest = person(903);
    f.store.upsert_principal(&guest).await.unwrap();
    let captured = original(&guest.id, HostRole::Guest).await;
    let services = Services::new(f.store.clone());
    assert_eq!(
        check_stage_gates(
            &services,
            &captured,
            &f.workspace.id,
            &[NativeReviewStage::Commit]
        )
        .await,
        Err(AdmissionError::Denied)
    );
    assert_eq!(
        check_stage_gates(&services, &captured, &f.workspace.id, &[]).await,
        Err(AdmissionError::InvalidPlan)
    );
    f.store.close().await;
    assert_eq!(
        check_stage_gates(
            &services,
            &captured,
            &f.workspace.id,
            &[NativeReviewStage::Commit]
        )
        .await,
        Err(AdmissionError::Unavailable)
    );
}
