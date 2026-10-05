//! Composition tests use the actual private directory and callback. Durable
//! membership, Git observations and settled auth writers remain injected.

use std::time::Duration;

use crate::repository_credentials::authority::{CredentialFuture, RepositoryCredentialTransport};
use crate::repository_credentials::{
    BoundGitlabRequestCredentials, RepositoryConnectionBinding, RepositoryConnectionDirectory,
    RepositoryCredentialError, RepositoryCredentialSource, RepositoryCredentialUse,
    RepositoryMutationKind, RepositorySecretReader, RepositorySecretRequest,
    RepositorySecretSnapshot, SettledCredentialState, VerifiedRepositoryAccount,
};
use intent_sourcecontrol::{
    error::AdmissionUnavailable, gitlab::GitlabCredentialRequest, GitlabDescriptor, GitlabInstance,
    GitlabRequestCredentials, SecretString,
};

use super::*;

const INSTANCE: &str = "https://git.example:8443/gitlab";
const READ_PATH: &str = "projects/team%2Fsub%2Fapp/merge_requests";

#[derive(Default)]
struct Secrets {
    reads: Mutex<Vec<RepositorySecretRequest>>,
    pause: Mutex<Option<(Arc<Notify>, Arc<Notify>)>>,
}

impl RepositorySecretReader for Secrets {
    fn load<'a>(
        &'a self,
        expected: &'a RepositorySecretRequest,
    ) -> CredentialFuture<'a, RepositorySecretSnapshot> {
        Box::pin(async move {
            self.reads.lock().unwrap().push(expected.clone());
            let pause = self.pause.lock().unwrap().take();
            if let Some((entered, release)) = pause {
                entered.notify_one();
                release.notified().await;
            }
            Ok(RepositorySecretSnapshot {
                request: expected.clone(),
                token: SecretString::from(format!("fixture-revision-{}", expected.secret_revision)),
            })
        })
    }
}

struct Bridge {
    rig: Rig,
    directory: Arc<RepositoryConnectionDirectory>,
    descriptor: GitlabDescriptor,
    verified: VerifiedRepositoryAccount,
    secrets: Arc<Secrets>,
}

impl Bridge {
    fn new() -> Self {
        let rig = Rig::new(HostRole::Owner, None);
        let descriptor = GitlabDescriptor::new(GitlabInstance::parse(INSTANCE).unwrap());
        let verified = VerifiedRepositoryAccount::from_verified_user(
            descriptor.clone(),
            71,
            RepositoryCredentialSource::GitlabSecretSlot,
        )
        .unwrap();
        let directory = Arc::new(RepositoryConnectionDirectory::new("daemon-A".to_owned()));
        let binding = settle(
            &directory,
            RepositoryMutationKind::Replace,
            verified.clone(),
        );
        rig.change_root(|facts| {
            facts.preparation.source.connection = Some(binding.scope.clone());
            facts.preparation.target.connection = Some(binding.scope.clone());
            facts.push_destinations = facts.fetch_destinations.clone();
            facts.credential_requests = vec![
                RepositoryAuthorityRequest {
                    execution: facts.preparation.scope.clone(),
                    target: facts.preparation.source.repository.clone(),
                    connection: binding.scope.clone(),
                    use_kind: RepositoryCredentialUse::NativePush,
                    allowed_transport: RepositoryCredentialTransport::GitHttps(
                        facts.push_destinations.clone(),
                    ),
                },
                RepositoryAuthorityRequest {
                    execution: facts.preparation.scope.clone(),
                    target: facts.preparation.target.repository.clone(),
                    connection: binding.scope,
                    use_kind: RepositoryCredentialUse::NativeReviewCreate,
                    allowed_transport: RepositoryCredentialTransport::GitlabApi(descriptor.clone()),
                },
            ];
        });
        Self {
            rig,
            directory,
            descriptor,
            verified,
            secrets: Arc::new(Secrets::default()),
        }
    }

    async fn create(&self) -> (RepositoryOperationAdmission, RepositoryDispatchStamp) {
        let operation = self
            .rig
            .capture(vec![NativeReviewStage::CreatePr])
            .await
            .unwrap();
        let stamp = start(&operation, NativeReviewStage::CreatePr).await;
        (operation, stamp)
    }

    fn adapter(&self, stamp: &RepositoryDispatchStamp) -> BoundGitlabRequestCredentials {
        let (request, authority) = stamp.credential_authority().unwrap();
        let admission = self
            .directory
            .admit(&self.directory.binding().unwrap(), request, authority)
            .unwrap();
        BoundGitlabRequestCredentials::new(
            self.directory.clone(),
            admission,
            self.secrets.clone(),
            Duration::from_secs(2),
        )
        .unwrap()
    }

    async fn token(
        &self,
        callback: &BoundGitlabRequestCredentials,
        writing: bool,
    ) -> intent_sourcecontrol::Result<SecretString> {
        callback
            .token_for_request(
                self.descriptor.instance(),
                GitlabCredentialRequest::direct(&self.descriptor, READ_PATH, writing),
            )
            .await
    }
}

fn settle(
    directory: &RepositoryConnectionDirectory,
    kind: RepositoryMutationKind,
    verified: VerifiedRepositoryAccount,
) -> RepositoryConnectionBinding {
    let ticket = directory.reserve_mutation(kind).unwrap();
    directory.begin_mutation(&ticket).unwrap();
    directory
        .finish_mutation(&ticket, SettledCredentialState::Verified(verified))
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn actual_callback_revalidates_original_caller_for_read_and_create() {
    let bridge = Bridge::new();
    let (operation, stamp) = bridge.create().await;
    let callback = bridge.adapter(&stamp);
    let before = bridge.rig.source.reads.load(Ordering::SeqCst);
    with_caller(Caller::Daemon, async {
        assert!(bridge.token(&callback, false).await.is_ok());
        assert!(bridge.token(&callback, true).await.is_ok());
    })
    .await;
    assert_eq!(bridge.rig.source.reads.load(Ordering::SeqCst), before + 2);
    assert_eq!(bridge.secrets.reads.lock().unwrap().len(), 2);
    assert!(operation.execution().unwrap().git_receipts.is_empty());
    assert!(matches!(
        operation.execution().unwrap().publication,
        NativeReviewPublication::Unknown { .. }
    ));
    drop(stamp);
    assert!(matches!(
        bridge.token(&callback, false).await,
        Err(intent_sourcecontrol::Error::AdmissionRetired)
    ));
    assert!(matches!(
        operation.execution().unwrap().outcome,
        NativeReviewOutcome::Uncertain { .. }
    ));
}

#[tokio::test]
async fn same_binding_refresh_reacquires_the_new_secret_revision() {
    let bridge = Bridge::new();
    let (_, stamp) = bridge.create().await;
    let callback = bridge.adapter(&stamp);
    assert!(bridge.token(&callback, false).await.is_ok());
    let original = bridge.directory.binding().unwrap();
    let refreshed = settle(
        &bridge.directory,
        RepositoryMutationKind::Refresh,
        bridge.verified.clone(),
    );
    assert_eq!(refreshed, original);
    bridge.directory.set_child_policy(&original, true).unwrap();
    bridge.directory.set_child_policy(&original, false).unwrap();
    assert!(bridge.token(&callback, true).await.is_ok());
    let reads = bridge.secrets.reads.lock().unwrap();
    assert_eq!(reads[0].binding, reads[1].binding);
    assert!(reads[1].secret_revision > reads[0].secret_revision);
    assert!(bridge.rig.retirement.check_current().is_ok());
}

#[tokio::test]
async fn same_account_replacement_does_not_rebind_an_existing_stage() {
    let bridge = Bridge::new();
    let (_, stamp) = bridge.create().await;
    let callback = bridge.adapter(&stamp);
    assert!(bridge.token(&callback, false).await.is_ok());
    let original = bridge.directory.binding().unwrap();
    let replacement = settle(
        &bridge.directory,
        RepositoryMutationKind::Replace,
        bridge.verified.clone(),
    );
    assert_eq!(replacement.account, original.account);
    assert_ne!(replacement.scope, original.scope);
    assert!(matches!(
        bridge.token(&callback, true).await,
        Err(intent_sourcecontrol::Error::AdmissionRetired)
    ));
    assert_eq!(bridge.secrets.reads.lock().unwrap().len(), 1);
    bridge.rig.change_root(|facts| {
        facts.preparation.target.connection = Some(replacement.scope);
    });
    let (request, authority) = stamp.credential_authority().unwrap();
    assert!(matches!(
        authority.revalidate(&request).await,
        Err(RepositoryCredentialError::Retired)
    ));
}

#[tokio::test]
async fn private_requests_cannot_upgrade_purpose_target_account_or_transport() {
    let bridge = Bridge::new();
    let (_, stamp) = bridge.create().await;
    let (request, authority) = stamp.credential_authority().unwrap();
    for variant in 0..8 {
        let mut wrong = request.clone();
        match variant {
            0 => wrong.use_kind = RepositoryCredentialUse::NativeRead,
            1 => wrong.use_kind = RepositoryCredentialUse::NativePush,
            2 => wrong.use_kind = RepositoryCredentialUse::ChildGit,
            3 => wrong.execution.daemon_id = "local-B".to_owned(),
            4 => wrong.target.project_path = "other/project".to_owned(),
            5 => wrong.connection.account_id = "replacement".to_owned(),
            6 => wrong.connection.connection_generation += 1,
            _ => {
                wrong.allowed_transport = RepositoryCredentialTransport::GitlabApi(
                    GitlabDescriptor::with_loopback_endpoint(
                        bridge.descriptor.instance().clone(),
                        "http://127.0.0.1:4321/other",
                    )
                    .unwrap(),
                );
            }
        }
        assert!(matches!(
            authority.revalidate(&wrong).await,
            Err(RepositoryCredentialError::BoundaryMismatch)
        ));
    }
    assert!(authority.revalidate(&request).await.is_ok());
    assert!(bridge.secrets.reads.lock().unwrap().is_empty());
}

#[tokio::test]
async fn active_callback_stops_on_original_bearer_role_and_remove_readd_changes() {
    for variant in 0..4 {
        let bridge = Bridge::new();
        let (_, stamp) = bridge.create().await;
        let callback = bridge.adapter(&stamp);
        bridge.rig.change_authority(|facts| match variant {
            0 => facts.credential.as_mut().unwrap().revoked_at = Some("now".to_owned()),
            1 => facts.credential.as_mut().unwrap().token_hash = "replacement".to_owned(),
            2 => {
                facts.caller = Caller::Wire {
                    principal_id: PrincipalId::from("principal-A"),
                    host_role: HostRole::Guest,
                }
            }
            _ => advance_injected_provenance(facts),
        });
        assert!(matches!(
            bridge.token(&callback, false).await,
            Err(intent_sourcecontrol::Error::AdmissionUnavailable(
                AdmissionUnavailable::AuthorityDenied
            ))
        ));
        assert!(bridge.rig.retirement.check_current().is_err());
        assert!(bridge.directory.binding().is_ok());
    }
}

#[tokio::test]
async fn active_callback_rechecks_private_root_and_all_effective_destinations() {
    for variant in 0..7 {
        let bridge = Bridge::new();
        let (_, stamp) = bridge.create().await;
        let callback = bridge.adapter(&stamp);
        bridge.rig.change_root(|facts| match variant {
            0 => facts.source_ref = "refs/heads/other".to_owned(),
            1 => facts.worktree_path = PathBuf::from("/other/worktree"),
            2 => facts
                .fetch_destinations
                .push("https://other/project.git".to_owned()),
            3 => facts
                .push_destinations
                .push("https://backup/project.git".to_owned()),
            4 => facts.preparation.local_head_sha = Some("new-head".to_owned()),
            5 => facts.preparation.scope.authority_generation += 1,
            _ => {
                facts.credential_requests[1].allowed_transport =
                    RepositoryCredentialTransport::GitlabApi(
                        GitlabDescriptor::with_loopback_endpoint(
                            bridge.descriptor.instance().clone(),
                            "http://127.0.0.1:4567/replaced",
                        )
                        .unwrap(),
                    );
            }
        });
        assert!(matches!(
            bridge.token(&callback, false).await,
            Err(intent_sourcecontrol::Error::AdmissionRetired)
        ));
        assert!(bridge.directory.binding().is_ok());
    }
}

#[tokio::test]
async fn commit_and_unadmitted_create_cannot_mint_forge_authority() {
    let bridge = Bridge::new();
    let commit = bridge
        .rig
        .capture(vec![NativeReviewStage::Commit])
        .await
        .unwrap();
    let stamp = start(&commit, NativeReviewStage::Commit).await;
    assert!(matches!(
        stamp.credential_authority(),
        Err(AdmissionError::Denied)
    ));
    let local = Rig::new(HostRole::Owner, None);
    let operation = local
        .capture(vec![NativeReviewStage::CreatePr])
        .await
        .unwrap();
    let stamp = start(&operation, NativeReviewStage::CreatePr).await;
    assert!(matches!(
        stamp.credential_authority(),
        Err(AdmissionError::Denied)
    ));
    assert!(bridge.secrets.reads.lock().unwrap().is_empty());
}

#[tokio::test]
async fn invalid_private_admission_never_fills_authority_from_display_urls() {
    for variant in 0..6 {
        let bridge = Bridge::new();
        bridge.rig.change_root(|facts| match variant {
            0 => facts.credential_requests[0].use_kind = RepositoryCredentialUse::NativeRead,
            1 => facts.credential_requests[1].use_kind = RepositoryCredentialUse::ChildGit,
            2 => {
                facts.credential_requests[0].allowed_transport =
                    RepositoryCredentialTransport::GitHttps(vec![]);
            }
            3 => facts
                .push_destinations
                .push("https://extra/destination.git".to_owned()),
            4 => facts.credential_requests[1].connection.account_id = "other".to_owned(),
            _ => facts
                .credential_requests
                .push(facts.credential_requests[1].clone()),
        });
        assert!(matches!(
            bridge.rig.capture(vec![NativeReviewStage::CreatePr]).await,
            Err(AdmissionError::InvalidPlan)
        ));
    }
}

#[tokio::test]
async fn final_consuming_fence_stops_retired_completed_and_older_revision_handles() {
    for variant in 0..3 {
        let bridge = Bridge::new();
        let (_, stamp) = bridge.create().await;
        let (request, authority) = stamp.credential_authority().unwrap();
        let fence = authority.revalidate(&request).await.unwrap();
        match variant {
            0 => bridge.rig.retirement.retire(),
            1 => {
                classify_repository_completion(
                    stamp,
                    RepositoryCompletion::Reused(actual_review()),
                )
                .unwrap();
            }
            _ => {
                bridge.rig.change_root(|facts| {
                    facts.preparation.context_revision =
                        RepositoryContextRevision::new("daemon-boot-1", 9_007_199_254_740_994);
                });
                let newer = authority.revalidate(&request).await.unwrap();
                let mut fresh_calls = 0;
                newer
                    .dispatch(&mut || {
                        fresh_calls += 1;
                        Ok(())
                    })
                    .unwrap();
                assert_eq!(fresh_calls, 1);
            }
        }
        let mut calls = 0;
        assert!(matches!(
            fence.dispatch(&mut || {
                calls += 1;
                Ok(())
            }),
            Err(RepositoryCredentialError::Retired)
        ));
        assert_eq!(calls, 0);
    }
}

#[tokio::test]
async fn original_legacy_lease_is_held_only_through_the_consuming_fence() {
    let mut bridge = Bridge::new();
    let legacy = Arc::new(LegacyAuthority {
        valid: AtomicBool::new(true),
        calls: AtomicUsize::new(0),
        leases: Arc::new(AtomicUsize::new(0)),
    });
    bridge.rig.wire = Some(WireCredential::Legacy {
        principal_id: PrincipalId::from("principal-A"),
        authority: legacy.clone(),
    });
    let (_, stamp) = bridge.create().await;
    let (request, authority) = stamp.credential_authority().unwrap();
    let fence = authority.revalidate(&request).await.unwrap();
    assert_eq!(legacy.leases.load(Ordering::SeqCst), 1);
    fence
        .dispatch(&mut || {
            assert_eq!(legacy.leases.load(Ordering::SeqCst), 1);
            Ok(())
        })
        .unwrap();
    assert_eq!(legacy.leases.load(Ordering::SeqCst), 0);
    legacy.valid.store(false, Ordering::SeqCst);
    assert!(matches!(
        authority.revalidate(&request).await,
        Err(RepositoryCredentialError::AuthorityDenied)
    ));
}

#[tokio::test]
async fn secret_wait_cannot_outlive_stage_retirement_or_directory_replacement() {
    for variant in 0..3 {
        let bridge = Arc::new(Bridge::new());
        let (operation, stamp) = bridge.create().await;
        let callback = bridge.adapter(&stamp);
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        *bridge.secrets.pause.lock().unwrap() = Some((entered.clone(), release.clone()));
        let owner = bridge.clone();
        let pending = tokio::spawn(async move { owner.token(&callback, false).await });
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        match variant {
            0 => bridge.rig.retirement.retire(),
            1 => {
                classify_repository_completion(
                    stamp,
                    RepositoryCompletion::Reused(actual_review()),
                )
                .unwrap();
            }
            _ => {
                settle(
                    &bridge.directory,
                    RepositoryMutationKind::Replace,
                    bridge.verified.clone(),
                );
            }
        }
        release.notify_one();
        assert!(matches!(
            pending.await.unwrap(),
            Err(intent_sourcecontrol::Error::AdmissionRetired)
        ));
        if variant == 1 {
            assert!(matches!(
                operation.execution().unwrap().outcome,
                NativeReviewOutcome::Reused { .. }
            ));
        }
    }
}

#[tokio::test]
async fn retirement_during_authority_read_cannot_release_a_secret() {
    let bridge = Arc::new(Bridge::new());
    let (_, stamp) = bridge.create().await;
    let callback = bridge.adapter(&stamp);
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    *bridge.rig.source.pause.lock().unwrap() = Some((entered.clone(), release.clone()));
    let owner = bridge.clone();
    let pending = tokio::spawn(async move { owner.token(&callback, false).await });
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    bridge.rig.retirement.retire();
    release.notify_one();
    assert!(matches!(
        pending.await.unwrap(),
        Err(intent_sourcecontrol::Error::AdmissionRetired)
    ));
}

#[tokio::test]
async fn transient_authority_failure_is_local_and_does_not_retire_the_account() {
    let bridge = Bridge::new();
    let (_, stamp) = bridge.create().await;
    let callback = bridge.adapter(&stamp);
    bridge.rig.source.unavailable.store(true, Ordering::SeqCst);
    assert!(matches!(
        bridge.token(&callback, false).await,
        Err(intent_sourcecontrol::Error::AdmissionUnavailable(
            AdmissionUnavailable::AuthorityUnavailable
        ))
    ));
    assert!(bridge.rig.retirement.check_current().is_ok());
    assert!(bridge.directory.binding().is_ok());
    bridge.rig.source.unavailable.store(false, Ordering::SeqCst);
    assert!(bridge.token(&callback, false).await.is_ok());
}

#[tokio::test]
async fn push_credential_release_is_not_a_receipt_and_completed_push_survives_retirement() {
    let bridge = Bridge::new();
    let operation = bridge
        .rig
        .capture(vec![NativeReviewStage::Push, NativeReviewStage::CreatePr])
        .await
        .unwrap();
    let stamp = start(&operation, NativeReviewStage::Push).await;
    let (request, authority) = stamp.credential_authority().unwrap();
    let admission = bridge
        .directory
        .admit(&bridge.directory.binding().unwrap(), request, authority)
        .unwrap();
    assert!(bridge
        .directory
        .acquire_exact(&admission, bridge.secrets.as_ref(), Duration::from_secs(2))
        .await
        .is_ok());
    assert!(operation.execution().unwrap().git_receipts.is_empty());
    bridge.rig.retirement.retire();
    let result = classify_repository_completion(
        stamp,
        RepositoryCompletion::Pushed {
            sha: "local-B".to_owned(),
        },
    )
    .unwrap();
    assert_eq!(
        result.git_receipts,
        vec![NativeReviewGitReceipt::Push {
            pushed_sha: "local-B".to_owned()
        }]
    );
    assert!(matches!(
        result.publication,
        NativeReviewPublication::Unknown { .. }
    ));
    assert!(matches!(
        revalidate_repository_stage(&operation, NativeReviewStage::CreatePr).await,
        Err(AdmissionError::Retired)
    ));
    assert!(matches!(
        bridge
            .directory
            .acquire_exact(&admission, bridge.secrets.as_ref(), Duration::from_secs(2))
            .await,
        Err(RepositoryCredentialError::Retired)
    ));
}
