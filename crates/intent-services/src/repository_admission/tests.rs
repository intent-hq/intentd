//! Deterministic engine tests with injected durable/Git/writer facts.
//! No real entry points, auth writers, store permissions or Git effects are wired.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use intent_core::caller::{
    with_caller, with_wire_credential, Caller, LegacyCredentialAuthority, WireCredential,
};
use intent_core::{
    AgentId, HostRole, PrincipalCredential, PrincipalId, WorkspaceId, WorkspaceRole,
};
use tokio::sync::Notify;

use super::*;

struct FakeSource {
    authority: Mutex<RepositoryAuthorityFacts>,
    observed: Mutex<RepositoryOperationFacts>,
    reads: AtomicUsize,
    unavailable: AtomicBool,
    pause: Mutex<Option<(Arc<Notify>, Arc<Notify>)>>,
}

impl RepositoryAuthoritySource for FakeSource {
    fn read<'a>(
        &'a self,
        original: &'a OriginalRepositoryCaller,
        workspace: &'a WorkspaceId,
    ) -> BoxFuture<'a, AdmissionResult<RepositoryAuthorityFacts>> {
        Box::pin(async move {
            self.reads.fetch_add(1, Ordering::SeqCst);
            let pause = self.pause.lock().unwrap().take();
            if let Some((entered, release)) = pause {
                entered.notify_one();
                release.notified().await;
            }
            if self.unavailable.load(Ordering::SeqCst) {
                return Err(AdmissionError::Unavailable);
            }
            // Verify the read is for the original entry, never the ambient
            // caller of a later spawned task or a replacement bearer.
            if let Some(credential) = original.wire_credential() {
                assert_eq!(
                    credential.principal_id(),
                    original.caller().principal_id().unwrap()
                );
            }
            let facts = self.authority.lock().unwrap().clone();
            assert_eq!(&facts.workspace, workspace);
            Ok(facts)
        })
    }
}

impl RepositoryOperationSource for FakeSource {
    fn observe<'a>(
        &'a self,
        _original: &'a RepositoryOperationFacts,
    ) -> BoxFuture<'a, AdmissionResult<RepositoryOperationFacts>> {
        Box::pin(async move { Ok(self.observed.lock().unwrap().clone()) })
    }
}

struct Rig {
    caller: Caller,
    wire: Option<WireCredential>,
    source: Arc<FakeSource>,
    retirement: RepositoryRetirement,
}

fn native_fixture() -> serde_json::Value {
    serde_json::from_str(include_str!(
        "../../../intent-core/tests/fixtures/native_review_v1.json"
    ))
    .unwrap()
}

fn bound_facts() -> RepositoryOperationFacts {
    let preparation: NativeReviewPreparation =
        serde_json::from_value(native_fixture()["prepare"]["reviewPreparation"].clone()).unwrap();
    RepositoryOperationFacts {
        preparation,
        worktree_path: PathBuf::from("/admitted/worktree"),
        git_dir: PathBuf::from("/admitted/git/worktrees/one"),
        common_dir: PathBuf::from("/admitted/git"),
        source_ref: "refs/heads/feature".to_owned(),
        staging_fingerprint: Some("staged-original".to_owned()),
        fetch_destinations: vec!["https://git.example:8443/gitlab/team/sub/app.git".to_owned()],
        push_destinations: vec![
            "ssh://git@git.example/team/sub/app.git".to_owned(),
            "ssh://backup/team/sub/app.git".to_owned(),
        ],
        credential_requests: Vec::new(),
    }
}

#[path = "credential_bridge_tests.rs"]
mod credential_bridge_tests;

impl Rig {
    fn new(role: HostRole, workspace_role: Option<WorkspaceRole>) -> Self {
        let principal = PrincipalId::from("principal-A");
        let caller = Caller::Wire {
            principal_id: principal.clone(),
            host_role: role,
        };
        let facts = bound_facts();
        let authority = RepositoryAuthorityFacts {
            caller: caller.clone(),
            workspace: facts.preparation.root.workspace_id.clone(),
            workspace_exists: true,
            primary_principal_id: Some(if role == HostRole::Owner {
                principal.clone()
            } else {
                PrincipalId::from("primary")
            }),
            workspace_role,
            credential: Some(PrincipalCredential {
                token_hash: "original-hash".to_owned(),
                principal_id: principal.clone(),
                created_at: "2026-09-27T00:00:00Z".to_owned(),
                last_used_at: None,
                revoked_at: None,
            }),
            provenance: RepositoryAuthorityProvenance::Injected(9_007_199_254_740_993),
            internal_stages: Vec::new(),
        };
        Self {
            caller,
            wire: Some(WireCredential::Principal {
                principal_id: principal,
                token_hash: "original-hash".to_owned(),
            }),
            source: Arc::new(FakeSource {
                authority: Mutex::new(authority),
                observed: Mutex::new(facts),
                reads: AtomicUsize::new(0),
                unavailable: AtomicBool::new(false),
                pause: Mutex::new(None),
            }),
            retirement: RepositoryRetirement::default(),
        }
    }

    async fn original(&self, entry: RepositoryEntry) -> AdmissionResult<OriginalRepositoryCaller> {
        with_caller(
            self.caller.clone(),
            with_wire_credential(self.wire.clone(), async {
                OriginalRepositoryCaller::capture(entry)
            }),
        )
        .await
    }

    async fn capture(
        &self,
        stages: Vec<NativeReviewStage>,
    ) -> AdmissionResult<RepositoryOperationAdmission> {
        self.capture_as(stages, RepositoryEntry::Bearer, "request-one")
            .await
    }

    async fn capture_as(
        &self,
        stages: Vec<NativeReviewStage>,
        entry: RepositoryEntry,
        request: &str,
    ) -> AdmissionResult<RepositoryOperationAdmission> {
        let original = self.original(entry).await?;
        let facts = self.source.observed.lock().unwrap().clone();
        capture_repository_operation(
            original,
            request.to_owned(),
            facts,
            stages,
            self.source.clone(),
            self.retirement.clone(),
        )
        .await
    }

    fn change_authority(&self, change: impl FnOnce(&mut RepositoryAuthorityFacts)) {
        change(&mut self.source.authority.lock().unwrap());
    }

    fn change_root(&self, change: impl FnOnce(&mut RepositoryOperationFacts)) {
        change(&mut self.source.observed.lock().unwrap());
    }
}

async fn start(
    operation: &RepositoryOperationAdmission,
    stage: NativeReviewStage,
) -> RepositoryDispatchStamp {
    begin_repository_stage(revalidate_repository_stage(operation, stage).await.unwrap()).unwrap()
}

fn actual_review() -> Box<NativeReviewDetails> {
    Box::new(
        serde_json::from_value(
            native_fixture()["execute"]["reviewExecution"]["outcome"]["review"].clone(),
        )
        .unwrap(),
    )
}

#[tokio::test]
async fn absent_or_mismatched_provenance_never_becomes_local_owner() {
    assert!(matches!(
        OriginalRepositoryCaller::capture(RepositoryEntry::AdmittedLocal),
        Err(AdmissionError::Denied)
    ));
    let mut rig = Rig::new(HostRole::Owner, None);
    rig.wire = None;
    assert!(matches!(
        rig.original(RepositoryEntry::Bearer).await,
        Err(AdmissionError::Denied)
    ));
    assert!(rig
        .capture_as(
            vec![NativeReviewStage::Commit],
            RepositoryEntry::AdmittedLocal,
            "local"
        )
        .await
        .is_ok());
    rig.wire = Some(WireCredential::Principal {
        principal_id: PrincipalId::from("other"),
        token_hash: "other".to_owned(),
    });
    assert!(matches!(
        rig.original(RepositoryEntry::Bearer).await,
        Err(AdmissionError::Denied)
    ));
    assert!(matches!(
        rig.original(RepositoryEntry::AdmittedLocal).await,
        Err(AdmissionError::Denied)
    ));
}

#[tokio::test]
async fn owner_member_guest_permissions_remain_operation_specific() {
    for (role, workspace_role, git_allowed, create_allowed) in [
        (HostRole::Owner, None, true, true),
        (HostRole::Member, None, true, true),
        (HostRole::Guest, Some(WorkspaceRole::Owner), true, false),
        (
            HostRole::Guest,
            Some(WorkspaceRole::Collaborator),
            true,
            false,
        ),
        (HostRole::Guest, None, false, false),
    ] {
        let rig = Rig::new(role, workspace_role);
        for (stage, expected) in [
            (NativeReviewStage::Commit, git_allowed),
            (NativeReviewStage::Push, git_allowed),
            (NativeReviewStage::CreatePr, create_allowed),
        ] {
            assert_eq!(
                rig.capture(vec![stage]).await.is_ok(),
                expected,
                "{role:?} {workspace_role:?} {stage:?}"
            );
        }
    }
}

#[tokio::test]
async fn inherited_member_permission_excludes_chief_and_missing_workspaces() {
    let rig = Rig::new(HostRole::Member, Some(WorkspaceRole::Owner));
    rig.change_root(|facts| facts.preparation.root.workspace_id = WorkspaceId::chief());
    rig.change_authority(|facts| facts.workspace = WorkspaceId::chief());
    assert!(matches!(
        rig.capture(vec![NativeReviewStage::Commit]).await,
        Err(AdmissionError::Denied)
    ));
    let rig = Rig::new(HostRole::Owner, None);
    rig.change_authority(|facts| facts.workspace_exists = false);
    assert!(matches!(
        rig.capture(vec![NativeReviewStage::Commit]).await,
        Err(AdmissionError::Denied)
    ));
}

#[tokio::test]
async fn internal_callers_keep_their_own_explicit_permissions() {
    for (caller, entry) in [
        (
            Caller::Agent {
                agent_id: AgentId::from("agent-original"),
            },
            RepositoryEntry::AgentCallback,
        ),
        (Caller::Daemon, RepositoryEntry::DaemonTask),
    ] {
        let mut rig = Rig::new(HostRole::Owner, None);
        rig.caller = caller.clone();
        rig.wire = None;
        rig.change_authority(|facts| {
            facts.caller = caller;
            facts.internal_stages = vec![NativeReviewStage::Commit];
            facts.credential = None;
        });
        assert!(rig
            .capture_as(vec![NativeReviewStage::Commit], entry, "internal")
            .await
            .is_ok());
        assert!(matches!(
            rig.capture_as(vec![NativeReviewStage::CreatePr], entry, "internal")
                .await,
            Err(AdmissionError::Denied)
        ));
        assert!(matches!(
            rig.original(RepositoryEntry::AdmittedLocal).await,
            Err(AdmissionError::Denied)
        ));
    }
}

#[tokio::test]
async fn captured_personal_bearer_survives_task_change_without_impersonation() {
    let rig = Rig::new(HostRole::Guest, Some(WorkspaceRole::Collaborator));
    let operation = rig.capture(vec![NativeReviewStage::Commit]).await.unwrap();
    let checked = tokio::spawn(async move {
        with_caller(
            Caller::Daemon,
            revalidate_repository_stage(&operation, NativeReviewStage::Commit),
        )
        .await
    })
    .await
    .unwrap()
    .unwrap();
    let result = classify_repository_completion(
        begin_repository_stage(checked).unwrap(),
        RepositoryCompletion::Committed {
            hash: "local-C".to_owned(),
            staging_after: None,
        },
    )
    .unwrap();
    assert_eq!(result.git_receipts.len(), 1);
    assert_eq!(rig.source.reads.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn original_owner_bearer_and_primary_are_rechecked_each_time() {
    for change in 0..4 {
        let rig = Rig::new(HostRole::Owner, None);
        let operation = rig.capture(vec![NativeReviewStage::Commit]).await.unwrap();
        rig.change_authority(|facts| match change {
            0 => facts.credential.as_mut().unwrap().revoked_at = Some("revoked".to_owned()),
            1 => facts.credential.as_mut().unwrap().token_hash = "replacement-hash".to_owned(),
            2 => facts.credential.as_mut().unwrap().principal_id = PrincipalId::from("other"),
            _ => facts.primary_principal_id = Some(PrincipalId::from("new-primary")),
        });
        assert!(matches!(
            revalidate_repository_stage(&operation, NativeReviewStage::Commit).await,
            Err(AdmissionError::Denied)
        ));
        assert_eq!(rig.retirement.check_current(), Err(AdmissionError::Retired));
    }
}

#[tokio::test]
async fn role_changes_and_remove_readd_do_not_reuse_old_authority() {
    for change in 0..3 {
        let rig = Rig::new(HostRole::Member, None);
        let operation = rig.capture(vec![NativeReviewStage::Commit]).await.unwrap();
        rig.change_authority(|facts| match change {
            0 => {
                facts.caller = Caller::Wire {
                    principal_id: PrincipalId::from("principal-A"),
                    host_role: HostRole::Owner,
                }
            }
            1 => advance_injected_provenance(facts),
            _ => facts.workspace_role = Some(WorkspaceRole::Owner),
        });
        assert!(matches!(
            revalidate_repository_stage(&operation, NativeReviewStage::Commit).await,
            Err(AdmissionError::Denied)
        ));
    }
}

struct LegacyAuthority {
    valid: AtomicBool,
    calls: AtomicUsize,
    leases: Arc<AtomicUsize>,
}
struct HeldLease(Arc<AtomicUsize>);
impl Drop for HeldLease {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}
impl LegacyCredentialAuthority for LegacyAuthority {
    fn authorize(&self) -> BoxFuture<'_, intent_core::Result<CredentialLease>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if !self.valid.load(Ordering::SeqCst) {
                return Err(intent_core::Error::Forbidden(
                    "retired original bearer".to_owned(),
                ));
            }
            self.leases.fetch_add(1, Ordering::SeqCst);
            let lease: CredentialLease = Box::new(HeldLease(self.leases.clone()));
            Ok(lease)
        })
    }
}

#[tokio::test]
async fn original_legacy_authority_is_used_and_lease_drops_before_effect() {
    let mut rig = Rig::new(HostRole::Owner, None);
    let legacy = Arc::new(LegacyAuthority {
        valid: AtomicBool::new(true),
        calls: AtomicUsize::new(0),
        leases: Arc::new(AtomicUsize::new(0)),
    });
    rig.wire = Some(WireCredential::Legacy {
        principal_id: PrincipalId::from("principal-A"),
        authority: legacy.clone(),
    });
    let operation = rig
        .capture(vec![NativeReviewStage::Commit, NativeReviewStage::Push])
        .await
        .unwrap();
    assert_eq!(legacy.leases.load(Ordering::SeqCst), 0);
    let checked = revalidate_repository_stage(&operation, NativeReviewStage::Commit)
        .await
        .unwrap();
    assert_eq!(legacy.leases.load(Ordering::SeqCst), 1);
    let stamp = begin_repository_stage(checked).unwrap();
    assert_eq!(legacy.leases.load(Ordering::SeqCst), 0);
    classify_repository_completion(
        stamp,
        RepositoryCompletion::Committed {
            hash: "local-C".to_owned(),
            staging_after: None,
        },
    )
    .unwrap();
    rig.change_root(|facts| {
        facts.preparation.local_head_sha = Some("local-C".to_owned());
        facts.staging_fingerprint = None;
    });
    legacy.valid.store(false, Ordering::SeqCst);
    assert!(matches!(
        revalidate_repository_stage(&operation, NativeReviewStage::Push).await,
        Err(AdmissionError::Denied)
    ));
    assert_eq!(legacy.calls.load(Ordering::SeqCst), 3);
    assert_eq!(operation.execution().unwrap().git_receipts.len(), 1);
}

#[tokio::test]
async fn all_root_ref_transport_account_and_epoch_changes_stop_queued_work() {
    for change in 0..13 {
        let rig = Rig::new(HostRole::Member, None);
        let operation = rig
            .capture(vec![NativeReviewStage::CreatePr])
            .await
            .unwrap();
        rig.change_root(|facts| match change {
            0 => facts.worktree_path = PathBuf::from("/other"),
            1 => facts.git_dir = PathBuf::from("/other/git"),
            2 => facts.common_dir = PathBuf::from("/other/common"),
            3 => facts.source_ref = "refs/heads/other".to_owned(),
            4 => facts.preparation.local_head_sha = Some("unreceipted-C".to_owned()),
            5 => facts.staging_fingerprint = None,
            6 => facts.push_destinations[1] = "ssh://replacement/project".to_owned(),
            7 => facts
                .fetch_destinations
                .push("https://other/project".to_owned()),
            8 => {
                facts
                    .preparation
                    .source
                    .connection
                    .as_mut()
                    .unwrap()
                    .account_id = "account-B".to_owned();
            }
            9 => {
                facts
                    .preparation
                    .target
                    .connection
                    .as_mut()
                    .unwrap()
                    .connection_id = "replacement".to_owned();
            }
            10 => facts.preparation.scope.daemon_id = "daemon-B".to_owned(),
            11 => {
                facts.preparation.context_revision =
                    RepositoryContextRevision::new("restarted", 9_007_199_254_740_993);
            }
            _ => facts.preparation.worktree_id = "replacement-worktree".to_owned(),
        });
        assert!(
            matches!(
                revalidate_repository_stage(&operation, NativeReviewStage::CreatePr).await,
                Err(AdmissionError::BindingChanged)
            ),
            "change {change}"
        );
        assert_eq!(rig.retirement.check_current(), Err(AdmissionError::Retired));
    }
}

#[tokio::test]
async fn refreshed_inventory_sequence_is_exact_and_never_orders_across_epoch() {
    let rig = Rig::new(HostRole::Member, None);
    let operation = rig.capture(vec![NativeReviewStage::Commit]).await.unwrap();
    rig.change_root(|facts| {
        facts.preparation.context_revision =
            RepositoryContextRevision::new("daemon-boot-1", 9_007_199_254_740_994);
    });
    drop(
        revalidate_repository_stage(&operation, NativeReviewStage::Commit)
            .await
            .unwrap(),
    );
    rig.change_root(|facts| {
        facts.preparation.context_revision =
            RepositoryContextRevision::new("daemon-boot-1", 9_007_199_254_740_993);
    });
    assert!(matches!(
        revalidate_repository_stage(&operation, NativeReviewStage::Commit).await,
        Err(AdmissionError::BindingChanged)
    ));
}

#[tokio::test]
async fn retirement_during_durable_read_cannot_produce_a_live_stage_stamp() {
    let rig = Rig::new(HostRole::Member, None);
    let operation = rig.capture(vec![NativeReviewStage::Commit]).await.unwrap();
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    *rig.source.pause.lock().unwrap() = Some((entered.clone(), release.clone()));
    let worker = tokio::spawn(async move {
        revalidate_repository_stage(&operation, NativeReviewStage::Commit).await
    });
    entered.notified().await;
    rig.retirement.retire();
    release.notify_one();
    assert!(matches!(
        worker.await.unwrap(),
        Err(AdmissionError::Retired)
    ));
}

#[tokio::test]
async fn leaf_fence_excludes_retired_checked_work_and_never_revives_on_replacement() {
    let rig = Rig::new(HostRole::Member, None);
    let operation = rig.capture(vec![NativeReviewStage::Commit]).await.unwrap();
    let checked = revalidate_repository_stage(&operation, NativeReviewStage::Commit)
        .await
        .unwrap();
    rig.retirement.retire();
    let replacement = RepositoryRetirement::default();
    assert!(replacement.check_current().is_ok());
    assert!(matches!(
        begin_repository_stage(checked),
        Err(AdmissionError::Retired)
    ));
    let result = operation
        .fail_before_dispatch(NativeReviewStage::Commit, AdmissionError::Retired)
        .unwrap();
    assert!(matches!(result.outcome, NativeReviewOutcome::Failed { .. }));
    assert!(result.git_receipts.is_empty());
}

#[tokio::test]
async fn multiple_checked_handles_dispatch_once_and_plan_never_retries() {
    let rig = Rig::new(HostRole::Owner, None);
    let operation = rig.capture(vec![NativeReviewStage::Push]).await.unwrap();
    let first = revalidate_repository_stage(&operation, NativeReviewStage::Push)
        .await
        .unwrap();
    let second = revalidate_repository_stage(&operation, NativeReviewStage::Push)
        .await
        .unwrap();
    let stamp = begin_repository_stage(first).unwrap();
    assert!(matches!(
        begin_repository_stage(second),
        Err(AdmissionError::StageOrder)
    ));
    let result = classify_repository_completion(
        stamp,
        RepositoryCompletion::Pushed {
            sha: "local-B".to_owned(),
        },
    )
    .unwrap();
    assert!(matches!(
        result.publication,
        NativeReviewPublication::Unknown { .. }
    ));
    assert!(matches!(
        revalidate_repository_stage(&operation, NativeReviewStage::Push).await,
        Err(AdmissionError::StageOrder)
    ));
}

#[tokio::test]
async fn local_commit_needs_no_forge_connection_and_only_receipt_advances_head() {
    let rig = Rig::new(HostRole::Member, None);
    rig.change_root(|facts| {
        facts.preparation.source.connection = None;
        facts.preparation.target.connection = None;
    });
    let operation = rig.capture(vec![NativeReviewStage::Commit]).await.unwrap();
    let result = classify_repository_completion(
        start(&operation, NativeReviewStage::Commit).await,
        RepositoryCompletion::Committed {
            hash: "local-C".to_owned(),
            staging_after: None,
        },
    )
    .unwrap();
    assert_eq!(
        result.preparation.local_head_sha.as_deref(),
        Some("local-B")
    );
    assert!(
        matches!(result.git_receipts.as_slice(), [NativeReviewGitReceipt::Commit { commit_hash }] if commit_hash == "local-C")
    );
    assert!(
        matches!(result.publication, NativeReviewPublication::Unknown { local_head_sha: Some(ref sha), .. } if sha == "local-C")
    );
}

#[tokio::test]
async fn committed_stage_survives_retirement_while_next_stage_stops() {
    let rig = Rig::new(HostRole::Member, None);
    let operation = rig
        .capture(vec![NativeReviewStage::Commit, NativeReviewStage::Push])
        .await
        .unwrap();
    let stamp = start(&operation, NativeReviewStage::Commit).await;
    rig.retirement.retire();
    classify_repository_completion(
        stamp,
        RepositoryCompletion::Committed {
            hash: "local-C".to_owned(),
            staging_after: None,
        },
    )
    .unwrap();
    assert!(matches!(
        revalidate_repository_stage(&operation, NativeReviewStage::Push).await,
        Err(AdmissionError::Retired)
    ));
    let result = operation
        .fail_before_dispatch(NativeReviewStage::Push, AdmissionError::Retired)
        .unwrap();
    assert_eq!(result.git_receipts.len(), 1);
    assert!(matches!(
        result.outcome,
        NativeReviewOutcome::Failed {
            stage: NativeReviewStage::Push,
            ..
        }
    ));
}

#[tokio::test]
async fn unfinished_dispatched_stage_is_uncertain_even_after_retirement() {
    let rig = Rig::new(HostRole::Owner, None);
    let operation = rig
        .capture(vec![NativeReviewStage::Push, NativeReviewStage::CreatePr])
        .await
        .unwrap();
    let stamp = start(&operation, NativeReviewStage::Push).await;
    rig.retirement.retire();
    drop(stamp);
    let result = operation.execution().unwrap();
    assert!(matches!(
        result.outcome,
        NativeReviewOutcome::Uncertain {
            stage: NativeReviewStage::Push,
            ..
        }
    ));
    assert!(result.git_receipts.is_empty());
    assert!(operation
        .fail_before_dispatch(NativeReviewStage::Push, AdmissionError::Retired)
        .is_err());
}

#[tokio::test]
async fn explicit_partial_failure_and_sent_uncertainty_keep_completed_commit() {
    for uncertain in [false, true] {
        let rig = Rig::new(HostRole::Owner, None);
        let operation = rig
            .capture(vec![NativeReviewStage::Commit, NativeReviewStage::CreatePr])
            .await
            .unwrap();
        classify_repository_completion(
            start(&operation, NativeReviewStage::Commit).await,
            RepositoryCompletion::Committed {
                hash: "local-C".to_owned(),
                staging_after: None,
            },
        )
        .unwrap();
        rig.change_root(|facts| {
            facts.preparation.local_head_sha = Some("local-C".to_owned());
            facts.staging_fingerprint = None;
        });
        let completion = if uncertain {
            RepositoryCompletion::Uncertain {
                message: "Response lost after dispatch".to_owned(),
            }
        } else {
            RepositoryCompletion::Failed {
                code: None,
                message: "Provider confirmed rejection".to_owned(),
            }
        };
        let result = classify_repository_completion(
            start(&operation, NativeReviewStage::CreatePr).await,
            completion,
        )
        .unwrap();
        assert_eq!(result.git_receipts.len(), 1);
        assert_eq!(
            matches!(result.outcome, NativeReviewOutcome::Uncertain { .. }),
            uncertain
        );
        assert!(matches!(
            revalidate_repository_stage(&operation, NativeReviewStage::CreatePr).await,
            Err(AdmissionError::StageOrder)
        ));
    }
}

#[tokio::test]
async fn actual_created_reused_metadata_never_implies_local_publication() {
    for reuse in [false, true] {
        let rig = Rig::new(HostRole::Owner, None);
        let operation = rig
            .capture(vec![NativeReviewStage::CreatePr])
            .await
            .unwrap();
        let mut review = actual_review();
        review.title = "Actual remote title".to_owned();
        review.draft = Some(true);
        review.head_sha = Some("remote-A".to_owned());
        let expected = review.clone();
        let outcome = if reuse {
            RepositoryCompletion::Reused(review)
        } else {
            RepositoryCompletion::Created(review)
        };
        let result = classify_repository_completion(
            start(&operation, NativeReviewStage::CreatePr).await,
            outcome,
        )
        .unwrap();
        assert!(matches!(
            result.publication,
            NativeReviewPublication::Unknown { .. }
        ));
        assert!(result.git_receipts.is_empty());
        match result.outcome {
            NativeReviewOutcome::Created { review } | NativeReviewOutcome::Reused { review } => {
                assert_eq!(review, expected);
            }
            _ => panic!("missing actual review"),
        }
        operation
            .record_publication(NativeReviewPublication::LocalAhead {
                local_head_sha: "local-B".to_owned(),
                remote_source_sha: "remote-A".to_owned(),
            })
            .unwrap();
        assert!(matches!(
            operation.execution().unwrap().publication,
            NativeReviewPublication::LocalAhead { .. }
        ));
    }
}

#[tokio::test]
async fn absent_remote_and_unobserved_remote_remain_distinct() {
    let rig = Rig::new(HostRole::Owner, None);
    let operation = rig
        .capture(vec![NativeReviewStage::CreatePr])
        .await
        .unwrap();
    assert!(matches!(
        operation.execution().unwrap().publication,
        NativeReviewPublication::Unknown { .. }
    ));
    operation
        .record_publication(NativeReviewPublication::RemoteBranchMissing {
            local_head_sha: Some("local-B".to_owned()),
        })
        .unwrap();
    assert!(matches!(
        operation.execution().unwrap().publication,
        NativeReviewPublication::RemoteBranchMissing { .. }
    ));
    assert_eq!(
        operation.record_publication(NativeReviewPublication::Included {
            local_head_sha: "foreign-C".to_owned(),
            remote_source_sha: "foreign-C".to_owned()
        }),
        Err(AdmissionError::BindingChanged)
    );
}

#[tokio::test]
async fn sidebar_requests_keep_separate_commit_and_create_receipts() {
    let rig = Rig::new(HostRole::Owner, None);
    let commit = rig
        .capture_as(
            vec![NativeReviewStage::Commit],
            RepositoryEntry::Bearer,
            "sidebar-commit",
        )
        .await
        .unwrap();
    let committed = classify_repository_completion(
        start(&commit, NativeReviewStage::Commit).await,
        RepositoryCompletion::Committed {
            hash: "local-C".to_owned(),
            staging_after: None,
        },
    )
    .unwrap();
    rig.change_root(|facts| {
        facts.preparation.local_head_sha = Some("local-C".to_owned());
        facts.staging_fingerprint = None;
        facts.preparation.operation_id = "new-create".to_owned();
    });
    let create = rig
        .capture_as(
            vec![NativeReviewStage::CreatePr],
            RepositoryEntry::Bearer,
            "sidebar-create",
        )
        .await
        .unwrap();
    let failed = classify_repository_completion(
        start(&create, NativeReviewStage::CreatePr).await,
        RepositoryCompletion::Failed {
            code: None,
            message: "No remote branch".to_owned(),
        },
    )
    .unwrap();
    assert_ne!(committed.request_id, failed.request_id);
    assert_eq!(committed.git_receipts.len(), 1);
    assert!(failed.git_receipts.is_empty());
    assert_eq!(
        failed.preparation.local_head_sha.as_deref(),
        Some("local-C")
    );
}

#[tokio::test]
async fn invalid_stage_order_and_wrong_completion_never_grant_more_actions() {
    let rig = Rig::new(HostRole::Owner, None);
    for stages in [
        vec![],
        vec![NativeReviewStage::Push, NativeReviewStage::Commit],
        vec![NativeReviewStage::Commit, NativeReviewStage::Commit],
    ] {
        assert!(matches!(
            rig.capture(stages).await,
            Err(AdmissionError::InvalidPlan)
        ));
    }
    let operation = rig
        .capture(vec![NativeReviewStage::Commit, NativeReviewStage::CreatePr])
        .await
        .unwrap();
    assert!(matches!(
        revalidate_repository_stage(&operation, NativeReviewStage::CreatePr).await,
        Err(AdmissionError::StageOrder)
    ));
    assert!(matches!(
        classify_repository_completion(
            start(&operation, NativeReviewStage::Commit).await,
            RepositoryCompletion::Created(actual_review())
        ),
        Err(AdmissionError::InvalidCompletion)
    ));
    assert!(matches!(
        operation.execution().unwrap().outcome,
        NativeReviewOutcome::Uncertain { .. }
    ));
}

#[tokio::test]
async fn transient_durable_read_failure_is_not_provider_denial_or_logout() {
    let rig = Rig::new(HostRole::Owner, None);
    let operation = rig.capture(vec![NativeReviewStage::Commit]).await.unwrap();
    rig.source.unavailable.store(true, Ordering::SeqCst);
    assert!(matches!(
        revalidate_repository_stage(&operation, NativeReviewStage::Commit).await,
        Err(AdmissionError::Unavailable)
    ));
    assert!(rig.retirement.check_current().is_ok());
    rig.source.unavailable.store(false, Ordering::SeqCst);
    assert!(
        revalidate_repository_stage(&operation, NativeReviewStage::Commit)
            .await
            .is_ok()
    );
}

fn advance_injected_provenance(facts: &mut RepositoryAuthorityFacts) {
    let RepositoryAuthorityProvenance::Injected(value) = &mut facts.provenance else {
        panic!("fixture expects injected provenance");
    };
    *value += 2;
}
