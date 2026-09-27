use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use tokio::sync::{Notify, Semaphore};

use super::authority::{CredentialFuture, RepositoryAuthorityFence, RepositoryCredentialTransport};
use super::*;

pub(super) const INSTANCE: &str = "https://git.example:8443/forge";
pub(super) const PROJECT: &str = "team/sub/project";

pub(super) struct Pause {
    pub(super) entered: Notify,
    pub(super) release: Semaphore,
}
impl Pause {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: Notify::new(),
            release: Semaphore::new(0),
        })
    }
    async fn wait(&self) {
        self.entered.notify_one();
        self.release.acquire().await.unwrap().forget();
    }
}
pub(super) struct TestSecrets {
    pub(super) value: Mutex<String>,
    pub(super) pause: Mutex<Option<Arc<Pause>>>,
    pub(super) calls: AtomicUsize,
    wrong_revision: AtomicBool,
    missing: AtomicBool,
}
impl RepositorySecretReader for TestSecrets {
    fn load<'a>(
        &'a self,
        expected: &'a RepositorySecretRequest,
    ) -> CredentialFuture<'a, RepositorySecretSnapshot> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.missing.load(Ordering::SeqCst) {
                return Err(RepositoryCredentialError::Missing);
            }
            let token = SecretString::from(self.value.lock().unwrap().clone());
            let pause = self.pause.lock().unwrap().take();
            if let Some(pause) = pause {
                pause.wait().await;
            }
            let mut request = expected.clone();
            if self.wrong_revision.load(Ordering::SeqCst) {
                request.secret_revision += 1;
            }
            Ok(RepositorySecretSnapshot { request, token })
        })
    }
}
pub(super) struct TestAuthority {
    pub(super) revision: Arc<Mutex<u64>>,
    pause: Mutex<Option<Arc<Pause>>>,
    pub(super) requests: Mutex<Vec<RepositoryAuthorityRequest>>,
    denied: AtomicBool,
}
struct Fence {
    revision: Arc<Mutex<u64>>,
    expected: u64,
}
impl RepositoryAuthorityFence for Fence {
    fn dispatch(self: Box<Self>, action: &mut (dyn FnMut() -> Result<()> + Send)) -> Result<()> {
        let current = self.revision.lock().unwrap();
        if *current != self.expected {
            return Err(RepositoryCredentialError::Retired);
        }
        action()
    }
}
impl RepositoryAuthority for TestAuthority {
    fn revalidate<'a>(
        &'a self,
        request: &'a RepositoryAuthorityRequest,
    ) -> CredentialFuture<'a, Box<dyn RepositoryAuthorityFence>> {
        Box::pin(async move {
            self.requests.lock().unwrap().push(request.clone());
            if self.denied.load(Ordering::SeqCst) {
                return Err(RepositoryCredentialError::AuthorityDenied);
            }
            let expected = *self.revision.lock().unwrap();
            if expected != request.execution.authority_generation {
                return Err(RepositoryCredentialError::Retired);
            }
            let pause = self.pause.lock().unwrap().take();
            if let Some(pause) = pause {
                pause.wait().await;
            }
            Ok(Box::new(Fence {
                revision: self.revision.clone(),
                expected,
            }) as Box<dyn RepositoryAuthorityFence>)
        })
    }
}

pub(super) struct Test {
    pub(super) directory: Arc<RepositoryConnectionDirectory>,
    pub(super) verified: VerifiedRepositoryAccount,
    pub(super) secrets: Arc<TestSecrets>,
    pub(super) authority: Arc<TestAuthority>,
}
impl Test {
    pub(super) fn new() -> Self {
        Self::with_descriptor(GitlabDescriptor::new(
            intent_sourcecontrol::GitlabInstance::parse(INSTANCE).unwrap(),
        ))
    }
    pub(super) fn with_descriptor(descriptor: GitlabDescriptor) -> Self {
        let verified = VerifiedRepositoryAccount::from_verified_user(
            descriptor,
            71,
            RepositoryCredentialSource::GitlabSecretSlot,
        )
        .unwrap();
        let test = Self {
            directory: Arc::new(RepositoryConnectionDirectory::new("daemon-A".into())),
            verified,
            secrets: Arc::new(TestSecrets {
                value: Mutex::new("token-old".into()),
                pause: Mutex::new(None),
                calls: AtomicUsize::new(0),
                wrong_revision: AtomicBool::new(false),
                missing: AtomicBool::new(false),
            }),
            authority: Arc::new(TestAuthority {
                revision: Arc::new(Mutex::new(1)),
                pause: Mutex::new(None),
                requests: Mutex::new(vec![]),
                denied: AtomicBool::new(false),
            }),
        };
        test.replace(test.verified.clone());
        test
    }
    pub(super) fn request(&self, use_kind: RepositoryCredentialUse) -> RepositoryAuthorityRequest {
        RepositoryAuthorityRequest {
            execution: ExecutionScope {
                daemon_id: "daemon-A".into(),
                authority_scope_id: "original-caller-root".into(),
                authority_generation: 1,
            },
            target: RepositoryTarget {
                provider: RepositoryProvider::Gitlab,
                instance_base_url: INSTANCE.into(),
                project_path: PROJECT.into(),
            },
            connection: self.directory.binding().unwrap().scope,
            use_kind,
            allowed_transport: match use_kind {
                RepositoryCredentialUse::NativePush | RepositoryCredentialUse::ChildGit => {
                    RepositoryCredentialTransport::GitHttps(vec![format!(
                        "{INSTANCE}/{PROJECT}.git"
                    )])
                }
                _ => RepositoryCredentialTransport::GitlabApi(self.verified.descriptor.clone()),
            },
        }
    }
    pub(super) fn admit(&self, use_kind: RepositoryCredentialUse) -> RepositoryCredentialAdmission {
        self.directory
            .admit(
                &self.directory.binding().unwrap(),
                self.request(use_kind),
                self.authority.clone(),
            )
            .unwrap()
    }
    pub(super) fn replace(
        &self,
        verified: VerifiedRepositoryAccount,
    ) -> RepositoryConnectionBinding {
        let ticket = self
            .directory
            .reserve_mutation(RepositoryMutationKind::Replace)
            .unwrap();
        self.directory.begin_mutation(&ticket).unwrap();
        self.directory
            .finish_mutation(&ticket, SettledCredentialState::Verified(verified))
            .unwrap()
            .unwrap()
    }
    pub(super) fn refresh(&self, value: &str) {
        let ticket = self
            .directory
            .reserve_mutation(RepositoryMutationKind::Refresh)
            .unwrap();
        self.directory.begin_mutation(&ticket).unwrap();
        *self.secrets.value.lock().unwrap() = value.into();
        self.directory
            .finish_mutation(
                &ticket,
                SettledCredentialState::Verified(self.verified.clone()),
            )
            .unwrap();
    }
    pub(super) async fn acquire(
        &self,
        admission: &RepositoryCredentialAdmission,
    ) -> Result<RepositoryCredentialTicket> {
        self.directory
            .acquire_exact(admission, self.secrets.as_ref(), Duration::from_secs(2))
            .await
    }
}

#[test]
fn canonical_name_and_zero_user_id_do_not_publish_a_binding() {
    let directory = RepositoryConnectionDirectory::new("daemon-A".into());
    assert_eq!(
        directory.binding().unwrap_err(),
        RepositoryCredentialError::Unverified
    );
    let descriptor =
        GitlabDescriptor::new(intent_sourcecontrol::GitlabInstance::parse("gitlab.com").unwrap());
    assert_eq!(
        VerifiedRepositoryAccount::from_verified_user(
            descriptor,
            0,
            RepositoryCredentialSource::GitlabSecretSlot
        )
        .unwrap_err(),
        RepositoryCredentialError::Unverified
    );
}

#[test]
fn superseded_reservation_cannot_retire_or_publish_and_cancel_keeps_ready_binding() {
    let test = Test::new();
    let admission = test.admit(RepositoryCredentialUse::NativeRead);
    let old = test
        .directory
        .reserve_mutation(RepositoryMutationKind::Replace)
        .unwrap();
    let new = test
        .directory
        .reserve_mutation(RepositoryMutationKind::Replace)
        .unwrap();
    assert_eq!(
        test.directory.begin_mutation(&old),
        Err(RepositoryCredentialError::StaleMutation)
    );
    assert_eq!(
        test.directory.finish_mutation(
            &old,
            SettledCredentialState::Verified(test.verified.clone())
        ),
        Err(RepositoryCredentialError::StaleMutation)
    );
    assert_eq!(test.directory.check_current(&admission), Ok(()));
    test.directory.cancel_reservation(&new).unwrap();
    assert_eq!(
        test.directory.begin_mutation(&new),
        Err(RepositoryCredentialError::StaleMutation)
    );
    assert_eq!(test.directory.check_current(&admission), Ok(()));
}

#[tokio::test]
async fn refresh_blocks_old_token_then_preserves_binding_with_new_secret_revision() {
    let test = Test::new();
    let admission = test.admit(RepositoryCredentialUse::NativeRead);
    let before = test.acquire(&admission).await.unwrap();
    let mutation = test
        .directory
        .reserve_mutation(RepositoryMutationKind::Refresh)
        .unwrap();
    test.directory.begin_mutation(&mutation).unwrap();
    assert_eq!(
        test.acquire(&admission).await.unwrap_err(),
        RepositoryCredentialError::Mutating
    );
    *test.secrets.value.lock().unwrap() = "token-new".into();
    test.directory
        .finish_mutation(
            &mutation,
            SettledCredentialState::Verified(test.verified.clone()),
        )
        .unwrap();
    let after = test.acquire(&admission).await.unwrap();
    assert_eq!(before.stamp.binding, after.stamp.binding);
    assert!(after.stamp.secret_revision > before.stamp.secret_revision);
    assert_eq!(
        test.directory.reject_current_credential(&before.stamp),
        Ok(false)
    );
    assert_eq!(test.directory.check_current(&admission), Ok(()));
    assert_eq!(
        test.directory.reject_current_credential(&after.stamp),
        Ok(true)
    );
    assert_eq!(
        test.directory.binding().unwrap_err(),
        RepositoryCredentialError::Disconnected
    );
    assert_eq!(
        test.directory.check_current(&admission),
        Err(RepositoryCredentialError::Retired)
    );
}

#[tokio::test]
async fn refresh_during_secret_load_rejects_old_snapshot_without_retiring_account() {
    let test = Arc::new(Test::new());
    let admission = test.admit(RepositoryCredentialUse::NativeRead);
    let pause = Pause::new();
    *test.secrets.pause.lock().unwrap() = Some(pause.clone());
    let task_test = test.clone();
    let read = tokio::spawn(async move { task_test.acquire(&admission).await });
    pause.entered.notified().await;
    test.refresh("changed");
    pause.release.add_permits(1);
    assert_eq!(
        read.await.unwrap().unwrap_err(),
        RepositoryCredentialError::SecretMismatch
    );
    assert!(test
        .acquire(&test.admit(RepositoryCredentialUse::NativeRead))
        .await
        .is_ok());
}

#[tokio::test]
async fn replacement_during_secret_load_prevents_release_to_old_account() {
    let test = Arc::new(Test::new());
    let admission = test.admit(RepositoryCredentialUse::NativeRead);
    let pause = Pause::new();
    *test.secrets.pause.lock().unwrap() = Some(pause.clone());
    let task_test = test.clone();
    let read = tokio::spawn(async move { task_test.acquire(&admission).await });
    pause.entered.notified().await;
    test.replace(test.verified.clone());
    pause.release.add_permits(1);
    assert_eq!(
        read.await.unwrap().unwrap_err(),
        RepositoryCredentialError::Retired
    );
}

#[test]
fn compensation_and_indeterminate_settlement_never_restore_old_generation() {
    for kind in [
        RepositoryMutationKind::Replace,
        RepositoryMutationKind::Refresh,
        RepositoryMutationKind::Disconnect,
    ] {
        let test = Test::new();
        let admission = test.admit(RepositoryCredentialUse::NativeRead);
        let old = test.directory.binding().unwrap();
        let ticket = test.directory.reserve_mutation(kind).unwrap();
        test.directory.begin_mutation(&ticket).unwrap();
        test.directory
            .finish_mutation(&ticket, SettledCredentialState::Indeterminate)
            .unwrap();
        assert_eq!(
            test.directory.binding().unwrap_err(),
            RepositoryCredentialError::Indeterminate
        );
        assert_eq!(
            test.directory
                .reserve_mutation(RepositoryMutationKind::Replace)
                .unwrap_err(),
            RepositoryCredentialError::Indeterminate
        );
        let new = test
            .directory
            .finish_mutation(
                &ticket,
                SettledCredentialState::Compensated(test.verified.clone()),
            )
            .unwrap()
            .unwrap();
        assert_ne!(old.scope.connection_id, new.scope.connection_id);
        assert!(new.scope.connection_generation > old.scope.connection_generation);
        assert_eq!(
            test.directory.check_current(&admission),
            Err(RepositoryCredentialError::Retired)
        );
    }
}

#[test]
fn account_instance_transport_and_source_changes_cannot_be_refreshes() {
    let test = Test::new();
    let mut alternatives = vec![];
    let mut account = test.verified.clone();
    account.account_id = "72".into();
    alternatives.push(account);
    let mut instance = test.verified.clone();
    instance.descriptor = GitlabDescriptor::new(
        intent_sourcecontrol::GitlabInstance::parse("https://git.example:8443/other").unwrap(),
    );
    alternatives.push(instance);
    let mut transport = test.verified.clone();
    transport.descriptor = GitlabDescriptor::with_loopback_endpoint(
        transport.descriptor.instance().clone(),
        "http://127.0.0.1:9999",
    )
    .unwrap();
    alternatives.push(transport);
    let mut source = test.verified.clone();
    source.source = RepositoryCredentialSource::GitlabEnvironment;
    alternatives.push(source);
    for changed in alternatives {
        let test = Test::new();
        let admission = test.admit(RepositoryCredentialUse::NativeRead);
        let ticket = test
            .directory
            .reserve_mutation(RepositoryMutationKind::Refresh)
            .unwrap();
        test.directory.begin_mutation(&ticket).unwrap();
        assert_eq!(
            test.directory
                .finish_mutation(&ticket, SettledCredentialState::Verified(changed)),
            Err(RepositoryCredentialError::Unverified)
        );
        assert_eq!(
            test.directory.check_current(&admission),
            Err(RepositoryCredentialError::Retired)
        );
        assert_eq!(
            test.directory.binding().unwrap_err(),
            RepositoryCredentialError::Indeterminate
        );
    }
}

#[test]
fn one_flat_slot_and_reconnect_aba_never_revive_old_admissions() {
    let test = Test::new();
    let admission = test.admit(RepositoryCredentialUse::NativeRead);
    let old = test.directory.binding().unwrap();
    let mut other = test.verified.clone();
    other.account_id = "72".into();
    let second = test.replace(other);
    assert_ne!(old, second);
    let third = test.replace(test.verified.clone());
    assert_ne!(old.scope, third.scope);
    assert_eq!(
        test.directory.check_current(&admission),
        Err(RepositoryCredentialError::Retired)
    );
    let ticket = test
        .directory
        .reserve_mutation(RepositoryMutationKind::Disconnect)
        .unwrap();
    test.directory.begin_mutation(&ticket).unwrap();
    test.directory
        .finish_mutation(&ticket, SettledCredentialState::Disconnected)
        .unwrap();
    assert_eq!(
        test.directory.binding().unwrap_err(),
        RepositoryCredentialError::Disconnected
    );
    assert_ne!(test.replace(test.verified.clone()).scope, third.scope);
}

#[test]
fn child_opt_out_does_not_retire_native_and_reenable_does_not_revive_child() {
    let test = Test::new();
    let binding = test.directory.binding().unwrap();
    let native = test.admit(RepositoryCredentialUse::NativeRead);
    assert_eq!(
        test.directory
            .admit(
                &binding,
                test.request(RepositoryCredentialUse::ChildGit),
                test.authority.clone()
            )
            .unwrap_err(),
        RepositoryCredentialError::ChildDisabled
    );
    test.directory.set_child_policy(&binding, true).unwrap();
    let child = test.admit(RepositoryCredentialUse::ChildGit);
    test.directory.set_child_policy(&binding, false).unwrap();
    assert_eq!(test.directory.check_current(&native), Ok(()));
    assert_eq!(
        test.directory.check_current(&child),
        Err(RepositoryCredentialError::Retired)
    );
    test.directory.set_child_policy(&binding, true).unwrap();
    assert_eq!(
        test.directory.check_current(&child),
        Err(RepositoryCredentialError::Retired)
    );
    let mut other = test.verified.clone();
    other.account_id = "72".into();
    let other = test.replace(other);
    assert_eq!(
        test.directory
            .admit(
                &other,
                test.request(RepositoryCredentialUse::ChildGit),
                test.authority.clone()
            )
            .unwrap_err(),
        RepositoryCredentialError::ChildDisabled
    );
}

#[test]
fn restart_and_all_counter_overflows_retire_without_wrapping() {
    let test = Test::new();
    let old = test.admit(RepositoryCredentialUse::NativeRead);
    let restarted = Test::new();
    assert_eq!(
        restarted.directory.check_current(&old),
        Err(RepositoryCredentialError::Retired)
    );
    test.directory.retire().unwrap();
    assert_eq!(
        test.directory.check_current(&old),
        Err(RepositoryCredentialError::Retired)
    );
    for counter in 0..4 {
        let test = Test::new();
        let admission = test.admit(RepositoryCredentialUse::NativeRead);
        {
            let mut state = test.directory.state.lock().unwrap();
            match counter {
                0 => state.mutation_id = u64::MAX,
                1 => state.generation = u64::MAX,
                2 => state.secret_revision = u64::MAX,
                _ => state.child_revision = u64::MAX,
            }
        }
        let result = if counter == 3 {
            test.directory
                .set_child_policy(&test.directory.binding().unwrap(), true)
        } else {
            test.directory
                .reserve_mutation(RepositoryMutationKind::Replace)
                .and_then(|ticket| {
                    test.directory.begin_mutation(&ticket)?;
                    test.directory
                        .finish_mutation(
                            &ticket,
                            SettledCredentialState::Verified(test.verified.clone()),
                        )
                        .map(|_| ())
                })
        };
        assert_eq!(result, Err(RepositoryCredentialError::CounterExhausted));
        assert_eq!(
            test.directory.check_current(&admission),
            Err(RepositoryCredentialError::Retired)
        );
    }
}

#[tokio::test]
async fn child_opt_out_during_secret_load_preserves_native_push_admission() {
    let test = Arc::new(Test::new());
    let binding = test.directory.binding().unwrap();
    test.directory.set_child_policy(&binding, true).unwrap();
    let child = test.admit(RepositoryCredentialUse::ChildGit);
    let native = test.admit(RepositoryCredentialUse::NativePush);
    let pause = Pause::new();
    *test.secrets.pause.lock().unwrap() = Some(pause.clone());
    let task_test = test.clone();
    let read = tokio::spawn(async move { task_test.acquire(&child).await });
    pause.entered.notified().await;
    test.directory.set_child_policy(&binding, false).unwrap();
    pause.release.add_permits(1);
    assert_eq!(
        read.await.unwrap().unwrap_err(),
        RepositoryCredentialError::Retired
    );
    assert_eq!(test.acquire(&native).await.unwrap().stamp.binding, binding);
    assert_eq!(test.directory.binding().unwrap(), binding);
}

#[tokio::test]
async fn unsettled_refresh_during_secret_load_blocks_release_until_compensation_settles() {
    let test = Arc::new(Test::new());
    let old = test.admit(RepositoryCredentialUse::NativeRead);
    let pending = test.admit(RepositoryCredentialUse::NativeRead);
    let pause = Pause::new();
    *test.secrets.pause.lock().unwrap() = Some(pause.clone());
    let task_test = test.clone();
    let read = tokio::spawn(async move { task_test.acquire(&pending).await });
    pause.entered.notified().await;
    let writer = test
        .directory
        .reserve_mutation(RepositoryMutationKind::Refresh)
        .unwrap();
    test.directory.begin_mutation(&writer).unwrap();
    test.directory
        .finish_mutation(&writer, SettledCredentialState::Indeterminate)
        .unwrap();
    pause.release.add_permits(1);
    assert_eq!(
        read.await.unwrap().unwrap_err(),
        RepositoryCredentialError::Indeterminate
    );
    test.directory
        .finish_mutation(
            &writer,
            SettledCredentialState::Compensated(test.verified.clone()),
        )
        .unwrap();
    assert_eq!(
        test.acquire(&old).await.unwrap_err(),
        RepositoryCredentialError::Retired
    );
    assert!(test
        .acquire(&test.admit(RepositoryCredentialUse::NativeRead))
        .await
        .is_ok());
}

#[tokio::test]
async fn replacement_preserves_full_instance_and_large_immutable_account_identity() {
    for instance in [
        "https://git.example:8444/forge",
        "https://git.example:8443/other",
    ] {
        let test = Test::new();
        let old = test.admit(RepositoryCredentialUse::NativeRead);
        let verified = VerifiedRepositoryAccount::from_verified_user(
            GitlabDescriptor::new(intent_sourcecontrol::GitlabInstance::parse(instance).unwrap()),
            u64::MAX,
            RepositoryCredentialSource::GitlabSecretSlot,
        )
        .unwrap();
        let binding = test.replace(verified.clone());
        assert_eq!(binding.account.instance_base_url, instance);
        assert_eq!(binding.account.account_id, u64::MAX.to_string());
        assert_eq!(binding.scope.account_id, u64::MAX.to_string());
        assert_eq!(
            test.acquire(&old).await.unwrap_err(),
            RepositoryCredentialError::Retired
        );
        let mut request = test.request(RepositoryCredentialUse::NativeRead);
        request.target.instance_base_url = instance.into();
        request.allowed_transport = RepositoryCredentialTransport::GitlabApi(verified.descriptor);
        let new = test
            .directory
            .admit(&binding, request, test.authority.clone())
            .unwrap();
        assert_eq!(test.acquire(&new).await.unwrap().stamp.binding, binding);
    }
}

#[tokio::test]
async fn original_authority_rechecked_after_load_and_at_final_dispatch() {
    for after_revalidation in [false, true] {
        let test = Arc::new(Test::new());
        let admission = test.admit(RepositoryCredentialUse::NativeRead);
        let pause = Pause::new();
        if after_revalidation {
            *test.authority.pause.lock().unwrap() = Some(pause.clone());
        } else {
            *test.secrets.pause.lock().unwrap() = Some(pause.clone());
        }
        let task_test = test.clone();
        let read = tokio::spawn(async move { task_test.acquire(&admission).await });
        pause.entered.notified().await;
        *test.authority.revision.lock().unwrap() += 1;
        pause.release.add_permits(1);
        assert_eq!(
            read.await.unwrap().unwrap_err(),
            RepositoryCredentialError::Retired
        );
    }
}

#[tokio::test(start_paused = true)]
async fn acquisition_budget_bounds_both_secret_and_authority_work() {
    for slow_authority in [false, true] {
        let test = Test::new();
        let pause = Pause::new();
        if slow_authority {
            *test.authority.pause.lock().unwrap() = Some(pause);
        } else {
            *test.secrets.pause.lock().unwrap() = Some(pause);
        }
        assert_eq!(
            test.acquire(&test.admit(RepositoryCredentialUse::NativeRead))
                .await
                .unwrap_err(),
            RepositoryCredentialError::TimedOut
        );
    }
}

#[tokio::test]
async fn missing_unverified_secret_authority_and_quota_are_distinct_local_outcomes() {
    let test = Test::new();
    let admission = test.admit(RepositoryCredentialUse::NativeRead);
    test.secrets.missing.store(true, Ordering::SeqCst);
    assert_eq!(
        test.acquire(&admission).await.unwrap_err(),
        RepositoryCredentialError::Missing
    );
    test.secrets.missing.store(false, Ordering::SeqCst);
    test.secrets.wrong_revision.store(true, Ordering::SeqCst);
    assert_eq!(
        test.acquire(&admission).await.unwrap_err(),
        RepositoryCredentialError::SecretMismatch
    );
    test.secrets.wrong_revision.store(false, Ordering::SeqCst);
    test.authority.denied.store(true, Ordering::SeqCst);
    assert_eq!(
        test.acquire(&admission).await.unwrap_err(),
        RepositoryCredentialError::AuthorityDenied
    );
    test.authority.denied.store(false, Ordering::SeqCst);
    let ticket = test.acquire(&admission).await.unwrap();
    assert!(!format!("{ticket:?}").contains("token-old"));
    test.directory
        .record_backoff(&ticket.stamp, Instant::now() + Duration::from_secs(60))
        .unwrap();
    test.refresh("updated");
    assert_eq!(
        test.acquire(&admission).await.unwrap_err(),
        RepositoryCredentialError::Backoff
    );
    let replacement = test.replace(test.verified.clone());
    assert_eq!(
        test.directory
            .record_backoff(&ticket.stamp, Instant::now() + Duration::from_secs(120)),
        Ok(false)
    );
    assert_eq!(test.directory.binding().unwrap(), replacement);
}

#[test]
fn boundary_and_secret_errors_are_safe_and_never_become_upstream_denials() {
    let test = Test::new();
    for changed in 0..5 {
        let mut request = test.request(RepositoryCredentialUse::NativeRead);
        match changed {
            0 => request.target.instance_base_url = "https://git.example/forge".into(),
            1 => request.connection.account_id = "elsewhere".into(),
            2 => request.execution.daemon_id = "daemon-B".into(),
            3 => request.target.project_path = "team/../secret".into(),
            _ => {
                request.allowed_transport = RepositoryCredentialTransport::GitHttps(vec![
                    "https://token@foreign.example/private".into(),
                ]);
            }
        }
        let error = test
            .directory
            .admit(
                &test.directory.binding().unwrap(),
                request,
                test.authority.clone(),
            )
            .unwrap_err();
        assert_eq!(error, RepositoryCredentialError::BoundaryMismatch);
        let provider: intent_sourcecontrol::Error = error.into();
        assert!(matches!(
            provider,
            intent_sourcecontrol::Error::AdmissionUnavailable(_)
        ));
        assert!(!format!("{provider:?}").contains("foreign"));
    }
    assert!(matches!(
        intent_sourcecontrol::Error::from(RepositoryCredentialError::Retired),
        intent_sourcecontrol::Error::AdmissionRetired
    ));
}
