use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use intent_sourcecontrol::{
    gitlab::GitlabCredentialRequest, GitlabDescriptor, GitlabInstance, GitlabRequestCredentials,
};

use crate::repository_credential_writers::*;
use crate::repository_credentials::*;
use crate::support::*;

#[tokio::test]
async fn invalid_noop_placeholder_and_abandoned_preflight_preserve_access() {
    let test = Test::new();
    let original = test.directory.binding().unwrap();
    let admission = test.native();
    for label in [
        "unchanged token",
        "placeholder",
        "unrelated setting",
        "cancelled startup",
        "wrong host",
        "unmatched token",
    ] {
        let reservation = test
            .writers
            .reserve(RepositoryMutationKind::Replace)
            .unwrap();
        let result = reservation
            .begin(|| {
                assert_eq!(test.directory.binding().unwrap(), original, "{label}");
                Ok(RepositoryWriterPreflight::NoChange)
            })
            .unwrap();
        assert!(result.is_none(), "{label}");
    }
    let invalid = test
        .writers
        .reserve(RepositoryMutationKind::Replace)
        .unwrap()
        .begin(|| Err(RepositoryCredentialError::Unverified));
    assert!(matches!(
        invalid,
        Err(RepositoryCredentialError::Unverified)
    ));
    drop(
        test.writers
            .reserve(RepositoryMutationKind::Replace)
            .unwrap(),
    );
    assert_eq!(test.directory.binding().unwrap(), original);
    test.acquire(&admission).await.unwrap();
    assert_eq!(test.secrets.writes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn old_startup_reservation_cannot_replace_a_new_intent_after_await() {
    let test = Arc::new(Test::new());
    let old = test
        .writers
        .reserve(RepositoryMutationKind::Replace)
        .unwrap();
    let pause = Pause::new();
    let wait = pause.clone();
    let original_owner = tokio::spawn(async move {
        wait.wait().await;
        old.begin(|| Ok(RepositoryWriterPreflight::Change))
    });
    pause.entered.notified().await;
    let new = test
        .writers
        .reserve(RepositoryMutationKind::Replace)
        .unwrap();
    pause.release.add_permits(1);
    assert!(matches!(
        original_owner.await.unwrap(),
        Err(RepositoryCredentialError::StaleMutation)
    ));
    let mutation = new
        .begin(|| Ok(RepositoryWriterPreflight::Change))
        .unwrap()
        .unwrap();
    mutation
        .complete(SettledCredentialState::Verified(test.verified.clone()))
        .unwrap();
    test.acquire(&test.native()).await.unwrap();
}

#[test]
fn final_owner_check_rejects_foreign_account_or_descriptor_without_effects() {
    let test = Test::new();
    let binding = test.directory.binding().unwrap();
    for (instance, id) in [
        (INSTANCE, 72),
        ("https://git.example:8443/other", 71),
        ("https://git.example:8444/forge", 71),
    ] {
        let original_verified = verified(
            GitlabDescriptor::new(GitlabInstance::parse(instance).unwrap()),
            id,
            RepositoryCredentialSource::GitlabSecretSlot,
        );
        let result = test
            .writers
            .reserve(RepositoryMutationKind::Disconnect)
            .unwrap()
            .begin(|| {
                Ok(if original_verified == test.verified {
                    RepositoryWriterPreflight::Change
                } else {
                    RepositoryWriterPreflight::NoChange
                })
            })
            .unwrap();
        assert!(result.is_none());
        assert_eq!(test.directory.binding().unwrap(), binding);
    }
    assert_eq!(test.secrets.writes.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn waiting_old_acquisition_cannot_escape_first_persistence_fence() {
    let test = Arc::new(Test::new());
    let admission = test.native();
    let pause = Pause::new();
    *test.secrets.pause.lock().unwrap() = Some(pause.clone());
    let reader = test.clone();
    let old = tokio::spawn(async move { reader.acquire(&admission).await });
    pause.entered.notified().await;
    let mutation = test.begin(RepositoryMutationKind::Replace);
    assert_eq!(
        test.directory.binding().unwrap_err(),
        RepositoryCredentialError::Mutating
    );
    test.secrets.persist("replacement-token");
    pause.release.add_permits(1);
    assert_eq!(
        old.await.unwrap().unwrap_err(),
        RepositoryCredentialError::Retired
    );
    mutation
        .complete(SettledCredentialState::Verified(test.verified.clone()))
        .unwrap();
}

#[tokio::test]
async fn actual_callback_reacquires_only_after_verified_refresh_settles() {
    let test = Test::new();
    let binding = test.directory.binding().unwrap();
    let callback = BoundGitlabRequestCredentials::new(
        test.directory.clone(),
        test.native(),
        test.secrets.clone(),
        Duration::from_secs(2),
    )
    .unwrap();
    let path = "projects/team%2Fnested%2Fproject/merge_requests";
    let token = callback
        .token_for_request(
            test.descriptor.instance(),
            GitlabCredentialRequest::direct(&test.descriptor, path, false),
        )
        .await
        .unwrap();
    assert!(!format!("{token:?}").contains("old-local-token"));
    let mutation = test.begin(RepositoryMutationKind::Refresh);
    test.secrets.persist("refreshed-token");
    let error = callback
        .token_for_request(
            test.descriptor.instance(),
            GitlabCredentialRequest::direct(&test.descriptor, path, false),
        )
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        intent_sourcecontrol::Error::AdmissionUnavailable(_)
    ));
    assert_eq!(test.secrets.reads.lock().unwrap().len(), 1);
    mutation
        .complete(SettledCredentialState::Verified(test.verified.clone()))
        .unwrap();
    assert_eq!(test.directory.binding().unwrap(), binding);
    let token = callback
        .token_for_request(
            test.descriptor.instance(),
            GitlabCredentialRequest::direct(&test.descriptor, path, false),
        )
        .await
        .unwrap();
    assert!(!format!("{token:?}").contains("refreshed-token"));
    let reads = test.secrets.reads.lock().unwrap();
    assert_eq!(reads[0].binding, reads[1].binding);
    assert_eq!(reads[1].secret_revision, reads[0].secret_revision + 1);
}

#[tokio::test]
async fn refresh_rejects_secret_loaded_before_revision_change() {
    let test = Arc::new(Test::new());
    let admission = test.native();
    let pause = Pause::new();
    *test.secrets.pause.lock().unwrap() = Some(pause.clone());
    let reader = test.clone();
    let old = tokio::spawn(async move { reader.acquire(&admission).await });
    pause.entered.notified().await;
    let mutation = test.begin(RepositoryMutationKind::Refresh);
    test.secrets.persist("refreshed-token");
    mutation
        .complete(SettledCredentialState::Verified(test.verified.clone()))
        .unwrap();
    pause.release.add_permits(1);
    assert_eq!(
        old.await.unwrap().unwrap_err(),
        RepositoryCredentialError::SecretMismatch
    );
}

#[test]
fn replacement_changes_source_account_or_instance_without_reusing_old_admissions() {
    for (instance, id, source) in [
        (INSTANCE, 71, RepositoryCredentialSource::GitlabSecretSlot),
        (INSTANCE, 72, RepositoryCredentialSource::GitlabSecretSlot),
        (
            "https://git.example:8444/forge",
            71,
            RepositoryCredentialSource::GitlabSecretSlot,
        ),
        (
            "https://git.example:8443/other",
            71,
            RepositoryCredentialSource::GitlabSecretSlot,
        ),
        (INSTANCE, 71, RepositoryCredentialSource::GitlabEnvironment),
    ] {
        let test = Test::new();
        let binding = test.directory.binding().unwrap();
        let admission = test.native();
        let replacement = verified(
            GitlabDescriptor::new(GitlabInstance::parse(instance).unwrap()),
            id,
            source,
        );
        let next = test
            .begin(RepositoryMutationKind::Replace)
            .complete(SettledCredentialState::Verified(replacement))
            .unwrap()
            .unwrap();
        assert_ne!(next.scope.connection_id, binding.scope.connection_id);
        assert!(next.scope.connection_generation > binding.scope.connection_generation);
        assert_eq!(
            test.directory.check_current(&admission).unwrap_err(),
            RepositoryCredentialError::Retired
        );
    }
}

#[test]
fn unverified_refresh_cannot_become_ready_and_proved_compensation_retires_old_generation() {
    let test = Test::new();
    let old = test.native();
    let binding = test.directory.binding().unwrap();
    let mutation = test.begin(RepositoryMutationKind::Refresh);
    let completion = mutation.completion();
    let foreign = verified(
        test.descriptor.clone(),
        72,
        RepositoryCredentialSource::GitlabSecretSlot,
    );
    assert_eq!(
        mutation
            .complete(SettledCredentialState::Verified(foreign))
            .unwrap_err(),
        RepositoryCredentialError::Unverified
    );
    assert_eq!(
        test.directory.binding().unwrap_err(),
        RepositoryCredentialError::Indeterminate
    );
    let restored = completion
        .complete(SettledCredentialState::Compensated(test.verified.clone()))
        .unwrap()
        .unwrap();
    assert_ne!(restored.scope.connection_id, binding.scope.connection_id);
    assert!(restored.scope.connection_generation > binding.scope.connection_generation);
    assert_eq!(
        test.directory.check_current(&old).unwrap_err(),
        RepositoryCredentialError::Retired
    );
}

#[test]
fn partial_batch_waits_for_all_cleanup_and_compensation_without_second_persistence() {
    let test = Test::new();
    let old = test.native();
    let mutation = test.begin(RepositoryMutationKind::Replace);
    let completion = mutation.completion();
    test.secrets.persist("new-token-before-sibling-failure");
    completion.indeterminate().unwrap();
    assert_eq!(
        test.directory.binding().unwrap_err(),
        RepositoryCredentialError::Indeterminate
    );
    assert!(matches!(
        test.writers.reserve(RepositoryMutationKind::Replace),
        Err(RepositoryCredentialError::Indeterminate)
    ));
    test.secrets.persist("old-local-token");
    mutation
        .complete(SettledCredentialState::Compensated(test.verified.clone()))
        .unwrap();
    assert_eq!(test.secrets.writes.load(Ordering::SeqCst), 3);
    assert_eq!(
        test.directory.check_current(&old).unwrap_err(),
        RepositoryCredentialError::Retired
    );
    assert!(matches!(
        completion.complete(SettledCredentialState::Verified(test.verified.clone())),
        Err(RepositoryCredentialError::StaleMutation)
    ));
}

#[tokio::test]
async fn detached_owner_can_settle_after_timeout_but_cannot_overwrite_a_new_completion() {
    let test = Arc::new(Test::new());
    let mutation = test.begin(RepositoryMutationKind::Replace);
    let completion = mutation.completion();
    let late = completion.clone();
    let pause = Pause::new();
    let writer = test.clone();
    let wait = pause.clone();
    let job = tokio::spawn(async move {
        wait.wait().await;
        writer.secrets.persist("late-actual-write");
        completion
            .complete(SettledCredentialState::Verified(writer.verified.clone()))
            .unwrap();
    });
    pause.entered.notified().await;
    drop(mutation);
    assert_eq!(
        test.directory.binding().unwrap_err(),
        RepositoryCredentialError::Indeterminate
    );
    assert!(matches!(
        test.writers.reserve(RepositoryMutationKind::Replace),
        Err(RepositoryCredentialError::Indeterminate)
    ));
    pause.release.add_permits(1);
    job.await.unwrap();
    let newer = test
        .begin(RepositoryMutationKind::Replace)
        .complete(SettledCredentialState::Verified(test.verified.clone()))
        .unwrap();
    assert!(matches!(
        late.complete(SettledCredentialState::Compensated(test.verified.clone())),
        Err(RepositoryCredentialError::StaleMutation)
    ));
    late.indeterminate().unwrap();
    assert_eq!(Some(test.directory.binding().unwrap()), newer);
    assert_eq!(test.secrets.writes.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn cancelled_started_task_is_indeterminate_until_original_owner_proves_settlement() {
    let test = Test::new();
    let mutation = test.begin(RepositoryMutationKind::Replace);
    let completion = mutation.completion();
    let pause = Pause::new();
    let wait = pause.clone();
    let job = tokio::spawn(async move {
        let _mutation = mutation;
        wait.wait().await;
    });
    pause.entered.notified().await;
    job.abort();
    assert!(job.await.unwrap_err().is_cancelled());
    assert_eq!(
        test.directory.binding().unwrap_err(),
        RepositoryCredentialError::Indeterminate
    );
    completion
        .complete(SettledCredentialState::Compensated(test.verified.clone()))
        .unwrap();
    assert!(test.directory.binding().is_ok());
}

#[test]
fn settings_candidate_is_retired_before_visibility_and_completion_is_one_shot() {
    let test = Test::new();
    let old = test.native();
    let published = AtomicBool::new(false);
    let mutation = test
        .writers
        .reserve(RepositoryMutationKind::Replace)
        .unwrap()
        .begin(|| {
            assert!(!published.load(Ordering::SeqCst));
            assert!(test.directory.check_current(&old).is_ok());
            Ok(RepositoryWriterPreflight::Change)
        })
        .unwrap()
        .unwrap();
    assert_eq!(
        test.directory.check_current(&old).unwrap_err(),
        RepositoryCredentialError::Retired
    );
    published.store(true, Ordering::SeqCst);
    let completion = mutation.completion();
    mutation
        .complete(SettledCredentialState::Verified(test.verified.clone()))
        .unwrap();
    assert!(matches!(
        completion.complete(SettledCredentialState::Disconnected),
        Err(RepositoryCredentialError::StaleMutation)
    ));
    assert!(test.directory.binding().is_ok());
}

#[test]
fn reentrant_preflight_has_no_new_lock_and_cannot_win_after_it_is_superseded() {
    let test = Test::new();
    let old = test
        .writers
        .reserve(RepositoryMutationKind::Replace)
        .unwrap();
    let mut newer = None;
    let result = old.begin(|| {
        newer = Some(test.writers.reserve(RepositoryMutationKind::Replace)?);
        Ok(RepositoryWriterPreflight::Change)
    });
    assert!(matches!(
        result,
        Err(RepositoryCredentialError::StaleMutation)
    ));
    newer
        .unwrap()
        .begin(|| Ok(RepositoryWriterPreflight::Change))
        .unwrap()
        .unwrap()
        .complete(SettledCredentialState::Verified(test.verified.clone()))
        .unwrap();
}

#[test]
fn disconnect_and_restart_never_turn_old_completion_into_adoption() {
    let test = Test::new();
    let old = test.native();
    test.begin(RepositoryMutationKind::Disconnect)
        .complete(SettledCredentialState::Disconnected)
        .unwrap();
    assert_eq!(
        test.directory.binding().unwrap_err(),
        RepositoryCredentialError::Disconnected
    );
    let late = test.begin(RepositoryMutationKind::Replace);
    let completion = late.completion();
    test.directory.retire().unwrap();
    assert!(matches!(
        late.complete(SettledCredentialState::Verified(test.verified.clone())),
        Err(RepositoryCredentialError::StaleMutation)
    ));
    let restarted = Test::new();
    assert_eq!(
        restarted.directory.check_current(&old).unwrap_err(),
        RepositoryCredentialError::Retired
    );
    assert!(completion
        .complete(SettledCredentialState::Verified(test.verified.clone()))
        .is_err());
    assert!(restarted.directory.binding().is_ok());
}

#[test]
fn concurrent_completions_publish_once_and_old_guard_drop_cannot_hide_new_binding() {
    let test = Test::new();
    let mutation = test.begin(RepositoryMutationKind::Replace);
    let completion: RepositoryWriterCompletion = mutation.completion();
    let barrier = std::sync::Barrier::new(2);
    let results = std::thread::scope(|scope| {
        let first = scope.spawn(|| {
            barrier.wait();
            completion.complete(SettledCredentialState::Verified(test.verified.clone()))
        });
        let second = scope.spawn(|| {
            barrier.wait();
            completion.complete(SettledCredentialState::Verified(test.verified.clone()))
        });
        [first.join().unwrap(), second.join().unwrap()]
    });
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Err(RepositoryCredentialError::StaleMutation)))
            .count(),
        1
    );
    let current = test
        .begin(RepositoryMutationKind::Replace)
        .complete(SettledCredentialState::Verified(test.verified.clone()))
        .unwrap();
    drop(mutation);
    assert_eq!(Some(test.directory.binding().unwrap()), current);
    assert_eq!(test.secrets.writes.load(Ordering::SeqCst), 1);
}

#[test]
fn preflight_panic_cancels_reservation_without_retiring_the_connection() {
    let test = Test::new();
    let admission = test.native();
    let reservation = test
        .writers
        .reserve(RepositoryMutationKind::Replace)
        .unwrap();
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = reservation.begin(|| panic!("existing owner preflight failed"));
    }));
    assert!(outcome.is_err());
    assert!(test.directory.check_current(&admission).is_ok());
    test.begin(RepositoryMutationKind::Refresh)
        .complete(SettledCredentialState::Verified(test.verified.clone()))
        .unwrap();
}
