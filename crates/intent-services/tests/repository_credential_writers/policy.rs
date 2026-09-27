use crate::repository_credential_writers::*;
use crate::repository_credentials::*;
use crate::support::*;

#[tokio::test]
async fn child_write_retires_only_child_and_compensation_never_revives_old_grant() {
    let test = Test::new();
    test.policy().complete(true).unwrap();
    let binding = test.directory.binding().unwrap();
    let native = test.native();
    let child = test.admit(RepositoryCredentialUse::ChildGit).unwrap();
    let mutation = test.policy();
    assert!(test.directory.check_current(&child).is_err());
    test.acquire(&native).await.unwrap();
    assert_eq!(
        test.admit(RepositoryCredentialUse::ChildGit).unwrap_err(),
        RepositoryCredentialError::ChildDisabled
    );
    mutation.complete(true).unwrap();
    assert_eq!(test.directory.binding().unwrap(), binding);
    test.acquire(&native).await.unwrap();
    assert_eq!(
        test.directory.check_current(&child).unwrap_err(),
        RepositoryCredentialError::Retired
    );
    test.acquire(&test.admit(RepositoryCredentialUse::ChildGit).unwrap())
        .await
        .unwrap();
}

#[tokio::test]
async fn detached_child_completion_stays_disabled_without_blocking_native_refresh() {
    let test = Test::new();
    test.policy().complete(true).unwrap();
    let native = test.native();
    let mutation = test.policy();
    let completion: RepositoryChildPolicyCompletion = mutation.completion();
    drop(mutation);
    assert!(matches!(
        test.writers
            .reserve_child_policy(test.directory.binding().unwrap()),
        Err(RepositoryCredentialError::Indeterminate)
    ));
    test.acquire(&native).await.unwrap();
    test.begin(RepositoryMutationKind::Refresh)
        .complete(SettledCredentialState::Verified(test.verified.clone()))
        .unwrap();
    completion.complete(false).unwrap();
    assert_eq!(
        test.admit(RepositoryCredentialUse::ChildGit).unwrap_err(),
        RepositoryCredentialError::ChildDisabled
    );
    test.acquire(&native).await.unwrap();
}

#[test]
fn stale_child_reservation_and_noop_do_not_revoke_current_child() {
    let test = Test::new();
    test.policy().complete(true).unwrap();
    let binding = test.directory.binding().unwrap();
    let child = test.admit(RepositoryCredentialUse::ChildGit).unwrap();
    let old = test.writers.reserve_child_policy(binding.clone()).unwrap();
    let newer = test.writers.reserve_child_policy(binding.clone()).unwrap();
    assert!(matches!(
        old.begin(|| Ok(RepositoryWriterPreflight::Change)),
        Err(RepositoryCredentialError::StaleMutation)
    ));
    assert!(newer
        .begin(|| Ok(RepositoryWriterPreflight::NoChange))
        .unwrap()
        .is_none());
    drop(test.writers.reserve_child_policy(binding).unwrap());
    assert!(test.directory.check_current(&child).is_ok());
}

#[test]
fn stale_enable_completion_cannot_change_new_policy_or_replacement_account() {
    let test = Test::new();
    let mutation = test.policy();
    let old = mutation.completion();
    mutation.complete(true).unwrap();
    test.policy().complete(false).unwrap();
    assert_eq!(
        old.complete(true).unwrap_err(),
        RepositoryCredentialError::StaleMutation
    );
    old.indeterminate().unwrap();
    assert_eq!(
        test.admit(RepositoryCredentialUse::ChildGit).unwrap_err(),
        RepositoryCredentialError::ChildDisabled
    );

    let mutation = test.policy();
    let foreign = verified(
        test.descriptor.clone(),
        72,
        RepositoryCredentialSource::GitlabSecretSlot,
    );
    test.begin(RepositoryMutationKind::Replace)
        .complete(SettledCredentialState::Verified(foreign))
        .unwrap();
    assert_eq!(
        mutation.complete(true).unwrap_err(),
        RepositoryCredentialError::Retired
    );
    assert_eq!(
        test.admit(RepositoryCredentialUse::ChildGit).unwrap_err(),
        RepositoryCredentialError::ChildDisabled
    );
    test.policy().complete(true).unwrap();
    assert!(test.admit(RepositoryCredentialUse::ChildGit).is_ok());
}

#[test]
fn foreign_child_binding_and_failed_preflight_leave_original_policy_and_reservation_intact() {
    let test = Test::new();
    test.policy().complete(true).unwrap();
    let child = test.admit(RepositoryCredentialUse::ChildGit).unwrap();
    let foreign = Test::new().directory.binding().unwrap();
    let result = test.writers.reserve_child_policy(foreign);
    assert!(matches!(result, Err(RepositoryCredentialError::Retired)));
    let error = test
        .writers
        .reserve_child_policy(test.directory.binding().unwrap())
        .unwrap()
        .begin(|| Err(RepositoryCredentialError::Unverified));
    assert!(matches!(error, Err(RepositoryCredentialError::Unverified)));
    assert!(test.directory.check_current(&child).is_ok());
    test.policy().complete(false).unwrap();
}

#[test]
fn child_completion_is_one_shot_even_when_original_guard_outlives_a_later_write() {
    let test = Test::new();
    let mutation = test.policy();
    let completion = mutation.completion();
    completion.complete(true).unwrap();
    test.policy().complete(false).unwrap();
    drop(mutation);
    assert_eq!(
        completion.complete(true).unwrap_err(),
        RepositoryCredentialError::StaleMutation
    );
    assert_eq!(
        test.admit(RepositoryCredentialUse::ChildGit).unwrap_err(),
        RepositoryCredentialError::ChildDisabled
    );
    test.policy().complete(true).unwrap();
}

#[tokio::test]
async fn authoritative_child_write_fences_an_older_enable_completion() {
    for intermediate_enable in [false, true] {
        let test = Test::new();
        let binding = test.directory.binding().unwrap();
        let native = test.native();
        let pending = test.policy();
        if intermediate_enable {
            test.directory.set_child_policy(&binding, true).unwrap();
        }
        // Even a newer disable when already disabled owns a new policy lifetime.
        test.directory.set_child_policy(&binding, false).unwrap();
        assert!(matches!(
            pending.complete(true),
            Err(RepositoryCredentialError::StaleMutation)
        ));
        assert_eq!(
            test.admit(RepositoryCredentialUse::ChildGit).unwrap_err(),
            RepositoryCredentialError::ChildDisabled
        );
        test.acquire(&native).await.unwrap();
        test.policy().complete(true).unwrap();
    }
}

#[test]
fn authoritative_child_write_fences_an_older_reservation_before_begin() {
    for intermediate_enable in [false, true] {
        let test = Test::new();
        let binding = test.directory.binding().unwrap();
        let pending = test.writers.reserve_child_policy(binding.clone()).unwrap();
        if intermediate_enable {
            test.directory.set_child_policy(&binding, true).unwrap();
        }
        test.directory.set_child_policy(&binding, false).unwrap();
        assert!(matches!(
            pending.begin(|| Ok(RepositoryWriterPreflight::Change)),
            Err(RepositoryCredentialError::StaleMutation)
        ));
        assert_eq!(
            test.admit(RepositoryCredentialUse::ChildGit).unwrap_err(),
            RepositoryCredentialError::ChildDisabled
        );
        test.policy().complete(true).unwrap();
    }
}

#[test]
fn authoritative_child_write_allows_fresh_owner_without_stale_drop_overwrite() {
    let test = Test::new();
    let binding = test.directory.binding().unwrap();
    let stale = test.policy();
    let old = stale.completion();
    test.directory.set_child_policy(&binding, false).unwrap();
    let fresh = test.policy();
    fresh.complete(true).unwrap();
    let child = test.admit(RepositoryCredentialUse::ChildGit).unwrap();
    drop(stale);
    assert!(matches!(
        old.indeterminate(),
        Err(RepositoryCredentialError::StaleMutation)
    ));
    assert!(matches!(
        old.complete(false),
        Err(RepositoryCredentialError::StaleMutation)
    ));
    assert!(test.directory.check_current(&child).is_ok());
}

#[test]
fn child_policy_ownership_is_shared_across_writer_adapters() {
    let test = Test::new();
    let other = RepositoryCredentialWriters::new(test.directory.clone());
    let binding = test.directory.binding().unwrap();
    let first = test.writers.reserve_child_policy(binding.clone()).unwrap();
    let second = other.reserve_child_policy(binding.clone()).unwrap();
    let active = first
        .begin(|| Ok(RepositoryWriterPreflight::Change))
        .unwrap()
        .unwrap();
    assert!(matches!(
        second.begin(|| Ok(RepositoryWriterPreflight::Change)),
        Err(RepositoryCredentialError::StaleMutation)
    ));
    assert!(matches!(
        other.reserve_child_policy(binding.clone()),
        Err(RepositoryCredentialError::Mutating)
    ));
    let completion = active.completion();
    drop(active);
    assert!(matches!(
        other.reserve_child_policy(binding.clone()),
        Err(RepositoryCredentialError::Indeterminate)
    ));
    test.directory.set_child_policy(&binding, false).unwrap();
    let fresh = other
        .reserve_child_policy(binding)
        .unwrap()
        .begin(|| Ok(RepositoryWriterPreflight::Change))
        .unwrap()
        .unwrap();
    fresh.complete(true).unwrap();
    let child = test.admit(RepositoryCredentialUse::ChildGit).unwrap();
    assert!(matches!(
        completion.complete(false),
        Err(RepositoryCredentialError::StaleMutation)
    ));
    assert!(test.directory.check_current(&child).is_ok());
}

#[test]
fn child_policy_checks_directory_revision_after_preflight_without_partial_retirement() {
    let test = Test::new();
    let binding = test.directory.binding().unwrap();
    let reservation = test.writers.reserve_child_policy(binding.clone()).unwrap();
    let mut current_child = None;
    assert!(matches!(
        reservation.begin(|| {
            test.directory.set_child_policy(&binding, true).unwrap();
            current_child = Some(test.admit(RepositoryCredentialUse::ChildGit).unwrap());
            Ok(RepositoryWriterPreflight::Change)
        }),
        Err(RepositoryCredentialError::StaleMutation)
    ));
    assert!(test
        .directory
        .check_current(&current_child.unwrap())
        .is_ok());
    test.policy().complete(false).unwrap();
}

#[test]
fn child_policy_stale_drop_cannot_mark_a_new_mutation_indeterminate() {
    let test = Test::new();
    let stale = test.policy();
    let old = stale.completion();
    test.directory
        .set_child_policy(&test.directory.binding().unwrap(), false)
        .unwrap();
    let fresh = test.policy();
    drop(stale);
    assert!(matches!(
        old.indeterminate(),
        Err(RepositoryCredentialError::StaleMutation)
    ));
    assert!(matches!(
        test.writers
            .reserve_child_policy(test.directory.binding().unwrap()),
        Err(RepositoryCredentialError::Mutating)
    ));
    fresh.complete(true).unwrap();
    assert!(test.admit(RepositoryCredentialUse::ChildGit).is_ok());
}
