//! Checkout response metadata uses the same directory and original selection.
//! Caller authority is supplied by the pre-workspace owner, never invented here.
use super::*;

impl RepositoryConnectionDirectory {
    pub(crate) fn with_checkout_metadata<T>(
        &self,
        original: &RepositoryConnectionBinding,
        expected: Option<&RepositorySecretRequest>,
        dispatch: bool,
        action: impl FnOnce(
            &GitlabDescriptor,
            &RepositorySecretRequest,
            RepositoryDispatchStamp,
        ) -> Result<T>,
    ) -> Result<T> {
        self.with_checkout_child_metadata(original, expected, dispatch, None, action)
    }

    pub(crate) fn with_checkout_child_metadata<T>(
        &self,
        original: &RepositoryConnectionBinding,
        expected: Option<&RepositorySecretRequest>,
        dispatch: bool,
        child_revision: Option<u64>,
        action: impl FnOnce(
            &GitlabDescriptor,
            &RepositorySecretRequest,
            RepositoryDispatchStamp,
        ) -> Result<T>,
    ) -> Result<T> {
        let state = self.lock()?;
        if state.status == RepositoryConnectionState::Retired
            || original.daemon_id != self.daemon_id
            || state.generation != original.scope.connection_generation
            || state
                .published
                .as_ref()
                .is_none_or(|p| p.binding != *original)
        {
            return Err(RepositoryCredentialError::Retired);
        }
        let published = state.ready()?;
        if let Some(revision) = child_revision {
            state.child_idle()?;
            if revision != state.child_revision {
                return Err(RepositoryCredentialError::Retired);
            }
            if !state.child_enabled {
                return Err(RepositoryCredentialError::ChildDisabled);
            }
        }
        let selected = RepositorySecretRequest {
            binding: published.binding.clone(),
            secret_revision: state.secret_revision,
            source: published.verified.source,
        };
        if expected.is_some_and(|expected| *expected != selected) {
            return Err(RepositoryCredentialError::SecretMismatch);
        }
        if dispatch
            && state
                .backoff_until
                .is_some_and(|until| until > Instant::now())
        {
            return Err(RepositoryCredentialError::Backoff);
        }
        let stamp = RepositoryDispatchStamp {
            epoch: self.epoch,
            binding: selected.binding.clone(),
            secret_revision: selected.secret_revision,
            child_revision,
            use_kind: if child_revision.is_some() {
                RepositoryCredentialUse::ChildGit
            } else {
                RepositoryCredentialUse::NativeRead
            },
        };
        action(&published.verified.descriptor, &selected, stamp)
    }

    /// Metadata output accepts disabled policy too, but retains the original
    /// binding and child mutation/revision guards through the caller's action.
    pub(crate) fn with_child_policy_metadata<T>(
        &self,
        original: &RepositoryConnectionBinding,
        expected: Option<(bool, u64)>,
        action: impl FnOnce(&GitlabDescriptor, &RepositorySecretRequest, bool, u64) -> Result<T>,
    ) -> Result<T> {
        let state = self.lock()?;
        if state.status == RepositoryConnectionState::Retired
            || original.daemon_id != self.daemon_id
            || state.generation != original.scope.connection_generation
            || state
                .published
                .as_ref()
                .is_none_or(|p| p.binding != *original)
        {
            return Err(RepositoryCredentialError::Retired);
        }
        let published = state.ready()?;
        state.child_idle()?;
        if expected.is_some_and(|choice| choice != (state.child_enabled, state.child_revision)) {
            return Err(RepositoryCredentialError::Retired);
        }
        action(
            &published.verified.descriptor,
            &RepositorySecretRequest {
                binding: published.binding.clone(),
                secret_revision: state.secret_revision,
                source: published.verified.source,
            },
            state.child_enabled,
            state.child_revision,
        )
    }

    /// Hydration from the original writer's durable, account-qualified choice.
    /// Unchanged refresh never advances a child revision or clears a pending owner.
    pub(crate) fn restore_child_policy(
        &self,
        binding: &RepositoryConnectionBinding,
        enabled: bool,
    ) -> Result<()> {
        let mut state = self.lock()?;
        if &state.ready()?.binding != binding {
            return Err(RepositoryCredentialError::Retired);
        }
        state.child_idle()?;
        if state.child_enabled != enabled {
            let current = state.child_revision;
            state.child_revision = state.advance(current)?;
            state.child_enabled = enabled;
        }
        Ok(())
    }
}
