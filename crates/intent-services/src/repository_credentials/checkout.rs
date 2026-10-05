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
            child_revision: None,
            use_kind: RepositoryCredentialUse::NativeRead,
        };
        action(&published.verified.descriptor, &selected, stamp)
    }
}
