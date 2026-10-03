use super::*;
use crate::repository_credentials::{RepositoryChildPolicyCheckpoint, RepositoryChildPolicyTicket};

/// Only preflight reservations are local. Active ownership and effective policy
/// live together in the directory. Lock order is slot -> directory at begin,
/// completion -> directory at settlement; no callback or await runs under either.
#[derive(Default)]
pub(super) struct PolicySlot {
    reservation: Option<Arc<()>>,
}

pub(super) fn reserve(
    directory: Arc<RepositoryConnectionDirectory>,
    slot: Arc<Mutex<PolicySlot>>,
    binding: RepositoryConnectionBinding,
) -> Result<RepositoryChildPolicyReservation> {
    let owner = Arc::new(());
    let checkpoint = {
        let mut state = slot
            .lock()
            .map_err(|_| RepositoryCredentialError::Indeterminate)?;
        let checkpoint = directory.child_policy_checkpoint(binding)?;
        state.reservation = Some(owner.clone());
        checkpoint
    };
    Ok(RepositoryChildPolicyReservation {
        directory,
        slot,
        owner,
        checkpoint,
    })
}

pub(crate) struct RepositoryChildPolicyReservation {
    directory: Arc<RepositoryConnectionDirectory>,
    slot: Arc<Mutex<PolicySlot>>,
    owner: Arc<()>,
    checkpoint: RepositoryChildPolicyCheckpoint,
}
impl RepositoryChildPolicyReservation {
    /// Fence matching child admissions BEFORE a real policy write/publication.
    /// Both enable and disable stay disabled until the original owner settles.
    pub(crate) fn begin(
        self,
        validate: impl FnOnce() -> Result<RepositoryWriterPreflight>,
    ) -> Result<Option<RepositoryChildPolicyMutation>> {
        if validate()? == RepositoryWriterPreflight::NoChange {
            return Ok(None);
        }
        let mut state = self
            .slot
            .lock()
            .map_err(|_| RepositoryCredentialError::Indeterminate)?;
        if !state
            .reservation
            .as_ref()
            .is_some_and(|id| Arc::ptr_eq(id, &self.owner))
        {
            return Err(RepositoryCredentialError::StaleMutation);
        }
        let ticket = self.directory.begin_child_policy(&self.checkpoint)?;
        state.reservation = None;
        Ok(Some(RepositoryChildPolicyMutation {
            completion: RepositoryChildPolicyCompletion(Arc::new(Completion {
                directory: self.directory.clone(),
                ticket,
                settled: Mutex::new(false),
            })),
        }))
    }
}
impl Drop for RepositoryChildPolicyReservation {
    fn drop(&mut self) {
        if let Ok(mut slot) = self.slot.lock() {
            if slot
                .reservation
                .as_ref()
                .is_some_and(|id| Arc::ptr_eq(id, &self.owner))
            {
                slot.reservation = None;
            }
        }
    }
}

pub(crate) struct RepositoryChildPolicyMutation {
    completion: RepositoryChildPolicyCompletion,
}
impl RepositoryChildPolicyMutation {
    pub(crate) fn completion(&self) -> RepositoryChildPolicyCompletion {
        self.completion.clone()
    }
    pub(crate) fn complete(self, settled_enabled: bool) -> Result<()> {
        self.completion.complete(settled_enabled)
    }
}
impl Drop for RepositoryChildPolicyMutation {
    fn drop(&mut self) {
        let _ = self.completion.indeterminate();
    }
}

struct Completion {
    directory: Arc<RepositoryConnectionDirectory>,
    ticket: RepositoryChildPolicyTicket,
    settled: Mutex<bool>,
}
#[derive(Clone)]
pub(crate) struct RepositoryChildPolicyCompletion(Arc<Completion>);
impl RepositoryChildPolicyCompletion {
    /// Actual settled policy, including proved compensation, for the original
    /// full binding. An enabled replacement account is never inferred from it.
    pub(crate) fn complete(&self, settled_enabled: bool) -> Result<()> {
        let mut finished = self
            .0
            .settled
            .lock()
            .map_err(|_| RepositoryCredentialError::Indeterminate)?;
        if *finished {
            return Err(RepositoryCredentialError::StaleMutation);
        }
        let result = self
            .0
            .directory
            .finish_child_policy(&self.0.ticket, settled_enabled);
        if result.is_ok()
            || matches!(
                result,
                Err(RepositoryCredentialError::Retired
                    | RepositoryCredentialError::Disconnected
                    | RepositoryCredentialError::StaleMutation)
            )
        {
            *finished = true;
        }
        result
    }

    pub(crate) fn indeterminate(&self) -> Result<()> {
        let finished = self
            .0
            .settled
            .lock()
            .map_err(|_| RepositoryCredentialError::Indeterminate)?;
        if *finished {
            return Ok(());
        }
        self.0
            .directory
            .mark_child_policy_indeterminate(&self.0.ticket)
    }
}
