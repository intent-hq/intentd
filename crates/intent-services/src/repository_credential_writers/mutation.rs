use super::*;

/// Dropping an unstarted operation cancels only its own reservation. A newer
/// reservation remains owned by its original caller.
pub(crate) struct RepositoryWriterReservation {
    pub(super) directory: Arc<RepositoryConnectionDirectory>,
    pub(super) ticket: Option<RepositoryMutationTicket>,
}

impl RepositoryWriterReservation {
    /// `validate` performs only the existing owner's synchronous final check.
    /// The owner must keep its existing writer gates through the first effect;
    /// this adapter adds no lock around the callback or across secret/network I/O.
    pub(crate) fn begin(
        mut self,
        validate: impl FnOnce() -> Result<RepositoryWriterPreflight>,
    ) -> Result<Option<RepositoryWriterMutation>> {
        if validate()? == RepositoryWriterPreflight::NoChange {
            return Ok(None);
        }
        let ticket = self
            .ticket
            .as_ref()
            .ok_or(RepositoryCredentialError::StaleMutation)?;
        self.directory.begin_mutation(ticket)?;
        let ticket = self
            .ticket
            .take()
            .ok_or(RepositoryCredentialError::StaleMutation)?;
        Ok(Some(RepositoryWriterMutation {
            completion: RepositoryWriterCompletion(Arc::new(Completion {
                directory: self.directory.clone(),
                ticket,
                settled: Mutex::new(false),
            })),
        }))
    }
}

impl Drop for RepositoryWriterReservation {
    fn drop(&mut self) {
        if let Some(ticket) = &self.ticket {
            let _ = self.directory.cancel_reservation(ticket);
        }
    }
}

/// Held across the existing write/batch/compensation. If the caller times out,
/// its existing detached writer must retain `completion()` until it settles.
/// Dropping this guard closes availability, never rolls back or assumes success.
pub(crate) struct RepositoryWriterMutation {
    completion: RepositoryWriterCompletion,
}

impl RepositoryWriterMutation {
    /// Checks original ownership again at the actual write handoff, including
    /// when an earlier exchange already began this mutation.
    pub(crate) fn check_current(&self) -> Result<()> {
        let finished = self
            .completion
            .0
            .settled
            .lock()
            .map_err(|_| RepositoryCredentialError::Indeterminate)?;
        if *finished {
            return Err(RepositoryCredentialError::StaleMutation);
        }
        self.completion
            .0
            .directory
            .check_mutation(&self.completion.0.ticket)
    }

    pub(crate) fn completion(&self) -> RepositoryWriterCompletion {
        self.completion.clone()
    }

    pub(crate) fn complete(
        self,
        settled: SettledCredentialState,
    ) -> Result<Option<RepositoryConnectionBinding>> {
        self.completion.complete(settled)
    }
}

impl Drop for RepositoryWriterMutation {
    fn drop(&mut self) {
        let _ = self.completion.indeterminate();
    }
}

struct Completion {
    directory: Arc<RepositoryConnectionDirectory>,
    ticket: RepositoryMutationTicket,
    settled: Mutex<bool>,
}

/// A capability retained only by the original persistence/compensation owner.
/// Clones share one terminal settlement. None contains a credential or a store.
#[derive(Clone)]
pub(crate) struct RepositoryWriterCompletion(Arc<Completion>);

impl RepositoryWriterCompletion {
    /// `Verified`/`Compensated` must attest the exact token/descriptor/account
    /// after the original writer has settled, including siblings and settings.
    /// An uncertain result leaves the SAME owner able to report settlement later.
    pub(crate) fn complete(
        &self,
        settled: SettledCredentialState,
    ) -> Result<Option<RepositoryConnectionBinding>> {
        let mut finished = self
            .0
            .settled
            .lock()
            .map_err(|_| RepositoryCredentialError::Indeterminate)?;
        if *finished {
            return Err(RepositoryCredentialError::StaleMutation);
        }
        let terminal = !matches!(settled, SettledCredentialState::Indeterminate);
        let binding = self.0.directory.finish_mutation(&self.0.ticket, settled)?;
        *finished = terminal;
        Ok(binding)
    }

    pub(crate) fn indeterminate(&self) -> Result<()> {
        let finished = self
            .0
            .settled
            .lock()
            .map_err(|_| RepositoryCredentialError::Indeterminate)?;
        if !*finished {
            self.0
                .directory
                .finish_mutation(&self.0.ticket, SettledCredentialState::Indeterminate)?;
        }
        Ok(())
    }
}
