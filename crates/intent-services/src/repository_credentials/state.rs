use super::*;

impl RepositoryConnectionDirectory {
    pub(crate) fn new(daemon_id: String) -> Self {
        Self {
            epoch: Uuid::new_v4(),
            daemon_id,
            state: Mutex::new(State {
                status: RepositoryConnectionState::Unverified,
                published: None,
                generation: 0,
                secret_revision: 0,
                child_revision: 0,
                child_enabled: false,
                child_active: None,
                mutation_id: 0,
                reservation: None,
                active: None,
                backoff_until: None,
            }),
        }
    }

    pub(crate) fn reserve_mutation(
        &self,
        kind: RepositoryMutationKind,
    ) -> Result<RepositoryMutationTicket> {
        let mut state = self.lock()?;
        if state.active.is_some() {
            return Err(state.unavailable());
        }
        if state.status == RepositoryConnectionState::Retired {
            return Err(RepositoryCredentialError::Retired);
        }
        if kind == RepositoryMutationKind::Refresh {
            state.ready()?;
        }
        let current = state.mutation_id;
        state.mutation_id = state.advance(current)?;
        state.reservation = Some(state.mutation_id);
        Ok(RepositoryMutationTicket {
            epoch: self.epoch,
            id: state.mutation_id,
            generation: state.generation,
            secret_revision: state.secret_revision,
            kind,
        })
    }

    pub(crate) fn cancel_reservation(&self, ticket: &RepositoryMutationTicket) -> Result<()> {
        let mut state = self.lock()?;
        self.owns_reservation(&state, ticket)?;
        state.reservation = None;
        Ok(())
    }

    fn owns_reservation(&self, state: &State, ticket: &RepositoryMutationTicket) -> Result<()> {
        if ticket.epoch != self.epoch
            || state.reservation != Some(ticket.id)
            || ticket.generation != state.generation
            || ticket.secret_revision != state.secret_revision
            || state.active.is_some()
        {
            return Err(RepositoryCredentialError::StaleMutation);
        }
        Ok(())
    }

    pub(crate) fn begin_mutation(&self, ticket: &RepositoryMutationTicket) -> Result<()> {
        let mut state = self.lock()?;
        self.owns_reservation(&state, ticket)?;
        if ticket.kind != RepositoryMutationKind::Refresh {
            let current = state.generation;
            state.generation = state.advance(current)?;
            state.child_active = None;
        }
        state.active = Some(Mutation {
            id: ticket.id,
            kind: ticket.kind,
            previous: state.published.clone(),
            previous_child_enabled: state.child_enabled,
        });
        state.reservation = None;
        state.status = RepositoryConnectionState::Mutating;
        Ok(())
    }

    /// Revalidates the original, still-unsettled writer immediately before an
    /// admitted effect. The existing writer retains its own gate through the
    /// effect; this read-only check neither settles nor recalls admitted work.
    pub(crate) fn check_mutation(&self, ticket: &RepositoryMutationTicket) -> Result<()> {
        let state = self.lock()?;
        if !matches!(
            state.status,
            RepositoryConnectionState::Mutating | RepositoryConnectionState::Indeterminate
        ) || ticket.epoch != self.epoch
            || !state
                .active
                .as_ref()
                .is_some_and(|active| active.id == ticket.id && active.kind == ticket.kind)
        {
            return Err(RepositoryCredentialError::StaleMutation);
        }
        Ok(())
    }

    /// Publishes metadata only. The caller already held the existing writer gates
    /// through persistence/verification/compensation; no callback or await occurs here.
    pub(crate) fn finish_mutation(
        &self,
        ticket: &RepositoryMutationTicket,
        settled: SettledCredentialState,
    ) -> Result<Option<RepositoryConnectionBinding>> {
        let mut state = self.lock()?;
        let active = state
            .active
            .as_ref()
            .ok_or(RepositoryCredentialError::StaleMutation)?;
        if ticket.epoch != self.epoch || active.id != ticket.id || active.kind != ticket.kind {
            return Err(RepositoryCredentialError::StaleMutation);
        }
        let previous = active.previous.clone();
        let previous_child_enabled = active.previous_child_enabled;
        match settled {
            SettledCredentialState::Indeterminate => {
                state.status = RepositoryConnectionState::Indeterminate;
                return Ok(None);
            }
            SettledCredentialState::Disconnected => {
                let current = state.generation;
                state.generation = state.advance(current)?;
                let current = state.child_revision;
                state.child_revision = state.advance(current)?;
                state.published = None;
                state.child_enabled = false;
                state.child_active = None;
                state.status = RepositoryConnectionState::Disconnected;
                state.backoff_until = None;
            }
            SettledCredentialState::Verified(verified) => {
                self.publish(
                    &mut state,
                    ticket,
                    verified,
                    previous,
                    previous_child_enabled,
                    false,
                )?;
            }
            SettledCredentialState::Compensated(verified) => {
                self.publish(
                    &mut state,
                    ticket,
                    verified,
                    previous,
                    previous_child_enabled,
                    true,
                )?;
            }
        }
        state.active = None;
        Ok(state.published.as_ref().map(|p| p.binding.clone()))
    }

    fn publish(
        &self,
        state: &mut State,
        ticket: &RepositoryMutationTicket,
        verified: VerifiedRepositoryAccount,
        previous: Option<Published>,
        previous_child_enabled: bool,
        compensated: bool,
    ) -> Result<()> {
        if ticket.kind == RepositoryMutationKind::Disconnect && !compensated {
            state.status = RepositoryConnectionState::Indeterminate;
            return Err(RepositoryCredentialError::Unverified);
        }
        let same = previous.as_ref().is_some_and(|p| p.verified == verified);
        let requested_refresh = ticket.kind == RepositoryMutationKind::Refresh && !compensated;
        let refresh = requested_refresh && state.generation == ticket.generation;
        if (requested_refresh || compensated) && !same {
            let current = state.generation;
            state.generation = state.advance(current)?;
            state.status = RepositoryConnectionState::Indeterminate;
            return Err(RepositoryCredentialError::Unverified);
        }
        if compensated {
            let current = state.generation;
            state.generation = state.advance(current)?;
        }
        let current = state.secret_revision;
        state.secret_revision = state.advance(current)?;
        let binding = if refresh {
            previous
                .ok_or(RepositoryCredentialError::Unverified)?
                .binding
        } else {
            let current = state.child_revision;
            state.child_revision = state.advance(current)?;
            state.child_enabled = same && previous_child_enabled;
            state.child_active = None;
            RepositoryConnectionBinding {
                daemon_id: self.daemon_id.clone(),
                account: RepositoryAccountKey {
                    provider: RepositoryProvider::Gitlab,
                    instance_base_url: verified.descriptor.instance().as_str().into(),
                    account_id: verified.account_id.clone(),
                },
                scope: RepositoryConnectionScope {
                    connection_id: Uuid::new_v4().to_string(),
                    account_id: verified.account_id.clone(),
                    connection_generation: state.generation,
                },
            }
        };
        state.published = Some(Published { binding, verified });
        state.status = RepositoryConnectionState::Ready;
        if !refresh {
            state.backoff_until = None;
        }
        Ok(())
    }

    pub(crate) fn binding(&self) -> Result<RepositoryConnectionBinding> {
        let state = self.lock()?;
        Ok(state.ready()?.binding.clone())
    }

    /// A proved authoritative write, including a same-value write, supersedes
    /// pending child work. Rejected/no-op preflight must not call this method.
    pub(crate) fn set_child_policy(
        &self,
        binding: &RepositoryConnectionBinding,
        enabled: bool,
    ) -> Result<()> {
        let mut state = self.lock()?;
        if &state.ready()?.binding != binding {
            return Err(RepositoryCredentialError::Retired);
        }
        let current = state.child_revision;
        state.child_revision = state.advance(current)?;
        state.child_enabled = enabled;
        state.child_active = None;
        Ok(())
    }

    pub(crate) fn child_policy_checkpoint(
        &self,
        binding: RepositoryConnectionBinding,
    ) -> Result<RepositoryChildPolicyCheckpoint> {
        let state = self.lock()?;
        if state.ready()?.binding != binding {
            return Err(RepositoryCredentialError::Retired);
        }
        state.child_idle()?;
        Ok(RepositoryChildPolicyCheckpoint {
            epoch: self.epoch,
            binding,
            revision: state.child_revision,
        })
    }

    /// Conditional retirement precedes the existing owner's first policy effect.
    /// Native admission, connection generation and secret revision are untouched.
    pub(crate) fn begin_child_policy(
        &self,
        checkpoint: &RepositoryChildPolicyCheckpoint,
    ) -> Result<RepositoryChildPolicyTicket> {
        let mut state = self.lock()?;
        self.check_child_checkpoint(&state, checkpoint)?;
        state.ready()?;
        state.child_idle()?;
        let current = state.child_revision;
        state.child_revision = state.advance(current)?;
        state.child_enabled = false;
        state.child_active = Some(ChildPolicyMutation {
            revision: state.child_revision,
            indeterminate: false,
        });
        Ok(RepositoryChildPolicyTicket {
            checkpoint: RepositoryChildPolicyCheckpoint {
                epoch: self.epoch,
                binding: checkpoint.binding.clone(),
                revision: state.child_revision,
            },
        })
    }

    /// Only the still-current writer can publish its proved settlement. An
    /// indeterminate writer may later settle; a superseded one never can.
    pub(crate) fn finish_child_policy(
        &self,
        ticket: &RepositoryChildPolicyTicket,
        enabled: bool,
    ) -> Result<()> {
        let mut state = self.lock()?;
        self.owns_child_policy(&state, ticket)?;
        state.ready()?;
        state.child_enabled = enabled;
        state.child_active = None;
        Ok(())
    }

    /// Dropped/detached work remains child-disabled, including during refresh.
    /// A late drop cannot change a newer policy or connection lifetime.
    pub(crate) fn mark_child_policy_indeterminate(
        &self,
        ticket: &RepositoryChildPolicyTicket,
    ) -> Result<()> {
        let mut state = self.lock()?;
        self.owns_child_policy(&state, ticket)?;
        if let Some(active) = &mut state.child_active {
            active.indeterminate = true;
        }
        Ok(())
    }

    fn check_child_checkpoint(
        &self,
        state: &State,
        checkpoint: &RepositoryChildPolicyCheckpoint,
    ) -> Result<()> {
        if checkpoint.epoch != self.epoch
            || state.status == RepositoryConnectionState::Retired
            || state.generation != checkpoint.binding.scope.connection_generation
            || !state
                .published
                .as_ref()
                .is_some_and(|p| p.binding == checkpoint.binding)
        {
            return Err(RepositoryCredentialError::Retired);
        }
        if state.child_revision != checkpoint.revision {
            return Err(RepositoryCredentialError::StaleMutation);
        }
        Ok(())
    }

    fn owns_child_policy(&self, state: &State, ticket: &RepositoryChildPolicyTicket) -> Result<()> {
        self.check_child_checkpoint(state, &ticket.checkpoint)?;
        if state
            .child_active
            .as_ref()
            .is_none_or(|a| a.revision != ticket.checkpoint.revision)
        {
            return Err(RepositoryCredentialError::StaleMutation);
        }
        Ok(())
    }

    /// Retire the directory at daemon shutdown. Old admissions cannot be rebound.
    pub(crate) fn retire(&self) -> Result<()> {
        let mut state = self.lock()?;
        state.status = RepositoryConnectionState::Retired;
        state.reservation = None;
        state.active = None;
        state.child_active = None;
        Ok(())
    }

    /// A response under an older token revision cannot disconnect a newer token.
    /// This only fences volatile eligibility; auth state/deletion stays writer-owned.
    pub(crate) fn reject_current_credential(
        &self,
        stamp: &RepositoryDispatchStamp,
    ) -> Result<bool> {
        let mut state = self.lock()?;
        if !self.current_stamp(&state, stamp) || state.secret_revision != stamp.secret_revision {
            return Ok(false);
        }
        let current = state.generation;
        state.generation = state.advance(current)?;
        state.status = RepositoryConnectionState::Disconnected;
        state.child_enabled = false;
        state.child_active = None;
        state.reservation = None;
        Ok(true)
    }

    /// Connection quota can arrive after refresh has paused credential release.
    /// Preserve it provisionally for that same binding; settlement clears it if
    /// replacement/compensation/disconnect creates a different lifetime. Unknown
    /// settlement accepts no further receipts, and never permits acquisition.
    pub(crate) fn record_backoff(
        &self,
        stamp: &RepositoryDispatchStamp,
        until: Instant,
    ) -> Result<bool> {
        let mut state = self.lock()?;
        if !self.current_stamp(&state, stamp) && !self.refreshing_stamp(&state, stamp) {
            return Ok(false);
        }
        state.backoff_until = Some(state.backoff_until.map_or(until, |old| old.max(until)));
        Ok(true)
    }

    fn current_stamp(&self, state: &State, stamp: &RepositoryDispatchStamp) -> bool {
        state.status == RepositoryConnectionState::Ready && self.matches_stamp(state, stamp)
    }

    // Quota bookkeeping only. In particular, credential rejection must continue
    // using the Ready-only predicate and the exact current secret revision.
    fn refreshing_stamp(&self, state: &State, stamp: &RepositoryDispatchStamp) -> bool {
        state.status == RepositoryConnectionState::Mutating
            && self.matches_stamp(state, stamp)
            && state.active.as_ref().is_some_and(|active| {
                active.kind == RepositoryMutationKind::Refresh
                    && active.previous.as_ref().is_some_and(|previous| {
                        state.published.as_ref().is_some_and(|published| {
                            previous.binding == published.binding
                                && previous.verified == published.verified
                        })
                    })
            })
    }

    fn matches_stamp(&self, state: &State, stamp: &RepositoryDispatchStamp) -> bool {
        stamp.epoch == self.epoch
            && state
                .published
                .as_ref()
                .is_some_and(|p| p.binding == stamp.binding)
            && state.generation == stamp.binding.scope.connection_generation
            && (stamp.use_kind != RepositoryCredentialUse::ChildGit
                || stamp.child_revision == Some(state.child_revision) && state.child_enabled)
    }

    pub(super) fn lock(&self) -> Result<std::sync::MutexGuard<'_, State>> {
        self.state
            .lock()
            .map_err(|_| RepositoryCredentialError::Indeterminate)
    }
}

impl State {
    fn child_idle(&self) -> Result<()> {
        match &self.child_active {
            Some(active) if active.indeterminate => Err(RepositoryCredentialError::Indeterminate),
            Some(_) => Err(RepositoryCredentialError::Mutating),
            None => Ok(()),
        }
    }
    fn unavailable(&self) -> RepositoryCredentialError {
        match self.status {
            RepositoryConnectionState::Unverified => RepositoryCredentialError::Unverified,
            RepositoryConnectionState::Ready => RepositoryCredentialError::Missing,
            RepositoryConnectionState::Mutating => RepositoryCredentialError::Mutating,
            RepositoryConnectionState::Disconnected => RepositoryCredentialError::Disconnected,
            RepositoryConnectionState::Indeterminate => RepositoryCredentialError::Indeterminate,
            RepositoryConnectionState::Retired => RepositoryCredentialError::Retired,
        }
    }
    pub(super) fn ready(&self) -> Result<&Published> {
        if self.status != RepositoryConnectionState::Ready {
            return Err(self.unavailable());
        }
        self.published
            .as_ref()
            .ok_or(RepositoryCredentialError::Missing)
    }
    fn advance(&mut self, value: u64) -> Result<u64> {
        value.checked_add(1).ok_or_else(|| {
            self.status = RepositoryConnectionState::Retired;
            self.reservation = None;
            self.active = None;
            self.child_active = None;
            RepositoryCredentialError::CounterExhausted
        })
    }
}
