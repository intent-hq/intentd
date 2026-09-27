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

    pub(crate) fn set_child_policy(
        &self,
        binding: &RepositoryConnectionBinding,
        enabled: bool,
    ) -> Result<()> {
        let mut state = self.lock()?;
        if &state.ready()?.binding != binding {
            return Err(RepositoryCredentialError::Retired);
        }
        if state.child_enabled != enabled {
            let current = state.child_revision;
            state.child_revision = state.advance(current)?;
            state.child_enabled = enabled;
        }
        Ok(())
    }

    /// Retire the directory at daemon shutdown. Old admissions cannot be rebound.
    pub(crate) fn retire(&self) -> Result<()> {
        let mut state = self.lock()?;
        state.status = RepositoryConnectionState::Retired;
        state.reservation = None;
        state.active = None;
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
            RepositoryCredentialError::CounterExhausted
        })
    }
}
