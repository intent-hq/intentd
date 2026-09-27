//! Private invalidation registry for the original request retirement leaves.
//!
//! Store coordinates only invalidation. Neither an installed observer nor a
//! matching key authorizes an operation. Real physical-origin producers and
//! uncovered writers remain required before production entry registration.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, Weak};

use intent_core::caller::Caller;
use intent_store::{
    RepositoryLifecycleKey, RepositoryLifecycleMutationTicket, RepositoryLifecycleObserver, Store,
};

use super::{AdmissionError, AdmissionResult, RepositoryRetirement};

#[derive(Default)]
struct State {
    next_id: u64,
    pending: HashMap<u64, HashSet<RepositoryLifecycleKey>>,
    subscriptions: HashMap<u64, Subscription>,
    retiring: HashMap<u64, Subscription>,
    origins: HashMap<u64, Origin>,
}

impl State {
    fn next(&mut self) -> AdmissionResult<u64> {
        self.next_id = self
            .next_id
            .checked_add(1)
            .ok_or(AdmissionError::Unavailable)?;
        Ok(self.next_id)
    }

    fn blocked(&self, keys: &HashSet<RepositoryLifecycleKey>) -> bool {
        self.pending.values().any(|pending| overlaps(pending, keys))
    }

    fn detach(&mut self, matches: impl Fn(&Subscription) -> bool) {
        let mut detached = Vec::new();
        self.subscriptions.retain(|id, entry| {
            if matches(entry) {
                detached.push((*id, entry.clone()));
                false
            } else {
                true
            }
        });
        self.retiring.extend(detached);
    }

    fn pending_leaves(
        &self,
        matches: impl Fn(&Subscription) -> bool,
    ) -> Vec<(u64, RepositoryRetirement)> {
        self.retiring
            .iter()
            .filter(|(_, entry)| matches(entry))
            .map(|(id, entry)| (*id, entry.retirement.clone()))
            .collect()
    }
}

#[derive(Clone)]
struct Subscription {
    origin: u64,
    keys: HashSet<RepositoryLifecycleKey>,
    retirement: RepositoryRetirement,
}

struct Origin {
    token: Weak<()>,
    caller: Caller,
    keys: HashSet<RepositoryLifecycleKey>,
    retired: bool,
}

fn overlaps(
    mutation: &HashSet<RepositoryLifecycleKey>,
    keys: &HashSet<RepositoryLifecycleKey>,
) -> bool {
    mutation.contains(&RepositoryLifecycleKey::Database) || !mutation.is_disjoint(keys)
}

/// The sole observer allocation installed in a managed Store domain.
#[derive(Default)]
pub(crate) struct RepositoryLifecycleRegistry {
    state: Arc<Mutex<State>>,
}

/// The physical owner supplies this original allocation. No production
/// constructor exists in this increment: a current AgentId/row is insufficient.
#[derive(Clone)]
pub(crate) struct RepositoryPhysicalOrigin {
    registry: Weak<RepositoryLifecycleRegistry>,
    id: u64,
    token: Weak<()>,
}

/// Caller-owned session input, never a serialized or default permission grant.
pub(crate) struct RepositorySourceLifetime {
    registry: Arc<RepositoryLifecycleRegistry>,
    origin: Option<RepositoryPhysicalOrigin>,
    retirement: RepositoryRetirement,
}

impl RepositorySourceLifetime {
    pub(crate) fn new(
        registry: Arc<RepositoryLifecycleRegistry>,
        origin: Option<RepositoryPhysicalOrigin>,
        retirement: RepositoryRetirement,
    ) -> Self {
        Self {
            registry,
            origin,
            retirement,
        }
    }

    pub(crate) fn retirement(&self) -> RepositoryRetirement {
        self.retirement.clone()
    }

    pub(crate) fn subscribe(
        &self,
        store: &Store,
        caller: &Caller,
        keys: &[RepositoryLifecycleKey],
    ) -> AdmissionResult<RepositorySubscription> {
        let observer: Arc<dyn RepositoryLifecycleObserver> = self.registry.clone();
        if !store.has_repository_lifecycle_observer(&observer) {
            return Err(AdmissionError::Unavailable);
        }
        self.registry.subscribe(
            self.origin.as_ref().ok_or(AdmissionError::Unavailable)?,
            caller,
            keys,
            self.retirement.clone(),
        )
    }
}

pub(crate) struct RepositorySubscription {
    state: Arc<Mutex<State>>,
    id: u64,
    retirement: RepositoryRetirement,
}

impl Drop for RepositorySubscription {
    fn drop(&mut self) {
        {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(entry) = state.subscriptions.remove(&self.id) {
                state.retiring.insert(self.id, entry);
            }
        }
        finish_retirement(&self.state, &[(self.id, self.retirement.clone())]);
    }
}

fn finish_retirement(state: &Mutex<State>, leaves: &[(u64, RepositoryRetirement)]) {
    // Detached leaves remain discoverable by EVERY concurrent writer until
    // permanently retired. No registry lock crosses a leaf wait.
    for (_, leaf) in leaves {
        leaf.retire();
    }
    let mut state = state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    for (id, _) in leaves {
        state.retiring.remove(id);
    }
}

impl RepositoryLifecycleRegistry {
    pub(super) fn capture_request(
        self: &Arc<Self>,
        origin: &RepositoryPhysicalOrigin,
        retirement: RepositoryRetirement,
    ) -> AdmissionResult<(Caller, RepositorySubscription)> {
        let (caller, keys) = {
            let state = self.state.lock().map_err(|_| AdmissionError::Retired)?;
            let physical = state
                .origins
                .get(&origin.id)
                .ok_or(AdmissionError::Retired)?;
            (
                physical.caller.clone(),
                physical.keys.iter().cloned().collect::<Vec<_>>(),
            )
        };
        let subscription = self.subscribe(origin, &caller, &keys, retirement)?;
        Ok((caller, subscription))
    }

    /// Interrupt the requests already captured by this allocation. A later
    /// request may use the same live physical allocation after normal Idle.
    pub(super) fn cancel_origin_requests(&self, origin: &RepositoryPhysicalOrigin) {
        let retire = {
            let mut state = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let Some(physical) = state.origins.get(&origin.id) else {
                return;
            };
            if !Weak::ptr_eq(&physical.token, &origin.token) {
                return;
            }
            state.detach(|entry| entry.origin == origin.id);
            state.pending_leaves(|entry| entry.origin == origin.id)
        };
        finish_retirement(&self.state, &retire);
    }

    pub(crate) async fn install(self: &Arc<Self>, store: &Store) -> AdmissionResult<()> {
        store
            .install_repository_lifecycle_observer(self.clone())
            .await
            .map_err(|_| AdmissionError::Unavailable)
    }

    fn subscribe(
        self: &Arc<Self>,
        origin: &RepositoryPhysicalOrigin,
        caller: &Caller,
        keys: &[RepositoryLifecycleKey],
        retirement: RepositoryRetirement,
    ) -> AdmissionResult<RepositorySubscription> {
        let bound = origin.registry.upgrade().ok_or(AdmissionError::Retired)?;
        if !Arc::ptr_eq(self, &bound) {
            return Err(AdmissionError::Denied);
        }
        retirement.check_current()?;
        let keys: HashSet<_> = keys.iter().cloned().collect();
        if !keys.contains(&RepositoryLifecycleKey::Database) {
            return Err(AdmissionError::Unavailable);
        }
        let mut state = self.state.lock().map_err(|_| AdmissionError::Retired)?;
        let physical = state
            .origins
            .get(&origin.id)
            .ok_or(AdmissionError::Retired)?;
        if physical.retired
            || physical.token.upgrade().is_none()
            || origin.token.upgrade().is_none()
        {
            return Err(AdmissionError::Retired);
        }
        if &physical.caller != caller || !Weak::ptr_eq(&physical.token, &origin.token) {
            return Err(AdmissionError::Denied);
        }
        if state.blocked(&keys) {
            return Err(AdmissionError::Unavailable);
        }
        let id = state.next()?;
        state.subscriptions.insert(
            id,
            Subscription {
                origin: origin.id,
                keys,
                retirement: retirement.clone(),
            },
        );
        drop(state);
        Ok(RepositorySubscription {
            state: self.state.clone(),
            id,
            retirement,
        })
    }
}

struct Mutation {
    state: Arc<Mutex<State>>,
    id: u64,
}

impl RepositoryLifecycleMutationTicket for Mutation {
    fn settle_confirmed(self: Box<Self>) {
        if let Ok(mut state) = self.state.lock() {
            state.pending.remove(&self.id);
        }
    }
}

// No Drop settlement: cancellation or an unknown outcome keeps these keys
// blocked. Reconciliation requires a later original-owner integration.
impl RepositoryLifecycleObserver for RepositoryLifecycleRegistry {
    fn begin_mutation(
        &self,
        keys: &[RepositoryLifecycleKey],
    ) -> intent_core::Result<Box<dyn RepositoryLifecycleMutationTicket>> {
        let denied = || intent_core::Error::Internal("repository lifecycle unavailable".into());
        if keys.is_empty() {
            return Err(denied());
        }
        let keys: HashSet<_> = keys.iter().cloned().collect();
        let mut state = self.state.lock().map_err(|_| denied())?;
        let id = state.next().map_err(|_| denied())?;
        state.pending.insert(id, keys.clone());
        let mut origins = HashSet::new();
        for (origin_id, origin) in &mut state.origins {
            if overlaps(&keys, &origin.keys) {
                origin.retired = true;
                origins.insert(*origin_id);
            }
        }
        let affected =
            |entry: &Subscription| origins.contains(&entry.origin) || overlaps(&keys, &entry.keys);
        state.detach(affected);
        let retire = state.pending_leaves(affected);
        drop(state);
        // The Store writer cannot proceed until every detached leaf is retired.
        finish_retirement(&self.state, &retire);
        Ok(Box::new(Mutation {
            state: self.state.clone(),
            id,
        }))
    }
}

/// Explicit fixture owner, not an actual manager/session-origin integration.
/// Production deliberately has no equivalent constructor in this increment.
#[cfg(test)]
pub(crate) struct FixtureOriginOwner {
    registry: Arc<RepositoryLifecycleRegistry>,
    id: u64,
    token: Arc<()>,
}

#[cfg(test)]
impl FixtureOriginOwner {
    pub(crate) fn new(
        registry: &Arc<RepositoryLifecycleRegistry>,
        caller: Caller,
    ) -> AdmissionResult<Self> {
        let mut keys = HashSet::from([RepositoryLifecycleKey::Database]);
        if let Caller::Agent { agent_id } = &caller {
            keys.insert(RepositoryLifecycleKey::Agent(agent_id.clone()));
        }
        let token = Arc::new(());
        let mut state = registry.state.lock().map_err(|_| AdmissionError::Retired)?;
        if state.blocked(&keys) {
            return Err(AdmissionError::Unavailable);
        }
        let id = state.next()?;
        state.origins.insert(
            id,
            Origin {
                token: Arc::downgrade(&token),
                caller,
                keys,
                retired: false,
            },
        );
        Ok(Self {
            registry: registry.clone(),
            id,
            token,
        })
    }

    pub(crate) fn origin(&self) -> RepositoryPhysicalOrigin {
        RepositoryPhysicalOrigin {
            registry: Arc::downgrade(&self.registry),
            id: self.id,
            token: Arc::downgrade(&self.token),
        }
    }
}

#[cfg(test)]
impl Drop for FixtureOriginOwner {
    fn drop(&mut self) {
        let retire = {
            let mut state = self
                .registry
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            state.origins.remove(&self.id);
            state.detach(|entry| entry.origin == self.id);
            state.pending_leaves(|entry| entry.origin == self.id)
        };
        finish_retirement(&self.registry.state, &retire);
    }
}

#[cfg(test)]
#[path = "lifecycle/tests.rs"]
mod tests;
