//! Turn lifecycle: who holds an agent's in-flight slot, and the proof every
//! teardown needs.
//!
//! A claim mints a [`TurnAdmission`]. Only the holder of the current
//! admission can register a worker, take (abort) that worker, or release
//! the slot; every one of those calls is a no-op for a stale admission.
//! Retry and redrive decide whether a saved `Error` belongs to the turn in
//! the slot with [`TurnSlots::retry_ownership`], not by re-reading the store.
//!
//! The fields are private so code outside this module cannot release a slot
//! or abort a worker without naming the admission it targets. Before this
//! existed, each overlapping pair (Try again vs a new message, a failed
//! `Active` write, an interrupt releasing after its awaits) needed its own
//! fix (intentd#3694, #3723, #3739, #3742, #3747, #3749, #3754).

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, Weak};

use intent_core::{AgentId, AgentStatus};
use tokio::task::JoinHandle;

/// Proof of one claim of an agent's in-flight slot. Minted only by
/// [`SlotGuard::admit`]; a released or replaced admission is stale.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TurnAdmission(usize);

#[cfg(test)]
impl TurnAdmission {
    pub(crate) fn for_test(n: usize) -> Self {
        Self(n)
    }
}

#[derive(Debug, Clone, Copy)]
struct Held {
    admission: TurnAdmission,
    /// This turn is writing (or wrote) the saved `Error`: it is the failed
    /// turn a retry may tear down.
    failed: bool,
}

/// Who may restart a saved `Error`, decided from the slot, never the store
/// alone: a turn that started after the failure (even one whose `Active`
/// write failed) is not the failed turn.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RetryOwnership {
    /// The saved status is not `Error`.
    NotFailed,
    /// A turn that did not fail holds the slot: it started after the
    /// failure, so the caller leaves it running.
    StartedSince,
    /// The caller owns the restart. `failed_turn` is the failed turn still
    /// holding the slot, which the caller tears down with this admission.
    Owned {
        failed_turn: Option<TurnAdmission>,
    },
}

struct Inner {
    /// Lock order: `held` → the manager's per-agent maps (`reap_claims`,
    /// `live_turns`, `idle_since`, `agent_ws`, …) → `workers`.
    held: Mutex<HashMap<AgentId, Held>>,
    next: AtomicUsize,
    /// The worker running the holder's turn. Registered and taken only
    /// against the current admission.
    workers: Mutex<HashMap<AgentId, JoinHandle<()>>>,
}

/// The in-flight slots and turn workers of every agent of one manager.
#[derive(Clone)]
pub(crate) struct TurnSlots(Arc<Inner>);

impl Default for TurnSlots {
    fn default() -> Self {
        Self(Arc::new(Inner {
            held: Mutex::new(HashMap::new()),
            next: AtomicUsize::new(0),
            workers: Mutex::new(HashMap::new()),
        }))
    }
}

/// The slot table, locked. Claims and releases happen under this guard so
/// callers can keep their own bookkeeping atomic with them.
pub(crate) struct SlotGuard<'a> {
    held: MutexGuard<'a, HashMap<AgentId, Held>>,
    next: &'a AtomicUsize,
}

impl SlotGuard<'_> {
    pub(crate) fn contains(&self, agent_id: &AgentId) -> bool {
        self.held.contains_key(agent_id)
    }

    pub(crate) fn len(&self) -> usize {
        self.held.len()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.held.is_empty()
    }

    pub(crate) fn agents(&self) -> impl Iterator<Item = &AgentId> {
        self.held.keys()
    }

    /// Claim a free slot. The caller checked [`Self::contains`] under this
    /// same guard.
    pub(crate) fn admit(&mut self, agent_id: &AgentId) -> TurnAdmission {
        debug_assert!(!self.held.contains_key(agent_id), "slot already held");
        let admission = TurnAdmission(self.next.fetch_add(1, Ordering::Relaxed));
        self.held.insert(
            agent_id.clone(),
            Held {
                admission,
                failed: false,
            },
        );
        admission
    }

    /// Free the slot if `admission` still holds it.
    pub(crate) fn release(&mut self, agent_id: &AgentId, admission: TurnAdmission) -> bool {
        if self.held.get(agent_id).map(|held| held.admission) != Some(admission) {
            return false;
        }
        self.held.remove(agent_id);
        true
    }
}

/// A weak handle for background watchers that must not keep the manager
/// alive.
pub(crate) struct WeakTurnSlots(Weak<Inner>);

impl WeakTurnSlots {
    pub(crate) fn upgrade(&self) -> Option<TurnSlots> {
        self.0.upgrade().map(TurnSlots)
    }
}

impl TurnSlots {
    pub(crate) fn lock(&self) -> SlotGuard<'_> {
        SlotGuard {
            held: self.0.held.lock().unwrap(),
            next: &self.0.next,
        }
    }

    pub(crate) fn downgrade(&self) -> WeakTurnSlots {
        WeakTurnSlots(Arc::downgrade(&self.0))
    }

    pub(crate) fn is_busy(&self, agent_id: &AgentId) -> bool {
        self.lock().contains(agent_id)
    }

    /// The admission holding the slot now. A caller that acts on "the
    /// current turn" captures it when it decides and passes it to every
    /// later teardown, so a turn that started in between is left alone.
    pub(crate) fn holder(&self, agent_id: &AgentId) -> Option<TurnAdmission> {
        self.lock().held.get(agent_id).map(|held| held.admission)
    }

    pub(crate) fn owns(&self, agent_id: &AgentId, admission: TurnAdmission) -> bool {
        self.holder(agent_id) == Some(admission)
    }

    /// Mark the holder as the failed turn. Called BEFORE the `Error` write,
    /// so a retry that reads the written `Error` always finds the mark.
    pub(crate) fn mark_failing(&self, agent_id: &AgentId) {
        if let Some(held) = self.lock().held.get_mut(agent_id) {
            held.failed = true;
        }
    }

    /// A status other than `Error` was saved: the holder is no longer the
    /// failed turn.
    pub(crate) fn clear_failing(&self, agent_id: &AgentId) {
        if let Some(held) = self.lock().held.get_mut(agent_id) {
            held.failed = false;
        }
    }

    /// Who may restart `saved`. Call under the turn-start gate: claims take
    /// it too, so the holder cannot change to a new turn meanwhile.
    pub(crate) fn retry_ownership(&self, agent_id: &AgentId, saved: AgentStatus) -> RetryOwnership {
        if saved != AgentStatus::Error {
            return RetryOwnership::NotFailed;
        }
        match self.lock().held.get(agent_id) {
            None => RetryOwnership::Owned { failed_turn: None },
            Some(held) if held.failed => RetryOwnership::Owned {
                failed_turn: Some(held.admission),
            },
            Some(_) => RetryOwnership::StartedSince,
        }
    }

    /// Register the worker for `admission`. Refused (the handle is given
    /// back) when the admission is stale. Returns any handle it replaced.
    pub(crate) fn register_worker(
        &self,
        agent_id: &AgentId,
        admission: TurnAdmission,
        worker: JoinHandle<()>,
    ) -> Result<Option<JoinHandle<()>>, JoinHandle<()>> {
        let held = self.lock();
        if held.held.get(agent_id).map(|held| held.admission) != Some(admission) {
            return Err(worker);
        }
        let previous = self
            .0
            .workers
            .lock()
            .unwrap()
            .insert(agent_id.clone(), worker);
        drop(held);
        Ok(previous)
    }

    /// Take the worker of `admission` to abort it; `None` when the
    /// admission is stale or no worker is registered.
    pub(crate) fn take_worker(
        &self,
        agent_id: &AgentId,
        admission: TurnAdmission,
    ) -> Option<JoinHandle<()>> {
        self.take_turn_worker(agent_id, Some(admission))
    }

    /// Take the worker of `turn`, or with `None` a worker past its slot
    /// release (its tail may still re-claim). Never the worker of a turn
    /// other than `turn`.
    pub(crate) fn take_turn_worker(
        &self,
        agent_id: &AgentId,
        turn: Option<TurnAdmission>,
    ) -> Option<JoinHandle<()>> {
        let held = self.lock();
        if held.held.get(agent_id).map(|held| held.admission) != turn {
            return None;
        }
        let worker = self.0.workers.lock().unwrap().remove(agent_id);
        drop(held);
        worker
    }

    /// Deregister the calling task's own worker entry, never a replacement
    /// registered after this task released its slot (intent#6594).
    pub(crate) fn take_own_worker(&self, agent_id: &AgentId) -> Option<JoinHandle<()>> {
        let mut workers = self.0.workers.lock().unwrap();
        if workers
            .get(agent_id)
            .is_some_and(|worker| Some(worker.id()) == tokio::task::try_id())
        {
            workers.remove(agent_id)
        } else {
            None
        }
    }

    pub(crate) fn worker_agents(&self) -> Vec<AgentId> {
        self.0.workers.lock().unwrap().keys().cloned().collect()
    }

    /// Shutdown: abort every worker without deregistering it.
    pub(crate) fn abort_all_workers(&self) {
        for worker in self.0.workers.lock().unwrap().values() {
            worker.abort();
        }
    }

    /// Shutdown: take every worker.
    pub(crate) fn take_all_workers(&self) -> HashMap<AgentId, JoinHandle<()>> {
        std::mem::take(&mut *self.0.workers.lock().unwrap())
    }

    #[cfg(test)]
    pub(crate) fn live_worker_count(&self) -> usize {
        self.0
            .workers
            .lock()
            .unwrap()
            .values()
            .filter(|worker| !worker.is_finished())
            .count()
    }

    #[cfg(test)]
    pub(crate) fn has_workers(&self) -> bool {
        !self.0.workers.lock().unwrap().is_empty()
    }

    /// Register a worker with no admission check, for tests that stage one.
    #[cfg(test)]
    pub(crate) fn insert_worker_for_test(&self, agent_id: AgentId, worker: JoinHandle<()>) {
        self.0.workers.lock().unwrap().insert(agent_id, worker);
    }

    #[cfg(test)]
    pub(crate) fn has_worker_for_test(&self, agent_id: &AgentId) -> bool {
        self.0.workers.lock().unwrap().contains_key(agent_id)
    }

    #[cfg(test)]
    pub(crate) fn remove_worker_for_test(&self, agent_id: &AgentId) -> Option<JoinHandle<()>> {
        self.0.workers.lock().unwrap().remove(agent_id)
    }

    #[cfg(test)]
    pub(crate) fn worker_count_for_test(&self) -> usize {
        self.0.workers.lock().unwrap().len()
    }

    #[cfg(test)]
    pub(crate) fn worker_id_for_test(&self, agent_id: &AgentId) -> Option<tokio::task::Id> {
        self.0.workers.lock().unwrap().get(agent_id).map(JoinHandle::id)
    }

    #[cfg(test)]
    pub(crate) fn worker_abort_handle_for_test(
        &self,
        agent_id: &AgentId,
    ) -> Option<tokio::task::AbortHandle> {
        self.0
            .workers
            .lock()
            .unwrap()
            .get(agent_id)
            .map(JoinHandle::abort_handle)
    }

    #[cfg(test)]
    pub(crate) fn worker_finished_for_test(&self, agent_id: &AgentId) -> bool {
        self.0.workers.lock().unwrap()[agent_id].is_finished()
    }
}
