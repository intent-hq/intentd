//! Lock order for the agent and workspace lifecycle locks.
//!
//! **Order: retirement gate → turn-start gate → archive fence.** Never wait
//! on a lock while holding one that comes later in this list.
//!
//! - Retirement gates (`AgentRetirementGates`, per agent and per workspace):
//!   retire holds one while it stops the runtime (which takes the turn-start
//!   gate); restore holds one while it kicks the queue drain (turn-start
//!   gate, then the archive fence for an archived workspace).
//! - Turn-start gates (`AgentManager::turn_start_gates`): a turn start takes
//!   the archive fence under it (`Services::turn_start_archive_fence`).
//! - The archive fence ([`crate::ArchiveFence`]): nothing holding it takes a
//!   gate. `script_monitor` (intent-svc-runtime) takes the agent's
//!   retirement gate first, then the fence.
//!
//! A path taking two of these the other way round can deadlock: before this
//! order was written down, `script_monitor` took the fence first and formed a
//! cycle with retire and turn start.
//!
//! Debug builds record which task holds each lock and panic when a task
//! acquires a lock while it holds a later-ranked one, so a reversed order
//! fails in tests instead of hanging a daemon. The check runs before the
//! wait. Release builds record nothing.
//!
//! Limits of the debug check:
//! - A later-ranked hold H is skipped when the lock held just before it in
//!   the same task (H's "under" hold) is still held and ranks at or after the
//!   lock being acquired. This keeps in-order paths joined in one task
//!   (`tokio::join!`) from tripping the check. Holds are attributed per task,
//!   not per joined branch, so a reversed branch joined with an in-order one
//!   can be missed, and a hold recorded under another branch's lock can be
//!   flagged once that branch releases it.
//! - Holds belong to the task that acquired them; a guard moved into another
//!   task still counts for the acquiring task, and the receiving task does
//!   not see it.
//! - Two locks of the same rank (two agents' gates, two workspaces' fences)
//!   are not ordered against each other and are not checked.

use std::future::Future;
use std::panic::Location;
use std::sync::Arc;

use tokio::sync::{Mutex, OwnedMutexGuard, TryLockError};

/// The locks in the order, earliest first.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Rank {
    RetirementGate,
    TurnStartGate,
    ArchiveFence,
}

/// One gate in the lock order (see the module docs).
#[derive(Clone)]
pub struct OrderedGate {
    lock: Arc<Mutex<()>>,
    rank: Rank,
}

impl Default for OrderedGate {
    /// A fresh, unshared retirement gate.
    fn default() -> Self {
        Self::new(Rank::RetirementGate, Arc::default())
    }
}

impl OrderedGate {
    pub(crate) fn new(rank: Rank, lock: Arc<Mutex<()>>) -> Self {
        Self { lock, rank }
    }

    /// Wait for the gate. Debug builds panic first when the calling task
    /// holds a later-ranked lock.
    #[track_caller]
    pub fn lock(&self) -> impl Future<Output = OrderedGuard> + Send + '_ {
        let at = Location::caller();
        async move {
            check(self.rank, at);
            let guard = Arc::clone(&self.lock).lock_owned().await;
            OrderedGuard {
                _guard: guard,
                _hold: hold(self.rank, at),
            }
        }
    }

    /// Take the gate only if it is free.
    ///
    /// # Errors
    ///
    /// [`TryLockError`] when another caller holds the gate.
    #[track_caller]
    pub fn try_lock(&self) -> Result<OrderedGuard, TryLockError> {
        let at = Location::caller();
        check(self.rank, at);
        let guard = Arc::clone(&self.lock).try_lock_owned()?;
        Ok(OrderedGuard {
            _guard: guard,
            _hold: hold(self.rank, at),
        })
    }

    /// Whether both handles name the same gate.
    #[must_use]
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.lock, &other.lock)
    }

    #[cfg(test)]
    pub(crate) fn handle_count(&self) -> usize {
        Arc::strong_count(&self.lock)
    }
}

/// A retirement gate: an [`OrderedGate`] of rank `RetirementGate` plus, on
/// a store other processes share, the store lease of the same target (see
/// `intent_store::lifecycle_leases`), taken after the local gate.
#[derive(Clone, Default)]
pub struct RetirementGate {
    gate: OrderedGate,
    lease: Option<(intent_store::Store, &'static str, String)>,
}

impl RetirementGate {
    pub(crate) fn new(
        gate: OrderedGate,
        lease: Option<(intent_store::Store, &'static str, String)>,
    ) -> Self {
        Self { gate, lease }
    }

    /// Wait for the local gate, then for the store lease. Debug builds
    /// panic first when the calling task holds a later-ranked lock.
    ///
    /// # Errors
    ///
    /// The lease could not be read (the database did not answer); nothing
    /// is held.
    #[track_caller]
    pub fn lock(&self) -> impl Future<Output = intent_core::Result<RetirementGuard>> + Send + '_ {
        let local = self.gate.lock();
        async move {
            let guard = local.await;
            let lease = match &self.lease {
                Some((store, scope, key)) => store.hold_lifecycle_lease(scope, key).await?,
                None => intent_store::LifecycleLease::local(),
            };
            Ok(RetirementGuard {
                lease,
                _guard: guard,
            })
        }
    }

    /// Whether both handles name the same local gate.
    #[must_use]
    pub fn ptr_eq(&self, other: &Self) -> bool {
        self.gate.ptr_eq(&other.gate)
    }

    #[cfg(test)]
    pub(crate) fn handle_count(&self) -> usize {
        self.gate.handle_count()
    }
}

/// A hold on a [`RetirementGate`]. Drop releases the lease (in the
/// background), then the local gate.
pub struct RetirementGuard {
    lease: intent_store::LifecycleLease,
    _guard: OrderedGuard,
}

impl RetirementGuard {
    /// Runs `fut` with its store writes fenced by this hold's lease (see
    /// `crate::lifecycle_fence`).
    pub fn guard_writes<F: Future>(&self, fut: F) -> impl Future<Output = F::Output> {
        self.lease.guard_writes(fut)
    }

    /// `Ok` while the lease is held.
    ///
    /// # Errors
    ///
    /// `Error::LeaseLost` once it was lost: stop the retirement.
    pub fn check(&self) -> intent_core::Result<()> {
        self.lease.check()
    }
}

/// A hold on an [`OrderedGate`]; releases it on drop.
pub struct OrderedGuard {
    _guard: OwnedMutexGuard<()>,
    _hold: LockHold,
}

/// Debug-build record of one held lock; see the module docs.
pub(crate) struct LockHold {
    _hold: held::Hold,
}

/// Record a hold of `rank`, taken at `at`, for the current task.
pub(crate) fn hold(rank: Rank, at: &'static Location<'static>) -> LockHold {
    LockHold {
        _hold: held::Hold::new(rank, at),
    }
}

/// Panic (debug builds) when the current task holds a lock ranked after
/// `rank`: acquiring `rank` at `at` would reverse the order.
pub(crate) fn check(rank: Rank, at: &Location<'_>) {
    #[cfg(debug_assertions)]
    if let Some((held_rank, held_at)) = held::later_than(rank) {
        panic!(
            "lock order violation: {rank:?} acquired at {at} while this task holds the \
             {held_rank:?} taken at {held_at}; the order is retirement gate → turn-start gate \
             → archive fence (see intent_services::lock_order)"
        );
    }
    #[cfg(not(debug_assertions))]
    let _ = (rank, at);
}

#[cfg(debug_assertions)]
mod held {
    use std::panic::Location;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Mutex, PoisonError};
    use std::thread::ThreadId;

    use super::Rank;

    /// A tokio task, or the thread of a `block_on` root future (no task id).
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Holder {
        Task(tokio::task::Id),
        Thread(ThreadId),
    }

    fn current() -> Holder {
        tokio::task::try_id()
            .map_or_else(|| Holder::Thread(std::thread::current().id()), Holder::Task)
    }

    struct Entry {
        id: u64,
        holder: Holder,
        rank: Rank,
        /// An earlier-ranked hold of the same task, held when this one was
        /// taken: while it stays held, this hold is in order.
        under: Option<u64>,
        at: &'static Location<'static>,
    }

    static HOLDS: Mutex<Vec<Entry>> = Mutex::new(Vec::new());
    static NEXT: AtomicU64 = AtomicU64::new(0);

    fn holds() -> std::sync::MutexGuard<'static, Vec<Entry>> {
        HOLDS.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(super) struct Hold(u64);

    impl Hold {
        pub(super) fn new(rank: Rank, at: &'static Location<'static>) -> Self {
            let id = NEXT.fetch_add(1, Ordering::Relaxed);
            let holder = current();
            let mut holds = holds();
            let under = holds
                .iter()
                .rev()
                .find(|e| e.holder == holder && e.rank < rank)
                .map(|e| e.id);
            holds.push(Entry {
                id,
                holder,
                rank,
                under,
                at,
            });
            Self(id)
        }
    }

    impl Drop for Hold {
        fn drop(&mut self) {
            holds().retain(|e| e.id != self.0);
        }
    }

    /// A hold of the current task ranked after `rank` that is not excused by
    /// a still-held "under" hold ranked at or after `rank`.
    pub(super) fn later_than(rank: Rank) -> Option<(Rank, &'static Location<'static>)> {
        let me = current();
        let holds = holds();
        let excused = |under: u64| holds.iter().any(|e| e.id == under && e.rank >= rank);
        holds
            .iter()
            .find(|e| e.holder == me && e.rank > rank && !e.under.is_some_and(excused))
            .map(|e| (e.rank, e.at))
    }
}

#[cfg(not(debug_assertions))]
mod held {
    use std::panic::Location;

    use super::Rank;

    pub(super) struct Hold;

    impl Hold {
        pub(super) fn new(_rank: Rank, _at: &'static Location<'static>) -> Self {
            Self
        }
    }
}

#[cfg(all(test, debug_assertions))]
mod tests;
