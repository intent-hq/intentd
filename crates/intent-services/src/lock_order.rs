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
mod tests {
    use std::sync::Arc;

    use intent_core::WorkspaceId;

    use super::{OrderedGate, Rank};
    use crate::ArchiveFence;

    fn ws() -> WorkspaceId {
        WorkspaceId::from("ws-lock-order")
    }

    fn turn_start() -> OrderedGate {
        OrderedGate::new(Rank::TurnStartGate, Arc::default())
    }

    #[tokio::test]
    async fn the_documented_order_is_allowed() {
        let fence = Arc::new(ArchiveFence::default());
        let retirement = OrderedGate::default();
        let starting = turn_start();
        let _r = retirement.lock().await;
        let _t = starting.lock().await;
        let _f = fence.acquire(&ws()).await;
    }

    #[tokio::test]
    #[should_panic(expected = "lock order violation: RetirementGate")]
    async fn a_retirement_gate_under_an_archive_fence_panics() {
        let fence = Arc::new(ArchiveFence::default());
        let _f = fence.acquire(&ws()).await;
        let _r = OrderedGate::default().lock().await;
    }

    #[tokio::test]
    #[should_panic(expected = "lock order violation: TurnStartGate")]
    async fn a_turn_start_gate_under_an_archive_fence_panics() {
        let fence = Arc::new(ArchiveFence::default());
        let _f = fence.acquire(&ws()).await;
        let _t = turn_start().lock().await;
    }

    #[tokio::test]
    #[should_panic(expected = "lock order violation: RetirementGate")]
    async fn a_retirement_gate_under_a_turn_start_gate_panics() {
        let _t = turn_start().lock().await;
        let _r = OrderedGate::default().try_lock().unwrap();
    }

    #[tokio::test]
    #[should_panic(expected = "lock order violation")]
    async fn the_check_fires_before_waiting_on_a_held_gate() {
        let fence = Arc::new(ArchiveFence::default());
        let gate = OrderedGate::default();
        let other = {
            let gate = gate.clone();
            tokio::spawn(async move { gate.lock().await })
                .await
                .unwrap()
        };
        let _f = fence.acquire(&ws()).await;
        let _r = gate.lock().await;
        drop(other);
    }

    #[tokio::test]
    async fn in_order_paths_joined_in_one_task_are_allowed() {
        let fence = Arc::new(ArchiveFence::default());
        let in_order = || async {
            let _r = OrderedGate::default().lock().await;
            let _f = fence.acquire(&ws()).await;
            tokio::task::yield_now().await;
        };
        tokio::join!(in_order(), in_order());
    }

    #[tokio::test]
    #[should_panic(expected = "lock order violation: RetirementGate")]
    async fn a_hold_whose_earlier_lock_was_released_counts() {
        let fence = Arc::new(ArchiveFence::default());
        let first = OrderedGate::default().lock().await;
        let _f = fence.acquire(&ws()).await;
        drop(first);
        let _r = OrderedGate::default().lock().await;
    }

    #[tokio::test]
    #[should_panic(expected = "lock order violation: TurnStartGate")]
    async fn a_turn_start_gate_under_a_fence_taken_under_a_retirement_gate_panics() {
        let fence = Arc::new(ArchiveFence::default());
        let _r = OrderedGate::default().lock().await;
        let _f = fence.acquire(&ws()).await;
        let _t = turn_start().lock().await;
    }

    #[tokio::test]
    async fn a_released_hold_no_longer_counts() {
        let fence = Arc::new(ArchiveFence::default());
        drop(fence.acquire(&ws()).await);
        drop(turn_start().lock().await);
        let _r = OrderedGate::default().lock().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_lock_held_by_another_task_does_not_count() {
        let fence = Arc::new(ArchiveFence::default());
        let (held_tx, held_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let holder = {
            let fence = Arc::clone(&fence);
            tokio::spawn(async move {
                let _f = fence.acquire(&ws()).await;
                held_tx.send(()).unwrap();
                release_rx.await.ok();
            })
        };
        held_rx.await.unwrap();
        drop(OrderedGate::default().lock().await);
        release_tx.send(()).unwrap();
        holder.await.unwrap();
    }
}
