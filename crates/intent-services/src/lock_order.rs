//! Lock order between the workspace archive fence and the agent retirement
//! gates.
//!
//! **Order: [`ArchiveFence`](crate::ArchiveFence) first, then a
//! [`RetirementGate`].** Never wait on a workspace's archive fence while
//! holding a retirement gate. `script_monitor` (intent-svc-runtime) takes the
//! fence and then the agent's gate; a path taking them the other way round
//! can deadlock against it (intentd 62ed1acd, `agent_restore_op`, removed in
//! 112e980e).
//!
//! Debug builds record which task holds a retirement gate and panic when that
//! task then acquires an archive fence, so a reversed order fails in tests
//! instead of hanging a daemon. Release builds record nothing.

use std::future::Future;
use std::panic::Location;
use std::sync::Arc;

use tokio::sync::{Mutex, OwnedMutexGuard, TryLockError};

/// One agent's (or one workspace's) retirement gate. Take it only after any
/// archive fence the caller needs (see the module docs).
#[derive(Clone, Default)]
pub struct RetirementGate(Arc<Mutex<()>>);

impl RetirementGate {
    pub(crate) fn new(lock: Arc<Mutex<()>>) -> Self {
        Self(lock)
    }

    /// Wait for the gate.
    #[track_caller]
    pub fn lock(&self) -> impl Future<Output = RetirementGuard> + Send + '_ {
        let at = Location::caller();
        async move {
            let guard = Arc::clone(&self.0).lock_owned().await;
            RetirementGuard {
                _guard: guard,
                _hold: held::Hold::register(at),
            }
        }
    }

    /// Take the gate only if it is free.
    ///
    /// # Errors
    ///
    /// [`TryLockError`] when another caller holds the gate.
    #[track_caller]
    pub fn try_lock(&self) -> Result<RetirementGuard, TryLockError> {
        let at = Location::caller();
        let guard = Arc::clone(&self.0).try_lock_owned()?;
        Ok(RetirementGuard {
            _guard: guard,
            _hold: held::Hold::register(at),
        })
    }

    /// Whether both handles name the same gate.
    #[must_use]
    pub fn ptr_eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }

    #[cfg(test)]
    pub(crate) fn handle_count(&self) -> usize {
        Arc::strong_count(&self.0)
    }
}

/// A hold on a [`RetirementGate`]; releases it on drop.
pub struct RetirementGuard {
    _guard: OwnedMutexGuard<()>,
    _hold: held::Hold,
}

/// Panic (debug builds) when the current task holds a retirement gate:
/// `fence_at` is about to wait on an archive fence, which reverses the order.
pub(crate) fn check_archive_fence_order(fence_at: &Location<'_>) {
    #[cfg(debug_assertions)]
    if let Some(gate_at) = held::by_current_task() {
        panic!(
            "lock order violation: archive fence acquired at {fence_at} while this task holds \
             the retirement gate taken at {gate_at}; take the archive fence first \
             (see intent_services::lock_order)"
        );
    }
    #[cfg(not(debug_assertions))]
    let _ = fence_at;
}

#[cfg(debug_assertions)]
mod held {
    use std::panic::Location;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Mutex, PoisonError};
    use std::thread::ThreadId;

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

    type Entry = (u64, Holder, &'static Location<'static>);

    static HOLDS: Mutex<Vec<Entry>> = Mutex::new(Vec::new());
    static NEXT: AtomicU64 = AtomicU64::new(0);

    fn holds() -> std::sync::MutexGuard<'static, Vec<Entry>> {
        HOLDS.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Records the acquiring task, so a guard moved to another task still
    /// counts for the task that took it.
    pub(super) struct Hold(u64);

    impl Hold {
        pub(super) fn register(at: &'static Location<'static>) -> Self {
            let id = NEXT.fetch_add(1, Ordering::Relaxed);
            holds().push((id, current(), at));
            Self(id)
        }
    }

    impl Drop for Hold {
        fn drop(&mut self) {
            holds().retain(|(id, _, _)| *id != self.0);
        }
    }

    pub(super) fn by_current_task() -> Option<&'static Location<'static>> {
        let me = current();
        holds()
            .iter()
            .find(|(_, holder, _)| *holder == me)
            .map(|(_, _, at)| *at)
    }
}

#[cfg(not(debug_assertions))]
mod held {
    use std::panic::Location;

    pub(super) struct Hold;

    impl Hold {
        pub(super) fn register(_at: &'static Location<'static>) -> Self {
            Self
        }
    }
}

#[cfg(all(test, debug_assertions))]
mod tests {
    use std::sync::Arc;

    use intent_core::WorkspaceId;

    use super::RetirementGate;
    use crate::ArchiveFence;

    fn ws() -> WorkspaceId {
        WorkspaceId::from("ws-lock-order")
    }

    #[tokio::test]
    #[should_panic(expected = "lock order violation")]
    async fn archive_fence_after_a_retirement_gate_panics() {
        let fence = Arc::new(ArchiveFence::default());
        let gate = RetirementGate::default();
        let _retirement = gate.lock().await;
        let _archive = fence.acquire(&ws()).await;
    }

    #[tokio::test]
    #[should_panic(expected = "lock order violation")]
    async fn the_check_fires_before_waiting_on_a_held_fence() {
        let fence = Arc::new(ArchiveFence::default());
        let _held = fence.acquire(&ws()).await;
        let gate = RetirementGate::default();
        let _retirement = gate.try_lock().unwrap();
        let _archive = fence.acquire(&ws()).await;
    }

    #[tokio::test]
    async fn archive_fence_then_retirement_gate_is_allowed() {
        let fence = Arc::new(ArchiveFence::default());
        let gate = RetirementGate::default();
        let _archive = fence.acquire(&ws()).await;
        let _retirement = gate.lock().await;
    }

    #[tokio::test]
    async fn a_released_gate_no_longer_counts() {
        let fence = Arc::new(ArchiveFence::default());
        let first = RetirementGate::default();
        let second = RetirementGate::default();
        let a = first.lock().await;
        let b = second.lock().await;
        drop(a);
        drop(b);
        let _archive = fence.acquire(&ws()).await;
    }

    #[tokio::test]
    #[should_panic(expected = "lock order violation")]
    async fn one_released_gate_leaves_the_other_held() {
        let fence = Arc::new(ArchiveFence::default());
        let first = RetirementGate::default();
        let second = RetirementGate::default();
        let a = first.lock().await;
        let _b = second.lock().await;
        drop(a);
        let _archive = fence.acquire(&ws()).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_gate_held_by_another_task_does_not_count() {
        let fence = Arc::new(ArchiveFence::default());
        let gate = RetirementGate::default();
        let (held_tx, held_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let holder = {
            let gate = gate.clone();
            tokio::spawn(async move {
                let _retirement = gate.lock().await;
                held_tx.send(()).unwrap();
                release_rx.await.ok();
            })
        };
        held_rx.await.unwrap();
        drop(fence.acquire(&ws()).await);
        release_tx.send(()).unwrap();
        holder.await.unwrap();
    }
}
