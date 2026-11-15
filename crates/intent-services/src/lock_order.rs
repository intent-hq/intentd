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
//! Debug builds record which task holds each fence and gate, and panic when a
//! task acquires an archive fence while it holds a retirement gate it took
//! with no fence held, so a reversed order fails in tests instead of hanging
//! a daemon. A gate taken under a fence is in order: it is skipped, so two
//! in-order paths joined in one task (`tokio::join!`) do not trip the check.
//! Release builds record nothing.

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
                _hold: held::Hold::gate(at),
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
            _hold: held::Hold::gate(at),
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

/// Debug-build record of one held archive fence; see the module docs.
pub(crate) struct FenceHold {
    _hold: held::Hold,
}

/// Check the order, then record the fence hold the caller is about to take.
/// Panics (debug builds) when the current task holds a retirement gate it
/// took with no fence held: `fence_at` reverses the order.
pub(crate) fn enter_archive_fence(fence_at: &'static Location<'static>) -> FenceHold {
    #[cfg(debug_assertions)]
    if let Some(gate_at) = held::unordered_gate() {
        panic!(
            "lock order violation: archive fence acquired at {fence_at} while this task holds \
             the retirement gate taken at {gate_at}; take the archive fence first \
             (see intent_services::lock_order)"
        );
    }
    FenceHold {
        _hold: held::Hold::fence(fence_at),
    }
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

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum Kind {
        Fence,
        /// `under_fence`: the task held a fence when it took the gate.
        Gate {
            under_fence: bool,
        },
    }

    struct Entry {
        id: u64,
        holder: Holder,
        kind: Kind,
        at: &'static Location<'static>,
    }

    static HOLDS: Mutex<Vec<Entry>> = Mutex::new(Vec::new());
    static NEXT: AtomicU64 = AtomicU64::new(0);

    fn holds() -> std::sync::MutexGuard<'static, Vec<Entry>> {
        HOLDS.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Records the acquiring task, so a guard moved to another task still
    /// counts for the task that took it.
    pub(crate) struct Hold(u64);

    impl Hold {
        fn register(
            kind: impl FnOnce(&[Entry], Holder) -> Kind,
            at: &'static Location<'static>,
        ) -> Self {
            let id = NEXT.fetch_add(1, Ordering::Relaxed);
            let holder = current();
            let mut holds = holds();
            let kind = kind(&holds, holder);
            holds.push(Entry {
                id,
                holder,
                kind,
                at,
            });
            Self(id)
        }

        pub(super) fn fence(at: &'static Location<'static>) -> Self {
            Self::register(|_, _| Kind::Fence, at)
        }

        pub(super) fn gate(at: &'static Location<'static>) -> Self {
            Self::register(
                |holds, holder| Kind::Gate {
                    under_fence: holds
                        .iter()
                        .any(|e| e.holder == holder && e.kind == Kind::Fence),
                },
                at,
            )
        }
    }

    impl Drop for Hold {
        fn drop(&mut self) {
            holds().retain(|e| e.id != self.0);
        }
    }

    /// A gate the current task holds and took with no fence held.
    pub(super) fn unordered_gate() -> Option<&'static Location<'static>> {
        let me = current();
        holds()
            .iter()
            .find(|e| e.holder == me && e.kind == Kind::Gate { under_fence: false })
            .map(|e| e.at)
    }
}

#[cfg(not(debug_assertions))]
mod held {
    use std::panic::Location;

    pub(crate) struct Hold;

    impl Hold {
        pub(super) fn fence(_at: &'static Location<'static>) -> Self {
            Self
        }

        pub(super) fn gate(_at: &'static Location<'static>) -> Self {
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
        let other = {
            let fence = Arc::clone(&fence);
            tokio::spawn(async move { fence.acquire(&ws()).await })
                .await
                .unwrap()
        };
        let gate = RetirementGate::default();
        let _retirement = gate.try_lock().unwrap();
        let _archive = fence.acquire(&ws()).await;
        drop(other);
    }

    #[tokio::test]
    async fn archive_fence_then_retirement_gate_is_allowed() {
        let fence = Arc::new(ArchiveFence::default());
        let gate = RetirementGate::default();
        let _archive = fence.acquire(&ws()).await;
        let _retirement = gate.lock().await;
    }

    #[tokio::test]
    async fn in_order_paths_joined_in_one_task_are_allowed() {
        let fence = Arc::new(ArchiveFence::default());
        let gate = RetirementGate::default();
        let in_order = || async {
            let _archive = fence.acquire(&ws()).await;
            let _retirement = gate.lock().await;
            tokio::task::yield_now().await;
        };
        tokio::join!(in_order(), in_order());
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
