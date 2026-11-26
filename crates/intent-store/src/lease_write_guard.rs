//! Fencing every write that work under a lease makes.
//!
//! [`LeaseKeeper::guard_writes`](crate::LeaseKeeper::guard_writes) runs a
//! future with the kept lease in a task-local set. On Postgres, every write
//! transaction that future opens on a store of the lease's tenant
//! (`begin_write`, the owner-checked writer) guards each lease in the set
//! right after the writer lock (`guard_lease`: the lease row, `FOR SHARE`).
//! A holder that lost its lease (expired by the database clock, or taken by
//! another process) then fails with `Error::LeaseLost` and writes nothing,
//! and no other process can acquire the lease before the write commits.
//! Lease writes themselves (`Fence::LeaseOnly`) and tenant claims are not
//! guarded.
//!
//! The set follows the future, not the thread: a task the work spawns
//! keeps it only through [`inherit_lease_write_guards`], and a blocking
//! closure only through [`LeaseWriteGuards::sync_scope`].
//!
//! `SQLite` writes are not guarded: one process uses the database, and its
//! in-process leases are only lost when the holder stops renewing.

use std::future::Future;
use std::sync::Arc;

use crate::Lease;

/// One guarded lease: its identity, and the Postgres tenant whose writes
/// guard it (`None` for `SQLite`, whose writes guard nothing).
pub(crate) struct GuardedLease {
    #[cfg_attr(
        not(feature = "postgres"),
        expect(dead_code, reason = "only Postgres writes guard leases")
    )]
    pub(crate) tenant: Option<String>,
    pub(crate) lease: Lease,
}

tokio::task_local! {
    static GUARDS: LeaseWriteGuards;
}

/// The leases that the running work's writes guard; see the module docs.
#[derive(Clone, Default)]
pub struct LeaseWriteGuards(Arc<Vec<Arc<GuardedLease>>>);

impl LeaseWriteGuards {
    /// The guards of the running task; empty outside
    /// [`LeaseKeeper::guard_writes`](crate::LeaseKeeper::guard_writes).
    #[must_use]
    pub fn current() -> Self {
        GUARDS.try_with(Clone::clone).unwrap_or_default()
    }

    /// Whether no lease is guarded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Runs `fut` with these guards, for work moved into another task.
    pub async fn scope<F: Future>(self, fut: F) -> F::Output {
        GUARDS.scope(self, fut).await
    }

    /// Runs `f` with these guards, for a blocking closure (a secret-store
    /// write on a blocking thread).
    pub fn sync_scope<R>(self, f: impl FnOnce() -> R) -> R {
        GUARDS.sync_scope(self, f)
    }

    fn with(&self, guard: Arc<GuardedLease>) -> Self {
        let mut guards = Vec::clone(&self.0);
        guards.push(guard);
        Self(Arc::new(guards))
    }

    /// The guarded leases a write of `tenant` checks.
    #[cfg(feature = "postgres")]
    pub(crate) fn for_tenant<'a>(&'a self, tenant: &'a str) -> impl Iterator<Item = &'a Lease> {
        self.0
            .iter()
            .filter(move |g| g.tenant.as_deref() == Some(tenant))
            .map(|g| &g.lease)
    }
}

/// Runs `fut` with the guards of the running task: wrap a future before
/// spawning it so its writes keep guarding the caller's leases.
pub fn inherit_lease_write_guards<F: Future>(fut: F) -> impl Future<Output = F::Output> {
    LeaseWriteGuards::current().scope(fut)
}

/// Runs `fut` with `guard` added to the guards of the task that polls it.
pub(crate) async fn guard_scope<F: Future>(guard: Arc<GuardedLease>, fut: F) -> F::Output {
    LeaseWriteGuards::current().with(guard).scope(fut).await
}
