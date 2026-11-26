//! Lifecycle leases: the cross-process half of the multi-step lifecycle
//! gates in `intent-services` (agent retirement, the archive fence and
//! workspace deletion admission).
//!
//! Each of those gates is an in-process lock that is held across several
//! write transactions and process work (stopping sessions, publishing
//! events), so the tenant writer lock cannot stand in for it. On a store
//! that several processes share (Postgres), the holder also holds a lease
//! on the gate's key for as long as it holds the local lock. A process that
//! finds the lease held waits for it ([`Store::hold_lease_when_free`]) the
//! way the local lock waits.
//!
//! `SQLite` has one process per database file, where the local lock alone
//! serializes the work, so no lease is taken there.
//!
//! Workspace deletion also closes admission in the database: while a
//! process holds the [`WORKSPACE_DELETION_SCOPE`] lease of a workspace,
//! every OTHER process's agent-session insert and user-message append in it
//! fails with `Error::NotFound` (checked inside that write's transaction;
//! see `postgres/lifecycle_leases.rs`). Writes of the deleting process stay
//! under its local admission gate.
//!
//! Limits: the work under a lifecycle lease is not fenced by it. If renewals
//! fail until the TTL passes (the database stopped answering), the keeper's
//! fence fires and is logged, but the work continues; another process may
//! then take the key. A holder that crashed blocks the key until the TTL
//! ([`LIFECYCLE_LEASE_TTL`]) passes, and so does an acquire whose caller was
//! cancelled after the database granted it.

use std::time::Duration;

use intent_core::Result;

use crate::{LeaseFence, LeaseKeeper, Store};

/// Agent retirement, restore and delete (key: agent id).
pub const AGENT_RETIREMENT_SCOPE: &str = "agent-retirement";
/// The last-top-level-agent retire guard (key: workspace id).
pub const WORKSPACE_RETIREMENT_SCOPE: &str = "workspace-retirement";
/// Workspace archive and unarchive (key: workspace id).
pub const ARCHIVE_FENCE_SCOPE: &str = "archive-fence";
/// An incremental workspace deletion (key: workspace id); see the module
/// docs.
pub const WORKSPACE_DELETION_SCOPE: &str = "workspace-deletion";

/// TTL of a lifecycle lease. The keeper renews it at half the TTL; a
/// crashed holder blocks its key for at most this long.
pub const LIFECYCLE_LEASE_TTL: Duration = Duration::from_secs(30);

/// The shortest pause between acquire attempts while another process holds
/// the key.
const WAIT_MIN: Duration = Duration::from_millis(20);
/// The longest pause between acquire attempts.
const WAIT_MAX: Duration = Duration::from_millis(500);

/// A held lifecycle lease, or nothing on a store that only one process uses.
/// Dropping it releases the lease in the background (best effort: a lease
/// whose release fails lapses after its TTL); [`Self::release`] releases it
/// before it returns.
#[must_use = "the lease is released when the hold drops"]
pub struct LifecycleLease {
    held: Option<(Store, LeaseKeeper)>,
}

impl LifecycleLease {
    /// A hold with no lease (a store only this process uses).
    pub fn local() -> Self {
        Self { held: None }
    }

    /// Whether a store lease is held.
    #[must_use]
    pub fn is_shared(&self) -> bool {
        self.held.is_some()
    }

    /// The fencing token of the held lease; `None` without one.
    #[must_use]
    pub fn token(&self) -> Option<u64> {
        self.held.as_ref().map(|(_, keeper)| keeper.token())
    }

    /// The keeper's fence; `None` without a lease.
    #[must_use]
    pub fn fence(&self) -> Option<LeaseFence> {
        self.held.as_ref().map(|(_, keeper)| keeper.fence())
    }

    /// Releases the lease now.
    pub async fn release(mut self) {
        if let Some((store, keeper)) = self.held.take() {
            keeper.release(&store).await;
        }
    }
}

impl Drop for LifecycleLease {
    fn drop(&mut self) {
        let Some((store, keeper)) = self.held.take() else {
            return;
        };
        if keeper.is_fired() {
            tracing::warn!(
                scope = keeper.scope(),
                key = keeper.key(),
                reason = keeper.fence().reason().unwrap_or_default(),
                "lifecycle lease was lost while its work ran"
            );
        }
        match tokio::runtime::Handle::try_current() {
            Ok(runtime) => {
                runtime.spawn(async move { keeper.release(&store).await });
            }
            // No runtime to release on: the lease lapses after its TTL.
            Err(_) => drop(keeper),
        }
    }
}

impl Store {
    /// Whether other processes share this store, so a lifecycle gate also
    /// needs its lease (Postgres). `false` on `SQLite`.
    #[must_use]
    pub fn lifecycle_leases_shared(&self) -> bool {
        match &self.backend {
            crate::Backend::Sqlite(_) => false,
            #[cfg(feature = "postgres")]
            crate::Backend::Postgres(_) => true,
        }
    }

    /// Holds the lifecycle lease (`scope`, `key`) with
    /// [`LIFECYCLE_LEASE_TTL`], waiting while another holder has it. On a
    /// store only this process uses it holds nothing and returns at once.
    ///
    /// # Errors
    ///
    /// As [`Store::acquire_lease`]: the database did not answer, and
    /// nothing is held.
    pub async fn hold_lifecycle_lease(&self, scope: &str, key: &str) -> Result<LifecycleLease> {
        if !self.lifecycle_leases_shared() {
            return Ok(LifecycleLease::local());
        }
        self.hold_lease_when_free(scope, key, LIFECYCLE_LEASE_TTL)
            .await
    }

    /// Holds (`scope`, `key`) for `ttl` through a [`LeaseKeeper`], waiting
    /// (polling with backoff) while another holder has it, on any backend.
    /// Cancel-safe, except that a lease the database granted to a cancelled
    /// call lapses only after `ttl`.
    ///
    /// # Errors
    ///
    /// As [`Store::acquire_lease`].
    pub async fn hold_lease_when_free(
        &self,
        scope: &str,
        key: &str,
        ttl: Duration,
    ) -> Result<LifecycleLease> {
        let mut pause = WAIT_MIN;
        loop {
            if let Some(keeper) = LeaseKeeper::acquire(self, scope, key, ttl).await? {
                return Ok(LifecycleLease {
                    held: Some((self.clone(), keeper)),
                });
            }
            #[cfg(any(test, feature = "test-support"))]
            contended::record(scope, key);
            tokio::time::sleep(pause).await;
            pause = (pause * 2).min(WAIT_MAX);
        }
    }
}

/// How many acquire attempts of each (`scope`, `key`) found the key held,
/// in this process: a test waits on it to know a waiter is really waiting.
#[cfg(any(test, feature = "test-support"))]
mod contended {
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};

    fn counts() -> &'static Mutex<HashMap<(String, String), u64>> {
        static COUNTS: OnceLock<Mutex<HashMap<(String, String), u64>>> = OnceLock::new();
        COUNTS.get_or_init(Mutex::default)
    }

    pub(super) fn record(scope: &str, key: &str) {
        *counts()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .entry((scope.to_owned(), key.to_owned()))
            .or_default() += 1;
    }

    pub(super) fn get(scope: &str, key: &str) -> u64 {
        counts()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&(scope.to_owned(), key.to_owned()))
            .copied()
            .unwrap_or_default()
    }
}

/// Tests and `test-support` builds: how many times a waiter in this process
/// found (`scope`, `key`) held by another holder
/// ([`Store::hold_lease_when_free`]). Counts are per process, not per store,
/// so a test uses keys of its own.
#[cfg(any(test, feature = "test-support"))]
#[must_use]
pub fn contended_lease_attempts_for_tests(scope: &str, key: &str) -> u64 {
    contended::get(scope, key)
}
