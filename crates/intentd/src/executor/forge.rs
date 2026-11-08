//! The backend's GitHub token on this machine (nodeProtocol 15, node-link.md
//! § Forge token on the machine): held in memory only, never written to
//! disk, and dropped on a new `headSession`, a revoked credential, a
//! machine restart or a `forgeToken` without a value.
//!
//! It also keeps the values the agents' journals mask (rule 10): the held
//! value and every value a live agent was spawned with. A value leaves that
//! set only when no live agent holds it.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use zeroize::Zeroizing;

/// The longest token held.
const MAX_TOKEN_BYTES: usize = 16 * 1024;

/// See the module docs.
#[derive(Default)]
pub(super) struct ForgeToken {
    held: Mutex<Option<Zeroizing<String>>>,
    /// Values live agents were spawned with, each with its count of agents.
    spawned: Mutex<Vec<(Zeroizing<String>, usize)>>,
    /// Bumped on every change of the masked values.
    version: AtomicU64,
}

impl ForgeToken {
    /// Hold `token`, or drop the held one with `None`.
    pub(super) fn set(&self, token: Option<Zeroizing<String>>) {
        *lock(&self.held) = token;
        self.version.fetch_add(1, Ordering::SeqCst);
    }

    pub(super) fn clear(&self) {
        self.set(None);
    }

    /// A copy of the held token, wiped when dropped.
    pub(super) fn get(&self) -> Option<Zeroizing<String>> {
        lock(&self.held).clone()
    }

    /// The held token for an agent's spawn, counted as held by that agent
    /// until the returned lease is dropped.
    pub(super) fn spawn(self: &Arc<Self>) -> Option<(Zeroizing<String>, SpawnLease)> {
        let token = self.get()?;
        {
            let mut spawned = lock(&self.spawned);
            match spawned.iter_mut().find(|(held, _)| *held == token) {
                Some((_, count)) => *count += 1,
                None => spawned.push((token.clone(), 1)),
            }
        }
        self.version.fetch_add(1, Ordering::SeqCst);
        let lease = SpawnLease {
            owner: Arc::downgrade(self),
            token: token.clone(),
        };
        Some((token, lease))
    }

    /// Changes whenever the masked values do.
    pub(super) fn version(&self) -> u64 {
        self.version.load(Ordering::SeqCst)
    }

    /// The values to mask now: the held one and every live agent's.
    pub(super) fn masked(&self) -> Vec<Zeroizing<String>> {
        let mut values: Vec<Zeroizing<String>> = lock(&self.spawned)
            .iter()
            .map(|(token, _)| token.clone())
            .collect();
        if let Some(held) = self.get() {
            if !values.contains(&held) {
                values.push(held);
            }
        }
        values
    }

    /// Whether `token` may be held: not empty, at most 16 KiB, and no
    /// whitespace or control character (node-link.md § Forge token on the
    /// machine, rule 6).
    pub(super) fn usable(token: &str) -> bool {
        !token.is_empty()
            && token.len() <= MAX_TOKEN_BYTES
            && !token.chars().any(|c| c.is_whitespace() || c.is_control())
    }
}

impl std::fmt::Debug for ForgeToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ForgeToken")
            .field("held", &lock(&self.held).is_some())
            .field("spawned", &lock(&self.spawned).len())
            .finish_non_exhaustive()
    }
}

/// One agent's hold on the value it was spawned with, released when its
/// process is replaced or the agent is gone.
pub(super) struct SpawnLease {
    owner: std::sync::Weak<ForgeToken>,
    token: Zeroizing<String>,
}

impl Drop for SpawnLease {
    fn drop(&mut self) {
        let Some(owner) = self.owner.upgrade() else {
            return;
        };
        {
            let mut spawned = lock(&owner.spawned);
            if let Some(at) = spawned.iter().position(|(held, _)| *held == self.token) {
                spawned[at].1 -= 1;
                if spawned[at].1 == 0 {
                    spawned.remove(at);
                }
            }
        }
        owner.version.fetch_add(1, Ordering::SeqCst);
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
