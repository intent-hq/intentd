//! In-memory delete grace-window registry (PROTOCOL §5.1).
//!
//! A `*.delete` with `undoDelayMs > 0` registers a pending deletion here
//! instead of committing immediately: the entry pairs the ISO `deleteAt`
//! deadline with the timer task that commits the delete on expiry. Entries
//! are **never persisted** — a daemon restart drops every pending deletion
//! and the entity survives (the spec's restart semantics).
//!
//! Race safety is generation-based: each schedule mints a generation token
//! the timer must present to claim its entry at fire time. A cancel (or an
//! immediate delete while pending) removes the entry and aborts the timer;
//! a timer that already claimed its entry can no longer be cancelled — the
//! cancel observes "nothing pending" and reports `false`, the non-error
//! race-safe outcome.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Cap on the caller-supplied `undoDelayMs` (the "sane cap, e.g. 60s" from
/// the wire contract). Values above it are clamped, never rejected.
pub(crate) const MAX_UNDO_DELAY_MS: u64 = 60_000;

/// Clamp a caller-supplied grace delay to [`MAX_UNDO_DELAY_MS`].
pub(crate) fn clamp_undo_delay_ms(ms: u64) -> u64 {
    ms.min(MAX_UNDO_DELAY_MS)
}

struct Pending {
    delete_at: String,
    generation: u64,
    handle: tokio::task::JoinHandle<()>,
}

/// Registry of pending deletions keyed by entity id. Shared across
/// [`crate::Services`] clones so every front door observes one set.
#[derive(Clone, Default)]
pub(crate) struct PendingDeletes {
    inner: Arc<Mutex<HashMap<String, Pending>>>,
    next_generation: Arc<AtomicU64>,
}

impl PendingDeletes {
    /// ISO deadline of the pending deletion for `key`, when one is scheduled.
    /// Backs both the idempotent re-schedule and the `pendingDeleteAt` row
    /// projection.
    pub(crate) fn deadline(&self, key: &str) -> Option<String> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(key)
            .map(|p| p.delete_at.clone())
    }

    /// Register a pending deletion unless one is already pending for `key`.
    /// The idempotent re-schedule check runs under the registry lock, so
    /// concurrent schedules for the same key cannot each arm a timer: the
    /// loser observes the winner's entry and gets its deadline back as
    /// `Some(existing)` without `spawn` ever running. `None` means the
    /// entry was newly armed with the supplied `delete_at`. `spawn`
    /// receives the minted generation token and must return the timer task
    /// that will present it to [`PendingDeletes::claim`] at fire time; it
    /// runs under the registry lock, so a (pathologically fast) timer
    /// cannot observe the map before its own entry is inserted.
    pub(crate) fn schedule(
        &self,
        key: String,
        delete_at: String,
        spawn: impl FnOnce(u64) -> tokio::task::JoinHandle<()>,
    ) -> Option<String> {
        let mut map = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(existing) = map.get(&key) {
            return Some(existing.delete_at.clone());
        }
        let generation = self.next_generation.fetch_add(1, Ordering::Relaxed);
        let handle = spawn(generation);
        map.insert(
            key,
            Pending {
                delete_at,
                generation,
                handle,
            },
        );
        None
    }

    /// Timer-side claim at fire time: removes the entry only when it still
    /// belongs to this timer (same generation). `true` means the timer owns
    /// the commit; `false` means the entry was cancelled or superseded and
    /// the timer must do nothing.
    pub(crate) fn claim(&self, key: &str, generation: u64) -> bool {
        let mut map = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match map.get(key) {
            Some(p) if p.generation == generation => {
                map.remove(key);
                true
            }
            _ => false,
        }
    }

    /// Cancel a pending deletion: removes the entry and aborts its timer.
    /// Returns `true` when something was pending, `false` otherwise (already
    /// committed, or never scheduled) — the race-safe non-error outcome.
    pub(crate) fn cancel(&self, key: &str) -> bool {
        let removed = self
            .inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(key);
        match removed {
            Some(p) => {
                p.handle.abort();
                true
            }
            None => false,
        }
    }
}
