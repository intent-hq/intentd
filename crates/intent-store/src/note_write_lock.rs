//! Per-note content-write lock (intent-hq/intent#7184).
//!
//! Every [`Store`] method that rewrites a note's text — or bumps its `rev`
//! with a metadata-only write — takes a [`NoteWriteLock`] for that note, and the only way to get one is
//! [`Store::lock_note_content`], so a writer that never took the note's lock
//! does not compile. A writer holds it from the read its new text derives
//! from through the gated persist, so N parallel writers to one note apply one
//! after another against the latest text instead of racing a bounded
//! `Conflict` retry loop. Locks are per `(workspace_id, note_id)`: writers to
//! different notes never wait on each other, and an entry is dropped once
//! nobody holds or awaits it.
//!
//! Hold one lock for one read→persist window, and never while taking another
//! note's lock (or the same note's again): locks never nest, so they cannot
//! deadlock.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use intent_core::{Error, Note, NoteId, Result, WorkspaceId};

use crate::Store;

type NoteKey = (WorkspaceId, NoteId);

/// One note's lock plus the callers holding or awaiting it.
struct Lane {
    mutex: Arc<tokio::sync::Mutex<()>>,
    users: usize,
}

/// The registry behind [`Store::lock_note_content`], shared by every clone
/// of one [`Store`].
#[derive(Default)]
pub(crate) struct NoteWriteLanes {
    lanes: Mutex<HashMap<NoteKey, Lane>>,
}

/// Proof that the caller holds one note's content-write lock; the lock is
/// released on drop. Obtained only from [`Store::lock_note_content`].
#[must_use = "the note's write lock is released as soon as this is dropped"]
pub struct NoteWriteLock {
    // Field order matters: the mutex guard is released before the slot
    // gives up its registry entry.
    _guard: tokio::sync::OwnedMutexGuard<()>,
    slot: LaneSlot,
}

/// One caller's registration on a note's lane, taken before it starts
/// waiting so a lock future dropped mid-wait still deregisters.
struct LaneSlot {
    registry: Arc<NoteWriteLanes>,
    key: NoteKey,
}

impl Drop for LaneSlot {
    fn drop(&mut self) {
        let mut lanes = self
            .registry
            .lanes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(lane) = lanes.get_mut(&self.key) {
            lane.users -= 1;
            if lane.users == 0 {
                lanes.remove(&self.key);
            }
        }
    }
}

impl NoteWriteLanes {
    async fn lock(self: &Arc<Self>, key: NoteKey) -> NoteWriteLock {
        let mutex = {
            let mut lanes = self
                .lanes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let lane = lanes.entry(key.clone()).or_insert_with(|| Lane {
                mutex: Arc::default(),
                users: 0,
            });
            lane.users += 1;
            Arc::clone(&lane.mutex)
        };
        let slot = LaneSlot {
            registry: Arc::clone(self),
            key,
        };
        let guard = mutex.lock_owned().await;
        NoteWriteLock {
            _guard: guard,
            slot,
        }
    }

    #[cfg(any(test, feature = "test-support"))]
    fn users(&self, key: &NoteKey) -> usize {
        self.lanes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(key)
            .map_or(0, |lane| lane.users)
    }

    #[cfg(any(test, feature = "test-support"))]
    fn is_empty(&self) -> bool {
        self.lanes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
    }
}

impl NoteWriteLock {
    /// Workspace of the note this lock covers.
    #[must_use]
    pub fn workspace_id(&self) -> &WorkspaceId {
        &self.slot.key.0
    }

    /// The note this lock covers.
    #[must_use]
    pub fn note_id(&self) -> &NoteId {
        &self.slot.key.1
    }

    /// Refuse a write of `note` unless this lock was taken from `store`'s
    /// registry for exactly that note.
    pub(crate) fn ensure_covers(&self, store: &Store, note: &Note) -> Result<()> {
        if !Arc::ptr_eq(&self.slot.registry, &store.note_write_lanes) {
            return Err(Error::Internal(format!(
                "note write lock for {}/{} was taken from another store",
                self.workspace_id().0,
                self.note_id().0
            )));
        }
        if self.workspace_id() != &note.workspace_id || self.note_id() != &note.id {
            return Err(Error::Internal(format!(
                "note write lock for {}/{} cannot write note {}/{}",
                self.workspace_id().0,
                self.note_id().0,
                note.workspace_id.0,
                note.id.0
            )));
        }
        Ok(())
    }
}

impl Store {
    /// Take `note_id`'s content-write lock, waiting for the current holder.
    /// Every method that rewrites a note's text or metadata requires it. Hold it from
    /// the read a write derives from through its persist, and drop it before
    /// any follow-up that may write a note (locks never nest).
    pub async fn lock_note_content(
        &self,
        workspace_id: &WorkspaceId,
        note_id: &NoteId,
    ) -> NoteWriteLock {
        self.note_write_lanes
            .lock((workspace_id.clone(), note_id.clone()))
            .await
    }

    /// Callers holding or awaiting `note_id`'s content-write lock. A test
    /// seam for observing that a writer is queued.
    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub fn note_content_lock_users(&self, workspace_id: &WorkspaceId, note_id: &NoteId) -> usize {
        self.note_write_lanes
            .users(&(workspace_id.clone(), note_id.clone()))
    }

    /// `true` when no note's content-write lock is held or awaited. A test
    /// seam for checking that locks are released.
    #[cfg(any(test, feature = "test-support"))]
    #[must_use]
    pub fn note_content_locks_idle(&self) -> bool {
        self.note_write_lanes.is_empty()
    }
}
