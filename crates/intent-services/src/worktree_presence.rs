//! In-memory worktree presence tracking (intent-hq/intent#7183).
//!
//! A workspace whose checkout folder was deleted (or whose volume is
//! unmounted) outside Intent keeps its `Active` row. Without a gate, every
//! `agent:idle` re-attempts auto-commit and logs a warning, so hook and
//! PR-monitor wakes turn into a steady stream of identical failures.
//!
//! [`WorktreePresence`] records which workspaces were last observed with a
//! missing checkout and logs only the transitions: one `warn!` when a
//! checkout goes missing and one `info!` when it reappears. Repeat
//! observations log at `debug!`. Callers ([`crate::Services`] auto-commit, the
//! tracked-changes sweep, and the watcher registry's presence probe) gate
//! their worktree work on [`WorktreePresence::observe`].
//!
//! The map is derived and never persisted. A daemon restart starts with
//! every workspace presumed present, and a remounted volume recovers on the
//! next observation.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use intent_core::WorkspaceId;

/// One workspace last observed with a missing checkout.
#[derive(Debug, Clone)]
struct Missing {
    path: PathBuf,
    since: SystemTime,
}

/// Shared, cloneable map of workspaces whose checkout is currently missing.
/// Clones share one map, like the other in-memory registries on `Services`.
#[derive(Debug, Clone, Default)]
pub struct WorktreePresence {
    missing: Arc<Mutex<HashMap<WorkspaceId, Missing>>>,
}

/// Whether `path` is an existing directory. A cheap `stat`; a symlink to a
/// directory counts as present (checkouts may live behind one), a dangling
/// symlink or a regular file does not.
#[must_use]
pub fn checkout_dir_exists(path: &Path) -> bool {
    path.is_dir()
}

impl WorktreePresence {
    /// Stat `path` for `workspace_id`, record the result, and return whether
    /// the checkout is present. Logs one `warn!` on present → missing, one
    /// `info!` on missing → present, and `debug!` otherwise.
    pub fn observe(&self, workspace_id: &WorkspaceId, path: &Path) -> bool {
        let present = checkout_dir_exists(path);
        self.record(workspace_id, path, present);
        present
    }

    /// Record an already-made observation (see [`Self::observe`]). Split out
    /// so tests can drive transitions without touching the filesystem.
    pub(crate) fn record(&self, workspace_id: &WorkspaceId, path: &Path, present: bool) {
        let mut missing = self
            .missing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if present {
            if let Some(prev) = missing.remove(workspace_id) {
                let missing_secs = prev
                    .since
                    .elapsed()
                    .map(|d| d.as_secs())
                    .unwrap_or_default();
                tracing::info!(
                    workspace = %workspace_id,
                    path = %path.display(),
                    missing_secs,
                    "workspace checkout is back; auto-commit and git refresh resumed"
                );
            }
        } else if let Some(prev) = missing.get(workspace_id) {
            tracing::debug!(
                workspace = %workspace_id,
                path = %prev.path.display(),
                "workspace checkout still missing; skipping worktree work"
            );
        } else {
            tracing::warn!(
                workspace = %workspace_id,
                path = %path.display(),
                "workspace checkout missing; auto-commit and git refresh paused until it reappears"
            );
            missing.insert(
                workspace_id.clone(),
                Missing {
                    path: path.to_path_buf(),
                    since: SystemTime::now(),
                },
            );
        }
    }

    /// Whether `workspace_id` was last observed with a missing checkout.
    #[must_use]
    pub fn is_missing(&self, workspace_id: &WorkspaceId) -> bool {
        self.missing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains_key(workspace_id)
    }

    /// Drop any record for `workspace_id` (deleted, archived, or closed
    /// workspaces) without logging a transition.
    pub fn forget(&self, workspace_id: &WorkspaceId) {
        self.missing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(workspace_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transitions_are_recorded_and_forget_clears() {
        let presence = WorktreePresence::default();
        let id = WorkspaceId::from("ws-a");
        let path = Path::new("/nonexistent/intent-7183");
        assert!(!presence.is_missing(&id));
        presence.record(&id, path, false);
        assert!(presence.is_missing(&id));
        presence.record(&id, path, false);
        assert!(presence.is_missing(&id));
        presence.record(&id, path, true);
        assert!(!presence.is_missing(&id));
        presence.record(&id, path, false);
        presence.forget(&id);
        assert!(!presence.is_missing(&id));
    }

    #[test]
    fn observe_stats_the_path() {
        let presence = WorktreePresence::default();
        let id = WorkspaceId::from("ws-b");
        let dir = std::env::temp_dir().join(format!("intent-7183-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        assert!(!presence.observe(&id, &dir));
        assert!(presence.is_missing(&id));
        std::fs::create_dir_all(&dir).expect("mkdir");
        assert!(presence.observe(&id, &dir));
        assert!(!presence.is_missing(&id));
        let file = dir.join("plain-file");
        std::fs::write(&file, "x").expect("write");
        assert!(
            !presence.observe(&id, &file),
            "a regular file is not a checkout"
        );
        std::fs::remove_dir_all(&dir).expect("cleanup");
    }
}
