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
//! A checkout that sits exactly at a mountpoint stays behind as an empty
//! directory when its volume unmounts, so "is a directory" alone would report
//! it present forever. A workspace known to be a Git checkout — its row
//! carries a `checkoutMode`, or this process has seen a `.git` entry (dir or
//! gitfile) at its root — is therefore present only while that `.git` entry
//! exists. Plain non-Git workspaces only need the directory.
//!
//! The map is derived and never persisted. A daemon restart starts with
//! every workspace presumed present, and a remounted volume recovers on the
//! next observation.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::SystemTime;

use std::collections::HashSet;

use intent_core::{Workspace, WorkspaceId};

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
    /// Workspaces known to be Git checkouts (see the module docs).
    git: Arc<Mutex<HashSet<WorkspaceId>>>,
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
    #[must_use]
    pub fn observe(&self, workspace_id: &WorkspaceId, path: &Path) -> bool {
        let present = self.available(workspace_id, path);
        self.record(workspace_id, path, present);
        present
    }

    /// [`Self::observe`] for a workspace row: a row with a `checkoutMode` is
    /// a daemon-provisioned Git checkout, so its `.git` entry is required.
    #[must_use]
    pub fn observe_workspace(&self, ws: &Workspace, path: &Path) -> bool {
        if ws.checkout_mode.is_some() {
            self.mark_git(&ws.id);
        }
        self.observe(&ws.id, path)
    }

    /// Remember that `workspace_id` is a Git checkout.
    pub fn mark_git(&self, workspace_id: &WorkspaceId) {
        self.git
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(workspace_id.clone());
    }

    /// Learn from a present root without recording a transition: a root
    /// with a `.git` entry marks the workspace as a Git checkout.
    pub fn learn(&self, workspace_id: &WorkspaceId, path: &Path) {
        if path.join(".git").exists() {
            self.mark_git(workspace_id);
        }
    }

    /// Whether the checkout at `path` is usable: the directory exists and,
    /// for a known Git checkout, so does its `.git` entry.
    fn available(&self, workspace_id: &WorkspaceId, path: &Path) -> bool {
        if !checkout_dir_exists(path) {
            return false;
        }
        if path.join(".git").exists() {
            self.mark_git(workspace_id);
            return true;
        }
        !self
            .git
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .contains(workspace_id)
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
        self.git
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

    /// A Git checkout at a mountpoint leaves an empty directory behind when
    /// the volume unmounts: once the workspace is known to be a Git checkout,
    /// that empty directory counts as missing, and the restored checkout
    /// recovers. A plain non-Git folder only needs the directory.
    #[test]
    fn known_git_checkout_without_git_entry_is_missing() {
        let presence = WorktreePresence::default();
        let base = std::env::temp_dir().join(format!("intent-7183-mnt-{}", uuid::Uuid::new_v4()));
        let mount = base.join("checkout");
        let away = base.join("unmounted");
        std::fs::create_dir_all(mount.join(".git")).expect("mkdir .git");
        let id = WorkspaceId::from("ws-mnt");
        assert!(presence.observe(&id, &mount));

        std::fs::rename(&mount, &away).expect("unmount");
        std::fs::create_dir_all(&mount).expect("empty mountpoint");
        assert!(
            !presence.observe(&id, &mount),
            "empty mountpoint must be missing"
        );
        assert!(presence.is_missing(&id));

        std::fs::remove_dir(&mount).expect("rm mountpoint");
        std::fs::rename(&away, &mount).expect("remount");
        assert!(presence.observe(&id, &mount));
        assert!(!presence.is_missing(&id));

        let plain = base.join("plain");
        std::fs::create_dir_all(&plain).expect("mkdir plain");
        let plain_id = WorkspaceId::from("ws-plain");
        assert!(
            presence.observe(&plain_id, &plain),
            "non-Git folder is present"
        );

        let mut row = intent_core::chief_workspace();
        row.id = WorkspaceId::from("ws-row");
        row.checkout_mode = Some(intent_core::CheckoutMode::Worktree);
        assert!(
            !presence.observe_workspace(&row, &plain),
            "a provisioned checkout without .git is missing even on first sight"
        );
        std::fs::remove_dir_all(&base).expect("cleanup");
    }
}
