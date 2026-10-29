//! Local git work that blocks the automatic lifecycle sweeps (PROTOCOL §5.1
//! "Automatic archive" and "Automatic purge"), read from one
//! `workspace.localChanges` evaluation.
//!
//! Both sweeps share the evaluation and the per-root walk; they differ in
//! which work counts:
//!
//! - [`LocalWorkRule::Archive`] — the workspace is still in progress while
//!   any of its roots (primary or secondary, wherever it lives) has
//!   uncommitted changes or commits on no remote-tracking ref. A repository
//!   with no remote refs at all has nothing to push, so only its
//!   uncommitted changes count.
//! - [`LocalWorkRule::Purge`] — only work the purge would destroy counts:
//!   uncommitted changes in any root inside the folder, and unpushed commits
//!   in a secondary root inside it (the primary branch is kept). Roots
//!   outside the folder survive the purge and never block it.
//!
//! Under either rule a relevant root that could not be evaluated blocks, so
//! the sweep retries on its next run instead of guessing.

use std::path::Path;

use intent_core::Workspace;

use crate::Services;

/// Which automatic sweep is asking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LocalWorkRule {
    /// Idle auto-archive: any uncommitted or unpushed work blocks.
    Archive,
    /// Automatic purge: only work that lives solely in the folder blocks.
    Purge,
}

/// Why the sweep governed by `rule` must leave `folder`'s workspace alone,
/// given its `workspace.localChanges` evaluation. `None` = no blocking work.
pub(crate) fn local_work_blocker(
    local_changes: &serde_json::Value,
    folder: Option<&Path>,
    rule: LocalWorkRule,
) -> Option<&'static str> {
    let Some(roots) = local_changes
        .get("roots")
        .and_then(serde_json::Value::as_array)
    else {
        return Some("local changes unreadable");
    };
    for root in roots {
        let primary = root.get("kind").and_then(serde_json::Value::as_str) == Some("primary");
        let inside = primary
            || root
                .get("path")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|path| {
                    folder.is_some_and(|folder| Path::new(path).starts_with(folder))
                });
        if rule == LocalWorkRule::Purge && !inside {
            continue;
        }
        if root.get("error").is_some_and(|e| !e.is_null()) {
            return Some("local changes could not be evaluated");
        }
        let count = |key: &str| {
            root.get(key)
                .and_then(serde_json::Value::as_u64)
                .unwrap_or(0)
        };
        if count("uncommittedCount") > 0 {
            return Some("uncommitted changes");
        }
        if count("unpushedCount") == 0 {
            continue;
        }
        match rule {
            LocalWorkRule::Archive => {
                let has_remote = root
                    .get("hasRemoteRefs")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false);
                if has_remote {
                    return Some("unpushed commits");
                }
            }
            LocalWorkRule::Purge => {
                if !primary {
                    return Some("unpushed commits in a nested repository");
                }
            }
        }
    }
    None
}

impl Services {
    /// The [`local_work_blocker`] verdict for `ws` under `rule`; a failed
    /// evaluation blocks (retried next sweep).
    pub(crate) async fn blocked_by_local_work(
        &self,
        ws: &Workspace,
        rule: LocalWorkRule,
    ) -> Option<&'static str> {
        let folder = crate::git_ops::worktree_path(ws);
        match self.workspace_local_changes_op(ws.id.clone()).await {
            Ok(changes) => local_work_blocker(&changes, folder.as_deref(), rule),
            Err(e) => {
                tracing::warn!(workspace = %ws.id, ?rule, error = %e, "local changes lookup failed");
                Some("local changes lookup failed")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{local_work_blocker, LocalWorkRule};
    use serde_json::json;
    use std::path::Path;

    fn row(kind: &str, path: &str, uncommitted: u64, unpushed: u64) -> serde_json::Value {
        json!({ "kind": kind, "path": path, "uncommittedCount": uncommitted, "unpushedCount": unpushed, "hasRemoteRefs": true })
    }

    fn purge_blocker(changes: &serde_json::Value, folder: Option<&Path>) -> Option<&'static str> {
        local_work_blocker(changes, folder, LocalWorkRule::Purge)
    }

    fn archive_blocker(changes: &serde_json::Value, folder: Option<&Path>) -> Option<&'static str> {
        local_work_blocker(changes, folder, LocalWorkRule::Archive)
    }

    /// Purge is blocked by uncommitted work anywhere in the folder, by
    /// unpushed commits in a nested (secondary) repository inside it, and by
    /// a root that could not be read; unpushed commits on the primary branch
    /// (kept by the purge) and anything in a repository outside the folder
    /// never block it.
    #[test]
    fn purge_blocker_protects_work_that_lives_only_in_the_folder() {
        let folder = Some(Path::new("/ws/one"));
        let clean = json!({ "roots": [row("primary", "/ws/one", 0, 3)] });
        assert_eq!(
            purge_blocker(&clean, folder),
            None,
            "primary unpushed commits survive on the branch"
        );
        assert_eq!(purge_blocker(&json!({ "roots": [] }), folder), None);

        let dirty = json!({ "roots": [row("primary", "/ws/one", 2, 0)] });
        assert_eq!(purge_blocker(&dirty, folder), Some("uncommitted changes"));

        let nested_unpushed = json!({ "roots": [row("primary", "/ws/one", 0, 0), row("secondary", "/ws/one/packages/sub", 0, 1)] });
        assert_eq!(
            purge_blocker(&nested_unpushed, folder),
            Some("unpushed commits in a nested repository")
        );
        let nested_dirty = json!({ "roots": [row("secondary", "/ws/one/packages/sub", 1, 0)] });
        assert_eq!(
            purge_blocker(&nested_dirty, folder),
            Some("uncommitted changes")
        );

        let outside = json!({ "roots": [row("primary", "/ws/one", 0, 0), row("secondary", "/elsewhere/clone", 4, 4)] });
        assert_eq!(
            purge_blocker(&outside, folder),
            None,
            "a repository outside the folder survives the purge"
        );

        let mut errored = row("primary", "/ws/one", 0, 0);
        errored["error"] = json!("not a git repository");
        assert_eq!(
            purge_blocker(&json!({ "roots": [errored] }), folder),
            Some("local changes could not be evaluated")
        );
        assert_eq!(
            purge_blocker(&json!({}), folder),
            Some("local changes unreadable")
        );
    }

    /// Auto-archive is blocked by uncommitted changes or unpushed commits in
    /// any root — the primary branch and repositories outside the folder
    /// included — and by an unreadable root. A repository with no remote
    /// refs has nothing to push, so only its uncommitted changes count.
    #[test]
    fn archive_blocker_keeps_any_unsaved_or_unpushed_work_active() {
        let folder = Some(Path::new("/ws/one"));
        assert_eq!(archive_blocker(&json!({ "roots": [] }), folder), None);
        let clean = json!({ "roots": [row("primary", "/ws/one", 0, 0)] });
        assert_eq!(archive_blocker(&clean, folder), None);

        let dirty = json!({ "roots": [row("primary", "/ws/one", 1, 0)] });
        assert_eq!(archive_blocker(&dirty, folder), Some("uncommitted changes"));

        let primary_unpushed = json!({ "roots": [row("primary", "/ws/one", 0, 2)] });
        assert_eq!(
            archive_blocker(&primary_unpushed, folder),
            Some("unpushed commits"),
            "unlike purge, the primary branch's unpushed commits block"
        );

        let outside = json!({ "roots": [row("primary", "/ws/one", 0, 0), row("secondary", "/elsewhere/clone", 0, 1)] });
        assert_eq!(
            archive_blocker(&outside, folder),
            Some("unpushed commits"),
            "a repository outside the folder still holds the workspace's work"
        );
        let outside_dirty = json!({ "roots": [row("secondary", "/elsewhere/clone", 3, 0)] });
        assert_eq!(
            archive_blocker(&outside_dirty, folder),
            Some("uncommitted changes")
        );

        let mut no_remote = row("primary", "/ws/one", 0, 1000);
        no_remote["hasRemoteRefs"] = json!(false);
        assert_eq!(
            archive_blocker(&json!({ "roots": [no_remote.clone()] }), folder),
            None,
            "nothing to push without a remote"
        );
        no_remote["uncommittedCount"] = json!(1);
        assert_eq!(
            archive_blocker(&json!({ "roots": [no_remote] }), folder),
            Some("uncommitted changes")
        );

        let mut errored = row("secondary", "/elsewhere/clone", 0, 0);
        errored["error"] = json!("not a git repository");
        assert_eq!(
            archive_blocker(&json!({ "roots": [errored] }), folder),
            Some("local changes could not be evaluated")
        );
        assert_eq!(
            archive_blocker(&json!({}), folder),
            Some("local changes unreadable")
        );
    }
}
