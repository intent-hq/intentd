//! Local git work that blocks the automatic lifecycle sweeps (PROTOCOL §5.1
//! "Automatic archive" and "Automatic purge"), read from one
//! `workspace.localChanges` evaluation.
//!
//! Both sweeps share the evaluation and the per-root walk, and both look
//! only at the workspace's own folder: the primary worktree and secondary
//! roots inside it. A secondary root outside the folder (e.g. the user's own
//! checkout, possibly registered by many workspaces) is not this workspace's
//! work and never blocks either sweep. They differ in which work counts:
//!
//! - [`LocalWorkRule::Archive`] — the workspace is still in progress while
//!   any of those roots has uncommitted changes or commits on no
//!   remote-tracking ref. A repository with no remote refs at all has
//!   nothing to push, so only its uncommitted changes count. Unpushed
//!   commits also stop counting once a merged PR the daemon already knows
//!   about covers the root's `HEAD` ([`merged_pr_heads`]): after a squash
//!   merge whose branch was deleted and pruned, every original commit is on
//!   no remote ref, yet the work is done. Uncommitted changes always count.
//! - [`LocalWorkRule::Purge`] — only work the purge would destroy counts:
//!   uncommitted changes in any of those roots, and unpushed commits in a
//!   nested secondary root (the primary branch is kept).
//!
//! Under either rule a relevant root that could not be evaluated, or a row
//! missing a field the rule needs, blocks, so the sweep retries on its next
//! run instead of guessing.

use std::path::Path;

use intent_core::{PullRequestInfo, PullRequestStatus, Workspace};

use crate::Services;

/// Which automatic sweep is asking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LocalWorkRule {
    /// Idle auto-archive: any uncommitted or unpushed work blocks.
    Archive,
    /// Automatic purge: only work that lives solely in the folder blocks.
    Purge,
}

/// The blocker reported for unpushed commits under [`LocalWorkRule::Archive`]
/// — the one verdict a merged PR can lift.
const UNPUSHED_COMMITS: &str = "unpushed commits";

/// Why the sweep governed by `rule` must leave `folder`'s workspace alone,
/// given its `workspace.localChanges` evaluation. `None` = no blocking work.
///
/// `merged_covers(path)` answers, for a root under [`LocalWorkRule::Archive`]
/// whose only blocking work is unpushed commits, whether a merged PR covers
/// its `HEAD` (see [`intent_git::head_reached_by`]); `true` lifts the block.
/// It is never asked under [`LocalWorkRule::Purge`].
pub(crate) fn local_work_blocker(
    local_changes: &serde_json::Value,
    folder: Option<&Path>,
    rule: LocalWorkRule,
    merged_covers: &mut dyn FnMut(&str) -> bool,
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
        if !inside {
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
                match root
                    .get("hasRemoteRefs")
                    .and_then(serde_json::Value::as_bool)
                {
                    Some(false) => {}
                    Some(true) => {
                        let covered = root
                            .get("path")
                            .and_then(serde_json::Value::as_str)
                            .is_some_and(&mut *merged_covers);
                        if !covered {
                            return Some(UNPUSHED_COMMITS);
                        }
                    }
                    None => return Some("local changes could not be evaluated"),
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
    /// evaluation blocks (retried next sweep). Only when the first verdict is
    /// unpushed commits under [`LocalWorkRule::Archive`] does it read the
    /// workspace's merged PRs ([`Services::merged_pr_heads`]) and re-evaluate
    /// with the per-root coverage check, which runs git on the blocking pool.
    pub(crate) async fn blocked_by_local_work(
        &self,
        ws: &Workspace,
        rule: LocalWorkRule,
    ) -> Option<&'static str> {
        let folder = crate::git_ops::worktree_path(ws);
        let changes = match self.workspace_local_changes_op(ws.id.clone()).await {
            Ok(changes) => changes,
            Err(e) => {
                tracing::warn!(workspace = %ws.id, ?rule, error = %e, "local changes lookup failed");
                return Some("local changes lookup failed");
            }
        };
        let verdict = local_work_blocker(&changes, folder.as_deref(), rule, &mut |_| false);
        if rule != LocalWorkRule::Archive || verdict != Some(UNPUSHED_COMMITS) {
            return verdict;
        }
        let heads = self.merged_pr_heads(ws).await;
        if heads.is_empty() {
            return verdict;
        }
        let workspace = ws.id.clone();
        tokio::task::spawn_blocking(move || {
            local_work_blocker(&changes, folder.as_deref(), rule, &mut |path| {
                intent_git::head_reached_by(Path::new(path), &heads).unwrap_or_else(|e| {
                    tracing::warn!(%workspace, path, error = %e, "merged-PR coverage check failed");
                    false
                })
            })
        })
        .await
        .unwrap_or(verdict)
    }

    /// Head commits of the workspace's merged PRs, from daemon-known data
    /// only (no forge call): the stored PR records with status `merged` of
    /// the workspace and of its secondary git roots, and every PR monitor
    /// whose persisted snapshot shows the PR merged. Entries without a head
    /// SHA are dropped; a failed store read contributes nothing, so
    /// unpushed commits keep blocking.
    pub(crate) async fn merged_pr_heads(&self, ws: &Workspace) -> Vec<String> {
        let mut records: Vec<PullRequestInfo> = ws.pull_requests.clone().unwrap_or_default();
        match self.store.list_workspace_git_roots(&ws.id).await {
            Ok(roots) => records.extend(
                roots
                    .into_iter()
                    .flat_map(|r| r.pull_requests.unwrap_or_default()),
            ),
            Err(e) => {
                tracing::warn!(workspace = %ws.id, error = %e, "merged PR git-root lookup failed");
            }
        }
        let mut heads: Vec<String> = records
            .into_iter()
            .filter(|pr| pr.status == PullRequestStatus::Merged)
            .filter_map(|pr| pr.head_sha)
            .collect();
        match self.store.list_pr_monitors_by_workspace(&ws.id).await {
            Ok(monitors) => heads.extend(monitors.iter().filter_map(|m| {
                let snapshot: crate::pr_monitor::PrMonitorSnapshot =
                    serde_json::from_str(m.last_snapshot.as_deref()?).ok()?;
                (snapshot.requirements.state == "merged")
                    .then_some(snapshot.head_sha)
                    .flatten()
            })),
            Err(e) => {
                tracing::warn!(workspace = %ws.id, error = %e, "merged PR monitor lookup failed");
            }
        }
        heads.retain(|h| !h.trim().is_empty());
        heads.sort();
        heads.dedup();
        heads
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
        local_work_blocker(changes, folder, LocalWorkRule::Purge, &mut |_| {
            panic!("purge never asks about merged PRs")
        })
    }

    fn archive_blocker(changes: &serde_json::Value, folder: Option<&Path>) -> Option<&'static str> {
        local_work_blocker(changes, folder, LocalWorkRule::Archive, &mut |_| false)
    }

    /// Archive with every root in `merged` covered by a merged PR; records
    /// which roots were asked.
    fn archive_blocker_merged(
        changes: &serde_json::Value,
        folder: Option<&Path>,
        merged: &[&str],
        asked: &mut Vec<String>,
    ) -> Option<&'static str> {
        local_work_blocker(changes, folder, LocalWorkRule::Archive, &mut |path| {
            asked.push(path.to_string());
            merged.contains(&path)
        })
    }

    /// A merged PR covering a root lifts only that root's unpushed-commits
    /// block: uncommitted changes, an unreadable root, a missing
    /// `hasRemoteRefs` and another root's uncovered commits still block, and
    /// a root without unpushed commits is never asked about.
    #[test]
    fn archive_merged_pr_lifts_only_the_unpushed_block_it_covers() {
        let folder = Some(Path::new("/ws/one"));
        let mut asked = Vec::new();

        let pruned = json!({ "roots": [row("primary", "/ws/one", 0, 5)] });
        assert_eq!(
            archive_blocker_merged(&pruned, folder, &["/ws/one"], &mut asked),
            None,
            "squash-merged, branch pruned"
        );
        assert_eq!(
            archive_blocker_merged(&pruned, folder, &[], &mut asked),
            Some("unpushed commits"),
            "never-pushed branch, no merged PR"
        );

        let dirty = json!({ "roots": [row("primary", "/ws/one", 1, 5)] });
        assert_eq!(
            archive_blocker_merged(&dirty, folder, &["/ws/one"], &mut asked),
            Some("uncommitted changes")
        );

        let both = json!({ "roots": [row("primary", "/ws/one", 0, 2), row("secondary", "/ws/one/packages/sub", 0, 3)] });
        assert_eq!(
            archive_blocker_merged(
                &both,
                folder,
                &["/ws/one", "/ws/one/packages/sub"],
                &mut asked
            ),
            None,
            "a submodule PR covers the nested root"
        );
        assert_eq!(
            archive_blocker_merged(&both, folder, &["/ws/one"], &mut asked),
            Some("unpushed commits")
        );

        let mut no_flag = row("primary", "/ws/one", 0, 1);
        no_flag.as_object_mut().unwrap().remove("hasRemoteRefs");
        assert_eq!(
            archive_blocker_merged(
                &json!({ "roots": [no_flag] }),
                folder,
                &["/ws/one"],
                &mut asked
            ),
            Some("local changes could not be evaluated")
        );

        asked.clear();
        let clean = json!({ "roots": [row("primary", "/ws/one", 0, 0)] });
        assert_eq!(
            archive_blocker_merged(&clean, folder, &["/ws/one"], &mut asked),
            None
        );
        assert!(
            asked.is_empty(),
            "nothing unpushed, nothing to ask: {asked:?}"
        );
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
    /// the folder's roots — the primary branch included — and by an
    /// unreadable root or a row without `hasRemoteRefs`. A repository with
    /// no remote refs has nothing to push, so only its uncommitted changes
    /// count. Repositories outside the folder never block it.
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

        let nested = json!({ "roots": [row("primary", "/ws/one", 0, 0), row("secondary", "/ws/one/packages/sub", 0, 1)] });
        assert_eq!(archive_blocker(&nested, folder), Some("unpushed commits"));
        let nested_dirty = json!({ "roots": [row("secondary", "/ws/one/packages/sub", 3, 0)] });
        assert_eq!(
            archive_blocker(&nested_dirty, folder),
            Some("uncommitted changes")
        );

        let mut outside_errored = row("secondary", "/elsewhere/clone", 0, 0);
        outside_errored["error"] = json!("not a git repository");
        let outside = json!({ "roots": [row("primary", "/ws/one", 0, 0), row("secondary", "/elsewhere/clone", 3, 4), outside_errored] });
        assert_eq!(
            archive_blocker(&outside, folder),
            None,
            "a shared or moved repository outside the folder is not this workspace's work"
        );

        let mut no_flag = row("primary", "/ws/one", 0, 1);
        no_flag.as_object_mut().unwrap().remove("hasRemoteRefs");
        assert_eq!(
            archive_blocker(&json!({ "roots": [no_flag] }), folder),
            Some("local changes could not be evaluated"),
            "a row missing hasRemoteRefs fails safe"
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

        let mut errored = row("secondary", "/ws/one/packages/sub", 0, 0);
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
