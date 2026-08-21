//! Sandbox provisioning and lifecycle for CoW agent isolation (direct-mode and
//! CoW-checkout workspaces).

use std::path::PathBuf;
use std::time::Duration;

use intent_core::{AgentId, CheckoutMode, Error, Result, Workspace, WorkspaceId};
use intent_git::{cow_clone, cow_probe, CowSupport};
use intent_store::{Sandbox, SandboxStatus, Store};

use crate::nested_repos::{is_dirty, stage_all_skipping_nested};
use crate::now_iso;

/// Test hook: artificial delay (milliseconds) at the top of
/// [`provision_sandbox`], standing in for a slow CoW clone of a large
/// checkout. Lets e2e tests prove provisioning runs off the delegate
/// critical path (monorepo#871). NOTE: this seam is compiled into release
/// binaries too (release-mode e2e runs need it); it is inert unless the
/// namespaced env var is set to a positive integer.
pub(crate) const TEST_PROVISION_DELAY_MS_ENV: &str = "INTENTD_TEST_SANDBOX_PROVISION_DELAY_MS";

/// Test hook: force [`provision_sandbox`] to fail with an internal error
/// (after the optional delay above), exercising the fallback-to-shared-mode
/// path on provisioning failure. Inert unless set to `1`.
pub(crate) const TEST_PROVISION_ERROR_ENV: &str = "INTENTD_TEST_SANDBOX_PROVISION_ERROR";

/// Parse the delay override in milliseconds; anything unset, non-numeric, or
/// non-positive disables the hook.
fn test_provision_delay_from(raw: Option<&str>) -> Option<Duration> {
    raw.and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&ms| ms > 0)
        .map(Duration::from_millis)
}

/// Apply the test-only provisioning seams: sleep out the configured delay,
/// then fail if the error hook is armed. No-op when neither env var is set.
async fn apply_test_provision_hooks() -> Result<()> {
    if let Some(delay) =
        test_provision_delay_from(std::env::var(TEST_PROVISION_DELAY_MS_ENV).ok().as_deref())
    {
        tracing::warn!(
            delay_ms = delay.as_millis() as u64,
            "provision_sandbox: artificial delay (test seam)"
        );
        tokio::time::sleep(delay).await;
    }
    if std::env::var(TEST_PROVISION_ERROR_ENV).is_ok_and(|v| v == "1") {
        return Err(Error::Internal(
            "forced provisioning failure (test seam)".to_string(),
        ));
    }
    Ok(())
}

/// Outcome of sandbox provisioning.
#[derive(Debug, Clone)]
pub enum ProvisionOutcome {
    /// CoW is supported; sandbox was created.
    Supported {
        path: PathBuf,
        branch: String,
        base_commit_sha: String,
        snapshot_commit_sha: Option<String>,
    },
    /// CoW is not supported; fallback to shared mode (no bytes copied).
    Unsupported,
}

/// Outcome of a merge-back attempt.
#[derive(Debug, Clone)]
pub enum MergeOutcome {
    /// Clean merge; sandbox commits applied to canonical.
    Merged {
        commit_range: String,
        canonical_head: String,
    },
    /// Conflicts detected; user's repo left pristine.
    Conflict {
        conflicting_paths: Vec<String>,
        canonical_head: String,
    },
    /// Blocked: the merge could not proceed but stays retryable — canonical
    /// has uncommitted user edits overlapping merge paths (`overlapping_paths`
    /// populated), or the sandbox branch is missing/unborn (`overlapping_paths`
    /// empty; see `reason`).
    Blocked {
        reason: String,
        overlapping_paths: Vec<String>,
    },
}

/// Configuration for sandbox provisioning.
pub(crate) struct ProvisionConfig {
    /// Workspaces root directory (from config.workspaces_root).
    pub workspaces_root: PathBuf,
}

/// Provision a sandbox for an agent in a sandbox-eligible (direct-mode or
/// CoW-checkout) workspace.
///
/// 1. Probe CoW support between the canonical repository directory and the sandbox parent.
/// 2. If Unsupported, return `ProvisionOutcome::Unsupported` (fallback to shared mode; ZERO bytes copied).
/// 3. If Supported: cow_clone the canonical directory to `<workspaces_root>/<workspaceId>/sandboxes/<agentId>/<repo-slug>`.
/// 4. Create branch `sb/<agentId>` in the sandbox.
/// 5. If the source had uncommitted changes, create a snapshot commit of the dirty state.
/// 6. Persist the sandbox record.
/// 7. Return `ProvisionOutcome::Supported` with the sandbox details.
///
/// The canonical directory is never modified by this operation.
pub async fn provision_sandbox(
    store: &Store,
    workspace_id: &WorkspaceId,
    agent_id: &AgentId,
    config: &ProvisionConfig,
) -> Result<ProvisionOutcome> {
    // Test seams: artificial delay (slow-clone stand-in) and forced failure.
    // Inert unless the namespaced env vars are set.
    apply_test_provision_hooks().await?;

    // Load workspace
    let workspace = store.get_workspace(workspace_id).await?;

    // The CoW probe/clone + git2 setup are synchronous and can run for tens
    // of seconds on large checkouts; run them on the blocking pool so they
    // never occupy a core runtime worker (monorepo#954).
    let outcome = {
        let workspace_id = workspace_id.clone();
        let agent_id = agent_id.clone();
        let workspaces_root = config.workspaces_root.clone();
        tokio::task::spawn_blocking(move || {
            provision_sandbox_blocking(&workspace, &workspace_id, &agent_id, &workspaces_root)
        })
        .await
        .map_err(|e| Error::Internal(format!("sandbox provisioning task failed: {e}")))??
    };

    let ProvisionOutcome::Supported {
        path: sandbox_path,
        branch: branch_name,
        base_commit_sha,
        snapshot_commit_sha,
    } = outcome
    else {
        return Ok(ProvisionOutcome::Unsupported);
    };

    // Persist the sandbox record
    let now = now_iso();
    let sandbox = Sandbox {
        id: uuid::Uuid::new_v4().to_string(),
        workspace_id: workspace_id.clone(),
        agent_id: agent_id.clone(),
        path: sandbox_path.to_string_lossy().to_string(),
        branch: branch_name.clone(),
        base_commit_sha: base_commit_sha.clone(),
        snapshot_commit_sha: snapshot_commit_sha.clone(),
        status: SandboxStatus::Created,
        retry_count: 0,
        created_at: now.clone(),
        updated_at: now,
    };
    if let Err(e) = store.insert_sandbox(&sandbox).await {
        // The agent session row can vanish mid-clone (`agent.delete` races
        // the background provisioning; the sandbox FK cascades) — don't
        // strand the just-cloned directory when the record insert fails.
        // Best-effort, but log failures so a leaked clone is observable.
        let cleanup_path = sandbox_path.clone();
        match tokio::task::spawn_blocking(move || std::fs::remove_dir_all(&cleanup_path)).await {
            Ok(Ok(())) => {}
            Ok(Err(remove_err)) => tracing::warn!(
                sandbox_path = %sandbox_path.display(),
                error = %remove_err,
                "failed to remove sandbox directory after record insert failure"
            ),
            Err(join_err) => tracing::warn!(
                sandbox_path = %sandbox_path.display(),
                error = %join_err,
                "sandbox cleanup task failed after record insert failure"
            ),
        }
        return Err(e);
    }

    Ok(ProvisionOutcome::Supported {
        path: sandbox_path,
        branch: branch_name,
        base_commit_sha,
        snapshot_commit_sha,
    })
}

/// Synchronous half of [`provision_sandbox`]: filesystem probe, CoW clone,
/// and git2 branch/snapshot setup. Runs on the blocking pool — no store or
/// event-bus interaction happens here.
fn provision_sandbox_blocking(
    workspace: &Workspace,
    workspace_id: &WorkspaceId,
    agent_id: &AgentId,
    workspaces_root: &std::path::Path,
) -> Result<ProvisionOutcome> {
    // Resolve the canonical directory (direct mode: the user's repo folder;
    // CoW checkout: the workspace checkout). Worktree mode is rejected.
    let user_dir = resolve_user_directory(workspace)?;

    // Construct sandbox path: <workspaces_root>/<workspaceId>/sandboxes/<agentId>/<repo-slug>
    let repo_slug = repo_slug_from_workspace(workspace);
    let sandbox_parent = workspaces_root
        .join(&workspace_id.0)
        .join("sandboxes")
        .join(&agent_id.0);
    let sandbox_path = sandbox_parent.join(&repo_slug);

    // A linked worktree's `.git` is a gitfile pointing into the original
    // repository's `.git/worktrees/<name>`: CoW-cloning it would give the
    // sandbox a `.git` that still points at the ORIGINAL repo, so the branch
    // creation + checkout below would rewrite the user's source checkout.
    // Fall back to shared mode instead.
    if user_dir.join(".git").is_file() {
        tracing::warn!(
            user_dir = %user_dir.display(),
            "provision_sandbox: canonical directory is a linked git worktree (gitfile .git); falling back to shared mode"
        );
        return Ok(ProvisionOutcome::Unsupported);
    }

    // Ensure sandbox parent exists (needed for cow_probe)
    std::fs::create_dir_all(&sandbox_parent)
        .map_err(|e| Error::Internal(format!("create sandbox parent dir failed: {e}")))?;

    // Probe CoW support
    let probe_result = cow_probe(&user_dir, &sandbox_parent)?;
    if probe_result == CowSupport::Unsupported {
        return Ok(ProvisionOutcome::Unsupported);
    }

    // CoW clone the user's directory. The probe can pass while the clone
    // itself is still unsupported (e.g. a nested cross-volume mount inside the
    // tree); degrade to shared mode instead of failing the agent start.
    if let Err(e) = cow_clone(&user_dir, &sandbox_path) {
        if let Err(remove_err) = std::fs::remove_dir_all(&sandbox_path) {
            if remove_err.kind() != std::io::ErrorKind::NotFound {
                tracing::warn!(
                    sandbox_path = %sandbox_path.display(),
                    error = %remove_err,
                    "failed to remove partial sandbox directory after clone failure"
                );
            }
        }
        if matches!(e, Error::Unsupported(_)) {
            tracing::warn!(
                user_dir = %user_dir.display(),
                error = %e,
                "provision_sandbox: CoW clone unsupported despite a passing probe; falling back to shared mode"
            );
            return Ok(ProvisionOutcome::Unsupported);
        }
        return Err(e);
    }

    // Open the sandbox repo and record the base commit SHA
    let sandbox_repo = git2::Repository::open(&sandbox_path)
        .map_err(|e| Error::Internal(format!("open sandbox repo failed: {e}")))?;
    let base_commit_sha = sandbox_repo
        .head()
        .ok()
        .and_then(|h| h.target())
        .map(|oid| oid.to_string())
        .ok_or_else(|| Error::Internal("sandbox has no HEAD commit".to_string()))?;

    // Create branch sb/<agentId> in the sandbox
    let branch_name = format!("sb/{}", agent_id.0);
    let head_commit = sandbox_repo
        .head()
        .map_err(|e| Error::Internal(format!("get HEAD failed: {e}")))?
        .peel_to_commit()
        .map_err(|e| Error::Internal(format!("peel HEAD to commit failed: {e}")))?;
    sandbox_repo
        .branch(&branch_name, &head_commit, false)
        .map_err(|e| Error::Internal(format!("create branch failed: {e}")))?;

    // Check out the new branch
    let refname = format!("refs/heads/{branch_name}");
    sandbox_repo
        .set_head(&refname)
        .map_err(|e| Error::Internal(format!("set HEAD failed: {e}")))?;

    // The best-effort clone can skip a TRACKED regular file (per-entry
    // unsupported errno, or a tracked file under a skipped nested mount).
    // Unlike `provision_cow_checkout` there is no hard reset here to heal
    // it, and left alone the missing file would read as a deletion that
    // `create_snapshot_commit` records — and a later merge-back could
    // propagate to the user's tree. Restore just the index-tracked paths
    // missing from the worktree before the dirty check; genuinely dirty
    // state is untouched.
    restore_missing_tracked_files(&sandbox_repo, &sandbox_path)?;

    // Check for dirty state and create a snapshot commit if needed
    let snapshot_commit_sha = if is_dirty(&sandbox_repo)? {
        Some(create_snapshot_commit(&sandbox_repo, agent_id)?)
    } else {
        None
    };

    Ok(ProvisionOutcome::Supported {
        path: sandbox_path,
        branch: branch_name,
        base_commit_sha,
        snapshot_commit_sha,
    })
}

/// Discard a sandbox: remove the directory and the database record.
pub(crate) async fn discard_sandbox(
    store: &Store,
    workspace_id: &WorkspaceId,
    agent_id: &AgentId,
) -> Result<()> {
    // Load the sandbox record to get the path
    let sandbox = store.get_sandbox(workspace_id, agent_id).await?;
    if let Some(s) = sandbox {
        // Remove the directory
        let path = PathBuf::from(&s.path);
        if path.exists() {
            std::fs::remove_dir_all(&path)
                .map_err(|e| Error::Internal(format!("remove sandbox directory failed: {e}")))?;
        }
    }

    // Delete the record (whether or not the directory existed)
    store.delete_sandbox(workspace_id, agent_id).await?;

    Ok(())
}

/// Garbage-collect orphaned sandboxes: remove sandboxes whose agent no longer exists
/// or whose directory is missing.
#[cfg(test)]
pub(crate) async fn gc_orphaned_sandboxes(store: &Store) -> Result<()> {
    let all_sandboxes = store.list_all_sandboxes().await?;

    for sandbox in all_sandboxes {
        let mut should_remove = false;

        // Check if the agent session still exists
        let agent_exists = store.get_agent_session(&sandbox.agent_id).await.is_ok();
        if !agent_exists {
            should_remove = true;
        }

        // Check if the directory exists
        let path = PathBuf::from(&sandbox.path);
        if !path.exists() {
            should_remove = true;
        }

        if should_remove {
            // Remove the directory if it exists
            if path.exists() {
                let _ = std::fs::remove_dir_all(&path);
            }
            // Delete the record
            store
                .delete_sandbox(&sandbox.workspace_id, &sandbox.agent_id)
                .await?;
        }
    }

    Ok(())
}

/// Merge sandbox commits back to the canonical repository.
///
/// 1. Auto-commit any dirty sandbox state (if present).
/// 2. Check canonical repository for dirty state overlapping with sandbox changes.
/// 3. Fetch sandbox branch into canonical.
/// 4. Apply commits after the snapshot (or base if no snapshot) via cherry-pick.
/// 5. On conflict: abort cleanly, return Conflict with paths.
/// 6. On dirty overlap: return Blocked.
/// 7. On success: return Merged with the applied range.
///
/// The canonical repository is never left mid-merge/cherry-pick (always abort on failure).
pub(crate) async fn merge_sandbox(
    store: &Store,
    workspace_id: &WorkspaceId,
    agent_id: &AgentId,
) -> Result<MergeOutcome> {
    // Load sandbox record
    let sandbox = store
        .get_sandbox(workspace_id, agent_id)
        .await?
        .ok_or_else(|| Error::NotFound(format!("sandbox not found for agent {}", agent_id.0)))?;

    // Load workspace to get canonical repo path
    let workspace = store.get_workspace(workspace_id).await?;
    let canonical_path = resolve_user_directory(&workspace)?;
    let sandbox_path = PathBuf::from(&sandbox.path);

    // Open both repositories
    let canonical_repo = git2::Repository::open(&canonical_path)
        .map_err(|e| Error::Internal(format!("open canonical repo failed: {e}")))?;
    let sandbox_repo = git2::Repository::open(&sandbox_path)
        .map_err(|e| Error::Internal(format!("open sandbox repo failed: {e}")))?;

    // Auto-commit any dirty sandbox state (preserving agent attribution)
    if is_dirty(&sandbox_repo)? {
        let sig = resolve_signature(&sandbox_repo)?;
        let mut index = sandbox_repo
            .index()
            .map_err(|e| Error::Internal(format!("get sandbox index failed: {e}")))?;
        // Untracked nested repos/worktrees cannot be staged: libgit2's
        // add_all rejects their paths (`invalid path`), and `git add` skips
        // embedded repos too.
        let skipped = stage_all_skipping_nested(&sandbox_repo, &mut index)?;
        if !skipped.is_empty() {
            tracing::warn!(
                sandbox = %sandbox_path.display(),
                skipped = ?skipped,
                "sandbox auto-commit: skipping untracked nested git repos/worktrees"
            );
        }
        index
            .write()
            .map_err(|e| Error::Internal(format!("write sandbox index failed: {e}")))?;
        let tree_oid = index
            .write_tree()
            .map_err(|e| Error::Internal(format!("write sandbox tree failed: {e}")))?;
        let tree = sandbox_repo
            .find_tree(tree_oid)
            .map_err(|e| Error::Internal(format!("find sandbox tree failed: {e}")))?;
        let head = sandbox_repo
            .head()
            .map_err(|e| Error::Internal(format!("get sandbox HEAD failed: {e}")))?;
        let parent = head
            .peel_to_commit()
            .map_err(|e| Error::Internal(format!("peel sandbox HEAD failed: {e}")))?;
        sandbox_repo
            .commit(
                Some("HEAD"),
                &sig,
                &sig,
                &format!("Auto-commit dirty state for {}", agent_id.0),
                &tree,
                &[&parent],
            )
            .map_err(|e| Error::Internal(format!("auto-commit sandbox failed: {e}")))?;
    }

    // Get canonical HEAD
    let canonical_head_ref = canonical_repo
        .head()
        .map_err(|e| Error::Internal(format!("get canonical HEAD failed: {e}")))?;
    let canonical_head_commit = canonical_head_ref
        .peel_to_commit()
        .map_err(|e| Error::Internal(format!("peel canonical HEAD failed: {e}")))?;

    // Check for dirty state in canonical
    let canonical_dirty = is_dirty(&canonical_repo)?;
    if canonical_dirty {
        // Get the list of changed files in canonical
        let canonical_changed = get_changed_files(&canonical_repo)?;

        // Get the list of files changed by the sandbox (from base to HEAD)
        let base_sha = sandbox
            .snapshot_commit_sha
            .as_ref()
            .unwrap_or(&sandbox.base_commit_sha);
        let sandbox_changed = get_files_in_range(&sandbox_repo, base_sha, "HEAD")?;

        // Check for overlap
        let overlap: Vec<String> = canonical_changed
            .iter()
            .filter(|f| sandbox_changed.contains(f))
            .cloned()
            .collect();

        if !overlap.is_empty() {
            return Ok(MergeOutcome::Blocked {
                reason:
                    "Canonical repository has uncommitted changes overlapping with sandbox changes"
                        .to_string(),
                overlapping_paths: overlap,
            });
        }
    }

    // A missing or unborn sandbox branch is not an internal error: the agent
    // never committed anything on it. Surface a typed Blocked outcome so
    // callers keep the sandbox retryable.
    let branch_ref_name = format!("refs/heads/{}", sandbox.branch);
    let branch_is_committish = sandbox_repo
        .find_reference(&branch_ref_name)
        .and_then(|r| r.peel_to_commit())
        .is_ok();
    if !branch_is_committish {
        tracing::warn!(
            agent = %agent_id.0,
            branch = %sandbox.branch,
            sandbox_path = %sandbox.path,
            "sandbox merge skipped: branch is missing or unborn in the sandbox repo"
        );
        return Ok(MergeOutcome::Blocked {
            reason: format!(
                "sandbox branch '{}' is missing or unborn in the sandbox repository",
                sandbox.branch
            ),
            overlapping_paths: Vec::new(),
        });
    }

    // Defensive audit: today only sb/<agentId> ever diverges from the
    // workspace repo; warn if any OTHER local sandbox branch has a tip the
    // workspace repo cannot reach.
    let diverged = audit_diverged_sandbox_branches(&sandbox_repo, &canonical_repo, &sandbox.branch);
    if !diverged.is_empty() {
        tracing::warn!(
            agent = %agent_id.0,
            branches = ?diverged,
            sandbox_path = %sandbox.path,
            "sandbox has local branches (other than the merge branch) not reachable in the workspace repo"
        );
    }

    // Fetch sandbox branch into canonical (no checkout, just fetch).
    // Shell out to git with an explicit full refspec into a temporary ref and
    // tag auto-follow disabled: CoW repos carry non-commit refs
    // (refs/intent/blobs/*, refs/stash) that libgit2's local transport trips
    // over ("object is not a committish", InvalidSpec) — its pack negotiation
    // revwalk-hides every local ref and rejects blob targets.
    let sandbox_path_str = sandbox_path
        .to_str()
        .ok_or_else(|| Error::Internal("sandbox path not UTF-8".to_string()))?;
    let temp_ref = format!("refs/intent/sandbox-merge/{}", agent_id.0);
    let refspec = format!("+{branch_ref_name}:{temp_ref}");

    // Sandbox paths are intentd-controlled absolute paths under
    // workspaces_root, so the positional <repository> argument cannot be
    // mistaken for an option or a remote-helper URL. GIT_TERMINAL_PROMPT=0 and
    // a null stdin force fail-fast instead of a hidden credential prompt
    // (parity with intent-git/src/fetch.rs); the transport is local-only.
    let fetch_out = std::process::Command::new("git")
        .arg("fetch")
        .arg("--no-tags")
        .arg("--quiet")
        .arg(sandbox_path_str)
        .arg(&refspec)
        .current_dir(&canonical_path)
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(std::process::Stdio::null())
        .output()
        .map_err(|e| Error::Internal(format!("fetch sandbox branch failed: {e}")))?;
    if !fetch_out.status.success() {
        return Err(Error::Internal(format!(
            "fetch sandbox branch failed: {}",
            String::from_utf8_lossy(&fetch_out.stderr).trim()
        )));
    }

    let outcome = apply_sandbox_commits(
        &canonical_repo,
        &sandbox_repo,
        &sandbox,
        &canonical_head_commit,
    );

    // The temp ref only anchors the fetch; drop it regardless of outcome.
    if let Ok(mut r) = canonical_repo.find_reference(&temp_ref) {
        let _ = r.delete();
    }

    outcome
}

/// Cherry-pick the sandbox commits (post-snapshot, or post-base) onto the
/// canonical HEAD. Assumes the sandbox branch objects are already present in
/// the canonical ODB (fetched by [`merge_sandbox`]).
fn apply_sandbox_commits(
    canonical_repo: &git2::Repository,
    sandbox_repo: &git2::Repository,
    sandbox: &Sandbox,
    canonical_head_commit: &git2::Commit<'_>,
) -> Result<MergeOutcome> {
    let canonical_head_sha = canonical_head_commit.id().to_string();

    // Get the range of commits to cherry-pick: from snapshot (or base) to the
    // sandbox branch tip. Resolve the branch ref rather than HEAD — the fetch
    // in `merge_sandbox` only brought over `refs/heads/<branch>` objects, so a
    // detached or re-pointed HEAD would yield a range whose commits are absent
    // from the canonical ODB.
    let start_sha = sandbox
        .snapshot_commit_sha
        .as_ref()
        .unwrap_or(&sandbox.base_commit_sha);
    let sandbox_head = sandbox_repo
        .find_reference(&format!("refs/heads/{}", sandbox.branch))
        .map_err(|e| Error::Internal(format!("get sandbox branch failed: {e}")))?
        .peel_to_commit()
        .map_err(|e| Error::Internal(format!("peel sandbox branch failed: {e}")))?;
    let sandbox_head_sha = sandbox_head.id().to_string();

    // Get commits to apply (reversed for cherry-pick order)
    let commits_to_apply = get_commits_after(sandbox_repo, start_sha, &sandbox_head_sha)?;

    if commits_to_apply.is_empty() {
        // No commits to apply (only the snapshot, or base == HEAD)
        return Ok(MergeOutcome::Merged {
            commit_range: format!("{start_sha}..{sandbox_head_sha} (empty)"),
            canonical_head: canonical_head_sha,
        });
    }

    // Cherry-pick each commit onto canonical
    let canonical_oid = canonical_head_commit.id();
    let mut current_oid = canonical_oid;

    for commit_sha in &commits_to_apply {
        let commit_oid = git2::Oid::from_str(commit_sha)
            .map_err(|e| Error::Internal(format!("parse commit OID failed: {e}")))?;
        let commit = canonical_repo
            .find_commit(commit_oid)
            .map_err(|e| Error::Internal(format!("find commit failed: {e}")))?;

        let current_commit = canonical_repo
            .find_commit(current_oid)
            .map_err(|e| Error::Internal(format!("find current commit failed: {e}")))?;

        // Try to cherry-pick
        let mut cherry_pick_opts = git2::CherrypickOptions::new();
        match canonical_repo.cherrypick(&commit, Some(&mut cherry_pick_opts)) {
            Ok(()) => {
                // Check if there are conflicts
                let mut index = canonical_repo
                    .index()
                    .map_err(|e| Error::Internal(format!("get index failed: {e}")))?;

                if index.has_conflicts() {
                    // Get conflicting paths before cleanup
                    let conflicting_paths = get_conflicting_paths(&index)?;

                    // Clean up the repository state (reset index and working directory)
                    canonical_repo
                        .reset(
                            canonical_head_commit.as_object(),
                            git2::ResetType::Hard,
                            None,
                        )
                        .map_err(|e| {
                            Error::Internal(format!("reset after conflict failed: {e}"))
                        })?;
                    canonical_repo.cleanup_state().ok();

                    return Ok(MergeOutcome::Conflict {
                        conflicting_paths,
                        canonical_head: canonical_head_sha,
                    });
                }

                // Commit the cherry-pick
                let tree_oid = index
                    .write_tree()
                    .map_err(|e| Error::Internal(format!("write tree failed: {e}")))?;
                let tree = canonical_repo
                    .find_tree(tree_oid)
                    .map_err(|e| Error::Internal(format!("find tree failed: {e}")))?;

                // Preserve original commit message and author
                let new_oid = canonical_repo
                    .commit(
                        Some("HEAD"),
                        &commit.author(),
                        &commit.committer(),
                        commit.message().unwrap_or(""),
                        &tree,
                        &[&current_commit],
                    )
                    .map_err(|e| Error::Internal(format!("commit cherry-pick failed: {e}")))?;
                canonical_repo.cleanup_state().ok();

                current_oid = new_oid;
            }
            Err(e) => {
                // Cherry-pick failed, reset to clean state
                let _ = canonical_repo.reset(
                    canonical_head_commit.as_object(),
                    git2::ResetType::Hard,
                    None,
                );
                canonical_repo.cleanup_state().ok();
                return Err(Error::Internal(format!("cherrypick failed: {e}")));
            }
        }
    }

    Ok(MergeOutcome::Merged {
        commit_range: format!("{start_sha}..{sandbox_head_sha}"),
        canonical_head: current_oid.to_string(),
    })
}

/// Defensive audit: list local sandbox branches (other than the merge branch)
/// whose tips are not reachable from any local branch tip (or HEAD) of the
/// workspace repo. Today only `sb/<agentId>` ever diverges; a non-empty result
/// means that assumption broke. Best-effort: errors are swallowed.
fn audit_diverged_sandbox_branches(
    sandbox_repo: &git2::Repository,
    canonical_repo: &git2::Repository,
    merge_branch: &str,
) -> Vec<String> {
    let mut diverged = Vec::new();
    let Ok(branches) = sandbox_repo.branches(Some(git2::BranchType::Local)) else {
        return diverged;
    };

    let mut canonical_tips: Vec<git2::Oid> = Vec::new();
    if let Some(oid) = canonical_repo.head().ok().and_then(|h| h.target()) {
        canonical_tips.push(oid);
    }
    if let Ok(canonical_branches) = canonical_repo.branches(Some(git2::BranchType::Local)) {
        for (branch, _) in canonical_branches.flatten() {
            if let Some(oid) = branch.get().target() {
                canonical_tips.push(oid);
            }
        }
    }

    for (branch, _) in branches.flatten() {
        let Ok(Some(name)) = branch.name().map(|n| n.map(str::to_string)) else {
            continue;
        };
        if name == merge_branch {
            continue;
        }
        let Some(tip) = branch.get().target() else {
            continue;
        };
        let reachable = canonical_repo.find_commit(tip).is_ok()
            && canonical_tips
                .iter()
                .any(|c| *c == tip || canonical_repo.graph_descendant_of(*c, tip).unwrap_or(false));
        if !reachable {
            diverged.push(name);
        }
    }
    diverged
}

/// Resolve the canonical repository directory for a workspace's sandboxes —
/// the sandbox clone source and the merge-back target.
///
/// - CoW-checkout workspaces (`checkoutMode == "cow"`): the workspace
///   checkout (`worktree_path`) is canonical.
/// - Direct-checkout workspaces (`checkoutMode == "direct"`, standalone plain
///   repo): the workspace checkout (`worktree_path`) when one was provisioned
///   (cache hydration), else the repository folder itself (`isNewRepo`
///   initialization).
/// - Worktree workspaces share the checkout with the user (no sandboxes):
///   returns an error.
/// - Otherwise (skip_worktree = true OR no worktree provisioned): the user's
///   repository folder (`repository_path`).
fn resolve_user_directory(workspace: &Workspace) -> Result<PathBuf> {
    let repo_path = match workspace.checkout_mode {
        Some(CheckoutMode::Cow) => workspace.worktree_path.as_ref().ok_or_else(|| {
            Error::InvalidParams("CoW workspace has no worktree_path".to_string())
        })?,
        Some(CheckoutMode::Direct) => workspace
            .worktree_path
            .as_ref()
            .or(workspace.repository_path.as_ref())
            .ok_or_else(|| {
                Error::InvalidParams(
                    "direct workspace has neither worktree_path nor repository_path".to_string(),
                )
            })?,
        Some(CheckoutMode::Worktree) => {
            return Err(Error::InvalidParams(
                "worktree-mode workspaces do not support agent sandboxes".to_string(),
            ));
        }
        None => workspace
            .repository_path
            .as_ref()
            .ok_or_else(|| Error::InvalidParams("workspace has no repository_path".to_string()))?,
    };

    let path = PathBuf::from(repo_path);
    if !path.exists() {
        return Err(Error::InvalidParams(format!(
            "repository path does not exist: {repo_path}"
        )));
    }

    // Verify it's a git repository
    if !path.join(".git").exists() {
        return Err(Error::InvalidParams(format!(
            "repository path is not a git repository: {repo_path}"
        )));
    }

    Ok(path)
}

/// Derive a repository slug from the workspace (repository name, sanitized).
fn repo_slug_from_workspace(workspace: &Workspace) -> String {
    workspace.repository_name.as_ref().map_or_else(
        || {
            workspace
                .repository_path
                .as_ref()
                .and_then(|p| {
                    PathBuf::from(p)
                        .file_name()
                        .map(|n| n.to_string_lossy().to_string())
                })
                .unwrap_or_else(|| "repo".to_string())
        },
        |n| slugify(n),
    )
}

/// Simple slugification: lowercase, replace non-alphanumeric with hyphens.
fn slugify(s: &str) -> String {
    s.to_lowercase()
        .chars()
        .map(|c| if c.is_alphanumeric() { c } else { '-' })
        .collect::<String>()
        .trim_matches('-')
        .to_string()
}

/// Restore index-tracked files that are missing from the worktree (the
/// best-effort CoW clone may have skipped them as non-clonable). Checks out
/// ONLY those paths from the index, so genuinely dirty state — modified
/// tracked files, staged changes, untracked files — is left untouched.
fn restore_missing_tracked_files(
    repo: &git2::Repository,
    worktree_root: &std::path::Path,
) -> Result<()> {
    let index = repo
        .index()
        .map_err(|e| Error::Internal(format!("get index failed: {e}")))?;
    let mut missing: Vec<Vec<u8>> = Vec::new();
    for entry in index.iter() {
        let rel = std::path::Path::new(
            std::str::from_utf8(&entry.path)
                .map_err(|e| Error::Internal(format!("non-UTF-8 path in sandbox index: {e}")))?,
        );
        if worktree_root.join(rel).symlink_metadata().is_err() {
            missing.push(entry.path.clone());
        }
    }
    if missing.is_empty() {
        return Ok(());
    }
    tracing::warn!(
        worktree = %worktree_root.display(),
        count = missing.len(),
        "provision_sandbox: restoring tracked files missing from the CoW clone (skipped by the best-effort walk)"
    );
    let mut opts = git2::build::CheckoutBuilder::new();
    opts.force().update_index(false);
    for path in &missing {
        opts.path(std::path::Path::new(std::str::from_utf8(path).unwrap()));
    }
    repo.checkout_index(None, Some(&mut opts))
        .map_err(|e| Error::Internal(format!("restore missing tracked files failed: {e}")))?;
    Ok(())
}

/// Resolve a git signature for authoring commits, falling back to a stable
/// default identity when the user has no `user.name`/`user.email` configured.
///
/// When git identity IS configured, the real signature is used unchanged. The
/// fallback to a fixed `Intent <intent@localhost>` identity is narrowed to the
/// missing-identity error class (`git2::ErrorCode::NotFound`, i.e. "config
/// value 'user.name' was not found"), so sandbox snapshot/auto-commit still
/// succeeds on machines/CI without a git identity. Any other
/// `repo.signature()` failure (e.g. corrupt/unreadable config) is propagated
/// instead of being masked by the default identity.
fn resolve_signature(repo: &git2::Repository) -> Result<git2::Signature<'static>> {
    signature_or_fallback(repo.signature())
}

/// Error-classification half of [`resolve_signature`], split out so the
/// fallback narrowing is unit-testable with constructed `git2::Error`s.
fn signature_or_fallback(
    sig: std::result::Result<git2::Signature<'static>, git2::Error>,
) -> Result<git2::Signature<'static>> {
    match sig {
        Ok(sig) => Ok(sig),
        Err(e) if e.code() == git2::ErrorCode::NotFound => {
            git2::Signature::now("Intent", "intent@localhost")
                .map_err(|e| Error::Internal(format!("construct fallback signature failed: {e}")))
        }
        Err(e) => Err(Error::Internal(format!(
            "resolve git signature failed: {e}"
        ))),
    }
}

/// Create a snapshot commit of the current dirty state in the repository.
/// Stages all changes (tracked and untracked) and commits them with a snapshot message.
fn create_snapshot_commit(repo: &git2::Repository, agent_id: &AgentId) -> Result<String> {
    // Stage all changes
    let mut index = repo
        .index()
        .map_err(|e| Error::Internal(format!("get index failed: {e}")))?;

    // Add all files (including untracked), skipping untracked nested git
    // repos/worktrees: libgit2's add_all rejects their paths (`invalid
    // path`), and `git add` skips embedded repos too.
    let skipped = stage_all_skipping_nested(repo, &mut index)?;
    if !skipped.is_empty() {
        tracing::warn!(
            path = ?repo.workdir(),
            skipped = ?skipped,
            "sandbox snapshot: skipping untracked nested git repos/worktrees"
        );
    }

    index
        .write()
        .map_err(|e| Error::Internal(format!("write index failed: {e}")))?;

    // Create the tree
    let tree_oid = index
        .write_tree()
        .map_err(|e| Error::Internal(format!("write tree failed: {e}")))?;
    let tree = repo
        .find_tree(tree_oid)
        .map_err(|e| Error::Internal(format!("find tree failed: {e}")))?;

    // Get parent commit
    let parent = repo
        .head()
        .ok()
        .and_then(|h| h.target())
        .and_then(|oid| repo.find_commit(oid).ok());

    let sig = resolve_signature(repo)?;

    let message = format!("WIP snapshot for {}", agent_id.0);
    let parents: Vec<&git2::Commit> = parent.iter().collect();

    let oid = repo
        .commit(Some("HEAD"), &sig, &sig, &message, &tree, &parents)
        .map_err(|e| Error::Internal(format!("create commit failed: {e}")))?;

    Ok(oid.to_string())
}

/// Get the list of changed files in a repository (dirty state).
fn get_changed_files(repo: &git2::Repository) -> Result<Vec<String>> {
    let mut opts = git2::StatusOptions::new();
    opts.include_untracked(true)
        .recurse_untracked_dirs(true)
        .include_ignored(false);
    let statuses = repo
        .statuses(Some(&mut opts))
        .map_err(|e| Error::Internal(format!("git status failed: {e}")))?;

    let mut files = Vec::new();
    for entry in statuses.iter() {
        if let Ok(path) = entry.path() {
            files.push(path.to_string());
        }
    }
    Ok(files)
}

/// Get the list of files changed in a commit range.
fn get_files_in_range(
    repo: &git2::Repository,
    start_sha: &str,
    end_sha: &str,
) -> Result<Vec<String>> {
    let start_oid = git2::Oid::from_str(start_sha)
        .map_err(|e| Error::Internal(format!("parse start OID failed: {e}")))?;
    let end_oid = if end_sha == "HEAD" {
        repo.head()
            .map_err(|e| Error::Internal(format!("get HEAD failed: {e}")))?
            .target()
            .ok_or_else(|| Error::Internal("HEAD has no target".to_string()))?
    } else {
        git2::Oid::from_str(end_sha)
            .map_err(|e| Error::Internal(format!("parse end OID failed: {e}")))?
    };

    let start_commit = repo
        .find_commit(start_oid)
        .map_err(|e| Error::Internal(format!("find start commit failed: {e}")))?;
    let end_commit = repo
        .find_commit(end_oid)
        .map_err(|e| Error::Internal(format!("find end commit failed: {e}")))?;

    let start_tree = start_commit
        .tree()
        .map_err(|e| Error::Internal(format!("get start tree failed: {e}")))?;
    let end_tree = end_commit
        .tree()
        .map_err(|e| Error::Internal(format!("get end tree failed: {e}")))?;

    let diff = repo
        .diff_tree_to_tree(Some(&start_tree), Some(&end_tree), None)
        .map_err(|e| Error::Internal(format!("diff trees failed: {e}")))?;

    let mut files = Vec::new();
    diff.foreach(
        &mut |delta, _| {
            if let Some(path) = delta.new_file().path() {
                if let Some(path_str) = path.to_str() {
                    files.push(path_str.to_string());
                }
            }
            true
        },
        None,
        None,
        None,
    )
    .map_err(|e| Error::Internal(format!("diff foreach failed: {e}")))?;

    Ok(files)
}

/// Get the list of commits after start_sha up to end_sha (exclusive of start, inclusive of end).
fn get_commits_after(
    repo: &git2::Repository,
    start_sha: &str,
    end_sha: &str,
) -> Result<Vec<String>> {
    let start_oid = git2::Oid::from_str(start_sha)
        .map_err(|e| Error::Internal(format!("parse start OID failed: {e}")))?;
    let end_oid = git2::Oid::from_str(end_sha)
        .map_err(|e| Error::Internal(format!("parse end OID failed: {e}")))?;

    let mut revwalk = repo
        .revwalk()
        .map_err(|e| Error::Internal(format!("create revwalk failed: {e}")))?;
    revwalk
        .push(end_oid)
        .map_err(|e| Error::Internal(format!("push end OID failed: {e}")))?;
    revwalk
        .hide(start_oid)
        .map_err(|e| Error::Internal(format!("hide start OID failed: {e}")))?;

    let mut commits = Vec::new();
    for oid in revwalk {
        let oid = oid.map_err(|e| Error::Internal(format!("revwalk iteration failed: {e}")))?;
        commits.push(oid.to_string());
    }

    // Reverse to get chronological order (oldest first)
    commits.reverse();
    Ok(commits)
}

/// Get the list of conflicting file paths from an index with conflicts.
fn get_conflicting_paths(index: &git2::Index) -> Result<Vec<String>> {
    let mut paths = Vec::new();
    for entry in index
        .conflicts()
        .map_err(|e| Error::Internal(format!("get conflicts failed: {e}")))?
    {
        let conflict =
            entry.map_err(|e| Error::Internal(format!("iterate conflicts failed: {e}")))?;
        // Use the "our" side path (or "their" side if "our" is missing)
        if let Some(our) = conflict.our {
            let path = String::from_utf8_lossy(&our.path).to_string();
            paths.push(path);
        } else if let Some(their) = conflict.their {
            let path = String::from_utf8_lossy(&their.path).to_string();
            paths.push(path);
        }
    }

    Ok(paths)
}

#[cfg(test)]
mod test_hook_tests {
    use super::*;

    #[test]
    fn unset_disables_delay() {
        assert_eq!(test_provision_delay_from(None), None);
    }

    #[test]
    fn positive_millis_enable_delay() {
        assert_eq!(
            test_provision_delay_from(Some("10000")),
            Some(Duration::from_millis(10_000))
        );
        assert_eq!(
            test_provision_delay_from(Some(" 500 ")),
            Some(Duration::from_millis(500))
        );
    }

    #[test]
    fn invalid_values_disable_delay() {
        for raw in ["0", "-5", "abc", "", "1.5"] {
            assert_eq!(test_provision_delay_from(Some(raw)), None, "raw={raw:?}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::WorkspaceStatus;
    use intent_store::Store;
    use std::fs;
    use std::path::Path;

    struct TempDb {
        path: PathBuf,
    }

    impl TempDb {
        fn new() -> Self {
            let path =
                std::env::temp_dir().join(format!("sandbox-test-{}.db", uuid::Uuid::new_v4()));
            Self { path }
        }
    }

    impl Drop for TempDb {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm"] {
                let _ =
                    std::fs::remove_file(PathBuf::from(format!("{}{suffix}", self.path.display())));
            }
        }
    }

    async fn temp_store() -> (Store, TempDb) {
        let db = TempDb::new();
        let store = Store::open(&db.path).await.unwrap();
        (store, db)
    }

    fn temp_repo(name: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::TempDir::new().unwrap();
        let repo_path = dir.path().join(name);
        init_test_repo(&repo_path);
        (dir, repo_path)
    }

    /// Create a test repo under a specific parent directory (for same-volume CoW tests)
    /// Uses workspace root's target dir, not crate's target dir, to ensure same volume.
    fn temp_repo_in_target(name: &str) -> (PathBuf, PathBuf) {
        // Navigate to workspace root (up from crates/intent-services)
        let workspace_root = std::env::current_dir()
            .unwrap()
            .ancestors()
            .nth(2) // packages/intentd
            .unwrap()
            .to_path_buf();
        let test_root = workspace_root
            .join("target")
            .join(format!("test-sandbox-{}", uuid::Uuid::new_v4()));
        let repo_path = test_root.join(name);
        init_test_repo(&repo_path);
        (test_root, repo_path)
    }

    fn init_test_repo(repo_path: &Path) {
        fs::create_dir_all(repo_path).unwrap();

        // Initialize a git repository
        let repo = git2::Repository::init(repo_path).unwrap();

        // Create an initial commit
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();
        let tree_id = {
            let mut index = repo.index().unwrap();
            index.write_tree().unwrap()
        };
        let tree = repo.find_tree(tree_id).unwrap();
        repo.commit(Some("HEAD"), &sig, &sig, "Initial commit", &tree, &[])
            .unwrap();
    }

    async fn create_test_agent(store: &Store, ws_id: &WorkspaceId, agent_id: &AgentId) {
        let agent = intent_core::AgentSession {
            harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
            harness_features: None,
            id: agent_id.clone(),
            workspace_id: ws_id.clone(),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: "Test Agent".to_string(),
            name_explicitly_set: false,
            model: None,
            reasoning_effort: None,
            effort_levels: None,
            provider: None,
            system_prompt: None,
            specialist: None,
            status: intent_core::AgentStatus::Active,
            is_active: true,
            messages: vec![],
            stats: None,
            task_note_id: None,
            skip_auto_commit: false,
            completion_report: None,
            completion_report_timestamp: None,
            attention_request_kind: None,
            attention_request_reason: None,
            attention_request_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            file_blocks: None,
            is_background: false,
            metadata: None,
            created_at: now_iso(),
            updated_at: now_iso(),
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
            pending_delete_at: None,
        };
        store.insert_agent_session(&agent).await.unwrap();
    }

    fn workspace_for_repo(repo_path: &Path) -> Workspace {
        let now = now_iso();
        Workspace {
            id: WorkspaceId::new(),
            title: "Test WS".to_string(),
            branch: "main".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: WorkspaceStatus::Active,
            status_message: None,
            status_image_asset_id: None,
            activity: intent_core::WorkspaceActivity::Idle,
            attention: intent_core::WorkspaceAttention::None,
            created_at: now.clone(),
            updated_at: now,
            last_activity: None,
            tags: vec![],
            path: None,
            repository_path: Some(repo_path.to_string_lossy().to_string()),
            repository_owner: None,
            repository_name: Some("test-repo".to_string()),
            worktree_path: None,
            scope: None,
            skip_worktree: true,
            setup_script: None,
            is_remote: false,
            default_model: None,
            pr_number: None,
            pr_url: None,
            pr_status: None,
            active_pull_request: None,
            pull_requests: None,
            archived: false,
            archived_at: None,
            task_stats: None,
            agent_summary: None,
            diff_summary: None,
            token_usage: None,
            cow_supported: None,
            display_status: None,
            waiting: false,
            checkout_mode: None,
            disk_usage: None,
            pending_delete_at: None,
        }
    }

    #[tokio::test]
    async fn provision_creates_sandbox_and_leaves_source_untouched() {
        let (store, _db) = temp_store().await;
        let (test_root, repo_path) = temp_repo_in_target("source");

        // Use same test_root for workspaces to ensure same volume
        let workspaces_root = test_root.join("workspaces");

        // Early probe check - skip test if CoW not available (e.g., non-CoW filesystem)
        fs::create_dir_all(&workspaces_root).unwrap();
        let probe = cow_probe(&repo_path, &workspaces_root).unwrap();
        if probe == CowSupport::Unsupported {
            eprintln!(
                "Skipping test: CoW not supported between {repo_path:?} and {workspaces_root:?}"
            );
            let _ = fs::remove_dir_all(&test_root);
            return;
        }

        // Add a test file to the source repo
        fs::write(repo_path.join("test.txt"), "hello").unwrap();
        let source_repo = git2::Repository::open(&repo_path).unwrap();
        let mut index = source_repo.index().unwrap();
        index.add_path(std::path::Path::new("test.txt")).unwrap();
        index.write().unwrap();
        let tree_id = index.write_tree().unwrap();
        let tree = source_repo.find_tree(tree_id).unwrap();
        let parent = source_repo.head().unwrap().peel_to_commit().unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();
        source_repo
            .commit(Some("HEAD"), &sig, &sig, "Add test file", &tree, &[&parent])
            .unwrap();

        // Capture state before provisioning
        let source_head_before = source_repo.head().unwrap().target().unwrap().to_string();
        let source_index_before = source_repo.index().unwrap();
        let staged_count_before = source_index_before.len();
        let test_file_content_before = fs::read_to_string(repo_path.join("test.txt")).unwrap();

        // Create workspace
        let ws = workspace_for_repo(&repo_path);
        store.insert_workspace(&ws).await.unwrap();

        // Create agent
        let agent_id = AgentId::new();
        let agent = intent_core::AgentSession {
            harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
            harness_features: None,
            id: agent_id.clone(),
            workspace_id: ws.id.clone(),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: "Test Agent".to_string(),
            name_explicitly_set: false,
            model: None,
            reasoning_effort: None,
            effort_levels: None,
            provider: None,
            system_prompt: None,
            specialist: None,
            status: intent_core::AgentStatus::Active,
            is_active: true,
            messages: vec![],
            stats: None,
            task_note_id: None,
            skip_auto_commit: false,
            completion_report: None,
            completion_report_timestamp: None,
            attention_request_kind: None,
            attention_request_reason: None,
            attention_request_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            file_blocks: None,
            is_background: false,
            metadata: None,
            created_at: now_iso(),
            updated_at: now_iso(),
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
            pending_delete_at: None,
        };
        store.insert_agent_session(&agent).await.unwrap();

        // Provision sandbox (use workspaces_root in same TempDir to ensure same volume)
        fs::create_dir_all(&workspaces_root).unwrap();
        let config = ProvisionConfig {
            workspaces_root: workspaces_root.clone(),
        };
        let outcome = provision_sandbox(&store, &ws.id, &agent_id, &config)
            .await
            .unwrap();

        // We probed upfront, so this MUST be Supported
        let ProvisionOutcome::Supported {
            path,
            branch,
            base_commit_sha,
            snapshot_commit_sha,
        } = outcome
        else {
            panic!("Expected Supported after probe confirmed CoW available");
        };

        // Verify sandbox was created
        assert!(path.exists());
        assert!(path.join(".git").exists());
        assert_eq!(branch, format!("sb/{}", agent_id.0));

        // Verify the source repo was COMPLETELY UNTOUCHED
        let source_head_after = source_repo.head().unwrap().target().unwrap().to_string();
        assert_eq!(
            source_head_before, source_head_after,
            "HEAD must not change"
        );

        let source_index_after = source_repo.index().unwrap();
        assert_eq!(
            staged_count_before,
            source_index_after.len(),
            "Index must not change"
        );

        let test_file_content_after = fs::read_to_string(repo_path.join("test.txt")).unwrap();
        assert_eq!(
            test_file_content_before, test_file_content_after,
            "File content must be byte-identical"
        );

        // Verify base commit matches source HEAD
        assert_eq!(base_commit_sha, source_head_before);

        // Clean sandbox has no snapshot commit
        assert_eq!(snapshot_commit_sha, None);

        // Verify sandbox record was persisted
        let sandbox = store.get_sandbox(&ws.id, &agent_id).await.unwrap();
        assert!(sandbox.is_some());

        // Clean up
        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn provision_dirty_state_creates_snapshot_commit() {
        let (store, _db) = temp_store().await;
        let (test_root, repo_path) = temp_repo_in_target("source-dirty");

        // Use same test_root for workspaces to ensure same volume
        let workspaces_root = test_root.join("workspaces");

        // Early probe check - skip test if CoW not available
        fs::create_dir_all(&workspaces_root).unwrap();
        let probe = cow_probe(&repo_path, &workspaces_root).unwrap();
        if probe == CowSupport::Unsupported {
            eprintln!(
                "Skipping test: CoW not supported between {repo_path:?} and {workspaces_root:?}"
            );
            let _ = fs::remove_dir_all(&test_root);
            return;
        }

        // Add a committed file
        fs::write(repo_path.join("committed.txt"), "committed").unwrap();
        let source_repo = git2::Repository::open(&repo_path).unwrap();
        let mut index = source_repo.index().unwrap();
        index
            .add_path(std::path::Path::new("committed.txt"))
            .unwrap();
        index.write().unwrap();
        let tree_id = index.write_tree().unwrap();
        let tree = source_repo.find_tree(tree_id).unwrap();
        let parent = source_repo.head().unwrap().peel_to_commit().unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();
        source_repo
            .commit(
                Some("HEAD"),
                &sig,
                &sig,
                "Add committed file",
                &tree,
                &[&parent],
            )
            .unwrap();

        let base_sha = source_repo.head().unwrap().target().unwrap().to_string();

        // Add an uncommitted file (dirty state)
        fs::write(repo_path.join("dirty.txt"), "dirty WIP").unwrap();

        let ws = workspace_for_repo(&repo_path);
        store.insert_workspace(&ws).await.unwrap();

        let agent_id = AgentId::new();
        let agent = intent_core::AgentSession {
            harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
            harness_features: None,
            id: agent_id.clone(),
            workspace_id: ws.id.clone(),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: "Test Agent".to_string(),
            name_explicitly_set: false,
            model: None,
            reasoning_effort: None,
            effort_levels: None,
            provider: None,
            system_prompt: None,
            specialist: None,
            status: intent_core::AgentStatus::Active,
            is_active: true,
            messages: vec![],
            stats: None,
            task_note_id: None,
            skip_auto_commit: false,
            completion_report: None,
            completion_report_timestamp: None,
            attention_request_kind: None,
            attention_request_reason: None,
            attention_request_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            file_blocks: None,
            is_background: false,
            metadata: None,
            created_at: now_iso(),
            updated_at: now_iso(),
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
            pending_delete_at: None,
        };
        store.insert_agent_session(&agent).await.unwrap();

        fs::create_dir_all(&workspaces_root).unwrap();
        let config = ProvisionConfig {
            workspaces_root: workspaces_root.clone(),
        };

        let outcome = provision_sandbox(&store, &ws.id, &agent_id, &config)
            .await
            .unwrap();

        // We probed upfront, so this MUST be Supported
        let ProvisionOutcome::Supported {
            path,
            branch,
            base_commit_sha,
            snapshot_commit_sha,
        } = outcome
        else {
            panic!("Expected Supported after probe confirmed CoW available");
        };

        // Verify snapshot commit was created
        let snapshot_sha = snapshot_commit_sha.expect("Snapshot commit must exist for dirty state");

        // Verify base SHA matches source HEAD
        assert_eq!(base_commit_sha, base_sha, "Base SHA must match source HEAD");

        // Verify the sandbox contains the dirty file
        assert!(path.join("dirty.txt").exists());
        let content = fs::read_to_string(path.join("dirty.txt")).unwrap();
        assert_eq!(content, "dirty WIP", "Sandbox must contain the dirty WIP");

        // Open sandbox repo and verify snapshot commit exists on the sandbox branch
        let sandbox_repo = git2::Repository::open(&path).unwrap();
        let snapshot_commit = sandbox_repo
            .find_commit(git2::Oid::from_str(&snapshot_sha).unwrap())
            .unwrap();
        assert_eq!(
            snapshot_commit.message().unwrap(),
            format!("WIP snapshot for {}", agent_id.0),
            "Snapshot commit message must match"
        );

        // Verify snapshot commit is on the sandbox branch
        let sb_branch_ref = sandbox_repo
            .find_branch(&branch, git2::BranchType::Local)
            .unwrap();
        let sb_head = sb_branch_ref.get().target().unwrap().to_string();
        assert_eq!(
            sb_head, snapshot_sha,
            "Sandbox branch must point to snapshot commit"
        );

        // Verify sandbox record has correct SHAs
        let sandbox = store.get_sandbox(&ws.id, &agent_id).await.unwrap().unwrap();
        assert_eq!(sandbox.base_commit_sha, base_sha);
        assert_eq!(sandbox.snapshot_commit_sha, Some(snapshot_sha));

        // Verify the source repo still has dirty state (unchanged)
        assert!(repo_path.join("dirty.txt").exists());
        let statuses = source_repo.statuses(None).unwrap();
        assert!(
            !statuses.is_empty(),
            "Source repo must still have dirty state"
        );

        // Clean up
        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn discard_sandbox_removes_directory_and_record() {
        let (store, _db) = temp_store().await;
        let (test_root, repo_path) = temp_repo_in_target("discard-test");

        // Use same test_root for workspaces to ensure same volume
        let workspaces_root = test_root.join("workspaces");

        // Early probe check - skip test if CoW not available
        fs::create_dir_all(&workspaces_root).unwrap();
        let probe = cow_probe(&repo_path, &workspaces_root).unwrap();
        if probe == CowSupport::Unsupported {
            eprintln!(
                "Skipping test: CoW not supported between {repo_path:?} and {workspaces_root:?}"
            );
            let _ = fs::remove_dir_all(&test_root);
            return;
        }

        let ws = workspace_for_repo(&repo_path);
        store.insert_workspace(&ws).await.unwrap();

        let agent_id = AgentId::new();
        let agent = intent_core::AgentSession {
            harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
            harness_features: None,
            id: agent_id.clone(),
            workspace_id: ws.id.clone(),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: "Test Agent".to_string(),
            name_explicitly_set: false,
            model: None,
            reasoning_effort: None,
            effort_levels: None,
            provider: None,
            system_prompt: None,
            specialist: None,
            status: intent_core::AgentStatus::Active,
            is_active: true,
            messages: vec![],
            stats: None,
            task_note_id: None,
            skip_auto_commit: false,
            completion_report: None,
            completion_report_timestamp: None,
            attention_request_kind: None,
            attention_request_reason: None,
            attention_request_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            file_blocks: None,
            is_background: false,
            metadata: None,
            created_at: now_iso(),
            updated_at: now_iso(),
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
            pending_delete_at: None,
        };
        store.insert_agent_session(&agent).await.unwrap();

        fs::create_dir_all(&workspaces_root).unwrap();
        let config = ProvisionConfig {
            workspaces_root: workspaces_root.clone(),
        };

        let outcome = provision_sandbox(&store, &ws.id, &agent_id, &config)
            .await
            .unwrap();

        // We probed upfront, so this MUST be Supported
        let ProvisionOutcome::Supported { path, .. } = outcome else {
            panic!("Expected Supported after probe confirmed CoW available");
        };

        // Verify sandbox was created
        assert!(path.exists(), "Sandbox directory must exist before discard");
        assert!(
            path.join(".git").exists(),
            "Sandbox .git directory must exist"
        );

        // Verify sandbox record exists
        let sandbox_before = store.get_sandbox(&ws.id, &agent_id).await.unwrap();
        assert!(
            sandbox_before.is_some(),
            "Sandbox record must exist before discard"
        );

        // Discard the sandbox
        discard_sandbox(&store, &ws.id, &agent_id).await.unwrap();

        // Verify directory was COMPLETELY removed
        assert!(
            !path.exists(),
            "Sandbox directory must be completely removed after discard"
        );

        // Verify record was deleted from the database
        let sandbox_after = store.get_sandbox(&ws.id, &agent_id).await.unwrap();
        assert!(
            sandbox_after.is_none(),
            "Sandbox record must be deleted from DB after discard"
        );

        // Clean up
        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn gc_orphaned_sandboxes_removes_missing_agents() {
        let (store, _db) = temp_store().await;
        let workspaces_root = tempfile::TempDir::new().unwrap();

        // Create workspace first (FK requirement)
        let ws_id = WorkspaceId::new();
        let ws = workspace_for_repo(&PathBuf::from("/tmp/fake"));
        let mut ws_copy = ws.clone();
        ws_copy.id = ws_id.clone();
        store.insert_workspace(&ws_copy).await.unwrap();

        // Create agent temporarily to satisfy FK, then we'll delete it
        let agent_id = AgentId::new();
        let agent = intent_core::AgentSession {
            harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
            harness_features: None,
            id: agent_id.clone(),
            workspace_id: ws_id.clone(),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: "Test Agent".to_string(),
            name_explicitly_set: false,
            model: None,
            reasoning_effort: None,
            effort_levels: None,
            provider: None,
            system_prompt: None,
            specialist: None,
            status: intent_core::AgentStatus::Active,
            is_active: true,
            messages: vec![],
            stats: None,
            task_note_id: None,
            skip_auto_commit: false,
            completion_report: None,
            completion_report_timestamp: None,
            attention_request_kind: None,
            attention_request_reason: None,
            attention_request_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            file_blocks: None,
            is_background: false,
            metadata: None,
            created_at: now_iso(),
            updated_at: now_iso(),
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
            pending_delete_at: None,
        };
        store.insert_agent_session(&agent).await.unwrap();

        // Create a sandbox record
        let sandbox = intent_store::Sandbox {
            id: uuid::Uuid::new_v4().to_string(),
            workspace_id: ws_id.clone(),
            agent_id: agent_id.clone(),
            path: workspaces_root
                .path()
                .join("orphaned")
                .to_string_lossy()
                .to_string(),
            branch: "sb/test".to_string(),
            base_commit_sha: "abc123".to_string(),
            snapshot_commit_sha: None,
            status: intent_store::SandboxStatus::Created,
            retry_count: 0,
            created_at: now_iso(),
            updated_at: now_iso(),
        };
        store.insert_sandbox(&sandbox).await.unwrap();

        // Now delete the agent session to make the sandbox orphaned
        store.delete_agent_session(&ws_id, &agent_id).await.unwrap();

        // Run GC
        gc_orphaned_sandboxes(&store).await.unwrap();

        // Verify the orphaned sandbox was removed
        let result = store.get_sandbox(&ws_id, &agent_id).await.unwrap();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn provision_returns_unsupported_when_cow_unavailable() {
        let (store, _db) = temp_store().await;
        let (_repo_dir_guard, repo_path) = temp_repo("no-cow");

        let ws = workspace_for_repo(&repo_path);
        store.insert_workspace(&ws).await.unwrap();

        let agent_id = AgentId::new();
        let agent = intent_core::AgentSession {
            harness_version: intent_core::CURRENT_HARNESS_VERSION.to_string(),
            harness_features: None,
            id: agent_id.clone(),
            workspace_id: ws.id.clone(),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: "Test Agent".to_string(),
            name_explicitly_set: false,
            model: None,
            reasoning_effort: None,
            effort_levels: None,
            provider: None,
            system_prompt: None,
            specialist: None,
            status: intent_core::AgentStatus::Active,
            is_active: true,
            messages: vec![],
            stats: None,
            task_note_id: None,
            skip_auto_commit: false,
            completion_report: None,
            completion_report_timestamp: None,
            attention_request_kind: None,
            attention_request_reason: None,
            attention_request_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            file_blocks: None,
            is_background: false,
            metadata: None,
            created_at: now_iso(),
            updated_at: now_iso(),
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
            stop_reason: None,
            stop_reason_timestamp: None,
            session_corrupted: false,
            pending_delete_at: None,
        };
        store.insert_agent_session(&agent).await.unwrap();

        // Try to use a cross-volume destination
        // On most systems, /tmp is a different volume, but on this APFS machine it's the same
        let cross_volume_root = std::env::temp_dir().join(format!(
            "intentd-cross-volume-test-{}",
            uuid::Uuid::new_v4()
        ));
        fs::create_dir_all(&cross_volume_root).unwrap();

        // Probe first to check if we actually have a cross-volume scenario
        let probe = cow_probe(&repo_path, &cross_volume_root).unwrap();
        if probe == CowSupport::Supported {
            eprintln!("Skipping test: test environment has no cross-volume paths available");
            let _ = fs::remove_dir_all(&cross_volume_root);
            return;
        }

        let config = ProvisionConfig {
            workspaces_root: cross_volume_root.clone(),
        };

        let outcome = provision_sandbox(&store, &ws.id, &agent_id, &config)
            .await
            .unwrap();

        // When CoW is not supported, we return Unsupported and copy ZERO bytes
        let ProvisionOutcome::Unsupported = outcome else {
            panic!("Expected Unsupported outcome for cross-volume CoW attempt");
        };

        // Verify no sandbox directory was created
        let would_be_path = config.workspaces_root.join(&ws.id.0).join(&agent_id.0);
        assert!(
            !would_be_path.exists(),
            "No sandbox directory should exist when CoW is unsupported"
        );

        // Verify no sandbox record was created in the database
        let sandbox = store.get_sandbox(&ws.id, &agent_id).await.unwrap();
        assert!(
            sandbox.is_none(),
            "No sandbox record should exist when CoW is unsupported"
        );

        // Clean up
        let _ = fs::remove_dir_all(&cross_volume_root);
    }

    #[tokio::test]
    async fn provision_returns_unsupported_for_gitfile_canonical_dir() {
        // A linked-worktree canonical dir (gitfile .git) must degrade to
        // shared mode BEFORE any probe/clone: CoW-cloning it would corrupt
        // the user's source checkout.
        let (store, _db) = temp_store().await;
        let (test_root, repo_path) = temp_repo_in_target("gitfile-src");

        // Give the repo a real worktree with a commit, then link a worktree.
        fs::write(repo_path.join("a.txt"), "x").unwrap();
        {
            let repo = git2::Repository::open(&repo_path).unwrap();
            let mut index = repo.index().unwrap();
            index.add_path(Path::new("a.txt")).unwrap();
            index.write().unwrap();
            let tree = repo.find_tree(index.write_tree().unwrap()).unwrap();
            let sig = git2::Signature::now("Test", "test@example.com").unwrap();
            let parent = repo.head().unwrap().peel_to_commit().unwrap();
            repo.commit(Some("HEAD"), &sig, &sig, "add a", &tree, &[&parent])
                .unwrap();
        }
        let wt_path = test_root.join("linked-wt");
        intent_git::worktree::provision_worktree(
            &repo_path,
            "linked-wt",
            &wt_path,
            "wt-branch",
            None,
            "origin",
        )
        .unwrap();
        assert!(wt_path.join(".git").is_file(), "worktree .git is a gitfile");

        // The workspace's canonical dir is the linked worktree.
        let ws = workspace_for_repo(&wt_path);
        store.insert_workspace(&ws).await.unwrap();
        let agent_id = AgentId::new();
        create_test_agent(&store, &ws.id, &agent_id).await;

        let config = ProvisionConfig {
            workspaces_root: test_root.join("workspaces"),
        };
        let outcome = provision_sandbox(&store, &ws.id, &agent_id, &config)
            .await
            .unwrap();
        let ProvisionOutcome::Unsupported = outcome else {
            panic!("Expected Unsupported outcome for a gitfile canonical dir");
        };
        // The user's worktree is untouched.
        assert_eq!(fs::read_to_string(wt_path.join("a.txt")).unwrap(), "x");
        let sandbox = store.get_sandbox(&ws.id, &agent_id).await.unwrap();
        assert!(sandbox.is_none());

        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn provision_returns_unsupported_when_clone_fails_midflight() {
        // Probe passes but the clone itself fails as unsupported (forced via
        // the test seam): provision must degrade to shared mode, not error.
        let (store, _db) = temp_store().await;
        let unique = format!("midflight-{}", uuid::Uuid::new_v4());
        let (test_root, repo_path) = temp_repo_in_target(&unique);
        let workspaces_root = test_root.join("workspaces");

        fs::create_dir_all(&workspaces_root).unwrap();
        let probe = cow_probe(&repo_path, &workspaces_root).unwrap();
        if probe == CowSupport::Unsupported {
            eprintln!("Skipping test: CoW not supported");
            let _ = fs::remove_dir_all(&test_root);
            return;
        }

        let ws = workspace_for_repo(&repo_path);
        store.insert_workspace(&ws).await.unwrap();
        let agent_id = AgentId::new();
        create_test_agent(&store, &ws.id, &agent_id).await;

        // Needle is the test-unique repo dir name, so parallel tests cloning
        // other paths are unaffected.
        std::env::set_var(intent_git::TEST_COW_CLONE_UNSUPPORTED_PATH_ENV, &unique);
        let config = ProvisionConfig {
            workspaces_root: workspaces_root.clone(),
        };
        let outcome = provision_sandbox(&store, &ws.id, &agent_id, &config).await;
        std::env::remove_var(intent_git::TEST_COW_CLONE_UNSUPPORTED_PATH_ENV);

        let ProvisionOutcome::Unsupported = outcome.unwrap() else {
            panic!("Expected Unsupported outcome for a mid-flight unsupported clone");
        };
        let sandbox = store.get_sandbox(&ws.id, &agent_id).await.unwrap();
        assert!(sandbox.is_none());

        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn provision_cleans_up_clone_when_record_insert_fails() {
        // agent.delete can race the background clone (monorepo#871): with no
        // agent_session row the sandbox record insert fails its FK
        // (ON DELETE CASCADE reference), and the just-cloned directory must
        // not be stranded on disk.
        let (store, _db) = temp_store().await;
        let (test_root, repo_path) = temp_repo_in_target("insert-fk-race");
        let workspaces_root = test_root.join("workspaces");

        fs::create_dir_all(&workspaces_root).unwrap();
        let probe = cow_probe(&repo_path, &workspaces_root).unwrap();
        if probe == CowSupport::Unsupported {
            eprintln!("Skipping test: CoW not supported");
            let _ = fs::remove_dir_all(&test_root);
            return;
        }

        let ws = workspace_for_repo(&repo_path);
        store.insert_workspace(&ws).await.unwrap();
        // No agent session inserted: mirrors a hard agent.delete completing
        // while the clone ran.
        let agent_id = AgentId::new();

        let config = ProvisionConfig {
            workspaces_root: workspaces_root.clone(),
        };
        let err = provision_sandbox(&store, &ws.id, &agent_id, &config)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("insert sandbox failed"),
            "expected FK insert failure, got: {err}"
        );

        // The cloned sandbox directory must have been removed.
        let sandbox_parent = workspaces_root
            .join(&ws.id.0)
            .join("sandboxes")
            .join(&agent_id.0);
        let leftover: Vec<_> = fs::read_dir(&sandbox_parent)
            .map(|entries| entries.flatten().collect())
            .unwrap_or_default();
        assert!(
            leftover.is_empty(),
            "cloned sandbox directory must be cleaned up on insert failure: {leftover:?}"
        );

        let _ = fs::remove_dir_all(&test_root);
    }

    #[test]
    fn restore_missing_tracked_files_restores_only_missing_paths() {
        // A tracked file missing from the worktree (as if skipped by the
        // best-effort clone) is restored from the index; dirty tracked files
        // and untracked files are untouched.
        let (test_root, repo_path) = temp_repo_in_target("restore-missing");
        let repo = git2::Repository::open(&repo_path).unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();
        fs::write(repo_path.join("kept.txt"), "kept").unwrap();
        fs::write(repo_path.join("skipped.txt"), "skipped").unwrap();
        let mut index = repo.index().unwrap();
        index.add_path(Path::new("kept.txt")).unwrap();
        index.add_path(Path::new("skipped.txt")).unwrap();
        index.write().unwrap();
        let tree = repo.find_tree(index.write_tree().unwrap()).unwrap();
        let parent = repo.head().unwrap().peel_to_commit().unwrap();
        repo.commit(Some("HEAD"), &sig, &sig, "add files", &tree, &[&parent])
            .unwrap();

        // Simulate the walk skipping one tracked file, plus genuine dirt.
        fs::remove_file(repo_path.join("skipped.txt")).unwrap();
        fs::write(repo_path.join("kept.txt"), "dirty edit").unwrap();
        fs::write(repo_path.join("untracked.txt"), "new").unwrap();

        restore_missing_tracked_files(&repo, &repo_path).unwrap();

        assert_eq!(
            fs::read_to_string(repo_path.join("skipped.txt")).unwrap(),
            "skipped",
            "missing tracked file is restored from the index"
        );
        assert_eq!(
            fs::read_to_string(repo_path.join("kept.txt")).unwrap(),
            "dirty edit",
            "dirty tracked file is untouched"
        );
        assert_eq!(
            fs::read_to_string(repo_path.join("untracked.txt")).unwrap(),
            "new",
            "untracked file is untouched"
        );

        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn test_sandbox_merge_clean() {
        use git2::Repository;
        use std::fs;

        let (store, _db) = temp_store().await;
        let (test_root, repo_path) = temp_repo_in_target("merge-clean");
        let workspaces_root = test_root.join("workspaces");

        fs::create_dir_all(&workspaces_root).unwrap();
        let probe = cow_probe(&repo_path, &workspaces_root).unwrap();
        if probe == CowSupport::Unsupported {
            eprintln!("Skipping test: CoW not supported on this filesystem");
            let _ = fs::remove_dir_all(&test_root);
            return;
        }

        let ws = workspace_for_repo(&repo_path);
        store.insert_workspace(&ws).await.unwrap();
        let agent_id = AgentId::from("agent-test-merge");
        create_test_agent(&store, &ws.id, &agent_id).await;

        // Canonical repo already initialized by temp_repo_in_target
        let canonical_path = repo_path.clone();
        let repo = Repository::open(&canonical_path).unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();

        // Get base commit
        let base_commit = repo.head().unwrap().peel_to_commit().unwrap().id();

        // Provision sandbox
        let config = ProvisionConfig {
            workspaces_root: workspaces_root.clone(),
        };
        let outcome = provision_sandbox(&store, &ws.id, &agent_id, &config)
            .await
            .unwrap();
        let ProvisionOutcome::Supported {
            path: sandbox_path, ..
        } = outcome
        else {
            panic!("Expected Supported outcome");
        };

        // Make a commit in the sandbox
        let sandbox_repo = Repository::open(&sandbox_path).unwrap();
        fs::write(PathBuf::from(&sandbox_path).join("file2.txt"), "content2").unwrap();
        let mut sandbox_index = sandbox_repo.index().unwrap();
        sandbox_index.add_path(Path::new("file2.txt")).unwrap();
        sandbox_index.write().unwrap();
        let sandbox_tree_oid = sandbox_index.write_tree().unwrap();
        let sandbox_tree = sandbox_repo.find_tree(sandbox_tree_oid).unwrap();
        let parent = sandbox_repo.find_commit(base_commit).unwrap();
        sandbox_repo
            .commit(
                Some("HEAD"),
                &sig,
                &sig,
                "Sandbox work",
                &sandbox_tree,
                &[&parent],
            )
            .unwrap();

        // Attempt merge
        let outcome = merge_sandbox(&store, &ws.id, &agent_id).await.unwrap();

        // Verify clean merge
        match outcome {
            MergeOutcome::Merged { commit_range, .. } => {
                assert!(!commit_range.is_empty());
                // Verify file2.txt is in canonical
                assert!(canonical_path.join("file2.txt").exists());
            }
            _ => panic!("Expected Merged outcome, got {outcome:?}"),
        }

        // Clean up
        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn test_sandbox_merge_dirty_overlap_blocked() {
        let (store, _db) = temp_store().await;
        let (test_root, canonical_path) = temp_repo_in_target("canonical");

        // Create workspace
        let ws = workspace_for_repo(&canonical_path);
        store.insert_workspace(&ws).await.unwrap();

        let agent_id = AgentId(uuid::Uuid::new_v4().to_string());
        create_test_agent(&store, &ws.id, &agent_id).await;

        // Clone canonical to sandbox (so they share commit history)
        let sandbox_path = test_root.join("sandbox");
        git2::Repository::clone(canonical_path.to_str().unwrap(), &sandbox_path).unwrap();

        let base_sha = {
            let canonical_repo = git2::Repository::open(&canonical_path).unwrap();
            let head_ref = canonical_repo.head().unwrap();
            let oid = head_ref.target().unwrap();
            oid.to_string()
        };

        // Make a change in sandbox
        let sandbox_file = sandbox_path.join("file1.txt");
        fs::write(&sandbox_file, "sandbox change").unwrap();
        {
            let sandbox_repo = git2::Repository::open(&sandbox_path).unwrap();
            let mut index = sandbox_repo.index().unwrap();
            index.add_path(Path::new("file1.txt")).unwrap();
            index.write().unwrap();
            let tree_oid = index.write_tree().unwrap();
            let tree = sandbox_repo.find_tree(tree_oid).unwrap();
            let sig = git2::Signature::now("Test", "test@example.com").unwrap();
            let parent = sandbox_repo.head().unwrap().peel_to_commit().unwrap();
            sandbox_repo
                .commit(Some("HEAD"), &sig, &sig, "Sandbox work", &tree, &[&parent])
                .unwrap();
        }

        // Make a DIFFERENT change in canonical (but to the SAME file) and leave it uncommitted
        let canonical_file = canonical_path.join("file1.txt");
        fs::write(&canonical_file, "canonical dirty change").unwrap();

        // Create sandbox record
        let sandbox = Sandbox {
            id: uuid::Uuid::new_v4().to_string(),
            workspace_id: ws.id.clone(),
            agent_id: agent_id.clone(),
            path: sandbox_path.to_string_lossy().to_string(),
            branch: "sb/test".to_string(),
            base_commit_sha: base_sha,
            snapshot_commit_sha: None,
            status: SandboxStatus::Created,
            retry_count: 0,
            created_at: now_iso(),
            updated_at: now_iso(),
        };
        store.insert_sandbox(&sandbox).await.unwrap();

        // Attempt merge - should be Blocked due to dirty overlap
        let outcome = merge_sandbox(&store, &ws.id, &agent_id).await.unwrap();
        match outcome {
            MergeOutcome::Blocked {
                reason,
                overlapping_paths,
            } => {
                assert!(reason.contains("uncommitted"));
                assert_eq!(overlapping_paths.len(), 1);
                assert_eq!(overlapping_paths[0], "file1.txt");
            }
            _ => panic!("Expected Blocked outcome, got {outcome:?}"),
        }

        // Cleanup
        let _ = fs::remove_dir_all(test_root);
    }

    #[tokio::test]
    async fn test_sandbox_merge_conflict() {
        use git2::Repository;
        use std::fs;

        let (store, _db) = temp_store().await;
        let (test_root, repo_path) = temp_repo_in_target("merge-conflict");
        let workspaces_root = test_root.join("workspaces");

        fs::create_dir_all(&workspaces_root).unwrap();
        let probe = cow_probe(&repo_path, &workspaces_root).unwrap();
        if probe == CowSupport::Unsupported {
            eprintln!("Skipping test: CoW not supported");
            let _ = fs::remove_dir_all(&test_root);
            return;
        }

        let ws = workspace_for_repo(&repo_path);
        store.insert_workspace(&ws).await.unwrap();
        let agent_id = AgentId::from("agent-test-conflict");
        create_test_agent(&store, &ws.id, &agent_id).await;
        let canonical_path = repo_path.clone();
        let repo = Repository::open(&canonical_path).unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();

        // Add a file to work with
        fs::write(canonical_path.join("file.txt"), "line1\n").unwrap();
        let mut index = repo.index().unwrap();
        index.add_path(Path::new("file.txt")).unwrap();
        index.write().unwrap();
        let tree_oid = index.write_tree().unwrap();
        let tree = repo.find_tree(tree_oid).unwrap();
        let head = repo.head().unwrap().peel_to_commit().unwrap();
        let base_commit = repo
            .commit(Some("HEAD"), &sig, &sig, "Add file", &tree, &[&head])
            .unwrap();

        // Provision sandbox
        let config = ProvisionConfig {
            workspaces_root: workspaces_root.clone(),
        };
        let outcome = provision_sandbox(&store, &ws.id, &agent_id, &config)
            .await
            .unwrap();
        let ProvisionOutcome::Supported {
            path: sandbox_path, ..
        } = outcome
        else {
            panic!("Expected Supported outcome");
        };

        // Modify same file in sandbox
        let sandbox_repo = Repository::open(&sandbox_path).unwrap();
        fs::write(
            PathBuf::from(&sandbox_path).join("file.txt"),
            "line1\nsandbox change\n",
        )
        .unwrap();
        let mut sandbox_index = sandbox_repo.index().unwrap();
        sandbox_index.add_path(Path::new("file.txt")).unwrap();
        sandbox_index.write().unwrap();
        let sandbox_tree_oid = sandbox_index.write_tree().unwrap();
        let sandbox_tree = sandbox_repo.find_tree(sandbox_tree_oid).unwrap();
        let parent = sandbox_repo.find_commit(base_commit).unwrap();
        sandbox_repo
            .commit(
                Some("HEAD"),
                &sig,
                &sig,
                "Sandbox change",
                &sandbox_tree,
                &[&parent],
            )
            .unwrap();

        // Modify same file in canonical to create conflict
        fs::write(canonical_path.join("file.txt"), "line1\ncanonical change\n").unwrap();
        let mut canonical_index = repo.index().unwrap();
        canonical_index.add_path(Path::new("file.txt")).unwrap();
        canonical_index.write().unwrap();
        let canonical_tree_oid = canonical_index.write_tree().unwrap();
        let canonical_tree = repo.find_tree(canonical_tree_oid).unwrap();
        let canonical_parent = repo.find_commit(base_commit).unwrap();
        repo.commit(
            Some("HEAD"),
            &sig,
            &sig,
            "Canonical change",
            &canonical_tree,
            &[&canonical_parent],
        )
        .unwrap();

        // Attempt merge - should detect conflict
        let outcome = merge_sandbox(&store, &ws.id, &agent_id).await.unwrap();

        // Verify conflict detected
        match outcome {
            MergeOutcome::Conflict {
                conflicting_paths, ..
            } => {
                assert!(conflicting_paths.contains(&"file.txt".to_string()));
                // Verify canonical is pristine (not mid-merge)
                assert!(repo.state() == git2::RepositoryState::Clean);
            }
            _ => panic!("Expected Conflict outcome, got {outcome:?}"),
        }

        // Clean up
        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn test_snapshot_excluded_from_merge() {
        // This test verifies that WIP snapshot commits are NOT merged back to canonical
        use git2::Repository;
        use std::fs;

        let (store, _db) = temp_store().await;
        let (test_root, repo_path) = temp_repo_in_target("snapshot-exclude");
        let workspaces_root = test_root.join("workspaces");

        fs::create_dir_all(&workspaces_root).unwrap();
        let probe = cow_probe(&repo_path, &workspaces_root).unwrap();
        if probe == CowSupport::Unsupported {
            eprintln!("Skipping test: CoW not supported");
            let _ = fs::remove_dir_all(&test_root);
            return;
        }

        let ws = workspace_for_repo(&repo_path);
        store.insert_workspace(&ws).await.unwrap();
        let agent_id = AgentId::from("agent-test-snapshot");
        create_test_agent(&store, &ws.id, &agent_id).await;
        let canonical_path = repo_path.clone();
        let _repo = Repository::open(&canonical_path).unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();

        // Add dirty file to canonical before provisioning
        fs::write(canonical_path.join("wip.txt"), "user wip").unwrap();

        let config = ProvisionConfig {
            workspaces_root: workspaces_root.clone(),
        };
        let outcome = provision_sandbox(&store, &ws.id, &agent_id, &config)
            .await
            .unwrap();
        let ProvisionOutcome::Supported {
            path: sandbox_path,
            snapshot_commit_sha,
            ..
        } = outcome
        else {
            panic!("Expected Supported outcome");
        };

        // Verify snapshot was created
        assert!(
            snapshot_commit_sha.is_some(),
            "Snapshot should be created for WIP"
        );

        // Make agent commit in sandbox
        let sandbox_repo = Repository::open(&sandbox_path).unwrap();
        fs::write(sandbox_path.join("agent_work.txt"), "agent work").unwrap();
        let mut sandbox_index = sandbox_repo.index().unwrap();
        sandbox_index.add_path(Path::new("agent_work.txt")).unwrap();
        sandbox_index.write().unwrap();
        let sandbox_tree_oid = sandbox_index.write_tree().unwrap();
        let sandbox_tree = sandbox_repo.find_tree(sandbox_tree_oid).unwrap();
        let head = sandbox_repo.head().unwrap().peel_to_commit().unwrap();
        sandbox_repo
            .commit(
                Some("HEAD"),
                &sig,
                &sig,
                "Agent work",
                &sandbox_tree,
                &[&head],
            )
            .unwrap();

        // Clean canonical WIP so merge can proceed
        fs::remove_file(canonical_path.join("wip.txt")).unwrap();

        // Attempt merge
        let outcome = merge_sandbox(&store, &ws.id, &agent_id).await.unwrap();

        // Verify merge succeeded and WIP snapshot was excluded
        match outcome {
            MergeOutcome::Merged { .. } => {
                // Verify agent work landed
                assert!(canonical_path.join("agent_work.txt").exists());
                // Verify WIP snapshot did NOT land (critical!)
                assert!(
                    !canonical_path.join("wip.txt").exists(),
                    "WIP snapshot must not be merged"
                );
            }
            _ => panic!("Expected Merged outcome, got {outcome:?}"),
        }

        // Clean up
        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn test_sandbox_merge_preserves_attribution() {
        // This test verifies that agent commits preserve the agent's identity in canonical
        use git2::Repository;
        use std::fs;

        let (store, _db) = temp_store().await;
        let (test_root, repo_path) = temp_repo_in_target("attribution");
        let workspaces_root = test_root.join("workspaces");

        fs::create_dir_all(&workspaces_root).unwrap();
        let probe = cow_probe(&repo_path, &workspaces_root).unwrap();
        if probe == CowSupport::Unsupported {
            eprintln!("Skipping test: CoW not supported");
            let _ = fs::remove_dir_all(&test_root);
            return;
        }

        // Create workspace
        let ws = workspace_for_repo(&repo_path);
        store.insert_workspace(&ws).await.unwrap();

        let agent_id = AgentId("agent-test-123".to_string());
        create_test_agent(&store, &ws.id, &agent_id).await;
        let config = ProvisionConfig {
            workspaces_root: workspaces_root.clone(),
        };
        let provision_outcome = provision_sandbox(&store, &ws.id, &agent_id, &config)
            .await
            .unwrap();
        let sandbox_path = match provision_outcome {
            ProvisionOutcome::Supported { path, .. } => path,
            _ => panic!("Expected Supported"),
        };

        // In the sandbox, make a commit with a specific author (simulating the agent)
        let agent_work = sandbox_path.join("agent_work.txt");
        fs::write(&agent_work, "work by agent").unwrap();
        let sandbox_repo = Repository::open(&sandbox_path).unwrap();
        let mut index = sandbox_repo.index().unwrap();
        index.add_path(Path::new("agent_work.txt")).unwrap();
        index.write().unwrap();
        let tree_oid = index.write_tree().unwrap();
        let tree = sandbox_repo.find_tree(tree_oid).unwrap();
        let parent = sandbox_repo.head().unwrap().peel_to_commit().unwrap();

        // Use a custom signature for the agent
        let agent_sig = git2::Signature::now("Agent Bot", "agent@example.com").unwrap();
        let _commit_oid = sandbox_repo
            .commit(
                Some("HEAD"),
                &agent_sig,
                &agent_sig,
                "Agent's work",
                &tree,
                &[&parent],
            )
            .unwrap();

        // Merge back
        let outcome = merge_sandbox(&store, &ws.id, &agent_id).await.unwrap();
        match outcome {
            MergeOutcome::Merged { .. } => {}
            _ => panic!("Expected Merged outcome, got {outcome:?}"),
        }

        // Verify attribution was preserved in canonical
        let canonical_repo = Repository::open(&repo_path).unwrap();
        let head_commit = canonical_repo.head().unwrap().peel_to_commit().unwrap();

        // The latest commit should have the agent's signature
        assert_eq!(head_commit.author().name().unwrap(), "Agent Bot");
        assert_eq!(head_commit.author().email().unwrap(), "agent@example.com");
        assert_eq!(head_commit.message().unwrap(), "Agent's work");

        // Clean up
        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn test_sandbox_retry_tracking() {
        // This test verifies that retry count get/increment/clear work correctly
        let (store, _db) = temp_store().await;
        let (test_root, canonical_path) = temp_repo_in_target("retry-track");

        // Create workspace
        let ws = workspace_for_repo(&canonical_path);
        store.insert_workspace(&ws).await.unwrap();

        let agent_id = AgentId(uuid::Uuid::new_v4().to_string());
        create_test_agent(&store, &ws.id, &agent_id).await;

        // Create a minimal sandbox record (doesn't need real repo)
        let sandbox = Sandbox {
            id: uuid::Uuid::new_v4().to_string(),
            workspace_id: ws.id.clone(),
            agent_id: agent_id.clone(),
            path: "/tmp/fake".to_string(),
            branch: "sb/test".to_string(),
            base_commit_sha: "abc123".to_string(),
            snapshot_commit_sha: None,
            status: SandboxStatus::Created,
            retry_count: 0,
            created_at: now_iso(),
            updated_at: now_iso(),
        };
        store.insert_sandbox(&sandbox).await.unwrap();

        // Initial retry count should be 0
        let retry_count = store
            .get_sandbox_retry_count(&ws.id, &agent_id)
            .await
            .unwrap();
        assert_eq!(retry_count, 0, "Initial retry count should be 0");

        // Increment once
        store
            .increment_sandbox_retry_count(&ws.id, &agent_id)
            .await
            .unwrap();
        let retry_count = store
            .get_sandbox_retry_count(&ws.id, &agent_id)
            .await
            .unwrap();
        assert_eq!(
            retry_count, 1,
            "Retry count should be 1 after first increment"
        );

        // Increment again
        store
            .increment_sandbox_retry_count(&ws.id, &agent_id)
            .await
            .unwrap();
        let retry_count = store
            .get_sandbox_retry_count(&ws.id, &agent_id)
            .await
            .unwrap();
        assert_eq!(
            retry_count, 2,
            "Retry count should be 2 after second increment"
        );

        // Clear retry count
        store
            .clear_sandbox_retry_count(&ws.id, &agent_id)
            .await
            .unwrap();
        let retry_count = store
            .get_sandbox_retry_count(&ws.id, &agent_id)
            .await
            .unwrap();
        assert_eq!(retry_count, 0, "Retry count should be 0 after clear");

        // Clean up
        let _ = fs::remove_dir_all(&test_root);
    }

    /// Build a CoW-checkout workspace: the source repo is `repository_path`,
    /// the workspace checkout is `worktree_path`, `checkout_mode = cow`.
    fn cow_workspace(source_repo: &Path, checkout: &Path) -> Workspace {
        let mut ws = workspace_for_repo(source_repo);
        ws.skip_worktree = false;
        ws.worktree_path = Some(checkout.to_string_lossy().to_string());
        ws.checkout_mode = Some(CheckoutMode::Cow);
        ws
    }

    /// Stage and commit a single file in the repo at `repo_path`; returns the commit SHA.
    fn commit_file(repo_path: &Path, file: &str, content: &str, message: &str) -> String {
        let repo = git2::Repository::open(repo_path).unwrap();
        fs::write(repo_path.join(file), content).unwrap();
        let mut index = repo.index().unwrap();
        index.add_path(Path::new(file)).unwrap();
        index.write().unwrap();
        let tree_oid = index.write_tree().unwrap();
        let tree = repo.find_tree(tree_oid).unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();
        let parent = repo.head().unwrap().peel_to_commit().unwrap();
        repo.commit(Some("HEAD"), &sig, &sig, message, &tree, &[&parent])
            .unwrap()
            .to_string()
    }

    #[tokio::test]
    async fn cow_checkout_provision_sources_from_workspace_checkout() {
        let (store, _db) = temp_store().await;
        let (test_root, source_path) = temp_repo_in_target("cow-provision-src");
        let workspaces_root = test_root.join("workspaces");

        fs::create_dir_all(&workspaces_root).unwrap();
        let probe = cow_probe(&source_path, &workspaces_root).unwrap();
        if probe == CowSupport::Unsupported {
            eprintln!("Skipping test: CoW not supported");
            let _ = fs::remove_dir_all(&test_root);
            return;
        }

        // Simulate a Task-1 CoW checkout: clone the source, then diverge it.
        let checkout_path = test_root.join("checkout");
        cow_clone(&source_path, &checkout_path).unwrap();
        let checkout_head = commit_file(
            &checkout_path,
            "checkout-only.txt",
            "only in checkout",
            "Checkout-only commit",
        );

        let ws = cow_workspace(&source_path, &checkout_path);
        store.insert_workspace(&ws).await.unwrap();
        let agent_id = AgentId::new();
        create_test_agent(&store, &ws.id, &agent_id).await;

        let source_head_before = {
            let repo = git2::Repository::open(&source_path).unwrap();
            let head_ref = repo.head().unwrap();
            let oid = head_ref.target().unwrap();
            oid.to_string()
        };

        let config = ProvisionConfig {
            workspaces_root: workspaces_root.clone(),
        };
        let outcome = provision_sandbox(&store, &ws.id, &agent_id, &config)
            .await
            .unwrap();
        let ProvisionOutcome::Supported {
            path,
            base_commit_sha,
            ..
        } = outcome
        else {
            panic!("Expected Supported after probe confirmed CoW available");
        };

        // Sandbox is cloned from the workspace checkout, not the source repo.
        assert_eq!(
            base_commit_sha, checkout_head,
            "sandbox base must be the checkout HEAD"
        );
        assert_ne!(
            base_commit_sha, source_head_before,
            "sandbox base must not be the source repo HEAD"
        );
        assert!(
            path.join("checkout-only.txt").exists(),
            "sandbox must contain the checkout-only file"
        );

        // Source repo untouched.
        let source_head_after = {
            let repo = git2::Repository::open(&source_path).unwrap();
            let head_ref = repo.head().unwrap();
            let oid = head_ref.target().unwrap();
            oid.to_string()
        };
        assert_eq!(source_head_before, source_head_after);

        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn cow_checkout_merge_targets_workspace_checkout() {
        let (store, _db) = temp_store().await;
        let (test_root, source_path) = temp_repo_in_target("cow-merge-src");
        let workspaces_root = test_root.join("workspaces");

        fs::create_dir_all(&workspaces_root).unwrap();
        let probe = cow_probe(&source_path, &workspaces_root).unwrap();
        if probe == CowSupport::Unsupported {
            eprintln!("Skipping test: CoW not supported");
            let _ = fs::remove_dir_all(&test_root);
            return;
        }

        let checkout_path = test_root.join("checkout");
        cow_clone(&source_path, &checkout_path).unwrap();

        let ws = cow_workspace(&source_path, &checkout_path);
        store.insert_workspace(&ws).await.unwrap();
        let agent_id = AgentId::new();
        create_test_agent(&store, &ws.id, &agent_id).await;

        let config = ProvisionConfig {
            workspaces_root: workspaces_root.clone(),
        };
        let outcome = provision_sandbox(&store, &ws.id, &agent_id, &config)
            .await
            .unwrap();
        let ProvisionOutcome::Supported {
            path: sandbox_path, ..
        } = outcome
        else {
            panic!("Expected Supported after probe confirmed CoW available");
        };

        // Agent commits in the sandbox (distinct author for attribution check).
        {
            let sandbox_repo = git2::Repository::open(&sandbox_path).unwrap();
            fs::write(sandbox_path.join("agent.txt"), "agent work").unwrap();
            let mut index = sandbox_repo.index().unwrap();
            index.add_path(Path::new("agent.txt")).unwrap();
            index.write().unwrap();
            let tree_oid = index.write_tree().unwrap();
            let tree = sandbox_repo.find_tree(tree_oid).unwrap();
            let sig = git2::Signature::now("Agent Author", "agent@example.com").unwrap();
            let parent = sandbox_repo.head().unwrap().peel_to_commit().unwrap();
            sandbox_repo
                .commit(Some("HEAD"), &sig, &sig, "Agent work", &tree, &[&parent])
                .unwrap();
        }

        let source_head_before = {
            let repo = git2::Repository::open(&source_path).unwrap();
            let head_ref = repo.head().unwrap();
            let oid = head_ref.target().unwrap();
            oid.to_string()
        };
        let checkout_head_before = {
            let repo = git2::Repository::open(&checkout_path).unwrap();
            let head_ref = repo.head().unwrap();
            let oid = head_ref.target().unwrap();
            oid.to_string()
        };

        let outcome = merge_sandbox(&store, &ws.id, &agent_id).await.unwrap();
        let MergeOutcome::Merged { canonical_head, .. } = outcome else {
            panic!("Expected Merged outcome, got {outcome:?}");
        };

        // Merge landed in the workspace checkout with attribution preserved.
        let checkout_repo = git2::Repository::open(&checkout_path).unwrap();
        let checkout_head_after = checkout_repo.head().unwrap().target().unwrap().to_string();
        assert_eq!(canonical_head, checkout_head_after);
        assert_ne!(checkout_head_before, checkout_head_after);
        assert!(checkout_path.join("agent.txt").exists());
        let head_commit = checkout_repo.head().unwrap().peel_to_commit().unwrap();
        let author = head_commit.author();
        assert_eq!(author.name().unwrap(), "Agent Author");
        assert_eq!(head_commit.message().unwrap(), "Agent work");

        // Source repo untouched.
        let source_repo = git2::Repository::open(&source_path).unwrap();
        let source_head_after = source_repo.head().unwrap().target().unwrap().to_string();
        assert_eq!(source_head_before, source_head_after);
        assert!(!source_path.join("agent.txt").exists());

        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn worktree_checkout_mode_rejects_sandbox_provisioning() {
        let (store, _db) = temp_store().await;
        let (test_root, repo_path) = temp_repo_in_target("worktree-reject");
        let workspaces_root = test_root.join("workspaces");
        fs::create_dir_all(&workspaces_root).unwrap();

        let mut ws = workspace_for_repo(&repo_path);
        ws.skip_worktree = false;
        ws.worktree_path = Some(repo_path.to_string_lossy().to_string());
        ws.checkout_mode = Some(CheckoutMode::Worktree);
        store.insert_workspace(&ws).await.unwrap();
        let agent_id = AgentId::new();
        create_test_agent(&store, &ws.id, &agent_id).await;

        let config = ProvisionConfig {
            workspaces_root: workspaces_root.clone(),
        };
        let err = provision_sandbox(&store, &ws.id, &agent_id, &config)
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("worktree-mode"),
            "expected worktree-mode rejection, got: {err}"
        );

        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn test_sandbox_merge_with_non_commit_advertised_refs() {
        // Regression: CoW clones inherit refs/intent/blobs/* (blob objects) and
        // refs/stash from the source repo. The merge-back fetch must not fail
        // with `object is not a committish; code=InvalidSpec (-12)` when the
        // sandbox advertises such non-commit refs.
        let (store, _db) = temp_store().await;
        let (test_root, canonical_path) = temp_repo_in_target("noncommit-refs");

        let ws = workspace_for_repo(&canonical_path);
        store.insert_workspace(&ws).await.unwrap();

        let agent_id = AgentId(uuid::Uuid::new_v4().to_string());
        create_test_agent(&store, &ws.id, &agent_id).await;

        let base_sha = commit_file(&canonical_path, "file1.txt", "base", "Add file1");

        // Blob ref in the canonical repo (intentd writes refs/intent/blobs/*
        // there); the local-transport fetch walks every canonical ref and must
        // not choke on the non-commit target.
        {
            let canonical_repo = git2::Repository::open(&canonical_path).unwrap();
            let blob_oid = canonical_repo.blob(b"intent blob payload").unwrap();
            canonical_repo
                .reference(
                    &format!("refs/intent/blobs/{blob_oid}"),
                    blob_oid,
                    false,
                    "test blob ref",
                )
                .unwrap();
        }

        // Clone canonical to sandbox (stands in for the CoW clone)
        let sandbox_path = test_root.join("sandbox");
        git2::Repository::clone(canonical_path.to_str().unwrap(), &sandbox_path).unwrap();

        let branch_name = format!("sb/{}", agent_id.0);
        {
            let mut sandbox_repo = git2::Repository::open(&sandbox_path).unwrap();
            {
                let head_commit = sandbox_repo.head().unwrap().peel_to_commit().unwrap();
                sandbox_repo
                    .branch(&branch_name, &head_commit, false)
                    .unwrap();
                sandbox_repo
                    .set_head(&format!("refs/heads/{branch_name}"))
                    .unwrap();

                // Blob ref, as inherited from the source repo by CoW clones.
                let blob_oid = sandbox_repo.blob(b"intent blob payload").unwrap();
                sandbox_repo
                    .reference(
                        &format!("refs/intent/blobs/{blob_oid}"),
                        blob_oid,
                        false,
                        "test blob ref",
                    )
                    .unwrap();
            }

            // A stash (refs/stash), also inherited by CoW clones.
            fs::write(sandbox_path.join("stashme.txt"), "wip").unwrap();
            let sig = git2::Signature::now("Test", "test@example.com").unwrap();
            sandbox_repo
                .stash_save(&sig, "wip stash", Some(git2::StashFlags::INCLUDE_UNTRACKED))
                .unwrap();
        }

        // Agent work on the sandbox branch.
        commit_file(&sandbox_path, "agent.txt", "agent work", "Agent work");

        let sandbox = Sandbox {
            id: uuid::Uuid::new_v4().to_string(),
            workspace_id: ws.id.clone(),
            agent_id: agent_id.clone(),
            path: sandbox_path.to_string_lossy().to_string(),
            branch: branch_name,
            base_commit_sha: base_sha,
            snapshot_commit_sha: None,
            status: SandboxStatus::Created,
            retry_count: 0,
            created_at: now_iso(),
            updated_at: now_iso(),
        };
        store.insert_sandbox(&sandbox).await.unwrap();

        let outcome = merge_sandbox(&store, &ws.id, &agent_id)
            .await
            .expect("merge must not fail on advertised non-commit refs");
        match outcome {
            MergeOutcome::Merged { .. } => {
                assert!(canonical_path.join("agent.txt").exists());
            }
            _ => panic!("Expected Merged outcome, got {outcome:?}"),
        }

        // The temp fetch ref is cleaned up after the merge.
        let canonical_repo = git2::Repository::open(&canonical_path).unwrap();
        assert!(
            canonical_repo
                .find_reference(&format!("refs/intent/sandbox-merge/{}", agent_id.0))
                .is_err(),
            "temp fetch ref must be deleted after merge"
        );

        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn test_sandbox_merge_uses_branch_tip_not_head() {
        // Regression: the commit range must come from refs/heads/<branch>, not
        // sandbox HEAD — the merge-back fetch only brings over the branch's
        // objects, so a detached/re-pointed HEAD would reference commits absent
        // from the canonical ODB.
        let (store, _db) = temp_store().await;
        let (test_root, canonical_path) = temp_repo_in_target("branch-tip-not-head");

        let ws = workspace_for_repo(&canonical_path);
        store.insert_workspace(&ws).await.unwrap();

        let agent_id = AgentId(uuid::Uuid::new_v4().to_string());
        create_test_agent(&store, &ws.id, &agent_id).await;

        let base_sha = commit_file(&canonical_path, "file1.txt", "base", "Add file1");

        let sandbox_path = test_root.join("sandbox");
        git2::Repository::clone(canonical_path.to_str().unwrap(), &sandbox_path).unwrap();

        let branch_name = format!("sb/{}", agent_id.0);
        {
            let sandbox_repo = git2::Repository::open(&sandbox_path).unwrap();
            let head_commit = sandbox_repo.head().unwrap().peel_to_commit().unwrap();
            sandbox_repo
                .branch(&branch_name, &head_commit, false)
                .unwrap();
            sandbox_repo
                .set_head(&format!("refs/heads/{branch_name}"))
                .unwrap();
        }

        // Agent work lands on the sandbox branch...
        commit_file(&sandbox_path, "agent.txt", "agent work", "Agent work");

        // ...then HEAD is detached at the pre-work base commit. The merge must
        // still pick up the branch tip's commit.
        {
            let sandbox_repo = git2::Repository::open(&sandbox_path).unwrap();
            let base_oid = git2::Oid::from_str(&base_sha).unwrap();
            sandbox_repo.set_head_detached(base_oid).unwrap();
            sandbox_repo
                .checkout_head(Some(git2::build::CheckoutBuilder::new().force()))
                .unwrap();
        }

        let sandbox = Sandbox {
            id: uuid::Uuid::new_v4().to_string(),
            workspace_id: ws.id.clone(),
            agent_id: agent_id.clone(),
            path: sandbox_path.to_string_lossy().to_string(),
            branch: branch_name,
            base_commit_sha: base_sha,
            snapshot_commit_sha: None,
            status: SandboxStatus::Created,
            retry_count: 0,
            created_at: now_iso(),
            updated_at: now_iso(),
        };
        store.insert_sandbox(&sandbox).await.unwrap();

        let outcome = merge_sandbox(&store, &ws.id, &agent_id)
            .await
            .expect("merge must resolve the branch tip, not HEAD");
        match outcome {
            MergeOutcome::Merged { .. } => {
                assert!(canonical_path.join("agent.txt").exists());
            }
            _ => panic!("Expected Merged outcome, got {outcome:?}"),
        }

        let _ = fs::remove_dir_all(&test_root);
    }

    #[tokio::test]
    async fn test_sandbox_merge_missing_branch_yields_blocked() {
        // A missing (or unborn) sb/<agentId> branch must yield a typed Blocked
        // outcome (callers mark the sandbox merge-pending, which is retryable)
        // rather than an internal fetch error.
        let (store, _db) = temp_store().await;
        let (test_root, canonical_path) = temp_repo_in_target("missing-branch");

        let ws = workspace_for_repo(&canonical_path);
        store.insert_workspace(&ws).await.unwrap();

        let agent_id = AgentId(uuid::Uuid::new_v4().to_string());
        create_test_agent(&store, &ws.id, &agent_id).await;

        let base_sha = commit_file(&canonical_path, "file1.txt", "base", "Add file1");

        // Clone canonical to sandbox; the sb/<agentId> branch is never created.
        let sandbox_path = test_root.join("sandbox");
        git2::Repository::clone(canonical_path.to_str().unwrap(), &sandbox_path).unwrap();

        let sandbox = Sandbox {
            id: uuid::Uuid::new_v4().to_string(),
            workspace_id: ws.id.clone(),
            agent_id: agent_id.clone(),
            path: sandbox_path.to_string_lossy().to_string(),
            branch: format!("sb/{}", agent_id.0),
            base_commit_sha: base_sha,
            snapshot_commit_sha: None,
            status: SandboxStatus::Created,
            retry_count: 0,
            created_at: now_iso(),
            updated_at: now_iso(),
        };
        store.insert_sandbox(&sandbox).await.unwrap();

        let outcome = merge_sandbox(&store, &ws.id, &agent_id)
            .await
            .expect("missing branch must not be an internal error");
        match outcome {
            MergeOutcome::Blocked {
                reason,
                overlapping_paths,
            } => {
                assert!(
                    reason.contains("missing or unborn"),
                    "unexpected reason: {reason}"
                );
                assert!(overlapping_paths.is_empty());
            }
            _ => panic!("Expected Blocked outcome, got {outcome:?}"),
        }

        // The sandbox record is untouched, so the merge stays retryable.
        assert!(store
            .get_sandbox(&ws.id, &agent_id)
            .await
            .unwrap()
            .is_some());

        let _ = fs::remove_dir_all(&test_root);
    }

    #[test]
    fn audit_reports_diverged_non_merge_branches() {
        // Only branches OTHER than the merge branch with tips unreachable in
        // the workspace repo are reported.
        let (test_root, canonical_path) = temp_repo_in_target("audit-diverged");
        commit_file(&canonical_path, "f.txt", "x", "base");

        let sandbox_path = test_root.join("sandbox");
        git2::Repository::clone(canonical_path.to_str().unwrap(), &sandbox_path).unwrap();
        let sandbox_repo = git2::Repository::open(&sandbox_path).unwrap();
        let head_commit = sandbox_repo.head().unwrap().peel_to_commit().unwrap();

        // Merge branch with agent work (diverged, but excluded from the audit).
        sandbox_repo
            .branch("sb/agent-1", &head_commit, false)
            .unwrap();
        sandbox_repo.set_head("refs/heads/sb/agent-1").unwrap();
        commit_file(&sandbox_path, "agent.txt", "agent", "Agent work");

        // In-sync branch at the canonical HEAD (reachable; not reported).
        sandbox_repo.branch("in-sync", &head_commit, false).unwrap();

        // Rogue branch with a commit the workspace repo has never seen.
        sandbox_repo.branch("rogue", &head_commit, false).unwrap();
        sandbox_repo.set_head("refs/heads/rogue").unwrap();
        commit_file(&sandbox_path, "rogue.txt", "rogue", "Rogue work");

        let canonical_repo = git2::Repository::open(&canonical_path).unwrap();
        let diverged =
            audit_diverged_sandbox_branches(&sandbox_repo, &canonical_repo, "sb/agent-1");
        assert_eq!(diverged, vec!["rogue".to_string()]);

        let _ = fs::remove_dir_all(&test_root);
    }

    #[test]
    fn resolve_signature_uses_configured_identity() {
        let (_dir, repo_path) = temp_repo("sig-configured");
        let repo = git2::Repository::open(&repo_path).unwrap();
        let mut cfg = repo.config().unwrap();
        cfg.set_str("user.name", "Configured User").unwrap();
        cfg.set_str("user.email", "configured@example.com").unwrap();

        let sig = resolve_signature(&repo).unwrap();
        assert_eq!(sig.name().unwrap(), "Configured User");
        assert_eq!(sig.email().unwrap(), "configured@example.com");
    }

    #[test]
    fn signature_fallback_defaults_on_missing_identity() {
        // Missing user.name/user.email surfaces as ErrorCode::NotFound.
        let err = git2::Error::new(
            git2::ErrorCode::NotFound,
            git2::ErrorClass::Config,
            "config value 'user.name' was not found",
        );
        let sig = signature_or_fallback(Err(err)).unwrap();
        assert_eq!(sig.name().unwrap(), "Intent");
        assert_eq!(sig.email().unwrap(), "intent@localhost");
    }

    #[test]
    fn signature_fallback_propagates_other_errors() {
        let err = git2::Error::new(
            git2::ErrorCode::GenericError,
            git2::ErrorClass::Config,
            "failed to parse config file",
        );
        let msg = match signature_or_fallback(Err(err)) {
            Ok(_) => panic!("expected non-NotFound signature error to propagate"),
            Err(e) => e.to_string(),
        };
        assert!(
            msg.contains("resolve git signature failed"),
            "unexpected error: {msg}"
        );
        assert!(msg.contains("failed to parse config file"));
    }

    /// Untracked nested repo like agents leave behind: a directory with its
    /// own real `.git` directory and file content (an empty worktree would
    /// not even show up in the parent's status).
    fn make_nested_repo(parent: &Path, name: impl AsRef<Path>) -> PathBuf {
        let nested = parent.join(name);
        init_test_repo(&nested);
        fs::write(nested.join("inner.txt"), "inner\n").unwrap();
        nested
    }

    /// Worktree-style nested checkout: `git worktree add` creates a directory
    /// whose `.git` is a FILE pointing at the parent repo's worktree metadata.
    fn make_nested_worktree(repo_path: &Path, name: &str) -> PathBuf {
        let out = std::process::Command::new("git")
            .current_dir(repo_path)
            .args(["worktree", "add", "-b"])
            .arg(format!("wt-{}", name.trim_start_matches('.')))
            .arg(name)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        repo_path.join(name)
    }

    #[test]
    fn is_dirty_ignores_exclusively_untracked_nested_repos() {
        let (_dir, repo_path) = temp_repo("dirty-nested-only");
        make_nested_repo(&repo_path, ".import-wt");

        // A repo whose only anomaly is an untracked nested repo is not dirty:
        // the nested checkout cannot be staged, so a snapshot commit would be
        // empty (or fail outright).
        let repo = git2::Repository::open(&repo_path).unwrap();
        assert!(!is_dirty(&repo).unwrap());

        // Ordinary untracked files still count.
        fs::write(repo_path.join("new.txt"), "x").unwrap();
        assert!(is_dirty(&repo).unwrap());
    }

    #[test]
    fn create_snapshot_commit_skips_nested_repos_and_worktrees() {
        let (_dir, repo_path) = temp_repo("snapshot-nested");
        commit_file(&repo_path, "tracked.txt", "base", "Add tracked");
        // Untracked nested git repo (real `.git` dir) and a worktree-style
        // checkout (`.git` FILE), alongside ordinary dirty state.
        make_nested_repo(&repo_path, ".import-wt");
        make_nested_worktree(&repo_path, ".roundtrip-wt");
        fs::write(repo_path.join("dirty.txt"), "dirty").unwrap();
        fs::write(repo_path.join("tracked.txt"), "modified").unwrap();

        let repo = git2::Repository::open(&repo_path).unwrap();
        assert!(is_dirty(&repo).unwrap());

        let agent_id = AgentId::from("agent-nested-snapshot");
        let sha = create_snapshot_commit(&repo, &agent_id)
            .expect("snapshot must not fail on untracked nested repos");

        let commit = repo
            .find_commit(git2::Oid::from_str(&sha).unwrap())
            .unwrap();
        let tree = commit.tree().unwrap();
        assert!(tree.get_name("dirty.txt").is_some());
        assert!(tree.get_name("tracked.txt").is_some());
        // No gitlink entries for the nested checkouts...
        assert!(tree.get_name(".import-wt").is_none());
        assert!(tree.get_name(".roundtrip-wt").is_none());
        // ...and the directories stay intact on disk.
        assert!(repo_path.join(".import-wt/.git").is_dir());
        assert!(repo_path.join(".roundtrip-wt/.git").is_file());
    }

    /// Nested repos with non-UTF-8 directory names must still be detected
    /// and skipped: `StatusEntry::path()` returns `None` for such names, so
    /// the detection goes through `path_bytes` instead.
    #[cfg(unix)]
    #[test]
    fn create_snapshot_commit_skips_non_utf8_nested_repo_names() {
        use std::os::unix::ffi::OsStrExt;

        let (_dir, repo_path) = temp_repo("snapshot-nested-non-utf8");
        let name = std::ffi::OsStr::from_bytes(b"bad-\xff-wt");
        // Some filesystems reject non-UTF-8 file names outright (APFS on
        // macOS fails with EILSEQ), so the fixture cannot exist there. Probe
        // at runtime and skip instead of failing (intent-hq/monorepo#3028);
        // the test still runs fully on Linux/CI. Only the name-rejection
        // errno may skip — anything else (permissions, ENOSPC) must surface.
        if let Err(err) = fs::create_dir(repo_path.join(name)) {
            assert!(
                matches!(err.raw_os_error(), Some(libc::EILSEQ) | Some(libc::EINVAL)),
                "unexpected error creating non-UTF-8 fixture: {err}"
            );
            eprintln!(
                "skipping create_snapshot_commit_skips_non_utf8_nested_repo_names: \
                 filesystem rejects non-UTF-8 file names"
            );
            return;
        }
        commit_file(&repo_path, "tracked.txt", "base", "Add tracked");
        make_nested_repo(&repo_path, Path::new(name));
        fs::write(repo_path.join("dirty.txt"), "dirty").unwrap();

        let repo = git2::Repository::open(&repo_path).unwrap();
        let agent_id = AgentId::from("agent-nested-non-utf8");
        let sha = create_snapshot_commit(&repo, &agent_id)
            .expect("snapshot must not fail on a non-UTF-8 nested repo name");

        let commit = repo
            .find_commit(git2::Oid::from_str(&sha).unwrap())
            .unwrap();
        let tree = commit.tree().unwrap();
        assert!(tree.get_name("dirty.txt").is_some());
        assert_eq!(
            tree.iter()
                .filter(|e| e.name_bytes().starts_with(b"bad-"))
                .count(),
            0
        );
        assert!(repo_path.join(name).join(".git").is_dir());
    }

    #[tokio::test]
    async fn test_sandbox_merge_auto_commit_skips_nested_repos() {
        let (store, _db) = temp_store().await;
        let (test_root, canonical_path) = temp_repo_in_target("merge-nested");

        let ws = workspace_for_repo(&canonical_path);
        store.insert_workspace(&ws).await.unwrap();
        let agent_id = AgentId(uuid::Uuid::new_v4().to_string());
        create_test_agent(&store, &ws.id, &agent_id).await;

        let base_sha = commit_file(&canonical_path, "file1.txt", "base", "Add file1");

        let sandbox_path = test_root.join("sandbox");
        git2::Repository::clone(canonical_path.to_str().unwrap(), &sandbox_path).unwrap();
        let branch_name = format!("sb/{}", agent_id.0);
        {
            let sandbox_repo = git2::Repository::open(&sandbox_path).unwrap();
            let head_commit = sandbox_repo.head().unwrap().peel_to_commit().unwrap();
            sandbox_repo
                .branch(&branch_name, &head_commit, false)
                .unwrap();
            sandbox_repo
                .set_head(&format!("refs/heads/{branch_name}"))
                .unwrap();
        }
        commit_file(&sandbox_path, "agent.txt", "agent work", "Agent work");

        // Dirty state alongside untracked nested checkouts: the merge's
        // auto-commit must stage the ordinary file and skip the embedded
        // repos instead of failing with `invalid path`.
        make_nested_repo(&sandbox_path, ".import-wt");
        make_nested_worktree(&sandbox_path, ".roundtrip-wt");
        fs::write(sandbox_path.join("wip.txt"), "wip").unwrap();

        let sandbox = Sandbox {
            id: uuid::Uuid::new_v4().to_string(),
            workspace_id: ws.id.clone(),
            agent_id: agent_id.clone(),
            path: sandbox_path.to_string_lossy().to_string(),
            branch: branch_name,
            base_commit_sha: base_sha,
            snapshot_commit_sha: None,
            status: SandboxStatus::Created,
            retry_count: 0,
            created_at: now_iso(),
            updated_at: now_iso(),
        };
        store.insert_sandbox(&sandbox).await.unwrap();

        let outcome = merge_sandbox(&store, &ws.id, &agent_id)
            .await
            .expect("merge must not fail on untracked nested repos");
        match outcome {
            MergeOutcome::Merged { .. } => {}
            other => panic!("Expected Merged outcome, got {other:?}"),
        }
        assert!(canonical_path.join("agent.txt").exists());
        assert!(canonical_path.join("wip.txt").exists());
        assert!(!canonical_path.join(".import-wt").exists());
        assert!(!canonical_path.join(".roundtrip-wt").exists());

        // No gitlink entries in the auto-commit; nested checkouts survive
        // untouched on disk.
        let sandbox_repo = git2::Repository::open(&sandbox_path).unwrap();
        let head_tree = sandbox_repo
            .head()
            .unwrap()
            .peel_to_commit()
            .unwrap()
            .tree()
            .unwrap();
        assert!(head_tree.get_name("wip.txt").is_some());
        assert!(head_tree.get_name(".import-wt").is_none());
        assert!(head_tree.get_name(".roundtrip-wt").is_none());
        assert!(sandbox_path.join(".import-wt/.git").is_dir());
        assert!(sandbox_path.join(".roundtrip-wt/.git").is_file());

        let _ = fs::remove_dir_all(&test_root);
    }

    // P0-2: Completion-interception tests — BLOCKER IDENTIFIED
    //
    // Per packages/intentd/AGENTS.md: "Every feature MUST have an end-to-end test that
    // drives the real WSS transport." Services-level tests that call handle_completion_event
    // directly are DISHONEST — they bypass the event bus, subscription delivery, and the
    // full lifecycle path that production uses.
    //
    // Required: WSS e2e tests (in crates/intentd/tests/e2e_wss_*.rs) that:
    // 1. Boot intentd serve with CoW-capable workspaces_root
    // 2. Provision a sandbox via the agent.create flow
    // 3. Drive git operations in both canonical and sandbox repos
    // 4. Subscribe to events.event and assert agent:idle + completion delivery
    // 5. Verify merge outcomes, bounce messages, retry caps via the wire protocol
    //
    // Blocked on: extending the existing e2e_wss harness (e2e_wss_agent_lifecycle.rs) to
    // support CoW provisioning, git operation setup, and event subscription assertions.
    //
    // The previous Services-level tests have been removed as misleading. Implementing the
    // WSS e2e infrastructure is ~1 session of focused work but is beyond the current P0-2
    // fix scope without explicit coordinator approval.
}
