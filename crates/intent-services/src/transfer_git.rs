//! Source-side git packaging for workspace transfer (spec §4, resolved
//! decision 1): snapshot dirty state as sentinel-marked WIP commits (the
//! workspace worktree AND every sandbox), then build one `git bundle`
//! carrying the workspace branch, the base ref, and all `sb/<agentId>`
//! sandbox branches. The inverse helper ([`unwind_wip`]) removes a WIP
//! snapshot commit and restores the exact staged/unstaged/untracked split —
//! reused by the import side after materialization and by the source-side
//! failure/cleanup paths. No wire code lives here.

use std::path::{Path, PathBuf};
use std::process::Command;

use intent_core::{Error, Result, Workspace};
use intent_store::Sandbox;
use serde::{Deserialize, Serialize};

/// First line of every transfer WIP snapshot commit message. The import side
/// identifies snapshot commits by this sentinel and unwinds them via
/// [`unwind_wip`]; keep it stable across versions.
pub const TRANSFER_WIP_SENTINEL: &str = "intent-transfer: WIP snapshot";

/// Commit-message trailer carrying the pre-snapshot index tree OID, so
/// [`unwind_wip`] can restore the exact staged/unstaged split (a plain soft
/// reset would leave everything staged).
const INDEX_TREE_TRAILER: &str = "Intent-Index-Tree:";

/// Namespace for the temporary refs anchoring non-branch bundle entries in
/// the worktree repo. Created for the duration of `git bundle create` and
/// deleted afterwards (success or failure); the names survive inside the
/// bundle header, which is how the target addresses them.
const TRANSFER_REF_NS: &str = "refs/intent/transfer";

/// Bundle ref name recording the workspace base commit.
pub const BASE_BUNDLE_REF: &str = "refs/intent/transfer/base";

/// Bundle ref name recording one sandbox's `sb/<agentId>` branch tip.
pub fn sandbox_bundle_ref(agent_id: &str) -> String {
    format!("{TRANSFER_REF_NS}/sandbox/{agent_id}")
}

/// Ref inventory of a transfer bundle: what each bundle ref is and how it
/// maps back to workspace/sandbox branches on the target.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferRefsManifest {
    /// Branch name the workspace worktree was on (normally `Workspace.branch`).
    pub workspace_branch: String,
    /// Bundle ref carrying the workspace branch (`refs/heads/<branch>`).
    pub workspace_bundle_ref: String,
    /// Tip of the workspace branch as bundled (the WIP snapshot commit when
    /// the worktree was dirty).
    pub workspace_head_sha: String,
    /// WIP snapshot commit created on the workspace branch, if the worktree
    /// was dirty. Left in place on success — the orchestrator unwinds it via
    /// [`unwind_wip`] once the export settles.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub workspace_wip_commit_sha: Option<String>,
    /// The workspace `baseRef` name (e.g. `main`), when set.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_ref: Option<String>,
    /// Bundle ref anchoring the base commit ([`BASE_BUNDLE_REF`]); omitted
    /// when no base could be resolved locally.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_bundle_ref: Option<String>,
    /// The resolved base commit SHA.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub base_sha: Option<String>,
    /// One entry per sandbox whose branch made it into the bundle.
    pub sandboxes: Vec<SandboxBundleRef>,
}

/// One sandbox branch as recorded in the bundle.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SandboxBundleRef {
    pub agent_id: String,
    /// The sandbox branch name (`sb/<agentId>`).
    pub branch: String,
    /// Bundle ref carrying the branch (`refs/intent/transfer/sandbox/<agentId>`).
    pub bundle_ref: String,
    /// Tip of the sandbox branch as bundled (the WIP snapshot commit when the
    /// sandbox was dirty).
    pub head_sha: String,
    /// WIP snapshot commit created in the sandbox, if it was dirty. Left in
    /// place on success, like the workspace one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wip_commit_sha: Option<String>,
    /// The sandbox's recorded provisioning base (`Sandbox.base_commit_sha`).
    pub base_commit_sha: String,
    /// The provisioning-time dirty-state snapshot (`Sandbox.snapshot_commit_sha`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_commit_sha: Option<String>,
}

/// Snapshot a repository's dirty state (staged + unstaged + untracked) as a
/// single sentinel-marked WIP commit on HEAD's branch. Returns the commit SHA,
/// or `None` when the repository is clean. The pre-snapshot index tree is
/// recorded as a commit-message trailer so [`unwind_wip`] restores the exact
/// staged/unstaged split.
pub fn snapshot_wip(repo_path: &Path) -> Result<Option<String>> {
    let repo = git2::Repository::open(repo_path)
        .map_err(|e| Error::Internal(format!("open repo for WIP snapshot failed: {e}")))?;
    if !is_dirty(&repo)? {
        return Ok(None);
    }
    let head_commit = repo
        .head()
        .ok()
        .and_then(|h| h.peel_to_commit().ok())
        .ok_or_else(|| {
            Error::Internal("cannot snapshot dirty state: repository HEAD is unborn".to_string())
        })?;
    let mut index = repo
        .index()
        .map_err(|e| Error::Internal(format!("get index failed: {e}")))?;
    // Pre-snapshot staged state, before add_all coarsens it.
    let orig_index_tree = index
        .write_tree()
        .map_err(|e| Error::Internal(format!("write pre-snapshot index tree failed: {e}")))?;
    index
        .add_all(["*"].iter(), git2::IndexAddOption::DEFAULT, None)
        .map_err(|e| Error::Internal(format!("stage all files failed: {e}")))?;
    index
        .write()
        .map_err(|e| Error::Internal(format!("write index failed: {e}")))?;
    let tree_oid = index
        .write_tree()
        .map_err(|e| Error::Internal(format!("write tree failed: {e}")))?;
    let tree = repo
        .find_tree(tree_oid)
        .map_err(|e| Error::Internal(format!("find tree failed: {e}")))?;
    let sig = resolve_signature(&repo)?;
    let message = format!("{TRANSFER_WIP_SENTINEL}\n\n{INDEX_TREE_TRAILER} {orig_index_tree}");
    let oid = repo
        .commit(Some("HEAD"), &sig, &sig, &message, &tree, &[&head_commit])
        .map_err(|e| Error::Internal(format!("create WIP snapshot commit failed: {e}")))?;
    Ok(Some(oid.to_string()))
}

/// Inverse of [`snapshot_wip`]: when HEAD is a transfer WIP snapshot commit,
/// soft-reset it away and restore the pre-snapshot index from the
/// [`INDEX_TREE_TRAILER`], leaving the worktree exactly as found (staged
/// stays staged, unstaged stays unstaged, untracked stays untracked).
/// Returns `false` (no-op) when HEAD is not a transfer WIP commit.
pub fn unwind_wip(repo_path: &Path) -> Result<bool> {
    let repo = git2::Repository::open(repo_path)
        .map_err(|e| Error::Internal(format!("open repo for WIP unwind failed: {e}")))?;
    let head_commit = match repo.head().ok().and_then(|h| h.peel_to_commit().ok()) {
        Some(c) => c,
        None => return Ok(false),
    };
    let message = head_commit.message().unwrap_or("").to_string();
    if !message.starts_with(TRANSFER_WIP_SENTINEL) {
        return Ok(false);
    }
    let parent = head_commit
        .parent(0)
        .map_err(|e| Error::Internal(format!("transfer WIP snapshot commit has no parent: {e}")))?;
    repo.reset(parent.as_object(), git2::ResetType::Soft, None)
        .map_err(|e| Error::Internal(format!("soft reset of WIP snapshot failed: {e}")))?;
    // Restore the exact pre-snapshot staged state; without the trailer the
    // soft reset alone leaves everything staged (degraded but safe).
    if let Some(tree_oid) = parse_index_tree_trailer(&message) {
        if let Ok(tree) = repo.find_tree(tree_oid) {
            let mut index = repo
                .index()
                .map_err(|e| Error::Internal(format!("get index failed: {e}")))?;
            index
                .read_tree(&tree)
                .map_err(|e| Error::Internal(format!("restore pre-snapshot index failed: {e}")))?;
            index
                .write()
                .map_err(|e| Error::Internal(format!("write restored index failed: {e}")))?;
        }
    }
    Ok(true)
}

/// Build the transfer bundle for a workspace: WIP-snapshot the worktree and
/// every sandbox, anchor the base commit and each sandbox branch under
/// temporary `refs/intent/transfer/*` refs in the worktree repo, and run
/// `git bundle create` + `verify`. Returns the bundle path and the ref
/// inventory.
///
/// On success the WIP snapshot commits are left in place (they are what the
/// bundle refs point at); the caller unwinds them via [`unwind_wip`] once the
/// export settles. On failure every created WIP commit is unwound, temporary
/// refs are deleted, and any partial bundle file is removed — the source is
/// restored exactly as found.
pub fn create_transfer_bundle(
    ws: &Workspace,
    sandboxes: &[Sandbox],
    staging_dir: &Path,
) -> Result<(PathBuf, TransferRefsManifest)> {
    let worktree = crate::git_ops::worktree_path(ws).ok_or_else(|| {
        Error::Internal("workspace has no worktree or repository path".to_string())
    })?;
    std::fs::create_dir_all(staging_dir)
        .map_err(|e| Error::Internal(format!("create staging dir failed: {e}")))?;
    let bundle_path = staging_dir.join(format!("{}.bundle", ws.id.0));

    let mut snapshotted: Vec<PathBuf> = Vec::new();
    let mut temp_refs: Vec<String> = Vec::new();
    let result = build_bundle(
        ws,
        sandboxes,
        &worktree,
        &bundle_path,
        &mut snapshotted,
        &mut temp_refs,
    );

    // The temp refs only anchor the bundle build; drop them regardless of
    // outcome (their names live on inside the bundle header).
    cleanup_temp_refs(&worktree, &temp_refs);

    if result.is_err() {
        for path in snapshotted.iter().rev() {
            if let Err(e) = unwind_wip(path) {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "transfer bundle cleanup: failed to unwind WIP snapshot"
                );
            }
        }
        if bundle_path.exists() {
            let _ = std::fs::remove_file(&bundle_path);
        }
    }

    result.map(|manifest| (bundle_path, manifest))
}

fn build_bundle(
    ws: &Workspace,
    sandboxes: &[Sandbox],
    worktree: &Path,
    bundle_path: &Path,
    snapshotted: &mut Vec<PathBuf>,
    temp_refs: &mut Vec<String>,
) -> Result<TransferRefsManifest> {
    let repo = git2::Repository::open(worktree)
        .map_err(|e| Error::Internal(format!("open workspace repo failed: {e}")))?;

    // 1. Snapshot the worktree's dirty state onto its branch.
    let workspace_wip = snapshot_wip(worktree)?;
    if workspace_wip.is_some() {
        snapshotted.push(worktree.to_path_buf());
    }

    // 2. The workspace branch is whatever HEAD points at — that is where the
    //    WIP snapshot landed. A detached HEAD has no branch to bundle.
    let head = repo
        .head()
        .map_err(|e| Error::Internal(format!("resolve workspace HEAD failed: {e}")))?;
    if !head.is_branch() {
        return Err(Error::Internal(
            "workspace HEAD is detached; cannot bundle a branch".to_string(),
        ));
    }
    let workspace_branch = head.shorthand().unwrap_or_default().to_string();
    let workspace_bundle_ref = format!("refs/heads/{workspace_branch}");
    let workspace_head_sha = head
        .target()
        .map(|oid| oid.to_string())
        .ok_or_else(|| Error::Internal("workspace branch has no target".to_string()))?;
    if workspace_branch != ws.branch {
        tracing::warn!(
            head_branch = %workspace_branch,
            workspace_branch = %ws.branch,
            "transfer bundle: worktree HEAD branch differs from the workspace row; bundling the HEAD branch"
        );
    }

    // 3. Anchor the base commit under a temp ref so the target can fetch it
    //    by name. Best-effort: a missing base just omits the entry.
    let base_oid = resolve_base_commit(&repo, ws);
    let (base_bundle_ref, base_sha) = match base_oid {
        Some(oid) => {
            repo.reference(BASE_BUNDLE_REF, oid, true, "transfer base anchor")
                .map_err(|e| Error::Internal(format!("create base transfer ref failed: {e}")))?;
            temp_refs.push(BASE_BUNDLE_REF.to_string());
            (Some(BASE_BUNDLE_REF.to_string()), Some(oid.to_string()))
        }
        None => (None, None),
    };

    // 4. Snapshot each sandbox and fetch its branch into a temp ref.
    let mut sandbox_refs = Vec::new();
    for sb in sandboxes {
        let sb_path = PathBuf::from(&sb.path);
        if !sb_path.exists() {
            tracing::warn!(
                agent = %sb.agent_id.0,
                path = %sb.path,
                "transfer bundle: sandbox directory missing; skipping"
            );
            continue;
        }
        let wip = snapshot_wip(&sb_path)?;
        if wip.is_some() {
            snapshotted.push(sb_path.clone());
        }
        let branch_ref = format!("refs/heads/{}", sb.branch);
        {
            let sb_repo = git2::Repository::open(&sb_path)
                .map_err(|e| Error::Internal(format!("open sandbox repo failed: {e}")))?;
            let committish = sb_repo
                .find_reference(&branch_ref)
                .and_then(|r| r.peel_to_commit())
                .is_ok();
            if !committish {
                tracing::warn!(
                    agent = %sb.agent_id.0,
                    branch = %sb.branch,
                    "transfer bundle: sandbox branch missing or unborn; skipping"
                );
                continue;
            }
        }
        let bundle_ref = sandbox_bundle_ref(&sb.agent_id.0);
        fetch_local_ref(worktree, &sb_path, &branch_ref, &bundle_ref)?;
        temp_refs.push(bundle_ref.clone());
        let head_sha = repo
            .find_reference(&bundle_ref)
            .ok()
            .and_then(|r| r.target())
            .map(|oid| oid.to_string())
            .ok_or_else(|| {
                Error::Internal(format!(
                    "fetched sandbox transfer ref {bundle_ref} did not resolve"
                ))
            })?;
        sandbox_refs.push(SandboxBundleRef {
            agent_id: sb.agent_id.0.clone(),
            branch: sb.branch.clone(),
            bundle_ref,
            head_sha,
            wip_commit_sha: wip,
            base_commit_sha: sb.base_commit_sha.clone(),
            snapshot_commit_sha: sb.snapshot_commit_sha.clone(),
        });
    }

    // 5. Create and verify the bundle (full history — self-contained, no
    //    prerequisites, so the target can clone/fetch with no other remote).
    let mut ref_args = vec![workspace_bundle_ref.clone()];
    if let Some(r) = &base_bundle_ref {
        ref_args.push(r.clone());
    }
    ref_args.extend(sandbox_refs.iter().map(|s| s.bundle_ref.clone()));
    run_git(worktree, |cmd| {
        cmd.arg("bundle").arg("create").arg(bundle_path);
        cmd.args(&ref_args);
    })
    .map_err(|e| Error::Internal(format!("git bundle create failed: {e}")))?;
    run_git(worktree, |cmd| {
        cmd.arg("bundle").arg("verify").arg(bundle_path);
    })
    .map_err(|e| Error::Internal(format!("git bundle verify failed: {e}")))?;

    Ok(TransferRefsManifest {
        workspace_branch,
        workspace_bundle_ref,
        workspace_head_sha,
        workspace_wip_commit_sha: workspace_wip,
        base_ref: ws.base_ref.clone(),
        base_bundle_ref,
        base_sha,
        sandboxes: sandbox_refs,
    })
}

/// Resolve the workspace base commit locally: the remote-tracking ref for
/// `baseRef` first (matches `provision_worktree`'s preference), then the
/// local branch, then the recorded `baseCommitSha` if the object exists.
fn resolve_base_commit(repo: &git2::Repository, ws: &Workspace) -> Option<git2::Oid> {
    if let Some(base) = ws.base_ref.as_deref() {
        for candidate in [
            format!("refs/remotes/origin/{base}"),
            format!("refs/heads/{base}"),
        ] {
            if let Some(oid) = repo
                .find_reference(&candidate)
                .and_then(|r| r.peel_to_commit())
                .ok()
                .map(|c| c.id())
            {
                return Some(oid);
            }
        }
    }
    if let Some(sha) = ws.base_commit_sha.as_deref() {
        if let Ok(oid) = git2::Oid::from_str(sha) {
            if repo.find_commit(oid).is_ok() {
                return Some(oid);
            }
        }
    }
    None
}

/// Fetch `src_ref` from a local repository into `dst_ref` of the worktree
/// repo. Shells out with an explicit full refspec and tag auto-follow
/// disabled, for the same reason as the sandbox merge path: CoW clones carry
/// non-commit refs (`refs/intent/blobs/*`, `refs/stash`) that libgit2's local
/// transport trips over.
fn fetch_local_ref(worktree: &Path, from_repo: &Path, src_ref: &str, dst_ref: &str) -> Result<()> {
    let from = from_repo
        .to_str()
        .ok_or_else(|| Error::Internal("sandbox path not UTF-8".to_string()))?;
    let refspec = format!("+{src_ref}:{dst_ref}");
    run_git(worktree, |cmd| {
        cmd.arg("fetch")
            .arg("--no-tags")
            .arg("--quiet")
            .arg(from)
            .arg(&refspec);
    })
    .map_err(|e| Error::Internal(format!("fetch sandbox branch failed: {e}")))
}

/// Run a git subcommand in `dir` with prompts disabled; returns the trimmed
/// stderr as the error string on non-zero exit.
fn run_git(dir: &Path, configure: impl FnOnce(&mut Command)) -> std::result::Result<(), String> {
    let mut cmd = Command::new("git");
    configure(&mut cmd);
    let out = cmd
        .current_dir(dir)
        .env("GIT_TERMINAL_PROMPT", "0")
        .stdin(std::process::Stdio::null())
        .output()
        .map_err(|e| e.to_string())?;
    if out.status.success() {
        Ok(())
    } else {
        Err(String::from_utf8_lossy(&out.stderr).trim().to_string())
    }
}

/// Delete the temporary transfer refs from the worktree repo (best-effort).
fn cleanup_temp_refs(worktree: &Path, refs: &[String]) {
    if refs.is_empty() {
        return;
    }
    let Ok(repo) = git2::Repository::open(worktree) else {
        return;
    };
    for name in refs {
        if let Ok(mut r) = repo.find_reference(name) {
            let _ = r.delete();
        }
    }
}

/// Extract the pre-snapshot index tree OID from a WIP commit message.
fn parse_index_tree_trailer(message: &str) -> Option<git2::Oid> {
    message
        .lines()
        .find_map(|line| line.strip_prefix(INDEX_TREE_TRAILER))
        .and_then(|v| git2::Oid::from_str(v.trim()).ok())
}

/// Whether a repository has uncommitted changes (staged, unstaged, or
/// untracked). Local copy of the `sandbox_ops` helper — this module stays
/// self-contained.
fn is_dirty(repo: &git2::Repository) -> Result<bool> {
    let mut opts = git2::StatusOptions::new();
    opts.include_untracked(true)
        .recurse_untracked_dirs(true)
        .include_ignored(false);
    let statuses = repo
        .statuses(Some(&mut opts))
        .map_err(|e| Error::Internal(format!("git status failed: {e}")))?;
    Ok(!statuses.is_empty())
}

/// Resolve a commit signature, falling back to a stable default identity when
/// the user has no `user.name`/`user.email` configured (parity with the
/// `sandbox_ops` helper of the same name).
fn resolve_signature(repo: &git2::Repository) -> Result<git2::Signature<'static>> {
    match repo.signature() {
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

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::{AgentId, WorkspaceId, WorkspaceStatus};
    use intent_store::SandboxStatus;
    use std::fs;

    fn now_iso() -> String {
        intent_core::now_iso()
    }

    /// Init a repo on branch `main` with one committed file.
    fn init_repo(repo_path: &Path) {
        fs::create_dir_all(repo_path).unwrap();
        let repo = git2::Repository::init_opts(
            repo_path,
            git2::RepositoryInitOptions::new().initial_head("main"),
        )
        .unwrap();
        fs::write(repo_path.join("README.md"), "hello\n").unwrap();
        let sig = git2::Signature::now("Test", "test@example.com").unwrap();
        let tree_id = {
            let mut index = repo.index().unwrap();
            index.add_path(Path::new("README.md")).unwrap();
            index.write().unwrap();
            index.write_tree().unwrap()
        };
        let tree = repo.find_tree(tree_id).unwrap();
        repo.commit(Some("HEAD"), &sig, &sig, "Initial commit", &tree, &[])
            .unwrap();
    }

    fn temp_repo(name: &str) -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::TempDir::new().unwrap();
        let repo_path = dir.path().join(name);
        init_repo(&repo_path);
        (dir, repo_path)
    }

    /// Stage and commit a single file; returns the commit SHA.
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

    fn head_sha(repo_path: &Path) -> String {
        let repo = git2::Repository::open(repo_path).unwrap();
        let sha = repo.head().unwrap().target().unwrap().to_string();
        sha
    }

    /// Status entries as (path, staged, wt_modified, untracked) tuples,
    /// sorted, for exact before/after comparisons.
    fn status_fingerprint(repo_path: &Path) -> Vec<(String, bool, bool, bool)> {
        let repo = git2::Repository::open(repo_path).unwrap();
        let mut opts = git2::StatusOptions::new();
        opts.include_untracked(true)
            .recurse_untracked_dirs(true)
            .include_ignored(false);
        let statuses = repo.statuses(Some(&mut opts)).unwrap();
        let mut rows: Vec<_> = statuses
            .iter()
            .map(|e| {
                let s = e.status();
                (
                    e.path().unwrap_or_default().to_string(),
                    s.intersects(
                        git2::Status::INDEX_NEW
                            | git2::Status::INDEX_MODIFIED
                            | git2::Status::INDEX_DELETED,
                    ),
                    s.intersects(git2::Status::WT_MODIFIED | git2::Status::WT_DELETED),
                    s.contains(git2::Status::WT_NEW),
                )
            })
            .collect();
        rows.sort();
        rows
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
            checkout_mode: None,
            disk_usage: None,
        }
    }

    fn sandbox_row(ws: &Workspace, agent_id: &AgentId, path: &Path, branch: &str) -> Sandbox {
        let now = now_iso();
        Sandbox {
            id: format!("sb-{}", agent_id.0),
            workspace_id: ws.id.clone(),
            agent_id: agent_id.clone(),
            path: path.to_string_lossy().to_string(),
            branch: branch.to_string(),
            base_commit_sha: head_sha(path),
            snapshot_commit_sha: None,
            status: SandboxStatus::Created,
            retry_count: 0,
            created_at: now.clone(),
            updated_at: now,
        }
    }

    /// Simulate a sandbox: clone the workspace repo and check out `sb/<agent>`.
    fn make_sandbox_clone(src: &Path, dst: &Path, branch: &str) {
        let out = Command::new("git")
            .arg("clone")
            .arg("--quiet")
            .arg(src)
            .arg(dst)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let repo = git2::Repository::open(dst).unwrap();
        let head = repo.head().unwrap().peel_to_commit().unwrap();
        repo.branch(branch, &head, false).unwrap();
        repo.set_head(&format!("refs/heads/{branch}")).unwrap();
    }

    /// Refs listed in a bundle header, via `git bundle list-heads`.
    fn bundle_refs(cwd: &Path, bundle: &Path) -> Vec<String> {
        let out = Command::new("git")
            .arg("bundle")
            .arg("list-heads")
            .arg(bundle)
            .current_dir(cwd)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8_lossy(&out.stdout)
            .lines()
            .filter_map(|l| l.split_whitespace().nth(1).map(|s| s.to_string()))
            .collect()
    }

    fn repo_ref_names(repo_path: &Path) -> Vec<String> {
        let repo = git2::Repository::open(repo_path).unwrap();
        let names: Vec<String> = repo
            .references()
            .unwrap()
            .names()
            .map(|n| n.unwrap().to_string())
            .collect();
        names
    }

    #[test]
    fn snapshot_wip_clean_repo_is_noop() {
        let (_dir, repo) = temp_repo("clean");
        assert_eq!(snapshot_wip(&repo).unwrap(), None);
        assert!(
            !unwind_wip(&repo).unwrap(),
            "nothing to unwind on clean repo"
        );
    }

    #[test]
    fn snapshot_and_unwind_roundtrip_preserves_staged_unstaged_untracked() {
        let (_dir, repo) = temp_repo("dirty");
        let base = head_sha(&repo);

        // staged change
        fs::write(repo.join("staged.txt"), "staged\n").unwrap();
        {
            let r = git2::Repository::open(&repo).unwrap();
            let mut index = r.index().unwrap();
            index.add_path(Path::new("staged.txt")).unwrap();
            index.write().unwrap();
        }
        // unstaged modification of a tracked file
        fs::write(repo.join("README.md"), "modified\n").unwrap();
        // untracked file
        fs::write(repo.join("untracked.txt"), "untracked\n").unwrap();

        let before = status_fingerprint(&repo);
        let wip = snapshot_wip(&repo).unwrap().expect("dirty repo snapshots");
        assert_eq!(head_sha(&repo), wip);
        {
            let r = git2::Repository::open(&repo).unwrap();
            let head = r.head().unwrap().peel_to_commit().unwrap();
            assert!(head.message().unwrap().starts_with(TRANSFER_WIP_SENTINEL));
        }

        assert!(unwind_wip(&repo).unwrap());
        assert_eq!(
            head_sha(&repo),
            base,
            "HEAD back at the pre-snapshot commit"
        );
        assert_eq!(
            status_fingerprint(&repo),
            before,
            "staged/unstaged/untracked split restored exactly"
        );
        // File contents intact.
        assert_eq!(
            fs::read_to_string(repo.join("README.md")).unwrap(),
            "modified\n"
        );
        assert_eq!(
            fs::read_to_string(repo.join("untracked.txt")).unwrap(),
            "untracked\n"
        );
    }

    #[test]
    fn unwind_wip_ignores_ordinary_commits() {
        let (_dir, repo) = temp_repo("ordinary");
        commit_file(&repo, "a.txt", "a\n", "feat: ordinary commit");
        let head = head_sha(&repo);
        assert!(!unwind_wip(&repo).unwrap());
        assert_eq!(head_sha(&repo), head);
    }

    #[test]
    fn bundle_clean_workspace_contains_branch_and_cleans_temp_refs() {
        let (dir, repo) = temp_repo("ws-clean");
        let ws = workspace_for_repo(&repo);
        let staging = dir.path().join("staging");

        let (bundle_path, manifest) = create_transfer_bundle(&ws, &[], &staging).unwrap();
        assert!(bundle_path.exists());
        assert_eq!(manifest.workspace_branch, "main");
        assert_eq!(manifest.workspace_bundle_ref, "refs/heads/main");
        assert_eq!(manifest.workspace_head_sha, head_sha(&repo));
        assert_eq!(manifest.workspace_wip_commit_sha, None);
        assert!(manifest.sandboxes.is_empty());

        let refs = bundle_refs(&repo, &bundle_path);
        assert!(refs.contains(&"refs/heads/main".to_string()), "{refs:?}");

        // Clone from the bundle and check the file arrived.
        let clone_dst = dir.path().join("from-bundle");
        let out = Command::new("git")
            .arg("clone")
            .arg("--quiet")
            .arg("-b")
            .arg("main")
            .arg(&bundle_path)
            .arg(&clone_dst)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(
            fs::read_to_string(clone_dst.join("README.md")).unwrap(),
            "hello\n"
        );

        // No temporary transfer refs left behind.
        assert!(
            repo_ref_names(&repo)
                .iter()
                .all(|r| !r.starts_with(TRANSFER_REF_NS)),
            "temp refs cleaned up"
        );
    }

    #[test]
    fn bundle_dirty_workspace_snapshots_wip_and_unwinds() {
        let (dir, repo) = temp_repo("ws-dirty");
        let base = head_sha(&repo);
        fs::write(repo.join("wip.txt"), "wip\n").unwrap();
        let before = status_fingerprint(&repo);

        let ws = workspace_for_repo(&repo);
        let (bundle_path, manifest) =
            create_transfer_bundle(&ws, &[], &dir.path().join("staging")).unwrap();
        let wip = manifest
            .workspace_wip_commit_sha
            .clone()
            .expect("dirty worktree produces a WIP commit");
        assert_eq!(manifest.workspace_head_sha, wip);
        assert_eq!(head_sha(&repo), wip, "WIP left in place on success");
        assert!(bundle_path.exists());

        // The caller unwinds after the export settles.
        assert!(unwind_wip(&repo).unwrap());
        assert_eq!(head_sha(&repo), base);
        assert_eq!(status_fingerprint(&repo), before);
    }

    #[test]
    fn bundle_includes_base_and_sandbox_branches() {
        let (dir, repo) = temp_repo("ws-sb");
        let ws = {
            let mut ws = workspace_for_repo(&repo);
            ws.base_ref = Some("main".to_string());
            ws
        };

        // Sandbox with a committed change plus a dirty file.
        let agent = AgentId::new();
        let branch = format!("sb/{}", agent.0);
        let sb_path = dir.path().join("sandbox");
        make_sandbox_clone(&repo, &sb_path, &branch);
        commit_file(&sb_path, "sb.txt", "sandbox work\n", "feat: sandbox commit");
        fs::write(sb_path.join("sb-wip.txt"), "sandbox wip\n").unwrap();
        let sb = sandbox_row(&ws, &agent, &sb_path, &branch);

        let (bundle_path, manifest) =
            create_transfer_bundle(&ws, &[sb], &dir.path().join("staging")).unwrap();

        assert_eq!(manifest.base_ref.as_deref(), Some("main"));
        assert_eq!(manifest.base_bundle_ref.as_deref(), Some(BASE_BUNDLE_REF));
        assert_eq!(manifest.base_sha.as_deref(), Some(head_sha(&repo).as_str()));

        assert_eq!(manifest.sandboxes.len(), 1);
        let entry = &manifest.sandboxes[0];
        assert_eq!(entry.agent_id, agent.0);
        assert_eq!(entry.branch, branch);
        assert_eq!(entry.bundle_ref, sandbox_bundle_ref(&agent.0));
        let sb_wip = entry
            .wip_commit_sha
            .clone()
            .expect("dirty sandbox snapshots");
        assert_eq!(entry.head_sha, sb_wip);
        assert_eq!(head_sha(&sb_path), sb_wip);

        let refs = bundle_refs(&repo, &bundle_path);
        assert!(refs.contains(&"refs/heads/main".to_string()), "{refs:?}");
        assert!(refs.contains(&BASE_BUNDLE_REF.to_string()), "{refs:?}");
        assert!(refs.contains(&entry.bundle_ref), "{refs:?}");

        // The sandbox WIP content is reachable from the bundle: fetch the
        // sandbox ref into a fresh clone and check the dirty file arrived.
        let clone_dst = dir.path().join("from-bundle");
        let out = Command::new("git")
            .arg("clone")
            .arg("--quiet")
            .arg("-b")
            .arg("main")
            .arg(&bundle_path)
            .arg(&clone_dst)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let out = Command::new("git")
            .arg("fetch")
            .arg("--quiet")
            .arg(bundle_path.to_str().unwrap())
            .arg(format!("{}:refs/heads/{branch}", entry.bundle_ref))
            .current_dir(&clone_dst)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        let cloned = git2::Repository::open(&clone_dst).unwrap();
        let tip = cloned
            .find_reference(&format!("refs/heads/{branch}"))
            .unwrap()
            .peel_to_commit()
            .unwrap();
        assert_eq!(tip.id().to_string(), sb_wip);
        assert!(tip.tree().unwrap().get_name("sb-wip.txt").is_some());

        // Temp refs cleaned from the source worktree.
        assert!(
            repo_ref_names(&repo)
                .iter()
                .all(|r| !r.starts_with(TRANSFER_REF_NS)),
            "temp refs cleaned up"
        );
        // Sandbox WIP left in place on success for the caller to unwind.
        assert!(unwind_wip(&sb_path).unwrap());
    }

    #[test]
    fn bundle_skips_missing_sandbox_directory() {
        let (dir, repo) = temp_repo("ws-missing-sb");
        let ws = workspace_for_repo(&repo);
        let agent = AgentId::new();
        let sb = sandbox_row(&ws, &agent, &repo, &format!("sb/{}", agent.0));
        let mut missing = sb.clone();
        missing.path = dir.path().join("gone").to_string_lossy().to_string();

        let (_bundle, manifest) =
            create_transfer_bundle(&ws, &[missing], &dir.path().join("staging")).unwrap();
        assert!(manifest.sandboxes.is_empty(), "missing sandbox skipped");
    }

    #[test]
    fn bundle_failure_restores_source_exactly() {
        let (dir, repo) = temp_repo("ws-fail");
        let base = head_sha(&repo);
        fs::write(repo.join("wip.txt"), "wip\n").unwrap();
        let before = status_fingerprint(&repo);

        // A sandbox whose path exists but is not a git repo forces a failure
        // AFTER the workspace WIP snapshot has been taken.
        let bogus = dir.path().join("not-a-repo");
        fs::create_dir_all(&bogus).unwrap();
        let ws = workspace_for_repo(&repo);
        let agent = AgentId::new();
        let mut sb = sandbox_row(&ws, &agent, &repo, &format!("sb/{}", agent.0));
        sb.path = bogus.to_string_lossy().to_string();

        let staging = dir.path().join("staging");
        let err = create_transfer_bundle(&ws, &[sb], &staging);
        assert!(err.is_err());

        // Worktree restored exactly as found; no bundle, no temp refs.
        assert_eq!(head_sha(&repo), base, "WIP snapshot unwound on failure");
        assert_eq!(status_fingerprint(&repo), before);
        assert!(!staging.join(format!("{}.bundle", ws.id.0)).exists());
        assert!(
            repo_ref_names(&repo)
                .iter()
                .all(|r| !r.starts_with(TRANSFER_REF_NS)),
            "temp refs cleaned up"
        );
    }
}
