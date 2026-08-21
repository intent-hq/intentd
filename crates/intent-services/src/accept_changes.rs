//! Wire-policy glue for the `accept-changes.*` methods (§5.18).
//!
//! Pure mapping/validation ported from the TS `AcceptChangesService`: the
//! `WorkspaceGitStatus` shaping, the `isValidGitRemoteUrl` allowlist, action
//! validation, the `prepare` stats, and the `AcceptChangesResult` step/result
//! builders. The git/forge side-effects (commit, push, create-PR, merge) live on
//! `Services` in `lib.rs`; this module owns only the parity-critical, network-
//! free glue so it stays unit-testable.

use std::path::Path;

use intent_core::{Error, PullRequestStatus, Result, Workspace};
use serde_json::{json, Map, Value};

use crate::file_tracking_ops::commit_to_value;

/// The accept-changes `action` set (PROTOCOL §5.18; the TS zod enum).
pub(crate) const ACTIONS: [&str; 9] = [
    "commit",
    "push",
    "create-pr",
    "merge",
    "export",
    "undo-push",
    "undo-commit",
    "reset-to-trunk",
    "rebase-onto-trunk",
];

/// Validate an `action` against the canonical set, returning it on success.
/// An unknown value is malformed params (`-32602`), mirroring the TS zod enum.
pub(crate) fn validate_action(action: &str) -> Result<&'static str> {
    ACTIONS
        .into_iter()
        .find(|a| *a == action)
        .ok_or_else(|| Error::InvalidParams(format!("invalid action: {action}")))
}

/// Branch-name guard mirroring the TS `SAFE_REF_PATTERN`
/// (`/^[a-zA-Z0-9._\/][a-zA-Z0-9._\-\/]*$/`): a non-empty ref whose first
/// character is alphanumeric or one of `._/`, with the remaining characters also
/// allowing `-`. Used to reject unsafe branch names before the
/// reset-to-trunk / rebase-onto-trunk / merge handlers act on them.
pub(crate) fn is_safe_ref(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first.is_ascii_alphanumeric() || matches!(first, '.' | '_' | '/')) {
        return false;
    }
    chars.all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | '/'))
}

/// Characters that are dangerous in shell contexts — any remote URL containing
/// one is rejected (defense-in-depth; ports the TS `SHELL_UNSAFE_CHARS`).
fn has_shell_unsafe_char(url: &str) -> bool {
    url.chars().any(|c| {
        c.is_whitespace()
            || matches!(
                c,
                ';' | '|'
                    | '&'
                    | '`'
                    | '$'
                    | '('
                    | ')'
                    | '{'
                    | '}'
                    | '['
                    | ']'
                    | '!'
                    | '#'
                    | '~'
                    | '<'
                    | '>'
                    | '\''
                    | '"'
                    | '\\'
            )
    })
}

/// Strict allowlist for git remote URLs (ports `isValidGitRemoteUrl`): only
/// `https?://`, `git@host:path`, `ssh://`, or `git://`, and never a URL with a
/// shell-unsafe character.
pub(crate) fn is_valid_git_remote_url(url: &str) -> bool {
    if has_shell_unsafe_char(url) {
        return false;
    }
    if url.starts_with("https://") || url.starts_with("http://") {
        return url.len() > "https://".len();
    }
    if url.starts_with("ssh://") || url.starts_with("git://") {
        return url.len() > "ssh://".len();
    }
    // `git@<host>:<path>` SSH shorthand.
    if let Some(rest) = url.strip_prefix("git@") {
        if let Some((host, path)) = rest.split_once(':') {
            return !host.is_empty() && !path.is_empty();
        }
    }
    false
}

/// Parse `(owner, repo)` from a GitHub remote URL (`github.com[:/]owner/repo`),
/// tolerating a trailing `.git` and repo names with dots.
pub(crate) fn parse_owner_repo(url: &str) -> Option<(String, String)> {
    let idx = url.find("github.com")?;
    let after = &url[idx + "github.com".len()..];
    let after = after
        .strip_prefix(':')
        .or_else(|| after.strip_prefix('/'))?;
    let mut parts = after.splitn(2, '/');
    let owner = parts.next()?.trim();
    let repo_raw = parts.next()?.trim();
    if owner.is_empty() || repo_raw.is_empty() {
        return None;
    }
    let repo = repo_raw
        .trim_end_matches('/')
        .strip_suffix(".git")
        .unwrap_or_else(|| repo_raw.trim_end_matches('/'));
    if repo.is_empty() {
        return None;
    }
    Some((owner.to_string(), repo.to_string()))
}

/// The trunk branch name for a workspace: its `baseRef` (with a leading
/// `origin/` stripped) or `main`.
pub(crate) fn trunk_branch(ws: &Workspace) -> String {
    ws.base_ref
        .as_deref()
        .map(|r| r.strip_prefix("origin/").unwrap_or(r))
        .filter(|s| !s.is_empty())
        .unwrap_or("main")
        .to_string()
}

/// Map a persisted [`PullRequestStatus`] to the `WorkspaceGitStatus.existingPR`
/// `state` wire word.
fn pr_state_word(status: PullRequestStatus) -> &'static str {
    match status {
        PullRequestStatus::Open => "open",
        PullRequestStatus::Closed => "closed",
        PullRequestStatus::Merged => "merged",
        PullRequestStatus::Draft => "draft",
    }
}

/// Build the `existingPR` fragment from the workspace's linked PR snapshot.
fn existing_pr_value(ws: &Workspace) -> Option<Value> {
    let pr = ws.active_pull_request.as_ref()?;
    Some(json!({
        "number": pr.number,
        "url": pr.url,
        "htmlUrl": pr.url,
        "title": pr.title,
        "state": pr_state_word(pr.status),
    }))
}

/// Build the `WorkspaceGitStatus` JSON (§5.18) for a local repository worktree.
///
/// Network-free: ahead/behind + `isPushed` are derived from the local
/// `refs/remotes/origin/*` refs (advanced by [`intent_git::push`]). `localCommits`
/// is the `trunk..HEAD` first-parent range with attribution; `existingPR` reflects
/// the workspace's linked-PR snapshot.
pub(crate) fn build_git_status_value(worktree: &Path, ws: &Workspace) -> Result<Value> {
    build_git_status_value_with(worktree, ws, None)
}

/// [`build_git_status_value`] with an optional pre-computed working-tree
/// status, so callers that already paid (or coalesced onto) a
/// [`intent_git::status::status`] scan do not run a second one. `None` scans
/// inline.
pub(crate) fn build_git_status_value_with(
    worktree: &Path,
    ws: &Workspace,
    scanned: Option<std::sync::Arc<intent_core::GitStatus>>,
) -> Result<Value> {
    let trunk = trunk_branch(ws);

    // No git dir → the minimal status (branch from the workspace, zeros).
    if !worktree.join(".git").exists() {
        return Ok(minimal_status_value(ws, &trunk));
    }

    let started = std::time::Instant::now();
    // `status_ms` only attributes a scan this call actually ran; with an
    // injected status the scan was paid by (or shared with) another caller's
    // flight, so it logs as absent rather than a misleading ~0.
    let (status, status_ms) = if let Some(s) = scanned {
        (s, None)
    } else {
        let s = std::sync::Arc::new(intent_git::status::status(worktree)?);
        (
            s,
            Some(u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)),
        )
    };
    let branch = if status.branch.is_empty() {
        ws.branch.clone()
    } else {
        status.branch.clone()
    };

    let remote_started = std::time::Instant::now();
    let remote_url = intent_git::remote::origin_url(worktree)?;
    let has_remote = remote_url.is_some();
    let is_pushed = has_remote
        && intent_git::remote::remote_tracking_exists(worktree, "origin", &branch).unwrap_or(false);
    let remote_ms = u64::try_from(remote_started.elapsed().as_millis()).unwrap_or(u64::MAX);

    let trunk_started = std::time::Instant::now();
    let trunk_ref = resolve_trunk_ref(worktree, &trunk, has_remote);
    let trunk_ms = u64::try_from(trunk_started.elapsed().as_millis()).unwrap_or(u64::MAX);

    let ahead_behind_started = std::time::Instant::now();
    let (ahead, behind) = intent_git::remote::ahead_behind(worktree, &trunk_ref)?;
    let ahead_behind_ms =
        u64::try_from(ahead_behind_started.elapsed().as_millis()).unwrap_or(u64::MAX);

    let history_started = std::time::Instant::now();
    // Metadata-only walk: no per-commit tree diffs. `localCommits` entries omit
    // `files`/`filesChanged`; the FE fetches per-file data on demand via
    // `git.commitDetails` (PROTOCOL §5.6).
    let commits = intent_git::history::history_since(worktree, Some(&trunk_ref), 200, false)?;
    let local_commits: Vec<Value> = commits.iter().map(commit_to_value).collect();
    let history_ms = u64::try_from(history_started.elapsed().as_millis()).unwrap_or(u64::MAX);

    let mut uncommitted = std::collections::HashSet::new();
    let mut staged = std::collections::HashSet::new();
    for f in &status.files {
        uncommitted.insert(f.path.clone());
        if f.staged {
            staged.insert(f.path.clone());
        }
    }

    let (owner, repo) = remote_url
        .as_deref()
        .and_then(parse_owner_repo)
        .map_or((None, None), |(o, r)| (Some(o), Some(r)));

    tracing::debug!(
        files = status.files.len(),
        local_commits = local_commits.len(),
        status_ms,
        remote_ms,
        trunk_resolve_ms = trunk_ms,
        ahead_behind_ms,
        history_walk_ms = history_ms,
        total_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX),
        "accept-changes.getStatus: status scan + remote/trunk resolve + history walk"
    );

    Ok(json!({
        "branch": branch,
        "trunkBranch": trunk,
        "aheadOfTrunk": ahead,
        "behindTrunk": behind,
        "hasRemote": has_remote,
        "isPushed": is_pushed,
        "uncommittedCount": uncommitted.len(),
        "stagedCount": staged.len(),
        "localCommits": local_commits,
        "existingPR": existing_pr_value(ws),
        "remoteUrl": remote_url,
        "owner": owner,
        "repo": repo,
    }))
}

/// The status for a workspace whose worktree is not (yet) a git repository: the
/// branch from the workspace metadata, everything else zeroed, but still carrying
/// any linked-PR snapshot.
pub(crate) fn minimal_status_value(ws: &Workspace, trunk: &str) -> Value {
    json!({
        "branch": ws.branch,
        "trunkBranch": trunk,
        "aheadOfTrunk": 0,
        "behindTrunk": 0,
        "hasRemote": false,
        "isPushed": false,
        "uncommittedCount": 0,
        "stagedCount": 0,
        "localCommits": [],
        "existingPR": existing_pr_value(ws),
        "remoteUrl": Value::Null,
        "owner": Value::Null,
        "repo": Value::Null,
    })
}

/// Pick the ref to compare the branch against: `origin/<trunk>` when that
/// tracking ref exists locally, else the local `<trunk>`.
fn resolve_trunk_ref(worktree: &Path, trunk: &str, has_remote: bool) -> String {
    if has_remote
        && intent_git::remote::remote_tracking_exists(worktree, "origin", trunk).unwrap_or(false)
    {
        format!("origin/{trunk}")
    } else {
        trunk.to_string()
    }
}

/// Build an invalid `PrepareResult` (`valid:false`) carrying a single error and
/// zeroed stats — the TS early returns for missing workspace / path.
pub(crate) fn prepare_invalid(error: &str) -> Value {
    json!({
        "valid": false,
        "warnings": [],
        "errors": [error],
        "filesCount": 0,
        "additions": 0,
        "deletions": 0,
        "files": [],
    })
}

/// One `PrepareResult.files` entry (`{ path, additions, deletions, staged }`).
struct PrepFile {
    path: String,
    additions: i64,
    deletions: i64,
    staged: bool,
}

/// Build the `PrepareResult` (§5.18): committed range stats feed the totals;
/// staged (`HEAD→index`) and unstaged (`index→workdir`, incl. untracked) changes
/// populate `files` (a file changed in both stages yields two entries, matching
/// the TS), plus suggested commit/PR text derived from the trunk..HEAD commits.
pub(crate) fn build_prepare_value(
    worktree: &Path,
    ws: &Workspace,
    action: &str,
    files_filter: Option<&[String]>,
) -> Result<Value> {
    let trunk = trunk_branch(ws);
    let has_git = worktree.join(".git").exists();
    let has_remote = has_git && intent_git::remote::origin_url(worktree)?.is_some();
    let trunk_ref = resolve_trunk_ref(worktree, &trunk, has_remote);

    let mut warnings: Vec<String> = Vec::new();
    let mut errors: Vec<String> = Vec::new();
    if (action == "push" || action == "create-pr") && !has_remote {
        errors.push("No remote configured for this repository".to_string());
    }
    if action == "merge" && has_git {
        let (_ahead, behind) = intent_git::remote::ahead_behind(worktree, &trunk_ref)?;
        if behind > 0 {
            warnings.push(format!(
                "Branch is {behind} commits behind {trunk}. Consider rebasing first.",
            ));
        }
    }

    let mut additions = 0i64;
    let mut deletions = 0i64;
    let mut files: Vec<PrepFile> = Vec::new();

    if has_git {
        // Committed range stats (trunk...HEAD): counted but not listed as files.
        for fd in intent_git::diff::diff_range(worktree, &trunk_ref)? {
            additions += i64::try_from(fd.additions).expect("value fits in i64");
            deletions += i64::try_from(fd.deletions).expect("value fits in i64");
        }
        // Staged changes (HEAD→index).
        for fd in intent_git::diff::diff_head_to_index(worktree)? {
            additions += i64::try_from(fd.additions).expect("value fits in i64");
            deletions += i64::try_from(fd.deletions).expect("value fits in i64");
            files.push(PrepFile {
                path: fd.path,
                additions: i64::try_from(fd.additions).expect("value fits in i64"),
                deletions: i64::try_from(fd.deletions).expect("value fits in i64"),
                staged: true,
            });
        }
        // Unstaged changes (index→workdir, incl. untracked).
        for fd in intent_git::diff::diff_index_to_workdir(worktree)? {
            additions += i64::try_from(fd.additions).expect("value fits in i64");
            deletions += i64::try_from(fd.deletions).expect("value fits in i64");
            files.push(PrepFile {
                path: fd.path,
                additions: i64::try_from(fd.additions).expect("value fits in i64"),
                deletions: i64::try_from(fd.deletions).expect("value fits in i64"),
                staged: false,
            });
        }
        // Any status file not yet represented (same path+staged) → 0/0 entry.
        if let Ok(status) = intent_git::status::status(worktree) {
            for f in status.files {
                if !files
                    .iter()
                    .any(|p| p.path == f.path && p.staged == f.staged)
                {
                    files.push(PrepFile {
                        path: f.path,
                        additions: 0,
                        deletions: 0,
                        staged: f.staged,
                    });
                }
            }
        }
    }

    if let Some(filter) = files_filter {
        if !filter.is_empty() {
            files.retain(|f| filter.iter().any(|p| p == &f.path));
        }
    }

    let mut unique: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for f in &files {
        unique.insert(f.path.as_str());
    }
    let files_count = unique.len();

    // Only commit messages are needed here — skip the per-commit tree diffs.
    let commit_messages: Vec<String> =
        intent_git::history::history_since(worktree, Some(&trunk_ref), 200, false)
            .unwrap_or_default()
            .into_iter()
            .map(|c| c.message)
            .collect();
    let suggested_commit_message = if commit_messages.len() == 1 {
        commit_messages[0].clone()
    } else {
        commit_messages.join("\n- ")
    };
    let title = if ws.title.is_empty() {
        "Changes".to_string()
    } else {
        ws.title.clone()
    };
    let suggested_pr_title = if commit_messages.len() == 1 {
        commit_messages[0].clone()
    } else {
        format!("{title} ({} commits)", commit_messages.len())
    };

    let files_json: Vec<Value> = files
        .iter()
        .map(|f| {
            json!({
                "path": f.path,
                "additions": f.additions,
                "deletions": f.deletions,
                "staged": f.staged,
            })
        })
        .collect();

    Ok(json!({
        "valid": errors.is_empty(),
        "warnings": warnings,
        "errors": errors,
        "suggestedCommitMessage": suggested_commit_message,
        "suggestedPRTitle": suggested_pr_title,
        "suggestedPRBody": "",
        "filesCount": files_count,
        "additions": additions,
        "deletions": deletions,
        "files": files_json,
    }))
}

/// Build one `AcceptChangesResult` step (`{ id, name, status, message?, error? }`).
pub(crate) fn step(
    id: &str,
    name: &str,
    status: &str,
    message: Option<&str>,
    error: Option<&str>,
) -> Value {
    let mut obj = Map::new();
    obj.insert("id".to_string(), json!(id));
    obj.insert("name".to_string(), json!(name));
    obj.insert("status".to_string(), json!(status));
    if let Some(m) = message {
        obj.insert("message".to_string(), json!(m));
    }
    if let Some(e) = error {
        obj.insert("error".to_string(), json!(e));
    }
    Value::Object(obj)
}

/// Assemble an `AcceptChangesResult` (`{ success, steps, result?, error? }`).
pub(crate) fn accept_result(
    success: bool,
    steps: Vec<Value>,
    result: Option<Value>,
    error: Option<String>,
) -> Value {
    let mut obj = Map::new();
    obj.insert("success".to_string(), json!(success));
    obj.insert("steps".to_string(), Value::Array(steps));
    if let Some(r) = result {
        obj.insert("result".to_string(), r);
    }
    if let Some(e) = error {
        obj.insert("error".to_string(), json!(e));
    }
    Value::Object(obj)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use git2::{Repository, Signature};
    use intent_core::{
        now_iso, PullRequestInfo, PullRequestStatus, Workspace, WorkspaceActivity,
        WorkspaceAttention, WorkspaceId, WorkspaceStatus,
    };

    // ----- Workspace + repo helpers (test-only) -----

    /// Build a minimally-populated `Workspace` for tests. Optional fields can be
    /// patched by callers after construction.
    fn mk_workspace() -> Workspace {
        let ts = now_iso();
        Workspace {
            id: WorkspaceId::from("ws-test"),
            title: "WS".to_string(),
            branch: "feature/x".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: WorkspaceStatus::Active,
            status_message: None,
            status_image_asset_id: None,
            activity: WorkspaceActivity::Idle,
            attention: WorkspaceAttention::None,
            created_at: ts.clone(),
            updated_at: ts,
            last_activity: None,
            tags: vec![],
            path: None,
            repository_path: None,
            repository_owner: None,
            repository_name: None,
            worktree_path: None,
            scope: None,
            skip_worktree: false,
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

    fn mk_pr(status: PullRequestStatus) -> PullRequestInfo {
        let ts = now_iso();
        PullRequestInfo {
            id: "pr-1".to_string(),
            number: 42,
            url: "https://example.com/pr/42".to_string(),
            title: "Test PR".to_string(),
            status,
            created_at: ts.clone(),
            updated_at: ts,
            base_ref: None,
            head_ref: None,
            head_sha: None,
            author: None,
            mergeable: None,
            mergeable_state: None,
            is_draft: None,
        }
    }

    /// Self-cleaning temp directory tied to a unique counter so parallel tests
    /// can each get their own worktree.
    struct TempDir {
        path: PathBuf,
    }

    impl TempDir {
        fn new(tag: &str) -> Self {
            static N: AtomicU64 = AtomicU64::new(0);
            let nanos = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let n = N.fetch_add(1, Ordering::SeqCst);
            let path = std::env::temp_dir().join(format!("intent-svc-ac-{tag}-{nanos}-{n}"));
            std::fs::create_dir_all(&path).unwrap();
            Self { path }
        }
        fn path(&self) -> &Path {
            &self.path
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    /// Initialize a real git repo with a seed commit on `main`. Returns the
    /// owning `TempDir`.
    fn init_repo(tag: &str) -> TempDir {
        let dir = TempDir::new(tag);
        let repo = Repository::init(dir.path()).unwrap();
        {
            let mut cfg = repo.config().unwrap();
            cfg.set_str("user.name", "Test").unwrap();
            cfg.set_str("user.email", "test@example.com").unwrap();
        }
        // Move HEAD to refs/heads/main so the seed commit lives on `main` even
        // when the default init branch differs.
        repo.set_head("refs/heads/main").unwrap();
        // Seed commit so HEAD exists.
        std::fs::write(dir.path().join("seed.txt"), "seed\n").unwrap();
        let mut index = repo.index().unwrap();
        index.add_path(Path::new("seed.txt")).unwrap();
        index.write().unwrap();
        let tree_oid = index.write_tree().unwrap();
        let tree = repo.find_tree(tree_oid).unwrap();
        let sig = Signature::now("Test", "test@example.com").unwrap();
        repo.commit(Some("HEAD"), &sig, &sig, "seed", &tree, &[])
            .unwrap();
        dir
    }

    /// Add a tracked + committed file on top of HEAD with the supplied message.
    fn commit_file(worktree: &Path, rel: &str, contents: &str, msg: &str) {
        std::fs::write(worktree.join(rel), contents).unwrap();
        let repo = Repository::open(worktree).unwrap();
        let mut index = repo.index().unwrap();
        index.add_path(Path::new(rel)).unwrap();
        index.write().unwrap();
        let tree_oid = index.write_tree().unwrap();
        let tree = repo.find_tree(tree_oid).unwrap();
        let sig = Signature::now("Test", "test@example.com").unwrap();
        let head = repo.head().unwrap().target().unwrap();
        let parent = repo.find_commit(head).unwrap();
        repo.commit(Some("HEAD"), &sig, &sig, msg, &tree, &[&parent])
            .unwrap();
    }

    /// Create a branch off HEAD and check it out.
    fn checkout_new_branch(worktree: &Path, name: &str) {
        let repo = Repository::open(worktree).unwrap();
        let head = repo.head().unwrap().target().unwrap();
        let commit = repo.find_commit(head).unwrap();
        repo.branch(name, &commit, false).unwrap();
        repo.set_head(&format!("refs/heads/{name}")).unwrap();
        let mut opts = git2::build::CheckoutBuilder::new();
        opts.force();
        repo.checkout_head(Some(&mut opts)).unwrap();
    }

    // ----- ACTIONS / validate_action -----

    #[test]
    fn validates_actions() {
        assert_eq!(validate_action("commit").unwrap(), "commit");
        assert_eq!(validate_action("create-pr").unwrap(), "create-pr");
        assert!(validate_action("nope").is_err());
    }

    #[test]
    fn validate_action_accepts_every_listed_action() {
        // Every canonical action returns itself.
        for a in ACTIONS {
            assert_eq!(validate_action(a).unwrap(), a);
        }
        assert_eq!(ACTIONS.len(), 9);
    }

    #[test]
    fn validate_action_rejects_invalid_with_invalid_params_error() {
        // Empty, casing, and stray separators all map to -32602 (InvalidParams).
        for bad in ["", "Commit", " commit", "commit ", "merge-pr", "create_pr"] {
            let err = validate_action(bad).unwrap_err();
            assert!(matches!(err, Error::InvalidParams(_)), "{bad}: {err:?}");
            if let Error::InvalidParams(msg) = err {
                assert!(msg.contains(bad), "msg `{msg}` should mention `{bad}`");
            }
        }
    }

    // ----- is_safe_ref -----

    #[test]
    fn safe_ref_matches_ts_pattern() {
        assert!(is_safe_ref("main"));
        assert!(is_safe_ref("feature/foo-bar.baz_1"));
        assert!(is_safe_ref("origin/main"));
        assert!(is_safe_ref(".hidden"));
        // First char cannot be '-'; shell-unsafe chars are rejected.
        assert!(!is_safe_ref("-rf"));
        assert!(!is_safe_ref(""));
        assert!(!is_safe_ref("a b"));
        assert!(!is_safe_ref("a;rm -rf"));
        assert!(!is_safe_ref("$(whoami)"));
    }

    #[test]
    fn safe_ref_first_char_class() {
        // Allowed first chars: alphanumeric or one of '.' '_' '/'.
        assert!(is_safe_ref("0"));
        assert!(is_safe_ref("_a"));
        assert!(is_safe_ref("/a"));
        // Hyphen is allowed in interior but not as first char.
        assert!(!is_safe_ref("-name"));
        assert!(is_safe_ref("a-b-c"));
    }

    #[test]
    fn safe_ref_rejects_interior_unsafe_chars() {
        // Any character outside the allowed alphabet rejects the whole ref.
        for bad in [
            "a$b", "a;b", "a|b", "a&b", "a*b", "a?b", "a@b", "a%b", "a:b", "a\tb",
        ] {
            assert!(!is_safe_ref(bad), "{bad} should be unsafe");
        }
    }

    // ----- is_valid_git_remote_url -----

    #[test]
    fn url_allowlist_matches_ts() {
        assert!(is_valid_git_remote_url("https://github.com/o/r.git"));
        assert!(is_valid_git_remote_url("git@github.com:o/r.git"));
        assert!(is_valid_git_remote_url("ssh://git@host/o/r.git"));
        assert!(is_valid_git_remote_url("git://host/o/r.git"));
        // Shell-unsafe characters are rejected.
        assert!(!is_valid_git_remote_url(
            "https://github.com/o/r.git; rm -rf /"
        ));
        assert!(!is_valid_git_remote_url("https://exa mple.com/r.git"));
        // Unknown schemes rejected.
        assert!(!is_valid_git_remote_url("file:///tmp/r"));
        assert!(!is_valid_git_remote_url(""));
    }

    #[test]
    fn url_allowlist_rejects_scheme_only_and_accepts_with_path() {
        // Bare schemes (no host/path) are rejected; one extra char passes.
        assert!(!is_valid_git_remote_url("https://"));
        assert!(!is_valid_git_remote_url("ssh://"));
        assert!(!is_valid_git_remote_url("git://"));
        assert!(is_valid_git_remote_url("https://a"));
        assert!(is_valid_git_remote_url("ssh://a"));
        assert!(is_valid_git_remote_url("git://a"));
    }

    #[test]
    fn url_allowlist_git_shorthand_requires_host_and_path() {
        // `git@host:path` shorthand: both host and path must be non-empty.
        assert!(is_valid_git_remote_url("git@github.com:o/r"));
        assert!(!is_valid_git_remote_url("git@:o/r"));
        assert!(!is_valid_git_remote_url("git@github.com:"));
        // Missing colon entirely → falls through and is rejected.
        assert!(!is_valid_git_remote_url("git@github.com"));
    }

    #[test]
    fn url_allowlist_rejects_every_shell_unsafe_char() {
        for c in [
            ';', '|', '&', '`', '$', '(', ')', '{', '}', '[', ']', '!', '#', '~', '<', '>', '\'',
            '"', '\\',
        ] {
            let bad = format!("https://example.com/r{c}");
            assert!(!is_valid_git_remote_url(&bad), "{bad} should be rejected");
        }
        // Whitespace too.
        assert!(!is_valid_git_remote_url("https://example.com/r\tname"));
        assert!(!is_valid_git_remote_url("https://example.com/r\nname"));
    }

    // ----- parse_owner_repo -----

    #[test]
    fn parses_owner_repo_with_dots_and_git_suffix() {
        assert_eq!(
            parse_owner_repo("https://github.com/octo/molecules.gg.git"),
            Some(("octo".into(), "molecules.gg".into()))
        );
        assert_eq!(
            parse_owner_repo("git@github.com:o/r.git"),
            Some(("o".into(), "r".into()))
        );
        assert_eq!(parse_owner_repo("https://gitlab.com/o/r.git"), None);
    }

    #[test]
    fn parses_owner_repo_handles_trailing_slash_and_no_git() {
        assert_eq!(
            parse_owner_repo("https://github.com/o/r/"),
            Some(("o".into(), "r".into()))
        );
        assert_eq!(
            parse_owner_repo("https://github.com/o/r"),
            Some(("o".into(), "r".into()))
        );
    }

    #[test]
    fn parses_owner_repo_rejects_missing_owner_or_repo() {
        // Missing path segments after `github.com:` or `github.com/`.
        assert_eq!(parse_owner_repo("https://github.com/"), None);
        assert_eq!(parse_owner_repo("https://github.com/owner"), None);
        // Empty owner.
        assert_eq!(parse_owner_repo("https://github.com//repo"), None);
        // Trailing-slash-only repo segment collapses to empty.
        assert_eq!(parse_owner_repo("https://github.com/o//"), None);
        // Bare `.git` repo name collapses to empty after suffix strip.
        assert_eq!(parse_owner_repo("https://github.com/o/.git"), None);
    }

    // ----- trunk_branch -----

    #[test]
    fn trunk_branch_defaults_to_main() {
        let ws = mk_workspace();
        assert_eq!(trunk_branch(&ws), "main");
    }

    #[test]
    fn trunk_branch_strips_origin_prefix_and_falls_back_when_empty() {
        let mut ws = mk_workspace();
        ws.base_ref = Some("origin/develop".into());
        assert_eq!(trunk_branch(&ws), "develop");
        // Plain branch name passes through.
        ws.base_ref = Some("trunk".into());
        assert_eq!(trunk_branch(&ws), "trunk");
        // Empty base_ref string falls back to "main".
        ws.base_ref = Some(String::new());
        assert_eq!(trunk_branch(&ws), "main");
    }

    // ----- existing_pr_value / minimal_status_value -----

    #[test]
    fn minimal_status_value_has_documented_shape_and_no_pr() {
        // §5.18 WorkspaceGitStatus shape — minimal worktree (no .git).
        let ws = mk_workspace();
        let v = minimal_status_value(&ws, "develop");
        assert_eq!(v["branch"], "feature/x");
        assert_eq!(v["trunkBranch"], "develop");
        assert_eq!(v["aheadOfTrunk"], 0);
        assert_eq!(v["behindTrunk"], 0);
        assert_eq!(v["hasRemote"], false);
        assert_eq!(v["isPushed"], false);
        assert_eq!(v["uncommittedCount"], 0);
        assert_eq!(v["stagedCount"], 0);
        assert!(v["localCommits"].as_array().unwrap().is_empty());
        assert!(v["existingPR"].is_null());
        assert!(v["remoteUrl"].is_null());
        assert!(v["owner"].is_null());
        assert!(v["repo"].is_null());
    }

    #[test]
    fn minimal_status_carries_existing_pr_in_lowercase_states() {
        // Each persisted status maps to its lowercase wire word (§5.18).
        for (status, word) in [
            (PullRequestStatus::Open, "open"),
            (PullRequestStatus::Closed, "closed"),
            (PullRequestStatus::Merged, "merged"),
            (PullRequestStatus::Draft, "draft"),
        ] {
            let mut ws = mk_workspace();
            ws.active_pull_request = Some(mk_pr(status));
            let v = minimal_status_value(&ws, "main");
            let pr = v["existingPR"].as_object().expect("PR object");
            assert_eq!(pr["number"], 42);
            assert_eq!(pr["url"], "https://example.com/pr/42");
            // htmlUrl always falls back to url (PROTOCOL §5.18 schema).
            assert_eq!(pr["htmlUrl"], "https://example.com/pr/42");
            assert_eq!(pr["title"], "Test PR");
            assert_eq!(pr["state"], word);
        }
    }

    #[test]
    fn build_git_status_uses_minimal_path_without_dot_git() {
        // No `.git` under worktree → minimal-status branch.
        let dir = TempDir::new("nogit");
        let ws = mk_workspace();
        let v = build_git_status_value(dir.path(), &ws).unwrap();
        assert_eq!(v["branch"], "feature/x");
        assert_eq!(v["trunkBranch"], "main");
        assert_eq!(v["hasRemote"], false);
        assert_eq!(v["isPushed"], false);
        assert!(v["localCommits"].as_array().unwrap().is_empty());
    }

    #[test]
    fn build_git_status_for_real_repo_reports_branch_and_local_commits() {
        // Real repo on `main` with two commits, no remote → branch from git,
        // hasRemote=false, isPushed=false, owner/repo null.
        let repo = init_repo("status");
        commit_file(repo.path(), "a.txt", "a\n", "add a");
        // Create a feature branch and add another commit there.
        checkout_new_branch(repo.path(), "feature/foo");
        commit_file(repo.path(), "b.txt", "b\n", "add b");

        let ws = mk_workspace();
        let v = build_git_status_value(repo.path(), &ws).unwrap();
        assert_eq!(v["branch"], "feature/foo");
        assert_eq!(v["trunkBranch"], "main");
        assert_eq!(v["hasRemote"], false);
        assert_eq!(v["isPushed"], false);
        // One commit ahead of `main` (the feature-branch commit).
        assert_eq!(v["aheadOfTrunk"], 1);
        assert_eq!(v["behindTrunk"], 0);
        // localCommits is a non-empty array of CommitWithAttribution.
        let commits = v["localCommits"].as_array().unwrap();
        assert_eq!(commits.len(), 1);
        assert_eq!(commits[0]["message"].as_str().unwrap(), "add b");
        // Metadata-only walk: no per-commit tree diffs, so `files` and
        // `filesChanged` are omitted (fetched on demand via git.commitDetails).
        assert!(commits[0].get("files").is_none());
        assert!(commits[0].get("filesChanged").is_none());
        assert!(commits[0]["hash"].is_string());
        assert_eq!(commits[0]["isPushed"], false);
        // No remote → null fields.
        assert!(v["remoteUrl"].is_null());
        assert!(v["owner"].is_null());
        assert!(v["repo"].is_null());
        assert!(v["existingPR"].is_null());
    }

    // ----- prepare_invalid -----

    #[test]
    fn prepare_invalid_has_expected_shape() {
        let v = prepare_invalid("missing workspace");
        assert_eq!(v["valid"], false);
        assert_eq!(v["filesCount"], 0);
        assert_eq!(v["additions"], 0);
        assert_eq!(v["deletions"], 0);
        assert!(v["warnings"].as_array().unwrap().is_empty());
        assert_eq!(v["errors"][0], "missing workspace");
        assert!(v["files"].as_array().unwrap().is_empty());
    }

    // ----- build_prepare_value -----

    #[test]
    fn prepare_without_git_errors_for_push_and_create_pr() {
        let dir = TempDir::new("nogit-prep");
        let ws = mk_workspace();
        for action in ["push", "create-pr"] {
            let v = build_prepare_value(dir.path(), &ws, action, None).unwrap();
            assert_eq!(v["valid"], false, "{action}");
            assert_eq!(
                v["errors"][0], "No remote configured for this repository",
                "{action}"
            );
            assert!(v["warnings"].as_array().unwrap().is_empty(), "{action}");
            assert_eq!(v["filesCount"], 0);
            assert_eq!(v["additions"], 0);
            assert_eq!(v["deletions"], 0);
            assert!(v["files"].as_array().unwrap().is_empty());
        }
    }

    #[test]
    fn prepare_without_git_is_valid_for_non_remote_actions() {
        let dir = TempDir::new("nogit-prep-ok");
        let ws = mk_workspace();
        // No-git + non-remote action → no errors emitted by the validator.
        for action in ["commit", "merge", "undo-commit", "reset-to-trunk"] {
            let v = build_prepare_value(dir.path(), &ws, action, None).unwrap();
            assert_eq!(v["valid"], true, "{action}");
            assert!(v["errors"].as_array().unwrap().is_empty(), "{action}");
            // No commits → suggestedPRTitle is "<title> (0 commits)".
            assert_eq!(v["suggestedPRTitle"], "WS (0 commits)");
            assert_eq!(v["suggestedCommitMessage"], "");
            assert_eq!(v["suggestedPRBody"], "");
        }
    }

    #[test]
    fn prepare_without_git_defaults_title_to_changes_when_workspace_title_empty() {
        let dir = TempDir::new("nogit-prep-empty-title");
        let mut ws = mk_workspace();
        ws.title = String::new();
        let v = build_prepare_value(dir.path(), &ws, "commit", None).unwrap();
        assert_eq!(v["suggestedPRTitle"], "Changes (0 commits)");
    }

    #[test]
    fn prepare_in_real_repo_reports_staged_and_unstaged_files() {
        let repo = init_repo("prep-files");
        // Create a fresh branch off main; one commit on top.
        checkout_new_branch(repo.path(), "topic");
        commit_file(repo.path(), "kept.txt", "k\n", "topic: keep");
        // Now: one staged file and one unstaged file in the worktree.
        std::fs::write(repo.path().join("staged.txt"), "s\n").unwrap();
        let g = Repository::open(repo.path()).unwrap();
        let mut idx = g.index().unwrap();
        idx.add_path(Path::new("staged.txt")).unwrap();
        idx.write().unwrap();
        std::fs::write(repo.path().join("unstaged.txt"), "u\n").unwrap();

        let ws = mk_workspace();
        let v = build_prepare_value(repo.path(), &ws, "commit", None).unwrap();
        assert_eq!(v["valid"], true);

        let files = v["files"].as_array().unwrap();
        // staged.txt staged=true, unstaged.txt staged=false. Order is staged then
        // unstaged per the implementation.
        let staged: Vec<_> = files
            .iter()
            .filter(|f| f["staged"].as_bool().unwrap())
            .collect();
        let unstaged: Vec<_> = files
            .iter()
            .filter(|f| !f["staged"].as_bool().unwrap())
            .collect();
        assert!(staged.iter().any(|f| f["path"] == "staged.txt"));
        assert!(unstaged.iter().any(|f| f["path"] == "unstaged.txt"));
        // filesCount is the number of distinct paths.
        let count = v["filesCount"].as_u64().unwrap();
        assert!(count >= 2, "expected ≥2 distinct files, got {count}");

        // One commit on the branch → suggestedCommitMessage equals that message
        // and suggestedPRTitle equals that message (single-commit branch).
        assert_eq!(v["suggestedCommitMessage"], "topic: keep");
        assert_eq!(v["suggestedPRTitle"], "topic: keep");
    }

    #[test]
    fn prepare_files_filter_narrows_to_requested_paths() {
        let repo = init_repo("prep-filter");
        // Stage two files; filter to just one.
        std::fs::write(repo.path().join("a.txt"), "a\n").unwrap();
        std::fs::write(repo.path().join("b.txt"), "b\n").unwrap();
        let g = Repository::open(repo.path()).unwrap();
        let mut idx = g.index().unwrap();
        idx.add_path(Path::new("a.txt")).unwrap();
        idx.add_path(Path::new("b.txt")).unwrap();
        idx.write().unwrap();

        let ws = mk_workspace();
        let filter = vec!["a.txt".to_string()];
        let v = build_prepare_value(repo.path(), &ws, "commit", Some(&filter)).unwrap();
        let files = v["files"].as_array().unwrap();
        assert!(files.iter().all(|f| f["path"] == "a.txt"));
        assert!(!files.is_empty());

        // Empty filter slice means "no filter": both files appear.
        let empty: Vec<String> = Vec::new();
        let v2 = build_prepare_value(repo.path(), &ws, "commit", Some(&empty)).unwrap();
        let files2 = v2["files"].as_array().unwrap();
        let paths: std::collections::HashSet<&str> =
            files2.iter().map(|f| f["path"].as_str().unwrap()).collect();
        assert!(paths.contains("a.txt"));
        assert!(paths.contains("b.txt"));
    }

    #[test]
    fn prepare_multi_commit_branch_joins_messages_and_counts_in_title() {
        let repo = init_repo("prep-multi");
        checkout_new_branch(repo.path(), "topic");
        commit_file(repo.path(), "f1.txt", "1\n", "first");
        commit_file(repo.path(), "f2.txt", "2\n", "second");

        let ws = mk_workspace();
        let v = build_prepare_value(repo.path(), &ws, "commit", None).unwrap();
        // history_since returns newest-first; expect "second\n- first".
        let msg = v["suggestedCommitMessage"].as_str().unwrap();
        assert!(msg.contains("first"));
        assert!(msg.contains("second"));
        assert!(msg.contains("\n- "));
        assert_eq!(v["suggestedPRTitle"], "WS (2 commits)");
    }

    // ----- step / accept_result -----

    #[test]
    fn step_includes_only_supplied_fields() {
        let v = step("s1", "Commit", "pending", None, None);
        assert_eq!(v["id"], "s1");
        assert_eq!(v["name"], "Commit");
        assert_eq!(v["status"], "pending");
        // Absent optionals are omitted, not `null` (mirrors JSON.stringify).
        let obj = v.as_object().unwrap();
        assert!(!obj.contains_key("message"));
        assert!(!obj.contains_key("error"));

        let v2 = step("s2", "Push", "failed", Some("up to date"), Some("denied"));
        assert_eq!(v2["message"], "up to date");
        assert_eq!(v2["error"], "denied");
    }

    #[test]
    fn accept_result_assembles_success_failure_and_optional_payloads() {
        // Success-with-result: `result` present, `error` absent.
        let payload = json!({ "commitHash": "deadbeef" });
        let v = accept_result(
            true,
            vec![step("c", "Commit", "completed", None, None)],
            Some(payload.clone()),
            None,
        );
        assert_eq!(v["success"], true);
        assert_eq!(v["result"], payload);
        let obj = v.as_object().unwrap();
        assert!(!obj.contains_key("error"));
        assert_eq!(v["steps"].as_array().unwrap().len(), 1);

        // Failure-with-error: `error` present, `result` absent.
        let v = accept_result(
            false,
            vec![step("p", "Push", "failed", None, Some("boom"))],
            None,
            Some("boom".into()),
        );
        assert_eq!(v["success"], false);
        assert_eq!(v["error"], "boom");
        assert!(!v.as_object().unwrap().contains_key("result"));

        // Neither result nor error → only `success` + `steps`.
        let v = accept_result(true, Vec::new(), None, None);
        assert_eq!(v["success"], true);
        let obj = v.as_object().unwrap();
        assert!(!obj.contains_key("result"));
        assert!(!obj.contains_key("error"));
        assert!(v["steps"].as_array().unwrap().is_empty());
    }
}
