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
    let trunk = trunk_branch(ws);

    // No git dir → the minimal status (branch from the workspace, zeros).
    if !worktree.join(".git").exists() {
        return Ok(minimal_status_value(ws, &trunk));
    }

    let status = intent_git::status::status(worktree)?;
    let branch = if status.branch.is_empty() {
        ws.branch.clone()
    } else {
        status.branch.clone()
    };

    let remote_url = intent_git::remote::origin_url(worktree)?;
    let has_remote = remote_url.is_some();
    let is_pushed = has_remote
        && intent_git::remote::remote_tracking_exists(worktree, "origin", &branch).unwrap_or(false);

    let trunk_ref = resolve_trunk_ref(worktree, &trunk, has_remote);
    let (ahead, behind) = intent_git::remote::ahead_behind(worktree, &trunk_ref)?;

    let commits = intent_git::history::history_since(worktree, Some(&trunk_ref), 200)?;
    let local_commits: Vec<Value> = commits.iter().map(commit_to_value).collect();

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
        .map(|(o, r)| (Some(o), Some(r)))
        .unwrap_or((None, None));

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
            additions += fd.additions as i64;
            deletions += fd.deletions as i64;
        }
        // Staged changes (HEAD→index).
        for fd in intent_git::diff::diff_head_to_index(worktree)? {
            additions += fd.additions as i64;
            deletions += fd.deletions as i64;
            files.push(PrepFile {
                path: fd.path,
                additions: fd.additions as i64,
                deletions: fd.deletions as i64,
                staged: true,
            });
        }
        // Unstaged changes (index→workdir, incl. untracked).
        for fd in intent_git::diff::diff_index_to_workdir(worktree)? {
            additions += fd.additions as i64;
            deletions += fd.deletions as i64;
            files.push(PrepFile {
                path: fd.path,
                additions: fd.additions as i64,
                deletions: fd.deletions as i64,
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

    let commit_messages: Vec<String> =
        intent_git::history::history_since(worktree, Some(&trunk_ref), 200)
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

    #[test]
    fn validates_actions() {
        assert_eq!(validate_action("commit").unwrap(), "commit");
        assert_eq!(validate_action("create-pr").unwrap(), "create-pr");
        assert!(validate_action("nope").is_err());
    }

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
}
