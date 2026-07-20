//! Wire-policy glue for the `git.*` methods (§5.6).
//!
//! Worktree-path resolution, the `git.stage` CSV/array parse + `.`/`*`/`--all`
//! rejection (ported from the TS `ws.git.stage` builder), and the
//! `git.getBranches`/`git.branchStatus` repo-path validation. The actual git
//! operations live in `intent-git`; this module owns only the parity-critical
//! wire policy.

use std::path::{Path, PathBuf};

use intent_core::settings_file::SettingsFile;
use intent_core::{Error, Result, Workspace};
use serde_json::{json, Map, Value};

/// TS `ws.git.stage` message when an agent tries to stage everything.
const STAGE_ALL_MSG: &str = "Staging all files is not allowed. Please specify individual file paths to stage. Use git_status to see which files you have modified, then stage only those specific files.";

/// TS `ws.git.stage` message when no usable paths remain after parsing.
const NO_PATHS_MSG: &str =
    "No file paths provided. Please specify at least one file path to stage.";

/// `git.discard` message when the caller tries to discard everything (parity
/// with the stage-all guard but discard-oriented). Rejects `.` / `*` /
/// `--all` in both the CSV-string and array shapes so the array-form
/// bypass (`["*"]`, `["--all"]`) cannot devolve into a silent no-op.
const DISCARD_ALL_MSG: &str =
    "Discarding all files is not allowed. Please specify individual file paths to discard.";

/// `git.discard` message when no usable paths remain after parsing.
const NO_DISCARD_PATHS_MSG: &str =
    "No file paths provided. Please specify at least one file path to discard.";

/// TS `assertAgentCommitAllowed` rejection message (auto-commit disabled).
const AUTO_COMMIT_DISABLED_MSG: &str = "Auto-commit is disabled for this workspace. \
Use agent_commit_changes with userRequested: true if the user asked you to commit.";

/// Port of `assertAgentCommitAllowed`: block an agent-initiated commit when
/// auto-commit is disabled, unless `user_requested` bypasses it. The gate reads
/// the effective `git.autoCommit` setting (§9.8 OQ#2), whose schema default is
/// `true` so the established behavior is preserved when the key is unset.
pub(crate) fn assert_agent_commit_allowed(
    settings: &SettingsFile,
    user_requested: bool,
) -> Result<()> {
    if user_requested || settings.git.auto_commit {
        return Ok(());
    }
    Err(Error::Internal(AUTO_COMMIT_DISABLED_MSG.to_string()))
}

/// Resolve a workspace's worktree path: the explicit `worktreePath`, else the
/// `repositoryPath` (TS parity). `None` when neither is set.
pub(crate) fn worktree_path(ws: &Workspace) -> Option<PathBuf> {
    ws.worktree_path
        .as_ref()
        .or(ws.repository_path.as_ref())
        .filter(|p| !p.is_empty())
        .map(PathBuf::from)
}

/// Parse the `git.stage` `paths` param and enforce the stage-all rejection,
/// mirroring the TS builder exactly. Rejections and an empty result surface as
/// [`Error::Internal`] (→ `-32603`).
pub(crate) fn parse_stage_paths(paths: &Value) -> Result<Vec<String>> {
    // Reject staging everything — operates on the original value (TS parity):
    // the literal strings "." / "*" and any string containing "--all".
    if let Value::String(s) = paths {
        if s == "." || s == "*" || s.contains("--all") {
            return Err(Error::Internal(STAGE_ALL_MSG.to_string()));
        }
    }

    let list: Vec<String> = match paths {
        Value::Array(items) => items
            .iter()
            .filter_map(Value::as_str)
            .map(|p| p.trim().to_string())
            .filter(|p| !p.is_empty())
            .collect(),
        Value::String(s) => s
            .split(',')
            .map(|p| p.trim().to_string())
            .filter(|p| !p.is_empty())
            .collect(),
        _ => Vec::new(),
    };

    if list.is_empty() {
        return Err(Error::Internal(NO_PATHS_MSG.to_string()));
    }
    Ok(list)
}

/// Parse the `git.discard` `paths` param with discard-oriented error messages
/// and a discard-all rejection that closes the array-shape bypass. Unlike
/// [`parse_stage_paths`], the `.` / `*` / `--all` tokens are refused in every
/// shape — top-level string, CSV entry, or array element — so callers cannot
/// smuggle a discard-all through `["*"]`, `["--all"]`, or `[".", "a.ts"]`.
/// Rejections and an empty result surface as [`Error::Internal`] (→ `-32603`).
pub(crate) fn parse_discard_paths(paths: &Value) -> Result<Vec<String>> {
    // Top-level string discard-all (parity with `parse_stage_paths`).
    if let Value::String(s) = paths {
        if s == "." || s == "*" || s.contains("--all") {
            return Err(Error::Internal(DISCARD_ALL_MSG.to_string()));
        }
    }

    let list: Vec<String> = match paths {
        Value::Array(items) => items
            .iter()
            .filter_map(Value::as_str)
            .map(|p| p.trim().to_string())
            .filter(|p| !p.is_empty())
            .collect(),
        Value::String(s) => s
            .split(',')
            .map(|p| p.trim().to_string())
            .filter(|p| !p.is_empty())
            .collect(),
        _ => Vec::new(),
    };

    // Element-level discard-all: `["*"]`, `["--all"]`, `["."]`, etc. Closes
    // the array-shape bypass where the shared stage parser only inspected
    // the top-level string.
    for p in &list {
        if p == "." || p == "*" || p.contains("--all") {
            return Err(Error::Internal(DISCARD_ALL_MSG.to_string()));
        }
    }

    if list.is_empty() {
        return Err(Error::Internal(NO_DISCARD_PATHS_MSG.to_string()));
    }
    Ok(list)
}

/// Validate the path-based `repoPath` param of the read-only branch methods
/// (`git.getBranches`, `git.branchStatus`): the path must exist locally and be
/// a git repository. Mirrors the ungated legacy IPC handlers (`git:getBranches`
/// `{ repoPath }` variant and `git:getBranchStatus`), which ran git directly
/// against any user-picked path — the workspace-create flow lists branches
/// *before* the repo is registered as a workspace, so a known-repo gate breaks
/// it. Nonexistent paths and non-git directories are rejected with distinct
/// `-32602` errors; mutating `git.*` methods remain workspace-scoped.
pub(crate) fn validate_repo_path(repo_path: &str) -> Result<()> {
    let path = Path::new(repo_path);
    if !path.exists() {
        return Err(Error::InvalidParams(format!(
            "Repository path does not exist: {repo_path}"
        )));
    }
    if !intent_git::is_repository(path) {
        return Err(Error::InvalidParams(format!(
            "Path is not a git repository: {repo_path}"
        )));
    }
    Ok(())
}

/// Build the wire `CommitInfo` (§8.9) for a history record: `files` is the list
/// of changed paths, `sha` the short hash, and the optional agent/note linkage
/// is included only when present.
pub(crate) fn commit_to_commit_info(c: &intent_git::history::CommitRecord) -> Value {
    let mut obj = Map::new();
    obj.insert("hash".to_string(), json!(c.hash));
    obj.insert(
        "sha".to_string(),
        json!(c.hash.chars().take(7).collect::<String>()),
    );
    obj.insert("author".to_string(), json!(c.author));
    obj.insert("email".to_string(), json!(c.author_email));
    obj.insert("date".to_string(), json!(c.date));
    obj.insert("message".to_string(), json!(c.message));
    obj.insert("files".to_string(), json!(c.files));
    if let Some(agent_id) = &c.agent_id {
        obj.insert("agentId".to_string(), json!(agent_id));
    }
    if let Some(note_id) = &c.linked_note_id {
        obj.insert("linkedNoteId".to_string(), json!(note_id));
    }
    Value::Object(obj)
}

/// Build the `git.diffs` wire result (`[{ path, hunks }]`) for a worktree.
/// When `commit_hash` is set, returns hunks for `<commit_hash>^..<commit_hash>`
/// and `staged` is ignored. Otherwise `staged` selects the HEAD→index diff
/// (else index→workdir). `path` filters to a single file. Hunks for staged /
/// committed changes are hydrated from the recorded blob SHAs; unstaged changes
/// read workdir content directly. Binary files yield an empty `hunks` array.
/// A `commit_hash` that does not resolve degrades to an empty array.
pub(crate) fn build_diffs(
    worktree: &Path,
    path: Option<&str>,
    staged: bool,
    commit_hash: Option<&str>,
) -> Result<Value> {
    let files = match commit_hash {
        Some(hash) => match intent_git::diff::diff_commit(worktree, hash) {
            Ok(f) => f,
            Err(Error::NotFound(_)) => return Ok(Value::Array(Vec::new())),
            Err(e) => return Err(e),
        },
        None => {
            if staged {
                intent_git::diff::diff_head_to_index(worktree)?
            } else {
                intent_git::diff::diff_index_to_workdir(worktree)?
            }
        }
    };
    let use_blob_hunks = commit_hash.is_some() || staged;
    let mut out = Vec::new();
    for fd in &files {
        if let Some(p) = path {
            if fd.path != p {
                continue;
            }
        }
        let hunks = if fd.is_binary {
            Vec::new()
        } else if use_blob_hunks {
            intent_git::diff::hunks_between(
                worktree,
                fd.old_blob.as_deref(),
                fd.new_blob.as_deref(),
            )?
        } else {
            intent_git::diff::hunks_index_to_workdir(worktree, &fd.path)?
        };
        out.push(json!({ "path": fd.path, "hunks": hunks_to_value(&hunks) }));
    }
    Ok(Value::Array(out))
}

/// Build the `git.commitDetails` wire result for a single commit. Returns the
/// flattened shape consumed by the FE ChangesTabType: metadata plus the
/// per-file `fileDetails: [{ path, additions, deletions }]` array (`files` is
/// the flat path-string list, kept for callers that only want names).
pub(crate) fn build_commit_details(worktree: &Path, commit_hash: &str) -> Result<Value> {
    let details = intent_git::history::commit_details(worktree, commit_hash)?;
    let files: Vec<Value> = details.files.iter().map(|f| json!(f.path)).collect();
    let file_details: Vec<Value> = details
        .files
        .iter()
        .map(|f| {
            json!({
                "path": f.path,
                "additions": f.additions,
                "deletions": f.deletions,
            })
        })
        .collect();
    Ok(json!({
        "commitHash": details.hash,
        "author": details.author,
        "authorEmail": details.author_email,
        "date": details.date,
        "message": details.message,
        "files": files,
        "fileDetails": file_details,
    }))
}

/// Build the `git.numstat` wire result for a worktree. Selection rules match
/// the FE `git:numstat` handler: when `base_ref` or `base_sha` is set, the
/// branch boundary (merge-base of `target_ref` and `base_ref`, else `base_sha`
/// when it is an ancestor of `target_ref`) drives a two-dot `<boundary>..
/// <target>` diff; else `staged=Some(true)` → HEAD→index, `staged=Some(false)`
/// → index→workdir tracked, `staged=None` → HEAD→workdir tracked. `paths`
/// filters to the given repo-relative paths. An unresolved boundary yields
/// an empty array. Result shape: `[{ filePath, additions, deletions }]`.
pub(crate) fn build_numstat(
    worktree: &Path,
    staged: Option<bool>,
    base_ref: Option<&str>,
    base_sha: Option<&str>,
    target_ref: &str,
    paths: Option<&[String]>,
) -> Result<Value> {
    let has_base =
        base_ref.is_some_and(|s| !s.is_empty()) || base_sha.is_some_and(|s| !s.is_empty());
    let files = if has_base {
        let Some(boundary) =
            intent_git::diff::resolve_branch_boundary(worktree, base_ref, base_sha, target_ref)?
        else {
            return Ok(Value::Array(Vec::new()));
        };
        intent_git::diff::diff_two_dot(worktree, &boundary, target_ref)?
    } else {
        match staged {
            Some(true) => intent_git::diff::diff_head_to_index(worktree)?,
            Some(false) => intent_git::diff::diff_index_to_workdir_tracked(worktree)?,
            None => intent_git::diff::diff_head_to_workdir_tracked(worktree)?,
        }
    };
    let filter: Option<std::collections::HashSet<&str>> =
        paths.map(|p| p.iter().map(String::as_str).collect());
    let items: Vec<Value> = files
        .into_iter()
        .filter(|fd| match &filter {
            Some(set) => set.contains(fd.path.as_str()),
            None => true,
        })
        .map(|fd| {
            json!({
                "filePath": fd.path,
                "additions": fd.additions,
                "deletions": fd.deletions,
            })
        })
        .collect();
    Ok(Value::Array(items))
}

/// Build the `git.branchDiff` wire result: one entry per changed file in the
/// two-dot `<boundary>..<target_ref>` range, carrying the full file contents
/// at the boundary and the target so the FE branch-base viewer can render the
/// diff from `oldContent`/`newContent` alone (parity with
/// `batchedGitBranchBaseDiff` / TrackedChangeDiffViewer). `chunks` is always
/// an empty array — the FE consumer ignores it for the branch-base shape.
/// Boundary resolution matches [`build_numstat`]; an unresolved boundary
/// yields an empty array.
pub(crate) fn build_branch_diff(
    worktree: &Path,
    base_ref: Option<&str>,
    base_sha: Option<&str>,
    target_ref: &str,
    paths: Option<&[String]>,
) -> Result<Value> {
    let Some(boundary) =
        intent_git::diff::resolve_branch_boundary(worktree, base_ref, base_sha, target_ref)?
    else {
        return Ok(Value::Array(Vec::new()));
    };
    let files = intent_git::diff::diff_two_dot(worktree, &boundary, target_ref)?;
    let filter: Option<std::collections::HashSet<&str>> =
        paths.map(|p| p.iter().map(String::as_str).collect());
    let mut out = Vec::new();
    for fd in files {
        if let Some(set) = &filter {
            if !set.contains(fd.path.as_str()) {
                continue;
            }
        }
        // `show_file` folds a missing path at the ref to `Ok("")`, which
        // matches the FE handler's per-side `showFileAt` and gives us empty
        // pre-images for added files and empty post-images for deletions.
        // Any other error (revparse failure, repository IO) is a real problem
        // — surface it instead of silently returning empty content.
        let old_content = intent_git::show::show_file(worktree, &boundary, &fd.path)?;
        let new_content = intent_git::show::show_file(worktree, target_ref, &fd.path)?;
        out.push(json!({
            "file": fd.path,
            "chunks": Vec::<Value>::new(),
            "oldContent": old_content,
            "newContent": new_content,
        }));
    }
    Ok(Value::Array(out))
}

/// Empty `git.commitDetails` envelope returned for non-repo / remote / unknown
/// workspaces and for an unresolvable `commit_hash`. Mirrors the graceful-empty
/// pattern used by `git_diffs`/`git_commits`.
pub(crate) fn empty_commit_details(commit_hash: &str) -> Value {
    json!({
        "commitHash": commit_hash,
        "author": "",
        "authorEmail": "",
        "date": "",
        "message": "",
        "files": Vec::<Value>::new(),
        "fileDetails": Vec::<Value>::new(),
    })
}

/// Convert the internal hunk list to the wire shape consumed by the FE diff
/// viewer: `{ oldStart, oldLines, newStart, newLines, lines }`.
fn hunks_to_value(hunks: &[intent_git::diff::DiffHunk]) -> Value {
    Value::Array(hunks.iter().map(hunk_to_value).collect())
}

fn hunk_to_value(h: &intent_git::diff::DiffHunk) -> Value {
    json!({
        "oldStart": h.old_start,
        "oldLines": h.old_lines,
        "newStart": h.new_start,
        "newLines": h.new_lines,
        "lines": h.lines.iter().map(line_to_value).collect::<Vec<_>>(),
    })
}

/// Convert one diff line to the wire `DiffLine` (`type`/`content` plus optional
/// 1-based `oldNumber`/`newNumber`), matching `LineType`
/// (Context/Addition/Deletion) from the shared types.
fn line_to_value(l: &intent_git::diff::DiffLine) -> Value {
    use intent_git::diff::DiffLineKind;
    let kind = match l.kind {
        DiffLineKind::Context => "Context",
        DiffLineKind::Addition => "Addition",
        DiffLineKind::Deletion => "Deletion",
    };
    let mut obj = Map::new();
    obj.insert("type".to_string(), json!(kind));
    obj.insert("content".to_string(), json!(l.content));
    if let Some(n) = l.old_lineno {
        obj.insert("oldNumber".to_string(), json!(n));
    }
    if let Some(n) = l.new_lineno {
        obj.insert("newNumber".to_string(), json!(n));
    }
    Value::Object(obj)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn rejects_stage_all_forms() {
        for v in [json!("."), json!("*"), json!("git add --all")] {
            let err = parse_stage_paths(&v).unwrap_err();
            assert!(matches!(err, Error::Internal(_)));
            assert!(format!("{err}").contains("Staging all files is not allowed"));
        }
    }

    #[test]
    fn parses_csv_string_and_array() {
        let csv = parse_stage_paths(&json!(" a.ts , b.ts ,")).unwrap();
        assert_eq!(csv, vec!["a.ts".to_string(), "b.ts".to_string()]);
        let arr = parse_stage_paths(&json!(["a.ts", " b.ts ", ""])).unwrap();
        assert_eq!(arr, vec!["a.ts".to_string(), "b.ts".to_string()]);
    }

    #[test]
    fn empty_paths_error() {
        let err = parse_stage_paths(&json!("   ,  ")).unwrap_err();
        assert!(format!("{err}").contains("No file paths provided"));
        let err = parse_stage_paths(&json!([])).unwrap_err();
        assert!(format!("{err}").contains("No file paths provided"));
    }

    #[test]
    fn repo_path_nonexistent_is_invalid_params() {
        let err = validate_repo_path("/no/such/repo").unwrap_err();
        assert!(matches!(err, Error::InvalidParams(_)));
        assert_eq!(
            format!("{err}"),
            "invalid params: Repository path does not exist: /no/such/repo"
        );
    }

    #[test]
    fn repo_path_non_git_dir_is_invalid_params() {
        let dir = std::env::temp_dir().join(format!("intentd-nongit-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let err = validate_repo_path(dir.to_str().unwrap()).unwrap_err();
        assert!(matches!(err, Error::InvalidParams(_)));
        assert!(format!("{err}").contains("Path is not a git repository:"));
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn repo_path_unregistered_git_repo_is_accepted() {
        let dir = std::env::temp_dir().join(format!("intentd-gitok-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        git2::Repository::init(&dir).unwrap();
        assert!(validate_repo_path(dir.to_str().unwrap()).is_ok());
        std::fs::remove_dir_all(&dir).ok();
    }
}
