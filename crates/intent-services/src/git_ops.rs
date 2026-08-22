//! Wire-policy glue for the `git.*` methods (§5.6).
//!
//! Worktree-path resolution, the `git.stage` CSV/array parse + `.`/`*`/`--all`
//! rejection (ported from the TS `ws.git.stage` builder), and the
//! `git.getBranches`/`git.branchStatus` repo-path validation. The actual git
//! operations live in `intent-git`; this module owns only the parity-critical
//! wire policy.

use std::path::{Path, PathBuf};

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
/// The `Auto-commit is disabled` prefix is load-bearing: `auto_commit.rs`
/// matches on it (`AUTO_COMMIT_DISABLED_MARK`) to treat the rejection as a
/// silent skip.
const AUTO_COMMIT_DISABLED_MSG: &str = "Auto-commit is disabled for this workspace: the user \
has turned off agent commits here and does not want agents committing. Do not work around \
this gate — do not commit via raw `git commit` or any other means. Retry `ws.git.commit` \
with `userRequested: true` only if the user has explicitly asked you to commit.";

/// Port of `assertAgentCommitAllowed`: block an agent-initiated commit when
/// auto-commit is disabled, unless `user_requested` bypasses it. The gate
/// reads the workspace-resolved auto-commit state (per-workspace override
/// when persisted, else the effective `git.autoCommit` setting — see
/// `Services::effective_auto_commit`), whose schema default is `true` so the
/// established behavior is preserved when nothing is set.
pub(crate) fn assert_agent_commit_allowed(
    auto_commit_enabled: bool,
    user_requested: bool,
) -> Result<()> {
    if user_requested || auto_commit_enabled {
        return Ok(());
    }
    Err(Error::Internal(AUTO_COMMIT_DISABLED_MSG.to_string()))
}

/// Drop submodule-internal entries from the attribution-filtered
/// `git.agentCommit` commit set (monorepo#1714 follow-up): auto-commit must
/// never flatten a submodule's gitlink into the superproject, so a path that
/// lands strictly inside a registered submodule is dropped here — before the
/// caller's emptiness check — rather than committed. The gitlink path itself
/// (a pin bump) is never dropped. Detection is delegated to
/// `intent_git::submodule` (Task 1's helper); this function only does the
/// filtering + per-submodule drop-count bookkeeping for the caller's debug
/// log. When the worktree cannot be opened as a repo or has no registered
/// submodules, `paths` passes through unchanged.
pub(crate) fn drop_submodule_internal_paths(
    worktree: &Path,
    paths: Vec<String>,
) -> (Vec<String>, Vec<(String, usize)>) {
    let Ok(repo) = git2::Repository::open(worktree) else {
        return (paths, Vec::new());
    };
    let Ok(submodules) = intent_git::submodule::submodule_paths(&repo) else {
        return (paths, Vec::new());
    };
    if submodules.is_empty() {
        return (paths, Vec::new());
    }
    let ignore_case = intent_git::submodule::ignores_case(&repo);
    let mut dropped: std::collections::BTreeMap<String, usize> = std::collections::BTreeMap::new();
    let kept: Vec<String> = paths
        .into_iter()
        .filter(|p| {
            match intent_git::submodule::submodule_containing(&submodules, p, ignore_case) {
                Some(sm) => {
                    *dropped.entry(sm.to_string()).or_insert(0) += 1;
                    false
                }
                None => true,
            }
        })
        .collect();
    (kept, dropped.into_iter().collect())
}

/// Refuse an explicit `git.agentCommit` `files` list that names a path
/// strictly inside a registered submodule (monorepo#1714 follow-up): unlike
/// the attribution-filtered fallback, an explicit request cannot be silently
/// pruned — it fails with a message naming the offending path(s), the
/// containing submodule, and advising the caller to commit from within the
/// submodule repo instead. The gitlink path itself (a pin bump) is always
/// allowed. Detection is delegated to `intent_git::submodule` (Task 1's
/// helper) — no duplicate detection logic here. Matching happens on the
/// repo-relative form of each entry so an in-worktree absolute path is
/// refused exactly like its relative spelling. A no-op when the worktree
/// cannot be opened as a repo or has no registered submodules (the caller's
/// downstream commit call surfaces any real repo problem).
pub(crate) fn reject_submodule_internal_files(worktree: &Path, files: &[String]) -> Result<()> {
    let Ok(repo) = git2::Repository::open(worktree) else {
        return Ok(());
    };
    let submodules = intent_git::submodule::submodule_paths(&repo)?;
    if submodules.is_empty() {
        return Ok(());
    }
    let ignore_case = intent_git::submodule::ignores_case(&repo);
    let workdir = repo.workdir().map(Path::to_path_buf);
    let mut by_submodule: std::collections::BTreeMap<&str, Vec<&str>> =
        std::collections::BTreeMap::new();
    for f in files {
        let rel = intent_git::submodule::to_repo_relative(workdir.as_deref(), f, ignore_case);
        if let Some(sm) =
            intent_git::submodule::submodule_containing(&submodules, &rel, ignore_case)
        {
            by_submodule.entry(sm).or_default().push(f.as_str());
        }
    }
    if by_submodule.is_empty() {
        return Ok(());
    }
    let detail = by_submodule
        .into_iter()
        .map(|(sm, paths)| format!("{} (submodule '{sm}')", paths.join(", ")))
        .collect::<Vec<_>>()
        .join("; ");
    Err(Error::Internal(format!(
        "Cannot commit path(s) inside a submodule from the superproject: {detail}. \
         Commit these changes from within the submodule repo instead."
    )))
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

/// Worktree-relative submodule `path` entries parsed from `.gitmodules`
/// content (multi git root tracking, monorepo#2053). A tolerant INI-ish
/// parse: only `path = <value>` lines inside `[submodule "..."]` sections
/// count, order is preserved, duplicates are dropped, and unsafe values
/// (empty, absolute on ANY platform, containing `..`) are skipped — the
/// background sweep joins these against the worktree root, and `Path::join`
/// REPLACES the base when handed an absolute path, so a hostile
/// `.gitmodules` must never smuggle one in. `/`-rooted, Windows
/// drive-absolute (`C:\...`, `C:/...`), and UNC (`\\server\...`) forms are
/// all rejected regardless of the host platform.
pub(crate) fn parse_gitmodules_paths(content: &str) -> Vec<String> {
    let mut paths: Vec<String> = Vec::new();
    let mut in_submodule_section = false;
    for line in content.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            in_submodule_section = line.starts_with("[submodule");
            continue;
        }
        if !in_submodule_section {
            continue;
        }
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };
        if key.trim() != "path" {
            continue;
        }
        let value = value.trim();
        if value.is_empty()
            || is_any_platform_absolute(value)
            || value.split(['/', '\\']).any(|seg| seg == "..")
        {
            continue;
        }
        if !paths.iter().any(|p| p == value) {
            paths.push(value.to_string());
        }
    }
    paths
}

/// True when `value` is absolute in ANY platform's spelling — POSIX
/// `/`-rooted, Windows drive-absolute (`C:\` / `C:/`), or UNC / rooted
/// backslash (`\\server\share`, `\foo`). Checked textually (not via
/// `Path::is_absolute`, which is host-platform-specific) because a hostile
/// `.gitmodules` written on one platform can be swept on another.
fn is_any_platform_absolute(value: &str) -> bool {
    if value.starts_with('/') || value.starts_with('\\') {
        return true;
    }
    let bytes = value.as_bytes();
    // Windows drive prefix: ASCII letter + `:` (covers `C:\`, `C:/`, and the
    // drive-relative `C:foo`, which still escapes the worktree on Windows).
    bytes.len() >= 2 && bytes[0].is_ascii_alphabetic() && bytes[1] == b':'
}

/// Normalize `git.diffs` `paths` entries against the worktree root
/// (defense-in-depth, mirroring `git.showFile`'s absolute→relative
/// conversion): an entry under `<worktree>/` is stripped to its
/// worktree-relative form; every other entry — already relative, equal to
/// the root, or outside it — passes through verbatim (an absolute path
/// outside the worktree matches nothing, exactly as before). Callers must
/// normalize BEFORE computing the single-flight key so equivalent
/// absolute/relative requests coalesce onto one walk.
pub(crate) fn normalize_diff_paths(worktree: &Path, paths: Vec<String>) -> Vec<String> {
    let prefix = format!("{}/", worktree.to_string_lossy());
    paths
        .into_iter()
        .map(|p| match p.strip_prefix(&prefix) {
            Some(rel) => rel.to_string(),
            None => p,
        })
        .collect()
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

/// Build the wire `CommitSummary` (§5.6 `git.commits`) for a history record:
/// `sha` is the short hash and the optional agent/note linkage is included
/// only when present. `files` (changed paths) is emitted only when the record
/// carries a computed file list; records from a metadata-only walk
/// (`include_files = false`) omit it — clients fetch per-file data on demand
/// via `git.commitDetails`.
pub(crate) fn commit_to_commit_summary(c: &intent_git::history::CommitRecord) -> Value {
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
    if let Some(files) = &c.files {
        obj.insert("files".to_string(), json!(files));
    }
    if let Some(agent_id) = &c.agent_id {
        obj.insert("agentId".to_string(), json!(agent_id));
    }
    if let Some(note_id) = &c.linked_note_id {
        obj.insert("linkedNoteId".to_string(), json!(note_id));
    }
    Value::Object(obj)
}

/// Cap on a single file's serialized hunks in `git.diffs`. Larger files are
/// listed with empty `hunks` so the FE still sees the path (and can open via
/// path-scoped `git.diffs`) without shipping tens of MB of line content.
const MAX_DIFF_FILE_HUNKS_BYTES: usize = 512 * 1024;
/// Cap on the whole `git.diffs` JSON payload. Observed failure: unscoped
/// unstaged walk on a 5.8 GB worktree produced a **277 MiB** UDS frame and
/// HOL'd the connection writer for ~38s (`frame_bytes=276998604`), timing out
/// every interactive RPC. Prefer path-scoped calls for large trees.
const MAX_DIFFS_RESPONSE_BYTES: usize = 4 * 1024 * 1024;

/// Approximate wire size of hunk JSON (content-dominated).
fn approx_hunks_bytes(hunks: &[intent_git::diff::DiffHunk]) -> usize {
    let mut n = 0usize;
    for h in hunks {
        for line in &h.lines {
            // JSON escaping overhead is small vs content; content is the bulk.
            n = n.saturating_add(line.content.len()).saturating_add(24);
        }
        n = n.saturating_add(64);
    }
    n
}

/// Build one `{ path, hunks }` entry, omitting hunk bodies that would blow the
/// per-file budget. Returns `(value, included_bytes)`.
fn file_diff_entry(path: &str, hunks: &[intent_git::diff::DiffHunk]) -> (Value, usize) {
    let bytes = approx_hunks_bytes(hunks);
    if bytes > MAX_DIFF_FILE_HUNKS_BYTES {
        tracing::warn!(
            path,
            hunk_bytes = bytes,
            limit = MAX_DIFF_FILE_HUNKS_BYTES,
            "git.diffs: omitting oversize file hunks (path kept, empty hunks)"
        );
        (
            json!({ "path": path, "hunks": [] }),
            path.len().saturating_add(32),
        )
    } else {
        let v = json!({ "path": path, "hunks": hunks_to_value(hunks) });
        (v, bytes.saturating_add(path.len()).saturating_add(32))
    }
}

/// Build the `git.diffs` wire result (`[{ path, hunks }]`) for a worktree.
/// When `commit_hash` is set, returns hunks for `<commit_hash>^..<commit_hash>`
/// and `staged` is ignored. Otherwise `staged` selects the HEAD→index diff
/// (else index→workdir). `paths` filters to exactly those files (`None` or
/// empty ⇒ full tree). Hunks for staged / committed changes are hydrated from
/// the recorded blob SHAs (full-tree walk, set-filtered); the unstaged case
/// comes from a **single** index→workdir traversal (summaries + hunks together,
/// pathspec-narrowed when `paths` is set) instead of one scan per changed file.
/// Binary files yield an empty `hunks` array. A `commit_hash` that does not
/// resolve degrades to an empty array.
///
/// Response size is hard-capped ([`MAX_DIFFS_RESPONSE_BYTES`]): further files
/// are emitted as path-only rows with empty hunks once the budget is spent.
pub(crate) fn build_diffs(
    worktree: &Path,
    paths: Option<&[String]>,
    staged: bool,
    commit_hash: Option<&str>,
) -> Result<Value> {
    let paths = paths.filter(|p| !p.is_empty());
    let requested: Option<std::collections::HashSet<&str>> =
        paths.map(|p| p.iter().map(String::as_str).collect());
    let mut out = Vec::new();
    let mut total = 0usize;
    let mut truncated_files = 0usize;

    let push = |out: &mut Vec<Value>,
                total: &mut usize,
                truncated_files: &mut usize,
                path: &str,
                hunks: &[intent_git::diff::DiffHunk]| {
        if *total >= MAX_DIFFS_RESPONSE_BYTES {
            // Budget exhausted: still surface the path so the FE can request
            // a scoped `git.diffs` later.
            *truncated_files += 1;
            out.push(json!({ "path": path, "hunks": [] }));
            *total = total.saturating_add(path.len().saturating_add(32));
            return;
        }
        let (entry, n) = file_diff_entry(path, hunks);
        if total.saturating_add(n) > MAX_DIFFS_RESPONSE_BYTES && *total > 0 {
            *truncated_files += 1;
            out.push(json!({ "path": path, "hunks": [] }));
            *total = total.saturating_add(path.len().saturating_add(32));
            return;
        }
        *total = total.saturating_add(n);
        out.push(entry);
    };

    if commit_hash.is_none() && !staged {
        let specs: Option<Vec<&str>> = paths.map(|p| p.iter().map(String::as_str).collect());
        let entries =
            intent_git::diff::diff_index_to_workdir_with_hunks(worktree, specs.as_deref())?;
        for entry in &entries {
            // The pathspec prunes the walk but can match more than the exact
            // paths; keep the strict equality filter the wire contract promises.
            if let Some(req) = &requested {
                if !req.contains(entry.file.path.as_str()) {
                    continue;
                }
            }
            // A gitlink delta has no blob content for libgit2 to patch, so
            // synthesize the `Subproject commit <sha>` pseudo-hunk from the
            // pin SHAs (monorepo#1739). Only the gitlink side(s) contribute a
            // pin line — on a gitlink↔file type change the regular side's
            // blob is never rendered as a pin.
            let gitlink_hunks;
            let hunks: &[intent_git::diff::DiffHunk] = if entry.file.is_gitlink() {
                gitlink_hunks = intent_git::diff::gitlink_hunks(
                    entry.file.gitlink_old_sha(),
                    entry.file.gitlink_new_sha(),
                );
                &gitlink_hunks
            } else if entry.file.is_binary {
                &[]
            } else {
                &entry.hunks
            };
            push(
                &mut out,
                &mut total,
                &mut truncated_files,
                &entry.file.path,
                hunks,
            );
        }
        if truncated_files > 0 {
            tracing::warn!(
                worktree = %worktree.display(),
                truncated_files,
                approx_bytes = total,
                limit = MAX_DIFFS_RESPONSE_BYTES,
                "git.diffs: response truncated to stay under wire budget"
            );
        }
        return Ok(Value::Array(out));
    }

    let files = match commit_hash {
        Some(hash) => match intent_git::diff::diff_commit(worktree, hash) {
            Ok(f) => f,
            Err(Error::NotFound(_)) => return Ok(Value::Array(Vec::new())),
            Err(e) => return Err(e),
        },
        None => intent_git::diff::diff_head_to_index(worktree)?,
    };
    for fd in &files {
        if let Some(req) = &requested {
            if !req.contains(fd.path.as_str()) {
                continue;
            }
        }
        let hunks = if fd.is_gitlink() {
            // Gitlink pins are commit SHAs in the submodule's odb — not blobs
            // here — so `hunks_between` cannot hydrate them; synthesize the
            // `Subproject commit <sha>` pseudo-hunk instead (monorepo#1739),
            // from the pin side(s) only.
            intent_git::diff::gitlink_hunks(fd.gitlink_old_sha(), fd.gitlink_new_sha())
        } else if fd.is_binary {
            Vec::new()
        } else {
            intent_git::diff::hunks_between(
                worktree,
                fd.old_blob.as_deref(),
                fd.new_blob.as_deref(),
            )?
        };
        push(&mut out, &mut total, &mut truncated_files, &fd.path, &hunks);
    }
    if truncated_files > 0 {
        tracing::warn!(
            worktree = %worktree.display(),
            truncated_files,
            approx_bytes = total,
            limit = MAX_DIFFS_RESPONSE_BYTES,
            "git.diffs: response truncated to stay under wire budget"
        );
    }
    Ok(Value::Array(out))
}
/// Build the `git.commitDetails` wire result for a single commit. Returns the
/// flattened shape consumed by the FE `ChangesTabType`: metadata plus the
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
/// `batchedGitBranchBaseDiff` / `TrackedChangeDiffViewer`). `chunks` is always
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
        // A gitlink side is not a blob (`show_file` would fail typed
        // `NotAFile` and sink the whole call), so synthesize the same
        // `Subproject commit <sha>` pseudo-content `git.diffs` renders
        // (monorepo#1739); a missing side stays empty content.
        let pin_content = |sha: Option<&str>| {
            sha.map(|s| format!("Subproject commit {s}\n"))
                .unwrap_or_default()
        };
        let old_content = if fd.old_is_gitlink {
            pin_content(fd.gitlink_old_sha())
        } else {
            intent_git::show::show_file(worktree, &boundary, &fd.path)?
        };
        let new_content = if fd.new_is_gitlink {
            pin_content(fd.gitlink_new_sha())
        } else {
            intent_git::show::show_file(worktree, target_ref, &fd.path)?
        };
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
    fn parse_gitmodules_paths_extracts_submodule_paths() {
        let content = "[submodule \"packages/intentd\"]\n\
             \tpath = packages/intentd\n\
             \turl = https://github.com/intent-hq/intentd.git\n\
             [submodule \"packages/fe\"]\n\
             \tpath = packages/fe\n\
             \turl = git@github.com:intent-hq/cloudlands-fe.git\n\
             \tupdate = none\n";
        assert_eq!(
            parse_gitmodules_paths(content),
            vec!["packages/intentd".to_string(), "packages/fe".to_string()]
        );
    }

    #[test]
    fn parse_gitmodules_paths_skips_unsafe_and_foreign_entries() {
        // `path` keys outside a submodule section, absolute paths, traversal
        // segments, empty values, and duplicates are all dropped.
        let content = "[core]\n\
             \tpath = not-a-submodule\n\
             [submodule \"a\"]\n\
             \tpath = /abs/path\n\
             [submodule \"b\"]\n\
             \tpath = ../escape\n\
             [submodule \"c\"]\n\
             \tpath =\n\
             [submodule \"d\"]\n\
             \tpath = sub\n\
             [submodule \"e\"]\n\
             \tpath = sub\n";
        assert_eq!(parse_gitmodules_paths(content), vec!["sub".to_string()]);
    }

    #[test]
    fn parse_gitmodules_paths_rejects_windows_absolute_and_unc_entries() {
        // Windows drive-absolute, drive-relative, UNC, and rooted-backslash
        // values must be dropped on every host platform: `Path::join`
        // replaces the base for absolute paths, so any of these escaping the
        // guard would let a hostile `.gitmodules` register an arbitrary
        // host repo.
        let content = "[submodule \"a\"]\n\
             \tpath = C:\\evil\\repo\n\
             [submodule \"b\"]\n\
             \tpath = c:/evil/repo\n\
             [submodule \"c\"]\n\
             \tpath = C:relative-but-drive-qualified\n\
             [submodule \"d\"]\n\
             \tpath = \\\\server\\share\\repo\n\
             [submodule \"e\"]\n\
             \tpath = \\rooted\\backslash\n\
             [submodule \"f\"]\n\
             \tpath = safe/sub\n";
        assert_eq!(
            parse_gitmodules_paths(content),
            vec!["safe/sub".to_string()]
        );
    }

    #[test]
    fn normalize_diff_paths_strips_absolute_entries_under_the_worktree() {
        let worktree = Path::new("/ws/root");
        let normalized = normalize_diff_paths(
            worktree,
            vec![
                "/ws/root/src/a.rs".to_string(),
                "/ws/root/deep/nested/b.txt".to_string(),
            ],
        );
        assert_eq!(
            normalized,
            vec!["src/a.rs".to_string(), "deep/nested/b.txt".to_string()]
        );
    }

    #[test]
    fn normalize_diff_paths_passes_other_entries_verbatim() {
        let worktree = Path::new("/ws/root");
        // Relative entries, the root itself, a sibling that shares the root's
        // string prefix without the directory boundary, and paths outside the
        // root all pass through unchanged.
        let entries = vec![
            "src/a.rs".to_string(),
            "/ws/root".to_string(),
            "/ws/rootbeer/c.txt".to_string(),
            "/elsewhere/d.txt".to_string(),
        ];
        assert_eq!(normalize_diff_paths(worktree, entries.clone()), entries);
    }

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

    #[test]
    fn disabled_rejection_matches_silent_skip_mark() {
        // `auto_commit.rs` treats a rejection whose message contains
        // `AUTO_COMMIT_DISABLED_MARK` as a silent skip; a rewording that
        // drops the prefix would demote every OFF-state wrap-up attempt to a
        // WARN-level failure.
        let err = assert_agent_commit_allowed(false, false).unwrap_err();
        assert!(err
            .to_string()
            .contains(crate::auto_commit::AUTO_COMMIT_DISABLED_MARK));
    }

    #[test]
    fn commit_allowed_when_enabled_or_user_requested() {
        assert!(assert_agent_commit_allowed(true, false).is_ok());
        assert!(assert_agent_commit_allowed(false, true).is_ok());
        assert!(assert_agent_commit_allowed(true, true).is_ok());
    }

    /// `git.branchDiff` on a range containing a gitlink pin bump synthesizes
    /// `Subproject commit <sha>` pseudo-content instead of failing the whole
    /// call with the typed `NotAFile` error `show_file` now returns for
    /// non-blob entries (monorepo#1739 review follow-up).
    #[test]
    fn branch_diff_gitlink_bump_synthesizes_pin_content() {
        let dir = std::env::temp_dir().join(format!("intentd-bdgl-{}", std::process::id()));
        std::fs::remove_dir_all(&dir).ok();
        std::fs::create_dir_all(&dir).unwrap();
        let repo = git2::Repository::init(&dir).unwrap();
        let mut cfg = repo.config().unwrap();
        cfg.set_str("user.name", "Test").unwrap();
        cfg.set_str("user.email", "test@example.com").unwrap();
        let sig = repo.signature().unwrap();
        let commit = |repo: &git2::Repository, msg: &str, parents: &[&git2::Commit]| {
            let tree_id = repo.index().unwrap().write_tree().unwrap();
            let tree = repo.find_tree(tree_id).unwrap();
            repo.commit(Some("HEAD"), &sig, &sig, msg, &tree, parents)
                .unwrap()
        };
        std::fs::write(dir.join("seed.txt"), "seed\n").unwrap();
        let mut index = repo.index().unwrap();
        index.add_path(Path::new("seed.txt")).unwrap();
        let old_pin = "7257a190564088376227525989c4994e46082ad1";
        index
            .add(&git2::IndexEntry {
                ctime: git2::IndexTime::new(0, 0),
                mtime: git2::IndexTime::new(0, 0),
                dev: 0,
                ino: 0,
                mode: 0o160_000,
                uid: 0,
                gid: 0,
                file_size: 0,
                id: git2::Oid::from_str(old_pin).unwrap(),
                flags: 0,
                flags_extended: 0,
                path: b"sub".to_vec(),
            })
            .unwrap();
        index.write().unwrap();
        let base_oid = commit(&repo, "base with gitlink", &[]);
        let base = repo.find_commit(base_oid).unwrap();
        // Bump the pin on the branch tip.
        let new_pin = "7908777602d4e96f13c663c8a70a617163f38413";
        let mut index = repo.index().unwrap();
        index
            .add(&git2::IndexEntry {
                ctime: git2::IndexTime::new(0, 0),
                mtime: git2::IndexTime::new(0, 0),
                dev: 0,
                ino: 0,
                mode: 0o160_000,
                uid: 0,
                gid: 0,
                file_size: 0,
                id: git2::Oid::from_str(new_pin).unwrap(),
                flags: 0,
                flags_extended: 0,
                path: b"sub".to_vec(),
            })
            .unwrap();
        index.write().unwrap();
        commit(&repo, "bump gitlink", &[&base]);

        let result =
            build_branch_diff(&dir, None, Some(&base_oid.to_string()), "HEAD", None).unwrap();
        let items = result.as_array().unwrap();
        let sub = items.iter().find(|i| i["file"] == "sub").unwrap();
        assert_eq!(sub["oldContent"], format!("Subproject commit {old_pin}\n"));
        assert_eq!(sub["newContent"], format!("Subproject commit {new_pin}\n"));
        std::fs::remove_dir_all(&dir).ok();
    }
}
