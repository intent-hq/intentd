//! Wire-policy glue for the `git.*` methods (§5.6).
//!
//! Worktree-path resolution, the `git.stage` CSV/array parse + `.`/`*`/`--all`
//! rejection (ported from the TS `ws.git.stage` builder), and the
//! `git.getBranches` "known repo" authorization check. The actual git operations
//! live in `intent-git`; this module owns only the parity-critical wire policy.

use std::path::{Path, PathBuf};

use intent_core::{Error, Result, Workspace};
use intent_store::Store;
use serde_json::{json, Map, Value};

/// TS `ws.git.stage` message when an agent tries to stage everything.
const STAGE_ALL_MSG: &str = "Staging all files is not allowed. Please specify individual file paths to stage. Use git_status to see which files you have modified, then stage only those specific files.";

/// TS `ws.git.stage` message when no usable paths remain after parsing.
const NO_PATHS_MSG: &str =
    "No file paths provided. Please specify at least one file path to stage.";

/// TS `assertAgentCommitAllowed` rejection message (auto-commit disabled).
const AUTO_COMMIT_DISABLED_MSG: &str = "Auto-commit is disabled for this workspace. \
Use agent_commit_changes with userRequested: true if the user asked you to commit.";

/// Port of `assertAgentCommitAllowed`: block an agent-initiated commit when
/// auto-commit is disabled, unless `user_requested` bypasses it. The gate reads
/// the persisted `git.autoCommit` setting (§9.8 OQ#2), which defaults to `true`
/// so the established behavior is preserved when the setting is unset.
pub(crate) async fn assert_agent_commit_allowed(store: &Store, user_requested: bool) -> Result<()> {
    if user_requested || crate::settings::auto_commit_enabled(store).await {
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

/// Whether `repo_path` matches a known workspace path or worktree path. Mirrors
/// the TS `getAllRepos()` authorization check (intentd derives the known set
/// from persisted workspaces, archived included, rather than a separate
/// registry).
pub(crate) fn is_known_repo(workspaces: &[Workspace], repo_path: &str) -> bool {
    workspaces.iter().any(|ws| {
        ws.path.as_deref() == Some(repo_path) || ws.worktree_path.as_deref() == Some(repo_path)
    })
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
}
