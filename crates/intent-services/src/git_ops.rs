//! Wire-policy glue for the `git.*` methods (§5.6).
//!
//! Worktree-path resolution, the `git.stage` CSV/array parse + `.`/`*`/`--all`
//! rejection (ported from the TS `ws.git.stage` builder), and the
//! `git.getBranches` "known repo" authorization check. The actual git operations
//! live in `intent-git`; this module owns only the parity-critical wire policy.

use std::path::PathBuf;

use intent_core::{Error, Result, Workspace};
use serde_json::Value;

/// TS `ws.git.stage` message when an agent tries to stage everything.
const STAGE_ALL_MSG: &str = "Staging all files is not allowed. Please specify individual file paths to stage. Use git_status to see which files you have modified, then stage only those specific files.";

/// TS `ws.git.stage` message when no usable paths remain after parsing.
const NO_PATHS_MSG: &str =
    "No file paths provided. Please specify at least one file path to stage.";

/// Resolve a workspace's worktree path: the explicit `worktreePath`, else the
/// repository `path`. `None` when neither is set.
pub(crate) fn worktree_path(ws: &Workspace) -> Option<PathBuf> {
    ws.worktree_path
        .as_ref()
        .or(ws.path.as_ref())
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
