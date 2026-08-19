//! `services::diffs` — internal diff computation + storage (§17.3, §9.11).
//!
//! BE-internal: there are **no** `diffs.*` wire methods (diffs surface to the FE
//! via file-tracking reads + change events). [`compute_and_store`] runs one
//! pathspec-narrowed `intent-git` traversal that yields the per-file summary
//! **and** its hunks together, persists the hunks as the `diffs.hunks_json`
//! index, and returns the additions/deletions + blob SHAs the attribution writer
//! records on `tracked_changes`. Per §3.2 this depends only on `intent-store` and
//! `intent-git`; it never imports a sibling service module (e.g. `file_tracking`).

use std::path::Path;

use intent_core::{Result, WorkspaceId};
use intent_git::diff::{diff_index_to_workdir_with_hunks, DiffHunk, DiffLineKind};
use intent_store::{NewDiff, Store};
use serde_json::{json, Value};

/// The per-file stats the diff compute yields, consumed by the attribution
/// writer (§17.4) to populate `tracked_changes`. Content stays lazy via the blob
/// SHAs rather than being inlined.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct DiffSummary {
    pub additions: i64,
    pub deletions: i64,
    pub old_blob_sha: Option<String>,
    pub new_blob_sha: Option<String>,
    pub is_binary: bool,
}

/// Compute the index→workdir diff for `rel_path`, persist its hunks into the
/// `diffs` table (keyed by `(workspace, file, staged)`), and return the summary.
/// Returns `Ok(None)` when the path has no pending change in the worktree.
///
/// Cost: a **single** pathspec-narrowed traversal — the summary and hunks come
/// from one walk pruned to `rel_path`, not two full-tree scans.
///
/// Full file content is **not** inlined (`old_content`/`new_content` stay NULL);
/// content is recoverable lazily via the blob SHAs in the returned summary.
pub(crate) async fn compute_and_store(
    store: &Store,
    worktree_path: &Path,
    workspace_id: &WorkspaceId,
    rel_path: &str,
    staged: bool,
) -> Result<Option<DiffSummary>> {
    let entries = diff_index_to_workdir_with_hunks(worktree_path, Some(&[rel_path]))?;
    // The pathspec prunes the walk but can match more than the exact path;
    // keep the strict equality match.
    let Some(entry) = entries.into_iter().find(|e| e.file.path == rel_path) else {
        return Ok(None);
    };
    let fd = entry.file;

    let hunks_json = if fd.is_binary {
        "[]".to_string()
    } else {
        serialize_hunks(&entry.hunks)
    };

    store
        .upsert_diff(&NewDiff {
            workspace_id: workspace_id.clone(),
            file_path: rel_path.to_string(),
            staged,
            old_content: None,
            new_content: None,
            hunks_json,
        })
        .await?;

    Ok(Some(DiffSummary {
        additions: fd.additions as i64,
        deletions: fd.deletions as i64,
        old_blob_sha: fd.old_blob,
        new_blob_sha: fd.new_blob,
        is_binary: fd.is_binary,
    }))
}

/// Serialize git hunks into the TS `DiffHunk[]` wire shape (`oldStart`/`oldLines`
/// /`newStart`/`newLines` + camelCase line records with `add|remove|context`),
/// matching `extract-change-hunks.ts` consumers.
fn serialize_hunks(hunks: &[DiffHunk]) -> String {
    let arr: Vec<Value> = hunks.iter().map(hunk_to_value).collect();
    serde_json::to_string(&arr).unwrap_or_else(|_| "[]".to_string())
}

fn hunk_to_value(h: &DiffHunk) -> Value {
    let lines: Vec<Value> = h
        .lines
        .iter()
        .map(|l| {
            json!({
                "type": line_kind_word(l.kind),
                "content": l.content,
                "oldLineNumber": l.old_lineno,
                "newLineNumber": l.new_lineno,
            })
        })
        .collect();
    json!({
        "oldStart": h.old_start,
        "oldLines": h.old_lines,
        "newStart": h.new_start,
        "newLines": h.new_lines,
        "lines": lines,
    })
}

fn line_kind_word(kind: DiffLineKind) -> &'static str {
    match kind {
        DiffLineKind::Addition => "add",
        DiffLineKind::Deletion => "remove",
        DiffLineKind::Context => "context",
    }
}
