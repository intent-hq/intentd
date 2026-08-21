//! Wire-policy glue for the `file-tracking.*` read methods (§5.19).
//!
//! Pure converters between the M4.7 persistence rows (`tracked_changes`) plus the
//! `intent-git` commit-history records and the parity-exact wire shapes
//! (`TrackedChange` / `CommitWithAttribution`, camelCase). The store/git calls
//! themselves live in the `WorkspaceApi` impl; this module owns only the
//! shape/filter policy so it stays unit-testable.
//!
//! PARITY NOTES (flagged against the TS file-tracking ground truth):
//! - `getChanges` filters on `stage` / `agentId` / `filePattern` only, matching
//!   `file-tracking.service.ts` `getChanges` (its `sessionId`/`turnNumber`/date
//!   filters are no-ops there). The PROTOCOL §5.19 `filter` lists
//!   `sessionId`/`turnNumber`/`since`/`until`; those are accepted but ignored, as
//!   in TS. `filter.stage` is accepted as a string or an array.
//! - `AgentAttribution.agentName` is not persisted on `tracked_changes` (M4.7),
//!   so it is emitted as an empty string until the agent name is sourced.
//! - The wire `TrackedChange.id` is the persisted row id (`UUIDv7`), not the TS
//!   synthetic `git-<n>-<path>` id, since intentd reads persisted rows.

use std::path::Path;

use intent_core::{parse_iso, Error, Result};
use intent_git::history::CommitRecord;
use intent_store::TrackedChangeRow;
use serde_json::{json, Map, Value};

/// The per-workspace tracked-change cap (TS `TRACKING_CONFIG.fileTracking
/// .maxTrackedFiles`). Beyond it `load`/`getChanges` report `truncated:true` and
/// return only the most recent rows.
pub(crate) const MAX_TRACKED_FILES: usize = 1000;

/// A parsed `getChanges` filter (the subset TS actually applies).
#[derive(Debug, Default, Clone)]
pub(crate) struct ChangeFilterParsed {
    pub stage: Option<Vec<String>>,
    pub agent_id: Option<String>,
    pub file_pattern: Option<String>,
}

impl ChangeFilterParsed {
    /// Whether every set predicate matches a row (`file_abs` is the absolute path
    /// the TS `filePattern` substring test runs against).
    pub fn matches(&self, row: &TrackedChangeRow, file_abs: &str) -> bool {
        if let Some(stages) = &self.stage {
            if !stages.iter().any(|s| s == &row.stage) {
                return false;
            }
        }
        if let Some(agent) = &self.agent_id {
            if row.agent_id.as_deref() != Some(agent.as_str()) {
                return false;
            }
        }
        if let Some(pattern) = &self.file_pattern {
            if !file_abs.contains(pattern) {
                return false;
            }
        }
        true
    }
}

/// Parse the `getChanges` `filter` object. Absent/null → an all-pass filter.
/// `stage` accepts a string or an array of strings.
pub(crate) fn parse_filter(filter: Option<&Value>) -> ChangeFilterParsed {
    let Some(Value::Object(map)) = filter else {
        return ChangeFilterParsed::default();
    };
    ChangeFilterParsed {
        stage: parse_stage_filter(map.get("stage")),
        agent_id: opt_str(map.get("agentId")),
        file_pattern: opt_str(map.get("filePattern")),
    }
}

fn parse_stage_filter(value: Option<&Value>) -> Option<Vec<String>> {
    match value {
        Some(Value::String(s)) if !s.is_empty() => Some(vec![s.clone()]),
        Some(Value::Array(items)) => {
            let stages: Vec<String> = items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect();
            if stages.is_empty() {
                None
            } else {
                Some(stages)
            }
        }
        _ => None,
    }
}

fn opt_str(value: Option<&Value>) -> Option<String> {
    match value {
        Some(Value::String(s)) if !s.is_empty() => Some(s.clone()),
        _ => None,
    }
}

/// Parse the `stage`/`unstage` `paths` param: an array of strings or a CSV
/// string, trimmed with empties dropped. An empty result is `-32603` with the TS
/// "No file paths provided" message.
pub(crate) fn parse_paths(paths: &Value) -> Result<Vec<String>> {
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
        return Err(Error::Internal(
            "No file paths provided. Please specify at least one file path.".to_string(),
        ));
    }
    Ok(list)
}

/// Convert an RFC-3339 timestamp into epoch milliseconds (the TS
/// `attribution.timestamp` numeric form). Malformed input → `0`.
pub(crate) fn iso_to_millis(iso: &str) -> i64 {
    parse_iso(iso).map_or(0, |dt| {
        i64::try_from(dt.unix_timestamp_nanos() / 1_000_000).unwrap_or(0)
    })
}

/// The absolute file path for a row: `<worktree>/<relativePath>` when a worktree
/// is known, else the relative path (TS `TrackedChange.file`).
pub(crate) fn absolute_path(worktree: Option<&Path>, rel_path: &str) -> String {
    match worktree {
        Some(w) => w.join(rel_path).to_string_lossy().to_string(),
        None => rel_path.to_string(),
    }
}

/// Build the wire `TrackedChange` for a row (camelCase, PROTOCOL §5.18 shape).
pub(crate) fn row_to_tracked_change(row: &TrackedChangeRow, worktree: Option<&Path>) -> Value {
    let file = absolute_path(worktree, &row.path);
    let timestamp = iso_to_millis(&row.updated_at);

    let mut attribution = Map::new();
    if let Some(agent_id) = &row.agent_id {
        attribution.insert(
            "agent".to_string(),
            json!({
                "agentId": agent_id,
                "agentName": "",
                "sessionId": row.session_id.clone().unwrap_or_default(),
                "turnNumber": row.turn.unwrap_or(0),
                "timestamp": timestamp,
            }),
        );
    }
    attribution.insert("timestamp".to_string(), json!(timestamp));

    json!({
        "id": row.id,
        "file": file,
        "relativePath": row.path,
        "stage": row.stage,
        "status": row.status,
        "stats": { "additions": row.additions, "deletions": row.deletions },
        "attribution": Value::Object(attribution),
    })
}

/// Build the `{ changes, truncated, totalCount }` result for `load`/`getChanges`
/// (TS `getChanges`): `totalCount` is the pre-filter row count; when it exceeds
/// [`MAX_TRACKED_FILES`] the rows are sorted newest-first and capped (`truncated`
/// set), then the filter is applied to what remains.
pub(crate) fn build_changes_result(
    mut rows: Vec<TrackedChangeRow>,
    worktree: Option<&Path>,
    filter: &ChangeFilterParsed,
) -> Value {
    let total_count = rows.len();
    let truncated = total_count > MAX_TRACKED_FILES;
    if truncated {
        rows.sort_by_key(|r| std::cmp::Reverse(iso_to_millis(&r.updated_at)));
        rows.truncate(MAX_TRACKED_FILES);
    }
    let changes: Vec<Value> = rows
        .iter()
        .filter(|r| filter.matches(r, &absolute_path(worktree, &r.path)))
        .map(|r| row_to_tracked_change(r, worktree))
        .collect();
    json!({ "changes": changes, "truncated": truncated, "totalCount": total_count })
}

/// Convert a stage/unstage `paths` entry to the worktree-relative form used as
/// the `tracked_changes.path` lookup key: an absolute path is made relative to
/// `worktree` (TS `path.relative`); a relative path is kept as-is.
pub(crate) fn worktree_relative(worktree: &Path, raw: &str) -> String {
    let p = Path::new(raw);
    if p.is_absolute() {
        p.strip_prefix(worktree)
            .map_or_else(|_| raw.to_string(), |r| r.to_string_lossy().to_string())
    } else {
        raw.to_string()
    }
}

/// Build the wire `CommitWithAttribution` for a history record (PROTOCOL §5.18).
/// `files`/`filesChanged` are emitted only when the record carries a computed
/// file list; records from a metadata-only walk (`include_files = false`) omit
/// both — clients fetch per-file data on demand via `git.commitDetails`.
pub(crate) fn commit_to_value(c: &CommitRecord) -> Value {
    let mut obj = Map::new();
    obj.insert("hash".to_string(), json!(c.hash));
    obj.insert("message".to_string(), json!(c.message));
    obj.insert("author".to_string(), json!(c.author));
    obj.insert("date".to_string(), json!(c.date));
    obj.insert("isPushed".to_string(), json!(c.is_pushed));
    if let Some(files) = &c.files {
        let files: Vec<Value> = files.iter().map(|p| json!({ "path": p })).collect();
        obj.insert("filesChanged".to_string(), json!(c.files_changed));
        obj.insert("files".to_string(), Value::Array(files));
    }
    if let Some(agent_id) = &c.agent_id {
        obj.insert("agentId".to_string(), json!(agent_id));
    }
    if let Some(note_id) = &c.linked_note_id {
        obj.insert("linkedNoteId".to_string(), json!(note_id));
    }
    Value::Object(obj)
}

#[cfg(test)]
mod tests {
    use super::*;
    use intent_core::WorkspaceId;

    fn row(stage: &str, agent: Option<&str>) -> TrackedChangeRow {
        TrackedChangeRow {
            id: "row-1".to_string(),
            workspace_id: WorkspaceId::from("ws-1"),
            path: "src/x.ts".to_string(),
            stage: stage.to_string(),
            status: "modified".to_string(),
            agent_id: agent.map(str::to_string),
            session_id: Some("sess-9".to_string()),
            turn: Some(4),
            commit_hash: None,
            old_blob_sha: None,
            new_blob_sha: None,
            additions: 10,
            deletions: 2,
            created_at: "2025-01-01T00:00:00Z".to_string(),
            updated_at: "2025-01-01T00:00:00Z".to_string(),
        }
    }

    #[test]
    fn parse_paths_array_and_csv() {
        assert_eq!(
            parse_paths(&json!(["a.ts", " b.ts ", ""])).unwrap(),
            vec!["a.ts".to_string(), "b.ts".to_string()]
        );
        assert_eq!(
            parse_paths(&json!(" a.ts , b.ts ,")).unwrap(),
            vec!["a.ts".to_string(), "b.ts".to_string()]
        );
        assert!(parse_paths(&json!([])).is_err());
        assert!(parse_paths(&json!("   ,  ")).is_err());
    }

    #[test]
    fn filter_matches_stage_agent_and_pattern() {
        let r = row("staged", Some("agent-123"));
        let abs = absolute_path(Some(Path::new("/ws")), &r.path);
        let f = parse_filter(Some(&json!({ "stage": "staged", "agentId": "agent-123" })));
        assert!(f.matches(&r, &abs));
        let f2 = parse_filter(Some(&json!({ "stage": ["committed"] })));
        assert!(!f2.matches(&r, &abs));
        let f3 = parse_filter(Some(&json!({ "agentId": "agent-999" })));
        assert!(!f3.matches(&r, &abs));
        let f4 = parse_filter(Some(&json!({ "filePattern": "src/" })));
        assert!(f4.matches(&r, &abs));
        let f5 = parse_filter(Some(&json!({ "filePattern": "nope" })));
        assert!(!f5.matches(&r, &abs));
    }

    #[test]
    fn empty_filter_passes_all() {
        let r = row("unstaged", None);
        let abs = absolute_path(None, &r.path);
        assert!(parse_filter(None).matches(&r, &abs));
        assert!(parse_filter(Some(&json!({}))).matches(&r, &abs));
    }

    #[test]
    fn tracked_change_shape_is_parity_exact() {
        let r = row("committed", Some("agent-123"));
        let v = row_to_tracked_change(&r, Some(Path::new("/ws")));
        assert_eq!(v["id"], json!("row-1"));
        assert_eq!(v["file"], json!("/ws/src/x.ts"));
        assert_eq!(v["relativePath"], json!("src/x.ts"));
        assert_eq!(v["stage"], json!("committed"));
        assert_eq!(v["status"], json!("modified"));
        assert_eq!(v["stats"], json!({ "additions": 10, "deletions": 2 }));
        let agent = &v["attribution"]["agent"];
        assert_eq!(agent["agentId"], json!("agent-123"));
        assert_eq!(agent["sessionId"], json!("sess-9"));
        assert_eq!(agent["turnNumber"], json!(4));
        assert!(v["attribution"]["timestamp"].is_number());
    }

    #[test]
    fn tracked_change_without_agent_omits_agent() {
        let r = row("unstaged", None);
        let v = row_to_tracked_change(&r, None);
        assert!(v["attribution"].get("agent").is_none());
        assert!(v["attribution"]["timestamp"].is_number());
    }

    #[test]
    fn commit_shape_includes_attribution_when_present() {
        let c = CommitRecord {
            hash: "abc".to_string(),
            message: "msg".to_string(),
            author: "Test".to_string(),
            author_email: "t@e.com".to_string(),
            date: "2025-01-01T00:00:00Z".to_string(),
            files: Some(vec!["a.ts".to_string()]),
            files_changed: 1,
            is_pushed: true,
            agent_id: Some("agent-1".to_string()),
            linked_note_id: None,
        };
        let v = commit_to_value(&c);
        assert_eq!(v["hash"], json!("abc"));
        assert_eq!(v["filesChanged"], json!(1));
        assert_eq!(v["isPushed"], json!(true));
        assert_eq!(v["files"], json!([{ "path": "a.ts" }]));
        assert_eq!(v["agentId"], json!("agent-1"));
        assert!(v.get("linkedNoteId").is_none());
    }

    #[test]
    fn commit_shape_omits_files_when_not_computed() {
        let c = CommitRecord {
            hash: "abc".to_string(),
            message: "msg".to_string(),
            author: "Test".to_string(),
            author_email: "t@e.com".to_string(),
            date: "2025-01-01T00:00:00Z".to_string(),
            files: None,
            files_changed: 0,
            is_pushed: false,
            agent_id: None,
            linked_note_id: None,
        };
        let v = commit_to_value(&c);
        assert_eq!(v["hash"], json!("abc"));
        assert_eq!(v["isPushed"], json!(false));
        assert!(v.get("files").is_none());
        assert!(v.get("filesChanged").is_none());
    }
}
