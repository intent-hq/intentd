//! Wire glue for the `search.*` methods (§5.15).
//!
//! Resolves a workspace's search root (its worktree) and parses the
//! `search.inFiles` `opts` object. The ripgrep-equivalent walk/search itself
//! lives in `intent-search`; this module owns only the services-layer glue.

use std::path::PathBuf;

use intent_core::{Error, Event, Result, RetrieveResult, WorkspaceId};
use intent_search::{
    contains_ci, extract_symbol, fts_preview, make_preview, CodebaseMatch, ContentSearchResult,
    EventMatch, MessageMatch, NoteMatch,
};
use intent_store::{MessageFtsMatch, NoteFtsCursor, NoteFtsMatch, NoteFtsOptions, Store};
use serde_json::Value;

/// Result sets at or below this many matches are returned inline in the method
/// result; larger sets are streamed via `search:result`/`search:done` (§6.5).
pub(crate) const INLINE_THRESHOLD: usize = 25;

/// Number of matches per streamed `search:result` batch.
pub(crate) const STREAM_BATCH_SIZE: usize = 10;

/// `search.messages` / `search.events` match count when `limit` is absent
/// (§5.15). Before the read payload budget an absent limit meant no cap.
pub(crate) const MATCH_LIMIT_DEFAULT: usize = 50;

/// Largest `search.messages` / `search.events` `limit`; larger values clamp.
pub(crate) const MATCH_LIMIT_MAX: usize = 200;

/// Serialized-size ceiling for the matches of one inline `search.*` reply,
/// 480,000 bytes (the per-page budget shared by bounded reads), below the
/// 500 KiB read payload budget with room for the envelope. Matches
/// past it are dropped and the reply carries `truncated: true`.
pub(crate) const INLINE_MATCHES_BUDGET_BYTES: usize = 480_000;

/// Resolve a `search.messages` / `search.events` `limit`: absent → the
/// default, negative → 0, over the maximum → the maximum.
pub(crate) fn match_limit(limit: Option<i64>) -> usize {
    limit.map_or(MATCH_LIMIT_DEFAULT, |n| {
        usize::try_from(n.max(0))
            .unwrap_or(usize::MAX)
            .min(MATCH_LIMIT_MAX)
    })
}

/// Keep the longest prefix of `matches` whose serialized size fits
/// [`INLINE_MATCHES_BUDGET_BYTES`]; returns whether any match was dropped.
pub(crate) fn bound_inline_matches(matches: &mut Vec<Value>) -> bool {
    let mut used = 0usize;
    for (i, m) in matches.iter().enumerate() {
        used += intent_core::slim_body_size(m) + 1;
        if used > INLINE_MATCHES_BUDGET_BYTES {
            matches.truncate(i);
            return true;
        }
    }
    false
}

/// `search.notes` result budget: 480,000 serialized bytes (the shared
/// per-page budget), so the frame stays under 512,000 bytes. Hits past it
/// move to the next page through `nextToken`; none is dropped.
pub(crate) const NOTE_PAGE_BUDGET_BYTES: usize = 480_000;

/// Previous hits a `search.notes` `nextToken` carries as resume anchors.
const NOTE_PAGE_ANCHORS: usize = 8;

/// Digest of the request fields that select a `search.notes` result list
/// (query, hard scope, preference, archive filter), so a `nextToken` cannot
/// continue a different search and stays small for a long query.
pub(crate) fn note_page_scope(
    query: &str,
    workspace_id: Option<&WorkspaceId>,
    prefer_workspace_id: Option<&WorkspaceId>,
    include_archived: bool,
) -> String {
    use sha2::{Digest, Sha256};
    use std::fmt::Write;
    let key = serde_json::json!([
        "search.notes",
        query,
        workspace_id.map(WorkspaceId::as_str),
        prefer_workspace_id.map(WorkspaceId::as_str),
        include_archived,
    ]);
    Sha256::digest(key.to_string().as_bytes()).iter().fold(
        String::with_capacity(64),
        |mut out, byte| {
            let _ = write!(out, "{byte:02x}");
            out
        },
    )
}

fn cursor_json(key: &NoteFtsCursor) -> Value {
    serde_json::json!({
        "rank": key.rank,
        "updatedAt": key.updated_at,
        "workspaceId": key.workspace_id,
        "noteId": key.note_id,
    })
}

/// `nextToken` resuming a `search.notes` result list after `anchors` (the
/// page's last hits, in ranked order): versioned JSON scoped to the search
/// ([`note_page_scope`]).
fn encode_note_page_token(scope: &str, anchors: &[NoteFtsCursor]) -> String {
    let anchors: Vec<Value> = anchors.iter().map(cursor_json).collect();
    serde_json::json!({ "version": 2, "method": "search.notes", "scope": scope, "anchors": anchors })
        .to_string()
}

/// The resume anchors of a `search.notes` `nextToken`, oldest first; a
/// malformed token or one minted for a different search is `InvalidParams`.
pub(crate) fn decode_note_page_token(token: &str, scope: &str) -> Result<Vec<NoteFtsCursor>> {
    let invalid = || Error::InvalidParams("search.notes: invalid or mismatched nextToken".into());
    let cursor: Value = serde_json::from_str(token).map_err(|_| invalid())?;
    if cursor.get("version") != Some(&Value::from(2))
        || cursor.get("method").and_then(Value::as_str) != Some("search.notes")
        || cursor.get("scope").and_then(Value::as_str) != Some(scope)
    {
        return Err(invalid());
    }
    let anchors = cursor
        .get("anchors")
        .and_then(Value::as_array)
        .filter(|a| !a.is_empty() && a.len() <= NOTE_PAGE_ANCHORS)
        .ok_or_else(invalid)?;
    anchors
        .iter()
        .map(|a| {
            let text = |k: &str| a.get(k).and_then(Value::as_str).map(str::to_string);
            Some(NoteFtsCursor {
                rank: a.get("rank").and_then(Value::as_f64)?,
                updated_at: text("updatedAt")?,
                workspace_id: text("workspaceId")?,
                note_id: text("noteId")?,
            })
        })
        .collect::<Option<Vec<_>>>()
        .ok_or_else(invalid)
}

/// Where the next `search.notes` page starts. Ranks move with the index
/// statistics (deleting one matching note re-weights every other hit), so
/// the page re-anchors on the newest anchor that still matches and was not
/// itself edited (same `updatedAt`), at its current position: hits after
/// it keep their order relative to it and are not skipped. When every
/// anchor was deleted or edited, the newest anchor's stored key is used.
pub(crate) async fn note_page_resume(
    store: &Store,
    match_expr: &str,
    options: &NoteFtsOptions<'_>,
    anchors: Vec<NoteFtsCursor>,
) -> Result<Option<NoteFtsCursor>> {
    let ids: Vec<(String, String)> = anchors
        .iter()
        .map(|a| (a.workspace_id.clone(), a.note_id.clone()))
        .collect();
    let current = store.note_fts_positions(match_expr, options, &ids).await?;
    let anchored = anchors.iter().rev().find_map(|a| {
        current.iter().find(|c| {
            c.workspace_id == a.workspace_id
                && c.note_id == a.note_id
                && c.updated_at == a.updated_at
        })
    });
    Ok(anchored.cloned().or_else(|| anchors.last().cloned()))
}

/// Shorten the `title` of a hit whose serialized size exceeds `budget` to
/// fit, on a UTF-8 boundary, adding `titleTruncated: true` and `titleBytes`
/// (the full title's UTF-8 length; `note.get` reads it whole). The title
/// stays a string so older clients still accept the hit.
fn fit_note_hit(row: &mut Value, budget: usize) {
    use crate::pagination::{floor_char_boundary, serialized_size};
    if serialized_size(row) <= budget {
        return;
    }
    let Some(title) = row.get("title").and_then(Value::as_str).map(str::to_owned) else {
        return;
    };
    row["titleTruncated"] = Value::Bool(true);
    row["titleBytes"] = Value::from(title.len());
    let mut keep = title.len();
    loop {
        let over = serialized_size(row).saturating_sub(budget);
        if over == 0 || keep == 0 {
            return;
        }
        keep = floor_char_boundary(&title, keep.saturating_sub(over));
        row["title"] = Value::String(title[..keep].to_string());
    }
}

/// Build one `search.notes` page from the ranked `hits` after the page
/// start, fetched with `limit + 1` so a further page is detectable; `keys`
/// are their sort keys. The page keeps at most `limit` hits that fit
/// [`NOTE_PAGE_BUDGET_BYTES`] (always at least one), and `nextToken`
/// resumes right after the last kept hit (`null` when none remain).
pub(crate) fn note_page_reply(
    request_id: &str,
    hits: Vec<NoteMatch>,
    keys: &[NoteFtsCursor],
    limit: usize,
    scope: &str,
) -> Result<Value> {
    let more_rows = hits.len() > limit;
    let mut rows = crate::to_value_vec(hits.into_iter().take(limit).collect())?;
    // Reserve the largest token any cut could mint.
    let mut widest: Vec<&NoteFtsCursor> = keys.iter().take(limit).collect();
    widest.sort_by_key(|k| std::cmp::Reverse(cursor_json(k).to_string().len()));
    widest.truncate(NOTE_PAGE_ANCHORS);
    let widest: Vec<NoteFtsCursor> = widest.into_iter().cloned().collect();
    let envelope = serde_json::json!({
        "requestId": request_id,
        "matches": [],
        "indexed": true,
        "nextToken": encode_note_page_token(scope, &widest),
    });
    let budget = NOTE_PAGE_BUDGET_BYTES.saturating_sub(intent_core::slim_body_size(&envelope));
    for row in &mut rows {
        fit_note_hit(row, budget.saturating_sub(3));
    }
    let kept = crate::pagination::fit_rows_to_budget(&rows, budget);
    let cut = kept < rows.len();
    rows.truncate(kept);
    let next_token = (kept > 0 && (cut || more_rows)).then(|| {
        encode_note_page_token(scope, &keys[kept.saturating_sub(NOTE_PAGE_ANCHORS)..kept])
    });
    Ok(serde_json::json!({
        "requestId": request_id,
        "matches": rows,
        "indexed": true,
        "nextToken": next_token,
    }))
}

/// Resolve a workspace's search root (its worktree path), or `None` when the
/// workspace has no usable path (remote/non-repo) — callers return an empty
/// result in that case. When `caller_agent_id` is provided and the agent has a
/// sandbox, the search root is the sandbox path (sandboxed agent containment).
pub(crate) async fn search_root(
    store: &Store,
    workspace_id: &WorkspaceId,
    caller_agent_id: Option<&intent_core::AgentId>,
) -> Result<Option<PathBuf>> {
    if let Some(agent_id) = caller_agent_id {
        if let Ok(session) = store.get_agent_session_summary(agent_id).await {
            if let Some(sandbox_path) = session.sandbox_path.clone() {
                return Ok(Some(sandbox_path.into()));
            }
        }
    }
    let ws = store.get_workspace(workspace_id).await?;
    Ok(crate::git_ops::worktree_path(&ws))
}

/// `search.inFiles` `opts.maxResults` when absent (§5.15).
pub(crate) const IN_FILES_MAX_RESULTS_DEFAULT: usize = 500;

/// Largest `search.inFiles` `opts.maxResults`; larger values clamp.
pub(crate) const IN_FILES_MAX_RESULTS_MAX: usize = 2_000;

/// Parse the raw `opts` object into [`SearchOpts`]; an unusable shape surfaces
/// as `InvalidParams` (→ `-32602`). Absent/null yields the defaults.
pub(crate) fn parse_opts(opts: Option<Value>) -> Result<intent_search::SearchOpts> {
    match opts {
        None | Some(Value::Null) => Ok(intent_search::SearchOpts::default()),
        Some(value) => serde_json::from_value(value)
            .map_err(|_| Error::InvalidParams("Invalid search opts".to_string())),
    }
}

/// Bound a `search.inFiles` request: `maxResults` defaults to
/// [`IN_FILES_MAX_RESULTS_DEFAULT`] and clamps to
/// [`IN_FILES_MAX_RESULTS_MAX`], and the matches stop at
/// [`INLINE_MATCHES_BUDGET_BYTES`]. Either cut sets `truncated`.
pub(crate) fn bound_in_files_opts(opts: &mut intent_search::SearchOpts) {
    opts.max_results = Some(
        opts.max_results
            .unwrap_or(IN_FILES_MAX_RESULTS_DEFAULT)
            .min(IN_FILES_MAX_RESULTS_MAX),
    );
    opts.max_bytes = Some(INLINE_MATCHES_BUDGET_BYTES);
}

/// Extract searchable plain text from an agent message's JSON `content`. A bare
/// string is used as-is; an array of content blocks contributes each block's
/// `text` field (mirroring the renderer's `messageSearch` extraction); any other
/// shape falls back to its compact JSON encoding.
pub(crate) fn message_text(content: &Value) -> String {
    match content {
        Value::String(s) => s.clone(),
        Value::Array(blocks) => blocks
            .iter()
            .filter_map(|b| b.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join(" "),
        other => other.to_string(),
    }
}

/// Map ranked FTS hits ([`Store::search_agent_messages_fts`]) into
/// `search.messages` wire matches: the preview windows onto the first
/// raw-query token occurring in the extracted text, and the wire `score`
/// negates the adjusted bm25 rank so higher = more relevant. One match per
/// matching message — no per-agent collapse.
pub(crate) fn message_fts_matches(
    hits: Vec<MessageFtsMatch>,
    raw_query: &str,
) -> Vec<MessageMatch> {
    hits.into_iter()
        .map(|hit| MessageMatch {
            agent_id: hit.agent_id,
            message_id: hit.message_id,
            preview: fts_preview(&message_text(&hit.content), raw_query),
            score: Some(-hit.rank),
            workspace_id: hit.workspace_id,
            agent_name: hit.agent_name,
            role: hit.role,
            timestamp: hit.created_at,
        })
        .collect()
}

/// Build `search.events` matches over the event log. The searchable text is the
/// event `type` plus its JSON `data`; `limit` caps the number of matches.
pub(crate) fn event_matches(events: &[Event], query: &str, limit: usize) -> Vec<EventMatch> {
    let mut out = Vec::new();
    if limit == 0 {
        return out;
    }
    for ev in events {
        let text = format!("{} {}", ev.event_type, ev.data);
        if !contains_ci(&text, query) {
            continue;
        }
        out.push(EventMatch {
            event_id: ev.id.clone(),
            preview: make_preview(&text, query),
            score: None,
        });
        if out.len() >= limit {
            break;
        }
    }
    out
}

/// Build bounded, plain-text previews only for the indexed winners, preserving
/// the store's ranked order and negating its lower-is-better rank for the wire.
pub(crate) fn note_fts_matches(hits: Vec<NoteFtsMatch>, query: &str) -> Vec<NoteMatch> {
    hits.into_iter()
        .map(|hit| {
            let text = format!("{} {} {}", hit.title, hit.content, hit.tags.join(" "));
            NoteMatch {
                note_id: hit.note_id,
                workspace_id: hit.workspace_id,
                title: hit.title,
                preview: fts_preview(&text, query),
                score: Some(-hit.rank),
                updated_at: hit.updated_at,
                is_archived: hit.is_archived,
                workspace_archived: hit.workspace_archived,
            }
        })
        .collect()
}

/// Map a ripgrep content-search result into `search.codebase` matches, attaching
/// a lightweight detected symbol (and a small score boost) when the matching
/// line looks like a definition.
pub(crate) fn codebase_matches(content: &ContentSearchResult) -> Vec<CodebaseMatch> {
    content
        .matches
        .iter()
        .map(|m| {
            let symbol = extract_symbol(&m.preview);
            let score = if symbol.is_some() { 1.0 } else { 0.5 };
            CodebaseMatch {
                file: m.file.clone(),
                symbol,
                line: Some(m.line),
                preview: m.preview.clone(),
                score: Some(score),
            }
        })
        .collect()
}

/// Map context-engine retrieval hits into `search.codebase` matches, preserving
/// the engine's file/symbol/line/preview and optional relevance `score` (§5.15
/// parity; engine hits may carry a `score`).
pub(crate) fn engine_matches(result: &RetrieveResult) -> Vec<CodebaseMatch> {
    result
        .items
        .iter()
        .map(|item| CodebaseMatch {
            file: item.file.clone(),
            symbol: item.symbol.clone(),
            line: item.line,
            preview: item.preview.clone(),
            score: item.score,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn match_limit_defaults_and_clamps() {
        assert_eq!(match_limit(None), MATCH_LIMIT_DEFAULT);
        assert_eq!(match_limit(Some(10)), 10);
        assert_eq!(match_limit(Some(0)), 0);
        assert_eq!(match_limit(Some(-1)), 0);
        assert_eq!(match_limit(Some(100_000)), MATCH_LIMIT_MAX);
    }

    #[test]
    fn inline_matches_are_cut_at_the_budget() {
        let small = vec![serde_json::json!({ "preview": "x" }); 10];
        let mut untouched = small.clone();
        assert!(!bound_inline_matches(&mut untouched));
        assert_eq!(untouched, small);

        let big = serde_json::json!({ "preview": "x".repeat(10 * 1024) });
        let mut matches = vec![big; 100];
        assert!(bound_inline_matches(&mut matches));
        let bytes = serde_json::to_vec(&matches).unwrap().len();
        assert!(bytes <= INLINE_MATCHES_BUDGET_BYTES, "{bytes}");
        assert!(matches.len() >= 40, "{}", matches.len());
    }

    #[test]
    fn in_files_opts_default_and_clamp_max_results() {
        let mut opts = parse_opts(None).unwrap();
        bound_in_files_opts(&mut opts);
        assert_eq!(opts.max_results, Some(IN_FILES_MAX_RESULTS_DEFAULT));
        assert_eq!(opts.max_bytes, Some(INLINE_MATCHES_BUDGET_BYTES));

        let mut opts = parse_opts(Some(serde_json::json!({ "maxResults": 100_000 }))).unwrap();
        bound_in_files_opts(&mut opts);
        assert_eq!(opts.max_results, Some(IN_FILES_MAX_RESULTS_MAX));

        let mut opts = parse_opts(Some(serde_json::json!({ "maxResults": 7 }))).unwrap();
        bound_in_files_opts(&mut opts);
        assert_eq!(opts.max_results, Some(7));
    }

    #[test]
    fn event_matches_honour_limit() {
        let events: Vec<Event> = (0..5)
            .map(|i| {
                serde_json::from_value(serde_json::json!({
                    "id": format!("e-{i}"),
                    "workspaceId": "ws-1",
                    "type": "agent:idle",
                    "timestamp": "2026-11-02T00:00:00Z",
                    "actor": { "type": "system" },
                    "data": {},
                }))
                .unwrap()
            })
            .collect();
        assert_eq!(event_matches(&events, "agent", 3).len(), 3);
        assert!(event_matches(&events, "agent", 0).is_empty());
    }
}
