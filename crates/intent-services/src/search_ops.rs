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
use intent_store::{MessageFtsMatch, NoteFtsMatch, Store};
use serde_json::Value;

/// Result sets at or below this many matches are returned inline in the method
/// result; larger sets are streamed via `search:result`/`search:done` (§6.5).
pub(crate) const INLINE_THRESHOLD: usize = 25;

/// Number of matches per streamed `search:result` batch.
pub(crate) const STREAM_BATCH_SIZE: usize = 10;

/// Whether this caller gets the bounded read defaults below (absent limits
/// default to a page, explicit limits clamp to a maximum). A placeholder for
/// the shared per-connection `boundedReads` flag: old apps cannot see a new
/// server capability, so the count defaults stay off until that flag says
/// the client opted in. The byte ceilings apply to every caller.
pub(crate) fn bounded_read_defaults() -> bool {
    false
}

/// `search.messages` / `search.events` match count when `limit` is absent
/// and bounded defaults apply (§5.15).
pub(crate) const MATCH_LIMIT_DEFAULT: usize = 50;

/// Largest `search.messages` / `search.events` `limit` under bounded
/// defaults; larger values clamp.
pub(crate) const MATCH_LIMIT_MAX: usize = 200;

/// Serialized-size ceiling for the matches of one inline `search.*` reply,
/// 480,000 bytes (the per-page budget shared by bounded reads), below the
/// 500 KiB read payload budget with room for the envelope. Matches
/// past it are dropped and the reply carries `truncated: true`.
pub(crate) const INLINE_MATCHES_BUDGET_BYTES: usize = 480_000;

/// Resolve a `search.messages` / `search.events` `limit`. Bounded: absent →
/// the default, negative → 0, over the maximum → the maximum. Otherwise the
/// caller's value passes through (absent or negative → no cap).
pub(crate) fn match_limit(limit: Option<i64>, bounded: bool) -> Option<usize> {
    if !bounded {
        return limit.and_then(|n| usize::try_from(n).ok());
    }
    Some(limit.map_or(MATCH_LIMIT_DEFAULT, |n| {
        usize::try_from(n.max(0))
            .unwrap_or(usize::MAX)
            .min(MATCH_LIMIT_MAX)
    }))
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
        if let Ok(session) = store.get_agent_session(agent_id).await {
            if let Some(sandbox_path) = session.sandbox_path {
                return Ok(Some(sandbox_path.into()));
            }
        }
    }
    let ws = store.get_workspace(workspace_id).await?;
    Ok(crate::git_ops::worktree_path(&ws))
}

/// `search.inFiles` `opts.maxResults` when absent under bounded defaults.
pub(crate) const IN_FILES_MAX_RESULTS_DEFAULT: usize = 500;

/// Largest `search.inFiles` `opts.maxResults` under bounded defaults.
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

/// Bound a `search.inFiles` request: the matches always stop at
/// [`INLINE_MATCHES_BUDGET_BYTES`]; under bounded defaults `maxResults` also
/// defaults to [`IN_FILES_MAX_RESULTS_DEFAULT`] and clamps to
/// [`IN_FILES_MAX_RESULTS_MAX`]. Either cut sets `truncated`.
pub(crate) fn bound_in_files_opts(opts: &mut intent_search::SearchOpts, bounded: bool) {
    if bounded {
        opts.max_results = Some(
            opts.max_results
                .unwrap_or(IN_FILES_MAX_RESULTS_DEFAULT)
                .min(IN_FILES_MAX_RESULTS_MAX),
        );
    }
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
pub(crate) fn event_matches(
    events: &[Event],
    query: &str,
    limit: Option<usize>,
) -> Vec<EventMatch> {
    let mut out = Vec::new();
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
        if limit.is_some_and(|n| out.len() >= n) {
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
    fn match_limit_bounded_defaults_and_clamps() {
        assert_eq!(match_limit(None, true), Some(MATCH_LIMIT_DEFAULT));
        assert_eq!(match_limit(Some(10), true), Some(10));
        assert_eq!(match_limit(Some(0), true), Some(0));
        assert_eq!(match_limit(Some(-1), true), Some(0));
        assert_eq!(match_limit(Some(100_000), true), Some(MATCH_LIMIT_MAX));
    }

    #[test]
    fn match_limit_unbounded_keeps_the_callers_value() {
        assert_eq!(match_limit(None, false), None);
        assert_eq!(match_limit(Some(100_000), false), Some(100_000));
        assert_eq!(match_limit(Some(-1), false), None);
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
    fn in_files_opts_bounded_default_and_clamp() {
        let mut opts = parse_opts(None).unwrap();
        bound_in_files_opts(&mut opts, true);
        assert_eq!(opts.max_results, Some(IN_FILES_MAX_RESULTS_DEFAULT));
        assert_eq!(opts.max_bytes, Some(INLINE_MATCHES_BUDGET_BYTES));

        let mut opts = parse_opts(Some(serde_json::json!({ "maxResults": 100_000 }))).unwrap();
        bound_in_files_opts(&mut opts, true);
        assert_eq!(opts.max_results, Some(IN_FILES_MAX_RESULTS_MAX));

        let mut opts = parse_opts(Some(serde_json::json!({ "maxResults": 7 }))).unwrap();
        bound_in_files_opts(&mut opts, true);
        assert_eq!(opts.max_results, Some(7));
    }

    #[test]
    fn in_files_opts_unbounded_keep_max_results_but_cap_bytes() {
        let mut opts = parse_opts(None).unwrap();
        bound_in_files_opts(&mut opts, false);
        assert_eq!(opts.max_results, None);
        assert_eq!(opts.max_bytes, Some(INLINE_MATCHES_BUDGET_BYTES));

        let mut opts = parse_opts(Some(serde_json::json!({ "maxResults": 100_000 }))).unwrap();
        bound_in_files_opts(&mut opts, false);
        assert_eq!(opts.max_results, Some(100_000));
    }

    #[test]
    fn bounded_read_defaults_stay_off_until_the_client_opts_in() {
        assert!(!bounded_read_defaults());
    }
}
