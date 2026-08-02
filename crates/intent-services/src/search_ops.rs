//! Wire glue for the `search.*` methods (§5.15).
//!
//! Resolves a workspace's search root (its worktree) and parses the
//! `search.inFiles` `opts` object. The ripgrep-equivalent walk/search itself
//! lives in `intent-search`; this module owns only the services-layer glue.

use std::path::PathBuf;

use intent_core::{Error, Event, Note, Result, RetrieveResult, WorkspaceId};
use intent_search::{
    contains_ci, extract_symbol, fts_preview, make_preview, CodebaseMatch, ContentSearchResult,
    EventMatch, MessageMatch, NoteMatch,
};
use intent_store::{MessageFtsMatch, Store};
use serde_json::Value;

/// Result sets at or below this many matches are returned inline in the method
/// result; larger sets are streamed via `search:result`/`search:done` (§6.5).
pub(crate) const INLINE_THRESHOLD: usize = 25;

/// Number of matches per streamed `search:result` batch.
pub(crate) const STREAM_BATCH_SIZE: usize = 10;

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

/// Parse the raw `opts` object into [`SearchOpts`]; an unusable shape surfaces
/// as `InvalidParams` (→ `-32602`). Absent/null yields the defaults.
pub(crate) fn parse_opts(opts: Option<Value>) -> Result<intent_search::SearchOpts> {
    match opts {
        None | Some(Value::Null) => Ok(intent_search::SearchOpts::default()),
        Some(value) => serde_json::from_value(value)
            .map_err(|_| Error::InvalidParams("Invalid search opts".to_string())),
    }
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

/// Build `search.notes` matches over the notes store. The searchable text is the
/// note title plus its content.
pub(crate) fn note_matches(notes: &[Note], query: &str) -> Vec<NoteMatch> {
    let mut out = Vec::new();
    for note in notes {
        let text = format!("{} {}", note.title, note.content);
        if !contains_ci(&text, query) {
            continue;
        }
        out.push(NoteMatch {
            note_id: note.id.as_str().to_string(),
            preview: make_preview(&text, query),
            score: None,
        });
    }
    out
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
