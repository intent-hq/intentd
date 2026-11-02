//! `ws.crossWorkspace.*` bindings (WSAPI-6).
//!
//! Thin wrappers over the [`WorkspaceApi`] cross-workspace surface: sibling
//! discovery, sibling search (`search`, binding-only — no wire method), and
//! cross-workspace note reads. The daemon already enforces the
//! "same repositoryPath" access rule and shapes the returned payload — the
//! binding peels arguments and echoes the trait's `serde_json::Value`
//! result, adding only agent-facing workspace links to sibling rows (reference parity with `buildCrossWorkspaceApi` in
//! `ws-misc-api.ts`).

use std::sync::Arc;

use intent_core::{CrossWorkspaceSearch, NoteId, PullRequestSelector, WorkspaceApi, WorkspaceId};
use serde_json::Value;

use super::{map_err, req_str};

pub(crate) const PRELUDE: &str = r"
    globalThis.ws = globalThis.ws || {};
    ws.crossWorkspace = {
        listSiblings: (options) => host({ method: 'crossWorkspace.listSiblings', args: options || {} }),
        readNote: (targetWorkspaceId, noteId) =>
            host({ method: 'crossWorkspace.readNote', args: { targetWorkspaceId, noteId } }),
        listNotes: (targetWorkspaceId) =>
            host({ method: 'crossWorkspace.listNotes', args: { targetWorkspaceId } }),
        search: (options) => host({
            method: 'crossWorkspace.search',
            args: typeof options === 'string' ? { query: options } : (options || {}),
        }),
    };
";

pub(crate) async fn dispatch(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    match method {
        "listSiblings" => list_siblings(api, ws, args).await,
        "readNote" => read_note(api, ws, args).await,
        "listNotes" => list_notes(api, ws, args).await,
        "search" => search(api, ws, args).await,
        other => Err(format!("host: unknown method `crossWorkspace.{other}`")),
    }
}

/// Retired siblings are opt-in: `ws.crossWorkspace.listSiblings({ includeRetired: true })`.
/// Each row gains `link` / `markdownLink` (agent-facing only; the daemon RPC
/// row is unchanged) so agents can name a sibling with a clickable link.
async fn list_siblings(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let include_retired = super::opt_strict_bool(args, "includeRetired")?.unwrap_or(false);
    let mut siblings = api
        .cross_workspace_list_siblings(ws.clone(), include_retired)
        .await
        .map_err(map_err)?;
    if let Some(rows) = siblings.as_array_mut() {
        rows.iter_mut().for_each(super::insert_workspace_links);
    }
    Ok(siblings)
}

async fn read_note(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let target = req_str(args, "targetWorkspaceId")
        .map_err(|_| "Both workspaceId and noteId are required".to_string())?;
    let note_id = req_str(args, "noteId")
        .map_err(|_| "Both workspaceId and noteId are required".to_string())?;
    api.cross_workspace_read_note(
        ws.clone(),
        WorkspaceId::from_string(target),
        NoteId::from_string(&note_id),
    )
    .await
    .map_err(map_err)
}

async fn list_notes(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let target =
        req_str(args, "targetWorkspaceId").map_err(|_| "workspaceId is required".to_string())?;
    api.cross_workspace_list_notes(ws.clone(), WorkspaceId::from_string(target))
        .await
        .map_err(map_err)
}

/// Default and maximum `limit` for `ws.crossWorkspace.search`.
const SEARCH_DEFAULT_LIMIT: usize = 20;
const SEARCH_MAX_LIMIT: usize = 100;

/// `ws.crossWorkspace.search({ query?, pr?, includeRetired?, limit? })`
/// (a bare string is shorthand for `{ query }`). `pr` accepts a number or a
/// `123` / `#123` / `owner/repo#123` / PR-URL string. Each match row gains
/// `link` / `markdownLink`, as `listSiblings` rows do.
async fn search(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let search = parse_search(args)?;
    let mut result = api
        .cross_workspace_search(ws.clone(), search)
        .await
        .map_err(map_err)?;
    if let Some(rows) = result.get_mut("matches").and_then(Value::as_array_mut) {
        rows.iter_mut().for_each(super::insert_workspace_links);
    }
    Ok(result)
}

pub(crate) fn parse_search(args: &Value) -> Result<CrossWorkspaceSearch, String> {
    let query = match args.get("query") {
        None | Some(Value::Null) => None,
        Some(Value::String(q)) => Some(q.trim().to_string()).filter(|q| !q.is_empty()),
        Some(_) => return Err("query must be a string".to_string()),
    };
    let pr = match args.get("pr") {
        None | Some(Value::Null) => None,
        Some(Value::Number(n)) => Some(n.to_string()),
        Some(Value::String(s)) => Some(s.clone()),
        Some(_) => return Err("pr must be a number or a string".to_string()),
    }
    .map(|raw| {
        PullRequestSelector::parse(&raw).ok_or_else(|| {
            format!(
                "pr `{raw}` is not a PR number, #number, owner/repo#number, or pull-request URL"
            )
        })
    })
    .transpose()?;
    if query.is_none() && pr.is_none() {
        return Err("Provide a non-empty query, a pr, or both".to_string());
    }
    let include_retired = super::opt_strict_bool(args, "includeRetired")?.unwrap_or(false);
    let limit = match args.get("limit") {
        None | Some(Value::Null) => SEARCH_DEFAULT_LIMIT,
        Some(v) => v
            .as_u64()
            .and_then(|n| usize::try_from(n).ok())
            .filter(|n| (1..=SEARCH_MAX_LIMIT).contains(n))
            .ok_or_else(|| format!("limit must be an integer from 1 to {SEARCH_MAX_LIMIT}"))?,
    };
    Ok(CrossWorkspaceSearch {
        query,
        pr,
        include_retired,
        limit,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_search_accepts_query_pr_and_both() {
        let s = parse_search(&json!({ "query": "  login bug " })).unwrap();
        assert_eq!(s.query.as_deref(), Some("login bug"));
        assert_eq!(s.pr, None);
        assert_eq!(s.limit, SEARCH_DEFAULT_LIMIT);
        assert!(!s.include_retired);

        let s = parse_search(&json!({ "pr": 2100 })).unwrap();
        assert_eq!(s.pr, PullRequestSelector::parse("2100"));
        let s = parse_search(&json!({ "pr": "intent-hq/intentd#7", "query": "x", "limit": 5, "includeRetired": true }))
            .unwrap();
        assert_eq!(s.pr, PullRequestSelector::parse("intent-hq/intentd#7"));
        assert_eq!(s.limit, 5);
        assert!(s.include_retired);
    }

    #[test]
    fn parse_search_rejects_bad_input() {
        for (args, want) in [
            (json!({}), "Provide a non-empty query"),
            (json!({ "query": "   " }), "Provide a non-empty query"),
            (json!({ "query": 3 }), "query must be a string"),
            (json!({ "pr": true }), "pr must be a number"),
            (json!({ "pr": "intentd#7" }), "is not a PR number"),
            (json!({ "pr": -1 }), "is not a PR number"),
            (json!({ "query": "x", "limit": 0 }), "limit must be"),
            (json!({ "query": "x", "limit": 101 }), "limit must be"),
            (json!({ "query": "x", "limit": "5" }), "limit must be"),
            (
                json!({ "query": "x", "includeRetired": "yes" }),
                "includeRetired",
            ),
        ] {
            let err = parse_search(&args).unwrap_err();
            assert!(err.contains(want), "{args}: {err}");
        }
    }
}
