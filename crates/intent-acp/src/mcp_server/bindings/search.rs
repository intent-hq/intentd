//! `ws.search` binding — full-text search over the caller's own workspace.
//!
//! One call runs the ranked `search.notes` and `search.messages` daemon
//! searches hard-scoped to the caller's workspace and returns both hit lists.
//! `ws.crossWorkspace.search` covers the siblings (it never returns the
//! caller); this is its same-workspace counterpart. Binding-only: there is no
//! new wire method.

use std::sync::Arc;

use intent_core::{WorkspaceApi, WorkspaceId};
use serde_json::{json, Map, Value};

use super::map_err;

pub(crate) const PRELUDE: &str = r"
    globalThis.ws = globalThis.ws || {};
    ws.search = (query, options) => host({
        method: 'search.workspace',
        args: query !== null && typeof query === 'object' ? query : { ...(options || {}), query },
    });
";

/// Default and maximum hits per kind. The maximum stays under the daemon's
/// inline-reply threshold (25) with the one extra probe hit, so a scoped
/// message search always answers inline instead of streaming events.
const DEFAULT_LIMIT: usize = 10;
const MAX_LIMIT: usize = 20;

#[derive(Debug, PartialEq, Eq)]
struct SearchArgs {
    query: String,
    notes: bool,
    messages: bool,
    agent_id: Option<String>,
    role: Option<String>,
    limit: usize,
}

pub(crate) async fn dispatch(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    match method {
        "workspace" => search(api, ws, args).await,
        other => Err(format!("host: unknown method `search.{other}`")),
    }
}

async fn search(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let args = parse_args(args)?;
    let probe = i64::try_from(args.limit + 1).expect("limit fits in i64");
    let mut out = Map::new();
    let mut truncated = false;
    if args.notes {
        let reply = api
            .search_notes(
                args.query.clone(),
                Some(ws.clone()),
                None,
                Some(probe),
                true,
                None,
                None,
            )
            .await
            .map_err(map_err)?;
        let (rows, more) = shape(&reply, args.limit, |m| note_row(ws, m));
        truncated |= more;
        out.insert("notes".into(), Value::Array(rows));
    }
    if args.messages {
        let reply = api
            .search_messages(
                Some(ws.clone()),
                args.query.clone(),
                args.agent_id.clone(),
                args.role.clone(),
                Some(probe),
                None,
                None,
            )
            .await
            .map_err(map_err)?;
        let (rows, more) = shape(&reply, args.limit, message_row);
        truncated |= more;
        out.insert("messages".into(), Value::Array(rows));
    }
    if truncated {
        out.insert("truncated".into(), Value::Bool(true));
    }
    Ok(Value::Object(out))
}

/// Cut a daemon reply's `matches` to `limit` rows, reporting whether more
/// hits exist: the probe hit, a message `truncated` budget cut, or a note
/// `nextToken` (how `search.notes` reports its budget cut).
fn shape(reply: &Value, limit: usize, row: impl Fn(&Value) -> Value) -> (Vec<Value>, bool) {
    let matches = reply
        .get("matches")
        .and_then(Value::as_array)
        .map_or(&[][..], Vec::as_slice);
    let cut = reply.get("truncated").and_then(Value::as_bool) == Some(true)
        || reply.get("nextToken").is_some_and(Value::is_string);
    let rows = matches.iter().take(limit).map(row).collect();
    (rows, cut || matches.len() > limit)
}

fn note_row(ws: &WorkspaceId, m: &Value) -> Value {
    let note_id = m.get("noteId").and_then(Value::as_str).unwrap_or_default();
    let title = m.get("title").and_then(Value::as_str).unwrap_or_default();
    let link = format!("intent://local/{}/note/{note_id}", ws.as_str());
    let label = if title.trim().is_empty() {
        "Untitled"
    } else {
        title
    };
    json!({
        "noteId": note_id,
        "title": title,
        "preview": m.get("preview"),
        "updatedAt": m.get("updatedAt"),
        "isArchived": m.get("isArchived"),
        "link": link,
        "markdownLink": super::markdown_link(label, &link),
    })
}

fn message_row(m: &Value) -> Value {
    json!({
        "agentId": m.get("agentId"),
        "agentName": m.get("agentName"),
        "messageId": m.get("messageId"),
        "role": m.get("role"),
        "timestamp": m.get("timestamp"),
        "preview": m.get("preview"),
    })
}

fn parse_args(args: &Value) -> Result<SearchArgs, String> {
    let query = match args.get("query") {
        Some(Value::String(q)) if !q.trim().is_empty() => q.trim().to_string(),
        None | Some(Value::Null | Value::String(_)) => {
            return Err("query must be a non-empty string".to_string())
        }
        Some(_) => return Err("query must be a string".to_string()),
    };
    let (notes, messages) = match args.get("in") {
        None | Some(Value::Null) => (true, true),
        Some(Value::String(s)) if s == "notes" => (true, false),
        Some(Value::String(s)) if s == "messages" => (false, true),
        Some(_) => return Err("in must be \"notes\" or \"messages\"".to_string()),
    };
    let agent_id = opt_filter(args, "agentId")?;
    let role = opt_filter(args, "role")?;
    if let Some(r) = role.as_deref() {
        if r != "user" && r != "assistant" {
            return Err("role must be \"user\" or \"assistant\"".to_string());
        }
    }
    if !messages && (agent_id.is_some() || role.is_some()) {
        return Err(
            "agentId and role filter messages; they cannot be used with in: \"notes\"".to_string(),
        );
    }
    let limit = match args.get("limit") {
        None | Some(Value::Null) => DEFAULT_LIMIT,
        Some(v) => v
            .as_u64()
            .and_then(|n| usize::try_from(n).ok())
            .filter(|n| (1..=MAX_LIMIT).contains(n))
            .ok_or_else(|| format!("limit must be an integer from 1 to {MAX_LIMIT}"))?,
    };
    Ok(SearchArgs {
        query,
        notes,
        messages,
        agent_id,
        role,
        limit,
    })
}

fn opt_filter(args: &Value, key: &str) -> Result<Option<String>, String> {
    match args.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(s)) if !s.trim().is_empty() => Ok(Some(s.trim().to_string())),
        Some(_) => Err(format!("{key} must be a non-empty string")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_args_defaults_and_kind_selection() {
        let a = parse_args(&json!({ "query": "  login bug " })).unwrap();
        assert_eq!(a.query, "login bug");
        assert!(a.notes && a.messages);
        assert_eq!(a.limit, DEFAULT_LIMIT);

        let a = parse_args(&json!({ "query": "x", "in": "notes", "limit": 20 })).unwrap();
        assert!(a.notes && !a.messages);
        assert_eq!(a.limit, MAX_LIMIT);

        let a = parse_args(
            &json!({ "query": "x", "in": "messages", "agentId": "a-1", "role": "user" }),
        )
        .unwrap();
        assert!(!a.notes && a.messages);
        assert_eq!(a.agent_id.as_deref(), Some("a-1"));
        assert_eq!(a.role.as_deref(), Some("user"));
    }

    #[test]
    fn parse_args_rejects_bad_input() {
        for (args, want) in [
            (json!({}), "non-empty string"),
            (json!({ "query": "  " }), "non-empty string"),
            (json!({ "query": 3 }), "query must be a string"),
            (json!({ "query": "x", "in": "files" }), "in must be"),
            (json!({ "query": "x", "role": "system" }), "role must be"),
            (json!({ "query": "x", "agentId": 7 }), "agentId must be"),
            (
                json!({ "query": "x", "in": "notes", "role": "user" }),
                "filter messages",
            ),
            (json!({ "query": "x", "limit": 0 }), "limit must be"),
            (json!({ "query": "x", "limit": 21 }), "limit must be"),
            (json!({ "query": "x", "limit": "5" }), "limit must be"),
        ] {
            let err = parse_args(&args).unwrap_err();
            assert!(err.contains(want), "{args}: {err}");
        }
    }

    #[test]
    fn shape_cuts_to_limit_and_flags_more() {
        let reply = json!({ "matches": [{ "messageId": "1" }, { "messageId": "2" }, { "messageId": "3" }] });
        let (rows, more) = shape(&reply, 2, message_row);
        assert_eq!(rows.len(), 2);
        assert!(more);
        let (rows, more) = shape(&reply, 3, message_row);
        assert_eq!(rows.len(), 3);
        assert!(!more);
        let (_, more) = shape(&json!({ "matches": [], "truncated": true }), 3, message_row);
        assert!(more);
        let (_, more) = shape(&json!({ "matches": [], "nextToken": "t" }), 3, message_row);
        assert!(more);
        let (_, more) = shape(&json!({ "matches": [], "nextToken": null }), 3, message_row);
        assert!(!more);
    }
}
