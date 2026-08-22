//! `ws.browser.*` bindings (WSAPI-6).
//!
//! `browser.exec` is a **client-callable trigger** — the actual CDP work happens
//! on the connected frontend. The binding validates the actions envelope,
//! forwards it via the [`WorkspaceApi::browser_exec`] seam (the concrete impl
//! wraps a per-connection reverse channel — see `intent-transport::browser`),
//! and echoes the FE's reshaped result. `browser.docs` is offline: it returns
//! the ported `BROWSER_DOCS` topic text verbatim, matching the reference
//! `BrowserDocsTool` in `browser-tools.ts`.

use std::sync::Arc;

use intent_core::{AgentId, WorkspaceApi, WorkspaceId};
use serde_json::Value;

use super::{map_err, req_str};

/// The three topics `ws.browser.docs` accepts, ported verbatim from
/// `browser-tools.ts` `BROWSER_DOCS`. Kept as an `include_str!` triple so the
/// reference markdown stays byte-for-byte reviewable.
const OVERVIEW_DOC: &str = include_str!("browser_docs/overview.md");
const CAPTURE_DOC: &str = include_str!("browser_docs/capture.md");
const EXAMPLES_DOC: &str = include_str!("browser_docs/examples.md");

pub(crate) const PRELUDE: &str = r"
    globalThis.ws = globalThis.ws || {};
    ws.browser = {
        exec: (actions, tabId) => host({ method: 'browser.exec', args: { actions, tabId } }),
        docs: (topic) => host({ method: 'browser.docs', args: { topic } }),
    };
";

pub(crate) async fn dispatch(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller_agent_id: Option<&AgentId>,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    match method {
        "exec" => exec(api, ws, caller_agent_id, args).await,
        "docs" => docs(args),
        other => Err(format!("host: unknown method `browser.{other}`")),
    }
}

async fn exec(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller_agent_id: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    // Reference parity (`buildBrowserApi.exec`): `actions` must be present and
    // a non-empty array; the FE surfaces a friendlier error than the daemon's
    // envelope check would, so we mirror those messages here.
    let actions = match args.get("actions") {
        Some(Value::Array(a)) => a.clone(),
        _ => return Err("actions parameter is required and must be an array".to_string()),
    };
    if actions.is_empty() {
        return Err("actions array cannot be empty".to_string());
    }
    let tab_id = args
        .get("tabId")
        .and_then(Value::as_str)
        .map(str::to_string);
    api.browser_exec(ws.clone(), actions, tab_id, caller_agent_id.cloned())
        .await
        .map_err(map_err)
}

fn docs(args: &Value) -> Result<Value, String> {
    // Reference parity (`BrowserDocsTool.execute`): the tool exposes the topic
    // list on the schema; if a caller supplies an unknown topic, echo the
    // available topic list back so they can retry without a round-trip.
    let topic = req_str(args, "topic").map_err(|_| topic_error(""))?;
    match topic.as_str() {
        "overview" => Ok(Value::String(OVERVIEW_DOC.to_string())),
        "capture" => Ok(Value::String(CAPTURE_DOC.to_string())),
        "examples" => Ok(Value::String(EXAMPLES_DOC.to_string())),
        other => Err(topic_error(other)),
    }
}

fn topic_error(topic: &str) -> String {
    format!("Unknown topic: {topic}. Available topics: overview, capture, examples")
}
