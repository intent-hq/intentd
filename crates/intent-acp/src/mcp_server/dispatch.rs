//! Tool-call dispatch: after the WSAPI-8 cutover the daemon exposes exactly
//! one MCP tool — `workspace_api` — whose arguments carry agent-supplied
//! JavaScript that is evaluated against the shared `WorkspaceApi` via the
//! `ws.*` bindings in [`super::bindings`] (the "two front doors" rule; the
//! FE's JSON-RPC router uses the same trait, §6.8).

use std::sync::Arc;
use std::time::Duration;

use intent_core::{AgentId, WorkspaceApi, WorkspaceId};
use intent_js::{eval as js_eval, BoxFuture, EvalOptions, HostFn, JsError};
use serde_json::{json, Value};

use super::WorkspaceMcpServer;

/// Wall-clock budget for one `workspace_api` invocation — matches the 30s
/// timeout in the reference `workspace-js-api-tool.ts`.
const WORKSPACE_API_TIMEOUT: Duration = Duration::from_secs(30);

impl WorkspaceMcpServer {
    /// WSAPI-2 dispatch: evaluate agent-supplied JavaScript against the
    /// workspace API and shape the MCP tool result in-line (reference parity
    /// with `workspace-js-api-tool.ts` — a pretty-printed JSON body on
    /// success, `(no return value)` for `undefined`, and a readable text
    /// body with `isError: true` on any JS-side failure).
    pub(super) async fn dispatch_workspace_api(&self, args: &Value) -> Value {
        let Some(code) = args.get("code").and_then(Value::as_str) else {
            return workspace_api_error("`code` is required and must be a string");
        };
        // `summary` is required by the input schema but is not fed into the
        // engine — it is a UI hint for the caller, not part of the eval
        // environment. Accept and ignore for now.
        let host = make_workspace_host(
            self.api.clone(),
            self.workspace_id.clone(),
            self.caller_agent_id.clone(),
        );
        // Wrap user code so the engine sees a small `{__k, __v}` envelope,
        // preserving the `undefined` vs `null` distinction that
        // `serde_json::Value` cannot represent on its own. `__k` is `"u"` for
        // an undefined return (prints "(no return value)") and `"v"` for a
        // JSON-serializable value (prints as pretty JSON, including `null`).
        let bindings_prelude = super::bindings::prelude();
        let full_code = format!(
            "{bindings_prelude}\n\
             const __wsapi_user = await (async () => {{ {code}\n}})();\n\
             return {{ __k: __wsapi_user === undefined ? 'u' : 'v', __v: __wsapi_user }};"
        );
        let opts = EvalOptions {
            timeout: WORKSPACE_API_TIMEOUT,
            ..EvalOptions::default()
        };
        match js_eval(&full_code, &opts, Some(host)).await {
            Ok(v) => match v.get("__k").and_then(Value::as_str) {
                Some("u") => workspace_api_success("(no return value)"),
                Some("v") => {
                    let value = v.get("__v").cloned().unwrap_or(Value::Null);
                    let pretty = serde_json::to_string_pretty(&value)
                        .unwrap_or_else(|_| "(unserializable)".into());
                    workspace_api_success(&pretty)
                }
                _ => workspace_api_error("Error: engine: unexpected workspace_api envelope"),
            },
            Err(e) => workspace_api_error(&format_js_error(&e)),
        }
    }
}

/// Build the `HostFn` bridging JS `host({ method, args })` calls back into
/// the shared `WorkspaceApi`. Every namespace lives in `super::bindings`;
/// unknown methods surface as a JS-visible error frame. `caller_agent_id`
/// is forwarded to bindings that attribute their calls back to the spawning
/// agent (e.g. `workspace.setAgentName`, `git.commit`, `git.agentCommit`,
/// `ws.browser.exec`, and the caller-aware `ws.agent.*` methods).
fn make_workspace_host(
    api: Arc<dyn WorkspaceApi>,
    workspace_id: WorkspaceId,
    caller_agent_id: Option<AgentId>,
) -> HostFn {
    Arc::new(move |arg| {
        let api = api.clone();
        let workspace_id = workspace_id.clone();
        let caller = caller_agent_id.clone();
        Box::pin(async move { workspace_host_dispatch(api, workspace_id, caller, arg).await })
            as BoxFuture<'static, std::result::Result<Value, String>>
    })
}

/// Route one `host({method, args})` frame to a `WorkspaceApi` method via
/// [`super::bindings::try_dispatch`], which owns the per-namespace method →
/// trait mapping.
async fn workspace_host_dispatch(
    api: Arc<dyn WorkspaceApi>,
    workspace_id: WorkspaceId,
    caller_agent_id: Option<AgentId>,
    arg: Value,
) -> std::result::Result<Value, String> {
    let method = arg
        .get("method")
        .and_then(Value::as_str)
        .ok_or_else(|| "host: `method` is required".to_string())?;
    let args = arg.get("args").cloned().unwrap_or(Value::Null);
    if let Some(v) =
        super::bindings::try_dispatch(&api, &workspace_id, &caller_agent_id, method, &args).await?
    {
        return Ok(v);
    }
    Err(format!("host: unknown method `{method}`"))
}

/// Success MCP tool result for `workspace_api`: a single text content block
/// with `isError: false`.
fn workspace_api_success(text: &str) -> Value {
    json!({
        "content": [{ "type": "text", "text": text }],
        "isError": false,
    })
}

/// Error MCP tool result for `workspace_api`: a single text content block
/// with `isError: true`. JS-side failures are surfaced as tool results
/// (not JSON-RPC protocol errors) to mirror the reference TS tool.
fn workspace_api_error(text: &str) -> Value {
    json!({
        "content": [{ "type": "text", "text": text }],
        "isError": true,
    })
}

/// Render a [`JsError`] into the reference tool's error-text style. Syntax
/// errors and `Cannot read properties of undefined` TypeErrors get a
/// clearer human-facing rewrite; everything else falls through as `Error: …`.
fn format_js_error(err: &JsError) -> String {
    match err {
        JsError::Timeout { ms } => format!("Error: javascript execution timed out after {ms}ms"),
        JsError::Engine(msg) => format!("Error: {msg}"),
        JsError::Runtime(msg) => {
            if looks_like_syntax_error(msg) {
                format!(
                    "SyntaxError in your code: {msg}. Check for unclosed brackets, braces, quotes, or template literals."
                )
            } else if msg.contains("TypeError")
                && msg.contains("Cannot read properties of undefined")
            {
                // Reference message: name the missing property to help the
                // agent notice the wrong namespace on `ws.*`.
                let prop = extract_missing_prop(msg);
                match prop {
                    Some(p) => format!(
                        "TypeError: Attempted to call '{p}' on an undefined object. Check that the namespace exists on the `ws` object (e.g. ws.workspace)."
                    ),
                    None => format!("TypeError: {msg}"),
                }
            } else {
                format!("Error: {msg}")
            }
        }
    }
}

/// QuickJS reports syntax errors as bare `Error: ...` with an indicative
/// phrase in the body (e.g. `unexpected token`, `expected identifier`),
/// unlike V8 which stamps `SyntaxError:` on the message. Match both so the
/// friendlier prefix still triggers on either engine.
fn looks_like_syntax_error(msg: &str) -> bool {
    msg.contains("SyntaxError")
        || msg.contains("unexpected token")
        || msg.contains("expected identifier")
        || msg.contains("unexpected end of input")
        || msg.contains("Unexpected end of input")
        || msg.contains("Invalid or unexpected token")
}

/// Pull the property name out of a `Cannot read properties of undefined
/// (reading 'foo')` TypeError message, matching the reference regex.
fn extract_missing_prop(msg: &str) -> Option<String> {
    let key = "(reading '";
    let start = msg.find(key)? + key.len();
    let rest = &msg[start..];
    let end = rest.find('\'')?;
    Some(rest[..end].to_string())
}
