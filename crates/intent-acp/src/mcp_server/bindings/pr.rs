//! `ws.pr.*` bindings (WSAPI-6).
//!
//! The namespace exposes a single binding: `pr.snapshot`, the compact,
//! diff-friendly PR state used by background-hook monitoring. Every other PR
//! operation (create, view, comment, review threads, branch update, merge)
//! is intentionally unbound — agents use the `gh` CLI instead. The binding
//! only peels arguments and forwards the trait's `serde_json::Value` result
//! unchanged.

use std::sync::Arc;

use intent_core::{WorkspaceApi, WorkspaceId};
use serde_json::Value;

use super::{map_err, req_i64};

pub(crate) const PRELUDE: &str = r#"
    globalThis.ws = globalThis.ws || {};
    ws.pr = {
        snapshot: (prNumber, options) =>
            host({ method: 'pr.snapshot', args: { prNumber, ...(options || {}) } }),
    };
"#;

pub(crate) async fn dispatch(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    match method {
        "snapshot" => snapshot(api, ws, args).await,
        other => Err(format!("host: unknown method `pr.{other}`")),
    }
}

async fn snapshot(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let pr_number =
        req_i64(args, "prNumber").map_err(|_| "prNumber is required and must be a number")?;
    if pr_number <= 0 {
        return Err("prNumber is required and must be a number".to_string());
    }
    // Optional cross-repo override; slug validation lives in the engine, but
    // a present-yet-non-string value fails fast rather than silently falling
    // back to the workspace repo.
    let repo = match args.get("repo") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) => Some(s.clone()),
        Some(_) => return Err("repo must be an \"owner/name\" string".to_string()),
    };
    api.pr_state(ws.clone(), pr_number as u64, repo)
        .await
        .map_err(map_err)
}
