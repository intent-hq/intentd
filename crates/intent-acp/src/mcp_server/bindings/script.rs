//! `ws.script.*` bindings (WSAPI-5).
//!
//! Thin wrappers over the `WorkspaceApi` script surface, mirroring the
//! reference `ws-script-api.ts` builder. `script.create` accepts the same
//! `(name, command, mode, options)` positional signature; `mode` must be
//! `"service"` or `"command"` and any other value is rejected with the
//! reference error text before the call reaches the daemon.

use std::collections::BTreeMap;
use std::sync::Arc;

use intent_core::{ScriptCreateParams, ScriptMode, WorkspaceApi, WorkspaceId};
use serde_json::Value;

use super::{map_err, opt_bool, opt_i64, opt_str, req_str};

pub(crate) const PRELUDE: &str = r"
    globalThis.ws = globalThis.ws || {};
    ws.script = {
        list: () => host({ method: 'script.list' }),
        create: (name, command, mode, options) =>
            host({ method: 'script.create', args: { name, command, mode, ...(options || {}) } }),
        remove: (scriptId) => host({ method: 'script.remove', args: { scriptId } }),
        start: (scriptId) => host({ method: 'script.start', args: { scriptId } }),
        stop: (scriptId) => host({ method: 'script.stop', args: { scriptId } }),
        restart: (scriptId) => host({ method: 'script.restart', args: { scriptId } }),
        output: (scriptId, maxLines) =>
            host({ method: 'script.output', args: { scriptId, maxLines } }),
        status: (scriptId) => host({ method: 'script.status', args: { scriptId } }),
        run: (scriptId, options) =>
            host({ method: 'script.run', args: { scriptId, ...(options || {}) } }),
    };
";

pub(crate) async fn dispatch(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    match method {
        "list" => list(api, ws).await,
        "create" => create(api, ws, args).await,
        "remove" => remove(api, ws, args).await,
        "start" => start(api, ws, args).await,
        "stop" => stop(api, ws, args).await,
        "restart" => restart(api, ws, args).await,
        "output" => output(api, ws, args).await,
        "status" => status(api, ws, args).await,
        "run" => run(api, ws, args).await,
        other => Err(format!("host: unknown method `script.{other}`")),
    }
}

async fn list(api: &Arc<dyn WorkspaceApi>, ws: &WorkspaceId) -> Result<Value, String> {
    // The reference `ws.script.list()` (see `ws-script-api.ts`) returns a
    // bare array of scripts, but the production `WorkspaceApi::script_list`
    // (via `intent-services::ScriptManager::list`) wraps them as
    // `{ "scripts": [...] }`. Unwrap the `scripts` field when present so
    // JS callers get the documented shape; fall back to the raw value for
    // forward-compatibility with any daemon that already returns a bare
    // array.
    let raw = api.script_list(ws.clone()).await.map_err(map_err)?;
    if let Some(inner) = raw.get("scripts") {
        return Ok(inner.clone());
    }
    Ok(raw)
}

async fn create(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let name =
        req_str(args, "name").map_err(|_| "name, command, and mode are required.".to_string())?;
    let command = req_str(args, "command")
        .map_err(|_| "name, command, and mode are required.".to_string())?;
    let mode_raw =
        req_str(args, "mode").map_err(|_| "name, command, and mode are required.".to_string())?;
    let mode = match mode_raw.as_str() {
        "service" => ScriptMode::Service,
        "command" => ScriptMode::Command,
        _ => return Err("mode must be \"service\" or \"command\".".to_string()),
    };
    // `ScriptCreateParams.env` is `BTreeMap<String, String>`, so surface an
    // input error for non-object `env` or any non-string value rather than
    // silently dropping entries (`{ PORT: 3000 } → {}`) or ignoring the
    // whole field.
    let env = match args.get("env") {
        None | Some(Value::Null) => None,
        Some(Value::Object(obj)) => {
            let mut map = BTreeMap::new();
            for (k, v) in obj {
                let s = v
                    .as_str()
                    .ok_or_else(|| format!("env.{k} must be a string (got {})", type_name(v)))?;
                map.insert(k.clone(), s.to_string());
            }
            Some(map)
        }
        Some(other) => {
            return Err(format!(
                "env must be an object of string values (got {})",
                type_name(other)
            ));
        }
    };
    let params = ScriptCreateParams {
        name,
        command,
        mode,
        cwd: opt_str(args, "cwd"),
        env,
        category: opt_str(args, "category"),
        auto_start: opt_bool(args, "autoStart"),
        script_id: opt_str(args, "scriptId"),
    };
    api.script_create(ws.clone(), params).await.map_err(map_err)
}

async fn remove(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let script_id = req_str(args, "scriptId").map_err(|_| "scriptId is required".to_string())?;
    api.script_remove(ws.clone(), script_id)
        .await
        .map_err(map_err)
}

async fn start(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let script_id = req_str(args, "scriptId").map_err(|_| "scriptId is required".to_string())?;
    api.script_start(ws.clone(), script_id)
        .await
        .map_err(map_err)
}

async fn stop(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let script_id = req_str(args, "scriptId").map_err(|_| "scriptId is required".to_string())?;
    api.script_stop(ws.clone(), script_id)
        .await
        .map_err(map_err)
}

async fn restart(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let script_id = req_str(args, "scriptId").map_err(|_| "scriptId is required".to_string())?;
    api.script_restart(ws.clone(), script_id)
        .await
        .map_err(map_err)
}

async fn output(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let script_id = req_str(args, "scriptId").map_err(|_| "scriptId is required".to_string())?;
    let max_lines = opt_i64(args, "maxLines");
    api.script_output(ws.clone(), script_id, max_lines, None, None)
        .await
        .map_err(map_err)
}

async fn status(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let script_id = req_str(args, "scriptId").map_err(|_| "scriptId is required".to_string())?;
    api.script_status(ws.clone(), script_id)
        .await
        .map_err(map_err)
}

async fn run(api: &Arc<dyn WorkspaceApi>, ws: &WorkspaceId, args: &Value) -> Result<Value, String> {
    let script_id = req_str(args, "scriptId").map_err(|_| "scriptId is required".to_string())?;
    let max_lines = opt_i64(args, "maxLines");
    // `timeoutSeconds` with the `timeout` alias (reference parity).
    let timeout_seconds = opt_i64(args, "timeoutSeconds").or_else(|| opt_i64(args, "timeout"));
    api.script_run(ws.clone(), script_id, max_lines, timeout_seconds)
        .await
        .map_err(map_err)
}

fn type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}
