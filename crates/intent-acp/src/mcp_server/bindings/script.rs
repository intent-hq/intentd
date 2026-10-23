//! `ws.script.*` bindings (WSAPI-5).
//!
//! Thin wrappers over the `WorkspaceApi` script surface, mirroring the
//! reference `ws-script-api.ts` builder. `script.create` accepts the same
//! `(name, command, mode, options)` positional signature; `mode` must be
//! `"service"` or `"command"` and any other value is rejected with the
//! reference error text before the call reaches the daemon.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use intent_core::{AgentId, ScriptCreateParams, ScriptMode, WorkspaceApi, WorkspaceId};
use serde_json::Value;

use super::{map_err, opt_bool, opt_i64, opt_str, req_str};

/// Seconds held back from the `workspace_api` eval budget so `script.run`
/// can return its `timedOut` envelope before the transport aborts the call.
/// Covers the PTY host's 2s TERM grace before SIGKILL on a TERM-trapping
/// child, the kill / straggler-reap / scrollback-read tail after the run
/// timeout, and the eval time already spent before the binding is invoked.
const RUN_TIMEOUT_MARGIN_SECS: u64 = 5;

/// The largest `timeoutSeconds` one `ws.script.run` call can honor under
/// `budget`: `floor(budget) - 5`, never below 1 (25s on the default 30s
/// budget). `script_run` kills the process when its timeout elapses, so a
/// wait longer than the eval budget would be aborted by the transport while
/// the process keeps running — the binding rejects such requests up front
/// instead of clamping them (monorepo#4703).
fn run_timeout_ceiling_secs(budget: Duration) -> i64 {
    let ceiling = budget
        .as_secs()
        .saturating_sub(RUN_TIMEOUT_MARGIN_SECS)
        .max(1);
    i64::try_from(ceiling).unwrap_or(i64::MAX)
}

pub(crate) const PRELUDE: &str = r"
    globalThis.ws = globalThis.ws || {};
    ws.script = {
        monitor: (scriptId, options) => host({ method: 'script.monitor', args: { scriptId, ...(options || {}) } }),
        listPage: (options) => host({ method: 'script.listPage', args: options || {} }),
        monitorsPage: (options) => host({ method: 'script.monitorsPage', args: options || {} }),
        get: (scriptId) => host({ method: 'script.get', args: { scriptId } }),
        monitorDetails: (monitorId) => host({ method: 'script.monitorDetails', args: { monitorId } }),
        monitors: () => host({ method: 'script.monitors', args: {} }),
        unmonitor: (monitorId) => host({ method: 'script.unmonitor', args: { monitorId } }),
        list: (options) => host({ method: 'script.list', args: options || {} }),
        archive: (scriptIds) => host({ method: 'script.archive', args: { scriptIds } }),
        restore: (scriptIds) => host({ method: 'script.restore', args: { scriptIds } }),
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

/// `budget` is the caller's effective `workspace_api` eval budget; `run`
/// derives its `timeoutSeconds` ceiling from it.
pub(crate) async fn dispatch(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    budget: Duration,
    caller: Option<&AgentId>,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    match method {
        "listPage" => api
            .script_list_view(ws.clone(), page_options(args)?)
            .await
            .map_err(map_err),
        "monitorsPage" => api
            .script_monitor_list_view(
                ws.clone(),
                Some(monitor_owner(caller)?),
                page_options(args)?,
            )
            .await
            .map_err(map_err),
        "get" => api
            .script_get(ws.clone(), req_str(args, "scriptId")?)
            .await
            .map_err(map_err),
        "monitorDetails" => api
            .script_monitor_get(
                ws.clone(),
                req_str(args, "monitorId")?,
                Some(monitor_owner(caller)?),
            )
            .await
            .map_err(map_err),
        "monitor" => api
            .script_monitor(
                ws.clone(),
                monitor_owner(caller)?,
                req_str(args, "scriptId")?,
                args.clone(),
            )
            .await
            .map_err(map_err),
        "monitors" => api
            .script_monitor_list(ws.clone(), Some(monitor_owner(caller)?))
            .await
            .map(|v| v["monitors"].clone())
            .map_err(map_err),
        "unmonitor" => api
            .script_monitor_cancel(
                ws.clone(),
                req_str(args, "monitorId")?,
                Some(monitor_owner(caller)?),
                false,
            )
            .await
            .map_err(map_err),
        "list" => list(api, ws, args).await,
        "archive" => archive(api, ws, args, true).await,
        "restore" => archive(api, ws, args, false).await,
        "create" => create(api, ws, args).await,
        "remove" => remove(api, ws, args).await,
        "start" => {
            let mut reply = start(api, ws, args).await?;
            arm_start_monitor(api, ws, caller, args, &mut reply).await;
            Ok(reply)
        }
        "stop" => stop(api, ws, args).await,
        "restart" => {
            let mut reply = restart(api, ws, args).await?;
            arm_start_monitor(api, ws, caller, args, &mut reply).await;
            Ok(reply)
        }
        "output" => output(api, ws, args).await,
        "status" => status(api, ws, args).await,
        "run" => run(api, ws, budget, args).await,
        other => Err(format!("host: unknown method `script.{other}`")),
    }
}

fn page_options(args: &Value) -> Result<Value, String> {
    if !args.is_object() {
        return Err("script page options must be an object".into());
    }
    let mut options = args.clone();
    options["view"] = Value::String("summary".into());
    Ok(options)
}

/// Default-on completion wake: an agent that starts or restarts a
/// command-mode script gets the same monitor `ws.script.monitor` would arm,
/// reported under `autoMonitor` (the `script.monitor` reply: `{ ok, monitor }`
/// or a refusal). Agents ended turns "waiting for the build" with nothing
/// armed and stalled until someone noticed. Best-effort: a failed arm never
/// fails the start the agent asked for; it is reported as `autoMonitorError`.
async fn arm_start_monitor(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller: Option<&AgentId>,
    args: &Value,
    reply: &mut Value,
) {
    let (Some(agent), Ok(script_id), Some(run_id)) = (
        caller,
        req_str(args, "scriptId"),
        reply
            .get("runId")
            .and_then(Value::as_str)
            .filter(|r| !r.is_empty())
            .map(str::to_owned),
    ) else {
        return;
    };
    match api
        .script_start_monitor(ws.clone(), agent.clone(), script_id, run_id)
        .await
    {
        Ok(Value::Null) => {}
        Ok(monitor) => reply["autoMonitor"] = monitor,
        Err(error) => reply["autoMonitorError"] = Value::String(error.to_string()),
    }
}

fn monitor_owner(caller: Option<&AgentId>) -> Result<AgentId, String> {
    caller
        .cloned()
        .ok_or_else(|| "script monitoring requires an authenticated agent caller".into())
}

async fn archive(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
    archive: bool,
) -> Result<Value, String> {
    let ids: Vec<String> =
        serde_json::from_value(args.get("scriptIds").cloned().unwrap_or(Value::Null))
            .map_err(|e| format!("Invalid scriptIds: {e}"))?;
    if archive {
        api.script_archive(ws.clone(), ids).await.map_err(map_err)
    } else {
        api.script_restore(ws.clone(), ids).await.map_err(map_err)
    }
}

async fn list(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    // The reference `ws.script.list()` (see `ws-script-api.ts`) returns a
    // bare array of scripts, but the production `WorkspaceApi::script_list`
    // (via `intent-services::ScriptManager::list`) wraps them as
    // `{ "scripts": [...] }`. Unwrap the `scripts` field when present so
    // JS callers get the documented shape; fall back to the raw value for
    // forward-compatibility with any daemon that already returns a bare
    // array.
    let options =
        intent_core::script_lists::ScriptListOptions::parse(args, false).map_err(map_err)?;
    if options.summary {
        return Err("use script.listPage for summary pages".into());
    }
    let archive = args
        .get("archive")
        .map(|value| serde_json::from_value(value.clone()))
        .transpose()
        .map_err(|e| format!("Invalid archive: {e}"))?
        .unwrap_or(intent_core::ScriptArchiveFilter::Active);
    let raw = api
        .script_list_filtered(ws.clone(), archive)
        .await
        .map_err(map_err)?;
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
        purpose: args
            .get("purpose")
            .map(|value| serde_json::from_value(value.clone()))
            .transpose()
            .map_err(|e| format!("Invalid purpose: {e}"))?,
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

async fn run(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    budget: Duration,
    args: &Value,
) -> Result<Value, String> {
    let script_id = req_str(args, "scriptId").map_err(|_| "scriptId is required".to_string())?;
    let max_lines = opt_i64(args, "maxLines");
    let ceiling = run_timeout_ceiling_secs(budget);
    // `timeoutSeconds` with the `timeout` alias (reference parity). An
    // omitted or non-positive timeout defaults to the ceiling: `None` and
    // non-positive values both make the service layer wait unbounded, so the
    // `timedOut` envelope would never be reachable within the budget.
    let timeout_seconds = match opt_i64(args, "timeoutSeconds").or_else(|| opt_i64(args, "timeout"))
    {
        Some(requested) if requested > ceiling => {
            return Err(format!(
                "ws.script.run: timeoutSeconds {requested} exceeds what one workspace_api call \
                 can wait for (ceiling {ceiling}s, budget {}s). Start the script with \
                 ws.script.start(scriptId), carry its nonempty runId into ws.script.monitor with required ttlMs, \
                 and inspect the result. Wait only on success. Handle run-finished immediately; attribute ws.script.output \
                 only when status-before/output/status-after preserve that runId, otherwise unavailable. \
                 Follow already-monitored owner instructions; never silently rearm.",
                budget.as_secs()
            ));
        }
        Some(requested) if requested > 0 => Some(requested),
        _ => Some(ceiling),
    };
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_timeout_ceiling_is_budget_minus_margin_floored_at_one() {
        assert_eq!(run_timeout_ceiling_secs(Duration::from_secs(30)), 25);
        assert_eq!(run_timeout_ceiling_secs(Duration::from_millis(30_900)), 25);
        assert_eq!(run_timeout_ceiling_secs(Duration::from_secs(120)), 115);
        assert_eq!(run_timeout_ceiling_secs(Duration::from_secs(6)), 1);
        assert_eq!(run_timeout_ceiling_secs(Duration::from_secs(3)), 1);
        assert_eq!(run_timeout_ceiling_secs(Duration::from_millis(250)), 1);
    }
}
