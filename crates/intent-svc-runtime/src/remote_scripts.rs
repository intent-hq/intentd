//! Saved scripts for remote agents (`remoteScripts: 1`): a remote agent's
//! forwarded `ws.script.*` calls, and the app's calls addressed to a machine
//! run with `remoteAgentId`. Definitions, reads and monitors stay on the
//! backend; a remote agent's runs are its machine runs
//! ([`crate::script_ops::machine_runs`]), never the home run.

use std::sync::Arc;
use std::time::Duration;

use intent_core::{
    AgentId, BoxFuture, Error, Result, Script, ScriptCreateParams, ScriptRemoteAuthor,
    WorkspaceApi, WorkspaceId,
};
use intent_services::feature_api::feature_ports::RemoteScriptCaller;
use intent_services::feature_api::Services;
use serde_json::{json, Value};

use crate::runtime::RuntimeStateExt as _;
use crate::script_ops::machine_runs::{MachineRuns, RunPlace};
use crate::script_ops::ScriptManager;

const NOT_OWNED: &str = "remote-script-not-owned";
const AUTOSTART: &str = "remote-script-autostart-unsupported";
const AUTHOR_READONLY: &str = "remote-script-author-readonly";
const START_REMOTE_ONLY: &str = "remote-script-start-remote-only";
const DEFAULT_OUTPUT_LINES: usize = 200;

fn invalid(code: &str, detail: impl std::fmt::Display) -> Error {
    Error::InvalidParams(format!("{code}: {detail}"))
}

fn runs(mgr: &ScriptManager) -> Result<&MachineRuns> {
    mgr.machine_runs()
        .ok_or_else(|| Error::Unsupported("machine runs are not available".into()))
}

async fn definition(svc: &Services, ws: &WorkspaceId, script_id: &str) -> Result<Script> {
    svc.store()
        .get_script_in_workspace(ws, script_id)
        .await?
        .ok_or_else(|| Error::NotFound(format!("script {script_id}")))
}

fn str_arg(args: &Value, key: &str) -> Result<String> {
    args.get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| Error::InvalidParams(format!("{key} is required")))
}

fn max_lines(args: &Value) -> usize {
    args.get("maxLines")
        .and_then(Value::as_u64)
        .and_then(|n| usize::try_from(n).ok())
        .filter(|n| (1..=10_000).contains(n))
        .unwrap_or(DEFAULT_OUTPUT_LINES)
}

/// A remote agent may change only definitions it authored.
async fn require_authored(
    svc: &Services,
    caller: &RemoteScriptCaller,
    script_id: &str,
) -> Result<Script> {
    let def = definition(svc, &caller.workspace_id, script_id).await?;
    if def.remote_author.as_ref().map(|a| &a.agent_id) != Some(&caller.agent_id) {
        return Err(invalid(
            NOT_OWNED,
            format!("script {script_id} was not created by this remote agent"),
        ));
    }
    Ok(def)
}

/// A remote agent's forwarded `ws.script.<method>(args)`.
pub(crate) async fn remote_call(
    svc: &Services,
    caller: RemoteScriptCaller,
    method: &str,
    args: Value,
    budget: Duration,
) -> std::result::Result<Value, String> {
    remote_call_inner(svc, &caller, method, args, budget)
        .await
        .map_err(|error| error.to_string())
}

async fn remote_call_inner(
    svc: &Services,
    caller: &RemoteScriptCaller,
    method: &str,
    args: Value,
    budget: Duration,
) -> Result<Value> {
    if let Some(other) = args.get("remoteAgentId").and_then(Value::as_str) {
        if other != caller.agent_id.as_str() {
            return Err(invalid(
                NOT_OWNED,
                "a remote agent drives only its own machine runs",
            ));
        }
    }
    let mgr = svc.script_manager();
    let place = |args: &Value| -> Result<RunPlace> {
        Ok(RunPlace {
            workspace_id: caller.workspace_id.clone(),
            script_id: str_arg(args, "scriptId")?,
            agent_id: caller.agent_id.clone(),
        })
    };
    match method {
        "script.start" | "script.restart" | "script.run" => {
            let place = place(&args)?;
            let def = definition(svc, &caller.workspace_id, &place.script_id).await?;
            let runs = runs(&mgr)?;
            match method {
                "script.start" => Ok(json!(runs.start(&place, &def).await?)),
                "script.restart" => {
                    if runs.state(&place).is_none() {
                        Ok(json!(runs.start(&place, &def).await?))
                    } else {
                        Ok(json!(runs.restart(&place, &def).await?))
                    }
                }
                _ => {
                    let ceiling = budget
                        .saturating_sub(Duration::from_secs(5))
                        .max(Duration::from_secs(1));
                    let timeout = args
                        .get("timeoutSeconds")
                        .and_then(Value::as_u64)
                        .map_or(ceiling, Duration::from_secs)
                        .min(ceiling);
                    runs.run(&place, &def, timeout, max_lines(&args)).await
                }
            }
        }
        "script.stop" => Ok(json!(runs(&mgr)?.stop(&place(&args)?).await?)),
        "script.status" => Ok(json!(runs(&mgr)?.state(&place(&args)?).unwrap_or_default())),
        "script.output" => {
            let place = place(&args)?;
            Ok(runs(&mgr)?
                .output_reply(&place, max_lines(&args))
                .unwrap_or_else(|| Value::String("No output yet.".to_owned())))
        }
        "script.monitor" => runs(&mgr)?.monitor(&place(&args)?, &args).await,
        "script.monitors" => Ok(json!(runs(&mgr)?.monitors_of(&caller.agent_id))),
        "script.monitorsPage" => {
            Ok(json!({ "view": "summary", "monitors": runs(&mgr)?.monitors_of(&caller.agent_id) }))
        }
        "script.monitorDetails" => {
            let id = str_arg(&args, "monitorId")?;
            runs(&mgr)?
                .monitors_of(&caller.agent_id)
                .into_iter()
                .find(|m| m["monitorId"] == id)
                .ok_or_else(|| Error::NotFound(format!("monitor {id}")))
        }
        "script.unmonitor" => {
            let id = str_arg(&args, "monitorId")?;
            Ok(json!({ "ok": runs(&mgr)?.unmonitor(&caller.agent_id, &id).await, "monitorId": id }))
        }
        "script.create" => {
            if args.get("remoteAuthor").is_some() {
                return Err(invalid(
                    AUTHOR_READONLY,
                    "remoteAuthor is set by the backend",
                ));
            }
            if args.get("autoStart").and_then(Value::as_bool) == Some(true) {
                return Err(invalid(AUTOSTART, "a remote agent cannot set autoStart"));
            }
            if let Some(id) = args
                .get("scriptId")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
            {
                if svc
                    .store()
                    .get_script_in_workspace(&caller.workspace_id, id)
                    .await?
                    .is_some()
                {
                    let def = require_authored(svc, caller, id).await?;
                    if def.auto_start == Some(true) {
                        return Err(invalid(AUTOSTART, "the script has autoStart set"));
                    }
                }
            }
            definitions(svc, caller, method, &args, budget).await
        }
        "script.remove" => {
            require_authored(svc, caller, &str_arg(&args, "scriptId")?).await?;
            definitions(svc, caller, method, &args, budget).await
        }
        "script.archive" | "script.restore" => {
            for id in args
                .get("scriptIds")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
            {
                require_authored(svc, caller, id).await?;
            }
            definitions(svc, caller, method, &args, budget).await
        }
        "script.list" | "script.listPage" | "script.get" => {
            definitions(svc, caller, method, &args, budget).await
        }
        other => Err(Error::InvalidParams(format!(
            "remote-agent-unsupported: `{other}` is not supported on remote agents"
        ))),
    }
}

/// The definition methods, through the shared `ws.script.*` binding so the
/// arguments parse exactly as a local agent's; `create` records the
/// caller as `remoteAuthor`.
async fn definitions(
    svc: &Services,
    caller: &RemoteScriptCaller,
    method: &str,
    args: &Value,
    budget: Duration,
) -> Result<Value> {
    let api: Arc<dyn WorkspaceApi> = Arc::new(AuthoredScripts {
        svc: svc.clone(),
        author: ScriptRemoteAuthor {
            agent_id: caller.agent_id.clone(),
            node_id: caller.node_id.clone(),
        },
    });
    let name = method.strip_prefix("script.").unwrap_or(method);
    intent_acp::dispatch_script_method(
        &api,
        &caller.workspace_id,
        budget,
        Some(&caller.agent_id),
        name,
        args,
    )
    .await
    .map_err(Error::InvalidParams)
}

/// The backend's script definitions as a remote agent sees them: every
/// read as for any caller, and `create` recording the agent as the new
/// script's `remoteAuthor`.
struct AuthoredScripts {
    svc: Services,
    author: ScriptRemoteAuthor,
}

impl WorkspaceApi for AuthoredScripts {
    fn script_list(&self, workspace_id: WorkspaceId) -> BoxFuture<'_, Result<Value>> {
        self.svc.script_list(workspace_id)
    }
    fn script_list_filtered(
        &self,
        workspace_id: WorkspaceId,
        archive: intent_core::ScriptArchiveFilter,
    ) -> BoxFuture<'_, Result<Value>> {
        self.svc.script_list_filtered(workspace_id, archive)
    }
    fn script_list_view(
        &self,
        workspace_id: WorkspaceId,
        options: Value,
    ) -> BoxFuture<'_, Result<Value>> {
        self.svc.script_list_view(workspace_id, options)
    }
    fn script_get(
        &self,
        workspace_id: WorkspaceId,
        script_id: String,
    ) -> BoxFuture<'_, Result<Value>> {
        self.svc.script_get(workspace_id, script_id)
    }
    fn script_remove(
        &self,
        workspace_id: WorkspaceId,
        script_id: String,
    ) -> BoxFuture<'_, Result<Value>> {
        self.svc.script_remove(workspace_id, script_id)
    }
    fn script_archive(
        &self,
        workspace_id: WorkspaceId,
        script_ids: Vec<String>,
    ) -> BoxFuture<'_, Result<Value>> {
        self.svc.script_archive(workspace_id, script_ids)
    }
    fn script_restore(
        &self,
        workspace_id: WorkspaceId,
        script_ids: Vec<String>,
    ) -> BoxFuture<'_, Result<Value>> {
        self.svc.script_restore(workspace_id, script_ids)
    }
    fn script_create(
        &self,
        workspace_id: WorkspaceId,
        params: ScriptCreateParams,
    ) -> BoxFuture<'_, Result<Value>> {
        let mgr = self.svc.script_manager();
        let author = self.author.clone();
        Box::pin(async move {
            mgr.create_by_remote_agent(workspace_id, params, author)
                .await
        })
    }
}

/// The app's (or a local agent's) `script.<method>` with `remoteAgentId`:
/// it reads, stops or restarts that agent's existing machine run; only the
/// remote agent itself starts one.
pub(crate) async fn machine_run_call(
    svc: &Services,
    workspace_id: WorkspaceId,
    method: &str,
    params: Value,
) -> Result<Value> {
    let agent = str_arg(&params, "remoteAgentId")?;
    let script_id = str_arg(&params, "scriptId")?;
    // Authorized exactly as the home run's `script.<method>`.
    svc.require_saved_script_operator(&workspace_id, &script_id, &format!("script.{method}"))
        .await?;
    let place = RunPlace {
        workspace_id: workspace_id.clone(),
        script_id: str_arg(&params, "scriptId")?,
        agent_id: AgentId::from(agent.as_str()),
    };
    let mgr = svc.script_manager();
    let runs = runs(&mgr)?;
    let unknown = || {
        Error::InvalidParams(format!(
            "script {} has no machine run for remote agent {agent}",
            place.script_id
        ))
    };
    match method {
        "start" | "run" => Err(invalid(
            START_REMOTE_ONLY,
            "only the remote agent can start a run on its machine",
        )),
        "status" => Ok(json!(runs.state(&place).ok_or_else(unknown)?)),
        "output" => {
            runs.state(&place).ok_or_else(unknown)?;
            Ok(runs
                .output_reply(&place, max_lines(&params))
                .unwrap_or_default())
        }
        "stop" => Ok(json!(runs.stop(&place).await?)),
        "restart" => {
            runs.state(&place).ok_or_else(unknown)?;
            let def = definition(svc, &workspace_id, &place.script_id).await?;
            Ok(json!(runs.restart(&place, &def).await?))
        }
        other => Err(Error::InvalidParams(format!(
            "script.{other} does not take remoteAgentId"
        ))),
    }
}
