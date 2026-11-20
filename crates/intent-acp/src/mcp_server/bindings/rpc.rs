//! `ws.rpc.*` — the Assistant's generic daemon JSON-RPC tool. Installed and
//! served only for a top-level agent in the Assistant workspace; the services
//! layer re-checks the workspace and holds mutating calls for approval.

use std::sync::Arc;

use intent_core::agent_rpc::{AgentRpcCallSpec, AgentRpcRequest};
use intent_core::{AgentId, WorkspaceApi, WorkspaceId};
use serde_json::Value;

use super::{map_err, opt_str, EvalBudget};

pub(crate) const PRELUDE: &str = r"
    globalThis.ws = globalThis.ws || {};
    ws.rpc = {
        info: (opts) => host({ method: 'rpc.info', args: opts || {} }),
        methods: (opts) => host({ method: 'rpc.methods', args: typeof opts === 'string' ? { prefix: opts } : (opts || {}) }),
        backends: () => host({ method: 'rpc.backends', args: {} }),
        call: (method, params, opts) => host({ method: 'rpc.call', args: { ...(opts || {}), method, params: params ?? {} } }),
        batch: (calls, opts) => host({ method: 'rpc.batch', args: { ...(opts || {}), calls } }),
    };
";

/// Refusal for a frame from outside the Assistant's top-level bridge.
pub(crate) const RPC_ASSISTANT_ONLY: &str =
    "ws.rpc is only available to the top-level Assistant agent";

pub(crate) async fn dispatch(
    api: &Arc<dyn WorkspaceApi>,
    workspace_id: &WorkspaceId,
    caller_agent_id: Option<&AgentId>,
    is_sub_agent: bool,
    eval_budget: EvalBudget,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    if !workspace_id.is_chief() || is_sub_agent {
        return Err(format!("host: {RPC_ASSISTANT_ONLY}"));
    }
    match method {
        "info" => describe(api, workspace_id, "info", args, eval_budget).await,
        "methods" => describe(api, workspace_id, "methods", args, eval_budget).await,
        "backends" => describe(api, workspace_id, "backends", args, eval_budget).await,
        "call" => {
            let rpc_method = opt_str(args, "method")
                .filter(|m| !m.trim().is_empty())
                .ok_or_else(|| "method is required".to_string())?;
            let request = AgentRpcRequest {
                calls: vec![AgentRpcCallSpec {
                    method: rpc_method,
                    params: args.get("params").cloned().unwrap_or(Value::Null),
                    backend: opt_str(args, "backend"),
                }],
                batch: false,
                title: opt_str(args, "title"),
            };
            call(api, workspace_id, caller_agent_id, request, eval_budget).await
        }
        "batch" => {
            let calls = args
                .get("calls")
                .and_then(Value::as_array)
                .ok_or_else(|| {
                    "calls must be an array of { method, params?, backend? }".to_string()
                })?
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    serde_json::from_value::<AgentRpcCallSpec>(c.clone())
                        .map_err(|e| format!("calls[{i}]: {e}"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let request = AgentRpcRequest {
                calls,
                batch: true,
                title: opt_str(args, "title"),
            };
            call(api, workspace_id, caller_agent_id, request, eval_budget).await
        }
        other => Err(format!("host: unknown method `rpc.{other}`")),
    }
}

async fn describe(
    api: &Arc<dyn WorkspaceApi>,
    workspace_id: &WorkspaceId,
    what: &str,
    args: &Value,
    eval_budget: EvalBudget,
) -> Result<Value, String> {
    let mut args = if args.is_object() {
        args.clone()
    } else {
        serde_json::json!({})
    };
    args["budgetMs"] =
        serde_json::json!(u64::try_from(eval_budget.remaining().as_millis()).unwrap_or(u64::MAX));
    api.agent_rpc_describe(workspace_id.clone(), what.to_string(), args)
        .await
        .map_err(map_err)
}

async fn call(
    api: &Arc<dyn WorkspaceApi>,
    workspace_id: &WorkspaceId,
    caller_agent_id: Option<&AgentId>,
    request: AgentRpcRequest,
    eval_budget: EvalBudget,
) -> Result<Value, String> {
    api.agent_rpc_call(
        workspace_id.clone(),
        caller_agent_id.cloned(),
        request,
        eval_budget.remaining(),
    )
    .await
    .map_err(map_err)
}
