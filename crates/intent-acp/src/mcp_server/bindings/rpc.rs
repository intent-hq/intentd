//! `ws.rpc.*` — the Assistant's generic daemon JSON-RPC tool. Installed and
//! served only for a top-level agent in the Assistant workspace; the services
//! layer re-checks the workspace and holds mutating calls for approval.

use std::fmt::Write as _;
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

/// Older Assistant bindings that `ws.rpc` fully replaces, with the call to
/// use instead. The top-level Assistant gets a stub for each that throws the
/// pointer, and its docs omit them; sub-agents (no `ws.rpc`) keep them.
/// Bindings the versioned Assistant prompts teach, whose results have no RPC
/// equivalent (`workspaces.list` / `search`, the slim `agents.*` and `chats.*`
/// reads), or whose cards do more than an RPC card (archive / delete show
/// titles, allow deselecting and continue past a failure), stay.
pub(crate) const REPLACED_BY_RPC: &[(&str, &str)] = &[
    (
        "app.workspaces.get",
        r#"ws.rpc.call("workspace.get", { workspaceId })"#,
    ),
    ("app.mcp.listServers", r#"ws.rpc.call("mcp.servers.list")"#),
    (
        "app.mcp.getServerStatus",
        r#"ws.rpc.call("mcp.servers.getStatus", { serverId })"#,
    ),
    ("app.settings.list", r#"ws.rpc.call("settings.list")"#),
    (
        "app.settings.get",
        r#"ws.rpc.call("settings.get", { path })"#,
    ),
    ("app.specialists.list", r#"ws.rpc.call("specialist.list")"#),
    (
        "app.specialists.get",
        r#"ws.rpc.call("specialist.get", { id })"#,
    ),
    (
        "crossWorkspace.listSiblings",
        "ws.app.workspaces.list() (ws.crossWorkspace is repo-scoped)",
    ),
    (
        "crossWorkspace.listNotes",
        r#"ws.rpc.call("note.list", { workspaceId })"#,
    ),
    (
        "crossWorkspace.readNote",
        r#"ws.rpc.call("note.get", { workspaceId, noteId })"#,
    ),
    ("crossWorkspace.search", "ws.app.workspaces.search(query)"),
];

/// Whether a `ws.*` doc line documents a [`REPLACED_BY_RPC`] binding (or the
/// `ws.crossWorkspace` index entry).
pub(crate) fn documents_replaced_binding(line: &str) -> bool {
    let Some(rest) = line.trim_start().strip_prefix("ws.") else {
        return false;
    };
    rest.starts_with("crossWorkspace.")
        || REPLACED_BY_RPC
            .iter()
            .any(|(path, _)| rest.strip_prefix(path).is_some_and(|r| r.starts_with('(')))
}

/// JS run after every namespace prelude for the top-level Assistant: each
/// replaced binding becomes a stub that throws its `ws.rpc` pointer.
pub(crate) fn replaced_prelude() -> String {
    let mut js =
        String::from("\n    globalThis.ws = globalThis.ws || {};\n    ws.crossWorkspace = {};\n");
    for (path, use_instead) in REPLACED_BY_RPC {
        let (ns, name) = path.rsplit_once('.').unwrap_or(("", path));
        let message = format!("ws.{path} is retired for the Assistant: use {use_instead}");
        let _ = writeln!(
            js,
            "    if (ws.{ns}) ws.{ns}.{name} = async () => {{ throw new Error({}); }};",
            Value::String(message)
        );
    }
    js
}

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

#[cfg(test)]
mod tests {
    use super::super::super::tools::workspace_api_description_for_bridge;
    use super::*;
    use intent_core::settings_file::AgentFeaturesSettings;

    fn doc_lines(is_sub_agent: bool) -> Vec<String> {
        workspace_api_description_for_bridge(true, &AgentFeaturesSettings::default(), is_sub_agent)
            .lines()
            .map(str::to_string)
            .collect()
    }

    #[test]
    fn every_replaced_binding_is_documented_and_hidden_only_from_the_top_level_assistant() {
        let sub = doc_lines(true);
        let top = doc_lines(false);
        for (path, _) in REPLACED_BY_RPC {
            let line = format!("  ws.{path}(");
            assert!(
                sub.iter().any(|l| l.starts_with(&line)),
                "{path} undocumented"
            );
            assert!(
                !top.iter().any(|l| l.starts_with(&line)),
                "{path} still shown"
            );
        }
        let top = top.join("\n");
        assert!(!top.contains("ws.crossWorkspace"), "{top}");
        assert!(!top.contains("listServers first"), "{top}");
        assert!(top.contains("  ws.app.workspaces.list("));
        assert!(top.contains("  ws.app.agents.readConversation("));
    }

    #[test]
    fn only_the_assistant_agent_bridge_gets_the_stubs() {
        let features = AgentFeaturesSettings::default();
        let chief = WorkspaceId::chief();
        let marker = "is retired for the Assistant";
        assert!(
            super::super::prelude_for_agent_bridge(&features, false, &chief, false)
                .contains(marker)
        );
        for (is_sub_agent, workspace) in [(true, &chief), (false, &WorkspaceId::new())] {
            assert!(!super::super::prelude_for_agent_bridge(
                &features,
                is_sub_agent,
                workspace,
                false
            )
            .contains(marker));
        }
        // Hooks load this prelude; their saved code keeps the older bindings.
        assert!(
            !super::super::prelude_for_workspace(&features, false, &chief, false).contains(marker)
        );
    }

    #[test]
    fn each_stub_throws_its_pointer() {
        let js = replaced_prelude();
        for (path, use_instead) in REPLACED_BY_RPC {
            let (ns, name) = path.rsplit_once('.').expect("namespaced path");
            assert!(
                js.contains(&format!("ws.{ns}.{name} = async () =>")),
                "{path}"
            );
            let message = Value::String(format!(
                "ws.{path} is retired for the Assistant: use {use_instead}"
            ));
            assert!(js.contains(&message.to_string()), "{path}");
        }
    }
}
