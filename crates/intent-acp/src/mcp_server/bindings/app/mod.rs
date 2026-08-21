//! `ws.app.*` bindings (chief-gated, with one exception).
//!
//! Namespace module for Chief-of-Staff workspace-only APIs. All `ws.app.*`
//! methods gate on `workspace_id.is_chief()` — non-chief agents receive a
//! clear "ws.app.* is only available in the Chief of Staff workspace" error
//! — EXCEPT `ws.app.question.*`, which is available to every TOP-LEVEL
//! workspace agent regardless of chief-ness (see the `question` module docs;
//! sub-agent callers are denied one layer up, in the dispatch host).
//!
//! Structure: `app/mod.rs` owns the PRELUDE assembly and top-level dispatch
//! routing to submodules (`workspaces`, `agents`, `settings`, `specialists`).
//! Each submodule implements its own chief-gating check and dispatch.

use std::sync::Arc;

use intent_core::settings_file::AgentFeaturesSettings;
use intent_core::{AgentId, TurnAttachmentRegistry, WorkspaceApi, WorkspaceId};
use serde_json::Value;

pub(crate) mod agents;
pub(crate) mod proposal;
pub(crate) mod question;
pub(crate) mod settings;
pub(crate) mod specialists;
pub(crate) mod ui;
pub(crate) mod workspaces;

/// Assemble the `ws.app.*` PRELUDE from all submodules. Each submodule
/// installs its portion of the namespace (e.g., `ws.app.workspaces`,
/// `ws.app.agents`). The prelude is unconditional (chief-gating is enforced
/// server-side in dispatch) with ONE exception: `ws.app.question` is omitted
/// when `agentFeatures.structuredQuestions` is off, so a disabled bridge
/// fails with a clear `ws.app.question is undefined` `TypeError`.
pub(crate) fn prelude_for(features: &AgentFeaturesSettings) -> String {
    let mut fragments = vec![workspaces::PRELUDE, agents::PRELUDE, proposal::PRELUDE];
    if features.structured_questions {
        fragments.push(question::PRELUDE);
    }
    fragments.extend([settings::PRELUDE, specialists::PRELUDE, ui::PRELUDE]);
    fragments.join("\n")
}

/// Dispatch one `ws.app.<subns>.<method>` call to the matching submodule.
/// Returns `Ok(None)` when the subnamespace is unknown; `Ok(Some(v))` on
/// success; `Err(msg)` on failure. The chief-workspace gate is delegated to
/// each submodule's dispatch so they can surface the same clear error —
/// except `question.*`, which is deliberately chief-un-gated (any TOP-LEVEL
/// agent may ask; the sub-agent gate lives in the dispatch host, before
/// this router is reached).
/// `caller_agent_id` threads the tool-call's agent context to the
/// caller-aware `agents` methods (`waitFor`) and to `question.ask` (the
/// turn-attachment registry keys pending questions by agent);
/// `turn_attachments` is the registry `question.ask` registers into.
pub(crate) async fn try_dispatch(
    api: &Arc<dyn WorkspaceApi>,
    workspace_id: &WorkspaceId,
    caller_agent_id: Option<&AgentId>,
    turn_attachments: Option<&Arc<TurnAttachmentRegistry>>,
    method: &str,
    args: &Value,
) -> Result<Option<Value>, String> {
    if let Some(rest) = method.strip_prefix("question.") {
        return question::dispatch(turn_attachments, caller_agent_id, rest, args).map(Some);
    }
    if let Some(rest) = method.strip_prefix("workspaces.") {
        return workspaces::dispatch(api, workspace_id, rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("agents.") {
        return agents::dispatch(api, workspace_id, caller_agent_id, rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("proposal.") {
        return proposal::dispatch(api, workspace_id, rest, args).map(Some);
    }
    if let Some(rest) = method.strip_prefix("settings.") {
        return settings::dispatch(api, workspace_id, rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("specialists.") {
        return specialists::dispatch(api, workspace_id, rest, args)
            .await
            .map(Some);
    }
    if let Some(rest) = method.strip_prefix("ui.") {
        return ui::dispatch(api, workspace_id, rest, args).await.map(Some);
    }
    Ok(None)
}
