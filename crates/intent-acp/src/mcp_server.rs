//! In-process agent→BE MCP server (§6.8) — the key orchestration loop.
//!
//! Exposes the workspace tools over the SAME `Arc<dyn WorkspaceApi>` the FE's
//! JSON-RPC router uses ("one impl, two front doors"). Agents reach these tools
//! via MCP (`tools/list` / `tools/call`); the per-agent-type denylist (§18.4) is
//! enforced internally here while filtering the exposed set — there is no
//! `agent.getAvailableTools` wire method.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use intent_core::settings_file::AgentFeaturesSettings;
use intent_core::{AgentId, TurnAttachmentRegistry, WorkspaceApi, WorkspaceId};
use serde_json::{json, Value};

use crate::tool_restrictions::get_tool_denylist_for_agent_type;

mod bindings;
mod dispatch;
mod tools;

pub(crate) use tools::ToolDef;
pub use tools::{
    SpecialistModelOption, SpecialistModelOptions, WORKSPACE_API_SYSTEM_PROMPT_HEADING,
};

// Static description const, exposed for the segment-assembly parity tests
// in `crate::tests` (the assembled all-defaults description must be
// byte-identical to it).
#[cfg(test)]
pub(crate) use tools::WORKSPACE_API_DESCRIPTION;

// Canonical proposal helpers (§7.1): the collapsed-output proposal lift in
// `intent-services::tool_block` reuses these so validation and resource-item
// construction cannot drift from what `ws.app.proposal.show` emits.
pub use bindings::app::proposal::{
    is_valid_proposal, proposal_resource_uri, PROPOSAL_RESOURCE_MIME_TYPE,
};

// Canonical question MIME type (§7.1): the question-hold derivation in
// `intent-services` reuses this so hold detection cannot drift from what
// `ws.app.question.ask` emits.
pub use bindings::app::question::QUESTION_RESOURCE_MIME_TYPE;

// Hook-scheduler seam: the background hook runner in `intent-services`
// evaluates agent scripts with the same `ws.*` prelude + host dispatch the
// `workspace_api` tool installs, so the two environments cannot drift.
pub use bindings::prelude_for_bridge as bindings_prelude_for_bridge;
pub use dispatch::make_workspace_host_for_bridge;

/// Protocol version advertised on `initialize` (matches the TS server).
pub const MCP_PROTOCOL_VERSION: &str = "2024-11-05";

/// The agent→BE MCP server: a fixed workspace context, the shared service
/// surface, and the set of tool names denied for this agent's type.
pub struct WorkspaceMcpServer {
    api: Arc<dyn WorkspaceApi>,
    workspace_id: WorkspaceId,
    denylist: HashSet<String>,
    name: String,
    version: String,
    /// The agent this server front-doors for. Caller-aware tools (e.g.
    /// `delegate_task`) attribute their actions to this id so children can be
    /// stamped with their spawning parent. `None` for the FE/RPC front door.
    caller_agent_id: Option<AgentId>,
    /// Whether this server serves the chief workspace. Used to select between
    /// the base tool description (which omits most `ws.app.*`) and the chief
    /// variant (which advertises the full `ws.app.*` surface).
    is_chief: bool,
    /// Wall-clock budget for one `workspace_api` invocation. Production keeps
    /// the 30s default; tests compress it so timeout-path coverage completes
    /// in milliseconds.
    workspace_api_timeout: Duration,
    /// Turn-attachment registry (§7.1 deterministic attach). When wired —
    /// together with a `caller_agent_id` — a `workspace_api` result carrying
    /// a resource content item registers the canonical payload in-process
    /// (nonce stamped into the model-facing output) so the transcript writer
    /// attaches it without depending on the provider's echo fidelity. `None`
    /// keeps the legacy echo-parse-only behavior (FE front door, tests).
    turn_attachments: Option<Arc<TurnAttachmentRegistry>>,
    /// `[agentFeatures]` toggles captured at bridge creation (new sessions
    /// only — mid-session settings changes never affect a live bridge).
    /// Disabled features are pruned from the tool description and JS prelude
    /// and denied at dispatch. Defaults to all-on (FE front door, tests).
    agent_features: AgentFeaturesSettings,
    /// Per-specialist delegation model options (PROTOCOL §5.11
    /// `modelOptions`), captured at bridge creation like `agent_features` and
    /// injected into the `workspace_api` description's `ws.agent.delegate`
    /// docs. Only specialists that carry options appear; empty — the default
    /// — leaves the description byte-identical.
    specialist_model_options: Vec<tools::SpecialistModelOptions>,
    /// Whether this server front-doors a sub-agent (a session with a
    /// `parent_agent_id` or `is_background`), captured once at bridge
    /// creation like `agent_features`. Sub-agents don't own a user-facing
    /// chat turn, so `ws.app.question.*` is pruned from their description
    /// and prelude and denied at dispatch with a redirect to the
    /// attention-request methods. Defaults to `false` (top-level).
    is_sub_agent: bool,
    /// Whether `tools/list` serves the compact `workspace_api` description
    /// (`ProviderConfig::truncates_tool_descriptions` — providers whose MCP
    /// client cuts long descriptions at ~2k chars). The condensed reference
    /// then rides the session's system prompt via
    /// [`Self::condensed_workspace_api_description`]. Defaults to `false`:
    /// every non-flagged provider keeps today's full description
    /// byte-identical.
    compact_tool_descriptions: bool,
}

impl WorkspaceMcpServer {
    /// Build a server with no tool restrictions (interactive/foreground agents).
    pub fn new(api: Arc<dyn WorkspaceApi>, workspace_id: WorkspaceId) -> Self {
        let is_chief = workspace_id.is_chief();
        Self {
            api,
            workspace_id,
            denylist: HashSet::new(),
            name: "workspace-mcp".to_string(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            caller_agent_id: None,
            is_chief,
            workspace_api_timeout: dispatch::default_workspace_api_timeout(),
            turn_attachments: None,
            agent_features: AgentFeaturesSettings::default(),
            specialist_model_options: Vec::new(),
            is_sub_agent: false,
            compact_tool_descriptions: false,
        }
    }

    /// Build a server whose exposed tool set has the §18.4 denylist for
    /// `agent_type` removed (the spawn-time enforcement point).
    pub fn for_agent_type(
        api: Arc<dyn WorkspaceApi>,
        workspace_id: WorkspaceId,
        agent_type: &str,
    ) -> Self {
        let denylist = get_tool_denylist_for_agent_type(agent_type)
            .into_iter()
            .map(std::string::ToString::to_string)
            .collect();
        Self {
            denylist,
            ..Self::new(api, workspace_id)
        }
    }

    /// Override the denied tool names directly (testing / custom policies).
    #[cfg(test)]
    pub(crate) fn with_denylist<I, S>(mut self, names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.denylist = names.into_iter().map(Into::into).collect();
        self
    }

    /// Set the calling agent this server front-doors for (the spawn-time wiring
    /// point). Caller-aware tools attribute their actions to this id.
    #[must_use]
    pub fn with_caller_agent_id(mut self, caller: Option<AgentId>) -> Self {
        self.caller_agent_id = caller;
        self
    }

    /// Wire the daemon-wide turn-attachment registry (§7.1 deterministic
    /// attach). Registration only activates when a `caller_agent_id` is also
    /// set — the registry keys pending attachments by agent.
    #[must_use]
    pub fn with_turn_attachments(mut self, registry: Option<Arc<TurnAttachmentRegistry>>) -> Self {
        self.turn_attachments = registry;
        self
    }

    /// Capture the `[agentFeatures]` toggles for this bridge (the spawn-time
    /// wiring point — settings are read once at bridge creation so live
    /// sessions keep their original surface). Disabled features are pruned
    /// from the tool description and JS prelude and denied at dispatch.
    #[must_use]
    pub fn with_agent_features(mut self, features: AgentFeaturesSettings) -> Self {
        self.agent_features = features;
        self
    }

    /// Set the per-specialist delegation model options advertised in the
    /// `workspace_api` description (the spawn-time wiring point — resolved
    /// once at bridge creation, like `agent_features`). Pass only specialists
    /// that carry options; an empty list keeps the default description.
    #[must_use]
    pub fn with_specialist_model_options(
        mut self,
        options: Vec<tools::SpecialistModelOptions>,
    ) -> Self {
        self.specialist_model_options = options;
        self
    }

    /// Mark this bridge as serving a sub-agent (the spawn-time wiring point —
    /// derived once at bridge creation from the session:
    /// `parent_agent_id.is_some() || is_background`). Sub-agent bridges prune
    /// `ws.app.question.*` from the tool description and JS prelude and deny
    /// it at dispatch with a redirect to the attention-request methods.
    #[must_use]
    pub fn with_sub_agent(mut self, is_sub_agent: bool) -> Self {
        self.is_sub_agent = is_sub_agent;
        self
    }

    /// Serve the compact `workspace_api` description from `tools/list` (the
    /// spawn-time wiring point for providers with
    /// `ProviderConfig::truncates_tool_descriptions`). The caller pairs this
    /// with [`Self::condensed_workspace_api_description`] appended to the
    /// system prompt so the reference survives client-side truncation.
    #[must_use]
    pub fn with_compact_tool_descriptions(mut self, compact: bool) -> Self {
        self.compact_tool_descriptions = compact;
        self
    }

    /// The fully assembled `workspace_api` description for THIS bridge —
    /// chief-ness, effective `[agentFeatures]` gating (sub-agent question
    /// gate folded in), and specialist model options all applied — exactly
    /// what a non-truncating provider's `tools/list` serves.
    #[must_use]
    pub fn full_workspace_api_description(&self) -> String {
        tools::workspace_api_description_with_model_options(
            self.is_chief,
            &self.effective_agent_features(),
            &self.specialist_model_options,
        )
        .into_owned()
    }

    /// The condensed `workspace_api` reference for THIS bridge — the same
    /// per-agent assembly as [`Self::full_workspace_api_description`]
    /// (chief-ness, gating, model options) with every API method line cut at
    /// its first summary sentence and continuation lines dropped (full docs
    /// stay reachable via `ws.help()`). Used by the spawn path to append the
    /// reference to the system prompt when `tools/list` serves the compact
    /// variant.
    #[must_use]
    pub fn condensed_workspace_api_description(&self) -> String {
        tools::condensed_workspace_api_description(
            self.is_chief,
            &self.effective_agent_features(),
            &self.specialist_model_options,
        )
    }

    /// Override the wall-clock budget for one `workspace_api` invocation
    /// (testing) — compresses the 30s production default so timeout-path
    /// tests finish in milliseconds.
    #[cfg(test)]
    pub(crate) fn with_workspace_api_timeout(mut self, timeout: Duration) -> Self {
        self.workspace_api_timeout = timeout;
        self
    }

    /// The effective wall-clock budget for one `workspace_api` invocation
    /// (30s default, `INTENTD_WORKSPACE_API_TIMEOUT_MS` override, or the
    /// builder override above). The bridge derives its dispatch watchdog
    /// deadline from this so the two budgets can never cross (monorepo#2709).
    pub(crate) fn workspace_api_timeout(&self) -> Duration {
        self.workspace_api_timeout
    }

    /// Whether `name` is denied for this agent.
    #[cfg(test)]
    pub(crate) fn is_denied(&self, name: &str) -> bool {
        self.denylist.contains(name)
    }

    /// The `[agentFeatures]` toggles as they apply to THIS bridge: a
    /// sub-agent bridge sees `structuredQuestions` forced off, so the
    /// description and prelude layers prune `ws.app.question.*` through the
    /// exact same machinery as the settings toggle (the surfaces cannot
    /// drift). The dispatch layer additionally checks `is_sub_agent` FIRST,
    /// so a raw `host({...})` frame gets the explicit top-level-only
    /// redirect error rather than a misleading "disabled in settings".
    fn effective_agent_features(&self) -> AgentFeaturesSettings {
        let mut features = self.agent_features.clone();
        if self.is_sub_agent {
            features.structured_questions = false;
        }
        features
    }

    /// The tool definitions exposed to this agent (full registry minus denylist).
    pub(crate) fn available_tools(&self) -> Vec<&'static ToolDef> {
        tools::all_tools(self.is_chief)
            .iter()
            .filter(|t| !self.denylist.contains(t.name))
            .collect()
    }

    /// Handle one MCP JSON-RPC message. Returns `Some(response)` for requests and
    /// `None` for notifications (port of `MCPServer.handleMessage`).
    pub async fn handle_message(&self, message: &Value) -> Option<Value> {
        let method = message.get("method").and_then(Value::as_str)?;
        let id = message.get("id").cloned();
        match id {
            Some(id) => Some(self.handle_request(&id, method, message).await),
            None => None,
        }
    }

    async fn handle_request(&self, id: &Value, method: &str, message: &Value) -> Value {
        match method {
            "initialize" => ok(id, self.initialize_result()),
            "tools/list" => ok(id, self.list_tools_result()),
            "tools/call" => self.call_tool(id, message.get("params")).await,
            other => err(id, -32601, &format!("Unknown method: {other}")),
        }
    }

    fn initialize_result(&self) -> Value {
        json!({
            "protocolVersion": MCP_PROTOCOL_VERSION,
            "capabilities": { "tools": { "listChanged": false } },
            "serverInfo": { "name": self.name, "version": self.version },
        })
    }

    fn list_tools_result(&self) -> Value {
        let tools: Vec<Value> = self
            .available_tools()
            .iter()
            .map(|t| {
                // `workspace_api` gets its description assembled per-bridge so
                // `[agentFeatures]` toggles captured at creation (with the
                // sub-agent question gate folded in) prune the disabled
                // surface and specialist `modelOptions` extend the delegate
                // docs; with all defaults on (and no options, top-level) the
                // assembled text is the static const unchanged. Bridges for
                // truncating providers serve the compact variant instead —
                // the condensed reference rides the system prompt
                // (`condensed_workspace_api_description`).
                let description = if t.name == "workspace_api" {
                    if self.compact_tool_descriptions {
                        tools::compact_workspace_api_description(
                            self.is_chief,
                            &self.effective_agent_features(),
                        )
                    } else {
                        self.full_workspace_api_description()
                    }
                } else {
                    t.description.to_string()
                };
                json!({
                    "name": t.name,
                    "description": description,
                    "inputSchema": t.schema(),
                })
            })
            .collect();
        json!({ "tools": tools })
    }

    async fn call_tool(&self, id: &Value, params: Option<&Value>) -> Value {
        let params = params.cloned().unwrap_or_else(|| json!({}));
        let Some(name) = params.get("name").and_then(Value::as_str) else {
            return err(id, -32602, "Missing tool name");
        };
        // After the WSAPI-8 cutover `workspace_api` is the only tool the
        // daemon registers or dispatches. Check registration BEFORE the
        // denylist so that legacy discrete names or agent-provider built-ins
        // that happen to also appear on the denylist surface the accurate
        // "Tool not found" error rather than the misleading
        // "Tool not available", and so that a future registry entry cannot
        // silently mis-dispatch through the JS-eval path below.
        if name != "workspace_api" {
            return err(id, -32602, &format!("Tool not found: {name}"));
        }
        if self.denylist.contains(name) {
            return err(id, -32602, &format!("Tool not available: {name}"));
        }
        let args = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        // The `workspace_api` input schema declares both `code` and `summary`
        // as required; `code` is validated inside `dispatch_workspace_api`,
        // and `summary` is enforced here so malformed calls fail with a clear
        // MCP error before we spin up the JS engine (reference parity with
        // the TS `workspace-js-api-tool`, which lists both as required).
        if !args.get("summary").is_some_and(Value::is_string) {
            return err(id, -32602, "`summary` is required and must be a string");
        }
        // `workspace_api` shapes its own MCP tool result (isError=true
        // text bodies for JS-side failures — reference parity with the TS
        // tool) instead of the discrete-tool `Ok(value) -> tool_content` map.
        ok(id, self.dispatch_workspace_api(&args).await)
    }
}

// By-value: callers hand over freshly built payloads.
#[allow(clippy::needless_pass_by_value)]
fn ok(id: &Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

fn err(id: &Value, code: i32, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}
