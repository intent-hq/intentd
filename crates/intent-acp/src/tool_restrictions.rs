//! Per-agent-type tool denylist (§18.4) — internal spawn-time enforcement.
//!
//! Direct port of `background-agent-tool-restrictions.ts`. The mapping is
//! **hardcoded** and applied internally while assembling each agent's tool set
//! on spawn (§6.8); it is intentionally NOT a wire method (there is no
//! `agent.getAvailableTools`). It uses a **denylist** (tools to remove) rather
//! than an allowlist, so new tools are denied by default for restricted agents.

/// File modification tools — agents with these can edit the codebase.
pub(crate) const FILE_WRITE_TOOLS: &[&str] = &[
    // Built-in auggie tools
    "str-replace-editor",
    "save-file",
    "remove-files",
    "str_replace",
    "create",
    "apply_patch",
    // MCP workspace tools (bare registry names; the provider appends the
    // `_workspace-mcp` server suffix on its side, §6.8)
    "write_file",
    "delete_file",
    "create_directory",
    "rename_file",
];

/// Git tools — agents with these can mutate git state. (`git_status` is
/// read-only and intentionally omitted.)
pub(crate) const GIT_TOOLS: &[&str] = &["git_stage", "git_commit"];

/// Agent creation/delegation tools — agents with these can spawn/message agents.
pub(crate) const AGENT_CREATION_TOOLS: &[&str] = &[
    "create_agent",
    "delegate_task",
    "send_message_to_agent",
    "send_message_to_task_agent",
    "wake_or_create_task_agent",
    "report_to_parent",
];

/// Note + task + comment + primitive mutation tools.
pub(crate) const NOTE_WRITE_TOOLS: &[&str] = &[
    "create_note",
    "set_note_content",
    "add_to_note",
    "edit_note",
    "edit_note_lines",
    "update_note_metadata",
    "delete_note",
    "update_task_status",
    "update_note_task_status",
    "update_task",
    "mark_as_task",
    "convert_task_blocks",
    "create_prerequisite",
    "assign_agent",
    "add_note_comment",
    "respond_to_comment_thread",
    "add_reference_primitive",
    "add_cli_primitive",
    "add_patch_primitive",
    "add_agent_action_primitive",
];

/// Workspace modification tools.
pub(crate) const WORKSPACE_WRITE_TOOLS: &[&str] = &[
    "rename_space",
    "rename_agent",
    "set_workspace_title",
    "set_workspace_status_message",
];

/// Unified workspace JS API tool (bare + server-suffixed). It can perform any
/// workspace mutation, so pure-text background agents must deny it.
pub(crate) const UNIFIED_WORKSPACE_TOOLS: &[&str] =
    &["workspace_api", "workspace_api_workspace-mcp"];

/// Process/command execution tools.
pub(crate) const EXECUTION_TOOLS: &[&str] = &["launch-process", "execute_command"];

/// External communication tools.
pub(crate) const EXTERNAL_TOOLS: &[&str] = &["web-fetch", "web-search", "github-api"];

/// Subagent orchestration tools.
///
/// auggie's `--remove-tool` matches tool names exactly (a `Set<string>.has(name)`
/// check inside auggie — no wildcard / prefix support was found in `auggie --help`
/// nor in the shipped binary; sub-agent tool names are also enumerated
/// dynamically from configured subagents at runtime), so every current sub-agent
/// tool name has to be listed here. This list must include:
/// - the built-in sub-agent tools auggie ships with (`sub-agent-explore`,
///   `sub-agent-plan`);
/// - the sub-agent tool names generated from the currently configured specialist
///   subagents (`sub-agent-{name}`);
/// - legacy names kept for safety across auggie versions that may still register
///   them.
///
/// Rot risk: if new sub-agents are added to auggie's built-ins or to the
/// specialist config, this list falls out of date silently — the tools would be
/// exposed to spawned agents. Keep this list in sync with `auggie tools list`
/// output and the specialist subagent definitions until a wholesale
/// disable-subagents mechanism is available upstream.
pub const SUBAGENT_TOOLS: &[&str] = &[
    // Legacy names — kept for older auggie versions that may still register them.
    "sub-agent",
    "sub-agent-code-review-local-analyzer",
    // Built-in auggie sub-agents (observed via `auggie tools list`).
    "sub-agent-explore",
    "sub-agent-plan",
    // Configured specialist sub-agents (observed via `auggie tools list`).
    "sub-agent-auggie-guide",
    "sub-agent-general-purpose",
    "sub-agent-research",
    "sub-agent-code",
    "sub-agent-validate",
];

/// Built-in tools that conflict with their workspace-MCP equivalents and are
/// always removed (the MCP versions integrate with the agent lifecycle).
pub(crate) const CONFLICTING_BUILTIN_TOOLS: &[&str] = &["create_agent"];

/// Claude Agent SDK built-in file-write tools disallowed for orchestrator-role
/// agents on the claude-code provider, delivered via
/// `_meta.claudeCode.options.disallowedTools` on `session/new`/`session/load`
/// (the claude-code counterpart of [`FILE_WRITE_TOOLS`], whose auggie-native /
/// MCP names the SDK never exposes). Bare names remove the tool from the
/// model's context entirely — no tool schema, no permission prompt.
///
/// Names match Claude Agent SDK 0.3.220 (pinned by claude-agent-acp 0.66.0):
/// `Edit` covers single- and multi-edit (the SDK has no separate `MultiEdit`),
/// `Write` creates/overwrites files, `NotebookEdit` edits notebook cells.
/// `Task` (native subagents) is denied separately for EVERY claude-code agent,
/// not just orchestrators (see `build_session_meta` in `intent-services`).
/// `Bash` is deliberately NOT listed: orchestrators legitimately run read-only
/// commands (git status, builds, searches), and the role prompt — not tool
/// removal — governs not-editing-via-shell.
pub const CLAUDE_CODE_ORCHESTRATOR_DISALLOWED_TOOLS: &[&str] = &["Edit", "Write", "NotebookEdit"];

/// Droid built-in tools disallowed for orchestrator-role agents, delivered via
/// droid's `--disabled-tools` spawn flag (comma-separated list of tool IDs).
/// `Edit`/`Create`/`ApplyPatch` are the file-write tools, `Task` spawns
/// subagents. `Execute` (shell) is deliberately NOT listed — same rationale
/// as the claude-code list above.
///
/// Naming: tool IDs are case-sensitive `PascalCase`, sourced from
/// docs.factory.ai (custom-droid `tools` arrays like `["Read", "Edit",
/// "Execute"]`, hook matchers `Execute`/`Read`/`Edit`/`Create`/`ApplyPatch`/
/// `Task`, and the sandbox enforcement table) — no droid binary was available
/// to run `droid exec --list-tools` against; re-verify if a droid release
/// renames its built-ins.
pub const DROID_ORCHESTRATOR_DISALLOWED_TOOLS: &[&str] = &["Edit", "Create", "ApplyPatch", "Task"];

/// The full set of categories denied for pure text-generation/analysis agents.
fn full_denylist() -> Vec<&'static str> {
    let mut out = Vec::new();
    for cat in [
        FILE_WRITE_TOOLS,
        GIT_TOOLS,
        AGENT_CREATION_TOOLS,
        NOTE_WRITE_TOOLS,
        WORKSPACE_WRITE_TOOLS,
        UNIFIED_WORKSPACE_TOOLS,
        EXECUTION_TOOLS,
        EXTERNAL_TOOLS,
        SUBAGENT_TOOLS,
    ] {
        out.extend_from_slice(cat);
    }
    out
}

/// Get the tool denylist for an agent type (port of
/// `getToolDenylistForAgentType`). Returns an empty vec for any type that is
/// not a restricted background agent (interactive/foreground agents are
/// unrestricted).
#[must_use]
pub fn get_tool_denylist_for_agent_type(agent_type: &str) -> Vec<&'static str> {
    match agent_type {
        // Pure text-generation / analysis agents: no side effects.
        "commit-message" | "pr-description" | "code-review" | "code-walkthrough" => full_denylist(),
        // Full working agents, but no nested sub-agent spawning.
        "task-loop" | "ralph-loop" | "chat" => SUBAGENT_TOOLS.to_vec(),
        _ => Vec::new(),
    }
}

/// Whether an agent type is a restricted background agent (port of
/// `isBackgroundAgentType`).
#[cfg(test)]
pub(crate) fn is_background_agent_type(agent_type: &str) -> bool {
    matches!(
        agent_type,
        "commit-message"
            | "pr-description"
            | "code-review"
            | "code-walkthrough"
            | "task-loop"
            | "ralph-loop"
            | "chat"
    )
}

/// All restricted background agent types (port of `getBackgroundAgentTypes`).
#[cfg(test)]
pub(crate) fn background_agent_types() -> &'static [&'static str] {
    &[
        "commit-message",
        "pr-description",
        "code-review",
        "code-walkthrough",
        "task-loop",
        "ralph-loop",
        "chat",
    ]
}

/// Resolve the auggie-native tools to strip via `--remove-tool` at spawn time
/// (port of `getToolRestrictionsForAgent` in the reference `acp-provider.ts`).
///
/// Precedence:
/// 1. Orchestrator-role specialist (`is_orchestrator` — resolved by the
///    caller from the specialist registry's `role` frontmatter, with a
///    name-based fallback for the historical `spec-writer`/`coordinator`
///    ids; see `SpecialistsService::resolve_is_orchestrator` in
///    `intent-services`): [`FILE_WRITE_TOOLS`] + [`SUBAGENT_TOOLS`] +
///    [`CONFLICTING_BUILTIN_TOOLS`]. Note: the reference
///    comment mentions `EXECUTION_TOOLS` should also be removed here, but the
///    reference **code** does not include it — we match the code to preserve
///    parity (documented in the PR).
/// 2. Background agent type with a non-empty denylist
///    ([`get_tool_denylist_for_agent_type`]): that denylist +
///    [`CONFLICTING_BUILTIN_TOOLS`]. Note: the background-agent path is
///    additive to the MCP-side filter — the CLI-side `--remove-tool` covers
///    auggie-native tools (e.g. `str-replace-editor`, `launch-process`) that
///    the MCP filter cannot reach.
/// 3. Global fallback: [`SUBAGENT_TOOLS`] + [`CONFLICTING_BUILTIN_TOOLS`] —
///    sub-agents have no UI representation, so every agent must go through the
///    workspace `ws.agent.*` surface instead of the auggie-native sub-agent.
#[must_use]
pub fn get_tools_to_remove(is_orchestrator: bool, agent_type: &str) -> Vec<&'static str> {
    if is_orchestrator {
        let mut out = Vec::with_capacity(
            FILE_WRITE_TOOLS.len() + SUBAGENT_TOOLS.len() + CONFLICTING_BUILTIN_TOOLS.len(),
        );
        out.extend_from_slice(FILE_WRITE_TOOLS);
        out.extend_from_slice(SUBAGENT_TOOLS);
        out.extend_from_slice(CONFLICTING_BUILTIN_TOOLS);
        return out;
    }

    let background = get_tool_denylist_for_agent_type(agent_type);
    if !background.is_empty() {
        let mut out = Vec::with_capacity(background.len() + CONFLICTING_BUILTIN_TOOLS.len());
        out.extend(background);
        out.extend_from_slice(CONFLICTING_BUILTIN_TOOLS);
        return out;
    }

    let mut out = Vec::with_capacity(SUBAGENT_TOOLS.len() + CONFLICTING_BUILTIN_TOOLS.len());
    out.extend_from_slice(SUBAGENT_TOOLS);
    out.extend_from_slice(CONFLICTING_BUILTIN_TOOLS);
    out
}

/// Resolve the provider-NATIVE tools to strip via the provider's spawn-time
/// removal flag (`ProviderConfig::remove_tool_flag`). Each provider names its
/// built-in tools differently, so the generic categories (file-write,
/// sub-agents) map to per-provider name lists — this mapping lives here (not
/// in `intent-providers`) because it extends the §18.4 denylist policy that
/// this module owns.
///
/// - `auggie`: the full [`get_tools_to_remove`] resolution (orchestrator,
///   background-type, and global-fallback paths — auggie-native + MCP names).
/// - `droid`: orchestrator-role agents get the provider's file-write and
///   subagent tool IDs ([`DROID_ORCHESTRATOR_DISALLOWED_TOOLS`]); everyone
///   else gets nothing — MCP-side filtering (§6.8) still covers workspace
///   tools, and the background-agent denylist names are auggie/MCP-specific.
///   Deliberate scope cut: unlike auggie (global fallback strips
///   [`SUBAGENT_TOOLS`] for every agent) and claude-code (`Task` denied for
///   every agent in `build_session_meta`), droid's `Task` is only stripped
///   for orchestrators here — non-orchestrator droid agents keep native
///   sub-agent spawning until a follow-up extends the denylist.
/// - every other provider: nothing. claude-code delivers its orchestrator
///   denylist via `session/new` `_meta` instead (see
///   [`CLAUDE_CODE_ORCHESTRATOR_DISALLOWED_TOOLS`]); grok has no reachable
///   spawn-time knob — its `--disallowed-tools` flag is headless-mode
///   (`grok -p …`) only and is not defined on the `agent stdio` (ACP)
///   subcommand (see the grok entry in `intent-providers`' registry), so
///   grok orchestrator restrictions are prompt-only.
#[must_use]
pub fn get_native_tools_to_remove(
    provider_id: &str,
    is_orchestrator: bool,
    agent_type: &str,
) -> Vec<&'static str> {
    match provider_id {
        "auggie" => get_tools_to_remove(is_orchestrator, agent_type),
        "droid" if is_orchestrator => DROID_ORCHESTRATOR_DISALLOWED_TOOLS.to_vec(),
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn contains_all(haystack: &[&str], needles: &[&str]) -> bool {
        needles.iter().all(|n| haystack.contains(n))
    }

    #[test]
    fn orchestrator_removes_file_write_and_subagents() {
        let tools = get_tools_to_remove(true, "interactive");
        assert!(contains_all(&tools, FILE_WRITE_TOOLS));
        assert!(contains_all(&tools, SUBAGENT_TOOLS));
        assert!(contains_all(&tools, CONFLICTING_BUILTIN_TOOLS));
        // Reference code does NOT include EXECUTION_TOOLS for orchestrators.
        assert!(!tools.contains(&"launch-process"));
    }

    #[test]
    fn background_task_loop_gets_subagent_block_plus_conflicts() {
        let tools = get_tools_to_remove(false, "task-loop");
        assert!(contains_all(&tools, SUBAGENT_TOOLS));
        assert!(contains_all(&tools, CONFLICTING_BUILTIN_TOOLS));
        // task-loop denylist is SUBAGENT_TOOLS only — no file-write.
        assert!(!tools.contains(&"str-replace-editor"));
    }

    #[test]
    fn background_pure_text_agent_gets_full_denylist() {
        let tools = get_tools_to_remove(false, "commit-message");
        assert!(contains_all(&tools, FILE_WRITE_TOOLS));
        assert!(contains_all(&tools, EXECUTION_TOOLS));
        assert!(contains_all(&tools, SUBAGENT_TOOLS));
        assert!(contains_all(&tools, CONFLICTING_BUILTIN_TOOLS));
    }

    #[test]
    fn interactive_agent_gets_global_subagent_block() {
        // Non-orchestrator agents (plain or non-orchestrator specialist)
        // take the global fallback path.
        let tools = get_tools_to_remove(false, "interactive");
        assert!(contains_all(&tools, SUBAGENT_TOOLS));
        assert!(contains_all(&tools, CONFLICTING_BUILTIN_TOOLS));
        // Non-restricted agents keep file-write, execution, etc.
        assert!(!tools.contains(&"str-replace-editor"));
        assert!(!tools.contains(&"launch-process"));
    }

    #[test]
    fn orchestrator_beats_background_agent_type() {
        // An orchestrator running as a background task-loop still gets the
        // orchestrator restrictions (the orchestrator check runs first).
        let tools = get_tools_to_remove(true, "task-loop");
        assert!(contains_all(&tools, FILE_WRITE_TOOLS));
    }

    #[test]
    fn native_resolution_maps_auggie_to_full_denylist_flow() {
        assert_eq!(
            get_native_tools_to_remove("auggie", true, "interactive"),
            get_tools_to_remove(true, "interactive")
        );
        assert_eq!(
            get_native_tools_to_remove("auggie", false, "task-loop"),
            get_tools_to_remove(false, "task-loop")
        );
    }

    #[test]
    fn native_resolution_droid_orchestrator_only() {
        assert_eq!(
            get_native_tools_to_remove("droid", true, "interactive"),
            DROID_ORCHESTRATOR_DISALLOWED_TOOLS.to_vec()
        );
        // Non-orchestrator agents on droid get no CLI-side stripping —
        // MCP-side filtering (§6.8) still applies.
        assert!(get_native_tools_to_remove("droid", false, "interactive").is_empty());
        assert!(get_native_tools_to_remove("droid", false, "task-loop").is_empty());
    }

    #[test]
    fn native_resolution_other_providers_get_nothing() {
        // grok included: its `--disallowed-tools` flag is headless-only and
        // unreachable from `agent stdio`, so grok must resolve to nothing
        // even for orchestrators.
        for id in [
            "claude-code",
            "codex",
            "cortex",
            "grok",
            "opencode",
            "pi",
            "mock",
        ] {
            assert!(
                get_native_tools_to_remove(id, true, "interactive").is_empty(),
                "{id} unexpectedly received native tools to remove"
            );
        }
    }

    #[test]
    fn subagent_tools_covers_current_auggie_names() {
        // Rot-check: the currently-observed sub-agent tool names (from
        // `auggie tools list`) must all be enumerated.
        for name in [
            "sub-agent-explore",
            "sub-agent-plan",
            "sub-agent-auggie-guide",
            "sub-agent-general-purpose",
            "sub-agent-research",
            "sub-agent-code",
            "sub-agent-validate",
        ] {
            assert!(
                SUBAGENT_TOOLS.contains(&name),
                "SUBAGENT_TOOLS missing current sub-agent tool `{name}` — refresh from `auggie tools list`"
            );
        }
    }
}
