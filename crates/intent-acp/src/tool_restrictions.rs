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
/// 1. Coordinator / spec-writer specialist: [`FILE_WRITE_TOOLS`] +
///    [`SUBAGENT_TOOLS`] + [`CONFLICTING_BUILTIN_TOOLS`]. Note: the reference
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
pub fn get_tools_to_remove(specialist: Option<&str>, agent_type: &str) -> Vec<&'static str> {
    if matches!(specialist, Some("spec-writer") | Some("coordinator")) {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn contains_all(haystack: &[&str], needles: &[&str]) -> bool {
        needles.iter().all(|n| haystack.contains(n))
    }

    #[test]
    fn spec_writer_removes_file_write_and_subagents() {
        let tools = get_tools_to_remove(Some("spec-writer"), "interactive");
        assert!(contains_all(&tools, FILE_WRITE_TOOLS));
        assert!(contains_all(&tools, SUBAGENT_TOOLS));
        assert!(contains_all(&tools, CONFLICTING_BUILTIN_TOOLS));
        // Reference code does NOT include EXECUTION_TOOLS for spec-writer.
        assert!(!tools.contains(&"launch-process"));
    }

    #[test]
    fn coordinator_alias_matches_spec_writer() {
        let a = get_tools_to_remove(Some("spec-writer"), "interactive");
        let b = get_tools_to_remove(Some("coordinator"), "interactive");
        assert_eq!(a, b);
    }

    #[test]
    fn background_task_loop_gets_subagent_block_plus_conflicts() {
        let tools = get_tools_to_remove(None, "task-loop");
        assert!(contains_all(&tools, SUBAGENT_TOOLS));
        assert!(contains_all(&tools, CONFLICTING_BUILTIN_TOOLS));
        // task-loop denylist is SUBAGENT_TOOLS only — no file-write.
        assert!(!tools.contains(&"str-replace-editor"));
    }

    #[test]
    fn background_pure_text_agent_gets_full_denylist() {
        let tools = get_tools_to_remove(None, "commit-message");
        assert!(contains_all(&tools, FILE_WRITE_TOOLS));
        assert!(contains_all(&tools, EXECUTION_TOOLS));
        assert!(contains_all(&tools, SUBAGENT_TOOLS));
        assert!(contains_all(&tools, CONFLICTING_BUILTIN_TOOLS));
    }

    #[test]
    fn interactive_agent_gets_global_subagent_block() {
        let tools = get_tools_to_remove(None, "interactive");
        assert!(contains_all(&tools, SUBAGENT_TOOLS));
        assert!(contains_all(&tools, CONFLICTING_BUILTIN_TOOLS));
        // Non-restricted agents keep file-write, execution, etc.
        assert!(!tools.contains(&"str-replace-editor"));
        assert!(!tools.contains(&"launch-process"));
    }

    #[test]
    fn implementor_specialist_gets_global_only() {
        // Non-coordinator specialists take the global fallback path.
        let tools = get_tools_to_remove(Some("implementor"), "interactive");
        assert!(contains_all(&tools, SUBAGENT_TOOLS));
        assert!(!tools.contains(&"str-replace-editor"));
    }

    #[test]
    fn specialist_beats_background_agent_type() {
        // spec-writer running as a background task-loop still gets the
        // spec-writer restrictions (specialist check runs first).
        let tools = get_tools_to_remove(Some("spec-writer"), "task-loop");
        assert!(contains_all(&tools, FILE_WRITE_TOOLS));
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
