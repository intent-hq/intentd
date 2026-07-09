//! Per-agent-type tool denylist (§18.4) — internal spawn-time enforcement.
//!
//! Direct port of `background-agent-tool-restrictions.ts`. The mapping is
//! **hardcoded** and applied internally while assembling each agent's tool set
//! on spawn (§6.8); it is intentionally NOT a wire method (there is no
//! `agent.getAvailableTools`). It uses a **denylist** (tools to remove) rather
//! than an allowlist, so new tools are denied by default for restricted agents.

/// File modification tools — agents with these can edit the codebase.
pub const FILE_WRITE_TOOLS: &[&str] = &[
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
pub const GIT_TOOLS: &[&str] = &["git_stage", "git_commit"];

/// Agent creation/delegation tools — agents with these can spawn/message agents.
pub const AGENT_CREATION_TOOLS: &[&str] = &[
    "create_agent",
    "delegate_task",
    "send_message_to_agent",
    "send_message_to_task_agent",
    "wake_or_create_task_agent",
    "report_to_parent",
];

/// Note + task + comment + primitive mutation tools.
pub const NOTE_WRITE_TOOLS: &[&str] = &[
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
pub const WORKSPACE_WRITE_TOOLS: &[&str] = &[
    "rename_space",
    "rename_agent",
    "set_workspace_title",
    "set_workspace_status_message",
];

/// Unified workspace JS API tool (bare + server-suffixed). It can perform any
/// workspace mutation, so pure-text background agents must deny it.
pub const UNIFIED_WORKSPACE_TOOLS: &[&str] = &["workspace_api", "workspace_api_workspace-mcp"];

/// Process/command execution tools.
pub const EXECUTION_TOOLS: &[&str] = &["launch-process", "execute_command"];

/// External communication tools.
pub const EXTERNAL_TOOLS: &[&str] = &["web-fetch", "web-search", "github-api"];

/// Subagent orchestration tools.
pub const SUBAGENT_TOOLS: &[&str] = &[
    "sub-agent",
    "sub-agent-explore",
    "sub-agent-plan",
    "sub-agent-code-review-local-analyzer",
];

/// Built-in tools that conflict with their workspace-MCP equivalents and are
/// always removed (the MCP versions integrate with the agent lifecycle).
pub const CONFLICTING_BUILTIN_TOOLS: &[&str] = &["create_agent"];

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
pub fn is_background_agent_type(agent_type: &str) -> bool {
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
pub fn background_agent_types() -> &'static [&'static str] {
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
