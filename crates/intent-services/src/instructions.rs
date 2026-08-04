//! Bundled built-in agent-type instructions and their composition (§18.1).
//!
//! PARITY NOTE: faithful port of
//! `~/src/intent/src/features/agent/instructions/index.ts`
//! (`getInstructionById` / `getInstructionWithCommon`, the `UTILITY_AGENTS` and
//! `NON_INTERACTIVE_BACKGROUND_AGENTS` sets, and the `fix`/`review`/`walkthrough`
//! aliases). The bundled bodies are the verbatim `instructions/*.ts` template
//! literals, extracted to `resources/agent-instructions/*.md` and compiled in via
//! [`include_str!`] (no file I/O, no env override needed — they ship with the
//! binary, mirroring the TS "bundled with code" rationale). Two intentd-only
//! addenda: `common.md` gains a "Raising Attention" section covering the
//! `ws.agent.requestDiscussion` / `ws.agent.reportBlocker` bindings, and a
//! "Waiting on External Conditions" section covering the `ws.hook.*` background
//! hooks — neither exists in the TS reference.
//!
//! The base-system-prompt bundled default (`getBaseInstruction`) is intentionally
//! **not** ported here — it is a separate prompt slot (§18.1 layer 1).
//!
//! Consequence for the spawn default: [`crate::agent_manager`]'s
//! `DEFAULT_AGENT_TYPE = "interactive"` is an unknown instruction id, so
//! `get_instruction_by_id` takes the `fallbackToWorkspace` branch and
//! [`get_instruction_with_common`] composes `common + workspace + workspace` (the
//! workspace body appears as both the prepended workspace layer and the resolved
//! "specific" instruction) — exactly as the reference does for unknown types.

use intent_core::settings_file::AgentFeaturesSettings;
use std::borrow::Cow;

macro_rules! instr {
    ($name:literal) => {
        include_str!(concat!("../resources/agent-instructions/", $name, ".md"))
    };
}

const CHAT: &str = instr!("chat");
const COMMON: &str = instr!("common");
const DEBUG: &str = instr!("debug");
const WORKSPACE: &str = instr!("workspace");
const TASK_BREAKDOWN: &str = instr!("task-breakdown");
const TASK_DEBUG: &str = instr!("task-debug");
const TASK_FOCUSED: &str = instr!("task-focused");
const TASK_LOOP: &str = instr!("task-loop");
const RALPH_LOOP: &str = instr!("ralph-loop");
const WORKSPACE_AGENT: &str = instr!("workspace-agent");
const NOTES_SYSTEM_GUIDE: &str = instr!("notes-system-guide");
const CODE_REVIEW: &str = instr!("code-review");
const CODE_WALKTHROUGH: &str = instr!("code-walkthrough");
const COMMIT_MESSAGE: &str = instr!("commit-message");
const PR_DESCRIPTION: &str = instr!("pr-description");

// `setup-script-generator` selects its body by host platform at build time,
// mirroring the reference's `process.platform === 'win32'` runtime switch.
#[cfg(windows)]
const SETUP_SCRIPT_GENERATOR: &str = instr!("setup-script-generator.powershell");
#[cfg(not(windows))]
const SETUP_SCRIPT_GENERATOR: &str = instr!("setup-script-generator.bash");

/// Utility agents that don't get the workspace instruction layer (port of
/// `UTILITY_AGENTS`).
fn is_utility_agent(agent_type: &str) -> bool {
    matches!(
        agent_type,
        "code-review" | "code-walkthrough" | "commit-message" | "pr-description"
    )
}

/// Truly non-interactive background agents that get **no** common layer (port of
/// `NON_INTERACTIVE_BACKGROUND_AGENTS`).
fn is_non_interactive_background(agent_type: &str) -> bool {
    matches!(
        agent_type,
        "commit-message" | "pr-description" | "code-review" | "code-walkthrough"
    )
}

/// Resolve a bundled instruction body by id (port of `getInstructionById`,
/// including the `fix`/`review`/`walkthrough` aliases). Unknown ids return
/// `Some(workspace)` when `fallback_to_workspace`, else `None` (the reference
/// throws in that case).
fn get_instruction_by_id(id: &str, fallback_to_workspace: bool) -> Option<&'static str> {
    let found = match id {
        "chat" => CHAT,
        "common" => COMMON,
        "debug" => DEBUG,
        "workspace" => WORKSPACE,
        "setup-script-generator" => SETUP_SCRIPT_GENERATOR,
        "task-breakdown" => TASK_BREAKDOWN,
        "task-debug" => TASK_DEBUG,
        "task-focused" => TASK_FOCUSED,
        "task-loop" => TASK_LOOP,
        "ralph-loop" => RALPH_LOOP,
        "workspace-agent" => WORKSPACE_AGENT,
        "notes-system-guide" => NOTES_SYSTEM_GUIDE,
        "code-review" => CODE_REVIEW,
        "code-walkthrough" => CODE_WALKTHROUGH,
        "commit-message" => COMMIT_MESSAGE,
        "pr-description" => PR_DESCRIPTION,
        // Aliases for common agent types.
        "fix" => DEBUG,
        "review" => CODE_REVIEW,
        "walkthrough" => CODE_WALKTHROUGH,
        _ => {
            return if fallback_to_workspace {
                Some(WORKSPACE)
            } else {
                None
            };
        }
    };
    Some(found)
}

/// Remove one `## <heading>` markdown section: the heading line through the
/// text just before the next `## ` heading (or end of string). No-op when the
/// heading is absent.
fn remove_section(text: &str, heading: &str) -> String {
    let marker = format!("## {heading}");
    let Some(start) = text.find(&marker) else {
        return text.to_string();
    };
    let after = start + marker.len();
    match text[after..].find("\n## ") {
        Some(i) => {
            // Keep the newline so the next heading still starts on its own line;
            // the blank line before the removed heading becomes its separator.
            let end = after + i + 1;
            format!("{}{}", &text[..start], &text[end..])
        }
        None => {
            // Section runs to end of string: trim the now-dangling blank lines
            // so the body keeps the bundled no-trailing-newline convention.
            text[..start].trim_end_matches('\n').to_string()
        }
    }
}

/// The `common` body with feature-gated sections omitted (spec audit rows 1,
/// 7, and 8): `backgroundHooks` gates "Waiting on External Conditions",
/// `richChatBlocks` gates "Rich Chat Rendering", `attentionRequests` gates
/// "Raising Attention". With all defaults on this borrows the bundled body
/// untouched (byte-identical prompts).
fn gated_common(features: &AgentFeaturesSettings) -> Cow<'static, str> {
    let mut body = Cow::Borrowed(COMMON);
    if !features.attention_requests {
        body = Cow::Owned(remove_section(&body, "Raising Attention"));
    }
    if !features.background_hooks {
        body = Cow::Owned(remove_section(&body, "Waiting on External Conditions"));
    }
    if !features.rich_chat_blocks {
        body = Cow::Owned(remove_section(&body, "Rich Chat Rendering"));
    }
    body
}

/// The dev-server guideline in `workspace-agent.md` gated by
/// `agentFeatures.scripts` (spec audit row 3).
const WORKSPACE_AGENT_SCRIPTS_GUIDELINE: &str =
    "8. **Use script tools for dev servers** - Always use `ws.script.list()`, `ws.script.create(name, command, mode, opts?)`, and `ws.script.start(scriptId)` via the `workspace_api` tool instead of terminal/launch-process for dev servers, watchers, and long-running processes\n";

/// The `workspace-agent` body with feature-gated content omitted: `scripts`
/// gates guideline 8 ("Use script tools for dev servers"); when scripts stay
/// on but `terminalAccess` is off, the guideline's incidental terminal mention
/// is dropped. With all defaults on this borrows the bundled body untouched.
fn gated_workspace_agent(features: &AgentFeaturesSettings) -> Cow<'static, str> {
    if !features.scripts {
        Cow::Owned(WORKSPACE_AGENT.replacen(WORKSPACE_AGENT_SCRIPTS_GUIDELINE, "", 1))
    } else if !features.terminal_access {
        Cow::Owned(WORKSPACE_AGENT.replacen(
            "instead of terminal/launch-process",
            "instead of launch-process",
            1,
        ))
    } else {
        Cow::Borrowed(WORKSPACE_AGENT)
    }
}

/// Compose the bundled specialization rules for `agent_type` (port of
/// `getInstructionWithCommon`): common → workspace → specific, with the
/// `common`/`workspace`/utility/non-interactive special cases. Unknown ids fall
/// back to the `workspace` body via [`get_instruction_by_id`]. Feature-gated
/// sections (per `[agentFeatures]`, captured at session creation) are omitted
/// from the bundled bodies; with all defaults on the output is byte-identical
/// to the ungated composition.
pub(crate) fn get_instruction_with_common(
    agent_type: &str,
    features: &AgentFeaturesSettings,
) -> String {
    let specific: Cow<'static, str> = if agent_type == "workspace-agent" {
        gated_workspace_agent(features)
    } else {
        Cow::Borrowed(get_instruction_by_id(agent_type, true).unwrap_or(WORKSPACE))
    };
    if agent_type == "common" {
        return gated_common(features).into_owned();
    }
    let common = gated_common(features);
    if agent_type == "workspace" {
        return format!("{common}\n\n---\n\n{specific}");
    }
    if is_non_interactive_background(agent_type) {
        return specific.into_owned();
    }
    if is_utility_agent(agent_type) {
        return format!("{common}\n\n---\n\n{specific}");
    }
    format!("{common}\n\n---\n\n{WORKSPACE}\n\n---\n\n{specific}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn defaults() -> AgentFeaturesSettings {
        AgentFeaturesSettings::default()
    }

    #[test]
    fn common_only_is_not_self_wrapped() {
        assert_eq!(get_instruction_with_common("common", &defaults()), COMMON);
    }

    #[test]
    fn workspace_gets_common_prepended_once() {
        let out = get_instruction_with_common("workspace", &defaults());
        assert_eq!(out, format!("{COMMON}\n\n---\n\n{WORKSPACE}"));
    }

    #[test]
    fn non_interactive_background_has_no_common() {
        let out = get_instruction_with_common("commit-message", &defaults());
        assert_eq!(out, COMMIT_MESSAGE);
        assert!(!out.contains("## Delegating Tasks"));
    }

    #[test]
    fn all_background_utility_agents_get_specific_only() {
        // PARITY NOTE: `UTILITY_AGENTS` and `NON_INTERACTIVE_BACKGROUND_AGENTS`
        // hold the same four ids, and the non-interactive check runs first — so
        // every utility agent takes the specific-only branch and the
        // common-then-specific utility branch is unreachable (preserved for
        // faithful parity with the reference).
        assert_eq!(
            get_instruction_with_common("code-review", &defaults()),
            CODE_REVIEW
        );
        assert_eq!(
            get_instruction_with_common("code-walkthrough", &defaults()),
            CODE_WALKTHROUGH
        );
        assert_eq!(
            get_instruction_with_common("commit-message", &defaults()),
            COMMIT_MESSAGE
        );
        assert_eq!(
            get_instruction_with_common("pr-description", &defaults()),
            PR_DESCRIPTION
        );
    }

    #[test]
    fn workspace_agent_gets_common_workspace_specific() {
        let out = get_instruction_with_common("task-loop", &defaults());
        assert_eq!(
            out,
            format!("{COMMON}\n\n---\n\n{WORKSPACE}\n\n---\n\n{TASK_LOOP}")
        );
    }

    #[test]
    fn unknown_type_falls_back_to_workspace_specific() {
        // The spawn default agent type is unknown → fallbackToWorkspace.
        let out = get_instruction_with_common("interactive", &defaults());
        assert_eq!(
            out,
            format!("{COMMON}\n\n---\n\n{WORKSPACE}\n\n---\n\n{WORKSPACE}")
        );
    }

    #[test]
    fn fix_alias_resolves_to_debug() {
        let out = get_instruction_with_common("fix", &defaults());
        assert_eq!(
            out,
            format!("{COMMON}\n\n---\n\n{WORKSPACE}\n\n---\n\n{DEBUG}")
        );
    }

    #[test]
    fn review_and_walkthrough_aliases_resolve_bodies_but_keep_raw_id_semantics() {
        // The alias resolves to the utility *body*, but the utility/non-interactive
        // membership checks use the raw id ("review"/"walkthrough"), which are not
        // in those sets — so they take the full common+workspace+specific path.
        assert_eq!(
            get_instruction_with_common("review", &defaults()),
            format!("{COMMON}\n\n---\n\n{WORKSPACE}\n\n---\n\n{CODE_REVIEW}")
        );
        assert_eq!(
            get_instruction_with_common("walkthrough", &defaults()),
            format!("{COMMON}\n\n---\n\n{WORKSPACE}\n\n---\n\n{CODE_WALKTHROUGH}")
        );
    }

    // --- [agentFeatures] prompt gating ---

    #[test]
    fn defaults_keep_gated_bodies_borrowed_and_byte_identical() {
        let d = defaults();
        assert!(matches!(gated_common(&d), Cow::Borrowed(_)));
        assert!(matches!(gated_workspace_agent(&d), Cow::Borrowed(_)));
        assert_eq!(gated_common(&d).as_ref(), COMMON);
        assert_eq!(gated_workspace_agent(&d).as_ref(), WORKSPACE_AGENT);
    }

    #[test]
    fn background_hooks_off_removes_only_waiting_section() {
        let features = AgentFeaturesSettings {
            background_hooks: false,
            ..defaults()
        };
        let out = get_instruction_with_common("task-loop", &features);
        assert!(!out.contains("## Waiting on External Conditions"));
        assert!(!out.contains("ws.hook."));
        // Neighboring sections survive intact, with clean separation.
        assert!(out.contains("## Raising Attention"));
        assert!(out.contains("progressing work.\n\n## Response Organization"));
        assert!(out.contains("## Rich Chat Rendering"));
    }

    #[test]
    fn rich_chat_blocks_off_removes_trailing_rendering_section() {
        let features = AgentFeaturesSettings {
            rich_chat_blocks: false,
            ..defaults()
        };
        let common = gated_common(&features);
        assert!(!common.contains("## Rich Chat Rendering"));
        assert!(!common.contains("mermaid"));
        // The section is last in common.md: the body ends cleanly after the
        // previous section, and composition keeps the layer separator intact.
        assert!(common.ends_with("work as closing tags."));
        let out = get_instruction_with_common("task-loop", &features);
        assert!(out.contains("work as closing tags.\n\n---\n\n"));
        assert!(out.contains("## Waiting on External Conditions"));
    }

    #[test]
    fn attention_requests_off_removes_only_raising_attention_section() {
        let features = AgentFeaturesSettings {
            attention_requests: false,
            ..defaults()
        };
        let out = get_instruction_with_common("task-loop", &features);
        assert!(!out.contains("## Raising Attention"));
        assert!(!out.contains("ws.agent.reportBlocker"));
        assert!(!out.contains("ws.agent.requestDiscussion"));
        // Neighboring sections survive intact, with clean separation.
        assert!(out.contains("## Note Editing"));
        assert!(out.contains("(which replaces everything).\n\n## Waiting on External Conditions"));
        // The ungated reportToParent guidance elsewhere in common survives.
        assert!(out.contains("ws.agent.reportToParent"));
    }

    #[test]
    fn all_common_gates_off_removes_all_gated_sections() {
        let features = AgentFeaturesSettings {
            background_hooks: false,
            rich_chat_blocks: false,
            attention_requests: false,
            ..defaults()
        };
        let common = gated_common(&features);
        assert!(!common.contains("## Waiting on External Conditions"));
        assert!(!common.contains("## Rich Chat Rendering"));
        assert!(!common.contains("## Raising Attention"));
        assert!(common.contains("## Delegating Tasks"));
        assert!(common.contains("## Note Editing"));
        assert!(common.contains("## Response Organization"));
    }

    #[test]
    fn scripts_off_removes_workspace_agent_dev_server_guideline() {
        // Guard: the gated guideline text still matches the bundled body.
        assert!(WORKSPACE_AGENT.contains(WORKSPACE_AGENT_SCRIPTS_GUIDELINE));
        let features = AgentFeaturesSettings {
            scripts: false,
            ..defaults()
        };
        let out = get_instruction_with_common("workspace-agent", &features);
        assert!(!out.contains("Use script tools for dev servers"));
        assert!(!out.contains("ws.script."));
        // Neighboring guidelines survive.
        assert!(out.contains("7. **Use notes**"));
    }

    #[test]
    fn terminal_access_off_rewrites_dev_server_guideline_mention() {
        let features = AgentFeaturesSettings {
            terminal_access: false,
            ..defaults()
        };
        let out = get_instruction_with_common("workspace-agent", &features);
        assert!(out.contains("Use script tools for dev servers"));
        assert!(!out.contains("instead of terminal/launch-process"));
        assert!(out.contains("instead of launch-process"));
    }

    #[test]
    fn scripts_off_wins_over_terminal_rewrite() {
        let features = AgentFeaturesSettings {
            scripts: false,
            terminal_access: false,
            ..defaults()
        };
        let out = get_instruction_with_common("workspace-agent", &features);
        assert!(!out.contains("Use script tools for dev servers"));
    }

    #[test]
    fn gating_leaves_other_agent_bodies_untouched() {
        let features = AgentFeaturesSettings {
            background_hooks: false,
            scripts: false,
            terminal_access: false,
            rich_chat_blocks: false,
            structured_questions: false,
            ..defaults()
        };
        // Non-interactive background agents have no common layer and no gated
        // sections of their own.
        assert_eq!(
            get_instruction_with_common("commit-message", &features),
            COMMIT_MESSAGE
        );
    }

    #[test]
    fn remove_section_is_noop_for_missing_heading() {
        assert_eq!(remove_section(COMMON, "No Such Section"), COMMON);
    }
}
