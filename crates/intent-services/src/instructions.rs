//! Bundled built-in agent-type instructions and their composition (§18.1).
//!
//! PARITY NOTE: faithful port of
//! `~/src/intent/src/features/agent/instructions/index.ts`
//! (`getInstructionById` / `getInstructionWithCommon`, the `UTILITY_AGENTS` and
//! `NON_INTERACTIVE_BACKGROUND_AGENTS` sets, and the `fix`/`review`/`walkthrough`
//! aliases). The bundled bodies are the verbatim `instructions/*.ts` template
//! literals, extracted to `resources/agent-instructions/*.md` and compiled in via
//! [`include_str!`] (no file I/O, no env override needed — they ship with the
//! binary, mirroring the TS "bundled with code" rationale). One intentd-only
//! addendum: `common.md` gains a "Raising Attention" section covering the
//! `ws.agent.requestDiscussion` / `ws.agent.reportBlocker` bindings, which do
//! not exist in the TS reference.
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

/// Compose the bundled specialization rules for `agent_type` (port of
/// `getInstructionWithCommon`): common → workspace → specific, with the
/// `common`/`workspace`/utility/non-interactive special cases. Unknown ids fall
/// back to the `workspace` body via [`get_instruction_by_id`].
pub(crate) fn get_instruction_with_common(agent_type: &str) -> String {
    let specific = get_instruction_by_id(agent_type, true).unwrap_or(WORKSPACE);
    if agent_type == "common" {
        return specific.to_string();
    }
    if agent_type == "workspace" {
        return format!("{COMMON}\n\n---\n\n{specific}");
    }
    if is_non_interactive_background(agent_type) {
        return specific.to_string();
    }
    if is_utility_agent(agent_type) {
        return format!("{COMMON}\n\n---\n\n{specific}");
    }
    format!("{COMMON}\n\n---\n\n{WORKSPACE}\n\n---\n\n{specific}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn common_only_is_not_self_wrapped() {
        assert_eq!(get_instruction_with_common("common"), COMMON);
    }

    #[test]
    fn workspace_gets_common_prepended_once() {
        let out = get_instruction_with_common("workspace");
        assert_eq!(out, format!("{COMMON}\n\n---\n\n{WORKSPACE}"));
    }

    #[test]
    fn non_interactive_background_has_no_common() {
        let out = get_instruction_with_common("commit-message");
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
        assert_eq!(get_instruction_with_common("code-review"), CODE_REVIEW);
        assert_eq!(
            get_instruction_with_common("code-walkthrough"),
            CODE_WALKTHROUGH
        );
        assert_eq!(
            get_instruction_with_common("commit-message"),
            COMMIT_MESSAGE
        );
        assert_eq!(
            get_instruction_with_common("pr-description"),
            PR_DESCRIPTION
        );
    }

    #[test]
    fn workspace_agent_gets_common_workspace_specific() {
        let out = get_instruction_with_common("task-loop");
        assert_eq!(
            out,
            format!("{COMMON}\n\n---\n\n{WORKSPACE}\n\n---\n\n{TASK_LOOP}")
        );
    }

    #[test]
    fn unknown_type_falls_back_to_workspace_specific() {
        // The spawn default agent type is unknown → fallbackToWorkspace.
        let out = get_instruction_with_common("interactive");
        assert_eq!(
            out,
            format!("{COMMON}\n\n---\n\n{WORKSPACE}\n\n---\n\n{WORKSPACE}")
        );
    }

    #[test]
    fn fix_alias_resolves_to_debug() {
        let out = get_instruction_with_common("fix");
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
            get_instruction_with_common("review"),
            format!("{COMMON}\n\n---\n\n{WORKSPACE}\n\n---\n\n{CODE_REVIEW}")
        );
        assert_eq!(
            get_instruction_with_common("walkthrough"),
            format!("{COMMON}\n\n---\n\n{WORKSPACE}\n\n---\n\n{CODE_WALKTHROUGH}")
        );
    }
}
