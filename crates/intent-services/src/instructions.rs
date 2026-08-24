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
    ($ver:literal, $name:literal) => {
        include_str!(concat!(
            "../resources/agent-instructions/",
            $ver,
            "/",
            $name,
            ".md"
        ))
    };
}

const CHAT: &str = instr!("v1", "chat");
const COMMON: &str = instr!("v1", "common");
const DEBUG: &str = instr!("v1", "debug");
const WORKSPACE: &str = instr!("v1", "workspace");
const TASK_BREAKDOWN: &str = instr!("v1", "task-breakdown");
const TASK_DEBUG: &str = instr!("v1", "task-debug");
const TASK_FOCUSED: &str = instr!("v1", "task-focused");
const TASK_LOOP: &str = instr!("v1", "task-loop");
const RALPH_LOOP: &str = instr!("v1", "ralph-loop");
const WORKSPACE_AGENT: &str = instr!("v1", "workspace-agent");
const NOTES_SYSTEM_GUIDE: &str = instr!("v1", "notes-system-guide");
const CODE_REVIEW: &str = instr!("v1", "code-review");
const CODE_WALKTHROUGH: &str = instr!("v1", "code-walkthrough");
const COMMIT_MESSAGE: &str = instr!("v1", "commit-message");
const PR_DESCRIPTION: &str = instr!("v1", "pr-description");

// `setup-script-generator` selects its body by host platform at build time,
// mirroring the reference's `process.platform === 'win32'` runtime switch.
#[cfg(windows)]
const SETUP_SCRIPT_GENERATOR: &str = instr!("v1", "setup-script-generator.powershell");
#[cfg(not(windows))]
const SETUP_SCRIPT_GENERATOR: &str = instr!("v1", "setup-script-generator.bash");

/// One harness version's bundled instruction set: every body the
/// [`get_instruction_with_common_for`] composition can resolve. Each version
/// keeps its markdown under `resources/agent-instructions/<ver>/` and exposes
/// a static like [`V1`]; the version's `harness::HarnessEntry` doctrine points
/// at it, so prompt assembly resolves the SESSION's pinned set instead of the
/// live latest. Adding a version = a new directory + a new static + a
/// registry entry (H2, intent-hq/monorepo#2459).
pub(crate) struct InstructionSet {
    pub chat: &'static str,
    pub common: &'static str,
    pub debug: &'static str,
    pub workspace: &'static str,
    pub setup_script_generator: &'static str,
    pub task_breakdown: &'static str,
    pub task_debug: &'static str,
    pub task_focused: &'static str,
    pub task_loop: &'static str,
    pub ralph_loop: &'static str,
    pub workspace_agent: &'static str,
    pub notes_system_guide: &'static str,
    pub code_review: &'static str,
    pub code_walkthrough: &'static str,
    pub commit_message: &'static str,
    pub pr_description: &'static str,
}

/// The v1 instruction set (`resources/agent-instructions/v1/`): today's text,
/// byte-identical to the pre-versioned layout (pinned by
/// `v1_goldens::golden_bundled_doctrine_hashes`).
pub(crate) static V1: InstructionSet = InstructionSet {
    chat: CHAT,
    common: COMMON,
    debug: DEBUG,
    workspace: WORKSPACE,
    setup_script_generator: SETUP_SCRIPT_GENERATOR,
    task_breakdown: TASK_BREAKDOWN,
    task_debug: TASK_DEBUG,
    task_focused: TASK_FOCUSED,
    task_loop: TASK_LOOP,
    ralph_loop: RALPH_LOOP,
    workspace_agent: WORKSPACE_AGENT,
    notes_system_guide: NOTES_SYSTEM_GUIDE,
    code_review: CODE_REVIEW,
    code_walkthrough: CODE_WALKTHROUGH,
    commit_message: COMMIT_MESSAGE,
    pr_description: PR_DESCRIPTION,
};

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

/// Resolve a bundled instruction body by id from `set` (port of
/// `getInstructionById`, including the `fix`/`review`/`walkthrough` aliases).
/// Unknown ids return `Some(set.workspace)` when `fallback_to_workspace`,
/// else `None` (the reference throws in that case).
fn get_instruction_by_id(
    set: &'static InstructionSet,
    id: &str,
    fallback_to_workspace: bool,
) -> Option<&'static str> {
    let found = match id {
        "chat" => set.chat,
        "common" => set.common,
        "debug" | "fix" => set.debug,
        "workspace" => set.workspace,
        "setup-script-generator" => set.setup_script_generator,
        "task-breakdown" => set.task_breakdown,
        "task-debug" => set.task_debug,
        "task-focused" => set.task_focused,
        "task-loop" => set.task_loop,
        "ralph-loop" => set.ralph_loop,
        "workspace-agent" => set.workspace_agent,
        "notes-system-guide" => set.notes_system_guide,
        "code-review" | "review" => set.code_review,
        "code-walkthrough" | "walkthrough" => set.code_walkthrough,
        "commit-message" => set.commit_message,
        "pr-description" => set.pr_description,
        // Aliases for common agent types.
        _ => {
            return if fallback_to_workspace {
                Some(set.workspace)
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

/// The `ws.host.exec` fallback sentence of the "Cross-repo PRs" bullet in
/// common.md's "Waiting on External Conditions" section: its `gh api` recipe
/// only works when `agentFeatures.hostExec` is on, so it is scrubbed when the
/// toggle is off — the bullet itself survives, since its primary
/// `ws.pr.snapshot({ repo })` path needs no host exec (a unit test guards
/// that this text still matches the bundled body verbatim).
const WAITING_HOST_EXEC_SENTENCE: &str = " For fields the snapshot does not carry, run `gh api repos/{owner}/{repo}/pulls/{n}` via `ws.host.exec` instead.";

/// Start/end markers of common.md's "Task relations during delegation"
/// subsection — the advisory task-graph teaching (intent-hq/monorepo#2457,
/// reworked from the intentd#1116/#1147 batch-delegation doctrine) — removed
/// when `agentFeatures.taskGraph` is off (intent-hq/monorepo#2445). It is an
/// H3 inside "## Delegating Tasks" followed by plain section text, so
/// [`remove_section`]'s H2 scan cannot express it; instead the text from the
/// heading through the blank line before the "Keep delegated tasks visible"
/// paragraph is cut by marker (unit tests guard both markers verbatim).
const COMMON_TASK_RELATIONS_START: &str = "### Task relations during delegation";
const COMMON_TASK_RELATIONS_END: &str = "Keep delegated tasks visible in the note";

/// Remove the text between `start` (inclusive) and `end` (exclusive) markers.
/// No-op unless both markers are present in order.
fn remove_between(text: &str, start: &str, end: &str) -> String {
    let Some(s) = text.find(start) else {
        return text.to_string();
    };
    match text[s..].find(end) {
        Some(e) => format!("{}{}", &text[..s], &text[s + e..]),
        None => text.to_string(),
    }
}

/// The `common` body with feature-gated sections omitted (spec audit rows 1,
/// 7, and 8): `backgroundHooks` gates "Waiting on External Conditions",
/// `richChatBlocks` gates "Rich Chat Rendering", `attentionRequests` gates
/// "Raising Attention", and `taskGraph` gates the "Task relations during
/// delegation" subsection (dispositions, `unlockPlan`, unblocked-wake
/// guidance). When `hostExec` is off but the Waiting section survives, the
/// `ws.host.exec` fallback sentence of its "Cross-repo PRs" bullet is
/// scrubbed. With every gate open (the defaults) this borrows the
/// bundled body untouched (byte-identical prompts). The markers are pinned to
/// the v1 text by unit tests; on a future set where one is absent the gate is
/// a no-op (that version's gating adds its own markers).
fn gated_common(
    set: &'static InstructionSet,
    features: &AgentFeaturesSettings,
) -> Cow<'static, str> {
    let mut body = Cow::Borrowed(set.common);
    if !features.task_graph {
        body = Cow::Owned(remove_between(
            &body,
            COMMON_TASK_RELATIONS_START,
            COMMON_TASK_RELATIONS_END,
        ));
    }
    if !features.attention_requests {
        body = Cow::Owned(remove_section(&body, "Raising Attention"));
    }
    if !features.background_hooks {
        body = Cow::Owned(remove_section(&body, "Waiting on External Conditions"));
    } else if !features.host_exec {
        body = Cow::Owned(body.replacen(WAITING_HOST_EXEC_SENTENCE, "", 1));
    }
    if !features.rich_chat_blocks {
        body = Cow::Owned(remove_section(&body, "Rich Chat Rendering"));
    }
    body
}

/// Markers of the task-graph teaching in `task-breakdown.md` (from
/// intentd#1116/#1135), removed when `agentFeatures.taskGraph` is off
/// (intent-hq/monorepo#2445): the "Contract-First Splitting" and "Declaring
/// Task Relations" H3 blocks run back-to-back and end at the next H2, and
/// three inline mentions (the `dependsOn=` decomposition clause and the two
/// fence-attribute lines of the Document Update Format example, which sit in
/// a tab-indented block) are rewritten to their pre-attribute wording. Unit
/// tests guard every needle verbatim.
const BREAKDOWN_RELATIONS_START: &str = "### Contract-First Splitting";
const BREAKDOWN_RELATIONS_END: &str = "## Breakdown Process";
const BREAKDOWN_DEPENDS_ON_CLAUSE: &str =
    " and declare the ordering as `dependsOn=` attributes on the task blocks";
const BREAKDOWN_EXAMPLE_HEADER: &str = "\tExample (two subtasks, ordered with an inline relation):";
const BREAKDOWN_EXAMPLE_HEADER_OFF: &str = "\tExample (two subtasks):";
const BREAKDOWN_EXAMPLE_KEY_LINE: &str = "\t@@@task key=research";
const BREAKDOWN_EXAMPLE_DEPENDS_LINE: &str = "\t@@@task dependsOn=research";
const BREAKDOWN_EXAMPLE_PLAIN_LINE: &str = "\t@@@task";

/// The `task-breakdown` body with the task-graph teaching omitted when
/// `agentFeatures.taskGraph` is off; borrows the bundled body untouched when
/// the toggle is on.
fn gated_task_breakdown(
    set: &'static InstructionSet,
    features: &AgentFeaturesSettings,
) -> Cow<'static, str> {
    if features.task_graph {
        return Cow::Borrowed(set.task_breakdown);
    }
    let body = remove_between(
        set.task_breakdown,
        BREAKDOWN_RELATIONS_START,
        BREAKDOWN_RELATIONS_END,
    )
    .replacen(BREAKDOWN_DEPENDS_ON_CLAUSE, "", 1)
    .replacen(BREAKDOWN_EXAMPLE_HEADER, BREAKDOWN_EXAMPLE_HEADER_OFF, 1)
    .replacen(BREAKDOWN_EXAMPLE_KEY_LINE, BREAKDOWN_EXAMPLE_PLAIN_LINE, 1)
    .replacen(
        BREAKDOWN_EXAMPLE_DEPENDS_LINE,
        BREAKDOWN_EXAMPLE_PLAIN_LINE,
        1,
    );
    Cow::Owned(body)
}

/// The dev-server guideline in `workspace-agent.md` gated by
/// `agentFeatures.scripts` (spec audit row 3).
const WORKSPACE_AGENT_SCRIPTS_GUIDELINE: &str =
    "8. **Use script tools for dev servers** - Always use `ws.script.list()`, `ws.script.create(name, command, mode, opts?)`, and `ws.script.start(scriptId)` via the `workspace_api` tool instead of terminal/launch-process for dev servers, watchers, and long-running processes\n";

/// The `workspace-agent` body with feature-gated content omitted: `scripts`
/// gates guideline 8 ("Use script tools for dev servers"); when scripts stay
/// on but `terminalAccess` is off, the guideline's incidental terminal mention
/// is dropped. With all defaults on this borrows the bundled body untouched.
fn gated_workspace_agent(
    set: &'static InstructionSet,
    features: &AgentFeaturesSettings,
) -> Cow<'static, str> {
    if !features.scripts {
        Cow::Owned(
            set.workspace_agent
                .replacen(WORKSPACE_AGENT_SCRIPTS_GUIDELINE, "", 1),
        )
    } else if !features.terminal_access {
        Cow::Owned(set.workspace_agent.replacen(
            "instead of terminal/launch-process",
            "instead of launch-process",
            1,
        ))
    } else {
        Cow::Borrowed(set.workspace_agent)
    }
}

/// [`get_instruction_with_common_for`] over the LATEST instruction set.
/// Test-only convenience (byte-pin goldens, unit tests): every production
/// caller has a session in scope and resolves the session's pinned set via
/// the harness registry instead (prompt assembly, and the commit-message
/// background body in `auto_commit.rs`).
#[cfg(test)]
pub(crate) fn get_instruction_with_common(
    agent_type: &str,
    features: &AgentFeaturesSettings,
) -> String {
    get_instruction_with_common_for(
        crate::harness::latest_entry().doctrine.instructions,
        agent_type,
        features,
    )
}

/// Compose the bundled specialization rules for `agent_type` from `set` (port
/// of `getInstructionWithCommon`): common → workspace → specific, with the
/// `common`/`workspace`/utility/non-interactive special cases. Unknown ids fall
/// back to the `workspace` body via [`get_instruction_by_id`]. Feature-gated
/// sections (per `[agentFeatures]`, captured at session creation) are omitted
/// from the bundled bodies; with all defaults on the output is byte-identical
/// to the ungated composition.
pub(crate) fn get_instruction_with_common_for(
    set: &'static InstructionSet,
    agent_type: &str,
    features: &AgentFeaturesSettings,
) -> String {
    let specific: Cow<'static, str> = if agent_type == "workspace-agent" {
        gated_workspace_agent(set, features)
    } else if agent_type == "task-breakdown" {
        gated_task_breakdown(set, features)
    } else {
        Cow::Borrowed(get_instruction_by_id(set, agent_type, true).unwrap_or(set.workspace))
    };
    if agent_type == "common" {
        return gated_common(set, features).into_owned();
    }
    let common = gated_common(set, features);
    if agent_type == "workspace" {
        return format!("{common}\n\n---\n\n{specific}");
    }
    if is_non_interactive_background(agent_type) {
        return specific.into_owned();
    }
    if is_utility_agent(agent_type) {
        return format!("{common}\n\n---\n\n{specific}");
    }
    let workspace = set.workspace;
    format!("{common}\n\n---\n\n{workspace}\n\n---\n\n{specific}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn defaults() -> AgentFeaturesSettings {
        AgentFeaturesSettings::default()
    }

    /// Every gate open: the defaults (all toggles on, `taskGraph` included
    /// since the default flip), so gated bodies are byte-identical to the
    /// bundled sources.
    fn all_on() -> AgentFeaturesSettings {
        defaults()
    }

    #[test]
    fn common_only_is_not_self_wrapped() {
        assert_eq!(get_instruction_with_common("common", &all_on()), COMMON);
    }

    #[test]
    fn workspace_gets_common_prepended_once() {
        let out = get_instruction_with_common("workspace", &all_on());
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
        let out = get_instruction_with_common("task-loop", &all_on());
        assert_eq!(
            out,
            format!("{COMMON}\n\n---\n\n{WORKSPACE}\n\n---\n\n{TASK_LOOP}")
        );
    }

    #[test]
    fn unknown_type_falls_back_to_workspace_specific() {
        // The spawn default agent type is unknown → fallbackToWorkspace.
        let out = get_instruction_with_common("interactive", &all_on());
        assert_eq!(
            out,
            format!("{COMMON}\n\n---\n\n{WORKSPACE}\n\n---\n\n{WORKSPACE}")
        );
    }

    #[test]
    fn fix_alias_resolves_to_debug() {
        let out = get_instruction_with_common("fix", &all_on());
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
            get_instruction_with_common("review", &all_on()),
            format!("{COMMON}\n\n---\n\n{WORKSPACE}\n\n---\n\n{CODE_REVIEW}")
        );
        assert_eq!(
            get_instruction_with_common("walkthrough", &all_on()),
            format!("{COMMON}\n\n---\n\n{WORKSPACE}\n\n---\n\n{CODE_WALKTHROUGH}")
        );
    }

    // --- [agentFeatures] prompt gating ---

    #[test]
    fn all_on_keeps_gated_bodies_borrowed_and_byte_identical() {
        let a = all_on();
        assert!(matches!(gated_common(&V1, &a), Cow::Borrowed(_)));
        assert!(matches!(gated_workspace_agent(&V1, &a), Cow::Borrowed(_)));
        assert!(matches!(gated_task_breakdown(&V1, &a), Cow::Borrowed(_)));
        assert_eq!(gated_common(&V1, &a).as_ref(), COMMON);
        assert_eq!(gated_workspace_agent(&V1, &a).as_ref(), WORKSPACE_AGENT);
        assert_eq!(gated_task_breakdown(&V1, &a).as_ref(), TASK_BREAKDOWN);
    }

    #[test]
    fn task_graph_off_removes_task_relations_from_common() {
        // Guards: the markers still match the bundled body verbatim.
        assert!(COMMON.contains(COMMON_TASK_RELATIONS_START));
        assert!(COMMON.contains(COMMON_TASK_RELATIONS_END));
        // `taskGraph` defaults on; opt out explicitly.
        let features = AgentFeaturesSettings {
            task_graph: false,
            ..defaults()
        };
        let common = gated_common(&V1, &features);
        assert!(!common.contains("### Task relations during delegation"));
        assert!(!common.contains("unlockPlan"));
        assert!(!common.contains("Tasks now unblocked"));
        // Single-task delegation guidance and the surrounding section survive
        // with a clean seam (the pre-teaching layout).
        assert!(common.contains("## Delegating Tasks"));
        assert!(common.contains(
            "waitMode: \"after_all\" })\n```\n\nKeep delegated tasks visible in the note"
        ));
    }

    #[test]
    fn task_graph_on_common_teaching_is_advisory_not_doctrine() {
        // Flag ON: the advisory subsection is present…
        let common = gated_common(&V1, &all_on());
        assert!(common.contains("### Task relations during delegation"));
        assert!(common.contains("**Holds are advisory, not final.**"));
        // …and none of the batch-delegation doctrine phrases remain
        // (intent-hq/monorepo#2445 regression, reworked per monorepo#2457).
        for gone in [
            "preferred for multi-task plans",
            "remaining list",
            "tasks: [\"t1\", \"t2\", \"t3\"]",
            "Re-call delegate",
            "re-call",
            "greedy",
        ] {
            assert!(!common.contains(gone), "doctrine phrase survived: {gone:?}");
        }
    }

    #[test]
    fn task_graph_off_removes_relations_teaching_from_task_breakdown() {
        // Guards: every needle still matches the bundled body verbatim.
        assert!(TASK_BREAKDOWN.contains(BREAKDOWN_RELATIONS_START));
        assert!(TASK_BREAKDOWN.contains(BREAKDOWN_RELATIONS_END));
        assert!(TASK_BREAKDOWN.contains(BREAKDOWN_DEPENDS_ON_CLAUSE));
        assert!(TASK_BREAKDOWN.contains(BREAKDOWN_EXAMPLE_HEADER));
        assert!(TASK_BREAKDOWN.contains(BREAKDOWN_EXAMPLE_KEY_LINE));
        assert!(TASK_BREAKDOWN.contains(BREAKDOWN_EXAMPLE_DEPENDS_LINE));
        let features = AgentFeaturesSettings {
            task_graph: false,
            ..defaults()
        };
        let body = gated_task_breakdown(&V1, &features);
        assert!(!body.contains("### Contract-First Splitting"));
        assert!(!body.contains("### Declaring Task Relations"));
        assert!(!body.contains("dependsOn"));
        assert!(!body.contains("conflictsWith"));
        assert!(!body.contains("key=research"));
        assert!(!body.contains("effort="));
        // Neighboring content survives with a clean seam, and the example
        // reverts to plain `@@@task` fences.
        assert!(body.contains("- \"Add single line comment\"\n\n## Breakdown Process"));
        assert!(body.contains("\tExample (two subtasks):"));
        assert!(body.contains("\t@@@task\n\t# Research existing patterns"));
        assert!(body.contains("\t@@@task\n\t# Implement validation"));
        assert!(body.contains("- Order subtasks by dependencies (what needs to happen first)\n"));
        // The ungated convertBlocks materialization rule survives.
        assert!(body.contains("ws.task.convertBlocks(\"spec\")"));
        // Composition routes task-breakdown through the gate.
        let out = get_instruction_with_common("task-breakdown", &features);
        assert!(!out.contains("### Declaring Task Relations"));
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
    fn host_exec_off_scrubs_gh_fallback_from_waiting_section() {
        // Guard: the gated sentence text still matches the bundled body.
        assert!(COMMON.contains(WAITING_HOST_EXEC_SENTENCE));
        let features = AgentFeaturesSettings {
            host_exec: false,
            ..defaults()
        };
        let common = gated_common(&V1, &features);
        assert!(common.contains("## Waiting on External Conditions"));
        assert!(!common.contains("ws.host.exec"));
        // The bullet survives, still leading with the snapshot override…
        assert!(common.contains("**Cross-repo PRs**"));
        assert!(common.contains("ws.pr.snapshot(prNumber, { repo: \"owner/name\" })"));
        // …and the scrub leaves a clean sentence boundary before the next bullet.
        assert!(common.contains("diff that snapshot against `hookState`.\n- **Hygiene**"));
    }

    #[test]
    fn rich_chat_blocks_off_removes_trailing_rendering_section() {
        let features = AgentFeaturesSettings {
            rich_chat_blocks: false,
            ..defaults()
        };
        let common = gated_common(&V1, &features);
        assert!(!common.contains("## Rich Chat Rendering"));
        assert!(!common.contains("mermaid"));
        assert!(!common.contains("intent://local/file/"));
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
        let common = gated_common(&V1, &features);
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
