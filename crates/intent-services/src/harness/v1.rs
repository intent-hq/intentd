//! Harness **v1**: today's post-#2457 text set, byte-pinned by
//! `crate::v1_goldens` and `agent_manager::v1_turn_envelope_goldens`. Every
//! string here was moved verbatim from `rules.rs` / `agent_manager.rs` /
//! `lib.rs` / `agent_ops.rs` (H5 byte-neutral refactor); any edit MUST fail
//! the goldens and force a deliberate new-version decision instead.

use super::{Harness, TurnEnvelopeParams};

/// The v1 harness singleton.
pub(crate) struct V1;

/// The `\n\n---\n\n` separator every assembled-prompt layer is joined with.
const LAYER_SEPARATOR: &str = "\n\n---\n\n";

/// Fallback phrasing for the workspace-naming nudge when the provider's MCP
/// tool naming convention is unknown (or its workspace-MCP wiring hasn't
/// landed yet).
pub(crate) const GENERIC_NAMING_TOOL_REFERENCE: &str =
    "the `set_workspace_title` tool from the workspace MCP server";

impl Harness for V1 {
    fn join_prompt_layers(&self, parts: &[String]) -> String {
        parts.join(LAYER_SEPARATOR)
    }

    fn user_rules_wrapper(&self, content: &str, source: &str) -> String {
        format!(
            "## User Rules & Guidelines\n\nThe following rules and guidelines have been configured for this project. Please follow these conventions and best practices:\n\n```\n{content}\n```\n\nThese rules are loaded from: {source}"
        )
    }

    fn rtk_instruction_line(&self, subcommands: &[String]) -> String {
        format!(
            "Prefix these commands with rtk for compressed, LLM-friendly output: {}",
            subcommands.join(", ")
        )
    }

    fn sandboxed_implementor_hint(&self, sandbox_path: &str, sandbox_branch: &str) -> String {
        // Base commit isn't on AgentSession; it is tracked in the sandbox
        // record, so the hint names that instead of a SHA.
        let base_sha_note = "base commit tracked in sandbox metadata";
        format!(
            "## Workspace Isolation\n\n\
             You are working in an **isolated CoW (copy-on-write) sandbox** at `{sandbox_path}` \
             on branch `{sandbox_branch}` ({base_sha_note}). Your dependency caches (node_modules, \
             target/, .venv, etc.) are warm — you inherited them from the canonical workspace.\n\n\
             **Critical constraints:**\n\
             - Do NOT switch branches or checkout other refs in your sandbox.\n\
             - On completion, the system automatically merges your branch back to the canonical workspace.\n\
             - If your changes conflict with canonical, you will be **woken with the conflicting paths** \
             and a ref to reconcile against. When that happens, resolve the conflicts **in your sandbox only** \
             (rebase or merge onto the fetched canonical ref), then end your turn again. The system will \
             retry the merge. Do NOT attempt to touch other checkouts or the canonical workspace directly.\n\
             - You have up to 2 conflict-resolution attempts before the merge is deferred to manual intervention."
        )
    }

    fn coordinator_cow_hint(&self) -> String {
        "## Agent Delegation & Isolation\n\n\
         Delegated agents in this workspace run in **isolated CoW sandboxes** when you \
         use `isolation: \"cow\"` (or when the workspace's `cowIsolation` setting defaults it). \
         Each sandboxed agent works in its own copy-on-write clone of the workspace directory, \
         so parallel delegation is safe even when tasks touch overlapping files — agents cannot \
         stomp each other's work.\n\n\
         **Merge-back is automatic:** when a sandboxed agent completes, the system merges its \
         commits back into the canonical workspace **before** waking you. Clean merges propagate \
         completion normally. Conflicts suppress completion propagation and wake the agent (not you) \
         with conflict paths and resolution instructions; the agent fixes its sandbox and retries \
         the merge (up to 2 attempts).\n\n\
         **You only handle `blocked` outcomes:** if the canonical workspace has uncommitted changes \
         overlapping with the agent's work, or if conflict retries are exhausted, completion propagates \
         with `merge_pending` status. Use `sandbox.cow.merge` or `sandbox.cow.discard` RPCs, or ask the user \
         to commit/stash their WIP, then manually merge."
            .to_string()
    }

    fn specialist_role_section(&self, behavior_prompt: &str) -> String {
        format!(
            "# Your Specialist Role\n\n<specialist_role>\n{behavior_prompt}\n</specialist_role>\n\n\
             The instructions in <specialist_role> define your primary function. \
             Prioritize them above general guidance."
        )
    }

    fn commit_policy_clause(&self) -> String {
        "## Commit Policy\n\n\
         Commit through `ws.git.commit` — never run `git commit` yourself \
         unless the user explicitly asks for a git workflow that \
         `ws.git.commit` cannot express (e.g. multiple scoped commits on a \
         branch). You may commit when it makes sense for the work; the system \
         may also automatically commit any remaining changes when your turn \
         ends."
            .to_string()
    }

    fn role_reminder_footer(&self, name: &str, reminder: Option<&str>) -> String {
        let reminder = reminder.unwrap_or("Follow the instructions in <specialist_role> above.");
        format!("## Role Reminder\n\nYou are a {name}. {reminder}")
    }

    fn ask_questions_block(&self) -> String {
        "## Asking the User Questions\n\n\
         When requirements are ambiguous or a decision needs user input, ask \
         structured clarifying questions with `ws.app.question.ask` via the \
         `workspace_api` tool instead of burying questions in prose. Call it once \
         per question with 2-4 options; do not add an \"Other\" option — a \
         free-form answer is always offered automatically. Ask all your \
         questions, then end the turn: questions are presented when your turn \
         ends, and the answers arrive in the next user message."
            .to_string()
    }

    fn suggested_next_steps_block(&self, effective_auto_commit: bool) -> String {
        let example_second_line = if effective_auto_commit {
            "Check the changes in the diff view."
        } else {
            "Review changes before committing."
        };
        let auto_commit_clause = if effective_auto_commit {
            " Auto-commit is enabled; do not include prompts about committing or reviewing changes before committing."
        } else {
            ""
        };
        format!(
            "## Suggested Next Steps\n\n\
             At the end of your response, offer the user clear next actions as a \
             `<!-- suggested-prompts ... -->` HTML comment block:\n\n\
             ```\n\
             <!-- suggested-prompts\n\
             Run the tests to verify the implementation.\n\
             {example_second_line}\n\
             -->\n\
             ```\n\n\
             Write 2–4 prompts, each a short directive sentence phrased as \
             something the user might say next.{auto_commit_clause}"
        )
    }

    fn first_turn_prepend_block(&self, prompt: &str) -> String {
        format!("<system>\n{prompt}\n</system>")
    }

    fn snapshot_line(&self, json: &str) -> String {
        format!("current ws.agent.snapshot() => {json}")
    }

    fn naming_tool_reference(&self, provider_id: &str) -> &'static str {
        // Providers affix the MCP server name differently: auggie exposes
        // `<tool>_<server>` (trailing suffix), opencode exposes
        // `<server>_<tool>` (leading prefix; confirmed against captured
        // opencode 1.18.3 traffic). Every other provider gets the generic
        // fallback phrasing.
        match provider_id {
            "auggie" => "the `set_workspace_title_workspace-mcp` tool",
            "opencode" => "the `workspace-mcp_set_workspace_title` tool",
            _ => GENERIC_NAMING_TOOL_REFERENCE,
        }
    }

    fn naming_nudge(&self, tool_reference: &str) -> String {
        format!(
            "<system>\nThis workspace needs a title. As your first action, call {tool_reference} with a short 3\u{2013}5 word sentence-case title describing the task. This can be called in parallel with information-gathering.\n</system>"
        )
    }

    fn role_reminder_prefix(&self, name: &str, reminder: &str) -> String {
        format!("[Role Reminder: You are a {name}. {reminder}]")
    }

    fn compose_turn_prompt(&self, params: &TurnEnvelopeParams<'_>) -> String {
        // Inside-out layering, `\n\n` joins: body ← role reminder ← naming
        // nudge ← Context block ← snapshot line ← FirstTurnPrepend.
        let prompt_text = match params.role_reminder {
            Some(r) => format!("{r}\n\n{}", params.body),
            None => params.body.to_string(),
        };
        let prompt_text = match params.naming_nudge {
            Some(sys) => format!("{sys}\n\n{prompt_text}"),
            None => prompt_text,
        };
        let prompt_text = match params.stdin_context {
            Some(ctx) => format!("Context:\n{ctx}\n\n---\n\n{prompt_text}"),
            None => prompt_text,
        };
        let prompt_text = match params.snapshot_line {
            Some(line) => format!("{line}\n\n{prompt_text}"),
            None => prompt_text,
        };
        match params.first_turn_prepend {
            Some(sys) => format!("{sys}\n\n{prompt_text}"),
            None => prompt_text,
        }
    }
}
