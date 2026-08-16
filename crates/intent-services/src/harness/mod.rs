//! Versioned prompt **harness** (H5, intent-hq/monorepo#2459): the single
//! owner of every system-generated string that shapes an agent's system
//! prompt or per-turn prompt envelope.
//!
//! One trait, one module per version. [`Harness`] exposes a method per text
//! surface; each version implements it ([`v1`] = today's post-#2457 set,
//! byte-pinned by `crate::v1_goldens` and
//! `agent_manager::v1_turn_envelope_goldens`). Call sites carry typed data
//! into the harness and never format doctrine/envelope text themselves, so a
//! future version can reword or reorder surfaces without touching managers.
//! A new version starts as `pub use` re-exports of the prior version's
//! surface functions and overrides only what changed — the v(N)→v(N+1) diff
//! is exactly the changed surfaces.
//!
//! Wake/queue system messages (hook/PR-monitor/watch wakes, dequeue notes,
//! delegation preamble, …) migrate behind the harness separately (H6).

pub(crate) mod v1;

/// Typed inputs for [`Harness::compose_turn_prompt`]: the per-turn envelope
/// layers, outermost-first. Each optional layer is either raw data the
/// harness wraps itself (`stdin_context`) or a surface string already
/// rendered by this same harness (`first_turn_prepend` via
/// [`Harness::first_turn_prepend_block`], `snapshot_line` via
/// [`Harness::snapshot_line`], `naming_nudge` via [`Harness::naming_nudge`],
/// `role_reminder` via [`Harness::role_reminder_prefix`]) — the caller only
/// decides presence, never wording.
pub(crate) struct TurnEnvelopeParams<'a> {
    /// Fire-once `<system>`-wrapped assembled system prompt (§18.1 fallback).
    pub first_turn_prepend: Option<&'a str>,
    /// Recurring `current ws.agent.snapshot() => {json}` line.
    pub snapshot_line: Option<&'a str>,
    /// Raw stdin/context-reference text; the harness owns the `Context:`
    /// block shape around it.
    pub stdin_context: Option<&'a str>,
    /// Fire-once `<system>`-wrapped workspace-naming instruction.
    pub naming_nudge: Option<&'a str>,
    /// Per-turn `[Role Reminder: …]` prefix (specialist agents only).
    pub role_reminder: Option<&'a str>,
    /// The turn body: user content, possibly history-wrapped.
    pub body: &'a str,
}

/// One method per system-generated text surface. Implementations own 100% of
/// the wording; callers own gating/data resolution only.
pub(crate) trait Harness: Send + Sync {
    // --- System prompt assembly (`rules::assemble_system_prompt`) ---

    /// Join assembled prompt layers with the versioned separator.
    fn join_prompt_layers(&self, parts: &[String]) -> String;
    /// `## User Rules & Guidelines` wrapper around workspace rule-file /
    /// repo-config content.
    fn user_rules_wrapper(&self, content: &str, source: &str) -> String;
    /// RTK instruction line for the given usable subcommands.
    fn rtk_instruction_line(&self, subcommands: &[String]) -> String;
    /// `## Workspace Isolation` hint for a sandboxed implementor.
    fn sandboxed_implementor_hint(&self, sandbox_path: &str, sandbox_branch: &str) -> String;
    /// `## Agent Delegation & Isolation` hint for a coordinator in a
    /// CoW-enabled direct-mode workspace.
    fn coordinator_cow_hint(&self) -> String;
    /// `# Your Specialist Role` section wrapping the behavior prompt.
    fn specialist_role_section(&self, behavior_prompt: &str) -> String;
    /// The status-neutral `## Commit Policy` clause (every agent).
    fn commit_policy_clause(&self) -> String;
    /// `## Role Reminder` footer; `None` reminder uses the versioned default.
    fn role_reminder_footer(&self, name: &str, reminder: Option<&str>) -> String;
    /// `## Asking the User Questions` block (top-level agents,
    /// `structuredQuestions` on).
    fn ask_questions_block(&self) -> String;
    /// `## Suggested Next Steps` block; wording tracks the session's
    /// effective auto-commit state.
    fn suggested_next_steps_block(&self, effective_auto_commit: bool) -> String;

    // --- Turn envelope (`agent_manager::build_turn_prompt`) ---

    /// `<system>`-wrapped assembled system prompt for the FirstTurnPrepend
    /// fallback.
    fn first_turn_prepend_block(&self, prompt: &str) -> String;
    /// The per-turn state snapshot line around the serialized snapshot JSON.
    fn snapshot_line(&self, json: &str) -> String;
    /// Provider-correct spelling of the workspace-MCP rename tool for the
    /// naming nudge.
    fn naming_tool_reference(&self, provider_id: &str) -> &'static str;
    /// Fire-once `<system>` workspace-naming instruction.
    fn naming_nudge(&self, tool_reference: &str) -> String;
    /// Per-turn `[Role Reminder: You are a {name}. {reminder}]` prefix.
    fn role_reminder_prefix(&self, name: &str, reminder: &str) -> String;
    /// Compose the full outbound turn prompt: the layering order
    /// (FirstTurnPrepend → snapshot → Context → naming nudge → role reminder
    /// → body) is itself versioned.
    fn compose_turn_prompt(&self, params: &TurnEnvelopeParams<'_>) -> String;
}

/// The latest harness version identifier — the version stamped onto new
/// sessions once per-session stamping lands.
pub(crate) const LATEST_VERSION: &str = "v1";

/// The latest harness implementation. Call sites use this until per-session
/// `harnessVersion` stamping lands and routes them through [`resolve`].
pub(crate) fn latest() -> &'static dyn Harness {
    &v1::V1
}

/// Resolve a `harnessVersion` string to its implementation. Unknown versions
/// fall back to the latest with a WARN (never fail a turn over a stale or
/// corrupt stamp). Unused until per-session stamping lands (follow-up task);
/// exercised by unit tests meanwhile.
#[allow(dead_code)]
pub(crate) fn resolve(version: &str) -> &'static dyn Harness {
    match version {
        "v1" => &v1::V1,
        other => {
            tracing::warn!(
                version = %other,
                latest = LATEST_VERSION,
                "unknown harness version; falling back to latest"
            );
            latest()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data_ptr(h: &'static dyn Harness) -> *const () {
        h as *const dyn Harness as *const ()
    }

    #[test]
    fn registry_resolves_v1() {
        assert!(std::ptr::eq(data_ptr(resolve("v1")), data_ptr(&v1::V1)));
    }

    #[test]
    fn registry_unknown_version_falls_back_to_latest() {
        assert!(std::ptr::eq(data_ptr(resolve("v999")), data_ptr(latest())));
        assert!(std::ptr::eq(data_ptr(resolve("")), data_ptr(latest())));
    }

    #[test]
    fn latest_is_v1() {
        assert_eq!(LATEST_VERSION, "v1");
        assert!(std::ptr::eq(data_ptr(latest()), data_ptr(&v1::V1)));
    }
}
