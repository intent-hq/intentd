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
//!
//! Each version also owns a [`Doctrine`] — its bundled instruction/specialist
//! markdown set under `resources/agent-instructions/<ver>/` and
//! `resources/specialists/<ver>/` — and the [`REGISTRY`] maps the stamped
//! session `harnessVersion` (intent-core's `"1.0"` form) to the pair, so a
//! session keeps assembling the exact doctrine it was created with even after
//! the binary ships a newer set. All past versions stay bundled.

pub(crate) mod v1;

use intent_core::settings_file::AgentFeaturesSettings;

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

/// One registry row: a stamped `harnessVersion` and everything that version
/// owns — the [`Harness`] text surfaces, the versioned [`Doctrine`] (bundled
/// instruction + specialist markdown), the feature **defaults** new sessions
/// of that version assumed, and human-readable labels for each feature key.
pub(crate) struct HarnessEntry {
    /// The wire/session version string (intent-core's `"1.0"` form, exactly
    /// what `AgentSession::harness_version` stores).
    pub version: &'static str,
    /// The version's text surfaces.
    pub harness: &'static dyn Harness,
    /// The version's bundled doctrine set.
    pub doctrine: &'static Doctrine,
    /// The `agentFeatures` default values this version's doctrine assumes —
    /// what a NULL-snapshot gate would have resolved to when the version was
    /// current. Not consumed at runtime yet: legacy NULL-features rows keep
    /// the read-live behavior (`session_agent_features`); exercised by
    /// registry tests meanwhile.
    #[allow(dead_code)]
    pub default_features: fn() -> AgentFeaturesSettings,
    /// `(camelCase key, human-readable label)` for every `agentFeatures`
    /// toggle this version knows about. For diagnostics/UI surfaces;
    /// exercised by registry tests meanwhile.
    #[allow(dead_code)]
    pub feature_labels: &'static [(&'static str, &'static str)],
}

/// A version's bundled doctrine: the instruction markdown set composed by
/// [`crate::instructions`] plus the embedded specialist prompt bundle
/// (`(id, body)` pairs, the floor of the specialist 3-tier resolution).
pub(crate) struct Doctrine {
    pub instructions: &'static crate::instructions::InstructionSet,
    /// The version's embedded specialist bundle — the floor of the specialist
    /// 3-tier resolution for sessions stamped with this version
    /// (`SpecialistsService::with_embedded`); session-less `specialist.*`
    /// RPCs keep reading the latest bundle.
    pub specialists: &'static [(&'static str, &'static str)],
}

/// The latest harness version identifier — intent-core's
/// `CURRENT_HARNESS_VERSION`, the exact string stamped onto new sessions
/// (H1, intentd#1255), consumed here so the stamp and the registry can never
/// drift.
pub(crate) const LATEST_VERSION: &str = intent_core::CURRENT_HARNESS_VERSION;

/// Every bundled harness version, oldest first. All past versions stay
/// bundled so an old session keeps resolving the doctrine it was created
/// with. Adding a version = a `resources/**/<ver>/` directory + a module +
/// one row here.
static REGISTRY: &[&HarnessEntry] = &[&v1::ENTRY];

/// The registry row for [`LATEST_VERSION`]. A unit test pins that the row
/// exists; the tail fallback is unreachable and only avoids a panic path.
pub(crate) fn latest_entry() -> &'static HarnessEntry {
    REGISTRY
        .iter()
        .find(|e| e.version == LATEST_VERSION)
        .copied()
        .unwrap_or(REGISTRY[REGISTRY.len() - 1])
}

/// The latest harness implementation. Call sites not yet routed through a
/// session's stamp (turn envelope, wakes) use this.
pub(crate) fn latest() -> &'static dyn Harness {
    latest_entry().harness
}

/// Resolve a session's stamped `harnessVersion` to its registry row. Unknown
/// versions fall back to the latest with a WARN (never fail a turn over a
/// stale or corrupt stamp).
pub(crate) fn resolve_entry(version: &str) -> &'static HarnessEntry {
    match REGISTRY.iter().find(|e| e.version == version) {
        Some(e) => e,
        None => {
            tracing::warn!(
                version = %version,
                latest = LATEST_VERSION,
                "unknown harness version; falling back to latest"
            );
            latest_entry()
        }
    }
}

/// Resolve a `harnessVersion` string to its implementation
/// ([`resolve_entry`]'s harness projection).
#[allow(dead_code)]
pub(crate) fn resolve(version: &str) -> &'static dyn Harness {
    resolve_entry(version).harness
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data_ptr(h: &'static dyn Harness) -> *const () {
        h as *const dyn Harness as *const ()
    }

    /// The registry keys on the exact version string sessions are stamped
    /// with (intent-core's `CURRENT_HARNESS_VERSION`, "1.0"): the stamp and
    /// the resolved harness can never drift.
    #[test]
    fn registry_resolves_stamped_current_version() {
        let entry = resolve_entry(intent_core::CURRENT_HARNESS_VERSION);
        assert_eq!(entry.version, intent_core::CURRENT_HARNESS_VERSION);
        assert!(std::ptr::eq(
            data_ptr(resolve(intent_core::CURRENT_HARNESS_VERSION)),
            data_ptr(&v1::V1)
        ));
    }

    #[test]
    fn registry_unknown_version_falls_back_to_latest() {
        assert!(std::ptr::eq(data_ptr(resolve("v999")), data_ptr(latest())));
        assert!(std::ptr::eq(data_ptr(resolve("")), data_ptr(latest())));
        assert!(std::ptr::eq(
            data_ptr(resolve_entry("v999").harness),
            data_ptr(latest())
        ));
    }

    #[test]
    fn latest_is_current_harness_version() {
        assert_eq!(LATEST_VERSION, intent_core::CURRENT_HARNESS_VERSION);
        assert_eq!(latest_entry().version, LATEST_VERSION);
        assert!(std::ptr::eq(data_ptr(latest()), data_ptr(&v1::V1)));
    }

    /// Every registry row is coherent: unique version keys, a doctrine whose
    /// instruction set and specialist bundle are wired, and non-empty feature
    /// labels.
    #[test]
    fn registry_rows_are_coherent() {
        let mut seen = std::collections::HashSet::new();
        for entry in REGISTRY {
            assert!(seen.insert(entry.version), "duplicate: {}", entry.version);
            assert!(!entry.doctrine.specialists.is_empty());
            assert!(!entry.doctrine.instructions.common.is_empty());
            assert!(!entry.feature_labels.is_empty());
            let _ = (entry.default_features)();
        }
    }

    /// Adding a hypothetical v2 is a directory + registry entry: a fixture
    /// registry with a second row resolves each version to its own doctrine
    /// and still falls back to its latest for unknown stamps.
    #[test]
    fn hypothetical_v2_is_a_directory_plus_registry_entry() {
        // A future set would `include_str!` from
        // `resources/agent-instructions/v2/`; the fixture only needs
        // distinct bytes.
        static V2_INSTRUCTIONS: crate::instructions::InstructionSet =
            crate::instructions::InstructionSet {
                chat: "v2",
                common: "v2 common",
                debug: "v2",
                workspace: "v2",
                setup_script_generator: "v2",
                task_breakdown: "v2",
                task_debug: "v2",
                task_focused: "v2",
                task_loop: "v2",
                ralph_loop: "v2",
                workspace_agent: "v2",
                notes_system_guide: "v2",
                code_review: "v2",
                code_walkthrough: "v2",
                commit_message: "v2",
                pr_description: "v2",
            };
        static V2_DOCTRINE: Doctrine = Doctrine {
            instructions: &V2_INSTRUCTIONS,
            specialists: &[("implementor", "v2 implementor body")],
        };
        static V2_ENTRY: HarnessEntry = HarnessEntry {
            version: "2.0",
            harness: &v1::V1,
            doctrine: &V2_DOCTRINE,
            default_features: intent_core::settings_file::AgentFeaturesSettings::default,
            feature_labels: &[("taskGraph", "Task-graph workflow teaching")],
        };
        let fixture: &[&HarnessEntry] = &[&v1::ENTRY, &V2_ENTRY];
        let find = |v: &str| {
            fixture
                .iter()
                .find(|e| e.version == v)
                .copied()
                .unwrap_or(fixture[fixture.len() - 1])
        };
        assert_eq!(
            find("1.0").doctrine.instructions.common,
            v1::ENTRY.doctrine.instructions.common
        );
        assert_eq!(find("2.0").doctrine.instructions.common, "v2 common");
        // Unknown stamps fall back to the fixture's latest row.
        assert_eq!(find("9.9").version, "2.0");
        // An old session pinned to 1.0 keeps its original doctrine even
        // though 2.0 exists.
        assert_ne!(
            find("1.0").doctrine.instructions.common,
            find("2.0").doctrine.instructions.common
        );
    }
}
