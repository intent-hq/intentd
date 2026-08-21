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
//! delegation preamble, notices — H6) live behind the same trait: the
//! producing functions in the managers remain as thin delegators (so tests
//! and goldens keep their call paths) while every byte of wording lives in
//! the version module.
//!
//! Each version also owns a [`Doctrine`] — its bundled instruction/specialist
//! markdown set under `resources/agent-instructions/<ver>/` and
//! `resources/specialists/<ver>/` — and the [`REGISTRY`] maps the stamped
//! session `harnessVersion` (intent-core's `"1.0"` form) to the pair, so a
//! session keeps assembling the exact doctrine it was created with even after
//! the binary ships a newer set. All past versions stay bundled.

pub(crate) mod v1;
pub(crate) mod v2;

use crate::agent_ops::ready_delta::UnblockedTask;
use crate::pr_monitor::PrMonitorSnapshot;
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

/// Typed inputs for the per-child settlement texts
/// ([`Harness::completion_wake`] / [`Harness::group_child_line`]): the data
/// half of a child-settlement report, extracted from the `agent:*` event by
/// the caller. All text fields arrive pre-filtered (empty strings resolved
/// to `None`); precedence between them (report over summary, stall tail
/// suppression next to a rendered report) is wording and belongs to the
/// harness.
pub(crate) struct ChildSettlementParams<'a> {
    /// The child's agent id (`agent-…`), always present.
    pub child_id: &'a str,
    /// Resolved display name (event `agentName`, falling back to the event
    /// actor); `None` renders the bare id.
    pub agent_name: Option<&'a str>,
    /// The settlement event type (`agent:idle` / `agent:failed` /
    /// `agent:deleted`); unknown types render verbatim.
    pub event_type: &'a str,
    /// The child's completion report (persisted or event-carried).
    pub completion_report: Option<&'a str>,
    /// The event's `lastResponseSummary`; loses to a present report.
    pub last_response_summary: Option<&'a str>,
    /// The event's `error` text.
    pub error: Option<&'a str>,
    /// Pending attention request folded onto a group line:
    /// `(kind, reason)` with kind `"blocker"` / `"discussion"`.
    pub attention: Option<(&'a str, &'a str)>,
    /// Suspected stall (monorepo#1016): `(task_title, task_status)`.
    pub stall: Option<(&'a str, &'a str)>,
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

    /// `<system>`-wrapped assembled system prompt for the `FirstTurnPrepend`
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
    /// (`FirstTurnPrepend` → snapshot → Context → naming nudge → role reminder
    /// → body) is itself versioned.
    fn compose_turn_prompt(&self, params: &TurnEnvelopeParams<'_>) -> String;

    // --- Queue notes and warnings (`agent_manager.rs`) ---

    /// `[SYSTEM NOTE]` appended to a stale queued-message redrive (#576).
    fn stale_redrive_note(&self, report_timestamp: &str) -> String;
    /// `[SYSTEM NOTE]` appended to a drained queue entry (monorepo#2353).
    fn dequeue_wait_note(&self, queued_at: &str, waited: &str) -> String;
    /// Human-readable wait for [`Harness::dequeue_wait_note`]: `Ns` under a
    /// minute, then `Nm Ss`, then `Nh Mm`; negative waits clamp to `0s`.
    fn wait_duration(&self, secs: i64) -> String;
    /// `[SYSTEM WARNING]` injected after a prompt idle-timeout interrupt;
    /// `window` is the pre-rendered seconds value (e.g. `1800` / `1.5`).
    fn idle_timeout_warning(&self, window: &str) -> String;
    /// `[SYSTEM NOTE]` auto-redrive nudge injected after a suspected-truncated
    /// turn on a delegated in-task agent (intent-hq/monorepo#2863).
    fn truncation_redrive_nudge(&self) -> String;
    /// `[SYSTEM NOTE]` auto-recovery nudge injected after a harness-wake turn
    /// that produced no meaningful output on a delegated in-task agent
    /// (intent-hq/monorepo#3262).
    fn empty_wake_redrive_nudge(&self) -> String;
    /// Attention-request reason recorded when an empty harness-wake recovery
    /// cannot be redriven — root/user-facing or taskless agent, or the
    /// consecutive-redrive cap is spent (intent-hq/monorepo#3262).
    fn empty_wake_attention_reason(&self) -> String;

    // --- Prompt notices (`agent_manager.rs`) ---

    /// `[System: {n} image(s) …]` notice after note-referenced images are
    /// inlined (Fidelity B, PROTOCOL §5.5).
    fn note_images_notice(&self, n: usize) -> String;
    /// `[Attachment: …]` reference notice (PROTOCOL §5.5): metadata plus the
    /// `ws.file.getAttachment` retrieval instruction.
    fn attachment_reference_notice(
        &self,
        name: &str,
        mime: Option<&str>,
        size: Option<u64>,
        id: &str,
    ) -> String;

    // --- Completion / group / watch wakes (`lib.rs`, `agent_ops.rs`) ---

    /// `[WORKSPACE EVENTS] Child agent {label} {kind}.` completion wake with
    /// report/summary/error tail and the #2051 watch-retired notes.
    fn completion_wake(&self, params: &ChildSettlementParams<'_>, watch_retired: bool) -> String;
    /// One `- {label} {kind}.…` per-child line of an `after_all` group wake,
    /// including the attention fold.
    fn group_child_line(&self, params: &ChildSettlementParams<'_>) -> String;
    /// The aggregated group-settlement wake: header plus accumulated
    /// per-child lines.
    fn group_settlement_wake(&self, total: usize, partial: bool, child_lines: &[String]) -> String;
    /// `[WORKSPACE EVENTS] Child agent … reported.` wake for an ungrouped
    /// `agent.reportToParent`, with the consumed-watch note when the report
    /// disarmed the parent's one-shot watch (monorepo#2528).
    fn report_to_parent_wake(
        &self,
        agent_name: &str,
        agent_id: &str,
        report: &str,
        watch_consumed: bool,
    ) -> String;
    /// Kind-flavored attention-request wake to the parent (`kind` is
    /// `"discussion"` / `"blocker"`).
    fn attention_parent_wake(
        &self,
        agent_name: &str,
        agent_id: &str,
        kind: &str,
        reason: &str,
    ) -> String;
    /// Attention-request fan-out wake to an explicit watcher (monorepo#1229);
    /// `grouped_watch` flips the completion promise to group settlement.
    fn attention_watcher_wake(
        &self,
        agent_name: &str,
        agent_id: &str,
        kind: &str,
        reason: &str,
        grouped_watch: bool,
    ) -> String;
    /// `[WORKSPACE EVENTS] {n} event(s) matched your subscription: {types}.`
    fn event_subscription_wake(&self, event_count: usize, event_types: &[&str]) -> String;
    /// The advisory "Tasks now unblocked by …" section (monorepo#2044).
    fn unblocked_section(&self, delta: &[UnblockedTask], multiple_triggers: bool) -> String;

    // --- Hook wakes and notices (`hook_manager.rs`) ---

    /// Append a run's captured console output as a `[hook logs]` section,
    /// head-truncated so a log-heavy run cannot flood the owner's queue.
    fn hook_wake_logs_section(&self, message: &str, logs: Option<&str>) -> String;
    /// Log-line warning for a returned hook `state` exceeding the byte cap.
    fn hook_state_dropped_warning(&self, state_bytes: usize, cap_bytes: usize) -> String;
    /// Diagnostic summary for a run whose `ws.host.exec` calls failed
    /// (nonzero exit / timeout) without the script throwing — persisted to
    /// `lastError` so silent check failures stay observable (monorepo#3231).
    /// `total` is the uncapped failure count; when it exceeds `lines.len()`
    /// (the per-run capture cap) the summary flags the omitted rest.
    fn hook_exec_failures_warning(&self, lines: &[&str], total: usize) -> String;
    /// `[Background hook "{name}"] {message}` framing plus the optional
    /// trailing state note.
    fn hook_wake_framing(&self, hook_name: &str, message: &str, state_note: Option<&str>)
        -> String;
    /// State note on a re-armed perpetual dispatch: the hook remains active
    /// until `expires_at` (`None` renders the TTL-elapses fallback).
    fn hook_dispatch_active_note(&self, expires_at: Option<&str>) -> String;
    /// State note on a one-shot dispatch: the hook is retired.
    fn hook_dispatch_retired_note(&self) -> String;
    /// State note on an eviction wake: the hook will not run again.
    fn hook_evicted_state_note(&self) -> String;
    /// Eviction notice body after a failed run.
    fn hook_evicted_failed_run_notice(&self, hook_name: &str, error: &str) -> String;
    /// Eviction notice body after an internal (store) error.
    fn hook_evicted_internal_error_notice(&self, hook_name: &str, error: &str) -> String;
    /// TTL-expiry notice body, with the perpetual runs+dispatches tally.
    fn hook_expired_notice(
        &self,
        hook_name: &str,
        perpetual: bool,
        run_count: i64,
        dispatch_count: i64,
    ) -> String;
    /// FE-cancel notice body (`hook.cancel` with no agent caller).
    fn hook_cancelled_from_app_notice(&self) -> String;
    /// Archive-sweep cancel notice body.
    fn hook_cancelled_workspace_archived_notice(&self) -> String;

    // --- PR monitor wakes and notices (`pr_monitor.rs`) ---

    /// The `<owner>/<name>#<number>` label every wake and event payload uses.
    fn pr_monitor_label(&self, owner: &str, name: &str, number: i64) -> String;
    /// The refreshed merge-requirements checklist ("where the PR stands
    /// now").
    fn pr_checklist(&self, snapshot: &PrMonitorSnapshot) -> String;
    /// Per-field change lines between two snapshots (the diff wording is
    /// versioned: the lines are both wake bullets and the persisted
    /// `pending_changes` set).
    fn pr_diff_lines(&self, old: &PrMonitorSnapshot, new: &PrMonitorSnapshot) -> Vec<String>;
    /// The consolidated change wake: bullets plus the refreshed checklist.
    fn pr_change_wake(
        &self,
        label: &str,
        changes: &[String],
        snapshot: &PrMonitorSnapshot,
    ) -> String;
    /// The terminal wake: merged/closed, monitoring stopped.
    fn pr_terminal_wake(
        &self,
        label: &str,
        changes: &[String],
        snapshot: &PrMonitorSnapshot,
    ) -> String;
    /// FE-cancel notice (`pr.unmonitor` with no agent caller).
    fn pr_monitor_cancelled_from_app_notice(&self, label: &str) -> String;
    /// Archive-sweep cancel notice.
    fn pr_monitor_cancelled_workspace_archived_notice(&self, label: &str) -> String;

    // --- Other conversation-reaching strings (`agent_ops.rs`) ---

    /// The delegated child's first message: the optional body joined with the
    /// TASK-C "Your Task Note" preamble.
    fn delegation_first_message(&self, body: Option<&str>, title: &str, note_id: &str) -> String;
    /// System notice after the user dismissed pending structured questions.
    fn questions_dismissed_notice(&self, count: usize) -> String;
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
    /// registry tests meanwhile (hence the allow — the lib build has no
    /// reader).
    #[allow(dead_code)]
    pub default_features: fn() -> AgentFeaturesSettings,
    /// `(camelCase key, human-readable label)` for every `agentFeatures`
    /// toggle this version knows about. For diagnostics/UI surfaces;
    /// exercised by registry tests meanwhile (hence the allow — the lib
    /// build has no reader).
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
static REGISTRY: &[&HarnessEntry] = &[&v1::ENTRY, &v2::ENTRY];

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
    if let Some(e) = REGISTRY.iter().find(|e| e.version == version) {
        e
    } else {
        tracing::warn!(
            version = %version,
            latest = LATEST_VERSION,
            "unknown harness version; falling back to latest"
        );
        latest_entry()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn data_ptr(h: &'static dyn Harness) -> *const () {
        std::ptr::from_ref::<dyn Harness>(h).cast::<()>()
    }

    /// The registry keys on the exact version string sessions are stamped
    /// with (intent-core's `CURRENT_HARNESS_VERSION`): the stamp and
    /// the resolved harness can never drift.
    #[test]
    fn registry_resolves_stamped_current_version() {
        let entry = resolve_entry(intent_core::CURRENT_HARNESS_VERSION);
        assert_eq!(entry.version, intent_core::CURRENT_HARNESS_VERSION);
        assert!(std::ptr::eq(
            data_ptr(resolve_entry(intent_core::CURRENT_HARNESS_VERSION).harness),
            data_ptr(&v1::V1)
        ));
    }

    #[test]
    fn registry_unknown_version_falls_back_to_latest() {
        assert!(std::ptr::eq(
            data_ptr(resolve_entry("v999").harness),
            data_ptr(latest())
        ));
        assert!(std::ptr::eq(
            data_ptr(resolve_entry("").harness),
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

    /// The v2 registry row selects new doctrine while v1 remains available.
    #[test]
    fn v2_selects_new_doctrine_without_changing_v1() {
        assert_eq!(
            resolve_entry("1.0").doctrine.instructions.common,
            v1::ENTRY.doctrine.instructions.common
        );
        assert_eq!(
            resolve_entry("2.0").doctrine.instructions.common,
            crate::instructions::V2.common
        );
        assert_ne!(
            resolve_entry("1.0").doctrine.instructions.common,
            resolve_entry("2.0").doctrine.instructions.common
        );
    }
}
