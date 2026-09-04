//! Harness v2.3: v2.2 doctrine with the `## Suggested Next Steps` prompt
//! hint refined so the model offers the user levers on its plan (holds,
//! alternatives, scope changes, decisions) instead of restating steps it
//! already said it will take. Every other text surface forwards to v1
//! unchanged; doctrine and specialist bytes are v2.2's.

use super::{ChildSettlementParams, Doctrine, Harness, HarnessEntry, TurnEnvelopeParams};
use crate::agent_ops::ready_delta::UnblockedTask;
use crate::pr_monitor::PrMonitorSnapshot;

/// The v2.3 text surfaces: [`Harness::suggested_next_steps_block`] is
/// reworded; everything else delegates to [`super::v1::V1`].
pub(crate) struct V2_3;

static DOCTRINE: Doctrine = Doctrine {
    instructions: &crate::instructions::V2_2,
    specialists: crate::specialists::EMBEDDED_BUNDLED_V2_1,
};

pub(crate) static ENTRY: HarnessEntry = HarnessEntry {
    version: "2.3",
    harness: &V2_3,
    doctrine: &DOCTRINE,
    default_features: intent_core::settings_file::AgentFeaturesSettings::default,
    feature_labels: super::v1::FEATURE_LABELS,
};

const V1: &super::v1::V1 = &super::v1::V1;

impl Harness for V2_3 {
    fn join_prompt_layers(&self, parts: &[String]) -> String {
        V1.join_prompt_layers(parts)
    }

    fn user_rules_wrapper(&self, content: &str, source: &str) -> String {
        V1.user_rules_wrapper(content, source)
    }

    fn rtk_instruction_line(&self, subcommands: &[String]) -> String {
        V1.rtk_instruction_line(subcommands)
    }

    fn sandboxed_implementor_hint(&self, sandbox_path: &str, sandbox_branch: &str) -> String {
        V1.sandboxed_implementor_hint(sandbox_path, sandbox_branch)
    }

    fn coordinator_cow_hint(&self) -> String {
        V1.coordinator_cow_hint()
    }

    fn specialist_role_section(&self, behavior_prompt: &str) -> String {
        V1.specialist_role_section(behavior_prompt)
    }

    fn commit_policy_clause(&self) -> String {
        V1.commit_policy_clause()
    }

    fn role_reminder_footer(&self, name: &str, reminder: Option<&str>) -> String {
        V1.role_reminder_footer(name, reminder)
    }

    fn ask_questions_block(&self) -> String {
        V1.ask_questions_block()
    }

    fn suggested_next_steps_block(&self, effective_auto_commit: bool) -> String {
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
             Hold off on opening the PR until I have reviewed the diff.\n\
             Skip the verifier pass and open the PR now.\n\
             -->\n\
             ```\n\n\
             Write 2–4 prompts, each a short directive sentence phrased as \
             something the user might say next. Never suggest a step you already \
             said you will take — the user does not need to ask for it. Instead \
             give the user levers on your plan: a hold or constraint (\"Do not open \
             the PR even if the verifier approves\"), an alternative path, a scope \
             change, or a decision only they can make.{auto_commit_clause}"
        )
    }

    fn first_turn_prepend_block(&self, prompt: &str) -> String {
        V1.first_turn_prepend_block(prompt)
    }

    fn snapshot_line(&self, json: &str) -> String {
        V1.snapshot_line(json)
    }

    fn naming_tool_reference(&self, provider_id: &str) -> &'static str {
        V1.naming_tool_reference(provider_id)
    }

    fn agent_naming_tool_reference(&self, provider_id: &str) -> &'static str {
        V1.agent_naming_tool_reference(provider_id)
    }

    fn naming_nudge(
        &self,
        agent_tool_reference: Option<&str>,
        workspace_tool_reference: Option<&str>,
    ) -> String {
        V1.naming_nudge(agent_tool_reference, workspace_tool_reference)
    }

    fn role_reminder_prefix(&self, name: &str, reminder: &str) -> String {
        V1.role_reminder_prefix(name, reminder)
    }

    fn compose_turn_prompt(&self, params: &TurnEnvelopeParams<'_>) -> String {
        V1.compose_turn_prompt(params)
    }

    fn stale_redrive_note(&self, report_timestamp: &str) -> String {
        V1.stale_redrive_note(report_timestamp)
    }

    fn dequeue_wait_note(&self, queued_at: &str, waited: &str) -> String {
        V1.dequeue_wait_note(queued_at, waited)
    }

    fn a2a_sender_note(&self, name: Option<&str>, agent_id: &str) -> String {
        V1.a2a_sender_note(name, agent_id)
    }

    fn wait_duration(&self, secs: i64) -> String {
        V1.wait_duration(secs)
    }

    fn idle_timeout_warning(&self, window: &str) -> String {
        V1.idle_timeout_warning(window)
    }

    fn truncation_redrive_nudge(&self) -> String {
        V1.truncation_redrive_nudge()
    }

    fn empty_wake_redrive_nudge(&self) -> String {
        V1.empty_wake_redrive_nudge()
    }

    fn empty_wake_attention_reason(&self) -> String {
        V1.empty_wake_attention_reason()
    }

    fn note_images_notice(&self, n: usize) -> String {
        V1.note_images_notice(n)
    }

    fn attachment_reference_notice(
        &self,
        name: &str,
        mime: Option<&str>,
        size: Option<u64>,
        id: &str,
    ) -> String {
        V1.attachment_reference_notice(name, mime, size, id)
    }

    fn completion_wake(&self, params: &ChildSettlementParams<'_>, watch_retired: bool) -> String {
        V1.completion_wake(params, watch_retired)
    }

    fn group_child_line(&self, params: &ChildSettlementParams<'_>) -> String {
        V1.group_child_line(params)
    }

    fn group_settlement_wake(&self, total: usize, partial: bool, child_lines: &[String]) -> String {
        V1.group_settlement_wake(total, partial, child_lines)
    }

    fn report_to_parent_wake(
        &self,
        agent_name: &str,
        agent_id: &str,
        report: &str,
        watch_consumed: bool,
    ) -> String {
        V1.report_to_parent_wake(agent_name, agent_id, report, watch_consumed)
    }

    fn attention_parent_wake(
        &self,
        agent_name: &str,
        agent_id: &str,
        kind: &str,
        reason: &str,
    ) -> String {
        V1.attention_parent_wake(agent_name, agent_id, kind, reason)
    }

    fn attention_watcher_wake(
        &self,
        agent_name: &str,
        agent_id: &str,
        kind: &str,
        reason: &str,
        grouped_watch: bool,
    ) -> String {
        V1.attention_watcher_wake(agent_name, agent_id, kind, reason, grouped_watch)
    }

    fn event_subscription_wake(&self, event_count: usize, event_types: &[&str]) -> String {
        V1.event_subscription_wake(event_count, event_types)
    }

    fn unblocked_section(&self, delta: &[UnblockedTask], multiple_triggers: bool) -> String {
        V1.unblocked_section(delta, multiple_triggers)
    }

    fn hook_wake_logs_section(&self, message: &str, logs: Option<&str>) -> String {
        V1.hook_wake_logs_section(message, logs)
    }

    fn hook_state_dropped_warning(&self, state_bytes: usize, cap_bytes: usize) -> String {
        V1.hook_state_dropped_warning(state_bytes, cap_bytes)
    }

    fn hook_exec_failures_warning(&self, lines: &[&str], total: usize) -> String {
        V1.hook_exec_failures_warning(lines, total)
    }

    fn hook_wake_framing(
        &self,
        hook_name: &str,
        message: &str,
        state_note: Option<&str>,
    ) -> String {
        V1.hook_wake_framing(hook_name, message, state_note)
    }

    fn hook_dispatch_active_note(&self, expires_at: Option<&str>) -> String {
        V1.hook_dispatch_active_note(expires_at)
    }

    fn hook_dispatch_retired_note(&self, hook_id: &str) -> String {
        V1.hook_dispatch_retired_note(hook_id)
    }

    fn hook_evicted_state_note(&self, hook_id: &str) -> String {
        V1.hook_evicted_state_note(hook_id)
    }

    fn hook_evicted_failed_run_notice(&self, hook_name: &str, error: &str) -> String {
        V1.hook_evicted_failed_run_notice(hook_name, error)
    }

    fn hook_evicted_internal_error_notice(&self, hook_name: &str, error: &str) -> String {
        V1.hook_evicted_internal_error_notice(hook_name, error)
    }

    fn hook_expired_notice(
        &self,
        hook_name: &str,
        hook_id: &str,
        perpetual: bool,
        run_count: i64,
        dispatch_count: i64,
    ) -> String {
        V1.hook_expired_notice(hook_name, hook_id, perpetual, run_count, dispatch_count)
    }

    fn hook_run_at_fired_notice(&self, hook_name: &str, hook_id: &str, run_at: &str) -> String {
        V1.hook_run_at_fired_notice(hook_name, hook_id, run_at)
    }

    fn hook_cancelled_from_app_notice(&self) -> String {
        V1.hook_cancelled_from_app_notice()
    }

    fn hook_cancelled_workspace_archived_notice(&self) -> String {
        V1.hook_cancelled_workspace_archived_notice()
    }

    fn pr_monitor_label(&self, owner: &str, name: &str, number: i64) -> String {
        V1.pr_monitor_label(owner, name, number)
    }

    fn pr_checklist(&self, snapshot: &PrMonitorSnapshot) -> String {
        V1.pr_checklist(snapshot)
    }

    fn pr_diff_lines(&self, old: &PrMonitorSnapshot, new: &PrMonitorSnapshot) -> Vec<String> {
        V1.pr_diff_lines(old, new)
    }

    fn pr_change_wake(
        &self,
        label: &str,
        changes: &[String],
        snapshot: &PrMonitorSnapshot,
    ) -> String {
        V1.pr_change_wake(label, changes, snapshot)
    }

    fn pr_terminal_wake(
        &self,
        label: &str,
        changes: &[String],
        snapshot: &PrMonitorSnapshot,
    ) -> String {
        V1.pr_terminal_wake(label, changes, snapshot)
    }

    fn pr_monitor_cancelled_from_app_notice(&self, label: &str) -> String {
        V1.pr_monitor_cancelled_from_app_notice(label)
    }

    fn pr_monitor_cancelled_workspace_archived_notice(&self, label: &str) -> String {
        V1.pr_monitor_cancelled_workspace_archived_notice(label)
    }

    fn delegation_first_message(&self, body: Option<&str>, title: &str, note_id: &str) -> String {
        V1.delegation_first_message(body, title, note_id)
    }

    fn questions_dismissed_notice(&self, count: usize) -> String {
        V1.questions_dismissed_notice(count)
    }

    fn proposal_applied_notice(&self, title: &str, detail: Option<&str>) -> String {
        V1.proposal_applied_notice(title, detail)
    }

    fn proposal_dismissed_notice(&self, title: &str) -> String {
        V1.proposal_dismissed_notice(title)
    }
}
