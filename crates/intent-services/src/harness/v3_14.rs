//! Harness 3.14: 3.13 with one turn-envelope change. The first-turn
//! workspace-title instruction named the `set_workspace_title` MCP tool,
//! removed in WSAPI-8; it now asks for `ws.workspace.setTitle` through the
//! `workspace_api` tool, the same tool the agent-naming instruction names.
//! Every other surface, the doctrine and the context are 3.13's.

use super::{ChildSettlementParams, Harness, HarnessEntry, HostMemberSender, TurnEnvelopeParams};
use crate::agent_ops::ready_delta::UnblockedTask;
use crate::pr_monitor::PrMonitorSnapshot;

/// The 3.14 naming surfaces; every other surface forwards to v2.9.
pub(crate) struct V3_14;

pub(crate) static ENTRY: HarnessEntry = HarnessEntry {
    version: "3.14",
    harness: &V3_14,
    doctrine: super::v3_13::ENTRY.doctrine,
    context: super::v3_13::ENTRY.context,
    default_features: intent_core::settings_file::AgentFeaturesSettings::default,
    feature_labels: super::v1::FEATURE_LABELS,
};

const PREVIOUS: &super::v2_9::V2_9 = &super::v2_9::V2_9;

impl Harness for V3_14 {
    // The same spelling as `agent_naming_tool_reference` today; kept a
    // separate surface so a later version can name a different tool.
    fn naming_tool_reference(&self, provider_id: &str) -> &'static str {
        PREVIOUS.agent_naming_tool_reference(provider_id)
    }

    fn naming_nudge(
        &self,
        agent_tool_reference: Option<&str>,
        workspace_tool_reference: Option<&str>,
    ) -> String {
        let mut instructions = Vec::new();
        if let Some(tool_reference) = agent_tool_reference {
            instructions.push(format!(
                "This agent still has a generated name. Early in your first turn, call \
                 `ws.workspace.setAgentName` through {tool_reference} with a short 1\u{2013}5 word \
                 task-specific name. Do this independently of workspace title naming and in \
                 parallel with information-gathering."
            ));
        }
        if let Some(tool_reference) = workspace_tool_reference {
            instructions.push(format!(
                "This workspace needs a title. As your first action, call \
                 `ws.workspace.setTitle` through {tool_reference} with a short 3\u{2013}5 word \
                 sentence-case title describing the task. This can be called in parallel with \
                 information-gathering."
            ));
        }
        format!("<system>\n{}\n</system>", instructions.join("\n"))
    }

    fn join_prompt_layers(&self, parts: &[String]) -> String {
        PREVIOUS.join_prompt_layers(parts)
    }

    fn user_rules_wrapper(&self, content: &str, source: &str) -> String {
        PREVIOUS.user_rules_wrapper(content, source)
    }

    fn rtk_instruction_line(&self, subcommands: &[String]) -> String {
        PREVIOUS.rtk_instruction_line(subcommands)
    }

    fn specialist_role_section(&self, behavior_prompt: &str) -> String {
        PREVIOUS.specialist_role_section(behavior_prompt)
    }

    fn commit_policy_clause(&self) -> String {
        PREVIOUS.commit_policy_clause()
    }

    fn role_reminder_footer(&self, name: &str, reminder: Option<&str>) -> String {
        PREVIOUS.role_reminder_footer(name, reminder)
    }

    fn ask_questions_block(&self) -> String {
        PREVIOUS.ask_questions_block()
    }

    fn suggested_next_steps_block(&self, effective_auto_commit: bool) -> String {
        PREVIOUS.suggested_next_steps_block(effective_auto_commit)
    }

    fn first_turn_prepend_block(&self, prompt: &str) -> String {
        PREVIOUS.first_turn_prepend_block(prompt)
    }

    fn snapshot_line(&self, json: &str) -> String {
        PREVIOUS.snapshot_line(json)
    }

    fn agent_naming_tool_reference(&self, provider_id: &str) -> &'static str {
        PREVIOUS.agent_naming_tool_reference(provider_id)
    }

    fn role_reminder_prefix(&self, name: &str, reminder: &str) -> String {
        PREVIOUS.role_reminder_prefix(name, reminder)
    }

    fn setup_in_progress_notice(&self, terminal_name: &str) -> String {
        PREVIOUS.setup_in_progress_notice(terminal_name)
    }

    fn setup_failed_notice(&self, exit_code: Option<u32>, terminal_name: &str) -> String {
        PREVIOUS.setup_failed_notice(exit_code, terminal_name)
    }

    fn compose_turn_prompt(&self, params: &TurnEnvelopeParams<'_>) -> String {
        PREVIOUS.compose_turn_prompt(params)
    }

    fn stale_redrive_note(&self, report_timestamp: &str) -> String {
        PREVIOUS.stale_redrive_note(report_timestamp)
    }

    fn dequeue_wait_note(&self, queued_at: &str, waited: &str) -> String {
        PREVIOUS.dequeue_wait_note(queued_at, waited)
    }

    fn a2a_sender_note(&self, name: Option<&str>, agent_id: &str) -> String {
        PREVIOUS.a2a_sender_note(name, agent_id)
    }

    fn preemption_attribution_note(&self) -> String {
        PREVIOUS.preemption_attribution_note()
    }

    fn collaborator_sender_preamble(
        &self,
        login: Option<&str>,
        display_name: Option<&str>,
        principal_id: &str,
    ) -> String {
        PREVIOUS.collaborator_sender_preamble(login, display_name, principal_id)
    }

    fn host_member_sender_preamble(&self, sender: HostMemberSender<'_>) -> String {
        PREVIOUS.host_member_sender_preamble(sender)
    }

    fn wait_duration(&self, secs: i64) -> String {
        PREVIOUS.wait_duration(secs)
    }

    fn idle_timeout_warning(&self, window: &str) -> String {
        PREVIOUS.idle_timeout_warning(window)
    }

    fn truncation_redrive_nudge(&self) -> String {
        PREVIOUS.truncation_redrive_nudge()
    }

    fn empty_wake_redrive_nudge(&self) -> String {
        PREVIOUS.empty_wake_redrive_nudge()
    }

    fn empty_wake_attention_reason(&self) -> String {
        PREVIOUS.empty_wake_attention_reason()
    }

    fn note_reference_notice(
        &self,
        title: &str,
        id: &str,
        image_urls: &[String],
        omitted: usize,
    ) -> String {
        PREVIOUS.note_reference_notice(title, id, image_urls, omitted)
    }

    fn attachment_reference_notice(
        &self,
        name: &str,
        mime: Option<&str>,
        size: Option<u64>,
        id: &str,
    ) -> String {
        PREVIOUS.attachment_reference_notice(name, mime, size, id)
    }

    fn context_size_requeue_marker(&self, original_chars: usize) -> String {
        PREVIOUS.context_size_requeue_marker(original_chars)
    }

    fn completion_wake(&self, params: &ChildSettlementParams<'_>, watch_retired: bool) -> String {
        PREVIOUS.completion_wake(params, watch_retired)
    }

    fn group_child_line(&self, params: &ChildSettlementParams<'_>) -> String {
        PREVIOUS.group_child_line(params)
    }

    fn group_settlement_wake(&self, total: usize, partial: bool, child_lines: &[String]) -> String {
        PREVIOUS.group_settlement_wake(total, partial, child_lines)
    }

    fn report_to_parent_wake(
        &self,
        agent_name: &str,
        agent_id: &str,
        report: &str,
        watch_consumed: bool,
    ) -> String {
        PREVIOUS.report_to_parent_wake(agent_name, agent_id, report, watch_consumed)
    }

    fn attention_parent_wake(
        &self,
        agent_name: &str,
        agent_id: &str,
        kind: &str,
        reason: &str,
    ) -> String {
        PREVIOUS.attention_parent_wake(agent_name, agent_id, kind, reason)
    }

    fn attention_watcher_wake(
        &self,
        agent_name: &str,
        agent_id: &str,
        kind: &str,
        reason: &str,
        grouped_watch: bool,
    ) -> String {
        PREVIOUS.attention_watcher_wake(agent_name, agent_id, kind, reason, grouped_watch)
    }

    fn event_subscription_wake(&self, event_count: usize, event_types: &[&str]) -> String {
        PREVIOUS.event_subscription_wake(event_count, event_types)
    }

    fn unblocked_section(&self, delta: &[UnblockedTask], multiple_triggers: bool) -> String {
        PREVIOUS.unblocked_section(delta, multiple_triggers)
    }

    fn hook_wake_logs_section(&self, message: &str, logs: Option<&str>) -> String {
        PREVIOUS.hook_wake_logs_section(message, logs)
    }

    fn hook_state_dropped_warning(&self, state_bytes: usize, cap_bytes: usize) -> String {
        PREVIOUS.hook_state_dropped_warning(state_bytes, cap_bytes)
    }

    fn hook_wake_message_truncated_marker(
        &self,
        omitted_chars: usize,
        total_chars: usize,
        cap_chars: usize,
    ) -> String {
        PREVIOUS.hook_wake_message_truncated_marker(omitted_chars, total_chars, cap_chars)
    }

    fn hook_exec_failures_warning(&self, lines: &[&str], total: usize) -> String {
        PREVIOUS.hook_exec_failures_warning(lines, total)
    }

    fn hook_wake_framing(
        &self,
        hook_name: &str,
        message: &str,
        state_note: Option<&str>,
    ) -> String {
        PREVIOUS.hook_wake_framing(hook_name, message, state_note)
    }

    fn hook_dispatch_active_note(&self, expires_at: Option<&str>) -> String {
        PREVIOUS.hook_dispatch_active_note(expires_at)
    }

    fn hook_dispatch_retired_note(&self, hook_id: &str) -> String {
        PREVIOUS.hook_dispatch_retired_note(hook_id)
    }

    fn hook_evicted_state_note(&self, hook_id: &str) -> String {
        PREVIOUS.hook_evicted_state_note(hook_id)
    }

    fn hook_evicted_failed_run_notice(&self, hook_name: &str, error: &str) -> String {
        PREVIOUS.hook_evicted_failed_run_notice(hook_name, error)
    }

    fn hook_evicted_internal_error_notice(&self, hook_name: &str, error: &str) -> String {
        PREVIOUS.hook_evicted_internal_error_notice(hook_name, error)
    }

    fn hook_expired_notice(
        &self,
        hook_name: &str,
        hook_id: &str,
        perpetual: bool,
        run_count: i64,
        dispatch_count: i64,
    ) -> String {
        PREVIOUS.hook_expired_notice(hook_name, hook_id, perpetual, run_count, dispatch_count)
    }

    fn hook_run_at_fired_notice(&self, hook_name: &str, hook_id: &str, run_at: &str) -> String {
        PREVIOUS.hook_run_at_fired_notice(hook_name, hook_id, run_at)
    }

    fn hook_cancelled_from_app_notice(&self) -> String {
        PREVIOUS.hook_cancelled_from_app_notice()
    }

    fn hook_cancelled_workspace_archived_notice(&self) -> String {
        PREVIOUS.hook_cancelled_workspace_archived_notice()
    }

    fn pr_monitor_label(&self, owner: &str, name: &str, number: i64) -> String {
        PREVIOUS.pr_monitor_label(owner, name, number)
    }

    fn pr_checklist(&self, snapshot: &PrMonitorSnapshot) -> String {
        PREVIOUS.pr_checklist(snapshot)
    }

    fn pr_diff_lines(&self, old: &PrMonitorSnapshot, new: &PrMonitorSnapshot) -> Vec<String> {
        PREVIOUS.pr_diff_lines(old, new)
    }

    fn pr_change_wake(
        &self,
        label: &str,
        changes: &[String],
        snapshot: &PrMonitorSnapshot,
    ) -> String {
        PREVIOUS.pr_change_wake(label, changes, snapshot)
    }

    fn pr_terminal_wake(
        &self,
        label: &str,
        changes: &[String],
        snapshot: &PrMonitorSnapshot,
    ) -> String {
        PREVIOUS.pr_terminal_wake(label, changes, snapshot)
    }

    fn pr_monitor_cancelled_from_app_notice(&self, label: &str) -> String {
        PREVIOUS.pr_monitor_cancelled_from_app_notice(label)
    }

    fn pr_monitor_cancelled_workspace_archived_notice(&self, label: &str) -> String {
        PREVIOUS.pr_monitor_cancelled_workspace_archived_notice(label)
    }

    fn pr_monitor_transferred_to_parent_notice(&self, label: &str, parent_id: &str) -> String {
        PREVIOUS.pr_monitor_transferred_to_parent_notice(label, parent_id)
    }

    fn workspace_archived_watches_cancelled_notice(
        &self,
        hooks: &[(&str, &str)],
        monitors: &[&str],
    ) -> String {
        PREVIOUS.workspace_archived_watches_cancelled_notice(hooks, monitors)
    }

    fn delegation_first_message(&self, body: Option<&str>, title: &str, note_id: &str) -> String {
        PREVIOUS.delegation_first_message(body, title, note_id)
    }

    fn questions_dismissed_notice(&self, count: usize) -> String {
        PREVIOUS.questions_dismissed_notice(count)
    }

    fn proposal_applied_notice(&self, title: &str, detail: Option<&str>) -> String {
        PREVIOUS.proposal_applied_notice(title, detail)
    }

    fn proposal_dismissed_notice(&self, title: &str) -> String {
        PREVIOUS.proposal_dismissed_notice(title)
    }
}

#[cfg(test)]
mod tests {
    use super::super::Harness as _;
    use super::{PREVIOUS, V3_14};

    #[test]
    fn v3_14_is_3_13_with_the_workspace_api_title_instruction() {
        let current = super::super::resolve_entry("3.14");
        let prior = super::super::resolve_entry("3.13");
        assert_eq!(current.version, "3.14");
        assert!(std::ptr::eq(current.context, prior.context));
        assert!(std::ptr::eq(current.doctrine, prior.doctrine));
        assert_eq!((current.default_features)(), (prior.default_features)());
        assert_eq!(current.feature_labels, prior.feature_labels);
        assert!(intent_core::prompt_breakdown::harness_records_prompt_breakdown("3.14"));

        for provider in ["auggie", "opencode", "claude-code", "codex"] {
            assert_eq!(
                V3_14.naming_tool_reference(provider),
                PREVIOUS.agent_naming_tool_reference(provider),
                "{provider}"
            );
            assert!(!V3_14
                .naming_tool_reference(provider)
                .contains("set_workspace_title"));
        }
        // The agent line is 3.13's, byte for byte.
        assert_eq!(
            V3_14.naming_nudge(Some("the `workspace_api` tool"), None),
            PREVIOUS.naming_nudge(Some("the `workspace_api` tool"), None)
        );
        assert_ne!(
            V3_14.naming_nudge(None, Some("ws-ref")),
            PREVIOUS.naming_nudge(None, Some("ws-ref"))
        );
        assert_eq!(
            V3_14.naming_nudge(Some("a-ref"), Some("ws-ref")),
            "<system>\nThis agent still has a generated name. Early in your first turn, call \
             `ws.workspace.setAgentName` through a-ref with a short 1\u{2013}5 word task-specific \
             name. Do this independently of workspace title naming and in parallel with \
             information-gathering.\nThis workspace needs a title. As your first action, call \
             `ws.workspace.setTitle` through ws-ref with a short 3\u{2013}5 word sentence-case \
             title describing the task. This can be called in parallel with \
             information-gathering.\n</system>"
        );
    }
}
