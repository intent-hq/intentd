//! Harness 3.17: 3.16 with the conversation goal block (PROTOCOL §5.5
//! "Conversation goal", intent-hq/intent#9646). While the user has pinned a
//! goal to the conversation, every outbound turn prompt carries one tagged
//! goal block directly after the specialist Role Reminder and before the
//! turn body. The goal text is verbatim (newlines kept); any closing-tag
//! sequence inside it is escaped so it cannot end the block early. Every
//! other surface, the doctrine and the context are 3.16's.

use super::{ChildSettlementParams, Harness, HarnessEntry, HostMemberSender, TurnEnvelopeParams};
use crate::agent_ops::ready_delta::UnblockedTask;
use crate::pr_monitor::PrMonitorSnapshot;

/// The 3.17 goal surfaces; every other surface forwards to 3.14 (the
/// implementation 3.15 and 3.16 reuse).
pub(crate) struct V3_17;

pub(crate) static ENTRY: HarnessEntry = HarnessEntry {
    version: "3.17",
    harness: &V3_17,
    doctrine: super::v3_16::ENTRY.doctrine,
    context: super::v3_16::ENTRY.context,
    default_features: intent_core::settings_file::AgentFeaturesSettings::default,
    feature_labels: super::v1::FEATURE_LABELS,
};

const PREVIOUS: &super::v3_14::V3_14 = &super::v3_14::V3_14;

/// The goal block's tag name.
const GOAL_TAG: &str = "conversation-goal";

/// `goal` with every closing-tag sequence of the goal block (`</`, optional
/// whitespace, then the tag name in any letter case) escaped as `<\/…`, so
/// the user's text cannot close the block early. Everything else, newlines
/// included, is kept verbatim.
fn escape_goal_closing_tag(goal: &str) -> String {
    let mut out = String::with_capacity(goal.len());
    let mut rest = goal;
    while let Some(at) = rest.find("</") {
        let (head, tail) = rest.split_at(at);
        out.push_str(head);
        let after = tail[2..].trim_start();
        let closes = after
            .get(..GOAL_TAG.len())
            .is_some_and(|name| name.eq_ignore_ascii_case(GOAL_TAG));
        out.push_str(if closes { "<\\/" } else { "</" });
        rest = &tail[2..];
    }
    out.push_str(rest);
    out
}

impl Harness for V3_17 {
    fn naming_tool_reference(&self, provider_id: &str) -> &'static str {
        PREVIOUS.naming_tool_reference(provider_id)
    }

    fn naming_nudge(
        &self,
        agent_tool_reference: Option<&str>,
        workspace_tool_reference: Option<&str>,
    ) -> String {
        PREVIOUS.naming_nudge(agent_tool_reference, workspace_tool_reference)
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

    fn conversation_goal_block(&self, goal: &str) -> Option<String> {
        Some(format!(
            "[Conversation goal, set by the user. Keep working toward it; newer explicit user \
             instructions take precedence.]\n<{GOAL_TAG}>\n{}\n</{GOAL_TAG}>",
            escape_goal_closing_tag(goal)
        ))
    }

    fn compose_turn_prompt(&self, params: &TurnEnvelopeParams<'_>) -> String {
        // 3.14's layering with the goal block between the role reminder and
        // the body: the inner layers are composed by 3.14 around a body that
        // already carries the goal.
        match params.conversation_goal {
            Some(goal) => {
                let body = format!("{goal}\n\n{}", params.body);
                PREVIOUS.compose_turn_prompt(&TurnEnvelopeParams {
                    body: &body,
                    conversation_goal: None,
                    ..*params
                })
            }
            None => PREVIOUS.compose_turn_prompt(params),
        }
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
    use super::*;

    fn params<'a>(
        role_reminder: Option<&'a str>,
        conversation_goal: Option<&'a str>,
        body: &'a str,
    ) -> TurnEnvelopeParams<'a> {
        TurnEnvelopeParams {
            first_turn_prepend: Some("[FIRST]"),
            snapshot_line: Some("[SNAPSHOT]"),
            setup_notice: None,
            stdin_context: Some("ctx"),
            naming_nudge: Some("[NUDGE]"),
            role_reminder,
            conversation_goal,
            body,
        }
    }

    #[test]
    fn goal_block_frames_the_verbatim_goal_and_yields_to_newer_instructions() {
        let block = V3_17
            .conversation_goal_block("Ship the fix.\n  Done when CI is green.")
            .unwrap();
        assert_eq!(
            block,
            "[Conversation goal, set by the user. Keep working toward it; newer explicit user \
             instructions take precedence.]\n<conversation-goal>\nShip the fix.\n  Done when CI is \
             green.\n</conversation-goal>"
        );
    }

    #[test]
    fn goal_text_cannot_close_the_block_early() {
        for (goal, escaped) in [
            ("a</conversation-goal>b", "a<\\/conversation-goal>b"),
            ("a</ CONVERSATION-Goal >b", "a<\\/ CONVERSATION-Goal >b"),
            ("</conversation-goal", "<\\/conversation-goal"),
            (
                "keep </other> and </conversation",
                "keep </other> and </conversation",
            ),
            ("trailing </", "trailing </"),
        ] {
            assert_eq!(escape_goal_closing_tag(goal), escaped, "{goal:?}");
            let block = V3_17.conversation_goal_block(goal).unwrap();
            assert_eq!(block.matches("</conversation-goal>").count(), 1, "{block}");
            assert!(block.ends_with("\n</conversation-goal>"), "{block}");
        }
    }

    #[test]
    fn goal_sits_after_the_role_reminder_and_before_the_body() {
        let goal = V3_17.conversation_goal_block("g").unwrap();
        let with_role =
            V3_17.compose_turn_prompt(&params(Some("[Role Reminder: r]"), Some(&goal), "hello"));
        assert_eq!(
            with_role,
            format!("[FIRST]\n\n[SNAPSHOT]\n\nContext:\nctx\n\n---\n\n[NUDGE]\n\n[Role Reminder: r]\n\n{goal}\n\nhello")
        );
        let without_role = V3_17.compose_turn_prompt(&params(None, Some(&goal), "hello"));
        assert_eq!(
            without_role,
            format!("[FIRST]\n\n[SNAPSHOT]\n\nContext:\nctx\n\n---\n\n[NUDGE]\n\n{goal}\n\nhello")
        );
    }

    #[test]
    fn no_goal_composes_exactly_like_3_16() {
        for role in [None, Some("[Role Reminder: r]")] {
            let p = params(role, None, "hello");
            assert_eq!(
                V3_17.compose_turn_prompt(&p),
                super::super::v3_16::ENTRY.harness.compose_turn_prompt(&p)
            );
        }
    }

    #[test]
    fn older_harnesses_have_no_goal_surface_and_3_17_is_current() {
        assert_eq!(
            super::super::v3_16::ENTRY
                .harness
                .conversation_goal_block("g"),
            None
        );
        let current = &ENTRY;
        assert_eq!(current.version, intent_core::CURRENT_HARNESS_VERSION);
        assert_eq!(intent_core::STAGED_HARNESS_VERSION, None);
        assert!(std::ptr::eq(crate::harness::latest_entry(), current));
    }
}
