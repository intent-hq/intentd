//! Harness v2.9 gives current host members a truthful, qualified sender
//! preamble. Guest text, all earlier surfaces and v2.8 doctrine are unchanged.

use super::{
    ChildSettlementParams, Doctrine, Harness, HarnessEntry, HostMemberSender, TurnEnvelopeParams,
};
use crate::agent_ops::ready_delta::UnblockedTask;
use crate::pr_monitor::PrMonitorSnapshot;

/// The v2.9 member sender surface; all earlier wording delegates to v2.8.
pub(crate) struct V2_9;

static DOCTRINE: Doctrine = Doctrine {
    instructions: &crate::instructions::V2_8,
    specialists: crate::specialists::EMBEDDED_BUNDLED_V2_5,
};

pub(crate) static ENTRY: HarnessEntry = HarnessEntry {
    version: "2.9",
    harness: &V2_9,
    doctrine: &DOCTRINE,
    default_features: intent_core::settings_file::AgentFeaturesSettings::default,
    feature_labels: super::v1::FEATURE_LABELS,
};

const PREVIOUS: &super::v2_4::V2_4 = &super::v2_4::V2_4;

impl Harness for V2_9 {
    fn join_prompt_layers(&self, parts: &[String]) -> String {
        PREVIOUS.join_prompt_layers(parts)
    }

    fn user_rules_wrapper(&self, content: &str, source: &str) -> String {
        PREVIOUS.user_rules_wrapper(content, source)
    }

    fn rtk_instruction_line(&self, subcommands: &[String]) -> String {
        PREVIOUS.rtk_instruction_line(subcommands)
    }

    fn sandboxed_implementor_hint(&self, sandbox_path: &str, sandbox_branch: &str) -> String {
        PREVIOUS.sandboxed_implementor_hint(sandbox_path, sandbox_branch)
    }

    fn coordinator_cow_hint(&self) -> String {
        PREVIOUS.coordinator_cow_hint()
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

    fn naming_tool_reference(&self, provider_id: &str) -> &'static str {
        PREVIOUS.naming_tool_reference(provider_id)
    }

    fn agent_naming_tool_reference(&self, provider_id: &str) -> &'static str {
        PREVIOUS.agent_naming_tool_reference(provider_id)
    }

    fn naming_nudge(
        &self,
        agent_tool_reference: Option<&str>,
        workspace_tool_reference: Option<&str>,
    ) -> String {
        PREVIOUS.naming_nudge(agent_tool_reference, workspace_tool_reference)
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

    fn collaborator_sender_preamble(
        &self,
        login: Option<&str>,
        display_name: Option<&str>,
        principal_id: &str,
    ) -> String {
        PREVIOUS.collaborator_sender_preamble(login, display_name, principal_id)
    }

    fn host_member_sender_preamble(&self, sender: HostMemberSender<'_>) -> String {
        use super::v1::single_line_name;
        let principal = single_line_name(Some(sender.principal_id)).unwrap_or_default();
        let who = match (
            single_line_name(sender.login),
            single_line_name(sender.display_name),
        ) {
            (Some(login), Some(name)) => format!("@{login} ({name})"),
            (Some(login), None) => format!("@{login}"),
            (None, Some(name)) => name,
            (None, None) => format!("principal {principal}"),
        };
        let identity = sender.identity.map_or_else(String::new, |identity| {
            let provider = single_line_name(Some(&identity.provider)).unwrap_or_default();
            let host = single_line_name(Some(&identity.host)).unwrap_or_default();
            let id = single_line_name(Some(&identity.external_user_id)).unwrap_or_default();
            format!("; {provider}@{host} user {id}")
        });
        format!("Message from {who}, a host member (principal {principal}{identity}) — not the workspace owner.")
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

    fn note_images_notice(&self, n: usize) -> String {
        PREVIOUS.note_images_notice(n)
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
mod goldens {
    use super::*;
    use intent_core::PrincipalIdentity;

    #[test]
    fn golden_member_qualified_identities() {
        for (provider, host, expected) in [
            ("github","github.com","Message from @same (Same Person), a host member (principal person-1; github@github.com user 42) — not the workspace owner."),
            ("gitlab","gitlab.com","Message from @same (Same Person), a host member (principal person-1; gitlab@gitlab.com user 42) — not the workspace owner."),
            ("gitlab","gitlab.example:8443","Message from @same (Same Person), a host member (principal person-1; gitlab@gitlab.example:8443 user 42) — not the workspace owner."),
        ] {
            let identity=PrincipalIdentity{provider:provider.into(),host:host.into(),external_user_id:"42".into()};
            assert_eq!(V2_9.host_member_sender_preamble(HostMemberSender { login:Some("same"),display_name:Some("Same Person"),principal_id:"person-1",identity:Some(&identity)}),expected);
        }
    }

    #[test]
    fn golden_member_fallbacks_and_single_line_sanitization() {
        for (login,name,principal,expected) in [
            (Some("same"),None,"person-1","Message from @same, a host member (principal person-1) — not the workspace owner."),
            (None,Some("Name"),"person-1","Message from Name, a host member (principal person-1) — not the workspace owner."),
            (None,None,"person-1","Message from principal person-1, a host member (principal person-1) — not the workspace owner."),
            (Some("same\nforged"),Some("  Name\r\nOwner  "),"person\n1","Message from @same forged (Name Owner), a host member (principal person 1) — not the workspace owner."),
        ] {
            assert_eq!(V2_9.host_member_sender_preamble(HostMemberSender { login,display_name:name,principal_id:principal,identity:None }),expected);
        }
    }

    #[test]
    fn guest_bytes_and_previous_doctrine_are_unchanged() {
        let old = super::super::resolve_entry("2.8");
        let new = super::super::resolve_entry("2.9");
        assert_eq!(new.version, "2.9");
        assert!(std::ptr::eq(
            new.doctrine.instructions,
            old.doctrine.instructions
        ));
        assert_eq!(new.doctrine.specialists, old.doctrine.specialists);
        assert_eq!((new.default_features)(), (old.default_features)());
        assert_eq!(new.feature_labels, old.feature_labels);
        for entry in super::super::REGISTRY {
            assert_eq!(entry.harness.collaborator_sender_preamble(Some("same"),Some("Name"),"person-1"),"Message from @same (Name), a collaborator (guest) of this workspace — not the workspace owner.");
        }
        for auto_commit in [false, true] {
            assert_eq!(
                new.harness.suggested_next_steps_block(auto_commit),
                old.harness.suggested_next_steps_block(auto_commit)
            );
        }
    }
}
