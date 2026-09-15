//! Harness v2.4 golden fixtures. This version keeps the v2.2 doctrine
//! (instructions + specialist bundle, exactly as v2.3 does) and extends
//! exactly one text surface: the `## Suggested Next Steps` prompt hint,
//! which now tells the model it may write markdown inline links
//! `[label](url)` inside a prompt so PR/issue references render clickable,
//! that bare `#N` is not linkified, and that the visible text must stay
//! short. Every other [`crate::harness::Harness`] surface forwards to v1
//! unchanged.

const NEXT_STEPS_OFF: &str = "## Suggested Next Steps\n\n\
    At the end of your response, offer the user clear next actions as a \
    `<!-- suggested-prompts ... -->` HTML comment block:\n\n\
    ```\n\
    <!-- suggested-prompts\n\
    Hold off on merging [#5034](https://github.com/owner/repo/pull/5034) until I have reviewed the diff.\n\
    Skip the verifier pass and open the PR now.\n\
    -->\n\
    ```\n\n\
    Write 2–4 prompts, each a short directive sentence phrased as \
    something the user might say next. Never suggest a step you already \
    said you will take — the user does not need to ask for it. Instead \
    give the user levers on your plan: a hold or constraint (\"Do not open \
    the PR even if the verifier approves\"), an alternative path, a scope \
    change, or a decision only they can make. You may write markdown \
    inline links `[label](url)` inside a prompt so PR and issue references \
    render as clickable links (e.g. \
    `[#5034](https://github.com/owner/repo/pull/5034)`); a bare `#5034` or \
    bare URL is not linkified. Keep each prompt's visible text (link \
    labels, not URLs) short — well under 200 characters — or the prompt \
    is dropped.";

const AUTO_COMMIT_CLAUSE: &str = " Auto-commit is enabled; do not include prompts about \
    committing or reviewing changes before committing.";

#[test]
fn current_harness_version_is_v2_4() {
    assert_eq!(intent_core::model::CURRENT_HARNESS_VERSION, "2.4");
    assert_eq!(
        crate::harness::resolve_entry(intent_core::model::CURRENT_HARNESS_VERSION).version,
        "2.4"
    );
}

/// Exact bytes of the extended block, both auto-commit variants. The
/// auto-commit clause is the v1 sentence verbatim; only the example lines
/// and the trailing guidance changed.
#[test]
fn golden_v2_4_suggested_next_steps_block() {
    let h = crate::harness::resolve_entry("2.4").harness;
    assert_eq!(h.suggested_next_steps_block(false), NEXT_STEPS_OFF);
    assert_eq!(
        h.suggested_next_steps_block(true),
        format!("{NEXT_STEPS_OFF}{AUTO_COMMIT_CLAUSE}")
    );
    let v1 = crate::harness::resolve_entry("1.0").harness;
    let v1_on = v1.suggested_next_steps_block(true);
    assert!(
        v1_on.ends_with(AUTO_COMMIT_CLAUSE),
        "auto-commit clause is identical to v1"
    );
    let v2_3 = crate::harness::resolve_entry("2.3").harness;
    assert!(!v2_3
        .suggested_next_steps_block(false)
        .contains("inline links"));
}

/// v2.4 is v2.3 plus the markdown-link guidance: the block starts with
/// v2.3's guidance sentences verbatim and appends the link sentences.
#[test]
fn v2_4_next_steps_extends_v2_3_text() {
    let v2_3 = crate::harness::resolve_entry("2.3").harness;
    let v2_4 = crate::harness::resolve_entry("2.4").harness;
    let v2_3_off = v2_3.suggested_next_steps_block(false);
    let v2_4_off = v2_4.suggested_next_steps_block(false);
    let (v2_3_head, v2_3_tail) = v2_3_off.split_once("```\n\n").expect("v2.3 example block");
    let (v2_4_head, v2_4_tail) = v2_4_off.split_once("```\n\n").expect("v2.4 example block");
    assert_ne!(v2_3_head, v2_4_head, "the example shows one link");
    assert!(v2_4_head.contains("[#5034](https://github.com/owner/repo/pull/5034)"));
    assert!(
        v2_4_tail.starts_with(v2_3_tail),
        "v2.4 keeps v2.3's guidance and appends the link sentences"
    );
    for auto_commit in [false, true] {
        assert_ne!(
            v2_3.suggested_next_steps_block(auto_commit),
            v2_4.suggested_next_steps_block(auto_commit)
        );
    }
}

/// v2.4 selects the v2.2 doctrine unchanged — the same instruction set and
/// the same specialist bundle v2.3 carries — so the v2.3→v2.4 diff is one
/// text surface.
#[test]
fn v2_4_registry_keeps_v2_3_doctrine() {
    let v2_2 = crate::harness::resolve_entry("2.2");
    let v2_3 = crate::harness::resolve_entry("2.3");
    let v2_4 = crate::harness::resolve_entry("2.4");
    assert!(std::ptr::eq(
        v2_4.doctrine.instructions,
        v2_3.doctrine.instructions
    ));
    assert!(std::ptr::eq(
        v2_4.doctrine.instructions,
        v2_2.doctrine.instructions
    ));
    assert_eq!(
        v2_4.doctrine.instructions.workspace,
        crate::instructions::V2_2.workspace
    );
    assert_eq!(v2_4.doctrine.specialists, v2_3.doctrine.specialists);
    assert_eq!(
        v2_4.doctrine.specialists,
        crate::specialists::EMBEDDED_BUNDLED_V2_1
    );
    assert_eq!((v2_3.default_features)(), (v2_4.default_features)());
    assert_eq!(v2_3.feature_labels, v2_4.feature_labels);
}

/// The `Harness` trait methods `v2_4_matches_v2_3_on_every_other_surface`
/// exercises. `harness_surface_list_is_exhaustive` diffs this against the
/// trait declaration in `harness/mod.rs`, so adding a method to the trait
/// without extending the equivalence test fails here by name.
const COMPARED_SURFACES: &[&str] = &[
    "join_prompt_layers",
    "user_rules_wrapper",
    "rtk_instruction_line",
    "sandboxed_implementor_hint",
    "coordinator_cow_hint",
    "specialist_role_section",
    "commit_policy_clause",
    "role_reminder_footer",
    "ask_questions_block",
    "suggested_next_steps_block",
    "first_turn_prepend_block",
    "snapshot_line",
    "naming_tool_reference",
    "agent_naming_tool_reference",
    "naming_nudge",
    "role_reminder_prefix",
    "compose_turn_prompt",
    "stale_redrive_note",
    "dequeue_wait_note",
    "a2a_sender_note",
    "wait_duration",
    "idle_timeout_warning",
    "truncation_redrive_nudge",
    "empty_wake_redrive_nudge",
    "empty_wake_attention_reason",
    "note_images_notice",
    "attachment_reference_notice",
    "context_size_requeue_marker",
    "completion_wake",
    "group_child_line",
    "group_settlement_wake",
    "report_to_parent_wake",
    "attention_parent_wake",
    "attention_watcher_wake",
    "event_subscription_wake",
    "unblocked_section",
    "hook_wake_logs_section",
    "hook_state_dropped_warning",
    "hook_wake_message_truncated_marker",
    "hook_exec_failures_warning",
    "hook_wake_framing",
    "hook_dispatch_active_note",
    "hook_dispatch_retired_note",
    "hook_evicted_state_note",
    "hook_evicted_failed_run_notice",
    "hook_evicted_internal_error_notice",
    "hook_expired_notice",
    "hook_run_at_fired_notice",
    "hook_cancelled_from_app_notice",
    "hook_cancelled_workspace_archived_notice",
    "pr_monitor_label",
    "pr_checklist",
    "pr_diff_lines",
    "pr_change_wake",
    "pr_terminal_wake",
    "pr_monitor_cancelled_from_app_notice",
    "pr_monitor_cancelled_workspace_archived_notice",
    "delegation_first_message",
    "questions_dismissed_notice",
    "proposal_applied_notice",
    "proposal_dismissed_notice",
];

/// Method names declared in the `Harness` trait body, read from the trait's
/// source so the list above cannot silently fall behind the trait.
fn harness_trait_method_names() -> Vec<String> {
    let src = include_str!("harness/mod.rs");
    let start = src
        .find("pub(crate) trait Harness")
        .expect("Harness trait declaration");
    let body = &src[start..];
    let end = body.find("\n}\n").expect("Harness trait body end");
    body[..end]
        .lines()
        .filter_map(|line| line.trim_start().strip_prefix("fn "))
        .map(|rest| {
            rest.chars()
                .take_while(|c| c.is_ascii_alphanumeric() || *c == '_')
                .collect()
        })
        .collect()
}

#[test]
fn harness_surface_list_is_exhaustive() {
    let declared = harness_trait_method_names();
    assert!(
        !declared.is_empty(),
        "parsed no methods from the Harness trait"
    );
    let missing: Vec<&String> = declared
        .iter()
        .filter(|m| !COMPARED_SURFACES.contains(&m.as_str()))
        .collect();
    assert!(
        missing.is_empty(),
        "Harness methods not covered by v2_4_matches_v2_3_on_every_other_surface: {missing:?}"
    );
    let stale: Vec<&&str> = COMPARED_SURFACES
        .iter()
        .filter(|m| !declared.iter().any(|d| d == **m))
        .collect();
    assert!(
        stale.is_empty(),
        "COMPARED_SURFACES names methods the Harness trait no longer declares: {stale:?}"
    );
    assert_eq!(declared.len(), COMPARED_SURFACES.len());
}

/// Every `Harness` surface except `suggested_next_steps_block` renders
/// byte-identically on v2.4 and v2.3 (both forward to v1), so every existing
/// golden for those bytes stays valid across the bump. One call per trait
/// method, in declaration order; `COMPARED_SURFACES` mirrors the list.
#[test]
fn v2_4_matches_v2_3_on_every_other_surface() {
    use crate::agent_ops::ready_delta::{UnblockedReason, UnblockedTask};
    use crate::harness::{ChildSettlementParams, TurnEnvelopeParams};

    let v1 = crate::harness::resolve_entry("1.0").harness;
    let v2_3 = crate::harness::resolve_entry("2.3").harness;
    let v2_4 = crate::harness::resolve_entry("2.4").harness;

    macro_rules! same {
        ($($call:tt)*) => {{
            let a = v2_3.$($call)*;
            let b = v2_4.$($call)*;
            assert_eq!(a, b, "v2.4 differs from v2.3 on {}", stringify!($($call)*));
            assert_eq!(v1.$($call)*, b, "v2.4 differs from v1 on {}", stringify!($($call)*));
        }};
    }

    // --- System prompt assembly ---
    let parts = vec!["a".to_string(), "b".to_string()];
    same!(join_prompt_layers(&parts));
    same!(join_prompt_layers(&[]));
    same!(user_rules_wrapper("body", "src"));
    let subs = vec!["grep".to_string(), "ls".to_string()];
    same!(rtk_instruction_line(&subs));
    same!(rtk_instruction_line(&[]));
    same!(sandboxed_implementor_hint("/sb", "sb/x"));
    same!(coordinator_cow_hint());
    same!(specialist_role_section("Implement."));
    same!(commit_policy_clause());
    same!(role_reminder_footer("Implementor", Some("Stay in scope.")));
    same!(role_reminder_footer("Verifier", None));
    same!(ask_questions_block());
    // suggested_next_steps_block is the one surface v2.4 changes; it is
    // pinned by `golden_v2_4_suggested_next_steps_block` above.
    for auto_commit in [false, true] {
        assert_ne!(
            v2_3.suggested_next_steps_block(auto_commit),
            v2_4.suggested_next_steps_block(auto_commit)
        );
    }

    // --- Turn envelope ---
    same!(first_turn_prepend_block("go"));
    same!(snapshot_line("{\"time\":\"t\"}"));
    for provider in ["claude-code", "codex", "augment", "unknown-provider"] {
        same!(naming_tool_reference(provider));
        same!(agent_naming_tool_reference(provider));
    }
    same!(naming_nudge(Some("agent-ref"), Some("ws-ref")));
    same!(naming_nudge(Some("agent-ref"), None));
    same!(naming_nudge(None, Some("ws-ref")));
    same!(naming_nudge(None, None));
    same!(role_reminder_prefix("Implementor", "Stay in scope."));
    let full = TurnEnvelopeParams {
        first_turn_prepend: Some("<system>prepend</system>"),
        snapshot_line: Some("current ws.agent.snapshot() => {}"),
        stdin_context: Some("ctx"),
        naming_nudge: Some("<system>name it</system>"),
        role_reminder: Some("[Role Reminder: x]"),
        body: "hello",
    };
    let bare = TurnEnvelopeParams {
        first_turn_prepend: None,
        snapshot_line: None,
        stdin_context: None,
        naming_nudge: None,
        role_reminder: None,
        body: "hello",
    };
    same!(compose_turn_prompt(&full));
    same!(compose_turn_prompt(&bare));

    // --- Queue notes and warnings ---
    same!(stale_redrive_note("2026-01-02T03:04:05Z"));
    same!(dequeue_wait_note("2026-01-02T03:04:05Z", "5 minutes"));
    same!(a2a_sender_note(Some("Coordinator"), "agent-1"));
    same!(a2a_sender_note(None, "agent-1"));
    for secs in [0, 59, 60, 3599, 3600, 90_000] {
        same!(wait_duration(secs));
    }
    same!(idle_timeout_warning("30 minutes"));
    same!(truncation_redrive_nudge());
    same!(empty_wake_redrive_nudge());
    same!(empty_wake_attention_reason());

    // --- Prompt notices ---
    for n in [1, 2] {
        same!(note_images_notice(n));
    }
    same!(attachment_reference_notice(
        "a.png",
        Some("image/png"),
        Some(1024),
        "att-1"
    ));
    same!(attachment_reference_notice("a.bin", None, None, "att-2"));
    same!(context_size_requeue_marker(120_000));

    // --- Completion / group / watch wakes ---
    let settled = ChildSettlementParams {
        child_id: "agent-child",
        child_of_recipient: true,
        agent_name: Some("Worker"),
        event_type: intent_core::events::AGENT_IDLE,
        completion_report: Some("Done."),
        last_response_summary: Some("summary"),
        error: None,
        attention: None,
        stall: None,
        report_already_delivered: false,
    };
    let failed = ChildSettlementParams {
        child_id: "agent-child",
        child_of_recipient: false,
        agent_name: None,
        event_type: intent_core::events::AGENT_FAILED,
        completion_report: None,
        last_response_summary: None,
        error: Some("boom"),
        attention: Some(("blocker", "no creds")),
        stall: Some(("Wire adapter", "in_progress")),
        report_already_delivered: true,
    };
    for params in [&settled, &failed] {
        for retired in [false, true] {
            same!(completion_wake(params, retired));
        }
        same!(group_child_line(params));
    }
    let child_lines = vec!["- a".to_string(), "- b".to_string()];
    for partial in [false, true] {
        same!(group_settlement_wake(2, partial, &child_lines));
    }
    for consumed in [false, true] {
        same!(report_to_parent_wake(
            "Worker",
            "agent-child",
            "Report body",
            consumed
        ));
    }
    for kind in ["blocker", "discussion"] {
        same!(attention_parent_wake(
            "Worker",
            "agent-child",
            kind,
            "reason"
        ));
        for grouped in [false, true] {
            same!(attention_watcher_wake(
                "Worker",
                "agent-child",
                kind,
                "reason",
                grouped
            ));
        }
    }
    same!(event_subscription_wake(3, &["file:*", "task:*"]));
    let delta = vec![
        UnblockedTask {
            note_id: "n-1".to_string(),
            title: "Wire the adapter".to_string(),
            reason: UnblockedReason::DepsSatisfied,
            attention: None,
        },
        UnblockedTask {
            note_id: "n-2".to_string(),
            title: "Ship docs".to_string(),
            reason: UnblockedReason::ConflictCleared,
            attention: Some(intent_core::TaskStatus::Blocked),
        },
    ];
    for multiple in [false, true] {
        same!(unblocked_section(&delta, multiple));
    }
    same!(unblocked_section(&[], false));

    // --- Hook wakes and notices ---
    same!(hook_wake_logs_section("msg", Some("log line")));
    same!(hook_wake_logs_section("msg", None));
    same!(hook_state_dropped_warning(20_000, 16_384));
    same!(hook_wake_message_truncated_marker(100, 600, 500));
    same!(hook_exec_failures_warning(&["curl exited 7"], 1));
    same!(hook_exec_failures_warning(&["a", "b"], 5));
    same!(hook_wake_framing(
        "pr-watch",
        "PR moved",
        Some("state note")
    ));
    same!(hook_wake_framing("pr-watch", "PR moved", None));
    same!(hook_dispatch_active_note(Some("2026-01-03T00:00:00Z")));
    same!(hook_dispatch_active_note(None));
    same!(hook_dispatch_retired_note("hook-1"));
    same!(hook_evicted_state_note("hook-1"));
    same!(hook_evicted_failed_run_notice("pr-watch", "boom"));
    same!(hook_evicted_internal_error_notice("pr-watch", "boom"));
    for perpetual in [false, true] {
        same!(hook_expired_notice("pr-watch", "hook-1", perpetual, 12, 3));
    }
    same!(hook_run_at_fired_notice(
        "pr-watch",
        "hook-1",
        "2026-01-03T00:00:00Z"
    ));
    same!(hook_cancelled_from_app_notice());
    same!(hook_cancelled_workspace_archived_notice());

    // --- PR monitor wakes and notices ---
    same!(pr_monitor_label("o", "r", 42));
    let open = crate::v1_goldens::pr_snapshot("open");
    let merged = crate::v1_goldens::pr_snapshot("merged");
    same!(pr_checklist(&open));
    same!(pr_checklist(&merged));
    same!(pr_diff_lines(&open, &merged));
    same!(pr_diff_lines(&open, &open));
    let changes = vec!["new approval (0 → 1 approving)".to_string()];
    same!(pr_change_wake("o/r#42", &changes, &open));
    same!(pr_terminal_wake("o/r#42", &changes, &merged));
    same!(pr_monitor_cancelled_from_app_notice("o/r#42"));
    same!(pr_monitor_cancelled_workspace_archived_notice("o/r#42"));

    // --- Other conversation-reaching strings ---
    same!(delegation_first_message(Some("body"), "Title", "note-1"));
    same!(delegation_first_message(None, "Title", "note-1"));
    for count in [1, 3] {
        same!(questions_dismissed_notice(count));
    }
    same!(proposal_applied_notice("Title", Some("detail")));
    same!(proposal_applied_notice("Title", None));
    same!(proposal_dismissed_notice("Title"));
}
