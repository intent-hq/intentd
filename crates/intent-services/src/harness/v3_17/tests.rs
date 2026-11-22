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
    assert_eq!(ENTRY.version, intent_core::CURRENT_HARNESS_VERSION);
    assert_eq!(intent_core::STAGED_HARNESS_VERSION, None);
    assert_eq!(
        crate::harness::latest_entry().version,
        intent_core::CURRENT_HARNESS_VERSION
    );
}
