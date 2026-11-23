use serde_json::json;

use super::*;

/// The recognition table of PROTOCOL §5.5 "Conversation goal".
#[test]
fn parse_command_follows_the_recognition_table() {
    let cases: &[(&str, Option<&str>)] = &[
        ("/goal x", Some("x")),
        ("  /goal\nx", Some("x")),
        ("/goal", Some("")),
        ("  /goal  \n ", Some("")),
        (
            "/goal\tShip it.\nDone when green.  ",
            Some("Ship it.\nDone when green."),
        ),
        ("/goal   a\n\n  b  ", Some("a\n\n  b")),
        ("/Goal x", None),
        ("/GOAL x", None),
        ("/goals x", None),
        ("/goal:", None),
        ("/goal: x", None),
        ("please /goal x", None),
        ("x /goal", None),
        ("", None),
        ("goal x", None),
    ];
    for (content, expected) in cases {
        assert_eq!(parse_command(content), *expected, "{content:?}");
    }
}

#[test]
fn untyped_metadata_keeps_a_send_plain() {
    assert!(metadata_is_plain(None));
    assert!(metadata_is_plain(Some(&json!(null))));
    assert!(metadata_is_plain(Some(&json!({}))));
    assert!(metadata_is_plain(Some(&json!({
        "mentionTokens": ["@src"],
        "contextReferences": [],
        "userAppMessageId": "app-1",
        "fromPrincipalId": "p-1",
    }))));
    assert!(metadata_is_plain(Some(
        &json!({ "source": "suggested_prompt" })
    )));
    assert!(metadata_is_plain(Some(&json!({ "answers": [] }))));
    for typed in [
        json!({ "type": "question_answers", "answeredQuestionsMessageId": "m-1" }),
        json!({ "type": "wizard_submission" }),
        json!({ "type": null }),
        json!({ "mentionTokens": [], "type": "anything" }),
        json!("text"),
        json!([]),
    ] {
        assert!(!metadata_is_plain(Some(&typed)), "{typed}");
    }
}

#[test]
fn normalize_trims_removes_and_caps() {
    assert_eq!(normalize(None).unwrap(), None);
    assert_eq!(normalize(Some("   \n ")).unwrap(), None);
    assert_eq!(normalize(Some("  a\nb ")).unwrap().as_deref(), Some("a\nb"));
    let at_cap = "é".repeat(MAX_CHARS);
    assert_eq!(normalize(Some(&at_cap)).unwrap(), Some(at_cap.clone()));
    let padded = format!("  {at_cap}  ");
    assert_eq!(normalize(Some(&padded)).unwrap(), Some(at_cap));
    let over = "x".repeat(MAX_CHARS + 1);
    match normalize(Some(&over)) {
        Err(Error::ConversationGoal(GoalError::TooLong)) => {}
        other => panic!("expected goal-too-long, got {other:?}"),
    }
}

#[test]
fn write_action_covers_every_transition() {
    assert_eq!(write_action(None, Some("a")), GoalAction::Set);
    assert_eq!(write_action(Some("a"), Some("b")), GoalAction::Updated);
    assert_eq!(write_action(Some("a"), Some("a")), GoalAction::Unchanged);
    assert_eq!(write_action(Some("a"), None), GoalAction::Cleared);
    assert_eq!(write_action(None, None), GoalAction::Unchanged);
    for (action, writes) in [
        (GoalAction::Set, true),
        (GoalAction::Updated, true),
        (GoalAction::Cleared, true),
        (GoalAction::Unchanged, false),
        (GoalAction::Shown, false),
    ] {
        assert_eq!(action.writes(), writes, "{action:?}");
    }
}

#[test]
fn notice_rows_carry_the_documented_text_and_metadata() {
    let cases = [
        (GoalAction::Set, Some("g"), "Goal set: g"),
        (GoalAction::Updated, Some("g"), "Goal updated: g"),
        (GoalAction::Unchanged, Some("g"), "Goal unchanged: g"),
        (GoalAction::Shown, Some("g"), "Current goal: g"),
        (GoalAction::Shown, None, NO_GOAL_NOTICE),
        (GoalAction::Cleared, None, "Goal removed."),
    ];
    for (action, goal, text) in cases {
        assert_eq!(notice_text(action, goal), text);
        assert_eq!(
            notice_content(action, goal),
            json!([{ "type": "text", "text": text }])
        );
    }
    assert_eq!(
        notice_metadata(GoalAction::Set, Some("g"), None),
        json!({ "type": "goal_command", "action": "set", "goal": "g" })
    );
    assert_eq!(
        notice_metadata(GoalAction::Cleared, None, Some("p-1")),
        json!({ "type": "goal_command", "action": "cleared", "goal": null, "fromPrincipalId": "p-1" })
    );
}

#[test]
fn goal_errors_carry_the_documented_message_and_data() {
    assert_eq!(
        GoalError::TooLong.message(),
        "goal is too long (max 1000 characters)"
    );
    assert_eq!(
        GoalError::TooLong.data(),
        Some(json!({ "code": "goal-too-long", "maxLength": 1000 }))
    );
    assert_eq!(
        GoalError::Attachments.message(),
        "/goal does not take attachments"
    );
    assert_eq!(
        GoalError::Attachments.data(),
        Some(json!({ "code": "goal-attachments-not-allowed" }))
    );
    assert_eq!(GoalError::AgentNotFound.message(), "agent not found");
    assert_eq!(GoalError::AgentNotFound.data(), None);
    assert_eq!(GoalError::AgentRetired.message(), "agent is retired");
    assert_eq!(
        GoalError::AgentRetired.data(),
        Some(json!({ "code": "agent-retired" }))
    );
    let err = Error::ConversationGoal(GoalError::TooLong);
    assert_eq!(err.code(), -32602);
    assert_eq!(err.to_string(), TOO_LONG_ERROR);
}

#[test]
fn set_by_snapshot_caps_names_on_a_char_boundary_and_serializes_nulls() {
    use super::{GoalSetBy, SET_BY_NAME_CAP_BYTES};
    let mut principal = crate::Principal {
        id: crate::PrincipalId("p-1".into()),
        identity: None,
        github_user_id: None,
        login: None,
        display_name: Some(format!("a{}", "\u{1F3AF}".repeat(SET_BY_NAME_CAP_BYTES))),
        avatar_url: None,
        is_primary: false,
        created_at: String::new(),
        updated_at: String::new(),
    };
    let snapshot = GoalSetBy::snapshot(&principal);
    let display = snapshot.display_name.as_deref().unwrap();
    assert!(display.len() <= SET_BY_NAME_CAP_BYTES);
    assert_eq!(display.len(), 1 + (SET_BY_NAME_CAP_BYTES - 1) / 4 * 4);
    assert_eq!(
        serde_json::to_value(&snapshot).unwrap()["login"],
        serde_json::Value::Null
    );

    principal.display_name = Some("Ada".into());
    principal.login = Some("ada".into());
    assert_eq!(
        serde_json::to_value(GoalSetBy::snapshot(&principal)).unwrap(),
        serde_json::json!({ "principalId": "p-1", "displayName": "Ada", "login": "ada" })
    );
}
