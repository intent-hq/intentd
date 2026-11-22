//! Conversation goal (PROTOCOL §5.5 "Conversation goal"): `agent.setGoal`,
//! the `/goal` command path and the per-turn goal resolver, against a real
//! store and event bus.
use std::time::Duration;

use intent_core::conversation_goal::{GoalError, MAX_CHARS, NO_GOAL_NOTICE};
use intent_core::events::{AGENT_MESSAGE, AGENT_UPDATED};
use intent_core::{AgentId, Error, Event, WorkspaceId};
use serde_json::{json, Value};

use super::tests::{create_agent, setup_with_bus, workspace};
use crate::{EventBus, Services, Subscription, SubscriptionFilter};

/// `(request, action, goal after, notice text when it writes)`.
type SetGoalCase = (
    Option<&'static str>,
    &'static str,
    Option<&'static str>,
    Option<&'static str>,
);

const OLD: &str = "2020-01-01T00:00:00.000Z";

/// A fresh agent whose activity timestamp is pinned to [`OLD`], so a test can
/// prove a goal write never advances it.
async fn agent_with_old_timestamp(svc: &Services, ws: &WorkspaceId) -> AgentId {
    let id = create_agent(svc, ws, "Goal").await;
    let mut session = svc
        .store
        .get_agent_session_with_transcript(&id)
        .await
        .unwrap();
    session.updated_at = OLD.to_string();
    svc.store.update_agent_session(ws, &session).await.unwrap();
    id
}

fn subscribe(bus: &EventBus) -> Subscription {
    bus.subscribe(SubscriptionFilter {
        event_types: vec![AGENT_UPDATED.to_string(), AGENT_MESSAGE.to_string()],
        ..Default::default()
    })
}

/// Every event delivered within a short quiet window.
async fn drain(sub: &mut Subscription) -> Vec<Event> {
    let mut out = Vec::new();
    while let Ok(Some(batch)) = tokio::time::timeout(
        intentd_test_support::test_timeout(Duration::from_millis(300)),
        sub.recv(),
    )
    .await
    {
        out.extend(batch);
    }
    out
}

fn updates(events: &[Event]) -> Vec<Value> {
    events
        .iter()
        .filter(|e| e.event_type == AGENT_UPDATED)
        .map(|e| e.data.clone())
        .collect()
}

fn message_ids(events: &[Event]) -> Vec<String> {
    events
        .iter()
        .filter(|e| e.event_type == AGENT_MESSAGE)
        .filter_map(|e| {
            e.data
                .get("messageId")
                .and_then(Value::as_str)
                .map(str::to_string)
        })
        .collect()
}

async fn stored_goal(svc: &Services, id: &AgentId) -> Option<String> {
    svc.agent_get_session_op(id.clone()).await.unwrap().goal
}

async fn rows(svc: &Services, id: &AgentId) -> Vec<intent_core::AgentMessage> {
    svc.store
        .get_agent_session_with_transcript(id)
        .await
        .unwrap()
        .messages
}

/// Assert `id`'s last transcript row is the `goal_command` notice `notice_id`
/// with `text` and `metadata`.
async fn assert_notice(svc: &Services, id: &AgentId, notice_id: &str, text: &str, metadata: Value) {
    let rows = rows(svc, id).await;
    let row = rows.last().expect("notice row");
    assert_eq!(row.id, notice_id);
    assert_eq!(row.role, "system");
    assert_eq!(row.content, json!([{ "type": "text", "text": text }]));
    assert_eq!(row.metadata.as_ref(), Some(&metadata));
}

#[intent_test_macros::daemon_test]
async fn set_goal_follows_the_action_table_without_touching_activity() {
    let (_t, svc, ws, bus) = setup_with_bus().await;
    let id = agent_with_old_timestamp(&svc, &ws).await;
    let mut sub = subscribe(&bus);
    let before_rows = rows(&svc, &id).await.len();

    let cases: [SetGoalCase; 6] = [
        (
            Some("  Ship it.\nDone when green.  "),
            "set",
            Some("Ship it.\nDone when green."),
            Some("Goal set: Ship it.\nDone when green."),
        ),
        (
            Some("Ship it.\nDone when green."),
            "unchanged",
            Some("Ship it.\nDone when green."),
            None,
        ),
        (
            Some("Second"),
            "updated",
            Some("Second"),
            Some("Goal updated: Second"),
        ),
        (Some("   "), "cleared", None, Some("Goal removed.")),
        (None, "unchanged", None, None),
        (Some(""), "unchanged", None, None),
    ];
    let mut writes = 0;
    for (request, action, goal, notice) in cases {
        let result = svc
            .agent_set_goal_op(Some(ws.clone()), id.clone(), request.map(str::to_string))
            .await
            .unwrap();
        assert_eq!(result["action"], action, "{request:?}");
        assert_eq!(result["goal"], json!(goal), "{request:?}");
        assert_eq!(stored_goal(&svc, &id).await.as_deref(), goal);
        let events = drain(&mut sub).await;
        if let Some(text) = notice {
            writes += 1;
            let notice_id = result["noticeMessageId"].as_str().expect("noticeMessageId");
            assert_notice(
                &svc,
                &id,
                notice_id,
                text,
                json!({ "type": "goal_command", "action": action, "goal": goal }),
            )
            .await;
            assert_eq!(
                updates(&events),
                vec![json!({ "agentId": id.0, "goal": goal })]
            );
            assert_eq!(message_ids(&events), vec![notice_id.to_string()]);
        } else {
            assert!(result.get("noticeMessageId").is_none(), "{result}");
            assert!(events.is_empty(), "{events:?}");
        }
        let session = svc.agent_get_session_op(id.clone()).await.unwrap();
        assert_eq!(
            session.updated_at, OLD,
            "a goal write never advances updatedAt"
        );
    }
    assert_eq!(rows(&svc, &id).await.len(), before_rows + writes);
}

#[intent_test_macros::daemon_test]
async fn goal_is_served_on_projections_only_while_set() {
    let (_t, svc, ws, _bus) = setup_with_bus().await;
    let id = create_agent(&svc, &ws, "Goal").await;
    let lite = svc
        .agent_get_op(id.clone(), Some(ws.clone()))
        .await
        .unwrap();
    assert!(serde_json::to_value(&lite).unwrap().get("goal").is_none());
    let session =
        serde_json::to_value(svc.agent_get_session_op(id.clone()).await.unwrap()).unwrap();
    assert!(session.get("goal").is_none());

    svc.agent_set_goal_op(None, id.clone(), Some("Done".into()))
        .await
        .unwrap();
    let lite = svc
        .agent_get_op(id.clone(), Some(ws.clone()))
        .await
        .unwrap();
    assert_eq!(serde_json::to_value(&lite).unwrap()["goal"], "Done");
    let session =
        serde_json::to_value(svc.agent_get_session_op(id.clone()).await.unwrap()).unwrap();
    assert_eq!(session["goal"], "Done");
    assert_eq!(
        svc.agent_conversation_goal(&id).await.as_deref(),
        Some("Done")
    );
}

/// A full-row session write that read the row before the goal was set must
/// not revert it (the goal is excluded from full-row writes).
#[intent_test_macros::daemon_test]
async fn full_row_session_writes_never_clobber_the_goal() {
    let (_t, svc, ws, _bus) = setup_with_bus().await;
    let id = create_agent(&svc, &ws, "Goal").await;
    let mut stale = svc
        .store
        .get_agent_session_with_transcript(&id)
        .await
        .unwrap();
    svc.agent_set_goal_op(None, id.clone(), Some("Keep me".into()))
        .await
        .unwrap();
    stale.name = "Renamed".into();
    assert_eq!(stale.goal, None);
    svc.store.update_agent_session(&ws, &stale).await.unwrap();
    assert_eq!(stored_goal(&svc, &id).await.as_deref(), Some("Keep me"));
}

#[intent_test_macros::daemon_test]
async fn the_store_writer_is_compare_and_set() {
    let (_t, svc, ws, _bus) = setup_with_bus().await;
    let id = create_agent(&svc, &ws, "Goal").await;
    assert!(svc
        .store
        .set_agent_goal(&ws, &id, None, Some("a"))
        .await
        .unwrap());
    // A writer that observed "no goal" lost the race and must not write.
    assert!(!svc
        .store
        .set_agent_goal(&ws, &id, None, Some("b"))
        .await
        .unwrap());
    // Writing the stored value is a no-op.
    assert!(!svc
        .store
        .set_agent_goal(&ws, &id, Some("a"), Some("a"))
        .await
        .unwrap());
    assert!(svc
        .store
        .set_agent_goal(&ws, &id, Some("a"), None)
        .await
        .unwrap());
    assert_eq!(stored_goal(&svc, &id).await, None);
    let other = WorkspaceId::new();
    assert!(matches!(
        svc.store.set_agent_goal(&other, &id, None, Some("x")).await,
        Err(Error::NotFound(_))
    ));
}

/// Racing writers from the same old value: exactly one answers `set`; the
/// other recomputes against the winner's goal (`updated` or `unchanged`).
#[intent_test_macros::daemon_test]
async fn racing_writers_cannot_both_answer_set() {
    let (_t, svc, ws, _bus) = setup_with_bus().await;
    let id = create_agent(&svc, &ws, "Goal").await;
    for round in 0..5 {
        svc.agent_set_goal_op(None, id.clone(), None).await.unwrap();
        let (a, b) = tokio::join!(
            svc.agent_set_goal_op(None, id.clone(), Some(format!("a{round}"))),
            svc.agent_set_goal_op(None, id.clone(), Some(format!("b{round}"))),
        );
        let actions = [a.unwrap()["action"].clone(), b.unwrap()["action"].clone()];
        assert_eq!(
            actions.iter().filter(|a| *a == "set").count(),
            1,
            "{actions:?}"
        );
        assert!(actions.iter().any(|a| a == "updated"), "{actions:?}");
    }
}

#[intent_test_macros::daemon_test]
async fn goal_command_sets_shows_and_never_delivers() {
    let (_t, svc, ws, bus) = setup_with_bus().await;
    let id = agent_with_old_timestamp(&svc, &ws).await;
    let status_before = svc.agent_get_session_op(id.clone()).await.unwrap().status;
    let mut sub = subscribe(&bus);

    let cases = [
        ("", "shown", None, NO_GOAL_NOTICE.to_string()),
        (
            "Fix the bug",
            "set",
            Some("Fix the bug"),
            "Goal set: Fix the bug".to_string(),
        ),
        (
            "Fix the bug",
            "unchanged",
            Some("Fix the bug"),
            "Goal unchanged: Fix the bug".to_string(),
        ),
        (
            "",
            "shown",
            Some("Fix the bug"),
            "Current goal: Fix the bug".to_string(),
        ),
        (
            "Fix both bugs",
            "updated",
            Some("Fix both bugs"),
            "Goal updated: Fix both bugs".to_string(),
        ),
    ];
    for (statement, action, goal, text) in cases {
        let rows_before = rows(&svc, &id).await.len();
        let result = svc
            .agent_goal_command_op(Some(ws.clone()), id.clone(), statement.into())
            .await
            .unwrap();
        let notice_id = result["command"]["noticeMessageId"]
            .as_str()
            .unwrap()
            .to_string();
        assert_eq!(
            result,
            json!({
                "success": true,
                "queued": false,
                "command": { "name": "goal", "action": action, "goal": goal, "noticeMessageId": notice_id },
            })
        );
        // One notice row, no user row.
        assert_eq!(rows(&svc, &id).await.len(), rows_before + 1);
        assert_notice(
            &svc,
            &id,
            &notice_id,
            &text,
            json!({ "type": "goal_command", "action": action, "goal": goal }),
        )
        .await;
        let events = drain(&mut sub).await;
        assert_eq!(message_ids(&events), vec![notice_id.clone()]);
        let expected_updates = if matches!(action, "set" | "updated") {
            vec![json!({ "agentId": id.0, "goal": goal })]
        } else {
            Vec::new()
        };
        assert_eq!(updates(&events), expected_updates, "{action}");
    }
    let session = svc.agent_get_session_op(id.clone()).await.unwrap();
    assert_eq!(session.updated_at, OLD);
    assert_eq!(session.status, status_before, "no turn started");
    let queue = svc
        .agent_get_queue_op(id.clone(), Some(ws.clone()))
        .await
        .unwrap();
    assert_eq!(queue["queue"], json!([]), "nothing enqueued: {queue}");
}

#[intent_test_macros::daemon_test]
async fn refusals_change_nothing() {
    let (_t, svc, ws, _bus) = setup_with_bus().await;
    let id = create_agent(&svc, &ws, "Goal").await;
    svc.agent_set_goal_op(None, id.clone(), Some("Keep".into()))
        .await
        .unwrap();
    let rows_before = rows(&svc, &id).await.len();
    let too_long = "x".repeat(MAX_CHARS + 1);

    let err = svc
        .agent_set_goal_op(None, id.clone(), Some(too_long.clone()))
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::ConversationGoal(GoalError::TooLong)),
        "{err:?}"
    );
    let err = svc
        .agent_goal_command_op(None, id.clone(), too_long)
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::ConversationGoal(GoalError::TooLong)),
        "{err:?}"
    );
    // At the cap (after trimming) is accepted.
    let at_cap = format!("  {}  ", "é".repeat(MAX_CHARS));
    svc.agent_set_goal_op(None, id.clone(), Some(at_cap))
        .await
        .unwrap();
    svc.agent_set_goal_op(None, id.clone(), Some("Keep".into()))
        .await
        .unwrap();
    let rows_before = rows_before + 2;

    let unknown = AgentId::from("agent-does-not-exist");
    for err in [
        svc.agent_set_goal_op(None, unknown.clone(), Some("x".into()))
            .await
            .unwrap_err(),
        svc.agent_goal_command_op(None, unknown, "x".into())
            .await
            .unwrap_err(),
    ] {
        assert!(
            matches!(err, Error::ConversationGoal(GoalError::AgentNotFound)),
            "{err:?}"
        );
    }

    // The agent "in" another workspace is not found there.
    let other = WorkspaceId::new();
    svc.store
        .insert_workspace(&workspace(&other))
        .await
        .unwrap();
    let err = svc
        .agent_set_goal_op(Some(other), id.clone(), Some("x".into()))
        .await
        .unwrap_err();
    assert!(
        matches!(err, Error::ConversationGoal(GoalError::AgentNotFound)),
        "{err:?}"
    );

    // One agent cannot set another agent's goal.
    let err = intent_core::with_caller(
        intent_core::Caller::Agent {
            agent_id: AgentId::from("agent-other"),
        },
        svc.agent_set_goal_op(None, id.clone(), Some("x".into())),
    )
    .await
    .unwrap_err();
    assert!(matches!(err, Error::Forbidden(_)), "{err:?}");

    assert_eq!(rows(&svc, &id).await.len(), rows_before);
    assert_eq!(stored_goal(&svc, &id).await.as_deref(), Some("Keep"));

    // Retired: refused, and the goal survives retire / restore.
    svc.agent_retire_op(id.clone(), Some(ws.clone()), None)
        .await
        .unwrap();
    for err in [
        svc.agent_set_goal_op(None, id.clone(), None)
            .await
            .unwrap_err(),
        svc.agent_goal_command_op(None, id.clone(), String::new())
            .await
            .unwrap_err(),
    ] {
        assert!(
            matches!(err, Error::ConversationGoal(GoalError::AgentRetired)),
            "{err:?}"
        );
    }
    assert_eq!(stored_goal(&svc, &id).await.as_deref(), Some("Keep"));
}

/// `agent.update` keeps rejecting `goal` like any key outside its whitelist.
#[intent_test_macros::daemon_test]
async fn agent_update_rejects_goal() {
    let (_t, svc, ws, _bus) = setup_with_bus().await;
    let id = create_agent(&svc, &ws, "Goal").await;
    let err = svc
        .agent_update_op(id.clone(), json!({ "goal": "x" }))
        .await
        .unwrap_err();
    assert_eq!(err.code(), -32602, "{err:?}");
    assert_eq!(stored_goal(&svc, &id).await, None);
}

/// The goal is a store column: it survives reopening the database.
#[intent_test_macros::daemon_test]
async fn goal_survives_a_store_reopen() {
    let (tmp, svc, ws, _bus) = setup_with_bus().await;
    let id = create_agent(&svc, &ws, "Goal").await;
    svc.agent_set_goal_op(None, id.clone(), Some("Persist me".into()))
        .await
        .unwrap();
    drop(svc);
    let store = intent_store::Store::open(&tmp.path).await.unwrap();
    let session = store.get_agent_session_summary(&id).await.unwrap();
    assert_eq!(session.goal.as_deref(), Some("Persist me"));
}
