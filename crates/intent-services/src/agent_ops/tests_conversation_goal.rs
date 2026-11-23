//! Conversation goal (PROTOCOL §5.5 "Conversation goal"): `agent.setGoal`,
//! the `/goal` command path and the per-turn goal resolver, against a real
//! store and event bus.
use std::time::Duration;

use intent_core::conversation_goal::{GoalError, MAX_CHARS, NO_GOAL_NOTICE};
use intent_core::events::{AGENT_MESSAGE, AGENT_UPDATED};
use intent_core::{
    with_caller, AgentId, Caller, Error, Event, Principal, PrincipalId, WorkspaceApi, WorkspaceId,
};
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
                vec![json!({ "agentId": id.0, "goal": goal, "goalSetBy": null })]
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
        .set_agent_goal(&ws, &id, None, Some("a"), None)
        .await
        .unwrap());
    // A writer that observed "no goal" lost the race and must not write.
    assert!(!svc
        .store
        .set_agent_goal(&ws, &id, None, Some("b"), None)
        .await
        .unwrap());
    // Writing the stored value is a no-op.
    assert!(!svc
        .store
        .set_agent_goal(&ws, &id, Some("a"), Some("a"), None)
        .await
        .unwrap());
    assert!(svc
        .store
        .set_agent_goal(&ws, &id, Some("a"), None, None)
        .await
        .unwrap());
    assert_eq!(stored_goal(&svc, &id).await, None);
    let other = WorkspaceId::new();
    assert!(matches!(
        svc.store
            .set_agent_goal(&other, &id, None, Some("x"), None)
            .await,
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
            vec![json!({ "agentId": id.0, "goal": goal, "goalSetBy": null })]
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

/// A retired workspace is read-only (§5.1): `agent.setGoal` and a `/goal`
/// command (which `agent.sendMessage` would otherwise let through, since a
/// plain user message revives the workspace) are refused, and nothing
/// changes or revives.
#[intent_test_macros::daemon_test]
async fn retired_workspace_refuses_goal_writes() {
    let (_t, svc, ws, _bus) = setup_with_bus().await;
    let id = create_agent(&svc, &ws, "Goal").await;
    svc.agent_set_goal_op(None, id.clone(), Some("Keep".into()))
        .await
        .unwrap();
    svc.archive_workspace(ws.clone(), None)
        .await
        .expect("archive");
    assert!(svc
        .store
        .retire_workspace_if_archived(&ws, &intent_core::now_iso())
        .await
        .unwrap());
    let rows_before = rows(&svc, &id).await.len();
    for err in [
        svc.agent_set_goal_op(Some(ws.clone()), id.clone(), Some("New".into()))
            .await
            .unwrap_err(),
        svc.agent_goal_command_op(Some(ws.clone()), id.clone(), "New".into())
            .await
            .unwrap_err(),
        svc.agent_goal_command_op(None, id.clone(), String::new())
            .await
            .unwrap_err(),
    ] {
        assert!(matches!(err, Error::WorkspaceRetired { .. }), "{err:?}");
    }
    assert_eq!(stored_goal(&svc, &id).await.as_deref(), Some("Keep"));
    assert_eq!(rows(&svc, &id).await.len(), rows_before);
    let row = svc.store.get_workspace(&ws).await.unwrap();
    assert!(row.retired_at.is_some(), "nothing revived the workspace");
}

/// Goal writes and notices touch no attention, question or last-message
/// state (§5.5 "No side effects").
#[intent_test_macros::daemon_test]
async fn goal_writes_leave_attention_and_last_message_alone() {
    let (_t, svc, ws, _bus) = setup_with_bus().await;
    let id = create_agent(&svc, &ws, "Goal").await;
    svc.store
        .set_attention_request(
            &ws,
            &id,
            "discussion",
            "Need a decision",
            &intent_core::now_iso(),
        )
        .await
        .unwrap();
    let before = svc.agent_get_session_op(id.clone()).await.unwrap();
    let lite_before = serde_json::to_value(
        svc.agent_get_op(id.clone(), Some(ws.clone()))
            .await
            .unwrap(),
    )
    .unwrap();
    svc.agent_set_goal_op(None, id.clone(), Some("A".into()))
        .await
        .unwrap();
    svc.agent_goal_command_op(None, id.clone(), "B".into())
        .await
        .unwrap();
    svc.agent_goal_command_op(None, id.clone(), String::new())
        .await
        .unwrap();
    svc.agent_set_goal_op(None, id.clone(), None).await.unwrap();
    let after = svc.agent_get_session_op(id.clone()).await.unwrap();
    assert_eq!(after.attention_request_kind, before.attention_request_kind);
    assert_eq!(
        after.attention_request_reason,
        before.attention_request_reason
    );
    assert_eq!(
        after.attention_request_timestamp,
        before.attention_request_timestamp
    );
    assert_eq!(after.status, before.status);
    assert_eq!(after.updated_at, before.updated_at);
    assert_eq!(
        after.metadata, before.metadata,
        "no pending-question / seen marker change"
    );
    let lite_after = serde_json::to_value(
        svc.agent_get_op(id.clone(), Some(ws.clone()))
            .await
            .unwrap(),
    )
    .unwrap();
    for key in [
        "lastMessageRole",
        "lastMessageId",
        "lastUserMessage",
        "lastAgentResponse",
        "unread",
        "attentionRequest",
    ] {
        assert_eq!(lite_after.get(key), lite_before.get(key), "{key}");
    }
}

/// Regression (intentd#3683 review): an out-of-turn goal notice racing a
/// turn's assistant append must not take its transcript `seq`. The store
/// allocates `seq` inside the insert, so every concurrent append lands with
/// a unique, gap-free `seq`.
#[intent_test_macros::daemon_test]
async fn goal_notices_never_steal_a_concurrent_appends_seq() {
    let (_t, svc, ws, _bus) = setup_with_bus().await;
    let id = create_agent(&svc, &ws, "Goal").await;
    let before = rows(&svc, &id).await.len();
    const N: usize = 24;
    let mut set = tokio::task::JoinSet::new();
    for i in 0..N {
        let (svc_a, id_a) = (svc.clone(), id.clone());
        set.spawn(intent_core::with_caller(
            intent_core::Caller::Daemon,
            async move {
                svc_a
                    .store
                    .append_agent_message_with_metadata(
                        &id_a,
                        "assistant",
                        &json!([{ "type": "text", "text": format!("answer {i}") }]),
                        None,
                        &intent_core::now_iso(),
                    )
                    .await
                    .map(|_| ())
            },
        ));
        let (svc_n, id_n) = (svc.clone(), id.clone());
        set.spawn(intent_core::with_caller(
            intent_core::Caller::Daemon,
            async move {
                svc_n
                    // `/goal` alone: a notice row with no goal write, so this
                    // races only the transcript `seq`, not the goal's own
                    // compare-and-set (covered by `racing_writers_cannot_both_answer_set`).
                    .agent_goal_command_op(None, id_n, String::new())
                    .await
                    .map(|_| ())
            },
        ));
    }
    while let Some(joined) = set.join_next().await {
        joined
            .expect("task")
            .expect("every concurrent append lands");
    }
    let rows = rows(&svc, &id).await;
    assert_eq!(rows.len(), before + 2 * N);
    let seqs: Vec<i64> = rows.iter().map(|m| m.seq).collect();
    let expected: Vec<i64> = (0..i64::try_from(rows.len()).unwrap()).collect();
    assert_eq!(seqs, expected, "unique, gap-free seq");
}

/// A collaborator principal with a profile, added to `ws`.
async fn member(svc: &Services, ws: &WorkspaceId, login: &str, display: &str) -> PrincipalId {
    let id = PrincipalId::new();
    svc.store
        .upsert_principal(&Principal {
            id: id.clone(),
            identity: None,
            github_user_id: None,
            login: Some(login.into()),
            display_name: Some(display.into()),
            avatar_url: None,
            is_primary: false,
            created_at: intent_core::now_iso(),
            updated_at: intent_core::now_iso(),
        })
        .await
        .unwrap();
    svc.store
        .add_workspace_member(ws, &id, intent_core::WorkspaceRole::Collaborator)
        .await
        .unwrap();
    id
}

fn as_principal(id: &PrincipalId) -> Caller {
    Caller::Wire {
        principal_id: id.clone(),
        host_role: intent_core::HostRole::Guest,
    }
}

fn set_by_json(id: &PrincipalId, login: &str, display: &str) -> Value {
    json!({ "principalId": id.0, "displayName": display, "login": login })
}

/// The `goalSetBy` served on `agent.get` (`AgentLite`) and the
/// `agent.getSession` session form, which must agree.
async fn served_set_by(svc: &Services, ws: &WorkspaceId, id: &AgentId) -> Option<Value> {
    let lite = serde_json::to_value(
        svc.agent_get_op(id.clone(), Some(ws.clone()))
            .await
            .unwrap(),
    )
    .unwrap();
    let session =
        serde_json::to_value(svc.agent_get_session_op(id.clone()).await.unwrap()).unwrap();
    assert_eq!(lite.get("goalSetBy"), session.get("goalSetBy"));
    lite.get("goalSetBy").cloned()
}

/// Attribution (§5.5 "Conversation goal" → "Attribution"): every write
/// records its writer; a later writer replaces it; `unchanged` / `shown`
/// keep it; it survives retire / restore; a clear removes it with the goal;
/// and `agent:updated` carries it (`null` on remove).
#[intent_test_macros::daemon_test]
async fn goal_set_by_follows_the_last_writer() {
    let (_t, svc, ws, bus) = setup_with_bus().await;
    let id = create_agent(&svc, &ws, "Goal").await;
    let alice = member(&svc, &ws, "alice", "Alice A").await;
    let bob = member(&svc, &ws, "bob", "Bob B").await;
    let alice_json = set_by_json(&alice, "alice", "Alice A");
    let bob_json = set_by_json(&bob, "bob", "Bob B");
    let mut sub = subscribe(&bus);

    let set = with_caller(
        as_principal(&alice),
        svc.agent_set_goal_op(Some(ws.clone()), id.clone(), Some("Ship it".into())),
    )
    .await
    .unwrap();
    assert_eq!(set["action"], "set");
    assert_eq!(set["goalSetBy"], alice_json);
    assert_eq!(
        served_set_by(&svc, &ws, &id).await,
        Some(alice_json.clone())
    );

    // Bob repeats the same text: `unchanged` keeps Alice; his `/goal` shows
    // it with her attribution.
    let same = with_caller(
        as_principal(&bob),
        svc.agent_set_goal_op(None, id.clone(), Some("Ship it".into())),
    )
    .await
    .unwrap();
    assert_eq!(same["action"], "unchanged");
    assert_eq!(same["goalSetBy"], alice_json);
    let shown = with_caller(
        as_principal(&bob),
        svc.agent_goal_command_op(None, id.clone(), String::new()),
    )
    .await
    .unwrap();
    assert_eq!(shown["command"]["action"], "shown");
    assert_eq!(shown["command"]["goalSetBy"], alice_json);
    assert_eq!(
        served_set_by(&svc, &ws, &id).await,
        Some(alice_json.clone())
    );

    // Bob replaces it with a `/goal` command: now Bob.
    let updated = with_caller(
        as_principal(&bob),
        svc.agent_goal_command_op(None, id.clone(), "Ship it twice".into()),
    )
    .await
    .unwrap();
    assert_eq!(updated["command"]["action"], "updated");
    assert_eq!(updated["command"]["goalSetBy"], bob_json);
    assert_eq!(served_set_by(&svc, &ws, &id).await, Some(bob_json.clone()));

    // Retire / restore keeps it with the goal.
    svc.agent_retire_op(id.clone(), Some(ws.clone()), None)
        .await
        .unwrap();
    svc.agent_restore_op(id.clone(), Some(ws.clone()))
        .await
        .unwrap();
    assert_eq!(
        stored_goal(&svc, &id).await.as_deref(),
        Some("Ship it twice")
    );
    assert_eq!(served_set_by(&svc, &ws, &id).await, Some(bob_json.clone()));

    // Alice clears it: the attribution goes with the goal.
    let cleared = with_caller(
        as_principal(&alice),
        svc.agent_set_goal_op(None, id.clone(), None),
    )
    .await
    .unwrap();
    assert_eq!(cleared["action"], "cleared");
    assert!(cleared.get("goalSetBy").is_none(), "{cleared}");
    assert_eq!(served_set_by(&svc, &ws, &id).await, None);

    let goal_updates: Vec<Value> = updates(&drain(&mut sub).await)
        .into_iter()
        .filter(|u| u.get("goal").is_some())
        .collect();
    assert_eq!(
        goal_updates,
        vec![
            json!({ "agentId": id.0, "goal": "Ship it", "goalSetBy": alice_json }),
            json!({ "agentId": id.0, "goal": "Ship it twice", "goalSetBy": bob_json }),
            json!({ "agentId": id.0, "goal": null, "goalSetBy": null }),
        ]
    );
}

/// A write with no resolvable principal (a daemon caller, or a wire
/// principal with no stored profile) stores no attribution and replaces a
/// previous writer's; `agent:updated` then carries `goalSetBy: null`.
#[intent_test_macros::daemon_test]
async fn goal_write_without_a_principal_stores_no_attribution() {
    let (_t, svc, ws, bus) = setup_with_bus().await;
    let id = create_agent(&svc, &ws, "Goal").await;
    let alice = member(&svc, &ws, "alice", "Alice A").await;
    with_caller(
        as_principal(&alice),
        svc.agent_set_goal_op(None, id.clone(), Some("First".into())),
    )
    .await
    .unwrap();
    let mut sub = subscribe(&bus);

    let result = with_caller(
        Caller::Daemon,
        svc.agent_set_goal_op(None, id.clone(), Some("Second".into())),
    )
    .await
    .unwrap();
    assert_eq!(result["action"], "updated");
    assert!(result.get("goalSetBy").is_none(), "{result}");
    assert_eq!(served_set_by(&svc, &ws, &id).await, None);
    let command = with_caller(
        Caller::Daemon,
        svc.agent_goal_command_op(None, id.clone(), "Third".into()),
    )
    .await
    .unwrap();
    assert!(command["command"].get("goalSetBy").is_none(), "{command}");

    let goal_updates: Vec<Value> = updates(&drain(&mut sub).await)
        .into_iter()
        .filter(|u| u.get("goal").is_some())
        .collect();
    assert_eq!(
        goal_updates,
        vec![
            json!({ "agentId": id.0, "goal": "Second", "goalSetBy": null }),
            json!({ "agentId": id.0, "goal": "Third", "goalSetBy": null }),
        ]
    );
}

/// The attribution is a store column written with the goal: it survives
/// reopening the database, and full-row session writes never revert it.
#[intent_test_macros::daemon_test]
async fn goal_set_by_survives_reopen_and_full_row_writes() {
    let (tmp, svc, ws, _bus) = setup_with_bus().await;
    let id = create_agent(&svc, &ws, "Goal").await;
    let alice = member(&svc, &ws, "alice", "Alice A").await;
    let mut stale = svc
        .store
        .get_agent_session_with_transcript(&id)
        .await
        .unwrap();
    with_caller(
        as_principal(&alice),
        svc.agent_set_goal_op(None, id.clone(), Some("Persist me".into())),
    )
    .await
    .unwrap();
    stale.name = "Renamed".into();
    svc.store.update_agent_session(&ws, &stale).await.unwrap();
    drop(svc);
    let store = intent_store::Store::open(&tmp.path).await.unwrap();
    let session = store.get_agent_session_summary(&id).await.unwrap();
    assert_eq!(
        session
            .goal_set_by
            .as_ref()
            .map(|s| serde_json::to_value(s).unwrap()),
        Some(set_by_json(&alice, "alice", "Alice A"))
    );
}
