//! Regression test for STAB-28 completion-watch re-registration.
//!
//! Covers the re-wake subscription-loss failure mode: a parent that re-messages
//! a settled child via agent.send must be woken when that child settles again.
//! The interrupt-path wedge is tested in `agent_manager/tests.rs`.

use intent_core::{AgentId, WorkspaceId};
use intent_store::Store;
use serde_json::json;

use crate::Services;
use intent_core::events::AGENT_IDLE;

use super::tests::{completion_event, workspace, TempDb};

async fn setup() -> (TempDb, Services, WorkspaceId) {
    let tmp = TempDb::new();
    let store = Store::open(&tmp.path).await.expect("open store");
    let ws = WorkspaceId::new();
    store.insert_workspace(&workspace(&ws)).await.expect("ws");
    let services = Services::new(store);
    (tmp, services, ws)
}

async fn create_agent(
    svc: &Services,
    ws: &WorkspaceId,
    name: &str,
    parent: Option<&AgentId>,
) -> AgentId {
    let created = svc
        .agent_create_op(
            ws.clone(),
            Some(name.to_string()),
            Some("sonnet4.5".into()),
            None,
            parent.cloned(),
            None,
            false,
            intent_core::AgentCreateExtra {
                provider: Some("auggie".into()),
                ..Default::default()
            },
        )
        .await
        .expect("create");
    AgentId::from(created["agent"]["id"].as_str().unwrap())
}

#[tokio::test]
async fn parent_rewoken_after_send_to_settled_child() {
    // STAB-28 regression test: parent delegates → child settles (parent woken once)
    // → parent sends follow-up via agent.send → child settles again → parent MUST
    // be woken again.
    let (_t, svc, ws) = setup().await;
    let parent = create_agent(&svc, &ws, "Parent", None).await;
    // The child carries real parent linkage: the SUB-1 independent-peer
    // suppression skips the auto-watch for top-level foreground targets, and
    // this test is about the parent→child re-wake path.
    let child = create_agent(&svc, &ws, "Child", Some(&parent)).await;

    // Initial delegation: register completion watch.
    svc.register_completion_watch(
        &ws,
        &ws,
        parent.clone(),
        "Parent".into(),
        child.clone(),
        None,
    )
    .expect("register watch");

    // Child settles for the first time → parent woken, watch removed.
    let event1 = completion_event(
        &ws,
        AGENT_IDLE,
        &child,
        json!({ "agentId": child.0, "lastResponseSummary": "first turn done" }),
    );
    svc.handle_completion_event(&event1).await;

    // Parent received the first wake message.
    let parent_session = svc
        .store()
        .get_agent_session(&parent)
        .await
        .expect("parent session");
    assert_eq!(
        parent_session.messages.len(),
        1,
        "Parent should have 1 message after first child completion"
    );

    // OneShot watch was removed.
    assert!(
        svc.find_watches_for_child(&child).is_empty(),
        "OneShot watch should be removed after first completion"
    );

    // Parent re-messages the child via agent.send (simulates agent.send in MCP).
    // This should register a NEW completion watch.
    let send_result = svc
        .agent_watch_completion_for_sender_op(ws.clone(), parent.clone(), child.clone())
        .await
        .expect("watch registration");

    assert_eq!(
        send_result["ok"],
        json!(true),
        "Sender watch registration should succeed"
    );
    assert!(
        send_result["subscriptionId"].is_string(),
        "Sender watch should return subscriptionId"
    );

    // Verify watch exists before second completion.
    let watches_before = svc.find_watches_for_child(&child);
    assert_eq!(
        watches_before.len(),
        1,
        "Parent→child watch should exist after agent.send"
    );

    // Child settles for the SECOND time → parent must be woken again.
    let event2 = completion_event(
        &ws,
        AGENT_IDLE,
        &child,
        json!({ "agentId": child.0, "lastResponseSummary": "second turn done" }),
    );
    svc.handle_completion_event(&event2).await;

    // Parent received the SECOND wake message.
    let parent_session_after = svc
        .store()
        .get_agent_session(&parent)
        .await
        .expect("parent session after second completion");
    assert_eq!(
        parent_session_after.messages.len(),
        2,
        "Parent should have 2 messages after second child completion (STAB-28: re-wake must fire)"
    );

    // Second watch was also removed.
    assert!(
        svc.find_watches_for_child(&child).is_empty(),
        "OneShot watch should be removed after second completion"
    );
}

// NOTE: The above test covers the re-wake completion-watch registration/delivery
// mechanism (failure mode (a)). The interrupt-path wedge (failure mode (b)) is
// covered by agent_manager/tests.rs:interrupt_emits_terminal_stream_end_and_idle_when_no_queue,
// which asserts that interrupt() now emits agent:idle after the worker is aborted.
