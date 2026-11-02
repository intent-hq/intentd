//! Shutdown reconciliation of the durable `agent_queue` snapshot.
use super::tests::{create_agent, setup};
use super::*;

fn enqueue(svc: &Services, agent: &AgentId, id: &str, text: &str) -> QueuedMessage {
    svc.enqueue_message_with_id(
        agent,
        Some(id.into()),
        text.into(),
        None,
        None,
        None,
        None,
        false,
        MessageOrigin::User,
    )
    .0
}

async fn durable_ids(svc: &Services, agent: &AgentId) -> Vec<String> {
    svc.store
        .load_all_agent_queues()
        .await
        .unwrap()
        .into_iter()
        .filter(|row| row.agent_id == *agent)
        .map(|row| row.id)
        .collect()
}

/// Shutdown aborts turn workers. A failed delivery requeues its entry in
/// memory and only then persists it; aborting that persist used to leave the
/// row deleted by the earlier dequeue write-through, so a clean restart lost
/// the message (`context_recovery_direct_both_over_wss` flake).
#[tokio::test]
async fn shutdown_persists_a_requeue_whose_write_through_was_cancelled() {
    let (_tmp, svc, ws) = setup().await;
    let agent = create_agent(&svc, &ws, "Requeue at shutdown").await;
    enqueue(&svc, &agent, "m1", "hello");
    svc.persist_queue_snapshot(&agent).await;
    assert_eq!(durable_ids(&svc, &agent).await, ["m1"]);

    let popped = svc.dequeue_message(&agent).unwrap();
    svc.persist_queue_snapshot(&agent).await;
    assert!(durable_ids(&svc, &agent).await.is_empty());

    let mut requeued = popped;
    requeued.requeued_after_failure = true;
    svc.requeue_front(&agent, requeued);
    {
        // Poll the write-through once while the gate is held, then drop it:
        // the same cancellation a shutdown abort applies to a turn worker.
        let _held = svc.agent_queue_persist_gate.lock().await;
        let mut persist = std::pin::pin!(svc.persist_queue_snapshot(&agent));
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(
            std::future::Future::poll(persist.as_mut(), &mut cx).is_pending(),
            "the write-through must still be pending"
        );
    }
    assert!(durable_ids(&svc, &agent).await.is_empty());

    svc.freeze_shutdown_drains();
    svc.persist_shutdown_queues().await;

    let restarted = Services::new_isolated(svc.store.clone());
    restarted.rehydrate_agent_queues().await.unwrap();
    let rows = restarted.queue_snapshot(&agent);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["id"], "m1");
    assert_eq!(rows[0]["content"], "hello");
    assert_eq!(rows[0]["requeuedAfterFailure"], true);
}

/// A removal whose write-through was cancelled must not resurrect the
/// removed entry after restart.
#[tokio::test]
async fn shutdown_persists_a_removal_whose_write_through_was_cancelled() {
    let (_tmp, svc, ws) = setup().await;
    let agent = create_agent(&svc, &ws, "Remove at shutdown").await;
    enqueue(&svc, &agent, "keep", "keep");
    enqueue(&svc, &agent, "gone", "gone");
    svc.persist_queue_snapshot(&agent).await;
    assert_eq!(durable_ids(&svc, &agent).await, ["keep", "gone"]);

    svc.agent_queues
        .lock()
        .unwrap()
        .get_mut(&agent)
        .unwrap()
        .retain(|m| m.id != "gone");
    svc.freeze_shutdown_drains();
    svc.persist_shutdown_queues().await;

    assert_eq!(durable_ids(&svc, &agent).await, ["keep"]);
}

fn undecodable_row(agent: &AgentId, id: &str) -> intent_store::AgentQueueRow {
    intent_store::AgentQueueRow {
        id: id.into(),
        agent_id: agent.clone(),
        position: 0,
        payload: json!({"unknownShape": true}),
        created_at: intent_core::now_iso(),
        turn_id: id.into(),
    }
}

/// Agents whose durable queue already matches memory are left untouched, so
/// rows this daemon cannot decode survive a shutdown that never mutated them,
/// wherever they sit: rehydration skips them and renumbers the valid rows, so
/// the comparison must not depend on absolute positions.
#[tokio::test]
async fn shutdown_leaves_matching_queues_and_undecodable_rows_alone() {
    let (_tmp, svc, ws) = setup().await;
    let agent = create_agent(&svc, &ws, "Untouched").await;
    enqueue(&svc, &agent, "known-1", "one");
    enqueue(&svc, &agent, "known-2", "two");
    svc.persist_queue_snapshot(&agent).await;
    let mut valid = svc.store.load_all_agent_queues().await.unwrap();
    valid.retain(|row| row.agent_id == agent);
    assert_eq!(valid.len(), 2);
    let mut rows = vec![
        undecodable_row(&agent, "newer-before"),
        valid[0].clone(),
        undecodable_row(&agent, "newer-between"),
        valid[1].clone(),
        undecodable_row(&agent, "newer-after"),
    ];
    for (position, row) in rows.iter_mut().enumerate() {
        row.position = i64::try_from(position).unwrap();
    }
    svc.store.replace_agent_queue(&agent, &rows).await.unwrap();

    let restarted = Services::new_isolated(svc.store.clone());
    restarted.rehydrate_agent_queues().await.unwrap();
    assert_eq!(restarted.queue_snapshot(&agent).len(), 2);
    restarted.freeze_shutdown_drains();
    restarted.persist_shutdown_queues().await;

    assert_eq!(
        durable_ids(&restarted, &agent).await,
        [
            "newer-before",
            "known-1",
            "newer-between",
            "known-2",
            "newer-after"
        ]
    );
}
