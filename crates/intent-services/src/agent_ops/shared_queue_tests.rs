//! The shared-store queue paths on `SQLite` (one writer): the claimed pop is
//! the plain pop with nothing lost, a refresh changes nothing, and the
//! write-through is the plain replace. The two-process behavior is covered on
//! Postgres by `intentd-orchestrator/tests/shared_agent_queue.rs` and the
//! store's `queue_syncs_from_two_processes_merge_and_claim_once`.
use super::tests::{create_agent, setup};
use super::*;

fn enqueue(svc: &Services, agent: &AgentId, id: &str) -> QueuedMessage {
    svc.enqueue_message_with_id(
        agent,
        Some(id.into()),
        format!("text {id}"),
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

#[tokio::test]
async fn a_claimed_pop_on_an_unshared_store_is_the_plain_pop() {
    let (_tmp, svc, ws) = setup().await;
    assert!(!svc.store.agent_queues_shared());
    let agent = create_agent(&svc, &ws, "Unshared queue").await;
    enqueue(&svc, &agent, "m1");
    enqueue(&svc, &agent, "m2");
    svc.persist_queue_snapshot(&agent).await;
    assert_eq!(durable_ids(&svc, &agent).await, ["m1", "m2"]);

    // The store row of a popped entry is still there: on SQLite the caller
    // persists the shrink, as before.
    let ((popped, draining), lost) = svc
        .claim_queue_pop(
            &agent,
            || svc.dequeue_message_draining(&agent),
            |(next, _)| vec![next.id.clone()],
        )
        .await
        .expect("the head pops");
    assert_eq!(popped.id, "m1");
    assert!(lost.is_empty());
    assert_eq!(durable_ids(&svc, &agent).await, ["m1", "m2"]);
    svc.persist_queue_snapshot(&agent).await;
    drop(draining);
    assert_eq!(durable_ids(&svc, &agent).await, ["m2"]);

    // A refresh reads nothing and leaves memory alone.
    svc.refresh_queue_from_store(&agent).await;
    let ids: Vec<String> = svc
        .queue_snapshot(&agent)
        .iter()
        .map(|entry| entry["id"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(ids, ["m2"]);
}

#[tokio::test]
async fn a_draining_guard_releases_only_the_given_entries() {
    let (_tmp, svc, ws) = setup().await;
    let agent = create_agent(&svc, &ws, "Release lost entries").await;
    enqueue(&svc, &agent, "m1");
    enqueue(&svc, &agent, "m2");
    enqueue(&svc, &agent, "m3");
    let (batch, mut draining) = svc
        .dequeue_ready_batch_draining(&agent, false, 2)
        .expect("a flush batch");
    assert_eq!(batch.len(), 3);
    let listed = |svc: &Services| -> Vec<String> {
        svc.queue_snapshot(&agent)
            .iter()
            .map(|entry| entry["id"].as_str().unwrap().to_owned())
            .collect()
    };
    assert_eq!(
        listed(&svc),
        ["m1", "m2", "m3"],
        "the overlay lists the batch"
    );
    draining.release(&["m2".to_owned()]);
    assert_eq!(listed(&svc), ["m1", "m3"]);
    drop(draining);
    assert!(listed(&svc).is_empty());
}
