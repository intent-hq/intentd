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
            |(next, _)| vec![next.clone()],
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

fn live_ids(svc: &Services, agent: &AgentId) -> Vec<String> {
    svc.agent_queues
        .lock()
        .unwrap()
        .get(agent)
        .map(|queue| queue.iter().map(|m| m.id.clone()).collect())
        .unwrap_or_default()
}

/// A row of another process's entry: a real `QueuedMessage` payload that this
/// process's memory does not hold.
fn foreign_row(svc: &Services, agent: &AgentId, id: &str) -> intent_store::AgentQueueRow {
    enqueue(svc, agent, id);
    let row = svc
        .queue_rows(agent)
        .into_iter()
        .find(|row| row.id == id)
        .expect("the foreign entry's row");
    svc.agent_queues
        .lock()
        .unwrap()
        .get_mut(agent)
        .unwrap()
        .retain(|m| m.id != id);
    row
}

/// The store's merged queue arrives after memory moved on: a local removal
/// and a local enqueue made during the sync both survive next to the
/// foreign entry, and a local edit wins over the stored copy.
#[tokio::test]
async fn adopting_the_stored_queue_keeps_mutations_made_during_the_sync() {
    let (_tmp, svc, ws) = setup().await;
    let agent = create_agent(&svc, &ws, "Adopt during sync").await;
    enqueue(&svc, &agent, "m1");
    enqueue(&svc, &agent, "m2");
    let foreign = foreign_row(&svc, &agent, "f1");
    // What the sync started from, and what the store sent back: our queue
    // with another process's f1 appended.
    let from = svc.queue_rows(&agent);
    let mut merged = from.clone();
    merged.push(foreign);
    // Meanwhile: m1 is removed, m2 edited and m3 enqueued here.
    svc.agent_queues
        .lock()
        .unwrap()
        .get_mut(&agent)
        .unwrap()
        .retain(|m| m.id != "m1");
    svc.agent_queues
        .lock()
        .unwrap()
        .get_mut(&agent)
        .unwrap()
        .iter_mut()
        .find(|m| m.id == "m2")
        .unwrap()
        .content = "edited meanwhile".into();
    enqueue(&svc, &agent, "m3");

    svc.adopt_stored_queue(&agent, &from, merged);
    assert_eq!(live_ids(&svc, &agent), ["m2", "f1", "m3"]);
    let queue = svc
        .agent_queues
        .lock()
        .unwrap()
        .get(&agent)
        .cloned()
        .unwrap();
    assert_eq!(queue[0].content, "edited meanwhile");
}

/// With memory unchanged during the sync, memory becomes the stored queue,
/// including another process's removal of an entry this process holds.
#[tokio::test]
async fn adopting_the_stored_queue_takes_a_foreign_removal() {
    let (_tmp, svc, ws) = setup().await;
    let agent = create_agent(&svc, &ws, "Adopt a removal").await;
    enqueue(&svc, &agent, "m1");
    enqueue(&svc, &agent, "m2");
    let from = svc.queue_rows(&agent);
    let merged: Vec<_> = from.iter().filter(|row| row.id != "m1").cloned().collect();
    svc.adopt_stored_queue(&agent, &from, merged);
    assert_eq!(live_ids(&svc, &agent), ["m2"]);
}

/// A failed claim puts its popped entries back at their place, after the
/// nearest earlier entry still queued, keeping an enqueue made meanwhile.
/// The entries come from the pop itself: m3 was enqueued after the
/// pre-pop snapshot (which lists only m1, m2) and is still restored, right
/// after m2, ahead of m4 enqueued after the pop.
#[tokio::test]
async fn an_unclaimed_pop_returns_to_its_place() {
    let (_tmp, svc, ws) = setup().await;
    let agent = create_agent(&svc, &ws, "Restore unclaimed").await;
    enqueue(&svc, &agent, "m1");
    enqueue(&svc, &agent, "m2");
    let before = live_ids(&svc, &agent);
    enqueue(&svc, &agent, "m3");
    let (popped, draining) = svc
        .dequeue_ready_batch_draining(&agent, false, 2)
        .expect("a batch");
    drop(draining);
    assert_eq!(
        popped.iter().map(|m| m.id.as_str()).collect::<Vec<_>>(),
        ["m1", "m2", "m3"]
    );
    enqueue(&svc, &agent, "m4");
    svc.restore_unclaimed(&agent, &before, &popped);
    assert_eq!(live_ids(&svc, &agent), ["m1", "m2", "m3", "m4"]);
    // Restoring twice adds nothing.
    svc.restore_unclaimed(&agent, &before, &popped);
    assert_eq!(live_ids(&svc, &agent), ["m1", "m2", "m3", "m4"]);
}
