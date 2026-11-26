use std::sync::Arc;

use intent_core::WorkspaceId;

use super::{OrderedGate, Rank};
use crate::ArchiveFence;

fn ws() -> WorkspaceId {
    WorkspaceId::from("ws-lock-order")
}

fn turn_start() -> OrderedGate {
    OrderedGate::new(Rank::TurnStartGate, Arc::default())
}

#[tokio::test]
async fn the_documented_order_is_allowed() {
    let fence = Arc::new(ArchiveFence::default());
    let retirement = OrderedGate::default();
    let starting = turn_start();
    let _r = retirement.lock().await;
    let _t = starting.lock().await;
    let _f = fence.acquire(&ws()).await.unwrap();
}

#[tokio::test]
#[should_panic(expected = "lock order violation: RetirementGate")]
async fn a_retirement_gate_under_an_archive_fence_panics() {
    let fence = Arc::new(ArchiveFence::default());
    let _f = fence.acquire(&ws()).await.unwrap();
    let _r = OrderedGate::default().lock().await;
}

#[tokio::test]
#[should_panic(expected = "lock order violation: TurnStartGate")]
async fn a_turn_start_gate_under_an_archive_fence_panics() {
    let fence = Arc::new(ArchiveFence::default());
    let _f = fence.acquire(&ws()).await.unwrap();
    let _t = turn_start().lock().await;
}

#[tokio::test]
#[should_panic(expected = "lock order violation: RetirementGate")]
async fn a_retirement_gate_under_a_turn_start_gate_panics() {
    let _t = turn_start().lock().await;
    let _r = OrderedGate::default().try_lock().unwrap();
}

#[tokio::test]
#[should_panic(expected = "lock order violation")]
async fn the_check_fires_before_waiting_on_a_held_gate() {
    let fence = Arc::new(ArchiveFence::default());
    let gate = OrderedGate::default();
    let other = {
        let gate = gate.clone();
        tokio::spawn(async move { gate.lock().await })
            .await
            .unwrap()
    };
    let _f = fence.acquire(&ws()).await.unwrap();
    let _r = gate.lock().await;
    drop(other);
}

#[tokio::test]
async fn in_order_paths_joined_in_one_task_are_allowed() {
    let fence = Arc::new(ArchiveFence::default());
    let in_order = || async {
        let _r = OrderedGate::default().lock().await;
        let _f = fence.acquire(&ws()).await.unwrap();
        tokio::task::yield_now().await;
    };
    tokio::join!(in_order(), in_order());
}

#[tokio::test]
#[should_panic(expected = "lock order violation: RetirementGate")]
async fn a_hold_whose_earlier_lock_was_released_counts() {
    let fence = Arc::new(ArchiveFence::default());
    let first = OrderedGate::default().lock().await;
    let _f = fence.acquire(&ws()).await.unwrap();
    drop(first);
    let _r = OrderedGate::default().lock().await;
}

#[tokio::test]
#[should_panic(expected = "lock order violation: TurnStartGate")]
async fn a_turn_start_gate_under_a_fence_taken_under_a_retirement_gate_panics() {
    let fence = Arc::new(ArchiveFence::default());
    let _r = OrderedGate::default().lock().await;
    let _f = fence.acquire(&ws()).await.unwrap();
    let _t = turn_start().lock().await;
}

#[tokio::test]
async fn a_released_hold_no_longer_counts() {
    let fence = Arc::new(ArchiveFence::default());
    drop(fence.acquire(&ws()).await.unwrap());
    drop(turn_start().lock().await);
    let _r = OrderedGate::default().lock().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lock_held_by_another_task_does_not_count() {
    let fence = Arc::new(ArchiveFence::default());
    let (held_tx, held_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
    let holder = {
        let fence = Arc::clone(&fence);
        tokio::spawn(async move {
            let _f = fence.acquire(&ws()).await.unwrap();
            held_tx.send(()).unwrap();
            release_rx.await.ok();
        })
    };
    held_rx.await.unwrap();
    drop(OrderedGate::default().lock().await);
    release_tx.send(()).unwrap();
    holder.await.unwrap();
}
