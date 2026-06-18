//! In-process event bus (§10).
//!
//! Append-then-broadcast over the M2.1 `event` store, mirroring
//! `~/src/intent/src/main/websocket-event-bridge.ts`: [`EventBus::publish`]
//! persists the event first (so the durable log is the source of truth), then
//! fans it out to live subscribers. [`EventBus::subscribe`] returns a
//! [`Subscription`] whose per-subscriber delivery task applies the
//! [`SubscriptionFilter`] and coalesces matched events within `batch_window`
//! (the TS `batchFlushWorker`: the timer starts on the first matched event).

use std::sync::Arc;

use intent_core::{Event, Result};
use intent_store::{NewEvent, Store};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;

use super::filter::{event_matches, SubscriptionFilter};

/// Capacity of the broadcast channel that fans published events out to every
/// subscriber's delivery task. Slow subscribers that fall this far behind are
/// signalled via `Lagged` and skip the dropped events (they remain in the log).
const BROADCAST_CAPACITY: usize = 1024;

/// Capacity of each subscriber's outbound batch queue.
const SUBSCRIBER_QUEUE_CAPACITY: usize = 256;

/// In-process broadcast bus layered over the durable event [`Store`]. Cheap to
/// clone (the store handle and broadcast sender are both shared).
#[derive(Clone)]
pub struct EventBus {
    store: Store,
    tx: broadcast::Sender<Arc<Event>>,
}

impl EventBus {
    /// Wire a bus over a persistence handle.
    pub fn new(store: Store) -> Self {
        let (tx, _rx) = broadcast::channel(BROADCAST_CAPACITY);
        Self { store, tx }
    }

    /// Borrow the underlying store.
    pub fn store(&self) -> &Store {
        &self.store
    }

    /// Number of live subscribers (active delivery tasks). Read-only
    /// observability used to assert per-connection subscription cleanup; each
    /// [`EventBus::subscribe`] adds one and dropping the [`Subscription`]
    /// removes it.
    pub fn subscriber_count(&self) -> usize {
        self.tx.receiver_count()
    }

    /// Append `ev` to the durable log, then broadcast the persisted event to
    /// live subscribers. The broadcast is best-effort: with no subscribers the
    /// send is a no-op, and the event is already durably stored either way.
    pub async fn publish(&self, ev: &NewEvent) -> Result<Event> {
        let stored = self.store.insert_event(ev).await?;
        let _ = self.tx.send(Arc::new(stored.clone()));
        Ok(stored)
    }

    /// Subscribe with `filter`. The returned [`Subscription`] yields batches of
    /// matched events; when `filter.batch_window` is `None` each matched event
    /// is delivered immediately as a single-element batch.
    pub fn subscribe(&self, filter: SubscriptionFilter) -> Subscription {
        let rx = self.tx.subscribe();
        let (out_tx, out_rx) = mpsc::channel(SUBSCRIBER_QUEUE_CAPACITY);
        let handle = tokio::spawn(delivery_task(rx, filter, out_tx));
        Subscription { rx: out_rx, handle }
    }
}

/// A live subscription. Yields filtered (and optionally batched) events via
/// [`Subscription::recv`]; dropping it aborts the backing delivery task.
pub struct Subscription {
    rx: mpsc::Receiver<Vec<Event>>,
    handle: JoinHandle<()>,
}

impl Subscription {
    /// Await the next batch of matched events. Returns `None` once the bus is
    /// dropped and all buffered batches have been drained.
    pub async fn recv(&mut self) -> Option<Vec<Event>> {
        self.rx.recv().await
    }
}

impl Drop for Subscription {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Per-subscriber delivery loop: filter, then coalesce within `batch_window`.
async fn delivery_task(
    mut rx: broadcast::Receiver<Arc<Event>>,
    filter: SubscriptionFilter,
    out: mpsc::Sender<Vec<Event>>,
) {
    let batch_window = filter.batch_window;
    let mut buffer: Vec<Event> = Vec::new();
    // A pending flush deadline, armed on the first matched event of a batch.
    let mut deadline: Option<std::pin::Pin<Box<tokio::time::Sleep>>> = None;
    loop {
        tokio::select! {
            recv = rx.recv() => match recv {
                Ok(ev) => {
                    if !event_matches(&filter, &ev) {
                        continue;
                    }
                    match batch_window {
                        None => {
                            if out.send(vec![(*ev).clone()]).await.is_err() {
                                return;
                            }
                        }
                        Some(window) => {
                            buffer.push((*ev).clone());
                            if deadline.is_none() {
                                deadline = Some(Box::pin(tokio::time::sleep(window)));
                            }
                        }
                    }
                }
                // Slow consumer: skip dropped events (still durable in the log).
                Err(broadcast::error::RecvError::Lagged(_)) => continue,
                // Bus dropped: flush any buffered batch, then stop.
                Err(broadcast::error::RecvError::Closed) => {
                    if !buffer.is_empty() {
                        let _ = out.send(std::mem::take(&mut buffer)).await;
                    }
                    return;
                }
            },
            // Batch window elapsed → flush the coalesced events.
            _ = async { deadline.as_mut().unwrap().await }, if deadline.is_some() => {
                deadline = None;
                if !buffer.is_empty() && out.send(std::mem::take(&mut buffer)).await.is_err() {
                    return;
                }
            }
        }
    }
}
