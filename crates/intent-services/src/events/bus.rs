//! In-process event bus (§10).
//!
//! Append-then-broadcast over the M2.1 `event` store, mirroring
//! `~/src/intent/src/main/websocket-event-bridge.ts`: [`EventBus::publish`]
//! persists the event first (so the durable log is the source of truth), then
//! fans it out to live subscribers. [`EventBus::publish_transient`] mints an
//! event id and broadcasts WITHOUT persisting (for high-volume ephemeral events
//! like `agent:stream:chunk`). [`EventBus::subscribe`] returns a
//! [`Subscription`] whose per-subscriber delivery task applies the
//! [`SubscriptionFilter`] and coalesces matched events within `batch_window`
//! (the TS `batchFlushWorker`: the timer starts on the first matched event).

use std::sync::Arc;

use intent_core::{Error, Event, Result};
use intent_store::{NewEvent, Store};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use uuid::Uuid;

use super::filter::{event_matches, SubscriptionFilter};

/// Capacity of the broadcast channel that fans published events out to every
/// subscriber's delivery task. Slow subscribers that fall this far behind are
/// signalled via `Lagged` and skip the dropped events (they remain in the log).
const BROADCAST_CAPACITY: usize = 1024;

/// Capacity of each subscriber's outbound batch queue.
const SUBSCRIBER_QUEUE_CAPACITY: usize = 256;

/// Capacity of the writer task's inbound channel. Controls backpressure when
/// publishers send events faster than the writer task can batch-persist them.
/// At 512 slots, the channel can buffer ~8 full batches (64 events each) before
/// backpressure kicks in. When the channel is full, `EventBus::publish()` awaits
/// the `send()` until a slot opens (blocking the publisher until the writer task
/// drains a batch). This prevents unbounded memory growth under sustained bursts
/// while keeping typical publish calls non-blocking.
const WRITER_CHANNEL_CAPACITY: usize = 512;

/// Max events drained per batch by the writer task (to bound transaction size).
const WRITER_BATCH_SIZE: usize = 64;

/// Max time the writer task waits to accumulate events before flushing (ms).
const WRITER_BATCH_WINDOW_MS: u64 = 20;

/// Request sent to the writer task: the event to persist and a oneshot to
/// return the result (or error).
type WriterRequest = (NewEvent, oneshot::Sender<Result<Event>>);

/// In-process broadcast bus layered over the durable event [`Store`]. Cheap to
/// clone (the store handle, broadcast sender, and writer channel are all shared).
#[derive(Clone)]
pub struct EventBus {
    store: Store,
    tx: broadcast::Sender<Arc<Event>>,
    writer_tx: mpsc::Sender<WriterRequest>,
}

impl EventBus {
    /// Wire a bus over a persistence handle and spawn the writer task.
    pub fn new(store: Store) -> Self {
        let (tx, _rx) = broadcast::channel(BROADCAST_CAPACITY);
        let (writer_tx, writer_rx) = mpsc::channel(WRITER_CHANNEL_CAPACITY);
        // Spawn the writer task that drains events and batch-persists them.
        tokio::spawn(writer_task(store.clone(), writer_rx, tx.clone()));
        Self {
            store,
            tx,
            writer_tx,
        }
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
    /// live subscribers. Round-trips through the writer task that coalesces
    /// high-volume inserts into batched transactions. Returns an error if the
    /// writer task has shut down or the send fails.
    pub async fn publish(&self, ev: &NewEvent) -> Result<Event> {
        let (tx, rx) = oneshot::channel();
        let req = (ev.clone(), tx);
        self.writer_tx
            .send(req)
            .await
            .map_err(|_| Error::Internal("event writer task closed".to_string()))?;
        rx.await
            .map_err(|_| Error::Internal("event writer task dropped response".to_string()))?
    }

    /// Mint an event id (UUIDv7) + timestamp and broadcast to live subscribers
    /// WITHOUT persisting. Used for high-volume ephemeral events (e.g.
    /// `agent:stream:chunk`) that do not need durable storage. The wire shape
    /// matches persisted events exactly (same id/timestamp minting as
    /// `Store::insert_event`), so subscribers see identical structure.
    ///
    /// **Ordering guarantee**: Events broadcast from a single publisher (e.g., one
    /// agent session) are delivered to subscribers in publish order, whether they
    /// are transient or persisted. Cross-publisher ordering is not guaranteed; if
    /// publisher A calls `publish_transient(ev1)` and publisher B calls
    /// `publish(ev2).await` concurrently, A's transient event may broadcast before
    /// B's persisted event commits, even if B's call started first.
    pub fn publish_transient(&self, ev: &NewEvent) -> Event {
        let id = Uuid::now_v7().to_string();
        let event = Event {
            id,
            workspace_id: ev.workspace_id.clone(),
            timestamp: ev.timestamp.clone(),
            event_type: ev.event_type.clone(),
            actor: ev.actor.clone(),
            session_id: ev.session_id.clone(),
            correlation_id: ev.correlation_id.clone(),
            parent_event_id: ev.parent_event_id.clone(),
            metadata: ev.metadata.clone(),
            data: ev.data.clone(),
        };
        // Broadcast on the same channel so transient/persisted events interleave
        // correctly for subscribers (ordering preserved from a single publisher).
        let _ = self.tx.send(Arc::new(event.clone()));
        event
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

impl Drop for EventBus {
    fn drop(&mut self) {
        // Dropping writer_tx signals the writer task to shut down after draining.
        // (The channel closes when all Senders drop; the task sees Err on recv.)
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

/// Writer task: drains events from the inbound channel, batch-persists them in
/// a single transaction (up to WRITER_BATCH_SIZE or WRITER_BATCH_WINDOW_MS),
/// resolves each oneshot with the result, and broadcasts each stored event in
/// order. Stops when the channel closes (all EventBus handles dropped).
///
/// **Shutdown invariant**: The task receives `None` from `rx.recv()` only after
/// all `EventBus` clones (and their `writer_tx` senders) have been dropped. This
/// guarantees no `publish()` call can be awaiting a response when the task exits,
/// because `publish()` requires a live `writer_tx` to send the request. Any pending
/// events at shutdown are flushed before the task returns.
async fn writer_task(
    store: Store,
    mut rx: mpsc::Receiver<WriterRequest>,
    broadcast_tx: broadcast::Sender<Arc<Event>>,
) {
    let batch_window = std::time::Duration::from_millis(WRITER_BATCH_WINDOW_MS);
    let mut pending: Vec<WriterRequest> = Vec::new();
    let mut deadline: Option<std::pin::Pin<Box<tokio::time::Sleep>>> = None;

    loop {
        tokio::select! {
            // Recv new event publish requests.
            recv = rx.recv() => match recv {
                Some(req) => {
                    pending.push(req);
                    // Arm deadline on first event if not already armed.
                    if deadline.is_none() {
                        deadline = Some(Box::pin(tokio::time::sleep(batch_window)));
                    }
                    // Flush if we've hit the batch size limit.
                    if pending.len() >= WRITER_BATCH_SIZE {
                        flush_batch(&store, &mut pending, &broadcast_tx).await;
                        deadline = None;
                    }
                }
                // Channel closed (all EventBus dropped): flush remaining, then stop.
                None => {
                    if !pending.is_empty() {
                        flush_batch(&store, &mut pending, &broadcast_tx).await;
                    }
                    return;
                }
            },
            // Batch window elapsed → flush accumulated events.
            _ = async { deadline.as_mut().unwrap().await }, if deadline.is_some() => {
                deadline = None;
                if !pending.is_empty() {
                    flush_batch(&store, &mut pending, &broadcast_tx).await;
                }
            }
        }
    }
}

/// Helper: batch-insert pending events, resolve oneshots, and broadcast in order.
async fn flush_batch(
    store: &Store,
    pending: &mut Vec<WriterRequest>,
    broadcast_tx: &broadcast::Sender<Arc<Event>>,
) {
    let events: Vec<NewEvent> = pending.iter().map(|(ev, _)| ev.clone()).collect();
    let result = store.insert_events(&events).await;

    match result {
        Ok(stored) => {
            // Resolve each oneshot with its corresponding event.
            for (i, (_, tx)) in pending.drain(..).enumerate() {
                let evt = stored[i].clone();
                let _ = tx.send(Ok(evt.clone()));
                // Broadcast in insertion order (append-then-broadcast semantics).
                let _ = broadcast_tx.send(Arc::new(evt));
            }
        }
        Err(e) => {
            // On batch failure, resolve all oneshots with the same error message.
            let err_msg = format!("batch insert failed: {e}");
            for (_, tx) in pending.drain(..) {
                let _ = tx.send(Err(Error::Internal(err_msg.clone())));
            }
        }
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
                // Slow consumer: skip dropped events (persisted events remain in
                // the log; transient events are lost, matching their semantics).
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
