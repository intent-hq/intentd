//! In-process event bus (§10).
//!
//! Append-then-broadcast over the M2.1 `event` store, mirroring
//! `~/src/intent/src/main/websocket-event-bridge.ts`: [`EventBus::publish`]
//! persists the event first (so the durable log is the source of truth), then
//! fans it out to live subscribers. [`EventBus::publish_transient`] mints an
//! event id and broadcasts WITHOUT persisting (for high-volume ephemeral events
//! like `chat:stream:delta`). [`EventBus::subscribe`] returns a
//! [`Subscription`] whose per-subscriber delivery task applies the
//! [`SubscriptionFilter`] and coalesces matched events within `batch_window`
//! (the TS `batchFlushWorker`: the timer starts on the first matched event).
//!
//! `file:*` events are the one family with hybrid persistence (see
//! [`is_transient_file_event`]): only agent-attributed file changes are durable
//! (they feed `event.agentActivity` / `event.workspaceSummary`); watcher-observed
//! changes attributed to the system/user are broadcast-only, since they are
//! high-volume noise that no read path queries back out of the log.

use std::sync::Arc;

use intent_core::{ActorType, Error, Event, Result};
use intent_store::{NewEvent, Store};
use serde_json::{json, Value};
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

/// Total attempts for a batch insert that fails transiently (write-pool
/// acquire timeout / SQLITE_BUSY under contention — the write pool has
/// `max_connections=1`, so bursts serialize at `pool.acquire()`). Because the
/// bus is append-then-broadcast, a failed batch is lost for live subscribers
/// too (monorepo#2673), so transient contention is worth a couple of retries
/// before declaring the batch dead. Permanent failures (constraint
/// violations, serialization errors) never retry.
pub(crate) const INSERT_RETRY_MAX_ATTEMPTS: u32 = 3;

/// Base backoff between insert retry attempts; attempt N sleeps N times this
/// (25ms, then 50ms). Short on purpose: the contention observed in practice
/// clears in tens of milliseconds, and while retrying the writer task is not
/// draining its channel, so publishers feel backpressure sooner.
const INSERT_RETRY_BACKOFF: std::time::Duration = std::time::Duration::from_millis(25);

/// Byte cap on the persisted `data_json` of `agent:tool:call` events. Payloads
/// at or under the cap persist verbatim; larger ones have their free-form
/// fields (`output`, `input`, `registeredAttachments`) replaced with a bounded
/// preview + original byte count before the row is written. 16 KiB sits at the
/// low end of the 16–64 KiB range considered: on the dev seat the mean payload
/// is ~3 KiB (so >96% of events persist untouched) while the ~3% of rows above
/// 16 KiB carried ~33% of all tool-call bytes — the cap bounds those outliers
/// (historically multi-MB tool outputs) without losing signal for any reader.
/// No consumer reads persisted `input`/`output` back: conversation replay uses
/// `agent_message` rows, `event.agentActivity`/`event.workspaceSummary` read
/// only `data.filesModified`, and the FE synthesizes live tool blocks from the
/// broadcast, which keeps the FULL payload — only the durable row is capped.
pub(crate) const TOOL_CALL_PERSIST_CAP_BYTES: usize = 16 * 1024;

/// Serialized-JSON prefix retained as `preview` on each truncated field.
const TOOL_CALL_FIELD_PREVIEW_BYTES: usize = 2 * 1024;

/// Request sent to the writer task: the event to persist and a oneshot to
/// return the result (or error).
pub(crate) type WriterRequest = (NewEvent, oneshot::Sender<Result<Event>>);

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
    ///
    /// Non-agent `file:*` events are downgraded to a transient broadcast
    /// ([`is_transient_file_event`]) so watcher noise never reaches SQLite;
    /// callers see the same `Ok(Event)` shape either way.
    pub async fn publish(&self, ev: &NewEvent) -> Result<Event> {
        if is_transient_file_event(ev) {
            let event = self.publish_transient(ev);
            // The persisted path awaits `writer_tx.send()`, which yields and lets
            // delivery tasks drain the broadcast buffer. The transient path never
            // pends, so a non-collapsing burst (e.g. the watcher's shutdown
            // `flush_all`) could otherwise push past BROADCAST_CAPACITY in one
            // non-yielding loop and lag every subscriber off events that — being
            // transient — are not recoverable from the log. Yield to keep the
            // publish loop cooperative.
            tokio::task::yield_now().await;
            return Ok(event);
        }
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
    /// `chat:stream:delta`) that do not need durable storage. The wire shape
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
/// a single transaction (up to WRITER_BATCH_SIZE per batch), resolves each
/// oneshot with the result, and broadcasts each stored event in order. Stops
/// when the channel closes (all EventBus handles dropped).
///
/// **Latency/batching**: after awaiting the first event, the task greedily
/// drains whatever is already queued (`try_recv`) and flushes immediately — a
/// lone publish commits without any artificial batch-window wait, while
/// sustained bursts still coalesce because events that queue during the
/// previous flush's SQLite commit drain into the next batch.
///
/// **Shutdown invariant**: The task receives `None` from `rx.recv()` only after
/// all `EventBus` clones (and their `writer_tx` senders) have been dropped. This
/// guarantees no `publish()` call can be awaiting a response when the task exits,
/// because `publish()` requires a live `writer_tx` to send the request. Every
/// iteration flushes `pending` before looping, and `recv()` keeps returning
/// buffered events after the channel closes (before yielding `None`), so no
/// event is dropped at shutdown.
async fn writer_task(
    store: Store,
    mut rx: mpsc::Receiver<WriterRequest>,
    broadcast_tx: broadcast::Sender<Arc<Event>>,
) {
    let mut pending: Vec<WriterRequest> = Vec::with_capacity(WRITER_BATCH_SIZE);

    loop {
        // Idle: block until the next publish arrives (or the channel closes).
        match rx.recv().await {
            Some(req) => pending.push(req),
            // Channel closed (all EventBus dropped): nothing pending (every
            // iteration flushes before looping), so just stop.
            None => return,
        }
        // Greedily drain events that are already queued, up to the batch size;
        // any leftovers stay in the channel for the next iteration.
        while pending.len() < WRITER_BATCH_SIZE {
            match rx.try_recv() {
                Ok(req) => pending.push(req),
                Err(_) => break,
            }
        }
        flush_batch(&store, &mut pending, &broadcast_tx).await;
    }
}

/// Helper: batch-insert pending events, resolve oneshots, and broadcast in order.
/// Oversized `agent:tool:call` payloads are bounded in the persisted copy only
/// ([`TOOL_CALL_PERSIST_CAP_BYTES`]); the broadcast (and the publisher's
/// returned event) keeps the original full payload so live consumers (§7.1
/// tool-block synthesis) are unaffected.
async fn flush_batch(
    store: &Store,
    pending: &mut Vec<WriterRequest>,
    broadcast_tx: &broadcast::Sender<Arc<Event>>,
) {
    let events: Vec<NewEvent> = pending
        .iter()
        .map(|(ev, _)| truncate_tool_call_for_persist(ev).unwrap_or_else(|| ev.clone()))
        .collect();
    flush_prepared(|| store.insert_events(&events), pending, broadcast_tx).await;
}

/// Insert-retry + resolve/broadcast core of [`flush_batch`], generic over the
/// insert operation so tests can inject failures (monorepo#2673).
///
/// Transient insert failures ([`is_transient_insert_error`]) retry up to
/// [`INSERT_RETRY_MAX_ATTEMPTS`] total attempts with a short linear backoff
/// ([`INSERT_RETRY_BACKOFF`]); permanent failures fail immediately. On final
/// failure the batch is dropped for live subscribers too (append-then-
/// broadcast: no durable append → no broadcast), so the drop is logged at
/// error level with the event count and types before the publishers'
/// oneshots resolve with the error.
pub(crate) async fn flush_prepared<F, Fut>(
    mut insert: F,
    pending: &mut Vec<WriterRequest>,
    broadcast_tx: &broadcast::Sender<Arc<Event>>,
) where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<Vec<Event>>>,
{
    let mut attempt = 1u32;
    let result = loop {
        match insert().await {
            Ok(stored) => break Ok(stored),
            Err(e) if attempt < INSERT_RETRY_MAX_ATTEMPTS && is_transient_insert_error(&e) => {
                tracing::warn!(
                    attempt,
                    events = pending.len(),
                    error = %e,
                    "transient event batch insert failure; retrying"
                );
                tokio::time::sleep(INSERT_RETRY_BACKOFF * attempt).await;
                attempt += 1;
            }
            Err(e) => break Err(e),
        }
    };

    match result {
        Ok(stored) => {
            // Resolve each oneshot with its corresponding event.
            for (i, (orig, tx)) in pending.drain(..).enumerate() {
                let mut evt = stored[i].clone();
                // Restore the original payload (identical when not truncated).
                evt.data = orig.data;
                let _ = tx.send(Ok(evt.clone()));
                // Broadcast in insertion order (append-then-broadcast semantics).
                let _ = broadcast_tx.send(Arc::new(evt));
            }
        }
        Err(e) => {
            // The batch is lost both durably and live: publishers get the
            // error, but subscribers (FE UI, agent subscriptions, hooks) see
            // nothing at all — log loudly so drops are visible (monorepo#2673).
            let mut event_types: Vec<&str> = pending
                .iter()
                .map(|(ev, _)| ev.event_type.as_str())
                .collect();
            event_types.sort_unstable();
            event_types.dedup();
            tracing::error!(
                events = pending.len(),
                event_types = ?event_types,
                error = %e,
                "event batch insert failed; dropping batch (not persisted, not broadcast)"
            );
            // On batch failure, resolve all oneshots with the same error message.
            let err_msg = format!("batch insert failed: {e}");
            for (_, tx) in pending.drain(..) {
                let _ = tx.send(Err(Error::Internal(err_msg.clone())));
            }
        }
    }
}

/// Whether a batch-insert error is transient — worth retrying because it
/// reflects momentary contention, not a defect in the batch itself: the
/// single-connection write pool's acquire timed out (`insert_events` maps
/// this to "acquire connection failed: pool timed out …"), or SQLite
/// reported the database busy/locked (a cross-process writer holding the
/// lock past `busy_timeout`). Everything else (constraint violations,
/// payload serialization failures, I/O errors) is permanent and fails the
/// batch immediately. String matching is the only classification available:
/// `insert_events` flattens every failure into `Error::Internal(String)`.
fn is_transient_insert_error(e: &Error) -> bool {
    let msg = e.to_string();
    msg.contains("acquire connection failed")
        || msg.contains("pool timed out")
        || msg.contains("database is locked")
        || msg.contains("database table is locked")
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

/// Whether `ev` is a `file:*` event that must be broadcast-only.
///
/// Hybrid `file:*` persistence: agent-attributed file changes stay durable
/// because `event.agentActivity` and `event.workspaceSummary` read them back out
/// of the log. Everything else (the watcher's system-attributed changes, user
/// edits) is broadcast to live subscribers and dropped — high-volume rows with
/// no reader.
pub(crate) fn is_transient_file_event(ev: &NewEvent) -> bool {
    ev.event_type.starts_with(intent_core::events::FILE_PREFIX)
        && ev.actor.actor_type != ActorType::Agent
}

/// Returns a persistence copy of `ev` with its `data` bounded to
/// [`TOOL_CALL_PERSIST_CAP_BYTES`], or `None` when the event is not an
/// `agent:tool:call` or its payload already fits (persist the original as-is).
///
/// Oversized payloads have each free-form field (`output`, `input`,
/// `registeredAttachments`) replaced with a marker object:
/// `{ "truncated": true, "originalBytes": N, "preview": "…" }` where `preview`
/// is the first [`TOOL_CALL_FIELD_PREVIEW_BYTES`] bytes of the field's
/// serialized JSON. If the payload is still over the cap afterwards (e.g. an
/// unexpectedly huge scalar field), everything except identity fields is
/// dropped and a top-level `"truncated": true` is set.
fn truncate_tool_call_for_persist(ev: &NewEvent) -> Option<NewEvent> {
    if ev.event_type != intent_core::events::AGENT_TOOL_CALL {
        return None;
    }
    if json_byte_len(&ev.data) <= TOOL_CALL_PERSIST_CAP_BYTES {
        return None;
    }
    let mut data = ev.data.clone();
    if let Some(obj) = data.as_object_mut() {
        for field in ["output", "input", "registeredAttachments"] {
            if let Some(v) = obj.get(field) {
                let serialized = v.to_string();
                if serialized.len() > TOOL_CALL_FIELD_PREVIEW_BYTES {
                    obj.insert(
                        field.to_string(),
                        json!({
                            "truncated": true,
                            "originalBytes": serialized.len(),
                            "preview": utf8_prefix(&serialized, TOOL_CALL_FIELD_PREVIEW_BYTES),
                        }),
                    );
                }
            }
        }
    }
    // Defensive fallback: some other field is unexpectedly huge. Keep only
    // identity fields (plus `filesModified`, the one field `event.agentActivity`
    // reads back from these rows) so the row stays bounded.
    if json_byte_len(&data) > TOOL_CALL_PERSIST_CAP_BYTES {
        if let Some(obj) = data.as_object_mut() {
            let keep = [
                "toolCallId",
                "toolName",
                "title",
                "status",
                "agentId",
                "toolKind",
                "filesModified",
            ];
            obj.retain(|k, _| keep.contains(&k.as_str()));
            obj.insert("truncated".to_string(), Value::Bool(true));
        }
    }
    Some(NewEvent {
        workspace_id: ev.workspace_id.clone(),
        timestamp: ev.timestamp.clone(),
        event_type: ev.event_type.clone(),
        actor: ev.actor.clone(),
        session_id: ev.session_id.clone(),
        correlation_id: ev.correlation_id.clone(),
        parent_event_id: ev.parent_event_id.clone(),
        metadata: ev.metadata.clone(),
        data,
    })
}

/// Byte length of a value's serialized JSON (what `insert_events` writes).
fn json_byte_len(v: &Value) -> usize {
    serde_json::to_string(v)
        .map(|s| s.len())
        .unwrap_or(usize::MAX)
}

/// The longest prefix of `s` that is at most `max_bytes` bytes and ends on a
/// UTF-8 character boundary.
fn utf8_prefix(s: &str, max_bytes: usize) -> &str {
    if s.len() <= max_bytes {
        return s;
    }
    let mut end = max_bytes;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}
