//! Lossless egress conflation of high-volume stream events under backpressure.
//!
//! When a connection's outbound queue is congested (its bounded mpsc lane is
//! full), the per-subscription forwarders in [`crate::conn`] buffer the
//! *conflatable* transient stream types here instead of blocking, merging
//! same-key arrivals so a slow consumer receives a bounded, superseding set of
//! frames rather than the full firehose — without ever dropping content:
//!
//! - `chat:stream:delta` — per block: a full-text delta (CS-0 D2 default
//!   encoding) merges latest-wins — each carries the FULL accumulated text,
//!   so the newest supersedes; an incremental delta (`textDelta`,
//!   monorepo#2675) merges by fragment concat in arrival order (size-capped;
//!   an oversized merge seals the entry), like `terminal:data`.
//! - `agent:stream:activity` — latest-wins per agent: a content-free signal
//!   carrying the newest live preview.
//! - `terminal:data` — byte-concat per terminal: chunks are decoded, merged
//!   in arrival order, and re-encoded as one frame (size-capped; an oversized
//!   merge seals the entry and starts a new one).
//! - `file:*` — latest-wins per (workspace, path): refetch triggers, not
//!   content carriers; burst summaries carry `path` = directory, so
//!   per-directory conflation falls out of the same key.
//!
//! Everything else is never conflated: the forwarders treat a non-conflatable
//! frame as a barrier that flushes this buffer first (preserving order — a
//! conflated frame always lands before its stream's terminal event such as
//! `agent:stream:end` or `terminal:exit`) and then blocks as before. Under no
//! congestion the buffer stays empty and frames pass straight through, so
//! local/fast clients see zero added latency.

use std::collections::{HashMap, VecDeque};

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use intent_core::events::{AGENT_STREAM_ACTIVITY, CHAT_STREAM_DELTA, FILE_PREFIX, TERMINAL_DATA};
use intent_core::Event;
use serde_json::{json, Value};
use tokio::sync::mpsc;

use crate::events;
use crate::subscriptions;

/// Cap on one merged `terminal:data` frame's decoded byte length. A merge that
/// would exceed it is refused: the pending entry is sealed in place and the
/// newer chunk starts a fresh entry, so no merged frame ever approaches
/// [`crate::MAX_OUTBOUND_MESSAGE_BYTES`].
pub(crate) const TERMINAL_CONCAT_CAP_BYTES: usize = 256 * 1024;

/// Cap on one conflated chat entity's merged `textDelta` byte length
/// (incremental encoding, monorepo#2675). Same discipline as
/// [`TERMINAL_CONCAT_CAP_BYTES`]: a merge that would exceed it is refused, the
/// pending entry seals in place, and the newer fragment starts a fresh entry —
/// fragments compose across frames, so nothing is lost.
pub(crate) const CHAT_TEXT_CONCAT_CAP_BYTES: usize = 256 * 1024;

/// Cap on the number of pending entries in one [`ConflationBuffer`]. Conflation
/// bounds repeated keys, but a slow consumer observing unbounded *distinct*
/// keys (file paths, terminal ids) would otherwise grow the buffer without
/// limit. A push that would exceed this is rejected ([`Enqueue::Rejected`]) and
/// the forwarder falls back to the pre-conflation blocking (backpressured)
/// path, so the bound never loses data.
pub(crate) const MAX_PENDING_ENTRIES: usize = 256;

/// Cap on the total approximate payload bytes buffered in one
/// [`ConflationBuffer`], complementing [`MAX_PENDING_ENTRIES`] (terminal
/// entries alone can reach [`TERMINAL_CONCAT_CAP_BYTES`] each). Same rejection
/// semantics: over the cap, pushes are refused and the forwarder blocks.
pub(crate) const MAX_PENDING_BYTES: usize = 8 * 1024 * 1024;

/// Conflation key: frames with equal keys supersede/merge; distinct keys never
/// interact (no cross-stream reordering).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum Key {
    /// `agent:stream:activity` per agent.
    Activity(String),
    /// `terminal:data` per (workspace, terminal).
    Terminal(String, String),
    /// `file:*` per (workspace, path); burst summaries carry the directory.
    File(String, String),
    /// `chat:stream:delta` per block (chat-channel forwarder).
    ChatBlock(String),
}

/// The conflation key for a bus event on the `events.event` forwarder, or
/// `None` when the event type is not conflatable (barrier).
pub(crate) fn event_key(event: &Event) -> Option<Key> {
    let t = event.event_type.as_str();
    if t == AGENT_STREAM_ACTIVITY {
        let agent = event
            .session_id
            .clone()
            .or_else(|| str_field(&event.data, "agentId"))?;
        return Some(Key::Activity(agent));
    }
    if t == TERMINAL_DATA {
        let terminal = str_field(&event.data, "terminalId")?;
        return Some(Key::Terminal(event.workspace_id.to_string(), terminal));
    }
    if t.starts_with(FILE_PREFIX) {
        let path = str_field(&event.data, "path")?;
        return Some(Key::File(event.workspace_id.to_string(), path));
    }
    None
}

fn str_field(data: &Value, name: &str) -> Option<String> {
    data.get(name).and_then(Value::as_str).map(str::to_string)
}

/// How two same-key items combine. `merge` consumes the newer item and returns
/// `None` on success, or gives it back to refuse (the buffer then seals the
/// existing entry in place and appends the newer item as a fresh tail entry).
/// `cost` is the item's approximate payload byte size, feeding the buffer's
/// [`MAX_PENDING_BYTES`] bound.
pub(crate) trait Conflate: Sized {
    fn merge(&mut self, newer: Self) -> Option<Self>;
    fn cost(&self) -> usize;
}

/// Ordered pending-frame buffer: FIFO of entries plus a key index. Same-key
/// pushes merge into the pending entry IN PLACE (the first pending frame's
/// position is kept, so relative order across keys is preserved); `pop` drains
/// in arrival order. Hard-bounded by [`MAX_PENDING_ENTRIES`] entries and
/// [`MAX_PENDING_BYTES`] approximate payload bytes — a push that would exceed
/// either bound is handed back to the caller, never silently dropped.
pub(crate) struct ConflationBuffer<T> {
    /// `(id, key, cost, item)` — ids are strictly ascending along the deque;
    /// `cost` is the item's byte cost at insert/merge time.
    entries: VecDeque<(u64, Key, usize, T)>,
    /// key → id of its live (merge-target) entry.
    index: HashMap<Key, u64>,
    next_id: u64,
    /// Sum of every entry's `cost`.
    cost: usize,
    max_entries: usize,
    max_bytes: usize,
}

impl<T: Conflate> ConflationBuffer<T> {
    pub(crate) fn new() -> Self {
        Self::bounded(MAX_PENDING_ENTRIES, MAX_PENDING_BYTES)
    }

    /// A buffer with explicit bounds (tests exercise small ones).
    pub(crate) fn bounded(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            entries: VecDeque::new(),
            index: HashMap::new(),
            next_id: 0,
            cost: 0,
            max_entries,
            max_bytes,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Buffer one item: merge into the key's pending entry when present,
    /// otherwise append. A refused merge seals the old entry (it keeps its
    /// place but stops being a merge target) and appends the newer item.
    ///
    /// Returns `Some(item)` — the item handed back untouched — when accepting
    /// it would exceed the buffer's bounds: the byte bound for any push
    /// (conservatively pre-checked, since even a latest-wins merge may grow
    /// the entry), the entry bound for pushes that add an entry. The caller
    /// must then fall back to a blocking send.
    pub(crate) fn push(&mut self, key: Key, item: T) -> Option<T> {
        if self.cost + item.cost() > self.max_bytes {
            return Some(item);
        }
        if let Some(&id) = self.index.get(&key) {
            // Ids ascend along the deque, so the entry is found by binary search.
            let pos = self.entries.partition_point(|(eid, ..)| *eid < id);
            debug_assert!(pos < self.entries.len() && self.entries[pos].0 == id);
            let entry = &mut self.entries[pos];
            match entry.3.merge(item) {
                None => {
                    let new_cost = entry.3.cost();
                    self.cost = self.cost - entry.2 + new_cost;
                    entry.2 = new_cost;
                    return None;
                }
                Some(refused) => {
                    if self.entries.len() >= self.max_entries {
                        return Some(refused);
                    }
                    let id = self.alloc();
                    self.cost += refused.cost();
                    let cost = refused.cost();
                    self.entries.push_back((id, key.clone(), cost, refused));
                    self.index.insert(key, id);
                    return None;
                }
            }
        }
        if self.entries.len() >= self.max_entries {
            return Some(item);
        }
        let id = self.alloc();
        let cost = item.cost();
        self.cost += cost;
        self.entries.push_back((id, key.clone(), cost, item));
        self.index.insert(key, id);
        None
    }

    /// Remove and return the oldest pending item.
    pub(crate) fn pop(&mut self) -> Option<T> {
        let (id, key, cost, item) = self.entries.pop_front()?;
        self.cost -= cost;
        if self.index.get(&key) == Some(&id) {
            self.index.remove(&key);
        }
        Some(item)
    }

    fn alloc(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id
    }

    /// Drain every pending entry with blocking (backpressured) sends — the
    /// barrier path. Returns `false` when the outbound channel closed.
    pub(crate) async fn drain_all(
        &mut self,
        out_tx: &mpsc::Sender<String>,
        mut build: impl FnMut(T) -> String,
    ) -> bool {
        while let Some(item) = self.pop() {
            if out_tx.send(build(item)).await.is_err() {
                return false;
            }
        }
        true
    }
}

/// A pending bus event on the `events.event` forwarder.
pub(crate) enum EventItem {
    /// Latest-wins types (`agent:stream:activity`, `file:*`): the newest event
    /// supersedes the pending one.
    Latest(Event),
    /// `terminal:data`: decoded chunk bytes merged in arrival order. `bytes`
    /// is `None` when the chunk failed to decode — such an entry refuses
    /// merges (frame passes through unmodified, nothing is dropped).
    Terminal {
        event: Event,
        bytes: Option<Vec<u8>>,
    },
}

impl EventItem {
    /// Wrap a conflatable bus event for buffering, per its key kind.
    pub(crate) fn new(key: &Key, event: Event) -> Self {
        match key {
            Key::Terminal(..) => {
                let bytes = event
                    .data
                    .get("chunk")
                    .and_then(Value::as_str)
                    .and_then(|c| BASE64.decode(c).ok());
                EventItem::Terminal { event, bytes }
            }
            _ => EventItem::Latest(event),
        }
    }

    /// Serialize as the `events.event` notification frame, re-encoding a
    /// merged terminal chunk into the newest event's skeleton.
    pub(crate) fn into_frame(self, subscription_id: &str) -> String {
        match self {
            EventItem::Latest(event) => events::build_event_notification(subscription_id, &event),
            EventItem::Terminal { mut event, bytes } => {
                if let (Some(bytes), Some(obj)) = (bytes, event.data.as_object_mut()) {
                    obj.insert("chunk".to_string(), Value::String(BASE64.encode(bytes)));
                }
                events::build_event_notification(subscription_id, &event)
            }
        }
    }
}

impl Conflate for EventItem {
    fn cost(&self) -> usize {
        match self {
            EventItem::Latest(event) => approx_size(&event.data),
            EventItem::Terminal { event, bytes } => match bytes {
                Some(bytes) => bytes.len(),
                None => approx_size(&event.data),
            },
        }
    }

    fn merge(&mut self, newer: Self) -> Option<Self> {
        match (self, newer) {
            (EventItem::Latest(pending), EventItem::Latest(newer)) => {
                *pending = newer;
                None
            }
            (
                EventItem::Terminal { event, bytes },
                EventItem::Terminal {
                    event: new_event,
                    bytes: new_bytes,
                },
            ) => {
                let (Some(acc), Some(add)) = (bytes.as_mut(), new_bytes) else {
                    // An undecodable chunk on either side: refuse, preserving
                    // both frames verbatim.
                    return Some(EventItem::Terminal {
                        event: new_event,
                        bytes: None,
                    });
                };
                if acc.len() + add.len() > TERMINAL_CONCAT_CAP_BYTES {
                    return Some(EventItem::Terminal {
                        event: new_event,
                        bytes: Some(add),
                    });
                }
                acc.extend_from_slice(&add);
                // The merged frame rides the newest event's envelope
                // (id/timestamp), carrying all bytes in arrival order.
                let merged = std::mem::take(bytes);
                *event = new_event;
                *bytes = merged;
                None
            }
            // Key kinds never mix under one key; refuse defensively.
            (_, newer) => Some(newer),
        }
    }
}

/// A pending chat-channel block delta: the newest full-block entity, delivered
/// in the `added`/`updated` bucket of the FIRST pending delta for the block
/// (so a subscriber that never saw the block still receives it as `added`).
pub(crate) struct ChatItem {
    added: bool,
    entity: Value,
}

impl ChatItem {
    /// Extract the single upserted entity from a chunk delta, with its block
    /// id as the conflation key. `None` for any other delta shape (barrier).
    pub(crate) fn from_delta(delta: &Value) -> Option<(Key, Self)> {
        if !delta
            .get("removedIds")
            .and_then(Value::as_array)
            .is_some_and(Vec::is_empty)
        {
            return None;
        }
        let added_arr = delta.get("added").and_then(Value::as_array)?;
        let updated_arr = delta.get("updated").and_then(Value::as_array)?;
        let (added, entity) = match (added_arr.as_slice(), updated_arr.as_slice()) {
            ([entity], []) => (true, entity.clone()),
            ([], [entity]) => (false, entity.clone()),
            _ => return None,
        };
        let block_id = entity
            .get("block")
            .and_then(|b| b.get("id"))
            .and_then(Value::as_str)?
            .to_string();
        Some((Key::ChatBlock(block_id), ChatItem { added, entity }))
    }

    /// Rebuild the single-entity delta envelope for the wire.
    pub(crate) fn into_delta(self) -> Value {
        if self.added {
            json!({ "added": [self.entity], "updated": [], "removedIds": [] })
        } else {
            json!({ "added": [], "updated": [self.entity], "removedIds": [] })
        }
    }

    /// Serialize as a `subscription.push` delta frame at `seq`.
    pub(crate) fn into_frame(self, subscription_id: &str, seq: u64) -> String {
        subscriptions::build_delta_push(subscription_id, seq, &self.into_delta())
    }
}

impl Conflate for ChatItem {
    fn cost(&self) -> usize {
        approx_size(&self.entity)
    }

    fn merge(&mut self, newer: Self) -> Option<Self> {
        match (text_delta_of(&self.entity), text_delta_of(&newer.entity)) {
            // Incremental encoding (monorepo#2675): fragments compose in
            // arrival order, so append the newer fragment onto the pending
            // entity's `textDelta` — capped like `terminal:data`; an
            // oversized merge is refused (the entry seals, the newer
            // fragment starts a fresh one). The pending entity's envelope
            // (and the first delta's added/updated bucket) is preserved.
            (Some(acc), Some(add)) => {
                if acc.len() + add.len() > CHAT_TEXT_CONCAT_CAP_BYTES {
                    return Some(newer);
                }
                let add = add.to_string();
                if let Some(Value::String(slot)) = self
                    .entity
                    .get_mut("block")
                    .and_then(|b| b.get_mut("textDelta"))
                {
                    slot.push_str(&add);
                }
                None
            }
            // Full-text encoding: latest entity wins (each carries the FULL
            // accumulated text, CS-0 D2); the bucket of the first pending
            // delta is preserved.
            (None, None) => {
                self.entity = newer.entity;
                None
            }
            // Mixed shapes never occur within one subscription (the encoding
            // is fixed at subscribe time); refuse defensively.
            _ => Some(newer),
        }
    }
}

/// The `block.textDelta` string of a chat delta entity, or `None` for the
/// full-text encoding's entities (which carry `block.text` instead). Gated on
/// the mapper-owned `text`/`thinking` block types: non-text chunks pass
/// provider content through verbatim, so a foreign block that happens to carry
/// its own `textDelta` field must take the latest-entity-wins path, never the
/// concatenate path.
fn text_delta_of(entity: &Value) -> Option<&str> {
    let block = entity.get("block")?;
    match block.get("type").and_then(Value::as_str) {
        Some("text") | Some("thinking") => block.get("textDelta").and_then(Value::as_str),
        _ => None,
    }
}

/// Approximate in-memory payload size of a JSON value: string/byte content
/// plus a small per-node constant. Feeds the buffer's byte bound — an
/// estimate is enough, the bound is a memory backstop, not an exact quota.
fn approx_size(value: &Value) -> usize {
    match value {
        Value::Null | Value::Bool(_) | Value::Number(_) => 8,
        Value::String(s) => s.len() + 8,
        Value::Array(items) => items.iter().map(approx_size).sum::<usize>() + 8,
        Value::Object(map) => {
            map.iter()
                .map(|(k, v)| k.len() + approx_size(v))
                .sum::<usize>()
                + 8
        }
    }
}

/// Whether a chat-channel bus event's built delta may be conflated: only the
/// content chunk stream (`chat:stream:delta`) is — tool calls, terminal
/// reconciles, and message-row echoes are barriers.
pub(crate) fn chat_event_conflatable(event: &Event) -> bool {
    event.event_type == CHAT_STREAM_DELTA
}

/// Outcome of [`offer`]: sent straight through, buffered for conflation,
/// rejected because the buffer is at capacity (the caller must fall back to a
/// blocking send of the returned item), or the outbound channel is closed
/// (the forwarder must stop).
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Enqueue<T> {
    Sent,
    Buffered,
    Overflow(T),
    Closed,
}

/// The uncongested fast path with congestion fallback: when nothing is
/// pending and the outbound lane has room, the frame is sent immediately
/// (no added latency); otherwise the item enters the buffer, merging with any
/// pending same-key item. A buffer at its entry/byte bound hands the item
/// back as [`Enqueue::Overflow`] — the caller then blocks (original
/// backpressure semantics), so the bound never loses data.
pub(crate) fn offer<T: Conflate>(
    buffer: &mut ConflationBuffer<T>,
    key: Key,
    item: T,
    out_tx: &mpsc::Sender<String>,
    build: impl FnOnce(T) -> String,
) -> Enqueue<T> {
    if buffer.is_empty() {
        match out_tx.try_reserve() {
            Ok(permit) => {
                permit.send(build(item));
                return Enqueue::Sent;
            }
            Err(mpsc::error::TrySendError::Closed(())) => return Enqueue::Closed,
            Err(mpsc::error::TrySendError::Full(())) => {}
        }
    }
    match buffer.push(key, item) {
        None => Enqueue::Buffered,
        Some(rejected) => Enqueue::Overflow(rejected),
    }
}

#[cfg(test)]
mod tests;
