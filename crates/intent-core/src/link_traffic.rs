//! Owner-only capture of executor-link traffic (monorepo
//! `docs/protocol/methods/nodes.md`, "Daemon-link traffic").
//!
//! A daemon keeps one [`LinkTraffic`]: the backend taps its outbound links
//! to exposed machines and a machine taps its inbound executor links, each
//! through a [`LinkTap`] naming the peer. Capture is off until
//! [`LinkTraffic::start`]: while off, observing a frame is one atomic load.
//! While on, the link task clones the frame into a bounded queue (never
//! waiting: a full queue drops the record and counts it), and a collector
//! task redacts it, renders the record and keeps it in a bounded ring
//! (evicting, and counting, the oldest), then publishes it in batches.
//! [`LinkTraffic::stop`] turns capture off and clears everything. Nothing is
//! persisted.
//!
//! Redaction happens before a record enters the ring: profile and script
//! environment values (every member named `env`), any key naming a token, secret, password, credential,
//! authorization, cookie, private key or certificate, and any PEM block are
//! replaced by `"***"`. Payloads larger than the limit after redaction are
//! omitted (`truncated: true`), the record itself kept.

use std::collections::VecDeque;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use serde::Serialize;
use serde_json::{json, Map, Value};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio::sync::mpsc;

use crate::node_link::{JournalBody, LinkFrame, LinkOutcome};

/// Records kept before the oldest is evicted.
pub const MAX_RECORDS: usize = 2000;
/// Largest redacted payload kept, in bytes of JSON.
pub const MAX_PAYLOAD_BYTES: usize = 64 * 1024;
/// Records a snapshot returns when no limit is given, and at most.
pub const SNAPSHOT_DEFAULT_LIMIT: usize = 500;
pub const SNAPSHOT_MAX_LIMIT: usize = 2000;
/// Most bytes of record JSON one snapshot returns, so a reply stays within
/// the read payload budget: past it a snapshot returns fewer than `limit`
/// records (always at least one when any match), and the client pages on.
pub const SNAPSHOT_MAX_BYTES: usize = 448 * 1024;
/// Frames waiting for the collector before new ones are dropped.
const QUEUE: usize = 1024;
/// Wire bytes of the frames waiting for the collector before new ones are
/// dropped: two of the largest frames a link carries.
const QUEUE_BYTES: usize = 16 * 1024 * 1024;
/// What a light capture counts against the queue's byte budget, and the
/// most JSON its record holds: every string it copies is clipped to
/// [`FIELD_BYTES`] and its summary to [`SUMMARY_BYTES`].
pub(crate) const LIGHT_BYTES: usize = 1024;
/// Longest string a light capture copies from a frame.
const FIELD_BYTES: usize = 96;
/// Longest summary a record keeps.
const SUMMARY_BYTES: usize = 200;
/// Most records in one published batch, and the longest a record waits for
/// its batch.
const BATCH_MAX: usize = 100;
const BATCH_WINDOW: Duration = Duration::from_millis(250);

/// The mask written over a secret.
const MASK: &str = "***";

/// Which way a frame went, relative to this daemon.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TrafficDirection {
    In,
    Out,
}

/// What the other end of a link is to this daemon.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PeerKind {
    /// An exposed machine this backend dialed.
    Machine,
    /// A backend that bound this machine's executor link.
    Backend,
}

/// The other end of a link.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct TrafficPeer {
    pub kind: PeerKind,
    pub id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

/// The capture's bounds.
#[derive(Debug, Clone, Copy)]
pub struct Limits {
    pub max_records: usize,
    pub max_payload_bytes: usize,
    pub queue: usize,
    /// Wire bytes of the frames waiting for the collector, at most.
    pub queue_bytes: usize,
    pub batch_window: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_records: MAX_RECORDS,
            max_payload_bytes: MAX_PAYLOAD_BYTES,
            queue: QUEUE,
            queue_bytes: QUEUE_BYTES,
            batch_window: BATCH_WINDOW,
        }
    }
}

/// Receives each published batch, `{ records, capture }`.
pub type RecordsSink = Arc<dyn Fn(Value) + Send + Sync>;

/// One frame waiting for the collector.
struct Captured {
    at: OffsetDateTime,
    peer: TrafficPeer,
    direction: TrafficDirection,
    body: Body,
    bytes: usize,
    /// What it counts against the queue's byte budget.
    cost: usize,
}

/// What a waiting frame keeps.
enum Body {
    /// The whole frame, to render with its payload.
    Full(Box<LinkFrame>),
    /// Only the type, ids and summary fields (see [`light_frame`]): the
    /// payload would be omitted anyway (payloads off, or the frame is past
    /// `max_payload_bytes`), so the frame is never cloned.
    Light { frame: Value, truncated: bool },
}

/// A running capture.
struct Session {
    epoch: u64,
    started_at: String,
    tx: mpsc::Sender<Captured>,
    /// Bytes waiting for the collector (each frame's `cost`).
    queued: Arc<AtomicUsize>,
    /// Each record with its seq and its JSON size in bytes.
    records: VecDeque<(u64, Value, usize)>,
    next_seq: u64,
    evicted: u64,
    dropped: u64,
    oversized: u64,
}

#[derive(Default)]
struct State {
    session: Option<Session>,
    epochs: u64,
    /// The last capture's start, so the next one's `startedAt` is strictly
    /// later: clients tell captures apart by it.
    last_start: Option<OffsetDateTime>,
}

/// A daemon's link-traffic capture. See the module docs.
pub struct LinkTraffic {
    active: AtomicBool,
    payloads: AtomicBool,
    limits: Limits,
    state: Mutex<State>,
    /// Held from reading the status a batch carries until it is published,
    /// so batches go out in the order of the state they describe: no batch
    /// follows the stop that cleared it. Taken before `state`.
    publishing: Mutex<()>,
    sink: Mutex<Option<RecordsSink>>,
}

impl Default for LinkTraffic {
    fn default() -> Self {
        Self::with_limits(Limits::default())
    }
}

impl std::fmt::Debug for LinkTraffic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LinkTraffic")
            .field("active", &self.is_active())
            .finish_non_exhaustive()
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// `now` at millisecond precision, or 1 ms after `last` when that is not
/// strictly earlier, so each capture's `startedAt` differs.
fn next_start(last: Option<OffsetDateTime>, now: OffsetDateTime) -> OffsetDateTime {
    let now = now
        .replace_nanosecond(now.nanosecond() / 1_000_000 * 1_000_000)
        .unwrap_or(now);
    match last {
        Some(last) if last >= now => last + time::Duration::milliseconds(1),
        _ => now,
    }
}

fn iso_ms(at: OffsetDateTime) -> String {
    let at = at
        .replace_nanosecond(at.nanosecond() / 1_000_000 * 1_000_000)
        .unwrap_or(at);
    at.format(&Rfc3339).unwrap_or_default()
}

impl LinkTraffic {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_limits(limits: Limits) -> Self {
        Self {
            active: AtomicBool::new(false),
            payloads: AtomicBool::new(true),
            limits,
            state: Mutex::new(State::default()),
            publishing: Mutex::new(()),
            sink: Mutex::new(None),
        }
    }

    /// Where published batches go (the `remoteLinkTraffic:records` event).
    pub fn set_sink(&self, sink: RecordsSink) {
        *lock(&self.sink) = Some(sink);
    }

    /// Whether capture is on.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::Relaxed)
    }

    /// Turn capture on, or, while on, only update whether payloads are
    /// kept. Returns the capture status, also published with no records.
    /// Must run inside a Tokio runtime.
    pub fn start(self: &Arc<Self>, payloads: bool) -> Value {
        let _publishing = lock(&self.publishing);
        let mut state = lock(&self.state);
        self.payloads.store(payloads, Ordering::Relaxed);
        if state.session.is_none() {
            state.epochs += 1;
            let epoch = state.epochs;
            let (tx, rx) = mpsc::channel(self.limits.queue.max(1));
            let queued = Arc::new(AtomicUsize::new(0));
            let started_at = next_start(state.last_start, OffsetDateTime::now_utc());
            state.last_start = Some(started_at);
            state.session = Some(Session {
                epoch,
                started_at: iso_ms(started_at),
                tx,
                queued: queued.clone(),
                records: VecDeque::new(),
                next_seq: 1,
                evicted: 0,
                dropped: 0,
                oversized: 0,
            });
            // caller-binding: allow — the capture's collector; it ends when the capture stops
            tokio::spawn(collect(
                Arc::downgrade(self),
                epoch,
                rx,
                queued,
                self.limits.batch_window,
            ));
            self.active.store(true, Ordering::Relaxed);
        }
        let status = self.status_of(&state);
        drop(state);
        self.publish(&[], &status);
        status
    }

    /// Turn capture off and clear it; a no-op while off. Returns the
    /// (cleared) status, also published with no records when capture was
    /// on.
    pub fn stop(&self) -> Value {
        let _publishing = lock(&self.publishing);
        let mut state = lock(&self.state);
        self.active.store(false, Ordering::Relaxed);
        let was_on = state.session.take().is_some();
        self.payloads.store(true, Ordering::Relaxed);
        let status = self.status_of(&state);
        drop(state);
        if was_on {
            self.publish(&[], &status);
        }
        status
    }

    fn publish(&self, records: &[Value], capture: &Value) {
        let sink = lock(&self.sink).clone();
        if let Some(sink) = sink {
            sink(json!({ "records": records, "capture": capture }));
        }
    }

    /// The capture status (`CaptureStatus`).
    #[must_use]
    pub fn status(&self) -> Value {
        self.status_of(&lock(&self.state))
    }

    /// The retained records after `after_seq` (exclusive), oldest first, at
    /// most `limit` (default [`SNAPSHOT_DEFAULT_LIMIT`], capped at
    /// [`SNAPSHOT_MAX_LIMIT`]) and stopping before their JSON passes
    /// [`SNAPSHOT_MAX_BYTES`] (at least one record when any match):
    /// `{ capture, records }`.
    #[must_use]
    pub fn snapshot(&self, after_seq: Option<u64>, limit: Option<usize>) -> Value {
        let limit = limit
            .unwrap_or(SNAPSHOT_DEFAULT_LIMIT)
            .min(SNAPSHOT_MAX_LIMIT);
        let after = after_seq.unwrap_or(0);
        let state = lock(&self.state);
        let mut records: Vec<Value> = Vec::new();
        // `[` and `]`, then each record and its separating comma.
        let mut bytes: usize = 2;
        for (_, record, size) in state
            .session
            .iter()
            .flat_map(|session| session.records.iter())
            .filter(|(seq, _, _)| *seq > after)
            .take(limit)
        {
            let size = size + 1;
            if !records.is_empty() && bytes.saturating_add(size) > SNAPSHOT_MAX_BYTES {
                break;
            }
            bytes = bytes.saturating_add(size);
            records.push(record.clone());
        }
        json!({ "capture": self.status_of(&state), "records": records })
    }

    fn status_of(&self, state: &State) -> Value {
        let mut status = Map::new();
        status.insert("active".into(), json!(state.session.is_some()));
        if let Some(session) = &state.session {
            status.insert("startedAt".into(), json!(session.started_at));
        }
        status.insert(
            "payloads".into(),
            json!(self.payloads.load(Ordering::Relaxed)),
        );
        status.insert("maxRecords".into(), json!(self.limits.max_records));
        status.insert(
            "maxPayloadBytes".into(),
            json!(self.limits.max_payload_bytes),
        );
        let session = state.session.as_ref();
        status.insert(
            "retained".into(),
            json!(session.map_or(0, |s| s.records.len())),
        );
        status.insert("evicted".into(), json!(session.map_or(0, |s| s.evicted)));
        status.insert("dropped".into(), json!(session.map_or(0, |s| s.dropped)));
        status.insert(
            "oversized".into(),
            json!(session.map_or(0, |s| s.oversized)),
        );
        status.insert("nextSeq".into(), json!(session.map_or(1, |s| s.next_seq)));
        Value::Object(status)
    }

    fn observe(
        &self,
        peer: &TrafficPeer,
        direction: TrafficDirection,
        frame: &LinkFrame,
        bytes: usize,
    ) {
        if !self.is_active() {
            return;
        }
        let payloads = self.payloads.load(Ordering::Relaxed);
        let light = !payloads || bytes > self.limits.max_payload_bytes;
        let cost = if light { LIGHT_BYTES } else { bytes };
        // Reserve the frame's place under the lock; copy it after.
        let (epoch, tx, queued) = {
            let mut state = lock(&self.state);
            let Some(session) = state.session.as_mut() else {
                return;
            };
            if session.queued.load(Ordering::Relaxed).saturating_add(cost) > self.limits.queue_bytes
            {
                session.dropped += 1;
                return;
            }
            session.queued.fetch_add(cost, Ordering::Relaxed);
            (session.epoch, session.tx.clone(), session.queued.clone())
        };
        let body = if light {
            Body::Light {
                frame: light_frame(frame),
                // Only a payload left out for size is truncated.
                truncated: payloads && bytes > self.limits.max_payload_bytes,
            }
        } else {
            Body::Full(Box::new(masked_copy(frame)))
        };
        let captured = Captured {
            at: OffsetDateTime::now_utc(),
            peer: peer.clone(),
            direction,
            body,
            bytes,
            cost,
        };
        if tx.try_send(captured).is_err() {
            queued.fetch_sub(cost, Ordering::Relaxed);
            let mut state = lock(&self.state);
            if let Some(session) = state.session.as_mut().filter(|s| s.epoch == epoch) {
                session.dropped += 1;
            }
        }
    }

    /// Bytes waiting for the collector.
    #[cfg(test)]
    pub(crate) fn queued_bytes(&self) -> usize {
        lock(&self.state)
            .session
            .as_ref()
            .map_or(0, |s| s.queued.load(Ordering::Relaxed))
    }

    /// Render `batch` and keep it, if capture `epoch` is still running;
    /// then publish it.
    fn ingest(&self, epoch: u64, batch: Vec<Captured>) {
        let payloads = self.payloads.load(Ordering::Relaxed);
        let drafts: Vec<Draft> = batch
            .into_iter()
            .map(|captured| render(&captured, payloads, self.limits.max_payload_bytes))
            .collect();
        let _publishing = lock(&self.publishing);
        let (records, capture) = {
            let mut state = lock(&self.state);
            let Some(session) = state.session.as_mut() else {
                return;
            };
            if session.epoch != epoch {
                return;
            }
            let mut records = Vec::with_capacity(drafts.len());
            for draft in drafts {
                let seq = session.next_seq;
                session.next_seq += 1;
                if draft.oversized {
                    session.oversized += 1;
                }
                let mut record = draft.record;
                record.insert("seq".into(), json!(seq));
                let record = Value::Object(record);
                session.records.push_back((seq, record.clone(), draft.size));
                while session.records.len() > self.limits.max_records {
                    session.records.pop_front();
                    session.evicted += 1;
                }
                records.push(record);
            }
            (records, self.status_of(&state))
        };
        self.publish(&records, &capture);
    }
}

/// Drain capture `epoch`'s queue in batches until the capture stops.
async fn collect(
    traffic: Weak<LinkTraffic>,
    epoch: u64,
    mut rx: mpsc::Receiver<Captured>,
    queued: Arc<AtomicUsize>,
    window: Duration,
) {
    loop {
        let Some(first) = rx.recv().await else {
            return;
        };
        let mut batch = vec![first];
        let deadline = tokio::time::Instant::now() + window;
        let mut open = true;
        while batch.len() < BATCH_MAX {
            match rx.try_recv() {
                Ok(next) => {
                    batch.push(next);
                    continue;
                }
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    open = false;
                    break;
                }
                Err(mpsc::error::TryRecvError::Empty) => {}
            }
            if window.is_zero() {
                break;
            }
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(next)) => batch.push(next),
                Ok(None) => {
                    open = false;
                    break;
                }
                Err(_) => break,
            }
        }
        let cost: usize = batch.iter().map(|captured| captured.cost).sum();
        queued.fetch_sub(cost, Ordering::Relaxed);
        let Some(traffic) = traffic.upgrade() else {
            return;
        };
        traffic.ingest(epoch, batch);
        if !open {
            return;
        }
    }
}

/// A rendered record without its `seq`.
struct Draft {
    record: Map<String, Value>,
    oversized: bool,
    /// The record's JSON size once its `seq` is added, at most.
    size: usize,
}

/// Room for `,"seq":` and a `u64`.
const SEQ_BYTES: usize = 28;

fn render(captured: &Captured, payloads: bool, max_payload_bytes: usize) -> Draft {
    let (mut payload, light) = match &captured.body {
        Body::Full(frame) => (serde_json::to_value(frame).unwrap_or(Value::Null), None),
        Body::Light { frame, truncated } => (frame.clone(), Some(*truncated)),
    };
    redact(&mut payload);
    let frame_type = payload
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("unknown")
        .to_owned();
    let mut record = Map::new();
    record.insert("at".into(), json!(iso_ms(captured.at)));
    record.insert("direction".into(), json!(captured.direction));
    record.insert("peer".into(), json!(captured.peer));
    record.insert("frameType".into(), json!(frame_type));
    if let Some(agent_id) = payload.get("agentId").and_then(Value::as_str) {
        record.insert("agentId".into(), json!(agent_id));
    }
    if let Some(request_id) = payload.get("requestId").and_then(Value::as_str) {
        record.insert("requestId".into(), json!(request_id));
    }
    record.insert("bytes".into(), json!(captured.bytes));
    record.insert(
        "summary".into(),
        json!(clip(&summarize(&frame_type, &payload), SUMMARY_BYTES)),
    );
    let mut oversized = false;
    if let Some(truncated) = light {
        if truncated {
            oversized = true;
            record.insert("truncated".into(), json!(true));
        }
    } else if payloads {
        let size = serde_json::to_vec(&payload).map_or(usize::MAX, |bytes| bytes.len());
        if size > max_payload_bytes {
            oversized = true;
            record.insert("truncated".into(), json!(true));
        } else {
            record.insert("payload".into(), payload);
        }
    }
    let size = serde_json::to_vec(&record).map_or(usize::MAX, |json| json.len()) + SEQ_BYTES;
    Draft {
        record,
        oversized,
        size,
    }
}

/// `frame` as the capture keeps it: an agent secret's value and a
/// terminal's input never enter the queue.
fn masked_copy(frame: &LinkFrame) -> LinkFrame {
    match frame {
        LinkFrame::PtyInput(input) => LinkFrame::PtyInput(crate::node_link::PtyInput {
            terminal_id: input.terminal_id.clone(),
            data: MASK.to_owned(),
        }),
        LinkFrame::SecretPut(put) => LinkFrame::SecretPut(crate::node_link::SecretPut {
            value: crate::node_link::SecretValue::from(MASK),
            ..put.clone()
        }),
        LinkFrame::ForgeToken(forge) => LinkFrame::ForgeToken(crate::node_link::ForgeToken {
            token: forge
                .token
                .as_ref()
                .map(|_| crate::node_link::SecretValue::from(MASK)),
            ..forge.clone()
        }),
        other => other.clone(),
    }
}

/// The parts of `frame` a record shows without its payload: the type, the
/// ids and the fields [`summarize`] reads, never a prompt, params body,
/// result, journal body or launch configuration.
fn light_frame(frame: &LinkFrame) -> Value {
    let agent = |id: &crate::AgentId| json!(clip(id.as_str(), FIELD_BYTES));
    let field = |text: &str| json!(clip(text, FIELD_BYTES));
    match frame {
        LinkFrame::Hello(hello) => json!({
            "type": "hello",
            "binding": { "generation": hello.binding.generation },
            "leaseAgentCount": hello.lease_agents.len(),
        }),
        LinkFrame::Welcome(welcome) => json!({
            "type": "welcome",
            "protocol": welcome.protocol,
            "runningAgentCount": welcome.running_agents.len(),
        }),
        LinkFrame::Heartbeat { nonce } => json!({ "type": "heartbeat", "nonce": nonce }),
        LinkFrame::HeartbeatReply { nonce } => json!({ "type": "heartbeatReply", "nonce": nonce }),
        LinkFrame::Launch(launch) => json!({
            "type": "launch",
            "agentId": agent(&launch.agent_id),
            "provider": field(&launch.provider),
            "model": launch.model.as_deref().map(field),
        }),
        LinkFrame::Prompt {
            agent_id, turn_id, ..
        } => json!({ "type": "prompt", "agentId": agent(agent_id), "turnId": field(turn_id) }),
        LinkFrame::Cancel { agent_id } => json!({ "type": "cancel", "agentId": agent(agent_id) }),
        LinkFrame::Stop {
            agent_id,
            stop_id,
            final_stop,
        } => {
            let mut light =
                json!({ "type": "stop", "agentId": agent(agent_id), "stopId": stop_id });
            if *final_stop {
                light["final"] = json!(true);
            }
            light
        }
        LinkFrame::StopAck {
            stop_id,
            agent_id,
            final_push,
            ..
        } => {
            let mut light =
                json!({ "type": "stopAck", "agentId": agent(agent_id), "stopId": stop_id });
            if let Some(push) = final_push {
                light["finalPush"] = json!(push);
            }
            light
        }
        LinkFrame::Journal(record) => {
            let mut light = json!({
                "type": "journal",
                "seq": record.seq,
                "body": { "kind": journal_kind(&record.body) },
            });
            match &record.owner {
                crate::node_link::RecordOwner::Agent(id) => light["agentId"] = agent(id),
                crate::node_link::RecordOwner::Home(id) => light["homeId"] = field(id.as_str()),
            }
            light
        }
        LinkFrame::Ack { seq } => json!({ "type": "ack", "seq": seq }),
        LinkFrame::ReplayComplete { through_seq } => {
            json!({ "type": "replayComplete", "throughSeq": through_seq })
        }
        LinkFrame::Request(request) => {
            let mut light = json!({
                "type": "request",
                "requestId": request.request_id,
                "agentId": agent(&request.agent_id),
                "method": field(&request.method),
            });
            if let Some(method) = request.params.get("method").and_then(Value::as_str) {
                light["params"] = json!({ "method": field(method) });
            }
            light
        }
        LinkFrame::Response {
            request_id,
            outcome,
        } => json!({
            "type": "response",
            "requestId": request_id,
            "outcome": match outcome {
                LinkOutcome::Ok(_) => json!({ "ok": null }),
                LinkOutcome::Err(error) => json!({ "err": { "code": error.code } }),
            },
        }),
        LinkFrame::CancelRequest { request_id } => {
            json!({ "type": "cancelRequest", "requestId": request_id })
        }
        LinkFrame::Capabilities => json!({ "type": "capabilities" }),
        LinkFrame::CapabilitiesReply { .. } => json!({ "type": "capabilitiesReply" }),
        LinkFrame::Skills(query) => json!({ "type": "skills", "queryId": query.query_id }),
        LinkFrame::SkillsReply(reply) => json!({
            "type": "skillsReply",
            "queryId": reply.query_id,
            "skills": reply.skills.len(),
        }),
        LinkFrame::ScriptStart(start) => json!({
            "type": "scriptStart",
            "agentId": agent(&start.agent_id),
            "scriptId": field(&start.script_id),
            "spawnId": field(&start.spawn_id),
            "mode": start.mode,
        }),
        LinkFrame::ScriptStop(stop) => json!({
            "type": "scriptStop",
            "agentId": agent(&stop.agent_id),
            "scriptId": field(&stop.script_id),
            "spawnId": field(&stop.spawn_id),
        }),
        LinkFrame::ScriptStarted(started) => json!({
            "type": "scriptStarted",
            "agentId": agent(&started.agent_id),
            "scriptId": field(&started.script_id),
            "spawnId": field(&started.spawn_id),
            "pid": started.pid,
        }),
        LinkFrame::ScriptOutput(output) => json!({
            "type": "scriptOutput",
            "agentId": agent(&output.agent_id),
            "scriptId": field(&output.script_id),
            "spawnId": field(&output.spawn_id),
            "seq": output.seq,
        }),
        LinkFrame::ScriptExited(exited) => json!({
            "type": "scriptExited",
            "agentId": agent(&exited.agent_id),
            "scriptId": field(&exited.script_id),
            "spawnId": field(&exited.spawn_id),
            "exitCode": exited.exit_code,
            "error": exited.error.as_deref().map(field),
        }),
        LinkFrame::SetupOutput(output) => {
            let mut light = json!({ "type": "setupOutput", "seq": output.seq });
            match &output.owner {
                crate::node_link::SetupOwner::Agent(id) => light["agentId"] = agent(id),
                crate::node_link::SetupOwner::Home(home) => {
                    light["homeId"] = field(home.home_id.as_str());
                }
            }
            light
        }
        LinkFrame::PushWip(request) => json!({
            "type": "pushWip",
            "requestId": request.request_id,
            "agentId": agent(&request.agent_id),
        }),
        LinkFrame::PushWipReply(reply) => json!({
            "type": "pushWipReply",
            "requestId": reply.request_id,
            "agentId": agent(&reply.agent_id),
            "outcome": reply.outcome,
            "error": reply.error.as_deref().map(field),
        }),
        LinkFrame::SecretPut(put) => json!({
            "type": "secretPut",
            "requestId": put.request_id,
            "agentId": agent(&put.agent_id),
            "name": field(&put.name),
        }),
        LinkFrame::SecretForget(forget) => json!({
            "type": "secretForget",
            "requestId": forget.request_id,
            "agentId": agent(&forget.agent_id),
            "name": forget.name.as_deref().map(field),
        }),
        LinkFrame::SecretList(list) => json!({
            "type": "secretList",
            "requestId": list.request_id,
            "agentId": agent(&list.agent_id),
        }),
        LinkFrame::SecretReply(reply) => json!({
            "type": "secretReply",
            "requestId": reply.request_id,
            "agentId": agent(&reply.agent_id),
            "outcome": reply.outcome,
            "error": reply.error.as_deref().map(field),
        }),
        LinkFrame::ForgeToken(forge) => json!({
            "type": "forgeToken",
            "requestId": forge.request_id,
            "host": field(&forge.host),
            "held": forge.token.is_some(),
        }),
        LinkFrame::ForgeTokenReply(reply) => json!({
            "type": "forgeTokenReply",
            "requestId": reply.request_id,
            "outcome": reply.outcome,
            "error": reply.error.as_deref().map(field),
        }),
        LinkFrame::OneShot(one_shot) => json!({
            "type": "oneShot",
            "requestId": one_shot.request_id,
            "operation": one_shot.operation,
            "timeoutMs": one_shot.timeout_ms,
            "profileEnv": one_shot.profile_env.is_some(),
        }),
        LinkFrame::OneShotCancel(cancel) => json!({
            "type": "oneShotCancel",
            "requestId": cancel.request_id,
        }),
        LinkFrame::OneShotReply(reply) => json!({
            "type": "oneShotReply",
            "requestId": reply.request_id,
            "outcome": reply.outcome,
            "code": reply.error.as_ref().map(|error| field(&error.code)),
        }),
        LinkFrame::HomeOpen(open) => json!({
            "type": "homeOpen",
            "requestId": open.request_id,
            "homeId": field(open.home_id.as_str()),
            "restore": open.restore.is_some(),
        }),
        LinkFrame::HomeOpenReply(reply) => json!({
            "type": "homeOpenReply",
            "requestId": reply.request_id,
            "homeId": field(reply.home_id.as_str()),
            "outcome": reply.outcome,
            "error": reply.error.as_deref().map(field),
        }),
        LinkFrame::HomePush(push) => json!({
            "type": "homePush",
            "requestId": push.request_id,
            "homeId": field(push.home_id.as_str()),
        }),
        LinkFrame::HomePushReply(reply) => json!({
            "type": "homePushReply",
            "requestId": reply.request_id,
            "homeId": field(reply.home_id.as_str()),
            "outcome": reply.outcome,
            "error": reply.error.as_deref().map(field),
        }),
        LinkFrame::PtyOpen(open) => json!({
            "type": "ptyOpen",
            "terminalId": field(&open.terminal_id),
            "homeId": field(open.home_id.as_str()),
            "mode": open.mode,
        }),
        LinkFrame::PtyInput(input) => json!({
            "type": "ptyInput",
            "terminalId": field(&input.terminal_id),
            "bytes": input.data.len(),
        }),
        LinkFrame::PtyResize(resize) => json!({
            "type": "ptyResize",
            "terminalId": field(&resize.terminal_id),
            "cols": resize.cols,
            "rows": resize.rows,
        }),
        LinkFrame::PtyKill(kill) => json!({
            "type": "ptyKill",
            "terminalId": field(&kill.terminal_id),
        }),
        LinkFrame::PtyAck(ack) => json!({
            "type": "ptyAck",
            "terminalId": field(&ack.terminal_id),
            "seq": ack.seq,
        }),
        LinkFrame::PtyOutput(output) => json!({
            "type": "ptyOutput",
            "terminalId": field(&output.terminal_id),
            "seq": output.seq,
            "bytes": output.data.len(),
        }),
        LinkFrame::PtyExited(exited) => json!({
            "type": "ptyExited",
            "terminalId": field(&exited.terminal_id),
            "exitCode": exited.exit_code,
            "error": exited.error.as_deref().map(field),
        }),
        LinkFrame::CheckoutRead(read) => json!({
            "type": "checkoutRead",
            "requestId": read.request_id,
            "agentId": agent(&read.agent_id),
            "offset": read.offset,
            "length": read.length,
        }),
        LinkFrame::CheckoutReadReply(reply) => json!({
            "type": "checkoutReadReply",
            "requestId": reply.request_id,
            "agentId": agent(&reply.agent_id),
            "outcome": reply.outcome,
            "offset": reply.piece.as_ref().map(|piece| piece.offset),
            "size": reply.piece.as_ref().map(|piece| piece.size),
            "error": reply.error.as_deref().map(field),
        }),
    }
}

/// `text` cut to at most `max` bytes on a character boundary, with `…`
/// marking a cut.
fn clip(text: &str, max: usize) -> String {
    if text.len() <= max {
        return text.to_owned();
    }
    let mut end = max - '…'.len_utf8();
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &text[..end])
}

fn journal_kind(body: &JournalBody) -> &'static str {
    match body {
        JournalBody::Started { .. } => "started",
        JournalBody::Notification { .. } => "notification",
        JournalBody::TurnEnded { .. } => "turnEnded",
        JournalBody::Exited { .. } => "exited",
        JournalBody::SetupStarted { .. } => "setupStarted",
        JournalBody::SetupFinished { .. } => "setupFinished",
        JournalBody::Skills { .. } => "skills",
        JournalBody::HomePushed { .. } => "homePushed",
    }
}

fn text<'a>(value: &'a Value, key: &str) -> &'a str {
    value.get(key).and_then(Value::as_str).unwrap_or("?")
}

/// A counter, which may serialize as a string (`NodeCounter`), unquoted.
fn number(value: &Value, key: &str) -> String {
    match value.get(key) {
        Some(Value::String(text)) => text.clone(),
        Some(other) => other.to_string(),
        None => "?".to_owned(),
    }
}

/// A short line naming the frame, from fields that are never secret.
fn summarize(frame_type: &str, frame: &Value) -> String {
    let agent = text(frame, "agentId");
    match frame_type {
        "hello" => format!(
            "hello generation {}",
            frame
                .get("binding")
                .map_or_else(|| "?".to_owned(), |binding| number(binding, "generation"))
        ),
        "welcome" => format!(
            "welcome protocol {}, {} running agents",
            number(frame, "protocol"),
            frame
                .get("runningAgents")
                .and_then(Value::as_array)
                .map(Vec::len)
                .or_else(|| {
                    frame
                        .get("runningAgentCount")
                        .and_then(Value::as_u64)
                        .and_then(|count| usize::try_from(count).ok())
                })
                .unwrap_or(0)
        ),
        "heartbeat" | "heartbeatReply" => format!("{frame_type} {}", number(frame, "nonce")),
        "launch" => {
            let mut line = format!("launch {agent} with {}", text(frame, "provider"));
            if let Some(model) = frame.get("model").and_then(Value::as_str) {
                let _ = write!(line, " ({model})");
            }
            line
        }
        "prompt" => format!("prompt {agent} turn {}", text(frame, "turnId")),
        "cancel" | "stop" => format!("{frame_type} {agent}"),
        "stopAck" => format!("stopAck {agent}"),
        "journal" => format!(
            "journal #{} {} {}",
            number(frame, "seq"),
            frame
                .get("homeId")
                .and_then(Value::as_str)
                .map_or_else(|| agent.to_owned(), |home| format!("home {home}")),
            frame
                .pointer("/body/kind")
                .and_then(Value::as_str)
                .unwrap_or("?")
        ),
        "ack" => format!("ack #{}", number(frame, "seq")),
        "replayComplete" => format!("replayComplete through #{}", number(frame, "throughSeq")),
        // Every forwarded call is `workspace.call`: name the workspace
        // method it carries.
        "request" => {
            let method = match frame.get("method").and_then(Value::as_str) {
                Some(crate::remote_launch::workspace_api::CALL_METHOD) | None => {
                    frame.pointer("/params/method").and_then(Value::as_str)
                }
                method => method,
            };
            format!("request {} for {agent}", method.unwrap_or("?"))
        }
        "response" => match frame.get("outcome") {
            Some(outcome) if outcome.get("err").is_some() => format!(
                "response error {}",
                outcome
                    .pointer("/err/code")
                    .map_or_else(|| "?".to_owned(), ToString::to_string)
            ),
            _ => "response ok".to_owned(),
        },
        "scriptStart" => format!(
            "scriptStart {} spawn {} for {agent} ({})",
            text(frame, "scriptId"),
            text(frame, "spawnId"),
            text(frame, "mode")
        ),
        "scriptStop" => format!(
            "scriptStop {} spawn {} for {agent}",
            text(frame, "scriptId"),
            text(frame, "spawnId")
        ),
        "scriptStarted" => format!(
            "scriptStarted {} spawn {} for {agent} pid {}",
            text(frame, "scriptId"),
            text(frame, "spawnId"),
            number(frame, "pid")
        ),
        "scriptOutput" => format!(
            "scriptOutput {} spawn {} for {agent} #{}",
            text(frame, "scriptId"),
            text(frame, "spawnId"),
            number(frame, "seq")
        ),
        "scriptExited" => {
            let mut line = format!(
                "scriptExited {} spawn {} for {agent}",
                text(frame, "scriptId"),
                text(frame, "spawnId")
            );
            match frame.get("exitCode").filter(|code| !code.is_null()) {
                Some(code) => {
                    let _ = write!(line, " code {code}");
                }
                None => line.push_str(" without a code"),
            }
            line
        }
        "homeOpen" | "homePush" => format!("{frame_type} home {}", text(frame, "homeId")),
        "homeOpenReply" | "homePushReply" => format!(
            "{frame_type} home {} {}",
            text(frame, "homeId"),
            text(frame, "outcome")
        ),
        other => other.to_owned(),
    }
}

/// Keys that stay visible although they match a secret pattern: ids, not
/// secrets.
const VISIBLE_KEYS: &[&str] = &["fingerprint", "credentialId", "stopId", "requestId"];

/// Whether `key` contains a match for
/// `token|secret|password|credential|authorization|cookie|private.?key|cert`
/// (case-insensitive, unanchored) and is not one of [`VISIBLE_KEYS`].
pub(crate) fn secret_key(key: &str) -> bool {
    if VISIBLE_KEYS.contains(&key) {
        return false;
    }
    let key = key.to_lowercase();
    let plain = [
        "token",
        "secret",
        "password",
        "credential",
        "authorization",
        "cookie",
        "cert",
    ];
    plain.iter().any(|needle| key.contains(needle))
        || key.match_indices("private").any(|(at, _)| {
            let rest = &key[at + "private".len()..];
            rest.starts_with("key")
                || rest
                    .char_indices()
                    .nth(1)
                    .is_some_and(|(next, _)| rest[next..].starts_with("key"))
        })
}

/// Mask every secret in `value` in place. A secret key's string, object or
/// array value is masked whole; a number, boolean or null stays (so usage
/// counts such as `inputTokens` stay readable).
pub(crate) fn redact(value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (key, child) in map.iter_mut() {
                if key == "profileEnv" {
                    redact_profile_env(child);
                } else if key == "env" {
                    redact_env(child);
                } else if secret_key(key)
                    && !matches!(child, Value::Null | Value::Number(_) | Value::Bool(_))
                {
                    *child = json!(MASK);
                } else {
                    redact(child);
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(redact),
        Value::String(text) if text.trim_start().starts_with("-----BEGIN") => {
            *value = json!(MASK);
        }
        _ => {}
    }
}

/// A `profileEnv` overlay `{ providerRef, fingerprint, env: [{ key, value }] }`:
/// its `env` values are masked, its `providerRef` and `fingerprint` stay
/// readable. Recognized only under the `profileEnv` key, never by shape, so
/// an ordinary env map with variables named `env` or `providerRef` is not
/// mistaken for one.
fn redact_profile_env(overlay: &mut Value) {
    match overlay {
        Value::Object(map) => {
            for (key, child) in map.iter_mut() {
                if key == "env" {
                    redact_env(child);
                } else {
                    redact(child);
                }
            }
        }
        other => redact_env(other),
    }
}

/// An environment under an `env` key: a script's `env: [{ key, value }]`
/// (`scriptStart`) or a saved script's `env: { KEY: value }` (forwarded
/// `script.*` params and results). Every non-null value is masked and the
/// keys stay visible.
fn redact_env(env: &mut Value) {
    match env {
        Value::Array(entries) => {
            for entry in entries.iter_mut() {
                // A `{ key, value }` pair masks its value; anything else (a
                // bare string, an object of other members) still gets the
                // generic redaction, so nothing is left as it was.
                if let Some(value) = entry.get_mut("value") {
                    if !value.is_null() {
                        *value = json!(MASK);
                    }
                }
                redact(entry);
            }
        }
        Value::Object(map) => {
            for value in map.values_mut() {
                if !value.is_null() {
                    *value = json!(MASK);
                }
            }
        }
        Value::String(_) => *env = json!(MASK),
        _ => {}
    }
}

/// One link's view of the daemon's capture: the peer every frame it sees
/// is recorded against.
#[derive(Clone)]
pub struct LinkTap {
    traffic: Arc<LinkTraffic>,
    peer: TrafficPeer,
}

impl std::fmt::Debug for LinkTap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LinkTap")
            .field("peer", &self.peer)
            .finish_non_exhaustive()
    }
}

impl LinkTap {
    #[must_use]
    pub fn new(traffic: Arc<LinkTraffic>, peer: TrafficPeer) -> Self {
        Self { traffic, peer }
    }

    /// Whether the daemon's capture is on.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.traffic.is_active()
    }

    /// Record `frame`, `bytes` long on the wire, when capture is on.
    pub fn observe(&self, direction: TrafficDirection, frame: &LinkFrame, bytes: usize) {
        self.traffic.observe(&self.peer, direction, frame, bytes);
    }
}

#[cfg(test)]
mod tests;
