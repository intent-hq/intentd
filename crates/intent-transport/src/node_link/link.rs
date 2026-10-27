//! An established link: a writer task, a reader task that answers
//! heartbeats and routes RPC responses and cancellations, and a heartbeat
//! task that closes a silent link.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{SinkExt as _, StreamExt as _};
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

use intent_core::node_link::{
    LeaseBinding, LinkFrame, LinkId, LinkOutcome, LinkRequest, LinkRpcError,
};
use intent_core::{AgentId, Error, Result};

use super::codes;

/// Heartbeat cadence, the silence after which a link is declared dead, and
/// queue sizes.
#[derive(Debug, Clone, Copy)]
pub(super) struct Tuning {
    pub heartbeat: Duration,
    pub dead_after: Duration,
    pub outbound_queue: usize,
    pub event_queue: usize,
}

impl Default for Tuning {
    fn default() -> Self {
        Self {
            heartbeat: Duration::from_secs(15),
            dead_after: Duration::from_secs(45),
            outbound_queue: 256,
            event_queue: 1024,
        }
    }
}

/// Room for cancellations and dropped-request replies, which skip the
/// outbound queue. Each pending call or inbound request produces at most
/// one; overflowing it closes the link rather than losing one.
const URGENT_QUEUE: usize = 1024;
/// JSON-RPC internal error, used when a request handler is dropped.
const DROPPED_CODE: i32 = -32603;

struct Inner {
    binding: LeaseBinding,
    out: mpsc::Sender<Message>,
    urgent: mpsc::Sender<Message>,
    pending: Mutex<HashMap<LinkId, oneshot::Sender<LinkOutcome>>>,
    inbound: Mutex<HashMap<LinkId, watch::Sender<bool>>>,
    close: watch::Sender<bool>,
}

impl Inner {
    fn is_closed(&self) -> bool {
        *self.close.borrow()
    }

    fn shut(&self) {
        self.close.send_replace(true);
        lock(&self.pending).clear();
        for (_, cancel) in lock(&self.inbound).drain() {
            cancel.send_replace(true);
        }
    }

    /// Queue a cancellation or dropped-request reply ahead of ordinary
    /// frames. It is never silently lost: if even the urgent queue is full,
    /// the link closes, which cancels everything on both sides.
    fn send_urgent(&self, frame: &LinkFrame) {
        if self.is_closed() {
            return;
        }
        let Ok(bytes) = frame.encode() else {
            return;
        };
        if let Err(mpsc::error::TrySendError::Full(_)) =
            self.urgent.try_send(Message::Binary(bytes.into()))
        {
            tracing::warn!(
                code = codes::CLOSED,
                "node link: urgent queue full; closing"
            );
            self.shut();
        }
    }

    /// Best effort, for heartbeats: a lost one is replaced by the next.
    fn try_send_frame(&self, frame: &LinkFrame) {
        if self.is_closed() {
            return;
        }
        if let Ok(bytes) = frame.encode() {
            let _ = self.out.try_send(Message::Binary(bytes.into()));
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

fn closed_error() -> Error {
    Error::Internal(codes::CLOSED.into())
}

/// One established, authenticated link. Cloning shares the link.
#[derive(Clone)]
pub struct NodeLink {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for NodeLink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeLink")
            .field("binding", &self.inner.binding)
            .field("closed", &self.inner.is_closed())
            .finish()
    }
}

/// What the peer sent that the owner must handle.
#[derive(Debug)]
pub enum LinkEvent {
    /// A control or journal frame.
    Frame(LinkFrame),
    /// An RPC request to answer with [`IncomingRequest::respond`].
    Request(IncomingRequest),
}

/// The receiving half of a link. `recv` returns `None` once the link has
/// closed and every received event has been taken.
#[derive(Debug)]
pub struct LinkEvents {
    rx: mpsc::Receiver<LinkEvent>,
}

impl LinkEvents {
    pub async fn recv(&mut self) -> Option<LinkEvent> {
        self.rx.recv().await
    }
}

/// A request from the peer. Dropping it unanswered replies with an error,
/// so the caller never waits for its timeout.
pub struct IncomingRequest {
    request: LinkRequest,
    cancelled: watch::Receiver<bool>,
    inner: Arc<Inner>,
    answered: bool,
}

impl std::fmt::Debug for IncomingRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IncomingRequest")
            .field("request_id", &self.request.request_id)
            .field("method", &self.request.method)
            .finish_non_exhaustive()
    }
}

impl IncomingRequest {
    #[must_use]
    pub fn request(&self) -> &LinkRequest {
        &self.request
    }

    /// The binding of the link the request arrived on; head checks it with
    /// [`LeaseBinding::ensure_current`] before dispatching.
    #[must_use]
    pub fn binding(&self) -> &LeaseBinding {
        &self.inner.binding
    }

    /// Whether the caller cancelled the request or the link closed.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        *self.cancelled.borrow()
    }

    /// Resolves when the caller cancels the request or the link closes. Use
    /// it in a `select!` against the handler.
    pub async fn cancelled(&self) {
        let mut rx = self.cancelled.clone();
        super::wait_true(&mut rx).await;
    }

    /// Send the outcome.
    ///
    /// # Errors
    ///
    /// `node-link-closed` when the link is gone.
    pub async fn respond(mut self, outcome: LinkOutcome) -> Result<()> {
        self.answered = true;
        lock(&self.inner.inbound).remove(&self.request.request_id);
        let frame = LinkFrame::Response {
            request_id: self.request.request_id,
            outcome,
        };
        NodeLink {
            inner: self.inner.clone(),
        }
        .send_any(&frame)
        .await
    }
}

impl Drop for IncomingRequest {
    fn drop(&mut self) {
        if self.answered {
            return;
        }
        lock(&self.inner.inbound).remove(&self.request.request_id);
        self.inner.send_urgent(&LinkFrame::Response {
            request_id: self.request.request_id,
            outcome: LinkOutcome::Err(LinkRpcError {
                code: DROPPED_CODE,
                message: "node-link-request-dropped".into(),
                data: None,
            }),
        });
    }
}

/// Removes a call's pending slot and tells the peer to stop working on it
/// if the caller gives up (timeout, or the future is dropped).
struct CallGuard {
    inner: Arc<Inner>,
    id: LinkId,
    settled: bool,
}

impl Drop for CallGuard {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        if lock(&self.inner.pending).remove(&self.id).is_some() {
            self.inner.send_urgent(&LinkFrame::CancelRequest {
                request_id: self.id,
            });
        }
    }
}

impl NodeLink {
    /// Run the link over an upgraded socket whose handshake is complete.
    pub(super) fn start<S>(
        socket: WebSocketStream<S>,
        binding: LeaseBinding,
        tuning: Tuning,
    ) -> (Self, LinkEvents)
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (out, out_rx) = mpsc::channel(tuning.outbound_queue);
        let (urgent, urgent_rx) = mpsc::channel(URGENT_QUEUE);
        let (events_tx, events_rx) = mpsc::channel(tuning.event_queue);
        let (close, _) = watch::channel(false);
        let inner = Arc::new(Inner {
            binding,
            out,
            urgent,
            pending: Mutex::new(HashMap::new()),
            inbound: Mutex::new(HashMap::new()),
            close,
        });
        let (sink, stream) = socket.split();
        let last_seen = Arc::new(Mutex::new(Instant::now()));
        tokio::spawn(write_loop(sink, out_rx, urgent_rx, inner.clone()));
        tokio::spawn(read_loop(
            stream,
            events_tx,
            inner.clone(),
            last_seen.clone(),
        ));
        tokio::spawn(heartbeat_loop(inner.clone(), last_seen, tuning));
        (Self { inner }, LinkEvents { rx: events_rx })
    }

    #[must_use]
    pub fn binding(&self) -> &LeaseBinding {
        &self.inner.binding
    }

    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.inner.is_closed()
    }

    /// Close the link. Pending calls fail with `node-link-closed` and
    /// incoming requests observe cancellation.
    pub fn close(&self) {
        self.inner.shut();
    }

    /// Resolves once the link has closed.
    pub async fn closed(&self) {
        let mut rx = self.inner.close.subscribe();
        super::wait_true(&mut rx).await;
    }

    /// Send a control or journal frame. RPC frames go through
    /// [`Self::call`] and [`IncomingRequest::respond`]; handshake frames
    /// only through the handshake.
    ///
    /// # Errors
    ///
    /// `node-link-protocol` for a frame that must not be sent this way;
    /// `node-link-closed` when the link is gone.
    pub async fn send(&self, frame: &LinkFrame) -> Result<()> {
        if matches!(
            frame,
            LinkFrame::Hello(_)
                | LinkFrame::Welcome(_)
                | LinkFrame::Heartbeat { .. }
                | LinkFrame::HeartbeatReply { .. }
                | LinkFrame::Request(_)
                | LinkFrame::Response { .. }
                | LinkFrame::CancelRequest { .. }
        ) {
            return Err(super::protocol("frame is not sent directly"));
        }
        self.send_any(frame).await
    }

    async fn send_any(&self, frame: &LinkFrame) -> Result<()> {
        if self.is_closed() {
            return Err(closed_error());
        }
        let bytes = frame.encode()?;
        let mut close = self.inner.close.subscribe();
        // A stalled writer must not hold a sender past the link's closure.
        tokio::select! {
            sent = self.inner.out.send(Message::Binary(bytes.into())) => {
                sent.map_err(|_| closed_error())
            }
            () = super::wait_true(&mut close) => Err(closed_error()),
        }
    }

    /// Call `method` on the peer for `agent_id` and wait up to `timeout`
    /// for its outcome. Dropping the returned future or timing out sends a
    /// cancellation to the peer.
    ///
    /// # Errors
    ///
    /// `node-link-timeout` after `timeout`; `node-link-closed` when the link
    /// closes first.
    pub async fn call(
        &self,
        agent_id: AgentId,
        method: impl Into<String>,
        params: Value,
        timeout: Duration,
    ) -> Result<LinkOutcome> {
        let request_id = uuid::Uuid::new_v4();
        let (tx, rx) = oneshot::channel();
        lock(&self.inner.pending).insert(request_id, tx);
        let mut guard = CallGuard {
            inner: self.inner.clone(),
            id: request_id,
            settled: false,
        };
        if self.is_closed() {
            guard.settled = true;
            lock(&self.inner.pending).remove(&request_id);
            return Err(closed_error());
        }
        let request = LinkFrame::Request(LinkRequest {
            request_id,
            agent_id,
            method: method.into(),
            params,
            timeout_ms: u32::try_from(timeout.as_millis()).unwrap_or(u32::MAX),
        });
        // One deadline covers queueing the request and waiting for its
        // outcome, so a full outbound queue cannot outlast `timeout`.
        let exchange = async {
            self.send_any(&request).await?;
            rx.await.map_err(|_| closed_error())
        };
        let result = tokio::time::timeout(timeout, exchange).await;
        match result {
            Ok(Ok(outcome)) => {
                guard.settled = true;
                Ok(outcome)
            }
            Ok(Err(error)) => {
                guard.settled = true;
                lock(&self.inner.pending).remove(&request_id);
                Err(error)
            }
            Err(_) => Err(Error::Internal(codes::TIMEOUT.into())),
        }
    }
}

async fn write_loop<S>(
    mut sink: futures_util::stream::SplitSink<WebSocketStream<S>, Message>,
    mut out: mpsc::Receiver<Message>,
    mut urgent: mpsc::Receiver<Message>,
    inner: Arc<Inner>,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut close = inner.close.subscribe();
    loop {
        tokio::select! {
            biased;
            () = super::wait_true(&mut close) => break,
            message = urgent.recv() => {
                let Some(message) = message else { break };
                tokio::select! {
                    sent = sink.send(message) => {
                        if sent.is_err() {
                            break;
                        }
                    }
                    () = super::wait_true(&mut close) => break,
                }
            }
            message = out.recv() => {
                let Some(message) = message else { break };
                tokio::select! {
                    sent = sink.send(message) => {
                        if sent.is_err() {
                            break;
                        }
                    }
                    () = super::wait_true(&mut close) => break,
                }
            }
        }
    }
    inner.shut();
    // A peer that stopped reading must not hold this task open.
    let _ = tokio::time::timeout(Duration::from_secs(5), async {
        let _ = sink.send(Message::Close(None)).await;
        let _ = sink.close().await;
    })
    .await;
}

async fn read_loop<S>(
    mut stream: futures_util::stream::SplitStream<WebSocketStream<S>>,
    events: mpsc::Sender<LinkEvent>,
    inner: Arc<Inner>,
    last_seen: Arc<Mutex<Instant>>,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut close = inner.close.subscribe();
    loop {
        let message = tokio::select! {
            message = stream.next() => message,
            () = super::wait_true(&mut close) => break,
        };
        let bytes = match message {
            Some(Ok(Message::Binary(bytes))) => bytes,
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => {
                *lock(&last_seen) = Instant::now();
                continue;
            }
            _ => break,
        };
        *lock(&last_seen) = Instant::now();
        let Ok(frame) = LinkFrame::decode(&bytes) else {
            tracing::warn!(
                code = codes::PROTOCOL,
                "node link: undecodable frame; closing"
            );
            break;
        };
        // A consumer that stops draining events must not keep a closed link
        // (and its reader) alive.
        let keep = tokio::select! {
            keep = route(frame, &events, &inner) => keep,
            () = super::wait_true(&mut close) => false,
        };
        if !keep {
            break;
        }
    }
    inner.shut();
}

/// Handle one received frame; `false` closes the link.
async fn route(frame: LinkFrame, events: &mpsc::Sender<LinkEvent>, inner: &Arc<Inner>) -> bool {
    match frame {
        LinkFrame::Hello(_) | LinkFrame::Welcome(_) => {
            tracing::warn!(
                code = codes::PROTOCOL,
                "node link: handshake frame after handshake"
            );
            false
        }
        LinkFrame::Heartbeat { nonce } => {
            inner.try_send_frame(&LinkFrame::HeartbeatReply { nonce });
            true
        }
        LinkFrame::HeartbeatReply { .. } => true,
        LinkFrame::Response {
            request_id,
            outcome,
        } => {
            if let Some(tx) = lock(&inner.pending).remove(&request_id) {
                let _ = tx.send(outcome);
            }
            true
        }
        LinkFrame::CancelRequest { request_id } => {
            if let Some(cancel) = lock(&inner.inbound).remove(&request_id) {
                cancel.send_replace(true);
            }
            true
        }
        LinkFrame::Request(request) => {
            let (cancel, cancelled) = watch::channel(false);
            {
                let mut inbound = lock(&inner.inbound);
                if inbound.contains_key(&request.request_id) {
                    tracing::warn!(code = codes::PROTOCOL, "node link: duplicate request id");
                    return false;
                }
                inbound.insert(request.request_id, cancel);
            }
            let incoming = IncomingRequest {
                request,
                cancelled,
                inner: inner.clone(),
                answered: false,
            };
            let _ = events.send(LinkEvent::Request(incoming)).await;
            true
        }
        frame => {
            let _ = events.send(LinkEvent::Frame(frame)).await;
            true
        }
    }
}

async fn heartbeat_loop(inner: Arc<Inner>, last_seen: Arc<Mutex<Instant>>, tuning: Tuning) {
    let mut close = inner.close.subscribe();
    let mut tick = tokio::time::interval(tuning.heartbeat);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut nonce = 0_u64;
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            () = super::wait_true(&mut close) => return,
        }
        if lock(&last_seen).elapsed() > tuning.dead_after {
            tracing::warn!(code = codes::UNAVAILABLE, "node link: peer silent; closing");
            inner.shut();
            return;
        }
        nonce = nonce.wrapping_add(1);
        inner.try_send_frame(&LinkFrame::Heartbeat { nonce });
    }
}
