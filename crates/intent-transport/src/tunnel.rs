//! `/tunnel` — authenticated WebSocket loopback port-forwarding endpoint
//! (intent-hq/monorepo#2323, tunneling fallback).
//!
//! A remote client that cannot reach a daemon-host port directly (server bound
//! to `127.0.0.1`, firewall) opens ONE WebSocket to `/tunnel` (same bearer-token
//! auth as `/ws`, but **binary** frames) and multiplexes TCP streams over it.
//! Each frame is `[opcode u8][streamId u32 BE][payload]` — the WebSocket
//! provides message boundaries, so no length prefix is needed. Opcodes:
//!
//! | opcode | name       | payload            | direction |
//! |--------|------------|--------------------|-----------|
//! | 0x01   | `OPEN`     | port `u16` BE      | client → daemon |
//! | 0x02   | `OPEN_OK`  | (empty)            | daemon → client |
//! | 0x03   | `OPEN_ERR` | UTF-8 message      | daemon → client |
//! | 0x04   | `DATA`     | raw bytes          | both |
//! | 0x05   | `EOF`      | (empty)            | both |
//! | 0x06   | `CLOSE`    | (empty)            | both |
//! | 0x07   | `CREDIT`   | credit `u32` BE    | client → daemon |
//!
//! Per `OPEN` the daemon connects `TcpStream` to `127.0.0.1:<port>` — connect
//! targets are hard-limited to the daemon's IPv4 loopback (a service bound
//! only to `::1` is intentionally out of scope) — and answers `OPEN_OK` (then
//! relays `DATA` both ways) or `OPEN_ERR` with the connect error. `EOF`
//! half-closes one direction (client `EOF` ⇒ TCP write shutdown; TCP read EOF
//! ⇒ daemon sends `EOF`); `CLOSE` tears the stream down fully. The daemon
//! sends a final `CLOSE` when an *established* stream ends for any reason;
//! `OPEN_ERR` is itself terminal for a stream that never opened. A daemon-side
//! teardown can race a client `CLOSE`, so frames for unknown stream ids are
//! ignored (a duplicate `CLOSE` is harmless).
//!
//! Daemon→client `DATA` is flow-controlled per stream (intent-hq/intent#5482):
//! every stream starts with [`TUNNEL_INITIAL_CREDIT_BYTES`] of credit at
//! `OPEN_OK`, each `DATA` payload the daemon sends consumes its length, and
//! the client replenishes with `CREDIT` (exactly 4 payload bytes, a non-zero
//! `credit`, saturating at `u32::MAX`) once it has flushed bytes to its local
//! socket. A stream with no credit pauses only its own loopback read (so the
//! peer's `EOF` is also seen only once credit is available again) — its
//! inbound direction, its siblings, and the heartbeat keep flowing — so a
//! credit-aware client that keeps reading the WebSocket but stops flushing
//! one local socket can no longer park the connection loop in a WebSocket
//! write for everyone. That is the scope of the guarantee: the initial window
//! is fixed, not negotiated, so a peer that stops reading the WebSocket
//! altogether can still block `sink.send` before its window is spent (a
//! bounded connection write is a separate follow-up). Clients that never send
//! `CREDIT` keep working for the first window per stream and then stall only
//! that stream. A stream starved of credit while loopback payload is waiting
//! is closed after [`TunnelLimits::idle_timeout`]; a zero-credit stream with
//! nothing waiting — including one whose peer has only half-closed — is not.
//! `CREDIT` for an unknown stream is ignored like any other teardown race.
//!
//! Stream queues are bounded and admission never waits on a TCP consumer:
//! a full queue closes only that stream, leaving sibling frames and pings
//! readable. Client `CLOSE` is handled out-of-band (never queued behind
//! `DATA`), and a blocked TCP write or idle stream is torn down after
//! [`TunnelLimits::idle_timeout`]. Inbound messages are capped at
//! [`MAX_TUNNEL_MESSAGE_BYTES`] (1009 close on violation) and concurrent
//! streams are capped per connection.

use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicI64, AtomicU32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Notify, OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Message};
use tokio_tungstenite::tungstenite::Bytes;
use tokio_tungstenite::WebSocketStream;

use crate::ws::{mono_ms, ConnCmd};

/// `OPEN` — client asks the daemon to connect `127.0.0.1:<port>` (payload:
/// port `u16` big-endian).
pub const OP_OPEN: u8 = 0x01;
/// `OPEN_OK` — the daemon-side TCP connect succeeded (no payload).
pub(crate) const OP_OPEN_OK: u8 = 0x02;
/// `OPEN_ERR` — the connect failed / was refused (payload: UTF-8 message).
pub(crate) const OP_OPEN_ERR: u8 = 0x03;
/// `DATA` — raw stream bytes (payload: bytes, may be empty).
pub(crate) const OP_DATA: u8 = 0x04;
/// `EOF` — half-close: no more data in the sender's direction (no payload).
pub(crate) const OP_EOF: u8 = 0x05;
/// `CLOSE` — full stream teardown (no payload).
pub(crate) const OP_CLOSE: u8 = 0x06;
/// `CREDIT` — the client grants `credit` more bytes of daemon→client `DATA`
/// payload on a stream (payload: `u32` big-endian, must be non-zero).
pub const OP_CREDIT: u8 = 0x07;

/// Frame header length: opcode (1 byte) + streamId (4 bytes, big-endian).
pub(crate) const HEADER_LEN: usize = 5;
/// `CREDIT` payload length: the granted byte count as a `u32`.
const CREDIT_PAYLOAD_LEN: usize = 4;

/// Daemon→client `DATA` credit every stream holds at `OPEN_OK` without any
/// client action. Equals [`MAX_DATA_PAYLOAD_BYTES`], so a client that never
/// sends `CREDIT` still receives one maximal reply per stream unchanged.
pub const TUNNEL_INITIAL_CREDIT_BYTES: u32 = 1024 * 1024;

/// Maximum concurrent streams per `/tunnel` connection; further `OPEN`s are
/// answered with `OPEN_ERR` until a stream closes.
pub const MAX_STREAMS_PER_CONNECTION: usize = 256;
/// A single forward cannot consume another preview port's admission budget.
pub const MAX_STREAMS_PER_PORT: usize = 32;
/// Shared budget for inbound payloads, including data in blocked TCP writes.
const INBOUND_BYTES_PER_CONNECTION: usize = 64 * 1024 * 1024;

/// Largest `DATA` payload accepted from a client. The shared 40 MiB
/// transport limit is sized for JSON-RPC envelopes; tunnel frames get a much
/// smaller cap so the bounded frame-count queues cannot buffer multi-GiB of
/// payload against a stalled consumer (32 slots × payload is the worst case).
pub(crate) const MAX_DATA_PAYLOAD_BYTES: usize = 1024 * 1024;
/// `/tunnel` inbound WebSocket message cap: one frame header + a max payload.
pub const MAX_TUNNEL_MESSAGE_BYTES: usize = HEADER_LEN + MAX_DATA_PAYLOAD_BYTES;

/// Deadline for the daemon-side `TcpStream::connect` before `OPEN_ERR`.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
/// A stream with no data in either direction for this long is closed.
const IDLE_STREAM_TIMEOUT: Duration = Duration::from_secs(300);
/// Bound of the shared daemon→client frame queue (backpressure on TCP reads).
const OUTBOUND_QUEUE_FRAMES: usize = 64;
/// Bound of each stream's client→daemon message queue (backpressure on the
/// WebSocket read loop).
const STREAM_QUEUE_FRAMES: usize = 32;
/// TCP read chunk size — one `DATA` frame per read.
const READ_CHUNK_BYTES: usize = 16 * 1024;

/// Caps and timeouts for one `/tunnel` connection.
#[derive(Debug, Clone, Copy)]
pub struct TunnelLimits {
    /// Concurrent-stream cap per connection.
    pub max_streams: usize,
    /// Concurrent stream cap for a single remote port.
    pub max_streams_per_port: usize,
    /// Daemon-side TCP connect deadline before `OPEN_ERR`.
    pub connect_timeout: Duration,
    /// Idle-stream (no data either way) teardown deadline; also bounds a
    /// single blocked TCP write and a wait for daemon→client queue space.
    pub idle_timeout: Duration,
}

impl Default for TunnelLimits {
    fn default() -> Self {
        Self {
            max_streams: MAX_STREAMS_PER_CONNECTION,
            max_streams_per_port: MAX_STREAMS_PER_PORT,
            connect_timeout: CONNECT_TIMEOUT,
            idle_timeout: IDLE_STREAM_TIMEOUT,
        }
    }
}

/// One decoded mux frame (`[opcode u8][streamId u32 BE][payload]`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Frame {
    /// Connect `127.0.0.1:<port>` on the daemon and bind it to `stream_id`.
    Open { stream_id: u32, port: u16 },
    /// The `OPEN` connect succeeded; `DATA` may now flow both ways.
    OpenOk { stream_id: u32 },
    /// The `OPEN` failed; `message` names the connect error. Terminal.
    OpenErr { stream_id: u32, message: String },
    /// Raw stream bytes.
    Data { stream_id: u32, payload: Vec<u8> },
    /// Half-close: no more `DATA` from the sender on this stream.
    Eof { stream_id: u32 },
    /// Full teardown of the stream (both directions).
    Close { stream_id: u32 },
    /// The client grants `credit` more bytes of daemon→client `DATA` payload.
    Credit { stream_id: u32, credit: u32 },
}

/// Why a byte buffer failed to decode as a [`Frame`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameError {
    /// Shorter than the 5-byte `[opcode][streamId]` header.
    TooShort,
    /// The opcode byte is not one of the seven defined opcodes.
    UnknownOpcode(u8),
    /// `OPEN` payload must be exactly 2 bytes (port, big-endian).
    BadOpenPayload,
    /// `OPEN_OK` / `EOF` / `CLOSE` must carry no payload.
    UnexpectedPayload(u8),
    /// `OPEN_ERR` message must be valid UTF-8.
    BadErrMessage,
    /// `CREDIT` payload must be exactly 4 bytes (credit, big-endian).
    BadCreditPayload,
    /// `CREDIT` must grant at least one byte.
    ZeroCredit,
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TooShort => write!(f, "frame shorter than the {HEADER_LEN}-byte header"),
            Self::UnknownOpcode(op) => write!(f, "unknown opcode 0x{op:02x}"),
            Self::BadOpenPayload => write!(f, "OPEN payload must be exactly 2 bytes (port)"),
            Self::UnexpectedPayload(op) => {
                write!(f, "opcode 0x{op:02x} must not carry a payload")
            }
            Self::BadErrMessage => write!(f, "OPEN_ERR message is not valid UTF-8"),
            Self::BadCreditPayload => {
                write!(
                    f,
                    "CREDIT payload must be exactly {CREDIT_PAYLOAD_LEN} bytes (credit)"
                )
            }
            Self::ZeroCredit => write!(f, "CREDIT must grant at least one byte"),
        }
    }
}

impl std::error::Error for FrameError {}

impl Frame {
    /// The stream this frame belongs to.
    #[must_use]
    pub fn stream_id(&self) -> u32 {
        match self {
            Self::Open { stream_id, .. }
            | Self::OpenOk { stream_id }
            | Self::OpenErr { stream_id, .. }
            | Self::Data { stream_id, .. }
            | Self::Eof { stream_id }
            | Self::Close { stream_id }
            | Self::Credit { stream_id, .. } => *stream_id,
        }
    }

    /// Encode into the `[opcode u8][streamId u32 BE][payload]` wire form.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        fn build(opcode: u8, stream_id: u32, payload: &[u8]) -> Vec<u8> {
            let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
            out.push(opcode);
            out.extend_from_slice(&stream_id.to_be_bytes());
            out.extend_from_slice(payload);
            out
        }
        match self {
            Self::Open { stream_id, port } => build(OP_OPEN, *stream_id, &port.to_be_bytes()),
            Self::OpenOk { stream_id } => build(OP_OPEN_OK, *stream_id, &[]),
            Self::OpenErr { stream_id, message } => {
                build(OP_OPEN_ERR, *stream_id, message.as_bytes())
            }
            Self::Data { stream_id, payload } => build(OP_DATA, *stream_id, payload),
            Self::Eof { stream_id } => build(OP_EOF, *stream_id, &[]),
            Self::Close { stream_id } => build(OP_CLOSE, *stream_id, &[]),
            Self::Credit { stream_id, credit } => {
                build(OP_CREDIT, *stream_id, &credit.to_be_bytes())
            }
        }
    }

    /// Decode one wire frame. Rejects short buffers, unknown opcodes, wrong
    /// `OPEN` / `CREDIT` payload sizes, payloads on payload-less opcodes,
    /// non-UTF-8 `OPEN_ERR` messages, and a zero `CREDIT` grant.
    ///
    /// # Errors
    ///
    /// Returns a [`FrameError`] for short buffers, unknown opcodes, wrong `OPEN` / `CREDIT` payload sizes, payloads on payload-less opcodes, non-UTF-8 `OPEN_ERR` messages, or a zero `CREDIT` grant.
    ///
    /// # Panics
    ///
    /// Never panics in practice: the header slice converted into the stream-id array is always exactly 4 bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, FrameError> {
        if bytes.len() < HEADER_LEN {
            return Err(FrameError::TooShort);
        }
        let opcode = bytes[0];
        let stream_id = u32::from_be_bytes(bytes[1..HEADER_LEN].try_into().expect("4 bytes"));
        let payload = &bytes[HEADER_LEN..];
        match opcode {
            OP_OPEN if payload.len() != 2 => Err(FrameError::BadOpenPayload),
            OP_OPEN => Ok(Self::Open {
                stream_id,
                port: u16::from_be_bytes([payload[0], payload[1]]),
            }),
            OP_OPEN_OK | OP_EOF | OP_CLOSE if !payload.is_empty() => {
                Err(FrameError::UnexpectedPayload(opcode))
            }
            OP_OPEN_OK => Ok(Self::OpenOk { stream_id }),
            OP_OPEN_ERR => match std::str::from_utf8(payload) {
                Ok(message) => Ok(Self::OpenErr {
                    stream_id,
                    message: message.to_string(),
                }),
                Err(_) => Err(FrameError::BadErrMessage),
            },
            OP_DATA => Ok(Self::Data {
                stream_id,
                payload: payload.to_vec(),
            }),
            OP_EOF => Ok(Self::Eof { stream_id }),
            OP_CLOSE => Ok(Self::Close { stream_id }),
            OP_CREDIT if payload.len() != CREDIT_PAYLOAD_LEN => Err(FrameError::BadCreditPayload),
            OP_CREDIT => {
                let credit = u32::from_be_bytes(payload.try_into().expect("4 bytes"));
                if credit == 0 {
                    return Err(FrameError::ZeroCredit);
                }
                Ok(Self::Credit { stream_id, credit })
            }
            other => Err(FrameError::UnknownOpcode(other)),
        }
    }
}

/// A client→daemon message forwarded from the connection loop to one stream's
/// relay task through its bounded queue. Client `CLOSE` is deliberately NOT
/// queued here — it is handled out-of-band by aborting the relay task, so a
/// full queue can never delay a teardown.
enum StreamMsg {
    /// Bytes to write to the daemon-side TCP socket.
    Data(Vec<u8>, OwnedSemaphorePermit),
    /// Client half-close: shut down the TCP write side.
    Eof,
}

/// Outcome of the relay's loopback poll: a credit-bounded read, or — with an
/// empty window — a non-consuming peek at what is waiting behind it.
enum Loopback {
    Read(std::io::Result<usize>),
    Peeked(std::io::Result<usize>),
}

/// Daemon→client flow-control window of one stream, shared between the
/// connection loop (which grants on `CREDIT`) and the relay (which gates its
/// loopback read on it and consumes per `DATA` payload byte). Grants bypass
/// the stream's bounded message queue so a blocked loopback write or a full
/// queue can never delay a replenishment.
struct CreditWindow {
    bytes: AtomicU32,
    replenished: Notify,
}

impl CreditWindow {
    fn new(initial: u32) -> Arc<Self> {
        Arc::new(Self {
            bytes: AtomicU32::new(initial),
            replenished: Notify::new(),
        })
    }

    fn available(&self) -> u32 {
        self.bytes.load(Ordering::Acquire)
    }

    /// Add `credit`, saturating at `u32::MAX`, and wake a relay paused on an
    /// empty window (the permit is stored if it is not waiting right now).
    fn grant(&self, credit: u32) {
        let _ = self
            .bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                Some(current.saturating_add(credit))
            });
        self.replenished.notify_one();
    }

    /// Consume `n` bytes just read for a `DATA` payload; the relay never
    /// reads more than the window it observed, so this cannot underflow.
    fn consume(&self, n: usize) {
        let n = u32::try_from(n).expect("read bounded by the u32 window");
        self.bytes.fetch_sub(n, Ordering::AcqRel);
    }
}

/// Connection-loop handle to one live stream's relay task.
struct StreamHandle {
    port: u16,
    generation: Arc<()>,
    msg_tx: mpsc::Sender<StreamMsg>,
    /// Payload bytes admitted to `msg_tx` and not yet received by the relay;
    /// named in the full-queue close warning.
    queued_bytes: Arc<AtomicUsize>,
    /// Daemon→client credit the client has granted this stream.
    credit: Arc<CreditWindow>,
    abort: tokio::task::AbortHandle,
}

/// A daemon→client frame tagged with the local incarnation of its stream id.
/// The generation is not part of the wire format; it only prevents buffered
/// output from a retired task crossing a later reuse of the same id.
struct OutboundFrame {
    generation: Arc<()>,
    frame: Frame,
}

/// Drive one `/tunnel` WebSocket connection: decode inbound mux frames,
/// spawn/feed per-stream relay tasks, drain their outbound frames to the
/// socket, answer pings, and honour heartbeat/shutdown control commands.
/// All remaining stream tasks are aborted when the connection ends.
pub(crate) async fn run_tunnel_connection<S>(
    ws: WebSocketStream<S>,
    mut cmd_rx: mpsc::Receiver<ConnCmd>,
    last_pong: Arc<AtomicI64>,
    limits: TunnelLimits,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (mut sink, mut stream) = ws.split();
    let (out_tx, mut out_rx) = mpsc::channel::<OutboundFrame>(OUTBOUND_QUEUE_FRAMES);
    let mut streams: HashMap<u32, StreamHandle> = HashMap::new();
    let inbound_budget = Arc::new(Semaphore::new(INBOUND_BYTES_PER_CONNECTION));
    loop {
        tokio::select! {
            incoming = stream.next() => match incoming {
                Some(Err(e)) => {
                    // Over-limit inbound message/frame: tell the client why
                    // with a 1009 close, mirroring the `/ws` connection loop.
                    if matches!(e, tokio_tungstenite::tungstenite::Error::Capacity(_)) {
                        let _ = sink
                            .send(Message::Close(Some(CloseFrame {
                                code: CloseCode::Size,
                                reason: "message exceeds inbound size limit".into(),
                            })))
                            .await;
                    }
                    break;
                }
                Some(Ok(Message::Binary(bytes))) => {
                    let frame = match Frame::decode(&bytes) {
                        Ok(frame) => frame,
                        Err(e) => {
                            protocol_close(&mut sink, &format!("malformed tunnel frame: {e}"))
                                .await;
                            break;
                        }
                    };
                    if !handle_frame(
                        frame,
                        &mut sink,
                        &mut streams,
                        &out_tx,
                        limits,
                        &inbound_budget,
                    )
                    .await
                    {
                        break;
                    }
                }
                Some(Ok(Message::Ping(payload))) => {
                    if sink.send(Message::Pong(payload)).await.is_err() {
                        break;
                    }
                }
                Some(Ok(Message::Pong(_))) => last_pong.store(mono_ms(), Ordering::Relaxed),
                None | Some(Ok(Message::Close(_))) => break,
                Some(Ok(Message::Text(_))) => {
                    protocol_close(&mut sink, "text frames not allowed on /tunnel").await;
                    break;
                }
                Some(Ok(Message::Frame(_))) => {}
            },
            Some(frame) = out_rx.recv() => {
                if !send_outbound_frame(&mut sink, &mut streams, frame).await {
                    break;
                }
            }
            cmd = cmd_rx.recv() => match cmd {
                None => break,
                Some(ConnCmd::Ping) => {
                    if sink.send(Message::Ping(Bytes::new())).await.is_err() {
                        break;
                    }
                }
                Some(ConnCmd::Close) => {
                    let _ = sink
                        .send(Message::Close(Some(CloseFrame {
                            code: CloseCode::Away,
                            reason: "Server shutting down".into(),
                        })))
                        .await;
                    break;
                }
            }
        }
    }
    for (_, handle) in streams.drain() {
        handle.abort.abort();
    }
    let _ = sink.close().await;
}

/// Handle one decoded client frame on the connection loop. Returns `false`
/// when the connection must end (client protocol violation or a dead socket).
async fn handle_frame<S>(
    frame: Frame,
    sink: &mut SplitSink<WebSocketStream<S>, Message>,
    streams: &mut HashMap<u32, StreamHandle>,
    out_tx: &mpsc::Sender<OutboundFrame>,
    limits: TunnelLimits,
    inbound_budget: &Arc<Semaphore>,
) -> bool
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    match frame {
        Frame::Open { stream_id, port } => {
            let reject = if streams.contains_key(&stream_id) {
                Some("duplicate stream id".to_string())
            } else if streams.len() >= limits.max_streams {
                Some(format!(
                    "too many concurrent streams (max {})",
                    limits.max_streams
                ))
            } else if streams
                .values()
                .filter(|stream| stream.port == port)
                .count()
                >= limits.max_streams_per_port
            {
                Some(format!(
                    "too many concurrent streams for port {port} (max {})",
                    limits.max_streams_per_port
                ))
            } else {
                None
            };
            if let Some(message) = reject {
                tracing::info!(stream_id, port, active_streams = streams.len(), max_streams = limits.max_streams,
                    max_streams_per_port = limits.max_streams_per_port, reason = %message, "tunnel OPEN rejected");
                let frame = Frame::OpenErr { stream_id, message };
                return sink
                    .send(Message::Binary(frame.encode().into()))
                    .await
                    .is_ok();
            }
            // Pointer identity is unique while any queued frame retains the
            // old incarnation, so generations cannot wrap or alias.
            let generation = Arc::new(());
            let (msg_tx, msg_rx) = mpsc::channel::<StreamMsg>(STREAM_QUEUE_FRAMES);
            let queued_bytes = Arc::new(AtomicUsize::new(0));
            let credit = CreditWindow::new(TUNNEL_INITIAL_CREDIT_BYTES);
            let task = tokio::spawn(run_stream(
                stream_id,
                generation.clone(),
                port,
                msg_rx,
                queued_bytes.clone(),
                credit.clone(),
                out_tx.clone(),
                limits,
            ));
            streams.insert(
                stream_id,
                StreamHandle {
                    port,
                    generation,
                    msg_tx,
                    queued_bytes,
                    credit,
                    abort: task.abort_handle(),
                },
            );
            true
        }
        Frame::Credit { stream_id, credit } => {
            // Bypasses the stream queue on purpose: a replenishment must
            // reach a relay whose inbound queue is full or whose loopback
            // write is blocked, or the pause it lifts could never end.
            if let Some(handle) = streams.get(&stream_id) {
                handle.credit.grant(credit);
            }
            true
        }
        Frame::Data { stream_id, payload } => {
            if payload.len() > MAX_DATA_PAYLOAD_BYTES {
                protocol_close(
                    sink,
                    &format!("DATA payload exceeds {MAX_DATA_PAYLOAD_BYTES} bytes"),
                )
                .await;
                return false;
            }
            if !streams.contains_key(&stream_id) {
                return true;
            }
            let Ok(permit) = inbound_budget.clone().try_acquire_many_owned(
                u32::try_from(payload.len()).expect("payload bounded to one MiB"),
            ) else {
                if let Some(handle) = streams.remove(&stream_id) {
                    handle.abort.abort();
                }
                tracing::warn!(
                    stream_id,
                    "closing tunnel stream: shared inbound byte budget exhausted"
                );
                return sink
                    .send(Message::Binary(Frame::Close { stream_id }.encode().into()))
                    .await
                    .is_ok();
            };
            forward_to_stream(sink, streams, stream_id, StreamMsg::Data(payload, permit)).await
        }
        Frame::Eof { stream_id } => {
            forward_to_stream(sink, streams, stream_id, StreamMsg::Eof).await
        }
        Frame::Close { stream_id } => {
            // Out-of-band teardown: never queued behind `DATA` on a full
            // stream queue — this is the client's escape hatch for a stream
            // wedged on a stalled consumer. Abort the relay task (dropping
            // its TCP socket), confirm with the final `CLOSE`, free the id.
            if let Some(handle) = streams.remove(&stream_id) {
                handle.abort.abort();
                let frame = Frame::Close { stream_id };
                return sink
                    .send(Message::Binary(frame.encode().into()))
                    .await
                    .is_ok();
            }
            true
        }
        Frame::OpenOk { .. } | Frame::OpenErr { .. } => {
            protocol_close(sink, "unexpected daemon-only opcode from client").await;
            false
        }
    }
}

/// Send output only for the current incarnation of a stream id. Natural
/// terminal frames release the id after reaching the socket; direct client or
/// overload teardown removes the handle first, so any already-buffered output
/// from the retired task is discarded here.
async fn send_outbound_frame<S>(
    sink: &mut SplitSink<WebSocketStream<S>, Message>,
    streams: &mut HashMap<u32, StreamHandle>,
    outbound: OutboundFrame,
) -> bool
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let stream_id = outbound.frame.stream_id();
    let Some(handle) = streams.get(&stream_id) else {
        return true;
    };
    if !Arc::ptr_eq(&handle.generation, &outbound.generation) {
        return true;
    }
    let terminal = matches!(&outbound.frame, Frame::OpenErr { .. } | Frame::Close { .. });
    if sink
        .send(Message::Binary(outbound.frame.encode().into()))
        .await
        .is_err()
    {
        return false;
    }
    if terminal
        && streams
            .get(&stream_id)
            .is_some_and(|handle| Arc::ptr_eq(&handle.generation, &outbound.generation))
    {
        streams.remove(&stream_id);
    }
    true
}

/// Admit a message without parking the shared WebSocket reader. The
/// client→daemon direction has no credit window: once the bounded queue is
/// full, close that stream rather than blocking unrelated requests and
/// heartbeats.
/// The relay keeps draining this queue while its own output waits on a
/// lagging client, so a full queue means the loopback consumer has stopped
/// reading (or the client is flooding one stream), not that a large reply is
/// in flight. Unknown/finished streams are ordinary teardown races and are
/// ignored.
async fn forward_to_stream<S>(
    sink: &mut SplitSink<WebSocketStream<S>, Message>,
    streams: &mut HashMap<u32, StreamHandle>,
    stream_id: u32,
    msg: StreamMsg,
) -> bool
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let Some(handle) = streams.get(&stream_id) else {
        return true;
    };
    let bytes = match &msg {
        StreamMsg::Data(payload, _) => payload.len(),
        StreamMsg::Eof => 0,
    };
    // Counted before admission so the relay's decrement can never underflow.
    handle.queued_bytes.fetch_add(bytes, Ordering::Relaxed);
    match handle.msg_tx.try_send(msg) {
        Ok(()) => true,
        Err(mpsc::error::TrySendError::Closed(_)) => {
            handle.queued_bytes.fetch_sub(bytes, Ordering::Relaxed);
            true
        }
        Err(mpsc::error::TrySendError::Full(_)) => {
            let pending_bytes = handle.queued_bytes.fetch_sub(bytes, Ordering::Relaxed) - bytes;
            let pending_frames = handle.msg_tx.max_capacity() - handle.msg_tx.capacity();
            if let Some(handle) = streams.remove(&stream_id) {
                handle.abort.abort();
            }
            tracing::warn!(
                stream_id,
                pending_frames,
                pending_bytes,
                rejected_bytes = bytes,
                "closing tunnel stream with a full inbound queue"
            );
            sink.send(Message::Binary(Frame::Close { stream_id }.encode().into()))
                .await
                .is_ok()
        }
    }
}

/// Relay one stream: connect to the daemon loopback, then copy bytes both
/// ways until EOF in both directions, an idle timeout, or a socket error.
/// An established stream always ends with a final `CLOSE`; a stream that never
/// opened ends with the terminal `OPEN_ERR` instead (no `CLOSE`). The
/// connection loop releases the id only after sending that terminal frame.
/// Client `CLOSE` does not arrive here — the connection loop aborts this task
/// directly and emits the final `CLOSE` itself.
///
/// Neither direction's I/O is awaited outside the `select!`. Output waits for
/// a slot in the shared daemon→client queue as a branch, never by parking the
/// whole task: a reply larger than that queue (a 1 MiB JSON-RPC frame is 64
/// chunks) sent to a lagging client must not stop `msg_rx` draining, or the
/// client's ordinary requests behind it fill this stream's inbound queue and
/// close it (intent-hq/intent#5461). The loopback write is a branch for the
/// same reason in the other direction: a client frame whose write blocks
/// (the loopback peer is itself busy producing that reply and not reading)
/// must not stop the held output from being admitted when a slot frees, or
/// the two sides deadlock until the idle timeout.
///
/// The loopback read is additionally gated on the stream's daemon→client
/// credit window (intent-hq/intent#5482): it reads at most `min(16 KiB,
/// credit)` bytes and each `DATA` payload consumes its length, so the daemon
/// never queues output the client has not granted room for. An exhausted
/// window pauses only this read, as a `select!` condition; the pause ends
/// when the connection loop grants a client `CREDIT`. While the window is
/// empty AND the loopback has payload waiting (observed with a non-consuming
/// `peek`), a starvation deadline of `idle_timeout` runs independently of the
/// plain idle timer, so an upload that keeps the stream busy cannot keep a
/// starved reply parked forever; a grant clears it. A zero-credit stream with
/// nothing waiting (e.g. an inbound-only upload from a pre-credit client, or
/// one whose peer has half-closed after sending exactly the window — the EOF
/// is forwarded by the first credited read) is never closed by it. The
/// isolation this buys is scoped to a credit-aware
/// client that keeps reading the WebSocket: a peer that stops reading the
/// socket altogether can still park the connection's `sink.send` before its
/// window is spent — the fixed initial window is not negotiated, and a
/// bounded connection write is outside this change.
#[expect(clippy::too_many_arguments)]
async fn run_stream(
    stream_id: u32,
    generation: Arc<()>,
    port: u16,
    mut msg_rx: mpsc::Receiver<StreamMsg>,
    queued_bytes: Arc<AtomicUsize>,
    credit: Arc<CreditWindow>,
    out_tx: mpsc::Sender<OutboundFrame>,
    limits: TunnelLimits,
) {
    // Connect targets are hard-limited to the daemon loopback by construction.
    let connect = tokio::time::timeout(
        limits.connect_timeout,
        TcpStream::connect((Ipv4Addr::LOCALHOST, port)),
    )
    .await;
    let tcp = match connect {
        Ok(Ok(tcp)) => tcp,
        Ok(Err(e)) => {
            let _ = out_tx
                .send(OutboundFrame {
                    generation: generation.clone(),
                    frame: Frame::OpenErr {
                        stream_id,
                        message: format!("connect 127.0.0.1:{port}: {e}"),
                    },
                })
                .await;
            return;
        }
        Err(_) => {
            let _ = out_tx
                .send(OutboundFrame {
                    generation: generation.clone(),
                    frame: Frame::OpenErr {
                        stream_id,
                        message: format!(
                            "connect 127.0.0.1:{port}: timed out after {:?}",
                            limits.connect_timeout
                        ),
                    },
                })
                .await;
            return;
        }
    };
    let _ = tcp.set_nodelay(true);
    if out_tx
        .send(OutboundFrame {
            generation: generation.clone(),
            frame: Frame::OpenOk { stream_id },
        })
        .await
        .is_err()
    {
        return;
    }
    let (mut rd, mut wr) = tcp.into_split();
    let mut buf = vec![0u8; READ_CHUNK_BYTES];
    let mut read_done = false;
    let mut write_done = false;
    // The next daemon→client frame, held until the shared queue has a slot.
    // No TCP read happens while one is pending, so the loopback producer sees
    // the same backpressure as before; only `msg_rx` keeps flowing.
    let mut pending: Option<Frame> = None;
    // The client→daemon frame being written: bytes, write offset, and its
    // share of the shared inbound byte budget (released once fully written).
    // No further `msg_rx` message is taken while one is in flight, so the
    // per-stream queue keeps its bound and bytes stay ordered.
    let mut inbound: Option<(Vec<u8>, usize, OwnedSemaphorePermit)> = None;
    let idle = tokio::time::sleep(limits.idle_timeout);
    tokio::pin!(idle);
    // When loopback output (payload bytes) was first seen waiting behind an
    // empty credit window; `None` while credit is available or nothing is
    // waiting. That starvation lasting `idle_timeout` closes the stream even
    // if its inbound direction keeps resetting `idle`.
    let mut credit_exhausted_since: Option<Instant> = None;
    let credit_stall = tokio::time::sleep(limits.idle_timeout);
    tokio::pin!(credit_stall);
    let mut peek = [0u8; 1];
    // A peer EOF / read error seen while the window was empty. It is not
    // payload, so it never arms the starvation deadline; it is forwarded
    // only once a grant lets the read observe it, and is not probed again
    // until then.
    let mut peer_eof_deferred = false;
    loop {
        let credit_available = credit.available();
        if credit_available > 0 {
            credit_exhausted_since = None;
            peer_eof_deferred = false;
        }
        // Bounded by the window so `consume` observes the same figure.
        let read_limit = usize::try_from(credit_available)
            .unwrap_or(READ_CHUNK_BYTES)
            .min(READ_CHUNK_BYTES);
        // With credit, read the next chunk (never while a frame is held, so
        // the loopback producer keeps its backpressure). Without credit, only
        // peek — one non-consuming probe that arms the starvation deadline
        // when payload is waiting — until a grant re-evaluates the window.
        let poll_loopback = !read_done
            && if credit_available > 0 {
                pending.is_none()
            } else {
                credit_exhausted_since.is_none() && !peer_eof_deferred
            };
        // Fixed priority: admit the held frame first, then progress the
        // in-flight loopback write, then drain client→daemon messages, then
        // read more loopback output. A burst of reads may not starve `msg_rx`
        // — that is the coupling behind #5461.
        tokio::select! {
            biased;
            permit = out_tx.reserve(), if pending.is_some() => {
                let Ok(permit) = permit else { break };
                let frame = pending.take().expect("guarded by pending.is_some()");
                permit.send(OutboundFrame {
                    generation: generation.clone(),
                    frame,
                });
                if read_done && write_done {
                    break;
                }
            }
            // The expression is evaluated even when the branch is disabled,
            // so it must not unwrap `inbound`.
            written = wr.write(inbound.as_ref().map_or(&[][..], |(bytes, off, _)| &bytes[*off..])),
                if inbound.is_some() =>
            {
                match written {
                    Ok(0) | Err(_) => break,
                    Ok(n) => {
                        idle.as_mut().reset(Instant::now() + limits.idle_timeout);
                        let (bytes, off, _) = inbound.as_mut().expect("guarded by inbound.is_some()");
                        *off += n;
                        if *off >= bytes.len() {
                            inbound = None;
                        }
                    }
                }
            }
            msg = msg_rx.recv(), if inbound.is_none() => match msg {
                Some(StreamMsg::Data(bytes, permit)) => {
                    queued_bytes.fetch_sub(bytes.len(), Ordering::Relaxed);
                    // Data after the client's own EOF is a client error; drop it.
                    if write_done || bytes.is_empty() {
                        continue;
                    }
                    idle.as_mut().reset(Instant::now() + limits.idle_timeout);
                    inbound = Some((bytes, 0, permit));
                }
                Some(StreamMsg::Eof) => {
                    write_done = true;
                    let _ = wr.shutdown().await;
                    // With our own EOF still pending, the reserve arm breaks
                    // once it has been admitted.
                    if read_done && pending.is_none() {
                        break;
                    }
                }
                None => break,
            },
            event = async {
                if credit_available > 0 {
                    Loopback::Read(rd.read(&mut buf[..read_limit]).await)
                } else {
                    Loopback::Peeked(rd.peek(&mut peek).await)
                }
            }, if poll_loopback => match event {
                // Read errors (e.g. RST) surface as EOF toward the client;
                // the write side keeps draining until the client is done too.
                Loopback::Read(Ok(0) | Err(_)) => {
                    read_done = true;
                    pending = Some(Frame::Eof { stream_id });
                }
                Loopback::Read(Ok(n)) => {
                    idle.as_mut().reset(Instant::now() + limits.idle_timeout);
                    credit.consume(n);
                    pending = Some(Frame::Data {
                        stream_id,
                        payload: buf[..n].to_vec(),
                    });
                }
                // Payload waiting behind the empty window: start the
                // starvation deadline.
                Loopback::Peeked(Ok(1..)) => {
                    credit_exhausted_since = Some(Instant::now());
                    credit_stall.as_mut().reset(Instant::now() + limits.idle_timeout);
                }
                // Peer EOF / error with nothing waiting: not starvation. The
                // stream lives by the plain idle timer (its inbound direction
                // may still be active) and the EOF is forwarded, in order,
                // by the first credited read.
                Loopback::Peeked(Ok(_) | Err(_)) => peer_eof_deferred = true,
            },
            // Paused on an empty window: wake on the next client `CREDIT`
            // (a grant that lands before this arm is polled is kept as a
            // stored permit) and re-evaluate the window — also while a frame
            // is held, so the grant clears the starvation deadline.
            () = credit.replenished.notified(), if !read_done && credit_available == 0 => {}
            () = &mut credit_stall, if !read_done && credit_exhausted_since.is_some() => {
                // A grant that landed since the window was last observed
                // has already ended the starvation.
                if credit.available() > 0 {
                    credit_exhausted_since = None;
                    continue;
                }
                let credit_exhausted_for_ms = credit_exhausted_since
                    .map_or(0, |since| since.elapsed().as_millis());
                tracing::warn!(
                    stream_id,
                    credit_exhausted_for_ms,
                    "closing tunnel stream: daemon→client credit window exhausted for the idle timeout without a client CREDIT"
                );
                break;
            }
            () = &mut idle => break,
        }
    }
    let _ = out_tx
        .send(OutboundFrame {
            generation,
            frame: Frame::Close { stream_id },
        })
        .await;
}

/// Send a `1002 Protocol Error` close frame with `reason` (best effort).
async fn protocol_close<S>(sink: &mut SplitSink<WebSocketStream<S>, Message>, reason: &str)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let _ = sink
        .send(Message::Close(Some(CloseFrame {
            code: CloseCode::Protocol,
            reason: reason.to_string().into(),
        })))
        .await;
}

#[cfg(test)]
mod tests;
