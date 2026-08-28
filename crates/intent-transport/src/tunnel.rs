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
//! Flow control is per-connection, not per-stream, so one stream with a
//! stalled consumer can briefly head-of-line-block its siblings' inbound
//! frames. The wedge is bounded on every axis: client `CLOSE` is handled
//! out-of-band (never queued behind `DATA`), a blocked TCP write or idle
//! stream is torn down after [`TunnelLimits::idle_timeout`], and a stream
//! whose full queue parks the connection loop longer than
//! [`TunnelLimits::forward_timeout`] is killed so the mux resumes servicing
//! pings well inside the heartbeat window. Inbound messages are capped at
//! [`MAX_TUNNEL_MESSAGE_BYTES`] (1009 close on violation) and concurrent
//! streams are capped per connection.

use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::stream::SplitSink;
use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
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

/// Frame header length: opcode (1 byte) + streamId (4 bytes, big-endian).
pub(crate) const HEADER_LEN: usize = 5;

/// Maximum concurrent streams per `/tunnel` connection; further `OPEN`s are
/// answered with `OPEN_ERR` until a stream closes.
pub const MAX_STREAMS_PER_CONNECTION: usize = 32;

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
/// Longest the connection loop will park on one stream's full queue before
/// killing that stream. Kept well inside the 60s heartbeat window: while
/// parked the WebSocket read half is unpolled (pongs go unprocessed), so an
/// unbounded park would let one wedged stream get the whole connection
/// reaped as heartbeat-dead.
const FORWARD_TIMEOUT: Duration = Duration::from_secs(15);
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
    /// Daemon-side TCP connect deadline before `OPEN_ERR`.
    pub connect_timeout: Duration,
    /// Idle-stream (no data either way) teardown deadline; also bounds a
    /// single blocked TCP write.
    pub idle_timeout: Duration,
    /// Longest one stream's full queue may park the connection loop before
    /// that stream is killed to unwedge the mux.
    pub forward_timeout: Duration,
}

impl Default for TunnelLimits {
    fn default() -> Self {
        Self {
            max_streams: MAX_STREAMS_PER_CONNECTION,
            connect_timeout: CONNECT_TIMEOUT,
            idle_timeout: IDLE_STREAM_TIMEOUT,
            forward_timeout: FORWARD_TIMEOUT,
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
}

/// Why a byte buffer failed to decode as a [`Frame`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FrameError {
    /// Shorter than the 5-byte `[opcode][streamId]` header.
    TooShort,
    /// The opcode byte is not one of the six defined opcodes.
    UnknownOpcode(u8),
    /// `OPEN` payload must be exactly 2 bytes (port, big-endian).
    BadOpenPayload,
    /// `OPEN_OK` / `EOF` / `CLOSE` must carry no payload.
    UnexpectedPayload(u8),
    /// `OPEN_ERR` message must be valid UTF-8.
    BadErrMessage,
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
            | Self::Close { stream_id } => *stream_id,
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
        }
    }

    /// Decode one wire frame. Rejects short buffers, unknown opcodes, wrong
    /// `OPEN` payload sizes, payloads on payload-less opcodes, and non-UTF-8
    /// `OPEN_ERR` messages.
    ///
    /// # Errors
    ///
    /// Returns a [`FrameError`] for short buffers, unknown opcodes, wrong `OPEN` payload sizes, payloads on payload-less opcodes, or non-UTF-8 `OPEN_ERR` messages.
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
    Data(Vec<u8>),
    /// Client half-close: shut down the TCP write side.
    Eof,
}

/// Connection-loop handle to one live stream's relay task.
struct StreamHandle {
    msg_tx: mpsc::Sender<StreamMsg>,
    abort: tokio::task::AbortHandle,
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
    let (out_tx, mut out_rx) = mpsc::channel::<Frame>(OUTBOUND_QUEUE_FRAMES);
    let (done_tx, mut done_rx) = mpsc::unbounded_channel::<u32>();
    let mut streams: HashMap<u32, StreamHandle> = HashMap::new();
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
                        &mut out_rx,
                        &done_tx,
                        &mut done_rx,
                        limits,
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
                if sink.send(Message::Binary(frame.encode().into())).await.is_err() {
                    break;
                }
            }
            Some(id) = done_rx.recv() => {
                streams.remove(&id);
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
#[allow(clippy::too_many_arguments)]
async fn handle_frame<S>(
    frame: Frame,
    sink: &mut SplitSink<WebSocketStream<S>, Message>,
    streams: &mut HashMap<u32, StreamHandle>,
    out_tx: &mpsc::Sender<Frame>,
    out_rx: &mut mpsc::Receiver<Frame>,
    done_tx: &mpsc::UnboundedSender<u32>,
    done_rx: &mut mpsc::UnboundedReceiver<u32>,
    limits: TunnelLimits,
) -> bool
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    match frame {
        Frame::Open { stream_id, port } => {
            // Reap already-finished streams first so an id can be reused as
            // soon as the client has seen its final `CLOSE`.
            while let Ok(id) = done_rx.try_recv() {
                streams.remove(&id);
            }
            let reject = if streams.contains_key(&stream_id) {
                Some("duplicate stream id".to_string())
            } else if streams.len() >= limits.max_streams {
                Some(format!(
                    "too many concurrent streams (max {})",
                    limits.max_streams
                ))
            } else {
                None
            };
            if let Some(message) = reject {
                let frame = Frame::OpenErr { stream_id, message };
                return sink
                    .send(Message::Binary(frame.encode().into()))
                    .await
                    .is_ok();
            }
            let (msg_tx, msg_rx) = mpsc::channel::<StreamMsg>(STREAM_QUEUE_FRAMES);
            let task = tokio::spawn(run_stream(
                stream_id,
                port,
                msg_rx,
                out_tx.clone(),
                done_tx.clone(),
                limits,
            ));
            streams.insert(
                stream_id,
                StreamHandle {
                    msg_tx,
                    abort: task.abort_handle(),
                },
            );
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
            forward_to_stream(
                sink,
                streams,
                out_rx,
                stream_id,
                StreamMsg::Data(payload),
                limits,
            )
            .await
        }
        Frame::Eof { stream_id } => {
            forward_to_stream(sink, streams, out_rx, stream_id, StreamMsg::Eof, limits).await
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

/// Forward one message into a stream's bounded queue while continuing to
/// drain outbound frames to the socket, so a full stream queue can never
/// deadlock against a full outbound queue (the stream task may be blocked on
/// `out_tx.send` at the same time). Frames for unknown stream ids are dropped
/// — they are ordinary races with a daemon-side teardown already in flight.
/// A stream whose queue stays full past [`TunnelLimits::forward_timeout`] is
/// killed (abort + final `CLOSE`) so one wedged stream cannot park the whole
/// connection past the heartbeat window. Returns `false` only when the socket
/// is dead.
async fn forward_to_stream<S>(
    sink: &mut SplitSink<WebSocketStream<S>, Message>,
    streams: &mut HashMap<u32, StreamHandle>,
    out_rx: &mut mpsc::Receiver<Frame>,
    stream_id: u32,
    msg: StreamMsg,
    limits: TunnelLimits,
) -> bool
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let Some(handle) = streams.get(&stream_id) else {
        return true;
    };
    let msg_tx = handle.msg_tx.clone();
    let send = msg_tx.send(msg);
    tokio::pin!(send);
    let deadline = tokio::time::sleep(limits.forward_timeout);
    tokio::pin!(deadline);
    loop {
        tokio::select! {
            // A send error means the stream task already finished; its final
            // `CLOSE` and done-notification are on their way. Not fatal.
            _ = &mut send => return true,
            Some(frame) = out_rx.recv() => {
                if sink.send(Message::Binary(frame.encode().into())).await.is_err() {
                    return false;
                }
            }
            () = &mut deadline => {
                if let Some(handle) = streams.remove(&stream_id) {
                    handle.abort.abort();
                }
                let frame = Frame::Close { stream_id };
                return sink.send(Message::Binary(frame.encode().into())).await.is_ok();
            }
        }
    }
}

/// Relay one stream: connect to the daemon loopback, then copy bytes both
/// ways until EOF in both directions, an idle timeout, or a socket error.
/// An established stream always ends with a final `CLOSE` and a
/// done-notification so the connection loop can free the stream id; a stream
/// that never opened ends with the terminal `OPEN_ERR` instead (no `CLOSE`).
/// Client `CLOSE` does not arrive here — the connection loop aborts this task
/// directly and emits the final `CLOSE` itself.
async fn run_stream(
    stream_id: u32,
    port: u16,
    mut msg_rx: mpsc::Receiver<StreamMsg>,
    out_tx: mpsc::Sender<Frame>,
    done_tx: mpsc::UnboundedSender<u32>,
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
                .send(Frame::OpenErr {
                    stream_id,
                    message: format!("connect 127.0.0.1:{port}: {e}"),
                })
                .await;
            let _ = done_tx.send(stream_id);
            return;
        }
        Err(_) => {
            let _ = out_tx
                .send(Frame::OpenErr {
                    stream_id,
                    message: format!(
                        "connect 127.0.0.1:{port}: timed out after {:?}",
                        limits.connect_timeout
                    ),
                })
                .await;
            let _ = done_tx.send(stream_id);
            return;
        }
    };
    let _ = tcp.set_nodelay(true);
    if out_tx.send(Frame::OpenOk { stream_id }).await.is_err() {
        let _ = done_tx.send(stream_id);
        return;
    }
    let (mut rd, mut wr) = tcp.into_split();
    let mut buf = vec![0u8; READ_CHUNK_BYTES];
    let mut read_done = false;
    let mut write_done = false;
    let idle = tokio::time::sleep(limits.idle_timeout);
    tokio::pin!(idle);
    loop {
        tokio::select! {
            n = rd.read(&mut buf), if !read_done => match n {
                // Read errors (e.g. RST) surface as EOF toward the client;
                // the write side keeps draining until the client is done too.
                Ok(0) | Err(_) => {
                    read_done = true;
                    if out_tx.send(Frame::Eof { stream_id }).await.is_err() {
                        break;
                    }
                    if write_done {
                        break;
                    }
                }
                Ok(n) => {
                    idle.as_mut().reset(Instant::now() + limits.idle_timeout);
                    let payload = buf[..n].to_vec();
                    if out_tx.send(Frame::Data { stream_id, payload }).await.is_err() {
                        break;
                    }
                }
            },
            msg = msg_rx.recv() => match msg {
                Some(StreamMsg::Data(bytes)) => {
                    // Data after the client's own EOF is a client error; drop it.
                    if write_done {
                        continue;
                    }
                    idle.as_mut().reset(Instant::now() + limits.idle_timeout);
                    // Bound the write with the idle deadline: the idle timer
                    // in the select is not polled while parked here, so an
                    // unbounded `write_all` against a non-reading consumer
                    // would leak this task + socket indefinitely.
                    match tokio::time::timeout(limits.idle_timeout, wr.write_all(&bytes)).await {
                        Ok(Ok(())) => {}
                        Ok(Err(_)) | Err(_) => break,
                    }
                }
                Some(StreamMsg::Eof) => {
                    write_done = true;
                    let _ = wr.shutdown().await;
                    if read_done {
                        break;
                    }
                }
                None => break,
            },
            () = &mut idle => break,
        }
    }
    let _ = out_tx.send(Frame::Close { stream_id }).await;
    let _ = done_tx.send(stream_id);
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
