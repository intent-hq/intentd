//! Unit tests for the `/tunnel` frame codec and bounded queue admission.
//! TCP relay/lifecycle behavior is covered by the
//! `wss_tunnel` integration suite in the `intentd` crate.

use super::*;
use futures_util::FutureExt;
use tokio::net::TcpListener;

fn data(bytes: Vec<u8>) -> StreamMsg {
    let permit = Arc::new(Semaphore::new(bytes.len()))
        .try_acquire_many_owned(u32::try_from(bytes.len()).unwrap())
        .unwrap();
    StreamMsg::Data(bytes, permit)
}

/// Queue saturation is isolated even when the next frame is a half-close.
/// Use an unpolled receiver to make saturation deterministic, without relying
/// on kernel TCP buffer sizes or timing a slow consumer.
#[tokio::test]
async fn full_stream_queue_closes_only_that_stream() {
    for message in [data(vec![2]), StreamMsg::Eof] {
        let (server_io, client_io) = tokio::io::duplex(1024);
        let server = WebSocketStream::from_raw_socket(
            server_io,
            tokio_tungstenite::tungstenite::protocol::Role::Server,
            None,
        )
        .await;
        let mut client = WebSocketStream::from_raw_socket(
            client_io,
            tokio_tungstenite::tungstenite::protocol::Role::Client,
            None,
        )
        .await;
        let (mut sink, _) = server.split();
        let (blocked_tx, _blocked_rx) = mpsc::channel(1);
        assert!(blocked_tx.try_send(data(vec![1])).is_ok());
        let (healthy_tx, mut healthy_rx) = mpsc::channel(2);
        let blocked = tokio::spawn(std::future::pending::<()>());
        let healthy = tokio::spawn(std::future::pending::<()>());
        let blocked_generation = Arc::new(());
        let healthy_generation = Arc::new(());
        let (out_tx, mut out_rx) = mpsc::channel(8);
        let mut streams = HashMap::from([
            (
                1,
                StreamHandle {
                    port: 0,
                    generation: blocked_generation.clone(),
                    msg_tx: blocked_tx,
                    queued_bytes: Arc::new(AtomicUsize::new(1)),
                    credit: CreditWindow::new(TUNNEL_INITIAL_CREDIT_BYTES),
                    abort: blocked.abort_handle(),
                },
            ),
            (
                2,
                StreamHandle {
                    port: 0,
                    generation: healthy_generation,
                    msg_tx: healthy_tx,
                    queued_bytes: Arc::new(AtomicUsize::new(0)),
                    credit: CreditWindow::new(TUNNEL_INITIAL_CREDIT_BYTES),
                    abort: healthy.abort_handle(),
                },
            ),
        ]);
        for frame in stale_frames(1) {
            out_tx
                .send(OutboundFrame {
                    generation: blocked_generation.clone(),
                    frame,
                })
                .await
                .unwrap();
        }
        assert!(tokio::time::timeout(
            Duration::from_secs(1),
            forward_to_stream(&mut sink, &mut streams, 1, message),
        )
        .await
        .expect("queue admission must not wait for the consumer"));
        assert!(!streams.contains_key(&1));
        assert!(streams.contains_key(&2));
        assert!(blocked
            .await
            .expect_err("blocked relay aborted")
            .is_cancelled());
        let Message::Binary(bytes) = tokio::time::timeout(Duration::from_secs(1), client.next())
            .await
            .expect("stream CLOSE must arrive promptly")
            .expect("stream CLOSE message")
            .expect("read stream CLOSE")
        else {
            panic!("expected stream CLOSE");
        };
        assert_eq!(
            Frame::decode(&bytes).unwrap(),
            Frame::Close { stream_id: 1 }
        );
        assert!(
            handle_frame(
                Frame::Open {
                    stream_id: 1,
                    port: 0,
                },
                &mut sink,
                &mut streams,
                &out_tx,
                TunnelLimits::default(),
                &Arc::new(Semaphore::new(INBOUND_BYTES_PER_CONNECTION)),
            )
            .await
        );
        let replacement_generation = streams
            .get(&1)
            .expect("replacement allocated")
            .generation
            .clone();
        assert!(!Arc::ptr_eq(&blocked_generation, &replacement_generation));
        for _ in 0..stale_frames(1).len() {
            assert!(
                send_outbound_frame(
                    &mut sink,
                    &mut streams,
                    out_rx.recv().await.expect("queued stale output"),
                )
                .await
            );
            assert!(client.next().now_or_never().is_none());
            assert!(streams
                .get(&1)
                .is_some_and(|handle| Arc::ptr_eq(&handle.generation, &replacement_generation)));
        }
        assert!(forward_to_stream(&mut sink, &mut streams, 2, data(vec![3])).await);
        assert!(forward_to_stream(&mut sink, &mut streams, 2, StreamMsg::Eof).await);
        assert!(
            matches!(healthy_rx.recv().await, Some(StreamMsg::Data(bytes, _permit)) if bytes == vec![3])
        );
        assert!(matches!(healthy_rx.recv().await, Some(StreamMsg::Eof)));
        streams
            .remove(&1)
            .expect("replacement handle")
            .abort
            .abort();
        healthy.abort();
        assert!(healthy.await.expect_err("cleanup").is_cancelled());
    }
}

/// Client `CLOSE` retires an incarnation before allocating a replacement, so
/// every kind of queued output from the old relay is ignored after id reuse.
#[tokio::test]
async fn client_close_drops_stale_output_after_stream_id_reuse() {
    let (server_io, client_io) = tokio::io::duplex(1024);
    let server = WebSocketStream::from_raw_socket(
        server_io,
        tokio_tungstenite::tungstenite::protocol::Role::Server,
        None,
    )
    .await;
    let mut client = WebSocketStream::from_raw_socket(
        client_io,
        tokio_tungstenite::tungstenite::protocol::Role::Client,
        None,
    )
    .await;
    let (mut sink, _) = server.split();
    let (out_tx, mut out_rx) = mpsc::channel(8);
    let (msg_tx, _msg_rx) = mpsc::channel(1);
    let old_relay = tokio::spawn(std::future::pending::<()>());
    let stale_generation = Arc::new(());
    let mut streams = HashMap::from([(
        7,
        StreamHandle {
            port: 0,
            generation: stale_generation.clone(),
            msg_tx,
            queued_bytes: Arc::new(AtomicUsize::new(0)),
            credit: CreditWindow::new(TUNNEL_INITIAL_CREDIT_BYTES),
            abort: old_relay.abort_handle(),
        },
    )]);
    for frame in stale_frames(7) {
        out_tx
            .send(OutboundFrame {
                generation: stale_generation.clone(),
                frame,
            })
            .await
            .unwrap();
    }

    assert!(
        handle_frame(
            Frame::Close { stream_id: 7 },
            &mut sink,
            &mut streams,
            &out_tx,
            TunnelLimits::default(),
            &Arc::new(Semaphore::new(INBOUND_BYTES_PER_CONNECTION)),
        )
        .await
    );
    let Message::Binary(bytes) = client.next().await.unwrap().unwrap() else {
        panic!("expected client CLOSE confirmation");
    };
    assert_eq!(
        Frame::decode(&bytes).unwrap(),
        Frame::Close { stream_id: 7 }
    );
    assert!(old_relay
        .await
        .expect_err("old relay aborted")
        .is_cancelled());

    assert!(
        handle_frame(
            Frame::Open {
                stream_id: 7,
                port: 0,
            },
            &mut sink,
            &mut streams,
            &out_tx,
            TunnelLimits::default(),
            &Arc::new(Semaphore::new(INBOUND_BYTES_PER_CONNECTION)),
        )
        .await
    );
    let current_generation = streams
        .get(&7)
        .expect("replacement allocated")
        .generation
        .clone();
    streams.get(&7).unwrap().abort.abort();
    assert!(!Arc::ptr_eq(&stale_generation, &current_generation));

    for _ in 0..stale_frames(7).len() {
        assert!(
            send_outbound_frame(
                &mut sink,
                &mut streams,
                out_rx.recv().await.expect("queued stale output"),
            )
            .await
        );
        assert!(client.next().now_or_never().is_none());
        assert!(streams
            .get(&7)
            .is_some_and(|handle| Arc::ptr_eq(&handle.generation, &current_generation)));
    }

    streams.remove(&7);
}

/// Non-terminal output preserves ownership; a matching natural `CLOSE` or
/// `OPEN_ERR` releases only its own incarnation after the frame is sent.
#[tokio::test]
async fn natural_terminal_frames_release_only_the_matching_generation() {
    let (server_io, client_io) = tokio::io::duplex(1024);
    let server = WebSocketStream::from_raw_socket(
        server_io,
        tokio_tungstenite::tungstenite::protocol::Role::Server,
        None,
    )
    .await;
    let mut client = WebSocketStream::from_raw_socket(
        client_io,
        tokio_tungstenite::tungstenite::protocol::Role::Client,
        None,
    )
    .await;
    let (mut sink, _) = server.split();
    let current_generation = Arc::new(());
    let (msg_tx, _msg_rx) = mpsc::channel(1);
    let relay = tokio::spawn(std::future::pending::<()>());
    let mut streams = HashMap::from([(
        7,
        StreamHandle {
            port: 0,
            generation: current_generation.clone(),
            msg_tx,
            queued_bytes: Arc::new(AtomicUsize::new(0)),
            credit: CreditWindow::new(TUNNEL_INITIAL_CREDIT_BYTES),
            abort: relay.abort_handle(),
        },
    )]);

    for frame in [
        Frame::OpenOk { stream_id: 7 },
        Frame::Data {
            stream_id: 7,
            payload: b"current".to_vec(),
        },
        Frame::Eof { stream_id: 7 },
    ] {
        assert!(
            send_outbound_frame(
                &mut sink,
                &mut streams,
                OutboundFrame {
                    generation: current_generation.clone(),
                    frame: frame.clone(),
                },
            )
            .await
        );
        let Message::Binary(bytes) = client.next().await.unwrap().unwrap() else {
            panic!("expected current output");
        };
        assert_eq!(Frame::decode(&bytes).unwrap(), frame);
        assert!(streams.contains_key(&7));
    }

    assert!(
        send_outbound_frame(
            &mut sink,
            &mut streams,
            OutboundFrame {
                generation: current_generation.clone(),
                frame: Frame::Close { stream_id: 7 },
            },
        )
        .await
    );
    let Message::Binary(bytes) = client.next().await.unwrap().unwrap() else {
        panic!("expected current CLOSE");
    };
    assert_eq!(
        Frame::decode(&bytes).unwrap(),
        Frame::Close { stream_id: 7 }
    );
    assert!(!streams.contains_key(&7));
    relay.abort();

    let failed_generation = Arc::new(());
    let (msg_tx, _msg_rx) = mpsc::channel(1);
    let failed_relay = tokio::spawn(std::future::pending::<()>());
    streams.insert(
        7,
        StreamHandle {
            port: 0,
            generation: failed_generation.clone(),
            msg_tx,
            queued_bytes: Arc::new(AtomicUsize::new(0)),
            credit: CreditWindow::new(TUNNEL_INITIAL_CREDIT_BYTES),
            abort: failed_relay.abort_handle(),
        },
    );
    assert!(
        send_outbound_frame(
            &mut sink,
            &mut streams,
            OutboundFrame {
                generation: failed_generation,
                frame: Frame::OpenErr {
                    stream_id: 7,
                    message: "connect failed".to_string(),
                },
            },
        )
        .await
    );
    let Message::Binary(bytes) = client.next().await.unwrap().unwrap() else {
        panic!("expected current OPEN_ERR");
    };
    assert_eq!(
        Frame::decode(&bytes).unwrap(),
        Frame::OpenErr {
            stream_id: 7,
            message: "connect failed".to_string(),
        }
    );
    assert!(!streams.contains_key(&7));
    failed_relay.abort();
}

fn stale_frames(stream_id: u32) -> [Frame; 5] {
    [
        Frame::OpenOk { stream_id },
        Frame::OpenErr {
            stream_id,
            message: "stale connect failure".to_string(),
        },
        Frame::Data {
            stream_id,
            payload: b"stale".to_vec(),
        },
        Frame::Eof { stream_id },
        Frame::Close { stream_id },
    ]
}

/// Every frame variant survives an encode → decode round-trip unchanged.
#[test]
fn round_trip_all_variants() {
    let frames = vec![
        Frame::Open {
            stream_id: 0,
            port: 1,
        },
        Frame::Open {
            stream_id: u32::MAX,
            port: u16::MAX,
        },
        Frame::OpenOk { stream_id: 7 },
        Frame::OpenErr {
            stream_id: 8,
            message: "connect 127.0.0.1:80: refused".to_string(),
        },
        Frame::OpenErr {
            stream_id: 9,
            message: String::new(),
        },
        Frame::Data {
            stream_id: 10,
            payload: b"hello tunnel".to_vec(),
        },
        Frame::Data {
            stream_id: 11,
            payload: Vec::new(),
        },
        Frame::Eof { stream_id: 12 },
        Frame::Close { stream_id: 13 },
        Frame::Credit {
            stream_id: 14,
            credit: 1,
        },
        Frame::Credit {
            stream_id: 15,
            credit: u32::MAX,
        },
    ];
    for frame in frames {
        let bytes = frame.encode();
        let decoded = Frame::decode(&bytes).expect("decode");
        assert_eq!(decoded, frame);
    }
}

/// The wire layout is exactly `[opcode u8][streamId u32 BE][payload]`.
#[test]
fn wire_layout_is_opcode_stream_id_payload() {
    let bytes = Frame::Open {
        stream_id: 0x0102_0304,
        port: 0x1F90, // 8080
    }
    .encode();
    assert_eq!(bytes, vec![OP_OPEN, 0x01, 0x02, 0x03, 0x04, 0x1F, 0x90]);

    let bytes = Frame::Data {
        stream_id: 1,
        payload: b"ab".to_vec(),
    }
    .encode();
    assert_eq!(bytes, vec![OP_DATA, 0, 0, 0, 1, b'a', b'b']);

    let bytes = Frame::Credit {
        stream_id: 2,
        credit: 0x0001_0000, // 64 KiB
    }
    .encode();
    assert_eq!(bytes, vec![OP_CREDIT, 0, 0, 0, 2, 0x00, 0x01, 0x00, 0x00]);
}

/// Buffers shorter than the 5-byte header are rejected, including empty.
#[test]
fn rejects_short_buffers() {
    for len in 0..HEADER_LEN {
        let bytes = vec![OP_DATA; len];
        assert_eq!(
            Frame::decode(&bytes),
            Err(FrameError::TooShort),
            "len {len}"
        );
    }
}

/// Opcode bytes outside the defined set are rejected.
#[test]
fn rejects_unknown_opcodes() {
    for op in [0x00u8, 0x08, 0x7F, 0xFF] {
        let mut bytes = vec![op];
        bytes.extend_from_slice(&1u32.to_be_bytes());
        assert_eq!(Frame::decode(&bytes), Err(FrameError::UnknownOpcode(op)));
    }
}

/// `OPEN` must carry exactly a 2-byte port payload.
#[test]
fn rejects_bad_open_payload_sizes() {
    for payload_len in [0usize, 1, 3, 8] {
        let mut bytes = vec![OP_OPEN];
        bytes.extend_from_slice(&1u32.to_be_bytes());
        bytes.extend(std::iter::repeat_n(0u8, payload_len));
        assert_eq!(
            Frame::decode(&bytes),
            Err(FrameError::BadOpenPayload),
            "payload len {payload_len}"
        );
    }
}

/// `OPEN_OK` / `EOF` / `CLOSE` must not carry a payload.
#[test]
fn rejects_payload_on_payloadless_opcodes() {
    for op in [OP_OPEN_OK, OP_EOF, OP_CLOSE] {
        let mut bytes = vec![op];
        bytes.extend_from_slice(&1u32.to_be_bytes());
        bytes.push(0xAA);
        assert_eq!(
            Frame::decode(&bytes),
            Err(FrameError::UnexpectedPayload(op))
        );
    }
}

/// `OPEN_ERR` payloads must be valid UTF-8.
#[test]
fn rejects_non_utf8_open_err_message() {
    let mut bytes = vec![OP_OPEN_ERR];
    bytes.extend_from_slice(&1u32.to_be_bytes());
    bytes.extend_from_slice(&[0xFF, 0xFE]);
    assert_eq!(Frame::decode(&bytes), Err(FrameError::BadErrMessage));
}

/// `CREDIT` must carry exactly a 4-byte credit payload.
#[test]
fn rejects_bad_credit_payload_sizes() {
    for payload_len in [0usize, 1, 3, 5, 8] {
        let mut bytes = vec![OP_CREDIT];
        bytes.extend_from_slice(&1u32.to_be_bytes());
        bytes.extend(std::iter::repeat_n(1u8, payload_len));
        assert_eq!(
            Frame::decode(&bytes),
            Err(FrameError::BadCreditPayload),
            "payload len {payload_len}"
        );
    }
}

/// A `CREDIT` grant of zero bytes is a protocol violation, not a no-op.
#[test]
fn rejects_zero_credit() {
    let mut bytes = vec![OP_CREDIT];
    bytes.extend_from_slice(&1u32.to_be_bytes());
    bytes.extend_from_slice(&0u32.to_be_bytes());
    assert_eq!(Frame::decode(&bytes), Err(FrameError::ZeroCredit));
}

/// The window adds grants, saturates at `u32::MAX` instead of wrapping, and
/// consumption reduces it by exactly the bytes read.
#[test]
fn credit_window_grants_saturate_and_consume() {
    let window = CreditWindow::new(TUNNEL_INITIAL_CREDIT_BYTES);
    assert_eq!(window.available(), TUNNEL_INITIAL_CREDIT_BYTES);
    window.consume(READ_CHUNK_BYTES);
    assert_eq!(
        window.available(),
        TUNNEL_INITIAL_CREDIT_BYTES - u32::try_from(READ_CHUNK_BYTES).unwrap()
    );
    window.grant(u32::MAX);
    assert_eq!(window.available(), u32::MAX);
    window.grant(1);
    assert_eq!(window.available(), u32::MAX);
}

/// `stream_id()` returns the id for every variant.
#[test]
fn stream_id_accessor_covers_all_variants() {
    let cases: Vec<(Frame, u32)> = vec![
        (
            Frame::Open {
                stream_id: 1,
                port: 80,
            },
            1,
        ),
        (Frame::OpenOk { stream_id: 2 }, 2),
        (
            Frame::OpenErr {
                stream_id: 3,
                message: "x".into(),
            },
            3,
        ),
        (
            Frame::Data {
                stream_id: 4,
                payload: vec![1],
            },
            4,
        ),
        (Frame::Eof { stream_id: 5 }, 5),
        (Frame::Close { stream_id: 6 }, 6),
        (
            Frame::Credit {
                stream_id: 7,
                credit: 1,
            },
            7,
        ),
    ];
    for (frame, id) in cases {
        assert_eq!(frame.stream_id(), id);
    }
}

/// The shared payload budget follows queued data through consumption and is
/// returned on drop; overload must not abort a different stream's relay.
#[tokio::test]
async fn inbound_byte_budget_is_shared_and_released() {
    let (server_io, client_io) = tokio::io::duplex(1024);
    let server = WebSocketStream::from_raw_socket(
        server_io,
        tokio_tungstenite::tungstenite::protocol::Role::Server,
        None,
    )
    .await;
    let mut client = WebSocketStream::from_raw_socket(
        client_io,
        tokio_tungstenite::tungstenite::protocol::Role::Client,
        None,
    )
    .await;
    let (mut sink, _) = server.split();
    let (out_tx, _out_rx) = mpsc::channel(8);
    let budget = Arc::new(Semaphore::new(1));
    let mut streams = HashMap::new();
    let mut receivers = Vec::new();
    for id in [1, 2] {
        let (msg_tx, msg_rx) = mpsc::channel(4);
        receivers.push(msg_rx);
        let task = tokio::spawn(std::future::pending::<()>());
        streams.insert(
            id,
            StreamHandle {
                port: u16::try_from(id).unwrap(),
                generation: Arc::new(()),
                msg_tx,
                queued_bytes: Arc::new(AtomicUsize::new(0)),
                credit: CreditWindow::new(TUNNEL_INITIAL_CREDIT_BYTES),
                abort: task.abort_handle(),
            },
        );
    }
    for id in [1, 2] {
        assert!(
            handle_frame(
                Frame::Data {
                    stream_id: id,
                    payload: vec![7]
                },
                &mut sink,
                &mut streams,
                &out_tx,
                TunnelLimits::default(),
                &budget
            )
            .await
        );
    }
    let Message::Binary(bytes) = client.next().await.unwrap().unwrap() else {
        panic!("expected CLOSE");
    };
    assert_eq!(
        Frame::decode(&bytes).unwrap(),
        Frame::Close { stream_id: 2 }
    );
    assert!(streams.contains_key(&1));
    assert!(!streams.contains_key(&2));
    assert_eq!(budget.available_permits(), 0);
    let writing = receivers[0].recv().await.unwrap();
    assert_eq!(
        budget.available_permits(),
        0,
        "currently writing bytes remain budgeted"
    );
    drop(writing);
    assert_eq!(budget.available_permits(), 1);
    streams.remove(&1).unwrap().abort.abort();
}

/// Poll `ready` between scheduler yields until it holds or `attempts` runs
/// out. Returns whether it held, so callers can wait for progress that the
/// fixed code makes without hanging on code that never makes it.
async fn yield_until(attempts: usize, ready: impl Fn() -> bool) -> bool {
    for _ in 0..attempts {
        if ready() {
            return true;
        }
        tokio::task::yield_now().await;
    }
    ready()
}

/// Regression for intent-hq/intent#5461, at the relay level. A loopback
/// response larger than the shared daemon→client queue parks the relay on
/// outbound admission while the WebSocket client lags (`out_rx` left undrained
/// stands in for the connection loop blocked on a slow sink). Client→daemon
/// frames for the SAME stream must keep draining into the loopback socket
/// meanwhile: ordinary request traffic behind a large reply may not fill the
/// stream's inbound queue and close it. The sibling/heartbeat checks here
/// exercise `forward_to_stream` and the sink directly, so they only show the
/// parked relay holds no lock those paths need; the connection loop is driven
/// by `slow_client_keeps_stream_and_heartbeats_alive_behind_large_response`.
/// Fails on the pre-fix relay at request `STREAM_QUEUE_FRAMES`.
#[tokio::test]
async fn lagging_client_does_not_fill_inbound_queue_behind_large_response() {
    const RESPONSE_BYTES: usize = 2 * OUTBOUND_QUEUE_FRAMES * READ_CHUNK_BYTES;
    const REQUESTS: usize = STREAM_QUEUE_FRAMES + 1;
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    // Like a JSON-RPC server flushing one oversized reply, the consumer writes
    // the whole response before it reads the requests queued behind it.
    let consumer = tokio::spawn(async move {
        let (mut tcp, _) = listener.accept().await.unwrap();
        tcp.write_all(&vec![0xAB; RESPONSE_BYTES]).await.unwrap();
        let mut requests = vec![0u8; REQUESTS];
        tcp.read_exact(&mut requests).await.unwrap();
        requests
    });
    let (server_io, client_io) = tokio::io::duplex(1024);
    let server = WebSocketStream::from_raw_socket(
        server_io,
        tokio_tungstenite::tungstenite::protocol::Role::Server,
        None,
    )
    .await;
    let mut client = WebSocketStream::from_raw_socket(
        client_io,
        tokio_tungstenite::tungstenite::protocol::Role::Client,
        None,
    )
    .await;
    let (mut sink, _) = server.split();
    let (out_tx, mut out_rx) = mpsc::channel(OUTBOUND_QUEUE_FRAMES);
    let budget = Arc::new(Semaphore::new(INBOUND_BYTES_PER_CONNECTION));
    let mut streams = HashMap::new();
    let (sibling_tx, mut sibling_rx) = mpsc::channel(STREAM_QUEUE_FRAMES);
    let sibling = tokio::spawn(std::future::pending::<()>());
    streams.insert(
        2,
        StreamHandle {
            port: 0,
            generation: Arc::new(()),
            msg_tx: sibling_tx,
            queued_bytes: Arc::new(AtomicUsize::new(0)),
            credit: CreditWindow::new(TUNNEL_INITIAL_CREDIT_BYTES),
            abort: sibling.abort_handle(),
        },
    );
    assert!(
        handle_frame(
            Frame::Open { stream_id: 1, port },
            &mut sink,
            &mut streams,
            &out_tx,
            TunnelLimits::default(),
            &budget,
        )
        .await
    );
    let opened = tokio::time::timeout(Duration::from_secs(5), out_rx.recv())
        .await
        .expect("OPEN_OK within the deadline")
        .expect("relay alive");
    assert_eq!(opened.frame, Frame::OpenOk { stream_id: 1 });
    // The lagging client has room for the whole reply (it is slow, not
    // blocked): grant the credit beyond the initial window up front so the
    // relay's pause is the shared queue, not the per-stream window (#5482).
    assert!(
        handle_frame(
            Frame::Credit {
                stream_id: 1,
                credit: u32::try_from(RESPONSE_BYTES).unwrap(),
            },
            &mut sink,
            &mut streams,
            &out_tx,
            TunnelLimits::default(),
            &budget,
        )
        .await
    );
    // Client pauses: the relay fills the shared queue and is left holding a
    // chunk it cannot admit.
    assert!(
        yield_until(100_000, || out_tx.capacity() == 0).await,
        "response must saturate the outbound queue"
    );
    let relay_queue = streams[&1].msg_tx.clone();
    for request in 0..REQUESTS {
        assert!(tokio::time::timeout(
            Duration::from_secs(1),
            handle_frame(
                Frame::Data {
                    stream_id: 1,
                    payload: vec![u8::try_from(request).unwrap()],
                },
                &mut sink,
                &mut streams,
                &out_tx,
                TunnelLimits::default(),
                &budget,
            ),
        )
        .await
        .expect("queue admission must not wait for the consumer"));
        assert!(
            streams.contains_key(&1),
            "request {request} closed the stream behind the lagging response"
        );
        // A relay that keeps draining empties its queue between requests.
        yield_until(64, || relay_queue.capacity() == relay_queue.max_capacity()).await;
    }
    assert!(client.next().now_or_never().is_none(), "no stream CLOSE");
    // Siblings and heartbeats are unaffected by the parked relay.
    assert!(forward_to_stream(&mut sink, &mut streams, 2, data(vec![9])).await);
    assert!(
        matches!(sibling_rx.recv().await, Some(StreamMsg::Data(bytes, _permit)) if bytes == vec![9])
    );
    assert!(sink.send(Message::Ping(Bytes::new())).await.is_ok());
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), client.next())
            .await
            .expect("ping reaches the client promptly"),
        Some(Ok(Message::Ping(_)))
    ));
    // Client resumes: the whole response arrives, and the consumer then reads
    // every request the relay wrote through while the client lagged.
    let mut received = 0;
    while received < RESPONSE_BYTES {
        let outbound = tokio::time::timeout(Duration::from_secs(5), out_rx.recv())
            .await
            .expect("response keeps flowing once the client resumes")
            .expect("relay alive");
        match outbound.frame {
            Frame::Data {
                stream_id: 1,
                payload,
            } => received += payload.len(),
            other => panic!("unexpected frame {other:?}"),
        }
    }
    assert_eq!(received, RESPONSE_BYTES);
    let requests = tokio::time::timeout(Duration::from_secs(5), consumer)
        .await
        .expect("consumer reads the queued requests")
        .unwrap();
    assert_eq!(
        requests,
        (0..REQUESTS)
            .map(|i| u8::try_from(i).unwrap())
            .collect::<Vec<_>>()
    );
    streams.remove(&1).unwrap().abort.abort();
    sibling.abort();
}

/// The mirror image of the test above: the client→daemon write is a `select!`
/// branch too. The loopback peer floods a response past the shared queue and
/// then never reads (a 1 KiB receive buffer means its socket fills at once),
/// so the relay is left holding a chunk it cannot admit. A 1 MiB client frame
/// then arrives whose loopback write cannot complete. When the client drains
/// the queue, the held chunk must still be admitted: a relay parked inside
/// `write_all` never polls `reserve`, so the two directions deadlock until
/// the idle timeout. Fails on a relay that awaits the loopback write inline.
#[tokio::test]
async fn blocked_loopback_write_does_not_hold_back_admitted_output() {
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket.set_recv_buffer_size(1024).unwrap();
    socket.bind((Ipv4Addr::LOCALHOST, 0).into()).unwrap();
    let listener = socket.listen(1).unwrap();
    let port = listener.local_addr().unwrap().port();
    let consumer = tokio::spawn(async move {
        let (mut tcp, _) = listener.accept().await.unwrap();
        tcp.write_all(&vec![0xAB; (OUTBOUND_QUEUE_FRAMES + 1) * READ_CHUNK_BYTES])
            .await
            .unwrap();
        std::future::pending::<()>().await;
    });
    let (msg_tx, msg_rx) = mpsc::channel(STREAM_QUEUE_FRAMES);
    let (out_tx, mut out_rx) = mpsc::channel(OUTBOUND_QUEUE_FRAMES);
    let queued_bytes = Arc::new(AtomicUsize::new(0));
    // Credit covers the whole reply so this test exercises only the outbound
    // queue slot, never the credit window.
    let reply_bytes = u32::try_from((OUTBOUND_QUEUE_FRAMES + 1) * READ_CHUNK_BYTES).unwrap();
    let relay = tokio::spawn(run_stream(
        1,
        Arc::new(()),
        port,
        msg_rx,
        queued_bytes.clone(),
        CreditWindow::new(reply_bytes),
        out_tx.clone(),
        TunnelLimits::default(),
    ));
    let opened = tokio::time::timeout(Duration::from_secs(5), out_rx.recv())
        .await
        .expect("OPEN_OK within the deadline")
        .expect("relay alive");
    assert_eq!(opened.frame, Frame::OpenOk { stream_id: 1 });
    assert!(
        yield_until(100_000, || out_tx.capacity() == 0).await,
        "response must saturate the outbound queue"
    );
    // The relay takes the frame (queued bytes drop to zero) and starts a write
    // that cannot finish against the non-reading peer.
    let request = vec![9u8; MAX_DATA_PAYLOAD_BYTES];
    queued_bytes.fetch_add(request.len(), Ordering::Relaxed);
    msg_tx.try_send(data(request)).ok().unwrap();
    assert!(
        yield_until(100_000, || queued_bytes.load(Ordering::Relaxed) == 0).await,
        "relay must take the client frame while its output is held"
    );
    // Client resumes: the queue drains, and the held chunk must follow.
    for _ in 0..OUTBOUND_QUEUE_FRAMES {
        let outbound = tokio::time::timeout(Duration::from_secs(5), out_rx.recv())
            .await
            .expect("queued response chunks")
            .expect("relay alive");
        assert!(matches!(outbound.frame, Frame::Data { stream_id: 1, .. }));
    }
    let held = tokio::time::timeout(Duration::from_secs(1), out_rx.recv())
        .await
        .expect("held chunk is admitted once a slot frees, despite the blocked loopback write")
        .expect("relay alive");
    assert!(matches!(held.frame, Frame::Data { stream_id: 1, .. }));
    relay.abort();
    consumer.abort();
}

/// Drain `DATA` frames for stream 1 from `out_rx` until `expected` payload
/// bytes have arrived, failing on any other frame or on a stall.
async fn recv_data_bytes(
    out_rx: &mut mpsc::Receiver<OutboundFrame>,
    expected: usize,
) -> Vec<usize> {
    let mut received = 0;
    let mut chunks = Vec::new();
    while received < expected {
        let outbound = tokio::time::timeout(Duration::from_secs(5), out_rx.recv())
            .await
            .unwrap_or_else(|_| {
                panic!("relay stalled after {received} of {expected} granted bytes")
            })
            .expect("relay alive");
        match outbound.frame {
            Frame::Data {
                stream_id: 1,
                payload,
            } => {
                received += payload.len();
                chunks.push(payload.len());
            }
            other => panic!("unexpected frame {other:?} after {received} bytes"),
        }
    }
    assert_eq!(received, expected, "relay overshot the granted window");
    chunks
}

/// Wait until the producer's `written` counter stops moving for a while and
/// return where it stalled.
async fn producer_stall(written: &AtomicUsize) -> usize {
    let (mut stalled_at, mut stable) = (0, 0);
    while stable < 1_000 {
        let now = written.load(Ordering::Relaxed);
        if now == stalled_at {
            stable += 1;
        } else {
            (stalled_at, stable) = (now, 0);
        }
        tokio::task::yield_now().await;
    }
    stalled_at
}

/// Per-stream daemon→client flow control at the relay level
/// (intent-hq/intent#5482). With a consumer draining `out_rx` eagerly, the
/// relay delivers exactly the initial window and then pauses its loopback
/// read — the producer stalls short of its full reply — while nothing else
/// about the relay changes. A `CREDIT` grant resumes it for exactly the
/// granted byte count, split at the 16 KiB read chunk, and it pauses again.
#[tokio::test]
async fn credit_replenishment_resumes_exactly_the_granted_bytes() {
    // Far more than the window plus any kernel socket buffering, so a relay
    // that ignores credit visibly lets the producer run ahead.
    const REPLY_BYTES: usize = 8 * 1024 * 1024;
    const GRANT: usize = READ_CHUNK_BYTES + 3_616;
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    socket
        .set_send_buffer_size(u32::try_from(READ_CHUNK_BYTES).unwrap())
        .unwrap();
    socket.bind((Ipv4Addr::LOCALHOST, 0).into()).unwrap();
    let listener = socket.listen(1).unwrap();
    let port = listener.local_addr().unwrap().port();
    let written = Arc::new(AtomicUsize::new(0));
    let producer = tokio::spawn({
        let written = written.clone();
        async move {
            let (mut tcp, _) = listener.accept().await.unwrap();
            let chunk = vec![0xAB; READ_CHUNK_BYTES];
            for _ in 0..REPLY_BYTES / READ_CHUNK_BYTES {
                tcp.write_all(&chunk).await.unwrap();
                written.fetch_add(chunk.len(), Ordering::Relaxed);
            }
            std::future::pending::<()>().await;
        }
    });
    let (_msg_tx, msg_rx) = mpsc::channel(STREAM_QUEUE_FRAMES);
    let (out_tx, mut out_rx) = mpsc::channel(OUTBOUND_QUEUE_FRAMES);
    let credit = CreditWindow::new(TUNNEL_INITIAL_CREDIT_BYTES);
    let relay = tokio::spawn(run_stream(
        1,
        Arc::new(()),
        port,
        msg_rx,
        Arc::new(AtomicUsize::new(0)),
        credit.clone(),
        out_tx,
        TunnelLimits::default(),
    ));
    let opened = tokio::time::timeout(Duration::from_secs(5), out_rx.recv())
        .await
        .expect("OPEN_OK within the deadline")
        .expect("relay alive");
    assert_eq!(opened.frame, Frame::OpenOk { stream_id: 1 });
    let window = usize::try_from(TUNNEL_INITIAL_CREDIT_BYTES).unwrap();
    recv_data_bytes(&mut out_rx, window).await;
    assert_eq!(credit.available(), 0, "the initial window is spent");
    // Paused: the producer backs up against the unread socket, and nothing
    // further is admitted downstream.
    let stalled_at = producer_stall(&written).await;
    assert!(
        stalled_at < REPLY_BYTES,
        "producer wrote the whole reply while the relay held no credit"
    );
    assert!(
        out_rx.recv().now_or_never().is_none(),
        "relay sent DATA beyond the granted window"
    );
    assert!(!relay.is_finished(), "pause must not end the stream");
    credit.grant(u32::try_from(GRANT).unwrap());
    let chunks = recv_data_bytes(&mut out_rx, GRANT).await;
    assert!(
        chunks.iter().all(|len| *len <= READ_CHUNK_BYTES),
        "reads stay within the 16 KiB chunk: {chunks:?}"
    );
    assert!(
        chunks.len() >= 2,
        "a grant above one chunk is split: {chunks:?}"
    );
    assert_eq!(credit.available(), 0);
    producer_stall(&written).await;
    assert!(
        out_rx.recv().now_or_never().is_none(),
        "relay sent DATA beyond the replenished window"
    );
    assert!(!relay.is_finished());
    relay.abort();
    producer.abort();
}

/// Short idle/starvation deadline for the credit-exhaustion tests.
const STARVATION_IDLE: Duration = Duration::from_millis(600);

/// A relay under test for credit starvation: its loopback peer writes
/// `reply_bytes` (then half-closes its write side if `peer_half_closes`),
/// then drains everything the client uploads; the relay starts with a
/// `window` of credit and `STARVATION_IDLE` as its idle timeout. A feeder
/// task keeps uploading small client frames every quarter of that timeout so
/// the plain idle timer can never fire.
struct StarvedRelay {
    relay: tokio::task::JoinHandle<()>,
    out_rx: mpsc::Receiver<OutboundFrame>,
    credit: Arc<CreditWindow>,
    _peer: tokio::task::JoinHandle<()>,
    _feeder: tokio::task::JoinHandle<()>,
}

async fn starved_relay(reply_bytes: usize, window: u32) -> StarvedRelay {
    starved_relay_with(reply_bytes, window, false).await
}

async fn starved_relay_with(
    reply_bytes: usize,
    window: u32,
    peer_half_closes: bool,
) -> StarvedRelay {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let peer = tokio::spawn(async move {
        let (mut tcp, _) = listener.accept().await.unwrap();
        tcp.write_all(&vec![0xCD; reply_bytes]).await.unwrap();
        if peer_half_closes {
            tcp.shutdown().await.unwrap();
        }
        let mut sink = [0u8; 1024];
        while !matches!(tcp.read(&mut sink).await, Ok(0) | Err(_)) {}
    });
    let (msg_tx, msg_rx) = mpsc::channel(STREAM_QUEUE_FRAMES);
    let (out_tx, mut out_rx) = mpsc::channel(OUTBOUND_QUEUE_FRAMES);
    let credit = CreditWindow::new(window);
    let relay = tokio::spawn(run_stream(
        1,
        Arc::new(()),
        port,
        msg_rx,
        Arc::new(AtomicUsize::new(0)),
        credit.clone(),
        out_tx,
        TunnelLimits {
            idle_timeout: STARVATION_IDLE,
            ..TunnelLimits::default()
        },
    ));
    let feeder = tokio::spawn(async move {
        let mut tick = tokio::time::interval(STARVATION_IDLE / 4);
        loop {
            tick.tick().await;
            if msg_tx.send(data(vec![0x01; 64])).await.is_err() {
                break;
            }
        }
    });
    let opened = tokio::time::timeout(Duration::from_secs(5), out_rx.recv())
        .await
        .expect("OPEN_OK within the deadline")
        .expect("relay alive");
    assert_eq!(opened.frame, Frame::OpenOk { stream_id: 1 });
    StarvedRelay {
        relay,
        out_rx,
        credit,
        _peer: peer,
        _feeder: feeder,
    }
}

/// Zero credit alone is not starvation: a pre-credit
/// client that received exactly its window and then keeps uploading must
/// keep its stream — nothing is waiting on the loopback, so the
/// credit-exhaustion deadline never arms and the upload resets the idle
/// timer as usual.
#[tokio::test]
async fn zero_credit_stream_with_nothing_waiting_survives_past_idle_timeout() {
    let window = 4 * READ_CHUNK_BYTES;
    let mut t = starved_relay(window, u32::try_from(window).unwrap()).await;
    recv_data_bytes(&mut t.out_rx, window).await;
    assert_eq!(t.credit.available(), 0, "the window is spent");
    let next = tokio::time::timeout(STARVATION_IDLE * 5, t.out_rx.recv())
        .await
        .ok()
        .map(|frame| frame.map(|f| f.frame));
    assert!(
        next.is_none(),
        "zero-credit stream with nothing waiting was torn down: {next:?}"
    );
    assert!(!t.relay.is_finished());
    t.relay.abort();
}

/// A peer half-close is not waiting output either: a peer that sends exactly
/// the window and then shuts its write side while still draining the upload
/// leaves no payload pending, so the zero-credit stream survives past
/// `idle_timeout` with its upload flowing; the first grant then forwards the
/// deferred `EOF` and the stream stays open for the inbound direction.
#[tokio::test]
async fn half_closed_zero_credit_stream_survives_and_forwards_eof_on_grant() {
    let window = 4 * READ_CHUNK_BYTES;
    let mut t = starved_relay_with(window, u32::try_from(window).unwrap(), true).await;
    recv_data_bytes(&mut t.out_rx, window).await;
    assert_eq!(t.credit.available(), 0, "the window is spent");
    let next = tokio::time::timeout(STARVATION_IDLE * 5, t.out_rx.recv())
        .await
        .ok()
        .map(|frame| frame.map(|f| f.frame));
    assert!(
        next.is_none(),
        "half-closed zero-credit stream was torn down: {next:?}"
    );
    assert!(!t.relay.is_finished());
    t.credit.grant(1);
    let eof = tokio::time::timeout(Duration::from_secs(5), t.out_rx.recv())
        .await
        .expect("deferred EOF forwarded once credit returns")
        .expect("relay alive");
    assert_eq!(eof.frame, Frame::Eof { stream_id: 1 });
    let next = tokio::time::timeout(STARVATION_IDLE * 2, t.out_rx.recv())
        .await
        .ok()
        .map(|frame| frame.map(|f| f.frame));
    assert!(
        next.is_none(),
        "stream torn down after forwarding EOF while its upload is active: {next:?}"
    );
    assert!(!t.relay.is_finished());
    t.relay.abort();
}

/// Genuine starvation closes: with the window spent AND loopback output
/// waiting behind it, the stream is closed after `idle_timeout` even though
/// the client's upload keeps resetting the plain idle timer.
#[tokio::test]
async fn starved_stream_with_waiting_output_closes_after_idle_timeout() {
    let window = 4 * READ_CHUNK_BYTES;
    let mut t = starved_relay(2 * window, u32::try_from(window).unwrap()).await;
    recv_data_bytes(&mut t.out_rx, window).await;
    let exhausted_at = Instant::now();
    assert_eq!(t.credit.available(), 0, "the window is spent");
    let closed = tokio::time::timeout(STARVATION_IDLE * 5, t.out_rx.recv())
        .await
        .expect("starved stream closed within the deadline")
        .expect("relay alive");
    assert_eq!(closed.frame, Frame::Close { stream_id: 1 });
    assert!(
        exhausted_at.elapsed() >= STARVATION_IDLE / 2,
        "closed after {:?}, before the starvation deadline",
        exhausted_at.elapsed()
    );
    tokio::time::timeout(Duration::from_secs(5), t.relay)
        .await
        .expect("relay task ends with its CLOSE")
        .unwrap();
}

/// A client `CREDIT` cancels the pending starvation deadline: grants spaced
/// well inside `idle_timeout` keep a stream with waiting output alive for
/// several timeouts, each delivering exactly the granted byte, and only once
/// the grants stop does the stream close.
#[tokio::test]
async fn credit_grant_clears_the_starvation_deadline() {
    let window = 4 * READ_CHUNK_BYTES;
    let mut t = starved_relay(2 * window, u32::try_from(window).unwrap()).await;
    recv_data_bytes(&mut t.out_rx, window).await;
    assert_eq!(t.credit.available(), 0, "the window is spent");
    let mut tick = tokio::time::interval(STARVATION_IDLE / 4);
    for _ in 0..12 {
        tick.tick().await;
        t.credit.grant(1);
        recv_data_bytes(&mut t.out_rx, 1).await;
        assert!(!t.relay.is_finished(), "grant did not clear the deadline");
    }
    let closed = tokio::time::timeout(STARVATION_IDLE * 5, t.out_rx.recv())
        .await
        .expect("stream closes once grants stop")
        .expect("relay alive");
    assert_eq!(closed.frame, Frame::Close { stream_id: 1 });
}

type ClientWs = WebSocketStream<tokio::io::DuplexStream>;

/// Send one tunnel frame from the client side of an in-process WebSocket.
async fn client_send(sink: &mut SplitSink<ClientWs, Message>, frame: Frame) {
    assert!(sink
        .send(Message::Binary(frame.encode().into()))
        .await
        .is_ok());
}

/// Next client-side message, failing loudly if the connection stops progressing.
async fn client_next(stream: &mut futures_util::stream::SplitStream<ClientWs>) -> Message {
    tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("the connection keeps making progress")
        .expect("connection alive")
        .expect("clean WebSocket message")
}

/// Companion to the relay-level tests above, through `run_tunnel_connection`
/// on an in-process WebSocket. The client first withholds reads until the
/// response (twice the shared outbound queue) has visibly saturated that queue
/// — the consumer's own write backs up behind the parked relay — and only then
/// pipelines its requests, a sibling-stream echo, and a heartbeat ping, all
/// while the reply is still queued. It then reads slowly but keeps progressing
/// (a 4 KiB duplex buffer means every 16 KiB `DATA` frame blocks the sink
/// until the client takes it). The stream survives, the consumer gets every
/// request in order, and the echo and ping reach the client before the reply
/// has drained. This guards the connection-loop contract rather than
/// reproducing the bug: the connection loop hands requests over interleaved
/// with reply chunks, so the pre-fix relay may drain between them too — the
/// RED reproductions are the relay-level tests above.
///
/// A client that stops reading altogether parks the connection loop in
/// `sink.send`, which stalls the whole socket — every stream and heartbeat —
/// until the WebSocket write completes. That is bounded by the per-stream
/// credit window (intent-hq/intent#5482), covered by
/// `blocked_sink_client_stalls_only_its_own_stream_at_the_credit_window`.
#[tokio::test]
async fn slow_client_keeps_stream_and_heartbeats_alive_behind_large_response() {
    const RESPONSE_BYTES: usize = 2 * OUTBOUND_QUEUE_FRAMES * READ_CHUNK_BYTES;
    const REQUESTS: usize = STREAM_QUEUE_FRAMES + 1;
    let (requests_tx, requests_rx) = tokio::sync::oneshot::channel();
    // A one-chunk send buffer keeps the consumer's write from running ahead
    // into the kernel once the relay stops reading (the relay's receive window
    // cannot grow while it is not reading), so queue saturation shows up as
    // the consumer stalling short of the full response.
    let response_socket = tokio::net::TcpSocket::new_v4().unwrap();
    response_socket
        .set_send_buffer_size(u32::try_from(READ_CHUNK_BYTES).unwrap())
        .unwrap();
    response_socket
        .bind((Ipv4Addr::LOCALHOST, 0).into())
        .unwrap();
    let response_listener = response_socket.listen(1).unwrap();
    let response_port = response_listener.local_addr().unwrap().port();
    let written = Arc::new(AtomicUsize::new(0));
    let (start_tx, start_rx) = tokio::sync::oneshot::channel::<()>();
    // Once released, flushes the oversized reply first, then reads the
    // requests queued behind it, then stays connected so no EOF/CLOSE races
    // the assertions.
    let response_consumer = tokio::spawn({
        let written = written.clone();
        async move {
            let (mut tcp, _) = response_listener.accept().await.unwrap();
            start_rx.await.unwrap();
            let chunk = vec![0xAB; READ_CHUNK_BYTES];
            for _ in 0..RESPONSE_BYTES / READ_CHUNK_BYTES {
                tcp.write_all(&chunk).await.unwrap();
                written.fetch_add(chunk.len(), Ordering::Relaxed);
            }
            let mut requests = vec![0u8; REQUESTS];
            tcp.read_exact(&mut requests).await.unwrap();
            requests_tx.send(requests).unwrap();
            std::future::pending::<()>().await;
        }
    });
    let echo_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let echo_port = echo_listener.local_addr().unwrap().port();
    let echo_consumer = tokio::spawn(async move {
        let (mut tcp, _) = echo_listener.accept().await.unwrap();
        while let Ok(byte) = tcp.read_u8().await {
            tcp.write_u8(byte).await.unwrap();
        }
    });
    let (server_io, client_io) = tokio::io::duplex(4 * 1024);
    let server = WebSocketStream::from_raw_socket(
        server_io,
        tokio_tungstenite::tungstenite::protocol::Role::Server,
        None,
    )
    .await;
    let client = WebSocketStream::from_raw_socket(
        client_io,
        tokio_tungstenite::tungstenite::protocol::Role::Client,
        None,
    )
    .await;
    let (mut client_sink, mut client_stream) = client.split();
    let (cmd_tx, cmd_rx) = mpsc::channel(4);
    let connection = tokio::spawn(run_tunnel_connection(
        server,
        cmd_rx,
        Arc::new(AtomicI64::new(0)),
        TunnelLimits::default(),
    ));
    for (stream_id, port) in [(2, echo_port), (1, response_port)] {
        client_send(&mut client_sink, Frame::Open { stream_id, port }).await;
        match client_next(&mut client_stream).await {
            Message::Binary(bytes) => assert!(
                matches!(Frame::decode(&bytes).unwrap(), Frame::OpenOk { stream_id: id } if id == stream_id),
                "stream {stream_id} did not open first"
            ),
            other => panic!("unexpected message {other:?}"),
        }
    }
    // The client is slow, not blocked: it has room for the whole reply, so it
    // grants the credit beyond the initial window up front, while the
    // connection is idle. The pause below is then the shared queue and the
    // parked sink, not the per-stream window (#5482). The grant carries no
    // acknowledgement, so a sibling echo sent behind it proves the connection
    // loop consumed it (inbound frames are handled in order) before the reply
    // is released — otherwise the grant races the first reply chunks for the
    // loop and can sit unread behind a parked `sink.send`.
    client_send(
        &mut client_sink,
        Frame::Credit {
            stream_id: 1,
            credit: u32::try_from(RESPONSE_BYTES).unwrap(),
        },
    )
    .await;
    client_send(
        &mut client_sink,
        Frame::Data {
            stream_id: 2,
            payload: vec![0],
        },
    )
    .await;
    match client_next(&mut client_stream).await {
        Message::Binary(bytes) => assert!(
            matches!(
                Frame::decode(&bytes).unwrap(),
                Frame::Data { stream_id: 2, payload } if payload == [0]
            ),
            "sibling echo did not confirm the credit grant"
        ),
        other => panic!("unexpected message {other:?}"),
    }
    start_tx.send(()).unwrap();
    // Client pauses: the connection loop parks in `sink.send` on one chunk,
    // the relay fills the shared queue and holds the next, and the consumer
    // has therefore pushed at least that many chunks — then stalls, because
    // nothing downstream is taking more.
    let saturated = (OUTBOUND_QUEUE_FRAMES + 2) * READ_CHUNK_BYTES;
    assert!(
        yield_until(100_000, || written.load(Ordering::Relaxed) >= saturated).await,
        "response must fill the outbound queue while the client pauses"
    );
    let (mut stalled_at, mut stable) = (0, 0);
    while stable < 1_000 {
        let now = written.load(Ordering::Relaxed);
        if now == stalled_at {
            stable += 1;
        } else {
            (stalled_at, stable) = (now, 0);
        }
        tokio::task::yield_now().await;
    }
    assert!(
        stalled_at < RESPONSE_BYTES,
        "consumer wrote the whole {RESPONSE_BYTES}-byte reply without the client reading: \
         the outbound queue never saturated"
    );
    // Everything the client sends now lands behind the saturated queue.
    for request in 0..REQUESTS {
        client_send(
            &mut client_sink,
            Frame::Data {
                stream_id: 1,
                payload: vec![u8::try_from(request).unwrap()],
            },
        )
        .await;
    }
    client_send(
        &mut client_sink,
        Frame::Data {
            stream_id: 2,
            payload: vec![7],
        },
    )
    .await;
    cmd_tx.send(ConnCmd::Ping).await.unwrap();
    let mut received = 0;
    // Response bytes received when the sibling echo / heartbeat ping arrived:
    // both must land while the large reply is still draining.
    let mut echo_at: Option<usize> = None;
    let mut ping_at: Option<usize> = None;
    while received < RESPONSE_BYTES || echo_at.is_none() || ping_at.is_none() {
        match client_next(&mut client_stream).await {
            Message::Binary(bytes) => match Frame::decode(&bytes).unwrap() {
                Frame::Data {
                    stream_id: 1,
                    payload,
                } => received += payload.len(),
                Frame::Data {
                    stream_id: 2,
                    payload,
                } => {
                    assert_eq!(payload, vec![7]);
                    echo_at = Some(received);
                }
                Frame::Data { stream_id, payload } => panic!(
                    "unexpected {} DATA bytes on stream {stream_id} after {received} response bytes",
                    payload.len()
                ),
                other => panic!("unexpected frame {other:?} after {received} response bytes"),
            },
            Message::Ping(_) => ping_at = Some(received),
            other => panic!("unexpected message {other:?}"),
        }
    }
    assert_eq!(received, RESPONSE_BYTES);
    let echo_at = echo_at.unwrap();
    let ping_at = ping_at.unwrap();
    assert!(
        echo_at < RESPONSE_BYTES,
        "sibling echo arrived only after the {RESPONSE_BYTES}-byte reply fully drained"
    );
    assert!(
        ping_at < RESPONSE_BYTES,
        "heartbeat ping arrived only after the {RESPONSE_BYTES}-byte reply fully drained"
    );
    let requests = tokio::time::timeout(Duration::from_secs(5), requests_rx)
        .await
        .expect("consumer reads the queued requests")
        .unwrap();
    assert_eq!(
        requests,
        (0..REQUESTS)
            .map(|i| u8::try_from(i).unwrap())
            .collect::<Vec<_>>()
    );
    drop(cmd_tx);
    drop(client_sink);
    drop(client_stream);
    tokio::time::timeout(Duration::from_secs(5), connection)
        .await
        .expect("connection loop ends once its command channel closes")
        .unwrap();
    response_consumer.abort();
    echo_consumer.abort();
}

/// Regression for intent-hq/intent#5482 through `run_tunnel_connection`: a
/// client whose local socket for stream A is blocked stops reading its
/// WebSocket entirely once it has taken A's initial window. Stream B's
/// loopback peer then produces an echo and a heartbeat ping falls due. On a
/// daemon without per-stream credit the relay keeps queuing A's reply and the
/// connection loop parks in `sink.send` on a chunk the client will never
/// take, so the ping command is never even consumed and B's echo sits behind
/// it — the whole socket stalls. With the credit window the daemon sends
/// exactly `TUNNEL_INITIAL_CREDIT_BYTES` on A and then pauses only A's
/// loopback read: the ping and B's echo are still delivered, and a later
/// `CREDIT` resumes A for exactly the granted byte count before it pauses
/// again.
#[tokio::test]
async fn blocked_sink_client_stalls_only_its_own_stream_at_the_credit_window() {
    const REPLY_BYTES: usize = 8 * 1024 * 1024;
    const GRANT: usize = 4 * READ_CHUNK_BYTES;
    let window = usize::try_from(TUNNEL_INITIAL_CREDIT_BYTES).unwrap();
    let reply_socket = tokio::net::TcpSocket::new_v4().unwrap();
    reply_socket
        .set_send_buffer_size(u32::try_from(READ_CHUNK_BYTES).unwrap())
        .unwrap();
    reply_socket.bind((Ipv4Addr::LOCALHOST, 0).into()).unwrap();
    let reply_listener = reply_socket.listen(1).unwrap();
    let reply_port = reply_listener.local_addr().unwrap().port();
    let written = Arc::new(AtomicUsize::new(0));
    let reply_producer = tokio::spawn({
        let written = written.clone();
        async move {
            let (mut tcp, _) = reply_listener.accept().await.unwrap();
            let chunk = vec![0xAB; READ_CHUNK_BYTES];
            for _ in 0..REPLY_BYTES / READ_CHUNK_BYTES {
                tcp.write_all(&chunk).await.unwrap();
                written.fetch_add(chunk.len(), Ordering::Relaxed);
            }
            std::future::pending::<()>().await;
        }
    });
    let echo_listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let echo_port = echo_listener.local_addr().unwrap().port();
    let echo_peer = tokio::spawn(async move {
        let (mut tcp, _) = echo_listener.accept().await.unwrap();
        loop {
            let byte = tcp.read_u8().await.unwrap();
            tcp.write_u8(byte).await.unwrap();
        }
    });
    let (server_io, client_io) = tokio::io::duplex(4 * 1024);
    let server = WebSocketStream::from_raw_socket(
        server_io,
        tokio_tungstenite::tungstenite::protocol::Role::Server,
        None,
    )
    .await;
    let client = WebSocketStream::from_raw_socket(
        client_io,
        tokio_tungstenite::tungstenite::protocol::Role::Client,
        None,
    )
    .await;
    let (mut client_sink, mut client_stream) = client.split();
    // Capacity one: whether the loop has taken the ping command is observable
    // from outside, independent of whether the client reads the ping itself.
    let (cmd_tx, cmd_rx) = mpsc::channel(1);
    let connection = tokio::spawn(run_tunnel_connection(
        server,
        cmd_rx,
        Arc::new(AtomicI64::new(0)),
        TunnelLimits::default(),
    ));
    for (stream_id, port) in [(2, echo_port), (1, reply_port)] {
        client_send(&mut client_sink, Frame::Open { stream_id, port }).await;
        match client_next(&mut client_stream).await {
            Message::Binary(bytes) => assert!(
                matches!(Frame::decode(&bytes).unwrap(), Frame::OpenOk { stream_id: id } if id == stream_id),
                "stream {stream_id} did not open first"
            ),
            other => panic!("unexpected message {other:?}"),
        }
    }
    // The client takes A's initial window, then its local socket blocks and
    // it stops polling the WebSocket altogether.
    let mut received = 0;
    while received < window {
        match client_next(&mut client_stream).await {
            Message::Binary(bytes) => match Frame::decode(&bytes).unwrap() {
                Frame::Data {
                    stream_id: 1,
                    payload,
                } => received += payload.len(),
                other => panic!("unexpected frame {other:?} after {received} reply bytes"),
            },
            other => panic!("unexpected message {other:?}"),
        }
    }
    let stalled_at = producer_stall(&written).await;
    assert!(
        stalled_at < REPLY_BYTES,
        "producer wrote the whole {REPLY_BYTES}-byte reply without the client reading"
    );
    // Stream B's peer produces, and the heartbeat falls due, while A's
    // consumer is blocked.
    client_send(
        &mut client_sink,
        Frame::Data {
            stream_id: 2,
            payload: vec![7],
        },
    )
    .await;
    cmd_tx.send(ConnCmd::Ping).await.unwrap();
    assert!(
        yield_until(100_000, || cmd_tx.capacity() == 1).await,
        "heartbeat ping stalled behind the WebSocket write for stream A: the daemon kept \
         sending A's reply past the client's {window}-byte window"
    );
    // A's consumer is still blocked, but the client can read again because
    // nothing more arrived on A: only B's echo and the ping are waiting.
    let (mut echoed, mut pinged) = (false, false);
    while !(echoed && pinged) {
        match client_next(&mut client_stream).await {
            Message::Binary(bytes) => match Frame::decode(&bytes).unwrap() {
                Frame::Data {
                    stream_id: 2,
                    payload,
                } => {
                    assert_eq!(payload, vec![7]);
                    echoed = true;
                }
                Frame::Data {
                    stream_id: 1,
                    payload,
                } => panic!(
                    "stream A sent {} DATA bytes beyond its {window}-byte window without CREDIT",
                    payload.len()
                ),
                other => panic!("unexpected frame {other:?}"),
            },
            Message::Ping(_) => pinged = true,
            other => panic!("unexpected message {other:?}"),
        }
    }
    assert_eq!(received, window, "A delivered exactly its initial window");
    // A's consumer frees room: the client grants credit and receives exactly
    // that much more, after which A pauses again while B keeps echoing.
    client_send(
        &mut client_sink,
        Frame::Credit {
            stream_id: 1,
            credit: u32::try_from(GRANT).unwrap(),
        },
    )
    .await;
    let mut granted = 0;
    while granted < GRANT {
        match client_next(&mut client_stream).await {
            Message::Binary(bytes) => match Frame::decode(&bytes).unwrap() {
                Frame::Data {
                    stream_id: 1,
                    payload,
                } => granted += payload.len(),
                other => panic!("unexpected frame {other:?} after {granted} granted bytes"),
            },
            other => panic!("unexpected message {other:?}"),
        }
    }
    assert_eq!(granted, GRANT, "A overshot the granted credit");
    client_send(
        &mut client_sink,
        Frame::Data {
            stream_id: 2,
            payload: vec![8],
        },
    )
    .await;
    match client_next(&mut client_stream).await {
        Message::Binary(bytes) => match Frame::decode(&bytes).unwrap() {
            Frame::Data {
                stream_id: 2,
                payload,
            } => assert_eq!(payload, vec![8]),
            other => panic!("expected B's echo after the grant was spent, got {other:?}"),
        },
        other => panic!("unexpected message {other:?}"),
    }
    assert!(
        client_stream.next().now_or_never().is_none(),
        "A sent DATA beyond the replenished window"
    );
    drop(cmd_tx);
    drop(client_sink);
    drop(client_stream);
    tokio::time::timeout(Duration::from_secs(5), connection)
        .await
        .expect("connection loop ends once its command channel closes")
        .unwrap();
    reply_producer.abort();
    echo_peer.abort();
}
