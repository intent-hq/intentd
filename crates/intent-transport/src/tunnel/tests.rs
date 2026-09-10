//! Unit tests for the `/tunnel` frame codec and bounded queue admission.
//! TCP relay/lifecycle behavior is covered by the
//! `wss_tunnel` integration suite in the `intentd` crate.

use super::*;
use futures_util::FutureExt;

/// Queue saturation is isolated even when the next frame is a half-close.
/// Use an unpolled receiver to make saturation deterministic, without relying
/// on kernel TCP buffer sizes or timing a slow consumer.
#[tokio::test]
async fn full_stream_queue_closes_only_that_stream() {
    for message in [StreamMsg::Data(vec![2]), StreamMsg::Eof] {
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
        assert!(blocked_tx.try_send(StreamMsg::Data(vec![1])).is_ok());
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
                    generation: blocked_generation.clone(),
                    msg_tx: blocked_tx,
                    abort: blocked.abort_handle(),
                },
            ),
            (
                2,
                StreamHandle {
                    generation: healthy_generation,
                    msg_tx: healthy_tx,
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
        assert!(forward_to_stream(&mut sink, &mut streams, 2, StreamMsg::Data(vec![3])).await);
        assert!(forward_to_stream(&mut sink, &mut streams, 2, StreamMsg::Eof).await);
        assert!(
            matches!(healthy_rx.recv().await, Some(StreamMsg::Data(bytes)) if bytes == vec![3])
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
            generation: stale_generation.clone(),
            msg_tx,
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
            generation: current_generation.clone(),
            msg_tx,
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
            generation: failed_generation.clone(),
            msg_tx,
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
    for op in [0x00u8, 0x07, 0x7F, 0xFF] {
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
    ];
    for (frame, id) in cases {
        assert_eq!(frame.stream_id(), id);
    }
}
