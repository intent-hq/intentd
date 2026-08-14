//! Pure unit tests for the `/tunnel` frame codec: encode/decode round-trips
//! and malformed-frame rejection. Relay/lifecycle behavior is covered by the
//! `wss_tunnel` integration suite in the `intentd` crate.

use super::*;

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
