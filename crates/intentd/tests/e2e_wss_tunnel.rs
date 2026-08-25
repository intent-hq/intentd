//! `/tunnel` WebSocket loopback port-forwarding e2e (intent-hq/monorepo#2323).
//!
//! Drives a real [`WsApiServer`] over pinned TLS and exercises the binary mux
//! end-to-end: authenticated upgrade, `OPEN` against a live test TCP listener,
//! echo data both ways, `EOF`/`CLOSE` semantics, `OPEN_ERR` for a closed port,
//! the concurrent-stream cap, protocol violations, and the 401 auth gate that
//! `/tunnel` shares with `/ws`.

mod common;

use std::net::Ipv4Addr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use intent_core::{Result as CoreResult, WorkspaceApi};
use intent_services::{EventBus, Services};
use intent_store::Store;
use intent_transport::tunnel::{Frame, TunnelLimits, MAX_TUNNEL_MESSAGE_BYTES, OP_OPEN};
use intent_transport::{
    ensure_tls_certificate, AsyncTokenStore, TokenStore, WsApiServer, WsOptions,
};
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::{ClientConfig, DigitallySignedStruct, SignatureScheme};
use rustls_pki_types::{CertificateDer, ServerName, UnixTime};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_tungstenite::tungstenite::Message;

/// A fixed 64-char hex token (valid shape) shared by server + client in tests.
const TOKEN: &str = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";

/// In-memory [`TokenStore`] so tests never touch the real OS keychain.
#[derive(Default)]
struct MemTokenStore(Mutex<Option<String>>);

impl TokenStore for MemTokenStore {
    fn load_token(&self) -> Option<String> {
        self.0.lock().unwrap().clone()
    }
    fn store_token(&self, token: &str) -> CoreResult<()> {
        *self.0.lock().unwrap() = Some(token.to_string());
        Ok(())
    }
}

/// Client cert verifier pinning the server's SHA-256 fingerprint (colon hex).
#[derive(Debug)]
struct PinnedVerifier {
    fingerprint: String,
    provider: Arc<CryptoProvider>,
}

impl ServerCertVerifier for PinnedVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let fp = Sha256::digest(end_entity.as_ref())
            .iter()
            .map(|b| format!("{b:02X}"))
            .collect::<Vec<_>>()
            .join(":");
        if fp == self.fingerprint {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(rustls::Error::General("fingerprint mismatch".into()))
        }
    }
    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }
    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(
            message,
            cert,
            dss,
            &self.provider.signature_verification_algorithms,
        )
    }
    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.provider
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// A pinning [`ClientConfig`] built on the ring provider.
fn client_config(fingerprint: &str) -> Arc<ClientConfig> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let config = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .unwrap()
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(PinnedVerifier {
            fingerprint: fingerprint.to_string(),
            provider,
        }))
        .with_no_client_auth();
    Arc::new(config)
}

/// A started WSS listener plus the pinned client config (services + tempdir
/// are held so the listener stays alive for the test's lifetime).
struct Server {
    ws: WsApiServer,
    port: u16,
    cfg: Arc<ClientConfig>,
    _api: Arc<dyn WorkspaceApi>,
    _dir: tempfile::TempDir,
}

/// Build + start a WSS listener (TLS + bearer auth) on a free port with the
/// production tunnel limits.
async fn start() -> Server {
    start_with(TunnelLimits::default()).await
}

/// Build + start a WSS listener with custom `/tunnel` limits (tests shrink
/// the timeouts to make idle/connect/forward teardown observable).
async fn start_with(limits: TunnelLimits) -> Server {
    let dir = common::test_tempdir("intentd-wss-tunnel-");
    let store = Store::open(&dir.path().join("intentd.db"))
        .await
        .expect("open store");
    let bus = EventBus::new(store.clone());
    let workspaces_root = dir.path().join("workspaces");
    std::fs::create_dir_all(&workspaces_root).expect("mkdir hermetic workspaces root");
    let services = Services::new(store.clone())
        .with_workspaces_root(workspaces_root)
        .with_event_bus(bus.clone());
    let api: Arc<dyn WorkspaceApi> = Arc::new(services);
    let tls = ensure_tls_certificate(dir.path()).expect("cert");
    let token_store_inner = Arc::new(MemTokenStore::default());
    token_store_inner.store_token(TOKEN).unwrap();
    let token_store = Arc::new(AsyncTokenStore::new(token_store_inner));
    let opts = WsOptions {
        base_port: 0,
        bind_addresses: vec![Ipv4Addr::LOCALHOST.into()],
        tunnel_limits: limits,
        ..WsOptions::default()
    };
    let ws =
        WsApiServer::new(api.clone(), bus.clone(), &tls, &token_store, opts, None).expect("server");
    let cfg = client_config(&tls.fingerprint256);
    let port = ws.start().await.expect("start");
    Server {
        ws,
        port,
        cfg,
        _api: api,
        _dir: dir,
    }
}

/// Establish an authenticated `/tunnel` WSS connection (token in the query).
async fn connect_tunnel(port: u16, cfg: Arc<ClientConfig>) -> common::TlsWs {
    let url = format!("wss://localhost:{port}/tunnel?token={TOKEN}");
    common::wss_connect_with_retry(port, cfg, &url).await
}

/// Send one mux frame as a binary WebSocket message.
async fn send_frame(ws: &mut common::TlsWs, frame: Frame) {
    ws.send(Message::Binary(frame.encode().into()))
        .await
        .expect("send frame");
}

/// Receive the next binary mux frame, skipping pings; panics on anything else
/// or after a 10s (multiplier-scaled) stall.
async fn recv_frame(ws: &mut common::TlsWs) -> Frame {
    loop {
        let next = tokio::time::timeout(common::test_timeout(Duration::from_secs(10)), ws.next())
            .await
            .expect("timed out waiting for tunnel frame");
        match next {
            Some(Ok(Message::Binary(bytes))) => return Frame::decode(&bytes).expect("decode"),
            Some(Ok(Message::Ping(p))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            other => panic!("expected binary frame, got {other:?}"),
        }
    }
}

/// Start a TCP echo server on a free loopback port; each connection copies
/// reads back to the writer until EOF, then closes.
async fn spawn_echo_listener() -> u16 {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind echo listener");
    let port = listener.local_addr().expect("local addr").port();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                loop {
                    match sock.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => {
                            if sock.write_all(&buf[..n]).await.is_err() {
                                break;
                            }
                        }
                    }
                }
            });
        }
    });
    port
}

/// Reserve a loopback port with nothing listening on it (bind, read the port,
/// drop the listener). Connects to it are refused.
async fn closed_port() -> u16 {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind");
    let port = listener.local_addr().expect("local addr").port();
    drop(listener);
    port
}

/// OPEN a live echo port, push data both ways, then tear down with EOF: the
/// client half-close propagates to the echo server, whose own close comes
/// back as a daemon `EOF` followed by the final `CLOSE`.
#[tokio::test]
async fn tunnel_open_echo_eof_close_lifecycle() {
    let srv = start().await;
    let echo_port = spawn_echo_listener().await;
    let mut ws = connect_tunnel(srv.port, srv.cfg.clone()).await;

    send_frame(
        &mut ws,
        Frame::Open {
            stream_id: 1,
            port: echo_port,
        },
    )
    .await;
    assert_eq!(recv_frame(&mut ws).await, Frame::OpenOk { stream_id: 1 });

    // Client → daemon → echo → daemon → client, twice to prove the stream
    // stays usable.
    for payload in [b"hello tunnel".to_vec(), b"second message".to_vec()] {
        send_frame(
            &mut ws,
            Frame::Data {
                stream_id: 1,
                payload: payload.clone(),
            },
        )
        .await;
        let mut echoed = Vec::new();
        while echoed.len() < payload.len() {
            match recv_frame(&mut ws).await {
                Frame::Data {
                    stream_id: 1,
                    payload: chunk,
                } => echoed.extend_from_slice(&chunk),
                other => panic!("expected DATA, got {other:?}"),
            }
        }
        assert_eq!(echoed, payload);
    }

    // Client half-close: the daemon shuts down the TCP write side, the echo
    // server sees EOF and closes, and the daemon reports EOF then CLOSE.
    send_frame(&mut ws, Frame::Eof { stream_id: 1 }).await;
    assert_eq!(recv_frame(&mut ws).await, Frame::Eof { stream_id: 1 });
    assert_eq!(recv_frame(&mut ws).await, Frame::Close { stream_id: 1 });
    ws.close(None).await.expect("close ws");
    srv.ws.stop().await;
}

/// A client `CLOSE` tears the stream down immediately; the daemon confirms
/// with its own final `CLOSE` and the stream id becomes reusable.
#[tokio::test]
async fn tunnel_close_tears_down_and_frees_stream_id() {
    let srv = start().await;
    let echo_port = spawn_echo_listener().await;
    let mut ws = connect_tunnel(srv.port, srv.cfg.clone()).await;

    for round in 0..2u8 {
        send_frame(
            &mut ws,
            Frame::Open {
                stream_id: 9,
                port: echo_port,
            },
        )
        .await;
        assert_eq!(
            recv_frame(&mut ws).await,
            Frame::OpenOk { stream_id: 9 },
            "round {round}"
        );
        send_frame(&mut ws, Frame::Close { stream_id: 9 }).await;
        assert_eq!(
            recv_frame(&mut ws).await,
            Frame::Close { stream_id: 9 },
            "round {round}"
        );
    }
    ws.close(None).await.expect("close ws");
    srv.ws.stop().await;
}

/// `OPEN` on a port nothing listens on answers `OPEN_ERR` naming the target,
/// and the connection stays healthy for a subsequent successful `OPEN`.
#[tokio::test]
async fn tunnel_open_err_for_closed_port_keeps_connection_alive() {
    let srv = start().await;
    let dead_port = closed_port().await;
    let echo_port = spawn_echo_listener().await;
    let mut ws = connect_tunnel(srv.port, srv.cfg.clone()).await;

    send_frame(
        &mut ws,
        Frame::Open {
            stream_id: 1,
            port: dead_port,
        },
    )
    .await;
    match recv_frame(&mut ws).await {
        Frame::OpenErr { stream_id, message } => {
            assert_eq!(stream_id, 1);
            assert!(
                message.contains(&format!("127.0.0.1:{dead_port}")),
                "OPEN_ERR names the target: {message}"
            );
        }
        other => panic!("expected OPEN_ERR, got {other:?}"),
    }

    // The failed stream id is freed and the connection still works.
    send_frame(
        &mut ws,
        Frame::Open {
            stream_id: 1,
            port: echo_port,
        },
    )
    .await;
    assert_eq!(recv_frame(&mut ws).await, Frame::OpenOk { stream_id: 1 });
    ws.close(None).await.expect("close ws");
    srv.ws.stop().await;
}

/// A duplicate live stream id is rejected with `OPEN_ERR` without disturbing
/// the original stream.
#[tokio::test]
async fn tunnel_duplicate_stream_id_rejected() {
    let srv = start().await;
    let echo_port = spawn_echo_listener().await;
    let mut ws = connect_tunnel(srv.port, srv.cfg.clone()).await;

    send_frame(
        &mut ws,
        Frame::Open {
            stream_id: 5,
            port: echo_port,
        },
    )
    .await;
    assert_eq!(recv_frame(&mut ws).await, Frame::OpenOk { stream_id: 5 });
    send_frame(
        &mut ws,
        Frame::Open {
            stream_id: 5,
            port: echo_port,
        },
    )
    .await;
    match recv_frame(&mut ws).await {
        Frame::OpenErr { stream_id, message } => {
            assert_eq!(stream_id, 5);
            assert!(message.contains("duplicate"), "message: {message}");
        }
        other => panic!("expected OPEN_ERR, got {other:?}"),
    }

    // The original stream still relays data.
    send_frame(
        &mut ws,
        Frame::Data {
            stream_id: 5,
            payload: b"still alive".to_vec(),
        },
    )
    .await;
    assert_eq!(
        recv_frame(&mut ws).await,
        Frame::Data {
            stream_id: 5,
            payload: b"still alive".to_vec()
        }
    );
    ws.close(None).await.expect("close ws");
    srv.ws.stop().await;
}

/// The 33rd concurrent stream (default cap 32) is refused with `OPEN_ERR`,
/// and closing one stream frees a slot for a new `OPEN`.
#[tokio::test]
async fn tunnel_concurrent_stream_cap_enforced() {
    let srv = start().await;
    let echo_port = spawn_echo_listener().await;
    let mut ws = connect_tunnel(srv.port, srv.cfg.clone()).await;

    let cap =
        u32::try_from(intent_transport::tunnel::MAX_STREAMS_PER_CONNECTION).expect("small cap");
    for id in 0..cap {
        send_frame(
            &mut ws,
            Frame::Open {
                stream_id: id,
                port: echo_port,
            },
        )
        .await;
        assert_eq!(recv_frame(&mut ws).await, Frame::OpenOk { stream_id: id });
    }
    send_frame(
        &mut ws,
        Frame::Open {
            stream_id: cap,
            port: echo_port,
        },
    )
    .await;
    match recv_frame(&mut ws).await {
        Frame::OpenErr { stream_id, message } => {
            assert_eq!(stream_id, cap);
            assert!(message.contains("too many"), "message: {message}");
        }
        other => panic!("expected OPEN_ERR, got {other:?}"),
    }

    // Close one stream; a slot frees up for a fresh OPEN.
    send_frame(&mut ws, Frame::Close { stream_id: 0 }).await;
    assert_eq!(recv_frame(&mut ws).await, Frame::Close { stream_id: 0 });
    send_frame(
        &mut ws,
        Frame::Open {
            stream_id: cap,
            port: echo_port,
        },
    )
    .await;
    assert_eq!(recv_frame(&mut ws).await, Frame::OpenOk { stream_id: cap });
    ws.close(None).await.expect("close ws");
    srv.ws.stop().await;
}

/// A malformed binary frame (unknown opcode) closes the connection with a
/// `1002 Protocol Error` close frame.
#[tokio::test]
async fn tunnel_malformed_frame_closes_with_protocol_error() {
    let srv = start().await;
    let mut ws = connect_tunnel(srv.port, srv.cfg.clone()).await;

    let mut bytes = vec![0xEEu8];
    bytes.extend_from_slice(&1u32.to_be_bytes());
    ws.send(Message::Binary(bytes.into())).await.expect("send");
    expect_protocol_close(&mut ws, "malformed tunnel frame").await;
    srv.ws.stop().await;
}

/// Text frames are a protocol violation on `/tunnel` (binary-only endpoint).
#[tokio::test]
async fn tunnel_text_frame_closes_with_protocol_error() {
    let srv = start().await;
    let mut ws = connect_tunnel(srv.port, srv.cfg.clone()).await;

    ws.send(Message::Text("{\"jsonrpc\":\"2.0\"}".into()))
        .await
        .expect("send");
    expect_protocol_close(&mut ws, "text frames not allowed").await;
    srv.ws.stop().await;
}

/// Daemon-only opcodes (`OPEN_OK` / `OPEN_ERR`) from the client are protocol
/// violations that close the connection.
#[tokio::test]
async fn tunnel_daemon_only_opcode_from_client_rejected() {
    let srv = start().await;
    let mut ws = connect_tunnel(srv.port, srv.cfg.clone()).await;

    send_frame(&mut ws, Frame::OpenOk { stream_id: 1 }).await;
    expect_protocol_close(&mut ws, "daemon-only opcode").await;
    srv.ws.stop().await;
}

/// Await a `1002 Protocol Error` close frame whose reason contains `needle`.
async fn expect_protocol_close(ws: &mut common::TlsWs, needle: &str) {
    loop {
        let next = tokio::time::timeout(common::test_timeout(Duration::from_secs(10)), ws.next())
            .await
            .expect("timed out waiting for close frame");
        match next {
            Some(Ok(Message::Close(Some(frame)))) => {
                assert_eq!(u16::from(frame.code), 1002, "close frame: {frame:?}");
                assert!(
                    frame.reason.contains(needle),
                    "close reason {:?} should contain {needle:?}",
                    frame.reason
                );
                return;
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            other => panic!("expected close frame, got {other:?}"),
        }
    }
}

/// `/tunnel` shares the `/ws` upgrade gate: no bearer token ⇒ 401, bad token
/// ⇒ 401, before any WebSocket handshake completes.
#[tokio::test]
async fn tunnel_unauthenticated_upgrade_rejected() {
    let srv = start().await;
    for query in [
        "",
        "?token=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    ] {
        let mut tls = common::tls_connect_with_retry(srv.port, srv.cfg.clone()).await;
        let request = format!(
            "GET /tunnel{query} HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n"
        );
        tls.write_all(request.as_bytes()).await.expect("write");
        tls.flush().await.expect("flush");
        let mut buf = Vec::new();
        let _ = tls.read_to_end(&mut buf).await;
        let response = String::from_utf8_lossy(&buf);
        assert!(
            response.starts_with("HTTP/1.1 401"),
            "expected 401, got: {response}"
        );
    }
    srv.ws.stop().await;
}

/// The raw wire layout is exactly `[opcode u8][streamId u32 BE][payload]` on
/// the socket — proven with a hand-built `OPEN` (no codec on the send side).
#[tokio::test]
async fn tunnel_accepts_hand_built_wire_frames() {
    let srv = start().await;
    let echo_port = spawn_echo_listener().await;
    let mut ws = connect_tunnel(srv.port, srv.cfg.clone()).await;

    let mut raw = vec![OP_OPEN];
    raw.extend_from_slice(&7u32.to_be_bytes());
    raw.extend_from_slice(&echo_port.to_be_bytes());
    ws.send(Message::Binary(raw.into())).await.expect("send");
    assert_eq!(recv_frame(&mut ws).await, Frame::OpenOk { stream_id: 7 });
    ws.close(None).await.expect("close ws");
    srv.ws.stop().await;
}

/// `TcpStream` sanity: the daemon connects to the loopback target, so a
/// listener bound to `127.0.0.1` (unreachable from other hosts) is reachable
/// through the tunnel.
#[tokio::test]
async fn tunnel_reaches_loopback_bound_listener() {
    let srv = start().await;
    // Explicitly loopback-only listener.
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind");
    let port = listener.local_addr().expect("local addr").port();
    let served = tokio::spawn(async move {
        let (mut sock, peer) = listener.accept().await.expect("accept");
        assert!(peer.ip().is_loopback(), "tunnel connects from loopback");
        sock.write_all(b"greeting from loopback")
            .await
            .expect("write");
        sock.shutdown().await.expect("shutdown");
    });
    let mut ws = connect_tunnel(srv.port, srv.cfg.clone()).await;
    send_frame(&mut ws, Frame::Open { stream_id: 3, port }).await;
    assert_eq!(recv_frame(&mut ws).await, Frame::OpenOk { stream_id: 3 });
    let mut received = Vec::new();
    loop {
        match recv_frame(&mut ws).await {
            Frame::Data {
                stream_id: 3,
                payload,
            } => received.extend_from_slice(&payload),
            Frame::Eof { stream_id: 3 } => break,
            other => panic!("expected DATA/EOF, got {other:?}"),
        }
    }
    assert_eq!(received, b"greeting from loopback");
    served.await.expect("listener task");
    ws.close(None).await.expect("close ws");
    srv.ws.stop().await;
}

/// An idle stream (no data either way) is torn down with a final `CLOSE`
/// after `TunnelLimits::idle_timeout`, and the connection stays usable.
#[tokio::test]
async fn tunnel_idle_stream_times_out_with_close() {
    let srv = start_with(TunnelLimits {
        idle_timeout: Duration::from_millis(200),
        ..TunnelLimits::default()
    })
    .await;
    let echo_port = spawn_echo_listener().await;
    let mut ws = connect_tunnel(srv.port, srv.cfg.clone()).await;

    send_frame(
        &mut ws,
        Frame::Open {
            stream_id: 1,
            port: echo_port,
        },
    )
    .await;
    assert_eq!(recv_frame(&mut ws).await, Frame::OpenOk { stream_id: 1 });
    // No data in either direction: the idle timer fires and the daemon closes.
    assert_eq!(recv_frame(&mut ws).await, Frame::Close { stream_id: 1 });

    // The freed id is reusable on the same connection.
    send_frame(
        &mut ws,
        Frame::Open {
            stream_id: 1,
            port: echo_port,
        },
    )
    .await;
    assert_eq!(recv_frame(&mut ws).await, Frame::OpenOk { stream_id: 1 });
    ws.close(None).await.expect("close ws");
    srv.ws.stop().await;
}

/// The daemon-side TCP connect deadline answers `OPEN_ERR` naming the
/// timeout. A firewalled/blackholed port is simulated with a bound listener
/// whose backlog is exhausted; if the connect happens to be accepted by the
/// kernel anyway the test is skipped rather than flaking.
#[tokio::test]
async fn tunnel_connect_timeout_answers_open_err() {
    let srv = start_with(TunnelLimits {
        connect_timeout: Duration::from_millis(150),
        ..TunnelLimits::default()
    })
    .await;
    // Fill a listener's accept backlog so further connects hang in SYN.
    let listener = std::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let mut parked = Vec::new();
    for _ in 0..1024 {
        match std::net::TcpStream::connect_timeout(
            &(Ipv4Addr::LOCALHOST, port).into(),
            Duration::from_millis(50),
        ) {
            Ok(sock) => parked.push(sock),
            Err(_) => break,
        }
    }
    let mut ws = connect_tunnel(srv.port, srv.cfg.clone()).await;
    send_frame(&mut ws, Frame::Open { stream_id: 1, port }).await;
    match recv_frame(&mut ws).await {
        Frame::OpenErr { stream_id, message } => {
            assert_eq!(stream_id, 1);
            assert!(
                message.contains("timed out"),
                "OPEN_ERR names the timeout: {message}"
            );
        }
        // Kernel backlog behavior varies; an accepted connect is not a
        // failure of the timeout path, just an environment we can't shape.
        Frame::OpenOk { stream_id: 1 } => {}
        other => panic!("expected OPEN_ERR/OPEN_OK, got {other:?}"),
    }
    drop(parked);
    ws.close(None).await.expect("close ws");
    srv.ws.stop().await;
}

/// A `DATA` message over the 1 MiB inbound cap closes the connection with
/// `1009 Message Too Big`, and an over-limit single frame still terminates
/// the connection.
#[tokio::test]
async fn tunnel_oversize_data_closes_with_1009() {
    use tokio_tungstenite::tungstenite::protocol::frame::coding::{Data, OpCode};
    use tokio_tungstenite::tungstenite::protocol::frame::Frame as WsFrame;

    let srv = start().await;
    let echo_port = spawn_echo_listener().await;

    // Over-limit fragmented `DATA` on a live stream: the first fragment sits
    // exactly at the cap (legal on its own), the continuation pushes the
    // accumulated size past it, surfacing tungstenite's message-capacity
    // error only after the client has finished writing — so the 1009 close
    // frame the server sends is reliably delivered even under parallel suite
    // load: no bytes are left in flight to reset the socket (monorepo#3469,
    // mirroring the `/ws` oversize coverage in `wss_integration`).
    let mut ws = connect_tunnel(srv.port, srv.cfg.clone()).await;
    send_frame(
        &mut ws,
        Frame::Open {
            stream_id: 1,
            port: echo_port,
        },
    )
    .await;
    assert_eq!(recv_frame(&mut ws).await, Frame::OpenOk { stream_id: 1 });
    let header_len = Frame::Data {
        stream_id: 1,
        payload: Vec::new(),
    }
    .encode()
    .len();
    let at_cap = Frame::Data {
        stream_id: 1,
        payload: vec![0u8; MAX_TUNNEL_MESSAGE_BYTES - header_len],
    }
    .encode();
    assert_eq!(at_cap.len(), MAX_TUNNEL_MESSAGE_BYTES);
    ws.send(Message::Frame(WsFrame::message(
        at_cap,
        OpCode::Data(Data::Binary),
        false,
    )))
    .await
    .expect("send first fragment");
    ws.send(Message::Frame(WsFrame::message(
        vec![0u8; 1024],
        OpCode::Data(Data::Continue),
        true,
    )))
    .await
    .expect("send continuation");
    loop {
        let next = tokio::time::timeout(common::test_timeout(Duration::from_secs(10)), ws.next())
            .await
            .expect("timed out waiting for close frame");
        match next {
            Some(Ok(Message::Close(Some(frame)))) => {
                assert_eq!(u16::from(frame.code), 1009, "close frame: {frame:?}");
                break;
            }
            Some(Ok(Message::Ping(p))) => {
                let _ = ws.send(Message::Pong(p)).await;
            }
            other => panic!("expected 1009 close, got {other:?}"),
        }
    }

    // Over-limit single frame: rejected fast on the frame header, without
    // buffering the payload — so the teardown can reset the socket while the
    // client is still mid-write, and the 1009 close frame may be lost to the
    // reset. Only termination is asserted (the close code is still checked
    // opportunistically when a close frame does arrive).
    let mut ws = connect_tunnel(srv.port, srv.cfg.clone()).await;
    let _ = ws
        .send(Message::Binary(
            Frame::Data {
                stream_id: 1,
                payload: vec![0u8; MAX_TUNNEL_MESSAGE_BYTES + 1],
            }
            .encode()
            .into(),
        ))
        .await;
    let closed = tokio::time::timeout(common::test_timeout(Duration::from_secs(10)), async {
        loop {
            match ws.next().await {
                None | Some(Err(_)) => break,
                Some(Ok(Message::Close(frame))) => {
                    if let Some(frame) = frame {
                        assert_eq!(u16::from(frame.code), 1009, "close frame: {frame:?}");
                    }
                    break;
                }
                Some(Ok(_)) => {}
            }
        }
    })
    .await;
    assert!(
        closed.is_ok(),
        "oversized frame must terminate the connection"
    );
    srv.ws.stop().await;
}

/// `DATA` after the client's own `EOF` is dropped: the write side stays shut,
/// the read side keeps relaying, and the stream ends cleanly.
#[tokio::test]
async fn tunnel_data_after_eof_is_dropped() {
    let srv = start().await;
    // Serve a fixed greeting AFTER seeing EOF from the daemon side, proving
    // the read relay outlives the client's half-close.
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .await
        .expect("bind");
    let port = listener.local_addr().expect("addr").port();
    let served = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.expect("accept");
        let mut buf = Vec::new();
        sock.read_to_end(&mut buf).await.expect("read to EOF");
        assert_eq!(buf, b"before eof", "post-EOF DATA must not arrive");
        sock.write_all(b"reply after eof").await.expect("write");
        sock.shutdown().await.expect("shutdown");
    });
    let mut ws = connect_tunnel(srv.port, srv.cfg.clone()).await;
    send_frame(&mut ws, Frame::Open { stream_id: 1, port }).await;
    assert_eq!(recv_frame(&mut ws).await, Frame::OpenOk { stream_id: 1 });
    send_frame(
        &mut ws,
        Frame::Data {
            stream_id: 1,
            payload: b"before eof".to_vec(),
        },
    )
    .await;
    send_frame(&mut ws, Frame::Eof { stream_id: 1 }).await;
    // Client error: DATA after its own EOF. The daemon drops it.
    send_frame(
        &mut ws,
        Frame::Data {
            stream_id: 1,
            payload: b"after eof".to_vec(),
        },
    )
    .await;
    let mut received = Vec::new();
    loop {
        match recv_frame(&mut ws).await {
            Frame::Data {
                stream_id: 1,
                payload,
            } => received.extend_from_slice(&payload),
            Frame::Eof { stream_id: 1 } => break,
            other => panic!("expected DATA/EOF, got {other:?}"),
        }
    }
    assert_eq!(received, b"reply after eof");
    assert_eq!(recv_frame(&mut ws).await, Frame::Close { stream_id: 1 });
    served.await.expect("listener task");
    ws.close(None).await.expect("close ws");
    srv.ws.stop().await;
}

/// `/tunnel` connections live in the same client registry as `/ws`: the
/// `client_count()` backing the `/health` count includes them, and `stop()`
/// tears them down.
#[tokio::test]
async fn tunnel_connections_counted_in_health_and_stopped() {
    let srv = start().await;
    assert_eq!(srv.ws.client_count(), 0);
    let mut ws = connect_tunnel(srv.port, srv.cfg.clone()).await;
    // Registration happens server-side just after the 101 is written; poll
    // briefly instead of racing it.
    let deadline = Instant::now() + common::test_timeout(Duration::from_secs(10));
    while srv.ws.client_count() != 1 {
        assert!(Instant::now() < deadline, "tunnel client never registered");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(srv.ws.client_count(), 1);

    // stop() sends the shutdown close to tunnel connections too.
    srv.ws.stop().await;
    let deadline = tokio::time::timeout(common::test_timeout(Duration::from_secs(10)), async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Close(Some(frame)))) => {
                    assert_eq!(u16::from(frame.code), 1001, "close frame: {frame:?}");
                    break;
                }
                Some(Ok(_)) => {}
                None | Some(Err(_)) => break,
            }
        }
    })
    .await;
    deadline.expect("tunnel connection closed by stop()");
    assert_eq!(srv.ws.client_count(), 0);
}
