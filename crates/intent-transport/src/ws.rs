//! HTTPS + WebSocket listener (§5.2).
//!
//! Ports `src/main/websocket-api-server.ts`: a TLS listener on
//! `<bindAddress>:<port>` (default `0.0.0.0:5180`) serving a WebSocket endpoint
//! at `/ws` and a plain `GET /health` → `{ "status":"ok", "clients":<n> }`.
//! Bearer auth + the origin allow-list are enforced during the HTTP upgrade
//! (401 bad token / 403 disabled or bad origin, socket destroyed). The accepted
//! WebSocket reuses the SAME JSON-RPC router + event bus as the UDS listener
//! (via [`crate::conn`]), so the wire result is transport-identical. Lifecycle
//! hardening (single-flight start/stop, port backoff, graceful shutdown) lives
//! in [`crate::lifecycle`].

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use intent_core::{Error, Result, WorkspaceApi};
use intent_services::EventBus;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot};
use tokio::task::AbortHandle;
use tokio_rustls::server::TlsStream;
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Message, Role};
use tokio_tungstenite::WebSocketStream;

use crate::auth::{extract_token, is_allowed_origin, validate_token, TokenStore};
use crate::conn::{self, ConnSubs, OUTBOUND_CAPACITY};
use crate::forward::ForwardRegistry;
use crate::lifecycle::{StartState, DEFAULT_PORT, HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT};
use crate::reverse::ReverseChannel;
use crate::tls::TlsCertificate;

/// Maximum bytes accepted for an HTTP request head before `\r\n\r\n`.
const MAX_HEAD_BYTES: usize = 16 * 1024;

/// Tuning for a [`WsApiServer`]. [`Default`] mirrors the production posture:
/// bind `0.0.0.0:5180`, WS API enabled, bearer auth on (TCP), 30s/60s heartbeat.
#[derive(Debug, Clone)]
pub struct WsOptions {
    pub bind_address: IpAddr,
    pub base_port: u16,
    pub enabled: bool,
    pub auth_enabled: bool,
    /// Advertise the bound port + fingerprint over mDNS (§5.4). Default off.
    pub discovery_enabled: bool,
    /// Force the connection locality (§5.14) regardless of transport:
    /// `Some(true)` = local (`--mode local`/`server.locality=local`),
    /// `Some(false)` = remote, `None` = infer from transport (TCP/WSS ⇒ remote).
    pub locality_override: Option<bool>,
    pub heartbeat_interval: Duration,
    pub heartbeat_timeout: Duration,
}

impl Default for WsOptions {
    fn default() -> Self {
        Self {
            bind_address: IpAddr::from([0, 0, 0, 0]),
            base_port: DEFAULT_PORT,
            enabled: true,
            auth_enabled: true,
            discovery_enabled: false,
            locality_override: None,
            heartbeat_interval: HEARTBEAT_INTERVAL,
            heartbeat_timeout: HEARTBEAT_TIMEOUT,
        }
    }
}

/// A control command pushed to a connection's loop by the heartbeat / shutdown.
pub(crate) enum ConnCmd {
    /// Send a WebSocket ping frame (heartbeat).
    Ping,
    /// Send a `1001 Server shutting down` close and end the connection.
    Close,
}

/// Registry record for one live WebSocket client.
pub(crate) struct ClientHandle {
    pub cmd_tx: mpsc::Sender<ConnCmd>,
    pub last_pong: Arc<AtomicI64>,
    pub abort: AbortHandle,
}

/// Shared listener state (the TS server instance). Lifecycle methods are in
/// [`crate::lifecycle`]; transport mechanics are below.
pub(crate) struct WsInner {
    pub api: Arc<dyn WorkspaceApi>,
    pub bus: EventBus,
    pub acceptor: TlsAcceptor,
    pub token_store: Arc<dyn TokenStore>,
    pub enabled: bool,
    pub auth_enabled: bool,
    pub discovery_enabled: bool,
    /// Resolved connection locality for this listener (§5.14): `true` = local,
    /// `false` = remote. TCP/WSS defaults to remote unless forced via
    /// `WsOptions::locality_override`.
    pub locality_is_local: bool,
    pub bind_address: IpAddr,
    pub base_port: u16,
    pub fingerprint: String,
    pub heartbeat_interval: Duration,
    pub heartbeat_timeout: Duration,
    pub clients: Mutex<HashMap<u64, ClientHandle>>,
    pub next_client_id: AtomicU64,
    pub external_stop_generation: AtomicU64,
    pub state: tokio::sync::Mutex<StartState>,
}

/// The HTTPS+WSS listener. Cheap to clone (`Arc` inside); `start()`/`stop()` are
/// single-flight and idempotent.
#[derive(Clone)]
pub struct WsApiServer {
    inner: Arc<WsInner>,
}

impl WsApiServer {
    /// Build a listener from the shared API + event bus, the M5.1 self-signed
    /// certificate, and the M5.2 token store. Fails only if the cert/key PEM
    /// cannot be parsed into a rustls server config.
    pub fn new(
        api: Arc<dyn WorkspaceApi>,
        bus: EventBus,
        tls: &TlsCertificate,
        token_store: Arc<dyn TokenStore>,
        options: WsOptions,
    ) -> Result<Self> {
        let acceptor = build_acceptor(tls)?;
        let inner = WsInner {
            api,
            bus,
            acceptor,
            token_store,
            enabled: options.enabled,
            auth_enabled: options.auth_enabled,
            discovery_enabled: options.discovery_enabled,
            // The WSS transport is remote by default; an override forces it
            // local/remote (§5.14).
            locality_is_local: crate::host::resolve_is_local(false, options.locality_override),
            bind_address: options.bind_address,
            base_port: options.base_port,
            fingerprint: tls.fingerprint256.clone(),
            heartbeat_interval: options.heartbeat_interval,
            heartbeat_timeout: options.heartbeat_timeout,
            clients: Mutex::new(HashMap::new()),
            next_client_id: AtomicU64::new(0),
            external_stop_generation: AtomicU64::new(0),
            state: tokio::sync::Mutex::new(StartState::default()),
        };
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// Start the listener, returning the bound port (single-flight).
    pub async fn start(&self) -> std::io::Result<u16> {
        self.inner.start().await
    }

    /// Gracefully stop the listener (idempotent).
    pub async fn stop(&self) {
        self.inner.stop().await
    }

    /// The bound port, or `None` when not currently running.
    pub async fn bound_port(&self) -> Option<u16> {
        self.inner.state.lock().await.port
    }

    /// The number of currently-connected WebSocket clients (the `/health` count).
    pub fn client_count(&self) -> usize {
        self.inner
            .clients
            .lock()
            .expect("ws clients poisoned")
            .len()
    }

    /// The pinned SHA-256 certificate fingerprint (colon-separated hex).
    pub fn fingerprint(&self) -> &str {
        &self.inner.fingerprint
    }
}

impl WsInner {
    /// Accept TLS connections until `shutdown` fires. A failed accept is logged,
    /// never fatal (post-bind durable error handler). Dropping the listener on
    /// exit frees the port before `stop()` returns.
    pub(crate) async fn accept_loop(
        self: Arc<Self>,
        listener: TcpListener,
        mut shutdown: oneshot::Receiver<()>,
    ) {
        loop {
            tokio::select! {
                _ = &mut shutdown => break,
                accepted = listener.accept() => match accepted {
                    Ok((tcp, _peer)) => {
                        let me = self.clone();
                        tokio::spawn(async move {
                            if let Err(e) = me.handle_tls_conn(tcp).await {
                                tracing::debug!(error = %e, "ws connection setup failed");
                            }
                        });
                    }
                    Err(e) => tracing::warn!(error = %e, "ws accept failed"),
                }
            }
        }
        tracing::info!("intentd WSS listener stopped");
    }

    /// Ping every client each interval; terminate any that has not ponged within
    /// the timeout, cleaning up its subscriptions (port of `startHeartbeat`).
    pub(crate) async fn heartbeat_loop(self: Arc<Self>) {
        let mut tick = tokio::time::interval(self.heartbeat_interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let timeout_ms = self.heartbeat_timeout.as_millis() as i64;
        loop {
            tick.tick().await;
            let now = now_ms();
            let snapshot: Vec<(u64, i64, mpsc::Sender<ConnCmd>, AbortHandle)> = {
                let map = self.clients.lock().expect("ws clients poisoned");
                map.iter()
                    .map(|(id, h)| {
                        (
                            *id,
                            h.last_pong.load(Ordering::Relaxed),
                            h.cmd_tx.clone(),
                            h.abort.clone(),
                        )
                    })
                    .collect()
            };
            for (id, last_pong, cmd_tx, abort) in snapshot {
                if now - last_pong > timeout_ms {
                    abort.abort();
                    self.deregister(id);
                    tracing::debug!(client = id, "ws client heartbeat timeout; terminated");
                } else {
                    let _ = cmd_tx.try_send(ConnCmd::Ping);
                }
            }
        }
    }

    /// Complete the TLS handshake, parse the HTTP head, and either answer
    /// `/health`, reject a bad `/ws` upgrade (401/403/404), or perform the
    /// WebSocket handshake and start the connection loop.
    async fn handle_tls_conn(self: Arc<Self>, tcp: TcpStream) -> std::io::Result<()> {
        let _ = tcp.set_nodelay(true);
        let mut tls = self.acceptor.accept(tcp).await?;
        let head = read_request_head(&mut tls).await?;
        let mut headers = [httparse::EMPTY_HEADER; 64];
        let mut req = httparse::Request::new(&mut headers);
        match req.parse(&head) {
            Ok(httparse::Status::Complete(_)) => {}
            _ => return reject(&mut tls, 400, "Bad Request").await,
        }
        let method = req.method.unwrap_or("");
        let target = req.path.unwrap_or("");
        let path = target.split('?').next().unwrap_or(target);
        let (mut origin, mut authorization, mut ws_key) = (None, None, None);
        for h in req.headers.iter() {
            if h.name.eq_ignore_ascii_case("origin") {
                origin = header_str(h.value);
            } else if h.name.eq_ignore_ascii_case("authorization") {
                authorization = header_str(h.value);
            } else if h.name.eq_ignore_ascii_case("sec-websocket-key") {
                ws_key = header_str(h.value);
            }
        }
        if method.eq_ignore_ascii_case("GET") && path == "/health" {
            return self.write_health(&mut tls).await;
        }
        if path != "/ws" {
            return reject(&mut tls, 404, "Not Found").await;
        }
        // §5.3 upgrade gate: enable flag, origin allow-list, then bearer token.
        if !self.enabled {
            return reject(&mut tls, 403, "Forbidden").await;
        }
        if !is_allowed_origin(origin.as_deref()) {
            return reject(&mut tls, 403, "Forbidden").await;
        }
        if self.auth_enabled {
            let ok = extract_token(authorization.as_deref(), target)
                .map(|t| validate_token(&*self.token_store, &t))
                .unwrap_or(false);
            if !ok {
                return reject(&mut tls, 401, "Unauthorized").await;
            }
        }
        let Some(key) = ws_key else {
            return reject(&mut tls, 400, "Bad Request").await;
        };
        let accept = derive_accept_key(key.as_bytes());
        let response = format!(
            "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
        );
        tls.write_all(response.as_bytes()).await?;
        tls.flush().await?;
        let ws = WebSocketStream::from_raw_socket(tls, Role::Server, None).await;
        self.spawn_connection(ws);
        Ok(())
    }

    /// Write the plain `GET /health` response and close.
    async fn write_health(&self, tls: &mut TlsStream<TcpStream>) -> std::io::Result<()> {
        let count = self.clients.lock().expect("ws clients poisoned").len();
        let body = format!("{{\"status\":\"ok\",\"clients\":{count}}}");
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        tls.write_all(response.as_bytes()).await?;
        tls.flush().await?;
        let _ = tls.shutdown().await;
        Ok(())
    }

    /// Register a new client and spawn its connection loop.
    fn spawn_connection(self: &Arc<Self>, ws: WebSocketStream<TlsStream<TcpStream>>) {
        let id = self.next_client_id.fetch_add(1, Ordering::Relaxed);
        let (cmd_tx, cmd_rx) = mpsc::channel::<ConnCmd>(8);
        let last_pong = Arc::new(AtomicI64::new(now_ms()));
        let handle = tokio::spawn(
            self.clone()
                .connection_loop(id, ws, cmd_rx, last_pong.clone()),
        );
        let abort = handle.abort_handle();
        self.clients.lock().expect("ws clients poisoned").insert(
            id,
            ClientHandle {
                cmd_tx,
                last_pong,
                abort,
            },
        );
    }

    /// Remove a client from the registry (idempotent).
    fn deregister(&self, id: u64) {
        self.clients
            .lock()
            .expect("ws clients poisoned")
            .remove(&id);
    }

    /// Drive one WebSocket connection: dispatch incoming text via the shared
    /// router, push outbound frames, answer pings, and honour control commands.
    /// On exit, subscriptions are dropped and the socket is closed.
    async fn connection_loop(
        self: Arc<Self>,
        id: u64,
        ws: WebSocketStream<TlsStream<TcpStream>>,
        mut cmd_rx: mpsc::Receiver<ConnCmd>,
        last_pong: Arc<AtomicI64>,
    ) {
        let (mut sink, mut stream) = ws.split();
        let (app_tx, mut app_rx) = mpsc::channel::<String>(OUTBOUND_CAPACITY);
        let mut subs = ConnSubs::default();
        let mut forwards = ForwardRegistry::default();
        let reverse = ReverseChannel::new(app_tx.clone());
        // Per-connection logical-client binding (§16): `None` until `client.hello`.
        let mut client_id: Option<intent_core::ClientId> = None;
        loop {
            tokio::select! {
                incoming = stream.next() => match incoming {
                    None | Some(Err(_)) => break,
                    Some(Ok(Message::Text(text))) => {
                        // The WSS transport does not expose the `system.*` control
                        // surface (those are served over the local UDS); pass `None`.
                        // `host.status` IS answered here, with the resolved WSS
                        // locality (remote unless overridden, §5.14).
                        if !conn::process_frame(&text, &self.api, &self.bus, &app_tx, &mut subs, &mut forwards, &reverse, None, &mut client_id, self.locality_is_local).await {
                            break;
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        if sink.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Pong(_))) => last_pong.store(now_ms(), Ordering::Relaxed),
                    Some(Ok(Message::Close(_))) => break,
                    Some(Ok(Message::Binary(_) | Message::Frame(_))) => {}
                },
                Some(frame) = app_rx.recv() => {
                    if sink.send(Message::Text(frame)).await.is_err() {
                        break;
                    }
                }
                cmd = cmd_rx.recv() => match cmd {
                    None => break,
                    Some(ConnCmd::Ping) => {
                        if sink.send(Message::Ping(Vec::new())).await.is_err() {
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
        drop(subs);
        drop(forwards);
        let _ = sink.close().await;
        self.deregister(id);
    }
}

/// Build a rustls `TlsAcceptor` from the self-signed cert/key, pinning the ring
/// crypto provider so the process never relies on an ambiguous default.
fn build_acceptor(tls: &TlsCertificate) -> Result<TlsAcceptor> {
    let certs = parse_certs(&tls.cert)?;
    let key = parse_key(&tls.key)?;
    let config = rustls::ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|e| Error::Internal(format!("tls protocol versions: {e}")))?
    .with_no_client_auth()
    .with_single_cert(certs, key)
    .map_err(|e| Error::Internal(format!("tls certificate: {e}")))?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

fn parse_certs(pem: &str) -> Result<Vec<CertificateDer<'static>>> {
    let mut reader: &[u8] = pem.as_bytes();
    let certs: std::result::Result<Vec<_>, _> = rustls_pemfile::certs(&mut reader).collect();
    let certs = certs.map_err(|e| Error::Internal(format!("parse certificate pem: {e}")))?;
    if certs.is_empty() {
        return Err(Error::Internal("no certificate found in PEM".to_string()));
    }
    Ok(certs)
}

fn parse_key(pem: &str) -> Result<PrivateKeyDer<'static>> {
    let mut reader: &[u8] = pem.as_bytes();
    rustls_pemfile::private_key(&mut reader)
        .map_err(|e| Error::Internal(format!("parse private key pem: {e}")))?
        .ok_or_else(|| Error::Internal("no private key found in PEM".to_string()))
}

/// Read an HTTP request head up to and including the terminating `\r\n\r\n`,
/// without consuming any following bytes (the client waits for `101`).
async fn read_request_head(tls: &mut TlsStream<TcpStream>) -> std::io::Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(1024);
    let mut byte = [0u8; 1];
    loop {
        let n = tls.read(&mut byte).await?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "connection closed before request head",
            ));
        }
        buf.push(byte[0]);
        if buf.ends_with(b"\r\n\r\n") {
            break;
        }
        if buf.len() > MAX_HEAD_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "HTTP request head too large",
            ));
        }
    }
    Ok(buf)
}

/// Write a bodyless HTTP error status line and destroy the socket.
async fn reject(tls: &mut TlsStream<TcpStream>, code: u16, reason: &str) -> std::io::Result<()> {
    let response =
        format!("HTTP/1.1 {code} {reason}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n");
    tls.write_all(response.as_bytes()).await?;
    tls.flush().await?;
    let _ = tls.shutdown().await;
    Ok(())
}

/// Trim a header value to a non-empty UTF-8 string, or `None`.
fn header_str(value: &[u8]) -> Option<String> {
    std::str::from_utf8(value)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Current wall-clock time in milliseconds since the Unix epoch.
fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
