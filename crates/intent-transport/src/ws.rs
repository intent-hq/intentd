//! HTTPS + WebSocket listener (§5.2).
//!
//! Ports `src/main/websocket-api-server.ts`: a TLS listener on
//! `<bindAddress>:<port>` (default `127.0.0.1:5181`) serving a WebSocket endpoint
//! at `/ws` and a plain `GET /health` → `{ "status":"ok", "clients":<n> }`.
//! Bearer auth + the origin allow-list are enforced during the HTTP upgrade
//! (401 bad token / 403 disabled or bad origin, socket destroyed). The accepted
//! WebSocket reuses the SAME JSON-RPC router + event bus as the UDS listener
//! (via [`crate::conn`]), so the wire result is transport-identical. Lifecycle
//! hardening (single-flight start/stop, fail-fast bind, graceful shutdown)
//! lives in [`crate::lifecycle`].
//!
//! An **insecure dev mode** (constructed via [`WsApiServer::new_insecure`])
//! serves plain `ws://` with no TLS acceptor and no bearer-token enforcement;
//! it is the only path in this module that ever bypasses those checks.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use intent_core::{Error, Result, WorkspaceApi};
use intent_services::EventBus;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot};
use tokio::task::AbortHandle;
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::tungstenite::extensions::compression::deflate::DeflateConfig;
use tokio_tungstenite::tungstenite::extensions::{Extensions, ExtensionsConfig};
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Message, Role, WebSocketConfig};
use tokio_tungstenite::tungstenite::Bytes;
use tokio_tungstenite::WebSocketStream;

use crate::auth::{extract_token, is_allowed_origin, validate_token, AsyncTokenStore};
use crate::conn::{self, ConnSubs};
use crate::forward::ForwardRegistry;
use crate::lifecycle::{StartState, DEFAULT_PORT, HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT};
use crate::reverse::{PrimaryReverseRegistry, ReverseChannel};
use crate::rpc_limit::RpcLimiter;
use crate::tls::TlsCertificate;

/// Maximum bytes accepted for an HTTP request head before `\r\n\r\n`.
const MAX_HEAD_BYTES: usize = 16 * 1024;

/// Tuning for a [`WsApiServer`]. [`Default`] mirrors the production posture:
/// bind `127.0.0.1:5181` (loopback; `server.bindAddress` widens it
/// deliberately), WS API enabled, bearer auth on (TCP), 30s/60s heartbeat.
///
/// The TLS + auth posture is picked by the constructor: [`WsApiServer::new`]
/// uses TLS + bearer auth; [`WsApiServer::new_insecure`] disables both.
#[derive(Debug, Clone)]
pub struct WsOptions {
    pub bind_address: IpAddr,
    pub base_port: u16,
    pub enabled: bool,
    pub auth_enabled: bool,
    /// Force the connection locality (§5.14) regardless of transport:
    /// `Some(true)` = local (`--mode local`/`server.locality=local`),
    /// `Some(false)` = remote, `None` = infer from transport (TCP/WSS ⇒ remote).
    pub locality_override: Option<bool>,
    pub heartbeat_interval: Duration,
    pub heartbeat_timeout: Duration,
    /// Daemon-wide cap on outstanding slow-path RPCs
    /// (`server.maxOutstandingRpcs`). The composition root builds ONE limiter
    /// and hands the same clone to every listener, so the cap spans UDS + WSS;
    /// the default is unlimited for standalone / test wiring.
    pub rpc_limiter: RpcLimiter,
    /// `/tunnel` caps and timeouts; defaults are production values, tests
    /// shrink them to exercise idle/connect/forward timeout behavior.
    pub tunnel_limits: crate::tunnel::TunnelLimits,
}

impl Default for WsOptions {
    fn default() -> Self {
        Self {
            bind_address: IpAddr::from([127, 0, 0, 1]),
            base_port: DEFAULT_PORT,
            enabled: true,
            auth_enabled: true,
            locality_override: None,
            heartbeat_interval: HEARTBEAT_INTERVAL,
            heartbeat_timeout: HEARTBEAT_TIMEOUT,
            rpc_limiter: RpcLimiter::unlimited(),
            tunnel_limits: crate::tunnel::TunnelLimits::default(),
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
    /// TLS acceptor for secure listeners; `None` puts the listener in the
    /// insecure dev-mode plain-`ws://` accept path.
    pub acceptor: Option<TlsAcceptor>,
    /// Bearer-token store consulted only when `auth_enabled` is set; `None` in
    /// insecure mode where auth is unconditionally off. Wrapped in
    /// [`AsyncTokenStore`] so keychain reads run on the blocking pool with a
    /// bounded per-call timeout + single-flight cache.
    pub token_store: Option<AsyncTokenStore>,
    pub enabled: bool,
    pub auth_enabled: bool,
    /// Resolved connection locality for this listener (§5.14): `true` = local,
    /// `false` = remote. TCP/WSS defaults to remote unless forced via
    /// `WsOptions::locality_override`.
    pub locality_is_local: bool,
    pub bind_address: IpAddr,
    pub base_port: u16,
    /// Pinned SHA-256 cert fingerprint; `None` in insecure mode (no TLS cert).
    pub fingerprint: Option<String>,
    pub heartbeat_interval: Duration,
    pub heartbeat_timeout: Duration,
    pub clients: Mutex<HashMap<u64, ClientHandle>>,
    pub next_client_id: AtomicU64,
    pub external_stop_generation: AtomicU64,
    pub state: tokio::sync::Mutex<StartState>,
    /// REV-1 first-client-sticky reverse-dispatch target set. Every accepted
    /// connection registers its per-connection [`ReverseChannel`] here so
    /// agent-initiated reverse RPCs (`browser.exec`) can be routed to the
    /// first-connected live client. Defaults to a fresh (empty) registry, so
    /// standalone / test wiring keeps the pre-REV-1 behavior (agent-initiated
    /// reverse RPCs still surface `NoClient` when the composition root did not
    /// share a registry across listeners).
    pub reverse_registry: Arc<PrimaryReverseRegistry>,
    /// Server pairing info provider for `server.pairingInfo` / `server.rotateToken`
    /// fast-path (§5.2). `None` means the methods are unavailable on this listener.
    pub server_pairing_info: Option<Arc<dyn crate::server::ServerPairingInfo>>,
    /// System control surface (§5.7) for `system.status`/`system.shutdown`. When
    /// present, fast-path `system.*` RPCs are handled inline on the connection
    /// loop. Shared with the UDS listener; `None` in test harnesses that don't
    /// wire a daemon control surface.
    pub control: Option<Arc<dyn crate::control::SystemControl>>,
    /// Daemon-wide outstanding-slow-path-RPC cap shared with the UDS listener
    /// (`server.maxOutstandingRpcs`); unlimited unless the composition root
    /// wires one through [`WsOptions::rpc_limiter`].
    pub rpc_limiter: RpcLimiter,
    /// `/tunnel` caps and timeouts (from [`WsOptions::tunnel_limits`]).
    pub tunnel_limits: crate::tunnel::TunnelLimits,
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
        token_store: Arc<AsyncTokenStore>,
        options: WsOptions,
        control: Option<Arc<dyn crate::control::SystemControl>>,
    ) -> Result<Self> {
        let acceptor = build_acceptor(tls)?;
        let inner = WsInner {
            api,
            bus,
            acceptor: Some(acceptor),
            token_store: Some((*token_store).clone()),
            enabled: options.enabled,
            auth_enabled: options.auth_enabled,
            // The WSS transport is remote by default; an override forces it
            // local/remote (§5.14).
            locality_is_local: crate::host::resolve_is_local(false, options.locality_override),
            bind_address: options.bind_address,
            base_port: options.base_port,
            fingerprint: Some(tls.fingerprint256.clone()),
            heartbeat_interval: options.heartbeat_interval,
            heartbeat_timeout: options.heartbeat_timeout,
            clients: Mutex::new(HashMap::new()),
            next_client_id: AtomicU64::new(0),
            external_stop_generation: AtomicU64::new(0),
            state: tokio::sync::Mutex::new(StartState::default()),
            reverse_registry: Arc::new(PrimaryReverseRegistry::new()),
            server_pairing_info: None,
            control,
            rpc_limiter: options.rpc_limiter,
            tunnel_limits: options.tunnel_limits,
        };
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// Build an **insecure** listener that serves plain `ws://` with no TLS and
    /// no bearer-token enforcement. Intended for the local dev seat (`make
    /// run-intentd` / `intentd serve --insecure`), never for production.
    /// `WsOptions::auth_enabled` is ignored.
    pub fn new_insecure(
        api: Arc<dyn WorkspaceApi>,
        bus: EventBus,
        options: WsOptions,
        control: Option<Arc<dyn crate::control::SystemControl>>,
    ) -> Self {
        let inner = WsInner {
            api,
            bus,
            acceptor: None,
            token_store: None,
            enabled: options.enabled,
            auth_enabled: false,
            locality_is_local: crate::host::resolve_is_local(false, options.locality_override),
            bind_address: options.bind_address,
            base_port: options.base_port,
            fingerprint: None,
            heartbeat_interval: options.heartbeat_interval,
            heartbeat_timeout: options.heartbeat_timeout,
            clients: Mutex::new(HashMap::new()),
            next_client_id: AtomicU64::new(0),
            external_stop_generation: AtomicU64::new(0),
            state: tokio::sync::Mutex::new(StartState::default()),
            reverse_registry: Arc::new(PrimaryReverseRegistry::new()),
            server_pairing_info: None,
            control,
            rpc_limiter: options.rpc_limiter,
            tunnel_limits: options.tunnel_limits,
        };
        Self {
            inner: Arc::new(inner),
        }
    }

    /// [`new`](Self::new) variant that shares an existing REV-1 primary
    /// reverse-dispatch registry across the UDS and WSS listeners of the same
    /// daemon. Every accepted connection registers with `reverse_registry` so
    /// agent-initiated reverse RPCs (`browser.exec`, PROTOCOL §5.14/§12.4)
    /// see the union of both listeners' clients.
    pub fn new_with_reverse(
        api: Arc<dyn WorkspaceApi>,
        bus: EventBus,
        tls: &TlsCertificate,
        token_store: Arc<AsyncTokenStore>,
        options: WsOptions,
        reverse_registry: Arc<PrimaryReverseRegistry>,
        control: Option<Arc<dyn crate::control::SystemControl>>,
    ) -> Result<Self> {
        let mut server = Self::new(api, bus, tls, token_store, options, control)?;
        Self::install_registry(&mut server, reverse_registry);
        Ok(server)
    }

    /// [`new_insecure`](Self::new_insecure) variant sharing the REV-1
    /// primary reverse-dispatch registry (see [`new_with_reverse`](Self::new_with_reverse)).
    pub fn new_insecure_with_reverse(
        api: Arc<dyn WorkspaceApi>,
        bus: EventBus,
        options: WsOptions,
        reverse_registry: Arc<PrimaryReverseRegistry>,
        control: Option<Arc<dyn crate::control::SystemControl>>,
    ) -> Self {
        let mut server = Self::new_insecure(api, bus, options, control);
        Self::install_registry(&mut server, reverse_registry);
        server
    }

    /// Swap the reverse-dispatch registry on the inner state. The `WsInner`
    /// carries interior-mutable state (mutexes, atomics), so it cannot be
    /// cloned via `Arc::make_mut`; instead we borrow it exclusively with
    /// `Arc::get_mut` — safe because the builder chain owns the sole strong
    /// reference before [`start`](Self::start) publishes clones. Called only
    /// from the two `*_with_reverse` constructors.
    fn install_registry(server: &mut Self, reverse_registry: Arc<PrimaryReverseRegistry>) {
        let inner = Arc::get_mut(&mut server.inner)
            .expect("WsApiServer inner not yet shared before install_registry");
        inner.reverse_registry = reverse_registry;
    }

    /// Install server pairing info provider on the inner state. Uses the same
    /// `Arc::get_mut` pattern as `install_registry`. Called from composition root.
    pub fn install_pairing_info(
        &mut self,
        server_pairing_info: Arc<dyn crate::server::ServerPairingInfo>,
    ) {
        let inner = Arc::get_mut(&mut self.inner)
            .expect("WsApiServer inner not yet shared before install_pairing_info");
        inner.server_pairing_info = Some(server_pairing_info);
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

    /// The pinned SHA-256 certificate fingerprint (colon-separated hex), or
    /// `None` when running in insecure dev mode without a TLS certificate.
    pub fn fingerprint(&self) -> Option<&str> {
        self.inner.fingerprint.as_deref()
    }

    /// Whether the listener is running in insecure (plain-`ws://`, no bearer
    /// auth) dev mode. Used by `system.status` so remote clients see the
    /// real TLS posture rather than a phantom fingerprint.
    pub fn is_insecure(&self) -> bool {
        self.inner.acceptor.is_none()
    }
}

impl WsInner {
    /// Accept TCP connections until `shutdown` fires. When a TLS acceptor is
    /// configured the raw TCP stream is first wrapped in TLS (production posture,
    /// `wss://`); in insecure dev mode the plain TCP stream drives the HTTP
    /// upgrade directly (`ws://`). A failed accept is logged, never fatal
    /// (post-bind durable error handler). Dropping the listener on exit frees
    /// the port before `stop()` returns.
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
                            let _ = tcp.set_nodelay(true);
                            let result = match me.acceptor.clone() {
                                Some(acceptor) => match acceptor.accept(tcp).await {
                                    Ok(tls) => me.handle_conn(tls).await,
                                    Err(e) => Err(e),
                                },
                                None => me.handle_conn(tcp).await,
                            };
                            if let Err(e) = result {
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

    /// Parse the HTTP head from an established stream (TLS or plain TCP), and
    /// either answer `/health`, reject a bad `/ws` upgrade (401/403/404), or
    /// perform the WebSocket handshake and start the connection loop.
    async fn handle_conn<S>(self: Arc<Self>, mut stream: S) -> std::io::Result<()>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let head = read_request_head(&mut stream).await?;
        let mut headers = [httparse::EMPTY_HEADER; 64];
        let mut req = httparse::Request::new(&mut headers);
        match req.parse(&head) {
            Ok(httparse::Status::Complete(_)) => {}
            _ => return reject(&mut stream, 400, "Bad Request").await,
        }
        let method = req.method.unwrap_or("");
        let target = req.path.unwrap_or("");
        let path = target.split('?').next().unwrap_or(target);
        let (mut origin, mut authorization, mut ws_key) = (None, None, None);
        let mut ws_extensions: Vec<String> = Vec::new();
        for h in req.headers.iter() {
            if h.name.eq_ignore_ascii_case("origin") {
                origin = header_str(h.value);
            } else if h.name.eq_ignore_ascii_case("authorization") {
                authorization = header_str(h.value);
            } else if h.name.eq_ignore_ascii_case("sec-websocket-key") {
                ws_key = header_str(h.value);
            } else if h.name.eq_ignore_ascii_case("sec-websocket-extensions") {
                // A client may spread its extension offers over multiple
                // header lines (RFC 9110 §5.3); collect them all, in order.
                if let Some(v) = header_str(h.value) {
                    ws_extensions.push(v);
                }
            }
        }
        if method.eq_ignore_ascii_case("GET") && path == "/health" {
            return self.write_health(&mut stream).await;
        }
        if path != "/ws" && path != "/tunnel" {
            return reject(&mut stream, 404, "Not Found").await;
        }
        // §5.3 upgrade gate (shared by `/ws` and `/tunnel`): enable flag,
        // origin allow-list, then bearer token.
        if !self.enabled {
            return reject(&mut stream, 403, "Forbidden").await;
        }
        if !is_allowed_origin(origin.as_deref()) {
            return reject(&mut stream, 403, "Forbidden").await;
        }
        if self.auth_enabled {
            // Keychain-backed token reads can stall on a locked/prompting OS
            // keychain; [`AsyncTokenStore`] offloads to the blocking pool with
            // a bounded per-call timeout + single-flight cache so a hung
            // upgrade never wedges the accept loop or delays other connections.
            let ok = match (
                self.token_store.as_ref(),
                extract_token(authorization.as_deref(), target),
            ) {
                (Some(store), Some(t)) => validate_token(store, &t).await,
                _ => false,
            };
            if !ok {
                return reject(&mut stream, 401, "Unauthorized").await;
            }
        }
        let Some(key) = ws_key else {
            return reject(&mut stream, 400, "Bad Request").await;
        };
        let accept = derive_accept_key(key.as_bytes());
        // RFC 7692 permessage-deflate: negotiate the client's
        // `Sec-WebSocket-Extensions` offer(s). When an offer is accepted the
        // agreed parameters are echoed in the 101 response and the socket is
        // built with the negotiated compression context; when the client
        // offers nothing (or nothing acceptable) no header is emitted and the
        // connection is a plain uncompressed WebSocket, exactly as before.
        let (extensions, extensions_header) = negotiate_extensions(&ws_extensions);
        let mut response = format!(
            "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {accept}\r\n"
        );
        if let Some(value) = &extensions_header {
            response.push_str(&format!("Sec-WebSocket-Extensions: {value}\r\n"));
        }
        response.push_str("\r\n");
        stream.write_all(response.as_bytes()).await?;
        stream.flush().await?;
        // Explicit inbound size limits (monorepo#472): cap a whole message at
        // the shared transport limit, and raise `max_frame_size` (tungstenite
        // default 16 MiB) to the same value so a legitimate large payload sent
        // as a single unfragmented frame is still accepted. Over-limit frames
        // fail fast on the frame header, without buffering the payload.
        // `/tunnel` gets a much smaller cap: the 40 MiB limit is sized for
        // JSON-RPC envelopes, while tunnel frames are bounded per-`DATA` so
        // the frame-count relay queues cannot buffer GiBs of payload.
        let max_message = if path == "/tunnel" {
            crate::tunnel::MAX_TUNNEL_MESSAGE_BYTES
        } else {
            crate::MAX_INBOUND_MESSAGE_BYTES
        };
        let config = WebSocketConfig::default()
            .max_message_size(Some(max_message))
            .max_frame_size(Some(max_message));
        let ws = if extensions_header.is_some() {
            WebSocketStream::from_raw_socket_with_extensions(
                stream,
                Role::Server,
                Some(config),
                extensions,
            )
            .await
        } else {
            WebSocketStream::from_raw_socket(stream, Role::Server, Some(config)).await
        };
        if path == "/tunnel" {
            self.spawn_tunnel_connection(ws);
        } else {
            self.spawn_connection(ws);
        }
        Ok(())
    }

    /// Write the plain `GET /health` response and close.
    async fn write_health<W>(&self, stream: &mut W) -> std::io::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        let count = self.clients.lock().expect("ws clients poisoned").len();
        let body = format!("{{\"status\":\"ok\",\"clients\":{count}}}");
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).await?;
        stream.flush().await?;
        let _ = stream.shutdown().await;
        Ok(())
    }

    /// Register a new client and spawn its connection loop.
    fn spawn_connection<S>(self: &Arc<Self>, ws: WebSocketStream<S>)
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
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

    /// Register a new `/tunnel` client and spawn its mux loop. Tunnel
    /// connections live in the same registry as `/ws` clients, so the
    /// heartbeat reaper, `stop()` shutdown close, and the `/health` count all
    /// cover them identically.
    fn spawn_tunnel_connection<S>(self: &Arc<Self>, ws: WebSocketStream<S>)
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let id = self.next_client_id.fetch_add(1, Ordering::Relaxed);
        let (cmd_tx, cmd_rx) = mpsc::channel::<ConnCmd>(8);
        let last_pong = Arc::new(AtomicI64::new(now_ms()));
        let this = self.clone();
        let limits = self.tunnel_limits;
        let handle = tokio::spawn({
            let last_pong = last_pong.clone();
            async move {
                crate::tunnel::run_tunnel_connection(ws, cmd_rx, last_pong, limits).await;
                this.deregister(id);
            }
        });
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
    async fn connection_loop<S>(
        self: Arc<Self>,
        id: u64,
        ws: WebSocketStream<S>,
        mut cmd_rx: mpsc::Receiver<ConnCmd>,
        last_pong: Arc<AtomicI64>,
    ) where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (mut sink, mut stream) = ws.split();
        // Two-lane outbound queue: RPC responses on the priority lane, event/
        // subscription pushes on the bulk lane; `recv()` drains priority first
        // so responses overtake queued bulk traffic on a saturated link.
        let (app_tx, mut app_rx) = conn::outbound_channel();
        let mut subs = ConnSubs::default();
        let mut forwards = ForwardRegistry::default();
        let reverse = ReverseChannel::new(app_tx.priority_sender());
        // REV-1: register this connection's reverse channel with the shared
        // primary-target set so agent-initiated `browser.exec` calls can route
        // to whichever client connected first. Guard drops when this loop
        // returns (normal exit, remote close, heartbeat timeout, shutdown), so
        // failover is exactly the connection arrival order.
        let _reverse_guard = self.reverse_registry.register(reverse.clone());
        // Per-connection logical-client binding (§16): `None` until `client.hello`.
        let mut client_id: Option<intent_core::ClientId> = None;
        loop {
            tokio::select! {
                incoming = stream.next() => match incoming {
                    None => break,
                    Some(Err(e)) => {
                        // Over-limit inbound message or frame (monorepo#495):
                        // tell the client why with a 1009 (Message Too Big)
                        // close frame before terminating; other read errors
                        // keep the bare drop.
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
                    Some(Ok(Message::Text(text))) => {
                        // The `system.*` control surface IS wired here (the
                        // composition root shares `Some(control)` with the UDS
                        // listener); UDS-only methods (`system.shutdown`,
                        // `system.importLegacy`) reject remote callers with -32001.
                        // `host.status` IS answered here, with the resolved WSS
                        // locality (remote unless overridden, §5.14).
                        // Wrap in connection context (is_tcp=true for WSS) so server.*
                        // RPCs gate on real origin, not the locality flag (§5.2).
                        let frame_ok = crate::context::with_connection_context(true, async {
                            conn::process_frame(&text, &self.api, &self.bus, &app_tx, &mut subs, &mut forwards, &reverse, self.control.as_ref(), self.server_pairing_info.as_ref(), &mut client_id, self.locality_is_local, &self.rpc_limiter).await
                        }).await;
                        if !frame_ok {
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
                    // Last-resort backstop for non-response frames
                    // (subscription pushes/events): oversized router
                    // responses are already replaced with a `-32010` error
                    // at serialization, where the request id is known.
                    if frame.len() > crate::MAX_OUTBOUND_MESSAGE_BYTES {
                        tracing::error!(
                            frame_bytes = frame.len(),
                            limit = crate::MAX_OUTBOUND_MESSAGE_BYTES,
                            "dropping oversized outbound WSS frame"
                        );
                        continue;
                    }
                    if sink.send(Message::Text(frame.into())).await.is_err() {
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
async fn read_request_head<S>(stream: &mut S) -> std::io::Result<Vec<u8>>
where
    S: AsyncRead + Unpin,
{
    let mut buf = Vec::with_capacity(1024);
    let mut byte = [0u8; 1];
    loop {
        let n = stream.read(&mut byte).await?;
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
async fn reject<W>(stream: &mut W, code: u16, reason: &str) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let response =
        format!("HTTP/1.1 {code} {reason}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n");
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await?;
    let _ = stream.shutdown().await;
    Ok(())
}

/// The server's extension posture for the WSS listener: accept RFC 7692
/// permessage-deflate offers with the default parameter set (deflate level
/// per the flate2 default, 15-bit windows, context takeover allowed —
/// per-parameter negotiation narrows these to what the client asked for).
fn server_extensions_config() -> ExtensionsConfig {
    let mut config = ExtensionsConfig::default();
    config.permessage_deflate = Some(DeflateConfig::default());
    config
}

/// Negotiate the client's `Sec-WebSocket-Extensions` offer(s) against the
/// server posture. Returns the negotiated [`Extensions`] for the connection
/// plus the exact header value to echo in the `101` response when an offer
/// was accepted. No offers, unacceptable offers, and malformed offers all
/// decline to a clean uncompressed connection (RFC 7692 §7 requires declining
/// rather than failing the upgrade), leaving the wire behavior identical to a
/// client that never offered compression.
fn negotiate_extensions(offers: &[String]) -> (Extensions, Option<String>) {
    if offers.is_empty() {
        return (Extensions::default(), None);
    }
    match server_extensions_config().negotiate_offers(offers) {
        Ok((extensions, header)) => (extensions, header),
        Err(e) => {
            tracing::debug!(error = %e, "declining Sec-WebSocket-Extensions offer");
            (Extensions::default(), None)
        }
    }
}

/// Trim a header value to a non-empty UTF-8 string, or `None`.
fn header_str(value: &[u8]) -> Option<String> {
    std::str::from_utf8(value)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// Current wall-clock time in milliseconds since the Unix epoch.
pub(crate) fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::negotiate_extensions;

    /// No `Sec-WebSocket-Extensions` header ⇒ no response header, plain
    /// uncompressed connection (the pre-deflate behavior).
    #[test]
    fn negotiate_declines_when_client_offers_nothing() {
        let (_extensions, header) = negotiate_extensions(&[]);
        assert_eq!(header, None);
    }

    /// A standard browser offer is accepted and the response header names the
    /// agreed extension.
    #[test]
    fn negotiate_accepts_browser_deflate_offer() {
        let offers = vec!["permessage-deflate; client_max_window_bits".to_string()];
        let (_extensions, header) = negotiate_extensions(&offers);
        let header = header.expect("deflate offer accepted");
        assert!(
            header.starts_with("permessage-deflate"),
            "response names the agreed extension: {header}"
        );
    }

    /// An unknown extension is ignored: no response header, clean connection.
    #[test]
    fn negotiate_declines_unknown_extension() {
        let offers = vec!["x-unknown-extension".to_string()];
        let (_extensions, header) = negotiate_extensions(&offers);
        assert_eq!(header, None);
    }

    /// RFC 7692 §7 offers a server MUST decline (unknown parameter, invalid
    /// value, duplicate parameter) fall back to an uncompressed connection
    /// instead of failing the upgrade.
    #[test]
    fn negotiate_declines_unacceptable_deflate_offers() {
        for offer in [
            "permessage-deflate; parameter-from-the-future=3",
            "permessage-deflate; client_max_window_bits=99",
            "permessage-deflate; client_no_context_takeover; client_no_context_takeover",
        ] {
            let (_extensions, header) = negotiate_extensions(&[offer.to_string()]);
            assert_eq!(header, None, "offer must be declined: {offer}");
        }
    }

    /// Multiple header lines are negotiated in order: a declined first offer
    /// falls back to an acceptable second one.
    #[test]
    fn negotiate_accepts_fallback_offer_across_header_lines() {
        let offers = vec![
            "permessage-deflate; parameter-from-the-future=3".to_string(),
            "permessage-deflate".to_string(),
        ];
        let (_extensions, header) = negotiate_extensions(&offers);
        assert_eq!(header.as_deref(), Some("permessage-deflate"));
    }

    /// A syntactically malformed header declines cleanly rather than erroring
    /// the upgrade.
    #[test]
    fn negotiate_declines_malformed_header() {
        let offers = vec!["permessage-deflate; =".to_string()];
        let (_extensions, header) = negotiate_extensions(&offers);
        assert_eq!(header, None);
    }
}
