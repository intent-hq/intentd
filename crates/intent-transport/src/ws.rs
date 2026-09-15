//! HTTPS + WebSocket listener (§5.2).
//!
//! Ports `src/main/websocket-api-server.ts`: a TLS listener on
//! `<bindAddress>:<port>` (default `127.0.0.1:5181`) serving a WebSocket endpoint
//! at `/ws` and a plain `GET /health` → `{ "status":"ok", "clients":<n>,
//! "guestConnections":<n> }`. Bearer auth + the origin allow-list are
//! enforced during the HTTP upgrade (401 bad token / 403 disabled or bad
//! origin / 503 guest caps spent, socket destroyed). The accepted
//! WebSocket reuses the SAME JSON-RPC router + event bus as the UDS listener
//! (via [`crate::conn`]), so the wire result is transport-identical. Lifecycle
//! hardening (single-flight start/stop, fail-fast bind, graceful shutdown)
//! lives in [`crate::lifecycle`].
//!
//! An **insecure dev mode** (constructed via [`WsApiServer::new_insecure`])
//! serves plain `ws://` with no TLS acceptor and no bearer-token enforcement;
//! it is the only path in this module that ever bypasses those checks.

use std::collections::HashMap;
use std::fmt::Write as _;
use std::net::IpAddr;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use futures_util::{SinkExt, StreamExt};
use intent_core::{Error, Result, WorkspaceApi};
use intent_services::EventBus;
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, watch, OwnedSemaphorePermit, Semaphore};
use tokio::task::{AbortHandle, JoinSet};
use tokio_rustls::TlsAcceptor;
use tokio_tungstenite::tungstenite::extensions::compression::deflate::DeflateConfig;
use tokio_tungstenite::tungstenite::extensions::{Extensions, ExtensionsConfig};
use tokio_tungstenite::tungstenite::handshake::derive_accept_key;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Message, Role, WebSocketConfig};
use tokio_tungstenite::tungstenite::Bytes;
use tokio_tungstenite::WebSocketStream;

use crate::accept_backoff::{sleep_unless_shutdown, AcceptBackoff, AcceptFailure};
use crate::auth::{
    extract_token, is_allowed_origin, validate_token, AsyncTokenStore, ResolvedCredential,
};
use crate::conn::{self, ConnSubs};
use crate::context::Caller;
use crate::forward::ForwardRegistry;
use crate::lifecycle::{StartState, DEFAULT_PORT, HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT};
use crate::reverse::{PrimaryReverseRegistry, ReverseChannel, ReverseTransport};
use crate::rpc_limit::RpcLimiter;
use crate::tls::TlsCertificate;

/// Maximum bytes accepted for an HTTP request head before `\r\n\r\n`.
const MAX_HEAD_BYTES: usize = 16 * 1024;

/// The unauthenticated invite-redemption endpoint (multiplayer w4).
pub(crate) const INVITE_PATH: &str = "/invite";

/// Concurrent `/invite` connections the listener admits; the endpoint is
/// reachable without a credential, so it must not be able to exhaust the
/// connection registry. Excess upgrades are refused with `503`. Each
/// admitted connection holds one semaphore permit for exactly as long as
/// its task lives (returned on any exit, including a heartbeat abort).
pub(crate) const MAX_INVITE_CONNECTIONS: usize = 32;

/// Concurrent `invite.redeem` requests one `/invite` connection may have in
/// flight (a well-behaved client needs two: a start and its wait). Excess
/// requests are refused with `flow-busy` immediately instead of spawning
/// work; the response queue is sized so every admitted request always has a
/// slot to answer into, so no task ever blocks on a full queue.
pub(crate) const MAX_INFLIGHT_INVITE_REQUESTS: usize = 4;

/// Inbound message cap on `/invite`: an `invite.redeem` envelope is a few
/// hundred bytes; anything larger is an anonymous peer wasting memory.
pub(crate) const MAX_INVITE_MESSAGE_BYTES: usize = 16 * 1024;

/// Upper bound on how long a revoked connection keeps draining in-flight RPC
/// responses (its own `principal.revokeSelf` result) before the policy close.
const REVOKE_FLUSH_GRACE: Duration = Duration::from_secs(5);

/// Caps on WSS connections held by guests — connections admitted on a
/// per-principal credential (`sharing.maxGuestConnections` /
/// `sharing.maxConnectionsPerGuest`; `0` = unlimited). The primary
/// credential (the legacy bearer token) is never counted. Sized at listener
/// construction, so a settings change applies on daemon restart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GuestConnectionLimits {
    /// Listener-wide cap on concurrent guest connections.
    pub max_guest_connections: u32,
    /// Cap on concurrent connections one guest principal may hold.
    pub max_connections_per_guest: u32,
}

impl Default for GuestConnectionLimits {
    fn default() -> Self {
        Self {
            max_guest_connections: intent_core::config::DEFAULT_SHARING_MAX_GUEST_CONNECTIONS,
            max_connections_per_guest:
                intent_core::config::DEFAULT_SHARING_MAX_CONNECTIONS_PER_GUEST,
        }
    }
}

/// Live guest-connection bookkeeping behind [`GuestConnectionLimits`]: the
/// listener-wide total and the per-principal counts, checked and bumped
/// under ONE lock so two racing upgrades cannot both take the last seat.
#[derive(Debug, Default)]
struct GuestCounts {
    total: usize,
    per_principal: HashMap<intent_core::PrincipalId, usize>,
}

/// Admission control for guest connections (see [`GuestConnectionLimits`]).
/// [`admit`](Self::admit) is called at the upgrade gate before the `101`;
/// the returned [`GuestAdmission`] rides with the connection task and gives
/// both seats back on drop — on a clean exit, a remote close, a panic
/// unwind, and when the heartbeat reaper aborts the task.
#[derive(Debug)]
pub(crate) struct GuestRegistry {
    limits: GuestConnectionLimits,
    counts: Mutex<GuestCounts>,
}

impl GuestRegistry {
    pub(crate) fn new(limits: GuestConnectionLimits) -> Arc<Self> {
        Arc::new(Self {
            limits,
            counts: Mutex::new(GuestCounts::default()),
        })
    }

    /// Take a listener-wide seat and a per-principal seat for `principal`,
    /// or `None` when either cap is spent (`0` = unlimited for that cap).
    pub(crate) fn admit(
        self: &Arc<Self>,
        principal: &intent_core::PrincipalId,
    ) -> Option<GuestAdmission> {
        let mut counts = self.counts.lock().expect("guest counts poisoned");
        let listener_cap = usize::try_from(self.limits.max_guest_connections).unwrap_or(usize::MAX);
        let per_guest_cap =
            usize::try_from(self.limits.max_connections_per_guest).unwrap_or(usize::MAX);
        if listener_cap != 0 && counts.total >= listener_cap {
            return None;
        }
        let held = counts.per_principal.get(principal).copied().unwrap_or(0);
        if per_guest_cap != 0 && held >= per_guest_cap {
            return None;
        }
        counts.total += 1;
        counts.per_principal.insert(principal.clone(), held + 1);
        Some(GuestAdmission {
            registry: self.clone(),
            principal: principal.clone(),
        })
    }

    /// Guest connections currently admitted (the `/health` count).
    pub(crate) fn connections(&self) -> usize {
        self.counts.lock().expect("guest counts poisoned").total
    }

    fn release(&self, principal: &intent_core::PrincipalId) {
        let mut counts = self.counts.lock().expect("guest counts poisoned");
        counts.total = counts.total.saturating_sub(1);
        if let Some(held) = counts.per_principal.get_mut(principal) {
            *held = held.saturating_sub(1);
            if *held == 0 {
                counts.per_principal.remove(principal);
            }
        }
    }
}

/// One admitted guest connection's seats; returned to the
/// [`GuestRegistry`] on drop.
#[derive(Debug)]
pub(crate) struct GuestAdmission {
    registry: Arc<GuestRegistry>,
    principal: intent_core::PrincipalId,
}

impl Drop for GuestAdmission {
    fn drop(&mut self) {
        self.registry.release(&self.principal);
    }
}

/// Tuning for a [`WsApiServer`]. [`Default`] mirrors the production posture:
/// bind `127.0.0.1:5181` (loopback; `server.bindAddress` widens it
/// deliberately), WS API enabled, bearer auth on (TCP), 30s/60s heartbeat.
///
/// The TLS + auth posture is picked by the constructor: [`WsApiServer::new`]
/// uses TLS + bearer auth; [`WsApiServer::new_insecure`] disables both.
#[derive(Debug, Clone)]
pub struct WsOptions {
    /// The bind set (`server.bindAddress`): one TCP listener per address, all
    /// on `base_port`. Binding is all-or-nothing — any failed address fails
    /// `start()`. Must be non-empty and semantically valid (no duplicates,
    /// unspecified only alone); [`intent_core::settings_file::BindAddress::resolve`]
    /// produces exactly that shape.
    pub bind_addresses: Vec<IpAddr>,
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
    /// Guest (per-principal credential) connection caps
    /// (`sharing.maxGuestConnections` / `sharing.maxConnectionsPerGuest`).
    pub guest_limits: GuestConnectionLimits,
    /// Test-only seam: when set, a closing connection's loop parks after it
    /// has left the reverse registry and before the rest of its cleanup runs,
    /// until the watched value becomes `true`. Lets a test hold that window
    /// open deterministically (e.g. to register a same-client reconnect inside
    /// it) instead of racing it. `None` (production) parks nowhere.
    pub cleanup_gate: Option<watch::Receiver<bool>>,
    /// Test-only seam: when set, the heartbeat reaper keeps pinging but does
    /// not abort a connection whose pong deadline has elapsed until the
    /// watched value becomes `true`. Lets a test observe a connection's
    /// pre-abort state deterministically instead of racing the deadline.
    /// `None` (production) reaps on the deadline.
    pub heartbeat_gate: Option<watch::Receiver<bool>>,
}

impl Default for WsOptions {
    fn default() -> Self {
        Self {
            bind_addresses: vec![IpAddr::from([127, 0, 0, 1])],
            base_port: DEFAULT_PORT,
            enabled: true,
            auth_enabled: true,
            locality_override: None,
            heartbeat_interval: HEARTBEAT_INTERVAL,
            heartbeat_timeout: HEARTBEAT_TIMEOUT,
            rpc_limiter: RpcLimiter::unlimited(),
            tunnel_limits: crate::tunnel::TunnelLimits::default(),
            guest_limits: GuestConnectionLimits::default(),
            cleanup_gate: None,
            heartbeat_gate: None,
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
    /// Last pong receipt on the monotonic clock ([`mono_ms`]), NOT wall time.
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
    pub bind_addresses: Vec<IpAddr>,
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
    /// Test-only post-deregistration gate (from [`WsOptions::cleanup_gate`]).
    pub cleanup_gate: Option<watch::Receiver<bool>>,
    /// Test-only reaper gate (from [`WsOptions::heartbeat_gate`]).
    pub heartbeat_gate: Option<watch::Receiver<bool>>,
    /// Admission permits for `/invite` connections
    /// ([`MAX_INVITE_CONNECTIONS`]); a permit is acquired before the `101`
    /// and travels with the connection task.
    pub invite_permits: Arc<Semaphore>,
    /// Listener-wide rate limit over phase-1 `invite.redeem` starts
    /// ([`crate::invite::RedeemThrottle`]); shared by every `/invite`
    /// connection so a reconnect never resets it.
    pub redeem_throttle: crate::invite::SharedRedeemThrottle,
    /// Guest connection admission ([`GuestConnectionLimits`]): seats are
    /// taken before the `101` and travel with the connection task.
    pub guests: Arc<GuestRegistry>,
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
    ///
    /// # Errors
    ///
    /// Returns an error if the cert/key PEM cannot be parsed into a rustls server config.
    pub fn new(
        api: Arc<dyn WorkspaceApi>,
        bus: EventBus,
        tls: &TlsCertificate,
        token_store: &Arc<AsyncTokenStore>,
        options: WsOptions,
        control: Option<Arc<dyn crate::control::SystemControl>>,
    ) -> Result<Self> {
        let acceptor = build_acceptor(tls)?;
        let inner = WsInner {
            api,
            bus,
            acceptor: Some(acceptor),
            token_store: Some((**token_store).clone()),
            enabled: options.enabled,
            auth_enabled: options.auth_enabled,
            // The WSS transport is remote by default; an override forces it
            // local/remote (§5.14).
            locality_is_local: crate::host::resolve_is_local(false, options.locality_override),
            bind_addresses: options.bind_addresses.clone(),
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
            cleanup_gate: options.cleanup_gate,
            heartbeat_gate: options.heartbeat_gate,
            invite_permits: Arc::new(Semaphore::new(MAX_INVITE_CONNECTIONS)),
            redeem_throttle: crate::invite::new_redeem_throttle(),
            guests: GuestRegistry::new(options.guest_limits),
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
            bind_addresses: options.bind_addresses.clone(),
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
            cleanup_gate: options.cleanup_gate,
            heartbeat_gate: options.heartbeat_gate,
            invite_permits: Arc::new(Semaphore::new(MAX_INVITE_CONNECTIONS)),
            redeem_throttle: crate::invite::new_redeem_throttle(),
            guests: GuestRegistry::new(options.guest_limits),
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
    ///
    /// # Errors
    ///
    /// Returns an error if the cert/key PEM cannot be parsed into a rustls server config.
    pub fn new_with_reverse(
        api: Arc<dyn WorkspaceApi>,
        bus: EventBus,
        tls: &TlsCertificate,
        token_store: &Arc<AsyncTokenStore>,
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
    ///
    /// # Panics
    ///
    /// Panics if the inner state is already shared (called after `start` published clones).
    pub fn install_pairing_info(
        &mut self,
        server_pairing_info: Arc<dyn crate::server::ServerPairingInfo>,
    ) {
        let inner = Arc::get_mut(&mut self.inner)
            .expect("WsApiServer inner not yet shared before install_pairing_info");
        inner.server_pairing_info = Some(server_pairing_info);
    }

    /// Start the listener, returning the bound port (single-flight).
    ///
    /// # Errors
    ///
    /// Returns the underlying I/O error if binding the listener fails.
    pub async fn start(&self) -> std::io::Result<u16> {
        self.inner.start().await
    }

    /// Gracefully stop the listener (idempotent).
    pub async fn stop(&self) {
        self.inner.stop().await;
    }

    /// The bound port, or `None` when not currently running.
    pub async fn bound_port(&self) -> Option<u16> {
        self.inner.state.lock().await.port
    }

    /// The number of currently-connected WebSocket clients (the `/health` count).
    ///
    /// # Panics
    ///
    /// Panics if the client-set mutex is poisoned (a prior panic while holding the lock).
    #[must_use]
    pub fn client_count(&self) -> usize {
        self.inner
            .clients
            .lock()
            .expect("ws clients poisoned")
            .len()
    }

    /// The pinned SHA-256 certificate fingerprint (colon-separated hex), or
    /// `None` when running in insecure dev mode without a TLS certificate.
    #[must_use]
    pub fn fingerprint(&self) -> Option<&str> {
        self.inner.fingerprint.as_deref()
    }

    /// Whether the listener is running in insecure (plain-`ws://`, no bearer
    /// auth) dev mode. Used by `system.status` so remote clients see the
    /// real TLS posture rather than a phantom fingerprint.
    #[must_use]
    pub fn is_insecure(&self) -> bool {
        self.inner.acceptor.is_none()
    }
}

impl WsInner {
    /// Accept TCP connections until `shutdown` fires. When a TLS acceptor is
    /// configured the raw TCP stream is first wrapped in TLS (production posture,
    /// `wss://`); in insecure dev mode the plain TCP stream drives the HTTP
    /// upgrade directly (`ws://`). A failed accept is logged, never fatal
    /// (post-bind durable error handler); descriptor exhaustion backs off with
    /// jitter instead of spinning (intent-hq/intent#4390). Dropping the
    /// listener on exit frees the port before `stop()` returns.
    pub(crate) async fn accept_loop(
        self: Arc<Self>,
        listener: TcpListener,
        mut shutdown: oneshot::Receiver<()>,
    ) {
        let mut backoff = AcceptBackoff::default();
        loop {
            tokio::select! {
                _ = &mut shutdown => break,
                accepted = listener.accept() => match accepted {
                    Ok((tcp, _peer)) => {
                        if let Some(failures) = backoff.on_success() {
                            tracing::info!(failures, "ws accept recovered");
                        }
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
                    Err(e) => match backoff.on_error(&e) {
                        AcceptFailure::Backoff { delay, streak, warn } => {
                            if warn {
                                tracing::warn!(
                                    error = %e,
                                    streak,
                                    delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
                                    "ws accept failed: out of descriptors, backing off"
                                );
                            }
                            if sleep_unless_shutdown(delay, &mut shutdown).await {
                                break;
                            }
                        }
                        AcceptFailure::Other => tracing::warn!(error = %e, "ws accept failed"),
                    },
                }
            }
        }
        tracing::info!("intentd WSS listener stopped");
    }

    /// Ping every client each interval; terminate any that has not ponged within
    /// the timeout, cleaning up its subscriptions (port of `startHeartbeat`).
    ///
    /// Staleness is measured on the monotonic clock ([`mono_ms`]), so time the
    /// host spends suspended (or wall-clock skew) never counts against a
    /// client's pong deadline (intent-hq/intent#3712).
    pub(crate) async fn heartbeat_loop(self: Arc<Self>) {
        let mut tick = tokio::time::interval(self.heartbeat_interval);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let timeout_ms = i64::try_from(self.heartbeat_timeout.as_millis()).unwrap_or(i64::MAX);
        loop {
            tick.tick().await;
            let now = mono_ms();
            let reap = self.heartbeat_gate.as_ref().is_none_or(|g| *g.borrow());
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
                if reap && now - last_pong > timeout_ms {
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
        if path != "/ws" && path != "/tunnel" && path != INVITE_PATH {
            return reject(&mut stream, 404, "Not Found").await;
        }
        // §5.3 upgrade gate (shared by `/ws`, `/tunnel` and `/invite`):
        // enable flag, origin allow-list, then bearer token.
        if !self.enabled {
            return reject(&mut stream, 403, "Forbidden").await;
        }
        if !is_allowed_origin(origin.as_deref()) {
            return reject(&mut stream, 403, "Forbidden").await;
        }
        // `/invite` (multiplayer w4): the ONE unauthenticated endpoint. It
        // has no bearer token by construction — the invitee holds only the
        // link — so it skips credential resolution and gets a dedicated loop
        // that serves `invite.redeem` and nothing else. Bounded: the accept
        // is refused with 503 once `MAX_INVITE_CONNECTIONS` permits are held;
        // the permit is taken atomically here, before the `101`, and rides
        // with the connection task so an aborted (heartbeat-reaped) task
        // returns it like a clean exit does.
        if path == INVITE_PATH {
            let Some(key) = ws_key else {
                return reject(&mut stream, 400, "Bad Request").await;
            };
            let Ok(permit) = self.invite_permits.clone().try_acquire_owned() else {
                return reject(&mut stream, 503, "Service Unavailable").await;
            };
            let accept = derive_accept_key(key.as_bytes());
            let response = format!(
                "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
            );
            stream.write_all(response.as_bytes()).await?;
            stream.flush().await?;
            let config = WebSocketConfig::default()
                .max_message_size(Some(MAX_INVITE_MESSAGE_BYTES))
                .max_frame_size(Some(MAX_INVITE_MESSAGE_BYTES));
            let ws = WebSocketStream::from_raw_socket(stream, Role::Server, Some(config)).await;
            self.spawn_invite_connection(ws, permit);
            return Ok(());
        }
        // The credential resolved at the gate binds the connection's caller
        // for its whole lifetime (multiplayer w1): the legacy file token is
        // the primary user, a hashed per-principal credential its principal.
        // Identity is never taken from `client.hello`. The insecure dev seat
        // (auth off) is the local user, exactly like UDS.
        let credential = if self.auth_enabled {
            // Keychain-backed token reads can stall on a locked/prompting OS
            // keychain; [`AsyncTokenStore`] offloads to the blocking pool with
            // a bounded per-call timeout + single-flight cache so a hung
            // upgrade never wedges the accept loop or delays other connections.
            let resolved = match (
                self.token_store.as_ref(),
                extract_token(authorization.as_deref(), target),
            ) {
                (Some(store), Some(t)) => validate_token(store, self.api.as_ref(), &t).await,
                _ => None,
            };
            let Some(resolved) = resolved else {
                return reject(&mut stream, 401, "Unauthorized").await;
            };
            resolved
        } else {
            ResolvedCredential::Legacy
        };
        // Port forwarding is owner-only (multiplayer w3): the `/tunnel` mux
        // reaches host loopback, so a per-principal (collaborator) credential
        // is refused at the upgrade — the fe renders "Only the workspace
        // owner can open forwarded ports" instead of a connection failure.
        if path == "/tunnel" && matches!(credential, ResolvedCredential::Principal(_)) {
            return reject(&mut stream, 403, "Forbidden").await;
        }
        // Guest connection caps: a per-principal credential takes a
        // listener-wide seat (`sharing.maxGuestConnections`) and one of its
        // own (`sharing.maxConnectionsPerGuest`) here, before the `101`;
        // both are refused with 503. The seats ride with the connection task
        // so an aborted (heartbeat-reaped) task returns them like a clean
        // exit does. The legacy token — the primary — is never counted.
        let guest = match &credential {
            ResolvedCredential::Principal(principal_id) => {
                let Some(admission) = self.guests.admit(principal_id) else {
                    return reject(&mut stream, 503, "Service Unavailable").await;
                };
                Some(admission)
            }
            ResolvedCredential::Legacy => None,
        };
        let caller = credential.into_caller(self.api.as_ref()).await;
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
            let _ = write!(response, "Sec-WebSocket-Extensions: {value}\r\n");
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
            self.spawn_connection(ws, caller, guest);
        }
        Ok(())
    }

    /// Write the plain `GET /health` response and close.
    async fn write_health<W>(&self, stream: &mut W) -> std::io::Result<()>
    where
        W: AsyncWrite + Unpin,
    {
        let count = self.clients.lock().expect("ws clients poisoned").len();
        let guests = self.guests.connections();
        let body =
            format!("{{\"status\":\"ok\",\"clients\":{count},\"guestConnections\":{guests}}}");
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        );
        stream.write_all(response.as_bytes()).await?;
        stream.flush().await?;
        let _ = stream.shutdown().await;
        Ok(())
    }

    /// Register a new client and spawn its connection loop. `caller` is the
    /// principal binding resolved at the upgrade gate, fixed for the life of
    /// the connection; `guest` is the guest-cap admission a per-principal
    /// credential took there, owned by the task's future so it is released
    /// when the loop returns *and* when the reaper aborts the task.
    fn spawn_connection<S>(
        self: &Arc<Self>,
        ws: WebSocketStream<S>,
        caller: Option<Caller>,
        guest: Option<GuestAdmission>,
    ) where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let id = self.next_client_id.fetch_add(1, Ordering::Relaxed);
        let (cmd_tx, cmd_rx) = mpsc::channel::<ConnCmd>(8);
        let last_pong = Arc::new(AtomicI64::new(mono_ms()));
        let handle = tokio::spawn({
            let this = self.clone();
            let last_pong = last_pong.clone();
            async move {
                let _guest = guest;
                this.connection_loop(id, ws, cmd_rx, last_pong, caller)
                    .await;
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
        let last_pong = Arc::new(AtomicI64::new(mono_ms()));
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

    /// Register a new `/invite` client and spawn its redemption loop. Invite
    /// connections share the registry with `/ws` clients (heartbeat reaper,
    /// `stop()` close, `/health` count) and additionally hold one
    /// [`MAX_INVITE_CONNECTIONS`] permit for their lifetime: it is owned by
    /// the task's future, so it is released when the loop returns *and* when
    /// the reaper aborts the task.
    fn spawn_invite_connection<S>(
        self: &Arc<Self>,
        ws: WebSocketStream<S>,
        permit: OwnedSemaphorePermit,
    ) where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let id = self.next_client_id.fetch_add(1, Ordering::Relaxed);
        let (cmd_tx, cmd_rx) = mpsc::channel::<ConnCmd>(8);
        let last_pong = Arc::new(AtomicI64::new(mono_ms()));
        let this = self.clone();
        let handle = tokio::spawn({
            let last_pong = last_pong.clone();
            async move {
                let _permit = permit;
                this.clone()
                    .invite_connection_loop(ws, cmd_rx, last_pong)
                    .await;
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

    /// Drive one `/invite` connection (multiplayer w4). No caller is bound
    /// and nothing but `invite.redeem` is served: every other frame that
    /// carries an id is answered `-32001`, and the `events.`/subscription
    /// fast paths, the router and the reverse channel are never reached. Each
    /// `invite.redeem` runs on its own task (phase 2 blocks for up to the
    /// device-code lifetime) so pings keep flowing and the reaper never
    /// mistakes a waiting invitee for a dead peer — but that work is bounded
    /// per connection: at most [`MAX_INFLIGHT_INVITE_REQUESTS`] tasks, each
    /// holding a pre-reserved response slot (so none ever waits to send), all
    /// owned by a [`JoinSet`] that aborts them when the connection ends.
    /// Phase-1 starts additionally pass the listener-wide
    /// [`crate::invite::RedeemThrottle`] before any store or upstream work.
    /// Frames the loop answers itself (parse errors, throttle and non-invite
    /// refusals) go straight to the sink and never contend for those slots.
    async fn invite_connection_loop<S>(
        self: Arc<Self>,
        ws: WebSocketStream<S>,
        mut cmd_rx: mpsc::Receiver<ConnCmd>,
        last_pong: Arc<AtomicI64>,
    ) where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (mut sink, mut stream) = ws.split();
        let (out_tx, mut out_rx) = mpsc::channel::<String>(MAX_INFLIGHT_INVITE_REQUESTS);
        let admission = Arc::new(Semaphore::new(MAX_INFLIGHT_INVITE_REQUESTS));
        let mut tasks: JoinSet<()> = JoinSet::new();
        loop {
            tokio::select! {
                incoming = stream.next() => match incoming {
                    Some(Ok(Message::Text(text))) => {
                        let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) else {
                            let frame = crate::events::error_frame(
                                &serde_json::Value::Null, -32700, "Parse error");
                            if sink.send(Message::Text(frame.into())).await.is_err() { break; }
                            continue;
                        };
                        match crate::invite::classify(&value) {
                            Some(req) if req.method == crate::invite::InviteMethod::Redeem => {
                                if let Err(refusal) = crate::invite::admit_redeem(
                                    &req, &self.redeem_throttle, Instant::now())
                                {
                                    if let Some(frame) = refusal {
                                        if sink.send(Message::Text(frame.into())).await.is_err() { break; }
                                    }
                                    continue;
                                }
                                let admitted = match admission.clone().try_acquire_owned() {
                                    Ok(permit) => out_tx
                                        .clone()
                                        .try_reserve_owned()
                                        .ok()
                                        .map(|slot| (permit, slot)),
                                    Err(_) => None,
                                };
                                let Some((permit, slot)) = admitted else {
                                    if let Some(frame) = crate::invite::refuse_busy(&req) {
                                        if sink.send(Message::Text(frame.into())).await.is_err() { break; }
                                    }
                                    continue;
                                };
                                let api = self.api.clone();
                                tasks.spawn(async move {
                                    let _permit = permit;
                                    if let Some(frame) = crate::invite::handle_redeem(req, &api).await {
                                        slot.send(frame);
                                    }
                                });
                            }
                            _ => {
                                if let Some(frame) = crate::invite::refuse_non_invite(&value) {
                                    if sink.send(Message::Text(frame.into())).await.is_err() { break; }
                                }
                            }
                        }
                    }
                    Some(Ok(Message::Ping(payload))) => {
                        if sink.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Some(Ok(Message::Pong(_))) => last_pong.store(mono_ms(), Ordering::Relaxed),
                    None | Some(Err(_) | Ok(Message::Close(_))) => break,
                    Some(Ok(Message::Binary(_) | Message::Frame(_))) => {}
                },
                Some(frame) = out_rx.recv() => {
                    if sink.send(Message::Text(frame.into())).await.is_err() {
                        break;
                    }
                }
                // Reap finished redeem tasks so the set never accumulates
                // results across a long-lived connection.
                Some(_) = tasks.join_next(), if !tasks.is_empty() => {}
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
        // Dropping the set aborts every redeem still in flight for this
        // peer (the reaper's task abort drops it too).
        tasks.abort_all();
        let _ = sink.close().await;
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
        caller: Option<Caller>,
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
        // A connection bound to a non-administrator principal never serves
        // reverse RPCs and is never an eligible reverse target (multiplayer
        // w3); an unbound legacy-token connection keeps the administrator
        // default (see `context::is_non_administrator_caller`).
        let reverse = ReverseChannel::new(app_tx.priority_sender())
            .with_administrator(caller.as_ref().is_none_or(Caller::is_administrator));
        // REV-2: register this connection's reverse channel with the shared
        // target registry; it becomes an eligible `browser.exec` target once
        // `client.hello` binds an identity advertising `browserExec`. The
        // guard drops when this loop exits (normal exit, remote close,
        // shutdown), on panic-unwind, AND when the heartbeat reaper aborts
        // this task — the registry announces `client:disconnected` for a
        // departed logical client on every one of those paths.
        let reverse_guard = self
            .reverse_registry
            .register(reverse.clone(), ReverseTransport::Wss);
        // Per-connection logical-client binding (§16): `None` until `client.hello`.
        let mut client_id: Option<intent_core::ClientId> = None;
        // Credential revocation (multiplayer w4): a connection bound to a
        // non-administrator principal closes the moment that principal's
        // credentials are revoked (`principal.revokeSelf`), instead of
        // lingering until its next RPC fails. Administrator and unbound
        // connections never subscribe.
        let revoked_principal = match &caller {
            Some(Caller::Wire {
                principal_id,
                is_administrator: false,
            }) => Some(principal_id.clone()),
            _ => None,
        };
        let mut revocations = revoked_principal
            .as_ref()
            .and_then(|_| self.api.subscribe_principal_revocations());
        loop {
            tokio::select! {
                revoked = recv_revocation(&mut revocations) => {
                    match revoked {
                        Some(id) if Some(&id) == revoked_principal.as_ref() => {
                            // Deliver in-flight RPC responses before the
                            // close: when the revocation is the caller's own
                            // `principal.revokeSelf`, the broadcast fires
                            // inside the handler, so its response may not be
                            // queued yet — it holds a reserved priority slot
                            // until it is. Drain until the lane is idle,
                            // bounded so a stuck handler cannot keep a revoked
                            // connection open.
                            let deadline = tokio::time::Instant::now() + REVOKE_FLUSH_GRACE;
                            while !app_tx.priority_idle() {
                                let next = tokio::time::timeout_at(deadline, app_rx.recv()).await;
                                let Ok(Some(frame)) = next else { break };
                                if frame.len() > crate::MAX_OUTBOUND_MESSAGE_BYTES {
                                    continue;
                                }
                                if sink.send(Message::Text(frame.into())).await.is_err() {
                                    break;
                                }
                            }
                            let _ = sink
                                .send(Message::Close(Some(CloseFrame {
                                    code: CloseCode::Policy,
                                    reason: "credential revoked".into(),
                                })))
                                .await;
                            break;
                        }
                        Some(_) => {}
                        None => revocations = None,
                    }
                }
                incoming = stream.next() => match incoming {
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
                        // RPCs gate on real origin, not the locality flag (§5.2), and
                        // bind the caller resolved at upgrade (multiplayer w1).
                        let frame_ok = crate::context::with_request_context(true, caller.clone(), async {
                            conn::process_frame(&text, &self.api, &self.bus, &app_tx, &mut subs, &mut forwards, &reverse, &reverse_guard, self.control.as_ref(), self.server_pairing_info.as_ref(), &mut client_id, self.locality_is_local, &self.rpc_limiter).await
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
                    Some(Ok(Message::Pong(_))) => last_pong.store(mono_ms(), Ordering::Relaxed),
                    None | Some(Ok(Message::Close(_))) => break,
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
        reverse.close();
        drop(reverse_guard);
        if let Some(mut gate) = self.cleanup_gate.clone() {
            let _ = gate.wait_for(|open| *open).await;
        }
        let _ = sink.close().await;
        self.deregister(id);
    }
}

/// Await the next principal revocation on an optional feed: `Some(id)` per
/// revoked principal (a lagged receiver skips ahead — a missed close only
/// means that connection fails on its next RPC instead), `None` once the
/// feed is closed, and pending forever when there is no feed so the
/// `select!` branch never fires for administrator/unbound connections.
async fn recv_revocation(
    rx: &mut Option<tokio::sync::broadcast::Receiver<intent_core::PrincipalId>>,
) -> Option<intent_core::PrincipalId> {
    let Some(rx) = rx.as_mut() else {
        return std::future::pending().await;
    };
    loop {
        match rx.recv().await {
            Ok(id) => return Some(id),
            Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
            Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
        }
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

/// Milliseconds elapsed on the **monotonic** clock since this process first
/// called it. Used for heartbeat pong bookkeeping instead of wall-clock time:
/// `Instant` never jumps on wall-clock skew and (on the platforms we ship —
/// `CLOCK_MONOTONIC` on Linux, `CLOCK_UPTIME_RAW` / `mach_absolute_time` on
/// Darwin) does not advance while the host is suspended, so a sleep/resume
/// cannot make every client look 60s+ stale and get reaped on the first
/// post-resume tick (intent-hq/intent#3712).
pub(crate) fn mono_ms() -> i64 {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    let epoch = *EPOCH.get_or_init(Instant::now);
    i64::try_from(epoch.elapsed().as_millis()).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::{mono_ms, negotiate_extensions, GuestConnectionLimits, GuestRegistry};
    use intent_core::PrincipalId;

    fn limits(listener: u32, per_guest: u32) -> GuestConnectionLimits {
        GuestConnectionLimits {
            max_guest_connections: listener,
            max_connections_per_guest: per_guest,
        }
    }

    /// The listener-wide cap refuses any guest once spent, whoever holds the
    /// seats, and a dropped admission gives its seat back.
    #[test]
    fn guest_registry_enforces_the_listener_wide_cap_and_releases_on_drop() {
        let registry = GuestRegistry::new(limits(2, 0));
        let (a, b) = (PrincipalId::new(), PrincipalId::new());
        let first = registry.admit(&a).expect("first seat");
        let second = registry.admit(&a).expect("second seat");
        assert_eq!(registry.connections(), 2);
        assert!(
            registry.admit(&b).is_none(),
            "listener full for a new guest"
        );
        drop(first);
        assert_eq!(registry.connections(), 1);
        let third = registry.admit(&b).expect("released seat admits again");
        assert!(registry.admit(&a).is_none());
        drop((second, third));
        assert_eq!(registry.connections(), 0);
    }

    /// The per-guest cap is independent of the listener-wide one: one guest
    /// at its own cap is refused while another guest is still admitted.
    #[test]
    fn guest_registry_enforces_the_per_guest_cap_independently() {
        let registry = GuestRegistry::new(limits(0, 1));
        let (a, b) = (PrincipalId::new(), PrincipalId::new());
        let a1 = registry.admit(&a).expect("a's seat");
        assert!(registry.admit(&a).is_none(), "a is at its cap");
        let _b1 = registry.admit(&b).expect("b is unaffected");
        drop(a1);
        let _a2 = registry.admit(&a).expect("a's released seat admits again");
        assert_eq!(registry.connections(), 2);
    }

    /// `0` means unlimited for either cap.
    #[test]
    fn guest_registry_zero_is_unlimited() {
        let registry = GuestRegistry::new(limits(0, 0));
        let a = PrincipalId::new();
        let seats: Vec<_> = (0..100)
            .map(|_| registry.admit(&a).expect("seat"))
            .collect();
        assert_eq!(registry.connections(), 100);
        drop(seats);
        assert_eq!(registry.connections(), 0);
    }

    /// Tripwire for intent-hq/intent#3712: heartbeat bookkeeping must stay on
    /// the monotonic clock. A wall-clock regression would make `mono_ms()`
    /// return epoch-scale values (~1.7e12 ms and rising); the monotonic
    /// process-relative clock stays far below that for any realistic daemon
    /// uptime (the bound below is ~31 years).
    #[test]
    fn mono_ms_is_process_relative_not_wall_clock() {
        let ms = mono_ms();
        assert!(ms >= 0, "monotonic ms must be non-negative: {ms}");
        assert!(
            ms < 1_000_000_000_000,
            "mono_ms() looks like a wall-clock epoch timestamp: {ms}"
        );
    }

    /// `mono_ms()` never goes backwards across successive calls.
    #[test]
    fn mono_ms_is_non_decreasing() {
        let a = mono_ms();
        let b = mono_ms();
        assert!(b >= a, "monotonic clock went backwards: {a} -> {b}");
    }

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
