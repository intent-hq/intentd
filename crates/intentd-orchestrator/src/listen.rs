//! The default tenant's client listeners: `intentd`'s own local socket and
//! WSS listener, with `intentd`'s auth, serving that tenant's `Services`.
//!
//! - The local socket ([`SOCKET_ENV`]) is mode 0600 and its callers are the
//!   owner, exactly as `intentd`'s.
//! - The WSS listener ([`BIND_ENV`], [`WS_PORT_ENV`]) checks the origin and
//!   the bearer credential at the upgrade, as `intentd`'s: the tenant's owner
//!   token, or a member or guest credential from the tenant's store. The
//!   owner token is kept in the tenant's secret store, so every process
//!   serving the tenant accepts the same token. The TLS certificate is this
//!   process's ([`TLS_DIR_ENV`]).
//! - [`INSECURE_ENV`] turns both TLS and the bearer check off, as
//!   `intentd serve --insecure` does: for local development only.
//!
//! Every accepted connection joins the tenant's
//! [`ClientConnections`], so the tenant's self-fence closes it; once the
//! set is closed the listeners refuse new connections before serving them
//! (WSS `503`, the socket drops them unread). Requests already running when
//! the fence fires are not cancelled: the tenant's store fence refuses
//! their writes (`NotOwner`), as for every other piece of the tenant's work.

use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use intent_app::tenant::ClientConnections;
use intent_core::WorkspaceApi;
use intent_services::{EventBus, Services};
use intent_transport::{
    AsyncTokenStore, ConnectionGuard, ConnectionHook, ConnectionSet, HostEnvironment,
    PairingSnapshot, PrimaryReverseRegistry, RpcLimiter, SecretTokenStore, ServerPairingInfo,
    WsApiServer, WsOptions,
};

/// The local socket path; `<data_dir>/orchestrator.sock` when unset.
pub const SOCKET_ENV: &str = "INTENTD_ORCHESTRATOR_SOCKET";

/// The WSS bind addresses, comma-separated; `127.0.0.1` when unset. The
/// IPv4 loopback is always bound too, as in `intentd`.
pub const BIND_ENV: &str = "INTENTD_ORCHESTRATOR_BIND";

/// The WSS port; [`DEFAULT_WS_PORT`] when unset, `0` for any free port.
pub const WS_PORT_ENV: &str = "INTENTD_ORCHESTRATOR_WS_PORT";

/// Where this process keeps its TLS certificate; the data dir when unset.
pub const TLS_DIR_ENV: &str = "INTENTD_ORCHESTRATOR_TLS_DIR";

/// `1` or `true`: plain `ws://` with no TLS and no bearer check, as
/// `intentd serve --insecure`. Local development only.
pub const INSECURE_ENV: &str = "INTENTD_ORCHESTRATOR_INSECURE";

/// The default WSS port: next to `intentd`'s 5181, so both run on one host.
pub const DEFAULT_WS_PORT: u16 = 5191;

/// How long the local socket may take to bind.
const SOCKET_BIND_BUDGET: std::time::Duration = std::time::Duration::from_secs(5);

/// Where and how the client listeners listen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ListenConfig {
    pub socket_path: PathBuf,
    pub bind_addresses: Vec<IpAddr>,
    pub ws_port: u16,
    pub tls_dir: PathBuf,
    pub insecure: bool,
}

impl ListenConfig {
    /// The configuration from the environment, read through `var`, with
    /// files under `data_dir`.
    ///
    /// # Errors
    /// When a set variable is not a valid value.
    pub fn from_env(var: impl Fn(&str) -> Option<String>, data_dir: &Path) -> Result<Self, String> {
        let set = |name: &str| var(name).filter(|value| !value.trim().is_empty());
        let socket_path = set(SOCKET_ENV).map_or_else(
            || data_dir.join("orchestrator.sock"),
            |path| PathBuf::from(path.trim()),
        );
        let bind_addresses = match set(BIND_ENV) {
            Some(raw) => parse_bind(&raw)?,
            None => vec![IpAddr::from([127, 0, 0, 1])],
        };
        let ws_port = match set(WS_PORT_ENV) {
            Some(raw) => raw
                .trim()
                .parse()
                .map_err(|_| format!("{WS_PORT_ENV}: {raw:?} is not a port"))?,
            None => DEFAULT_WS_PORT,
        };
        let tls_dir = set(TLS_DIR_ENV)
            .map_or_else(|| data_dir.to_path_buf(), |path| PathBuf::from(path.trim()));
        let insecure = match set(INSECURE_ENV).as_deref().map(str::trim) {
            None | Some("0" | "false") => false,
            Some("1" | "true") => true,
            Some(other) => return Err(format!("{INSECURE_ENV}: {other:?} is not 1/true/0/false")),
        };
        Ok(Self {
            socket_path,
            bind_addresses,
            ws_port,
            tls_dir,
            insecure,
        })
    }
}

/// `raw` ([`BIND_ENV`]): comma-separated IP addresses, validated as
/// `server.bindAddress` is, plus the IPv4 loopback unless a wildcard or the
/// loopback is already listed.
fn parse_bind(raw: &str) -> Result<Vec<IpAddr>, String> {
    let entries = raw
        .split(',')
        .map(|entry| entry.trim().to_owned())
        .collect();
    let mut addresses = intent_core::settings_file::BindAddress::Many(entries)
        .resolve()
        .map_err(|e| format!("{BIND_ENV}: {e}"))?;
    let loopback = IpAddr::from([127, 0, 0, 1]);
    if !addresses
        .iter()
        .any(|address| address.is_unspecified() || *address == loopback)
    {
        addresses.push(loopback);
    }
    Ok(addresses)
}

/// A tenant's [`ClientConnections`], as the listeners take it.
struct TenantConnections(ClientConnections);

impl ConnectionSet for TenantConnections {
    fn register(&self, close: Box<dyn FnOnce() + Send>) -> Option<ConnectionGuard> {
        let id = self.0.register(close)?;
        let connections = self.0.clone();
        Some(ConnectionGuard::new(move || connections.remove(id)))
    }

    fn is_closed(&self) -> bool {
        self.0.is_closed()
    }
}

/// `server.pairingInfo` and `server.rotateToken` on the local socket: the
/// tenant's owner token, this process's certificate and WSS port.
struct Pairing {
    tls_dir: PathBuf,
    tokens: AsyncTokenStore,
    host: HostEnvironment,
    ws: Arc<OnceLock<WsApiServer>>,
}

impl ServerPairingInfo for Pairing {
    fn pairing_snapshot(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = PairingSnapshot> + Send + '_>> {
        Box::pin(async move {
            let (port, bind_addresses) = match self.ws.get() {
                Some(ws) => (ws.bound_port().await, Some(ws.bound_addresses())),
                None => (None, None),
            };
            PairingSnapshot {
                port,
                bind_addresses,
                tc_address: None,
            }
        })
    }

    fn host_environment(&self) -> HostEnvironment {
        self.host.clone()
    }

    fn data_dir(&self) -> &Path {
        &self.tls_dir
    }

    fn token_store(&self) -> &AsyncTokenStore {
        &self.tokens
    }

    fn daemon_mode(&self) -> intent_core::settings_file::DaemonMode {
        intent_core::settings_file::DaemonMode::Orchestrator
    }
}

/// The running client listeners. They serve until [`Self::stop`] or the
/// end of the process; the tenant's fence closes their connections.
pub struct ClientListeners {
    stop: ListenersStop,
    ws_port: u16,
    socket_path: PathBuf,
    socket: tokio::task::JoinHandle<std::io::Result<()>>,
}

/// Stops both listeners; cheap to clone.
#[derive(Clone)]
pub struct ListenersStop {
    ws: WsApiServer,
    stop_socket: Arc<std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>>,
    stopped: Arc<tokio::sync::watch::Sender<bool>>,
}

impl ListenersStop {
    /// Stop accepting on both listeners and close their connections.
    /// Idempotent.
    pub async fn stop(&self) {
        self.ws.stop().await;
        let stop = self
            .stop_socket
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(stop) = stop {
            let _ = stop.send(());
        }
        self.stopped.send_replace(true);
    }

    /// Wait until [`Self::stop`] ran. Cancel-safe.
    pub async fn stopped(&self) {
        let mut stopped = self.stopped.subscribe();
        let _ = stopped.wait_for(|stopped| *stopped).await;
    }
}

impl ClientListeners {
    /// The bound WSS port.
    #[must_use]
    pub fn ws_port(&self) -> u16 {
        self.ws_port
    }

    /// The local socket path.
    #[must_use]
    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }

    /// The pinned SHA-256 certificate fingerprint; `None` when insecure.
    #[must_use]
    pub fn fingerprint(&self) -> Option<&str> {
        self.stop.ws.fingerprint()
    }

    /// A handle that stops both listeners.
    #[must_use]
    pub fn stopper(&self) -> ListenersStop {
        self.stop.clone()
    }

    /// Stop both listeners, close their connections, and wait until the
    /// local socket is gone.
    pub async fn stop(mut self) {
        self.stop.stop().await;
        let _ = (&mut self.socket).await;
    }
}

/// Start the client listeners for the tenant served by `services` (its
/// event bus `bus`), joining every connection to `connections`.
///
/// `reverse` must be the registry installed in `services` (and so in every
/// clone of it) with `Services::with_reverse_dispatch`: the listeners
/// register their desktops there, which is where agent browser actions
/// look for them.
///
/// # Errors
/// When the certificate or the owner token cannot be set up, or a listener
/// cannot bind.
pub async fn start_client_listeners(
    config: &ListenConfig,
    services: &Services,
    bus: &EventBus,
    reverse: Arc<PrimaryReverseRegistry>,
    connections: &ClientConnections,
) -> Result<ClientListeners, String> {
    let hook = ConnectionHook::new(Arc::new(TenantConnections(connections.clone())));
    // The tenant's `server.maxOutstandingRpcs` (its own config.toml), across
    // both listeners, as `intentd` caps its own.
    let limiter = RpcLimiter::new(services.settings_registry().map_or(
        intent_core::config::DEFAULT_SERVER_MAX_OUTSTANDING_RPCS,
        |registry| registry.snapshot().effective.server.max_outstanding_rpcs,
    ));
    let api_services = Arc::new(services.clone());
    if let Err(error) = api_services.initialize_repository_wire().await {
        tracing::debug!(%error, "native repository context remains unavailable");
    }
    let api: Arc<dyn WorkspaceApi> = api_services;
    let options = WsOptions {
        bind_addresses: config.bind_addresses.clone(),
        base_port: config.ws_port,
        rpc_limiter: limiter.clone(),
        ..WsOptions::default()
    };
    let ws_slot = Arc::new(OnceLock::new());
    let (mut ws, pairing) = if config.insecure {
        tracing::warn!(
            bind = ?config.bind_addresses,
            port = config.ws_port,
            "{INSECURE_ENV}: TLS and bearer-token auth are off, plain ws:// — local development only"
        );
        let ws = WsApiServer::new_insecure_with_reverse(
            api.clone(),
            bus.clone(),
            options,
            reverse.clone(),
            None,
        );
        (ws, None)
    } else {
        let tls = intent_transport::ensure_tls_certificate(&config.tls_dir)
            .map_err(|e| format!("TLS certificate in {}: {e}", config.tls_dir.display()))?;
        let tokens = Arc::new(AsyncTokenStore::new(Arc::new(SecretTokenStore::new(
            services.secret_source(),
        ))));
        intent_transport::get_or_create_token(&tokens)
            .await
            .map_err(|e| format!("owner token: {e}"))?;
        let pairing: Arc<dyn ServerPairingInfo> = Arc::new(Pairing {
            tls_dir: config.tls_dir.clone(),
            tokens: (*tokens).clone(),
            host: tokio::task::spawn_blocking(intent_transport::detect_host_environment)
                .await
                .map_err(|e| format!("host environment: {e}"))?,
            ws: ws_slot.clone(),
        });
        let mut ws = WsApiServer::new_with_reverse(
            api.clone(),
            bus.clone(),
            &tls,
            &tokens,
            options,
            reverse.clone(),
            None,
        )
        .map_err(|e| format!("WSS listener: {e}"))?;
        ws.install_pairing_info(pairing.clone());
        (ws, Some(pairing))
    };
    ws.install_connection_set(hook.clone());
    let ws_port = ws
        .start()
        .await
        .map_err(|e| format!("WSS listener on port {}: {e}", config.ws_port))?;
    let _ = ws_slot.set(ws.clone());

    let socket_path = config.socket_path.clone();
    // A stale socket file would pass the bind wait below; the listener
    // removes it before binding anyway, as `intentd` does.
    let _ = std::fs::remove_file(&socket_path);
    let (stop_socket, socket_stopped) = tokio::sync::oneshot::channel::<()>();
    let mut socket = tokio::spawn({
        let socket_path = socket_path.clone();
        let bus = bus.clone();
        async move {
            intent_transport::serve_uds_with_connections(
                api,
                bus,
                &socket_path,
                None,
                pairing,
                reverse,
                limiter,
                Some(hook),
                async {
                    let _ = socket_stopped.await;
                },
            )
            .await
        }
    });
    if let Err(error) = wait_for_socket(&socket_path, &mut socket).await {
        ws.stop().await;
        return Err(error);
    }
    tracing::info!(
        ws_port,
        socket = %socket_path.display(),
        secure = !config.insecure,
        "serving the default tenant's clients"
    );
    Ok(ClientListeners {
        stop: ListenersStop {
            ws,
            stop_socket: Arc::new(std::sync::Mutex::new(Some(stop_socket))),
            stopped: Arc::new(tokio::sync::watch::channel(false).0),
        },
        ws_port,
        socket_path,
        socket,
    })
}

/// Wait until the local socket is bound, or fail with why it is not.
async fn wait_for_socket(
    path: &Path,
    socket: &mut tokio::task::JoinHandle<std::io::Result<()>>,
) -> Result<(), String> {
    let bound = async {
        while !path.exists() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    };
    tokio::select! {
        () = bound => Ok(()),
        ended = &mut *socket => Err(match ended {
            Ok(Ok(())) => format!("local socket {} stopped at once", path.display()),
            Ok(Err(e)) => format!("local socket {}: {e}", path.display()),
            Err(e) => format!("local socket {}: {e}", path.display()),
        }),
        () = tokio::time::sleep(SOCKET_BIND_BUDGET) => {
            socket.abort();
            Err(format!("local socket {} did not bind", path.display()))
        }
    }
}

#[cfg(test)]
mod tests;
