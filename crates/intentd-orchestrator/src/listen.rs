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
//! - Invite links name the public addresses in [`PUBLIC_HOSTS_ENV`]: the
//!   orchestrator has no tunnel, so without them no invite is minted.
//! - The Assistant's `ws.rpc` runs over an in-process connection with the
//!   local socket's semantics, as in `intentd`.
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
    SystemControl, SystemStatus, WsApiServer, WsOptions,
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

/// The public host names (or addresses) clients reach the WSS port at,
/// comma-separated: what invite links name (`host=`). None when unset.
pub const PUBLIC_HOSTS_ENV: &str = "INTENTD_ORCHESTRATOR_PUBLIC_HOSTS";

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

/// The public hosts in [`PUBLIC_HOSTS_ENV`], read through `var`; none
/// when unset.
///
/// # Errors
/// When an entry is not a host name or IP address.
pub fn public_hosts_from_env(var: impl Fn(&str) -> Option<String>) -> Result<Vec<String>, String> {
    match var(PUBLIC_HOSTS_ENV).filter(|value| !value.trim().is_empty()) {
        Some(raw) => parse_public_hosts(&raw),
        None => Ok(Vec::new()),
    }
}

/// `raw` ([`PUBLIC_HOSTS_ENV`]): comma-separated host names or IP
/// addresses, with no scheme, port or path.
fn parse_public_hosts(raw: &str) -> Result<Vec<String>, String> {
    raw.split(',')
        .map(str::trim)
        .filter(|host| !host.is_empty())
        .map(|host| {
            // IPv6 as the app takes pairing hosts: bare, no brackets.
            let host = host
                .strip_prefix('[')
                .and_then(|h| h.strip_suffix(']'))
                .filter(|h| h.parse::<std::net::Ipv6Addr>().is_ok())
                .unwrap_or(host);
            let valid = host.parse::<IpAddr>().is_ok()
                || host
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '.');
            if valid {
                Ok(host.to_owned())
            } else {
                Err(format!(
                    "{PUBLIC_HOSTS_ENV}: {host:?} is not a host name or address \
                     (no scheme, port or path)"
                ))
            }
        })
        .collect()
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
/// tenant's owner token, this process's certificate and WSS port; and the
/// envelope of the tenant's invite links.
struct Pairing {
    tls_dir: PathBuf,
    tokens: AsyncTokenStore,
    host: HostEnvironment,
    /// Weak, as [`StatusControl::ws`]: the tenant's `Services` hold this
    /// (invite links, `ws.rpc`), and the listener holds the `Services`.
    ws: Arc<OnceLock<std::sync::Weak<WsApiServer>>>,
    public_hosts: Vec<String>,
}

impl ServerPairingInfo for Pairing {
    fn pairing_snapshot(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = PairingSnapshot> + Send + '_>> {
        Box::pin(async move {
            let (port, bind_addresses) = match self.ws.get().and_then(std::sync::Weak::upgrade) {
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

    fn invite_hosts(&self) -> Vec<String> {
        self.public_hosts.clone()
    }

    fn daemon_mode(&self) -> intent_core::settings_file::DaemonMode {
        intent_core::settings_file::DaemonMode::Orchestrator
    }
}

/// `system.status` on both listeners: `intentd`'s wire shape with this
/// process's values, so the app's daemon-health poll sees a healthy
/// backend. Status only ([`SystemServices::StatusOnly`]): `system.shutdown`,
/// `system.requestUpdate` and `system.gitCredential` stay unserved, since a
/// client must not stop or update a process that serves other tenants.
///
/// [`SystemServices::StatusOnly`]: intent_transport::control::SystemServices::StatusOnly
struct StatusControl {
    /// Weak: the tenant's `Services` hold this control (`ws.rpc`).
    services: std::sync::Weak<Services>,
    host: HostEnvironment,
    /// Weak: the listener owns this control, so a strong reference would
    /// keep a stopped listener (and the tenant's `Services`) alive. The
    /// strong one is [`ListenersStop`]'s until they stop.
    ws: Arc<OnceLock<std::sync::Weak<WsApiServer>>>,
    /// The WSS port once bound; for the synchronous [`SystemControl::status`].
    port: Arc<OnceLock<u16>>,
    started: std::time::Instant,
    /// Kept fresh by [`sample_usage`], never collected on the request path.
    sample: Arc<std::sync::Mutex<UsageSample>>,
}

/// What [`sample_usage`] last read: this process's CPU (100 = one core) and
/// resident memory, the host's addresses for `localIps` and its names, and
/// the `(available, total)` bytes of the volume the tenant's workspaces are
/// on.
#[derive(Default)]
struct UsageSample {
    cpu_percent: f32,
    memory_bytes: u64,
    local_v4: Vec<String>,
    local_v6: Vec<String>,
    names: Option<(String, String)>,
    workspaces_disk: Option<(u64, u64)>,
}

/// The `(available, total)` bytes of the mounted volume holding `root`
/// (the longest mount point it is under), as `intentd`'s status reports.
fn disk_sample(root: &Path) -> Option<(u64, u64)> {
    let target = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    let disks = sysinfo::Disks::new_with_refreshed_list().ok()?;
    disks
        .iter()
        .filter(|disk| target.starts_with(disk.mount_point()))
        .max_by_key(|disk| disk.mount_point().as_os_str().len())
        .map(|disk| (disk.available_space(), disk.total_space()))
}

/// How often [`sample_usage`] reads CPU and memory: CPU is a delta between
/// two reads, so a fixed interval keeps it steady however often clients poll.
const USAGE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(1);

/// How many [`USAGE_INTERVAL`]s apart the host's addresses, names and
/// workspaces disk are re-read.
const ROUTES_EVERY: u32 = 10;

/// Keep `sample` fresh until the listeners stop (or are dropped): CPU and memory every
/// [`USAGE_INTERVAL`], the addresses every [`ROUTES_EVERY`] of them, off the
/// runtime's worker threads. Without `sysinfo` on this host, usage reads 0.
async fn sample_usage(
    sample: Arc<std::sync::Mutex<UsageSample>>,
    workspaces_root: Option<PathBuf>,
    mut stopped: tokio::sync::watch::Receiver<bool>,
) {
    let process = sysinfo::System::new()
        .ok()
        .zip(sysinfo::get_current_pid().ok());
    let process = Arc::new(std::sync::Mutex::new(process));
    let mut tick = tokio::time::interval(USAGE_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut ticks = 0_u32;
    loop {
        tokio::select! {
            _ = tick.tick() => {}
            // Stopped, or every handle to the listeners dropped.
            _ = stopped.wait_for(|stopped| *stopped) => return,
        }
        let routes = ticks.is_multiple_of(ROUTES_EVERY);
        ticks = ticks.wrapping_add(1);
        let process = process.clone();
        let workspaces_root = workspaces_root.clone();
        let read = tokio::task::spawn_blocking(move || {
            let mut process = process
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let usage = process.as_mut().and_then(|(system, pid)| {
                system.refresh_processes_specifics(
                    sysinfo::ProcessesToUpdate::Some(&[*pid]),
                    true,
                    sysinfo::ProcessRefreshKind::nothing()
                        .with_cpu()
                        .with_memory(),
                );
                system
                    .process(*pid)
                    .map(|process| (process.cpu_usage(), process.memory()))
            });
            let routes = routes.then(|| {
                (
                    intent_transport::collect_local_ips(),
                    intent_transport::collect_local_ipv6s(),
                    (
                        intent_transport::local_hostname(),
                        intent_transport::pretty_hostname(),
                    ),
                    workspaces_root.as_deref().and_then(disk_sample),
                )
            });
            (usage, routes)
        })
        .await;
        let Ok((usage, routes)) = read else { continue };
        let mut sample = sample
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((cpu, memory)) = usage {
            sample.cpu_percent = cpu;
            sample.memory_bytes = memory;
        }
        if let Some((v4, v6, names, disk)) = routes {
            sample.local_v4 = v4;
            sample.local_v6 = v6;
            sample.names = Some(names);
            sample.workspaces_disk = disk;
        }
    }
}

impl StatusControl {
    fn snapshot(&self, port: Option<u16>) -> SystemStatus {
        use intent_app::remote::RemoteServicesExt as _;
        let ws = self.ws.get().and_then(std::sync::Weak::upgrade);
        let ws = ws.as_deref();
        let tcp = port.is_some();
        // Agents run only on other machines; a local manager, if any, is
        // counted as `intentd` counts its own.
        let services = self.services.upgrade();
        let manager = services.as_ref().and_then(|s| s.agent_manager());
        let (cpu_percent, memory_bytes, local_ips, names, workspaces_disk) = {
            let sample = self
                .sample
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let local_ips = match (ws, tcp) {
                (Some(ws), true) => intent_transport::advertised_hosts(
                    Some(&ws.bound_addresses()),
                    &sample.local_v4,
                    &sample.local_v6,
                ),
                _ => Vec::new(),
            };
            (
                sample.cpu_percent,
                sample.memory_bytes,
                local_ips,
                sample.names.clone(),
                sample.workspaces_disk,
            )
        };
        let (hostname, pretty_hostname) = names.unwrap_or_else(|| {
            (
                self.host.hostname.clone(),
                self.host.pretty_hostname.clone(),
            )
        });
        SystemStatus {
            listen_mode: if tcp { "both" } else { "uds" }.to_owned(),
            uds: true,
            tcp,
            port,
            clients: ws.map_or(0, WsApiServer::client_count),
            // The cap is this host's: agents on machines use theirs.
            agents: manager.as_ref().map_or(0, |m| m.registry().local_size()),
            fingerprint: ws.and_then(|ws| ws.fingerprint().map(str::to_owned)),
            os: std::env::consts::OS.to_owned(),
            arch: std::env::consts::ARCH.to_owned(),
            // A server process: no display of its own.
            has_display: false,
            max_agents: manager.as_ref().map_or(0, |m| m.registry().cap()),
            version: self.daemon_version().to_owned(),
            build_commit: intent_transport::BUILD_COMMIT.map(str::to_owned),
            uptime_seconds: self.started.elapsed().as_secs(),
            local_ips,
            tc_address: None,
            hostname,
            pretty_hostname,
            device_kind: self.host.device_kind.clone(),
            hardware_model: self.host.hardware_model.clone(),
            cpu_percent,
            memory_bytes,
            child_processes: None,
            child_memory_bytes: None,
            child_memory_peak_bytes: None,
            agent_memory_bytes: None,
            agent_process_count: None,
            agent_memory_budget_bytes: None,
            agent_memory_charged_bytes: None,
            queued_spawns: None,
            workspaces_disk_available_bytes: workspaces_disk.map(|(available, _)| available),
            workspaces_disk_total_bytes: workspaces_disk.map(|(_, total)| total),
            file_watch: None,
            fd_count: None,
            fd_limit: intent_core::fd_limit::soft_limit(),
            // No sitter supervises the orchestrator.
            update_supported: false,
            busy_agents: manager.as_ref().map_or(0, |m| m.list_busy().len()),
            idle_update_check: intent_transport::IdleUpdateCheckStatus::default(),
            remote_links: Some(intent_transport::RemoteLinks {
                backends: 0,
                machines: services
                    .as_ref()
                    .and_then(|s| s.remote_feature())
                    .map_or(0, intent_app::remote::RemoteFeature::connected_machines),
            }),
            workload_containment: Some(intent_core::workload::status()),
        }
    }
}

impl SystemControl for StatusControl {
    fn services(&self) -> intent_transport::control::SystemServices {
        intent_transport::control::SystemServices::StatusOnly
    }

    fn status(&self) -> SystemStatus {
        self.snapshot(self.port.get().copied())
    }

    fn metadata_status(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = SystemStatus> + Send + '_>> {
        Box::pin(async move {
            let ws = self.ws.get().and_then(std::sync::Weak::upgrade);
            let port = match ws {
                Some(ws) => ws.bound_port().await,
                None => None,
            };
            self.snapshot(port)
        })
    }

    /// The version `client.hello` advertised before this control was
    /// installed: the transport's, which ships in `intentd`'s version group.
    fn daemon_version(&self) -> &str {
        intent_transport::VERSION
    }

    fn host_environment(&self) -> HostEnvironment {
        self.host.clone()
    }

    fn request_shutdown(&self) {}

    fn request_update(&self) -> Result<(), String> {
        Err("the orchestrator is not updated through a client".to_owned())
    }

    fn git_credential(
        &self,
        _client_pid: Option<u64>,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Option<intent_transport::control::GitCredential>>
                + Send
                + '_,
        >,
    > {
        Box::pin(async { None })
    }
}

/// The running client listeners. They serve until [`Self::stop`] or the
/// end of the process; the tenant's fence closes their connections.
pub struct ClientListeners {
    stop: ListenersStop,
    ws_port: u16,
    fingerprint: Option<String>,
    socket_path: PathBuf,
    socket: tokio::task::JoinHandle<std::io::Result<()>>,
}

/// Stops both listeners; cheap to clone.
#[derive(Clone)]
pub struct ListenersStop {
    /// Taken by the first [`Self::stop`], so held handles do not keep a
    /// stopped tenant's `Services` alive.
    running: Arc<std::sync::Mutex<Option<Running>>>,
    stopped: Arc<tokio::sync::watch::Sender<bool>>,
}

/// What serves until the listeners stop.
struct Running {
    /// The only strong handle: the status control and the pairing reach
    /// it weakly.
    ws: Arc<WsApiServer>,
    stop_socket: tokio::sync::oneshot::Sender<()>,
    /// Its client-event publisher holds the tenant's API until stopped,
    /// and the tenant's `Services` hold the registry.
    reverse: Arc<PrimaryReverseRegistry>,
}

impl ListenersStop {
    /// Stop accepting on both listeners and close their connections.
    /// Idempotent: a later call waits until the first one is done.
    pub async fn stop(&self) {
        let running = self
            .running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let Some(Running {
            ws,
            stop_socket,
            reverse,
        }) = running
        else {
            self.stopped().await;
            return;
        };
        ws.stop().await;
        drop(ws);
        let _ = stop_socket.send(());
        if let Err(error) = reverse.shutdown_publisher().await {
            tracing::warn!(%error, "the client-event publisher did not stop cleanly");
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
        self.fingerprint.as_deref()
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
    start_client_listeners_with_public_hosts(
        config,
        services,
        bus,
        reverse,
        connections,
        Vec::new(),
    )
    .await
}

/// [`start_client_listeners`], with invite links naming `public_hosts`
/// ([`PUBLIC_HOSTS_ENV`]; none: no invite is minted without a tunnel).
///
/// # Errors
/// As [`start_client_listeners`].
pub async fn start_client_listeners_with_public_hosts(
    config: &ListenConfig,
    services: &Services,
    bus: &EventBus,
    reverse: Arc<PrimaryReverseRegistry>,
    connections: &ClientConnections,
    public_hosts: Vec<String>,
) -> Result<ClientListeners, String> {
    let hook = ConnectionHook::new(Arc::new(TenantConnections(connections.clone())));
    // The tenant's `server.maxOutstandingRpcs` (its own config.toml), across
    // both listeners, as `intentd` caps its own. Client host commands run
    // under the tenant's host command runtime, which its fence stops.
    let limiter = RpcLimiter::new(services.settings_registry().map_or(
        intent_core::config::DEFAULT_SERVER_MAX_OUTSTANDING_RPCS,
        |registry| registry.snapshot().effective.server.max_outstanding_rpcs,
    ))
    .with_host_exec(services.host_exec_runtime());
    let api_services = Arc::new(services.clone());
    if let Err(error) = api_services.initialize_repository_wire().await {
        tracing::debug!(%error, "native repository context remains unavailable");
    }
    let weak_services = Arc::downgrade(&api_services);
    let api: Arc<dyn WorkspaceApi> = api_services;
    let options = WsOptions {
        bind_addresses: config.bind_addresses.clone(),
        base_port: config.ws_port,
        rpc_limiter: limiter.clone(),
        ..WsOptions::default()
    };
    let host = tokio::task::spawn_blocking(intent_transport::detect_host_environment)
        .await
        .map_err(|e| format!("host environment: {e}"))?;
    let port_slot = Arc::new(OnceLock::new());
    let ws_slot = Arc::new(OnceLock::new());
    let sample = Arc::new(std::sync::Mutex::new(UsageSample::default()));
    let control: Arc<dyn SystemControl> = Arc::new(StatusControl {
        services: weak_services,
        host: host.clone(),
        ws: ws_slot.clone(),
        port: port_slot.clone(),
        started: std::time::Instant::now(),
        sample: sample.clone(),
    });
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
            Some(control.clone()),
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
            host,
            ws: ws_slot.clone(),
            public_hosts,
        });
        let mut ws = WsApiServer::new_with_reverse(
            api.clone(),
            bus.clone(),
            &tls,
            &tokens,
            options,
            reverse.clone(),
            Some(control.clone()),
        )
        .map_err(|e| format!("WSS listener: {e}"))?;
        ws.install_pairing_info(pairing.clone());
        (ws, Some(pairing))
    };
    ws.install_connection_set(hook.clone());
    attach_tenant_surfaces(
        services,
        &api,
        bus,
        &control,
        pairing.as_ref(),
        &limiter,
        &reverse,
    );
    let ws_port = ws
        .start()
        .await
        .map_err(|e| format!("WSS listener on port {}: {e}", config.ws_port))?;
    let ws = Arc::new(ws);
    let _ = ws_slot.set(Arc::downgrade(&ws));
    let _ = port_slot.set(ws_port);

    let socket_path = config.socket_path.clone();
    // A stale socket file would pass the bind wait below; the listener
    // removes it before binding anyway, as `intentd` does.
    let _ = std::fs::remove_file(&socket_path);
    let (stop_socket, socket_stopped) = tokio::sync::oneshot::channel::<()>();
    let mut socket = tokio::spawn({
        let socket_path = socket_path.clone();
        let bus = bus.clone();
        let reverse_for_socket = reverse.clone();
        async move {
            intent_transport::serve_uds_with_connections(
                api,
                bus,
                &socket_path,
                Some(control),
                pairing,
                reverse_for_socket,
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
        let _ = reverse.shutdown_publisher().await;
        return Err(error);
    }
    tracing::info!(
        ws_port,
        socket = %socket_path.display(),
        secure = !config.insecure,
        "serving the default tenant's clients"
    );
    let fingerprint = ws.fingerprint().map(str::to_owned);
    let stop = ListenersStop {
        running: Arc::new(std::sync::Mutex::new(Some(Running {
            ws,
            stop_socket,
            reverse,
        }))),
        stopped: Arc::new(tokio::sync::watch::channel(false).0),
    };
    tokio::spawn(sample_usage(
        sample,
        services.configured_workspaces_root().map(Path::to_path_buf),
        stop.stopped.subscribe(),
    ));
    Ok(ClientListeners {
        stop,
        ws_port,
        fingerprint,
        socket_path,
        socket,
    })
}

/// What the tenant's `Services` reach through the listeners, as `intentd`
/// attaches them: invite links (`workspace.invite.list` rows carry their
/// `url`) and the Assistant's `ws.rpc` loopback.
fn attach_tenant_surfaces(
    services: &Services,
    api: &Arc<dyn WorkspaceApi>,
    bus: &EventBus,
    control: &Arc<dyn SystemControl>,
    pairing: Option<&Arc<dyn ServerPairingInfo>>,
    limiter: &RpcLimiter,
    reverse: &Arc<PrimaryReverseRegistry>,
) {
    if let Some(pairing) = pairing {
        services.attach_invite_link_builder(Arc::new(intent_transport::InviteLinkResolver::new(
            pairing.clone(),
        )));
    }
    services.attach_agent_rpc(Arc::new(
        intent_transport::LoopbackAgentRpc::new(
            api,
            bus.clone(),
            Some(control.clone()),
            pairing.cloned(),
            limiter.clone(),
            intent_transport::VERSION,
        )
        .with_reverse_registry(reverse.clone()),
    ));
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
