//! intentd — the Intent backend daemon and its own control client (§5.7).
//!
//! This binary is the composition root (§3.2 rule 5): it is the only place that
//! wires concrete implementations together (store → services → transport).

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::{Parser, Subcommand};
use intent_core::{Config, WorkspaceApi};
use intent_services::{
    default_process_cap, AgentManager, BusEventSink, EventBus, FileWatcher, Services,
};
use intent_store::Store;
use intent_transport::{
    detect_has_display, ensure_tls_certificate, get_or_create_token, serve_uds, CertStatus,
    KeyringTokenStore, SystemControl, SystemStatus, TokenStore, WsApiServer, WsOptions,
};
use serde_json::{json, Value};

mod client;
mod import;
mod service;
use client::rpc_call;

/// intentd — local-first JSON-RPC daemon for the Intent domain model.
#[derive(Debug, Parser)]
#[command(name = "intentd", version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Start the daemon and serve JSON-RPC. `--listen` selects the transport(s):
    /// `uds` (default), `tcp` (HTTPS+WSS on 0.0.0.0:5180), or `both`.
    Serve {
        /// Transport to listen on: `uds`, `tcp`, or `both`.
        #[arg(long, default_value = "uds")]
        listen: String,
        /// Force connection locality (§5.14): `local` or `remote`. Overrides the
        /// transport default (UDS ⇒ local, TCP/WSS ⇒ remote) for `host.status`
        /// and the mDNS TXT record. Omit to infer from the transport.
        #[arg(long)]
        mode: Option<String>,
    },
    /// One-shot JSON-RPC call to a running daemon; prints the JSON result.
    Call {
        /// JSON-RPC method, e.g. `workspace.list`.
        method: String,
        /// Params as a JSON object string, e.g. `{"workspaceId":"ws-1"}`.
        #[arg(long)]
        params: Option<String>,
    },
    /// Probe daemon liveness and print live status (transports, port, clients,
    /// agents, cert fingerprint, host OS/arch + hasDisplay + locality, §5.7).
    Status,
    /// Ask a running daemon to shut down gracefully (control RPC → SIGTERM →
    /// SIGKILL escalation, signalled via the pidfile, §5.7).
    Stop,
    /// Diagnostics: data-dir writable, SQLite/migrations current, providers,
    /// ports free, cert validity, GitHub token, context engine, host caps (§5.7).
    Doctor,
    /// Install/uninstall/validate the platform service unit (launchd/systemd,
    /// §5.8) so the daemon runs unattended under the OS service manager.
    Service {
        #[command(subcommand)]
        action: ServiceAction,
    },
    /// stdio↔TCP MCP proxy referenced from a generated `--mcp-config`; forwards a
    /// spawned provider's MCP frames to the daemon's in-process server (§6.8).
    McpBridge {
        /// The per-agent MCP listener address to connect to (`host:port`).
        #[arg(long)]
        connect: String,
    },
    /// Migrate an existing Intent (Electron) install into intentd's SQLite store
    /// (§9.7): read `<dir>/workspaces.json` and each workspace's `.workspace/`
    /// entities and idempotently upsert them. Read-only toward the source.
    Import {
        /// Path to the Intent `userData` directory to import from.
        #[arg(long)]
        from: PathBuf,
    },
}

/// Sub-actions for `intentd service` (daemonization, §5.8).
#[derive(Debug, Subcommand)]
enum ServiceAction {
    /// Install (or refresh) the launchd/systemd user unit.
    Install,
    /// Remove the installed unit.
    Uninstall,
    /// Report whether the unit is installed and current (non-zero if not).
    Status,
}

#[tokio::main]
async fn main() -> ExitCode {
    init_tracing();
    match Cli::parse().command {
        Command::Serve { listen, mode } => to_exit(cmd_serve(&listen, mode.as_deref()).await),
        Command::Call { method, params } => to_exit(cmd_call(&method, params.as_deref()).await),
        Command::Status => cmd_status().await,
        Command::Stop => cmd_stop().await,
        Command::Doctor => cmd_doctor().await,
        Command::Service { action } => to_exit(cmd_service(&action)),
        Command::McpBridge { connect } => to_exit(cmd_mcp_bridge(&connect).await),
        Command::Import { from } => to_exit(cmd_import(&from).await),
    }
}

/// Migrate a legacy Intent `userData` dir into intentd's SQLite store (§9.7).
/// Opens (creating + migrating) the configured DB, runs the idempotent import,
/// and prints the per-domain summary. Exits non-zero on a hard source failure.
async fn cmd_import(from: &Path) -> anyhow::Result<()> {
    let config = resolve_config()?;
    std::fs::create_dir_all(&config.data_dir)?;
    let store = Store::open(&config.db_path)
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    let summary = import::run(&store, from).await?;
    println!("{summary}");
    Ok(())
}

async fn cmd_mcp_bridge(connect: &str) -> anyhow::Result<()> {
    intent_acp::run_stdio_bridge(connect)
        .await
        .map_err(|e| anyhow::anyhow!("mcp bridge: {e}"))
}

fn to_exit(result: anyhow::Result<()>) -> ExitCode {
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn init_tracing() {
    use tracing_subscriber::{fmt, EnvFilter};
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let _ = fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init();
}

fn resolve_config() -> anyhow::Result<Config> {
    Config::resolve().map_err(|e| anyhow::anyhow!(e.to_string()))
}

async fn cmd_serve(listen: &str, mode: Option<&str>) -> anyhow::Result<()> {
    let (serve_uds_enabled, serve_tcp_enabled) = match listen {
        "uds" => (true, false),
        "tcp" => (false, true),
        "both" => (true, true),
        other => anyhow::bail!("unsupported --listen '{other}'; expected uds|tcp|both"),
    };
    // Resolve the optional locality override (§5.14): `--mode local|remote`
    // forces the value reported over `host.status` + mDNS regardless of
    // transport; absent ⇒ infer from the transport (UDS local, TCP/WSS remote).
    let locality_override = parse_locality_mode(mode)?;
    let config = resolve_config()?;
    std::fs::create_dir_all(&config.data_dir)?;
    // Single-instance guard (§5.6): refuse to start if a live daemon owns the
    // UDS or a live pid holds the pidfile; clean a stale socket/pidfile whose
    // owner is gone. The returned guard removes our pidfile on shutdown.
    let _pidfile = acquire_single_instance(&config, serve_uds_enabled).await?;
    let store = Store::open(&config.db_path)
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    // The event bus shares the store with the services surface so subscribers
    // see the same durable event log that future mutations will publish to.
    let bus = EventBus::new(store.clone());
    // Hold a store handle for the §10.2 retention sweep before the store is
    // moved into the services surface below.
    let retention_store = store.clone();
    // The services surface publishes CRUD change events onto the same bus that
    // transport subscriptions read, so a mutation on one connection streams to
    // subscribers on another (§10).
    let services = Services::new(store)
        .with_assets_root(config.data_dir.join("assets"))
        .with_event_bus(bus.clone());
    // The AgentManager multiplexes spawned agent processes over the ACP client
    // (§6.8). Its concrete EventSink bridges the client-served fs/permission
    // events (M3.5) onto the same bus, and `run_turn` drives the streaming
    // router (M3.4); a global process cap + LRU registry bound concurrency.
    let manager = Arc::new(AgentManager::new(
        services.clone(),
        Arc::new(BusEventSink::new(bus.clone())),
        default_process_cap(),
    ));
    // Attach the manager to the services surface so the `agent.*` RPC handlers
    // drive the live spawn/turn/MCP loop at runtime (the shared `OnceLock` is
    // visible to every clone, including the api handed to the transport below).
    services.attach_agent_manager(&manager);
    tracing::info!(
        process_cap = manager.registry().cap(),
        "agent manager ready"
    );
    // Background PR refresh (§7.6): periodically re-fetch every linked PR,
    // persist any change, and emit `pr:*` events so clients update without
    // polling. Safe when source control is unconfigured (each refresh logs and
    // swallows the missing-provider error). Aborted on clean shutdown.
    let pr_refresh = services.spawn_pr_refresh_loop(std::time::Duration::from_secs(60));
    // Idle agent reaping (§5.6/§6.7): periodically evict agents idle past the
    // configured TTL, killing each one's whole process group. Disabled entirely
    // when `agents.idleReapMinutes == 0`.
    let reap_task = spawn_idle_reap_loop(manager.clone(), config.idle_reap_minutes);
    // Event retention/compaction (§10.2): periodically delete `agent:stream:*`
    // chunk events older than the configured TTL, preserving every other event
    // family. Disabled entirely when `events.streamRetentionHours == 0`.
    let retention_task =
        spawn_stream_retention_loop(retention_store, config.stream_retention_hours);
    // External MCP servers (§18.3): start every enabled, non-disabled server,
    // then run the health monitor (periodic ping + auto-restart pushing
    // `mcp.servers:status-changed`). The hub is reaped on shutdown so no orphan
    // server processes remain (PTY-host reaping parity).
    services.start_enabled_mcp_servers().await;
    let mcp_hub = services.mcp_hub();
    let mcp_monitor = mcp_hub.spawn_health_monitor();
    let api: Arc<dyn WorkspaceApi> = Arc::new(services);
    // Start a filesystem watcher per active workspace with a resolvable on-disk
    // path; each publishes debounced `file:changed` events to the shared bus.
    // The handles are held for the lifetime of `serve` and torn down on return.
    let _watchers = start_workspace_watchers(&bus, api.as_ref()).await;

    // Start the HTTPS+WSS listener when requested. TLS and bearer auth are
    // auto-on for TCP (§5.2/§5.3): the self-signed cert (M5.1) is reused across
    // restarts and the persisted token (M5.2) gates upgrades. The listener runs
    // in the background and is gracefully stopped after the shutdown signal.
    let (ws_server, ws_port) = if serve_tcp_enabled {
        let tls =
            ensure_tls_certificate(&config.data_dir).map_err(|e| anyhow::anyhow!(e.to_string()))?;
        let token_store = resolve_token_store();
        get_or_create_token(&*token_store).map_err(|e| anyhow::anyhow!(e.to_string()))?;
        let mut ws_options = ws_options_from_env();
        ws_options.locality_override = locality_override;
        let server = WsApiServer::new(api.clone(), bus.clone(), &tls, token_store, ws_options)
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        let port = server.start().await?;
        tracing::info!(port, fingerprint = %server.fingerprint(), "intentd WSS listening");
        (Some(server), Some(port))
    } else {
        (None, None)
    };

    // System control surface (§5.7): exposes `system.status` / `system.shutdown`
    // to local UDS clients (`intentd status` / `intentd stop`). The `Notify`
    // lets the `system.shutdown` RPC trigger the same graceful teardown as an OS
    // signal, so `stop` can ask politely before escalating to SIGTERM/SIGKILL.
    let shutdown_notify = Arc::new(tokio::sync::Notify::new());
    let control: Arc<dyn SystemControl> = Arc::new(DaemonControl {
        listen_mode: listen.to_string(),
        uds: serve_uds_enabled,
        tcp: serve_tcp_enabled,
        port: ws_port,
        fingerprint: ws_server.as_ref().map(|s| s.fingerprint().to_string()),
        ws_server: ws_server.clone(),
        manager: manager.clone(),
        shutdown: shutdown_notify.clone(),
    });

    let shutdown = {
        let notify = shutdown_notify.clone();
        async move {
            tokio::select! {
                _ = shutdown_signal() => {}
                _ = notify.notified() => tracing::info!("shutdown requested via system.shutdown"),
            }
        }
    };

    if serve_uds_enabled {
        tracing::info!(socket = %config.socket_path.display(), "starting intentd");
        serve_uds(api, bus, &config.socket_path, Some(control), shutdown).await?;
    } else {
        // TCP-only: no local control transport, but the shutdown notify is still
        // wired so a future control path could trigger it. Wait for a signal.
        let _ = control;
        shutdown.await;
    }

    // Clean shutdown: stop the WSS listener (graceful close + port release),
    // stop the PR refresh loop, then kill every spawned agent child and clear
    // the registry (§6.8 teardown). Idle reaping during the run is the M5
    // `reap_idle` hook.
    if let Some(server) = ws_server {
        server.stop().await;
    }
    pr_refresh.abort();
    if let Some(reap_task) = reap_task {
        reap_task.abort();
    }
    if let Some(retention_task) = retention_task {
        retention_task.abort();
    }
    // Stop the MCP health monitor and reap every external MCP server's process
    // group so no orphan stdio servers survive the daemon (§18.3).
    mcp_monitor.abort();
    mcp_hub.shutdown().await;
    manager.shutdown().await;
    Ok(())
}

/// Live daemon control surface backing `system.status` / `system.shutdown`
/// (§5.7). Built post-bind so the resolved WSS `port`/`fingerprint` are real
/// (not guessed); `client_count`/agent count are read live on each status call.
struct DaemonControl {
    listen_mode: String,
    uds: bool,
    tcp: bool,
    port: Option<u16>,
    fingerprint: Option<String>,
    ws_server: Option<WsApiServer>,
    manager: Arc<AgentManager>,
    shutdown: Arc<tokio::sync::Notify>,
}

impl SystemControl for DaemonControl {
    fn status(&self) -> SystemStatus {
        SystemStatus {
            listen_mode: self.listen_mode.clone(),
            uds: self.uds,
            tcp: self.tcp,
            port: self.port,
            clients: self
                .ws_server
                .as_ref()
                .map(|s| s.client_count())
                .unwrap_or(0),
            agents: self.manager.registry().size(),
            fingerprint: self.fingerprint.clone(),
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            has_display: detect_has_display(),
        }
    }

    fn request_shutdown(&self) {
        // `notify_one` stores a permit if the serve loop is not yet awaiting, so
        // the shutdown is never lost to a race with a freshly-arrived RPC.
        self.shutdown.notify_one();
    }
}

/// Fixed-token [`TokenStore`] selected only when `INTENTD_AUTH_TOKEN` is set.
/// TEST-ONLY SEAM (§13.1 E2E): lets the E2E suite authenticate a real `intentd
/// serve --listen tcp/both` daemon hermetically, without touching the OS
/// keychain. Production always uses [`KeyringTokenStore`].
struct EnvTokenStore(String);

impl TokenStore for EnvTokenStore {
    fn load_token(&self) -> Option<String> {
        Some(self.0.clone())
    }
    fn store_token(&self, _token: &str) -> intent_core::Result<()> {
        Ok(())
    }
}

/// Select the WSS token store: a fixed env token when `INTENTD_AUTH_TOKEN` is
/// set (test-only hermetic seam, §13.1), otherwise the OS keychain.
fn resolve_token_store() -> Arc<dyn TokenStore> {
    match std::env::var("INTENTD_AUTH_TOKEN") {
        Ok(t) if !t.is_empty() => Arc::new(EnvTokenStore(t)),
        _ => Arc::new(KeyringTokenStore),
    }
}

/// Build [`WsOptions`] from the production defaults plus optional env seams:
/// mDNS discovery (`INTENTD_DISCOVERY=1`, default off) and an explicit base port
/// (`INTENTD_TCP_PORT`, `0` = OS-assigned ephemeral). Both are §13.1 E2E seams:
/// the port seam keeps the suite hermetic (no fixed-5180 contention) and the
/// discovery seam lets it assert the advertise→resolve + fingerprint round-trip.
fn ws_options_from_env() -> WsOptions {
    let mut opts = WsOptions::default();
    if env_flag("INTENTD_DISCOVERY") {
        opts.discovery_enabled = true;
    }
    if let Some(port) = std::env::var("INTENTD_TCP_PORT")
        .ok()
        .and_then(|v| v.trim().parse::<u16>().ok())
    {
        opts.base_port = port;
    }
    opts
}

/// Parse the optional `--mode` locality override (§5.14) into the transport
/// override flag: `local` ⇒ `Some(true)`, `remote` ⇒ `Some(false)`, absent ⇒
/// `None` (infer from the transport). Any other value is a hard CLI error. The
/// override is applied to the TCP/WSS listener (`host.status` + mDNS); the local
/// UDS control path is always `local`.
fn parse_locality_mode(mode: Option<&str>) -> anyhow::Result<Option<bool>> {
    match mode {
        None => Ok(None),
        Some("local") => Ok(Some(true)),
        Some("remote") => Ok(Some(false)),
        Some(other) => anyhow::bail!("unsupported --mode '{other}'; expected local|remote"),
    }
}

/// Parse a boolean-ish env flag (`1`/`true`/`yes`, case-insensitive).
fn env_flag(name: &str) -> bool {
    matches!(
        std::env::var(name)
            .ok()
            .map(|v| v.trim().to_ascii_lowercase())
            .as_deref(),
        Some("1") | Some("true") | Some("yes")
    )
}

/// RAII single-instance pidfile: removes the file on drop, but only if it still
/// holds our pid (so a racing replacement is never clobbered).
struct PidFile {
    path: PathBuf,
}

impl Drop for PidFile {
    fn drop(&mut self) {
        if read_pid(&self.path) == Some(std::process::id()) {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// Read a pid from a pidfile, returning `None` when absent/unparseable.
fn read_pid(path: &Path) -> Option<u32> {
    std::fs::read_to_string(path)
        .ok()?
        .trim()
        .parse::<u32>()
        .ok()
}

/// Whether a process with `pid` is currently alive (signal-0 probe).
#[cfg(unix)]
fn pid_is_alive(pid: u32) -> bool {
    use nix::sys::signal::kill;
    use nix::unistd::Pid;
    // `EPERM` means the process exists but we may not signal it — still alive.
    matches!(
        kill(Pid::from_raw(pid as i32), None),
        Ok(()) | Err(nix::errno::Errno::EPERM)
    )
}

#[cfg(not(unix))]
fn pid_is_alive(_pid: u32) -> bool {
    true
}

/// UDS liveness probe: a successful connect means a daemon is listening.
/// UDS is Unix-only; on other platforms there is no socket to probe.
#[cfg(unix)]
async fn uds_is_live(socket_path: &Path) -> bool {
    tokio::net::UnixStream::connect(socket_path).await.is_ok()
}

#[cfg(not(unix))]
async fn uds_is_live(_socket_path: &Path) -> bool {
    false
}

/// Enforce single-instance startup (§5.6). Refuses to start when a live daemon
/// owns the UDS or a live pid holds the pidfile; otherwise removes a stale
/// socket/pidfile whose owner is gone and claims the pidfile with our pid.
async fn acquire_single_instance(
    config: &Config,
    serve_uds_enabled: bool,
) -> anyhow::Result<PidFile> {
    if serve_uds_enabled && config.socket_path.exists() {
        if uds_is_live(&config.socket_path).await {
            anyhow::bail!(
                "intentd is already running on {} — refusing to start a second instance",
                config.socket_path.display()
            );
        }
        tracing::warn!(socket = %config.socket_path.display(), "removing stale socket (owner gone)");
        let _ = std::fs::remove_file(&config.socket_path);
    }

    if let Some(pid) = read_pid(&config.pid_path) {
        if pid != std::process::id() && pid_is_alive(pid) {
            anyhow::bail!(
                "intentd is already running (pid {pid}, pidfile {}) — refusing to start a second instance",
                config.pid_path.display()
            );
        }
        tracing::warn!(pid, pidfile = %config.pid_path.display(), "removing stale pidfile (owner gone)");
        let _ = std::fs::remove_file(&config.pid_path);
    }

    std::fs::write(&config.pid_path, std::process::id().to_string())
        .map_err(|e| anyhow::anyhow!("write pidfile {}: {e}", config.pid_path.display()))?;
    Ok(PidFile {
        path: config.pid_path.clone(),
    })
}

/// Spawn the periodic idle-reap sweep (§5.6/§6.7), or `None` when disabled
/// (`idle_reap_minutes == 0`). The sweep interval is derived from the TTL
/// (≈4×/TTL), clamped so long TTLs still sweep and short ones do not busy-loop.
fn spawn_idle_reap_loop(
    manager: Arc<AgentManager>,
    idle_reap_minutes: u32,
) -> Option<tokio::task::JoinHandle<()>> {
    let Some((ttl, interval)) = reap_timings(idle_reap_minutes) else {
        tracing::info!("idle agent reaping disabled (agents.idleReapMinutes = 0)");
        return None;
    };
    tracing::info!(
        ttl_ms = ttl.as_millis() as u64,
        interval_ms = interval.as_millis() as u64,
        "idle agent reaping enabled"
    );
    Some(tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let reaped = manager.reap_idle_older_than(ttl).await;
            if reaped > 0 {
                tracing::info!(reaped, "idle agent sweep evicted idle agents");
            }
        }
    }))
}

/// Compute the idle-reap `(ttl, sweep interval)`, or `None` when disabled
/// (`idle_reap_minutes == 0`). Production rule: interval ≈ ttl/4, clamped to
/// `[30s, 300s]`. The `INTENTD_IDLE_REAP_MS` env seam (§13.1 E2E, test-only)
/// forces a sub-second TTL+interval so the E2E suite can assert reaping without
/// a ≥30s wait; it is ignored in production deployments.
fn reap_timings(idle_reap_minutes: u32) -> Option<(Duration, Duration)> {
    if let Some(ms) = std::env::var("INTENTD_IDLE_REAP_MS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&m| m > 0)
    {
        let d = Duration::from_millis(ms);
        return Some((d, d));
    }
    if idle_reap_minutes == 0 {
        return None;
    }
    let ttl = Duration::from_secs(idle_reap_minutes as u64 * 60);
    let interval = (ttl / 4).clamp(Duration::from_secs(30), Duration::from_secs(300));
    Some((ttl, interval))
}

/// Spawn the periodic event-retention/compaction sweep (§10.2), or `None` when
/// disabled (`stream_retention_hours == 0`). Each tick deletes `agent:stream:*`
/// chunk events older than the TTL while preserving every other event family.
/// The sweep interval is derived from the TTL (≈4×/TTL), clamped so long TTLs
/// still sweep periodically and short ones do not busy-loop. A failed sweep is
/// logged and retried on the next tick (never aborts the loop).
fn spawn_stream_retention_loop(
    store: Store,
    stream_retention_hours: u32,
) -> Option<tokio::task::JoinHandle<()>> {
    if stream_retention_hours == 0 {
        tracing::info!("event retention sweep disabled (events.streamRetentionHours = 0)");
        return None;
    }
    let ttl = Duration::from_secs(stream_retention_hours as u64 * 3600);
    let interval = (ttl / 4).clamp(Duration::from_secs(300), Duration::from_secs(3600));
    tracing::info!(
        ttl_hours = stream_retention_hours,
        interval_secs = interval.as_secs(),
        "event retention sweep enabled (agent:stream:* only)"
    );
    Some(tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let cutoff = intent_core::iso_minutes_ago(stream_retention_hours as i64 * 60);
            match store.delete_stream_events_before(&cutoff).await {
                Ok(removed) if removed > 0 => {
                    tracing::info!(
                        removed,
                        cutoff,
                        "event retention sweep trimmed stream events"
                    );
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "event retention sweep failed"),
            }
        }
    }))
}

/// Start a [`FileWatcher`] for every non-archived workspace that exposes an
/// existing on-disk path (`path`, falling back to `worktree_path`). Returns the
/// live handles; dropping them stops the watchers (clean shutdown).
async fn start_workspace_watchers(bus: &EventBus, services: &dyn WorkspaceApi) -> Vec<FileWatcher> {
    let workspaces = match services.list_workspaces(false).await {
        Ok(ws) => ws,
        Err(e) => {
            tracing::warn!(error = %e, "could not list workspaces for file watching");
            return Vec::new();
        }
    };
    let mut watchers = Vec::new();
    for ws in workspaces {
        let Some(root) = ws.path.clone().or_else(|| ws.worktree_path.clone()) else {
            continue;
        };
        let path = std::path::PathBuf::from(&root);
        if !path.is_dir() {
            continue;
        }
        match FileWatcher::start(bus.clone(), ws.id.clone(), path) {
            Ok(w) => {
                tracing::info!(workspace = %ws.id, path = %root, "watching workspace files");
                watchers.push(w);
            }
            Err(e) => {
                tracing::warn!(workspace = %ws.id, path = %root, error = %e, "file watcher start failed")
            }
        }
    }
    tracing::info!(count = watchers.len(), "file watchers started");
    watchers
}

async fn cmd_call(method: &str, params: Option<&str>) -> anyhow::Result<()> {
    let config = resolve_config()?;
    let params: Value = match params {
        Some(s) => {
            serde_json::from_str(s).map_err(|e| anyhow::anyhow!("invalid --params JSON: {e}"))?
        }
        None => json!({}),
    };
    let response = rpc_call(&config.socket_path, method, params).await?;
    if let Some(error) = response.get("error") {
        eprintln!("{}", serde_json::to_string_pretty(error)?);
        anyhow::bail!("rpc error");
    }
    let result = response.get("result").cloned().unwrap_or(Value::Null);
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

async fn cmd_status() -> ExitCode {
    let config = match resolve_config() {
        Ok(c) => c,
        Err(e) => return to_exit(Err(e)),
    };
    // Source the status from the live `system.status` control RPC (§5.7) rather
    // than guessing — a successful response is itself the liveness proof.
    match rpc_call(&config.socket_path, "system.status", json!({})).await {
        Ok(resp) if resp.get("result").is_some() => {
            print_status(&config, &resp["result"]);
            ExitCode::SUCCESS
        }
        Ok(resp) => {
            // Reachable but the control method is unavailable (older daemon): the
            // socket answered, so it is up, but we cannot render full status.
            println!("intentd: up (status rpc unavailable)");
            println!("  socket: {}", config.socket_path.display());
            if let Some(e) = resp.get("error") {
                println!("  error: {e}");
            }
            ExitCode::SUCCESS
        }
        Err(_) => {
            println!("intentd: down");
            println!("  socket: {} (not reachable)", config.socket_path.display());
            ExitCode::FAILURE
        }
    }
}

/// Render the `system.status` result (§5.7): liveness, transports + listen mode,
/// bound port, connected clients, active agents, cert fingerprint, host caps.
fn print_status(config: &Config, r: &Value) {
    let transports = r["transports"]
        .as_array()
        .map(|a| {
            a.iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(", ")
        })
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "(none)".to_string());
    println!("intentd: up");
    println!("  socket: {}", config.socket_path.display());
    println!("  listenMode: {}", r["listenMode"].as_str().unwrap_or("?"));
    println!("  transports: {transports}");
    match r["port"].as_u64() {
        Some(p) => println!("  port: {p}"),
        None => println!("  port: (uds-only)"),
    }
    println!("  clients: {}", r["clients"].as_u64().unwrap_or(0));
    println!("  agents: {}", r["agents"].as_u64().unwrap_or(0));
    match r["fingerprint"].as_str() {
        Some(fp) => println!("  fingerprint: {fp}"),
        None => println!("  fingerprint: (none)"),
    }
    let host = &r["host"];
    println!(
        "  host: {} / {} (hasDisplay={}, locality={})",
        host["os"].as_str().unwrap_or("?"),
        host["arch"].as_str().unwrap_or("?"),
        host["hasDisplay"].as_bool().unwrap_or(false),
        host["locality"].as_str().unwrap_or("?"),
    );
}

/// Ask a running daemon to stop (§5.7): issue the graceful `system.shutdown`
/// control RPC, then escalate via the pidfile (SIGTERM → SIGKILL) with timeouts.
/// Exits non-zero only if shutdown cannot be confirmed.
async fn cmd_stop() -> ExitCode {
    let config = match resolve_config() {
        Ok(c) => c,
        Err(e) => return to_exit(Err(e)),
    };
    let Some(pid) = read_pid(&config.pid_path) else {
        println!(
            "intentd: not running (no pidfile at {})",
            config.pid_path.display()
        );
        return ExitCode::SUCCESS;
    };
    if !pid_is_alive(pid) {
        println!("intentd: not running (stale pidfile for pid {pid}); cleaning up");
        let _ = std::fs::remove_file(&config.pid_path);
        return ExitCode::SUCCESS;
    }

    // (1) Politely request a graceful shutdown over the control RPC.
    let graceful = match rpc_call(&config.socket_path, "system.shutdown", json!({})).await {
        Ok(resp) if resp.get("result").is_some() => {
            println!("intentd: graceful shutdown requested (pid {pid})");
            true
        }
        _ => {
            println!("intentd: control RPC unavailable; signalling pid {pid}");
            false
        }
    };

    // (2)-(4) Wait, then escalate SIGTERM → SIGKILL with timeouts.
    let outcome = run_stop_escalation(pid, graceful).await;
    match outcome {
        StopOutcome::AlreadyDown => println!("intentd: stopped"),
        StopOutcome::Graceful => println!("intentd: stopped gracefully"),
        StopOutcome::Terminated => println!("intentd: stopped (SIGTERM)"),
        StopOutcome::Killed => println!("intentd: stopped (SIGKILL)"),
        StopOutcome::Failed => {
            eprintln!("error: could not confirm intentd shutdown (pid {pid})");
            return ExitCode::FAILURE;
        }
    }
    ExitCode::SUCCESS
}

/// Run the escalation with production timeouts, using the real OS signaller on
/// unix. On non-unix there is no UDS daemon to signal, so report failure.
async fn run_stop_escalation(pid: u32, graceful: bool) -> StopOutcome {
    #[cfg(unix)]
    {
        escalate_stop(
            &NixSignaller,
            pid,
            graceful,
            Duration::from_secs(5),
            Duration::from_secs(5),
            Duration::from_secs(3),
            Duration::from_millis(100),
        )
        .await
    }
    #[cfg(not(unix))]
    {
        let _ = (pid, graceful);
        StopOutcome::Failed
    }
}

/// Install/uninstall/validate the platform service unit (§5.8).
fn cmd_service(action: &ServiceAction) -> anyhow::Result<()> {
    let config = resolve_config()?;
    match action {
        ServiceAction::Install => service::install(&config),
        ServiceAction::Uninstall => service::uninstall(&config),
        ServiceAction::Status => {
            if service::status(&config)? {
                Ok(())
            } else {
                anyhow::bail!("service unit not installed or stale")
            }
        }
    }
}

/// The terminal result of a stop escalation (§5.7).
#[derive(Debug, PartialEq, Eq)]
enum StopOutcome {
    /// The process was already gone before any escalation.
    AlreadyDown,
    /// Exited after the graceful control RPC, before any signal.
    Graceful,
    /// Exited after SIGTERM.
    Terminated,
    /// Exited after SIGKILL.
    Killed,
    /// Still alive after SIGKILL + timeout — shutdown unconfirmed.
    Failed,
}

/// Process-signalling seam so the escalation logic is unit-testable with a fake
/// (§5.7 verification). The real impl uses `nix` signal-0/SIGTERM/SIGKILL.
trait Signaller {
    fn is_alive(&self, pid: u32) -> bool;
    fn term(&self, pid: u32);
    fn kill(&self, pid: u32);
}

/// SIGTERM → SIGKILL escalation. The caller has already issued the graceful
/// control RPC; `graceful_requested` says whether to first wait for a polite
/// exit. Each phase polls liveness up to its timeout before escalating.
async fn escalate_stop<S: Signaller>(
    sig: &S,
    pid: u32,
    graceful_requested: bool,
    grace: Duration,
    term_timeout: Duration,
    kill_timeout: Duration,
    poll: Duration,
) -> StopOutcome {
    if !sig.is_alive(pid) {
        return StopOutcome::AlreadyDown;
    }
    if graceful_requested && wait_for_exit(sig, pid, grace, poll).await {
        return StopOutcome::Graceful;
    }
    sig.term(pid);
    if wait_for_exit(sig, pid, term_timeout, poll).await {
        return StopOutcome::Terminated;
    }
    sig.kill(pid);
    if wait_for_exit(sig, pid, kill_timeout, poll).await {
        return StopOutcome::Killed;
    }
    StopOutcome::Failed
}

/// Poll `is_alive` until the process exits or `timeout` elapses; `true` on exit.
async fn wait_for_exit<S: Signaller>(sig: &S, pid: u32, timeout: Duration, poll: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if !sig.is_alive(pid) {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(poll).await;
    }
}

/// The real OS signaller (unix): signal-0 liveness probe + SIGTERM/SIGKILL.
#[cfg(unix)]
struct NixSignaller;

#[cfg(unix)]
impl Signaller for NixSignaller {
    fn is_alive(&self, pid: u32) -> bool {
        pid_is_alive(pid)
    }
    fn term(&self, pid: u32) {
        use nix::sys::signal::{kill, Signal};
        use nix::unistd::Pid;
        let _ = kill(Pid::from_raw(pid as i32), Signal::SIGTERM);
    }
    fn kill(&self, pid: u32) {
        use nix::sys::signal::{kill, Signal};
        use nix::unistd::Pid;
        let _ = kill(Pid::from_raw(pid as i32), Signal::SIGKILL);
    }
}

async fn cmd_doctor() -> ExitCode {
    let config = match resolve_config() {
        Ok(c) => c,
        Err(e) => return to_exit(Err(e)),
    };
    let mut ok = true;

    match check_data_dir_writable(&config) {
        Ok(()) => println!("[ok] data dir writable: {}", config.data_dir.display()),
        Err(e) => {
            ok = false;
            println!("[FAIL] data dir not writable: {e}");
        }
    }

    match Store::open(&config.db_path).await {
        Ok(store) => {
            println!("[ok] sqlite openable: {}", config.db_path.display());
            match store.migration_status().await {
                Ok(status) if status.is_current() => {
                    println!(
                        "[ok] migrations current: {} applied {:?}",
                        status.applied.len(),
                        status.applied
                    );
                    // Explicit gate on the agent_session schema (migration 0004),
                    // the persistence behind the M3 orchestration loop (§9.2).
                    if status.applied.contains(&4) {
                        println!("[ok] migration 0004 (agent_session) applied");
                    } else {
                        ok = false;
                        println!("[FAIL] migration 0004 (agent_session) missing");
                    }
                }
                Ok(status) => {
                    ok = false;
                    println!(
                        "[FAIL] migrations not current: expected {:?}, applied {:?}",
                        status.expected, status.applied
                    );
                }
                Err(e) => {
                    ok = false;
                    println!("[FAIL] migration status: {e}");
                }
            }
        }
        Err(e) => {
            ok = false;
            println!("[FAIL] sqlite/migrations: {e}");
        }
    }

    report_provider_availability().await;

    // §5.7 additions: ports-free window, cert validity, GitHub token presence,
    // context-engine availability, and host display/locality. The first two are
    // gating; the rest are graceful-degradation reports (never fatal, G6).
    if !check_ports_free() {
        ok = false;
    }
    if !check_cert_validity(&config) {
        ok = false;
    }
    report_github_token();
    report_context_engine();
    report_host_capabilities();

    if ok {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

/// §5.7 ports-free check: count bindable ports in the WSS bind window
/// (`DEFAULT_PORT ..= +MAX_PORT_ATTEMPTS`). Fails only when the entire window is
/// occupied (no TCP listener could start); a busy base port alone is reported.
fn check_ports_free() -> bool {
    use intent_transport::lifecycle::{DEFAULT_PORT, MAX_PORT_ATTEMPTS};
    let mut free = 0u16;
    for offset in 0..MAX_PORT_ATTEMPTS {
        let Some(port) = DEFAULT_PORT.checked_add(offset) else {
            break;
        };
        if std::net::TcpListener::bind((std::net::Ipv4Addr::UNSPECIFIED, port)).is_ok() {
            free += 1;
        }
    }
    let last = DEFAULT_PORT + MAX_PORT_ATTEMPTS - 1;
    if free == 0 {
        println!("[FAIL] no free port in WSS window {DEFAULT_PORT}..={last}");
        false
    } else {
        println!(
            "[ok] {free}/{MAX_PORT_ATTEMPTS} ports free in WSS window {DEFAULT_PORT}..={last}"
        );
        true
    }
}

/// §5.7 cert-validity check: inspect (never generate) the persisted WSS cert. A
/// missing cert is fine (generated on first TCP serve); an expired/unparseable
/// one is a real failure to surface (the pinned fingerprint will change).
fn check_cert_validity(config: &Config) -> bool {
    match intent_transport::inspect_cert(&config.data_dir) {
        CertStatus::Missing => {
            println!("[ok] TLS cert: none yet (generated on first `serve --listen tcp`)");
            true
        }
        CertStatus::Valid { fingerprint } => {
            println!("[ok] TLS cert valid (fingerprint {fingerprint})");
            true
        }
        CertStatus::Expired => {
            println!("[FAIL] TLS cert expired/not-yet-valid (regenerated on next TCP serve)");
            false
        }
        CertStatus::Unparseable => {
            println!("[FAIL] TLS cert unparseable on disk");
            false
        }
    }
}

/// §5.7 / §11.3 GitHub-token presence: report presence only, never the value.
/// Non-fatal — GitHub features degrade gracefully when no token is configured.
fn report_github_token() {
    let env_present =
        std::env::var_os("GITHUB_TOKEN").is_some() || std::env::var_os("GH_TOKEN").is_some();
    let gh_cli = intent_providers::resolve_on_path("gh").is_some();
    if env_present {
        println!("[ok] GitHub token present (GITHUB_TOKEN/GH_TOKEN set)");
    } else if gh_cli {
        println!("[--] GitHub token: none in env; `gh` CLI on PATH (may provide one)");
    } else {
        println!("[--] GitHub token: not present (GitHub features degrade gracefully)");
    }
}

/// §5.7 context-engine availability: best-effort `auggie` PATH probe. Non-fatal
/// — codebase retrieval degrades gracefully when the engine is absent (§8.3).
fn report_context_engine() {
    match intent_providers::resolve_on_path("auggie") {
        Some(p) => println!("[ok] context engine: auggie available ({})", p.display()),
        None => {
            println!("[--] context engine: auggie not on PATH (retrieval degrades gracefully)")
        }
    }
}

/// §5.7 / §12.3 host capabilities: display availability + derived locality. The
/// local UDS control path is `local`; remote WSS clients are `remote` (the live
/// value is reported per-connection by `intentd status`).
fn report_host_capabilities() {
    println!(
        "[ok] host: {} / {} (hasDisplay={}, locality=local over UDS)",
        std::env::consts::OS,
        std::env::consts::ARCH,
        detect_has_display(),
    );
}

/// Doctor provider-discovery section (§6.9): print which configured ACP
/// providers are installed (resolvable on `PATH`) and, best-effort, which are
/// authenticated. Provider availability never fails `doctor` — a host with no
/// providers installed is a valid (if limited) state.
async fn report_provider_availability() {
    println!("providers:");
    for provider in intent_providers::discover_providers() {
        if let Some(reason) = &provider.gated_off {
            println!("  [--] {} ({})", provider.id, reason);
            continue;
        }
        if !provider.installed {
            println!(
                "  [--] {} not installed ({} not on PATH)",
                provider.id, provider.command
            );
            continue;
        }
        let path = provider
            .resolved_path
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_default();
        let auth = check_provider_auth(provider.command, provider.auth_check_args).await;
        println!("  [ok] {} installed: {path}{auth}", provider.id);
    }
}

/// Best-effort authentication probe for an installed provider: run its
/// `auth_check_args` (exit 0 ⇒ authenticated) with a short timeout. Returns a
/// trailing status fragment for the doctor line, or empty when no probe applies.
async fn check_provider_auth(command: &str, auth_check_args: Option<&[&str]>) -> String {
    let Some(args) = auth_check_args else {
        return String::new();
    };
    let run = tokio::process::Command::new(command)
        .args(args)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status();
    match tokio::time::timeout(std::time::Duration::from_secs(8), run).await {
        Ok(Ok(status)) if status.success() => " (authenticated)".to_string(),
        Ok(Ok(_)) => " (not authenticated)".to_string(),
        Ok(Err(_)) => " (auth check failed)".to_string(),
        Err(_) => " (auth check timed out)".to_string(),
    }
}

fn check_data_dir_writable(config: &Config) -> anyhow::Result<()> {
    std::fs::create_dir_all(&config.data_dir)?;
    let probe = config.data_dir.join(".intentd-doctor-probe");
    std::fs::write(&probe, b"ok")?;
    std::fs::remove_file(&probe)?;
    Ok(())
}

#[cfg(unix)]
async fn shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(_) => {
            let _ = tokio::signal::ctrl_c().await;
            return;
        }
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
    tracing::info!("shutdown signal received");
}

#[cfg(not(unix))]
async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_config() -> Config {
        let id = uuid::Uuid::new_v4().to_string();
        let dir = std::env::temp_dir().join(format!("intentd-si-{id}"));
        std::fs::create_dir_all(&dir).unwrap();
        Config {
            config_path: dir.join("config.toml"),
            db_path: dir.join("intentd.db"),
            // UDS paths are capped at ~104 bytes (`SUN_LEN`); keep the socket on
            // a short path so a deep temp data dir does not overflow the bind.
            socket_path: short_socket_path(&id),
            pid_path: dir.join("intentd.pid"),
            idle_reap_minutes: 30,
            stream_retention_hours: 0,
            data_dir: dir,
        }
    }

    #[cfg(unix)]
    fn short_socket_path(id: &str) -> PathBuf {
        PathBuf::from(format!("/tmp/itd-{}.sock", &id[..8]))
    }

    #[cfg(not(unix))]
    fn short_socket_path(id: &str) -> PathBuf {
        std::env::temp_dir().join(format!("itd-{}.sock", &id[..8]))
    }

    #[tokio::test]
    async fn refuses_when_live_pid_holds_pidfile() {
        let config = temp_config();
        // pid 1 (init/launchd) is always alive; a signal-0 probe yields EPERM.
        std::fs::write(&config.pid_path, "1").unwrap();
        let result = acquire_single_instance(&config, false).await;
        assert!(result.is_err(), "a live pidfile owner must refuse startup");
        std::fs::remove_dir_all(&config.data_dir).ok();
    }

    #[tokio::test]
    async fn cleans_stale_pidfile_and_proceeds() {
        let config = temp_config();
        // A pid essentially guaranteed not to be running.
        std::fs::write(&config.pid_path, "2147483640").unwrap();
        let guard = acquire_single_instance(&config, false)
            .await
            .expect("startup proceeds past a stale pidfile");
        assert_eq!(
            read_pid(&config.pid_path),
            Some(std::process::id()),
            "claims the pidfile with our pid"
        );
        drop(guard);
        assert!(
            read_pid(&config.pid_path).is_none(),
            "the guard removes our pidfile on shutdown"
        );
        std::fs::remove_dir_all(&config.data_dir).ok();
    }

    #[tokio::test]
    async fn cleans_stale_socket_and_proceeds() {
        let config = temp_config();
        // A leftover socket path with nothing listening → connect refused → stale.
        std::fs::write(&config.socket_path, b"").unwrap();
        let _guard = acquire_single_instance(&config, true)
            .await
            .expect("startup proceeds past a stale socket");
        assert!(!config.socket_path.exists(), "stale socket removed");
        std::fs::remove_file(&config.socket_path).ok();
        std::fs::remove_dir_all(&config.data_dir).ok();
    }

    #[tokio::test]
    async fn refuses_when_uds_is_live() {
        let config = temp_config();
        let listener = tokio::net::UnixListener::bind(&config.socket_path).expect("bind live uds");
        let result = acquire_single_instance(&config, true).await;
        assert!(result.is_err(), "a live UDS owner must refuse startup");
        drop(listener);
        std::fs::remove_file(&config.socket_path).ok();
        std::fs::remove_dir_all(&config.data_dir).ok();
    }

    use std::sync::Mutex;

    /// A scriptable [`Signaller`] for the stop-escalation unit tests: it models
    /// process death either after N liveness polls (a "graceful" exit) or in
    /// response to SIGTERM / SIGKILL, and records which signals were sent.
    #[derive(Default)]
    struct FakeState {
        term_called: bool,
        kill_called: bool,
        polls: u32,
    }

    struct FakeSignaller {
        inner: Mutex<FakeState>,
        die_after_polls: Option<u32>,
        die_on_term: bool,
        die_on_kill: bool,
    }

    impl FakeSignaller {
        fn new(die_after_polls: Option<u32>, die_on_term: bool, die_on_kill: bool) -> Self {
            Self {
                inner: Mutex::new(FakeState::default()),
                die_after_polls,
                die_on_term,
                die_on_kill,
            }
        }
    }

    impl Signaller for FakeSignaller {
        fn is_alive(&self, _pid: u32) -> bool {
            let mut s = self.inner.lock().unwrap();
            s.polls += 1;
            if s.kill_called && self.die_on_kill {
                return false;
            }
            if s.term_called && self.die_on_term {
                return false;
            }
            if let Some(n) = self.die_after_polls {
                if s.polls > n {
                    return false;
                }
            }
            true
        }
        fn term(&self, _pid: u32) {
            self.inner.lock().unwrap().term_called = true;
        }
        fn kill(&self, _pid: u32) {
            self.inner.lock().unwrap().kill_called = true;
        }
    }

    // Tiny timeouts keep the escalation tests fast while exercising real waits.
    const GRACE: Duration = Duration::from_millis(200);
    const TERM_T: Duration = Duration::from_millis(60);
    const KILL_T: Duration = Duration::from_millis(60);
    const POLL: Duration = Duration::from_millis(2);

    #[tokio::test]
    async fn stop_already_down_when_not_alive() {
        // Dead on the very first liveness probe.
        let sig = FakeSignaller::new(Some(0), false, false);
        let outcome = escalate_stop(&sig, 123, true, GRACE, TERM_T, KILL_T, POLL).await;
        assert_eq!(outcome, StopOutcome::AlreadyDown);
        assert!(!sig.inner.lock().unwrap().term_called);
        assert!(!sig.inner.lock().unwrap().kill_called);
    }

    #[tokio::test]
    async fn stop_graceful_when_exits_before_signal() {
        // Alive for the first couple of polls, then exits during the grace wait.
        let sig = FakeSignaller::new(Some(2), false, false);
        let outcome = escalate_stop(&sig, 123, true, GRACE, TERM_T, KILL_T, POLL).await;
        assert_eq!(outcome, StopOutcome::Graceful);
        assert!(!sig.inner.lock().unwrap().term_called, "no signal needed");
    }

    #[tokio::test]
    async fn stop_escalates_to_sigterm() {
        // Never exits on its own; dies on SIGTERM. No graceful wait requested.
        let sig = FakeSignaller::new(None, true, false);
        let outcome = escalate_stop(&sig, 123, false, GRACE, TERM_T, KILL_T, POLL).await;
        assert_eq!(outcome, StopOutcome::Terminated);
        assert!(sig.inner.lock().unwrap().term_called);
        assert!(!sig.inner.lock().unwrap().kill_called);
    }

    #[tokio::test]
    async fn stop_escalates_to_sigkill() {
        // Survives SIGTERM, dies on SIGKILL.
        let sig = FakeSignaller::new(None, false, true);
        let outcome = escalate_stop(&sig, 123, false, GRACE, TERM_T, KILL_T, POLL).await;
        assert_eq!(outcome, StopOutcome::Killed);
        assert!(sig.inner.lock().unwrap().term_called);
        assert!(sig.inner.lock().unwrap().kill_called);
    }

    #[tokio::test]
    async fn stop_fails_when_process_never_dies() {
        let sig = FakeSignaller::new(None, false, false);
        let outcome = escalate_stop(&sig, 123, true, GRACE, TERM_T, KILL_T, POLL).await;
        assert_eq!(outcome, StopOutcome::Failed);
        assert!(sig.inner.lock().unwrap().term_called);
        assert!(sig.inner.lock().unwrap().kill_called);
    }
}
