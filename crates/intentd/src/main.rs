//! intentd — the Intent backend daemon and its own control client (§5.7).
//!
//! This binary is the composition root (§3.2 rule 5): it is the only place that
//! wires concrete implementations together (store → services → transport).

use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use clap::{Parser, Subcommand};
#[cfg(test)]
use intent_core::config::DEFAULT_STREAM_RETENTION_HOURS;
use intent_core::{Config, ServerControl, WorkspaceApi};
use intent_services::{
    default_process_cap, max_concurrent_agents, AgentManager, BusEventSink, EventBus, FileWatcher,
    PermissionPolicy, Services, SkillsWatcher,
};
use intent_store::Store;
use intent_transport::{
    detect_has_display, ensure_tls_certificate, generate_token, get_or_create_token,
    serve_uds_with_reverse, AsyncTokenStore, CertStatus, FileTokenStore, PrimaryReverseRegistry,
    SystemControl, SystemStatus, TokenStore, WsApiServer, WsOptions,
};
use serde_json::{json, Value};
use sqlx::Row;

mod client;
mod import;
mod service;
use client::rpc_call;

/// Global guard for the file log writer thread. Must be kept alive for the
/// process lifetime to ensure file logging continues working.
static LOG_GUARD: OnceLock<tracing_appender::non_blocking::WorkerGuard> = OnceLock::new();

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
    /// `uds` (default), `tcp` (HTTPS+WSS on 0.0.0.0:5181), or `both`. The TCP
    /// listener binds exactly that port and exits non-zero on any bind error
    /// (no port walking). `--insecure` (or `INTENTD_INSECURE=1`) serves plain
    /// `ws://` on the TCP path with no TLS and no bearer-token auth — dev only.
    Serve {
        /// Transport to listen on: `uds`, `tcp`, or `both`.
        #[arg(long, default_value = "uds")]
        listen: String,
        /// Force connection locality (§5.14): `local` or `remote`. Overrides the
        /// transport default (UDS ⇒ local, TCP/WSS ⇒ remote) for `host.status`.
        /// Omit to infer from the transport.
        #[arg(long)]
        mode: Option<String>,
        /// Dev-only: serve plain `ws://` with no TLS and no bearer-token
        /// enforcement on the TCP path. Also enabled by `INTENTD_INSECURE=1`.
        #[arg(long)]
        insecure: bool,
        /// Headless deployment: automatically resume all interrupted agents at
        /// startup instead of waiting for `agent.resolveInterrupted` RPC.
        #[arg(long)]
        resume_all: bool,
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
    /// Print WSS pairing credentials (bearer token + TLS fingerprint); --rotate
    /// regenerates the token.
    Token {
        /// Mint and persist a NEW token, replacing the old one. Ignored when
        /// `INTENTD_AUTH_TOKEN` is set (the token is fixed by the env var).
        #[arg(long)]
        rotate: bool,
    },
    /// WSAPI-1 spike: evaluate an `(async () => { <code> })()` snippet in an
    /// isolated QuickJS context with a wall-clock timeout, and print the
    /// JSON-serialized result. Present only when built with `--features js-engine`.
    #[cfg(feature = "js-engine")]
    #[command(hide = true)]
    JsEval {
        /// JavaScript source; the body of an implicit `async () => { … }`.
        code: String,
        /// Wall-clock budget in milliseconds (default 30000, matching the FE tool).
        #[arg(long, default_value_t = 30_000)]
        timeout_ms: u64,
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
    install_panic_hook();
    match Cli::parse().command {
        Command::Serve {
            listen,
            mode,
            insecure,
            resume_all,
        } => to_exit(cmd_serve(&listen, mode.as_deref(), insecure, resume_all).await),
        Command::Call { method, params } => to_exit(cmd_call(&method, params.as_deref()).await),
        Command::Status => cmd_status().await,
        Command::Stop => cmd_stop().await,
        Command::Doctor => cmd_doctor().await,
        Command::Service { action } => to_exit(cmd_service(&action)),
        Command::McpBridge { connect } => to_exit(cmd_mcp_bridge(&connect).await),
        Command::Import { from } => to_exit(cmd_import(&from).await),
        Command::Token { rotate } => to_exit(cmd_token(rotate).await),
        #[cfg(feature = "js-engine")]
        Command::JsEval { code, timeout_ms } => to_exit(cmd_js_eval(&code, timeout_ms).await),
    }
}

/// WSAPI-1 spike: run one JS snippet in a fresh QuickJS context, enforce a
/// wall-clock timeout, and print the resulting JSON to stdout. This is the
/// smoke test we point at from the PR write-up.
#[cfg(feature = "js-engine")]
async fn cmd_js_eval(code: &str, timeout_ms: u64) -> anyhow::Result<()> {
    let opts = intent_js::EvalOptions {
        timeout: Duration::from_millis(timeout_ms),
        ..intent_js::EvalOptions::default()
    };
    match intent_js::eval(code, &opts, None).await {
        Ok(v) => {
            println!("{}", serde_json::to_string(&v)?);
            Ok(())
        }
        Err(e) => Err(anyhow::Error::from(e)),
    }
}

/// Print the WSS pairing credentials (§5.2/§5.3): the bearer token a client
/// sends and the TLS cert fingerprint it pins. Resolves the token via
/// [`resolve_token_store`] (env seam ⇒ secrets file) and the fingerprint via
/// [`ensure_tls_certificate`] (the same cert `serve` reuses, so it is stable to
/// pin). `rotate` mints+persists a NEW token first — but when
/// `INTENTD_AUTH_TOKEN` is set the token is fixed by the env var and cannot be
/// rotated: a note is written to stderr and the env token is printed unchanged.
/// The token is never logged via `tracing`; both lines go to stdout.
async fn cmd_token(rotate: bool) -> anyhow::Result<()> {
    let config = resolve_config()?;
    std::fs::create_dir_all(&config.data_dir)?;
    let store = AsyncTokenStore::new(resolve_token_store());
    let env_fixed = std::env::var("INTENTD_AUTH_TOKEN")
        .map(|t| !t.is_empty())
        .unwrap_or(false);
    let token = if rotate && !env_fixed {
        generate_token(&store)
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?
    } else {
        if rotate {
            eprintln!(
                "note: INTENTD_AUTH_TOKEN is set; the token is fixed by the env var and cannot be rotated"
            );
        }
        get_or_create_token(&store)
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?
    };
    let tls =
        ensure_tls_certificate(&config.data_dir).map_err(|e| anyhow::anyhow!(e.to_string()))?;
    println!("token:       {token}");
    println!("fingerprint: {}", tls.fingerprint256);
    Ok(())
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
    use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

    // Resolve the log file path: INTENTD_DATA_DIR/intentd.log
    let log_dir = match std::env::var_os("INTENTD_DATA_DIR") {
        Some(p) => std::path::PathBuf::from(p),
        None => {
            if let Some(proj) = directories::ProjectDirs::from("", "", "intentd") {
                proj.data_dir().to_path_buf()
            } else {
                // Fallback to current directory if platform dirs unavailable
                std::path::PathBuf::from(".")
            }
        }
    };

    // Create the data directory if it doesn't exist
    if let Err(e) = std::fs::create_dir_all(&log_dir) {
        eprintln!("WARN: failed to create log directory {:?}: {}", log_dir, e);
    }

    // Set up file appender with rotation: keep ~5 files, rotate daily
    // Note: tracing-appender's max_log_files works with time-based rotation
    // (DAILY/HOURLY/etc), not size-based rotation. We use daily rotation
    // to prevent unbounded growth on long-running daemons.
    let file_appender = match tracing_appender::rolling::RollingFileAppender::builder()
        .rotation(tracing_appender::rolling::Rotation::DAILY)
        .max_log_files(5)
        .filename_prefix("intentd")
        .filename_suffix("log")
        .build(log_dir)
    {
        Ok(appender) => Some(appender),
        Err(e) => {
            eprintln!(
                "WARN: failed to create log file appender: {}, continuing with stderr-only logging",
                e
            );
            None
        }
    };

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    // Set up dual output: stderr (for interactive use) and optionally file (for diagnostics)
    let stderr_layer = fmt::layer().with_writer(std::io::stderr);

    let subscriber = tracing_subscriber::registry()
        .with(filter)
        .with(stderr_layer);

    if let Some(appender) = file_appender {
        let (non_blocking, guard) = tracing_appender::non_blocking(appender);
        let file_layer = fmt::layer().with_writer(non_blocking).with_ansi(false);
        match subscriber.with(file_layer).try_init() {
            Ok(_) => {
                // Store the guard in a static to keep it alive for the process lifetime.
                // Dropping it would stop the background file writer thread.
                let _ = LOG_GUARD.set(guard);
            }
            Err(e) => eprintln!(
                "WARN: failed to initialize tracing (already initialized?): {}",
                e
            ),
        }
    } else {
        match subscriber.try_init() {
            Ok(_) => {}
            Err(e) => eprintln!(
                "WARN: failed to initialize tracing (already initialized?): {}",
                e
            ),
        }
    }
}

/// Install a panic hook that logs the panic message and backtrace to the
/// tracing log. This ensures panic details are written to the rotating log
/// file (INTENTD_DATA_DIR/intentd.log) for post-mortem diagnosis of unexpected
/// daemon deaths. Chains the default panic hook to preserve standard Rust
/// panic formatting (thread name, etc.). The process will panic/unwind/abort
/// according to Rust's standard behavior after both hooks run.
fn install_panic_hook() {
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |panic_info| {
        let backtrace = std::backtrace::Backtrace::force_capture();

        let location = panic_info
            .location()
            .map(|l| format!("{}:{}:{}", l.file(), l.line(), l.column()))
            .unwrap_or_else(|| "unknown location".to_string());

        let message = if let Some(s) = panic_info.payload().downcast_ref::<&str>() {
            s.to_string()
        } else if let Some(s) = panic_info.payload().downcast_ref::<String>() {
            s.clone()
        } else {
            "unknown panic payload".to_string()
        };

        tracing::error!(
            location = %location,
            message = %message,
            backtrace = %backtrace,
            "PANIC: daemon panicked"
        );

        // Also write to stderr so it's visible in immediate context
        eprintln!("PANIC at {}: {}", location, message);
        eprintln!("Backtrace:\n{}", backtrace);

        // Chain the default hook to preserve standard Rust panic formatting
        default_hook(panic_info);
    }));
}

fn resolve_config() -> anyhow::Result<Config> {
    Config::resolve().map_err(|e| anyhow::anyhow!(e.to_string()))
}

async fn cmd_serve(
    listen: &str,
    mode: Option<&str>,
    insecure: bool,
    resume_all: bool,
) -> anyhow::Result<()> {
    let (serve_uds_enabled, serve_tcp_enabled) = match listen {
        "uds" => (true, false),
        "tcp" => (false, true),
        "both" => (true, true),
        other => anyhow::bail!("unsupported --listen '{other}'; expected uds|tcp|both"),
    };
    // Insecure dev mode: `--insecure` OR `INTENTD_INSECURE=1` disables TLS and
    // bearer-token enforcement on the TCP path (plain `ws://`), and skips cert
    // provisioning entirely. Dev-only; loudly warned at startup.
    let insecure = insecure || env_flag("INTENTD_INSECURE");
    // Resolve the optional locality override (§5.14): `--mode local|remote`
    // forces the value reported over `host.status` regardless of transport;
    // absent ⇒ infer from the transport (UDS local, TCP/WSS remote).
    let locality_override = parse_locality_mode(mode)?;
    let config = resolve_config()?;
    std::fs::create_dir_all(&config.data_dir)?;
    // CoW isolation startup probe: probe the workspaces root once at daemon startup
    // so the result is cached for future cow_probe calls. This runs before the
    // store/services are initialized so the cache is ready when sandbox provisioning
    // needs it. The probe result is also reported by `intentd doctor`.
    probe_cow_at_startup(&config);
    // OS-level single-instance backstop (§5.6): hold an exclusive advisory lock
    // on `data_dir/intentd.lock` for the whole process. Acquired before the
    // socket/pidfile guard so the strongest, configuration-independent guard
    // fires first; released automatically when the held fd closes on drop.
    let _datadir_lock = acquire_data_dir_lock(&config)?;
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
    // Hold a store handle for the §5.4 idempotency reaper (same lifecycle root
    // as the retention sweep) before the store is moved into the services below.
    let idempotency_store = store.clone();
    // REV-1: build the shared first-client-sticky reverse-dispatch registry
    // BEFORE the services surface + listeners so both sides observe the same
    // live-client set. Every accepted UDS/WSS connection registers its
    // per-connection `ReverseChannel` here; agent-initiated `browser.exec`
    // routes through the same registry via `Services::with_reverse_dispatch`.
    let reverse_registry = Arc::new(PrimaryReverseRegistry::new());
    // Resolve the concurrent agent cap: positive stored value → explicit override;
    // 0/unset/invalid → auto (RAM-based default_process_cap). The setting applies
    // on daemon restart (§9.8 agents.maxConcurrent). Must read before the store
    // is moved into `services`.
    let process_cap = max_concurrent_agents(&store)
        .await
        .unwrap_or_else(default_process_cap);
    // The services surface publishes CRUD change events onto the same bus that
    // transport subscriptions read, so a mutation on one connection streams to
    // subscribers on another (§10).
    let services = Services::new(store)
        .with_assets_root(config.data_dir.join("assets"))
        .with_event_bus(bus.clone())
        .with_reverse_dispatch(reverse_registry.clone());
    // The AgentManager multiplexes spawned agent processes over the ACP client
    // (§6.8). Its concrete EventSink bridges the client-served fs/permission
    // events (M3.5) onto the same bus, and `run_turn` drives the streaming
    // router (M3.4); a global process cap + LRU registry bound concurrency.
    // The shipped default is `AllowAll` for reference parity with the TS
    // acp-provider: the manager first tries `session/set_mode bypassPermissions`
    // on providers that advertise it (auggie today) and then unconditionally
    // auto-approves any `session/request_permission` the provider still sends.
    // The previous `AutoByRisk` default silently denied medium/high prompts,
    // which diverged from the reference. An FE-attached deployment sets
    // `INTENTD_PERMISSION_POLICY=interactive` to surface every prompt over
    // `agent.pendingPermissions` and resolve it via `agent.respondPermission`;
    // `auto` / `deny` remain selectable for headless-with-guardrails deployments
    // (§6.7/M3.5).
    let permission_policy = resolve_permission_policy();
    tracing::info!(?permission_policy, "agent permission policy");
    let manager = Arc::new(
        AgentManager::new(
            services.clone(),
            Arc::new(BusEventSink::new(bus.clone())),
            process_cap,
        )
        .with_policy(permission_policy)
        // STAB-53: capture each spawned child's stderr under
        // `<data_dir>/agent-logs/<agent-id>/<YYYY-MM-DD>.log`.
        .with_agent_log_root(intent_core::agent_logs_root(&config.data_dir)),
    );
    // Attach the manager to the services surface so the `agent.*` RPC handlers
    // drive the live spawn/turn/MCP loop at runtime (the shared `OnceLock` is
    // visible to every clone, including the api handed to the transport below).
    services.attach_agent_manager(&manager);
    tracing::info!(
        process_cap = manager.registry().cap(),
        "agent manager ready"
    );
    // Wave B: unconditionally reset all is_active=1 rows (ACP sessions cannot
    // survive a daemon restart — they are process-local). Any is_active=1 flag
    // after boot is stale. This runs BEFORE heal_stale_agent_sessions so the
    // stale-status heal sees is_active=0 rows across the board.
    match services.store().reset_all_active_flags().await {
        Ok(0) => {}
        Ok(reset) => tracing::warn!(reset, "reset stale is_active=1 flags on startup"),
        Err(e) => tracing::warn!(error = %e, "is_active reset failed"),
    }
    // Heal stale in-flight conversations from any prior crash BEFORE the chat
    // subscription path can observe them (iter#1c). Sessions left in an
    // active status (`Active`/`Processing`/`Waiting`) without a live worker
    // would otherwise drive the FE's `isActiveAgentThread` selector and
    // surface a phantom "Thinking" indicator. Best-effort: a failure is
    // logged but never aborts startup.
    match services.heal_stale_agent_sessions().await {
        Ok(0) => {}
        Ok(healed) => tracing::info!(healed, "healed stale in-flight agent sessions on startup"),
        Err(e) => tracing::warn!(error = %e, "stale agent session heal sweep failed"),
    }
    // Hydrate the script registry from the persisted definitions (§5.8) so
    // `script.*` survives daemon restarts. Best-effort: a failure is logged
    // but never aborts startup (scripts can still be re-created live).
    match services.hydrate_scripts().await {
        Ok(0) => {}
        Ok(loaded) => tracing::info!(loaded, "hydrated persisted script definitions"),
        Err(e) => tracing::warn!(error = %e, "script registry hydration failed"),
    }
    // Background PR refresh (§7.6): periodically re-fetch every linked PR,
    // persist any change, and emit `pr:*` events so clients update without
    // polling. Safe when source control is unconfigured (each refresh logs and
    // swallows the missing-provider error). Aborted on clean shutdown.
    let pr_refresh = services.spawn_pr_refresh_loop(std::time::Duration::from_secs(60));
    // Daemon-internal token-usage scan (§5.23/§19.1): periodically re-tally each
    // workspace's per-agent/per-model token usage, persist the durable
    // `tokenUsage` field, and emit `workspace:tokenUsage-changed` on deltas.
    // There is no scan RPC. Aborted on clean shutdown.
    let token_usage_scan = services.spawn_token_usage_scan_loop(std::time::Duration::from_secs(60));
    // Completion-delivery worker (AS-3): wake parents holding a oneShot
    // completion watch when their delegated child finishes. No-op-safe without
    // an event bus. Held for the process lifetime and aborted on clean shutdown.
    let completion_delivery = services.spawn_completion_delivery_loop();
    // Auto-commit-on-idle worker (LNI-1, §5.6): subscribe to `agent:idle` and
    // commit each task-linked agent's changes with `Agent-Id:` and
    // `Linked-Note-Id:` trailers via `git_agent_commit`. No-op-safe without an
    // event bus. Aborted on clean shutdown.
    let auto_commit_loop = services.spawn_auto_commit_loop();
    // CRDT session sweeper (A5, §5.2 CRDT): every hour, drop cached yrs docs
    // for `(workspace, note)` pairs whose last access is older than 24h so
    // long-lived daemons do not accumulate per-note session state. Aborted on
    // clean shutdown.
    let crdt_session_sweep = services.spawn_crdt_session_sweep_loop();
    // Idle agent reaping (§5.6/§6.7): periodically evict agents idle past the
    // configured TTL, killing each one's whole process group. Disabled entirely
    // when `agents.idleReapMinutes == 0`.
    let reap_task = spawn_idle_reap_loop(manager.clone(), config.idle_reap_minutes);
    // Event retention/compaction (§10.2 / finding F4): periodically delete
    // high-volume ephemeral events (`agent:stream:*`, `file:*`, `terminal:data`,
    // `host:exec:*`) older than the configured TTL, preserving lifecycle/tool/
    // note/task/workspace events. Disabled when `events.streamRetentionHours == 0`.
    let retention_task =
        spawn_stream_retention_loop(retention_store, config.stream_retention_hours);
    // Idempotency-key reaper (§5.4): hourly sweep deleting dedupe rows older than
    // 24h so the `idempotency_key` table stays bounded. The same cadence prunes
    // per-agent stderr capture files older than 7 days (STAB-53); the first tick
    // fires immediately so both sweeps run on startup. Aborted on clean shutdown.
    let idempotency_reap_task = spawn_idempotency_reap_loop(
        idempotency_store,
        intent_core::agent_logs_root(&config.data_dir),
    );
    // External MCP servers (§18.3): start every enabled, non-disabled server,
    // then run the health monitor (periodic ping + auto-restart pushing
    // `mcp.servers:status-changed`). The hub is reaped on shutdown so no orphan
    // server processes remain (PTY-host reaping parity).
    services.start_enabled_mcp_servers().await;
    let mcp_hub = services.mcp_hub();
    let mcp_monitor = mcp_hub.spawn_health_monitor();

    // Build api Arc early so it can be cloned for runtime control (§5.12).
    // ServerControl is attached after DaemonControl is built via the OnceLock seam.
    let api: Arc<dyn WorkspaceApi> = Arc::new(services.clone());
    // Start a filesystem watcher per active workspace with a resolvable on-disk
    // path; each publishes debounced `file:changed` events to the shared bus.
    // The handles are held for the lifetime of `serve` and torn down on return.
    let _watchers = start_workspace_watchers(&bus, api.as_ref()).await;

    // Start skills directory watchers (user-tier + project-tier per workspace).
    // Publishes debounced `skills:changed` events when SKILL.md files are modified.
    let _skills_watcher = start_skills_watcher(&bus, api.as_ref()).await;

    // Prepare runtime control for the HTTPS+WSS listener (§5.12). Build the
    // construction args ALWAYS (regardless of --listen mode) so settings can
    // toggle the listener on/off at runtime. Boot-time auto-start of the listener
    // remains ONLY for --listen tcp/both (CLI/env win over persisted settings).
    // With --listen uds: listener does NOT auto-start at boot (regardless of
    // persisted server.wsApi.enabled), but settings.update server.wsApi.enabled=true
    // can start it at runtime (TLS + bearer auth on, same as any TCP listener unless
    // --insecure). Note: persisted settings are NOT honored at boot for any mode —
    // only CLI --listen and env (INTENTD_TCP_PORT) matter.
    let mut ws_options = ws_options_from_env();
    ws_options.locality_override = locality_override;

    // TLS + bearer auth: provision the cert (lazy; cert stays on disk) + build
    // the token store for auth layers (§5.2/§5.3). Always provision for runtime
    // toggle, even under --listen uds (listener can be started later via settings).
    let (tls_cert, token_store) = if insecure {
        tracing::warn!(
            "intentd INSECURE dev mode: TLS disabled, bearer-token auth disabled, plain ws:// on {}:{} — do NOT use outside local dev",
            ws_options.bind_address,
            ws_options.base_port
        );
        (None, None)
    } else {
        let tls =
            ensure_tls_certificate(&config.data_dir).map_err(|e| anyhow::anyhow!(e.to_string()))?;
        let async_token_store = Arc::new(AsyncTokenStore::new(resolve_token_store()));
        get_or_create_token(&async_token_store)
            .await
            .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        (Some(tls), Some(async_token_store))
    };

    // Build runtime control struct (always, regardless of --listen mode)
    let runtime = Arc::new(WsRuntimeControl {
        api: api.clone(),
        bus: bus.clone(),
        tls_cert: tls_cert.clone(),
        token_store: token_store.clone(),
        ws_options: ws_options.clone(),
        reverse_registry: reverse_registry.clone(),
        data_dir: config.data_dir.clone(),
        state: tokio::sync::Mutex::new(WsRuntimeState {
            ws_server: None,
            port: None,
        }),
        control: std::sync::OnceLock::new(),
    });

    // System control surface (§5.7 + §5.12): exposes `system.status` /
    // `system.shutdown` to local UDS clients plus runtime WSS listener control.
    // The `Notify` lets the `system.shutdown` RPC trigger the same graceful
    // teardown as an OS signal, so `stop` can ask politely before escalating.
    // Must be built BEFORE the WSS server so it can be passed to the constructor.
    let shutdown_notify = Arc::new(tokio::sync::Notify::new());
    let control = Arc::new(DaemonControl {
        listen_mode: listen.to_string(),
        uds: serve_uds_enabled,
        tcp: serve_tcp_enabled,
        manager: manager.clone(),
        shutdown: shutdown_notify.clone(),
        ws_runtime: runtime.clone(),
        start_time: std::time::Instant::now(),
    });

    // Populate the runtime control OnceLock so runtime-toggled WSS listeners can
    // serve system.status (§5.7). This breaks the circular Arc dependency between
    // DaemonControl and WsRuntimeControl.
    if runtime.control.set(control.clone()).is_err() {
        panic!("control OnceLock should only be set once");
    }

    // Boot-time auto-start of the listener ONLY when --listen tcp/both
    // (CLI --listen wins over persisted settings)
    let (_ws_server, _ws_port) = if serve_tcp_enabled {
        let system_control: Arc<dyn SystemControl> = control.clone();
        let mut server = if insecure {
            WsApiServer::new_insecure_with_reverse(
                api.clone(),
                bus.clone(),
                ws_options.clone(),
                reverse_registry.clone(),
                Some(system_control.clone()),
            )
        } else {
            WsApiServer::new_with_reverse(
                api.clone(),
                bus.clone(),
                tls_cert.as_ref().unwrap(),
                token_store.clone().unwrap(),
                ws_options.clone(),
                reverse_registry.clone(),
                Some(system_control.clone()),
            )
            .map_err(|e| anyhow::anyhow!(e.to_string()))?
        };

        // Install pairing info provider on the server (§5.2) if in secure mode
        if !insecure {
            let pairing_provider = Arc::new(DaemonPairingInfo {
                data_dir: config.data_dir.clone(),
                token_store: token_store.clone().unwrap(),
                ws_runtime: runtime.clone(),
            });
            server.install_pairing_info(pairing_provider);
        }

        let port = server.start().await?;
        match server.fingerprint() {
            Some(fp) => tracing::info!(port, fingerprint = %fp, "intentd WSS listening"),
            None => tracing::info!(port, "intentd WS listening (insecure, no TLS)"),
        }

        // Store the server in runtime state
        {
            let mut state = runtime.state.lock().await;
            state.ws_server = Some(server.clone());
            state.port = Some(port);
        }

        (Some(server), Some(port))
    } else {
        (None, None)
    };

    // Wire ServerControl to Services for settings-driven runtime control (§5.12).
    // The control is attached after the api Arc is built via the `OnceLock` seam.
    let server_control: Arc<dyn intent_core::ServerControl> = control.clone();
    services.attach_server_control(server_control);

    // Build pairing info provider for `server.pairingInfo` / `server.rotateToken` (§5.2).
    // Only built when there's a token store (secure mode); `None` in insecure mode.
    // Available to UDS clients even when TCP is disabled (they can still call the RPCs).
    let pairing_info: Option<Arc<dyn intent_transport::ServerPairingInfo>> =
        if let Some(ref ts) = token_store {
            // Share the same AsyncTokenStore instance as the WSS listener
            // so rotations propagate to the live auth layer.
            Some(Arc::new(DaemonPairingInfo {
                data_dir: config.data_dir.clone(),
                token_store: ts.clone(),
                ws_runtime: runtime.clone(),
            }))
        } else {
            None
        };

    let shutdown = {
        let notify = shutdown_notify.clone();
        async move {
            tokio::select! {
                _ = shutdown_signal() => {}
                _ = notify.notified() => tracing::info!("shutdown requested via system.shutdown"),
            }
        }
    };

    // Auto-resume interrupted agents when --resume-all is set (headless deployment).
    // Spawn in the background so it doesn't block startup; log failures per-agent.
    if resume_all {
        let services_clone = services.clone();
        tokio::spawn(async move {
            tracing::info!("--resume-all: enumerating interrupted agents");
            // List all pending interrupted agents
            let rows = match services_clone.store().list_interrupted_agents().await {
                Ok(r) => r,
                Err(e) => {
                    tracing::error!(error = %e, "--resume-all: failed to list interrupted agents");
                    return;
                }
            };
            if rows.is_empty() {
                tracing::info!("--resume-all: no interrupted agents to resume");
                return;
            }
            tracing::info!(
                count = rows.len(),
                "--resume-all: resuming interrupted agents"
            );
            let mut resumed = Vec::new();
            let mut failed = Vec::new();
            // Resume each agent using the same service operation as agent.resolveInterrupted
            for interrupted in rows {
                let agent_id = interrupted.agent_id.clone();
                match services_clone.resume_interrupted_agent(&agent_id).await {
                    Ok(()) => {
                        tracing::info!(
                            agent_id = %agent_id,
                            workspace = %interrupted.workspace_id,
                            "--resume-all: resumed agent"
                        );
                        resumed.push(agent_id.0);
                    }
                    Err(e) => {
                        tracing::warn!(
                            agent_id = %agent_id,
                            error = %e,
                            "--resume-all: failed to resume agent"
                        );
                        failed.push((agent_id.0, e.to_string()));
                    }
                }
            }
            tracing::info!(
                resumed = resumed.len(),
                failed = failed.len(),
                "--resume-all: auto-resume sweep complete"
            );
        });
    }

    if serve_uds_enabled {
        tracing::info!(socket = %config.socket_path.display(), "starting intentd");
        let system_control: Arc<dyn SystemControl> = control.clone();
        serve_uds_with_reverse(
            api,
            bus,
            &config.socket_path,
            Some(system_control),
            pairing_info,
            reverse_registry.clone(),
            shutdown,
        )
        .await?;
    } else {
        // TCP-only: no local control transport, but the shutdown notify is still
        // wired so a future control path could trigger it. Wait for a signal.
        let _ = control;
        shutdown.await;
    }

    // Clean shutdown: stop the WSS listener (graceful close + port release),
    // stop the PR refresh loop, then kill every spawned agent child and clear
    // the registry (§6.8 teardown). Idle reaping during the run is the M5
    // `reap_idle` hook. Stop via ServerControl so we stop the runtime listener
    // (ws_runtime.state.ws_server), not the stale boot-time ws_server variable.
    control.stop_ws_listener().await;
    pr_refresh.abort();
    token_usage_scan.abort();
    completion_delivery.abort();
    auto_commit_loop.abort();
    crdt_session_sweep.abort();
    if let Some(reap_task) = reap_task {
        reap_task.abort();
    }
    if let Some(retention_task) = retention_task {
        retention_task.abort();
    }
    idempotency_reap_task.abort();
    // Stop the MCP health monitor and reap every external MCP server's process
    // group so no orphan stdio servers survive the daemon (§18.3).
    mcp_monitor.abort();
    mcp_hub.shutdown().await;
    manager.shutdown().await;
    Ok(())
}

/// Live daemon control surface backing `system.status` / `system.shutdown`
/// (§5.7) plus runtime WSS listener control (§5.12). Built post-bind so the
/// resolved WSS `port`/`fingerprint` are real (not guessed); `client_count`/agent
/// count are read live on each status call. The runtime fields (`ws_server`,
/// `ws_runtime`) allow settings-driven start/stop without daemon restart.
struct DaemonControl {
    listen_mode: String,
    uds: bool,
    tcp: bool,
    manager: Arc<AgentManager>,
    shutdown: Arc<tokio::sync::Notify>,
    /// Runtime state for settings-driven listener control (§5.12). Holds the
    /// WsApiServer construction args so `start_ws_listener` can build a fresh
    /// server when toggled on. Always present (constructed regardless of --listen
    /// mode so runtime toggle works for all modes, including --listen uds).
    ws_runtime: Arc<WsRuntimeControl>,
    /// Daemon start time (Instant) for uptime calculation.
    start_time: std::time::Instant,
}

/// Runtime control for the WSS listener, shared between DaemonControl and
/// the lifecycle hooks (§5.12). Holds WsApiServer construction args plus mutable
/// state guarded by a Mutex so settings.update can start/stop the listener.
struct WsRuntimeControl {
    api: Arc<dyn WorkspaceApi>,
    bus: EventBus,
    tls_cert: Option<intent_transport::TlsCertificate>,
    token_store: Option<Arc<AsyncTokenStore>>,
    ws_options: WsOptions,
    reverse_registry: Arc<PrimaryReverseRegistry>,
    /// Data directory for building pairing info provider (§5.2) in start_ws_listener.
    data_dir: PathBuf,
    /// Mutable runtime state: the live WsApiServer (when started).
    state: tokio::sync::Mutex<WsRuntimeState>,
    /// System control surface (§5.7) for system.status/shutdown over WSS. Set via
    /// OnceLock after DaemonControl construction to break circular Arc dependency.
    control: std::sync::OnceLock<Arc<dyn SystemControl>>,
}

struct WsRuntimeState {
    ws_server: Option<WsApiServer>,
    /// Cached port for sync system.status access
    port: Option<u16>,
}

/// Pairing info provider for `server.pairingInfo` / `server.rotateToken` (§5.2).
/// Implemented by the daemon composition root and wired to UDS and WSS listeners.
struct DaemonPairingInfo {
    data_dir: PathBuf,
    token_store: Arc<AsyncTokenStore>,
    /// Runtime control reference to read the current bound port. Always present
    /// (WsRuntimeControl is constructed for all listen modes; the listener is
    /// auto-started at boot only for --listen tcp/both, but can be started at
    /// runtime for all modes including --listen uds).
    ws_runtime: Arc<WsRuntimeControl>,
}

impl intent_transport::ServerPairingInfo for DaemonPairingInfo {
    fn pairing_snapshot(
        &self,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = intent_transport::PairingSnapshot> + Send + '_>,
    > {
        Box::pin(async move {
            let state = self.ws_runtime.state.lock().await;
            intent_transport::PairingSnapshot { port: state.port }
        })
    }

    fn data_dir(&self) -> &std::path::Path {
        &self.data_dir
    }

    fn token_store(&self) -> &AsyncTokenStore {
        &self.token_store
    }
}

impl SystemControl for DaemonControl {
    fn status(&self) -> SystemStatus {
        // Read live port/fingerprint/client count from runtime state (§5.12 fix).
        // Use try_lock to avoid blocking; if locked, report as unavailable.
        let (port, fingerprint, clients) = if let Ok(state) = self.ws_runtime.state.try_lock() {
            let port = state.port;
            let fingerprint = state
                .ws_server
                .as_ref()
                .and_then(|s| s.fingerprint().map(str::to_string));
            let clients = state
                .ws_server
                .as_ref()
                .map(|s| s.client_count())
                .unwrap_or(0);
            (port, fingerprint, clients)
        } else {
            (None, None, 0)
        };

        SystemStatus {
            listen_mode: self.listen_mode.clone(),
            uds: self.uds,
            tcp: self.tcp,
            port,
            clients,
            agents: self.manager.registry().size(),
            fingerprint,
            os: std::env::consts::OS.to_string(),
            arch: std::env::consts::ARCH.to_string(),
            has_display: detect_has_display(),
            max_agents: self.manager.registry().cap(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            uptime_seconds: self.start_time.elapsed().as_secs(),
        }
    }

    fn request_shutdown(&self) {
        // `notify_one` stores a permit if the serve loop is not yet awaiting, so
        // the shutdown is never lost to a race with a freshly-arrived RPC.
        self.shutdown.notify_one();
    }
}

impl intent_core::ServerControl for DaemonControl {
    fn start_ws_listener(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = intent_core::Result<u16>> + Send + '_>>
    {
        Box::pin(async move {
            let runtime = &self.ws_runtime;

            // Check if already running (don't hold lock across await)
            let existing_server = {
                let state = runtime.state.lock().await;
                state.ws_server.clone()
            };

            // If already started, return the current port (idempotent)
            if let Some(ref server) = existing_server {
                if let Some(port) = server.bound_port().await {
                    return Ok(port);
                }
            }

            // Read the persisted port from settings (fall back to env/default)
            let desired_port = match runtime
                .api
                .settings_get("server.wsApi.port".to_string())
                .await
            {
                Ok(result) => {
                    result
                        .get("value")
                        .and_then(|v| v.as_f64())
                        .map(|p| p as u16)
                        .unwrap_or_else(|| {
                            // Fall back to env INTENTD_TCP_PORT / default 5181
                            std::env::var("INTENTD_TCP_PORT")
                                .ok()
                                .and_then(|v| v.trim().parse::<u16>().ok())
                                .unwrap_or(runtime.ws_options.base_port)
                        })
                }
                Err(_) => {
                    // Fall back to env INTENTD_TCP_PORT / default 5181
                    std::env::var("INTENTD_TCP_PORT")
                        .ok()
                        .and_then(|v| v.trim().parse::<u16>().ok())
                        .unwrap_or(runtime.ws_options.base_port)
                }
            };

            // Clone ws_options and override the port
            let mut ws_options = runtime.ws_options.clone();
            ws_options.base_port = desired_port;

            // Build a fresh WsApiServer and start it. The control is populated via
            // OnceLock after DaemonControl construction (breaking the circular Arc).
            let system_control: Option<Arc<dyn SystemControl>> = runtime.control.get().cloned();
            let mut server = if let Some(ref tls) = runtime.tls_cert {
                // Secure mode
                let token_store = runtime
                    .token_store
                    .as_ref()
                    .ok_or_else(|| {
                        intent_core::Error::Internal(
                            "token_store missing in secure mode".to_string(),
                        )
                    })?
                    .clone();
                WsApiServer::new_with_reverse(
                    runtime.api.clone(),
                    runtime.bus.clone(),
                    tls,
                    token_store.clone(),
                    ws_options,
                    runtime.reverse_registry.clone(),
                    system_control.clone(),
                )
                .map_err(|e| intent_core::Error::Internal(e.to_string()))?
            } else {
                // Insecure mode
                WsApiServer::new_insecure_with_reverse(
                    runtime.api.clone(),
                    runtime.bus.clone(),
                    ws_options,
                    runtime.reverse_registry.clone(),
                    system_control.clone(),
                )
            };

            // Install pairing info provider (§5.2) on runtime-started servers
            if let Some(ref ts) = runtime.token_store {
                let pairing_provider = Arc::new(DaemonPairingInfo {
                    data_dir: runtime.data_dir.clone(),
                    token_store: ts.clone(),
                    ws_runtime: self.ws_runtime.clone(),
                })
                    as Arc<dyn intent_transport::ServerPairingInfo>;
                server.install_pairing_info(pairing_provider);
            }

            let port = server.start().await.map_err(|e| {
                // Map bind failures to friendly, actionable error messages
                let error_kind = e.kind();
                let error_msg = if error_kind == std::io::ErrorKind::AddrInUse {
                    format!(
                        "Port {} is already in use — choose a different port or stop the process using it",
                        desired_port
                    )
                } else {
                    // Other bind errors: include the port and OS error text
                    format!("failed to bind port {}: {}", desired_port, e)
                };
                intent_core::Error::Internal(error_msg)
            })?;

            // Store server + port (acquire lock only after all awaits done)
            {
                let mut state = runtime.state.lock().await;
                state.ws_server = Some(server);
                state.port = Some(port);
            }

            Ok(port)
        })
    }

    fn stop_ws_listener(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
        Box::pin(async move {
            let runtime = &self.ws_runtime;
            // Extract server without holding lock across await
            let server = {
                let mut state = runtime.state.lock().await;
                state.port = None;
                state.ws_server.take()
            };

            // Stop the WS server
            if let Some(s) = server {
                s.stop().await;
            }
        })
    }

    fn ws_listener_port(
        &self,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<u16>> + Send + '_>> {
        Box::pin(async move {
            let runtime = &self.ws_runtime;
            // Extract server without holding lock across await
            let server = {
                let state = runtime.state.lock().await;
                state.ws_server.clone()
            };
            if let Some(ref s) = server {
                s.bound_port().await
            } else {
                None
            }
        })
    }

    fn is_tcp_connection(&self) -> bool {
        // Read from task-local connection context set by the transport layer.
        // Returns true for TCP (WSS) connections, false for UDS or when called
        // outside a request context.
        intent_transport::is_tcp_connection()
    }
}

/// Fixed-token [`TokenStore`] selected only when `INTENTD_AUTH_TOKEN` is set.
/// TEST-ONLY SEAM (§13.1 E2E): lets the E2E suite authenticate a real `intentd
/// serve --listen tcp/both` daemon hermetically, without touching the shared
/// secrets file. Production always uses [`FileTokenStore`].
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
/// set (test-only hermetic seam, §13.1), otherwise the file-backed secrets
/// store.
fn resolve_token_store() -> Arc<dyn TokenStore> {
    match std::env::var("INTENTD_AUTH_TOKEN") {
        Ok(t) if !t.is_empty() => Arc::new(EnvTokenStore(t)),
        _ => Arc::new(FileTokenStore::default()),
    }
}

/// Build [`WsOptions`] from the production defaults plus an optional env seam:
/// an explicit base port (`INTENTD_TCP_PORT`, `0` = OS-assigned ephemeral).
/// A §13.1 E2E seam: keeps the suite hermetic (no fixed-5180 contention).
fn ws_options_from_env() -> WsOptions {
    let mut opts = WsOptions::default();
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
/// override is applied to the TCP/WSS listener (`host.status`); the local
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

/// Map the `INTENTD_PERMISSION_POLICY` value to a [`PermissionPolicy`]
/// (`interactive`|`auto`|`allow`|`deny`, case-insensitive). Absent/blank or an
/// unrecognized value falls back to `AllowAll` — reference parity with the TS
/// acp-provider, which unconditionally auto-approves (`AutoByRisk`'s
/// silent-deny of medium/high prompts diverged from that behavior).
/// `interactive` remains available via this env var and is what an FE-attached
/// deployment selects to drive the `agent.respondPermission` /
/// `agent.pendingPermissions` round-trip.
fn parse_permission_policy(raw: Option<&str>) -> PermissionPolicy {
    match raw.map(str::trim).map(str::to_ascii_lowercase).as_deref() {
        Some("interactive") => PermissionPolicy::Interactive,
        Some("auto") => PermissionPolicy::AutoByRisk,
        Some("allow") => PermissionPolicy::AllowAll,
        Some("deny") => PermissionPolicy::DenyAll,
        _ => PermissionPolicy::AllowAll,
    }
}

/// Resolve the permission policy from `INTENTD_PERMISSION_POLICY`, defaulting to
/// `AllowAll` when unset or unrecognized (see [`parse_permission_policy`]).
fn resolve_permission_policy() -> PermissionPolicy {
    parse_permission_policy(std::env::var("INTENTD_PERMISSION_POLICY").ok().as_deref())
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

/// RAII exclusive `flock` on `data_dir/intentd.lock`, held for the daemon's
/// lifetime as an OS-level single-instance backstop (§5.6) independent of the
/// socket/pidfile. The kernel releases the advisory lock when the held file
/// descriptor closes on drop. The lockfile itself is intentionally NOT removed
/// on shutdown: a stale lockfile is harmless (only a live holder's lock blocks a
/// second instance), and removing it would race a concurrent acquirer.
#[cfg(unix)]
struct DataDirLock {
    _lock: nix::fcntl::Flock<std::fs::File>,
}

#[cfg(not(unix))]
struct DataDirLock;

/// Acquire the data-dir lock (§5.6): open/create `data_dir/intentd.lock` and take
/// a non-blocking exclusive advisory `flock`. On contention another live instance
/// already holds the lock, so refuse to start.
#[cfg(unix)]
fn acquire_data_dir_lock(config: &Config) -> anyhow::Result<DataDirLock> {
    use nix::fcntl::{Flock, FlockArg};
    let lock_path = config.data_dir.join("intentd.lock");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(&lock_path)
        .map_err(|e| anyhow::anyhow!("open data-dir lockfile {}: {e}", lock_path.display()))?;
    match Flock::lock(file, FlockArg::LockExclusiveNonblock) {
        Ok(lock) => Ok(DataDirLock { _lock: lock }),
        Err((_, errno)) => anyhow::bail!(
            "intentd data dir {} is locked by another running instance ({errno}) — refusing to start a second instance",
            config.data_dir.display()
        ),
    }
}

/// Non-unix has no `flock`; the lock is a no-op success (the socket/pidfile
/// guards remain the single-instance enforcement on those platforms).
#[cfg(not(unix))]
fn acquire_data_dir_lock(_config: &Config) -> anyhow::Result<DataDirLock> {
    Ok(DataDirLock)
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

/// Spawn the periodic event-retention/compaction sweep (§10.2 / finding F4),
/// or `None` when disabled (`stream_retention_hours == 0`). Each tick deletes
/// high-volume ephemeral events (`agent:stream:*`, `file:*`, `terminal:data`,
/// `host:exec:*`) older than the TTL while preserving lifecycle/tool/note/task/
/// workspace events. The sweep interval is derived from the TTL (≈4×/TTL),
/// clamped so long TTLs still sweep periodically and short ones do not busy-loop.
/// A failed sweep is logged and retried on the next tick (never aborts the loop).
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
        "event retention sweep enabled (agent:stream:*, file:*, terminal:data, host:exec:*)"
    );
    Some(tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let cutoff = intent_core::iso_minutes_ago(stream_retention_hours as i64 * 60);
            match store.delete_ephemeral_events_before(&cutoff).await {
                Ok(removed) if removed > 0 => {
                    tracing::info!(
                        removed,
                        cutoff,
                        "event retention sweep trimmed ephemeral events"
                    );
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "event retention sweep failed"),
            }
        }
    }))
}

/// Spawn the periodic idempotency-key reaper (design note TB-0 §5.4). Runs
/// ~hourly: deletes `idempotency_key` rows whose `created_at` is older than 24h
/// (via `idx_idempotency_created`), bounding the dedupe store. The same tick
/// prunes per-agent stderr capture files older than 7 days under
/// `agent_log_root` (STAB-53). The first tick fires immediately so a long-lived
/// daemon trims on startup; a failed sweep is logged and retried on the next
/// tick (never aborts the loop).
fn spawn_idempotency_reap_loop(
    store: Store,
    agent_log_root: std::path::PathBuf,
) -> tokio::task::JoinHandle<()> {
    const RETENTION_HOURS: i64 = 24;
    let interval = Duration::from_secs(3600);
    tracing::info!(
        retention_hours = RETENTION_HOURS,
        interval_secs = interval.as_secs(),
        "idempotency reaper enabled"
    );
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let cutoff = intent_core::iso_minutes_ago(RETENTION_HOURS * 60);
            match store.reap_idempotent(&cutoff).await {
                Ok(removed) if removed > 0 => {
                    tracing::info!(removed, cutoff, "idempotency reaper trimmed dedupe keys");
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "idempotency reaper sweep failed"),
            }
            let sweep_result = tokio::task::spawn_blocking({
                let root = agent_log_root.clone();
                move || {
                    intent_core::sweep_agent_logs(
                        &root,
                        Duration::from_secs(intent_core::AGENT_LOG_RETENTION_DAYS * 86_400),
                    )
                }
            })
            .await;

            match sweep_result {
                Ok(Ok(removed)) if removed > 0 => {
                    tracing::info!(removed, "agent stderr log sweep pruned old capture files");
                }
                Ok(Ok(_)) => {}
                Ok(Err(e)) => tracing::warn!(error = %e, "agent stderr log sweep failed"),
                Err(e) => tracing::warn!(error = %e, "agent stderr log sweep task failed"),
            }
        }
    })
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

/// Start a [`SkillsWatcher`] covering all skills directories (user-tier + project-tier
/// per workspace). Returns the live handle; dropping it stops the watcher.
async fn start_skills_watcher(
    bus: &EventBus,
    services: &dyn WorkspaceApi,
) -> Option<SkillsWatcher> {
    let workspaces = match services.list_workspaces(false).await {
        Ok(ws) => ws,
        Err(e) => {
            tracing::warn!(error = %e, "could not list workspaces for skills watching");
            return None;
        }
    };

    let workspace_pairs: Vec<_> = workspaces
        .into_iter()
        .filter_map(|ws| {
            let root = ws.path.clone().or_else(|| ws.worktree_path.clone())?;
            let path = std::path::PathBuf::from(&root);
            if path.is_dir() {
                Some((ws.id, path))
            } else {
                None
            }
        })
        .collect();

    let watcher = SkillsWatcher::start(bus.clone(), workspace_pairs);
    tracing::info!("skills watcher started");
    Some(watcher)
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

            // DB health checks (STAB-15 observability)
            report_db_health(&store).await;
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
    report_context_engine().await;
    report_host_capabilities();
    report_cow_support(&config);

    if ok {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

/// §5.7 ports-free check: probe the WSS listen port `serve` will actually bind.
/// Honours the same `INTENTD_TCP_PORT` seam the daemon reads (§13.1 E2E), and
/// falls back to `DEFAULT_PORT` otherwise. Fails when the port cannot be bound
/// — the listener would exit immediately with the same error, so surface it
/// here before `serve` is attempted.
fn check_ports_free() -> bool {
    use intent_transport::lifecycle::DEFAULT_PORT;
    let port = std::env::var("INTENTD_TCP_PORT")
        .ok()
        .and_then(|v| v.trim().parse::<u16>().ok())
        .unwrap_or(DEFAULT_PORT);
    // Port 0 is the "OS-assigned ephemeral" seam — always bindable, do not
    // probe it (there is no fixed port to reserve).
    if port == 0 {
        println!("[ok] WSS port ephemeral (INTENTD_TCP_PORT=0)");
        return true;
    }
    match std::net::TcpListener::bind((std::net::Ipv4Addr::UNSPECIFIED, port)) {
        Ok(_) => {
            println!("[ok] WSS port {port} bindable");
            true
        }
        Err(e) => {
            println!("[FAIL] WSS port {port} not bindable: {e}");
            false
        }
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

/// §5.7 context-engine availability via the real [`ContextEngine::availability`]
/// probe (§8.1). Non-fatal — codebase retrieval degrades gracefully when the
/// engine is absent or unauthenticated (§8.3).
async fn report_context_engine() {
    use intent_context::{AuggieContextEngine, ContextEngine, EngineAvailability};
    let engine = AuggieContextEngine::new();
    match engine.availability().await {
        EngineAvailability::Available { name, version } => {
            let version = version.unwrap_or_else(|| "unknown".to_string());
            println!("[ok] context engine: {name} available (version {version})");
        }
        EngineAvailability::Unavailable { reason } => {
            println!("[--] context engine: unavailable ({reason}) — retrieval degrades gracefully")
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

/// Probe CoW support for the workspaces root at daemon startup. This populates
/// the cache so later cow_probe calls for the same volume pair are instant. Best-effort;
/// failures are silent (the probe will be retried on demand if needed).
fn probe_cow_at_startup(config: &Config) {
    let workspaces_root = config.data_dir.join("workspaces");
    if std::fs::create_dir_all(&workspaces_root).is_ok() {
        // Probe and cache; ignore errors (doctor will report them if persistent)
        let _ = intent_git::cow_probe(&workspaces_root, &workspaces_root);
    }
}

/// CoW isolation support: probe the workspaces root for copy-on-write capability.
/// Non-fatal — CoW isolation degrades gracefully when unsupported (shared mode).
/// Uses the cached result if available (populated by probe_cow_at_startup).
fn report_cow_support(config: &Config) {
    let workspaces_root = config.data_dir.join("workspaces");
    // Create workspaces dir if it doesn't exist (probe needs it)
    if !workspaces_root.exists() {
        if let Err(e) = std::fs::create_dir_all(&workspaces_root) {
            println!("[--] CoW isolation: probe failed (cannot create workspaces dir: {e})");
            return;
        }
    }

    match intent_git::cow_probe(&workspaces_root, &workspaces_root) {
        Ok(intent_git::CowSupport::Supported) => {
            #[cfg(target_os = "macos")]
            println!("[ok] CoW isolation: supported (apfs)");
            #[cfg(target_os = "linux")]
            println!("[ok] CoW isolation: supported (btrfs/xfs/bcachefs/zfs)");
            #[cfg(target_os = "windows")]
            println!("[ok] CoW isolation: supported (refs)");
            #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
            println!("[ok] CoW isolation: supported");
        }
        Ok(intent_git::CowSupport::Unsupported) => {
            #[cfg(target_os = "macos")]
            println!("[--] CoW isolation: unsupported (not apfs or different volumes)");
            #[cfg(target_os = "linux")]
            println!(
                "[--] CoW isolation: unsupported (not btrfs/xfs/bcachefs/zfs or different volumes)"
            );
            #[cfg(target_os = "windows")]
            println!("[--] CoW isolation: unsupported (not ReFS or different volumes)");
            #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
            println!("[--] CoW isolation: unsupported (platform not supported)");
        }
        Err(e) => {
            println!("[--] CoW isolation: probe failed ({e})");
        }
    }
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

/// Report database health metrics for diagnostics (STAB-15 observability).
/// Runs PRAGMA integrity_check, PRAGMA wal_checkpoint(PASSIVE), and reports
/// connection pool stats. Never fails the doctor check — all checks are
/// informational.
async fn report_db_health(store: &Store) {
    println!("database health:");

    // PRAGMA integrity_check: verify DB structural integrity
    // Can return multiple rows if issues are found; treat anything other
    // than a single "ok" row as a warning.
    match sqlx::query("PRAGMA integrity_check")
        .fetch_all(store.pool())
        .await
    {
        Ok(rows) => {
            if rows.len() == 1 {
                match rows[0].try_get::<String, _>(0) {
                    Ok(result) if result == "ok" => println!("  [ok] integrity_check: ok"),
                    Ok(result) => println!("  [WARN] integrity_check: {}", result),
                    Err(e) => println!("  [WARN] integrity_check: failed to decode result: {}", e),
                }
            } else {
                println!("  [WARN] integrity_check: {} issues found", rows.len());
                for row in rows {
                    match row.try_get::<String, _>(0) {
                        Ok(result) => println!("    - {}", result),
                        Err(e) => println!("    - [decode error: {}]", e),
                    }
                }
            }
        }
        Err(e) => {
            println!("  [WARN] integrity_check failed: {}", e);
        }
    }

    // PRAGMA wal_checkpoint(PASSIVE): report checkpoint stats
    // Returns (busy, log, checkpointed) — number of frames in WAL and how many
    // were checkpointed. PASSIVE mode does not block writers. busy > 0 means the
    // checkpoint couldn't complete.
    match sqlx::query("PRAGMA wal_checkpoint(PASSIVE)")
        .fetch_one(store.pool())
        .await
    {
        Ok(row) => {
            let busy = row.try_get::<i64, _>(0);
            let log = row.try_get::<i64, _>(1);
            let checkpointed = row.try_get::<i64, _>(2);

            match (busy, log, checkpointed) {
                (Ok(busy), Ok(log), Ok(checkpointed)) => {
                    if busy != 0 {
                        println!(
                            "  [WARN] wal_checkpoint(PASSIVE): busy={}, log={} frames, checkpointed={} frames (checkpoint incomplete)",
                            busy, log, checkpointed
                        );
                    } else if checkpointed < log {
                        println!(
                            "  [WARN] wal_checkpoint(PASSIVE): log={} frames, checkpointed={} frames (partial checkpoint)",
                            log, checkpointed
                        );
                    } else {
                        println!(
                            "  [ok] wal_checkpoint(PASSIVE): log={} frames, checkpointed={} frames",
                            log, checkpointed
                        );
                    }
                }
                _ => {
                    println!("  [WARN] wal_checkpoint(PASSIVE): failed to decode PRAGMA result");
                }
            }
        }
        Err(e) => {
            println!("  [WARN] wal_checkpoint failed: {}", e);
        }
    }

    // Connection pool stats: report size and idle connections
    let pool = store.pool();
    let size = pool.size();
    let idle = pool.num_idle();
    println!("  [ok] pool: size={}, idle={}", size, idle);
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
            stream_retention_hours: DEFAULT_STREAM_RETENTION_HOURS,
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

    #[cfg(unix)]
    #[test]
    fn data_dir_lock_refuses_second_acquire_while_held() {
        let config = temp_config();
        let guard = acquire_data_dir_lock(&config).expect("first lock acquires");
        let second = acquire_data_dir_lock(&config);
        assert!(
            second.is_err(),
            "a held data-dir lock must refuse a second acquire"
        );
        drop(guard);
        std::fs::remove_dir_all(&config.data_dir).ok();
    }

    #[cfg(unix)]
    #[test]
    fn data_dir_lock_reacquires_after_drop() {
        let config = temp_config();
        let guard = acquire_data_dir_lock(&config).expect("first lock acquires");
        drop(guard);
        let _again =
            acquire_data_dir_lock(&config).expect("re-acquire succeeds after the guard is dropped");
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

    #[test]
    fn permission_policy_parses_each_keyword_case_insensitively() {
        assert_eq!(
            parse_permission_policy(Some("interactive")),
            PermissionPolicy::Interactive
        );
        assert_eq!(
            parse_permission_policy(Some("  AUTO ")),
            PermissionPolicy::AutoByRisk
        );
        assert_eq!(
            parse_permission_policy(Some("Allow")),
            PermissionPolicy::AllowAll
        );
        assert_eq!(
            parse_permission_policy(Some("deny")),
            PermissionPolicy::DenyAll
        );
    }

    #[test]
    fn permission_policy_defaults_to_allow_all() {
        // Absent, blank, and unrecognized values all fall back to the reference
        // default (AllowAll) rather than failing startup or silently denying.
        assert_eq!(parse_permission_policy(None), PermissionPolicy::AllowAll);
        assert_eq!(
            parse_permission_policy(Some("   ")),
            PermissionPolicy::AllowAll
        );
        assert_eq!(
            parse_permission_policy(Some("bogus")),
            PermissionPolicy::AllowAll
        );
    }
}
