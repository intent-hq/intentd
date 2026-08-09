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
    default_process_cap, max_concurrent_agents, AgentManager, BusEventSink, EventBus,
    GitStatusRefresher, PermissionPolicy, Services, WatcherRegistry,
};
use intent_store::Store;
use intent_transport::{
    detect_has_display, ensure_tls_certificate, generate_token, get_or_create_token,
    serve_uds_with_reverse, AsyncTokenStore, CertStatus, FileTokenStore, PrimaryReverseRegistry,
    RpcLimiter, SystemControl, SystemStatus, TokenStore, WsApiServer, WsOptions,
};
use serde_json::{json, Value};
use sqlx::Row;

mod client;
mod git_credential;
mod import;
mod legacy_import;
mod rpc_profile;
mod suspend;
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
    /// Start the daemon and serve JSON-RPC. The UDS listener always serves;
    /// the HTTPS+WSS listener (0.0.0.0, port from `server.wsApi.port` /
    /// `INTENTD_TCP_PORT`, default 5181) boot-starts iff the effective
    /// `server.wsApi.enabled` setting is true (config.toml or runtime toggle).
    /// The TCP listener binds exactly that port — no port walking. A WSS
    /// bind failure at boot is non-fatal (UDS keeps serving; toggle the
    /// setting to retry). `--insecure` (or `INTENTD_INSECURE=1`) always
    /// starts the TCP listener, serving plain `ws://` with no TLS and no
    /// bearer-token auth — dev only; its bind errors are fatal.
    Serve {
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
    /// Import legacy per-directory Intent workspaces
    /// (`<root>/<id>/.workspace/workspace.json`) into the SQLite store. Scans
    /// `~/intent/workspaces`, `~/intent`, and `~/.workspaces` by default;
    /// idempotent (ids already in the DB are skipped) and read-only toward the
    /// source. The same module backs the automatic first-boot import in `serve`.
    ImportLegacy {
        /// Scan only these directories instead of the default legacy roots
        /// (repeatable: `--root a --root b`; each must exist).
        #[arg(long)]
        root: Vec<PathBuf>,
        /// Legacy Electron app-level dir holding `config.json` /
        /// `repo-registry.json`; defaults to the platform userData dir.
        #[arg(long)]
        app_dir: Option<PathBuf>,
        /// Print the per-workspace plan without writing anything.
        #[arg(long)]
        dry_run: bool,
        /// Update rows whose workspace id already exists instead of skipping.
        #[arg(long)]
        force: bool,
    },
    /// Print WSS pairing credentials (bearer token + TLS fingerprint); --rotate
    /// regenerates the token.
    Token {
        /// Mint and persist a NEW token, replacing the old one. Ignored when
        /// `INTENTD_AUTH_TOKEN` is set (the token is fixed by the env var).
        #[arg(long)]
        rotate: bool,
    },
    /// Render the LAN pairing QR code in the terminal plus the plaintext
    /// `intent://pair?…` payload URI. Requires a running daemon: queries
    /// `pairing.getInfo` over UDS so the payload uses the exact same
    /// host/fingerprint/token sources as `intentd token`. When the TCP (WSS)
    /// listener is not running, offers to enable it on the spot (persisting
    /// `server.wsApi.enabled = true` via `settings.update`, which also starts
    /// the listener) — interactively via a [Y/n] prompt, or unattended with
    /// `--yes`; non-interactive runs without `--yes` refuse instead.
    Pair {
        /// Also write the QR code as a PNG image to this path.
        #[arg(long, value_name = "PATH")]
        png: Option<PathBuf>,
        /// Also write the QR code as an SVG document to this path.
        #[arg(long, value_name = "PATH")]
        svg: Option<PathBuf>,
        /// Enable the WSS listener without prompting when it is not running
        /// (persists `server.wsApi.enabled = true`).
        #[arg(long, short = 'y')]
        yes: bool,
    },
    /// Daemon-backed git credential helper (monorepo#884): speaks the
    /// git-credential protocol on stdin/stdout and answers `get` for HTTPS
    /// github.com from the running daemon over UDS (`system.gitCredential`).
    /// Silent (exit 0, no output) on `store`/`erase`, other hosts, a
    /// stopped daemon, or when no credential is available, so git falls
    /// through to its remaining helpers. Wired up via git config as e.g.
    /// `credential.https://github.com.helper=!intentd git-credential`.
    #[command(hide = true)]
    GitCredential {
        /// The git-credential operation: `get`, `store`, or `erase`.
        operation: String,
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

#[tokio::main]
async fn main() -> ExitCode {
    init_tracing();
    install_panic_hook();
    match Cli::parse().command {
        Command::Serve {
            mode,
            insecure,
            resume_all,
        } => to_exit(cmd_serve(mode.as_deref(), insecure, resume_all).await),
        Command::Call { method, params } => to_exit(cmd_call(&method, params.as_deref()).await),
        Command::Status => cmd_status().await,
        Command::Stop => cmd_stop().await,
        Command::Doctor => cmd_doctor().await,
        Command::McpBridge { connect } => {
            // The bridge reads stdin via `tokio::io::stdin()`, whose pending
            // blocking-pool read outlives `run_stdio_bridge`; returning
            // through the runtime drop would wait on it — i.e. until the
            // provider closes stdin — so an initial-connect give-up would
            // never actually exit (monorepo#908). Exit explicitly instead;
            // there is no bridge state to unwind and stdout is flushed per
            // line.
            match cmd_mcp_bridge(&connect).await {
                Ok(()) => std::process::exit(0),
                Err(e) => {
                    eprintln!("error: {e}");
                    std::process::exit(1);
                }
            }
        }
        Command::Import { from } => to_exit(cmd_import(&from).await),
        Command::ImportLegacy {
            root,
            app_dir,
            dry_run,
            force,
        } => to_exit(cmd_import_legacy(root, app_dir, dry_run, force).await),
        Command::Token { rotate } => to_exit(cmd_token(rotate).await),
        Command::Pair { png, svg, yes } => {
            to_exit(cmd_pair(png.as_deref(), svg.as_deref(), yes).await)
        }
        Command::GitCredential { operation } => cmd_git_credential(&operation).await,
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

/// True when a `pairing.getInfo` error frame means the TCP (WSS) listener is
/// not running — the one failure `intentd pair` can fix on the spot by
/// enabling `server.wsApi.enabled`.
fn is_listener_down_error(error: &Value) -> bool {
    error
        .get("message")
        .and_then(Value::as_str)
        .is_some_and(|m| m.contains("TCP listener is not running"))
}

/// Extract the most useful human-readable text from a JSON-RPC error frame:
/// the router maps internal errors to a generic `message` ("Internal error")
/// and carries the friendly text in `data`, while transport-level errors put
/// it straight in `message`.
fn rpc_error_text(error: &Value) -> String {
    error
        .get("data")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .or_else(|| error.get("message").and_then(Value::as_str))
        .unwrap_or("unknown error")
        .to_string()
}

/// Ask the user on the terminal whether to enable the WSS listener; default
/// is yes (empty input). Returns an error when stdin is not a TTY — an
/// unattended run must pass `--yes` to opt in explicitly.
fn confirm_enable_wss() -> anyhow::Result<bool> {
    use std::io::{BufRead, IsTerminal, Write};
    if !std::io::stdin().is_terminal() {
        anyhow::bail!(
            "the WSS listener is not running and stdin is not a terminal — \
             re-run with --yes to enable it (persists server.wsApi.enabled = true), \
             or enable it in config.toml"
        );
    }
    eprint!("The WSS listener is not running. Enable it now? [Y/n] ");
    std::io::stderr().flush()?;
    let mut line = String::new();
    // EOF (0 bytes read, e.g. Ctrl-D before any input) is not a response —
    // only an actual empty *line* (plain Enter) means the default yes.
    if std::io::stdin().lock().read_line(&mut line)? == 0 {
        return Ok(false);
    }
    let answer = line.trim().to_ascii_lowercase();
    Ok(answer.is_empty() || answer == "y" || answer == "yes")
}

/// Enable the WSS listener via `settings.update` over UDS: persists
/// `server.wsApi.enabled = true` to config.toml and starts the listener
/// through the server-control hooks — the same path the FE settings UI uses.
async fn enable_wss_listener(socket: &Path) -> anyhow::Result<()> {
    let response = rpc_call(
        socket,
        "settings.update",
        json!({ "changes": [{ "path": "server.wsApi.enabled", "value": true }] }),
    )
    .await?;
    if let Some(error) = response.get("error") {
        anyhow::bail!("cannot enable the WSS listener: {}", rpc_error_text(error));
    }
    eprintln!("enabled server.wsApi.enabled (persisted to config.toml); WSS listener started");
    Ok(())
}

/// Render the LAN pairing QR code in the terminal (§5.2). Queries
/// `pairing.getInfo` over UDS — so the payload embeds the exact same
/// hosts/fingerprint/token the daemon serves via `server.pairingInfo` and
/// `intentd token` — then renders the `intent://pair?…` payload URI as a QR
/// code in half-height unicode blocks, followed by the plaintext URI.
/// When the TCP (WSS) listener is down, offers to enable it (prompt, or
/// unattended via `yes`) through `settings.update` and retries the query.
async fn cmd_pair(png: Option<&Path>, svg: Option<&Path>, yes: bool) -> anyhow::Result<()> {
    let config = resolve_config()?;
    let mut response = rpc_call(&config.socket_path, "pairing.getInfo", json!({})).await?;
    if response.get("error").is_some_and(is_listener_down_error) {
        if !yes && !confirm_enable_wss()? {
            anyhow::bail!(
                "pairing requires the WSS listener — enable it later with \
                 `intentd pair --yes` or via server.wsApi.enabled in config.toml"
            );
        }
        enable_wss_listener(&config.socket_path).await?;
        response = rpc_call(&config.socket_path, "pairing.getInfo", json!({})).await?;
    }
    if let Some(error) = response.get("error") {
        anyhow::bail!("pairing.getInfo failed: {}", rpc_error_text(error));
    }
    let result = &response["result"];
    let uri = result["uri"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("malformed pairing.getInfo result: missing `uri`"))?;

    let code = qrcode::QrCode::new(uri.as_bytes())
        .map_err(|e| anyhow::anyhow!("cannot encode pairing payload as a QR code: {e}"))?;
    let art = code
        .render::<qrcode::render::unicode::Dense1x2>()
        .quiet_zone(true)
        .build();
    println!("{art}");
    println!();
    println!("{uri}");

    if let Some(path) = png {
        let img = code.render::<image::Luma<u8>>().build();
        // Always encode PNG regardless of the path's extension: extension-based
        // format inference would fail confusingly otherwise.
        let mut buf = std::io::Cursor::new(Vec::new());
        img.write_to(&mut buf, image::ImageFormat::Png)
            .map_err(|e| anyhow::anyhow!("cannot encode PNG: {e}"))?;
        write_secret_file(path, &buf.into_inner())
            .map_err(|e| anyhow::anyhow!("cannot write PNG to {}: {e}", path.display()))?;
        eprintln!("wrote {}", path.display());
    }
    if let Some(path) = svg {
        let doc = code.render::<qrcode::render::svg::Color>().build();
        write_secret_file(path, doc.as_bytes())
            .map_err(|e| anyhow::anyhow!("cannot write SVG to {}: {e}", path.display()))?;
        eprintln!("wrote {}", path.display());
    }
    Ok(())
}

/// Run the daemon-backed git credential helper (monorepo#884). ALWAYS exits 0:
/// git interprets a non-zero helper exit as a hard error and aborts the whole
/// credential search, whereas a silent zero-exit lets it fall through to the
/// remaining helpers/prompt rules. Even a config-resolution failure is silent.
async fn cmd_git_credential(operation: &str) -> ExitCode {
    if let Ok(config) = resolve_config() {
        let _ = git_credential::run(operation, &config.socket_path).await;
    }
    ExitCode::SUCCESS
}

/// Write an exported pairing image with owner-only (0600) permissions: the QR
/// code embeds the bearer token, so it deserves the same treatment as the
/// secrets file. The file is created/truncated with restrictive permissions
/// BEFORE the sensitive bytes are written — never exposed under the umask.
fn write_secret_file(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::io::Write;
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    // If the file pre-existed, `mode` above does not apply; enforce it.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    file.write_all(bytes)
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

/// Import legacy per-directory Intent workspaces into the configured SQLite
/// store. `--root` (repeatable) narrows the scan to explicit directories
/// (each must exist); otherwise the default legacy roots are scanned. The
/// first-boot completion marker does not gate explicit CLI runs. A
/// non-dry-run run without manifest compatibility failures rewrites the marker;
/// `--force` only controls whether existing workspace rows are updated.
/// Per-workspace problems are soft (reported, exit 0); only an unusable
/// explicit `--root` or a store-open failure exits non-zero. A dry-run
/// against a not-yet-created DB removes the freshly created DB file
/// afterwards, so a later `serve` still sees a fresh DB and the first-boot
/// auto-import still fires. Empty resolved roots (legacy import disabled)
/// exit early without opening the store or writing the marker.
async fn cmd_import_legacy(
    roots: Vec<PathBuf>,
    app_dir: Option<PathBuf>,
    dry_run: bool,
    force: bool,
) -> anyhow::Result<()> {
    let config = resolve_config()?;
    std::fs::create_dir_all(&config.data_dir)?;
    let roots = if roots.is_empty() {
        legacy_import::default_roots()
    } else {
        for dir in &roots {
            if !dir.is_dir() {
                anyhow::bail!("--root is not a directory: {}", dir.display());
            }
        }
        roots
    };
    // Empty resolved roots (e.g. `INTENTD_LEGACY_IMPORT_ROOTS=""` or the
    // hermetic test harness) mean "legacy import disabled": return before
    // touching the store so no app-level blobs land and no completion marker
    // is written — consistent with `maybe_import_on_first_boot`.
    if roots.is_empty() {
        println!("legacy import disabled: no legacy roots to scan");
        return Ok(());
    }
    let app_dir = app_dir.or_else(legacy_import::default_app_dir);
    let db_existed = config.db_path.exists();
    let store = Store::open(&config.db_path)
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    let report = legacy_import::run(
        &store,
        &legacy_import::Options {
            roots,
            dry_run,
            force,
            assets_root: Some(config.data_dir.join("assets")),
            app_dir,
        },
    )
    .await?;
    println!("{report}");
    if !dry_run {
        if !report.has_compatibility_failures() {
            // Marker write failure is a warning, not a command failure — the
            // import itself completed (mirrors the first-boot hook in `serve`).
            // Without the marker a later run/first-boot may re-import, which is
            // safe: the import is idempotent.
            if let Err(e) = legacy_import::write_completion_marker(&store).await {
                eprintln!(
                    "warning: import completed but the completion marker could not \
                     be written ({e}); a later run or first boot may re-import \
                     (idempotent, existing rows are skipped)"
                );
            }
        }
    } else if !db_existed {
        // Dry-run on a fresh install: don't leave behind the DB file that
        // `Store::open` just created, or the first-boot auto-import in
        // `serve` (gated on DB-file existence) would silently never fire.
        store.close().await;
        for suffix in ["", "-wal", "-shm"] {
            let mut path = config.db_path.as_os_str().to_owned();
            path.push(suffix);
            let path = PathBuf::from(path);
            if let Err(e) = std::fs::remove_file(&path) {
                if e.kind() != std::io::ErrorKind::NotFound {
                    eprintln!(
                        "warning: could not remove {} ({e}); delete it manually or the \
                         first-boot auto-import in `serve` will not fire",
                        path.display()
                    );
                }
            }
        }
    }
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
    use tracing_subscriber::{
        fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer,
    };

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

    // The env filter is applied per output layer (not globally) so the RPC
    // profiling layer below can observe the DEBUG-level `sqlx::query`
    // statement events that the default `info` filter would otherwise
    // disable at the callsite.
    let output_filter =
        || EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    // Set up dual output: stderr (for interactive use) and optionally file (for diagnostics)
    let stderr_layer = fmt::layer()
        .with_writer(std::io::stderr)
        .with_filter(output_filter());

    // Per-RPC statement-count / duration WARN profiling (expensive-RPC
    // guardrail); its warns flow through the output layers above.
    let profile_layer =
        rpc_profile::RpcProfileLayer::from_env().with_filter(rpc_profile::profile_filter());

    let subscriber = tracing_subscriber::registry()
        .with(profile_layer)
        .with(stderr_layer);

    if let Some(appender) = file_appender {
        let (non_blocking, guard) = tracing_appender::non_blocking(appender);
        let file_layer = fmt::layer()
            .with_writer(non_blocking)
            .with_ansi(false)
            .with_filter(output_filter());
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

async fn cmd_serve(mode: Option<&str>, insecure: bool, resume_all: bool) -> anyhow::Result<()> {
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
    // Prewarm the login-shell PATH capture (`$SHELL -ilc`, ~5-10s cold: an
    // interactive-shell attempt plus a non-interactive fallback, each up to
    // 5s) off the async runtime so it races startup instead of blocking the
    // first `host.providerDiscovery` / `host.findBinary` / `host.toolAvailability`
    // RPC. `spawn_blocking` fires-and-forgets: the shared `OnceLock` behind it
    // (`intent_core::path_utils::login_shell_dirs`) makes the eventual
    // on-demand caller either reuse this warm result or, if it runs first,
    // perform the one real capture itself — either way the shell spawns at
    // most once per process.
    tokio::task::spawn_blocking(intent_core::prewarm_login_shell_path);
    // OS-level single-instance backstop (§5.6): hold an exclusive advisory lock
    // on `data_dir/intentd.lock` for the whole process. Acquired before the
    // socket/pidfile guard so the strongest, configuration-independent guard
    // fires first; released automatically when the held fd closes on drop.
    let _datadir_lock = acquire_data_dir_lock(&config)?;
    // Single-instance guard (§5.6): refuse to start if a live daemon owns the
    // UDS or a live pid holds the pidfile; clean a stale socket/pidfile whose
    // owner is gone. The returned guard removes our pidfile on shutdown.
    let _pidfile = acquire_single_instance(&config).await?;
    // Snapshot DB-file existence before `Store::open` creates it: the one-time
    // legacy workspace import below fires only on a truly fresh database.
    let db_existed = config.db_path.exists();
    let store = Store::open(&config.db_path)
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    // One-time auto_vacuum activation (monorepo#720 finding 1): a legacy
    // database created before the `auto_vacuum = INCREMENTAL` pragma stays in
    // NONE mode until a full VACUUM rebuilds the file, leaving the retention
    // loop's bounded incremental_vacuum a no-op. Run it now — before any
    // listener serves — so no client is blocked by the rebuild and no
    // transaction is open on the write connection (a VACUUM requirement).
    // Failure is non-fatal: the daemon runs degraded exactly as before
    // (freelist pages are simply never returned to the filesystem).
    // Pre-log the rebuild (PR #500 review): on a large legacy DB the VACUUM
    // can take a while with no socket accepting yet, so an operator tailing
    // the log needs to see why startup is slow before it completes.
    if let Ok(row) = sqlx::query("PRAGMA auto_vacuum")
        .fetch_one(store.write_pool())
        .await
    {
        if row.get::<i64, _>(0) == 0 {
            tracing::info!(
                "legacy database in auto_vacuum=NONE mode; running one-time VACUUM \
                 (may take a while on large databases)"
            );
        }
    }
    match store.activate_incremental_vacuum().await {
        Ok(intent_store::AutoVacuumActivation::Activated {
            duration,
            pages_before,
            pages_after,
        }) => tracing::info!(
            duration_ms = duration.as_millis() as u64,
            pages_before,
            pages_after,
            "auto_vacuum activated: one-time VACUUM converted the database to incremental mode"
        ),
        Ok(intent_store::AutoVacuumActivation::AlreadyIncremental) => {}
        Err(e) => tracing::warn!(
            error = %e,
            "auto_vacuum activation failed; continuing without incremental vacuum"
        ),
    }
    // First-boot legacy workspace import: on a fresh DB with no completion
    // marker, scan the legacy roots and import `.workspace/workspace.json`
    // workspaces. Runs to completion inline after migrations (inside
    // `Store::open`) and before any transport serves RPCs; it never fails
    // startup, but a large legacy tree does delay this first boot (accepted
    // one-time tradeoff — see `maybe_import_on_first_boot`).
    legacy_import::maybe_import_on_first_boot(
        &store,
        db_existed,
        legacy_import::default_roots(),
        Some(config.data_dir.join("assets")),
        legacy_import::default_app_dir(),
    )
    .await;
    // Spawn the periodic WAL checkpoint task (every 60s) to prevent unbounded
    // WAL growth when continuous readers hold long-lived transactions. Aborted
    // during shutdown before Store::close().
    let checkpoint_handle = store.spawn_periodic_wal_checkpoint();
    // The event bus shares the store with the services surface so subscribers
    // see the same durable event log that future mutations will publish to.
    let bus = EventBus::new(store.clone());
    // Hold a store handle for the §10.2 retention sweep before the store is
    // moved into the services surface below.
    let retention_store = store.clone();
    // Hold a store handle for the §5.4 idempotency reaper (same lifecycle root
    // as the retention sweep) before the store is moved into the services below.
    let idempotency_store = store.clone();
    // Hold a store handle for the graceful close at shutdown.
    let shutdown_store = store.clone();
    // REV-1: build the shared first-client-sticky reverse-dispatch registry
    // BEFORE the services surface + listeners so both sides observe the same
    // live-client set. Every accepted UDS/WSS connection registers its
    // per-connection `ReverseChannel` here; agent-initiated `browser.exec`
    // routes through the same registry via `Services::with_reverse_dispatch`.
    let reverse_registry = Arc::new(PrimaryReverseRegistry::new());
    // Layered `config.toml` registry backing the TOML-backed `settings.*` keys
    // (defaults < file < startup pins). `Config::resolve()` above already
    // parsed the file strictly (malformed file → startup error), so a load
    // failure here is unexpected and equally fatal.
    let settings_registry = Arc::new(
        intent_services::SettingsRegistry::load(&config.config_path)
            .map_err(|e| anyhow::anyhow!(e.to_string()))?,
    );
    // One-time carry-over of the renamed `[backgroundAgents]` table into the
    // `quickActions.*` keys (monorepo#1729), before the legacy import below
    // discards and strips it. Unset quick-action keys inherit the old value;
    // already-set ones are left alone.
    intent_services::migrate_quick_action_settings(&settings_registry)
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    // One-time legacy handling: retired keys still present in config.toml
    // (e.g. the `[ai]` table, `model.workspaceOverrides`) were tolerated +
    // captured by the load above; import any that still have a catalog entry
    // into the settings table (currently none), then strip them from the
    // file with a comment-preserving rewrite. A failed import keeps the file
    // intact so the next boot retries.
    intent_services::import_legacy_settings(&settings_registry, &store)
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    // One-time cleanup of stale SQLite rows for retired settings (e.g. the
    // per-workspace `model.workspaceOverrides` blob, monorepo#1000).
    intent_services::cleanup_retired_settings(&store)
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    // One-time migration for the trimmed `voice.vocabulary` default: a stored
    // row that only ever persisted the retired 17-term seed default is
    // deleted so the new `["Intent"]` default applies; user-modified lists
    // are never touched.
    intent_services::migrate_default_vocabulary(&store)
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    // Startup flag/env pins (§9.8 precedence: defaults < config.toml < pins):
    // pinned keys take the flag value, report origin `flag` on the wire,
    // reject `settings.update`, and ignore the file value on live-reload. An
    // invalid pin value (e.g. out-of-range INTENTD_TCP_PORT) refuses startup.
    apply_startup_pins(&settings_registry, insecure)?;
    // Snapshot of the effective boot settings for the boot-time reads below
    // (agents.maxConcurrent, server.wsApi.enabled).
    let boot_settings = settings_registry.snapshot();
    // Resolve the concurrent agent cap: positive effective value → explicit
    // override; 0 (the schema default) → auto (RAM-based default_process_cap).
    // The setting applies on daemon restart (§9.8 agents.maxConcurrent).
    let process_cap =
        max_concurrent_agents(&boot_settings.effective).unwrap_or_else(default_process_cap);
    // The services surface publishes CRUD change events onto the same bus that
    // transport subscriptions read, so a mutation on one connection streams to
    // subscribers on another (§10).
    let legacy_import_store = store.clone();
    let assets_root = config.data_dir.join("assets");

    // Suspend/wake detector (clock-skew): infers host sleep/resume with no OS
    // integration and exposes a shared tracker. Spawned in the common serve
    // path (covers UDS/WSS/insecure/headless); skipped entirely when
    // wakeResume.enabled is false. The tracker feeds two consumers: Task C
    // enrollment (injected into `Services` as the `SuspendOverlapQuery` below)
    // and the Task D wake-triggered resume orchestrator (subscribes to its
    // resume-event stream further down).
    let suspend_tracker: Option<Arc<suspend::SuspendTracker>> =
        config.wake_resume_enabled.then(|| {
            suspend::spawn_suspend_detector(Duration::from_secs(
                config.wake_resume_threshold_seconds as u64,
            ))
        });

    let services = Services::new(store)
        .with_assets_root(assets_root.clone())
        // Persist the per-provider models.list cache in the data dir (§5.30).
        .with_models_cache_dir(config.data_dir.clone())
        .with_event_bus(bus.clone())
        .with_reverse_dispatch(reverse_registry.clone())
        .with_settings_registry(settings_registry.clone())
        .with_hooks_max_per_agent(config.hooks_max_per_agent);
    // Inject the suspend-overlap query so Task C can recognize sleep-induced
    // turn failures and enroll them for wake-resume. Left unset when wakeResume
    // is disabled, keeping today's terminal behavior for transient disconnects.
    //
    // §13.1 E2E seam: `INTENTD_TEST_FORCE_SUSPEND_OVERLAP_SECS` swaps in a
    // `ForcedSuspendOverlap` query (any transient disconnect counts as
    // suspend-overlapping) so the WSS e2e can drive enrollment + the
    // self-heal resume deterministically without a real host sleep. The real
    // tracker still drives the wake orchestrator; it simply never records a
    // suspend, so the enrolled row recovers via the self-heal, not a broadcast.
    let suspend_overlap_query: Option<Arc<dyn intent_services::SuspendOverlapQuery>> =
        match std::env::var("INTENTD_TEST_FORCE_SUSPEND_OVERLAP_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|secs| *secs > 0 && config.wake_resume_enabled)
        {
            Some(secs) => Some(Arc::new(suspend::ForcedSuspendOverlap::new(
                Duration::from_secs(secs),
            ))),
            None => suspend_tracker
                .clone()
                .map(|t| t as Arc<dyn intent_services::SuspendOverlapQuery>),
        };
    let services = match suspend_overlap_query {
        Some(query) => services.with_suspend_tracker(query),
        None => services,
    };
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
        .with_agent_log_root(intent_core::agent_logs_root(&config.data_dir))
        // Per-agent generated config files (`--mcp-config`, `--rules`,
        // pi-extension delivery) are written under the daemon-owned
        // `<data_dir>/agent-configs` instead of the global OS temp dir
        // (monorepo#1302). Swept at startup (no agent child is live yet)
        // so files leaked by a killed daemon don't accumulate.
        .with_agent_config_root({
            let root = intent_core::agent_configs_root(&config.data_dir);
            if let Err(e) = intent_core::sweep_agent_configs(&root) {
                tracing::warn!(error = %e, path = %root.display(), "agent-configs sweep failed");
            }
            root
        })
        // STAB-50: chief provider children spawn in the dedicated, empty
        // `<data_dir>/chief-cwd` directory instead of `/tmp`. Swept at
        // startup (no chief child is live yet) so leftovers a provider
        // scribbled into its cwd don't accumulate across daemon runs and
        // get re-indexed.
        .with_chief_cwd_root({
            let root = intent_core::chief_cwd_root(&config.data_dir);
            if let Err(e) = intent_core::sweep_chief_cwd(&root) {
                tracing::warn!(error = %e, path = %root.display(), "chief-cwd sweep failed");
            }
            root
        }),
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
    // Rehydrate persisted agent send queues (write-through `agent_queue`
    // table) into the in-memory map before any listener serves RPCs, so
    // messages queued at the previous shutdown survive the restart. This only
    // restores state — it never starts a turn; queued messages sit until an
    // explicit kick (resume, sendMessage, queueMessage, retry). Best-effort:
    // a failure is logged but never aborts startup.
    match services.rehydrate_agent_queues().await {
        Ok(0) => {}
        Ok(rehydrated) => tracing::info!(rehydrated, "rehydrated persisted agent queue messages"),
        Err(e) => tracing::warn!(error = %e, "agent queue rehydration failed"),
    }
    // STAB-108: Rehydrate undelivered delegation groups on startup so groups
    // survive daemon restarts without requiring the resume path. Groups are
    // reconciled against current agent state (already-completed children are
    // recorded) and ready groups fire immediately. Best-effort: a failure is
    // logged but never aborts startup.
    match services.heal_delegation_groups_on_startup().await {
        Ok(0) => {}
        Ok(loaded) => tracing::info!(
            loaded,
            "rehydrated undelivered delegation groups on startup"
        ),
        Err(e) => tracing::warn!(error = %e, "delegation group startup rehydration failed"),
    }
    // Rehydrate persisted completion watches AFTER delegation groups so
    // grouped watches can find their live groups (a grouped watch whose group
    // is gone is pruned). Watches whose child completed during the downtime
    // wake the parent immediately. Best-effort: a failure is logged but never
    // aborts startup.
    match services.heal_completion_watches_on_startup().await {
        Ok(0) => {}
        Ok(loaded) => tracing::info!(loaded, "rehydrated persisted completion watches on startup"),
        Err(e) => tracing::warn!(error = %e, "completion watch startup rehydration failed"),
    }
    // Rehydrate persisted event subscriptions (monorepo#937) so `event.subscribe`
    // registrations survive daemon restarts; rows whose subscriber agent is
    // gone are pruned. Best-effort: a failure is logged but never aborts
    // startup (agents can re-subscribe).
    match services.heal_event_subscriptions_on_startup().await {
        Ok(0) => {}
        Ok(loaded) => tracing::info!(
            loaded,
            "rehydrated persisted event subscriptions on startup"
        ),
        Err(e) => tracing::warn!(error = %e, "event subscription startup rehydration failed"),
    }
    // Hydrate the script registry from the persisted definitions (§5.8) so
    // `script.*` survives daemon restarts. Best-effort: a failure is logged
    // but never aborts startup (scripts can still be re-created live).
    match services.hydrate_scripts().await {
        Ok(0) => {}
        Ok(loaded) => tracing::info!(loaded, "hydrated persisted script definitions"),
        Err(e) => tracing::warn!(error = %e, "script registry hydration failed"),
    }
    // Rehydrate active background hooks (`scheduled`/`running` rows) so their
    // schedules resume after a restart; hooks whose owning agent is gone are
    // cancelled instead. Best-effort: a failure is logged but never aborts
    // startup (agents can re-schedule).
    match services.rehydrate_hooks().await {
        Ok(0) => {}
        Ok(resumed) => tracing::info!(resumed, "rehydrated active background hooks on startup"),
        Err(e) => tracing::warn!(error = %e, "background hook rehydration failed"),
    }
    // Rehydrate active PR monitors (`ws.pr.monitor`) so watches resume after a
    // restart; monitors whose owning agent is gone are cancelled. Each resumed
    // monitor is marked for catch-up, so its first poll delivers any change
    // detected across the downtime immediately (no debounce). Best-effort: a
    // failure is logged but never aborts startup.
    match services.rehydrate_pr_monitors().await {
        Ok(0) => {}
        Ok(resumed) => tracing::info!(resumed, "rehydrated active PR monitors on startup"),
        Err(e) => tracing::warn!(error = %e, "PR monitor rehydration failed"),
    }
    // Sweep orphaned `*.deleting-*` worktree trash dirs left behind when a
    // prior daemon crashed between the locked detach rename and the unlocked
    // recursive removal (monorepo#473). Spawned so the potentially multi-GB
    // removal never blocks startup; best-effort throughout — a failure never
    // aborts startup, and a missing workspaces root is a silent no-op.
    let services_trash_sweep = services.clone();
    tokio::spawn(async move {
        let removed = services_trash_sweep.sweep_orphaned_worktree_trash().await;
        if removed > 0 {
            tracing::info!(
                removed,
                "startup sweep removed orphaned worktree trash dirs"
            );
        }
    });
    // Background PR refresh (§7.6): periodically re-fetch linked PRs (and
    // discover/link PRs for workspaces without one), persist any change, and
    // emit `pr:*` events so clients update without polling.
    // Tiered by workspace recency to trim forge load (§7.7): recently-active
    // workspaces refresh every 180s tick, idle ones only on every 10th tick
    // (~30 min). Safe when source control is unconfigured (a sweep with due
    // workspaces logs and swallows the missing-provider error). Aborted on
    // clean shutdown.
    let pr_refresh = services.spawn_pr_refresh_loop(std::time::Duration::from_secs(180));
    // Centralized PR-monitor loop (`ws.pr.monitor`): every `[prMonitor]
    // pollSeconds` (read live, floor 10s), poll each active monitor, diff it
    // against its persisted baseline, and deliver one consolidated wake once
    // the PR has been quiet for the debounce window. Safe when source control
    // is unconfigured (the tick logs and returns). Aborted on clean shutdown.
    let pr_monitor_loop = services.spawn_pr_monitor_loop();
    // Daemon-internal token-usage scan (§5.23/§19.1): every 300s, re-tally each
    // workspace's per-agent/per-model token usage, persist the durable
    // `tokenUsage` field, and emit `workspace:tokenUsage-changed` on deltas.
    // There is no scan RPC. Aborted on clean shutdown.
    let token_usage_scan =
        services.spawn_token_usage_scan_loop(std::time::Duration::from_secs(300));
    // Completion-delivery worker (AS-3): wake parents holding a
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
    // `host:exec:*`, `script:output`, plus the high-churn state-notification
    // families — see `Store::delete_ephemeral_events_before`) older than the
    // configured TTL, preserving lifecycle/tool/note/task events. Disabled
    // when `events.streamRetentionHours == 0`.
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
    // Merge-pending retry sweep: periodically retry merge-back for sandboxes
    // stranded `merge_pending` (daemon restart mid-merge, historical failures
    // like the pre-#592 fetch bug). First tick fires immediately so stuck
    // sandboxes self-heal on startup. Aborted on clean shutdown.
    let merge_retry_task = spawn_sandbox_merge_retry_loop(services.clone());
    // External MCP servers (§18.3): the health monitor (periodic ping +
    // auto-restart pushing `mcp.servers:status-changed`) starts immediately;
    // starting the enabled servers themselves is deferred to the background
    // init below because each handshake can take seconds. The hub is reaped on
    // shutdown so no orphan server processes remain (PTY-host reaping parity).
    let mcp_hub = services.mcp_hub();
    let mcp_monitor = mcp_hub.spawn_health_monitor();

    // Build api Arc early so it can be cloned for runtime control (§5.12).
    // ServerControl is attached after DaemonControl is built via the OnceLock seam.
    let api: Arc<dyn WorkspaceApi> = Arc::new(services.clone());
    // Bridge `file:*` → debounced `changes:git-status` (monorepo#1397): external
    // file edits refresh the FE Changes panel without any in-app git action.
    // Arc'd so the watcher registry's `.git` metadata watches feed the same
    // debounced trigger path. Held for the lifetime of `serve` and torn down
    // on return.
    let git_status_refresher = Arc::new(GitStatusRefresher::start(
        bus.clone(),
        api.clone(),
        services.git_status_cache(),
    ));
    // Slow initializations run in the background so the listeners below bind —
    // and `system.status` answers — without waiting on them (monorepo#1581):
    // enabled MCP servers (started serially, each handshake up to a multi-second
    // timeout) and the watcher registry (serial FSEvents registrations, which on
    // a loaded macOS `fseventsd` cost seconds each). Both handles are aborted on
    // clean shutdown, which drops the registry and every watcher it owns.
    let mut mcp_start_task = {
        let services = services.clone();
        tokio::spawn(async move { services.start_enabled_mcp_servers().await })
    };
    let watcher_init_task =
        spawn_watcher_registry_init(bus.clone(), api.clone(), Arc::clone(&git_status_refresher));

    // Prepare runtime control for the HTTPS+WSS listener (§5.12). Build the
    // construction args ALWAYS so settings can toggle the listener on/off at
    // runtime. Boot-time auto-start of the listener (see [`boot_ws_listener`]):
    // `--insecure` → the plain-ws TCP listener always starts (dev posture);
    // otherwise the WSS listener starts ONLY if the effective
    // server.wsApi.enabled is true (config.toml / persisted toggle state
    // honored across relaunches). Env (INTENTD_TCP_PORT) takes precedence
    // over persisted settings for the port.
    let mut ws_options = ws_options_from_env();
    ws_options.locality_override = locality_override;
    // ONE daemon-wide outstanding-slow-path-RPC cap (`server.maxOutstandingRpcs`,
    // 0 = unlimited) shared by the UDS and WSS listeners so the limit is global,
    // not per-connection or per-transport.
    let rpc_limiter = RpcLimiter::new(config.server_max_outstanding_rpcs);
    if config.server_max_outstanding_rpcs == 0 {
        tracing::warn!(
            "outstanding-RPC overload cap disabled (server.maxOutstandingRpcs = 0): \
             slow-path RPC concurrency is unlimited"
        );
    }
    ws_options.rpc_limiter = rpc_limiter.clone();

    // TLS + bearer auth: provision the cert (lazy; cert stays on disk) + build
    // the token store for auth layers (§5.2/§5.3). Always provision for runtime
    // toggle, even when the listener is not boot-started (it can be started
    // later via settings).
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

    // Build runtime control struct (always, regardless of boot listener state)
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
    let proc_usage = spawn_proc_usage_sampler();

    let control = Arc::new(DaemonControl {
        manager: manager.clone(),
        shutdown: shutdown_notify.clone(),
        ws_runtime: runtime.clone(),
        start_time: std::time::Instant::now(),
        proc_usage,
        legacy_import_store,
        legacy_import_assets_root: assets_root,
        legacy_import_lock: tokio::sync::Mutex::new(()),
        settings_registry: settings_registry.clone(),
    });

    // Populate the runtime control OnceLock so runtime-toggled WSS listeners can
    // serve system.status (§5.7). This breaks the circular Arc dependency between
    // DaemonControl and WsRuntimeControl.
    if runtime.control.set(control.clone()).is_err() {
        panic!("control OnceLock should only be set once");
    }

    // Resolve the boot-time TCP listener decision once: `--insecure` always
    // starts the plain-ws listener; otherwise the secure WSS listener starts
    // iff the effective server.wsApi.enabled is true (handled further below,
    // after the config watcher is up).
    let boot_listener = boot_ws_listener(insecure, boot_settings.effective.server.ws_api.enabled);

    // Boot-time plain-ws listener start under --insecure (dev posture): binds
    // exactly ws_options.base_port (INTENTD_TCP_PORT honored, 0 = ephemeral)
    // and any bind error is fatal (no port walking). No pairing provider —
    // pairing is a secure-mode surface.
    if boot_listener == BootWsListener::InsecurePlainWs {
        let system_control: Arc<dyn SystemControl> = control.clone();
        let server = WsApiServer::new_insecure_with_reverse(
            api.clone(),
            bus.clone(),
            ws_options.clone(),
            reverse_registry.clone(),
            Some(system_control),
        );
        let port = server.start().await?;
        tracing::info!(port, "intentd WS listening (insecure, no TLS)");

        // Store the server in runtime state
        {
            let mut state = runtime.state.lock().await;
            state.ws_server = Some(server);
            state.port = Some(port);
        }
    }

    // Wire ServerControl to Services for settings-driven runtime control (§5.12).
    // The control is attached after the api Arc is built via the `OnceLock` seam.
    let server_control: Arc<dyn intent_core::ServerControl> = control.clone();
    services.attach_server_control(server_control);

    // Live-reload of config.toml (§9.8): watch the file's parent directory
    // (survives editor rename/atomic-save), debounce, and strictly re-parse.
    // Valid external edits update the registry, run the same server runtime
    // hooks as `settings.update`, and emit `settings:changed`; invalid edits
    // keep last-good values. Registration is a synchronous FSEvents call, so
    // it too runs in the background (monorepo#1581) with the guard held by the
    // task for the lifetime of `serve`; aborting the handle at shutdown drops
    // the guard and tears the watch down with the daemon.
    let config_watcher_task =
        spawn_config_watcher_init(settings_registry.clone(), services.clone());

    // Boot-time secure WSS listener auto-start when the effective
    // server.wsApi.enabled is true (config.toml or persisted runtime toggle).
    // A bind failure at boot (port in use) is non-fatal: UDS stays up, setting
    // stays true, warning logged (UI shows "not running" via pairingInfo.port=null).
    if boot_listener == BootWsListener::SecureWss {
        match control.start_ws_listener().await {
            Ok(port) => {
                tracing::info!(
                    port,
                    "WSS listener auto-started at boot (persisted server.wsApi.enabled=true)"
                );
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "failed to auto-start WSS listener at boot (persisted enabled=true); \
                     UDS still serving, setting remains true, toggle OFF→ON to retry"
                );
            }
        }
    }

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

    // Wake-triggered auto-resume (sleep-resume Task D). When wakeResume is
    // enabled, subscribe to the suspend detector's resume-event stream and, on
    // each host wake, resume the turns Task C enrolled as
    // `system_suspend`-interrupted (only those). A burst of wake ticks (sleep
    // flaps) is debounced into a single sweep, and the per-row atomic claim in
    // `resume_interrupted_agent` dedupes against a concurrent
    // `agent.resolveInterrupted` / `--resume-all`. Skipped entirely when
    // wakeResume is disabled (no tracker exists), honoring the config gate.
    if let Some(tracker) = suspend_tracker.clone() {
        let services_clone = services.clone();
        let mut resume_rx = tracker.subscribe();
        // Coalesce wake events landing within this window into one sweep.
        const WAKE_RESUME_DEBOUNCE: Duration = Duration::from_secs(2);
        tokio::spawn(async move {
            use tokio::sync::broadcast::error::RecvError;
            loop {
                match resume_rx.recv().await {
                    Ok(ev) => {
                        tracing::info!(
                            suspended_for_secs = ev.suspended_for.as_secs(),
                            "wake-resume: host wake detected; scheduling resume sweep"
                        );
                        // Debounce flaps: drain any further wake events that
                        // arrive within the window before running one sweep.
                        loop {
                            tokio::select! {
                                _ = tokio::time::sleep(WAKE_RESUME_DEBOUNCE) => break,
                                drained = resume_rx.recv() => match drained {
                                    Ok(_) | Err(RecvError::Lagged(_)) => continue,
                                    Err(RecvError::Closed) => break,
                                },
                            }
                        }
                        services_clone.resume_suspend_interrupted_agents().await;
                    }
                    // A lag means we dropped some wake events; the sweep is
                    // idempotent (it re-enumerates pending rows), so run a
                    // catch-up sweep rather than missing a wake.
                    Err(RecvError::Lagged(skipped)) => {
                        tracing::warn!(
                            skipped,
                            "wake-resume: resume stream lagged; running a catch-up sweep"
                        );
                        services_clone.resume_suspend_interrupted_agents().await;
                    }
                    Err(RecvError::Closed) => {
                        tracing::debug!("wake-resume: resume stream closed; orchestrator exiting");
                        break;
                    }
                }
            }
        });
    }

    // UDS always serves — it is the local control transport every deployment
    // relies on (status/stop/doctor, FE sidecar, pairing RPCs).
    tracing::info!(socket = %config.socket_path.display(), "starting intentd");
    let system_control: Arc<dyn SystemControl> = control.clone();
    serve_uds_with_reverse(
        api,
        bus,
        &config.socket_path,
        Some(system_control),
        pairing_info,
        reverse_registry.clone(),
        rpc_limiter,
        shutdown,
    )
    .await?;

    // Clean shutdown: stop the WSS listener (graceful close + port release),
    // stop the PR refresh loop, then kill every spawned agent child and clear
    // the registry (§6.8 teardown). Idle reaping during the run is the M5
    // `reap_idle` hook. Stop via ServerControl so we stop the runtime listener
    // (ws_runtime.state.ws_server), not the stale boot-time ws_server variable.
    control.stop_ws_listener().await;
    pr_refresh.abort();
    pr_monitor_loop.abort();
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
    merge_retry_task.abort();
    // Drop the watcher registry (and every filesystem/skills/specialists watch
    // it owns) plus the config.toml live-reload watch by aborting the tasks
    // that hold them.
    watcher_init_task.abort();
    config_watcher_task.abort();
    // Stop the MCP health monitor and reap every external MCP server's process
    // group so no orphan stdio servers survive the daemon (§18.3). The deferred
    // start task is JOINED (bounded) rather than merely aborted: a server still
    // mid-handshake is not in the hub map yet, so cancelling it there would drop
    // the child outside the process-group reap and its grandchildren would
    // survive (`kill_on_drop` only covers the direct child). Letting the sweep
    // settle first puts every child it spawned in the map, so `shutdown` reaps
    // them. Only if the grace expires do we abort and accept the drop path.
    match tokio::time::timeout(MCP_START_JOIN_GRACE, &mut mcp_start_task).await {
        Ok(_) => {}
        Err(_) => {
            tracing::warn!(
                grace_ms = MCP_START_JOIN_GRACE.as_millis() as u64,
                "deferred MCP start sweep did not settle within the shutdown grace; \
                 aborting it — a server mid-handshake may leave orphan grandchildren"
            );
            mcp_start_task.abort();
        }
    }
    mcp_monitor.abort();
    mcp_hub.shutdown().await;
    manager.shutdown().await;

    // Kill every daemon-owned PTY session — terminals and scripts — so no
    // child survives the daemon as an orphan (monorepo#1526). Scripts are
    // flagged user-stopped before any PTY dies so no auto-restart supervisor
    // races the sweep; the whole teardown is bounded by one SIGTERM grace
    // (plus a bounded supervisor-settle backstop), staying well inside the
    // FE sidecar's own kill grace.
    let (scripts_stopped, ptys_killed) = services.shutdown_pty_sessions().await;
    if scripts_stopped > 0 || ptys_killed > 0 {
        tracing::info!(
            scripts = scripts_stopped,
            ptys = ptys_killed,
            "graceful shutdown: reaped daemon-owned PTY sessions"
        );
    }

    // Stop the periodic WAL checkpoint task before closing the store.
    checkpoint_handle.abort();

    // Close the store pool gracefully, checkpointing the WAL so persisted data
    // is visible to the next daemon instance.
    shutdown_store.close().await;

    Ok(())
}

/// Live daemon control surface backing `system.status`, `system.shutdown`, and
/// `system.importLegacy` (§5.7) plus runtime WSS listener control (§5.12). Built post-bind so the
/// resolved WSS `port`/`fingerprint` are real (not guessed); `client_count`/agent
/// count are read live on each status call. The runtime fields (`ws_server`,
/// `ws_runtime`) allow settings-driven start/stop without daemon restart.
struct DaemonControl {
    manager: Arc<AgentManager>,
    shutdown: Arc<tokio::sync::Notify>,
    /// Runtime state for settings-driven listener control (§5.12). Holds the
    /// WsApiServer construction args so `start_ws_listener` can build a fresh
    /// server when toggled on. Always present so the runtime toggle works
    /// whether or not the listener was boot-started.
    ws_runtime: Arc<WsRuntimeControl>,
    /// Daemon start time (Instant) for uptime calculation.
    start_time: std::time::Instant,
    /// Latest own-process CPU/memory sample from the background sampler.
    proc_usage: Arc<ProcUsage>,
    /// Live store and asset destination shared with Services for legacy import.
    legacy_import_store: Store,
    legacy_import_assets_root: PathBuf,
    /// Prevent overlapping import runs from racing workspace inserts/copies.
    legacy_import_lock: tokio::sync::Mutex<()>,
    /// Settings registry backing the `system.gitCredential` gate + token
    /// source (monorepo#884).
    settings_registry: Arc<intent_services::SettingsRegistry>,
}

/// Latest own-process resource sample for `system.status`, written by the
/// background sampler task (~1s tick) and read lock-free from `status()`.
/// `cpu_percent` follows the raw `sysinfo` convention (100 = one full core,
/// may exceed 100 on multi-core hosts); `memory_bytes` is resident memory.
#[derive(Default)]
struct ProcUsage {
    /// `f32` CPU percent stored as raw bits (`f32::to_bits`).
    cpu_bits: std::sync::atomic::AtomicU32,
    memory_bytes: std::sync::atomic::AtomicU64,
}

impl ProcUsage {
    fn store(&self, cpu_percent: f32, memory_bytes: u64) {
        use std::sync::atomic::Ordering;
        self.cpu_bits
            .store(cpu_percent.to_bits(), Ordering::Relaxed);
        self.memory_bytes.store(memory_bytes, Ordering::Relaxed);
    }

    fn load(&self) -> (f32, u64) {
        use std::sync::atomic::Ordering;
        (
            f32::from_bits(self.cpu_bits.load(Ordering::Relaxed)),
            self.memory_bytes.load(Ordering::Relaxed),
        )
    }
}

/// Spawn the own-process CPU/memory sampler backing `system.status` (§5.7).
/// Takes one synchronous sample first so `memoryBytes` is populated before the
/// listeners come up (the first CPU reading may legitimately be 0 — sysinfo
/// needs two refreshes to compute a delta), then refreshes on a ~1s tick.
/// Refreshes are scoped to the daemon's own PID — never a full-system scan.
fn spawn_proc_usage_sampler() -> Arc<ProcUsage> {
    use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System};

    let usage = Arc::new(ProcUsage::default());
    let Ok(pid) = sysinfo::get_current_pid() else {
        tracing::warn!("cannot resolve own pid; cpu/memory sampling disabled");
        return usage;
    };
    let refresh_kind = ProcessRefreshKind::nothing().with_cpu().with_memory();
    let mut sys = System::new();
    let sample = move |sys: &mut System, usage: &ProcUsage| {
        sys.refresh_processes_specifics(ProcessesToUpdate::Some(&[pid]), true, refresh_kind);
        if let Some(proc) = sys.process(pid) {
            usage.store(proc.cpu_usage(), proc.memory());
        }
    };
    sample(&mut sys, &usage);

    let task_usage = usage.clone();
    tokio::spawn(async move {
        // Start one period out: `interval`'s first tick fires immediately,
        // which would re-refresh right after the startup sample — under
        // sysinfo's MINIMUM_CPU_UPDATE_INTERVAL, yielding an unreliable delta.
        let period = Duration::from_secs(1);
        let mut tick = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            sample(&mut sys, &task_usage);
        }
    });
    usage
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
    /// (WsRuntimeControl is always constructed; the WSS listener boot-starts
    /// only when server.wsApi.enabled is true, but can be started at runtime
    /// via settings regardless).
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

        let (cpu_percent, memory_bytes) = self.proc_usage.load();
        // Derived transport surface: UDS always serves; `tcp`/`listenMode`
        // reflect the live TCP listener state (runtime toggles included), so
        // `listenMode` is `both` while the listener is up and `uds` otherwise.
        // Under try_lock contention above `port` reads `None`, so a status
        // call racing a listener start/stop may transiently report `uds` —
        // matching the port/fingerprint/clients fallback, and self-correcting
        // on the next call.
        let tcp = port.is_some();
        SystemStatus {
            listen_mode: if tcp { "both" } else { "uds" }.to_string(),
            uds: true,
            tcp,
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
            cpu_percent,
            memory_bytes,
        }
    }

    fn request_shutdown(&self) {
        // `notify_one` stores a permit if the serve loop is not yet awaiting, so
        // the shutdown is never lost to a race with a freshly-arrived RPC.
        self.shutdown.notify_one();
    }

    fn import_legacy(
        &self,
        force: bool,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Value, String>> + Send + '_>>
    {
        Box::pin(async move {
            let _guard = self.legacy_import_lock.lock().await;
            let report = legacy_import::run(
                &self.legacy_import_store,
                &legacy_import::Options {
                    roots: legacy_import::default_roots(),
                    dry_run: false,
                    force,
                    assets_root: Some(self.legacy_import_assets_root.clone()),
                    app_dir: legacy_import::default_app_dir(),
                },
            )
            .await
            .map_err(|e| e.to_string())?;

            let compatibility_failures = report.has_compatibility_failures();
            let marker_written = if compatibility_failures {
                false
            } else {
                match legacy_import::write_completion_marker(&self.legacy_import_store).await {
                    Ok(()) => true,
                    Err(e) => {
                        tracing::warn!(error = %e, "legacy import RPC marker write failed");
                        false
                    }
                }
            };
            let skip_summary: Vec<Value> = report
                .entries
                .iter()
                .filter_map(|entry| match &entry.outcome {
                    legacy_import::Outcome::Skipped(reason) => {
                        Some(json!({ "id": &entry.id, "reason": reason }))
                    }
                    _ => None,
                })
                .take(20)
                .collect();
            Ok(json!({
                "imported": report.imported(),
                "updated": report.updated(),
                "skipped": report.skipped(),
                "notes": report.notes_imported(),
                "comments": report.comments_imported(),
                "agents": report.agent_sessions_imported(),
                "assets": report.assets_imported(),
                "skipSummary": skip_summary,
                "compatibilityFailures": compatibility_failures,
                "markerWritten": marker_written,
            }))
        })
    }

    fn git_credential(
        &self,
        client_pid: Option<u64>,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Option<(String, String)>> + Send + '_>>
    {
        Box::pin(async move {
            let credential =
                intent_services::github_git_credential(Some(&self.settings_registry)).await;
            // Audit trail (monorepo#884): record every grant/denial with the
            // helper's self-reported pid. The token value is never logged.
            match &credential {
                Some(_) => {
                    tracing::info!(client_pid, "git credential granted to helper over UDS");
                }
                None => {
                    tracing::debug!(
                        client_pid,
                        "git credential request denied (gate off or no token)"
                    );
                }
            }
            credential
        })
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

            // Read the persisted port from settings, then resolve against the
            // env seam (see `resolve_ws_listener_port` for the precedence).
            let settings_port = match runtime
                .api
                .settings_get("server.wsApi.port".to_string())
                .await
            {
                Ok(result) => result
                    .get("value")
                    .and_then(|v| v.as_f64())
                    .map(|p| p as u16),
                Err(_) => None,
            };
            let desired_port = resolve_ws_listener_port(
                std::env::var("INTENTD_TCP_PORT").ok().as_deref(),
                settings_port,
                runtime.ws_options.base_port,
            );

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
/// serve` daemon (WSS listener enabled) hermetically, without touching the
/// shared secrets file. Production always uses [`FileTokenStore`].
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

/// Pin `SettingsRegistry` keys from startup flags/env vars (§9.8 precedence:
/// defaults < config.toml < pins). A pinned key takes the flag value for the
/// process lifetime: the wire reports origin `flag`, `settings.update` /
/// `settings.reset` reject with `-32602` naming the flag, and live-reload
/// ignores the file value while pinned. An invalid pin value (e.g.
/// out-of-range `INTENTD_TCP_PORT`) refuses startup. `INTENTD_TCP_PORT=0` is
/// the E2E ephemeral-port seam, not a real port — it stays unpinned, exactly
/// like an unparseable value (matching [`ws_options_from_env`]).
fn apply_startup_pins(
    registry: &intent_services::SettingsRegistry,
    insecure: bool,
) -> anyhow::Result<()> {
    let pin = |path: &str, value: Value, flag: &str| {
        registry
            .pin(path, value, flag)
            .map_err(|e| anyhow::anyhow!("invalid startup override {flag}: {e}"))
    };
    if insecure {
        // Dev mode hard-disables TLS + bearer auth for the process lifetime.
        pin("server.tls.enabled", json!(false), "--insecure")?;
        pin("server.auth.enabled", json!(false), "--insecure")?;
    }
    if let Some(port) = std::env::var("INTENTD_TCP_PORT")
        .ok()
        .and_then(|v| v.trim().parse::<u16>().ok())
        .filter(|p| *p != 0)
    {
        pin("server.wsApi.port", json!(port), "INTENTD_TCP_PORT")?;
    }
    for (env, path) in [
        ("INTENTD_IDLE_REAP_MINUTES", "agents.idleReapMinutes"),
        (
            "INTENTD_STREAM_RETENTION_HOURS",
            "events.streamRetentionHours",
        ),
    ] {
        // Unset/unparseable falls through to the file value — the same
        // semantics `Config::resolve` applies to these two knobs.
        if let Some(v) = std::env::var(env)
            .ok()
            .and_then(|v| v.trim().parse::<u32>().ok())
        {
            pin(path, json!(v), env)?;
        }
    }
    if let Some(dir) = std::env::var_os("INTENTD_DATA_DIR") {
        pin(
            "storage.dataDir",
            json!(dir.to_string_lossy()),
            "INTENTD_DATA_DIR",
        )?;
    }
    if let Some(dir) = std::env::var_os("INTENTD_WORKSPACES_DIR") {
        pin(
            "workspaces.root",
            json!(dir.to_string_lossy()),
            "INTENTD_WORKSPACES_DIR",
        )?;
    }
    Ok(())
}

/// The boot-time TCP listener decision for `serve` (UDS always serves).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BootWsListener {
    /// No TCP listener at boot; the WSS listener may still be started later
    /// via the `server.wsApi.enabled` runtime toggle.
    None,
    /// `--insecure`: plain-ws TCP listener, no TLS, no bearer auth (dev only).
    InsecurePlainWs,
    /// Effective `server.wsApi.enabled` is true: secure HTTPS+WSS listener.
    SecureWss,
}

/// Resolve the boot-time TCP listener decision: `--insecure` always starts the
/// plain-ws listener (dev posture, overrides the setting); otherwise the
/// secure WSS listener boot-starts iff the effective `server.wsApi.enabled`
/// is true.
fn boot_ws_listener(insecure: bool, ws_api_enabled: bool) -> BootWsListener {
    if insecure {
        BootWsListener::InsecurePlainWs
    } else if ws_api_enabled {
        BootWsListener::SecureWss
    } else {
        BootWsListener::None
    }
}

/// Resolve the port [`start_ws_listener`](DaemonControl) binds. Precedence:
///
/// 1. `INTENTD_TCP_PORT=0` — the E2E ephemeral-port seam (the same seam
///    [`apply_startup_pins`] leaves unpinned): an OS-assigned bind (port 0)
///    wins over any settings value, so a test daemon never races another
///    process for a pre-reserved port (monorepo#1051).
/// 2. The persisted `server.wsApi.port` settings value (a nonzero
///    `INTENTD_TCP_PORT` is already pinned into settings by
///    [`apply_startup_pins`], so settings-first keeps flag > file).
/// 3. A parseable nonzero `INTENTD_TCP_PORT` when settings has no value.
/// 4. The `fallback` default (5181).
fn resolve_ws_listener_port(
    env_port: Option<&str>,
    settings_port: Option<u16>,
    fallback: u16,
) -> u16 {
    let env_port = env_port.and_then(|v| v.trim().parse::<u16>().ok());
    if env_port == Some(0) {
        return 0;
    }
    settings_port.or(env_port).unwrap_or(fallback)
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

/// Local-transport liveness probe: a successful connect means a daemon is
/// listening. Probes the UDS on Unix and the derived named pipe on Windows.
#[cfg(unix)]
async fn uds_is_live(socket_path: &Path) -> bool {
    tokio::net::UnixStream::connect(socket_path).await.is_ok()
}

#[cfg(windows)]
async fn uds_is_live(socket_path: &Path) -> bool {
    use tokio::net::windows::named_pipe::ClientOptions;
    const ERROR_PIPE_BUSY: i32 = 231;
    let Ok(pipe) = intent_transport::pipe_name_for_socket_path(socket_path) else {
        return false;
    };
    match ClientOptions::new().open(&pipe) {
        Ok(_) => true,
        // Every instance momentarily taken still means a live daemon owns it.
        Err(e) => e.raw_os_error() == Some(ERROR_PIPE_BUSY),
    }
}

#[cfg(not(any(unix, windows)))]
async fn uds_is_live(_socket_path: &Path) -> bool {
    false
}

/// Enforce single-instance startup (§5.6). Refuses to start when a live daemon
/// owns the UDS or a live pid holds the pidfile; otherwise removes a stale
/// socket/pidfile whose owner is gone and claims the pidfile with our pid.
async fn acquire_single_instance(config: &Config) -> anyhow::Result<PidFile> {
    // On Unix a leftover socket file marks a candidate daemon: probe it, and
    // remove it when its owner is gone. On Windows named pipes are per-boot
    // kernel objects with no filesystem entry, so probe the pipe directly —
    // there is nothing stale to remove.
    #[cfg(windows)]
    if uds_is_live(&config.socket_path).await {
        anyhow::bail!(
            "intentd is already running on {} — refusing to start a second instance",
            config.socket_path.display()
        );
    }
    #[cfg(not(windows))]
    if config.socket_path.exists() {
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
        // On Windows `pid_is_alive` cannot probe (no signal-0), and the pipe
        // probe above is the authoritative liveness check — a pidfile that
        // survives it is stale by definition and must not block startup.
        let holder_is_alive = if cfg!(windows) {
            false
        } else {
            pid_is_alive(pid)
        };
        if pid != std::process::id() && holder_is_alive {
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

/// Fixed TTL for persisted `agent:tool:call` events, swept on the same tick as
/// the ephemeral families. Tool calls are the dominant share of the event
/// table (87% of live data on the dev seat) and no consumer reads them beyond
/// bounded recent windows — replay uses `agent_message`, live streaming uses
/// the in-memory bus — so 6h comfortably covers every durable reader
/// (`event.agentActivity` / `event.workspaceSummary` default to ≤60-minute
/// windows) while capping steady-state storage at a quarter of the old 24h.
const TOOL_CALL_RETENTION_HOURS: u32 = 6;

/// Upper bound on pages released per `PRAGMA incremental_vacuum(N)` call in
/// the retention loop. 2000 pages ≈ 8 MiB at the 4 KiB default page size —
/// enough to keep up with sweep-driven churn while keeping each call short on
/// the single-connection write pool. A large backlog (e.g. the dev seat's
/// ~54k free pages) drains over successive ticks instead of one long stall.
const INCREMENTAL_VACUUM_MAX_PAGES: u32 = 2000;

/// Spawn the periodic event-retention/compaction sweep (§10.2 / finding F4),
/// or `None` when disabled (`stream_retention_hours == 0`). Each tick deletes
/// high-volume ephemeral events (`agent:stream:*`, `file:*`, `terminal:data`,
/// `host:exec:*`, `script:output`, plus the high-churn state-notification
/// families — see `Store::delete_ephemeral_events_before`) older than the
/// TTL, plus `agent:tool:call` events older than
/// [`TOOL_CALL_RETENTION_HOURS`], while preserving lifecycle/note/task
/// events. After the sweeps each tick runs a
/// bounded `PRAGMA incremental_vacuum` ([`INCREMENTAL_VACUUM_MAX_PAGES`]) to
/// release freelist pages back to the filesystem (effective on
/// incremental-auto-vacuum databases; a no-op otherwise — see
/// `intent_store::connect_write` for the activation story) and
/// `PRAGMA optimize` to keep planner statistics current. The sweep interval
/// is derived from the TTL (≈4×/TTL), clamped so long TTLs still sweep
/// periodically and short ones do not busy-loop. A failed sweep is logged and
/// retried on the next tick (never aborts the loop).
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
        tool_call_ttl_hours = TOOL_CALL_RETENTION_HOURS,
        interval_secs = interval.as_secs(),
        "event retention sweep enabled (agent:stream:*, file:*, terminal:data, host:exec:*, script:output, state-notification churn families, agent:tool:call)"
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
            let tool_cutoff = intent_core::iso_minutes_ago(TOOL_CALL_RETENTION_HOURS as i64 * 60);
            match store.delete_tool_call_events_before(&tool_cutoff).await {
                Ok(removed) if removed > 0 => {
                    tracing::info!(
                        removed,
                        cutoff = tool_cutoff,
                        "event retention sweep trimmed agent:tool:call events"
                    );
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "tool-call retention sweep failed"),
            }
            match store.incremental_vacuum(INCREMENTAL_VACUUM_MAX_PAGES).await {
                Ok(freed) if freed > 0 => {
                    tracing::info!(
                        pages_freed = freed,
                        "incremental vacuum released freelist pages"
                    );
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "incremental vacuum failed"),
            }
            if let Err(e) = store.optimize().await {
                tracing::warn!(error = %e, "PRAGMA optimize failed");
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
            // Same cadence caps the per-minute token-rate history (§5.39) at
            // 24h — at most 1440 minute rows survive a sweep.
            match store.delete_usage_rate_before(&cutoff).await {
                Ok(removed) if removed > 0 => {
                    tracing::info!(removed, cutoff, "usage-rate reaper trimmed minute buckets");
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "usage-rate reaper sweep failed"),
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

/// Interval between merge-pending retry sweeps.
const SANDBOX_MERGE_SWEEP_INTERVAL: Duration = Duration::from_secs(600);

/// Spawn the periodic merge-pending retry sweep. Merge-back otherwise only
/// triggers on agent completion or the manual `sandbox.cow.merge` RPC, so a
/// sandbox stranded `merge_pending` (daemon restart mid-merge, or historical
/// failures like the pre-#592 fetch bug) never self-heals. Each tick calls
/// [`Services::sweep_merge_pending_sandboxes`]: retries every `merge_pending`
/// sandbox (up to the per-sandbox retry cap), skipping agents that are
/// mid-turn. The first tick fires immediately so stuck sandboxes recover on
/// startup; a no-op sweep is silent, an active one logs its tally. Aborted on
/// clean shutdown.
fn spawn_sandbox_merge_retry_loop(services: Services) -> tokio::task::JoinHandle<()> {
    tracing::info!(
        interval_secs = SANDBOX_MERGE_SWEEP_INTERVAL.as_secs(),
        "merge-pending retry sweep enabled"
    );
    tokio::spawn(async move {
        // Crash recovery: a daemon that died mid-merge leaves sandboxes
        // stranded `merging` — invisible to the sweep. No merge can be in
        // flight on a fresh daemon, so reset them to `merge_pending` before
        // the first tick picks them up.
        services.recover_stranded_merging_sandboxes().await;
        let mut ticker = tokio::time::interval(SANDBOX_MERGE_SWEEP_INTERVAL);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let summary = services.sweep_merge_pending_sandboxes().await;
            // INFO only when the sweep attempted work; skip-only passes
            // (e.g. a permanently capped sandbox every tick) log at debug so
            // they do not spam the log forever.
            let attempted =
                summary.merged + summary.conflicts + summary.blocked + summary.errors > 0;
            if attempted {
                tracing::info!(
                    merged = summary.merged,
                    conflicts = summary.conflicts,
                    blocked = summary.blocked,
                    skipped_capped = summary.skipped_capped,
                    skipped_busy = summary.skipped_busy,
                    skipped_raced = summary.skipped_raced,
                    errors = summary.errors,
                    "merge-pending retry sweep completed"
                );
            } else if !summary.is_empty() {
                tracing::debug!(
                    skipped_capped = summary.skipped_capped,
                    skipped_busy = summary.skipped_busy,
                    skipped_raced = summary.skipped_raced,
                    "merge-pending retry sweep: skips only, nothing attempted"
                );
            }
        }
    })
}

/// Test hook: artificial delay (milliseconds) before the watcher registry is
/// started, standing in for a macOS `fseventsd` that takes seconds per
/// `FSEventStreamStart` registration. Lets e2e tests prove the listeners bind
/// off the watcher-init critical path (monorepo#1581). NOTE: this seam is
/// compiled into release binaries too (release-mode e2e runs need it); it is
/// inert unless the namespaced env var is set to a positive integer.
const TEST_WATCHER_INIT_DELAY_MS_ENV: &str = "INTENTD_TEST_WATCHER_INIT_DELAY_MS";

/// Bounded wait for the deferred MCP start sweep to settle at shutdown, so a
/// server spawned mid-handshake lands in the hub map and is covered by the
/// process-group reap (monorepo#1581). Sized to absorb an in-flight handshake
/// while staying well inside the FE sidecar's kill grace.
const MCP_START_JOIN_GRACE: Duration = Duration::from_secs(2);

/// Parse the watcher-init delay override; anything unset, non-numeric, or
/// non-positive disables the hook.
fn test_watcher_init_delay(raw: Option<&str>) -> Option<Duration> {
    raw.and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&ms| ms > 0)
        .map(Duration::from_millis)
}

/// Start the watcher registry (#611) in the background and hold it for the
/// task's lifetime: a filesystem watcher per active workspace (debounced
/// `file:*` events), a narrow `.git` metadata watch per git workspace
/// (external git operations → git-status refresh, monorepo#1397), the skills
/// watcher (`skills:changed`), and the specialists watcher
/// (`specialists:changed`), then workspace lifecycle following so workspaces
/// created/opened after boot gain watching and deleted/closed workspaces are
/// torn down without a restart.
///
/// Spawned rather than awaited inline because each FSEvents registration is a
/// synchronous IPC to `fseventsd` that can take seconds on a loaded machine,
/// which would otherwise delay the UDS bind past the FE sidecar's probe window
/// (monorepo#1581), and run under `block_in_place` so the blocking registration
/// cannot starve the worker driving `cmd_serve` either. The task parks after
/// startup so it owns the registry; aborting the returned handle drops it,
/// tearing down every watcher.
fn spawn_watcher_registry_init(
    bus: EventBus,
    api: Arc<dyn WorkspaceApi>,
    refresher: Arc<GitStatusRefresher>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // `block_in_place`, not a bare `spawn`: the registrations inside are
        // synchronous `fseventsd` IPC that block the calling *thread*, so
        // spawning alone would only move them onto another Tokio worker — on a
        // saturated (or single-worker) runtime that can still be the worker
        // driving `cmd_serve` toward the UDS bind. `block_in_place` hands this
        // worker's remaining tasks to another thread for the duration.
        let registry = tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(async {
                if let Some(delay) = test_watcher_init_delay(
                    std::env::var(TEST_WATCHER_INIT_DELAY_MS_ENV)
                        .ok()
                        .as_deref(),
                ) {
                    tracing::warn!(
                        delay_ms = delay.as_millis() as u64,
                        "watcher registry startup: artificial delay (test seam)"
                    );
                    // A *blocking* sleep, standing in for the synchronous
                    // FSEvents call: a yielding `tokio::time::sleep` would not
                    // exercise worker starvation at all.
                    std::thread::sleep(delay);
                }
                WatcherRegistry::start(bus, api, refresher).await
            })
        });
        tracing::info!("watcher registry ready");
        // Park forever so the registry (and every watcher it owns) stays alive
        // until the handle is aborted at shutdown.
        std::future::pending::<()>().await;
        drop(registry);
    })
}

/// Start the `config.toml` live-reload watcher (§9.8) in the background and
/// hold it for the task's lifetime.
///
/// Spawned rather than started inline for the same reason as
/// [`spawn_watcher_registry_init`]: `notify`'s FSEvents registration is a
/// synchronous IPC to `fseventsd` that can take seconds on a loaded machine,
/// which would otherwise delay the UDS bind past the FE sidecar's probe window
/// (monorepo#1581), and it runs under `block_in_place` for the same reason. The
/// task parks after startup so it owns the watcher guard; aborting the returned
/// handle drops it, ending the OS subscription.
fn spawn_config_watcher_init(
    registry: Arc<intent_services::SettingsRegistry>,
    services: Services,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let watcher_services = services.clone();
        // `ConfigWatcher::start` is synchronous and its `notify` registration
        // blocks the calling thread on `fseventsd` IPC, so it runs under
        // `block_in_place` for the same reason as the watcher registry above.
        // It stays inside the runtime context, so the watcher's own
        // `tokio::spawn` of its debounce loop keeps working.
        let started = tokio::task::block_in_place(|| {
            intent_services::ConfigWatcher::start(registry, move |notice| {
                let services = watcher_services.clone();
                async move { services.apply_external_settings_change(&notice).await }
            })
        });
        let watcher = match started {
            Ok(watcher) => watcher,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    "config.toml live-reload watcher failed to start; \
                     external edits will require a daemon restart"
                );
                return;
            }
        };
        tracing::info!("config.toml live-reload watcher ready");
        // Park forever so the watch stays alive until the handle is aborted.
        std::future::pending::<()>().await;
        drop(watcher);
    })
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
    println!(
        "  cpuPercent: {:.1}",
        r["cpuPercent"].as_f64().unwrap_or(0.0)
    );
    println!("  memoryBytes: {}", r["memoryBytes"].as_u64().unwrap_or(0));
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
    // `resolve_config` parses config.toml strictly — the same gate `serve`
    // applies. A malformed file exits non-zero here with the offending key.
    let config = match resolve_config() {
        Ok(c) => c,
        Err(e) => return to_exit(Err(e)),
    };
    let mut ok = true;

    report_config_status(&config);

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

    report_provider_availability(&config).await;

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
            println!("[ok] TLS cert: none yet (generated on first secure `serve`)");
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
/// providers are installed (resolvable on `PATH` or via a valid
/// `providers.paths` override — monorepo#1065) and, best-effort, which are
/// authenticated. Provider availability never fails `doctor` — a host with no
/// providers installed is a valid (if limited) state.
async fn report_provider_availability(config: &Config) {
    // Same settings source `serve` uses; a missing/unreadable file degrades
    // to no overrides (auto-detection only) rather than failing doctor.
    let provider_paths =
        intent_core::settings_file::SettingsFile::load_or_init(&config.config_path)
            .map(|f| f.providers.paths)
            .unwrap_or_default();
    println!("providers:");
    for provider in intent_providers::discover_providers_with_overrides(&|key| {
        provider_paths
            .get(key)
            .filter(|p| !p.trim().is_empty())
            .cloned()
    }) {
        if let Some(reason) = &provider.gated_off {
            println!("  [--] {} ({})", provider.id, reason);
            continue;
        }
        // npx-only providers (claude-code, pi) never resolve a local binary;
        // report npx availability instead (the auth probe would need a package
        // download, so it is skipped — auth is the external `claude` CLI).
        if let Some(pkg) = provider.npx_only_package {
            match &provider.resolved_path {
                Some(npx) => {
                    // pi additionally requires the `pi` CLI (which the pi-acp
                    // adapter spawns) at PI_CLI_MIN_VERSION+ (monorepo#1662);
                    // append its verdict to the doctor line.
                    let pi_cli = if provider.id == "pi" {
                        Some(report_pi_cli_verdict().await)
                    } else {
                        None
                    };
                    match pi_cli {
                        Some((true, verdict)) => println!(
                            "  [ok] {} via npx: {} -y {pkg}{verdict}",
                            provider.id,
                            npx.display()
                        ),
                        Some((false, verdict)) => {
                            println!("  [--] {} unavailable{verdict}", provider.id)
                        }
                        None => {
                            println!("  [ok] {} via npx: {} -y {pkg}", provider.id, npx.display())
                        }
                    }
                }
                None => println!(
                    "  [--] {} unavailable (npx not found — {} is required)",
                    provider.id,
                    intent_providers::CLAUDE_AGENT_ACP_NODE_REQUIREMENT
                ),
            }
            continue;
        }
        if !provider.installed {
            // Name the actually-missing binary: dual-binary providers
            // (unsloth: opencode + the unsloth CLI) must not blame the
            // primary command when the secondary is what failed to resolve
            // (monorepo#935).
            println!(
                "  [--] {} not installed ({})",
                provider.id,
                intent_providers::not_installed_detail(
                    provider.command,
                    provider.resolved_path.is_some(),
                    provider
                        .secondary_binary
                        .as_ref()
                        .map(|s| (s.command, s.resolved)),
                )
            );
            continue;
        }
        let path = provider
            .resolved_path
            .as_ref()
            .map(|p| p.display().to_string())
            .unwrap_or_default();
        // Spawn via the resolved path when available (grok's binary may live
        // outside PATH at ~/.grok/bin/grok), else the bare command.
        let program = provider
            .resolved_path
            .as_ref()
            .map(|p| p.as_os_str().to_os_string())
            .unwrap_or_else(|| std::ffi::OsString::from(provider.command));
        let auth = check_provider_auth(provider.id, &program, provider.auth_check_args).await;
        println!("  [ok] {} installed: {path}{auth}", provider.id);
    }
}

/// Probe the `pi` CLI (the binary the pi-acp adapter spawns) and render the
/// doctor verdict fragment (monorepo#1662): `(ok, fragment)` where `ok` is
/// whether pi stays available. Missing/too-old CLI names
/// `PI_CLI_REQUIREMENT` (like the Node requirement line for npx); an
/// inconclusive probe is permissive with a warning fragment. The probe is
/// blocking (subprocess, ≤3s), so it runs off the async runtime.
async fn report_pi_cli_verdict() -> (bool, String) {
    use intent_providers::PiCliGate;
    let status = tokio::task::spawn_blocking(intent_services::pi_cli::probe_pi_cli)
        .await
        .expect("pi CLI probe task");
    match &status.gate {
        PiCliGate::Ok => {
            let path = status
                .resolved_path
                .as_ref()
                .map(|p| p.display().to_string())
                .unwrap_or_else(|| status.command.clone());
            let version = status.version_output.as_deref().unwrap_or("unknown");
            (true, format!(" (pi CLI {version}: {path})"))
        }
        PiCliGate::Unknown => (
            true,
            " (pi CLI version unknown — probe inconclusive)".to_string(),
        ),
        gate => {
            let reason = intent_providers::pi_gate_reason(gate)
                .unwrap_or_else(|| intent_providers::PI_CLI_REQUIREMENT.to_string());
            (false, format!(" ({reason})"))
        }
    }
}

/// Best-effort authentication probe for an installed provider: run its
/// `auth_check_args` via the shared CLI probe
/// (`intent_services::provider_auth::check_provider_auth_cli` — the same
/// implementation backing `host.providerAuthStatus`, so doctor and the RPC
/// cannot drift). Returns a trailing status fragment for the doctor line, or
/// empty when no probe applies.
async fn check_provider_auth(
    provider_id: &str,
    program: &std::ffi::OsStr,
    auth_check_args: Option<&[&str]>,
) -> String {
    use intent_services::provider_auth::{check_provider_auth_cli, CliAuthProbe};
    let Some(args) = auth_check_args else {
        return String::new();
    };
    match check_provider_auth_cli(provider_id, program, args).await {
        CliAuthProbe::Authenticated => " (authenticated)".to_string(),
        CliAuthProbe::NotAuthenticated => " (not authenticated)".to_string(),
        CliAuthProbe::StatusUnknown => " (auth status unknown)".to_string(),
        CliAuthProbe::Failed => " (auth check failed)".to_string(),
        CliAuthProbe::TimedOut => " (auth check timed out)".to_string(),
    }
}

/// Doctor config section (§9.8): the file already parsed strictly via
/// `resolve_config`, so report its path plus every env override that will pin
/// a settings key at serve time (flag > file precedence). `--insecure` pins
/// are serve-CLI-scoped and not visible here.
fn report_config_status(config: &Config) {
    println!("[ok] config.toml parsed: {}", config.config_path.display());
    // Mirror `apply_startup_pins` exactly: a numeric env var only pins when
    // it parses (and `INTENTD_TCP_PORT=0` is the ephemeral-port seam, never a
    // pin); the two path overrides pin whenever set.
    let tcp_port_pins = std::env::var("INTENTD_TCP_PORT")
        .ok()
        .and_then(|v| v.trim().parse::<u16>().ok())
        .is_some_and(|p| p != 0);
    let env_u32_pins = |name: &str| {
        std::env::var(name)
            .ok()
            .and_then(|v| v.trim().parse::<u32>().ok())
            .is_some()
    };
    let env_pins = [
        ("INTENTD_TCP_PORT", "server.wsApi.port", tcp_port_pins),
        (
            "INTENTD_IDLE_REAP_MINUTES",
            "agents.idleReapMinutes",
            env_u32_pins("INTENTD_IDLE_REAP_MINUTES"),
        ),
        (
            "INTENTD_STREAM_RETENTION_HOURS",
            "events.streamRetentionHours",
            env_u32_pins("INTENTD_STREAM_RETENTION_HOURS"),
        ),
        (
            "INTENTD_DATA_DIR",
            "storage.dataDir",
            std::env::var_os("INTENTD_DATA_DIR").is_some(),
        ),
        (
            "INTENTD_WORKSPACES_DIR",
            "workspaces.root",
            std::env::var_os("INTENTD_WORKSPACES_DIR").is_some(),
        ),
    ];
    let mut any = false;
    for (env, path, pins) in env_pins {
        if pins {
            println!("  [--] {path} pinned by {env} (file value ignored)");
            any = true;
        }
    }
    if !any {
        println!("  [--] no env overrides; all settings follow config.toml");
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
        .fetch_all(store.read_pool())
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
        .fetch_one(store.write_pool())
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

    // auto_vacuum / freelist: new databases are created with
    // auto_vacuum=INCREMENTAL so the retention loop's bounded
    // incremental_vacuum can release deleted pages. Existing databases stay
    // in NONE mode until a one-time VACUUM, which `intentd serve` runs
    // automatically at startup (monorepo#720 finding 1); print that story
    // plus the manual fallback.
    let auto_vacuum = sqlx::query("PRAGMA auto_vacuum")
        .fetch_one(store.read_pool())
        .await
        .ok()
        .and_then(|row| row.try_get::<i64, _>(0).ok());
    let freelist = store.freelist_count().await;
    match (auto_vacuum, &freelist) {
        (Some(2), Ok(freelist)) => println!(
            "  [ok] auto_vacuum: INCREMENTAL (freelist_count={} pages)",
            freelist
        ),
        (Some(mode), Ok(freelist)) => {
            let label = if mode == 1 { "FULL" } else { "NONE" };
            println!(
                "  [WARN] auto_vacuum: {} (freelist_count={} pages; deleted pages are not returned to the filesystem)",
                label, freelist
            );
            if mode == 0 {
                println!(
                    "         will be activated automatically on the next daemon start (one-time VACUUM); manual fallback (daemon must be STOPPED): sqlite3 <db_path> \"PRAGMA auto_vacuum=INCREMENTAL; VACUUM;\""
                );
            }
        }
        _ => println!("  [WARN] auto_vacuum/freelist_count: failed to query"),
    }

    // Connection pool stats: report size and idle connections for both pools
    let write_pool = store.write_pool();
    let write_size = write_pool.size();
    let write_idle = write_pool.num_idle();
    let read_pool = store.read_pool();
    let read_size = read_pool.size();
    let read_idle = read_pool.num_idle();
    println!(
        "  [ok] write_pool: size={}, idle={} | read_pool: size={}, idle={}",
        write_size, write_idle, read_size, read_idle
    );
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

    #[test]
    fn listener_down_error_is_detected_from_message() {
        let err = json!({
            "code": -32603,
            "message": "TCP listener is not running — ensure the WSS listener is enabled"
        });
        assert!(is_listener_down_error(&err));
        let other = json!({ "code": -32601, "message": "Method not found" });
        assert!(!is_listener_down_error(&other));
        let no_message = json!({ "code": -32603 });
        assert!(!is_listener_down_error(&no_message));
    }

    #[test]
    fn rpc_error_text_prefers_data_over_message() {
        let with_data = json!({
            "code": -32603,
            "message": "Internal error",
            "data": "failed to start WSS listener: port 5181 already in use"
        });
        assert_eq!(
            rpc_error_text(&with_data),
            "failed to start WSS listener: port 5181 already in use"
        );
        let message_only = json!({ "code": -32001, "message": "pairing.getInfo is local-only" });
        assert_eq!(
            rpc_error_text(&message_only),
            "pairing.getInfo is local-only"
        );
        let empty_data = json!({ "code": -32603, "message": "Internal error", "data": "" });
        assert_eq!(rpc_error_text(&empty_data), "Internal error");
        assert_eq!(rpc_error_text(&json!({})), "unknown error");
    }

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
            hooks_max_per_agent: intent_core::config::DEFAULT_HOOKS_MAX_PER_AGENT,
            server_max_outstanding_rpcs: intent_core::config::DEFAULT_SERVER_MAX_OUTSTANDING_RPCS,
            wake_resume_enabled: intent_core::config::DEFAULT_WAKE_RESUME_ENABLED,
            wake_resume_threshold_seconds:
                intent_core::config::DEFAULT_WAKE_RESUME_THRESHOLD_SECONDS,
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
        let result = acquire_single_instance(&config).await;
        assert!(result.is_err(), "a live pidfile owner must refuse startup");
        std::fs::remove_dir_all(&config.data_dir).ok();
    }

    #[tokio::test]
    async fn cleans_stale_pidfile_and_proceeds() {
        let config = temp_config();
        // A pid essentially guaranteed not to be running.
        std::fs::write(&config.pid_path, "2147483640").unwrap();
        let guard = acquire_single_instance(&config)
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
        let _guard = acquire_single_instance(&config)
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
        let result = acquire_single_instance(&config).await;
        assert!(result.is_err(), "a live UDS owner must refuse startup");
        drop(listener);
        std::fs::remove_file(&config.socket_path).ok();
        std::fs::remove_dir_all(&config.data_dir).ok();
    }

    #[test]
    fn boot_ws_listener_insecure_always_starts_plain_ws() {
        // --insecure overrides the setting in both directions.
        assert_eq!(
            boot_ws_listener(true, false),
            BootWsListener::InsecurePlainWs
        );
        assert_eq!(
            boot_ws_listener(true, true),
            BootWsListener::InsecurePlainWs
        );
    }

    #[test]
    fn boot_ws_listener_follows_ws_api_enabled_when_secure() {
        assert_eq!(boot_ws_listener(false, true), BootWsListener::SecureWss);
        assert_eq!(boot_ws_listener(false, false), BootWsListener::None);
    }

    // test_watcher_init_delay takes the raw env value as a parameter, so the
    // seam is testable without process-env mutation races.
    #[test]
    fn watcher_init_delay_parses_positive_milliseconds() {
        assert_eq!(
            test_watcher_init_delay(Some("250")),
            Some(Duration::from_millis(250))
        );
        assert_eq!(
            test_watcher_init_delay(Some(" 5000 ")),
            Some(Duration::from_millis(5000))
        );
    }

    #[test]
    fn watcher_init_delay_inert_unless_positive_integer() {
        // Unset, non-numeric, negative, and zero all leave the hook off.
        assert_eq!(test_watcher_init_delay(None), None);
        assert_eq!(test_watcher_init_delay(Some("")), None);
        assert_eq!(test_watcher_init_delay(Some("soon")), None);
        assert_eq!(test_watcher_init_delay(Some("-1")), None);
        assert_eq!(test_watcher_init_delay(Some("0")), None);
    }

    // resolve_ws_listener_port is pure (the env value is a parameter), so the
    // precedence is testable without process-env mutation races.
    #[test]
    fn ws_listener_port_env_zero_wins_over_settings() {
        // The E2E ephemeral seam: env 0 beats a seeded settings port.
        assert_eq!(resolve_ws_listener_port(Some("0"), Some(5999), 5181), 0);
        assert_eq!(resolve_ws_listener_port(Some(" 0 "), Some(5999), 5181), 0);
        assert_eq!(resolve_ws_listener_port(Some("0"), None, 5181), 0);
    }

    #[test]
    fn ws_listener_port_absent_env_keeps_settings_first() {
        assert_eq!(resolve_ws_listener_port(None, Some(5999), 5181), 5999);
        assert_eq!(resolve_ws_listener_port(None, None, 5181), 5181);
    }

    #[test]
    fn ws_listener_port_nonzero_env_unchanged() {
        // Nonzero env is pinned into settings by apply_startup_pins, so
        // settings-first stays correct; env is only a fallback without one.
        assert_eq!(
            resolve_ws_listener_port(Some("6000"), Some(5999), 5181),
            5999
        );
        assert_eq!(resolve_ws_listener_port(Some("6000"), None, 5181), 6000);
    }

    #[test]
    fn ws_listener_port_unparseable_env_ignored() {
        assert_eq!(
            resolve_ws_listener_port(Some("nope"), Some(5999), 5181),
            5999
        );
        assert_eq!(resolve_ws_listener_port(Some(""), None, 5181), 5181);
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
