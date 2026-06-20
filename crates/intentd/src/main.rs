//! intentd — the Intent backend daemon and its own control client (§5.7).
//!
//! This binary is the composition root (§3.2 rule 5): it is the only place that
//! wires concrete implementations together (store → services → transport).

use std::process::ExitCode;
use std::sync::Arc;

use clap::{Parser, Subcommand};
use intent_core::{Config, WorkspaceApi};
use intent_services::{
    default_process_cap, AgentManager, BusEventSink, EventBus, FileWatcher, Services,
};
use intent_store::Store;
use intent_transport::{
    ensure_tls_certificate, get_or_create_token, serve_uds, KeyringTokenStore, WsApiServer,
    WsOptions,
};
use serde_json::{json, Value};

mod client;
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
    },
    /// One-shot JSON-RPC call to a running daemon; prints the JSON result.
    Call {
        /// JSON-RPC method, e.g. `workspace.list`.
        method: String,
        /// Params as a JSON object string, e.g. `{"workspaceId":"ws-1"}`.
        #[arg(long)]
        params: Option<String>,
    },
    /// Probe daemon liveness and print basic info.
    Status,
    /// Diagnostics: data-dir writable + SQLite openable/migrations current.
    Doctor,
    /// stdio↔TCP MCP proxy referenced from a generated `--mcp-config`; forwards a
    /// spawned provider's MCP frames to the daemon's in-process server (§6.8).
    McpBridge {
        /// The per-agent MCP listener address to connect to (`host:port`).
        #[arg(long)]
        connect: String,
    },
}

#[tokio::main]
async fn main() -> ExitCode {
    init_tracing();
    match Cli::parse().command {
        Command::Serve { listen } => to_exit(cmd_serve(&listen).await),
        Command::Call { method, params } => to_exit(cmd_call(&method, params.as_deref()).await),
        Command::Status => cmd_status().await,
        Command::Doctor => cmd_doctor().await,
        Command::McpBridge { connect } => to_exit(cmd_mcp_bridge(&connect).await),
    }
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

async fn cmd_serve(listen: &str) -> anyhow::Result<()> {
    let (serve_uds_enabled, serve_tcp_enabled) = match listen {
        "uds" => (true, false),
        "tcp" => (false, true),
        "both" => (true, true),
        other => anyhow::bail!("unsupported --listen '{other}'; expected uds|tcp|both"),
    };
    let config = resolve_config()?;
    std::fs::create_dir_all(&config.data_dir)?;
    let store = Store::open(&config.db_path)
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
    // The event bus shares the store with the services surface so subscribers
    // see the same durable event log that future mutations will publish to.
    let bus = EventBus::new(store.clone());
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
    let api: Arc<dyn WorkspaceApi> = Arc::new(services);
    // Start a filesystem watcher per active workspace with a resolvable on-disk
    // path; each publishes debounced `file:changed` events to the shared bus.
    // The handles are held for the lifetime of `serve` and torn down on return.
    let _watchers = start_workspace_watchers(&bus, api.as_ref()).await;

    // Start the HTTPS+WSS listener when requested. TLS and bearer auth are
    // auto-on for TCP (§5.2/§5.3): the self-signed cert (M5.1) is reused across
    // restarts and the persisted token (M5.2) gates upgrades. The listener runs
    // in the background and is gracefully stopped after the shutdown signal.
    let ws_server = if serve_tcp_enabled {
        let tls =
            ensure_tls_certificate(&config.data_dir).map_err(|e| anyhow::anyhow!(e.to_string()))?;
        get_or_create_token(&KeyringTokenStore).map_err(|e| anyhow::anyhow!(e.to_string()))?;
        let server = WsApiServer::new(
            api.clone(),
            bus.clone(),
            &tls,
            Arc::new(KeyringTokenStore),
            WsOptions::default(),
        )
        .map_err(|e| anyhow::anyhow!(e.to_string()))?;
        let port = server.start().await?;
        tracing::info!(port, fingerprint = %server.fingerprint(), "intentd WSS listening");
        Some(server)
    } else {
        None
    };

    if serve_uds_enabled {
        tracing::info!(socket = %config.socket_path.display(), "starting intentd");
        serve_uds(api, bus, &config.socket_path, shutdown_signal()).await?;
    } else {
        // TCP-only: keep serving in the background until a signal arrives.
        shutdown_signal().await;
    }

    // Clean shutdown: stop the WSS listener (graceful close + port release),
    // stop the PR refresh loop, then kill every spawned agent child and clear
    // the registry (§6.8 teardown). Idle reaping during the run is the M5
    // `reap_idle` hook.
    if let Some(server) = ws_server {
        server.stop().await;
    }
    pr_refresh.abort();
    manager.shutdown().await;
    Ok(())
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
    match rpc_call(&config.socket_path, "workspace.list", json!({})).await {
        Ok(resp) if resp.get("result").is_some() => {
            let count = resp["result"]["workspaces"]
                .as_array()
                .map(|a| a.len())
                .unwrap_or(0);
            println!("intentd: up");
            println!("  socket: {}", config.socket_path.display());
            println!("  workspaces: {count}");
            ExitCode::SUCCESS
        }
        Ok(resp) => {
            println!("intentd: up (rpc error)");
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

    if ok {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
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
