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
use intent_transport::serve_uds;
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
    /// Start the daemon and serve JSON-RPC over a Unix-domain socket.
    Serve {
        /// Transport to listen on (only `uds` is supported in this slice).
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
}

#[tokio::main]
async fn main() -> ExitCode {
    init_tracing();
    match Cli::parse().command {
        Command::Serve { listen } => to_exit(cmd_serve(&listen).await),
        Command::Call { method, params } => to_exit(cmd_call(&method, params.as_deref()).await),
        Command::Status => cmd_status().await,
        Command::Doctor => cmd_doctor().await,
    }
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
    if listen != "uds" {
        anyhow::bail!("unsupported --listen '{listen}'; only 'uds' is supported");
    }
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
    let manager = AgentManager::new(
        services.clone(),
        Arc::new(BusEventSink::new(bus.clone())),
        default_process_cap(),
    );
    tracing::info!(
        process_cap = manager.registry().cap(),
        "agent manager ready"
    );
    let api: Arc<dyn WorkspaceApi> = Arc::new(services);
    // Start a filesystem watcher per active workspace with a resolvable on-disk
    // path; each publishes debounced `file:changed` events to the shared bus.
    // The handles are held for the lifetime of `serve` and torn down on return.
    let _watchers = start_workspace_watchers(&bus, api.as_ref()).await;
    tracing::info!(socket = %config.socket_path.display(), "starting intentd");
    serve_uds(api, bus, &config.socket_path, shutdown_signal()).await?;
    // Clean shutdown: kill every spawned agent child and clear the registry
    // (§6.8 teardown). Idle reaping during the run is the M5 `reap_idle` hook.
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
                Ok(status) if status.is_current() => println!(
                    "[ok] migrations current: {} applied {:?}",
                    status.applied.len(),
                    status.applied
                ),
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

    if ok {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
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
