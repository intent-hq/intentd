//! `terminal.*` + ACP `terminal/*` support over the unified `intent-pty` host
//! (§5.13, §6.7, §12).
//!
//! Two front doors share one [`PtyHost`]:
//!
//! - The transport `terminal.*` RPC (dispatched through [`WorkspaceApi`]) spawns
//!   PTYs scoped to a workspace and fans their output to event subscribers as
//!   `terminal:data` (base64 `chunk`) / `terminal:exit` events (§6.5). Late
//!   attach back-fills via `terminal.getBuffer`, then tails the live events.
//! - The ACP [`TerminalHost`] adapter ([`PtyTerminalHost`]) lets an agent's
//!   client-served `terminal/*` calls run on the same host, scoped to the agent
//!   session id.
//!
//! [`WorkspaceApi`]: intent_core::WorkspaceApi

use std::path::PathBuf;
use std::sync::Arc;

use base64::Engine as _;
use intent_acp::{
    AcpError, AcpResult, TerminalCreateParams, TerminalExitInfo, TerminalHost, TerminalOutputInfo,
};
use intent_core::events::{TERMINAL_DATA, TERMINAL_EXIT};
use intent_core::{now_iso, BoxFuture, Error, Result, WorkspaceId};
use intent_pty::{PtyExit, PtyHost, PtyId, PtySize, SpawnSpec};
use intent_store::NewEvent;
use serde_json::{json, Value};
use std::time::Duration;

use tokio::sync::broadcast::error::{RecvError, TryRecvError};

use crate::events::EventBus;
use crate::{publish_event, system_actor};

/// How often the output streamer polls for natural process exit. The broadcast
/// sender lives in the (still-tracked) session, so a child that exits on its own
/// — without a `terminal.kill` — never closes the live channel; polling the
/// child is what lets the streamer emit `terminal:exit` in that case.
const EXIT_POLL: Duration = Duration::from_millis(25);

/// The default shell spawned when `terminal.create` omits a command. Mirrors the
/// ancestor's reliance on the user's login shell (`$SHELL`, then `/bin/sh`).
fn default_shell() -> String {
    std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".to_string())
}

/// Resolve a wire terminal id (`pty-{n}`) to a [`PtyId`], or `NotFound`.
fn resolve(terminal_id: &str) -> Result<PtyId> {
    PtyId::parse(terminal_id).ok_or_else(|| Error::NotFound(format!("terminal {terminal_id}")))
}

/// Spawn a workspace-scoped PTY for `terminal.create` and begin streaming its
/// output onto the bus; returns `{ terminalId }`.
pub(crate) async fn create(
    pty: Arc<PtyHost>,
    bus: Option<EventBus>,
    workspace_id: WorkspaceId,
    cols: u16,
    rows: u16,
    cwd: Option<String>,
    command: Option<String>,
) -> Result<Value> {
    let mut spec = SpawnSpec::new(workspace_id.as_str(), command.unwrap_or_else(default_shell));
    spec.size = PtySize { rows, cols };
    if let Some(cwd) = cwd {
        spec.cwd = Some(PathBuf::from(cwd));
    }
    let pty_id = pty.spawn(spec)?;
    let terminal_id = pty_id.to_string();
    spawn_output_stream(pty, bus, workspace_id, pty_id, terminal_id.clone());
    Ok(json!({ "terminalId": terminal_id }))
}

/// Write base64-encoded input to a PTY's stdin.
pub(crate) fn write(pty: &PtyHost, terminal_id: &str, data: &str) -> Result<Value> {
    let id = resolve(terminal_id)?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(data)
        .map_err(|e| Error::InvalidParams(format!("invalid base64 data: {e}")))?;
    pty.write(id, &bytes)?;
    Ok(json!({ "ok": true }))
}

/// Resize a PTY's visible area.
pub(crate) fn resize(pty: &PtyHost, terminal_id: &str, cols: u16, rows: u16) -> Result<Value> {
    let id = resolve(terminal_id)?;
    pty.resize(id, PtySize { rows, cols })?;
    Ok(json!({ "ok": true }))
}

/// Kill a PTY; the streamer emits `terminal:exit` once its process ends.
pub(crate) async fn kill(pty: &PtyHost, terminal_id: &str) -> Result<Value> {
    let id = resolve(terminal_id)?;
    pty.kill(id).await;
    Ok(json!({ "ok": true }))
}

/// Snapshot a PTY's scrollback for replay, base64-encoded (optionally keeping
/// only the trailing `max_bytes`).
pub(crate) fn get_buffer(
    pty: &PtyHost,
    terminal_id: &str,
    max_bytes: Option<i64>,
) -> Result<Value> {
    let id = resolve(terminal_id)?;
    let mut bytes = pty.scrollback(id)?;
    if let Some(max) = max_bytes.and_then(|n| usize::try_from(n).ok()) {
        if bytes.len() > max {
            bytes = bytes.split_off(bytes.len() - max);
        }
    }
    let data = base64::engine::general_purpose::STANDARD.encode(&bytes);
    Ok(json!({ "terminalId": terminal_id, "data": data }))
}

/// The workspace's live terminals (`{ terminals: [{ id, alive }] }`).
pub(crate) fn list(pty: &PtyHost, workspace_id: &WorkspaceId) -> Result<Value> {
    let mut terminals: Vec<Value> = pty
        .list_scope(workspace_id.as_str())
        .into_iter()
        .map(|id| json!({ "id": id.to_string(), "alive": pty.is_alive(id) }))
        .collect();
    terminals.sort_by(|a, b| a["id"].as_str().cmp(&b["id"].as_str()));
    Ok(json!({ "terminals": terminals }))
}

/// Attach to a freshly created PTY and fan its output onto the bus as
/// `terminal:data`, emitting a terminal `terminal:exit` when the stream closes.
fn spawn_output_stream(
    pty: Arc<PtyHost>,
    bus: Option<EventBus>,
    workspace_id: WorkspaceId,
    pty_id: PtyId,
    terminal_id: String,
) {
    let attachment = match pty.attach(pty_id) {
        Ok(a) => a,
        Err(_) => return,
    };
    tokio::spawn(async move {
        let mut live = attachment.live;
        // Emit any output captured between spawn and attach exactly once, then
        // tail live chunks (the host guarantees history XOR live, never both).
        if !attachment.backlog.is_empty() {
            emit_data(&bus, &workspace_id, &terminal_id, &attachment.backlog).await;
        }
        loop {
            tokio::select! {
                recv = live.recv() => match recv {
                    Ok(chunk) => emit_data(&bus, &workspace_id, &terminal_id, &chunk).await,
                    Err(RecvError::Lagged(_)) => continue,
                    // A `terminal.kill` tore down the session and dropped the
                    // sender; the process is gone.
                    Err(RecvError::Closed) => break,
                },
                _ = tokio::time::sleep(EXIT_POLL) => {
                    if matches!(pty.try_exit(pty_id), Ok(Some(_))) {
                        // Reaped: drain any output the reader flushed just before
                        // EOF, then stop tailing.
                        drain_pending(&mut live, &bus, &workspace_id, &terminal_id).await;
                        break;
                    }
                }
            }
        }
        let exit = pty.try_exit(pty_id).ok().flatten();
        emit_exit(&bus, &workspace_id, &terminal_id, exit).await;
    });
}

/// Flush any output buffered on the live channel without blocking (used once the
/// child has exited so trailing output still streams before `terminal:exit`).
async fn drain_pending(
    live: &mut tokio::sync::broadcast::Receiver<Arc<Vec<u8>>>,
    bus: &Option<EventBus>,
    workspace_id: &WorkspaceId,
    terminal_id: &str,
) {
    loop {
        match live.try_recv() {
            Ok(chunk) => emit_data(bus, workspace_id, terminal_id, &chunk).await,
            Err(TryRecvError::Lagged(_)) => continue,
            Err(TryRecvError::Empty) | Err(TryRecvError::Closed) => break,
        }
    }
}

/// Publish a `terminal:data` event carrying a base64 output `chunk`.
async fn emit_data(bus: &Option<EventBus>, ws: &WorkspaceId, terminal_id: &str, bytes: &[u8]) {
    let chunk = base64::engine::general_purpose::STANDARD.encode(bytes);
    publish_event(
        bus,
        terminal_event(
            ws,
            TERMINAL_DATA,
            json!({ "terminalId": terminal_id, "chunk": chunk }),
        ),
    )
    .await;
}

/// Publish a self-sufficient `terminal:exit` event.
async fn emit_exit(
    bus: &Option<EventBus>,
    ws: &WorkspaceId,
    terminal_id: &str,
    exit: Option<PtyExit>,
) {
    let exit_code = exit.map(|e| e.exit_code);
    publish_event(
        bus,
        terminal_event(
            ws,
            TERMINAL_EXIT,
            json!({ "terminalId": terminal_id, "exitCode": exit_code, "signal": Value::Null }),
        ),
    )
    .await;
}

/// Build a terminal change event with the daemon system actor.
fn terminal_event(workspace_id: &WorkspaceId, event_type: &str, data: Value) -> NewEvent {
    NewEvent {
        workspace_id: workspace_id.clone(),
        timestamp: now_iso(),
        event_type: event_type.to_string(),
        actor: system_actor(),
        session_id: None,
        correlation_id: None,
        parent_event_id: None,
        data,
    }
}

/// ACP [`TerminalHost`] adapter: runs an agent's client-served `terminal/*`
/// calls on the shared [`PtyHost`], scoped to the agent session id (§6.7).
pub struct PtyTerminalHost {
    pty: Arc<PtyHost>,
}

impl PtyTerminalHost {
    /// Wire the adapter over the shared host.
    pub fn new(pty: Arc<PtyHost>) -> Self {
        Self { pty }
    }
}

impl TerminalHost for PtyTerminalHost {
    fn create(&self, params: TerminalCreateParams) -> BoxFuture<'_, AcpResult<String>> {
        let pty = self.pty.clone();
        Box::pin(async move {
            let mut spec = SpawnSpec::new(params.session_id, params.command);
            spec.args = params.args;
            spec.env = params.env;
            spec.cwd = params.cwd;
            if let Some(limit) = params
                .output_byte_limit
                .and_then(|n| usize::try_from(n).ok())
            {
                spec.scrollback_bytes = limit;
            }
            let pty_id = pty.spawn(spec).map_err(acp_err)?;
            Ok(pty_id.to_string())
        })
    }

    fn output(&self, terminal_id: String) -> BoxFuture<'_, AcpResult<TerminalOutputInfo>> {
        let pty = self.pty.clone();
        Box::pin(async move {
            let id = acp_resolve(&terminal_id)?;
            let limit = pty.scrollback(id).map_err(acp_err)?;
            let exit = pty.try_exit(id).ok().flatten().map(to_exit_info);
            Ok(TerminalOutputInfo {
                output: String::from_utf8_lossy(&limit).into_owned(),
                truncated: false,
                exit_status: exit,
            })
        })
    }

    fn wait_for_exit(&self, terminal_id: String) -> BoxFuture<'_, AcpResult<TerminalExitInfo>> {
        let pty = self.pty.clone();
        Box::pin(async move {
            let id = acp_resolve(&terminal_id)?;
            let exit = pty.wait(id).await.map_err(acp_err)?;
            Ok(to_exit_info(exit))
        })
    }

    fn release(&self, terminal_id: String) -> BoxFuture<'_, AcpResult<()>> {
        let pty = self.pty.clone();
        Box::pin(async move {
            let id = acp_resolve(&terminal_id)?;
            pty.kill(id).await;
            Ok(())
        })
    }

    fn kill(&self, terminal_id: String) -> BoxFuture<'_, AcpResult<()>> {
        let pty = self.pty.clone();
        Box::pin(async move {
            let id = acp_resolve(&terminal_id)?;
            pty.kill(id).await;
            Ok(())
        })
    }
}

/// Map a host error into an ACP terminal error.
fn acp_err(e: Error) -> AcpError {
    AcpError::Terminal(e.to_string())
}

/// Resolve a wire terminal id into a [`PtyId`] for the ACP adapter.
fn acp_resolve(terminal_id: &str) -> AcpResult<PtyId> {
    PtyId::parse(terminal_id)
        .ok_or_else(|| AcpError::Terminal(format!("unknown terminal {terminal_id}")))
}

/// Convert a host [`PtyExit`] into the ACP exit shape (`signal` is unavailable
/// through the host abstraction).
fn to_exit_info(exit: PtyExit) -> TerminalExitInfo {
    TerminalExitInfo {
        exit_code: Some(exit.exit_code),
        signal: None,
    }
}
