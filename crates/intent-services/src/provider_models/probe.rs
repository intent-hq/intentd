//! One-shot ACP model probe: spawn an adapter, `initialize` + `session/new`,
//! extract the model rows, kill the child.
//!
//! Ports the FE probe loop shared by claude-code / codex / pi / droid
//! (`*.ipc.ts` / `droid-acp-probe.ts`): models may arrive either in the
//! `session/new` result or in a `session/update`-style notification, so the
//! probe watches both. Every path is bounded by a hard overall timeout —
//! the probe never hangs and never panics.

use std::path::PathBuf;
use std::process::Stdio;
use std::time::Duration;

use intent_acp::{Connection, ConnectionHooks, JsonRpcError};
use intent_providers::enhanced_path;
use serde_json::{json, Value};
use tokio::io::AsyncRead;
use tokio::sync::mpsc;

/// Hard cap on the whole probe (mirrors the FE's 15s outer timeout).
const OVERALL_TIMEOUT: Duration = Duration::from_secs(15);
/// Per-request timeout for `initialize` (FE: 4s).
const INITIALIZE_TIMEOUT: Duration = Duration::from_secs(4);
/// Per-request timeout for `session/new` (FE: 8–10s).
const SESSION_NEW_TIMEOUT: Duration = Duration::from_secs(10);
/// Grace window to catch a late model notification after an empty
/// `session/new` result.
const NOTIFICATION_GRACE: Duration = Duration::from_secs(2);

/// How to launch the adapter for a probe.
pub(super) struct AcpProbeCommand {
    program: PathBuf,
    args: Vec<String>,
}

impl AcpProbeCommand {
    /// Run a pinned npm package via `npx -y <package>`.
    pub(super) fn npx(npx: PathBuf, package: &str) -> Self {
        Self {
            program: npx,
            args: vec!["-y".to_string(), package.to_string()],
        }
    }

    /// Run a resolved adapter binary with the given args.
    pub(super) fn binary(bin: PathBuf, args: Vec<String>) -> Self {
        Self { program: bin, args }
    }
}

/// Machine-readable probe failure reasons.
#[derive(Debug)]
pub(super) enum ProbeError {
    /// The adapter process could not be spawned.
    Spawn(String),
    /// A handshake request failed at the transport level or timed out.
    Transport(String),
    /// The adapter returned a JSON-RPC error (auth detection keys off this).
    Rpc(JsonRpcError),
    /// The whole probe hit the hard overall timeout.
    Timeout,
    /// The handshake succeeded but no models were reported.
    Empty,
}

impl std::fmt::Display for ProbeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProbeError::Spawn(e) => write!(f, "failed to spawn adapter: {e}"),
            ProbeError::Transport(e) => write!(f, "probe transport failed: {e}"),
            ProbeError::Rpc(e) => write!(f, "adapter returned an error: {e}"),
            ProbeError::Timeout => write!(f, "model probe timed out"),
            ProbeError::Empty => write!(f, "no models reported"),
        }
    }
}

/// Spawn the adapter, drive the probe handshake, and reap the child.
pub(super) async fn run_acp_probe<F>(
    cmd: AcpProbeCommand,
    extract: F,
) -> Result<Vec<Value>, ProbeError>
where
    F: Fn(&Value) -> Vec<Value>,
{
    let mut command = tokio::process::Command::new(&cmd.program);
    command
        .args(&cmd.args)
        .current_dir(std::env::temp_dir())
        .env("PATH", enhanced_path(Some(&cmd.program)))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(unix)]
    command.process_group(0);

    let mut child = command
        .spawn()
        .map_err(|e| ProbeError::Spawn(format!("{}: {e}", cmd.program.display())))?;
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| ProbeError::Spawn("child stdin not piped".to_string()))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| ProbeError::Spawn("child stdout not piped".to_string()))?;
    let stderr = child
        .stderr
        .take()
        .map(|s| Box::new(s) as Box<dyn AsyncRead + Unpin + Send>);

    let (note_tx, note_rx) = mpsc::unbounded_channel();
    let hooks = ConnectionHooks {
        notifications: Some(note_tx),
        ..Default::default()
    };
    let conn = Connection::new(stdin, stdout, stderr, hooks);

    let result = tokio::time::timeout(OVERALL_TIMEOUT, drive_probe(&conn, note_rx, extract))
        .await
        .unwrap_or(Err(ProbeError::Timeout));

    let _ = child.kill().await;
    result
}

/// `initialize` → `session/new`, watching for model rows in either the
/// `session/new` result or a `session/update`-style notification.
async fn drive_probe<F>(
    conn: &Connection,
    mut notifications: mpsc::UnboundedReceiver<intent_acp::IncomingNotification>,
    extract: F,
) -> Result<Vec<Value>, ProbeError>
where
    F: Fn(&Value) -> Vec<Value>,
{
    let init_params = json!({
        "protocolVersion": 1,
        "clientInfo": { "name": "Intent", "version": env!("CARGO_PKG_VERSION") },
        "clientCapabilities": { "fs": { "readTextFile": false, "writeTextFile": false } },
    });
    conn.request_timeout("initialize", init_params, INITIALIZE_TIMEOUT)
        .await
        .map_err(map_acp_error)?;

    let cwd = std::env::temp_dir();
    let session_params = json!({
        "cwd": cwd.to_string_lossy(),
        "mcpServers": [],
    });

    // Race the session/new response against model notifications: some
    // adapters publish the catalog via a session update before (or instead
    // of) including it in the session/new result.
    let session_new = conn.request_timeout("session/new", session_params, SESSION_NEW_TIMEOUT);
    tokio::pin!(session_new);
    let session_result = loop {
        tokio::select! {
            resp = &mut session_new => break resp,
            note = notifications.recv() => {
                if let Some(note) = note {
                    if is_model_update_method(&note.method) {
                        let models = extract(&note.params);
                        if !models.is_empty() {
                            return Ok(models);
                        }
                    }
                }
            }
        }
    };

    let result = session_result.map_err(map_acp_error)?;
    let models = extract(&result);
    if !models.is_empty() {
        return Ok(models);
    }

    // Empty session/new result: give a late notification a short grace window.
    let deadline = tokio::time::Instant::now() + NOTIFICATION_GRACE;
    while let Ok(Some(note)) = tokio::time::timeout_at(deadline, notifications.recv()).await {
        if is_model_update_method(&note.method) {
            let models = extract(&note.params);
            if !models.is_empty() {
                return Ok(models);
            }
        }
    }
    Err(ProbeError::Empty)
}

/// Notification method names adapters use to publish the model catalog
/// (parity with the FE probes' accepted variants).
fn is_model_update_method(method: &str) -> bool {
    matches!(
        method,
        "sessionUpdate" | "session/update" | "session/updateModels"
    )
}

fn map_acp_error(err: intent_acp::AcpError) -> ProbeError {
    match err {
        intent_acp::AcpError::Rpc(e) => ProbeError::Rpc(e),
        other => ProbeError::Transport(other.to_string()),
    }
}
