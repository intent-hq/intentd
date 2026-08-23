//! One-shot ACP model probe: spawn an adapter, `initialize` + `session/new`,
//! extract the model rows, kill the child.
//!
//! Ports the FE probe loop shared by claude-code / codex / pi / droid
//! (`*.ipc.ts` / `droid-acp-probe.ts`): models may arrive either in the
//! `session/new` result or in a `session/update`-style notification, so the
//! probe watches both. Every path is bounded by a hard overall timeout —
//! the probe never hangs and never panics.
//!
//! The launch description, staged npx-aware timeouts, spawn, exit
//! observation, and process-group reaping are shared with the one-shot
//! completion runner in [`crate::acp_adapter`]; only the probe's own stage
//! sequencing lives here.

use std::time::Duration;

use intent_acp::{Connection, JsonRpcError};
use serde_json::{json, Value};
use tokio::sync::mpsc;

use crate::acp_adapter::{
    exited_detail, initialize_params, observe_exit_status, reap_child, spawn_adapter, SpawnError,
};

/// The probe's launch description (shared with the one-shot runner).
pub(super) use crate::acp_adapter::AcpAdapterCommand as AcpProbeCommand;

/// Grace window to catch a late model notification after an empty
/// `session/new` result.
const NOTIFICATION_GRACE: Duration = Duration::from_secs(2);

/// Machine-readable probe failure reasons.
#[derive(Debug)]
pub(super) enum ProbeError {
    /// The probe's budget expired while queued for a slot in the daemon-wide
    /// adapter bound — nothing was spawned (monorepo#2062). Treated like any
    /// other probe failure by the caller: the static model list stands in.
    QueueTimeout { waited_ms: u64, limit: u32 },
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
    /// The adapter process exited unsuccessfully before reporting models
    /// (e.g. a corrupt npx cache producing an ENOENT from node); carries the
    /// exit status plus a bounded tail of recent stderr when available.
    Exited(String),
}

impl std::fmt::Display for ProbeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProbeError::QueueTimeout { waited_ms, limit } => write!(
                f,
                "timed out after {waited_ms}ms waiting for a free adapter slot (limit {limit})"
            ),
            ProbeError::Spawn(e) => write!(f, "failed to spawn adapter: {e}"),
            ProbeError::Transport(e) => write!(f, "probe transport failed: {e}"),
            ProbeError::Rpc(e) => write!(f, "adapter returned an error: {e}"),
            ProbeError::Timeout => write!(f, "model probe timed out"),
            ProbeError::Empty => write!(f, "no models reported"),
            ProbeError::Exited(detail) => {
                write!(f, "adapter exited before reporting models: {detail}")
            }
        }
    }
}

/// Claim a slot in the daemon-wide adapter bound, spawn the adapter, drive the
/// probe handshake, and reap the child.
///
/// A probe queues for at most its own setup budget — the window it already
/// accepts for the handshake — and reports the expiry as
/// [`ProbeError::QueueTimeout`] rather than spawning late or hanging. Probes
/// share the bound with one-shot completions because they are the same
/// ~610 MB adapter chain (monorepo#2062); under contention this is visible as
/// a `models.list` refresh falling back to the static list.
pub(super) async fn run_acp_probe<F>(
    cmd: AcpProbeCommand,
    extract: F,
) -> Result<Vec<Value>, ProbeError>
where
    F: Fn(&Value) -> Vec<Value>,
{
    let mut adapter = spawn_adapter(&cmd, cmd.setup_timeout())
        .await
        .map_err(|e| match e {
            SpawnError::QueueTimeout { waited, limit } => ProbeError::QueueTimeout {
                waited_ms: u64::try_from(waited.as_millis()).unwrap_or(u64::MAX),
                limit,
            },
            SpawnError::Spawn(detail) => ProbeError::Spawn(detail),
        })?;

    let result = tokio::time::timeout(
        cmd.setup_timeout(),
        drive_probe(
            &adapter.conn,
            adapter.notifications,
            extract,
            cmd.initialize_timeout(),
            cmd.session_new_timeout(),
        ),
    )
    .await
    .unwrap_or(Err(ProbeError::Timeout));

    let result = match result {
        Ok(models) => Ok(models),
        Err(err) => Err(attribute_early_exit(err, &mut adapter.child, &adapter.conn).await),
    };
    reap_child(&mut adapter.child).await;
    result
}

/// Fold an early adapter exit into the probe error: when the child already
/// exited before the probe could report models, delegate to
/// [`exit_attribution`] with the observed exit status and recent stderr.
async fn attribute_early_exit(
    err: ProbeError,
    child: &mut tokio::process::Child,
    conn: &Connection,
) -> ProbeError {
    if matches!(
        err,
        ProbeError::Spawn(_) | ProbeError::Rpc(_) | ProbeError::QueueTimeout { .. }
    ) {
        return err;
    }
    let status = observe_exit_status(child, conn).await;
    exit_attribution(err, status, &conn.recent_stderr())
}

/// Decide whether a probe error should be re-attributed to a dead adapter
/// (e.g. a corrupt `~/.npm/_npx` entry making node fail with ENOENT):
/// unsuccessful exits carry their exit status plus a bounded tail of recent
/// stderr instead of a generic transport/timeout/empty reason. Spawn and RPC
/// errors pass through untouched (auth detection keys off `Rpc`), as do
/// clean exits — an adapter that finishes the handshake, reports zero
/// models, and exits 0 is genuinely "no models reported".
pub(super) fn exit_attribution(
    err: ProbeError,
    status: Option<std::process::ExitStatus>,
    stderr: &[String],
) -> ProbeError {
    if matches!(err, ProbeError::Spawn(_) | ProbeError::Rpc(_)) {
        return err;
    }
    match exited_detail(status, stderr) {
        Some(detail) => ProbeError::Exited(detail),
        None => err,
    }
}

/// `initialize` → `session/new`, watching for model rows in either the
/// `session/new` result or a `session/update`-style notification.
async fn drive_probe<F>(
    conn: &Connection,
    mut notifications: mpsc::UnboundedReceiver<intent_acp::IncomingNotification>,
    extract: F,
    initialize_timeout: Duration,
    session_new_timeout: Duration,
) -> Result<Vec<Value>, ProbeError>
where
    F: Fn(&Value) -> Vec<Value>,
{
    conn.request_timeout("initialize", initialize_params(), initialize_timeout)
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
    let session_new = conn.request_timeout("session/new", session_params, session_new_timeout);
    tokio::pin!(session_new);
    let mut notifications_open = true;
    let session_result = loop {
        tokio::select! {
            resp = &mut session_new => break resp,
            note = notifications.recv(), if notifications_open => {
                match note {
                    Some(note) => {
                        if is_model_update_method(&note.method) {
                            let models = extract(&note.params);
                            if !models.is_empty() {
                                return Ok(models);
                            }
                        }
                    }
                    // Channel closed (connection dropped the sender): disable
                    // this branch so the select! cannot busy-spin and the
                    // session/new future still resolves (or times out).
                    None => notifications_open = false,
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
        // The transport synthesizes a code-0 "agent stdout closed" JSON-RPC
        // error when the child's stdout closes with requests still pending.
        // That is a transport failure, not an adapter response — keeping it
        // out of `Rpc` lets exit attribution rewrite it (a crashed adapter
        // is the main way stdout closes mid-probe) and keeps auth detection
        // keyed to genuine adapter errors.
        intent_acp::AcpError::Rpc(e) if e.code == 0 && e.message == "agent stdout closed" => {
            ProbeError::Transport(e.message)
        }
        intent_acp::AcpError::Rpc(e) => ProbeError::Rpc(e),
        other => ProbeError::Transport(other.to_string()),
    }
}
