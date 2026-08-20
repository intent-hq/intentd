//! Client-served terminal capability (§6.7).
//!
//! The handshake advertises `terminal: true`, so a provider may issue
//! `terminal/*` requests. `intent-acp` must stay free of any PTY dependency, so
//! the actual work is delegated to a [`TerminalHost`] the service layer
//! implements over the unified `intent-pty` host (mirroring how [`EventSink`]
//! breaks the layering for events). When no host is wired (read-only/test
//! wiring), the handler answers with [`unsupported_error`].
//!
//! [`EventSink`]: crate::handler::EventSink

use std::path::PathBuf;

use intent_core::BoxFuture;

use crate::error::{AcpResult, JsonRpcError};

/// JSON-RPC error code for an unsupported method (mirrors `-32601`
/// "Method not found").
const METHOD_NOT_FOUND: i64 = -32601;

/// The `terminal/*` methods a provider may call (parity with the ACP schema).
pub(crate) const TERMINAL_METHODS: [&str; 5] = [
    "terminal/create",
    "terminal/output",
    "terminal/wait_for_exit",
    "terminal/release",
    "terminal/kill",
];

/// Whether `method` is a client-served terminal method.
pub(crate) fn is_terminal_method(method: &str) -> bool {
    TERMINAL_METHODS.contains(&method)
}

/// The error returned for any `terminal/*` request when no PTY host is wired.
pub(crate) fn unsupported_error(method: &str) -> JsonRpcError {
    JsonRpcError {
        code: METHOD_NOT_FOUND,
        message: format!("{method} is not supported (no terminal host wired)"),
        data: None,
    }
}

/// Inputs for an agent's `terminal/create` request, normalized off the ACP
/// schema so the host layer needs no ACP types.
pub struct TerminalCreateParams {
    /// The agent session id (used as the PTY lifetime scope).
    pub session_id: String,
    /// The program to execute.
    pub command: String,
    /// Arguments passed to the program.
    pub args: Vec<String>,
    /// Environment overrides (name, value).
    pub env: Vec<(String, String)>,
    /// Working directory (absolute) when provided.
    pub cwd: Option<PathBuf>,
    /// Retained output budget in bytes; `None` uses the host default.
    pub output_byte_limit: Option<u64>,
}

/// A terminated terminal's exit status (ACP `TerminalExitStatus` shape). `signal`
/// is unavailable through the host abstraction and is always `None`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TerminalExitInfo {
    /// Process exit code, when it exited normally.
    pub exit_code: Option<u32>,
    /// Terminating signal name, when known (always `None` here).
    pub signal: Option<String>,
}

/// A snapshot of a terminal's captured output (ACP `terminal/output` shape).
#[derive(Clone, Debug)]
pub struct TerminalOutputInfo {
    /// Captured output decoded to a UTF-8 (lossy) string.
    pub output: String,
    /// Whether the retained output was truncated to the byte budget.
    pub truncated: bool,
    /// Exit status when the process has already completed.
    pub exit_status: Option<TerminalExitInfo>,
}

/// The PTY operations the client-served `terminal/*` handlers delegate to. The
/// service layer implements this over the unified `intent-pty` host so
/// `intent-acp` stays PTY-free (§6.7).
pub trait TerminalHost: Send + Sync {
    /// Spawn a PTY for the agent session, returning its terminal id.
    fn create(&self, params: TerminalCreateParams) -> BoxFuture<'_, AcpResult<String>>;
    /// Snapshot the terminal's current output (and exit status, if exited).
    fn output(&self, terminal_id: String) -> BoxFuture<'_, AcpResult<TerminalOutputInfo>>;
    /// Block until the terminal's process exits and return its status.
    fn wait_for_exit(&self, terminal_id: String) -> BoxFuture<'_, AcpResult<TerminalExitInfo>>;
    /// Release the terminal, killing its process group.
    fn release(&self, terminal_id: String) -> BoxFuture<'_, AcpResult<()>>;
    /// Kill the terminal's process group.
    fn kill(&self, terminal_id: String) -> BoxFuture<'_, AcpResult<()>>;
}
