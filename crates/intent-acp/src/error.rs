//! Error type for the ACP client transport and handshake (§6.2–§6.4).

use std::fmt;

/// A JSON-RPC 2.0 error object returned by the agent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonRpcError {
    /// Numeric error code.
    pub code: i64,
    /// Human-readable error message.
    pub message: String,
    /// Optional structured error data.
    pub data: Option<serde_json::Value>,
}

/// Cap on the rendered `data` payload appended by [`JsonRpcError`]'s Display
/// (monorepo#519): `data` is provider-controlled and unbounded, and the
/// rendered string flows into `stop_reason` persistence, `agent:failed`
/// events, and logs. Sized so real actionable details (e.g. the ChatGPT
/// backend 400 nested by codex-acp, ~300 bytes — monorepo#479) render in
/// full while pathological payloads stay bounded.
pub(crate) const MAX_RENDERED_DATA_BYTES: usize = 1024;

impl fmt::Display for JsonRpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "JSON-RPC error {}: {}", self.code, self.message)?;
        // Providers put the real failure detail in `data` (e.g. codex-acp's
        // -32603 "Internal error" carries the actual cause there), so append
        // it when present. Strings render raw (no JSON quoting noise); other
        // values render as compact JSON; null/absent/empty add nothing. The
        // appended portion is capped at [`MAX_RENDERED_DATA_BYTES`] — the
        // leading detail is where the actionable message lives.
        match &self.data {
            None | Some(serde_json::Value::Null) => Ok(()),
            Some(serde_json::Value::String(s)) if s.is_empty() => Ok(()),
            Some(serde_json::Value::String(s)) => write_bounded_data(f, s),
            Some(other) => write_bounded_data(f, &other.to_string()),
        }
    }
}

/// Append `: {data}` to the rendered error, truncating past
/// [`MAX_RENDERED_DATA_BYTES`] with an ellipsis marker (backing off to the
/// previous char boundary so multi-byte chars never split).
fn write_bounded_data(f: &mut fmt::Formatter<'_>, data: &str) -> fmt::Result {
    if data.len() <= MAX_RENDERED_DATA_BYTES {
        return write!(f, ": {data}");
    }
    let mut end = MAX_RENDERED_DATA_BYTES;
    while !data.is_char_boundary(end) {
        end -= 1;
    }
    write!(f, ": {}… [truncated]", &data[..end])
}

/// Errors raised while spawning, talking to, or handshaking with an ACP agent.
#[derive(Debug, thiserror::Error)]
pub enum AcpError {
    /// The provider process could not be spawned or its pipes were missing.
    #[error("failed to spawn provider: {0}")]
    Spawn(String),

    /// The transport (writer/reader task or pipe) is closed or broke.
    #[error("transport closed: {0}")]
    Transport(String),

    /// A request did not receive a response within the timeout.
    #[error("request `{0}` timed out")]
    Timeout(String),

    /// The agent returned a JSON-RPC error response.
    #[error("{0}")]
    Rpc(JsonRpcError),

    /// Serialization/deserialization of a payload failed.
    #[error("serialization error: {0}")]
    Serde(String),

    /// The agent requires authentication; carries the provider login hint.
    #[error("{0}")]
    Auth(String),

    /// A protocol-level violation (malformed/unexpected message).
    #[error("protocol error: {0}")]
    Protocol(String),

    /// A client-served filesystem request failed: either a sandbox violation
    /// (path outside the session worktree) or an underlying IO error (§6.7).
    #[error("filesystem error: {0}")]
    Fs(String),

    /// A client-served terminal request failed on the PTY host (spawn failure,
    /// unknown terminal id, or IO error) (§6.7).
    #[error("terminal error: {0}")]
    Terminal(String),
}

impl From<serde_json::Error> for AcpError {
    fn from(e: serde_json::Error) -> Self {
        AcpError::Serde(e.to_string())
    }
}

/// Convenience result alias for ACP operations.
pub type AcpResult<T> = Result<T, AcpError>;
