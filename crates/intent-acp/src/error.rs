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

impl fmt::Display for JsonRpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "JSON-RPC error {}: {}", self.code, self.message)
    }
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
}

impl From<serde_json::Error> for AcpError {
    fn from(e: serde_json::Error) -> Self {
        AcpError::Serde(e.to_string())
    }
}

/// Convenience result alias for ACP operations.
pub type AcpResult<T> = Result<T, AcpError>;
