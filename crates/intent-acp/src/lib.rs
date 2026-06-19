//! intent-acp — ACP client, session multiplexing, agent→BE MCP server (§3.1, §6).
//!
//! Depends on `intent-core` and `intent-providers` (§3.2 rule 3). It must NOT
//! depend on `intent-services`; instead it calls back into business logic
//! through the `WorkspaceApi` trait (defined in core, implemented in services),
//! breaking the `services → acp → services` cycle (§6.8).
//!
//! M3.3 lands the client core: spawning piped-stdio providers ([`spawn`]), the
//! serialized NDJSON JSON-RPC transport ([`transport`]), and the ACP handshake
//! ([`handshake`]). M3.4 adds session new/load/prompt/streaming ([`session`]);
//! M3.5 adds the client-served handlers ([`handler`]) backed by a sandboxed file
//! service ([`fs`]), mediated permission prompts ([`permission`]), and the
//! terminal stub ([`terminal`]). The agent→BE MCP server (M3.7) remains a stub
//! below.

use std::sync::Arc;

use intent_core::WorkspaceApi;

pub mod error;
pub mod fs;
pub mod handler;
pub mod handshake;
pub mod permission;
pub mod session;
pub mod spawn;
pub mod terminal;
pub mod transport;

pub use error::{AcpError, AcpResult, JsonRpcError};
pub use fs::{FileAction, FileChange, FileService};
pub use handler::{ClientRequestHandler, EventSink, SinkEvent};
pub use handshake::{handshake, HandshakeResult};
pub use permission::{
    PermissionOutcome, PermissionPolicy, PermissionRegistry, PermissionRequestData,
    DEFAULT_PERMISSION_TIMEOUT,
};
pub use session::{MappedToolCall, MappedUpdate};
pub use spawn::{spawn_provider, SpawnOptions, SpawnedAgent};
pub use transport::{
    Connection, ConnectionHooks, IncomingNotification, IncomingRequest, DEFAULT_REQUEST_TIMEOUT,
};

#[cfg(test)]
mod tests;

/// ACP client handle. Holds the `WorkspaceApi` callback supplied by the
/// composition root (§6.8). Provider configuration is resolved on demand from
/// the static `intent_providers` registry (§6.9).
pub struct AcpClient {
    _workspace: Arc<dyn WorkspaceApi>,
}

impl AcpClient {
    /// Construct the client with the business-logic callback wired in.
    pub fn new(workspace: Arc<dyn WorkspaceApi>) -> Self {
        Self {
            _workspace: workspace,
        }
    }
}

pub mod mcp_server {
    //! agent→BE MCP server reusing the `WorkspaceApi` surface — stub.
}
