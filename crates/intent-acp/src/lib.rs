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
//! terminal stub ([`terminal`]). M3.7 adds the agent→BE MCP server
//! ([`mcp_server`]), the universal MCP config conversions ([`mcp_config`]), the
//! baseline-env + redaction helpers ([`mcp_env`]), and the per-agent-type tool
//! denylist ([`tool_restrictions`]).

use std::sync::Arc;

use intent_core::WorkspaceApi;

#[cfg(unix)]
pub mod descendant_sweep;
pub mod error;
pub mod fs;
pub mod handler;
pub mod handshake;
pub mod mcp_bridge;
pub mod mcp_config;
pub mod mcp_env;
pub mod mcp_server;
pub mod permission;
pub mod session;
pub mod spawn;
pub mod terminal;
pub mod tool_restrictions;
pub mod transport;

#[cfg(unix)]
pub use descendant_sweep::{descendant_pids, descendant_pids_many, sweep_escaped_descendants};
pub use error::{AcpError, AcpResult, JsonRpcError, PROMPT_IDLE_TIMEOUT_PREFIX};
pub use fs::{FileAction, FileChange, FileService};
pub use handler::{ClientRequestHandler, EventSink, SinkEvent};
pub use handshake::{handshake, HandshakeResult};
pub use mcp_bridge::{run_stdio_bridge, serve_workspace_mcp_tcp, McpBridge};
pub use mcp_config::{
    apply_baseline_env_to_stdio_servers, normalize_mcp_servers, normalize_spaced_bridge_command,
    to_acp_mcp_servers, to_acp_session_mcp_servers, to_auggie_mcp_config, to_claude_mcp_json,
    to_codex_mcp_overrides, to_opencode_mcp_config, CodexConfigOverride, NormalizedMcpServer,
    NormalizedMcpServers,
};
pub use mcp_env::{
    build_baseline_mcp_env, build_baseline_mcp_env_from_process, is_likely_secret_env_key,
    merge_mcp_env, redact_mcp_env_for_logging, EnvMap, REDACTED_VALUE,
};
pub use mcp_server::{
    bindings_prelude, make_workspace_host, ToolDef, WorkspaceMcpServer, MCP_PROTOCOL_VERSION,
};
pub use permission::{
    PermissionOutcome, PermissionPolicy, PermissionRegistry, PermissionRequestData,
    DEFAULT_PERMISSION_TIMEOUT,
};
pub use session::{MappedToolCall, MappedUpdate};
pub use spawn::{spawn_provider, SpawnOptions, SpawnedAgent};
pub use terminal::{TerminalCreateParams, TerminalExitInfo, TerminalHost, TerminalOutputInfo};
pub use tool_restrictions::{
    get_tool_denylist_for_agent_type, get_tools_to_remove, is_background_agent_type,
    AGENT_CREATION_TOOLS, CONFLICTING_BUILTIN_TOOLS, EXECUTION_TOOLS, EXTERNAL_TOOLS,
    FILE_WRITE_TOOLS, GIT_TOOLS, NOTE_WRITE_TOOLS, SUBAGENT_TOOLS, UNIFIED_WORKSPACE_TOOLS,
    WORKSPACE_WRITE_TOOLS,
};
pub use transport::{
    Connection, ConnectionHooks, IncomingNotification, IncomingRequest, DEFAULT_REQUEST_TIMEOUT,
};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod tests_wsapi5;

/// Test-only process-global env setup. Runs before `main()` — and therefore
/// before any test threads exist, making `set_var` race-free. Node children
/// spawned by lib tests (e.g. MCP fixture servers) inherit this and skip
/// `module.enableCompileCache()`, which would otherwise leave a
/// `node-compile-cache/` residue at the TMPDIR root after the suite.
#[cfg(test)]
#[ctor::ctor(unsafe)]
fn disable_node_compile_cache() {
    std::env::set_var("NODE_DISABLE_COMPILE_CACHE", "1");
}

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
