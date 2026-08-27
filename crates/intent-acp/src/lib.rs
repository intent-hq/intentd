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

#[cfg(unix)]
pub mod descendant_sweep;
pub mod error;
pub mod fs;
pub mod handler;
pub mod handshake;
pub mod mcp_bridge;
pub mod mcp_config;
pub(crate) mod mcp_env;
pub mod mcp_server;
pub mod permission;
pub mod session;
pub mod spawn;
pub mod terminal;
pub(crate) mod tool_restrictions;
pub mod transport;

#[cfg(unix)]
pub use descendant_sweep::{descendant_pids, descendant_pids_many, sweep_escaped_descendants};
pub use error::{
    is_transient_provider_fetch_failure, is_transient_upstream_disconnect, AcpError, AcpResult,
    JsonRpcError, PROMPT_IDLE_TIMEOUT_PREFIX,
};
pub use fs::FileService;
pub use handler::{ClientRequestHandler, EventSink, SinkEvent};
pub use handshake::handshake;
pub use mcp_bridge::{run_stdio_bridge, serve_workspace_mcp_tcp, McpBridge};
pub use mcp_config::{
    apply_baseline_env_to_stdio_servers, normalize_mcp_servers, normalize_spaced_bridge_command,
    to_acp_session_mcp_servers, to_auggie_mcp_config, to_opencode_mcp_config, NormalizedMcpServer,
    NormalizedMcpServers,
};
pub use mcp_env::{build_baseline_mcp_env_from_process, EnvMap};
pub use mcp_server::bindings_prelude_for_bridge;
pub use mcp_server::{
    make_workspace_host_for_bridge, SpecialistModelOption, SpecialistModelOptions,
    WorkspaceMcpServer, MCP_PROTOCOL_VERSION, WORKSPACE_API_SYSTEM_PROMPT_HEADING,
};
pub use permission::{
    PermissionOutcome, PermissionPolicy, PermissionRegistry, PermissionRequestData,
};
pub use session::{MappedToolCall, MappedUpdate};
pub use spawn::{spawn_provider, SpawnOptions};
pub use terminal::{TerminalCreateParams, TerminalExitInfo, TerminalHost, TerminalOutputInfo};
pub use tool_restrictions::{
    get_native_tools_to_remove, get_tool_denylist_for_agent_type, get_tools_to_remove,
    CLAUDE_CODE_ORCHESTRATOR_DISALLOWED_TOOLS, DROID_ORCHESTRATOR_DISALLOWED_TOOLS,
    GROK_ORCHESTRATOR_DISALLOWED_TOOLS, SUBAGENT_TOOLS,
};
pub use transport::{Connection, ConnectionHooks, IncomingNotification, IncomingRequest};

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
