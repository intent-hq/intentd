//! intent-transport — listeners + JSON-RPC router (§16).
//!
//! Depends ONLY on `intent-core` and `intent-services` (§3.2 rule 2); it never
//! touches `intent-store` directly. This guarantees the WS router and the agent
//! MCP server share one code path. This slice implements the UDS listener and
//! the transport-agnostic JSON-RPC router.

pub use intent_core::Result;
pub use intent_services::Services;

pub use auth::{generate_token, get_or_create_token, AsyncTokenStore, FileTokenStore, TokenStore};
pub use context::{is_tcp_connection, with_connection_context};
pub use control::{FileWatchStatus, SystemControl, SystemStatus};
pub use host_env::{
    detect_has_display, detect_host_environment, local_hostname, pretty_hostname, HostEnvironment,
};
#[cfg(windows)]
pub use listener::pipe_name_for_socket_path;
pub use listener::{serve_uds, serve_uds_with_reverse};
pub use protocol::{MAX_INBOUND_MESSAGE_BYTES, MAX_OUTBOUND_MESSAGE_BYTES, PROTOCOL_VERSION};
pub use reverse::{PrimaryReverseGuard, PrimaryReverseRegistry, ReverseChannel};
pub use router::handle_message;
pub use rpc_limit::RpcLimiter;
pub use server::{
    advertised_hosts, collect_bind_interfaces, collect_local_ips, collect_local_ipv6s,
    PairingSnapshot, ServerPairingInfo,
};
pub use tls::{ensure_tls_certificate, inspect_cert, CertStatus, TlsCertificate};
pub use ws::{WsApiServer, WsOptions};

/// Source commit embedded at build time, when the build environment can identify it.
pub const BUILD_COMMIT: Option<&str> = option_env!("INTENTD_EMBEDDED_BUILD_COMMIT");

/// Test-only process-global env setup. Runs before `main()` — and therefore
/// before any test threads exist, making `set_var` race-free. Node children
/// spawned by lib tests (e.g. real provider CLIs in host-ops probes) inherit
/// this and skip `module.enableCompileCache()`, which would otherwise leave a
/// `node-compile-cache/` residue at the TMPDIR root after the suite.
#[cfg(test)]
#[ctor::ctor(unsafe)]
fn disable_node_compile_cache() {
    std::env::set_var("NODE_DISABLE_COMPILE_CACHE", "1");
}

pub mod auth;
pub(crate) mod browser;
pub mod catalog;
mod client;
mod conflate;
mod conn;
pub mod context;
pub mod control;
mod drafts;
mod events;
mod forward;
pub mod host;
pub mod host_env;
mod host_ops;
pub mod lifecycle;
pub mod listener;
pub mod pairing;
mod panic_guard;
mod protocol;
mod provider_setup;
pub mod reverse;
pub mod router;
pub(crate) mod rpc_limit;
pub mod server;
mod subscriptions;
pub mod tls;
pub mod tunnel;
pub mod ws;
