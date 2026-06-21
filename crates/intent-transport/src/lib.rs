//! intent-transport — listeners + JSON-RPC router (§16).
//!
//! Depends ONLY on `intent-core` and `intent-services` (§3.2 rule 2); it never
//! touches `intent-store` directly. This guarantees the WS router and the agent
//! MCP server share one code path. This slice implements the UDS listener and
//! the transport-agnostic JSON-RPC router; TLS/auth/mdns remain stubs.

pub use intent_core::Result;
pub use intent_services::Services;

pub use auth::{
    extract_bearer_token, extract_token, generate_token, get_or_create_token, is_allowed_origin,
    is_auth_enabled, is_discovery_enabled, validate_token, KeyringTokenStore, TokenStore,
};
pub use control::{SystemControl, SystemStatus};
pub use discovery::{detect_display_server, detect_has_display, Discovery, SERVICE_TYPE};
pub use host::{open_external, resolve_is_local, ExternalOpener, OpenExternalError, OsOpener};
pub use listener::serve_uds;
pub use reverse::{ReverseChannel, ReverseError, DEFAULT_REVERSE_TIMEOUT};
pub use router::handle_message;
pub use tls::{cert_fingerprint, ensure_tls_certificate, inspect_cert, CertStatus, TlsCertificate};
pub use ws::{WsApiServer, WsOptions};

pub mod auth;
mod client;
mod conn;
pub mod control;
pub mod discovery;
mod drafts;
mod events;
mod forward;
pub mod host;
pub mod lifecycle;
pub mod listener;
pub mod reverse;
pub mod router;
pub mod tls;
pub mod ws;
