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
    is_auth_enabled, is_discovery_enabled, validate_token, AsyncTokenStore, FileTokenStore,
    TokenStore,
};
pub use control::{SystemControl, SystemStatus};
pub use discovery::{detect_display_server, detect_has_display, Discovery, SERVICE_TYPE};
pub use host::{
    open_external, open_in_editor, pick_application, resolve_is_local, AppPicker, EditorLauncher,
    EditorTarget, ExternalOpener, NoopAppPicker, OpenExternalError, OpenInEditorError,
    OsEditorLauncher, OsOpener, PickApplicationError, ResolvedEditor,
};
pub use listener::serve_uds;
#[cfg(unix)]
pub use listener::serve_uds_with_reverse;
pub use reverse::{
    PrimaryReverseGuard, PrimaryReverseRegistry, ReverseChannel, ReverseError,
    DEFAULT_REVERSE_TIMEOUT,
};
pub use router::handle_message;
pub use tls::{cert_fingerprint, ensure_tls_certificate, inspect_cert, CertStatus, TlsCertificate};
pub use ws::{WsApiServer, WsOptions};

pub mod auth;
pub(crate) mod browser;
mod client;
mod conn;
pub mod control;
pub mod discovery;
mod drafts;
mod events;
mod forward;
pub mod host;
mod host_ops;
pub mod lifecycle;
pub mod listener;
pub mod reverse;
pub mod router;
mod subscriptions;
pub mod tls;
pub mod ws;
