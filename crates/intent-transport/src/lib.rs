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
pub use listener::serve_uds;
pub use router::handle_message;
pub use tls::{cert_fingerprint, ensure_tls_certificate, TlsCertificate};

pub mod auth;
mod events;
pub mod listener;
pub mod router;
pub mod tls;

pub mod heartbeat {
    //! Connection heartbeat — stub.
}

pub mod lifecycle {
    //! Single-flight start/stop, race guards, port backoff (§5.6) — stub.
}

pub mod mdns {
    //! mDNS advertisement of `_intent-ws._tcp` (§5.4) — stub.
}

pub mod client_map {
    //! live-connection → logical `clientId` map + `client.hello` (§16) — stub.
}
