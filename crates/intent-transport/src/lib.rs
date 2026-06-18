//! intent-transport — listeners + JSON-RPC router (§16).
//!
//! Depends ONLY on `intent-core` and `intent-services` (§3.2 rule 2); it never
//! touches `intent-store` directly. This guarantees the WS router and the agent
//! MCP server share one code path. This slice implements the UDS listener and
//! the transport-agnostic JSON-RPC router; TLS/auth/mdns remain stubs.

pub use intent_core::Result;
pub use intent_services::Services;

pub use listener::serve_uds;
pub use router::handle_message;

mod events;
pub mod listener;
pub mod router;

pub mod tls {
    //! TLS termination + SHA-256 fingerprint pinning — stub.
}

pub mod auth {
    //! Bearer auth + origin allow-list — stub.
}

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
