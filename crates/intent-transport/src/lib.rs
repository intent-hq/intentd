//! intent-transport — listeners + JSON-RPC router (§16).
//!
//! Depends ONLY on `intent-core` and `intent-services` (§3.2 rule 2); it never
//! touches `intent-store` directly. This guarantees the WS router and the agent
//! MCP server share one code path. Stub only — tokio/axum/rustls/mdns are pulled
//! when transport lands in Wave 3.

pub use intent_core::Result;
pub use intent_services::Services;

pub mod listener {
    //! UDS/TCP listeners — stub.
}

pub mod tls {
    //! TLS termination + SHA-256 fingerprint pinning — stub.
}

pub mod auth {
    //! Bearer auth + origin allow-list — stub.
}

pub mod router {
    //! JSON-RPC method router over the services surface — stub.
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
