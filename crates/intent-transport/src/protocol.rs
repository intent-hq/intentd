//! Protocol version constant (§5.17, §5.7).
//!
//! The protocol version is independent of the daemon crate version and is
//! exposed on the wire in `client.hello` → `server.protocolVersion` and
//! `system.status` → `protocolVersion`. Version 2.0 is the first frozen
//! surface, covering ~271 methods + aliases + notifications + reverse RPCs.

/// Protocol version exposed on the wire (§5.17, §5.7).
pub const PROTOCOL_VERSION: &str = "2.0";
