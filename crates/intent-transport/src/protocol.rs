//! Protocol version constant (§5.17, §5.7).
//!
//! The protocol version is independent of the daemon crate version and is
//! exposed on the wire in `client.hello` → `server.protocolVersion` and
//! `system.status` → `protocolVersion`. Version 2.0 is the first frozen
//! surface, covering 279 dispatchable method names (250 router + 27 fast-path
//! + 2 aliases) + 1 notification + 4 reverse RPCs.

/// Protocol version exposed on the wire (§5.17, §5.7).
pub const PROTOCOL_VERSION: &str = "2.0";
