//! Protocol version constant (§5.17, §5.7).
//!
//! The protocol version is independent of the daemon crate version and is
//! exposed on the wire in `client.hello` → `server.protocolVersion` and
//! `system.status` → `protocolVersion`. Version 2.9 adds the
//! `stats.getRateHistory` router method for the per-minute all-workspace
//! token-rate history and the optional `parentAgentId` field on
//! `agentSummary.agents[]` entries (§5.1 `WorkspaceAgentInfo`), covering
//! 309 dispatchable method names (273 router + 34 fast-path + 2 aliases) +
//! 1 notification + 4 reverse RPCs.

/// Protocol version exposed on the wire (§5.17, §5.7).
pub const PROTOCOL_VERSION: &str = "2.9";

/// Maximum size in bytes of a single inbound JSON-RPC message accepted by
/// either transport (one newline-delimited UDS frame, one WebSocket text
/// message). Sized to comfortably cover the largest legitimate payload: the
/// 25 MB drafts-attachments cap base64-encodes to ~33.4 MiB on the wire, plus
/// JSON envelope overhead → 40 MiB. Anything larger is rejected without
/// buffering the full payload (monorepo#472).
pub const MAX_INBOUND_MESSAGE_BYTES: usize = 40 * 1024 * 1024;

/// Maximum size of a single outbound frame. A full-tree git.diffs on a huge
/// dirty worktree produced a 277 MiB response and HOL'd the UDS writer for
/// ~38s. Cap matches inbound; producers should also size payloads down.
pub const MAX_OUTBOUND_MESSAGE_BYTES: usize = MAX_INBOUND_MESSAGE_BYTES;
