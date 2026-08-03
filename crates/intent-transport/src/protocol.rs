//! Protocol version constant (§5.17, §5.7).
//!
//! The protocol version is independent of the daemon crate version and is
//! exposed on the wire in `client.hello` → `server.protocolVersion` and
//! `system.status` → `protocolVersion`. Version 3.0 removes the
//! `pr.waitForChanges` router method (breaking; superseded by background
//! hooks, §5.40), covering 311 dispatchable method names (275 router +
//! 34 fast-path + 2 aliases) + 1 notification + 4 reverse RPCs. Version 3.1
//! adds the hook TTL (additive; §5.40): `ttlMs` / `expiresAt`, the terminal
//! `expired` state, and the `hook:expired` event — no method-catalog change.
//! Version 4.0 changes the `terminal.list` response shape (breaking; §5.13,
//! monorepo#1334): the bare terminals array is retired in favor of the
//! `{ terminals, daemonBootId }` envelope — no method-catalog change.
//! Version 4.1 adds the execution-environment profile surface (additive;
//! §5.35): the `sandbox.profiles.list` / `sandbox.profiles.update` /
//! `sandbox.options` router methods (278 router methods, 315 dispatchable
//! names) and the `system.capabilities.microvmSupported` field (§5.7).
//! Version 4.2 adds execution-environment selection at workspace creation
//! (additive; §5.1): the `workspace.create` `executionEnvironment` param, the
//! persisted `Workspace.executionEnvironment` field, and the structured
//! `execution-environment-unavailable` / `execution-environment-not-implemented`
//! error payloads (§9) — no method-catalog change.

/// Protocol version exposed on the wire (§5.17, §5.7).
pub const PROTOCOL_VERSION: &str = "4.2";

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
