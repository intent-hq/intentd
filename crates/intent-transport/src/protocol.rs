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
//! `{ terminals, daemonBootId }` envelope — no method-catalog change. Version
//! 4.1 adds `agent.listActive` (additive; §5.5, monorepo#1395). Version 4.2
//! adds `workspace.diskUsage` and stops populating `Workspace.diskUsage` on
//! `workspace.list` / `workspace.get` rows (§5.1, monorepo#1396) — the field
//! was optional, so row shapes remain valid for existing clients. Version 4.3
//! adds `voice.transcribe` (additive): daemon-side speech-to-text over a
//! pluggable provider (ElevenLabs Scribe | OpenAI) — 278 router methods,
//! 315 total. Version 4.4 structures the `voice.transcribe` no-API-key error
//! data as `{ code: "voice-no-api-key", detail }` (§5.41, monorepo#1448) —
//! same `-32603` / "Internal error" envelope, no method-catalog change.
//! Version 4.5 adds `agent.markSeen` (additive; §5.5): the per-conversation
//! seen marker (`lastSeenMessageId` in session metadata, monotonic advance,
//! `agent:updated` emit, served on the `AgentLite` metadata projection) —
//! 279 router methods, 316 total. Version 5.0 removes the 11 caller-less
//! `pr.*` router methods (breaking; §5.7, monorepo#1506) — `pr.capabilities`,
//! `pr.listComments`, `pr.listReviewComments`, `pr.getReviews`,
//! `pr.listCheckRuns`, `pr.merge`, `pr.updateBranch`, `pr.postComment`,
//! `pr.replyToReviewComment`, `pr.resolveThread`, `pr.createReview` — keeping
//! only `pr.status` and `pr.refresh`; the `github.*` explicit-addressing
//! surface (§5.27) and the MCP-only `ws.pr.snapshot` engine are unchanged —
//! 268 router methods, 305 total. Version 5.1 adds workspace-derived
//! vocabulary for voice dictation (additive; §5.41): the optional
//! `workspaceId` param on `voice.transcribe` and the new
//! `voice.getWorkspaceVocabulary` router method, plus the
//! `voice.workspaceVocabulary.maxTerms` setting — 269 router methods,
//! 306 total. Version 5.2 adds the first-class `reasoningEffort` session
//! field (additive; §5.5): accepted on `agent.create`, patchable/clearable
//! via `agent.update` `changes`, and served on the `AgentSession` /
//! `AgentLite` projections (omitted when unset) — no method-catalog change.
//! Version 6.0 removes the `event.recentFiles` and `event.directoryChanges`
//! router methods (breaking; §5.10, follow-up to intentd#951, matching the
//! v3.0/v5.0 removal precedent): both are superseded end-to-end by the
//! hybrid `file:*` event persistence introduced in the same change, and now
//! return `-32601` (method not found) — no other method-catalog change.
//! Version 6.1 adds the PR-monitor FE surface (additive; §5.42):
//! `prMonitor.list`, `prMonitor.cancel` and `prMonitor.flush` router methods
//! backing the agent-side `ws.pr.monitor` engine, plus the `prMonitor:*`
//! event category — 270 router methods, 307 total. Version 6.2 adds
//! `github.branches.listCached` (additive; §5.27): branch names read from
//! the daemon's local repo cache with no network I/O — 271 router methods,
//! 308 total. Version 6.3 adds `debug.sampleStacks` (additive; §5.43,
//! monorepo#1755): a point-in-time sample of the daemon's own thread stacks
//! rendered as a human-readable text report — 272 router methods, 309 total.
//! Version 6.4 adds the `host.checkNode` and `host.checkGh` fast-path methods
//! (additive; §5.14, monorepo#1891): uncached node/gh detection mirroring
//! `host.checkGit` (`{ available, version?, path? }`) so a fresh install is
//! seen immediately — 272 router methods, 311 total. Version 6.5 adds the
//! `file.placeAttachment` router method (additive; §5.9, monorepo#1948):
//! daemon-mediated attachment placement into the git-ignored
//! `.intent/attachments/` workspace directory with collision-safe naming —
//! 273 router methods, 312 total.

/// Protocol version exposed on the wire (§5.17, §5.7).
pub const PROTOCOL_VERSION: &str = "6.5";

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
