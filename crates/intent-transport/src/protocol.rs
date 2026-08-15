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
//! 273 router methods, 312 total. Version 6.6 adds `workspace.transfer.plan`
//! (additive; §5.1): read-only transfer preview — the versioned export
//! manifest (tables, assets, git summary) plus the size estimate (DB rows +
//! assets + estimated git bundle) and pre-flight warnings — 274 router
//! methods, 313 total. Version 6.7 adds the delete grace window
//! (additive; §5.1 / §5.5): the optional `undoDelayMs` param on
//! `workspace.delete` and `agent.delete` (schedules an in-memory pending
//! deletion, returning `{ success, scheduled, deleteAt }`), the
//! `workspace.cancelDelete` / `agent.cancelDelete` router methods, the
//! `workspace:delete-scheduled` / `workspace:delete-cancelled` and
//! `agent:delete-scheduled` / `agent:delete-cancelled` events (§6.5), and
//! the optional `pendingDeleteAt` field on `Workspace` rows and the
//! `AgentLite` / `AgentSession` projections. Scheduling an agent delete
//! does NOT stop the agent (the deadline commit does), and a workspace
//! delete — immediate or committed-from-pending — supersedes pending agent
//! deletes inside it — 276 router methods, 315 total. Version 6.8 adds the
//! `task.setRelations` router method (additive; §5.4, monorepo#1974): writes
//! the first-class `dependsOn` / `conflictsWith` task relations (validated,
//! cycle-checked) that `task.getMyTask` / `task.list` / `note.listTasks`
//! project with the computed `unmetDependsOn` — 277 router methods, 316 total.
//! Version 6.9 adds the staged import surface `workspace.import.begin` /
//! `.chunk` / `.commit` / `.abort` (additive; §5.1): chunked, idempotent
//! upload of a transfer zip archive with an atomic checksum-verified commit
//! — nothing is visible in `workspace.list` until commit succeeds — 281
//! router methods, 320 total. Version 6.10 adds the `repo.warmCache` router
//! method (additive; §5.6): opportunistic background refresh of the
//! daemon-managed repo cache for one GitHub repo — `{ started: true, owner,
//! repo }` returned immediately, the fetch running detached with no events;
//! at most one warm daemon-wide, a second call while one is in flight is
//! rejected with `-32603` carrying `error.data = { code: "warm-in-flight",
//! owner, repo }` — 282 router methods, 321 total. Version 6.11 adds the
//! source-side export surface `workspace.export.start` / `.read` /
//! `.finalize` / `.abort` (additive; §5.1): agents stopped and the transfer
//! zip archive built on a background task (progress/outcome on the new
//! `workspace:transfer:progress` / `:ready` / `:failed` events, §6.5),
//! chunked idempotent download, and a finalize that applies the
//! post-transfer source state (status message + optional archive) — 286
//! router methods, 325 total. Version 6.12 adds the attachment registry
//! (additive; §5.9): `file.placeAttachment` records each placed file under a
//! daemon-minted UUID and additively returns `{ attachmentId, mimeType?,
//! uploadedAt }`, the new `file.getAttachmentInfo` router method serves the
//! registry row (`{ attachmentId, fileName, mimeType?, size, uploadedAt,
//! path, exists }`), file blocks gain the attachment-reference variant
//! (`{ type: "file", attachmentId, fileName, mimeType?, size? }` — exactly
//! one of `data` / `attachmentId`, §5.5), and the MCP `ws.file.getAttachment`
//! binding copies a registered attachment into the calling agent's working
//! directory (§6.8) — 287 router methods, 326 total. Version 6.13 adds the
//! additive `childProcesses` / `childMemoryBytes` / `childMemoryPeakBytes`
//! result fields to `system.status` (§5.7): the process count, aggregate
//! resident memory, and since-start high-water mark of the daemon's whole
//! descendant tree. The count and the instantaneous total are sampled every
//! 5s; the peak additionally takes a 500ms-cadence reading while an ephemeral
//! adapter chain is live, so it can exceed any instantaneous value ever
//! published (monorepo#2107). The existing `memoryBytes` covers only
//! the daemon binary and understates its real cost by more than an order of
//! magnitude once agents are live — measured on a dev seat, a 183 MB daemon
//! owned a 21.5 GB process tree — so a client cannot attribute system memory
//! pressure to agents without these. The peak is carried separately because
//! ephemeral quick-action and model-probe adapters live only seconds and have
//! drained by the time a debug bundle is captured. All three are `null` until
//! the first sample lands; no new methods — 287 router methods, 326 total.
//! Version 6.14 adds the additive `adapter-busy` error shape to
//! `agent.completeOnce` (§5.32): the daemon now bounds concurrently live
//! ephemeral ACP adapters (`agents.maxConcurrentAdapters`, default 6), so an
//! over-limit call queues instead of spawning, and one whose own `timeoutMs`
//! expires while queued is rejected with `-32603` carrying
//! `error.data = { code: "adapter-busy", provider, waitedMs, limit }`. Each
//! adapter chain costs ~610 MB and holds no agent slot, so before the bound a
//! quick-action fan-out could spawn until `server.maxOutstandingRpcs`
//! (monorepo#2062). Nothing was spawned when this error is returned, so a
//! retry is always safe — no new methods, 287 router methods, 326 total.
//! Version 6.15 adds multi git root tracking (additive; §5.6, monorepo#2053):
//! the `gitRoot.list` router method serves the workspace's registered
//! secondary git roots as `{ gitRoots: [...] }` (each wire row is the
//! persisted `WorkspaceGitRoot` plus a live-read `branch?`), six git reads —
//! `git.status`, `git.changes`, `git.diffs`, `git.commits`, `git.showFile`,
//! and `git.branchStatus` (where `workspaceId` + `gitRootId` may stand in
//! for `repoPath`; `repoPath` wins when both are supplied) — gain the
//! optional `gitRootId` param scoping the read to a registered root (an
//! unknown or foreign-workspace id is `-32602` with an identical message;
//! empty/whitespace values read as absent), and the `gitRoot:registered` /
//! `gitRoot:updated` / `gitRoot:unregistered` event family surfaces the
//! daemon-owned root lifecycle (agent registration via the MCP-only
//! `ws.git.registerRoot` / `ws.git.unregisterRoot` / `ws.git.listRoots`
//! bindings, submodule auto-detection, auto-prune, per-root PR discovery) —
//! 288 router methods, 327 total. Version 6.16 adds the staged chunked
//! attachment upload surface `file.attachmentUpload.begin` / `.chunk` /
//! `.commit` / `.abort` (§5.9): chunked, idempotent upload of an attachment
//! payload larger than one RPC frame (16 MiB decoded per chunk, 1 GiB per
//! attachment), with `commit` SHA-256-verifying the assembled payload and
//! placing it through the same collision-safe placement + attachment
//! registry path as `file.placeAttachment` — the commit result is
//! byte-shape-identical to a successful `placeAttachment` result. Sessions
//! are in-memory only: a daemon restart drops them and orphaned staging
//! dirs are swept lazily by the next `begin` — 292 router methods, 331
//! total. Version 6.17 adds the daemon-owned orthogonal `waiting` flag on
//! `Workspace` projections plus the `workspace:waiting-changed` event
//! (additive; §5.1, §6.5), and unwinds the hook/PR-monitor/completion-watch
//! folds from the `displayStatus` `in_progress` promotion — no
//! method-catalog change, 292 router methods, 331 total. Version 6.18 adds
//! the `file.readChunk` router method (additive; §5.9, monorepo#2458): one
//! offset-windowed slice of a workspace file's raw bytes served FE-ward as
//! `{ content (base64), bytesRead, size }` — the binary counterpart of the
//! UTF-8-only `file.read`, with the same within-workspace containment
//! guard, a 16 MiB decoded per-call cap (over-cap → -32602), directory
//! rejection (-32602), and empty-chunk reads at/past EOF — 293 router
//! methods, 332 total.

/// Protocol version exposed on the wire (§5.17, §5.7).
pub const PROTOCOL_VERSION: &str = "6.18";

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
