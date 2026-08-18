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
//! methods, 332 total. Version 7.0 reworks the batch `agent.delegate`
//! form (breaking; §5.5, part 2 of monorepo#2457): each `tasks` entry now
//! accepts a bare taskNoteId string OR an object
//! `{ taskNoteId, specialist?, model?, reasoningEffort? }` whose per-task
//! options override the call's top-level defaults (additive half), while
//! the `greedy` batch param is REMOVED — a request passing it is rejected
//! with `-32602` ("greedy was removed; delegate a held task individually
//! to force it past the conflict hold"), the batch result no longer echoes
//! `greedy`, `started` rows never carry conflict overlap, and the
//! `held:conflict` reason now points at individual delegation — no
//! method-catalog change, 293 router methods, 332 total. Version 7.1 adds
//! the ordinal seek param `aroundIndex` on `agent.getConversation`
//! (additive; §5.5): the page containing the 0-based ordinal from the
//! OLDEST message, clamped into `[0, totalMessages - 1]` (negative or
//! non-integer is `-32602`; supplying it together with `aroundMessageId`
//! is `-32602` naming the conflict), carrying the same `nextToken` /
//! `prevToken` seek cursors as `aroundMessageId` pages — no method-catalog
//! change, 293 router methods, 332 total. Version 7.2 adds the
//! `agent.getMessageBlock` router method (additive; §5.5): one FULL content
//! block of one persisted message by block id — the on-demand counterpart
//! of the `projection: "slim"` conversation read — `{ block }`, resolving
//! persisted assistant ids and serve-time synthetic `{messageId}:{index}`
//! ids alike (unknown message/block id is `-32602` naming the id; unknown
//! agent or workspace mismatch is not-found) — 294 router methods, 333
//! total.

use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Protocol version exposed on the wire (§5.17, §5.7).
pub const PROTOCOL_VERSION: &str = "7.2";

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

/// Soft warning threshold for a single JSON-RPC frame in either direction.
/// Frames above this size are far below the hard 40 MiB caps but usually
/// indicate a payload that should be chunked, projected, or paged instead of
/// shipped whole (see the RPC cost contract in `packages/intentd/AGENTS.md`),
/// so the transport emits a throttled `warn` log — log-only, no wire
/// behavior change. Known bulk-transfer methods are exempt (see
/// [`is_bulk_transfer_method`]).
pub const LARGE_MESSAGE_WARN_BYTES: usize = 1024 * 1024;

/// Minimum interval between two large-frame warns for the same method, so a
/// burst of oversized frames (e.g. a chunked upload missing an exemption)
/// does not flood the log.
const LARGE_MESSAGE_WARN_INTERVAL: Duration = Duration::from_secs(1);

/// Methods whose payloads are legitimately large — chunked/base64 bulk
/// transfers in either direction. One shared list serves both the inbound
/// and outbound checks; a method only ever trips one direction in practice.
const BULK_TRANSFER_METHODS: &[&str] = &[
    // Inbound: chunked/base64 uploads and whole-content writes.
    "file.attachmentUpload.chunk",
    "workspace.import.chunk",
    "drafts.set",
    "file.write",
    "note.saveAsset",
    "voice.transcribe",
    // Outbound: chunked/base64 downloads and whole-content reads.
    "workspace.export.read",
    "file.read",
    "file.readChunk",
    "note.readAsset",
    "drafts.get",
];

/// Whether `method` is a known bulk-transfer method exempt from the
/// [`LARGE_MESSAGE_WARN_BYTES`] warning.
pub(crate) fn is_bulk_transfer_method(method: &str) -> bool {
    BULK_TRANSFER_METHODS.contains(&method)
}

/// Direction tag for the large-frame warn log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FrameDirection {
    Inbound,
    Outbound,
}

/// Hard cap on tracked `(method, last-warn)` entries. Method names arrive
/// before catalog validation, so they are client-controlled; without a cap a
/// stream of >1 MiB frames with distinct bogus methods would grow the
/// structure (and its scan) without bound. On overflow the stalest entry is
/// evicted — harmless for a throttle: worst case the evicted method re-warns
/// early.
const MAX_TRACKED_METHODS: usize = 128;

/// Per-method throttle state for large-frame warns. A `Vec` keyed by method
/// name is deliberate: only methods that actually exceed the threshold ever
/// land here, the length is bounded by [`MAX_TRACKED_METHODS`], so the scan
/// stays short and the const-initializable `Vec` avoids a lazy static.
pub(crate) struct LargeFrameWarnThrottle(Mutex<Vec<(String, Instant)>>);

impl LargeFrameWarnThrottle {
    pub(crate) const fn new() -> Self {
        Self(Mutex::new(Vec::new()))
    }

    /// Decide whether a frame of `bytes` for `method` warrants a warn at
    /// `now`: over-threshold, not exempt, and no warn for the same method
    /// within [`LARGE_MESSAGE_WARN_INTERVAL`]. `now` is injectable so tests
    /// can drive the throttle deterministically. Never blocks beyond the
    /// short mutex hold; a `true` return records `now` as the method's last
    /// warn time.
    pub(crate) fn should_warn(&self, method: &str, bytes: usize, now: Instant) -> bool {
        if bytes <= LARGE_MESSAGE_WARN_BYTES || is_bulk_transfer_method(method) {
            return false;
        }
        let mut entries = match self.0.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        match entries.iter_mut().find(|(m, _)| m == method) {
            Some((_, last)) => {
                if now.duration_since(*last) < LARGE_MESSAGE_WARN_INTERVAL {
                    false
                } else {
                    *last = now;
                    true
                }
            }
            None => {
                if entries.len() >= MAX_TRACKED_METHODS {
                    if let Some(stalest) = entries
                        .iter()
                        .enumerate()
                        .min_by_key(|(_, (_, last))| *last)
                        .map(|(i, _)| i)
                    {
                        entries.swap_remove(stalest);
                    }
                }
                entries.push((method.to_string(), now));
                true
            }
        }
    }

    /// Test-only view of how many methods are currently tracked.
    #[cfg(test)]
    fn tracked_len(&self) -> usize {
        match self.0.lock() {
            Ok(guard) => guard.len(),
            Err(poisoned) => poisoned.into_inner().len(),
        }
    }
}

/// Process-wide throttle shared by both directions: one warn per method per
/// second regardless of which side the oversized frame appeared on.
static LARGE_FRAME_WARN_THROTTLE: LargeFrameWarnThrottle = LargeFrameWarnThrottle::new();

/// Emit a throttled `warn` when a JSON-RPC frame exceeds
/// [`LARGE_MESSAGE_WARN_BYTES`] and the method is not a known bulk transfer.
/// Log-only: never rejects or mutates the frame. Frames without a method
/// (unparseable / envelope-invalid) are skipped — there is nothing useful to
/// attribute the size to.
pub(crate) fn warn_if_large_frame(direction: FrameDirection, method: &str, bytes: usize) {
    if method.is_empty() {
        return;
    }
    if !LARGE_FRAME_WARN_THROTTLE.should_warn(method, bytes, Instant::now()) {
        return;
    }
    match direction {
        FrameDirection::Inbound => tracing::warn!(
            method,
            bytes,
            limit = LARGE_MESSAGE_WARN_BYTES,
            "large inbound JSON-RPC frame"
        ),
        FrameDirection::Outbound => tracing::warn!(
            method,
            bytes,
            limit = LARGE_MESSAGE_WARN_BYTES,
            "large outbound JSON-RPC frame"
        ),
    }
}

/// Test-only capturing `tracing` subscriber shared by the large-frame warn
/// tests here and in `panic_guard` / `router`. Hand-rolled because
/// `tracing-subscriber` is not a dependency of this crate.
#[cfg(test)]
pub(crate) mod test_capture {
    use std::fmt::Write as _;
    use std::sync::{Arc, Mutex as StdMutex};

    /// Records `(level, rendered fields + message)` per event.
    #[derive(Clone, Default)]
    pub(crate) struct Capture(Arc<StdMutex<Vec<(tracing::Level, String)>>>);

    impl Capture {
        pub(crate) fn lines(&self) -> Vec<(tracing::Level, String)> {
            self.0.lock().unwrap().clone()
        }
    }

    impl tracing::Subscriber for Capture {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            struct Visitor(String);
            impl tracing::field::Visit for Visitor {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    let _ = write!(self.0, "{}={:?} ", field.name(), value);
                }
            }
            let mut visitor = Visitor(String::new());
            event.record(&mut visitor);
            self.0
                .lock()
                .unwrap()
                .push((*event.metadata().level(), visitor.0));
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    /// Run `f` with a fresh [`Capture`] installed as the thread-default
    /// subscriber and return the recorded events.
    pub(crate) fn capture_events(f: impl FnOnce()) -> Vec<(tracing::Level, String)> {
        let capture = Capture::default();
        tracing::subscriber::with_default(capture.clone(), f);
        capture.lines()
    }
}

#[cfg(test)]
mod tests {
    use super::test_capture::capture_events as capture_warns;
    use super::*;

    const OVER: usize = LARGE_MESSAGE_WARN_BYTES + 1;

    #[test]
    fn bulk_transfer_exemptions_cover_both_directions() {
        for method in [
            "file.attachmentUpload.chunk",
            "workspace.import.chunk",
            "drafts.set",
            "file.write",
            "note.saveAsset",
            "voice.transcribe",
            "workspace.export.read",
            "file.read",
            "file.readChunk",
            "note.readAsset",
            "drafts.get",
        ] {
            assert!(is_bulk_transfer_method(method), "{method} must be exempt");
        }
        for method in ["git.diffs", "agent.getConversation", "workspace.list", ""] {
            assert!(!is_bulk_transfer_method(method), "{method:?} must warn");
        }
    }

    #[test]
    fn should_warn_fires_only_over_threshold() {
        let throttle = LargeFrameWarnThrottle::new();
        let now = Instant::now();
        assert!(!throttle.should_warn("a.b", LARGE_MESSAGE_WARN_BYTES, now));
        assert!(!throttle.should_warn("a.b", 10, now));
        assert!(throttle.should_warn("a.b", LARGE_MESSAGE_WARN_BYTES + 1, now));
    }

    #[test]
    fn should_warn_suppresses_exempt_methods() {
        let throttle = LargeFrameWarnThrottle::new();
        let now = Instant::now();
        assert!(!throttle.should_warn("file.attachmentUpload.chunk", OVER, now));
        assert!(!throttle.should_warn("workspace.export.read", OVER, now));
    }

    #[test]
    fn should_warn_throttles_per_method_per_second() {
        let throttle = LargeFrameWarnThrottle::new();
        let t0 = Instant::now();
        assert!(throttle.should_warn("a.b", OVER, t0));
        // Same method within 1s: silently skipped.
        assert!(!throttle.should_warn("a.b", OVER, t0 + Duration::from_millis(500)));
        assert!(!throttle.should_warn("a.b", OVER, t0 + Duration::from_millis(999)));
        // At/after the interval: fires again and re-arms from that instant.
        assert!(throttle.should_warn("a.b", OVER, t0 + Duration::from_secs(1)));
        assert!(!throttle.should_warn("a.b", OVER, t0 + Duration::from_millis(1500)));
        assert!(throttle.should_warn("a.b", OVER, t0 + Duration::from_secs(2)));
    }

    #[test]
    fn throttle_is_independent_per_method() {
        let throttle = LargeFrameWarnThrottle::new();
        let t0 = Instant::now();
        assert!(throttle.should_warn("a.b", OVER, t0));
        // A different method is not throttled by a.b's recent warn.
        assert!(throttle.should_warn("c.d", OVER, t0 + Duration::from_millis(10)));
        assert!(!throttle.should_warn("c.d", OVER, t0 + Duration::from_millis(20)));
    }

    #[test]
    fn throttle_caps_tracked_methods_and_evicts_stalest() {
        let throttle = LargeFrameWarnThrottle::new();
        let t0 = Instant::now();
        // Fill the structure with distinct (client-controlled) method names,
        // each a millisecond apart so staleness ordering is deterministic.
        for i in 0..MAX_TRACKED_METHODS {
            assert!(throttle.should_warn(
                &format!("bogus.m{i}"),
                OVER,
                t0 + Duration::from_millis(i as u64)
            ));
        }
        assert_eq!(throttle.tracked_len(), MAX_TRACKED_METHODS);
        // One more distinct method: warns, but the map does not grow — the
        // stalest entry (bogus.m0) is evicted instead.
        let t_new = t0 + Duration::from_millis(MAX_TRACKED_METHODS as u64);
        assert!(throttle.should_warn("bogus.overflow", OVER, t_new));
        assert_eq!(throttle.tracked_len(), MAX_TRACKED_METHODS);
        // Surviving entries keep their throttle state: bogus.m1 warned at
        // t0+1ms and stays suppressed inside its 1s window.
        assert!(!throttle.should_warn("bogus.m1", OVER, t0 + Duration::from_millis(500)));
        // The evicted method (bogus.m0, the stalest) re-warns early with a
        // fresh entry — the documented worst case, not a suppression bug —
        // and the map still does not grow.
        assert!(throttle.should_warn("bogus.m0", OVER, t_new + Duration::from_millis(1)));
        assert_eq!(throttle.tracked_len(), MAX_TRACKED_METHODS);
    }

    // The tests below go through `warn_if_large_frame`, which uses the
    // process-global throttle — each uses method names unique to that test so
    // parallel test threads cannot throttle each other.

    #[test]
    fn warn_if_large_frame_emits_one_warn_with_method_and_bytes() {
        let lines = capture_warns(|| {
            warn_if_large_frame(FrameDirection::Inbound, "test.inboundEmit", OVER);
        });
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0].0, tracing::Level::WARN);
        assert!(lines[0].1.contains("large inbound JSON-RPC frame"));
        assert!(lines[0].1.contains("method=\"test.inboundEmit\""));
        assert!(lines[0].1.contains(&format!("bytes={OVER}")));
        assert!(lines[0]
            .1
            .contains(&format!("limit={LARGE_MESSAGE_WARN_BYTES}")));
    }

    #[test]
    fn warn_if_large_frame_outbound_direction_message() {
        let lines = capture_warns(|| {
            warn_if_large_frame(FrameDirection::Outbound, "test.outboundEmit", OVER);
        });
        assert_eq!(lines.len(), 1);
        assert!(lines[0].1.contains("large outbound JSON-RPC frame"));
    }

    #[test]
    fn warn_if_large_frame_skips_small_exempt_and_methodless_frames() {
        let lines = capture_warns(|| {
            warn_if_large_frame(
                FrameDirection::Inbound,
                "test.underThreshold",
                LARGE_MESSAGE_WARN_BYTES,
            );
            warn_if_large_frame(FrameDirection::Inbound, "file.attachmentUpload.chunk", OVER);
            warn_if_large_frame(FrameDirection::Outbound, "workspace.export.read", OVER);
            warn_if_large_frame(FrameDirection::Inbound, "", OVER);
        });
        assert!(lines.is_empty(), "unexpected warns: {lines:?}");
    }

    #[test]
    fn warn_if_large_frame_throttles_repeat_warns() {
        let lines = capture_warns(|| {
            warn_if_large_frame(FrameDirection::Inbound, "test.throttleRepeat", OVER);
            warn_if_large_frame(FrameDirection::Inbound, "test.throttleRepeat", OVER);
            warn_if_large_frame(FrameDirection::Outbound, "test.throttleRepeat", OVER);
        });
        assert_eq!(lines.len(), 1, "expected exactly one warn: {lines:?}");
    }
}
