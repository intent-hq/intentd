//! Unified `workspace.create` provisioning progress (PROTOCOL §5.1 / §6.5).
//!
//! When a create carries a client-supplied `progressId`, every
//! `git:clone:progress` / `git:clone:done` frame it emits echoes the id as
//! `data.progressId`, and the daemon normalizes progress across the whole
//! provisioning pipeline to a single 0–100 scale — the network/cache phases
//! map into 0–85 and the local provisioning tail (checkout / worktree /
//! CoW copy / finalizing) fills 85–100. Provisioning paths that historically
//! streamed nothing (linked worktree, CoW checkout, direct/isNewRepo) emit
//! coarse milestone frames through the same reporter.
//!
//! [`CreateProgress`] owns the invariants: the reported percent never
//! decreases within one create, identical consecutive frames are deduped,
//! and exactly one terminal `git:clone:done` is emitted per create — the
//! `workspace.create` wrapper calls [`CreateProgress::done_ok`] /
//! [`CreateProgress::done_err`] on every exit, and the clone/cache arms
//! defer their legacy terminal frames when a reporter is active.
//!
//! Absent a `progressId` the reporter is never constructed and every event
//! path behaves exactly as before (rollback safety: the field is additive).

use std::sync::Mutex;

use intent_core::{CloneErrorCategory, WorkspaceId};

use crate::clone_ops;
use crate::events::EventBus;

/// Upper bound of the clone/cache segment on the unified scale: network-bound
/// work maps into `0..=85`, the local provisioning tail into `85..=100`.
pub(crate) const CLONE_SEGMENT_END: u32 = 85;

/// Boundary between the superproject clone/cache work and the submodule
/// work inside the clone segment: superproject phases map into
/// `0..=70`, submodule progress into `70..=`[`CLONE_SEGMENT_END`].
pub(crate) const SUBMODULE_SEGMENT_START: u32 = 70;

/// Map a local `0..=100` progress value into the `lo..=hi` segment of the
/// unified scale. Values above 100 clamp; `lo >= hi` collapses to `lo`.
pub(crate) fn map_segment(lo: u32, hi: u32, local_percent: u32) -> u32 {
    let (lo, hi) = (lo.min(100), hi.min(100));
    if hi <= lo {
        return lo;
    }
    lo + (hi - lo) * local_percent.min(100) / 100
}

/// Weighted sub-segments for the `git clone --progress` phases, so a clone's
/// per-phase 0–100 stderr percentages become one non-decreasing 0–100 series
/// (receiving dominates wall-clock time; counting/compressing are cheap).
/// Unknown phases span the whole range (their raw percent passes through).
/// Shared with the submodule-aware parser ([`crate::clone_ops`]), which uses
/// it to weight per-submodule clone phases inside one submodule's slice.
pub(crate) fn clone_phase_segment(phase: &str) -> (u32, u32) {
    match phase {
        "starting" => (0, 0),
        "counting" => (0, 5),
        "compressing" => (5, 15),
        "receiving" => (15, 70),
        "resolving" => (70, 90),
        "checkout" => (90, 100),
        "complete" => (100, 100),
        _ => (0, 100),
    }
}

/// Normalize a clone stderr phase + percent into the unified scale's
/// `0..=`[`CLONE_SEGMENT_END`] clone segment. Superproject phases map into
/// `0..=`[`SUBMODULE_SEGMENT_START`]; the `submodules` phase (whose percent
/// is already 0–100 across all submodules, produced by the submodule-aware
/// parser) fills [`SUBMODULE_SEGMENT_START`]`..=`[`CLONE_SEGMENT_END`];
/// `complete` tops the whole clone segment.
pub(crate) fn clone_overall_percent(phase: &str, percent: u32) -> u32 {
    match phase {
        "submodules" => map_segment(SUBMODULE_SEGMENT_START, CLONE_SEGMENT_END, percent),
        "complete" => CLONE_SEGMENT_END,
        _ => {
            let (lo, hi) = clone_phase_segment(phase);
            map_segment(0, SUBMODULE_SEGMENT_START, map_segment(lo, hi, percent))
        }
    }
}

/// Sub-segment of the provisioning tail used for the direct-hydration
/// submodule population (`git submodule update … --progress` in the
/// checkout): after the `checkout`/`cow-copy` milestone (88), before the
/// `finalizing` milestone (95).
const HYDRATE_SUBMODULE_LO: u32 = 89;
const HYDRATE_SUBMODULE_HI: u32 = 94;

/// Band the warm-cache refresh's `git fetch --progress` stream maps into on
/// the unified scale: from the `fetch` step milestone to the `reset` step
/// milestone, so the fetch visibly advances between the two instead of
/// freezing at the former.
const CACHE_REFRESH_FETCH_LO: u32 = 5;
const CACHE_REFRESH_FETCH_HI: u32 = 60;

/// Normalize a refresh-fetch stderr phase + percent into the
/// [`CACHE_REFRESH_FETCH_LO`]`..=`[`CACHE_REFRESH_FETCH_HI`] band: the same
/// per-phase weighting a clone gets ([`clone_phase_segment`]), compressed
/// into the fetch→reset slice of the cache segment.
fn refresh_fetch_percent(phase: &str, percent: u32) -> u32 {
    let (lo, hi) = clone_phase_segment(phase);
    map_segment(
        CACHE_REFRESH_FETCH_LO,
        CACHE_REFRESH_FETCH_HI,
        map_segment(lo, hi, percent),
    )
}

/// One frame the cache-ensure pump wants reported: `Clone` frames carry the
/// parser's raw (phase, percent) and go through
/// [`CreateProgress::clone_progress`] (cache-miss full clone — mapping
/// unchanged); `Milestone` frames carry an already-unified percent and go
/// through [`CreateProgress::milestone`].
enum CacheFrame {
    Clone(&'static str, u32, String),
    Milestone(&'static str, u32, String),
}

/// State machine translating raw [`intent_git::repo_cache::CacheEnsureEvent`]s
/// into [`CacheFrame`]s. `CloneChunk`s arriving with no prior `Step` event
/// belong to a cache-miss full clone (raw clone mapping); once `Step("fetch")`
/// has been seen the ensure is a warm-cache refresh, and subsequent
/// `CloneChunk`s (the refresh's `git fetch --progress` stderr) map into the
/// fetch→reset band via [`refresh_fetch_percent`], labeled `cache` so the
/// stream stays cache-phased. `Step("re-clone")` (the ensure escalated to a
/// wipe + from-scratch clone) reverts to the raw clone mapping with a fresh
/// clone parser, since the chunks that follow are a new clone's stderr.
struct CacheEnsurePump {
    // One parser per stream shape: the ensure clone/fetch is
    // superproject-shaped; the refresh submodule update is
    // submodule-scoped throughout.
    clone_parser: crate::clone_ops::SubmoduleAwareParser,
    sub_parser: crate::clone_ops::SubmoduleAwareParser,
    refresh_fetch: bool,
}

impl CacheEnsurePump {
    fn new() -> Self {
        Self {
            clone_parser: crate::clone_ops::SubmoduleAwareParser::for_clone(),
            sub_parser: crate::clone_ops::SubmoduleAwareParser::for_submodule_update(),
            refresh_fetch: false,
        }
    }

    fn handle(&mut self, ev: intent_git::repo_cache::CacheEnsureEvent) -> Vec<CacheFrame> {
        use intent_git::repo_cache::CacheEnsureEvent as Ev;
        match ev {
            Ev::CloneChunk(text) => {
                let frames = self.clone_parser.parse(&text);
                if self.refresh_fetch {
                    frames
                        .into_iter()
                        .map(|(phase, pct, msg)| {
                            CacheFrame::Milestone("cache", refresh_fetch_percent(phase, pct), msg)
                        })
                        .collect()
                } else {
                    frames
                        .into_iter()
                        .map(|(phase, pct, msg)| CacheFrame::Clone(phase, pct, msg))
                        .collect()
                }
            }
            Ev::SubmoduleChunk(text) => self
                .sub_parser
                .parse(&text)
                .into_iter()
                .map(|(phase, pct, msg)| CacheFrame::Clone(phase, pct, msg))
                .collect(),
            // Refresh step boundaries: coarse milestones so a warm-cache
            // refresh moves even when its steps stream nothing. Percents
            // sit on the unified scale (monotonic clamp orders them
            // against any streamed frames).
            Ev::Step(step) => {
                if step == "fetch" {
                    self.refresh_fetch = true;
                }
                if step == "re-clone" {
                    // The cache was wiped: subsequent CloneChunks are a
                    // from-scratch clone, so drop the refresh-fetch band
                    // mapping and start a fresh parser for the new stream.
                    // The milestone sits at the cache band floor — the
                    // reporter's monotonic clamp keeps any higher percent
                    // already reached (only the message changes).
                    self.refresh_fetch = false;
                    self.clone_parser = crate::clone_ops::SubmoduleAwareParser::for_clone();
                }
                let (phase, pct, msg) = match step {
                    "fetch" => (
                        "cache",
                        CACHE_REFRESH_FETCH_LO,
                        "Refreshing repository cache...",
                    ),
                    "re-clone" => (
                        "cache",
                        CACHE_REFRESH_FETCH_LO,
                        "Rebuilding repository cache...",
                    ),
                    "reset" => ("cache", CACHE_REFRESH_FETCH_HI, "Updating cache branch..."),
                    "submodule-sync" => (
                        "submodules",
                        SUBMODULE_SEGMENT_START,
                        "Syncing submodules...",
                    ),
                    "submodule-update" => (
                        "submodules",
                        SUBMODULE_SEGMENT_START,
                        "Updating submodules...",
                    ),
                    "clean" => (
                        "cache",
                        CLONE_SEGMENT_END - 1,
                        "Cleaning repository cache...",
                    ),
                    _ => return Vec::new(),
                };
                vec![CacheFrame::Milestone(phase, pct, msg.to_string())]
            }
        }
    }
}

/// Bridge a repo-cache ensure to `reporter`: returns the callback to pass to
/// `intent_git::repo_cache::ensure_cached_repo_with_progress` plus the pump
/// task translating its raw events into unified-scale frames. The callback is
/// invoked on blocking/drain threads and only does a channel send; the pump
/// parses chunks (submodule-aware) and reports. The pump ends when the last
/// callback clone is dropped — await the handle to flush buffered frames
/// before emitting any later milestone.
pub(crate) fn cache_ensure_reporter(
    reporter: std::sync::Arc<CreateProgress>,
) -> (
    intent_git::repo_cache::CacheEnsureProgress,
    tokio::task::JoinHandle<()>,
) {
    use intent_git::repo_cache::CacheEnsureEvent as Ev;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Ev>();
    let handle = tokio::spawn(async move {
        let mut pump = CacheEnsurePump::new();
        while let Some(ev) = rx.recv().await {
            for frame in pump.handle(ev) {
                match frame {
                    CacheFrame::Clone(phase, pct, msg) => {
                        reporter.clone_progress(phase, pct, &msg).await;
                    }
                    CacheFrame::Milestone(phase, pct, msg) => {
                        reporter.milestone(phase, pct, &msg).await;
                    }
                }
            }
        }
    });
    let cb: intent_git::repo_cache::CacheEnsureProgress = std::sync::Arc::new(move |ev| {
        let _ = tx.send(ev);
    });
    (cb, handle)
}

/// Bridge the direct-hydration submodule population to `reporter`: returns
/// the chunk callback to pass to
/// `intent_git::repo_cache::provision_direct_checkout_with_progress` plus the
/// pump task mapping the update's progress into the provisioning tail's
/// [`HYDRATE_SUBMODULE_LO`]`..=`[`HYDRATE_SUBMODULE_HI`] sub-segment.
pub(crate) fn submodule_hydration_reporter(
    reporter: std::sync::Arc<CreateProgress>,
) -> (
    intent_git::repo_cache::ProgressChunkFn,
    tokio::task::JoinHandle<()>,
) {
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();
    let handle = tokio::spawn(async move {
        let mut parser = crate::clone_ops::SubmoduleAwareParser::for_submodule_update();
        while let Some(text) = rx.recv().await {
            for (_phase, pct, msg) in parser.parse(&text) {
                reporter
                    .milestone(
                        "submodules",
                        map_segment(HYDRATE_SUBMODULE_LO, HYDRATE_SUBMODULE_HI, pct),
                        &msg,
                    )
                    .await;
            }
        }
    });
    let cb: intent_git::repo_cache::ProgressChunkFn = std::sync::Arc::new(move |chunk: &str| {
        let _ = tx.send(chunk.to_string());
    });
    (cb, handle)
}

struct ProgressState {
    last_percent: u32,
    last_frame: Option<(String, u32, String)>,
    done: bool,
}

/// Per-create progress reporter: publishes `git:clone:progress` /
/// `git:clone:done` frames carrying the caller's `progressId` under one
/// server-minted `requestId`, clamping percent monotonically non-decreasing
/// and guaranteeing at most one terminal done. Shared (via `Arc`) between the
/// create body and the clone's stderr reader task.
pub(crate) struct CreateProgress {
    bus: EventBus,
    workspace_id: WorkspaceId,
    request_id: String,
    progress_id: String,
    state: Mutex<ProgressState>,
}

impl CreateProgress {
    pub(crate) fn new(bus: EventBus, workspace_id: WorkspaceId, progress_id: String) -> Self {
        Self {
            bus,
            workspace_id,
            request_id: uuid::Uuid::new_v4().to_string(),
            progress_id,
            state: Mutex::new(ProgressState {
                last_percent: 0,
                last_frame: None,
                done: false,
            }),
        }
    }

    /// Emit one progress frame on the unified scale. The percent is clamped
    /// to never decrease below the last reported value (milestone callers can
    /// request their nominal position without tracking what ran before them);
    /// a frame identical to the previous one is skipped; frames after the
    /// terminal done are dropped.
    pub(crate) async fn milestone(&self, phase: &str, percent: u32, message: &str) {
        let percent = {
            let mut st = self.state.lock().unwrap();
            if st.done {
                return;
            }
            let pct = percent.min(100).max(st.last_percent);
            let key = (phase.to_string(), pct, message.to_string());
            if st.last_frame.as_ref() == Some(&key) {
                return;
            }
            st.last_percent = pct;
            st.last_frame = Some(key);
            pct
        };
        clone_ops::publish(
            &self.bus,
            &self.workspace_id,
            clone_ops::progress_event(
                &self.workspace_id,
                &self.request_id,
                phase,
                percent,
                message,
                Some(&self.progress_id),
            ),
        )
        .await;
    }

    /// Emit a clone stderr frame, mapped through the per-phase weights into
    /// the unified scale's clone segment (`0..=`[`CLONE_SEGMENT_END`]).
    /// The clone's own terminal `complete` frame is re-labeled `checkout`
    /// (message rephrased to match) — on the unified scale the clone
    /// finishing (85%) is not "complete"; only the create's own terminal
    /// frame carries `complete 100`.
    pub(crate) async fn clone_progress(&self, phase: &str, percent: u32, message: &str) {
        let (unified_phase, message) = if phase == "complete" {
            ("checkout", "Repository cloned")
        } else {
            (phase, message)
        };
        self.milestone(
            unified_phase,
            clone_overall_percent(phase, percent),
            message,
        )
        .await;
    }

    /// Mark the terminal state and report whether this call won the race —
    /// only the winner publishes a done frame (exactly-once).
    fn take_done(&self) -> bool {
        let mut st = self.state.lock().unwrap();
        if st.done {
            return false;
        }
        st.done = true;
        true
    }

    /// Terminal success: a final `complete 100` progress frame plus the one
    /// `git:clone:done { ok: true }`. Idempotent — later calls no-op.
    pub(crate) async fn done_ok(&self) {
        self.milestone("complete", 100, "Workspace ready").await;
        if !self.take_done() {
            return;
        }
        clone_ops::publish(
            &self.bus,
            &self.workspace_id,
            clone_ops::done_event(
                &self.workspace_id,
                &self.request_id,
                true,
                None,
                None,
                Some(&self.progress_id),
            ),
        )
        .await;
    }

    /// Terminal failure: the one `git:clone:done { ok: false, error }`.
    /// Idempotent — later calls no-op.
    pub(crate) async fn done_err(&self, error: &str, error_code: Option<CloneErrorCategory>) {
        if !self.take_done() {
            return;
        }
        clone_ops::publish(
            &self.bus,
            &self.workspace_id,
            clone_ops::done_event(
                &self.workspace_id,
                &self.request_id,
                false,
                Some(error),
                error_code,
                Some(&self.progress_id),
            ),
        )
        .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn map_segment_maps_endpoints_and_midpoint() {
        assert_eq!(map_segment(0, 100, 0), 0);
        assert_eq!(map_segment(0, 100, 100), 100);
        assert_eq!(map_segment(20, 80, 0), 20);
        assert_eq!(map_segment(20, 80, 50), 50);
        assert_eq!(map_segment(20, 80, 100), 80);
    }

    #[test]
    fn map_segment_clamps_out_of_range_input() {
        assert_eq!(map_segment(0, 85, 250), 85);
        // Collapsed / inverted segments pin to `lo`.
        assert_eq!(map_segment(90, 90, 50), 90);
        assert_eq!(map_segment(90, 10, 50), 90);
    }

    #[test]
    fn clone_overall_percent_is_monotonic_across_phases() {
        // A representative clone stderr sequence (submodules included): each
        // mapped value must be ≥ the previous one and the whole series stays
        // within 0..=85, with superproject phases below the submodule segment.
        let seq = [
            ("starting", 0),
            ("counting", 0),
            ("compressing", 40),
            ("compressing", 100),
            ("receiving", 0),
            ("receiving", 55),
            ("receiving", 100),
            ("resolving", 0),
            ("resolving", 100),
            ("checkout", 30),
            ("checkout", 100),
            ("submodules", 0),
            ("submodules", 50),
            ("submodules", 100),
            ("complete", 100),
        ];
        let mut last = 0;
        for (phase, pct) in seq {
            let overall = clone_overall_percent(phase, pct);
            assert!(
                overall >= last,
                "{phase} {pct}% mapped to {overall}, below previous {last}"
            );
            assert!(overall <= CLONE_SEGMENT_END);
            if phase != "submodules" && phase != "complete" {
                assert!(overall <= SUBMODULE_SEGMENT_START, "{phase} {pct}%");
            }
            last = overall;
        }
        assert_eq!(last, CLONE_SEGMENT_END, "a finished clone tops the segment");
    }

    #[test]
    fn clone_overall_percent_submodule_segment() {
        assert_eq!(
            clone_overall_percent("submodules", 0),
            SUBMODULE_SEGMENT_START
        );
        assert_eq!(clone_overall_percent("submodules", 100), CLONE_SEGMENT_END);
        let mid = clone_overall_percent("submodules", 50);
        assert!(mid > SUBMODULE_SEGMENT_START && mid < CLONE_SEGMENT_END);
    }

    #[test]
    fn clone_overall_percent_unknown_phase_passes_raw_percent() {
        assert_eq!(
            clone_overall_percent("mystery", 100),
            SUBMODULE_SEGMENT_START,
            "unknown phases span the superproject sub-segment"
        );
        assert_eq!(clone_overall_percent("mystery", 0), 0);
    }

    use intent_git::repo_cache::CacheEnsureEvent as Ev;

    #[test]
    fn cache_pump_chunks_before_any_step_keep_clone_mapping() {
        // A cache-miss full clone streams CloneChunks with no prior Step:
        // frames must pass through as raw Clone frames (the parser's own
        // phase + percent, mapped later by `clone_progress` exactly as today).
        let mut pump = CacheEnsurePump::new();
        let frames = pump.handle(Ev::CloneChunk(
            "Receiving objects:  42% (42/100)\r".to_string(),
        ));
        assert_eq!(frames.len(), 1);
        match &frames[0] {
            CacheFrame::Clone(phase, pct, msg) => {
                assert_eq!(*phase, "receiving");
                assert_eq!(*pct, 42);
                assert_eq!(msg, "Receiving objects: 42%");
            }
            CacheFrame::Milestone(..) => panic!("cache-miss chunk must stay a Clone frame"),
        }
    }

    #[test]
    fn cache_pump_refresh_fetch_chunks_stream_into_band() {
        // Chunks after Step("fetch") are the refresh fetch's stderr: they
        // must become cache-phased Milestone frames strictly increasing
        // within the fetch→reset band (5..=60).
        let mut pump = CacheEnsurePump::new();
        let fetch = pump.handle(Ev::Step("fetch"));
        assert!(matches!(
            fetch.as_slice(),
            [CacheFrame::Milestone("cache", CACHE_REFRESH_FETCH_LO, _)]
        ));
        let chunks = [
            "remote: Counting objects: 10, done.\n",
            "remote: Compressing objects:  60% (6/10)\r",
            "Receiving objects:  10% (10/100)\r",
            "Receiving objects:  55% (55/100)\r",
            "Receiving objects: 100% (100/100), done.\n",
            "Resolving deltas: 100% (40/40), done.\n",
        ];
        let mut last = CACHE_REFRESH_FETCH_LO;
        let mut advanced = false;
        for chunk in chunks {
            for frame in pump.handle(Ev::CloneChunk(chunk.to_string())) {
                match frame {
                    CacheFrame::Milestone(phase, pct, _) => {
                        assert_eq!(phase, "cache");
                        assert!((CACHE_REFRESH_FETCH_LO..=CACHE_REFRESH_FETCH_HI).contains(&pct));
                        assert!(pct >= last, "band percent regressed: {pct} < {last}");
                        if pct > last {
                            advanced = true;
                        }
                        last = pct;
                    }
                    CacheFrame::Clone(phase, pct, _) => {
                        panic!("refresh-fetch chunk leaked a Clone frame: {phase} {pct}%")
                    }
                }
            }
        }
        assert!(advanced, "the fetch stream must advance past the 5% floor");
    }

    #[test]
    fn cache_pump_reset_milestone_still_lands_at_60() {
        let mut pump = CacheEnsurePump::new();
        pump.handle(Ev::Step("fetch"));
        pump.handle(Ev::CloneChunk(
            "Receiving objects: 100% (100/100), done.\n".to_string(),
        ));
        let frames = pump.handle(Ev::Step("reset"));
        match frames.as_slice() {
            [CacheFrame::Milestone(phase, pct, msg)] => {
                assert_eq!(*phase, "cache");
                assert_eq!(*pct, 60);
                assert_eq!(msg, "Updating cache branch...");
            }
            _ => panic!("Step(reset) must emit exactly one milestone frame"),
        }
    }

    #[test]
    fn cache_pump_reclone_resets_to_full_clone_mapping() {
        // Step("fetch") → Step("re-clone"): the escalation must emit the
        // rebuild milestone and revert subsequent CloneChunks to the raw
        // full-clone mapping (fresh parser), not the refresh-fetch band.
        let mut pump = CacheEnsurePump::new();
        pump.handle(Ev::Step("fetch"));
        pump.handle(Ev::CloneChunk(
            "Receiving objects:  80% (80/100)\r".to_string(),
        ));
        let frames = pump.handle(Ev::Step("re-clone"));
        match frames.as_slice() {
            [CacheFrame::Milestone(phase, pct, msg)] => {
                assert_eq!(*phase, "cache");
                assert_eq!(*pct, CACHE_REFRESH_FETCH_LO);
                assert_eq!(msg, "Rebuilding repository cache...");
            }
            _ => panic!("Step(re-clone) must emit exactly one milestone frame"),
        }
        let frames = pump.handle(Ev::CloneChunk(
            "Receiving objects:  42% (42/100)\r".to_string(),
        ));
        assert_eq!(frames.len(), 1);
        match &frames[0] {
            CacheFrame::Clone(phase, pct, msg) => {
                assert_eq!(*phase, "receiving");
                assert_eq!(*pct, 42);
                assert_eq!(msg, "Receiving objects: 42%");
            }
            CacheFrame::Milestone(..) => {
                panic!("post-re-clone chunks must map as raw Clone frames")
            }
        }
    }

    #[test]
    fn refresh_fetch_percent_stays_within_band() {
        assert_eq!(refresh_fetch_percent("starting", 0), CACHE_REFRESH_FETCH_LO);
        assert_eq!(
            refresh_fetch_percent("complete", 100),
            CACHE_REFRESH_FETCH_HI
        );
        let mid = refresh_fetch_percent("receiving", 50);
        assert!(mid > CACHE_REFRESH_FETCH_LO && mid < CACHE_REFRESH_FETCH_HI);
    }
}
