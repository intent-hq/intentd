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
fn clone_phase_segment(phase: &str) -> (u32, u32) {
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
/// `0..=`[`CLONE_SEGMENT_END`] clone segment.
pub(crate) fn clone_overall_percent(phase: &str, percent: u32) -> u32 {
    let (lo, hi) = clone_phase_segment(phase);
    map_segment(0, CLONE_SEGMENT_END, map_segment(lo, hi, percent))
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
    /// The clone's own terminal `complete` frame is re-labeled `checkout` —
    /// on the unified scale the clone finishing (85%) is not "complete";
    /// only the create's own terminal frame carries `complete 100`.
    pub(crate) async fn clone_progress(&self, phase: &str, percent: u32, message: &str) {
        let unified_phase = if phase == "complete" {
            "checkout"
        } else {
            phase
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
        // A representative clone stderr sequence: each mapped value must be
        // ≥ the previous one and the whole series stays within 0..=85.
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
            last = overall;
        }
        assert_eq!(last, CLONE_SEGMENT_END, "a finished clone tops the segment");
    }

    #[test]
    fn clone_overall_percent_unknown_phase_passes_raw_percent() {
        assert_eq!(
            clone_overall_percent("mystery", 100),
            CLONE_SEGMENT_END,
            "unknown phases span the full clone segment"
        );
        assert_eq!(clone_overall_percent("mystery", 0), 0);
    }
}
