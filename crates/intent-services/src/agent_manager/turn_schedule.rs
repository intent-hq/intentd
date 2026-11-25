//! Scripted scheduler for turn-lifecycle tests. Production code names the
//! points where one action can be overtaken by another ([`TurnStep`]) and
//! the store writes a test can fail ([`TurnFault`]). A test parks an action
//! at a step, runs a second action to its end, then resumes the first, so an
//! interleaving is reproduced without depending on task scheduling.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use super::TurnStartPause;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum TurnStep {
    /// A claim holds the slot and the turn-start gate; `Active` is not
    /// written yet.
    TurnStarting,
    /// A delivery is about to persist its user row.
    UserPersist,
    /// A message worker left its loop and still holds its admission.
    WorkerFinish,
    /// A failed harness wake is about to dispose of its child.
    FailedWakeDisposal,
    /// Shutdown holds the gate before persisting the interrupted turn.
    ShutdownPersist,
    /// `agent_retry` passed its unlocked `Error` check, before the gate.
    RetryChecked,
    /// `agent_retry` lifted the failed park and freed the slot, before its
    /// drain.
    RetryLifted,
    /// `redrive_failed_turn` is about to take the turn-start gate.
    RedriveChecked,
    /// `interrupt_inner` read the session after choosing its turn, before
    /// it re-checks the slot.
    InterruptObserved,
    /// `interrupt_inner` cancelled the turn and is about to free its slot.
    InterruptReleasing,
    /// A hard stop aborted the worker and is about to free its slot.
    DetachReleasing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum TurnFault {
    /// The next turn start's `Active` write fails.
    ActiveWrite,
    /// The next failed-turn redrive's status read fails.
    RedriveStore,
}

#[derive(Default)]
pub(crate) struct TurnSchedule {
    pauses: Mutex<HashMap<TurnStep, Arc<TurnStartPause>>>,
    faults: Mutex<HashSet<TurnFault>>,
}

impl TurnSchedule {
    /// Park the next action that reaches `step` on `pause`.
    pub(crate) fn arm(&self, step: TurnStep, pause: Arc<TurnStartPause>) {
        self.pauses.lock().unwrap().insert(step, pause);
    }

    /// Drop an armed pause the action finished without reaching.
    pub(crate) fn disarm(&self, step: TurnStep) {
        self.pauses.lock().unwrap().remove(&step);
    }

    /// [`Self::arm`] with a fresh pause.
    pub(crate) fn pause(&self, step: TurnStep) -> Arc<TurnStartPause> {
        let pause = Arc::new(TurnStartPause::default());
        self.arm(step, pause.clone());
        pause
    }

    /// Production hook: park here once if a test armed `step`.
    pub(crate) async fn reach(&self, step: TurnStep) {
        let pause = self.pauses.lock().unwrap().remove(&step);
        if let Some(pause) = pause {
            pause.reached.notify_one();
            pause.resume.notified().await;
        }
    }

    pub(crate) fn inject(&self, fault: TurnFault) {
        self.faults.lock().unwrap().insert(fault);
    }

    pub(crate) fn clear_faults(&self) {
        self.faults.lock().unwrap().clear();
    }

    /// Production hook: whether to fail this write (one-shot).
    pub(crate) fn take_fault(&self, fault: TurnFault) -> bool {
        self.faults.lock().unwrap().remove(&fault)
    }
}
