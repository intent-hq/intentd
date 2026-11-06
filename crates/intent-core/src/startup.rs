//! In-memory startup observations. No database, event bus, or caller data.
mod estimate;

use serde::Serialize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const MAX_SAFE: u64 = 9_007_199_254_740_991;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum StartupPhase {
    Preparing,
    Indexing,
    Finalizing,
    Ready,
    Failed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum FinalizingStep {
    Anchors,
    Committing,
    Services,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StartupProgress {
    pub completed_notes: u64,
    pub total_notes: u64,
    pub processed_source_bytes: u64,
    pub total_source_bytes: u64,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StartupSnapshot {
    pub startup_status: u8,
    pub daemon_version: String,
    pub attempt_id: String,
    pub phase: StartupPhase,
    pub elapsed_ms: u64,
    pub phase_elapsed_ms: u64,
    pub progress_age_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_after_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress: Option<StartupProgress>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub estimated_remaining_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finalizing_step: Option<FinalizingStep>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure: Option<StartupFailure>,
}

#[derive(Clone, Debug, Serialize)]
pub struct StartupFailure {
    pub code: &'static str,
    pub message: &'static str,
}

struct State {
    daemon_version: String,
    attempt_id: String,
    started: Instant,
    phase_started: Instant,
    changed: Instant,
    published: Instant,
    terminal: Option<Instant>,
    phase: StartupPhase,
    step: Option<FinalizingStep>,
    progress: Option<StartupProgress>,
    pending: Option<StartupProgress>,
    estimator: estimate::Estimator,
}

/// Shared bounded snapshot. Writers hold the lock only for scalar assignments.
/// Polling never advances work age, and observers never participate in a transaction.
#[derive(Clone)]
pub struct StartupStatus(Arc<Mutex<State>>, Arc<tokio::sync::Notify>);

impl Default for StartupStatus {
    fn default() -> Self {
        Self::new()
    }
}

impl StartupStatus {
    #[must_use]
    pub fn new() -> Self {
        Self::with_daemon_version(env!("CARGO_PKG_VERSION"))
    }

    /// The composition root supplies the responding daemon's build version;
    /// callers must never substitute the version of a probing CLI binary.
    #[must_use]
    pub fn with_daemon_version(version: impl Into<String>) -> Self {
        let now = Instant::now();
        Self(
            Arc::new(Mutex::new(State {
                daemon_version: version.into(),
                attempt_id: uuid::Uuid::new_v4().to_string(),
                started: now,
                phase_started: now,
                changed: now,
                published: now,
                terminal: None,
                phase: StartupPhase::Preparing,
                step: None,
                progress: None,
                pending: None,
                estimator: estimate::Estimator::default(),
            })),
            Arc::new(tokio::sync::Notify::new()),
        )
    }

    #[must_use]
    pub fn phase(&self) -> StartupPhase {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .phase
    }

    #[must_use]
    pub fn snapshot(&self) -> StartupSnapshot {
        self.snapshot_at(Instant::now())
    }

    fn snapshot_at(&self, now: Instant) -> StartupSnapshot {
        let s = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let end = s.terminal.unwrap_or(now);
        StartupSnapshot {
            startup_status: 1,
            daemon_version: s.daemon_version.clone(),
            attempt_id: s.attempt_id.clone(),
            phase: s.phase,
            elapsed_ms: millis(end.saturating_duration_since(s.started)),
            phase_elapsed_ms: if s.terminal.is_some() {
                0
            } else {
                millis(end.saturating_duration_since(s.phase_started))
            },
            progress_age_ms: if s.terminal.is_some() {
                0
            } else {
                millis(end.saturating_duration_since(s.changed))
            },
            retry_after_ms: s.terminal.is_none().then_some(1000),
            progress: s.progress,
            estimated_remaining_ms: (s.phase == StartupPhase::Indexing)
                .then(|| s.estimator.estimate(end))
                .flatten(),
            finalizing_step: s.step,
            failure: (s.phase == StartupPhase::Failed).then_some(StartupFailure {
                code: "startup-failed",
                message: "Daemon startup failed. Check daemon logs.",
            }),
        }
    }

    /// Publish a fixed inventory, then completed whole-note counts. Intermediate
    /// updates are limited to four per second; the final counters publish exactly.
    pub fn indexing(&self, progress: StartupProgress) {
        self.indexing_at(progress, Instant::now());
    }

    fn indexing_at(&self, p: StartupProgress, now: Instant) {
        let mut s = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if s.terminal.is_some()
            || !matches!(s.phase, StartupPhase::Preparing | StartupPhase::Indexing)
            || p.completed_notes > p.total_notes
            || p.processed_source_bytes > p.total_source_bytes
            || p.total_notes > MAX_SAFE
            || p.total_source_bytes > MAX_SAFE
        {
            return;
        }
        if let Some(old) = s.pending {
            if old.total_notes != p.total_notes
                || old.total_source_bytes != p.total_source_bytes
                || p.completed_notes < old.completed_notes
                || p.processed_source_bytes < old.processed_source_bytes
            {
                return;
            }
            if old == p {
                return;
            }
        }
        s.pending = Some(p);
        s.changed = now;
        if s.phase != StartupPhase::Indexing
            || p.completed_notes == p.total_notes
            || now.saturating_duration_since(s.published) >= Duration::from_millis(250)
        {
            if s.phase != StartupPhase::Indexing {
                s.phase = StartupPhase::Indexing;
                s.phase_started = now;
            }
            s.estimator.record(now, p);
            s.progress = Some(p);
            s.published = now;
        }
    }

    pub fn finalizing(&self, step: FinalizingStep) {
        self.transition(StartupPhase::Finalizing, Some(step));
    }

    #[must_use]
    pub fn ready(&self) -> bool {
        self.transition(StartupPhase::Ready, None);
        self.phase() == StartupPhase::Ready
    }
    pub fn fail(&self) {
        self.transition(StartupPhase::Failed, None);
        self.1.notify_waiters();
    }

    /// Cancellation/failure signal for startup work, independent of observers.
    pub async fn cancelled(&self) {
        loop {
            let notified = self.1.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.snapshot().phase == StartupPhase::Failed {
                return;
            }
            notified.await;
        }
    }

    fn transition(&self, phase: StartupPhase, step: Option<FinalizingStep>) {
        let now = Instant::now();
        let mut s = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if s.terminal.is_some() || (s.phase == phase && s.step == step) {
            return;
        }
        if s.phase != phase {
            s.phase_started = now;
        }
        s.phase = phase;
        s.step = step;
        s.changed = now;
        s.published = now;
        s.progress = s.pending;
        if matches!(phase, StartupPhase::Ready | StartupPhase::Failed) {
            s.terminal = Some(now);
        }
    }
}

fn millis(d: Duration) -> u64 {
    u64::try_from(d.as_millis())
        .unwrap_or(MAX_SAFE)
        .min(MAX_SAFE)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn work_age_throttling_and_terminal_snapshots() {
        let status = StartupStatus::new();
        let start = status.0.lock().unwrap().started;
        let inventory = StartupProgress {
            total_notes: 3,
            total_source_bytes: 12,
            ..Default::default()
        };
        status.indexing_at(inventory, start);
        let one = StartupProgress {
            completed_notes: 1,
            processed_source_bytes: 4,
            ..inventory
        };
        status.indexing_at(one, start + Duration::from_millis(100));
        let s = status.snapshot_at(start + Duration::from_millis(200));
        assert_eq!(s.progress, Some(inventory));
        assert_eq!(s.progress_age_ms, 100);
        let two = StartupProgress {
            completed_notes: 2,
            processed_source_bytes: 8,
            ..inventory
        };
        status.indexing_at(two, start + Duration::from_millis(250));
        assert_eq!(
            status
                .snapshot_at(start + Duration::from_secs(40))
                .progress_age_ms,
            39750
        );
        assert_eq!(status.snapshot().progress, Some(two));
        status.indexing_at(inventory, start + Duration::from_secs(1)); // reject regression
        assert_eq!(status.snapshot().progress, Some(two));
        let done = StartupProgress {
            completed_notes: 3,
            processed_source_bytes: 12,
            ..inventory
        };
        status.indexing_at(done, start + Duration::from_millis(260));
        assert_eq!(status.snapshot().progress, Some(done));
        status.finalizing(FinalizingStep::Committing);
        assert_eq!(status.snapshot().phase, StartupPhase::Finalizing);
        status.fail();
        assert!(!status.ready());
        let failed = status.snapshot();
        assert_eq!(failed.phase, StartupPhase::Failed);
        assert_eq!(failed.progress_age_ms, 0);
        assert_eq!(failed.phase_elapsed_ms, 0);
        assert!(failed.retry_after_ms.is_none());
        assert!(failed.failure.is_some());
        assert_eq!(
            failed.elapsed_ms,
            status
                .snapshot_at(start + Duration::from_secs(900))
                .elapsed_ms
        );
        let json = serde_json::to_value(failed).unwrap();
        assert!(json.get("estimatedRemainingMs").is_none());
        assert_ne!(
            status.snapshot().attempt_id,
            StartupStatus::new().snapshot().attempt_id
        );
    }
}
