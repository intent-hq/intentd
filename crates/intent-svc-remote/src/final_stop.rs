//! Final stops: an agent that ends for good (deleted or retired, its
//! workspace retired or deleted, its machine removed) is stopped on its
//! machine, which pushes its work one last time and removes its checkout.
//!
//! The stop is recorded durably before the session or workspace is torn
//! down, so a backend restart before the machine acknowledges it still
//! resends it at the next bind. While the record exists the machine's final
//! push is accepted into the workspace's repository, even once the session
//! (and its placement record) or the worktree is gone. The record is dropped
//! on the acknowledgement or after [`FINAL_STOP_BOUND`].
//!
//! [`FINAL_STOP_BOUND`]: intent_core::remote_launch::FINAL_STOP_BOUND

use std::time::Duration;

use intent_core::node_link::StopId;
use intent_core::remote_launch::{FinalStopReason, RemoteAgentRecord, RemoteFinalStopRecord};
use intent_core::{AgentSession, AgentStatus, Result, Workspace};
use intent_store::Store;

/// Whether `session` has ended for good or for now (deleted, completed or
/// retired): its agent must not run on a machine.
pub(crate) fn session_ended(session: &AgentSession) -> bool {
    session.retired_at.is_some()
        || matches!(
            session.status,
            AgentStatus::Deleted | AgentStatus::Completed
        )
}

/// The wall clock final-stop bounds are measured against; tests move it.
#[derive(Debug, Clone, Default)]
pub(crate) struct Clock {
    /// Test seam: seconds added to the real time.
    #[cfg(test)]
    pub offset_secs: std::sync::Arc<std::sync::atomic::AtomicI64>,
}

impl Clock {
    #[cfg_attr(
        not(test),
        expect(clippy::unused_self, reason = "only tests move the clock")
    )]
    pub(crate) fn now(&self) -> time::OffsetDateTime {
        let now = time::OffsetDateTime::now_utc();
        #[cfg(test)]
        let now = now
            + time::Duration::seconds(self.offset_secs.load(std::sync::atomic::Ordering::SeqCst));
        now
    }
}

/// Whether `record` is older than `bound` at `now`. An unreadable timestamp
/// counts as expired, so a bad record is never kept forever.
pub(crate) fn expired(
    record: &RemoteFinalStopRecord,
    bound: Duration,
    now: time::OffsetDateTime,
) -> bool {
    use time::format_description::well_known::Rfc3339;
    let Ok(requested) = time::OffsetDateTime::parse(&record.requested_at, &Rfc3339) else {
        return true;
    };
    let age = now - requested;
    age >= time::Duration::try_from(bound).unwrap_or(time::Duration::MAX)
}

/// Where a final push for `workspace`'s agents lands: the workspace's
/// repository, whose refs its worktrees share, so the push still lands once
/// the worktree is removed. Empty when the workspace has neither.
pub(crate) fn repo_path(workspace: &Workspace) -> String {
    workspace
        .repository_path
        .as_ref()
        .or(workspace.worktree_path.as_ref())
        .filter(|path| !path.is_empty())
        .cloned()
        .unwrap_or_default()
}

/// Record a final stop for `placed` with `reason` (keeping one already
/// recorded) and return its stop id.
///
/// # Errors
///
/// When the record cannot be written.
pub(crate) async fn record(
    store: &Store,
    placed: &RemoteAgentRecord,
    reason: FinalStopReason,
    repo_path: String,
) -> Result<StopId> {
    let recorded = store
        .record_final_stop(&RemoteFinalStopRecord {
            agent_id: placed.agent_id.clone(),
            workspace_id: placed.workspace_id.clone(),
            machine_id: placed.node_id.clone(),
            lease_id: placed.lease_id.clone(),
            stop_id: StopId::new_v4().to_string(),
            reason,
            repo_path,
            requested_at: intent_core::now_iso(),
        })
        .await?;
    recorded.stop_id.parse().map_err(|_| {
        intent_core::Error::Internal(format!(
            "final stop of {} has a malformed id",
            recorded.agent_id
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record_at(requested_at: &str) -> RemoteFinalStopRecord {
        RemoteFinalStopRecord {
            agent_id: "agent".into(),
            workspace_id: "workspace".into(),
            machine_id: "machine".into(),
            lease_id: "lease".into(),
            stop_id: StopId::new_v4().to_string(),
            reason: FinalStopReason::AgentDeleted,
            repo_path: "/repo".into(),
            requested_at: requested_at.into(),
        }
    }

    #[test]
    fn a_final_stop_expires_after_its_bound() {
        let bound = Duration::from_secs(7 * 24 * 60 * 60);
        let now = time::OffsetDateTime::now_utc();
        assert!(!expired(&record_at(&intent_core::now_iso()), bound, now));
        assert!(expired(&record_at("2020-01-01T00:00:00Z"), bound, now));
        assert!(expired(&record_at("not a time"), bound, now));
        let clock = Clock::default();
        clock
            .offset_secs
            .store(7 * 24 * 60 * 60, std::sync::atomic::Ordering::SeqCst);
        assert!(expired(
            &record_at(&intent_core::now_iso()),
            bound,
            clock.now()
        ));
    }
}
