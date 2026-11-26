//! One PR monitor's poll, single-flight across the tenant's processes.
//!
//! Each process sweeps its monitors under its own in-process lane (which
//! orders this process's memory: the observation feed, the PR cache, the
//! delivery records). Across processes, a monitor is polled only under its
//! lease (`"pr-monitor-poll"`, keyed by monitor id), so two processes never
//! poll one monitor at once, and the poll's writes guard the lease
//! ([`LeaseKeeper::guard_writes`]): a process that lost it writes nothing.
//!
//! The sweep chose the monitor from a row it read earlier. Once the lease
//! is held the row is read again: if it changed, another process polled or
//! changed the monitor in between, and this sweep leaves it to the next
//! tick instead of polling it again from a stale image.

use intent_core::PrMonitor;
use intent_store::LeaseKeeper;

use super::Services;
use crate::work_leases;

impl Services {
    /// Takes `monitor`'s poll lease: `None` while another process holds it,
    /// when the stored row no longer equals `monitor`, or when the store
    /// cannot answer (the monitor is polled on a later tick).
    pub(super) async fn take_monitor_poll_lease(&self, monitor: &PrMonitor) -> Option<LeaseKeeper> {
        let id = monitor.monitor_id.0.as_str();
        let lease = match self.acquire_monitor_poll_lease(monitor).await {
            Ok(Some(lease)) => lease,
            Ok(None) => {
                tracing::debug!(
                    monitor = id,
                    "PR monitor poll: another process is polling it; skipped"
                );
                return None;
            }
            Err(error) => {
                tracing::warn!(monitor = id, %error, "PR monitor poll: could not take its poll lease; skipped");
                return None;
            }
        };
        match self
            .store
            .get_qualified_pr_monitor(&monitor.monitor_id)
            .await
        {
            Ok(current) if current.monitor() == monitor => Some(lease),
            Ok(_) => {
                tracing::debug!(
                    monitor = id,
                    "PR monitor poll: the row changed since the sweep read it; skipped"
                );
                lease.release(&self.store).await;
                None
            }
            Err(error) => {
                tracing::debug!(monitor = id, %error, "PR monitor poll: re-read failed; skipped");
                lease.release(&self.store).await;
                None
            }
        }
    }

    /// `monitor`'s poll lease, without re-reading the row: `None` while
    /// another process holds it.
    pub(super) async fn acquire_monitor_poll_lease(
        &self,
        monitor: &PrMonitor,
    ) -> intent_core::Result<Option<LeaseKeeper>> {
        LeaseKeeper::acquire(
            &self.store,
            work_leases::PR_MONITOR_POLL_SCOPE,
            &monitor.monitor_id.0,
            work_leases::WORK_LEASE_TTL,
        )
        .await
    }
}
