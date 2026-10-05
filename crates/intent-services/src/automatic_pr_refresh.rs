//! Admission shared by automatic linkage commands and the background sweep.
//! Reserve before I/O, including failures, so reconnects and concurrent windows
//! cannot turn a failed refresh into an unbounded retry loop.
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use intent_core::{Result, WorkspaceId};
use tokio::time::Instant;

use crate::{pr_ops, Services};

type Key = (WorkspaceId, Option<String>);

struct Attempt {
    started: Instant,
    running: bool,
}

#[derive(Default)]
pub(crate) struct Admission {
    attempts: HashMap<Key, Attempt>,
}

pub(crate) struct Permit {
    admission: Arc<Mutex<Admission>>,
    key: Key,
}

impl Drop for Permit {
    fn drop(&mut self) {
        if let Some(attempt) = self.admission.lock().unwrap().attempts.get_mut(&self.key) {
            attempt.running = false;
        }
    }
}

impl Admission {
    fn reserve(&mut self, key: &Key, interval: Duration, now: Instant) -> bool {
        if self.attempts.get(key).is_some_and(|attempt| {
            attempt.running || now.saturating_duration_since(attempt.started) < interval
        }) {
            return false;
        }
        self.attempts.insert(
            key.clone(),
            Attempt {
                started: now,
                running: true,
            },
        );
        true
    }

    pub(crate) fn retain_workspaces(&mut self, ids: &[WorkspaceId]) {
        let ids: HashSet<_> = ids.iter().collect();
        self.attempts
            .retain(|(id, _), attempt| attempt.running || ids.contains(id));
    }
}

impl Services {
    pub(crate) fn admit_automatic_pr_refresh(
        &self,
        workspace_id: &WorkspaceId,
        root_id: Option<&str>,
        interval_secs: u64,
    ) -> Option<Permit> {
        let key = (workspace_id.clone(), root_id.map(str::to_owned));
        if !self.automatic_pr_refresh.lock().unwrap().reserve(
            &key,
            Duration::from_secs(interval_secs),
            Instant::now(),
        ) {
            return None;
        }
        Some(Permit {
            admission: self.automatic_pr_refresh.clone(),
            key,
        })
    }

    #[cfg(test)]
    pub(crate) fn age_automatic_pr_refresh(&self, workspace_id: &WorkspaceId, seconds: u64) {
        for ((id, _), attempt) in &mut self.automatic_pr_refresh.lock().unwrap().attempts {
            if id == workspace_id {
                attempt.started -= Duration::from_secs(seconds);
            }
        }
    }

    pub(crate) async fn refresh_workspace_pr_automatically(
        &self,
        workspace_id: &WorkspaceId,
    ) -> Result<pr_ops::PrRefreshOutcome> {
        let now = time::OffsetDateTime::now_utc();
        let intervals = self
            .workspace_automatic_check_intervals(std::slice::from_ref(workspace_id), now)
            .await?;
        if self.sweeps_rate_limited() {
            return Ok(pr_ops::PrRefreshOutcome::Skipped);
        }
        let interval_secs = intervals.get(workspace_id).copied().unwrap_or(60);
        let Some(permit) = self.admit_automatic_pr_refresh(workspace_id, None, interval_secs)
        else {
            return Ok(pr_ops::PrRefreshOutcome::Skipped);
        };
        let services = self.clone();
        let workspace_id = workspace_id.clone();
        let caller = intent_core::current_caller().unwrap_or(intent_core::Caller::Daemon);
        let wire = intent_core::caller::current_wire_credential();
        let owner = self
            .store_tasks
            .spawn_draining(intent_sourcecontrol::traffic::inherit_context(
                intent_core::with_caller(
                    caller,
                    intent_core::caller::with_wire_credential(wire, async move {
                        let _permit = permit;
                        // Deliberately not `explicitly_refresh`: automatic retries must
                        // also respect provider-scoped failure caching and budgets.
                        let outcome = crate::pr_discovery::automatically_refresh(
                            interval_secs,
                            services.refresh_workspace_pr_cached(&workspace_id),
                        )
                        .await;
                        if let Err(intent_core::Error::RateLimited(detail)) = &outcome {
                            if let Ok(sc) =
                                pr_ops::resolve_source_control(services.source_control.clone())
                                    .await
                            {
                                services.pause_sweeps_for_rate_limit(&sc, detail).await;
                            }
                        }
                        outcome
                    }),
                ),
            ))
            .ok_or_else(|| {
                intent_core::Error::Internal("PR refresh writers are shutting down".into())
            })?;
        owner
            .await
            .map_err(|e| intent_core::Error::Internal(format!("PR refresh worker failed: {e}")))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn admission_reserves_failures_and_running_work_at_every_interval() {
        for secs in [60, 120, 300, 600, 900] {
            let now = Instant::now();
            let interval = Duration::from_secs(secs);
            let key = (WorkspaceId::new(), None);
            let mut admission = Admission::default();
            assert!(admission.reserve(&key, interval, now));
            assert!(!admission.reserve(&key, interval, now + interval * 2));
            admission.attempts.get_mut(&key).unwrap().running = false;
            assert!(!admission.reserve(&key, interval, now + interval - Duration::from_nanos(1)));
            assert!(admission.reserve(&key, interval, now + interval));
        }
    }

    #[test]
    fn resumed_activity_and_independent_workspace_and_root_admission() {
        let now = Instant::now();
        let mut admission = Admission::default();
        let key = (WorkspaceId::new(), None);
        assert!(admission.reserve(&key, Duration::from_secs(900), now));
        admission.attempts.get_mut(&key).unwrap().running = false;
        assert!(!admission.reserve(
            &key,
            Duration::from_secs(900),
            now + Duration::from_secs(60)
        ));
        assert!(admission.reserve(&key, Duration::from_secs(60), now + Duration::from_secs(60)));
        assert!(admission.reserve(&(WorkspaceId::new(), None), Duration::from_secs(900), now));
        assert!(admission.reserve(&(key.0, Some("root".into())), Duration::from_secs(900), now));
    }
}
