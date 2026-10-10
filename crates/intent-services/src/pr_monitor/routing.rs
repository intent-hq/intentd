//! Direct qualified monitor actions. Polling/restoration do not use these
//! admission handles yet; production recipient activation remains disabled.

use super::{
    json, now_iso, pr_monitor_wire, pr_ops, qualified_cache, AgentId, Arc, Error, PrMonitor,
    PrMonitorHolder, PrMonitorId, PrMonitorState, Result, Services, Value, WorkspaceId,
};
use crate::repository_admission::read_request::RepositoryReadRequest;
use crate::repository_admission::AdmissionResult;
use crate::repository_read_source::{
    self as source, CapturedReview, MonitorAuthority, ReadOutcome, ResolvedReview,
};
use intent_core::ReviewTarget;
use intent_store::{MonitorTargetProvenance, PersistedMonitorTarget};

/// Proven producer classification only. Qualified output needs its own
/// original recipient/authority route; stripping its fields is never legacy.
pub(super) fn legacy_output(record: &intent_store::QualifiedPrMonitor) -> bool {
    matches!(
        record.target(),
        PersistedMonitorTarget::Resolved {
            provenance: MonitorTargetProvenance::LegacyGithubWriter,
            ..
        }
    )
}

struct QualifiedBaseline {
    snapshot: Option<String>,
    polled_at: Option<String>,
    last_error: Option<String>,
    now: String,
}

pub(crate) fn wire(m: &PrMonitor, target: &ReviewTarget) -> Result<Value> {
    let mut value = pr_monitor_wire(m, None);
    value["repo"] = json!(target.repository.project_path);
    value["resource"] = json!(target);
    value["monitorAvailability"] = json!({"state":"available"});
    let envelope: Value =
        serde_json::from_str(m.last_snapshot.as_deref().ok_or_else(source::refused)?)
            .map_err(|_| source::refused())?;
    value["lastSnapshot"] = Value::Null;
    if let Some(snapshot) = envelope
        .get("qualifiedSnapshot")
        .filter(|value| !value.is_null())
    {
        let mut summary = snapshot["requirements"].clone();
        // Row consumers use counts; the full snapshot retains named check lists.
        // Missing evidence stays null rather than becoming a known zero.
        for field in ["failingRequired", "pendingRequired"] {
            summary["checks"][field] = summary["checks"][field]
                .as_array()
                .map_or(Value::Null, |items| json!(items.len()));
        }
        if let Some(checks) = summary["checks"].as_object_mut() {
            checks.remove("items");
        }
        summary["availability"] = snapshot["availability"].clone();
        value["lastSnapshot"] = summary;
        for field in ["title", "url"] {
            if let Some(text) = snapshot.get(field).and_then(Value::as_str) {
                value[field] = json!(text);
            }
        }
    }
    if m.state == PrMonitorState::Active {
        if let Some(until) = envelope
            .get("retryAt")
            .and_then(Value::as_str)
            .filter(|until| {
                chrono::DateTime::parse_from_rfc3339(until).is_ok_and(|at| at > chrono::Utc::now())
            })
        {
            value["monitorAvailability"] = json!({"state":"backoff","retryAt":until});
            value["pausedUntil"] = json!(until);
        } else if value["lastSnapshot"].is_null() {
            value["monitorAvailability"] = json!({"state":"pending"});
            value
                .as_object_mut()
                .expect("monitor object")
                .remove("lastError");
        }
    }
    Ok(value)
}

pub(super) fn same_connection(m: &PrMonitor, authority: &MonitorAuthority) -> Result<()> {
    let envelope: Value =
        serde_json::from_str(m.last_snapshot.as_deref().ok_or_else(source::refused)?)
            .map_err(|_| source::refused())?;
    let evidence: intent_core::review_monitor::ReviewMonitorBinding =
        serde_json::from_value(envelope["monitorBinding"].clone())
            .map_err(|_| source::refused())?;
    // This comparison constrains independently admitted authority. The stored
    // evidence never supplies authority or reconstructs a runtime handle.
    if evidence.resource != authority.binding.resource
        || evidence.connection != authority.binding.connection
        || evidence.scope.daemon_id != authority.binding.scope.daemon_id
    {
        return Err(source::refused());
    }
    Ok(())
}

impl Services {
    #[cfg(test)]
    fn monitor_write_ready(&self) {
        if let Some(ready) = self
            .test_review_monitor_write_ready
            .lock()
            .unwrap()
            .as_ref()
        {
            ready.notify_one();
        }
    }

    /// Native recipients have no qualified receipt in this wave. Refuse before
    /// cancellation/flush can disclose an owner, emit a wake or read a provider.
    pub(crate) async fn require_legacy_monitor_control(&self, id: &PrMonitorId) -> Result<()> {
        let row = self.store.get_qualified_pr_monitor(id).await?;
        if matches!(
            row.target(),
            PersistedMonitorTarget::Resolved {
                provenance: MonitorTargetProvenance::LegacyGithubWriter,
                ..
            }
        ) {
            Ok(())
        } else {
            Err(source::unsupported())
        }
    }

    pub(crate) async fn review_monitor_start_op(
        &self,
        workspace: &WorkspaceId,
        agent: &AgentId,
        number: u64,
        captured: CapturedReview,
        retained: AdmissionResult<Arc<RepositoryReadRequest>>,
    ) -> Result<Value> {
        let prepared = match captured
            .resolve(self)
            .await
            .map_err(|_| source::refused())?
        {
            ResolvedReview::Unsupported => return Err(source::unsupported()),
            ResolvedReview::Github(repo) => {
                return self
                    .pr_monitor_start_op(
                        workspace,
                        agent,
                        number,
                        Some(format!("{}/{}", repo.owner, repo.name)),
                    )
                    .await
            }
            ResolvedReview::Managed(prepared) => prepared,
        };
        let authority = prepared
            .monitor_authority(retained.map_err(|_| source::refused())?)
            .await
            .map_err(|_| source::refused())?;
        if authority.owner() != agent {
            return Err(source::refused());
        }
        authority
            .reserve_output(self)
            .map_err(|_| source::refused())?;
        let target = prepared.target.clone();
        let registration = self.pr_monitor_registration.enter(agent.clone());
        let _admission = registration.acquire().await;
        let _mutation = self.workspace_mutations.enter(workspace)?;
        authority
            .validate(self)
            .await
            .map_err(|_| source::refused())?;
        let holder = self
            .store
            .find_active_qualified_pr_monitor_in_workspace(workspace, &target)
            .await?;
        if let Some(existing) = &holder {
            same_connection(existing.monitor(), &authority)?;
            if let Some(PrMonitorHolder::Live(refusal)) = self
                .classify_pr_monitor_holder(existing.monitor().clone(), agent)
                .await?
            {
                authority
                    .validate(self)
                    .await
                    .map_err(|_| source::refused())?;
                let mut out = refusal.to_wire();
                out["resource"] = json!(target);
                return Ok(out);
            }
        }
        if holder
            .as_ref()
            .is_none_or(|m| &m.monitor().agent_id != agent)
        {
            let cap = self.pr_monitor_max_per_agent() as usize;
            let active = self
                .store
                .list_active_pr_monitors_by_agent(agent)
                .await?
                .len();
            if active >= cap {
                return Err(Error::InvalidParams(format!("pr.monitor: agent already monitors {active} PRs (max {cap}); configure prMonitor.maxPerAgent (1–100) for a larger inventory")));
            }
        }
        let ReadOutcome::Managed { result, .. } =
            prepared.read().await.map_err(|_| source::refused())?
        else {
            return Err(source::refused());
        };
        let (snapshot, retry_at) = match *result {
            Ok(read) => (
                Some(pr_ops::qualified_review_snapshot(&target, &read.value)?),
                None,
            ),
            Err(qualified_cache::CacheFailure {
                cause:
                    qualified_cache::CacheError::Provider(intent_sourcecontrol::Error::RateLimited(_)),
                quota,
            }) => {
                // This is evidence for the captured GitLab connection only.
                // Do not feed it to the legacy daemon-wide GitHub quota gate.
                let reset = quota
                    .reset_at
                    .and_then(|value| i64::try_from(value).ok())
                    .and_then(|value| chrono::DateTime::from_timestamp(value, 0))
                    .filter(|at| *at > chrono::Utc::now());
                (None, reset.map(|at| at.to_rfc3339()))
            }
            Err(failure) => {
                return Err(match failure.cause {
                    qualified_cache::CacheError::Provider(error) => pr_ops::map_sc_err(error),
                    _ => source::refused(),
                })
            }
        };
        authority
            .validate(self)
            .await
            .map_err(|_| source::refused())?;
        let baseline = Some(
            json!({"monitorBinding": authority.binding, "qualifiedSnapshot":snapshot, "retryAt":retry_at}).to_string(),
        );
        let now = now_iso();
        let polled_at = snapshot.as_ref().map(|_| now.clone());
        let last_error = retry_at
            .as_ref()
            .map(|until| format!("GitLab request quota exhausted; paused until {until}"));
        // Recheck the holder after the provider await; a child can pick up work
        // without changing the monitor row, and a competing owner can register.
        let current = self
            .store
            .find_active_qualified_pr_monitor_in_workspace(workspace, &target)
            .await?;
        let mut adopted_from = None;
        let monitor = if let Some(current) = current {
            let old = current.monitor().clone();
            same_connection(&old, &authority)?;
            if old.agent_id == *agent {
                self.refresh_qualified_monitor(
                    current.clone(),
                    &authority,
                    agent,
                    QualifiedBaseline {
                        snapshot: baseline,
                        polled_at: polled_at.clone(),
                        last_error: last_error.clone(),
                        now: now.clone(),
                    },
                )
                .await?
            } else {
                match self.classify_pr_monitor_holder(old, agent).await? {
                    Some(PrMonitorHolder::Live(refusal)) => {
                        let mut out = refusal.to_wire();
                        out["resource"] = json!(target);
                        return Ok(out);
                    }
                    Some(PrMonitorHolder::Orphaned(old) | PrMonitorHolder::SettledChild(old)) => {
                        adopted_from = Some(old.agent_id.clone());
                        self.refresh_qualified_monitor(
                            current.clone(),
                            &authority,
                            agent,
                            QualifiedBaseline {
                                snapshot: baseline,
                                polled_at: polled_at.clone(),
                                last_error: last_error.clone(),
                                now: now.clone(),
                            },
                        )
                        .await?
                    }
                    None => None,
                }
            }
        } else {
            let (owner, name) = target
                .repository
                .project_path
                .rsplit_once('/')
                .ok_or_else(source::refused)?;
            let monitor = PrMonitor {
                monitor_id: PrMonitorId::new(),
                workspace_id: workspace.clone(),
                agent_id: agent.clone(),
                repo_owner: owner.into(),
                repo_name: name.into(),
                pr_number: target.number.cast_signed(),
                state: PrMonitorState::Active,
                last_snapshot: baseline.clone(),
                baseline_snapshot: baseline,
                pending_changes: Vec::new(),
                pending_since: None,
                last_change_at: None,
                last_polled_at: polled_at,
                last_error,
                created_at: now.clone(),
                updated_at: now,
            };
            #[cfg(test)]
            self.monitor_write_ready();
            let write = self.store.prepare_monitor_insert(&monitor, &target).await?;
            authority.commit(self, write).await?.then_some(monitor)
        }
        .ok_or_else(|| {
            Error::InvalidParams("pr.monitor: registration changed concurrently; retry".into())
        })?;
        authority
            .validate(self)
            .await
            .map_err(|_| source::refused())?;
        self.review_monitor_authorities
            .lock()
            .map_err(|_| source::refused())?
            .insert(monitor.monitor_id.clone(), authority.clone());
        self.emit_qualified_monitor_event(
            &authority,
            intent_core::events::PR_MONITOR_REGISTERED,
            &monitor,
            adopted_from
                .as_ref()
                .map(|from| json!({"adoptedFrom":from})),
        );
        let mut out = json!({"ok":true,"monitor":wire(&monitor,&target)?,"requirements":snapshot.as_ref().map(|value| &value["requirements"])});
        if let Some(until) = out["monitor"].get("pausedUntil").cloned() {
            out["pausedUntil"] = until;
        }
        if let Some(from) = adopted_from {
            out["adoptedFrom"] = json!(from);
        }
        Ok(out)
    }

    /// Qualified deferred registrations retain their private binding even with
    /// no baseline. A nonempty envelope is not a successful provider poll.
    async fn refresh_qualified_monitor(
        &self,
        expected: intent_store::QualifiedPrMonitor,
        authority: &Arc<MonitorAuthority>,
        agent: &AgentId,
        baseline: QualifiedBaseline,
    ) -> Result<Option<PrMonitor>> {
        let mut monitor = expected.monitor().clone();
        monitor.agent_id = agent.clone();
        monitor.last_snapshot = baseline.snapshot.clone();
        monitor.baseline_snapshot = baseline.snapshot;
        monitor.pending_changes.clear();
        monitor.pending_since = None;
        monitor.last_change_at = None;
        monitor.last_polled_at = baseline.polled_at;
        monitor.last_error = baseline.last_error;
        monitor.updated_at = baseline.now;
        #[cfg(test)]
        self.monitor_write_ready();
        let write = self
            .store
            .prepare_monitor_replace(&expected, &monitor)
            .await?;
        if !authority.commit(self, write).await? {
            return Ok(None);
        }
        self.consume_pr_monitor_catch_up(&monitor.monitor_id);
        Ok(Some(monitor))
    }

    pub(crate) async fn review_monitor_stop_op(
        &self,
        workspace: &WorkspaceId,
        agent: &AgentId,
        number: u64,
        captured: CapturedReview,
    ) -> Result<Value> {
        let prepared = match captured
            .resolve(self)
            .await
            .map_err(|_| source::refused())?
        {
            ResolvedReview::Unsupported => return Err(source::unsupported()),
            ResolvedReview::Github(repo) => {
                return self
                    .pr_monitor_stop_op(
                        workspace,
                        agent,
                        number,
                        Some(format!("{}/{}", repo.owner, repo.name)),
                    )
                    .await
            }
            ResolvedReview::Managed(prepared) => prepared,
        };
        let target = prepared.target;
        let record = self
            .store
            .find_active_qualified_pr_monitor(agent, &target)
            .await?
            .filter(|m| &m.monitor().workspace_id == workspace)
            .ok_or_else(|| {
                Error::NotFound("pr.unmonitor: no active monitor on the requested review".into())
            })?;
        let mut monitor = record.monitor().clone();
        let authority = self
            .review_monitor_authorities
            .lock()
            .map_err(|_| source::refused())?
            .get(&monitor.monitor_id)
            .cloned()
            .ok_or_else(source::refused)?;
        authority
            .validate(self)
            .await
            .map_err(|_| source::refused())?;
        same_connection(&monitor, &authority)?;
        authority
            .reserve_output(self)
            .map_err(|_| source::refused())?;
        let now = now_iso();
        #[cfg(test)]
        self.monitor_write_ready();
        let write = self.store.prepare_monitor_cancel(&record, &now).await?;
        if !authority.commit(self, write).await? {
            return Err(Error::InvalidParams(
                "pr.unmonitor: monitor changed; retry cancellation".into(),
            ));
        }
        monitor.state = PrMonitorState::Cancelled;
        monitor.updated_at = now;
        self.emit_qualified_monitor_event(
            &authority,
            intent_core::events::PR_MONITOR_CANCELLED,
            &monitor,
            None,
        );
        self.settle_unresolved_monitor_cancel(&monitor).await;
        let out = wire(&monitor, &target)?;
        self.review_monitor_authorities
            .lock()
            .map_err(|_| source::refused())?
            .remove(&monitor.monitor_id);
        Ok(json!({"ok":true,"monitor":out}))
    }

    pub(crate) async fn review_monitor_list_entry(
        &self,
        monitor: &PrMonitor,
    ) -> Result<Option<Value>> {
        let qualified = self
            .store
            .get_qualified_pr_monitor(&monitor.monitor_id)
            .await?;
        match qualified.target() {
            PersistedMonitorTarget::Resolved {
                provenance: MonitorTargetProvenance::LegacyGithubWriter,
                ..
            } => Ok(Some(pr_monitor_wire(
                monitor,
                self.sweep_rate_limit_paused_until().as_deref(),
            ))),
            PersistedMonitorTarget::Resolved { target, .. } => {
                let Ok(current) = crate::repository_read_policy::capture(self) else {
                    return Ok(None);
                };
                if !current.request().supports_review_monitoring() {
                    return Ok(None);
                }
                let authority = self
                    .review_monitor_authorities
                    .lock()
                    .map_err(|_| source::refused())?
                    .get(&monitor.monitor_id)
                    .cloned();
                let Some(authority) = authority else {
                    return Ok(None);
                };
                if authority.validate(self).await.is_err() {
                    return Ok(None);
                }
                authority
                    .reserve_output(self)
                    .map_err(|_| source::refused())?;
                wire(monitor, target).map(Some)
            }
            PersistedMonitorTarget::Unresolved { .. } => Ok(None),
        }
    }
}
