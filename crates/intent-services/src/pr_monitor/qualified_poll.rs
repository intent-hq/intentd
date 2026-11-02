//! Qualified polling keeps private binding evidence out of public snapshots.
use super::qualified_cache::CacheError;
use super::{polled_within, Arc, PrMonitorState, Services, SweepScope, Value};
use crate::repository_admission::{AdmissionError, AdmissionResult};
use crate::repository_read_source::MonitorAuthority;
use intent_store::{MonitorTargetProvenance, PersistedMonitorTarget, QualifiedPrMonitor};

impl Services {
    #[cfg(test)]
    pub(crate) async fn sweep_qualified_pr_monitors_at(
        &self,
        skip_fresh: bool,
        now: time::OffsetDateTime,
    ) {
        let scope = if skip_fresh {
            SweepScope::Due
        } else {
            SweepScope::All
        };
        self.sweep_qualified_pr_monitors_scoped(&scope, now).await;
    }

    pub(super) async fn sweep_qualified_pr_monitors_scoped(
        &self,
        scope: &SweepScope,
        now: time::OffsetDateTime,
    ) {
        let Ok(_lane) = self.review_monitor_poll_lane.clone().try_lock_owned() else {
            return;
        };
        self.review_monitor_deliveries
            .lock()
            .unwrap()
            .retain(|_, record| !record.acknowledged());
        let Ok(rows) = self.store.load_active_qualified_pr_monitors().await else {
            return;
        };
        let workspace_ids = rows
            .iter()
            .map(|row| row.monitor().workspace_id.clone())
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let intervals = if matches!(scope, SweepScope::Due) {
            match self
                .workspace_automatic_check_intervals(&workspace_ids, now)
                .await
            {
                Ok(intervals) => intervals,
                Err(_) => return,
            }
        } else {
            std::collections::HashMap::default()
        };
        for row in rows {
            if let SweepScope::Nudge { workspace_id, .. } = scope {
                if &row.monitor().workspace_id != workspace_id {
                    continue;
                }
            }
            if !matches!(
                row.target(),
                PersistedMonitorTarget::Resolved {
                    provenance: MonitorTargetProvenance::CapturedRequest,
                    ..
                }
            ) {
                continue;
            }
            let monitor = row.monitor();
            let authority = self
                .review_monitor_authorities
                .lock()
                .unwrap()
                .get(&monitor.monitor_id)
                .cloned();
            let Some(authority) = authority else { continue };
            let Ok(original) = authority.original_services() else {
                continue;
            };
            let delivery = self
                .review_monitor_deliveries
                .lock()
                .unwrap()
                .get(&monitor.monitor_id)
                .cloned();
            if let Some(delivery) = delivery {
                if !delivery.active() && delivery.transferred() {
                    // A transferred prompt is never resent because its SQL
                    // acknowledgement failed. Retry only that original write.
                    if let Ok(stamp) = now.format(&time::format_description::well_known::Rfc3339) {
                        let _ = delivery.acknowledge(&stamp).await;
                    }
                }
                if delivery.acknowledged() || (!delivery.active() && !delivery.transferred()) {
                    let mut records = self.review_monitor_deliveries.lock().unwrap();
                    if records
                        .get(&monitor.monitor_id)
                        .is_some_and(|r| Arc::ptr_eq(r, &delivery))
                    {
                        records.remove(&monitor.monitor_id);
                    }
                }
                continue;
            }
            // Recovery never retries an uncertain obligation in the background.
            // Only the original successful transfer acknowledgement releases it.
            if authority.requires_explicit_recovery() {
                continue;
            }
            let fresh_secs = match scope {
                SweepScope::All => None,
                SweepScope::Due => Some(
                    self.pr_monitor_poll_interval()
                        .as_secs()
                        .max(intervals.get(&monitor.workspace_id).copied().unwrap_or(0)),
                ),
                SweepScope::Nudge { min_age_secs, .. } => Some(*min_age_secs),
            };
            if fresh_secs
                .is_some_and(|secs| polled_within(monitor.last_polled_at.as_deref(), secs, now))
            {
                continue;
            }
            let polled = authority
                .run_poll(Box::pin(
                    original.poll_qualified_monitor(&row, &authority, now, None),
                ))
                .await;
            // Prompt capture is a daemon-manager entry. Leave the original
            // polling caller scope before preparing a destination; the retained
            // authority and cache delivery still gate every resulting effect.
            let polled = match polled {
                Ok(Some((observed, delivery))) => original
                    .queue_qualified_monitor_wake(&observed, &authority, delivery, now, false)
                    .await
                    .map(|_| ()),
                Ok(None) => Ok(()),
                Err(error) => Err(error),
            };
            if let Err(error) = polled {
                // An original authority failure cannot mutate its saved binding
                // or substitute current-account permission. Later list/delivery
                // admission withholds this row's private content.
                tracing::debug!(monitor = %monitor.monitor_id.0, ?error, "qualified monitor poll withheld");
            }
        }
    }

    pub(crate) async fn flush_qualified_pr_monitor(
        &self,
        expected: QualifiedPrMonitor,
        captured_authority: Option<Arc<MonitorAuthority>>,
        admitted_connection: intent_core::RepositoryConnectionScope,
        control: Arc<dyn intent_core::repository_request::RepositoryMonitorEventSource>,
        check: bool,
    ) -> AdmissionResult<bool> {
        let _lane = self.review_monitor_poll_lane.clone().lock_owned().await;
        control
            .with_current(&mut || Ok(()))
            .map_err(|_| AdmissionError::Retired)?;
        let current = self
            .store
            .get_qualified_pr_monitor(&expected.monitor().monitor_id)
            .await
            .map_err(|_| AdmissionError::Unavailable)?;
        if current.monitor() != expected.monitor() || current.target() != expected.target() {
            return Ok(false);
        }
        let current_authority = self
            .review_monitor_authorities
            .lock()
            .unwrap()
            .get(&expected.monitor().monitor_id)
            .cloned();
        match (&captured_authority, &current_authority) {
            (Some(expected), Some(current)) if Arc::ptr_eq(expected, current) => {}
            (None, None) => {}
            _ => return Ok(false),
        }
        let previous = self
            .review_monitor_deliveries
            .lock()
            .unwrap()
            .get(&expected.monitor().monitor_id)
            .cloned();
        if let Some(previous) = previous {
            if !previous.active() && previous.transferred() {
                let _ = previous.acknowledge(&intent_core::now_iso()).await;
            }
            if previous.acknowledged() || (!previous.active() && !previous.transferred()) {
                let mut records = self.review_monitor_deliveries.lock().unwrap();
                if records
                    .get(&expected.monitor().monitor_id)
                    .is_some_and(|r| Arc::ptr_eq(r, &previous))
                {
                    records.remove(&expected.monitor().monitor_id);
                }
            }
            return Ok(false);
        }
        if !check && expected.monitor().pending_changes.is_empty() {
            return Ok(false);
        }
        let authority = match captured_authority.as_ref() {
            Some(authority) if authority.binding.connection == admitted_connection => {
                authority.clone()
            }
            _ => {
                let manager = self.agent_manager().ok_or(AdmissionError::Unavailable)?;
                let recovered = manager
                    .recover_monitor_authority(&expected, &admitted_connection)
                    .await
                    .map_err(|_| AdmissionError::Unavailable)?;
                let original = recovered.original_services()?;
                if !Arc::ptr_eq(
                    &original.review_monitor_authorities,
                    &self.review_monitor_authorities,
                ) {
                    return Err(AdmissionError::BindingChanged);
                }
                control
                    .with_current(&mut || {
                        let mut authorities = self.review_monitor_authorities.lock().unwrap();
                        match (
                            captured_authority.as_ref(),
                            authorities.get(&expected.monitor().monitor_id),
                        ) {
                            (Some(expected), Some(current)) if Arc::ptr_eq(expected, current) => {}
                            (None, None) => {}
                            _ => return Err(crate::repository_read_source::refused()),
                        }
                        authorities
                            .insert(expected.monitor().monitor_id.clone(), recovered.clone());
                        Ok(())
                    })
                    .map_err(|_| AdmissionError::Retired)?;
                recovered
            }
        };
        let original = authority.original_services()?;
        let now = time::OffsetDateTime::now_utc();
        // Even plain Flush derives sendable content from a freshly admitted read.
        // Stored private text never becomes a replacement prompt.
        let polled = authority
            .run_poll(Box::pin(original.poll_qualified_monitor(
                &expected,
                &authority,
                now,
                Some(control),
            )))
            .await?;
        let Some((observed, delivery)) = polled else {
            return Ok(false);
        };
        original
            .queue_qualified_monitor_wake(&observed, &authority, delivery, now, true)
            .await
    }

    async fn poll_qualified_monitor(
        &self,
        expected: &QualifiedPrMonitor,
        authority: &Arc<MonitorAuthority>,
        now: time::OffsetDateTime,
        control: Option<Arc<dyn intent_core::repository_request::RepositoryMonitorEventSource>>,
    ) -> AdmissionResult<
        Option<(
            intent_core::PrMonitor,
            super::qualified_cache::CacheDelivery,
        )>,
    > {
        authority.validate(self).await?;
        let old = expected.monitor();
        if old.agent_id != *authority.owner() || old.state != PrMonitorState::Active {
            return Err(AdmissionError::BindingChanged);
        }
        super::routing::same_connection(old, authority)
            .map_err(|_| AdmissionError::BindingChanged)?;
        let mut envelope: Value = serde_json::from_str(
            old.last_snapshot
                .as_deref()
                .ok_or(AdmissionError::Unavailable)?,
        )
        .map_err(|_| AdmissionError::Unavailable)?;
        if envelope["retryAt"]
            .as_str()
            .and_then(intent_core::parse_iso)
            .is_some_and(|deadline| deadline > now)
        {
            return Ok(None);
        }
        let response = tokio::time::timeout(
            self.pr_monitor_fetch_timeout,
            authority.read_poll_controlled(self, control.clone()),
        )
        .await;
        let mut updated = old.clone();
        updated.updated_at = now
            .format(&time::format_description::well_known::Rfc3339)
            .map_err(|_| AdmissionError::Unavailable)?;
        updated.last_polled_at = Some(updated.updated_at.clone());
        let mut delivery = None;
        match response {
            Ok(Ok(Ok(read))) => {
                delivery = Some(read.delivery.clone());
                let fresh = crate::pr_ops::qualified_review_snapshot(
                    &authority.binding.resource,
                    &read.value,
                )
                .map_err(|_| AdmissionError::Unavailable)?;
                let baseline: Value = old
                    .baseline_snapshot
                    .as_deref()
                    .and_then(|value| serde_json::from_str(value).ok())
                    .unwrap_or_else(|| envelope.clone());
                let before = envelope
                    .get("qualifiedDiffSnapshot")
                    .unwrap_or(&envelope["qualifiedSnapshot"]);
                let observed = qualified_diff::observed(before, &fresh);
                let pending = diff(
                    baseline
                        .get("qualifiedDiffSnapshot")
                        .unwrap_or(&baseline["qualifiedSnapshot"]),
                    &observed,
                );
                // A fresh no-op is not evidence that an old uncertain wake was
                // delivered. Keep the complete original row and raw fields.
                let terminal = matches!(fresh["state"].as_str(), Some("merged" | "closed"));
                if authority.requires_explicit_recovery() && pending.is_empty() && !terminal {
                    return Ok(None);
                }
                let activity = !diff(before, &observed).is_empty();
                if authority.requires_explicit_recovery() {
                    envelope["monitorBinding"] = serde_json::to_value(&authority.binding)
                        .map_err(|_| AdmissionError::Unavailable)?;
                }
                envelope["qualifiedDiffSnapshot"] = observed;
                envelope["qualifiedSnapshot"] = fresh;
                envelope["retryAt"] = Value::Null;
                updated.last_snapshot = Some(envelope.to_string());
                if baseline["qualifiedSnapshot"].is_null() {
                    updated.baseline_snapshot = updated.last_snapshot.clone();
                }
                updated.pending_since = if pending.is_empty() {
                    None
                } else {
                    old.pending_since
                        .clone()
                        .or_else(|| Some(updated.updated_at.clone()))
                };
                updated.last_change_at = if pending.is_empty() {
                    None
                } else if activity {
                    Some(updated.updated_at.clone())
                } else {
                    old.last_change_at.clone()
                };
                updated.pending_changes = pending;
                updated.last_error = None;
            }
            Ok(Ok(Err(failure))) => {
                // A failed explicit attempt cannot consume or rewrite old evidence.
                if control.is_some() {
                    return Err(AdmissionError::Unavailable);
                }

                if !matches!(failure.cause, CacheError::Provider(_)) {
                    return Err(AdmissionError::Retired);
                }
                if matches!(
                    failure.cause,
                    CacheError::Provider(intent_sourcecontrol::Error::RateLimited(_))
                ) {
                    let deadline = failure
                        .quota
                        .reset_at
                        .and_then(|value| i64::try_from(value).ok())
                        .and_then(|value| time::OffsetDateTime::from_unix_timestamp(value).ok())
                        .filter(|deadline| *deadline > now)
                        .unwrap_or(now + time::Duration::seconds(60));
                    let retry = deadline
                        .format(&time::format_description::well_known::Rfc3339)
                        .map_err(|_| AdmissionError::Unavailable)?;
                    envelope["retryAt"] = Value::String(retry.clone());
                    updated.last_snapshot = Some(envelope.to_string());
                    updated.last_error = Some(format!(
                        "GitLab request quota exhausted; paused until {retry}"
                    ));
                } else {
                    updated.last_error = Some("Merge request refresh unavailable".into());
                }
            }
            Ok(Err(error)) => return Err(error),
            Err(_) if control.is_some() => return Err(AdmissionError::Unavailable),
            Err(_) => updated.last_error = Some("Merge request refresh timed out".into()),
        }
        let write = self
            .store
            .prepare_monitor_replace(expected, &updated)
            .await
            .map_err(|_| AdmissionError::Unavailable)?;
        if !authority
            .commit_poll(self, write, delivery.clone())
            .await
            .map_err(|_| AdmissionError::Retired)?
        {
            return Ok(None);
        }
        if updated.pending_changes != old.pending_changes
            || updated.last_error != old.last_error
            || envelope.get("retryAt")
                != serde_json::from_str::<Value>(old.last_snapshot.as_deref().unwrap_or("{}"))
                    .ok()
                    .as_ref()
                    .and_then(|old| old.get("retryAt"))
        {
            self.emit_qualified_monitor_event(
                authority,
                intent_core::events::PR_MONITOR_CHANGED,
                &updated,
                Some(serde_json::json!({"changes":updated.pending_changes})),
            );
        }
        Ok(delivery.map(|delivery| (updated, delivery)))
    }

    async fn queue_qualified_monitor_wake(
        &self,
        observed: &intent_core::PrMonitor,
        authority: &Arc<MonitorAuthority>,
        delivery: super::qualified_cache::CacheDelivery,
        now: time::OffsetDateTime,
        force: bool,
    ) -> AdmissionResult<bool> {
        let envelope: Value = observed
            .last_snapshot
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok())
            .ok_or(AdmissionError::Unavailable)?;
        let snapshot = &envelope["qualifiedSnapshot"];
        let state = snapshot["state"]
            .as_str()
            .ok_or(AdmissionError::Unavailable)?;
        let terminal = matches!(state, "merged" | "closed");
        if !terminal
            && (observed.pending_changes.is_empty()
                || (!force && !self.pr_monitor_debounce_elapsed_at(observed, now)))
        {
            return Ok(false);
        }
        let Some(manager) = self.agent_manager() else {
            return Ok(false);
        };
        // Read a fully qualified row for the next prepared CAS. A racing edit
        // must not be turned into a wake backed by another observation.
        let expected = self
            .store
            .get_qualified_pr_monitor(&observed.monitor_id)
            .await
            .map_err(|_| AdmissionError::Unavailable)?;
        if expected.monitor() != observed {
            return Ok(false);
        }
        let target = &authority.binding.resource;
        let label = format!("{}!{}", target.repository.project_path, target.number);
        let title = snapshot["title"]
            .as_str()
            .ok_or(AdmissionError::Unavailable)?;
        let url = snapshot["url"]
            .as_str()
            .ok_or(AdmissionError::Unavailable)?;
        let text = if terminal {
            crate::harness::review_wake::terminal(
                "MR",
                &label,
                &observed.pending_changes,
                title,
                url,
                state == "merged",
            )
        } else {
            let checklist = crate::harness::review_wake::checklist(
                &snapshot["requirements"],
                snapshot["headSha"].as_str(),
                false,
            );
            crate::harness::review_wake::changed(
                "MR",
                &label,
                &observed.pending_changes,
                title,
                url,
                &checklist,
            )
        };
        let metadata = serde_json::from_value(serde_json::json!({
            "type":"pr_monitor_wake", "monitorId":observed.monitor_id,
            "repo":target.repository.project_path, "prNumber":target.number,
            "reason":if terminal { "completed" } else { "changed" }, "url":url,
            "resource":target, "monitorAvailability":{"state":"available"},
        }))
        .map_err(|_| AdmissionError::Unavailable)?;
        let prepared = manager
            .prepare_monitor_turn(expected, authority.clone(), delivery, text, metadata)
            .await
            .map_err(|_| AdmissionError::Unavailable)?;
        let Some(prepared) = prepared.track(self) else {
            return Ok(false);
        };
        prepared
            .enqueue(self)
            .await
            .map_err(|_| AdmissionError::Retired)?;
        manager
            .try_drain_queue(observed.agent_id.clone(), observed.workspace_id.clone())
            .await;
        Ok(true)
    }
}

#[path = "qualified_diff.rs"]
mod qualified_diff;
use qualified_diff::diff;
