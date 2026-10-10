//! Qualified polling keeps private binding evidence out of public snapshots.
use super::{Arc, PrMonitorState, Services, Value};
use crate::repository_admission::{AdmissionError, AdmissionResult};
use crate::repository_read_source::MonitorAuthority;
use intent_store::{MonitorTargetProvenance, PersistedMonitorTarget, QualifiedPrMonitor};

impl Services {
    pub(super) async fn sweep_qualified_pr_monitors(&self, skip_fresh: bool) {
        self.sweep_qualified_pr_monitors_at(skip_fresh, time::OffsetDateTime::now_utc())
            .await;
    }

    pub(crate) async fn sweep_qualified_pr_monitors_at(
        &self,
        skip_fresh: bool,
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
        let intervals = if skip_fresh {
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
            if skip_fresh
                && monitor
                    .last_polled_at
                    .as_deref()
                    .and_then(intent_core::parse_iso)
                    .is_some_and(|at| {
                        now - at
                            < time::Duration::seconds(
                                i64::try_from(self.pr_monitor_poll_interval().as_secs().max(
                                    intervals.get(&monitor.workspace_id).copied().unwrap_or(0),
                                ))
                                .unwrap_or(i64::MAX),
                            )
                    })
            {
                continue;
            }
            let polled = authority
                .run_poll(Box::pin(
                    original.poll_qualified_monitor(&row, &authority, now),
                ))
                .await;
            // Prompt capture is a daemon-manager entry. Leave the original
            // polling caller scope before preparing a destination; the retained
            // authority and cache delivery still gate every resulting effect.
            let polled = match polled {
                Ok(Some((observed, delivery))) => {
                    original
                        .queue_qualified_monitor_wake(&observed, &authority, delivery, now)
                        .await
                }
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

    async fn poll_qualified_monitor(
        &self,
        expected: &QualifiedPrMonitor,
        authority: &Arc<MonitorAuthority>,
        now: time::OffsetDateTime,
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
        let response =
            tokio::time::timeout(self.pr_monitor_fetch_timeout, authority.read_poll(self)).await;
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
                let activity = !diff(before, &observed).is_empty();
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
                use super::qualified_cache::CacheError;
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
        Ok(delivery.map(|delivery| (updated, delivery)))
    }

    async fn queue_qualified_monitor_wake(
        &self,
        observed: &intent_core::PrMonitor,
        authority: &Arc<MonitorAuthority>,
        delivery: super::qualified_cache::CacheDelivery,
        now: time::OffsetDateTime,
    ) -> AdmissionResult<()> {
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
                || !self.pr_monitor_debounce_elapsed_at(observed, now))
        {
            return Ok(());
        }
        let Some(manager) = self.agent_manager() else {
            return Ok(());
        };
        // Read a fully qualified row for the next prepared CAS. A racing edit
        // must not be turned into a wake backed by another observation.
        let expected = self
            .store
            .get_qualified_pr_monitor(&observed.monitor_id)
            .await
            .map_err(|_| AdmissionError::Unavailable)?;
        if expected.monitor() != observed {
            return Ok(());
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
            let checklist =
                crate::harness::review_wake::checklist(&snapshot["requirements"], false);
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
            return Ok(());
        };
        prepared
            .enqueue(self)
            .await
            .map_err(|_| AdmissionError::Retired)?;
        manager
            .try_drain_queue(observed.agent_id.clone(), observed.workspace_id.clone())
            .await;
        Ok(())
    }
}

#[path = "qualified_diff.rs"]
mod qualified_diff;
use qualified_diff::diff;
