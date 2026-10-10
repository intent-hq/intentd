//! Reads for a monitor retain its original caller and pinned resource.
use super::{
    credential, read_managed_review, refused, AdmissionError, AdmissionResult, Arc,
    CacheAdmissionGuard, CacheDelivery, CacheFailure, CacheRead, CacheRequest, Duration, HashSet,
    ManagedCacheRequest, MonitorAuthority, Mutex, PrReadPolicy, RepositoryCredentialError,
    RepositoryReadChild, RepositoryReadOperation, RepositoryReadRequest, ReviewObservation,
    Services,
};
use crate::repository_admission::request_context::RepositoryManagerPromptRequest;
use intent_acp::transport::{
    AcpPromptAdmission, AcpPromptAdmissionOutcome, AcpPromptBoundary, PreparedAcpPromptTransfer,
    PromptVariant,
};

pub(crate) struct MonitorPromptAdmission {
    authority: Arc<MonitorAuthority>,
    destination: Arc<RepositoryManagerPromptRequest>,
    child: Arc<RepositoryReadChild>,
    delivery: CacheDelivery,
    connection: std::sync::Weak<intent_acp::Connection>,
    session_id: String,
    text: String,
    params: serde_json::Value,
    transferred: Arc<std::sync::atomic::AtomicBool>,
}

impl MonitorPromptAdmission {
    /// Retains the original leaves and exact captured row. A transfer fact alone
    /// grants no write permission; acknowledgement revalidates these leaves.
    pub(crate) fn completion(
        &self,
        expected: &intent_store::QualifiedPrMonitor,
    ) -> MonitorDeliveryCompletion {
        MonitorDeliveryCompletion {
            authority: self.authority.clone(),
            destination: self.destination.clone(),
            child: self.child.clone(),
            delivery: self.delivery.clone(),
            transferred: self.transferred.clone(),
            expected: expected.clone(),
            active: std::sync::atomic::AtomicBool::new(true),
            acknowledged: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// The sole effect is a synchronous insertion into the existing in-memory
    /// queue. Retained original leaves fence that insertion; no ID or stored
    /// metadata can reconstruct this consumed owner after restart.
    pub(crate) async fn enqueue<T: Send>(
        self,
        insert: impl FnOnce(Self) -> T + Send,
    ) -> intent_core::Result<T> {
        let authority = self.authority.clone();
        let destination = self.destination.clone();
        let child = self.child.clone();
        let delivery = self.delivery.clone();
        destination
            .run(Box::pin(async move {
                let services = authority.original_services()?;
                authority.validate(&services).await?;
                authority.check_destination(self.destination.original().read())?;
                let mut owner = Some(self);
                let mut insert = Some(insert);
                let mut result = None;
                delivery
                    .with_current(&mut || {
                        authority
                            .lifetime
                            .transfer(|| {
                                child.transfer(|| {
                                    authority
                                        .eligibility
                                        .get()
                                        .ok_or(AdmissionError::Retired)?
                                        .with_current(&mut || {
                                            result = Some(insert
                                                .take()
                                                .ok_or(RepositoryCredentialError::Retired)?(
                                                owner
                                                    .take()
                                                    .ok_or(RepositoryCredentialError::Retired)?,
                                            ));
                                            Ok(())
                                        })
                                        .map_err(|_| AdmissionError::Retired)
                                })
                            })
                            .map_err(credential)
                    })
                    .map_err(|_| AdmissionError::Retired)?;
                result.ok_or(AdmissionError::Retired)
            }))
            .await
            .map_err(|_| refused())
    }

    pub(crate) fn session_id(&self) -> &str {
        &self.session_id
    }

    pub(crate) fn prompt(&self) -> intent_core::Result<Vec<intent_acp::session::ContentBlock>> {
        serde_json::from_value(self.params["prompt"].clone()).map_err(|_| refused())
    }

    /// A fact about this one transfer, never permission for a later effect.
    pub(crate) fn transfer_observation(&self) -> MonitorTransferObservation {
        MonitorTransferObservation(self.transferred.clone())
    }
    /// Persist a current wake under the same original destination used by the
    /// final prompt. This is not delivery: pending acknowledgement and every
    /// output copy still require their own admitted boundary.
    pub(crate) async fn commit_message(
        &self,
        expected: &intent_store::QualifiedPrMonitor,
        id: &str,
        metadata: &intent_core::review_monitor::ReviewMonitorWakeMetadata,
        created_at: &str,
    ) -> intent_core::Result<Option<intent_core::AgentMessage>> {
        let services = self.authority.original_services().map_err(|_| refused())?;
        self.destination.run(Box::pin(async {
            self.authority.validate(&services).await?;
            self.authority.check_destination(self.destination.original().read())?;
            let monitor = expected.monitor();
            if monitor.agent_id != *self.authority.owner()
                || monitor.workspace_id != self.authority.member.agent.workspace_id
                || !matches!(expected.target(), intent_store::PersistedMonitorTarget::Resolved { target, provenance: intent_store::MonitorTargetProvenance::CapturedRequest } if target == &self.authority.binding.resource)
                || !services.review_monitor_authorities.lock().unwrap()
                    .get(&monitor.monitor_id)
                    .is_some_and(|actual| Arc::ptr_eq(actual, &self.authority))
            {
                return Err(AdmissionError::Denied);
            }
            Ok(())
        })).await.map_err(|_| refused())?;
        #[cfg(test)]
        if let Some(ready) = services
            .test_review_monitor_write_ready
            .lock()
            .unwrap()
            .as_ref()
        {
            ready.notify_one();
        }
        let write = services
            .store
            .prepare_monitor_message(expected, id, &self.text, metadata, created_at)
            .await?;
        // Recheck after reserving the writer, then move only the prebuilt SQL
        // effect and original leaves to the blocking commit worker.
        self.destination
            .run(Box::pin(async {
                self.authority.validate(&services).await?;
                self.authority
                    .check_destination(self.destination.original().read())
            }))
            .await
            .map_err(|_| refused())?;
        let authority = self.authority.clone();
        let destination = self.destination.clone();
        let child = self.child.clone();
        let delivery = self.delivery.clone();
        tokio::task::spawn_blocking(move || {
            // Retain the request owner through COMMIT even if its waiter drops.
            let _destination = destination;
            let mut write = Some(write);
            let mut result = None;
            delivery
                .with_current(&mut || {
                    result = Some(
                        authority
                            .lifetime
                            .commit_monitor_message(
                                &child,
                                authority
                                    .eligibility
                                    .get()
                                    .ok_or(RepositoryCredentialError::Retired)?,
                                write.take().ok_or(RepositoryCredentialError::Retired)?,
                            )
                            .map_err(credential)?,
                    );
                    Ok(())
                })
                .map_err(|_| refused())?;
            result.ok_or_else(refused)?
        })
        .await
        .map_err(|_| refused())?
    }
}

pub(crate) struct MonitorDeliveryCompletion {
    authority: Arc<MonitorAuthority>,
    destination: Arc<RepositoryManagerPromptRequest>,
    child: Arc<RepositoryReadChild>,
    delivery: CacheDelivery,
    transferred: Arc<std::sync::atomic::AtomicBool>,
    expected: intent_store::QualifiedPrMonitor,
    active: std::sync::atomic::AtomicBool,
    acknowledged: Arc<std::sync::atomic::AtomicBool>,
}

pub(crate) type MonitorDeliveries =
    Arc<Mutex<std::collections::HashMap<intent_core::PrMonitorId, Arc<MonitorDeliveryCompletion>>>>;

impl MonitorDeliveryCompletion {
    pub(crate) fn monitor_id(&self) -> &intent_core::PrMonitorId {
        &self.expected.monitor().monitor_id
    }
    pub(crate) fn active(&self) -> bool {
        self.active.load(std::sync::atomic::Ordering::Acquire)
    }
    pub(crate) fn finish(&self) {
        self.active
            .store(false, std::sync::atomic::Ordering::Release);
    }
    pub(crate) fn transferred(&self) -> bool {
        self.transferred.load(std::sync::atomic::Ordering::Acquire)
    }
    pub(crate) fn acknowledged(&self) -> bool {
        self.acknowledged.load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) async fn acknowledge(&self, now: &str) -> intent_core::Result<bool> {
        if self.acknowledged() {
            return Ok(false);
        }
        if !self.transferred.load(std::sync::atomic::Ordering::Acquire) {
            return Err(refused());
        }
        let services = self.authority.original_services().map_err(|_| refused())?;
        self.destination.run(Box::pin(async {
            self.authority.validate(&services).await?;
            self.authority.check_destination(self.destination.original().read())?;
            let monitor = self.expected.monitor();
            if monitor.agent_id != *self.authority.owner()
                || monitor.workspace_id != self.authority.member.agent.workspace_id
                || !matches!(self.expected.target(), intent_store::PersistedMonitorTarget::Resolved {
                    target, provenance: intent_store::MonitorTargetProvenance::CapturedRequest
                } if target == &self.authority.binding.resource)
                || !services.review_monitor_authorities.lock().unwrap()
                    .get(&monitor.monitor_id)
                    .is_some_and(|actual| Arc::ptr_eq(actual, &self.authority))
            {
                return Err(AdmissionError::Denied);
            }
            Ok(())
        })).await.map_err(|_| refused())?;
        let mut acknowledged = self.expected.monitor().clone();
        let envelope: serde_json::Value = acknowledged
            .last_snapshot
            .as_deref()
            .and_then(|s| serde_json::from_str(s).ok())
            .ok_or_else(refused)?;
        let state = envelope["qualifiedSnapshot"]["state"]
            .as_str()
            .ok_or_else(refused)?;
        if matches!(state, "merged" | "closed") {
            acknowledged.state = intent_core::PrMonitorState::Completed;
        }
        acknowledged.baseline_snapshot = acknowledged.last_snapshot.clone();
        acknowledged.pending_changes.clear();
        acknowledged.pending_since = None;
        acknowledged.last_change_at = None;
        acknowledged.updated_at = now.to_owned();
        let write = services
            .store
            .prepare_monitor_replace(&self.expected, &acknowledged)
            .await?;
        self.destination
            .run(Box::pin(async {
                self.authority.validate(&services).await?;
                self.authority
                    .check_destination(self.destination.original().read())
            }))
            .await
            .map_err(|_| refused())?;
        let authority = self.authority.clone();
        let destination = self.destination.clone();
        let child = self.child.clone();
        let delivery = self.delivery.clone();
        let acknowledged_monitor = acknowledged;
        let acknowledged = self.acknowledged.clone();
        tokio::task::spawn_blocking(move || {
            let _destination = destination;
            let mut write = Some(write);
            let mut result = None;
            delivery
                .with_current(&mut || {
                    result = Some(
                        authority
                            .lifetime
                            .commit_monitor_delivery(
                                &child,
                                authority
                                    .eligibility
                                    .get()
                                    .ok_or(RepositoryCredentialError::Retired)?,
                                write.take().ok_or(RepositoryCredentialError::Retired)?,
                            )
                            .map_err(credential)?,
                    );
                    Ok(())
                })
                .map_err(|_| refused())?;
            let result = result.ok_or_else(refused)?;
            if matches!(result, Ok(true)) {
                // Record completion on the admitted worker, even if its async
                // waiter was cancelled during SQL COMMIT.
                acknowledged.store(true, std::sync::atomic::Ordering::Release);
                let event_type =
                    if acknowledged_monitor.state == intent_core::PrMonitorState::Completed {
                        intent_core::events::PR_MONITOR_COMPLETED
                    } else {
                        intent_core::events::PR_MONITOR_EMITTED
                    };
                services.emit_qualified_monitor_event(
                    &authority,
                    event_type,
                    &acknowledged_monitor,
                    None,
                );
            }
            result
        })
        .await
        .map_err(|_| refused())?
    }
}

pub(crate) struct MonitorTransferObservation(Arc<std::sync::atomic::AtomicBool>);
impl MonitorTransferObservation {
    pub(crate) fn transferred(&self) -> bool {
        self.0.load(std::sync::atomic::Ordering::Acquire)
    }
}

impl AcpPromptAdmission for MonitorPromptAdmission {
    fn admit<'a>(
        &'a self,
        boundary: &'a AcpPromptBoundary,
        packet: PreparedAcpPromptTransfer<'a>,
    ) -> intent_core::BoxFuture<'a, AcpPromptAdmissionOutcome> {
        Box::pin(async move {
            let Some(connection) = self.connection.upgrade() else {
                return AcpPromptAdmissionOutcome::ConsumerClosed;
            };
            if !boundary.is_for(&connection, &self.session_id) || !boundary.has_params(&self.params)
            {
                return AcpPromptAdmissionOutcome::ForeignBoundary;
            }
            self.destination
                .run(Box::pin(async move {
                    let services = self.authority.original_services()?;
                    self.authority.validate(&services).await?;
                    self.authority
                        .check_destination(self.destination.original().read())?;
                    let eligibility = self
                        .authority
                        .eligibility
                        .get()
                        .ok_or(AdmissionError::Unavailable)?;
                    let mut packet = Some(packet);
                    let mut result = None;
                    self.delivery
                        .with_current(&mut || {
                            self.authority
                                .lifetime
                                .transfer(|| {
                                    self.child.transfer(|| {
                                        eligibility
                                            .with_current(&mut || {
                                                let outcome = packet
                                                    .take()
                                                    .ok_or(RepositoryCredentialError::Retired)?
                                                    .transfer(
                                                        boundary,
                                                        PromptVariant::WithGuidance,
                                                    );
                                                if matches!(
                                                    outcome,
                                                    AcpPromptAdmissionOutcome::Transferred(_)
                                                ) {
                                                    self.transferred.store(
                                                        true,
                                                        std::sync::atomic::Ordering::Release,
                                                    );
                                                }
                                                result = Some(outcome);
                                                Ok(())
                                            })
                                            .map_err(|_| AdmissionError::Retired)
                                    })
                                })
                                .map_err(credential)
                        })
                        .map_err(|_| AdmissionError::Retired)?;
                    result.ok_or(AdmissionError::Retired)
                }))
                .await
                .unwrap_or(AcpPromptAdmissionOutcome::OmitOptional)
        })
    }
}

impl MonitorAuthority {
    fn check_destination(&self, destination: &RepositoryReadRequest) -> AdmissionResult<()> {
        if !destination.supports_review_monitoring() {
            return Err(AdmissionError::Denied);
        }
        self.request.same_physical_session(destination)
    }

    pub(crate) async fn prepare_prompt(
        self: &Arc<Self>,
        destination: RepositoryManagerPromptRequest,
        delivery: CacheDelivery,
        connection: std::sync::Weak<intent_acp::Connection>,
        session_id: String,
        text: String,
    ) -> AdmissionResult<MonitorPromptAdmission> {
        if text.trim().is_empty() {
            return Err(AdmissionError::Denied);
        }
        let child = destination
            .run(Box::pin(async {
                let services = self.original_services()?;
                self.validate(&services).await?;
                self.check_destination(destination.original().read())?;
                destination.original().read().child()
            }))
            .await?;
        Ok(MonitorPromptAdmission {
            authority: self.clone(),
            destination: Arc::new(destination),
            child: Arc::new(child),
            delivery,
            connection,
            params: serde_json::json!({"sessionId":session_id, "prompt":[{"type":"text", "text":text}]}),
            session_id,
            text,
            transferred: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        })
    }

    pub(crate) fn original_services(&self) -> AdmissionResult<Arc<Services>> {
        self.services.upgrade().ok_or(AdmissionError::Retired)
    }

    /// The reserved writer is acquired before cache/physical/account fences.
    /// Keep the exact successful observation current through SQL COMMIT too.
    pub(crate) async fn commit_poll(
        self: &Arc<Self>,
        services: &Services,
        write: intent_store::PreparedMonitorWrite,
        delivery: Option<CacheDelivery>,
    ) -> intent_core::Result<bool> {
        self.validate(services).await.map_err(|_| refused())?;
        let authority = self.clone();
        tokio::task::spawn_blocking(move || {
            let mut write = Some(write);
            let mut result = None;
            let mut commit = || {
                result = Some(
                    authority
                        .lifetime
                        .commit_monitor_write(
                            authority
                                .eligibility
                                .get()
                                .ok_or(RepositoryCredentialError::Retired)?,
                            write.take().ok_or(RepositoryCredentialError::Retired)?,
                        )
                        .map_err(credential)?,
                );
                Ok(())
            };
            if let Some(delivery) = delivery {
                delivery.with_current(&mut commit).map_err(|_| refused())?;
            } else {
                commit().map_err(|_| refused())?;
            }
            result.ok_or_else(refused)?
        })
        .await
        .map_err(|_| refused())?
    }

    pub(crate) fn run_poll<'a, T: Send + 'a>(
        &'a self,
        body: intent_core::BoxFuture<'a, AdmissionResult<T>>,
    ) -> intent_core::BoxFuture<'a, AdmissionResult<T>> {
        self.request.run_monitor(body)
    }

    pub(crate) async fn read_poll(
        self: &Arc<Self>,
        services: &Services,
    ) -> AdmissionResult<std::result::Result<CacheRead<ReviewObservation>, CacheFailure>> {
        self.validate(services).await?;
        let directory = services.repository_connection_directory();
        let admission = || {
            directory
                .admit(
                    &self.settled.selected().binding,
                    self.credential_request(),
                    self.source
                        .get()
                        .ok_or(AdmissionError::Unavailable)?
                        .clone(),
                )
                .map_err(|_| AdmissionError::Unavailable)
        };
        let reader = services
            .gitlab_repository_secret_reader()
            .map_err(|_| AdmissionError::Unavailable)?;
        let operation = || {
            RepositoryReadOperation::new(
                directory.clone(),
                admission()?,
                reader.clone(),
                Duration::from_secs(30),
                self.binding.resource.clone(),
            )
            .map_err(|_| AdmissionError::Unavailable)
        };
        let primary = operation()?;
        let full = operation()?;
        let eligibility = self.eligibility.get().ok_or(AdmissionError::Unavailable)?;
        let revalidate = || self.request.check_current().map_err(|_| refused());
        let guard: &CacheAdmissionGuard<'_> = &|action| {
            self.lifetime
                .transfer(|| Ok(action()))
                .map_err(credential)?
        };
        let request = ManagedCacheRequest {
            request: CacheRequest {
                connection: &self.observations,
                target: &self.binding.resource,
                revalidate: &revalidate,
            },
            eligibility,
            with_authority: guard,
        };
        let outcome = read_managed_review(
            &services.pr_cache,
            &request,
            PrReadPolicy::Poll,
            &HashSet::new(),
            || primary.review_details(),
            || full.review_observation(),
        )
        .await;
        self.validate(services).await?;
        Ok(outcome)
    }
}

/// A native event keeps the actual original agent admission alive, not a row or
/// account identifier. Native recipient admission independently requires this
/// producer's exact current account generation through final transport egress.
struct NativeMonitorEventSource(Arc<MonitorAuthority>);
impl intent_core::repository_request::RepositoryMonitorEventSource for NativeMonitorEventSource {
    fn with_current(
        &self,
        action: &mut (dyn FnMut() -> intent_core::Result<()> + Send),
    ) -> intent_core::Result<()> {
        self.0
            .lifetime
            .retirement()
            .native_dispatch(|| Ok(action()))
            .map_err(|_| refused())?
    }
}
impl MonitorAuthority {
    pub(crate) fn native_event_source(
        self: &Arc<Self>,
        monitor: &intent_core::PrMonitor,
    ) -> intent_core::Result<Arc<dyn intent_core::repository_request::RepositoryMonitorEventSource>>
    {
        if monitor.agent_id != *self.owner()
            || monitor.workspace_id != self.member.agent.workspace_id
        {
            return Err(refused());
        }
        Ok(Arc::new(NativeMonitorEventSource(self.clone())))
    }
}
