//! A negotiated consumer receipt belongs to one physical socket and hello.
//! It supplies compatibility only; private output still needs fresh admission.
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use intent_core::repository_request::RepositoryPrivateOutput;
use intent_core::repository_request::ReviewMonitorHello;
use intent_core::review_monitor::ReviewMonitorRecipientSupport;

use super::{Connection, RepositoryRetirement};

#[path = "monitor/admission.rs"]
mod admission;
#[path = "monitor/flush.rs"]
pub(crate) mod flush;
#[path = "monitor/history.rs"]
mod history;
pub(super) use admission::Projection;
pub(super) use history::capture as capture_history;

#[derive(Default)]
pub(super) struct ConnectionState {
    generation: Mutex<Option<Arc<Receipt>>>,
    closed: AtomicBool,
}

pub(super) struct Receipt {
    support: ReviewMonitorRecipientSupport,
    acknowledged: AtomicBool,
    lifetime: RepositoryRetirement,
}

impl ConnectionState {
    pub(super) fn capture(&self) -> Option<Arc<Receipt>> {
        let receipt = self.generation.lock().ok()?.clone()?;
        receipt.support().map(|_| receipt)
    }
    pub(super) fn close(&self) {
        self.closed.store(true, Ordering::Release);
        let old = self.generation.lock().unwrap().take();
        if let Some(old) = old {
            old.lifetime.end_scope();
        }
    }

    fn replace(&self, support: ReviewMonitorRecipientSupport) -> Option<Arc<Receipt>> {
        let receipt = Arc::new(Receipt {
            support,
            acknowledged: AtomicBool::new(false),
            lifetime: RepositoryRetirement::default(),
        });
        let old = {
            let mut current = self.generation.lock().ok()?;
            if self.closed.load(Ordering::Acquire) {
                return None;
            }
            current.replace(receipt.clone())
        };
        // Never join admitted users while holding the cohort map.
        if let Some(old) = old {
            old.lifetime.end_scope();
        }
        Some(receipt)
    }
}

impl Receipt {
    pub(super) fn support(&self) -> Option<ReviewMonitorRecipientSupport> {
        self.lifetime
            .native_dispatch(|| {
                if self.acknowledged.load(Ordering::Acquire) {
                    Ok(self.support)
                } else {
                    Err(super::AdmissionError::Denied)
                }
            })
            .ok()
    }
}

struct Hello {
    connection: Weak<Connection>,
    receipt: Arc<Receipt>,
}

pub(super) fn hello(
    connection: &Connection,
    support: ReviewMonitorRecipientSupport,
) -> Option<Arc<dyn ReviewMonitorHello>> {
    // Even a failed/rejected new hello retires the old cohort. A caller with no
    // original native entry cannot create a usable replacement.
    let receipt = connection.monitor.replace(support)?;
    connection.entered().ok()?;
    Some(Arc::new(Hello {
        connection: connection.weak.clone(),
        receipt,
    }))
}

impl ReviewMonitorHello for Hello {
    fn retire(&self) {
        self.receipt.lifetime.end_scope();
    }

    fn acknowledge(&self) {
        let Some(connection) = self.connection.upgrade() else {
            return;
        };
        if connection.entered().is_err() {
            return;
        }
        let _ = connection.parent.native_dispatch(|| {
            self.receipt.lifetime.native_dispatch(|| {
                self.receipt.acknowledged.store(true, Ordering::Release);
                Ok(())
            })
        });
    }
}

/// Workspace disclosure belongs to this acknowledged physical recipient. It is
/// independent of the short RPC, and supplies no provider/account authority.
pub(super) struct Recipient {
    workspace: intent_core::WorkspaceId,
    authority: intent_store::RepositoryAuthoritySnapshot,
    connection: Arc<Connection>,
    receipt: Arc<Receipt>,
    lifetime: super::RepositorySourceLifetime,
    _registration: super::RepositorySubscription,
    _subscription: super::RepositorySubscription,
}

impl Receipt {
    fn capable(&self) -> bool {
        self.with_current(|| Ok(())).is_ok()
    }

    fn with_current<T>(
        &self,
        action: impl FnOnce() -> super::AdmissionResult<T>,
    ) -> super::AdmissionResult<T> {
        self.lifetime.native_dispatch(|| {
            use intent_core::review_monitor::{ReviewMonitorOutput, ReviewMonitoringCapabilities};
            if !self.acknowledged.load(Ordering::Acquire)
                || ReviewMonitorOutput::Qualified(())
                    .project(
                        &ReviewMonitoringCapabilities {
                            review_monitoring: Some(1),
                        },
                        self.support,
                    )
                    .is_none()
            {
                return Err(super::AdmissionError::Denied);
            }
            action()
        })
    }
}

impl Recipient {
    async fn acquire(
        request: &Arc<super::Request>,
        workspace: &intent_core::WorkspaceId,
    ) -> intent_core::Result<Arc<Self>> {
        Self::acquire_for(request, workspace, &[]).await
    }

    async fn acquire_for(
        request: &Arc<super::Request>,
        workspace: &intent_core::WorkspaceId,
        additional: &[super::RepositoryLifecycleKey],
    ) -> intent_core::Result<Arc<Self>> {
        let receipt = request
            .monitor_receipt
            .clone()
            .filter(|r| r.capable())
            .ok_or_else(super::unavailable)?;
        let c = &request.connection;
        let (lifetime, registration) = c.new_lifetime().map_err(|_| super::unavailable())?;
        let mut keys = vec![
            super::RepositoryLifecycleKey::Database,
            super::RepositoryLifecycleKey::WireAuthority,
            super::RepositoryLifecycleKey::Workspace(workspace.clone()),
        ];
        keys.extend_from_slice(additional);
        let subscription = lifetime
            .subscribe(&c.services.store, c.caller.caller(), &keys)
            .map_err(|_| super::unavailable())?;
        // The original native caller must freshly pass the same Store-backed
        // workspace/member/credential checks as existing repository reads.
        let authority = super::authority(request, workspace)
            .await
            .map_err(|_| super::unavailable())?;
        let recipient = Arc::new(Self {
            workspace: workspace.clone(),
            authority,
            connection: c.clone(),
            receipt,
            lifetime,
            _registration: registration,
            _subscription: subscription,
        });
        recipient.with_current(&mut || Ok(()))?;
        Ok(recipient)
    }

    pub(super) fn with_current(
        &self,
        action: &mut (dyn FnMut() -> intent_core::Result<()> + Send),
    ) -> intent_core::Result<()> {
        self.connection
            .parent
            .native_dispatch(|| {
                self.receipt
                    .with_current(|| self.lifetime.retirement().native_dispatch(|| Ok(action())))
            })
            .map_err(|_| super::unavailable())?
    }
}

/// Whole-envelope history projection for the original acknowledged native
/// recipient. Proof is loaded before fresh admission and that exact message is
/// retained; current monitor state supplies neither content nor authorization.
pub(crate) async fn history(
    services: &super::Services,
    workspace: &intent_core::WorkspaceId,
    agent: &intent_core::AgentId,
    messages: Vec<intent_core::AgentMessage>,
) -> intent_core::Result<Vec<intent_core::AgentMessage>> {
    let ids = messages
        .iter()
        .filter(|m| crate::agent_ops::is_qualified_monitor_message(m))
        .map(|m| m.id.clone())
        .collect::<Vec<_>>();
    let mut admitted = admitted_history(services, workspace, agent, &ids).await?;
    Ok(messages
        .into_iter()
        .filter_map(|message| {
            if crate::agent_ops::is_qualified_monitor_message(&message) {
                admitted.remove(&message.id)
            } else {
                Some(message)
            }
        })
        .collect())
}

pub(crate) async fn history_index(
    services: &super::Services,
    workspace: &intent_core::WorkspaceId,
    agent: &intent_core::AgentId,
    items: Vec<intent_store::UserMessageIndexItem>,
    preview_chars: usize,
) -> intent_core::Result<Vec<intent_store::UserMessageIndexItem>> {
    let ids = items
        .iter()
        .filter(|i| i.qualified_monitor)
        .map(|i| i.id.clone())
        .collect::<Vec<_>>();
    let mut admitted = admitted_history(services, workspace, agent, &ids).await?;
    Ok(items
        .into_iter()
        .filter_map(|item| {
            if !item.qualified_monitor {
                return Some(item);
            }
            let message = admitted.remove(&item.id)?;
            // Derive every private byte from the exact verified message, never the
            // earlier index read. Match Store's text extraction and character cap.
            if message.role != "user" {
                return None;
            }
            let text = match &message.content {
                serde_json::Value::String(text) => text.clone(),
                serde_json::Value::Array(blocks) => blocks
                    .iter()
                    .filter_map(|b| b.get("text").and_then(serde_json::Value::as_str))
                    .collect::<Vec<_>>()
                    .join(" "),
                other => other.to_string(),
            };
            Some(intent_store::UserMessageIndexItem {
                qualified_monitor: true,
                id: message.id,
                preview: text.chars().take(preview_chars).collect(),
                metadata: message
                    .metadata
                    .as_ref()
                    .and_then(intent_store::project_user_message_index_metadata),
                created_at: message.created_at,
            })
        })
        .collect())
}

async fn admitted_history(
    services: &super::Services,
    workspace: &intent_core::WorkspaceId,
    agent: &intent_core::AgentId,
    ids: &[String],
) -> intent_core::Result<std::collections::HashMap<String, intent_core::AgentMessage>> {
    let mut retained = std::collections::HashMap::new();
    if ids.is_empty() {
        return Ok(retained);
    }
    let subscription = history::current();
    let request = if let Some(subscription) = &subscription {
        subscription.request(services)?
    } else {
        let captured = super::NATIVE_REQUEST.try_with(Clone::clone).ok();
        if !captured.as_ref().is_some_and(|request| {
            request
                .monitor_receipt
                .as_ref()
                .is_some_and(|receipt| receipt.capable())
        }) {
            return Ok(retained);
        }
        super::Request::current(services).map_err(|_| super::unavailable())?
    };
    let recipient = Recipient::acquire_for(
        &request,
        workspace,
        &[
            super::RepositoryLifecycleKey::Agent(agent.clone()),
            super::RepositoryLifecycleKey::MonitorHistory(agent.clone()),
        ],
    )
    .await?;
    let mut admissions = Vec::new();
    for id in ids {
        let Some(proof) = services
            .store
            .get_monitor_message_provenance(agent, id)
            .await?
        else {
            continue;
        };
        if &proof.workspace_id != workspace || proof.message.agent_id != *agent {
            continue;
        }
        if let Ok(admitted) = admission::acquire_history(&request, &recipient, &proof.binding).await
        {
            admissions.push(admitted.eligibility);
            retained.insert(proof.message.id.clone(), proof.message);
        }
    }
    let projection = Projection::new(recipient, admissions);
    projection.with_current(&mut || Ok(()))?;
    if let Some(subscription) = subscription {
        subscription.retain(agent, &retained, &projection)?;
    }
    *request.state.lock().map_err(|_| super::unavailable())? =
        super::RequestState::Monitor(projection);
    Ok(retained)
}

/// Only the original supported native request enters this route. Legacy and ACP
/// callers keep their existing producer. Unknown provenance never triggers reads.
pub(crate) async fn list(
    services: &super::Services,
    workspace: &intent_core::WorkspaceId,
    agent: Option<&intent_core::AgentId>,
) -> intent_core::Result<Option<serde_json::Value>> {
    let Some(captured) = super::NATIVE_REQUEST.try_with(Clone::clone).ok() else {
        return Ok(None);
    };
    if !captured
        .monitor_receipt
        .as_ref()
        .is_some_and(|r| r.capable())
    {
        return Ok(None);
    }
    let request = super::Request::current(services).map_err(|_| super::unavailable())?;
    let recipient = Recipient::acquire(&request, workspace).await?;
    let rows = match agent {
        Some(agent) => {
            services
                .store
                .list_qualified_pr_monitors_by_agent(agent)
                .await?
        }
        None => {
            services
                .store
                .list_qualified_pr_monitors_by_workspace(workspace)
                .await?
        }
    };
    let mut monitors = Vec::new();
    let mut admissions = Vec::new();
    for row in rows
        .into_iter()
        .filter(|row| &row.monitor().workspace_id == workspace)
    {
        match row.target() {
            intent_store::PersistedMonitorTarget::Unresolved { reason } => monitors.push(
                crate::pr_monitor::unresolved_pr_monitor_wire(row.monitor(), *reason),
            ),
            intent_store::PersistedMonitorTarget::Resolved {
                provenance: intent_store::MonitorTargetProvenance::LegacyGithubWriter,
                ..
            } => {
                if let Some(value) = services.review_monitor_list_entry(row.monitor()).await? {
                    monitors.push(value);
                }
            }
            intent_store::PersistedMonitorTarget::Resolved { target, .. } => {
                // Stored identity constrains fresh admission; neither the old
                // runtime map nor today's selected Git remote authorizes this.
                if let Ok(admitted) = admission::acquire(&request, &recipient, &row).await {
                    monitors.push(crate::pr_monitor::qualified_pr_monitor_wire(
                        row.monitor(),
                        target,
                    )?);
                    admissions.push(admitted.eligibility);
                } else {
                    monitors.push(crate::pr_monitor::recovery_pr_monitor_wire(
                        row.monitor(),
                        intent_core::review_monitor::ReviewMonitorUnresolvedReason::MissingAuthority,
                    ));
                }
            }
        }
    }
    let projection = Projection::new(recipient, admissions);
    projection.with_current(&mut || Ok(()))?;
    *request.state.lock().map_err(|_| super::unavailable())? =
        super::RequestState::Monitor(projection);
    Ok(Some(serde_json::json!({"monitors":monitors})))
}

/// An unresolved row has no provider authority to recover. Its original
/// workspace manager may cancel it without interpreting any saved private data.
pub(crate) async fn cancel(
    services: &super::Services,
    workspace: &intent_core::WorkspaceId,
    monitor_id: &intent_core::PrMonitorId,
) -> intent_core::Result<Option<serde_json::Value>> {
    let Some(captured) = super::NATIVE_REQUEST.try_with(Clone::clone).ok() else {
        return Ok(None);
    };
    if !captured
        .monitor_receipt
        .as_ref()
        .is_some_and(|r| r.capable())
    {
        return Ok(None);
    }
    let row = services.store.get_qualified_pr_monitor(monitor_id).await?;
    if matches!(
        row.target(),
        intent_store::PersistedMonitorTarget::Resolved {
            provenance: intent_store::MonitorTargetProvenance::LegacyGithubWriter,
            ..
        }
    ) {
        return Ok(None);
    }
    if &row.monitor().workspace_id != workspace
        || row.monitor().state != intent_core::PrMonitorState::Active
    {
        return Err(super::unavailable());
    }
    let request = super::Request::current(services).map_err(|_| super::unavailable())?;
    let recipient = Recipient::acquire(&request, workspace).await?;
    let mut account = None;
    let reason = match row.target() {
        intent_store::PersistedMonitorTarget::Unresolved { reason } => {
            use intent_core::review_monitor::ReviewMonitorUnresolvedReason as Reason;
            match reason {
                intent_store::MonitorTargetUnresolvedReason::MissingProvenance => {
                    Reason::MissingProvenance
                }
                intent_store::MonitorTargetUnresolvedReason::UnverifiedImport => {
                    Reason::UnverifiedImport
                }
                intent_store::MonitorTargetUnresolvedReason::InvalidTarget => Reason::InvalidTarget,
            }
        }
        intent_store::PersistedMonitorTarget::Resolved { .. } => {
            account = admission::acquire(&request, &recipient, &row).await.ok();
            intent_core::review_monitor::ReviewMonitorUnresolvedReason::MissingAuthority
        }
    };
    // Reserve the writer before entering source/recipient fences. Membership
    // changes after acquisition retire this lifetime through the Store observer.
    let now = intent_core::now_iso();
    #[cfg(test)]
    if let Some(ready) = services
        .test_review_monitor_write_ready
        .lock()
        .unwrap()
        .as_ref()
    {
        ready.notify_one();
    }
    let write = services.store.prepare_monitor_cancel(&row, &now).await?;
    services
        .require_workspace_manager(workspace, "prMonitor.cancel")
        .await?;
    let event_source = EventSource::capture(recipient.clone())?;
    let binding = account.as_ref().map(|account| account.binding.clone());
    let projection = Projection::new(
        recipient.clone(),
        account
            .into_iter()
            .map(|account| account.eligibility)
            .collect(),
    );
    let admitted = projection.clone();
    let event_binding = binding.clone();
    let services = services.clone();
    let mut monitor = row.monitor().clone();
    // Once admitted, the commit and terminal settlement survive a lost caller.
    let monitor = intent_core::caller::spawn_with_current_caller(async move {
        let changed = tokio::task::spawn_blocking(move || {
            let mut write = Some(write);
            let mut changed = false;
            admitted.with_current(&mut || {
                changed = write.take().ok_or_else(super::unavailable)?.commit()?;
                Ok(())
            })?;
            Ok::<_, intent_core::Error>(changed)
        })
        .await
        .map_err(|_| super::unavailable())??;
        if !changed {
            return Err(super::unavailable());
        }
        monitor.state = intent_core::PrMonitorState::Cancelled;
        monitor.updated_at = now;
        services.settle_unresolved_monitor_cancel(&monitor).await;
        if let Some(bus) = &services.event_bus {
            let mut event = intent_core::Event {
                id: uuid::Uuid::now_v7().to_string(), workspace_id: monitor.workspace_id.clone(),
                timestamp: intent_core::now_iso(), event_type: intent_core::events::PR_MONITOR_CANCELLED.into(),
                actor: crate::system_actor(), session_id: Some(monitor.agent_id.0.clone()),
                correlation_id: None, parent_event_id: None, metadata: None,
                data: serde_json::json!({
                    "workspaceId":monitor.workspace_id,"agentId":monitor.agent_id,"monitorId":monitor.monitor_id,
                    "repo":format!("{}/{}",monitor.repo_owner,monitor.repo_name),"prNumber":monitor.pr_number,"state":monitor.state,
                }),
            };
            if let Some(binding) = &event_binding {
                event.data["resource"] = serde_json::json!(binding.resource);
                event.data["monitorAvailability"] = serde_json::json!({"state":"available"});
            }
            let _ = bus.publish_monitor(event, event_binding, event_source);
        }
        Ok::<_, intent_core::Error>(monitor)
    })
    .await
    .map_err(|_| super::unavailable())??;
    projection.with_current(&mut || Ok(()))?;
    let wire = match binding {
        Some(binding) => crate::pr_monitor::qualified_pr_monitor_wire(&monitor, &binding.resource)?,
        None => crate::pr_monitor::recovery_pr_monitor_wire(&monitor, reason),
    };
    *request.state.lock().map_err(|_| super::unavailable())? =
        super::RequestState::Monitor(projection);
    Ok(Some(serde_json::json!({"ok": true, "monitor": wire})))
}

pub(super) fn capture_events(
    connection: &Connection,
) -> Option<Arc<dyn intent_core::repository_request::RepositoryMonitorEventRecipient>> {
    // Subscription construction runs inside the already captured RPC. Generic
    // capture_native deliberately refuses nested captures; fork only this
    // subscription lifetime from the exact original request and hello receipt.
    let original = super::NATIVE_REQUEST.try_with(Clone::clone).ok()?;
    if !std::ptr::eq(original.connection.as_ref(), connection) {
        return None;
    }
    original.check().ok()?;
    let receipt = original.monitor_receipt.clone().filter(|r| r.capable())?;
    let (lifetime, registration) = connection.new_lifetime().ok()?;
    let request = Arc::new_cyclic(|weak| super::Request {
        connection: original.connection.clone(),
        weak: weak.clone(),
        lifetime: Some(lifetime),
        _registration: Some(registration),
        state: Mutex::new(super::RequestState::Ordinary),
        completed: AtomicBool::new(false),
        consumed: AtomicBool::new(false),
        read_lane: Mutex::new(None),
        subscriptions: Mutex::new(Vec::new()),
        queued_query: None,
        monitor_receipt: Some(receipt),
    });
    request
        .subscribe(&[
            super::RepositoryLifecycleKey::Database,
            super::RepositoryLifecycleKey::WireAuthority,
        ])
        .ok()?;
    Some(Arc::new(EventRecipient { request }))
}
struct EventRecipient {
    request: Arc<super::Request>,
}
impl Drop for EventRecipient {
    fn drop(&mut self) {
        self.request.finish();
    }
}
impl intent_core::repository_request::RepositoryMonitorEventRecipient for EventRecipient {
    fn retire(&self) {
        self.request.finish();
    }

    fn project<'a>(
        &'a self,
        event: &'a intent_core::Event,
        binding: Option<&'a intent_core::review_monitor::ReviewMonitorBinding>,
        source: Arc<dyn intent_core::repository_request::RepositoryMonitorEventSource>,
        subscription_id: &'a str,
    ) -> intent_core::BoxFuture<
        'a,
        intent_core::Result<Arc<dyn intent_core::repository_request::RepositoryPrivateOutput>>,
    > {
        Box::pin(async move {
            let c = &self.request.connection;
            let recipient = super::with_caller(
                c.caller.caller().clone(),
                super::with_wire_credential(c.caller.wire_credential().cloned(), async {
                    let recipient = Recipient::acquire(&self.request, &event.workspace_id).await?;
                    let accounts = if let Some(binding) = binding {
                        if event.data.get("resource") != Some(&serde_json::json!(binding.resource))
                        {
                            return Err(super::unavailable());
                        }
                        vec![
                            admission::acquire_event(&self.request, &recipient, binding)
                                .await?
                                .eligibility,
                        ]
                    } else {
                        Vec::new()
                    };
                    Ok::<_, intent_core::Error>(Projection::new(recipient, accounts))
                }),
            )
            .await?;
            let frame = serde_json::json!({
                "jsonrpc":"2.0", "method":"events.event",
                "params":{"subscriptionId":subscription_id,"event":{
                    "type":event.event_type,"workspaceId":event.workspace_id,
                    "id":event.id,"timestamp":event.timestamp,"actor":event.actor,"data":event.data,
                }},
            })
            .to_string();
            let output = Arc::new(EventOutput {
                recipient,
                subscription: self.request.clone(),
                source,
                frame,
            });

            output.with_current(&mut || Ok(()))?;
            Ok(output as Arc<dyn RepositoryPrivateOutput>)
        })
    }
}
struct EventOutput {
    recipient: Arc<Projection>,
    subscription: Arc<super::Request>,
    source: Arc<dyn intent_core::repository_request::RepositoryMonitorEventSource>,
    frame: String,
}
impl intent_core::repository_request::RepositoryPrivateOutput for EventOutput {
    fn frame(&self) -> &str {
        &self.frame
    }
    fn with_current(
        &self,
        action: &mut (dyn FnMut() -> intent_core::Result<()> + Send),
    ) -> intent_core::Result<()> {
        self.source.with_current(&mut || {
            self.subscription
                .lifetime
                .as_ref()
                .ok_or_else(super::unavailable)?
                .retirement()
                .native_dispatch(|| Ok(self.recipient.with_current(action)))
                .map_err(|_| super::unavailable())?
        })
    }
}
struct EventSource {
    _recipient: Arc<Recipient>,
    leaf: RepositoryRetirement,
    parents: Vec<RepositoryRetirement>,
}
impl EventSource {
    fn capture(recipient: Arc<Recipient>) -> intent_core::Result<Arc<Self>> {
        let leaf = RepositoryRetirement::default();
        let parents = vec![
            recipient.connection.parent.clone(),
            recipient.receipt.lifetime.clone(),
            recipient.lifetime.retirement(),
        ];
        for parent in &parents {
            parent
                .native_link(&leaf)
                .map_err(|_| super::unavailable())?;
        }
        Ok(Arc::new(Self {
            _recipient: recipient,
            leaf,
            parents,
        }))
    }
}
impl intent_core::repository_request::RepositoryMonitorEventSource for EventSource {
    fn with_current(
        &self,
        action: &mut (dyn FnMut() -> intent_core::Result<()> + Send),
    ) -> intent_core::Result<()> {
        // Each original parent joins this independent leaf before its mutation.
        // Do not reacquire parent fences: producer and subscriber may be the
        // same socket, and two native recipients may receive each other's events.
        self.leaf
            .native_dispatch(|| {
                if self.parents.iter().any(RepositoryRetirement::native_closed) {
                    return Err(super::AdmissionError::Retired);
                }
                Ok(action())
            })
            .map_err(|_| super::unavailable())?
    }
}
