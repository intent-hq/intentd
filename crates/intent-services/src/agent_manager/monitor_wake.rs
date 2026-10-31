//! One current monitor delivery owns its original runtime and prompt admission.
//! Queue metadata and persisted rows cannot construct or restore this owner.
use super::{AgentManager, OriginalTurn};
use crate::pr_monitor::qualified_cache::CacheDelivery;
use crate::repository_read_source::{refused, MonitorAuthority, MonitorPromptAdmission};
use intent_core::review_monitor::ReviewMonitorWakeMetadata;
use intent_core::{now_iso, Result};
use intent_store::QualifiedPrMonitor;
use std::sync::Arc;

/// Queue cloning shares one consumable owner; it never clones a prompt grant.
pub(crate) struct QueuedMonitorTurn(std::sync::Mutex<Option<PreparedMonitorTurn>>);
impl std::fmt::Debug for QueuedMonitorTurn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("QueuedMonitorTurn(original admission)")
    }
}
impl QueuedMonitorTurn {
    pub(crate) fn take(&self) -> Option<PreparedMonitorTurn> {
        self.0.lock().unwrap().take()
    }
}

pub(crate) struct PreparedMonitorTurn {
    original: OriginalTurn,
    admission: MonitorPromptAdmission,
    row: QualifiedPrMonitor,
    metadata: ReviewMonitorWakeMetadata,
    delivery: Option<MonitorDeliveryGuard>,
}

struct MonitorDeliveryGuard {
    registry: crate::repository_read_source::MonitorDeliveries,
    completion: Arc<crate::repository_read_source::MonitorDeliveryCompletion>,
}
impl Drop for MonitorDeliveryGuard {
    fn drop(&mut self) {
        self.completion.finish();
        if !self.completion.transferred() || self.completion.acknowledged() {
            let mut records = self.registry.lock().unwrap();
            if records
                .get(self.completion.monitor_id())
                .is_some_and(|r| Arc::ptr_eq(r, &self.completion))
            {
                records.remove(self.completion.monitor_id());
            }
        }
    }
}

impl AgentManager {
    pub(crate) async fn recover_monitor_authority(
        &self,
        row: &QualifiedPrMonitor,
        connection: &intent_core::RepositoryConnectionScope,
    ) -> Result<Arc<MonitorAuthority>> {
        let mut original = self.capture_turn(&row.monitor().agent_id)?;
        original
            .prompt
            .as_mut()
            .ok_or_else(refused)?
            .recover_monitor(row, connection)
            .await
            .map_err(|_| refused())
    }

    pub(crate) async fn prepare_monitor_turn(
        &self,
        row: QualifiedPrMonitor,
        authority: Arc<MonitorAuthority>,
        delivery: CacheDelivery,
        text: String,
        metadata: ReviewMonitorWakeMetadata,
    ) -> Result<PreparedMonitorTurn> {
        // Capture from an already initialized runtime, before any delivery
        // wait. Admission compares that physical allocation with registration.
        let mut original = self.capture_turn(&row.monitor().agent_id)?;
        let admission = original
            .prompt
            .as_mut()
            .ok_or_else(refused)?
            .prepare_monitor(authority, delivery, text)
            .await
            .map_err(|_| refused())?;
        Ok(PreparedMonitorTurn {
            original,
            admission,
            row,
            metadata,
            delivery: None,
        })
    }
}

impl PreparedMonitorTurn {
    /// Track only this original owner. The record prevents duplicate delivery;
    /// it never substitutes for admission at enqueue, persistence or transfer.
    pub(crate) fn track(mut self, services: &crate::Services) -> Option<Self> {
        let completion = Arc::new(self.admission.completion(&self.row));
        let registry = services.review_monitor_deliveries.clone();
        {
            let mut records = registry.lock().unwrap();
            if records.contains_key(completion.monitor_id()) {
                return None;
            }
            records.insert(completion.monitor_id().clone(), completion.clone());
        }
        self.delivery = Some(MonitorDeliveryGuard {
            registry,
            completion,
        });
        Some(self)
    }

    pub(crate) async fn enqueue(self, services: &crate::Services) -> Result<()> {
        let Self {
            original,
            admission,
            row,
            metadata,
            delivery,
        } = self;
        let agent = row.monitor().agent_id.clone();
        admission
            .enqueue(move |admission| {
                services.enqueue_monitor_turn(
                    &agent,
                    Arc::new(QueuedMonitorTurn(std::sync::Mutex::new(Some(Self {
                        original,
                        admission,
                        row,
                        metadata,
                        delivery,
                    })))),
                );
            })
            .await
    }

    /// The caller already owns the ordinary manager turn slot. This method
    /// never claims a replacement runtime, appends to an ordinary retry queue,
    /// publishes a private message event. Only a tracked delivery attempts a
    /// guarded pending acknowledgement after an actual original transfer.
    /// The result records actual ACP writer transfer, even on a later error.
    pub(crate) async fn run(
        self,
        manager: &AgentManager,
        slot: super::TurnAdmission,
    ) -> Result<bool> {
        let Self {
            original,
            admission,
            row,
            metadata,
            delivery,
        } = self;
        let monitor = row.monitor();
        if !manager.owns_admission(&monitor.agent_id, slot) {
            return Err(refused());
        }
        let id = uuid::Uuid::now_v7().to_string();
        let prompt = crate::provider_images::PreparedPrompt::new(admission.prompt()?).await?;
        let session_id = admission.session_id().to_owned();
        let transferred = admission.transfer_observation();
        let connection = original.connection.as_ref().ok_or_else(refused)?;
        // Take the actual original stream receiver before writing or sending;
        // a concurrent original turn cannot have its notifications stolen.
        let mut notifications = original.notifications.lock().await;
        if !manager.owns_admission(&monitor.agent_id, slot) {
            return Err(refused());
        }
        let Some(message) = admission
            .commit_message(&row, &id, &metadata, &now_iso())
            .await?
        else {
            return Ok(false);
        };
        // The row is durable even if later native transfer fails. Notify only
        // qualified chat readers; they must independently authorize the fresh
        // read and keep its original fences through the socket write.
        if let Some(bus) = &manager.services.event_bus {
            bus.notify_monitor_history(monitor.agent_id.as_str());
        }
        admission.bind_committed(&message).await?;
        if !manager.owns_admission(&monitor.agent_id, slot) {
            return Err(refused());
        }
        manager.registry.mark_active(&monitor.agent_id);
        let result = manager
            .services
            .run_prompt_turn_captured(
                original.runtime.as_ref(),
                &mut notifications,
                &monitor.agent_id,
                &monitor.workspace_id,
                &session_id,
                prompt,
                None,
                Some(crate::agent_session::LocalPromptInput {
                    connection,
                    captured: None,
                    required: Some(Box::new(admission)),
                }),
                false,
            )
            .await;
        manager.registry.mark_idle_slot_held(&monitor.agent_id);
        if transferred.transferred() {
            if let Some(delivery) = &delivery {
                // A failed acknowledgement retains its original record for a
                // later guarded retry, without another prompt transfer.
                delivery.completion.acknowledge(&now_iso()).await?;
            }
            return Ok(true);
        }
        result.map(|_| false)
    }
}
