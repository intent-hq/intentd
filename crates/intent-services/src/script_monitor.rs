//! Durable script-monitor wake delivery and lifecycle suppression.
use crate::Services;
use intent_core::{AgentId, MessageOrigin, Result, ScriptMonitor, WorkspaceId};
use serde_json::{json, Value};

pub(crate) fn wake_id(metadata: Option<&Value>) -> Option<String> {
    let md = metadata?;
    (md["type"] == "script_monitor_wake")
        .then(|| {
            md["monitorId"]
                .as_str()
                .map(|id| format!("script-monitor:{id}"))
        })
        .flatten()
}

pub(crate) fn monitor_id(metadata: Option<&Value>) -> Option<&str> {
    let md = metadata?;
    (md["type"] == "script_monitor_wake")
        .then(|| md["monitorId"].as_str())
        .flatten()
}

impl Services {
    pub(crate) fn track_script_monitor_worker(&self, agent: &AgentId, metadata: Option<&Value>) {
        let mut workers = self
            .script_locks
            .monitor_workers
            .lock()
            .expect("monitor worker registry");
        if let Some(id) = monitor_id(metadata) {
            workers.insert(agent.clone(), id.to_owned());
        } else {
            workers.remove(agent);
        }
    }

    pub(crate) async fn settle_script_monitor_worker(&self, agent: &AgentId) {
        let id = self
            .script_locks
            .monitor_workers
            .lock()
            .expect("monitor worker registry")
            .remove(agent);
        if let Some(id) = id {
            self.complete_script_monitor_delivery(agent, &id).await;
        }
    }

    pub(crate) async fn complete_script_monitor_delivery(&self, agent: &AgentId, id: &str) {
        {
            let mut workers = self
                .script_locks
                .monitor_workers
                .lock()
                .expect("monitor worker registry");
            if workers.get(agent).is_some_and(|current| current == id) {
                workers.remove(agent);
            }
        }
        // Record the exact finished delivery before awaiting SQL. A temporary
        // marker-write failure must neither lose retry ownership nor re-prompt.
        self.script_locks
            .monitor_completions
            .lock()
            .expect("monitor completion retries")
            .insert((agent.clone(), id.to_owned()));
        self.retry_script_monitor_completion(agent, id).await;
    }

    pub(crate) async fn retry_script_monitor_completion(&self, agent: &AgentId, id: &str) {
        match self.store.complete_script_notification(agent, id).await {
            Ok(()) => {
                self.script_locks
                    .monitor_completions
                    .lock()
                    .expect("monitor completion retries")
                    .remove(&(agent.clone(), id.to_owned()));
                // The earlier idle event may have been deferred by this receipt.
                self.redeliver_completion_after_queue_mutation(agent).await;
            }
            Err(error) => {
                tracing::warn!(%error, monitor=id, "notification completion remains owned for retry");
            }
        }
    }

    pub(crate) fn script_monitor_export_blocked(&self, metadata: Option<&Value>) -> bool {
        let Some(md) = metadata.filter(|md| monitor_id(Some(md)).is_some()) else {
            return false;
        };
        self.transfer_exports
            .lock()
            .expect("export registry")
            .values()
            .any(|session| md["workspaceId"].as_str() == Some(session.workspace_id.as_str()))
    }

    pub(crate) fn defer_script_monitor_for_export(
        &self,
        agent: &AgentId,
        content: &str,
        metadata: Option<&Value>,
    ) -> bool {
        if !self.script_monitor_export_blocked(metadata) {
            return false;
        }
        self.enqueue_message_with_id(
            agent,
            wake_id(metadata),
            content.to_owned(),
            None,
            None,
            metadata.cloned(),
            None,
            false,
            MessageOrigin::Automatic,
        );
        true
    }
    pub(crate) fn start_script_monitor_maintenance(&self) {
        if self
            .script_locks
            .monitor_maintenance
            .swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            return;
        }
        let services = self.clone();
        intent_core::spawn_daemon(async move {
            let mut sweep = tokio::time::interval(std::time::Duration::from_secs(1));
            loop {
                sweep.tick().await;
                let completions = services
                    .script_locks
                    .monitor_completions
                    .lock()
                    .expect("monitor completion retries")
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>();
                for (agent, id) in completions {
                    services.retry_script_monitor_completion(&agent, &id).await;
                }
                if let Err(error) = services
                    .store
                    .cleanup_completed_script_notifications()
                    .await
                {
                    tracing::warn!(%error, "completed notification cleanup will retry");
                }
                if let Ok(rows) = services.store.pending_script_monitors().await {
                    for (row, _) in rows {
                        if row.state == "active" {
                            if let Err(error) = services
                                .script_manager()
                                .reconcile_monitor(&row.workspace_id, &row.monitor_id)
                                .await
                            {
                                tracing::warn!(%error, monitor=%row.monitor_id, "script monitor reconciliation failed; will retry");
                            }
                        } else {
                            services.dispatch_script_monitor(&row).await;
                        }
                    }
                }
                if let Ok(jobs) = services.store.script_notification_jobs().await {
                    for job in jobs {
                        if let Err(error) = services.dispatch_script_notification(&job).await {
                            tracing::warn!(%error, wake=%job.wake_id, "script notification remains pending");
                        }
                    }
                }
                if let (Some(manager), Ok(owners)) = (
                    services.agent_manager(),
                    services.store.script_notification_delivery_owners().await,
                ) {
                    for (agent, ws) in owners {
                        manager.clone().try_drain_queue(agent, ws).await;
                    }
                }
            }
        });
    }

    pub(crate) async fn active_script_monitors_for_agent(&self, agent: &AgentId) -> Vec<Value> {
        self.store
            .script_monitor_waiting(None, Some(agent))
            .await
            .unwrap_or_default()
            .into_iter()
            .map(|(_, row)| row)
            .collect()
    }

    pub(crate) async fn annotate_waiting_on_script_monitors(
        &self,
        agent: &AgentId,
        data: &mut Value,
    ) {
        let rows = self.active_script_monitors_for_agent(agent).await;
        if !rows.is_empty() {
            data["waitingOnScriptMonitors"] = json!(rows);
        }
    }

    pub(crate) async fn dispatch_script_monitor(&self, row: &ScriptMonitor) {
        if let Err(error) = self.dispatch_script_monitor_inner(row).await {
            tracing::warn!(%error,monitor=%row.monitor_id,"script monitor wake remains pending");
        }
        self.redeliver_completion_after_queue_mutation(&row.agent_id)
            .await;
    }

    async fn dispatch_script_monitor_inner(&self, row: &ScriptMonitor) -> Result<()> {
        if let Some(job) = self
            .store
            .script_notification_jobs()
            .await?
            .into_iter()
            .find(|job| job.wake_id == format!("script-monitor:{}", row.monitor_id))
        {
            self.dispatch_script_notification(&job).await?;
        }
        Ok(())
    }

    async fn dispatch_script_notification(
        &self,
        job: &intent_store::ScriptNotificationJob,
    ) -> Result<()> {
        let lane = self.script_locks.monitor_lane.lock().await;
        if self.script_monitor_export_blocked(Some(&job.metadata)) {
            return Ok(());
        }
        let Some(id) = monitor_id(Some(&job.metadata)) else {
            return Ok(());
        };
        if !self.store.script_monitor_wake_pending(id).await? {
            return Ok(());
        }
        if !self
            .script_monitor_delivery_allowed(&job.agent_id, Some(&job.metadata))
            .await?
        {
            self.store.finish_script_monitor_wake(id, true).await?;
            return Ok(());
        }
        let Some(manager) = self.agent_manager() else {
            return Ok(());
        };
        let (queued, position) = self.enqueue_message_with_id(
            &job.agent_id,
            Some(job.wake_id.clone()),
            job.body.clone(),
            None,
            None,
            Some(job.metadata.clone()),
            None,
            false,
            MessageOrigin::Automatic,
        );
        let receipt = intent_store::AgentQueueRow {
            id: queued.id.clone(),
            agent_id: job.agent_id.clone(),
            position: i64::try_from(position).unwrap_or(i64::MAX),
            payload: serde_json::to_value(&queued)
                .map_err(|e| intent_core::Error::Internal(e.to_string()))?,
            created_at: queued.queued_at.clone(),
            turn_id: queued.turn_id.clone(),
        };
        // This transaction, not the transcript or best-effort snapshot, transfers ownership.
        self.store.admit_script_notification(&receipt).await?;
        self.publish_queue_updated(&job.agent_id).await;
        drop(lane);
        manager
            .clone()
            .try_drain_queue(job.agent_id.clone(), job.workspace_id.clone())
            .await;
        Ok(())
    }

    pub(crate) async fn admit_script_monitor_turn(
        &self,
        agent: &AgentId,
        metadata: Option<&Value>,
    ) -> Result<bool> {
        if monitor_id(metadata).is_none() {
            return Ok(true);
        }
        let _lane = self.script_locks.monitor_lane.lock().await;
        if !self
            .script_monitor_delivery_allowed(agent, metadata)
            .await?
        {
            return Ok(false);
        }
        self.store
            .accept_script_notification(agent, monitor_id(metadata).expect("monitor checked"))
            .await
    }

    pub(crate) async fn script_monitor_delivery_allowed(
        &self,
        agent: &AgentId,
        metadata: Option<&Value>,
    ) -> Result<bool> {
        let Some(id) = monitor_id(metadata) else {
            return Ok(true);
        };
        if self
            .script_locks
            .monitor_completions
            .lock()
            .expect("monitor completion retries")
            .contains(&(agent.clone(), id.to_owned()))
        {
            return Ok(false);
        }
        let owner = match self.store.get_agent_session_summary(agent).await {
            Ok(owner) => owner,
            Err(intent_core::Error::NotFound(_)) => return Ok(false),
            Err(error) => return Err(error),
        };
        if metadata.and_then(|md| md["workspaceId"].as_str()) != Some(owner.workspace_id.as_str())
            || metadata
                .and_then(|md| md["agentId"].as_str())
                .is_some_and(|id| id != agent.as_str())
            || self
                .store
                .get_agent_session_retired_at(agent)
                .await?
                .is_some()
            || (!owner.workspace_id.is_chief()
                && match self.store.get_workspace(&owner.workspace_id).await {
                    Ok(workspace) => workspace.archived,
                    Err(intent_core::Error::NotFound(_)) => true,
                    Err(error) => return Err(error),
                })
        {
            return Ok(false);
        }
        self.store
            .script_monitor_wake_allowed_for_agent(id, agent, &owner.workspace_id)
            .await
    }

    pub(crate) async fn cancel_script_monitors(
        &self,
        ws: &WorkspaceId,
        agent: Option<&AgentId>,
        reason: &str,
    ) -> Result<()> {
        let owners = self
            .script_manager()
            .cleanup_monitors(ws, agent, reason)
            .await?;
        {
            let mut queues = self.agent_queues.lock().expect("queue registry");
            for (id, queue) in queues.iter_mut() {
                if agent.is_some_and(|a| a != id) {
                    continue;
                }
                queue.retain(|entry| {
                    entry.message_metadata.as_ref().is_none_or(|md| {
                        md["type"] != "script_monitor_wake"
                            || md["workspaceId"].as_str() != Some(ws.as_str())
                    })
                });
            }
        }
        for agent in owners {
            self.redeliver_completion_after_queue_mutation(&agent).await;
        }
        Ok(())
    }
}
