//! Settlement runs after the process owner releases its supervisor/run lease.
//! The finalizer may take the definition lock; a supervisor must only enqueue it,
//! never await it (stop/restart/remove join supervisors while holding that lock).
use super::{
    json, now_iso, publish_event, script_event, Result, ScriptManager, WorkspaceId,
    EXIT_CODE_UNOBSERVABLE, LOST_AT_DAEMON_STOP_ERROR, SCRIPT_CHANGED,
};
use intent_core::{ScriptLastRun, ScriptPurpose, ScriptRunOutcome};

impl ScriptManager {
    pub(super) fn record_result(
        &self,
        ws: &WorkspaceId,
        id: &str,
        generation: u64,
        failure: bool,
        timeout: bool,
    ) {
        let mut scripts = self.scripts.lock().unwrap();
        let Some(m) = scripts
            .get_mut(&(ws.clone(), id.to_owned()))
            .filter(|m| m.generation == generation && m.run_id.is_some())
        else {
            return;
        };
        let cancelled = m.cancellation.is_some() || timeout;
        let interrupted = m.running_at_shutdown
            || (!failure && m.state.exit_code == Some(EXIT_CODE_UNOBSERVABLE));
        let outcome = if m.running_at_shutdown {
            ScriptRunOutcome::Interrupted
        } else if cancelled {
            ScriptRunOutcome::Cancelled
        } else if interrupted {
            ScriptRunOutcome::Interrupted
        } else if failure || m.state.exit_code != Some(0) {
            ScriptRunOutcome::Failed
        } else {
            ScriptRunOutcome::Succeeded
        };
        m.pending_result = Some(ScriptLastRun {
            outcome,
            exit_code: if cancelled && m.state.started_at.is_none() {
                None
            } else {
                m.state.exit_code
            },
            started_at: m.state.started_at.clone(),
            stopped_at: m.state.stopped_at.clone().unwrap_or_else(now_iso),
            error: if m.running_at_shutdown {
                Some(LOST_AT_DAEMON_STOP_ERROR.into())
            } else if timeout {
                Some("command timed out".into())
            } else if cancelled {
                m.cancellation.map(str::to_owned)
            } else {
                m.state.error.clone()
            },
        });
    }

    pub(super) fn queue_settlement(
        &self,
        ws: &WorkspaceId,
        id: &str,
        generation: u64,
    ) -> tokio::task::JoinHandle<()> {
        let mgr = self.clone();
        let ws = ws.clone();
        let id = id.to_owned();
        intent_core::spawn_daemon(async move {
            let lock = mgr.locks.definition_lock(&id);
            let _guard = lock.lock().await;
            mgr.finish_run_locked(&ws, &id, generation).await;
        })
    }

    /// Caller owns the definition lock. Join only the process owner, which never
    /// awaits this finalizer. A generation mismatch means another operation won.
    pub(super) async fn finish_run_locked(&self, ws: &WorkspaceId, id: &str, generation: u64) {
        let key = (ws.clone(), id.to_owned());
        let handle = {
            let mut scripts = self.scripts.lock().unwrap();
            let Some(m) = scripts
                .get_mut(&key)
                .filter(|m| m.generation == generation && m.pending_result.is_some())
            else {
                return;
            };
            m.supervisor.take()
        };
        if let Some(handle) = handle {
            let _ = handle.await;
        }
        loop {
            let done = self
                .scripts
                .lock()
                .unwrap()
                .get(&key)
                .filter(|m| m.generation == generation)
                .and_then(|m| m.run_reserved.clone());
            let Some(done) = done else {
                break;
            };
            let notified = done.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self
                .scripts
                .lock()
                .unwrap()
                .get(&key)
                .is_none_or(|m| m.generation != generation || m.run_reserved.is_none())
            {
                break;
            }
            notified.await;
        }
        if let Some(park) = &self.parks.settlement_ready {
            park.entered.notify_one();
            park.release.notified().await;
        }
        let pending = {
            let scripts = self.scripts.lock().unwrap();
            scripts
                .get(&key)
                .filter(|m| m.generation == generation && !m.running_at_shutdown)
                .and_then(|m| Some((m.run_id.clone()?, m.pending_result.clone()?)))
        };
        let Some((token, result)) = pending else {
            return;
        };
        match self
            .store
            .settle_script_run(ws, id, &token, &result, false)
            .await
        {
            Ok(true) => {
                if let Some(park) = &self.parks.settlement_committed {
                    park.entered.notify_one();
                    park.release.notified().await;
                }
                let mut scripts = self.scripts.lock().unwrap();
                if let Some(m) = scripts.get_mut(&key).filter(|m| m.generation == generation) {
                    if m.def.purpose == ScriptPurpose::OneOff {
                        m.def
                            .archived_at
                            .get_or_insert_with(|| result.stopped_at.clone());
                    }
                    m.def.last_run = Some(result);
                    m.run_id = None;
                    m.pending_result = None;
                }
            }
            Ok(false) => return,
            Err(error) => {
                tracing::warn!(script = %id, %error, "script result persistence failed; leaving active");
                return;
            }
        }
        publish_event(
            self.bus.as_ref(),
            script_event(
                ws,
                SCRIPT_CHANGED,
                json!({"scriptId":id,"action":"updated"}),
            ),
        )
        .await;
    }

    pub(super) async fn finish_previous_locked(&self, ws: &WorkspaceId, id: &str) {
        let generation = self
            .scripts
            .lock()
            .unwrap()
            .get(&(ws.clone(), id.to_owned()))
            .map(|m| m.generation);
        if let Some(generation) = generation {
            self.finish_run_locked(ws, id, generation).await;
        }
    }

    pub(super) async fn recover_commands(&self) -> Result<()> {
        for (ws, id, token, started_at) in self.store.pending_script_runs().await? {
            let lock = self.locks.definition_lock(&id);
            let _guard = lock.lock().await;
            // Hydration is repeatable within a live daemon; never recover its
            // own launch or an entry already reconciled into this registry.
            if self
                .scripts
                .lock()
                .unwrap()
                .contains_key(&(ws.clone(), id.clone()))
            {
                continue;
            }
            let result = ScriptLastRun {
                outcome: ScriptRunOutcome::Interrupted,
                exit_code: Some(EXIT_CODE_UNOBSERVABLE),
                started_at,
                stopped_at: now_iso(),
                error: Some(LOST_AT_DAEMON_STOP_ERROR.into()),
            };
            let recovered = match self
                .store
                .settle_script_run(&ws, &id, &token, &result, true)
                .await
            {
                Ok(recovered) => recovered,
                Err(error) => {
                    tracing::warn!(script = %id, %error, "script recovery persistence failed; leaving active");
                    false
                }
            };
            if recovered {
                publish_event(
                    self.bus.as_ref(),
                    script_event(
                        &ws,
                        SCRIPT_CHANGED,
                        json!({"scriptId":id,"action":"updated"}),
                    ),
                )
                .await;
            }
        }
        Ok(())
    }
}
