//! Durable definition changes share admission locks with start/run/restart.
use super::{
    json, now_iso, publish_event, script_event, Error, HashSet, ManagedScript, Result,
    ScriptManager, ScriptMode, ScriptStatus, Value, WorkspaceId, SCRIPT_CHANGED,
};

impl ScriptManager {
    pub(super) fn is_live(&self, ws: &WorkspaceId, id: &str) -> Result<bool> {
        let scripts = self.scripts.lock().unwrap();
        let m = scripts
            .get(&(ws.clone(), id.to_owned()))
            .ok_or_else(|| Error::NotFound(format!("script {id}")))?;
        Ok(self.entry_is_live(m))
    }

    fn entry_is_live(&self, m: &ManagedScript) -> bool {
        !matches!(m.state.status, ScriptStatus::Idle | ScriptStatus::Exited)
            || m.run_reserved.is_some()
            || m.supervisor
                .as_ref()
                .is_some_and(|task| !task.is_finished())
            || m.pty_id.is_some_and(|id| self.pty.is_alive(id))
    }

    /// Caller holds the definition lock through launch admission. A failed
    /// durable restore refuses the launch; the invalidation precedes live state.
    pub(super) async fn prepare_launch(&self, ws: &WorkspaceId, id: &str) -> Result<()> {
        self.prepare_admission(ws, id, false).await
    }

    pub(super) async fn prepare_restart(&self, ws: &WorkspaceId, id: &str) -> Result<()> {
        self.prepare_admission(ws, id, true).await
    }

    async fn prepare_admission(&self, ws: &WorkspaceId, id: &str, restart: bool) -> Result<()> {
        self.set_archive(ws, id, None).await?;
        let command = self
            .scripts
            .lock()
            .unwrap()
            .get(&(ws.clone(), id.to_owned()))
            .is_some_and(|m| m.def.mode == ScriptMode::Command);
        if command {
            let token = uuid::Uuid::new_v4().to_string();
            self.store.admit_script_run(ws, id, &token).await?;
            let mut scripts = self.scripts.lock().unwrap();
            let m = scripts.get_mut(&(ws.clone(), id.to_owned())).unwrap();
            m.run_id = Some(token);
            m.run_generation = (!restart).then_some(m.generation);
            m.pending_result = None;
        }
        Ok(())
    }

    /// Persist first, then change the registry. Never replace the runtime/PTY.
    async fn set_archive(
        &self,
        ws: &WorkspaceId,
        id: &str,
        timestamp: Option<String>,
    ) -> Result<()> {
        let old = self
            .scripts
            .lock()
            .unwrap()
            .get(&(ws.clone(), id.to_owned()))
            .ok_or_else(|| Error::NotFound(format!("script {id}")))?
            .def
            .archived_at
            .clone();
        if old == timestamp {
            return Ok(());
        }
        if timestamp.is_some() {
            if let Some(park) = &self.parks.archive_persist {
                park.entered.notify_one();
                park.release.notified().await;
            }
        }
        self.store
            .set_script_archived_at(ws, id, timestamp.as_deref())
            .await?;
        if let Some(park) = &self.parks.archive_committed {
            park.entered.notify_one();
            park.release.notified().await;
        }
        self.scripts
            .lock()
            .unwrap()
            .get_mut(&(ws.clone(), id.to_owned()))
            .ok_or_else(|| Error::NotFound(format!("script {id}")))?
            .def
            .archived_at = timestamp;
        publish_event(
            self.bus.as_ref(),
            script_event(
                ws,
                SCRIPT_CHANGED,
                json!({"scriptId": id, "action": "updated"}),
            ),
        )
        .await;
        Ok(())
    }

    pub(crate) async fn archive(
        &self,
        ws: &WorkspaceId,
        ids: Vec<String>,
        archive: bool,
    ) -> Result<Value> {
        if ids.is_empty() || ids.len() > 1000 || ids.iter().any(|id| id.trim().is_empty()) {
            return Err(Error::InvalidParams(
                "scriptIds must contain 1–1000 nonempty IDs".into(),
            ));
        }
        let mgr = self.clone();
        let ws = ws.clone();
        intent_core::spawn_daemon(async move { mgr.archive_owned(&ws, ids, archive).await })
            .await
            .map_err(|e| Error::Internal(format!("archive task failed: {e}")))?
    }

    async fn archive_owned(
        &self,
        ws: &WorkspaceId,
        ids: Vec<String>,
        archive: bool,
    ) -> Result<Value> {
        let mut seen = HashSet::new();
        let mut succeeded = Vec::new();
        let mut skipped = Vec::new();
        for id in ids.into_iter().filter(|id| seen.insert(id.clone())) {
            let lock = self.locks.definition_lock(&id);
            let _guard = lock.lock().await;
            // Store scope is checked too: legacy owner operations can move an
            // ID between workspaces while retaining an older runtime entry.
            let durable = self.store.get_script_in_workspace(ws, &id).await?;
            let (reason, timestamp) = {
                let scripts = self.scripts.lock().unwrap();
                match scripts
                    .get(&(ws.clone(), id.clone()))
                    .filter(|_| durable.is_some())
                {
                    None => (Some("notFound"), None),
                    Some(m) if archive && m.def.mode == ScriptMode::Service => {
                        (Some("service"), None)
                    }
                    Some(m) if archive && self.entry_is_live(m) => (Some("live"), None),
                    Some(m) => (
                        None,
                        if archive {
                            Some(m.def.archived_at.clone().unwrap_or_else(now_iso))
                        } else {
                            None
                        },
                    ),
                }
            };
            if let Some(reason) = reason {
                skipped.push(json!({"scriptId": id, "reason": reason}));
                continue;
            }
            self.set_archive(ws, &id, timestamp).await?;
            succeeded.push(id);
        }
        Ok(if archive {
            json!({"archived": succeeded, "skipped": skipped})
        } else {
            json!({"restored": succeeded, "skipped": skipped})
        })
    }
}

/// Covers script.run's detached completion, including final output/event work.
pub(super) struct RunLease {
    pub(super) mgr: ScriptManager,
    pub(super) key: (WorkspaceId, String),
    pub(super) generation: u64,
}

impl Drop for RunLease {
    fn drop(&mut self) {
        if let Ok(mut scripts) = self.mgr.scripts.lock() {
            if let Some(m) = scripts
                .get_mut(&self.key)
                .filter(|m| m.generation == self.generation)
            {
                if let Some(done) = m.run_reserved.take() {
                    done.notify_waiters();
                }
            }
        }
    }
}
