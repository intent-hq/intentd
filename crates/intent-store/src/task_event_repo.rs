//! Pre-write baselines for changed-only task projection events.
use crate::Store;
use intent_core::{TaskListResult, WorkspaceId};
use std::{
    collections::HashMap,
    sync::{atomic::Ordering, Arc, Mutex},
};

type Baseline = Arc<tokio::sync::Mutex<Option<TaskListResult>>>;

/// The event bus holds a workspace's lock through projection and publication.
/// Independent workspaces never block each other's note mutations. Baselines
/// are process-local; reconnects hydrate from the authoritative store.
#[derive(Default)]
pub struct TaskEventBaselines {
    workspaces: Mutex<HashMap<WorkspaceId, Baseline>>,
}

impl TaskEventBaselines {
    /// Serialize projection publication for one workspace.
    ///
    /// # Panics
    /// Panics if the baseline registry mutex was poisoned.
    pub async fn lock(
        &self,
        workspace_id: &WorkspaceId,
    ) -> tokio::sync::OwnedMutexGuard<Option<TaskListResult>> {
        let baseline = self
            .workspaces
            .lock()
            .expect("task baseline registry")
            .entry(workspace_id.clone())
            .or_default()
            .clone();
        baseline.lock_owned().await
    }

    /// Release the baseline when a workspace is deleted.
    ///
    /// # Panics
    /// Panics if the baseline registry mutex was poisoned.
    pub fn remove(&self, workspace_id: &WorkspaceId) {
        self.workspaces
            .lock()
            .expect("task baseline registry")
            .remove(workspace_id);
    }
}

impl Store {
    /// Enable task projection events and obtain their shared publication baselines.
    #[must_use]
    pub fn task_event_baselines(&self) -> Arc<TaskEventBaselines> {
        self.task_events_enabled.store(true, Ordering::Release);
        self.task_event_baselines.clone()
    }

    /// Capture the first baseline BEFORE a mutation, never on an initial read.
    /// This avoids falsely reporting a statistics change on the first ordinary
    /// note edit after restart. Once primed, writes perform no additional query.
    pub(crate) async fn prepare_task_events(&self, workspace_id: &WorkspaceId) {
        if !self.task_events_enabled.load(Ordering::Acquire) {
            return;
        }
        let mut baseline = self.task_event_baselines.lock(workspace_id).await;
        if baseline.is_some() {
            return;
        }
        match Box::pin(self.list_workspace_tasks(workspace_id, None)).await {
            Ok(snapshot) => {
                *baseline = Some(snapshot);
            }
            Err(error) => tracing::warn!(%workspace_id, %error, "task event baseline unavailable"),
        }
    }
}
