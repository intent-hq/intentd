//! A home's state and backup warning, orchestrator side (monorepo
//! `docs/protocol/methods/nodes.md`, "Detached workspaces" and "Continuous
//! backup"): a home whose machine's link stays down for the link-loss grace
//! is detached, and attached again at the machine's next bind; a failing
//! backup streak raises `backupWarning` 15 minutes in. Each change is
//! announced as `workspace:updated { changes: { home } }`.

use std::collections::HashMap;
use std::sync::{Mutex, Weak};
use std::time::Duration;

use intent_core::machines::ExposedMachine;
use intent_core::{HomeId, HomeState, WorkspaceHome, WorkspaceId};
use intent_services::feature_api::Services;
use tokio::task::JoinHandle;

use crate::connector::Shared;
use crate::RemoteServicesExt as _;

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// One machine's detach grace and the backup warnings of its homes.
#[derive(Default)]
pub(crate) struct HomeLink {
    grace: Mutex<Option<JoinHandle<()>>>,
    warnings: Mutex<HashMap<HomeId, JoinHandle<()>>>,
}

impl Drop for HomeLink {
    fn drop(&mut self) {
        if let Some(grace) = lock(&self.grace).take() {
            grace.abort();
        }
        for (_, warning) in lock(&self.warnings).drain() {
            warning.abort();
        }
    }
}

impl HomeLink {
    /// The machine's link is down (or not up yet at start): its homes are
    /// detached once `grace` passes without a bind.
    pub(crate) fn link_lost(&self, shared: Weak<Shared>, grace: Duration) {
        let lost_at = intent_core::now_iso();
        let mut slot = lock(&self.grace);
        if let Some(previous) = slot.take() {
            previous.abort();
        }
        // caller-binding: allow — detaches one machine's homes after the link-loss grace
        *slot = Some(tokio::spawn(async move {
            tokio::time::sleep(grace).await;
            let Some(shared) = shared.upgrade() else {
                return;
            };
            if shared.current_link().is_some() {
                return;
            }
            detach_homes(&shared, &lost_at).await;
        }));
    }

    /// A link bound: no home of the machine is detached for the loss.
    pub(crate) fn bound(&self) {
        if let Some(grace) = lock(&self.grace).take() {
            grace.abort();
        }
    }

    /// A backup of `home_id` arrived: its streak is over.
    pub(crate) fn backed_up(&self, home_id: &HomeId) {
        if let Some(warning) = lock(&self.warnings).remove(home_id) {
            warning.abort();
        }
    }

    /// Announce the home once its failing streak, begun at
    /// `failing_since`, raises `backupWarning`. One wait per home.
    fn watch_warning(
        &self,
        svc: &Services,
        workspace_id: &WorkspaceId,
        home_id: &HomeId,
        failing_since: String,
    ) {
        let mut warnings = lock(&self.warnings);
        if warnings
            .get(home_id)
            .is_some_and(|waiting| !waiting.is_finished())
        {
            return;
        }
        let svc = svc.clone();
        let workspace_id = workspace_id.clone();
        let watched = home_id.clone();
        // caller-binding: allow — announces one home's backup warning when its failing streak reaches 15 minutes
        let wait = tokio::spawn(async move {
            let Some(left) =
                intent_store::backup_warning_in(&failing_since, time::OffsetDateTime::now_utc())
            else {
                return;
            };
            tokio::time::sleep(left.unsigned_abs()).await;
            let warned = svc
                .store()
                .get_workspace_home(&workspace_id)
                .await
                .ok()
                .flatten()
                .is_some_and(|home| home.home_id == watched && home.backup_warning);
            if warned {
                svc.publish_workspace_home_changed(&workspace_id).await;
            }
        });
        warnings.insert(home_id.clone(), wait);
    }
}

/// Whether a home on `row` is detached when its link stays down. A cloud
/// home is resumed on demand and never detached: cloud homes decide how
/// one becomes unavailable (a sandbox deleted or failed to resume).
fn may_detach(row: Option<&ExposedMachine>) -> bool {
    row.is_none_or(|row| !row.is_cloud())
}

/// Detach each attached home of `shared`'s machine; its backups have been
/// failing since the link was lost at `lost_at`.
async fn detach_homes(shared: &std::sync::Arc<Shared>, lost_at: &str) {
    let Some(svc) = shared.services() else {
        return;
    };
    if !svc.workspace_homes() {
        return;
    }
    let machine = shared.machine_id();
    let row = svc
        .store()
        .get_exposed_machine(&machine)
        .await
        .ok()
        .flatten();
    if !may_detach(row.as_ref()) {
        return;
    }
    let homes = match svc.store().homes_on_machine(&machine).await {
        Ok(homes) => homes,
        Err(error) => {
            tracing::warn!(%machine, %error, "remote machine: could not read the homes to detach");
            return;
        }
    };
    for (workspace_id, home) in homes {
        let detached = svc
            .store()
            .move_workspace_home_state(
                &workspace_id,
                &home.home_id,
                HomeState::Attached,
                HomeState::Detached,
                &intent_core::now_iso(),
            )
            .await;
        match detached {
            Ok(true) => {
                tracing::info!(workspace = %workspace_id, %machine, "remote machine: the workspace's home is detached");
                record_failure(svc, shared, &workspace_id, &home.home_id, lost_at).await;
                svc.publish_workspace_home_changed(&workspace_id).await;
            }
            Ok(false) => {}
            Err(error) => {
                tracing::warn!(workspace = %workspace_id, %error, "remote machine: could not detach the workspace's home");
            }
        }
    }
}

/// After a bind: each detached home of the machine is attached again.
pub(crate) async fn attach_homes(svc: &Services, homes: &[(WorkspaceId, WorkspaceHome)]) {
    for (workspace_id, home) in homes {
        match svc
            .store()
            .move_workspace_home_state(
                workspace_id,
                &home.home_id,
                HomeState::Detached,
                HomeState::Attached,
                &intent_core::now_iso(),
            )
            .await
        {
            Ok(true) => {
                tracing::info!(workspace = %workspace_id, "remote machine: the workspace's home is attached again");
                svc.publish_workspace_home_changed(workspace_id).await;
            }
            Ok(false) => {}
            Err(error) => {
                tracing::warn!(workspace = %workspace_id, %error, "remote machine: could not attach the workspace's home");
            }
        }
    }
}

/// Record a failed backup of `home_id` at `at`, and wait for its streak to
/// raise `backupWarning`.
async fn record_failure(
    svc: &Services,
    shared: &Shared,
    workspace_id: &WorkspaceId,
    home_id: &HomeId,
    at: &str,
) {
    match svc
        .store()
        .record_workspace_home_backup_failure(workspace_id, home_id, at)
        .await
    {
        Ok(Some(since)) => shared
            .home_link
            .watch_warning(svc, workspace_id, home_id, since),
        Ok(None) => {}
        Err(error) => {
            tracing::warn!(workspace = %workspace_id, %error, "remote machine: could not record a failed home backup");
        }
    }
}

/// A backup the orchestrator asked `home` for failed.
pub(crate) async fn backup_failed(
    svc: &Services,
    workspace_id: &WorkspaceId,
    home: &WorkspaceHome,
) {
    let Some(node_id) = home.node_id.as_deref() else {
        return;
    };
    let Some(feature) = svc.remote_feature() else {
        return;
    };
    let Ok(node) = feature.node(svc, node_id).await else {
        return;
    };
    record_failure(
        svc,
        node.shared_ref(),
        workspace_id,
        &home.home_id,
        &intent_core::now_iso(),
    )
    .await;
}
