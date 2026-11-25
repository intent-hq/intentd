//! A tenant's repository context and file watchers, as `intentd serve`
//! starts them for its one backend. Its settings live in its Postgres row
//! and are followed there (`crate::settings`), so no `config.toml` is
//! watched.
//!
//! The watchers see the checkouts on head. A workspace whose home is on a
//! machine is watched there (the machine's `HomeWatches`): its file and
//! git-status events arrive as `homeEvents` and are published for the
//! workspace (`Services::publish_home_events`). Head neither watches nor
//! scans its own copy of such a workspace ([`HeadCheckouts`]): a scan of
//! that stale copy would overwrite the home's status.

use std::sync::Arc;

use intent_app::tenant::Admission;
use intent_core::tenant_fence::SelfFence;
use intent_core::{BoxFuture, Result, Workspace, WorkspaceApi, WorkspaceId, WorkspaceListFilter};
use intent_services::{EventBus, GitStatusRefresher, Services, WatchHealth, WatcherRegistry};

/// Restore the tenant's GitLab repository binding from its settings and
/// secrets, before its work starts (`intentd` does it at start). Without a
/// binding the GitLab repository features stay unavailable; the tenant
/// still serves.
pub(crate) async fn restore_repository_binding(services: &Services) {
    if let Err(error) = services.initialize_gitlab_repository_binding().await {
        tracing::debug!(%error, "repository GitLab binding remains unavailable at startup");
    }
}

/// Start the tenant's repository metadata prewarm, its file and `.git`
/// watchers with the git-status refresher, and register their stop on
/// `runner`. Nothing starts once
/// `admission` refuses.
pub(crate) fn start(
    services: &Services,
    bus: &EventBus,
    admission: &impl Admission,
    runner: &mut SelfFence,
) {
    if !admission.allowed() {
        return;
    }
    let prewarm = services.clone();
    runner.adopt_task(intent_core::spawn_daemon(async move {
        prewarm.prewarm_repository_metadata().await;
    }));

    let api: Arc<dyn WorkspaceApi> = Arc::new(HeadCheckouts(services.clone()));
    let git_status_refresher = Arc::new(GitStatusRefresher::start_with_presence(
        bus.clone(),
        api.clone(),
        services.git_status_cache(),
        services.worktree_presence(),
    ));
    let hub = intent_services::SharedWatchHub::new();
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let watchers = {
        let services = services.clone();
        let bus = bus.clone();
        let refresher = git_status_refresher.clone();
        intent_core::spawn_daemon(async move {
            // Each OS registration can block its thread (macOS `fseventsd`):
            // keep it off the workers serving the tenant, as `intentd` does.
            let registry = tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(WatcherRegistry::start_with_health(
                    &hub,
                    bus,
                    api,
                    refresher,
                    &WatchHealth::default(),
                    services.worktree_presence(),
                ))
            });
            let _ = stopped.await;
            registry.shutdown().await;
        })
    };
    runner.on_tasks(move || async move {
        let _ = stop.send(());
        let abort = watchers.abort_handle();
        let joined = tokio::time::timeout(STOP_BUDGET, async {
            tokio::join!(
                async {
                    let _ = watchers.await;
                },
                git_status_refresher.shutdown(),
            );
        })
        .await;
        if joined.is_err() {
            // An overrun fence step ends the process: cut the watchers short
            // instead (dropping them aborts their tasks).
            abort.abort();
            tracing::warn!("the tenant's watchers did not stop in time; aborted");
        }
    });
}

/// How long the watchers may take to stop: under the fence's step budget
/// (`intent_core::tenant_fence::SELF_FENCE_STEP_BUDGET`, one second).
const STOP_BUDGET: std::time::Duration = std::time::Duration::from_millis(750);

/// The tenant's workspaces as head's watchers see them: a workspace homed on
/// a machine is left out of the list, is not watched here when it is
/// created, opened or unarchived (its watches stop when its home moves to a
/// machine), and reads as remote, so the git-status refresher does not scan
/// head's copy whatever triggers a refresh.
struct HeadCheckouts(Services);

impl HeadCheckouts {
    async fn homed_on_machine(&self, id: &WorkspaceId) -> Result<bool> {
        Ok(self
            .0
            .store()
            .get_workspace_home(id)
            .await?
            .is_some_and(|home| on_machine(&home)))
    }
}

fn on_machine(home: &intent_core::WorkspaceHome) -> bool {
    home.node_id.is_some() || home.cloud_machine_id.is_some()
}

impl WorkspaceApi for HeadCheckouts {
    fn get_workspace(&self, id: WorkspaceId) -> BoxFuture<'_, Result<Workspace>> {
        Box::pin(async move {
            let mut workspace = WorkspaceApi::get_workspace(&self.0, id.clone()).await?;
            if self.homed_on_machine(&id).await? {
                workspace.is_remote = true;
            }
            Ok(workspace)
        })
    }

    fn watches_checkout_here(&self, id: WorkspaceId) -> BoxFuture<'_, bool> {
        Box::pin(async move {
            match self.homed_on_machine(&id).await {
                Ok(homed) => !homed,
                Err(error) => {
                    tracing::warn!(workspace = %id, %error, "workspace home read failed; not watching its checkout");
                    false
                }
            }
        })
    }

    fn list_workspaces(
        &self,
        filter: WorkspaceListFilter,
    ) -> BoxFuture<'_, Result<Vec<Workspace>>> {
        Box::pin(async move {
            let homes = self.0.store().list_workspace_homes().await?;
            let workspaces = WorkspaceApi::list_workspaces(&self.0, filter).await?;
            Ok(workspaces
                .into_iter()
                .filter(|workspace| !homes.get(&workspace.id).is_some_and(on_machine))
                .collect())
        })
    }
}
