//! A tenant's repository context, file watchers and `config.toml` live
//! reload, as `intentd serve` starts them for its one backend.
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
use intent_services::{
    ConfigWatcher, EventBus, GitStatusRefresher, Services, WatchHealth, WatcherRegistry,
};

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
/// watchers with the git-status refresher, and its `config.toml` live
/// reload, and register their stop on `runner`. Nothing starts once
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
            let mut config = start_config_reload(&hub, &services);
            let _ = stopped.await;
            tokio::join!(
                async {
                    if let Some(config) = config.as_mut() {
                        config.shutdown().await;
                    }
                },
                registry.shutdown(),
            );
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

/// Watch the tenant's `config.toml` over `hub`: a valid edit is applied as
/// `settings.update` applies it; an invalid one keeps the last good values.
fn start_config_reload(
    hub: &Arc<intent_services::SharedWatchHub>,
    services: &Services,
) -> Option<ConfigWatcher> {
    let registry = services.settings_registry()?;
    let reload = services.clone();
    match ConfigWatcher::start_prepared_reload(
        hub,
        registry,
        services.settings_revision_gate(),
        move |text, expected, admission| {
            let services = reload.clone();
            async move {
                services
                    .apply_prepared_settings_reload(text, expected, admission)
                    .await
            }
        },
    ) {
        Ok(watcher) => Some(watcher),
        Err(error) => {
            tracing::warn!(
                %error,
                "config.toml live reload did not start; edits apply when the tenant restarts"
            );
            None
        }
    }
}

/// The tenant's workspaces as head's watchers see them: a workspace homed on
/// a machine is left out of the list and reads as remote, so the registry
/// does not watch head's copy and the git-status refresher does not scan it,
/// whatever triggers a refresh (a relayed home file event, a head watch).
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
