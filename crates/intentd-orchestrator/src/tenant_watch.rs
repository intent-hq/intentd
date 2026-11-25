//! A tenant's repository context, file watchers and `config.toml` live
//! reload, as `intentd serve` starts them for its one backend.
//!
//! The watchers see the checkouts on head: workspaces over a repository
//! here, and head's copies. A workspace whose home is on a machine is
//! watched there (the machine's `HomeWatches`): its file and git-status
//! events arrive as `homeEvents` and are published for the workspace
//! (`Services::publish_home_events`), so head does not watch those files.

use std::sync::Arc;

use intent_app::tenant::Admission;
use intent_core::tenant_fence::SelfFence;
use intent_core::WorkspaceApi;
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

    let api: Arc<dyn WorkspaceApi> = Arc::new(services.clone());
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
            if let Some(config) = config.as_mut() {
                config.shutdown().await;
            }
            registry.shutdown().await;
        })
    };
    runner.on_tasks(move || async move {
        let _ = stop.send(());
        let _ = watchers.await;
        git_status_refresher.shutdown().await;
    });
}

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
