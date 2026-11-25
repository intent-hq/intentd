//! Serving one tenant: open it under its lease, build its `Services`, and
//! start its work under its self-fence (see the crate docs for the order).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use intent_app::tenant::{Admission, TenantRuntime};
use intent_app::StartupExt as _;
use intent_core::tenant_fence::{SelfFenceReport, TenantFence};
use intent_core::{Error, Result};
use intent_services::retention::TenantSweepTiming;
use intent_services::{
    AgentManager, EventBus, Services, SettingsRegistry, StartupCompletionRecovery,
};
use intent_store::postgres::{
    LeaseHeartbeat, LeaseHeld, PgPool, PostgresOptions, SecretKeyring, TenantId,
    LEASE_RENEW_INTERVAL, LEASE_TTL,
};
use intent_store::{OrchestratorOnly, Store};
use intent_transport::PrimaryReverseRegistry;

/// How a tenant's lease is held: claimed for `ttl`, renewed every
/// `interval`.
#[derive(Debug, Clone, Copy)]
pub struct LeaseTiming {
    pub ttl: Duration,
    pub interval: Duration,
}

impl Default for LeaseTiming {
    fn default() -> Self {
        Self {
            ttl: LEASE_TTL,
            interval: LEASE_RENEW_INTERVAL,
        }
    }
}

/// One tenant this process serves.
pub struct ServedTenant {
    tenant: String,
    dir: PathBuf,
    services: Services,
    secrets: Arc<crate::secrets::TenantSecrets>,
    bus: EventBus,
    runtime: TenantRuntime,
    /// The tenant's reverse registry: installed in `services` before any
    /// clone of it exists, and shared by its client listeners, so agent
    /// browser actions reach the desktops those listeners connect.
    reverse: Arc<PrimaryReverseRegistry>,
    fence: TenantFence,
    /// Renews the lease; dropping it fires the fence.
    heartbeat: LeaseHeartbeat,
    /// The tenant's agent manager: `services` holds only a weak handle, so
    /// this one keeps every `agent.*` turn running while the tenant serves.
    agents: Arc<AgentManager>,
    report: Option<tokio::task::JoinHandle<SelfFenceReport>>,
}

/// The tenant's work is admitted until its fence fires.
struct Unfenced(TenantFence);

impl Admission for Unfenced {
    fn allowed(&self) -> bool {
        !self.0.is_fenced()
    }
}

/// Open tenant `tenant` on `pool` as `instance` and start serving it, with
/// its files under [`crate::tenant_dir`] of `data_dir` and its secrets in
/// its Postgres rows, sealed with `keyring` ([`crate::secrets`]). Call it
/// on the multi-thread runtime: the secret store needs it.
///
/// # Errors
/// For an invalid tenant id; `Error::SchemaMismatch` when the schema does
/// not match this build; `Error::NotOwner` when another instance holds a
/// live lease (nothing was written); when head's git receive hooks, the
/// tenant's directory or its settings cannot be set up; when the runtime is
/// not multi-thread.
pub async fn serve_tenant(
    pool: PgPool,
    tenant: &str,
    data_dir: &Path,
    instance: &str,
    timing: LeaseTiming,
    keyring: Arc<SecretKeyring>,
    orchestrator: OrchestratorOnly,
) -> Result<ServedTenant> {
    serve_tenant_with(
        pool,
        tenant,
        data_dir,
        instance,
        timing,
        TenantSweepTiming::default(),
        keyring,
        orchestrator,
    )
    .await
}

/// [`serve_tenant`], with the tenant's store sweeps (event retention and
/// the idempotency reaper) on `sweeps`' timing.
///
/// # Errors
/// As [`serve_tenant`].
#[expect(clippy::too_many_arguments)]
pub async fn serve_tenant_with(
    pool: PgPool,
    tenant: &str,
    data_dir: &Path,
    instance: &str,
    timing: LeaseTiming,
    sweeps: TenantSweepTiming,
    keyring: Arc<SecretKeyring>,
    orchestrator: OrchestratorOnly,
) -> Result<ServedTenant> {
    match try_serve_tenant_with(
        pool,
        tenant,
        data_dir,
        instance,
        timing,
        sweeps,
        keyring,
        orchestrator,
    )
    .await?
    {
        Serving::Served(served) => Ok(*served),
        Serving::Held(held) => Err(held.into_error()),
    }
}

/// What [`try_serve_tenant`] found.
pub enum Serving {
    /// This process serves the tenant (boxed: it is large).
    Served(Box<ServedTenant>),
    /// Another instance holds a live lease on the tenant: nothing was
    /// claimed, written or started.
    Held(LeaseHeld),
}

/// [`serve_tenant`], with another instance's live lease as
/// [`Serving::Held`] instead of `Error::NotOwner`, so a caller can tell
/// that refusal from a failure after the claim.
///
/// # Errors
/// As [`serve_tenant`], except a live lease.
pub async fn try_serve_tenant(
    pool: PgPool,
    tenant: &str,
    data_dir: &Path,
    instance: &str,
    timing: LeaseTiming,
    keyring: Arc<SecretKeyring>,
    orchestrator: OrchestratorOnly,
) -> Result<Serving> {
    try_serve_tenant_with(
        pool,
        tenant,
        data_dir,
        instance,
        timing,
        TenantSweepTiming::default(),
        keyring,
        orchestrator,
    )
    .await
}

#[expect(clippy::too_many_arguments)]
async fn try_serve_tenant_with(
    pool: PgPool,
    tenant: &str,
    data_dir: &Path,
    instance: &str,
    timing: LeaseTiming,
    sweeps: TenantSweepTiming,
    keyring: Arc<SecretKeyring>,
    orchestrator: OrchestratorOnly,
) -> Result<Serving> {
    let id = TenantId::new(tenant)?;
    // The tenant's secret store blocks on the multi-thread runtime; refuse
    // any other before the lease is claimed, so nothing is set up.
    if tokio::runtime::Handle::current().runtime_flavor()
        != tokio::runtime::RuntimeFlavor::MultiThread
    {
        return Err(Error::Internal(format!(
            "tenant {tenant}: serving needs the multi-thread Tokio runtime"
        )));
    }
    // Machines push their agents' and homes' work into this head: without
    // the hooks every push is refused. Fatal on purpose, unlike `intentd`
    // (which also runs local agents): an orchestrator runs agents only on
    // machines, so a head that refuses every push cannot serve. Before the
    // claim, so a failure claims nothing.
    install_receive_hooks(data_dir)
        .map_err(|e| Error::Internal(format!("tenant {tenant}: node git receive hooks: {e}")))?;
    let (store, lease) = match Store::try_open_postgres_owned(
        pool,
        id,
        PostgresOptions::default(),
        instance,
        timing.ttl,
        orchestrator,
    )
    .await?
    {
        Ok(opened) => opened,
        Err(held) => return Ok(Serving::Held(held)),
    };
    let heartbeat = LeaseHeartbeat::spawn(store.clone(), &lease, timing.ttl, timing.interval);
    let fence = heartbeat.fence();
    let dir = crate::tenant_dir(data_dir, tenant);
    std::fs::create_dir_all(&dir)
        .map_err(|e| Error::Internal(format!("tenant {tenant}: create {}: {e}", dir.display())))?;
    crate::process::probe_cow(dir.join("workspaces"));
    let bus = EventBus::new(store.clone());
    let reverse = Arc::new(PrimaryReverseRegistry::new());
    let secrets = Arc::new(crate::secrets::TenantSecrets::new(&store, keyring)?);
    let services = tenant_services(store, bus.clone(), &dir, reverse.clone(), secrets.clone())?;
    let mut runner = intent_services::self_fence::tenant_self_fence(services.clone())
        .ok_or_else(|| Error::Internal(format!("tenant {tenant}: the store has no fence")))?;
    let admission = Unfenced(fence.clone());
    let recovered = match recover_tenant(&services, &bus, &dir, &fence).await {
        Ok(recovered) => recovered,
        Err(failed) => {
            // Nothing of the tenant's work started: stop what recovery set
            // up (the agent manager, delete timers it re-armed) and give the
            // lease back so another instance claims the tenant at once.
            fence.fire("the tenant's startup recovery failed");
            if let Some(agents) = failed.agents {
                agents.shutdown().await;
            }
            let _ = runner.stop().await;
            if let Err(error) = heartbeat.release().await {
                tracing::warn!(tenant, %error, "could not release the tenant lease");
            }
            return Err(Error::Internal(format!(
                "tenant {tenant}: startup recovery: {}",
                failed.error
            )));
        }
    };
    let runtime = TenantRuntime::start_with_sweeps(
        &services,
        &dir,
        &admission,
        &admission,
        &mut runner,
        sweeps,
    )
    .await;
    if let Some(completions) = recovered.completions {
        // Deliver what finished while no one served the tenant, off the
        // open path. Machines receive the wakes; no client needs to be
        // connected for them.
        runtime.reconcile_completions(&services, completions);
    }
    tracing::info!(tenant, instance, epoch = lease.epoch, "serving the tenant");
    Ok(Serving::Served(Box::new(ServedTenant {
        tenant: tenant.to_owned(),
        dir,
        services,
        secrets,
        bus,
        runtime,
        reverse,
        fence,
        heartbeat,
        agents: recovered.agents,
        report: Some(runner.spawn()),
    })))
}

/// Why [`recover_tenant`] failed, and the agent manager it had attached
/// by then, for the caller to stop.
struct RecoveryFailed {
    error: Error,
    agents: Option<Arc<AgentManager>>,
}

/// What [`recover_tenant`] restored.
struct Recovered {
    /// The tenant's agent manager (the strong handle: `Services` holds a
    /// weak one).
    agents: Arc<AgentManager>,
    /// The completion bookkeeping to reconcile once the tenant serves;
    /// `None` once the fence fired.
    completions: Option<StartupCompletionRecovery>,
}

/// The agent state a restart left in the tenant's store, restored before
/// its work starts, in `intentd`'s order (`cmd_serve_owned`): the boot
/// settings migrations and provider profile cleanups, the agent manager
/// (so `agent.*` turns run, on machines too), stale session flags, the
/// machine connector's tailcat binary, deletes a previous owner stopped
/// part-way, the agent send queues, stop redeliveries, and the completion
/// bookkeeping. Each step is skipped once `fence` fired; the steps
/// `intentd` treats as startup failures (the settings migrations, the
/// queues) fail the open, with the agent manager attached so far.
async fn recover_tenant(
    services: &Services,
    bus: &EventBus,
    dir: &Path,
    fence: &TenantFence,
) -> std::result::Result<Recovered, RecoveryFailed> {
    if let Some(registry) = services.settings_registry() {
        intent_services::run_boot_settings_migrations(&registry, services.store())
            .await
            .map_err(|error| RecoveryFailed {
                error,
                agents: None,
            })?;
    }
    // Remove secrets journaled by an interrupted profile create/delete.
    services.sweep_provider_profile_secrets().await;
    services.migrate_remote_choices().await;
    let agents = intent_app::agents::start_agent_manager(
        services,
        bus,
        intent_app::agents::AgentManagerSetup {
            data_dir: dir,
            chief_cwd_root: chief_cwd_root(dir),
            process_cap: intent_app::agents::process_cap(&services.effective_settings()),
            policy_override: None,
        },
    );
    tracing::info!(process_cap = agents.registry().cap(), "agent manager ready");
    let live = || !fence.is_fenced();
    if live() {
        // Agent sessions are process-local: every active flag a previous
        // owner left is stale, and so is every in-flight status.
        match services.store().reset_all_active_flags().await {
            Ok(0) => {}
            Ok(reset) => tracing::warn!(reset, "reset stale is_active=1 flags on startup"),
            Err(e) => tracing::warn!(error = %e, "is_active reset failed"),
        }
        match services.heal_stale_agent_sessions().await {
            Ok(0) => {}
            Ok(healed) => {
                tracing::info!(healed, "healed stale in-flight agent sessions on startup");
            }
            Err(e) => tracing::warn!(error = %e, "stale agent session heal sweep failed"),
        }
    }
    // Machines with only a tailcat route are dialed through this binary.
    // Set before the pending-delete sweep, whose deletes send machines
    // their final stops.
    services.configure_remote_machines(Some(intent_core::tailcat::resolve_tailcat_bin()));
    if live() {
        if let Err(error) = services.finish_pending_deletes(|| fence.is_fenced()).await {
            tracing::warn!(%error, "pending delete sweep failed");
        }
        match services.rearm_delete_graces().await {
            Ok(0) => {}
            Ok(count) => tracing::info!(count, "re-armed stored delete deadlines"),
            Err(error) => tracing::warn!(%error, "re-arming stored delete deadlines failed"),
        }
        let queued = match intent_app::agents::restore_startup_queues(services).await {
            Ok(queued) => queued,
            Err(error) => {
                return Err(RecoveryFailed {
                    error,
                    agents: Some(agents),
                })
            }
        };
        if queued > 0 {
            tracing::info!(queued, "rehydrated persisted agent queue messages");
        }
    }
    if live() {
        match agents.rehydrate_stop_redeliveries().await {
            Ok(0) => {}
            Ok(rehydrated) => tracing::info!(
                rehydrated,
                "rehydrated persisted stop-redelivery payloads on startup"
            ),
            Err(e) => tracing::warn!(error = %e, "stop-redelivery rehydration failed"),
        }
    }
    let completions = if live() {
        Some(services.prepare_startup_completion_recovery().await)
    } else {
        None
    };
    Ok(Recovered {
        agents,
        completions,
    })
}

/// The tenant's chief cwd, as [`tenant_services`] configures it.
fn chief_cwd_root(dir: &Path) -> PathBuf {
    intent_core::chief_cwd_root(dir)
}

/// Install head's git receive hooks under `data_dir`, once per process
/// (`intent_git::node_git::configure_receive_hooks`, as `intentd` does at
/// start). The hooks are process-wide and the same for every tenant, so
/// only the first open installs them: installing clears the hooks
/// directory first, and a push running then would see no `update` hook and
/// land unchecked. Later opens (any `data_dir`) keep the first install;
/// every push re-verifies it and reinstalls it in place when altered. A
/// failed install is retried by the next open.
fn install_receive_hooks(data_dir: &Path) -> Result<()> {
    static INSTALLED: std::sync::Mutex<bool> = std::sync::Mutex::new(false);
    let mut installed = INSTALLED
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if !*installed {
        intent_git::node_git::configure_receive_hooks(data_dir)?;
        *installed = true;
    }
    Ok(())
}

/// The tenant's `Services` over `bus`: every path under `dir`, in
/// orchestrator mode, dispatching to clients through `reverse`, with
/// `secrets` as the store every secret reader and writer uses (the tenant's
/// Postgres rows in [`serve_tenant`]). The GitLab credential pair is the one
/// exception: `Services` keeps it in a concrete file store, here the
/// tenant's own `secrets.json`, never the process-wide default.
fn tenant_services(
    store: Store,
    bus: EventBus,
    dir: &Path,
    reverse: Arc<PrimaryReverseRegistry>,
    secrets: Arc<dyn intent_services::SecretStore>,
) -> Result<Services> {
    let registry = SettingsRegistry::load(dir.join("config.toml"))?;
    let gitlab_pair = intent_core::FileSecretStore::with_path(dir.join("secrets.json"));
    Ok(intent_app::compose(
        Services::new_with_secret_file(store, gitlab_pair)
            .with_event_bus(bus)
            .with_reverse_dispatch(reverse)
            .with_settings_registry(Arc::new(registry))
            .with_secret_store(secrets)
            .with_workspaces_root(dir.join("workspaces"))
            .with_assets_root(dir.join("assets"))
            .with_chief_cwd_root(chief_cwd_root(dir))
            .with_models_cache_dir(dir)
            .with_provider_profiles_dir(dir)
            .with_script_output_dir(dir)
            .with_orchestrator_mode(true),
    ))
}

impl ServedTenant {
    /// The tenant id.
    #[must_use]
    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    /// Where the tenant keeps its files.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// The tenant's `Services`.
    #[must_use]
    pub fn services(&self) -> &Services {
        &self.services
    }

    /// The secret store the tenant's `Services` were built with: its
    /// Postgres rows.
    #[must_use]
    pub fn secrets(&self) -> &Arc<crate::secrets::TenantSecrets> {
        &self.secrets
    }

    /// The tenant's agent manager.
    #[must_use]
    pub fn agents(&self) -> &Arc<AgentManager> {
        &self.agents
    }

    /// The tenant's event bus.
    #[must_use]
    pub fn bus(&self) -> &EventBus {
        &self.bus
    }

    /// Start the tenant's client listeners ([`crate::listen`]): every
    /// connection joins the tenant's client connections, which its fence
    /// closes.
    ///
    /// # Errors
    /// As [`crate::listen::start_client_listeners`].
    ///
    /// The listeners stop (and unbind) once the tenant's fence fires, so
    /// clients stop reaching a process that no longer serves the tenant.
    #[cfg(unix)]
    pub async fn listen(
        &self,
        config: &crate::listen::ListenConfig,
    ) -> std::result::Result<crate::listen::ClientListeners, String> {
        let listeners = crate::listen::start_client_listeners(
            config,
            &self.services,
            &self.bus,
            self.reverse.clone(),
            self.runtime.connections(),
        )
        .await?;
        let fence = self.fence.clone();
        let stop = listeners.stopper();
        let tenant = self.tenant.clone();
        // Ends with the listeners, whichever comes first.
        tokio::spawn(async move {
            tokio::select! {
                () = fence.fenced() => {
                    stop.stop().await;
                    tracing::info!(tenant, "the tenant is fenced: its client listeners stopped");
                }
                () = stop.stopped() => {}
            }
        });
        Ok(listeners)
    }

    /// The tenant's running work.
    #[must_use]
    pub fn runtime(&self) -> &TenantRuntime {
        &self.runtime
    }

    /// The fence of the tenant's claim.
    #[must_use]
    pub fn fence(&self) -> &TenantFence {
        &self.fence
    }

    /// The lease heartbeat.
    #[must_use]
    pub fn heartbeat(&self) -> &LeaseHeartbeat {
        &self.heartbeat
    }

    /// Wait until the tenant's fence fired and its work stopped. Once
    /// only: later calls (and calls after [`Self::watch_report`]) wait
    /// forever.
    pub async fn stopped(&mut self) -> SelfFenceReport {
        let Some(report) = self.report.take() else {
            return std::future::pending().await;
        };
        let (tx, rx) = tokio::sync::oneshot::channel();
        crate::watch_report(report, move |report| {
            let _ = tx.send(report);
        });
        match rx.await {
            Ok(report) => report,
            Err(_) => std::future::pending().await,
        }
    }

    /// Hand the tenant's fence report to `on_stop` as soon as it is ready,
    /// on a task of its own ([`crate::watch_report`]).
    pub fn watch_report(&mut self, on_stop: impl FnOnce(SelfFenceReport) + Send + 'static) {
        if let Some(report) = self.report.take() {
            crate::watch_report(report, on_stop);
        }
    }
}

#[cfg(test)]
mod tests {
    use intent_core::WorkspaceApi;
    use serde_json::json;

    use super::*;

    const CREDENTIALS: &[&str] = &[
        "sourceControl.github.token",
        "sourceControl.gitlab.token",
        "linear.token",
        "voice.elevenlabs.apiKey",
        "voice.openai.apiKey",
    ];

    /// Sets env vars for one test and restores them on drop, even when an
    /// assertion panics (nextest runs each test in its own process).
    struct EnvVars(Vec<(&'static str, Option<std::ffi::OsString>)>);

    impl EnvVars {
        fn set(vars: &[(&'static str, &std::ffi::OsStr)]) -> Self {
            Self(
                vars.iter()
                    .map(|(key, value)| {
                        let old = std::env::var_os(key);
                        std::env::set_var(key, value);
                        (*key, old)
                    })
                    .collect(),
            )
        }
    }

    impl Drop for EnvVars {
        fn drop(&mut self) {
            for (key, old) in self.0.drain(..) {
                match old {
                    Some(value) => std::env::set_var(key, value),
                    None => std::env::remove_var(key),
                }
            }
        }
    }

    async fn tenant(root: &Path, name: &str) -> (Services, PathBuf) {
        let dir = root.join(name);
        std::fs::create_dir_all(&dir).unwrap();
        let store = Store::open(&dir.join("db.sqlite")).await.unwrap();
        let bus = EventBus::new(store.clone());
        let reverse = Arc::new(PrimaryReverseRegistry::new());
        let secrets = Arc::new(intent_core::FileSecretStore::with_path(
            dir.join("secrets.json"),
        ));
        (
            tenant_services(store, bus, &dir, reverse, secrets).unwrap(),
            dir,
        )
    }

    /// A desktop that connects through the tenant's listeners is reachable
    /// from a clone of the tenant's `Services` made before they started —
    /// as the runtime's and remote calls' clones are — so agent browser
    /// actions find it.
    #[cfg(unix)]
    #[tokio::test]
    async fn a_listener_desktop_is_reachable_from_earlier_services_clones() {
        use futures_util::{SinkExt as _, StreamExt as _};

        let root = tempfile::tempdir().unwrap();
        let dir = root.path().join("t");
        std::fs::create_dir_all(&dir).unwrap();
        let store = Store::open(&dir.join("db.sqlite")).await.unwrap();
        let bus = EventBus::new(store.clone());
        let reverse = Arc::new(PrimaryReverseRegistry::new());
        let secrets = Arc::new(intent_core::FileSecretStore::with_path(
            dir.join("secrets.json"),
        ));
        let services = tenant_services(store, bus.clone(), &dir, reverse.clone(), secrets).unwrap();
        let earlier = services.clone();
        let dispatch = earlier
            .feature_reverse_dispatch()
            .cloned()
            .expect("the tenant's Services carry its reverse registry");
        assert!(!dispatch.is_connected());

        let connections = intent_app::tenant::ClientConnections::default();
        let config = crate::listen::ListenConfig {
            socket_path: dir.join("o.sock"),
            bind_addresses: vec![std::net::IpAddr::from([127, 0, 0, 1])],
            ws_port: 0,
            tls_dir: dir.join("tls"),
            insecure: true,
        };
        let listeners =
            crate::listen::start_client_listeners(&config, &services, &bus, reverse, &connections)
                .await
                .unwrap();
        let (mut ws, _) =
            tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{}/ws", listeners.ws_port()))
                .await
                .unwrap();
        let hello = json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "client.hello",
            "params": { "clientId": "desktop-a", "capabilities": { "browserExec": true } },
        });
        ws.send(tokio_tungstenite::tungstenite::Message::Text(
            hello.to_string().into(),
        ))
        .await
        .unwrap();
        let reply = tokio::time::timeout(
            intentd_test_support::test_timeout(std::time::Duration::from_secs(30)),
            async {
                loop {
                    if let Some(Ok(tokio_tungstenite::tungstenite::Message::Text(text))) =
                        ws.next().await
                    {
                        let value: serde_json::Value = serde_json::from_str(&text).unwrap();
                        if value["id"] == json!(1) {
                            return value;
                        }
                    }
                }
            },
        )
        .await
        .expect("client.hello reply");
        assert!(reply.get("error").is_none(), "{reply}");
        assert!(
            dispatch.is_connected(),
            "the earlier clone sees the listener's desktop"
        );
        listeners.stop().await;
    }

    /// Every credential a tenant saves — including the GitLab token, which
    /// goes through the paired GitLab source — lands in, and resolves from,
    /// that tenant's own `secrets.json`; the process-wide default file is
    /// neither read nor written.
    #[tokio::test]
    async fn tenants_keep_credentials_in_their_own_secrets_file() {
        let root = tempfile::tempdir().unwrap();
        let host_file = root.path().join("host.secrets.json");
        let host = intent_core::FileSecretStore::with_path(&host_file);
        for account in CREDENTIALS {
            host.store(account, "host-value").unwrap();
        }
        let _env = EnvVars::set(&[("INTENTD_SECRETS_FILE", host_file.as_os_str())]);

        let (a, a_dir) = tenant(root.path(), "a").await;
        let (b, b_dir) = tenant(root.path(), "b").await;
        for account in CREDENTIALS {
            assert_eq!(
                a.secret_source().load(account).unwrap(),
                None,
                "a fresh tenant must not see the host's {account}"
            );
        }
        for (svc, tag) in [(&a, "a"), (&b, "b")] {
            let changes: Vec<_> = CREDENTIALS
                .iter()
                .map(|path| json!({ "path": path, "value": format!("{tag}-{path}") }))
                .collect();
            intent_core::with_caller(
                intent_core::Caller::Daemon,
                svc.settings_update(json!(changes)),
            )
            .await
            .unwrap();
        }

        for (svc, dir, tag) in [(&a, &a_dir, "a"), (&b, &b_dir, "b")] {
            let file = intent_core::FileSecretStore::with_path(dir.join("secrets.json"));
            for account in CREDENTIALS {
                let want = format!("{tag}-{account}");
                assert_eq!(file.load(account).unwrap().as_deref(), Some(want.as_str()));
                assert_eq!(
                    svc.secret_source().load(account).unwrap().as_deref(),
                    Some(want.as_str()),
                    "tenant {tag} resolves another store's {account}"
                );
            }
        }
        for account in CREDENTIALS {
            assert_eq!(host.load(account).unwrap().as_deref(), Some("host-value"));
        }
    }

    /// The default claim wait outlasts a crashed instance's lease twice over.
    #[test]
    fn the_default_claim_wait_is_two_lease_ttls() {
        assert_eq!(crate::DEFAULT_CLAIM_WAIT, LEASE_TTL * 2);
    }

    /// A tenant's resolvers never fall back to host-wide credentials (env
    /// vars, the host `gh` login).
    #[tokio::test]
    async fn tenant_secret_source_denies_host_credentials() {
        let root = tempfile::tempdir().unwrap();
        let (a, _) = tenant(root.path(), "a").await;
        assert!(a.orchestrator_mode());
        assert!(!a.secret_source().allows_host_credentials());
    }
}
