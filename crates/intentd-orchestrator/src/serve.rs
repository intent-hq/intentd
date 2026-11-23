//! Serving one tenant: open it under its lease, build its `Services`, and
//! start its work under its self-fence (see the crate docs for the order).

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use intent_app::tenant::{Admission, TenantRuntime};
use intent_core::tenant_fence::{SelfFenceReport, TenantFence};
use intent_core::{Error, Result};
use intent_services::{EventBus, Services, SettingsRegistry};
use intent_store::postgres::{
    LeaseHeartbeat, PgPool, PostgresOptions, TenantId, LEASE_RENEW_INTERVAL, LEASE_TTL,
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
    bus: EventBus,
    runtime: TenantRuntime,
    /// The tenant's reverse registry: installed in `services` before any
    /// clone of it exists, and shared by its client listeners, so agent
    /// browser actions reach the desktops those listeners connect.
    reverse: Arc<PrimaryReverseRegistry>,
    fence: TenantFence,
    /// Renews the lease; dropping it fires the fence.
    heartbeat: LeaseHeartbeat,
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
/// its files under [`crate::tenant_dir`] of `data_dir`.
///
/// # Errors
/// For an invalid tenant id; `Error::SchemaMismatch` when the schema does
/// not match this build; `Error::NotOwner` when another instance holds a
/// live lease (nothing was written); when the tenant's directory or
/// settings cannot be set up.
pub async fn serve_tenant(
    pool: PgPool,
    tenant: &str,
    data_dir: &Path,
    instance: &str,
    timing: LeaseTiming,
    orchestrator: OrchestratorOnly,
) -> Result<ServedTenant> {
    let id = TenantId::new(tenant)?;
    let (store, lease) = Store::open_postgres_owned(
        pool,
        id,
        PostgresOptions::default(),
        instance,
        timing.ttl,
        orchestrator,
    )
    .await?;
    let heartbeat = LeaseHeartbeat::spawn(store.clone(), &lease, timing.ttl, timing.interval);
    let fence = heartbeat.fence();
    let dir = crate::tenant_dir(data_dir, tenant);
    std::fs::create_dir_all(&dir)
        .map_err(|e| Error::Internal(format!("tenant {tenant}: create {}: {e}", dir.display())))?;
    let bus = EventBus::new(store.clone());
    let reverse = Arc::new(PrimaryReverseRegistry::new());
    let services = tenant_services(store, bus.clone(), &dir, reverse.clone())?;
    let mut runner = intent_services::self_fence::tenant_self_fence(services.clone())
        .ok_or_else(|| Error::Internal(format!("tenant {tenant}: the store has no fence")))?;
    let admission = Unfenced(fence.clone());
    let runtime = TenantRuntime::start(&services, &dir, &admission, &admission, &mut runner).await;
    tracing::info!(tenant, instance, epoch = lease.epoch, "serving the tenant");
    Ok(ServedTenant {
        tenant: tenant.to_owned(),
        dir,
        services,
        bus,
        runtime,
        reverse,
        fence,
        heartbeat,
        report: Some(runner.spawn()),
    })
}

/// The tenant's `Services` over `bus`: every path under `dir`, in
/// orchestrator mode, dispatching to clients through `reverse`.
fn tenant_services(
    store: Store,
    bus: EventBus,
    dir: &Path,
    reverse: Arc<PrimaryReverseRegistry>,
) -> Result<Services> {
    let registry = SettingsRegistry::load(dir.join("config.toml"))?;
    let secrets = intent_core::FileSecretStore::with_path(dir.join("secrets.json"));
    Ok(intent_app::compose(
        Services::new_with_secret_file(store, secrets)
            .with_event_bus(bus)
            .with_reverse_dispatch(reverse)
            .with_settings_registry(Arc::new(registry))
            .with_workspaces_root(dir.join("workspaces"))
            .with_assets_root(dir.join("assets"))
            .with_chief_cwd_root(dir.join("chief-cwd"))
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
        (tenant_services(store, bus, &dir, reverse).unwrap(), dir)
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
        let services = tenant_services(store, bus.clone(), &dir, reverse.clone()).unwrap();
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
