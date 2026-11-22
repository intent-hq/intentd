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
    runtime: TenantRuntime,
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
    let services = tenant_services(store, &dir)?;
    let mut runner = intent_services::self_fence::tenant_self_fence(services.clone())
        .ok_or_else(|| Error::Internal(format!("tenant {tenant}: the store has no fence")))?;
    let admission = Unfenced(fence.clone());
    let runtime = TenantRuntime::start(&services, &dir, &admission, &admission, &mut runner).await;
    tracing::info!(tenant, instance, epoch = lease.epoch, "serving the tenant");
    Ok(ServedTenant {
        tenant: tenant.to_owned(),
        dir,
        services,
        runtime,
        fence,
        heartbeat,
        report: Some(runner.spawn()),
    })
}

/// The tenant's `Services`: every path under `dir`, in orchestrator mode.
fn tenant_services(store: Store, dir: &Path) -> Result<Services> {
    let registry = SettingsRegistry::load(dir.join("config.toml"))?;
    let secrets = intent_core::FileSecretStore::with_path(dir.join("secrets.json"));
    let bus = EventBus::new(store.clone());
    Ok(intent_app::compose(
        Services::new(store)
            .with_event_bus(bus)
            .with_settings_registry(Arc::new(registry))
            .with_secret_store(Arc::new(secrets))
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
    /// only: later calls wait forever.
    pub async fn stopped(&mut self) -> SelfFenceReport {
        let Some(report) = self.report.as_mut() else {
            return std::future::pending().await;
        };
        let report = report.await;
        self.report = None;
        report.unwrap_or_else(|e| SelfFenceReport {
            reason: format!("the self-fence task ended: {e}"),
            fenced_at: std::time::Instant::now(),
            steps: Vec::new(),
            // Its steps may not have run: treat it as overrun.
            overran: vec![intent_core::tenant_fence::FenceStep::Tasks],
        })
    }
}
