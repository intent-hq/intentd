//! `intentd-orchestrator`: serve every tenant in [`TENANTS_ENV`] from the
//! Postgres database at [`POSTGRES_URL_ENV`], each under its own lease and
//! self-fence (see the library docs). With [`TENANTS_ENV`] unset, it serves
//! the one tenant [`DEFAULT_TENANT`]; a set value must name at least one.
//!
//! A tenant whose fence fires stops on its own; the others keep running.
//! The process exits [`FENCE_OVERRAN_EXIT`] when a fence step overran (its
//! work may outlive the lease), 1 when a tenant cannot be opened at start
//! or every tenant stopped, and 0 on Ctrl-C or SIGTERM after stopping every tenant.
//!
//! Once [`DEFAULT_TENANT`] is served, its clients connect over the local
//! socket and the WSS listener with `intentd`'s auth (see
//! `intentd_orchestrator::listen` for the environment variables).

use std::process::ExitCode;

use intentd_orchestrator::serve::{serve_tenant, LeaseTiming, ServedTenant};
use intentd_orchestrator::{
    instance_id, is_fatal, tenants_from_env, DEFAULT_TENANT, FENCE_OVERRAN_EXIT, INSTANCE_ENV,
    POSTGRES_URL_ENV, TENANTS_ENV,
};

/// Connections the shared pool keeps; each tenant writes through it.
const POOL_CONNECTIONS: u32 = 16;

fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    // Read and clear the URL (it may carry a password) while this process
    // is still single-threaded, so no child process inherits it.
    let url = std::env::var(POSTGRES_URL_ENV);
    std::env::remove_var(POSTGRES_URL_ENV);
    let Ok(url) = url else {
        tracing::error!("{POSTGRES_URL_ENV} is unset");
        return ExitCode::FAILURE;
    };
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            tracing::error!(%error, "could not start the async runtime");
            return ExitCode::FAILURE;
        }
    };
    match runtime.block_on(run(url)) {
        Ok(code) => code,
        Err(error) => {
            tracing::error!(%error, "orchestrator stopped");
            ExitCode::FAILURE
        }
    }
}

async fn run(url: String) -> Result<ExitCode, String> {
    // Unset serves the default tenant; set but not UTF-8 is an error, never
    // the default.
    let raw = match std::env::var(TENANTS_ENV) {
        Ok(raw) => Some(raw),
        Err(std::env::VarError::NotPresent) => None,
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(format!("{TENANTS_ENV} is not valid UTF-8"));
        }
    };
    let tenants = tenants_from_env(raw.as_deref())?;
    if raw.is_none() {
        tracing::info!(
            tenant = DEFAULT_TENANT,
            "{TENANTS_ENV} is unset: serving the default tenant"
        );
    }
    let data_dir = intent_core::Config::resolve()
        .map_err(|e| format!("data dir: {e}"))?
        .data_dir;
    let instance = instance_id(std::env::var(INSTANCE_ENV).ok().as_deref());
    let pool = intent_store::postgres::connect(&url, POOL_CONNECTIONS)
        .await
        .map_err(|e| format!("connect: {e}"))?;

    // Every tenant's report is watched from the moment it opens, startup
    // included: an overrun fence exits at once, whatever the other tenants
    // are doing.
    let (stopped_tx, mut stopped) = tokio::sync::mpsc::unbounded_channel::<String>();
    let mut served: Vec<ServedTenant> = Vec::new();
    for tenant in &tenants {
        let opened = serve_tenant(
            pool.clone(),
            tenant,
            &data_dir,
            &instance,
            LeaseTiming::default(),
            orchestrator_token(),
        )
        .await
        .map_err(|e| format!("tenant {tenant}: {e}"));
        match opened {
            Ok(mut opened) => {
                let name = opened.tenant().to_owned();
                let tx = stopped_tx.clone();
                opened.watch_report(move |report| on_stopped(&name, &report, &tx));
                served.push(opened);
            }
            Err(error) => {
                stop_all(&mut served, &mut stopped).await;
                return Err(error);
            }
        }
    }

    // The default tenant's clients, once it is served (so a process that
    // does not hold its lease never binds); its fence closes them.
    #[cfg(unix)]
    let _listeners = match listen_for_default_tenant(&served, &data_dir).await {
        Ok(listeners) => listeners,
        Err(error) => {
            stop_all(&mut served, &mut stopped).await;
            return Err(error);
        }
    };

    loop {
        if served.is_empty() {
            tracing::error!("every tenant stopped");
            return Ok(ExitCode::FAILURE);
        }
        tokio::select! {
            Some(tenant) = stopped.recv() => served.retain(|served| served.tenant() != tenant),
            () = shutdown_signal() => {
                tracing::info!("stopping every tenant");
                return Ok(stop_all(&mut served, &mut stopped).await);
            }
        }
    }
}

/// Start the client listeners of [`DEFAULT_TENANT`] when this process
/// serves it ([`intentd_orchestrator::listen`]); `None` when it does not.
#[cfg(unix)]
async fn listen_for_default_tenant(
    served: &[ServedTenant],
    data_dir: &std::path::Path,
) -> Result<Option<intentd_orchestrator::listen::ClientListeners>, String> {
    let Some(tenant) = served
        .iter()
        .find(|tenant| tenant.tenant() == DEFAULT_TENANT)
    else {
        tracing::info!(
            tenant = DEFAULT_TENANT,
            "the default tenant is not served: no client listener"
        );
        return Ok(None);
    };
    let max_outstanding_rpcs = intent_core::Config::resolve()
        .map_err(|e| format!("config: {e}"))?
        .server_max_outstanding_rpcs;
    let config = intentd_orchestrator::listen::ListenConfig::from_env(
        |name| std::env::var(name).ok(),
        data_dir,
        max_outstanding_rpcs,
    )?;
    tenant
        .listen(&config)
        .await
        .map(Some)
        .map_err(|e| format!("tenant {DEFAULT_TENANT}: client listeners: {e}"))
}

/// A tenant's work stopped: exit at once when its fence overran (its work
/// may outlive the lease), else tell the supervisor.
fn on_stopped(
    tenant: &str,
    report: &intent_core::tenant_fence::SelfFenceReport,
    stopped: &tokio::sync::mpsc::UnboundedSender<String>,
) {
    tracing::warn!(tenant, reason = %report.reason, "tenant stopped");
    if is_fatal(report) {
        tracing::error!(
            tenant,
            overran = ?report.overran,
            "a self-fence step overran: exiting so no work outlives the lease"
        );
        std::process::exit(FENCE_OVERRAN_EXIT);
    }
    let _ = stopped.send(tenant.to_owned());
}

/// Ctrl-C, or SIGTERM on Unix (systemd, `docker stop`, Kubernetes): either
/// stops every tenant before the process exits.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut term = match tokio::signal::unix::signal(
            tokio::signal::unix::SignalKind::terminate(),
        ) {
            Ok(term) => term,
            Err(error) => {
                tracing::warn!(%error, "SIGTERM cannot be handled; only Ctrl-C stops the tenants");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

/// Fire every tenant's fence and wait until every one stopped; an overrun
/// exits from its watcher as soon as it is reported. The exit code.
async fn stop_all(
    served: &mut Vec<ServedTenant>,
    stopped: &mut tokio::sync::mpsc::UnboundedReceiver<String>,
) -> ExitCode {
    for tenant in served.iter() {
        tenant.fence().fire("the orchestrator is stopping");
    }
    while !served.is_empty() {
        let Some(tenant) = stopped.recv().await else {
            break;
        };
        served.retain(|served| served.tenant() != tenant);
    }
    ExitCode::SUCCESS
}

/// The orchestrator-mode proof the Postgres opener takes: this binary runs
/// only in orchestrator mode.
fn orchestrator_token() -> intent_store::OrchestratorOnly {
    intent_store::OrchestratorOnly::mint_for_intentd_serve()
}
