//! `intentd-orchestrator`: serve every tenant in [`TENANTS_ENV`] from the
//! Postgres database at [`POSTGRES_URL_ENV`], each under its own lease and
//! self-fence (see the library docs).
//!
//! A tenant whose fence fires stops on its own; the others keep running.
//! The process exits [`FENCE_OVERRAN_EXIT`] when a fence step overran (its
//! work may outlive the lease), 1 when a tenant cannot be opened at start
//! or every tenant stopped, and 0 on Ctrl-C or SIGTERM after stopping every tenant.
//! On Ctrl-C or SIGTERM each tenant whose work stopped cleanly releases its
//! lease, so a restart or standby takes it over at once instead of waiting
//! out the TTL.

use std::process::ExitCode;

use intentd_orchestrator::serve::{serve_tenant, LeaseTiming, ServedTenant};
use intentd_orchestrator::{
    instance_id, is_fatal, parse_tenants, FENCE_OVERRAN_EXIT, INSTANCE_ENV, POSTGRES_URL_ENV,
    TENANTS_ENV,
};

/// Connections the shared pool keeps; each tenant writes through it.
const POOL_CONNECTIONS: u32 = 16;

/// Why [`stop_all`] fires every tenant's fence.
const STOPPING: &str = "the orchestrator is stopping";

/// How long one tenant's lease release may take on shutdown; past it the
/// lease lapses after its TTL instead.
const RELEASE_BUDGET: std::time::Duration = std::time::Duration::from_secs(2);

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
    let tenants =
        parse_tenants(&std::env::var(TENANTS_ENV).map_err(|_| format!("{TENANTS_ENV} is unset"))?)?;
    let data_dir = intent_core::Config::resolve()
        .map_err(|e| format!("data dir: {e}"))?
        .data_dir;
    let instance = instance_id(std::env::var(INSTANCE_ENV).ok().as_deref());
    // Listen before any tenant opens: a signal during startup is then a
    // clean stop once the tenants are up, not the default kill.
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
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

    loop {
        if served.is_empty() {
            tracing::error!("every tenant stopped");
            return Ok(ExitCode::FAILURE);
        }
        tokio::select! {
            Some(tenant) = stopped.recv() => served.retain(|served| served.tenant() != tenant),
            () = &mut shutdown => {
                tracing::info!("stopping every tenant");
                return Ok(stop_all(&mut served, &mut stopped).await);
            }
        }
    }
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
/// stops every tenant before the process exits. On Unix both handlers are
/// installed when this is called, not when the future is first polled.
#[cfg(unix)]
fn shutdown_signal() -> impl std::future::Future<Output = ()> {
    use tokio::signal::unix::{signal, SignalKind};
    let interrupt = signal(SignalKind::interrupt());
    let term = signal(SignalKind::terminate());
    async move {
        match (interrupt, term) {
            (Ok(mut interrupt), Ok(mut term)) => {
                tokio::select! {
                    _ = interrupt.recv() => {}
                    _ = term.recv() => {}
                }
            }
            (Ok(mut interrupt), Err(error)) => {
                tracing::warn!(%error, "SIGTERM cannot be handled; only Ctrl-C stops the tenants");
                interrupt.recv().await;
            }
            (Err(error), Ok(mut term)) => {
                tracing::warn!(%error, "Ctrl-C cannot be handled; only SIGTERM stops the tenants");
                term.recv().await;
            }
            (Err(error), Err(_)) => {
                tracing::warn!(%error, "no stop signal can be handled");
                std::future::pending::<()>().await;
            }
        }
    }
}

/// Ctrl-C: stops every tenant before the process exits.
#[cfg(not(unix))]
fn shutdown_signal() -> impl std::future::Future<Output = ()> {
    async {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Fire every tenant's fence and wait until every one stopped; an overrun
/// exits from its watcher as soon as it is reported, before any release.
/// A tenant whose fence this call fired releases its lease once its work
/// stopped; one already fenced (lease lost, a write met another owner)
/// keeps it until the TTL. The exit code.
async fn stop_all(
    served: &mut Vec<ServedTenant>,
    stopped: &mut tokio::sync::mpsc::UnboundedReceiver<String>,
) -> ExitCode {
    let ours: Vec<String> = served
        .iter()
        .filter(|tenant| tenant.fence().fire(STOPPING))
        .map(|tenant| tenant.tenant().to_owned())
        .collect();
    while !served.is_empty() {
        let Some(tenant) = stopped.recv().await else {
            break;
        };
        let Some(at) = served.iter().position(|served| served.tenant() == tenant) else {
            continue;
        };
        let done = served.swap_remove(at);
        if ours.contains(&tenant) {
            release(&done).await;
        }
    }
    ExitCode::SUCCESS
}

/// Release a cleanly stopped tenant's lease, within [`RELEASE_BUDGET`]; a
/// failure only leaves the lease to lapse after its TTL.
async fn release(tenant: &ServedTenant) {
    let name = tenant.tenant();
    match tokio::time::timeout(RELEASE_BUDGET, tenant.heartbeat().release()).await {
        Ok(Ok(true)) => tracing::info!(tenant = name, "released the tenant lease"),
        Ok(Ok(false)) => {
            tracing::warn!(
                tenant = name,
                "the tenant lease was no longer ours; nothing released"
            );
        }
        Ok(Err(error)) => {
            tracing::warn!(tenant = name, %error, "could not release the tenant lease");
        }
        Err(_) => tracing::warn!(
            tenant = name,
            budget_ms = RELEASE_BUDGET.as_millis(),
            "releasing the tenant lease timed out; it lapses after its TTL"
        ),
    }
}

/// The orchestrator-mode proof the Postgres opener takes: this binary runs
/// only in orchestrator mode.
fn orchestrator_token() -> intent_store::OrchestratorOnly {
    intent_store::OrchestratorOnly::mint_for_intentd_serve()
}
