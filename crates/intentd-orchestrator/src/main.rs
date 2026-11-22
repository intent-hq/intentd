//! `intentd-orchestrator`: serve every tenant in [`TENANTS_ENV`] from the
//! Postgres database at [`POSTGRES_URL_ENV`], each under its own lease and
//! self-fence (see the library docs).
//!
//! A tenant whose fence fires stops on its own; the others keep running.
//! The process exits [`FENCE_OVERRAN_EXIT`] when a fence step overran (its
//! work may outlive the lease), 1 when a tenant cannot be opened at start
//! or every tenant stopped, and 0 on Ctrl-C after stopping every tenant.

use std::process::ExitCode;

use intentd_orchestrator::serve::{serve_tenant, LeaseTiming, ServedTenant};
use intentd_orchestrator::{
    instance_id, is_fatal, parse_tenants, FENCE_OVERRAN_EXIT, INSTANCE_ENV, POSTGRES_URL_ENV,
    TENANTS_ENV,
};

/// Connections the shared pool keeps; each tenant writes through it.
const POOL_CONNECTIONS: u32 = 16;

#[tokio::main]
async fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    match run().await {
        Ok(code) => code,
        Err(error) => {
            tracing::error!(%error, "orchestrator stopped");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<ExitCode, String> {
    let url =
        std::env::var(POSTGRES_URL_ENV).map_err(|_| format!("{POSTGRES_URL_ENV} is unset"))?;
    // Keep the URL (and its password) from every child process.
    std::env::remove_var(POSTGRES_URL_ENV);
    let tenants =
        parse_tenants(&std::env::var(TENANTS_ENV).map_err(|_| format!("{TENANTS_ENV} is unset"))?)?;
    let data_dir = intent_core::Config::resolve()
        .map_err(|e| format!("data dir: {e}"))?
        .data_dir;
    let instance = instance_id(std::env::var(INSTANCE_ENV).ok().as_deref());
    let pool = intent_store::postgres::connect(&url, POOL_CONNECTIONS)
        .await
        .map_err(|e| format!("connect: {e}"))?;

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
            Ok(tenant) => served.push(tenant),
            Err(error) => {
                stop_all(&mut served).await;
                return Err(error);
            }
        }
    }

    loop {
        if served.is_empty() {
            tracing::error!("every tenant stopped");
            return Ok(ExitCode::FAILURE);
        }
        let stopped = {
            let waits = served.iter_mut().map(|tenant| Box::pin(tenant.stopped()));
            tokio::select! {
                (report, index) = first_of(waits) => Some((index, report)),
                _ = tokio::signal::ctrl_c() => None,
            }
        };
        let Some((index, report)) = stopped else {
            tracing::info!("stopping every tenant");
            return Ok(stop_all(&mut served).await);
        };
        let tenant = served.swap_remove(index);
        tracing::warn!(tenant = tenant.tenant(), reason = %report.reason, "tenant stopped");
        if is_fatal(&report) {
            tracing::error!(
                tenant = tenant.tenant(),
                overran = ?report.overran,
                "a self-fence step overran: exiting so no work outlives the lease"
            );
            std::process::exit(FENCE_OVERRAN_EXIT);
        }
    }
}

/// Fire every tenant's fence and wait for its work to stop; the exit code.
async fn stop_all(served: &mut Vec<ServedTenant>) -> ExitCode {
    for tenant in served.iter() {
        tenant.fence().fire("the orchestrator is stopping");
    }
    for mut tenant in served.drain(..) {
        if is_fatal(&tenant.stopped().await) {
            std::process::exit(FENCE_OVERRAN_EXIT);
        }
    }
    ExitCode::SUCCESS
}

/// The first of `waits` to finish: its output and index.
async fn first_of<F: std::future::Future + Unpin>(
    waits: impl IntoIterator<Item = F>,
) -> (F::Output, usize) {
    let mut waits: Vec<F> = waits.into_iter().collect();
    std::future::poll_fn(move |cx| {
        for (index, wait) in waits.iter_mut().enumerate() {
            if let std::task::Poll::Ready(output) = std::pin::Pin::new(wait).poll(cx) {
                return std::task::Poll::Ready((output, index));
            }
        }
        std::task::Poll::Pending
    })
    .await
}

/// The orchestrator-mode proof the Postgres opener takes: this binary runs
/// only in orchestrator mode.
fn orchestrator_token() -> intent_store::OrchestratorOnly {
    intent_store::OrchestratorOnly::mint_for_intentd_serve()
}
