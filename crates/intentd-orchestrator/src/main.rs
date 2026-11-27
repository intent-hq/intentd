//! `intentd-orchestrator`: serve every tenant in [`TENANTS_ENV`] from the
//! Postgres database at [`POSTGRES_URL_ENV`], each under its own lease and
//! self-fence (see the library docs). With [`TENANTS_ENV`] unset, it serves
//! the one tenant [`DEFAULT_TENANT`]; a set value must name at least one.
//!
//! A tenant whose fence fires stops on its own; the others keep running.
//! A tenant another instance still holds (a crashed one's lease has not
//! lapsed yet) is claimed once its lease lapses or is released, re-read
//! every second, with all tenants waiting within one [`CLAIM_WAIT_ENV`]
//! (two lease TTLs by default).
//!
//! The process exits [`FENCE_OVERRAN_EXIT`] when a fence step overran (its
//! work may outlive the lease), 1 when a tenant cannot be opened at start
//! (its lease still held after the claim wait included) or every tenant
//! stopped, and 0 on Ctrl-C or SIGTERM after stopping every tenant (also
//! while waiting for a lease, which it then leaves alone).
//! On Ctrl-C or SIGTERM each tenant whose work stopped cleanly releases its
//! lease, so a restart or standby takes it over at once instead of waiting
//! out the TTL.
//!
//! Once [`DEFAULT_TENANT`] is served, its clients connect over the local
//! socket and the WSS listener with `intentd`'s auth (see
//! `intentd_orchestrator::listen` for the environment variables).

use std::process::ExitCode;

use std::sync::Arc;

use intent_store::postgres::SecretKeyring;
use intentd_orchestrator::process::{ceiling_from_env, WORKLOAD_MEMORY_ENV};
use intentd_orchestrator::secrets::{keyring_from_env, KEY_ENV};
use intentd_orchestrator::serve::{try_serve_tenant, LeaseTiming, ServedTenant, Serving};
use intentd_orchestrator::{
    claim_wait_from_env, instance_id, is_fatal, tenants_from_env, wait_for_lease, CLAIM_WAIT_ENV,
    DEFAULT_TENANT, FENCE_OVERRAN_EXIT, INSTANCE_ENV, POSTGRES_URL_ENV, TENANTS_ENV,
};

/// Connections the shared pool keeps; each tenant writes through it.
const POOL_CONNECTIONS: u32 = 16;

/// Why [`stop_all`] fires every tenant's fence.
const STOPPING: &str = "the orchestrator is stopping";

/// How long a tenant open in flight when a stop signal arrives may still
/// take, so it is stopped and released with the others.
const OPEN_GRACE: std::time::Duration = std::time::Duration::from_secs(10);

/// How long one tenant's lease release may take on shutdown; past it the
/// lease lapses after its TTL instead.
const RELEASE_BUDGET: std::time::Duration = std::time::Duration::from_secs(2);

/// How long marking one stopped tenant closed may take on shutdown, before
/// its lease release; past it the tenant stays closing until its next start.
const MARK_CLOSED_BUDGET: std::time::Duration = std::time::Duration::from_secs(1);

/// How long closing the tenants may take together on shutdown, after their
/// drains; past it their fences fire anyway.
const CLOSE_BUDGET: std::time::Duration = std::time::Duration::from_secs(3);

/// How long the tenants' drains may take together on shutdown; past it
/// their fences fire anyway and refuse whatever the drains still write.
const DRAIN_BUDGET: std::time::Duration = std::time::Duration::from_secs(16);

/// The usual stop grace before SIGKILL (systemd, Kubernetes).
const STOP_GRACE: std::time::Duration = std::time::Duration::from_secs(30);

// A stop that overruns every budget (drain, close, the self-fence's
// budgeted steps, mark closed, release) still releases the lease before
// the stop grace ends, so a standby claims at once instead of waiting a TTL.
const _: () = assert!(
    DRAIN_BUDGET.as_millis()
        + CLOSE_BUDGET.as_millis()
        + intent_core::tenant_fence::SELF_FENCE_STEP_BUDGET.as_millis()
            * intent_core::tenant_fence::SELF_FENCE_BUDGETED_STEPS as u128
        + MARK_CLOSED_BUDGET.as_millis()
        + RELEASE_BUDGET.as_millis()
        < STOP_GRACE.as_millis(),
    "the shutdown budgets must fit inside the stop grace"
);

fn main() -> ExitCode {
    // The PTY workload shim: a contained terminal runs as
    // `/proc/self/exe cgroup-exec …`, which joins its cgroup and execs.
    if let Some(code) = intent_core::workload::dispatch_shim() {
        return code;
    }
    // Before reading any secret from the environment: same-user processes
    // (tenant workloads) must not read this one's environ or memory.
    if let Err(error) = intentd_orchestrator::secrets::harden_process() {
        eprintln!("intentd-orchestrator: {error}");
        return ExitCode::FAILURE;
    }
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();
    // Read and clear the URL (it may carry a password) and the tenant
    // secret key while this process is still single-threaded, so no child
    // process inherits them: not even the process setup's own (`systemctl`).
    let url = std::env::var(POSTGRES_URL_ENV);
    std::env::remove_var(POSTGRES_URL_ENV);
    let keyring = keyring_from_env(|name| std::env::var(name).ok());
    std::env::remove_var(KEY_ENV);
    let ceiling =
        std::env::var_os(WORKLOAD_MEMORY_ENV).map(|value| value.into_string().unwrap_or_default());
    let ceiling = match ceiling_from_env(ceiling.as_deref()) {
        Ok(ceiling) => ceiling,
        Err(error) => {
            tracing::error!("{error}");
            return ExitCode::FAILURE;
        }
    };
    match intent_core::Config::resolve() {
        Ok(config) => intentd_orchestrator::process::setup(&config.data_dir, ceiling),
        Err(error) => {
            tracing::error!(%error, "data dir");
            return ExitCode::FAILURE;
        }
    }
    let Ok(url) = url else {
        tracing::error!("{POSTGRES_URL_ENV} is unset");
        return ExitCode::FAILURE;
    };
    // No key: no tenant opens.
    let keyring = match keyring {
        Ok(keyring) => Arc::new(keyring),
        Err(error) => {
            tracing::error!(%error, "tenant secrets");
            return ExitCode::FAILURE;
        }
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
    match runtime.block_on(run(url, keyring)) {
        Ok(code) => code,
        Err(error) => {
            tracing::error!(%error, "orchestrator stopped");
            ExitCode::FAILURE
        }
    }
}

async fn run(url: String, keyring: Arc<SecretKeyring>) -> Result<ExitCode, String> {
    // Process-owned, like the containment it cleans up after: every tenant's
    // finished workloads leave leaves only this releases.
    let _leaf_sweeper = intentd_orchestrator::process::spawn_leaf_sweeper();
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
    let claim_wait = claim_wait_from_env(std::env::var(CLAIM_WAIT_ENV).ok().as_deref())?;
    // Listen before any tenant opens, and race every startup step against
    // the signal, so a hung step cannot swallow it. On a signal, the open
    // in flight gets up to OPEN_GRACE to finish; then every open tenant is
    // stopped and released. An open still running past the grace is
    // dropped, keeping whatever it claimed until the TTL.
    let shutdown = shutdown_signal();
    tokio::pin!(shutdown);
    let pool = tokio::select! {
        pool = intent_store::postgres::connect(&url, POOL_CONNECTIONS) => {
            pool.map_err(|e| format!("connect: {e}"))?
        }
        () = &mut shutdown => {
            tracing::info!("stopped before connecting");
            return Ok(ExitCode::SUCCESS);
        }
    };

    // Every tenant's report is watched from the moment it opens, startup
    // included: an overrun fence exits at once, whatever the other tenants
    // are doing.
    let (stopped_tx, mut stopped) = tokio::sync::mpsc::unbounded_channel::<String>();
    let mut served: Vec<ServedTenant> = Vec::new();
    // A live lease on a tenant (an instance that just crashed, or one still
    // running) is waited out, all tenants together within one claim wait.
    let waiting_until = tokio::time::Instant::now()
        .checked_add(claim_wait)
        .ok_or_else(|| format!("{CLAIM_WAIT_ENV}: too long"))?;
    for tenant in &tenants {
        let opened = loop {
            // Boxed so the grace below takes ownership: a timed-out open
            // (and the lease keeper it may already run) is dropped right
            // there, not kept renewing while stop_all waits for the others.
            let mut opening = Box::pin(try_serve_tenant(
                pool.clone(),
                tenant,
                &data_dir,
                &instance,
                LeaseTiming::default(),
                keyring.clone(),
                orchestrator_token(),
            ));
            let opened = tokio::select! {
                opened = &mut opening => opened,
                () = &mut shutdown => {
                    tracing::info!(tenant, "stopping every tenant during startup");
                    let graced = tokio::time::timeout(OPEN_GRACE, opening).await;
                    match graced {
                        Ok(Ok(Serving::Served(opened))) => {
                            let mut opened = *opened;
                            watch(&mut opened, &stopped_tx);
                            served.push(opened);
                        }
                        Ok(Ok(Serving::Held(_))) => {}
                        Ok(Err(error)) => tracing::warn!(tenant, %error, "the tenant did not open"),
                        Err(_) => tracing::warn!(
                            tenant,
                            "the tenant did not open in time; its lease lapses after its TTL"
                        ),
                    }
                    return Ok(stop_all(&mut served, &mut stopped).await);
                }
            };
            let held = match opened {
                Ok(Serving::Served(opened)) => break Ok(*opened),
                Ok(Serving::Held(held)) => held,
                Err(error) => break Err(format!("tenant {tenant}: {error}")),
            };
            // Only a refused claim waits: nothing of this tenant's was
            // claimed or written, so a signal from here on has nothing of it
            // to stop or release.
            tracing::info!(
                tenant,
                owner = %held.owner,
                expires_at = %held.expires_at,
                "waiting for the tenant lease to lapse before claiming it"
            );
            let waited = tokio::select! {
                waited = wait_for_tenant_lease(&pool, tenant, waiting_until) => waited,
                () = &mut shutdown => {
                    tracing::info!(tenant, "stopped while waiting for the tenant lease");
                    return Ok(stop_all(&mut served, &mut stopped).await);
                }
            };
            if !waited {
                break Err(format!("tenant {tenant}: {}", held.into_error()));
            }
        };
        match opened {
            Ok(mut opened) => {
                watch(&mut opened, &stopped_tx);
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
            () = &mut shutdown => {
                tracing::info!("stopping every tenant");
                return Ok(stop_all(&mut served, &mut stopped).await);
            }
        }
    }
}

/// [`wait_for_lease`] on `tenant`'s lease row
/// (`Store::postgres_lease_expires_in`), until `until`.
async fn wait_for_tenant_lease(
    pool: &intent_store::postgres::PgPool,
    tenant: &str,
    until: tokio::time::Instant,
) -> bool {
    let Ok(id) = intent_store::postgres::TenantId::new(tenant) else {
        return false;
    };
    let read = || intent_store::Store::postgres_lease_expires_in(pool.clone(), id.clone());
    wait_for_lease(read, until).await
}

/// Watch `tenant`'s fence report ([`on_stopped`]).
fn watch(tenant: &mut ServedTenant, stopped: &tokio::sync::mpsc::UnboundedSender<String>) {
    let name = tenant.tenant().to_owned();
    let tx = stopped.clone();
    tenant.watch_report(move |report| on_stopped(&name, &report, &tx));
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
    let var = |name: &str| std::env::var(name).ok();
    let config = intentd_orchestrator::listen::ListenConfig::from_env(var, data_dir)?;
    let public_hosts = intentd_orchestrator::listen::public_hosts_from_env(var)?;
    tenant
        .listen_with_public_hosts(&config, public_hosts)
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

/// Drain every tenant ([`ServedTenant::drain`], within [`DRAIN_BUDGET`]),
/// close it ([`ServedTenant::close`], within [`CLOSE_BUDGET`]), fire every
/// tenant's fence and wait until every one stopped; an overrun exits from
/// its watcher as soon as it is reported, before any release. A tenant
/// whose fence this call fired is marked closed and releases its lease once
/// its work stopped, concurrently with the others; one already fenced
/// (lease lost, a write met another owner) keeps both until the TTL. The
/// exit code.
async fn stop_all(
    served: &mut Vec<ServedTenant>,
    stopped: &mut tokio::sync::mpsc::UnboundedReceiver<String>,
) -> ExitCode {
    drain_all(served).await;
    close_all(served).await;
    let ours: Vec<String> = served
        .iter()
        .filter(|tenant| tenant.fence().fire(STOPPING))
        .map(|tenant| tenant.tenant().to_owned())
        .collect();
    let mut releases = tokio::task::JoinSet::new();
    while !served.is_empty() {
        let Some(tenant) = stopped.recv().await else {
            break;
        };
        let Some(at) = served.iter().position(|served| served.tenant() == tenant) else {
            continue;
        };
        let done = served.swap_remove(at);
        if ours.contains(&tenant) {
            releases.spawn(async move { release(&done).await });
        }
    }
    while releases.join_next().await.is_some() {}
    ExitCode::SUCCESS
}

/// Drain every tenant concurrently, with the process's host command
/// streams: the process stops.
async fn drain_all(served: &[ServedTenant]) {
    let streams = intent_services::host_exec_stream::registry();
    let mut drains = tokio::task::JoinSet::new();
    for tenant in served {
        drains.spawn(tenant.drain(Some(streams)));
    }
    let drained = tokio::time::timeout(DRAIN_BUDGET, async {
        while drains.join_next().await.is_some() {}
    })
    .await;
    if drained.is_err() {
        tracing::error!(
            budget_ms = DRAIN_BUDGET.as_millis(),
            "draining the tenants timed out; their fences refuse the rest"
        );
        drains.detach_all();
    }
}

/// Close every tenant concurrently, once drained.
async fn close_all(served: &[ServedTenant]) {
    let mut tasks = tokio::task::JoinSet::new();
    for tenant in served {
        tasks.spawn(tenant.close());
    }
    let finished = tokio::time::timeout(CLOSE_BUDGET, async {
        while tasks.join_next().await.is_some() {}
    })
    .await;
    if finished.is_err() {
        tracing::error!(
            budget_ms = CLOSE_BUDGET.as_millis(),
            "closing the tenants timed out; their fences refuse this process's writes"
        );
        tasks.detach_all();
    }
}

/// Mark a cleanly stopped tenant closed (within [`MARK_CLOSED_BUDGET`]),
/// then release its lease (within [`RELEASE_BUDGET`]); a failure leaves the tenant closing (the next
/// start opens it) or the lease to lapse after its TTL.
async fn release(tenant: &ServedTenant) {
    let name = tenant.tenant();
    match tokio::time::timeout(MARK_CLOSED_BUDGET, tenant.mark_closed()).await {
        Ok(Ok(moved)) if moved.reached(intent_store::TenantState::Closed) => {
            tracing::info!(
                tenant = name,
                last_row_seq = moved.last_row_seq,
                "the tenant is closed"
            );
        }
        Ok(Ok(moved)) => tracing::warn!(
            tenant = name,
            state = %moved.state,
            "the tenant was not closing; it stays as it is"
        ),
        Ok(Err(error)) => {
            tracing::warn!(tenant = name, %error, "could not mark the tenant closed");
        }
        Err(_) => tracing::warn!(
            tenant = name,
            budget_ms = MARK_CLOSED_BUDGET.as_millis(),
            "marking the tenant closed timed out; it stays closing until its next start"
        ),
    }
    match tokio::time::timeout(RELEASE_BUDGET, tenant.lease().release()).await {
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
