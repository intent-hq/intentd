//! The startup restores, sweeps and background loops one backend runs over
//! its [`Services`]: the daemon runs them once for its one backend, and an
//! orchestrator once per tenant.
//!
//! Each step is gated by one of two [`Admission`]s, exactly as the daemon
//! gates them: `startup_work` (the backend's own upkeep, closed when a
//! startup prerequisite failed or a stop was requested) and `orchestration`
//! (agents, workspaces, hooks, scripts, schedulers, PR monitors; also closed
//! in node mode). A refused step starts nothing.

use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use intent_core::tenant_fence::SelfFence;
use intent_services::Services;
use tokio::task::{AbortHandle, JoinHandle};

use crate::{RuntimeServicesExt as _, StartupExt as _};

/// Whether a class of startup work may start, and the spawn it gates.
pub trait Admission {
    /// Whether the work may start now.
    fn allowed(&self) -> bool;

    /// Start `start`'s task when admitted; otherwise a finished no-op task,
    /// so the caller's join graph is the same either way.
    fn spawn(&self, start: impl FnOnce() -> JoinHandle<()>) -> JoinHandle<()> {
        if self.allowed() {
            start()
        } else {
            // Keep the normal cleanup join graph without constructing or
            // polling the rejected operational task.
            intent_core::spawn_daemon(async {})
        }
    }
}

/// How many loops [`start_background`] keeps in [`Background`] when both
/// admissions allow them.
pub const LOOPS: usize = 13;

/// How many of them are the backend's own upkeep (`startup_work`), started
/// in node mode too.
pub const UPKEEP_LOOPS: usize = 1;

/// Restore the registries a restart left in the store, before any listener
/// serves: event subscriptions, scripts, background hooks and PR monitors
/// (orchestration), then the primary principal's GitHub identity refresh
/// (upkeep). Each is best-effort: a failure is logged, never fatal.
pub async fn restore_registries(
    services: &Services,
    startup_work: &impl Admission,
    orchestration: &impl Admission,
) {
    // Rehydrate persisted event subscriptions (monorepo#937) so `event.subscribe`
    // registrations survive daemon restarts; rows whose subscriber agent is
    // gone are pruned. Best-effort: a failure is logged but never aborts
    // startup (agents can re-subscribe).
    if orchestration.allowed() {
        match services.heal_event_subscriptions_on_startup().await {
            Ok(0) => {}
            Ok(loaded) => tracing::info!(
                loaded,
                "rehydrated persisted event subscriptions on startup"
            ),
            Err(e) => tracing::warn!(error = %e, "event subscription startup rehydration failed"),
        }
    }

    // Hydrate the script registry from the persisted definitions (§5.8) so
    // `script.*` survives daemon restarts. Best-effort: a failure is logged
    // but never aborts startup (scripts can still be re-created live).
    if orchestration.allowed() {
        match services.hydrate_scripts().await {
            Ok(0) => {}
            Ok(loaded) => tracing::info!(loaded, "hydrated persisted script definitions"),
            Err(e) => tracing::warn!(error = %e, "script registry hydration failed"),
        }
    }

    // Rehydrate active background hooks (`scheduled`/`running` rows) so their
    // schedules resume after a restart; hooks whose owning agent is gone are
    // cancelled instead. Best-effort: a failure is logged but never aborts
    // startup (agents can re-schedule).
    if orchestration.allowed() {
        match services.rehydrate_hooks().await {
            Ok(0) => {}
            Ok(resumed) => tracing::info!(resumed, "rehydrated active background hooks on startup"),
            Err(e) => tracing::warn!(error = %e, "background hook rehydration failed"),
        }
    }

    // Rehydrate active PR monitors (`ws.pr.monitor`) so watches resume after a
    // restart; monitors whose owning agent is gone are cancelled. Each resumed
    // monitor is marked for catch-up, so its first poll delivers any change
    // detected across the downtime immediately (no debounce). Best-effort: a
    // failure is logged but never aborts startup.
    if orchestration.allowed() {
        match services.rehydrate_pr_monitors().await {
            Ok(0) => {}
            Ok(resumed) => tracing::info!(resumed, "rehydrated active PR monitors on startup"),
            Err(e) => tracing::warn!(error = %e, "PR monitor rehydration failed"),
        }
    }

    // Populate the primary principal's GitHub identity once at boot when the
    // row still predates the GitHub connection (`login: null`), so roster
    // reads do not wait for a `principal.me` (intent-hq/intent#5534).
    // Fire-and-forget: only spawns the bounded off-path refresh (one refresh
    // per IDENTITY_REFRESH_INTERVAL across all trigger sites); a no-op
    // without GitHub auth and never a startup failure.
    if startup_work.allowed() {
        services.refresh_primary_identity_at_startup().await;
    }
}

/// The backend's startup sweeps and background loops, started under their
/// admissions.
#[must_use]
pub fn start_background(
    services: &Services,
    data_dir: &Path,
    startup_work: &impl Admission,
    orchestration: &impl Admission,
) -> Background {
    let mut sweeps = Vec::new();
    // Sweep orphaned `*.deleting-*` worktree trash dirs left behind when a
    // prior daemon crashed between the locked detach rename and the unlocked
    // recursive removal (monorepo#473). Spawned so the potentially multi-GB
    // removal never blocks startup; best-effort throughout — a failure never
    // aborts startup, and a missing workspaces root is a silent no-op.
    let services_trash_sweep = services.clone();
    sweeps.push(startup_work.spawn(|| {
        intent_core::spawn_daemon(async move {
            let removed = services_trash_sweep.sweep_orphaned_worktree_trash().await;
            if removed > 0 {
                tracing::info!(
                    removed,
                    "startup sweep removed orphaned worktree trash dirs"
                );
            }
        })
    }));
    // Sweep stale export staging dirs (workspace.export.*): export sessions
    // are in-memory only, so after a restart every leftover staging dir is an
    // orphan. Spawned + best-effort like the worktree trash sweep above.
    let services_export_sweep = services.clone();
    sweeps.push(startup_work.spawn(|| {
        intent_core::spawn_daemon(async move {
            services_export_sweep.sweep_stale_export_staging().await;
        })
    }));
    // Agent temp folders persist across an agent's relaunches, so a daemon
    // that runs for days must also reclaim the ones left idle while it runs
    // (deleted agents, agents nobody resumed). Folders a live launch holds
    // are skipped.
    let agent_tmp_sweep_root = intent_core::agent_tmp_root(data_dir);
    sweeps.push(startup_work.spawn(|| {
        intent_core::spawn_daemon(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(60 * 60));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            // The first tick is immediate; startup already swept.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let root = agent_tmp_sweep_root.clone();
                let swept = tokio::task::spawn_blocking(move || {
                    intent_core::sweep_agent_tmp_root(&root, intent_core::AGENT_TMP_MAX_IDLE)
                })
                .await;
                if let Ok(Err(e)) = swept {
                    tracing::warn!(error = %e, "agent temp sweep failed");
                }
            }
        })
    }));
    // Machines that may still run this backend's agents reconnect on their
    // own after a restart, in the background, so a machine that is down
    // never slows the start.
    let services_remote_resume = services.clone();
    sweeps.push(startup_work.spawn(|| {
        intent_core::spawn_daemon(async move {
            services_remote_resume.resume_exposed_machines().await;
        })
    }));
    // Sweep expired attachment idempotency-key bindings (7-day retention,
    // intent-hq/intent#4691); also swept lazily by keyed placements/begins.
    let services_idempotency_sweep = services.clone();
    let attachment_retention = startup_work.spawn(|| {
        intent_core::spawn_daemon(async move {
            services_idempotency_sweep
                .sweep_expired_attachment_idempotency_keys()
                .await;
        })
    });
    // Background PR refresh (§7.6): periodically re-fetch linked PRs (and
    // discover/link PRs for workspaces without one), persist any change, and
    // emit `pr:*` events so clients update without polling.
    // Tiered by workspace recency to trim forge load (§7.7): recently-active
    // workspaces refresh every 180s tick, idle ones only on every 10th tick
    // (~30 min). Safe when source control is unconfigured (a sweep with due
    // workspaces logs and swallows the missing-provider error). Aborted on
    // clean shutdown.
    let pr_refresh =
        orchestration.spawn(|| services.spawn_pr_refresh_loop(std::time::Duration::from_secs(60)));
    // Centralized PR-monitor loop (`ws.pr.monitor`): every `[prMonitor]
    // pollSeconds` (read live, floor 10s), poll the due active monitors —
    // each PR on an effective interval stretched to fit the `[prMonitor]
    // hourlyRequestBudget` cost model (a cadence planner, not a request
    // limiter) and stretched further ahead of exhaustion when the tick's
    // shared quota probe shows the remaining quota would not cover the
    // projected spend to reset within `quotaSharePercent`, a capped
    // oldest-first subset per tick — diff each
    // against its persisted baseline, and deliver one consolidated wake once
    // the PR has been quiet for the debounce window. Safe when source control
    // is unconfigured (the tick logs and returns). Aborted on clean shutdown.
    let pr_monitor_loop = orchestration.spawn(|| services.spawn_pr_monitor_loop());
    // Daemon-internal token-usage scan (§5.23/§19.1): every 300s, re-tally each
    // workspace's per-agent/per-model token usage, persist the durable
    // `tokenUsage` field, and emit `workspace:tokenUsage-changed` on deltas.
    // There is no scan RPC. Aborted on clean shutdown.
    let token_usage_scan = orchestration
        .spawn(|| services.spawn_token_usage_scan_loop(std::time::Duration::from_secs(300)));
    // Completion-delivery worker (AS-3): wake parents holding a
    // completion watch when their delegated child finishes. No-op-safe without
    // an event bus. Held for the process lifetime and aborted on clean shutdown.
    let completion_delivery = orchestration.spawn(|| services.spawn_completion_delivery_loop());
    // Auto-commit-on-idle worker (LNI-1, §5.6): subscribe to `agent:idle` and
    // commit each task-linked agent's changes with `Agent-Id:` and
    // `Linked-Note-Id:` trailers via `git_agent_commit`. No-op-safe without an
    // event bus. Aborted on clean shutdown.
    let auto_commit_loop = orchestration.spawn(|| services.spawn_auto_commit_loop());
    // Push relay publisher: idle until `push.relayEnabled` is on and a phone
    // is linked. Aborted on clean shutdown.
    let push_publisher = orchestration.spawn(|| services.spawn_push_publisher());
    // Idle PR refresh worker: on `agent:idle`, check that workspace's branch
    // PR, git roots, and stale PR monitors once, through the same automatic
    // admission, rate-limit pause, and budgets as the 60 s sweep. No-op-safe
    // without an event bus. Aborted on clean shutdown.
    let idle_pr_refresh_loop = orchestration.spawn(|| services.spawn_idle_pr_refresh_loop());
    // Agent-locks recompute worker (§5.19/§6.5): watch agent lifecycle, task
    // status, auto-commit policy, and tracked-change events and publish the
    // per-workspace `changes:agent-locks` snapshot when it changes. No-op-safe
    // without an event bus. Aborted on clean shutdown.
    let agent_locks_loop = orchestration.spawn(|| services.spawn_agent_locks_loop());
    // Stale tracked-change sweep (intent-hq/intent#7266): shortly after
    // startup and then periodically, delete working-stage attribution rows
    // whose path git reports clean (ignored, deleted, reverted, committed
    // outside the daemon). Aborted on clean shutdown.
    let tracked_changes_sweep = orchestration.spawn(|| services.spawn_tracked_changes_sweep_loop());
    // Script output cleanup: at startup delete the on-disk output of
    // workspaces that no longer exist, then hourly delete run logs older than
    // `scripts.outputRetentionHours` (read live; `0` disables), keeping the
    // current run of any running script. Aborted on clean shutdown.
    let script_output_sweep = orchestration.spawn(|| services.spawn_script_output_sweep_loop());
    // Automatic purge (PROTOCOL §5.1): shortly after startup and then
    // hourly, retire (purge the folders of) workspaces archived longer than
    // `workspace.purgeArchivedAfterDays` (read live; absent = off).
    // Aborted on clean shutdown.
    let auto_retire_loop = orchestration.spawn(|| services.spawn_auto_retire_loop());
    // Automatic archive (PROTOCOL §5.1): shortly after startup and then
    // hourly, archive workspaces idle longer than
    // `workspace.autoArchiveIdleAfterDays` (read live; default 0 = off).
    // Aborted on clean shutdown.
    let auto_archive_loop = orchestration.spawn(|| services.spawn_auto_archive_loop());
    let execution_context_loop = startup_work.spawn(|| services.spawn_execution_context_loop());
    Background {
        attachment_retention,
        sweeps,
        loops: vec![
            pr_refresh,
            pr_monitor_loop,
            token_usage_scan,
            completion_delivery,
            auto_commit_loop,
            push_publisher,
            idle_pr_refresh_loop,
            agent_locks_loop,
            tracked_changes_sweep,
            script_output_sweep,
            auto_retire_loop,
            auto_archive_loop,
            execution_context_loop,
        ],
    }
}

/// The handles of what [`start_background`] started.
pub struct Background {
    /// The boot sweep of expired attachment idempotency keys: finite, and
    /// joined before the store closes.
    pub attachment_retention: JoinHandle<()>,
    /// The other startup sweeps (finite, except the hourly agent temp
    /// sweep): the daemon lets them run until the process ends; a tenant's
    /// self-fence cancels them.
    sweeps: Vec<JoinHandle<()>>,
    /// The recurring loops, in start order.
    loops: Vec<JoinHandle<()>>,
}

impl Background {
    /// How many loops are still running.
    #[must_use]
    pub fn loops_running(&self) -> usize {
        self.loops
            .iter()
            .filter(|handle| !handle.is_finished())
            .count()
    }

    /// Every handle: the sweeps, the attachment sweep and the loops.
    #[must_use]
    pub fn into_tasks(self) -> Vec<JoinHandle<()>> {
        let mut tasks = self.sweeps;
        tasks.push(self.attachment_retention);
        tasks.extend(self.loops);
        tasks
    }

    /// Stop every loop's recurrence, in start order, and wait for each to
    /// end. Each admitted refresh, monitor publication, tally write, or Git
    /// commit retains its finite service writer owner.
    pub async fn stop_loops(&mut self) {
        for handle in self.loops.drain(..) {
            handle.abort();
            let _ = handle.await;
        }
    }
}

/// One tenant's client connections: each registers its close, and the
/// tenant's self-fence closes them all. Picking the tenant a connection
/// belongs to is the listener's job.
#[derive(Clone, Default)]
pub struct ClientConnections(Arc<Mutex<Connections>>);

#[derive(Default)]
struct Connections {
    next: u64,
    open: HashMap<u64, Box<dyn FnOnce() + Send>>,
    closed: bool,
}

/// A registered connection, for [`ClientConnections::remove`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ConnectionId(u64);

impl ClientConnections {
    /// Register a connection's `close`. After [`Self::close_all`] the
    /// connection is closed at once and `None` is returned.
    pub fn register(&self, close: impl FnOnce() + Send + 'static) -> Option<ConnectionId> {
        let mut connections = lock(&self.0);
        if connections.closed {
            drop(connections);
            close();
            return None;
        }
        connections.next += 1;
        let id = connections.next;
        connections.open.insert(id, Box::new(close));
        Some(ConnectionId(id))
    }

    /// Forget a connection that ended on its own.
    pub fn remove(&self, id: ConnectionId) {
        lock(&self.0).open.remove(&id.0);
    }

    /// How many connections are open.
    #[must_use]
    pub fn len(&self) -> usize {
        lock(&self.0).open.len()
    }

    /// Whether none is open.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether [`Self::close_all`] ran.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        lock(&self.0).closed
    }

    /// Close every connection and refuse new ones. Returns how many were
    /// closed.
    #[must_use = "the count says how many connections the close reached"]
    pub fn close_all(&self) -> usize {
        let open: Vec<_> = {
            let mut connections = lock(&self.0);
            connections.closed = true;
            connections.open.drain().map(|(_, close)| close).collect()
        };
        let closed = open.len();
        for close in open {
            close();
        }
        closed
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// One tenant's running work, handed to its self-fence: what
/// [`restore_registries`] and [`start_background`] start, the tenant's
/// client connections and machine links, and its agents and PTYs.
pub struct TenantRuntime {
    connections: ClientConnections,
    tasks: Vec<AbortHandle>,
    loops: Vec<AbortHandle>,
}

impl TenantRuntime {
    /// Restore and start the tenant's work over `services`, as the daemon
    /// does for its one backend, and register all of it on `runner`, the
    /// tenant's self-fence, which stops it in the fence's order: the
    /// client connections, the machine links, then the tasks (every sweep
    /// and loop started here is cancelled, then the PTYs and the agent
    /// manager are shut down).
    pub async fn start(
        services: &Services,
        data_dir: &Path,
        startup_work: &impl Admission,
        orchestration: &impl Admission,
        runner: &mut SelfFence,
    ) -> Self {
        let connections = ClientConnections::default();
        let closing = connections.clone();
        runner.on_client_connections(move || async move {
            let closed = closing.close_all();
            tracing::info!(closed, "self-fence: closed the tenant's client connections");
        });
        let links = services.clone();
        runner.on_machine_links(move || async move {
            if let Some(feature) = crate::remote::RemoteServicesExt::remote_feature(&links) {
                feature.close_links();
            }
        });
        restore_registries(services, startup_work, orchestration).await;
        let background = start_background(services, data_dir, startup_work, orchestration);
        let loops = background
            .loops
            .iter()
            .map(JoinHandle::abort_handle)
            .collect();
        let mut tasks = Vec::new();
        for task in background.into_tasks() {
            tasks.push(task.abort_handle());
            runner.adopt_task(task);
        }
        let stopping = services.clone();
        runner.on_tasks(move || async move {
            stopping.shutdown_pty_sessions().await;
            if let Some(manager) = stopping.agent_manager() {
                manager.shutdown().await;
            }
        });
        Self {
            connections,
            tasks,
            loops,
        }
    }

    /// The tenant's client connections.
    #[must_use]
    pub fn connections(&self) -> &ClientConnections {
        &self.connections
    }

    /// How many of the tenant's tasks are still running.
    #[must_use]
    pub fn tasks_running(&self) -> usize {
        self.tasks.iter().filter(|task| !task.is_finished()).count()
    }

    /// How many of the tenant's loops are still running.
    #[must_use]
    pub fn loops_running(&self) -> usize {
        self.loops.iter().filter(|task| !task.is_finished()).count()
    }
}
