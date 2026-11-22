//! One managed Unsloth server per model, within a memory budget.
//!
//! Unsloth supports several of its servers at once (per-port pid records;
//! keys stored per server URL). The pool keeps one [`UnslothServerManager`]
//! per model, each on its own port: the first takes the configured port, so
//! adoption of a user-run server still works, and later ones a free port.
//!
//! Before starting a model the pool checks memory. A running server counts
//! as the larger of its measured process-tree memory and its expected
//! footprint ([`FOOTPRINT_FACTOR`] times its GGUF size: KV cache at the
//! model's default context made the resident size about 2.5-3x the Q4 file
//! in the 2026-11 spike). When the new model does not fit, idle servers stop
//! first, least recently used first, then busy ones. With an unknown budget
//! or model size the pool keeps one server, as the single-server manager did.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use intent_core::Result;
use intent_providers::UnslothEndpoint;
use tokio::sync::Mutex as TokioMutex;

use super::{
    best_fitting_quant, fetch_repo_file_listing, is_shutting_down_abort, lock_ignore_poison,
    parse_repo_quant_sizes, server_stopped_error, shutting_down_error, StatusCallback, StatusLevel,
    UnslothConfig, UnslothServerManager, UnslothStatus,
};

/// Expected resident size as a multiple of the selected GGUF file size.
const FOOTPRINT_FACTOR: u64 = 3;

/// How long an unknown model size is remembered, so the send-time check and
/// the start that follows plan from the same answer.
const UNKNOWN_FOOTPRINT_TTL: Duration = Duration::from_secs(300);

/// How long a daemon-started server may sit with no live agents before it
/// stops to free its memory.
pub(crate) const UNSLOTH_IDLE_STOP_AFTER: Duration = Duration::from_secs(15 * 60);

/// How often the agent manager checks for idle servers.
pub(crate) const UNSLOTH_IDLE_CHECK_EVERY: Duration = Duration::from_secs(60);

/// One running server's share of the memory budget.
#[derive(Debug, Clone)]
pub(crate) struct Load {
    pub repo_id: String,
    pub bytes: u64,
    /// Agents use it, or it is still starting for a waiting spawn.
    pub busy: bool,
    /// The daemon may stop it (not an adopted server).
    pub evictable: bool,
    pub last_used: Instant,
}

/// Whether a failed start was aborted because the pool stopped its server
/// (a stop or an eviction) while the daemon keeps running. Any other error,
/// including a real failure that a later stop raced, keeps its message.
fn stopped_by_pool(
    err: &intent_core::Error,
    manager_shut_down: bool,
    pool_shutting_down: bool,
) -> bool {
    manager_shut_down && !pool_shutting_down && is_shutting_down_abort(err)
}

/// The servers to stop so a model needing `need` bytes fits in `budget`, in
/// stop order, each with whether it is busy. Idle servers go first, least
/// recently used first, then busy ones. An unknown budget or need stops all
/// evictable servers. Servers the daemon may not stop still use memory.
pub(crate) fn plan_evictions(
    budget: Option<u64>,
    need: Option<u64>,
    loads: &[Load],
) -> Vec<(String, bool)> {
    let mut order: Vec<&Load> = loads.iter().filter(|load| load.evictable).collect();
    order.sort_by_key(|load| (load.busy, load.last_used));
    let (Some(budget), Some(need)) = (budget, need) else {
        return order
            .into_iter()
            .map(|load| (load.repo_id.clone(), load.busy))
            .collect();
    };
    let mut used: u64 = loads.iter().map(|load| load.bytes).sum();
    let mut plan = Vec::new();
    for load in order {
        if used.saturating_add(need) <= budget {
            break;
        }
        used = used.saturating_sub(load.bytes);
        plan.push((load.repo_id.clone(), load.busy));
    }
    plan
}

struct Entry {
    manager: Arc<UnslothServerManager>,
    /// The port this server was configured with (its actual port may differ
    /// after a re-port around a foreign occupant). Reserved while the entry
    /// exists, including before its server has spawned.
    port: u16,
    last_used: Instant,
    /// When the idle check first saw this server with no agents and no
    /// start; cleared when one joins.
    idle_since: Option<Instant>,
    /// `ensure_endpoint` calls running through this entry. While one runs,
    /// the entry is a starting server even before it reports a status.
    in_flight: Arc<AtomicUsize>,
}

/// One `ensure_endpoint` call running through an entry.
struct InFlight(Arc<AtomicUsize>);

impl Drop for InFlight {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// The daemon's managed Unsloth servers, one per model.
pub(crate) struct UnslothServerPool {
    base: Arc<UnslothConfig>,
    servers: Mutex<HashMap<String, Entry>>,
    /// Serializes planning, stopping and starting, so two new models never
    /// race for memory or ports. Never held across a model's startup wait.
    admission: TokioMutex<()>,
    /// Expected footprint per model; an unknown size is kept for
    /// [`UNKNOWN_FOOTPRINT_TTL`].
    footprints: Mutex<HashMap<String, (Option<u64>, Instant)>>,
    shutting_down: std::sync::atomic::AtomicBool,
}

impl Default for UnslothServerPool {
    fn default() -> Self {
        Self::with_config(UnslothConfig::default())
    }
}

impl UnslothServerPool {
    pub(crate) fn with_config(config: UnslothConfig) -> Self {
        Self {
            base: Arc::new(config),
            servers: Mutex::new(HashMap::new()),
            admission: TokioMutex::new(()),
            footprints: Mutex::new(HashMap::new()),
            shutting_down: std::sync::atomic::AtomicBool::new(false),
        }
    }

    /// A per-model config on `port` that delegates every seam to the base.
    fn config_for(&self, port: u16) -> UnslothConfig {
        let base = &self.base;
        let resolve = base.clone();
        let memory = base.clone();
        let free = base.clone();
        let pick = base.clone();
        UnslothConfig {
            resolve_binary: Box::new(move |explicit| (resolve.resolve_binary)(explicit)),
            home_dir: base.home_dir.clone(),
            port,
            server_up_timeout: base.server_up_timeout,
            model_ready_timeout: base.model_ready_timeout,
            probe_interval: base.probe_interval,
            mint_timeout: base.mint_timeout,
            adopt_mint_timeout: base.adopt_mint_timeout,
            hf_api_base: base.hf_api_base.clone(),
            hf_files_timeout: base.hf_files_timeout,
            total_memory_bytes: Box::new(move || (memory.total_memory_bytes)()),
            port_is_free: Box::new(move |port| (free.port_is_free)(port)),
            pick_free_port: Box::new(move || (pick.pick_free_port)()),
            mint_lock: base.mint_lock.clone(),
        }
    }

    /// Join `repo_id`'s entry when its server is alive or a start that went
    /// through admission is in flight: count this call in flight and mark the
    /// entry used. Decided and counted under one lock, so a crashed server is
    /// never joined: every caller for it goes through [`Self::admit`].
    fn join(&self, repo_id: &str) -> Option<(Arc<UnslothServerManager>, InFlight)> {
        let mut servers = lock_ignore_poison(&self.servers);
        let entry = servers.get_mut(repo_id)?;
        if entry.in_flight.load(Ordering::SeqCst) == 0 && !entry.manager.has_live_server() {
            return None;
        }
        entry.last_used = Instant::now();
        entry.idle_since = None;
        entry.in_flight.fetch_add(1, Ordering::SeqCst);
        Some((entry.manager.clone(), InFlight(entry.in_flight.clone())))
    }

    /// Whether `repo_id` has a running or starting server. Read-only.
    async fn is_up(&self, repo_id: &str) -> bool {
        let entry = lock_ignore_poison(&self.servers).get(repo_id).map(|entry| {
            (
                entry.manager.clone(),
                entry.in_flight.load(Ordering::SeqCst),
            )
        });
        match entry {
            Some((_, in_flight)) if in_flight > 0 => true,
            Some((manager, _)) => manager.status_snapshot().await.is_some(),
            None => false,
        }
    }

    fn budget(&self) -> Option<u64> {
        (self.base.total_memory_bytes)().map(crate::provider_models::ram_budget_bytes)
    }

    /// Expected footprint of `repo_id`, or `None` when its size is unknown.
    /// Known sizes are kept; an unknown one for [`UNKNOWN_FOOTPRINT_TTL`].
    async fn footprint(&self, repo_id: &str) -> Option<u64> {
        if let Some((bytes, at)) = lock_ignore_poison(&self.footprints).get(repo_id) {
            if bytes.is_some() || at.elapsed() < UNKNOWN_FOOTPRINT_TTL {
                return *bytes;
            }
        }
        let bytes = self.lookup_footprint(repo_id).await;
        lock_ignore_poison(&self.footprints).insert(repo_id.to_string(), (bytes, Instant::now()));
        bytes
    }

    async fn lookup_footprint(&self, repo_id: &str) -> Option<u64> {
        let body = fetch_repo_file_listing(&self.base, repo_id).await.ok()?;
        let sizes = parse_repo_quant_sizes(&body);
        let quant = best_fitting_quant(&sizes, (self.base.total_memory_bytes)())?;
        Some(sizes.get(&quant)?.saturating_mul(FOOTPRINT_FACTOR))
    }

    fn known_footprint(&self, repo_id: &str) -> Option<u64> {
        lock_ignore_poison(&self.footprints)
            .get(repo_id)
            .and_then(|(bytes, _)| *bytes)
    }

    /// Loads of the servers other than `repo_id`. A starting server counts
    /// as busy at its expected size; an adopted one uses memory but is not
    /// ours to stop. Entries whose server is gone and idle are dropped.
    async fn loads(&self, repo_id: &str, attached: &HashMap<String, usize>) -> Vec<Load> {
        type Snapshot = (String, Arc<UnslothServerManager>, Instant, Arc<AtomicUsize>);
        let entries: Vec<Snapshot> = lock_ignore_poison(&self.servers)
            .iter()
            .filter(|(repo, _)| repo.as_str() != repo_id)
            .map(|(repo, entry)| {
                (
                    repo.clone(),
                    entry.manager.clone(),
                    entry.last_used,
                    entry.in_flight.clone(),
                )
            })
            .collect();
        let mut loads = Vec::new();
        for (repo, manager, last_used, in_flight) in entries {
            let expected = self.known_footprint(&repo).unwrap_or(0);
            let has_agents = attached.get(&repo).is_some_and(|n| *n > 0);
            let load = match manager.status_snapshot().await {
                None if in_flight.load(Ordering::SeqCst) > 0 => Load {
                    repo_id: repo,
                    bytes: expected,
                    busy: true,
                    evictable: true,
                    last_used,
                },
                None => {
                    self.forget_if_idle(&repo, &manager);
                    continue;
                }
                Some(status) if status.pid.is_none() => Load {
                    repo_id: repo,
                    bytes: expected,
                    busy: true,
                    evictable: false,
                    last_used,
                },
                Some(status) => Load {
                    repo_id: repo,
                    bytes: status.memory_bytes.max(expected),
                    busy: status.phase != "ready" || has_agents,
                    evictable: true,
                    last_used,
                },
            };
            loads.push(load);
        }
        loads
    }

    /// Drop `repo_id`'s entry if it still holds `manager` and no call runs
    /// through it.
    fn forget_if_idle(&self, repo_id: &str, manager: &Arc<UnslothServerManager>) {
        let mut servers = lock_ignore_poison(&self.servers);
        if servers.get(repo_id).is_some_and(|entry| {
            Arc::ptr_eq(&entry.manager, manager) && entry.in_flight.load(Ordering::SeqCst) == 0
        }) {
            servers.remove(repo_id);
        }
    }

    /// Models with attached agents that starting `repo_id` would stop. Empty
    /// when `repo_id` already runs or fits. Stops nothing.
    pub(crate) async fn plan_for(
        &self,
        repo_id: &str,
        attached: &HashMap<String, usize>,
    ) -> Vec<String> {
        if self.is_up(repo_id).await {
            return Vec::new();
        }
        let loads = self.loads(repo_id, attached).await;
        if loads.is_empty() {
            return Vec::new();
        }
        let need = self.footprint(repo_id).await;
        plan_evictions(self.budget(), need, &loads)
            .into_iter()
            .filter(|(repo, _)| attached.get(repo).is_some_and(|n| *n > 0))
            .map(|(repo, _)| repo)
            .collect()
    }

    /// Ensure a server for `repo_id` and return its endpoint, starting one
    /// (and stopping others to make room) when needed. `attached` counts the
    /// live agents per model, for the warning before a busy server stops.
    pub(crate) async fn ensure_endpoint(
        &self,
        repo_id: &str,
        unsloth_cli_override: Option<&str>,
        attached: &HashMap<String, usize>,
        status: &StatusCallback,
    ) -> Result<UnslothEndpoint> {
        if self.is_shutting_down() {
            return Err(shutting_down_error());
        }
        // A missing or crashed server starts through the memory check.
        let (manager, in_flight) = match self.join(repo_id) {
            Some(joined) => joined,
            None => self.admit(repo_id, attached, status).await?,
        };
        let result = manager
            .ensure_endpoint(repo_id, unsloth_cli_override, 0, status)
            .await;
        drop(in_flight);
        if result.is_err() && !manager.has_live_server() {
            self.forget_if_idle(repo_id, &manager);
        }
        match result {
            Err(err)
                if stopped_by_pool(&err, manager.is_shutting_down(), self.is_shutting_down()) =>
            {
                Err(server_stopped_error())
            }
            result => result,
        }
    }

    /// Make room for `repo_id` and register its (not yet started) manager,
    /// with this call counted in flight.
    async fn admit(
        &self,
        repo_id: &str,
        attached: &HashMap<String, usize>,
        status: &StatusCallback,
    ) -> Result<(Arc<UnslothServerManager>, InFlight)> {
        let need = self.footprint(repo_id).await;
        let _admission = self.admission.lock().await;
        if self.is_shutting_down() {
            return Err(shutting_down_error());
        }
        if let Some(joined) = self.join(repo_id) {
            return Ok(joined);
        }
        let dead = lock_ignore_poison(&self.servers)
            .get(repo_id)
            .map(|entry| entry.manager.clone());
        if let Some(manager) = dead {
            self.forget_if_idle(repo_id, &manager);
        }
        let loads = self.loads(repo_id, attached).await;
        for (repo, busy) in plan_evictions(self.budget(), need, &loads) {
            let agents = attached.get(&repo).copied().unwrap_or(0);
            if busy && agents > 0 {
                let plural = if agents == 1 { "" } else { "s" };
                status(
                    StatusLevel::Warning,
                    format!(
                        "Starting Unsloth model {repo_id} needs memory, so the server for {repo} stops — {agents} live agent session{plural} on {repo} will lose the loaded model"
                    ),
                );
            }
            let removed = lock_ignore_poison(&self.servers).remove(&repo);
            if let Some(entry) = removed {
                tracing::info!(stopped = %repo, starting = %repo_id, "stopping unsloth server to make room");
                // Terminal for this manager: also aborts a start in flight.
                entry.manager.shutdown().await;
            }
        }
        let port = self.next_port();
        let manager = Arc::new(UnslothServerManager::with_config(self.config_for(port)));
        let in_flight = Arc::new(AtomicUsize::new(1));
        let mut servers = lock_ignore_poison(&self.servers);
        // Checked under the map lock: `shutdown` sets the flag before it
        // drains the map, so an entry inserted here is always drained.
        if self.is_shutting_down() {
            return Err(shutting_down_error());
        }
        servers.insert(
            repo_id.to_string(),
            Entry {
                manager: manager.clone(),
                port,
                last_used: Instant::now(),
                idle_since: None,
                in_flight: in_flight.clone(),
            },
        );
        Ok((manager, InFlight(in_flight)))
    }

    /// The configured port when no server uses it, otherwise a free one.
    fn next_port(&self) -> u16 {
        let configured = self.base.port;
        let taken = lock_ignore_poison(&self.servers)
            .values()
            .any(|entry| entry.port == configured);
        if taken {
            (self.base.pick_free_port)().unwrap_or(configured)
        } else {
            configured
        }
    }

    /// Every running server, most recently used first.
    pub(crate) async fn statuses(&self) -> Vec<UnslothStatus> {
        let mut entries: Vec<(Arc<UnslothServerManager>, Instant)> =
            lock_ignore_poison(&self.servers)
                .values()
                .map(|entry| (entry.manager.clone(), entry.last_used))
                .collect();
        entries.sort_by_key(|entry| std::cmp::Reverse(entry.1));
        let mut statuses = Vec::new();
        for (manager, _) in entries {
            if let Some(status) = manager.status_snapshot().await {
                statuses.push(status);
            }
        }
        statuses
    }

    /// Stop every server, including starts in flight; `false` when none
    /// was running or starting.
    pub(crate) async fn stop_all(&self) -> bool {
        let _admission = self.admission.lock().await;
        let entries = self.drain();
        let mut stopped = false;
        for entry in &entries {
            stopped |= entry.in_flight.load(Ordering::SeqCst) > 0
                || entry.manager.status_snapshot().await.is_some();
        }
        Self::shut_down_all(entries).await;
        stopped
    }

    /// Stop every daemon-started, ready server that no live agent and no
    /// start has used for `after`, as of `now`; returns their models. An
    /// adopted, loading or starting server is never idle.
    pub(crate) async fn reap_idle(
        &self,
        attached: &HashMap<String, usize>,
        now: Instant,
        after: Duration,
    ) -> Vec<String> {
        let _admission = self.admission.lock().await;
        if self.is_shutting_down() {
            return Vec::new();
        }
        let entries: Vec<(String, Arc<UnslothServerManager>)> = lock_ignore_poison(&self.servers)
            .iter()
            .map(|(repo, entry)| (repo.clone(), entry.manager.clone()))
            .collect();
        let mut idle = Vec::new();
        for (repo, manager) in entries {
            let unused = attached.get(&repo).is_none_or(|count| *count == 0);
            let ours_and_ready = manager
                .status_snapshot()
                .await
                .is_some_and(|status| status.pid.is_some() && status.phase == "ready");
            idle.push((repo, manager, unused && ours_and_ready));
        }

        let mut stopped = Vec::new();
        {
            let mut servers = lock_ignore_poison(&self.servers);
            for (repo, manager, unused) in idle {
                let Some(entry) = servers.get_mut(&repo) else {
                    continue;
                };
                // A join since the snapshot counts as use.
                let idle_now = unused
                    && Arc::ptr_eq(&entry.manager, &manager)
                    && entry.in_flight.load(Ordering::SeqCst) == 0;
                if !idle_now {
                    entry.idle_since = None;
                    continue;
                }
                let since = *entry.idle_since.get_or_insert(now);
                if now.saturating_duration_since(since) >= after {
                    if let Some(entry) = servers.remove(&repo) {
                        stopped.push((repo, entry));
                    }
                }
            }
        }
        let repos = stopped.iter().map(|(repo, _)| repo.clone()).collect();
        for (repo, _) in &stopped {
            tracing::info!(repo = %repo, "stopping idle managed unsloth server");
        }
        Self::shut_down_all(stopped.into_iter().map(|(_, entry)| entry).collect()).await;
        repos
    }

    /// Stop the server for one model, including a start in flight; `false`
    /// when that model had no server. The others keep running.
    pub(crate) async fn stop_model(&self, repo_id: &str) -> bool {
        let _admission = self.admission.lock().await;
        let Some(entry) = lock_ignore_poison(&self.servers).remove(repo_id) else {
            return false;
        };
        let stopped = entry.in_flight.load(Ordering::SeqCst) > 0
            || entry.manager.status_snapshot().await.is_some();
        Self::shut_down_all(vec![entry]).await;
        stopped
    }

    /// Stop every server for good (daemon shutdown).
    pub(crate) async fn shutdown(&self) {
        self.shutting_down
            .store(true, std::sync::atomic::Ordering::SeqCst);
        Self::shut_down_all(self.drain()).await;
    }

    fn drain(&self) -> Vec<Entry> {
        lock_ignore_poison(&self.servers)
            .drain()
            .map(|(_, entry)| entry)
            .collect()
    }

    /// Signal every manager before waiting on any, so a start waiting behind
    /// another server's mint aborts instead of holding up the rest.
    async fn shut_down_all(entries: Vec<Entry>) {
        for entry in &entries {
            entry.manager.request_shutdown();
        }
        for entry in entries {
            entry.manager.shutdown().await;
        }
    }

    fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(std::sync::atomic::Ordering::SeqCst)
    }

    #[cfg(all(test, unix))]
    pub(crate) fn seed_footprint_for_test(&self, repo_id: &str, bytes: u64) {
        lock_ignore_poison(&self.footprints)
            .insert(repo_id.to_string(), (Some(bytes), Instant::now()));
    }

    /// Register a fake running server for `repo_id` (owned or adopted).
    #[cfg(all(test, unix))]
    pub(crate) fn insert_running_for_test(&self, repo_id: &str, owned: bool, ready: bool) {
        let port = self.next_port();
        let manager = Arc::new(UnslothServerManager::with_config(self.config_for(port)));
        if owned {
            manager.set_owned_server_for_test(repo_id);
        } else {
            manager.set_adopted_server_for_test(repo_id);
        }
        if ready {
            manager.set_phase(Some("ready"));
        }
        lock_ignore_poison(&self.servers).insert(
            repo_id.to_string(),
            Entry {
                manager,
                port,
                last_used: Instant::now(),
                idle_since: None,
                in_flight: Arc::new(AtomicUsize::new(0)),
            },
        );
    }

    /// Register a server that has not spawned yet, with a start in flight.
    #[cfg(all(test, unix))]
    pub(crate) fn insert_starting_for_test(&self, repo_id: &str) -> u16 {
        let port = self.next_port();
        let manager = Arc::new(UnslothServerManager::with_config(self.config_for(port)));
        lock_ignore_poison(&self.servers).insert(
            repo_id.to_string(),
            Entry {
                manager,
                port,
                last_used: Instant::now(),
                idle_since: None,
                in_flight: Arc::new(AtomicUsize::new(1)),
            },
        );
        port
    }

    /// Whether `repo_id`'s server is alive (reaps a dead child).
    #[cfg(all(test, unix))]
    pub(crate) fn server_is_live_for_test(&self, repo_id: &str) -> bool {
        lock_ignore_poison(&self.servers)
            .get(repo_id)
            .is_some_and(|entry| entry.manager.has_live_server())
    }

    #[cfg(all(test, unix))]
    pub(crate) fn tracks_for_test(&self, repo_id: &str) -> bool {
        lock_ignore_poison(&self.servers).contains_key(repo_id)
    }
}

#[cfg(test)]
mod tests;
