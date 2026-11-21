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
use std::sync::{Arc, Mutex};
use std::time::Instant;

use intent_core::Result;
use intent_providers::UnslothEndpoint;
use tokio::sync::Mutex as TokioMutex;

use super::{
    best_fitting_quant, fetch_repo_file_listing, lock_ignore_poison, parse_repo_quant_sizes,
    shutting_down_error, StatusCallback, StatusLevel, UnslothConfig, UnslothServerManager,
    UnslothStatus,
};

/// Expected resident size as a multiple of the selected GGUF file size.
const FOOTPRINT_FACTOR: u64 = 3;

/// One running server's share of the memory budget.
#[derive(Debug, Clone)]
pub(crate) struct Load {
    pub repo_id: String,
    pub bytes: u64,
    /// Agents use it, or it is still starting for a waiting spawn.
    pub busy: bool,
    pub last_used: Instant,
}

/// The servers to stop so a model needing `need` bytes fits in `budget`, in
/// stop order, each with whether it is busy. Idle servers go first, least
/// recently used first, then busy ones. An unknown budget or need stops all.
pub(crate) fn plan_evictions(
    budget: Option<u64>,
    need: Option<u64>,
    loads: &[Load],
) -> Vec<(String, bool)> {
    let mut order: Vec<&Load> = loads.iter().collect();
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
    /// after a re-port around a foreign occupant).
    port: u16,
    last_used: Instant,
}

/// The daemon's managed Unsloth servers, one per model.
pub(crate) struct UnslothServerPool {
    base: Arc<UnslothConfig>,
    servers: Mutex<HashMap<String, Entry>>,
    /// Serializes planning, stopping and starting, so two new models never
    /// race for memory or ports. Never held across a model's startup wait.
    admission: TokioMutex<()>,
    /// Expected footprint per model (successful lookups only).
    footprints: Mutex<HashMap<String, u64>>,
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

    fn lookup(&self, repo_id: &str) -> Option<Arc<UnslothServerManager>> {
        let mut servers = lock_ignore_poison(&self.servers);
        let entry = servers.get_mut(repo_id)?;
        entry.last_used = Instant::now();
        Some(entry.manager.clone())
    }

    fn budget(&self) -> Option<u64> {
        (self.base.total_memory_bytes)().map(crate::provider_models::ram_budget_bytes)
    }

    /// Expected footprint of `repo_id`, or `None` when its size is unknown.
    async fn footprint(&self, repo_id: &str) -> Option<u64> {
        if let Some(bytes) = lock_ignore_poison(&self.footprints).get(repo_id) {
            return Some(*bytes);
        }
        let body = fetch_repo_file_listing(&self.base, repo_id).await.ok()?;
        let sizes = parse_repo_quant_sizes(&body);
        let quant = best_fitting_quant(&sizes, (self.base.total_memory_bytes)())?;
        let bytes = sizes.get(&quant)?.saturating_mul(FOOTPRINT_FACTOR);
        lock_ignore_poison(&self.footprints).insert(repo_id.to_string(), bytes);
        Some(bytes)
    }

    /// Loads of the daemon-started servers other than `repo_id`, dropping
    /// entries whose server is gone. Adopted servers are not ours to stop.
    async fn loads(&self, repo_id: &str, attached: &HashMap<String, usize>) -> Vec<Load> {
        let entries: Vec<(String, Arc<UnslothServerManager>, Instant)> =
            lock_ignore_poison(&self.servers)
                .iter()
                .filter(|(repo, _)| repo.as_str() != repo_id)
                .map(|(repo, entry)| (repo.clone(), entry.manager.clone(), entry.last_used))
                .collect();
        let mut loads = Vec::new();
        for (repo, manager, last_used) in entries {
            let Some(status) = manager.status_snapshot().await else {
                self.forget(&repo, &manager);
                continue;
            };
            if status.pid.is_none() {
                continue;
            }
            let expected = lock_ignore_poison(&self.footprints)
                .get(&repo)
                .copied()
                .unwrap_or(0);
            loads.push(Load {
                bytes: status.memory_bytes.max(expected),
                busy: status.phase != "ready" || attached.get(&repo).is_some_and(|n| *n > 0),
                repo_id: repo,
                last_used,
            });
        }
        loads
    }

    /// Drop `repo_id`'s entry if it still holds `manager`.
    fn forget(&self, repo_id: &str, manager: &Arc<UnslothServerManager>) {
        let mut servers = lock_ignore_poison(&self.servers);
        if servers
            .get(repo_id)
            .is_some_and(|entry| Arc::ptr_eq(&entry.manager, manager))
        {
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
        if let Some(manager) = self.lookup(repo_id) {
            if manager.status_snapshot().await.is_some() {
                return Vec::new();
            }
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
        let manager = match self.lookup(repo_id) {
            Some(manager) => manager,
            None => self.admit(repo_id, attached, status).await?,
        };
        let result = manager
            .ensure_endpoint(repo_id, unsloth_cli_override, 0, status)
            .await;
        if result.is_err() && manager.status_snapshot().await.is_none() {
            self.forget(repo_id, &manager);
        }
        result
    }

    /// Make room for `repo_id` and register its (not yet started) manager.
    async fn admit(
        &self,
        repo_id: &str,
        attached: &HashMap<String, usize>,
        status: &StatusCallback,
    ) -> Result<Arc<UnslothServerManager>> {
        let need = self.footprint(repo_id).await;
        let _admission = self.admission.lock().await;
        if self.is_shutting_down() {
            return Err(shutting_down_error());
        }
        if let Some(manager) = self.lookup(repo_id) {
            return Ok(manager);
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
                entry.manager.stop().await;
            }
        }
        let port = self.next_port();
        let manager = Arc::new(UnslothServerManager::with_config(self.config_for(port)));
        lock_ignore_poison(&self.servers).insert(
            repo_id.to_string(),
            Entry {
                manager: manager.clone(),
                port,
                last_used: Instant::now(),
            },
        );
        Ok(manager)
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

    /// Stop every server; `false` when none was running.
    pub(crate) async fn stop_all(&self) -> bool {
        let _admission = self.admission.lock().await;
        let entries: Vec<Entry> = lock_ignore_poison(&self.servers)
            .drain()
            .map(|(_, entry)| entry)
            .collect();
        let mut stopped = false;
        for entry in entries {
            stopped |= entry.manager.stop().await;
        }
        stopped
    }

    /// Stop every server for good (daemon shutdown).
    pub(crate) async fn shutdown(&self) {
        self.shutting_down
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let entries: Vec<Entry> = lock_ignore_poison(&self.servers)
            .drain()
            .map(|(_, entry)| entry)
            .collect();
        for entry in entries {
            entry.manager.shutdown().await;
        }
    }

    fn is_shutting_down(&self) -> bool {
        self.shutting_down
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn seed_footprint_for_test(&self, repo_id: &str, bytes: u64) {
        lock_ignore_poison(&self.footprints).insert(repo_id.to_string(), bytes);
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
            },
        );
    }
}

#[cfg(test)]
mod tests;
