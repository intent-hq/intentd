//! Agent orchestration: multiplex many concurrent agents with a lifecycle /
//! concurrency [`ProcessRegistry`] and a concrete [`EventSink`] over the M2
//! event bus (§6.8).
//!
//! [`AgentManager`] owns one [`AgentHandle`] per [`AgentId`] (the spawned child,
//! its ACP [`Connection`], the streaming-notification receiver, and the
//! client-served request loop). Each connection carries its own JSON-RPC id
//! space + pending-request map (`intent-acp`), so response correlation is
//! per-connection and the manager keys everything by `AgentId` — the stable
//! analog of the TS registry's `pid`. The [`ProcessRegistry`] ports
//! `agent-process-registry` (acquire/register/markActive/markIdle/deregister +
//! a global concurrency cap with LRU idle eviction); full timer/memory-pressure
//! reaping is M5, exposed here as the [`AgentManager::reap_idle`] hook.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use intent_acp::session::{ContentBlock, StopReason};
use intent_acp::{
    build_baseline_mcp_env_from_process, handshake, serve_workspace_mcp_tcp, spawn_provider,
    to_auggie_mcp_config, ClientRequestHandler, Connection, ConnectionHooks, EventSink,
    FileService, IncomingNotification, IncomingRequest, McpBridge, NormalizedMcpServer,
    NormalizedMcpServers, PermissionOutcome, PermissionPolicy, PermissionRegistry,
    PermissionRequestData, SinkEvent, SpawnOptions, WorkspaceMcpServer,
};
use intent_core::events::AGENT_STATUS_CHANGED;
use intent_core::{
    now_iso, ActorType, AgentId, AgentSession, AgentStatus, BoxFuture, Error, EventActor, Result,
    WorkspaceApi, WorkspaceAttention, WorkspaceId,
};
use intent_providers::ProviderConfig;
use intent_store::{NewEvent, NewTrackedChange};
use serde_json::{json, Value};
use tokio::process::Child;
use tokio::sync::{mpsc, Mutex as TokioMutex};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::agent_ops::new_message_id;

use crate::events::EventBus;
use crate::Services;

#[cfg(test)]
mod tests;

const GB: u64 = 1024 * 1024 * 1024;

/// Conservative cap used when total system memory cannot be determined.
const DEFAULT_PROCESS_CAP: usize = 8;

/// Maximum concurrent agent processes for `total_memory_bytes`, ported verbatim
/// from `agent-process-registry.computeProcessCap` (lower-RAM machines get a
/// tighter cap so the daemon does not overwhelm the system).
pub fn compute_process_cap(total_memory_bytes: u64) -> usize {
    if total_memory_bytes <= 8 * GB {
        4
    } else if total_memory_bytes <= 16 * GB {
        8
    } else if total_memory_bytes <= 32 * GB {
        20
    } else if total_memory_bytes <= 64 * GB {
        30
    } else {
        100
    }
}

/// Best-effort process cap from detected system RAM, falling back to
/// [`DEFAULT_PROCESS_CAP`] when total memory is unknown (RAM detection is
/// currently Linux-only; broader detection is deferred).
pub fn default_process_cap() -> usize {
    match total_memory_bytes() {
        Some(bytes) => compute_process_cap(bytes),
        None => DEFAULT_PROCESS_CAP,
    }
}

#[cfg(target_os = "linux")]
fn total_memory_bytes() -> Option<u64> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in meminfo.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            let kb: u64 = rest.trim().trim_end_matches("kB").trim().parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

#[cfg(not(target_os = "linux"))]
fn total_memory_bytes() -> Option<u64> {
    None
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Async callback that tears down one process when the registry evicts/reaps it
/// (the Rust analog of the TS `ProcessEntry.kill`). The manager wires this to
/// drop the agent's [`AgentHandle`], killing the child and aborting its tasks.
pub type KillFn = Arc<dyn Fn() -> BoxFuture<'static, ()> + Send + Sync>;

struct ProcessEntry {
    last_active_ms: u64,
    is_active: bool,
    kill: KillFn,
}

#[derive(Default)]
struct RegistryInner {
    entries: HashMap<AgentId, ProcessEntry>,
    wait_queue: Vec<tokio::sync::oneshot::Sender<()>>,
}

fn pop_waiter(inner: &mut RegistryInner) -> Option<tokio::sync::oneshot::Sender<()>> {
    if inner.wait_queue.is_empty() {
        None
    } else {
        Some(inner.wait_queue.remove(0))
    }
}

fn lru_idle(inner: &RegistryInner) -> Option<(AgentId, KillFn)> {
    inner
        .entries
        .iter()
        .filter(|(_, e)| !e.is_active)
        .min_by_key(|(_, e)| e.last_active_ms)
        .map(|(id, e)| (id.clone(), e.kill.clone()))
}

/// Idle entries whose `last_active_ms` is at/older than `cutoff_ms`, ordered
/// least-recently-used first (the TTL idle-reap candidate list).
fn idle_older_than(inner: &RegistryInner, cutoff_ms: u64) -> Vec<(AgentId, KillFn)> {
    let mut candidates: Vec<(AgentId, u64, KillFn)> = inner
        .entries
        .iter()
        .filter(|(_, e)| !e.is_active && e.last_active_ms <= cutoff_ms)
        .map(|(id, e)| (id.clone(), e.last_active_ms, e.kill.clone()))
        .collect();
    candidates.sort_by_key(|(_, ms, _)| *ms);
    candidates
        .into_iter()
        .map(|(id, _, kill)| (id, kill))
        .collect()
}

/// The concrete service-layer [`EventSink`]: the bridge `intent-acp`'s
/// client-served request handler publishes its `file:changed` /
/// `agent:permission:*` events through, appended + broadcast over the M2
/// [`EventBus`] (the sink stamps the timestamp; §6.7/§10).
pub struct BusEventSink {
    bus: EventBus,
}

impl BusEventSink {
    /// Wire a sink over the shared event bus.
    pub fn new(bus: EventBus) -> Self {
        Self { bus }
    }
}

impl EventSink for BusEventSink {
    fn publish(&self, event: SinkEvent) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            // An agent `file:changed` also feeds the BE-internal code-review
            // pipeline (§17.1): record diff + attribution after the event lands.
            // The wiring composes `diffs` + `file_tracking` here so neither
            // service module depends on the other (§3.2).
            let track_ctx = (event.event_type == intent_core::events::FILE_CHANGED
                && event.actor.actor_type == ActorType::Agent)
                .then(|| {
                    (
                        event.workspace_id.clone(),
                        event.actor.id.clone(),
                        event.session_id.clone(),
                        event.data.clone(),
                    )
                });

            let new_event = NewEvent {
                workspace_id: event.workspace_id,
                timestamp: now_iso(),
                event_type: event.event_type,
                actor: event.actor,
                session_id: event.session_id,
                correlation_id: None,
                parent_event_id: None,
                metadata: None,
                data: event.data,
            };
            if let Err(e) = self.bus.publish(&new_event).await {
                tracing::warn!(error = %e, "failed to publish agent client event");
            }

            if let Some((workspace_id, agent_id, session_id, data)) = track_ctx {
                self.record_agent_file_change(workspace_id, agent_id, session_id, data)
                    .await;
            }
        })
    }
}

impl BusEventSink {
    /// Record the code-review state for one agent `file:changed` (§17.3/§17.4):
    /// compute + persist the file's diff, then upsert its attribution row on
    /// `tracked_changes` (stage `unstaged`). Best-effort — every failure is
    /// logged and swallowed so a tracking miss never breaks the agent's edit.
    async fn record_agent_file_change(
        &self,
        workspace_id: WorkspaceId,
        agent_id: Option<String>,
        session_id: Option<String>,
        data: Value,
    ) {
        let rel_path = match data
            .get("relativePath")
            .or_else(|| data.get("path"))
            .and_then(Value::as_str)
        {
            Some(p) if !p.is_empty() => p.to_string(),
            _ => return,
        };
        let status = match data.get("action").and_then(Value::as_str) {
            Some("create") => "added",
            Some("delete") => "deleted",
            _ => "modified",
        };

        let store = self.bus.store();
        let ws = match store.get_workspace(&workspace_id).await {
            Ok(ws) => ws,
            Err(e) => {
                tracing::warn!(error = %e, "file-tracking: workspace lookup failed");
                return;
            }
        };
        let Some(worktree) = crate::git_ops::worktree_path(&ws) else {
            return;
        };

        // Diff compute is best-effort: a missing repo / clean worktree still
        // records the attribution row (with zero stats) so provenance is kept.
        let summary = match crate::diffs::compute_and_store(
            store,
            &worktree,
            &workspace_id,
            &rel_path,
            false,
        )
        .await
        {
            Ok(summary) => summary,
            Err(e) => {
                tracing::warn!(error = %e, "file-tracking: diff compute failed");
                None
            }
        };

        let change = NewTrackedChange {
            workspace_id: workspace_id.clone(),
            path: rel_path,
            stage: "unstaged".to_string(),
            status: status.to_string(),
            agent_id,
            session_id,
            turn: None,
            commit_hash: None,
            old_blob_sha: summary.as_ref().and_then(|s| s.old_blob_sha.clone()),
            new_blob_sha: summary.as_ref().and_then(|s| s.new_blob_sha.clone()),
            additions: summary.as_ref().map(|s| s.additions).unwrap_or(0),
            deletions: summary.as_ref().map(|s| s.deletions).unwrap_or(0),
        };
        if let Err(e) = crate::file_tracking::track_change(store, change).await {
            tracing::warn!(error = %e, "file-tracking: track_change failed");
            return;
        }
        // Recompute the durable line-change aggregates so the metrics.* reads
        // (§17.5) reflect this edit. Best-effort: attribution is already recorded.
        if let Err(e) = crate::metrics::recompute(store, &workspace_id).await {
            tracing::warn!(error = %e, "metrics: recompute failed");
        }
    }
}

/// Global concurrency registry for spawned agent processes (port of
/// `agent-process-registry`). Enforces a hard cap across all workspaces and, on
/// [`ProcessRegistry::acquire`], evicts the least-recently-used idle process (or
/// queues the request when every process is active).
pub struct ProcessRegistry {
    cap: usize,
    inner: Mutex<RegistryInner>,
}

impl ProcessRegistry {
    /// A registry with a fixed concurrency `cap`.
    pub fn new(cap: usize) -> Self {
        Self {
            cap: cap.max(1),
            inner: Mutex::new(RegistryInner::default()),
        }
    }

    /// The configured concurrency cap.
    pub fn cap(&self) -> usize {
        self.cap
    }

    /// Number of registered processes.
    pub fn size(&self) -> usize {
        self.inner.lock().unwrap().entries.len()
    }

    /// Whether `agent_id` is currently registered.
    pub fn is_registered(&self, agent_id: &AgentId) -> bool {
        self.inner.lock().unwrap().entries.contains_key(agent_id)
    }

    /// Register a freshly spawned process (starts idle). `kill` tears the
    /// process down when the registry evicts/reaps it.
    pub fn register(&self, agent_id: AgentId, kill: KillFn) {
        self.inner.lock().unwrap().entries.insert(
            agent_id,
            ProcessEntry {
                last_active_ms: now_ms(),
                is_active: false,
                kill,
            },
        );
    }

    /// Remove a process and wake the next queued spawn, if any.
    pub fn deregister(&self, agent_id: &AgentId) -> bool {
        let mut inner = self.inner.lock().unwrap();
        let had = inner.entries.remove(agent_id).is_some();
        if had {
            if let Some(waiter) = pop_waiter(&mut inner) {
                let _ = waiter.send(());
            }
        }
        had
    }

    /// Mark a process as actively streaming (never evicted while active).
    pub fn mark_active(&self, agent_id: &AgentId) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(entry) = inner.entries.get_mut(agent_id) {
            entry.is_active = true;
            entry.last_active_ms = now_ms();
        }
    }

    /// Mark a process idle (eligible for eviction) and wake a queued spawn so it
    /// can take the freed slot immediately.
    pub fn mark_idle(&self, agent_id: &AgentId) {
        let mut inner = self.inner.lock().unwrap();
        let existed = match inner.entries.get_mut(agent_id) {
            Some(entry) => {
                entry.is_active = false;
                entry.last_active_ms = now_ms();
                true
            }
            None => false,
        };
        if existed {
            if let Some(waiter) = pop_waiter(&mut inner) {
                let _ = waiter.send(());
            }
        }
    }

    /// Ensure a slot is free before spawning: returns immediately under the cap,
    /// otherwise evicts the LRU idle process, or queues until one frees.
    pub async fn acquire(&self) {
        loop {
            enum Action {
                Slot,
                Evict(AgentId, KillFn),
                Wait(tokio::sync::oneshot::Receiver<()>),
            }
            let action = {
                let mut inner = self.inner.lock().unwrap();
                if inner.entries.len() < self.cap {
                    Action::Slot
                } else if let Some((id, kill)) = lru_idle(&inner) {
                    Action::Evict(id, kill)
                } else {
                    let (tx, rx) = tokio::sync::oneshot::channel();
                    inner.wait_queue.push(tx);
                    Action::Wait(rx)
                }
            };
            match action {
                Action::Slot => return,
                Action::Evict(id, kill) => {
                    kill().await;
                    self.deregister(&id);
                }
                Action::Wait(rx) => {
                    let _ = rx.await;
                }
            }
        }
    }

    /// Set a process's `last_active` timestamp directly (deterministic LRU
    /// ordering in tests).
    #[cfg(test)]
    pub(crate) fn set_last_active(&self, agent_id: &AgentId, ms: u64) {
        if let Some(entry) = self.inner.lock().unwrap().entries.get_mut(agent_id) {
            entry.last_active_ms = ms;
        }
    }

    /// Whether a process is marked active (test observability).
    #[cfg(test)]
    pub(crate) fn is_active(&self, agent_id: &AgentId) -> bool {
        self.inner
            .lock()
            .unwrap()
            .entries
            .get(agent_id)
            .map(|e| e.is_active)
            .unwrap_or(false)
    }

    /// Evict idle processes in LRU order (the idle-reap hook; full
    /// timer/memory-pressure triggering is M5). Returns the number evicted.
    pub async fn evict_idle(&self, max: Option<usize>) -> usize {
        let max = max.unwrap_or(usize::MAX);
        let mut evicted = 0;
        while evicted < max {
            let Some((id, kill)) = ({
                let inner = self.inner.lock().unwrap();
                lru_idle(&inner)
            }) else {
                break;
            };
            kill().await;
            self.deregister(&id);
            evicted += 1;
        }
        evicted
    }

    /// TTL-based idle reaping (§5.6/§6.7): evict every idle process whose last
    /// activity is older than `ttl`, skipping any the `eligible` predicate
    /// rejects (e.g. an agent with an in-flight prompt). Active processes and
    /// those within the TTL are always kept. Returns the number evicted.
    pub async fn evict_idle_older_than<F>(&self, ttl: Duration, eligible: F) -> usize
    where
        F: Fn(&AgentId) -> bool,
    {
        let cutoff = now_ms().saturating_sub(ttl.as_millis() as u64);
        let candidates = {
            let inner = self.inner.lock().unwrap();
            idle_older_than(&inner, cutoff)
        };
        let mut evicted = 0;
        for (id, kill) in candidates {
            if !eligible(&id) {
                continue;
            }
            kill().await;
            self.deregister(&id);
            evicted += 1;
        }
        evicted
    }
}

/// A generated `--mcp-config` file on disk, removed when the owning agent's
/// handle is dropped (the file only needs to outlive the child that reads it).
struct TempConfigFile {
    path: PathBuf,
}

impl Drop for TempConfigFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// One live agent: its ACP [`Connection`] (own id space + pending map), the
/// streaming-notification receiver consumed during a turn, the client-served
/// request loop, the owned child (its process group is killed on teardown via
/// [`kill_child_tree`], with `kill_on_drop` as a direct-child safety net), and
/// the per-agent MCP bridge + generated config that back the agent→BE tool loop.
struct AgentHandle {
    connection: Arc<Connection>,
    notifications: Arc<TokioMutex<mpsc::UnboundedReceiver<IncomingNotification>>>,
    serve_task: JoinHandle<()>,
    _child: Option<Child>,
    _mcp_bridge: Option<McpBridge>,
    _mcp_config: Option<TempConfigFile>,
    _rules_config: Option<TempConfigFile>,
}

impl Drop for AgentHandle {
    fn drop(&mut self) {
        self.serve_task.abort();
    }
}

type Handles = Arc<Mutex<HashMap<AgentId, AgentHandle>>>;

/// Central multiplexer over the ACP client + process registry (§6.8). Owns a
/// [`HashMap<AgentId, AgentHandle>`], the [`ProcessRegistry`], and the shared
/// [`EventSink`]/permission state the per-agent client handlers use.
pub struct AgentManager {
    services: Services,
    registry: Arc<ProcessRegistry>,
    handles: Handles,
    sink: Arc<dyn EventSink>,
    permissions: Arc<PermissionRegistry>,
    policy: PermissionPolicy,
    mcp_bridge_exe: PathBuf,
    /// Agents with an in-flight turn loop (a worker is draining their stream).
    /// `agent.sendMessage` consults this to flip a message to the queue while a
    /// turn is mid-stream (the TS "queue while streaming" semantics).
    busy: Arc<Mutex<HashSet<AgentId>>>,
    /// Workspace each in-flight agent belongs to, recorded when the agent claims
    /// its in-flight slot so the slot release can recompute the workspace's
    /// derived `WorkspaceActivity` (§9.9) even on the `stop` path, which only
    /// knows the agent id.
    agent_ws: Arc<Mutex<HashMap<AgentId, WorkspaceId>>>,
    /// Abortable background turn workers, keyed by agent. `stop`/`forceMessage`
    /// abort the in-flight worker (interrupting the current stream).
    workers: Arc<Mutex<HashMap<AgentId, JoinHandle<()>>>>,
    /// Agents whose ACP session was recreated (the resume-impossible fallback in
    /// [`AgentManager::start_session`] replaced a lost `acpSessionId` with a fresh
    /// `session/new`). The next turn prepends the prior conversation history as
    /// `<supervisor>` XML so the fresh session has context, then clears the flag
    /// (parity: TS `sessionWasRecreated`).
    recreated: Arc<Mutex<HashSet<AgentId>>>,
    /// Most recent interrupt-priority `messageId` delivered per agent
    /// (PROTOCOL §5.5). [`AgentManager::interrupt_send_message`] records the
    /// client-supplied id under this lock BEFORE preempting, so the SAME
    /// interrupt delivered twice (client retry / event double-fire) preempts
    /// exactly once — the duplicate is acknowledged idempotently instead of
    /// cancelling the interrupt turn it raced and re-persisting the message.
    interrupt_ids: Arc<Mutex<HashMap<AgentId, String>>>,
}

impl AgentManager {
    /// Wire a manager over the services surface and a concrete event sink, with
    /// a global concurrency `cap`.
    pub fn new(services: Services, sink: Arc<dyn EventSink>, cap: usize) -> Self {
        Self {
            services,
            registry: Arc::new(ProcessRegistry::new(cap)),
            handles: Arc::new(Mutex::new(HashMap::new())),
            sink,
            permissions: Arc::new(PermissionRegistry::new()),
            // Headless default (§6.7/M3.5): auto-allow low-risk reads, auto-deny
            // medium/high-risk prompts. An FE-attached deployment selects
            // `Interactive` via `with_policy()` (wired from `INTENTD_PERMISSION_POLICY`)
            // to drive the `agent.respondPermission`/`agent.pendingPermissions` RPCs.
            policy: PermissionPolicy::AutoByRisk,
            mcp_bridge_exe: std::env::current_exe().unwrap_or_else(|_| PathBuf::from("intentd")),
            busy: Arc::new(Mutex::new(HashSet::new())),
            agent_ws: Arc::new(Mutex::new(HashMap::new())),
            workers: Arc::new(Mutex::new(HashMap::new())),
            recreated: Arc::new(Mutex::new(HashSet::new())),
            interrupt_ids: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Override the permission policy used by spawned agents' client handlers.
    pub fn with_policy(mut self, policy: PermissionPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// The active permission policy (headless `AutoByRisk` unless overridden).
    pub fn policy(&self) -> PermissionPolicy {
        self.policy
    }

    /// Override the executable used as the generated `--mcp-config` bridge
    /// command (defaults to the current `intentd` binary). Tests point this at
    /// `CARGO_BIN_EXE_intentd` so a spawned child reaches the in-process server.
    pub fn with_mcp_bridge_exe(mut self, exe: impl Into<PathBuf>) -> Self {
        self.mcp_bridge_exe = exe.into();
        self
    }

    /// Borrow the process registry (lifecycle / diagnostics).
    pub fn registry(&self) -> &ProcessRegistry {
        &self.registry
    }

    /// Resolve an outstanding interactive permission prompt (`agent.respondPermission`,
    /// PROTOCOL §8): deliver `outcome` to the blocked client handler. Returns
    /// `false` when no such prompt is outstanding (already answered or timed out).
    pub fn respond_permission(&self, request_id: &str, outcome: PermissionOutcome) -> bool {
        self.permissions.resolve(request_id, outcome)
    }

    /// Snapshot every outstanding permission prompt (`agent.pendingPermissions`,
    /// PROTOCOL §8), for a (re)connecting client to recover what awaits an answer.
    pub fn pending_permissions(&self) -> Vec<PermissionRequestData> {
        self.permissions.pending()
    }

    /// Number of tracked agents.
    pub fn len(&self) -> usize {
        self.handles.lock().unwrap().len()
    }

    /// Whether no agents are tracked.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether `agent_id` is currently tracked (lookup).
    pub fn contains(&self, agent_id: &AgentId) -> bool {
        self.handles.lock().unwrap().contains_key(agent_id)
    }

    /// Spawn a provider child, acquire a concurrency slot, stand up the per-agent
    /// agent→BE MCP server + bridge (denylisted for `agent_type`, §6.8/§18.4),
    /// write the generated `--mcp-config` for providers that consume it, wire the
    /// client-served request loop, and track it. Each connection's pending-id
    /// correlation lives in `intent-acp`; the manager keys the handle by
    /// `AgentId` and registers it for lifecycle/eviction.
    pub async fn create_agent(
        &self,
        agent_id: AgentId,
        workspace_id: WorkspaceId,
        agent_name: impl Into<String>,
        agent_type: &str,
        cwd: PathBuf,
        opts: &SpawnOptions<'_>,
    ) -> Result<()> {
        self.registry.acquire().await;

        // Per-agent in-process MCP server over the SAME services surface the FE
        // uses, with the §18.4 denylist for this agent type applied, served over
        // a loopback bridge a real spawned child reaches via `--mcp-config`.
        let api: Arc<dyn WorkspaceApi> = Arc::new(self.services.clone());
        let server = Arc::new(
            WorkspaceMcpServer::for_agent_type(api, workspace_id.clone(), agent_type)
                // Caller-aware tools attribute back to this spawning agent.
                .with_caller_agent_id(Some(agent_id.clone())),
        );
        let bridge = serve_workspace_mcp_tcp(server)
            .await
            .map_err(|e| Error::Internal(format!("mcp bridge bind failed: {e}")))?;

        // Generated MCP config (auggie format) pointing at the bridge
        // subcommand, written only for providers that consume an MCP-config flag.
        let mut mcp_config: Option<TempConfigFile> = None;
        let mut mcp_config_path: Option<String> = None;
        if opts.provider.supports_mcp_config {
            let config = self.generate_mcp_config(&bridge);
            let path = std::env::temp_dir().join(format!("intentd-mcp-{}.json", Uuid::new_v4()));
            let bytes = serde_json::to_vec_pretty(&config)
                .map_err(|e| Error::Internal(format!("serialize mcp config failed: {e}")))?;
            std::fs::write(&path, bytes)
                .map_err(|e| Error::Internal(format!("write mcp config failed: {e}")))?;
            mcp_config_path = Some(path.to_string_lossy().into_owned());
            mcp_config = Some(TempConfigFile { path });
        }

        // Assemble the effective system prompt (the §18.1 injection pipeline:
        // base/specialization/workspace user overrides + live workspace rule
        // files) into a temp `--rules` file when the caller supplies none. The
        // handle owns the temp file so it outlives the child that reads it.
        let mut rules_config: Option<TempConfigFile> = None;
        let mut rules_file_path: Option<String> = None;
        if opts.rules_file.is_none() {
            if let Some(prompt) =
                crate::rules::assemble_system_prompt(&self.services.store, Some(&cwd), agent_type)
                    .await
            {
                let path =
                    std::env::temp_dir().join(format!("intentd-rules-{}.md", Uuid::new_v4()));
                if std::fs::write(&path, prompt.as_bytes()).is_ok() {
                    rules_file_path = Some(path.to_string_lossy().into_owned());
                    rules_config = Some(TempConfigFile { path });
                }
            }
        }

        // Reconstruct the spawn options with the generated config path injected.
        let mut spawn_opts = SpawnOptions::new(opts.provider);
        spawn_opts.model = opts.model;
        spawn_opts.cwd = opts.cwd;
        spawn_opts.rules_file = opts.rules_file.or(rules_file_path.as_deref());
        spawn_opts.quiet = opts.quiet;
        spawn_opts.provider_binary = opts.provider_binary;
        spawn_opts.extra_env = opts.extra_env.clone();
        if let Some(ref p) = mcp_config_path {
            spawn_opts.mcp_config_file = Some(p.as_str());
        }

        let (req_tx, mut req_rx) = mpsc::unbounded_channel::<IncomingRequest>();
        let (note_tx, note_rx) = mpsc::unbounded_channel::<IncomingNotification>();
        let hooks = ConnectionHooks {
            requests: Some(req_tx),
            notifications: Some(note_tx),
            auth_error_patterns: opts
                .provider
                .auth_error_patterns
                .map(|p| p.iter().map(|s| s.to_string()).collect())
                .unwrap_or_default(),
        };
        let spawned = spawn_provider(&spawn_opts, hooks)
            .map_err(|e| Error::Internal(format!("spawn provider failed: {e}")))?;
        let (child, connection) = spawned.into_parts();
        let connection = Arc::new(connection);

        let terminal_host: Arc<dyn intent_acp::TerminalHost> =
            Arc::new(crate::PtyTerminalHost::new(self.services.pty()));
        let handler = Arc::new(
            ClientRequestHandler::new(
                workspace_id,
                agent_id.clone(),
                agent_name.into(),
                FileService::new(cwd),
                self.permissions.clone(),
                self.policy,
                self.sink.clone(),
            )
            .with_terminal_host(terminal_host),
        );
        let serve_conn = connection.clone();
        let serve_task = tokio::spawn(async move {
            while let Some(req) = req_rx.recv().await {
                if let Err(e) = handler.serve(serve_conn.as_ref(), req).await {
                    tracing::warn!(error = %e, "client-served request failed");
                }
            }
        });

        self.registry
            .register(agent_id.clone(), self.make_kill(agent_id.clone()));
        let handle = AgentHandle {
            connection,
            notifications: Arc::new(TokioMutex::new(note_rx)),
            serve_task,
            _child: Some(child),
            _mcp_bridge: Some(bridge),
            _mcp_config: mcp_config,
            _rules_config: rules_config,
        };
        // Concurrency safety: fully reap any stale handle + child for this agent
        // BEFORE installing the new one, reusing the process-group teardown.
        // A bare `insert` would only drop the old handle (aborting its serve
        // loop, with `kill_on_drop` reaping just the direct child) — orphaning
        // grandchildren and risking a lingering streamer from a lost/old session
        // that could keep appending to the agentId-keyed transcript. The
        // per-agent single-flight slot serializes turns; this closes the
        // respawn-time window. (Drop the lock before awaiting the kill.)
        let stale = self.handles.lock().unwrap().remove(&agent_id);
        if let Some(mut stale) = stale {
            if let Some(child) = stale._child.take() {
                kill_child_tree(child).await;
            }
        }
        self.handles.lock().unwrap().insert(agent_id, handle);
        Ok(())
    }

    /// Build the generated `--mcp-config` (auggie `{ mcpServers }` shape): the
    /// `workspace-mcp` server is the `intentd mcp-bridge --connect <addr>`
    /// subcommand, with the safe baseline env injected (§6.8).
    fn generate_mcp_config(&self, bridge: &McpBridge) -> serde_json::Value {
        let mut servers = NormalizedMcpServers::new();
        servers.insert(
            "workspace-mcp".to_string(),
            NormalizedMcpServer::Stdio {
                command: self.mcp_bridge_exe.to_string_lossy().into_owned(),
                args: vec![
                    "mcp-bridge".to_string(),
                    "--connect".to_string(),
                    bridge.connect_addr(),
                ],
                env: build_baseline_mcp_env_from_process(),
            },
        );
        to_auggie_mcp_config(&servers)
    }

    /// Complete the connection handshake and establish an ACP session for a
    /// spawned agent (the agent→BE MCP server is delivered out-of-band via the
    /// generated `--mcp-config`, so `session/new` carries no `mcpServers`). On a
    /// daemon respawn the agent may already have a persisted `acpSessionId`:
    ///
    /// 1. Resume it via `session/load` when the agent advertised `loadSession` —
    ///    the agent keeps its prior context, so no history resend is needed.
    /// 2. Otherwise (no `loadSession`, or `session/load` failed) fall back to a
    ///    fresh `session/new` that REPLACES the lost id (relaxing the write-once
    ///    invariant only here) and flag the agent so the next turn resends the
    ///    prior conversation history as `<supervisor>` XML.
    /// 3. With no persisted id (a brand-new agent) open a first session and
    ///    persist it write-once.
    ///
    /// Returns the `acpSessionId` to drive [`AgentManager::run_turn`] (§6.5).
    pub async fn start_session(
        &self,
        agent_id: &AgentId,
        cwd: PathBuf,
        provider: &ProviderConfig,
    ) -> Result<String> {
        let conn = {
            let map = self.handles.lock().unwrap();
            map.get(agent_id)
                .ok_or_else(|| Error::NotFound(format!("agent {agent_id}")))?
                .connection
                .clone()
        };
        let handshake = handshake(conn.as_ref(), provider)
            .await
            .map_err(|e| Error::Internal(format!("handshake failed: {e}")))?;

        // The persisted id (if any) decides the no-resume branch: a brand-new
        // agent (no id) opens a first session; an agent with a lost id recreates
        // (CAS-replacing exactly this id) and resends history.
        let stored_id = self
            .services
            .store
            .get_agent_session(agent_id)
            .await?
            .acp_session_id;

        // 1) Try to resume the persisted session (gated on stored id + capability).
        match self
            .services
            .resume_acp_session(
                conn.as_ref(),
                &handshake.initialize,
                agent_id,
                cwd.clone(),
                Vec::new(),
            )
            .await
        {
            Ok(Some(acp_session_id)) => {
                // `session/load` replays the prior conversation as a buffered
                // `session/update` burst; discard it before the first turn so it
                // is neither re-published as events nor re-accumulated into the
                // transcript (parity with TS's "no active streaming handler ⇒
                // drop"). Only the resume path needs this settle-window drain —
                // new/recreate sessions have no buffered replay.
                let notes = self
                    .handles
                    .lock()
                    .unwrap()
                    .get(agent_id)
                    .map(|h| h.notifications.clone());
                if let Some(notes) = notes {
                    let mut guard = notes.lock().await;
                    Services::drain_replay_notifications(&mut guard).await;
                }
                return Ok(acp_session_id);
            }
            Ok(None) => {}
            // `session/load` was attempted but failed → fall through to recreate.
            Err(e) => {
                tracing::warn!(agent = %agent_id, error = %e, "session/load failed; recreating");
            }
        }

        // 2) Resume impossible but a session existed → recreate + flag for resend.
        // The fresh `session/new` runs on the child just spawned for this turn
        // (the lost session's child, if any, was already reaped before the
        // respawn — see `create_agent`'s defensive teardown), so no streamer from
        // the old session can append to the agentId-keyed transcript. The CAS
        // replace keeps the id canonical, swapping only the exact id we failed to
        // load.
        if let Some(expected_old) = stored_id {
            let acp_session_id = self
                .services
                .recreate_acp_session(conn.as_ref(), agent_id, &expected_old, cwd, Vec::new())
                .await?;
            self.recreated.lock().unwrap().insert(agent_id.clone());
            return Ok(acp_session_id);
        }

        // 3) Brand-new agent → open and persist the first session (write-once).
        self.services
            .open_acp_session(conn.as_ref(), agent_id, cwd, Vec::new())
            .await
    }

    /// Take (clear) the recreate flag for `agent_id`: `true` when the agent's ACP
    /// session was recreated by the resume-impossible fallback since the last
    /// turn, meaning the next prompt must resend the prior conversation history.
    fn take_recreated(&self, agent_id: &AgentId) -> bool {
        self.recreated.lock().unwrap().remove(agent_id)
    }

    /// Build the prompt blocks for an agent's next turn. Normally just the user
    /// `content`; but when the ACP session was recreated (the resume-impossible
    /// fallback), prepend the prior conversation history as `<supervisor>` XML so
    /// the fresh session has context, then clear the flag (parity: TS
    /// `sessionWasRecreated` → `formatHistoryAsXml`). The just-persisted current
    /// user message is excluded from the rendered history.
    async fn build_turn_prompt(&self, agent_id: &AgentId, content: &str) -> Vec<ContentBlock> {
        // Role reminder is rebuilt every turn (interval = 1, port of
        // acp-provider.ts) and prepended to the outbound prompt for specialist
        // agents; absent for non-specialist agents. Because it fires every turn
        // it also covers the session-recreated case handled by `build_turn_body`.
        let reminder = self.services.agent_role_reminder(agent_id).await;
        let body = self.build_turn_body(agent_id, content).await;
        let prompt_text = match reminder {
            Some(r) => format!("{r}\n\n{body}"),
            None => body,
        };
        text_prompt(&prompt_text)
    }

    /// Build the user-turn body: normally just `content`, but when the ACP
    /// session was recreated (the resume-impossible fallback), prepend the prior
    /// conversation history as `<supervisor>` XML so the fresh session has
    /// context, then clear the flag (parity: TS `sessionWasRecreated` →
    /// `formatHistoryAsXml`). The just-persisted current user message is excluded
    /// from the rendered history.
    async fn build_turn_body(&self, agent_id: &AgentId, content: &str) -> String {
        if !self.take_recreated(agent_id) {
            return content.to_string();
        }
        let messages = self
            .services
            .store
            .get_agent_messages(agent_id, None)
            .await
            .unwrap_or_default();
        // The current user message was already appended → render all but the last.
        let prior = messages.split_last().map(|(_, rest)| rest).unwrap_or(&[]);
        if prior.is_empty() {
            return content.to_string();
        }
        let history_xml =
            crate::history_xml::format_history_as_xml(prior, crate::history_xml::MAX_HISTORY_CHARS);
        format!("{history_xml}\n\n{content}")
    }

    /// Drive one `session/prompt` turn for `agent_id`, marking it active for the
    /// duration so the registry never evicts a streaming process. Streams
    /// updates onto the event bus via the M3.4 router (`run_prompt_turn`).
    pub async fn run_turn(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        acp_session_id: &str,
        prompt: Vec<ContentBlock>,
    ) -> Result<StopReason> {
        let (conn, notes) = {
            let map = self.handles.lock().unwrap();
            let handle = map
                .get(agent_id)
                .ok_or_else(|| Error::NotFound(format!("agent {agent_id}")))?;
            (handle.connection.clone(), handle.notifications.clone())
        };
        self.registry.mark_active(agent_id);
        let mut guard = notes.lock().await;
        let result = self
            .services
            .run_prompt_turn(
                conn.as_ref(),
                &mut guard,
                agent_id,
                workspace_id,
                acp_session_id,
                prompt,
            )
            .await;
        self.registry.mark_idle(agent_id);
        result
    }

    /// Stop one agent: abort its in-flight turn worker (interrupting the current
    /// stream), clear its busy flag, drop its handle, and deregister it. The
    /// child's whole process group is signalled (SIGTERM→SIGKILL) so no orphaned
    /// grandchildren linger. Returns whether a handle existed. This is the
    /// `agent.stop` / hard-cancel cancel semantics.
    pub async fn stop(&self, agent_id: &AgentId) -> bool {
        if let Some(worker) = self.workers.lock().unwrap().remove(agent_id) {
            worker.abort();
        }
        // Drop any pending recreate flag: the next spawn re-decides resume vs
        // recreate from scratch, so a stale flag must not survive a teardown.
        self.recreated.lock().unwrap().remove(agent_id);
        self.end_turn(agent_id).await;
        let handle = self.handles.lock().unwrap().remove(agent_id);
        let removed = handle.is_some();
        if let Some(mut handle) = handle {
            if let Some(child) = handle._child.take() {
                kill_child_tree(child).await;
            }
        }
        self.registry.deregister(agent_id);
        removed
    }

    /// Interrupt one agent's in-flight turn WITHOUT killing its child — the TS
    /// `agent.stop` keep-alive semantics (`ConsolidatedBackend.backendStop` with
    /// `killProcess: false` → `provider.interrupt()`): cancel the current turn
    /// over the wire (`session/cancel`), abort the draining worker, release the
    /// in-flight slot, mark the process idle, and emit the single terminal
    /// `agent:stream:end` (the aborted worker can no longer emit it). The child +
    /// ACP session stay alive, so a follow-up `agent.sendMessage` resumes the
    /// same session. Falls back to the hard [`AgentManager::stop`] kill ONLY when
    /// no live session is available to interrupt (no handle / no `acpSessionId`),
    /// the Rust analog of TS reserving the kill for `killProcess: true`. Returns
    /// whether an agent was found.
    pub async fn interrupt(&self, agent_id: &AgentId) -> bool {
        // The live connection is the interrupt capability; grab it WITHOUT
        // removing the handle so the child stays alive for resume.
        let conn = self
            .handles
            .lock()
            .unwrap()
            .get(agent_id)
            .map(|h| h.connection.clone());
        let Some(conn) = conn else {
            // No live session to interrupt → keep-alive is a no-op; fall back to
            // the hard kill path (itself a no-op when the agent is already gone).
            return self.stop(agent_id).await;
        };
        // Resolve the persisted session for the workspace (terminal event) + the
        // `acpSessionId` to cancel. Without an `acpSessionId` there is no
        // in-flight turn to interrupt, so fall back to the kill path.
        let session = self.services.store.get_agent_session(agent_id).await.ok();
        let acp_session_id = session.as_ref().and_then(|s| s.acp_session_id.clone());
        let Some(acp_session_id) = acp_session_id else {
            return self.stop(agent_id).await;
        };
        // Abort the in-flight worker so it stops draining the turn/queue; the
        // child is kept alive (unlike `stop`, which also kills the child).
        if let Some(worker) = self.workers.lock().unwrap().remove(agent_id) {
            worker.abort();
        }
        // Cancel the current turn over the wire (keep-alive interrupt). The agent
        // resolves its in-flight `session/prompt` with `StopReason::Cancelled`;
        // best-effort — a wire error never blocks the stop.
        if let Err(e) = intent_acp::session::cancel(&conn, &acp_session_id).await {
            tracing::warn!(agent = %agent_id, error = %e, "session/cancel failed");
        }
        // Release the in-flight slot (recomputes workspace activity) and capture
        // the owning workspace BEFORE the slot is dropped so the terminal event
        // is stamped on the right workspace; fall back to the persisted session.
        let workspace_id = self
            .agent_ws
            .lock()
            .unwrap()
            .get(agent_id)
            .cloned()
            .or_else(|| session.as_ref().map(|s| s.workspace_id.clone()));
        self.end_turn(agent_id).await;
        // Mark the process idle (reapable) but keep its handle so it survives for
        // a follow-up resume.
        self.registry.mark_idle(agent_id);
        // Emit the single terminal `agent:stream:end` on stop (parity #14): the
        // aborted worker's `run_prompt_turn` no longer reaches its own emit.
        if let Some(workspace_id) = workspace_id {
            self.services
                .publish_agent_event(
                    &workspace_id,
                    agent_id,
                    intent_core::events::AGENT_STREAM_END,
                    json!({ "agentId": agent_id.0 }),
                )
                .await;
        }
        true
    }

    /// Whether a turn loop is currently in flight for `agent_id` (consulted by
    /// `agent.sendMessage` to decide queue-vs-stream).
    pub fn is_busy(&self, agent_id: &AgentId) -> bool {
        self.busy.lock().unwrap().contains(agent_id)
    }

    /// Atomically claim the in-flight slot for `agent_id` in `workspace_id`:
    /// `true` when the agent was idle (now marked busy), `false` when a turn is
    /// already running. On a successful claim the agent's workspace is recorded
    /// and the workspace's derived `WorkspaceActivity` is recomputed (§9.9),
    /// emitting `workspace:activity-changed` on the `Idle → AgentRunning` edge.
    /// Also persists the `agent_session.status` transition to `Active` and
    /// emits `agent:status-changed` (PROTOCOL §6.5/§6.7) so a hydrated chat
    /// reflects the live runtime rather than the stored `Pending` placeholder.
    async fn try_begin(&self, agent_id: &AgentId, workspace_id: &WorkspaceId) -> bool {
        let claimed = self.busy.lock().unwrap().insert(agent_id.clone());
        if claimed {
            self.agent_ws
                .lock()
                .unwrap()
                .insert(agent_id.clone(), workspace_id.clone());
            self.services.agent_activity_begin(workspace_id).await;
            self.persist_status(agent_id, workspace_id, AgentStatus::Active, true)
                .await;
        }
        claimed
    }

    /// Release the in-flight slot, recomputing the owning workspace's derived
    /// `WorkspaceActivity` (§9.9) and emitting `workspace:activity-changed` on
    /// the `AgentRunning → Idle` edge. Also persists the `agent_session.status`
    /// transition to `RuntimeIdle` and emits `agent:status-changed` (PROTOCOL
    /// §6.5/§6.7) so a hydrated chat reflects the post-turn idle state.
    async fn end_turn(&self, agent_id: &AgentId) {
        let was_busy = self.busy.lock().unwrap().remove(agent_id);
        if !was_busy {
            return;
        }
        let workspace_id = self.agent_ws.lock().unwrap().remove(agent_id);
        if let Some(workspace_id) = workspace_id {
            self.services.agent_activity_end(&workspace_id).await;
            self.persist_status(agent_id, &workspace_id, AgentStatus::RuntimeIdle, false)
                .await;
        }
    }

    /// Persist `agent_session.status` + `is_active` and publish the
    /// `agent:status-changed` self-sufficient event (PROTOCOL §6.5/§6.7). All
    /// failures are logged and swallowed: the runtime turn is the source of
    /// truth and a transient store/bus error must not abort the in-flight slot
    /// transition.
    async fn persist_status(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        status: AgentStatus,
        is_active: bool,
    ) {
        let ts = now_iso();
        if let Err(e) = self
            .services
            .store
            .set_agent_session_status(agent_id, status, is_active, &ts)
            .await
        {
            // Sessions are persisted before the runtime path opens (see
            // `agent_create_op`), so NotFound here means the row was deleted
            // mid-turn — swallow it the same as any other transient store error.
            tracing::warn!(agent = %agent_id, error = %e, "failed to persist agent status");
            return;
        }
        let serialized_status = match serde_json::to_value(status) {
            Ok(Value::String(s)) => s,
            _ => return,
        };
        let event = NewEvent {
            workspace_id: workspace_id.clone(),
            timestamp: ts,
            event_type: AGENT_STATUS_CHANGED.to_string(),
            actor: EventActor {
                actor_type: ActorType::Agent,
                id: Some(agent_id.0.clone()),
                ..Default::default()
            },
            session_id: Some(agent_id.0.clone()),
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data: json!({
                "agentId": agent_id.0,
                "status": serialized_status,
                "isActive": is_active,
            }),
        };
        crate::publish_event(&self.services.event_bus, event).await;
    }

    /// Forget a finished worker's join handle.
    fn clear_worker(&self, agent_id: &AgentId) {
        self.workers.lock().unwrap().remove(agent_id);
    }

    /// `agent.sendMessage` runtime path (§5.5/§6.8): when a turn is already in
    /// flight, enqueue (the worker flips it to in-flight when the current turn
    /// ends); otherwise persist the user message and spawn a background worker
    /// that lazily spawns the child on first turn, drives the ACP turn through
    /// [`AgentManager::run_turn`], and drains the queue. Returns the TS-shaped
    /// `{ success, queued, messageId | queuedMessage }`.
    pub async fn send_message(
        self: &Arc<Self>,
        agent_id: AgentId,
        workspace_id: WorkspaceId,
        content: String,
        message_id: Option<String>,
    ) -> Result<Value> {
        if !self.try_begin(&agent_id, &workspace_id).await {
            let (queued, position) = self.services.enqueue_message(&agent_id, content, None);
            let result = json!({
                "success": true,
                "queued": true,
                "queuedMessage": queued.to_value(position),
            });
            self.services.publish_queue_updated(&agent_id).await;
            return Ok(result);
        }
        let message_id = message_id.unwrap_or_else(new_message_id);
        let blocks = user_text_blocks(&content);
        if self
            .services
            .store
            .append_agent_message(&agent_id, "user", &blocks, &now_iso())
            .await
            .is_err()
        {
            // Store write failed (e.g. session not yet persisted) → auto-queue,
            // matching the `agent.sendMessage` fallback (PROTOCOL §5.5). Self-drain:
            // the slot we just released will be reclaimed below if the queue is
            // ready and the agent is otherwise free.
            self.end_turn(&agent_id).await;
            let (queued, position) = self.services.enqueue_message(&agent_id, content, None);
            let result = json!({
                "success": true,
                "queued": true,
                "queuedMessage": queued.to_value(position),
            });
            self.services.publish_queue_updated(&agent_id).await;
            self.clone().try_drain_queue(agent_id, workspace_id).await;
            return Ok(result);
        }
        self.spawn_worker(agent_id, workspace_id, content);
        Ok(json!({ "success": true, "queued": false, "messageId": message_id }))
    }

    /// Self-drain entrypoint (PROTOCOL §5.5). Invoked from `agent.queueMessage`
    /// (and the `send_message` auto-queue fallback above) so a queued message
    /// never sits unworked when the agent is idle. Claims the in-flight slot,
    /// dequeues the head of the queue, persists it, and spawns the turn worker
    /// (which then drains the rest of the queue via its end-of-turn loop).
    /// When the slot is already held by another worker this is a no-op — that
    /// worker's drain loop will pick the message up at turn-end.
    pub async fn try_drain_queue(self: Arc<Self>, agent_id: AgentId, workspace_id: WorkspaceId) {
        if self.is_busy(&agent_id) {
            return;
        }
        // Only claim the in-flight slot when at least one ready-to-send (not
        // under edit) message is waiting — an editing-only queue must stay
        // idle (PROTOCOL §5.5/§6.5 invariant: idle is permitted iff every
        // remaining queued item has `editing = true`).
        if !self.services.has_ready_to_send(&agent_id) {
            return;
        }
        if !self.try_begin(&agent_id, &workspace_id).await {
            return;
        }
        let next = match self.services.dequeue_message(&agent_id) {
            Some(msg) => msg,
            None => {
                // Raced with another mutation (e.g. remove) that emptied the
                // ready-to-send queue between the check above and the dequeue.
                self.end_turn(&agent_id).await;
                return;
            }
        };
        self.services
            .publish_queue_updated_for(
                &agent_id,
                &workspace_id,
                self.services.queue_snapshot(&agent_id),
            )
            .await;
        persist_user(&self, &agent_id, &next.content).await;
        self.spawn_worker(agent_id, workspace_id, next.content);
    }

    /// `agent.forceMessage` runtime path (§5.5): stop the current stream (abort
    /// the worker + kill the child), discard the pending queue, then deliver the
    /// forced message immediately as a fresh turn.
    pub async fn force_message(
        self: &Arc<Self>,
        agent_id: AgentId,
        workspace_id: WorkspaceId,
        message_id: String,
        content: String,
    ) -> Result<Value> {
        self.stop(&agent_id).await;
        if self.services.clear_queue(&agent_id) {
            self.services
                .publish_queue_updated_for(&agent_id, &workspace_id, Vec::new())
                .await;
        }
        let blocks = user_text_blocks(&content);
        self.services
            .store
            .append_agent_message(&agent_id, "user", &blocks, &now_iso())
            .await?;
        self.try_begin(&agent_id, &workspace_id).await;
        self.spawn_worker(agent_id, workspace_id, content);
        Ok(json!({ "success": true, "queued": false, "messageId": message_id }))
    }

    /// `agent.sendMessage` with `priority: "interrupt"` (§5.5): preempt the
    /// in-flight turn instead of queueing behind it, then deliver `content`
    /// immediately as a fresh turn on the SAME live session. The preemption is
    /// the keep-alive [`AgentManager::interrupt`] (`session/cancel` + worker
    /// abort) — unlike [`AgentManager::force_message`], the child process is
    /// never killed and the pending queue is preserved, so the interrupted
    /// agent keeps processing (the queue drains after the interrupt turn). An
    /// idle agent falls through to the normal [`AgentManager::send_message`]
    /// path unchanged.
    ///
    /// Two crash timings from the reference app are guarded here:
    /// - **Duplicate delivery.** The SAME interrupt (same client-supplied
    ///   `messageId`) delivered twice in quick succession preempts exactly
    ///   once: the id is recorded under [`AgentManager::interrupt_ids`] BEFORE
    ///   preempting, so the duplicate returns an idempotent
    ///   `{ success, queued: false, messageId, deduplicated: true }` ack
    ///   without cancelling the interrupt turn it raced and without
    ///   re-persisting the message. Dedup requires a stable `messageId`; a
    ///   distinct id is a genuinely new interrupt and preempts normally.
    /// - **Turn startup.** When the busy slot is claimed but there is no
    ///   cancellable turn yet (child handle / `acpSessionId` not live — the
    ///   spawn/`session/new` window), [`AgentManager::interrupt`] would fall
    ///   back to the hard `stop` kill. Preemption is skipped instead and the
    ///   message queues keep-alive behind the starting turn (`queued: true`),
    ///   draining right after it — the agent is never killed.
    pub async fn interrupt_send_message(
        self: &Arc<Self>,
        agent_id: AgentId,
        workspace_id: WorkspaceId,
        content: String,
        message_id: Option<String>,
    ) -> Result<Value> {
        // Duplicate-delivery guard: check-and-record is atomic under the lock,
        // so of two racing duplicates exactly one proceeds to preempt.
        if let Some(mid) = message_id.as_deref() {
            let mut ids = self.interrupt_ids.lock().unwrap();
            if ids.get(&agent_id).map(String::as_str) == Some(mid) {
                return Ok(json!({
                    "success": true,
                    "queued": false,
                    "messageId": mid,
                    "deduplicated": true,
                }));
            }
            ids.insert(agent_id.clone(), mid.to_string());
        }
        if self.is_busy(&agent_id) {
            // Preempt only when a cancellable turn is live (handle +
            // `acpSessionId`); during turn startup the keep-alive interrupt
            // would fall back to the `stop` kill path, so skip it and let
            // `send_message` queue behind the starting turn instead.
            let cancellable = self.contains(&agent_id)
                && self
                    .services
                    .store
                    .get_agent_session(&agent_id)
                    .await
                    .ok()
                    .and_then(|s| s.acp_session_id)
                    .is_some();
            if cancellable {
                // Keep-alive: cancels the turn over the wire, aborts the
                // draining worker, releases the in-flight slot, and emits the
                // terminal `agent:stream:end` — the child + ACP session stay
                // alive.
                self.interrupt(&agent_id).await;
            }
        }
        // The slot was just released (or was never held): the send path claims
        // it and streams the interrupt message right away rather than queueing.
        // If a concurrent send wins the race the message queues instead — it is
        // still delivered by that worker's drain loop, never dropped.
        self.send_message(agent_id, workspace_id, content, message_id)
            .await
    }

    /// Spawn (and track) the background turn worker for an agent. The caller must
    /// already hold the in-flight slot (`try_begin`).
    fn spawn_worker(
        self: &Arc<Self>,
        agent_id: AgentId,
        workspace_id: WorkspaceId,
        content: String,
    ) {
        let mgr = self.clone();
        let id = agent_id.clone();
        let handle = tokio::spawn(async move {
            run_message_worker(mgr, id, workspace_id, content).await;
        });
        self.workers.lock().unwrap().insert(agent_id, handle);
    }

    /// Ensure the agent's child process + ACP session exist, spawning lazily on
    /// first turn (the TS spawn-on-first-message semantics) and reusing the live
    /// session otherwise. Returns the `acpSessionId` to drive the turn.
    async fn ensure_started(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
    ) -> Result<String> {
        let session = self.services.store.get_agent_session(agent_id).await?;
        if self.contains(agent_id) {
            if let Some(acp) = session.acp_session_id.clone() {
                return Ok(acp);
            }
        }
        let workspace = self.services.store.get_workspace(workspace_id).await.ok();
        let resolved = resolve_spawn(&session, workspace.as_ref())?;
        let mut opts = SpawnOptions::new(&resolved.provider);
        opts.cwd = Some(&resolved.cwd);
        opts.model = resolved.model.as_deref();
        opts.extra_env = resolved.extra_env.clone();
        if !self.contains(agent_id) {
            // Derive the agent type from the session's specialist `agentType`
            // frontmatter (SP-B); falls back to the default interactive type so
            // plain agents and specialists without `agentType` are unchanged.
            let agent_type = derive_agent_type(&self.services, &session, workspace.as_ref());
            self.create_agent(
                agent_id.clone(),
                workspace_id.clone(),
                session.name.clone(),
                &agent_type,
                resolved.cwd.clone(),
                &opts,
            )
            .await?;
        }
        self.start_session(agent_id, resolved.cwd.clone(), &resolved.provider)
            .await
    }

    /// Tear down every tracked agent (clean daemon shutdown kills all children).
    pub async fn shutdown(&self) {
        let ids: Vec<AgentId> = self.handles.lock().unwrap().keys().cloned().collect();
        for id in &ids {
            self.stop(id).await;
        }
    }

    /// Idle-reap hook: evict up to `max` idle agents in LRU order (count-based;
    /// the LRU `acquire`-eviction companion).
    pub async fn reap_idle(&self, max: Option<usize>) -> usize {
        self.registry.evict_idle(max).await
    }

    /// TTL idle-reap sweep (§5.6/§6.7): evict every idle agent whose last
    /// activity is older than `ttl`, skipping any with an in-flight prompt (a
    /// live turn loop in `busy`). Active streaming agents are protected by the
    /// registry's `is_active` flag. Returns the number reaped.
    pub async fn reap_idle_older_than(&self, ttl: Duration) -> usize {
        let busy = self.busy.clone();
        self.registry
            .evict_idle_older_than(ttl, move |id| !busy.lock().unwrap().contains(id))
            .await
    }

    /// Build the kill callback for `agent_id`: removing the handle signals the
    /// child's whole process group (SIGTERM→SIGKILL) and aborts its request
    /// loop, so no orphaned grandchildren linger.
    fn make_kill(&self, agent_id: AgentId) -> KillFn {
        let handles: Weak<Mutex<HashMap<AgentId, AgentHandle>>> = Arc::downgrade(&self.handles);
        Arc::new(move || {
            let handles = handles.clone();
            let id = agent_id.clone();
            Box::pin(async move {
                let removed = handles
                    .upgrade()
                    .and_then(|h| h.lock().unwrap().remove(&id));
                if let Some(mut handle) = removed {
                    if let Some(child) = handle._child.take() {
                        kill_child_tree(child).await;
                    }
                }
            })
        })
    }
}

/// Grace period between SIGTERM and SIGKILL when tearing down a provider's
/// process group, giving the tree a chance to exit cleanly first.
#[cfg(unix)]
const PROCESS_GROUP_TERM_GRACE: Duration = Duration::from_secs(2);

/// Terminate a spawned provider's WHOLE process tree (§5.6). The child is its
/// own process-group leader (`process_group(0)` at spawn), so `killpg(pgid,…)`
/// reaches every descendant — `kill_on_drop` alone only reaps the direct child,
/// orphaning grandchildren. SIGTERM first for a clean exit, then SIGKILL after a
/// grace period to sweep anything that ignored it.
#[cfg(unix)]
async fn kill_child_tree(mut child: Child) {
    use nix::sys::signal::{killpg, Signal};
    use nix::unistd::Pid;

    let Some(pid) = child.id() else {
        let _ = child.start_kill();
        return;
    };
    let pgid = Pid::from_raw(pid as i32);
    let _ = killpg(pgid, Signal::SIGTERM);
    // Wait briefly for the group to drain, then SIGKILL the whole group so any
    // grandchild that ignored SIGTERM is still removed.
    let _ = tokio::time::timeout(PROCESS_GROUP_TERM_GRACE, child.wait()).await;
    let _ = killpg(pgid, Signal::SIGKILL);
}

/// Non-unix fallback: no process groups, so fall back to killing the direct
/// child (`kill_on_drop` remains the safety net on drop).
#[cfg(not(unix))]
async fn kill_child_tree(mut child: Child) {
    let _ = child.start_kill();
}

/// A single user text content block (the persisted/prompt message shape).
fn user_text_blocks(content: &str) -> Value {
    json!([{ "type": "text", "text": content }])
}

/// One `text` ACP prompt content block for a user message.
fn text_prompt(content: &str) -> Vec<ContentBlock> {
    serde_json::from_value(json!([{ "type": "text", "text": content }])).unwrap_or_default()
}

/// Resolved spawn inputs for an agent: the provider config plus the owned model,
/// cwd, and extra env the borrowing [`SpawnOptions`] reference during a spawn.
struct ResolvedSpawn {
    provider: ProviderConfig,
    model: Option<String>,
    cwd: PathBuf,
    extra_env: BTreeMap<String, String>,
}

/// Resolve the provider config, model, cwd, and extra env for spawning an
/// The default agent type for an agent with no specialist-declared `agentType`
/// (the foreground/interactive type, which has no internal tool denylist).
const DEFAULT_AGENT_TYPE: &str = "interactive";

/// Derive the spawn `agent_type` for a session (SP-B): when the session was
/// created with a specialist that declares an `agentType` frontmatter scalar
/// (e.g. `ralph` → `ralph-loop`), that value becomes the agent's type so the
/// matching internal tool denylist (§18.4,
/// [`get_tool_denylist_for_agent_type`](intent_acp::get_tool_denylist_for_agent_type))
/// engages on spawn. Otherwise (no specialist, or a specialist without
/// `agentType`) the existing [`DEFAULT_AGENT_TYPE`] is kept — no regression for
/// plain agents. The specialist project tier resolves from the workspace path.
fn derive_agent_type(
    services: &Services,
    session: &AgentSession,
    workspace: Option<&intent_core::Workspace>,
) -> String {
    if let Some(specialist) = session.specialist.as_deref().filter(|s| !s.is_empty()) {
        let workspace_path = workspace
            .and_then(|w| w.path.clone().or_else(|| w.worktree_path.clone()))
            .map(PathBuf::from);
        if let Some(agent_type) =
            services.specialist_agent_type(specialist, workspace_path.as_deref())
        {
            return agent_type;
        }
    }
    DEFAULT_AGENT_TYPE.to_string()
}

/// agent's child from its persisted session + workspace. The provider id comes
/// from the session's explicit `provider`, else the `provider:model` compound
/// id, else the default provider. The `mock` provider (E2E) reads its script
/// from `MOCK_AGENT_SCRIPT_PATH` and enables `--mcp-config` so a daemon-spawned
/// child reaches the per-agent workspace MCP server, forwarding
/// `MOCK_AGENT_BEHAVIOR` to the child.
fn resolve_spawn(
    session: &AgentSession,
    workspace: Option<&intent_core::Workspace>,
) -> Result<ResolvedSpawn> {
    let provider_id = session
        .provider
        .clone()
        .or_else(|| {
            session
                .model
                .as_ref()
                .map(|m| intent_providers::parse_compound_model_id(m).0)
        })
        .unwrap_or_else(|| intent_providers::default_provider_id().to_string());
    let model = session
        .model
        .as_ref()
        .map(|m| intent_providers::parse_compound_model_id(m).1)
        .filter(|m| !m.is_empty());
    let cwd = workspace
        .and_then(|w| w.path.clone().or_else(|| w.worktree_path.clone()))
        .map(PathBuf::from)
        .filter(|p| p.is_dir())
        .unwrap_or_else(std::env::temp_dir);

    let mut extra_env = BTreeMap::new();
    if provider_id == "mock" {
        let script = std::env::var("MOCK_AGENT_SCRIPT_PATH").map_err(|_| {
            Error::Internal("mock provider requires MOCK_AGENT_SCRIPT_PATH".to_string())
        })?;
        // `'static` leaks are bounded to the mock (E2E-only) path; real
        // providers carry static `base_args` and never leak.
        let script_static: &'static str = Box::leak(script.into_boxed_str());
        let base_args: &'static [&'static str] = Box::leak(vec![script_static].into_boxed_slice());
        if let Ok(behavior) = std::env::var("MOCK_AGENT_BEHAVIOR") {
            extra_env.insert("MOCK_AGENT_BEHAVIOR".to_string(), behavior);
        }
        let base = intent_providers::find_provider("mock")
            .ok_or_else(|| Error::Internal("mock provider missing from registry".to_string()))?;
        let provider = ProviderConfig {
            command: "node",
            base_args,
            supports_authenticate: true,
            supports_mcp_config: true,
            mcp_config_flag: Some("--mcp-config"),
            ..*base
        };
        return Ok(ResolvedSpawn {
            provider,
            model: None,
            cwd,
            extra_env,
        });
    }

    Ok(ResolvedSpawn {
        provider: *intent_providers::provider_config(&provider_id),
        model,
        cwd,
        extra_env,
    })
}

/// Background turn worker: drive the current message to completion, then drain
/// any queued messages (flipping each to in-flight). After the slot is released
/// the loop re-checks the queue and reclaims the slot **as long as another
/// message is waiting**; only when the queue is truly empty (or a concurrent
/// worker has won the slot) does the loop exit. Each dequeue publishes
/// `agent:queue:updated` so subscribed FE clients mirror the live queue
/// (§5.5/§6). Spawn/turn failures are logged so the loop always releases the
/// in-flight slot and worker handle.
async fn run_message_worker(
    mgr: Arc<AgentManager>,
    agent_id: AgentId,
    workspace_id: WorkspaceId,
    initial_content: String,
) {
    let mut content = initial_content;
    'outer: loop {
        match mgr.ensure_started(&agent_id, &workspace_id).await {
            Ok(acp_session_id) => {
                let prompt = mgr.build_turn_prompt(&agent_id, &content).await;
                if let Err(e) = mgr
                    .run_turn(&agent_id, &workspace_id, &acp_session_id, prompt)
                    .await
                {
                    tracing::warn!(agent = %agent_id, error = %e, "agent turn failed");
                }
            }
            Err(e) => {
                tracing::warn!(agent = %agent_id, error = %e, "agent spawn failed");
            }
        }
        // Drain the next queued message while still holding the in-flight slot.
        if let Some(next) = mgr.services.dequeue_message(&agent_id) {
            mgr.services
                .publish_queue_updated_for(
                    &agent_id,
                    &workspace_id,
                    mgr.services.queue_snapshot(&agent_id),
                )
                .await;
            persist_user(&mgr, &agent_id, &next.content).await;
            content = next.content;
            continue;
        }
        // Queue drained: release the slot, then re-check for a message that
        // raced in just before / after the release. The re-check is wrapped in
        // the outer `'outer` loop (not its own inner loop) so the agent never
        // goes idle while ready-to-send messages remain — each re-claim of the
        // slot continues `'outer` and re-enters the drain at the top.
        mgr.end_turn(&agent_id).await;
        let Some(next) = mgr.services.dequeue_message(&agent_id) else {
            break 'outer;
        };
        if mgr.try_begin(&agent_id, &workspace_id).await {
            mgr.services
                .publish_queue_updated_for(
                    &agent_id,
                    &workspace_id,
                    mgr.services.queue_snapshot(&agent_id),
                )
                .await;
            persist_user(&mgr, &agent_id, &next.content).await;
            content = next.content;
            continue 'outer;
        }
        // A concurrent send won the slot; hand the message back to it and
        // exit — that worker's own drain loop will pick it up.
        mgr.services.requeue_front(&agent_id, next);
        break 'outer;
    }
    mgr.clear_worker(&agent_id);
    // The agent finished its work (queue drained, slot released): raise the
    // server-owned `attention` blue dot so every client surfaces it (§9.9).
    if let Err(e) = mgr
        .services
        .raise_attention(&workspace_id, WorkspaceAttention::Unread)
        .await
    {
        tracing::warn!(agent = %agent_id, error = %e, "failed to raise attention");
    }
}

/// Persist a queued user message into the append-only transcript before its turn
/// (best-effort; a store error is logged and the turn still proceeds).
async fn persist_user(mgr: &AgentManager, agent_id: &AgentId, content: &str) {
    if let Err(e) = mgr
        .services
        .store
        .append_agent_message(agent_id, "user", &user_text_blocks(content), &now_iso())
        .await
    {
        tracing::warn!(agent = %agent_id, error = %e, "failed to persist queued user message");
    }
}

#[cfg(test)]
mod role_reminder_tests {
    //! Role-reminder injection cadence over [`AgentManager::build_turn_prompt`]
    //! (port of acp-provider.ts): every user turn (interval = 1) and also after a
    //! session recreate for specialist agents; never for non-specialist agents.

    use super::*;
    use crate::events::EventBus;
    use intent_core::{AgentStatus, Workspace, WorkspaceActivity, WorkspaceStatus};
    use intent_store::Store;

    /// Seed a hermetic specialists dir under temp with one `<id>.md`.
    fn write_specialist(id: &str, content: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("intentd-spc-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(format!("{id}.md")), content).unwrap();
        dir
    }

    fn workspace(id: &WorkspaceId) -> Workspace {
        let ts = now_iso();
        Workspace {
            id: id.clone(),
            title: "WS".to_string(),
            branch: "main".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: WorkspaceStatus::Active,
            status_message: None,
            activity: WorkspaceActivity::Idle,
            attention: WorkspaceAttention::None,
            created_at: ts.clone(),
            updated_at: ts,
            last_activity: None,
            tags: vec![],
            path: None,
            repository_path: None,
            repository_owner: None,
            repository_name: None,
            worktree_path: None,
            scope: None,
            skip_worktree: false,
            setup_script: None,
            is_remote: false,
            default_model: None,
            pr_number: None,
            pr_url: None,
            pr_status: None,
            active_pull_request: None,
            archived: false,
            archived_at: None,
            task_stats: None,
            agent_summary: None,
            diff_summary: None,
            token_usage: None,
        }
    }

    fn session(
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        specialist: Option<&str>,
    ) -> AgentSession {
        let ts = now_iso();
        AgentSession {
            id: agent_id.clone(),
            workspace_id: workspace_id.clone(),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: None,
            name: "Builder".to_string(),
            name_explicitly_set: false,
            model: None,
            provider: None,
            system_prompt: None,
            specialist: specialist.map(str::to_string),
            status: AgentStatus::Pending,
            is_active: true,
            messages: Vec::new(),
            stats: None,
            task_note_id: None,
            skip_auto_commit: false,
            completion_report: None,
            completion_report_timestamp: None,
            delegation_depth: None,
            initial_message: None,
            context_references: None,
            image_blocks: None,
            is_background: false,
            created_at: ts.clone(),
            updated_at: ts,
        }
    }

    /// Build a manager over a temp store seeded with a workspace + agent session.
    async fn manager_with(
        specialist: Option<&str>,
        specialists_dir: Option<PathBuf>,
    ) -> (AgentManager, AgentId) {
        let path = std::env::temp_dir().join(format!("intentd-rr-{}.db", uuid::Uuid::new_v4()));
        let store = Store::open(&path).await.expect("open store");
        let bus = EventBus::new(store.clone());
        let services = Services::new(store.clone())
            .with_event_bus(bus.clone())
            .with_specialist_dirs(
                specialists_dir,
                Some(std::env::temp_dir().join("nonexistent-bundled")),
            );
        let workspace_id = WorkspaceId::from("ws-1");
        let agent_id = AgentId::from("agent-1");
        store
            .insert_workspace(&workspace(&workspace_id))
            .await
            .unwrap();
        store
            .insert_agent_session(&session(&agent_id, &workspace_id, specialist))
            .await
            .unwrap();
        let sink = Arc::new(BusEventSink::new(bus));
        (AgentManager::new(services, sink, 4), agent_id)
    }

    /// First text block's text from a built prompt.
    fn prompt_text(prompt: &[ContentBlock]) -> String {
        serde_json::to_value(prompt).unwrap()[0]["text"]
            .as_str()
            .unwrap()
            .to_string()
    }

    #[tokio::test]
    async fn injects_reminder_every_turn_for_specialist() {
        let dir = write_specialist(
            "implementor",
            "---\nname: \"Implementor\"\ndescription: \"d\"\nroleReminder: \"Stay in scope.\"\n---\n\nbody",
        );
        let (mgr, agent_id) = manager_with(Some("implementor"), Some(dir)).await;
        // Interval = 1 → every turn carries the prefix.
        for _ in 0..2 {
            let prompt = mgr.build_turn_prompt(&agent_id, "do the thing").await;
            let text = prompt_text(&prompt);
            assert!(
                text.starts_with("[Role Reminder: You are a Implementor. Stay in scope.]\n\n"),
                "missing reminder prefix: {text:?}"
            );
            assert!(text.ends_with("do the thing"));
        }
    }

    #[tokio::test]
    async fn injects_reminder_on_session_recreate() {
        let dir = write_specialist(
            "implementor",
            "---\nname: \"Implementor\"\ndescription: \"d\"\nroleReminder: \"Stay in scope.\"\n---\n\nbody",
        );
        let (mgr, agent_id) = manager_with(Some("implementor"), Some(dir)).await;
        // Flag the agent's session as recreated; the reminder must still prepend.
        mgr.recreated.lock().unwrap().insert(agent_id.clone());
        let prompt = mgr.build_turn_prompt(&agent_id, "resume work").await;
        let text = prompt_text(&prompt);
        assert!(
            text.starts_with("[Role Reminder: You are a Implementor. Stay in scope.]\n\n"),
            "missing reminder prefix on recreate: {text:?}"
        );
        // Flag consumed by the turn.
        assert!(!mgr.recreated.lock().unwrap().contains(&agent_id));
    }

    #[tokio::test]
    async fn no_injection_without_specialist() {
        let (mgr, agent_id) = manager_with(None, None).await;
        let prompt = mgr.build_turn_prompt(&agent_id, "plain message").await;
        assert_eq!(prompt_text(&prompt), "plain message");
    }
}
