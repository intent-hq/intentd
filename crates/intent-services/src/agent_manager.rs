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
use std::time::{SystemTime, UNIX_EPOCH};

use intent_acp::session::{ContentBlock, StopReason};
use intent_acp::{
    build_baseline_mcp_env_from_process, handshake, serve_workspace_mcp_tcp, spawn_provider,
    to_auggie_mcp_config, ClientRequestHandler, Connection, ConnectionHooks, EventSink,
    FileService, IncomingNotification, IncomingRequest, McpBridge, NormalizedMcpServer,
    NormalizedMcpServers, PermissionPolicy, PermissionRegistry, SinkEvent, SpawnOptions,
    WorkspaceMcpServer,
};
use intent_core::{
    now_iso, AgentId, AgentSession, BoxFuture, Error, Result, WorkspaceApi, WorkspaceId,
};
use intent_providers::ProviderConfig;
use intent_store::NewEvent;
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
            let new_event = NewEvent {
                workspace_id: event.workspace_id,
                timestamp: now_iso(),
                event_type: event.event_type,
                actor: event.actor,
                session_id: event.session_id,
                correlation_id: None,
                parent_event_id: None,
                data: event.data,
            };
            if let Err(e) = self.bus.publish(&new_event).await {
                tracing::warn!(error = %e, "failed to publish agent client event");
            }
        })
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
/// request loop, the owned child (killed on drop via `kill_on_drop`), and the
/// per-agent MCP bridge + generated config that back the agent→BE tool loop.
struct AgentHandle {
    connection: Arc<Connection>,
    notifications: Arc<TokioMutex<mpsc::UnboundedReceiver<IncomingNotification>>>,
    serve_task: JoinHandle<()>,
    _child: Option<Child>,
    _mcp_bridge: Option<McpBridge>,
    _mcp_config: Option<TempConfigFile>,
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
    /// Abortable background turn workers, keyed by agent. `stop`/`forceMessage`
    /// abort the in-flight worker (interrupting the current stream).
    workers: Arc<Mutex<HashMap<AgentId, JoinHandle<()>>>>,
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
            policy: PermissionPolicy::default(),
            mcp_bridge_exe: std::env::current_exe().unwrap_or_else(|_| PathBuf::from("intentd")),
            busy: Arc::new(Mutex::new(HashSet::new())),
            workers: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Override the permission policy used by spawned agents' client handlers.
    pub fn with_policy(mut self, policy: PermissionPolicy) -> Self {
        self.policy = policy;
        self
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
        let server = Arc::new(WorkspaceMcpServer::for_agent_type(
            api,
            workspace_id.clone(),
            agent_type,
        ));
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

        // Reconstruct the spawn options with the generated config path injected.
        let mut spawn_opts = SpawnOptions::new(opts.provider);
        spawn_opts.model = opts.model;
        spawn_opts.cwd = opts.cwd;
        spawn_opts.rules_file = opts.rules_file;
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

        let handler = Arc::new(ClientRequestHandler::new(
            workspace_id,
            agent_id.clone(),
            agent_name.into(),
            FileService::new(cwd),
            self.permissions.clone(),
            self.policy,
            self.sink.clone(),
        ));
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
        };
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

    /// Complete the connection handshake and open an ACP session for a spawned
    /// agent (the agent→BE MCP server is delivered out-of-band via the generated
    /// `--mcp-config`, so `session/new` carries no `mcpServers`). Returns the
    /// persisted `acpSessionId` to drive [`AgentManager::run_turn`].
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
        handshake(conn.as_ref(), provider)
            .await
            .map_err(|e| Error::Internal(format!("handshake failed: {e}")))?;
        self.services
            .open_acp_session(conn.as_ref(), agent_id, cwd, Vec::new())
            .await
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
    /// stream), clear its busy flag, drop its handle (killing the child via
    /// `kill_on_drop` and aborting its request loop), and deregister it. Returns
    /// whether a handle existed. This is the `agent.stop` cancel semantics.
    pub async fn stop(&self, agent_id: &AgentId) -> bool {
        if let Some(worker) = self.workers.lock().unwrap().remove(agent_id) {
            worker.abort();
        }
        self.end_turn(agent_id);
        let removed = self.handles.lock().unwrap().remove(agent_id).is_some();
        self.registry.deregister(agent_id);
        removed
    }

    /// Whether a turn loop is currently in flight for `agent_id` (consulted by
    /// `agent.sendMessage` to decide queue-vs-stream).
    pub fn is_busy(&self, agent_id: &AgentId) -> bool {
        self.busy.lock().unwrap().contains(agent_id)
    }

    /// Atomically claim the in-flight slot: `true` when the agent was idle (now
    /// marked busy), `false` when a turn is already running.
    fn try_begin(&self, agent_id: &AgentId) -> bool {
        self.busy.lock().unwrap().insert(agent_id.clone())
    }

    /// Release the in-flight slot.
    fn end_turn(&self, agent_id: &AgentId) {
        self.busy.lock().unwrap().remove(agent_id);
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
        if !self.try_begin(&agent_id) {
            let queued = self.services.enqueue_message(&agent_id, content, None);
            return Ok(
                json!({ "success": true, "queued": true, "queuedMessage": queued.to_value() }),
            );
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
            // matching the `agent.sendMessage` fallback (PROTOCOL §5.5).
            self.end_turn(&agent_id);
            let queued = self.services.enqueue_message(&agent_id, content, None);
            return Ok(
                json!({ "success": true, "queued": true, "queuedMessage": queued.to_value() }),
            );
        }
        self.spawn_worker(agent_id, workspace_id, content);
        Ok(json!({ "success": true, "queued": false, "messageId": message_id }))
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
        self.services.clear_queue(&agent_id);
        let blocks = user_text_blocks(&content);
        self.services
            .store
            .append_agent_message(&agent_id, "user", &blocks, &now_iso())
            .await?;
        self.try_begin(&agent_id);
        self.spawn_worker(agent_id, workspace_id, content);
        Ok(json!({ "success": true, "queued": false, "messageId": message_id }))
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
            self.create_agent(
                agent_id.clone(),
                workspace_id.clone(),
                session.name.clone(),
                "interactive",
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

    /// Idle-reap hook: evict up to `max` idle agents in LRU order (full
    /// timer/memory-pressure reaping is M5).
    pub async fn reap_idle(&self, max: Option<usize>) -> usize {
        self.registry.evict_idle(max).await
    }

    /// Build the kill callback for `agent_id`: dropping the handle kills the
    /// child (`kill_on_drop`) and aborts its request loop.
    fn make_kill(&self, agent_id: AgentId) -> KillFn {
        let handles: Weak<Mutex<HashMap<AgentId, AgentHandle>>> = Arc::downgrade(&self.handles);
        Arc::new(move || {
            let handles = handles.clone();
            let id = agent_id.clone();
            Box::pin(async move {
                if let Some(handles) = handles.upgrade() {
                    handles.lock().unwrap().remove(&id);
                }
            })
        })
    }
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
/// any queued messages (flipping each to in-flight), re-checking once after the
/// busy flag clears to absorb a late enqueue. Spawn/turn failures are logged so
/// the loop always releases the in-flight slot and worker handle.
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
                if let Err(e) = mgr
                    .run_turn(
                        &agent_id,
                        &workspace_id,
                        &acp_session_id,
                        text_prompt(&content),
                    )
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
            persist_user(&mgr, &agent_id, &next.content).await;
            content = next.content;
            continue;
        }
        // Queue drained: release the slot, then re-check once for a message that
        // raced in just before the release (otherwise it would sit unworked).
        mgr.end_turn(&agent_id);
        if let Some(next) = mgr.services.dequeue_message(&agent_id) {
            if mgr.try_begin(&agent_id) {
                persist_user(&mgr, &agent_id, &next.content).await;
                content = next.content;
                continue 'outer;
            }
            // A concurrent send won the slot; hand the message back to it.
            mgr.services.requeue_front(&agent_id, next);
        }
        break;
    }
    mgr.clear_worker(&agent_id);
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
