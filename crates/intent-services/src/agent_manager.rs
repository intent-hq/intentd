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

use intent_acp::handshake::try_bypass_permissions_mode;
use intent_acp::session::{ContentBlock, SessionModeState, StopReason};
use intent_acp::{
    apply_baseline_env_to_stdio_servers, build_baseline_mcp_env_from_process, handshake,
    normalize_mcp_servers, serve_workspace_mcp_tcp, spawn_provider, to_auggie_mcp_config,
    ClientRequestHandler, Connection, ConnectionHooks, EnvMap, EventSink, FileService,
    IncomingNotification, IncomingRequest, McpBridge, NormalizedMcpServer, NormalizedMcpServers,
    PermissionOutcome, PermissionPolicy, PermissionRegistry, PermissionRequestData, SinkEvent,
    SpawnOptions, WorkspaceMcpServer,
};
use intent_core::events::AGENT_STATUS_CHANGED;
use intent_core::{
    now_iso, slug::is_workspace_slug, ActorType, AgentId, AgentSession, AgentStatus, BoxFuture,
    Error, EventActor, Result, WorkspaceApi, WorkspaceAttention, WorkspaceId,
};
use intent_providers::ProviderConfig;
use intent_store::{NewEvent, NewTrackedChange};
use serde_json::{json, Value};
use tokio::process::Child;
use tokio::sync::{mpsc, Mutex as TokioMutex};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::agent_ops::new_message_id;
use crate::agent_session::agent_actor;
use crate::events::EventBus;
use crate::Services;

#[cfg(test)]
mod tests;

/// Capitalize the leading ASCII byte of `s` (leaves the rest of the string
/// untouched). Used to normalize OAuth `token_type` values into the
/// conventional `Bearer` header form when a bag stores the RFC 6749 lower-case
/// spelling.
fn title_case_ascii(s: &str) -> String {
    let mut chars = s.chars();
    let Some(first) = chars.next() else {
        return String::new();
    };
    let mut out = String::with_capacity(s.len());
    out.push(first.to_ascii_uppercase());
    out.push_str(chars.as_str());
    out
}

const GB: u64 = 1024 * 1024 * 1024;

/// Per-turn prompt-assembly hints threaded through `agent.sendMessage` /
/// `agent.forceMessage` (PROTOCOL §5.5). `stdin_context` is prepended
/// verbatim to the outbound prompt as a `Context:` block (reference-parity
/// `acp-provider.ts`); `note_ids` and `context_references` are carried
/// forward for downstream note-image / context-reference resolution and are
/// otherwise inert today.
///
/// Only the FIRST turn triggered by a `sendMessage` / `forceMessage` call
/// carries these options; queue-drained follow-up turns run with
/// [`TurnOptions::default`] since a `QueuedMessage` has no per-turn hints of
/// its own.
#[derive(Debug, Default, Clone)]
pub struct TurnOptions {
    pub stdin_context: Option<String>,
    pub note_ids: Option<serde_json::Value>,
    pub context_references: Option<serde_json::Value>,
    /// FE-supplied image attachments: each `{ data, mimeType }` becomes an ACP
    /// `Image` content block appended after the text prompt (reference-parity
    /// `acp-provider.ts`).
    pub image_blocks: Option<serde_json::Value>,
    /// FE-supplied file attachments: each `{ data, mimeType, fileName }`
    /// becomes an ACP `Resource` content block (`EmbeddedResource` with
    /// `BlobResourceContents`) appended after the text prompt and any image
    /// blocks; the `fileName` becomes the resource `uri` as `file:///<name>`
    /// so downstream consumers can reference it.
    pub file_blocks: Option<serde_json::Value>,
    /// Opaque per-message payload from `agent.sendMessage` / `agent.forceMessage`
    /// `messageMetadata` (PROTOCOL §5.5). Persisted verbatim on the user
    /// message row (via [`Store::append_agent_message_with_metadata`]) for the
    /// FIRST turn only; queue-drained follow-up turns run with
    /// [`TurnOptions::default`] and therefore carry no metadata of their own.
    pub message_metadata: Option<serde_json::Value>,
}

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
            // Shipped default (§6.7/M3.5): `AllowAll` for reference parity with
            // the TS acp-provider — [`start_session`] additionally attempts
            // `session/set_mode bypassPermissions` on providers that advertise
            // set-mode (auggie today), and the local `AllowAll` auto-approve
            // handles anything the provider still surfaces. An FE-attached
            // deployment selects `Interactive` via `with_policy()` (wired from
            // `INTENTD_PERMISSION_POLICY`) to drive the
            // `agent.respondPermission` / `agent.pendingPermissions` RPCs;
            // `AutoByRisk` / `DenyAll` remain selectable via the same env var.
            policy: PermissionPolicy::AllowAll,
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
            let config = self.generate_mcp_config(&bridge).await?;
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
        // files, plus — for specialist agents — the PP-1 `<specialist_role>`
        // section and role-reminder footer, and — for top-level agents — the
        // SP-1 `## Suggested Next Steps` directive) into a temp `--rules` file
        // when the caller supplies none. The handle owns the temp file so it
        // outlives the child that reads it.
        let mut rules_config: Option<TempConfigFile> = None;
        let mut rules_file_path: Option<String> = None;
        if opts.rules_file.is_none() {
            let specialist = self
                .services
                .agent_specialist_injection(&agent_id, Some(&cwd))
                .await;
            // `git.autoCommit` is a global (non-workspace-scoped) setting, so
            // this lookup is independent of the session and cheap to do here.
            let auto_commit_enabled =
                crate::settings::auto_commit_enabled(&self.services.store).await;
            // Sub-agent gating: delegated children (`parent_agent_id` set) and
            // background workers (`is_background`) skip the suggested-prompts
            // directive, matching the reference `isSubAgent` derivation. The
            // session was inserted by the caller before `create_agent` runs,
            // so propagate any store error rather than silently defaulting to
            // top-level (which would mis-scope the SP-1 footer and hide DB
            // failures).
            let session = self.services.store.get_agent_session(&agent_id).await?;
            let is_sub_agent = session.parent_agent_id.is_some() || session.is_background;
            if let Some(prompt) = crate::rules::assemble_system_prompt(
                &self.services.store,
                Some(&cwd),
                agent_type,
                specialist.as_ref(),
                is_sub_agent,
                auto_commit_enabled,
            )
            .await
            {
                let path =
                    std::env::temp_dir().join(format!("intentd-rules-{}.md", Uuid::new_v4()));
                std::fs::write(&path, prompt.as_bytes())
                    .map_err(|e| Error::Internal(format!("write rules file failed: {e}")))?;
                rules_file_path = Some(path.to_string_lossy().into_owned());
                rules_config = Some(TempConfigFile { path });
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
        spawn_opts.tools_to_remove = opts.tools_to_remove.clone();
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
        // Pre-first-token turn-startup hint: the child process is about to be
        // spawned for this agent, so surface the `launch` phase before the
        // (potentially slow) `spawn_provider` call blocks the turn (STAT-1 /
        // PROTOCOL §7). Emitted whether or not a session is subsequently opened
        // — the parent turn may still be gated on the child coming up.
        self.services
            .publish_status_event(
                &workspace_id,
                &agent_id,
                "launch",
                "Launching agent\u{2026}",
                "info",
            )
            .await;
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
    /// subcommand, with the user's `mcp.servers` catalog merged in and the safe
    /// baseline env injected across every stdio entry (§6.8, §18.4). Mirrors
    /// the FE `mergeUserMcpServersWithAuth` path: honours the
    /// `mcp.enableUserServers` gate, filters out globally-disabled servers, and
    /// — for http/sse transports — injects an `Authorization` header from the
    /// persisted OAuth token bag when the catalog entry does not already set
    /// one. `workspace-mcp` is reserved and never overridden.
    async fn generate_mcp_config(&self, bridge: &McpBridge) -> Result<serde_json::Value> {
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
                env: EnvMap::new(),
            },
        );
        self.merge_user_mcp_servers(&mut servers).await?;
        let baseline = build_baseline_mcp_env_from_process();
        let servers = apply_baseline_env_to_stdio_servers(&servers, &baseline);
        Ok(to_auggie_mcp_config(&servers))
    }

    /// Fold user-configured MCP servers (sensitive `mcp.servers` secret) into
    /// `out`, honouring the `mcp.enableUserServers` gate and the global
    /// `mcp.disabledServers` list, and injecting an `Authorization` header from
    /// the persisted OAuth bag on http/sse entries when the catalog does not
    /// already set one. Any config that collides with a reserved built-in name
    /// (e.g. `workspace-mcp`) is skipped so the bridge cannot be shadowed.
    async fn merge_user_mcp_servers(&self, out: &mut NormalizedMcpServers) -> Result<()> {
        if !crate::mcp_servers::enable_user_servers(&self.services.store).await {
            return Ok(());
        }
        let configs = crate::mcp_servers::read_configs(&self.services.secrets).await;
        if configs.is_empty() {
            return Ok(());
        }
        let disabled = crate::mcp_servers::disabled_servers(&self.services.store).await;
        let disabled: HashSet<&str> = disabled.iter().map(String::as_str).collect();

        let mut reshaped = serde_json::Map::new();
        for (id, cfg) in &configs {
            let Some(obj) = cfg.as_object() else { continue };
            if !obj.get("enabled").and_then(Value::as_bool).unwrap_or(false) {
                continue;
            }
            if disabled.contains(id.as_str()) {
                continue;
            }
            let name = obj
                .get("name")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .unwrap_or(id.as_str())
                .to_string();
            if out.contains_key(&name) {
                tracing::debug!(server = %name, "user MCP server collides with reserved name; skipping");
                continue;
            }
            let Some(entry) = self.reshape_user_mcp_config(id, obj).await? else {
                continue;
            };
            reshaped.insert(name, entry);
        }
        if reshaped.is_empty() {
            return Ok(());
        }
        let normalized = normalize_mcp_servers(&Value::Object(reshaped));
        for (name, server) in normalized {
            out.entry(name).or_insert(server);
        }
        Ok(())
    }

    /// Reshape one `mcp.servers` entry into the shape [`normalize_mcp_servers`]
    /// expects — stdio entries stay untouched (`command`/`args`/`env`), remote
    /// entries get a `type` tag plus an `Authorization` header sourced from the
    /// persisted OAuth bag when the config does not already set one. Returns
    /// `None` for malformed entries (missing `command`/`url`) so they drop out
    /// of the merge silently.
    async fn reshape_user_mcp_config(
        &self,
        id: &str,
        obj: &serde_json::Map<String, Value>,
    ) -> Result<Option<Value>> {
        let transport = obj
            .get("transport")
            .and_then(Value::as_str)
            .unwrap_or("stdio");
        let mut out = serde_json::Map::new();
        match transport {
            "http" | "sse" => {
                let Some(url) = obj
                    .get("url")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                else {
                    return Ok(None);
                };
                out.insert("type".into(), Value::String(transport.to_string()));
                out.insert("url".into(), Value::String(url.to_string()));
                let mut headers = obj
                    .get("headers")
                    .and_then(Value::as_object)
                    .cloned()
                    .unwrap_or_default();
                let has_auth = headers
                    .keys()
                    .any(|k| k.eq_ignore_ascii_case("authorization"));
                if !has_auth {
                    if let Some(auth) = self.oauth_authorization_header(id).await? {
                        headers.insert("Authorization".to_string(), Value::String(auth));
                    }
                }
                if !headers.is_empty() {
                    out.insert("headers".into(), Value::Object(headers));
                }
            }
            _ => {
                let Some(command) = obj
                    .get("command")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                else {
                    return Ok(None);
                };
                out.insert("command".into(), Value::String(command.to_string()));
                if let Some(a) = obj.get("args") {
                    out.insert("args".into(), a.clone());
                }
                if let Some(e) = obj.get("env") {
                    out.insert("env".into(), e.clone());
                }
            }
        }
        Ok(Some(Value::Object(out)))
    }

    /// Build the `Authorization: <token_type> <access_token>` header value from
    /// the persisted OAuth bag for `server_id`, or `None` when no bag is
    /// stored / the bag is malformed / `access_token` is missing. `token_type`
    /// defaults to `Bearer` and is title-cased so a bag storing the RFC 6749
    /// lower-case `bearer` still produces the conventional header form.
    async fn oauth_authorization_header(&self, server_id: &str) -> Result<Option<String>> {
        let Some(raw) = self.services.store.get_mcp_oauth_token(server_id).await? else {
            return Ok(None);
        };
        let Ok(bag) = serde_json::from_str::<Value>(&raw) else {
            return Ok(None);
        };
        let Some(access) = bag
            .get("access_token")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
        else {
            return Ok(None);
        };
        let token_type = bag
            .get("token_type")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .unwrap_or("Bearer");
        Ok(Some(format!("{} {}", title_case_ascii(token_type), access)))
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
        // Load the agent session record once and reuse both `workspace_id` (for
        // the pre-handshake status hint) and `acp_session_id` (for the resume
        // branch decision below) from the same struct.
        let session_record = self.services.store.get_agent_session(agent_id).await?;
        // Pre-first-token turn-startup hint: the ACP `initialize` handshake is
        // about to run for this agent (STAT-1 / PROTOCOL §7). The status payload
        // carries `workspaceId` (the FE routes hints per-agent but callers key
        // the timeline on it).
        self.services
            .publish_status_event(
                &session_record.workspace_id,
                agent_id,
                "init",
                "Initializing protocol\u{2026}",
                "info",
            )
            .await;
        let handshake = handshake(conn.as_ref(), provider)
            .await
            .map_err(|e| Error::Internal(format!("handshake failed: {e}")))?;

        // The persisted id (if any) decides the no-resume branch: a brand-new
        // agent (no id) opens a first session; an agent with a lost id recreates
        // (CAS-replacing exactly this id) and resends history.
        let stored_id = session_record.acp_session_id;

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
            Ok(Some(opened)) => {
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
                self.maybe_bypass_permissions(
                    conn.as_ref(),
                    provider,
                    &opened.session_id,
                    opened.modes.as_ref(),
                )
                .await;
                return Ok(opened.session_id);
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
            let opened = self
                .services
                .recreate_acp_session(conn.as_ref(), agent_id, &expected_old, cwd, Vec::new())
                .await?;
            self.recreated.lock().unwrap().insert(agent_id.clone());
            self.maybe_bypass_permissions(
                conn.as_ref(),
                provider,
                &opened.session_id,
                opened.modes.as_ref(),
            )
            .await;
            return Ok(opened.session_id);
        }

        // 3) Brand-new agent → open and persist the first session (write-once).
        let opened = self
            .services
            .open_acp_session(conn.as_ref(), agent_id, cwd, Vec::new())
            .await?;
        self.maybe_bypass_permissions(
            conn.as_ref(),
            provider,
            &opened.session_id,
            opened.modes.as_ref(),
        )
        .await;
        Ok(opened.session_id)
    }

    /// Under the shipped `AllowAll` policy, best-effort ask the provider to run
    /// in a permissive mode via `session/set_mode` (parity with the TS
    /// acp-provider). The mode id is picked by
    /// [`try_bypass_permissions_mode`] from the modes the provider actually
    /// advertised in `session/new` / `session/load`, so agents that don't
    /// offer a bypass-equivalent (auggie today) are left alone rather than
    /// triggering a `-32602`; every other policy is a no-op so Interactive /
    /// `AutoByRisk` / `DenyAll` decisions stay authoritative.
    async fn maybe_bypass_permissions(
        &self,
        conn: &Connection,
        provider: &ProviderConfig,
        acp_session_id: &str,
        modes: Option<&SessionModeState>,
    ) {
        if self.policy != PermissionPolicy::AllowAll {
            return;
        }
        try_bypass_permissions_mode(conn, provider, acp_session_id, modes).await;
    }

    /// Take (clear) the recreate flag for `agent_id`: `true` when the agent's ACP
    /// session was recreated by the resume-impossible fallback since the last
    /// turn, meaning the next prompt must resend the prior conversation history.
    fn take_recreated(&self, agent_id: &AgentId) -> bool {
        self.recreated.lock().unwrap().remove(agent_id)
    }
    /// Compute the fire-once workspace-naming instruction for the outbound
    /// prompt, or `None` when it should be omitted. Ported from the reference
    /// `agent-backend-handler.service.ts` (`namingInstructions` block):
    ///
    /// * Fires only on the agent's **first** turn — detected by the absence of
    ///   any prior `assistant` message in the persisted transcript.
    /// * Fires only when the workspace lookup succeeds AND the current title
    ///   is empty/whitespace OR still shaped like an auto-generated slug
    ///   ([`intent_core::slug::is_workspace_slug`]).
    /// * Names the concrete daemon tool the agent must call
    ///   (`set_workspace_title_workspace-mcp`), not the FE `workspace_api`
    ///   JS surface (which daemon-spawned agents do not have).
    ///
    /// The agent-rename half of the reference block is intentionally SKIPPED:
    /// the daemon currently exposes no `set_agent_name` tool. Restore that
    /// branch once such a tool exists.
    async fn build_workspace_naming_instruction(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
    ) -> Option<String> {
        let messages = self
            .services
            .store
            .get_agent_messages(agent_id, None)
            .await
            .ok()?;
        if messages.iter().any(|m| m.role == "assistant") {
            return None;
        }
        let workspace = self.services.store.get_workspace(workspace_id).await.ok()?;
        let title = workspace.title.trim();
        let needs_rename = title.is_empty() || is_workspace_slug(title);
        if !needs_rename {
            return None;
        }
        Some(
            "<system>\nThis workspace needs a title. As your first action, call the `set_workspace_title_workspace-mcp` tool with a short 3\u{2013}5 word sentence-case title describing the task. This can be called in parallel with information-gathering.\n</system>"
                .to_string(),
        )
    }

    /// Build the prompt blocks for an agent's next turn. Normally just the user
    /// `content`; but when the ACP session was recreated (the resume-impossible
    /// fallback), prepend the prior conversation history as `<supervisor>` XML so
    /// the fresh session has context, then clear the flag (parity: TS
    /// `sessionWasRecreated` → `formatHistoryAsXml`). The just-persisted current
    /// user message is excluded from the rendered history.
    ///
    /// When `options.stdin_context` is set the prompt is prefixed with a
    /// `Context:\n<stdin>\n\n---\n\n` block, reference-parity with
    /// `acp-provider.ts`; other [`TurnOptions`] fields are reserved for
    /// downstream note-image / context-reference resolution.
    async fn build_turn_prompt(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        content: &str,
        options: &TurnOptions,
    ) -> Vec<ContentBlock> {
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
        // Fire-once workspace-naming instruction (port of
        // `agent-backend-handler.service.ts` `namingInstructions`): on the
        // first turn of an agent in a still-untitled / slug-titled workspace,
        // prepend a `<system>` block asking the agent to set the workspace
        // title as its first action. Never mutates the persisted user
        // message; agent-rename half is deferred until the daemon exposes a
        // `set_agent_name` tool.
        let naming = self
            .build_workspace_naming_instruction(agent_id, workspace_id)
            .await;
        let prompt_text = match naming {
            Some(sys) => format!("{sys}\n\n{prompt_text}"),
            None => prompt_text,
        };
        // `stdinContext` is prepended verbatim as a `Context:` block; the
        // trailing separator matches the reference `acp-provider.ts` so
        // downstream consumers see the same shape whether the prompt
        // originates from the daemon or the legacy Electron main path.
        // When `stdinContext` is absent/empty we synthesise one from
        // `contextReferences` (port of the FE reference builder in
        // `agent-backend-handler.service.ts`); an explicit `stdinContext`
        // always wins.
        let synthesised = match options.stdin_context.as_deref() {
            Some(ctx) if !ctx.is_empty() => None,
            _ => build_stdin_context_from_context_references(options.context_references.as_ref()),
        };
        let prompt_text = match options.stdin_context.as_deref() {
            Some(ctx) if !ctx.is_empty() => format!("Context:\n{ctx}\n\n---\n\n{prompt_text}"),
            _ => match synthesised.as_deref() {
                Some(ctx) if !ctx.is_empty() => {
                    format!("Context:\n{ctx}\n\n---\n\n{prompt_text}")
                }
                _ => prompt_text,
            },
        };
        let mut blocks = text_prompt(&prompt_text);
        append_attachment_blocks(&mut blocks, options);
        // Resolve `noteIds` to `workspace-asset://` image content blocks
        // (Fidelity B, PROTOCOL §5.5): each note is scanned for markdown
        // image references whose URL is a workspace-asset in the current
        // workspace; the referenced bytes are loaded and appended as ACP
        // `image` content blocks. A single system text block is added when
        // any images are resolved so the agent knows they are inlined for
        // direct viewing (parity with the FE notice).
        if let Some(ids_json) = options.note_ids.as_ref() {
            let ids: Vec<String> = ids_json
                .as_array()
                .map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default();
            if !ids.is_empty() {
                let images = self
                    .services
                    .load_note_image_blocks(workspace_id, &ids)
                    .await;
                if !images.is_empty() {
                    for (data, mime) in &images {
                        if let Ok(img) = serde_json::from_value::<ContentBlock>(json!({
                            "type": "image",
                            "data": data,
                            "mimeType": mime,
                        })) {
                            blocks.push(img);
                        }
                    }
                    let notice = format!(
                        "[System: {n} image(s) from the referenced note(s) are attached to this message.]",
                        n = images.len(),
                    );
                    blocks.extend(text_prompt(&notice));
                }
            }
        }
        blocks
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
            // STAB-28: emit agent:idle after interrupt so completion watches fire.
            // The aborted worker never reaches run_prompt_turn's idle-emit path, so
            // we must emit here. Without this, a parent that re-messages via agent.send
            // after the child settles registers a completion watch that never fires
            // (no idle event → watch never delivered). Only emit when the agent has
            // no queued ready-to-send messages (mirrors run_prompt_turn line 587).
            if !self.services.has_ready_to_send(agent_id) {
                let mut data = json!({
                    "agentId": agent_id.0,
                    "reason": "interrupted",
                    "status": "idle",
                });
                // Enrich with agentName + completion report (reuse session loaded at
                // line 1466; avoids duplicate I/O).
                if let Some(ref session) = session {
                    data["agentName"] = json!(session.name);
                    if let Some(ref report) = session.completion_report {
                        data["report"] = json!(report);
                    }
                }
                self.services
                    .publish_agent_event(
                        &workspace_id,
                        agent_id,
                        intent_core::events::AGENT_IDLE,
                        data,
                    )
                    .await;
            }
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

    /// Release the in-flight slot without persisting agent status (used when
    /// terminal spawn failure already persisted Error status and we only need
    /// to release busy/agent_ws so a future message can restart the worker).
    async fn release_in_flight_slot(&self, agent_id: &AgentId) {
        let was_busy = self.busy.lock().unwrap().remove(agent_id);
        if !was_busy {
            return;
        }
        let workspace_id = self.agent_ws.lock().unwrap().remove(agent_id);
        if let Some(workspace_id) = workspace_id {
            self.services.agent_activity_end(&workspace_id).await;
        }
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
            .set_agent_session_status(workspace_id, agent_id, status, is_active, &ts)
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
        options: TurnOptions,
    ) -> Result<Value> {
        if !self.try_begin(&agent_id, &workspace_id).await {
            let (queued, position) = self.services.enqueue_message(
                &agent_id,
                content,
                options.image_blocks.clone(),
                options.file_blocks.clone(),
            );
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
            .append_agent_message_with_metadata(
                &agent_id,
                "user",
                &blocks,
                options.message_metadata.as_ref(),
                &now_iso(),
            )
            .await
            .is_err()
        {
            // Store write failed (e.g. session not yet persisted) → auto-queue,
            // matching the `agent.sendMessage` fallback (PROTOCOL §5.5). Self-drain:
            // the slot we just released will be reclaimed below if the queue is
            // ready and the agent is otherwise free.
            self.end_turn(&agent_id).await;
            let (queued, position) = self.services.enqueue_message(
                &agent_id,
                content,
                options.image_blocks.clone(),
                options.file_blocks.clone(),
            );
            let result = json!({
                "success": true,
                "queued": true,
                "queuedMessage": queued.to_value(position),
            });
            self.services.publish_queue_updated(&agent_id).await;
            self.clone().try_drain_queue(agent_id, workspace_id).await;
            return Ok(result);
        }
        self.spawn_worker(agent_id, workspace_id, content, options);
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
        persist_user(&self, &agent_id, &workspace_id, &next.content).await;
        // Queue-drained turns carry no per-turn prompt hints of their own,
        // but the FE-supplied attachments captured at enqueue time do ride
        // along so the drained turn receives the same image + file blocks.
        let options = TurnOptions {
            image_blocks: next.image_blocks.clone(),
            file_blocks: next.file_blocks.clone(),
            ..TurnOptions::default()
        };
        self.spawn_worker(agent_id, workspace_id, next.content, options);
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
        options: TurnOptions,
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
            .append_agent_message_with_metadata(
                &agent_id,
                "user",
                &blocks,
                options.message_metadata.as_ref(),
                &now_iso(),
            )
            .await?;
        self.try_begin(&agent_id, &workspace_id).await;
        self.spawn_worker(agent_id, workspace_id, content, options);
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
        options: TurnOptions,
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
        self.send_message(agent_id, workspace_id, content, message_id, options)
            .await
    }

    /// Spawn (and track) the background turn worker for an agent. The caller must
    /// already hold the in-flight slot (`try_begin`).
    fn spawn_worker(
        self: &Arc<Self>,
        agent_id: AgentId,
        workspace_id: WorkspaceId,
        content: String,
        options: TurnOptions,
    ) {
        let mgr = self.clone();
        let id = agent_id.clone();
        let handle = tokio::spawn(async move {
            run_message_worker(mgr, id, workspace_id, content, options).await;
        });
        self.workers.lock().unwrap().insert(agent_id, handle);
    }

    /// Claim the in-flight slot for a delivery-driven turn. Companion to
    /// [`AgentManager::finish_prepersisted_turn_spawn`] and
    /// [`AgentManager::release_slot`]: the caller uses this two-step protocol
    /// so the user-message row is persisted BETWEEN slot claim and worker
    /// spawn — a persist failure at that point releases the slot without ever
    /// having launched a worker that could produce assistant output for a
    /// row that isn't in the transcript.
    ///
    /// Returns `true` when the slot was claimed, `false` when a turn was
    /// already in flight (the caller must enqueue instead).
    pub(crate) async fn try_begin_turn(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
    ) -> bool {
        self.try_begin(agent_id, workspace_id).await
    }

    /// Retry a failed agent spawn (`agent.retry` RPC path). Only valid when
    /// the agent status is `error`; returns `{ ok: false }` otherwise. Clears
    /// the error status back to pending, tears down any stale child, and
    /// attempts to redrive the front-of-queue message (requeued at exhaustion)
    /// plus any subsequent messages. Reuses the spawn-retry/backoff machinery,
    /// so a retry that fails again lands back in the `error` state with events.
    pub async fn agent_retry(
        self: &Arc<Self>,
        agent_id: AgentId,
        _workspace_id: WorkspaceId,
    ) -> Result<Value> {
        // Fetch current session status
        let session = self.services.store.get_agent_session(&agent_id).await?;

        // Only allow retry when the session status is `error`
        if session.status != AgentStatus::Error {
            return Ok(json!({ "ok": false }));
        }

        // Use the session's persisted workspace_id for safety (cross-workspace guard)
        let workspace_id = &session.workspace_id;

        // Clear the error status back to pending
        let ts = now_iso();
        self.services
            .store
            .set_agent_session_status(workspace_id, &agent_id, AgentStatus::Pending, false, &ts)
            .await?;

        // Emit agent:status-changed
        let event = NewEvent {
            workspace_id: workspace_id.clone(),
            timestamp: ts,
            event_type: AGENT_STATUS_CHANGED.to_string(),
            actor: agent_actor(&agent_id),
            session_id: Some(agent_id.0.clone()),
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data: json!({
                "agentId": agent_id.0,
                "status": "pending",
                "isActive": false,
            }),
        };
        crate::publish_event(&self.services.event_bus, event).await;

        // Abort any in-flight worker task and release the in-flight slot
        if let Some(worker) = self.workers.lock().unwrap().remove(&agent_id) {
            worker.abort();
        }
        self.release_in_flight_slot(&agent_id).await;

        // Tear down any stale child handle (use kill_child_only to avoid
        // overwriting the status we just set to Pending)
        self.kill_child_only(&agent_id).await;

        // Start the drain loop to redrive the requeued message
        self.clone()
            .try_drain_queue(agent_id, workspace_id.clone())
            .await;

        Ok(json!({ "ok": true }))
    }

    /// Spawn the background turn worker after the caller has already claimed
    /// the in-flight slot via [`AgentManager::try_begin_turn`] AND persisted
    /// the user-message row. The worker path does NOT re-persist the initial
    /// `content` (it flows in-memory to `build_turn_prompt`), so the persist
    /// MUST have succeeded before this call.
    pub(crate) fn finish_prepersisted_turn_spawn(
        self: &Arc<Self>,
        agent_id: AgentId,
        workspace_id: WorkspaceId,
        content: String,
        options: TurnOptions,
    ) {
        self.spawn_worker(agent_id, workspace_id, content, options);
    }

    /// Release an in-flight slot claimed via [`AgentManager::try_begin_turn`]
    /// but not followed by a worker spawn (persist failed before
    /// [`AgentManager::finish_prepersisted_turn_spawn`]). Public-in-crate seam
    /// so `Services::deliver_wake_message` can hand control back to the drain
    /// loop after a store error, mirroring the `send_message` self-drain path.
    pub(crate) async fn release_slot(&self, agent_id: &AgentId) {
        self.end_turn(agent_id).await;
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
            // §18.4 CLI-side denylist: strip provider-native tools (e.g.
            // auggie's built-in `str-replace-editor`, `sub-agent-*`) via
            // `--remove-tool`. MCP-side filtering (§6.8) already blocks
            // workspace-MCP tools, but the provider's native tools can only be
            // stripped through this spawn-time flag.
            opts.tools_to_remove =
                intent_acp::get_tools_to_remove(session.specialist.as_deref(), &agent_type);
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

    /// Tear down only the agent's child process + handle, without touching the
    /// worker or busy flag. Safe to call from within the worker itself (e.g.,
    /// retry loop). Use `stop()` for full teardown from external callers.
    async fn kill_child_only(&self, agent_id: &AgentId) {
        let handle = self.handles.lock().unwrap().remove(agent_id);
        if let Some(mut handle) = handle {
            if let Some(child) = handle._child.take() {
                kill_child_tree(child).await;
            }
        }
        self.registry.deregister(agent_id);
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

/// Port of the FE `contextReferences` → `stdinContext` builder
/// (`agent-backend-handler.service.ts` — the ~3170–3248 block). Iterates the
/// raw JSON array in order and emits one context entry per reference,
/// joined by `\n\n`. Only entries the reference supports today are
/// materialised: type-specific labels for `selection` / `task` /
/// `code_chunk` / `file` (with content) / `linear-issue` / `github-issue` /
/// `sentry-issue` / `terminal`, a `Note: <id>` line for `note`, a bare
/// `File: <path>` line for a file reference whose content was not inlined
/// on the wire (the FE variant would try to read from disk here — that
/// on-disk fallback is deferred), and a fall-through that emits the raw
/// content when no `type` matches. Returns `None` when nothing produces a
/// non-empty entry so the caller can leave the prompt untouched.
fn build_stdin_context_from_context_references(refs: Option<&Value>) -> Option<String> {
    let arr = refs?.as_array()?;
    if arr.is_empty() {
        return None;
    }
    let mut parts: Vec<String> = Vec::new();
    for r in arr {
        let obj = match r.as_object() {
            Some(o) => o,
            None => continue,
        };
        // Content resolution mirrors the FE: `content` → `selectedText` →
        // `taskText` → `codeChunk` (first non-empty wins).
        let content = ["content", "selectedText", "taskText", "codeChunk"]
            .iter()
            .find_map(|k| obj.get(*k).and_then(Value::as_str))
            .filter(|s| !s.is_empty());
        // Same aliasing rule for the path field.
        let file_path = obj
            .get("path")
            .or_else(|| obj.get("filePath"))
            .and_then(Value::as_str);
        let ref_type = obj.get("type").and_then(Value::as_str);
        if let Some(content) = content {
            let entry = match ref_type {
                Some("selection") => format!("Selected text:\n{content}"),
                Some("task") => format!("Task:\n{content}"),
                Some("code_chunk") => format!("Code:\n{content}"),
                Some("file") => match file_path {
                    Some(p) => format!("File {p}:\n{content}"),
                    None => content.to_string(),
                },
                Some("linear-issue") => format!("Linear Issue:\n{content}"),
                Some("github-issue") => format!("GitHub Issue:\n{content}"),
                Some("sentry-issue") => format!("Sentry Issue:\n{content}"),
                Some("terminal") => {
                    let meta = obj.get("metadata").and_then(Value::as_object);
                    let terminal_id = meta
                        .and_then(|m| m.get("terminalId"))
                        .and_then(Value::as_str)
                        .unwrap_or("unknown");
                    let terminal_name = meta
                        .and_then(|m| m.get("terminalName"))
                        .and_then(Value::as_str)
                        .or_else(|| obj.get("title").and_then(Value::as_str))
                        .unwrap_or("Terminal");
                    format!("Terminal \"{terminal_name}\" (terminal_id: {terminal_id}):\n{content}")
                }
                _ => content.to_string(),
            };
            parts.push(entry);
        } else if ref_type == Some("file") {
            if let Some(p) = file_path {
                // Reference builds a bare `File: <path>` line when content is
                // not inlined and disk read is skipped/unavailable.
                parts.push(format!("File: {p}"));
            }
        } else if ref_type == Some("note") {
            let note_id = obj.get("noteId").and_then(Value::as_str).or_else(|| {
                obj.get("metadata")
                    .and_then(Value::as_object)
                    .and_then(|m| m.get("noteId"))
                    .and_then(Value::as_str)
            });
            if let Some(id) = note_id {
                parts.push(format!("Note: {id}"));
            }
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("\n\n"))
    }
}

/// Append one ACP content block per FE-supplied attachment to `blocks`
/// (reference-parity `acp-provider.ts`): image entries `{ data, mimeType }`
/// become `image` content blocks; file entries `{ data, mimeType, fileName }`
/// become `resource` blocks carrying a `BlobResourceContents` with the file
/// name lifted into the resource URI (`file:///<fileName>`). Malformed entries
/// (missing required fields, wrong types) are silently skipped so a partial
/// attachment array can never break the turn.
fn append_attachment_blocks(blocks: &mut Vec<ContentBlock>, options: &TurnOptions) {
    if let Some(imgs) = options.image_blocks.as_ref().and_then(Value::as_array) {
        for img in imgs {
            let data = img.get("data").and_then(Value::as_str);
            let mime = img.get("mimeType").and_then(Value::as_str);
            if let (Some(data), Some(mime)) = (data, mime) {
                if let Ok(block) = serde_json::from_value::<ContentBlock>(json!({
                    "type": "image",
                    "data": data,
                    "mimeType": mime,
                })) {
                    blocks.push(block);
                }
            }
        }
    }
    if let Some(files) = options.file_blocks.as_ref().and_then(Value::as_array) {
        for file in files {
            let data = file.get("data").and_then(Value::as_str);
            let mime = file.get("mimeType").and_then(Value::as_str);
            let name = file.get("fileName").and_then(Value::as_str);
            if let (Some(data), Some(mime), Some(name)) = (data, mime, name) {
                if let Ok(block) = serde_json::from_value::<ContentBlock>(json!({
                    "type": "resource",
                    "resource": {
                        "blob": data,
                        "mimeType": mime,
                        "uri": format!("file:///{name}"),
                    },
                })) {
                    blocks.push(block);
                }
            }
        }
    }
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
    // Chief has no worktree/repo on disk; the TS `agent-factory` fallback
    // (`workspace.id === CHIEF_WORKSPACE_ID ? '/tmp' : undefined`) pins its
    // spawn `cwd` to `/tmp` so provider processes have a stable, existing
    // working directory instead of `std::env::temp_dir()`'s longer
    // `/var/folders/…/T/` path.
    //
    // Task 3: If the session has a sandbox_path (CoW isolation), use it as the cwd.
    let cwd = session
        .sandbox_path
        .clone()
        .map(PathBuf::from)
        .filter(|p| p.is_dir())
        .or_else(|| {
            workspace
                .and_then(|w| w.path.clone().or_else(|| w.worktree_path.clone()))
                .map(PathBuf::from)
                .filter(|p| p.is_dir())
        })
        .or_else(|| {
            workspace
                .filter(|w| w.id.is_chief())
                .map(|_| PathBuf::from("/tmp"))
                .filter(|p| p.is_dir())
        })
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
    initial_options: TurnOptions,
) {
    let mut content = initial_content;
    // Only the first turn carries the caller's per-turn prompt-assembly hints
    // (`stdinContext` / `noteIds` / `contextReferences`) — a `QueuedMessage`
    // has none. Attachment blocks (`imageBlocks` / `fileBlocks`) are captured
    // at enqueue time and DO ride along on drain, so a queued turn reaches the
    // agent with the same ACP content blocks as if it had run inline.
    let mut options = initial_options;
    'outer: loop {
        match retry_spawn(&mgr, &agent_id, &workspace_id).await {
            Ok(acp_session_id) => {
                let prompt = mgr
                    .build_turn_prompt(&agent_id, &workspace_id, &content, &options)
                    .await;
                if let Err(e) = mgr
                    .run_turn(&agent_id, &workspace_id, &acp_session_id, prompt)
                    .await
                {
                    tracing::warn!(agent = %agent_id, error = %e, "agent turn failed");
                }
            }
            Err(e) => {
                tracing::warn!(agent = %agent_id, error = %e, "agent spawn failed after all retries");
                handle_terminal_spawn_failure(
                    &mgr,
                    &agent_id,
                    &workspace_id,
                    &content,
                    &options,
                    &e,
                )
                .await;
                // Release the in-flight slot without overwriting the Error status
                // that handle_terminal_spawn_failure just persisted. This allows
                // a future message (or agent.retry) to restart the worker.
                mgr.release_in_flight_slot(&agent_id).await;
                break 'outer;
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
            let next_image_blocks = next.image_blocks.clone();
            let next_file_blocks = next.file_blocks.clone();
            persist_user(&mgr, &agent_id, &workspace_id, &next.content).await;
            content = next.content;
            options = TurnOptions {
                image_blocks: next_image_blocks,
                file_blocks: next_file_blocks,
                ..TurnOptions::default()
            };
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
            let next_image_blocks = next.image_blocks.clone();
            let next_file_blocks = next.file_blocks.clone();
            persist_user(&mgr, &agent_id, &workspace_id, &next.content).await;
            content = next.content;
            options = TurnOptions {
                image_blocks: next_image_blocks,
                file_blocks: next_file_blocks,
                ..TurnOptions::default()
            };
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
/// and publish the `agent:message` event so chat subscribers and the transcript
/// reflect the dequeued message (STAB-4 fix). Best-effort; a store or publish error
/// is logged and the turn still proceeds.
async fn persist_user(
    mgr: &AgentManager,
    agent_id: &AgentId,
    workspace_id: &WorkspaceId,
    content: &str,
) {
    let created_at = now_iso();
    match mgr
        .services
        .store
        .append_agent_message(agent_id, "user", &user_text_blocks(content), &created_at)
        .await
    {
        Ok(message) => {
            // Refresh agent_session.updated_at so the FE agent-card timestamp
            // reflects message activity, not just status transitions (STAB-19).
            if let Err(e) = mgr
                .services
                .store
                .refresh_agent_session_timestamp(workspace_id, agent_id, &created_at)
                .await
            {
                tracing::warn!(agent = %agent_id, error = %e, "refresh_agent_session_timestamp failed");
            }
            mgr.services
                .publish_agent_mutation_event(
                    workspace_id,
                    agent_id,
                    intent_core::events::AGENT_MESSAGE,
                    serde_json::json!({ "agentId": agent_id.0, "messageId": message.id, "role": message.role }),
                )
                .await;
        }
        Err(e) => {
            tracing::warn!(agent = %agent_id, error = %e, "failed to persist queued user message");
        }
    }
}

/// Max number of spawn attempts (includes the initial attempt).
const MAX_SPAWN_ATTEMPTS: u32 = 3;
/// Default backoff delays between retry attempts (in milliseconds).
const DEFAULT_RETRY_BACKOFF_MS: &[u64] = &[2000, 5000];

/// Get retry backoff delays, overridable via INTENTD_SPAWN_RETRY_BACKOFF_MS
/// (comma-separated milliseconds, e.g. "100,200"). Primarily for tests/CI.
fn retry_backoff_ms() -> Vec<u64> {
    if let Ok(val) = std::env::var("INTENTD_SPAWN_RETRY_BACKOFF_MS") {
        let mut delays = Vec::new();
        for part in val.split(',') {
            if let Ok(ms) = part.trim().parse::<u64>() {
                delays.push(ms);
            } else {
                // Invalid format, fall back to default
                return DEFAULT_RETRY_BACKOFF_MS.to_vec();
            }
        }
        if !delays.is_empty() {
            return delays;
        }
    }
    DEFAULT_RETRY_BACKOFF_MS.to_vec()
}

/// Classify whether an error from `ensure_started` is retryable. Retryable
/// errors include session/new or session/load timeouts and handshake failures
/// (e.g., "agent stdout closed" when the child dies immediately). Non-retryable
/// errors include InvalidParams, NotFound, Conflict, provider resolution
/// failures, mock provider missing env, and unknown Internal errors (fail-fast
/// by default to avoid retry loops on non-transient errors).
fn is_retryable_spawn_error(err: &Error) -> bool {
    // Non-retryable: InvalidParams, NotFound, Conflict are client/state issues,
    // not transient spawn failures that benefit from retry.
    match err {
        Error::InvalidParams(_) | Error::NotFound(_) | Error::Conflict { .. } => {
            return false;
        }
        _ => {}
    }

    let msg = err.to_string();
    // Retryable: session setup timeout, handshake failures, transport errors
    if msg.contains("session/new failed")
        || msg.contains("session/load failed")
        || msg.contains("handshake failed")
        || msg.contains("agent stdout closed")
        || msg.contains("timed out")
    {
        return true;
    }
    // Non-retryable: provider resolution failures (missing env, etc.)
    if msg.contains("provider") && msg.contains("missing") {
        return false;
    }
    // Default to non-retryable for unexpected Internal errors (conservative:
    // only retry explicitly-known transient failures to avoid masking bugs).
    false
}

/// Retry `ensure_started` up to `MAX_SPAWN_ATTEMPTS` times with exponential
/// backoff. On each retry (after the first failure), tear down the failed
/// child, publish an `agent:stream:status` retry hint, and spawn a fresh
/// process. Returns the `acpSessionId` on success, or the final error after
/// exhausting all attempts.
async fn retry_spawn(
    mgr: &AgentManager,
    agent_id: &AgentId,
    workspace_id: &WorkspaceId,
) -> Result<String> {
    let mut last_error: Option<Error> = None;

    for attempt in 1..=MAX_SPAWN_ATTEMPTS {
        match mgr.ensure_started(agent_id, workspace_id).await {
            Ok(session_id) => return Ok(session_id),
            Err(e) => {
                let retryable = is_retryable_spawn_error(&e);
                let error_msg = e.to_string();
                tracing::warn!(
                    agent = %agent_id,
                    attempt = attempt,
                    max = MAX_SPAWN_ATTEMPTS,
                    retryable = retryable,
                    error = %e,
                    "agent spawn attempt failed"
                );

                last_error = Some(e);

                // If non-retryable or last attempt, fail immediately
                if !retryable || attempt == MAX_SPAWN_ATTEMPTS {
                    break;
                }

                // Tear down the failed child so the next attempt spawns fresh
                // (narrower than full stop() — only kills child/handle, no worker/busy-flag touch)
                mgr.kill_child_only(agent_id).await;

                // Publish retry status hint with the actual failure kind
                let retry_num = attempt;
                let failure_kind = if error_msg.contains("timed out") {
                    "timed out"
                } else if error_msg.contains("agent stdout closed") {
                    "stdout closed"
                } else {
                    "failed"
                };
                let message = format!(
                    "Agent spawn {} — retrying (attempt {}/{})…",
                    failure_kind,
                    retry_num + 1,
                    MAX_SPAWN_ATTEMPTS
                );
                mgr.services
                    .publish_status_event(
                        workspace_id,
                        agent_id,
                        "spawn-retry",
                        &message,
                        "warning",
                    )
                    .await;

                // Backoff before retry
                let backoff = retry_backoff_ms();
                if let Some(&delay_ms) = backoff.get((attempt - 1) as usize) {
                    tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                }
            }
        }
    }

    Err(last_error
        .unwrap_or_else(|| Error::Internal("spawn retry loop exhausted without error".to_string())))
}

/// Handle terminal spawn failure after all retries are exhausted. Publishes
/// terminal `agent:failed` and `agent:stream:end` events, persists the agent
/// status as `Error`, requeues the failed message to the front of the queue,
/// and stops draining further messages.
async fn handle_terminal_spawn_failure(
    mgr: &AgentManager,
    agent_id: &AgentId,
    workspace_id: &WorkspaceId,
    content: &str,
    options: &TurnOptions,
    error: &Error,
) {
    use intent_core::events::{AGENT_FAILED, AGENT_STATUS_CHANGED, AGENT_STREAM_END};
    use serde_json::json;

    // Build error message. We do NOT include recent stderr in the agent:failed
    // event to avoid leaking secrets (API keys, tokens, file paths) to subscribed
    // clients. Stderr is available server-side in logs for debugging.
    let error_msg = error.to_string();

    // Publish terminal agent:failed event
    mgr.services
        .publish_agent_event(
            workspace_id,
            agent_id,
            AGENT_FAILED,
            json!({ "agentId": agent_id.0, "error": error_msg }),
        )
        .await;

    // Publish terminal agent:stream:end event
    mgr.services
        .publish_agent_event(
            workspace_id,
            agent_id,
            AGENT_STREAM_END,
            json!({ "agentId": agent_id.0 }),
        )
        .await;

    // Persist agent status as Error and emit agent:status-changed
    let ts = now_iso();
    if let Err(e) = mgr
        .services
        .store
        .set_agent_session_status(workspace_id, agent_id, AgentStatus::Error, false, &ts)
        .await
    {
        tracing::warn!(agent = %agent_id, error = %e, "failed to persist error status");
    } else {
        // Emit agent:status-changed
        let event = NewEvent {
            workspace_id: workspace_id.clone(),
            timestamp: ts,
            event_type: AGENT_STATUS_CHANGED.to_string(),
            actor: agent_actor(agent_id),
            session_id: Some(agent_id.0.clone()),
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data: json!({
                "agentId": agent_id.0,
                "status": "error",
                "isActive": false,
            }),
        };
        crate::publish_event(&mgr.services.event_bus, event).await;
    }

    // Requeue the failed message to the front of the queue
    let queued = crate::agent_ops::QueuedMessage {
        id: new_message_id(),
        content: content.to_string(),
        image_blocks: options.image_blocks.clone(),
        file_blocks: options.file_blocks.clone(),
        queued_at: now_iso(),
        editing: false,
    };
    mgr.services.requeue_front(agent_id, queued);

    // Publish queue updated so FE reflects the requeued message
    mgr.services
        .publish_queue_updated_for(
            agent_id,
            workspace_id,
            mgr.services.queue_snapshot(agent_id),
        )
        .await;
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
            pull_requests: None,
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
            metadata: None,
            created_at: ts.clone(),
            updated_at: ts,
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
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
            let prompt = mgr
                .build_turn_prompt(
                    &agent_id,
                    &WorkspaceId::from("ws-role"),
                    "do the thing",
                    &TurnOptions::default(),
                )
                .await;
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
        let prompt = mgr
            .build_turn_prompt(
                &agent_id,
                &WorkspaceId::from("ws-role"),
                "resume work",
                &TurnOptions::default(),
            )
            .await;
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
        let prompt = mgr
            .build_turn_prompt(
                &agent_id,
                &WorkspaceId::from("ws-role"),
                "plain message",
                &TurnOptions::default(),
            )
            .await;
        assert_eq!(prompt_text(&prompt), "plain message");
    }

    #[tokio::test]
    async fn stdin_context_is_prepended_as_context_block() {
        // Reference-parity `acp-provider.ts` §5.5: `stdinContext` is prepended
        // to the outbound prompt as `Context:\n<ctx>\n\n---\n\n<body>` before
        // any role reminder. Applies to both plain and specialist agents.
        let (mgr, agent_id) = manager_with(None, None).await;
        let opts = TurnOptions {
            stdin_context: Some("hello ctx".to_string()),
            ..TurnOptions::default()
        };
        let prompt = mgr
            .build_turn_prompt(
                &agent_id,
                &WorkspaceId::from("ws-role"),
                "user says hi",
                &opts,
            )
            .await;
        let text = prompt_text(&prompt);
        assert_eq!(text, "Context:\nhello ctx\n\n---\n\nuser says hi");
    }

    #[tokio::test]
    async fn stdin_context_empty_string_is_not_prepended() {
        // An empty `stdinContext` is treated as absent so we do not emit a
        // stray `Context:` header with nothing under it.
        let (mgr, agent_id) = manager_with(None, None).await;
        let opts = TurnOptions {
            stdin_context: Some(String::new()),
            ..TurnOptions::default()
        };
        let prompt = mgr
            .build_turn_prompt(&agent_id, &WorkspaceId::from("ws-role"), "body", &opts)
            .await;
        assert_eq!(prompt_text(&prompt), "body");
    }

    #[tokio::test]
    async fn stdin_context_precedes_role_reminder() {
        // Ordering: `Context:` block first, then the role reminder, then the
        // body — matching the reference `acp-provider.ts` prompt-assembly.
        let dir = write_specialist(
            "implementor",
            "---\nname: \"Implementor\"\ndescription: \"d\"\nroleReminder: \"Stay in scope.\"\n---\n\nbody",
        );
        let (mgr, agent_id) = manager_with(Some("implementor"), Some(dir)).await;
        let opts = TurnOptions {
            stdin_context: Some("ctx".to_string()),
            ..TurnOptions::default()
        };
        let prompt = mgr
            .build_turn_prompt(&agent_id, &WorkspaceId::from("ws-role"), "do it", &opts)
            .await;
        let text = prompt_text(&prompt);
        assert!(
            text.starts_with("Context:\nctx\n\n---\n\n[Role Reminder:"),
            "unexpected ordering: {text:?}"
        );
        assert!(text.ends_with("do it"));
    }

    // ---- Spawn-prompt specialist injection (PP-1) ----

    #[tokio::test]
    async fn specialist_injection_resolves_prompt_from_file() {
        let dir = write_specialist(
            "implementor",
            "---\nname: \"Implementor\"\ndescription: \"d\"\nroleReminder: \"Stay in scope.\"\n---\n\nImplement the task.",
        );
        let (mgr, agent_id) = manager_with(Some("implementor"), Some(dir)).await;
        let inj = mgr
            .services
            .agent_specialist_injection(&agent_id, None)
            .await
            .expect("injection for specialist agent");
        assert_eq!(inj.behavior_prompt.as_deref(), Some("Implement the task."));
        assert_eq!(inj.specialist_name.as_deref(), Some("Implementor"));
        assert_eq!(inj.role_reminder.as_deref(), Some("Stay in scope."));
    }

    #[tokio::test]
    async fn specialist_injection_metadata_behavior_prompt_wins() {
        // The session's persisted `metadata.behaviorPrompt` override wins over
        // the specialist file's body; name/reminder still come from the file.
        let dir = write_specialist(
            "implementor",
            "---\nname: \"Implementor\"\ndescription: \"d\"\nroleReminder: \"Stay in scope.\"\n---\n\nFile body.",
        );
        let (mgr, _first) = manager_with(Some("implementor"), Some(dir)).await;
        let agent_id = AgentId::from("agent-2");
        let mut s = session(&agent_id, &WorkspaceId::from("ws-1"), Some("implementor"));
        s.metadata = Some(serde_json::json!({ "behaviorPrompt": "Custom override." }));
        mgr.services
            .store
            .insert_agent_session(&s)
            .await
            .expect("insert session");
        let inj = mgr
            .services
            .agent_specialist_injection(&agent_id, None)
            .await
            .expect("injection");
        assert_eq!(inj.behavior_prompt.as_deref(), Some("Custom override."));
        assert_eq!(inj.specialist_name.as_deref(), Some("Implementor"));
        assert_eq!(inj.role_reminder.as_deref(), Some("Stay in scope."));
    }

    #[tokio::test]
    async fn specialist_injection_none_for_plain_agent() {
        let (mgr, agent_id) = manager_with(None, None).await;
        assert!(mgr
            .services
            .agent_specialist_injection(&agent_id, None)
            .await
            .is_none());
    }
}

#[cfg(test)]
mod retry_tests {
    //! Unit tests for spawn retry logic (classify retryable errors, tear down
    //! failed child between attempts, emit terminal failure events).

    use super::*;

    #[test]
    fn session_new_timeout_is_retryable() {
        let err =
            Error::Internal("session/new failed: request `session/new` timed out".to_string());
        assert!(is_retryable_spawn_error(&err));
    }

    #[test]
    fn session_load_timeout_is_retryable() {
        let err = Error::Internal("session/load failed: timed out after 60s".to_string());
        assert!(is_retryable_spawn_error(&err));
    }

    #[test]
    fn handshake_agent_stdout_closed_is_retryable() {
        let err =
            Error::Internal("handshake failed: JSON-RPC error 0: agent stdout closed".to_string());
        assert!(is_retryable_spawn_error(&err));
    }

    #[test]
    fn not_found_is_not_retryable() {
        let err = Error::NotFound("agent session not found".to_string());
        assert!(!is_retryable_spawn_error(&err));
    }

    #[test]
    fn provider_missing_is_not_retryable() {
        let err =
            Error::Internal("provider auggie missing required env ANTHROPIC_API_KEY".to_string());
        assert!(!is_retryable_spawn_error(&err));
    }

    #[test]
    fn generic_internal_error_is_not_retryable() {
        // Default changed to non-retryable for unknown Internal errors to avoid
        // masking bugs — only explicitly-known transient failures are retried.
        let err = Error::Internal("transport error: connection reset".to_string());
        assert!(!is_retryable_spawn_error(&err));
    }

    #[test]
    fn invalid_params_is_not_retryable() {
        let err = Error::InvalidParams("missing required parameter".to_string());
        assert!(!is_retryable_spawn_error(&err));
    }

    #[test]
    fn conflict_is_not_retryable() {
        let err = Error::Conflict {
            current: serde_json::json!({"rev": 2}),
        };
        assert!(!is_retryable_spawn_error(&err));
    }
}

#[cfg(test)]
mod agent_retry_tests {
    //! Unit tests for agent.retry RPC (retry a failed agent spawn).

    use super::*;
    use crate::events::EventBus;
    use crate::BusEventSink;
    use intent_core::{
        AgentSession, AgentStatus, Workspace, WorkspaceActivity, WorkspaceAttention,
        WorkspaceStatus,
    };
    use intent_store::Store;
    use std::sync::Arc;

    fn workspace(id: &WorkspaceId) -> Workspace {
        let ts = now_iso();
        Workspace {
            id: id.clone(),
            title: "Test WS".to_string(),
            branch: "main".to_string(),
            base_ref: None,
            base_commit_sha: None,
            status: WorkspaceStatus::Active,
            status_message: None,
            activity: WorkspaceActivity::Idle,
            attention: WorkspaceAttention::None,
            created_at: ts.clone(),
            updated_at: ts.clone(),
            last_activity: None,
            tags: vec![],
            path: None,
            repository_path: None,
            repository_owner: None,
            repository_name: None,
            worktree_path: None,
            pull_requests: None,
            scope: None,
            skip_worktree: false,
            setup_script: None,
            is_remote: false,
            default_model: None,
            archived: false,
            archived_at: None,
            active_pull_request: None,
            pr_number: None,
            pr_status: None,
            pr_url: None,
            agent_summary: None,
            diff_summary: None,
            token_usage: None,
            task_stats: None,
        }
    }

    fn session(agent_id: &AgentId, ws: &WorkspaceId, status: AgentStatus) -> AgentSession {
        let ts = now_iso();
        AgentSession {
            id: agent_id.clone(),
            workspace_id: ws.clone(),
            parent_agent_id: None,
            backend_session_id: None,
            acp_session_id: Some("session-1".to_string()),
            name: "Agent".to_string(),
            name_explicitly_set: false,
            model: Some("model-1".to_string()),
            provider: Some("provider-1".to_string()),
            system_prompt: None,
            specialist: None,
            status,
            is_active: false,
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
            metadata: None,
            created_at: ts.clone(),
            updated_at: ts,
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
        }
    }

    async fn manager_with_session(
        agent_id: &AgentId,
        ws: &WorkspaceId,
        status: AgentStatus,
    ) -> Arc<AgentManager> {
        let path = std::env::temp_dir().join(format!("intentd-retry-{}.db", uuid::Uuid::new_v4()));
        let db = Store::open(&path).await.expect("temp store");
        db.insert_workspace(&workspace(ws))
            .await
            .expect("insert workspace");
        db.insert_agent_session(&session(agent_id, ws, status))
            .await
            .expect("insert session");
        let bus = EventBus::new(db.clone());
        let services = Services::new(db).with_event_bus(bus.clone());
        let sink: Arc<dyn EventSink> = Arc::new(BusEventSink::new(bus));
        Arc::new(AgentManager::new(services, sink, 8))
    }

    #[tokio::test]
    async fn retry_from_error_status_returns_ok_true() {
        let agent_id = AgentId::from("agent-1");
        let ws = WorkspaceId::from("ws-1");
        let mgr = manager_with_session(&agent_id, &ws, AgentStatus::Error).await;

        let result = mgr
            .agent_retry(agent_id.clone(), ws.clone())
            .await
            .expect("retry");
        assert_eq!(result["ok"], true);

        // Status should be cleared to Pending
        let session = mgr
            .services
            .store
            .get_agent_session(&agent_id)
            .await
            .expect("session");
        assert_eq!(session.status, AgentStatus::Pending);
    }

    #[tokio::test]
    async fn retry_from_pending_status_returns_ok_false() {
        let agent_id = AgentId::from("agent-2");
        let ws = WorkspaceId::from("ws-2");
        let mgr = manager_with_session(&agent_id, &ws, AgentStatus::Pending).await;

        let result = mgr
            .agent_retry(agent_id.clone(), ws.clone())
            .await
            .expect("retry");
        assert_eq!(result["ok"], false);

        // Status should remain Pending
        let session = mgr
            .services
            .store
            .get_agent_session(&agent_id)
            .await
            .expect("session");
        assert_eq!(session.status, AgentStatus::Pending);
    }

    #[tokio::test]
    async fn retry_from_active_status_returns_ok_false() {
        let agent_id = AgentId::from("agent-3");
        let ws = WorkspaceId::from("ws-3");
        let mgr = manager_with_session(&agent_id, &ws, AgentStatus::Active).await;

        let result = mgr
            .agent_retry(agent_id.clone(), ws.clone())
            .await
            .expect("retry");
        assert_eq!(result["ok"], false);

        // Status should remain Active
        let session = mgr
            .services
            .store
            .get_agent_session(&agent_id)
            .await
            .expect("session");
        assert_eq!(session.status, AgentStatus::Active);
    }
}
