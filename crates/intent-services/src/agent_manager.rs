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
use intent_acp::session::{ContentBlock, McpServer, SessionModeState, StopReason};
use intent_acp::{
    apply_baseline_env_to_stdio_servers, build_baseline_mcp_env_from_process, handshake,
    normalize_mcp_servers, serve_workspace_mcp_tcp, spawn_provider, to_acp_session_mcp_servers,
    to_auggie_mcp_config, to_opencode_mcp_config, ClientRequestHandler, Connection,
    ConnectionHooks, EnvMap, EventSink, FileService, IncomingNotification, IncomingRequest,
    McpBridge, NormalizedMcpServer, NormalizedMcpServers, PermissionOutcome, PermissionPolicy,
    PermissionRegistry, PermissionRequestData, SinkEvent, SpawnOptions, WorkspaceMcpServer,
};
use intent_core::events::AGENT_STATUS_CHANGED;
use intent_core::{
    now_iso, slug::is_workspace_slug, ActorType, AgentId, AgentSession, AgentStatus, BoxFuture,
    Error, EventActor, Result, WorkspaceApi, WorkspaceAttention, WorkspaceId,
};
use intent_providers::{InjectionMechanism, ProviderConfig};
use intent_store::{NewEvent, NewTrackedChange};
use serde_json::{json, Value};
use tokio::process::Child;
use tokio::sync::{mpsc, Mutex as TokioMutex};
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::agent_ops::{new_message_id, user_message_blocks, MAX_MESSAGE_ID_LEN};
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

/// Whether a `session/cancel` error means the child's transport is already
/// closed (writer task / pipe gone because the child died mid-turn). That is
/// the EXPECTED outcome of cancelling a dead turn — the interrupt path logs it
/// at DEBUG. Anything else (protocol error, malformed payload, timeout on a
/// live socket) is a real anomaly and stays at WARN.
fn is_cancel_transport_closed(e: &intent_acp::AcpError) -> bool {
    matches!(e, intent_acp::AcpError::Transport(_))
}

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
    /// message row (via [`Store::append_agent_message_with_metadata`]). When a
    /// send is enqueued behind a running turn the metadata rides along on the
    /// `QueuedMessage` entry; drained turns rebuild their `TurnOptions` with
    /// the entry's captured metadata so both the drain-time persist and a
    /// later terminal-failure requeue keep the tag.
    pub message_metadata: Option<serde_json::Value>,
}

/// Conservative cap used when total system memory cannot be determined.
const DEFAULT_PROCESS_CAP: usize = 8;

/// Maximum concurrent agent processes for `total_memory_bytes`: reserve 8 GB
/// for the OS/other apps, then budget 1 GB per agent, clamped to [4, 100].
/// The 1 GB/agent budget is 2–4× the measured worst case (auggie ≈ 230 MB RSS
/// avg, claude-code chain ≈ 700 MB), so lower-RAM machines still get a tight
/// cap while high-RAM machines are not artificially throttled (for exact byte
/// counts: 16 GB → 8, 32 GB → 24, 64 GB → 56, ≥108 GB → 100; Linux MemTotal
/// runs slightly below nominal RAM, so a nominal 16 GB box may compute 7).
pub fn compute_process_cap(total_memory_bytes: u64) -> usize {
    let budget_gb = total_memory_bytes.saturating_sub(8 * GB) / GB;
    budget_gb.clamp(4, 100) as usize
}

/// Best-effort process cap from detected system RAM, falling back to
/// [`DEFAULT_PROCESS_CAP`] when total memory is unknown (RAM detection
/// supports Linux and macOS; other platforms fall back to the default).
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

#[cfg(target_os = "macos")]
fn total_memory_bytes() -> Option<u64> {
    use std::mem;
    use std::ptr;

    let mut size: u64 = 0;
    let mut len = mem::size_of::<u64>();
    let name = b"hw.memsize\0";

    let result = unsafe {
        libc::sysctlbyname(
            name.as_ptr() as *const libc::c_char,
            &mut size as *mut u64 as *mut libc::c_void,
            &mut len,
            ptr::null_mut(),
            0,
        )
    };

    if result == 0 {
        Some(size)
    } else {
        None
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
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

/// Async callback for process-cap lifecycle events (queueing/resuming/eviction).
/// Invoked by the registry when a spawn queues, resumes, or an idle process is
/// evicted; the manager wires this to log + publish workspace events.
pub type ProcessEventFn =
    Arc<dyn Fn(&AgentId, &str, usize, usize) -> BoxFuture<'static, ()> + Send + Sync>;

struct ProcessEntry {
    last_active_ms: u64,
    is_active: bool,
    kill: KillFn,
}

#[derive(Default)]
struct RegistryInner {
    entries: HashMap<AgentId, ProcessEntry>,
    /// Queue of waiting spawns, each carrying the agent id + oneshot channel.
    wait_queue: Vec<(AgentId, tokio::sync::oneshot::Sender<()>)>,
}

fn pop_waiter(inner: &mut RegistryInner) -> Option<(AgentId, tokio::sync::oneshot::Sender<()>)> {
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
    /// Optional callback for lifecycle events (queue/resume/evict). Wired by the
    /// manager to publish events + log; the registry stays testable without it.
    event_fn: Option<ProcessEventFn>,
}

impl ProcessRegistry {
    /// A registry with a fixed concurrency `cap`.
    pub fn new(cap: usize) -> Self {
        Self {
            cap: cap.max(1),
            inner: Mutex::new(RegistryInner::default()),
            event_fn: None,
        }
    }

    /// Attach an event callback for lifecycle events (queueing/resuming/eviction).
    /// Chainable builder; returns `Self` so the manager can wire this after
    /// construction. The callback signature is `(agent_id, event_type, used, cap)`.
    pub fn with_event_fn(mut self, f: ProcessEventFn) -> Self {
        self.event_fn = Some(f);
        self
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

    /// Remove a process and wake the next queued spawn, if any. When a waiter is
    /// resumed, logs + emits `agent:process:resumed` via the event callback.
    pub fn deregister(&self, agent_id: &AgentId) -> bool {
        let resumed_agent = {
            let mut inner = self.inner.lock().unwrap();
            let had = inner.entries.remove(agent_id).is_some();
            if !had {
                return false;
            }
            pop_waiter(&mut inner)
        };
        if let Some((resumed_id, tx)) = resumed_agent {
            let _ = tx.send(());
            let used = self.size();
            tracing::info!(
                agent = %resumed_id,
                used = used,
                cap = self.cap,
                "process registry: queued spawn resumed"
            );
            if let Some(ref f) = self.event_fn {
                let fut = f(&resumed_id, "agent:process:resumed", used, self.cap);
                tokio::spawn(fut);
            }
        }
        true
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
    /// can take the freed slot immediately. When a waiter is resumed, logs + emits
    /// `agent:process:resumed` via the event callback.
    pub fn mark_idle(&self, agent_id: &AgentId) {
        let resumed_agent = {
            let mut inner = self.inner.lock().unwrap();
            let existed = match inner.entries.get_mut(agent_id) {
                Some(entry) => {
                    entry.is_active = false;
                    entry.last_active_ms = now_ms();
                    true
                }
                None => false,
            };
            if !existed {
                return;
            }
            pop_waiter(&mut inner)
        };
        if let Some((resumed_id, tx)) = resumed_agent {
            let _ = tx.send(());
            let used = self.size();
            tracing::info!(
                agent = %resumed_id,
                used = used,
                cap = self.cap,
                "process registry: queued spawn resumed"
            );
            if let Some(ref f) = self.event_fn {
                let fut = f(&resumed_id, "agent:process:resumed", used, self.cap);
                tokio::spawn(fut);
            }
        }
    }

    /// Ensure a slot is free before spawning: returns immediately under the cap,
    /// otherwise evicts the LRU idle process, or queues until one frees. Logs +
    /// emits `agent:process:queued` / `agent:process:evicted` via the event callback.
    pub async fn acquire(&self, agent_id: &AgentId) {
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
                    inner.wait_queue.push((agent_id.clone(), tx));
                    let used = inner.entries.len();
                    tracing::info!(
                        agent = %agent_id,
                        used = used,
                        cap = self.cap,
                        "process registry: spawn queued (all slots active)"
                    );
                    if let Some(ref f) = self.event_fn {
                        let fut = f(agent_id, "agent:process:queued", used, self.cap);
                        tokio::spawn(fut);
                    }
                    Action::Wait(rx)
                }
            };
            match action {
                Action::Slot => return,
                Action::Evict(id, kill) => {
                    let used = self.size();
                    tracing::info!(
                        evicted = %id,
                        used = used,
                        cap = self.cap,
                        "process registry: LRU idle process evicted"
                    );
                    if let Some(ref f) = self.event_fn {
                        let fut = f(&id, "agent:process:evicted", used, self.cap);
                        tokio::spawn(fut);
                    }
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
///
/// `spawned_model` and `spawned_provider` track the model/provider the child was
/// spawned with, enabling `ensure_started` to detect model changes (via `agent.setModel`)
/// and respawn the child with the new model before the next turn.
struct AgentHandle {
    connection: Arc<Connection>,
    notifications: Arc<TokioMutex<mpsc::UnboundedReceiver<IncomingNotification>>>,
    serve_task: JoinHandle<()>,
    _child: Option<Child>,
    _mcp_bridge: Option<McpBridge>,
    _mcp_config: Option<TempConfigFile>,
    _rules_config: Option<TempConfigFile>,
    /// MCP servers (workspace bridge + user servers) delivered via the ACP
    /// `session/new` / `session/load` `mcpServers` field for providers that
    /// consume them there (claude-code, codex, droid, grok). Empty for providers
    /// that receive MCP config out-of-band (auggie `--mcp-config`, opencode
    /// env config) — passing servers they'd ignore is avoided for wire parity.
    session_mcp_servers: Vec<McpServer>,
    spawned_model: Option<String>,
    spawned_provider: String,
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
    /// Root the per-agent stderr capture files live under (STAB-53), laid out
    /// as `<root>/<agent-id>/<YYYY-MM-DD>.log`. The composition root wires
    /// `<data_dir>/agent-logs`; `None` (tests / bare wiring) disables capture.
    agent_log_root: Option<PathBuf>,
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
    /// Agents whose NEXT turn must carry the assembled system prompt prepended
    /// as a `<system>` block — the FirstTurnPrepend fallback (§18.1) for
    /// providers with no (usable) native injection mechanism (codex, cortex,
    /// pi, grok, mock). Set when a
    /// FRESH ACP session is opened (`session/new`, brand-new or recreate) for a
    /// provider whose `injection_mechanism` is
    /// [`InjectionMechanism::FirstTurnPrepend`]; NOT set on `session/load`
    /// resume (the provider kept its prior context, which already saw the
    /// prompt). Consumed by [`AgentManager::build_turn_prompt`] so the block
    /// fires exactly once per fresh session and re-fires after a recreate.
    prepend_pending: Arc<Mutex<HashSet<AgentId>>>,
    /// Most recent interrupt-priority `messageId` delivered per agent
    /// (PROTOCOL §5.5). [`AgentManager::interrupt_send_message`] records the
    /// client-supplied id under this lock BEFORE preempting, so the SAME
    /// interrupt delivered twice (client retry / event double-fire) preempts
    /// exactly once — the duplicate is acknowledged idempotently instead of
    /// cancelling the interrupt turn it raced and re-persisting the message.
    interrupt_ids: Arc<Mutex<HashMap<AgentId, String>>>,
    /// Agents whose NEXT session establishment must SKIP the `session/load`
    /// resume and open a fresh `session/new` instead — armed by
    /// [`AgentManager::edit_and_regenerate`] (immediately after target
    /// validation, before the stop) because a resumed provider session would
    /// retain the truncated turns in its context. Deliberately NOT cleared by
    /// [`AgentManager::stop`] (unlike `recreated`/`prepend_pending`): the
    /// truncation is already persisted, so the stale provider history must
    /// never be resumed regardless of intervening stops. Enforced at BOTH
    /// establishment points — [`AgentManager::start_session`] skips the
    /// resume, and [`AgentManager::ensure_started`]'s live-child reuse path
    /// tears the child down when armed — and consumed only when a fresh
    /// session is successfully opened, so spawn retries keep the flag.
    ///
    /// KNOWN LIMITATION: in-memory only, like `recreated` (and the same gap
    /// `agent.replaceMessages` has today). If the daemon restarts before the
    /// regenerated turn opens its fresh session (e.g. the spawn failed
    /// terminally and the agent parked in `Error` awaiting `agent.retry`),
    /// the intent is lost and the next spawn may resume the stale provider
    /// session via `session/load`. Persisting a needs-recreate marker on the
    /// session row would close this; not done here to keep parity with the
    /// existing replaceMessages semantics.
    force_recreate: Arc<Mutex<HashSet<AgentId>>>,
}

impl AgentManager {
    /// Wire a manager over the services surface and a concrete event sink, with
    /// a global concurrency `cap`.
    pub fn new(services: Services, sink: Arc<dyn EventSink>, cap: usize) -> Self {
        // Wire the registry event function to publish process-cap lifecycle events.
        let services_clone = services.clone();
        let event_fn: ProcessEventFn = Arc::new(move |agent_id, event_type, used, cap| {
            let services = services_clone.clone();
            let agent_id = agent_id.clone();
            let event_type = event_type.to_string();
            Box::pin(async move {
                // Best-effort workspace lookup: process-cap events are global across
                // workspaces, so when the session row is missing (mid-create or already
                // deleted) swallow the lookup error and skip the publish rather than
                // blocking the registry path. The tracing log still fires above.
                let workspace_id = match services.store.get_agent_session(&agent_id).await {
                    Ok(session) => session.workspace_id,
                    Err(_) => return,
                };
                services
                    .publish_agent_event(
                        &workspace_id,
                        &agent_id,
                        &event_type,
                        json!({
                            "agentId": agent_id.0,
                            "used": used,
                            "cap": cap,
                        }),
                    )
                    .await;
            })
        });

        Self {
            services,
            registry: Arc::new(ProcessRegistry::new(cap).with_event_fn(event_fn)),
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
            agent_log_root: None,
            busy: Arc::new(Mutex::new(HashSet::new())),
            agent_ws: Arc::new(Mutex::new(HashMap::new())),
            workers: Arc::new(Mutex::new(HashMap::new())),
            recreated: Arc::new(Mutex::new(HashSet::new())),
            prepend_pending: Arc::new(Mutex::new(HashSet::new())),
            interrupt_ids: Arc::new(Mutex::new(HashMap::new())),
            force_recreate: Arc::new(Mutex::new(HashSet::new())),
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

    /// Enable per-agent stderr capture (STAB-53): every spawned child's stderr
    /// is appended to `<root>/<agent-id>/<YYYY-MM-DD>.log`. The composition
    /// root passes `intent_core::agent_logs_root(&config.data_dir)`.
    pub fn with_agent_log_root(mut self, root: impl Into<PathBuf>) -> Self {
        self.agent_log_root = Some(root.into());
        self
    }

    /// Stderr capture directory for `agent_id`, when capture is enabled —
    /// the "agent stderr captured at …" hint on terminal-failure WARN lines.
    /// Points at the per-agent directory rather than today's daily file: the
    /// writer rotates by UTC date, so around midnight the last lines may sit
    /// in yesterday's file, making a file path misleading. The directory is
    /// rollover-stable and still immediately actionable.
    fn agent_stderr_log_dir(&self, agent_id: &AgentId) -> Option<PathBuf> {
        self.agent_log_root
            .as_ref()
            .map(|root| root.join(&agent_id.0))
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
        self.registry.acquire(&agent_id).await;

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

        // For env-config providers (opencode), the same normalized server set
        // (workspace bridge + user servers) rides in `OPENCODE_CONFIG_CONTENT`
        // as an `mcp` block instead of an `--mcp-config` file, pointing at the
        // same bridge endpoint.
        let mut env_mcp_config: Option<String> = None;
        if opts.provider.injection_mechanism == InjectionMechanism::EnvConfig {
            env_mcp_config = Some(self.opencode_env_mcp_config(bridge.connect_addr()).await?);
        }

        // For providers that consume MCP servers from the ACP session setup
        // (claude-code, codex, droid, grok), the same normalized server set is
        // carried as the typed `session/new` / `session/load` `mcpServers`
        // field, pointing at the same bridge endpoint. Kept on the handle so
        // `start_session` (which runs after `create_agent`) can pass it into
        // every session-open branch.
        let mut session_mcp_servers: Vec<McpServer> = Vec::new();
        if opts.provider.supports_session_mcp_servers {
            let servers = self.normalized_mcp_servers(bridge.connect_addr()).await?;
            session_mcp_servers = to_acp_session_mcp_servers(&servers);
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
            // `git.autoCommit` / `rtk.enabled` are global (non-workspace-scoped)
            // settings, so this snapshot is independent of the session and
            // cheap to take here.
            let settings = self.services.effective_settings();
            let auto_commit_enabled = settings.git.auto_commit;
            // Sub-agent gating: delegated children (`parent_agent_id` set) and
            // background workers (`is_background`) skip the suggested-prompts
            // directive, matching the reference `isSubAgent` derivation. The
            // session was inserted by the caller before `create_agent` runs,
            // so propagate any store error rather than silently defaulting to
            // top-level (which would mis-scope the SP-1 footer and hide DB
            // failures).
            let session = self.services.store.get_agent_session(&agent_id).await?;
            let is_sub_agent = session.parent_agent_id.is_some() || session.is_background;
            // Fetch workspace for mode-dependent prompt hints (Task 6).
            let workspace = self.services.store.get_workspace(&workspace_id).await.ok();
            if let Some(prompt) = crate::rules::assemble_system_prompt(
                &self.services.store,
                Some(&cwd),
                agent_type,
                specialist.as_ref(),
                is_sub_agent,
                auto_commit_enabled,
                settings.rtk.enabled,
                workspace.as_ref(),
                Some(&session),
            )
            .await
            {
                let path =
                    std::env::temp_dir().join(format!("intentd-rules-{}.md", Uuid::new_v4()));
                std::fs::write(&path, prompt.as_bytes())
                    .map_err(|e| Error::Internal(format!("write rules file failed: {e}")))?;
                rules_file_path = Some(path.to_string_lossy().into_owned());
                rules_config = Some(TempConfigFile { path });
                // Persist the assembled systemPrompt on the session so
                // `agent.getSession` can return it without re-assembly.
                let mut updated_session = session;
                updated_session.system_prompt = Some(prompt);
                if let Err(e) = self
                    .services
                    .store
                    .update_agent_session(&workspace_id, &updated_session)
                    .await
                {
                    tracing::warn!(agent = %agent_id, error = %e, "failed to persist system_prompt on session");
                }
            }
        }

        // Reconstruct the spawn options with the generated config path injected.
        let spawn_opts = rebuild_spawn_opts(
            opts,
            rules_file_path.as_deref(),
            mcp_config_path.as_deref(),
            env_mcp_config.as_deref(),
        );

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
            // STAB-53: capture the child's stderr under
            // `<agent-logs>/<agent-id>/<YYYY-MM-DD>.log` so a child that dies
            // mid-turn leaves a diagnosable trace.
            stderr_log_dir: self
                .agent_log_root
                .as_ref()
                .map(|root| root.join(&agent_id.0)),
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
            session_mcp_servers,
            spawned_model: opts.model.map(|s| s.to_string()),
            spawned_provider: opts.provider.command.to_string(),
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

    /// Build the generated `--mcp-config` (auggie `{ mcpServers }` shape) from
    /// the normalized spawn server set ([`Self::normalized_mcp_servers`]).
    async fn generate_mcp_config(&self, bridge: &McpBridge) -> Result<serde_json::Value> {
        let servers = self.normalized_mcp_servers(bridge.connect_addr()).await?;
        Ok(to_auggie_mcp_config(&servers))
    }

    /// Serialize the same normalized spawn server set as the OpenCode config
    /// `mcp` block, merged into `OPENCODE_CONFIG_CONTENT` at spawn for
    /// env-config providers. The bridge entry points at the same endpoint the
    /// auggie `--mcp-config` path uses.
    async fn opencode_env_mcp_config(&self, connect_addr: String) -> Result<String> {
        let servers = self.normalized_mcp_servers(connect_addr).await?;
        serde_json::to_string(&to_opencode_mcp_config(&servers))
            .map_err(|e| Error::Internal(format!("serialize opencode mcp config failed: {e}")))
    }

    /// Normalized MCP server set for a spawn: the `workspace-mcp` server is
    /// the `intentd mcp-bridge --connect <addr>` subcommand, with the user's
    /// `mcp.servers` catalog merged in and the safe baseline env injected
    /// across every stdio entry (§6.8, §18.4). Mirrors the FE
    /// `mergeUserMcpServersWithAuth` path: honours the `mcp.enableUserServers`
    /// gate, filters out globally-disabled servers, and — for http/sse
    /// transports — injects an `Authorization` header from the persisted OAuth
    /// token bag when the catalog entry does not already set one.
    /// `workspace-mcp` is reserved and never overridden.
    async fn normalized_mcp_servers(&self, connect_addr: String) -> Result<NormalizedMcpServers> {
        let mut servers = NormalizedMcpServers::new();
        servers.insert(
            "workspace-mcp".to_string(),
            NormalizedMcpServer::Stdio {
                command: self.mcp_bridge_exe.to_string_lossy().into_owned(),
                args: vec![
                    "mcp-bridge".to_string(),
                    "--connect".to_string(),
                    connect_addr,
                ],
                env: EnvMap::new(),
            },
        );
        self.merge_user_mcp_servers(&mut servers).await?;
        let baseline = build_baseline_mcp_env_from_process();
        Ok(apply_baseline_env_to_stdio_servers(&servers, &baseline))
    }

    /// Fold user-configured MCP servers (sensitive `mcp.servers` secret) into
    /// `out`, honouring the `mcp.enableUserServers` gate and the global
    /// `mcp.disabledServers` list, and injecting an `Authorization` header from
    /// the persisted OAuth bag on http/sse entries when the catalog does not
    /// already set one. Any config that collides with a reserved built-in name
    /// (e.g. `workspace-mcp`) is skipped so the bridge cannot be shadowed.
    async fn merge_user_mcp_servers(&self, out: &mut NormalizedMcpServers) -> Result<()> {
        let settings = self.services.effective_settings();
        if !crate::mcp_servers::enable_user_servers(&settings) {
            return Ok(());
        }
        let configs = crate::mcp_servers::read_configs(&self.services.secrets).await;
        if configs.is_empty() {
            return Ok(());
        }
        let disabled = crate::mcp_servers::disabled_servers(&settings);
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
    /// spawned agent. The agent→BE MCP server is delivered per the provider's
    /// mechanism: out-of-band via the generated `--mcp-config` (auggie) or env
    /// config (opencode) — those sessions carry no `mcpServers` — or in the
    /// `session/new` / `session/load` `mcpServers` field for providers with
    /// `supports_session_mcp_servers` (claude-code, codex, droid, grok), using the
    /// server list `create_agent` stashed on the handle. On a daemon respawn
    /// the agent may already have a persisted `acpSessionId`:
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
        let (conn, session_mcp_servers) = {
            let map = self.handles.lock().unwrap();
            let handle = map
                .get(agent_id)
                .ok_or_else(|| Error::NotFound(format!("agent {agent_id}")))?;
            (
                handle.connection.clone(),
                handle.session_mcp_servers.clone(),
            )
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

        // Per the ACP schema, http/sse `McpServer` entries are only valid when
        // the agent advertised `mcpCapabilities.http`/`sse` in `initialize` —
        // an agent that didn't may reject the whole `session/new`. Filter here
        // (post-handshake) so a user-configured http/sse catalog entry can't
        // break agent spawn; stdio (the workspace bridge) is mandatory per
        // spec and always passes.
        let mcp_caps = &handshake.initialize.agent_capabilities.mcp_capabilities;
        let session_mcp_servers: Vec<McpServer> = session_mcp_servers
            .into_iter()
            .filter(|s| match s {
                McpServer::Stdio(_) => true,
                McpServer::Http(_) => mcp_caps.http,
                McpServer::Sse(_) => mcp_caps.sse,
                _ => false,
            })
            .collect();

        // The persisted model (bare part of a compound id) feeds the
        // post-session model application for providers with no CLI model
        // flag — `session/set_model` (grok) or `session/set_config_option`
        // (claude-code) — see `maybe_apply_session_model`.
        let stored_model = session_record.model.clone();

        // The persisted id (if any) decides the no-resume branch: a brand-new
        // agent (no id) opens a first session; an agent with a lost id recreates
        // (CAS-replacing exactly this id) and resends history.
        let stored_id = session_record.acp_session_id;

        // Forced recreate (`agent.editAndRegenerate`): the transcript was
        // truncated, so resuming the provider session would retain the
        // truncated turns in its context. Skip the `session/load` attempt and
        // fall through to the recreate/new branches, which open a fresh
        // `session/new` and replay the truncated history as `<supervisor>`
        // XML. Peeked here and consumed only after a fresh session is
        // successfully opened, so a failed spawn attempt retries with the
        // flag still armed instead of resuming the stale session.
        let forced = self.force_recreate.lock().unwrap().contains(agent_id);

        // 1) Try to resume the persisted session (gated on stored id + capability).
        match if forced {
            Ok(None)
        } else {
            self.services
                .resume_acp_session(
                    conn.as_ref(),
                    &handshake.initialize,
                    agent_id,
                    cwd.clone(),
                    session_mcp_servers.clone(),
                )
                .await
        } {
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
                Self::maybe_apply_session_model(
                    conn.as_ref(),
                    provider,
                    &opened.session_id,
                    stored_model.as_deref(),
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
                .recreate_acp_session(
                    conn.as_ref(),
                    agent_id,
                    &expected_old,
                    cwd,
                    session_mcp_servers.clone(),
                )
                .await?;
            self.force_recreate.lock().unwrap().remove(agent_id);
            self.recreated.lock().unwrap().insert(agent_id.clone());
            self.arm_first_turn_prepend(agent_id, provider);
            self.maybe_bypass_permissions(
                conn.as_ref(),
                provider,
                &opened.session_id,
                opened.modes.as_ref(),
            )
            .await;
            Self::maybe_apply_session_model(
                conn.as_ref(),
                provider,
                &opened.session_id,
                stored_model.as_deref(),
            )
            .await;
            return Ok(opened.session_id);
        }

        // 3) Brand-new agent → open and persist the first session (write-once).
        let opened = self
            .services
            .open_acp_session(conn.as_ref(), agent_id, cwd, session_mcp_servers)
            .await?;
        self.force_recreate.lock().unwrap().remove(agent_id);
        self.arm_first_turn_prepend(agent_id, provider);
        self.maybe_bypass_permissions(
            conn.as_ref(),
            provider,
            &opened.session_id,
            opened.modes.as_ref(),
        )
        .await;
        Self::maybe_apply_session_model(
            conn.as_ref(),
            provider,
            &opened.session_id,
            stored_model.as_deref(),
        )
        .await;
        Ok(opened.session_id)
    }

    /// Best-effort post-session model application, gated per provider
    /// capability (parity with the reference acp-provider): `session/set_model`
    /// for providers whose ACP subcommand has no CLI model flag
    /// (`supports_set_model`; grok today), and
    /// `session/set_config_option { configId: "model" }` for providers that
    /// expose the model as a session config option
    /// (`supports_config_option_model`; claude-code today). Compound ids are
    /// honored only when their provider prefix matches the running provider (a
    /// stale id from a pre-spawn provider switch must not be sent); bare ids
    /// are treated as provider-local. The `default` sentinel and empty ids are
    /// no-ops. Failures are logged at WARN and never fail session startup.
    async fn maybe_apply_session_model(
        conn: &Connection,
        provider: &ProviderConfig,
        acp_session_id: &str,
        stored_model: Option<&str>,
    ) {
        if let Some(model_id) = Self::set_model_target(provider, stored_model) {
            match intent_acp::session::set_session_model(conn, acp_session_id, model_id).await {
                Ok(()) => {
                    tracing::debug!(
                        provider = provider.id,
                        session_id = acp_session_id,
                        model = %model_id,
                        "session/set_model accepted"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        provider = provider.id,
                        session_id = acp_session_id,
                        model = %model_id,
                        error = %e,
                        "session/set_model failed; provider keeps its default model"
                    );
                }
            }
        }
        if let Some(model_id) = Self::config_option_model_target(provider, stored_model) {
            match intent_acp::session::set_session_config_option(
                conn,
                acp_session_id,
                "model",
                model_id,
            )
            .await
            {
                Ok(()) => {
                    tracing::debug!(
                        provider = provider.id,
                        session_id = acp_session_id,
                        model = %model_id,
                        "session/set_config_option accepted"
                    );
                }
                Err(e) => {
                    tracing::warn!(
                        provider = provider.id,
                        session_id = acp_session_id,
                        model = %model_id,
                        error = %e,
                        "session/set_config_option failed; provider keeps its default model"
                    );
                }
            }
        }
    }

    /// Resolve the model id `maybe_apply_session_model` should send via
    /// `session/set_model`, or `None` when the call should not be issued:
    /// providers without `supports_set_model`, or ids rejected by
    /// [`Self::provider_local_model_target`].
    fn set_model_target<'m>(
        provider: &ProviderConfig,
        stored_model: Option<&'m str>,
    ) -> Option<&'m str> {
        if !provider.supports_set_model {
            return None;
        }
        Self::provider_local_model_target(provider, stored_model)
    }

    /// Resolve the model id `maybe_apply_session_model` should send via
    /// `session/set_config_option { configId: "model" }`, or `None` when the
    /// call should not be issued: providers without
    /// `supports_config_option_model`, or ids rejected by
    /// [`Self::provider_local_model_target`].
    fn config_option_model_target<'m>(
        provider: &ProviderConfig,
        stored_model: Option<&'m str>,
    ) -> Option<&'m str> {
        if !provider.supports_config_option_model {
            return None;
        }
        Self::provider_local_model_target(provider, stored_model)
    }

    /// Shared gating for the post-session model-application paths: `None` for
    /// absent/empty models, the `default` sentinel, and compound ids whose
    /// provider prefix does not match the running provider (a stale id from a
    /// pre-spawn provider switch must not be sent). Bare ids are treated as
    /// provider-local; compound ids are stripped to their bare part.
    fn provider_local_model_target<'m>(
        provider: &ProviderConfig,
        stored_model: Option<&'m str>,
    ) -> Option<&'m str> {
        let model = stored_model?;
        let model_id = match model.split_once(':') {
            Some((prefix, bare)) if prefix == provider.id => bare,
            Some(_) => return None,
            None => model,
        };
        if model_id.is_empty() || model_id == "default" {
            return None;
        }
        Some(model_id)
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

    /// Arm the FirstTurnPrepend flag for `agent_id` when the provider has no
    /// native system-prompt mechanism (§18.1 fallback). Called only from the
    /// fresh-session branches of [`AgentManager::start_session`] (`session/new`
    /// for a brand-new agent, or the resume-impossible recreate) — never on a
    /// `session/load` resume, where the provider retained the prior context
    /// that already carried the prompt.
    fn arm_first_turn_prepend(&self, agent_id: &AgentId, provider: &ProviderConfig) {
        if provider.injection_mechanism == InjectionMechanism::FirstTurnPrepend {
            self.prepend_pending
                .lock()
                .unwrap()
                .insert(agent_id.clone());
        }
    }

    /// Compute the `<system>`-wrapped assembled system prompt for the
    /// FirstTurnPrepend fallback, or `None` when nothing is pending. The
    /// prompt text comes from the session's persisted `system_prompt`
    /// (written by [`AgentManager::create_agent`] at spawn time from
    /// `assemble_system_prompt`). The pending flag is consumed only on a
    /// definitive outcome (prompt built, or session provably has no usable
    /// prompt); a transient store error keeps it armed so the NEXT turn
    /// retries instead of silently dropping the system prompt for the whole
    /// session.
    async fn build_first_turn_prepend(&self, agent_id: &AgentId) -> Option<String> {
        if !self.prepend_pending.lock().unwrap().contains(agent_id) {
            return None;
        }
        let session = match self.services.store.get_agent_session(agent_id).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(
                    agent = %agent_id,
                    error = %e,
                    "first-turn prepend: session lookup failed; keeping flag armed for retry"
                );
                return None;
            }
        };
        self.prepend_pending.lock().unwrap().remove(agent_id);
        let prompt = session.system_prompt?;
        let prompt = prompt.trim();
        if prompt.is_empty() {
            return None;
        }
        Some(format!("<system>\n{prompt}\n</system>"))
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
    /// * Names the concrete daemon tool the agent must call — spelled the way
    ///   the session's provider will actually surface it (see
    ///   [`workspace_naming_tool_reference`]) — not the FE `workspace_api`
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
        // Spell the rename tool the way this session's provider surfaces it;
        // a failed session lookup falls back to the generic phrasing.
        let tool_ref = match self.services.store.get_agent_session(agent_id).await {
            Ok(s) => workspace_naming_tool_reference(&session_provider_id(&s)),
            Err(e) => {
                tracing::warn!(
                    agent = %agent_id,
                    error = %e,
                    "naming nudge: session lookup failed; using generic tool phrasing"
                );
                GENERIC_NAMING_TOOL_REFERENCE
            }
        };
        Some(format!(
            "<system>\nThis workspace needs a title. As your first action, call {tool_ref} with a short 3\u{2013}5 word sentence-case title describing the task. This can be called in parallel with information-gathering.\n</system>"
        ))
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
        // FirstTurnPrepend fallback (§18.1): for providers with no (usable)
        // native system-prompt mechanism (codex, cortex, pi, grok, mock), the
        // assembled system prompt
        // is delivered as the OUTERMOST `<system>` block on the first prompt
        // of each fresh ACP session — before context/naming/reminder/user
        // content. Armed by `start_session` on `session/new` (brand-new or
        // recreate, never `session/load` resume) and consumed here so it
        // fires exactly once per fresh session.
        let prepend = self.build_first_turn_prepend(agent_id).await;
        let prompt_text = match prepend {
            Some(sys) => format!("{sys}\n\n{prompt_text}"),
            None => prompt_text,
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
        let (removed, child) = self.detach(agent_id).await;
        if let Some(child) = child {
            kill_child_tree(child).await;
        }
        removed
    }

    /// Shared teardown body of [`AgentManager::stop`]: abort the worker, drop
    /// stale flags, settle the turn, remove the handle, deregister — and hand
    /// back the detached child (if any) so the caller decides how to kill it.
    /// `stop()` kills the single tree inline (SIGTERM→grace→SIGKILL);
    /// `shutdown()` collects every detached child and kills all process groups
    /// concurrently under ONE shared grace window.
    async fn detach(&self, agent_id: &AgentId) -> (bool, Option<Child>) {
        // Snapshot the live-turn slot BEFORE aborting the worker (the abort
        // drops LiveTurnGuard, clearing the slot), then flush the partial
        // in-flight assistant content AFTER the abort — same convention as
        // the graceful-shutdown flush. A worker append already in flight at
        // abort time can still land, but the `agent_message.id` PK keeps the
        // outcome convergent (exactly one row; the UNIQUE collision is
        // absorbed inside the flush). No-op when the slot is empty or was
        // already flushed by a caller (e.g. shutdown(), which flushes before
        // delegating here).
        let partial_turn = self.services.live_turn(agent_id);
        if let Some(worker) = self.workers.lock().unwrap().remove(agent_id) {
            worker.abort();
        }
        if let Some(live) = partial_turn {
            self.services
                .flush_partial_turn_on_interruption(agent_id, live)
                .await;
        }
        // Drop any pending recreate/prepend flags: the next spawn re-decides
        // resume vs recreate from scratch, so stale flags must not survive a
        // teardown (a session/load resume must not fire a stale prepend).
        self.recreated.lock().unwrap().remove(agent_id);
        self.prepend_pending.lock().unwrap().remove(agent_id);
        self.end_turn(agent_id).await;
        let handle = self.handles.lock().unwrap().remove(agent_id);
        let removed = handle.is_some();
        let child = handle.and_then(|mut h| h._child.take());
        self.registry.deregister(agent_id);
        (removed, child)
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
        self.interrupt_inner(agent_id, false).await
    }

    /// Shared body of [`AgentManager::interrupt`] with one extra knob:
    /// `suppress_idle_emit` skips the STAB-28 synthetic `agent:idle`
    /// (reason: `interrupted`) emit. The only caller passing `true` is
    /// [`AgentManager::interrupt_send_message`] — an interrupt that carries a
    /// follow-up message is a preemption, not a settlement: the child is about
    /// to run the interrupt turn, so waking completion watches here would
    /// deliver a spurious "child settled" report to the parent. The plain
    /// `interrupt()` / `agent.stop` keep-alive path passes `false` so STAB-28
    /// behavior (watches fire on interrupt) is preserved. `agent:stream:end`
    /// is emitted unconditionally in both paths.
    async fn interrupt_inner(&self, agent_id: &AgentId, suppress_idle_emit: bool) -> bool {
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
        // Snapshot the live-turn slot BEFORE aborting the worker: the abort
        // drops the worker future and with it the LiveTurnGuard, which clears
        // the slot — reading after the abort would race that drop and
        // frequently lose the partial content.
        let partial_turn = self.services.live_turn(agent_id);
        // Abort the in-flight worker so it stops draining the turn/queue; the
        // child is kept alive (unlike `stop`, which also kills the child).
        if let Some(worker) = self.workers.lock().unwrap().remove(agent_id) {
            worker.abort();
        }
        // Persist the streamed-so-far assistant content as an interrupted
        // assistant row (no-op for empty blocks, so the STAB-114 zero-output
        // requeue in `interrupt_send_message` never sees a phantom row). Runs
        // AFTER the abort (a worker append already in flight can still land,
        // but the `agent_message.id` PK keeps the outcome convergent — the
        // flush absorbs the UNIQUE collision) and BEFORE the terminal
        // `agent:stream:end` emit below so the chat-channel terminal
        // reconcile sees the persisted row and keeps the blocks instead of
        // removing them.
        if let Some(live) = partial_turn {
            self.services
                .flush_partial_turn_on_interruption(agent_id, live)
                .await;
        }
        // Cancel the current turn over the wire (keep-alive interrupt). The agent
        // resolves its in-flight `session/prompt` with `StopReason::Cancelled`;
        // best-effort — a wire error never blocks the stop.
        if let Err(e) = intent_acp::session::cancel(&conn, &acp_session_id).await {
            if is_cancel_transport_closed(&e) {
                // Child already dead — expected race when cancelling a dead
                // turn; the run_turn branch surfaces the real failure.
                tracing::debug!(agent = %agent_id, error = %e, "session/cancel skipped: transport already closed");
            } else {
                tracing::warn!(agent = %agent_id, error = %e, "session/cancel failed");
            }
        }
        // STAB-124: the cancelled child echoes `tool_call_update`s for the
        // aborted tool call (title-less, status failed). With the worker gone,
        // they buffer in the handle's notification channel and would be drained
        // by the NEXT turn's fresh transcript — which fabricated an anonymous
        // `tool_use` block (`name: ""`) that broke FE conversation loading.
        // Discard them with the same bounded settle-window drain the resume
        // path uses for the `session/load` replay burst. The aborted worker's
        // channel lock is released when its task drops, so this cannot deadlock.
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
            // The aborted worker never reaches the settlement idle-emit in
            // `run_prompt_turn` (agent_session.rs), so we must emit here.
            // Without this, a parent that re-messages via agent.send after the
            // child settles registers a completion watch that never fires (no
            // idle event → watch never delivered). Only emit when the agent has
            // no queued ready-to-send messages (settlement coalescing: mirrors
            // the `run_prompt_turn` check) AND the interrupt is not part of
            // interrupt-with-message (`suppress_idle_emit` — the follow-up
            // content has not been queued yet at this point, so the
            // ready-to-send check alone cannot see the imminent interrupt turn).
            if !suppress_idle_emit && !self.services.has_ready_to_send(agent_id) {
                let mut data = json!({
                    "agentId": agent_id.0,
                    "reason": "interrupted",
                    "status": "idle",
                });
                // Enrich with agentName + completion report (reuse the session
                // loaded earlier in this method; avoids duplicate I/O).
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
    /// Also persists the `agent_session.status` transition to `Active` (and clears
    /// any persisted `stop_reason`) and emits `agent:status-changed` (PROTOCOL
    /// §6.5/§6.7) so a hydrated chat reflects the live runtime rather than the
    /// stored `Pending` placeholder.
    async fn try_begin(&self, agent_id: &AgentId, workspace_id: &WorkspaceId) -> bool {
        let claimed = self.busy.lock().unwrap().insert(agent_id.clone());
        if claimed {
            self.agent_ws
                .lock()
                .unwrap()
                .insert(agent_id.clone(), workspace_id.clone());
            self.services.agent_activity_begin(workspace_id).await;
            // Clear stop_reason when starting a new turn: successful turns leave it cleared.
            self.persist_status_with_stop_reason(
                agent_id,
                workspace_id,
                AgentStatus::Active,
                true,
                Some(None),
            )
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

    /// Clear a persisted completion report when a new turn begins. Skips the
    /// store write and event when no report is set (the common case). Emits
    /// `agent:updated` with `completionReportCleared: true` when a report was
    /// present and cleared. Called at the start of each prompt turn (including
    /// queue-drained turns inside a running worker) so a delegated agent's
    /// completion report does not stick across new work.
    async fn clear_completion_report_if_present(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
    ) {
        let ts = now_iso();
        match self
            .services
            .store
            .clear_completion_report(workspace_id, agent_id, &ts)
            .await
        {
            Ok(true) => {
                // Report was present and cleared — emit agent:updated.
                self.services
                    .publish_agent_mutation_event(
                        workspace_id,
                        agent_id,
                        intent_core::events::AGENT_UPDATED,
                        json!({ "agentId": agent_id.0, "completionReportCleared": true }),
                    )
                    .await;
            }
            Ok(false) => {
                // No report was set — skip the event.
            }
            Err(e) => {
                // Store error (session not found, workspace mismatch) — log and
                // swallow so the turn can proceed. The next successful load will
                // reflect the stale report, but the runtime must not abort.
                tracing::warn!(
                    agent = %agent_id,
                    error = %e,
                    "clear completion report failed"
                );
            }
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
            .set_agent_session_status(workspace_id, agent_id, status, is_active, &ts, None)
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
        // Schedule debounced lastActivity event (§10.1).
        self.services
            .schedule_last_activity_event(workspace_id.clone());
    }

    /// Persist `agent_session.status` + `is_active` + optional `stop_reason` and
    /// publish the `agent:status-changed` self-sufficient event (PROTOCOL §6.5/§6.7).
    /// Companion to [`persist_status`]; add `stop_reason` control: `None` leaves it
    /// untouched, `Some(None)` clears it, `Some(Some(reason))` sets it. All failures
    /// are logged and swallowed: the runtime turn is the source of truth and a
    /// transient store/bus error must not abort the in-flight slot transition.
    async fn persist_status_with_stop_reason(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        status: AgentStatus,
        is_active: bool,
        stop_reason: Option<Option<String>>,
    ) {
        let ts = now_iso();
        // Clone stop_reason for event emission (we need it after the store call moves it).
        let stop_reason_for_event = stop_reason.clone();
        if let Err(e) = self
            .services
            .store
            .set_agent_session_status(workspace_id, agent_id, status, is_active, &ts, stop_reason)
            .await
        {
            tracing::warn!(agent = %agent_id, error = %e, "failed to persist agent status + stop_reason");
            return;
        }
        let serialized_status = match serde_json::to_value(status) {
            Ok(Value::String(s)) => s,
            _ => return,
        };
        // Build the event data. When stop_reason is Some(_) — i.e. the call sets or
        // clears the persisted value — include "stopReason" in the event: the string
        // when setting (Some(Some(x))), JSON null when clearing (Some(None)). When the
        // parameter is None (unchanged), omit the field so unrelated status changes
        // don't clobber the FE's canonical session state (cloudlands-fe#147).
        let mut data = json!({
            "agentId": agent_id.0,
            "status": serialized_status,
            "isActive": is_active,
        });
        if let Some(reason) = &stop_reason_for_event {
            data["stopReason"] = match reason {
                Some(r) => Value::String(r.clone()),
                None => Value::Null,
            };
        }
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
            data,
        };
        crate::publish_event(&self.services.event_bus, event).await;
        // Schedule debounced lastActivity event (§10.1).
        self.services
            .schedule_last_activity_event(workspace_id.clone());
    }

    /// Forget a finished worker's join handle.
    fn clear_worker(&self, agent_id: &AgentId) {
        self.workers.lock().unwrap().remove(agent_id);
    }

    /// `agent.sendMessage` runtime path (§5.5/§6.8): when a turn is already in
    /// flight, enqueue (the worker flips it to in-flight when the current turn
    /// ends); otherwise persist the user message (under the client-supplied
    /// `messageId` when given, else a minted `user-msg-{uuid}`), publish
    /// `agent:message` (role=user) with the persisted row id, and spawn a
    /// background worker that lazily spawns the child on first turn, drives
    /// the ACP turn through [`AgentManager::run_turn`], and drains the queue.
    /// Returns the TS-shaped `{ success, queued, messageId | queuedMessage }`
    /// where `messageId` IS the persisted row id.
    pub async fn send_message(
        self: &Arc<Self>,
        agent_id: AgentId,
        workspace_id: WorkspaceId,
        content: String,
        message_id: Option<String>,
        options: TurnOptions,
    ) -> Result<Value> {
        // Validate the caller-supplied id length BEFORE any state change
        // (mirrors `agent_send_message_op`'s unconditional guard — the row id
        // is now the client id). Hoisted above `try_begin` so a doomed
        // request never claims the slot (no Active→RuntimeIdle status flap)
        // and the busy branch never queues an oversized id.
        if let Some(ref id) = message_id {
            if id.len() > MAX_MESSAGE_ID_LEN {
                return Err(Error::InvalidParams(format!(
                    "messageId exceeds maximum length of {MAX_MESSAGE_ID_LEN} bytes"
                )));
            }
        }
        // monorepo#564: reject nonexistent targets BEFORE any state change —
        // a truncated/mistyped id must not claim the slot or queue a phantom
        // message that never drains (the sender then waits forever).
        self.services.require_agent_session(&agent_id).await?;
        if !self.try_begin(&agent_id, &workspace_id).await {
            let (queued, position) = self.services.enqueue_message(
                &agent_id,
                content,
                options.image_blocks.clone(),
                options.file_blocks.clone(),
                options.message_metadata.clone(),
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
        // STAB-133: persist FE-supplied attachments alongside the text block so
        // the transcript row carries them (the conversation view renders them).
        let blocks = user_message_blocks(
            &content,
            options.image_blocks.as_ref(),
            options.file_blocks.as_ref(),
        );
        // Persist the row UNDER the resolved `message_id` so the RPC result's
        // `messageId` and the `agent:message` event both name the actual
        // transcript row (PROTOCOL §5.5 — previously the store minted its own
        // UUIDv7 id and the result id named nothing).
        let message = match self
            .services
            .store
            .append_agent_message_with_id(
                &agent_id,
                &message_id,
                "user",
                &blocks,
                options.message_metadata.as_ref(),
                &now_iso(),
            )
            .await
        {
            Ok(message) => message,
            Err(append_err) => {
                // Store write failed on a validated agent (e.g. duplicate
                // client-supplied messageId) → auto-queue, matching the
                // `agent.sendMessage` fallback (PROTOCOL §5.5). Self-drain:
                // the slot we just released will be reclaimed below if the
                // queue is ready and the agent is otherwise free.
                self.end_turn(&agent_id).await;
                // Check-then-act race guard (monorepo#564): if the session
                // vanished between the up-front validation and the append
                // (concurrent delete), fail closed like the guard rather than
                // auto-queueing a phantom message for a gone agent.
                if self
                    .services
                    .store
                    .get_agent_session(&agent_id)
                    .await
                    .is_err()
                {
                    tracing::warn!(agent = %agent_id, error = %append_err, "agent session vanished mid-send; rejecting instead of auto-queueing");
                    return Err(Error::InvalidParams(format!(
                        "unknown agent id: {}",
                        agent_id.0
                    )));
                }
                let (queued, position) = self.services.enqueue_message(
                    &agent_id,
                    content,
                    options.image_blocks.clone(),
                    options.file_blocks.clone(),
                    options.message_metadata.clone(),
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
        };
        // Emit `agent:message` (role=user) with the persisted row id — the
        // direct-send branch previously emitted nothing, so an
        // `agent.editAndRegenerate` regenerated user message never reached
        // clients until a full reload (PROTOCOL §5.5 step 6: "the usual
        // agent:message / agent:stream:* events follow"). Mirrors the
        // queue-drain (`persist_user`) and wake-delivery emits.
        self.services
            .publish_agent_mutation_event(
                &workspace_id,
                &agent_id,
                intent_core::events::AGENT_MESSAGE,
                crate::agent_ops::agent_message_event_payload(&agent_id, &message),
            )
            .await;
        self.spawn_worker(agent_id, workspace_id, content, options, true);
        Ok(json!({ "success": true, "queued": false, "messageId": message.id }))
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
        // A session parked in `Error` must NOT be auto-redriven (STAB-52): the
        // terminal spawn/turn-failure handler requeues the failed message and
        // persists `Error` so redriving it is a deliberate act — `agent.retry`
        // (which resets the status to `Pending` before draining) or a fresh
        // `agent.sendMessage`. Without this gate any queue kick (queueMessage,
        // edit-save, wake delivery) re-claims the slot, re-spawns the failing
        // turn, and crash-loops the agent — flapping `is_active` and leaking
        // `is_active=1` rows whenever the cycle is interrupted mid-claim.
        // Fail closed: a session lookup error (transient store error, missing
        // row) also skips the drain — a later queue kick retries, and silently
        // redriving a possibly-errored agent is the exact bug this gate stops.
        match self
            .services
            .store
            .get_agent_session_status(&agent_id)
            .await
        {
            Ok(AgentStatus::Error) => {
                tracing::debug!(
                    agent = %agent_id,
                    "skipping queue drain: session parked in error state (awaiting agent.retry)"
                );
                return;
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(
                    agent = %agent_id,
                    error = %e,
                    "skipping queue drain: agent session status lookup failed"
                );
                return;
            }
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
        // Skip the transcript append for a terminal-failure requeue whose
        // user row already reached the transcript before the failed turn
        // began; otherwise persist now (with `persist_user`'s bounded retry).
        // Fail closed (#547): if the append still fails, do NOT start the
        // turn — park the agent in `Error` with the message requeued
        // (`persisted: false`) so `agent.retry` re-attempts the append,
        // instead of producing assistant output for a user row that never
        // reached the transcript.
        let user_persisted = if next.persisted {
            true
        } else {
            persist_user(
                &self,
                &agent_id,
                &workspace_id,
                &next.content,
                next.image_blocks.as_ref(),
                next.file_blocks.as_ref(),
                next.message_metadata.as_ref(),
            )
            .await
        };
        // Queue-drained turns carry no per-turn prompt hints of their own,
        // but the FE-supplied attachments and `messageMetadata` captured at
        // enqueue time do ride along so the drained turn receives the same
        // image + file blocks and a terminal-failure requeue keeps the tag.
        let options = TurnOptions {
            image_blocks: next.image_blocks.clone(),
            file_blocks: next.file_blocks.clone(),
            message_metadata: next.message_metadata.clone(),
            ..TurnOptions::default()
        };
        if !user_persisted {
            handle_drain_persist_failure(&self, &agent_id, &workspace_id, &next.content, &options)
                .await;
            // Release the slot without overwriting the Error status just
            // persisted, so `agent.retry` (or a future message) can redrive.
            self.release_in_flight_slot(&agent_id).await;
            return;
        }
        self.spawn_worker(
            agent_id,
            workspace_id,
            next.content,
            options,
            user_persisted,
        );
    }

    /// `agent.forceMessage` runtime path (§5.5): stop the current stream (abort
    /// the worker + kill the child — the preempted turn's streamed-so-far
    /// output persists as an interrupted assistant row via the `detach` flush),
    /// discard the pending queue, then deliver the forced message immediately
    /// as a fresh turn.
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
        // STAB-133: persist FE-supplied attachments alongside the text block.
        let blocks = user_message_blocks(
            &content,
            options.image_blocks.as_ref(),
            options.file_blocks.as_ref(),
        );
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
        self.spawn_worker(agent_id, workspace_id, content, options, true);
        Ok(json!({ "success": true, "queued": false, "messageId": message_id }))
    }

    /// `agent.editAndRegenerate` runtime path (§5.5): edit a past user message
    /// and regenerate from that point. Orchestrates, in order:
    ///
    /// 1. Validate `message_id` refers to an existing **user** message
    ///    (read-only, BEFORE any state changes — a bad id surfaces `-32602`
    ///    without stopping the turn or touching the transcript).
    /// 2. Stop any in-flight turn (same hard-cancel + queue-discard semantics
    ///    as [`AgentManager::force_message`]).
    /// 3. Optionally switch the model (the `model` param, via `agent.setModel`
    ///    semantics) before the regenerated turn spawns.
    /// 4. Truncate the transcript to just before the edited message (emits
    ///    `agent:updated` with `{ truncatedCount, remainingCount }`).
    /// 5. Arm the forced-recreate flag so the next prompt SKIPS `session/load`
    ///    and opens a fresh `session/new` (the provider must not retain the
    ///    truncated turns), plus the `recreated` flag so the truncated history
    ///    replays as `<supervisor>` XML on that prompt.
    /// 6. Send `content` as a fresh user message (normal
    ///    [`AgentManager::send_message`] path; stream events follow).
    pub async fn edit_and_regenerate(
        self: &Arc<Self>,
        agent_id: AgentId,
        workspace_id: WorkspaceId,
        message_id: String,
        content: String,
        model: Option<String>,
        options: TurnOptions,
    ) -> Result<Value> {
        self.services
            .agent_validate_edit_target_op(&agent_id, &message_id)
            .await?;
        // Arm `force_recreate` IMMEDIATELY after validation — before `stop()`
        // — to shrink the window where a concurrent turn could establish a
        // resumed session between the stop and the arm. It survives `stop()`
        // by design, and a spuriously-armed flag is safe (worst case: one
        // unnecessary fresh session + history replay). `ensure_started` also
        // consults it on the live-child reuse path, so an interleaved turn
        // that does establish a session before the truncation still gets torn
        // down and recreated on the next prompt.
        self.force_recreate.lock().unwrap().insert(agent_id.clone());
        self.stop(&agent_id).await;
        if self.services.clear_queue(&agent_id) {
            self.services
                .publish_queue_updated_for(&agent_id, &workspace_id, Vec::new())
                .await;
        }
        // Until the truncation actually lands, a failure must DISARM the
        // flag: nothing was truncated, so leaving it set would force an
        // unnecessary session recreate (lost provider warm state) on the next
        // unrelated turn. After the truncation persists, the flag must stay
        // armed no matter what fails later.
        let pre_truncate = async {
            if let Some(model_id) = model {
                self.services
                    .agent_set_model_op(agent_id.clone(), model_id)
                    .await?;
            }
            self.services
                .agent_edit_truncate_op(&agent_id, &message_id)
                .await
        };
        let truncated_count = match pre_truncate.await {
            Ok(count) => count,
            Err(e) => {
                self.force_recreate.lock().unwrap().remove(&agent_id);
                return Err(e);
            }
        };
        // Arm `recreated` AFTER `stop` (which clears it): it makes the next
        // turn prepend the truncated history as `<supervisor>` XML.
        self.recreated.lock().unwrap().insert(agent_id.clone());
        let mut result = self
            .send_message(agent_id, workspace_id, content, None, options)
            .await?;
        if let Some(obj) = result.as_object_mut() {
            obj.insert("truncatedCount".to_string(), json!(truncated_count));
        }
        Ok(result)
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
        // monorepo#564: reject nonexistent targets BEFORE the dedup record or
        // any preemption — same fail-closed guard as `send_message`.
        self.services.require_agent_session(&agent_id).await?;
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
                // STAB-114: Check if the current turn has produced zero output
                // (no assistant content chunks) BEFORE we cancel. Use the live-turn
                // slot (not persisted transcript) to detect zero output: assistant
                // rows are only persisted at turn END, so an interrupted mid-stream
                // turn would incorrectly look like zero output if we checked the
                // transcript. The LiveTurn.blocks are assistant blocks by construction
                // (see Transcript::snapshot_blocks), so non-empty means output exists.
                let has_output = self
                    .services
                    .live_turn(&agent_id)
                    .map(|live| !live.blocks.is_empty())
                    .unwrap_or(false);

                // Cancel the turn IMMEDIATELY to prevent it from finishing while
                // we prepare the re-queue logic below. This releases the in-flight
                // slot and aborts the draining worker. The STAB-28 synthetic
                // `agent:idle` is suppressed: this interrupt carries a
                // follow-up message (the child is being preempted, not
                // settling), so completion watches must not report "child
                // settled" to the parent here.
                self.interrupt_inner(&agent_id, true).await;

                if !has_output {
                    // Zero-output condition: re-queue the preempted message.
                    // Fetch last 10 transcript messages (bounded work) to find the
                    // user message + its attachments. If any non-user messages
                    // (assistant/tool/system) exist after the last user message, the
                    // turn has already progressed and we should NOT re-queue (avoids
                    // duplicate tool calls or re-running side effects).
                    if let Ok(messages) = self
                        .services
                        .store
                        .get_agent_messages(&agent_id, Some(10))
                        .await
                    {
                        if let Some(last_user_msg) =
                            messages.iter().rev().find(|m| m.role == "user")
                        {
                            let last_user_idx = messages
                                .iter()
                                .rposition(|m| m.id == last_user_msg.id)
                                .unwrap();
                            let has_non_user_after = messages
                                .iter()
                                .skip(last_user_idx + 1)
                                .any(|m| m.role != "user");

                            if !has_non_user_after {
                                // Extract text from content blocks (JSON array).
                                let text_content = if let Some(blocks) =
                                    last_user_msg.content.as_array()
                                {
                                    blocks
                                        .iter()
                                        .filter(|b| {
                                            b.get("type").and_then(Value::as_str) == Some("text")
                                        })
                                        .filter_map(|b| b.get("text").and_then(Value::as_str))
                                        .collect::<Vec<&str>>()
                                        .join("\n")
                                } else {
                                    String::new()
                                };

                                // Extract image_blocks and file_blocks from content.
                                let image_blocks =
                                    last_user_msg.content.as_array().and_then(|blocks| {
                                        let imgs: Vec<Value> = blocks
                                            .iter()
                                            .filter(|b| {
                                                b.get("type").and_then(Value::as_str)
                                                    == Some("image")
                                            })
                                            .cloned()
                                            .collect();
                                        if imgs.is_empty() {
                                            None
                                        } else {
                                            Some(Value::Array(imgs))
                                        }
                                    });

                                let file_blocks =
                                    last_user_msg.content.as_array().and_then(|blocks| {
                                        let files: Vec<Value> = blocks
                                            .iter()
                                            .filter(|b| {
                                                b.get("type").and_then(Value::as_str)
                                                    == Some("file")
                                            })
                                            .cloned()
                                            .collect();
                                        if files.is_empty() {
                                            None
                                        } else {
                                            Some(Value::Array(files))
                                        }
                                    });

                                // `persisted: true` prevents duplicate transcript append;
                                // `requeued_after_failure: false` so the FE does not show
                                // "failed — will retry" (interrupt ≠ failure, STAB-114).
                                let queued = crate::agent_ops::QueuedMessage {
                                    id: crate::agent_ops::new_message_id(),
                                    content: text_content,
                                    image_blocks,
                                    file_blocks,
                                    queued_at: crate::now_iso(),
                                    editing: false,
                                    persisted: true,
                                    requeued_after_failure: false,
                                    message_metadata: last_user_msg.metadata.clone(),
                                };
                                self.services.requeue_front(&agent_id, queued);

                                // Publish queue updated so FE reflects the re-queued message
                                self.services
                                    .publish_queue_updated_for(
                                        &agent_id,
                                        &workspace_id,
                                        self.services.queue_snapshot(&agent_id),
                                    )
                                    .await;
                            }
                        }
                    }
                }
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
    /// already hold the in-flight slot (`try_begin`). `user_persisted` reports
    /// whether the initial turn's user row durably reached the transcript, so a
    /// terminal-failure requeue carries the true durability state (STAB-51).
    fn spawn_worker(
        self: &Arc<Self>,
        agent_id: AgentId,
        workspace_id: WorkspaceId,
        content: String,
        options: TurnOptions,
        user_persisted: bool,
    ) {
        let mgr = self.clone();
        let id = agent_id.clone();
        let handle = tokio::spawn(async move {
            run_message_worker(mgr, id, workspace_id, content, options, user_persisted).await;
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
    /// the error status, tears down any stale child, and attempts to redrive
    /// the front-of-queue message (requeued at exhaustion) plus any subsequent
    /// messages. Reuses the spawn-retry/backoff machinery, so a retry that
    /// fails again lands back in the `error` state with events.
    ///
    /// The result carries `redriven` so clients can distinguish "a queued
    /// message is being redriven" (`true` — status cleared to `pending`, drain
    /// started) from "the queue was empty, nothing to redrive" (`false` —
    /// status cleared to `idle`; the next `agent.sendMessage` starts a fresh
    /// turn). Without this, an empty-queue retry was an invisible no-op: the
    /// agent parked in `pending` with no worker and the FE got a bare
    /// `{ ok: true }` (STAB-54).
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

        // Empty queue → nothing will drive a `pending` status forward, so
        // clear the error to `idle` instead (idle is permitted iff no
        // ready-to-send work remains, PROTOCOL §5.5/§6.5 invariant).
        let mut redriven = self.services.has_ready_to_send(&agent_id);
        let next_status = if redriven {
            AgentStatus::Pending
        } else {
            AgentStatus::RuntimeIdle
        };

        // Clear the error status and emit agent:status-changed
        self.persist_retry_status(&agent_id, workspace_id, next_status)
            .await?;

        // Abort any in-flight worker task and release the in-flight slot
        if let Some(worker) = self.workers.lock().unwrap().remove(&agent_id) {
            worker.abort();
        }
        self.release_in_flight_slot(&agent_id).await;

        // Tear down any stale child handle (use kill_child_only to avoid
        // overwriting the status we just set)
        self.kill_child_only(&agent_id).await;

        // Close the check-then-flip race: a message enqueued between the queue
        // check above and the status flip had its own drain attempt suppressed
        // by the Error gate in `try_drain_queue` (STAB-52), and this path was
        // about to skip the drain too — stranding a ready-to-send message.
        // Re-poll under the post-Error status; anything there is a redrive.
        if !redriven && self.services.has_ready_to_send(&agent_id) {
            redriven = true;
            self.persist_retry_status(&agent_id, workspace_id, AgentStatus::Pending)
                .await?;
        }

        // Start the drain loop to redrive the requeued message
        if redriven {
            self.clone()
                .try_drain_queue(agent_id, workspace_id.clone())
                .await;
        }

        Ok(json!({ "ok": true, "redriven": redriven }))
    }

    /// Persist an `agent.retry` status transition (clearing any persisted
    /// `stop_reason`) and publish the matching `agent:status-changed` event.
    /// Shared by the initial Error-clear flip and the post-flip re-check that
    /// promotes Idle → Pending when a message slipped into the queue during the
    /// retry (see [`AgentManager::agent_retry`]).
    async fn persist_retry_status(
        self: &Arc<Self>,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        status: AgentStatus,
    ) -> Result<()> {
        let is_active = false;
        // Clear stop_reason on retry: the agent is starting fresh, not stuck in an error.
        // Route through persist_status_with_stop_reason to ensure the agent:status-changed
        // event carries stopReason: null.
        self.persist_status_with_stop_reason(agent_id, workspace_id, status, is_active, Some(None))
            .await;
        Ok(())
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
        self.spawn_worker(agent_id, workspace_id, content, options, true);
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
    /// session otherwise. When the session's model/provider has changed (via
    /// `agent.setModel`), tears down the existing child and respawns with the
    /// new model before the next turn. Returns the `acpSessionId` to drive the turn.
    async fn ensure_started(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
    ) -> Result<String> {
        let session = self.services.store.get_agent_session(agent_id).await?;
        let workspace = self.services.store.get_workspace(workspace_id).await.ok();
        let resolved = resolve_spawn(
            &session,
            workspace.as_ref(),
            &self.services.effective_settings(),
        )?;

        // Check if the agent's model/provider has changed (via agent.setModel).
        // If so, tear down the existing child and force a respawn with the new model.
        if self.contains(agent_id) {
            let needs_respawn = {
                let handles = self.handles.lock().unwrap();
                if let Some(handle) = handles.get(agent_id) {
                    // Compare the session's currently-resolved model/provider against
                    // the values the child was spawned with. A mismatch means
                    // agent.setModel was called while the child was live.
                    let model_changed =
                        handle.spawned_model.as_deref() != resolved.model.as_deref();
                    let provider_changed = handle.spawned_provider != resolved.provider.command;
                    model_changed || provider_changed
                } else {
                    false
                }
            };

            // Forced recreate (`agent.editAndRegenerate`): the live child's
            // provider session predates the truncation, so it must not be
            // reused OR resumed — tear it down and let `start_session` open a
            // fresh `session/new`. Checking here (not just in `start_session`)
            // makes `ensure_started` the single enforcement point regardless
            // of how an edit interleaves with concurrent turns: without it,
            // the live-child reuse branch below would return the stale session
            // with the armed flag sitting unconsumed.
            let forced = self.force_recreate.lock().unwrap().contains(agent_id);
            if needs_respawn || forced {
                // Tear down the existing child (preserving the acpSessionId so
                // start_session can try session/load for providers that support it).
                // This is narrower than stop() — only kills the child/handle, no
                // worker/busy-flag touch, matching the retry-spawn teardown path.
                self.kill_child_only(agent_id).await;
            } else if let Some(acp) = session.acp_session_id.clone() {
                // Model unchanged and child is live — reuse the existing session.
                return Ok(acp);
            }
        }
        let mut opts = SpawnOptions::new(&resolved.provider);
        opts.cwd = Some(&resolved.cwd);
        opts.model = resolved.model.as_deref();
        opts.provider_binary = resolved.provider_binary.as_deref();
        opts.npx_fallback_binary = resolved.npx_fallback_binary.as_deref();
        opts.npx_fallback_package = resolved.npx_fallback_package;
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
    /// Before stopping each in-flight agent, capture it as an interrupted session
    /// so the FE modal offers resumption on next launch — same as a crash (INT-41
    /// graceful-shutdown gap).
    pub async fn shutdown(&self) {
        let ids: Vec<AgentId> = self.handles.lock().unwrap().keys().cloned().collect();
        let now = intent_core::now_iso();

        // Capture in-flight agents before stop() settles them to RuntimeIdle.
        for id in &ids {
            // Only agents currently in-flight (in the busy set) need interruption rows.
            if !self.busy.lock().unwrap().contains(id) {
                continue;
            }
            // Read the workspace from agent_ws (stop() will clear it via end_turn).
            let workspace_id = match self.agent_ws.lock().unwrap().get(id).cloned() {
                Some(ws) => ws,
                None => continue, // Stale busy entry (should not happen).
            };
            // Snapshot the live-turn slot BEFORE aborting the worker: the abort
            // drops the worker future and with it the LiveTurnGuard, which
            // clears the slot — reading after the abort would race that drop
            // and frequently lose the partial content.
            let partial_turn = self.services.live_turn(id);
            // Abort the turn worker BEFORE flushing so it cannot race the
            // partial flush by persisting the full turn under the same minted
            // message id (which would leave the transcript stuck on the partial
            // snapshot while the worker's own append errors on the UNIQUE id).
            // stop() below removes the (already-gone) worker entry harmlessly.
            if let Some(worker) = self.workers.lock().unwrap().remove(id) {
                worker.abort();
            }
            // Best-effort: persist any partial in-flight assistant content from
            // the snapshot so the transcript keeps the streamed-so-far output
            // across the restart. Runs before the status guards below so a
            // degenerate status read/encode failure never drops the content.
            if let Some(live) = partial_turn {
                self.services
                    .flush_partial_turn_on_interruption(id, live)
                    .await;
            }
            // Read the current persisted status BEFORE end_turn settles it to RuntimeIdle.
            // Use get_agent_session_status (lightweight, skips message log).
            // RACE: try_begin inserts into busy BEFORE persist_status(Active) completes, so
            // shutdown in that window may read Pending. Busy-set membership is authoritative:
            // if the agent is in busy, it's mid-turn regardless of the persisted status.
            let prev_status = match self.services.store.get_agent_session_status(id).await {
                Ok(status) => status,
                Err(e) => {
                    tracing::warn!(agent_id = %id, error = %e, "graceful shutdown: could not read session status");
                    continue;
                }
            };
            // Serialize the status via serde to match the DB form (e.g., "active", "Waiting").
            // If encoding fails or produces a non-string, skip this agent (do not persist an
            // undocumented status string). If the persisted status is non-in-flight (e.g.,
            // Pending due to the try_begin race), fall back to "active" — busy membership
            // proves the agent is mid-turn.
            let prev_str = match serde_json::to_value(prev_status) {
                Ok(serde_json::Value::String(s)) => {
                    // Non-in-flight statuses (pending/idle/error/deleted) mean we raced with
                    // persist_status. Busy membership is authoritative: use "active".
                    if matches!(
                        prev_status,
                        AgentStatus::Pending
                            | AgentStatus::RuntimeIdle
                            | AgentStatus::Idle
                            | AgentStatus::Error
                            | AgentStatus::Deleted
                    ) {
                        "active".to_string()
                    } else {
                        s
                    }
                }
                Ok(other) => {
                    tracing::warn!(agent_id = %id, status = ?prev_status, encoded = ?other, "graceful shutdown: status encoded to non-string, skipping interrupted_agent row");
                    continue;
                }
                Err(e) => {
                    tracing::warn!(agent_id = %id, status = ?prev_status, error = %e, "graceful shutdown: status encoding failed, skipping interrupted_agent row");
                    continue;
                }
            };
            // Insert the interrupted_agent row (idempotent upsert: if a prior crash captured
            // this agent and the daemon was restarted without the FE resolving it, the row
            // is refreshed to the latest state).
            if let Err(e) = self
                .services
                .store
                .insert_interrupted_agent(id, &workspace_id, &prev_str, &now)
                .await
            {
                tracing::warn!(agent_id = %id, workspace_id = %workspace_id, error = %e, "graceful shutdown: failed to insert interrupted_agent row");
            }
        }

        // Now tear down every agent's bookkeeping (settles to RuntimeIdle) and
        // collect the detached children, then kill all process groups in
        // parallel under ONE shared grace window — total teardown stays ~one
        // grace period regardless of agent count, instead of N sequential
        // SIGTERM→grace→SIGKILL cycles (which would blow past the 5s
        // SIGTERM→SIGKILL windows of `intentd stop` and the Electron sidecar).
        let mut children = Vec::new();
        for id in &ids {
            let (_, child) = self.detach(id).await;
            if let Some(child) = child {
                children.push(child);
            }
        }
        kill_child_trees(children).await;
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

/// Short bounded window after the SIGKILL sweep in [`kill_child_trees`] during
/// which the reap tasks are awaited so killed children are actually `wait()`ed
/// before returning (SIGKILLed children reap almost instantly).
#[cfg(unix)]
const KILL_SWEEP_REAP_GRACE: Duration = Duration::from_millis(500);

/// Terminate a spawned provider's WHOLE process tree (§5.6). The child is its
/// own process-group leader (`process_group(0)` at spawn), so `killpg(pgid,…)`
/// reaches every descendant — `kill_on_drop` alone only reaps the direct child,
/// orphaning grandchildren. SIGTERM first for a clean exit, then SIGKILL after a
/// grace period to sweep anything that ignored it. Descendants that escaped
/// into their OWN process groups survive the `killpg`, so they are snapshotted
/// before the kill and swept afterwards (`intent_acp::descendant_sweep`).
#[cfg(unix)]
async fn kill_child_tree(mut child: Child) {
    use intent_acp::{descendant_pids, sweep_escaped_descendants};
    use nix::sys::signal::{killpg, Signal};
    use nix::unistd::Pid;

    let Some(pid) = child.id() else {
        let _ = child.start_kill();
        return;
    };
    let descendants = descendant_pids(pid).await;
    let pgid = Pid::from_raw(pid as i32);
    let _ = killpg(pgid, Signal::SIGTERM);
    // Wait briefly for the group to drain, then SIGKILL the whole group so any
    // grandchild that ignored SIGTERM is still removed.
    let _ = tokio::time::timeout(PROCESS_GROUP_TERM_GRACE, child.wait()).await;
    let _ = killpg(pgid, Signal::SIGKILL);
    sweep_escaped_descendants(&descendants).await;
}

/// Non-unix fallback: no process groups, so fall back to killing the direct
/// child (`kill_on_drop` remains the safety net on drop).
#[cfg(not(unix))]
async fn kill_child_tree(mut child: Child) {
    let _ = child.start_kill();
}

/// Parallel shutdown kill sweep: terminate MANY provider process trees under
/// ONE shared grace window. Every group is SIGTERMed up-front, then a single
/// [`PROCESS_GROUP_TERM_GRACE`] window covers the whole batch, then every
/// still-live group is SIGKILLed — so total teardown is ~one grace period
/// regardless of how many agents were running (unlike per-child
/// [`kill_child_tree`], which serialises one grace window per tree). The
/// pre-kill descendant snapshot (bounded at 2s for a hung `ps`) and the
/// post-kill escape sweep (one extra grace window when something escaped)
/// add hard-bounded overhead on top of that shared window.
#[cfg(unix)]
async fn kill_child_trees(children: Vec<Child>) {
    use intent_acp::{descendant_pids_many, sweep_escaped_descendants};
    use nix::sys::signal::{killpg, Signal};
    use nix::unistd::Pid;

    // Phase 0: snapshot every tree's descendants BEFORE signalling (one shared
    // `ps` for the whole batch) so descendants that escaped into their own
    // process groups can be swept after the group kills — post-kill they
    // reparent to init and become invisible (`intent_acp::descendant_sweep`).
    let roots: Vec<u32> = children.iter().filter_map(|c| c.id()).collect();
    let descendants = descendant_pids_many(&roots).await;

    // Phase 1: SIGTERM every group up-front so all trees start exiting at once.
    let mut pgids = Vec::new();
    let mut waits = Vec::new();
    for mut child in children {
        match child.id() {
            Some(pid) => {
                let pgid = Pid::from_raw(pid as i32);
                let _ = killpg(pgid, Signal::SIGTERM);
                pgids.push(pgid);
                // Reap on a task so all waits run concurrently.
                waits.push(tokio::spawn(async move {
                    let _ = child.wait().await;
                }));
            }
            None => {
                // Already reaped — nothing to signal.
                let _ = child.start_kill();
            }
        }
    }
    // Phase 2: ONE shared grace window over the whole batch. Handles that
    // don't finish in time are kept so they can be awaited again after the
    // SIGKILL sweep.
    let deadline = tokio::time::Instant::now() + PROCESS_GROUP_TERM_GRACE;
    let mut pending = Vec::new();
    for mut w in waits {
        if tokio::time::timeout_at(deadline, &mut w).await.is_err() {
            pending.push(w);
        }
    }
    // Phase 3: concurrent SIGKILL sweep for anything that ignored SIGTERM
    // (no-op on groups that already exited).
    for pgid in pgids {
        let _ = killpg(pgid, Signal::SIGKILL);
    }
    // Phase 4: bounded reap — await the remaining wait tasks briefly so
    // SIGKILLed children are actually `wait()`ed before returning. Any
    // straggler past this window is still reaped by its background task; the
    // bound just keeps total shutdown within budget.
    let reap_deadline = tokio::time::Instant::now() + KILL_SWEEP_REAP_GRACE;
    for mut w in pending {
        let _ = tokio::time::timeout_at(reap_deadline, &mut w).await;
    }
    // Phase 5: sweep snapshotted descendants that survived the group kills
    // (foreign process groups). No-cost when nothing escaped; otherwise one
    // extra bounded SIGTERM→grace→SIGKILL pass over the survivors.
    sweep_escaped_descendants(&descendants).await;
}

/// Non-unix fallback: no process groups, so kill each direct child; the kills
/// are signal-only (no grace waits), so the sweep is already time-bounded.
#[cfg(not(unix))]
async fn kill_child_trees(children: Vec<Child>) {
    for child in children {
        kill_child_tree(child).await;
    }
}

#[cfg(all(test, unix))]
mod kill_sweep_tests {
    //! Timing proof for the parallel shutdown kill sweep: N children that
    //! ignore SIGTERM must tear down in ~ONE shared grace window, not N
    //! sequential ones.

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn slow_children_tear_down_in_one_shared_grace_window() {
        const N: usize = 4;
        let mut children = Vec::with_capacity(N);
        for _ in 0..N {
            let mut cmd = tokio::process::Command::new("sh");
            // Ignore SIGTERM so each child only dies on the SIGKILL sweep,
            // forcing the full grace window to elapse.
            cmd.args(["-c", "trap '' TERM; sleep 30"]);
            cmd.process_group(0);
            cmd.kill_on_drop(true);
            children.push(cmd.spawn().expect("spawn slow child"));
        }
        // Let each sh install its trap before SIGTERM arrives.
        tokio::time::sleep(Duration::from_millis(300)).await;

        let start = std::time::Instant::now();
        kill_child_trees(children).await;
        let elapsed = start.elapsed();

        // Serial teardown would take ~N * grace (8s for 4 children); the
        // shared window must finish in ~one grace period (<4s total).
        assert!(
            elapsed < PROCESS_GROUP_TERM_GRACE * 2,
            "parallel sweep took {elapsed:?}, expected ~one {PROCESS_GROUP_TERM_GRACE:?} grace window"
        );
        // The children ignored SIGTERM, so the full shared grace must have
        // elapsed (proves the window ran once, not that children died early).
        assert!(
            elapsed >= PROCESS_GROUP_TERM_GRACE - Duration::from_millis(500),
            "sweep returned after {elapsed:?}, before the shared grace window elapsed"
        );
    }
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
/// cwd, provider binary path (when resolved), and extra env the borrowing
/// [`SpawnOptions`] reference during a spawn.
struct ResolvedSpawn {
    provider: ProviderConfig,
    model: Option<String>,
    cwd: PathBuf,
    provider_binary: Option<PathBuf>,
    extra_env: BTreeMap<String, String>,
    /// When provider_binary is None and the provider has a fallback_npx_package,
    /// this is the resolved npx path. Otherwise None.
    npx_fallback_binary: Option<PathBuf>,
    /// The package name to pass to npx when npx_fallback_binary is set.
    npx_fallback_package: Option<&'static str>,
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

/// Effective provider id for a session. Provider precedence: when the model
/// carries an explicit `provider:` prefix (e.g., "opencode:kimi-k3"), that
/// prefix wins over `session.provider`, because a cross-provider model switch
/// should spawn the new provider's binary. `session.provider` is only used as
/// a fallback for bare model ids, then the default provider. Delegates to
/// [`crate::agent_session::resolve_provider_id`], which also guards against
/// malformed compound ids like `:sonnet` (empty prefixes fall through to the
/// provider field / default).
fn session_provider_id(session: &AgentSession) -> String {
    crate::agent_session::resolve_provider_id(session.model.as_deref(), session.provider.as_deref())
}

/// Fallback phrasing for the workspace-naming nudge when the provider's MCP
/// tool naming convention is unknown (or its workspace-MCP wiring hasn't
/// landed yet).
const GENERIC_NAMING_TOOL_REFERENCE: &str =
    "the `set_workspace_title` tool from the workspace MCP server";

/// Provider-correct spelling of the workspace-MCP rename tool for the naming
/// nudge. Providers affix the MCP server name differently: auggie exposes
/// `<tool>_<server>` (trailing suffix → `set_workspace_title_workspace-mcp`),
/// opencode exposes `<server>_<tool>` (leading prefix →
/// `workspace-mcp_set_workspace_title`; confirmed against captured opencode
/// 1.18.3 traffic). Every other provider gets the generic fallback phrasing.
fn workspace_naming_tool_reference(provider_id: &str) -> &'static str {
    match provider_id {
        "auggie" => "the `set_workspace_title_workspace-mcp` tool",
        "opencode" => "the `workspace-mcp_set_workspace_title` tool",
        _ => GENERIC_NAMING_TOOL_REFERENCE,
    }
}

/// Resolve everything needed to spawn (or respawn) this
/// agent's child from its persisted session + workspace. The provider id comes
/// from [`session_provider_id`]. The `mock` provider (E2E) reads its script
/// from `MOCK_AGENT_SCRIPT_PATH` and enables `--mcp-config` so a daemon-spawned
/// child reaches the per-agent workspace MCP server, forwarding
/// `MOCK_AGENT_BEHAVIOR` to the child. npx-only providers (claude-code) are
/// always spawned via `npx -y <pinned package>` — no local-binary discovery.
/// Other providers resolve their binary to an absolute path using the
/// precedence: `providers.paths` map → `~/.augment/bin/<command>` (for auggie)
/// → enhanced PATH scan.
fn resolve_spawn(
    session: &AgentSession,
    workspace: Option<&intent_core::Workspace>,
    settings: &intent_core::settings_file::SettingsFile,
) -> Result<ResolvedSpawn> {
    let provider_id = session_provider_id(session);
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
        // `MOCK_AGENT_SESSION_MCP=1` flips the mock from `--mcp-config` file
        // delivery to ACP session-setup delivery (`session/new` `mcpServers`),
        // so the E2E suite can exercise the claude-code/codex/droid/grok wire
        // path (STAB-156) against the real daemon.
        let session_mcp = std::env::var("MOCK_AGENT_SESSION_MCP").is_ok_and(|v| v == "1");
        // `MOCK_AGENT_CONFIG_OPTION_MODEL=1` marks the mock as a
        // config-option-model provider (claude-code-like), so the E2E suite
        // can exercise the post-session `session/set_config_option` model
        // application against the real daemon.
        let config_option_model =
            std::env::var("MOCK_AGENT_CONFIG_OPTION_MODEL").is_ok_and(|v| v == "1");
        let provider = ProviderConfig {
            command: "node",
            base_args,
            supports_authenticate: true,
            supports_mcp_config: !session_mcp,
            mcp_config_flag: if session_mcp {
                None
            } else {
                Some("--mcp-config")
            },
            supports_session_mcp_servers: session_mcp,
            supports_config_option_model: config_option_model,
            ..*base
        };
        return Ok(ResolvedSpawn {
            provider,
            model: None,
            cwd,
            provider_binary: None,
            extra_env,
            npx_fallback_binary: None,
            npx_fallback_package: None,
        });
    }

    let provider = *intent_providers::provider_config(&provider_id);

    // npx-only providers (claude-code) are spawned exclusively via
    // `npx -y <pinned package>`; local-binary discovery (settings path /
    // managed bin / PATH scan) is skipped entirely.
    if provider.npx_only_package.is_some() {
        if read_provider_path_setting(settings, &provider_id).is_some() {
            tracing::warn!(
                provider_id = provider_id,
                "providers.paths override ignored: {} always spawns via pinned npx",
                provider_id
            );
        }
        let (npx_binary, npx_package) = resolve_npx_only(&provider, intent_providers::find_npx())?;
        return Ok(ResolvedSpawn {
            provider,
            model,
            cwd,
            provider_binary: None,
            extra_env,
            npx_fallback_binary: Some(npx_binary),
            npx_fallback_package: Some(npx_package),
        });
    }

    // Resolve provider binary using the precedence: setting → managed → PATH
    let explicit_path = read_provider_path_setting(settings, &provider_id);
    let provider_binary = intent_providers::find_provider_binary(
        &provider_id,
        provider.command,
        explicit_path.as_deref(),
    );

    // When the provider binary is not found but the provider has a fallback npx
    // package, resolve npx itself and record the fallback decision
    let (npx_fallback_binary, npx_fallback_package) = if provider_binary.is_none() {
        if let Some(pkg) = provider.fallback_npx_package {
            if let Some(npx_path) = intent_providers::find_npx() {
                tracing::info!(
                    provider_id = provider_id,
                    npx_path = ?npx_path,
                    package = pkg,
                    "provider binary not found; falling back to npx"
                );
                (Some(npx_path), Some(pkg))
            } else {
                (None, None)
            }
        } else {
            (None, None)
        }
    } else {
        (None, None)
    };

    Ok(ResolvedSpawn {
        provider,
        model,
        cwd,
        provider_binary,
        extra_env,
        npx_fallback_binary,
        npx_fallback_package,
    })
}

/// Resolve the npx spawn inputs for an npx-only provider. `npx_path` is the
/// caller-supplied `find_npx()` result (parameterized as a test seam). Missing
/// npx is a hard, user-facing error — there is no local-binary fallback.
fn resolve_npx_only(
    provider: &ProviderConfig,
    npx_path: Option<PathBuf>,
) -> Result<(PathBuf, &'static str)> {
    let pkg = provider.npx_only_package.ok_or_else(|| {
        Error::Internal(format!(
            "provider {} is not configured for npx-only spawning",
            provider.id
        ))
    })?;
    let npx = npx_path.ok_or_else(|| {
        // InvalidInput (not Internal): this is an environment misconfiguration,
        // and its Display survives the JSON-RPC envelope (`domain_to_rpc` masks
        // Internal messages behind a literal "Internal error").
        Error::InvalidInput(format!(
            "npx not found — {} is required to run {}. Install Node.js (which provides npx) and try again.",
            intent_providers::CLAUDE_AGENT_ACP_NODE_REQUIREMENT,
            provider.display_name
        ))
    })?;
    tracing::info!(
        provider_id = provider.id,
        npx_path = ?npx,
        package = pkg,
        "spawning npx-only provider via pinned npx package"
    );
    Ok((npx, pkg))
}

/// Rebuild the caller's [`SpawnOptions`] for `create_agent`, injecting the
/// generated rules/MCP config paths while preserving every other field of the
/// incoming opts. Notably the npx fallback pair must survive: dropping it
/// makes `build_command` fall back to the bare provider command and fail with
/// ENOENT when no local provider binary exists (codex fallback / claude-code
/// npx-only spawns).
fn rebuild_spawn_opts<'a>(
    opts: &SpawnOptions<'a>,
    rules_file_path: Option<&'a str>,
    mcp_config_path: Option<&'a str>,
    env_mcp_config: Option<&'a str>,
) -> SpawnOptions<'a> {
    let mut spawn_opts = SpawnOptions::new(opts.provider);
    spawn_opts.model = opts.model;
    spawn_opts.cwd = opts.cwd;
    spawn_opts.rules_file = opts.rules_file.or(rules_file_path);
    spawn_opts.quiet = opts.quiet;
    spawn_opts.provider_binary = opts.provider_binary;
    spawn_opts.npx_fallback_binary = opts.npx_fallback_binary;
    spawn_opts.npx_fallback_package = opts.npx_fallback_package;
    spawn_opts.extra_env = opts.extra_env.clone();
    spawn_opts.tools_to_remove = opts.tools_to_remove.clone();
    spawn_opts.mcp_config_file = mcp_config_path;
    spawn_opts.env_mcp_config = env_mcp_config;
    spawn_opts
}

/// Read the provider path from the `providers.paths` map setting, if set.
fn read_provider_path_setting(
    settings: &intent_core::settings_file::SettingsFile,
    provider_id: &str,
) -> Option<String> {
    let path = settings.providers.paths.get(provider_id)?;
    let trimmed = path.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
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
    initial_persisted: bool,
) {
    let mut content = initial_content;
    // Only the first turn carries the caller's per-turn prompt-assembly hints
    // (`stdinContext` / `noteIds` / `contextReferences`) — a `QueuedMessage`
    // has none. Attachment blocks (`imageBlocks` / `fileBlocks`) are captured
    // at enqueue time and DO ride along on drain, so a queued turn reaches the
    // agent with the same ACP content blocks as if it had run inline.
    let mut options = initial_options;
    // Whether the CURRENT turn's user row durably reached the transcript
    // (STAB-51). Terminal spawn/turn failures thread it into the requeue so
    // a failed pre-turn persist is re-attempted by the `agent.retry` drain.
    let mut user_persisted = initial_persisted;
    'outer: loop {
        match retry_spawn(&mgr, &agent_id, &workspace_id).await {
            Ok(acp_session_id) => {
                // Clear any persisted completion report at the start of this turn
                // (including queue-drained turns). Skip the store write when no
                // report is set; the `agent:idle` wake for a prior turn that set a
                // report still includes it because the clear runs at the NEXT turn's
                // begin (after the `agent:idle` emit at the prior turn's end).
                mgr.clear_completion_report_if_present(&agent_id, &workspace_id)
                    .await;
                let prompt = mgr
                    .build_turn_prompt(&agent_id, &workspace_id, &content, &options)
                    .await;
                if let Err(e) = mgr
                    .run_turn(&agent_id, &workspace_id, &acp_session_id, prompt)
                    .await
                {
                    if is_benign_turn_error(&e) {
                        // Concurrent stop/cancel won the turn — not a failure.
                        // Keep draining: any queued message re-spawns lazily.
                        tracing::warn!(agent = %agent_id, error = %e, "agent turn ended (benign)");
                    } else {
                        // STAB-53: on a child-death failure, point at the
                        // captured stderr file so the crash is diagnosable.
                        match stderr_capture_hint(&mgr, &agent_id, &e) {
                            Some(log) => tracing::warn!(
                                agent = %agent_id,
                                error = %e,
                                "agent turn failed terminally (agent stderr captured at {})",
                                log.display()
                            ),
                            None => {
                                tracing::warn!(agent = %agent_id, error = %e, "agent turn failed terminally")
                            }
                        }
                        handle_terminal_turn_failure(
                            &mgr,
                            &agent_id,
                            &workspace_id,
                            &content,
                            &options,
                            user_persisted,
                            &e,
                        )
                        .await;
                        // Release the in-flight slot without overwriting the
                        // Error status just persisted, so `agent.retry` (or a
                        // future message) can restart the worker.
                        mgr.release_in_flight_slot(&agent_id).await;
                        break 'outer;
                    }
                }
            }
            Err(e) => {
                match stderr_capture_hint(&mgr, &agent_id, &e) {
                    Some(log) => tracing::warn!(
                        agent = %agent_id,
                        error = %e,
                        "agent spawn failed after all retries (agent stderr captured at {})",
                        log.display()
                    ),
                    None => {
                        tracing::warn!(agent = %agent_id, error = %e, "agent spawn failed after all retries")
                    }
                }
                handle_terminal_spawn_failure(
                    &mgr,
                    &agent_id,
                    &workspace_id,
                    &content,
                    &options,
                    user_persisted,
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
            // A terminal-failure requeue whose user row already reached the
            // transcript before its failed turn began must not duplicate the
            // row on retry; otherwise persist now and remember the outcome.
            user_persisted = if next.persisted {
                true
            } else {
                persist_user(
                    &mgr,
                    &agent_id,
                    &workspace_id,
                    &next.content,
                    next_image_blocks.as_ref(),
                    next_file_blocks.as_ref(),
                    next.message_metadata.as_ref(),
                )
                .await
            };
            content = next.content;
            options = TurnOptions {
                image_blocks: next_image_blocks,
                file_blocks: next_file_blocks,
                message_metadata: next.message_metadata.clone(),
                ..TurnOptions::default()
            };
            // Fail closed (#547): a persist failure that survived the bounded
            // retry parks the agent in Error instead of running the turn.
            if !user_persisted {
                handle_drain_persist_failure(&mgr, &agent_id, &workspace_id, &content, &options)
                    .await;
                mgr.release_in_flight_slot(&agent_id).await;
                break 'outer;
            }
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
            user_persisted = if next.persisted {
                true
            } else {
                persist_user(
                    &mgr,
                    &agent_id,
                    &workspace_id,
                    &next.content,
                    next_image_blocks.as_ref(),
                    next_file_blocks.as_ref(),
                    next.message_metadata.as_ref(),
                )
                .await
            };
            content = next.content;
            options = TurnOptions {
                image_blocks: next_image_blocks,
                file_blocks: next_file_blocks,
                message_metadata: next.message_metadata.clone(),
                ..TurnOptions::default()
            };
            // Fail closed (#547): same contract as the pre-release drain arm.
            if !user_persisted {
                handle_drain_persist_failure(&mgr, &agent_id, &workspace_id, &content, &options)
                    .await;
                mgr.release_in_flight_slot(&agent_id).await;
                break 'outer;
            }
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
/// reflect the dequeued message (STAB-4 fix). FE-supplied attachments captured at
/// enqueue time ride along so the persisted row carries them (STAB-133).
/// `message_metadata` is the queue entry's captured `messageMetadata` (e.g. a
/// parent wake's `event_notification` payload). It is written in BOTH placements
/// the two direct-delivery shapes use — folded onto the text block as
/// `messageMetadata` (parity with `deliver_wake_message`'s in-block tag) AND on
/// the row-level `metadata` column (parity with the direct `agent.sendMessage`
/// persist) — so transcript consumers find the tag regardless of which field
/// they read. The client-identity `userAppMessageId` key is excluded from the
/// in-block copy (it stays row-level only): the block embed exists for
/// attribution tags that history replay should surface, and a queued send's
/// content block should not diverge from its direct-send counterpart just
/// because a dedup id rode along. Best-effort; a store or publish error is
/// logged and the turn still proceeds.
///
/// Returns `true` when the user row was durably appended to the transcript,
/// `false` when the store append failed for every bounded retry attempt
/// (STAB-51 / #547). A transient store blip (busy database, lock contention)
/// self-heals inside the bounded retry (delays from
/// [`persist_retry_backoff_ms`]); on exhaustion the drain call sites fail
/// closed — they do NOT start the turn, park the agent in `Error`, and
/// requeue with `persisted: false` so `agent.retry` re-attempts the append.
async fn persist_user(
    mgr: &AgentManager,
    agent_id: &AgentId,
    workspace_id: &WorkspaceId,
    content: &str,
    image_blocks: Option<&Value>,
    file_blocks: Option<&Value>,
    message_metadata: Option<&Value>,
) -> bool {
    let created_at = now_iso();
    let mut blocks = user_message_blocks(content, image_blocks, file_blocks);
    let block_md = message_metadata.and_then(|md| match md {
        Value::Object(m) => {
            let mut m = m.clone();
            m.remove(intent_core::USER_APP_MESSAGE_ID_KEY);
            (!m.is_empty()).then_some(Value::Object(m))
        }
        other => Some(other.clone()),
    });
    if let Some(md) = block_md {
        if let Some(text_block) = blocks.get_mut(0).and_then(Value::as_object_mut) {
            text_block.insert("messageMetadata".into(), md);
        }
    }
    // Bounded retry (#547): initial attempt + one retry per backoff delay.
    let backoff = persist_retry_backoff_ms();
    let mut attempt = 0usize;
    let message = loop {
        match mgr
            .services
            .store
            .append_agent_message_with_metadata(
                agent_id,
                "user",
                &blocks,
                message_metadata,
                &created_at,
            )
            .await
        {
            Ok(message) => break message,
            Err(e) => {
                let Some(&delay_ms) = backoff.get(attempt) else {
                    tracing::warn!(
                        agent = %agent_id,
                        error = %e,
                        attempts = attempt + 1,
                        "failed to persist queued user message (all retries exhausted)"
                    );
                    return false;
                };
                attempt += 1;
                tracing::warn!(
                    agent = %agent_id,
                    error = %e,
                    attempt,
                    "failed to persist queued user message; retrying in {delay_ms}ms"
                );
                tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
            }
        }
    };
    // Refresh agent_session.updated_at so the FE agent-card timestamp
    // reflects message activity, not just status transitions (STAB-19).
    if let Err(e) = mgr
        .services
        .store
        .refresh_agent_session_timestamp(workspace_id, agent_id, &created_at)
        .await
    {
        tracing::warn!(agent = %agent_id, error = %e, "refresh_agent_session_timestamp failed");
    } else {
        // Schedule debounced lastActivity event (§10.1).
        mgr.services
            .schedule_last_activity_event(workspace_id.clone());
    }
    mgr.services
        .publish_agent_mutation_event(
            workspace_id,
            agent_id,
            intent_core::events::AGENT_MESSAGE,
            crate::agent_ops::agent_message_event_payload(agent_id, &message),
        )
        .await;
    true
}

/// Max number of spawn attempts (includes the initial attempt).
const MAX_SPAWN_ATTEMPTS: u32 = 3;
/// Default backoff delays between retry attempts (in milliseconds).
const DEFAULT_RETRY_BACKOFF_MS: &[u64] = &[2000, 5000];
/// Default backoff delays between pre-turn persist retry attempts (#547).
/// Short: the append is a local SQLite write, so a transient failure (busy
/// database, lock contention) clears quickly or not at all.
const DEFAULT_PERSIST_RETRY_BACKOFF_MS: &[u64] = &[250, 1000];

/// Parse comma-separated millisecond delays from `var`, falling back to
/// `default` when unset, empty, or malformed. Primarily for tests/CI.
fn env_backoff_ms(var: &str, default: &[u64]) -> Vec<u64> {
    if let Ok(val) = std::env::var(var) {
        let mut delays = Vec::new();
        for part in val.split(',') {
            if let Ok(ms) = part.trim().parse::<u64>() {
                delays.push(ms);
            } else {
                // Invalid format, fall back to default
                return default.to_vec();
            }
        }
        if !delays.is_empty() {
            return delays;
        }
    }
    default.to_vec()
}

/// Get spawn retry backoff delays, overridable via
/// INTENTD_SPAWN_RETRY_BACKOFF_MS (comma-separated milliseconds, e.g.
/// "100,200"). Primarily for tests/CI.
fn retry_backoff_ms() -> Vec<u64> {
    env_backoff_ms("INTENTD_SPAWN_RETRY_BACKOFF_MS", DEFAULT_RETRY_BACKOFF_MS)
}

/// Get persist retry backoff delays (#547), overridable via
/// INTENTD_PERSIST_RETRY_BACKOFF_MS with the same format.
fn persist_retry_backoff_ms() -> Vec<u64> {
    env_backoff_ms(
        "INTENTD_PERSIST_RETRY_BACKOFF_MS",
        DEFAULT_PERSIST_RETRY_BACKOFF_MS,
    )
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

/// Publish the terminal `agent:failed` + `agent:stream:end` event pair for a
/// failure the streaming path did NOT already surface. The error message
/// deliberately excludes recent stderr to avoid leaking secrets (API keys,
/// tokens, file paths) to subscribed clients; stderr stays server-side in logs.
async fn publish_terminal_failure_events(
    mgr: &AgentManager,
    agent_id: &AgentId,
    workspace_id: &WorkspaceId,
    error_msg: &str,
) {
    use intent_core::events::{AGENT_FAILED, AGENT_STREAM_END};

    mgr.services
        .publish_agent_event(
            workspace_id,
            agent_id,
            AGENT_FAILED,
            json!({ "agentId": agent_id.0, "error": error_msg }),
        )
        .await;
    mgr.services
        .publish_agent_event(
            workspace_id,
            agent_id,
            AGENT_STREAM_END,
            json!({ "agentId": agent_id.0 }),
        )
        .await;
}

/// Persist `AgentStatus::Error` (emitting `agent:status-changed`) and requeue
/// the failed message to the front of the queue so `agent.retry` — or a future
/// `agent.sendMessage` — can redrive it. Shared by the terminal spawn- and
/// turn-failure paths. The `error_text` argument is persisted into
/// `agent_session.stop_reason` and included in the `agent:status-changed` event's
/// `stopReason` field (durable-before-observable). `persisted` reports whether
/// the failed turn's user row durably reached the transcript (STAB-51).
async fn persist_error_and_requeue(
    mgr: &AgentManager,
    agent_id: &AgentId,
    workspace_id: &WorkspaceId,
    content: &str,
    options: &TurnOptions,
    persisted: bool,
    error_text: &str,
) {
    // Persist agent status as Error WITH stop_reason and emit agent:status-changed.
    // Durable-before-observable: the store write completes BEFORE the event is published,
    // so subscribers see the canonical field immediately via agent.get/getSession.
    let ts = now_iso();
    if let Err(e) = mgr
        .services
        .store
        .set_agent_session_status(
            workspace_id,
            agent_id,
            AgentStatus::Error,
            false,
            &ts,
            Some(Some(error_text.to_string())),
        )
        .await
    {
        tracing::warn!(agent = %agent_id, error = %e, "failed to persist error status + stop_reason");
    } else {
        // Emit agent:status-changed with stopReason so live subscribers get the canonical field.
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
                "stopReason": error_text,
            }),
        };
        crate::publish_event(&mgr.services.event_bus, event).await;
    }

    // Requeue the failed message to the front of the queue. `persisted`
    // carries the CONFIRMED durability of the user row (STAB-51): `true` only
    // when the pre-turn transcript append succeeded, so the retry drain skips
    // the duplicate append; `false` when it failed, so the retry drain
    // re-attempts it. `requeued_after_failure` is set so the wire emits
    // `requeuedAfterFailure: true` (STAB-112).
    let queued = crate::agent_ops::QueuedMessage {
        id: new_message_id(),
        content: content.to_string(),
        image_blocks: options.image_blocks.clone(),
        file_blocks: options.file_blocks.clone(),
        queued_at: now_iso(),
        editing: false,
        persisted,
        requeued_after_failure: true,
        message_metadata: options.message_metadata.clone(),
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

/// Handle terminal spawn failure after all retries are exhausted. Publishes
/// terminal `agent:failed` and `agent:stream:end` events, persists the agent
/// status as `Error` with the error text into `stop_reason`, requeues the
/// failed message to the front of the queue, and stops draining further messages.
async fn handle_terminal_spawn_failure(
    mgr: &AgentManager,
    agent_id: &AgentId,
    workspace_id: &WorkspaceId,
    content: &str,
    options: &TurnOptions,
    persisted: bool,
    error: &Error,
) {
    let error_text = error.to_string();
    publish_terminal_failure_events(mgr, agent_id, workspace_id, &error_text).await;
    persist_error_and_requeue(
        mgr,
        agent_id,
        workspace_id,
        content,
        options,
        persisted,
        &error_text,
    )
    .await;
}

/// Handle a pre-turn persist failure after `persist_user` exhausted its
/// bounded retry (#547, fail-closed drain). The turn was NOT started, so —
/// unlike the spawn/turn failure handlers — there is no child to tear down
/// and no partial stream to close; the terminal event pair + Error park +
/// front requeue (`persisted: false`) make the failure observable and
/// redrivable via `agent.retry`. Callers release the in-flight slot after.
async fn handle_drain_persist_failure(
    mgr: &AgentManager,
    agent_id: &AgentId,
    workspace_id: &WorkspaceId,
    content: &str,
    options: &TurnOptions,
) {
    let error_text = "failed to persist user message to transcript; turn not started".to_string();
    tracing::warn!(agent = %agent_id, "queue drain failed closed: {error_text}");
    publish_terminal_failure_events(mgr, agent_id, workspace_id, &error_text).await;
    persist_error_and_requeue(
        mgr,
        agent_id,
        workspace_id,
        content,
        options,
        false,
        &error_text,
    )
    .await;
}

/// Prefix `run_prompt_turn` wraps every post-prompt failure with (see
/// `agent_session.rs`): `Error::Internal(format!("session/prompt failed: {e}"))`.
const PROMPT_FAILED_PREFIX: &str = "session/prompt failed:";

/// The ACP cancellation surface inside the [`PROMPT_FAILED_PREFIX`] wrapper,
/// if any. The structured `AcpError` is flattened to a string at the wrap
/// boundary, so this recovers the cancellation signal from the two known
/// shapes: the JSON-RPC `-32800` request-cancelled code (rendered as
/// `JSON-RPC error -32800: …` by `intent-acp`'s `JsonRpcError` Display), or a
/// provider resolving the prompt with a "cancelled" error message.
///
/// RPC-shaped errors anchor on the code alone (monorepo#518): Display appends
/// the provider-controlled `error.data` payload after the message, and the
/// two are indistinguishable once flattened — a terminal error whose data
/// merely mentions "cancelled" must not be misclassified as benign. The ACP
/// spec's only sanctioned cancel-error shape is code `-32800` (the message is
/// free text there too). The "cancelled" substring heuristic remains for
/// non-RPC renderings, which carry no data suffix.
fn prompt_cancellation_error(err: &Error) -> bool {
    let Error::Internal(msg) = err else {
        return false;
    };
    let Some(inner) = msg.strip_prefix(PROMPT_FAILED_PREFIX) else {
        return false;
    };
    if let Some(rest) = inner.trim_start().strip_prefix("JSON-RPC error ") {
        let code = rest.split(':').next().unwrap_or("").trim();
        return code == "-32800";
    }
    inner.to_ascii_lowercase().contains("cancelled")
}

/// Classify a [`AgentManager::run_turn`] error as benign (an expected outcome
/// of a concurrent stop/cancel — NOT a failure to surface) vs terminal.
///
/// Benign:
/// - `NotFound` — the agent handle disappeared between `ensure_started` and
///   `run_turn`, i.e. a concurrent `agent.stop`/teardown won the race.
/// - a cancellation error inside the `session/prompt failed:` wrapper — the
///   provider resolved the in-flight `session/prompt` with the JSON-RPC
///   `-32800` request-cancelled code (or a "cancelled" message) instead of
///   `StopReason::Cancelled` after a `session/cancel`. Errors that merely
///   mention "cancelled" OUTSIDE that wrapper stay terminal.
///
/// Everything else (transport closed, agent stdout closed, response channel
/// dropped, prompt timeout, provider JSON-RPC errors, store append failures)
/// is terminal: the turn died mid-flight and must be surfaced (STAB-6
/// semantics). Deliberately errs on the side of terminal — a false "failed"
/// surface with a Retry button beats a silently dropped message.
fn is_benign_turn_error(err: &Error) -> bool {
    if matches!(err, Error::NotFound(_)) {
        return true;
    }
    prompt_cancellation_error(err)
}

/// STAB-53: when a terminal failure means the child died mid-turn ("agent
/// stdout closed") and stderr capture is enabled, return the capture directory
/// for `agent_id` so the WARN line can point at the child's last words.
/// Matches on the structured `Error::Internal` payload — the transport's
/// child-death error is always wrapped there (handshake/prompt failures) —
/// avoiding a Display allocation per check.
fn stderr_capture_hint(
    mgr: &AgentManager,
    agent_id: &AgentId,
    err: &Error,
) -> Option<std::path::PathBuf> {
    if !matches!(err, Error::Internal(msg) if msg.contains("agent stdout closed")) {
        return None;
    }
    mgr.agent_stderr_log_dir(agent_id)
}

/// Whether `run_prompt_turn` already emitted the terminal `agent:failed` +
/// `agent:stream:end` pair for this error. Its post-prompt failure path wraps
/// every prompt error as `Internal("session/prompt failed: …")` AFTER emitting
/// both events; errors WITHOUT that prefix (e.g. the transcript-append store
/// error, which propagates via `?` before the emits) still need the events.
/// Prefix-anchored on the structured `Error::Internal` payload so an
/// unrelated error that merely mentions the phrase mid-string cannot
/// suppress the terminal events.
fn turn_failure_events_already_emitted(err: &Error) -> bool {
    matches!(err, Error::Internal(msg) if msg.starts_with(PROMPT_FAILED_PREFIX))
}

/// Handle a terminal mid-turn failure (`run_turn` error that is not a benign
/// cancel): tear down the dead child, ensure the terminal `agent:failed` +
/// `agent:stream:end` pair reached the bus, persist `AgentStatus::Error` with
/// the error text into `stop_reason`, and requeue the message for `agent.retry`.
/// Mirrors [`handle_terminal_spawn_failure`] but does NOT auto-retry inline —
/// the prompt may have been partially processed, so redriving it is a user
/// decision (the STAB-6 Retry surface).
async fn handle_terminal_turn_failure(
    mgr: &AgentManager,
    agent_id: &AgentId,
    workspace_id: &WorkspaceId,
    content: &str,
    options: &TurnOptions,
    persisted: bool,
    error: &Error,
) {
    // Tear down the (likely dead) child so the retry path spawns fresh. Safe
    // from within the worker: only kills child/handle, no worker/busy touch.
    mgr.kill_child_only(agent_id).await;

    let error_text = error.to_string();
    if !turn_failure_events_already_emitted(error) {
        publish_terminal_failure_events(mgr, agent_id, workspace_id, &error_text).await;
    }
    persist_error_and_requeue(
        mgr,
        agent_id,
        workspace_id,
        content,
        options,
        persisted,
        &error_text,
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
            cow_supported: None,
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
            stop_reason: None,
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

    /// `stop()` clears `recreated`/`prepend_pending` (stale-flag hygiene) but
    /// MUST keep `force_recreate` armed: an `agent.editAndRegenerate`
    /// truncation is already persisted, so the stale provider session must
    /// never be resumed no matter how many stops intervene before the next
    /// turn.
    #[tokio::test]
    async fn stop_preserves_force_recreate_but_clears_recreated() {
        let (mgr, agent_id) = manager_with(None, None).await;
        mgr.force_recreate.lock().unwrap().insert(agent_id.clone());
        mgr.recreated.lock().unwrap().insert(agent_id.clone());
        mgr.stop(&agent_id).await;
        assert!(
            mgr.force_recreate.lock().unwrap().contains(&agent_id),
            "force_recreate survives stop()"
        );
        assert!(
            !mgr.recreated.lock().unwrap().contains(&agent_id),
            "recreated cleared by stop()"
        );
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

    // ---- FirstTurnPrepend fallback (§18.1) ----

    /// Persist an assembled system prompt on the seeded agent session (the
    /// spawn path does this in `create_agent`; unit tests seed it directly).
    async fn set_system_prompt(mgr: &AgentManager, agent_id: &AgentId, prompt: &str) {
        let mut s = mgr
            .services
            .store
            .get_agent_session(agent_id)
            .await
            .expect("session");
        let ws = s.workspace_id.clone();
        s.system_prompt = Some(prompt.to_string());
        mgr.services
            .store
            .update_agent_session(&ws, &s)
            .await
            .expect("persist system_prompt");
    }

    #[tokio::test]
    async fn first_turn_prepend_fires_once_per_fresh_session() {
        let (mgr, agent_id) = manager_with(None, None).await;
        set_system_prompt(&mgr, &agent_id, "You are helpful.").await;
        let mock = intent_providers::find_provider("mock").unwrap();
        mgr.arm_first_turn_prepend(&agent_id, mock);
        // First turn carries the <system>-wrapped assembled prompt first.
        let prompt = mgr
            .build_turn_prompt(
                &agent_id,
                &WorkspaceId::from("ws-1"),
                "first message",
                &TurnOptions::default(),
            )
            .await;
        let text = prompt_text(&prompt);
        assert!(
            text.starts_with("<system>\nYou are helpful.\n</system>\n\n"),
            "missing first-turn prepend: {text:?}"
        );
        assert!(text.ends_with("first message"));
        // Second turn on the SAME session must not repeat it.
        let prompt = mgr
            .build_turn_prompt(
                &agent_id,
                &WorkspaceId::from("ws-1"),
                "second message",
                &TurnOptions::default(),
            )
            .await;
        let text = prompt_text(&prompt);
        assert!(
            !text.contains("<system>\nYou are helpful."),
            "prepend repeated on second turn: {text:?}"
        );
    }

    #[tokio::test]
    async fn first_turn_prepend_refires_after_recreate() {
        let (mgr, agent_id) = manager_with(None, None).await;
        set_system_prompt(&mgr, &agent_id, "SP body").await;
        let mock = intent_providers::find_provider("mock").unwrap();
        // Fresh session → fires; consumed by the first turn.
        mgr.arm_first_turn_prepend(&agent_id, mock);
        let first = prompt_text(
            &mgr.build_turn_prompt(
                &agent_id,
                &WorkspaceId::from("ws-1"),
                "one",
                &TurnOptions::default(),
            )
            .await,
        );
        assert!(first.starts_with("<system>\nSP body\n</system>"));
        // Recreate path re-arms (start_session recreate branch) → fires again.
        mgr.arm_first_turn_prepend(&agent_id, mock);
        let again = prompt_text(
            &mgr.build_turn_prompt(
                &agent_id,
                &WorkspaceId::from("ws-1"),
                "two",
                &TurnOptions::default(),
            )
            .await,
        );
        assert!(
            again.starts_with("<system>\nSP body\n</system>"),
            "prepend must re-fire after session recreation: {again:?}"
        );
    }

    #[tokio::test]
    async fn first_turn_prepend_not_armed_for_native_mechanism_providers() {
        let (mgr, agent_id) = manager_with(None, None).await;
        set_system_prompt(&mgr, &agent_id, "native SP").await;
        // Native-mechanism providers (rules file / _meta / env) never arm the
        // fallback — no double injection.
        for id in ["auggie", "claude-code", "opencode", "droid"] {
            let provider = intent_providers::find_provider(id).unwrap();
            mgr.arm_first_turn_prepend(&agent_id, provider);
        }
        assert!(
            !mgr.prepend_pending.lock().unwrap().contains(&agent_id),
            "native-mechanism providers must not arm the prepend fallback"
        );
        let text = prompt_text(
            &mgr.build_turn_prompt(
                &agent_id,
                &WorkspaceId::from("ws-1"),
                "hello",
                &TurnOptions::default(),
            )
            .await,
        );
        assert_eq!(text, "hello");
    }

    #[test]
    fn set_model_target_gates_provider_sentinel_and_compound_prefix() {
        let grok = intent_providers::find_provider("grok").unwrap();
        let auggie = intent_providers::find_provider("auggie").unwrap();

        // Providers without supports_set_model never produce a target.
        assert_eq!(
            AgentManager::set_model_target(auggie, Some("opus4.7")),
            None
        );
        // Absent / empty / sentinel models are no-ops.
        assert_eq!(AgentManager::set_model_target(grok, None), None);
        assert_eq!(AgentManager::set_model_target(grok, Some("")), None);
        assert_eq!(AgentManager::set_model_target(grok, Some("default")), None);
        assert_eq!(
            AgentManager::set_model_target(grok, Some("grok:default")),
            None
        );
        // Bare ids are provider-local.
        assert_eq!(
            AgentManager::set_model_target(grok, Some("grok-4.5")),
            Some("grok-4.5")
        );
        // Matching compound prefix strips to the bare id.
        assert_eq!(
            AgentManager::set_model_target(grok, Some("grok:grok-4.5")),
            Some("grok-4.5")
        );
        // A compound id for a DIFFERENT provider (stale pre-spawn provider
        // switch) must not be sent to grok.
        assert_eq!(
            AgentManager::set_model_target(grok, Some("opencode:kimi-k3")),
            None
        );
    }

    #[test]
    fn config_option_model_target_gates_provider_sentinel_and_compound_prefix() {
        let claude = intent_providers::find_provider("claude-code").unwrap();
        let grok = intent_providers::find_provider("grok").unwrap();

        // Providers without supports_config_option_model never produce a
        // target (grok uses session/set_model instead) — and vice versa,
        // claude-code never produces a session/set_model target.
        assert_eq!(
            AgentManager::config_option_model_target(grok, Some("sonnet")),
            None
        );
        assert_eq!(AgentManager::set_model_target(claude, Some("sonnet")), None);
        // Absent / empty / sentinel models are no-ops.
        assert_eq!(AgentManager::config_option_model_target(claude, None), None);
        assert_eq!(
            AgentManager::config_option_model_target(claude, Some("")),
            None
        );
        assert_eq!(
            AgentManager::config_option_model_target(claude, Some("default")),
            None
        );
        assert_eq!(
            AgentManager::config_option_model_target(claude, Some("claude-code:default")),
            None
        );
        // Bare ids are provider-local.
        assert_eq!(
            AgentManager::config_option_model_target(claude, Some("sonnet")),
            Some("sonnet")
        );
        // Matching compound prefix strips to the bare id.
        assert_eq!(
            AgentManager::config_option_model_target(claude, Some("claude-code:opus")),
            Some("opus")
        );
        // A compound id for a DIFFERENT provider (stale pre-spawn provider
        // switch) must not be sent to claude-code.
        assert_eq!(
            AgentManager::config_option_model_target(claude, Some("grok:grok-4.5")),
            None
        );
    }

    #[tokio::test]
    async fn first_turn_prepend_skipped_when_no_system_prompt() {
        // Armed but the session has no persisted system_prompt (or blank) —
        // no stray empty <system> block; the flag is still consumed.
        let (mgr, agent_id) = manager_with(None, None).await;
        let mock = intent_providers::find_provider("mock").unwrap();
        mgr.arm_first_turn_prepend(&agent_id, mock);
        let text = prompt_text(
            &mgr.build_turn_prompt(
                &agent_id,
                &WorkspaceId::from("ws-1"),
                "no sp",
                &TurnOptions::default(),
            )
            .await,
        );
        assert_eq!(text, "no sp");
        assert!(!mgr.prepend_pending.lock().unwrap().contains(&agent_id));
    }

    #[tokio::test]
    async fn first_turn_prepend_precedes_context_and_reminder() {
        // Ordering: the FirstTurnPrepend <system> block is OUTERMOST — before
        // the stdinContext `Context:` block, role reminder, and body.
        let dir = write_specialist(
            "implementor",
            "---\nname: \"Implementor\"\ndescription: \"d\"\nroleReminder: \"Stay in scope.\"\n---\n\nbody",
        );
        let (mgr, agent_id) = manager_with(Some("implementor"), Some(dir)).await;
        set_system_prompt(&mgr, &agent_id, "SP").await;
        let mock = intent_providers::find_provider("mock").unwrap();
        mgr.arm_first_turn_prepend(&agent_id, mock);
        let opts = TurnOptions {
            stdin_context: Some("ctx".to_string()),
            ..TurnOptions::default()
        };
        let text = prompt_text(
            &mgr.build_turn_prompt(&agent_id, &WorkspaceId::from("ws-1"), "do it", &opts)
                .await,
        );
        assert!(
            text.starts_with("<system>\nSP\n</system>\n\nContext:\nctx\n\n---\n\n[Role Reminder:"),
            "unexpected ordering: {text:?}"
        );
    }
}

#[cfg(test)]
mod rebuild_spawn_opts_tests {
    //! Regression tests for the `create_agent` [`SpawnOptions`] reconstruction:
    //! it must preserve the npx fallback pair, otherwise providers without a
    //! local binary (codex fallback / claude-code npx-only) spawn the bare
    //! provider command and fail with ENOENT.

    use super::*;

    #[test]
    fn rebuild_preserves_npx_fallback_and_targets_npx() {
        let provider = intent_providers::find_provider("codex").unwrap();
        let npx_path = PathBuf::from("/usr/local/bin/npx");
        let mut opts = SpawnOptions::new(provider);
        opts.npx_fallback_binary = Some(&npx_path);
        opts.npx_fallback_package = provider.fallback_npx_package;

        let rebuilt = rebuild_spawn_opts(&opts, Some("/tmp/rules.md"), Some("/tmp/mcp.json"), None);
        assert_eq!(rebuilt.npx_fallback_binary, Some(npx_path.as_path()));
        assert_eq!(rebuilt.npx_fallback_package, provider.fallback_npx_package);

        // Through build_command/build_args: the rebuilt opts must spawn npx
        // with `-y <package>`, not the bare `codex-acp` command.
        let cmd = intent_acp::spawn::build_command(&rebuilt);
        assert_eq!(cmd.as_std().get_program(), npx_path.as_os_str());
        let args = intent_acp::spawn::build_args(&rebuilt);
        assert_eq!(args[0], "-y");
        assert_eq!(
            args[1],
            provider
                .fallback_npx_package
                .expect("codex has npx fallback")
        );
    }

    #[test]
    fn rebuild_injects_generated_paths_and_keeps_caller_fields() {
        let provider = intent_providers::find_provider("codex").unwrap();
        let binary = PathBuf::from("/custom/codex-acp");
        let cwd = PathBuf::from("/work/dir");
        let mut opts = SpawnOptions::new(provider);
        opts.model = Some("gpt-5");
        opts.cwd = Some(&cwd);
        opts.quiet = true;
        opts.provider_binary = Some(&binary);
        opts.extra_env = BTreeMap::from([("K".to_string(), "V".to_string())]);
        opts.tools_to_remove = vec!["shell"];

        let rebuilt = rebuild_spawn_opts(&opts, Some("/tmp/rules.md"), Some("/tmp/mcp.json"), None);
        assert_eq!(rebuilt.model, Some("gpt-5"));
        assert_eq!(rebuilt.cwd, Some(cwd.as_path()));
        assert!(rebuilt.quiet);
        assert_eq!(rebuilt.provider_binary, Some(binary.as_path()));
        assert_eq!(rebuilt.extra_env, opts.extra_env);
        assert_eq!(rebuilt.tools_to_remove, vec!["shell"]);
        assert_eq!(rebuilt.rules_file, Some("/tmp/rules.md"));
        assert_eq!(rebuilt.mcp_config_file, Some("/tmp/mcp.json"));
        assert_eq!(rebuilt.env_mcp_config, None);
    }

    #[test]
    fn rebuild_prefers_caller_rules_file_over_generated() {
        let provider = intent_providers::find_provider("codex").unwrap();
        let mut opts = SpawnOptions::new(provider);
        opts.rules_file = Some("/caller/rules.md");
        let rebuilt = rebuild_spawn_opts(&opts, Some("/tmp/generated.md"), None, None);
        assert_eq!(rebuilt.rules_file, Some("/caller/rules.md"));
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
mod turn_failure_tests {
    //! Unit tests for the mid-turn failure classifier (benign cancel vs
    //! terminal failure) and the events-already-emitted marker.

    use super::*;

    #[test]
    fn not_found_is_benign() {
        // Handle disappeared mid-worker → a concurrent stop/teardown won.
        let err = Error::NotFound("agent agent-x".to_string());
        assert!(is_benign_turn_error(&err));
    }

    #[test]
    fn cancelled_rpc_error_is_benign() {
        let err = Error::Internal(
            "session/prompt failed: JSON-RPC error -32800: Request cancelled".to_string(),
        );
        assert!(is_benign_turn_error(&err));
    }

    #[test]
    fn cancellation_code_without_message_is_benign() {
        // Some providers omit a human-readable message; the -32800 code alone
        // is the cancellation signal.
        let err = Error::Internal("session/prompt failed: JSON-RPC error -32800: ".to_string());
        assert!(is_benign_turn_error(&err));
    }

    #[test]
    fn cancelled_rpc_error_with_data_detail_is_benign() {
        // `JsonRpcError` Display now appends the `data` payload (strings raw,
        // objects as compact JSON); the richer message must not break the
        // -32800 benign-cancel classification.
        let err = Error::Internal(
            "session/prompt failed: JSON-RPC error -32800: Request cancelled: turn aborted"
                .to_string(),
        );
        assert!(is_benign_turn_error(&err));
        let err = Error::Internal(
            r#"session/prompt failed: JSON-RPC error -32800: Request cancelled: {"reason":"turn aborted"}"#
                .to_string(),
        );
        assert!(is_benign_turn_error(&err));
    }

    #[test]
    fn terminal_rpc_error_with_cancelled_in_data_is_terminal() {
        // monorepo#518 inverse case: a terminal provider error whose appended
        // `data` detail merely mentions "cancelled" must NOT be misclassified
        // as a benign cancel — it needs the full agent:failed / requeue /
        // Retry surface.
        let err = Error::Internal(
            r#"session/prompt failed: JSON-RPC error -32603: Internal error: {"message":"stream cancelled by backend","codex_error_info":"other"}"#
                .to_string(),
        );
        assert!(!is_benign_turn_error(&err));
        let err = Error::Internal(
            "session/prompt failed: JSON-RPC error -32603: Internal error: request cancelled upstream"
                .to_string(),
        );
        assert!(!is_benign_turn_error(&err));
    }

    #[test]
    fn rpc_error_with_cancelled_message_but_non_cancel_code_is_terminal() {
        // RPC-shaped errors anchor on the -32800 code alone: past the code,
        // the rendered message/data suffix is provider-controlled free text
        // (message and data are indistinguishable once flattened), so a
        // "cancelled" mention there errs toward terminal by design.
        let err = Error::Internal(
            "session/prompt failed: JSON-RPC error -32603: task cancelled".to_string(),
        );
        assert!(!is_benign_turn_error(&err));
    }

    #[test]
    fn non_rpc_cancelled_message_is_benign() {
        // The second known cancel shape: a provider resolving the prompt with
        // a plain "cancelled" error message. Non-RPC renderings carry no
        // provider-controlled data suffix, so the substring match is safe.
        let err = Error::Internal("session/prompt failed: prompt was cancelled".to_string());
        assert!(is_benign_turn_error(&err));
    }

    #[test]
    fn cancelled_outside_prompt_wrapper_is_terminal() {
        // "cancelled" in an unrelated error (no session/prompt wrapper) must
        // not be mistaken for a benign turn cancel.
        let err = Error::Internal("store: write cancelled by shutdown".to_string());
        assert!(!is_benign_turn_error(&err));
    }

    #[test]
    fn transport_closed_is_terminal() {
        let err = Error::Internal(
            "session/prompt failed: transport closed: writer task closed".to_string(),
        );
        assert!(!is_benign_turn_error(&err));
    }

    #[test]
    fn stdout_closed_is_terminal() {
        let err = Error::Internal(
            "session/prompt failed: JSON-RPC error 0: agent stdout closed".to_string(),
        );
        assert!(!is_benign_turn_error(&err));
    }

    #[test]
    fn prompt_timeout_is_terminal() {
        let err = Error::Internal(
            "session/prompt failed: request `session/prompt` timed out".to_string(),
        );
        assert!(!is_benign_turn_error(&err));
    }

    #[test]
    fn store_append_failure_is_terminal() {
        let err = Error::Internal("store: database is locked".to_string());
        assert!(!is_benign_turn_error(&err));
    }

    #[test]
    fn prompt_failed_marker_means_events_already_emitted() {
        // run_prompt_turn emits agent:failed + stream:end BEFORE wrapping the
        // error with the "session/prompt failed" prefix.
        let err = Error::Internal(
            "session/prompt failed: transport closed: writer task closed".to_string(),
        );
        assert!(turn_failure_events_already_emitted(&err));
    }

    #[test]
    fn store_error_needs_events_emitted() {
        // The transcript-append store error propagates via `?` before
        // run_prompt_turn reaches its emit path.
        let err = Error::Internal("store: database is locked".to_string());
        assert!(!turn_failure_events_already_emitted(&err));
    }

    #[test]
    fn mid_string_marker_needs_events_emitted() {
        // Prefix-anchored: an unrelated error merely mentioning the phrase
        // mid-string must not suppress the terminal event pair.
        let err =
            Error::Internal("store: could not log that session/prompt failed earlier".to_string());
        assert!(!turn_failure_events_already_emitted(&err));
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
            cow_supported: None,
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
            stop_reason: None,
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
    async fn retry_from_error_status_with_empty_queue_clears_to_idle() {
        let agent_id = AgentId::from("agent-1");
        let ws = WorkspaceId::from("ws-1");
        let mgr = manager_with_session(&agent_id, &ws, AgentStatus::Error).await;

        let result = mgr
            .agent_retry(agent_id.clone(), ws.clone())
            .await
            .expect("retry");
        assert_eq!(result["ok"], true);
        // Nothing queued → nothing redriven; the client is told explicitly
        // (STAB-54: an empty-queue retry must not be a silent no-op).
        assert_eq!(result["redriven"], false);

        // Status should be cleared to Idle — a `pending` status would park the
        // agent forever since no queued message will ever drive it forward.
        let session = mgr
            .services
            .store
            .get_agent_session(&agent_id)
            .await
            .expect("session");
        assert_eq!(session.status, AgentStatus::RuntimeIdle);
    }

    #[tokio::test]
    async fn retry_from_error_status_with_queued_message_redrives() {
        let agent_id = AgentId::from("agent-redrive");
        let ws = WorkspaceId::from("ws-redrive");
        let mgr = manager_with_session(&agent_id, &ws, AgentStatus::Error).await;

        // A requeued message is waiting (the persist_error_and_requeue path).
        mgr.services
            .enqueue_message(&agent_id, "requeued".to_string(), None, None, None);

        let result = mgr
            .agent_retry(agent_id.clone(), ws.clone())
            .await
            .expect("retry");
        assert_eq!(result["ok"], true);
        assert_eq!(result["redriven"], true);

        // The drain loop claimed the queued message (dequeued for redrive).
        assert!(
            !mgr.services.has_ready_to_send(&agent_id),
            "queued message dequeued for redrive"
        );
    }

    /// Regression for the check-then-flip race in `agent_retry`: a message
    /// enqueued while the session is still `Error` has its own drain kick
    /// suppressed (STAB-52 gate), so if it lands after retry's initial queue
    /// check the post-flip re-check must promote Idle → Pending and drain it.
    /// Sweeps interleavings by varying the number of yields before the
    /// concurrent enqueue; whatever the timing, the raced message must never
    /// be stranded ready-to-send on an `Idle` session.
    #[tokio::test]
    async fn retry_racing_concurrent_enqueue_never_strands_message() {
        for yields in 0..8u32 {
            let id = format!("agent-race-{yields}");
            let agent_id = AgentId::from(id.as_str());
            let ws = WorkspaceId::from("ws-race");
            let mgr = manager_with_session(&agent_id, &ws, AgentStatus::Error).await;

            let retry_fut = mgr.agent_retry(agent_id.clone(), ws.clone());
            let enqueue_fut = async {
                for _ in 0..yields {
                    tokio::task::yield_now().await;
                }
                mgr.services
                    .enqueue_message(&agent_id, "raced".to_string(), None, None, None);
                mgr.clone()
                    .try_drain_queue(agent_id.clone(), ws.clone())
                    .await;
            };
            let (retry_result, ()) = tokio::join!(retry_fut, enqueue_fut);
            let result = retry_result.expect("retry");
            assert_eq!(result["ok"], true);

            // Whichever side won the race, the message must have been claimed
            // by a drain: either retry's post-flip re-check redrove it, or the
            // enqueue's own drain kick ran after the Error gate lifted. An
            // `Idle` session with a ready-to-send message means it was
            // stranded — the exact bug the re-check closes.
            let session = mgr
                .services
                .store
                .get_agent_session(&agent_id)
                .await
                .expect("session");
            assert!(
                !(session.status == AgentStatus::RuntimeIdle
                    && mgr.services.has_ready_to_send(&agent_id)),
                "raced message stranded on idle session (yields={yields})"
            );
        }
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
