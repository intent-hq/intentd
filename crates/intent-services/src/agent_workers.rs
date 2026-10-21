//! Provider worker observations (PROTOCOL §5.5b).
//!
//! A bounded, memory-only projection of the background workers (shells,
//! workflows, monitors) an allowlisted provider adapter reports through the AIR
//! async-tasks extension. It is not a process monitor: rows are provider
//! reports, the only liveness signal is the provider connection itself, and a
//! stop is a request the provider may decline. The rules mirror the monorepo's
//! executable model (`scripts/agent-worker-contract.mjs`): first terminal wins,
//! seen-ID tombstones survive eviction, ended rows are evicted before running
//! ones, and nothing but a provider frame ever changes a row.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use intent_acp::transport::Connection;
use intent_acp::workers::WorkerFrame;
use intent_core::{AgentId, WorkspaceId};
use serde_json::{json, Map, Value};

/// Rows retained per generation.
pub(crate) const MAX_ROWS: usize = 256;
/// Distinct task IDs admitted per generation (tombstones included).
pub(crate) const MAX_SEEN: usize = 1024;
/// Largest timestamp the wire carries as an exact JSON number.
const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;
const SUBAGENT_TASK_TYPES: [&str; 2] = ["agent", "local_agent"];
const TERMINAL: [&str; 3] = ["completed", "failed", "stopped"];

/// One reviewed adapter pin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Adapter {
    /// Base provider ID (profiles `id@name` share it).
    pub provider: &'static str,
    /// `initialize` `agentInfo.name`.
    pub name: &'static str,
    /// `initialize` `agentInfo.version`.
    pub version: &'static str,
    /// Row `source`.
    pub source: &'static str,
}

/// The reviewed adapter allowlist (§5.5b table). Any other provider or version
/// is unsupported until reviewed; `claude_allowlist_tracks_the_pinned_adapter`
/// fails when the Claude adapter pin moves without a review here.
pub(crate) const SUPPORTED_ADAPTERS: [Adapter; 2] = [
    Adapter {
        provider: "codex",
        name: "@agentclientprotocol/codex-acp",
        version: "2.2.2",
        source: "codex-air",
    },
    Adapter {
        provider: "claude-code",
        name: "@agentclientprotocol/claude-agent-acp",
        version: "0.81.1",
        source: "claude-air",
    },
];

fn adapter_for(provider: &str) -> Option<&'static Adapter> {
    let base = intent_providers::base_provider_id(provider);
    SUPPORTED_ADAPTERS.iter().find(|a| a.provider == base)
}

/// The adapter's self-reported identity from its `initialize` response.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct AdapterIdentity {
    pub name: Option<String>,
    pub version: Option<String>,
}

impl AdapterIdentity {
    pub(crate) fn from_initialize(info: Option<&Value>) -> Self {
        let field = |k: &str| {
            info.and_then(|i| i.get(k))
                .and_then(Value::as_str)
                .map(str::to_owned)
        };
        Self {
            name: field("name"),
            version: field("version"),
        }
    }
}

use intent_acp::workers::{is_control, is_wire_id as valid_id};

/// Replace control characters with spaces and truncate to `max_bytes` on a
/// character boundary.
fn bounded(value: &str, max_bytes: usize) -> String {
    let mut out = String::new();
    for c in value.chars().map(|c| if is_control(c) { ' ' } else { c }) {
        if out.len() + c.len_utf8() > max_bytes {
            break;
        }
        out.push(c);
    }
    out
}

/// Sanitize first, then judge: Codex reports only shells; Claude also reports
/// workflows and monitors; subagents are never workers.
fn accepted_task_type(provider: &str, task_type: Option<&Value>) -> Option<String> {
    let task_type = bounded(task_type?.as_str()?, 64).trim().to_owned();
    if task_type.is_empty() || SUBAGENT_TASK_TYPES.contains(&task_type.as_str()) {
        return None;
    }
    if intent_providers::base_provider_id(provider) == "codex" && task_type != "shell" {
        return None;
    }
    Some(task_type)
}

pub(crate) use intent_acp::workers::now_ms;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Continuity {
    Continuous,
    Disconnected,
    Lost,
}

impl Continuity {
    fn as_str(self) -> &'static str {
        match self {
            Self::Continuous => "continuous",
            Self::Disconnected => "disconnected",
            Self::Lost => "lost",
        }
    }
}

#[derive(Debug, Clone)]
struct Row {
    async_task_id: String,
    tool_call_id: Option<String>,
    name: Option<String>,
    task_type: Option<String>,
    can_stop: bool,
    first_observed_at: u64,
    observed_at: u64,
    provider_state: &'static str,
}

impl Row {
    fn ended(&self) -> bool {
        TERMINAL.contains(&self.provider_state)
    }
}

/// The receiving scope of one outer ACP connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Scope {
    pub workspace_id: WorkspaceId,
    pub agent_id: AgentId,
    pub provider: String,
    pub acp_session_id: Option<String>,
    pub connection_generation: Option<String>,
}

/// What a stop request should do (§5.5b Stop).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StopDecision {
    /// Answer `stopped: false` without calling the provider.
    Decline,
    /// Send `_session/async_task/stop` on this session.
    Forward { session_id: String },
}

/// The bounded projection for one generation.
#[derive(Debug)]
pub(crate) struct Projection {
    scope: Scope,
    adapter: Option<&'static Adapter>,
    /// Static support before session/continuity: `None` when supported,
    /// otherwise the unsupported reason.
    unsupported: Option<&'static str>,
    continuity: Continuity,
    rows: Vec<Row>,
    seen: HashSet<String>,
    now: u64,
    truncated: bool,
    max_rows: usize,
    max_seen: usize,
}

impl Projection {
    pub(crate) fn new(scope: Scope, identity: &AdapterIdentity, negotiated: bool) -> Self {
        Self::with_bounds(scope, identity, negotiated, MAX_ROWS, MAX_SEEN)
    }

    fn with_bounds(
        scope: Scope,
        identity: &AdapterIdentity,
        negotiated: bool,
        max_rows: usize,
        max_seen: usize,
    ) -> Self {
        let adapter = adapter_for(&scope.provider);
        let unsupported = match adapter {
            None => Some("provider"),
            Some(a)
                if identity.name.as_deref() != Some(a.name)
                    || identity.version.as_deref() != Some(a.version) =>
            {
                Some("adapter-version")
            }
            Some(_) if !negotiated => Some("not-negotiated"),
            Some(_) => None,
        };
        let continuity = if scope.connection_generation.is_some() {
            Continuity::Continuous
        } else {
            Continuity::Lost
        };
        Self {
            scope,
            adapter,
            unsupported,
            continuity,
            rows: Vec::new(),
            seen: HashSet::new(),
            now: 0,
            truncated: false,
            max_rows,
            max_seen,
        }
    }

    /// The projection for an agent with no live connection in this daemon.
    pub(crate) fn no_session(
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        provider: String,
    ) -> Self {
        Self::new(
            Scope {
                workspace_id,
                agent_id,
                provider,
                acp_session_id: None,
                connection_generation: None,
            },
            &AdapterIdentity::default(),
            false,
        )
    }

    pub(crate) fn generation(&self) -> Option<&str> {
        self.scope.connection_generation.as_deref()
    }

    /// `(status, reason)`; `reason` is `None` only when supported.
    fn support_state(&self) -> (&'static str, Option<&'static str>) {
        if self.scope.acp_session_id.is_none() || self.scope.connection_generation.is_none() {
            return ("unavailable", Some("no-session"));
        }
        if let Some(reason) = self.unsupported {
            return ("unsupported", Some(reason));
        }
        if self.continuity == Continuity::Lost {
            return ("unavailable", Some("continuity-lost"));
        }
        ("supported", None)
    }

    fn support(&self) -> Value {
        match self.support_state() {
            (status, None) => json!({ "status": status }),
            (status, Some(reason)) => json!({ "status": status, "reason": reason }),
        }
    }

    fn supported(&self) -> bool {
        self.support_state().1.is_none()
    }

    /// Bind the ACP session once `session/new` or `session/load` returns.
    /// Returns whether the snapshot changed.
    pub(crate) fn bind_session(&mut self, session_id: &str) -> bool {
        if self.scope.acp_session_id.as_deref() == Some(session_id) || !valid_id(session_id) {
            return false;
        }
        if self.scope.acp_session_id.is_some() {
            // A different session on the same outer connection cannot inherit
            // these observations.
            self.rows.clear();
            self.seen.clear();
            self.truncated = false;
        }
        self.scope.acp_session_id = Some(session_id.to_owned());
        true
    }

    fn clock(&mut self, now: u64) -> u64 {
        self.now = self.now.max(now);
        self.now
    }

    /// Ingest one live frame. Returns whether state changed.
    pub(crate) fn ingest(&mut self, frame: &WorkerFrame) -> bool {
        if !self.supported() || self.continuity != Continuity::Continuous {
            return false;
        }
        if frame.session_id.is_none() || frame.session_id != self.scope.acp_session_id {
            return false;
        }
        let u = &frame.update;
        let Some(task_id) = u.get("asyncTaskId").and_then(Value::as_str) else {
            return false;
        };
        if !valid_id(task_id) {
            return false;
        }
        let tool_call_id = match u.get("toolCallId") {
            None => None,
            Some(Value::String(s)) if valid_id(s) => Some(s.clone()),
            Some(_) => return false,
        };
        let kind = u.get("sessionUpdate").and_then(Value::as_str);
        let task_type = if kind == Some("async_task_spawned") {
            accepted_task_type(&self.scope.provider, u.get("taskType"))
        } else {
            None
        };
        let at = frame.received_at_ms;
        if at > MAX_SAFE_INTEGER {
            return false;
        }
        if kind == Some("async_task_spawned") && task_type.is_none() {
            // Not a worker (a subagent, or a Codex non-shell task): tombstone
            // the ID without a row so its later end report cannot create one.
            if !self.seen.contains(task_id) && self.seen.len() < self.max_seen {
                self.seen.insert(task_id.to_owned());
            }
            return false;
        }
        let spawned = task_type.is_some();
        let ended_state = (kind == Some("async_task_state_update"))
            .then(|| u.get("state").and_then(Value::as_str))
            .flatten()
            .and_then(|s| TERMINAL.iter().find(|t| **t == s).copied());
        if !spawned && ended_state.is_none() {
            return false;
        }
        self.clock(at);
        let state = if spawned {
            "running"
        } else {
            ended_state.expect("checked above")
        };
        if let Some(row) = self.rows.iter_mut().find(|r| r.async_task_id == task_id) {
            // No sequence or epoch upstream: first terminal wins; a duplicate
            // spawn is not a heartbeat.
            if spawned || row.ended() || at < row.observed_at {
                return false;
            }
            row.provider_state = state;
            row.observed_at = at;
            return true;
        }
        // Tombstones survive eviction, so repeated frames cannot recreate rows.
        if self.seen.contains(task_id) {
            return false;
        }
        if self.seen.len() >= self.max_seen {
            let changed = !self.truncated;
            self.truncated = true;
            return changed;
        }
        self.seen.insert(task_id.to_owned());
        if self.rows.len() >= self.max_rows {
            // Running rows never expire: evict the oldest ended row first.
            let victim = self.rows.iter().position(Row::ended).unwrap_or(0);
            self.rows.remove(victim);
            self.truncated = true;
        }
        self.rows.push(Row {
            async_task_id: task_id.to_owned(),
            tool_call_id,
            name: u
                .get("name")
                .and_then(Value::as_str)
                .map(|n| bounded(n, 512)),
            task_type,
            can_stop: spawned && u.get("canStop") == Some(&Value::Bool(true)),
            first_observed_at: at,
            observed_at: at,
            provider_state: state,
        });
        true
    }

    /// The outer provider connection closed. Returns whether state changed.
    pub(crate) fn disconnect(&mut self) -> bool {
        if self.continuity != Continuity::Continuous {
            return false;
        }
        self.continuity = Continuity::Disconnected;
        true
    }

    /// Whether this generation can vouch for a still-running worker: it is
    /// supported, its connection is continuous, and at least one row has no
    /// end report yet. The idle reaper keeps such an agent alive, since
    /// killing its process would kill the worker with it.
    pub(crate) fn has_running_workers(&self) -> bool {
        self.supported()
            && self.continuity == Continuity::Continuous
            && self.rows.iter().any(|r| r.provider_state == "running")
    }

    /// The complete `WorkerSnapshot` (§5.5b).
    pub(crate) fn snapshot(&mut self, now: u64) -> Value {
        let as_of = self.clock(now);
        let s = &self.scope;
        let workers: Vec<Value> = self.rows.iter().map(|r| self.row_json(r)).collect();
        json!({
            "workspaceId": s.workspace_id,
            "agentId": s.agent_id,
            "provider": s.provider,
            "acpSessionId": s.acp_session_id,
            "connectionGeneration": s.connection_generation,
            "support": self.support(),
            "continuity": self.continuity.as_str(),
            "asOf": as_of,
            "truncated": self.truncated,
            "workers": workers,
        })
    }

    fn row_json(&self, r: &Row) -> Value {
        let s = &self.scope;
        let mut row = Map::new();
        row.insert(
            "identity".into(),
            json!({
                "workspaceId": s.workspace_id,
                "agentId": s.agent_id,
                "provider": s.provider,
                "acpSessionId": s.acp_session_id,
                "connectionGeneration": s.connection_generation,
                "asyncTaskId": r.async_task_id,
            }),
        );
        if let Some(id) = &r.tool_call_id {
            row.insert("toolCallId".into(), json!(id));
        }
        if let Some(name) = &r.name {
            row.insert("name".into(), json!(name));
        }
        if let Some(task_type) = &r.task_type {
            row.insert("taskType".into(), json!(task_type));
        }
        row.insert("canStop".into(), json!(r.can_stop));
        row.insert("firstObservedAt".into(), json!(r.first_observed_at));
        row.insert("observedAt".into(), json!(r.observed_at));
        row.insert("providerState".into(), json!(r.provider_state));
        row.insert(
            "source".into(),
            json!(self.adapter.map_or("unknown", |a| a.source)),
        );
        row.insert("cause".into(), json!("unavailable"));
        row.insert("exitEvidence".into(), json!({ "kind": "unknown" }));
        if r.provider_state == "running" && self.continuity != Continuity::Continuous {
            row.insert("status".into(), json!("unknown"));
            row.insert("unknownReason".into(), json!(self.continuity.as_str()));
        } else {
            row.insert("status".into(), json!(r.provider_state));
        }
        Value::Object(row)
    }

    /// Decide a stop request (§5.5b Stop). `Err(())` is not-found.
    pub(crate) fn stop_decision(
        &self,
        connection_generation: &str,
        async_task_id: &str,
    ) -> Result<StopDecision, ()> {
        if self.generation() != Some(connection_generation) {
            return Err(());
        }
        let row = self
            .rows
            .iter()
            .find(|r| r.async_task_id == async_task_id)
            .ok_or(())?;
        if !self.supported()
            || self.continuity != Continuity::Continuous
            || row.provider_state != "running"
            || !row.can_stop
        {
            return Ok(StopDecision::Decline);
        }
        Ok(StopDecision::Forward {
            session_id: self
                .scope
                .acp_session_id
                .clone()
                .expect("supported implies a bound session"),
        })
    }
}

struct Entry {
    projection: Projection,
    connection: Weak<Connection>,
    /// Revoked when this generation disconnects, is replaced or released;
    /// a stop request carries it to the connection's writer (§5.5b).
    fence: Arc<AtomicBool>,
}

impl Entry {
    fn revoke(&self) {
        self.fence.store(true, Ordering::SeqCst);
    }
}

/// What [`AgentWorkers::stop_target`] resolved.
pub(crate) enum StopTarget {
    Decline,
    Forward {
        connection: Arc<Connection>,
        session_id: String,
        /// The generation's write fence: if it is revoked before the
        /// request is written, nothing is sent.
        fence: Arc<AtomicBool>,
    },
    /// The deciding generation's connection is gone: send nothing.
    ConnectionGone,
}

/// Per-agent projections for the live generations in this daemon.
#[derive(Default)]
pub(crate) struct AgentWorkers {
    entries: Mutex<HashMap<AgentId, Entry>>,
}

impl AgentWorkers {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<AgentId, Entry>> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Start a new generation for a freshly initialized outer connection,
    /// atomically clearing the previous generation's rows and tombstones.
    /// Returns the new snapshot.
    pub(crate) fn begin(
        &self,
        scope: Scope,
        identity: &AdapterIdentity,
        negotiated: bool,
        connection: Weak<Connection>,
    ) -> Value {
        let agent_id = scope.agent_id.clone();
        let mut projection = Projection::new(scope, identity, negotiated);
        let snapshot = projection.snapshot(now_ms());
        let replaced = self.lock().insert(
            agent_id,
            Entry {
                projection,
                connection,
                fence: Arc::new(AtomicBool::new(false)),
            },
        );
        if let Some(old) = replaced {
            old.revoke();
        }
        snapshot
    }

    fn with_generation<T>(
        &self,
        agent_id: &AgentId,
        generation: &str,
        f: impl FnOnce(&mut Projection) -> Option<T>,
    ) -> Option<T> {
        let mut entries = self.lock();
        let entry = entries.get_mut(agent_id)?;
        if entry.projection.generation() != Some(generation) {
            return None;
        }
        f(&mut entry.projection)
    }

    /// The generation of `connection`, when it is the agent's current one.
    pub(crate) fn generation_for(
        &self,
        agent_id: &AgentId,
        connection: &Arc<Connection>,
    ) -> Option<String> {
        self.lock()
            .get(agent_id)
            .filter(|e| std::ptr::eq(e.connection.as_ptr(), Arc::as_ptr(connection)))
            .and_then(|e| e.projection.generation().map(str::to_owned))
    }

    /// Record the adapter identity and extension request once `initialize`
    /// returned on `generation`. Rows cannot exist yet (no session is bound),
    /// so the projection is rebuilt for the negotiated support.
    pub(crate) fn negotiate(
        &self,
        agent_id: &AgentId,
        generation: &str,
        identity: &AdapterIdentity,
        negotiated: bool,
    ) -> Option<Value> {
        self.with_generation(agent_id, generation, |p| {
            let mut scope = p.scope.clone();
            scope.acp_session_id = None;
            let continuity = p.continuity;
            *p = Projection::new(scope, identity, negotiated);
            // A connection that already dropped stays dropped.
            p.continuity = continuity;
            Some(p.snapshot(now_ms()))
        })
    }

    /// Bind the ACP session of `generation`. Returns the changed snapshot.
    pub(crate) fn bind_session(
        &self,
        agent_id: &AgentId,
        generation: &str,
        session_id: &str,
    ) -> Option<Value> {
        self.with_generation(agent_id, generation, |p| {
            p.bind_session(session_id).then(|| p.snapshot(now_ms()))
        })
    }

    /// Ingest a frame received on `generation`. Returns the changed snapshot.
    pub(crate) fn ingest(
        &self,
        agent_id: &AgentId,
        generation: &str,
        frame: &WorkerFrame,
    ) -> Option<Value> {
        self.with_generation(agent_id, generation, |p| {
            p.ingest(frame).then(|| p.snapshot(now_ms()))
        })
    }

    /// Mark `generation`'s connection disconnected. Returns the changed snapshot.
    pub(crate) fn disconnect(&self, agent_id: &AgentId, generation: &str) -> Option<Value> {
        let mut entries = self.lock();
        let entry = entries.get_mut(agent_id)?;
        if entry.projection.generation() != Some(generation) {
            return None;
        }
        entry.revoke();
        let p = &mut entry.projection;
        p.disconnect().then(|| p.snapshot(now_ms()))
    }

    /// Release an agent's projection (its live session was disposed: the
    /// provider tree, and with it every worker, is gone). Returns the
    /// replacement no-session snapshot for subscribers.
    pub(crate) fn release(&self, agent_id: &AgentId) -> Option<Value> {
        let entry = self.lock().remove(agent_id)?;
        entry.revoke();
        let s = entry.projection.scope;
        Some(Projection::no_session(s.workspace_id, s.agent_id, s.provider).snapshot(now_ms()))
    }

    /// The live snapshot, when this daemon holds a generation for the agent.
    pub(crate) fn snapshot(&self, agent_id: &AgentId) -> Option<Value> {
        self.lock()
            .get_mut(agent_id)
            .map(|e| e.projection.snapshot(now_ms()))
    }

    /// Whether the agent's current generation has a running worker on a
    /// continuous connection ([`Projection::has_running_workers`]). The
    /// workers lock is a leaf lock, so this is safe to call while holding the
    /// process registry lock.
    pub(crate) fn has_running_workers(&self, agent_id: &AgentId) -> bool {
        self.lock()
            .get(agent_id)
            .is_some_and(|e| e.projection.has_running_workers())
    }

    /// Resolve a stop request. `None` is not-found.
    pub(crate) fn stop_target(
        &self,
        agent_id: &AgentId,
        connection_generation: &str,
        async_task_id: &str,
    ) -> Option<StopTarget> {
        let entries = self.lock();
        let entry = entries.get(agent_id)?;
        match entry
            .projection
            .stop_decision(connection_generation, async_task_id)
            .ok()?
        {
            StopDecision::Decline => Some(StopTarget::Decline),
            StopDecision::Forward { session_id } => Some(match entry.connection.upgrade() {
                Some(connection) if !entry.fence.load(Ordering::SeqCst) => StopTarget::Forward {
                    connection,
                    session_id,
                    fence: Arc::clone(&entry.fence),
                },
                _ => StopTarget::ConnectionGone,
            }),
        }
    }
}

#[cfg(test)]
#[path = "agent_workers_tests.rs"]
mod tests;
