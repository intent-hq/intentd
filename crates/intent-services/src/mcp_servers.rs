//! External MCP-server lifecycle/config (§18.3, PROTOCOL §5.22): manage the set
//! of **user-configured external** MCP servers (distinct from the §6.8 agent→BE
//! callback). Config lives in the **sensitive** `mcp.servers` setting (`env`/
//! `headers` redacted over the wire); the [`McpHub`] spawns/stops/restarts
//! **stdio** servers, probes remote **http**/**sse** endpoints from the daemon
//! host, and a health monitor pings/re-probes them, pushing
//! `mcp.servers:status-changed` (§10) on every transition. Runtime status is
//! never persisted. Ports `mcp-hub.ts`/`server-manager.ts`/`health-monitor.ts`/
//! `user-mcp-settings.ts`.

use std::collections::HashMap;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use intent_acp::{Connection, ConnectionHooks};
use intent_core::settings_file::SettingsFile;
use intent_core::{events::MCP_SERVERS_STATUS_CHANGED, now_iso, Error, Result, WorkspaceId};
use intent_store::NewEvent;
use serde_json::{json, Map, Value};
use tokio::io::AsyncRead;
use tokio::process::{Child, Command};
use uuid::Uuid;

use crate::settings::{AsyncSecretStore, REDACTED_PLACEHOLDER};
use crate::settings_registry::SettingsRegistry;
use crate::{system_actor, EventBus};

/// Keychain account for the sensitive `mcp.servers` setting (§9.8). Mirrors the
/// `SettingsService` redaction seam — the config (with secrets) lives here.
const SETTING_KEY: &str = "mcp.servers";
/// MCP protocol version advertised on `initialize` (mirrors the stdio peers).
const MCP_PROTOCOL_VERSION: &str = "2024-11-05";
/// MCP protocol version advertised on remote `http` probes: the earliest
/// revision that defines the streamable-HTTP transport.
const MCP_HTTP_PROTOCOL_VERSION: &str = "2025-03-26";
/// Timeout for the `initialize`/`tools/list` handshake requests.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// Overall wall-clock bound for one remote probe (handshake + follow-ups),
/// so a slow endpoint cannot hold a probe for multiple per-request timeouts.
const PROBE_TIMEOUT: Duration = Duration::from_secs(15);
/// Timeout for a single health `ping`.
const PING_TIMEOUT: Duration = Duration::from_secs(5);
/// `Accept` header required by MCP streamable HTTP (the server may answer a
/// POST with either a plain JSON body or an SSE stream).
const HTTP_ACCEPT: &str = "application/json, text/event-stream";
/// Default health-check interval (parity: `HealthMonitor` 30s).
const HEALTH_INTERVAL: Duration = Duration::from_secs(30);
/// Consecutive ping failures before the monitor restarts a server (parity: 3).
const MAX_FAILURES: u32 = 3;
/// Grace window between SIGTERM and SIGKILL when reaping (PTY-host parity).
const TERM_GRACE: Duration = Duration::from_millis(500);
/// Poll cadence while waiting for a reaped child to exit.
const REAP_POLL: Duration = Duration::from_millis(25);

/// Epoch milliseconds (the `startedAt` shape in PROTOCOL §5.22's example).
fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// Build a wire `McpServerStatus` (§5.22), omitting absent optional fields.
fn status_value(
    server_id: &str,
    state: &str,
    pid: Option<u32>,
    tool_count: Option<u64>,
    last_error: Option<&str>,
    started_at: Option<u64>,
) -> Value {
    let mut m = Map::new();
    m.insert("serverId".into(), json!(server_id));
    m.insert("state".into(), json!(state));
    if let Some(pid) = pid {
        m.insert("pid".into(), json!(pid));
    }
    if let Some(tc) = tool_count {
        m.insert("toolCount".into(), json!(tc));
    }
    if let Some(err) = last_error {
        m.insert("lastError".into(), json!(err));
    }
    if let Some(at) = started_at {
        m.insert("startedAt".into(), json!(at));
    }
    Value::Object(m)
}

/// A `stopped` status snapshot for `server_id`.
fn status_stopped(server_id: &str) -> Value {
    status_value(server_id, "stopped", None, None, None, None)
}

/// An `error` status snapshot carrying `last_error` for `server_id`.
fn status_error(server_id: &str, last_error: &str) -> Value {
    status_value(server_id, "error", None, None, Some(last_error), None)
}

/// The `id` of a config Value (empty when absent).
fn config_id(config: &Value) -> String {
    config
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string()
}

/// Return a copy of `config` with every `env`/`headers` **value** replaced by the
/// redaction placeholder (presence/placeholder only, PROTOCOL §5.22 / §5.12).
fn redact_config(config: &Value) -> Value {
    let mut c = config.clone();
    for key in ["env", "headers"] {
        if let Some(obj) = c.get_mut(key).and_then(Value::as_object_mut) {
            for (_k, val) in obj.iter_mut() {
                *val = json!(REDACTED_PLACEHOLDER);
            }
        }
    }
    c
}

/// Fill defaults + validate a wire `McpServerConfig` (§5.22). `forced_id` pins
/// the id (update); otherwise an absent id is generated. stdio requires a
/// `command`; http/sse require a `url`.
fn normalize_config(mut config: Value, forced_id: Option<&str>) -> Result<Value> {
    let obj = config
        .as_object_mut()
        .ok_or_else(|| Error::InvalidParams("config must be an object".to_string()))?;
    let id = match forced_id {
        Some(i) => i.to_string(),
        None => obj
            .get("id")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map_or_else(
                || format!("srv-{}", &Uuid::new_v4().simple().to_string()[..8]),
                String::from,
            ),
    };
    obj.insert("id".into(), json!(id));
    let transport = obj
        .get("transport")
        .and_then(Value::as_str)
        .unwrap_or("stdio")
        .to_string();
    if !["stdio", "http", "sse"].contains(&transport.as_str()) {
        return Err(Error::InvalidParams(format!(
            "invalid transport: {transport}"
        )));
    }
    obj.insert("transport".into(), json!(transport.clone()));
    validate_transport_fields(obj, &transport)?;
    if obj
        .get("name")
        .and_then(Value::as_str)
        .is_none_or(str::is_empty)
    {
        obj.insert("name".into(), json!(id));
    }
    if !obj.get("enabled").is_some_and(Value::is_boolean) {
        obj.insert("enabled".into(), json!(false));
    }
    Ok(config)
}

/// Validate the transport-specific required fields of a config object.
fn validate_transport_fields(obj: &Map<String, Value>, transport: &str) -> Result<()> {
    if transport == "stdio" {
        let has_cmd = obj
            .get("command")
            .and_then(Value::as_str)
            .is_some_and(|s| !s.is_empty());
        if !has_cmd {
            return Err(Error::InvalidParams(
                "stdio server requires a non-empty command".to_string(),
            ));
        }
    } else {
        let has_url = obj
            .get("url")
            .and_then(Value::as_str)
            .is_some_and(|s| !s.is_empty());
        if !has_url {
            return Err(Error::InvalidParams(format!(
                "{transport} server requires a url"
            )));
        }
    }
    Ok(())
}

/// The effective `mcp.enableUserServers` gate (default `true`, §9.8 group A).
pub(crate) fn enable_user_servers(settings: &SettingsFile) -> bool {
    settings.mcp.enable_user_servers
}

/// The effective `mcp.disabledServers` list (ids that stay stopped, default `[]`).
pub(crate) fn disabled_servers(settings: &SettingsFile) -> Vec<String> {
    settings.mcp.disabled_servers.clone()
}

/// Persist the `mcp.disabledServers` list through the registry (`config.toml`).
/// Without a wired registry (read-only/test wiring) the write is a quiet no-op.
fn set_disabled_servers(registry: Option<&SettingsRegistry>, list: &[String]) -> Result<()> {
    match registry {
        Some(reg) => reg
            .apply(&[("mcp.disabledServers".to_string(), json!(list))])
            .map(|_| ()),
        None => Ok(()),
    }
}

/// Read the configured external servers from the sensitive `mcp.servers` secret,
/// keyed by id. A missing/garbled secret yields an empty map. Best-effort: errors
/// (timeout/backing failure) treated as absent.
pub(crate) async fn read_configs(secrets: &AsyncSecretStore) -> Map<String, Value> {
    match secrets.load(SETTING_KEY).await {
        Ok(Some(raw)) => serde_json::from_str::<Value>(&raw)
            .ok()
            .and_then(|v| v.as_object().cloned())
            .unwrap_or_default(),
        Ok(None) | Err(_) => Map::new(),
    }
}

/// Persist the configured external servers back to the sensitive secret.
async fn write_configs(secrets: &AsyncSecretStore, map: &Map<String, Value>) -> Result<()> {
    let raw = serde_json::to_string(&Value::Object(map.clone()))
        .map_err(|e| Error::Internal(format!("encode mcp.servers failed: {e}")))?;
    secrets.store(SETTING_KEY, &raw).await
}

/// Transport-specific runtime half of a tracked server entry.
enum ServerRuntime {
    /// A spawned stdio child + its JSON-RPC stdio connection.
    Stdio {
        child: Child,
        pid: Option<u32>,
        conn: Arc<Connection>,
    },
    /// A remote (`http`/`sse`) endpoint probed over the network — no process.
    Remote,
}

/// A tracked MCP server: its config, transport runtime, the last published
/// status, and the consecutive health-failure count (stdio only).
struct RunningServer {
    config: Value,
    runtime: ServerRuntime,
    status: Value,
    failures: u32,
}

/// Shared runtime state for the [`McpHub`].
struct HubInner {
    servers: Mutex<HashMap<String, RunningServer>>,
    bus: Mutex<Option<EventBus>>,
}

/// Runtime manager for external MCP servers (the `ServerManager` + `HealthMonitor`
/// + `McpHub` of §18.3 collapsed into one BE-owned handle). Cloneable; every
///   clone shares the same server map and event bus.
#[derive(Clone)]
pub struct McpHub {
    inner: Arc<HubInner>,
}

impl Default for McpHub {
    fn default() -> Self {
        Self::new()
    }
}

impl McpHub {
    /// Build an empty hub with no event bus wired yet.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(HubInner {
                servers: Mutex::new(HashMap::new()),
                bus: Mutex::new(None),
            }),
        }
    }

    /// Wire the event bus the hub publishes `mcp.servers:status-changed` onto.
    pub(crate) fn set_event_bus(&self, bus: EventBus) {
        *self.inner.bus.lock().unwrap() = Some(bus);
    }

    /// Build + publish a self-sufficient `mcp.servers:status-changed` event
    /// (`{ serverId, status }`, §10.1) when a bus is wired.
    async fn publish_status(&self, status: &Value) {
        let bus = { self.inner.bus.lock().unwrap().clone() };
        let Some(bus) = bus else {
            return;
        };
        let server_id = status
            .get("serverId")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let event = NewEvent {
            workspace_id: WorkspaceId::from_string(String::new()),
            timestamp: now_iso(),
            event_type: MCP_SERVERS_STATUS_CHANGED.to_string(),
            actor: system_actor(),
            session_id: None,
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data: json!({ "serverId": server_id, "status": status }),
        };
        if let Err(e) = bus.publish(&event).await {
            tracing::warn!(error = %e, "failed to publish mcp.servers:status-changed");
        }
    }

    /// Live status for `id` (running snapshot, else `stopped`).
    pub fn status(&self, id: &str) -> Value {
        self.inner
            .servers
            .lock()
            .unwrap()
            .get(id)
            .map_or_else(|| status_stopped(id), |rs| rs.status.clone())
    }

    /// Remove + reap `id` without emitting a `stopped` event (used before a
    /// (re)start, where a `running`/`error` event follows).
    async fn stop_inner(&self, id: &str) -> bool {
        let rs = self.inner.servers.lock().unwrap().remove(id);
        match rs {
            Some(mut rs) => {
                reap(&mut rs).await;
                true
            }
            None => false,
        }
    }

    /// Stop `id`, emitting `stopped` when a server was actually running.
    pub async fn stop(&self, id: &str) -> bool {
        let existed = self.stop_inner(id).await;
        if existed {
            self.publish_status(&status_stopped(id)).await;
        }
        existed
    }

    /// Start `config`. Replaces any existing instance and emits `running` (or
    /// `error`): stdio configs spawn a child + MCP handshake, `http`/`sse`
    /// configs are probed over the network from the daemon host. When
    /// `enable_user_servers` is false the server is left `stopped`.
    pub async fn start(&self, config: Value, enable_user_servers: bool) -> Value {
        let id = config_id(&config);
        self.stop_inner(&id).await;
        if !enable_user_servers {
            let st = status_stopped(&id);
            self.publish_status(&st).await;
            return st;
        }
        let transport = config
            .get("transport")
            .and_then(Value::as_str)
            .unwrap_or("stdio")
            .to_string();
        if transport != "stdio" {
            return self.start_remote(id, config).await;
        }
        match spawn_stdio(&config).await {
            Ok((child, pid, conn, tool_count)) => {
                let status =
                    status_value(&id, "running", pid, tool_count, None, Some(now_millis()));
                let rs = RunningServer {
                    config,
                    runtime: ServerRuntime::Stdio {
                        child,
                        pid,
                        conn: Arc::new(conn),
                    },
                    status: status.clone(),
                    failures: 0,
                };
                self.inner.servers.lock().unwrap().insert(id, rs);
                self.publish_status(&status).await;
                status
            }
            Err(e) => {
                let status = status_error(&id, &e.to_string());
                self.publish_status(&status).await;
                status
            }
        }
    }

    /// Probe a remote (`http`/`sse`) config and register the outcome. Unlike a
    /// failed stdio spawn, a failed probe keeps the entry tracked (state
    /// `error`) so the health sweep re-probes it — there is no process to
    /// restart, only status to flip.
    async fn start_remote(&self, id: String, config: Value) -> Value {
        let status = remote_probe_status(&id, &config).await;
        let rs = RunningServer {
            config,
            runtime: ServerRuntime::Remote,
            status: status.clone(),
            failures: 0,
        };
        self.inner.servers.lock().unwrap().insert(id, rs);
        self.publish_status(&status).await;
        status
    }

    /// Restart `config`: stop-then-start (emits `stopped` then `running`/`error`).
    pub async fn restart(&self, config: Value, enable_user_servers: bool) -> Value {
        let id = config_id(&config);
        self.stop(&id).await;
        self.start(config, enable_user_servers).await
    }

    /// One health sweep. stdio servers are pinged over their connection and
    /// restarted after [`MAX_FAILURES`] consecutive failures. Remote
    /// (`http`/`sse`) servers are re-probed from the daemon host and their
    /// status flipped on transition — never restarted (no process to manage).
    async fn health_tick(&self) {
        enum Probe {
            Stdio(Arc<Connection>),
            Remote(Value),
        }
        let targets: Vec<(String, Probe)> = self
            .inner
            .servers
            .lock()
            .unwrap()
            .iter()
            .map(|(id, rs)| {
                let probe = match &rs.runtime {
                    ServerRuntime::Stdio { conn, .. } => Probe::Stdio(conn.clone()),
                    ServerRuntime::Remote => Probe::Remote(rs.config.clone()),
                };
                (id.clone(), probe)
            })
            .collect();
        // Remote probes run concurrently (each bounded by [`PROBE_TIMEOUT`]),
        // so slow endpoints cannot serialize the sweep and starve stdio pings.
        let mut remote_probes = tokio::task::JoinSet::new();
        for (id, probe) in targets {
            let conn = match probe {
                Probe::Remote(config) => {
                    let hub = self.clone();
                    remote_probes.spawn(async move { hub.reprobe_remote(&id, &config).await });
                    continue;
                }
                Probe::Stdio(conn) => conn,
            };
            if ping(&conn).await {
                if let Some(rs) = self.inner.servers.lock().unwrap().get_mut(&id) {
                    rs.failures = 0;
                }
                continue;
            }
            let (failures, config) = {
                let mut map = self.inner.servers.lock().unwrap();
                match map.get_mut(&id) {
                    Some(rs) => {
                        rs.failures += 1;
                        (rs.failures, rs.config.clone())
                    }
                    None => continue,
                }
            };
            if failures >= MAX_FAILURES {
                tracing::warn!(server = %id, "mcp server unhealthy; restarting");
                self.restart(config, true).await;
            }
        }
        while remote_probes.join_next().await.is_some() {}
    }

    /// Re-probe a remote server and store the fresh status, emitting
    /// `mcp.servers:status-changed` on a state transition. `startedAt` is
    /// preserved across consecutive `running` probes.
    async fn reprobe_remote(&self, id: &str, config: &Value) {
        let mut next = remote_probe_status(id, config).await;
        let changed = {
            let mut map = self.inner.servers.lock().unwrap();
            // The entry may have been stopped or replaced while the probe ran.
            let Some(rs) = map.get_mut(id) else { return };
            if !matches!(rs.runtime, ServerRuntime::Remote) || rs.config != *config {
                return;
            }
            let changed = rs.status.get("state") != next.get("state");
            if !changed {
                if let (Some(prev), Some(obj)) =
                    (rs.status.get("startedAt").cloned(), next.as_object_mut())
                {
                    obj.insert("startedAt".into(), prev);
                }
            }
            rs.status = next.clone();
            changed
        };
        if changed {
            self.publish_status(&next).await;
        }
    }

    /// Spawn the periodic health-monitor loop (ping + auto-restart). The first
    /// sweep runs after one interval; missed ticks are skipped.
    pub fn spawn_health_monitor(&self) -> tokio::task::JoinHandle<()> {
        let hub = self.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(HEALTH_INTERVAL);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                hub.health_tick().await;
            }
        })
    }

    /// Reap every running server (clean daemon shutdown — no orphans, mirroring
    /// the PTY-host process-group reaping).
    pub async fn shutdown(&self) {
        let victims: Vec<RunningServer> = {
            let mut map = self.inner.servers.lock().unwrap();
            map.drain().map(|(_, rs)| rs).collect()
        };
        for mut rs in victims {
            reap(&mut rs).await;
        }
    }
}

/// Spawn a stdio MCP server child and complete the MCP handshake, returning the
/// child, its pid, the live [`Connection`], and the advertised tool count.
async fn spawn_stdio(config: &Value) -> Result<(Child, Option<u32>, Connection, Option<u64>)> {
    let transport = config
        .get("transport")
        .and_then(Value::as_str)
        .unwrap_or("stdio");
    if transport != "stdio" {
        return Err(Error::InvalidParams(format!(
            "transport {transport} not supported (stdio only)"
        )));
    }
    let command = config
        .get("command")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Error::InvalidParams("stdio server requires a command".to_string()))?;
    let args: Vec<String> = config
        .get("args")
        .and_then(Value::as_array)
        .map(|a| {
            a.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default();
    let mut cmd = Command::new(command);
    cmd.args(&args);
    if let Some(env) = config.get("env").and_then(Value::as_object) {
        for (k, v) in env {
            let val = match v {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            cmd.env(k, val);
        }
    }
    cmd.stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    // Own process group so reaping signals the whole tree (no orphans, §5.6).
    #[cfg(unix)]
    cmd.process_group(0);
    let mut child = cmd
        .spawn()
        .map_err(|e| Error::Internal(format!("{command}: {e}")))?;
    let pid = child.id();
    let stdin = child
        .stdin
        .take()
        .ok_or_else(|| Error::Internal("mcp server stdin not piped".to_string()))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| Error::Internal("mcp server stdout not piped".to_string()))?;
    let stderr = child
        .stderr
        .take()
        .map(|s| Box::new(s) as Box<dyn AsyncRead + Unpin + Send>);
    let conn = Connection::new(stdin, stdout, stderr, ConnectionHooks::default());
    let tool_count = mcp_handshake(&conn).await?;
    Ok((child, pid, conn, tool_count))
}

/// Run the MCP `initialize` → `notifications/initialized` → `tools/list`
/// handshake, returning the advertised tool count (best-effort).
async fn mcp_handshake(conn: &Connection) -> Result<Option<u64>> {
    let init = json!({
        "protocolVersion": MCP_PROTOCOL_VERSION,
        "capabilities": {},
        "clientInfo": { "name": "intentd", "version": env!("CARGO_PKG_VERSION") },
    });
    conn.request_timeout("initialize", init, HANDSHAKE_TIMEOUT)
        .await
        .map_err(|e| Error::Internal(format!("mcp initialize failed: {e}")))?;
    let _ = conn.notify("notifications/initialized", json!({})).await;
    let tool_count = match conn
        .request_timeout("tools/list", json!({}), HANDSHAKE_TIMEOUT)
        .await
    {
        Ok(v) => v
            .get("tools")
            .and_then(Value::as_array)
            .map(|a| a.len() as u64),
        Err(_) => None,
    };
    Ok(tool_count)
}

/// A health `ping`: true iff the server answers within [`PING_TIMEOUT`].
async fn ping(conn: &Connection) -> bool {
    conn.request_timeout("ping", json!({}), PING_TIMEOUT)
        .await
        .is_ok()
}

/// Probe `config` and shape the outcome as a wire `McpServerStatus` (§5.22).
async fn remote_probe_status(id: &str, config: &Value) -> Value {
    match probe_remote(config).await {
        Ok(tool_count) => status_value(id, "running", None, tool_count, None, Some(now_millis())),
        Err(e) => status_error(id, &e.to_string()),
    }
}

/// Probe a remote MCP endpoint from the daemon host. `http` runs the full MCP
/// handshake (`initialize` → `notifications/initialized` → `tools/list`) over
/// streamable HTTP POST; `sse` is a reachability probe only (full SSE sessions
/// are out of scope). `Ok` carries the advertised tool count (http only).
/// The whole probe is bounded by [`PROBE_TIMEOUT`] on top of the per-request
/// [`HANDSHAKE_TIMEOUT`]. Redirects are never followed: configured headers may
/// carry credentials that reqwest would forward to a cross-host redirect.
async fn probe_remote(config: &Value) -> Result<Option<u64>> {
    let transport = config
        .get("transport")
        .and_then(Value::as_str)
        .unwrap_or("stdio");
    let url = config
        .get("url")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Error::InvalidParams(format!("{transport} server requires a url")))?
        .to_string();
    let client = reqwest::Client::builder()
        .timeout(HANDSHAKE_TIMEOUT)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .map_err(|e| Error::Internal(format!("http client init failed: {e}")))?;
    let headers = config_headers(config);
    let probe = async move {
        match transport {
            "sse" => probe_sse(&client, &url, &headers).await.map(|()| None),
            _ => probe_http_handshake(&client, &url, &headers).await,
        }
    };
    tokio::time::timeout(PROBE_TIMEOUT, probe)
        .await
        .map_err(|_| Error::Internal("probe timed out".to_string()))?
}

/// The configured request headers of a remote config (`headers` object;
/// non-string values are serialized, mirroring the stdio `env` handling).
fn config_headers(config: &Value) -> Vec<(String, String)> {
    config
        .get("headers")
        .and_then(Value::as_object)
        .map(|obj| {
            obj.iter()
                .map(|(k, v)| {
                    let val = match v {
                        Value::String(s) => s.clone(),
                        other => other.to_string(),
                    };
                    (k.clone(), val)
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Reachability probe for an `sse` endpoint: a GET with
/// `Accept: text/event-stream` must answer 2xx. The stream body is never read.
async fn probe_sse(
    client: &reqwest::Client,
    url: &str,
    headers: &[(String, String)],
) -> Result<()> {
    let mut req = client
        .get(url)
        .header(reqwest::header::ACCEPT, "text/event-stream");
    for (k, v) in headers {
        req = req.header(k.as_str(), v.as_str());
    }
    let resp = req.send().await.map_err(|e| classify_send_error(e, url))?;
    check_http_status(resp.status())
}

/// MCP handshake over streamable HTTP POST, mirroring the stdio handshake:
/// `initialize` (required) → `notifications/initialized` (best-effort) →
/// `tools/list` (best-effort tool count). The `Mcp-Session-Id` issued by
/// `initialize` is echoed on follow-ups per the streamable-HTTP transport,
/// and the session is torn down with a best-effort `DELETE` afterwards so
/// periodic re-probes don't accumulate orphaned sessions server-side.
async fn probe_http_handshake(
    client: &reqwest::Client,
    url: &str,
    headers: &[(String, String)],
) -> Result<Option<u64>> {
    let init = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "initialize",
        "params": {
            "protocolVersion": MCP_HTTP_PROTOCOL_VERSION,
            "capabilities": {},
            "clientInfo": { "name": "intentd", "version": env!("CARGO_PKG_VERSION") },
        },
    });
    let resp = post_rpc(client, url, headers, None, None, &init).await?;
    check_http_status(resp.status())?;
    let session = resp
        .headers()
        .get("mcp-session-id")
        .and_then(|v| v.to_str().ok())
        .map(String::from);
    let envelope = read_rpc_response(resp, 1).await?;
    let result = validate_rpc_result(&envelope, 1, "initialize")?;
    // Revisions ≥2025-06-18 expect the negotiated version echoed back as an
    // `MCP-Protocol-Version` header on subsequent HTTP requests.
    let proto = result
        .get("protocolVersion")
        .and_then(Value::as_str)
        .map(String::from);
    let sid = session.as_deref();
    let ver = proto.as_deref();
    // Notification (servers typically answer 202); failures are non-fatal.
    let inited = json!({ "jsonrpc": "2.0", "method": "notifications/initialized" });
    let _ = post_rpc(client, url, headers, sid, ver, &inited).await;
    let tools = json!({ "jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {} });
    let tool_count = match post_rpc(client, url, headers, sid, ver, &tools).await {
        Ok(resp) if resp.status().is_success() => {
            read_rpc_response(resp, 2).await.ok().and_then(|v| {
                v.get("result")
                    .and_then(|r| r.get("tools"))
                    .and_then(Value::as_array)
                    .map(|a| a.len() as u64)
            })
        }
        _ => None,
    };
    // Best-effort session teardown (spec: clients SHOULD send HTTP DELETE).
    if let Some(sid) = sid {
        let mut req = client.delete(url).header("Mcp-Session-Id", sid);
        for (k, v) in headers {
            req = req.header(k.as_str(), v.as_str());
        }
        if let Some(ver) = ver {
            req = req.header("MCP-Protocol-Version", ver);
        }
        let _ = req.send().await;
    }
    Ok(tool_count)
}

/// Validate a JSON-RPC 2.0 response envelope: `jsonrpc: "2.0"`, matching `id`,
/// no `error`, and a `result` object present. Returns the `result`. This is
/// what keeps a non-MCP JSON endpoint from being reported as `running`.
fn validate_rpc_result<'a>(envelope: &'a Value, id: u64, method: &str) -> Result<&'a Value> {
    if let Some(err) = envelope.get("error") {
        return Err(Error::Internal(format!("mcp {method} failed: {err}")));
    }
    if envelope.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || envelope.get("id").and_then(Value::as_u64) != Some(id)
    {
        return Err(Error::Internal(format!(
            "mcp {method} failed: not a JSON-RPC 2.0 response"
        )));
    }
    envelope
        .get("result")
        .ok_or_else(|| Error::Internal(format!("mcp {method} failed: response carries no result")))
}

/// POST one JSON-RPC message to a streamable-HTTP endpoint with the configured
/// headers (plus the session id / negotiated protocol version when known).
async fn post_rpc(
    client: &reqwest::Client,
    url: &str,
    headers: &[(String, String)],
    session: Option<&str>,
    protocol_version: Option<&str>,
    body: &Value,
) -> Result<reqwest::Response> {
    let mut req = client
        .post(url)
        .header(reqwest::header::ACCEPT, HTTP_ACCEPT)
        .json(body);
    for (k, v) in headers {
        req = req.header(k.as_str(), v.as_str());
    }
    if let Some(sid) = session {
        req = req.header("Mcp-Session-Id", sid);
    }
    if let Some(ver) = protocol_version {
        req = req.header("MCP-Protocol-Version", ver);
    }
    req.send().await.map_err(|e| classify_send_error(e, url))
}

/// Read a JSON-RPC response envelope from a streamable-HTTP reply: a JSON body
/// directly, or the SSE frame carrying the response with `id`. The per-request
/// SSE stream closes once the response is delivered, and the read is bounded
/// by the client-wide [`HANDSHAKE_TIMEOUT`] either way.
async fn read_rpc_response(mut resp: reqwest::Response, id: u64) -> Result<Value> {
    let ct = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_string();
    if !ct.starts_with("text/event-stream") {
        let text = resp
            .text()
            .await
            .map_err(|e| Error::Internal(format!("read response failed: {e}")))?;
        return serde_json::from_str(&text)
            .map_err(|e| Error::Internal(format!("invalid JSON-RPC response: {e}")));
    }
    let mut buf = String::new();
    loop {
        match resp.chunk().await {
            Ok(Some(chunk)) => {
                buf.push_str(&String::from_utf8_lossy(&chunk));
                if let Some(v) = sse_response_for_id(&buf, id) {
                    return Ok(v);
                }
            }
            Ok(None) => break,
            Err(e) => return Err(Error::Internal(format!("read response failed: {e}"))),
        }
    }
    sse_response_for_id(&buf, id)
        .ok_or_else(|| Error::Internal("no JSON-RPC response in SSE stream".to_string()))
}

/// Find the JSON-RPC envelope with `id` among the `data:` lines of an SSE
/// buffer (a response fits one data line in practice).
fn sse_response_for_id(buf: &str, id: u64) -> Option<Value> {
    for line in buf.lines() {
        let Some(data) = line.strip_prefix("data:") else {
            continue;
        };
        let Ok(v) = serde_json::from_str::<Value>(data.trim()) else {
            continue;
        };
        if v.get("id").and_then(Value::as_u64) == Some(id) {
            return Some(v);
        }
    }
    None
}

/// Map a transport-level reqwest failure onto a user-facing `lastError`.
fn classify_send_error(e: reqwest::Error, url: &str) -> Error {
    if e.is_timeout() {
        Error::Internal(format!("timed out connecting to {url}"))
    } else if e.is_connect() {
        Error::Internal(format!("unreachable from daemon host: {url}"))
    } else {
        Error::Internal(format!("request to {url} failed: {e}"))
    }
}

/// Map a non-success HTTP status onto a user-facing `lastError`.
fn check_http_status(status: reqwest::StatusCode) -> Result<()> {
    if status.is_success() {
        return Ok(());
    }
    let code = status.as_u16();
    Err(match code {
        401 | 403 => Error::Internal(format!(
            "authentication failed (HTTP {code}) — check configured headers"
        )),
        500..=599 => Error::Internal(format!("server error (HTTP {code})")),
        _ => Error::Internal(format!("unexpected HTTP {code} from server")),
    })
}

/// Terminate a stdio server's whole process group (SIGTERM → grace → SIGKILL),
/// then let the [`Connection`] drop to abort its reader/writer tasks.
/// Descendants that escaped into their OWN process groups survive the group
/// kill, so they are snapshotted before signalling and swept afterwards
/// (`intent_acp::descendant_sweep`). Remote entries have no process — no-op.
async fn reap(rs: &mut RunningServer) {
    let ServerRuntime::Stdio { child, pid, .. } = &mut rs.runtime else {
        return;
    };
    #[cfg(unix)]
    {
        if let Some(pid) = *pid {
            let descendants = intent_acp::descendant_pids(pid).await;
            let _ = kill_group(pid, nix::sys::signal::Signal::SIGTERM);
            let mut exited = false;
            let iters = (TERM_GRACE.as_millis() / REAP_POLL.as_millis()).max(1);
            for _ in 0..iters {
                if matches!(child.try_wait(), Ok(Some(_))) {
                    exited = true;
                    break;
                }
                tokio::time::sleep(REAP_POLL).await;
            }
            if !exited {
                let _ = kill_group(pid, nix::sys::signal::Signal::SIGKILL);
            }
            intent_acp::sweep_escaped_descendants(&descendants).await;
        } else {
            let _ = child.kill().await;
        }
    }
    #[cfg(not(unix))]
    {
        let _ = pid;
        let _ = child.kill().await;
    }
}

/// Signal a whole process group by its leader pid (pgid == pid via `process_group`).
#[cfg(unix)]
fn kill_group(
    pid: u32,
    sig: nix::sys::signal::Signal,
) -> std::result::Result<(), nix::errno::Errno> {
    use nix::sys::signal::killpg;
    use nix::unistd::Pid;
    killpg(Pid::from_raw(pid as i32), sig)
}

/// Stateless executor for the `mcp.servers.*` namespace (PROTOCOL §5.22) over
/// the settings registry (`mcp.enableUserServers`/`mcp.disabledServers`), the
/// [`AsyncSecretStore`] (the sensitive `mcp.servers` config), and the runtime
/// [`McpHub`]. Construct one per call from the long-lived `Services`.
pub(crate) struct McpServersService<'a> {
    registry: Option<&'a SettingsRegistry>,
    secrets: &'a AsyncSecretStore,
    hub: &'a McpHub,
}

impl<'a> McpServersService<'a> {
    pub(crate) fn new(
        registry: Option<&'a SettingsRegistry>,
        secrets: &'a AsyncSecretStore,
        hub: &'a McpHub,
    ) -> Self {
        Self {
            registry,
            secrets,
            hub,
        }
    }

    /// The effective typed settings; schema defaults when no registry is wired.
    fn effective(&self) -> SettingsFile {
        self.registry
            .map(|r| r.snapshot().effective.clone())
            .unwrap_or_default()
    }

    /// Fetch one stored config by id, or `NotFound`.
    async fn require_config(&self, server_id: &str) -> Result<Value> {
        read_configs(self.secrets)
            .await
            .remove(server_id)
            .ok_or_else(|| Error::NotFound(format!("mcp server not found: {server_id}")))
    }

    /// `mcp.servers.list` → `{ servers: McpServerConfig[] }` (env/headers redacted),
    /// sorted by id for a stable wire order.
    pub(crate) async fn list(&self, _workspace_id: Option<&str>) -> Result<Value> {
        let configs = read_configs(self.secrets).await;
        let mut servers: Vec<Value> = configs.values().map(redact_config).collect();
        servers.sort_by_key(config_id);
        Ok(json!({ "servers": servers }))
    }

    /// `mcp.servers.create` → persist a new definition; `{ server }` (redacted).
    pub(crate) async fn create(&self, config: Value) -> Result<Value> {
        let normalized = normalize_config(config, None)?;
        let id = config_id(&normalized);
        let mut configs = read_configs(self.secrets).await;
        if configs.contains_key(&id) {
            return Err(Error::InvalidParams(format!(
                "mcp server already exists: {id}"
            )));
        }
        configs.insert(id, normalized.clone());
        write_configs(self.secrets, &configs).await?;
        Ok(json!({ "server": redact_config(&normalized) }))
    }

    /// `mcp.servers.update` → replace an existing definition; `{ server }`
    /// (redacted). A running server is restarted to apply the new config.
    pub(crate) async fn update(&self, server_id: &str, config: Value) -> Result<Value> {
        let mut configs = read_configs(self.secrets).await;
        if !configs.contains_key(server_id) {
            return Err(Error::NotFound(format!(
                "mcp server not found: {server_id}"
            )));
        }
        let normalized = normalize_config(config, Some(server_id))?;
        configs.insert(server_id.to_string(), normalized.clone());
        write_configs(self.secrets, &configs).await?;
        // Apply live: any tracked server (running, or a remote in `error`)
        // picks up the new definition on restart — an error-state remote must
        // re-probe the updated URL/headers, not keep probing the old config.
        let tracked = self.hub.status(server_id)["state"] != "stopped";
        if tracked {
            let enable = enable_user_servers(&self.effective());
            self.hub.restart(normalized.clone(), enable).await;
        }
        Ok(json!({ "server": redact_config(&normalized) }))
    }

    /// `mcp.servers.delete` → stop + remove a definition; `{ success: true }`.
    pub(crate) async fn delete(&self, server_id: &str) -> Result<Value> {
        let mut configs = read_configs(self.secrets).await;
        if configs.remove(server_id).is_none() {
            return Err(Error::NotFound(format!(
                "mcp server not found: {server_id}"
            )));
        }
        self.hub.stop(server_id).await;
        write_configs(self.secrets, &configs).await?;
        Ok(json!({ "success": true }))
    }

    /// `mcp.servers.toggle` → enable (start) / disable (stop). Updates the config's
    /// `enabled` flag + `mcp.disabledServers`, drives the lifecycle, and returns
    /// `{ status }`.
    pub(crate) async fn toggle(&self, server_id: &str, enabled: bool) -> Result<Value> {
        let mut config = self.require_config(server_id).await?;
        if let Some(obj) = config.as_object_mut() {
            obj.insert("enabled".into(), json!(enabled));
        }
        let mut configs = read_configs(self.secrets).await;
        configs.insert(server_id.to_string(), config.clone());
        write_configs(self.secrets, &configs).await?;

        let settings = self.effective();
        let mut disabled = disabled_servers(&settings);
        let was_present = disabled.iter().any(|d| d == server_id);
        if enabled {
            disabled.retain(|d| d != server_id);
        } else if !was_present {
            disabled.push(server_id.to_string());
        }
        set_disabled_servers(self.registry, &disabled)?;

        let status = if enabled {
            let enable = enable_user_servers(&settings);
            self.hub.start(config, enable).await
        } else {
            self.hub.stop(server_id).await;
            status_stopped(server_id)
        };
        Ok(json!({ "status": status }))
    }

    /// `mcp.servers.restart` → stop-then-start; `{ status }`.
    pub(crate) async fn restart(&self, server_id: &str) -> Result<Value> {
        let config = self.require_config(server_id).await?;
        let enable = enable_user_servers(&self.effective());
        let status = self.hub.restart(config, enable).await;
        Ok(json!({ "status": status }))
    }

    /// `mcp.servers.getStatus` → live status point read; `{ status }`.
    pub(crate) async fn get_status(&self, server_id: &str) -> Result<Value> {
        // Surface NotFound for an unknown id; otherwise the live runtime status.
        self.require_config(server_id).await?;
        Ok(json!({ "status": self.hub.status(server_id) }))
    }

    /// Start every enabled, non-disabled server (daemon boot). Best-effort: a
    /// failed spawn surfaces as an `error` status event, not a hard failure.
    ///
    /// The sweep runs in the background off the daemon's bind path, so
    /// `mcp.servers.*` RPCs can mutate definitions while a handshake is in
    /// flight. Eligibility is therefore re-read from the live settings/config
    /// immediately before each start, and re-checked after it, so a server the
    /// user deleted/disabled/updated mid-sweep is never left running from this
    /// task's stale snapshot.
    pub(crate) async fn start_enabled(&self) {
        if !enable_user_servers(&self.effective()) {
            return;
        }
        let ids: Vec<String> = read_configs(self.secrets)
            .await
            .keys()
            .map(String::from)
            .collect();
        for id in ids {
            let Some(config) = self.eligible_config(&id).await else {
                continue;
            };
            self.hub.start(config.clone(), true).await;
            // A mutation that landed during the handshake found no hub entry to
            // stop or restart, so reconcile it here.
            match self.eligible_config(&id).await {
                // Deleted or disabled mid-handshake → tear the server back down.
                None => {
                    self.hub.stop(&id).await;
                }
                // Redefined mid-handshake → replace with the live definition.
                Some(current) if current != config => {
                    self.hub.start(current, true).await;
                }
                Some(_) => {}
            }
        }
    }

    /// The current definition for `id` if it is still boot-eligible against the
    /// **live** settings and stored configs: the gate is on, the definition
    /// exists with `enabled: true`, and the id is not in `mcp.disabledServers`.
    async fn eligible_config(&self, id: &str) -> Option<Value> {
        let settings = self.effective();
        if !enable_user_servers(&settings) || disabled_servers(&settings).iter().any(|d| d == id) {
            return None;
        }
        let config = read_configs(self.secrets).await.remove(id)?;
        config
            .get("enabled")
            .and_then(Value::as_bool)
            .unwrap_or(false)
            .then_some(config)
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::time::Duration;

    use intent_store::Store;
    use serde_json::{json, Value};

    use super::*;
    use crate::events::{EventBus, SubscriptionFilter};
    use crate::settings::{InMemorySecretStore, SecretStore};

    /// Wrap an [`InMemorySecretStore`] in an [`AsyncSecretStore`] for tests, so
    /// helper call sites read like `mem_async()` without repeating the
    /// `Arc<dyn SecretStore>` scaffolding.
    fn mem_async() -> AsyncSecretStore {
        let inner: Arc<dyn SecretStore> = Arc::new(InMemorySecretStore::default());
        AsyncSecretStore::new(inner)
    }

    /// Self-cleaning sqlite path so the tests never share Store state.
    struct TempDb {
        path: PathBuf,
    }
    impl TempDb {
        fn new() -> Self {
            Self {
                path: std::env::temp_dir().join(format!("intentd-mcp-srv-{}.db", Uuid::new_v4())),
            }
        }
    }
    impl Drop for TempDb {
        fn drop(&mut self) {
            for suffix in ["", "-wal", "-shm"] {
                let _ =
                    std::fs::remove_file(PathBuf::from(format!("{}{suffix}", self.path.display())));
            }
        }
    }

    async fn open_store() -> (TempDb, Store) {
        let tmp = TempDb::new();
        let store = Store::open(&tmp.path).await.expect("open store");
        (tmp, store)
    }

    /// A command guaranteed not to exist so `Command::spawn` fails synchronously
    /// (keeps the `start`/`spawn_stdio` error paths fast and deterministic).
    const BOGUS_CMD: &str = "/this/does/not/exist/intentd-mcp-test-cmd";

    fn stdio_cfg(id: &str, command: &str) -> Value {
        json!({
            "id": id,
            "name": id,
            "transport": "stdio",
            "command": command,
            "enabled": true,
        })
    }

    // -- pure helpers ------------------------------------------------------

    #[test]
    fn now_millis_is_monotonic_enough() {
        let a = now_millis();
        let b = now_millis();
        assert!(b >= a, "now_millis must not go backwards");
    }

    #[test]
    fn status_value_omits_absent_optional_fields() {
        let v = status_value("s", "running", None, None, None, None);
        let obj = v.as_object().unwrap();
        assert_eq!(obj.get("serverId"), Some(&json!("s")));
        assert_eq!(obj.get("state"), Some(&json!("running")));
        assert!(obj.get("pid").is_none());
        assert!(obj.get("toolCount").is_none());
        assert!(obj.get("lastError").is_none());
        assert!(obj.get("startedAt").is_none());
    }

    #[test]
    fn status_value_includes_all_optionals_when_present() {
        let v = status_value("s", "running", Some(42), Some(3), Some("err"), Some(123));
        assert_eq!(v["pid"], json!(42));
        assert_eq!(v["toolCount"], json!(3));
        assert_eq!(v["lastError"], json!("err"));
        assert_eq!(v["startedAt"], json!(123));
    }

    #[test]
    fn status_stopped_is_minimal() {
        let v = status_stopped("abc");
        assert_eq!(v["serverId"], json!("abc"));
        assert_eq!(v["state"], json!("stopped"));
        assert_eq!(v.as_object().unwrap().len(), 2);
    }

    #[test]
    fn status_error_carries_last_error() {
        let v = status_error("abc", "boom");
        assert_eq!(v["state"], json!("error"));
        assert_eq!(v["lastError"], json!("boom"));
    }

    #[test]
    fn config_id_absent_is_empty_string() {
        assert_eq!(config_id(&json!({})), "");
        assert_eq!(config_id(&json!({"id": 7})), ""); // non-string id is ignored
    }

    #[test]
    fn config_id_returns_string_id() {
        assert_eq!(config_id(&json!({"id": "srv-1"})), "srv-1");
    }

    #[test]
    fn redact_config_replaces_env_and_headers_values() {
        let c = json!({
            "id": "s",
            "command": "node",
            "env": { "TOKEN": "supersecret", "OTHER": "value" },
            "headers": { "Authorization": "Bearer xyz" },
        });
        let r = redact_config(&c);
        assert_eq!(r["id"], json!("s"));
        assert_eq!(r["command"], json!("node"));
        for (_, v) in r["env"].as_object().unwrap() {
            assert_eq!(v, &json!(REDACTED_PLACEHOLDER));
        }
        for (_, v) in r["headers"].as_object().unwrap() {
            assert_eq!(v, &json!(REDACTED_PLACEHOLDER));
        }
        // Original is untouched.
        assert_eq!(c["env"]["TOKEN"], json!("supersecret"));
    }

    #[test]
    fn redact_config_noop_without_env_or_headers() {
        let c = json!({ "id": "s", "command": "x" });
        assert_eq!(redact_config(&c), c);
    }

    #[test]
    fn redact_config_skips_non_object_env_and_headers() {
        // env/headers must be objects to be redacted; non-object values are passthrough.
        let c = json!({ "env": "stringy", "headers": ["a"] });
        let r = redact_config(&c);
        assert_eq!(r, c);
    }

    // -- normalize_config branches ----------------------------------------

    #[test]
    fn normalize_config_rejects_non_object() {
        let err = normalize_config(json!("nope"), None).unwrap_err();
        assert!(matches!(err, Error::InvalidParams(_)));
    }

    #[test]
    fn normalize_config_generates_id_and_fills_defaults() {
        let v = normalize_config(json!({ "command": "x" }), None).unwrap();
        let id = v["id"].as_str().unwrap();
        assert!(id.starts_with("srv-") && id.len() > 4);
        assert_eq!(v["transport"], json!("stdio"));
        assert_eq!(v["name"], json!(id), "name defaults to id when absent");
        assert_eq!(v["enabled"], json!(false));
    }

    #[test]
    fn normalize_config_preserves_existing_id_name_enabled() {
        let v = normalize_config(
            json!({ "id": "keep", "name": "n", "enabled": true, "command": "x" }),
            None,
        )
        .unwrap();
        assert_eq!(v["id"], json!("keep"));
        assert_eq!(v["name"], json!("n"));
        assert_eq!(v["enabled"], json!(true));
    }

    #[test]
    fn normalize_config_forced_id_overrides_payload_id() {
        let v =
            normalize_config(json!({ "id": "ignored", "command": "x" }), Some("forced")).unwrap();
        assert_eq!(v["id"], json!("forced"));
    }

    #[test]
    fn normalize_config_empty_id_is_replaced() {
        let v = normalize_config(json!({ "id": "", "command": "x" }), None).unwrap();
        assert!(v["id"].as_str().unwrap().starts_with("srv-"));
    }

    #[test]
    fn normalize_config_rejects_unknown_transport() {
        let err = normalize_config(json!({ "transport": "carrier-pigeon" }), None).unwrap_err();
        let msg = format!("{err}");
        assert!(msg.contains("invalid transport"), "got: {msg}");
    }

    #[test]
    fn normalize_config_stdio_requires_non_empty_command() {
        let err = normalize_config(json!({}), None).unwrap_err();
        assert!(format!("{err}").contains("stdio server requires"));
        let err = normalize_config(json!({ "command": "" }), None).unwrap_err();
        assert!(format!("{err}").contains("stdio server requires"));
    }

    #[test]
    fn normalize_config_http_and_sse_require_url() {
        for t in ["http", "sse"] {
            let err = normalize_config(json!({ "transport": t }), None).unwrap_err();
            assert!(format!("{err}").contains("requires a url"), "{t}");
        }
        // happy path: url present → ok
        let v = normalize_config(json!({ "transport": "http", "url": "https://x" }), None).unwrap();
        assert_eq!(v["transport"], json!("http"));
    }

    // -- secret / setting accessors ---------------------------------------

    #[tokio::test]
    async fn read_configs_missing_returns_empty_map() {
        let s = mem_async();
        assert!(read_configs(&s).await.is_empty());
    }

    #[tokio::test]
    async fn read_configs_garbled_returns_empty_map() {
        let s = mem_async();
        s.store(SETTING_KEY, "this is not json").await.unwrap();
        assert!(read_configs(&s).await.is_empty());
    }

    #[tokio::test]
    async fn read_configs_non_object_returns_empty_map() {
        let s = mem_async();
        s.store(SETTING_KEY, "[1,2,3]").await.unwrap();
        assert!(read_configs(&s).await.is_empty());
    }

    #[tokio::test]
    async fn write_then_read_configs_round_trips() {
        let s = mem_async();
        let mut m = Map::new();
        m.insert("a".into(), json!({"id":"a","command":"x"}));
        write_configs(&s, &m).await.unwrap();
        let got = read_configs(&s).await;
        assert_eq!(got, m);
    }

    /// Fresh registry over a `config.toml` in a self-cleaning temp dir.
    fn temp_registry() -> (SettingsRegistry, tempfile::TempDir) {
        let dir = tempfile::tempdir().expect("temp config dir");
        let reg = SettingsRegistry::load(dir.path().join("config.toml")).expect("load registry");
        (reg, dir)
    }

    #[test]
    fn enable_user_servers_defaults_true_without_setting() {
        assert!(enable_user_servers(&SettingsFile::default()));
    }

    #[test]
    fn enable_user_servers_reads_false() {
        let (reg, _cfg) = temp_registry();
        reg.apply(&[("mcp.enableUserServers".to_string(), json!(false))])
            .unwrap();
        assert!(!enable_user_servers(&reg.snapshot().effective));
    }

    #[test]
    fn disabled_servers_empty_without_setting() {
        assert!(disabled_servers(&SettingsFile::default()).is_empty());
    }

    #[test]
    fn disabled_servers_round_trip_via_setter() {
        let (reg, _cfg) = temp_registry();
        let list = vec!["a".to_string(), "b".to_string()];
        set_disabled_servers(Some(&reg), &list).unwrap();
        let got = disabled_servers(&reg.snapshot().effective);
        assert_eq!(got, list);
    }

    #[test]
    fn set_disabled_servers_without_registry_is_noop() {
        set_disabled_servers(None, &["a".to_string()]).unwrap();
    }

    // -- McpHub: non-spawning lifecycle -----------------------------------

    #[test]
    fn hub_default_constructs_empty_hub() {
        let h = McpHub::default();
        assert_eq!(h.status("anything"), status_stopped("anything"));
    }

    #[tokio::test]
    async fn hub_status_unknown_id_is_stopped() {
        let h = McpHub::new();
        assert_eq!(h.status("ghost"), status_stopped("ghost"));
    }

    #[tokio::test]
    async fn hub_stop_unknown_id_returns_false() {
        let h = McpHub::new();
        assert!(!h.stop("nope").await);
    }

    #[tokio::test]
    async fn hub_start_with_user_servers_disabled_emits_stopped_no_spawn() {
        let h = McpHub::new();
        let status = h.start(stdio_cfg("s1", BOGUS_CMD), false).await;
        assert_eq!(status, status_stopped("s1"));
        // Server is never registered when the gate is off.
        assert_eq!(h.status("s1"), status_stopped("s1"));
    }

    #[tokio::test]
    async fn hub_start_spawn_failure_returns_error_status() {
        let h = McpHub::new();
        let status = h.start(stdio_cfg("err", BOGUS_CMD), true).await;
        assert_eq!(status["state"], json!("error"));
        let msg = status["lastError"].as_str().unwrap();
        assert!(
            msg.contains(BOGUS_CMD),
            "lastError should mention command: {msg}"
        );
        // Failed spawn does not insert into the live map.
        assert_eq!(h.status("err"), status_stopped("err"));
    }

    #[tokio::test]
    async fn hub_start_non_stdio_transport_probes_and_stays_tracked_on_error() {
        // Non-stdio configs are probed, not rejected. A failed probe keeps the
        // entry tracked in `error` (the sweep re-probes it) — unlike a failed
        // stdio spawn, which drops the entry back to `stopped`.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);

        let h = McpHub::new();
        let cfg = json!({ "id": "h", "transport": "http", "url": url });
        let status = h.start(cfg, true).await;
        assert_eq!(status["state"], json!("error"));
        assert_eq!(h.status("h")["state"], json!("error"));
    }

    #[tokio::test]
    async fn hub_start_missing_command_returns_error_status() {
        let h = McpHub::new();
        let cfg = json!({ "id": "h", "transport": "stdio" });
        let status = h.start(cfg, true).await;
        assert_eq!(status["state"], json!("error"));
        assert!(status["lastError"]
            .as_str()
            .unwrap()
            .contains("requires a command"));
    }

    #[tokio::test]
    async fn hub_restart_with_disabled_gate_returns_stopped() {
        let h = McpHub::new();
        let status = h.restart(stdio_cfg("r1", BOGUS_CMD), false).await;
        assert_eq!(status, status_stopped("r1"));
    }

    #[tokio::test]
    async fn hub_shutdown_empty_is_noop() {
        let h = McpHub::new();
        h.shutdown().await;
    }

    #[tokio::test]
    async fn hub_health_tick_empty_is_noop() {
        let h = McpHub::new();
        h.health_tick().await;
    }

    #[tokio::test]
    async fn hub_publish_status_without_bus_is_noop() {
        let h = McpHub::new();
        // Should simply return without panicking when no bus is wired.
        h.publish_status(&status_stopped("x")).await;
    }

    #[tokio::test]
    async fn hub_publish_status_emits_event_when_bus_is_wired() {
        let (_tmp, store) = open_store().await;
        let bus = EventBus::new(store);
        let mut sub = bus.subscribe(SubscriptionFilter::default());
        let h = McpHub::new();
        h.set_event_bus(bus);

        let status = status_value("s1", "running", Some(7), Some(2), None, Some(99));
        h.publish_status(&status).await;

        let batch = tokio::time::timeout(Duration::from_secs(2), sub.recv())
            .await
            .expect("event delivered")
            .expect("subscription open");
        assert_eq!(batch.len(), 1);
        let ev = &batch[0];
        assert_eq!(ev.event_type, MCP_SERVERS_STATUS_CHANGED);
        let data = serde_json::to_value(&ev.data).unwrap();
        assert_eq!(data["serverId"], json!("s1"));
        assert_eq!(data["status"]["state"], json!("running"));
        assert_eq!(data["status"]["pid"], json!(7));
    }

    // -- McpServersService -------------------------------------------------

    fn svc<'a>(
        registry: Option<&'a SettingsRegistry>,
        secrets: &'a AsyncSecretStore,
        hub: &'a McpHub,
    ) -> McpServersService<'a> {
        McpServersService::new(registry, secrets, hub)
    }

    #[tokio::test]
    async fn list_empty_when_no_configs() {
        let s = mem_async();
        let h = McpHub::new();
        let r = svc(None, &s, &h).list(None).await.unwrap();
        assert_eq!(r["servers"].as_array().unwrap().len(), 0);
    }

    #[tokio::test]
    async fn list_sorted_by_id_and_redacts_secrets() {
        let secrets = mem_async();
        let h = McpHub::new();
        let mut m = Map::new();
        m.insert(
            "b".into(),
            json!({"id":"b","command":"x","env":{"K":"VAL"}}),
        );
        m.insert(
            "a".into(),
            json!({"id":"a","command":"x","headers":{"H":"hidden"}}),
        );
        write_configs(&secrets, &m).await.unwrap();

        let r = svc(None, &secrets, &h).list(None).await.unwrap();
        let arr = r["servers"].as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["id"], json!("a"));
        assert_eq!(arr[1]["id"], json!("b"));
        assert_eq!(arr[0]["headers"]["H"], json!(REDACTED_PLACEHOLDER));
        assert_eq!(arr[1]["env"]["K"], json!(REDACTED_PLACEHOLDER));
    }

    #[tokio::test]
    async fn create_persists_and_redacts() {
        let secrets = mem_async();
        let h = McpHub::new();
        let s = svc(None, &secrets, &h);
        let out = s
            .create(json!({
                "id": "n1",
                "transport": "stdio",
                "command": "node",
                "env": { "T": "secret" },
            }))
            .await
            .unwrap();
        assert_eq!(out["server"]["id"], json!("n1"));
        assert_eq!(out["server"]["env"]["T"], json!(REDACTED_PLACEHOLDER));
        // Stored plaintext keeps the original secret.
        let stored = read_configs(&secrets).await;
        assert_eq!(stored["n1"]["env"]["T"], json!("secret"));
    }

    #[tokio::test]
    async fn create_rejects_duplicate_id() {
        let secrets = mem_async();
        let h = McpHub::new();
        let s = svc(None, &secrets, &h);
        let cfg = json!({ "id": "dup", "transport": "stdio", "command": "x" });
        s.create(cfg.clone()).await.unwrap();
        let err = s.create(cfg).await.unwrap_err();
        assert!(matches!(err, Error::InvalidParams(_)));
        assert!(format!("{err}").contains("already exists"));
    }

    #[tokio::test]
    async fn create_propagates_normalize_error() {
        let secrets = mem_async();
        let h = McpHub::new();
        let err = svc(None, &secrets, &h)
            .create(json!({ "transport": "stdio" }))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidParams(_)));
    }

    #[tokio::test]
    async fn update_returns_not_found_for_unknown_id() {
        let secrets = mem_async();
        let h = McpHub::new();
        let err = svc(None, &secrets, &h)
            .update("missing", json!({ "command": "x" }))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound(_)));
    }

    #[tokio::test]
    async fn update_pins_id_and_redacts() {
        let secrets = mem_async();
        let h = McpHub::new();
        let s = svc(None, &secrets, &h);
        s.create(json!({ "id": "u1", "transport": "stdio", "command": "x" }))
            .await
            .unwrap();
        // The payload id is ignored — server_id pins the id.
        let out = s
            .update(
                "u1",
                json!({
                    "id": "WRONG",
                    "transport": "stdio",
                    "command": "y",
                    "env": { "K": "v" },
                    "name": "renamed",
                }),
            )
            .await
            .unwrap();
        assert_eq!(out["server"]["id"], json!("u1"));
        assert_eq!(out["server"]["name"], json!("renamed"));
        assert_eq!(out["server"]["env"]["K"], json!(REDACTED_PLACEHOLDER));
    }

    #[tokio::test]
    async fn delete_returns_not_found_for_unknown_id() {
        let secrets = mem_async();
        let h = McpHub::new();
        let err = svc(None, &secrets, &h).delete("missing").await.unwrap_err();
        assert!(matches!(err, Error::NotFound(_)));
    }

    #[tokio::test]
    async fn delete_removes_config_and_returns_success() {
        let secrets = mem_async();
        let h = McpHub::new();
        let s = svc(None, &secrets, &h);
        s.create(json!({ "id": "d1", "transport": "stdio", "command": "x" }))
            .await
            .unwrap();
        let out = s.delete("d1").await.unwrap();
        assert_eq!(out["success"], json!(true));
        assert!(read_configs(&secrets).await.is_empty());
    }

    #[tokio::test]
    async fn toggle_enable_with_user_servers_off_returns_stopped_and_persists_flag() {
        let (reg, _cfg) = temp_registry();
        let secrets = mem_async();
        let h = McpHub::new();
        let s = svc(Some(&reg), &secrets, &h);
        s.create(json!({ "id": "t1", "transport": "stdio", "command": "x", "enabled": false }))
            .await
            .unwrap();
        // Gate off: even toggle(true) yields stopped status.
        reg.apply(&[("mcp.enableUserServers".to_string(), json!(false))])
            .unwrap();

        let out = s.toggle("t1", true).await.unwrap();
        assert_eq!(out["status"]["state"], json!("stopped"));
        // Persisted enabled flag flipped to true.
        assert_eq!(read_configs(&secrets).await["t1"]["enabled"], json!(true));
        // Enabled means the id should NOT be in disabledServers.
        assert!(!disabled_servers(&reg.snapshot().effective)
            .iter()
            .any(|d| d == "t1"));
    }

    #[tokio::test]
    async fn toggle_disable_emits_stopped_and_tracks_disabled_list() {
        let (reg, _cfg) = temp_registry();
        let secrets = mem_async();
        let h = McpHub::new();
        let s = svc(Some(&reg), &secrets, &h);
        s.create(json!({ "id": "t2", "transport": "stdio", "command": "x", "enabled": true }))
            .await
            .unwrap();

        let out = s.toggle("t2", false).await.unwrap();
        assert_eq!(out["status"]["state"], json!("stopped"));
        assert_eq!(read_configs(&secrets).await["t2"]["enabled"], json!(false));
        assert!(disabled_servers(&reg.snapshot().effective)
            .iter()
            .any(|d| d == "t2"));

        // Disabling again is idempotent — id appears only once.
        s.toggle("t2", false).await.unwrap();
        let list = disabled_servers(&reg.snapshot().effective);
        assert_eq!(list.iter().filter(|d| *d == "t2").count(), 1);
    }

    #[tokio::test]
    async fn toggle_enable_removes_id_from_disabled_list() {
        let (reg, _cfg) = temp_registry();
        let secrets = mem_async();
        let h = McpHub::new();
        let s = svc(Some(&reg), &secrets, &h);
        s.create(json!({ "id": "t3", "transport": "stdio", "command": "x" }))
            .await
            .unwrap();
        // Pre-populate the disabled list.
        set_disabled_servers(Some(&reg), &["t3".to_string(), "other".to_string()]).unwrap();
        reg.apply(&[("mcp.enableUserServers".to_string(), json!(false))])
            .unwrap();

        let _ = s.toggle("t3", true).await.unwrap();
        let list = disabled_servers(&reg.snapshot().effective);
        assert!(!list.iter().any(|d| d == "t3"));
        assert!(list.iter().any(|d| d == "other"));
    }

    #[tokio::test]
    async fn toggle_returns_not_found_for_unknown_id() {
        let secrets = mem_async();
        let h = McpHub::new();
        let err = svc(None, &secrets, &h)
            .toggle("ghost", true)
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound(_)));
    }

    #[tokio::test]
    async fn restart_returns_not_found_for_unknown_id() {
        let secrets = mem_async();
        let h = McpHub::new();
        let err = svc(None, &secrets, &h).restart("ghost").await.unwrap_err();
        assert!(matches!(err, Error::NotFound(_)));
    }

    #[tokio::test]
    async fn restart_returns_stopped_when_user_servers_gate_off() {
        let (reg, _cfg) = temp_registry();
        let secrets = mem_async();
        let h = McpHub::new();
        let s = svc(Some(&reg), &secrets, &h);
        s.create(json!({ "id": "r1", "transport": "stdio", "command": BOGUS_CMD }))
            .await
            .unwrap();
        reg.apply(&[("mcp.enableUserServers".to_string(), json!(false))])
            .unwrap();
        let out = s.restart("r1").await.unwrap();
        assert_eq!(out["status"]["state"], json!("stopped"));
    }

    #[tokio::test]
    async fn get_status_returns_not_found_for_unknown_id() {
        let secrets = mem_async();
        let h = McpHub::new();
        let err = svc(None, &secrets, &h)
            .get_status("ghost")
            .await
            .unwrap_err();
        assert!(matches!(err, Error::NotFound(_)));
    }

    #[tokio::test]
    async fn get_status_returns_stopped_for_known_but_inactive_server() {
        let secrets = mem_async();
        let h = McpHub::new();
        let s = svc(None, &secrets, &h);
        s.create(json!({ "id": "g1", "transport": "stdio", "command": "x" }))
            .await
            .unwrap();
        let out = s.get_status("g1").await.unwrap();
        assert_eq!(out["status"], status_stopped("g1"));
    }

    #[tokio::test]
    async fn start_enabled_no_op_when_gate_off() {
        let (reg, _cfg) = temp_registry();
        let secrets = mem_async();
        let h = McpHub::new();
        // Even a config that would normally be started is skipped.
        let mut m = Map::new();
        m.insert(
            "x".into(),
            json!({"id":"x","command":BOGUS_CMD,"enabled":true}),
        );
        write_configs(&secrets, &m).await.unwrap();
        reg.apply(&[("mcp.enableUserServers".to_string(), json!(false))])
            .unwrap();

        svc(Some(&reg), &secrets, &h).start_enabled().await;
        // Nothing was started.
        assert_eq!(h.status("x"), status_stopped("x"));
    }

    #[tokio::test]
    async fn start_enabled_skips_disabled_ids_and_disabled_configs() {
        let (reg, _cfg) = temp_registry();
        let secrets = mem_async();
        let h = McpHub::new();
        let mut m = Map::new();
        // (1) enabled=true but id in disabledServers → skipped
        m.insert(
            "blocked".into(),
            json!({"id":"blocked","command":BOGUS_CMD,"enabled":true}),
        );
        // (2) enabled=false → skipped
        m.insert(
            "off".into(),
            json!({"id":"off","command":BOGUS_CMD,"enabled":false}),
        );
        write_configs(&secrets, &m).await.unwrap();
        set_disabled_servers(Some(&reg), &["blocked".to_string()]).unwrap();

        svc(Some(&reg), &secrets, &h).start_enabled().await;

        // Neither id was started (no error status from a failed spawn either).
        assert_eq!(h.status("blocked"), status_stopped("blocked"));
        assert_eq!(h.status("off"), status_stopped("off"));
    }

    // The boot sweep runs in the background, so it re-reads eligibility from the
    // live settings/configs around every start instead of trusting its opening
    // snapshot. These cover each way a concurrent `mcp.servers.*` mutation makes
    // an id ineligible (monorepo#1581).
    #[tokio::test]
    async fn eligible_config_returns_live_definition_when_startable() {
        let (reg, _cfg) = temp_registry();
        let secrets = mem_async();
        let h = McpHub::new();
        let cfg = json!({"id":"go","command":BOGUS_CMD,"enabled":true});
        let mut m = Map::new();
        m.insert("go".into(), cfg.clone());
        write_configs(&secrets, &m).await.unwrap();

        assert_eq!(
            svc(Some(&reg), &secrets, &h).eligible_config("go").await,
            Some(cfg)
        );
    }

    #[tokio::test]
    async fn eligible_config_rejects_deleted_disabled_and_gated_off_ids() {
        let (reg, _cfg) = temp_registry();
        let secrets = mem_async();
        let h = McpHub::new();
        let mut m = Map::new();
        m.insert(
            "go".into(),
            json!({"id":"go","command":BOGUS_CMD,"enabled":true}),
        );
        m.insert(
            "off".into(),
            json!({"id":"off","command":BOGUS_CMD,"enabled":false}),
        );
        write_configs(&secrets, &m).await.unwrap();

        // Deleted definition (the `delete` RPC removed it mid-sweep).
        assert_eq!(
            svc(Some(&reg), &secrets, &h).eligible_config("gone").await,
            None
        );
        // `toggle(false)` cleared the config's `enabled` flag.
        assert_eq!(
            svc(Some(&reg), &secrets, &h).eligible_config("off").await,
            None
        );
        // `toggle(false)` also adds the id to `mcp.disabledServers`.
        set_disabled_servers(Some(&reg), &["go".to_string()]).unwrap();
        assert_eq!(
            svc(Some(&reg), &secrets, &h).eligible_config("go").await,
            None
        );
        // The global gate flipping off makes every id ineligible.
        set_disabled_servers(Some(&reg), &[]).unwrap();
        reg.apply(&[("mcp.enableUserServers".to_string(), json!(false))])
            .unwrap();
        assert_eq!(
            svc(Some(&reg), &secrets, &h).eligible_config("go").await,
            None
        );
    }

    #[tokio::test]
    async fn start_enabled_skips_ids_deleted_after_the_snapshot() {
        // The sweep snapshots ids first, then re-checks each one: an id removed
        // between the snapshot and its turn must never be started.
        let (reg, _cfg) = temp_registry();
        let secrets = mem_async();
        let h = McpHub::new();
        let mut m = Map::new();
        m.insert(
            "doomed".into(),
            json!({"id":"doomed","command":BOGUS_CMD,"enabled":true}),
        );
        write_configs(&secrets, &m).await.unwrap();
        // Stand in for the concurrent `delete`: the definition is gone by the
        // time the per-id re-check runs.
        write_configs(&secrets, &Map::new()).await.unwrap();

        svc(Some(&reg), &secrets, &h).start_enabled().await;
        assert_eq!(h.status("doomed"), status_stopped("doomed"));
    }

    #[tokio::test]
    async fn start_enabled_attempts_to_start_eligible_configs() {
        // Eligible config has enabled=true and is NOT in disabledServers; the gate
        // is on by default. A bogus command makes spawn fail fast (error status),
        // which is the best-effort branch we want to cover.
        let secrets = mem_async();
        let h = McpHub::new();
        let mut m = Map::new();
        m.insert(
            "go".into(),
            json!({"id":"go","command":BOGUS_CMD,"enabled":true,"transport":"stdio"}),
        );
        write_configs(&secrets, &m).await.unwrap();

        svc(None, &secrets, &h).start_enabled().await;
        // Spawn failed → no live entry → status stays stopped.
        assert_eq!(h.status("go"), status_stopped("go"));
    }

    // -- remote (http/sse) probing ------------------------------------------

    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    /// Minimal one-shot HTTP stub: accepts connections and answers every
    /// request on each connection with `response` (raw bytes). Returns the
    /// bound URL. Lives until the returned guard is dropped.
    async fn http_stub(response: &'static str) -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let handle = tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 65536];
                    loop {
                        // Read one request's worth of bytes (best-effort framing:
                        // the tiny probe requests always arrive in one read).
                        match sock.read(&mut buf).await {
                            Ok(0) | Err(_) => return,
                            Ok(_) => {}
                        }
                        if sock.write_all(response.as_bytes()).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        (format!("http://{addr}"), handle)
    }

    /// A well-formed streamable-HTTP MCP response body answering whatever id
    /// the request carried is impossible in a canned stub, so the stub answers
    /// id 1 (initialize) and relies on tools/list being best-effort.
    fn ok_json_response(body: &str) -> String {
        format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
            body.len(),
            body
        )
    }

    fn remote_cfg(id: &str, transport: &str, url: &str) -> Value {
        json!({
            "id": id,
            "name": id,
            "transport": transport,
            "url": url,
            "enabled": true,
        })
    }

    #[tokio::test]
    async fn http_probe_success_maps_to_running() {
        let body = r#"{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2024-11-05","capabilities":{},"serverInfo":{"name":"stub","version":"0"}}}"#;
        let resp = ok_json_response(body);
        let leaked: &'static str = Box::leak(resp.into_boxed_str());
        let (url, _guard) = http_stub(leaked).await;

        let h = McpHub::new();
        let status = h.start(remote_cfg("r-ok", "http", &url), true).await;
        assert_eq!(status["state"], json!("running"));
        assert!(status["pid"].is_null(), "remote servers have no pid");
        assert!(status["startedAt"].is_number());
    }

    #[tokio::test]
    async fn http_probe_401_maps_to_auth_error() {
        let (url, _guard) =
            http_stub("HTTP/1.1 401 Unauthorized\r\ncontent-length: 0\r\n\r\n").await;
        let h = McpHub::new();
        let status = h.start(remote_cfg("r-auth", "http", &url), true).await;
        assert_eq!(status["state"], json!("error"));
        let err = status["lastError"].as_str().unwrap();
        assert!(err.contains("authentication failed"), "got: {err}");
        assert!(err.contains("401"), "got: {err}");
    }

    #[tokio::test]
    async fn http_probe_500_maps_to_server_error() {
        let (url, _guard) =
            http_stub("HTTP/1.1 500 Internal Server Error\r\ncontent-length: 0\r\n\r\n").await;
        let h = McpHub::new();
        let status = h.start(remote_cfg("r-500", "http", &url), true).await;
        assert_eq!(status["state"], json!("error"));
        assert!(status["lastError"]
            .as_str()
            .unwrap()
            .contains("server error (HTTP 500)"));
    }

    #[tokio::test]
    async fn http_probe_closed_port_maps_to_unreachable() {
        // Bind + drop to get a port that is closed at probe time.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);

        let h = McpHub::new();
        let status = h.start(remote_cfg("r-dead", "http", &url), true).await;
        assert_eq!(status["state"], json!("error"));
        assert!(status["lastError"]
            .as_str()
            .unwrap()
            .contains("unreachable from daemon host"));
    }

    #[tokio::test]
    async fn sse_probe_2xx_maps_to_running_without_tool_count() {
        let (url, _guard) = http_stub(
            "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: 0\r\n\r\n",
        )
        .await;
        let h = McpHub::new();
        let status = h.start(remote_cfg("r-sse", "sse", &url), true).await;
        assert_eq!(status["state"], json!("running"));
        assert!(status["toolCount"].is_null());
    }

    #[tokio::test]
    async fn remote_config_without_url_maps_to_error() {
        let h = McpHub::new();
        let status = h
            .start(json!({ "id": "r-nourl", "transport": "http" }), true)
            .await;
        assert_eq!(status["state"], json!("error"));
        assert!(status["lastError"]
            .as_str()
            .unwrap()
            .contains("requires a url"));
    }

    #[tokio::test]
    async fn health_tick_reprobe_emits_event_on_remote_transition() {
        // Start against a live stub (running), kill the stub, then a sweep
        // must flip the status to error and publish the transition.
        let body = r#"{"jsonrpc":"2.0","id":1,"result":{}}"#;
        let resp = ok_json_response(body);
        let leaked: &'static str = Box::leak(resp.into_boxed_str());
        let (url, guard) = http_stub(leaked).await;

        let (_tmp, store) = open_store().await;
        let bus = EventBus::new(store);
        let mut sub = bus.subscribe(SubscriptionFilter::default());
        let h = McpHub::new();
        h.set_event_bus(bus);

        let status = h.start(remote_cfg("r-flip", "http", &url), true).await;
        assert_eq!(status["state"], json!("running"));
        // Drain the `running` event.
        let _ = tokio::time::timeout(Duration::from_secs(2), sub.recv())
            .await
            .expect("running event")
            .expect("subscription open");

        guard.abort(); // stub gone → next probe fails
        let _ = guard.await; // listener provably dropped before the re-probe
        h.health_tick().await;
        assert_eq!(h.status("r-flip")["state"], json!("error"));
        let batch = tokio::time::timeout(Duration::from_secs(2), sub.recv())
            .await
            .expect("error event delivered")
            .expect("subscription open");
        let data = serde_json::to_value(&batch[0].data).unwrap();
        assert_eq!(data["serverId"], json!("r-flip"));
        assert_eq!(data["status"]["state"], json!("error"));
    }

    #[tokio::test]
    async fn health_tick_reprobe_no_event_when_state_unchanged() {
        let body = r#"{"jsonrpc":"2.0","id":1,"result":{}}"#;
        let resp = ok_json_response(body);
        let leaked: &'static str = Box::leak(resp.into_boxed_str());
        let (url, _guard) = http_stub(leaked).await;

        let (_tmp, store) = open_store().await;
        let bus = EventBus::new(store);
        let mut sub = bus.subscribe(SubscriptionFilter::default());
        let h = McpHub::new();
        h.set_event_bus(bus);

        let started = h.start(remote_cfg("r-same", "http", &url), true).await;
        assert_eq!(started["state"], json!("running"));
        let _ = tokio::time::timeout(Duration::from_secs(2), sub.recv())
            .await
            .expect("running event")
            .expect("subscription open");
        let started_at = started["startedAt"].clone();

        h.health_tick().await;
        // Still running, startedAt preserved, and no second event.
        let now = h.status("r-same");
        assert_eq!(now["state"], json!("running"));
        assert_eq!(now["startedAt"], started_at);
        assert!(
            tokio::time::timeout(Duration::from_millis(300), sub.recv())
                .await
                .is_err(),
            "no event on an unchanged state"
        );
    }

    #[tokio::test]
    async fn hub_stop_removes_remote_entry() {
        let (url, _guard) =
            http_stub("HTTP/1.1 401 Unauthorized\r\ncontent-length: 0\r\n\r\n").await;
        let h = McpHub::new();
        let _ = h.start(remote_cfg("r-stop", "http", &url), true).await;
        assert_eq!(h.status("r-stop")["state"], json!("error"));
        assert!(h.stop("r-stop").await, "tracked entry is stopped");
        assert_eq!(h.status("r-stop"), status_stopped("r-stop"));
    }

    #[tokio::test]
    async fn http_probe_non_jsonrpc_body_maps_to_error() {
        // A JSON endpoint that is not an MCP server (no jsonrpc/id/result
        // envelope) must NOT be reported as running.
        let resp = ok_json_response(r#"{"hello":"world"}"#);
        let leaked: &'static str = Box::leak(resp.into_boxed_str());
        let (url, _guard) = http_stub(leaked).await;

        let h = McpHub::new();
        let status = h.start(remote_cfg("r-notmcp", "http", &url), true).await;
        assert_eq!(status["state"], json!("error"));
        assert!(status["lastError"]
            .as_str()
            .unwrap()
            .contains("not a JSON-RPC 2.0 response"));
    }

    #[tokio::test]
    async fn update_reprobes_error_state_remote() {
        // A remote stuck in `error` (dead URL) must re-probe the NEW config on
        // update, not stay pinned to the old one until the next health tick.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let dead_url = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        let body = r#"{"jsonrpc":"2.0","id":1,"result":{}}"#;
        let resp = ok_json_response(body);
        let leaked: &'static str = Box::leak(resp.into_boxed_str());
        let (live_url, _guard) = http_stub(leaked).await;

        let secrets = mem_async();
        let h = McpHub::new();
        let s = svc(None, &secrets, &h);
        s.create(json!({
            "id": "r-upd", "transport": "http", "url": dead_url, "enabled": true,
        }))
        .await
        .unwrap();
        let out = s.toggle("r-upd", true).await.unwrap();
        assert_eq!(out["status"]["state"], json!("error"));

        s.update(
            "r-upd",
            json!({ "transport": "http", "url": live_url, "enabled": true }),
        )
        .await
        .unwrap();
        assert_eq!(h.status("r-upd")["state"], json!("running"));
    }
}
