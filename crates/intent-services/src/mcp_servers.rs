//! External MCP-server lifecycle/config (§18.3, PROTOCOL §5.22): manage the set
//! of **user-configured external** MCP servers (distinct from the §6.8 agent→BE
//! callback). Config lives in the **sensitive** `mcp.servers` setting (`env`/
//! `headers` redacted over the wire); the [`McpHub`] spawns/stops/restarts
//! **stdio** servers and a health monitor pings them, pushing
//! `mcp.servers:status-changed` (§10) on every transition. Runtime status is
//! never persisted. Ports `mcp-hub.ts`/`server-manager.ts`/`health-monitor.ts`/
//! `user-mcp-settings.ts`.

use std::collections::HashMap;
use std::process::Stdio;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use intent_acp::{Connection, ConnectionHooks};
use intent_core::{events::MCP_SERVERS_STATUS_CHANGED, now_iso, Error, Result, WorkspaceId};
use intent_store::{NewEvent, Store};
use serde_json::{json, Map, Value};
use tokio::io::AsyncRead;
use tokio::process::{Child, Command};
use uuid::Uuid;

use crate::settings::{SecretStore, REDACTED_PLACEHOLDER};
use crate::{system_actor, EventBus};

/// Keychain account for the sensitive `mcp.servers` setting (§9.8). Mirrors the
/// `SettingsService` redaction seam — the config (with secrets) lives here.
const SETTING_KEY: &str = "mcp.servers";
/// MCP protocol version advertised on `initialize` (mirrors the stdio peers).
const MCP_PROTOCOL_VERSION: &str = "2024-11-05";
/// Timeout for the `initialize`/`tools/list` handshake requests.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// Timeout for a single health `ping`.
const PING_TIMEOUT: Duration = Duration::from_secs(5);
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
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
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
            .map(String::from)
            .unwrap_or_else(|| format!("srv-{}", &Uuid::new_v4().simple().to_string()[..8])),
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
    if !obj
        .get("name")
        .and_then(Value::as_str)
        .map(|s| !s.is_empty())
        .unwrap_or(false)
    {
        obj.insert("name".into(), json!(id));
    }
    if !obj.get("enabled").map(Value::is_boolean).unwrap_or(false) {
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
            .map(|s| !s.is_empty())
            .unwrap_or(false);
        if !has_cmd {
            return Err(Error::InvalidParams(
                "stdio server requires a non-empty command".to_string(),
            ));
        }
    } else {
        let has_url = obj
            .get("url")
            .and_then(Value::as_str)
            .map(|s| !s.is_empty())
            .unwrap_or(false);
        if !has_url {
            return Err(Error::InvalidParams(format!(
                "{transport} server requires a url"
            )));
        }
    }
    Ok(())
}

/// Read the `mcp.enableUserServers` gate (default `true`, §9.8 group A).
pub(crate) async fn enable_user_servers(store: &Store) -> bool {
    match store.get_setting("mcp.enableUserServers").await {
        Ok(Some(raw)) => serde_json::from_str::<Value>(&raw)
            .ok()
            .and_then(|v| v.as_bool())
            .unwrap_or(true),
        _ => true,
    }
}

/// Read the `mcp.disabledServers` list (ids that stay stopped, default `[]`).
async fn disabled_servers(store: &Store) -> Vec<String> {
    match store.get_setting("mcp.disabledServers").await {
        Ok(Some(raw)) => serde_json::from_str::<Value>(&raw)
            .ok()
            .and_then(|v| v.as_array().cloned())
            .map(|a| {
                a.into_iter()
                    .filter_map(|v| v.as_str().map(String::from))
                    .collect()
            })
            .unwrap_or_default(),
        _ => Vec::new(),
    }
}

/// Persist the `mcp.disabledServers` list.
async fn set_disabled_servers(store: &Store, list: &[String]) -> Result<()> {
    let raw = serde_json::to_string(&json!(list))
        .map_err(|e| Error::Internal(format!("encode disabledServers failed: {e}")))?;
    store.set_setting("mcp.disabledServers", &raw).await
}

/// Read the configured external servers from the sensitive `mcp.servers` secret,
/// keyed by id. A missing/garbled secret yields an empty map.
fn read_configs(secrets: &dyn SecretStore) -> Map<String, Value> {
    match secrets.load(SETTING_KEY) {
        Some(raw) => serde_json::from_str::<Value>(&raw)
            .ok()
            .and_then(|v| v.as_object().cloned())
            .unwrap_or_default(),
        None => Map::new(),
    }
}

/// Persist the configured external servers back to the sensitive secret.
fn write_configs(secrets: &dyn SecretStore, map: &Map<String, Value>) -> Result<()> {
    let raw = serde_json::to_string(&Value::Object(map.clone()))
        .map_err(|e| Error::Internal(format!("encode mcp.servers failed: {e}")))?;
    secrets.store(SETTING_KEY, &raw)
}

/// A live, spawned stdio MCP server: the child process, its JSON-RPC connection,
/// the last published status, and the consecutive health-failure count.
struct RunningServer {
    config: Value,
    child: Child,
    pid: Option<u32>,
    conn: Arc<Connection>,
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
    pub fn set_event_bus(&self, bus: EventBus) {
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
            .map(|rs| rs.status.clone())
            .unwrap_or_else(|| status_stopped(id))
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

    /// Start `config` (stdio only). Replaces any existing instance, performs the
    /// MCP handshake, and emits `running` (or `error`). When `enable_user_servers`
    /// is false the server is left `stopped`.
    pub async fn start(&self, config: Value, enable_user_servers: bool) -> Value {
        let id = config_id(&config);
        self.stop_inner(&id).await;
        if !enable_user_servers {
            let st = status_stopped(&id);
            self.publish_status(&st).await;
            return st;
        }
        match spawn_stdio(&config).await {
            Ok((child, pid, conn, tool_count)) => {
                let status =
                    status_value(&id, "running", pid, tool_count, None, Some(now_millis()));
                let rs = RunningServer {
                    config,
                    child,
                    pid,
                    conn: Arc::new(conn),
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

    /// Restart `config`: stop-then-start (emits `stopped` then `running`/`error`).
    pub async fn restart(&self, config: Value, enable_user_servers: bool) -> Value {
        let id = config_id(&config);
        self.stop(&id).await;
        self.start(config, enable_user_servers).await
    }

    /// One health sweep: ping every running server, reset the failure count on
    /// success, and restart a server that has exceeded [`MAX_FAILURES`].
    async fn health_tick(&self) {
        let targets: Vec<(String, Arc<Connection>)> = self
            .inner
            .servers
            .lock()
            .unwrap()
            .iter()
            .map(|(id, rs)| (id.clone(), rs.conn.clone()))
            .collect();
        for (id, conn) in targets {
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

/// Terminate a server's whole process group (SIGTERM → grace → SIGKILL), then
/// let the [`Connection`] drop to abort its reader/writer tasks.
async fn reap(rs: &mut RunningServer) {
    #[cfg(unix)]
    {
        if let Some(pid) = rs.pid {
            let _ = kill_group(pid, nix::sys::signal::Signal::SIGTERM);
            let mut exited = false;
            let iters = (TERM_GRACE.as_millis() / REAP_POLL.as_millis()).max(1);
            for _ in 0..iters {
                if matches!(rs.child.try_wait(), Ok(Some(_))) {
                    exited = true;
                    break;
                }
                tokio::time::sleep(REAP_POLL).await;
            }
            if !exited {
                let _ = kill_group(pid, nix::sys::signal::Signal::SIGKILL);
            }
        } else {
            let _ = rs.child.kill().await;
        }
    }
    #[cfg(not(unix))]
    {
        let _ = rs.child.kill().await;
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

/// Stateless executor for the `mcp.servers.*` namespace (PROTOCOL §5.22) over the
/// store (`mcp.enableUserServers`/`mcp.disabledServers`), the [`SecretStore`]
/// (the sensitive `mcp.servers` config), and the runtime [`McpHub`]. Construct
/// one per call from the long-lived `Services`.
pub(crate) struct McpServersService<'a> {
    store: &'a Store,
    secrets: &'a dyn SecretStore,
    hub: &'a McpHub,
}

impl<'a> McpServersService<'a> {
    pub(crate) fn new(store: &'a Store, secrets: &'a dyn SecretStore, hub: &'a McpHub) -> Self {
        Self {
            store,
            secrets,
            hub,
        }
    }

    /// Fetch one stored config by id, or `NotFound`.
    fn require_config(&self, server_id: &str) -> Result<Value> {
        read_configs(self.secrets)
            .remove(server_id)
            .ok_or_else(|| Error::NotFound(format!("mcp server not found: {server_id}")))
    }

    /// `mcp.servers.list` → `{ servers: McpServerConfig[] }` (env/headers redacted),
    /// sorted by id for a stable wire order.
    pub(crate) async fn list(&self, _workspace_id: Option<&str>) -> Result<Value> {
        let configs = read_configs(self.secrets);
        let mut servers: Vec<Value> = configs.values().map(redact_config).collect();
        servers.sort_by_key(config_id);
        Ok(json!({ "servers": servers }))
    }

    /// `mcp.servers.create` → persist a new definition; `{ server }` (redacted).
    pub(crate) async fn create(&self, config: Value) -> Result<Value> {
        let normalized = normalize_config(config, None)?;
        let id = config_id(&normalized);
        let mut configs = read_configs(self.secrets);
        if configs.contains_key(&id) {
            return Err(Error::InvalidParams(format!(
                "mcp server already exists: {id}"
            )));
        }
        configs.insert(id, normalized.clone());
        write_configs(self.secrets, &configs)?;
        Ok(json!({ "server": redact_config(&normalized) }))
    }

    /// `mcp.servers.update` → replace an existing definition; `{ server }`
    /// (redacted). A running server is restarted to apply the new config.
    pub(crate) async fn update(&self, server_id: &str, config: Value) -> Result<Value> {
        let mut configs = read_configs(self.secrets);
        if !configs.contains_key(server_id) {
            return Err(Error::NotFound(format!(
                "mcp server not found: {server_id}"
            )));
        }
        let normalized = normalize_config(config, Some(server_id))?;
        configs.insert(server_id.to_string(), normalized.clone());
        write_configs(self.secrets, &configs)?;
        // Apply live: a running server picks up the new command/env on restart.
        let running = self.hub.status(server_id)["state"] == "running";
        if running {
            let enable = enable_user_servers(self.store).await;
            self.hub.restart(normalized.clone(), enable).await;
        }
        Ok(json!({ "server": redact_config(&normalized) }))
    }

    /// `mcp.servers.delete` → stop + remove a definition; `{ success: true }`.
    pub(crate) async fn delete(&self, server_id: &str) -> Result<Value> {
        let mut configs = read_configs(self.secrets);
        if configs.remove(server_id).is_none() {
            return Err(Error::NotFound(format!(
                "mcp server not found: {server_id}"
            )));
        }
        self.hub.stop(server_id).await;
        write_configs(self.secrets, &configs)?;
        Ok(json!({ "success": true }))
    }

    /// `mcp.servers.toggle` → enable (start) / disable (stop). Updates the config's
    /// `enabled` flag + `mcp.disabledServers`, drives the lifecycle, and returns
    /// `{ status }`.
    pub(crate) async fn toggle(&self, server_id: &str, enabled: bool) -> Result<Value> {
        let mut config = self.require_config(server_id)?;
        if let Some(obj) = config.as_object_mut() {
            obj.insert("enabled".into(), json!(enabled));
        }
        let mut configs = read_configs(self.secrets);
        configs.insert(server_id.to_string(), config.clone());
        write_configs(self.secrets, &configs)?;

        let mut disabled = disabled_servers(self.store).await;
        let was_present = disabled.iter().any(|d| d == server_id);
        if enabled {
            disabled.retain(|d| d != server_id);
        } else if !was_present {
            disabled.push(server_id.to_string());
        }
        set_disabled_servers(self.store, &disabled).await?;

        let status = if enabled {
            let enable = enable_user_servers(self.store).await;
            self.hub.start(config, enable).await
        } else {
            self.hub.stop(server_id).await;
            status_stopped(server_id)
        };
        Ok(json!({ "status": status }))
    }

    /// `mcp.servers.restart` → stop-then-start; `{ status }`.
    pub(crate) async fn restart(&self, server_id: &str) -> Result<Value> {
        let config = self.require_config(server_id)?;
        let enable = enable_user_servers(self.store).await;
        let status = self.hub.restart(config, enable).await;
        Ok(json!({ "status": status }))
    }

    /// `mcp.servers.getStatus` → live status point read; `{ status }`.
    pub(crate) async fn get_status(&self, server_id: &str) -> Result<Value> {
        // Surface NotFound for an unknown id; otherwise the live runtime status.
        self.require_config(server_id)?;
        Ok(json!({ "status": self.hub.status(server_id) }))
    }

    /// Start every enabled, non-disabled server (daemon boot). Best-effort: a
    /// failed spawn surfaces as an `error` status event, not a hard failure.
    pub(crate) async fn start_enabled(&self) {
        if !enable_user_servers(self.store).await {
            return;
        }
        let disabled = disabled_servers(self.store).await;
        for (id, config) in read_configs(self.secrets) {
            let enabled = config
                .get("enabled")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            if enabled && !disabled.iter().any(|d| d == &id) {
                self.hub.start(config, true).await;
            }
        }
    }
}
