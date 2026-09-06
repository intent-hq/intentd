//! `browser.*` client-callable trigger fast-path: `browser.exec` (§5.14, §12.4).
//!
//! `browser.exec` is a **client-callable trigger** whose real work happens on
//! the connected frontend (Chrome `DevTools` Protocol against embedded browser
//! tabs — no CDP logic runs in Rust). The daemon validates the envelope, then
//! dispatches an FE-served reverse RPC — method name unchanged (`browser.exec`),
//! with a `rev-<n>` request id (mirroring `host.openInEditor` /
//! `host.pickApplication`) — and echoes the FE's result back to the caller.
//!
//! Wire shape (reference parity with the FE MCP tool `browser_exec`): a
//! validated non-empty `actions` batch forwards `{ actions, tabId?, agentId?,
//! workspaceId? }` to the FE; the reply is reduced to a single action's
//! result envelope for a one-action batch and to `{ results: [...] }` for a
//! multi-action batch. `-32602` on missing / empty / non-array `actions`;
//! `-32603` when the FE reverse RPC fails, times out, no client is connected,
//! or the FE surfaces its own error.
//!
//! The daemon-owned **browser tab registry** (REV-2 Model 2 & 6) shares the
//! namespace: `browser.listTabs` (any client) reads the persisted tabs of a
//! workspace decorated with host presence from the reverse registry
//! (`hostName` / `hostConnected`); `browser.upsertTab` / `browser.removeTab`
//! / `browser.syncTabs` are **host-only** reports keyed by the connection's
//! logical `clientId` from `client.hello` (§5.17) — never a wire parameter —
//! which is why they are transport interceptors like `drafts.*`. A connection
//! that never said hello cannot host tabs (`-32602`).

use std::collections::{HashMap, HashSet};
use std::fmt;

use intent_core::{BrowserTab, BrowserTabInput, ClientId, WorkspaceApi, WorkspaceId};
use intent_services::browser_ops;
use serde_json::{json, Map, Value};

use crate::events::{error_frame, success_frame};
use crate::reverse::{request_timeout, ClientPresence, PrimaryReverseRegistry, ReverseChannel};

/// The `browser.*` methods, once classified. Kept as an enum for parity with
/// `host::HostMethod`.
pub(crate) enum BrowserMethod {
    /// `browser.exec` client-callable trigger → FE-served reverse RPC.
    Exec,
    /// `browser.listTabs` — daemon-answered registry read (any client).
    ListTabs,
    /// `browser.upsertTab` — host-only tab report.
    UpsertTab,
    /// `browser.removeTab` — host-only close report.
    RemoveTab,
    /// `browser.syncTabs` — host-only full-snapshot reconciliation.
    SyncTabs,
}

impl BrowserMethod {
    /// Whether the method reports as a tab host and therefore needs the
    /// connection's hello-bound identity. Only these three resolve it: the
    /// lookup is an indexed read under the reverse registry lock, and
    /// `browser.listTabs` / `browser.exec` never consume the identity, so
    /// they skip it entirely and the list fast path stays free of it.
    pub(crate) fn reports_as_host(&self) -> bool {
        matches!(self, Self::UpsertTab | Self::RemoveTab | Self::SyncTabs)
    }
}

/// What the registry methods need from the connection task: the service
/// surface, the connection's hello'd identity (the reporting host), and the
/// live reverse registry for presence decoration. `client_id` is the identity
/// `client.hello` bound onto the reverse-registry entry
/// (`PrimaryReverseGuard::bound_client_id`), never the lazily minted
/// `drafts.*` binding — a connection that only touched drafts is not a host.
pub(crate) struct TabContext<'a> {
    pub api: &'a dyn WorkspaceApi,
    pub client_id: Option<&'a ClientId>,
    pub registry: Option<&'a PrimaryReverseRegistry>,
}

/// A classified `browser.*` request awaiting handling by the connection task.
pub(crate) struct BrowserRequest {
    pub method: BrowserMethod,
    pub id_present: bool,
    pub id_echo: Value,
    pub params: Map<String, Value>,
}

/// Classify a parsed frame as a `browser.*` request, or `None` to fall through
/// to the next fast-path / JSON-RPC dispatcher. Mirrors `host::classify`: a
/// JSON-RPC 2.0 object with a string `method` and an `id` (if present) that is
/// a string, number, or null.
pub(crate) fn classify(value: &Value) -> Option<BrowserRequest> {
    let obj = value.as_object()?;
    if obj.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return None;
    }
    let method = obj.get("method").and_then(Value::as_str)?;
    let id_member = obj.get("id");
    if let Some(v) = id_member {
        if !v.is_null() && !v.is_string() && !v.is_number() {
            return None;
        }
    }
    let method = match method {
        "browser.exec" => BrowserMethod::Exec,
        "browser.listTabs" => BrowserMethod::ListTabs,
        "browser.upsertTab" => BrowserMethod::UpsertTab,
        "browser.removeTab" => BrowserMethod::RemoveTab,
        "browser.syncTabs" => BrowserMethod::SyncTabs,
        _ => return None,
    };
    let params = obj
        .get("params")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    Some(BrowserRequest {
        method,
        id_present: id_member.is_some(),
        id_echo: id_member.cloned().unwrap_or(Value::Null),
        params,
    })
}

/// Handle a classified `browser.*` request. `browser.exec` validates the
/// envelope, forwards via the reverse channel, and shapes the FE's reply; the
/// registry methods run against `tabs` (persistence + the connection's host
/// identity). Returns `None` for a notification (no `id`), which gets no
/// response.
pub(crate) async fn handle(
    req: BrowserRequest,
    reverse: &ReverseChannel,
    tabs: TabContext<'_>,
) -> Option<String> {
    let BrowserRequest {
        method,
        id_present,
        id_echo,
        params,
    } = req;
    let frame = match method {
        BrowserMethod::Exec => match exec(&params, reverse).await {
            Ok(v) => success_frame(&id_echo, &v),
            Err(e) => error_frame(&id_echo, e.code(), &e.to_string()),
        },
        BrowserMethod::ListTabs => frame_result(&id_echo, list_tabs(&params, &tabs).await),
        BrowserMethod::UpsertTab => frame_result(&id_echo, upsert_tab(&params, &tabs).await),
        BrowserMethod::RemoveTab => frame_result(&id_echo, remove_tab(&params, &tabs).await),
        BrowserMethod::SyncTabs => frame_result(&id_echo, sync_tabs(&params, &tabs).await),
    };
    if !id_present {
        return None;
    }
    Some(frame)
}

fn frame_result(id_echo: &Value, result: Result<Value, (i32, String)>) -> String {
    match result {
        Ok(v) => success_frame(id_echo, &v),
        Err((code, message)) => error_frame(id_echo, code, &message),
    }
}

fn invalid(message: impl Into<String>) -> (i32, String) {
    (browser_ops::INVALID_PARAMS, message.into())
}

fn domain_err(e: &intent_core::Error) -> (i32, String) {
    (e.code(), e.to_string())
}

fn required_str<'a>(params: &'a Map<String, Value>, name: &str) -> Result<&'a str, (i32, String)> {
    params
        .get(name)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .ok_or_else(|| invalid(format!("Invalid parameter: {name} is required")))
}

/// The host-only methods require a hello'd connection: the reporting host is
/// the connection's logical `clientId`, never a wire parameter.
fn require_host(method: &str, tabs: &TabContext<'_>) -> Result<ClientId, (i32, String)> {
    tabs.client_id.cloned().ok_or_else(|| {
        invalid(format!(
            "{method}: client.hello is required before hosting tabs"
        ))
    })
}

/// Parse one host-reported tab. `workspace_id` (when given) overrides the
/// object's own `workspaceId` — `browser.upsertTab` carries it on the
/// envelope, `browser.syncTabs` per entry.
fn parse_tab_input(
    value: &Value,
    workspace_id: Option<&str>,
    what: &str,
) -> Result<BrowserTabInput, (i32, String)> {
    let Some(obj) = value.as_object() else {
        return Err(invalid(format!(
            "Invalid parameter: {what} must be an object"
        )));
    };
    let mut obj = obj.clone();
    if let Some(ws) = workspace_id {
        obj.insert("workspaceId".to_string(), Value::String(ws.to_string()));
    }
    let input: BrowserTabInput = serde_json::from_value(Value::Object(obj))
        .map_err(|e| invalid(format!("Invalid parameter: {what}: {e}")))?;
    if input.tab_id.is_empty() {
        return Err(invalid(format!(
            "Invalid parameter: {what}.tabId is required"
        )));
    }
    if input.workspace_id.0.is_empty() {
        return Err(invalid(format!(
            "Invalid parameter: {what}.workspaceId is required"
        )));
    }
    Ok(input)
}

/// `browser.listTabs { workspaceId }` → `{ tabs: [Tab & { hostName?,
/// hostConnected }] }`. Presence comes from the reverse registry's
/// mutation-maintained presence index
/// ([`crate::reverse::PrimaryReverseRegistry::host_presence`]): a host with
/// any live hello'd connection is `hostConnected` and carries its hello
/// `name`; an offline host has no name to report. Cost is one ordered index
/// scan for the rows plus one O(1) lookup per distinct host — O(rows
/// returned), never O(rows × connections).
async fn list_tabs(
    params: &Map<String, Value>,
    tabs: &TabContext<'_>,
) -> Result<Value, (i32, String)> {
    let workspace_id = required_str(params, "workspaceId")?;
    let rows = tabs
        .api
        .browser_list_tabs(WorkspaceId(workspace_id.to_string()))
        .await
        .map_err(|e| domain_err(&e))?;
    let hosts: HashSet<&ClientId> = rows.iter().map(|tab| &tab.host_client_id).collect();
    let presence = tabs
        .registry
        .map(|registry| registry.host_presence(&hosts))
        .unwrap_or_default();
    let decorated: Vec<Value> = rows
        .iter()
        .map(|tab| decorate_tab(tab, &presence))
        .collect();
    Ok(json!({ "tabs": decorated }))
}

fn decorate_tab(tab: &BrowserTab, presence: &HashMap<ClientId, ClientPresence>) -> Value {
    let mut value = json!(tab);
    let host = presence.get(&tab.host_client_id);
    if let Some(obj) = value.as_object_mut() {
        obj.insert("hostConnected".to_string(), Value::Bool(host.is_some()));
        if let Some(name) = host.and_then(|c| c.name.clone()) {
            obj.insert("hostName".to_string(), Value::String(name));
        }
    }
    value
}

/// `browser.upsertTab { workspaceId, tab }` (host only) → `{ tab }`.
async fn upsert_tab(
    params: &Map<String, Value>,
    tabs: &TabContext<'_>,
) -> Result<Value, (i32, String)> {
    let host = require_host("browser.upsertTab", tabs)?;
    let workspace_id = required_str(params, "workspaceId")?;
    let tab = params
        .get("tab")
        .ok_or_else(|| invalid("Invalid parameter: tab is required"))?;
    let input = parse_tab_input(tab, Some(workspace_id), "tab")?;
    let tab = tabs
        .api
        .browser_upsert_tab(host, input)
        .await
        .map_err(|e| domain_err(&e))?;
    Ok(json!({ "tab": tab }))
}

/// `browser.removeTab { tabId }` (host only) → `{ ok: true }`.
async fn remove_tab(
    params: &Map<String, Value>,
    tabs: &TabContext<'_>,
) -> Result<Value, (i32, String)> {
    let host = require_host("browser.removeTab", tabs)?;
    let tab_id = required_str(params, "tabId")?;
    tabs.api
        .browser_remove_tab(host, tab_id.to_string())
        .await
        .map_err(|e| domain_err(&e))?;
    Ok(json!({ "ok": true }))
}

/// `browser.syncTabs { tabs }` (host only) → `{ drop: tabId[] }`.
async fn sync_tabs(
    params: &Map<String, Value>,
    tabs: &TabContext<'_>,
) -> Result<Value, (i32, String)> {
    let host = require_host("browser.syncTabs", tabs)?;
    let snapshot = params
        .get("tabs")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid("Invalid parameter: tabs must be an array"))?;
    let inputs = snapshot
        .iter()
        .enumerate()
        .map(|(i, v)| parse_tab_input(v, None, &format!("tabs[{i}]")))
        .collect::<Result<Vec<_>, _>>()?;
    let drop = tabs
        .api
        .browser_sync_tabs(host, inputs)
        .await
        .map_err(|e| domain_err(&e))?;
    Ok(json!({ "drop": drop }))
}

/// Why a [`exec`] call could not be satisfied. `code()` maps each to a
/// standard JSON-RPC error code (PROTOCOL §9: `-32602` for invalid params,
/// `-32603` for internal / proxy failures).
#[derive(Debug)]
pub enum BrowserExecError {
    /// Envelope validation rejected the payload (missing / empty / non-array
    /// `actions`, or a non-string envelope field).
    InvalidParams(String),
    /// The FE-served reverse RPC failed / timed out / no client is connected,
    /// or the FE surfaced a failure envelope.
    Proxy(String),
}

impl BrowserExecError {
    /// JSON-RPC 2.0 numeric error code for this condition.
    pub fn code(&self) -> i32 {
        match self {
            BrowserExecError::InvalidParams(_) => browser_ops::INVALID_PARAMS,
            BrowserExecError::Proxy(_) => browser_ops::INTERNAL_ERROR,
        }
    }
}

impl fmt::Display for BrowserExecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BrowserExecError::InvalidParams(m) | BrowserExecError::Proxy(m) => f.write_str(m),
        }
    }
}

impl std::error::Error for BrowserExecError {}

/// Execute one `browser.exec` request: parse + validate the envelope, forward
/// it to the FE reverse intent, then reshape the FE's reply into the wire
/// result (single action → one result envelope, multiple → `{ results: [...]
/// }`). A closed outbound channel ("no frontend connected") and a reverse-RPC
/// timeout both surface as `-32603` with the underlying context so the caller
/// can distinguish them from a validation failure.
pub(crate) async fn exec(
    params: &Map<String, Value>,
    reverse: &ReverseChannel,
) -> Result<Value, BrowserExecError> {
    let args = browser_ops::parse_args(params).map_err(|e| {
        // Envelope validation errors are always `-32602`; the service module
        // pre-tagged them, so no need to re-classify here.
        BrowserExecError::InvalidParams(e.message)
    })?;
    let forwarded = browser_ops::build_forward_params(&args);
    let timeout = request_timeout("browser.exec", &forwarded);
    let fe_response = reverse
        .request("browser.exec", forwarded, timeout)
        .await
        .map_err(|e| BrowserExecError::Proxy(format!("browser.exec: {}", e.message)))?;
    browser_ops::shape_result(&fe_response).map_err(|e| BrowserExecError::Proxy(e.message))
}

#[cfg(test)]
mod tests;
