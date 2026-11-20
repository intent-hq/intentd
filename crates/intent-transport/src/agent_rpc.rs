//! The Assistant's generic JSON-RPC surface (`ws.rpc.*`): method classes
//! derived from [`PAYLOAD_CLASSES`] and the agent lists in [`crate::catalog`],
//! plus the in-process loopback that runs one call exactly like a desktop
//! client's UDS frame.

use std::sync::{Arc, Weak};

use intent_core::agent_rpc::{
    AgentRpcClass, AgentRpcError, AgentRpcMethod, AgentRpcSurface, AGENT_RPC_INTERNAL_CODE,
};
use intent_core::{BoxFuture, WorkspaceApi};
use intent_services::EventBus;
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::catalog::{
    canonical_method, PayloadClass, AGENT_FORBIDDEN_METHODS, AGENT_UNSUPPORTED_METHODS,
    PAYLOAD_CLASSES,
};
use crate::control::SystemControl;
use crate::rpc_limit::RpcLimiter;

/// Base URL of the canonical protocol docs (public monorepo, `main`).
pub const PROTOCOL_DOCS_URL: &str = "https://github.com/intent-hq/intent/tree/main/docs/protocol";
const PROTOCOL_DOC_FILE_URL: &str = "https://github.com/intent-hq/intent/blob/main/docs/protocol/";

/// Namespace → protocol doc page (relative to `docs/protocol/`).
const NAMESPACE_DOCS: &[(&str, &str)] = &[
    ("accept-changes", "methods/workspace.md"),
    ("agent", "methods/agents.md"),
    ("backends", "methods/agents.md"),
    ("browser", "methods/files-terminal-browser.md"),
    ("client", "methods/client-hello.md"),
    ("cloudMachines", "methods/nodes.md"),
    ("comment", "methods/notes-tasks.md"),
    ("crossWorkspace", "methods/misc-namespaces.md"),
    ("debug", "methods/system-observability.md"),
    ("desktop", "methods/desktop.md"),
    ("discussion", "methods/multiplayer.md"),
    ("drafts", "methods/search-drafts.md"),
    ("event", "06-events.md"),
    ("events", "06-events.md"),
    ("executor", "methods/nodes.md"),
    ("file", "methods/files-terminal-browser.md"),
    ("file-tracking", "methods/change-tracking.md"),
    ("git", "methods/git.md"),
    ("gitRoot", "methods/workspace.md"),
    ("github", "methods/integrations.md"),
    ("hook", "methods/hooks.md"),
    ("identity", "methods/integrations.md"),
    ("invite", "methods/multiplayer.md"),
    ("linear", "methods/integrations.md"),
    ("mcp", "methods/mcp-servers.md"),
    ("models", "methods/models-providers.md"),
    ("note", "methods/notes-tasks.md"),
    ("plugin", "methods/plugins.md"),
    ("pr", "methods/pr.md"),
    ("prMonitor", "methods/pr.md"),
    ("presence", "methods/presence.md"),
    ("principal", "methods/multiplayer.md"),
    ("providers", "methods/provider-profiles.md"),
    ("push", "methods/push.md"),
    ("remoteMachines", "methods/nodes.md"),
    ("rules", "methods/misc-namespaces.md"),
    ("script", "methods/scripts.md"),
    ("scriptMonitor", "methods/script-monitors.md"),
    ("search", "methods/search-drafts.md"),
    ("secret", "methods/secrets.md"),
    ("sentry", "methods/integrations.md"),
    ("server", "methods/client-hello.md"),
    ("settings", "methods/settings.md"),
    ("skill", "methods/misc-namespaces.md"),
    ("sourceControl", "methods/integrations.md"),
    ("specialist", "methods/misc-namespaces.md"),
    ("stats", "methods/system-observability.md"),
    ("task", "methods/notes-tasks.md"),
    ("terminal", "methods/files-terminal-browser.md"),
    ("unsloth", "methods/system-observability.md"),
    ("voice", "methods/voice.md"),
    ("workspace", "methods/workspace.md"),
];

fn payload_class(method: &str) -> Option<&'static crate::catalog::PayloadBudget> {
    PAYLOAD_CLASSES.iter().find(|row| row.method == method)
}

fn listed(list: &[(&str, &str)], method: &str) -> bool {
    list.iter().any(|(m, _)| *m == method)
}

fn is_channel_method(method: &str) -> bool {
    method.ends_with(".subscribe") || method.ends_with(".unsubscribe")
}

/// The agent class of `method` (aliases canonicalized). Order: forbidden,
/// unsupported, then [`PAYLOAD_CLASSES`]; an unknown name is mutating.
#[must_use]
pub fn agent_rpc_class(method: &str) -> AgentRpcClass {
    let method = canonical_method(method);
    if listed(AGENT_FORBIDDEN_METHODS, method) {
        return AgentRpcClass::ForbiddenToAgent;
    }
    if listed(AGENT_UNSUPPORTED_METHODS, method) || is_channel_method(method) {
        return AgentRpcClass::UnsupportedOnAgentPath;
    }
    match payload_class(method).map(|row| row.class) {
        Some(PayloadClass::Subscription) => AgentRpcClass::UnsupportedOnAgentPath,
        Some(PayloadClass::Mutation) | None => AgentRpcClass::Mutating,
        Some(_) => AgentRpcClass::Read,
    }
}

fn doc_url(method: &str) -> Option<String> {
    let namespace = method.split_once('.').map_or(method, |(ns, _)| ns);
    NAMESPACE_DOCS
        .iter()
        .find(|(ns, _)| *ns == namespace)
        .map(|(_, file)| format!("{PROTOCOL_DOC_FILE_URL}{file}"))
}

fn summary(method: &str) -> Option<String> {
    let method = canonical_method(method);
    let reason = AGENT_FORBIDDEN_METHODS
        .iter()
        .chain(AGENT_UNSUPPORTED_METHODS)
        .find(|(m, _)| *m == method)
        .map(|(_, r)| *r);
    let base = payload_class(method).map(|row| row.reason);
    match (base, reason) {
        (Some(base), Some(reason)) => Some(format!("{base} Agent path: {reason}")),
        (Some(base), None) => Some(base.to_string()),
        (None, reason) => reason.map(str::to_string),
    }
}

/// Every method this daemon dispatches ([`PAYLOAD_CLASSES`] covers every
/// router and fast-path method), with its agent class.
#[must_use]
pub fn agent_rpc_methods() -> Vec<AgentRpcMethod> {
    PAYLOAD_CLASSES
        .iter()
        .map(|row| AgentRpcMethod {
            method: row.method.to_string(),
            class: agent_rpc_class(row.method),
            summary: summary(row.method).unwrap_or_default(),
            doc: doc_url(row.method),
        })
        .collect()
}

/// The refusal for a class that never runs on the agent path.
#[must_use]
pub fn refusal(method: &str, class: AgentRpcClass) -> Option<AgentRpcError> {
    AgentRpcError::refusal(method, class, summary(method))
}

/// The loopback surface attached to services at the composition root.
pub struct LoopbackAgentRpc {
    api: Weak<dyn WorkspaceApi>,
    bus: EventBus,
    control: Option<Arc<dyn SystemControl>>,
    server_pairing_info: Option<Arc<dyn crate::server::ServerPairingInfo>>,
    limiter: RpcLimiter,
    daemon_version: String,
    reverse_registry: Option<Arc<crate::reverse::PrimaryReverseRegistry>>,
}

/// The `data.code` of a relay that failed before or while delivering.
fn relay_error(code: &str, message: impl Into<String>) -> AgentRpcError {
    AgentRpcError::coded(AGENT_RPC_INTERNAL_CODE, code, message)
}

impl LoopbackAgentRpc {
    /// `api` is held weakly: services own this surface, so a strong handle
    /// would keep them alive in a cycle.
    #[must_use]
    pub fn new(
        api: &Arc<dyn WorkspaceApi>,
        bus: EventBus,
        control: Option<Arc<dyn SystemControl>>,
        server_pairing_info: Option<Arc<dyn crate::server::ServerPairingInfo>>,
        limiter: RpcLimiter,
        daemon_version: impl Into<String>,
    ) -> Self {
        Self {
            api: Arc::downgrade(api),
            bus,
            control,
            server_pairing_info,
            limiter,
            daemon_version: daemon_version.into(),
            reverse_registry: None,
        }
    }

    /// Relay remote-backend calls through owner apps on `registry`
    /// (`backends.relayCall`).
    #[must_use]
    pub fn with_reverse_registry(
        mut self,
        registry: Arc<crate::reverse::PrimaryReverseRegistry>,
    ) -> Self {
        self.reverse_registry = Some(registry);
        self
    }

    #[cfg(any(unix, windows))]
    async fn run(&self, method: String, params: Value) -> Result<Value, AgentRpcError> {
        let unavailable = |detail: String| {
            AgentRpcError::coded(AGENT_RPC_INTERNAL_CODE, "agent-rpc-unavailable", detail)
        };
        let api = self
            .api
            .upgrade()
            .ok_or_else(|| unavailable("the daemon is shutting down".to_string()))?;
        let stream = crate::listener::spawn_loopback_connection(
            api,
            self.bus.clone(),
            self.control.clone(),
            self.server_pairing_info.clone(),
            self.limiter.clone(),
        );
        let (read_half, mut write_half) = tokio::io::split(stream);
        let id = "agent-rpc";
        let mut frame =
            json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }).to_string();
        frame.push('\n');
        write_half
            .write_all(frame.as_bytes())
            .await
            .map_err(|e| unavailable(format!("loopback write failed: {e}")))?;
        let mut reader = BufReader::new(read_half);
        let mut line = String::new();
        loop {
            line.clear();
            let read = reader
                .read_line(&mut line)
                .await
                .map_err(|e| unavailable(format!("loopback read failed: {e}")))?;
            if read == 0 {
                return Err(unavailable("loopback closed before a reply".to_string()));
            }
            let Ok(reply) = serde_json::from_str::<Value>(line.trim_end()) else {
                continue;
            };
            if reply.get("id").and_then(Value::as_str) != Some(id) {
                continue;
            }
            return decode_reply(reply);
        }
    }

    #[cfg(not(any(unix, windows)))]
    async fn run(&self, _method: String, _params: Value) -> Result<Value, AgentRpcError> {
        Err(AgentRpcError::coded(
            AGENT_RPC_INTERNAL_CODE,
            "agent-rpc-unavailable",
            "ws.rpc is not supported on this platform",
        ))
    }
}

fn decode_reply(mut reply: Value) -> Result<Value, AgentRpcError> {
    if let Some(error) = reply.get_mut("error").map(Value::take) {
        return Err(AgentRpcError {
            code: error
                .get("code")
                .and_then(Value::as_i64)
                .unwrap_or(AGENT_RPC_INTERNAL_CODE),
            message: error
                .get("message")
                .and_then(Value::as_str)
                .unwrap_or("error")
                .to_string(),
            data: error.get("data").cloned(),
        });
    }
    Ok(reply.get_mut("result").map_or(Value::Null, Value::take))
}

impl AgentRpcSurface for LoopbackAgentRpc {
    fn daemon_version(&self) -> String {
        self.daemon_version.clone()
    }

    fn protocol_version(&self) -> String {
        crate::protocol::PROTOCOL_VERSION.to_string()
    }

    fn local_backend_name(&self) -> String {
        crate::host_env::pretty_hostname()
    }

    fn classify(&self, method: &str) -> AgentRpcClass {
        agent_rpc_class(method)
    }

    fn summary(&self, method: &str) -> Option<String> {
        summary(method)
    }

    fn methods(&self) -> Vec<AgentRpcMethod> {
        agent_rpc_methods()
    }

    fn call(&self, method: String, params: Value) -> BoxFuture<'_, Result<Value, AgentRpcError>> {
        Box::pin(async move {
            if let Some(refusal) = refusal(&method, agent_rpc_class(&method)) {
                return Err(refusal);
            }
            self.run(method, params).await
        })
    }

    fn local_fingerprint(&self) -> Option<String> {
        self.control.as_ref()?.status().fingerprint
    }

    fn relay_online(&self, client_id: &str) -> bool {
        self.reverse_registry.as_ref().is_some_and(|registry| {
            registry
                .relay_channel(&intent_core::ClientId::from(client_id))
                .is_some()
        })
    }

    fn relay_call(
        &self,
        client_id: String,
        backend_id: String,
        method: String,
        params: Value,
        timeout: std::time::Duration,
    ) -> BoxFuture<'_, Result<Value, AgentRpcError>> {
        Box::pin(async move {
            let channel = self
                .reverse_registry
                .as_ref()
                .and_then(|r| r.relay_channel(&intent_core::ClientId::from(client_id.as_str())))
                .ok_or_else(|| {
                    relay_error("no-relay-online", "the reporting app is not connected")
                })?;
            let request = json!({
                "backendId": backend_id,
                "method": method,
                "params": params,
                "timeoutMs": u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX),
            });
            match channel.request(RELAY_METHOD, request, timeout).await {
                Ok(reply) => decode_reply(reply),
                Err(e) if e.timed_out => Err(relay_error(
                    "relay-timeout",
                    format!("`{method}` relayed to backend `{backend_id}` did not answer in time; it may still run"),
                )),
                // The app answered with a transport-level error of its own.
                Err(e) if e.code != 0 => Err(AgentRpcError {
                    code: e.code,
                    message: e.message,
                    data: e.data,
                }),
                Err(_) => Err(relay_error(
                    "relay-disconnected",
                    format!("the app relaying `{method}` to backend `{backend_id}` disconnected; the call may have run"),
                )),
            }
        })
    }
}

/// The daemon→client reverse RPC that relays one call to another backend.
pub const RELAY_METHOD: &str = "backends.relayCall";

#[cfg(test)]
mod tests;
