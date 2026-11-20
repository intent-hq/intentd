//! Generic daemon JSON-RPC for the Assistant (`ws.rpc.*`).
//!
//! The transport owns the method catalog and the loopback connection that
//! serves a call exactly like a desktop client's UDS frame. Services reach it
//! through [`AgentRpcSurface`], attached at the composition root, so the
//! approval registry and the `workspace_api` bindings never depend on the
//! transport crate.

use serde::Serialize;
use serde_json::Value;

use crate::BoxFuture;

/// How the agent path treats a method.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum AgentRpcClass {
    /// Runs immediately and returns its result.
    Read,
    /// Needs the user's approval on a card before it runs.
    Mutating,
    /// Refused on the agent path, even with approval.
    ForbiddenToAgent,
    /// Needs connection state the agent path does not have
    /// (`client.hello`, subscriptions, event streams).
    UnsupportedOnAgentPath,
}

impl AgentRpcClass {
    /// The wire label (`read`, `mutating`, `forbidden-to-agent`,
    /// `unsupported-on-agent-path`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Read => "read",
            Self::Mutating => "mutating",
            Self::ForbiddenToAgent => "forbidden-to-agent",
            Self::UnsupportedOnAgentPath => "unsupported-on-agent-path",
        }
    }
}

/// One row of `ws.rpc.methods()`.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AgentRpcMethod {
    pub method: String,
    pub class: AgentRpcClass,
    /// One-line summary of what the method returns or does.
    pub summary: String,
    /// Protocol doc page for the method's namespace, when one exists.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub doc: Option<String>,
}

/// A JSON-RPC error a call returned, or a local failure shaped like one.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AgentRpcError {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl AgentRpcError {
    #[must_use]
    pub fn new(code: i64, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: None,
        }
    }
}

/// `-32003` with `data.code = "forbidden-to-agent"`.
pub const AGENT_RPC_FORBIDDEN_CODE: i64 = -32003;
/// `-32601` with `data.code = "unsupported-on-agent-path"`.
pub const AGENT_RPC_UNSUPPORTED_CODE: i64 = -32601;
/// `-32603` with `data.code` naming the local failure (`agent-rpc-timeout`,
/// `agent-rpc-unavailable`). A timed-out call may still have run.
pub const AGENT_RPC_INTERNAL_CODE: i64 = -32603;

impl AgentRpcError {
    /// An error carrying `data: { code }`.
    #[must_use]
    pub fn coded(code: i64, data_code: &str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            data: Some(serde_json::json!({ "code": data_code })),
        }
    }

    /// The refusal for a class that never runs on the agent path
    /// (`summary` names why a forbidden method is refused).
    #[must_use]
    pub fn refusal(method: &str, class: AgentRpcClass, summary: Option<String>) -> Option<Self> {
        match class {
            AgentRpcClass::ForbiddenToAgent => Some(Self::coded(
                AGENT_RPC_FORBIDDEN_CODE,
                "forbidden-to-agent",
                format!(
                    "`{method}` is not available to the Assistant, even with approval: {}",
                    summary.unwrap_or_else(|| "owner-only".to_string())
                ),
            )),
            AgentRpcClass::UnsupportedOnAgentPath => Some(Self::coded(
                AGENT_RPC_UNSUPPORTED_CODE,
                "unsupported-on-agent-path",
                format!(
                    "`{method}` needs a client connection's state (hello, subscriptions or streams) and cannot run through ws.rpc"
                ),
            )),
            AgentRpcClass::Read | AgentRpcClass::Mutating => None,
        }
    }

    /// The `data.code` discriminator, when present.
    #[must_use]
    pub fn data_code(&self) -> Option<&str> {
        self.data.as_ref()?.get("code")?.as_str()
    }
}

/// The backend id of the daemon that owns the Assistant workspace.
pub const LOCAL_BACKEND_ID: &str = "local";

/// One call of a `ws.rpc.call` / `ws.rpc.batch` request.
#[derive(Debug, Clone, PartialEq, Serialize, serde::Deserialize)]
pub struct AgentRpcCallSpec {
    pub method: String,
    #[serde(default)]
    pub params: Value,
    /// Backend id; `None` is the local backend.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend: Option<String>,
}

/// A `ws.rpc.call` (one call, `batch: false`) or `ws.rpc.batch` request.
#[derive(Debug, Clone, PartialEq)]
pub struct AgentRpcRequest {
    pub calls: Vec<AgentRpcCallSpec>,
    pub batch: bool,
    /// Optional card title from the Assistant.
    pub title: Option<String>,
}

/// The daemon's own JSON-RPC surface, as seen by the agent path.
pub trait AgentRpcSurface: Send + Sync {
    /// `intentd --version` (e.g. `0.11.254`).
    fn daemon_version(&self) -> String;
    /// The wire protocol version (`client.hello` `protocolVersion`).
    fn protocol_version(&self) -> String;
    /// This machine's display name, shown on approval cards.
    fn local_backend_name(&self) -> String;
    /// Classify one method name (aliases canonicalized). Unknown names are
    /// [`AgentRpcClass::Mutating`] (fail closed).
    fn classify(&self, method: &str) -> AgentRpcClass;
    /// One-line summary of `method`, when cataloged.
    fn summary(&self, method: &str) -> Option<String>;
    /// Every method this daemon dispatches, sorted, with its class.
    fn methods(&self) -> Vec<AgentRpcMethod>;
    /// Run one call as the daemon's owner over an in-process connection.
    /// The caller bounds the wait; dropping the future abandons the reply.
    fn call(&self, method: String, params: Value) -> BoxFuture<'_, Result<Value, AgentRpcError>>;
}
