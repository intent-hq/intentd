//! Opt-in callback registration on one original ACP connection and Query.
//! Receipts and tool routes are transport correlation, never repository authority.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use agent_client_protocol::schema::v1::Meta;
use agent_client_protocol::schema::v1::{LoadSessionResponse, McpServer, NewSessionResponse};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

use crate::error::{AcpError, AcpResult};
use crate::handshake::HandshakeResult;
use crate::transport::{CallbackRequestOutcome, Connection};

pub(crate) const META_KEY: &str = "intentCallbackRegistration";
pub(crate) const METHOD: &str = "_intent/session/register_mcp_callback";
const REGISTRATION_TIMEOUT: Duration = Duration::from_secs(6);

/// An explicit local offer; ordinary handshakes always use `Disabled`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CallbackOffer {
    /// Do not offer or use the extension, even if the peer advertises it.
    #[default]
    Disabled,
    /// Offer the pinned, versioned registration contract.
    V1,
}

/// Ordinary initialization plus optional original-connection correlation.
pub struct CallbackHandshake {
    /// Complete unchanged ordinary handshake result.
    pub ordinary: HandshakeResult,
    /// Present only after exact opt-in negotiation.
    pub callbacks: Option<CallbackClient>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Capability {
    version: u8,
    method: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct Receipt {
    version: u8,
    query_receipt: String,
}

/// A negotiated client bound privately to its original connection.
pub struct CallbackClient {
    connection: Arc<Connection>,
}

impl CallbackClient {
    pub(crate) fn negotiated(
        connection: Arc<Connection>,
        offer: CallbackOffer,
        meta: Option<&Meta>,
    ) -> Option<Self> {
        if offer != CallbackOffer::V1 {
            return None;
        }
        let capability: Capability = serde_json::from_value(meta?.get(META_KEY)?.clone()).ok()?;
        (capability.version == 1 && capability.method == METHOD).then_some(Self { connection })
    }

    /// Execute the original `session/new` once and retain its optional receipt.
    ///
    /// # Errors
    /// Returns the unchanged ordinary ACP error.
    pub async fn new_session(
        &self,
        cwd: impl Into<PathBuf>,
        mcp_servers: Vec<McpServer>,
        meta: Option<Meta>,
    ) -> AcpResult<CallbackSession<NewSessionResponse>> {
        let response =
            crate::session::new_session(&self.connection, cwd, mcp_servers, meta).await?;
        let query = self.capture(response.session_id.to_string(), response.meta.as_ref());
        Ok(CallbackSession { response, query })
    }

    /// Execute the original load once; correlate with its original requested ID.
    ///
    /// # Errors
    /// Returns the unchanged ordinary ACP error.
    pub async fn load_session(
        &self,
        session_id: &str,
        cwd: impl Into<PathBuf>,
        mcp_servers: Vec<McpServer>,
        meta: Option<Meta>,
    ) -> AcpResult<CallbackSession<LoadSessionResponse>> {
        let response =
            crate::session::load_session(&self.connection, session_id, cwd, mcp_servers, meta)
                .await?;
        let query = self.capture(session_id.to_owned(), response.meta.as_ref());
        Ok(CallbackSession { response, query })
    }

    fn capture(&self, session_id: String, meta: Option<&Meta>) -> Option<CallbackQuery> {
        let receipt: Receipt = serde_json::from_value(meta?.get(META_KEY)?.clone()).ok()?;
        (receipt.version == 1 && !session_id.is_empty() && canonical_uuid(&receipt.query_receipt))
            .then(|| CallbackQuery {
                connection: Arc::clone(&self.connection),
                session_id,
                receipt: receipt.query_receipt,
            })
    }
}

/// The complete original response and its optional, nonclone sidecar.
pub struct CallbackSession<T> {
    /// Successful ordinary response, including all extension metadata.
    pub response: T,
    /// No receipt is synthesized for missing or malformed metadata.
    pub query: Option<CallbackQuery>,
}

/// A single original Query receipt; cannot be constructed from current IDs.
pub struct CallbackQuery {
    connection: Arc<Connection>,
    session_id: String,
    receipt: String,
}

/// Known tools at the offered endpoint. This list only controls attribution.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum CallbackTool {
    /// The workspace API tool at the immutable callback endpoint.
    WorkspaceApi,
}

/// Immutable stdio configuration; callers cannot select or redirect an alias.
pub struct CallbackStdioServer {
    command: String,
    args: Vec<String>,
    env: Option<BTreeMap<String, String>>,
    tools: BTreeSet<CallbackTool>,
}

impl CallbackStdioServer {
    /// Capture the complete configuration and known tool set before any await.
    ///
    /// # Errors
    /// Rejects an empty command, which the pinned adapter cannot accept.
    pub fn new(
        command: String,
        args: Vec<String>,
        env: Option<BTreeMap<String, String>>,
        tools: impl IntoIterator<Item = CallbackTool>,
    ) -> AcpResult<Self> {
        if command.is_empty() {
            return Err(AcpError::Protocol("empty callback command".into()));
        }
        Ok(Self {
            command,
            args,
            env,
            tools: tools.into_iter().collect(),
        })
    }
}

impl CallbackQuery {
    /// Consume this sidecar and synchronously capture one registration identity.
    #[must_use]
    pub fn registration(self, server: CallbackStdioServer) -> CallbackRegistration {
        CallbackRegistration {
            query: self,
            server,
            id: Uuid::new_v4().to_string(),
        }
    }
}

/// A consuming registration; neither its receipt nor configuration can change.
pub struct CallbackRegistration {
    query: CallbackQuery,
    server: CallbackStdioServer,
    id: String,
}

/// A local failure, separate from the adapter's factual outcome.
#[derive(Debug)]
pub enum CallbackFailure {
    /// Caller cancellation won the local race.
    Cancelled,
    /// The single writer-plus-response deadline expired.
    Deadline,
    /// Original transport or JSON-RPC error, without string reclassification.
    Connection(AcpError),
}

/// One terminal registration observation; none of these variants grants access.
#[derive(Debug)]
pub enum CallbackDeliveryOutcome {
    /// Fully correlated adapter response, with all reported effects retained.
    Remote(CallbackRemoteOutcome),
    /// The original request never entered the writer queue.
    NotSent(CallbackFailure),
    /// It may have executed; no retry or no-effect claim follows.
    Unknown(CallbackFailure),
    /// Received evidence failed correlation or schema validation; retained verbatim.
    Malformed(Value),
}

/// Status reported by the pinned adapter.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum CallbackStatus {
    /// SDK acknowledged the new name without removals or errors.
    Acknowledged,
    /// SDK reported errors or removals.
    Failed,
    /// Dispatch may have executed without a conclusive result.
    Uncertain,
    /// Original Query was replaced or closed.
    Stale,
    /// Adapter did not dispatch the SDK mutation.
    NotDispatched,
}

/// Original SDK effects, including failures and removals.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CallbackEffects {
    /// Names actually reported added.
    pub added: Vec<String>,
    /// Names actually reported removed.
    pub removed: Vec<String>,
    /// Original per-name errors.
    pub errors: BTreeMap<String, String>,
}

/// Original adapter reason, not a source of authority or effect inference.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum CallbackReason {
    /// Adapter diagnostic string.
    Text(String),
    /// Structured adapter error.
    Error(CallbackRemoteError),
}

/// Structured diagnostic from the SDK or adapter.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct CallbackRemoteError {
    /// Original error classification.
    pub code: CallbackRemoteErrorCode,
    /// Original diagnostic.
    pub message: String,
}

/// Supported adapter error classifications.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub enum CallbackRemoteErrorCode {
    /// SDK error, including before mutation dispatch.
    SdkError,
    /// Adapter internal error.
    Internal,
}

/// Fully validated response to this exact registration.
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CallbackRemoteOutcome {
    /// Schema version, always one after validation.
    pub version: u8,
    /// Original session correlation.
    pub session_id: String,
    /// Original Query allocation receipt.
    pub query_receipt: String,
    /// Original one-use registration identity.
    pub registration_id: String,
    /// Distinct adapter-generated immutable server name.
    pub server_name: String,
    /// Adapter's terminal status.
    pub status: CallbackStatus,
    /// Original SDK result or null when unavailable.
    pub result: Option<CallbackEffects>,
    /// Optional original diagnostic.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<CallbackReason>,
}

impl CallbackRegistration {
    /// Register once on the captured connection, bounded across queue and response.
    /// Dropping an unfinished future removes its pending entry before attempting
    /// cancellation of that same request. No detached work or retry is started.
    pub async fn run(self, cancelled: impl Future<Output = ()>) -> CallbackDeliveryOutcome {
        self.run_with_timeout(cancelled, REGISTRATION_TIMEOUT).await
    }

    async fn run_with_timeout(
        self,
        cancelled: impl Future<Output = ()>,
        timeout: Duration,
    ) -> CallbackDeliveryOutcome {
        let mut server =
            json!({"type":"stdio", "command": self.server.command, "args": self.server.args});
        if let Some(env) = &self.server.env {
            server["env"] = json!(env);
        }
        let params = json!({"version":1, "sessionId":self.query.session_id,
            "queryReceipt":self.query.receipt, "registrationId":self.id, "server":server});
        match self
            .query
            .connection
            .request_callback(METHOD, params, timeout, cancelled)
            .await
        {
            CallbackRequestOutcome::Response(raw) => self.validate(raw),
            CallbackRequestOutcome::NotSent(error) => CallbackDeliveryOutcome::NotSent(error),
            CallbackRequestOutcome::Unknown(error) => CallbackDeliveryOutcome::Unknown(error),
        }
    }

    fn validate(&self, raw: Value) -> CallbackDeliveryOutcome {
        let Ok(outcome) = serde_json::from_value::<CallbackRemoteOutcome>(raw.clone()) else {
            return CallbackDeliveryOutcome::Malformed(raw);
        };
        let valid_name = outcome
            .server_name
            .strip_prefix("intent-callback-")
            .is_some_and(canonical_uuid);
        let acknowledged = outcome.status != CallbackStatus::Acknowledged
            || outcome.result.as_ref().is_some_and(|r| {
                r.added.contains(&outcome.server_name)
                    && r.removed.is_empty()
                    && r.errors.is_empty()
            });
        if outcome.version != 1
            || outcome.session_id != self.query.session_id
            || outcome.query_receipt != self.query.receipt
            || outcome.registration_id != self.id
            || !valid_name
            || !acknowledged
            || raw.get("result").is_none()
            || raw.get("reason").is_some_and(Value::is_null)
        {
            return CallbackDeliveryOutcome::Malformed(raw);
        }
        self.query
            .connection
            .callback_tool_routes()
            .record(&outcome, &self.server.tools);
        CallbackDeliveryOutcome::Remote(outcome)
    }
}

fn canonical_uuid(value: &str) -> bool {
    Uuid::parse_str(value).is_ok_and(|id| id.get_version_num() == 4 && id.to_string() == value)
}

/// Append-only attribution for one original connection; never an alias redirect.
#[derive(Default)]
pub struct CallbackToolRoutes {
    names: Mutex<BTreeSet<(String, String)>>,
}

impl CallbackToolRoutes {
    fn record(&self, outcome: &CallbackRemoteOutcome, tools: &BTreeSet<CallbackTool>) {
        if tools.contains(&CallbackTool::WorkspaceApi) {
            self.names
                .lock()
                .unwrap()
                .insert((outcome.session_id.clone(), outcome.server_name.clone()));
        }
    }

    pub(crate) fn matches_workspace_api(
        &self,
        session_id: &str,
        title: &str,
        mapped_name: &str,
    ) -> bool {
        let Some(name) = title
            .strip_prefix("mcp__")
            .and_then(|s| s.strip_suffix("__workspace_api"))
        else {
            return false;
        };
        mapped_name == format!("{name}_workspace_api")
            && self
                .names
                .lock()
                .unwrap()
                .contains(&(session_id.to_owned(), name.to_owned()))
    }
}

#[cfg(test)]
mod tests;
