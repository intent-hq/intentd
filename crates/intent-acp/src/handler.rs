//! Client-served request handler: dispatch agent→client `fs/*`, `terminal/*`,
//! and `session/request_permission` requests and answer them over the
//! [`Connection`] (§6.7).
//!
//! `intent-acp` must not depend on `intent-services` (where the event bus and
//! store live), so the handler emits its side-effect events through an
//! [`EventSink`] the service layer implements (mirroring how `agent_session`
//! publishes mapped `session/update`s). The fs/permission logic itself lives in
//! the sibling [`crate::fs`] / [`crate::permission`] modules; this module only
//! wires them to the wire and the sink.

use intent_core::events::{AGENT_PERMISSION_REQUEST, AGENT_PERMISSION_RESOLVED, FILE_CHANGED};
use intent_core::{ActorType, AgentId, BoxFuture, EventActor, WorkspaceId};
use serde_json::{json, Value};

use agent_client_protocol::schema::{
    ReadTextFileRequest, ReadTextFileResponse, RequestPermissionRequest, WriteTextFileRequest,
    WriteTextFileResponse,
};

use crate::error::{AcpError, AcpResult, JsonRpcError};
use crate::permission::{self, PermissionOutcome, PermissionPolicy, PermissionRegistry};
use crate::transport::{Connection, IncomingRequest};
use crate::{fs, terminal};
use std::sync::Arc;

/// JSON-RPC error codes used when answering client-served requests.
const INVALID_PARAMS: i64 = -32602;
const INTERNAL_ERROR: i64 = -32603;

/// An event the handler asks the service layer to append + broadcast. Mirrors
/// the fields of `intent_store::NewEvent` the sink needs (the sink stamps the
/// timestamp and persists it).
pub struct SinkEvent {
    /// Workspace the event belongs to.
    pub workspace_id: WorkspaceId,
    /// Canonical `Event::event_type` string.
    pub event_type: String,
    /// Originating actor (the agent).
    pub actor: EventActor,
    /// Correlated session id (the agent id).
    pub session_id: Option<String>,
    /// Type-specific payload.
    pub data: Value,
}

/// Sink the handler publishes its side-effect events through; implemented by the
/// service layer over the M2 event bus (append-then-broadcast).
pub trait EventSink: Send + Sync {
    /// Append + broadcast one event.
    fn publish(&self, event: SinkEvent) -> BoxFuture<'_, ()>;
}

/// Serves the agent→client requests for one agent session: sandboxed file IO,
/// mediated permission prompts, and the terminal stub.
pub struct ClientRequestHandler {
    workspace_id: WorkspaceId,
    agent_id: AgentId,
    agent_name: String,
    files: fs::FileService,
    permissions: Arc<PermissionRegistry>,
    policy: PermissionPolicy,
    sink: Arc<dyn EventSink>,
}

impl ClientRequestHandler {
    /// Wire a handler for one agent session.
    pub fn new(
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        agent_name: impl Into<String>,
        files: fs::FileService,
        permissions: Arc<PermissionRegistry>,
        policy: PermissionPolicy,
        sink: Arc<dyn EventSink>,
    ) -> Self {
        Self {
            workspace_id,
            agent_id,
            agent_name: agent_name.into(),
            files,
            permissions,
            policy,
            sink,
        }
    }

    /// Dispatch one incoming request and answer it over `conn`.
    pub async fn serve(&self, conn: &Connection, req: IncomingRequest) -> AcpResult<()> {
        let IncomingRequest { id, method, params } = req;
        match method.as_str() {
            "fs/read_text_file" => self.handle_read(conn, id, params).await,
            "fs/write_text_file" => self.handle_write(conn, id, params).await,
            "session/request_permission" => self.handle_permission(conn, id, params).await,
            m if terminal::is_terminal_method(m) => {
                conn.respond_error(id, terminal::unsupported_error(m)).await
            }
            other => {
                conn.respond_error(
                    id,
                    JsonRpcError {
                        code: -32601,
                        message: format!("method not found: {other}"),
                        data: None,
                    },
                )
                .await
            }
        }
    }

    async fn handle_read(&self, conn: &Connection, id: Value, params: Value) -> AcpResult<()> {
        let parsed: ReadTextFileRequest = match serde_json::from_value(params) {
            Ok(p) => p,
            Err(e) => return conn.respond_error(id, invalid_params(e)).await,
        };
        match self.files.read(&parsed.path).await {
            Ok(content) => {
                let result = serde_json::to_value(ReadTextFileResponse::new(content))?;
                conn.respond_result(id, result).await
            }
            Err(e) => conn.respond_error(id, fs_error(&e)).await,
        }
    }

    async fn handle_write(&self, conn: &Connection, id: Value, params: Value) -> AcpResult<()> {
        let parsed: WriteTextFileRequest = match serde_json::from_value(params) {
            Ok(p) => p,
            Err(e) => return conn.respond_error(id, invalid_params(e)).await,
        };
        match self.files.write(&parsed.path, &parsed.content).await {
            Ok(change) => {
                let result = serde_json::to_value(WriteTextFileResponse::new())?;
                conn.respond_result(id, result).await?;
                self.emit(
                    FILE_CHANGED,
                    json!({
                        "path": change.relative_path,
                        "relativePath": change.relative_path,
                        "action": change.action.as_str(),
                    }),
                )
                .await;
                Ok(())
            }
            Err(e) => conn.respond_error(id, fs_error(&e)).await,
        }
    }

    async fn handle_permission(
        &self,
        conn: &Connection,
        id: Value,
        params: Value,
    ) -> AcpResult<()> {
        let parsed: RequestPermissionRequest = match serde_json::from_value(params) {
            Ok(p) => p,
            Err(e) => return conn.respond_error(id, invalid_params(e)).await,
        };
        let request_id = self.permissions.next_request_id();
        let data = permission::normalize_request(
            request_id.clone(),
            self.agent_id.0.clone(),
            self.agent_name.clone(),
            &parsed,
        );

        let outcome = match self.policy.auto_allow(data.risk_level) {
            // Headless: emit the prompt for observability, then resolve it now.
            Some(allow) => {
                self.emit(AGENT_PERMISSION_REQUEST, request_value(&data))
                    .await;
                match permission::select_option(&data.options, allow) {
                    Some(option_id) => PermissionOutcome::Selected { option_id },
                    None => PermissionOutcome::Cancelled,
                }
            }
            // Interactive: register, surface, and block until answered or timeout.
            None => {
                let rx = self.permissions.register(data.clone());
                self.emit(AGENT_PERMISSION_REQUEST, request_value(&data))
                    .await;
                match tokio::time::timeout(self.permissions.timeout(), rx).await {
                    Ok(Ok(outcome)) => outcome,
                    _ => {
                        self.permissions.remove(&request_id);
                        PermissionOutcome::Cancelled
                    }
                }
            }
        };

        self.emit(
            AGENT_PERMISSION_RESOLVED,
            json!({ "requestId": request_id, "outcome": outcome.to_event_value() }),
        )
        .await;
        conn.respond_result(id, outcome.to_response_value()).await
    }

    /// Publish one event attributed to this agent onto the sink.
    async fn emit(&self, event_type: &str, data: Value) {
        self.sink
            .publish(SinkEvent {
                workspace_id: self.workspace_id.clone(),
                event_type: event_type.to_string(),
                actor: EventActor {
                    actor_type: ActorType::Agent,
                    id: Some(self.agent_id.0.clone()),
                    name: Some(self.agent_name.clone()),
                    ..Default::default()
                },
                session_id: Some(self.agent_id.0.clone()),
                data,
            })
            .await;
    }
}

/// Serialize a permission request payload for the `agent:permission:request`
/// event (`PermissionRequestData` is infallible to serialize).
fn request_value(data: &permission::PermissionRequestData) -> Value {
    serde_json::to_value(data).unwrap_or_else(|_| json!({}))
}

/// Map a parse failure to an `invalid params` JSON-RPC error.
fn invalid_params(e: serde_json::Error) -> JsonRpcError {
    JsonRpcError {
        code: INVALID_PARAMS,
        message: format!("invalid params: {e}"),
        data: None,
    }
}

/// Map a filesystem failure (sandbox violation or IO error) to an `internal`
/// JSON-RPC error carrying the reason.
fn fs_error(e: &AcpError) -> JsonRpcError {
    JsonRpcError {
        code: INTERNAL_ERROR,
        message: e.to_string(),
        data: None,
    }
}
