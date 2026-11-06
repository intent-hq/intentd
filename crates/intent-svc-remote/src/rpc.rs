//! Head's answer to the node's forwarded `workspace_api` calls.
//!
//! A remote agent evaluates its `workspace_api` code on its node and
//! forwards the control-plane frames (notes, tasks, agents, PRs, comments,
//! events, primitives, discussions, sibling workspaces, the backend's
//! workspace data, hooks, structured questions, the browser and the
//! desktop) to head as [`CALL_METHOD`] link requests. Head serves one only on
//! the node's current binding, only for an agent recorded on that binding's
//! lease, and only for a frame its own routing table forwards (the node is
//! not trusted to route). The frame then runs through the same host
//! dispatch a local agent's bridge uses, as that agent, in its workspace.
//! The node's internal frames (its commit-policy read, a call's cards, the
//! placement text and an attachment's bytes) are answered here directly.

use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;

use intent_core::node_link::{
    LeaseBinding, LinkOutcome, LinkRequest, LinkRpcError, MAX_FRAME_BYTES,
};
use intent_core::remote_launch::workspace_api::{
    self, Route, ATTACHMENTS_METHOD, ATTACHMENT_DATA_METHOD, CALL_METHOD, COMMIT_POLICY_METHOD,
    INTERNAL_METHODS, MAX_FORWARDED_ATTACHMENTS, MAX_LINK_ATTACHMENT_BYTES,
    MCP_PROXY_CALL_TOOL_METHOD, MCP_PROXY_LINK_TIMEOUT, MCP_PROXY_LIST_TOOLS_METHOD,
    PLACEMENT_HELP_METHOD,
};
use intent_core::remote_launch::RemoteAgentRecord;
use intent_core::{with_caller, AgentSession, Caller, Error, WorkspaceApi};
use intent_services::feature_api::Services;
use intent_transport::node_link::IncomingRequest;

use crate::connector::Shared;

/// JSON-RPC "method not found".
const METHOD_NOT_FOUND: i32 = -32601;
/// JSON-RPC "invalid params".
const INVALID_PARAMS: i32 = -32602;
/// The frame was refused: not this lease's agent, or not forwarded.
const FORBIDDEN: i32 = -32003;
/// The frame ran and failed; the message is the `ws.*` error.
const CALL_FAILED: i32 = -32000;
/// Longest budget a forwarded frame gets, whatever the node asks for.
const MAX_BUDGET: Duration = Duration::from_secs(300);
/// Room the response frame needs beyond its result.
const RESPONSE_OVERHEAD: usize = 4 * 1024;

fn refusal(code: i32, message: impl Into<String>) -> LinkOutcome {
    LinkOutcome::Err(LinkRpcError {
        code,
        message: message.into(),
        data: None,
    })
}

/// Answers forwarded calls with head's services.
#[derive(Clone)]
pub(crate) struct HeadCalls {
    services: Services,
}

impl HeadCalls {
    pub(crate) fn new(services: Services) -> Self {
        Self { services }
    }

    /// Answer `request` on its own task, giving up if the node cancels it.
    pub(crate) fn answer(&self, shared: Arc<Shared>, request: IncomingRequest) {
        let calls = self.clone();
        // caller-binding: allow — the request is checked against the lease, then dispatched with the agent's caller bound in `outcome`
        tokio::spawn(async move {
            let current = shared.current_link().map(|link| link.binding().clone());
            let outcome = tokio::select! {
                outcome = calls.outcome(&shared, current.as_ref(), request.binding(), request.request()) => outcome,
                () = request.cancelled() => return,
            };
            let _ = request.respond(outcome).await;
        });
    }

    /// The outcome of one request that arrived on `binding`, given the
    /// node's `current` binding.
    pub(crate) async fn outcome(
        &self,
        shared: &Shared,
        current: Option<&LeaseBinding>,
        binding: &LeaseBinding,
        request: &LinkRequest,
    ) -> LinkOutcome {
        if request.method != CALL_METHOD {
            return refusal(
                METHOD_NOT_FOUND,
                format!(
                    "remote-request-unsupported: head does not serve `{}`",
                    request.method
                ),
            );
        }
        if current.is_none_or(|current| binding.ensure_current(current).is_err()) {
            return refusal(FORBIDDEN, "node-link-stale: the link was replaced");
        }
        let Some(method) = request.params.get("method").and_then(Value::as_str) else {
            return refusal(INVALID_PARAMS, "workspace.call needs a string `method`");
        };
        let internal = INTERNAL_METHODS.contains(&method);
        if !internal && workspace_api::route(method) != Route::Forward {
            return refusal(FORBIDDEN, workspace_api::unsupported_message(method));
        }
        // A frame whose link support is newer than this link (agent
        // secrets, nodeProtocol 14) is refused as the machine would.
        if method.starts_with("secret.")
            && shared.protocol() < intent_core::node_link::SECRETS_PROTOCOL
        {
            return refusal(FORBIDDEN, workspace_api::forward_refusal(method));
        }
        let record = match shared.store.get_remote_agent(&request.agent_id).await {
            Ok(Some(record)) if record.lease_id == binding.lease_id => record,
            Ok(_) => {
                return refusal(
                    FORBIDDEN,
                    "node-agent-denied: the agent is not running on this node's lease",
                )
            }
            Err(error) => return refusal(CALL_FAILED, error.to_string()),
        };
        let session = match self
            .services
            .store()
            .get_agent_session_without_messages(&record.agent_id)
            .await
        {
            Ok(session) => session,
            Err(Error::NotFound(_)) => {
                return refusal(FORBIDDEN, "node-agent-denied: the agent no longer exists")
            }
            Err(error) => return refusal(CALL_FAILED, error.to_string()),
        };
        // Every frame past the lease check runs as the leased agent, so no
        // branch can reach a capability gate unbound (intent#8630).
        let caller = Caller::Agent {
            agent_id: record.agent_id.clone(),
        };
        with_caller(caller, self.serve(&record, &session, method, request)).await
    }

    /// Serve one admitted frame for `record`'s agent; the caller is bound.
    async fn serve(
        &self,
        record: &RemoteAgentRecord,
        session: &AgentSession,
        method: &str,
        request: &LinkRequest,
    ) -> LinkOutcome {
        if method == COMMIT_POLICY_METHOD {
            let enabled = self
                .services
                .effective_auto_commit(&record.workspace_id)
                .await;
            return LinkOutcome::Ok(serde_json::json!({ "autoCommit": enabled }));
        }
        if method == PLACEMENT_HELP_METHOD {
            let help = self
                .services
                .feature_remote_launch_port()
                .placement_help(&self.services)
                .await;
            return LinkOutcome::Ok(serde_json::json!({ "help": help }));
        }
        if method == ATTACHMENTS_METHOD {
            return match forwarded_attachments(request.params.get("args")) {
                Ok(batch) => {
                    self.services
                        .turn_attachments()
                        .register_all(&record.agent_id, batch);
                    LinkOutcome::Ok(serde_json::json!({}))
                }
                Err(message) => refusal(INVALID_PARAMS, message),
            };
        }
        if method == ATTACHMENT_DATA_METHOD {
            let Some(id) = request
                .params
                .get("args")
                .and_then(|args| args.get("attachmentId"))
                .and_then(Value::as_str)
                .filter(|id| !id.trim().is_empty())
            else {
                return refusal(
                    INVALID_PARAMS,
                    format!("{ATTACHMENT_DATA_METHOD} needs a string `attachmentId`"),
                );
            };
            return match self
                .services
                .remote_attachment_data(&record.workspace_id, id, MAX_LINK_ATTACHMENT_BYTES)
                .await
            {
                Ok(value) => LinkOutcome::Ok(value),
                Err(Error::InvalidParams(message)) => refusal(INVALID_PARAMS, message),
                Err(error) => refusal(CALL_FAILED, error.to_string()),
            };
        }
        if method == MCP_PROXY_LIST_TOOLS_METHOD || method == MCP_PROXY_CALL_TOOL_METHOD {
            return self.mcp_proxy(record, method, request).await;
        }
        let features = self.services.session_agent_features(session);
        let is_sub_agent = session.parent_agent_id.is_some() || session.is_background;
        let budget = Duration::from_millis(u64::from(request.timeout_ms)).min(MAX_BUDGET);
        // Saved scripts (`remoteScripts: 1`): definitions and reads on the
        // backend; the agent's runs are its machine runs, never the home run.
        if method.starts_with("script.") {
            let caller = intent_services::feature_api::feature_ports::RemoteScriptCaller {
                workspace_id: record.workspace_id.clone(),
                agent_id: record.agent_id.clone(),
                node_id: record.node_id.clone(),
            };
            let args = request.params.get("args").cloned().unwrap_or(Value::Null);
            return match self
                .services
                .feature_runtime()
                .remote_script_call(&self.services, caller, method, args, budget)
                .await
            {
                Ok(value) => LinkOutcome::Ok(value),
                Err(message) => refusal(CALL_FAILED, message),
            };
        }
        let api: Arc<dyn WorkspaceApi> = Arc::new(self.services.clone());
        // The host also binds `Caller::Agent { record.agent_id }` around the
        // frame: a node's agent acts with exactly its own identity, never
        // the head daemon's.
        let host = intent_acp::make_workspace_host_for_bridge(
            api,
            record.workspace_id.clone(),
            record.agent_id.clone(),
            Some(self.services.turn_attachments()),
            features,
            is_sub_agent,
            budget,
        );
        let frame = serde_json::json!({
            "method": method,
            "args": request.params.get("args").cloned().unwrap_or(Value::Null),
        });
        match host(frame).await {
            Ok(value) => LinkOutcome::Ok(value),
            Err(message) => refusal(CALL_FAILED, message),
        }
    }
}

impl HeadCalls {
    /// One of the provider's proxied user MCP requests (nodeProtocol 11),
    /// run in head's hub as the bound agent, within the proxy's budget.
    async fn mcp_proxy(
        &self,
        record: &RemoteAgentRecord,
        method: &str,
        request: &LinkRequest,
    ) -> LinkOutcome {
        let args = request.params.get("args").cloned().unwrap_or(Value::Null);
        let Some(server_id) = args.get("serverId").and_then(Value::as_str) else {
            return refusal(
                INVALID_PARAMS,
                format!("{method} needs a string `serverId`"),
            );
        };
        let budget =
            Duration::from_millis(u64::from(request.timeout_ms)).min(MCP_PROXY_LINK_TIMEOUT);
        let call = async {
            if method == MCP_PROXY_LIST_TOOLS_METHOD {
                return self
                    .services
                    .remote_mcp_proxy_list_tools(&record.workspace_id, server_id)
                    .await
                    .map_err(|error| error.to_string())
                    .and_then(|result| fits_one_frame(result, server_id, "tools/list"));
            }
            let Some(name) = args.get("name").and_then(Value::as_str) else {
                return Err(format!("{method} needs a string `name`"));
            };
            let arguments = args.get("arguments").cloned().unwrap_or(Value::Null);
            let (result, server) = self
                .services
                .remote_mcp_proxy_call_tool(&record.workspace_id, server_id, name, arguments)
                .await
                .map_err(|error| error.to_string())?;
            fits_one_frame(result, &server, name)
        };
        match tokio::time::timeout(budget, call).await {
            Ok(Ok(result)) => LinkOutcome::Ok(result),
            Ok(Err(message)) => refusal(CALL_FAILED, message),
            Err(_) => refusal(
                CALL_FAILED,
                format!(
                    "mcp-proxy-timeout: {method} did not finish within {}s",
                    budget.as_secs()
                ),
            ),
        }
    }
}

/// `result` when its response frame fits the link; otherwise the
/// documented `mcp-proxy-result-too-large` error naming `server.tool`.
fn fits_one_frame(result: Value, server: &str, tool: &str) -> std::result::Result<Value, String> {
    let len = serde_json::to_vec(&result).map_or(usize::MAX, |bytes| bytes.len());
    if len.saturating_add(RESPONSE_OVERHEAD) > MAX_FRAME_BYTES {
        return Err(format!(
            "mcp-proxy-result-too-large: {server}.{tool} returned more than 8 MiB; the backend could not send it to this machine."
        ));
    }
    Ok(result)
}

/// The cards of one [`ATTACHMENTS_METHOD`] frame, for the tool result they
/// came from: claimed together by nonce or by the `workspace_api` fallback.
fn forwarded_attachments(
    args: Option<&Value>,
) -> std::result::Result<Vec<intent_core::TurnAttachment>, String> {
    let items = args
        .and_then(|args| args.get("attachments"))
        .and_then(Value::as_array)
        .ok_or_else(|| format!("{ATTACHMENTS_METHOD} needs an `attachments` array"))?;
    if items.len() > MAX_FORWARDED_ATTACHMENTS {
        return Err(format!(
            "{ATTACHMENTS_METHOD} carries at most {MAX_FORWARDED_ATTACHMENTS} attachments"
        ));
    }
    items
        .iter()
        .map(|item| {
            let field = |key: &str| {
                item.get(key)
                    .and_then(Value::as_str)
                    .map(str::to_owned)
                    .ok_or_else(|| {
                        format!("{ATTACHMENTS_METHOD}: each attachment needs a string `{key}`")
                    })
            };
            let id = field("id")?;
            if !intent_core::turn_attachments::is_attachment_id(&id) {
                return Err(format!(
                    "{ATTACHMENTS_METHOD}: `{id}` is not an attachment id"
                ));
            }
            Ok(intent_core::TurnAttachment {
                id,
                policy: intent_core::AttachmentPolicy::AtToolResult,
                mime_type: field("mimeType")?,
                uri: field("uri")?,
                name: field("name")?,
                text: field("text")?,
            })
        })
        .collect()
}

#[cfg(test)]
mod mcp_proxy_tests {
    use serde_json::json;

    use super::*;

    /// A result too large for one link frame fails with the documented
    /// message instead of a response the link would drop.
    #[test]
    fn a_result_past_the_frame_limit_is_refused_by_name() {
        let small = json!({ "content": [{ "type": "text", "text": "ok" }] });
        assert_eq!(fits_one_frame(small.clone(), "Linear", "search"), Ok(small));
        let large = json!({ "content": [{ "type": "text", "text": "x".repeat(MAX_FRAME_BYTES) }] });
        assert_eq!(
            fits_one_frame(large, "Linear", "search").unwrap_err(),
            "mcp-proxy-result-too-large: Linear.search returned more than 8 MiB; the backend could not send it to this machine."
        );
    }
}
