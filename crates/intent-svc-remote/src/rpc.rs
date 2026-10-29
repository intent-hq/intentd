//! Head's answer to the node's forwarded `workspace_api` calls.
//!
//! A remote agent evaluates its `workspace_api` code on its node and
//! forwards the control-plane frames (notes, tasks, agents, PRs, comments,
//! events) to head as [`CALL_METHOD`] link requests. Head serves one only on
//! the node's current binding, only for an agent recorded on that binding's
//! lease, and only for a frame its own routing table forwards (the node is
//! not trusted to route). The frame then runs through the same host
//! dispatch a local agent's bridge uses, as that agent, in its workspace.

use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;

use intent_core::node_link::{LeaseBinding, LinkOutcome, LinkRequest, LinkRpcError};
use intent_core::remote_launch::workspace_api::{self, Route, CALL_METHOD};
use intent_core::{Caller, Error, WorkspaceApi};
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
        if workspace_api::route(method) != Route::Forward {
            return refusal(FORBIDDEN, workspace_api::unsupported_message(method));
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
            .get_agent_session(&record.agent_id)
            .await
        {
            Ok(session) => session,
            Err(Error::NotFound(_)) => {
                return refusal(FORBIDDEN, "node-agent-denied: the agent no longer exists")
            }
            Err(error) => return refusal(CALL_FAILED, error.to_string()),
        };
        let features = self.services.session_agent_features(&session);
        let is_sub_agent = session.parent_agent_id.is_some() || session.is_background;
        let budget = Duration::from_millis(u64::from(request.timeout_ms)).min(MAX_BUDGET);
        let api: Arc<dyn WorkspaceApi> = Arc::new(self.services.clone());
        let host = intent_acp::make_workspace_host_for_bridge(
            api,
            record.workspace_id.clone(),
            Some(record.agent_id.clone()),
            Some(self.services.turn_attachments()),
            features,
            is_sub_agent,
            budget,
        );
        let frame = serde_json::json!({
            "method": method,
            "args": request.params.get("args").cloned().unwrap_or(Value::Null),
        });
        let caller = Caller::Agent {
            agent_id: record.agent_id,
        };
        match intent_core::with_caller(caller, host(frame)).await {
            Ok(value) => LinkOutcome::Ok(value),
            Err(message) => refusal(CALL_FAILED, message),
        }
    }
}
