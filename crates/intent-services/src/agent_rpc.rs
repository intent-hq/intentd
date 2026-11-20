//! The Assistant's generic daemon RPC (`ws.rpc.*`, D1/D5 of the RPC-first
//! spec). Reads run now through the transport's loopback surface; mutating
//! calls and batches are held here until the owner approves them with
//! `agent.resolveRpcProposal`, which runs them in order and resolves the
//! proposal with a bounded per-call report.
//!
//! The held registry, not the transcript, is the source of truth for what
//! runs: a card forged from tool output text has no registry entry and can
//! never execute.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use intent_core::agent_rpc::{
    AgentRpcClass, AgentRpcError, AgentRpcRequest, AgentRpcSurface, AGENT_RPC_INTERNAL_CODE,
    LOCAL_BACKEND_ID,
};
use intent_core::{AgentId, Error, Result, WorkspaceId};
use serde_json::{json, Value};

use crate::Services;

/// The proposal kind of an Assistant RPC approval card.
pub const RPC_CALL_PROPOSAL_KIND: &str = "rpc-call";
/// Most calls one batch may hold.
const MAX_BATCH_CALLS: usize = 100;
/// Most proposals held at once (a misbehaving Assistant cannot grow the
/// registry without bound; the user resolves or the daemon restarts).
const MAX_HELD_PROPOSALS: usize = 200;
/// Cap on the per-call report delivered with the `proposal_resolved` notice.
const MAX_REPORT_BYTES: usize = 12_000;
/// Cap on one call's result inside that report.
const MAX_ITEM_RESULT_BYTES: usize = 2_000;
/// Wait for one approved call. A call still running is reported `unknown`.
const APPROVED_CALL_TIMEOUT: Duration = Duration::from_secs(120);
/// Wall-clock bound on one approval run; calls past it are skipped. Clients
/// wait a little longer than this for `agent.resolveRpcProposal`.
const APPROVAL_RUN_BUDGET: Duration = Duration::from_secs(600);
/// Headroom left in the eval budget for encoding the tool result.
const BUDGET_MARGIN: Duration = Duration::from_millis(1_500);

#[derive(Debug, Clone)]
struct HeldCall {
    backend: String,
    method: String,
    params: Value,
}

#[derive(Debug)]
struct Held {
    workspace_id: WorkspaceId,
    agent_id: AgentId,
    calls: Vec<HeldCall>,
    running: bool,
}

/// What an approval does once the registry was consulted.
enum Step {
    Run(Vec<HeldCall>),
    Reject,
    Missing,
}

/// Proposals held for the user's approval, keyed by proposal id. In memory
/// only: a card held before a daemon restart resolves as expired.
#[derive(Debug, Default)]
pub(crate) struct RpcProposalRegistry {
    held: Mutex<HashMap<String, Held>>,
}

impl RpcProposalRegistry {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Held>> {
        self.held
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

fn rpc_error_value(error: &AgentRpcError) -> Value {
    serde_json::to_value(error).unwrap_or(Value::Null)
}

fn bounded_result(result: &Value, cap: usize) -> Value {
    let text = result.to_string();
    if text.len() <= cap {
        return result.clone();
    }
    let mut end = cap;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    json!({ "truncated": true, "bytes": text.len(), "preview": &text[..end] })
}

fn audit(agent_id: &AgentId, backend: &str, method: &str, class: AgentRpcClass, outcome: &str) {
    tracing::info!(
        target: "intent_services::agent_rpc",
        agent = %agent_id,
        backend,
        method,
        class = class.as_str(),
        outcome,
        "assistant rpc"
    );
}

impl Services {
    /// Attach the transport's loopback RPC surface (composition root).
    /// Idempotent like [`Services::attach_server_control`].
    pub fn attach_agent_rpc(&self, surface: Arc<dyn AgentRpcSurface>) {
        let _ = self.agent_rpc.set(surface);
    }

    fn agent_rpc_surface(&self) -> Result<Arc<dyn AgentRpcSurface>> {
        self.agent_rpc
            .get()
            .cloned()
            .ok_or_else(|| Error::Unsupported("ws.rpc is not available on this daemon".into()))
    }

    fn require_assistant(workspace_id: &WorkspaceId) -> Result<()> {
        if workspace_id.is_chief() {
            Ok(())
        } else {
            Err(Error::Forbidden(
                "ws.rpc is only available in the Assistant workspace".into(),
            ))
        }
    }

    /// `ws.rpc.info()` and `ws.rpc.methods()`.
    pub(crate) fn agent_rpc_describe_op(
        &self,
        workspace_id: &WorkspaceId,
        what: &str,
        args: &Value,
    ) -> Result<Value> {
        Self::require_assistant(workspace_id)?;
        let surface = self.agent_rpc_surface()?;
        match what {
            "info" => {
                let version = surface.daemon_version();
                Ok(json!({
                    "backend": {
                        "id": LOCAL_BACKEND_ID,
                        "name": surface.local_backend_name(),
                        "local": true,
                    },
                    "daemonVersion": version,
                    "protocolVersion": surface.protocol_version(),
                    "source": format!("https://github.com/intent-hq/intentd/tree/v{version}"),
                    "protocolDocs": "https://github.com/intent-hq/intent/tree/main/docs/protocol",
                    "docsNote": "The protocol docs track the monorepo's intentd pin, not this exact release; the intentd source at v<daemonVersion> is the exact match.",
                    "cli": "intentd call <method> --params '<json>' is the equivalent CLI form used in docs. Do not run it; use ws.rpc.call.",
                }))
            }
            "methods" => {
                let prefix = args.get("prefix").and_then(Value::as_str).unwrap_or("");
                let class = args.get("class").and_then(Value::as_str);
                let rows: Vec<Value> = surface
                    .methods()
                    .into_iter()
                    .filter(|row| row.method.starts_with(prefix))
                    .filter(|row| class.is_none_or(|c| row.class.as_str() == c))
                    .map(|row| serde_json::to_value(row).unwrap_or(Value::Null))
                    .collect();
                Ok(json!({ "count": rows.len(), "methods": rows }))
            }
            other => Err(Error::InvalidParams(format!(
                "unknown ws.rpc view `{other}`"
            ))),
        }
    }

    /// `ws.rpc.call()` / `ws.rpc.batch()`.
    pub(crate) async fn agent_rpc_call_op(
        &self,
        workspace_id: WorkspaceId,
        agent_id: Option<AgentId>,
        request: AgentRpcRequest,
        budget: Duration,
    ) -> Result<Value> {
        Self::require_assistant(&workspace_id)?;
        let agent_id =
            agent_id.ok_or_else(|| Error::Forbidden("ws.rpc needs a calling agent".into()))?;
        let surface = self.agent_rpc_surface()?;
        if request.calls.is_empty() {
            return Err(Error::InvalidParams("at least one call is required".into()));
        }
        if request.calls.len() > MAX_BATCH_CALLS {
            return Err(Error::InvalidParams(format!(
                "a batch holds at most {MAX_BATCH_CALLS} calls"
            )));
        }
        let mut held = Vec::with_capacity(request.calls.len());
        for (index, call) in request.calls.iter().enumerate() {
            let method = call.method.trim();
            if method.is_empty() {
                return Err(Error::InvalidParams(format!(
                    "call {index}: method is required"
                )));
            }
            let backend = call
                .backend
                .clone()
                .filter(|b| !b.is_empty())
                .unwrap_or_else(|| LOCAL_BACKEND_ID.to_string());
            if backend != LOCAL_BACKEND_ID {
                return Ok(json!({
                    "ok": false,
                    "index": index,
                    "backend": backend,
                    "method": method,
                    "error": rpc_error_value(&AgentRpcError::coded(
                        -32602,
                        "unknown-backend",
                        format!("unknown backend `{backend}`: only the local backend (`{LOCAL_BACKEND_ID}`) is available"),
                    )),
                }));
            }
            let params = match &call.params {
                Value::Null => json!({}),
                Value::Object(_) => call.params.clone(),
                _ => {
                    return Err(Error::InvalidParams(format!(
                        "call {index}: params must be an object"
                    )))
                }
            };
            let class = surface.classify(method);
            if let Some(refusal) = refusal_for(surface.as_ref(), method, class) {
                audit(&agent_id, &backend, method, class, "refused");
                return Ok(json!({
                    "ok": false,
                    "index": index,
                    "backend": backend,
                    "method": method,
                    "class": class.as_str(),
                    "error": rpc_error_value(&refusal),
                }));
            }
            held.push((
                HeldCall {
                    backend,
                    method: method.to_string(),
                    params,
                },
                class,
            ));
        }
        if !request.batch && held[0].1 == AgentRpcClass::Read {
            let (call, class) = held.remove(0);
            return Ok(self
                .run_read(surface.as_ref(), &agent_id, call, class, budget)
                .await);
        }
        self.hold_for_approval(
            surface.as_ref(),
            workspace_id,
            &agent_id,
            request.title.as_deref(),
            &held,
        )
    }

    async fn run_read(
        &self,
        surface: &dyn AgentRpcSurface,
        agent_id: &AgentId,
        call: HeldCall,
        class: AgentRpcClass,
        budget: Duration,
    ) -> Value {
        let wait = budget.saturating_sub(BUDGET_MARGIN);
        let outcome = if wait.is_zero() {
            Err(timeout_error(&call.method, wait))
        } else {
            tokio::time::timeout(wait, surface.call(call.method.clone(), call.params))
                .await
                .unwrap_or_else(|_| Err(timeout_error(&call.method, wait)))
        };
        let mut out = json!({ "backend": call.backend, "method": call.method });
        match outcome {
            Ok(result) => {
                audit(agent_id, &call.backend, &call.method, class, "done");
                out["ok"] = json!(true);
                out["result"] = result;
            }
            Err(error) => {
                audit(agent_id, &call.backend, &call.method, class, "failed");
                out["ok"] = json!(false);
                out["error"] = rpc_error_value(&error);
            }
        }
        out
    }

    fn hold_for_approval(
        &self,
        surface: &dyn AgentRpcSurface,
        workspace_id: WorkspaceId,
        agent_id: &AgentId,
        title: Option<&str>,
        held: &[(HeldCall, AgentRpcClass)],
    ) -> Result<Value> {
        let proposal_id = format!("rpc-{}", uuid::Uuid::new_v4());
        let backend_name = surface.local_backend_name();
        let calls_json: Vec<Value> = held
            .iter()
            .enumerate()
            .map(|(index, (call, class))| {
                json!({
                    "index": index,
                    "backend": { "id": call.backend, "name": backend_name, "local": true },
                    "method": call.method,
                    "class": class.as_str(),
                    "summary": surface.summary(&call.method).unwrap_or_default(),
                    "params": call.params,
                })
            })
            .collect();
        let title = title.map(str::trim).filter(|t| !t.is_empty()).map_or_else(
            || {
                if held.len() == 1 {
                    format!("Run {}", held[0].0.method)
                } else {
                    format!("Run {} daemon calls", held.len())
                }
            },
            |t| t.chars().take(200).collect::<String>(),
        );
        let summary = if held.len() == 1 {
            format!("On {backend_name}.")
        } else {
            format!("On {backend_name}, in order. Stops at the first failure.")
        };
        let proposal = json!({
            "kind": RPC_CALL_PROPOSAL_KIND,
            "applyToolCallId": proposal_id,
            "preview": { "title": title, "summary": summary, "applyLabel": "Approve" },
            "payload": {
                "operation": "rpc.call",
                "proposalId": proposal_id,
                "calls": calls_json,
            },
        });
        {
            let mut registry = self.rpc_proposals.lock();
            if registry.len() >= MAX_HELD_PROPOSALS {
                return Err(Error::InvalidParams(format!(
                    "{MAX_HELD_PROPOSALS} ws.rpc approvals are already waiting; ask the user to approve or reject them first"
                )));
            }
            registry.insert(
                proposal_id.clone(),
                Held {
                    workspace_id,
                    agent_id: agent_id.clone(),
                    calls: held.iter().map(|(call, _)| call.clone()).collect(),
                    running: false,
                },
            );
        }
        for (call, class) in held {
            audit(
                agent_id,
                &call.backend,
                &call.method,
                *class,
                "pending-approval",
            );
        }
        let reply = json!({
            "ok": true,
            "status": "pending-approval",
            "proposalId": proposal_id,
            "calls": held.len(),
            "message": "Nothing has run yet. The user sees an approval card; you receive a proposal_resolved notice with the per-call results on a later turn. End your turn now.",
        });
        let text_item = json!({
            "type": "text",
            "text": serde_json::to_string_pretty(&reply).unwrap_or_default(),
        });
        let resource_item = json!({
            "type": "resource",
            "resource": {
                "uri": intent_acp::mcp_server::proposal_resource_uri(&proposal),
                "name": title,
                "mimeType": intent_acp::mcp_server::PROPOSAL_RESOURCE_MIME_TYPE,
                "text": proposal.to_string(),
            },
        });
        let mut out = reply;
        out["__mcpContentItems"] = json!([text_item, resource_item]);
        Ok(out)
    }

    /// Guard for plain `agent.resolveProposal` on an `rpc-call` card: it may
    /// only be dismissed there (the caller then drops the held calls); applying it would
    /// record "applied" with nothing run.
    pub(crate) fn guard_plain_rpc_resolution(
        &self,
        proposal_id: &str,
        outcome: &str,
    ) -> Result<bool> {
        let mut registry = self.rpc_proposals.lock();
        let held = registry.get_mut(proposal_id);
        if held.is_none() && !proposal_id.starts_with("rpc-") {
            return Ok(false);
        }
        if outcome == intent_core::PROPOSAL_OUTCOME_APPLIED {
            return Err(Error::InvalidParams(
                "rpc-call proposals are approved with agent.resolveRpcProposal".into(),
            ));
        }
        let Some(held) = held else {
            return Ok(false);
        };
        if held.running {
            return Err(Error::Conflict {
                current: json!({ "proposalId": proposal_id, "state": "running" }),
            });
        }
        // Claim the entry in the same critical section as the check, so a
        // concurrent approval sees `running` until the dismissal settles.
        held.running = true;
        Ok(true)
    }

    /// Settle a dismissal claimed by [`Self::guard_plain_rpc_resolution`]:
    /// drop the held calls once it is recorded, or release the claim when it
    /// failed so the card stays approvable.
    pub(crate) fn settle_dismissed_rpc_proposal(&self, proposal_id: &str, recorded: bool) {
        let mut registry = self.rpc_proposals.lock();
        if recorded {
            registry.remove(proposal_id);
        } else if let Some(held) = registry.get_mut(proposal_id) {
            held.running = false;
        }
    }

    /// `agent.resolveRpcProposal` (owner only).
    pub(crate) async fn agent_resolve_rpc_proposal_op(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        proposal_id: String,
        decision: String,
    ) -> Result<Value> {
        let owner = intent_core::current_caller()
            .is_some_and(|c| c.is_administrator() && c.principal_id().is_some());
        if !owner {
            return Err(Error::Forbidden(
                "only the owner can approve or reject Assistant calls".into(),
            ));
        }
        let approve = match decision.as_str() {
            "approve" => true,
            "reject" => false,
            _ => {
                return Err(Error::InvalidParams(
                    "decision must be \"approve\" or \"reject\"".into(),
                ))
            }
        };
        let session = self.store.get_agent_session_summary(&agent_id).await?;
        if session.workspace_id != workspace_id {
            return Err(Error::NotFound(format!("agent session {agent_id}")));
        }
        let pending = session
            .pending_proposals()
            .iter()
            .any(|p| p.proposal_id == proposal_id);
        let step = {
            let mut registry = self.rpc_proposals.lock();
            match registry.get_mut(&proposal_id) {
                Some(held) if held.agent_id != agent_id || held.workspace_id != workspace_id => {
                    return Err(Error::NotFound(format!("proposal {proposal_id}")));
                }
                Some(held) if held.running => {
                    return Err(Error::Conflict {
                        current: json!({ "proposalId": proposal_id, "state": "running" }),
                    });
                }
                Some(_) if !pending => {
                    return Err(Error::Conflict {
                        current: json!({
                            "proposalId": proposal_id,
                            "state": "turn-in-progress",
                            "detail": "The Assistant's turn has not ended yet; try again in a moment.",
                        }),
                    });
                }
                Some(_) if !approve => {
                    registry.remove(&proposal_id);
                    Step::Reject
                }
                Some(held) => {
                    held.running = true;
                    Step::Run(held.calls.clone())
                }
                None => Step::Missing,
            }
        };
        let calls = match step {
            Step::Run(calls) => calls,
            Step::Missing => {
                return self
                    .resolve_missing_rpc_proposal(workspace_id, agent_id, proposal_id, pending)
                    .await;
            }
            Step::Reject => {
                let mut out = self
                    .agent_resolve_proposal_op(
                        workspace_id,
                        agent_id,
                        proposal_id,
                        intent_core::PROPOSAL_OUTCOME_DISMISSED.to_string(),
                        None,
                        None,
                    )
                    .await?;
                out["items"] = json!([]);
                return Ok(out);
            }
        };
        let items = self.run_approved(&agent_id, calls).await;
        let report = approved_report(&items);
        let resolved = self
            .resolve_proposal_with_detail_cap(
                workspace_id,
                agent_id,
                proposal_id.clone(),
                intent_core::PROPOSAL_OUTCOME_APPLIED.to_string(),
                Some(report),
                None,
                MAX_REPORT_BYTES + 256,
            )
            .await;
        // Removed only now: a concurrent approve or reject keeps seeing
        // `running` until the resolution (and its report) is recorded.
        self.rpc_proposals.lock().remove(&proposal_id);
        let outcome = match resolved {
            Ok(v) => v["outcome"].clone(),
            Err(e) => {
                tracing::warn!(proposal = %proposal_id, error = %e, "rpc proposal ran but could not be resolved");
                json!(intent_core::PROPOSAL_OUTCOME_APPLIED)
            }
        };
        Ok(json!({
            "success": true,
            "proposalId": proposal_id,
            "outcome": outcome,
            "items": items,
        }))
    }

    async fn resolve_missing_rpc_proposal(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
        proposal_id: String,
        pending: bool,
    ) -> Result<Value> {
        if !pending {
            // Already resolved: idempotent echo; never tracked: NotFound.
            return self
                .agent_resolve_proposal_op(
                    workspace_id,
                    agent_id,
                    proposal_id,
                    intent_core::PROPOSAL_OUTCOME_DISMISSED.to_string(),
                    None,
                    None,
                )
                .await;
        }
        let mut out = self
            .agent_resolve_proposal_op(
                workspace_id,
                agent_id,
                proposal_id,
                intent_core::PROPOSAL_OUTCOME_DISMISSED.to_string(),
                None,
                None,
            )
            .await?;
        out["expired"] = json!(true);
        out["items"] = json!([]);
        Ok(out)
    }

    async fn run_approved(&self, agent_id: &AgentId, calls: Vec<HeldCall>) -> Vec<Value> {
        let surface = self.agent_rpc_surface().ok();
        let mut items = Vec::with_capacity(calls.len());
        let mut stopped = false;
        let deadline = tokio::time::Instant::now() + APPROVAL_RUN_BUDGET;
        for (index, call) in calls.into_iter().enumerate() {
            let mut item =
                json!({ "index": index, "backend": call.backend, "method": call.method });
            if stopped {
                item["status"] = json!("skipped");
                items.push(item);
                continue;
            }
            let Some(surface) = surface.as_ref() else {
                item["status"] = json!("failed");
                item["error"] = rpc_error_value(&AgentRpcError::coded(
                    AGENT_RPC_INTERNAL_CODE,
                    "agent-rpc-unavailable",
                    "ws.rpc is not available on this daemon",
                ));
                stopped = true;
                items.push(item);
                continue;
            };
            let class = surface.classify(&call.method);
            let wait = APPROVED_CALL_TIMEOUT
                .min(deadline.saturating_duration_since(tokio::time::Instant::now()));
            if wait.is_zero() {
                item["status"] = json!("skipped");
                stopped = true;
                items.push(item);
                continue;
            }
            let outcome = match refusal_for(surface.as_ref(), &call.method, class) {
                Some(refusal) => Err((false, refusal)),
                None => tokio::time::timeout(wait, surface.call(call.method.clone(), call.params))
                    .await
                    .map_err(|_| (true, timeout_error(&call.method, wait)))
                    .and_then(|r| r.map_err(|e| (false, e))),
            };
            match outcome {
                Ok(result) => {
                    item["status"] = json!("done");
                    item["result"] = result;
                }
                Err((timed_out, error)) => {
                    item["status"] = json!(if timed_out { "unknown" } else { "failed" });
                    item["error"] = rpc_error_value(&error);
                    stopped = true;
                }
            }
            audit(
                agent_id,
                &call.backend,
                &call.method,
                class,
                item["status"].as_str().unwrap_or("failed"),
            );
            items.push(item);
        }
        items
    }
}

fn refusal_for(
    surface: &dyn AgentRpcSurface,
    method: &str,
    class: AgentRpcClass,
) -> Option<AgentRpcError> {
    AgentRpcError::refusal(method, class, surface.summary(method))
}

fn timeout_error(method: &str, wait: Duration) -> AgentRpcError {
    AgentRpcError::coded(
        AGENT_RPC_INTERNAL_CODE,
        "agent-rpc-timeout",
        format!(
            "`{method}` did not answer within {:.1}s (the workspace_api call budget). It may still finish; check its effect before retrying.",
            wait.as_secs_f64()
        ),
    )
}

/// The `proposal_resolved` detail: every item's status, with results
/// shrunk until the report fits [`MAX_REPORT_BYTES`].
fn approved_report(items: &[Value]) -> String {
    let render = |cap: Option<usize>| -> String {
        let rows: Vec<Value> = items
            .iter()
            .map(|item| {
                let mut row = item.clone();
                if let Some(result) = item.get("result") {
                    match cap {
                        Some(0) => {
                            if let Some(obj) = row.as_object_mut() {
                                obj.remove("result");
                            }
                        }
                        Some(cap) => row["result"] = bounded_result(result, cap),
                        None => {}
                    }
                }
                row
            })
            .collect();
        format!(
            "ws.rpc ran the approved calls in order. Per-call report: {}",
            Value::Array(rows)
        )
    };
    for cap in [Some(MAX_ITEM_RESULT_BYTES), Some(400), Some(0)] {
        let text = render(cap);
        if text.len() <= MAX_REPORT_BYTES {
            return text;
        }
    }
    let mut text = render(Some(0));
    let mut end = MAX_REPORT_BYTES;
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
    text
}
