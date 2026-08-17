//! `ws.agent.*` bindings (WSAPI-4).
//!
//! Each entry point is a thin JS wrapper around `host({ method, args })`;
//! the Rust dispatch here routes to the shared [`WorkspaceApi`]. Caller
//! attribution (parent auto-subscribe on `create`, SUB-1 sender watch on
//! `send`/`sendToTask`, depth guard on `create`/`wakeOrCreate`, and the
//! `-32603` gate on `reportToParent`/`requestDiscussion`/`reportBlocker`) is
//! threaded through the `caller_agent_id` argument that WSAPI-2 already
//! carries on the MCP seam.

use std::borrow::Cow;
use std::sync::Arc;

use intent_core::settings_file::AgentFeaturesSettings;
use intent_core::{
    model::{AgentDelegateInput, BatchTaskEntry},
    AgentCreateExtra, AgentId, AgentWakeOrCreateInput, MessageOrigin, NoteId, WorkspaceApi,
    WorkspaceId, MAX_DELEGATION_DEPTH,
};
use serde_json::{json, Value};

use super::{map_err, opt_bool, opt_str, opt_vec_str, req_str};

/// SUB-1 blurb surfaced by `send` / `sendToTask` when the sender is auto-
/// subscribed to the target's completion (parity with the TS `SendMessageTool`
/// and the identical constant in `dispatch.rs`).
const SENDER_WATCH_NOTIFICATION: &str = "You will be notified when the agent responds.";

pub(crate) const PRELUDE: &str = r#"
    globalThis.ws = globalThis.ws || {};
    ws.agent = {
        create: (name, message, opts) =>
            host({ method: 'agent.create', args: { name, message, ...(opts || {}) } }),
        delegate: (opts) => host({ method: 'agent.delegate', args: { ...(opts || {}) } }),
        send: (agentId, message, priority, messageMetadata) =>
            host({ method: 'agent.send', args: { agentId, message, priority, messageMetadata } }),
        sendToTask: (taskNoteId, message, priority, messageMetadata) =>
            host({ method: 'agent.sendToTask', args: { taskNoteId, message, priority, messageMetadata } }),
        subscribe: (eventTypes, opts) =>
            host({ method: 'agent.subscribe', args: { eventTypes, ...(opts || {}) } }),
        unsubscribe: (subscriptionId) =>
            host({ method: 'agent.unsubscribe', args: { subscriptionId } }),
        watch: (agentId) => host({ method: 'agent.watch', args: { agentId } }),
        unwatch: (subscriptionIdOrAgentId) => {
            const s = subscriptionIdOrAgentId == null ? '' : String(subscriptionIdOrAgentId);
            const args = s === '' ? {} : s.startsWith('agent-') ? { agentId: s } : { subscriptionId: s };
            return host({ method: 'agent.unwatch', args });
        },
        list: (includeCompleted) =>
            host({ method: 'agent.list', args: { includeCompleted } }),
        status: (agentId) => host({ method: 'agent.status', args: { agentId } }),
        getQueue: (agentId) => host({ method: 'agent.getQueue', args: { agentId } }),
        removeQueuedMessage: (agentId, messageId) =>
            host({ method: 'agent.removeQueuedMessage', args: { agentId, messageId } }),
        diagnostics: (opts) =>
            host({ method: 'agent.diagnostics', args: { ...(opts || {}) } }),
        snapshot: () => host({ method: 'agent.snapshot', args: {} }),
        wakeOrCreate: (taskNoteId, contextMessage, model, messageMetadata, reasoningEffort) =>
            host({ method: 'agent.wakeOrCreate', args: { taskNoteId, contextMessage, model, messageMetadata, reasoningEffort } }),
        readConversation: (agentId, opts) =>
            host({ method: 'agent.readConversation', args: { agentId, ...(opts || {}) } }),
        summary: (agentId) => host({ method: 'agent.summary', args: { agentId } }),
        reportToParent: (report) =>
            host({ method: 'agent.reportToParent', args: { report } }),
        requestDiscussion: (reason) =>
            host({ method: 'agent.requestDiscussion', args: { reason } }),
        reportBlocker: (reason) =>
            host({ method: 'agent.reportBlocker', args: { reason } }),
    };
"#;

/// The `ws.agent.requestDiscussion` / `ws.agent.reportBlocker` installer
/// lines inside [`PRELUDE`], removed when `agentFeatures.attentionRequests`
/// is off (a unit test guards that this segment still matches the prelude
/// verbatim).
pub(crate) const ATTENTION_PRELUDE_SEGMENT: &str = "        requestDiscussion: (reason) =>\n            host({ method: 'agent.requestDiscussion', args: { reason } }),\n        reportBlocker: (reason) =>\n            host({ method: 'agent.reportBlocker', args: { reason } }),\n";

/// Feature-aware `ws.agent` prelude: with `agentFeatures.attentionRequests`
/// off the two attention-request installers are omitted, so agent code
/// touching them fails with a clear `not a function` TypeError. Every other
/// `ws.agent.*` method (including `reportToParent`) stays un-gated. With the
/// toggle on — the default — this borrows [`PRELUDE`] byte-identically.
pub(crate) fn prelude_for(features: &AgentFeaturesSettings) -> Cow<'static, str> {
    if features.attention_requests {
        Cow::Borrowed(PRELUDE)
    } else {
        Cow::Owned(PRELUDE.replacen(ATTENTION_PRELUDE_SEGMENT, "", 1))
    }
}

pub(crate) async fn dispatch(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller: Option<&AgentId>,
    method: &str,
    args: &Value,
) -> Result<Value, String> {
    match method {
        "create" => create(api, ws, caller, args).await,
        "delegate" => delegate(api, ws, caller, args).await,
        "send" => send(api, ws, caller, args).await,
        "sendToTask" => send_to_task(api, ws, caller, args).await,
        "subscribe" => subscribe(api, ws, caller, args).await,
        "unsubscribe" => unsubscribe(api, ws, args).await,
        "watch" => watch(api, ws, caller, args).await,
        "unwatch" => unwatch(api, ws, caller, args).await,
        "list" => list(api, ws).await,
        "status" => status(api, ws, args).await,
        "getQueue" => get_queue(api, ws, args).await,
        "removeQueuedMessage" => remove_queued_message(api, ws, caller, args).await,
        "diagnostics" => diagnostics(api, ws, args).await,
        "snapshot" => snapshot(api, ws, caller).await,
        "wakeOrCreate" => wake_or_create(api, ws, caller, args).await,
        "readConversation" => read_conversation(api, ws, args).await,
        "summary" => summary(api, ws, args).await,
        "reportToParent" => report_to_parent(api, ws, caller, args).await,
        "requestDiscussion" => request_attention(api, ws, caller, "discussion", args).await,
        "reportBlocker" => request_attention(api, ws, caller, "blocker", args).await,
        other => Err(format!("host: unknown method `agent.{other}`")),
    }
}

async fn create(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    let name = req_str(args, "name").map_err(|_| "name is required".to_string())?;
    let initial_message =
        req_str(args, "message").map_err(|_| "message is required".to_string())?;
    let mut caller_depth: i64 = 0;
    let mut caller_name: Option<String> = None;
    if let Some(c) = caller {
        if let Ok(caller_lite) = api.agent_get(c.clone(), Some(ws.clone())).await {
            caller_depth = caller_lite.metadata.delegation_depth.unwrap_or(0);
            caller_name = Some(caller_lite.name);
        }
        if caller_depth >= MAX_DELEGATION_DEPTH {
            return Err(format!(
                "Cannot create sub-agent: maximum delegation depth ({MAX_DELEGATION_DEPTH}) reached. You are at depth {caller_depth}. Please complete this task directly instead of delegating further."
            ));
        }
    }
    let task_note_id = opt_str(args, "taskNoteId");
    let is_background = opt_bool(args, "isBackground").unwrap_or(true);
    let mut metadata = serde_json::Map::new();
    metadata.insert(
        "initialMessage".to_string(),
        Value::String(initial_message.clone()),
    );
    metadata.insert("isBackground".to_string(), Value::Bool(is_background));
    if let Some(c) = caller {
        metadata.insert(
            "createdByAgentId".to_string(),
            Value::String(c.as_str().to_string()),
        );
        metadata.insert("delegationDepth".to_string(), Value::from(caller_depth + 1));
    }
    if let Some(bp) = opt_str(args, "behaviorPrompt") {
        metadata.insert("behaviorPrompt".to_string(), Value::String(bp));
    }
    if let Some(tn) = &task_note_id {
        metadata.insert("taskNoteId".to_string(), Value::String(tn.clone()));
    }
    let extra = AgentCreateExtra {
        metadata: Some(Value::Object(metadata)),
        is_background: Some(is_background),
        reasoning_effort: opt_str(args, "reasoningEffort"),
        ..AgentCreateExtra::default()
    };
    let created = api
        .agent_create(
            ws.clone(),
            Some(name),
            opt_str(args, "model"),
            opt_str(args, "specialist"),
            caller.cloned(),
            opt_str(args, "idempotencyKey").or_else(|| Some(uuid::Uuid::new_v4().to_string())),
            extra,
        )
        .await
        .map_err(map_err)?;
    let agent_id = created["agent"]["id"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    let agent_name = created["agent"]["name"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    if let Some(tn) = task_note_id {
        // `ws.agent.create` with a taskNoteId is an explicit "create and
        // assign in one step" — keep the assignment best-effort and
        // unguarded (`force`) as before; the occupancy guard applies to
        // `agent.delegate` / `task.assignAgent`.
        let _ = api
            .assign_agent(
                ws.clone(),
                NoteId::from_string(tn),
                agent_id.clone(),
                Some(true),
            )
            .await;
    }
    let child = AgentId::from(agent_id.as_str());
    // Auto-subscribe caller to child completion (AS-5) — parity with the
    // WSAPI-2 `create_agent` tool path in `dispatch.rs`. Failure is
    // non-fatal (the child still runs) but is logged so SUB/AS drops are
    // diagnosable in production.
    let mut subscription_id: Option<String> = None;
    if let Some(c) = caller {
        match api
            .agent_watch_completion(ws.clone(), c.clone(), child.clone())
            .await
        {
            Ok(v) => {
                subscription_id = v
                    .get("subscriptionId")
                    .and_then(Value::as_str)
                    .map(String::from);
            }
            Err(e) => {
                tracing::warn!(agent = %agent_id, error = %e, "agent.create: failed to register completion watch");
            }
        }
    }
    // Deliver the initial message so the child actually starts its first
    // turn — parity with the WSAPI-2 path. Failure is non-fatal (the
    // session already exists) but is logged. Sender attribution reuses the
    // depth-guard lookup's name (no second `agent_get` round-trip); for
    // agent callers the attribution fields are daemon-stamped into any
    // explicit `messageMetadata` (`merge_sender_attribution`).
    let kickoff_metadata =
        merge_sender_attribution(explicit_metadata(args), caller, caller_name.as_deref());
    if let Err(e) = api
        .agent_send_message(
            ws.clone(),
            child,
            initial_message,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            kickoff_metadata,
            MessageOrigin::Automatic,
        )
        .await
    {
        tracing::warn!(agent = %agent_id, error = %e, "agent.create: failed to start child turn");
    }
    Ok(json!({
        "ok": true,
        "id": agent_id,
        "agentId": agent_id,
        "name": agent_name,
        "subscriptionId": subscription_id,
    }))
}

async fn delegate(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    let tasks = match args.get("tasks").and_then(Value::as_array) {
        Some(a) => {
            let mut entries: Vec<BatchTaskEntry> = Vec::with_capacity(a.len());
            for v in a {
                match serde_json::from_value::<BatchTaskEntry>(v.clone()) {
                    Ok(entry) => entries.push(entry),
                    Err(_) => {
                        return Err(format!(
                            "tasks entries must be task note id strings or {{ taskNoteId, specialist?, model?, reasoningEffort? }} objects, got: {v}"
                        ));
                    }
                }
            }
            Some(entries)
        }
        None => None,
    };
    let input = AgentDelegateInput {
        task_note_id: opt_str(args, "taskNoteId").map(NoteId::from_string),
        note_id: opt_str(args, "noteId").map(NoteId::from_string),
        task_text: opt_str(args, "taskText"),
        agent_instructions: opt_str(args, "agentInstructions"),
        specialist: opt_str(args, "specialist"),
        model: opt_str(args, "model"),
        reasoning_effort: opt_str(args, "reasoningEffort"),
        behavior_prompt: opt_str(args, "behaviorPrompt"),
        wait_mode: opt_str(args, "waitMode"),
        skip_auto_commit: opt_bool(args, "skipAutoCommit"),
        isolation: opt_str(args, "isolation"),
        force: opt_bool(args, "force"),
        tasks,
        // Presence-sensitive: `greedy` is REMOVED and any supplied value
        // (even `null`) must reach the service layer for its rejection.
        greedy: args.get("greedy").map(Value::as_bool),
    };
    let v = api
        .agent_delegate(ws.clone(), input, caller.cloned())
        .await
        .map_err(map_err)?;
    Ok(merge_ok(v))
}

/// Resolve the effective delivery priority for `ws.agent.send` /
/// `ws.agent.sendToTask`. These MCP bindings deliver with INTERRUPT priority
/// by default: an omitted (or `null`) `priority` resolves to `"interrupt"`,
/// and the explicit `priority: "queue"` opt-out restores queue-if-busy
/// delivery by mapping to `"normal"` (any value other than `"interrupt"` is
/// non-interrupt at the service layer). Every other explicit value passes
/// through unchanged. Binding-local by design: the wire-level
/// `agent.sendMessage` / `agent.sendToTask` RPC defaults (FE front door,
/// automated deliveries) are untouched.
fn effective_priority(args: &Value) -> Option<String> {
    match opt_str(args, "priority") {
        None => Some("interrupt".to_string()),
        Some(p) if p == "queue" => Some("normal".to_string()),
        other => other,
    }
}

/// `ws.agent.send`. Guarded by the single-pending-message rule: when the
/// caller (an agent) already has a pending entry in the target's queue, the
/// send is refused with an `ok: false` result echoing the target's queue —
/// see [`pending_send_refusal`].
async fn send(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    let agent_id_str = req_str(args, "agentId").map_err(|_| "agentId is required".to_string())?;
    let message = req_str(args, "message").map_err(|_| "message is required".to_string())?;
    let agent_id = AgentId::from(agent_id_str.as_str());
    if let Some(refusal) = pending_send_refusal(api, ws, caller, &agent_id).await {
        return Ok(refusal);
    }
    let mut result = api
        .agent_send_message(
            ws.clone(),
            agent_id.clone(),
            message,
            None,
            None,
            None,
            effective_priority(args),
            None,
            None,
            None,
            sender_metadata(api, ws, caller, args).await,
            MessageOrigin::Automatic,
        )
        .await
        .map_err(map_err)?;
    if let Some(sub) = watch_sender(api, ws, caller, &agent_id).await {
        result["subscriptionId"] = json!(sub);
        result["message"] = json!(SENDER_WATCH_NOTIFICATION);
    }
    let mut out = merge_ok(result);
    if let Some(obj) = out.as_object_mut() {
        obj.insert("agentId".to_string(), json!(agent_id_str));
    }
    Ok(out)
}

/// `ws.agent.sendToTask`. Same single-pending-message guard as [`send`],
/// applied against the task's assigned agent (resolution failures fall
/// through so the unguarded call surfaces its existing error/`ok: false`
/// shapes — e.g. "No agent assigned to task"). The guard's target
/// resolution (`task.assigned_agents.first()`) deliberately mirrors
/// `agent_send_to_task_op` in `intent-services` — if the op's resolution
/// ever changes, this site must change with it or the guard checks the
/// wrong agent.
async fn send_to_task(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    let task_note_id =
        req_str(args, "taskNoteId").map_err(|_| "taskNoteId is required".to_string())?;
    let message = req_str(args, "message").map_err(|_| "message is required".to_string())?;
    if caller.is_some() {
        if let Ok(task) = api
            .get_my_task(ws.clone(), NoteId::from_string(&task_note_id))
            .await
        {
            if let Some(target) = task.assigned_agents.first() {
                if let Some(mut refusal) = pending_send_refusal(api, ws, caller, target).await {
                    if let Some(obj) = refusal.as_object_mut() {
                        obj.insert("taskNoteId".to_string(), json!(task_note_id));
                    }
                    return Ok(refusal);
                }
            }
        }
    }
    let mut result = api
        .agent_send_to_task(
            ws.clone(),
            NoteId::from_string(&task_note_id),
            message,
            effective_priority(args),
            sender_metadata(api, ws, caller, args).await,
        )
        .await
        .map_err(map_err)?;
    let target = result
        .get("agentId")
        .and_then(Value::as_str)
        .map(AgentId::from);
    if let Some(target) = target {
        if let Some(sub) = watch_sender(api, ws, caller, &target).await {
            result["subscriptionId"] = json!(sub);
            result["message"] = json!(SENDER_WATCH_NOTIFICATION);
        }
    }
    let mut out = merge_ok(result);
    if let Some(obj) = out.as_object_mut() {
        obj.insert("taskNoteId".to_string(), json!(task_note_id));
    }
    Ok(out)
}

async fn subscribe(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    let event_types = opt_vec_str(args, "eventTypes").ok_or_else(|| {
        "eventTypes is required. Specify category wildcards like \"file:*\", \"task:*\" or specific types like \"file:changed\". Agent events are not subscribable — use ws.agent.watch(agentId) instead.".to_string()
    })?;
    if event_types.is_empty() {
        return Err(
            "eventTypes is required. Specify category wildcards like \"file:*\", \"task:*\" or specific types like \"file:changed\". Agent events are not subscribable — use ws.agent.watch(agentId) instead.".to_string(),
        );
    }
    let v = api
        .agent_subscribe(
            ws.clone(),
            caller.cloned(),
            event_types,
            opt_bool(args, "excludeSelf"),
            args.get("batchWindow").and_then(Value::as_i64),
        )
        .await
        .map_err(map_err)?;
    Ok(merge_ok(v))
}

async fn unsubscribe(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let subscription_id =
        req_str(args, "subscriptionId").map_err(|_| "subscriptionId is required".to_string())?;
    let _ = api
        .agent_unsubscribe(ws.clone(), subscription_id.clone())
        .await
        .map_err(map_err)?;
    Ok(json!({ "ok": true, "subscriptionId": subscription_id }))
}

/// `ws.agent.watch(agentId)` (monorepo#1229): explicit deliver-once
/// subscription to another agent's completion (idle with an empty pending
/// message queue, failed, deleted) with attention fan-out (blocker raised,
/// discussion requested) that does not consume the watch. Caller-only (the
/// front door has no wake target).
async fn watch(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    let caller = caller.ok_or_else(|| "agent.watch is only available to agents".to_string())?;
    let agent_id_str = req_str(args, "agentId").map_err(|_| "agentId is required".to_string())?;
    let v = api
        .agent_watch(
            ws.clone(),
            caller.clone(),
            AgentId::from(agent_id_str.as_str()),
        )
        .await
        .map_err(map_err)?;
    Ok(merge_ok(v))
}

/// `ws.agent.unwatch(subscriptionIdOrAgentId)` (monorepo#1229): remove one of
/// the caller's own watches, addressed by subscription id or watched agent id.
async fn unwatch(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    let caller = caller.ok_or_else(|| "agent.unwatch is only available to agents".to_string())?;
    let subscription_id = opt_str(args, "subscriptionId").filter(|s| !s.is_empty());
    let target = opt_str(args, "agentId")
        .filter(|s| !s.is_empty())
        .map(|s| AgentId::from(s.as_str()));
    if subscription_id.is_none() && target.is_none() {
        return Err("subscriptionId or agentId is required".to_string());
    }
    let v = api
        .agent_unwatch(ws.clone(), caller.clone(), subscription_id, target)
        .await
        .map_err(map_err)?;
    Ok(merge_ok(v))
}

async fn list(api: &Arc<dyn WorkspaceApi>, ws: &WorkspaceId) -> Result<Value, String> {
    let rows = api.agent_list(ws.clone()).await.map_err(map_err)?;
    serde_json::to_value(rows).map_err(|e| e.to_string())
}

/// `ws.agent.status` merges the target's pending queue into the result
/// (`queue` + `queueLength`) — MCP-side only, the wire `AgentLite` shape used
/// by `agent.get`/`agent.list` is untouched. Queue entries use the
/// `getQueue` presentation (drain order, lifted attribution) with `content`
/// truncated to [`STATUS_QUEUE_PREVIEW_MAX_CHARS`] chars (`…` appended).
async fn status(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let agent_id_str = req_str(args, "agentId").map_err(|_| "agentId is required".to_string())?;
    let agent_id = AgentId::from(agent_id_str.as_str());
    let agent = api
        .agent_get(agent_id.clone(), Some(ws.clone()))
        .await
        .map_err(map_err)?;
    let mut out = serde_json::to_value(agent).map_err(|e| e.to_string())?;
    let queue = fetch_presented_queue(api, ws, &agent_id).await?;
    let queue: Vec<Value> = queue.into_iter().map(truncate_entry_content).collect();
    if let Some(obj) = out.as_object_mut() {
        obj.insert("queueLength".to_string(), json!(queue.len()));
        obj.insert("queue".to_string(), Value::Array(queue));
    }
    Ok(out)
}

/// `ws.agent.getQueue`: the target's full pending queue — every entry
/// regardless of sender — in actual drain order (next delivery first).
async fn get_queue(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let agent_id_str = req_str(args, "agentId").map_err(|_| "agentId is required".to_string())?;
    let agent_id = AgentId::from(agent_id_str.as_str());
    let queue = fetch_presented_queue(api, ws, &agent_id).await?;
    Ok(json!({
        "ok": true,
        "agentId": agent_id_str,
        "queueLength": queue.len(),
        "queue": queue,
    }))
}

/// `ws.agent.removeQueuedMessage`: retract the caller's OWN pending message
/// from the target's queue. Ownership-guarded in the service op — an entry
/// whose `messageMetadata.fromAgentId` is not the caller (another agent's
/// send, or a user/FE entry with no attribution) is rejected.
async fn remove_queued_message(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    let agent_id_str = req_str(args, "agentId").map_err(|_| "agentId is required".to_string())?;
    let message_id = req_str(args, "messageId").map_err(|_| "messageId is required".to_string())?;
    let caller = caller
        .ok_or_else(|| "removeQueuedMessage requires an agent caller identity".to_string())?;
    let agent_id = AgentId::from(agent_id_str.as_str());
    // Workspace scoping: same defense-in-depth as getQueue — the target must
    // resolve inside the caller's workspace before any queue mutation.
    let _ = api
        .agent_get(agent_id.clone(), Some(ws.clone()))
        .await
        .map_err(map_err)?;
    let v = api
        .agent_remove_queued_message_owned(agent_id, message_id.clone(), caller.clone())
        .await
        .map_err(map_err)?;
    let mut out = merge_ok(v);
    if let Some(obj) = out.as_object_mut() {
        obj.insert("agentId".to_string(), json!(agent_id_str));
    }
    Ok(out)
}

async fn diagnostics(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let agent_id = opt_str(args, "agentId").map(|s| AgentId::from(s.as_str()));
    let task_note_id = opt_str(args, "taskNoteId").map(NoteId::from_string);
    let stale = args.get("staleRespondingAfterMs").and_then(Value::as_i64);
    let v = api
        .agent_diagnostics(ws.clone(), agent_id, task_note_id, stale)
        .await
        .map_err(map_err)?;
    Ok(merge_ok(v))
}

/// `ws.agent.snapshot()` — the CALLER's own state digest (no target
/// argument; always self-scoped). Deliberately never gated by
/// `agentFeatures.stateSnapshot`: the toggle governs only the per-turn
/// prompt injection, the tool stays callable.
async fn snapshot(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller: Option<&AgentId>,
) -> Result<Value, String> {
    let caller = caller.ok_or_else(|| "snapshot requires an agent caller identity".to_string())?;
    api.agent_snapshot(ws.clone(), caller.clone())
        .await
        .map_err(map_err)
}

async fn wake_or_create(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    let task_note_id =
        req_str(args, "taskNoteId").map_err(|_| "taskNoteId is required".to_string())?;
    let context_message =
        req_str(args, "contextMessage").map_err(|_| "contextMessage is required".to_string())?;
    let mut caller_name: Option<String> = None;
    if let Some(c) = caller {
        if let Ok(caller_lite) = api.agent_get(c.clone(), Some(ws.clone())).await {
            let depth = caller_lite.metadata.delegation_depth.unwrap_or(0);
            if depth >= MAX_DELEGATION_DEPTH {
                return Err(format!(
                    "Cannot delegate task: maximum delegation depth ({MAX_DELEGATION_DEPTH}) reached. You are at depth {depth}. Please complete this task directly instead of delegating further."
                ));
            }
            caller_name = Some(caller_lite.name);
        }
    }
    // Sender attribution on the delivered context message (monorepo#1015):
    // same daemon-stamped attribution semantics as `send`, reusing the
    // depth-guard lookup's name (no second `agent_get` round-trip).
    let message_metadata =
        merge_sender_attribution(explicit_metadata(args), caller, caller_name.as_deref());
    let v = api
        .agent_wake_or_create(
            ws.clone(),
            NoteId::from_string(&task_note_id),
            context_message,
            AgentWakeOrCreateInput {
                model: opt_str(args, "model"),
                reasoning_effort: opt_str(args, "reasoningEffort"),
                caller_agent_id: caller.cloned(),
                message_metadata,
                ..Default::default()
            },
        )
        .await
        .map_err(map_err)?;
    let mut out = merge_ok(v);
    if let Some(obj) = out.as_object_mut() {
        obj.insert("taskNoteId".to_string(), json!(task_note_id));
    }
    Ok(out)
}

async fn read_conversation(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let agent_id_str = req_str(args, "agentId").map_err(|_| "agentId is required".to_string())?;
    let last_n = args.get("lastN").and_then(Value::as_i64);
    let page_token = opt_str(args, "pageToken");
    let v = api
        .agent_get_conversation(
            AgentId::from(agent_id_str.as_str()),
            last_n,
            Some(ws.clone()),
            page_token,
            None,
            None,
        )
        .await
        .map_err(map_err)?;
    Ok(v)
}

async fn summary(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let agent_id_str = req_str(args, "agentId").map_err(|_| "agentId is required".to_string())?;
    api.agent_summary(ws.clone(), AgentId::from(agent_id_str.as_str()))
        .await
        .map_err(map_err)
}

async fn report_to_parent(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    let report = args
        .get("report")
        .cloned()
        .ok_or_else(|| "report is required".to_string())?;
    let v = api
        .agent_report_to_parent(ws.clone(), report, caller.cloned())
        .await
        .map_err(map_err)?;
    Ok(merge_ok(v))
}

/// Shared handler behind `ws.agent.requestDiscussion` (`kind = "discussion"`)
/// and `ws.agent.reportBlocker` (`kind = "blocker"`). Available to ALL agents
/// (delegated or not, with or without a linked task); `reason` is required.
async fn request_attention(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller: Option<&AgentId>,
    kind: &str,
    args: &Value,
) -> Result<Value, String> {
    let reason = req_str(args, "reason").map_err(|_| "reason is required".to_string())?;
    let v = api
        .agent_request_attention(ws.clone(), kind.to_string(), reason, caller.cloned())
        .await
        .map_err(map_err)?;
    Ok(merge_ok(v))
}

/// Build the `{ type: "agent_message", fromAgentId, fromAgentName }`
/// sender-attribution payload (PROTOCOL §5.5). `fromAgentName` is always
/// present for a stable schema; it is `null` when the caller lookup failed.
fn agent_message_metadata(caller: &AgentId, name: Option<&str>) -> Value {
    json!({
        "type": "agent_message",
        "fromAgentId": caller.as_str(),
        "fromAgentName": name,
    })
}

/// An explicit non-null `messageMetadata` arg, if the caller supplied one.
/// Explicit metadata wins over auto-tagging for its own fields — but for
/// agent callers the attribution fields are daemon-stamped and never
/// caller-controlled (see [`merge_sender_attribution`]); `null` is treated
/// as absent (it does not suppress the auto-tag).
fn explicit_metadata(args: &Value) -> Option<Value> {
    args.get("messageMetadata")
        .filter(|v| !v.is_null())
        .cloned()
}

/// Combine explicit caller-supplied metadata with the daemon-derived sender
/// attribution. Attribution (`fromAgentId`/`fromAgentName`) is
/// SECURITY-RELEVANT — the single-pending-message guard and
/// `ws.agent.removeQueuedMessage` ownership both key on it — so for agent
/// callers it is ALWAYS daemon-stamped, overwriting any caller-supplied
/// values: omitting it must not evade the guard, and spoofing it must not
/// misattribute the entry or hand removal rights to another agent. All other
/// explicit fields are preserved; a non-object explicit value cannot carry
/// attribution and is replaced by the pure auto-tag. Caller-less (FE/RPC
/// front door) invocations keep explicit metadata verbatim, or none at all.
fn merge_sender_attribution(
    explicit: Option<Value>,
    caller: Option<&AgentId>,
    name: Option<&str>,
) -> Option<Value> {
    let Some(caller) = caller else {
        return explicit;
    };
    match explicit {
        Some(Value::Object(mut obj)) => {
            obj.insert("fromAgentId".to_string(), json!(caller.as_str()));
            obj.insert("fromAgentName".to_string(), json!(name));
            Some(Value::Object(obj))
        }
        _ => Some(agent_message_metadata(caller, name)),
    }
}

/// `messageMetadata` for agent-originated sends: when the caller is an agent
/// (not the FE/RPC front door), the delivered message carries the
/// [`agent_message_metadata`] attribution — merged into any explicit
/// caller-supplied `messageMetadata` with the attribution fields
/// daemon-stamped ([`merge_sender_attribution`]) so clients can trust who
/// sent it. `fromAgentName` is resolved from the caller's session;
/// human-originated sends (`caller == None`) keep explicit metadata verbatim
/// and stay untagged when none was supplied.
async fn sender_metadata(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller: Option<&AgentId>,
    args: &Value,
) -> Option<Value> {
    let explicit = explicit_metadata(args);
    let Some(caller) = caller else {
        return explicit;
    };
    let name = api
        .agent_get(caller.clone(), Some(ws.clone()))
        .await
        .ok()
        .map(|lite| lite.name);
    merge_sender_attribution(explicit, Some(caller), name.as_deref())
}

/// SUB-1 sender auto-subscribe: register the caller→target completion watch
/// and surface the subscription id (parity with the identical helper on
/// `WorkspaceMcpServer::watch_completion_for_sender`).
async fn watch_sender(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller: Option<&AgentId>,
    target: &AgentId,
) -> Option<String> {
    let caller = caller?;
    match api
        .agent_watch_completion_for_sender(ws.clone(), caller.clone(), target.clone())
        .await
    {
        Ok(v) => v
            .get("subscriptionId")
            .and_then(Value::as_str)
            .map(String::from),
        Err(e) => {
            tracing::warn!(target = %target.0, error = %e, "agent.send: failed to register sender completion watch");
            None
        }
    }
}

/// Max `content` chars per queue entry embedded in `ws.agent.status` (same
/// preview cap as the services-side `QUEUE_PREVIEW_MAX_CHARS` used by
/// `agent.diagnostics`).
const STATUS_QUEUE_PREVIEW_MAX_CHARS: usize = 200;

/// Fetch the target's queue via `agent.getQueue` (workspace-scoped) and
/// project it into the MCP presentation via [`present_queue`].
async fn fetch_presented_queue(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    agent_id: &AgentId,
) -> Result<Vec<Value>, String> {
    let v = api
        .agent_get_queue(agent_id.clone(), Some(ws.clone()))
        .await
        .map_err(map_err)?;
    let raw = v
        .get("queue")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    Ok(present_queue(raw))
}

/// Single-pending-message guard on `ws.agent.send` / `ws.agent.sendToTask`:
/// when the caller (an agent) already has a pending entry in the target's
/// queue, refuse the send instead of stacking a second one. Returns
/// `Some(refusal)` — a **successful** tool result with `ok: false`, the
/// target's presented queue (drain order, [`truncate_entry_content`]'d), the
/// caller's pending entry id, and the instruction to either keep the existing
/// entry or remove it and re-send one combined message (which lands at the
/// end of the queue). Returns `None` when the guard does not apply: no agent
/// caller identity (user/FE-origin sends are never guarded), the queue could
/// not be fetched (fall through so the unguarded send surfaces its existing
/// error shapes — e.g. the monorepo#564 unknown-id `-32602`), or the caller
/// has no pending (non-editing) entry in the queue.
///
/// The guard is advisory hygiene, not a hard invariant: it is a
/// check-then-send without atomicity, so two concurrent sends from the same
/// caller can both observe no pending entry and both park (TOCTOU). The
/// queue registry lock lives in `Services`; consumers must not assume the
/// single-pending property is enforced.
async fn pending_send_refusal(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller: Option<&AgentId>,
    target: &AgentId,
) -> Option<Value> {
    let caller = caller?;
    let queue = fetch_presented_queue(api, ws, target).await.ok()?;
    let pending_id = caller_pending_entry(&queue, caller)?.to_string();
    let queue_length = queue.len();
    let queue: Vec<Value> = queue.into_iter().map(truncate_entry_content).collect();
    Some(json!({
        "ok": false,
        "agentId": target.as_str(),
        "error": format!(
            "send refused: you already have a pending message (id: {pending_id}) in this agent's queue"
        ),
        "pendingMessageId": pending_id,
        "queueLength": queue_length,
        "queue": queue,
        "instruction": "Only one pending message per target is allowed. Either keep your existing entry as-is, or remove it with ws.agent.removeQueuedMessage(agentId, messageId) and re-send ONE message combining everything you want to say. Note: a re-sent message lands at the END of the queue.",
    }))
}

/// The id of the caller's first pending entry in a presented queue
/// (`fromAgentId` attribution equals the caller), if any. Entries flagged
/// `editing: true` are SKIPPED — the drain skips them, so they are not
/// pending deliveries and must not block a send that would deliver now.
fn caller_pending_entry<'a>(queue: &'a [Value], caller: &AgentId) -> Option<&'a str> {
    queue.iter().find_map(|e| {
        let editing = e.get("editing").and_then(Value::as_bool).unwrap_or(false);
        (!editing && e.get("fromAgentId").and_then(Value::as_str) == Some(caller.as_str()))
            .then(|| e.get("id").and_then(Value::as_str))
            .flatten()
    })
}

/// Project raw `agent.getQueue` entries into the `ws.agent.getQueue` /
/// `ws.agent.status` presentation:
///
/// - **Drain order** — next delivery first: interrupt-priority entries (in
///   arrival order among themselves) ahead of normal FIFO entries. The stored
///   queue order already IS the drain order (interrupt sends park
///   front-of-queue), so this stable sort is a defensive normalization.
///   Entries with `editing: true` (skipped by the drain) are NOT excluded:
///   they sort to the end, explicitly flagged `editing: true`.
/// - **Attribution lifted top-level** — `fromAgentId?` / `fromAgentName?` are
///   surfaced from `messageMetadata` when present (absent for user/FE-origin
///   entries), and the bulky `messageMetadata` / `imageBlocks` / `fileBlocks`
///   payloads are dropped.
/// - **`position` renumbered** to the presented order (0 = next delivery).
fn present_queue(raw: Vec<Value>) -> Vec<Value> {
    let mut entries = raw;
    entries.sort_by_key(|e| {
        let editing = e.get("editing").and_then(Value::as_bool).unwrap_or(false);
        let interrupt = e
            .get("interruptPriority")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        match (editing, interrupt) {
            (false, true) => 0u8,
            (false, false) => 1,
            (true, _) => 2,
        }
    });
    entries
        .into_iter()
        .enumerate()
        .map(|(i, e)| {
            let mut out = serde_json::Map::new();
            for key in ["id", "content", "queuedAt", "turnId"] {
                if let Some(v) = e.get(key) {
                    out.insert(key.to_string(), v.clone());
                }
            }
            out.insert("position".to_string(), json!(i));
            for key in ["interruptPriority", "editing", "requeuedAfterFailure"] {
                if e.get(key).and_then(Value::as_bool).unwrap_or(false) {
                    out.insert(key.to_string(), Value::Bool(true));
                }
            }
            if let Some(md) = e.get("messageMetadata") {
                if let Some(from) = md.get("fromAgentId").and_then(Value::as_str) {
                    out.insert("fromAgentId".to_string(), json!(from));
                    if let Some(name) = md.get("fromAgentName").and_then(Value::as_str) {
                        out.insert("fromAgentName".to_string(), json!(name));
                    }
                }
            }
            Value::Object(out)
        })
        .collect()
}

/// Truncate a presented queue entry's `content` to
/// [`STATUS_QUEUE_PREVIEW_MAX_CHARS`] chars, appending `…` when truncated.
fn truncate_entry_content(mut entry: Value) -> Value {
    let truncated = entry
        .get("content")
        .and_then(Value::as_str)
        .filter(|c| c.chars().count() > STATUS_QUEUE_PREVIEW_MAX_CHARS)
        .map(|c| {
            let mut t: String = c.chars().take(STATUS_QUEUE_PREVIEW_MAX_CHARS).collect();
            t.push('…');
            t
        });
    if let Some(t) = truncated {
        entry["content"] = Value::String(t);
    }
    entry
}

/// Merge `{ ok: true }` into a daemon-shaped result object (parity with the
/// TS `buildToolResponse` fallback that always stamps `ok: true`). Non-object
/// results (e.g. bare arrays) pass through unchanged.
fn merge_ok(mut v: Value) -> Value {
    if let Some(obj) = v.as_object_mut() {
        obj.entry("ok".to_string()).or_insert(Value::Bool(true));
    }
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(id: &str, extra: Value) -> Value {
        let mut v = json!({
            "id": id,
            "content": format!("content-{id}"),
            "queuedAt": "2026-01-01T00:00:00Z",
            "position": 0,
        });
        if let (Some(obj), Some(ex)) = (v.as_object_mut(), extra.as_object()) {
            for (k, val) in ex {
                obj.insert(k.clone(), val.clone());
            }
        }
        v
    }

    #[test]
    fn present_queue_sorts_next_delivery_first() {
        // Deliberately scrambled input: normal, editing, interrupt, normal.
        let raw = vec![
            entry("normal-1", json!({})),
            entry("editing-1", json!({ "editing": true })),
            entry("interrupt-1", json!({ "interruptPriority": true })),
            entry("normal-2", json!({})),
        ];
        let out = present_queue(raw);
        let ids: Vec<&str> = out.iter().map(|e| e["id"].as_str().unwrap()).collect();
        assert_eq!(ids, ["interrupt-1", "normal-1", "normal-2", "editing-1"]);
        for (i, e) in out.iter().enumerate() {
            assert_eq!(e["position"], json!(i), "position renumbered: {e}");
        }
        assert_eq!(out[0]["interruptPriority"], json!(true));
        assert_eq!(out[3]["editing"], json!(true));
    }

    #[test]
    fn present_queue_keeps_interrupt_arrival_order() {
        let raw = vec![
            entry("interrupt-1", json!({ "interruptPriority": true })),
            entry("interrupt-2", json!({ "interruptPriority": true })),
            entry("normal-1", json!({})),
        ];
        let out = present_queue(raw);
        let ids: Vec<&str> = out.iter().map(|e| e["id"].as_str().unwrap()).collect();
        assert_eq!(ids, ["interrupt-1", "interrupt-2", "normal-1"]);
    }

    #[test]
    fn present_queue_lifts_attribution_and_drops_bulk() {
        let raw = vec![entry(
            "attributed",
            json!({
                "messageMetadata": {
                    "type": "agent_message",
                    "fromAgentId": "agent-abc",
                    "fromAgentName": "Sender",
                },
                "imageBlocks": [{ "type": "image", "data": "x" }],
            }),
        )];
        let out = present_queue(raw);
        assert_eq!(out[0]["fromAgentId"], json!("agent-abc"));
        assert_eq!(out[0]["fromAgentName"], json!("Sender"));
        assert!(out[0].get("messageMetadata").is_none());
        assert!(out[0].get("imageBlocks").is_none());
    }

    #[test]
    fn present_queue_user_entries_have_no_attribution() {
        let out = present_queue(vec![entry("user-entry", json!({}))]);
        assert!(out[0].get("fromAgentId").is_none());
        assert!(out[0].get("fromAgentName").is_none());
    }

    #[test]
    fn caller_pending_entry_matches_only_the_caller() {
        let caller = AgentId::from("agent-caller");
        let queue = present_queue(vec![
            entry(
                "foreign",
                json!({ "messageMetadata": { "fromAgentId": "agent-other" } }),
            ),
            entry("user-entry", json!({})),
            entry(
                "own",
                json!({ "messageMetadata": { "fromAgentId": "agent-caller" } }),
            ),
        ]);
        assert_eq!(caller_pending_entry(&queue, &caller), Some("own"));
        assert_eq!(
            caller_pending_entry(&queue, &AgentId::from("agent-unrelated")),
            None,
            "foreign + user entries never match another caller"
        );
        assert_eq!(caller_pending_entry(&[], &caller), None, "empty queue");
    }

    #[test]
    fn caller_pending_entry_skips_editing_entries() {
        let caller = AgentId::from("agent-caller");
        let queue = present_queue(vec![entry(
            "own-editing",
            json!({
                "editing": true,
                "messageMetadata": { "fromAgentId": "agent-caller" },
            }),
        )]);
        assert_eq!(
            caller_pending_entry(&queue, &caller),
            None,
            "editing entries are skipped by the drain — not pending deliveries"
        );

        let queue = present_queue(vec![
            entry(
                "own-editing",
                json!({
                    "editing": true,
                    "messageMetadata": { "fromAgentId": "agent-caller" },
                }),
            ),
            entry(
                "own-pending",
                json!({ "messageMetadata": { "fromAgentId": "agent-caller" } }),
            ),
        ]);
        assert_eq!(
            caller_pending_entry(&queue, &caller),
            Some("own-pending"),
            "the non-editing entry still matches"
        );
    }

    #[test]
    fn truncate_entry_content_caps_at_200_chars() {
        let long: String = "x".repeat(300);
        let out = truncate_entry_content(json!({ "id": "e", "content": long }));
        let content = out["content"].as_str().unwrap();
        assert_eq!(content.chars().count(), STATUS_QUEUE_PREVIEW_MAX_CHARS + 1);
        assert!(content.ends_with('…'));

        let short = truncate_entry_content(json!({ "id": "e", "content": "short" }));
        assert_eq!(short["content"], json!("short"));
    }
}
