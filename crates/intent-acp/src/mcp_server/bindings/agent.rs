//! `ws.agent.*` bindings (WSAPI-4).
//!
//! Each entry point is a thin JS wrapper around `host({ method, args })`;
//! the Rust dispatch here routes to the shared [`WorkspaceApi`]. Caller
//! attribution (parent auto-subscribe on `create`, SUB-1 sender watch on
//! `send`/`sendToTask`, depth guard on `create`/`wakeOrCreate`, and the
//! `-32603` gate on `reportToParent`) is threaded through the
//! `caller_agent_id` argument that WSAPI-2 already carries on the MCP seam.

use std::sync::Arc;

use intent_core::{
    model::AgentDelegateInput, AgentCreateExtra, AgentId, AgentWakeOrCreateInput, NoteId,
    WorkspaceApi, WorkspaceId, MAX_DELEGATION_DEPTH,
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
        list: (includeCompleted) =>
            host({ method: 'agent.list', args: { includeCompleted } }),
        status: (agentId) => host({ method: 'agent.status', args: { agentId } }),
        diagnostics: (opts) =>
            host({ method: 'agent.diagnostics', args: { ...(opts || {}) } }),
        wakeOrCreate: (taskNoteId, contextMessage, model, messageMetadata) =>
            host({ method: 'agent.wakeOrCreate', args: { taskNoteId, contextMessage, model, messageMetadata } }),
        readConversation: (agentId, opts) =>
            host({ method: 'agent.readConversation', args: { agentId, ...(opts || {}) } }),
        summary: (agentId) => host({ method: 'agent.summary', args: { agentId } }),
        reportToParent: (report) =>
            host({ method: 'agent.reportToParent', args: { report } }),
    };
"#;

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
        "list" => list(api, ws).await,
        "status" => status(api, ws, args).await,
        "diagnostics" => diagnostics(api, ws, args).await,
        "wakeOrCreate" => wake_or_create(api, ws, caller, args).await,
        "readConversation" => read_conversation(api, ws, args).await,
        "summary" => summary(api, ws, args).await,
        "reportToParent" => report_to_parent(api, ws, caller, args).await,
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
        let _ = api
            .assign_agent(ws.clone(), NoteId::from_string(tn), agent_id.clone())
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
    // depth-guard lookup's name (no second `agent_get` round-trip); an
    // explicit `messageMetadata` in opts wins over the auto-tag.
    let kickoff_metadata = explicit_metadata(args)
        .or_else(|| caller.map(|c| agent_message_metadata(c, caller_name.as_deref())));
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
    let input = AgentDelegateInput {
        task_note_id: opt_str(args, "taskNoteId").map(NoteId::from_string),
        note_id: opt_str(args, "noteId").map(NoteId::from_string),
        task_text: opt_str(args, "taskText"),
        agent_instructions: opt_str(args, "agentInstructions"),
        specialist: opt_str(args, "specialist"),
        model: opt_str(args, "model"),
        behavior_prompt: opt_str(args, "behaviorPrompt"),
        wait_mode: opt_str(args, "waitMode"),
        skip_auto_commit: opt_bool(args, "skipAutoCommit"),
        isolation: opt_str(args, "isolation"),
    };
    let v = api
        .agent_delegate(ws.clone(), input, caller.cloned())
        .await
        .map_err(map_err)?;
    Ok(merge_ok(v))
}

async fn send(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    let agent_id_str = req_str(args, "agentId").map_err(|_| "agentId is required".to_string())?;
    let message = req_str(args, "message").map_err(|_| "message is required".to_string())?;
    let agent_id = AgentId::from(agent_id_str.as_str());
    let mut result = api
        .agent_send_message(
            ws.clone(),
            agent_id.clone(),
            message,
            None,
            None,
            None,
            opt_str(args, "priority"),
            None,
            None,
            None,
            sender_metadata(api, ws, caller, args).await,
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

async fn send_to_task(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller: Option<&AgentId>,
    args: &Value,
) -> Result<Value, String> {
    let task_note_id =
        req_str(args, "taskNoteId").map_err(|_| "taskNoteId is required".to_string())?;
    let message = req_str(args, "message").map_err(|_| "message is required".to_string())?;
    let mut result = api
        .agent_send_to_task(
            ws.clone(),
            NoteId::from_string(&task_note_id),
            message,
            opt_str(args, "priority"),
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
        "eventTypes is required. Specify category wildcards like \"agent:*\", \"file:*\" or specific types like \"agent:idle\".".to_string()
    })?;
    if event_types.is_empty() {
        return Err(
            "eventTypes is required. Specify category wildcards like \"agent:*\", \"file:*\" or specific types like \"agent:idle\".".to_string(),
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

async fn list(api: &Arc<dyn WorkspaceApi>, ws: &WorkspaceId) -> Result<Value, String> {
    let rows = api.agent_list(ws.clone()).await.map_err(map_err)?;
    serde_json::to_value(rows).map_err(|e| e.to_string())
}

async fn status(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    args: &Value,
) -> Result<Value, String> {
    let agent_id_str = req_str(args, "agentId").map_err(|_| "agentId is required".to_string())?;
    let agent = api
        .agent_get(AgentId::from(agent_id_str.as_str()), Some(ws.clone()))
        .await
        .map_err(map_err)?;
    serde_json::to_value(agent).map_err(|e| e.to_string())
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
    // same explicit-wins/auto-tag semantics as `send`, reusing the
    // depth-guard lookup's name (no second `agent_get` round-trip).
    let message_metadata = explicit_metadata(args)
        .or_else(|| caller.map(|c| agent_message_metadata(c, caller_name.as_deref())));
    let v = api
        .agent_wake_or_create(
            ws.clone(),
            NoteId::from_string(&task_note_id),
            context_message,
            AgentWakeOrCreateInput {
                model: opt_str(args, "model"),
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
/// Explicit metadata always wins over auto-tagging; `null` is treated as
/// absent (it does not suppress the auto-tag).
fn explicit_metadata(args: &Value) -> Option<Value> {
    args.get("messageMetadata")
        .filter(|v| !v.is_null())
        .cloned()
}

/// `messageMetadata` for agent-originated sends: an explicit caller-supplied
/// `messageMetadata` arg takes precedence; otherwise, when the caller is an
/// agent (not the FE/RPC front door), auto-tag the delivered message with
/// [`agent_message_metadata`] so clients can render who sent it. `fromAgentName`
/// is resolved from the caller's session; human-originated sends
/// (`caller == None`, no explicit metadata) return `None` and stay untagged.
async fn sender_metadata(
    api: &Arc<dyn WorkspaceApi>,
    ws: &WorkspaceId,
    caller: Option<&AgentId>,
    args: &Value,
) -> Option<Value> {
    if let Some(explicit) = explicit_metadata(args) {
        return Some(explicit);
    }
    let caller = caller?;
    let name = api
        .agent_get(caller.clone(), Some(ws.clone()))
        .await
        .ok()
        .map(|lite| lite.name);
    Some(agent_message_metadata(caller, name.as_deref()))
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

/// Merge `{ ok: true }` into a daemon-shaped result object (parity with the
/// TS `buildToolResponse` fallback that always stamps `ok: true`). Non-object
/// results (e.g. bare arrays) pass through unchanged.
fn merge_ok(mut v: Value) -> Value {
    if let Some(obj) = v.as_object_mut() {
        obj.entry("ok".to_string()).or_insert(Value::Bool(true));
    }
    v
}
