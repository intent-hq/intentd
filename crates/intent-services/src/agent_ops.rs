//! `agent.*` RPC helpers (PROTOCOL §5.5).
//!
//! Pure projections + the model-catalog helpers that back the `agent.*`
//! `WorkspaceApi` methods (the trait bodies live in `lib.rs`). The
//! [`AgentLite`] derivation (`lastAgentResponse`/`digest`) ports the TS
//! `agent.list`/`agent.get` post-processing; [`static_models`] /
//! [`parse_model_list_output`] port the `agent.getModels` static-tier fallback
//! and auggie CLI parser respectively.

use std::collections::HashSet;

use intent_core::events::{AGENT_DELETED, AGENT_FAILED, AGENT_IDLE};
use intent_core::{
    now_iso, parse_iso, AgentId, AgentLite, AgentMessage, AgentSession, AgentStatus, Error, NoteId,
    Result, WorkspaceApi, WorkspaceId,
};

/// Default `agent.diagnostics` stale-responding threshold (10 minutes), matching
/// the TS `DEFAULT_STALE_RESPONDING_AFTER_MS`.
const DEFAULT_STALE_RESPONDING_AFTER_MS: i64 = 10 * 60 * 1000;

use crate::agent_subscriptions::CompletionWatch;

/// `waitMode` value that defers the completion watch into an `after_all`
/// delegation group (AS-4) rather than registering a standalone oneShot here.
const WAIT_MODE_AFTER_ALL: &str = "after_all";
use intent_providers::models::PROVIDER_MODEL_TIERS;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::Services;

#[cfg(test)]
mod tests;

/// Default `agent.getConversation` cap (TS `MAX_WEBSOCKET_CONVERSATION_MESSAGES`).
const MAX_CONVERSATION_MESSAGES: i64 = 200;

/// One pending message in an agent's in-memory send queue (`agent.getQueue`).
#[derive(Debug, Clone)]
pub(crate) struct QueuedMessage {
    pub id: String,
    pub content: String,
    pub image_blocks: Option<Value>,
    pub queued_at: String,
}

impl QueuedMessage {
    /// The camelCase wire shape for `agent.getQueue` / queue results, matching the
    /// TS `QueuedMessage` and the iOS decoder (`{id, content, queuedAt, position,
    /// imageBlocks?}`). `position` is the entry's 0-based index in the queue (0 =
    /// next to be sent) and is supplied by the caller since it is positional.
    pub(crate) fn to_value(&self, position: usize) -> Value {
        let mut v = json!({
            "id": self.id,
            "content": self.content,
            "queuedAt": self.queued_at,
            "position": position,
        });
        if let Some(blocks) = &self.image_blocks {
            v["imageBlocks"] = blocks.clone();
        }
        v
    }
}

/// Collect the `text` of every `type: "text"` content block in a message's
/// `content` (a JSON array of blocks; non-arrays yield nothing).
fn text_blocks(content: &Value) -> Vec<String> {
    let Some(blocks) = content.as_array() else {
        return Vec::new();
    };
    blocks
        .iter()
        .filter(|b| b.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|b| b.get("text").and_then(Value::as_str))
        .map(str::to_string)
        .collect()
}

/// Remove all `start..=end` delimited spans from `s` (inclusive of the markers).
fn strip_spans(s: &str, start: &str, end: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find(start) {
        out.push_str(&rest[..i]);
        let after = &rest[i + start.len()..];
        match after.find(end) {
            Some(j) => rest = &after[j + end.len()..],
            None => {
                rest = "";
                break;
            }
        }
    }
    out.push_str(rest);
    out
}

/// Drop `<group:…>` / `</group…>` tags (the streamed response group markers).
fn strip_group_tags(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find('<') {
        let tail = &rest[i..];
        let is_group = tail.starts_with("<group:") || tail.starts_with("</group");
        if is_group {
            if let Some(j) = tail.find('>') {
                out.push_str(&rest[..i]);
                rest = &tail[j + 1..];
                continue;
            }
        }
        out.push_str(&rest[..i + 1]);
        rest = &rest[i + 1..];
    }
    out.push_str(rest);
    out
}

/// Clean an assistant text block of digest/suggested-prompts/group markers,
/// mirroring the TS `agent.list` cleaning before the last-line extraction.
fn clean_response_text(text: &str) -> String {
    let mut cleaned = strip_spans(text, "<agent_digest>", "</agent_digest>");
    cleaned = strip_spans(&cleaned, "<!-- suggested-prompts", "-->");
    cleaned = strip_group_tags(&cleaned);
    cleaned.trim().to_string()
}

/// Derive `(lastAgentResponse, digest)` from the most-recent assistant message,
/// porting the TS `agent.list`/`agent.get` post-processing (PROTOCOL §5.5).
pub(crate) fn last_response_and_digest(
    messages: &[AgentMessage],
) -> (Option<String>, Option<String>) {
    for msg in messages.iter().rev() {
        if msg.role != "assistant" {
            continue;
        }
        let blocks = text_blocks(&msg.content);
        let mut digest: Option<String> = None;
        let mut last_response: Option<String> = None;
        for block in &blocks {
            let text = block.trim();
            if text.is_empty() {
                continue;
            }
            if digest.is_none() {
                if let Some(d) = strip_spans_capture(text, "<agent_digest>", "</agent_digest>") {
                    digest = Some(d.trim().to_string());
                }
            }
            let cleaned = clean_response_text(text);
            if !cleaned.is_empty() {
                let line = cleaned
                    .lines()
                    .rfind(|l| !l.trim().is_empty())
                    .map(|l| l.trim().to_string())
                    .unwrap_or_else(|| cleaned.chars().take(200).collect());
                last_response = Some(line);
            }
        }
        return (last_response, digest);
    }
    (None, None)
}

/// Derive `lastUserMessage` from the most-recent `user` message's text blocks
/// (joined), porting the TS `agent.list`/`agent.get` activity field.
pub(crate) fn last_user_message(messages: &[AgentMessage]) -> Option<String> {
    for msg in messages.iter().rev() {
        if msg.role != "user" {
            continue;
        }
        let text = text_blocks(&msg.content).join("\n").trim().to_string();
        return if text.is_empty() { None } else { Some(text) };
    }
    None
}

/// Capture the first `start..end` span's inner text (for digest extraction).
fn strip_spans_capture(s: &str, start: &str, end: &str) -> Option<String> {
    let i = s.find(start)?;
    let after = &s[i + start.len()..];
    let j = after.find(end)?;
    Some(after[..j].to_string())
}

/// The static model catalog used as the `agent.getModels` fallback when the
/// auggie CLI is unavailable: every `(provider, tier)` model from
/// `PROVIDER_MODEL_TIERS`, deduped by `provider:model` (PROTOCOL §5.5).
pub(crate) fn static_models() -> Vec<Value> {
    let mut seen = HashSet::new();
    let mut out = Vec::new();
    for (provider_id, tiers) in PROVIDER_MODEL_TIERS {
        for (tier, model_id) in [
            ("fast", tiers.fast),
            ("balanced", tiers.balanced),
            ("smart", tiers.smart),
        ] {
            let key = format!("{provider_id}:{model_id}");
            if seen.insert(key) {
                out.push(json!({
                    "id": model_id,
                    "name": format!("{model_id} ({tier})"),
                    "provider": provider_id,
                }));
            }
        }
    }
    out
}

/// Parse `auggie model list` output into `(value, label, description?)` rows,
/// porting the TS `parseModelListOutput` (`- Label [model-id]` + an optional
/// indented description on the next line).
pub(crate) fn parse_model_list_output(stdout: &str) -> Vec<(String, String, Option<String>)> {
    let lines: Vec<&str> = stdout.split('\n').collect();
    let mut models = Vec::new();
    let mut i = 0;
    while i < lines.len() {
        let trimmed = lines[i].trim();
        if trimmed.is_empty() || trimmed.starts_with("Available models") {
            i += 1;
            continue;
        }
        if let Some((label, value)) = parse_model_line(trimmed) {
            let mut description = None;
            if i + 1 < lines.len() {
                let next = lines[i + 1].trim();
                if !next.is_empty() && !next.starts_with('-') && !next.starts_with("Available") {
                    description = Some(next.to_string());
                    i += 1;
                }
            }
            models.push((value, label, description));
        }
        i += 1;
    }
    models
}

/// Parse a single `- Label [model-id]` line into `(label, value)`.
fn parse_model_line(line: &str) -> Option<(String, String)> {
    let rest = line.strip_prefix('-')?.trim_start();
    let open = rest.find('[')?;
    let close = rest[open + 1..].find(']')? + open + 1;
    let label = rest[..open].trim().to_string();
    let value = rest[open + 1..close].trim().to_string();
    if label.is_empty() || value.is_empty() {
        return None;
    }
    Some((label, value))
}

/// Best-effort `agent.getModels` dynamic fetch: run `auggie model list`, parse
/// stdout (then stderr), and map to wire models. Returns `Ok(None)` when the
/// CLI is unavailable or yields nothing, so the caller can fall back to
/// [`static_models`].
pub(crate) async fn fetch_auggie_models() -> Result<Option<Vec<Value>>> {
    let output = match tokio::process::Command::new("auggie")
        .args(["model", "list"])
        .output()
        .await
    {
        Ok(o) => o,
        Err(_) => return Ok(None),
    };
    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut parsed = parse_model_list_output(&stdout);
    if parsed.is_empty() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        parsed = parse_model_list_output(&stderr);
    }
    if parsed.is_empty() {
        return Ok(None);
    }
    let models = parsed
        .into_iter()
        .map(|(value, label, description)| {
            let mut m = json!({ "id": value, "name": label, "provider": "auggie" });
            if let Some(d) = description {
                m["description"] = Value::String(d);
            }
            m
        })
        .collect();
    Ok(Some(models))
}

/// Mint a stable user-message id (`user-msg-{uuid}`), mirroring the TS
/// `agent.sendMessage` `messageId` default.
pub(crate) fn new_message_id() -> String {
    format!("user-msg-{}", Uuid::new_v4())
}

/// A single user text content block (the persisted/queued message shape).
fn user_content_blocks(content: &str) -> Value {
    json!([{ "type": "text", "text": content }])
}

/// Project an [`AgentSession`] (with its loaded messages) into [`AgentLite`].
fn project_lite(session: AgentSession) -> AgentLite {
    let (last_response, digest) = last_response_and_digest(&session.messages);
    let last_user = last_user_message(&session.messages);
    let count = session.messages.len() as u64;
    AgentLite::from_session(session, count, last_response, last_user, digest)
}

impl Services {
    /// `agent.list` (PROTOCOL §5.5).
    pub(crate) async fn agent_list_op(&self, workspace_id: WorkspaceId) -> Result<Vec<AgentLite>> {
        let sessions = self.store.list_agent_sessions(&workspace_id).await?;
        Ok(sessions.into_iter().map(project_lite).collect())
    }

    /// `agent.get` (PROTOCOL §5.5). `NotFound` is surfaced to the router which
    /// maps it to `-32602 "Agent not found"`.
    pub(crate) async fn agent_get_op(&self, agent_id: AgentId) -> Result<AgentLite> {
        let session = self.store.get_agent_session(&agent_id).await?;
        Ok(project_lite(session))
    }

    /// `agent.getConversation` (PROTOCOL §5.5).
    pub(crate) async fn agent_get_conversation_op(
        &self,
        agent_id: AgentId,
        limit: Option<i64>,
    ) -> Result<Value> {
        let session = self.store.get_agent_session(&agent_id).await?;
        let mut messages = session.messages;
        let total = messages.len() as i64;
        let limit = limit.unwrap_or(MAX_CONVERSATION_MESSAGES);
        let truncated = limit >= 0 && total > limit;
        if truncated {
            let start = (total - limit) as usize;
            messages = messages.split_off(start);
        }
        Ok(json!({
            "agentId": agent_id,
            "messages": messages,
            "truncated": truncated,
            "totalMessages": total,
        }))
    }

    /// `agent.create`: persist a new session; the process spawns lazily on first
    /// turn (PROTOCOL §5.5).
    pub(crate) async fn agent_create_op(
        &self,
        workspace_id: WorkspaceId,
        name: Option<String>,
        model: Option<String>,
        specialist: Option<String>,
        parent_agent_id: Option<AgentId>,
    ) -> Result<Value> {
        let now = now_iso();
        let name_explicitly_set = name.is_some();
        let name =
            name.unwrap_or_else(|| format!("Agent {}", &Uuid::new_v4().simple().to_string()[..6]));
        let session = AgentSession {
            id: AgentId(format!("agent-{}", Uuid::new_v4())),
            workspace_id,
            parent_agent_id,
            backend_session_id: None,
            acp_session_id: None,
            name,
            name_explicitly_set,
            model,
            provider: None,
            system_prompt: None,
            specialist,
            status: AgentStatus::Pending,
            is_active: false,
            messages: Vec::new(),
            stats: None,
            created_at: now.clone(),
            updated_at: now,
        };
        self.store.insert_agent_session(&session).await?;
        Ok(json!({ "agent": { "id": session.id, "name": session.name } }))
    }

    /// `agent.rename` (PROTOCOL §5.5). A missing agent surfaces as `-32603`
    /// (matching the TS `renameAgentOnDisk` failure path).
    pub(crate) async fn agent_rename_op(&self, agent_id: AgentId, name: String) -> Result<Value> {
        let mut session = self.load_session_internal(&agent_id).await?;
        session.name = name.clone();
        session.name_explicitly_set = true;
        session.updated_at = now_iso();
        self.store.update_agent_session(&session).await?;
        Ok(json!({ "success": true, "name": name }))
    }

    /// `agent.setModel` (PROTOCOL §5.5).
    pub(crate) async fn agent_set_model_op(
        &self,
        agent_id: AgentId,
        model_id: String,
    ) -> Result<Value> {
        let mut session = self.load_session_internal(&agent_id).await?;
        session.model = Some(model_id.clone());
        session.updated_at = now_iso();
        self.store.update_agent_session(&session).await?;
        Ok(json!({ "success": true, "modelId": model_id }))
    }

    /// `agent.delete`: idempotent session delete (PROTOCOL §5.5).
    pub(crate) async fn agent_delete_op(&self, agent_id: AgentId) -> Result<Value> {
        // Capture the workspace before deleting so the post-delete agent:deleted
        // emit can be workspace-scoped. If the session is already gone, skip the
        // emit gracefully rather than failing the idempotent delete.
        let workspace_id = self
            .store
            .get_agent_session(&agent_id)
            .await
            .ok()
            .map(|s| s.workspace_id);
        self.store.delete_agent_session(&agent_id).await?;
        self.agent_queues
            .lock()
            .expect("agent queue registry poisoned")
            .remove(&agent_id);
        if let Some(workspace_id) = workspace_id {
            crate::publish_event(
                &self.event_bus,
                intent_store::NewEvent {
                    workspace_id,
                    timestamp: now_iso(),
                    event_type: intent_core::events::AGENT_DELETED.to_string(),
                    actor: crate::system_actor(),
                    session_id: Some(agent_id.0.clone()),
                    correlation_id: None,
                    parent_event_id: None,
                    metadata: None,
                    data: json!({ "agentId": agent_id.0 }),
                },
            )
            .await;
        }
        Ok(json!({ "success": true }))
    }

    /// `agent.getModels`: auggie CLI with the static-tier fallback (PROTOCOL §5.5).
    pub(crate) async fn agent_get_models_op(&self) -> Result<Value> {
        let models = match fetch_auggie_models().await? {
            Some(m) => m,
            None => static_models(),
        };
        Ok(json!({ "models": models }))
    }

    /// `agent.queueMessage` (PROTOCOL §5.5).
    pub(crate) async fn agent_queue_message_op(
        &self,
        agent_id: AgentId,
        content: String,
        image_blocks: Option<Value>,
    ) -> Result<Value> {
        let (queued, position) = self.enqueue_message(&agent_id, content, image_blocks);
        Ok(json!({ "success": true, "queuedMessage": queued.to_value(position) }))
    }

    /// `agent.getQueue` (PROTOCOL §5.5).
    pub(crate) async fn agent_get_queue_op(&self, agent_id: AgentId) -> Result<Value> {
        let queue: Vec<Value> = self
            .agent_queues
            .lock()
            .expect("agent queue registry poisoned")
            .get(&agent_id)
            .map(|q| q.iter().enumerate().map(|(i, m)| m.to_value(i)).collect())
            .unwrap_or_default();
        Ok(json!({ "success": true, "queue": queue }))
    }

    /// `agent.editQueuedMessage` (PROTOCOL §5.5).
    pub(crate) async fn agent_edit_queued_message_op(
        &self,
        agent_id: AgentId,
        message_id: String,
        content: String,
    ) -> Result<Value> {
        let mut guard = self
            .agent_queues
            .lock()
            .expect("agent queue registry poisoned");
        let queue = guard
            .get_mut(&agent_id)
            .ok_or_else(|| Error::Internal("Queued message not found".to_string()))?;
        let position = queue
            .iter()
            .position(|m| m.id == message_id)
            .ok_or_else(|| Error::Internal("Queued message not found".to_string()))?;
        queue[position].content = content;
        Ok(json!({ "success": true, "queuedMessage": queue[position].to_value(position) }))
    }

    /// `agent.removeQueuedMessage` (PROTOCOL §5.5).
    pub(crate) async fn agent_remove_queued_message_op(
        &self,
        agent_id: AgentId,
        message_id: String,
    ) -> Result<Value> {
        let mut guard = self
            .agent_queues
            .lock()
            .expect("agent queue registry poisoned");
        let queue = guard
            .get_mut(&agent_id)
            .ok_or_else(|| Error::Internal("Queued message not found".to_string()))?;
        let before = queue.len();
        queue.retain(|m| m.id != message_id);
        if queue.len() == before {
            return Err(Error::Internal("Queued message not found".to_string()));
        }
        Ok(json!({ "success": true }))
    }

    /// `agent.sendMessage`: persist the user message; on failure auto-queue
    /// (PROTOCOL §5.5).
    pub(crate) async fn agent_send_message_op(
        &self,
        agent_id: AgentId,
        content: String,
        message_id: Option<String>,
    ) -> Result<Value> {
        let message_id = message_id.unwrap_or_else(new_message_id);
        let blocks = user_content_blocks(&content);
        match self
            .store
            .append_agent_message(&agent_id, "user", &blocks, &now_iso())
            .await
        {
            Ok(_) => Ok(json!({ "success": true, "queued": false, "messageId": message_id })),
            Err(_) => {
                let (queued, position) = self.enqueue_message(&agent_id, content, None);
                Ok(json!({
                    "success": true,
                    "queued": true,
                    "queuedMessage": queued.to_value(position),
                }))
            }
        }
    }

    /// `agent.forceMessage`: stop the current stream (best-effort) then deliver
    /// immediately with the caller-supplied `messageId` (PROTOCOL §5.5).
    pub(crate) async fn agent_force_message_op(
        &self,
        agent_id: AgentId,
        message_id: String,
        content: String,
    ) -> Result<Value> {
        let blocks = user_content_blocks(&content);
        self.store
            .append_agent_message(&agent_id, "user", &blocks, &now_iso())
            .await?;
        Ok(json!({ "success": true, "queued": false, "messageId": message_id }))
    }

    /// `agent.summary`: a quick summary derived from the transcript (PROTOCOL §5.5).
    pub(crate) async fn agent_summary_op(&self, agent_id: AgentId) -> Result<Value> {
        let session = self.load_session_internal(&agent_id).await?;
        let last_response = last_assistant_text(&session.messages);
        let tool_counts = tool_call_counts(&session.messages);
        let mut out = json!({
            "agentId": agent_id,
            "agentName": session.name,
            "status": session.status,
            "messageCount": session.messages.len(),
            "toolCallCounts": tool_counts,
            "createdAt": session.created_at,
            "updatedAt": session.updated_at,
        });
        if let Some(text) = last_response {
            out["lastResponse"] = Value::String(text);
        }
        Ok(out)
    }

    /// `agent.reportToParent`: a delegated child reports back to its parent
    /// (PROTOCOL §5.5). Caller identity comes only from the MCP front door; the
    /// RPC dispatch path passes `None`, so it always surfaces `-32603`. When the
    /// caller has no `parentAgentId` (created directly by a user), this is also
    /// `-32603`. Otherwise the report is delivered to the parent by reusing the
    /// send-message path.
    pub(crate) async fn agent_report_to_parent_op(
        &self,
        _workspace_id: WorkspaceId,
        report: Value,
        caller_agent_id: Option<AgentId>,
    ) -> Result<Value> {
        let not_delegated = || {
            Error::Internal("report_to_parent is only available to delegated agents".to_string())
        };
        let caller = caller_agent_id.ok_or_else(not_delegated)?;
        let session = self.load_session_internal(&caller).await?;
        let parent = session.parent_agent_id.ok_or_else(not_delegated)?;
        // `report` is declared as a string on the MCP surface; coerce other
        // JSON shapes to their textual form for delivery.
        let report_text = match &report {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        let report_len = report_text.chars().count() as i64;
        // NOTE: TS persists a `completionReport` on the child that the parent
        // reads on completion; here we deliver eagerly via the send-message path
        // (no completion-subscription/waitMode behavior). The returned shape
        // mirrors the TS service result: { ok, parentAgentId, reportLength,
        // savedAt }.
        let _ = self
            .agent_send_message_op(parent.clone(), report_text, None)
            .await?;
        Ok(json!({
            "ok": true,
            "parentAgentId": parent,
            "reportLength": report_len,
            "savedAt": now_iso(),
        }))
    }

    /// `agent.delegate`: create a session and (best-effort) assign it to the
    /// target task note (PROTOCOL §5.5).
    pub(crate) async fn agent_delegate_op(
        &self,
        workspace_id: WorkspaceId,
        input: intent_core::AgentDelegateInput,
        parent_agent_id: Option<AgentId>,
    ) -> Result<Value> {
        let wait_mode = input.wait_mode.clone();
        let created = self
            .agent_create_op(
                workspace_id.clone(),
                None,
                input.model,
                input.specialist,
                parent_agent_id.clone(),
            )
            .await?;
        let agent_id = created["agent"]["id"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let name = created["agent"]["name"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        if let Some(task_note_id) = input.task_note_id.clone().or(input.note_id.clone()) {
            let _ = self
                .assign_agent(workspace_id.clone(), task_note_id, agent_id.clone())
                .await;
        }
        // Auto-subscribe the delegating caller to the child's completion (AS-2).
        // Only the MCP front door carries a caller (`parent_agent_id = Some`); the
        // RPC front door (`None`) registers nothing. `after_all` defers to the
        // delegation-group fan-in (AS-4); `immediate`/default registers a oneShot.
        if let Some(parent) = parent_agent_id {
            // Best-effort guard: skip if the parent agent is already deleted
            // (TS `selectIsAgentDeleted`).
            let parent_session = self.store.get_agent_session(&parent).await.ok();
            let parent_deleted = parent_session
                .as_ref()
                .map(|s| s.status == AgentStatus::Deleted)
                .unwrap_or(false);
            if !parent_deleted {
                let parent_name = parent_session.map(|s| s.name).unwrap_or_default();
                let child = AgentId::from(agent_id.as_str());
                if wait_mode.as_deref() == Some(WAIT_MODE_AFTER_ALL) {
                    // Enroll the child in the parent's after_all delegation group
                    // and register a group watch (group_id = Some, not oneShot) so
                    // the delivery worker routes its completion into the group
                    // fan-in instead of waking the parent immediately (AS-4).
                    let gid = self.get_or_create_delegation_group(&workspace_id, &parent);
                    self.enroll_child_in_group(&workspace_id, &gid, &child);
                    self.register_completion_watch(
                        &workspace_id,
                        parent,
                        parent_name,
                        child,
                        false,
                        Some(gid),
                    );
                } else {
                    self.register_completion_watch(
                        &workspace_id,
                        parent,
                        parent_name,
                        child,
                        true,
                        None,
                    );
                }
            }
        }
        Ok(json!({ "ok": true, "agentId": agent_id, "name": name }))
    }

    /// `agent.getSubscriptions`: live completion-watch payload for `agent_id`
    /// from the AS-2/AS-4 registry, in the TS camelCase wire shape with the
    /// `subscriptions`, `delegationGroups`, and `agentStatuses` fields.
    /// `awaitMode` maps the registry's `after_all` to TS's `"all"`;
    /// `agentStatuses` is best-effort, keyed off the persisted `AgentStatus` of
    /// the agents present in the payload.
    pub(crate) async fn agent_get_subscriptions_op(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
    ) -> Result<Value> {
        let watches = self.list_watches_for_parent(&workspace_id, &agent_id);
        let groups = self.list_groups_for_parent(&workspace_id, &agent_id);

        let event_types = [AGENT_IDLE, AGENT_FAILED, AGENT_DELETED];

        let mut present: Vec<AgentId> = vec![agent_id.clone()];
        let subscriptions: Vec<Value> = watches
            .iter()
            .map(|w| {
                if !present.contains(&w.child_agent_id) {
                    present.push(w.child_agent_id.clone());
                }
                let delegation_group = w.group_id.as_ref().and_then(|gid| {
                    groups.iter().find(|g| &g.group_id == gid).map(|g| {
                        json!({
                            "groupId": g.group_id,
                            "awaitMode": "all",
                            "expectedAgentIds": g.expected_agent_ids,
                        })
                    })
                });
                let description = describe_subscription(w, &event_types, delegation_group.as_ref());
                json!({
                    "id": w.id,
                    "agentId": w.parent_agent_id,
                    "agentName": w.parent_agent_name,
                    "workspaceId": workspace_id,
                    "createdAt": w.created_at,
                    "oneShot": w.one_shot,
                    "actorIds": [w.child_agent_id],
                    "eventTypes": event_types,
                    "delegationGroup": delegation_group,
                    "description": description,
                })
            })
            .collect();

        let delegation_groups: Vec<Value> = groups
            .iter()
            .map(|g| {
                for id in &g.expected_agent_ids {
                    if !present.contains(id) {
                        present.push(id.clone());
                    }
                }
                json!({
                    "groupId": g.group_id,
                    "parentAgentId": g.parent_agent_id,
                    "awaitMode": "all",
                    "expectedAgentIds": g.expected_agent_ids,
                    "completedAgentIds": g.completed_agent_ids,
                    "deletedAgentIds": g.deleted_agent_ids,
                    "delivered": g.delivered,
                })
            })
            .collect();

        let mut agent_statuses = serde_json::Map::new();
        for id in &present {
            if let Ok(session) = self.store.get_agent_session(id).await {
                if let Some(word) = agent_status_wire(session.status) {
                    agent_statuses.insert(id.0.clone(), json!(word));
                }
            }
        }

        Ok(json!({
            "subscriptions": subscriptions,
            "delegationGroups": delegation_groups,
            "agentStatuses": Value::Object(agent_statuses),
        }))
    }

    /// `agent.cancelSubscriptions`: remove every completion watch registered by
    /// `agent_id` and drop any delegation groups it parents. Idempotent — always
    /// returns `{ "success": true }` (TS shape).
    pub(crate) async fn agent_cancel_subscriptions_op(
        &self,
        workspace_id: WorkspaceId,
        agent_id: AgentId,
    ) -> Result<Value> {
        self.remove_all_for_parent(&workspace_id, &agent_id);
        self.remove_groups_for_parent(&workspace_id, &agent_id);
        Ok(json!({ "success": true }))
    }

    /// `agent.diagnostics`: a sanitized snapshot of agent statuses,
    /// subscriptions, delegation groups, and stuck-risk signals (PROTOCOL §5.5).
    ///
    /// Ports the TS `buildAgentDiagnosticsSnapshot` shape over the daemon's
    /// (simpler) runtime: completion-watch records back the `subscriptions` view
    /// and the delegation-group registry backs `delegationGroups`. The daemon
    /// does not track per-agent event queues, deleted-agent references, or
    /// delivery health, so `queues`, `deletedAgentReferences`, `recentEvents` are
    /// empty and `deliveryStats` is zeroed — honestly reflecting what the runtime
    /// knows about. Returns `{ ok, diagnostics, text }` (`buildToolResponse`).
    pub(crate) async fn agent_diagnostics_op(
        &self,
        workspace_id: WorkspaceId,
        agent_id: Option<AgentId>,
        task_note_id: Option<NoteId>,
        stale_responding_after_ms: Option<i64>,
    ) -> Result<Value> {
        let stale_after_ms = stale_responding_after_ms.unwrap_or(DEFAULT_STALE_RESPONDING_AFTER_MS);
        let now = now_iso();
        let now_ms = iso_ms(&now);

        let sessions = self.store.list_agent_sessions(&workspace_id).await?;
        let watches = self.all_watches(&workspace_id);
        let groups = self.all_groups(&workspace_id);

        let agent_filter = agent_id.as_ref().map(|a| a.0.clone());
        // Sessions carry no taskNoteId in the daemon model, so a taskNoteId
        // filter matches nothing (mirrors `agent.metadata?.taskNoteId` undefined).
        let task_filter = task_note_id.as_ref().map(|n| n.0.clone());
        let has_filter = agent_filter.is_some() || task_filter.is_some();

        let session_ids: HashSet<String> = sessions.iter().map(|s| s.id.0.clone()).collect();
        let session_by_id: std::collections::HashMap<String, &AgentSession> =
            sessions.iter().map(|s| (s.id.0.clone(), s)).collect();
        let watch_ids: HashSet<String> = watches.iter().map(|w| w.id.clone()).collect();

        let mut matching: HashSet<String> = HashSet::new();
        for s in &sessions {
            if let Some(aid) = &agent_filter {
                if &s.id.0 != aid {
                    continue;
                }
            }
            if task_filter.is_some() {
                continue;
            }
            matching.insert(s.id.0.clone());
        }
        if let Some(aid) = &agent_filter {
            matching.insert(aid.clone());
        }
        let in_scope = |id: &str| !has_filter || matching.contains(id);
        let intersects_scope =
            |ids: &[String]| !has_filter || ids.iter().any(|id| matching.contains(id));

        // Union of every agent id referenced anywhere in the snapshot.
        let mut all_agent_ids: Vec<String> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        let push_id = |id: &str, all: &mut Vec<String>, seen: &mut HashSet<String>| {
            if seen.insert(id.to_string()) {
                all.push(id.to_string());
            }
        };
        for s in &sessions {
            push_id(&s.id.0, &mut all_agent_ids, &mut seen);
        }
        for w in &watches {
            push_id(&w.parent_agent_id.0, &mut all_agent_ids, &mut seen);
            push_id(&w.child_agent_id.0, &mut all_agent_ids, &mut seen);
        }
        for g in &groups {
            push_id(&g.parent_agent_id.0, &mut all_agent_ids, &mut seen);
            for id in &g.expected_agent_ids {
                push_id(&id.0, &mut all_agent_ids, &mut seen);
            }
            for id in &g.completed_agent_ids {
                push_id(&id.0, &mut all_agent_ids, &mut seen);
            }
            for id in &g.deleted_agent_ids {
                push_id(&id.0, &mut all_agent_ids, &mut seen);
            }
        }

        let event_types = [AGENT_IDLE, AGENT_FAILED, AGENT_DELETED];

        // subscriptions (completion watches), filtered to scope.
        let subscriptions: Vec<Value> = watches
            .iter()
            .filter(|w| {
                in_scope(&w.parent_agent_id.0)
                    || intersects_scope(std::slice::from_ref(&w.child_agent_id.0))
            })
            .map(|w| {
                json!({
                    "id": w.id,
                    "agentId": w.parent_agent_id,
                    "agentName": w.parent_agent_name,
                    "createdAt": w.created_at,
                    "eventTypes": event_types,
                    "actorIds": [w.child_agent_id.clone()],
                    "priority": "normal",
                    "oneShot": w.one_shot,
                    "delegationGroupId": w.group_id,
                    "orphaned": !session_ids.contains(&w.parent_agent_id.0),
                })
            })
            .collect();

        // delegationGroups, filtered to scope.
        let delegation_groups: Vec<Value> = groups
            .iter()
            .filter(|g| {
                let mut ids = vec![g.parent_agent_id.0.clone()];
                ids.extend(g.expected_agent_ids.iter().map(|a| a.0.clone()));
                ids.extend(g.completed_agent_ids.iter().map(|a| a.0.clone()));
                ids.extend(g.deleted_agent_ids.iter().map(|a| a.0.clone()));
                intersects_scope(&ids)
            })
            .map(|g| {
                let done: HashSet<String> = g
                    .completed_agent_ids
                    .iter()
                    .chain(g.deleted_agent_ids.iter())
                    .map(|a| a.0.clone())
                    .filter(|id| g.expected_agent_ids.iter().any(|e| &e.0 == id))
                    .collect();
                let pending: Vec<String> = g
                    .expected_agent_ids
                    .iter()
                    .map(|a| a.0.clone())
                    .filter(|id| !done.contains(id))
                    .collect();
                let complete = !g.expected_agent_ids.is_empty()
                    && g.expected_agent_ids.iter().all(|id| {
                        g.completed_agent_ids.contains(id) || g.deleted_agent_ids.contains(id)
                    });
                let subscription_missing = match &g.subscription_id {
                    Some(sid) => !watch_ids.contains(sid),
                    None => true,
                };
                json!({
                    "groupId": g.group_id,
                    "parentAgentId": g.parent_agent_id,
                    "awaitMode": g.await_mode,
                    "expectedAgentIds": g.expected_agent_ids,
                    "completedAgentIds": g.completed_agent_ids,
                    "deletedAgentIds": g.deleted_agent_ids,
                    "pendingAgentIds": pending,
                    "subscriptionId": g.subscription_id.clone().unwrap_or_default(),
                    "subscriptionMissing": subscription_missing,
                    "delivered": g.delivered,
                    "complete": complete,
                    "eventCount": g.event_summaries.len(),
                })
            })
            .collect();

        // agents rows.
        all_agent_ids.sort();
        let mut agent_rows: Vec<Value> = Vec::new();
        for id in &all_agent_ids {
            if !in_scope(id) {
                continue;
            }
            let session = session_by_id.get(id).copied();
            let status = session
                .and_then(|s| agent_status_wire(s.status))
                .unwrap_or("unknown");
            let message_count = session.map(|s| s.messages.len() as u64);
            let last_activity = session.map(|s| s.updated_at.clone());
            let last_activity_age = last_activity.as_deref().map(|t| age_ms(now_ms, t));
            let stale_responding = status == "responding"
                && match last_activity_age {
                    None => true,
                    Some(age) => age > stale_after_ms,
                };
            let pending_initial_response = session.is_some()
                && status.eq_ignore_ascii_case("idle")
                && message_count == Some(1)
                && !session
                    .map(|s| has_assistant_message(&s.messages))
                    .unwrap_or(false);
            let subscription_count = watches
                .iter()
                .filter(|w| &w.parent_agent_id.0 == id)
                .count();

            let mut row = serde_json::Map::new();
            row.insert("id".into(), json!(id));
            if let Some(s) = session {
                row.insert("name".into(), json!(s.name));
                row.insert("sessionStatus".into(), json!(s.status));
                row.insert("createdAt".into(), json!(s.created_at));
            }
            row.insert("status".into(), json!(status));
            if let Some(mc) = message_count {
                row.insert("messageCount".into(), json!(mc));
            }
            row.insert("subscriptionCount".into(), json!(subscription_count));
            row.insert("queuedEventCount".into(), json!(0));
            row.insert("staleResponding".into(), json!(stale_responding));
            row.insert("deleted".into(), json!(false));
            row.insert("presentInBackend".into(), json!(session.is_some()));
            row.insert(
                "pendingInitialResponse".into(),
                json!(pending_initial_response),
            );
            if let Some(la) = &last_activity {
                row.insert("lastActivity".into(), json!(la));
            }
            agent_rows.push(Value::Object(row));
        }

        // stuck-risk signals.
        let mut stuck_risks: Vec<Value> = Vec::new();
        for row in &agent_rows {
            let aid = row["id"].as_str().unwrap_or_default();
            if row["staleResponding"].as_bool() == Some(true) {
                stuck_risks.push(json!({
                    "type": "stale-responding-status",
                    "severity": "warning",
                    "message": format!("Agent {aid} is marked responding without recent activity"),
                    "agentId": aid,
                }));
            }
            if row["pendingInitialResponse"].as_bool() == Some(true) {
                let present = row["presentInBackend"].as_bool() == Some(true);
                let age = row["lastActivity"].as_str().map(|t| age_ms(now_ms, t));
                let severity = match age {
                    Some(a) if a <= stale_after_ms => "info",
                    _ => "warning",
                };
                let message = if present {
                    format!("Agent {aid} has an initial user message but no assistant response")
                } else {
                    format!("Agent {aid} has an initial user message but no active backend session or assistant response")
                };
                let mut risk = serde_json::Map::new();
                risk.insert("type".into(), json!("initial-prompt-not-running"));
                risk.insert("severity".into(), json!(severity));
                risk.insert("message".into(), json!(message));
                risk.insert("agentId".into(), json!(aid));
                if let Some(a) = age {
                    risk.insert("ageMs".into(), json!(a));
                }
                stuck_risks.push(Value::Object(risk));
            }
        }
        for sub in &subscriptions {
            if sub["orphaned"].as_bool() == Some(true) {
                let sid = sub["id"].as_str().unwrap_or_default();
                let aid = sub["agentId"].as_str().unwrap_or_default();
                stuck_risks.push(json!({
                    "type": "orphaned-subscription",
                    "severity": "warning",
                    "message": format!("Subscription {sid} targets missing or deleted owner {aid}"),
                    "agentId": aid,
                    "subscriptionId": sid,
                }));
            }
        }
        for g in &delegation_groups {
            let complete = g["complete"].as_bool() == Some(true);
            let delivered = g["delivered"].as_bool() == Some(true);
            if !complete && !delivered {
                let gid = g["groupId"].as_str().unwrap_or_default();
                let pending = g["pendingAgentIds"].as_array().map_or(0, Vec::len);
                let severity = if g["subscriptionMissing"].as_bool() == Some(true) {
                    "critical"
                } else {
                    "warning"
                };
                stuck_risks.push(json!({
                    "type": "incomplete-delegation-group",
                    "severity": severity,
                    "message": format!("Delegation group {gid} is waiting for {pending} agent(s)"),
                    "groupId": gid,
                    "count": pending,
                }));
            }
        }

        let mut filters = serde_json::Map::new();
        if let Some(aid) = &agent_filter {
            filters.insert("agentId".into(), json!(aid));
        }
        if let Some(tid) = &task_filter {
            filters.insert("taskNoteId".into(), json!(tid));
        }

        let summary = json!({
            "agents": agent_rows.len(),
            "subscriptions": subscriptions.len(),
            "queuedAgents": 0,
            "queuedEvents": 0,
            "delegationGroups": delegation_groups.len(),
            "deletedAgents": 0,
            "stuckRisks": stuck_risks.len(),
        });

        let delivery_stats = json!({
            "totalDeliveries": 0,
            "successfulDeliveries": 0,
            "failedDeliveries": 0,
            "timeoutDeliveries": 0,
            "droppedEvents": 0,
            "lastDeliveryTime": Value::Null,
            "lastFailureTime": Value::Null,
        });

        let diagnostics = json!({
            "workspaceId": workspace_id,
            "generatedAt": now,
            "filters": Value::Object(filters),
            "summary": summary,
            "agents": agent_rows,
            "subscriptions": subscriptions,
            "queues": [],
            "delegationGroups": delegation_groups,
            "deliveryStats": delivery_stats,
            "deletedAgentReferences": [],
            "recentEvents": [],
            "stuckRisks": stuck_risks,
        });

        // Human-readable `text` (mirrors `GetAgentDiagnosticsTool`).
        let mut lines = vec![
            format!("Agent diagnostics for workspace {}", workspace_id.0),
            format!("Agents: {}", diagnostics["summary"]["agents"]),
            format!("Subscriptions: {}", diagnostics["summary"]["subscriptions"]),
            format!("Queued events: {}", diagnostics["summary"]["queuedEvents"]),
            format!(
                "Delegation groups: {}",
                diagnostics["summary"]["delegationGroups"]
            ),
            format!("Stuck risks: {}", diagnostics["summary"]["stuckRisks"]),
        ];
        if let Some(risks) = diagnostics["stuckRisks"].as_array() {
            if !risks.is_empty() {
                lines.push(String::new());
                lines.push("Stuck-risk signals:".to_string());
                for risk in risks.iter().take(10) {
                    let target = risk["agentId"]
                        .as_str()
                        .or_else(|| risk["groupId"].as_str())
                        .or_else(|| risk["subscriptionId"].as_str())
                        .unwrap_or("workspace");
                    let severity = risk["severity"].as_str().unwrap_or_default();
                    let rtype = risk["type"].as_str().unwrap_or_default();
                    lines.push(format!("- [{severity}] {rtype}: {target}"));
                }
            }
        }

        Ok(json!({
            "ok": true,
            "diagnostics": diagnostics,
            "text": lines.join("\n"),
        }))
    }

    /// `agent.sendToTask`: deliver to the agent assigned to a task note (PROTOCOL §5.5).
    pub(crate) async fn agent_send_to_task_op(
        &self,
        workspace_id: WorkspaceId,
        task_note_id: NoteId,
        message: String,
    ) -> Result<Value> {
        let task = self.get_my_task(workspace_id, task_note_id).await?;
        let Some(agent) = task.assigned_agents.first().cloned() else {
            return Ok(
                json!({ "ok": false, "delivered": false, "error": "No agent assigned to task" }),
            );
        };
        let result = self
            .agent_send_message_op(agent.clone(), message, None)
            .await?;
        Ok(json!({ "ok": true, "agentId": agent, "result": result }))
    }

    /// `agent.wakeOrCreate`: resume the assigned agent or create + assign a new
    /// one, then deliver the context message (PROTOCOL §5.5).
    pub(crate) async fn agent_wake_or_create_op(
        &self,
        workspace_id: WorkspaceId,
        task_note_id: NoteId,
        context_message: String,
        model: Option<String>,
    ) -> Result<Value> {
        let task = self
            .get_my_task(workspace_id.clone(), task_note_id.clone())
            .await?;
        if let Some(agent) = task.assigned_agents.first().cloned() {
            let result = self
                .agent_send_message_op(agent.clone(), context_message, None)
                .await?;
            return Ok(json!({ "ok": true, "agentId": agent, "created": false, "result": result }));
        }
        let created = self
            .agent_create_op(workspace_id.clone(), None, model, None, None)
            .await?;
        let agent_id = created["agent"]["id"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let _ = self
            .assign_agent(workspace_id, task_note_id, agent_id.clone())
            .await;
        let agent = AgentId::from(agent_id.as_str());
        let result = self
            .agent_send_message_op(agent.clone(), context_message, None)
            .await?;
        Ok(json!({ "ok": true, "agentId": agent, "created": true, "result": result }))
    }

    /// Load a session, mapping `NotFound` to `-32603` for the methods whose TS
    /// peers surface a generic failure (rename/setModel/summary).
    async fn load_session_internal(&self, agent_id: &AgentId) -> Result<AgentSession> {
        match self.store.get_agent_session(agent_id).await {
            Ok(s) => Ok(s),
            Err(Error::NotFound(_)) => {
                Err(Error::Internal(format!("Agent \"{agent_id}\" not found")))
            }
            Err(e) => Err(e),
        }
    }

    /// Push a message onto an agent's in-memory queue and return it together with
    /// its 0-based `position` in the queue (the index just appended).
    pub(crate) fn enqueue_message(
        &self,
        agent_id: &AgentId,
        content: String,
        image_blocks: Option<Value>,
    ) -> (QueuedMessage, usize) {
        let queued = QueuedMessage {
            id: new_message_id(),
            content,
            image_blocks,
            queued_at: now_iso(),
        };
        let mut guard = self
            .agent_queues
            .lock()
            .expect("agent queue registry poisoned");
        let queue = guard.entry(agent_id.clone()).or_default();
        queue.push(queued.clone());
        let position = queue.len() - 1;
        (queued, position)
    }

    /// Pop the oldest queued message for an agent (FIFO), if any. Used by the
    /// runtime turn loop to flip a queued message to in-flight when the current
    /// turn ends.
    pub(crate) fn dequeue_message(&self, agent_id: &AgentId) -> Option<QueuedMessage> {
        let mut guard = self
            .agent_queues
            .lock()
            .expect("agent queue registry poisoned");
        let queue = guard.get_mut(agent_id)?;
        if queue.is_empty() {
            None
        } else {
            Some(queue.remove(0))
        }
    }

    /// Re-insert a message at the front of an agent's queue (used when a
    /// concurrent turn won the in-flight slot during a drain race).
    pub(crate) fn requeue_front(&self, agent_id: &AgentId, message: QueuedMessage) {
        self.agent_queues
            .lock()
            .expect("agent queue registry poisoned")
            .entry(agent_id.clone())
            .or_default()
            .insert(0, message);
    }

    /// Drop all queued messages for an agent (used by `agent.forceMessage`,
    /// which supersedes the queue with the forced message).
    pub(crate) fn clear_queue(&self, agent_id: &AgentId) {
        self.agent_queues
            .lock()
            .expect("agent queue registry poisoned")
            .remove(agent_id);
    }
}

/// Join all text blocks of the most-recent assistant message (the summary's
/// `lastResponse`).
fn last_assistant_text(messages: &[AgentMessage]) -> Option<String> {
    for msg in messages.iter().rev() {
        if msg.role != "assistant" {
            continue;
        }
        let joined = text_blocks(&msg.content).join(" ");
        let trimmed = joined.trim();
        return if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        };
    }
    None
}

/// Map a persisted [`AgentStatus`] to the TS runtime status word used in the
/// `agent.getSubscriptions` `agentStatuses` map. Best-effort: statuses without a
/// runtime equivalent (e.g. `deleted`) are omitted so the caller drops the key.
/// Parse an RFC-3339 timestamp into epoch milliseconds, or `0` when malformed.
fn iso_ms(ts: &str) -> i64 {
    parse_iso(ts)
        .map(|dt| (dt.unix_timestamp_nanos() / 1_000_000) as i64)
        .unwrap_or(0)
}

/// Non-negative age in milliseconds of `ts` relative to `now_ms`.
fn age_ms(now_ms: i64, ts: &str) -> i64 {
    (now_ms - iso_ms(ts)).max(0)
}

/// Whether any message in the transcript was authored by the assistant.
fn has_assistant_message(messages: &[AgentMessage]) -> bool {
    messages.iter().any(|m| m.role == "assistant")
}

fn agent_status_wire(status: AgentStatus) -> Option<&'static str> {
    match status {
        AgentStatus::Pending | AgentStatus::Waiting => Some("waiting"),
        AgentStatus::Active | AgentStatus::Processing => Some("responding"),
        AgentStatus::RuntimeIdle | AgentStatus::Idle => Some("idle"),
        AgentStatus::Completed => Some("completed"),
        AgentStatus::Error => Some("failed"),
        AgentStatus::Deleted => None,
    }
}

/// Best-effort human description mirroring TS `describeAgentSubscription`:
/// `"<parent>: <n event types>, from <child>[, delegation group <id> (await all,
/// k expected)][, one-shot]"`. Exact wording is not asserted.
fn describe_subscription(
    watch: &CompletionWatch,
    event_types: &[&str],
    delegation_group: Option<&Value>,
) -> String {
    let mut desc = format!(
        "{}: {} event types, from {}",
        watch.parent_agent_name,
        event_types.len(),
        watch.child_agent_id.0
    );
    if let Some(group) = delegation_group {
        let group_id = group["groupId"].as_str().unwrap_or_default();
        let expected = group["expectedAgentIds"].as_array().map_or(0, Vec::len);
        desc.push_str(&format!(
            ", delegation group {group_id} (await all, {expected} expected)"
        ));
    }
    if watch.one_shot {
        desc.push_str(", one-shot");
    }
    desc
}

/// Count `tool_use` content blocks by tool name across all messages.
fn tool_call_counts(messages: &[AgentMessage]) -> Value {
    let mut counts: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
    for msg in messages {
        let Some(blocks) = msg.content.as_array() else {
            continue;
        };
        for block in blocks {
            if block.get("type").and_then(Value::as_str) != Some("tool_use") {
                continue;
            }
            let name = block
                .get("name")
                .and_then(Value::as_str)
                .unwrap_or("unknown")
                .to_string();
            *counts.entry(name).or_insert(0) += 1;
        }
    }
    json!(counts)
}
