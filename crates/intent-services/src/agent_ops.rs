//! `agent.*` RPC helpers (PROTOCOL §5.5).
//!
//! Pure projections + the model-catalog helpers that back the `agent.*`
//! `WorkspaceApi` methods (the trait bodies live in `lib.rs`). The
//! [`AgentLite`] derivation (`lastAgentResponse`/`digest`) ports the TS
//! `agent.list`/`agent.get` post-processing; [`static_models`] /
//! [`parse_model_list_output`] port the `agent.getModels` static-tier fallback
//! and auggie CLI parser respectively.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use intent_core::events::{
    AGENT_DELETED, AGENT_FAILED, AGENT_IDLE, AGENT_MESSAGE, AGENT_QUEUE_UPDATED, AGENT_UPDATED,
};
use intent_core::{
    now_iso, parse_iso, ActorType, AgentCreateExtra, AgentId, AgentLite, AgentMessage,
    AgentSession, AgentStatus, AgentWakeCreateOptions, AgentWakeOrCreateInput, Error, Event,
    EventActor, NoteId, Result, SessionStats, WorkspaceApi, WorkspaceId, MAX_DELEGATION_DEPTH,
};
/// Default `agent.diagnostics` stale-responding threshold (10 minutes), matching
/// the TS `DEFAULT_STALE_RESPONDING_AFTER_MS`.
const DEFAULT_STALE_RESPONDING_AFTER_MS: i64 = 10 * 60 * 1000;

/// Maximum length for caller-supplied message IDs to prevent unbounded storage
/// and DoS via oversized persisted IDs.
const MAX_MESSAGE_ID_LEN: usize = 256;

use crate::agent_subscriptions::CompletionWatch;

/// `waitMode` value that defers the completion watch into an `after_all`
/// delegation group (AS-4) rather than registering a standalone oneShot here.
const WAIT_MODE_AFTER_ALL: &str = "after_all";

/// Marker `metadata.source` written on new agents created by
/// `agent.wakeOrCreate` (C1d-10a). Mirrors the FE tool's own tag so downstream
/// consumers (activity feeds, filters) can trace provenance.
const WAKE_OR_CREATE_SOURCE: &str = "wake_or_create_task_agent";

/// Auto-cleanup window for the non-oneShot watch registered when
/// `agent.wakeOrCreate` queues the context message to an active agent
/// (SUB-1, mirrors the TS 5-minute `setTimeout` unsubscribe leak guard).
const QUEUED_WATCH_CLEANUP: std::time::Duration = std::time::Duration::from_secs(5 * 60);
use intent_providers::models::PROVIDER_MODEL_TIERS;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::Services;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod tests_stab28;

#[cfg(test)]
mod tests_stab115;

/// Resolve the default model from settings when no explicit model is supplied
/// at agent creation time. Precedence chain (documented in the task):
/// 1. `model.workspaceOverrides[workspaceId]`
/// 2. for background/delegated sessions: `backgroundAgents.typeOverrides[agentType]`, then `backgroundAgents.defaultModel`
/// 3. `model.providerDefaults[resolved provider]`
/// 4. `model.default`
/// 5. None → CLI default (current behavior, last resort)
///
/// The resolved model is persisted to `session.model` at creation time, pinning
/// it for the agent's lifetime. Later settings changes never affect existing
/// sessions; only new agents pick up the new default.
async fn resolve_default_model_from_settings(
    services: &Services,
    workspace_id: &WorkspaceId,
    is_background: bool,
    agent_type: Option<&str>,
    provider: Option<&str>,
) -> Option<String> {
    // 1. Check workspace-specific override
    if let Ok(Some(raw)) = services.store.get_setting("model.workspaceOverrides").await {
        if let Ok(Value::Object(overrides)) = serde_json::from_str::<Value>(&raw) {
            if let Some(model) = overrides.get(workspace_id.as_str()).and_then(Value::as_str) {
                if !model.is_empty() {
                    return Some(model.to_string());
                }
            }
        }
    }

    // 2. For background/delegated agents, check type-specific override, then default
    if is_background {
        // 2a. Check agent type override
        if let Some(typ) = agent_type {
            if let Ok(Some(raw)) = services
                .store
                .get_setting("backgroundAgents.typeOverrides")
                .await
            {
                if let Ok(Value::Object(overrides)) = serde_json::from_str::<Value>(&raw) {
                    if let Some(model) = overrides.get(typ).and_then(Value::as_str) {
                        if !model.is_empty() {
                            return Some(model.to_string());
                        }
                    }
                }
            }
        }

        // 2b. Check background agents default
        if let Ok(Some(raw)) = services
            .store
            .get_setting("backgroundAgents.defaultModel")
            .await
        {
            if let Ok(Value::String(model)) = serde_json::from_str::<Value>(&raw) {
                if !model.is_empty() {
                    return Some(model);
                }
            }
        }
    }

    // 3. Check provider defaults
    let provider_key = provider.unwrap_or_else(|| intent_providers::default_provider_id());
    if let Ok(Some(raw)) = services.store.get_setting("model.providerDefaults").await {
        if let Ok(Value::Object(defaults)) = serde_json::from_str::<Value>(&raw) {
            if let Some(model) = defaults.get(provider_key).and_then(Value::as_str) {
                if !model.is_empty() {
                    return Some(model.to_string());
                }
            }
        }
    }

    // 4. Check global default
    if let Ok(Some(raw)) = services.store.get_setting("model.default").await {
        if let Ok(Value::String(model)) = serde_json::from_str::<Value>(&raw) {
            if !model.is_empty() {
                return Some(model);
            }
        }
    }

    // 5. None → CLI default (session.model stays None)
    None
}

/// One pending message in an agent's in-memory send queue (`agent.getQueue`).
///
/// `editing` marks the entry as "under edit" — excluded from the **ready-to-send**
/// queue so the drain skips it (PROTOCOL §5.5/§6.5). The agent may go idle only
/// when every remaining queued entry has `editing == true`; setting `editing`
/// back to `false` re-includes the message and self-drains.
#[derive(Debug, Clone)]
pub(crate) struct QueuedMessage {
    pub id: String,
    pub content: String,
    pub image_blocks: Option<Value>,
    pub file_blocks: Option<Value>,
    pub queued_at: String,
    pub editing: bool,
    /// `true` when the user-message row already reached the transcript before
    /// this entry was (re)queued — set by both the terminal-failure requeue
    /// (STAB-112) and the interrupt zero-output requeue (STAB-114). Drain paths
    /// skip `persist_user` for such entries so a retry does not duplicate the
    /// user message in chat history.
    pub persisted: bool,
    /// `true` when this is a terminal-failure requeue (STAB-112); `to_value`
    /// emits `requeuedAfterFailure: true` on the wire. Interrupt requeues
    /// (STAB-114) leave this `false` so the FE does not show "failed — will retry".
    pub requeued_after_failure: bool,
}

impl QueuedMessage {
    /// The camelCase wire shape for `agent.getQueue` / queue results, matching the
    /// TS `QueuedMessage` and the iOS decoder (`{id, content, queuedAt, position,
    /// imageBlocks?, fileBlocks?, editing?, requeuedAfterFailure?}`). `position` is
    /// the entry's 0-based index in the queue (0 = next to be sent) and is supplied
    /// by the caller since it is positional. `editing` is only present when `true`
    /// (a client that hasn't migrated still sees the legacy shape unchanged).
    /// `requeuedAfterFailure` is only present when `true` (STAB-112: backward-compatible
    /// marker for terminal-failure requeues).
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
        if let Some(blocks) = &self.file_blocks {
            v["fileBlocks"] = blocks.clone();
        }
        if self.editing {
            v["editing"] = Value::Bool(true);
        }
        if self.requeued_after_failure {
            v["requeuedAfterFailure"] = Value::Bool(true);
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

/// How long a successful `models.list` CLI fetch stays fresh (PROTOCOL §5.30),
/// porting the reference app's 5-minute provider-model cache.
pub(crate) const MODELS_CACHE_TTL: std::time::Duration = std::time::Duration::from_secs(5 * 60);

/// The shared `models.list` success-cache slot on [`Services`]: the fetch
/// instant plus the finalized rows (PROTOCOL §5.30).
pub(crate) type ModelsCache = Arc<Mutex<Option<(std::time::Instant, Vec<Value>)>>>;

/// Parse `auggie model list --json` output into rich wire `ModelInfo` rows
/// (PROTOCOL §5.30), porting the TS `parseModelListJson`: expects
/// `{ models: [...] }`, maps `id` ← `shortName` and `name` ← `displayName`,
/// and skips rows missing either string. Optional picker metadata
/// (`description`, `modelGroupPriority`, `costTier`, `badges`, `effortLevels`,
/// `isDefault`, `priority`) is copied only when present/non-empty. The
/// transient `isLegacyModel` flag is kept so [`finalize_model_rows`] can
/// filter it, then stripped from the wire. Returns `None` when the payload is
/// not the expected JSON shape, so the caller falls back to plain text.
pub(crate) fn parse_model_list_json(stdout: &str) -> Option<Vec<Value>> {
    let parsed: Value = serde_json::from_str(stdout.trim()).ok()?;
    let models = parsed.get("models")?.as_array()?;
    let mut out = Vec::new();
    for m in models {
        let (Some(short), Some(display)) = (
            m.get("shortName").and_then(Value::as_str),
            m.get("displayName").and_then(Value::as_str),
        ) else {
            continue;
        };
        let mut row = json!({ "id": short, "name": display, "provider": "auggie" });
        if let Some(d) = m.get("description").and_then(Value::as_str) {
            if !d.is_empty() {
                row["description"] = Value::String(d.to_string());
            }
        }
        for key in ["modelGroupPriority", "costTier", "priority"] {
            if let Some(v) = m.get(key).filter(|v| v.is_number()) {
                row[key] = v.clone();
            }
        }
        for key in ["badges", "effortLevels"] {
            if let Some(v) = m.get(key).and_then(Value::as_array) {
                if !v.is_empty() {
                    row[key] = Value::Array(v.clone());
                }
            }
        }
        if m.get("isDefault").and_then(Value::as_bool) == Some(true) {
            row["isDefault"] = Value::Bool(true);
        }
        if m.get("isLegacyModel").and_then(Value::as_bool) == Some(true) {
            row["isLegacyModel"] = Value::Bool(true);
        }
        out.push(row);
    }
    Some(out)
}

/// Post-process parsed `models.list` rows (PROTOCOL §5.30), porting the
/// reference `fetchAuggieModels` tail: drop rows flagged `isLegacyModel`
/// (stripping the flag from survivors) and sort by `modelGroupPriority`, then
/// `priority`, then `name` — missing priorities sort last (`999`).
pub(crate) fn finalize_model_rows(rows: Vec<Value>) -> Vec<Value> {
    fn priority(row: &Value, key: &str) -> f64 {
        row.get(key).and_then(Value::as_f64).unwrap_or(999.0)
    }
    fn name(row: &Value) -> &str {
        row.get("name").and_then(Value::as_str).unwrap_or("")
    }
    let mut kept: Vec<Value> = rows
        .into_iter()
        .filter(|r| r.get("isLegacyModel").and_then(Value::as_bool) != Some(true))
        .map(|mut r| {
            if let Some(obj) = r.as_object_mut() {
                obj.remove("isLegacyModel");
            }
            r
        })
        .collect();
    kept.sort_by(|a, b| {
        priority(a, "modelGroupPriority")
            .total_cmp(&priority(b, "modelGroupPriority"))
            .then_with(|| priority(a, "priority").total_cmp(&priority(b, "priority")))
            .then_with(|| name(a).cmp(name(b)))
    });
    kept
}

/// Best-effort `models.list` dynamic fetch (PROTOCOL §5.30), porting the
/// reference `fetchAuggieModels`: try `auggie model list --json` for the rich
/// rows, fall back to the plain-text parser ([`parse_model_list_output`]),
/// then filter legacy models and sort ([`finalize_model_rows`]). Returns
/// `None` when the CLI is unavailable or yields nothing parseable, so the
/// caller can fall back to [`static_models`].
pub(crate) async fn fetch_auggie_models_rich() -> Option<Vec<Value>> {
    let mut rows: Option<Vec<Value>> = None;
    if let Ok(output) = tokio::process::Command::new("auggie")
        .args(["model", "list", "--json"])
        .output()
        .await
    {
        rows = parse_model_list_json(&String::from_utf8_lossy(&output.stdout))
            .filter(|r| !r.is_empty());
    }
    if rows.is_none() {
        let output = tokio::process::Command::new("auggie")
            .args(["model", "list"])
            .output()
            .await
            .ok()?;
        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut parsed = parse_model_list_output(&stdout);
        if parsed.is_empty() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            parsed = parse_model_list_output(&stderr);
        }
        if !parsed.is_empty() {
            rows = Some(
                parsed
                    .into_iter()
                    .map(|(value, label, description)| {
                        let mut m = json!({ "id": value, "name": label, "provider": "auggie" });
                        if let Some(d) = description {
                            m["description"] = Value::String(d);
                        }
                        m
                    })
                    .collect(),
            );
        }
    }
    let finalized = finalize_model_rows(rows?);
    if finalized.is_empty() {
        None
    } else {
        Some(finalized)
    }
}

/// Parse the JSON emitted by `auggie session stats <sessionId> --json` into a
/// [`SessionStats`] (PROTOCOL §5.24). Tolerant of the CLI's richer shape:
/// `creditsUsed` is nullable (absent/non-numeric → `None`, i.e. not yet
/// computed), and the message/tool counts default to 0 when absent. Returns
/// `None` when the payload is not a JSON object (e.g. the plain-text line the
/// CLI prints when the command is unavailable), so the caller degrades
/// gracefully rather than failing.
pub(crate) fn parse_session_stats_output(stdout: &str) -> Option<SessionStats> {
    let value: Value = serde_json::from_str(stdout.trim()).ok()?;
    let obj = value.as_object()?;
    Some(SessionStats {
        credits_used: obj.get("creditsUsed").and_then(Value::as_f64),
        message_count: obj.get("messageCount").and_then(Value::as_u64).unwrap_or(0),
        tool_count: obj.get("toolCount").and_then(Value::as_u64).unwrap_or(0),
    })
}

/// Best-effort `agent.getSessionStats` CLI refresh: run
/// `auggie session stats <sessionId> --json` and parse stdout (then stderr).
/// Returns `None` when the CLI is unavailable or emits nothing parseable, so the
/// caller can fall back to transcript-derived counts with `creditsUsed = null`.
pub(crate) async fn fetch_session_stats(session_id: &AgentId) -> Option<SessionStats> {
    let output = tokio::process::Command::new("auggie")
        .args(["session", "stats", session_id.0.as_str(), "--json"])
        .output()
        .await
        .ok()?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    parse_session_stats_output(&stdout).or_else(|| {
        let stderr = String::from_utf8_lossy(&output.stderr);
        parse_session_stats_output(&stderr)
    })
}

/// Transcript-derived `(messageCount, toolCount)` fallback used when the auggie
/// CLI is unavailable: every logged message counts, and every `tool_use` content
/// block counts as one tool call.
fn transcript_counts(messages: &[AgentMessage]) -> (u64, u64) {
    let mut tool_count = 0u64;
    for msg in messages {
        if let Some(blocks) = msg.content.as_array() {
            for block in blocks {
                if block.get("type").and_then(Value::as_str) == Some("tool_use") {
                    tool_count += 1;
                }
            }
        }
    }
    (messages.len() as u64, tool_count)
}

/// Mint a stable user-message id (`user-msg-{uuid}`), mirroring the TS
/// `agent.sendMessage` `messageId` default.
pub(crate) fn new_message_id() -> String {
    format!("user-msg-{}", Uuid::new_v4())
}

/// Whether a wire `priority` requests interrupt delivery (PROTOCOL §5.5):
/// `"interrupt"` preempts the in-flight turn keep-alive; anything else (or
/// absent) is normal queue-vs-stream delivery.
pub(crate) fn is_interrupt_priority(priority: Option<&str>) -> bool {
    priority == Some("interrupt")
}

/// Validate a client-supplied `agent.create` `agentId` (PROTOCOL §5.5): the id
/// must be the exact `agent-{uuid}` form (`agent-` prefix + a parsable UUID
/// tail), matching the form the daemon mints. Anything else surfaces as
/// `-32602` so a stray/hand-typed id cannot collide with future runtime ids.
pub(crate) fn validate_client_agent_id(id: &str) -> Result<()> {
    let Some(tail) = id.strip_prefix("agent-") else {
        return Err(Error::InvalidParams(format!(
            "agentId must be of the form 'agent-{{uuid}}' (got {id:?})"
        )));
    };
    Uuid::parse_str(tail).map_err(|_| {
        Error::InvalidParams(format!(
            "agentId must be of the form 'agent-{{uuid}}' (got {id:?})"
        ))
    })?;
    Ok(())
}

/// A single user text content block (the persisted/queued message shape).
fn user_content_blocks(content: &str) -> Value {
    json!([{ "type": "text", "text": content }])
}

/// Build the persisted `agent_session.metadata` blob for the create branch of
/// `agent.wakeOrCreate` (C1d-10a). Starts from any caller-supplied
/// `create.metadata` object (or `{}`), overlays the FE provenance fields the
/// tool guarantees (`createdByAgentId`, `delegationDepth`, `taskNoteId`,
/// `isBackground`, `source`), and folds `create.contextReferences` /
/// `create.agentType` in when present so a child's `agent.wakeOrCreate` can
/// read them back without a follow-up round-trip. Caller-supplied fields for
/// `taskNoteId`/`source`/`delegationDepth`/`createdByAgentId` are honored
/// verbatim only when the wake input did not supply the corresponding hint.
fn build_create_metadata(
    create_opts: &AgentWakeCreateOptions,
    input: &AgentWakeOrCreateInput,
    task_note_id: &NoteId,
    parent_depth: Option<i64>,
    agent_type: Option<String>,
) -> Option<Value> {
    let mut obj = create_opts
        .metadata
        .clone()
        .and_then(|v| v.as_object().cloned())
        .unwrap_or_default();
    if !obj.contains_key("taskNoteId") {
        obj.insert("taskNoteId".to_string(), json!(task_note_id.0));
    }
    if !obj.contains_key("source") {
        obj.insert("source".to_string(), json!(WAKE_OR_CREATE_SOURCE));
    }
    if !obj.contains_key("isBackground") {
        obj.insert("isBackground".to_string(), json!(true));
    }
    let child_depth = parent_depth.map(|d| d + 1).unwrap_or(0);
    obj.entry("delegationDepth".to_string())
        .or_insert(json!(child_depth));
    if let Some(caller) = input.caller_agent_id.as_ref() {
        obj.entry("createdByAgentId".to_string())
            .or_insert(json!(caller.0));
    }
    if let Some(refs) = create_opts.context_references.as_ref() {
        obj.entry("contextReferences".to_string())
            .or_insert(refs.clone());
    }
    if let Some(agent_type) = agent_type {
        obj.entry("agentType".to_string())
            .or_insert(json!(agent_type));
    }
    if let Some(skip) = create_opts.skip_auto_commit {
        obj.entry("skipAutoCommit".to_string())
            .or_insert(json!(skip));
    }
    if obj.is_empty() {
        None
    } else {
        Some(Value::Object(obj))
    }
}

/// Build the `agent.wakeOrCreate` response envelope (C1d-10a). `action` is one
/// of `message_queued_to_active_agent` / `woke_existing` / `created_new` — the
/// 3-way discriminator the FE tool exposes. `cleanedUpAgentIds` is omitted
/// when empty so pre-widening callers that only inspect `ok`/`agentId`/
/// `created`/`result` stay wire-compatible.
fn build_wake_response(
    agent_id: AgentId,
    agent_name: String,
    created: bool,
    action: &str,
    task_title: String,
    result: Value,
    cleaned_up: Vec<AgentId>,
) -> Value {
    let mut out = json!({
        "ok": true,
        "agentId": agent_id,
        "agentName": agent_name,
        "created": created,
        "action": action,
        "taskTitle": task_title,
        "result": result,
    });
    if !cleaned_up.is_empty() {
        out["cleanedUpAgentIds"] = json!(cleaned_up);
    }
    out
}

/// TS `isDelegatedBackgroundTaskSession`: a background session that was
/// delegated by another agent onto a task note. Both the delegator id and the
/// task linkage are read from the persisted `metadata` blob (the shape the
/// delegate/create writers populate), matching the reference field-for-field.
fn is_delegated_background_task_session(session: &AgentSession) -> bool {
    let md = session.metadata.as_ref();
    let md_is_background = md
        .and_then(|m| m.get("isBackground"))
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let md_has_str = |key: &str| {
        md.and_then(|m| m.get(key))
            .and_then(Value::as_str)
            .is_some()
    };
    (session.is_background || md_is_background)
        && md_has_str("createdByAgentId")
        && md_has_str("taskNoteId")
}

/// Project an [`AgentSession`] (with its loaded messages) into [`AgentLite`].
fn project_lite(session: AgentSession) -> AgentLite {
    let (last_response, digest) = last_response_and_digest(&session.messages);
    let last_user = last_user_message(&session.messages);
    let count = session.messages.len() as u64;
    AgentLite::from_session(session, count, last_response, last_user, digest)
}

/// Whether `blocks` contains a `tool_use` block with no matching `tool_result`
/// (matched by `tool_use_id == toolCallId`). The daemon-side port of the FE
/// `hasUnresolvedToolUse` content-block branch: a tool call that has been
/// emitted but whose result block has not yet been appended is "unresolved"
/// (the agent is blocked awaiting the tool).
fn has_unresolved_tool_use(blocks: &[Value]) -> bool {
    blocks.iter().any(|block| {
        if block.get("type").and_then(Value::as_str) != Some("tool_use") {
            return false;
        }
        let Some(id) = block.get("toolCallId").and_then(Value::as_str) else {
            return false;
        };
        !blocks.iter().any(|candidate| {
            candidate.get("type").and_then(Value::as_str) == Some("tool_result")
                && candidate.get("tool_use_id").and_then(Value::as_str) == Some(id)
        })
    })
}

/// STAB-122: strip anonymous `tool_use` blocks (`name` missing/empty) and the
/// `tool_result` blocks paired to them (matched by `tool_use_id == toolCallId`)
/// from a message before serving it. Pre-fix daemons persisted this malformed
/// pair when an interrupt landed mid-tool-call; the FE conversation load chokes
/// on the empty name. Messages without such blocks pass through unchanged.
fn strip_anonymous_tool_blocks(mut message: AgentMessage) -> AgentMessage {
    fn is_anonymous_tool_use(b: &Value) -> bool {
        b.get("type").and_then(Value::as_str) == Some("tool_use")
            && b.get("name")
                .and_then(Value::as_str)
                .map(|n| n.trim().is_empty())
                .unwrap_or(true)
    }
    let Some(blocks) = message.content.as_array() else {
        return message;
    };
    if !blocks.iter().any(is_anonymous_tool_use) {
        return message;
    }
    let anonymous_ids: HashSet<&str> = blocks
        .iter()
        .filter(|b| is_anonymous_tool_use(b))
        .filter_map(|b| b.get("toolCallId").and_then(Value::as_str))
        .collect();
    let kept: Vec<Value> = blocks
        .iter()
        .filter(|b| match b.get("type").and_then(Value::as_str) {
            Some("tool_use") => !is_anonymous_tool_use(b),
            Some("tool_result") => b
                .get("tool_use_id")
                .and_then(Value::as_str)
                .map(|id| !anonymous_ids.contains(id))
                .unwrap_or(true),
            _ => true,
        })
        .cloned()
        .collect();
    message.content = Value::Array(kept);
    message
}

impl Services {
    /// `agent.list` (PROTOCOL §5.5).
    pub(crate) async fn agent_list_op(&self, workspace_id: WorkspaceId) -> Result<Vec<AgentLite>> {
        let sessions = self.store.list_agent_sessions(&workspace_id).await?;
        Ok(sessions
            .into_iter()
            .map(|s| self.project_lite_with_flags(s))
            .collect())
    }

    /// `agent.get` (PROTOCOL §5.5). `NotFound` is surfaced to the router which
    /// maps it to `-32602 "Agent not found"`. When `workspace_id` is supplied
    /// the caller's workspace must match the session's; a mismatch surfaces as
    /// `NotFound` (defense-in-depth against bare-id probes across workspaces).
    pub(crate) async fn agent_get_op(
        &self,
        agent_id: AgentId,
        workspace_id: Option<WorkspaceId>,
    ) -> Result<AgentLite> {
        let session = self.store.get_agent_session(&agent_id).await?;
        if let Some(ws) = workspace_id.as_ref() {
            if session.workspace_id != *ws {
                return Err(Error::NotFound(format!("agent session {agent_id}")));
            }
        }
        Ok(self.project_lite_with_flags(session))
    }

    /// Project an [`AgentSession`] into [`AgentLite`] and overlay the daemon-owned
    /// runtime activity flags (PROTOCOL §5.5/§7.1): `isResponding`,
    /// `isWaitingOnTool`, `isWaitingForOtherAgents`, `waitingForAgentIds`. See
    /// [`agent_activity_flags_for`].
    pub(crate) fn project_lite_with_flags(&self, session: AgentSession) -> AgentLite {
        let (is_responding, is_waiting_on_tool, is_waiting_for_other_agents, waiting_for_agent_ids) =
            self.agent_activity_flags_for(&session);
        let mut lite = project_lite(session);
        lite.is_responding = is_responding;
        lite.is_waiting_on_tool = is_waiting_on_tool;
        lite.is_waiting_for_other_agents = is_waiting_for_other_agents;
        lite.waiting_for_agent_ids = waiting_for_agent_ids;
        lite
    }

    /// Compute the daemon-owned runtime activity flags for `session` — the port
    /// of the FE agent-state selectors so clients render verbatim (PROTOCOL
    /// §5.5/§7.1). Returns
    /// `(isResponding, isWaitingOnTool, isWaitingForOtherAgents, waitingForAgentIds)`:
    ///
    /// - `isResponding` — a worker is draining an in-flight turn for this agent
    ///   ([`agent_is_busy`], the authoritative "active worker" signal; mirrors the
    ///   FE `selectAgentIsResponding`). Builds on the existing busy/live-turn state
    ///   rather than adding a parallel notion of "busy".
    /// - `isWaitingOnTool` — that in-flight turn has an unresolved `tool_use` block
    ///   (a tool call awaiting its result; the port of FE `hasUnresolvedToolUse`).
    /// - `isWaitingForOtherAgents` — the agent parents one or more pending
    ///   completion watches (the port of FE `isAgentWaitingForOtherAgents`).
    /// - `waitingForAgentIds` — the distinct `child_agent_id`s of those pending
    ///   watches, in registration order. Always returned (defaults to empty);
    ///   non-empty iff `isWaitingForOtherAgents` is `true`, so clients can render
    ///   the waiting-on names verbatim without consulting `metadata`.
    ///
    /// Terminal agents (completed/error/deleted) report all flags `false` and an
    /// empty `waitingForAgentIds`, mirroring the FE selectors' terminal-status
    /// short-circuit.
    pub(crate) fn agent_activity_flags_for(
        &self,
        session: &AgentSession,
    ) -> (bool, bool, bool, Vec<AgentId>) {
        let terminal = matches!(
            session.status,
            AgentStatus::Completed | AgentStatus::Error | AgentStatus::Deleted
        );
        if terminal {
            return (false, false, false, Vec::new());
        }
        let is_responding = self.agent_is_busy(session.id.clone());
        let is_waiting_on_tool = is_responding && self.live_turn_has_unresolved_tool(&session.id);
        let watches = self.list_watches_for_parent(&session.workspace_id, &session.id);
        // Distinct child ids in registration order — a parent can register
        // multiple watches against the same child (e.g. successive `immediate`
        // delegates), but the FE only wants each waiting-on agent once.
        let mut waiting_for_agent_ids: Vec<AgentId> = Vec::with_capacity(watches.len());
        for w in &watches {
            if !waiting_for_agent_ids.contains(&w.child_agent_id) {
                waiting_for_agent_ids.push(w.child_agent_id.clone());
            }
        }
        let is_waiting_for_other_agents = !waiting_for_agent_ids.is_empty();
        (
            is_responding,
            is_waiting_on_tool,
            is_waiting_for_other_agents,
            waiting_for_agent_ids,
        )
    }

    /// Whether the agent's in-flight live turn (if any) is blocked on an
    /// unresolved tool call. `false` when no turn is streaming.
    fn live_turn_has_unresolved_tool(&self, agent_id: &AgentId) -> bool {
        self.live_turn(agent_id)
            .map(|live| has_unresolved_tool_use(&live.blocks))
            .unwrap_or(false)
    }

    /// `agent.getConversation` (PROTOCOL §5.5). Paginated per the TA-2 contract:
    /// the limit clamps to `[1,200]` (default 50) and an opaque `nextToken`
    /// walks backward to older pages. The `messages` array stays oldest→newest
    /// within a page (wire parity with the TS handler); `nextToken` is additive
    /// and is `null` once the oldest message has been returned.
    ///
    /// STAB-122 loading tolerance: rows persisted by pre-fix daemons can carry
    /// an anonymous `tool_use` block (`name: ""`, the fabricated echo of a
    /// tool call aborted by an interrupt) that breaks FE conversation loading.
    /// The served page strips those blocks (and their paired `tool_result`s)
    /// non-destructively — the stored rows are untouched, so the read is
    /// idempotent and covers old rows and restored backups alike.
    pub(crate) async fn agent_get_conversation_op(
        &self,
        agent_id: AgentId,
        limit: Option<i64>,
        workspace_id: Option<WorkspaceId>,
        page_token: Option<String>,
    ) -> Result<Value> {
        let session = self.store.get_agent_session(&agent_id).await?;
        if let Some(ws) = workspace_id.as_ref() {
            if session.workspace_id != *ws {
                return Err(Error::NotFound(format!("agent session {agent_id}")));
            }
        }
        let messages = session.messages;
        let total = messages.len();
        let win = crate::pagination::page_window(total, limit, page_token.as_deref());
        let page: Vec<AgentMessage> = messages[win.start..win.end]
            .iter()
            .cloned()
            .map(strip_anonymous_tool_blocks)
            .collect();
        Ok(json!({
            "agentId": agent_id,
            "messages": page,
            "truncated": win.next_token.is_some(),
            "totalMessages": total,
            "nextToken": win.next_token,
        }))
    }

    /// Publish an `agent:*` session-mutation event (P3-1.2b): every persisted
    /// session mutation emits an invalidation event so subscribed clients
    /// re-read the projection instead of relying on a local cache.
    pub(crate) async fn publish_agent_mutation_event(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        event_type: &str,
        data: Value,
    ) {
        crate::publish_event(
            &self.event_bus,
            intent_store::NewEvent {
                workspace_id: workspace_id.clone(),
                timestamp: now_iso(),
                event_type: event_type.to_string(),
                actor: crate::system_actor(),
                session_id: Some(agent_id.0.clone()),
                correlation_id: None,
                parent_event_id: None,
                metadata: None,
                data,
            },
        )
        .await;
    }

    /// Publish `agent:subscriptions-changed` for `parent_agent_id`, carrying the
    /// refreshed waiting flags derived from its live completion-watch set
    /// (`isWaitingForOtherAgents` / `waitingForAgentIds`, the same projection
    /// `agent.get` serves) so clients converge on watch-set changes without
    /// polling (PROTOCOL §6.5). Emitted when watches are added (delegate /
    /// watchCompletion) and when wake delivery removes them (oneShot /
    /// delegation-group clear).
    pub(crate) async fn publish_subscriptions_changed(
        &self,
        workspace_id: &WorkspaceId,
        parent_agent_id: &AgentId,
    ) {
        let watches = self.list_watches_for_parent(workspace_id, parent_agent_id);
        let mut waiting: Vec<AgentId> = Vec::with_capacity(watches.len());
        for w in &watches {
            if !waiting.contains(&w.child_agent_id) {
                waiting.push(w.child_agent_id.clone());
            }
        }
        self.publish_agent_mutation_event(
            workspace_id,
            parent_agent_id,
            intent_core::events::AGENT_SUBSCRIPTIONS_CHANGED,
            json!({
                "agentId": parent_agent_id.0,
                "isWaitingForOtherAgents": !waiting.is_empty(),
                "waitingForAgentIds": waiting,
            }),
        )
        .await;
    }

    /// `agent.create`: persist a new session; the process spawns lazily on first
    /// turn (PROTOCOL §5.5). `task_note_id`/`skip_auto_commit` are set by
    /// `agent.delegate` so the auto-commit-on-idle subscriber (LNI-1) can
    /// resolve the `Linked-Note-Id:` trailer and honor the opt-out.
    ///
    /// `requested_agent_id` is honored verbatim when it is a well-formed
    /// `agent-{uuid}`, so the FE can create + address the session under an id
    /// it already minted (fixes the UI create→sendMessage "not found: agent
    /// session" race). Malformed values surface as `-32602`; when `None` a
    /// fresh id is generated (existing behavior).
    ///
    /// `extra` carries the widened FE-facing spawn hints. `provider` lands on
    /// the persisted [`AgentSession`]; `metadata` is harvested for the
    /// persistence-gap fields (`delegationDepth`, `initialMessage`,
    /// `contextReferences`, `imageBlocks`; P3-1.2b — plus `isBackground`,
    /// G-A1/P3-1.2c) with the top-level `contextReferences`/`imageBlocks`/
    /// `isBackground` params winning over the `metadata` fallback.
    /// `agentType`/`workspacePath`/`workspaceContext` remain
    /// accepted-but-unpersisted (P2-12a audit).
    ///
    /// Emits `agent:created` after the insert.
    ///
    /// Returns `{ agent: <AgentLite> }` — the full projection so the FE can
    /// upsert the created session without a follow-up `agent.get` round-trip.
    /// This is a superset of the earlier `{ id, name }` shape, so existing
    /// callers that only read `agent.id` / `agent.name` stay green.
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn agent_create_op(
        &self,
        workspace_id: WorkspaceId,
        name: Option<String>,
        model: Option<String>,
        specialist: Option<String>,
        parent_agent_id: Option<AgentId>,
        task_note_id: Option<NoteId>,
        skip_auto_commit: bool,
        requested_agent_id: Option<AgentId>,
        extra: AgentCreateExtra,
    ) -> Result<Value> {
        // Depth guard at the service layer (LC-1): mirror the MCP `create_agent`
        // front-door check so every path that spawns a child for a parent
        // already at `MAX_DELEGATION_DEPTH` is refused — including RPC/service
        // callers that bypass the dispatch-level guard. An unknown parent reads
        // as depth 0 (same leniency as the dispatch check).
        if let Some(parent) = &parent_agent_id {
            let parent_depth = self
                .store
                .get_agent_session(parent)
                .await
                .ok()
                .and_then(|s| s.delegation_depth)
                .unwrap_or(0);
            if parent_depth >= MAX_DELEGATION_DEPTH {
                return Err(Error::InvalidParams(format!(
                    "Cannot create sub-agent: maximum delegation depth ({MAX_DELEGATION_DEPTH}) reached. You are at depth {parent_depth}. Please complete this task directly instead of delegating further."
                )));
            }
        }
        let now = now_iso();
        // `name_explicitly_set` defaults to `name.is_some()` so an explicit
        // `agent.create` with a client-supplied name still becomes
        // renameable-with-guard. Delegate flows override to `Some(false)`
        // via `AgentCreateExtra.name_explicitly_set` so their task-derived
        // name stays renameable by the child's opening-turn
        // `ws.workspace.setAgentName` (which uses `skipIfExplicitlySet: true`).
        let name_explicitly_set = extra.name_explicitly_set.unwrap_or_else(|| name.is_some());
        let name =
            name.unwrap_or_else(|| format!("Agent {}", &Uuid::new_v4().simple().to_string()[..6]));
        let id = match requested_agent_id {
            Some(requested) => {
                validate_client_agent_id(requested.as_str())?;
                requested
            }
            None => AgentId(format!("agent-{}", Uuid::new_v4())),
        };
        // `metadata` is persisted (C1d-10a, closes the metadata half of the
        // P2-12a deferral) so `agent.wakeOrCreate` chains can read back the
        // parent's `delegationDepth`/`createdByAgentId`/`taskNoteId`/
        // `isBackground`/`source`/`skipAutoCommit` without a follow-up round-trip.
        // `agent_type`, `workspace_path`, `workspace_context` remain deferred.
        let AgentCreateExtra {
            provider,
            agent_type: _,
            metadata,
            workspace_path: _,
            workspace_context: _,
            context_references,
            image_blocks,
            is_background,
            name_explicitly_set: _,
        } = extra;
        // Harvest the persistence-gap fields the FE writer kept under
        // `metadata` (P3-1.2b). Top-level params win over the metadata copy.
        let meta = metadata.as_ref().and_then(Value::as_object);
        let meta_get = |key: &str| meta.and_then(|m| m.get(key)).cloned();
        let delegation_depth = meta_get("delegationDepth").and_then(|v| v.as_i64());
        let initial_message = meta_get("initialMessage")
            .and_then(|v| v.as_str().map(str::to_string))
            .filter(|s| !s.trim().is_empty());
        let context_references = context_references
            .or_else(|| meta_get("contextReferences"))
            .or_else(|| meta_get("contextRefs"))
            .filter(|v| !v.is_null());
        let image_blocks = image_blocks
            .or_else(|| meta_get("imageBlocks"))
            .filter(|v| !v.is_null());
        let is_background = is_background
            .or_else(|| meta_get("isBackground").and_then(|v| v.as_bool()))
            .unwrap_or(false);

        // Resolve default model from settings when none is explicitly supplied.
        // The resolved model is persisted to session.model, pinning it for the
        // agent's lifetime. Settings changes only affect new agents created
        // afterwards; existing agents change model only via explicit agent.setModel.
        let resolved_model = match model {
            Some(m) => Some(m),
            None => {
                // Pass `specialist` as the agent_type parameter for
                // backgroundAgents.typeOverrides lookup. The specialist value
                // (e.g., "implementor", "verifier") is used as-is in the override
                // map. When `specialist` is None, the type-specific override is
                // skipped and we fall through to backgroundAgents.defaultModel or
                // model.default. A full solution would require passing the derived
                // agent_type through AgentCreateExtra, but that's outside this
                // task's scope and the current specialist-based lookup covers the
                // common case.
                resolve_default_model_from_settings(
                    self,
                    &workspace_id,
                    is_background,
                    specialist.as_deref(),
                    provider.as_deref(),
                )
                .await
            }
        };

        // Initialize provider from the model's compound prefix if not explicitly
        // set. This ensures sessions created with a compound model id like
        // "opencode:kimi-k3" have the correct provider from the start.
        let provider = provider.or_else(|| {
            resolved_model.as_ref().and_then(|m| {
                if m.contains(':') {
                    Some(intent_providers::parse_compound_model_id(m).0)
                } else {
                    None
                }
            })
        });
        let session = AgentSession {
            id,
            workspace_id,
            parent_agent_id,
            backend_session_id: None,
            acp_session_id: None,
            name,
            name_explicitly_set,
            model: resolved_model,
            provider,
            system_prompt: None,
            specialist,
            // Reference parity: `agent-factory.ts:435` persists `AgentStatus.Idle`
            // — the legacy capitalized `"Idle"` variant — on session creation, so
            // a freshly persisted session must be `Idle`, not `Pending`. On
            // `agent.get`/`agent.list` the `AgentLite.status` field is serialized
            // directly by serde, so `Pending` surfaces on the wire as `"pending"`
            // (not `"waiting"` — the `"waiting"` string only comes from the
            // separate `agent_status_wire` normalization used by the diagnostics
            // / subscription snapshots); the FE's session hydration and idle
            // selectors treat `"pending"` as a non-idle initial state, whereas
            // `Idle` correctly hydrates as idle. Both idle wire values,
            // capitalized `"Idle"` (`AgentStatus.Idle`) and lowercase `"idle"`
            // (`AgentStatus.RuntimeIdle`), are accepted equivalently by the FE
            // (see `consolidated-backend.service.ts:1207`
            // `status === AgentStatus.Idle || status === AgentStatus.RuntimeIdle`),
            // so persisting `Idle` here and later rewriting to `RuntimeIdle` at
            // end-of-turn presents a single idle state to the UI. Heal-on-
            // startup is unaffected: `is_stale_in_flight_status` only matches
            // `Active`/`Processing`/`Waiting`, so `Idle` (like the former
            // `Pending`) is left untouched by the sweep at
            // `crates/intent-services/src/lib.rs:653`.
            status: AgentStatus::Idle,
            is_active: false,
            messages: Vec::new(),
            stats: None,
            task_note_id,
            skip_auto_commit,
            completion_report: None,
            completion_report_timestamp: None,
            delegation_depth,
            initial_message,
            context_references,
            image_blocks,
            is_background,
            metadata,
            stop_reason: None,
            created_at: now.clone(),
            updated_at: now,
            sandbox_id: None,
            sandbox_path: None,
            sandbox_branch: None,
        };
        self.store.insert_agent_session(&session).await?;
        self.publish_agent_mutation_event(
            &session.workspace_id,
            &session.id,
            intent_core::events::AGENT_CREATED,
            json!({ "agentId": session.id.0, "name": session.name }),
        )
        .await;
        // Project into `AgentLite` so the wire returns the full agent object
        // (superset of `{ id, name }`). A fresh session has no messages, so the
        // derived counts/last-* fields are `None`/0; runtime activity flags stay
        // at their `AgentLite::from_session` defaults (no live runtime state).
        let lite = AgentLite::from_session(session, 0, None, None, None);
        Ok(json!({ "agent": lite }))
    }

    /// `agent.rename` (PROTOCOL §5.5). A missing agent surfaces as `-32603`
    /// (matching the TS `renameAgentOnDisk` failure path). When
    /// `skip_if_explicitly_set` is `true` and the session's name was already
    /// explicitly set, the rename is a no-op returning the existing name with
    /// `skipped: true` (the FE `renameAgent` semantics). An applied rename
    /// emits `agent:renamed`.
    pub(crate) async fn agent_rename_op(
        &self,
        agent_id: AgentId,
        name: String,
        skip_if_explicitly_set: bool,
    ) -> Result<Value> {
        let mut session = self.load_session_internal(&agent_id).await?;
        if skip_if_explicitly_set && session.name_explicitly_set {
            return Ok(json!({ "success": true, "name": session.name, "skipped": true }));
        }
        session.name = name.clone();
        session.name_explicitly_set = true;
        session.updated_at = now_iso();
        let workspace_id = session.workspace_id.clone();
        self.store
            .update_agent_session(&workspace_id, &session)
            .await?;
        self.publish_agent_mutation_event(
            &workspace_id,
            &agent_id,
            intent_core::events::AGENT_RENAMED,
            json!({ "agentId": agent_id.0, "name": name }),
        )
        .await;
        Ok(json!({ "success": true, "name": name }))
    }

    /// `agent.setModel` (PROTOCOL §5.5). Emits `agent:updated`.
    ///
    /// When the new model is a compound id (`provider:model`) whose provider
    /// differs from session.provider, this updates session.provider to match,
    /// ensuring the next spawn uses the correct binary.
    pub(crate) async fn agent_set_model_op(
        &self,
        agent_id: AgentId,
        model_id: String,
    ) -> Result<Value> {
        let mut session = self.load_session_internal(&agent_id).await?;
        session.model = Some(model_id.clone());
        // Reconcile session.provider when the model carries an explicit provider
        // prefix that differs from the current session provider. This ensures
        // cross-provider model switches spawn the new provider's binary.
        if model_id.contains(':') {
            let (model_provider, _) = intent_providers::parse_compound_model_id(&model_id);
            if session.provider.as_ref() != Some(&model_provider) {
                session.provider = Some(model_provider);
            }
        }
        session.updated_at = now_iso();
        let workspace_id = session.workspace_id.clone();
        self.store
            .update_agent_session(&workspace_id, &session)
            .await?;
        self.publish_agent_mutation_event(
            &workspace_id,
            &agent_id,
            intent_core::events::AGENT_UPDATED,
            json!({ "agentId": agent_id.0, "modelId": model_id }),
        )
        .await;
        // Schedule debounced lastActivity event (§10.1).
        self.schedule_last_activity_event(workspace_id.clone());
        Ok(json!({ "success": true, "modelId": model_id }))
    }

    /// `agent.delete`: idempotent session delete (PROTOCOL §5.5). When
    /// `workspace_id` is supplied the caller's workspace must match the
    /// session's; a mismatch surfaces as `NotFound` (defense-in-depth against
    /// bare-id probes across workspaces).
    pub(crate) async fn agent_delete_op(
        &self,
        agent_id: AgentId,
        workspace_id: Option<WorkspaceId>,
    ) -> Result<Value> {
        // Capture the workspace before deleting so the post-delete agent:deleted
        // emit can be workspace-scoped. If the session is already gone, skip the
        // emit gracefully rather than failing the idempotent delete. When the
        // caller declares a workspace, reject a cross-workspace bare-id probe
        // by mapping to `NotFound` before touching the store.
        let session_workspace_id = self
            .store
            .get_agent_session(&agent_id)
            .await
            .ok()
            .map(|s| s.workspace_id);
        if let (Some(ws), Some(session_ws)) = (workspace_id.as_ref(), session_workspace_id.as_ref())
        {
            if session_ws != ws {
                return Err(Error::NotFound(format!("agent session {agent_id}")));
            }
        }
        // Route the DELETE through the workspace guard so a stale-caller with the
        // wrong workspace cannot mutate the row even if the pre-check above races
        // with a concurrent workspace move.
        if let Some(session_ws) = session_workspace_id.as_ref() {
            self.store
                .delete_agent_session(session_ws, &agent_id)
                .await?;
        }
        self.agent_queues
            .lock()
            .expect("agent queue registry poisoned")
            .remove(&agent_id);
        if let Some(workspace_id) = session_workspace_id {
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

    /// `agent.getSession` (PROTOCOL §5.5). Full [`AgentSession`] projection —
    /// the superset that `agent.get`/[`AgentLite`] strips (`systemPrompt`,
    /// `specialist`, persisted metadata block, full `messages` log). Used by
    /// the FE-side agent-backend-handler retirement (C1d/C1e) so a `loadAgent`
    /// caller can rehydrate the full session shape from the daemon. Emits no
    /// events (a pure read). `NotFound` when the session is unknown.
    pub(crate) async fn agent_get_session_op(&self, agent_id: AgentId) -> Result<AgentSession> {
        self.store.get_agent_session(&agent_id).await
    }

    /// `agent.update` (PROTOCOL §5.5). Partial update from a `changes` object —
    /// only listed fields are touched; omitted fields are preserved. The store
    /// enforces the write-once (`acpSessionId`) and immutable (`provider`)
    /// invariants; malformed values in `changes` surface as `InvalidParams`.
    /// Emits `agent:updated` (or `agent:renamed` when `name` is the only field
    /// mutated) so subscribed clients invalidate their cached projection.
    pub(crate) async fn agent_update_op(&self, agent_id: AgentId, changes: Value) -> Result<Value> {
        let obj = match changes {
            Value::Object(m) => m,
            _ => {
                return Err(Error::InvalidParams(
                    "agent.update: `changes` must be an object".to_string(),
                ))
            }
        };
        let mut session = self.store.get_agent_session(&agent_id).await?;
        let allowed = [
            "status",
            "isActive",
            "acpSessionId",
            "backendSessionId",
            "name",
            "nameExplicitlySet",
            "model",
            "provider",
            "systemPrompt",
            "specialist",
            "taskNoteId",
            "skipAutoCommit",
            "completionReport",
            "completionReportTimestamp",
            "delegationDepth",
            "initialMessage",
            "contextReferences",
            "imageBlocks",
            "isBackground",
        ];
        for key in obj.keys() {
            if !allowed.contains(&key.as_str()) {
                return Err(Error::InvalidParams(format!(
                    "agent.update: unknown field `{key}` in `changes`"
                )));
            }
        }
        let mut mutated_only_name = obj.contains_key("name");
        for (key, value) in obj.iter() {
            if key != "name" {
                mutated_only_name = false;
            }
            match key.as_str() {
                "status" => {
                    session.status = serde_json::from_value(value.clone()).map_err(|e| {
                        Error::InvalidParams(format!("agent.update: invalid status: {e}"))
                    })?;
                }
                "isActive" => {
                    session.is_active = value.as_bool().ok_or_else(|| {
                        Error::InvalidParams(
                            "agent.update: `isActive` must be a boolean".to_string(),
                        )
                    })?;
                }
                "acpSessionId" => {
                    session.acp_session_id = update_optional_string(value, "acpSessionId")?;
                }
                "backendSessionId" => {
                    session.backend_session_id = update_optional_string(value, "backendSessionId")?
                        .map(|s| AgentId::from(s.as_str()));
                }
                "name" => {
                    session.name = value
                        .as_str()
                        .ok_or_else(|| {
                            Error::InvalidParams(
                                "agent.update: `name` must be a string".to_string(),
                            )
                        })?
                        .to_string();
                    session.name_explicitly_set = true;
                }
                "nameExplicitlySet" => {
                    session.name_explicitly_set = value.as_bool().ok_or_else(|| {
                        Error::InvalidParams(
                            "agent.update: `nameExplicitlySet` must be a boolean".to_string(),
                        )
                    })?;
                }
                "model" => {
                    session.model = update_optional_string(value, "model")?;
                }
                "provider" => {
                    session.provider = update_optional_string(value, "provider")?;
                }
                "systemPrompt" => {
                    session.system_prompt = update_optional_string(value, "systemPrompt")?;
                }
                "specialist" => {
                    session.specialist = update_optional_string(value, "specialist")?;
                }
                "taskNoteId" => {
                    session.task_note_id =
                        update_optional_string(value, "taskNoteId")?.map(NoteId::from);
                }
                "skipAutoCommit" => {
                    session.skip_auto_commit = value.as_bool().ok_or_else(|| {
                        Error::InvalidParams(
                            "agent.update: `skipAutoCommit` must be a boolean".to_string(),
                        )
                    })?;
                }
                "completionReport" => {
                    session.completion_report = update_optional_string(value, "completionReport")?;
                }
                "completionReportTimestamp" => {
                    session.completion_report_timestamp =
                        update_optional_string(value, "completionReportTimestamp")?;
                }
                "delegationDepth" => {
                    session.delegation_depth = if value.is_null() {
                        None
                    } else {
                        Some(value.as_i64().ok_or_else(|| {
                            Error::InvalidParams(
                                "agent.update: `delegationDepth` must be an integer".to_string(),
                            )
                        })?)
                    };
                }
                "initialMessage" => {
                    session.initial_message = update_optional_string(value, "initialMessage")?;
                }
                "contextReferences" => {
                    session.context_references = if value.is_null() {
                        None
                    } else {
                        Some(value.clone())
                    };
                }
                "imageBlocks" => {
                    session.image_blocks = if value.is_null() {
                        None
                    } else {
                        Some(value.clone())
                    };
                }
                "isBackground" => {
                    session.is_background = value.as_bool().ok_or_else(|| {
                        Error::InvalidParams(
                            "agent.update: `isBackground` must be a boolean".to_string(),
                        )
                    })?;
                }
                _ => unreachable!("guarded by allow-list above"),
            }
        }
        session.updated_at = now_iso();
        let workspace_id = session.workspace_id.clone();
        self.store
            .update_agent_session(&workspace_id, &session)
            .await?;
        let event_type = if mutated_only_name {
            intent_core::events::AGENT_RENAMED
        } else {
            AGENT_UPDATED
        };
        let mut event_data = serde_json::Map::new();
        event_data.insert("agentId".into(), json!(agent_id.0));
        for (k, v) in obj.iter() {
            event_data.insert(k.clone(), v.clone());
        }
        self.publish_agent_mutation_event(
            &session.workspace_id,
            &agent_id,
            event_type,
            Value::Object(event_data),
        )
        .await;
        // Schedule debounced lastActivity event (§10.1).
        self.schedule_last_activity_event(workspace_id.clone());
        let lite = self.project_lite_with_flags(session);
        Ok(json!({ "success": true, "agent": lite }))
    }

    /// `agent.appendMessage` (PROTOCOL §5.5). Append a single message to the
    /// transcript. Rejected with `InvalidParams` when the agent is mid-turn
    /// (message-log mutation must not race the daemon's streaming writer).
    /// `metadata` is persisted verbatim on the row. Emits `agent:message`.
    pub(crate) async fn agent_append_message_op(
        &self,
        agent_id: AgentId,
        role: String,
        content: Value,
        metadata: Option<Value>,
    ) -> Result<Value> {
        let session = self.store.get_agent_session(&agent_id).await?;
        if self.agent_is_busy(agent_id.clone()) {
            return Err(Error::InvalidParams(format!(
                "agent.appendMessage: session {} is busy — cannot mutate transcript during an active turn",
                agent_id.0
            )));
        }
        validate_message_role(&role)?;
        let created_at = now_iso();
        let message = self
            .store
            .append_agent_message_with_metadata(
                &agent_id,
                &role,
                &content,
                metadata.as_ref(),
                &created_at,
            )
            .await?;
        // Refresh agent_session.updated_at so the FE agent-card timestamp
        // reflects message activity, not just status transitions (STAB-19).
        if let Err(e) = self
            .store
            .refresh_agent_session_timestamp(&session.workspace_id, &agent_id, &created_at)
            .await
        {
            tracing::warn!(agent = %agent_id, error = %e, "refresh_agent_session_timestamp failed");
        } else {
            // Schedule debounced lastActivity event (§10.1).
            self.schedule_last_activity_event(session.workspace_id.clone());
        }
        self.publish_agent_mutation_event(
            &session.workspace_id,
            &agent_id,
            AGENT_MESSAGE,
            json!({ "agentId": agent_id.0, "messageId": message.id, "role": message.role }),
        )
        .await;
        Ok(json!({ "success": true, "message": message }))
    }

    /// `agent.replaceMessages` (PROTOCOL §5.5). Atomically swap the transcript
    /// with `messages`. Rejected with `InvalidParams` when the agent is mid-turn
    /// (same rationale as [`Services::agent_append_message_op`]). Row ids are
    /// minted by the store — callers cannot smuggle stale ids across the swap.
    /// Emits `agent:updated` with `{ replacedCount }`.
    pub(crate) async fn agent_replace_messages_op(
        &self,
        agent_id: AgentId,
        messages: Value,
    ) -> Result<Value> {
        let session = self.store.get_agent_session(&agent_id).await?;
        if self.agent_is_busy(agent_id.clone()) {
            return Err(Error::InvalidParams(format!(
                "agent.replaceMessages: session {} is busy — cannot mutate transcript during an active turn",
                agent_id.0
            )));
        }
        let raw = messages.as_array().ok_or_else(|| {
            Error::InvalidParams("agent.replaceMessages: `messages` must be an array".to_string())
        })?;
        struct Parsed {
            role: String,
            content: Value,
            metadata: Option<Value>,
            created_at: String,
        }
        let mut parsed: Vec<Parsed> = Vec::with_capacity(raw.len());
        let fallback_ts = now_iso();
        for (i, entry) in raw.iter().enumerate() {
            let obj = entry.as_object().ok_or_else(|| {
                Error::InvalidParams(format!(
                    "agent.replaceMessages: `messages[{i}]` must be an object"
                ))
            })?;
            let role = obj
                .get("role")
                .and_then(Value::as_str)
                .ok_or_else(|| {
                    Error::InvalidParams(format!(
                        "agent.replaceMessages: `messages[{i}].role` is required"
                    ))
                })?
                .to_string();
            validate_message_role(&role)?;
            let content = obj
                .get("contentBlocks")
                .or_else(|| obj.get("content"))
                .cloned()
                .ok_or_else(|| {
                    Error::InvalidParams(format!(
                        "agent.replaceMessages: `messages[{i}].contentBlocks` is required"
                    ))
                })?;
            let metadata = match obj.get("metadata") {
                Some(Value::Null) | None => None,
                Some(v) => Some(v.clone()),
            };
            let created_at = match obj.get("timestamp").or_else(|| obj.get("createdAt")) {
                Some(Value::String(s)) => s.clone(),
                Some(Value::Null) | None => fallback_ts.clone(),
                Some(_) => {
                    return Err(Error::InvalidParams(format!(
                        "agent.replaceMessages: `messages[{i}].timestamp` must be a string"
                    )))
                }
            };
            parsed.push(Parsed {
                role,
                content,
                metadata,
                created_at,
            });
        }
        let batch: Vec<intent_store::ReplaceMessage<'_>> = parsed
            .iter()
            .map(|p| intent_store::ReplaceMessage {
                role: p.role.as_str(),
                content: &p.content,
                metadata: p.metadata.as_ref(),
                created_at: p.created_at.as_str(),
            })
            .collect();
        let inserted = self.store.replace_agent_messages(&agent_id, &batch).await?;
        let replaced_count = inserted.len();
        self.publish_agent_mutation_event(
            &session.workspace_id,
            &agent_id,
            AGENT_UPDATED,
            json!({ "agentId": agent_id.0, "replacedCount": replaced_count }),
        )
        .await;
        Ok(json!({ "success": true, "messages": inserted }))
    }

    /// `agent.getModels`: auggie CLI with the static-tier fallback (PROTOCOL §5.5).
    pub(crate) async fn agent_get_models_op(&self) -> Result<Value> {
        let models = match fetch_auggie_models().await? {
            Some(m) => m,
            None => static_models(),
        };
        Ok(json!({ "models": models }))
    }

    /// `models.list`: the rich model catalog for FE model pickers (PROTOCOL
    /// §5.30) — auggie CLI (JSON → plain-text fallback) with a 5-minute
    /// success cache; degrades to the static tier catalog (`source: "static"`)
    /// when the CLI is unavailable, so the result is never empty.
    pub(crate) async fn models_list_op(&self) -> Result<Value> {
        if let Some(models) = self.cached_models() {
            return Ok(json!({ "models": models, "source": "auggie" }));
        }
        if let Some(models) = fetch_auggie_models_rich().await {
            self.store_models_cache(models.clone());
            return Ok(json!({ "models": models, "source": "auggie" }));
        }
        Ok(json!({ "models": static_models(), "source": "static" }))
    }

    /// The cached `models.list` rows when still within [`MODELS_CACHE_TTL`].
    fn cached_models(&self) -> Option<Vec<Value>> {
        self.models_cache
            .lock()
            .expect("models cache poisoned")
            .as_ref()
            .and_then(|(at, rows)| (at.elapsed() < MODELS_CACHE_TTL).then(|| rows.clone()))
    }

    /// Record a successful `models.list` CLI fetch for [`MODELS_CACHE_TTL`].
    fn store_models_cache(&self, rows: Vec<Value>) {
        *self.models_cache.lock().expect("models cache poisoned") =
            Some((std::time::Instant::now(), rows));
    }

    /// `agent.queueMessage` (PROTOCOL §5.5). Enqueues the message, publishes
    /// `agent:queue:updated`, and asks the runtime [`AgentManager`] (when attached)
    /// to drain the queue immediately if the agent is idle — closing the bug where
    /// a queued message would never be sent because the BE only drained the queue
    /// from a live worker loop.
    pub(crate) async fn agent_queue_message_op(
        &self,
        agent_id: AgentId,
        content: String,
        image_blocks: Option<Value>,
        file_blocks: Option<Value>,
    ) -> Result<Value> {
        let (queued, position) =
            self.enqueue_message(&agent_id, content, image_blocks, file_blocks);
        let result = json!({ "success": true, "queuedMessage": queued.to_value(position) });
        self.publish_queue_updated(&agent_id).await;
        if let Some(manager) = self.agent_manager() {
            if let Ok(session) = self.store.get_agent_session(&agent_id).await {
                manager
                    .try_drain_queue(agent_id, session.workspace_id)
                    .await;
            }
        }
        Ok(result)
    }

    /// `agent.getQueue` (PROTOCOL §5.5). When `workspace_id` is supplied the
    /// callee verifies the session belongs to that workspace (defense-in-depth
    /// against a bare `agentId` probe across workspaces); a mismatch surfaces
    /// as `NotFound`.
    pub(crate) async fn agent_get_queue_op(
        &self,
        agent_id: AgentId,
        workspace_id: Option<WorkspaceId>,
    ) -> Result<Value> {
        if let Some(ws) = workspace_id.as_ref() {
            let session = self.store.get_agent_session(&agent_id).await?;
            if session.workspace_id != *ws {
                return Err(Error::NotFound(format!("agent session {agent_id}")));
            }
        }
        let queue = self.queue_snapshot(&agent_id);
        Ok(json!({ "success": true, "queue": queue }))
    }

    /// `agent.editQueuedMessage` (PROTOCOL §5.5). Updates the entry's content
    /// in place (matching the reference's `handleEditQueuedMessage`) and, when
    /// the optional `editing` flag is provided, transitions the entry between
    /// "ready-to-send" (`editing = false`) and "under edit" (`editing = true`).
    /// Publishes `agent:queue:updated` with the post-edit snapshot. Returns
    /// `Internal` when the message id is unknown — only `removeQueuedMessage` is
    /// idempotent.
    ///
    /// When an entry transitions `editing: true → false` (the FE finished
    /// editing) we additionally fire `try_drain_queue` so the message
    /// self-drains as if it had just been enqueued — honouring the user's
    /// "re-queued on save, which self-drains" semantics (PROTOCOL §5.5/§6.5).
    pub(crate) async fn agent_edit_queued_message_op(
        &self,
        agent_id: AgentId,
        message_id: String,
        content: String,
        editing: Option<bool>,
    ) -> Result<Value> {
        let (edited, was_editing, now_editing) = {
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
            let was = queue[position].editing;
            queue[position].content = content;
            if let Some(flag) = editing {
                queue[position].editing = flag;
            }
            let now = queue[position].editing;
            (queue[position].to_value(position), was, now)
        };
        self.publish_queue_updated(&agent_id).await;
        // editing: true → false ⇒ the message is now ready-to-send. Self-drain.
        if was_editing && !now_editing {
            if let Some(manager) = self.agent_manager() {
                if let Ok(session) = self.store.get_agent_session(&agent_id).await {
                    manager
                        .try_drain_queue(agent_id, session.workspace_id)
                        .await;
                }
            }
        }
        Ok(json!({ "success": true, "queuedMessage": edited }))
    }

    /// `agent.removeQueuedMessage` (PROTOCOL §5.5). **Idempotent**: returns
    /// `{ success: true }` whether or not the message (or the agent's queue) was
    /// found. The FE's seeded queue can diverge from the BE's in-memory queue
    /// (especially after a daemon restart); the original "Queued message not
    /// found" error caused the FE's optimistic delete to roll back, leaving
    /// ghost messages on screen.
    pub(crate) async fn agent_remove_queued_message_op(
        &self,
        agent_id: AgentId,
        message_id: String,
    ) -> Result<Value> {
        let removed = {
            let mut guard = self
                .agent_queues
                .lock()
                .expect("agent queue registry poisoned");
            match guard.get_mut(&agent_id) {
                Some(queue) => {
                    let before = queue.len();
                    queue.retain(|m| m.id != message_id);
                    before != queue.len()
                }
                None => false,
            }
        };
        if removed {
            self.publish_queue_updated(&agent_id).await;
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
        image_blocks: Option<Value>,
        file_blocks: Option<Value>,
    ) -> Result<Value> {
        // Validate message_id length to prevent unbounded storage.
        if let Some(ref id) = message_id {
            if id.len() > MAX_MESSAGE_ID_LEN {
                return Err(Error::InvalidParams(format!(
                    "messageId exceeds maximum length of {} bytes",
                    MAX_MESSAGE_ID_LEN
                )));
            }
        }
        let blocks = user_content_blocks(&content);
        let created_at = now_iso();
        let message = match message_id {
            Some(id) => {
                self.store
                    .append_agent_message_with_id(
                        &agent_id,
                        &id,
                        "user",
                        &blocks,
                        None,
                        &created_at,
                    )
                    .await
            }
            None => {
                self.store
                    .append_agent_message(&agent_id, "user", &blocks, &created_at)
                    .await
            }
        };
        match message {
            Ok(message) => {
                // Refresh agent_session.updated_at so the FE agent-card timestamp
                // reflects message activity, not just status transitions (STAB-19).
                // Fetch the session to get workspace_id; best-effort (logged on error).
                match self.store.get_agent_session(&agent_id).await {
                    Ok(session) => {
                        if let Err(e) = self
                            .store
                            .refresh_agent_session_timestamp(
                                &session.workspace_id,
                                &agent_id,
                                &created_at,
                            )
                            .await
                        {
                            tracing::warn!(agent = %agent_id, error = %e, "refresh_agent_session_timestamp failed");
                        }
                        // Publish agent:message event using the store-returned message id.
                        self.publish_agent_mutation_event(
                            &session.workspace_id,
                            &agent_id,
                            AGENT_MESSAGE,
                            json!({ "agentId": agent_id.0, "messageId": message.id, "role": message.role }),
                        )
                        .await;
                    }
                    Err(e) => {
                        tracing::warn!(agent = %agent_id, error = %e, "get_agent_session failed; skipping timestamp refresh and agent:message event");
                    }
                }
                Ok(json!({ "success": true, "queued": false, "messageId": message.id }))
            }
            Err(_) => {
                // STAB-7: preserve image_blocks and file_blocks when auto-queueing
                // on store failure, matching the runtime-manager path's behavior.
                let (queued, position) =
                    self.enqueue_message(&agent_id, content, image_blocks, file_blocks);
                let result = json!({
                    "success": true,
                    "queued": true,
                    "queuedMessage": queued.to_value(position),
                });
                self.publish_queue_updated(&agent_id).await;
                Ok(result)
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
        // Validate message_id length to prevent unbounded storage.
        if message_id.len() > MAX_MESSAGE_ID_LEN {
            return Err(Error::InvalidParams(format!(
                "messageId exceeds maximum length of {} bytes",
                MAX_MESSAGE_ID_LEN
            )));
        }
        let session = self.store.get_agent_session(&agent_id).await?;
        let blocks = user_content_blocks(&content);
        let created_at = now_iso();
        let message = self
            .store
            .append_agent_message_with_id(
                &agent_id,
                &message_id,
                "user",
                &blocks,
                None,
                &created_at,
            )
            .await?;
        // Refresh agent_session.updated_at so the FE agent-card timestamp
        // reflects message activity, not just status transitions (STAB-19).
        if let Err(e) = self
            .store
            .refresh_agent_session_timestamp(&session.workspace_id, &agent_id, &created_at)
            .await
        {
            tracing::warn!(agent = %agent_id, error = %e, "refresh_agent_session_timestamp failed");
        } else {
            // Schedule debounced lastActivity event (§10.1).
            self.schedule_last_activity_event(session.workspace_id.clone());
        }
        // Publish agent:message event using the store-returned message id.
        self.publish_agent_mutation_event(
            &session.workspace_id,
            &agent_id,
            AGENT_MESSAGE,
            json!({ "agentId": agent_id.0, "messageId": message.id, "role": message.role }),
        )
        .await;
        Ok(json!({ "success": true, "queued": false, "messageId": message.id }))
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

    /// `agent.getSessionStats`: the per-session credit/message/tool rollup as
    /// `{ stats: SessionStats }` (PROTOCOL §5.24). `NotFound` is surfaced to the
    /// router which maps it to `-32602`. Stats are sourced from the auggie CLI
    /// (`session stats <sessionId> --json`); when the CLI is unavailable the
    /// counts fall back to the transcript and `creditsUsed` stays `null`
    /// (graceful degrade — never panics). A refreshed rollup that differs from
    /// the cached snapshot pushes `agent:session-stats-changed` (§6.5).
    pub(crate) async fn agent_get_session_stats_op(
        &self,
        session_id: AgentId,
        workspace_id: Option<WorkspaceId>,
    ) -> Result<Value> {
        let session = self.store.get_agent_session(&session_id).await?;
        if let Some(ws) = workspace_id.as_ref() {
            if session.workspace_id != *ws {
                return Err(Error::NotFound(format!("agent session {session_id}")));
            }
        }
        let stats = match fetch_session_stats(&session_id).await {
            Some(cli) => cli,
            None => {
                let (message_count, tool_count) = transcript_counts(&session.messages);
                SessionStats {
                    credits_used: None,
                    message_count,
                    tool_count,
                }
            }
        };
        self.cache_and_emit_session_stats(&session, &stats).await;
        Ok(json!({ "stats": stats }))
    }

    /// Cache the latest session-stats snapshot and, when it differs from the
    /// previously observed one, push the self-sufficient
    /// `agent:session-stats-changed` event (PROTOCOL §5.24 / §6.5). In this model
    /// a session id is the agent id, so the payload carries both.
    async fn cache_and_emit_session_stats(&self, session: &AgentSession, stats: &SessionStats) {
        let changed = {
            let mut cache = self
                .session_stats_cache
                .lock()
                .expect("session stats cache poisoned");
            if cache.get(&session.id) == Some(stats) {
                false
            } else {
                cache.insert(session.id.clone(), stats.clone());
                true
            }
        };
        if !changed {
            return;
        }
        crate::publish_event(
            &self.event_bus,
            intent_store::NewEvent {
                workspace_id: session.workspace_id.clone(),
                timestamp: now_iso(),
                event_type: intent_core::events::AGENT_SESSION_STATS_CHANGED.to_string(),
                actor: crate::system_actor(),
                session_id: Some(session.id.0.clone()),
                correlation_id: None,
                parent_event_id: None,
                metadata: None,
                data: json!({
                    "sessionId": session.id.0,
                    "agentId": session.id.0,
                    "stats": stats,
                }),
            },
        )
        .await;
    }

    /// `agent.reportToParent`: a delegated child reports back to its parent
    /// (PROTOCOL §5.5). Caller identity comes only from the MCP front door; the
    /// RPC dispatch path passes `None`, so it always surfaces `-32603`. When the
    /// caller has no `parentAgentId` (created directly by a user), this is also
    /// `-32603`. Otherwise the report is persisted on the child session
    /// (`metadata.completionReport` / `completionReportTimestamp`, the TS
    /// parity; P3-1.2b) — emitting `agent:updated` — and, when the caller has a
    /// linked task note whose current status is non-terminal, the note is
    /// transitioned to `review_required` (TASK-B, mirroring the reference
    /// `reportToParent` writer). No immediate parent wake is issued (SUB-2):
    /// the child's `agent:idle` (via the still-armed completion watch)
    /// delivers the single wake, whose formatted text/metadata includes the
    /// persisted `completionReport`. `after_all` grouping path is unchanged —
    /// the group's aggregated wake still folds this child's report in.
    pub(crate) async fn agent_report_to_parent_op(
        &self,
        workspace_id: WorkspaceId,
        report: Value,
        caller_agent_id: Option<AgentId>,
    ) -> Result<Value> {
        let not_delegated = || {
            Error::Internal("report_to_parent is only available to delegated agents".to_string())
        };
        let caller = caller_agent_id.ok_or_else(not_delegated)?;
        let mut session = self.load_session_internal(&caller).await?;
        // Copilot #104 (thread PRRT_kwDOS9Wxuc6QKTPK): scope-guard the
        // caller-supplied `workspace_id` the same way `agent_get_op` /
        // `agent_get_conversation_op` do — reject a cross-workspace mismatch
        // with `NotFound` before any state changes (report persistence,
        // `review_required` transition, subscription notification), so a
        // request targeting the wrong workspace never mutates the session's
        // actual workspace by side effect.
        if session.workspace_id != workspace_id {
            return Err(Error::NotFound(format!("agent session {caller}")));
        }
        let parent = session.parent_agent_id.clone().ok_or_else(not_delegated)?;
        // `report` is declared as a string on the MCP surface; coerce other
        // JSON shapes to their textual form for delivery.
        let report_text = match &report {
            Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        let report_len = report_text.chars().count() as i64;
        // Persist the completion report on the child so `agent.get`/`agent.list`
        // (and `ws.agent.summary`) can re-serve it after restarts.
        let saved_at = now_iso();
        session.completion_report = Some(report_text.clone());
        session.completion_report_timestamp = Some(saved_at.clone());
        session.updated_at = saved_at.clone();
        let workspace_id = session.workspace_id.clone();
        let task_note_id = session.task_note_id.clone();
        self.store
            .update_agent_session(&workspace_id, &session)
            .await?;
        self.publish_agent_mutation_event(
            &session.workspace_id,
            &caller,
            intent_core::events::AGENT_UPDATED,
            json!({ "agentId": caller.0, "completionReportLength": report_len }),
        )
        .await;
        // Schedule debounced lastActivity event (§10.1).
        self.schedule_last_activity_event(workspace_id.clone());
        // TASK-B: move the linked task note to `review_required` so the FE
        // reflects the child's completion. Terminal statuses (`complete`,
        // `cancelled`) are never overwritten; agents without a linked note are
        // a no-op. Errors from the store lookup or the status writer are
        // logged and swallowed — the report itself is already persisted, and
        // the caller's response must not depend on FE-facing task metadata.
        if let Some(note_id) = task_note_id {
            self.transition_linked_task_to_review_required(&workspace_id, note_id, caller.clone())
                .await;
        }

        // Immediate parent wake for non-grouped children: if this child is in an
        // `after_all` delegation group, skip the immediate wake — the group's
        // aggregated wake will fold this report in when all children settle. Otherwise,
        // deliver the wake NOW unconditionally to the parent, then mark any oneShot
        // watches to suppress their agent:idle delivery (report-time wake requirement).
        let grouped = self.child_in_undelivered_group(&workspace_id, &parent, &caller);
        if !grouped {
            // Deliver exactly ONE wake to the parent, regardless of watch count.
            // Format the wake message with the persisted report.
            let wake_text = format!(
                "[WORKSPACE EVENTS] Child agent {} ({}) completed. Report: {}",
                session.name, caller.0, report_text
            );
            // Build event notification metadata (mirroring deliver_completion_to_watches).
            let metadata = json!({
                "type": "event_notification",
                "eventCount": 1,
                "eventTypes": ["agent:reportToParent"],
                "events": [{
                    "id": uuid::Uuid::new_v4().to_string(),
                    "type": "agent:reportToParent",
                    "timestamp": saved_at,
                    "data": {
                        "agentId": caller.0,
                        "agentName": session.name.clone(),
                        "report": report_text.clone(),
                    },
                    "actor": {
                        "type": "agent",
                        "id": caller.0,
                        "name": session.name.clone(),
                    }
                }]
            });
            // Deliver the wake to the parent unconditionally (even if no watch exists).
            if let Err(e) = self
                .deliver_parent_wake(&workspace_id, parent.clone(), wake_text, Some(metadata))
                .await
            {
                tracing::warn!(
                    error = %e,
                    parent = %parent.0,
                    child = %caller.0,
                    "failed to deliver reportToParent wake to parent"
                );
            }

            // Mark any oneShot, ungrouped watches whose parent matches this parent as
            // report_delivered, so deliver_completion_to_watches will skip agent:idle
            // for them (suppressing the duplicate wake). Do NOT mark watches for other
            // watchers (e.g. SUB-1 sender-watches) — those keep their normal idle-time
            // completion wake and should not receive the report wake.
            let watches = self.find_watches_for_child(&workspace_id, &caller);
            for watch in watches
                .iter()
                .filter(|w| w.one_shot && w.group_id.is_none() && w.parent_agent_id == parent)
            {
                self.mark_watch_report_delivered(&workspace_id, &watch.id);
            }
        }

        Ok(json!({
            "ok": true,
            "parentAgentId": parent,
            "reportLength": report_len,
            "savedAt": saved_at,
        }))
    }

    /// TASK-B helper: transition the caller's linked task note to
    /// `review_required` iff its current status is non-terminal (i.e. not
    /// `complete`/`cancelled`). Uses the same `task.updateNoteStatus` writer
    /// the router path uses, so it publishes `task:status-changed` +
    /// `notes:ready-tasks-changed` with the caller as `agentId`. All errors
    /// are logged and swallowed: the persisted completion report is the
    /// contract of `agent.reportToParent`, and the FE-facing status update is
    /// best-effort.
    async fn transition_linked_task_to_review_required(
        &self,
        workspace_id: &WorkspaceId,
        task_note_id: NoteId,
        caller: AgentId,
    ) {
        let note = match crate::fetch_note(&self.store, workspace_id, &task_note_id).await {
            Ok(note) => note,
            // A missing or out-of-workspace linked note is the expected shape
            // for stale/cross-workspace session metadata — keep it a silent
            // no-op (debug-level) so normal operation isn't noisy. Real
            // internal failures still surface as warnings.
            Err(Error::NotFound(_)) => {
                tracing::debug!(
                    note = %task_note_id,
                    "report_to_parent: linked task note not found in this workspace; skipping status transition"
                );
                return;
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    note = %task_note_id,
                    "report_to_parent: failed to load linked task note for status transition"
                );
                return;
            }
        };
        let Some(task) = note.metadata.task.as_ref() else {
            return;
        };
        // Terminal statuses must not be downgraded (parity with the router
        // path's own no-op-when-unchanged branch), and a task already in
        // `review_required` must skip the writer entirely: TASK-B's
        // `task_update_note_status` always persists (bumping `updated_at` and
        // `rev`) before checking `previous_status != new_status`, so repeated
        // `reportToParent` calls would otherwise churn the note on every hop.
        if matches!(
            task.status,
            intent_core::TaskStatus::Complete
                | intent_core::TaskStatus::Cancelled
                | intent_core::TaskStatus::ReviewRequired
        ) {
            return;
        }
        if let Err(e) = WorkspaceApi::task_update_note_status(
            self,
            workspace_id.clone(),
            task_note_id.clone(),
            "review_required".into(),
            None,
            Some(caller),
        )
        .await
        {
            tracing::warn!(
                error = %e,
                note = %task_note_id,
                "report_to_parent: failed to transition linked task to review_required"
            );
        }
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
        // Persist the task linkage + skipAutoCommit on the session so the
        // auto-commit-on-idle subscriber (LNI-1) can resolve `Linked-Note-Id:`
        // and honor the opt-out without a reverse lookup on every idle event.
        let session_task_note_id = input.task_note_id.clone().or(input.note_id.clone());
        // Resolve the child's first message up front so it can be persisted as
        // `metadata.initialMessage` on the created session (P3-1.2b; the FE
        // stored it so a wake-up can resume). Source priority mirrors the TS
        // `DelegateTaskTool`: explicit `agentInstructions`, then `taskText`,
        // then the linked task note's content (falling back to its title).
        fn first_nonempty(s: &str) -> Option<String> {
            let trimmed = s.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_string())
        }
        let task_text_msg = input.task_text.as_deref().and_then(first_nonempty);
        let mut message = input
            .agent_instructions
            .as_deref()
            .and_then(first_nonempty)
            .or_else(|| task_text_msg.clone());
        // Load the linked task note whenever the delegation names one: the
        // note's title/body feeds the message fallback, the child name
        // derivation, and the TASK-C reference preamble that prefixes the
        // child's first message when a task is linked.
        let task_note = match session_task_note_id.as_ref() {
            Some(note_id) => crate::fetch_note(&self.store, &workspace_id, note_id)
                .await
                .ok(),
            None => None,
        };
        if message.is_none() {
            if let Some(note) = task_note.as_ref() {
                message = first_nonempty(&note.content).or_else(|| first_nonempty(&note.title));
            }
        }
        // TASK-C: mirror the reference `DelegateTaskTool` preamble
        // (agent-interaction-tools.ts). When the delegation links a task note,
        // APPEND the standard "Your Task Note" block after the user message
        // with a `---` separator so the child knows its note ID/title and the
        // single-task scope contract; without a linked note the message is
        // delivered verbatim. When `skipAutoCommit` is set, a follow-on
        // commit instruction is concatenated after the scope directive,
        // byte-for-byte matching the reference.
        if let (Some(note), Some(note_id)) = (task_note.as_ref(), session_task_note_id.as_ref()) {
            let title = first_nonempty(&note.title).unwrap_or_default();
            // Build the preamble from adjacent string literals (via `concat!`)
            // so no source-level indentation leaks into the emitted bytes. Every
            // `\n` is explicit; the resulting string is byte-for-byte the
            // reference `DelegateTaskTool` preamble
            // (`agent-interaction-tools.ts`).
            let preamble = format!(
                concat!(
                    "**Your Task Note:** \"{title}\" (ID: {note_id})\n",
                    "This note is your workspace for this task. Update it with your progress, findings, and deliverables.\n",
                    "\n",
                    "**SCOPE: Complete THIS task only.** When done, mark it complete and end your session. Do not pick up other tasks.",
                ),
                title = title,
                note_id = note_id,
            );
            let commit_instruction = if input.skip_auto_commit.unwrap_or(false) {
                "\n\n**Auto-commit is OFF.** Do not commit unless the user explicitly asks. If asked, use `agent_commit_changes` with `userRequested: true`."
            } else {
                ""
            };
            message = Some(match message {
                Some(body) if !body.is_empty() => {
                    format!("{body}\n\n---\n{preamble}{commit_instruction}")
                }
                _ => format!("{preamble}{commit_instruction}"),
            });
        }
        // Resolve the child agent's name to match the reference `DelegateTaskTool`
        // (agent-interaction-tools.ts): the taskText path uses `taskText`, the
        // taskNoteId path uses the note's title. Both truncate to 100 chars
        // (`len > 100 ? substring(0,97) + "..." : text`). Without this the
        // child inherits the generic `Agent xxxxxx` fallback from
        // `agent_create_op`, which then leaks into the waiting panel, agent
        // cards, and `agent:idle` wake reports (NAME-1).
        fn truncate_agent_name(name: String) -> String {
            // The reference uses JS `.length` / `.substring(0, 97)`, which
            // count UTF-16 code units — non-BMP characters (e.g. emoji)
            // count as 2. Match that to avoid off-by-one divergences on
            // emoji-heavy task titles (Copilot #84 review).
            let u16_len = name.encode_utf16().count();
            if u16_len > 100 {
                let units: Vec<u16> = name.encode_utf16().take(97).collect();
                // `substring(0, 97)` can split a surrogate pair; drop a
                // trailing lone high surrogate so the resulting Rust string
                // stays valid UTF-8 instead of embedding a U+FFFD replacement.
                let cutoff = if units
                    .last()
                    .is_some_and(|&u| (0xD800..=0xDBFF).contains(&u))
                {
                    units.len() - 1
                } else {
                    units.len()
                };
                let mut truncated = String::from_utf16_lossy(&units[..cutoff]);
                truncated.push_str("...");
                truncated
            } else {
                name
            }
        }
        let child_name = task_text_msg
            .or_else(|| task_note.as_ref().and_then(|n| first_nonempty(&n.title)))
            .map(truncate_agent_name);
        // Load the delegating parent once: it feeds the child's
        // `delegationDepth` (parent depth + 1; P3-1.2b), the depth-limit guard
        // below, and the completion-watch registration.
        let parent_session = match &parent_agent_id {
            Some(parent) => self.store.get_agent_session(parent).await.ok(),
            None => None,
        };
        // Depth guard (port of `MAX_DELEGATION_DEPTH` in the reference
        // `agent-interaction-tools.ts`): a caller already at the max depth
        // cannot delegate further. Enforced only when a caller is present
        // (MCP front door); RPC-level creates stay parentless and skip it.
        if parent_agent_id.is_some() {
            let parent_depth = parent_session
                .as_ref()
                .and_then(|s| s.delegation_depth)
                .unwrap_or(0);
            if parent_depth >= MAX_DELEGATION_DEPTH {
                return Err(Error::InvalidParams(format!(
                    "Cannot delegate task: maximum delegation depth ({MAX_DELEGATION_DEPTH}) reached. You are at depth {parent_depth}. Please complete this task directly instead of delegating further."
                )));
            }
        }
        let delegation_depth = parent_agent_id.as_ref().map(|_| {
            parent_session
                .as_ref()
                .and_then(|s| s.delegation_depth)
                .unwrap_or(0)
                + 1
        });
        let mut extra_metadata = serde_json::Map::new();
        if let Some(depth) = delegation_depth {
            extra_metadata.insert("delegationDepth".to_string(), json!(depth));
        }
        if let Some(msg) = &message {
            extra_metadata.insert("initialMessage".to_string(), json!(msg));
        }
        // Delegated agents are background agents (the TS `DelegateTaskTool`
        // always sets `metadata.isBackground: true`; G-A1/P3-1.2c).
        extra_metadata.insert("isBackground".to_string(), json!(true));
        let extra = AgentCreateExtra {
            metadata: (!extra_metadata.is_empty()).then_some(Value::Object(extra_metadata)),
            // Delegated agents carry a task-derived name but stay renameable
            // by the child's opening-turn `ws.workspace.setAgentName`
            // (`skipIfExplicitlySet: true`) — mirror the reference which
            // does not set `nameExplicitlySet` at delegate-time creation.
            name_explicitly_set: Some(false),
            ..AgentCreateExtra::default()
        };
        let created = self
            .agent_create_op(
                workspace_id.clone(),
                child_name,
                input.model,
                input.specialist,
                parent_agent_id.clone(),
                session_task_note_id.clone(),
                input.skip_auto_commit.unwrap_or(false),
                None,
                extra,
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

        // Track effective isolation mode for the result
        let mut effective_isolation: Option<&str> = None;

        // Provision sandbox if isolation=cow is requested (Task 3).
        // Check if isolation is "cow" (explicit or defaulted from workspace setting).
        // Default to "cow" if workspace.cowIsolation setting is enabled and no explicit
        // isolation parameter was provided (Task 5).
        let mut isolation = input.isolation.clone();
        if isolation.is_none() {
            // Check workspace.cowIsolation setting
            if let Ok(value) = self
                .settings_get("workspace.cowIsolation".to_string())
                .await
            {
                if value.as_bool().unwrap_or(false) {
                    isolation = Some("cow".to_string());
                }
            }
        }
        if isolation.as_deref() == Some("cow") {
            use crate::sandbox_ops::{provision_sandbox, ProvisionConfig, ProvisionOutcome};
            let workspace = self.store.get_workspace(&workspace_id).await.ok();
            if let Some(ws) = workspace {
                // Only direct-mode workspaces (no worktree or skip_worktree=true)
                let is_direct_mode = ws.skip_worktree || ws.worktree_path.is_none();
                if is_direct_mode && ws.repository_path.is_some() {
                    if let Some(root) = self.workspaces_root.clone() {
                        let config = ProvisionConfig {
                            workspaces_root: root,
                        };
                        let aid = AgentId::from(agent_id.as_str());
                        match provision_sandbox(&self.store, &workspace_id, &aid, &config).await {
                            Ok(ProvisionOutcome::Supported {
                                path,
                                branch,
                                base_commit_sha,
                                snapshot_commit_sha,
                            }) => {
                                // Update the session with sandbox metadata
                                if let Ok(mut session) = self.store.get_agent_session(&aid).await {
                                    session.sandbox_id = Some(format!(
                                        "sandbox-{}-{}",
                                        workspace_id.as_str(),
                                        aid.as_str()
                                    ));
                                    session.sandbox_path = Some(path.to_string_lossy().to_string());
                                    session.sandbox_branch = Some(branch.clone());
                                    let _ = self
                                        .store
                                        .update_agent_session(&workspace_id, &session)
                                        .await;

                                    // Track successful CoW provisioning
                                    effective_isolation = Some("cow");

                                    // Emit sandbox:created event
                                    crate::publish_event(
                                        &self.event_bus,
                                        intent_store::NewEvent {
                                            workspace_id: workspace_id.clone(),
                                            timestamp: crate::now_iso(),
                                            event_type: "sandbox:created".to_string(),
                                            actor: crate::system_actor(),
                                            session_id: Some(aid.0.clone()),
                                            correlation_id: None,
                                            parent_event_id: None,
                                            metadata: None,
                                            data: json!({
                                                "workspaceId": workspace_id.as_str(),
                                                "agentId": agent_id,
                                                "sandboxPath": path.to_string_lossy(),
                                                "branch": branch,
                                                "baseCommitSha": base_commit_sha,
                                                "snapshotCommitSha": snapshot_commit_sha,
                                            }),
                                        },
                                    )
                                    .await;
                                }
                            }
                            Ok(ProvisionOutcome::Unsupported) => {
                                // Fallback to shared mode (no action needed, session stays without sandbox fields)
                                effective_isolation = Some("direct");
                                tracing::debug!(
                                    workspace = %workspace_id,
                                    agent = %agent_id,
                                    "CoW not supported; fallback to shared mode"
                                );
                            }
                            Err(e) => {
                                effective_isolation = Some("direct");
                                tracing::warn!(
                                    workspace = %workspace_id,
                                    agent = %agent_id,
                                    error = %e,
                                    "Sandbox provisioning failed; fallback to shared mode"
                                );
                            }
                        }
                    }
                }
            }
        }

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
                        parent.clone(),
                        parent_name,
                        child,
                        false,
                        Some(gid),
                    );
                } else {
                    self.register_completion_watch(
                        &workspace_id,
                        parent.clone(),
                        parent_name,
                        child,
                        true,
                        None,
                    );
                }
                self.publish_subscriptions_changed(&workspace_id, &parent)
                    .await;
            }
        }
        // Deliver the child's first message (resolved above, persisted as
        // `metadata.initialMessage`) and start its turn (PROTOCOL §5.5).
        // Without this the child stays `Pending` and never runs. `wait_mode` is
        // already honored by the completion-watch registration above; the child
        // turn itself starts unconditionally.
        //
        // Delivery routes through the runtime `AgentManager` when attached (the
        // proven `agent.sendMessage` path: persist + spawn the turn worker, which
        // lazily spawns the child and streams `agent:stream:*` keyed by the CHILD
        // `agentId`); read-only/test wiring falls back to the store-only persist.
        if let Some(message) = message {
            let child = AgentId::from(agent_id.as_str());
            let send = match self.agent_manager() {
                Some(manager) => {
                    manager
                        .send_message(
                            child,
                            workspace_id,
                            message,
                            None,
                            crate::agent_manager::TurnOptions::default(),
                        )
                        .await
                }
                None => {
                    self.agent_send_message_op(child, message, None, None, None)
                        .await
                }
            };
            if let Err(e) = send {
                tracing::warn!(agent = %agent_id, error = %e, "delegate: failed to start child turn");
            }
        }

        // Include effective isolation in the result when isolation was requested
        let mut result = json!({ "ok": true, "agentId": agent_id, "name": name });
        if let Some(eff_iso) = effective_isolation {
            result
                .as_object_mut()
                .unwrap()
                .insert("effectiveIsolation".to_string(), json!(eff_iso));
        }
        Ok(result)
    }

    /// Auto-subscribe `parent_agent_id` to `child_agent_id`'s completion
    /// (AS-5, the MCP `create_agent` front door): register a oneShot watch,
    /// mirroring the immediate-mode branch of `agent_delegate_op` above —
    /// including the deleted-parent guard (TS `selectIsAgentDeleted`).
    /// Idempotent: reuses an existing watch when one already exists.
    pub(crate) async fn agent_watch_completion_op(
        &self,
        workspace_id: WorkspaceId,
        parent_agent_id: AgentId,
        child_agent_id: AgentId,
    ) -> Result<Value> {
        let parent_session = self.store.get_agent_session(&parent_agent_id).await.ok();
        let parent_deleted = parent_session
            .as_ref()
            .map(|s| s.status == AgentStatus::Deleted)
            .unwrap_or(false);
        if parent_deleted {
            return Ok(json!({ "ok": false, "subscriptionId": Value::Null }));
        }
        let parent_name = parent_session.as_ref().map(|s| s.name.clone());
        // Dedupe: reuse an existing oneShot watch if present, otherwise create new.
        let id = self
            .find_and_refresh_ungrouped_watch(
                &workspace_id,
                &parent_agent_id,
                &child_agent_id,
                true,
                parent_name.clone(),
            )
            .unwrap_or_else(|| {
                self.register_completion_watch(
                    &workspace_id,
                    parent_agent_id.clone(),
                    parent_name.unwrap_or_default(),
                    child_agent_id,
                    true,
                    None,
                )
            });
        self.publish_subscriptions_changed(&workspace_id, &parent_agent_id)
            .await;
        Ok(json!({ "ok": true, "subscriptionId": id }))
    }

    /// Conditionally auto-subscribe a coordination-message SENDER to the
    /// target's completion (SUB-1, the TS
    /// `maybeSubscribeCallerToAgentCompletionForCoordinationMessage`):
    /// register a oneShot caller→target watch UNLESS the caller is a
    /// delegated background task session — those often send sibling
    /// coordination messages, and passively subscribing them creates noisy
    /// wakeup cards unrelated to their own task. Idempotent: reuses an
    /// existing watch when one already exists.
    pub(crate) async fn agent_watch_completion_for_sender_op(
        &self,
        workspace_id: WorkspaceId,
        caller_agent_id: AgentId,
        target_agent_id: AgentId,
    ) -> Result<Value> {
        let caller_session = self.store.get_agent_session(&caller_agent_id).await.ok();
        let skip = caller_session
            .as_ref()
            .map(is_delegated_background_task_session)
            .unwrap_or(false);
        if skip {
            return Ok(json!({ "ok": false, "subscriptionId": Value::Null }));
        }
        // SUB-1 delegation-group conflict suppression: skip ungrouped watch
        // registration when the (caller, target) pair is already a member of
        // an undelivered after_all delegation group (mirrors the existing
        // child_in_undelivered_group suppression used for reportToParent wakes).
        // Prevents duplicate wakes when a coordinator sends coordination messages
        // (sendToTask) to children that are already covered by a grouped watch.
        if self.child_in_undelivered_group(&workspace_id, &caller_agent_id, &target_agent_id) {
            tracing::debug!(
                caller = %caller_agent_id.0,
                target = %target_agent_id.0,
                "skipping SUB-1 auto-watch — target already in undelivered after_all group"
            );
            return Ok(json!({ "ok": false, "subscriptionId": Value::Null }));
        }
        let caller_name = caller_session.as_ref().map(|s| s.name.clone());
        // Dedupe: reuse an existing oneShot watch if present, otherwise create new.
        let id = self
            .find_and_refresh_ungrouped_watch(
                &workspace_id,
                &caller_agent_id,
                &target_agent_id,
                true,
                caller_name.clone(),
            )
            .unwrap_or_else(|| {
                self.register_completion_watch(
                    &workspace_id,
                    caller_agent_id.clone(),
                    caller_name.unwrap_or_default(),
                    target_agent_id,
                    true,
                    None,
                )
            });
        self.publish_subscriptions_changed(&workspace_id, &caller_agent_id)
            .await;
        Ok(json!({ "ok": true, "subscriptionId": id }))
    }

    /// Remove `subscription_id` after `after` elapses (SUB-1 leak guard for
    /// the non-oneShot queued-message watch — mirrors the TS 5-minute
    /// `setTimeout` unsubscribe). A watch already removed by delivery is a
    /// no-op; only a real removal republishes the parent's subscriptions.
    ///
    /// SUB-2: repeated arm calls monotonically extend the effective deadline
    /// via [`Services::bump_watch_cleanup_deadline`]. Each spawned task then
    /// consults that shared deadline before deleting, so an earlier task
    /// waking first cannot remove a watch whose deadline was pushed out by a
    /// later call — the later task (spawned for the new deadline) is the one
    /// that performs the removal.
    pub(crate) fn spawn_watch_cleanup(
        &self,
        workspace_id: WorkspaceId,
        parent_agent_id: AgentId,
        subscription_id: String,
        after: std::time::Duration,
    ) -> bool {
        // Copilot #104 thread PRRT_kwDOS9Wxuc6QKWuM: only arm the cleanup
        // task when the deadline bump actually landed on a live watch —
        // otherwise the watch was removed concurrently (e.g. an oneShot
        // delivery raced this reuse, or a prior cleanup already expired)
        // and spawning a task that just sleeps and no-ops wastes a
        // tokio worker slot per repeated wake.
        let deadline = tokio::time::Instant::now() + after;
        if !self.bump_watch_cleanup_deadline(&workspace_id, &subscription_id, deadline) {
            return false;
        }
        let services = self.clone();
        tokio::spawn(async move {
            tokio::time::sleep(after).await;
            if services.remove_watch_if_deadline_passed(&workspace_id, &subscription_id) {
                services
                    .publish_subscriptions_changed(&workspace_id, &parent_agent_id)
                    .await;
            }
        });
        true
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

        // Finding F1/F3: use message-free summaries + lightweight stats for diagnostics.
        // Diagnostics needs session metadata and message counts/assistant-message presence,
        // but never needs full message bodies.
        let sessions = self
            .store
            .list_agent_session_summaries(&workspace_id)
            .await?;
        let message_stats = self
            .store
            .get_agent_session_message_stats(&workspace_id)
            .await?;
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
            // Use lightweight message stats instead of full hydration
            let (message_count_val, has_assistant) =
                message_stats.get(id).copied().unwrap_or((0, false));
            let message_count = if message_count_val > 0 {
                Some(message_count_val)
            } else {
                None
            };
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
                && !has_assistant;
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
    /// `priority: "interrupt"` preempts the assignee's in-flight turn keep-alive
    /// (never killing the child) and delivers immediately when the runtime
    /// manager is attached; other priorities keep the existing delivery.
    pub(crate) async fn agent_send_to_task_op(
        &self,
        workspace_id: WorkspaceId,
        task_note_id: NoteId,
        message: String,
        priority: Option<String>,
    ) -> Result<Value> {
        let task = self.get_my_task(workspace_id.clone(), task_note_id).await?;
        let Some(agent) = task.assigned_agents.first().cloned() else {
            return Ok(
                json!({ "ok": false, "delivered": false, "error": "No agent assigned to task" }),
            );
        };
        // DELIV-1: non-interrupt priority MUST also drive a real turn when
        // the runtime is attached — the store-only `agent_send_message_op`
        // fallback would persist the message without ever prompting the
        // assignee (the "coordinator send silently lost" bug). Mirror the
        // `agent_send_message` (WorkspaceApi) routing: the manager path
        // spawns the turn worker; only the read-only wiring with no
        // manager falls back to the store-only op.
        let result = match (
            self.agent_manager(),
            is_interrupt_priority(priority.as_deref()),
        ) {
            (Some(manager), true) => {
                manager
                    .interrupt_send_message(
                        agent.clone(),
                        workspace_id,
                        message,
                        None,
                        crate::agent_manager::TurnOptions::default(),
                    )
                    .await?
            }
            (Some(manager), false) => {
                manager
                    .send_message(
                        agent.clone(),
                        workspace_id,
                        message,
                        None,
                        crate::agent_manager::TurnOptions::default(),
                    )
                    .await?
            }
            (None, _) => {
                self.agent_send_message_op(agent.clone(), message, None, None, None)
                    .await?
            }
        };
        Ok(json!({ "ok": true, "agentId": agent, "result": result }))
    }

    /// `agent.wakeOrCreate` (PROTOCOL §5.5, widened by C1d-10a): resume the
    /// newest live/resumable agent assigned to the task, or — when none is
    /// found — create a new one with specialist/model inheritance from the
    /// most-recent previous session and the FE `WakeOrCreateTaskAgentTool`
    /// create payload (name, contextReferences, metadata, skipAutoCommit),
    /// then deliver the context message (optionally tagged with
    /// `messageMetadata`). Prunes stale assignments (`cleanedUpAgentIds`) and
    /// enforces `MAX_DELEGATION_DEPTH` when the caller provides
    /// `callerAgentId`/`delegationDepth`.
    pub(crate) async fn agent_wake_or_create_op(
        &self,
        workspace_id: WorkspaceId,
        task_note_id: NoteId,
        context_message: String,
        input: AgentWakeOrCreateInput,
    ) -> Result<Value> {
        // B3: delegation-depth guard. `parent_depth` mirrors the FE constant
        // (`MAX_DELEGATION_DEPTH = 2`, "error if parent >= 2" per the C1d-10
        // fence report). When neither `callerAgentId` nor `delegationDepth` is
        // provided the guard is a no-op (backward-compatible with the
        // pre-widening 3-param callers).
        let parent_depth = self.resolve_parent_delegation_depth(&input).await?;
        if let Some(depth) = parent_depth {
            if depth >= MAX_DELEGATION_DEPTH {
                return Err(Error::InvalidParams(format!(
                    "agent.wakeOrCreate: delegation depth {depth} exceeds \
                     MAX_DELEGATION_DEPTH ({MAX_DELEGATION_DEPTH})"
                )));
            }
        }

        let task = self
            .get_my_task(workspace_id.clone(), task_note_id.clone())
            .await?;
        let task_title = task.title.clone();

        // B1 + B2: iterate assigned_agent_ids newest-first (Vec::push
        // append-order means newest is the tail). Probe each session:
        //   * NotFound / Deleted → stale, queue for cleanup.
        //   * Otherwise → treat as resumable; the newest live session wins.
        // `inheritance_source` captures the newest **known** previous session
        // (live or deleted) so the create branch can still inherit
        // specialist/model when no live agent is available.
        let mut cleaned_up: Vec<AgentId> = Vec::new();
        let mut live_session: Option<AgentSession> = None;
        let mut inheritance_source: Option<AgentSession> = None;
        for candidate in task.assigned_agents.iter().rev().cloned() {
            match self.store.get_agent_session(&candidate).await {
                Ok(session) if session.status != AgentStatus::Deleted => {
                    if inheritance_source.is_none() {
                        inheritance_source = Some(session.clone());
                    }
                    live_session = Some(session);
                    break;
                }
                Ok(deleted_session) => {
                    if inheritance_source.is_none() {
                        inheritance_source = Some(deleted_session);
                    }
                    cleaned_up.push(candidate);
                }
                Err(Error::NotFound(_)) => cleaned_up.push(candidate),
                Err(e) => return Err(e),
            }
        }

        // B7: `messageMetadata` is applied to the delivered context message on
        // BOTH branches via `deliver_wake_message`.
        if let Some(session) = live_session {
            let agent_id = session.id.clone();
            let agent_name = session.name.clone();
            let agent_status = serde_json::to_value(session.status)
                .ok()
                .and_then(|v| v.as_str().map(String::from))
                .unwrap_or_default();
            let result = self
                .deliver_wake_message(
                    &workspace_id,
                    &agent_id,
                    &context_message,
                    input.message_metadata.as_ref(),
                )
                .await?;
            self.remove_agent_ids_from_workspace_tasks(&workspace_id, &cleaned_up)
                .await?;
            // B8: `action` distinguishes queued-to-active-agent from woke-existing
            // via the delivery's `queued` flag.
            let queued = result
                .get("queued")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            // Wave B: when the wake message was directly delivered (not queued),
            // force a queue drain so any previously-queued normal-priority messages
            // send immediately without waiting for an interrupt-priority nudge.
            // Post-restart recovery: a woken agent must resume pending work.
            if !queued {
                if let Some(mgr) = self.agent_manager() {
                    tokio::spawn({
                        let mgr = mgr.clone();
                        let agent_id = agent_id.clone();
                        let workspace_id = workspace_id.clone();
                        async move {
                            mgr.try_drain_queue(agent_id, workspace_id).await;
                        }
                    });
                }
            }
            let action = if queued {
                "message_queued_to_active_agent"
            } else {
                "woke_existing"
            };
            let mut response = build_wake_response(
                agent_id.clone(),
                agent_name,
                false,
                action,
                task_title.clone(),
                result,
                cleaned_up,
            );
            // SUB-1: auto-subscribe the waking caller to the target's
            // completion (TS `WakeOrCreateTaskAgentTool`). Woke-existing gets
            // a oneShot watch; a queued context message gets a NON-oneShot
            // watch (the target's `agent:idle` for its CURRENT turn fires
            // before the queued message is processed) with a 5-minute
            // auto-cleanup so the watch never leaks. Response text mirrors
            // the reference tool, including the notification line.
            //
            // SUB-2: repeated `wakeOrCreate` calls for the same caller/target
            // pair must not stack duplicate watches (which would multiply the
            // parent wakes on the next `agent:idle`). Reuse any existing live
            // ungrouped watch for this pair; for the queued branch, extend
            // its 5-minute leak-guard by respawning the cleanup timer against
            // the reused subscription id.
            if let Some(caller) = input.caller_agent_id.clone() {
                // SUB-2: only reuse a live ungrouped watch when its
                // `one_shot` mode matches this call. A queued wake needs a
                // non-oneShot watch (it must survive the assignee's current
                // `agent:idle`), so it must never inherit an existing
                // oneShot watch — and a non-queued wake must not degrade the
                // registry by re-observing an existing non-oneShot watch as
                // oneShot. Mismatched modes fall through to a fresh
                // `register_completion_watch`.
                let one_shot = !queued;
                // Resolve the caller's current display name up front so a
                // fresh watch is registered with it, and a reused watch has
                // its stored `parent_agent_name` refreshed against the same
                // source (SUB-2 Copilot #104): agents can rename via
                // `agent.rename` / `agent.update`, and `describe_subscription`
                // formats using `watch.parent_agent_name`, so a long-lived
                // reused watch would otherwise report a stale `agentName` /
                // `description` from `agent.getSubscriptions`.
                // Copilot #104 thread PRRT_kwDOS9Wxuc6QKWuU: keep the
                // resolved name as `Option<String>` so a failed session
                // lookup does not overwrite an existing watch's stored
                // `parent_agent_name` with an empty placeholder on the
                // reuse path. The reuse itself and the paired deadline bump
                // still proceed; only the fresh-register branch has to
                // materialize a name, and there `""` matches the pre-fix
                // behaviour for a brand-new watch.
                let caller_name = self
                    .store
                    .get_agent_session(&caller)
                    .await
                    .ok()
                    .map(|s| s.name);
                // SUB-2 (Copilot #104 follow-up, thread
                // PRRT_kwDOS9Wxuc6QKPyt): resolve reuse atomically. If a live
                // ungrouped watch is found, its `parent_agent_name` is
                // refreshed under the same lock when a fresh name was
                // resolved; otherwise we fall through to registering a fresh
                // watch. This closes the race where a concurrent oneShot
                // delivery or expired cleanup task removed the watch between
                // a prior find and refresh, which would otherwise leave the
                // caller subscribed to a dead id.
                let (subscription_id, reused) = if let Some(existing_id) = self
                    .find_and_refresh_ungrouped_watch(
                        &workspace_id,
                        &caller,
                        &agent_id,
                        one_shot,
                        caller_name.clone(),
                    ) {
                    (existing_id, true)
                } else {
                    let new_id = self.register_completion_watch(
                        &workspace_id,
                        caller.clone(),
                        caller_name.unwrap_or_default(),
                        agent_id.clone(),
                        one_shot,
                        None,
                    );
                    (new_id, false)
                };
                if !reused {
                    self.publish_subscriptions_changed(&workspace_id, &caller)
                        .await;
                }
                if queued {
                    self.spawn_watch_cleanup(
                        workspace_id.clone(),
                        caller,
                        subscription_id.clone(),
                        QUEUED_WATCH_CLEANUP,
                    );
                }
                let message = if queued {
                    format!(
                        "Agent \"{}\" is already actively working on task \"{task_title}\".\n\
                         Context message has been queued and will be delivered when the agent finishes its current response.\n\
                         You will be notified when the agent responds.",
                        agent_id.0
                    )
                } else {
                    format!(
                        "Woke existing agent \"{}\" for task \"{task_title}\".\n\
                         Agent status: {agent_status}\n\
                         Context message delivered.\nYou will be notified when the agent responds.",
                        agent_id.0
                    )
                };
                response["subscriptionId"] = json!(subscription_id);
                response["message"] = json!(message);
            }
            return Ok(response);
        }

        // Create branch: no live session. Purge stale assignments first so the
        // subsequent `assign_agent` starts from a clean list, then build the
        // rich create payload.
        self.remove_agent_ids_from_workspace_tasks(&workspace_id, &cleaned_up)
            .await?;
        let create_opts = input.create.clone().unwrap_or_default();

        // B4: specialist/model inheritance — the previous session's specialist
        // wins; the wake-level `model` override wins over both the previous
        // session's model and the `create.model` fallback.
        let specialist = inheritance_source
            .as_ref()
            .and_then(|s| s.specialist.clone())
            .or(create_opts.specialist.clone());
        let model = input
            .model
            .clone()
            .or_else(|| inheritance_source.as_ref().and_then(|s| s.model.clone()))
            .or(create_opts.model.clone());
        let provider = create_opts.provider.clone();
        let agent_type = create_opts.agent_type.clone();

        // B5: rich create payload (`name` default `Task: {title}`,
        // `contextReferences` + provenance metadata folded into the persisted
        // metadata blob so the daemon-side session read-back retains them).
        let name = Some(
            create_opts
                .name
                .clone()
                .unwrap_or_else(|| format!("Task: {task_title}")),
        );
        // B6: honor `create.skipAutoCommit` from the request; default `false`
        // preserves the pre-widening behavior.
        let skip_auto_commit = create_opts.skip_auto_commit.unwrap_or(false);
        let metadata = build_create_metadata(
            &create_opts,
            &input,
            &task_note_id,
            parent_depth,
            agent_type.clone(),
        );
        let extra = AgentCreateExtra {
            provider,
            agent_type,
            metadata,
            workspace_path: None,
            workspace_context: None,
            context_references: None,
            image_blocks: None,
            is_background: None,
            name_explicitly_set: None,
        };
        let created = self
            .agent_create_op(
                workspace_id.clone(),
                name,
                model,
                specialist,
                None,
                Some(task_note_id.clone()),
                skip_auto_commit,
                None,
                extra,
            )
            .await?;
        let agent_lite = &created["agent"];
        let agent_id_str = agent_lite
            .get("id")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let agent_name = agent_lite
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let agent = AgentId::from(agent_id_str.as_str());
        let _ = self
            .assign_agent(workspace_id.clone(), task_note_id, agent_id_str)
            .await;
        let result = self
            .deliver_wake_message(
                &workspace_id,
                &agent,
                &context_message,
                input.message_metadata.as_ref(),
            )
            .await?;
        Ok(build_wake_response(
            agent,
            agent_name,
            true,
            "created_new",
            task_title,
            result,
            cleaned_up,
        ))
    }

    /// Resolve the effective **parent** delegation depth for the
    /// `agent.wakeOrCreate` guard. `delegation_depth` on the wire wins when
    /// present (the FE surfaces it explicitly). Otherwise, when
    /// `caller_agent_id` is provided, read the caller's persisted
    /// `session.metadata.delegationDepth` (default `0`). Missing caller
    /// context → `None` (no guard).
    async fn resolve_parent_delegation_depth(
        &self,
        input: &AgentWakeOrCreateInput,
    ) -> Result<Option<i64>> {
        if let Some(depth) = input.delegation_depth {
            return Ok(Some(depth));
        }
        let Some(caller) = input.caller_agent_id.as_ref() else {
            return Ok(None);
        };
        match self.store.get_agent_session(caller).await {
            Ok(session) => Ok(Some(
                session
                    .metadata
                    .as_ref()
                    .and_then(|m| m.get("delegationDepth"))
                    .and_then(Value::as_i64)
                    .unwrap_or(0),
            )),
            Err(Error::NotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// `agent.wakeOrCreate` context-message delivery (both branches). When
    /// `message_metadata` is `Some`, it is folded onto the persisted text
    /// block as `messageMetadata` so subscribers/`agent.getConversation`
    /// consumers see the FE tag (`{type:'task_wake', source, taskNoteId,
    /// callerAgentId}`) verbatim; when `None`, the block matches the plain
    /// `agent.sendMessage` shape.
    ///
    /// **Runtime drive (DELIV-1).** When the [`AgentManager`] is attached,
    /// the context message is delivered as a real turn (the proven
    /// `agent.sendMessage` shape: try to claim the in-flight slot, persist
    /// the wake-tagged user block, spawn the turn worker) so the newly
    /// woken/created agent actually processes the message. A busy assignee
    /// gets the wake enqueued behind the running turn — its drain loop
    /// picks the message up at turn end (the FE `messageMetadata` tag is
    /// lost from the queued re-persist on this rare in-flight path, but
    /// turn delivery is preserved). Read-only/test wiring with no manager
    /// falls back to the pre-DELIV-1 store-only persist so hermetic tests
    /// keep working. Auto-queue-on-store-failure mirrors
    /// [`Services::agent_send_message_op`].
    async fn deliver_wake_message(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        content: &str,
        message_metadata: Option<&Value>,
    ) -> Result<Value> {
        let build_block = || match message_metadata {
            Some(md) => json!({ "type": "text", "text": content, "messageMetadata": md }),
            None => json!({ "type": "text", "text": content }),
        };
        let Some(manager) = self.agent_manager() else {
            return self
                .deliver_wake_message_store_only(workspace_id, agent_id, content, build_block)
                .await;
        };
        // Runtime path (DELIV-1): two-step claim/persist/spawn so the
        // user-message row is on disk BEFORE the turn worker starts, and no
        // worker is ever spawned for a row that failed to persist:
        //   1. Claim the in-flight slot (busy assignee → enqueue branch).
        //   2. Persist the wake-tagged user block. Slot released on failure
        //      and the message enqueued, then a best-effort
        //      `AgentManager::try_drain_queue` kick so the queued wake is
        //      picked up while the agent is idle (mirrors the
        //      `AgentManager::send_message` auto-queue fallback).
        //   3. Spawn the worker with the same content in-memory (the worker
        //      path does not re-persist).
        let content_owned = content.to_string();
        if !manager.try_begin_turn(agent_id, workspace_id).await {
            // Fast enqueue branch: the manager is already draining a turn.
            let (queued, position) = self.enqueue_message(agent_id, content_owned, None, None);
            self.publish_queue_updated(agent_id).await;
            return Ok(json!({
                "success": true,
                "queued": true,
                "queuedMessage": queued.to_value(position),
            }));
        }
        let blocks = json!([build_block()]);
        let created_at = now_iso();
        let message = match self
            .store
            .append_agent_message(agent_id, "user", &blocks, &created_at)
            .await
        {
            Ok(msg) => msg,
            Err(_) => {
                manager.release_slot(agent_id).await;
                let (queued, position) = self.enqueue_message(agent_id, content_owned, None, None);
                self.publish_queue_updated(agent_id).await;
                manager
                    .clone()
                    .try_drain_queue(agent_id.clone(), workspace_id.clone())
                    .await;
                return Ok(json!({
                    "success": true,
                    "queued": true,
                    "queuedMessage": queued.to_value(position),
                }));
            }
        };
        // Refresh agent_session.updated_at so the FE agent-card timestamp
        // reflects message activity, not just status transitions (STAB-19).
        if let Err(e) = self
            .store
            .refresh_agent_session_timestamp(workspace_id, agent_id, &created_at)
            .await
        {
            tracing::warn!(agent = %agent_id, error = %e, "refresh_agent_session_timestamp failed");
        } else {
            // Schedule debounced lastActivity event (§10.1).
            self.schedule_last_activity_event(workspace_id.clone());
        }
        // Publish agent:message event using the store-returned message id.
        self.publish_agent_mutation_event(
            workspace_id,
            agent_id,
            AGENT_MESSAGE,
            json!({ "agentId": agent_id.0, "messageId": message.id, "role": message.role }),
        )
        .await;
        manager.clone().finish_prepersisted_turn_spawn(
            agent_id.clone(),
            workspace_id.clone(),
            content_owned,
            crate::agent_manager::TurnOptions::default(),
        );
        Ok(json!({ "success": true, "queued": false, "messageId": message.id }))
    }

    /// Pre-DELIV-1 store-only delivery for the read-only/test path (no
    /// [`AgentManager`] attached): persist the wake-tagged block, and on
    /// store failure fall back to an in-memory enqueue with `queued: true`.
    async fn deliver_wake_message_store_only<F>(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        content: &str,
        build_block: F,
    ) -> Result<Value>
    where
        F: Fn() -> Value,
    {
        let blocks = json!([build_block()]);
        let created_at = now_iso();
        match self
            .store
            .append_agent_message(agent_id, "user", &blocks, &created_at)
            .await
        {
            Ok(message) => {
                // Refresh agent_session.updated_at so the FE agent-card timestamp
                // reflects message activity, not just status transitions (STAB-19).
                if let Err(e) = self
                    .store
                    .refresh_agent_session_timestamp(workspace_id, agent_id, &created_at)
                    .await
                {
                    tracing::warn!(agent = %agent_id, error = %e, "refresh_agent_session_timestamp failed");
                }
                // Publish agent:message event using the store-returned message id.
                self.publish_agent_mutation_event(
                    workspace_id,
                    agent_id,
                    AGENT_MESSAGE,
                    json!({ "agentId": agent_id.0, "messageId": message.id, "role": message.role }),
                )
                .await;
                Ok(json!({ "success": true, "queued": false, "messageId": message.id }))
            }
            Err(_) => {
                let (queued, position) =
                    self.enqueue_message(agent_id, content.to_string(), None, None);
                let result = json!({
                    "success": true,
                    "queued": true,
                    "queuedMessage": queued.to_value(position),
                });
                self.publish_queue_updated(agent_id).await;
                Ok(result)
            }
        }
    }

    /// Daemon-side equivalent of the FE `task.removeAgentFromAllTasks`: strip
    /// the given agent ids from every task note in the workspace. Silent on
    /// notes/tasks that never referenced the ids so it is safe to call with an
    /// empty or partially-stale list.
    async fn remove_agent_ids_from_workspace_tasks(
        &self,
        workspace_id: &WorkspaceId,
        stale: &[AgentId],
    ) -> Result<()> {
        if stale.is_empty() {
            return Ok(());
        }
        let notes = self.store.list_notes(workspace_id).await?;
        for mut note in notes {
            let Some(mut task) = note.metadata.task.clone() else {
                continue;
            };
            let before = task.assigned_agent_ids.len();
            task.assigned_agent_ids.retain(|a| !stale.contains(a));
            if task.assigned_agent_ids.len() == before {
                continue;
            }
            let now = now_iso();
            note.metadata.task = Some(task);
            note.updated_at = now;
            self.store.update_note(&note).await?;
        }
        Ok(())
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
    /// its 0-based `position` in the queue (the index just appended). New messages
    /// are always ready-to-send (`editing = false`) — the FE may transition an
    /// entry to `editing = true` later via `agent.editQueuedMessage`.
    pub(crate) fn enqueue_message(
        &self,
        agent_id: &AgentId,
        content: String,
        image_blocks: Option<Value>,
        file_blocks: Option<Value>,
    ) -> (QueuedMessage, usize) {
        let queued = QueuedMessage {
            id: new_message_id(),
            content,
            image_blocks,
            file_blocks,
            queued_at: now_iso(),
            editing: false,
            persisted: false,
            requeued_after_failure: false,
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

    /// Pop the oldest **ready-to-send** queued message for an agent, if any. Used
    /// by the runtime turn loop to flip a queued message to in-flight when the
    /// current turn ends. Entries with `editing = true` are skipped (left in
    /// place) so the agent stays idle only when *every* remaining entry is under
    /// edit (PROTOCOL §5.5/§6.5 invariant).
    pub(crate) fn dequeue_message(&self, agent_id: &AgentId) -> Option<QueuedMessage> {
        let mut guard = self
            .agent_queues
            .lock()
            .expect("agent queue registry poisoned");
        let queue = guard.get_mut(agent_id)?;
        let idx = queue.iter().position(|m| !m.editing)?;
        Some(queue.remove(idx))
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

    /// `true` iff the agent has at least one queued message that is **not**
    /// under edit (i.e. the "ready-to-send" queue is non-empty). Drives the
    /// self-drain trigger and gates `agent:idle` emission so the agent never
    /// reports idle while ready-to-send work remains (PROTOCOL §5.5/§6.5).
    pub(crate) fn has_ready_to_send(&self, agent_id: &AgentId) -> bool {
        self.agent_queues
            .lock()
            .expect("agent queue registry poisoned")
            .get(agent_id)
            .map(|q| q.iter().any(|m| !m.editing))
            .unwrap_or(false)
    }

    /// Drop all queued messages for an agent (used by `agent.forceMessage`,
    /// which supersedes the queue with the forced message). Returns `true` iff
    /// the queue previously held at least one message — the caller uses this to
    /// decide whether to publish `agent:queue:updated`.
    pub(crate) fn clear_queue(&self, agent_id: &AgentId) -> bool {
        let mut guard = self
            .agent_queues
            .lock()
            .expect("agent queue registry poisoned");
        let had = guard.get(agent_id).map(|q| !q.is_empty()).unwrap_or(false);
        guard.remove(agent_id);
        had
    }

    /// Snapshot the current queue contents as wire-shape `QueuedMessage` JSON
    /// (the §5.5 `{id, content, queuedAt, position, imageBlocks?, fileBlocks?}` shape) for
    /// `agent.getQueue` and the `agent:queue:updated` payload (§6).
    pub(crate) fn queue_snapshot(&self, agent_id: &AgentId) -> Vec<Value> {
        self.agent_queues
            .lock()
            .expect("agent queue registry poisoned")
            .get(agent_id)
            .map(|q| q.iter().enumerate().map(|(i, m)| m.to_value(i)).collect())
            .unwrap_or_default()
    }

    /// Publish `agent:queue:updated` with the **current** queue snapshot.
    /// Looks up the owning workspace from the agent session — when the session
    /// row is missing (e.g. an idempotent remove on an unknown agent) or no bus
    /// is wired, the call is a quiet no-op rather than an error: the durable
    /// mutation is the source of truth and a missing event is not fatal.
    ///
    /// The queue snapshot is taken **outside** the mutex it lives behind, but
    /// since this method only reads (under a brief lock that is dropped before
    /// the await) it never holds the queue lock across an `await` point.
    pub(crate) async fn publish_queue_updated(&self, agent_id: &AgentId) {
        let queue = self.queue_snapshot(agent_id);
        let workspace_id = match self.store.get_agent_session(agent_id).await {
            Ok(s) => s.workspace_id,
            Err(_) => return,
        };
        self.publish_queue_updated_for(agent_id, &workspace_id, queue)
            .await;
    }

    /// Like [`publish_queue_updated`] but takes the workspace id directly —
    /// used by call sites (the turn worker, `force_message`) that already hold
    /// it, avoiding a redundant `get_agent_session` round-trip per drain step.
    pub(crate) async fn publish_queue_updated_for(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        queue: Vec<Value>,
    ) {
        let event = intent_store::NewEvent {
            workspace_id: workspace_id.clone(),
            timestamp: now_iso(),
            event_type: AGENT_QUEUE_UPDATED.to_string(),
            actor: EventActor {
                actor_type: ActorType::Agent,
                id: Some(agent_id.0.clone()),
                ..Default::default()
            },
            session_id: Some(agent_id.0.clone()),
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data: json!({
                "agentId": agent_id.0,
                "queue": queue,
            }),
        };
        crate::publish_event(&self.event_bus, event).await;
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

/// Parse an optional string field in an `agent.update` `changes` object. A JSON
/// `null` clears the underlying `Option<String>`; a JSON string sets it to
/// `Some(_)`; any other value type is `-32602` (matching the trait's
/// [`Error::InvalidParams`] contract). Reused by every optional-string field in
/// [`Services::agent_update_op`] so the diagnostic wording stays uniform.
fn update_optional_string(value: &Value, field: &str) -> Result<Option<String>> {
    match value {
        Value::Null => Ok(None),
        Value::String(s) => Ok(Some(s.clone())),
        _ => Err(Error::InvalidParams(format!(
            "agent.update: `{field}` must be a string or null"
        ))),
    }
}

/// Reject transcript entries whose `role` is not one of the four wire values
/// (`user` | `assistant` | `tool` | `system`). Shared by `agent.appendMessage`
/// and `agent.replaceMessages` so callers cannot smuggle bogus roles that would
/// break the message-log invariant.
fn validate_message_role(role: &str) -> Result<()> {
    match role {
        "user" | "assistant" | "tool" | "system" => Ok(()),
        _ => Err(Error::InvalidParams(format!(
            "invalid message role `{role}` (expected one of user|assistant|tool|system)"
        ))),
    }
}

impl Services {
    /// Resume an interrupted agent (INT-41 phase 2): atomically claim the row, re-register
    /// parent completion watch when delegated, then deliver a continuation message.
    ///
    /// Claim-then-act semantics prevent concurrent resume races (exactly-one-winner).
    /// If any post-claim step fails (agent_send_message, session lookup), the row is
    /// reset to pending (resolution=NULL) to restore retryability, and the error is
    /// returned loudly.
    pub async fn resume_interrupted_agent(&self, agent_id: &AgentId) -> Result<()> {
        // Verify the agent is in pending interrupted state (O(1) query)
        let interrupted = self
            .store
            .get_interrupted_agent(agent_id)
            .await?
            .ok_or_else(|| {
                Error::InvalidParams(format!(
                    "Agent {agent_id} is not in pending interrupted state"
                ))
            })?;

        let workspace_id = interrupted.workspace_id.clone();

        // ATOMIC CLAIM: mark the row as resumed FIRST. If another process already claimed
        // it, set_interrupted_resolution returns false and we bail loudly. This prevents
        // concurrent resume races (e.g., --resume-all vs client resolveInterrupted).
        let updated = self
            .store
            .set_interrupted_resolution(agent_id, "resumed", &now_iso())
            .await?;
        if !updated {
            return Err(Error::InvalidParams(format!(
                "Agent {agent_id} is not in pending interrupted state (already resolved)"
            )));
        }

        // Helper to reset the row to pending if post-claim steps fail
        let reset_to_pending = || async {
            if let Err(e) = self.store.reset_interrupted_resolution(agent_id).await {
                tracing::warn!(
                    "Failed to reset interrupted_agent row to pending for {}: {}",
                    agent_id,
                    e
                );
            }
        };

        // Rehydrate delegation groups for this workspace (idempotent, best-effort).
        // Groups are sealed on rehydration since the original parent turn is gone.
        // Corrupted rows are logged at warn but do NOT fail the resume: the agent can
        // still proceed (group wake degrades to per-child) rather than failing entirely.
        if let Err(e) = self.rehydrate_delegation_groups(&workspace_id).await {
            tracing::warn!(
                "rehydrate_delegation_groups failed for workspace {}: {}. \
                 Agent will resume but after_all fan-in may degrade to per-child wakes.",
                workspace_id.0,
                e
            );
        }

        // Load the session to check for delegation metadata
        let session = match self.store.get_agent_session(agent_id).await {
            Ok(s) => s,
            Err(e) => {
                reset_to_pending().await;
                return Err(e);
            }
        };

        // Re-register parent completion watch if the agent was delegated
        if let Some(parent_id) = &session.parent_agent_id {
            // Extract createdByAgentId from metadata if present
            let created_by = session
                .metadata
                .as_ref()
                .and_then(|m| m.get("createdByAgentId"))
                .and_then(|v| v.as_str())
                .map(AgentId::from);

            if let Some(parent) = created_by.or_else(|| Some(parent_id.clone())) {
                // Fetch parent session for name
                let parent_name = self
                    .store
                    .get_agent_session(&parent)
                    .await
                    .ok()
                    .map(|s| s.name)
                    .unwrap_or_default();

                // Check if this agent is in a rehydrated delegation group
                let groups = self.list_groups_for_parent(&workspace_id, &parent);
                let group_id = groups
                    .iter()
                    .find(|g| g.expected_agent_ids.contains(agent_id))
                    .map(|g| g.group_id.clone());

                if let Some(gid) = group_id {
                    // Register grouped completion watch for after_all fan-in
                    let _sub_id = self.register_completion_watch(
                        &workspace_id,
                        parent,
                        parent_name,
                        agent_id.clone(),
                        false,
                        Some(gid),
                    );
                } else {
                    // Register ungrouped completion watch (dedupe via find_and_refresh)
                    let _sub_id = self
                        .find_and_refresh_ungrouped_watch(
                            &workspace_id,
                            &parent,
                            agent_id,
                            true,
                            Some(parent_name.clone()),
                        )
                        .unwrap_or_else(|| {
                            self.register_completion_watch(
                                &workspace_id,
                                parent,
                                parent_name,
                                agent_id.clone(),
                                true,
                                None,
                            )
                        });
                }
            }
        }

        // Deliver continuation message
        let continuation =
            "intentd restarted while you were working. Review your last steps and continue the task.";

        // Use the agent_send_message machinery to deliver the message
        // (lazily respawns provider and resumes via ACP session/load)
        if let Err(e) = self
            .agent_send_message(
                workspace_id.clone(),
                agent_id.clone(),
                continuation.to_string(),
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
            )
            .await
        {
            reset_to_pending().await;
            return Err(e);
        }

        Ok(())
    }

    /// Abandon an interrupted agent (INT-41 phase 2): mark row `abandoned`, append
    /// a system interruption message to the log, emit chat/agent events for live UIs.
    pub(crate) async fn abandon_interrupted_agent(&self, agent_id: &AgentId) -> Result<()> {
        // Verify the agent is in pending interrupted state (O(1) lookup)
        let interrupted = self
            .store
            .get_interrupted_agent(agent_id)
            .await?
            .ok_or_else(|| {
                Error::InvalidParams(format!(
                    "Agent {agent_id} is not in pending interrupted state"
                ))
            })?;

        let workspace_id = interrupted.workspace_id.clone();

        // Rehydrate delegation groups for this workspace (idempotent, best-effort).
        let _ = self.rehydrate_delegation_groups(&workspace_id).await;

        // Load the session to check for delegation group membership
        let session = self.store.get_agent_session(agent_id).await.ok();
        if let Some(session) = &session {
            if let Some(parent_id) = &session.parent_agent_id {
                // Check if this agent is in a delegation group
                let groups = self.list_groups_for_parent(&workspace_id, parent_id);
                for group in groups {
                    if group.expected_agent_ids.contains(agent_id) {
                        // Record this child as deleted in the group (AS-4 abandoned child path).
                        // Build a minimal deleted-agent event for group completeness tracking.
                        let deleted_event = Event {
                            id: format!("abandon-{}", agent_id.0),
                            workspace_id: workspace_id.clone(),
                            timestamp: now_iso(),
                            event_type: AGENT_DELETED.to_string(),
                            actor: EventActor {
                                actor_type: ActorType::System,
                                id: Some("intentd".to_string()),
                                name: Some("intentd".to_string()),
                                ..Default::default()
                            },
                            session_id: Some(agent_id.0.clone()),
                            correlation_id: None,
                            parent_event_id: None,
                            metadata: None,
                            data: json!({ "agentId": agent_id.0 }),
                        };
                        self.record_group_child_completion(
                            &workspace_id,
                            &group.group_id,
                            agent_id,
                            true,
                            format!("Agent {} abandoned during restart", agent_id.0),
                            deleted_event,
                        )
                        .await;
                    }
                }
            }
        }

        // Build the system interruption message
        let text = "This conversation was interrupted because intentd restarted. The agent's in-flight work was terminated.";
        let content = json!([{
            "type": "text",
            "text": text,
            "meta": { "kind": "interruption" }
        }]);

        // Append the system message
        let message = self
            .store
            .append_agent_message(agent_id, "system", &content, &now_iso())
            .await?;

        // Mark the interrupted_agent row as resolved
        let updated = self
            .store
            .set_interrupted_resolution(agent_id, "abandoned", &now_iso())
            .await?;
        if !updated {
            return Err(Error::InvalidParams(format!(
                "Agent {agent_id} is not in pending interrupted state"
            )));
        }

        // Emit agent:message event so live UIs see the new message
        self.publish_agent_mutation_event(
            &workspace_id,
            agent_id,
            AGENT_MESSAGE,
            json!({ "agentId": agent_id.0, "messageId": message.id, "role": "system" }),
        )
        .await;

        Ok(())
    }
}
