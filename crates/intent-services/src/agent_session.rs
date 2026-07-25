//! Agent session driver: drive an ACP turn and route streaming updates onto the
//! M2 event bus (§6.5/§6.6).
//!
//! `intent-acp` owns the wire (session lifecycle + pure `session/update` →
//! [`MappedUpdate`] mapping); this module owns the side effects it cannot: it
//! publishes the mapped updates onto the [`EventBus`] (append-then-broadcast, so
//! subscribed clients receive `events.event`) and accumulates the assistant
//! transcript into the append-only `agent_message` log. Exactly one terminal
//! `agent:stream:end` is emitted per turn — `complete` and `error` both map to it
//! (PROTOCOL §7).

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use intent_acp::session::{
    self, ContentBlock, InitializeResponse, MappedToolCall, MappedUpdate, McpServer, Meta,
    SessionModeState, StopReason,
};
use intent_acp::{Connection, IncomingNotification};
use intent_core::events::{
    AGENT_FAILED, AGENT_IDLE, AGENT_STREAM_CHUNK, AGENT_STREAM_END, AGENT_STREAM_STATUS,
    AGENT_TOOL_CALL,
};
use intent_core::{
    now_epoch_ms, now_iso, ActorType, AgentId, Error, EventActor, Result, WorkspaceId,
};
use intent_store::NewEvent;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::Services;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod tests_meta;

/// Result of opening or resuming an ACP session: the canonical `acpSessionId`
/// (persisted on `AgentSession`) plus the modes the provider advertised in the
/// same response, so the caller can pick a permissive `session/set_mode` target
/// only from `availableModes` rather than blindly asking for a mode the agent
/// never offered.
#[derive(Debug, Clone)]
pub struct AcpSessionOpened {
    /// The canonical `acpSessionId` to drive future turns.
    pub session_id: String,
    /// The modes the provider advertised in `session/new` / `session/load`, if
    /// any. `None` when the provider omitted the field or when a concurrent
    /// recreate won the CAS (the modes we captured belong to the wrong
    /// session).
    pub modes: Option<SessionModeState>,
}

/// Accumulates streamed assistant content into one transcript message per turn,
/// coalescing consecutive text chunks into a single text block and pushing
/// `tool_use`/`tool_result` blocks for tool calls (CS-0 D6). Every block is
/// stamped with a stable id `{messageId}:{blockIndex}` (CS-0 D1), where
/// `messageId` is the assistant `AgentMessage` id minted at turn start; blocks
/// are append-only so a block's index (and thus id) is fixed once assigned.
struct Transcript {
    /// Assistant `AgentMessage` id minted at turn start (the block-id prefix).
    message_id: String,
    blocks: Vec<Value>,
    text: String,
    /// `toolCallId` → index of its `tool_use` block (for status patching).
    tool_use_index: HashMap<String, usize>,
    /// `toolCallId` → index of its `tool_result` block (append-once, then patch).
    tool_result_index: HashMap<String, usize>,
    /// `toolCallId` → index of its standalone proposal-resource block (§7.1;
    /// append-once, then patch — mirrors `tool_result_index`).
    proposal_index: HashMap<String, usize>,
}

impl Transcript {
    fn new(message_id: String) -> Self {
        Self {
            message_id,
            blocks: Vec::new(),
            text: String::new(),
            tool_use_index: HashMap::new(),
            tool_result_index: HashMap::new(),
            proposal_index: HashMap::new(),
        }
    }

    /// The stable block id for a 0-based block index (`{messageId}:{index}`).
    fn block_id(&self, index: usize) -> String {
        format!("{}:{index}", self.message_id)
    }

    /// Index the currently pending (or next) coalesced text block will occupy
    /// once flushed — the same value for every consecutive text chunk, so they
    /// share one block id.
    fn current_text_index(&self) -> usize {
        self.blocks.len()
    }

    fn push_text(&mut self, t: &str) {
        self.text.push_str(t);
    }

    /// Push a non-text passthrough content block, stamping its id; returns its
    /// index.
    fn push_block(&mut self, mut block: Value) -> usize {
        self.flush_text();
        let index = self.blocks.len();
        if let Some(obj) = block.as_object_mut() {
            obj.insert("id".to_string(), Value::String(self.block_id(index)));
        }
        self.blocks.push(block);
        index
    }

    fn flush_text(&mut self) {
        if !self.text.is_empty() {
            let index = self.blocks.len();
            let id = self.block_id(index);
            self.blocks
                .push(json!({ "type": "text", "id": id, "text": std::mem::take(&mut self.text) }));
        }
    }

    /// Record a tool call into the transcript (CS-0 D6). On first sight of a
    /// `toolCallId`, flush any open text and push a `tool_use` block; on repeats,
    /// patch its `metadata.status`. When the tool reaches `completed`/`error`
    /// WITH output, append (then patch) a matching `tool_result` block; when a
    /// `completed` (not `error`) output carries a proposal-MIME resource item
    /// (§7.1), a standalone proposal-resource block is additionally appended
    /// right after the `tool_result` (the resource stays in
    /// `tool_result.output` too). Returns
    /// `Some(index)` of the `tool_use` block (the block the `agent:tool:call`
    /// event is enriched against), or `None` when the update was dropped —
    /// callers must skip event publishing for dropped updates.
    ///
    /// STAB-124: a first-sight update whose derived name is empty is DROPPED
    /// (returns `None`, nothing recorded). This is the stale shape a cancelled
    /// child echoes after an interrupt — a title-less `tool_call_update` for a
    /// toolCallId the (fresh) transcript never saw. Fabricating a `tool_use`
    /// block from it persists an anonymous block (`name: ""`) that breaks FE
    /// conversation loading. Known-id patching is unaffected.
    ///
    /// `registered` is the canonical resource-item batch claimed from the
    /// turn-attachment registry (§7.1 deterministic attach) for this
    /// completed call, if any. On a registry hit the batch is attached
    /// directly and echo parsing is skipped; otherwise the legacy
    /// lift/wrap-repair fallback inspects the echoed output.
    fn record_tool(&mut self, tc: &MappedToolCall, registered: Vec<Value>) -> Option<usize> {
        let use_index = match self.tool_use_index.get(&tc.tool_call_id) {
            Some(&i) => {
                if let Some(meta) = self.blocks[i]
                    .get_mut("metadata")
                    .and_then(Value::as_object_mut)
                {
                    meta.insert("status".to_string(), Value::String(tc.status.to_string()));
                }
                i
            }
            None if tc.tool_name.trim().is_empty() => return None,
            None => {
                self.flush_text();
                let index = self.blocks.len();
                let id = self.block_id(index);
                // Shared factory (§7.1): keeps the persisted block
                // byte-identical to the live `chat.subscribe` delta.
                self.blocks.push(crate::tool_block::build_tool_use_block(
                    &id,
                    &tc.tool_name,
                    &tc.title,
                    tc.input.clone(),
                    &tc.tool_call_id,
                    tc.tool_kind,
                    tc.status,
                ));
                self.tool_use_index.insert(tc.tool_call_id.clone(), index);
                index
            }
        };
        let completed = tc.status == "completed" || tc.status == "error";
        if completed {
            if let Some(output) = &tc.output {
                let is_error = tc.status == "error";
                match self.tool_result_index.get(&tc.tool_call_id) {
                    Some(&ri) => {
                        if let Some(obj) = self.blocks[ri].as_object_mut() {
                            obj.insert("output".to_string(), output.clone());
                            obj.insert("is_error".to_string(), Value::Bool(is_error));
                        }
                    }
                    None => {
                        self.flush_text();
                        let rindex = self.blocks.len();
                        let rid = self.block_id(rindex);
                        self.blocks.push(json!({
                            "type": "tool_result",
                            "id": rid,
                            "tool_use_id": tc.tool_call_id,
                            "output": output,
                            "is_error": is_error,
                        }));
                        self.tool_result_index
                            .insert(tc.tool_call_id.clone(), rindex);
                    }
                }
                // §7.1: attach the standalone resource block(s) so the FE can
                // render them directly (the items also stay in
                // `tool_result.output` when the provider echoed them). The
                // registry-claimed canonical batch wins (deterministic attach —
                // no echo parsing); otherwise fall back to lifting a
                // proposal-MIME resource item out of the echoed output.
                // Gated on `completed` only — an errored tool must not surface
                // an actionable ProposalCard. Asymmetry: a re-completion whose
                // output DROPS the item leaves a previously appended block in
                // place (the transcript is append-only; index-derived ids
                // preclude removal).
                if tc.status == "completed" {
                    let items = if registered.is_empty() {
                        crate::tool_block::lift_proposal_resource(output)
                            .into_iter()
                            .collect()
                    } else {
                        registered
                    };
                    for (i, item) in items.into_iter().enumerate() {
                        // The first item upserts via `proposal_index` (patch
                        // on re-completion); batch extras append. A claim
                        // consumes its registry batch, so extras cannot
                        // re-attach on a re-completion echo.
                        match (i == 0)
                            .then(|| self.proposal_index.get(&tc.tool_call_id))
                            .flatten()
                        {
                            Some(&pi) => {
                                let id = self.block_id(pi);
                                self.blocks[pi] =
                                    crate::tool_block::build_proposal_resource_block(&id, &item);
                            }
                            None => {
                                self.flush_text();
                                let pindex = self.blocks.len();
                                let pid = self.block_id(pindex);
                                self.blocks
                                    .push(crate::tool_block::build_proposal_resource_block(
                                        &pid, &item,
                                    ));
                                if i == 0 {
                                    self.proposal_index.insert(tc.tool_call_id.clone(), pindex);
                                }
                            }
                        }
                    }
                }
            }
        }
        Some(use_index)
    }

    /// The recorded tool name for a known `toolCallId` (from its `tool_use`
    /// block), or `None` when the id was never seen. ACP `tool_call_update`s
    /// are name-less — the name only arrives on the first `tool_call` — so
    /// the completion-side registry claim resolves the name here.
    fn tool_name_for(&self, tool_call_id: &str) -> Option<&str> {
        let &i = self.tool_use_index.get(tool_call_id)?;
        self.blocks[i].get("name").and_then(Value::as_str)
    }

    fn into_blocks(mut self) -> Vec<Value> {
        self.flush_text();
        self.blocks
    }

    /// A non-consuming snapshot of the coalesced blocks AS THEY STAND mid-turn
    /// (CS-0 D5): the pushed blocks plus, when text is pending, the synthetic
    /// `text` block it will flush into (same index/id it would ultimately take).
    /// Used to publish the in-flight partial into the per-agent live-turn slot so
    /// a `chat.subscribe` arriving mid-turn can reconstruct it.
    fn snapshot_blocks(&self) -> Vec<Value> {
        let mut blocks = self.blocks.clone();
        if !self.text.is_empty() {
            let index = blocks.len();
            blocks.push(json!({
                "type": "text",
                "id": self.block_id(index),
                "text": self.text.clone(),
            }));
        }
        blocks
    }
}

/// The per-agent in-flight ("live") turn slot (CS-0 D5): the assistant message
/// id minted at turn start plus a non-consuming [`Transcript::snapshot_blocks`]
/// of the partial assistant content AS IT STANDS. Published while a
/// `session/prompt` turn streams so a `chat.subscribe` arriving mid-turn can
/// reconstruct the in-flight message; cleared (by [`LiveTurnGuard`] and on the
/// happy path before `stream:end`) once the turn's message is persisted.
#[derive(Clone)]
pub(crate) struct LiveTurn {
    pub(crate) message_id: String,
    pub(crate) blocks: Vec<Value>,
    /// RFC-3339 timestamp of the most recent stream event observed for this
    /// turn (STAB-125): set when the slot opens and refreshed on every
    /// [`update_live_turn`](Services::update_live_turn), so pollers can tell a
    /// long-but-alive turn from a wedged agent even before anything persists.
    pub(crate) last_activity_at: String,
}

/// Per-agent map of live turn slots, shared across [`Services`] clones so the
/// `chat.subscribe` read door and the [`run_prompt_turn`](Services::run_prompt_turn)
/// writer observe the same state.
pub(crate) type LiveTurns = Arc<Mutex<HashMap<AgentId, LiveTurn>>>;

/// RAII guard that clears an agent's live-turn slot when a turn ends — including
/// the interrupt/abort path, where the worker future is dropped before
/// `stream:end` is reached. Without it an aborted turn would leave a stale
/// in-flight message in the snapshot forever.
pub(crate) struct LiveTurnGuard<'a> {
    live_turns: &'a LiveTurns,
    agent_id: AgentId,
}

impl Drop for LiveTurnGuard<'_> {
    fn drop(&mut self) {
        if let Ok(mut slots) = self.live_turns.lock() {
            slots.remove(&self.agent_id);
        }
    }
}

/// Extract a `lastResponseSummary` for the `agent:idle` payload from the turn's
/// assistant text blocks (mirrors the TS `emitAgentIdleEvent` `finalMessage`
/// summary): join the text blocks and keep the trailing 500 characters — the
/// tail is the meaningful completion, not the "I'll start by…" preamble.
/// `None` when the turn produced no text.
fn last_response_summary(blocks: &[Value]) -> Option<String> {
    let text = blocks
        .iter()
        .filter(|b| b.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|b| b.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join(" ");
    if text.is_empty() {
        return None;
    }
    let chars: Vec<char> = text.chars().collect();
    if chars.len() > 500 {
        let tail: String = chars[chars.len() - 500..].iter().collect();
        Some(format!("...{tail}"))
    } else {
        Some(text)
    }
}

/// The `agent` actor stamped on streaming events (carries the agent id).
pub(crate) fn agent_actor(agent_id: &AgentId) -> EventActor {
    EventActor {
        actor_type: ActorType::Agent,
        id: Some(agent_id.0.clone()),
        ..Default::default()
    }
}

/// Resolve the effective provider id for an agent session using the same precedence
/// as the spawn path (§6.9): model's compound prefix (if `model` contains `:` and
/// yields a non-empty provider) → `provider` field → default provider. Malformed
/// compound ids like `:sonnet` yield an empty prefix and fall through to the provider
/// field / default. This ensures `_meta` injection, spawn args, and all provider-keyed
/// logic use a consistent provider id.
pub(crate) fn resolve_provider_id(model: Option<&str>, provider: Option<&str>) -> String {
    model
        .filter(|m| m.contains(':'))
        .map(|m| intent_providers::parse_compound_model_id(m).0)
        .filter(|id| !id.is_empty()) // guard against malformed compound ids like ":sonnet"
        .or_else(|| provider.filter(|p| !p.is_empty()).map(|p| p.to_string()))
        .unwrap_or_else(|| intent_providers::default_provider_id().to_string())
}

/// Build provider-specific `_meta` for `session/new` and `session/load` from the
/// assembled system prompt (§18.1). Returns `None` for providers that do not use
/// `_meta` injection (auggie, codex, droid, opencode, cortex, pi, grok, mock
/// use other mechanisms — codex moved to the first-turn prepend fallback because
/// the pinned codex-acp adapter (1.1.7) ignores `_meta.developerInstructions`,
/// #479).
/// Provider-specific shapes:
/// - claude-code: `{ "claudeCode": { "options": { "disallowedTools": ["Task"] } }, "systemPrompt": { "append": "<prompt>" }? }`
///   (disallowedTools always present; systemPrompt.append present only when non-blank prompt)
fn build_session_meta(provider_id: &str, system_prompt: Option<&str>) -> Option<Meta> {
    match provider_id {
        "claude-code" => {
            let mut meta = Meta::new();

            // Always add disallowedTools to prevent provider-native Task tool
            // (verified against @agentclientprotocol/claude-agent-acp 0.59.0;
            // disallowedTools are merged with ACP's internal deny rules).
            meta.insert(
                "claudeCode".to_string(),
                serde_json::json!({
                    "options": {
                        "disallowedTools": ["Task"]
                    }
                }),
            );

            // Add systemPrompt.append if non-blank prompt exists
            if let Some(prompt) = system_prompt {
                let prompt = prompt.trim();
                if !prompt.is_empty() {
                    let mut system_prompt_obj = serde_json::Map::new();
                    system_prompt_obj
                        .insert("append".to_string(), Value::String(prompt.to_string()));
                    meta.insert("systemPrompt".to_string(), Value::Object(system_prompt_obj));
                }
            }

            Some(meta)
        }
        _ => None,
    }
}

impl Services {
    /// Begin a live-turn slot for `agent_id` (CS-0 D5): seed it with the freshly
    /// minted assistant `message_id` and no blocks yet, returning a
    /// [`LiveTurnGuard`] that clears the slot on drop (abort-safe). The slot is
    /// refreshed by [`update_live_turn`](Self::update_live_turn) as content streams.
    pub(crate) fn begin_live_turn(
        &self,
        agent_id: &AgentId,
        message_id: &str,
    ) -> LiveTurnGuard<'_> {
        self.set_live_turn(agent_id, message_id, Vec::new());
        LiveTurnGuard {
            live_turns: &self.live_turns,
            agent_id: agent_id.clone(),
        }
    }

    /// Set/replace an agent's live-turn slot. The streaming path drives this via
    /// [`update_live_turn`](Self::update_live_turn); it is also a test seam for
    /// simulating a mid-turn snapshot without spinning up a real ACP turn.
    pub fn set_live_turn(&self, agent_id: &AgentId, message_id: &str, blocks: Vec<Value>) {
        if let Ok(mut slots) = self.live_turns.lock() {
            slots.insert(
                agent_id.clone(),
                LiveTurn {
                    message_id: message_id.to_string(),
                    blocks,
                    last_activity_at: now_iso(),
                },
            );
        }
    }

    /// Refresh the agent's live-turn blocks from the current [`Transcript`] (a
    /// non-consuming [`Transcript::snapshot_blocks`]) and stamp the slot's
    /// `last_activity_at` (STAB-125). No-op if no slot is open.
    fn update_live_turn(&self, agent_id: &AgentId, transcript: &Transcript) {
        if let Ok(mut slots) = self.live_turns.lock() {
            if let Some(slot) = slots.get_mut(agent_id) {
                slot.blocks = transcript.snapshot_blocks();
                slot.last_activity_at = now_iso();
            }
        }
    }

    /// Clear an agent's live-turn slot (happy-path turn end + test seam). The
    /// [`LiveTurnGuard`] also clears on drop for the interrupt/abort path.
    pub fn clear_live_turn(&self, agent_id: &AgentId) {
        if let Ok(mut slots) = self.live_turns.lock() {
            slots.remove(agent_id);
        }
    }

    /// Read an agent's in-flight turn slot, if a turn is currently streaming.
    pub(crate) fn live_turn(&self, agent_id: &AgentId) -> Option<LiveTurn> {
        self.live_turns.lock().ok()?.get(agent_id).cloned()
    }

    /// Read just the live-turn slot's `last_activity_at` stamp (STAB-125)
    /// without cloning the streamed blocks — the liveness reads
    /// (`agent.get`/`agent.list`/`agent.getConversation`/snapshot overlay) poll
    /// this while a potentially large response is mid-stream.
    pub(crate) fn live_turn_activity_at(&self, agent_id: &AgentId) -> Option<String> {
        self.live_turns
            .lock()
            .ok()?
            .get(agent_id)
            .map(|live| live.last_activity_at.clone())
    }

    /// Best-effort flush of an agent's partial in-flight assistant content at
    /// interruption-capture time (graceful shutdown, INT-41 follow-up): persist
    /// the caller-captured live-turn snapshot as a normal `assistant` row tagged
    /// `metadata.interrupted = true` + `stopReason = "interrupted"` (the
    /// terminal-message convention the FE stopped-indicator keys off; `status`
    /// is kept as a redundant tag) so the transcript keeps the streamed-so-far
    /// output across the restart. Reuses the turn's minted `message_id` (CS-0
    /// D1) so persisted block ids `{messageId}:{index}` match what streamed.
    /// The caller snapshots the slot via [`live_turn`](Self::live_turn) BEFORE
    /// aborting the turn worker (the abort drops [`LiveTurnGuard`], clearing
    /// the slot) and flushes AFTER the abort so the worker cannot race the
    /// append; if the worker already persisted the full turn, the append
    /// collides on the UNIQUE id and is logged at debug (benign — the full row
    /// won; the stale slot, if any, is cleared). Errors are logged and
    /// swallowed: this must never block shutdown or the interrupted_agent row
    /// insert.
    ///
    /// Empty-blocks slots (turn started, nothing streamed yet) are a no-op
    /// unless `allow_empty` is set — the STAB-114 zero-output requeue in
    /// `interrupt_send_message` and the graceful-shutdown capture must never
    /// see a phantom row. Only the plain `agent.stop` interrupt opts in, so a
    /// pre-first-token stop durably records the interruption as an empty
    /// assistant row the FE can key the Stopped indicator off.
    ///
    /// Returns the persisted interrupted row's message id (`Some` only when
    /// this flush appended the row), so the interrupt path can carry
    /// `messageId` on the terminal `agent:stream:end`.
    pub(crate) async fn flush_partial_turn_on_interruption(
        &self,
        agent_id: &AgentId,
        live: LiveTurn,
        allow_empty: bool,
    ) -> Option<String> {
        if live.blocks.is_empty() && !allow_empty {
            return None;
        }
        let metadata = json!({
            "interrupted": true,
            "stopReason": "interrupted",
            "status": "interrupted",
        });
        match self
            .store
            .append_agent_message_with_id(
                agent_id,
                &live.message_id,
                "assistant",
                &Value::Array(live.blocks),
                Some(&metadata),
                &now_iso(),
            )
            .await
        {
            Ok(_) => {
                self.clear_live_turn(agent_id);
                Some(live.message_id)
            }
            // Only the `agent_message.id` violation means "the worker already
            // persisted the full turn under this minted id" — a `(agent_id,
            // seq)` collision is a different race and falls through to warn
            // (keeping the live-turn slot as the only copy of the content).
            Err(e)
                if e.to_string()
                    .contains("UNIQUE constraint failed: agent_message.id") =>
            {
                // The durable full row exists — drop the now-stale overlay too.
                self.clear_live_turn(agent_id);
                tracing::debug!(
                    agent = %agent_id,
                    error = %e,
                    "partial flush skipped: worker already persisted the full turn under this id"
                );
                None
            }
            Err(e) => {
                tracing::warn!(
                    agent = %agent_id,
                    error = %e,
                    "failed to flush partial in-flight assistant content at interruption capture"
                );
                None
            }
        }
    }

    /// Open a new ACP session and persist its id as `AgentSession.acpSessionId`
    /// (write-once, for later resume) (§6.5). Returns the fresh id plus the
    /// modes the provider advertised in `session/new` (used by the caller to
    /// pick a permissive `session/set_mode` target from `availableModes`).
    pub async fn open_acp_session(
        &self,
        conn: &Connection,
        agent_id: &AgentId,
        cwd: impl Into<PathBuf>,
        mcp_servers: Vec<McpServer>,
    ) -> Result<AcpSessionOpened> {
        // Load the session up front so the store write is scoped to the owning
        // workspace (the store's `set_acp_session_id` now requires it as a
        // defense-in-depth guard). This call is only reached after the caller
        // resolved this agent id inside a workspace-scoped path.
        let stored = self.store.get_agent_session(agent_id).await?;
        let workspace_id = stored.workspace_id.clone();
        // Resolve provider using the same precedence as spawn path (compound model
        // prefix → provider field → default), then build provider-specific _meta.
        let provider_id = resolve_provider_id(stored.model.as_deref(), stored.provider.as_deref());
        let meta = build_session_meta(&provider_id, stored.system_prompt.as_deref());
        self.publish_status_event(
            &workspace_id,
            agent_id,
            "session-create",
            "Creating session\u{2026}",
            "info",
        )
        .await;
        let resp = session::new_session(conn, cwd, mcp_servers, meta)
            .await
            .map_err(|e| Error::Internal(format!("session/new failed: {e}")))?;
        let acp_session_id = resp.session_id.0.to_string();
        self.store
            .set_acp_session_id(&workspace_id, agent_id, &acp_session_id)
            .await?;
        Ok(AcpSessionOpened {
            session_id: acp_session_id,
            modes: resp.modes,
        })
    }

    /// Open a FRESH ACP session that REPLACES a lost/unsupported stored id (the
    /// resume-impossible fallback): `session/new` then compare-and-swap the
    /// persisted `acpSessionId` from `expected_old` (the id we just failed to
    /// load) to the fresh one. Unlike [`open_acp_session`] (write-once first-set)
    /// this is used ONLY when resume is impossible — `loadSession` unsupported or
    /// `session/load` failed (§6.5). The CAS keeps the id canonical: if a
    /// concurrent recreate already swapped it, the stored value is returned and
    /// reused instead of being clobbered. Returns the canonical `acpSessionId`
    /// with modes only when the freshly-opened session won the CAS — otherwise
    /// the modes belong to some other session and callers must not act on them.
    pub async fn recreate_acp_session(
        &self,
        conn: &Connection,
        agent_id: &AgentId,
        expected_old: &str,
        cwd: impl Into<PathBuf>,
        mcp_servers: Vec<McpServer>,
    ) -> Result<AcpSessionOpened> {
        // Load the session up front so the CAS replace is scoped to the owning
        // workspace (see [`open_acp_session`]).
        let stored = self.store.get_agent_session(agent_id).await?;
        let workspace_id = stored.workspace_id.clone();
        // Resolve provider using the same precedence as spawn path, then build
        // provider-specific _meta for system-prompt injection (recreate path sends
        // the same prompt as new/load).
        let provider_id = resolve_provider_id(stored.model.as_deref(), stored.provider.as_deref());
        let meta = build_session_meta(&provider_id, stored.system_prompt.as_deref());
        self.publish_status_event(
            &workspace_id,
            agent_id,
            "session-create",
            "Creating session\u{2026}",
            "info",
        )
        .await;
        let resp = session::new_session(conn, cwd, mcp_servers, meta)
            .await
            .map_err(|e| Error::Internal(format!("session/new failed: {e}")))?;
        let new_acp_session_id = resp.session_id.0.to_string();
        let canonical = self
            .store
            .replace_acp_session_id(&workspace_id, agent_id, expected_old, &new_acp_session_id)
            .await?;
        // On CAS loss the canonical id belongs to a session we did not open;
        // our modes are meaningless for it and would target the wrong sid.
        let modes = if canonical == new_acp_session_id {
            resp.modes
        } else {
            None
        };
        Ok(AcpSessionOpened {
            session_id: canonical,
            modes,
        })
    }

    /// Resume the agent's persisted `acpSessionId` via `session/load`, but only
    /// when one was stored and the agent advertised the `loadSession` capability.
    /// Returns the resumed id plus the modes the provider advertised in
    /// `session/load`, or `None` when resume is not possible (§6.5).
    pub async fn resume_acp_session(
        &self,
        conn: &Connection,
        init: &InitializeResponse,
        agent_id: &AgentId,
        cwd: impl Into<PathBuf>,
        mcp_servers: Vec<McpServer>,
    ) -> Result<Option<AcpSessionOpened>> {
        let stored = self.store.get_agent_session(agent_id).await?;
        let workspace_id = stored.workspace_id.clone();
        let Some(acp_session_id) = stored.acp_session_id.clone() else {
            return Ok(None);
        };
        if !session::supports_load_session(init) {
            return Ok(None);
        }
        // Resolve provider using the same precedence as spawn path, then build
        // provider-specific _meta for system-prompt injection.
        let provider_id = resolve_provider_id(stored.model.as_deref(), stored.provider.as_deref());
        let meta = build_session_meta(&provider_id, stored.system_prompt.as_deref());
        self.publish_status_event(
            &workspace_id,
            agent_id,
            "session-load",
            "Resuming session\u{2026}",
            "info",
        )
        .await;
        let resp = session::load_session(conn, &acp_session_id, cwd, mcp_servers, meta)
            .await
            .map_err(|e| Error::Internal(format!("session/load failed: {e}")))?;
        Ok(Some(AcpSessionOpened {
            session_id: acp_session_id,
            modes: resp.modes,
        }))
    }

    /// Discard the `session/update` burst that `session/load` replays after a
    /// successful resume: auggie re-streams the prior conversation as
    /// notifications written to the wire *before* `session/load` returns, so they
    /// buffer in the agent handle's unbounded channel. Left in place they would
    /// leak into the next [`run_prompt_turn`](Self::run_prompt_turn), re-emitting
    /// old messages as live `agent:stream:chunk` events and re-accumulating them
    /// into the transcript. Draining them here mirrors TS's "drop `session/update`
    /// when there is no active streaming handler" gate (acp-provider.ts).
    ///
    /// Bounded so it cannot hang: empty whatever is already buffered with
    /// non-blocking `try_recv`, then wait out a short settle window for stragglers
    /// that may land just after `load_session` resolved (a per-message `recv`
    /// timeout; stop once the channel stays quiet), capping the total wait. The
    /// per-agent single-flight slot serialises this, so a brief block on the
    /// resume path is acceptable.
    pub(crate) async fn drain_replay_notifications(
        notifications: &mut mpsc::UnboundedReceiver<IncomingNotification>,
    ) {
        use tokio::time::{timeout, Duration, Instant};
        const SETTLE: Duration = Duration::from_millis(50);
        const CAP: Duration = Duration::from_millis(500);
        let deadline = Instant::now() + CAP;
        loop {
            while notifications.try_recv().is_ok() {}
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match timeout(SETTLE.min(remaining), notifications.recv()).await {
                Ok(Some(_)) => continue, // a straggler arrived → keep draining
                Ok(None) => break,       // channel closed
                Err(_) => break,         // quiet for the settle window → done
            }
        }
    }

    /// Drive a `session/prompt` turn: stream `session/update`s onto the bus and
    /// accumulate the transcript while the turn runs, then append the assistant
    /// message and emit the single terminal `agent:stream:end`. Returns the
    /// agent's [`StopReason`] (§6.5/§6.6).
    pub async fn run_prompt_turn(
        &self,
        conn: &Connection,
        notifications: &mut mpsc::UnboundedReceiver<IncomingNotification>,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        acp_session_id: &str,
        prompt: Vec<ContentBlock>,
    ) -> Result<StopReason> {
        // Mint the assistant message id at turn START (CS-0 D1) so streaming
        // block ids `{messageId}:{index}` match the blocks ultimately persisted.
        let message_id = Uuid::now_v7().to_string();
        let mut transcript = Transcript::new(message_id.clone());
        // Publish the in-flight turn so a `chat.subscribe` arriving mid-turn can
        // reconstruct the partial message (CS-0 D5). The guard clears the slot on
        // ANY exit — including the interrupt/abort path that drops this worker
        // before `stream:end` — so subscribers never see a stale in-flight turn.
        let _live_guard = self.begin_live_turn(agent_id, &message_id);
        // Pre-first-token turn-startup hint: the FE renders "Sent prompt…" next
        // to the spinner until the first `agent:stream:chunk` clears it. Emitted
        // exactly once per turn immediately before dispatching `session/prompt`.
        self.publish_status_event(
            workspace_id,
            agent_id,
            "prompt",
            "Sent prompt\u{2026}",
            "info",
        )
        .await;
        // Activity tracker for idle-based timeout: reset on every notification.
        let activity = session::ActivityTracker::new();
        let prompt_fut = session::prompt(conn, acp_session_id, prompt, &activity);
        tokio::pin!(prompt_fut);
        let mut closed = false;
        let result = loop {
            tokio::select! {
                res = &mut prompt_fut => break res,
                maybe = notifications.recv(), if !closed => match maybe {
                    Some(note) => {
                        activity.touch();
                        self.route_notification(&note, agent_id, workspace_id, &mut transcript)
                            .await;
                    }
                    None => closed = true,
                },
            }
        };
        // Drain updates buffered before the prompt response resolved.
        while let Ok(note) = notifications.try_recv() {
            self.route_notification(&note, agent_id, workspace_id, &mut transcript)
                .await;
        }
        // §7.1 deterministic attach — turn-end drain: append the registered
        // `AtTurnEnd` attachments as trailing resource blocks, and clear ALL
        // remaining registry entries for this agent (unclaimed `AtToolResult`
        // leftovers are dropped so they cannot attach to a later turn). Runs
        // on the error path too — the registry must not leak; the interrupt/
        // abort path (worker dropped) is covered by the next turn's drain +
        // the registry TTL.
        for attachment in self.turn_attachments.finish_turn(agent_id) {
            transcript.push_block(attachment.resource_item());
        }
        // Accumulate the assistant message (one per turn) into the append-only log.
        let blocks = transcript.into_blocks();
        let last_response_summary = last_response_summary(&blocks);
        if !blocks.is_empty() {
            self.store
                .append_agent_message_with_id(
                    agent_id,
                    &message_id,
                    "assistant",
                    &Value::Array(blocks),
                    None,
                    &now_iso(),
                )
                .await?;
        }
        // The turn's message is now durable: clear the live-turn slot so the next
        // `chat.subscribe` snapshot reflects the persisted message (not a stale
        // in-flight copy) BEFORE the terminal `stream:end` is observed. The guard
        // remains as the abort-path fallback.
        self.clear_live_turn(agent_id);
        // Exactly ONE terminal stream:end — complete and error both map here (§7).
        self.publish_agent_event(
            workspace_id,
            agent_id,
            AGENT_STREAM_END,
            json!({ "agentId": agent_id.0 }),
        )
        .await;
        // Session-completion lifecycle signal, emitted AFTER the terminal
        // stream:end (the auto-subscription wake keys off this). A normal
        // turn-end goes idle (`agent:idle`); a turn error maps to `agent:failed`.
        // The interrupt/resume path never reaches here — `interrupt()` aborts
        // this worker before the turn resolves and emits only `stream:end` — so
        // `agent:idle` is suppressed for interrupted agents (mirrors the TS
        // `emitAgentIdleEvent` interrupt suppression).
        //
        // PROTOCOL §5.5/§6.5 invariant: `agent:idle` is **also** suppressed
        // while the agent has at least one ready-to-send queued message — the
        // drain loop is about to flip the next message to in-flight, so the
        // agent is not actually idle. A queue containing only under-edit
        // messages (`editing = true`) is treated as empty for this check.
        match &result {
            Ok(stop_reason) if !self.has_ready_to_send(agent_id) => {
                let mut data = json!({
                    "agentId": agent_id.0,
                    "reason": "stream_complete",
                    "finishReason": stop_reason,
                    "status": "idle",
                });
                if let Some(summary) = last_response_summary {
                    data["lastResponseSummary"] = Value::String(summary);
                }
                // DELIV-1: enrich the idle payload with `agentName` (so
                // subscribers don't fall back to a generic "Agent" label)
                // and — when the child persisted one via `agent.reportToParent`
                // — the completion `report`. `isBackground` rides along so
                // subscribers (e.g. iOS notifications) can branch on the
                // session's background flag without a follow-up read. The
                // lookup is a single indexed row read per idle event; a store
                // error is swallowed and the event still fires with the base
                // payload.
                if let Ok(session) = self.store.get_agent_session(agent_id).await {
                    data["agentName"] = Value::String(session.name);
                    data["isBackground"] = Value::Bool(session.is_background);
                    if let Some(report) = session.completion_report {
                        data["report"] = Value::String(report);
                    }
                }
                // DURABLE-BEFORE-OBSERVABLE: record delegation-group completion
                // BEFORE publishing the idle event so the persisted state is
                // correct if the daemon is killed immediately after the event.
                self.record_group_completion_pre_publish(workspace_id, agent_id, &data)
                    .await;
                self.publish_agent_event(workspace_id, agent_id, AGENT_IDLE, data)
                    .await;
            }
            Ok(_) => {
                // Ready-to-send messages remain — stay busy and skip the idle
                // signal so the FE/auto-commit do not key off a transient
                // mid-drain "idle" snapshot. The terminal `agent:idle` will
                // fire when the queue is truly drained.
                tracing::debug!(
                    agent = %agent_id,
                    "agent:idle suppressed — ready-to-send queue non-empty",
                );
            }
            Err(e) => {
                self.publish_agent_event(
                    workspace_id,
                    agent_id,
                    AGENT_FAILED,
                    json!({ "agentId": agent_id.0, "error": e.to_string() }),
                )
                .await;
            }
        }
        result.map_err(|e| Error::Internal(format!("session/prompt failed: {e}")))
    }

    /// Map one `session/update` notification and publish/accumulate its effects.
    async fn route_notification(
        &self,
        note: &IncomingNotification,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        transcript: &mut Transcript,
    ) {
        let Some(mapped) = session::map_notification(note) else {
            return;
        };
        let message_id = transcript.message_id.clone();
        match mapped {
            MappedUpdate::Chunk { content, text } => {
                // Accumulate into the transcript and compute the block index this
                // chunk lands at; consecutive text chunks coalesce onto one index
                // (and thus one stable block id), a non-text block starts a new one.
                let (block_index, block_type) = match &text {
                    Some(t) => {
                        let index = transcript.current_text_index();
                        transcript.push_text(t);
                        (index, "text".to_string())
                    }
                    None => {
                        let block_type = content
                            .get("type")
                            .and_then(Value::as_str)
                            .unwrap_or("unknown")
                            .to_string();
                        (transcript.push_block(content.clone()), block_type)
                    }
                };
                // D4: enrich additively — keep `content`, add block identity.
                self.publish_agent_event(
                    workspace_id,
                    agent_id,
                    AGENT_STREAM_CHUNK,
                    json!({
                        "agentId": agent_id.0,
                        "content": content,
                        "messageId": message_id,
                        "blockIndex": block_index,
                        "blockId": transcript.block_id(block_index),
                        "blockType": block_type,
                    }),
                )
                .await;
            }
            MappedUpdate::ToolCall(tc) => {
                // §7.1 deterministic attach: claim the pending `AtToolResult`
                // registry batch for this completed call (nonce match against
                // the echoed output, `workspace_api` FIFO fallback). A hit
                // yields the canonical resource items to attach — no echo
                // parsing; a miss falls back to the legacy lift inside
                // `record_tool`. `tool_call_update`s are name-less, so the
                // FIFO gate resolves the name recorded at first sight.
                let registered: Vec<Value> = if tc.status == "completed" {
                    let name = transcript
                        .tool_name_for(&tc.tool_call_id)
                        .unwrap_or(&tc.tool_name)
                        .to_string();
                    self.turn_attachments
                        .claim_at_tool_result(agent_id, tc.output.as_ref(), &name)
                        .iter()
                        .map(|a| a.resource_item())
                        .collect()
                } else {
                    Vec::new()
                };
                // D6: accumulate tool_use/tool_result blocks into the transcript
                // so they persist (and reach `agent.getConversation`). A dropped
                // update (STAB-124: anonymous first sight) publishes no event.
                let Some(block_index) = transcript.record_tool(&tc, registered.clone()) else {
                    return;
                };
                // D4: enrich additively — keep the existing fields, add agentId,
                // the (previously dropped) toolCallId, and the block identity.
                let mut data = json!({
                    "agentId": agent_id.0,
                    "toolName": tc.tool_name,
                    "title": tc.title,
                    "toolKind": tc.tool_kind,
                    "toolCallId": tc.tool_call_id,
                    "input": tc.input,
                    "status": tc.status,
                    "messageId": message_id,
                    "blockIndex": block_index,
                    "blockId": transcript.block_id(block_index),
                });
                if let Some(output) = tc.output {
                    data["output"] = output;
                }
                // Carry the claimed canonical batch on the event so the live
                // `chat.subscribe` delta path attaches the SAME blocks the
                // persisted transcript does (byte-identical invariant).
                if !registered.is_empty() {
                    data["registeredAttachments"] = Value::Array(registered);
                }
                self.publish_agent_event(workspace_id, agent_id, AGENT_TOOL_CALL, data)
                    .await;
            }
        }
        // Refresh the live-turn slot with the partial transcript so a mid-turn
        // `chat.subscribe` snapshot reflects content streamed so far (CS-0 D5).
        self.update_live_turn(agent_id, transcript);
    }

    /// Publish an `agent:stream:status` turn-startup hint on the bus (PROTOCOL
    /// §6.5 / §7). Mirrors the TS reference `acp-provider.ts` `emitStatus()`
    /// call sites: the FE renders the phase message next to the pre-first-token
    /// spinner and clears it on the first chunk / `agent:stream:end` /
    /// `agent:failed`. Self-sufficient payload — no follow-up fetch required.
    /// `pub(crate)` so the [`AgentManager`] `launch` / `init` emit sites can
    /// hit the same helper.
    pub(crate) async fn publish_status_event(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        phase: &str,
        message: &str,
        level: &str,
    ) {
        self.publish_agent_event(
            workspace_id,
            agent_id,
            AGENT_STREAM_STATUS,
            json!({
                "agentId": agent_id.0,
                "workspaceId": workspace_id,
                "phase": phase,
                "message": message,
                "level": level,
                "timestamp": now_epoch_ms(),
            }),
        )
        .await;
    }

    /// Build and publish an agent streaming event onto the bus (§6.6/§10).
    /// `pub(crate)` so the [`AgentManager`] stop path can emit the terminal
    /// `agent:stream:end` when it interrupts a turn (the worker that would
    /// otherwise emit it is aborted). Routes `agent:stream:chunk` through the
    /// transient (broadcast-only, never persisted) path; all other event types
    /// persist durably.
    pub(crate) async fn publish_agent_event(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        event_type: &str,
        data: Value,
    ) {
        let event = NewEvent {
            workspace_id: workspace_id.clone(),
            timestamp: now_iso(),
            event_type: event_type.to_string(),
            actor: agent_actor(agent_id),
            session_id: Some(agent_id.0.clone()),
            correlation_id: None,
            parent_event_id: None,
            metadata: None,
            data,
        };
        // Route stream chunks through the transient path (broadcast-only);
        // persist all other agent events (stream:status, stream:end, tool:call,
        // lifecycle, etc.) for durable audit trail.
        if event_type == AGENT_STREAM_CHUNK {
            crate::publish_event_transient(&self.event_bus, event);
        } else {
            crate::publish_event(&self.event_bus, event).await;
        }
    }
}
