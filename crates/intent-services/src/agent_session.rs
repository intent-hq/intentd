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
    SessionConfigKind, SessionConfigOption, SessionConfigOptionCategory,
    SessionConfigSelectOptions, SessionModeState, StopReason,
};
use intent_acp::{AcpError, Connection, IncomingNotification};
use intent_core::events::{
    AGENT_FAILED, AGENT_IDLE, AGENT_STREAM_CHUNK, AGENT_STREAM_END, AGENT_STREAM_START,
    AGENT_STREAM_STATUS, AGENT_TOOL_CALL,
};
use intent_core::{
    now_epoch_ms, now_iso, ActorType, AgentId, Error, EventActor, Result, WorkspaceId,
};
use intent_store::NewEvent;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::{token_usage, usage_stats, Services};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod tests_meta;

/// Prefix marking a `session/prompt` failure that is a silent-redrive
/// candidate (monorepo#764): the transport to the child closed BEFORE the
/// turn streamed any output, so the prompt provably produced nothing and the
/// worker may redrive it once on a fresh child. [`Services::run_prompt_turn`]
/// suppresses the terminal `agent:failed` + `agent:stream:end` pair for these
/// errors — the worker either redrives silently (its retried attempt emits
/// the turn's terminal events) or, once the one-retry budget is spent, routes
/// the error through the terminal-failure path, which emits the pair itself.
pub(crate) const PROMPT_PRE_OUTPUT_TRANSPORT_PREFIX: &str =
    "session/prompt transport closed before output:";

/// Whether a `session/prompt` failure is transport-shaped: the writer task
/// observed a closed pipe (`transport closed: …`, e.g. "writer task closed")
/// or the child's stdout closed with the request still pending (the transport
/// synthesizes a code-0 "agent stdout closed" JSON-RPC error for that —
/// see `provider_models::probe::map_acp_error` for the same special case).
/// Provider JSON-RPC errors (including `-32800` cancels), timeouts, and
/// everything else are NOT transport-shaped.
fn transport_closed_error(err: &AcpError) -> bool {
    match err {
        AcpError::Transport(_) => true,
        AcpError::Rpc(e) => e.code == 0 && e.message == "agent stdout closed",
        _ => false,
    }
}

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
    /// The bare effective model resolved from the response's
    /// `configOptions[id="model"]` and persisted onto `agent_session.model`
    /// (D13), when the stored model was a placeholder and the resolution +
    /// guarded write both succeeded. The manager syncs the live handle's
    /// `spawned_model` to this value so the next `ensure_started` does not
    /// misread the persisted effective model as an `agent.setModel` change.
    pub effective_model: Option<String>,
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
    /// merge the NON-EMPTY update fields into the existing block —
    /// `tool_call_update`s are sparse (absent fields map to `""`/`Null`), so a
    /// status-only update must not wipe the recorded name/title/input, while a
    /// richer title/input arriving mid-flight must be persisted. Status always
    /// patches. When the tool reaches `completed`/`error`
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
                let block = &mut self.blocks[i];
                // A non-empty title refreshes the echoed `_acpTitle` (and the
                // derived name when non-empty); a non-null input replaces the
                // block input, re-attaching the freshest title.
                if !tc.title.is_empty() && !tc.tool_name.trim().is_empty() {
                    if let Some(obj) = block.as_object_mut() {
                        obj.insert("name".to_string(), Value::String(tc.tool_name.clone()));
                    }
                }
                if !tc.input.is_null() {
                    let title = if tc.title.is_empty() {
                        block["input"]
                            .get("_acpTitle")
                            .and_then(Value::as_str)
                            .unwrap_or_default()
                            .to_string()
                    } else {
                        tc.title.clone()
                    };
                    if let Some(obj) = block.as_object_mut() {
                        obj.insert(
                            "input".to_string(),
                            crate::tool_block::attach_acp_title(tc.input.clone(), &title),
                        );
                    }
                } else if !tc.title.is_empty() {
                    if let Some(obj) = block.get_mut("input").and_then(Value::as_object_mut) {
                        obj.insert("_acpTitle".to_string(), Value::String(tc.title.clone()));
                    }
                }
                if let Some(meta) = block.get_mut("metadata").and_then(Value::as_object_mut) {
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

/// Per-agent chain of detached turn-end usage-bookkeeping tasks
/// (monorepo#738): the `JoinHandle` of the most recently spawned bookkeeping
/// task per agent. Each new turn's task awaits its predecessor before
/// running, so per-agent bookkeeping stays ordered across turns even though
/// it is detached from the stream path — a delayed task from turn N could
/// otherwise let turn N+1's stats delta be computed against a stale snapshot
/// (durable double-count in `usage_stats_hourly`) or overwrite turn N+1's
/// newer cumulative snapshot (a regression the watermark scan cannot see).
/// One entry per agent, replaced each turn; shared across [`Services`] clones.
pub(crate) type TurnBookkeeping = Arc<Mutex<HashMap<AgentId, tokio::task::JoinHandle<()>>>>;

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

/// Resolve the effective model a provider is actually running from the
/// `configOptions` of a `session/new` / `session/load` response (D13): the
/// select option with `id == "model"` (falling back to `category == "model"`)
/// carries `currentValue`; map it to its option entry and find a known model
/// family in the entry's name or description — the first version-bearing
/// match wins (e.g. currentValue `"default"` → name "Default (recommended)" /
/// description "Opus 4.8 with 1M context · …" → `"Opus 4.8"`), with the raw
/// `currentValue` id itself as the last candidate. `None` when the response
/// has no model select or nothing resolves to a known family with a version —
/// version-less matches (bare "Opus") are rejected because they would merge
/// sibling versions and, persisted, are indistinguishable from real option
/// ids in the post-session model-application gate.
fn resolve_effective_model(config_options: Option<&[SessionConfigOption]>) -> Option<String> {
    let select = model_select(config_options?)?;
    let current = select.current_value.0.as_ref();
    let mut candidates: Vec<&str> = Vec::new();
    if let Some(entry) = select_entry(select, current) {
        candidates.push(entry.name.as_str());
        if let Some(desc) = entry.description.as_deref() {
            candidates.push(desc);
        }
    }
    candidates.push(current);
    version_bearing_display(candidates)
}

/// Resolve the display identity of an EXPLICITLY selected model id against
/// the same `configOptions[id="model"]` option list the default path uses
/// (D14): match the stored bare id (compound prefix stripped) against an
/// option's `value` and derive a version-bearing family display from that
/// entry's name/description (e.g. `claude-fable-5[1m]` → name "Fable" is
/// version-less, description "Fable 5 with 1M context · …" → `"Fable 5"`).
/// The select's `currentValue` is deliberately ignored — at `session/new`
/// time it may still be the provider default; the post-session
/// `session/set_config_option` applies the stored id only afterwards. Unlike
/// D13 the raw id is NOT a candidate: `normalize_model_name` already covers
/// ids that carry their own family+version at stats time, so the persisted
/// resolution is reserved for identities only the option list knows. `None`
/// when no option matches or nothing version-bearing resolves.
fn resolve_explicit_display_model(
    bare_id: &str,
    config_options: Option<&[SessionConfigOption]>,
) -> Option<String> {
    let select = model_select(config_options?)?;
    let entry = select_entry(select, bare_id)?;
    let mut candidates: Vec<&str> = vec![entry.name.as_str()];
    if let Some(desc) = entry.description.as_deref() {
        candidates.push(desc);
    }
    version_bearing_display(candidates)
}

/// The model select of a `session/new` / `session/load` response's
/// `configOptions`: `id == "model"` wins, `category == "model"` is the
/// fallback (shared by the D13 default and D14 explicit resolutions).
fn model_select(options: &[SessionConfigOption]) -> Option<&session::SessionConfigSelect> {
    let select_by = |pred: &dyn Fn(&SessionConfigOption) -> bool| {
        options.iter().find_map(|o| match &o.kind {
            SessionConfigKind::Select(s) if pred(o) => Some(s),
            _ => None,
        })
    };
    select_by(&|o| o.id.0.as_ref() == "model")
        .or_else(|| select_by(&|o| matches!(o.category, Some(SessionConfigOptionCategory::Model))))
}

/// Find a select's option entry by `value`, looking through groups when the
/// options are grouped.
fn select_entry<'a>(
    select: &'a session::SessionConfigSelect,
    value: &str,
) -> Option<&'a session::SessionConfigSelectOption> {
    match &select.options {
        SessionConfigSelectOptions::Ungrouped(opts) => {
            opts.iter().find(|e| e.value.0.as_ref() == value)
        }
        SessionConfigSelectOptions::Grouped(groups) => groups
            .iter()
            .flat_map(|g| g.options.iter())
            .find(|e| e.value.0.as_ref() == value),
        _ => None,
    }
}

/// First candidate that resolves to a known model family WITH a version
/// (bare "Opus" is rejected — it would merge sibling versions and, persisted,
/// is indistinguishable from a real option id in the post-session
/// model-application gate).
fn version_bearing_display<'a>(candidates: impl IntoIterator<Item = &'a str>) -> Option<String> {
    candidates
        .into_iter()
        .filter_map(usage_stats::known_family_model_name)
        .find(|name| name.chars().any(|c| c.is_ascii_digit()))
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
    /// unless `allow_empty` is set — the STAB-114 zero-output combined
    /// delivery in `interrupt_send_message` and the graceful-shutdown capture
    /// must never see a phantom row. Only the plain `agent.stop` interrupt
    /// opts in, so a pre-first-token stop durably records the interruption as
    /// an empty assistant row the FE can key the Stopped indicator off.
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

    /// Persist the effective model resolved from a session-open response's
    /// `configOptions` (D13): only when the stored model is a placeholder
    /// (NULL / blank / `default` sentinel — an explicitly user-selected model
    /// is NEVER overwritten), and only while it still is at write time (the
    /// store's guarded CAS write loses benignly to a concurrent
    /// `agent.setModel`). Always persisted as the compound
    /// `{provider_id}:{effective}` (e.g. `claude-code:Opus 4.8`) so
    /// [`resolve_provider_id`] keeps resolving the same provider even when
    /// the stored model was NULL and the `provider` field is empty. Returns
    /// the *bare* effective model when the write landed — the value
    /// `resolve_spawn` will yield next, which the manager syncs onto the
    /// live handle's `spawned_model`.
    ///
    /// A NON-placeholder (explicitly selected) model takes the D14 branch
    /// instead: the stored id is never rewritten (it keeps driving provider
    /// configuration — spawn flags / `session/set_config_option`); its
    /// display identity is resolved against the same option list via
    /// [`resolve_explicit_display_model`] and persisted to the separate
    /// `resolved_model` column, used only for usage-stats attribution. That
    /// branch returns `None` — `resolve_spawn` is unaffected.
    ///
    /// Best-effort: failures are logged, never propagated — model resolution
    /// must not fail session open.
    async fn persist_effective_model(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        provider_id: &str,
        stored_model: Option<&str>,
        config_options: Option<&[SessionConfigOption]>,
    ) -> Option<String> {
        if !usage_stats::is_placeholder_model(stored_model) {
            self.persist_resolved_display_model(
                workspace_id,
                agent_id,
                stored_model,
                config_options,
            )
            .await;
            return None;
        }
        let effective = resolve_effective_model(config_options)?;
        let persisted = format!("{provider_id}:{effective}");
        match self
            .store
            .set_agent_session_effective_model(workspace_id, agent_id, stored_model, &persisted)
            .await
        {
            Ok(true) => {
                tracing::debug!(
                    agent = %agent_id,
                    model = %persisted,
                    "persisted effective session model from configOptions"
                );
                Some(effective)
            }
            Ok(false) => None, // lost to a concurrent explicit model change
            Err(e) => {
                tracing::warn!(agent = %agent_id, error = %e, "persist effective session model failed");
                None
            }
        }
    }

    /// D14 companion to [`persist_effective_model`](Self::persist_effective_model):
    /// resolve an EXPLICITLY selected model id's display identity against the
    /// session-open `configOptions` and persist it to `resolved_model`. The
    /// bare id (compound `{provider}:` prefix stripped — stored explicit
    /// picks are compound, option values are bare) is matched against the
    /// model select's option values. The outcome is persisted EITHER way — a
    /// `None` resolution overwrites (clears) any previously persisted
    /// display name, so a resolution from an older option list can never go
    /// stale and mis-attribute stats after the provider's catalog changes.
    /// The store write is guarded on `model` still equalling the pre-open
    /// stored value, so a resolution is never attached to a model a
    /// concurrent `agent.setModel` changed. Best-effort: failures are
    /// logged, never propagated.
    async fn persist_resolved_display_model(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        stored_model: Option<&str>,
        config_options: Option<&[SessionConfigOption]>,
    ) {
        let Some(stored) = stored_model else { return };
        let (_, bare_id) = intent_providers::parse_compound_model_id(stored);
        let resolved = resolve_explicit_display_model(&bare_id, config_options);
        match self
            .store
            .set_agent_session_resolved_model(workspace_id, agent_id, stored, resolved.as_deref())
            .await
        {
            Ok(true) => {
                tracing::debug!(
                    agent = %agent_id,
                    model = %stored,
                    resolved = %resolved.as_deref().unwrap_or("<none>"),
                    "persisted resolved display model from configOptions"
                );
            }
            Ok(false) => {} // lost to a concurrent explicit model change
            Err(e) => {
                tracing::warn!(agent = %agent_id, error = %e, "persist resolved display model failed");
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
        let effective_model = self
            .persist_effective_model(
                &workspace_id,
                agent_id,
                &provider_id,
                stored.model.as_deref(),
                resp.config_options.as_deref(),
            )
            .await;
        Ok(AcpSessionOpened {
            session_id: acp_session_id,
            modes: resp.modes,
            effective_model,
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
        // The effective-model resolution is skipped for the same reason.
        let (modes, effective_model) = if canonical == new_acp_session_id {
            let effective_model = self
                .persist_effective_model(
                    &workspace_id,
                    agent_id,
                    &provider_id,
                    stored.model.as_deref(),
                    resp.config_options.as_deref(),
                )
                .await;
            (resp.modes, effective_model)
        } else {
            (None, None)
        };
        Ok(AcpSessionOpened {
            session_id: canonical,
            modes,
            effective_model,
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
        // A committed cross-provider `agent.setModel` deliberately leaves the
        // OLD provider's `acp_session_id` in place (deferred-commit: a switch
        // reverted before the next message must stay a no-op, and the original
        // id must remain usable for a same-provider resume). Never offer that
        // foreign id to the NEW provider's binary via `session/load`: a
        // provider that silently accepted it would skip the supervisor-XML
        // history replay entirely (monorepo#907). The stored id's owner is the
        // committed `last_turn_provider` (written at turn start once the spawn
        // identity is up); when it differs from the provider this turn
        // resolves to, skip resume so the caller falls into the recreate +
        // history-replay branch. `None` (no committed turn yet — legacy rows
        // or a crash before the identity commit) keeps today's behavior.
        // One crash window errs on the safe side: a cross-provider turn that
        // reached `recreate_acp_session` (stored id already the NEW
        // provider's) but died before the identity commit still carries the
        // OLD `last_turn_provider` on restart, so this guard skips a resume
        // that would have been legitimate — a redundant recreate + replay,
        // never a foreign load or context loss.
        // Both sides are canonicalized through the registry before comparing:
        // the commit stores the spawn-resolved `provider.id`, but the resolved
        // id here may be a legacy default alias (`acp`/`augment`/`default`)
        // from a persisted row — an alias spawns the same default binary, so
        // it must not read as a provider change.
        let (_, last_turn_provider) = self
            .store
            .get_agent_session_last_turn_model(&workspace_id, agent_id)
            .await?;
        if let Some(owner) = last_turn_provider {
            let canonical = |id: &str| intent_providers::provider_config(id).id;
            if canonical(&owner) != canonical(&provider_id) {
                tracing::info!(
                    agent = %agent_id,
                    from = %owner,
                    to = %provider_id,
                    "cross-provider switch: skipping session/load of the old provider's session"
                );
                return Ok(None);
            }
        }
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
        let effective_model = self
            .persist_effective_model(
                &workspace_id,
                agent_id,
                &provider_id,
                stored.model.as_deref(),
                resp.config_options.as_deref(),
            )
            .await;
        Ok(Some(AcpSessionOpened {
            session_id: acp_session_id,
            modes: resp.modes,
            effective_model,
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
        // Turn wall-clock start, for the global usage-stats longest-run MAX.
        let turn_started = std::time::Instant::now();
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
        // Whether ANY `session/update` for this turn was applied — an input to
        // the silent-redrive eligibility below (monorepo#764): once the
        // provider streamed anything, a transport failure is no longer
        // provably output-free.
        let mut updates_applied = false;
        let result = loop {
            tokio::select! {
                res = &mut prompt_fut => break res,
                maybe = notifications.recv(), if !closed => match maybe {
                    Some(note) => {
                        activity.touch();
                        updates_applied |= self
                            .route_notification(&note, agent_id, workspace_id, &mut transcript)
                            .await;
                    }
                    None => closed = true,
                },
            }
        };
        // Drain updates buffered before the prompt response resolved.
        while let Ok(note) = notifications.try_recv() {
            updates_applied |= self
                .route_notification(&note, agent_id, workspace_id, &mut transcript)
                .await;
        }
        // §7.1 deterministic attach — turn-end drain: append the registered
        // `AtTurnEnd` attachments as trailing resource blocks, and clear ALL
        // remaining registry entries for this agent (unclaimed `AtToolResult`
        // leftovers are dropped so they cannot attach to a later turn). Runs
        // on the error path too — the registry must not leak; the interrupt/
        // abort path (worker dropped) is covered by the next turn's drain +
        // the registry TTL.
        let drained_attachments = self.turn_attachments.finish_turn(agent_id);
        let trailing_count = drained_attachments.len();
        for attachment in drained_attachments {
            transcript.push_block(attachment.resource_item());
        }
        // Split the PromptOutcome into its stop reason and the optional
        // end-of-turn usage snapshot (persisted below once the turn's message
        // is durable).
        let mut turn_usage = None;
        let result = result.map(|outcome| {
            turn_usage = outcome.usage;
            outcome.stop_reason
        });
        // Accumulate the assistant message (one per turn) into the append-only log.
        let blocks = transcript.into_blocks();
        let last_response_summary = last_response_summary(&blocks);
        // Snapshot the drained AtTurnEnd blocks AS PERSISTED (post id-stamping
        // — the drain pushed them last, so they are the trailing slice) for
        // the terminal `agent:stream:end` payload below: the FE finalizes the
        // in-flight message from accumulated chunks at stream-end, so blocks
        // appended only after the stream loop would never reach it live
        // (monorepo#732 fix wave). Byte-identical to the persisted blocks.
        let trailing_blocks = blocks[blocks.len() - trailing_count..].to_vec();
        // Set only AFTER the successful store append below, so the terminal
        // emit can never advertise a `messageId` for a row that was never
        // written (append failures `?`-propagate before the emit).
        let mut message_persisted = false;
        // Silent-redrive eligibility (monorepo#764): the transport closed
        // before the turn streamed ANYTHING (no session/update applied, zero
        // transcript blocks) — the prompt provably never produced output, so
        // the worker may redrive it once on a fresh child. Classified here,
        // BEFORE the terminal emits below, so a redriven attempt never
        // flashes a failed turn at the FE. Any error after streamed content
        // keeps the existing terminal emit path unchanged.
        let pre_output_transport_failure = matches!(&result, Err(e) if transport_closed_error(e))
            && !updates_applied
            && blocks.is_empty();
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
            message_persisted = true;
        }
        // The turn's message is now durable: clear the live-turn slot so the next
        // `chat.subscribe` snapshot reflects the persisted message (not a stale
        // in-flight copy) BEFORE the terminal `stream:end` is observed. The guard
        // remains as the abort-path fallback.
        self.clear_live_turn(agent_id);
        // Turn-end usage bookkeeping, detached (monorepo#738): the global
        // usage-stats recording (fold this turn's token delta + run counters
        // into the current UTC hour bucket of `usage_stats_hourly`) and the
        // live token-usage update (§5.23: REPLACE the session's cumulative
        // snapshot — never sum, ACP counts are cumulative per session — then
        // re-aggregate the workspace tally and emit
        // `workspace:tokenUsage-changed`) run in a spawned task so the
        // terminal `agent:stream:end` below never waits on them —
        // `workspace:tokenUsage-changed` therefore has NO ordering guarantee
        // relative to `agent:stream:end` (the FE handles it independently and
        // no contract depends on the order). Best-effort: failures are logged
        // and never fail the turn.
        //
        // Ordering WITHIN the bookkeeping is still load-bearing: the stats
        // delta is computed against the previously persisted snapshot, so
        // `record_turn_usage_stats` runs before `persist_turn_token_usage`
        // replaces it, and tasks for the SAME agent are chained (each awaits
        // its predecessor via [`TurnBookkeeping`]) so a delayed task from
        // turn N can neither skew turn N+1's stats delta nor overwrite its
        // newer cumulative snapshot.
        let run_completed = result.is_ok();
        if turn_usage.is_some() || run_completed {
            let services = self.clone();
            let agent_id_task = agent_id.clone();
            let workspace_id_task = workspace_id.clone();
            let turn_duration = turn_started.elapsed();
            let turn_usage = turn_usage.take();
            let prev = self
                .turn_bookkeeping
                .lock()
                .ok()
                .and_then(|mut chain| chain.remove(agent_id));
            let handle = tokio::spawn(async move {
                if let Some(prev) = prev {
                    let _ = prev.await;
                }
                services
                    .record_turn_usage_stats(
                        &agent_id_task,
                        &workspace_id_task,
                        turn_usage.as_ref(),
                        turn_duration,
                        run_completed,
                    )
                    .await;
                if let Some(usage) = turn_usage {
                    services
                        .persist_turn_token_usage(&agent_id_task, &workspace_id_task, &usage)
                        .await;
                }
            });
            if let Ok(mut chain) = self.turn_bookkeeping.lock() {
                chain.insert(agent_id.clone(), handle);
            }
        }
        // Exactly ONE terminal stream:end — complete and error both map here
        // (§7), EXCEPT a pre-output transport failure (monorepo#764): the
        // worker either redrives the prompt (the redriven attempt emits the
        // turn's terminal events) or, when the one-retry budget is spent,
        // emits the pair itself via the terminal-failure path.
        //
        // The payload carries `messageId` when the turn persisted an
        // assistant message, and `trailingBlocks` (the drained AtTurnEnd
        // blocks, byte-identical to the persisted trailing blocks, in
        // registration order) when any were drained — omitted otherwise
        // (monorepo#732 fix wave: live delivery of turn-end attachments).
        if !pre_output_transport_failure {
            let mut end_data = json!({ "agentId": agent_id.0 });
            if message_persisted {
                end_data["messageId"] = json!(message_id);
            }
            if !trailing_blocks.is_empty() {
                end_data["trailingBlocks"] = Value::Array(trailing_blocks);
            }
            self.publish_agent_event(workspace_id, agent_id, AGENT_STREAM_END, end_data)
                .await;
        }
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
                // — the completion report, emitted under both
                // `completionReport` (canonical) and `report` (back-compat).
                // `isBackground` rides along so subscribers (e.g. iOS
                // notifications) can branch on the session's background flag
                // without a follow-up read. The lookup is a single indexed row
                // read per idle event; a store error is swallowed and the
                // event still fires with the base payload.
                if let Ok(session) = self.store.get_agent_session(agent_id).await {
                    data["agentName"] = Value::String(session.name);
                    data["isBackground"] = Value::Bool(session.is_background);
                    if let Some(report) = session.completion_report {
                        data["completionReport"] = Value::String(report.clone());
                        data["report"] = Value::String(report);
                    }
                }
                // `isWaitingForOtherAgents` is computed at emit time from the
                // idle agent's pending completion watches (same derivation as
                // the `AgentLite` flag) so notification clients can suppress
                // alerts snapshot-consistently — a follow-up `agent.list`
                // read can race the child's completion consuming the watch.
                data["isWaitingForOtherAgents"] =
                    Value::Bool(!self.list_watches_for_parent(agent_id).is_empty());
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
            Err(e) if pre_output_transport_failure => {
                // Suppressed (monorepo#764): no user-visible failure for this
                // attempt — the worker decides between a silent redrive and
                // the terminal path (which emits agent:failed + stream:end).
                tracing::debug!(
                    agent = %agent_id,
                    error = %e,
                    "pre-output transport failure — deferring terminal events to the turn worker",
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
        result.map_err(|e| {
            if pre_output_transport_failure {
                Error::Internal(format!("{PROMPT_PRE_OUTPUT_TRANSPORT_PREFIX} {e}"))
            } else {
                Error::Internal(format!("session/prompt failed: {e}"))
            }
        })
    }

    /// Drive one implicit agent-initiated turn (monorepo#855): the agent's
    /// harness produced out-of-turn `session/update`s with no prompt turn
    /// consuming the channel, so stream them live as their own turn. `first`
    /// is the notification that woke the idle listener; further updates are
    /// drained from `notifications` until the settle window (`settle`) elapses
    /// with no traffic — quiescence finalizes the turn. There is no
    /// `session/prompt` in flight, so the normal exits are quiescence and a
    /// user send racing in (a ready-to-send message breaks the drain early so
    /// the queued prompt turn starts promptly). An `interrupt` /
    /// `interrupt_send_message` / `stop` instead aborts the drive task the
    /// caller registered in `AgentManager::workers`, so the interrupt
    /// snapshot→abort→flush semantics apply, same as a prompt turn.
    ///
    /// Emits `agent:stream:start` `{ agentId, messageId, reason: "harness-wake" }`
    /// before routing, streams via the same [`route_notification`] path
    /// (chunks/tool events + live-turn slot updates), then finalizes:
    /// persists the assistant row (skipped when the burst produced zero
    /// transcript blocks — status-only updates), clears the live-turn slot,
    /// and emits exactly one `agent:stream:end` (with `messageId` when a row
    /// was persisted). The `agent:idle` lifecycle emit stays with the caller,
    /// which owns the single-flight slot.
    ///
    /// Returns the persisted assistant `messageId`, or `None` when the burst
    /// persisted nothing.
    pub(crate) async fn run_harness_wake_turn(
        &self,
        notifications: &mut mpsc::UnboundedReceiver<IncomingNotification>,
        first: IncomingNotification,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        settle: std::time::Duration,
    ) -> Option<String> {
        let message_id = Uuid::now_v7().to_string();
        let mut transcript = Transcript::new(message_id.clone());
        // Live-turn slot + abort-safe guard, same contract as a prompt turn:
        // a `chat.subscribe` arriving mid-wake reconstructs the partial
        // message, and an abort (preempting prompt / stop) clears the slot.
        let _live_guard = self.begin_live_turn(agent_id, &message_id);
        self.publish_agent_event(
            workspace_id,
            agent_id,
            AGENT_STREAM_START,
            json!({
                "agentId": agent_id.0,
                "messageId": message_id,
                "reason": "harness-wake",
            }),
        )
        .await;
        let mut updates_applied = self
            .route_notification(&first, agent_id, workspace_id, &mut transcript)
            .await;
        // Drain until quiescence: each received notification re-arms the
        // settle window; the window elapsing (or the channel closing)
        // finalizes the turn. Polled in short ticks so a user send that
        // raced in (queued behind this turn's slot) preempts promptly —
        // finalize first, then the caller hands the receiver off to the
        // drained prompt turn.
        let mut last_update = tokio::time::Instant::now();
        loop {
            if self.has_ready_to_send(agent_id) {
                break;
            }
            let elapsed = last_update.elapsed();
            if elapsed >= settle {
                break;
            }
            let tick = (settle - elapsed).min(std::time::Duration::from_millis(50));
            match tokio::time::timeout(tick, notifications.recv()).await {
                Ok(Some(note)) => {
                    updates_applied |= self
                        .route_notification(&note, agent_id, workspace_id, &mut transcript)
                        .await;
                    last_update = tokio::time::Instant::now();
                }
                Ok(None) => break,
                Err(_) => {}
            }
        }
        let blocks = transcript.into_blocks();
        let mut message_persisted = false;
        if !blocks.is_empty() {
            match self
                .store
                .append_agent_message_with_id(
                    agent_id,
                    &message_id,
                    "assistant",
                    &Value::Array(blocks),
                    None,
                    &now_iso(),
                )
                .await
            {
                Ok(_) => message_persisted = true,
                Err(e) => {
                    tracing::warn!(agent = %agent_id, error = %e, "harness-wake turn persist failed");
                }
            }
        } else if !updates_applied {
            tracing::debug!(agent = %agent_id, "harness-wake turn produced no content");
        }
        self.clear_live_turn(agent_id);
        let mut end_data = json!({ "agentId": agent_id.0 });
        if message_persisted {
            end_data["messageId"] = json!(message_id);
        }
        self.publish_agent_event(workspace_id, agent_id, AGENT_STREAM_END, end_data)
            .await;
        message_persisted.then_some(message_id)
    }

    /// Emit the `agent:idle` lifecycle signal for a finished harness-wake turn
    /// (monorepo#855), honoring the same ready-to-send suppression as a prompt
    /// turn's idle emit. `reason: "harness_wake_complete"` distinguishes it
    /// from `stream_complete` for subscribers.
    pub(crate) async fn publish_harness_wake_idle(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
    ) {
        if self.has_ready_to_send(agent_id) {
            tracing::debug!(
                agent = %agent_id,
                "agent:idle suppressed after harness-wake — ready-to-send queue non-empty",
            );
            return;
        }
        let mut data = json!({
            "agentId": agent_id.0,
            "reason": "harness_wake_complete",
            "status": "idle",
        });
        if let Ok(session) = self.store.get_agent_session(agent_id).await {
            data["agentName"] = Value::String(session.name);
            data["isBackground"] = Value::Bool(session.is_background);
            if let Some(report) = session.completion_report {
                data["completionReport"] = Value::String(report.clone());
                data["report"] = Value::String(report);
            }
        }
        // Same emit-time waiting flag as the prompt-turn idle (see
        // `run_prompt_turn`) so wake-turn subscribers get the identical signal.
        data["isWaitingForOtherAgents"] =
            Value::Bool(!self.list_watches_for_parent(agent_id).is_empty());
        self.record_group_completion_pre_publish(workspace_id, agent_id, &data)
            .await;
        self.publish_agent_event(workspace_id, agent_id, AGENT_IDLE, data)
            .await;
    }

    /// Persist one turn's end-of-turn usage report and refresh the workspace
    /// tally (§5.23). The report is interpreted as the session's cumulative
    /// snapshot (see `token_usage::snapshot_from_turn_usage`), so it REPLACES
    /// the previously stored snapshot; the workspace `TokenUsage` is then
    /// re-aggregated, persisted, and `workspace:tokenUsage-changed` emitted
    /// when it changed. Best-effort: errors are logged, never propagated —
    /// usage bookkeeping must not fail an otherwise-successful turn.
    pub(crate) async fn persist_turn_token_usage(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        usage: &session::Usage,
    ) {
        let snapshot = token_usage::snapshot_from_turn_usage(usage);
        if let Err(e) = self
            .store
            .set_agent_session_token_usage(workspace_id, agent_id, &snapshot)
            .await
        {
            tracing::warn!(agent = %agent_id, error = %e, "persist turn token usage failed");
            return;
        }
        if let Err(e) = self
            .recompute_workspace_token_usage(workspace_id, false)
            .await
        {
            tracing::warn!(
                workspace = %workspace_id.as_str(),
                error = %e,
                "recompute workspace token usage after turn failed"
            );
        }
    }

    /// Record one finished prompt turn into the global `usage_stats_hourly`
    /// store (usage-stats cards): the per-turn token delta — the new
    /// cumulative snapshot minus the previously persisted one, clamped ≥ 0
    /// per counter — plus, for completed turns only (agent runs = completed
    /// prompt turns), a `runs` increment and the turn's wall-clock duration
    /// folded into the bucket's `longest_run_ms` MAX. Counters land in the
    /// current UTC hour bucket keyed by the session's stats model key —
    /// normalized model name, falling back to the provider id for
    /// placeholder/absent models, `"unknown"` only when the provider is
    /// unknowable too (D13) — with no workspace dimension, stamped with the
    /// daemon's local wall-clock (D12). MUST run BEFORE
    /// `persist_turn_token_usage` replaces the session snapshot the delta is
    /// computed against — the per-agent chained bookkeeping task spawned in
    /// [`run_prompt_turn`](Self::run_prompt_turn) calls the two in that
    /// order. Best-effort: errors are logged, never propagated — stats
    /// bookkeeping must not fail a turn.
    pub(crate) async fn record_turn_usage_stats(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        usage: Option<&session::Usage>,
        turn_duration: std::time::Duration,
        run_completed: bool,
    ) {
        if usage.is_none() && !run_completed {
            return; // failed turn without a usage report — nothing to record
        }
        let (model, resolved_model, provider, prev, prev_readable) = match self
            .store
            .get_agent_session_token_usage(workspace_id, agent_id)
            .await
        {
            Ok((model, resolved, provider, prev)) => (model, resolved, provider, prev, true),
            Err(e) => {
                // Without the previous snapshot the delta would re-count the
                // session's full history — drop the token part, keep the run.
                tracing::warn!(agent = %agent_id, error = %e, "read prev token usage for stats failed");
                (None, None, None, None, false)
            }
        };
        let tokens = match usage {
            Some(u) if prev_readable => usage_stats::turn_token_delta(
                prev.as_ref(),
                &token_usage::snapshot_from_turn_usage(u),
            ),
            _ => Default::default(),
        };
        let delta = intent_store::UsageStatsDelta {
            input_tokens: tokens.input_tokens,
            output_tokens: tokens.output_tokens,
            cache_read_tokens: tokens.cache_read_tokens,
            cache_creation_tokens: tokens.cache_creation_tokens,
            runs: run_completed as u64,
            longest_run_ms: if run_completed {
                turn_duration.as_millis() as u64
            } else {
                0
            },
            ..Default::default()
        };
        let now = time::OffsetDateTime::now_utc();
        let bucket = usage_stats::hour_bucket_utc(now);
        let local = usage_stats::recording_local_offset().map(|o| usage_stats::local_stamp(now, o));
        let provider_id =
            prev_readable.then(|| resolve_provider_id(model.as_deref(), provider.as_deref()));
        let model = usage_stats::stats_model_key(
            model.as_deref(),
            resolved_model.as_deref(),
            provider_id.as_deref(),
        );
        let provider_key = usage_stats::stats_provider_key(provider_id.as_deref());
        if let Err(e) = self
            .store
            .add_usage_stats(&bucket, &model, &provider_key, local.as_ref(), &delta)
            .await
        {
            tracing::warn!(agent = %agent_id, error = %e, "record turn usage stats failed");
        }
    }

    /// Map one `session/update` notification and publish/accumulate its
    /// effects. Returns whether the notification mapped to a turn update
    /// (`true` even when a dropped tool update published no event) — the
    /// silent-redrive eligibility in [`run_prompt_turn`](Self::run_prompt_turn)
    /// keys off it (monorepo#764).
    async fn route_notification(
        &self,
        note: &IncomingNotification,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        transcript: &mut Transcript,
    ) -> bool {
        let Some(mapped) = session::map_notification(note) else {
            return false;
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
                let known = transcript.tool_name_for(&tc.tool_call_id).is_some();
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
                    return true;
                };
                // On a known toolCallId the transcript block is the
                // authoritative MERGED state — publish its name/title/kind/
                // input so a sparse (e.g. status-only) update doesn't wipe
                // the row live, and `tool_delta`'s rebuilt block stays
                // byte-identical to the persisted one (§7.1). First-sight
                // events carry the mapped fields verbatim, as before.
                let (tool_name, title, tool_kind, input) = if known {
                    let block = &transcript.blocks[block_index];
                    (
                        block["name"].as_str().unwrap_or(&tc.tool_name).to_string(),
                        block["input"]
                            .get("_acpTitle")
                            .and_then(Value::as_str)
                            .unwrap_or(&tc.title)
                            .to_string(),
                        block["metadata"]["toolKind"]
                            .as_str()
                            .unwrap_or(tc.tool_kind)
                            .to_string(),
                        block["input"].clone(),
                    )
                } else {
                    (
                        tc.tool_name.clone(),
                        tc.title.clone(),
                        tc.tool_kind.to_string(),
                        tc.input.clone(),
                    )
                };
                // D4: enrich additively — keep the existing fields, add agentId,
                // the (previously dropped) toolCallId, and the block identity.
                let mut data = json!({
                    "agentId": agent_id.0,
                    "toolName": tool_name,
                    "title": title,
                    "toolKind": tool_kind,
                    "toolCallId": tc.tool_call_id,
                    "input": input,
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
        true
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
