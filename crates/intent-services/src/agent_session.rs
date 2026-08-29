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

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use intent_acp::session::{
    self, ContentBlock, InitializeResponse, MappedToolCall, MappedUpdate, McpServer, Meta,
    SessionConfigKind, SessionConfigOption, SessionConfigOptionCategory,
    SessionConfigSelectOptions, SessionModeState, StopReason,
};
use intent_acp::{AcpError, Connection, IncomingNotification};
use intent_core::events::{
    AGENT_FAILED, AGENT_IDLE, AGENT_STREAM_ACTIVITY, AGENT_STREAM_END, AGENT_STREAM_START,
    AGENT_STREAM_STATUS, AGENT_TOOL_CALL, AGENT_UPDATED, CHAT_STREAM_DELTA,
};
use intent_core::{
    now_epoch_ms, now_iso, ActorType, AgentId, AgentSession, ContextUsage, Error, EventActor,
    Result, UsageCost, WorkspaceId, WorkspaceStatus,
};
use intent_store::NewEvent;
use serde_json::{json, Value};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::agent_ops::{
    last_response_and_digest_from_blocks, live_response_and_digest_from_blocks,
};
use crate::{token_usage, usage_stats, Services};

/// Derive the cross-layer, content-free stream correlation value used only in
/// diagnostics. The input is an existing wire `turnId` (or the assistant
/// `messageId` on interruption paths that have no turn id); the raw id is never
/// logged. FNV-1a is fixed here so non-Rust clients can derive the same 16-hex
/// value without a dependency or protocol field.
pub(crate) fn opaque_stream_ref(id: &str) -> String {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in id.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    format!("{hash:016x}")
}

/// Emit one bounded, content-free lifecycle diagnostic. Callers pass only a
/// fixed stage/outcome vocabulary and counts; no transcript, identifiers,
/// provider payloads, request ids, or errors enter this record. At most one
/// record is emitted for each stage a turn reaches.
pub(crate) fn trace_stream_lifecycle(
    correlation_id: Option<&str>,
    correlation_basis: &'static str,
    stage: &'static str,
    elapsed: Option<Duration>,
    block_count: usize,
    outcome: &'static str,
) {
    let Some(correlation_id) = correlation_id else {
        return;
    };
    let elapsed_ms = elapsed.map_or(0, |value| {
        u64::try_from(value.as_millis()).unwrap_or(u64::MAX)
    });
    tracing::info!(
        target: "intent_services::stream_lifecycle",
        turnCorrelation = %opaque_stream_ref(correlation_id),
        correlationBasis = correlation_basis,
        stage,
        elapsed_ms,
        elapsed_known = elapsed.is_some(),
        block_count,
        outcome,
        "stream lifecycle"
    );
}

/// Content-free correlation carried from a completed harness-wake turn to its
/// separately published idle stage.
pub(crate) struct HarnessWakeLifecycle {
    pub(crate) correlation_id: String,
    pub(crate) block_count: usize,
}

/// Join the assistant-message correlation used by content stages to the
/// turn-only correlation available to worker fallback paths. Both values are
/// fixed-size opaque hashes; neither raw id enters the diagnostic bundle.
pub(crate) fn trace_stream_correlation_mapping(message_id: &str, turn_id: Option<&str>) {
    let Some(turn_id) = turn_id else {
        return;
    };
    tracing::info!(
        target: "intent_services::stream_lifecycle",
        turnCorrelation = %opaque_stream_ref(message_id),
        turnOnlyCorrelation = %opaque_stream_ref(turn_id),
        correlationBasis = "mapping",
        stage = "correlation_mapping",
        "stream lifecycle correlation mapping"
    );
}

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

/// Suffix appended to the wrapped idle-timeout error when the timed-out turn
/// DID receive `session/update` traffic before going silent (`session/prompt
/// failed: session/prompt idle timeout (…) [turn streamed output]`). ANY
/// received update counts — including variants that don't map to turn updates
/// (plan/thought/mode/usage) — because each one reset the idle timer. The
/// turn worker's warn-and-continue path resets its consecutive-timeout
/// counter on this marker: intervening activity means the back-to-back
/// timeout accounting starts over.
pub(crate) const PROMPT_IDLE_TIMEOUT_STREAMED_SUFFIX: &str = "[turn streamed output]";

/// Prefix marking a `session/prompt` failure that was recognized as
/// sleep-induced (Task C): the turn died with a transient upstream disconnect
/// (per [`intent_acp::is_transient_upstream_disconnect`]) whose active window
/// overlapped a detected host suspend (per the injected [`SuspendOverlapQuery`]).
/// [`Services::run_prompt_turn`] enrolls such a turn as interrupted (persisting
/// the partial with [`InterruptReason::SystemSuspend`] + an `interrupted_agent`
/// row) and emits the interrupted terminal `agent:stream:end` instead of
/// `agent:failed`, so the turn worker suppresses the terminal-failure path (no
/// hard error, no manual-retry surface) and the wake orchestrator (Task D) can
/// resume it.
pub(crate) const PROMPT_SUSPEND_INTERRUPT_PREFIX: &str =
    "session/prompt interrupted by system suspend:";

/// Debounce before the self-healing resume that [`enroll_suspend_interrupted_turn`]
/// fires directly (independent of the host-wake broadcast). It must outlast the
/// turn worker's post-enrollment teardown (`kill_child_only` + `end_turn`) so the
/// resume spawns a fresh child and reloads via `session/load` rather than racing
/// the still-live handle, and it coalesces a burst of concurrent enrollments
/// (the resume itself is idempotent via the row's atomic claim). Overridable for
/// tests via `INTENTD_WAKE_RESUME_SELF_HEAL_MS`.
const WAKE_RESUME_SELF_HEAL_DEBOUNCE: Duration = Duration::from_secs(2);

/// Resolve the self-heal debounce, honoring the `INTENTD_WAKE_RESUME_SELF_HEAL_MS`
/// test seam (milliseconds) so e2e/unit coverage need not wait the production
/// window.
fn wake_resume_self_heal_debounce() -> Duration {
    std::env::var("INTENTD_WAKE_RESUME_SELF_HEAL_MS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .map_or(WAKE_RESUME_SELF_HEAL_DEBOUNCE, Duration::from_millis)
}

/// Query surface the turn driver uses to decide whether a failed turn's active
/// window overlapped a host suspend. Implemented by the daemon's
/// `SuspendTracker` (in the `intentd` binary crate, which depends on this one)
/// and injected via [`Services::with_suspend_tracker`]: the service layer only
/// needs the overlap query, so the concrete clock-skew detector/tracker stays
/// in the binary crate. Absent (read-only / unit-test wiring, or `wakeResume`
/// disabled), sleep-induced enrollment never triggers and turn failures keep
/// today's terminal behavior.
pub trait SuspendOverlapQuery: Send + Sync {
    /// Total suspend duration whose recorded monotonic bracket overlaps the
    /// window `[start, end]`, or `None` when no retained suspend overlaps it.
    fn did_suspend_overlap(&self, start: Instant, end: Instant) -> Option<Duration>;
}

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

/// Max in-place `session/prompt` retries after a transient provider-fetch
/// failure (intent-hq/monorepo#3007) — 3 attempts total before the error
/// surfaces through the existing terminal-failure path. Only output-free
/// attempts are retried (nothing streamed, nothing persisted), so the retried
/// unit is provably idempotent.
const MAX_TRANSIENT_PROMPT_FETCH_RETRIES: u32 = 2;

/// Base delay for the exponential backoff between transient-fetch retries
/// (doubling per attempt: 1s, 2s). Overridable via
/// `INTENTD_TRANSIENT_PROMPT_RETRY_BASE_MS` (test seam).
fn transient_prompt_retry_base_ms() -> u64 {
    if let Ok(val) = std::env::var("INTENTD_TRANSIENT_PROMPT_RETRY_BASE_MS") {
        if let Ok(ms) = val.parse::<u64>() {
            return ms;
        }
    }
    1000
}

/// Result of opening or resuming an ACP session: the canonical `acpSessionId`
/// (persisted on `AgentSession`) plus the modes the provider advertised in the
/// same response, so the caller can pick a permissive `session/set_mode` target
/// only from `availableModes` rather than blindly asking for a mode the agent
/// never offered.
#[derive(Debug, Clone)]
pub(crate) struct AcpSessionOpened {
    /// The canonical `acpSessionId` to drive future turns.
    pub session_id: String,
    /// The modes the provider advertised in `session/new` / `session/load`, if
    /// any. `None` when the provider omitted the field or when a concurrent
    /// recreate won the CAS (the modes we captured belong to the wrong
    /// session).
    pub modes: Option<SessionModeState>,
    /// The provider's reasoning-effort selector discovered in the same
    /// response's `configOptions` (PROTOCOL §5.5), if it advertised one. The
    /// manager applies the session's `reasoningEffort` through it and keeps it
    /// on the live handle so a mid-session change can be re-applied before the
    /// next prompt. `None` when the provider advertises no such option or when
    /// a concurrent recreate won the CAS (see `modes`).
    pub thought_level: Option<ThoughtLevelOption>,
}

/// A provider's reasoning-effort selector: the `thought_level`-category select
/// of a `session/new` / `session/load` response's `configOptions` (PROTOCOL
/// §5.5). Adapters name it differently (`effort` for claude-agent-acp,
/// `reasoning_effort` for codex-acp), so discovery keys on the CATEGORY and
/// carries the adapter's own id for the `session/set_config_option` call.
#[derive(Debug, Clone)]
pub(crate) struct ThoughtLevelOption {
    /// The adapter's config id, sent as `configId`.
    pub config_id: String,
    /// The value the adapter reported as current at session open — the
    /// provider's own default, restored when the session's `reasoningEffort`
    /// is cleared.
    pub initial_value: String,
    /// The value the adapter is currently on (tracked across applications so
    /// an unchanged effort is never re-sent).
    pub current_value: String,
    /// The values the select accepts (flattened across groups). Used to skip
    /// an effort the adapter would reject.
    pub values: Vec<String>,
}

impl ThoughtLevelOption {
    /// The levels worth surfacing to clients (PROTOCOL §5.5, Option C): the
    /// advertised values minus the adapter's case-insensitive `"default"`
    /// sentinel (claude-agent-acp lists it alongside the real levels; it is a
    /// clear-selection affordance, not a level). `None` when nothing remains —
    /// the persisted column stays NULL rather than `Some(empty)`.
    pub(crate) fn surfaced_levels(&self) -> Option<Vec<String>> {
        let levels: Vec<String> = self
            .values
            .iter()
            .filter(|v| !v.eq_ignore_ascii_case("default"))
            .cloned()
            .collect();
        (!levels.is_empty()).then_some(levels)
    }
}

/// Accumulates streamed assistant content into one transcript message per turn,
/// coalescing consecutive text chunks into a single text block and pushing
/// `tool_use`/`tool_result` blocks for tool calls (CS-0 D6). Every block is
/// stamped with a stable id `{messageId}:{blockIndex}` (CS-0 D1), where
/// `messageId` is the assistant `AgentMessage` id minted at turn start; blocks
/// are append-only so a block's index (and thus id) is fixed once assigned.
///
/// Streamed reasoning (`agent_thought_chunk`) shares the same coalescing
/// buffer but flushes as a `thinking` block: consecutive thoughts merge into
/// one block and a thought↔text switch closes the open block and starts a new
/// one (Zed's model), so thoughts interleave with text/tool blocks in stream
/// order.
struct Transcript {
    /// Assistant `AgentMessage` id minted at turn start (the block-id prefix).
    message_id: String,
    blocks: Vec<Value>,
    text: String,
    /// Whether the pending [`text`](Self::text) buffer holds reasoning (flushes
    /// as a `thinking` block) rather than assistant text.
    pending_thought: bool,
    /// `toolCallId` → index of its `tool_use` block (for status patching).
    tool_use_index: HashMap<String, usize>,
    /// `toolCallId` → index of its `tool_result` block (append-once, then patch).
    tool_result_index: HashMap<String, usize>,
    /// `toolCallId` → index of its standalone proposal-resource block (§7.1;
    /// append-once, then patch — mirrors `tool_result_index`).
    proposal_index: HashMap<String, usize>,
    /// Latest cumulative session cost seen in an ACP `usage_update` during
    /// this turn (§5.23). Cumulative per ACP session, so the last one wins;
    /// `None` when the provider reported no cost.
    usage_cost: Option<UsageCost>,
    /// `toolCallId`s recorded by [`record_tool`](Self::record_tool) whose
    /// latest status is non-terminal (neither `completed` nor `error`).
    /// Drives tool-call-aware stall suppression (intent-hq/monorepo#3466):
    /// while non-empty, the mid-turn `stalled` advisory is suppressed —
    /// long tool runs are legitimately silent. Anonymous updates dropped by
    /// `record_tool` (STAB-124) never enter this set.
    open_tool_calls: HashSet<String>,
}

/// The block indices one [`Transcript::record_tool`] call materialized. The
/// `agent:tool:call` event carries the ids derived from them (§7.1
/// `resultBlockId` / `proposalBlockIds`) so the live `chat.subscribe` delta
/// path stamps the REAL persisted ids on its synthesized `tool_result` /
/// proposal-resource blocks instead of predicting `tool_use index + 1` —
/// a prediction that collides with an interleaved text block or a parallel
/// call's `tool_use` (monorepo#2029).
struct RecordedToolBlocks {
    /// Index of the `tool_use` block (the block the event is enriched against).
    use_index: usize,
    /// Index of the `tool_result` block, once the call completed WITH output.
    result_index: Option<usize>,
    /// Indices of the standalone proposal-resource blocks, in attach order.
    proposal_indices: Vec<usize>,
}

impl Transcript {
    fn new(message_id: String) -> Self {
        Self {
            message_id,
            blocks: Vec::new(),
            text: String::new(),
            pending_thought: false,
            tool_use_index: HashMap::new(),
            tool_result_index: HashMap::new(),
            proposal_index: HashMap::new(),
            usage_cost: None,
            open_tool_calls: HashSet::new(),
        }
    }

    /// Number of recorded tool calls still awaiting a terminal
    /// `tool_call_update` (see [`open_tool_calls`](Self::open_tool_calls)).
    fn open_tool_call_count(&self) -> usize {
        self.open_tool_calls.len()
    }

    /// The stable block id for a 0-based block index (`{messageId}:{index}`).
    fn block_id(&self, index: usize) -> String {
        format!("{}:{index}", self.message_id)
    }

    /// Append a streamed chunk to the coalescing buffer and return the index
    /// the block it lands in will occupy once flushed — the same value for
    /// every consecutive chunk of the same kind, so they share one block id. A
    /// thought↔text switch (either direction) flushes the open block first, so
    /// the returned index names the freshly opened one.
    fn push_chunk(&mut self, t: &str, thought: bool) -> usize {
        if thought != self.pending_thought {
            self.flush_text();
            self.pending_thought = thought;
        }
        self.text.push_str(t);
        self.blocks.len()
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

    /// The block type the pending buffer flushes as: `thinking` for streamed
    /// reasoning, `text` for assistant text.
    fn pending_block_type(&self) -> &'static str {
        if self.pending_thought {
            "thinking"
        } else {
            "text"
        }
    }

    fn flush_text(&mut self) {
        if !self.text.is_empty() {
            let index = self.blocks.len();
            let id = self.block_id(index);
            let block_type = self.pending_block_type();
            self.blocks.push(
                json!({ "type": block_type, "id": id, "text": std::mem::take(&mut self.text) }),
            );
        }
        self.pending_thought = false;
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
    /// `Some(`[`RecordedToolBlocks`]`)` naming every block index this update
    /// materialized — the `tool_use` block the `agent:tool:call` event is
    /// enriched against plus the real `tool_result` / proposal-resource
    /// indices the event carries so the live delta path never has to guess
    /// them — or `None` when the update was dropped; callers must skip event
    /// publishing for dropped updates.
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
    fn record_tool(
        &mut self,
        tc: &MappedToolCall,
        registered: Vec<Value>,
    ) -> Option<RecordedToolBlocks> {
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
        let mut result_index = None;
        let mut proposal_indices = Vec::new();
        let completed = tc.status == "completed" || tc.status == "error";
        // Stall suppression bookkeeping (intent-hq/monorepo#3466): every
        // recorded (non-dropped) update flips the id's membership on its
        // latest status — open on a non-terminal status, closed on a
        // terminal one. Reaching here means the update was NOT dropped
        // (anonymous first sights returned above), so STAB-124 stale ids
        // can never leak into the set.
        if completed {
            self.open_tool_calls.remove(&tc.tool_call_id);
        } else {
            self.open_tool_calls.insert(tc.tool_call_id.clone());
        }
        if completed {
            if let Some(output) = &tc.output {
                let is_error = tc.status == "error";
                if let Some(&ri) = self.tool_result_index.get(&tc.tool_call_id) {
                    result_index = Some(ri);
                    if let Some(obj) = self.blocks[ri].as_object_mut() {
                        obj.insert("output".to_string(), output.clone());
                        obj.insert("is_error".to_string(), Value::Bool(is_error));
                    }
                } else {
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
                    result_index = Some(rindex);
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
                        if let Some(&pi) = (i == 0)
                            .then(|| self.proposal_index.get(&tc.tool_call_id))
                            .flatten()
                        {
                            let id = self.block_id(pi);
                            self.blocks[pi] =
                                crate::tool_block::build_proposal_resource_block(&id, &item);
                            proposal_indices.push(pi);
                        } else {
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
                            proposal_indices.push(pindex);
                        }
                    }
                }
            }
        }
        Some(RecordedToolBlocks {
            use_index,
            result_index,
            proposal_indices,
        })
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
    /// (CS-0 D5): the pushed blocks plus, when a chunk buffer is pending, the
    /// synthetic `text`/`thinking` block it will flush into (same index/id it
    /// would ultimately take).
    /// Used to publish the in-flight partial into the per-agent live-turn slot so
    /// a `chat.subscribe` arriving mid-turn can reconstruct it.
    fn snapshot_blocks(&self) -> Vec<Value> {
        let mut blocks = self.blocks.clone();
        if !self.text.is_empty() {
            let index = blocks.len();
            blocks.push(json!({
                "type": self.pending_block_type(),
                "id": self.block_id(index),
                "text": self.text.clone(),
            }));
        }
        blocks
    }

    /// The text of the coalesced `type: "text"` blocks AS THEY STAND mid-turn
    /// (the pushed text blocks plus, when assistant text is pending, the
    /// unflushed buffer) — the input to the `agent:stream:activity` live-preview
    /// derivation. Cheaper than [`snapshot_blocks`](Self::snapshot_blocks):
    /// tool payloads (which can be large mid-turn) are never cloned. Reasoning
    /// never contributes: `thinking` blocks are skipped by the block filter and
    /// a pending THOUGHT buffer is not appended.
    fn text_block_strings(&self) -> Vec<String> {
        let mut out = text_block_strings(&self.blocks);
        if !self.text.is_empty() && !self.pending_thought {
            out.push(self.text.clone());
        }
        out
    }

    /// Whether the FINAL text block is still open (assistant text pending in
    /// the coalescing buffer, not yet flushed by a block boundary). The
    /// live preview derivation only clips the trailing partial line of an
    /// OPEN final block — a text block closed by e.g. a tool call is complete
    /// even without a trailing newline. A pending THOUGHT buffer feeds no
    /// preview, so it leaves the final text block closed.
    fn final_text_block_open(&self) -> bool {
        !self.text.is_empty() && !self.pending_thought
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
    /// Whether the snapshot's FINAL text block was still open (mid-stream) at
    /// capture time — see [`Transcript::final_text_block_open`]. Drives
    /// whether the `AgentLite` live-preview derivation clips the trailing
    /// partial line.
    pub(crate) final_text_block_open: bool,
    /// RFC-3339 timestamp of the most recent stream event observed for this
    /// turn (STAB-125): set when the slot opens and refreshed on every
    /// [`update_live_turn`](Services::update_live_turn), so pollers can tell a
    /// long-but-alive turn from a wedged agent even before anything persists.
    pub(crate) last_activity_at: String,
    /// Leading-edge throttle state for the external `agent:stream:activity`
    /// broadcast: the instant of the last emission, `None` until the turn's
    /// first activity (which therefore emits immediately). Lives in the
    /// live-turn slot so the throttle resets with the slot on stream
    /// end/failure/abort — the next turn's first activity is immediate again.
    pub(crate) last_activity_emit: Option<std::time::Instant>,
    /// Pinned by [`pin_live_turn`](Services::pin_live_turn) on the teardown
    /// paths (monorepo#2056): the slot stays published across the
    /// `worker.abort()` → [`flush_partial_turn_on_interruption`] gap instead of
    /// vanishing with the [`LiveTurnGuard`] drop, so a `chat.subscribe`
    /// snapshot landing in that gap — busy still `true`, the interrupted row
    /// not yet durable — can still reconstruct the partial turn. Cleared with
    /// the slot itself by the flush.
    pub(crate) flush_pending: bool,
    /// Set when the owning flush RAN and could not persist (a genuine store
    /// error), so it deliberately kept the slot as the only copy of the content
    /// (monorepo#2104). The pin stays set — [`LiveTurnGuard::drop`] and
    /// [`clear_unpinned_live_turn`](Services::clear_unpinned_live_turn) must
    /// still leave that content alone, and a later teardown re-pins and gets a
    /// second chance at persisting it. What this flag adds is the distinction
    /// those two consumers do not need but
    /// [`AgentManager::try_begin`](crate::agent_manager::AgentManager) does:
    /// "a flush is IN FLIGHT and will settle this slot" (pinned, not abandoned)
    /// versus "no one is coming for it" (abandoned). A new turn's claim clears
    /// the latter, because an abandoned slot outliving its turn into the next
    /// one is exactly the stale content monorepo#2138 gets gilded as streaming.
    /// Reset by [`pin_live_turn`](Services::pin_live_turn): a fresh pin means a
    /// fresh flush attempt is in flight.
    pub(crate) flush_failed: bool,
}

/// What [`flush_pinned_turn_on_interruption`](Services::flush_pinned_turn_on_interruption)
/// persisted, derived from the slot AS OF FLUSH TIME (monorepo#2110) so the
/// interrupt path's downstream decisions agree with the durable row instead of
/// with a pre-abort clone.
pub(crate) struct FlushedTurn {
    /// Id of the interrupted assistant row this flush appended — `None` when
    /// nothing was appended (the worker's own full row won the `agent_message.id`
    /// UNIQUE collision, or the store errored).
    pub(crate) message_id: Option<String>,
    /// Whether the flushed slot carried any blocks — the zero-output test the
    /// stop-redelivery arm (intent-hq/monorepo#1757) keys off.
    pub(crate) had_output: bool,
    /// Total persisted block count, including tool/thought/resource blocks.
    pub(crate) block_count: usize,
    /// The flushed content's `type: "text"` block strings, for the terminal
    /// `agent:stream:end` live-preview fields.
    pub(crate) text_blocks: Vec<String>,
}

/// Machine-readable cause of a turn interruption, stamped as
/// `metadata.interruptReason` on the persisted interrupted assistant row and
/// carried on the terminal `agent:stream:end` (PROTOCOL §7) so clients can
/// render a reason-specific Stopped indicator live AND after reload. Rows
/// without the field are legacy and render the generic "Stopped".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InterruptReason {
    /// Plain `agent.stop` keep-alive interrupt (user clicked Stop).
    ///
    /// NOT exhaustive per-trigger: a user stop that lands with no live
    /// connection or no `acpSessionId` falls back to the hard kill path and
    /// surfaces as [`AgentStopped`](InterruptReason::AgentStopped) — clients
    /// must not assume every user-initiated stop carries `user_stop`.
    UserStop,
    /// Preempted by an interrupt-priority message (user or agent sender —
    /// see [`InterruptedBy`]).
    PreemptedByMessage,
    /// Graceful daemon shutdown captured the in-flight turn.
    DaemonShutdown,
    /// Hard stop/kill teardown (agent delete, kill-path fallback, …).
    AgentStopped,
    /// The turn's upstream stream dropped because the host suspended (laptop
    /// sleep) mid-turn (Task C): recognized as a transient upstream disconnect
    /// whose active window overlapped a detected suspend. Enrolled as
    /// interrupted for wake-triggered resume instead of surfacing terminally.
    SystemSuspend,
}

impl InterruptReason {
    /// The wire string persisted in metadata and emitted on `stream:end`.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            InterruptReason::UserStop => "user_stop",
            InterruptReason::PreemptedByMessage => "preempted_by_message",
            InterruptReason::DaemonShutdown => "daemon_shutdown",
            InterruptReason::AgentStopped => "agent_stopped",
            InterruptReason::SystemSuspend => "system_suspend",
        }
    }
}

/// Sender attribution for [`InterruptReason::PreemptedByMessage`]: who sent
/// the interrupt-priority message that preempted the turn. Serialized as
/// `{ "kind": "user" }` or `{ "kind": "agent", "agentId": "…", "name": "…" }`
/// under `metadata.interruptedBy` / the `stream:end` `interruptedBy` field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum InterruptedBy {
    /// FE-originated send (`MessageOrigin::User`).
    User,
    /// Agent-to-agent send, attributed via the `messageMetadata`
    /// `fromAgentId`/`fromAgentName` sender-attribution payload (PROTOCOL §5.5).
    Agent {
        agent_id: String,
        name: Option<String>,
    },
}

impl InterruptedBy {
    /// The wire JSON shape (see the enum docs).
    pub(crate) fn to_json(&self) -> Value {
        match self {
            InterruptedBy::User => json!({ "kind": "user" }),
            InterruptedBy::Agent { agent_id, name } => {
                let mut v = json!({ "kind": "agent", "agentId": agent_id });
                if let Some(name) = name {
                    v["name"] = json!(name);
                }
                v
            }
        }
    }
}

/// Minimum spacing between `agent:stream:activity` emissions per agent
/// (leading-edge throttle, PROTOCOL §7): the first activity of a turn emits
/// immediately, then at most one per window.
pub(crate) const ACTIVITY_THROTTLE: std::time::Duration = std::time::Duration::from_secs(1);

/// Per-agent map of live turn slots, shared across [`Services`] clones so the
/// `chat.subscribe` read door and the [`run_prompt_turn`](Services::run_prompt_turn)
/// writer observe the same state.
pub(crate) type LiveTurns = Arc<Mutex<HashMap<AgentId, LiveTurn>>>;

/// Per-agent latest context-window occupancy from ACP `usage_update`
/// (intent-hq/intent#3797): latest-wins per live session, in-memory only —
/// never a token-tally input, dropped with the session on delete and on
/// daemon restart. Shared across [`Services`] clones so the notification
/// writer and the `agent.get`/`agent.list` projection overlay observe the
/// same state.
pub(crate) type ContextUsages = Arc<Mutex<HashMap<AgentId, ContextUsage>>>;

/// Per-agent silent tail (ms of `session/update` silence before the prompt
/// resolved) of the most recently ended turn (intent-hq/monorepo#2669):
/// recorded at turn end by [`run_prompt_turn`](Services::run_prompt_turn) and
/// served by `agent.diagnostics` as `lastTurnSilentTailMs`. In-memory only —
/// the signal is a live-session health indicator, so losing it on restart is
/// fine. Shared across [`Services`] clones.
pub(crate) type LastTurnSilentTails = Arc<Mutex<HashMap<AgentId, u64>>>;

/// Silent-tail threshold past which a bare `end_turn` is suspected to be a
/// silently-truncated turn (intent-hq/monorepo#2669): in that incident,
/// bloated sessions resolved `session/prompt` with a clean `end_turn` after
/// 11–13 minutes of total silence. 8 minutes sits comfortably past normal
/// tool-free inference tails (and above the 5-minute [`stream_stall_ms`]
/// advisory, preserving the stall < silent-tail-suspect < 30-minute prompt
/// idle timeout ordering) while catching the incident signature well
/// before the 30-minute prompt idle timeout. Advisory only — the annotation
/// never interrupts or fails the turn, because healthy long silent tails
/// exist. Overridable via `INTENTD_SILENT_TAIL_SUSPECT_MS` (test seam).
pub(crate) fn silent_tail_suspect_ms() -> u64 {
    if let Ok(val) = std::env::var("INTENTD_SILENT_TAIL_SUSPECT_MS") {
        if let Ok(ms) = val.parse::<u64>() {
            return ms;
        }
    }
    8 * 60 * 1000
}

/// Mid-turn stream-stall threshold (intent-hq/monorepo#3402): after this many
/// ms of zero `session/update` traffic while `session/prompt` is still in
/// flight, [`run_prompt_turn`](Services::run_prompt_turn) emits ONE advisory
/// `agent:stream:status` with `phase: "stalled"` so subscribers can surface
/// the silence live instead of an indefinite spinner. 5 minutes clears the
/// silent thinking phases some models run well past 90s while staying below
/// the 8-minute #2669 silent-tail suspicion window
/// ([`silent_tail_suspect_ms`]) and far below the 30-minute prompt idle
/// timeout, which remains the only terminal mechanism — the stall event never
/// cancels or fails the turn. Tool-call-aware (intent-hq/monorepo#3466):
/// while ≥1 recorded tool call is still open the stalled advisory is fully
/// suppressed regardless of silence duration — long tool runs are expected
/// silence — with the 30-minute prompt idle timeout as the backstop for hung
/// tools. Overridable via `INTENTD_STREAM_STALL_MS` (test seam).
pub(crate) fn stream_stall_ms() -> u64 {
    if let Ok(val) = std::env::var("INTENTD_STREAM_STALL_MS") {
        if let Ok(ms) = val.parse::<u64>() {
            return ms;
        }
    }
    5 * 60 * 1000
}

/// Per-agent consecutive suspected-truncation auto-redrive counter
/// (intent-hq/monorepo#2863): incremented each time `run_prompt_turn` arms an
/// auto-redrive for a suspected-truncated turn, cleared on any turn that
/// resolves without the truncation suspicion (real progress ends the stall
/// episode). In-memory only, like [`LastTurnSilentTails`] — a restart resets
/// the episode, which is safe (the next truncated turn simply starts a fresh
/// bounded episode). Shared across [`Services`] clones.
pub(crate) type TruncationRedrives = Arc<Mutex<HashMap<AgentId, u32>>>;

/// One-shot per-agent truncation auto-redrive handoff flags
/// (intent-hq/monorepo#2863): armed by `run_prompt_turn` when it suppresses
/// the terminal `agent:idle` for a redrive-eligible truncated turn, taken by
/// the turn worker to inject the system nudge turn. Same single-shot
/// stash/take contract as the pending-terminal-error registry
/// (monorepo#2050); worker-abort paths discard stale flags. Shared across
/// [`Services`] clones.
pub(crate) type PendingTruncationRedrives = Arc<Mutex<std::collections::HashSet<AgentId>>>;

/// Max consecutive suspected-truncation auto-redrives per stall episode
/// (intent-hq/monorepo#2863): the 1st–3rd back-to-back truncated turns each
/// get a system nudge; past the cap the turn falls through to today's
/// behavior (terminal `agent:idle` with the #2669 advisory fields, which the
/// #1016 stall-annotation path then surfaces to the parent).
pub(crate) const MAX_CONSECUTIVE_TRUNCATION_REDRIVES: u32 = 3;

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
///
/// One exception (monorepo#2056): a slot pinned by
/// [`pin_live_turn`](Services::pin_live_turn) survives the drop, because the
/// teardown path that pinned it is about to persist the same content via
/// [`flush_partial_turn_on_interruption`](Services::flush_partial_turn_on_interruption)
/// — which clears the slot itself. That hands the slot's lifetime to the flush
/// and closes the window in which the content was neither published nor
/// durable.
pub(crate) struct LiveTurnGuard<'a> {
    live_turns: &'a LiveTurns,
    agent_id: AgentId,
}

impl Drop for LiveTurnGuard<'_> {
    fn drop(&mut self) {
        if let Ok(mut slots) = self.live_turns.lock() {
            // A pinned slot is owned by the in-progress interrupt flush; the
            // orphan case the guard exists for (no flush coming — panic, plain
            // abort) is unpinned and still cleared here.
            if slots.get(&self.agent_id).is_some_and(|s| s.flush_pending) {
                return;
            }
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

/// Whether a finalized harness-wake transcript carries no meaningful content
/// (intent-hq/monorepo#3262): every block is a `text`/`thinking` block whose
/// text is whitespace-only, or the transcript is empty. Tool blocks and
/// question/resource blocks always count as meaningful. Only wake turns that
/// actually OPENED consult this — status-only bursts never open a turn (the
/// wake tick drops unmappable variants), so they can never classify as an
/// empty response.
pub(crate) fn harness_wake_response_is_empty(blocks: &[Value]) -> bool {
    blocks.iter().all(|b| {
        matches!(
            b.get("type").and_then(Value::as_str),
            Some("text" | "thinking")
        ) && b
            .get("text")
            .and_then(Value::as_str)
            .is_none_or(|t| t.trim().is_empty())
    })
}

/// The outcome of a finished harness-wake turn (monorepo#855 /
/// intent-hq/monorepo#3262): the persisted assistant `message_id` (when the
/// burst persisted a row) plus the empty-response classification the caller
/// uses to decide whether the wake was a silent no-op needing recovery.
pub(crate) struct HarnessWakeOutcome {
    /// The persisted assistant row id, or `None` when the burst persisted
    /// nothing. The production drive task keys only off `empty_response`
    /// (the persisted-row event pair already carries the id); exercised by
    /// the wake-turn unit tests (hence the allow — the lib build has no
    /// reader).
    #[allow(dead_code)]
    pub message_id: Option<String>,
    /// `true` when the turn's finalized transcript carried no meaningful
    /// content (see [`harness_wake_response_is_empty`]) — the incident
    /// signature of a failed post-interrupt recovery wake (a single bare
    /// newline accepted as `harness_wake_complete`).
    pub empty_response: bool,
    /// Content-free correlation for the separately published idle stage.
    pub lifecycle: HarnessWakeLifecycle,
}

/// Extract the `type: "text"` block strings from content blocks — the input
/// shape [`last_response_and_digest_from_blocks`] expects.
pub(crate) fn text_block_strings(blocks: &[Value]) -> Vec<String> {
    blocks
        .iter()
        .filter(|b| b.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|b| b.get("text").and_then(Value::as_str))
        .map(str::to_string)
        .collect()
}

/// Cap on the stamped `lastAgentResponse` preview (chars). The helper's
/// last-line extraction is unbounded for a response without newlines — that
/// is tolerable on the pull path (`agent.list`), but the event payload is
/// re-broadcast up to 1/s and persisted on `agent:stream:end`, so the stamp
/// keeps only the trailing slice (same tail-wins convention as
/// [`last_response_summary`]).
const PREVIEW_RESPONSE_CAP: usize = 500;

/// Stamp the server-derived preview onto a TERMINAL event payload
/// (`agent:stream:end`): derive `(lastAgentResponse, digest)` from the full
/// turn's `text_blocks` — complete by definition — and set only the fields
/// that derived to `Some`; a turn that produced no text (or no digest) omits
/// that field rather than sending an empty string.
pub(crate) fn stamp_preview_fields(data: &mut Value, text_blocks: &[String]) {
    stamp_preview(data, last_response_and_digest_from_blocks(text_blocks));
}

/// [`stamp_preview_fields`] for MID-TURN frames (`agent:stream:activity`):
/// derives via the live variant, which clips the still-streaming trailing
/// partial line when the final text block is open (`final_block_open`) — the
/// preview advances on newline boundaries, a turn that has not completed a
/// non-empty line yet omits `lastAgentResponse`, and a partially-streamed
/// `<agent_digest>` span (or split marker at a chunk boundary) never
/// surfaces. A final text block CLOSED by a non-text block boundary (e.g. a
/// tool call) is complete and serves its last line unclipped.
pub(crate) fn stamp_live_preview_fields(
    data: &mut Value,
    text_blocks: &[String],
    final_block_open: bool,
) {
    stamp_preview(
        data,
        live_response_and_digest_from_blocks(text_blocks, final_block_open),
    );
}

/// Shared stamping core for [`stamp_preview_fields`] /
/// [`stamp_live_preview_fields`].
fn stamp_preview(data: &mut Value, (last_response, digest): (Option<String>, Option<String>)) {
    if let Some(r) = last_response {
        let chars: Vec<char> = r.chars().collect();
        let capped = if chars.len() > PREVIEW_RESPONSE_CAP {
            let tail: String = chars[chars.len() - PREVIEW_RESPONSE_CAP..].iter().collect();
            format!("...{tail}")
        } else {
            r
        };
        data["lastAgentResponse"] = Value::String(capped);
    }
    if let Some(d) = digest {
        data["digest"] = Value::String(d);
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

/// Derive the user-configured default provider from the effective settings:
/// the provider prefix of the configured default model (`model.default`
/// compound prefix), else `providers.active`. Each candidate is validated
/// against the provider registry ([`intent_providers::find_provider`]) so a
/// stale, mistyped, or foreign-build id falls through to the next precedence
/// step instead of being trusted (an unknown `model.default` prefix must not
/// shadow a perfectly valid `providers.active`). `None` when neither yields
/// a registered provider — no provider carries a hardcoded default
/// designation, and there is no positional last resort (monorepo#3044):
/// resolution that falls through entirely fails loudly at the caller.
pub(crate) fn derived_default_provider(
    settings: &intent_core::settings_file::SettingsFile,
) -> Option<String> {
    /// Accept a candidate id only when it names a registered provider
    /// (whitespace-trimmed, so padded settings values still resolve).
    fn registered(id: &str) -> Option<String> {
        let id = id.trim();
        intent_providers::find_provider(id).map(|p| p.id.to_string())
    }
    settings
        .model
        .default
        .as_deref()
        .filter(|m| m.contains(':'))
        .map(|m| intent_providers::parse_compound_model_id(m).0)
        .and_then(|id| registered(&id))
        .or_else(|| settings.providers.active.as_deref().and_then(registered))
}

/// Resolve the effective provider id for an agent session using the same precedence
/// as the spawn path (§6.9): model's compound prefix (if `model` contains `:` and
/// yields a non-empty provider) → `provider` field → `configured_default` (the
/// settings-derived default — see [`derived_default_provider`] — when the
/// caller has one to offer). Malformed compound ids like `:sonnet` yield an
/// empty prefix and fall through to the provider field / configured default.
/// This ensures `_meta` injection, spawn args, and all provider-keyed logic
/// use a consistent provider id.
///
/// `None` when nothing resolves (monorepo#3044): the former positional last
/// resort (the first registered provider, auggie) silently spawned a binary
/// that may not be installed. Spawn-adjacent callers surface
/// [`no_default_provider_error`] instead; stats-attribution callers (which
/// pass `configured_default: None`) fall to their existing `"unknown"` tail.
pub(crate) fn resolve_provider_id(
    model: Option<&str>,
    provider: Option<&str>,
    configured_default: Option<&str>,
) -> Option<String> {
    model
        .filter(|m| m.contains(':'))
        .map(|m| intent_providers::parse_compound_model_id(m).0)
        .filter(|id| !id.is_empty()) // guard against malformed compound ids like ":sonnet"
        .or_else(|| {
            provider
                .filter(|p| !p.is_empty())
                .map(std::string::ToString::to_string)
        })
        .or_else(|| {
            configured_default
                .filter(|p| !p.is_empty())
                .map(std::string::ToString::to_string)
        })
}

/// The loud `-32602`-style error for provider resolution that falls through
/// entirely (monorepo#3044): no explicit provider/model, no session provider,
/// and no settings-derived default. Mirrors the resolved-but-unavailable
/// error style (`ensure_provider_available`) so clients can surface it
/// directly. `context` names the failing operation (e.g. `agent.delegate`).
pub(crate) fn no_default_provider_error(context: &str) -> Error {
    Error::InvalidParams(format!(
        "{context}: no default provider/model is configured — no explicit \
         provider or model was given and neither providers.active nor a \
         compound model.default is set. Choose a provider in Settings > \
         Agents, or pass an explicit provider/model."
    ))
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
    usage_stats::version_bearing_display(candidates)
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
    usage_stats::version_bearing_display(candidates)
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

/// Discover the provider's reasoning-effort selector in a `session/new` /
/// `session/load` response's `configOptions` (PROTOCOL §5.5): the first
/// SELECT whose `category` is `thought_level`. Adapters pick their own ids
/// (`effort` for claude-agent-acp, `reasoning_effort` for codex-acp), so the
/// category is the only portable key; the discovered id is what the
/// subsequent `session/set_config_option` must carry. `None` when the
/// provider advertises no such option (every non-supporting provider, which
/// then silently ignores the session's `reasoningEffort`).
fn discover_thought_level(
    config_options: Option<&[SessionConfigOption]>,
) -> Option<ThoughtLevelOption> {
    let (option, select) = config_options?.iter().find_map(|o| match &o.kind {
        SessionConfigKind::Select(s)
            if matches!(o.category, Some(SessionConfigOptionCategory::ThoughtLevel)) =>
        {
            Some((o, s))
        }
        _ => None,
    })?;
    let values = match &select.options {
        SessionConfigSelectOptions::Ungrouped(opts) => {
            opts.iter().map(|e| e.value.0.to_string()).collect()
        }
        SessionConfigSelectOptions::Grouped(groups) => groups
            .iter()
            .flat_map(|g| g.options.iter())
            .map(|e| e.value.0.to_string())
            .collect(),
        _ => Vec::new(),
    };
    Some(ThoughtLevelOption {
        config_id: option.id.0.to_string(),
        initial_value: select.current_value.0.to_string(),
        current_value: select.current_value.0.to_string(),
        values,
    })
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

/// Build provider-specific `_meta` for `session/new` and `session/load` from the
/// assembled system prompt (§18.1), the agent's name (task-derived for
/// delegated agents; may be user-assigned or renamed), and the session
/// specialist's resolved orchestrator role (§18.4). Returns
/// `None` for providers that do not use `_meta` injection (auggie, droid,
/// opencode, cortex, pi, grok, mock use other mechanisms).
/// Provider-specific shapes:
/// - claude-code: `{ "claudeCode": { "options": { "disallowedTools": [...] } }, "systemPrompt": "<prompt>"? }`
///   (disallowedTools always present; systemPrompt present only when non-blank
///   prompt). `disallowedTools` carries `Task` for every agent (native-subagent
///   denial) plus, for orchestrator-role agents, the SDK's built-in file-write
///   tools ([`intent_acp::CLAUDE_CODE_ORCHESTRATOR_DISALLOWED_TOOLS`]) — bare
///   names remove the tool from the model's context entirely. Merge behavior
///   verified against the pinned adapter 0.66.0: `createSession` (shared by
///   `session/new` and `session/load`) spread-merges user-provided entries with
///   its own additions — `[...(userProvidedOptions?.disallowedTools || []),
///   ...internal]` — rather than overwriting them (the drop-user-entries
///   regression tracked upstream in claude-agent-acp #294/#334 is fixed there);
///   re-verify on adapter bumps.
///   A string `systemPrompt` fully REPLACES the `claude_code` preset
///   prompt (verified against adapter 0.66.0: a string `_meta.systemPrompt` is
///   passed to the SDK as-is, and SDK 0.3.220 treats a string as a custom
///   prompt) — the model sees only our assembled prompt, with none of the
///   preset's tool-usage/dynamic sections that previously diluted it via the
///   `{ append, excludeDynamicSections }` object shape.
/// - codex: `{ "sessionTitle": "<agent name>" }?` (present only when a non-blank
///   `session_title` is supplied — monorepo#3151; older adapters ignore the
///   unknown field). The system prompt stays on the first-turn prepend fallback
///   because the pinned codex-acp adapter (1.6.2) ignores
///   `_meta.developerInstructions` (#479) — it is never moved into `_meta`.
fn build_session_meta(
    provider_id: &str,
    system_prompt: Option<&str>,
    session_title: Option<&str>,
    is_orchestrator: bool,
) -> Option<Meta> {
    match provider_id {
        "claude-code" => {
            let mut meta = Meta::new();

            // Native tool denylist: `Task` always (agents must delegate via
            // the workspace `ws.agent.*` surface, not provider-native
            // subagents); orchestrators additionally lose the SDK's built-in
            // file-write tools — the same resolved role decision that gates
            // the spawn-time CLI-side denylist (`get_tools_to_remove`, §18.4).
            let mut disallowed = vec!["Task"];
            if is_orchestrator {
                disallowed.extend_from_slice(intent_acp::CLAUDE_CODE_ORCHESTRATOR_DISALLOWED_TOOLS);
            }
            meta.insert(
                "claudeCode".to_string(),
                serde_json::json!({
                    "options": {
                        "disallowedTools": disallowed
                    }
                }),
            );

            // Add systemPrompt as a plain string if a non-blank prompt exists:
            // the string shape fully replaces the claude_code preset prompt so
            // the model sees only our assembled instructions.
            if let Some(prompt) = system_prompt {
                let prompt = prompt.trim();
                if !prompt.is_empty() {
                    meta.insert(
                        "systemPrompt".to_string(),
                        Value::String(prompt.to_string()),
                    );
                }
            }

            Some(meta)
        }
        "codex" => {
            // Title the durable Codex thread out of band so it stops deriving
            // its title from the prepended system prompt (monorepo#3151).
            let title = session_title.map(str::trim).filter(|t| !t.is_empty())?;
            let mut meta = Meta::new();
            meta.insert("sessionTitle".to_string(), Value::String(title.to_string()));
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
    /// Marks the final text block as OPEN (mid-stream) — the common case; use
    /// [`set_live_turn_closed_final_block`](Self::set_live_turn_closed_final_block)
    /// to simulate a snapshot whose final text block was closed by a non-text
    /// block boundary.
    pub fn set_live_turn(&self, agent_id: &AgentId, message_id: &str, blocks: Vec<Value>) {
        self.insert_live_turn(agent_id, message_id, blocks, true);
    }

    /// Test seam: [`set_live_turn`](Self::set_live_turn) with the final text
    /// block marked CLOSED (e.g. flushed by a tool call, no new text since) —
    /// the live preview derivation must not clip it.
    #[cfg(test)]
    pub(crate) fn set_live_turn_closed_final_block(
        &self,
        agent_id: &AgentId,
        message_id: &str,
        blocks: Vec<Value>,
    ) {
        self.insert_live_turn(agent_id, message_id, blocks, false);
    }

    fn insert_live_turn(
        &self,
        agent_id: &AgentId,
        message_id: &str,
        blocks: Vec<Value>,
        final_text_block_open: bool,
    ) {
        if let Ok(mut slots) = self.live_turns.lock() {
            slots.insert(
                agent_id.clone(),
                LiveTurn {
                    message_id: message_id.to_string(),
                    blocks,
                    final_text_block_open,
                    last_activity_at: now_iso(),
                    last_activity_emit: None,
                    flush_pending: false,
                    flush_failed: false,
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
                slot.final_text_block_open = transcript.final_text_block_open();
                slot.last_activity_at = now_iso();
            }
        }
    }

    /// Clear an agent's live-turn slot unconditionally — the interrupt flush's
    /// own release (it owns the pin it clears) and the test seam. The
    /// [`LiveTurnGuard`] also clears on drop for the interrupt/abort path.
    pub fn clear_live_turn(&self, agent_id: &AgentId) {
        if let Ok(mut slots) = self.live_turns.lock() {
            slots.remove(agent_id);
        }
    }

    /// Record the latest context-window occupancy reported by an ACP
    /// `usage_update` for `agent_id` (intent-hq/intent#3797): latest-wins,
    /// in-memory only — never folded into token tallies. `pub(crate)` so
    /// in-crate tests can seed the registry without driving a stream.
    pub(crate) fn record_context_usage(&self, agent_id: &AgentId, used: u64, size: u64) {
        if let Ok(mut usages) = self.context_usages.lock() {
            usages.insert(
                agent_id.clone(),
                ContextUsage {
                    used,
                    size,
                    updated_at: now_iso(),
                },
            );
        }
    }

    /// The latest recorded context-window occupancy for `agent_id`, or `None`
    /// when no live `usage_update` has reported one (fresh session, daemon
    /// restart). Read by the `AgentLite` service projection overlay.
    pub(crate) fn context_usage_for(&self, agent_id: &AgentId) -> Option<ContextUsage> {
        self.context_usages
            .lock()
            .ok()
            .and_then(|usages| usages.get(agent_id).cloned())
    }

    /// Drop an agent's recorded context occupancy (registry hygiene, mirrors
    /// the other per-agent in-memory maps): called on agent delete, session
    /// recreate (CAS winner only — the old session's report is stale), and
    /// the vanished-session cleanup sweep, so the map never leaks entries or
    /// serves a snapshot for a session that no longer exists.
    pub(crate) fn clear_context_usage(&self, agent_id: &AgentId) {
        if let Ok(mut usages) = self.context_usages.lock() {
            usages.remove(agent_id);
        }
    }

    /// Record the just-ended turn's silent tail (intent-hq/monorepo#2669) —
    /// ms of `session/update` silence between the turn's last streamed
    /// activity and the prompt resolving. One slot per agent, latest turn
    /// wins; served by `agent.diagnostics` as `lastTurnSilentTailMs`.
    /// `pub(crate)` as a test seam so in-crate tests can seed the map
    /// without driving a real turn. The map is a plain `HashMap` with no
    /// invariant to tear, so a poisoned lock recovers via `into_inner()`
    /// (matching the workspace-delete sweep in `lib.rs`).
    pub(crate) fn record_turn_silent_tail(&self, agent_id: &AgentId, silent_tail_ms: u64) {
        self.last_turn_silent_tails
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(agent_id.clone(), silent_tail_ms);
    }

    /// The recorded silent tail of the agent's most recently ended turn, if
    /// any turn ended this daemon lifetime — see
    /// [`record_turn_silent_tail`](Self::record_turn_silent_tail).
    pub(crate) fn last_turn_silent_tail(&self, agent_id: &AgentId) -> Option<u64> {
        self.last_turn_silent_tails
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(agent_id)
            .copied()
    }

    /// Drop the agent's recorded silent tail (agent delete / workspace delete
    /// teardown) so the in-memory map never leaks entries for dead agents.
    pub(crate) fn clear_turn_silent_tail(&self, agent_id: &AgentId) {
        self.last_turn_silent_tails
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(agent_id);
    }

    /// Increment and return the agent's consecutive suspected-truncation
    /// auto-redrive count (intent-hq/monorepo#2863). Called when
    /// `run_prompt_turn` decides a truncated turn is redrive-eligible; the
    /// returned count (1-based) is compared against
    /// [`MAX_CONSECUTIVE_TRUNCATION_REDRIVES`] by the caller.
    pub(crate) fn bump_truncation_redrives(&self, agent_id: &AgentId) -> u32 {
        let mut map = self
            .truncation_redrives
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let count = map.entry(agent_id.clone()).or_insert(0);
        *count += 1;
        *count
    }

    /// Clear the agent's suspected-truncation redrive counter
    /// (intent-hq/monorepo#2863) — called on any turn resolution WITHOUT the
    /// truncation suspicion (the stall episode ended: the agent made real
    /// progress or failed through a different path) and on agent/workspace
    /// delete teardown so the map never leaks entries for dead agents.
    pub(crate) fn clear_truncation_redrives(&self, agent_id: &AgentId) {
        self.truncation_redrives
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(agent_id);
    }

    /// Arm the one-shot truncation auto-redrive handoff flag
    /// (intent-hq/monorepo#2863): set by `run_prompt_turn` when it suppresses
    /// the terminal `agent:idle` for a redrive-eligible truncated turn, taken
    /// by the turn worker right after `run_turn` returns to inject the nudge
    /// turn. Same stash/take shape as the pending-terminal-error handoff
    /// (monorepo#2050).
    pub(crate) fn arm_truncation_redrive(&self, agent_id: &AgentId) {
        self.pending_truncation_redrive
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(agent_id.clone());
    }

    /// Take (and clear) the truncation auto-redrive handoff flag for
    /// `agent_id` — see [`arm_truncation_redrive`](Self::arm_truncation_redrive).
    /// Worker-abort paths (stop/interrupt/retry/delete) also route here to
    /// discard a stale flag: an orphaned arm from an aborted turn must not
    /// redrive a later, unrelated turn.
    pub(crate) fn take_truncation_redrive(&self, agent_id: &AgentId) -> bool {
        self.pending_truncation_redrive
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(agent_id)
    }

    /// The intent-hq/monorepo#2863 auto-redrive eligibility predicate: the
    /// agent is DELEGATED (has a parent), has an assigned task note whose
    /// status is exactly `in_progress`, and has NOT persisted a completion
    /// report (a report means the child considers the work done — the parent
    /// must receive that wake, not a redrive). Root/user-facing agents and
    /// taskless agents keep today's WARN + advisory behavior. Every store
    /// failure fails CLOSED (returns `false`) — an auto-redrive is an
    /// automatic turn on the user's behalf, so uncertainty must fall through
    /// to the observable idle path, never spin a turn on stale state.
    pub(crate) async fn truncation_redrive_eligible(&self, agent_id: &AgentId) -> bool {
        let Ok(session) = self.store.get_agent_session(agent_id).await else {
            return false;
        };
        if session.parent_agent_id.is_none() {
            return false;
        }
        if session
            .completion_report
            .as_deref()
            .is_some_and(|r| !r.is_empty())
        {
            return false;
        }
        let Some(task_note_id) = session.task_note_id.as_ref() else {
            return false;
        };
        let Ok(note) = self
            .store
            .get_note(&session.workspace_id, task_note_id)
            .await
        else {
            return false;
        };
        matches!(
            note.metadata.task.as_ref().map(|t| t.status),
            Some(intent_core::TaskStatus::InProgress)
        )
    }

    /// Clear an agent's live-turn slot at a NORMAL turn end, leaving a pinned
    /// slot alone — the same rule [`LiveTurnGuard::drop`] applies, for the same
    /// reason: a pinned slot belongs to the teardown flush that is about to
    /// persist it.
    ///
    /// Without this the pin's invariant had a hole (monorepo#2110 review): a
    /// turn completing normally in the pin→flush gap unpublished the slot, and
    /// because `run_prompt_turn` persists an assistant row only for a turn that
    /// produced blocks, a ZERO-OUTPUT completion left nothing at all behind —
    /// no durable row and no slot. The teardown flush then had no content to
    /// record, costing the interruption both its marker row and (via
    /// `had_output`) the zero-output stop-redelivery arm. Holding the pinned
    /// slot means the flush always sees the turn as it really ended: empty
    /// blocks flush as the marker row, and a completion that DID persist a full
    /// row is absorbed by the flush's `agent_message.id` UNIQUE collision path.
    pub(crate) fn clear_unpinned_live_turn(&self, agent_id: &AgentId) {
        if let Ok(mut slots) = self.live_turns.lock() {
            if slots.get(agent_id).is_some_and(|s| s.flush_pending) {
                return;
            }
            slots.remove(agent_id);
        }
    }

    /// Clear an agent's live-turn slot at a new turn's CLAIM (monorepo#2138),
    /// leaving alone only a slot whose owning flush is still in flight.
    ///
    /// A slot can outlive its turn — a flush that hit a genuine store error
    /// keeps it as the only copy of the content — and if it survives into the
    /// next turn, the window between the claim and that turn's
    /// [`begin_live_turn`](Self::begin_live_turn) serves the PREVIOUS turn's
    /// content as `isStreaming: true`. Clearing it with the claim closes that.
    ///
    /// The exception is narrower than [`clear_unpinned_live_turn`]'s: an
    /// in-flight teardown flush owns its pinned slot and re-reads it at flush
    /// time (monorepo#2110), so clearing it under that flush would silently drop
    /// the content AND make the flush read the slot as vanished — which it
    /// interprets as "the worker already persisted the full row". `interrupt_inner`
    /// pins without a busy claim (a stop against an IDLE agent), so that
    /// interleaving is reachable. But a flush that already gave up is NOT
    /// coming back for the slot, so an abandoned slot is cleared like any other
    /// orphan — otherwise the deliberate flush-failure keep, the very case
    /// monorepo#2104 exists to make visible, would sail into the next turn and
    /// be gilded as streaming again.
    pub(crate) fn clear_live_turn_unless_flush_in_flight(&self, agent_id: &AgentId) {
        if let Ok(mut slots) = self.live_turns.lock() {
            if slots
                .get(agent_id)
                .is_some_and(|s| s.flush_pending && !s.flush_failed)
            {
                return;
            }
            slots.remove(agent_id);
        }
    }

    /// Record that the owning flush ran and could not persist, so the slot it
    /// deliberately kept is no longer waiting on anything — see
    /// [`LiveTurn::flush_failed`] and
    /// [`clear_live_turn_unless_flush_in_flight`](Self::clear_live_turn_unless_flush_in_flight).
    /// Leaves the pin and the content untouched.
    fn mark_live_turn_flush_failed(&self, agent_id: &AgentId) {
        if let Ok(mut slots) = self.live_turns.lock() {
            if let Some(slot) = slots.get_mut(agent_id) {
                slot.flush_failed = true;
            }
        }
    }

    /// Leading-edge throttle gate for the external `agent:stream:activity`
    /// broadcast (PROTOCOL §7): returns `true` (and stamps the slot) when the
    /// turn's live slot has never emitted or the last emission is at least
    /// [`ACTIVITY_THROTTLE`] ago — the first activity of a turn is therefore
    /// immediate, subsequent ones are at most one per window. The state lives
    /// in the live-turn slot, so it resets when the slot clears on stream
    /// end/failure/abort. `false` with no slot open (no turn to signal for).
    pub(crate) fn should_emit_activity(&self, agent_id: &AgentId) -> bool {
        let Ok(mut slots) = self.live_turns.lock() else {
            return false;
        };
        let Some(slot) = slots.get_mut(agent_id) else {
            return false;
        };
        let now = std::time::Instant::now();
        match slot.last_activity_emit {
            Some(last) if now.duration_since(last) < ACTIVITY_THROTTLE => false,
            _ => {
                slot.last_activity_emit = Some(now);
                true
            }
        }
    }

    /// Read an agent's in-flight turn slot, if a turn is currently streaming.
    pub(crate) fn live_turn(&self, agent_id: &AgentId) -> Option<LiveTurn> {
        self.live_turns.lock().ok()?.get(agent_id).cloned()
    }

    /// Pin an agent's in-flight turn slot (monorepo#2056): the
    /// [`LiveTurnGuard`] drop that follows `worker.abort()` leaves a pinned
    /// slot published, so the partial content stays visible to `chat.subscribe`
    /// until [`flush_pinned_turn_on_interruption`](Self::flush_pinned_turn_on_interruption)
    /// makes it durable (that flush clears the slot, releasing the pin).
    /// Without the pin the content is neither published nor persisted for the
    /// width of the interrupt flush's INSERT, and a snapshot taken there drops
    /// the whole partial turn — permanently, since nothing re-publishes it.
    ///
    /// Callers are exactly the three teardown paths (keep-alive interrupt,
    /// hard stop, graceful shutdown), which pin immediately BEFORE aborting
    /// the worker and flush AFTER it. Deliberately returns NOTHING (monorepo#2110):
    /// the flush re-reads the slot — which the pin guarantees is still there,
    /// against both the [`LiveTurnGuard`] drop and a normal turn end (see
    /// [`clear_unpinned_live_turn`](Self::clear_unpinned_live_turn)) — so a
    /// `session/update` processed in the pin→abort gap is persisted rather than
    /// trimmed off a stale pre-abort clone. A no-op when no turn is in flight;
    /// the flush's `None` says so on its own. Pinning is idempotent and never
    /// outlives the turn: the next turn's [`begin_live_turn`](Self::begin_live_turn)
    /// replaces the slot wholesale.
    pub(crate) fn pin_live_turn(&self, agent_id: &AgentId) {
        if let Ok(mut slots) = self.live_turns.lock() {
            if let Some(slot) = slots.get_mut(agent_id) {
                slot.flush_pending = true;
                // A fresh pin means a fresh flush attempt is in flight, so an
                // earlier give-up no longer describes this slot: re-pinning a
                // slot a previous flush abandoned (a later teardown, e.g.
                // shutdown, reaching the same stranded content) gives it a real
                // second chance at persisting, and must not leave it looking
                // abandoned to `try_begin` in the meantime.
                slot.flush_failed = false;
            }
        }
    }

    /// Read just the text of the live-turn slot's `type: "text"` blocks
    /// (plus the slot's final-text-block-open flag) without cloning the full
    /// slot — the `AgentLite` preview overlay only needs the text strings, so
    /// `tool_use`/`tool_result` payloads (which can be large mid-turn) stay
    /// untouched under the lock. `None` when no slot is open;
    /// `Some((vec![], _))` when a slot is open but has no text blocks yet.
    pub(crate) fn live_turn_text_blocks(&self, agent_id: &AgentId) -> Option<(Vec<String>, bool)> {
        self.live_turns
            .lock()
            .ok()?
            .get(agent_id)
            .map(|live| (text_block_strings(&live.blocks), live.final_text_block_open))
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

    /// Flush the CURRENT content of an agent's pinned live-turn slot — the
    /// teardown paths' entry point into
    /// [`flush_partial_turn_on_interruption`](Self::flush_partial_turn_on_interruption).
    ///
    /// The slot is re-read HERE, after `worker.abort()`, rather than taken from
    /// the clone [`pin_live_turn`](Self::pin_live_turn) used to hold
    /// (monorepo#2110). The abort does not stop the notification already being
    /// routed, so a `session/update` processed between the pin and the worker's
    /// cancellation used to be trimmed out of the durable row — after it had
    /// already been broadcast to every subscriber, leaving the transcript short
    /// of what clients saw stream. The pin is what makes re-reading safe: a
    /// pinned slot survives the [`LiveTurnGuard`] drop, so it is still there to
    /// read.
    ///
    /// `None` means nothing was pinned — no turn in flight. A pinned slot
    /// cannot vanish before this flush ([`LiveTurnGuard::drop`] and the normal
    /// turn-end clear both leave it to the flush that owns it), and the
    /// `flush_pending` filter below keeps this flush off a slot it does NOT
    /// own: the next turn's [`begin_live_turn`](Self::begin_live_turn) landing
    /// in the pin→flush window replaces the slot wholesale (unpinned), and
    /// persisting THAT content here would record a live turn as interrupted
    /// under its freshly minted id — poisoning the id the worker's own append
    /// still needs.
    pub(crate) async fn flush_pinned_turn_on_interruption(
        &self,
        agent_id: &AgentId,
        reason: InterruptReason,
        interrupted_by: Option<&InterruptedBy>,
    ) -> Option<FlushedTurn> {
        let live = self.live_turn(agent_id).filter(|live| live.flush_pending)?;
        // Derived before the flush consumes the blocks; `text_block_strings`
        // copies only the text, leaving mid-turn tool payloads uncloned.
        let had_output = !live.blocks.is_empty();
        let block_count = live.blocks.len();
        let text_blocks = text_block_strings(&live.blocks);
        let message_id = self
            .flush_partial_turn_on_interruption(agent_id, live, reason, interrupted_by, true)
            .await;
        Some(FlushedTurn {
            message_id,
            had_output,
            block_count,
            text_blocks,
        })
    }

    /// Best-effort flush of an agent's partial in-flight assistant content at
    /// interruption-capture time (graceful shutdown, INT-41 follow-up): persist
    /// the caller-captured live-turn snapshot as a normal `assistant` row tagged
    /// `metadata.interrupted = true` + `stopReason = "interrupted"` (the
    /// terminal-message convention the FE stopped-indicator keys off; `status`
    /// is kept as a redundant tag) so the transcript keeps the streamed-so-far
    /// output across the restart. Reuses the turn's minted `message_id` (CS-0
    /// D1) so persisted block ids `{messageId}:{index}` match what streamed.
    /// The teardown paths reach this through
    /// [`flush_pinned_turn_on_interruption`](Self::flush_pinned_turn_on_interruption),
    /// which supplies the pinned slot's content as of flush time; the suspend
    /// enrollment path (which owns the turn and has no worker to abort) passes
    /// its content directly. The teardown convention is pin BEFORE aborting the
    /// turn worker, flush AFTER the abort so the worker cannot race the append;
    /// the pin keeps the slot published across that gap (monorepo#2056) and this
    /// flush owns releasing it. If the worker already persisted the full turn,
    /// the append collides on the UNIQUE id and is logged at debug (benign —
    /// the full row won; the stale slot, if any, is cleared). Errors are logged
    /// and swallowed: this must never block shutdown or the `interrupted_agent`
    /// row insert. On a genuine store error the slot is deliberately KEPT (pin
    /// and all) as the only remaining copy of the content.
    ///
    /// The row also carries a machine-readable `interruptReason` (plus
    /// `interruptedBy` sender attribution for message preemption) so the FE
    /// can render a reason-specific Stopped indicator that survives reloads —
    /// see [`InterruptReason`] for the enum ↔ wire-string mapping.
    ///
    /// Empty-blocks slots (turn started, nothing streamed yet) persist too:
    /// EVERY interruption durably records an interrupted assistant row —
    /// empty blocks allowed — so the FE always has a row to anchor the
    /// indicator on, even when the turn produced zero output. (This
    /// supersedes the STAB-114 phantom-row-free zero-output preemption; the
    /// combined-delivery re-queue check in `preempt_busy_turn` excludes the
    /// row this flush appends.)
    ///
    /// Returns the persisted interrupted row's message id (`Some` only when
    /// this flush appended the row), so the interrupt path can carry
    /// `messageId` on the terminal `agent:stream:end`.
    /// `owns_slot` says whose slot this flush may release: the teardown flush
    /// owns the pin it is flushing and clears unconditionally; the suspend
    /// enrollment flushes caller-held content and must NOT release a pin a
    /// concurrent teardown holds on the agent's slot — clearing it would cost
    /// that teardown's flush the true `had_output` (the same monorepo#2110
    /// zero-output flip, resurfacing through this side door).
    pub(crate) async fn flush_partial_turn_on_interruption(
        &self,
        agent_id: &AgentId,
        live: LiveTurn,
        reason: InterruptReason,
        interrupted_by: Option<&InterruptedBy>,
        owns_slot: bool,
    ) -> Option<String> {
        let block_count = live.blocks.len();
        let mut metadata = json!({
            "interrupted": true,
            "stopReason": "interrupted",
            "status": "interrupted",
            "interruptReason": reason.as_str(),
        });
        // `interruptedBy` is defined ONLY for message preemption (wire
        // contract): the reason gate keeps a misusing caller from leaking
        // attribution onto other interruption reasons.
        if reason == InterruptReason::PreemptedByMessage {
            if let Some(by) = interrupted_by {
                metadata["interruptedBy"] = by.to_json();
            }
        }
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
            Ok(message) => {
                trace_stream_lifecycle(
                    Some(live.message_id.as_str()),
                    "message",
                    "assistant_persisted",
                    None,
                    block_count,
                    "interrupted",
                );
                // Best-effort: resolve workspace from the session so the
                // projection cache drops without requiring the caller to pass
                // workspace_id on this interrupt flush path. The same lookup
                // scopes the persisted-row event pair (§6.5) — the flushed
                // partial row is a transcript persist like any other, so
                // preview subscribers converge on its `lastToolUse` too.
                if let Ok(session) = self.store.get_agent_session_summary(agent_id).await {
                    self.invalidate_agent_list_cache(&session.workspace_id);
                    self.publish_agent_message_events(
                        &session.workspace_id,
                        agent_id,
                        &message,
                        None,
                    )
                    .await;
                }
                if owns_slot {
                    self.clear_live_turn(agent_id);
                } else {
                    self.clear_unpinned_live_turn(agent_id);
                }
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
                // The durable full row exists — drop the now-stale overlay too
                // (same ownership rule as the success arm).
                if owns_slot {
                    self.clear_live_turn(agent_id);
                } else {
                    self.clear_unpinned_live_turn(agent_id);
                }
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
                // The slot is KEPT (pin and all) as the only copy of the
                // content, but this flush is over — nothing is coming to settle
                // it. Record that, so the one consumer that needs to tell
                // "a flush will settle this" from "no one is coming" — a new
                // turn's `try_begin` claim — can clear it rather than let it
                // outlive its turn and be gilded as streaming (monorepo#2138).
                // The pin itself stays set: `LiveTurnGuard::drop` and the normal
                // turn-end clear must still leave this content alone, and a
                // later teardown re-pins it for a second attempt. Only the
                // owning flush may say this; the suspend path (`owns_slot =
                // false`) is flushing caller-held content and must not
                // characterize a pin a concurrent teardown holds.
                if owns_slot {
                    self.mark_live_turn_flush_failed(agent_id);
                }
                None
            }
        }
    }

    /// Persist the effective model resolved from a session-open response's
    /// `configOptions` (D13): when the stored model is a placeholder (NULL /
    /// blank / `default` sentinel), the effective display identity (e.g.
    /// "Opus 4.8") is persisted to the separate `resolved_model` column —
    /// `agent_session.model` is NEVER rewritten (monorepo#1534: a display
    /// name is not a selectable option id, so persisting it on `model` made
    /// the FE flag it unavailable and fall back to the default, re-triggering
    /// the rewrite and a "model changed" notice on every session open). The
    /// outcome is persisted EITHER way — a `None` resolution overwrites
    /// (clears) any previously persisted display name, so a resolution from
    /// an older option list can never go stale and mis-attribute stats. The
    /// store write is guarded on `model` still equalling the pre-open stored
    /// value (`None` matches NULL), so it loses benignly to a concurrent
    /// `agent.setModel`.
    ///
    /// Dropped guarantee (intentional): the old rewrite persisted the
    /// compound `{provider_id}:{effective}`, which as a side effect pinned
    /// the provider for legacy rows with a NULL `model` AND an empty
    /// `provider`. Such rows now fall through to the configured default /
    /// first-registered provider on every resolution — a reversion to
    /// pre-D13 behavior; current creation paths pin `model` at creation, so
    /// no new rows enter that population.
    ///
    /// A NON-placeholder (explicitly selected) model takes the D14 branch
    /// instead: its display identity is resolved against the same option
    /// list via [`resolve_explicit_display_model`] and persisted to the same
    /// `resolved_model` column. In both branches the stored `model` keeps
    /// driving provider configuration (spawn flags /
    /// `session/set_config_option`) and the resolution is used ONLY for
    /// usage-stats attribution.
    ///
    /// Best-effort: failures are logged, never propagated — model resolution
    /// must not fail session open.
    async fn persist_effective_model(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        stored_model: Option<&str>,
        config_options: Option<&[SessionConfigOption]>,
    ) {
        if !usage_stats::is_placeholder_model(stored_model) {
            self.persist_resolved_display_model(
                workspace_id,
                agent_id,
                stored_model,
                config_options,
            )
            .await;
            return;
        }
        let effective = resolve_effective_model(config_options);
        match self
            .store
            .set_agent_session_resolved_model(
                workspace_id,
                agent_id,
                stored_model,
                effective.as_deref(),
            )
            .await
        {
            Ok(true) => {
                tracing::debug!(
                    agent = %agent_id,
                    resolved = %effective.as_deref().unwrap_or("<none>"),
                    "persisted effective session model from configOptions to resolved_model"
                );
            }
            Ok(false) => {} // lost to a concurrent explicit model change
            Err(e) => {
                tracing::warn!(agent = %agent_id, error = %e, "persist effective session model failed");
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
            .set_agent_session_resolved_model(
                workspace_id,
                agent_id,
                Some(stored),
                resolved.as_deref(),
            )
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

    /// Resolve whether the stored session's specialist carries the
    /// `orchestrator` role — the SAME decision that gates the spawn-time
    /// CLI-side denylist (`derive_is_orchestrator` in `agent_manager`, §18.4),
    /// re-resolved here for the session-open paths because they run after
    /// spawn with only the stored session at hand. Only the claude-code
    /// `_meta` branch of [`build_session_meta`] consumes the decision — the
    /// other providers apply their denylists at spawn time — so any other
    /// `provider_id` short-circuits to `false` without touching the store.
    /// The frozen creation-time snapshot
    /// (`metadata.specialistIsOrchestrator`) also skips the workspace read
    /// (the path only feeds the legacy live-resolution project tier). The
    /// workspace read is best-effort: a failure resolves embedded/user-tier
    /// specialists only (never fails the session open). Plain agents (no
    /// specialist) skip the read entirely.
    async fn resolve_session_is_orchestrator(
        &self,
        provider_id: &str,
        stored: &AgentSession,
    ) -> bool {
        if provider_id != "claude-code" {
            return false;
        }
        if stored.specialist.as_deref().is_none_or(str::is_empty) {
            return false;
        }
        let has_snapshot = stored
            .metadata
            .as_ref()
            .and_then(|m| m.get("specialistIsOrchestrator"))
            .and_then(serde_json::Value::as_bool)
            .is_some();
        let workspace_path = if has_snapshot {
            None
        } else {
            match self.store.get_workspace(&stored.workspace_id).await {
                Ok(ws) => ws.effective_path().map(PathBuf::from),
                Err(e) => {
                    tracing::debug!(
                        workspace = %stored.workspace_id,
                        error = %e,
                        "orchestrator role resolution: workspace read failed; resolving without project tier"
                    );
                    None
                }
            }
        };
        self.session_specialist_is_orchestrator(stored, workspace_path.as_deref())
    }

    /// Open a new ACP session and persist its id as `AgentSession.acpSessionId`
    /// (write-once, for later resume) (§6.5). Returns the fresh id plus the
    /// modes the provider advertised in `session/new` (used by the caller to
    /// pick a permissive `session/set_mode` target from `availableModes`).
    pub(crate) async fn open_acp_session(
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
        // prefix → provider field → configured default), then build
        // provider-specific _meta. Reached only after a successful spawn (which
        // resolved the same inputs), so a fall-through here is a settings race —
        // fail loudly rather than fabricating a positional default (monorepo#3044).
        let provider_id = resolve_provider_id(
            stored.model.as_deref(),
            stored.provider.as_deref(),
            derived_default_provider(&self.effective_settings()).as_deref(),
        )
        .ok_or_else(|| no_default_provider_error("session/new"))?;
        let is_orchestrator = self
            .resolve_session_is_orchestrator(&provider_id, &stored)
            .await;
        let meta = build_session_meta(
            &provider_id,
            stored.system_prompt.as_deref(),
            Some(&stored.name),
            is_orchestrator,
        );
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
        self.persist_effective_model(
            &workspace_id,
            agent_id,
            stored.model.as_deref(),
            resp.config_options.as_deref(),
        )
        .await;
        let thought_level = discover_thought_level(resp.config_options.as_deref());
        self.persist_session_effort_levels(&workspace_id, agent_id, thought_level.as_ref())
            .await;
        Ok(AcpSessionOpened {
            session_id: acp_session_id,
            modes: resp.modes,
            thought_level,
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
    pub(crate) async fn recreate_acp_session(
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
        // the same prompt as new/load). Same loud fall-through as
        // [`open_acp_session`] (monorepo#3044).
        let provider_id = resolve_provider_id(
            stored.model.as_deref(),
            stored.provider.as_deref(),
            derived_default_provider(&self.effective_settings()).as_deref(),
        )
        .ok_or_else(|| no_default_provider_error("session/new"))?;
        let is_orchestrator = self
            .resolve_session_is_orchestrator(&provider_id, &stored)
            .await;
        let meta = build_session_meta(
            &provider_id,
            stored.system_prompt.as_deref(),
            Some(&stored.name),
            is_orchestrator,
        );
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
        // The effective-model, thought-level, and effort-levels resolutions
        // are skipped for the same reason — in particular the loser must NOT
        // persist (or clear) `effort_levels`: its `None` means "CAS lost /
        // unknown", not "the provider advertised no selector", and writing it
        // would clobber what the winner just persisted.
        let (modes, thought_level) = if canonical == new_acp_session_id {
            // The old ACP session is gone: its last context-occupancy report
            // no longer describes anything live, so drop it rather than serve
            // a stale snapshot until the new session's first `usage_update`
            // (intent-hq/intent#3797). Skipped on CAS loss — the winner owns
            // the canonical session and this cleanup with it.
            self.clear_context_usage(agent_id);
            self.persist_effective_model(
                &workspace_id,
                agent_id,
                stored.model.as_deref(),
                resp.config_options.as_deref(),
            )
            .await;
            let thought_level = discover_thought_level(resp.config_options.as_deref());
            self.persist_session_effort_levels(&workspace_id, agent_id, thought_level.as_ref())
                .await;
            (resp.modes, thought_level)
        } else {
            (None, None)
        };
        Ok(AcpSessionOpened {
            session_id: canonical,
            modes,
            thought_level,
        })
    }

    /// Resume the agent's persisted `acpSessionId` via `session/load`, but only
    /// when one was stored and the agent advertised the `loadSession` capability.
    /// Returns the resumed id plus the modes the provider advertised in
    /// `session/load`, or `None` when resume is not possible (§6.5).
    pub(crate) async fn resume_acp_session(
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
        // provider-specific _meta for system-prompt injection. Same loud
        // fall-through as [`open_acp_session`] (monorepo#3044).
        let provider_id = resolve_provider_id(
            stored.model.as_deref(),
            stored.provider.as_deref(),
            derived_default_provider(&self.effective_settings()).as_deref(),
        )
        .ok_or_else(|| no_default_provider_error("session/load"))?;
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
        // Resume path: no `sessionTitle` — the durable thread already has its
        // title and `session/load` behavior must stay unchanged (monorepo#3151).
        let is_orchestrator = self
            .resolve_session_is_orchestrator(&provider_id, &stored)
            .await;
        let meta = build_session_meta(
            &provider_id,
            stored.system_prompt.as_deref(),
            None,
            is_orchestrator,
        );
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
        self.persist_effective_model(
            &workspace_id,
            agent_id,
            stored.model.as_deref(),
            resp.config_options.as_deref(),
        )
        .await;
        let thought_level = discover_thought_level(resp.config_options.as_deref());
        self.persist_session_effort_levels(&workspace_id, agent_id, thought_level.as_ref())
            .await;
        Ok(Some(AcpSessionOpened {
            session_id: acp_session_id,
            modes: resp.modes,
            thought_level,
        }))
    }

    /// Discard the `session/update` burst that `session/load` replays after a
    /// successful resume: auggie re-streams the prior conversation as
    /// notifications written to the wire *before* `session/load` returns, so they
    /// buffer in the agent handle's unbounded channel. Left in place they would
    /// leak into the next [`run_prompt_turn`](Self::run_prompt_turn), re-emitting
    /// old messages as live `chat:stream:delta` events and re-accumulating them
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
                Ok(Some(_)) => {} // a straggler arrived → keep draining
                Ok(None) | Err(_) => break, // channel closed
                                   // quiet for the settle window → done
            }
        }
    }

    /// Drive a `session/prompt` turn: stream `session/update`s onto the bus and
    /// accumulate the transcript while the turn runs, then append the assistant
    /// message and emit the single terminal `agent:stream:end`. Returns the
    /// agent's [`StopReason`] (§6.5/§6.6). `turn_id` is the turn correlation
    /// id (monorepo#1022), stamped on the failure-arm `agent:failed` when
    /// present; bare callers (tests, harness paths) may pass `None` and the
    /// field is omitted.
    #[allow(clippy::too_many_arguments)]
    /// # Errors
    ///
    /// Returns `Error::Internal` if the `session/prompt` request fails or the transport drops mid-turn.
    pub async fn run_prompt_turn(
        &self,
        conn: &Connection,
        notifications: &mut mpsc::UnboundedReceiver<IncomingNotification>,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        acp_session_id: &str,
        prompt: Vec<ContentBlock>,
        turn_id: Option<&str>,
    ) -> Result<StopReason> {
        // Mint the assistant message id at turn START (CS-0 D1) so streaming
        // block ids `{messageId}:{index}` match the blocks ultimately persisted.
        let message_id = Uuid::now_v7().to_string();
        trace_stream_correlation_mapping(&message_id, turn_id);
        let mut transcript = Transcript::new(message_id.clone());
        // Turn wall-clock start, for the global usage-stats longest-run MAX.
        let turn_started = std::time::Instant::now();
        // Publish the in-flight turn so a `chat.subscribe` arriving mid-turn can
        // reconstruct the partial message (CS-0 D5). The guard clears the slot on
        // ANY exit — including the interrupt/abort path that drops this worker
        // before `stream:end` — so subscribers never see a stale in-flight turn.
        let _live_guard = self.begin_live_turn(agent_id, &message_id);
        // Pre-first-token turn-startup hint: the FE renders "Sent prompt…" next
        // to the spinner until the first `agent:stream:activity` clears it. Emitted
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
        let mut closed = false;
        // Whether ANY `session/update` for this turn was applied — an input to
        // the silent-redrive eligibility below (monorepo#764): once the
        // provider streamed anything, a transport failure is no longer
        // provably output-free.
        let mut updates_applied = false;
        // Whether ANY `session/update` arrived at all — a superset of
        // `updates_applied` (unmapped variants like plan/thought/mode/usage
        // return false from `route_notification` but still reset the idle
        // timer). Input to the idle-timeout streamed-activity marker below.
        let mut any_update_received = false;
        // Bounded in-place retry for transient provider-fetch failures
        // (intent-hq/monorepo#3007): a `-32603` wrapping a connect-level
        // fetch fault (EPIPE/ECONNRESET/ECONNREFUSED/timeout) or an explicit
        // provider `apiStatus: unavailable`/`overloaded` payload is routine
        // network weather, not a terminal turn failure. The prompt is
        // re-dispatched on the SAME live connection with exponential backoff,
        // but ONLY while the attempt is provably output-free (no
        // `session/update` received at all), side-effect-free (no
        // agent→client request — `fs/write_text_file`, `terminal/*`,
        // `session/request_permission` — forwarded since the turn started,
        // per the connection's client-request watermark) and the
        // notification channel is still open — nothing was streamed,
        // persisted, or emitted, so the retried unit is idempotent. Once
        // anything streamed or side-effected, or once the budget is spent,
        // the error falls through to the existing classification below
        // (terminal / silent redrive / sleep-resume) unchanged. Genuinely
        // terminal errors (auth, invalid request, model-not-found, 4xx)
        // never classify transient and fail fast.
        //
        // Two boundary notes:
        // - Bridge-side idempotency is assumed, not proven: the guard proves
        //   the DAEMON attempt was output-free, but whether the provider
        //   bridge (codex-acp / auggie) already appended the user prompt to
        //   its session history before the failed fetch is bridge-internal.
        //   Accepted risk: worst case the retried prompt duplicates the user
        //   message in provider-side context (never in daemon-persisted
        //   transcript), which is strictly better than killing the turn.
        // - No cancel race with the backoff sleep: a user stop is delivered
        //   by `AgentManager::interrupt`/`stop` aborting this turn worker
        //   (`worker.abort()`), which drops this whole future — sleep, loop,
        //   and all — so a retry can never re-dispatch after a stop.
        let mut fetch_retry_attempt: u32 = 0;
        // Agent→client request watermark at turn start: any bump means a
        // client-served handler may have side-effected on behalf of this
        // turn, so the attempt is no longer provably idempotent.
        let client_request_watermark = conn.client_request_seq();
        // Mid-turn stall detection (intent-hq/monorepo#3402): a timer arm in
        // the select loop below samples `activity.idle_ms()` on a fraction of
        // the stall threshold (clamped to 15s at the 5-minute default) and
        // emits ONE advisory `stalled` status event once the silence crosses
        // [`stream_stall_ms`].
        // The next received `session/update` emits `resumed` and re-arms the
        // detector, so a later second stall in the same turn reports again.
        // Advisory only: turn resolution is untouched (the 30-minute prompt
        // idle timeout stays the terminal backstop).
        //
        // Tool-call-aware (intent-hq/monorepo#3466): while ≥1 recorded tool
        // call is still open (`transcript.open_tool_call_count() > 0`), the
        // arm emits nothing regardless of silence duration — long tool runs
        // (builds, test suites) are legitimately silent between `tool_call`
        // and the terminal `tool_call_update`. Once the last open call
        // resolves, the standard threshold applies to subsequent silence
        // (`activity` was touched by the resolving update, so the window
        // restarts from that point). Hung tools stay covered by the
        // 30-minute prompt idle timeout.
        let stall_threshold_ms = stream_stall_ms();
        let stall_check = Duration::from_millis((stall_threshold_ms / 6).clamp(10, 15_000));
        let mut stall_emitted = false;
        let result = loop {
            let prompt_fut = session::prompt(conn, acp_session_id, prompt.clone(), &activity);
            tokio::pin!(prompt_fut);
            let attempt_result = loop {
                tokio::select! {
                    res = &mut prompt_fut => break res,
                    maybe = notifications.recv(), if !closed => match maybe {
                        Some(note) => {
                            activity.touch();
                            self.clear_stream_stall(&mut stall_emitted, workspace_id, agent_id)
                                .await;
                            any_update_received = true;
                            updates_applied |= self
                                .route_notification(&note, agent_id, workspace_id, &mut transcript)
                                .await;
                        }
                        None => closed = true,
                    },
                    () = tokio::time::sleep(stall_check), if !stall_emitted => {
                        let silent_ms = activity.idle_ms();
                        if silent_ms >= stall_threshold_ms && transcript.open_tool_call_count() == 0 {
                            stall_emitted = true;
                            tracing::warn!(
                                agent = %agent_id,
                                silent_ms,
                                "mid-turn stream stall — no session/update past threshold (monorepo#3402)"
                            );
                            self.publish_stalled_status_event(workspace_id, agent_id, silent_ms)
                                .await;
                        }
                    }
                }
            };
            // Drain updates buffered before this attempt settled BEFORE the
            // retry decision: `prompt_fut` can win the `select!` with streamed
            // notes still sitting in the channel, and a retry armed on a
            // stale `any_update_received = false` would re-dispatch a prompt
            // that did produce output (duplicating it). The post-loop drain
            // below still runs for the final attempt; draining twice is
            // harmless (`try_recv` on an empty channel is a no-op).
            if attempt_result.is_err() {
                while let Ok(note) = notifications.try_recv() {
                    activity.touch();
                    self.clear_stream_stall(&mut stall_emitted, workspace_id, agent_id)
                        .await;
                    any_update_received = true;
                    updates_applied |= self
                        .route_notification(&note, agent_id, workspace_id, &mut transcript)
                        .await;
                }
            }
            match &attempt_result {
                Err(e)
                    if fetch_retry_attempt < MAX_TRANSIENT_PROMPT_FETCH_RETRIES
                        && !closed
                        && !any_update_received
                        && conn.client_request_seq() == client_request_watermark
                        && intent_acp::is_transient_provider_fetch_failure(e) =>
                {
                    fetch_retry_attempt += 1;
                    let delay = Duration::from_millis(
                        transient_prompt_retry_base_ms() << (fetch_retry_attempt - 1),
                    );
                    tracing::warn!(
                        agent = %agent_id,
                        error = %e,
                        attempt = fetch_retry_attempt,
                        delay_ms = u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
                        "transient provider fetch failure — retrying session/prompt (monorepo#3007)"
                    );
                    tokio::time::sleep(delay).await;
                    // Re-drain after the backoff: a straggling update (or a
                    // channel close) can land DURING the sleep, and
                    // re-dispatching without observing it would duplicate
                    // output the provider already streamed. If the recheck
                    // flips any guard, fall through to classification with
                    // this attempt's error instead of retrying.
                    while let Ok(note) = notifications.try_recv() {
                        activity.touch();
                        self.clear_stream_stall(&mut stall_emitted, workspace_id, agent_id)
                            .await;
                        any_update_received = true;
                        updates_applied |= self
                            .route_notification(&note, agent_id, workspace_id, &mut transcript)
                            .await;
                    }
                    if any_update_received || conn.client_request_seq() != client_request_watermark
                    {
                        tracing::warn!(
                            agent = %agent_id,
                            attempt = fetch_retry_attempt,
                            "output arrived during retry backoff — abandoning retry (monorepo#3007)"
                        );
                        break attempt_result;
                    }
                }
                _ => break attempt_result,
            }
        };
        // Drain updates buffered before the prompt response resolved. Each
        // drained note is streamed activity that arrived BEFORE the prompt
        // settled (`prompt_fut` can win the `select!` while updates sit
        // buffered — e.g. an agent sending its final update and response
        // back-to-back), so `activity.touch()` here counts it against the
        // silent tail captured below; without it such a turn would read as
        // carrying a long tail it never had.
        while let Ok(note) = notifications.try_recv() {
            activity.touch();
            self.clear_stream_stall(&mut stall_emitted, workspace_id, agent_id)
                .await;
            any_update_received = true;
            updates_applied |= self
                .route_notification(&note, agent_id, workspace_id, &mut transcript)
                .await;
        }
        // Silent tail of the turn (intent-hq/monorepo#2669): ms of
        // `session/update` silence between the turn's last streamed activity
        // (buffered drain included) and the prompt resolving. In that
        // incident bloated sessions resolved a clean `end_turn` after
        // 11-13 min of total silence — the daemon held this exact signal
        // (`activity.idle_ms()`) and discarded it; now it is recorded for
        // `agent.diagnostics` and, past [`silent_tail_suspect_ms`], stamped
        // on the terminal `agent:idle` payload below.
        let silent_tail_ms = activity.idle_ms();
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
        // Split the PromptOutcome into its stop reason, the optional
        // end-of-turn usage snapshot (persisted below once the turn's message
        // is durable), and the raw `_meta` payload — the fallback usage
        // source for providers that bill only there (grok's `_meta.usage`
        // whole-prompt bill, intent-hq/intent#3803).
        let mut turn_usage = None;
        let mut turn_meta = None;
        let result = result.map(|outcome| {
            turn_usage = outcome.usage;
            turn_meta = outcome.meta;
            outcome.stop_reason
        });
        // Latest ACP `usage_update` cost of the turn (§5.23), accumulated by
        // `route_notification`; persisted alongside the token snapshot below.
        let turn_cost = transcript.usage_cost.clone();
        // Accumulate the assistant message (one per turn) into the append-only log.
        let blocks = transcript.into_blocks();
        let block_count = blocks.len();
        let last_response_summary = last_response_summary(&blocks);
        // Final-value preview for the terminal `agent:stream:end` below: the
        // last throttled `agent:stream:activity` may have missed the response
        // tail, so the terminal frame re-derives from the full turn text.
        let preview_text_blocks = text_block_strings(&blocks);
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
        // Idle-timeout classification (warn-and-continue): the turn went the
        // whole idle window with no `session/update` traffic. The partial
        // transcript (if any) is flushed and the normal `agent:stream:end`
        // below still fires, but `agent:failed` is suppressed — the turn
        // worker decides between a warning redrive and (once the consecutive
        // cap is spent) the terminal path, which emits `agent:failed` itself.
        let prompt_idle_timeout = matches!(&result, Err(AcpError::PromptIdleTimeout(_)));
        // Whether the timed-out turn saw ANY `session/update` before going
        // silent — intervening activity that resets the worker's consecutive-
        // timeout counter (marked via the wrapped error's suffix below). Keyed
        // off `any_update_received`, not `updates_applied`: unmapped variants
        // (plan/thought/mode/usage) also reset the idle timer, so they must
        // count as activity for the streak accounting too.
        let idle_timeout_streamed =
            prompt_idle_timeout && (any_update_received || !blocks.is_empty());
        // Whether this turn's persisted content carries question resource
        // blocks (`ws.app.question.ask`) — computed BEFORE the append consumes
        // `blocks`, used for the pending-questions marker write below.
        let questions_persisted = crate::agent_ops::question_block_count_in(&blocks) > 0;
        // Sleep-induced turn failure (Task C): the turn died with a transient
        // upstream disconnect AND a detected host suspend overlapped its active
        // window `[turn_started, now]`. Enroll it as interrupted (so the wake
        // orchestrator in Task D can resume it via `session/load`) instead of
        // surfacing a hard terminal failure. Gated on an injected
        // [`SuspendOverlapQuery`]: absent (read-only / unit wiring, or
        // `wakeResume` disabled), this is always false and failures keep
        // today's behavior. Placed BEFORE the plain persist + terminal emits
        // below so it takes precedence over the pre-output silent-redrive path
        // (monorepo#764) for the suspend-overlapping case — a `session/load`
        // resume preserves the partial turn, which a fresh-child redrive would
        // not. `PromptIdleTimeout` is classified non-transient, so an idle
        // timeout never routes here.
        let suspend_interrupt = matches!(&result, Err(e) if intent_acp::is_transient_upstream_disconnect(e))
            && self
                .suspend_tracker
                .as_ref()
                .and_then(|t| t.did_suspend_overlap(turn_started, Instant::now()))
                .is_some();
        if suspend_interrupt {
            // `matches!` above guarantees the `Err` arm.
            let err = result.expect_err("suspend_interrupt implies Err");
            return self
                .enroll_suspend_interrupted_turn(
                    agent_id,
                    workspace_id,
                    message_id,
                    blocks,
                    turn_id,
                    err,
                )
                .await;
        }
        // Record the silent tail for `agent.diagnostics`
        // (intent-hq/monorepo#2669). A pre-output transport failure is
        // excluded: the attempt is invisible (the worker may silently
        // redrive it), so its tail must not overwrite the last VISIBLE
        // turn's record. Also skipped when the agent's session row is
        // already gone: `agent.delete` does not stop a running turn, so a
        // turn settling after the delete's `clear_turn_silent_tail` sweep
        // would otherwise resurrect the entry for the daemon lifetime.
        // Best-effort — a delete interleaving between this check and the
        // insert leaves one stale u64, which the diagnostics read (it
        // iterates live sessions) never serves.
        if !pre_output_transport_failure
            && self.store.get_agent_session_summary(agent_id).await.is_ok()
        {
            self.record_turn_silent_tail(agent_id, silent_tail_ms);
        }
        // Suspicious bare normal ending (intent-hq/monorepo#2669): the turn
        // completed normally after a sustained silent tail — the incident
        // signature of a silently-truncated turn under session bloat. Both
        // normal stop reasons (`end_turn` / `stream_complete`, the same set
        // the abnormal-finish filter below treats as unremarkable) qualify:
        // the incident harness happened to resolve `end_turn`, but a harness
        // ending a truncated turn with `stream_complete` reads identically
        // everywhere else and must not dodge the annotation. Advisory only
        // (healthy long tool-free inference tails exist): stamped on the
        // terminal `agent:idle` payload below as `silentTailMs` +
        // `suspectedTruncated: true` so coordinators and the monorepo#1016
        // stall-annotation consumers get a machine-readable signal; the turn
        // itself is never failed.
        let suspected_truncated = silent_tail_ms >= silent_tail_suspect_ms()
            && result
                .as_ref()
                .ok()
                .and_then(|stop| serde_json::to_value(stop).ok())
                .is_some_and(|v| matches!(v.as_str(), Some("end_turn" | "stream_complete")));
        if suspected_truncated {
            tracing::warn!(
                agent = %agent_id,
                silent_tail_ms,
                "turn resolved normally after a sustained silent tail — suspected truncation (monorepo#2669)"
            );
        }
        // Auto-redrive decision (intent-hq/monorepo#2863): a suspected-
        // truncated turn on a DELEGATED agent whose assigned task is still
        // in_progress (and that has no persisted completion report) is sent
        // back to the agent with a system nudge instead of surfacing a
        // normal idle/completion to watchers — in the incident a single
        // manual nudge restored productivity instantly every time.
        // Bounded per stall episode: streamed activity before the silence
        // restarts the consecutive accounting at 1 (real progress, same
        // semantics as the idle-timeout streak); a fully silent truncated
        // turn increments it; past MAX_CONSECUTIVE_TRUNCATION_REDRIVES the
        // turn falls through to today's idle + advisory fields, which the
        // #1016 stall-annotation path surfaces to the parent. Excluded and
        // left to today's behavior: root/user-facing agents and taskless
        // agents (WARN + advisory only, per the issue's guard scope),
        // question-bearing turns (the redrive would bury the pending Q&A
        // behind the question hold's back), and turns with a ready-to-send
        // queue entry (the imminent drain is itself the nudge). The counter
        // clears on any clean (non-truncated) completion — the stall
        // episode is over.
        let mut truncation_terminal_outcome = "suspected_truncated_ineligible";
        let truncation_redrive = if suspected_truncated
            && !questions_persisted
            && !self.has_ready_to_send(agent_id)
            && self.truncation_redrive_eligible(agent_id).await
        {
            if any_update_received {
                self.clear_truncation_redrives(agent_id);
            }
            let streak = self.bump_truncation_redrives(agent_id);
            if streak <= MAX_CONSECUTIVE_TRUNCATION_REDRIVES {
                tracing::warn!(
                    agent = %agent_id,
                    streak,
                    silent_tail_ms,
                    "suspected truncation on a delegated in-task agent — arming auto-redrive (monorepo#2863)"
                );
                self.arm_truncation_redrive(agent_id);
                true
            } else {
                truncation_terminal_outcome = "truncation_cap_exhausted";
                tracing::warn!(
                    agent = %agent_id,
                    streak,
                    "suspected truncation — consecutive-redrive cap spent, falling through to idle + advisory (monorepo#2863)"
                );
                false
            }
        } else {
            if !suspected_truncated && result.is_ok() {
                self.clear_truncation_redrives(agent_id);
            }
            false
        };
        // Abnormal finish reason (PROTOCOL §7): a turn that resolved with a
        // non-`end_turn` stop reason (`refusal`, `max_tokens`,
        // `max_turn_requests`, …) is durably tagged on the assistant row so
        // clients can render the ending after a reload. Normal endings
        // (`end_turn` / `stream_complete`) stay metadata-free — no noise on
        // the common path. A zero-output abnormal turn still persists an
        // empty marker row (mirroring the §7.2 pre-first-token interrupt
        // marker): `agent:idle` / `agent:stream:end` are ephemeral, so the
        // row is the only durable record of the ending.
        let abnormal_finish_reason = result
            .as_ref()
            .ok()
            .map(|stop| serde_json::to_value(stop).unwrap_or(Value::Null))
            .filter(|v| !matches!(v.as_str(), Some("end_turn" | "stream_complete") | None));
        if !blocks.is_empty() || abnormal_finish_reason.is_some() {
            let row_metadata = abnormal_finish_reason
                .as_ref()
                .map(|reason| json!({ "finishReason": reason }));
            let message = self
                .store
                .append_agent_message_with_id(
                    agent_id,
                    &message_id,
                    "assistant",
                    &Value::Array(blocks),
                    row_metadata.as_ref(),
                    &now_iso(),
                )
                .await?;
            trace_stream_lifecycle(
                Some(message_id.as_str()),
                "message",
                "assistant_persisted",
                Some(turn_started.elapsed()),
                block_count,
                if result.is_ok() { "complete" } else { "failed" },
            );
            self.invalidate_agent_list_cache(workspace_id);
            // Persisted-row event pair (PROTOCOL §6.5): the assistant turn
            // flush emits `agent:message` + `agent:last-message` like every
            // other transcript persist, so preview subscribers learn the
            // final persisted `lastToolUse` with zero follow-up RPCs (the
            // streaming activity preview carries no tool input).
            self.publish_agent_message_events(workspace_id, agent_id, &message, turn_id)
                .await;
            message_persisted = true;
        }
        // Stored-on-write pending-questions marker (PROTOCOL §5.5, question
        // hold): a question-bearing assistant tail arms the hold under this
        // turn's message id (a newer question set overwrites an older marker
        // — single-slot). A question-FREE turn end deliberately does NOT
        // clear the marker: pendingness survives the agent's later turns
        // until the user answers or dismisses.
        let marker_moved = if message_persisted && questions_persisted {
            self.record_pending_questions_marker(workspace_id, agent_id, &message_id)
                .await
        } else {
            false
        };
        // A question-bearing tail arms the marker above and so RAISES the
        // workspace's needs_attention displayStatus (§6.5 step 0):
        // recompute-and-compare. A question-FREE tail no longer moves the
        // derivation at all — pendingness now survives the agent's later
        // turns — so those turn ends skip the workspace-wide probe entirely
        // instead of relying on the dedup cache to stay silent.
        if marker_moved {
            self.maybe_emit_display_status_changed(workspace_id).await;
        }
        // The turn's message is now durable: clear the live-turn slot so the next
        // `chat.subscribe` snapshot reflects the persisted message (not a stale
        // in-flight copy) BEFORE the terminal `stream:end` is observed. The guard
        // remains as the abort-path fallback. A PINNED slot is left for the
        // teardown flush that owns it (monorepo#2110) — see
        // [`clear_unpinned_live_turn`](Self::clear_unpinned_live_turn).
        self.clear_unpinned_live_turn(agent_id);
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
        if turn_usage.is_some() || turn_cost.is_some() || run_completed {
            let services = self.clone();
            let agent_id_task = agent_id.clone();
            let workspace_id_task = workspace_id.clone();
            let turn_duration = turn_started.elapsed();
            // Capture the turn-end wall clock next to the duration so the two
            // stay consistent: the bookkeeping task below may queue behind a
            // predecessor before it records, and a later clock read would push
            // both the hourly bucket and the per-minute spread window past the
            // real turn end.
            let turn_end = time::OffsetDateTime::now_utc();
            let turn_usage = turn_usage.take();
            let turn_meta = turn_meta.take();
            let prev = self
                .turn_bookkeeping
                .lock()
                .ok()
                .and_then(|mut chain| chain.remove(agent_id));
            let handle = tokio::spawn(async move {
                if let Some(prev) = prev {
                    let _ = prev.await;
                }
                // grok fallback (intent-hq/intent#3803): no standard report,
                // but the turn's `_meta` may carry the whole-prompt bill —
                // synthesize the standard per-turn report from it so the
                // ordinary seam below ingests it (SUM, grok is PerTurn). A
                // present standard report always wins; runs after the
                // predecessor await so its provider read cannot race turn
                // N-1's bookkeeping.
                let (turn_usage, turn_cost) = if turn_usage.is_none() {
                    match services
                        .prompt_meta_turn_usage(
                            &agent_id_task,
                            &workspace_id_task,
                            turn_meta.as_ref(),
                        )
                        .await
                    {
                        Some((usage, cost)) => (Some(usage), turn_cost.or(cost)),
                        None => (turn_usage, turn_cost),
                    }
                } else {
                    (turn_usage, turn_cost)
                };
                services
                    .record_turn_usage_stats(
                        &agent_id_task,
                        &workspace_id_task,
                        turn_usage.as_ref(),
                        turn_duration,
                        turn_end,
                        run_completed,
                    )
                    .await;
                // A turn with neither report has nothing to persist; either
                // one alone still updates its half of the snapshot.
                if turn_usage.is_some() || turn_cost.is_some() {
                    services
                        .persist_turn_token_usage(
                            &agent_id_task,
                            &workspace_id_task,
                            turn_usage.as_ref(),
                            turn_cost,
                        )
                        .await;
                }
            });
            if let Ok(mut chain) = self.turn_bookkeeping.lock() {
                chain.insert(agent_id.clone(), handle);
            }
        }
        // Durable-before-observable for the streaming terminal-failure path
        // (monorepo#2050): an ordinary mid-turn `session/prompt failed:` error
        // is terminal, and this function emits its own terminal
        // `agent:stream:end` + `agent:failed` below. Persist `status = error` +
        // `stop_reason` FIRST — ahead of those emits — and stash the persisted
        // context so the turn worker's `handle_terminal_turn_failure` reuses it
        // (recording the identical-failure streak and writing the Error status
        // EXACTLY once, monorepo#840). Excluded and left to their existing
        // handling: the pre-output transport failure (suppressed for a possible
        // silent redrive) and the idle timeout (warn-and-continue; the worker
        // owns the warn/terminal decision and its own persist). A benign
        // provider-resolved cancel (JSON-RPC `-32800`, or a "cancelled"
        // message) is NOT persisted as an Error — it is the expected outcome of
        // a concurrent stop/cancel — classified here with the SAME predicate
        // the worker's benign check uses so the two verdicts cannot drift. The
        // wrapped text matches the ordinary error the final `map_err` returns
        // below, so the persisted `stop_reason` is byte-identical to what the
        // worker would have written.
        if let Err(e) = &result {
            if !pre_output_transport_failure && !prompt_idle_timeout {
                let wrapped = Error::Internal(format!("session/prompt failed: {e}"));
                if !crate::agent_manager::prompt_cancellation_error(&wrapped) {
                    let persist = crate::agent_manager::persist_terminal_error_status_via_services(
                        self,
                        agent_id,
                        workspace_id,
                        &wrapped.to_string(),
                    )
                    .await;
                    self.stash_pending_terminal_error(agent_id, persist);
                    trace_stream_lifecycle(
                        Some(message_id.as_str()),
                        "message",
                        "terminal_failure",
                        Some(turn_started.elapsed()),
                        block_count,
                        "failed",
                    );
                }
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
            let outcome = if result.is_err() {
                "failed"
            } else if suspected_truncated && truncation_redrive {
                "suspected_truncated_redrive"
            } else if suspected_truncated {
                truncation_terminal_outcome
            } else {
                "complete"
            };
            trace_stream_lifecycle(
                Some(message_id.as_str()),
                "message",
                "agent_stream_end",
                Some(turn_started.elapsed()),
                block_count,
                outcome,
            );
            let mut end_data = json!({ "agentId": agent_id.0 });
            if message_persisted {
                end_data["messageId"] = json!(message_id);
            }
            if !trailing_blocks.is_empty() {
                end_data["trailingBlocks"] = Value::Array(trailing_blocks);
            }
            // Turn correlation (monorepo#1022): the terminal stream:end names
            // the logical turn it closes, same contract as `agent:failed`.
            if let Some(tid) = turn_id {
                end_data["turnId"] = json!(tid);
            }
            // Abnormal finish reason: same value tagged on the persisted
            // assistant row above — omitted on normal endings, never `null`.
            if let Some(reason) = &abnormal_finish_reason {
                end_data["finishReason"] = reason.clone();
            }
            // Final live-preview values (same fields as the throttled
            // activity frames) so a client tracking the preview push-style
            // lands on the turn's true final state.
            stamp_preview_fields(&mut end_data, &preview_text_blocks);
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
        //
        // An armed truncation auto-redrive (intent-hq/monorepo#2863) also
        // suppresses `agent:idle`: the agent is NOT idle — the turn worker
        // is about to inject the nudge turn on the same session — so a
        // completion wake here would tell the parent the work finished while
        // the redrive is still trying to resume it. On cap exhaustion the
        // flag is false and this arm fires as today (advisory fields
        // included), so watchers get exactly one idle per stall episode.
        match &result {
            Ok(_) if truncation_redrive => {
                tracing::debug!(
                    agent = %agent_id,
                    "agent:idle suppressed — truncation auto-redrive armed (monorepo#2863)",
                );
            }
            Ok(stop_reason) if !self.has_ready_to_send(agent_id) => {
                trace_stream_lifecycle(
                    Some(message_id.as_str()),
                    "message",
                    "agent_idle",
                    Some(turn_started.elapsed()),
                    block_count,
                    if suspected_truncated {
                        truncation_terminal_outcome
                    } else {
                        "complete"
                    },
                );
                let mut data = json!({
                    "agentId": agent_id.0,
                    "reason": "stream_complete",
                    "finishReason": stop_reason,
                    "status": "idle",
                });
                if let Some(summary) = last_response_summary {
                    data["lastResponseSummary"] = Value::String(summary);
                }
                // Suspicious bare `end_turn` after a sustained silent tail
                // (intent-hq/monorepo#2669): additive advisory fields —
                // `silentTailMs` (the measured gap) + `suspectedTruncated:
                // true` — omitted entirely on healthy turns (absent, never
                // `false`), so subscribers get a machine-readable truncation
                // signal without any change to the normal payload.
                if suspected_truncated {
                    data["silentTailMs"] = json!(silent_tail_ms);
                    data["suspectedTruncated"] = json!(true);
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
                    Value::Bool(!self.waiting_watches_for_parent(agent_id).is_empty());
                // Idle-visibility: an idle agent still owning active
                // (scheduled/running) background hooks is waiting, not
                // stalled — stamp `waitingOnHooks` (omitted when none) so
                // subscribers and the completion-watch wake can surface it.
                self.annotate_waiting_on_hooks(agent_id, &mut data).await;
                // Idle-visibility (unified external-wait, mirrors the hook
                // stamp above): same `waitingOnPrMonitors` stamp for active
                // PR monitors (omitted when none).
                self.annotate_waiting_on_pr_monitors(agent_id, &mut data)
                    .await;
                // Archived-workspace suppression hint: stamp
                // `workspaceArchived: true` (omitted when not archived) so
                // notification clients stay quiet for parked workspaces.
                self.annotate_workspace_archived(workspace_id, &mut data)
                    .await;
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
            Err(e) if prompt_idle_timeout => {
                // Suppressed (warn-and-continue): the idle timeout is not a
                // user-visible failure — the partial transcript was flushed
                // and the normal stream:end above closed the turn. The
                // worker decides between a warning redrive and the terminal
                // path (which emits agent:failed itself once the
                // consecutive-timeout cap is spent).
                tracing::debug!(
                    agent = %agent_id,
                    error = %e,
                    "prompt idle timeout — deferring the warn/terminal decision to the turn worker",
                );
            }
            Err(e) => {
                trace_stream_lifecycle(
                    Some(message_id.as_str()),
                    "message",
                    "agent_failed",
                    Some(turn_started.elapsed()),
                    block_count,
                    "failed",
                );
                let mut data = json!({ "agentId": agent_id.0, "error": e.to_string() });
                if let Some(tid) = turn_id {
                    data["turnId"] = json!(tid);
                }
                self.publish_agent_event(workspace_id, agent_id, AGENT_FAILED, data)
                    .await;
            }
        }
        result.map_err(|e| {
            if pre_output_transport_failure {
                Error::Internal(format!("{PROMPT_PRE_OUTPUT_TRANSPORT_PREFIX} {e}"))
            } else if idle_timeout_streamed {
                // Streamed-activity marker for the worker's consecutive-
                // timeout accounting (the suffix rides INSIDE the ordinary
                // wrapper so `prompt_idle_timeout_error` still classifies).
                Error::Internal(format!(
                    "session/prompt failed: {e} {PROMPT_IDLE_TIMEOUT_STREAMED_SUFFIX}"
                ))
            } else {
                Error::Internal(format!("session/prompt failed: {e}"))
            }
        })
    }

    /// Enroll a sleep-induced turn failure (Task C): a transient upstream
    /// disconnect whose active window overlapped a detected host suspend. The
    /// partial turn is persisted tagged [`InterruptReason::SystemSuspend`]
    /// (empty blocks still record a row — every interruption is durably
    /// anchored), an `interrupted_agent` row is written with `prev_status` = the
    /// session's running status (mirroring the daemon-restart heal path) so the
    /// wake orchestrator (Task D) can resume it via `session/load`, and the
    /// terminal event is the interrupted `agent:stream:end` (`stopReason:
    /// "interrupted"`, `interruptReason: "system_suspend"`) — NOT
    /// `agent:failed` — so no hard error or manual-retry surface reaches the FE.
    ///
    /// Returns the original error wrapped with [`PROMPT_SUSPEND_INTERRUPT_PREFIX`]
    /// so the turn worker suppresses the terminal-failure path.
    async fn enroll_suspend_interrupted_turn(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        message_id: String,
        blocks: Vec<Value>,
        turn_id: Option<&str>,
        err: AcpError,
    ) -> Result<StopReason> {
        let correlation_id = message_id.clone();
        let block_count = blocks.len();
        // Final live-preview values from the partial turn (same contract as the
        // interrupt terminal emit in `agent_manager`).
        let preview_text_blocks = text_block_strings(&blocks);
        // Persist the partial turn tagged as suspend-interrupted. Reuses the
        // shared interrupt-flush path so the persisted row carries the
        // `interruptReason` metadata (and clears the live-turn slot — but only
        // an UNPINNED one: this path flushes caller-held content, it does not
        // own the agent's slot, and a pin there belongs to a concurrent
        // teardown whose flush absorbs the resulting UNIQUE collision).
        let live = LiveTurn {
            message_id,
            blocks,
            final_text_block_open: false,
            last_activity_at: now_iso(),
            last_activity_emit: None,
            flush_pending: false,
            flush_failed: false,
        };
        let interrupted_message_id = self
            .flush_partial_turn_on_interruption(
                agent_id,
                live,
                InterruptReason::SystemSuspend,
                None,
                false,
            )
            .await;
        // Capture the session's running status for restore-on-resume, serialized
        // to the stored form (e.g. "active", "Waiting") exactly like
        // `heal_stale_agent_sessions`. A lookup failure falls back to "active".
        let prev_status = match self.store.get_agent_session_summary(agent_id).await {
            Ok(session) => serde_json::to_string(&session.status)
                .unwrap_or_else(|_| "\"active\"".to_string())
                .trim_matches('"')
                .to_string(),
            Err(e) => {
                tracing::debug!(
                    agent = %agent_id,
                    error = %e,
                    "suspend enrollment: session status lookup failed, defaulting prev_status=active"
                );
                "active".to_string()
            }
        };
        match self
            .store
            .insert_interrupted_agent_with_reason(
                agent_id,
                workspace_id,
                &prev_status,
                &now_iso(),
                Some(InterruptReason::SystemSuspend.as_str()),
            )
            .await
        {
            Ok(_) => {
                // Self-heal (independent of the host-wake broadcast): the wake
                // orchestrator runs one debounced sweep per detected wake, so a
                // disconnect that surfaces AFTER that sweep would strand this
                // row until the NEXT host wake — even though the normal
                // failure/retry surface was suppressed. Fire a gated, debounced
                // resume directly for this agent so it recovers on its own; the
                // wake sweep stays the catch-all. The debounce lets the worker's
                // post-enrollment `kill_child_only` + `end_turn` settle first,
                // and the row's atomic claim dedupes against a racing wake sweep
                // / `resolveInterrupted`.
                let services = self.clone();
                let debounce = wake_resume_self_heal_debounce();
                tokio::spawn(async move {
                    tokio::time::sleep(debounce).await;
                    services.resume_suspend_interrupted_agents().await;
                });
            }
            Err(e) => {
                // Fail-soft: the interrupted terminal state is still emitted
                // below so the FE never sees a hard error. A missing row only
                // forgoes the wake-triggered/self-heal resume (a manual retry
                // still works).
                tracing::warn!(
                    agent = %agent_id,
                    workspace_id = %workspace_id.0,
                    error = %e,
                    "failed to enroll suspend-interrupted agent row"
                );
            }
        }
        // Terminal event is the interrupted `agent:stream:end` (mirrors the
        // interrupt path in `agent_manager`), NOT `agent:failed` — the FE
        // renders a Stopped/resuming indicator rather than a hard error with a
        // Retry button.
        let mut end_data = json!({
            "agentId": agent_id.0,
            "stopReason": "interrupted",
            "interruptReason": InterruptReason::SystemSuspend.as_str(),
        });
        if let Some(ref mid) = interrupted_message_id {
            end_data["messageId"] = json!(mid);
        }
        if let Some(tid) = turn_id {
            end_data["turnId"] = json!(tid);
        }
        stamp_preview_fields(&mut end_data, &preview_text_blocks);
        trace_stream_lifecycle(
            Some(correlation_id.as_str()),
            "message",
            "agent_stream_end",
            None,
            block_count,
            "interrupted",
        );
        self.publish_agent_event(workspace_id, agent_id, AGENT_STREAM_END, end_data)
            .await;
        tracing::info!(
            agent = %agent_id,
            error = %err,
            "turn interrupted by system suspend — enrolled for wake-resume"
        );
        // The wrapped marker tells the turn worker to suppress the
        // terminal-failure path (no `agent:failed`, no Error status / retry).
        Err(Error::Internal(format!(
            "{PROMPT_SUSPEND_INTERRUPT_PREFIX} {err}"
        )))
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
    /// Returns a [`HarnessWakeOutcome`]: the persisted assistant `messageId`
    /// (`None` when the burst persisted nothing), the empty-response
    /// classification (intent-hq/monorepo#3262), and the content-free
    /// correlation needed by the caller's later idle diagnostic.
    pub(crate) async fn run_harness_wake_turn(
        &self,
        notifications: &mut mpsc::UnboundedReceiver<IncomingNotification>,
        first: IncomingNotification,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        settle: std::time::Duration,
    ) -> HarnessWakeOutcome {
        let turn_started = Instant::now();
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
            let tick = settle
                .saturating_sub(elapsed)
                .min(std::time::Duration::from_millis(50));
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
        // A wake turn has no `session/prompt` and therefore no end-of-turn
        // token report, but the burst may still carry an ACP `usage_update`
        // cost (§5.23) — persist it (cost-only) so it is not lost.
        if let Some(cost) = transcript.usage_cost.clone() {
            self.persist_cost_only_ordered(agent_id, workspace_id, cost)
                .await;
        }
        let blocks = transcript.into_blocks();
        let block_count = blocks.len();
        let preview_text_blocks = text_block_strings(&blocks);
        let questions_persisted = crate::agent_ops::question_block_count_in(&blocks) > 0;
        // Empty-response classification (intent-hq/monorepo#3262): the wake
        // turn OPENED (a chunk/tool-call materialized content) but finalized
        // with nothing meaningful — whitespace-only text/thinking blocks, the
        // incident's single bare "\n". Computed on the finalized blocks
        // BEFORE the append consumes them; the row (when any) still persists
        // as the durable record of the no-op wake.
        let empty_response = harness_wake_response_is_empty(&blocks);
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
                Ok(message) => {
                    trace_stream_lifecycle(
                        Some(message_id.as_str()),
                        "message",
                        "assistant_persisted",
                        Some(turn_started.elapsed()),
                        block_count,
                        "complete",
                    );
                    self.invalidate_agent_list_cache(workspace_id);
                    // Same persisted-row event pair as the prompt-turn flush
                    // (§6.5); wake turns carry no turn correlation id.
                    self.publish_agent_message_events(workspace_id, agent_id, &message, None)
                        .await;
                    message_persisted = true;
                }
                Err(e) => {
                    tracing::warn!(agent = %agent_id, error = %e, "harness-wake turn persist failed");
                }
            }
        } else if !updates_applied {
            tracing::debug!(agent = %agent_id, "harness-wake turn produced no content");
        }
        // Same stored-on-write pending-questions marker as the prompt-turn
        // persist: a question-bearing wake tail arms the hold (question-free
        // tails leave the marker untouched).
        let marker_moved = if message_persisted && questions_persisted {
            self.record_pending_questions_marker(workspace_id, agent_id, &message_id)
                .await
        } else {
            false
        };
        // Same §6.5 step-0 recompute as the prompt-turn persist: only a
        // question-bearing tail moves the question-hold derivation.
        if marker_moved {
            self.maybe_emit_display_status_changed(workspace_id).await;
        }
        // Pin-respecting, same as the prompt-turn end above (monorepo#2110).
        self.clear_unpinned_live_turn(agent_id);
        let mut end_data = json!({ "agentId": agent_id.0 });
        if message_persisted {
            end_data["messageId"] = json!(message_id);
        }
        // Final live-preview values, same contract as the prompt-turn
        // terminal `agent:stream:end` above.
        stamp_preview_fields(&mut end_data, &preview_text_blocks);
        trace_stream_lifecycle(
            Some(message_id.as_str()),
            "message",
            "agent_stream_end",
            Some(turn_started.elapsed()),
            block_count,
            "complete",
        );
        self.publish_agent_event(workspace_id, agent_id, AGENT_STREAM_END, end_data)
            .await;
        HarnessWakeOutcome {
            message_id: message_persisted.then_some(message_id.clone()),
            empty_response,
            lifecycle: HarnessWakeLifecycle {
                correlation_id: message_id,
                block_count,
            },
        }
    }

    /// Recover from an empty harness-wake response (intent-hq/monorepo#3262):
    /// the wake turn opened but finalized with no meaningful content — the
    /// post-interrupt incident signature where a bare newline was accepted as
    /// `harness_wake_complete` and the agent stalled silently. Runs after the
    /// wake turn's slot is released and BEFORE the wake idle emit:
    ///
    /// 1. **Skip** when a ready-to-send queue entry exists (the imminent
    ///    drain is itself the nudge; the wake idle is suppressed on the same
    ///    condition) or when an attention request is already pending (the
    ///    stall is already surfaced — do not stack).
    /// 2. **Redrive** when the agent is redrive-eligible (delegated, in-task,
    ///    no report — [`Self::truncation_redrive_eligible`]) and the
    ///    consecutive counter (shared with the #2863 stall-episode streak) is
    ///    within [`MAX_CONSECUTIVE_TRUNCATION_REDRIVES`]: the harness's
    ///    empty-wake nudge is enqueued as a system-origin message tagged
    ///    `{"type": "empty_wake_redrive"}`; the caller's ready-to-send kick
    ///    drains it as a normal prompt turn. The streak clears on the next
    ///    clean completion, same as the prompt-turn redrive.
    /// 3. **Raise attention** otherwise (root/user-facing or taskless agents,
    ///    or the cap is spent): a `"blocker"` attention request with the
    ///    harness's turn-ended-unexpectedly reason, via the standard
    ///    [`Self::agent_request_attention_op`] surfaces (session fields,
    ///    transcript notice, `agent:attention-requested`, `needs_attention`
    ///    display status, parent wake for delegated callers) — the workspace
    ///    visibly needs input instead of looking healthy.
    ///
    /// Returns `true` when a nudge was enqueued (the caller's queue kick
    /// delivers it), `false` otherwise. Best-effort throughout: store
    /// failures fall through to the attention arm's own error handling and
    /// never fail the wake path.
    pub(crate) async fn recover_empty_harness_wake(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
    ) -> bool {
        if self.has_ready_to_send(agent_id) {
            return false;
        }
        if let Ok(session) = self.store.get_agent_session_summary(agent_id).await {
            if session.attention_request_kind.is_some() {
                return false;
            }
        }
        if self.truncation_redrive_eligible(agent_id).await {
            let streak = self.bump_truncation_redrives(agent_id);
            if streak <= MAX_CONSECUTIVE_TRUNCATION_REDRIVES {
                tracing::warn!(
                    agent = %agent_id,
                    streak,
                    "empty harness-wake response on a delegated in-task agent — enqueueing recovery nudge (monorepo#3262)"
                );
                let nudge = crate::harness::latest().empty_wake_redrive_nudge();
                self.enqueue_message_with_origin(
                    agent_id,
                    nudge,
                    None,
                    None,
                    Some(json!({ "type": "empty_wake_redrive" })),
                    None,
                    false,
                    false,
                );
                self.publish_queue_updated(agent_id).await;
                return true;
            }
            tracing::warn!(
                agent = %agent_id,
                streak,
                "empty harness-wake response — consecutive-redrive cap spent, raising attention (monorepo#3262)"
            );
        }
        // Attention arm: not redrive-eligible (root/user-facing, taskless)
        // or the cap is spent — surface the stall instead of idling
        // silently. The op's attention fields are cleared when the agent
        // next receives a message, so a fresh user prompt retires it.
        let reason = crate::harness::latest().empty_wake_attention_reason();
        if let Err(e) = self
            .agent_request_attention_op(
                workspace_id.clone(),
                "blocker".to_string(),
                reason,
                Some(agent_id.clone()),
            )
            .await
        {
            tracing::warn!(
                agent = %agent_id,
                error = %e,
                "empty harness-wake recovery: attention request failed (monorepo#3262)"
            );
        }
        false
    }

    /// Emit the `agent:idle` lifecycle signal for a finished harness-wake turn
    /// (monorepo#855), honoring the same ready-to-send suppression as a prompt
    /// turn's idle emit. `reason: "harness_wake_complete"` distinguishes it
    /// from `stream_complete` for subscribers. `empty_wake_response` stamps
    /// the advisory `emptyWakeResponse: true` (intent-hq/monorepo#3262) so
    /// subscribers can tell a no-op recovery wake from a healthy one; omitted
    /// otherwise (absent ≠ present-false, additive field).
    pub(crate) async fn publish_harness_wake_idle(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        lifecycle: &HarnessWakeLifecycle,
        empty_wake_response: bool,
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
        if empty_wake_response {
            data["emptyWakeResponse"] = Value::Bool(true);
        }
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
            Value::Bool(!self.waiting_watches_for_parent(agent_id).is_empty());
        // Idle-visibility: same `waitingOnHooks` stamp as the prompt-turn
        // idle (omitted when the agent owns no active hook).
        self.annotate_waiting_on_hooks(agent_id, &mut data).await;
        // Idle-visibility (unified external-wait): same `waitingOnPrMonitors`
        // stamp as the prompt-turn idle (omitted when the agent owns no
        // active monitor).
        self.annotate_waiting_on_pr_monitors(agent_id, &mut data)
            .await;
        // Same archived-workspace suppression hint as the prompt-turn idle
        // (omitted when the workspace is not archived).
        self.annotate_workspace_archived(workspace_id, &mut data)
            .await;
        self.record_group_completion_pre_publish(workspace_id, agent_id, &data)
            .await;
        trace_stream_lifecycle(
            Some(lifecycle.correlation_id.as_str()),
            "message",
            "agent_idle",
            None,
            lifecycle.block_count,
            "complete",
        );
        self.publish_agent_event(workspace_id, agent_id, AGENT_IDLE, data)
            .await;
    }

    /// Stamp `workspaceArchived: true` on an `agent:idle` payload when the
    /// containing workspace's status is `Archived`, so notification clients
    /// can suppress "agent finished" alerts for parked workspaces without a
    /// follow-up `workspace.get` read. Omitted when not archived — absent ≠
    /// present-false (additive field; older-payload readers are unaffected).
    /// Best-effort: a workspace read failure omits the field and the event
    /// still fires with the base payload.
    pub(crate) async fn annotate_workspace_archived(
        &self,
        workspace_id: &WorkspaceId,
        data: &mut Value,
    ) {
        match self.store.get_workspace(workspace_id).await {
            Ok(ws) if ws.status == WorkspaceStatus::Archived => {
                data["workspaceArchived"] = Value::Bool(true);
            }
            Ok(_) => {}
            Err(e) => {
                tracing::debug!(
                    workspace = %workspace_id,
                    error = %e,
                    "agent:idle workspaceArchived stamp: workspace read failed; omitting"
                );
            }
        }
    }

    /// Persist a standalone ACP `usage_update` cost (§5.23), ordered against
    /// the per-agent [`TurnBookkeeping`] chain: a cost arriving between a
    /// prompt releasing its busy slot and that prompt's *detached* bookkeeping
    /// task writing would otherwise read the pre-turn snapshot and write back
    /// the older counters. Awaiting the predecessor first keeps the same
    /// per-agent ordering the prompt path relies on; the write itself is
    /// awaited inline (this path holds no stream deadline), so the chain slot
    /// is left free for the next turn.
    pub(crate) async fn persist_cost_only_ordered(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        cost: UsageCost,
    ) {
        let prev = self
            .turn_bookkeeping
            .lock()
            .ok()
            .and_then(|mut chain| chain.remove(agent_id));
        if let Some(prev) = prev {
            let _ = prev.await;
        }
        self.persist_turn_token_usage(agent_id, workspace_id, None, Some(cost))
            .await;
    }

    /// Synthesize the standard per-turn usage report from the
    /// `PromptResponse._meta.usage` whole-prompt bill for providers that
    /// report usage only there (grok, intent-hq/intent#3803; audit §8.4).
    /// Called by the turn-end bookkeeping task when the standard `usage`
    /// field was absent: parses the bill first (cheap, no I/O — most
    /// providers have no such `_meta` shape) and only then resolves the
    /// session's provider to gate on
    /// [`reads_prompt_meta_usage`](crate::usage_semantics::reads_prompt_meta_usage),
    /// so a non-grok provider that happens to attach a similar `_meta` never
    /// gets misread. `None` when there is no bill, it is empty, or the
    /// provider does not report via `_meta` — the caller then proceeds
    /// exactly as before (report-less turn).
    pub(crate) async fn prompt_meta_turn_usage(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        meta: Option<&Meta>,
    ) -> Option<(session::Usage, Option<UsageCost>)> {
        let bill = crate::usage_semantics::prompt_meta_usage_bill(meta?)?;
        let provider_id = match self
            .store
            .get_agent_session_token_usage(workspace_id, agent_id)
            .await
        {
            Ok((model, _, provider, _)) => resolve_provider_id(
                model.as_deref(),
                provider.as_deref(),
                derived_default_provider(&self.effective_settings()).as_deref(),
            ),
            // Fail closed: without the provider column the gate cannot
            // confirm a `_meta`-billing provider — skip rather than misread.
            Err(e) => {
                tracing::warn!(agent = %agent_id, error = %e, "read provider for _meta usage failed");
                return None;
            }
        };
        crate::usage_semantics::reads_prompt_meta_usage(provider_id.as_deref()).then_some(bill)
    }

    /// Persist one turn's end-of-turn usage report and refresh the workspace
    /// tally (§5.23). How the report folds into the stored cumulative
    /// snapshot is provider-keyed (see `usage_semantics`,
    /// intent-hq/intent#3794/#3795): for spec-compliant cumulative providers
    /// the mapped report REPLACES the previously stored snapshot; for
    /// per-turn / last-request providers it SUMS into it. Either way the
    /// STORED snapshot remains cumulative per ACP session — the recreate
    /// baseline fold (monorepo#737) is unaffected. The workspace
    /// `TokenUsage` is then re-aggregated, persisted, and
    /// `workspace:tokenUsage-changed` emitted when it changed. Best-effort:
    /// errors are logged, never propagated — usage bookkeeping must not fail
    /// an otherwise-successful turn.
    ///
    /// `cost` is the latest ACP `usage_update` cost observed during the turn,
    /// cumulative per ACP session (latest wins). The two reports are
    /// independent — a provider may send either alone — so each part of the
    /// stored snapshot falls back to its previously persisted value when this
    /// turn carried no fresh report for it: a cost-only turn never zeroes the
    /// counters, and a counters-only turn never drops a cost already
    /// reported for the session.
    ///
    /// Exception: for a provider whose cost arrives on the per-turn
    /// `_meta.usage` bill (grok, #3803 — `reads_prompt_meta_usage`), `cost`
    /// covers the just-finished prompt only, so it SUMS into the stored cost
    /// (`UsageCost::merge`) instead of replacing it — mirroring the counters'
    /// `PerTurn` fold.
    pub(crate) async fn persist_turn_token_usage(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        usage: Option<&session::Usage>,
        cost: Option<UsageCost>,
    ) {
        // The session row is read unconditionally: the stored snapshot backs
        // both the missing-half fallback and the SUM accumulation, and the
        // model/provider columns key the report semantics. The configured
        // default is passed through so the resolution mirrors the spawn
        // precedence exactly: a bare-model session with `provider = NULL`
        // actually runs on `providers.active`, and classifying it as the
        // Cumulative default would reintroduce the undercount for a SUM
        // default provider (#3794/#3795).
        let (stored, semantics, per_turn_cost) = match self
            .store
            .get_agent_session_token_usage(workspace_id, agent_id)
            .await
        {
            Ok((model, _, provider, stored)) => {
                let provider_id = resolve_provider_id(
                    model.as_deref(),
                    provider.as_deref(),
                    derived_default_provider(&self.effective_settings()).as_deref(),
                );
                (
                    stored,
                    crate::usage_semantics::usage_report_semantics(provider_id.as_deref()),
                    crate::usage_semantics::reads_prompt_meta_usage(provider_id.as_deref()),
                )
            }
            // Degrade, never drop: with no readable prior snapshot the report
            // is persisted as-is (the semantics fall to the REPLACE default —
            // a SUM provider under-counts this one turn rather than
            // re-counting or dropping history).
            Err(e) => {
                tracing::warn!(agent = %agent_id, error = %e, "read prior token usage failed");
                (
                    None,
                    crate::usage_semantics::UsageReportSemantics::Cumulative,
                    false,
                )
            }
        };
        let mut snapshot = match usage {
            Some(usage) => {
                let report = token_usage::snapshot_from_turn_usage(usage);
                if semantics.sums_reports() {
                    // Per-turn / last-request report: SUM into the stored
                    // cumulative snapshot (#3794/#3795).
                    let mut acc = stored.clone().unwrap_or_default();
                    token_usage::add_totals(&mut acc, &report);
                    acc
                } else {
                    // Cumulative report: the mapped report IS the snapshot.
                    report
                }
            }
            None => stored.clone().unwrap_or_default(),
        };
        let stored_cost = stored.and_then(|s| s.cost);
        snapshot.cost = if per_turn_cost {
            // Per-turn `_meta.usage` bill (#3803): the fresh figure covers
            // one prompt — SUM with the stored session cost. `merge` also
            // covers the either-half-absent fallbacks.
            //
            // INVARIANT: SUM is safe only because a `reads_prompt_meta_usage`
            // provider's costs all originate from per-turn meta bills — grok
            // never emits `usage_update` costs (audit §8.1), so the cumulative
            // figures `persist_cost_only_ordered` routes through here (and the
            // seam's `turn_cost.or(cost)` preference) can never reach this
            // branch. If grok ever grows `usage_update` costs, key this on the
            // cost's SOURCE (meta bill vs usage_update), not the provider.
            UsageCost::merge(stored_cost.as_ref(), cost.as_ref())
        } else {
            // Cumulative `usage_update` cost: latest wins, stored fallback.
            cost.or(stored_cost)
        };
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
    /// store (usage-stats cards): the per-turn token delta — provider-keyed
    /// (see `usage_semantics`, intent-hq/intent#3794/#3795): for cumulative
    /// providers the new snapshot minus the previously persisted one,
    /// clamped ≥ 0 per counter; for per-turn / last-request providers the
    /// report itself IS the delta (no subtraction) — plus, for completed
    /// turns only (agent runs = completed prompt turns), a `runs` increment
    /// and the turn's wall-clock duration folded into the bucket's
    /// `longest_run_ms` MAX. Counters land in the current UTC hour bucket
    /// keyed by the session's stats model key — normalized model name,
    /// falling back to the provider id for placeholder/absent models,
    /// `"unknown"` only when the provider is unknowable too (D13) — with no
    /// workspace dimension, stamped with the daemon's local wall-clock
    /// (D12). MUST run BEFORE `persist_turn_token_usage` updates the session
    /// snapshot the cumulative delta is computed against — the per-agent
    /// chained bookkeeping task spawned in
    /// [`run_prompt_turn`](Self::run_prompt_turn) calls the two in that
    /// order. Best-effort: errors are logged, never propagated — stats
    /// bookkeeping must not fail a turn.
    pub(crate) async fn record_turn_usage_stats(
        &self,
        agent_id: &AgentId,
        workspace_id: &WorkspaceId,
        usage: Option<&session::Usage>,
        turn_duration: std::time::Duration,
        turn_end: time::OffsetDateTime,
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
        // Resolution mirrors the spawn precedence (compound model prefix →
        // provider field → configured default) so a bare-model session with
        // `provider = NULL` — which actually runs on `providers.active` —
        // keys the correct report semantics instead of falling to the
        // Cumulative default (#3794/#3795). A still-unresolvable provider
        // falls to the `"unknown"` stats tail (and the cumulative semantics
        // default below).
        let provider_id = prev_readable
            .then(|| {
                resolve_provider_id(
                    model.as_deref(),
                    provider.as_deref(),
                    derived_default_provider(&self.effective_settings()).as_deref(),
                )
            })
            .flatten();
        let semantics = crate::usage_semantics::usage_report_semantics(provider_id.as_deref());
        let tokens = match usage {
            // Per-turn / last-request report: the report IS the turn's delta
            // (no snapshot subtraction — #3794/#3795).
            Some(u) if semantics.sums_reports() => token_usage::snapshot_from_turn_usage(u),
            Some(u) if prev_readable => usage_stats::turn_token_delta(
                prev.as_ref(),
                &token_usage::snapshot_from_turn_usage(u),
            ),
            _ => intent_core::TokenUsageTotals::default(),
        };
        let delta = intent_store::UsageStatsDelta {
            input_tokens: tokens.input_tokens,
            output_tokens: tokens.output_tokens,
            cache_read_tokens: tokens.cache_read_tokens,
            cache_creation_tokens: tokens.cache_creation_tokens,
            thought_tokens: tokens.thought_tokens,
            runs: u64::from(run_completed),
            longest_run_ms: if run_completed {
                u64::try_from(turn_duration.as_millis()).unwrap_or(u64::MAX)
            } else {
                0
            },
            ..Default::default()
        };
        // `turn_end` is captured at the same instant as `turn_duration` at the
        // call site, before this task is spawned/awaited — reading the clock
        // here instead would drift the hourly bucket and the per-minute spread
        // window past the real turn end by however long the bookkeeping queued.
        let now = turn_end;
        let bucket = usage_stats::hour_bucket_utc(now);
        let local = usage_stats::recording_local_offset().map(|o| usage_stats::local_stamp(now, o));
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
        // Same clamped per-turn delta, spread evenly across every per-minute
        // bucket the turn spanned in `stats.getRateHistory` (§5.39) so a
        // multi-minute turn reads as a plateau rather than a lone end-minute
        // spike. All-zero deltas (and all-zero split parts) are skipped — they
        // add nothing and would only churn the capped table.
        let rate_delta = intent_store::UsageRateDelta {
            input_tokens: tokens.input_tokens,
            output_tokens: tokens.output_tokens,
            cache_read_tokens: tokens.cache_read_tokens,
            cache_creation_tokens: tokens.cache_creation_tokens,
            thought_tokens: tokens.thought_tokens,
        };
        if !rate_delta.is_zero() {
            for (bucket, part) in
                crate::usage_rate::split_delta_across_minutes(now, turn_duration, &rate_delta)
            {
                if part.is_zero() {
                    continue;
                }
                if let Err(e) = self.store.add_usage_rate(&bucket, &part).await {
                    tracing::warn!(agent = %agent_id, error = %e, "record turn usage rate failed");
                }
            }
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
            MappedUpdate::Chunk {
                content,
                text,
                thought,
            } => {
                // Accumulate into the transcript and compute the block index this
                // chunk lands at; consecutive chunks of the same kind coalesce
                // onto one index (and thus one stable block id), while a
                // thought↔text switch or a non-text block starts a new one.
                // Thought chunks flush as `thinking` blocks (Zed's model) and
                // ride the same `chat:stream:delta` shape.
                let (block_index, block_type) = if let Some(t) = &text {
                    let index = transcript.push_chunk(t, thought);
                    let block_type = if thought { "thinking" } else { "text" };
                    (index, block_type.to_string())
                } else {
                    let block_type = content
                        .get("type")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown")
                        .to_string();
                    (transcript.push_block(content.clone()), block_type)
                };
                // Internal chat-channel delta (§7.1): the full content-bearing
                // payload the per-agent `chat.subscribe` forwarder accumulates
                // into block deltas (D4 block identity kept).
                self.publish_agent_event(
                    workspace_id,
                    agent_id,
                    CHAT_STREAM_DELTA,
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
                // External activity signal (§7): leading-edge throttled per
                // agent — the first chunk of a turn emits immediately
                // (preserves the FE's pre-first-token status-hint clearing
                // latency), then at most one per second. Carries the
                // server-derived live preview (`lastAgentResponse` / `digest`
                // from the streamed-so-far text) so watched-agent rows update
                // push-style without a refetch.
                if self.should_emit_activity(agent_id) {
                    let mut activity_data = json!({
                        "agentId": agent_id.0,
                        "messageId": message_id,
                    });
                    stamp_live_preview_fields(
                        &mut activity_data,
                        &transcript.text_block_strings(),
                        transcript.final_text_block_open(),
                    );
                    self.publish_agent_event(
                        workspace_id,
                        agent_id,
                        AGENT_STREAM_ACTIVITY,
                        activity_data,
                    )
                    .await;
                }
            }
            MappedUpdate::Usage(usage) => {
                // Context-window occupancy (intent-hq/intent#3797): recorded
                // latest-wins in the in-memory per-agent registry — a signal
                // for the `contextUsage` read overlay, NEVER folded into the
                // token tallies.
                self.record_context_usage(agent_id, usage.used, usage.size);
                // §5.23: cumulative per ACP session, so the latest report
                // wins. Recorded on the transcript and persisted with the
                // turn's token snapshot; it materializes no transcript
                // content and publishes no event, so this is NOT a turn
                // update (the redrive eligibility must stay unaffected).
                if let Some(cost) = usage.cost {
                    transcript.usage_cost = Some(UsageCost {
                        amount: cost.amount,
                        currency: cost.currency,
                    });
                }
                return false;
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
                        .map(intent_core::TurnAttachment::resource_item)
                        .collect()
                } else {
                    Vec::new()
                };
                // D6: accumulate tool_use/tool_result blocks into the transcript
                // so they persist (and reach `agent.getConversation`). A dropped
                // update (STAB-124: anonymous first sight) publishes no event.
                let Some(recorded) = transcript.record_tool(&tc, registered.clone()) else {
                    return true;
                };
                let block_index = recorded.use_index;
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
                    "toolName": &tool_name,
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
                // §7.1 (monorepo#2029): carry the REAL ids of the blocks this
                // update just materialized. The live `chat.subscribe` mapper
                // used to predict them as `tool_use index + 1`, which collides
                // with an interleaved text block or a parallel call's
                // `tool_use` and clobbers it on every id-keyed client until
                // the terminal reconcile heals it. Present only when the
                // block exists (a `started`/output-less update materializes
                // no result block, so both fields stay absent).
                if let Some(rindex) = recorded.result_index {
                    data["resultBlockIndex"] = json!(rindex);
                    data["resultBlockId"] = json!(transcript.block_id(rindex));
                }
                if !recorded.proposal_indices.is_empty() {
                    data["proposalBlockIds"] = Value::Array(
                        recorded
                            .proposal_indices
                            .iter()
                            .map(|&i| Value::String(transcript.block_id(i)))
                            .collect(),
                    );
                }
                // Carry the claimed canonical batch on the event so the live
                // `chat.subscribe` delta path attaches the SAME blocks the
                // persisted transcript does (byte-identical invariant).
                if !registered.is_empty() {
                    data["registeredAttachments"] = Value::Array(registered);
                }
                self.publish_agent_event(workspace_id, agent_id, AGENT_TOOL_CALL, data)
                    .await;
                // External activity signal (§7): tool calls keep the liveness
                // tick (and the pushed preview) alive through tool-heavy
                // stretches where no assistant text streams — otherwise a
                // watched-agent row freezes at the turn's last text
                // (monorepo#1414). Shares the ONE per-agent leading-edge
                // throttle window with the chunk arm, so a turn mixing text
                // and tool calls still emits at most one activity per second.
                // Adds `lastToolUse` describing the call just recorded.
                if self.should_emit_activity(agent_id) {
                    let mut activity_data = json!({
                        "agentId": agent_id.0,
                        "messageId": message_id,
                        "lastToolUse": {
                            "name": tool_name,
                            "status": tc.status,
                        },
                    });
                    stamp_live_preview_fields(
                        &mut activity_data,
                        &transcript.text_block_strings(),
                        transcript.final_text_block_open(),
                    );
                    self.publish_agent_event(
                        workspace_id,
                        agent_id,
                        AGENT_STREAM_ACTIVITY,
                        activity_data,
                    )
                    .await;
                }
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

    /// Publish the mid-turn `stalled` `agent:stream:status`
    /// (intent-hq/monorepo#3402): the [`Self::publish_status_event`] shape
    /// plus the additive `silentMs` field carrying the measured silence at
    /// emission, `level: "warn"`. Advisory only — emitted while the turn keeps
    /// running; the FE clears the presentation on `resumed`, any new stream
    /// delta, or turn end/failure.
    async fn publish_stalled_status_event(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        silent_ms: u64,
    ) {
        self.publish_agent_event(
            workspace_id,
            agent_id,
            AGENT_STREAM_STATUS,
            json!({
                "agentId": agent_id.0,
                "workspaceId": workspace_id,
                "phase": "stalled",
                "message": format!("No model activity for {}s", silent_ms / 1000),
                "level": "warn",
                "silentMs": silent_ms,
                "timestamp": now_epoch_ms(),
            }),
        )
        .await;
    }

    /// Clear an emitted mid-turn stall on stream activity
    /// (intent-hq/monorepo#3402): when a `stalled` status is outstanding,
    /// publish the paired `resumed` `agent:stream:status` and re-arm the
    /// detector. Called from EVERY point `run_prompt_turn` observes a
    /// `session/update` — the select-loop arm AND the buffered `try_recv`
    /// drains (`prompt_fut` can win the `select!` with notes still queued;
    /// without this, a stalled turn that resolves with buffered activity
    /// would end on `stalled` → `stream:end` with no `resumed` between).
    async fn clear_stream_stall(
        &self,
        stall_emitted: &mut bool,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
    ) {
        if *stall_emitted {
            *stall_emitted = false;
            self.publish_status_event(
                workspace_id,
                agent_id,
                "resumed",
                "Stream activity resumed",
                "info",
            )
            .await;
        }
    }

    /// Build and publish an agent streaming event onto the bus (§6.6/§10).
    /// `pub(crate)` so the [`AgentManager`] stop path can emit the terminal
    /// `agent:stream:end` when it interrupts a turn (the worker that would
    /// otherwise emit it is aborted). Routes the high-volume stream signals
    /// (`chat:stream:delta`, `agent:stream:activity`) through the transient
    /// (broadcast-only, never persisted) path; all other event types persist
    /// durably.
    pub(crate) async fn publish_agent_event(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        event_type: &str,
        mut data: Value,
    ) {
        // `agent:failed` carries the failing agent's delegation parentage so
        // subscribers (parent coordinators, FE grouping) can attribute a child
        // failure without a follow-up `agent.get`. Optional: OMITTED entirely
        // for parentless agents — never `null`. Enriched centrally here so
        // every terminal-failure emit site (prompt turn, idle-timeout cap,
        // spawn/turn terminal pair) carries it. The same session read also
        // stamps `agentName` (intent-hq/monorepo#2869) so completion-wake
        // subscribers never fall back to rendering the raw agent id.
        // Best-effort: a store error — or a non-object payload (guarded via
        // `as_object_mut` so a malformed `data` can't panic the index-assign)
        // — leaves the payload untouched.
        if event_type == AGENT_FAILED
            && (data.get("parentAgentId").is_none() || data.get("agentName").is_none())
        {
            if let Ok(session) = self.store.get_agent_session(agent_id).await {
                if let Some(map) = data.as_object_mut() {
                    if let Some(parent) = session.parent_agent_id {
                        map.entry("parentAgentId".to_string())
                            .or_insert(Value::String(parent.0));
                    }
                    map.entry("agentName".to_string())
                        .or_insert(Value::String(session.name));
                }
            }
        }
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
        // Route the stream firehose through the transient path (broadcast-
        // only); persist all other agent events (stream:status, stream:end,
        // tool:call, lifecycle, etc.) for durable audit trail.
        if event_type == CHAT_STREAM_DELTA || event_type == AGENT_STREAM_ACTIVITY {
            crate::publish_event_transient(self.event_bus.as_ref(), &event);
        } else {
            crate::publish_event(self.event_bus.as_ref(), event).await;
        }
    }

    /// Persist the session-discovered effort levels (PROTOCOL §5.5, Option C)
    /// after a session open: the `thought_level` selector's surfaced levels
    /// ([`ThoughtLevelOption::surfaced_levels`] — advertised values minus the
    /// `"default"` sentinel), `None` when the provider advertised no selector
    /// (clears a set persisted by a previous provider). Emits `agent:updated`
    /// with `effortLevels` ONLY when the stored set actually changed
    /// ([`set_agent_effort_levels`](intent_store::Store::set_agent_effort_levels)
    /// reports the change), so the every-open wholesale replace never spams
    /// the bus. Best-effort: a store error is logged and never fails session
    /// startup.
    pub(crate) async fn persist_session_effort_levels(
        &self,
        workspace_id: &WorkspaceId,
        agent_id: &AgentId,
        thought_level: Option<&ThoughtLevelOption>,
    ) {
        let levels = thought_level.and_then(ThoughtLevelOption::surfaced_levels);
        match self
            .store
            .set_agent_effort_levels(workspace_id, agent_id, levels.as_deref(), &now_iso())
            .await
        {
            Ok(true) => {
                self.publish_agent_mutation_event(
                    workspace_id,
                    agent_id,
                    AGENT_UPDATED,
                    json!({
                        "agentId": agent_id.0,
                        "effortLevels": levels.map_or(Value::Null, |l| json!(l)),
                    }),
                )
                .await;
            }
            Ok(false) => {
                // Unchanged set — no write, no event (the common case).
            }
            Err(e) => {
                tracing::warn!(
                    agent = %agent_id,
                    error = %e,
                    "persist session effort levels failed"
                );
            }
        }
    }
}
