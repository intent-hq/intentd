//! Snapshot-then-delta subscription fast-path (PROTOCOL §6, TB-0 §1).
//!
//! Pure, transport-agnostic helpers that mirror [`crate::events`] but emit
//! `subscription.push` frames (§3.3) instead of `events.event`. A `*.subscribe`
//! request is intercepted in [`crate::conn::process_frame`] before the JSON-RPC
//! dispatcher: the connection task subscribes to the bus, replies with the
//! `subscriptionId`, then a per-subscription forwarder materializes the snapshot
//! (seq 0) and tails live change events as `{ added, updated, removedIds }`
//! deltas (seq 1, 2, …). TB-4 wires the `note` channel end-to-end; other
//! channels (TB-5) reuse this machinery. The legacy `events.subscribe` firehose
//! (`events.event`) is left intact and coexists (Risk R1).

use intent_core::events::{
    AGENT_COMPLETED, AGENT_CREATED, AGENT_DELETED, AGENT_FAILED, AGENT_IDLE, AGENT_RENAMED,
    AGENT_RESTORED, AGENT_STARTED, AGENT_STATUS_CHANGED, AGENT_STREAM_CHUNK, AGENT_STREAM_END,
    AGENT_TOOL_CALL, AGENT_UPDATED, COMMENT_ADDED, NOTE_CREATED, NOTE_DELETED, NOTE_UPDATED,
    PR_LINKED, PR_UNLINKED, PR_UPDATED, TASK_STATUS_CHANGED, WORKSPACE_ACTIVITY_CHANGED,
    WORKSPACE_ATTENTION_CHANGED, WORKSPACE_CREATED, WORKSPACE_DELETED, WORKSPACE_UPDATED,
};
use intent_core::{now_iso, AgentId, Event, NoteId, WorkspaceApi, WorkspaceId};
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet};

use crate::events::IdInfo;

/// A subscription channel selected by the `*.subscribe` method (TB-0 §3). TB-4
/// wires the `note` collection channel end-to-end; TB-5 adds `task`, `agent`,
/// `workspace`, and `comment`, all reusing the same snapshot+delta machinery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Channel {
    Note,
    Task,
    Agent,
    Workspace,
    Comment,
    /// Per-agent chat stream (CS-0). Scoped by `agentId`; tails the
    /// `agent:stream:*` family for one agent (snapshot = newest conversation
    /// page; live deltas land in CS-3).
    Chat,
}

/// A classified subscription fast-path request awaiting handling by the
/// connection task. Mirrors [`crate::events::FastPath`] but for `subscription.push`.
pub(crate) enum SubFastPath {
    Subscribe {
        id: IdInfo,
        channel: Channel,
        params: Map<String, Value>,
    },
    Unsubscribe {
        id: IdInfo,
        params: Map<String, Value>,
    },
}

/// Parsed params for a workspace-scoped collection channel (`note`/`task`/
/// `agent`, §6.1). `workspaceId` is required; `replaceGroup` is optional;
/// `sinceSeq` is reserved for post-v1.0 replay (§6.4) and ignored here.
#[derive(Debug)]
pub(crate) struct NoteSubscribeParams {
    pub workspace_id: String,
    pub replace_group: Option<String>,
}

/// Parsed `workspace.subscribe` params (§6.1). The workspace channel is global
/// (per-user, no `workspaceId` scope); only `replaceGroup` is honored.
#[derive(Debug)]
pub(crate) struct WorkspaceSubscribeParams {
    pub replace_group: Option<String>,
}

/// Parsed `comment.subscribe` params (§6.1). The comment channel is per-resource:
/// both `workspaceId` and `noteId` are required; `replaceGroup` is optional.
#[derive(Debug)]
pub(crate) struct CommentSubscribeParams {
    pub workspace_id: String,
    pub note_id: String,
    pub replace_group: Option<String>,
}

/// Parsed `chat.subscribe` params (CS-0). The chat channel is per-resource,
/// scoped by `agentId` (like the comment channel's `noteId`, NOT `workspaceId`);
/// `replaceGroup` is optional.
#[derive(Debug)]
pub(crate) struct ChatSubscribeParams {
    pub agent_id: String,
    pub replace_group: Option<String>,
}

/// Classify a parsed frame as a subscription fast-path request, or `None` to
/// fall through to the firehose / JSON-RPC dispatcher. Same JSON-RPC 2.0
/// envelope pre-check as [`crate::events::classify`].
pub(crate) fn classify(value: &Value) -> Option<SubFastPath> {
    let obj = value.as_object()?;
    if obj.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return None;
    }
    let method = obj.get("method").and_then(Value::as_str)?;
    let id_member = obj.get("id");
    if let Some(v) = id_member {
        if !v.is_null() && !v.is_string() && !v.is_number() {
            return None;
        }
    }
    let id = IdInfo {
        present: id_member.is_some(),
        echo: id_member.cloned().unwrap_or(Value::Null),
    };
    let params = obj
        .get("params")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    match method {
        "note.subscribe" => Some(SubFastPath::Subscribe {
            id,
            channel: Channel::Note,
            params,
        }),
        "task.subscribe" => Some(SubFastPath::Subscribe {
            id,
            channel: Channel::Task,
            params,
        }),
        "workspace.subscribe" => Some(SubFastPath::Subscribe {
            id,
            channel: Channel::Workspace,
            params,
        }),
        "comment.subscribe" => Some(SubFastPath::Subscribe {
            id,
            channel: Channel::Comment,
            params,
        }),
        // `chat.*` is a new, distinct method name (no alias/deprecated path
        // collides with it), so it routes cleanly to the per-agent chat channel.
        "chat.subscribe" => Some(SubFastPath::Subscribe {
            id,
            channel: Channel::Chat,
            params,
        }),
        // The collection `agent` channel shares the `agent.subscribe` method
        // name with the pre-existing deprecated service-style alias (router,
        // §5.5). Disambiguate by params: the alias always carries `eventTypes`,
        // the collection channel never does — so an `eventTypes`-bearing frame
        // falls through to the router (coexistence; naming reconciliation is
        // TB-6, design R1). The alias likewise requires `workspaceId` on
        // `agent.unsubscribe`, so a bare `{ subscriptionId }` is ours.
        "agent.subscribe" if !params.contains_key("eventTypes") => Some(SubFastPath::Subscribe {
            id,
            channel: Channel::Agent,
            params,
        }),
        "note.unsubscribe"
        | "task.unsubscribe"
        | "workspace.unsubscribe"
        | "comment.unsubscribe"
        | "chat.unsubscribe" => Some(SubFastPath::Unsubscribe { id, params }),
        "agent.unsubscribe" if !params.contains_key("workspaceId") => {
            Some(SubFastPath::Unsubscribe { id, params })
        }
        _ => None,
    }
}

/// Validate `note.subscribe` params. A missing/empty `workspaceId` is a
/// `-32602` error (the note channel is workspace-scoped, §6.2).
pub(crate) fn parse_subscribe_params(
    params: &Map<String, Value>,
) -> Result<NoteSubscribeParams, String> {
    let workspace_id = match params.get("workspaceId").and_then(Value::as_str) {
        Some(s) if !s.is_empty() => s.to_string(),
        _ => return Err("workspaceId is required".to_string()),
    };
    Ok(NoteSubscribeParams {
        workspace_id,
        replace_group: replace_group(params),
    })
}

/// Validate `workspace.subscribe` params. The channel is global, so only the
/// optional `replaceGroup` is read (§6.2).
pub(crate) fn parse_workspace_subscribe_params(
    params: &Map<String, Value>,
) -> Result<WorkspaceSubscribeParams, String> {
    Ok(WorkspaceSubscribeParams {
        replace_group: replace_group(params),
    })
}

/// Validate `comment.subscribe` params. A missing/empty `workspaceId` or
/// `noteId` is a `-32602` error (the comment channel is note-scoped, §6.2).
pub(crate) fn parse_comment_subscribe_params(
    params: &Map<String, Value>,
) -> Result<CommentSubscribeParams, String> {
    let workspace_id = match params.get("workspaceId").and_then(Value::as_str) {
        Some(s) if !s.is_empty() => s.to_string(),
        _ => return Err("workspaceId is required".to_string()),
    };
    let note_id = match params.get("noteId").and_then(Value::as_str) {
        Some(s) if !s.is_empty() => s.to_string(),
        _ => return Err("noteId is required".to_string()),
    };
    Ok(CommentSubscribeParams {
        workspace_id,
        note_id,
        replace_group: replace_group(params),
    })
}

/// Validate `chat.subscribe` params. A missing/empty `agentId` is a `-32602`
/// error (the chat channel is per-agent, CS-0).
pub(crate) fn parse_chat_subscribe_params(
    params: &Map<String, Value>,
) -> Result<ChatSubscribeParams, String> {
    let agent_id = match params.get("agentId").and_then(Value::as_str) {
        Some(s) if !s.is_empty() => s.to_string(),
        _ => return Err("agentId is required".to_string()),
    };
    Ok(ChatSubscribeParams {
        agent_id,
        replace_group: replace_group(params),
    })
}

/// Read the optional `replaceGroup` string shared by every channel's params.
fn replace_group(params: &Map<String, Value>) -> Option<String> {
    params
        .get("replaceGroup")
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// Build a `subscription.push { kind: "snapshot", seq, snapshot }` notification
/// (§3.3 / §6.1). `seq` is 0 for the initial full state.
pub(crate) fn build_snapshot_push(subscription_id: &str, seq: u64, snapshot: &Value) -> String {
    let frame = json!({
        "jsonrpc": "2.0",
        "method": "subscription.push",
        "params": {
            "subscriptionId": subscription_id,
            "kind": "snapshot",
            "seq": seq,
            "snapshot": snapshot,
        }
    });
    serde_json::to_string(&frame).unwrap_or_default()
}

/// Build a `subscription.push { kind: "delta", seq, delta }` notification
/// (§3.3 / §6.3). `delta` carries the per-entity `{ added, updated, removedIds }`.
pub(crate) fn build_delta_push(subscription_id: &str, seq: u64, delta: &Value) -> String {
    let frame = json!({
        "jsonrpc": "2.0",
        "method": "subscription.push",
        "params": {
            "subscriptionId": subscription_id,
            "kind": "delta",
            "seq": seq,
            "delta": delta,
        }
    });
    serde_json::to_string(&frame).unwrap_or_default()
}

/// Map one `note:*` bus change event to a note-channel delta by re-reading the
/// latest entity (TB-0 §2.2 option B). `note:created` → `added`, `note:updated`
/// → `updated`, `note:deleted` → `removedIds`. Returns `None` for events that do
/// not translate (no `noteId`, unrelated type, or a re-read miss).
pub(crate) async fn note_delta(
    api: &dyn WorkspaceApi,
    workspace_id: &WorkspaceId,
    event: &Event,
) -> Option<Value> {
    let note_id = event.data.get("noteId").and_then(Value::as_str)?;
    match event.event_type.as_str() {
        NOTE_CREATED => {
            let note = api
                .get_note(workspace_id.clone(), NoteId::from(note_id))
                .await
                .ok()?;
            Some(json!({ "added": [serde_json::to_value(note).ok()?] }))
        }
        NOTE_UPDATED => {
            let note = api
                .get_note(workspace_id.clone(), NoteId::from(note_id))
                .await
                .ok()?;
            Some(json!({ "updated": [serde_json::to_value(note).ok()?] }))
        }
        NOTE_DELETED => Some(json!({ "removedIds": [note_id] })),
        _ => None,
    }
}

/// Whether a channel is global (no `workspaceId` scope on its bus filter). Only
/// `workspace` is per-user/global; every other v1.0 channel is workspace-scoped.
pub(crate) fn channel_is_global(channel: Channel) -> bool {
    matches!(channel, Channel::Workspace)
}

/// The bus event types a channel tails for deltas (TB-0 §3). The `agent:stream:*`
/// chat family is intentionally excluded (deferred per design R7).
pub(crate) fn channel_event_types(channel: Channel) -> Vec<String> {
    let types: &[&str] = match channel {
        Channel::Note => &[NOTE_CREATED, NOTE_UPDATED, NOTE_DELETED],
        Channel::Task => &[
            NOTE_CREATED,
            NOTE_UPDATED,
            NOTE_DELETED,
            TASK_STATUS_CHANGED,
        ],
        Channel::Agent => &[
            AGENT_CREATED,
            AGENT_STARTED,
            AGENT_COMPLETED,
            AGENT_FAILED,
            AGENT_IDLE,
            AGENT_STATUS_CHANGED,
            AGENT_RENAMED,
            AGENT_UPDATED,
            AGENT_RESTORED,
            AGENT_DELETED,
        ],
        Channel::Workspace => &[
            WORKSPACE_CREATED,
            WORKSPACE_UPDATED,
            WORKSPACE_DELETED,
            WORKSPACE_ACTIVITY_CHANGED,
            WORKSPACE_ATTENTION_CHANGED,
            PR_LINKED,
            PR_UPDATED,
            PR_UNLINKED,
        ],
        Channel::Comment => &[COMMENT_ADDED],
        // The chat channel is the one consumer of the `agent:stream:*` family
        // (CS-0); the forwarder additionally filters these to one agent by
        // `sessionId == agentId`.
        Channel::Chat => &[AGENT_STREAM_CHUNK, AGENT_TOOL_CALL, AGENT_STREAM_END],
    };
    types.iter().map(|s| s.to_string()).collect()
}

/// Materialize a channel's seq-0 snapshot as a JSON array (TB-0 §1.4). On a read
/// error the snapshot degrades to the empty array rather than failing the
/// subscription. `note_id` is required by the per-note `comment` channel.
pub(crate) async fn channel_snapshot(
    api: &dyn WorkspaceApi,
    channel: Channel,
    workspace_id: &WorkspaceId,
    note_id: Option<&NoteId>,
) -> Value {
    let empty = || Value::Array(Vec::new());
    match channel {
        Channel::Note => match api.list_notes(workspace_id).await {
            Ok(notes) => serde_json::to_value(notes).unwrap_or_else(|_| empty()),
            Err(_) => empty(),
        },
        Channel::Task => match api.list_notes(workspace_id).await {
            Ok(notes) => {
                let tasks: Vec<_> = notes
                    .into_iter()
                    .filter(|n| n.metadata.task.is_some())
                    .collect();
                serde_json::to_value(tasks).unwrap_or_else(|_| empty())
            }
            Err(_) => empty(),
        },
        Channel::Agent => match api.agent_list(workspace_id.clone()).await {
            Ok(agents) => serde_json::to_value(agents).unwrap_or_else(|_| empty()),
            Err(_) => empty(),
        },
        Channel::Workspace => match api.list_workspaces(false).await {
            Ok(workspaces) => serde_json::to_value(workspaces).unwrap_or_else(|_| empty()),
            Err(_) => empty(),
        },
        Channel::Comment => match note_id {
            Some(note_id) => match api
                .comment_list(
                    workspace_id.clone(),
                    note_id.clone(),
                    None,
                    None,
                    None,
                    true,
                )
                .await
            {
                Ok(result) => serde_json::to_value(result.threads).unwrap_or_else(|_| empty()),
                Err(_) => empty(),
            },
            None => empty(),
        },
        // The chat channel uses the dedicated `chat_snapshot` /
        // `forward_chat_subscription` path (a per-agent `messages[]` object
        // snapshot, CS-0 D3), so this generic arm is unreachable.
        Channel::Chat => empty(),
    }
}

/// Materialize the chat channel's seq-0 snapshot: the newest page of
/// `agent.getConversation` as the `{ agentId, messages, truncated,
/// totalMessages, nextToken }` OBJECT (CS-0 D3), reused verbatim from the
/// existing paginated read, then — when a turn is currently streaming —
/// the in-flight partial assistant message merged in (CS-0 D5) so a
/// `chat.subscribe` arriving mid-turn reconstructs a coherent in-flight message.
/// The merge is gated on [`WorkspaceApi::agent_is_busy`]: a lingering
/// `agent_live_turn` slot whose worker is gone (mid-turn crash, race between
/// abort and slot release) MUST NOT surface a phantom "streaming" message
/// when a client opens the chat. On a read error the snapshot degrades to an
/// empty messages page rather than failing the subscription (matching
/// [`channel_snapshot`]'s degrade-to-empty pattern).
pub(crate) async fn chat_snapshot(api: &dyn WorkspaceApi, agent_id: &AgentId) -> Value {
    let mut snapshot = match api
        .agent_get_conversation(agent_id.clone(), None, None, None)
        .await
    {
        Ok(v) => v,
        Err(_) => json!({
            "agentId": agent_id.as_str(),
            "messages": [],
            "truncated": false,
            "totalMessages": 0,
            "nextToken": Value::Null,
        }),
    };
    if api.agent_is_busy(agent_id.clone()) {
        if let Some(live) = api.agent_live_turn(agent_id.clone()) {
            merge_live_turn(&mut snapshot, agent_id, &live);
        }
    }
    // Overlay the daemon-owned activity flags (PROTOCOL §7.1) so a client
    // arriving mid-turn renders the same `isResponding`/`isWaitingOnTool`/
    // `isWaitingForOtherAgents` (+ the companion `waitingForAgentIds` list)
    // and STAB-125 turn-liveness (`turnInFlight`/`lastStreamActivityAt`)
    // state as the `AgentLite` projection (§5.5).
    let flags = api.agent_activity_flags(agent_id.clone()).await;
    if let (Some(obj), Some(flag_obj)) = (snapshot.as_object_mut(), flags.as_object()) {
        for (key, value) in flag_obj {
            obj.insert(key.clone(), value.clone());
        }
    }
    snapshot
}

/// Append the in-flight assistant message to a chat snapshot's `messages` page
/// (CS-0 D5). Its `seq` is the next monotonic value (`totalMessages`, since seq
/// is contiguous from 0) and it carries `isStreaming: true` as a render hint the
/// terminal reconcile clears via `streamingComplete`. Idempotent: if the turn's
/// message already persisted (id present in the page) it is left untouched, so a
/// snapshot taken in the window between persist and slot-clear never duplicates.
fn merge_live_turn(snapshot: &mut Value, agent_id: &AgentId, live: &Value) {
    let Some(message_id) = live.get("messageId").and_then(Value::as_str) else {
        return;
    };
    let blocks = live
        .get("contentBlocks")
        .cloned()
        .unwrap_or_else(|| json!([]));
    let Some(obj) = snapshot.as_object_mut() else {
        return;
    };
    let total = obj
        .get("totalMessages")
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let messages = obj.entry("messages").or_insert_with(|| json!([]));
    let Some(arr) = messages.as_array_mut() else {
        return;
    };
    if arr
        .iter()
        .any(|m| m.get("id").and_then(Value::as_str) == Some(message_id))
    {
        return;
    }
    arr.push(json!({
        "id": message_id,
        "agentId": agent_id.as_str(),
        "seq": total,
        "role": "assistant",
        "contentBlocks": blocks,
        "timestamp": now_iso(),
        "isStreaming": true,
    }));
    obj.insert("totalMessages".to_string(), json!(total + 1));
}

/// Stateful, event-payload-driven delta mapper for the per-agent chat channel
/// (CS-0 D2/D4/D6). Unlike the re-read channels, in-flight chat content is not
/// persisted until `stream:end`, so the forwarder builds block deltas straight
/// from the enriched `agent:stream:*` payloads (D4) and accumulates the per-block
/// state needed to emit the FULL current block on every change (D2). A turn's
/// blocks are keyed by their stable `{messageId}:{index}` id (D1): a text block's
/// first chunk arrives as `added`, each subsequent growth as `updated`; a tool
/// call synthesizes a `tool_use` block (then a `tool_result` block on completion,
/// D6). On `stream:end` it reconciles against the now-persisted message so the
/// client's accumulated state (seq-0 snapshot + deltas) equals a fresh
/// `agent.getConversation` snapshot (the CS-3 reconciliation invariant).
pub(crate) struct ChatDeltaState {
    agent_id: String,
    /// `blockId` → accumulated text for `text` blocks (deltas carry full text, D2).
    text_acc: HashMap<String, String>,
    /// `blockId`s emitted at least once this turn (added vs updated discriminator).
    seen_ids: HashSet<String>,
    /// `blockId`s emitted live this turn (to compute orphan `removedIds` at end).
    emitted_ids: HashSet<String>,
    /// The in-flight assistant message id (block-id prefix), learned from events.
    message_id: Option<String>,
}

impl ChatDeltaState {
    /// A fresh mapper for one chat subscription (one agent). The same instance is
    /// reused across turns; `finalize` resets the per-turn accumulation.
    pub(crate) fn new(agent_id: &AgentId) -> Self {
        Self {
            agent_id: agent_id.as_str().to_string(),
            text_acc: HashMap::new(),
            seen_ids: HashSet::new(),
            emitted_ids: HashSet::new(),
            message_id: None,
        }
    }

    /// Seed the per-turn accumulation from a seq-0 snapshot that carries an
    /// in-flight assistant message (CS-0 D5): the message arriving mid-turn has
    /// already streamed some blocks, so prime `message_id`, mark each block id as
    /// already seen+emitted (subsequent chunks arrive as `updated`, not `added`),
    /// and pre-load each `text` block's accumulated text so the NEXT chunk delta
    /// carries the FULL text (D2), not just the new fragment. Without this, a
    /// resuming subscriber's first chunk would restart the text from empty until
    /// the terminal reconcile. No-op when the snapshot has no in-flight message.
    pub(crate) fn seed_from_snapshot(&mut self, snapshot: &Value) {
        let Some(messages) = snapshot.get("messages").and_then(Value::as_array) else {
            return;
        };
        let Some(msg) = messages
            .iter()
            .find(|m| m.get("isStreaming") == Some(&Value::Bool(true)))
        else {
            return;
        };
        let Some(message_id) = msg.get("id").and_then(Value::as_str) else {
            return;
        };
        self.message_id = Some(message_id.to_string());
        let Some(blocks) = msg.get("contentBlocks").and_then(Value::as_array) else {
            return;
        };
        for block in blocks {
            let Some(bid) = block.get("id").and_then(Value::as_str) else {
                continue;
            };
            self.seen_ids.insert(bid.to_string());
            self.emitted_ids.insert(bid.to_string());
            if block.get("type").and_then(Value::as_str) == Some("text") {
                if let Some(text) = block.get("text").and_then(Value::as_str) {
                    self.text_acc.insert(bid.to_string(), text.to_string());
                }
            }
        }
    }

    /// Map one tailed `agent:stream:*` event to a chat block delta (or `None` for
    /// an unrelated/malformed event). `chunk`/`tool:call` build live block upserts
    /// from the payload; `stream:end` reconciles against the persisted message and
    /// resets the per-turn state.
    pub(crate) async fn delta(&mut self, api: &dyn WorkspaceApi, event: &Event) -> Option<Value> {
        match event.event_type.as_str() {
            AGENT_STREAM_CHUNK => self.chunk_delta(event),
            AGENT_TOOL_CALL => self.tool_delta(event),
            AGENT_STREAM_END => self.finalize(api).await,
            _ => None,
        }
    }

    /// Map an `agent:stream:chunk`: accumulate the chunk and emit the full block
    /// (D2/D4). Text chunks coalesce onto one `blockId` (`updated` on growth);
    /// non-text chunks pass through as the full block (`added`), stamped with the
    /// same id the persisted block carries.
    fn chunk_delta(&mut self, event: &Event) -> Option<Value> {
        let d = &event.data;
        let block_id = d.get("blockId").and_then(Value::as_str)?.to_string();
        let message_id = d.get("messageId").and_then(Value::as_str)?.to_string();
        self.message_id = Some(message_id.clone());
        let block_type = d.get("blockType").and_then(Value::as_str).unwrap_or("text");
        let content = d.get("content")?;
        let block = if block_type == "text" {
            let chunk = content.as_str().unwrap_or_default();
            let acc = self.text_acc.entry(block_id.clone()).or_default();
            acc.push_str(chunk);
            json!({ "type": "text", "id": block_id, "text": acc.clone() })
        } else {
            let mut obj = content.as_object()?.clone();
            obj.insert("id".to_string(), Value::String(block_id.clone()));
            Value::Object(obj)
        };
        let added = self.note_block(&block_id);
        let entity = self.entity(&message_id, block, None, None, false);
        Some(single_delta(added, entity))
    }

    /// Map an `agent:tool:call`: synthesize a `tool_use` block matching the
    /// persisted `record_tool` shape (D6). Once the call completes WITH
    /// output, also synthesize a `tool_result` block, and — when a `completed`
    /// (not `error`) output carries a proposal-MIME resource item (§7.1) — a
    /// standalone proposal-resource block. The `tool_result` block id is the
    /// `tool_use` index + 1 (it is appended right after) and the proposal
    /// block follows at + 2; a mispredicted id self-heals at the terminal
    /// reconcile via `removedIds`.
    fn tool_delta(&mut self, event: &Event) -> Option<Value> {
        let d = &event.data;
        let block_id = d.get("blockId").and_then(Value::as_str)?.to_string();
        let message_id = d.get("messageId").and_then(Value::as_str)?.to_string();
        self.message_id = Some(message_id.clone());
        let tool_call_id = d.get("toolCallId").and_then(Value::as_str)?.to_string();
        let status = d.get("status").and_then(Value::as_str).unwrap_or("started");
        // Synthesize the `tool_use` block via the shared factory so the live
        // delta stays byte-identical to the persisted block that `record_tool`
        // writes on `agent:stream:end` — the invariant chat.subscribe relies
        // on for its terminal reconcile.
        let use_block = intent_services::tool_block::build_tool_use_block(
            &block_id,
            d.get("toolName").and_then(Value::as_str).unwrap_or(""),
            d.get("title").and_then(Value::as_str).unwrap_or(""),
            d.get("input").cloned().unwrap_or(Value::Null),
            &tool_call_id,
            d.get("toolKind").and_then(Value::as_str).unwrap_or(""),
            status,
        );
        let use_added = self.note_block(&block_id);
        let mut added = Vec::new();
        let mut updated = Vec::new();
        push_entity(
            &mut added,
            &mut updated,
            use_added,
            self.entity(&message_id, use_block, None, None, false),
        );
        let completed = status == "completed" || status == "error";
        if completed {
            if let (Some(output), Some(result_id)) = (d.get("output"), next_block_id(&block_id)) {
                let result_block = json!({
                    "type": "tool_result",
                    "id": result_id,
                    "tool_use_id": tool_call_id,
                    "output": output,
                    "is_error": status == "error",
                });
                let res_added = self.note_block(&result_id);
                push_entity(
                    &mut added,
                    &mut updated,
                    res_added,
                    self.entity(&message_id, result_block, None, None, false),
                );
                // §7.1: the same standalone resource block the persisted
                // transcript appends right after the `tool_result`. The
                // registry-claimed canonical item carried on the event
                // (`registeredAttachment`, deterministic attach) wins;
                // otherwise fall back to lifting a proposal-MIME resource
                // item out of the echoed output. Gated on `completed` only
                // (matching `record_tool`) — an errored tool must not surface
                // an actionable ProposalCard.
                if status == "completed" {
                    if let Some(item) = d
                        .get("registeredAttachment")
                        .cloned()
                        .or_else(|| intent_services::tool_block::lift_proposal_resource(output))
                    {
                        if let Some(proposal_id) = next_block_id(&result_id) {
                            let proposal_block =
                                intent_services::tool_block::build_proposal_resource_block(
                                    &proposal_id,
                                    &item,
                                );
                            let prop_added = self.note_block(&proposal_id);
                            push_entity(
                                &mut added,
                                &mut updated,
                                prop_added,
                                self.entity(&message_id, proposal_block, None, None, false),
                            );
                        }
                    }
                }
            }
        }
        Some(json!({ "added": added, "updated": updated, "removedIds": [] }))
    }

    /// Finalize the turn on `agent:stream:end`: reconcile against the persisted
    /// message, then reset the per-turn accumulation so the next turn on this
    /// subscription starts clean.
    async fn finalize(&mut self, api: &dyn WorkspaceApi) -> Option<Value> {
        let delta = self.reconcile(api).await;
        self.text_acc.clear();
        self.seen_ids.clear();
        self.emitted_ids.clear();
        self.message_id = None;
        delta
    }

    /// Re-read the now-persisted assistant message and emit a terminal delta that
    /// drives the client's accumulated state to exactly the fresh snapshot: every
    /// persisted block as `updated` (or `added` if never emitted live) carrying
    /// the authoritative `messageSeq`/`timestamp` and `streamingComplete:true`,
    /// plus `removedIds` for any block emitted live that the persisted message
    /// does not contain (e.g. a mispredicted `tool_result` index).
    async fn reconcile(&self, api: &dyn WorkspaceApi) -> Option<Value> {
        let message_id = self.message_id.clone()?;
        let conv = api
            .agent_get_conversation(AgentId::from(self.agent_id.as_str()), None, None, None)
            .await
            .ok()?;
        let messages = conv.get("messages").and_then(Value::as_array)?;
        let mut added = Vec::new();
        let mut updated = Vec::new();
        let mut persisted_ids: HashSet<String> = HashSet::new();
        if let Some(msg) = messages
            .iter()
            .find(|m| m.get("id").and_then(Value::as_str) == Some(message_id.as_str()))
        {
            let seq = msg.get("seq").and_then(Value::as_u64);
            let ts = msg.get("timestamp").and_then(Value::as_str);
            if let Some(blocks) = msg.get("contentBlocks").and_then(Value::as_array) {
                for block in blocks {
                    let Some(bid) = block.get("id").and_then(Value::as_str) else {
                        continue;
                    };
                    persisted_ids.insert(bid.to_string());
                    let is_added = !self.seen_ids.contains(bid);
                    let entity = self.entity(&message_id, block.clone(), seq, ts, true);
                    push_entity(&mut added, &mut updated, is_added, entity);
                }
            }
        }
        let mut removed: Vec<String> = self
            .emitted_ids
            .iter()
            .filter(|id| !persisted_ids.contains(*id))
            .cloned()
            .collect();
        removed.sort();
        Some(json!({ "added": added, "updated": updated, "removedIds": removed }))
    }

    /// Wrap one upserted block as a delta entity: the message pointer plus the
    /// FULL block (D2). `messageSeq`/`timestamp`/`streamingComplete` are carried
    /// only on the authoritative terminal frame (mid-turn they are not yet known).
    fn entity(
        &self,
        message_id: &str,
        block: Value,
        seq: Option<u64>,
        timestamp: Option<&str>,
        streaming_complete: bool,
    ) -> Value {
        let mut e = Map::new();
        e.insert("agentId".to_string(), Value::String(self.agent_id.clone()));
        e.insert(
            "messageId".to_string(),
            Value::String(message_id.to_string()),
        );
        e.insert("role".to_string(), Value::String("assistant".to_string()));
        if let Some(seq) = seq {
            e.insert("messageSeq".to_string(), json!(seq));
        }
        if let Some(ts) = timestamp {
            e.insert("timestamp".to_string(), Value::String(ts.to_string()));
        }
        if streaming_complete {
            e.insert("streamingComplete".to_string(), Value::Bool(true));
        }
        e.insert("block".to_string(), block);
        Value::Object(e)
    }

    /// Record a freshly built block id as seen + emitted; returns `true` when it
    /// is the block's FIRST sighting this turn (→ `added`, else `updated`).
    fn note_block(&mut self, block_id: &str) -> bool {
        self.emitted_ids.insert(block_id.to_string());
        self.seen_ids.insert(block_id.to_string())
    }
}

/// Build a single-entity delta envelope, routing the entity to `added` (first
/// sighting) or `updated` (a known block grown/changed).
fn single_delta(added: bool, entity: Value) -> Value {
    if added {
        json!({ "added": [entity], "updated": [], "removedIds": [] })
    } else {
        json!({ "added": [], "updated": [entity], "removedIds": [] })
    }
}

/// Route an entity into the `added` or `updated` bucket by first-sighting flag.
fn push_entity(added: &mut Vec<Value>, updated: &mut Vec<Value>, is_added: bool, entity: Value) {
    if is_added {
        added.push(entity);
    } else {
        updated.push(entity);
    }
}

/// `{prefix}:{n}` → `{prefix}:{n+1}` — the `tool_result` block id follows its
/// `tool_use` block by one index in the persisted transcript (`record_tool`).
fn next_block_id(block_id: &str) -> Option<String> {
    let (prefix, idx) = block_id.rsplit_once(':')?;
    let n: usize = idx.parse().ok()?;
    Some(format!("{prefix}:{}", n + 1))
}

/// Map one bus change event to a channel delta via the re-read strategy (TB-0
/// §2.2 option B). Dispatches to the per-channel mapper; returns `None` for an
/// event that does not translate (unrelated type, missing id, or re-read miss).
pub(crate) async fn channel_delta(
    api: &dyn WorkspaceApi,
    channel: Channel,
    workspace_id: &WorkspaceId,
    note_id: Option<&NoteId>,
    event: &Event,
) -> Option<Value> {
    match channel {
        Channel::Note => note_delta(api, workspace_id, event).await,
        Channel::Task => task_delta(api, workspace_id, event).await,
        Channel::Agent => agent_delta(api, event).await,
        Channel::Workspace => workspace_delta(api, event).await,
        Channel::Comment => comment_delta(api, workspace_id, note_id?, event).await,
        // The chat channel uses the dedicated, stateful [`ChatDeltaState`] mapper
        // on the `forward_chat_subscription` path (CS-3) — its deltas are
        // event-payload-driven, not re-read — so this generic re-read arm is
        // unreachable for `Chat`.
        Channel::Chat => None,
    }
}

/// Map a `task` channel event by re-reading the note and keeping only task
/// notes. `note:created` → `added`, `note:updated`/`task:status-changed` →
/// `updated` when the note still projects a task or `removedIds` when the note
/// has been demoted (task block removed), `note:deleted` → `removedIds` (the
/// delete is idempotent on the client even when the removed note was not a
/// task).
pub(crate) async fn task_delta(
    api: &dyn WorkspaceApi,
    workspace_id: &WorkspaceId,
    event: &Event,
) -> Option<Value> {
    let note_id = event.data.get("noteId").and_then(Value::as_str)?;
    match event.event_type.as_str() {
        NOTE_DELETED => Some(json!({ "removedIds": [note_id] })),
        NOTE_CREATED => {
            let note = api
                .get_note(workspace_id.clone(), NoteId::from(note_id))
                .await
                .ok()?;
            note.metadata.task.as_ref()?;
            Some(json!({ "added": [serde_json::to_value(note).ok()?] }))
        }
        NOTE_UPDATED | TASK_STATUS_CHANGED => {
            let note = api
                .get_note(workspace_id.clone(), NoteId::from(note_id))
                .await
                .ok()?;
            if note.metadata.task.is_none() {
                return Some(json!({ "removedIds": [note_id] }));
            }
            Some(json!({ "updated": [serde_json::to_value(note).ok()?] }))
        }
        _ => None,
    }
}

/// Map an `agent` channel lifecycle event by re-reading the [`AgentLite`].
/// `agent:created` → `added`, `agent:deleted` → `removedIds`, every other
/// lifecycle/status event → `updated`. The agent id is read from `data.agentId`
/// (falling back to the agent-scoped `sessionId`).
pub(crate) async fn agent_delta(api: &dyn WorkspaceApi, event: &Event) -> Option<Value> {
    let agent_id = event
        .data
        .get("agentId")
        .and_then(Value::as_str)
        .or(event.session_id.as_deref())?;
    match event.event_type.as_str() {
        AGENT_DELETED => Some(json!({ "removedIds": [agent_id] })),
        AGENT_CREATED => {
            let agent = api.agent_get(AgentId::from(agent_id), None).await.ok()?;
            Some(json!({ "added": [serde_json::to_value(agent).ok()?] }))
        }
        AGENT_STARTED | AGENT_COMPLETED | AGENT_FAILED | AGENT_IDLE | AGENT_STATUS_CHANGED
        | AGENT_RENAMED | AGENT_UPDATED | AGENT_RESTORED => {
            let agent = api.agent_get(AgentId::from(agent_id), None).await.ok()?;
            Some(json!({ "updated": [serde_json::to_value(agent).ok()?] }))
        }
        _ => None,
    }
}

/// Map a `workspace` channel event by re-reading the [`Workspace`]. The channel
/// is global, so the id comes from `data.workspaceId` (falling back to the
/// event's `workspaceId`). `workspace:created` → `added`, `workspace:deleted` →
/// `removedIds`, every other status/PR event → `updated`.
pub(crate) async fn workspace_delta(api: &dyn WorkspaceApi, event: &Event) -> Option<Value> {
    let workspace_id = event
        .data
        .get("workspaceId")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| event.workspace_id.as_str().to_string());
    match event.event_type.as_str() {
        WORKSPACE_DELETED => Some(json!({ "removedIds": [workspace_id] })),
        WORKSPACE_CREATED => {
            let ws = api
                .get_workspace(WorkspaceId::from(workspace_id))
                .await
                .ok()?;
            Some(json!({ "added": [serde_json::to_value(ws).ok()?] }))
        }
        WORKSPACE_UPDATED
        | WORKSPACE_ACTIVITY_CHANGED
        | WORKSPACE_ATTENTION_CHANGED
        | PR_LINKED
        | PR_UPDATED
        | PR_UNLINKED => {
            let ws = api
                .get_workspace(WorkspaceId::from(workspace_id))
                .await
                .ok()?;
            Some(json!({ "updated": [serde_json::to_value(ws).ok()?] }))
        }
        _ => None,
    }
}

/// Map a `comment` channel event by re-reading the affected thread summary. A
/// `comment:added` for the subscribed note re-lists the threads (with their
/// comments) and emits the thread carrying the new comment as `updated` (a new
/// thread upserts by `threadId`; the client merges idempotently). Events for a
/// different note are ignored.
pub(crate) async fn comment_delta(
    api: &dyn WorkspaceApi,
    workspace_id: &WorkspaceId,
    note_id: &NoteId,
    event: &Event,
) -> Option<Value> {
    if event.event_type != COMMENT_ADDED {
        return None;
    }
    let event_note = event.data.get("noteId").and_then(Value::as_str)?;
    if event_note != note_id.as_str() {
        return None;
    }
    let comment_id = event.data.get("commentId").and_then(Value::as_str)?;
    let result = api
        .comment_list(
            workspace_id.clone(),
            note_id.clone(),
            None,
            None,
            None,
            true,
        )
        .await
        .ok()?;
    let thread = result.threads.into_iter().find(|t| {
        t.comments
            .as_ref()
            .is_some_and(|cs| cs.iter().any(|c| c.id == comment_id))
    })?;
    Some(json!({ "updated": [serde_json::to_value(thread).ok()?] }))
}

#[cfg(test)]
mod tests;
