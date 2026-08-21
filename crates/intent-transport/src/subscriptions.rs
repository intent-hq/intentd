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
    AGENT_COMPLETED, AGENT_CREATED, AGENT_DELETED, AGENT_FAILED, AGENT_IDLE, AGENT_MESSAGE,
    AGENT_RENAMED, AGENT_RESTORED, AGENT_STARTED, AGENT_STATUS_CHANGED, AGENT_STREAM_END,
    AGENT_TOOL_CALL, AGENT_UPDATED, CHAT_STREAM_DELTA, COMMENT_ADDED, NOTE_CREATED, NOTE_DELETED,
    NOTE_UPDATED, PR_LINKED, PR_UNLINKED, PR_UPDATED, TASK_STATUS_CHANGED,
    WORKSPACE_ACTIVITY_CHANGED, WORKSPACE_ATTENTION_CHANGED, WORKSPACE_CREATED, WORKSPACE_DELETED,
    WORKSPACE_DISPLAY_STATUS_CHANGED, WORKSPACE_UPDATED, WORKSPACE_WAITING_CHANGED,
};
use intent_core::{
    extract_spec_task_ids, now_iso, AgentId, ConversationProjection, Event, Note, NoteId,
    WorkspaceApi, WorkspaceId, SLIM_PAGE_BUDGET_BYTES,
};
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
/// `replaceGroup` is optional. `sinceMessageId` (optional) requests a resumed
/// seq-0 snapshot: only messages AFTER that id (§7.1 resume). `deltaEncoding`
/// (optional) selects the text-chunk delta encoding for the subscription's
/// lifetime (D2): `"full"`/absent for full accumulated text per chunk,
/// `"incremental"` for append-only `textDelta` fragments. `projection`
/// (optional) selects the slim conversation projection for the
/// subscription's lifetime: the seq-0 snapshot (including its live-turn
/// merge), lag-recovery snapshots, every delta re-read, AND the live
/// `tool_use`/`tool_result` deltas serve bounded tool/image block bodies
/// (§5.5) — no frame class is exempt.
#[derive(Debug)]
pub(crate) struct ChatSubscribeParams {
    pub agent_id: String,
    pub since_message_id: Option<String>,
    pub delta_encoding: DeltaEncoding,
    pub projection: Option<ConversationProjection>,
    pub replace_group: Option<String>,
}

/// The text-chunk delta encoding for one chat subscription (CS-0 D2). Fixed at
/// subscribe time; every snapshot the subscription emits echoes the active
/// mode when it is not the default (`stamp_delta_encoding`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum DeltaEncoding {
    /// Each text/thinking chunk delta carries the FULL accumulated `text`
    /// (the pre-#2675 wire shape; the newest delta supersedes).
    #[default]
    Full,
    /// Each text/thinking chunk delta carries only the new fragment as
    /// `textDelta`; the client appends. Eliminates the quadratic wire
    /// amplification of full-text re-sends (monorepo#2675).
    Incremental,
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
/// error (the chat channel is per-agent, CS-0). `sinceMessageId` is optional:
/// absent / `null` / empty string all mean "no resume" (standard snapshot,
/// no `resumed` key); a present non-string value is a `-32602` error.
/// `deltaEncoding` is optional: absent / `null` / `"full"` select the default
/// full-text encoding, `"incremental"` selects append-only text deltas; any
/// other value is a `-32602` error (a silently ignored typo would leave the
/// client appending fragments the daemon never sends as fragments).
pub(crate) fn parse_chat_subscribe_params(
    params: &Map<String, Value>,
) -> Result<ChatSubscribeParams, String> {
    let agent_id = match params.get("agentId").and_then(Value::as_str) {
        Some(s) if !s.is_empty() => s.to_string(),
        _ => return Err("agentId is required".to_string()),
    };
    let since_message_id = match params.get("sinceMessageId") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) if s.is_empty() => None,
        Some(Value::String(s)) => Some(s.clone()),
        Some(_) => return Err("sinceMessageId must be a string".to_string()),
    };
    let delta_encoding = match params.get("deltaEncoding") {
        None | Some(Value::Null) => DeltaEncoding::Full,
        Some(Value::String(s)) if s == "full" => DeltaEncoding::Full,
        Some(Value::String(s)) if s == "incremental" => DeltaEncoding::Incremental,
        Some(_) => return Err("deltaEncoding must be \"full\" or \"incremental\"".to_string()),
    };
    // `projection` is optional: absent / `null` serve full fidelity, `"slim"`
    // bounds tool/image block bodies (§5.5); any other value is `-32602` (a
    // silently ignored typo would hand the client the full-size frames it
    // opted out of).
    let projection = match params.get("projection") {
        None | Some(Value::Null) => None,
        Some(Value::String(s)) if s == "slim" => Some(ConversationProjection::Slim),
        Some(_) => return Err("projection must be \"slim\"".to_string()),
    };
    Ok(ChatSubscribeParams {
        agent_id,
        since_message_id,
        delta_encoding,
        projection,
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

/// Echo a non-default delta encoding on a chat snapshot (CS-0 D2): every
/// snapshot an incremental subscription emits (seq-0 and lag recovery) carries
/// `deltaEncoding: "incremental"`, so the client can assert the daemon honored
/// the requested mode before appending fragments. Full mode stamps nothing —
/// default subscriptions stay byte-identical to the pre-#2675 wire shape.
pub(crate) fn stamp_delta_encoding(snapshot: &mut Value, encoding: DeltaEncoding) {
    if encoding == DeltaEncoding::Incremental {
        if let Some(obj) = snapshot.as_object_mut() {
            obj.insert("deltaEncoding".to_string(), json!("incremental"));
        }
    }
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
            WORKSPACE_DISPLAY_STATUS_CHANGED,
            WORKSPACE_WAITING_CHANGED,
            PR_LINKED,
            PR_UPDATED,
            PR_UNLINKED,
        ],
        Channel::Comment => &[COMMENT_ADDED],
        // The chat channel is the one consumer of the content-bearing
        // `chat:stream:delta` firehose (CS-0); the forwarder additionally
        // filters these to one agent by `sessionId == agentId`.
        // `agent:message` is tailed for NON-assistant rows (user/system/tool
        // persists — queue drains, direct sends, model switches) so
        // subscribers render them live without a refetch; assistant echoes
        // map to `None` (the stream + terminal reconcile owns assistant
        // content).
        Channel::Chat => &[
            CHAT_STREAM_DELTA,
            AGENT_TOOL_CALL,
            AGENT_STREAM_END,
            AGENT_MESSAGE,
        ],
    };
    types.iter().map(std::string::ToString::to_string).collect()
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
        // Task snapshot rows are note-shaped entities enriched with the
        // additive `specLinked` flag (same semantics as `task.list`, §5.4).
        // The dedicated [`task_snapshot`] also hands the forwarder the link
        // set it needs to seed the stateful task-delta mapper.
        Channel::Task => task_snapshot(api, workspace_id).await.0,
        Channel::Agent => match api.agent_list(workspace_id.clone()).await {
            Ok(agents) => serde_json::to_value(agents).unwrap_or_else(|_| empty()),
            Err(_) => empty(),
        },
        // Archived workspaces are included so the snapshot agrees with the
        // deltas (which upsert archived workspaces as `updated`); clients
        // filter by `status` (intent-hq/monorepo#775).
        // Lite list: cheap status aggregates (taskStats/displayStatus/
        // cowSupported) only — no notes/sessions enrichment. Full list was
        // multi-MB.
        Channel::Workspace => match api.list_workspaces_lite(true).await {
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
/// existing paginated read, then — when the live-turn slot holds a message not
/// yet in that page — its partial assistant message merged in (CS-0 D5) so a
/// `chat.subscribe` arriving mid-turn reconstructs a coherent in-flight message.
///
/// **Bounded** (monorepo#958): exactly ONE conversation read, with no
/// `nextToken` follow-up and the omitted `limit` resolving to the
/// server-clamped default page, so the snapshot fetches/decodes only its
/// bounded newest page regardless of transcript length — the paginated op
/// selects just that page SQL-side and never re-hydrates the full history.
/// Older pages stay client-pulled via `agent.getConversation { nextToken }`.
/// [`WorkspaceApi::agent_is_busy`] decides the merged message's `isStreaming`
/// flag, NOT whether to merge at all (monorepo#2104): a populated
/// `agent_live_turn` slot whose worker is gone (mid-turn crash, a flush that
/// failed with a non-UNIQUE store error and kept the slot as the only copy of
/// the content) still holds real output the user watched arrive, so it is
/// served — just never as "streaming". The invariant that survives is the one
/// that mattered: an orphaned slot MUST NOT surface a phantom *streaming*
/// message. An EMPTY orphan slot is skipped entirely, so a dead turn that
/// streamed nothing adds no blank row. On a read error the snapshot degrades to
/// an empty messages page rather than failing the subscription (matching
/// [`channel_snapshot`]'s degrade-to-empty pattern).
///
/// **Resume (§7.1).** When `since_message_id` is provided, the same bounded
/// newest page is read (still exactly ONE read — the resume is a post-filter,
/// never a second fetch), then [`apply_resume_filter`] trims it: if the id is
/// found in the page the snapshot carries only the messages AFTER it with
/// `resumed: true`; otherwise (unknown / pruned / older than the bounded page)
/// the full standard page is served with `resumed: false` so the client
/// rehydrates. The live-turn merge and activity-flags overlay apply in both
/// cases, after the filter, so an in-flight partial is never trimmed away.
pub(crate) async fn chat_snapshot(
    api: &dyn WorkspaceApi,
    agent_id: &AgentId,
    since_message_id: Option<&str>,
    projection: Option<ConversationProjection>,
) -> Value {
    let mut snapshot = match api
        .agent_get_conversation(agent_id.clone(), None, None, None, None, None, projection)
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
    if let Some(since) = since_message_id {
        apply_resume_filter(&mut snapshot, since);
    }
    overlay_live_state(api, agent_id, &mut snapshot, projection).await;
    snapshot
}

/// [`chat_snapshot`] for the lag self-heal path — fallible instead of
/// degrade-to-empty. A recovery snapshot is emitted at a LATER `seq` than
/// content the client already rendered, so the seq-0 degrade contract is
/// inverted here: serving `{ messages: [] }` would make the client rebuild an
/// already-rendered transcript as EMPTY on a transient read failure, strictly
/// worse than the lag it was healing. The read is retried once (same policy as
/// the terminal reconcile's re-read); a persistent failure returns `None` and
/// the caller keeps the recovery pending instead of emitting anything. Still
/// bounded: at most two page reads per attempt, no resume filter (recovery
/// always serves the full standard page).
pub(crate) async fn chat_recovery_snapshot(
    api: &dyn WorkspaceApi,
    agent_id: &AgentId,
    projection: Option<ConversationProjection>,
) -> Option<Value> {
    let read =
        || api.agent_get_conversation(agent_id.clone(), None, None, None, None, None, projection);
    let mut snapshot = match read().await {
        Ok(v) => v,
        Err(_) => read().await.ok()?,
    };
    overlay_live_state(api, agent_id, &mut snapshot, projection).await;
    Some(snapshot)
}

/// Overlay the live in-flight turn and activity flags onto a chat snapshot's
/// persisted page (shared by [`chat_snapshot`] and [`chat_recovery_snapshot`]).
/// The in-flight message's blocks are bounded under the subscription's
/// `projection` (the persisted page already arrived bounded from the read) —
/// without this, a slim subscriber connecting mid-turn could receive a seq-0
/// snapshot carrying a multi-MB live `tool_result`/`image` block, the exact
/// frame class the projection exists to prevent.
async fn overlay_live_state(
    api: &dyn WorkspaceApi,
    agent_id: &AgentId,
    snapshot: &mut Value,
    projection: Option<ConversationProjection>,
) {
    // Read busy BEFORE the slot, never after. The two reads are separate lock
    // acquisitions, so a turn can claim `busy` between them; `try_begin` clears a
    // stale slot under the busy lock before publishing the claim, which makes
    // `busy == true` ⇒ "the stale slot is already gone" — but only for a reader
    // in this order. Reversed, a snapshot could read the PREVIOUS turn's content
    // and then a freshly-claimed `busy`, and label stale content `isStreaming:
    // true` — the phantom-streaming state this whole gate exists to prevent. In
    // this order the residual interleaving is the harmless mirror: the new turn's
    // content labelled settled, which the next delta or snapshot corrects.
    let is_streaming = api.agent_is_busy(agent_id.clone());
    if let Some(live) = api.agent_live_turn(agent_id.clone()) {
        merge_live_turn(snapshot, agent_id, &live, is_streaming, projection);
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
}

/// Trim a chat seq-0 snapshot to the messages AFTER `since` (§7.1 resume).
///
/// If `since` matches a message id in the bounded page, `messages` keeps only
/// the rows after it and the snapshot gains `resumed: true`; `truncated` /
/// `nextToken` are cleared (no gap exists — the client already holds
/// everything up to `since`). If `since` is not in the page (unknown, pruned,
/// or older than the bounded newest page — indistinguishable without a second
/// read, and the bounded-read contract forbids one), the page is left intact
/// and the snapshot gains `resumed: false`: the client must discard its cache
/// and rehydrate from the standard snapshot. `totalMessages` stays the
/// transcript-wide count in both cases (same semantics as the standard
/// snapshot, where `messages.len()` already ≠ `totalMessages` when truncated).
fn apply_resume_filter(snapshot: &mut Value, since: &str) {
    let Some(obj) = snapshot.as_object_mut() else {
        return;
    };
    let found_at = obj
        .get("messages")
        .and_then(Value::as_array)
        .and_then(|arr| {
            arr.iter()
                .position(|m| m.get("id").and_then(Value::as_str) == Some(since))
        });
    match found_at {
        Some(idx) => {
            if let Some(arr) = obj.get_mut("messages").and_then(Value::as_array_mut) {
                arr.drain(..=idx);
            }
            obj.insert("resumed".to_string(), Value::Bool(true));
            obj.insert("truncated".to_string(), Value::Bool(false));
            obj.insert("nextToken".to_string(), Value::Null);
        }
        None => {
            obj.insert("resumed".to_string(), Value::Bool(false));
        }
    }
}

/// Append the live turn's assistant message to a chat snapshot's `messages` page
/// (CS-0 D5). Its `seq` is the next monotonic value (`totalMessages`, since seq
/// is contiguous from 0) and it carries `isStreaming: is_streaming` — `true`
/// while a worker still holds the turn's busy claim, as a render hint the
/// terminal reconcile clears via `streamingComplete`; `false` for an orphaned
/// slot (monorepo#2104), whose content is real but final. Idempotent: if the
/// turn's message already persisted (id present in the page) it is left
/// untouched, so a snapshot taken in the window between persist and slot-clear
/// never duplicates.
///
/// An orphaned slot with NO content blocks is skipped: there is nothing to show,
/// and a dead turn that streamed nothing must not add a blank assistant row. A
/// still-streaming turn keeps merging empty — a turn that has only just begun
/// legitimately has no blocks yet, and the client needs the id to reconcile
/// against.
///
/// **Slim page budget (§5.5).** Under `projection: "slim"` the merged page is
/// re-budgeted after the append: the persisted page arrived within
/// [`SLIM_PAGE_BUDGET_BYTES`], but `slim_message_blocks` caps block *bodies*,
/// not block *count*, so a streaming turn with hundreds of capped blocks can
/// weigh MBs on its own — added beside an at-budget persisted page, the seq-0
/// frame would blow the budget the read path just enforced. The live turn is
/// the newest row and the page's anchor (always kept, even alone over
/// budget); oldest persisted rows are evicted until the page fits, with
/// `truncated`/`nextToken` re-minted at the first evicted row (row `seq` is
/// contiguous from 0, so a row's seq IS its global oldest-indexed position)
/// so the client pulls the evicted rows via `agent.getConversation` exactly
/// like any budget-trimmed page. Full (absent-projection) merges are never
/// budgeted, mirroring the read path.
fn merge_live_turn(
    snapshot: &mut Value,
    agent_id: &AgentId,
    live: &Value,
    is_streaming: bool,
    projection: Option<ConversationProjection>,
) {
    let Some(message_id) = live.get("messageId").and_then(Value::as_str) else {
        return;
    };
    let mut blocks = live
        .get("contentBlocks")
        .cloned()
        .unwrap_or_else(|| json!([]));
    // Bound the in-flight blocks under the slim projection (§5.5). No
    // write-time thumbnail exists mid-turn, so an oversized live image
    // degrades to data-omitted + flags; the terminal reconcile re-reads the
    // persisted row and serves the real thumbnail.
    if projection == Some(ConversationProjection::Slim) {
        if let Some(arr) = blocks.as_array_mut() {
            intent_services::tool_block::slim_message_blocks(arr, None);
        }
    }
    let populated = blocks.as_array().is_some_and(|b| !b.is_empty());
    if !is_streaming && !populated {
        return;
    }
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
        "isStreaming": is_streaming,
    }));
    obj.insert("totalMessages".to_string(), json!(total + 1));
    if projection == Some(ConversationProjection::Slim) {
        rebudget_merged_page(obj);
    }
}

/// Re-apply the §5.5 slim page budget to a chat snapshot's `messages` page
/// after the live-turn append (see [`merge_live_turn`]'s budget note). The
/// newest row — the just-appended live turn — is the anchor and always
/// serves; oldest rows are evicted until the page fits
/// [`SLIM_PAGE_BUDGET_BYTES`], with `truncated`/`nextToken` re-minted at the
/// oldest kept row's global position (its `seq`, contiguous from 0) so the
/// evicted rows stay reachable via `agent.getConversation { nextToken }`
/// with no gaps or duplicates. Sizes are counted through the same discarding
/// writer as the read path ([`intent_services::pagination::serialized_size`]),
/// so both sides of the budget agree on what a row weighs. No-op when the
/// merged page already fits — the common case, since the persisted page
/// arrived within budget and a typical live turn is small.
fn rebudget_merged_page(obj: &mut Map<String, Value>) {
    let Some(arr) = obj.get_mut("messages").and_then(Value::as_array_mut) else {
        return;
    };
    let sizes: Vec<usize> = arr
        .iter()
        .map(intent_services::pagination::serialized_size)
        .collect();
    let (lo, _hi) = intent_services::pagination::budget_page(
        &sizes,
        intent_services::pagination::BudgetAnchor::Newest,
        SLIM_PAGE_BUDGET_BYTES,
    );
    if lo == 0 {
        return;
    }
    let boundary = arr[lo].get("seq").and_then(Value::as_u64);
    arr.drain(..lo);
    if let Some(b) = boundary {
        obj.insert("truncated".to_string(), Value::Bool(true));
        let token = intent_services::pagination::remint_backward_token(
            usize::try_from(b).expect("value fits in usize"),
        )
        .map_or(Value::Null, Value::String);
        obj.insert("nextToken".to_string(), token);
    }
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
    /// The subscription's text-chunk encoding (D2): full accumulated text per
    /// chunk, or append-only `textDelta` fragments (monorepo#2675).
    encoding: DeltaEncoding,
    /// The subscription's conversation projection (§5.5), fixed at subscribe
    /// time like the encoding. Every delta re-read (`agent:message` rows,
    /// terminal reconcile) passes it through so the accumulated client state
    /// stays byte-identical to a fresh snapshot under the SAME projection
    /// (the CS-3 reconciliation invariant).
    projection: Option<ConversationProjection>,
    /// `blockId` → accumulated text for `text` blocks. In full mode every
    /// chunk delta re-materializes it (D2); in incremental mode it is kept
    /// (O(chunk) appends, linear memory) but consulted only by the degraded
    /// best-effort terminal frame, which must carry authoritative full text
    /// in both modes — a fragment there would clobber the client's
    /// accumulation.
    text_acc: HashMap<String, String>,
    /// `blockId`s emitted at least once this turn (added vs updated discriminator).
    seen_ids: HashSet<String>,
    /// `blockId`s emitted live this turn (to compute orphan `removedIds` at end).
    emitted_ids: HashSet<String>,
    /// The in-flight assistant message id (block-id prefix), learned from events.
    message_id: Option<String>,
    /// Last-known block per live block id, in first-sighting order — the
    /// content of the best-effort terminal frame when the terminal reconcile's
    /// re-read fails (the client must always receive a terminal frame).
    /// Text/thinking entries are `{type, id}` markers rebuilt from
    /// [`Self::text_acc`] at emit time; other blocks keep their full JSON.
    live_blocks: Vec<(String, Value)>,
}

impl ChatDeltaState {
    /// A fresh mapper for one chat subscription (one agent). The same instance is
    /// reused across turns; `finalize` resets the per-turn accumulation. The
    /// encoding is fixed for the subscription's lifetime (chosen at subscribe
    /// time) — a lag-recovery replacement mapper must be built with the SAME
    /// encoding.
    pub(crate) fn new(
        agent_id: &AgentId,
        encoding: DeltaEncoding,
        projection: Option<ConversationProjection>,
    ) -> Self {
        Self {
            agent_id: agent_id.as_str().to_string(),
            encoding,
            projection,
            text_acc: HashMap::new(),
            seen_ids: HashSet::new(),
            emitted_ids: HashSet::new(),
            message_id: None,
            live_blocks: Vec::new(),
        }
    }

    /// Seed the per-turn accumulation from a seq-0 snapshot that carries an
    /// in-flight assistant message (CS-0 D5): the message arriving mid-turn has
    /// already streamed some blocks, so prime `message_id`, mark each block id as
    /// already seen+emitted (subsequent chunks arrive as `updated`, not `added`),
    /// and pre-load each `text` block's accumulated text. In full mode the NEXT
    /// chunk delta then carries the FULL text (D2), not just the new fragment —
    /// without this, a resuming subscriber's first chunk would restart the text
    /// from empty until the terminal reconcile. In incremental mode
    /// (monorepo#2675) deltas carry only the post-snapshot fragment, so the
    /// pre-load doesn't shape the wire — but the accumulation still backs the
    /// DEGRADED terminal frame's best-effort full text, so seeding is identical
    /// in both modes. No-op when the snapshot has no in-flight message.
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
        let mut seeded = false;
        if let Some(blocks) = msg.get("contentBlocks").and_then(Value::as_array) {
            for block in blocks {
                let Some(bid) = block.get("id").and_then(Value::as_str) else {
                    continue;
                };
                self.seen_ids.insert(bid.to_string());
                self.emitted_ids.insert(bid.to_string());
                match block.get("type").and_then(Value::as_str) {
                    Some(t) if t == "text" || t == "thinking" => {
                        self.remember_text_marker(bid, t);
                        if let Some(text) = block.get("text").and_then(Value::as_str) {
                            self.text_acc.insert(bid.to_string(), text.to_string());
                        }
                    }
                    _ => self.remember_block(bid, block),
                }
                seeded = true;
            }
        }
        // A just-started streaming row with no blocks yet is still rendered
        // as streaming by the snapshot consumer. Remember a placeholder empty
        // text block under the id the first persisted block will take, so a
        // DEGRADED terminal frame still carries one `streamingComplete`
        // entity for the row — an empty frame would leave the blank row
        // permanently streaming (the delta reducer flips `isStreaming` only
        // while applying an entity). Inert on the authoritative path (which
        // never reads `live_blocks`), and upserted over by the first real
        // chunk, which takes the same `{messageId}:0` id.
        if !seeded {
            self.remember_text_marker(&format!("{message_id}:0"), "text");
        }
    }

    /// Map one tailed chat-channel event to a chat block delta (or `None` for
    /// an unrelated/malformed event). `chat:stream:delta`/`tool:call` build live
    /// block upserts from the payload; `stream:end` reconciles against the
    /// persisted message and resets the per-turn state; `agent:message` re-reads
    /// a persisted NON-assistant row and emits its blocks as authoritative
    /// entities.
    pub(crate) async fn delta(&mut self, api: &dyn WorkspaceApi, event: &Event) -> Option<Value> {
        match event.event_type.as_str() {
            CHAT_STREAM_DELTA => self.chunk_delta(event),
            AGENT_TOOL_CALL => self.tool_delta(event),
            AGENT_STREAM_END => self.finalize(api, event).await,
            AGENT_MESSAGE => self.message_row_delta(api, event).await,
            _ => None,
        }
    }

    /// Map an `agent:message` echo for a persisted NON-assistant row (user /
    /// system / tool — queue drains, direct sends, wake deliveries, model
    /// switches) to an authoritative block delta, so subscribers render the
    /// row live with no refetch. Assistant echoes map to `None`: the stream +
    /// terminal reconcile owns assistant content, and emitting here would
    /// double-deliver. The payload is intentionally lean (`{ agentId,
    /// messageId, role }`), so the row is re-read via the bounded newest
    /// `agent.getConversation` page — the same source the seq-0 snapshot and
    /// terminal reconcile use, preserving byte-parity. A re-read miss (row
    /// outside the newest page, or a read error) maps to `None`; the
    /// per-turn assistant accumulation state is not touched (a queue-drain
    /// user row lands right before the turn's first chunk).
    async fn message_row_delta(&mut self, api: &dyn WorkspaceApi, event: &Event) -> Option<Value> {
        let d = &event.data;
        let role = d.get("role").and_then(Value::as_str)?;
        if role == "assistant" {
            return None;
        }
        let message_id = d.get("messageId").and_then(Value::as_str)?.to_string();
        let conv = api
            .agent_get_conversation(
                AgentId::from(self.agent_id.as_str()),
                None,
                None,
                None,
                None,
                None,
                self.projection,
            )
            .await
            .ok()?;
        let messages = conv.get("messages").and_then(Value::as_array)?;
        let Some(msg) = messages
            .iter()
            .find(|m| m.get("id").and_then(Value::as_str) == Some(message_id.as_str()))
        else {
            tracing::debug!(
                agent = %self.agent_id,
                message = %message_id,
                "agent:message re-read miss; delta dropped"
            );
            return None;
        };
        let role = msg.get("role").and_then(Value::as_str).unwrap_or(role);
        // Defense-in-depth: the role above is re-resolved from the persisted
        // row, so a row reporting `assistant` despite a non-assistant event
        // payload must still map to `None` (the stream + terminal reconcile
        // owns assistant content).
        if role == "assistant" {
            return None;
        }
        let seq = msg.get("seq").and_then(Value::as_u64);
        let ts = msg.get("timestamp").and_then(Value::as_str);
        // Lift the persisted row's `metadata` onto each entity (additive) so
        // subscribers can render metadata-driven affordances live — e.g. the
        // sender attribution chip on an `agent_message`-tagged row — without
        // a refetch. Rows without metadata keep the lean entity shape.
        let metadata = msg.get("metadata").filter(|m| !m.is_null());
        // monorepo#1157: lift the row's top-level `appMessageId` (the
        // client-minted `userAppMessageId`, serialized by `AgentMessage` when
        // present) onto each entity so subscribers can dedup optimistic user
        // rows by id on the delta path. Omitted entirely when absent. The
        // null-guard is defensive: the real re-read never serves a null
        // `appMessageId` (`AgentMessage` skips the field when `None`), but a
        // hand-built row (tests, future callers) shouldn't leak `null`.
        let app_message_id = msg.get("appMessageId").filter(|v| !v.is_null());
        let blocks = msg.get("contentBlocks").and_then(Value::as_array)?;
        let mut added = Vec::new();
        let mut updated = Vec::new();
        for (index, block) in blocks.iter().enumerate() {
            // Defense-in-depth fallback: since monorepo#1114 the re-read
            // (`agent.getConversation`) already serves blocks stamped with the
            // stable synthetic `{messageId}:{index}` (CS-0 D1), so this
            // normally sees an id on every block; keep the stamp for rows
            // reaching here id-less so re-delivery still upserts by id
            // instead of duplicating.
            let mut block = block.clone();
            let bid = match block
                .get("id")
                .and_then(Value::as_str)
                .filter(|id| !id.is_empty())
            {
                Some(id) => id.to_string(),
                None => {
                    let id = format!("{message_id}:{index}");
                    if let Some(obj) = block.as_object_mut() {
                        obj.insert("id".to_string(), Value::String(id.clone()));
                    }
                    id
                }
            };
            let is_added = !self.seen_ids.contains(&bid);
            self.seen_ids.insert(bid.clone());
            let mut entity = self.entity_with_role(&message_id, role, block, seq, ts, true);
            if let Some(obj) = entity.as_object_mut() {
                if let Some(md) = metadata {
                    obj.insert("metadata".to_string(), md.clone());
                }
                if let Some(app_id) = app_message_id {
                    obj.insert("appMessageId".to_string(), app_id.clone());
                }
            }
            push_entity(&mut added, &mut updated, is_added, entity);
        }
        if added.is_empty() && updated.is_empty() {
            return None;
        }
        Some(json!({ "added": added, "updated": updated, "removedIds": [] }))
    }

    /// Map a `chat:stream:delta`: accumulate the chunk and emit the block
    /// (D2/D4). Text chunks — and the `thinking` chunks streamed reasoning
    /// flushes into — coalesce onto one `blockId` (`updated` on growth),
    /// carrying the FULL accumulated `text` in full mode or only the new
    /// fragment as `textDelta` in incremental mode (monorepo#2675);
    /// non-text chunks pass through as the full block (`added`), stamped with the
    /// same id the persisted block carries.
    fn chunk_delta(&mut self, event: &Event) -> Option<Value> {
        let d = &event.data;
        let block_id = d.get("blockId").and_then(Value::as_str)?.to_string();
        let message_id = d.get("messageId").and_then(Value::as_str)?.to_string();
        self.message_id = Some(message_id.clone());
        let block_type = d.get("blockType").and_then(Value::as_str).unwrap_or("text");
        let content = d.get("content")?;
        let block = if block_type == "text" || block_type == "thinking" {
            let chunk = content.as_str().unwrap_or_default();
            let acc = self.text_acc.entry(block_id.clone()).or_default();
            acc.push_str(chunk);
            let block = match self.encoding {
                DeltaEncoding::Full => {
                    json!({ "type": block_type, "id": block_id, "text": acc.clone() })
                }
                // Only the fragment travels — per-chunk wire cost is
                // O(chunk), not O(accumulated text). The accumulation above
                // still runs (O(chunk) append) so the degraded best-effort
                // terminal frame can carry authoritative full text.
                DeltaEncoding::Incremental => {
                    json!({ "type": block_type, "id": block_id, "textDelta": chunk })
                }
            };
            // A marker only — the full text lives once in `text_acc` and the
            // best-effort frame rebuilds it at emit time, so per-chunk cost
            // stays O(chunk), not O(accumulated text).
            self.remember_text_marker(&block_id, block_type);
            block
        } else {
            let mut obj = content.as_object()?.clone();
            obj.insert("id".to_string(), Value::String(block_id.clone()));
            let block = Value::Object(obj);
            self.remember_block(&block_id, &block);
            block
        };
        let added = self.note_block(&block_id);
        let entity = self.entity(&message_id, block, None, None, false);
        Some(single_delta(added, entity))
    }

    /// Map an `agent:tool:call`: synthesize a `tool_use` block matching the
    /// persisted `record_tool` shape (D6). Once the call completes WITH
    /// output, also synthesize a `tool_result` block, and — when a `completed`
    /// (not `error`) output carries a proposal-MIME resource item (§7.1) — a
    /// standalone proposal-resource block. Their ids are NOT derived from the
    /// `tool_use` id — they are read verbatim off the event (`resultBlockId` /
    /// `proposalBlockIds`), which `record_tool` stamps from the indices the
    /// blocks actually took in the durable transcript. Predicting them as
    /// `tool_use` index + 1 was wrong whenever text interleaved between a call
    /// and its completion, or a parallel call's `tool_use` already owned that
    /// index — the live block then clobbered a legitimate block on every
    /// id-keyed client until the terminal reconcile healed it
    /// (monorepo#2029). An event without the field (no such block was
    /// materialized) synthesizes nothing; a genuinely orphaned live block
    /// still self-heals at the terminal reconcile via `removedIds`.
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
        let mut use_block = intent_services::tool_block::build_tool_use_block(
            &block_id,
            d.get("toolName").and_then(Value::as_str).unwrap_or(""),
            d.get("title").and_then(Value::as_str).unwrap_or(""),
            d.get("input").cloned().unwrap_or(Value::Null),
            &tool_call_id,
            d.get("toolKind").and_then(Value::as_str).unwrap_or(""),
            status,
        );
        // A slim subscription bounds the live blocks too (§5.5) — the event
        // payload carries the FULL tool input/output, and forwarding it
        // unslimmed would hand the subscriber the very oversized frame the
        // projection opted out of, mid-turn. Same shared bounding as the
        // persisted read path, so the terminal reconcile (which re-reads
        // under the same projection) upserts byte-identical blocks.
        self.slim_block(&mut use_block);
        let use_added = self.note_block(&block_id);
        self.remember_block(&block_id, &use_block);
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
            let result_id = d
                .get("resultBlockId")
                .and_then(Value::as_str)
                .map(str::to_string);
            if let (Some(output), Some(result_id)) = (d.get("output"), result_id) {
                let mut result_block = json!({
                    "type": "tool_result",
                    "id": result_id,
                    "tool_use_id": tool_call_id,
                    "output": output,
                    "is_error": status == "error",
                });
                self.slim_block(&mut result_block);
                let res_added = self.note_block(&result_id);
                self.remember_block(&result_id, &result_block);
                push_entity(
                    &mut added,
                    &mut updated,
                    res_added,
                    self.entity(&message_id, result_block, None, None, false),
                );
                // §7.1: the same standalone resource block(s) the persisted
                // transcript appends right after the `tool_result`. The
                // registry-claimed canonical batch carried on the event
                // (`registeredAttachments`, deterministic attach) wins;
                // otherwise fall back to lifting a proposal-MIME resource
                // item out of the echoed output. Gated on `completed` only
                // (matching `record_tool`) — an errored tool must not surface
                // an actionable ProposalCard. Each item is paired positionally
                // with the id `record_tool` gave the block it wrote for that
                // same item; an item without an id (the event carried none —
                // nothing was materialized for it) is skipped.
                if status == "completed" {
                    let items: Vec<Value> = d
                        .get("registeredAttachments")
                        .and_then(Value::as_array)
                        .cloned()
                        .unwrap_or_else(|| {
                            intent_services::tool_block::lift_proposal_resource(output)
                                .into_iter()
                                .collect()
                        });
                    let attach_ids: Vec<&str> = d
                        .get("proposalBlockIds")
                        .and_then(Value::as_array)
                        .map(|ids| ids.iter().filter_map(Value::as_str).collect())
                        .unwrap_or_default();
                    for (item, attach_id) in items.iter().zip(attach_ids) {
                        let attach_block =
                            intent_services::tool_block::build_proposal_resource_block(
                                attach_id, item,
                            );
                        let attach_added = self.note_block(attach_id);
                        self.remember_block(attach_id, &attach_block);
                        push_entity(
                            &mut added,
                            &mut updated,
                            attach_added,
                            self.entity(&message_id, attach_block, None, None, false),
                        );
                    }
                }
            }
        }
        Some(json!({ "added": added, "updated": updated, "removedIds": [] }))
    }

    /// Finalize the turn on `agent:stream:end`: reconcile against the persisted
    /// message, then reset the per-turn accumulation so the next turn on this
    /// subscription starts clean.
    ///
    /// The turn's message id is normally learned live (`seed_from_snapshot` or
    /// the first chunk/tool delta), but a subscription that opened after the
    /// last chunk and merged no in-flight message never learns it — and would
    /// then emit no terminal frame at all, leaving the turn missing from its
    /// transcript until the client resubscribes or refetches
    /// ([monorepo#2105](https://github.com/intent-hq/monorepo/issues/2105)).
    /// The terminal event already names the row it closes, so fall back to its
    /// `messageId`: the reconcile below re-reads the same bounded page a fresh
    /// snapshot would, and with no live-emitted ids every persisted block
    /// arrives as `added` with no orphan `removedIds` — the client converges on
    /// the fresh-snapshot state, which is exactly the §7.1 invariant. The
    /// fallback is inert whenever the id is already known (the normal case) and
    /// when the event carries none (a turn that persisted no assistant row).
    async fn finalize(&mut self, api: &dyn WorkspaceApi, event: &Event) -> Option<Value> {
        if self.message_id.is_none() {
            self.message_id = event
                .data
                .get("messageId")
                .and_then(Value::as_str)
                .map(str::to_string);
        }
        let delta = self.reconcile(api).await;
        self.text_acc.clear();
        self.seen_ids.clear();
        self.emitted_ids.clear();
        self.message_id = None;
        self.live_blocks.clear();
        delta
    }

    /// Re-read the now-persisted assistant message and emit a terminal delta that
    /// drives the client's accumulated state to exactly the fresh snapshot: every
    /// persisted block as `updated` (or `added` if never emitted live) carrying
    /// the authoritative `messageSeq`/`timestamp` and `streamingComplete:true`,
    /// plus `removedIds` for any block emitted live that the persisted message
    /// does not contain (e.g. a mispredicted `tool_result` index).
    ///
    /// The re-read is retried once, and a persistent failure still emits a
    /// terminal frame — the best-effort one built from the accumulated live
    /// state — plus a WARN. Returning `None` here would suppress the terminal
    /// frame entirely: the transcript then stays silently stale mid-turn while
    /// the turn looks ended (the lifecycle flags clear via the separate agent
    /// lifecycle lane), permanently, until the client resubscribes.
    ///
    /// Cost stays bounded regardless of transcript length: the re-read is the
    /// server-clamped newest page (SQL-side pagination, monorepo#958) and the
    /// retry re-issues that same bounded read — worst case two page reads per
    /// turn end, never full-history hydration — while the fallback frame is
    /// built purely from the already-accumulated [`Self::live_blocks`].
    async fn reconcile(&self, api: &dyn WorkspaceApi) -> Option<Value> {
        let message_id = self.message_id.clone()?;
        let read = || {
            api.agent_get_conversation(
                AgentId::from(self.agent_id.as_str()),
                None,
                None,
                None,
                None,
                None,
                self.projection,
            )
        };
        let conv = match read().await {
            Ok(conv) => conv,
            Err(first) => match read().await {
                Ok(conv) => conv,
                Err(retry) => {
                    tracing::warn!(
                        agent = %self.agent_id,
                        message = %message_id,
                        first_error = %first,
                        retry_error = %retry,
                        "terminal reconcile re-read failed twice; emitting best-effort \
                         terminal frame from accumulated live state"
                    );
                    return Some(self.best_effort_terminal(&message_id));
                }
            },
        };
        let Some(messages) = conv.get("messages").and_then(Value::as_array) else {
            // A successful read without a `messages` array should be
            // impossible (`agent.getConversation` always serves one), but
            // suppressing the frame here would reopen the exact silent-skip
            // this path exists to close — degrade the same way a failed
            // read does.
            tracing::warn!(
                agent = %self.agent_id,
                message = %message_id,
                "terminal reconcile re-read returned no messages array; emitting \
                 best-effort terminal frame from accumulated live state"
            );
            return Some(self.best_effort_terminal(&message_id));
        };
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

    /// The degraded terminal frame for a reconcile whose re-read failed twice:
    /// every accumulated live block re-emitted as `updated` (each was already
    /// delivered live or seeded from the seq-0 snapshot) stamped
    /// `streamingComplete: true`, so the client still flips out of the
    /// streaming state on the content it has. No authoritative
    /// `messageSeq`/`timestamp` (the store read failed) and no `removedIds`
    /// (without the persisted message no orphan is provable). Text/thinking
    /// entries are markers — their FULL text is rebuilt from
    /// [`Self::text_acc`] here, the one place the degraded frame needs it.
    ///
    /// When the id was learned only from the terminal event's `messageId`
    /// fallback (monorepo#2105 — the subscription opened after the last
    /// chunk), no live state was ever accumulated and this emits an empty
    /// frame: net-equal to the pre-fix `None` for that path (nothing was
    /// rendered as streaming, so nothing needs the flag).
    fn best_effort_terminal(&self, message_id: &str) -> Value {
        let updated: Vec<Value> = self
            .live_blocks
            .iter()
            .map(|(id, block)| {
                let block = match block.get("type").and_then(Value::as_str) {
                    Some(t) if block.get("text").is_none() && (t == "text" || t == "thinking") => {
                        let text = self.text_acc.get(id).map_or("", String::as_str);
                        json!({ "type": t, "id": id, "text": text })
                    }
                    _ => block.clone(),
                };
                self.entity(message_id, block, None, None, true)
            })
            .collect();
        json!({ "added": [], "updated": updated, "removedIds": [] })
    }

    /// Upsert a live block's last-known full JSON into [`Self::live_blocks`]
    /// (first-sighting order preserved) so a failed terminal reconcile can
    /// still emit it in the best-effort frame. For text/thinking blocks use
    /// [`Self::remember_text_marker`] instead — retaining their full JSON
    /// would duplicate `text_acc` on every chunk (O(N·L) copying per turn).
    fn remember_block(&mut self, block_id: &str, block: &Value) {
        match self.live_blocks.iter_mut().find(|(id, _)| id == block_id) {
            Some((_, existing)) => *existing = block.clone(),
            None => self.live_blocks.push((block_id.to_string(), block.clone())),
        }
    }

    /// Remember a text/thinking live block as a text-less `{type, id}` marker;
    /// [`Self::best_effort_terminal`] rebuilds the full block from
    /// [`Self::text_acc`] at emit time. Upserting keeps per-chunk cost
    /// O(chunk) while preserving first-sighting order.
    fn remember_text_marker(&mut self, block_id: &str, block_type: &str) {
        let marker = json!({ "type": block_type, "id": block_id });
        self.remember_block(block_id, &marker);
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
        self.entity_with_role(
            message_id,
            "assistant",
            block,
            seq,
            timestamp,
            streaming_complete,
        )
    }

    /// [`Self::entity`] with an explicit role: the stream family always wraps
    /// assistant content, while the `agent:message` re-read path carries the
    /// persisted row's real role (user/system/tool).
    fn entity_with_role(
        &self,
        message_id: &str,
        role: &str,
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
        e.insert("role".to_string(), Value::String(role.to_string()));
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

    /// Bound a live-synthesized `tool_use`/`tool_result` block under the
    /// subscription's projection (§5.5) — a no-op for full-fidelity
    /// subscriptions. Applied BEFORE `remember_block`, so the degraded
    /// best-effort terminal frame replays the slimmed block, never the raw one.
    fn slim_block(&self, block: &mut Value) {
        if self.projection == Some(ConversationProjection::Slim) {
            intent_services::tool_block::slim_tool_block(block);
        }
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
        Channel::Agent => agent_delta(api, event).await,
        Channel::Workspace => workspace_delta(api, event).await,
        Channel::Comment => comment_delta(api, workspace_id, note_id?, event).await,
        // The task channel uses the stateful [`task_delta`] mapper directly in
        // the forwarder: it tracks the spec's task-link set across deltas so a
        // spec-body edit can refresh flipped `specLinked` flags
        // (monorepo#2407) — so this generic stateless arm is unreachable for
        // `Task`.
        Channel::Task => None,
        // The chat channel uses the dedicated, stateful [`ChatDeltaState`] mapper
        // on the `forward_chat_subscription` path (CS-3) — its deltas are
        // event-payload-driven, not re-read — so this generic re-read arm is
        // unreachable for `Chat`.
        Channel::Chat => None,
    }
}

/// Materialize the task channel's seq-0 snapshot AND the spec's
/// `intent://local/task/{id}` link set from ONE `list_notes` read (no extra
/// query — O(rows) cost contract). Snapshot rows are note-shaped entities
/// enriched with the additive `specLinked` flag (same semantics as
/// `task.list`, §5.4); the returned link set seeds the forwarder's stateful
/// [`task_delta`] mapper so later spec-body edits can be diffed against it
/// (monorepo#2407). On a read error both degrade to empty rather than
/// failing the subscription.
pub(crate) async fn task_snapshot(
    api: &dyn WorkspaceApi,
    workspace_id: &WorkspaceId,
) -> (Value, HashSet<String>) {
    match api.list_notes(workspace_id).await {
        Ok(notes) => {
            let linked = notes
                .iter()
                .find(|n| n.id.as_str() == "spec")
                .map(|n| extract_spec_task_ids(&n.content))
                .unwrap_or_default();
            let tasks: Vec<Value> = notes
                .into_iter()
                .filter(|n| n.metadata.task.is_some())
                .filter_map(|n| task_note_value(n, &linked))
                .collect();
            (Value::Array(tasks), linked)
        }
        Err(_) => (Value::Array(Vec::new()), HashSet::new()),
    }
}

/// Serialize a task note for the `task` channel wire, stamping the additive
/// `specLinked` flag: true iff the note id appears in `linked` (the spec
/// body's `intent://local/task/{id}` link set — same semantics as `task.list`,
/// §5.4). Serialization keeps the note shape untouched otherwise; a non-object
/// serialization is dropped (never happens for [`Note`]).
fn task_note_value(note: Note, linked: &HashSet<String>) -> Option<Value> {
    let spec_linked = linked.contains(note.id.as_str());
    let mut value = serde_json::to_value(note).ok()?;
    value
        .as_object_mut()?
        .insert("specLinked".to_string(), Value::Bool(spec_linked));
    Some(value)
}

/// Resolve `note_id`'s CURRENT spec linkage from ONE bounded
/// `get_note("spec")` read and record it in the tracked set — touching ONLY
/// that id. The set must keep reflecting what was last STAMPED onto emitted
/// rows, per id: a fresh spec read here may already contain link changes for
/// OTHER tasks whose spec `note:updated` has not reached this forwarder yet,
/// and adopting those memberships without re-emitting their rows would make
/// the later flip diff see no difference — a permanently stale subscriber
/// flag, the very bug class of monorepo#2407. A missing spec degrades to
/// unlinked (`specLinked: false`), matching `task.list`. Unlike `task_get`
/// (which propagates non-NotFound store errors), a TRANSIENT read error also
/// degrades to unlinked here: dropping the delta would lose the row change
/// itself, which is worse than a conservatively-false flag that self-corrects
/// on the task's next event (or on the next spec-body edit, whose flip diff
/// re-emits the affected rows — see [`spec_link_flip_delta`]).
async fn stamp_spec_linked(
    api: &dyn WorkspaceApi,
    workspace_id: &WorkspaceId,
    note_id: &str,
    linked: &mut HashSet<String>,
) {
    let is_linked = match api
        .get_note(workspace_id.clone(), NoteId::from("spec"))
        .await
    {
        Ok(spec) => extract_spec_task_ids(&spec.content).contains(note_id),
        Err(_) => false,
    };
    if is_linked {
        linked.insert(note_id.to_string());
    } else {
        linked.remove(note_id);
    }
}

/// Map a spec `note:updated` / `note:deleted` to `updated` rows for the
/// tasks whose `specLinked` flag flipped (monorepo#2407): ONE `list_notes`
/// read yields both the new link set (from the spec's own content; empty
/// when the spec is gone, so a deletion unlinks every tracked task) and the
/// task rows to re-emit, keeping the O(rows) cost contract — no per-task
/// scans. Ids in the symmetric difference against the tracked set that are
/// not task notes (dangling links) flip silently; an edit that leaves the
/// link set unchanged emits nothing. The tracked set is replaced so every
/// stamp the subscriber has seen agrees with it; on a read error the delta
/// is dropped and the set kept (the next spec read reconverges).
async fn spec_link_flip_delta(
    api: &dyn WorkspaceApi,
    workspace_id: &WorkspaceId,
    linked: &mut HashSet<String>,
) -> Option<Value> {
    let notes = api.list_notes(workspace_id).await.ok()?;
    let new_linked = notes
        .iter()
        .find(|n| n.id.as_str() == "spec")
        .map(|n| extract_spec_task_ids(&n.content))
        .unwrap_or_default();
    let flipped: HashSet<String> = linked.symmetric_difference(&new_linked).cloned().collect();
    let updated: Vec<Value> = notes
        .into_iter()
        .filter(|n| n.metadata.task.is_some() && flipped.contains(n.id.as_str()))
        .filter_map(|n| task_note_value(n, &new_linked))
        .collect();
    *linked = new_linked;
    if updated.is_empty() {
        None
    } else {
        Some(json!({ "updated": updated }))
    }
}

/// Map a `task` channel event by re-reading the note and keeping only task
/// notes. `note:created` → `added`, `note:updated`/`task:status-changed` →
/// `updated` when the note still projects a task or `removedIds` when the note
/// has been demoted (task block removed), `note:deleted` → `removedIds` (the
/// delete is idempotent on the client even when the removed note was not a
/// task). `added`/`updated` rows carry the additive `specLinked` flag (one
/// extra spec-note read per delta, still O(rows) — see [`task_note_value`]).
///
/// The mapper is stateful: `linked` tracks the spec's task-link set as last
/// stamped onto emitted rows (seeded from [`task_snapshot`]'s read).
/// `note:updated` AND `note:deleted` for the SPEC itself route to
/// [`spec_link_flip_delta`], which diffs the new link set against `linked`
/// (empty after a deletion) and re-emits exactly the flipped task rows
/// (monorepo#2407) instead of the old junk `removedIds: ["spec"]` demotion.
/// Per-task arms stamp the event's own row via [`stamp_spec_linked`], which
/// updates ONLY that id in `linked` — never the whole set, so link changes
/// for other tasks stay visible to the flip diff of the spec event that
/// carries them.
pub(crate) async fn task_delta(
    api: &dyn WorkspaceApi,
    workspace_id: &WorkspaceId,
    event: &Event,
    linked: &mut HashSet<String>,
) -> Option<Value> {
    let note_id = event.data.get("noteId").and_then(Value::as_str)?;
    match event.event_type.as_str() {
        NOTE_UPDATED | NOTE_DELETED if note_id == "spec" => {
            spec_link_flip_delta(api, workspace_id, linked).await
        }
        NOTE_DELETED => Some(json!({ "removedIds": [note_id] })),
        NOTE_CREATED => {
            let note = api
                .get_note(workspace_id.clone(), NoteId::from(note_id))
                .await
                .ok()?;
            note.metadata.task.as_ref()?;
            stamp_spec_linked(api, workspace_id, note_id, linked).await;
            Some(json!({ "added": [task_note_value(note, linked)?] }))
        }
        NOTE_UPDATED | TASK_STATUS_CHANGED => {
            let note = api
                .get_note(workspace_id.clone(), NoteId::from(note_id))
                .await
                .ok()?;
            if note.metadata.task.is_none() {
                return Some(json!({ "removedIds": [note_id] }));
            }
            stamp_spec_linked(api, workspace_id, note_id, linked).await;
            Some(json!({ "updated": [task_note_value(note, linked)?] }))
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
        .map_or_else(|| event.workspace_id.as_str().to_string(), str::to_string);
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
        | WORKSPACE_DISPLAY_STATUS_CHANGED
        | WORKSPACE_WAITING_CHANGED
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
