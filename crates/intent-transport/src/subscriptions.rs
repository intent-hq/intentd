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
    AGENT_RESTORED, AGENT_STARTED, AGENT_STATUS_CHANGED, COMMENT_ADDED, NOTE_CREATED, NOTE_DELETED,
    NOTE_UPDATED, PR_LINKED, PR_UNLINKED, PR_UPDATED, TASK_STATUS_CHANGED,
    WORKSPACE_ACTIVITY_CHANGED, WORKSPACE_ATTENTION_CHANGED, WORKSPACE_CREATED, WORKSPACE_DELETED,
    WORKSPACE_UPDATED,
};
use intent_core::{AgentId, Event, NoteId, WorkspaceApi, WorkspaceId};
use serde_json::{json, Map, Value};

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
        | "comment.unsubscribe" => Some(SubFastPath::Unsubscribe { id, params }),
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
                let tasks: Vec<_> = notes.into_iter().filter(|n| n.task.is_some()).collect();
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
        Channel::Task => task_delta(api, workspace_id, event).await,
        Channel::Agent => agent_delta(api, event).await,
        Channel::Workspace => workspace_delta(api, event).await,
        Channel::Comment => comment_delta(api, workspace_id, note_id?, event).await,
    }
}

/// Map a `task` channel event by re-reading the note and keeping only task
/// notes. `note:created` → `added`, `note:updated`/`task:status-changed` →
/// `updated`, `note:deleted` → `removedIds` (the delete is idempotent on the
/// client even when the removed note was not a task).
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
            note.task.as_ref()?;
            Some(json!({ "added": [serde_json::to_value(note).ok()?] }))
        }
        NOTE_UPDATED | TASK_STATUS_CHANGED => {
            let note = api
                .get_note(workspace_id.clone(), NoteId::from(note_id))
                .await
                .ok()?;
            note.task.as_ref()?;
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
        | AGENT_RENAMED | AGENT_RESTORED => {
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
