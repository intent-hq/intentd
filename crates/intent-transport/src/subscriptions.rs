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

use intent_core::events::{NOTE_CREATED, NOTE_DELETED, NOTE_UPDATED};
use intent_core::{Event, NoteId, WorkspaceApi, WorkspaceId};
use serde_json::{json, Map, Value};

use crate::events::IdInfo;

/// A subscription channel selected by the `*.subscribe` method (TB-0 §3). Only
/// the `note` collection channel is wired in TB-4; further channels land in TB-5.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Channel {
    Note,
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

/// Parsed `note.subscribe` params (§6.1). `workspaceId` is required for the
/// workspace-scoped note channel; `replaceGroup` is optional; `sinceSeq` is
/// reserved for post-v1.0 replay (§6.4) and ignored here.
#[derive(Debug)]
pub(crate) struct NoteSubscribeParams {
    pub workspace_id: String,
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
        "note.unsubscribe" => Some(SubFastPath::Unsubscribe { id, params }),
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
        replace_group: params
            .get("replaceGroup")
            .and_then(Value::as_str)
            .map(str::to_string),
    })
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

#[cfg(test)]
mod tests;
