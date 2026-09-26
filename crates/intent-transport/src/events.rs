//! Server-initiated event fast-path (PROTOCOL §6).
//!
//! Pure, transport-agnostic helpers that mirror the `events.subscribe` /
//! `events.unsubscribe` fast-path in `~/src/intent/src/main/`:
//! `websocket-api-server.ts` (the JSON-RPC-shape pre-check that routes the two
//! `events.` methods before the dispatcher) and `websocket-event-bridge.ts`
//! (`handleSubscribe` / `handleUnsubscribe` param validation, the global
//! `ws-sub-<n>` id counter, and the `events.event` notification envelope). The
//! connection orchestration that consumes these lives in [`crate::listener`].

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use intent_core::events::{AGENT_QUEUE_PROCESSING, AGENT_QUEUE_UPDATED, WORKSPACE_UPDATED};
use intent_core::{Caller, Event, PrincipalId, WorkspaceApi, WorkspaceId};
use serde_json::{json, Map, Value};

/// Delivery-time membership boundary for a non-administrator connection's
/// raw `events.subscribe` stream and its per-agent `chat` channel
/// (multiplayer w3). The bus filter only narrows event *types*; this gate
/// decides, per event, whether the subscriber may see the event's
/// *workspace*, by re-reading it through the API under the subscriber's
/// caller (`workspace.get` is `NotFound` for a non-member). Verdicts are
/// cached per workspace for [`Self::TTL`] so a busy stream costs one read
/// per workspace per window, and an unshare
/// (`workspace:updated { changes: { members, removedPrincipalId } }`)
/// invalidates the entry immediately: the removed member sees that one
/// event as its final notification and nothing after it.
pub(crate) struct MembershipGate {
    api: Arc<dyn WorkspaceApi>,
    principal_id: PrincipalId,
    verdicts: HashMap<String, (bool, Instant)>,
}

impl MembershipGate {
    const TTL: Duration = Duration::from_secs(30);

    /// A gate for the current request's caller, or `None` when the caller is
    /// an administrator (or unbound): those connections see every workspace.
    pub(crate) fn for_current_caller(api: &Arc<dyn WorkspaceApi>) -> Option<Self> {
        match crate::context::current_caller() {
            Some(Caller::Wire {
                principal_id,
                host_role: intent_core::HostRole::Member | intent_core::HostRole::Guest,
            }) => Some(Self {
                api: Arc::clone(api),
                principal_id,
                verdicts: HashMap::new(),
            }),
            _ => None,
        }
    }

    /// The `changes.removedPrincipalId` of an unshare event, if any.
    fn unshared_principal(event: &Event) -> Option<&str> {
        if event.event_type != WORKSPACE_UPDATED {
            return None;
        }
        event
            .data
            .get("changes")
            .and_then(|c| c.get("removedPrincipalId"))
            .and_then(Value::as_str)
    }

    /// Whether `event` names a membership change (add or remove) of its
    /// workspace — the cached verdict for that workspace is stale.
    fn is_membership_change(event: &Event) -> bool {
        event.event_type == WORKSPACE_UPDATED
            && event
                .data
                .get("changes")
                .and_then(|c| c.get("members"))
                .and_then(Value::as_bool)
                .unwrap_or(false)
    }

    /// Whether `event` is the subscriber's own removal from its workspace.
    pub(crate) fn is_own_unshare(&self, event: &Event) -> bool {
        Self::unshared_principal(event) == Some(self.principal_id.as_str())
    }

    /// Forget the verdict for a workspace whose membership just changed. Fed
    /// by the side subscription on `workspace:updated`, so a removal takes
    /// effect even when the subscriber's own patterns exclude that type.
    pub(crate) fn observe_membership_event(&mut self, event: &Event) {
        if !Self::is_membership_change(event) {
            return;
        }
        let workspace_id = event.workspace_id.as_str();
        if self.is_own_unshare(event) {
            self.verdicts
                .insert(workspace_id.to_string(), (false, Instant::now()));
        } else {
            self.verdicts.remove(workspace_id);
        }
    }

    /// Whether the subscriber may receive `event`.
    pub(crate) async fn allows(&mut self, event: &Event) -> bool {
        let workspace_id = event.workspace_id.as_str();
        if intent_core::events::is_member_execution_event_type(&event.event_type) {
            // Prompt/context events never use the cached visibility verdict.
            // Revocation takes effect before a socket is physically closed.
            if !self
                .api
                .principal_host_role(self.principal_id.clone())
                .await
                .is_ok_and(|role| {
                    matches!(
                        role,
                        intent_core::HostRole::Owner | intent_core::HostRole::Member
                    )
                })
            {
                return false;
            }
            if event.event_type == intent_core::events::HOST_EXECUTION_CONTEXT_CHANGED {
                return workspace_id.is_empty();
            }
            return !workspace_id.is_empty()
                && self
                    .api
                    .get_workspace(WorkspaceId::from(workspace_id))
                    .await
                    .is_ok();
        }
        if workspace_id.is_empty() {
            // Every collaborator-visible type is workspace-scoped; a global
            // event reaching here has nothing to authorize against.
            return false;
        }
        if self.is_own_unshare(event) {
            // The removed member's own final notification.
            self.verdicts
                .insert(workspace_id.to_string(), (false, Instant::now()));
            return true;
        }
        if Self::is_membership_change(event) {
            self.verdicts.remove(workspace_id);
        }
        if let Some((allowed, at)) = self.verdicts.get(workspace_id) {
            if at.elapsed() < Self::TTL {
                return *allowed;
            }
        }
        let allowed = self
            .api
            .get_workspace(WorkspaceId::from(workspace_id))
            .await
            .is_ok();
        self.verdicts
            .insert(workspace_id.to_string(), (allowed, Instant::now()));
        allowed
    }
}

/// Egress projection of the per-principal queue events for the current
/// request's caller. A non-administrator wire principal's
/// `agent:queue:updated` keeps in `data.queue` only the entries it may see
/// ([`intent_core::project_queue_for_caller`] — its own plus unattributed
/// ones; `position` is not renumbered); its `agent:queue:processing` for an
/// entry it may not see (the publisher's `metadata` attribution —
/// [`intent_core::queue_processing_event_attribution`]: another principal's
/// stamp, or the unknown-human marker of a human-origin entry the workspace
/// could not attribute — under the same
/// [`intent_core::queue_attribution_visible_to`] predicate) loses
/// `data.content` and keeps `agentId` / `messageId` / `turnId`, so the
/// drain-start signal still keys the turn without leaking the hidden
/// entry's text (intentd#2068). Every other event type, and every other
/// caller, passes through untouched. Called on events that passed the
/// [`MembershipGate`], under the subscriber's caller re-established by the
/// forwarder spawn.
pub(crate) fn project_queue_event_for_current_caller(event: &mut Event) {
    if event.event_type == AGENT_QUEUE_UPDATED {
        let Some(queue) = event.data.get_mut("queue").and_then(Value::as_array_mut) else {
            return;
        };
        let caller = crate::context::current_caller();
        let entries = std::mem::take(queue);
        *queue = intent_core::project_queue_for_caller(caller.as_ref(), entries);
    } else if event.event_type == AGENT_QUEUE_PROCESSING {
        let Some(caller) = crate::context::current_caller() else {
            return;
        };
        let attribution = intent_core::queue_processing_event_attribution(event.metadata.as_ref());
        if intent_core::queue_attribution_visible_to(&caller, &attribution) {
            return;
        }
        if let Some(data) = event.data.as_object_mut() {
            data.remove("content");
        }
    }
}

/// The `id` member of a fast-path request: whether it was present (a response is
/// only sent for requests, not notifications) and the value to echo (`id ?? null`).
pub(crate) struct IdInfo {
    pub present: bool,
    pub echo: Value,
}

/// A classified fast-path request awaiting handling by the connection task.
pub(crate) enum FastPath {
    Subscribe {
        id: IdInfo,
        params: Map<String, Value>,
    },
    Unsubscribe {
        id: IdInfo,
        params: Map<String, Value>,
    },
}

/// Parsed `events.subscribe` params (`handleSubscribe`).
#[derive(Debug)]
pub(crate) struct SubscribeParams {
    pub event_types: Vec<String>,
    pub workspace_id: Option<String>,
    pub replace_group: Option<String>,
}

/// Global, monotonic subscription id counter (`ws-sub-<n>`). Mirrors the TS
/// module-level `subCounter`, which is shared across all connections.
static SUB_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Mint the next `ws-sub-<n>` subscription id.
pub(crate) fn next_subscription_id() -> String {
    let n = SUB_COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
    format!("ws-sub-{n}")
}

/// Classify a parsed frame as a fast-path `events.` request, or `None` to fall
/// through to the JSON-RPC dispatcher. Mirrors the `websocket-api-server.ts`
/// pre-check: the frame must be a JSON-RPC 2.0 object with a string `method`,
/// and any present `id` must be a string, number, or null (otherwise it falls
/// through so the dispatcher returns the `-32600` invalid-request error).
pub(crate) fn classify(value: &Value) -> Option<FastPath> {
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
    // `parsed.params || {}`: a non-object (absent/null/array/scalar) yields `{}`,
    // which then fails the same required-param checks the TS handlers apply.
    let params = obj
        .get("params")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    match method {
        "events.subscribe" => Some(FastPath::Subscribe { id, params }),
        "events.unsubscribe" => Some(FastPath::Unsubscribe { id, params }),
        _ => None,
    }
}

/// Validate `events.subscribe` params. `eventTypes` must be a non-empty array
/// (TS throws otherwise → `-32602`); `workspaceId` / `replaceGroup` are optional.
pub(crate) fn parse_subscribe_params(
    params: &Map<String, Value>,
) -> Result<SubscribeParams, String> {
    let event_types = match params.get("eventTypes") {
        Some(Value::Array(arr)) if !arr.is_empty() => arr
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_string)
            .collect(),
        _ => return Err("eventTypes must be a non-empty array of event type strings".to_string()),
    };
    Ok(SubscribeParams {
        event_types,
        workspace_id: opt_str(params, "workspaceId"),
        replace_group: opt_str(params, "replaceGroup"),
    })
}

/// Validate `events.unsubscribe` params. A missing/empty `subscriptionId` throws
/// (`-32602`); a present-but-unknown id is handled by the caller as `success:false`.
pub(crate) fn parse_unsubscribe_id(params: &Map<String, Value>) -> Result<String, String> {
    match params.get("subscriptionId").and_then(Value::as_str) {
        Some(s) if !s.is_empty() => Ok(s.to_string()),
        _ => Err("subscriptionId is required".to_string()),
    }
}

fn opt_str(params: &Map<String, Value>, name: &str) -> Option<String> {
    params.get(name).and_then(Value::as_str).map(str::to_string)
}

/// Build the `events.event` notification (PROTOCOL §6.3). The `event` object
/// carries exactly `type`, `workspaceId`, `id`, `timestamp`, `actor`, `data` —
/// matching `websocket-event-bridge.ts` (session/correlation ids are omitted).
pub(crate) fn build_event_notification(subscription_id: &str, event: &Event) -> String {
    let frame = json!({
        "jsonrpc": "2.0",
        "method": "events.event",
        "params": {
            "subscriptionId": subscription_id,
            "event": {
                "type": &event.event_type,
                "workspaceId": &event.workspace_id,
                "id": &event.id,
                "timestamp": &event.timestamp,
                "actor": &event.actor,
                "data": &event.data,
            }
        }
    });
    serde_json::to_string(&frame).unwrap_or_default()
}

/// Serialize a JSON-RPC success response for a fast-path request.
pub(crate) fn success_frame(id: &Value, result: &Value) -> String {
    serde_json::to_string(&json!({ "jsonrpc": "2.0", "id": id, "result": result }))
        .unwrap_or_default()
}

/// Serialize a JSON-RPC error response for a fast-path request. A `-32602`
/// carries the machine-readable discriminator `error.data.code =
/// "invalid-params"` (PROTOCOL §3.3, monorepo#1364), mirroring the dispatcher's
/// `invalid_params` helper: every fast-path `-32602` is a parameter-validation
/// failure (the only entity-absent fast-path case — an unknown
/// `host.execStream` `requestId` — is `-32603`), so the discriminator is
/// attached centrally here rather than at each call site. A future fast-path
/// site addressing a missing entity must emit `"not-found"` via a dedicated
/// variant instead.
pub(crate) fn error_frame(id: &Value, code: i32, message: &str) -> String {
    let error = if code == -32602 {
        json!({ "code": code, "message": message, "data": { "code": "invalid-params" } })
    } else {
        json!({ "code": code, "message": message })
    };
    serde_json::to_string(&json!({ "jsonrpc": "2.0", "id": id, "error": error }))
        .unwrap_or_default()
}

/// Serialize a JSON-RPC error response carrying an explicit `error.data`
/// payload — for fast-path sites that attach a machine-readable
/// discriminator beyond the centralized `-32602` tagging in [`error_frame`]
/// (e.g. the pairing listener-down `{ "code": "listener-down" }`,
/// monorepo#1822).
pub(crate) fn error_frame_with_data(id: &Value, code: i32, message: &str, data: &Value) -> String {
    // §3.3 invariant: every -32602 carries an `error.data.code` discriminator
    // — a caller bypassing the centralized tagging must supply one itself.
    debug_assert!(
        code != -32602 || data.get("code").is_some(),
        "-32602 error.data must carry a `code` discriminator (PROTOCOL §3.3)"
    );
    serde_json::to_string(&json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": { "code": code, "message": message, "data": data }
    }))
    .unwrap_or_default()
}

#[cfg(test)]
mod tests;
