//! Presence fast paths (multiplayer w5): `presence.update` and
//! `note.presence.update`, plus the per-connection presence handle.
//!
//! Presence is keyed by (principal, connection), so like `client.hello` /
//! `drafts.*` these methods are a transport concern: they consume the
//! connection's presence id (minted per connection, never a wire parameter)
//! and are intercepted before the JSON-RPC dispatcher. The connection task
//! reports the connection's lifecycle to the service layer through
//! [`PresenceConn`]: `client.hello` marks it online, and dropping the handle
//! — clean close, heartbeat reap and shutdown alike — publishes the offline
//! transition and releases every `note.presence` lease it still held. The
//! `note.presence.subscribe` channel itself lives in `subscriptions.rs` /
//! `conn.rs` next to the other channels.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use intent_core::{Error, NoteId, WorkspaceApi, WorkspaceId};
use serde_json::{json, Value};

use crate::events::{error_frame, error_frame_with_data, success_frame};

static CONN_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A connection's presence identity and its disconnect hook. `Default`
/// mints the id; the hook is armed by [`Self::attach`] once the connection
/// has told the service layer about itself, so a connection that never
/// touched presence costs nothing on close.
pub(crate) struct PresenceConn {
    id: String,
    api: Option<Arc<dyn WorkspaceApi>>,
}

impl Default for PresenceConn {
    fn default() -> Self {
        let n = CONN_COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
        Self {
            id: format!("conn-{n}"),
            api: None,
        }
    }
}

impl PresenceConn {
    pub(crate) fn id(&self) -> &str {
        &self.id
    }

    /// Arm the disconnect hook (idempotent).
    pub(crate) fn attach(&mut self, api: &Arc<dyn WorkspaceApi>) {
        if self.api.is_none() {
            self.api = Some(Arc::clone(api));
        }
    }
}

impl Drop for PresenceConn {
    fn drop(&mut self) {
        let Some(api) = self.api.take() else {
            return;
        };
        let id = std::mem::take(&mut self.id);
        if tokio::runtime::Handle::try_current().is_ok() {
            intent_core::spawn_daemon(async move { api.presence_disconnect(id).await });
        }
    }
}

/// Guard held by a `note.presence` channel forwarder: dropping it (unsubscribe,
/// `replaceGroup` replacement, connection close, forwarder exit) releases
/// the lease with the service layer, which publishes the viewer's `left`.
pub(crate) struct NoteLease {
    api: Arc<dyn WorkspaceApi>,
    connection_id: String,
    lease_id: String,
}

impl NoteLease {
    pub(crate) fn new(api: Arc<dyn WorkspaceApi>, connection_id: &str, lease_id: &str) -> Self {
        Self {
            api,
            connection_id: connection_id.to_string(),
            lease_id: lease_id.to_string(),
        }
    }
}

impl Drop for NoteLease {
    fn drop(&mut self) {
        let api = Arc::clone(&self.api);
        let connection_id = std::mem::take(&mut self.connection_id);
        let lease_id = std::mem::take(&mut self.lease_id);
        if tokio::runtime::Handle::try_current().is_ok() {
            intent_core::spawn_daemon(async move {
                api.note_presence_leave(connection_id, lease_id).await;
            });
        }
    }
}

/// The two presence fast-path methods, once classified.
pub(crate) enum PresenceMethod {
    /// `presence.update` — params passed through whole to the service layer.
    Update(Value),
    /// `note.presence.update { workspaceId, noteId, rev, anchor, head }`.
    NoteUpdate {
        workspace_id: Option<String>,
        note_id: Option<String>,
        cursor: Value,
    },
}

/// A classified presence request awaiting handling by the connection task.
pub(crate) struct PresenceRequest {
    pub method: PresenceMethod,
    pub id_present: bool,
    pub id_echo: Value,
}

/// Classify a parsed frame as a presence fast-path request, or `None` to fall
/// through. Mirrors the `drafts`/`client` fast-path pre-check.
pub(crate) fn classify(value: &Value) -> Option<PresenceRequest> {
    let obj = value.as_object()?;
    if obj.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return None;
    }
    let method_name = obj.get("method").and_then(Value::as_str)?;
    let id_member = obj.get("id");
    if let Some(v) = id_member {
        if !v.is_null() && !v.is_string() && !v.is_number() {
            return None;
        }
    }
    let params = obj.get("params").cloned().unwrap_or_else(|| json!({}));
    let opt_str = |name: &str| {
        params
            .get(name)
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
    };
    let method = match method_name {
        "presence.update" => PresenceMethod::Update(params.clone()),
        "note.presence.update" => PresenceMethod::NoteUpdate {
            workspace_id: opt_str("workspaceId"),
            note_id: opt_str("noteId"),
            cursor: params.clone(),
        },
        _ => return None,
    };
    Some(PresenceRequest {
        method,
        id_present: id_member.is_some(),
        id_echo: id_member.cloned().unwrap_or(Value::Null),
    })
}

/// Handle a classified presence request on behalf of `conn` (arming its
/// disconnect hook), returning the response frame (`None` for a notification).
pub(crate) async fn handle(
    req: PresenceRequest,
    api: &Arc<dyn WorkspaceApi>,
    conn: &mut PresenceConn,
) -> Option<String> {
    conn.attach(api);
    let connection_id = conn.id().to_string();
    let result = match req.method {
        PresenceMethod::Update(params) => api.presence_update(connection_id, params).await,
        PresenceMethod::NoteUpdate {
            workspace_id,
            note_id,
            cursor,
        } => match (workspace_id, note_id) {
            (Some(workspace_id), Some(note_id)) => {
                api.note_presence_update(
                    connection_id,
                    WorkspaceId::from(workspace_id),
                    NoteId::from(note_id),
                    cursor,
                )
                .await
            }
            (None, _) => Err(Error::InvalidParams(
                "note.presence.update: workspaceId is required".to_string(),
            )),
            (_, None) => Err(Error::InvalidParams(
                "note.presence.update: noteId is required".to_string(),
            )),
        },
    };
    respond(req.id_present, &req.id_echo, result)
}

/// Frame a service outcome the way the dispatcher would (§9): `NotFound` is
/// `-32602 { code: "not-found" }` (a non-member sees no workspace), a
/// capability refusal is the allowlist's `-32003 "Forbidden"` envelope with
/// the reason in `data.detail`, and every other `-32602` carries
/// `"invalid-params"` through [`error_frame`].
pub(crate) fn respond(
    id_present: bool,
    id_echo: &Value,
    result: intent_core::Result<Value>,
) -> Option<String> {
    if !id_present {
        return None;
    }
    Some(match result {
        Ok(v) => success_frame(id_echo, &v),
        Err(e @ Error::NotFound(_)) => error_frame_with_data(
            id_echo,
            e.code(),
            &e.to_string(),
            &json!({ "code": "not-found" }),
        ),
        Err(Error::Forbidden(detail)) => error_frame_with_data(
            id_echo,
            crate::catalog::FORBIDDEN_ERROR_CODE,
            crate::catalog::FORBIDDEN_ERROR_MESSAGE,
            &json!({ "code": "forbidden", "detail": detail }),
        ),
        Err(e) => error_frame(id_echo, e.code(), &e.to_string()),
    })
}
