//! Intent apps at the connection (PROTOCOL §5.60, Labs).
//!
//! Three per-connection jobs, all run by [`crate::conn`]'s inbound
//! chokepoint before any other classify or dispatch path:
//!
//! 1. **Attach.** `app.session.attach { token }` as the *first* frame of an
//!    administrator connection narrows it, for life, to the app principal.
//!    Any earlier frame makes attach fail with `attach-not-first`; an
//!    unknown token is `session-invalid`. Every failure answers, then closes
//!    the connection.
//! 2. **The app allowlist.** A bound connection reaches only the methods of
//!    its level ([`catalog::app_may_call`], exact names); everything else,
//!    unknown names included, is `-32003` without `data`. An allowed frame
//!    is dispatched straight to the router with [`Caller::App`] bound and no
//!    wire credential, never through the administrator frame context the
//!    read loop captured.
//! 3. **Session lifetime.** The connection closes when its session ends
//!    ([`AppConn::end_signal`], watched by both read loops), and the
//!    sessions an administrator connection opened end when it closes
//!    ([`OpenedSessions`]).

use std::sync::Arc;

use intent_core::apps::AppPrincipal;
use intent_core::{Caller, WorkspaceApi};
use serde_json::Value;
use tokio::sync::watch;

use crate::catalog;
use crate::conn::OutboundSender;

/// Whether `method` is the attach fast path.
pub(crate) fn is_attach(method: &str) -> bool {
    method == "app.session.attach"
}

/// Per-connection app state, owned by `ConnSubs`.
#[derive(Default)]
pub(crate) struct AppConn {
    /// Inbound frames seen so far (every text frame, malformed included).
    frames: u64,
    bound: Option<Arc<AppPrincipal>>,
    /// Resolves (`true` or sender dropped) when the connection must close:
    /// the bound session ended, or an attach failed.
    end: Option<watch::Receiver<bool>>,
    opened: Option<OpenedSessions>,
}

impl AppConn {
    /// Count one inbound frame; returns how many came before it.
    pub(crate) fn note_frame(&mut self) -> u64 {
        let before = self.frames;
        self.frames = self.frames.saturating_add(1);
        before
    }

    pub(crate) fn bound(&self) -> Option<&Arc<AppPrincipal>> {
        self.bound.as_ref()
    }

    /// The close signal for the read loop's select; `None` while the
    /// connection has no reason to close.
    pub(crate) fn end_signal(&self) -> Option<watch::Receiver<bool>> {
        self.end.clone()
    }

    /// Close after the pending replies flush (an attach failure).
    fn close_after_reply(&mut self) {
        let (tx, rx) = watch::channel(true);
        drop(tx);
        self.end = Some(rx);
    }

    /// Record a session this administrator connection opened.
    pub(crate) fn opened(&mut self, api: &Arc<dyn WorkspaceApi>, session_id: String) {
        self.opened
            .get_or_insert_with(|| OpenedSessions {
                api: Arc::clone(api),
                ids: Vec::new(),
            })
            .ids
            .push(session_id);
    }
}

/// Wait for a close signal; pending forever without one.
pub(crate) async fn wait_end(rx: Option<watch::Receiver<bool>>) {
    let Some(mut rx) = rx else {
        return std::future::pending().await;
    };
    loop {
        if *rx.borrow_and_update() {
            return;
        }
        if rx.changed().await.is_err() {
            return;
        }
    }
}

/// The sessions an administrator connection opened. Dropping it (the
/// connection closed) ends them (§5.60 "Session end").
pub(crate) struct OpenedSessions {
    api: Arc<dyn WorkspaceApi>,
    ids: Vec<String>,
}

impl Drop for OpenedSessions {
    fn drop(&mut self) {
        let ids = std::mem::take(&mut self.ids);
        if ids.is_empty() {
            return;
        }
        let api = Arc::clone(&self.api);
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(intent_core::caller::with_caller(
                Caller::Daemon,
                async move {
                    api.app_sessions_end(ids).await;
                },
            ));
        }
    }
}

fn error_frame(id: &Value, code: i32, message: &str, data: Option<Value>) -> String {
    let mut error = serde_json::json!({ "code": code, "message": message });
    if let Some(data) = data {
        error["data"] = data;
    }
    serde_json::json!({ "jsonrpc": "2.0", "id": id, "error": error }).to_string()
}

/// Answer `app.session.attach` (the fast path). Returns `false` only when
/// the outbound channel is closed. On failure the connection is marked to
/// close once the reply flushes.
pub(crate) async fn handle_attach(
    value: &Value,
    rpc_id: Option<Value>,
    first: bool,
    api: &Arc<dyn WorkspaceApi>,
    out_tx: &OutboundSender,
    app: &mut AppConn,
) -> bool {
    let id = rpc_id.clone().unwrap_or(Value::Null);
    let result = if first {
        match value
            .get("params")
            .and_then(|p| p.get("token"))
            .and_then(Value::as_str)
        {
            Some(token) => api
                .app_session_attach(token.to_owned())
                .await
                .map_err(|e| match e {
                    intent_core::Error::App(app_error) => {
                        (app_error.rpc_code, app_error.message, Some(app_error.data))
                    }
                    intent_core::Error::Forbidden(_) => (
                        catalog::FORBIDDEN_ERROR_CODE,
                        catalog::FORBIDDEN_ERROR_MESSAGE.to_owned(),
                        None,
                    ),
                    other => {
                        tracing::warn!(error = %other, "app.session.attach failed");
                        (
                            -32603,
                            "Internal error".to_owned(),
                            Some(serde_json::json!({ "code": "internal" })),
                        )
                    }
                }),
            None => Err((
                -32602,
                "invalid params: token is required".to_owned(),
                Some(
                    serde_json::json!({ "code": "invalid-params", "detail": "token is required" }),
                ),
            )),
        }
    } else {
        Err((
            -32602,
            "app.session.attach must be the first request on the connection".to_owned(),
            Some(serde_json::json!({ "code": "attach-not-first" })),
        ))
    };
    match result {
        Ok(attachment) => {
            app.bound = Some(attachment.principal);
            app.end = Some(attachment.ended);
            match rpc_id {
                Some(id) => out_tx
                    .send_priority(
                        serde_json::json!({ "jsonrpc": "2.0", "id": id, "result": attachment.result })
                            .to_string(),
                    )
                    .await
                    .is_ok(),
                None => true,
            }
        }
        Err((code, message, data)) => {
            app.close_after_reply();
            match rpc_id {
                Some(_) => out_tx
                    .send_priority(error_frame(&id, code, &message, data))
                    .await
                    .is_ok(),
                None => !out_tx.is_closed(),
            }
        }
    }
}

/// Whether a bound app connection may make this call (exact name, level
/// list). A frame without a method name is refused too: it would only ever
/// reach the router's envelope errors, and an app needs none of them.
pub(crate) fn app_allows(app: &AppPrincipal, method: &str) -> bool {
    !method.is_empty() && catalog::app_may_call(app.level, method)
}
