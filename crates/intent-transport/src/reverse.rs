//! Daemon→client reverse JSON-RPC channel (§5.14, §12.4).
//!
//! Mirrors the ACP client-served pattern (PROTOCOL §6.7, `intent-acp`): the
//! daemon issues a JSON-RPC *request* TO the connected frontend and awaits the
//! frontend's response. This is the transport-side primitive that powers
//! FE-served intents such as `host.openExternal` on a remote/headless daemon —
//! "open this URL on the user's machine" is dispatched to the client rather
//! than executed on the daemon host.
//!
//! Frames are pushed through the connection's existing outbound queue (so they
//! never interleave with responses/notifications), and the client's replies are
//! delivered back via [`ReverseChannel::route_response`], called from the
//! connection's inbound loop. Reverse-request ids are minted with a distinct
//! `rev-<n>` prefix so an inbound response is unambiguously a reply to a
//! daemon-initiated request (a client *request* always carries a `method`).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Value};
use tokio::sync::{mpsc, oneshot};

/// Default reverse-request timeout. GUI intents are interactive, so this is more
/// generous than the ACP request timeout.
pub(crate) const DEFAULT_REVERSE_TIMEOUT: Duration = Duration::from_secs(30);

/// A JSON-RPC error returned by the client to a reverse request.
#[derive(Debug, Clone)]
pub struct ReverseError {
    pub code: i64,
    pub message: String,
}

type Pending = Arc<Mutex<HashMap<String, oneshot::Sender<Result<Value, ReverseError>>>>>;

/// Daemon→client reverse-RPC channel for one connection. Cheap to clone (`Arc`
/// inside); cloning shares the same pending map and id counter.
#[derive(Clone)]
pub struct ReverseChannel {
    out_tx: mpsc::Sender<String>,
    pending: Pending,
    next_id: Arc<AtomicU64>,
}

impl ReverseChannel {
    /// Build a channel that pushes reverse requests through `out_tx` (the
    /// connection's outbound frame queue).
    pub fn new(out_tx: mpsc::Sender<String>) -> Self {
        Self {
            out_tx,
            pending: Arc::new(Mutex::new(HashMap::new())),
            next_id: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Mint the next `rev-<n>` reverse-request id.
    fn mint_id(&self) -> String {
        let n = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        format!("rev-{n}")
    }

    /// Issue a reverse request to the connected client and await its response.
    /// Fails if the connection is closed, the response channel is dropped, or
    /// the client does not reply within `timeout`.
    ///
    /// # Errors
    ///
    /// Returns [`ReverseError`] if the connection is closed, the response channel is dropped, or the client does not reply within `timeout`.
    ///
    /// # Panics
    ///
    /// Panics if the pending-request mutex is poisoned (a prior panic while holding the lock).
    pub async fn request(
        &self,
        method: &str,
        params: Value,
        timeout: Duration,
    ) -> Result<Value, ReverseError> {
        let id = self.mint_id();
        let (tx, rx) = oneshot::channel();
        self.pending
            .lock()
            .expect("reverse pending poisoned")
            .insert(id.clone(), tx);

        let frame = serde_json::to_string(&json!({
            "jsonrpc": "2.0", "id": id, "method": method, "params": params,
        }))
        .unwrap_or_default();
        if self.out_tx.send(frame).await.is_err() {
            self.pending
                .lock()
                .expect("reverse pending poisoned")
                .remove(&id);
            return Err(ReverseError {
                code: 0,
                message: "client connection closed".to_string(),
            });
        }

        match tokio::time::timeout(timeout, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(ReverseError {
                code: 0,
                message: "reverse response channel dropped".to_string(),
            }),
            Err(_) => {
                self.pending
                    .lock()
                    .expect("reverse pending poisoned")
                    .remove(&id);
                Err(ReverseError {
                    code: 0,
                    message: format!("reverse request timed out: {method}"),
                })
            }
        }
    }

    /// Try to route an inbound frame as a response to a pending reverse request.
    /// Returns `true` when the frame was a reverse reply (a string `rev-*` id,
    /// no `method`) addressed to one of our pending requests and was delivered;
    /// otherwise `false`, so the caller continues normal classification.
    pub(crate) fn route_response(&self, value: &Value) -> bool {
        let Some(obj) = value.as_object() else {
            return false;
        };
        if obj.contains_key("method") {
            return false;
        }
        let Some(id) = obj.get("id").and_then(Value::as_str) else {
            return false;
        };
        let sender = self
            .pending
            .lock()
            .expect("reverse pending poisoned")
            .remove(id);
        let Some(sender) = sender else {
            return false;
        };
        if let Some(err) = obj.get("error") {
            let _ = sender.send(Err(ReverseError {
                code: err.get("code").and_then(Value::as_i64).unwrap_or(0),
                message: err
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_string(),
            }));
        } else {
            let _ = sender.send(Ok(obj.get("result").cloned().unwrap_or(Value::Null)));
        }
        true
    }
}

pub mod primary;
pub use primary::{PrimaryReverseGuard, PrimaryReverseRegistry};

#[cfg(test)]
mod tests;
