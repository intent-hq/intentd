//! Panic isolation for JSON-RPC frame handlers (#457).
//!
//! Every handler path reachable from [`crate::conn::process_frame`] — inline
//! or spawned — is wrapped so a panicking handler yields a `-32603 Internal
//! error` response with the echoed request `id` (or no frame at all for a
//! notification) instead of tearing down the read loop or the daemon. The
//! connection stays open and continues serving subsequent frames.

use std::any::Any;
use std::future::Future;
use std::panic::AssertUnwindSafe;

use futures::FutureExt;
use serde_json::{json, Value};
use tokio::sync::mpsc;

/// Extract the request `id` (as sent, `None` when absent → notification) and
/// the `method` name (empty string when absent) from a parsed frame.
pub(crate) fn request_identity(value: &Value) -> (Option<Value>, String) {
    let rpc_id = value.get("id").cloned();
    let method = value
        .get("method")
        .and_then(|m| m.as_str())
        .unwrap_or_default()
        .to_string();
    (rpc_id, method)
}

/// Serialize the `-32603 Internal error` response echoing the request `id`.
pub(crate) fn internal_error_frame(id: Value) -> String {
    serde_json::to_string(&json!({
        "jsonrpc": "2.0",
        "error": { "code": -32603, "message": "Internal error" },
        "id": id
    }))
    .unwrap_or_default()
}

/// Best-effort panic payload text for the log line.
fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

/// Test-only panic injection: panics when `method` is listed in the
/// comma-separated `INTENTD_TEST_PANIC_METHOD` env var. Compiled out of
/// release builds (`debug_assertions` off ⇒ unconditional no-op).
#[cfg(debug_assertions)]
pub(crate) fn maybe_inject_panic(method: &str) {
    if method.is_empty() {
        return;
    }
    if let Ok(list) = std::env::var("INTENTD_TEST_PANIC_METHOD") {
        if list.split(',').map(str::trim).any(|m| m == method) {
            panic!("injected test panic for method {method}");
        }
    }
}

/// Release builds: no injection hook.
#[cfg(not(debug_assertions))]
pub(crate) fn maybe_inject_panic(_method: &str) {}

/// Run an async handler that yields an optional response frame, converting a
/// panic into the `-32603` frame (requests) or `None` (notifications).
pub(crate) async fn guard_frame<F>(method: &str, rpc_id: Option<Value>, fut: F) -> Option<String>
where
    F: Future<Output = Option<String>>,
{
    let result = AssertUnwindSafe(async {
        maybe_inject_panic(method);
        fut.await
    })
    .catch_unwind()
    .await;
    match result {
        Ok(frame) => frame,
        Err(payload) => {
            tracing::error!(
                method,
                panic = %panic_message(payload.as_ref()),
                "JSON-RPC handler panicked; connection kept alive"
            );
            rpc_id.map(internal_error_frame)
        }
    }
}

/// Synchronous variant of [`guard_frame`] for inline sync handlers.
pub(crate) fn guard_frame_sync<F>(method: &str, rpc_id: Option<Value>, handler: F) -> Option<String>
where
    F: FnOnce() -> Option<String>,
{
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        maybe_inject_panic(method);
        handler()
    }));
    match result {
        Ok(frame) => frame,
        Err(payload) => {
            tracing::error!(
                method,
                panic = %panic_message(payload.as_ref()),
                "JSON-RPC handler panicked; connection kept alive"
            );
            rpc_id.map(internal_error_frame)
        }
    }
}

/// Run an async handler that sends its own frames and returns channel
/// liveness. On panic, sends the `-32603` frame for requests (nothing for
/// notifications) and reports whether the outbound channel is still open.
pub(crate) async fn guard_send<F>(
    method: &str,
    rpc_id: Option<Value>,
    out_tx: &mpsc::Sender<String>,
    fut: F,
) -> bool
where
    F: Future<Output = bool>,
{
    let result = AssertUnwindSafe(async {
        maybe_inject_panic(method);
        fut.await
    })
    .catch_unwind()
    .await;
    match result {
        Ok(open) => open,
        Err(payload) => {
            tracing::error!(
                method,
                panic = %panic_message(payload.as_ref()),
                "JSON-RPC handler panicked; connection kept alive"
            );
            match rpc_id {
                Some(id) => out_tx.send(internal_error_frame(id)).await.is_ok(),
                None => true,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Silence the default panic hook's stderr backtrace inside a scope; the
    /// guards rely on `catch_unwind`, not the hook, so behavior is unchanged.
    fn with_quiet_panics<T>(f: impl FnOnce() -> T) -> T {
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let out = f();
        std::panic::set_hook(prev);
        out
    }

    #[tokio::test]
    async fn guard_frame_panic_on_request_yields_internal_error_with_echoed_id() {
        let frame = with_quiet_panics(|| {
            futures::executor::block_on(guard_frame("x.y", Some(json!(7)), async {
                panic!("boom");
            }))
        })
        .expect("request panic must produce a frame");
        let v: Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(v["jsonrpc"], "2.0");
        assert_eq!(v["id"], json!(7));
        assert_eq!(v["error"]["code"], json!(-32603));
        assert_eq!(v["error"]["message"], "Internal error");
    }

    #[tokio::test]
    async fn guard_frame_panic_on_notification_yields_no_frame() {
        let frame = with_quiet_panics(|| {
            futures::executor::block_on(guard_frame("x.y", None, async {
                panic!("boom");
            }))
        });
        assert!(frame.is_none());
    }

    #[tokio::test]
    async fn guard_frame_passes_through_without_panic() {
        let frame = guard_frame("x.y", Some(json!(1)), async { Some("ok".to_string()) }).await;
        assert_eq!(frame.as_deref(), Some("ok"));
    }

    #[test]
    fn guard_frame_sync_panic_echoes_string_id() {
        let frame =
            with_quiet_panics(|| guard_frame_sync("x.y", Some(json!("abc")), || panic!("boom")))
                .expect("request panic must produce a frame");
        let v: Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(v["id"], json!("abc"));
        assert_eq!(v["error"]["code"], json!(-32603));
    }

    #[test]
    fn guard_frame_sync_panic_on_notification_yields_no_frame() {
        let frame = with_quiet_panics(|| guard_frame_sync("x.y", None, || panic!("boom")));
        assert!(frame.is_none());
    }

    #[tokio::test]
    async fn guard_send_panic_on_request_sends_internal_error() {
        let (tx, mut rx) = mpsc::channel::<String>(4);
        let open = with_quiet_panics(|| {
            futures::executor::block_on(guard_send("x.y", Some(json!(3)), &tx, async {
                panic!("boom");
            }))
        });
        assert!(open);
        let frame = rx.try_recv().expect("frame queued");
        let v: Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(v["id"], json!(3));
        assert_eq!(v["error"]["code"], json!(-32603));
    }

    #[tokio::test]
    async fn guard_send_panic_on_notification_sends_nothing() {
        let (tx, mut rx) = mpsc::channel::<String>(4);
        let open = with_quiet_panics(|| {
            futures::executor::block_on(guard_send("x.y", None, &tx, async {
                panic!("boom");
            }))
        });
        assert!(open);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn request_identity_extracts_id_and_method() {
        let v: Value =
            serde_json::from_str(r#"{"jsonrpc":"2.0","id":42,"method":"a.b","params":{}}"#)
                .unwrap();
        assert_eq!(request_identity(&v), (Some(json!(42)), "a.b".to_string()));
        let notif: Value =
            serde_json::from_str(r#"{"jsonrpc":"2.0","method":"a.b","params":{}}"#).unwrap();
        assert_eq!(request_identity(&notif), (None, "a.b".to_string()));
        let null_id: Value =
            serde_json::from_str(r#"{"jsonrpc":"2.0","id":null,"method":"a.b"}"#).unwrap();
        assert_eq!(
            request_identity(&null_id),
            (Some(Value::Null), "a.b".to_string())
        );
    }

    #[test]
    fn inject_panic_matches_env_list() {
        // Uses a process-wide env var; unique name-space per test binary run.
        std::env::set_var("INTENTD_TEST_PANIC_METHOD", "p.one , p.two");
        let hit =
            with_quiet_panics(|| std::panic::catch_unwind(|| maybe_inject_panic("p.two")).is_err());
        let miss = std::panic::catch_unwind(|| maybe_inject_panic("p.three")).is_ok();
        std::env::remove_var("INTENTD_TEST_PANIC_METHOD");
        assert!(hit, "listed method must panic");
        assert!(miss, "unlisted method must not panic");
    }
}
