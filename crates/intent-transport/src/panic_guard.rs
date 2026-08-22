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

use crate::conn::OutboundSender;

/// Extract the request `id` (`None` when absent → notification) and the
/// `method` name (empty string when absent) from a parsed frame. Invalid id
/// types (object/array/bool) are coerced to `null` per JSON-RPC 2.0, which
/// only allows string/number/null ids in responses.
pub(crate) fn request_identity(value: &Value) -> (Option<Value>, String) {
    let rpc_id = value.get("id").cloned().map(|id| match id {
        Value::String(_) | Value::Number(_) | Value::Null => id,
        _ => Value::Null,
    });
    let method = value
        .get("method")
        .and_then(|m| m.as_str())
        .unwrap_or_default()
        .to_string();
    (rpc_id, method)
}

/// Serialize the `-32603 Internal error` response echoing the request `id`.
/// If serialization itself ever fails, fall back to a deterministic valid
/// envelope (id null) rather than an empty frame, matching the router's
/// `internal_fallback`.
pub(crate) fn internal_error_frame(id: &Value) -> String {
    serde_json::to_string(&json!({
        "jsonrpc": "2.0",
        "error": { "code": -32603, "message": "Internal error" },
        "id": id
    }))
    .unwrap_or_else(|_| {
        r#"{"jsonrpc":"2.0","error":{"code":-32603,"message":"Internal error"},"id":null}"#
            .to_string()
    })
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
        assert!(
            !list.split(',').map(str::trim).any(|m| m == method),
            "injected test panic for method {method}"
        );
    }
}

/// Release builds: no injection hook.
#[cfg(not(debug_assertions))]
pub(crate) fn maybe_inject_panic(_method: &str) {}

/// Run an async handler that yields an optional response frame, converting a
/// panic into the `-32603` frame (requests) or `None` (notifications).
///
/// This is also the single outbound chokepoint for the log-only large-frame
/// warning: every response path in [`crate::conn::process_frame`] — inline or
/// spawned, router-dispatched or fast-path (`control.*`, `server.*`,
/// `pairing.*`, `host.*`, `browser.*`, `forward.*`, `client.*`, `drafts.*`) —
/// returns its frame through here, so responses that bypass the router (e.g.
/// a `host.exec` result embedding untruncated stdout) are covered too. No
/// double warn with the router's `-32010` oversized-response path: the router
/// replaces the oversized frame with a small error frame before it reaches
/// this point.
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
        Ok(frame) => {
            if let Some(frame) = &frame {
                crate::protocol::warn_if_large_frame(
                    crate::protocol::FrameDirection::Outbound,
                    method,
                    frame.len(),
                );
            }
            frame
        }
        Err(payload) => {
            tracing::error!(
                method,
                panic = %panic_message(payload.as_ref()),
                "JSON-RPC handler panicked; connection kept alive"
            );
            rpc_id.as_ref().map(internal_error_frame)
        }
    }
}

/// Run an async handler that sends its own frames and returns channel
/// liveness. On panic, sends the `-32603` frame for requests (nothing for
/// notifications) and reports whether the outbound channel is still open.
pub(crate) async fn guard_send<F>(
    method: &str,
    rpc_id: Option<Value>,
    out_tx: &OutboundSender,
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
                Some(id) => out_tx
                    .send_priority(internal_error_frame(&id))
                    .await
                    .is_ok(),
                None => !out_tx.is_closed(),
            }
        }
    }
}

/// Test-only serialization of process-global state. The panic hook is a
/// process-global, so every test in this crate that swaps it must go through
/// [`with_quiet_panics`] (or hold [`lock_global_state`] while swapping);
/// otherwise parallel tests interleave their swaps and restore each other's
/// hook.
#[cfg(test)]
pub(crate) mod test_support {
    use std::sync::{Mutex, MutexGuard};

    /// Serializes tests that mutate process-global state (the panic hook and
    /// the `INTENTD_TEST_PANIC_METHOD` env var) so parallel test threads
    /// cannot interleave hook swaps or env mutations.
    static GLOBAL_STATE: Mutex<()> = Mutex::new(());

    /// Hold the global-state lock for a hand-rolled swap (hook + env).
    pub(crate) fn lock_global_state() -> MutexGuard<'static, ()> {
        GLOBAL_STATE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Silence the default panic hook's stderr backtrace inside a scope; the
    /// guards rely on `catch_unwind`, not the hook, so behavior is unchanged.
    /// Holds the global-state lock for the duration so hook swaps never
    /// interleave. `f` must be synchronous (drive async work with
    /// `block_on`) so the lock is never held across an `await`.
    pub(crate) fn with_quiet_panics<T>(f: impl FnOnce() -> T) -> T {
        let _guard = lock_global_state();
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let out = f();
        std::panic::set_hook(prev);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{lock_global_state, with_quiet_panics};
    use super::*;

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

    // `guard_frame` is the single outbound chokepoint for the large-frame
    // warn, covering responses that bypass the router dispatcher (the
    // `host.*` fast path in particular — `host.exec` embeds untruncated
    // stdout). The warn throttle is process-global, so each test uses method
    // names unique to it.

    #[tokio::test]
    async fn guard_frame_warns_on_large_non_exempt_response() {
        let big = "x".repeat(crate::protocol::LARGE_MESSAGE_WARN_BYTES + 1);
        let lines = crate::protocol::test_capture::capture_events(|| {
            let frame =
                futures::executor::block_on(guard_frame("host.exec", Some(json!(1)), async {
                    Some(big)
                }));
            assert!(frame.is_some(), "frame must pass through unchanged");
        });
        assert_eq!(lines.len(), 1, "expected exactly one warn: {lines:?}");
        assert_eq!(lines[0].0, tracing::Level::WARN);
        assert!(lines[0].1.contains("large outbound JSON-RPC frame"));
        assert!(lines[0].1.contains("method=\"host.exec\""));
    }

    #[tokio::test]
    async fn guard_frame_does_not_warn_on_small_or_exempt_responses() {
        let big = "x".repeat(crate::protocol::LARGE_MESSAGE_WARN_BYTES + 1);
        let lines = crate::protocol::test_capture::capture_events(|| {
            // Small frame on a non-exempt method: under threshold, no warn.
            futures::executor::block_on(guard_frame("guard.small", Some(json!(1)), async {
                Some("ok".to_string())
            }));
            // Large frame on an exempt bulk-transfer method: no warn.
            futures::executor::block_on(guard_frame("file.readChunk", Some(json!(2)), async {
                Some(big)
            }));
        });
        assert!(lines.is_empty(), "unexpected warns: {lines:?}");
    }

    #[tokio::test]
    async fn guard_send_panic_on_request_sends_internal_error() {
        let (tx, mut rx) = crate::conn::outbound_channel();
        let open = with_quiet_panics(|| {
            futures::executor::block_on(guard_send("x.y", Some(json!(3)), &tx, async {
                panic!("boom");
            }))
        });
        assert!(open);
        // The internal-error frame travels on the priority lane.
        let frame = rx.priority.try_recv().expect("frame queued");
        let v: Value = serde_json::from_str(&frame).unwrap();
        assert_eq!(v["id"], json!(3));
        assert_eq!(v["error"]["code"], json!(-32603));
    }

    #[tokio::test]
    async fn guard_send_panic_on_notification_sends_nothing() {
        let (tx, mut rx) = crate::conn::outbound_channel();
        let open = with_quiet_panics(|| {
            futures::executor::block_on(guard_send("x.y", None, &tx, async {
                panic!("boom");
            }))
        });
        assert!(open);
        assert!(rx.priority.try_recv().is_err());
        assert!(rx.bulk.try_recv().is_err());
    }

    #[tokio::test]
    async fn guard_send_panic_on_notification_reports_closed_channel() {
        let (tx, rx) = crate::conn::outbound_channel();
        drop(rx);
        let open = with_quiet_panics(|| {
            futures::executor::block_on(guard_send("x.y", None, &tx, async {
                panic!("boom");
            }))
        });
        assert!(!open, "closed outbound channel must be reported");
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
    fn request_identity_coerces_invalid_id_types_to_null() {
        // JSON-RPC 2.0 response ids must be string/number/null; an invalid
        // request id (object/array/bool) still gets a response, with id null.
        for raw in [
            r#"{"jsonrpc":"2.0","id":{"k":1},"method":"a.b"}"#,
            r#"{"jsonrpc":"2.0","id":[1],"method":"a.b"}"#,
            r#"{"jsonrpc":"2.0","id":true,"method":"a.b"}"#,
        ] {
            let v: Value = serde_json::from_str(raw).unwrap();
            assert_eq!(request_identity(&v), (Some(Value::Null), "a.b".to_string()));
        }
    }

    // The injection hook is compiled out of release builds (`debug_assertions`
    // off ⇒ unconditional no-op), so this test only exists where it can pass.
    #[cfg(debug_assertions)]
    #[test]
    fn inject_panic_matches_env_list() {
        // Serialize with the other global-state tests: hold the lock across
        // both the env mutation and the hook swap (open-coded here because
        // `with_quiet_panics` takes the same lock).
        let _guard = lock_global_state();
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let prev_env = std::env::var("INTENTD_TEST_PANIC_METHOD").ok();
        std::env::set_var("INTENTD_TEST_PANIC_METHOD", "p.one , p.two");
        let hit = std::panic::catch_unwind(|| maybe_inject_panic("p.two")).is_err();
        let miss = std::panic::catch_unwind(|| maybe_inject_panic("p.three")).is_ok();
        match prev_env {
            Some(v) => std::env::set_var("INTENTD_TEST_PANIC_METHOD", v),
            None => std::env::remove_var("INTENTD_TEST_PANIC_METHOD"),
        }
        std::panic::set_hook(prev);
        assert!(hit, "listed method must panic");
        assert!(miss, "unlisted method must not panic");
    }
}
