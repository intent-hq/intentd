use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use rquickjs::{
    function::Async, object::Property, Array, AsyncContext, AsyncRuntime, CatchResultExt, Ctx,
    Function, IntoJs, Object, Promise, Value,
};

use crate::{
    EvalOptions, GuardedHostFn, HostAdmissionOutcome, HostCallId, HostFn, HostReply,
    HostTransferReceipt, JsError, PreparedHostTransfer, OUTER_SAFETY_MARGIN,
};

/// JS bridge exposed to user code when a host function is provided. It captures
/// the raw host function in a closure (so it never sits on `globalThis` where
/// user code could bypass the wrapper), rejects non-JSON-serializable arguments
/// with a clear `TypeError`, and turns a `{ok:false, error}` frame back into a
/// JS `Error`.
const HOST_BRIDGE_JS: &str = r#"
    (() => {
        const raw = globalThis.__hostRaw;
        delete globalThis.__hostRaw;
        globalThis.host = async function host(arg) {
            const stringified = JSON.stringify(arg === undefined ? null : arg);
            if (typeof stringified !== "string") {
                throw new TypeError("host(arg): argument is not JSON-serializable");
            }
            const reply = await raw(stringified);
            const frame = reply == null ? { ok: true, value: null } : reply;
            if (!frame.ok) {
                throw new Error(String(frame.error ?? "host call failed"));
            }
            return frame.value;
        };
    })();
"#;

/// Distinguish user-code failures (surfaced as [`JsError::Runtime`]) from
/// engine-internal failures (surfaced as [`JsError::Engine`]) inside the
/// per-eval runner. Kept private to this module.
enum RunErr {
    Runtime(String),
    Engine(String),
}

/// Evaluate a snippet of user JavaScript inside a fresh `QuickJS` context.
///
/// User `code` is wrapped as `(async () => { <code> })()`, its resolved value
/// is JSON-serialized inside the runtime, and the result is returned as a
/// [`serde_json::Value`]. `undefined` maps to `Value::Null`.
///
/// A wall-clock budget from `opts.timeout` is enforced by:
///   1. A `QuickJS` interrupt handler that raises an uncatchable exception once
///      the deadline is reached — kills hot loops that never yield.
///   2. An outer [`tokio::time::timeout`] with a small safety margin — bounds
///      pending `await`s whose futures otherwise never resolve.
///
/// Every call builds a fresh [`AsyncRuntime`] and [`AsyncContext`], so no
/// state leaks between calls.
///
/// # Errors
///
/// Returns a [`JsError`] when the runtime cannot be built, the script throws, the result cannot be serialized, or the timeout budget is exhausted.
pub async fn eval(
    code: &str,
    opts: &EvalOptions,
    host: Option<HostFn>,
) -> Result<serde_json::Value, JsError> {
    eval_inner(code, opts, host.map(ordinary_host), None).await
}

/// Evaluate with an interrupt flag for synchronous JavaScript execution.
/// Callers should also cancel the evaluation future to stop pending host calls.
///
/// # Errors
/// Returns the same errors as [`eval`], or a runtime cancellation error.
pub async fn eval_with_cancellation(
    code: &str,
    opts: &EvalOptions,
    host: Option<HostFn>,
    cancelled: Arc<AtomicBool>,
) -> Result<serde_json::Value, JsError> {
    eval_inner(code, opts, host.map(ordinary_host), Some(cancelled)).await
}

fn ordinary_host(h: HostFn) -> GuardedHostFn {
    Arc::new(move |value, _original| {
        // Legacy HostFn closures ran on first poll, not when the JS callback
        // created its promise. Only GuardedHostFn opts into synchronous capture.
        let h = h.clone();
        Box::pin(async move { HostReply::Ordinary(h(value).await) })
    })
}

/// Evaluate with an opt-in, typed admission for individual host replies.
///
/// Evaluation, encoding, and errors otherwise match [`eval`]. Successful host
/// transfer admits only that original JS promise, not later evaluation output.
/// Cancellation drops the original pending futures and completion slots.
///
/// # Errors
/// Returns the same [`JsError`] variants as [`eval`]. Refused host admission
/// rejects the JS host promise with a fixed non-private control error.
pub async fn eval_guarded(
    code: &str,
    opts: &EvalOptions,
    host: Option<GuardedHostFn>,
) -> Result<serde_json::Value, JsError> {
    eval_inner(code, opts, host, None).await
}

async fn eval_inner(
    code: &str,
    opts: &EvalOptions,
    host: Option<GuardedHostFn>,
    cancelled: Option<Arc<AtomicBool>>,
) -> Result<serde_json::Value, JsError> {
    let rt = AsyncRuntime::new().map_err(|e| JsError::Engine(e.to_string()))?;

    // Guard against pathological timeouts (e.g. CLI users passing an
    // overflowing millisecond value): `Instant + Duration` panics on overflow.
    let deadline = Instant::now()
        .checked_add(opts.timeout)
        .ok_or_else(|| JsError::Engine("timeout is too large: Instant overflow".into()))?;
    let interrupt = cancelled.clone();
    rt.set_interrupt_handler(Some(Box::new(move || {
        Instant::now() >= deadline
            || interrupt
                .as_ref()
                .is_some_and(|flag| flag.load(Ordering::Relaxed))
    })))
    .await;

    if let Some(limit) = opts.memory_limit_bytes {
        rt.set_memory_limit(limit).await;
    }

    let ctx = AsyncContext::full(&rt)
        .await
        .map_err(|e| JsError::Engine(e.to_string()))?;

    let code_owned = code.to_string();
    let host_for_bind = host.clone();

    let inner: Result<Result<serde_json::Value, RunErr>, ()> =
        tokio::time::timeout(opts.timeout.saturating_add(OUTER_SAFETY_MARGIN), async {
            let out: Result<serde_json::Value, RunErr> = ctx
                .async_with(async |ctx| {
                    if let Some(h) = host_for_bind {
                        bind_host(&ctx, h).map_err(|e| RunErr::Engine(stringify_js_err(e)))?;
                    }
                    run_user_code(ctx, &code_owned).await
                })
                .await;
            rt.idle().await;
            out
        })
        .await
        .map_err(|_| ());

    if cancelled
        .as_ref()
        .is_some_and(|flag| flag.load(Ordering::Relaxed))
    {
        return Err(JsError::Runtime("execution cancelled".into()));
    }
    match inner {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(RunErr::Runtime(msg))) => {
            // Interrupt-driven exceptions land here; if we've blown the budget
            // classify as Timeout rather than Runtime.
            if Instant::now() >= deadline {
                Err(JsError::Timeout {
                    ms: timeout_ms(opts.timeout),
                })
            } else {
                Err(JsError::Runtime(msg))
            }
        }
        Ok(Err(RunErr::Engine(msg))) => Err(JsError::Engine(msg)),
        Err(()) => Err(JsError::Timeout {
            ms: timeout_ms(opts.timeout),
        }),
    }
}

/// Millisecond count for `JsError::Timeout`, clamped to `u64::MAX` rather
/// than truncating the `u128` returned by `Duration::as_millis`.
fn timeout_ms(timeout: Duration) -> u64 {
    u64::try_from(timeout.as_millis()).unwrap_or(u64::MAX)
}

fn stringify_js_err<E: std::fmt::Display>(e: E) -> String {
    e.to_string()
}

fn bind_host(ctx: &rquickjs::Ctx<'_>, host: GuardedHostFn) -> rquickjs::Result<()> {
    let host_arc = host;
    let host_raw = Function::new(
        ctx.clone(),
        Async(move |arg_json: String| {
            // Allocate the original slot before invoking the trusted closure;
            // context capture is synchronous, before any host future is polled.
            let identity = Arc::new(());
            let (sender, mut receiver) = tokio::sync::oneshot::channel();
            let host_future = serde_json::from_str::<serde_json::Value>(&arg_json)
                .map(|value| host_arc(value, HostCallId(identity.clone())));
            async move {
                // A malformed argument frame is an engine-internal invariant
                // violation (the JS wrapper always calls `JSON.stringify`), so
                // surface it explicitly rather than silently coercing to null:
                // the bridge JS turns the `{ok:false, error}` frame back into a
                // JS `Error`, so the failure lands on the caller instead of
                // silently substituting `null`.
                let future = match host_future {
                    Ok(future) => future,
                    Err(e) => {
                        let frame = serde_json::json!({
                            "ok": false,
                            "error": format!("engine: host received non-JSON argument: {e}"),
                        });
                        return Ok::<HostFrame, rquickjs::Error>(HostFrame(frame.to_string()));
                    }
                };
                let encoded = match future.await {
                    HostReply::Ordinary(outcome) => encode_host_outcome(outcome),
                    HostReply::Guarded { outcome, admission } => {
                        // No encoding or JS is performed by the consuming action.
                        let packet = PreparedHostTransfer {
                            receipt: HostTransferReceipt(identity.clone()),
                            sender,
                            encoded: encode_host_outcome(outcome),
                        };
                        let outcome = admission.admit(packet).await;
                        // Admission has finished and released its guards. Never
                        // await a missing transfer, accept a foreign receipt, or
                        // fall back to the private outcome after refusal.
                        match outcome {
                            HostAdmissionOutcome::Transferred(receipt)
                                if Arc::ptr_eq(&receipt.0, &identity) =>
                            {
                                receiver.try_recv().unwrap_or_else(|_| refused_host_reply())
                            }
                            _ => refused_host_reply(),
                        }
                    }
                };
                Ok::<HostFrame, rquickjs::Error>(HostFrame(encoded))
            }
        }),
    )?;
    ctx.globals().set("__hostRaw", host_raw)?;
    ctx.eval::<(), _>(HOST_BRIDGE_JS)?;
    Ok(())
}

/// An encoded `{ok, value|error}` host frame on its way into JS.
///
/// The frame is decoded in Rust and built directly as JS values, so the
/// script heap holds only the final strings. Passing the JSON text and
/// calling `JSON.parse` in JS held the reply text, its UTF-8 copy, and the
/// parsed strings at once, which pushed multi-megabyte notes over the
/// runtime memory limit (intent-hq/intent#7688).
struct HostFrame(String);

impl<'js> IntoJs<'js> for HostFrame {
    fn into_js(self, ctx: &Ctx<'js>) -> rquickjs::Result<Value<'js>> {
        let encoded_len = self.0.len();
        let decoded = serde_json::from_str::<serde_json::Value>(&self.0);
        drop(self.0);
        let frame = match decoded {
            Ok(frame) => frame,
            Err(e) => {
                return error_frame(ctx, &format!("engine: malformed host reply frame: {e}"));
            }
        };
        let built = json_to_js(ctx, &frame);
        drop(frame);
        match built {
            Ok(value) => Ok(value),
            Err(rquickjs::Error::Exception) => {
                // Usually the memory limit. Drop the pending exception (the
                // partial values were released as the build unwound) and
                // reject with a bounded, actionable message instead.
                let _ = ctx.catch();
                error_frame(ctx, &oversized_reply_message(encoded_len))
            }
            Err(e) => Err(e),
        }
    }
}

fn oversized_reply_message(encoded_len: usize) -> String {
    format!(
        "host reply ({encoded_len} bytes of JSON) does not fit in the script memory limit; \
         if this call changes state it may already have been applied"
    )
}

fn error_frame<'js>(ctx: &Ctx<'js>, message: &str) -> rquickjs::Result<Value<'js>> {
    let obj = Object::new(ctx.clone())?;
    define_own(&obj, "ok", Value::new_bool(ctx.clone(), false))?;
    define_own(
        &obj,
        "error",
        rquickjs::String::from_str(ctx.clone(), message)?.into_value(),
    )?;
    Ok(obj.into_value())
}

/// Define `key` as an own enumerable, writable, configurable data property,
/// as `JSON.parse` does. Unlike `set`, this never runs setters a script may
/// have installed on `Object.prototype` / `Array.prototype`.
fn define_own<'js, K: rquickjs::IntoAtom<'js>>(
    obj: &Object<'js>,
    key: K,
    value: Value<'js>,
) -> rquickjs::Result<()> {
    obj.prop(
        key,
        Property::from(value).writable().enumerable().configurable(),
    )
}

/// Build `value` as JS values with `JSON.parse` semantics: object keys and
/// array indexes are defined as own data properties (so `__proto__` stays a
/// plain key and prototype setters never run), and
/// numbers become JS numbers. Depth is bounded by `serde_json`'s parser.
fn json_to_js<'js>(ctx: &Ctx<'js>, value: &serde_json::Value) -> rquickjs::Result<Value<'js>> {
    Ok(match value {
        serde_json::Value::Null => Value::new_null(ctx.clone()),
        serde_json::Value::Bool(b) => Value::new_bool(ctx.clone(), *b),
        serde_json::Value::Number(n) => match n.as_i64().and_then(|i| i32::try_from(i).ok()) {
            Some(i) => Value::new_int(ctx.clone(), i),
            None => Value::new_number(ctx.clone(), n.as_f64().unwrap_or(f64::NAN)),
        },
        serde_json::Value::String(s) => rquickjs::String::from_str(ctx.clone(), s)?.into_value(),
        serde_json::Value::Array(items) => {
            let array = Array::new(ctx.clone())?;
            for (idx, item) in items.iter().enumerate() {
                let idx = u32::try_from(idx).map_err(|_| rquickjs::Error::Exception)?;
                define_own(array.as_object(), idx, json_to_js(ctx, item)?)?;
            }
            array.into_value()
        }
        serde_json::Value::Object(map) => {
            let obj = Object::new(ctx.clone())?;
            for (key, item) in map {
                define_own(&obj, key.as_str(), json_to_js(ctx, item)?)?;
            }
            obj.into_value()
        }
    })
}

fn encode_host_outcome(outcome: Result<serde_json::Value, String>) -> String {
    match outcome {
        Ok(v) => serde_json::json!({ "ok": true, "value": v }),
        Err(msg) => serde_json::json!({ "ok": false, "error": msg }),
    }
    .to_string()
}

fn refused_host_reply() -> String {
    encode_host_outcome(Err("host reply admission refused".into()))
}

async fn run_user_code<'js>(
    ctx: rquickjs::Ctx<'js>,
    code: &str,
) -> Result<serde_json::Value, RunErr> {
    // The wrapper distinguishes two shapes internally: `{"kind":"undefined"}`
    // when the user code returned `undefined`, and `{"kind":"value","json":<v>}`
    // otherwise. A non-JSON-serializable return (e.g. a function) trips the
    // `JSON.stringify` check inside the wrapper and throws `TypeError`, which
    // lands on the caller as a `RunErr::Runtime`.
    let wrapped = format!(
        "(async () => {{ const __r = await (async () => {{ {code} }})(); \
         if (__r === undefined) return '{{\"kind\":\"undefined\"}}'; \
         const __s = JSON.stringify(__r); \
         if (typeof __s !== 'string') throw new TypeError('result is not JSON-serializable'); \
         return '{{\"kind\":\"value\",\"json\":' + __s + '}}'; }})()"
    );
    let val: rquickjs::Value<'js> = ctx
        .eval(wrapped.as_bytes())
        .catch(&ctx)
        .map_err(|e| RunErr::Runtime(format!("{e}")))?;
    let promise: Promise<'js> =
        Promise::from_value(val).map_err(|e| RunErr::Engine(stringify_js_err(e)))?;
    let envelope: String = promise
        .into_future()
        .await
        .catch(&ctx)
        .map_err(|e| RunErr::Runtime(format!("{e}")))?;
    // Envelope-parse and unknown-kind failures are engine-internal invariant
    // violations (the wrapper above is fixed): surface as `Engine` so the
    // caller does not confuse them with user-thrown JS errors.
    let parsed: serde_json::Value = serde_json::from_str(&envelope)
        .map_err(|e| RunErr::Engine(format!("engine: bad result envelope: {e}")))?;
    match parsed.get("kind").and_then(|k| k.as_str()) {
        Some("undefined") => Ok(serde_json::Value::Null),
        Some("value") => Ok(parsed
            .get("json")
            .cloned()
            .unwrap_or(serde_json::Value::Null)),
        other => Err(RunErr::Engine(format!(
            "engine: unknown result envelope kind: {other:?}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn ordinary_wrapper_defers_original_closure_until_polled() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let calls = Arc::new(AtomicUsize::new(0));
        let call_count = calls.clone();
        let original: HostFn = Arc::new(move |value| {
            call_count.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move { Ok(value) })
        });
        let wrapped = ordinary_host(original);
        let future = wrapped(serde_json::json!(7), HostCallId(Arc::new(())));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert!(matches!(future.await, HostReply::Ordinary(Ok(v)) if v == 7));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let unpolled = wrapped(serde_json::json!(8), HostCallId(Arc::new(())));
        drop(unpolled);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn timeout_ms_passes_through_small_values() {
        assert_eq!(timeout_ms(Duration::from_millis(1500)), 1500);
    }

    #[test]
    fn timeout_ms_clamps_instead_of_truncating() {
        // Duration::MAX yields ~5.9e35 ms — far past u64::MAX. An `as u64`
        // cast would silently truncate; the clamp must saturate instead.
        assert_eq!(timeout_ms(Duration::MAX), u64::MAX);
        // One past u64::MAX ms would truncate to 0 with a plain cast.
        let just_over = Duration::from_millis(u64::MAX).saturating_add(Duration::from_millis(1));
        assert_eq!(timeout_ms(just_over), u64::MAX);
    }
}
