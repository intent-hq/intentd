use std::time::Instant;

use rquickjs::{
    async_with, function::Async, AsyncContext, AsyncRuntime, CatchResultExt, Function, Promise,
};

use crate::{EvalOptions, HostFn, JsError, OUTER_SAFETY_MARGIN};

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
            const frame = reply == null ? { ok: true, value: null } : JSON.parse(reply);
            if (!frame.ok) {
                throw new Error(String(frame.error ?? "host call failed"));
            }
            return frame.value;
        };
    })();
"#;

/// Evaluate a snippet of user JavaScript inside a fresh QuickJS context.
///
/// User `code` is wrapped as `(async () => { <code> })()`, its resolved value
/// is JSON-serialized inside the runtime, and the result is returned as a
/// [`serde_json::Value`]. `undefined` maps to `Value::Null`.
///
/// A wall-clock budget from `opts.timeout` is enforced by:
///   1. A QuickJS interrupt handler that raises an uncatchable exception once
///      the deadline is reached — kills hot loops that never yield.
///   2. An outer [`tokio::time::timeout`] with a small safety margin — bounds
///      pending `await`s whose futures otherwise never resolve.
///
/// Every call builds a fresh [`AsyncRuntime`] and [`AsyncContext`], so no
/// state leaks between calls.
pub async fn eval(
    code: &str,
    opts: &EvalOptions,
    host: Option<HostFn>,
) -> Result<serde_json::Value, JsError> {
    let rt = AsyncRuntime::new().map_err(|e| JsError::Engine(e.to_string()))?;

    // Guard against pathological timeouts (e.g. CLI users passing an
    // overflowing millisecond value): `Instant + Duration` panics on overflow.
    let deadline = Instant::now()
        .checked_add(opts.timeout)
        .ok_or_else(|| JsError::Engine("timeout is too large: Instant overflow".into()))?;
    rt.set_interrupt_handler(Some(Box::new(move || Instant::now() >= deadline)))
        .await;

    if let Some(limit) = opts.memory_limit_bytes {
        rt.set_memory_limit(limit).await;
    }

    let ctx = AsyncContext::full(&rt)
        .await
        .map_err(|e| JsError::Engine(e.to_string()))?;

    let code_owned = code.to_string();
    let host_for_bind = host.clone();

    let inner: Result<Result<serde_json::Value, String>, ()> =
        tokio::time::timeout(opts.timeout + OUTER_SAFETY_MARGIN, async {
            let out: Result<serde_json::Value, String> = async_with!(ctx => |ctx| {
                if let Some(h) = host_for_bind {
                    bind_host(ctx.clone(), h).map_err(stringify_js_err)?;
                }
                run_user_code(ctx, &code_owned).await
            })
            .await;
            rt.idle().await;
            out
        })
        .await
        .map_err(|_| ());

    match inner {
        Ok(Ok(v)) => Ok(v),
        Ok(Err(msg)) => {
            // Interrupt-driven exceptions land here; if we've blown the budget
            // classify as Timeout rather than Runtime.
            if Instant::now() >= deadline {
                Err(JsError::Timeout {
                    ms: opts.timeout.as_millis() as u64,
                })
            } else {
                Err(JsError::Runtime(msg))
            }
        }
        Err(()) => Err(JsError::Timeout {
            ms: opts.timeout.as_millis() as u64,
        }),
    }
}

fn stringify_js_err<E: std::fmt::Display>(e: E) -> String {
    e.to_string()
}

fn bind_host<'js>(ctx: rquickjs::Ctx<'js>, host: HostFn) -> rquickjs::Result<()> {
    let host_arc = host;
    let host_raw = Function::new(
        ctx.clone(),
        Async(move |arg_json: String| {
            let h = host_arc.clone();
            async move {
                let value: serde_json::Value =
                    serde_json::from_str(&arg_json).unwrap_or(serde_json::Value::Null);
                let frame = match h(value).await {
                    Ok(v) => serde_json::json!({ "ok": true, "value": v }),
                    Err(msg) => serde_json::json!({ "ok": false, "error": msg }),
                };
                Ok::<String, rquickjs::Error>(frame.to_string())
            }
        }),
    )?;
    ctx.globals().set("__hostRaw", host_raw)?;
    ctx.eval::<(), _>(HOST_BRIDGE_JS)?;
    Ok(())
}

async fn run_user_code<'js>(
    ctx: rquickjs::Ctx<'js>,
    code: &str,
) -> Result<serde_json::Value, String> {
    // `undefined` and non-JSON-serializable results are surfaced as distinct
    // JSON envelopes so the host can tell "no return" from "unserializable
    // return" (which otherwise both become `undefined`).
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
        .map_err(|e| format!("{e}"))?;
    let promise: Promise<'js> = Promise::from_value(val).map_err(stringify_js_err)?;
    let envelope: String = promise
        .into_future()
        .await
        .catch(&ctx)
        .map_err(|e| format!("{e}"))?;
    let parsed: serde_json::Value =
        serde_json::from_str(&envelope).map_err(|e| format!("engine: bad result envelope: {e}"))?;
    match parsed.get("kind").and_then(|k| k.as_str()) {
        Some("undefined") => Ok(serde_json::Value::Null),
        Some("value") => Ok(parsed
            .get("json")
            .cloned()
            .unwrap_or(serde_json::Value::Null)),
        _ => Err("engine: unknown result envelope".into()),
    }
}
