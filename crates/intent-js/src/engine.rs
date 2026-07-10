use std::time::Instant;

use rquickjs::{
    async_with, function::Async, AsyncContext, AsyncRuntime, CatchResultExt, Function, Promise,
};

use crate::{EvalOptions, HostFn, JsError, OUTER_SAFETY_MARGIN};

/// JS bridge exposed to user code when a host function is provided. It hides
/// the JSON stringification round-trip and turns a `{ok:false, error}` frame
/// back into a JS `Error`.
const HOST_BRIDGE_JS: &str = r#"
    globalThis.host = async function host(arg) {
        const raw = await globalThis.__hostRaw(
            JSON.stringify(arg === undefined ? null : arg)
        );
        const frame = raw == null ? { ok: true, value: null } : JSON.parse(raw);
        if (!frame.ok) {
            throw new Error(String(frame.error ?? "host call failed"));
        }
        return frame.value;
    };
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

    let deadline = Instant::now() + opts.timeout;
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
    let wrapped = format!(
        "(async () => {{ const __r = await (async () => {{ {code} }})(); return __r === undefined ? null : JSON.stringify(__r); }})()"
    );
    let val: rquickjs::Value<'js> = ctx
        .eval(wrapped.as_bytes())
        .catch(&ctx)
        .map_err(|e| format!("{e}"))?;
    let promise: Promise<'js> = Promise::from_value(val).map_err(stringify_js_err)?;
    let s: Option<String> = promise
        .into_future()
        .await
        .catch(&ctx)
        .map_err(|e| format!("{e}"))?;
    Ok(match s {
        Some(s) => serde_json::from_str(&s).unwrap_or(serde_json::Value::Null),
        None => serde_json::Value::Null,
    })
}
