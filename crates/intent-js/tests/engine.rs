//! Behaviour tests for the [`intent_js`] spike.
//!
//! Together these exercises cover the acceptance criteria in the WSAPI-1
//! task note: happy path, thrown JS error surfaced as `Err`, infinite-loop
//! interrupted by the wall-clock timeout, pending-promise timeout, and
//! per-execution isolation of globals.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use intent_js::{eval, BoxFuture, EvalOptions, HostFn, JsError, DEFAULT_MEMORY_LIMIT_BYTES};

fn short_opts(ms: u64) -> EvalOptions {
    EvalOptions {
        timeout: Duration::from_millis(ms),
        memory_limit_bytes: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn happy_path_returns_json() {
    let out = eval(
        "return { hello: 'world', n: 1 + 2 };",
        &EvalOptions::default(),
        None,
    )
    .await
    .expect("happy path succeeds");
    assert_eq!(out["hello"], "world");
    assert_eq!(out["n"], 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn returned_undefined_maps_to_null() {
    let out = eval("/* no return */", &EvalOptions::default(), None)
        .await
        .expect("no-return should succeed");
    assert!(out.is_null(), "expected null, got {out}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn thrown_js_error_is_returned_as_err() {
    let err = eval("throw new Error('boom');", &EvalOptions::default(), None)
        .await
        .expect_err("throw should error");
    match err {
        JsError::Runtime(msg) => assert!(msg.contains("boom"), "got: {msg}"),
        other => panic!("expected Runtime, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hot_loop_is_killed_by_timeout() {
    let started = Instant::now();
    // Test-level fail-safe: if the interrupt handler regresses, the test must
    // fail fast rather than hang CI on the very mechanism under test.
    let err = tokio::time::timeout(
        Duration::from_secs(10),
        eval("while (true) {}", &short_opts(150), None),
    )
    .await
    .expect("fail-safe: eval must return before the test-level timeout")
    .expect_err("hot loop should timeout");
    assert!(matches!(err, JsError::Timeout { .. }), "got {err:?}");
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "took too long: {:?}",
        started.elapsed()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_promise_is_killed_by_timeout() {
    let host: HostFn = Arc::new(
        |_arg| -> BoxFuture<'static, Result<serde_json::Value, String>> {
            Box::pin(async move {
                // Sleep far longer than the wall clock — the outer timeout must
                // still reclaim control.
                tokio::time::sleep(Duration::from_secs(30)).await;
                Ok(serde_json::json!(null))
            })
        },
    );
    let started = Instant::now();
    // Test-level fail-safe: if the outer timeout regresses, the 30 s host
    // sleep must not hang CI on the very mechanism under test.
    let err = tokio::time::timeout(
        Duration::from_secs(10),
        eval("return await host(null);", &short_opts(150), Some(host)),
    )
    .await
    .expect("fail-safe: eval must return before the test-level timeout")
    .expect_err("pending await should timeout");
    assert!(matches!(err, JsError::Timeout { .. }), "got {err:?}");
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "took too long: {:?}",
        started.elapsed()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn overlarge_timeout_errors_instead_of_panicking() {
    // A pathological budget must be rejected up front (`Instant` overflow),
    // not panic inside `Instant + Duration` / `Duration + Duration`.
    let opts = EvalOptions {
        timeout: Duration::MAX,
        memory_limit_bytes: None,
    };
    let err = eval("return 1;", &opts, None)
        .await
        .expect_err("overlarge timeout should be rejected");
    match err {
        JsError::Engine(msg) => assert!(msg.contains("overflow"), "got: {msg}"),
        other => panic!("expected Engine, got {other:?}"),
    }
}

#[test]
fn default_memory_limit_is_capped() {
    // Untrusted-ish agent code must not run with unlimited memory by default;
    // `None` (unlimited) is only reachable by explicit opt-out.
    assert_eq!(
        EvalOptions::default().memory_limit_bytes,
        Some(DEFAULT_MEMORY_LIMIT_BYTES)
    );
    assert_eq!(DEFAULT_MEMORY_LIMIT_BYTES, 64 * 1024 * 1024);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn memory_hog_is_killed_by_default_limit() {
    // Repeated string doubling blows past the 64 MB default cap within a few
    // dozen iterations — long before the 30 s wall-clock budget.
    let started = Instant::now();
    // Test-level fail-safe: if the memory cap regresses, the hog loop would
    // otherwise run until the 30 s wall clock — fail fast instead.
    let err = tokio::time::timeout(
        Duration::from_secs(10),
        eval(
            "let s = 'x'; while (true) { s += s; } return s.length;",
            &EvalOptions::default(),
            None,
        ),
    )
    .await
    .expect("fail-safe: eval must return before the test-level timeout")
    .expect_err("memory hog must be rejected by the default cap");
    assert!(matches!(err, JsError::Runtime(_)), "got {err:?}");
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "memory cap should trip well before the wall clock: {:?}",
        started.elapsed()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sequential_evals_do_not_share_globals() {
    let first = eval(
        "globalThis.__leak = 'from first call'; return globalThis.__leak;",
        &EvalOptions::default(),
        None,
    )
    .await
    .expect("first eval");
    assert_eq!(first, "from first call");

    let second = eval(
        "return typeof globalThis.__leak;",
        &EvalOptions::default(),
        None,
    )
    .await
    .expect("second eval");
    assert_eq!(
        second, "undefined",
        "state must not leak across calls, got {second}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_host_call_returns_json_from_tokio_future() {
    let counter = Arc::new(AtomicUsize::new(0));
    let counter_for_host = counter.clone();
    let host: HostFn = Arc::new(move |arg| {
        let counter = counter_for_host.clone();
        Box::pin(async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            let n = counter.fetch_add(1, Ordering::SeqCst) as u64;
            Ok(serde_json::json!({ "echo": arg, "call": n }))
        })
    });

    let out = eval(
        "const a = await host({ n: 41 }); const b = await host('x'); return [a, b];",
        &EvalOptions::default(),
        Some(host),
    )
    .await
    .expect("async host call succeeds");
    assert_eq!(out[0]["echo"]["n"], 41);
    assert_eq!(out[0]["call"], 0);
    assert_eq!(out[1]["echo"], "x");
    assert_eq!(out[1]["call"], 1);
    assert_eq!(counter.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_error_surfaces_as_js_error() {
    let host: HostFn = Arc::new(|_| Box::pin(async { Err("no thanks".to_string()) }));
    let err = eval(
        "try { await host(null); return 'unreachable'; } catch (e) { throw new Error('caught:' + e.message); }",
        &EvalOptions::default(),
        Some(host),
    )
    .await
    .expect_err("host rejection should propagate");
    match err {
        JsError::Runtime(msg) => {
            assert!(msg.contains("caught:no thanks"), "got: {msg}");
        }
        other => panic!("expected Runtime, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_json_serializable_return_throws() {
    let err = eval("return () => 1;", &EvalOptions::default(), None)
        .await
        .expect_err("returning a function must fail");
    match err {
        JsError::Runtime(msg) => assert!(
            msg.contains("JSON-serializable") || msg.contains("TypeError"),
            "expected serialization error, got: {msg}"
        ),
        other => panic!("expected Runtime, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn host_raw_is_removed_from_globals() {
    let host: HostFn = Arc::new(|_| Box::pin(async { Ok(serde_json::json!(1)) }));
    let out = eval(
        "return typeof globalThis.__hostRaw;",
        &EvalOptions::default(),
        Some(host),
    )
    .await
    .expect("eval succeeds");
    assert_eq!(out, "undefined", "raw bridge must not leak onto globalThis");
}
