//! `host(arg)` refuses values JSON cannot represent instead of letting
//! `JSON.stringify` silently turn them into `null` or drop them.
//!
//! Before this check a computed `NaN` page size reached `ws.note.read` as an
//! absent option and read the whole note (intentd#2800). Every binding shares
//! this bridge, so the refusal lives here rather than in each binding.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use intent_js::{eval, EvalOptions, HostFn};
use serde_json::json;

/// Echo host that counts how often the daemon side was reached.
fn echo_host() -> (HostFn, Arc<AtomicUsize>) {
    let calls = Arc::new(AtomicUsize::new(0));
    let counter = calls.clone();
    let host: HostFn = Arc::new(move |arg| {
        counter.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { Ok(arg) })
    });
    (host, calls)
}

/// Run `prelude; host(<expr>)` and return the caught error as
/// `"<name>: <message>"`, or `"accepted"` when the call went through.
async fn host_outcome(prelude: &str, expr: &str) -> (String, usize) {
    let (host, calls) = echo_host();
    let code = format!(
        "{prelude}\ntry {{ await host({expr}); return 'accepted'; }} \
         catch (e) {{ return e.name + ': ' + e.message; }}"
    );
    let out = eval(&code, &EvalOptions::default(), Some(host))
        .await
        .expect("script itself must not fail");
    let text = out.as_str().expect("outcome is a string").to_string();
    (text, calls.load(Ordering::SeqCst))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unrepresentable_values_are_refused_with_their_path() {
    let cases: &[(&str, &str, &str)] = &[
        (
            "",
            "{ method: 'note.read', args: { id: 'n', maxLines: NaN } }",
            "note.read: args.maxLines is NaN",
        ),
        (
            "",
            "{ method: 'x.y', args: { n: Infinity } }",
            "x.y: args.n is Infinity",
        ),
        ("", "{ args: { n: -Infinity } }", "args.n is -Infinity"),
        ("", "{ a: [1, 2, Infinity] }", "a[2] is Infinity"),
        ("", "{ 'a-b': { c: NaN } }", "[\"a-b\"].c is NaN"),
        ("", "NaN", "argument is NaN"),
        ("", "[NaN]", "[0] is NaN"),
        ("", "{ n: new Number(5) }", "n is a boxed Number"),
        ("", "{ s: new String('s') }", "s is a boxed String"),
        ("", "{ b: [new Boolean(false)] }", "b[0] is a boxed Boolean"),
        ("", "{ n: Object(10n) }", "n is a boxed BigInt"),
        ("", "{ s: Object(Symbol('s')) }", "s is a boxed Symbol"),
        ("", "{ args: { cb: () => 1 } }", "args.cb is a function"),
        (
            "",
            "{ list: [function named() {}] }",
            "list[0] is a function",
        ),
        ("", "{ s: Symbol('s') }", "s is a symbol"),
        ("", "[1, Symbol.iterator]", "[1] is a symbol"),
        ("", "{ n: 10n }", "n is a BigInt"),
        ("", "10n", "argument is a BigInt"),
        ("", "{ d: { toJSON: () => NaN } }", "d is NaN"),
        // A shared (non-cyclic) reference must not confuse the reported path.
        (
            "const s = { ok: 1 };",
            "{ a: s, b: { c: s, d: NaN } }",
            "b.d is NaN",
        ),
    ];
    for (prelude, expr, expected) in cases {
        let (text, calls) = host_outcome(prelude, expr).await;
        assert!(
            text.starts_with("TypeError: host(arg): "),
            "{expr}: expected a TypeError from host(), got {text:?}"
        );
        assert!(
            text.contains(expected),
            "{expr}: expected {expected:?} in {text:?}"
        );
        assert!(
            text.contains("JSON cannot represent"),
            "{expr}: message should explain why, got {text:?}"
        );
        assert_eq!(calls, 0, "{expr}: host must not be reached");
    }
}

/// User code cannot disable the check by patching globals the bridge uses.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refusal_survives_patched_globals() {
    let (host, calls) = echo_host();
    // The eval result encoder also reads `JSON.stringify`, so restore it
    // before returning.
    let out = eval(
        "const realStringify = JSON.stringify; \
         Number.prototype.valueOf = () => 1; \
         globalThis.WeakMap = function () {}; \
         JSON.stringify = () => '{}'; \
         let text; \
         try { await host({ a: new Number(1), b: NaN }); text = 'accepted'; } \
         catch (e) { text = e.name + ': ' + e.message; } \
         JSON.stringify = realStringify; \
         return text;",
        &EvalOptions::default(),
        Some(host),
    )
    .await
    .expect("script itself must not fail");
    let text = out.as_str().expect("outcome is a string");
    assert!(text.contains("a is a boxed Number"), "got {text:?}");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn null_undefined_and_to_json_keep_their_serialization() {
    let (host, calls) = echo_host();
    let out = eval(
        "return [\
           await host({ a: undefined, b: null, c: [undefined, null, 1], \
                        d: new Date(0), e: { f: 'x', g: [] }, h: -0, i: 1.5 }), \
           await host(undefined), \
           await host(), \
           await host(null), \
         ];",
        &EvalOptions::default(),
        Some(host),
    )
    .await
    .expect("representable values pass");
    assert_eq!(
        out,
        json!([
            {
                "b": null,
                "c": [null, null, 1],
                "d": "1970-01-01T00:00:00.000Z",
                "e": { "f": "x", "g": [] },
                "h": 0,
                "i": 1.5
            },
            null,
            null,
            null
        ])
    );
    assert_eq!(calls.load(Ordering::SeqCst), 4);
}

/// Cycles still surface as a `TypeError` from the engine, as before.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cycles_still_throw() {
    let (text, calls) = host_outcome("const o = {}; o.self = o;", "o").await;
    assert!(text.starts_with("TypeError"), "got {text:?}");
    assert_eq!(calls, 0);
}
