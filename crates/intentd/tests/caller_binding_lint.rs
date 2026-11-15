//! Source lint: service-layer background tasks bind a `Caller`.
//!
//! Every `intent-services` capability gate refuses an unbound request
//! (`current_caller() == None`, fail-closed — multiplayer w3). `Caller` is a
//! `tokio::task_local!`, so a bare `tokio::spawn` drops the binding and the
//! spawned work reaches the gates unbound: the event fan-out, refreshers,
//! timers and finalisers would all be refused. Production code in
//! `intent-services` and the `intentd` composition root therefore spawns
//! daemon-internal work through `intent_core::spawn_daemon` (binds
//! `Caller::Daemon`) or re-establishes the request's caller with
//! `intent_core::with_caller`; this lint fails naming the `file:line` of every
//! bare `tokio::spawn(` in that production code. (The transport re-binds the
//! request caller in its own spawn sites and is covered by its e2e suite.)
//!
//! Test code is exempt as `source_lint::test_code_files` classifies it: whole
//! test files (`tests.rs`, `*_tests.rs`, `tests/` dirs, out-of-line test
//! modules such as `source_tests/`) and every `#[cfg(test)]`,
//! `#[cfg(all(test, …))]` and `#[cfg(any(test, feature = "test-hooks"))]`
//! item, which makes `test_support` test code too. A hand-rolled check that knew
//! only an exact `#[cfg(test)]` line ejected intentd#3391 from the merge
//! queue over a `#[cfg(all(test, debug_assertions))]` module (merge-group run
//! 43704464258).
//!
//! A spawn that provably never reaches the service layer may opt out with
//! `// caller-binding: allow — <reason>` on the line above or trailing the
//! spawn's line; the reason is required.

use intentd_test_support::source_lint::{
    classify_marker, test_code_files, tokenize, workspace_root, Marker, TestCodeText,
};

const TAG: &str = "caller-binding";
const SCANNED: &[&str] = &["crates/intent-services/src/", "crates/intentd/src/"];

/// The line of every `tokio::spawn(` outside test code in `src` with no
/// well-formed marker above it or trailing it.
fn bare_spawns(src: &str, whole: bool) -> Vec<usize> {
    if whole {
        return Vec::new();
    }
    let text = TestCodeText::new(src, false);
    let allowed = |line: usize| {
        text.lexed.line_comments.iter().any(|c| {
            let adjacent = if c.standalone {
                c.line + 1 == line
            } else {
                c.line == line
            };
            adjacent && classify_marker(&c.text, TAG) == Marker::WithReason
        })
    };
    tokenize(&text.chars)
        .windows(4)
        .filter_map(|window| {
            let [krate, sep, spawn, open] = window else {
                return None;
            };
            (krate.is("tokio")
                && sep.is("::")
                && spawn.is("spawn")
                && open.is("(")
                && !text.in_test(krate.start)
                && !allowed(krate.line))
            .then_some(krate.line)
        })
        .collect()
}

#[test]
fn service_layer_spawns_bind_a_caller() {
    let root = workspace_root();
    let mut scanned = 0;
    let mut offenders = Vec::new();
    for file in test_code_files(&root) {
        if !SCANNED.iter().any(|dir| file.rel.starts_with(dir)) {
            continue;
        }
        scanned += 1;
        for line in bare_spawns(&file.src, file.whole) {
            offenders.push(format!("{}:{line}", file.rel));
        }
    }
    assert!(scanned > 0, "no sources found under {SCANNED:?}");
    assert!(
        offenders.is_empty(),
        "bare `tokio::spawn(` in service-layer production code drops the task-local `Caller`; \
         use `intent_core::spawn_daemon` (or re-bind with `with_caller`), or annotate a spawn \
         that provably never reaches a capability gate with `// {TAG}: allow — <reason>`:\n  {}",
        offenders.join("\n  ")
    );
}

#[test]
fn markers_need_a_reason_and_adjacency() {
    let src = "\
fn a() {
    tokio::spawn(async move {});
    tokio::spawn(async move {}); // caller-binding: allow
    tokio::spawn(async move {}); // caller-binding: allow —
    tokio::spawn(async move {}); // caller-binding: allow — never touches the api
    // caller-binding: allow — never touches the api
    tokio::spawn(async move {});
    // caller-binding: allow — too far away

    tokio::spawn(async move {});
    // tokio::spawn(async move {});
    let s = \"tokio::spawn(\";
    spawn_daemon(async move {});
    tokio::spawn(with_caller(caller, future));
    ::tokio::spawn(
        async move {},
    );
}
";
    assert_eq!(bare_spawns(src, false), vec![2, 3, 4, 10, 14, 15]);
    assert!(bare_spawns(src, true).is_empty());
}

#[test]
fn test_modules_are_skipped_whatever_their_cfg() {
    let src = "\
fn prod() {
    tokio::spawn(async {});
}
#[cfg(test)]
mod tests {
    fn t() {
        tokio::spawn(async {});
    }
}
#[cfg(all(test, debug_assertions))]
mod debug_tests {
    fn t() {
        tokio::spawn(async {});
    }
}
#[cfg(any(test, feature = \"test-hooks\"))]
pub(crate) fn hook() { tokio::spawn(async {}); }
#[cfg(not(test))]
fn not_test() { tokio::spawn(async {}); }
fn after() { tokio::spawn(async {}); }
";
    assert_eq!(bare_spawns(src, false), vec![2, 19, 20]);
}
