//! Source lint: the settle-only shared-watch wait needs a stated reason.
//!
//! A shared watch registers with the OS off the caller's thread
//! (intent-hq/monorepo#1572), so a watcher test must wait for it before
//! mutating the watched tree. Under full-suite parallelism the group's
//! watcher creation can fail (inotify instance exhaustion) and be retried; the
//! registration then *settles as failed* until the retry re-registers it. A
//! wait that returns on "settled" returns at once in that window, the test
//! writes before any OS watch exists, and the awaited event never arrives —
//! the test hangs into nextest's kill. Three issues came from that one
//! pattern (intent-hq/intent#4845, #4852, #6128), each fixed one test at a
//! time, and intentd#2648 / #2655 moved about ten call sites by hand (#2648's
//! review found four more after the first pass).
//!
//! The live wait is now the default: `SubHandle::wait_established` and
//! `SharedWatchHub::wait_all_established` ride out a creation retry and panic
//! naming the root on a dead watch. The settle-only wait survives as
//! `SubHandle::wait_settled_even_if_failed`, for the few tests that expect a
//! registration to FAIL and then inspect it. This lint keeps it from being
//! picked by accident: it fails naming `file:line` for
//!
//! - every use of the identifier `wait_settled_even_if_failed` in any Rust
//!   file under `crates/` — test code included, since every caller is test
//!   code — other than its `fn` definition, unless a standalone
//!   `// settle-only-wait: allow — <reason>` comment sits on the line above
//!   the use or above the first line of its statement (so a method chain
//!   rustfmt breaks across lines can carry the marker above the receiver);
//! - such a marker with no reason;
//! - a stale marker: one on the line above no use.
//!
//! Comments and string literals are blanked by the shared lexer first, so a
//! mention in prose or a fixture never counts. Limit: the rule matches the
//! identifier only; a test polling `settled()` on a registration directly is
//! not seen.

use std::fs;

use intentd_test_support::source_lint::{
    lex, markers_by_line, rust_files, statement_line, workspace_root, Marker,
};

const TAG: &str = "settle-only-wait";
const METHOD: &str = "wait_settled_even_if_failed";

fn is_ident(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// Whether the identifier starting at `start` is preceded (across
/// whitespace) by the `fn` keyword, i.e. is the definition.
fn is_definition(chars: &[char], start: usize) -> bool {
    let mut end = start;
    while end > 0 && chars[end - 1].is_whitespace() {
        end -= 1;
    }
    let mut begin = end;
    while begin > 0 && is_ident(chars[begin - 1]) {
        begin -= 1;
    }
    chars[begin..end].iter().collect::<String>() == "fn"
}

/// Violations in one file, as `file:line: <what>` lines.
fn offenders_in(rel: &str, src: &str) -> Vec<String> {
    let lexed = lex(src);
    let markers = markers_by_line(src, &lexed.line_comments, TAG);
    let marker_at = |line: usize| markers.get(line).copied().unwrap_or(Marker::Absent);
    let chars: Vec<char> = lexed.blanked.chars().collect();
    let needle: Vec<char> = METHOD.chars().collect();
    let mut out = Vec::new();
    let mut covering_lines = Vec::new();
    let mut i = 0;
    while i + needle.len() <= chars.len() {
        let bounded = chars[i..i + needle.len()] == needle[..]
            && (i == 0 || !is_ident(chars[i - 1]))
            && chars.get(i + needle.len()).is_none_or(|c| !is_ident(*c));
        if !bounded {
            i += 1;
            continue;
        }
        let line = chars[..i].iter().filter(|c| **c == '\n').count() + 1;
        if !is_definition(&chars, i) {
            let candidates: Vec<usize> = [line, statement_line(&chars, i)]
                .into_iter()
                .filter(|l| *l > 1)
                .map(|l| l - 1)
                .collect();
            covering_lines.extend(candidates.iter().copied());
            let states: Vec<Marker> = candidates.iter().map(|l| marker_at(*l)).collect();
            if states.contains(&Marker::WithReason) {
                i += needle.len();
                continue;
            }
            if states.contains(&Marker::Malformed) {
                out.push(format!(
                    "{rel}:{line}: `// {TAG}: allow` marker above `{METHOD}` needs a reason \
                     (`// {TAG}: allow — <reason>`)"
                ));
            } else {
                out.push(format!(
                    "{rel}:{line}: `{METHOD}` returns on a FAILED registration; use \
                     `wait_established` (rides out a creation retry), or mark a test that \
                     expects the failure with `// {TAG}: allow — <reason>` on the line above"
                ));
            }
        }
        i += needle.len();
    }
    for (line, marker) in markers.iter().enumerate() {
        if *marker != Marker::Absent && !covering_lines.contains(&line) {
            out.push(format!(
                "{rel}:{line}: stale `// {TAG}: allow` marker: no `{METHOD}` use on the next line"
            ));
        }
    }
    out.sort();
    out
}

#[test]
fn settle_only_wait_is_marked_with_a_reason() {
    let root = workspace_root();
    let files = rust_files(&root.join("crates"));
    assert!(!files.is_empty(), "no Rust files under {}", root.display());
    let mut offenders = Vec::new();
    let mut uses = 0;
    for path in files {
        let rel = path
            .strip_prefix(&root)
            .unwrap_or(&path)
            .to_string_lossy()
            .replace('\\', "/");
        let text =
            fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        // Blanking only removes text, so a file whose raw text names neither
        // the method nor the marker tag can hold no use and no marker. Skipping
        // it keeps the lexer (tens of seconds over the whole tree in a debug
        // build) off the ~all files that are irrelevant.
        if !text.contains(METHOD) && !text.contains(TAG) {
            continue;
        }
        uses += lex(&text).blanked.matches(METHOD).count();
        offenders.extend(offenders_in(&rel, &text));
    }
    assert!(
        uses > 0,
        "`{METHOD}` no longer appears under crates/: retire or retarget this lint"
    );
    assert!(
        offenders.is_empty(),
        "settle-only shared-watch wait (intent-hq/intent#4845, #4852, #6128):\n{}",
        offenders.join("\n")
    );
}

#[test]
fn unmarked_use_is_flagged() {
    let src = "async fn t() {\n    sub.wait_settled_even_if_failed(LIVENESS).await;\n}\n";
    let got = offenders_in("a.rs", src);
    assert_eq!(got.len(), 1, "{got:?}");
    assert!(got[0].starts_with("a.rs:2: `wait_settled_even_if_failed` returns"));
}

#[test]
fn marked_use_passes() {
    let src = "async fn t() {\n    // settle-only-wait: allow — asserts the failure\n    \
               sub.wait_settled_even_if_failed(LIVENESS).await;\n}\n";
    assert!(offenders_in("a.rs", src).is_empty());
}

#[test]
fn marker_above_a_broken_chain_statement_passes() {
    let src =
        "async fn t() {\n    // settle-only-wait: allow — asserts the failure\n    sub\n        \
               .wait_settled_even_if_failed(LIVENESS)\n        .await;\n}\n";
    assert!(offenders_in("a.rs", src).is_empty());
}

#[test]
fn marker_without_reason_is_flagged() {
    let src = "async fn t() {\n    // settle-only-wait: allow\n    \
               sub.wait_settled_even_if_failed(LIVENESS).await;\n}\n";
    let got = offenders_in("a.rs", src);
    assert_eq!(got.len(), 1, "{got:?}");
    assert!(
        got[0].starts_with("a.rs:3:") && got[0].contains("needs a reason"),
        "{got:?}"
    );
}

#[test]
fn trailing_marker_does_not_count() {
    let src = "async fn t() {\n    \
               sub.wait_settled_even_if_failed(LIVENESS).await; // settle-only-wait: allow — x\n}\n";
    assert_eq!(offenders_in("a.rs", src).len(), 1);
}

#[test]
fn marker_two_lines_up_does_not_count() {
    let src = "async fn t() {\n    // settle-only-wait: allow — asserts the failure\n    let x = 1;\n    \
               sub.wait_settled_even_if_failed(LIVENESS).await;\n}\n";
    let got = offenders_in("a.rs", src);
    assert_eq!(got.len(), 2, "{got:?}");
    assert!(
        got.iter().any(|o| o.starts_with("a.rs:2: stale")),
        "{got:?}"
    );
    assert!(
        got.iter().any(|o| o.starts_with("a.rs:4: `wait_settled")),
        "{got:?}"
    );
}

#[test]
fn stale_marker_is_flagged() {
    let src = "async fn t() {\n    // settle-only-wait: allow — asserts the failure\n    \
               sub.wait_established(LIVENESS).await;\n}\n";
    let got = offenders_in("a.rs", src);
    assert_eq!(got.len(), 1, "{got:?}");
    assert!(got[0].starts_with("a.rs:2: stale"), "{got:?}");
}

#[test]
fn definition_comments_strings_and_longer_names_are_ignored() {
    let src = "impl S {\n    pub(crate) async fn wait_settled_even_if_failed(&self) {}\n}\n\
               /// Prefer this over `wait_settled_even_if_failed`.\n\
               // sub.wait_settled_even_if_failed(LIVENESS)\n\
               const S: &str = \"sub.wait_settled_even_if_failed(x)\";\n\
               fn f() { s.wait_settled_even_if_failed_v2(); s.my_wait_settled_even_if_failed(); }\n";
    let got = offenders_in("a.rs", src);
    assert!(got.is_empty(), "{got:?}");
}

#[test]
fn path_reference_counts_as_a_use() {
    let src = "fn f() {\n    let w = SubHandle::wait_settled_even_if_failed;\n}\n";
    assert_eq!(offenders_in("a.rs", src).len(), 1);
}
