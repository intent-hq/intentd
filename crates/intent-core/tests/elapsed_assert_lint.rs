//! Elapsed-time assertion lint.
//!
//! A test that times an operation and asserts the measured duration stayed
//! under a limit — `assert!(elapsed < Duration::from_secs(3))` — tests how
//! fast the machine is, not what the code does. On an overloaded CI runner
//! even a tiny step can take many seconds: on 2026-11-01 a merge-queue run of
//! intentd#2917 failed two such tests twice (`max_step: 8.75s` against a
//! limit already scaled to 3 s) and ejected the unrelated PR, costing about an
//! hour of queue time. Scaling the limit with `INTENTD_TEST_TIMEOUT_MULTIPLIER`
//! does not fix this shape: the step can still overrun any fixed multiple.
//! This source-scanning test fails, naming `file:line`, for every such
//! assertion in test code that carries no justified marker and is not
//! grandfathered by the committed baseline.
//!
//! - **Fix — assert what the limit stood for**, with no clock:
//!   - *work done*: count rows changed (SQLite's update hook,
//!     `total_changes()`), rows left in a cleanup at each step, batches or
//!     calls made, bytes read — and compare with the count the cheap case
//!     produces (intentd#2925);
//!   - *did not wait*: hold the resource the slow path would wait on (the
//!     write connection, a gate the fake blocks on until the test releases
//!     it) and require the operation to finish while it is still held; a
//!     regression then hangs into an `intentd_test_support::test_timeout(…)`
//!     hang guard instead of passing slowly;
//!   - *ordering*: subscribe to events before the action and assert which
//!     arrives first, or that a write got in while other work was still
//!     running;
//!   - *a timer of its own*: drive it under a paused tokio clock
//!     (`tokio::time::pause()`), where `tokio::time::Instant` is virtual.
//! - **What counts:** in test code (as
//!   `intentd_test_support::source_lint::test_code_files` defines it), the
//!   condition — first argument — of an `assert!` / `debug_assert!` /
//!   `prop_assert!` invocation containing a comparison whose smaller side is
//!   a measured duration: `<` / `<=` with it on the left, or `>` / `>=` with
//!   it on the right. A side is a measured duration when it calls
//!   `.elapsed()` or a `duration_since` variant, or names an identifier with
//!   an `_`-separated part `elapsed`, `waited` or `took` (`elapsed`,
//!   `fast_elapsed`, `waited_ms`) and no part naming a limit (`max`, `limit`,
//!   `budget`, `bound`, `cap`, `deadline`, `timeout`). A lower bound
//!   (`elapsed >= delay`) never counts: a slow machine only makes it larger.
//! - **Marker:** a standalone `// elapsed-assert: allow — <reason>` on the line
//!   above the comparison, or above the first line of the assertion, exempts
//!   it — for a measurement under a paused clock, or a limit no load can
//!   reach (say why). A marker without a reason never exempts and is itself
//!   reported.
//! - **Baseline** (`tests/elapsed_assert_baseline.txt`): one `<path> <count>`
//!   line per file that still has unmarked elapsed-time assertions, sorted by
//!   path. It only ratchets down: a file over its entry (or absent) fails
//!   naming every line; a file under its entry, or an entry whose file is
//!   gone, fails naming the exact line to write. To rewrite it after a bulk
//!   conversion, run the lint with `INTENTD_UPDATE_ELAPSED_ASSERT_BASELINE=1`;
//!   update mode refuses to raise or add an entry, or to record a malformed
//!   marker.
//! - **Limits:** only the three assertion macros are read — an
//!   `if elapsed > limit { panic!(…) }`, an `assert_eq!(elapsed < limit,
//!   true)` or an `.lt(&limit)` call is not seen, and neither is a measured
//!   value whose name has none of the three parts (`let d = start.elapsed();
//!   assert!(d < limit)`); a `<` right after `::` is taken as a turbofish and
//!   skipped with its closing `>`, while a generic type written in a
//!   condition outside a turbofish (a closure's `Vec<u8>`) reads as a
//!   comparison.

use std::collections::BTreeMap;
use std::fs;
use std::path::Path;

use intentd_test_support::source_lint::{
    markers_by_line, parse_count_baseline, statement_line, test_code_files, tokenize, Marker,
    TestCodeText, Token,
};

/// Opt-out tag: `// elapsed-assert: allow — <reason>`.
const TAG: &str = "elapsed-assert";

const BASELINE_FILE: &str = "crates/intent-core/tests/elapsed_assert_baseline.txt";

/// Environment variable that rewrites the baseline (lowering only).
const UPDATE_ENV: &str = "INTENTD_UPDATE_ELAPSED_ASSERT_BASELINE";

/// Assertion macros whose condition is read.
const ASSERT_MACROS: &[&str] = &["assert", "debug_assert", "prop_assert"];

/// `_`-separated identifier parts naming a measured duration.
const MEASURED_PARTS: &[&str] = &["elapsed", "waited", "took"];

/// `_`-separated identifier parts naming a limit rather than a measurement
/// (`max_elapsed`, `elapsed_budget`).
const LIMIT_PARTS: &[&str] = &[
    "max", "limit", "budget", "bound", "cap", "deadline", "timeout",
];

/// Methods returning a measured duration.
const MEASURING_CALLS: &[&str] = &[
    "elapsed",
    "duration_since",
    "saturating_duration_since",
    "checked_duration_since",
];

/// Tokens that end an operand of a comparison at the comparison's own depth.
const OPERAND_BOUNDARIES: &[&str] = &[
    "&&", "||", ",", ";", "=", "=>", "{", "}", "if", "else", "let", "return", "match", "in",
];

/// An unmarked elapsed-time assertion: its 1-based line and text.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Hit {
    line: usize,
    text: String,
    /// A consulted marker line carries a marker with no reason.
    marker_malformed: bool,
}

/// Does the operand `tokens` measure a duration?
fn is_measured(tokens: &[Token]) -> bool {
    tokens.iter().enumerate().any(|(k, t)| {
        if !t.is_ident() {
            return false;
        }
        let called = tokens.get(k + 1).is_some_and(|next| next.is("("))
            && k > 0
            && tokens[k - 1].is(".");
        if called && MEASURING_CALLS.contains(&t.text.as_str()) {
            return true;
        }
        let parts: Vec<String> = t
            .text
            .split('_')
            .map(str::to_ascii_lowercase)
            .collect();
        parts.iter().any(|p| MEASURED_PARTS.contains(&p.as_str()))
            && !parts.iter().any(|p| LIMIT_PARTS.contains(&p.as_str()))
    })
}

/// The operand tokens on each side of the comparison at `cmp` within
/// `cond`: back and forward to a boundary at the comparison's depth, or to
/// the bracket enclosing it.
fn operands(cond: &[Token], cmp: usize) -> (&[Token], &[Token]) {
    let depth = cond[cmp].depth;
    let stops = |t: &Token| {
        t.depth < depth || (t.depth == depth && OPERAND_BOUNDARIES.contains(&t.text.as_str()))
    };
    let start = cond[..cmp].iter().rposition(stops).map_or(0, |p| p + 1);
    let end = cond[cmp + 1..]
        .iter()
        .position(stops)
        .map_or(cond.len(), |p| cmp + 1 + p);
    (&cond[start..cmp], &cond[cmp + 1..end])
}

/// Indices (into `cond`) of every `<` opening a turbofish (right after
/// `::`) and of the `>` closing it, counting nested angles.
fn turbofish_angles(cond: &[Token]) -> Vec<usize> {
    let mut out = Vec::new();
    for (k, t) in cond.iter().enumerate() {
        if !(t.is("<") && k > 0 && cond[k - 1].is("::")) || out.contains(&k) {
            continue;
        }
        out.push(k);
        let mut depth = 1usize;
        for (j, inner) in cond.iter().enumerate().skip(k + 1) {
            if inner.is("<") {
                depth += 1;
            } else if inner.is(">") {
                depth -= 1;
                if depth == 0 {
                    out.push(j);
                    break;
                }
            }
        }
    }
    out
}

/// Indices (into `tokens`) of every comparison token in an assertion
/// condition whose smaller side is a measured duration.
fn elapsed_comparisons(tokens: &[Token]) -> Vec<usize> {
    let mut out = Vec::new();
    for (i, t) in tokens.iter().enumerate() {
        if !(t.is_ident() && ASSERT_MACROS.contains(&t.text.as_str())) {
            continue;
        }
        if i > 0 && (tokens[i - 1].is(".") || tokens[i - 1].is("fn")) {
            continue;
        }
        let (Some(bang), Some(open)) = (tokens.get(i + 1), tokens.get(i + 2)) else {
            continue;
        };
        if !bang.is("!") || !(open.is("(") || open.is("[") || open.is("{")) {
            continue;
        }
        let Some(close) = intentd_test_support::source_lint::matching_close(tokens, i + 2) else {
            continue;
        };
        let inner = open.depth + 1;
        let cond_end = tokens[i + 3..close]
            .iter()
            .position(|t| t.depth == inner && t.is(","))
            .map_or(close, |p| i + 3 + p);
        let cond = &tokens[i + 3..cond_end];
        let turbofish = turbofish_angles(cond);
        for (k, c) in cond.iter().enumerate() {
            let smaller_left = c.is("<") || c.is("<=");
            let smaller_right = c.is(">") || c.is(">=");
            if !(smaller_left || smaller_right) || turbofish.contains(&k) {
                continue;
            }
            let (left, right) = operands(cond, k);
            let smaller = if smaller_left { left } else { right };
            if !smaller.is_empty() && is_measured(smaller) {
                out.push(i + 3 + k);
            }
        }
    }
    out
}

/// The unmarked elapsed-time assertions in the test code of `src`: the whole
/// file when `whole_file_is_test`, otherwise only its `#[cfg(test)]` items.
fn scan_source(src: &str, whole_file_is_test: bool) -> Vec<Hit> {
    let text = TestCodeText::new(src, whole_file_is_test);
    let tokens = tokenize(&text.chars);
    let markers = markers_by_line(src, &text.lexed.line_comments, TAG);
    let marker = |line: usize| {
        line.checked_sub(1)
            .and_then(|above| markers.get(above))
            .copied()
            .unwrap_or(Marker::Absent)
    };
    let lines: Vec<&str> = src.lines().collect();
    let mut hits: Vec<Hit> = Vec::new();
    for cmp in elapsed_comparisons(&tokens) {
        let token = &tokens[cmp];
        if !text.in_test(token.start) {
            continue;
        }
        let consulted = [
            marker(token.line),
            marker(statement_line(&text.chars, token.start)),
        ];
        if consulted.contains(&Marker::WithReason) {
            continue;
        }
        hits.push(Hit {
            line: token.line,
            text: lines.get(token.line - 1).map_or("", |l| l.trim()).to_string(),
            marker_malformed: consulted.contains(&Marker::Malformed),
        });
    }
    hits
}

/// Unmarked elapsed-time assertions per crate file (zero-count files
/// included), keyed by workspace-relative path.
fn scan_tree(root: &Path) -> BTreeMap<String, Vec<Hit>> {
    test_code_files(root)
        .into_iter()
        .map(|file| {
            let hits = scan_source(&file.src, file.whole);
            (file.rel, hits)
        })
        .collect()
}

/// The failure report, empty when the tree matches the baseline and every
/// marker is well-formed.
fn report(found: &BTreeMap<String, Vec<Hit>>, baseline: &BTreeMap<String, usize>) -> String {
    let mut out = Vec::new();
    for (path, hits) in found {
        for hit in hits.iter().filter(|h| h.marker_malformed) {
            out.push(format!(
                "{path}:{}: {}\n  elapsed-assert marker is malformed: expected \
                 `// {TAG}: allow — <reason>` on its own line; the reason is required.",
                hit.line, hit.text
            ));
        }
        let count = hits.len();
        match baseline.get(path).copied() {
            Some(allowed) if count == allowed => {}
            None if count == 0 => {}
            Some(allowed) if count < allowed => out.push(if count == 0 {
                format!(
                    "{path}: no elapsed-time assertions left but the baseline allows {allowed}; \
                     the ratchet only moves down — remove its line from {BASELINE_FILE}: \
                     `{path} {allowed}`"
                )
            } else {
                format!(
                    "{path}: {count} elapsed-time assertion(s) but the baseline allows {allowed}; \
                     the ratchet only moves down — replace its line in {BASELINE_FILE} with: \
                     `{path} {count}`"
                )
            }),
            allowed => {
                let allowed = allowed.map_or_else(
                    || "the baseline has no entry for it".to_string(),
                    |n| format!("the baseline allows {n}"),
                );
                out.push(format!(
                    "{path}: {count} assertion(s) that a measured duration stayed under a limit, \
                     {allowed}:"
                ));
                for hit in hits {
                    out.push(format!("  {path}:{}: {}", hit.line, hit.text));
                }
                out.push(
                    "  fix: on a loaded CI runner any step can take seconds, so a limit on \
                     measured time fails unrelated PRs (intentd#2917). Assert what the limit \
                     stood for instead, with no clock:\n  \
                     - work done: rows changed (SQLite update hook, total_changes()), rows left \
                     at each step, batches or calls made — compared with the cheap case;\n  \
                     - did not wait: hold what the slow path would wait on (the write connection, \
                     a gate the fake blocks on) and require the operation to finish while it is \
                     held, under a test_timeout(…) hang guard;\n  \
                     - ordering: subscribe before the action and assert what arrives first;\n  \
                     - a timer of its own: run it under a paused tokio clock.\n  \
                     See the module doc of crates/intent-core/tests/elapsed_assert_lint.rs \
                     (precedent: intentd#2925)."
                        .to_string(),
                );
            }
        }
    }
    for (path, allowed) in baseline {
        if !found.contains_key(path) {
            out.push(format!(
                "{path}: no longer exists; remove its line from {BASELINE_FILE}: `{path} {allowed}`"
            ));
        }
    }
    out.join("\n")
}

#[test]
fn tests_do_not_assert_measured_durations_stay_under_a_limit() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join("..");
    let baseline_text = fs::read_to_string(root.join(BASELINE_FILE))
        .unwrap_or_else(|e| panic!("read {BASELINE_FILE}: {e}"));
    let baseline = parse_count_baseline(&baseline_text, "elapsed-time assertions")
        .unwrap_or_else(|e| panic!("{BASELINE_FILE}: {e}"));
    let found = scan_tree(&root);
    let scanned: usize = found.values().map(Vec::len).sum();
    assert!(
        found.contains_key("crates/intent-store/src/tests/write_acquire_retry.rs")
            && found.contains_key("crates/intentd/tests/common/mod.rs"),
        "test-code discovery no longer reaches the known test files ({} files, {scanned} hits)",
        found.len()
    );
    if std::env::var_os(UPDATE_ENV).is_some() {
        let raised: Vec<String> = found
            .iter()
            .filter(|(path, hits)| hits.len() > baseline.get(*path).copied().unwrap_or(0))
            .map(|(path, hits)| format!("{path} {}", hits.len()))
            .collect();
        let malformed = found.values().flatten().any(|h| h.marker_malformed);
        assert!(
            raised.is_empty() && !malformed,
            "update mode only lowers {BASELINE_FILE}; fix these first:\n{}",
            report(&found, &baseline)
        );
        let header = baseline_text.lines().take_while(|l| l.starts_with('#'));
        let entries = found
            .iter()
            .filter(|(_, hits)| !hits.is_empty())
            .map(|(path, hits)| format!("{path} {}", hits.len()));
        let lines: Vec<String> = header.map(str::to_string).chain(entries).collect();
        fs::write(root.join(BASELINE_FILE), lines.join("\n") + "\n").expect("write baseline");
        return;
    }
    let report = report(&found, &baseline);
    assert!(
        report.is_empty(),
        "elapsed-time assertions out of step with {BASELINE_FILE}:\n\n{report}\n"
    );
}

// ---- fixtures ---------------------------------------------------------------

fn lines_in(src: &str) -> Vec<usize> {
    scan_source(src, true).iter().map(|h| h.line).collect()
}

#[test]
fn an_upper_bound_on_measured_time_fails() {
    let src = "\
fn t() {
    assert!(elapsed < Duration::from_secs(3), \"took {elapsed:?}\");
    assert!(started.elapsed() < budget);
    assert!(
        fast_elapsed <= Duration::from_secs(2),
        \"slow\"
    );
    debug_assert!(Duration::from_secs(5) > note_elapsed);
    assert!(LIMIT >= t0.elapsed());
    assert!(waited < 200);
    assert!(took_ms.as_millis() < 500);
    prop_assert!(now.duration_since(start) < WINDOW);
    assert!(ok && elapsed < budget + margin, \"x\");
    assert!((elapsed < cap));
    assert![elapsed < cap];
    assert!(u128::from(waited_ms) <= bound.as_millis() + SLACK);
    assert!(started.elapsed().abs_diff(remainder) <= tick);
}
";
    assert_eq!(
        lines_in(src),
        vec![2, 3, 5, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17]
    );
}

#[test]
fn lower_bounds_and_non_measurements_never_count() {
    let src = "\
fn t() {
    assert!(elapsed >= Duration::from_millis(100));
    assert!(Duration::from_secs(1) <= started.elapsed());
    assert!(started.elapsed() > delay, \"fired early\");
    assert!(rows < 200_000);
    assert!(max_elapsed < budget);
    assert!(elapsed_budget <= cap);
    assert_eq!(elapsed < limit, true);
    assert!(calls.len() < 3, \"elapsed={elapsed:?} < limit\");
    // assert!(elapsed < Duration::from_secs(3));
    let s = \"assert!(elapsed < budget)\";
    if elapsed < budget { return; }
    assert!(Vec::<u8>::new().len() < took_count());
    fn assert(x: bool) {}
    assert!(HashMap::<String, Vec<u8>>::new().len() < took_count());
}
";
    assert_eq!(lines_in(src), Vec::<usize>::new());
}

#[test]
fn only_test_code_is_scanned_in_a_production_file() {
    let src = "\
fn production() {
    assert!(elapsed < Duration::from_secs(5));
}

#[cfg(test)]
mod tests {
    fn t() {
        assert!(elapsed < Duration::from_secs(5));
    }
}

#[cfg(all(test, unix))]
fn unix_only() { assert!(elapsed < cap); }
";
    assert_eq!(
        scan_source(src, false)
            .iter()
            .map(|h| h.line)
            .collect::<Vec<_>>(),
        vec![8, 13]
    );
    assert_eq!(lines_in(src), vec![2, 8, 13]);
}

#[test]
fn marker_above_the_line_or_statement_exempts_and_needs_a_reason() {
    let src = "\
fn t() {
    // elapsed-assert: allow — paused tokio clock: elapsed is virtual
    assert!(elapsed < Duration::from_secs(3));
    // elapsed-assert: allow — paused clock
    assert!(
        started.elapsed() < budget,
        \"x\"
    );
    assert!(
        // elapsed-assert: allow — paused clock
        waited < 200
    );
    // elapsed-assert: allow
    assert!(elapsed < cap);
    // elapsed-assert: allowance — no
    assert!(elapsed < cap);
    assert!(elapsed < cap); // elapsed-assert: allow — trailing does not count
}
";
    let hits = scan_source(src, true);
    assert_eq!(
        hits.iter()
            .map(|h| (h.line, h.marker_malformed))
            .collect::<Vec<_>>(),
        vec![(14, true), (16, true), (17, false)]
    );
}

#[test]
fn report_follows_the_ratchet() {
    let hit = |line: usize| Hit {
        line,
        text: "assert!(elapsed < cap);".to_string(),
        marker_malformed: false,
    };
    let found = BTreeMap::from([
        ("crates/a/tests/equal.rs".to_string(), vec![hit(3)]),
        ("crates/a/tests/over.rs".to_string(), vec![hit(4), hit(9)]),
        ("crates/a/tests/under.rs".to_string(), vec![hit(2)]),
        ("crates/a/tests/fixed.rs".to_string(), vec![]),
        ("crates/a/tests/new.rs".to_string(), vec![hit(7)]),
        ("crates/a/tests/clean.rs".to_string(), vec![]),
        (
            "crates/a/tests/marked.rs".to_string(),
            vec![Hit {
                marker_malformed: true,
                ..hit(5)
            }],
        ),
    ]);
    let baseline = BTreeMap::from([
        ("crates/a/tests/equal.rs".to_string(), 1),
        ("crates/a/tests/over.rs".to_string(), 1),
        ("crates/a/tests/under.rs".to_string(), 3),
        ("crates/a/tests/fixed.rs".to_string(), 2),
        ("crates/a/tests/gone.rs".to_string(), 1),
        ("crates/a/tests/marked.rs".to_string(), 1),
    ]);
    let report = report(&found, &baseline);
    for expected in [
        "crates/a/tests/over.rs: 2 assertion(s) that a measured duration stayed under a limit, the baseline allows 1:",
        "  crates/a/tests/over.rs:4: assert!(elapsed < cap);",
        "  crates/a/tests/over.rs:9: assert!(elapsed < cap);",
        "replace its line in crates/intent-core/tests/elapsed_assert_baseline.txt with: `crates/a/tests/under.rs 1`",
        "remove its line from crates/intent-core/tests/elapsed_assert_baseline.txt: `crates/a/tests/fixed.rs 2`",
        "crates/a/tests/new.rs: 1 assertion(s) that a measured duration stayed under a limit, the baseline has no entry for it:",
        "crates/a/tests/gone.rs: no longer exists",
        "crates/a/tests/marked.rs:5: assert!(elapsed < cap);\n  elapsed-assert marker is malformed",
    ] {
        assert!(report.contains(expected), "missing {expected:?} in:\n{report}");
    }
    assert!(!report.contains("equal.rs"), "{report}");
    assert!(!report.contains("clean.rs"), "{report}");
}
