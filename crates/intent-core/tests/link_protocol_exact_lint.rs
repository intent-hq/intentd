//! Exact link-protocol assertion lint.
//!
//! Two node-link peers agree on the highest protocol both speak, so a dial
//! in a test agrees on this build's `NODE_LINK_PROTOCOLS.max`, which rises
//! with every protocol bump. A test about an older feature that asserts
//! `assert_eq!(dialed.link.protocol(), HOMES_PROTOCOL)` passes until the
//! next bump and then fails, in a change that never touched the feature:
//! intentd#3662 raised the range to 20, and two homes tests in
//! `crates/intentd/src/executor/tests/homes.rs` pinned to `HOMES_PROTOCOL`
//! (19) failed CI's coverage-changed run 45036488346 after about 45 minutes,
//! with 273 tests left unrun behind fail-fast (fixed in `b54fa0a6`). This
//! source-scanning test fails, naming `file:line`, for every such assertion
//! in test code that carries no justified marker.
//!
//! - **Fix:** assert the feature's floor with
//!   `intentd_test_support::assert_link_supports(agreed, FEATURE_PROTOCOL)`.
//!   A test about the newest version compares with `NODE_LINK_PROTOCOLS.max`
//!   instead, which moves with the bump.
//! - **What counts:** in test code (as
//!   `intentd_test_support::source_lint::test_code_files` defines it), an
//!   `assert_eq!` / `assert_ne!` (also `debug_` and `prop_` forms) whose two
//!   first arguments are an agreed-protocol read and a named feature
//!   constant, in either order; and a `==` / `!=` with those two operands in
//!   the condition of an `assert!` / `debug_assert!` / `prop_assert!`. An
//!   agreed-protocol read is an operand ending in `.protocol`,
//!   `.protocol()` or `.last_protocol()` (`dialed.welcome.protocol`,
//!   `dialed.link.protocol()`, `head.last_protocol()`). A named feature
//!   constant is a bare path whose last segment is an upper-case name ending
//!   in `_PROTOCOL` (`HOMES_PROTOCOL`, `node_link::SECRETS_PROTOCOL`).
//!   Frame floors (`frame.min_protocol()`), range bounds
//!   (`NODE_LINK_PROTOCOLS.max`) and literals never count.
//! - **Marker:** a standalone `// link-protocol-exact: allow — <reason>` on
//!   the line above the assertion, or above the line of the comparison,
//!   exempts it — for a test that caps the offered range at the feature's
//!   version, where the exact match is the point (say so). A marker without
//!   a reason never exempts and is itself reported.
//! - **Limits:** the read must be written in the assertion: `let agreed =
//!   link.protocol(); assert_eq!(agreed, HOMES_PROTOCOL)` is not seen, and
//!   neither is a read with a trailing `.unwrap()`, a constant behind `&`,
//!   or an `if` that panics.
//!
//! A second test keeps the protocol range where `scripts/changed-tests.sh`
//! sees it: `NODE_LINK_PROTOCOLS` must be defined in `intent-core`, a
//! propagation root (`rdeps_roots`) whose src changes select the tests of
//! every crate depending on it — the node-link and transport crates and
//! intentd's binary, which holds the executor tests. Moved to another crate,
//! a range bump would run only that crate's own tests.

use std::collections::BTreeMap;

use intentd_test_support::source_lint::{
    markers_by_line, matching_close, test_code_files, tokenize, Marker, TestCodeFile, TestCodeText,
    Token,
};

/// Opt-out tag: `// link-protocol-exact: allow — <reason>`.
const TAG: &str = "link-protocol-exact";

/// Macros whose first two arguments are compared for equality.
const EQ_MACROS: &[&str] = &[
    "assert_eq",
    "assert_ne",
    "debug_assert_eq",
    "debug_assert_ne",
    "prop_assert_eq",
    "prop_assert_ne",
];

/// Macros whose first argument is a condition.
const COND_MACROS: &[&str] = &["assert", "debug_assert", "prop_assert"];

/// Members that read the protocol a link agreed on.
const AGREED_READS: &[&str] = &["protocol", "last_protocol"];

/// Tokens that end an operand of `==` / `!=` at the comparison's own depth.
const OPERAND_BOUNDARIES: &[&str] = &[
    "&&", "||", ",", ";", "=", "=>", "==", "!=", "<", ">", "<=", ">=", "{", "}", "if", "else",
    "let", "return", "match", "in",
];

/// The suffix of a named feature constant.
const FEATURE_SUFFIX: &str = "_PROTOCOL";

/// An unmarked exact-match assertion: its 1-based line and text.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Hit {
    line: usize,
    text: String,
    /// A consulted marker line carries a marker with no reason.
    marker_malformed: bool,
}

/// Is `op` a bare path naming a feature constant (`HOMES_PROTOCOL`,
/// `intent_core::node_link::HOMES_PROTOCOL`)?
fn is_feature_constant(op: &[Token]) -> bool {
    if op.len().is_multiple_of(2) {
        return false;
    }
    let path = op
        .iter()
        .enumerate()
        .all(|(k, t)| if k % 2 == 0 { t.is_ident() } else { t.is("::") });
    let name = &op[op.len() - 1].text;
    path && name.len() > FEATURE_SUFFIX.len()
        && name.ends_with(FEATURE_SUFFIX)
        && name
            .chars()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
}

/// Does `op` end in a read of the agreed protocol (`.protocol`,
/// `.protocol()`, `.last_protocol()`)?
fn is_agreed_read(op: &[Token]) -> bool {
    let n = op.len();
    let member = if n >= 2 && op[n - 2].is("(") && op[n - 1].is(")") {
        n - 2
    } else {
        n
    };
    member >= 2
        && op[member - 2].is(".")
        && op[member - 1].is_ident()
        && AGREED_READS.contains(&op[member - 1].text.as_str())
}

/// Is one operand an agreed read and the other a feature constant?
fn exact_pair(a: &[Token], b: &[Token]) -> bool {
    (is_agreed_read(a) && is_feature_constant(b)) || (is_feature_constant(a) && is_agreed_read(b))
}

/// The operands on each side of the comparison at `cmp` within `cond`.
fn operands(cond: &[Token], cmp: usize) -> (&[Token], &[Token]) {
    let depth = cond[cmp].depth;
    let stops = |k: usize| {
        let t = &cond[k];
        t.depth < depth || (t.depth == depth && OPERAND_BOUNDARIES.contains(&t.text.as_str()))
    };
    let start = (0..cmp).rev().find(|&k| stops(k)).map_or(0, |p| p + 1);
    let end = (cmp + 1..cond.len())
        .find(|&k| stops(k))
        .unwrap_or(cond.len());
    (&cond[start..cmp], &cond[cmp + 1..end])
}

/// `args` split at the commas at depth `depth`.
fn split_args(args: &[Token], depth: usize) -> Vec<&[Token]> {
    let mut out = Vec::new();
    let mut from = 0;
    for (k, t) in args.iter().enumerate() {
        if t.depth == depth && t.is(",") {
            out.push(&args[from..k]);
            from = k + 1;
        }
    }
    out.push(&args[from..]);
    out
}

/// `(macro token, hit token)` indices of every exact-match assertion.
fn exact_matches(tokens: &[Token]) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    for (i, t) in tokens.iter().enumerate() {
        let eq = EQ_MACROS.contains(&t.text.as_str());
        if !(t.is_ident() && (eq || COND_MACROS.contains(&t.text.as_str()))) {
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
        let Some(close) = matching_close(tokens, i + 2) else {
            continue;
        };
        let args = split_args(&tokens[i + 3..close], open.depth + 1);
        if eq {
            if args.len() >= 2 && exact_pair(args[0], args[1]) {
                out.push((i, i));
            }
            continue;
        }
        let cond = args[0];
        for (k, c) in cond.iter().enumerate() {
            if !(c.is("==") || c.is("!=")) {
                continue;
            }
            let (left, right) = operands(cond, k);
            if exact_pair(left, right) {
                out.push((i, i + 3 + k));
            }
        }
    }
    out
}

/// The unmarked exact-match assertions in the test code of `src`: the whole
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
    let mut hits = Vec::new();
    for (mac, at) in exact_matches(&tokens) {
        let (mac, at) = (&tokens[mac], &tokens[at]);
        if !text.in_test(mac.start) {
            continue;
        }
        let consulted = [marker(mac.line), marker(at.line)];
        if consulted.contains(&Marker::WithReason) {
            continue;
        }
        hits.push(Hit {
            line: at.line,
            text: lines.get(at.line - 1).map_or("", |l| l.trim()).to_string(),
            marker_malformed: consulted.contains(&Marker::Malformed),
        });
    }
    hits
}

/// Unmarked exact-match assertions per file (zero-count files included),
/// keyed by workspace-relative path.
fn scan_tree(files: &[TestCodeFile]) -> BTreeMap<String, Vec<Hit>> {
    files
        .iter()
        .map(|file| (file.rel.clone(), scan_source(&file.src, file.whole)))
        .collect()
}

/// The failure report, empty when no file has a hit.
fn report(found: &BTreeMap<String, Vec<Hit>>) -> String {
    let mut out = Vec::new();
    for (path, hits) in found {
        for hit in hits {
            out.push(format!("  {path}:{}: {}", hit.line, hit.text));
            if hit.marker_malformed {
                out.push(format!(
                    "    the marker above is malformed: expected `// {TAG}: allow — <reason>` \
                     on its own line; the reason is required."
                ));
            }
        }
    }
    if out.is_empty() {
        return String::new();
    }
    out.insert(
        0,
        "assertions that the agreed node-link protocol equals a feature's protocol:".to_string(),
    );
    out.push(
        "fix: a dial agrees on the newest protocol both peers speak, so the next protocol bump \
         breaks an exact match about an older feature (intentd#3662). Assert the floor with \
         `intentd_test_support::assert_link_supports(agreed, FEATURE_PROTOCOL)`, or compare \
         with `NODE_LINK_PROTOCOLS.max` in a test about the newest version. A test that caps \
         the offered range at the feature's version can keep the exact match with \
         `// link-protocol-exact: allow — <reason>` on the line above. See the module doc of \
         crates/intent-core/tests/link_protocol_exact_lint.rs."
            .to_string(),
    );
    out.join("\n")
}

#[test]
fn tests_do_not_pin_the_agreed_link_protocol_to_a_feature_constant() {
    let root = intentd_test_support::source_lint::workspace_root();
    let files = test_code_files(&root);
    for known in [
        "crates/intentd/src/executor/tests/homes.rs",
        "crates/intent-transport/src/node_link/tests.rs",
    ] {
        assert!(
            files.iter().any(|f| f.rel == known && f.whole),
            "test-code discovery no longer reads {known} as test code ({} files)",
            files.len()
        );
    }
    let report = report(&scan_tree(&files));
    assert!(report.is_empty(), "{report}\n");
}

#[test]
fn the_protocol_range_lives_where_a_change_selects_every_link_test() {
    let root = intentd_test_support::source_lint::workspace_root();
    let defining: Vec<String> = test_code_files(&root)
        .into_iter()
        .filter(|f| {
            f.rel.contains("/src/")
                && f.src
                    .lines()
                    .any(|l| l.starts_with("pub const NODE_LINK_PROTOCOLS:"))
        })
        .map(|f| f.rel)
        .collect();
    assert_eq!(
        defining,
        vec!["crates/intent-core/src/node_link.rs".to_string()],
        "NODE_LINK_PROTOCOLS moved: scripts/changed-tests.sh propagates only intent-core src \
         changes to every dependent crate (rdeps_roots), so a range bump elsewhere would not \
         run intentd's executor or the transport node-link tests. Keep it in intent-core, or \
         make its new crate a propagation root"
    );
    let script = std::fs::read_to_string(root.join("scripts/changed-tests.sh"))
        .expect("read scripts/changed-tests.sh");
    let roots = script
        .lines()
        .find_map(|l| l.strip_prefix("rdeps_roots=\""))
        .and_then(|rest| rest.split('"').next())
        .expect("scripts/changed-tests.sh sets rdeps_roots=\"…\"");
    assert!(
        roots.split_whitespace().any(|r| r == "intent-core"),
        "scripts/changed-tests.sh rdeps_roots ({roots:?}) no longer lists intent-core, where \
         NODE_LINK_PROTOCOLS lives: a range bump would not select the node-link tests of \
         its dependents"
    );
}

// ---- fixtures ---------------------------------------------------------------

fn lines_in(src: &str) -> Vec<usize> {
    scan_source(src, true).iter().map(|h| h.line).collect()
}

#[test]
fn an_exact_match_with_a_feature_constant_fails() {
    let src = "\
fn t() {
    assert_eq!(dialed.link.protocol(), HOMES_PROTOCOL);
    assert_eq!(dialed.welcome.protocol, HOMES_PROTOCOL, \"agreed\");
    assert_eq!(HOMES_PROTOCOL, end.link.protocol());
    assert_ne!(head.last_protocol(), intent_core::node_link::SECRETS_PROTOCOL);
    assert_eq!(
        dialed.link.protocol(),
        node_link::HOMES_PROTOCOL
    );
    debug_assert_eq!(link.protocol(), FORGE_FETCH_PROTOCOL);
    assert!(dialed.link.protocol() == HOMES_PROTOCOL);
    assert!(ok && HOMES_PROTOCOL == welcome.protocol, \"x\");
    std::assert_eq!(a.protocol(), V2_PROTOCOL);
}
";
    assert_eq!(lines_in(src), vec![2, 3, 4, 5, 6, 10, 11, 12, 13]);
}

#[test]
fn floors_ranges_literals_and_frame_versions_never_count() {
    let src = "\
fn t() {
    assert!(dialed.link.protocol() >= HOMES_PROTOCOL);
    assert_link_supports(dialed.link.protocol(), HOMES_PROTOCOL);
    assert_eq!(dialed.link.protocol(), NODE_LINK_PROTOCOLS.max);
    assert_eq!(dialed.welcome.protocol, NODE_LINK_PROTOCOLS.max);
    assert_eq!(head.link.protocol(), 8);
    assert_eq!(frame.min_protocol(), HOMES_PROTOCOL);
    assert_eq!(EXIT_REASON_PROTOCOL, NODE_LINK_PROTOCOLS.max);
    assert_eq!(HOMES_PROTOCOL, 19);
    assert_eq!(hello.protocol.max, HOMES_PROTOCOL);
    assert_eq!(dialed.link.protocol(), HOMES_PROTOCOL + 1);
    assert_eq!(dialed.link.protocol(), homes_protocol);
    assert!(dialed.link.protocol() == HOMES_PROTOCOL + 1);
    // assert_eq!(dialed.link.protocol(), HOMES_PROTOCOL);
    let s = \"assert_eq!(dialed.link.protocol(), HOMES_PROTOCOL)\";
    if link.protocol() == HOMES_PROTOCOL { return; }
    fn assert_eq(a: u32, b: u32) {}
}
";
    assert_eq!(lines_in(src), Vec::<usize>::new());
}

#[test]
fn only_test_code_is_scanned_in_a_production_file() {
    let src = "\
fn production() {
    assert_eq!(link.protocol(), HOMES_PROTOCOL);
}

#[cfg(test)]
mod tests {
    fn t() {
        assert_eq!(link.protocol(), HOMES_PROTOCOL);
    }
}
";
    assert_eq!(
        scan_source(src, false)
            .iter()
            .map(|h| h.line)
            .collect::<Vec<_>>(),
        vec![8]
    );
}

#[test]
fn marker_above_the_assertion_or_comparison_exempts_and_needs_a_reason() {
    let src = "\
fn t() {
    // link-protocol-exact: allow — the hello caps the range at HOMES_PROTOCOL
    assert_eq!(dialed.link.protocol(), HOMES_PROTOCOL);
    assert!(
        ok
            // link-protocol-exact: allow — capped range
            && dialed.link.protocol() == HOMES_PROTOCOL
    );
    // link-protocol-exact: allow
    assert_eq!(dialed.link.protocol(), HOMES_PROTOCOL);
    // link-protocol-exact: allowance — no
    assert_eq!(dialed.link.protocol(), HOMES_PROTOCOL);
    assert_eq!(dialed.link.protocol(), HOMES_PROTOCOL); // link-protocol-exact: allow — trailing
}
";
    let hits = scan_source(src, true);
    assert_eq!(
        hits.iter()
            .map(|h| (h.line, h.marker_malformed))
            .collect::<Vec<_>>(),
        vec![(10, true), (12, true), (13, false)]
    );
}

#[test]
fn report_names_every_hit_and_the_fix() {
    let found = BTreeMap::from([
        (
            "crates/a/src/tests.rs".to_string(),
            vec![
                Hit {
                    line: 4,
                    text: "assert_eq!(link.protocol(), HOMES_PROTOCOL);".to_string(),
                    marker_malformed: false,
                },
                Hit {
                    line: 9,
                    text: "assert_eq!(link.protocol(), HOMES_PROTOCOL);".to_string(),
                    marker_malformed: true,
                },
            ],
        ),
        ("crates/a/src/clean.rs".to_string(), vec![]),
    ]);
    let report = report(&found);
    for expected in [
        "  crates/a/src/tests.rs:4: assert_eq!(link.protocol(), HOMES_PROTOCOL);",
        "  crates/a/src/tests.rs:9: assert_eq!(link.protocol(), HOMES_PROTOCOL);\n    the marker above is malformed",
        "intentd_test_support::assert_link_supports(agreed, FEATURE_PROTOCOL)",
    ] {
        assert!(report.contains(expected), "missing {expected:?} in:\n{report}");
    }
    assert!(!report.contains("clean.rs"), "{report}");
    assert!(crate::report(&BTreeMap::from([("x".to_string(), vec![])])).is_empty());
}
