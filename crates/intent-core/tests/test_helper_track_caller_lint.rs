//! Source lint: a short sync test helper that asserts or panics must be
//! `#[track_caller]`.
//!
//! Integration tests funnel their checks through small helpers such as
//! `success(&reply)` / `refused(&reply)`. Without `#[track_caller]` a failed
//! check reports the helper's own line, not the test step that called it: in
//! intentd merge-group run 41006457347 the failure read
//! `e2e_repository_selection_wire.rs:340` — a line inside `success()` — and
//! finding the real failing call (the second release request) meant counting
//! request ids by hand. With the attribute, the helper's `assert!` /
//! `panic!` — and its `.unwrap()` / `.expect(…)` calls, which are
//! `#[track_caller]` themselves — report the caller's line instead.
//!
//! That pass-through is also the attribute's cost: every panic point in the
//! helper collapses onto the one caller line. For a long scenario helper with
//! dozens of asserts and bare `.unwrap()`s, the helper's own line is the more
//! useful one. So the rule applies to *short* helpers only.
//!
//! The rule is applied on comment- and literal-blanked text to the shared
//! test-helper code: every `crates/*/tests/**/*.rs` file, the `test_support`
//! modules under `src/` (`crates/*/src/**/test_support.rs` and
//! `crates/*/src/**/test_support/**`), and the whole `intentd-test-support`
//! crate. Unit-test modules under `src/` and an inline
//! `mod test_support { … }` inside another file are not scanned.
//!
//! In those files, a `fn` with a body must carry `#[track_caller]`
//! (directly or as `#[cfg_attr(…, track_caller)]`) when its own body invokes
//! at least one of `assert!`, `assert_eq!`, `assert_ne!`, `assert_matches!`,
//! `debug_assert!`, `debug_assert_eq!`, `debug_assert_ne!`, `panic!`,
//! `unreachable!`, `todo!` or `unimplemented!` (by last path segment, so
//! `std::panic!` counts) and has at most [`MAX_PANIC_POINTS`] panic points
//! in total, where a panic point is one of those macros or an `.unwrap()` /
//! `.expect(` / `.unwrap_err()` / `.expect_err(` call. Code the attribute
//! cannot reach is not the fn's own body: nested `fn` items (checked on their
//! own), closures, `async` blocks and `macro_rules!` bodies. Exempt, because
//! the attribute cannot apply or cannot help:
//!
//! - test functions — any attribute whose last path segment is `test` or ends
//!   in `_test` (`#[test]`, `#[tokio::test]`, `#[intent_test_macros::daemon_test]`);
//! - `async fn` — the attribute is a no-op there on stable Rust (the
//!   `ungated_async_fn_track_caller` warning, rust-lang/rust#110011). An async
//!   helper that must name its caller can be a sync `#[track_caller]` fn that
//!   captures `std::panic::Location::caller()` and returns the future (see
//!   `expect_ok` in `crates/intentd/tests/e2e_wss_rpc_overload_limit.rs`);
//! - `extern` fns (the attribute needs the Rust ABI) and `fn main`.
//!
//! Limits: closure detection is a heuristic (a `|` after `(`, `,`, `=`, `{`,
//! `;`, `[`, `=>`, `move` or `return`); a helper whose only panic source is
//! `.unwrap()` / `.expect()` is not required to carry the attribute; and a
//! long helper that already carries it is not flagged.
//!
//! Opt out with a standalone `// track-caller: allow — <reason>` line in the
//! comment block directly above the fn (doc comments included) or between its
//! attributes. A marker without a reason fails, and so does a stale one: a
//! marker attached to no fn, or to a fn the rule does not require it for.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use intentd_test_support::source_lint::{
    group_close, group_open, lex, markers_by_line, rust_files, skip_whitespace, word_at,
    workspace_root, LineIndex, Marker,
};

const TAG: &str = "track-caller";
/// A helper with more panic points than this keeps its own line: the
/// attribute would collapse all of them onto the caller's line.
const MAX_PANIC_POINTS: usize = 3;
const PANIC_MACROS: &[&str] = &[
    "assert",
    "assert_eq",
    "assert_ne",
    "assert_matches",
    "debug_assert",
    "debug_assert_eq",
    "debug_assert_ne",
    "panic",
    "unreachable",
    "todo",
    "unimplemented",
];
const PANIC_METHODS: &[&str] = &["unwrap", "expect", "unwrap_err", "expect_err"];
/// Words that may precede `fn` in an item's qualifier list.
const QUALIFIERS: &[&str] = &["pub", "const", "unsafe", "async", "extern", "default"];

/// One `fn` item found on blanked text.
#[derive(Debug)]
struct FnItem {
    name: String,
    /// Char offset of the `fn` keyword.
    fn_offset: usize,
    /// Char offset of the item's first attribute or qualifier.
    item_start: usize,
    /// `[open, close]` char offsets of the body braces, if it has a body.
    body: Option<(usize, usize)>,
    /// Already `#[track_caller]`, or a fn the rule does not apply to: a test,
    /// an `async` / `extern` fn, or `main`.
    exempt: bool,
}

/// What a panic point found in a file is attributed to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Owner {
    /// The body of the `fn` item at this index.
    Fn(usize),
    /// A closure, `async` block or `macro_rules!` body.
    Opaque,
}

#[derive(Debug, Clone, Copy)]
struct Region {
    start: usize,
    end: usize,
    owner: Owner,
}

#[derive(Debug, Default, Clone, Copy)]
struct PanicPoints {
    macros: usize,
    total: usize,
}

/// Why a file fails the lint.
#[derive(Debug, PartialEq, Eq)]
enum Problem {
    /// A short helper asserts or panics without `#[track_caller]`.
    MissingTrackCaller(String),
    /// An opt-out marker with no reason (or otherwise malformed).
    MalformedMarker,
    /// A well-formed opt-out marker that covers no fn needing it.
    StaleMarker,
}

#[derive(Debug, PartialEq, Eq)]
struct Violation {
    /// 1-based line of the `fn` keyword, or of the marker.
    line: usize,
    problem: Problem,
}

fn is_ident(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// Index of the last non-whitespace char before `j`, if any.
fn prev_non_ws(chars: &[char], j: usize) -> Option<usize> {
    (0..j).rev().find(|&k| !chars[k].is_whitespace())
}

/// The identifier ending at `end` (inclusive), as `(word, start)`.
fn word_ending_at(chars: &[char], end: usize) -> Option<(String, usize)> {
    if !is_ident(chars[end]) {
        return None;
    }
    let start = (0..=end).rev().take_while(|&w| is_ident(chars[w])).last()?;
    Some((chars[start..=end].iter().collect(), start))
}

/// Whether the attribute between `open` (`[`) and `close` (`]`) applies
/// `track_caller`, and whether it marks a test: `(track_caller, test)`.
fn classify_attribute(chars: &[char], open: usize, close: usize) -> (bool, bool) {
    let text: String = chars[open + 1..close]
        .iter()
        .filter(|c| !c.is_whitespace())
        .collect();
    let path: String = text
        .chars()
        .take_while(|&c| is_ident(c) || c == ':')
        .collect();
    let last = path.rsplit("::").next().unwrap_or(&path);
    let words: BTreeSet<&str> = text.split(|c: char| !is_ident(c)).collect();
    let track_caller =
        path == "track_caller" || (path == "cfg_attr" && words.contains("track_caller"));
    (track_caller, last == "test" || last.ends_with("_test"))
}

/// Walks back from the `fn` keyword over qualifiers (`pub(crate)`, `async`,
/// `extern "C"` — its ABI string is already blanked) and attributes:
/// `(item_start, exempt-by-prefix)`.
fn item_prefix(chars: &[char], fn_offset: usize) -> (usize, bool) {
    let mut start = fn_offset;
    let mut exempt = false;
    while let Some(k) = prev_non_ws(chars, start) {
        if chars[k] == ']' {
            let Some(open) = group_open(chars, k) else {
                break;
            };
            let Some(hash) = prev_non_ws(chars, open).filter(|&h| chars[h] == '#') else {
                break;
            };
            let (track_caller, test) = classify_attribute(chars, open, k);
            exempt |= track_caller || test;
            start = hash;
        } else if chars[k] == ')' {
            let Some(open) = group_open(chars, k) else {
                break;
            };
            match prev_non_ws(chars, open).and_then(|end| word_ending_at(chars, end)) {
                Some((word, w)) if word == "pub" => start = w,
                _ => break,
            }
        } else if let Some((word, w)) = word_ending_at(chars, k) {
            if !QUALIFIERS.contains(&word.as_str()) {
                break;
            }
            exempt |= word == "async" || word == "extern";
            start = w;
        } else {
            break;
        }
    }
    (start, exempt)
}

/// The `fn` item whose `fn` keyword starts at `i`, if it is one (`fn(` is a
/// function-pointer type).
fn fn_item_at(chars: &[char], i: usize, after: usize) -> Option<FnItem> {
    let (name, mut j) = word_at(chars, skip_whitespace(chars, after))?;
    let mut depth = 0isize;
    let mut body = None;
    while let Some(&c) = chars.get(j) {
        match c {
            '(' | '[' => depth += 1,
            ')' | ']' => depth -= 1,
            ';' if depth == 0 => break,
            '{' if depth == 0 => {
                body = group_close(chars, j).map(|close| (j, close));
                break;
            }
            _ => {}
        }
        j += 1;
    }
    let (item_start, prefix_exempt) = item_prefix(chars, i);
    Some(FnItem {
        exempt: prefix_exempt || name == "main",
        name,
        fn_offset: i,
        item_start,
        body,
    })
}

/// Where the body of a closure whose parameter list ends at `params_end`
/// (the closing `|`) ends: its `{…}` block, or its expression up to a `,` /
/// `;` / unmatched closing delimiter at depth 0.
fn closure_body_end(chars: &[char], params_end: usize) -> Option<usize> {
    let mut j = skip_whitespace(chars, params_end + 1);
    if chars.get(j) == Some(&'-') && chars.get(j + 1) == Some(&'>') {
        while chars.get(j).is_some_and(|&c| c != '{') {
            j += 1;
        }
    }
    if chars.get(j) == Some(&'{') {
        return group_close(chars, j);
    }
    let mut depth = 0usize;
    let mut k = j;
    while let Some(&c) = chars.get(k) {
        match c {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' | ',' | ';' if depth == 0 => return Some(k.saturating_sub(1)),
            ')' | ']' | '}' => depth -= 1,
            _ => {}
        }
        k += 1;
    }
    Some(chars.len().saturating_sub(1))
}

/// Whether the `|` at `i` opens a closure's parameter list rather than being
/// a binary or-operator or an or-pattern.
fn opens_closure(chars: &[char], i: usize) -> bool {
    let Some(p) = prev_non_ws(chars, i) else {
        return true;
    };
    match chars[p] {
        '(' | ',' | '=' | '{' | ';' | '[' => chars.get(i + 1) != Some(&'='),
        '>' => p > 0 && chars[p - 1] == '=',
        _ => word_ending_at(chars, p).is_some_and(|(w, _)| w == "move" || w == "return"),
    }
}

/// The region of a closure whose parameter list opens at `i`, if any.
fn closure_region(chars: &[char], i: usize) -> Option<Region> {
    let params_end = if chars.get(i + 1) == Some(&'|') {
        i + 1
    } else {
        (i + 1..chars.len()).find(|&k| chars[k] == '|')?
    };
    Some(Region {
        start: i,
        end: closure_body_end(chars, params_end)?,
        owner: Owner::Opaque,
    })
}

/// The region of an `async` / `async move` block whose `async` keyword ends
/// just before `after`, if any.
fn async_block_region(chars: &[char], after: usize) -> Option<Region> {
    let mut j = skip_whitespace(chars, after);
    if let Some((word, past)) = word_at(chars, j) {
        if word != "move" {
            return None;
        }
        j = skip_whitespace(chars, past);
    }
    (chars.get(j) == Some(&'{')).then_some(())?;
    Some(Region {
        start: j,
        end: group_close(chars, j)?,
        owner: Owner::Opaque,
    })
}

/// The region of a `macro_rules! name { … }` body whose keyword ends just
/// before `after`, if any.
fn macro_rules_region(chars: &[char], after: usize) -> Option<Region> {
    let bang = skip_whitespace(chars, after);
    (chars.get(bang) == Some(&'!')).then_some(())?;
    let (_, past) = word_at(chars, skip_whitespace(chars, bang + 1))?;
    let open = skip_whitespace(chars, past);
    Some(Region {
        start: open,
        end: group_close(chars, open)?,
        owner: Owner::Opaque,
    })
}

/// Every `fn` item plus every region whose panic points the enclosing fn's
/// `#[track_caller]` cannot reach (closures, `async` blocks, `macro_rules!`),
/// regions sorted by start (outermost first on a tie).
fn scan_items(chars: &[char]) -> (Vec<FnItem>, Vec<Region>) {
    let mut items = Vec::new();
    let mut regions = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '|' && opens_closure(chars, i) {
            regions.extend(closure_region(chars, i));
            i += 1;
            continue;
        }
        let at_word_start = i == 0 || !is_ident(chars[i - 1]);
        let Some((word, after)) = at_word_start.then(|| word_at(chars, i)).flatten() else {
            i += 1;
            continue;
        };
        match word.as_str() {
            "fn" => {
                if let Some(item) = fn_item_at(chars, i, after) {
                    if let Some((open, close)) = item.body {
                        regions.push(Region {
                            start: open,
                            end: close,
                            owner: Owner::Fn(items.len()),
                        });
                    }
                    items.push(item);
                }
            }
            "async" => regions.extend(async_block_region(chars, after)),
            "macro_rules" => regions.extend(macro_rules_region(chars, after)),
            _ => {}
        }
        i = after;
    }
    regions.sort_by_key(|r| (r.start, std::cmp::Reverse(r.end)));
    (items, regions)
}

/// Every panic point in the file: `(offset, is_macro)`, in offset order.
fn panic_points(chars: &[char]) -> Vec<(usize, bool)> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        if i > 0 && is_ident(chars[i - 1]) {
            i += 1;
            continue;
        }
        let Some((word, after)) = word_at(chars, i) else {
            i += 1;
            continue;
        };
        let next = chars.get(skip_whitespace(chars, after)).copied();
        if PANIC_MACROS.contains(&word.as_str()) && next == Some('!') {
            out.push((i, true));
        } else if PANIC_METHODS.contains(&word.as_str())
            && next == Some('(')
            && prev_non_ws(chars, i).is_some_and(|p| chars[p] == '.')
        {
            out.push((i, false));
        }
        i = after;
    }
    out
}

/// Panic points per fn item, each attributed to its innermost enclosing
/// region (one sweep over the sorted regions).
fn panic_points_by_fn(chars: &[char], regions: &[Region], n_items: usize) -> Vec<PanicPoints> {
    let mut counts = vec![PanicPoints::default(); n_items];
    let mut stack: Vec<Region> = Vec::new();
    let mut next = 0;
    for (offset, is_macro) in panic_points(chars) {
        while let Some(r) = regions.get(next).filter(|r| r.start < offset) {
            while stack.last().is_some_and(|top| top.end < r.start) {
                stack.pop();
            }
            stack.push(*r);
            next += 1;
        }
        while stack.last().is_some_and(|top| top.end < offset) {
            stack.pop();
        }
        if let Some(Region {
            owner: Owner::Fn(idx),
            ..
        }) = stack.last()
        {
            counts[*idx].total += 1;
            counts[*idx].macros += usize::from(is_macro);
        }
    }
    counts
}

/// Lines where a marker for the fn starting at `item_line` (first attribute
/// or qualifier) with its `fn` keyword on `fn_line` may sit: its attribute
/// lines, plus the contiguous `//` comment block (doc comments included)
/// directly above it.
fn marker_lines(src_lines: &[&str], item_line: usize, fn_line: usize) -> Vec<usize> {
    let mut lines: Vec<usize> = (item_line..fn_line).collect();
    let mut l = item_line - 1;
    while l >= 1
        && src_lines
            .get(l - 1)
            .is_some_and(|t| t.trim_start().starts_with("//"))
    {
        lines.push(l);
        l -= 1;
    }
    lines
}

/// The lint applied to one source file.
fn violations(src: &str) -> Vec<Violation> {
    let lexed = lex(src);
    let markers = markers_by_line(src, &lexed.line_comments, TAG);
    let chars: Vec<char> = lexed.blanked.chars().collect();
    let index = LineIndex::new(&chars);
    let src_lines: Vec<&str> = src.lines().collect();
    let (items, regions) = scan_items(&chars);
    let counts = panic_points_by_fn(&chars, &regions, items.len());

    let mut marker_owner: BTreeMap<usize, usize> = BTreeMap::new();
    let mut needed_markers = BTreeSet::new();
    let mut out = Vec::new();
    for (idx, item) in items.iter().enumerate() {
        if item.body.is_none() {
            continue;
        }
        let fn_line = index.line(item.fn_offset);
        let own_markers: Vec<usize> =
            marker_lines(&src_lines, index.line(item.item_start), fn_line)
                .into_iter()
                .filter(|&l| markers.get(l).is_some_and(|m| *m != Marker::Absent))
                .collect();
        for &l in &own_markers {
            marker_owner.insert(l, idx);
        }
        let PanicPoints { macros, total } = counts[idx];
        if item.exempt || macros == 0 || total > MAX_PANIC_POINTS {
            continue;
        }
        match own_markers
            .iter()
            .find(|&&l| markers[l] == Marker::WithReason)
        {
            Some(&l) => {
                needed_markers.insert(l);
            }
            None => out.push(Violation {
                line: fn_line,
                problem: Problem::MissingTrackCaller(item.name.clone()),
            }),
        }
    }
    for (line, marker) in markers.iter().enumerate() {
        let problem = match marker {
            Marker::Absent => continue,
            Marker::Malformed => Problem::MalformedMarker,
            Marker::WithReason if needed_markers.contains(&line) => continue,
            Marker::WithReason => Problem::StaleMarker,
        };
        out.push(Violation { line, problem });
    }
    out.sort_by_key(|v| v.line);
    out
}

/// Whether the workspace-relative, `/`-separated path `rel` is test-helper
/// code the rule scans: integration tests, the shared `test_support`
/// modules under `src/`, and the `intentd-test-support` crate.
fn is_scanned(rel: &str) -> bool {
    let parts: Vec<&str> = rel.split('/').collect();
    match parts.as_slice() {
        ["crates", _, "tests", ..] | ["crates", "intentd-test-support", "src", ..] => true,
        ["crates", _, "src", rest @ ..] => rest
            .iter()
            .any(|p| *p == "test_support" || *p == "test_support.rs"),
        _ => false,
    }
}

/// Every scanned `*.rs` file (see [`is_scanned`]) with its workspace-relative
/// path, sorted.
fn scanned_sources(root: &Path) -> Vec<(PathBuf, String)> {
    let mut files: Vec<(PathBuf, String)> = rust_files(&root.join("crates"))
        .into_iter()
        .filter_map(|path| {
            let rel = path
                .strip_prefix(root)
                .ok()?
                .components()
                .map(|c| c.as_os_str().to_string_lossy().into_owned())
                .collect::<Vec<_>>()
                .join("/");
            is_scanned(&rel).then_some((path, rel))
        })
        .collect();
    files.sort();
    files
}

#[test]
fn short_sync_test_helpers_that_assert_carry_track_caller() {
    let root = workspace_root();
    let files = scanned_sources(&root);
    assert!(
        files.iter().any(|(_, rel)| rel.contains("/tests/"))
            && files.iter().any(|(_, rel)| rel.contains("/test_support")),
        "expected both crates/*/tests and src test_support files under {}",
        root.display()
    );
    let mut failures = Vec::new();
    for (path, rel) in &files {
        let src = std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        for v in violations(&src) {
            let line = v.line;
            failures.push(match v.problem {
                Problem::MissingTrackCaller(name) => format!(
                    "{rel}:{line}: fn `{name}` asserts or panics without #[track_caller]; add \
                     #[track_caller] so a failure reports the calling test line"
                ),
                Problem::MalformedMarker => format!(
                    "{rel}:{line}: malformed `// {TAG}: allow — <reason>` marker (a reason is \
                     required)"
                ),
                Problem::StaleMarker => format!(
                    "{rel}:{line}: stale `// {TAG}: allow` marker: it covers no short helper \
                     that would need #[track_caller]; remove it"
                ),
            });
        }
    }
    assert!(
        failures.is_empty(),
        "{} test-helper track_caller finding(s) (see this lint's module doc):\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[cfg(test)]
mod fixtures {
    use super::{is_scanned, violations, Problem, Violation};

    fn missing(line: usize, name: &str) -> Violation {
        Violation {
            line,
            problem: Problem::MissingTrackCaller(name.to_string()),
        }
    }

    fn at(line: usize, problem: Problem) -> Violation {
        Violation { line, problem }
    }

    #[test]
    fn flags_a_sync_helper_that_asserts_without_the_attribute() {
        let src = "use serde_json::Value;\nfn success(value: &Value) -> &Value {\n    assert!(value.get(\"error\").is_none(), \"{value}\");\n    &value[\"result\"]\n}\n";
        assert_eq!(violations(src), vec![missing(2, "success")]);
    }

    #[test]
    fn accepts_the_attribute_among_other_attributes_qualifiers_and_cfg_attr() {
        let src = "#[track_caller]\n#[allow(dead_code)]\npub(crate) fn refused(v: &str) {\n    assert_eq!(v, \"x\");\n}\nimpl H {\n    #[track_caller]\n    pub fn expect_err(&self) { panic!(\"no\") }\n}\n#[cfg_attr(test, track_caller)]\nfn conditional() { assert!(true) }\n";
        assert_eq!(violations(src), vec![]);
    }

    #[test]
    fn flags_every_panicking_macro_and_qualified_paths() {
        for body in [
            "assert_ne!(a, b)",
            "std::panic!(\"x\")",
            "unreachable!()",
            "todo!()",
            "unimplemented!()",
            "debug_assert_eq!(1, 1)",
            "assert_matches!(x, Some(_))",
        ] {
            let src = format!("fn helper() {{\n    {body};\n}}\n");
            assert_eq!(violations(&src), vec![missing(1, "helper")], "{body}");
        }
    }

    #[test]
    fn long_helpers_keep_their_own_lines() {
        let at_limit = "fn helper(v: Option<u8>) {\n    let x = v.unwrap();\n    let y = r.expect(\"y\");\n    assert_eq!(x, y);\n}\n";
        assert_eq!(violations(at_limit), vec![missing(1, "helper")]);
        let over = "fn scenario(v: Option<u8>) {\n    let x = v.unwrap();\n    let y = r.expect(\"y\");\n    assert_eq!(x, y);\n    w.unwrap_err();\n}\n";
        assert_eq!(violations(over), vec![]);
        let only_unwraps = "fn setup() { v.unwrap(); }\n";
        assert_eq!(violations(only_unwraps), vec![]);
    }

    #[test]
    fn ignores_tests_async_extern_main_and_bodiless_fns() {
        let src = "#[test]\nfn plain() { assert!(false) }\n#[tokio::test]\nasync fn tokio() { assert!(false) }\n#[intent_test_macros::daemon_test]\nasync fn daemon() { assert!(false) }\nasync fn async_helper() { assert!(false) }\nimpl T { pub async fn method(&self) { panic!() } }\nextern \"C\" fn cb() { panic!() }\nfn main() { assert!(true) }\ntrait Check { fn check(&self); }\n";
        assert_eq!(violations(src), vec![]);
    }

    #[test]
    fn ignores_macros_in_comments_strings_and_non_macro_calls() {
        let src = "fn quiet() {\n    // assert!(false)\n    let _ = \"panic!()\";\n    let _ = r#\"assert_eq!(1, 2)\"#;\n    x.assert(1);\n    my_assert!(y);\n    value.unwrap();\n}\n";
        assert_eq!(violations(src), vec![]);
    }

    #[test]
    fn panics_the_attribute_cannot_reach_do_not_count() {
        let src = "fn fut() -> impl Future<Output = ()> {\n    async move { assert!(x) }\n}\nfn closure() {\n    let f = || panic!();\n    f()\n}\nfn block_closure() {\n    v.iter().for_each(|x| {\n        assert!(*x > 0);\n    });\n}\nfn with_macro() {\n    macro_rules! m { () => { assert!(true) } }\n}\n";
        assert_eq!(violations(src), vec![]);
    }

    #[test]
    fn panics_after_a_closure_still_count() {
        let src = "fn helper(v: &[u8]) {\n    let n = v.iter().filter(|x| **x > 0).count();\n    assert!(n > 0 || v.is_empty());\n}\n";
        assert_eq!(violations(src), vec![missing(1, "helper")]);
    }

    #[test]
    fn checks_nested_fns_on_their_own() {
        let src = "#[test]\nfn outer() {\n    fn inner() { assert!(true) }\n    inner();\n}\n#[track_caller]\nfn wrapper() {\n    fn quiet() {}\n    assert!(true);\n}\nfn only_nested_panics() {\n    #[track_caller]\n    fn inner() { panic!() }\n    inner();\n}\n";
        assert_eq!(violations(src), vec![missing(3, "inner")]);
    }

    #[test]
    fn fn_pointer_types_and_closures_are_not_items() {
        let src = "fn takes(f: fn(u8) -> u8) -> u8 {\n    f(1)\n}\nfn wraps() -> Box<dyn Fn()> {\n    Box::new(|| {})\n}\n";
        assert_eq!(violations(src), vec![]);
    }

    #[test]
    fn signature_semicolons_inside_brackets_do_not_end_the_item() {
        let src = "fn bytes() -> [u8; 2] {\n    assert!(true);\n    [0; 2]\n}\n";
        assert_eq!(violations(src), vec![missing(1, "bytes")]);
    }

    #[test]
    fn opt_out_marker_sits_above_docs_or_between_attributes() {
        let ok = "// track-caller: allow — the helper's own line is the useful one\n/// Docs.\nfn helper() { assert!(true) }\n#[allow(dead_code)]\n// track-caller: allow - between attributes\nfn other() { assert!(true) }\n";
        assert_eq!(violations(ok), vec![]);
    }

    #[test]
    fn opt_out_marker_needs_a_reason() {
        let bad = "// track-caller: allow\nfn helper() { assert!(true) }\n";
        assert_eq!(
            violations(bad),
            vec![at(1, Problem::MalformedMarker), missing(2, "helper")]
        );
    }

    #[test]
    fn scans_tests_test_support_modules_and_the_test_support_crate() {
        for rel in [
            "crates/intentd/tests/e2e_x.rs",
            "crates/intentd/tests/common/mod.rs",
            "crates/intent-services/src/test_support.rs",
            "crates/intent-services/src/test_support/fixtures.rs",
            "crates/intentd-test-support/src/timeout.rs",
        ] {
            assert!(is_scanned(rel), "{rel}");
        }
        for rel in [
            "crates/intent-services/src/lib.rs",
            "crates/intentd/src/main.rs",
            "crates/intent-services/src/tests/workspace.rs",
            "scripts/x.rs",
        ] {
            assert!(!is_scanned(rel), "{rel}");
        }
    }

    #[test]
    fn stale_markers_fail() {
        let src = "// track-caller: allow — belongs to nothing\n\nfn helper() { assert!(true) }\n// track-caller: allow — already attributed\n#[track_caller]\nfn attributed() { assert!(true) }\n// track-caller: allow — no panics\nfn quiet() {}\n";
        assert_eq!(
            violations(src),
            vec![
                at(1, Problem::StaleMarker),
                missing(3, "helper"),
                at(4, Problem::StaleMarker),
                at(7, Problem::StaleMarker),
            ]
        );
    }
}
