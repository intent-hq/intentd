//! Source lint: a sync test helper that asserts or panics must be
//! `#[track_caller]`.
//!
//! Integration tests funnel their checks through small helpers such as
//! `success(&reply)` / `refused(&reply)`. Without `#[track_caller]` a failed
//! check reports the helper's own line, not the test step that called it: in
//! intentd merge-group run 41006457347 the failure read
//! `e2e_repository_selection_wire.rs:340` — a line inside `success()` — and
//! finding the real failing call (the second release request) meant counting
//! request ids by hand. With the attribute, `assert!` / `panic!` (and
//! `Option::unwrap` / `Result::expect`, themselves `#[track_caller]`) report
//! the caller's line instead.
//!
//! The rule, applied to every `crates/*/tests/**/*.rs` file on comment- and
//! literal-blanked text: a `fn` with a body whose own body (nested `fn` items
//! excluded; they are checked on their own) invokes `assert!`, `assert_eq!`,
//! `assert_ne!`, `assert_matches!`, `debug_assert!`, `debug_assert_eq!`,
//! `debug_assert_ne!`, `panic!`, `unreachable!`, `todo!` or `unimplemented!`
//! (by last path segment, so `std::panic!` counts) must carry
//! `#[track_caller]`. Exempt, because the attribute cannot apply or cannot
//! help:
//!
//! - test functions — any attribute whose last path segment is `test` or ends
//!   in `_test` (`#[test]`, `#[tokio::test]`, `#[intent_test_macros::daemon_test]`);
//! - `async fn` — the attribute is a no-op there on stable Rust (the
//!   `ungated_async_fn_track_caller` warning, rust-lang/rust#110011). An async
//!   helper that must name its caller can be a sync `#[track_caller]` fn that
//!   captures `std::panic::Location::caller()` and returns the future, printing
//!   the location in its assertion message (see `expect_ok` in
//!   `crates/intentd/tests/e2e_wss_rpc_overload_limit.rs`);
//! - `extern` fns (the attribute needs the Rust ABI) and `fn main`.
//!
//! Limits: a helper whose only panic source is `.unwrap()` / `.expect(…)` is
//! not flagged (those already report their own call site), and a panic inside
//! a closure in the helper still reports the closure's line — closures cannot
//! take the attribute on stable Rust. Opt out with a standalone
//! `// track-caller: allow — <reason>` line directly above the fn's first
//! attribute or `fn` line (or between its attributes); a marker without a
//! reason fails.

use std::path::{Path, PathBuf};

use intentd_test_support::source_lint::{
    lex, markers_by_line, rust_files, skip_whitespace, word_at, workspace_root, Marker,
};

const TAG: &str = "track-caller";
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

/// Why a fn fails the lint.
#[derive(Debug, PartialEq, Eq)]
enum Problem {
    /// Asserts or panics without `#[track_caller]` and without an opt-out.
    MissingTrackCaller,
    /// Carries an opt-out marker with no reason (or otherwise malformed).
    MalformedMarker,
}

#[derive(Debug, PartialEq, Eq)]
struct Violation {
    /// 1-based line of the `fn` keyword.
    line: usize,
    name: String,
    problem: Problem,
}

fn is_ident(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

fn line_of(chars: &[char], offset: usize) -> usize {
    1 + chars[..offset].iter().filter(|&&c| c == '\n').count()
}

/// Index of the last non-whitespace char before `j`, if any.
fn prev_non_ws(chars: &[char], j: usize) -> Option<usize> {
    (0..j).rev().find(|&k| !chars[k].is_whitespace())
}

/// Index of the delimiter opening the group whose closing delimiter sits at
/// `close`.
fn matching_open(chars: &[char], close: usize, open_c: char, close_c: char) -> Option<usize> {
    let mut depth = 0usize;
    for k in (0..=close).rev() {
        if chars[k] == close_c {
            depth += 1;
        } else if chars[k] == open_c {
            depth -= 1;
            if depth == 0 {
                return Some(k);
            }
        }
    }
    None
}

/// Index of the delimiter closing the group opened at `open`.
fn matching_close(chars: &[char], open: usize, open_c: char, close_c: char) -> Option<usize> {
    let mut depth = 0usize;
    for (k, &c) in chars.iter().enumerate().skip(open) {
        if c == open_c {
            depth += 1;
        } else if c == close_c {
            depth -= 1;
            if depth == 0 {
                return Some(k);
            }
        }
    }
    None
}

/// The attribute's path with whitespace removed (`tokio::test`).
fn attribute_path(chars: &[char], open: usize, close: usize) -> String {
    chars[open + 1..close]
        .iter()
        .filter(|c| !c.is_whitespace())
        .take_while(|&&c| is_ident(c) || c == ':')
        .collect()
}

/// Walks back from the `fn` keyword over qualifiers (`pub(crate)`, `async`,
/// `extern "C"` — its ABI string is already blanked) and attributes.
fn item_prefix(chars: &[char], fn_offset: usize) -> (usize, Vec<String>, Vec<String>) {
    let mut start = fn_offset;
    let mut qualifiers = Vec::new();
    let mut attributes = Vec::new();
    while let Some(k) = prev_non_ws(chars, start) {
        if chars[k] == ']' {
            let Some(open) = matching_open(chars, k, '[', ']') else {
                break;
            };
            let Some(hash) = prev_non_ws(chars, open).filter(|&h| chars[h] == '#') else {
                break;
            };
            attributes.push(attribute_path(chars, open, k));
            start = hash;
        } else if chars[k] == ')' {
            let Some(open) = matching_open(chars, k, '(', ')') else {
                break;
            };
            let Some(end) = prev_non_ws(chars, open) else {
                break;
            };
            let word_start = (0..=end).rev().take_while(|&w| is_ident(chars[w])).last();
            match word_start {
                Some(w) if chars[w..=end].iter().collect::<String>() == "pub" => start = w,
                _ => break,
            }
        } else if is_ident(chars[k]) {
            let w = (0..=k)
                .rev()
                .take_while(|&w| is_ident(chars[w]))
                .last()
                .unwrap_or(k);
            let word: String = chars[w..=k].iter().collect();
            if !QUALIFIERS.contains(&word.as_str()) {
                break;
            }
            qualifiers.push(word);
            start = w;
        } else {
            break;
        }
    }
    (start, qualifiers, attributes)
}

/// Every `fn` item (with a name) in blanked source.
fn fn_items(chars: &[char]) -> Vec<FnItem> {
    let mut items = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let at_word_start = i == 0 || !is_ident(chars[i - 1]);
        let Some((word, after)) = at_word_start.then(|| word_at(chars, i)).flatten() else {
            i += 1;
            continue;
        };
        if word != "fn" {
            i = after;
            continue;
        }
        let name_at = skip_whitespace(chars, after);
        // `fn(` is a function-pointer type, not an item.
        let Some((name, mut j)) = word_at(chars, name_at) else {
            i = after;
            continue;
        };
        let mut depth = 0isize;
        let mut body = None;
        while let Some(&c) = chars.get(j) {
            match c {
                '(' | '[' => depth += 1,
                ')' | ']' => depth -= 1,
                ';' if depth == 0 => break,
                '{' if depth == 0 => {
                    body = matching_close(chars, j, '{', '}').map(|close| (j, close));
                    break;
                }
                _ => {}
            }
            j += 1;
        }
        let (item_start, qualifiers, attributes) = item_prefix(chars, i);
        let is_test = attributes.iter().any(|a| {
            let last = a.rsplit("::").next().unwrap_or(a);
            last == "test" || last.ends_with("_test")
        });
        let exempt = is_test
            || name == "main"
            || attributes.iter().any(|a| a == "track_caller")
            || qualifiers.iter().any(|q| q == "async" || q == "extern");
        items.push(FnItem {
            name,
            fn_offset: i,
            item_start,
            body,
            exempt,
        });
        i = after;
    }
    items
}

/// Whether `chars[from..to]`, minus the `excluded` ranges, invokes a
/// panicking macro.
fn invokes_panic_macro(
    chars: &[char],
    from: usize,
    to: usize,
    excluded: &[(usize, usize)],
) -> bool {
    let mut i = from;
    while i < to {
        if let Some(&(_, end)) = excluded.iter().find(|&&(s, e)| s <= i && i <= e) {
            i = end + 1;
            continue;
        }
        if i > 0 && is_ident(chars[i - 1]) {
            i += 1;
            continue;
        }
        if let Some((word, after)) = word_at(chars, i) {
            if PANIC_MACROS.contains(&word.as_str())
                && chars.get(skip_whitespace(chars, after)) == Some(&'!')
            {
                return true;
            }
            i = after;
        } else {
            i += 1;
        }
    }
    false
}

/// The lint applied to one source file.
fn violations(src: &str) -> Vec<Violation> {
    let lexed = lex(src);
    let markers = markers_by_line(src, &lexed.line_comments, TAG);
    let chars: Vec<char> = lexed.blanked.chars().collect();
    let items = fn_items(&chars);
    let mut out = Vec::new();
    for item in &items {
        let Some((open, close)) = item.body else {
            continue;
        };
        let line = line_of(&chars, item.fn_offset);
        let marker_lines = line_of(&chars, item.item_start).saturating_sub(1)..line;
        let marker = marker_lines
            .map(|l| markers.get(l).copied().unwrap_or(Marker::Absent))
            .find(|m| *m != Marker::Absent)
            .unwrap_or(Marker::Absent);
        if marker == Marker::Malformed {
            out.push(Violation {
                line,
                name: item.name.clone(),
                problem: Problem::MalformedMarker,
            });
            continue;
        }
        if item.exempt || marker == Marker::WithReason {
            continue;
        }
        let nested: Vec<(usize, usize)> = items
            .iter()
            .filter(|other| other.fn_offset > open && other.fn_offset < close)
            .filter_map(|other| other.body.map(|(_, end)| (other.item_start, end)))
            .collect();
        if invokes_panic_macro(&chars, open + 1, close, &nested) {
            out.push(Violation {
                line,
                name: item.name.clone(),
                problem: Problem::MissingTrackCaller,
            });
        }
    }
    out
}

/// Every `*.rs` file under any `crates/*/tests/`, sorted.
#[track_caller]
fn test_sources(root: &Path) -> Vec<PathBuf> {
    let crates_dir = root.join("crates");
    let mut files = Vec::new();
    for entry in std::fs::read_dir(&crates_dir)
        .unwrap_or_else(|e| panic!("read_dir {}: {e}", crates_dir.display()))
    {
        let tests = entry.expect("crate dir entry").path().join("tests");
        if tests.is_dir() {
            files.extend(rust_files(&tests));
        }
    }
    files.sort();
    files
}

#[test]
fn sync_test_helpers_that_assert_carry_track_caller() {
    let root = workspace_root();
    let files = test_sources(&root);
    assert!(
        !files.is_empty(),
        "no crates/*/tests/**/*.rs files found under {}",
        root.display()
    );
    let mut failures = Vec::new();
    for path in &files {
        let src = std::fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
        let rel = path
            .strip_prefix(&root)
            .unwrap_or(path)
            .display()
            .to_string();
        for v in violations(&src) {
            failures.push(match v.problem {
                Problem::MissingTrackCaller => format!(
                    "{rel}:{}: fn `{}` asserts or panics without #[track_caller]; add \
                     #[track_caller] so a failure reports the calling test line",
                    v.line, v.name
                ),
                Problem::MalformedMarker => format!(
                    "{rel}:{}: fn `{}` has a malformed `// {TAG}: allow — <reason>` marker \
                     (a reason is required)",
                    v.line, v.name
                ),
            });
        }
    }
    assert!(
        failures.is_empty(),
        "{} test helper(s) would report their own line instead of the caller's \
         (see this lint's module doc; opt out with `// {TAG}: allow — <reason>`):\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[cfg(test)]
mod fixtures {
    use super::{violations, Problem, Violation};

    fn missing(line: usize, name: &str) -> Violation {
        Violation {
            line,
            name: name.to_string(),
            problem: Problem::MissingTrackCaller,
        }
    }

    #[test]
    fn flags_a_sync_helper_that_asserts_without_the_attribute() {
        let src = "use serde_json::Value;\nfn success(value: &Value) -> &Value {\n    assert!(value.get(\"error\").is_none(), \"{value}\");\n    &value[\"result\"]\n}\n";
        assert_eq!(violations(src), vec![missing(2, "success")]);
    }

    #[test]
    fn accepts_the_attribute_among_other_attributes_and_qualifiers() {
        let src = "#[track_caller]\n#[allow(dead_code)]\npub(crate) fn refused(v: &str) {\n    assert_eq!(v, \"x\");\n}\nimpl H {\n    #[track_caller]\n    pub fn expect_err(&self) { panic!(\"no\") }\n}\n";
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
    fn opt_out_marker_needs_a_reason() {
        let ok = "// track-caller: allow — the helper's own line is the useful one\nfn helper() { assert!(true) }\n#[allow(dead_code)]\n// track-caller: allow - between attributes\nfn other() { assert!(true) }\n";
        assert_eq!(violations(ok), vec![]);
        let bad = "// track-caller: allow\nfn helper() { assert!(true) }\n";
        assert_eq!(
            violations(bad),
            vec![Violation {
                line: 2,
                name: "helper".to_string(),
                problem: Problem::MalformedMarker,
            }]
        );
    }

    #[test]
    fn a_marker_further_up_does_not_count() {
        let src = "// track-caller: allow — belongs to nothing\n\nfn helper() { assert!(true) }\n";
        assert_eq!(violations(src), vec![missing(3, "helper")]);
    }
}
