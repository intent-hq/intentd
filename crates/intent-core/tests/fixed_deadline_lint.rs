//! Fixed-deadline lint.
//!
//! Coverage CI runs the suites under llvm-cov on oversubscribed self-hosted
//! runners and exports `INTENTD_TEST_TIMEOUT_MULTIPLIER=3` so hang guards get
//! more time there. A test deadline written as a bare literal —
//! `tokio::time::timeout(Duration::from_secs(5), …)` — ignores that setting
//! and flakes as soon as the runner is slower than the number: on 2026-10-19
//! `indexed_note_pages_migrate_existing_notes_and_preserve_legacy_text` hit
//! `Err(Elapsed)` at 6.0–6.6s three times (intentd CI runs 39241756358,
//! 39248018551) and held a finished fix (intentd#2512) out of the merge queue
//! until intentd#2514 scaled that one file. This source-scanning test fails,
//! naming `file:line`, for every fixed deadline in test code that neither
//! goes through the shared helper nor carries a justified marker, and is not
//! grandfathered by the committed baseline.
//!
//! - **Fix:** wrap the budget in `intentd_test_support::test_timeout(…)` (or
//!   a crate-local helper that delegates to it: `common::test_timeout`,
//!   `test_support::fixture_timeout`, …).
//! - **What is test code:** every file under `crates/*/tests/`, and inside
//!   `crates/*/src/`: files named `tests.rs` / `*_tests.rs`, anything under a
//!   `tests/` directory, files reached through a `#[cfg(test)] mod name;`
//!   declaration (and that module's directory), and every `#[cfg(test)]`
//!   item. Production code is never scanned.
//! - **What counts:** on comment- and literal-blanked text (so quoted
//!   examples never count), a `Duration::from_*(` path — `std::time::` /
//!   `tokio::time::` qualified or not, any whitespace or line breaks between
//!   tokens — used directly as (a) the first argument of a call named one of
//!   [`DEADLINE_CALLS`] (`tokio::time::timeout`, `recv_timeout`, …; a
//!   `fn timeout(` definition is not a call), or (b) the operand of an
//!   `Instant::now() + …` / `Instant::now().checked_add(…)` deadline.
//! - **Marker:** a standalone `// fixed-deadline: allow — <reason>` on the
//!   line above the hit, or above the first line of its statement, exempts it
//!   — for a deliberately short "nothing should happen" window whose length
//!   is the thing under test. A marker without a reason never exempts and is
//!   itself reported.
//! - **Baseline** (`tests/fixed_deadline_baseline.txt`): one `<path> <count>`
//!   line per file that still has unmarked fixed deadlines, sorted by path. It
//!   only ratchets down: a file over its entry (or absent) fails naming every
//!   line; a file under its entry, or an entry whose file is gone, fails
//!   naming the exact line to write.
//!   To regenerate it after a bulk conversion, run the lint with
//!   `INTENTD_UPDATE_FIXED_DEADLINE_BASELINE=1`, then review the diff: it
//!   must only lower or remove lines.
//! - **Limits:** a fixed budget behind a `const` or a local binding
//!   (`const WAIT: Duration = Duration::from_secs(5); timeout(WAIT, …)`) is
//!   not seen, and a call is matched by name only, so a method also called
//!   `timeout` (a client builder's) counts too — wrap or mark it.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use intentd_test_support::source_lint::{
    cfg_test_items, lex, markers_by_line, rust_files, skip_whitespace, statement_line, Literal,
    Marker,
};

/// Opt-out tag: `// fixed-deadline: allow — <reason>`.
const TAG: &str = "fixed-deadline";

/// Calls whose first argument is a wall-clock budget.
const DEADLINE_CALLS: &[&str] = &[
    "timeout",
    "recv_timeout",
    "wait_timeout",
    "wait_with_timeout",
];

const BASELINE_FILE: &str = "crates/intent-core/tests/fixed_deadline_baseline.txt";

fn is_ident(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// The identifier starting at `j`, and the index just past it.
fn ident_at(chars: &[char], j: usize) -> Option<(String, usize)> {
    let end = chars[j..]
        .iter()
        .position(|c| !is_ident(*c))
        .map_or(chars.len(), |n| j + n);
    (end > j).then(|| (chars[j..end].iter().collect(), end))
}

/// Index past `token` at `j` (after whitespace), if it is there.
fn expect(chars: &[char], j: usize, token: &str) -> Option<usize> {
    let j = skip_whitespace(chars, j);
    let len = token.chars().count();
    (chars.len() >= j + len && chars[j..j + len].iter().copied().eq(token.chars()))
        .then_some(j + len)
}

/// Is the expression at `j` (after whitespace) a `…Duration::from_*(` call?
/// Reads a `::`-separated path and requires its last two segments to be
/// `Duration` and `from_<unit>`, followed by `(`.
fn is_duration_literal(chars: &[char], j: usize) -> bool {
    let mut segments = Vec::new();
    let mut j = skip_whitespace(chars, j);
    loop {
        let Some((word, end)) = chars
            .get(j)
            .filter(|c| is_ident(**c))
            .and_then(|_| ident_at(chars, j))
        else {
            return false;
        };
        segments.push(word);
        let Some(next) = expect(chars, end, "::") else {
            j = end;
            break;
        };
        j = skip_whitespace(chars, next);
    }
    let [.., ty, ctor] = segments.as_slice() else {
        return false;
    };
    ty == "Duration" && ctor.starts_with("from_") && expect(chars, j, "(").is_some()
}

/// The identifier ending just before `j` (skipping whitespace back).
fn word_before(chars: &[char], j: usize) -> String {
    let end = chars[..j]
        .iter()
        .rposition(|c| !c.is_whitespace())
        .map_or(0, |p| p + 1);
    let start = chars[..end]
        .iter()
        .rposition(|c| !is_ident(*c))
        .map_or(0, |p| p + 1);
    chars[start..end].iter().collect()
}

/// Char offsets (of the call name / `Instant`) of every fixed deadline in
/// `chars` (blanked source).
fn deadline_offsets(chars: &[char]) -> Vec<usize> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        if !is_ident(chars[i]) || (i > 0 && is_ident(chars[i - 1])) {
            i += 1;
            continue;
        }
        let Some((word, end)) = ident_at(chars, i) else {
            i += 1;
            continue;
        };
        let hit = if DEADLINE_CALLS.contains(&word.as_str()) {
            word_before(chars, i) != "fn"
                && expect(chars, end, "(").is_some_and(|j| is_duration_literal(chars, j))
        } else if word == "Instant" {
            let now = ["::", "now", "(", ")"]
                .iter()
                .try_fold(end, |j, token| expect(chars, j, token));
            now.is_some_and(|j| {
                expect(chars, j, "+").is_some_and(|k| is_duration_literal(chars, k))
                    || [".", "checked_add", "("]
                        .iter()
                        .try_fold(j, |k, token| expect(chars, k, token))
                        .is_some_and(|k| is_duration_literal(chars, k))
            })
        } else {
            false
        };
        if hit {
            out.push(i);
        }
        i = end;
    }
    out
}

/// A fixed deadline with no reasoned marker: its 1-based line and text.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Deadline {
    line: usize,
    text: String,
    /// The consulted marker line carries a marker with no reason.
    marker_malformed: bool,
}

/// What one source file contributes: its unmarked deadlines in test code,
/// and the out-of-line `#[cfg(test)]` modules it declares.
#[derive(Debug, Default, PartialEq, Eq)]
struct Scan {
    deadlines: Vec<Deadline>,
    /// `(name, Some(path attr))` per `#[cfg(test)] mod name;`.
    test_mods: Vec<(String, Option<String>)>,
}

fn line_of(chars: &[char], offset: usize) -> usize {
    chars[..offset].iter().filter(|c| **c == '\n').count() + 1
}

/// The `#[path = "…"]` value inside `[start, end)` of `src`, if any.
fn path_attr(chars: &[char], literals: &[Literal], start: usize, end: usize) -> Option<String> {
    literals
        .iter()
        .filter(|lit| (start..end).contains(&lit.offset))
        .find(|lit| {
            let before = chars[..lit.offset]
                .iter()
                .rposition(|c| !c.is_whitespace())
                .filter(|p| chars[*p] == '=');
            before.is_some_and(|p| word_before(chars, p) == "path")
        })
        .map(Literal::cooked)
}

/// Scans `src`: the whole file when `whole_file_is_test`, otherwise only its
/// `#[cfg(test)]` items.
fn scan_source(src: &str, whole_file_is_test: bool) -> Scan {
    let lexed = lex(src);
    let chars: Vec<char> = lexed.blanked.chars().collect();
    let items = cfg_test_items(&chars);
    let in_test = |offset: usize| {
        whole_file_is_test
            || items
                .iter()
                .any(|item| (item.start..item.end).contains(&offset))
    };
    let markers = markers_by_line(src, &lexed.line_comments, TAG);
    let marker = |line: usize| {
        line.checked_sub(1)
            .and_then(|above| markers.get(above))
            .copied()
            .unwrap_or(Marker::Absent)
    };
    let lines: Vec<&str> = src.lines().collect();
    let deadlines = deadline_offsets(&chars)
        .into_iter()
        .filter(|offset| in_test(*offset))
        .filter_map(|offset| {
            let line = line_of(&chars, offset);
            let consulted = [marker(line), marker(statement_line(&chars, offset))];
            if consulted.contains(&Marker::WithReason) {
                return None;
            }
            Some(Deadline {
                line,
                text: lines.get(line - 1).map_or("", |l| l.trim()).to_string(),
                marker_malformed: consulted.contains(&Marker::Malformed),
            })
        })
        .collect();
    let test_mods = items
        .iter()
        .filter_map(|item| {
            let name = item.out_of_line_mod.clone()?;
            Some((
                name,
                path_attr(&chars, &lexed.literals, item.start, item.end),
            ))
        })
        .collect();
    Scan {
        deadlines,
        test_mods,
    }
}

/// Test code by file-name convention under a crate's `src/` (`rel` is
/// relative to that `src/`).
fn is_conventional_test_file(rel: &Path) -> bool {
    let name = rel.file_name().and_then(|n| n.to_str()).unwrap_or("");
    name == "tests.rs"
        || name.ends_with("_tests.rs")
        || rel
            .parent()
            .is_some_and(|dir| dir.components().any(|c| c.as_os_str() == "tests"))
}

/// The files and directories a `#[cfg(test)] mod name;` in `file` makes
/// test code.
fn test_mod_targets(file: &Path, name: &str, path: Option<&str>) -> Vec<PathBuf> {
    let dir = file.parent().unwrap_or(Path::new(""));
    if let Some(path) = path {
        let target = dir.join(path);
        return vec![target.with_extension(""), target];
    }
    let stem = file.file_stem().and_then(|s| s.to_str()).unwrap_or("");
    let base = if matches!(stem, "mod" | "lib" | "main") {
        dir.to_path_buf()
    } else {
        dir.join(stem)
    };
    vec![base.join(format!("{name}.rs")), base.join(name)]
}

fn display_rel(rel: &Path) -> String {
    rel.components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

/// Unmarked fixed deadlines per scanned file (zero-count files included),
/// keyed by workspace-relative path.
fn scan_tree(root: &Path) -> BTreeMap<String, Vec<Deadline>> {
    let crates_dir = root.join("crates");
    let mut krates: Vec<PathBuf> = fs::read_dir(&crates_dir)
        .unwrap_or_else(|e| panic!("read_dir {}: {e}", crates_dir.display()))
        .map(|entry| entry.expect("crate dir entry").path())
        .filter(|path| path.is_dir())
        .collect();
    krates.sort();
    let mut whole = BTreeSet::new();
    let mut partial = Vec::new();
    for krate in &krates {
        let tests = krate.join("tests");
        if tests.is_dir() {
            whole.extend(rust_files(&tests));
        }
        let src = krate.join("src");
        if src.is_dir() {
            for file in rust_files(&src) {
                let rel = file.strip_prefix(&src).expect("under src").to_path_buf();
                if is_conventional_test_file(&rel) {
                    whole.insert(file);
                } else {
                    partial.push(file);
                }
            }
        }
    }
    let read = |file: &Path| {
        fs::read_to_string(file).unwrap_or_else(|e| panic!("read {}: {e}", file.display()))
    };
    let mut scans: BTreeMap<PathBuf, Scan> = BTreeMap::new();
    let mut test_targets = Vec::new();
    for file in &partial {
        let scan = scan_source(&read(file), false);
        for (name, path) in &scan.test_mods {
            test_targets.extend(test_mod_targets(file, name, path.as_deref()));
        }
        scans.insert(file.clone(), scan);
    }
    for file in &partial {
        if test_targets.iter().any(|target| file.starts_with(target)) {
            scans.insert(file.clone(), scan_source(&read(file), true));
        }
    }
    for file in &whole {
        scans.insert(file.clone(), scan_source(&read(file), true));
    }
    scans
        .into_iter()
        .map(|(file, scan)| {
            let rel = display_rel(file.strip_prefix(root).expect("under root"));
            (rel, scan.deadlines)
        })
        .collect()
}

/// Parses the baseline: `<path> <count>` per line, blank lines and `#`
/// comments ignored, paths strictly increasing, counts positive.
fn parse_baseline(text: &str) -> Result<BTreeMap<String, usize>, String> {
    let mut out = BTreeMap::new();
    let mut previous: Option<String> = None;
    for (i, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let n = i + 1;
        let (path, count) = line
            .rsplit_once([' ', '\t'])
            .ok_or_else(|| format!("line {n}: expected `<path> <count>`, got {raw:?}"))?;
        let path = path.trim_end();
        let count: usize = count
            .parse()
            .map_err(|_| format!("line {n}: count {count:?} is not a number"))?;
        if count == 0 {
            return Err(format!(
                "line {n}: a file with no fixed deadlines has no entry; remove {raw:?}"
            ));
        }
        if let Some(prev) = previous.as_deref().filter(|prev| *prev >= path) {
            return Err(format!(
                "line {n}: entries must be sorted by path and unique, but {path:?} follows {prev:?}"
            ));
        }
        previous = Some(path.to_string());
        out.insert(path.to_string(), count);
    }
    Ok(out)
}

/// The failure report, empty when the tree matches the baseline and every
/// marker is well-formed.
fn report(
    deadlines: &BTreeMap<String, Vec<Deadline>>,
    baseline: &BTreeMap<String, usize>,
) -> String {
    let mut out = Vec::new();
    for (path, found) in deadlines {
        for d in found.iter().filter(|d| d.marker_malformed) {
            out.push(format!(
                "{path}:{}: {}\n  fixed-deadline marker is malformed: expected \
                 `// {TAG}: allow — <reason>` on its own line; the reason is required.",
                d.line, d.text
            ));
        }
        let count = found.len();
        match baseline.get(path).copied() {
            Some(allowed) if count == allowed => {}
            None if count == 0 => {}
            Some(allowed) if count < allowed => out.push(if count == 0 {
                format!(
                    "{path}: no fixed deadlines left but the baseline allows {allowed}; the ratchet \
                     only moves down — remove its line from {BASELINE_FILE}: `{path} {allowed}`"
                )
            } else {
                format!(
                    "{path}: {count} fixed deadline(s) but the baseline allows {allowed}; the ratchet \
                     only moves down — replace its line in {BASELINE_FILE} with: `{path} {count}`"
                )
            }),
            allowed => {
                let allowed = allowed.map_or_else(
                    || "the baseline has no entry for it".to_string(),
                    |n| format!("the baseline allows {n}"),
                );
                out.push(format!("{path}: {count} fixed test deadline(s), {allowed}:"));
                for d in found {
                    out.push(format!("  {path}:{}: {}", d.line, d.text));
                }
                out.push(format!(
                    "  fix: wrap the budget in `intentd_test_support::test_timeout(…)` (or the \
                     crate's helper that delegates to it) so INTENTD_TEST_TIMEOUT_MULTIPLIER \
                     stretches it on slow coverage runners; for a deliberately short window, put \
                     `// {TAG}: allow — <reason>` on the line above."
                ));
            }
        }
    }
    for (path, allowed) in baseline {
        if !deadlines.contains_key(path) {
            out.push(format!(
                "{path}: no longer exists; remove its line from {BASELINE_FILE}: `{path} {allowed}`"
            ));
        }
    }
    out.join("\n")
}

#[test]
fn fixed_test_deadlines_use_the_timeout_multiplier() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join("..");
    let baseline_text = fs::read_to_string(root.join(BASELINE_FILE))
        .unwrap_or_else(|e| panic!("read {BASELINE_FILE}: {e}"));
    let baseline =
        parse_baseline(&baseline_text).unwrap_or_else(|e| panic!("{BASELINE_FILE}: {e}"));
    let deadlines = scan_tree(&root);
    assert!(
        deadlines.contains_key("crates/intent-store/src/tests/note_pages.rs")
            && deadlines.contains_key("crates/intent-services/src/test_support.rs"),
        "test-code discovery lost a known test file; scanned {} files",
        deadlines.len()
    );
    if std::env::var_os("INTENTD_UPDATE_FIXED_DEADLINE_BASELINE").is_some() {
        let header = baseline_text.lines().take_while(|l| l.starts_with('#'));
        let entries = deadlines
            .iter()
            .filter(|(_, found)| !found.is_empty())
            .map(|(path, found)| format!("{path} {}", found.len()));
        let lines: Vec<String> = header.map(str::to_string).chain(entries).collect();
        fs::write(root.join(BASELINE_FILE), lines.join("\n") + "\n").expect("write baseline");
        return;
    }
    let report = report(&deadlines, &baseline);
    assert!(
        report.is_empty(),
        "fixed test deadlines out of step with {BASELINE_FILE}:\n\n{report}\n"
    );
}

// ---- fixtures ---------------------------------------------------------------

fn lines_in(src: &str) -> Vec<usize> {
    scan_source(src, true)
        .deadlines
        .iter()
        .map(|d| d.line)
        .collect()
}

#[test]
fn a_new_fixed_five_second_deadline_fails_and_the_helper_passes() {
    let fixed = "\
async fn t() {
    tokio::time::timeout(Duration::from_secs(5), rx.recv()).await.unwrap();
}
";
    assert_eq!(lines_in(fixed), vec![2]);
    let scaled = "\
async fn t() {
    tokio::time::timeout(test_timeout(Duration::from_secs(5)), rx.recv()).await.unwrap();
    tokio::time::timeout(intentd_test_support::test_timeout(Duration::from_secs(5)), f).await;
    timeout(common::rpc_read_timeout(), f).await;
    timeout(wait(), f).await;
}
";
    assert_eq!(lines_in(scaled), Vec::<usize>::new());
}

#[test]
fn every_deadline_shape_is_caught() {
    let src = "\
fn t() {
    timeout(Duration::from_millis(500), f);
    tokio::time::timeout(std::time::Duration::from_secs(5), f);
    time::timeout(
        Duration::from_secs(5),
        f,
    );
    rx.recv_timeout(Duration::from_secs(1));
    child.wait_with_timeout(Duration::from_secs(30));
    let deadline = Instant::now() + Duration::from_secs(10);
    let deadline = tokio::time::Instant::now()
        + std::time::Duration::from_secs(10);
    let deadline = Instant::now().checked_add(Duration::from_secs(1));
    timeout ( Duration :: from_secs_f64 ( 0.5 ) , f);
}
";
    assert_eq!(lines_in(src), vec![2, 3, 4, 8, 9, 10, 11, 13, 14]);
}

#[test]
fn non_deadlines_never_count() {
    let src = "\
fn timeout(d: Duration) {}
async fn t() {
    // timeout(Duration::from_secs(5), f)
    let s = \"timeout(Duration::from_secs(5), f)\";
    /* timeout(Duration::from_secs(5), f) */
    my_timeout(Duration::from_secs(5));
    with_settle_timeout(Duration::from_secs(5));
    timeout_ms(Duration::from_secs(5));
    timeout(BUDGET, f);
    timeout(d, f);
    let tick = tokio::time::interval(Duration::from_secs(5));
    let later = Instant::now() + budget;
    let later = start + Duration::from_secs(5);
    let d = Duration::from_secs(5);
}
";
    assert_eq!(lines_in(src), Vec::<usize>::new());
}

#[test]
fn only_test_code_is_scanned_in_a_production_file() {
    let src = "\
fn production() {
    let _ = timeout(Duration::from_secs(5), f);
}

#[cfg(test)]
mod tests {
    fn t() {
        let _ = timeout(Duration::from_secs(5), f);
    }
}

#[cfg(test)]
mod helpers;
#[cfg(test)]
#[path = \"thing/tests/extra.rs\"]
mod extra;
";
    let scan = scan_source(src, false);
    assert_eq!(
        scan.deadlines.iter().map(|d| d.line).collect::<Vec<_>>(),
        vec![8]
    );
    assert_eq!(
        scan.test_mods,
        vec![
            ("helpers".to_string(), None),
            (
                "extra".to_string(),
                Some("thing/tests/extra.rs".to_string())
            ),
        ]
    );
    assert_eq!(
        scan_source(src, true)
            .deadlines
            .iter()
            .map(|d| d.line)
            .collect::<Vec<_>>(),
        vec![2, 8]
    );
}

#[test]
fn test_module_files_resolve_like_rustc() {
    let lib = Path::new("crates/a/src/lib.rs");
    assert_eq!(
        test_mod_targets(lib, "helpers", None),
        vec![
            PathBuf::from("crates/a/src/helpers.rs"),
            PathBuf::from("crates/a/src/helpers"),
        ]
    );
    let file = Path::new("crates/a/src/store.rs");
    assert_eq!(
        test_mod_targets(file, "harness", None),
        vec![
            PathBuf::from("crates/a/src/store/harness.rs"),
            PathBuf::from("crates/a/src/store/harness"),
        ]
    );
    assert_eq!(
        test_mod_targets(file, "x", Some("store/t/x.rs")),
        vec![
            PathBuf::from("crates/a/src/store/t/x"),
            PathBuf::from("crates/a/src/store/t/x.rs"),
        ]
    );
    for (rel, expected) in [
        ("tests.rs", true),
        ("a/tests.rs", true),
        ("a/bus_tests.rs", true),
        ("a/tests/mod.rs", true),
        ("a/tests/b/c.rs", true),
        ("a/latests.rs", false),
        ("a/test_support.rs", false),
        ("lib.rs", false),
    ] {
        assert_eq!(is_conventional_test_file(Path::new(rel)), expected, "{rel}");
    }
}

#[test]
fn marker_above_the_line_or_statement_exempts_and_needs_a_reason() {
    let src = "\
async fn t() {
    // fixed-deadline: allow — asserts nothing arrives within the window
    assert!(timeout(Duration::from_millis(50), rx.recv()).await.is_err());
    // fixed-deadline: allow — the statement's first line counts
    let quiet =
        tokio::time::timeout(Duration::from_millis(50), rx.recv());
    // fixed-deadline: allow
    timeout(Duration::from_secs(5), f);
    timeout(Duration::from_secs(5), f); // fixed-deadline: allow — trailing does not count
    // fixed-deadline: allow — two lines above does not count
    let _ = 1;
    timeout(Duration::from_secs(5), f);
}
";
    let found = scan_source(src, true).deadlines;
    assert_eq!(
        found
            .iter()
            .map(|d| (d.line, d.marker_malformed))
            .collect::<Vec<_>>(),
        vec![(8, true), (9, false), (12, false)]
    );
}

#[test]
fn baseline_parses_and_rejects_malformed_entries() {
    assert_eq!(
        parse_baseline("# c\n\ncrates/a/tests/x.rs 3\ncrates/b/src/tests.rs\t1\n").unwrap(),
        BTreeMap::from([
            ("crates/a/tests/x.rs".to_string(), 3),
            ("crates/b/src/tests.rs".to_string(), 1),
        ])
    );
    for (text, expected) in [
        ("crates/a/tests/x.rs", "expected `<path> <count>`"),
        ("crates/a/tests/x.rs many", "is not a number"),
        ("crates/a/tests/x.rs 0", "has no entry"),
        (
            "crates/b/x.rs 1\ncrates/a/x.rs 1\n",
            "sorted by path and unique",
        ),
        (
            "crates/a/x.rs 1\ncrates/a/x.rs 2\n",
            "sorted by path and unique",
        ),
    ] {
        let err = parse_baseline(text).expect_err(text);
        assert!(err.contains(expected), "{text:?}: {err}");
    }
}

#[test]
fn report_follows_the_ratchet() {
    let d = |line, marker_malformed| Deadline {
        line,
        text: "timeout(Duration::from_secs(5), f);".to_string(),
        marker_malformed,
    };
    let found = BTreeMap::from([
        ("crates/a/tests/equal.rs".to_string(), vec![d(1, false)]),
        ("crates/a/tests/new.rs".to_string(), vec![d(7, false)]),
        (
            "crates/a/tests/over.rs".to_string(),
            vec![d(3, false), d(9, false)],
        ),
        ("crates/a/tests/under.rs".to_string(), vec![d(4, false)]),
        ("crates/a/tests/fixed.rs".to_string(), vec![]),
        ("crates/a/tests/clean.rs".to_string(), vec![]),
        ("crates/a/tests/marked.rs".to_string(), vec![d(5, true)]),
    ]);
    let baseline = BTreeMap::from([
        ("crates/a/tests/equal.rs".to_string(), 1),
        ("crates/a/tests/over.rs".to_string(), 1),
        ("crates/a/tests/under.rs".to_string(), 2),
        ("crates/a/tests/fixed.rs".to_string(), 1),
        ("crates/a/tests/gone.rs".to_string(), 4),
        ("crates/a/tests/marked.rs".to_string(), 1),
    ]);
    let matching = BTreeMap::from([("crates/a/tests/x.rs".to_string(), vec![d(1, false)])]);
    let allowed = BTreeMap::from([("crates/a/tests/x.rs".to_string(), 1)]);
    assert_eq!(report(&matching, &allowed), "");
    let report = report(&found, &baseline);
    for expected in [
        "crates/a/tests/new.rs: 1 fixed test deadline(s), the baseline has no entry for it:",
        "  crates/a/tests/new.rs:7: timeout(Duration::from_secs(5), f);",
        "crates/a/tests/over.rs: 2 fixed test deadline(s), the baseline allows 1:",
        "  crates/a/tests/over.rs:9: timeout(Duration::from_secs(5), f);",
        "wrap the budget in `intentd_test_support::test_timeout(…)`",
        "replace its line in crates/intent-core/tests/fixed_deadline_baseline.txt with: `crates/a/tests/under.rs 1`",
        "remove its line from crates/intent-core/tests/fixed_deadline_baseline.txt: `crates/a/tests/fixed.rs 1`",
        "crates/a/tests/gone.rs: no longer exists",
        "crates/a/tests/marked.rs:5: timeout(Duration::from_secs(5), f);\n  fixed-deadline marker is malformed",
    ] {
        assert!(report.contains(expected), "missing {expected:?} in:\n{report}");
    }
    assert!(!report.contains("equal.rs"), "{report}");
    assert!(!report.contains("clean.rs"), "{report}");
}
