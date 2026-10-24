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
//! - **Fix — decide what the wait brackets:**
//!   - a **hang guard** waits for something that should happen (a reply, an
//!     event, a task finishing, a polled condition): wrap the budget in
//!     `intentd_test_support::test_timeout(…)` (or a crate-local helper that
//!     delegates to it: `common::test_timeout`,
//!     `test_support::fixture_timeout`, …). A "nothing arrives" check with no
//!     production timer behind it is a hang guard too — a longer window only
//!     makes it stricter.
//!   - an **observation window** proves nothing happens and must stay shorter
//!     than a fixed production timer or debounce (an 800 ms silence limit, a
//!     100 ms idle debounce, a 10 s handshake): keep it fixed and mark it with
//!     the marker below, naming that timer. Stretching it lets the timer fire
//!     inside the window — three intent-services tests failed only at 3x for
//!     that reason (intentd#2568, intentd#2576 reviews).
//! - **What is test code:** every file under `crates/*/tests/`, and inside
//!   `crates/*/src/`: files named `tests.rs` / `*_tests.rs`, anything under a
//!   `tests/` directory, files reached through a `#[cfg(test)] mod name;`
//!   declaration (and that module's directory, and any `mod` a test file
//!   declares in turn), and every `#[cfg(test)]` or `#[cfg(all(…, test,
//!   …))]` item, plus `cfg(any(test, feature = "test-hooks"))` ones (the
//!   `test-hooks` feature never builds into production:
//!   scripts/check-test-hooks-feature.sh). Other production code — including
//!   other `cfg(any(test, …))` items, which also build outside tests — is
//!   never scanned.
//! - **What counts:** on comment- and literal-blanked text (so quoted
//!   examples never count), a `Duration::from_*(` path — `std::time::` /
//!   `tokio::time::` qualified or not, any whitespace or line breaks between
//!   tokens — not scaled afterwards (`.mul_f64(m)`, `* m`) and used directly
//!   as (a) the budget argument of a call named in [`DEADLINE_CALLS`]
//!   (`tokio::time::timeout`, `recv_timeout`, `Condvar::wait_timeout`'s
//!   second argument, …; a `fn timeout(` definition is not a call), or (b) the operand of an
//!   `Instant::now() + …` / `Instant::now().checked_add(…)` deadline.
//! - **Marker:** a standalone `// fixed-deadline: allow — <reason>` on the
//!   line above the hit, or above the first line of its statement, exempts it
//!   — for an observation window; write the reason as
//!   `stays under <timer or constant>` so a reader can check the
//!   relationship. A marker without a reason never exempts and is itself
//!   reported.
//! - **Baseline** (`tests/fixed_deadline_baseline.txt`): one `<path> <count>`
//!   line per file that still has unmarked fixed deadlines, sorted by path. It
//!   only ratchets down: a file over its entry (or absent) fails naming every
//!   line; a file under its entry, or an entry whose file is gone, fails
//!   naming the exact line to write.
//!   To rewrite it after a bulk conversion, run the lint with
//!   `INTENTD_UPDATE_FIXED_DEADLINE_BASELINE=1`; update mode refuses to raise
//!   or add an entry, or to record a malformed marker.
//! - **Limits:** a fixed budget behind a `const` or a local binding
//!   (`const WAIT: Duration = Duration::from_secs(5); timeout(WAIT, …)`) is
//!   not seen; a call is matched by name only, so a method also called
//!   `timeout` (a client builder's) counts too — wrap or mark it; and a
//!   `#[cfg(test)] mod x;` nested inside an inline `mod a { … }` resolves as
//!   if declared at file level.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use intentd_test_support::source_lint::{
    cfg_test_items, lex, markers_by_line, rust_files, skip_whitespace, statement_line, Literal,
    Marker,
};

/// Opt-out tag: `// fixed-deadline: allow — <reason>`.
const TAG: &str = "fixed-deadline";

/// Calls taking a wall-clock budget, with the 0-based argument positions it
/// may sit at: `Condvar::wait_timeout(guard, budget)` /
/// `wait_timeout_while(guard, budget, f)` take it second, a child-process
/// `wait_timeout(budget)` first.
const DEADLINE_CALLS: &[(&str, &[usize])] = &[
    ("timeout", &[0]),
    ("recv_timeout", &[0]),
    ("wait_timeout", &[0, 1]),
    ("wait_timeout_while", &[1]),
    ("wait_with_timeout", &[0]),
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
    if ty != "Duration" || !ctor.starts_with("from_") {
        return false;
    }
    let Some(open) = expect(chars, j, "(") else {
        return false;
    };
    !is_scaled_after(chars, close_paren(chars, open))
}

/// Start index of each top-level argument of the call whose `(` ends just
/// before `open` (nested `(…)`, `[…]`, `{…}` respected).
fn argument_starts(chars: &[char], open: usize) -> Vec<usize> {
    let mut starts = vec![open];
    let mut depth = 0usize;
    for (j, c) in chars.iter().enumerate().skip(open) {
        match c {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' if depth == 0 => break,
            ')' | ']' | '}' => depth -= 1,
            ',' if depth == 0 => starts.push(j + 1),
            _ => {}
        }
    }
    starts
}

/// Index just past the `)` closing the `(` that ends just before `open`.
fn close_paren(chars: &[char], open: usize) -> usize {
    let mut depth = 1usize;
    let mut j = open;
    while j < chars.len() && depth > 0 {
        match chars[j] {
            '(' => depth += 1,
            ')' => depth -= 1,
            _ => {}
        }
        j += 1;
    }
    j
}

/// Does the `Duration` ending at `j` go on to be scaled at runtime —
/// `.mul_f64(<factor>)` / `.mul_f32(<factor>)` or `* <factor>`, where the
/// factor starts with an identifier (`timeout_multiplier()`, `m`)? A literal
/// factor (`* 2`, `.mul_f64(1.0)`) leaves the budget fixed.
fn is_scaled_after(chars: &[char], j: usize) -> bool {
    let non_literal = |k: usize| {
        chars
            .get(skip_whitespace(chars, k))
            .is_some_and(|c| c.is_alphabetic() || *c == '_')
    };
    let by_method = ["mul_f64", "mul_f32"].iter().any(|method| {
        [".", method, "("]
            .iter()
            .try_fold(j, |k, token| expect(chars, k, token))
            .is_some_and(non_literal)
    });
    by_method || expect(chars, j, "*").is_some_and(non_literal)
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
        let call = DEADLINE_CALLS.iter().find(|(name, _)| *name == word);
        let hit = if let Some((_, positions)) = call {
            word_before(chars, i) != "fn"
                && expect(chars, end, "(").is_some_and(|open| {
                    argument_starts(chars, open)
                        .iter()
                        .enumerate()
                        .any(|(n, j)| positions.contains(&n) && is_duration_literal(chars, *j))
                })
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

/// Rewrites every `#[cfg(all(…, test, …))]` attribute in blanked `chars` to
/// a padded `#[cfg(test)]` (length and newlines preserved), so
/// [`cfg_test_items`] — which matches only the bare form — finds those test
/// modules too. `any(…)` / `not(…)` predicates are left alone: they also
/// compile outside tests.
fn normalize_cfg_all_test(chars: &mut [char]) {
    let mut i = 0;
    while i < chars.len() {
        let head = (chars[i] == '#')
            .then(|| {
                ["#", "[", "cfg", "(", "all", "("]
                    .iter()
                    .try_fold(i, |j, token| expect(chars, j, token))
            })
            .flatten();
        let Some(args_start) = head else {
            i += 1;
            continue;
        };
        let args_end = close_paren(chars, args_start);
        let args: String = chars[args_start..args_end.saturating_sub(1)]
            .iter()
            .collect();
        let mut depth = 0usize;
        let mut has_test = false;
        let mut current = String::new();
        for c in args.chars().chain(std::iter::once(',')) {
            match c {
                '(' => depth += 1,
                ')' => depth = depth.saturating_sub(1),
                ',' if depth == 0 => {
                    has_test |= current.trim() == "test";
                    current.clear();
                    continue;
                }
                _ => {}
            }
            current.push(c);
        }
        let Some(end) = [")", "]"]
            .iter()
            .try_fold(args_end, |j, token| expect(chars, j, token))
        else {
            i += 1;
            continue;
        };
        if has_test {
            let mut replacement = "#[cfg(test)]".chars();
            for c in chars[i..end].iter_mut().filter(|c| **c != '\n') {
                *c = replacement.next().unwrap_or(' ');
            }
        }
        i = end;
    }
}

/// Rewrites every `#[cfg(any(test, feature = "test-hooks"))]` attribute in
/// blanked `chars` to a padded `#[cfg(test)]`, like
/// [`normalize_cfg_all_test`]: the `test-hooks` feature only builds test
/// seams for other crates' tests, and CI keeps it out of the production
/// feature graph (scripts/check-test-hooks-feature.sh). The feature name is
/// blanked with the literal, so it is read from `literals`.
fn normalize_cfg_test_hooks(chars: &mut [char], literals: &[Literal]) {
    let mut i = 0;
    while i < chars.len() {
        let head = (chars[i] == '#')
            .then(|| {
                [
                    "#", "[", "cfg", "(", "any", "(", "test", ",", "feature", "=",
                ]
                .iter()
                .try_fold(i, |j, token| expect(chars, j, token))
            })
            .flatten();
        let Some(after_eq) = head else {
            i += 1;
            continue;
        };
        // The blanked literal reads as whitespace, so find it by offset: the
        // first literal at or after `=` with only whitespace before it.
        let end = literals
            .iter()
            .find(|l| l.offset >= after_eq)
            .filter(|l| chars[after_eq..l.offset].iter().all(|c| c.is_whitespace()))
            .filter(|l| !l.raw && l.text == "test-hooks")
            .and_then(|l| {
                [")", ")", "]"]
                    .iter()
                    .try_fold(l.offset + l.text.chars().count() + 2, |k, token| {
                        expect(chars, k, token)
                    })
            });
        let Some(end) = end else {
            i += 1;
            continue;
        };
        let mut replacement = "#[cfg(test)]".chars();
        for c in chars[i..end].iter_mut().filter(|c| **c != '\n') {
            *c = replacement.next().unwrap_or(' ');
        }
        i = end;
    }
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
/// and the out-of-line modules whose files are test code too.
#[derive(Debug, Default, PartialEq, Eq)]
struct Scan {
    deadlines: Vec<Deadline>,
    /// `(name, Some(path attr))` per out-of-line test module: every
    /// `#[cfg(test)] mod name;`, plus every `mod name;` of a whole test file.
    test_mods: Vec<(String, Option<String>)>,
}

/// Every plain `mod name;` declaration in blanked `chars`: the char range of
/// the item (from after the previous `;` / `{` / `}` through its `;`, so it
/// covers any `#[path = "…"]`) and the name.
fn out_of_line_mods(chars: &[char]) -> Vec<(usize, usize, String)> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        let at_word = is_ident(chars[i]) && (i == 0 || !is_ident(chars[i - 1]));
        let Some((word, end)) = at_word.then(|| ident_at(chars, i)).flatten() else {
            i += 1;
            continue;
        };
        if word == "mod" {
            let name_at = skip_whitespace(chars, end);
            if let Some((name, after)) = ident_at(chars, name_at) {
                if let Some(semi) = expect(chars, after, ";") {
                    let start = chars[..i]
                        .iter()
                        .rposition(|c| matches!(c, ';' | '{' | '}'))
                        .map_or(0, |b| b + 1);
                    out.push((start, semi, name));
                }
            }
        }
        i = end;
    }
    out
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
    let mut chars: Vec<char> = lexed.blanked.chars().collect();
    normalize_cfg_all_test(&mut chars);
    normalize_cfg_test_hooks(&mut chars, &lexed.literals);
    let line_starts: Vec<usize> = std::iter::once(0)
        .chain(
            chars
                .iter()
                .enumerate()
                .filter(|(_, c)| **c == '\n')
                .map(|(i, _)| i + 1),
        )
        .collect();
    let line_of = |offset: usize| line_starts.partition_point(|start| *start <= offset);
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
            let line = line_of(offset);
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
    let mut test_mods: Vec<(String, Option<String>)> = items
        .iter()
        .filter_map(|item| {
            let name = item.out_of_line_mod.clone()?;
            Some((
                name,
                path_attr(&chars, &lexed.literals, item.start, item.end),
            ))
        })
        .collect();
    if whole_file_is_test {
        for (start, end, name) in out_of_line_mods(&chars) {
            let entry = (name, path_attr(&chars, &lexed.literals, start, end));
            if !test_mods.contains(&entry) {
                test_mods.push(entry);
            }
        }
    }
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

/// What [`scan_tree`] found.
struct Tree {
    /// Unmarked fixed deadlines per scanned file (zero-count files
    /// included), keyed by workspace-relative path.
    deadlines: BTreeMap<String, Vec<Deadline>>,
    /// Files scanned whole as test code, workspace-relative.
    whole: BTreeSet<String>,
}

/// Scans every crate: `tests/` files and conventional `src/` test files
/// whole, other `src/` files for their `#[cfg(test)]` items, then — until
/// nothing changes — promotes to whole every `src/` file an out-of-line test
/// module resolves to (including modules declared by a promoted file).
fn scan_tree(root: &Path) -> Tree {
    let crates_dir = root.join("crates");
    let mut krates: Vec<PathBuf> = fs::read_dir(&crates_dir)
        .unwrap_or_else(|e| panic!("read_dir {}: {e}", crates_dir.display()))
        .map(|entry| entry.expect("crate dir entry").path())
        .filter(|path| path.is_dir())
        .collect();
    krates.sort();
    let mut pending = Vec::new();
    let mut partial = Vec::new();
    for krate in &krates {
        let tests = krate.join("tests");
        if tests.is_dir() {
            pending.extend(rust_files(&tests));
        }
        let src = krate.join("src");
        if src.is_dir() {
            for file in rust_files(&src) {
                let rel = file.strip_prefix(&src).expect("under src").to_path_buf();
                if is_conventional_test_file(&rel) {
                    pending.push(file);
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
    let mut targets = Vec::new();
    let mut sources = BTreeMap::new();
    for file in &partial {
        let src = read(file);
        let scan = scan_source(&src, false);
        for (name, path) in &scan.test_mods {
            targets.extend(test_mod_targets(file, name, path.as_deref()));
        }
        scans.insert(file.clone(), scan);
        sources.insert(file.clone(), src);
    }
    let mut whole = BTreeSet::new();
    loop {
        pending.extend(
            partial
                .iter()
                .filter(|file| !whole.contains(*file))
                .filter(|file| targets.iter().any(|target| file.starts_with(target)))
                .cloned(),
        );
        if pending.is_empty() {
            break;
        }
        for file in std::mem::take(&mut pending) {
            if !whole.insert(file.clone()) {
                continue;
            }
            let scan = match sources.get(&file) {
                Some(src) => scan_source(src, true),
                None => scan_source(&read(&file), true),
            };
            for (name, path) in &scan.test_mods {
                targets.extend(test_mod_targets(&file, name, path.as_deref()));
            }
            scans.insert(file, scan);
        }
    }
    let rel = |file: &Path| display_rel(file.strip_prefix(root).expect("under root"));
    Tree {
        deadlines: scans
            .into_iter()
            .map(|(file, scan)| (rel(&file), scan.deadlines))
            .collect(),
        whole: whole.iter().map(|file| rel(file)).collect(),
    }
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
                 `// {TAG}: allow — <reason>` on its own line; the reason is required and \
                 should name the fixed timer the window stays under.",
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
                    "  fix: decide what each wait brackets.\n  \
                     - hang guard (the wait is for something that should happen): wrap the budget \
                     in `intentd_test_support::test_timeout(…)` (or the crate's helper that \
                     delegates to it) so INTENTD_TEST_TIMEOUT_MULTIPLIER stretches it on slow \
                     coverage runners. A \"nothing arrives\" check with no production timer \
                     behind it is a hang guard too: a longer window only makes it stricter.\n  \
                     - observation window (the wait proves nothing happens and must stay shorter \
                     than a fixed production timer or debounce): keep it fixed and put \
                     `// {TAG}: allow — stays under <timer>` on the line above, naming the timer \
                     or constant. Stretching it would let that timer fire inside the window and \
                     break the test on slow runners — never stretch an observation window by \
                     reflex."
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
    let Tree { deadlines, whole } = scan_tree(&root);
    for known in [
        "crates/intent-store/src/tests/write_acquire_retry.rs",
        "crates/intent-services/src/test_support.rs",
        "crates/intentd/tests/common/mod.rs",
    ] {
        assert!(
            whole.contains(known),
            "test-code discovery no longer scans {known} as test code; {} files scanned whole",
            whole.len()
        );
    }
    if std::env::var_os("INTENTD_UPDATE_FIXED_DEADLINE_BASELINE").is_some() {
        let raised: Vec<String> = deadlines
            .iter()
            .filter(|(path, found)| found.len() > baseline.get(*path).copied().unwrap_or(0))
            .map(|(path, found)| format!("{path} {}", found.len()))
            .collect();
        let malformed = deadlines.values().flatten().any(|d| d.marker_malformed);
        assert!(
            raised.is_empty() && !malformed,
            "update mode only lowers {BASELINE_FILE}; fix these first:\n{}",
            report(&deadlines, &baseline)
        );
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
    let (g, _) = cv.wait_timeout(lock(&m, [1, 2]), Duration::from_secs(5)).unwrap();
    let (g, _) = cv.wait_timeout_while(g, Duration::from_secs(5), |s| !*s).unwrap();
    cv.wait_timeout(g, test_timeout(Duration::from_secs(5)));
    timeout(f, Duration::from_secs(5));
}
";
    assert_eq!(lines_in(src), vec![2, 3, 4, 8, 9, 10, 11, 13, 14, 15, 16]);
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
        "hang guard (the wait is for something that should happen): wrap the budget in \
         `intentd_test_support::test_timeout(…)`",
        "observation window (the wait proves nothing happens and must stay shorter than a \
         fixed production timer or debounce): keep it fixed and put \
         `// fixed-deadline: allow — stays under <timer>` on the line above",
        "never stretch an observation window",
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

#[test]
fn cfg_any_test_or_test_hooks_items_are_test_code() {
    let src = "\
#[cfg(any(test, feature = \"test-hooks\"))]
fn seam() { timeout(Duration::from_secs(5), f); }
#[cfg(any( test , feature=\"test-hooks\" ))]
mod spaced {
    fn t() { timeout(Duration::from_secs(5), f); }
}
#[cfg(any(test, feature = \"test-hooks-docs\"))]
fn look_alike() { timeout(Duration::from_secs(5), f); }
#[cfg(any(test, feature = \"fixtures\"))]
fn shared() { timeout(Duration::from_secs(5), f); }
#[cfg(any(test, feature = r\"test-hooks\"))]
fn raw() { timeout(Duration::from_secs(5), f); }
";
    let found = scan_source(src, false).deadlines;
    assert_eq!(found.iter().map(|d| d.line).collect::<Vec<_>>(), vec![2, 5]);
}

#[test]
fn cfg_all_test_items_are_test_code_but_cfg_any_test_is_not() {
    let src = "\
#[cfg(all(test, unix))]
mod unix_tests {
    fn t() { timeout(Duration::from_secs(5), f); }
}
#[cfg(all(unix, test))]
fn helper() { timeout(Duration::from_secs(5), f); }
#[cfg(all(
    test,
    any(unix, windows)
))]
mod multi_line {
    fn t() { timeout(Duration::from_secs(5), f); }
}
#[cfg(any(test, feature = \"fixtures\"))]
fn shared() { timeout(Duration::from_secs(5), f); }
#[cfg(not(test))]
fn prod() { timeout(Duration::from_secs(5), f); }
#[cfg(all(not(test), unix))]
fn prod_unix() { timeout(Duration::from_secs(5), f); }
#[cfg(all(testing, unix))]
fn other() { timeout(Duration::from_secs(5), f); }
";
    let found = scan_source(src, false).deadlines;
    assert_eq!(
        found.iter().map(|d| d.line).collect::<Vec<_>>(),
        vec![3, 6, 12]
    );
}

#[test]
fn a_budget_scaled_at_runtime_is_not_fixed() {
    let src = "\
fn t() {
    timeout(Duration::from_secs(10).mul_f64(timeout_multiplier()), f);
    rx.recv_timeout(Duration::from_secs(10).mul_f32(m));
    let deadline = Instant::now() + Duration::from_secs(10) * multiplier;
    timeout(Duration::from_secs(5) * 2, f);
    timeout(Duration::from_millis(u64::from(n)).mul_f64(m), f);
    timeout(Duration::from_millis(u64::from(n)) + Duration::from_secs(1), f);
    timeout(Duration::from_secs(5).mul_f64(1.0), f);
    timeout(Duration::from_secs(5).mul_f32( 2.5 ), f);
}
";
    assert_eq!(lines_in(src), vec![5, 7, 8, 9]);
}

#[test]
fn a_whole_test_file_makes_its_out_of_line_modules_test_code() {
    let src = "\
mod helpers;
#[path = \"shared/fixtures.rs\"]
pub(crate) mod fixtures;
mod inline { }
use some::module;
";
    assert_eq!(
        scan_source(src, true).test_mods,
        vec![
            ("helpers".to_string(), None),
            (
                "fixtures".to_string(),
                Some("shared/fixtures.rs".to_string())
            ),
        ]
    );
    assert_eq!(scan_source(src, false).test_mods, vec![]);
    assert_eq!(
        test_mod_targets(Path::new("crates/a/src/bus_tests.rs"), "helpers", None),
        vec![
            PathBuf::from("crates/a/src/bus_tests/helpers.rs"),
            PathBuf::from("crates/a/src/bus_tests/helpers"),
        ]
    );
}
