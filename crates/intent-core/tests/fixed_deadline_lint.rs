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
//!   The same positions also count a bare name bound in test code to exactly
//!   such a literal: `const` / `static NAME: Duration = Duration::from_*(…)…;`
//!   (visible throughout its enclosing `{…}` block or file) or
//!   `let [mut] name[: Duration] = Duration::from_*(…)…;` (visible after its
//!   statement, inside its block, until a later `let` of that name shadows
//!   it; a `let mut` assigned again in its block is not followed) — the
//!   initializer counts as it would at the call — unless the use is scaled (`WAIT * m`, `WAIT.mul_f64(m)`) — intentd#2649
//!   passed with `const LIMIT: Duration = Duration::from_secs(60)`. The
//!   report names the binding (`[budget: const WAIT (line 21)]`). A const
//!   cannot call the helper, so a hang-guard constant becomes a helper fn
//!   (`fn wait() -> Duration { test_timeout(Duration::from_secs(15)) }`).
//! - **Marker:** a standalone `// fixed-deadline: allow — <reason>` on the
//!   line above the hit, or above the first line of its statement, exempts it
//!   — for an observation window; write the reason as
//!   `stays under <timer or constant>` so a reader can check the
//!   relationship. The same marker above a `const` / `static` / `let`
//!   binding exempts every use of it. A marker without a reason never
//!   exempts and is itself reported.
//! - **Baseline** (`tests/fixed_deadline_baseline.txt`): one `<path> <count>`
//!   line per file that still has unmarked fixed deadlines, sorted by path. It
//!   only ratchets down: a file over its entry (or absent) fails naming every
//!   line; a file under its entry, or an entry whose file is gone, fails
//!   naming the exact line to write.
//!   To rewrite it after a bulk conversion, run the lint with
//!   `INTENTD_UPDATE_FIXED_DEADLINE_BASELINE=1`; update mode refuses to raise
//!   or add an entry, or to record a malformed marker.
//! - **Limits:** a name is followed one step, within one file: a constant
//!   imported from another file (`use super::WAIT`) or reached by path
//!   (`Self::WAIT`, `super::WAIT`), a production constant used by a test, an
//!   alias chain (`let b = WAIT;`), a field or a function parameter is not
//!   seen, and a name rebound by a pattern (a closure or nested-fn parameter,
//!   `for`, `if let` / `while let`, a `match` arm) that reuses a fixed
//!   `let`'s name is mistaken for it — mark that use; a call is matched by name only, so a method also called
//!   `timeout` (a client builder's) counts too — wrap or mark it; and a
//!   `#[cfg(test)] mod x;` nested inside an inline `mod a { … }` resolves as
//!   if declared at file level.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::Path;

use intentd_test_support::source_lint::{
    markers_by_line, parse_count_baseline, skip_group, skip_whitespace, statement_line,
    test_code_files, Marker, TestCodeText,
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

/// Is the expression at `j` (after whitespace) a `…Duration::from_*(` call
/// not scaled afterwards? See [`duration_literal_end`].
fn is_duration_literal(chars: &[char], j: usize) -> bool {
    duration_literal_end(chars, j).is_some()
}

/// Index just past the `)` of the `…Duration::from_*(…)` call at `j` (after
/// whitespace), when it is one and is not scaled afterwards. Reads a
/// `::`-separated path and requires its last two segments to be `Duration`
/// and `from_<unit>`, followed by `(`.
fn duration_literal_end(chars: &[char], j: usize) -> Option<usize> {
    let mut segments = Vec::new();
    let mut j = skip_whitespace(chars, j);
    loop {
        let (word, end) = chars
            .get(j)
            .filter(|c| is_ident(**c))
            .and_then(|_| ident_at(chars, j))?;
        segments.push(word);
        let Some(next) = expect(chars, end, "::") else {
            j = end;
            break;
        };
        j = skip_whitespace(chars, next);
    }
    let [.., ty, ctor] = segments.as_slice() else {
        return None;
    };
    if ty != "Duration" || !ctor.starts_with("from_") {
        return None;
    }
    let close = skip_group(chars, expect(chars, j, "(")? - 1, '(', ')');
    (!is_scaled_after(chars, close)).then_some(close)
}

/// What sits in a budget position: a fixed `Duration` literal, or a bare
/// name (with the offset of the name) that may resolve to a binding of one.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Budget {
    Literal,
    Name(String, usize),
}

/// The budget expression at `j` (after whitespace): a fixed literal, or a
/// bare identifier that is not a call, path, method receiver or macro and is
/// not scaled at runtime (`WAIT * m`, `WAIT.mul_f64(m)`). `WAIT * 2` and
/// `WAIT + slack` keep the name: a literal factor leaves the budget fixed.
fn budget_at(chars: &[char], j: usize) -> Option<Budget> {
    if is_duration_literal(chars, j) {
        return Some(Budget::Literal);
    }
    let j = skip_whitespace(chars, j);
    if !chars.get(j).is_some_and(|c| c.is_alphabetic() || *c == '_') {
        return None;
    }
    let (name, end) = ident_at(chars, j)?;
    let next = skip_whitespace(chars, end);
    let continues = matches!(chars.get(next), Some('(' | '.' | '!' | '{'))
        || expect(chars, end, "::").is_some();
    (!continues && !is_scaled_after(chars, end)).then_some(Budget::Name(name, j))
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

/// A candidate deadline: the char offset of the call name / `Instant`, and
/// the budgets in its budget position(s).
struct Candidate {
    offset: usize,
    budgets: Vec<Budget>,
}

/// Every call or `Instant::now()` deadline in `chars` (blanked source) whose
/// budget is a fixed literal or a bare name — [`scan_source`] decides which
/// names resolve to a fixed binding.
fn deadline_candidates(chars: &[char]) -> Vec<Candidate> {
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
        let budgets: Vec<Budget> = if let Some((_, positions)) = call {
            if word_before(chars, i) == "fn" {
                Vec::new()
            } else {
                expect(chars, end, "(").map_or_else(Vec::new, |open| {
                    argument_starts(chars, open)
                        .iter()
                        .enumerate()
                        .filter(|(n, _)| positions.contains(n))
                        .filter_map(|(_, j)| budget_at(chars, *j))
                        .collect()
                })
            }
        } else if word == "Instant" {
            let now = ["::", "now", "(", ")"]
                .iter()
                .try_fold(end, |j, token| expect(chars, j, token));
            now.and_then(|j| {
                expect(chars, j, "+").or_else(|| {
                    [".", "checked_add", "("]
                        .iter()
                        .try_fold(j, |k, token| expect(chars, k, token))
                })
            })
            .and_then(|k| budget_at(chars, k))
            .into_iter()
            .collect()
        } else {
            Vec::new()
        };
        if !budgets.is_empty() {
            out.push(Candidate { offset: i, budgets });
        }
        i = end;
    }
    out
}

/// A `const` / `static` / `let` binding of a plain name.
#[derive(Debug, Clone)]
struct Binding {
    /// `const`, `static` or `let`.
    kind: &'static str,
    name: String,
    /// Char offset of the keyword.
    offset: usize,
    /// Its initializer starts with a fixed `Duration` literal (counted as
    /// the same expression would be at the call), and a `let mut` is never
    /// assigned again in its scope.
    literal: bool,
    /// A `let mut` / `static mut`.
    mutable: bool,
    /// The `{…}` block (or whole file) the name is visible in.
    scope: (usize, usize),
    /// A `let` is visible after its statement; an item in its whole scope.
    visible_from: usize,
}

/// Index of the `;` ending the statement that continues at `j` (depth 0 over
/// `(…)` / `[…]` / `{…}`), or `chars.len()`.
fn statement_end(chars: &[char], j: usize) -> usize {
    let mut depth = 0usize;
    for (k, c) in chars.iter().enumerate().skip(j) {
        match c {
            '(' | '[' | '{' => depth += 1,
            ')' | ']' | '}' | ';' if depth == 0 => return k,
            ')' | ']' | '}' => depth -= 1,
            _ => {}
        }
    }
    chars.len()
}

/// The innermost `{…}` block around each of `offsets` (sorted ascending),
/// as `(open, close)` char offsets, or the whole file at top level.
fn enclosing_blocks(chars: &[char], offsets: &[usize]) -> Vec<(usize, usize)> {
    let mut close_of = BTreeMap::new();
    let mut stack = Vec::new();
    let mut open_at = Vec::with_capacity(offsets.len());
    let mut queries = offsets.iter().peekable();
    for (k, c) in chars.iter().enumerate() {
        while queries.next_if(|q| **q == k).is_some() {
            open_at.push(stack.last().copied());
        }
        match c {
            '{' => stack.push(k),
            '}' => {
                if let Some(open) = stack.pop() {
                    close_of.insert(open, k);
                }
            }
            _ => {}
        }
    }
    open_at.resize(offsets.len(), None);
    open_at
        .into_iter()
        .map(|open| {
            open.map_or((0, chars.len()), |open| {
                (open, close_of.get(&open).copied().unwrap_or(chars.len()))
            })
        })
        .collect()
}

/// Every `const NAME: T = …;`, `static [mut] NAME: T = …;` and
/// `let [mut] NAME[: T] = …;` in blanked `chars` (destructuring patterns and
/// `const fn` are skipped), marking those whose initializer starts with an
/// unscaled `Duration::from_*(…)` literal — the rule a budget written at the
/// call follows, so `let d = Duration::from_secs(5) * 2;` is fixed too.
fn bindings(chars: &[char]) -> Vec<Binding> {
    let mut found = Vec::new();
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
        let kind = match word.as_str() {
            "const" => "const",
            "static" => "static",
            "let" => "let",
            _ => {
                i = end;
                continue;
            }
        };
        let mut j = skip_whitespace(chars, end);
        let mut name = ident_at(chars, j);
        let mutable = kind != "const" && name.as_ref().is_some_and(|(w, _)| w == "mut");
        if mutable {
            j = skip_whitespace(chars, name.as_ref().map_or(j, |(_, e)| *e));
            name = ident_at(chars, j);
        }
        let Some((name, after)) = name.filter(|(w, _)| !w.starts_with(|c: char| c.is_numeric()))
        else {
            i = end;
            continue;
        };
        let init = if let Some(colon) =
            expect(chars, after, ":").filter(|k| chars.get(*k) != Some(&':'))
        {
            chars[colon..]
                .iter()
                .position(|c| matches!(c, '=' | ';' | '{' | '}'))
                .map(|n| colon + n)
                .filter(|k| chars[*k] == '=')
                .map(|k| k + 1)
        } else if kind == "let" {
            expect(chars, after, "=").filter(|k| !matches!(chars.get(*k), Some('=' | '>')))
        } else {
            None
        };
        let Some(init) = init else {
            i = end;
            continue;
        };
        found.push(Binding {
            kind,
            name,
            offset: i,
            literal: is_duration_literal(chars, init),
            mutable,
            scope: (0, 0),
            visible_from: if kind == "let" {
                statement_end(chars, init)
            } else {
                0
            },
        });
        i = end;
    }
    let offsets: Vec<usize> = found.iter().map(|b| b.offset).collect();
    for (binding, scope) in found.iter_mut().zip(enclosing_blocks(chars, &offsets)) {
        binding.scope = scope;
        if binding.kind != "let" {
            binding.visible_from = scope.0;
        }
        if binding.literal && binding.mutable {
            binding.literal = !is_assigned(chars, &binding.name, binding.visible_from, scope.1);
        }
    }
    found
}

/// Whether `name` is assigned (`name = …`, `name *= …`, …) anywhere in
/// `chars[from..to]`: a reassigned `let mut` budget may no longer be fixed.
fn is_assigned(chars: &[char], name: &str, from: usize, to: usize) -> bool {
    let mut i = from;
    while i < to.min(chars.len()) {
        let at_word = is_ident(chars[i]) && (i == 0 || !is_ident(chars[i - 1]));
        let Some((word, end)) = at_word.then(|| ident_at(chars, i)).flatten() else {
            i += 1;
            continue;
        };
        if word == name
            && !matches!(
                chars[..i].iter().rev().find(|c| !c.is_whitespace()),
                Some('.' | ':')
            )
        {
            let k = skip_whitespace(chars, end);
            let op = chars.get(k..k + 2).unwrap_or(&[]);
            let compound = matches!(op, ['+' | '-' | '*' | '/', '=']);
            let plain = op.first() == Some(&'=') && !matches!(op.get(1), Some('=' | '>'));
            if compound || plain {
                return true;
            }
        }
        i = end;
    }
    false
}

/// The binding `name` refers to at `offset`: among bindings of that name
/// visible there, the innermost scope wins, then a `let` over an item, then
/// the latest `let` (shadowing).
fn resolve<'a>(bindings: &'a [Binding], name: &str, offset: usize) -> Option<&'a Binding> {
    bindings
        .iter()
        .filter(|b| b.name == name)
        .filter(|b| (b.scope.0..b.scope.1).contains(&offset) && b.visible_from <= offset)
        .max_by_key(|b| (b.scope.0, b.kind == "let", b.offset))
}

/// A fixed deadline with no reasoned marker: its 1-based line and text.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Deadline {
    line: usize,
    text: String,
    /// The consulted marker line carries a marker with no reason.
    marker_malformed: bool,
    /// The fixed binding the budget names (`const WAIT (line 21)`), when it
    /// is not a literal at the call.
    via: Option<String>,
}

/// The unmarked fixed deadlines in the test code of `src`: the whole file
/// when `whole_file_is_test`, otherwise only its `#[cfg(test)]` items.
fn scan_source(src: &str, whole_file_is_test: bool) -> Vec<Deadline> {
    let text = TestCodeText::new(src, whole_file_is_test);
    let chars = &text.chars;
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
    let in_test = |offset: usize| text.in_test(offset);
    let markers = markers_by_line(src, &text.lexed.line_comments, TAG);
    let marker = |line: usize| {
        line.checked_sub(1)
            .and_then(|above| markers.get(above))
            .copied()
            .unwrap_or(Marker::Absent)
    };
    let lines: Vec<&str> = src.lines().collect();
    let bindings = bindings(chars);
    deadline_candidates(chars)
        .into_iter()
        .filter(|candidate| in_test(candidate.offset))
        .filter_map(|candidate| {
            let mut via = None;
            let fixed = candidate.budgets.iter().find_map(|budget| match budget {
                Budget::Literal => Some(None),
                Budget::Name(name, at) => resolve(&bindings, name, *at)
                    .filter(|b| b.literal && in_test(b.offset))
                    .map(Some),
            })?;
            let line = line_of(candidate.offset);
            let mut consulted = vec![
                marker(line),
                marker(statement_line(chars, candidate.offset)),
            ];
            if let Some(binding) = fixed {
                let binding_line = line_of(binding.offset);
                consulted.push(marker(binding_line));
                consulted.push(marker(statement_line(chars, binding.offset)));
                via = Some(format!(
                    "{} {} (line {binding_line})",
                    binding.kind, binding.name
                ));
            }
            if consulted.contains(&Marker::WithReason) {
                return None;
            }
            Some(Deadline {
                line,
                text: lines.get(line - 1).map_or("", |l| l.trim()).to_string(),
                marker_malformed: consulted.contains(&Marker::Malformed),
                via,
            })
        })
        .collect()
}

/// What [`scan_tree`] found.
struct Tree {
    /// Unmarked fixed deadlines per scanned file (zero-count files
    /// included), keyed by workspace-relative path.
    deadlines: BTreeMap<String, Vec<Deadline>>,
    /// Files scanned whole as test code, workspace-relative.
    whole: BTreeSet<String>,
}

/// Scans the test code of every crate file (see
/// [`intentd_test_support::source_lint::test_code_files`] for what counts).
fn scan_tree(root: &Path) -> Tree {
    let files = test_code_files(root);
    Tree {
        whole: files
            .iter()
            .filter(|file| file.whole)
            .map(|file| file.rel.clone())
            .collect(),
        deadlines: files
            .into_iter()
            .map(|file| {
                let found = scan_source(&file.src, file.whole);
                (file.rel, found)
            })
            .collect(),
    }
}

/// Parses the baseline: `<path> <count>` per line, blank lines and `#`
/// comments ignored, paths strictly increasing, counts positive.
fn parse_baseline(text: &str) -> Result<BTreeMap<String, usize>, String> {
    parse_count_baseline(text, "fixed deadlines")
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
                    let via = d
                        .via
                        .as_ref()
                        .map_or_else(String::new, |via| format!("  [budget: {via}]"));
                    out.push(format!("  {path}:{}: {}{via}", d.line, d.text));
                }
                out.push(format!(
                    "  fix: decide what each wait brackets.\n  \
                     - hang guard (the wait is for something that should happen): wrap the budget \
                     in `intentd_test_support::test_timeout(…)` (or the crate's helper that \
                     delegates to it) so INTENTD_TEST_TIMEOUT_MULTIPLIER stretches it on slow \
                     coverage runners. A \"nothing arrives\" check with no production timer \
                     behind it is a hang guard too: a longer window only makes it stricter. For a \
                     budget held in a `const` (a const cannot call the helper), make it a helper \
                     — `fn wait() -> Duration {{ test_timeout(Duration::from_secs(15)) }}` — or \
                     wrap each use: `timeout(test_timeout(WAIT), …)`.\n  \
                     - observation window (the wait proves nothing happens and must stay shorter \
                     than a fixed production timer or debounce): keep it fixed and put \
                     `// {TAG}: allow — stays under <timer>` on the line above, naming the timer \
                     or constant (above the `const` / `let` it covers every use of the binding). Stretching it would let that timer fire inside the window and \
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
    assert_eq!(
        scan_source(src, false)
            .iter()
            .map(|d| d.line)
            .collect::<Vec<_>>(),
        vec![8]
    );
    assert_eq!(
        scan_source(src, true)
            .iter()
            .map(|d| d.line)
            .collect::<Vec<_>>(),
        vec![2, 8]
    );
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
    let found = scan_source(src, true);
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
        via: None,
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
    let found = scan_source(src, false);
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
    let found = scan_source(src, false);
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

fn hits_in(src: &str, whole_file_is_test: bool) -> Vec<(usize, Option<String>)> {
    scan_source(src, whole_file_is_test)
        .into_iter()
        .map(|d| (d.line, d.via))
        .collect()
}

#[test]
fn a_fixed_budget_behind_a_const_or_static_is_caught() {
    let src = "\
const WAIT: Duration = Duration::from_secs(60);
pub(crate) static LIMIT: std::time::Duration = std::time::Duration::from_millis(500);
const SCALED: Duration = Duration::from_secs(5).mul_f64(MULTIPLIER);
const COMPUTED: Duration = budget();
async fn t() {
    tokio::time::timeout(WAIT, rx.recv()).await.unwrap();
    rx.recv_timeout(LIMIT);
    let (g, _) = cv.wait_timeout(g, WAIT).unwrap();
    let deadline = Instant::now() + WAIT;
    let deadline = Instant::now().checked_add(LIMIT);
    timeout(WAIT * 2, f);
    timeout(test_timeout(WAIT), f);
    timeout(WAIT.mul_f64(m), f);
    timeout(WAIT * m, f);
    timeout(SCALED, f);
    timeout(COMPUTED, f);
    timeout(UNKNOWN, f);
    timeout(self.wait, f);
    timeout(wait(), f);
}
";
    let via_wait = Some("const WAIT (line 1)".to_string());
    let via_limit = Some("static LIMIT (line 2)".to_string());
    assert_eq!(
        hits_in(src, true),
        vec![
            (6, via_wait.clone()),
            (7, via_limit.clone()),
            (8, via_wait.clone()),
            (9, via_wait.clone()),
            (10, via_limit),
            (11, via_wait),
        ]
    );
}

#[test]
fn a_const_in_a_test_module_is_caught_but_a_production_const_is_not() {
    let src = "\
const PRODUCTION_TIMEOUT: Duration = Duration::from_secs(10);
fn production() {
    let _ = timeout(PRODUCTION_TIMEOUT, f);
}

#[cfg(test)]
mod tests {
    const WAIT: Duration = Duration::from_secs(15);
    async fn t() {
        timeout(WAIT, f).await;
        timeout(PRODUCTION_TIMEOUT, f).await;
    }
}
";
    assert_eq!(
        hits_in(src, false),
        vec![(10, Some("const WAIT (line 8)".to_string()))]
    );
}

#[test]
fn a_fixed_budget_behind_a_local_binding_is_caught_in_its_scope_only() {
    let src = "\
async fn t() {
    timeout(d, f);
    let d = Duration::from_secs(5);
    let mut e: Duration = std::time::Duration::from_millis(250);
    timeout(d, f);
    rx.recv_timeout(e);
    {
        let d = test_timeout(d);
        timeout(d, f);
    }
    timeout(d, f);
    let scaled = Duration::from_secs(5).mul_f64(m);
    timeout(scaled, f);
}
async fn other() {
    timeout(d, f);
    let inner = {
        let w = Duration::from_secs(1);
        w
    };
    timeout(w, f);
}
";
    assert_eq!(
        hits_in(src, true),
        vec![
            (5, Some("let d (line 3)".to_string())),
            (6, Some("let e (line 4)".to_string())),
            (11, Some("let d (line 3)".to_string())),
        ]
    );
}

#[test]
fn a_marker_above_the_binding_exempts_every_use_and_needs_a_reason() {
    let src = "\
// fixed-deadline: allow — stays under the 800 ms SILENCE_LIMIT
const QUIET: Duration = Duration::from_millis(50);
// fixed-deadline: allow
const BARE: Duration = Duration::from_secs(5);
const WAIT: Duration = Duration::from_secs(5);
async fn t() {
    timeout(QUIET, f);
    timeout(QUIET, g);
    timeout(BARE, f);
    // fixed-deadline: allow — stays under the 100 ms idle debounce
    timeout(WAIT, f);
    // fixed-deadline: allow — stays under the 10 s handshake
    let window =
        Duration::from_millis(500);
    timeout(window, f);
}
";
    let found = scan_source(src, true);
    assert_eq!(
        found
            .iter()
            .map(|d| (d.line, d.marker_malformed))
            .collect::<Vec<_>>(),
        vec![(9, true)]
    );
}

#[test]
fn the_report_names_the_binding_behind_a_budget() {
    let found = BTreeMap::from([(
        "crates/a/tests/x.rs".to_string(),
        vec![Deadline {
            line: 6,
            text: "timeout(WAIT, f);".to_string(),
            marker_malformed: false,
            via: Some("const WAIT (line 1)".to_string()),
        }],
    )]);
    let report = report(&found, &BTreeMap::new());
    assert!(
        report
            .contains("  crates/a/tests/x.rs:6: timeout(WAIT, f);  [budget: const WAIT (line 1)]"),
        "{report}"
    );
    assert!(
        report.contains("fn wait() -> Duration"),
        "the fix text explains how to scale a constant budget:\n{report}"
    );
}

#[test]
fn a_binding_counts_like_the_same_expression_at_the_call_unless_reassigned() {
    let src = "\
async fn t() {
    let doubled = Duration::from_secs(5) * 2;
    timeout(doubled, f);
    timeout(Duration::from_secs(5) * 2, f);
    let mut grown = Duration::from_secs(1);
    grown = test_timeout(grown);
    timeout(grown, f);
    let mut stretched = Duration::from_secs(1);
    stretched *= m;
    timeout(stretched, f);
    let mut compared = Duration::from_secs(1);
    assert!(compared == other);
    timeout(compared, f);
}
";
    assert_eq!(
        hits_in(src, true),
        vec![
            (3, Some("let doubled (line 2)".to_string())),
            (4, None),
            (13, Some("let compared (line 11)".to_string())),
        ]
    );
}
