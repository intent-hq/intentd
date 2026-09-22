//! Running-turn status rule lint.
//!
//! "The agent is running a turn" means `AgentStatus::Pending | Active |
//! Processing`, and before intent-hq/intentd#2058 that rule had drifted into
//! four hand-written copies (two free `is_running_turn` fns in
//! intent-services, an inline match in the transfer export, and a literal
//! SQL `IN` list in intent-store). #2058 consolidated them onto
//! `AgentStatus::is_running_turn` in `crates/intent-core/src/model.rs`;
//! nothing else stops a fifth copy. This source-scanning test fails, naming
//! `file:line`, whenever an or-pattern anywhere under `crates/*/src/**/*.rs`
//! other than `model.rs` spells out exactly that variant set again.
//!
//! The heuristic is deliberately small:
//!
//! - Skipped: `crates/intent-core/src/model.rs` (the definition), any file
//!   named `tests.rs` or under a `tests/` directory, and any `#[cfg(test)]`
//!   item, attribute to end of item. An item introduced by `const` /
//!   `static` / `type` / `use` (any `const` other than `const fn`) ends at
//!   the first `;` at brace depth 0, so a `const` initializer with its own
//!   blocks is skipped whole; one introduced by `fn` / `mod` / `impl` /
//!   `struct` / `enum` / `union` / `trait` / `macro_rules` ends at the `}`
//!   closing its body, or at a `;` at depth 0 seen first (`mod tests;`,
//!   `struct X;`, a trait method signature). Anything else falls back to the
//!   first balanced `}` or `;`. The attribute is matched as the token
//!   sequence `# [ cfg ( test ) ]` with any whitespace between tokens, after
//!   comments are blanked — so `#[cfg( test )]` and `#[cfg(/* c */ test)]`
//!   are skipped, while `#[cfg(not(test))]` and `#[cfg(all(test, …))]` are
//!   scanned like ordinary code.
//! - String literals and comments are blanked first, so the triple quoted in
//!   a doc comment or a message never counts. The lexer keeps every string
//!   literal's content with its line so a rule over literal text (the SQL
//!   `IN` list) can share this pass. A "statement" is the text between `;` /
//!   `{` / `}` boundaries, except that a block expression used as an operand
//!   — its `}` followed by `.`, `?`, or `else` — is chained: the text before
//!   its `{` and the text after its `}` form one statement, while the block
//!   bodies stay their own statements.
//! - An **or-group** is a maximal run of path tokens (`A::B::C`) joined by
//!   single `|` inside one statement — `matches!(s, A | B | C)`, an
//!   `A | B | C =>` match arm, any order, across lines. `||` ends a group.
//! - A group is a **hit** when the set of variant names it spells as
//!   `AgentStatus::<V>` (any path prefix, e.g. `intent_core::AgentStatus::<V>`)
//!   or `Self::<V>` is exactly `{Pending, Active, Processing}`. Supersets
//!   (`… | Waiting`) and subsets (`Active | Processing`) are distinct rules
//!   and are not hits.
//! - Opt-out: `// running-turn: allow — <reason>` on the line immediately
//!   above the group's first line, or above the statement's first line (the
//!   `matches!(` line for a multi-line `matches!`). The marker counts only
//!   as a standalone `//` line comment (nothing but whitespace before it,
//!   not inside a `/* … */` block comment or a string literal, not trailing
//!   code), the token must be exactly `running-turn: allow` (a longer word
//!   such as `allowance` is malformed), and it must be followed by
//!   whitespace, an em dash or hyphen, and a nonempty reason. A malformed
//!   marker never suppresses the hit; the report says so.
//!
//! Limits: bare imported variants (`use AgentStatus::*; Pending | Active |
//! Processing`) are not recognized, a `Self::` triple on some other enum
//! with the same variant names is a false positive (opt out with a reason),
//! and a test module file that is not named `tests.rs` and not under a
//! `tests/` directory is scanned like production code unless its item is
//! `#[cfg(test)]`.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Component, Path, PathBuf};

const RUNNING_TURN_VARIANTS: &[&str] = &["Pending", "Active", "Processing"];
const VARIANT_OWNERS: &[&str] = &["AgentStatus", "Self"];
const OPT_OUT_MARKER: &str = "// running-turn: allow";
const CFG_TEST_TOKENS: &[&str] = &["#", "[", "cfg", "(", "test", ")", "]"];
const EXEMPT_FILE: &[&str] = &["crates", "intent-core", "src", "model.rs"];
const EXCERPT_CHARS: usize = 120;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Hit {
    line: usize,
    excerpt: String,
    /// A line checked for the opt-out carried something that starts like the
    /// marker but is malformed (longer token, or no reason).
    marker_malformed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Marker {
    Absent,
    WithReason,
    Malformed,
}

/// A string literal found by the lexer, with its body as written (escape
/// sequences are not evaluated; raw strings are taken verbatim) and the
/// 1-based line it opens on.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Literal {
    /// Char offset of the opening quote in the source.
    offset: usize,
    line: usize,
    text: String,
}

/// A real `//` line comment found by the lexer (never one nested inside a
/// block comment or a string literal).
struct LineComment {
    line: usize,
    /// Only whitespace precedes the `//` on its line.
    standalone: bool,
    text: String,
}

/// Source text with comments/literals blanked, plus what the lexer passed
/// over on the way.
struct Lexed {
    blanked: String,
    literals: Vec<Literal>,
    line_comments: Vec<LineComment>,
}

/// Marker state of one line comment's text: `Absent` unless it starts with
/// the marker prefix; `WithReason` only when the token is exactly the marker
/// (not a longer word such as `allowance`) followed by whitespace, a dash,
/// and a nonempty reason; anything else that starts like the marker is
/// `Malformed`.
fn classify_marker(comment: &str) -> Marker {
    let Some(rest) = comment.strip_prefix(OPT_OUT_MARKER) else {
        return Marker::Absent;
    };
    if rest
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Marker::Malformed;
    }
    let after_space = rest.trim_start();
    if after_space.len() == rest.len() && !rest.is_empty() {
        return Marker::Malformed;
    }
    let Some(reason) = after_space
        .strip_prefix('—')
        .or_else(|| after_space.strip_prefix('-'))
    else {
        return Marker::Malformed;
    };
    if reason.trim().is_empty() {
        Marker::Malformed
    } else {
        Marker::WithReason
    }
}

/// Opt-out marker state per line; index 0 is a placeholder so the vector is
/// addressed by 1-based line number. Only a standalone `//` line comment can
/// carry the marker.
fn markers_by_line(src: &str, line_comments: &[LineComment]) -> Vec<Marker> {
    let mut out = vec![Marker::Absent; src.lines().count() + 1];
    for comment in line_comments.iter().filter(|c| c.standalone) {
        if let Some(slot) = out.get_mut(comment.line) {
            *slot = classify_marker(&comment.text);
        }
    }
    out
}

fn push_blank(out: &mut String, c: char) {
    out.push(if c == '\n' { '\n' } else { ' ' });
}

/// `Some(hashes)` when a raw string literal (`r"`, `r#"`, `br"`, `cr#"`, …)
/// starts at `i`; `None` otherwise. Cooked `b"…"` / `c"…"` strings need no
/// special case: their prefix letter is left as an inert identifier and the
/// `"` branch consumes the body.
fn raw_string_hashes(chars: &[char], i: usize) -> Option<usize> {
    let preceded_by_ident = i > 0 && (chars[i - 1].is_ascii_alphanumeric() || chars[i - 1] == '_');
    if preceded_by_ident {
        return None;
    }
    let mut j = i;
    if matches!(chars.get(j), Some('b' | 'c')) {
        j += 1;
    }
    if chars.get(j) != Some(&'r') {
        return None;
    }
    j += 1;
    let mut hashes = 0;
    while chars.get(j) == Some(&'#') {
        hashes += 1;
        j += 1;
    }
    (chars.get(j) == Some(&'"')).then_some(hashes)
}

/// Lexes `src` once: every comment, string literal, and char literal is
/// blanked (newlines preserved) in `blanked` so neither their contents nor
/// their delimiters take part in statement splitting or token matching;
/// every string literal's body is collected with its line; and every `//`
/// line comment is reported (only those may carry the opt-out marker).
fn lex(src: &str) -> Lexed {
    let chars: Vec<char> = src.chars().collect();
    let mut out = String::with_capacity(src.len());
    let mut literals = Vec::new();
    let mut line_comments = Vec::new();
    let mut line = 1usize;
    let mut line_start = 0usize;
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        let next = chars.get(i + 1).copied();
        if c == '\n' {
            out.push('\n');
            line += 1;
            line_start = i + 1;
            i += 1;
        } else if c == '/' && next == Some('/') {
            let start = i;
            while i < chars.len() && chars[i] != '\n' {
                out.push(' ');
                i += 1;
            }
            line_comments.push(LineComment {
                line,
                standalone: chars[line_start..start].iter().all(|c| c.is_whitespace()),
                text: chars[start..i].iter().collect(),
            });
        } else if c == '/' && next == Some('*') {
            let mut depth = 0usize;
            while i < chars.len() {
                if chars[i] == '/' && chars.get(i + 1) == Some(&'*') {
                    depth += 1;
                    out.push_str("  ");
                    i += 2;
                } else if chars[i] == '*' && chars.get(i + 1) == Some(&'/') {
                    depth -= 1;
                    out.push_str("  ");
                    i += 2;
                    if depth == 0 {
                        break;
                    }
                } else {
                    if chars[i] == '\n' {
                        line += 1;
                        line_start = i + 1;
                    }
                    push_blank(&mut out, chars[i]);
                    i += 1;
                }
            }
        } else if let Some(hashes) = raw_string_hashes(&chars, i) {
            while chars[i] != '"' {
                out.push(' ');
                i += 1;
            }
            let offset = i;
            let open_line = line;
            out.push(' ');
            i += 1;
            let body_start = i;
            let mut body_end = i;
            while i < chars.len() {
                let closing =
                    chars[i] == '"' && (1..=hashes).all(|k| chars.get(i + k) == Some(&'#'));
                if chars[i] == '\n' {
                    line += 1;
                    line_start = i + 1;
                }
                push_blank(&mut out, chars[i]);
                i += 1;
                if closing {
                    body_end = i - 1;
                    out.push_str(&" ".repeat(hashes));
                    i += hashes;
                    break;
                }
            }
            literals.push(Literal {
                offset,
                line: open_line,
                text: chars[body_start..body_end].iter().collect(),
            });
        } else if c == '"' {
            let offset = i;
            let open_line = line;
            out.push(' ');
            i += 1;
            let body_start = i;
            let mut body_end = i;
            while i < chars.len() {
                let d = chars[i];
                if d == '\n' {
                    line += 1;
                    line_start = i + 1;
                }
                push_blank(&mut out, d);
                i += 1;
                if d == '\\' {
                    if let Some(&escaped) = chars.get(i) {
                        if escaped == '\n' {
                            line += 1;
                            line_start = i + 1;
                        }
                        push_blank(&mut out, escaped);
                        i += 1;
                    }
                } else if d == '"' {
                    body_end = i - 1;
                    break;
                }
            }
            literals.push(Literal {
                offset,
                line: open_line,
                text: chars[body_start..body_end].iter().collect(),
            });
        } else if c == '\'' {
            // `'\…'` and `'x'` are char literals; anything else is a lifetime
            // or loop label, which carries no path token.
            if next == Some('\\') {
                let start = i;
                i += 2;
                if chars.get(i) == Some(&'u') {
                    while i < chars.len() && chars[i] != '}' {
                        i += 1;
                    }
                }
                i += 1;
                if chars.get(i) == Some(&'\'') {
                    i += 1;
                }
                i = i.min(chars.len());
                for &d in &chars[start..i] {
                    push_blank(&mut out, d);
                }
            } else if chars.get(i + 2) == Some(&'\'') {
                out.push_str("   ");
                i += 3;
            } else {
                out.push(' ');
                i += 1;
            }
        } else {
            out.push(c);
            i += 1;
        }
    }
    Lexed {
        blanked: out,
        literals,
        line_comments,
    }
}

/// Whether the `#[cfg(test)]` token sequence starts at `i`, ignoring any
/// whitespace between tokens (a blanked `/* comment */` inside the attribute
/// leaves spaces behind, and `# [cfg(test)]` is legal Rust).
fn starts_with_cfg_test(chars: &[char], i: usize) -> bool {
    if chars.get(i) != Some(&'#') {
        return false;
    }
    let mut j = i;
    for token in CFG_TEST_TOKENS {
        while chars.get(j).is_some_and(|c| c.is_whitespace()) {
            j += 1;
        }
        for want in token.chars() {
            if chars.get(j) != Some(&want) {
                return false;
            }
            j += 1;
        }
    }
    true
}

/// Items whose body is a brace block; they end at the `}` closing it (or at a
/// `;` at depth 0 seen first).
const BODY_ITEM_KEYWORDS: &[&str] = &[
    "fn",
    "mod",
    "impl",
    "struct",
    "enum",
    "union",
    "trait",
    "macro_rules",
];
/// Items that end at the first `;` at depth 0, whatever blocks their
/// initializer contains.
const SEMICOLON_ITEM_KEYWORDS: &[&str] = &["const", "static", "type", "use"];
/// Qualifiers that turn `const` into `const fn` / `const unsafe fn` / ….
const FN_QUALIFIERS: &[&str] = &["fn", "unsafe", "extern", "async"];

fn skip_whitespace(chars: &[char], mut j: usize) -> usize {
    while chars.get(j).is_some_and(|c| c.is_whitespace()) {
        j += 1;
    }
    j
}

/// Index just past the delimiter group (`(…)` or `[…]`) opening at `j`.
fn skip_group(chars: &[char], mut j: usize, open: char, close: char) -> usize {
    let mut depth = 0usize;
    while let Some(&c) = chars.get(j) {
        j += 1;
        if c == open {
            depth += 1;
        } else if c == close {
            depth -= 1;
            if depth == 0 {
                break;
            }
        }
    }
    j
}

/// `(word, index past it)` for the identifier starting at `j`, if any.
fn word_at(chars: &[char], j: usize) -> Option<(String, usize)> {
    let mut k = j;
    while chars
        .get(k)
        .is_some_and(|c| c.is_ascii_alphanumeric() || *c == '_')
    {
        k += 1;
    }
    (k > j).then(|| (chars[j..k].iter().collect(), k))
}

/// Whether the item introduced after the attribute(s) starting at `j` ends at
/// a `;` at brace depth 0 rather than at the `}` closing its body. Looks past
/// further attributes and qualifiers (`pub(crate)`, `unsafe`, …) to the item
/// keyword; an unrecognized item is treated as body-terminated.
fn cfg_test_item_ends_at_semicolon(chars: &[char], mut j: usize) -> bool {
    loop {
        j = skip_whitespace(chars, j);
        match chars.get(j) {
            Some('#') => {
                j += 1;
                if chars.get(j) == Some(&'!') {
                    j += 1;
                }
                j = skip_group(chars, j, '[', ']');
            }
            Some('(') => j = skip_group(chars, j, '(', ')'),
            Some(c) if c.is_ascii_alphabetic() || *c == '_' => {
                let (word, next) = word_at(chars, j).expect("identifier start");
                j = next;
                if BODY_ITEM_KEYWORDS.contains(&word.as_str()) {
                    return false;
                }
                if word == "const" {
                    let after = skip_whitespace(chars, j);
                    return !word_at(chars, after)
                        .is_some_and(|(w, _)| FN_QUALIFIERS.contains(&w.as_str()));
                }
                if SEMICOLON_ITEM_KEYWORDS.contains(&word.as_str()) {
                    return true;
                }
            }
            _ => return false,
        }
    }
}

/// Blanks every `#[cfg(test)]` attribute together with the whole item that
/// follows it (see the module doc for where each kind of item ends). Runs on
/// already-blanked text, so the attribute cannot hide inside a string or
/// comment.
fn blank_cfg_test_items(text: &str) -> String {
    let chars: Vec<char> = text.chars().collect();
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < chars.len() {
        if !starts_with_cfg_test(&chars, i) {
            out.push(chars[i]);
            i += 1;
            continue;
        }
        let ends_at_semicolon = cfg_test_item_ends_at_semicolon(&chars, i);
        let mut depth = 0usize;
        while i < chars.len() {
            let c = chars[i];
            push_blank(&mut out, c);
            i += 1;
            match c {
                '{' => depth += 1,
                '}' => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 && !ends_at_semicolon {
                        break;
                    }
                }
                ';' if depth == 0 => break,
                _ => {}
            }
        }
    }
    out
}

struct Statement {
    line: usize,
    text: String,
}

/// Whether the `}` just before `j` is followed by `.`, `?`, or `else`, i.e.
/// the block is an operand and the enclosing statement continues after it.
fn block_is_operand(chars: &[char], j: usize) -> bool {
    let j = skip_whitespace(chars, j);
    match chars.get(j) {
        Some('.' | '?') => true,
        Some(_) => word_at(chars, j).is_some_and(|(w, _)| w == "else"),
        None => false,
    }
}

/// Splits blanked source at `;` / `{` / `}`; each statement records the line
/// of its first non-whitespace character. The text before a `{` is held back
/// until the matching `}`: when that `}` is followed by `.`, `?`, or `else`
/// the held text resumes as the current statement (so an operand block chains
/// with what surrounds it), otherwise it is emitted as it stood. A chained
/// statement keeps one newline per source line the block body spanned, so
/// `tokenize` still reports source lines for the text after the block.
/// Statements come back in source order.
fn split_statements(text: &str) -> Vec<Statement> {
    let chars: Vec<char> = text.chars().collect();
    let mut out = Vec::new();
    let mut line = 1usize;
    let mut current = String::new();
    let mut start_line: Option<usize> = None;
    let mut held: Vec<(usize, Option<Statement>)> = Vec::new();
    let take = |current: &mut String, start_line: &mut Option<usize>| {
        let text = std::mem::take(current);
        start_line.take().map(|line| Statement { line, text })
    };
    for (i, &c) in chars.iter().enumerate() {
        match c {
            ';' => out.extend(take(&mut current, &mut start_line)),
            '{' => held.push((line, take(&mut current, &mut start_line))),
            '}' => {
                out.extend(take(&mut current, &mut start_line));
                if let Some((open_line, Some(prefix))) = held.pop() {
                    if block_is_operand(&chars, i + 1) {
                        current = prefix.text;
                        current.push(' ');
                        current.extend(std::iter::repeat_n('\n', line - open_line));
                        start_line = Some(prefix.line);
                    } else {
                        out.push(prefix);
                    }
                }
            }
            '\n' => {
                line += 1;
                current.push(c);
            }
            _ => {
                if !c.is_whitespace() && start_line.is_none() {
                    start_line = Some(line);
                }
                current.push(c);
            }
        }
    }
    out.extend(take(&mut current, &mut start_line));
    out.extend(held.into_iter().filter_map(|(_, s)| s));
    out.sort_by_key(|s| s.line);
    out
}

/// The tokens an or-group is made of. Everything that is neither a path nor
/// a `|` is `Other` and ends any group in progress.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    /// `A::B::C` (whitespace allowed around `::`), as its segments.
    Path(Vec<String>),
    /// A single `|`; `||` is `Other`.
    Pipe,
    Other,
}

/// Tokens of one statement's text with the 1-based line each starts on
/// (`first_line` is the line of the statement's first non-whitespace
/// character, as `split_statements` records it).
fn tokenize(text: &str, first_line: usize) -> Vec<(Token, usize)> {
    let chars: Vec<char> = text.trim_start().chars().collect();
    let mut out = Vec::new();
    let mut line = first_line;
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        if c == '\n' {
            line += 1;
            i += 1;
        } else if c.is_whitespace() {
            i += 1;
        } else if c.is_ascii_alphabetic() || c == '_' {
            let start_line = line;
            let mut segments = Vec::new();
            loop {
                let (word, next) = word_at(&chars, i).expect("identifier start");
                segments.push(word);
                i = next;
                let mut j = i;
                let mut newlines = 0usize;
                while chars.get(j).is_some_and(|c| c.is_whitespace()) {
                    newlines += usize::from(chars[j] == '\n');
                    j += 1;
                }
                if chars.get(j) == Some(&':') && chars.get(j + 1) == Some(&':') {
                    j += 2;
                    let mut k = j;
                    while chars.get(k).is_some_and(|c| c.is_whitespace()) {
                        newlines += usize::from(chars[k] == '\n');
                        k += 1;
                    }
                    if chars
                        .get(k)
                        .is_some_and(|c| c.is_ascii_alphabetic() || *c == '_')
                    {
                        line += newlines;
                        i = k;
                        continue;
                    }
                }
                break;
            }
            out.push((Token::Path(segments), start_line));
        } else if c.is_ascii_digit() {
            while chars
                .get(i)
                .is_some_and(|c| c.is_ascii_alphanumeric() || *c == '_' || *c == '.')
            {
                i += 1;
            }
            out.push((Token::Other, line));
        } else if c == '|' {
            if chars.get(i + 1) == Some(&'|') {
                out.push((Token::Other, line));
                i += 2;
            } else {
                out.push((Token::Pipe, line));
                i += 1;
            }
        } else {
            out.push((Token::Other, line));
            i += 1;
        }
    }
    out
}

/// A maximal run of path tokens joined by single `|` inside one statement.
#[derive(Debug, Clone, PartialEq, Eq)]
struct OrGroup {
    line: usize,
    paths: Vec<Vec<String>>,
}

impl OrGroup {
    fn text(&self) -> String {
        self.paths
            .iter()
            .map(|p| p.join("::"))
            .collect::<Vec<_>>()
            .join(" | ")
    }

    /// Variant names the group spells as `<owner>::<Variant>` with an owner in
    /// `VARIANT_OWNERS`, whatever precedes the owner (`intent_core::AgentStatus::Active`).
    fn variants(&self) -> BTreeSet<&str> {
        self.paths
            .iter()
            .filter(|p| p.len() >= 2 && VARIANT_OWNERS.contains(&p[p.len() - 2].as_str()))
            .map(|p| p[p.len() - 1].as_str())
            .collect()
    }
}

fn or_groups(tokens: &[(Token, usize)]) -> Vec<OrGroup> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < tokens.len() {
        let (Token::Path(first), line) = &tokens[i] else {
            i += 1;
            continue;
        };
        let mut paths = vec![first.clone()];
        i += 1;
        while tokens.get(i).is_some_and(|(t, _)| *t == Token::Pipe) {
            match tokens.get(i + 1) {
                Some((Token::Path(p), _)) => {
                    paths.push(p.clone());
                    i += 2;
                }
                _ => break,
            }
        }
        if paths.len() > 1 {
            out.push(OrGroup { line: *line, paths });
        }
    }
    out
}

/// Whether the group spells exactly the running-turn variant set.
fn is_running_turn_triple(group: &OrGroup) -> bool {
    let want: BTreeSet<&str> = RUNNING_TURN_VARIANTS.iter().copied().collect();
    group.variants() == want
}

fn excerpt(text: &str) -> String {
    let collapsed = text.split_whitespace().collect::<Vec<_>>().join(" ");
    let mut out: String = collapsed.chars().take(EXCERPT_CHARS).collect();
    if out.len() < collapsed.len() {
        out.push('…');
    }
    out
}

/// Scans one Rust source file's text and returns every or-group spelling the
/// running-turn triple that is not suppressed by a reasoned opt-out marker on
/// the line above the group or above its statement.
fn scan_source(src: &str) -> Vec<Hit> {
    let lexed = lex(src);
    let markers = markers_by_line(src, &lexed.line_comments);
    let blanked = blank_cfg_test_items(&lexed.blanked);
    let marker_at = |line: usize| markers.get(line - 1).copied().unwrap_or(Marker::Absent);
    let mut hits = Vec::new();
    for statement in split_statements(&blanked) {
        for group in or_groups(&tokenize(&statement.text, statement.line)) {
            if !is_running_turn_triple(&group) {
                continue;
            }
            let states = [marker_at(group.line), marker_at(statement.line)];
            if states.contains(&Marker::WithReason) {
                continue;
            }
            hits.push(Hit {
                line: group.line,
                excerpt: excerpt(&group.text()),
                marker_malformed: states.contains(&Marker::Malformed),
            });
        }
    }
    hits
}

/// Every `*.rs` under `dir`, skipping `tests/` directories and `tests.rs`.
fn collect_rust_sources(dir: &Path, out: &mut Vec<PathBuf>) {
    let entries = fs::read_dir(dir).unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display()));
    for entry in entries {
        let entry = entry.unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display()));
        let path = entry.path();
        let name = entry.file_name();
        if path.is_dir() {
            if name != "tests" {
                collect_rust_sources(&path, out);
            }
        } else if path.extension().is_some_and(|ext| ext == "rs") && name != "tests.rs" {
            out.push(path);
        }
    }
}

fn is_exempt(rel: &Path) -> bool {
    let parts: Vec<_> = rel.components().map(Component::as_os_str).collect();
    parts.len() == EXEMPT_FILE.len()
        && parts
            .iter()
            .zip(EXEMPT_FILE)
            .all(|(have, want)| *have == *want)
}

fn display_rel(rel: &Path) -> String {
    rel.components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join("/")
}

#[test]
fn running_turn_rule_is_only_spelled_in_agent_status() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join("..");
    let exempt: PathBuf = EXEMPT_FILE.iter().collect();
    let exempt_src = fs::read_to_string(root.join(&exempt)).unwrap_or_else(|e| {
        panic!(
            "{} moved ({e}); update EXEMPT_FILE so the exemption keeps pointing at AgentStatus",
            display_rel(&exempt)
        )
    });
    assert!(
        !scan_source(&exempt_src).is_empty(),
        "{} no longer spells the running-turn triple as an or-pattern; the exemption and \
         the rule's variant set need revisiting",
        display_rel(&exempt)
    );

    let mut files = Vec::new();
    let crates_dir = root.join("crates");
    let crates = fs::read_dir(&crates_dir)
        .unwrap_or_else(|e| panic!("read_dir {}: {e}", crates_dir.display()));
    for entry in crates {
        let src = entry.expect("crate dir entry").path().join("src");
        if src.is_dir() {
            collect_rust_sources(&src, &mut files);
        }
    }
    files.sort();
    assert!(
        !files.is_empty(),
        "no Rust sources found under {}",
        crates_dir.display()
    );

    let mut report = Vec::new();
    for file in &files {
        let rel = file
            .strip_prefix(&root)
            .expect("source path under the workspace root");
        if is_exempt(rel) {
            continue;
        }
        let src =
            fs::read_to_string(file).unwrap_or_else(|e| panic!("read {}: {e}", file.display()));
        for hit in scan_source(&src) {
            let note = if hit.marker_malformed {
                "  (opt-out marker is malformed: expected `// running-turn: allow — <reason>`)"
            } else {
                ""
            };
            report.push(format!(
                "{}:{}: {}{note}",
                display_rel(rel),
                hit.line,
                hit.excerpt
            ));
        }
    }

    assert!(
        report.is_empty(),
        "the running-turn status rule is spelled out again outside AgentStatus:\n\n{}\n\n\
         Call `AgentStatus::is_running_turn()` (crates/intent-core/src/model.rs) instead of \
         matching `Pending | Active | Processing` by hand; that method is the single source \
         for \"the agent is running a turn\" (intent-hq/intentd#2058). A site that genuinely \
         needs the same variant set for a different rule may opt out with \
         `// running-turn: allow — <reason>` on the line immediately above the pattern or \
         its statement; the reason is required.",
        report.join("\n")
    );
}

// ---- scanner fixtures -------------------------------------------------------

/// 1-based line of the first line containing `needle`.
fn line_of(src: &str, needle: &str) -> usize {
    src.lines()
        .position(|l| l.contains(needle))
        .map_or_else(|| panic!("fixture lacks {needle:?}"), |i| i + 1)
}

fn hit_lines(src: &str) -> Vec<usize> {
    scan_source(src).into_iter().map(|h| h.line).collect()
}

/// The retire-guard copy in `crates/intent-services/src/agent_ops.rs` as it
/// stood before intentd commit e6703788 (#2058).
const AGENT_OPS_IS_RUNNING_TURN_PRE_2058: &str = r#"
/// "Running a turn" statuses for the retire guard (§5.5, confirmed
/// decision): a descendant in `pending`/`active`/`Processing` blocks
/// `ws.agent.retire`; idle/waiting/settled children are cascade-retired.
fn is_running_turn(status: AgentStatus) -> bool {
    matches!(
        status,
        AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing
    )
}
"#;

/// The lock-scope copy in `crates/intent-services/src/agent_locks.rs` before
/// #2058 (the second free fn).
const AGENT_LOCKS_IS_RUNNING_TURN_PRE_2058: &str = r"
/// Whether `status` means the session is mid-turn (parity with the retire
/// guard): `pending` / `active` / legacy `Processing`.
fn is_running_turn(status: AgentStatus) -> bool {
    matches!(
        status,
        AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing
    )
}
";

/// The transfer export warning in `crates/intent-services/src/transfer.rs`
/// before #2058: inline, inside a closure block, in a different order.
const TRANSFER_RUNNING_PRE_2058: &str = r"
        let running = sessions
            .iter()
            .filter(|s| {
                s.is_active
                    || matches!(
                        s.status,
                        AgentStatus::Active | AgentStatus::Pending | AgentStatus::Processing
                    )
            })
            .count();
";

#[test]
fn pre_2058_copies_are_flagged() {
    let src = AGENT_OPS_IS_RUNNING_TURN_PRE_2058;
    let hits = scan_source(src);
    assert_eq!(
        hits,
        vec![Hit {
            line: line_of(src, "AgentStatus::Pending | AgentStatus::Active"),
            excerpt: "AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing".into(),
            marker_malformed: false,
        }]
    );

    let src = AGENT_LOCKS_IS_RUNNING_TURN_PRE_2058;
    assert_eq!(
        hit_lines(src),
        vec![line_of(src, "AgentStatus::Pending | AgentStatus::Active")]
    );

    let src = TRANSFER_RUNNING_PRE_2058;
    assert_eq!(
        hit_lines(src),
        vec![line_of(src, "AgentStatus::Active | AgentStatus::Pending")]
    );
}

#[test]
fn text_after_an_operand_block_keeps_its_source_line() {
    let src = r"
fn f(s: AgentStatus) -> bool {
    matches!(
        {
            s
        }.clone(),
        AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing
    )
}
fn g(s: AgentStatus) -> bool {
    let s = if s.is_active() {
        s
    } else {
        s.parent()
    }.status();
    matches!(s, AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing)
}
";
    assert_eq!(
        hit_lines(src),
        vec![
            line_of(src, "        AgentStatus::Pending | AgentStatus::Active"),
            line_of(src, "    matches!(s, AgentStatus::Pending"),
        ]
    );

    // A reasoned marker above the triple suppresses at that (correct) line.
    let src = r"
fn f(s: AgentStatus) -> bool {
    matches!(
        {
            s
        }.clone(),
        // running-turn: allow — fixture
        AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing
    )
}
";
    assert!(hit_lines(src).is_empty(), "{:?}", scan_source(src));
}

#[test]
fn match_arms_and_self_paths_are_recognized_across_lines_in_any_order() {
    let src = r"
impl Foo {
    fn running(self) -> bool {
        match self.status {
            AgentStatus::Processing
            | AgentStatus::Pending
            | AgentStatus::Active => true,
            _ => false,
        }
    }
    fn running_self(self) -> bool {
        matches!(self, Self::Pending | Self::Active | Self::Processing)
    }
    fn running_qualified(s: intent_core::AgentStatus) -> bool {
        matches!(s, intent_core::AgentStatus::Pending | intent_core::AgentStatus::Active | intent_core::AgentStatus::Processing)
    }
    fn nested(s: Option<AgentStatus>) -> bool {
        matches!(s, Some(AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing))
    }
}
";
    assert_eq!(
        hit_lines(src),
        vec![
            line_of(src, "            AgentStatus::Processing"),
            line_of(src, "Self::Pending | Self::Active"),
            line_of(src, "intent_core::AgentStatus::Pending |"),
            line_of(src, "Some(AgentStatus::Pending"),
        ]
    );
}

#[test]
fn supersets_subsets_and_other_enums_are_not_hits() {
    let src = r"
fn f(s: AgentStatus, t: Other) -> bool {
    let a = matches!(s, AgentStatus::Pending | AgentStatus::Active);
    let b = matches!(s, AgentStatus::Active | AgentStatus::Processing);
    let c = matches!(
        s,
        AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing | AgentStatus::Waiting
    );
    let d = matches!(t, Other::Pending | Other::Active | Other::Processing);
    let e = matches!(s, AgentStatus::Pending) || matches!(s, AgentStatus::Active) || matches!(s, AgentStatus::Processing);
    let f = s == AgentStatus::Pending || s == AgentStatus::Active || s == AgentStatus::Processing;
    let g = s.is_running_turn();
    a || b || c || d || e || f || g
}

// lib.rs / transfer_export.rs: the live-agent set.
fn is_live(s: AgentStatus) -> bool {
    matches!(
        s,
        AgentStatus::Active | AgentStatus::Processing | AgentStatus::Waiting
    )
}

// bindings/workspace.rs: the archive gate.
fn blocks_archive(s: AgentStatus) -> bool {
    matches!(
        s,
        AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing | AgentStatus::Waiting
    )
}

// agent_ops.rs: the wake-reason mapping.
fn agent_status_wire(status: AgentStatus) -> Option<&'static str> {
    match status {
        AgentStatus::Pending | AgentStatus::Waiting => Some(WAITING),
        AgentStatus::Active | AgentStatus::Processing => Some(RESPONDING),
        AgentStatus::RuntimeIdle | AgentStatus::Idle => Some(IDLE),
        AgentStatus::Completed => Some(COMPLETED),
        AgentStatus::Error => Some(FAILED),
        AgentStatus::Deleted => None,
    }
}

fn is_active(status: AgentStatus) -> bool {
    matches!(status, AgentStatus::Active)
}
";
    assert!(hit_lines(src).is_empty(), "{:?}", scan_source(src));
}

#[test]
fn comments_strings_and_test_code_are_skipped() {
    let src = r#"
//! Running means `AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing`.

/// See `AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing`.
fn doc(s: AgentStatus) -> &'static str {
    /* AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing */
    let msg = "AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing";
    let raw = r"AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing";
    if s.is_running_turn() { msg } else { raw }
}

#[cfg(test)]
fn in_test_fn(s: AgentStatus) -> bool {
    matches!(s, AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing)
}

#[cfg(test)]
mod tests {
    fn helper(s: AgentStatus) -> bool {
        matches!(s, AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing)
    }
}

#[cfg(test)]
const RUNNING: fn(AgentStatus) -> bool =
    |s| matches!(s, AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing);

#[cfg(not(test))]
fn scanned(s: AgentStatus) -> bool {
    matches!(s, AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing)
}
"#;
    assert_eq!(hit_lines(src), vec![line_of(src, "fn scanned") + 1]);
}

#[test]
fn reasoned_marker_suppresses_above_group_or_statement() {
    let src = r"
fn f(s: AgentStatus) -> bool {
    // running-turn: allow — fixture for the lint itself
    let a = matches!(s, AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing);
    // running-turn: allow - the statement line is the `matches!(` line
    let b = matches!(
        s,
        AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing
    );
    let c = matches!(
        s,
        // running-turn: allow — directly above the pattern
        AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing
    );
    let d = match s {
        // running-turn: allow — above the arm
        AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing => true,
        _ => false,
    };
    a || b || c || d
}
";
    assert!(hit_lines(src).is_empty(), "{:?}", scan_source(src));
}

#[test]
fn malformed_or_misplaced_markers_do_not_suppress() {
    let src = r"
fn f(s: AgentStatus) -> bool {
    // running-turn: allow
    let a = matches!(s, AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing);
    // running-turn: allowance — longer token
    let b = matches!(s, AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing);
    // running-turn: allow —
    let c = matches!(s, AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing);
    let d = matches!(s, AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing); // running-turn: allow — trailing
    /* running-turn: allow — block comment */
    let e = matches!(s, AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing);
    // running-turn: allow — two lines above

    let f = matches!(s, AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing);
    a || b || c || d || e || f
}
";
    let hits = scan_source(src);
    let expect: Vec<(usize, bool)> = ["let a", "let b", "let c", "let d", "let e", "let f"]
        .iter()
        .zip([true, true, true, false, false, false])
        .map(|(needle, malformed)| (line_of(src, needle), malformed))
        .collect();
    assert_eq!(
        hits.iter()
            .map(|h| (h.line, h.marker_malformed))
            .collect::<Vec<_>>(),
        expect
    );
}

#[test]
fn lexer_keeps_string_literal_bodies_with_their_lines() {
    let src = "let a = \"x'y\\\"z\";\nlet b = r#\"in ('a', 'b')\"#;\nlet c = 'q';\n// tail";
    let lexed = lex(src);
    assert_eq!(
        lexed.literals,
        vec![
            Literal {
                offset: 8,
                line: 1,
                text: "x'y\\\"z".into()
            },
            Literal {
                offset: 28,
                line: 2,
                text: "in ('a', 'b')".into()
            },
        ]
    );
    assert_eq!(lexed.line_comments.len(), 1);
    assert!(lexed.line_comments[0].standalone);
    assert_eq!(lexed.blanked.lines().count(), src.lines().count());
    assert!(!lexed.blanked.contains('"') && !lexed.blanked.contains("tail"));
}
