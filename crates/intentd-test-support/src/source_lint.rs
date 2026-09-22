//! Shared scaffolding for the source-scanning lints (`crates/*/tests/*_lint.rs`).
//!
//! Every source lint needs the same pre-processing before its own rule can
//! run over a Rust file: blank the comments and string literals so a quoted
//! example never counts, blank every `#[cfg(test)]` item so test fixtures are
//! skipped, cut the remaining text into statements, look up the opt-out
//! marker on the line above a hit, and walk the workspace's crates. Before
//! this module each lint carried its own copy of that scaffolding, and fixes
//! landed in one copy at a time. Two intent-hq/intentd#2073 review findings
//! are encoded here so no copy can lack them again:
//!
//! - **`102c347a` — operand-block line accounting.** [`split_statements`]
//!   chains a block used as an operand (its `}` followed by `.`, `?`, or
//!   `else`) with the text around it. The chained statement keeps one
//!   newline per source line the block body spanned, so the lines a lint
//!   reports for the text after the block — and the line it checks for the
//!   opt-out marker — stay aligned with the source.
//! - **`f2c685af` — `cfg(test)` bracket nesting.** [`cfg_test_item_ranges`]
//!   counts `(…)` and `[…]` as well as `{…}` when it looks for the end of a
//!   `;`-terminated item, so a `;` inside an array type (`[u8; 1]`) or a
//!   parameter list never ends the item early and leaves its body scanned as
//!   production code.
//!
//! Where a `#[cfg(test)]` item ends: one introduced by `const` / `static` /
//! `type` / `use` (any `const` other than `const fn`) or a `mod name;`
//! declaration ends at the first `;` at depth 0, so an initializer with its
//! own blocks is skipped whole; one introduced by `fn` / `mod` / `impl` /
//! `struct` / `enum` / `union` / `trait` / `macro_rules` ends at the `}`
//! closing its body, or at a `;` at depth 0 seen first (`struct X;`, a trait
//! method signature). Anything else falls back to the first balanced `}` or
//! `;`. The attribute is matched as the token sequence `# [ cfg ( test ) ]`
//! with any whitespace between tokens, on already-blanked text — so
//! `#[cfg( test )]` and `#[cfg(/* c */ test)]` are recognized, while
//! `#[cfg(not(test))]` and `#[cfg(all(test, …))]` are ordinary code.
//!
//! The opt-out marker is `// <tag>: allow — <reason>`: it counts only as a
//! standalone `//` line comment (nothing but whitespace before it, not inside
//! a `/* … */` block comment or a string literal, not trailing code), the
//! token must be exactly `<tag>: allow` (a longer word such as `allowance` is
//! malformed), and it must be followed by whitespace, an em dash or hyphen,
//! and a nonempty reason. Each lint supplies its own tag to
//! [`markers_by_line`] and decides which line(s) it consults.
//!
//! This module is test-only infrastructure: the crate is `publish = false`
//! and reachable only as a `[dev-dependencies]` entry, so nothing here ever
//! ships in the daemon.

use std::fs;
use std::path::{Path, PathBuf};

const CFG_TEST_TOKENS: &[&str] = &["#", "[", "cfg", "(", "test", ")", "]"];

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

/// A string literal found by [`lex`], with its body as written (escape
/// sequences are not evaluated; raw strings are taken verbatim) and the
/// 1-based line it opens on. Call [`Literal::cooked`] for the value the
/// program sees.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Literal {
    /// Char offset of the opening quote in the source.
    pub offset: usize,
    /// 1-based line the opening quote sits on.
    pub line: usize,
    /// The body between the delimiters, as written.
    pub text: String,
    /// A raw string (`r"…"`, `br#"…"#`, …): its body has no escapes to cook.
    pub raw: bool,
}

impl Literal {
    /// The literal's value with escape sequences evaluated (see [`cook`]); a
    /// raw string's body is returned as written.
    #[must_use]
    pub fn cooked(&self) -> String {
        if self.raw {
            self.text.clone()
        } else {
            cook(&self.text)
        }
    }
}

/// A real `//` line comment found by [`lex`] (never one nested inside a block
/// comment or a string literal).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineComment {
    /// 1-based line the comment sits on.
    pub line: usize,
    /// Only whitespace precedes the `//` on its line.
    pub standalone: bool,
    /// The comment text from `//` to the end of the line.
    pub text: String,
}

/// Source text with comments/literals blanked, plus what the lexer passed
/// over on the way.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Lexed {
    /// `src` with every comment, string literal, and char literal replaced
    /// by spaces; newlines and char offsets are preserved.
    pub blanked: String,
    /// Every string literal, in source order.
    pub literals: Vec<Literal>,
    /// Every `//` line comment, in source order.
    pub line_comments: Vec<LineComment>,
}

/// Opt-out marker state of one source line.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Marker {
    /// No standalone line comment starting with the marker prefix.
    Absent,
    /// `// <tag>: allow — <reason>` with a nonempty reason.
    WithReason,
    /// Starts like the marker but is not well-formed: a longer token, no
    /// dash, or an empty reason.
    Malformed,
}

/// One statement of blanked source, as [`split_statements`] cuts it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Statement {
    /// 1-based line of the statement's first non-whitespace character.
    pub line: usize,
    /// The statement text; newlines inside it map to source lines.
    pub text: String,
}

/// One `#[cfg(test)]` attribute together with the whole item that follows
/// it, as [`cfg_test_items`] finds them on blanked text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CfgTestItem {
    /// Char range `[start, end)` from the `#` to just past the item's end.
    pub start: usize,
    /// Char index just past the `}` or `;` ending the item.
    pub end: usize,
    /// `Some(name)` for a `mod name;` declaration: an out-of-line module
    /// whose file is test code too.
    pub out_of_line_mod: Option<String>,
}

/// Marker state of one line comment's text for the opt-out tag `tag` (the
/// bare tag, e.g. `running-turn`, whose marker is `// running-turn: allow`):
/// `Absent` unless it starts with the marker prefix; `WithReason` only when
/// the token is exactly the marker (not a longer word such as `allowance`)
/// followed by whitespace, a dash, and a nonempty reason; anything else that
/// starts like the marker is `Malformed`.
#[must_use]
pub fn classify_marker(comment: &str, tag: &str) -> Marker {
    let Some(rest) = comment
        .strip_prefix("// ")
        .and_then(|c| c.strip_prefix(tag))
        .and_then(|c| c.strip_prefix(": allow"))
    else {
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

/// Opt-out marker state per line for the tag `tag`; index 0 is a placeholder
/// so the vector is addressed by 1-based line number. Only a standalone `//`
/// line comment can carry the marker.
#[must_use]
pub fn markers_by_line(src: &str, line_comments: &[LineComment], tag: &str) -> Vec<Marker> {
    let mut out = vec![Marker::Absent; src.lines().count() + 1];
    for comment in line_comments.iter().filter(|c| c.standalone) {
        if let Some(slot) = out.get_mut(comment.line) {
            *slot = classify_marker(&comment.text, tag);
        }
    }
    out
}

/// Pushes `c` onto `out` blanked: a newline stays a newline, anything else
/// becomes a space, so line and char offsets survive blanking.
pub fn push_blank(out: &mut String, c: char) {
    out.push(if c == '\n' { '\n' } else { ' ' });
}

/// Evaluates the escape sequences of a non-raw string literal's body so a
/// rule over literal text sees the value the program sees (`"task\u{3a}bogus"`
/// is `task:bogus` at runtime). Unknown or malformed escapes are kept as
/// written; rustc rejects those files anyway.
#[must_use]
pub fn cook(body: &str) -> String {
    let chars: Vec<char> = body.chars().collect();
    let mut out = String::with_capacity(body.len());
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        i += 1;
        if c != '\\' {
            out.push(c);
            continue;
        }
        let Some(&e) = chars.get(i) else {
            out.push(c);
            break;
        };
        i += 1;
        match e {
            'n' => out.push('\n'),
            'r' => out.push('\r'),
            't' => out.push('\t'),
            '0' => out.push('\0'),
            '\\' | '"' | '\'' => out.push(e),
            '\n' => {
                while i < chars.len() && chars[i].is_whitespace() {
                    i += 1;
                }
            }
            'x' => {
                let hex: String = chars[i..chars.len().min(i + 2)].iter().collect();
                let byte = u8::from_str_radix(&hex, 16).ok().filter(|_| hex.len() == 2);
                if let Some(b) = byte {
                    out.push(b as char);
                    i += 2;
                } else {
                    out.push('\\');
                    out.push('x');
                }
            }
            'u' if chars.get(i) == Some(&'{') => {
                let close = chars[i..].iter().position(|&c| c == '}');
                let cooked = close.and_then(|len| {
                    let hex: String = chars[i + 1..i + len]
                        .iter()
                        .filter(|&&c| c != '_')
                        .collect();
                    u32::from_str_radix(&hex, 16)
                        .ok()
                        .and_then(char::from_u32)
                        .map(|ch| (ch, len))
                });
                if let Some((ch, len)) = cooked {
                    out.push(ch);
                    i += len + 1;
                } else {
                    out.push('\\');
                    out.push('u');
                }
            }
            _ => {
                out.push('\\');
                out.push(e);
            }
        }
    }
    out
}

/// `Some(hashes)` when a raw string literal (`r"`, `r#"`, `br"`, `cr#"`, …)
/// starts at `i`; `None` otherwise. Cooked `b"…"` / `c"…"` strings need no
/// special case: their prefix letter is left as an inert identifier and the
/// `"` branch of [`lex`] consumes the body.
#[must_use]
pub fn raw_string_hashes(chars: &[char], i: usize) -> Option<usize> {
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
#[must_use]
pub fn lex(src: &str) -> Lexed {
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
                raw: true,
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
                raw: false,
            });
        } else if c == '\'' {
            // `'\…'` and `'x'` are char literals; anything else is a lifetime
            // or loop label.
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
#[must_use]
pub fn starts_with_cfg_test(chars: &[char], i: usize) -> bool {
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

/// Index of the first non-whitespace char at or after `j` (`chars.len()`
/// when there is none).
#[must_use]
pub fn skip_whitespace(chars: &[char], mut j: usize) -> usize {
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

/// `(word, index past it)` for the ASCII identifier (`[A-Za-z0-9_]+`)
/// starting at `j`, if any.
#[must_use]
pub fn word_at(chars: &[char], j: usize) -> Option<(String, usize)> {
    let mut k = j;
    while chars
        .get(k)
        .is_some_and(|c| c.is_ascii_alphanumeric() || *c == '_')
    {
        k += 1;
    }
    (k > j).then(|| (chars[j..k].iter().collect(), k))
}

/// How the item introduced after the attribute(s) starting at `j` ends.
struct ItemShape {
    /// Ends at a `;` at depth 0 rather than at the `}` closing a body.
    ends_at_semicolon: bool,
    /// `Some(name)` for a `mod name;` declaration.
    out_of_line_mod: Option<String>,
}

/// Looks past further attributes and qualifiers (`pub(crate)`, `unsafe`, …)
/// to the item keyword; an unrecognized item is treated as body-terminated.
fn item_shape(chars: &[char], mut j: usize) -> ItemShape {
    let body = ItemShape {
        ends_at_semicolon: false,
        out_of_line_mod: None,
    };
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
                let Some((word, next)) = word_at(chars, j) else {
                    return body;
                };
                j = next;
                if word == "mod" {
                    let after = skip_whitespace(chars, j);
                    if let Some((name, end)) = word_at(chars, after) {
                        if chars.get(skip_whitespace(chars, end)) == Some(&';') {
                            return ItemShape {
                                ends_at_semicolon: true,
                                out_of_line_mod: Some(name),
                            };
                        }
                    }
                    return body;
                }
                if BODY_ITEM_KEYWORDS.contains(&word.as_str()) {
                    return body;
                }
                if word == "const" {
                    let after = skip_whitespace(chars, j);
                    let is_fn = word_at(chars, after)
                        .is_some_and(|(w, _)| FN_QUALIFIERS.contains(&w.as_str()));
                    return ItemShape {
                        ends_at_semicolon: !is_fn,
                        out_of_line_mod: None,
                    };
                }
                if SEMICOLON_ITEM_KEYWORDS.contains(&word.as_str()) {
                    return ItemShape {
                        ends_at_semicolon: true,
                        out_of_line_mod: None,
                    };
                }
            }
            _ => return body,
        }
    }
}

/// Whether the item introduced after the attribute(s) starting at `j` ends at
/// a `;` at depth 0 rather than at the `}` closing its body (see the module
/// doc for which items do). Looks past further attributes and qualifiers
/// (`pub(crate)`, `unsafe`, …) to the item keyword; an unrecognized item is
/// treated as body-terminated.
#[must_use]
pub fn cfg_test_item_ends_at_semicolon(chars: &[char], j: usize) -> bool {
    item_shape(chars, j).ends_at_semicolon
}

/// Every `#[cfg(test)]` attribute together with the whole item that follows
/// it, in source order. Depth counts `(…)` and `[…]` as well as `{…}`, so a
/// `;` inside an array type (`[&str; 1]`) or a parameter list never ends the
/// item (intent-hq/intentd#2073, `f2c685af`). Runs on already-blanked text,
/// so the attribute cannot hide inside a string or comment.
#[must_use]
pub fn cfg_test_items(chars: &[char]) -> Vec<CfgTestItem> {
    let mut out = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        if !starts_with_cfg_test(chars, i) {
            i += 1;
            continue;
        }
        let start = i;
        let shape = item_shape(chars, i);
        let mut depth = 0usize;
        while i < chars.len() {
            let c = chars[i];
            i += 1;
            match c {
                '{' | '(' | '[' => depth += 1,
                ')' | ']' => depth = depth.saturating_sub(1),
                '}' => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 && !shape.ends_at_semicolon {
                        break;
                    }
                }
                ';' if depth == 0 => break,
                _ => {}
            }
        }
        out.push(CfgTestItem {
            start,
            end: i,
            out_of_line_mod: shape.out_of_line_mod,
        });
    }
    out
}

/// Char ranges `[start, end)` of every `#[cfg(test)]` item of
/// [`cfg_test_items`].
#[must_use]
pub fn cfg_test_item_ranges(chars: &[char]) -> Vec<(usize, usize)> {
    cfg_test_items(chars)
        .into_iter()
        .map(|item| (item.start, item.end))
        .collect()
}

/// Blanks every `#[cfg(test)]` item range of `text` (newlines preserved).
#[must_use]
pub fn blank_cfg_test_items(text: &str) -> String {
    let mut chars: Vec<char> = text.chars().collect();
    for (start, end) in cfg_test_item_ranges(&chars) {
        for c in &mut chars[start..end] {
            if *c != '\n' {
                *c = ' ';
            }
        }
    }
    chars.into_iter().collect()
}

/// 1-based line of the first non-whitespace character of the statement (text
/// back to the previous `;` / `{` / `}`) containing the char at `offset` of
/// the blanked source; `offset` itself when nothing precedes it there.
#[must_use]
pub fn statement_line(chars: &[char], offset: usize) -> usize {
    let boundary = chars[..offset]
        .iter()
        .rposition(|c| matches!(c, ';' | '{' | '}'))
        .map_or(0, |b| b + 1);
    let start = skip_whitespace(chars, boundary).min(offset);
    chars[..start].iter().filter(|c| **c == '\n').count() + 1
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
/// statement keeps one newline per source line the block body spanned, so a
/// lint's own tokenizer still reports source lines for the text after the
/// block (intent-hq/intentd#2073, `102c347a`). Statements come back in
/// source order.
#[must_use]
pub fn split_statements(text: &str) -> Vec<Statement> {
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

/// The cargo workspace root (the directory holding `Cargo.toml` and
/// `crates/`), resolved from this crate's own manifest directory so every
/// lint gets the same answer whichever crate it lives in.
#[must_use]
pub fn workspace_root() -> PathBuf {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    match manifest.ancestors().nth(2) {
        Some(root) => root.to_path_buf(),
        None => manifest.to_path_buf(),
    }
}

/// The one traversal behind [`rust_files`] and [`crate_src_files`]: appends
/// every `*.rs` file under `dir` to `out`, descending into a subdirectory
/// only when `prune` returns `false` for it. A pruned directory is never
/// `read_dir`'d, so an excluded subtree may be unreadable without failing the
/// walk; every directory that is entered must be readable.
fn walk_rust_files(dir: &Path, prune: &dyn Fn(&Path) -> bool, out: &mut Vec<PathBuf>) {
    let entries = fs::read_dir(dir).unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display()));
    for entry in entries {
        let entry = entry.unwrap_or_else(|e| panic!("read_dir {}: {e}", dir.display()));
        let path = entry.path();
        if path.is_dir() {
            if !prune(&path) {
                walk_rust_files(&path, prune, out);
            }
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

/// Every `*.rs` file under `dir` (recursively, nothing pruned), sorted.
///
/// Fail-fast: a `read_dir` or entry error anywhere in the tree panics naming
/// the offending directory. A lint must never pass because it could not read
/// part of the tree — a silently empty subtree would still satisfy a
/// `!files.is_empty()` guard. Callers scanning an optional directory guard
/// with `is_dir()` first.
///
/// # Panics
///
/// When `dir` or any subdirectory cannot be read or listed.
#[must_use]
pub fn rust_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    walk_rust_files(dir, &|_| false, &mut out);
    out.sort();
    out
}

/// Every non-test source file `crates/*/src/**/*.rs` under `root`, sorted:
/// `tests/` directories and files named `tests.rs` are skipped (test code by
/// convention; `#[cfg(test)]` items inside the remaining files are the
/// caller's business via [`blank_cfg_test_items`]).
///
/// Fail-fast like [`rust_files`]: every directory under `crates/` is a crate
/// and must have a readable `src/`. Non-directory entries in `crates/`
/// (stray files) are not crates and are skipped. A `tests/` directory is
/// pruned before it is read, so an unreadable test fixture tree does not fail
/// a lint that never needed it.
///
/// # Panics
///
/// When `crates/` or any crate's `src/` tree, `tests/` directories excepted,
/// cannot be read or listed.
#[must_use]
pub fn crate_src_files(root: &Path) -> Vec<PathBuf> {
    let crates_dir = root.join("crates");
    let crates = fs::read_dir(&crates_dir)
        .unwrap_or_else(|e| panic!("read_dir {}: {e}", crates_dir.display()));
    let mut out = Vec::new();
    for entry in crates {
        let entry = entry.unwrap_or_else(|e| panic!("read_dir {}: {e}", crates_dir.display()));
        let krate = entry.path();
        if !krate.is_dir() {
            continue;
        }
        let mut files = Vec::new();
        walk_rust_files(&krate.join("src"), &is_tests_dir, &mut files);
        out.extend(
            files
                .into_iter()
                .filter(|path| path.file_name().is_some_and(|name| name != "tests.rs")),
        );
    }
    out.sort();
    out
}

fn is_tests_dir(dir: &Path) -> bool {
    dir.file_name().is_some_and(|name| name == "tests")
}

#[cfg(test)]
mod tests {
    use std::ffi::OsStr;
    use std::path::Component;

    use super::*;

    fn line_of(src: &str, needle: &str) -> usize {
        let Some(index) = src.lines().position(|l| l.contains(needle)) else {
            panic!("fixture has no line containing {needle:?}")
        };
        index + 1
    }

    /// `src` as a lint scans it: comments, literals, and `#[cfg(test)]` items
    /// blanked.
    fn production_text(src: &str) -> String {
        blank_cfg_test_items(&lex(src).blanked)
    }

    #[test]
    fn lexer_edge_cases_and_cfg_qualifiers() {
        let src = r###"
/* outer /* nested */ AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing */
fn a(c: char) -> bool {
    let quote = '\'';
    let backslash = '\\';
    let raw = r##"AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing "# still"##;
    c == quote || c == backslash || raw.is_empty()
}

#[cfg(all(test, unix))]
fn qualified(s: AgentStatus) -> bool {
    matches!(s, AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing)
}

#[cfg(test)]
mod tests {
    const RUNNING: [AgentStatus; 3] = [AgentStatus::Pending, AgentStatus::Active, AgentStatus::Processing];
    fn helper(s: AgentStatus) -> bool {
        matches!(s, AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing)
    }
}

fn scanned(s: AgentStatus) -> bool {
    matches!(s, AgentStatus::Pending | AgentStatus::Active | AgentStatus::Processing)
}
"###;
        let text = production_text(src);
        assert_eq!(text.lines().count(), src.lines().count());
        assert!(
            !text.contains("outer") && !text.contains("nested"),
            "nested block comment is blanked whole"
        );
        assert!(
            !text.contains('"') && !text.contains('\'') && !text.contains("still"),
            "char literals and the raw string are blanked with their delimiters"
        );
        assert!(text.contains("let quote =") && text.contains("raw.is_empty()"));
        assert!(
            text.contains("fn qualified"),
            "`#[cfg(all(test, unix))]` is ordinary code"
        );
        assert!(
            !text.contains("mod tests") && !text.contains("RUNNING") && !text.contains("helper"),
            "`#[cfg(test)] mod tests {{ … }}` is blanked through its closing brace"
        );
        assert_eq!(line_of(&text, "fn scanned"), line_of(src, "fn scanned"));
        assert_eq!(
            text.matches("AgentStatus::Pending").count(),
            2,
            "only the `qualified` and `scanned` patterns survive"
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
                    text: "x'y\\\"z".into(),
                    raw: false,
                },
                Literal {
                    offset: 28,
                    line: 2,
                    text: "in ('a', 'b')".into(),
                    raw: true,
                },
            ]
        );
        assert_eq!(lexed.line_comments.len(), 1);
        assert!(lexed.line_comments[0].standalone);
        assert_eq!(lexed.blanked.lines().count(), src.lines().count());
        assert!(!lexed.blanked.contains('"') && !lexed.blanked.contains("tail"));
    }

    #[test]
    fn cooked_literals_evaluate_escapes_but_raw_bodies_stay_as_written() {
        let src = r#"let a = "task\u{3a}bogus\x41\n"; let b = r"x\n"; let c = b"\
            joined";"#;
        let lexed = lex(src);
        assert_eq!(lexed.literals[0].cooked(), "task:bogusA\n");
        assert_eq!(lexed.literals[1].cooked(), "x\\n");
        assert_eq!(lexed.literals[2].cooked(), "joined");
        assert_eq!(lexed.literals[2].line, 1);
    }

    #[test]
    fn cfg_test_item_survives_semicolons_inside_brackets_and_parens() {
        let src = r"
#[cfg(test)]
fn probe(_: [u8; 1]) -> bool {
    true
}

fn after_fn() -> bool { false }

#[cfg(test)]
const TABLE: [fn() -> bool; 1] =
    [|| true];

fn after_const() -> bool { false }

#[cfg(test)]
static PAIRS: [(u8, bool); 1] = [(0, true)];

fn after_static() -> bool { false }

fn with_test_statement() -> bool {
    #[cfg(test)]
    let probe: [u8; 1] = [0];
    false
}
";
        let text = production_text(src);
        assert!(
            !text.contains("probe") && !text.contains("true"),
            "every `#[cfg(test)]` item is blanked to its real end:\n{text}"
        );
        assert!(!text.contains("TABLE") && !text.contains("PAIRS"));
        for name in [
            "fn after_fn",
            "fn after_const",
            "fn after_static",
            "fn with_test_statement",
        ] {
            assert_eq!(line_of(&text, name), line_of(src, name));
        }
        assert!(text.contains("    false\n}"));

        let chars: Vec<char> = lex(src).blanked.chars().collect();
        let ranges = cfg_test_item_ranges(&chars);
        let terminators: Vec<char> = ranges.iter().map(|&(_, end)| chars[end - 1]).collect();
        assert_eq!(terminators, vec!['}', ';', ';', ';']);
        assert_eq!(
            ranges,
            cfg_test_items(&chars)
                .iter()
                .map(|i| (i.start, i.end))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn cfg_test_items_name_out_of_line_modules_and_end_at_the_item_keyword_says() {
        let src = "#[cfg(test)]\nmod tests;\n\
                   #[cfg(test)]\npub(crate) mod fixtures { fn f() {} }\n\
                   #[cfg(test)]\nuse self::{a, b};\n\
                   fn live() {}\n\
                   # [cfg( test )]\nconst fn probe() -> u8 { 1 }\n\
                   struct Marker;\n";
        let chars: Vec<char> = src.chars().collect();
        let items = cfg_test_items(&chars);
        assert_eq!(
            items
                .iter()
                .filter_map(|i| i.out_of_line_mod.clone())
                .collect::<Vec<_>>(),
            vec!["tests".to_string()]
        );
        assert_eq!(items.len(), 4);
        assert!(cfg_test_item_ends_at_semicolon(&chars, items[0].start));
        assert!(!cfg_test_item_ends_at_semicolon(&chars, items[1].start));
        assert!(cfg_test_item_ends_at_semicolon(&chars, items[2].start));
        assert!(!cfg_test_item_ends_at_semicolon(&chars, items[3].start));

        let text = blank_cfg_test_items(src);
        assert!(text.contains("fn live") && text.contains("struct Marker"));
        assert!(!text.contains("fixtures") && !text.contains("self") && !text.contains("probe"));
    }

    #[test]
    fn operand_block_keeps_the_lines_of_the_text_after_it() {
        let src = "\
fn f(s: S) -> bool {
    matches!(
        {
            s
        }.clone(),
        // demo-lint: allow — fixture
        Pattern::A | Pattern::B
    )
}
";
        let lexed = lex(src);
        let statements = split_statements(&lexed.blanked);
        let chained = statements
            .iter()
            .find(|s| s.text.contains("matches!") && s.text.contains("Pattern::A"))
            .expect("the block operand chains with the `matches!` call");
        assert_eq!(chained.line, line_of(src, "matches!("));
        let body = chained.text.trim_start();
        let before = &body[..body.find("Pattern::A").expect("pattern")];
        let pattern_line = chained.line + before.matches('\n').count();
        assert_eq!(pattern_line, line_of(src, "Pattern::A"));
        let markers = markers_by_line(src, &lexed.line_comments, "demo-lint");
        assert_eq!(markers[pattern_line - 1], Marker::WithReason);
        assert_eq!(markers[pattern_line], Marker::Absent);

        let src = "let s = if c {\n    a\n} else {\n    b\n}.status();\nlet t = x?;\n";
        let statements = split_statements(src);
        let chained = &statements[0];
        assert_eq!(chained.line, 1);
        let before = &chained.text[..chained.text.find(".status()").expect("call")];
        assert_eq!(
            chained.line + before.matches('\n').count(),
            line_of(src, ".status()")
        );
        assert_eq!(statements.last().map(|s| s.line), Some(6));
    }

    #[test]
    fn markers_are_classified_per_tag() {
        let src = "\
// alpha-lint: allow — reasoned
let a = 1;
// beta-lint: allow — other tag
let b = 2;
// alpha-lint: allow
let c = 3;
// alpha-lint: allowance — longer token
let d = 4;
let e = 5; // alpha-lint: allow — trailing
/* alpha-lint: allow — block */
let f = 6;
// alpha-lint: allow - hyphen is fine
let g = 7;
// alpha-lint: allow —
let h = 8;
";
        let lexed = lex(src);
        let alpha = markers_by_line(src, &lexed.line_comments, "alpha-lint");
        let beta = markers_by_line(src, &lexed.line_comments, "beta-lint");
        assert_eq!(alpha.len(), src.lines().count() + 1);
        assert_eq!(alpha[0], Marker::Absent, "index 0 is the placeholder");
        assert_eq!((alpha[1], beta[1]), (Marker::WithReason, Marker::Absent));
        assert_eq!((alpha[3], beta[3]), (Marker::Absent, Marker::WithReason));
        assert_eq!(alpha[5], Marker::Malformed, "no reason");
        assert_eq!(alpha[7], Marker::Malformed, "longer token");
        assert_eq!(
            alpha[9],
            Marker::Absent,
            "trailing comment is not standalone"
        );
        assert_eq!(alpha[10], Marker::Absent, "block comment never counts");
        assert_eq!(alpha[12], Marker::WithReason, "hyphen");
        assert_eq!(alpha[14], Marker::Malformed, "empty reason");
        assert!(beta.iter().skip(4).all(|m| *m == Marker::Absent));
    }

    #[test]
    fn statement_line_walks_back_to_the_previous_boundary() {
        let src = "fn f() {\n    let x =\n        g(\"lit\");\n}\n";
        let lexed = lex(src);
        let chars: Vec<char> = lexed.blanked.chars().collect();
        assert_eq!(statement_line(&chars, lexed.literals[0].offset), 2);
        assert_eq!(statement_line(&chars, 0), 1);
    }

    #[test]
    fn walkers_are_sorted_and_skip_test_code() {
        let root = workspace_root();
        assert!(root.join("Cargo.toml").is_file() && root.join("crates").is_dir());

        let files = crate_src_files(&root);
        assert!(!files.is_empty());
        assert!(
            files.windows(2).all(|w| w[0] < w[1]),
            "sorted, no duplicates"
        );
        for file in &files {
            let rel = file.strip_prefix(&root).expect("under root");
            let comps: Vec<_> = rel.components().map(Component::as_os_str).collect();
            assert_eq!(&comps[..1], ["crates"]);
            assert_eq!(comps[2], "src");
            assert!(
                !comps[3..].contains(&OsStr::new("tests")),
                "{}",
                rel.display()
            );
            assert_ne!(file.file_name(), Some(OsStr::new("tests.rs")));
            assert_eq!(file.extension(), Some(OsStr::new("rs")));
        }
        let own = root.join("crates/intentd-test-support/src/source_lint.rs");
        assert!(files.contains(&own));

        let dir = root.join("crates/intentd-test-support/src");
        let listed = rust_files(&dir);
        assert!(listed.contains(&own) && listed.contains(&dir.join("lib.rs")));
        assert!(listed.windows(2).all(|w| w[0] < w[1]));
    }

    fn panic_message(f: impl FnOnce() + std::panic::UnwindSafe) -> String {
        let err = std::panic::catch_unwind(f).expect_err("expected a panic");
        err.downcast_ref::<String>()
            .cloned()
            .or_else(|| err.downcast_ref::<&str>().map(ToString::to_string))
            .expect("panic payload is a string")
    }

    #[test]
    fn rust_files_panics_naming_an_unreadable_directory() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let dir = tmp.path().join("not-a-dir");
        fs::write(&dir, "").expect("write");

        let msg = panic_message(|| {
            let _ = rust_files(&dir);
        });
        assert!(msg.starts_with("read_dir "), "{msg}");
        assert!(msg.contains(&dir.display().to_string()), "{msg}");

        let missing = tmp.path().join("missing");
        let msg = panic_message(|| {
            let _ = rust_files(&missing);
        });
        assert!(msg.contains(&missing.display().to_string()), "{msg}");
    }

    #[test]
    fn rust_files_walks_nested_directories_and_an_empty_directory_is_empty() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let src = tmp.path().join("src");
        fs::create_dir(&src).expect("mkdir");
        fs::write(src.join("lib.rs"), "").expect("write");
        fs::write(src.join("notes.md"), "").expect("write");
        let nested = src.join("nested");
        fs::create_dir(&nested).expect("mkdir");
        fs::write(nested.join("a.rs"), "").expect("write");

        assert_eq!(
            rust_files(&src),
            vec![src.join("lib.rs"), nested.join("a.rs")]
        );

        let empty = tmp.path().join("empty");
        fs::create_dir(&empty).expect("mkdir");
        assert!(rust_files(&empty).is_empty());
    }

    #[test]
    fn crate_src_files_panics_naming_an_unreadable_crate_src() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path();
        let good = root.join("crates/good/src");
        fs::create_dir_all(&good).expect("mkdir");
        fs::write(good.join("lib.rs"), "").expect("write");
        fs::write(good.join("tests.rs"), "").expect("write");
        fs::create_dir(good.join("tests")).expect("mkdir");
        fs::write(good.join("tests/t.rs"), "").expect("write");
        fs::write(root.join("crates/.stray-file"), "").expect("write");

        assert_eq!(crate_src_files(root), vec![good.join("lib.rs")]);

        let bad = root.join("crates/bad");
        fs::create_dir(&bad).expect("mkdir");
        fs::write(bad.join("src"), "").expect("write");
        let msg = panic_message(|| {
            let _ = crate_src_files(root);
        });
        assert!(msg.starts_with("read_dir "), "{msg}");
        assert!(
            msg.contains(&bad.join("src").display().to_string()),
            "{msg}"
        );

        let no_crates = tmp.path().join("no-crates");
        fs::create_dir(&no_crates).expect("mkdir");
        let msg = panic_message(|| {
            let _ = crate_src_files(&no_crates);
        });
        assert!(
            msg.contains(&no_crates.join("crates").display().to_string()),
            "{msg}"
        );
    }

    #[test]
    fn crate_src_files_prunes_tests_directories_before_reading_them() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let root = tmp.path();
        let src = root.join("crates/k/src");
        fs::create_dir_all(src.join("tests/nested")).expect("mkdir");
        fs::create_dir_all(src.join("inner/tests")).expect("mkdir");
        fs::write(src.join("lib.rs"), "").expect("write");
        fs::write(src.join("inner/a.rs"), "").expect("write");
        fs::write(src.join("tests/t.rs"), "").expect("write");
        fs::write(src.join("tests/nested/n.rs"), "").expect("write");
        fs::write(src.join("inner/tests/i.rs"), "").expect("write");

        // The shared traversal asks about a `tests` directory and never about
        // anything below it; the unpruned walk lists everything.
        let asked = std::cell::RefCell::new(Vec::new());
        let mut pruned = Vec::new();
        walk_rust_files(
            &src,
            &|dir| {
                asked.borrow_mut().push(dir.to_path_buf());
                is_tests_dir(dir)
            },
            &mut pruned,
        );
        pruned.sort();
        let mut asked = asked.into_inner();
        asked.sort();
        assert_eq!(pruned, vec![src.join("inner/a.rs"), src.join("lib.rs")]);
        assert_eq!(
            asked,
            vec![
                src.join("inner"),
                src.join("inner/tests"),
                src.join("tests")
            ]
        );
        assert_eq!(rust_files(&src).len(), 5);
        assert_eq!(
            crate_src_files(root),
            vec![src.join("inner/a.rs"), src.join("lib.rs")]
        );

        // With the fixture tree unreadable (a no-op as root, so only asserted
        // when it took effect) the production-only walk still succeeds while
        // the unpruned one fails fast naming the directory.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let tests = src.join("tests");
            let readable = fs::metadata(&tests).expect("metadata").permissions();
            fs::set_permissions(&tests, fs::Permissions::from_mode(0o000)).expect("chmod");
            let unreadable = fs::read_dir(&tests).is_err();
            let listed = std::panic::catch_unwind(|| crate_src_files(root));
            let unpruned = std::panic::catch_unwind(|| rust_files(&src));
            fs::set_permissions(&tests, readable).expect("chmod");

            assert_eq!(
                listed.expect("crate_src_files must not read a pruned tests/ directory"),
                vec![src.join("inner/a.rs"), src.join("lib.rs")]
            );
            if unreadable {
                let err = unpruned.expect_err("rust_files must fail fast on an unreadable dir");
                let msg = err
                    .downcast_ref::<String>()
                    .cloned()
                    .expect("panic payload is a string");
                assert!(msg.starts_with("read_dir "), "{msg}");
                assert!(msg.contains(&tests.display().to_string()), "{msg}");
            }
        }
    }
}
