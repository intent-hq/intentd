//! Event-type literal lint.
//!
//! `intent_core::events::ALL_EVENT_TYPES` is the canonical event catalog and
//! is mirrored into the checked-in golden `tests/goldens/event_types.json`
//! that clients copy verbatim. An emitter that spells a `note:` / `task:` /
//! `workspace:` / `agent:` type as a bare string literal can drift from that
//! catalog silently. This source-scanning test fails, naming `file:line`,
//! for every string literal under `crates/*/src/**/*.rs` that looks like an
//! event type in one of those namespaces but is not in `ALL_EVENT_TYPES`.
//!
//! The heuristic is deliberately small:
//!
//! - A literal "looks like an event type" when its whole content matches
//!   `(note|task|workspace|agent):[A-Za-z:_-]+`. Wildcard subscription
//!   patterns (`"task:*"`), bare namespaces (`"agent:"`), format strings, and
//!   prose containing a type are therefore never considered.
//! - A literal ending in `:` is a documented prefix and passes when some
//!   catalog type starts with it (`"agent:stream:"`).
//! - Skipped: `crates/intent-core/src/events.rs` (the catalog itself), any
//!   file named `tests.rs` or under a `tests/` directory, any module file
//!   declared as `#[cfg(test)] mod name;` (and everything under its
//!   directory), and any `#[cfg(test)]` item, attribute to end of item — an
//!   item introduced by `const` / `static` / `type` / `use` / `mod name;`
//!   ends at the first `;` at brace depth 0; one introduced by `fn` / `mod`
//!   / `impl` / `struct` / `enum` / `union` / `trait` / `macro_rules` ends at
//!   the `}` closing its body, or at a `;` at depth 0 seen first. Comments
//!   are ignored, so a type quoted in a doc comment never counts.
//! - Opt-out: `// event-type-lint: allow — <reason>` on the line immediately
//!   above the literal's line. The marker counts only as a standalone `//`
//!   line comment, the token must be exactly `event-type-lint: allow`, and
//!   it must be followed by whitespace, an em dash or hyphen, and a nonempty
//!   reason. A malformed marker never suppresses the hit; the report says so.
//!   The opt-out is for non-event uses of such a string (a message-metadata
//!   pseudo-type, a fixture); a real emitter must be added to the catalog.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Component, Path, PathBuf};

use intent_core::events::ALL_EVENT_TYPES;

const NAMESPACES: &[&str] = &["note", "task", "workspace", "agent"];
const OPT_OUT_MARKER: &str = "// event-type-lint: allow";
const CFG_TEST_TOKENS: &[&str] = &["#", "[", "cfg", "(", "test", ")", "]"];
const EXEMPT_FILE: &[&str] = &["crates", "intent-core", "src", "events.rs"];

#[derive(Debug, Clone, PartialEq, Eq)]
struct Hit {
    line: usize,
    literal: String,
    /// The line above carried something that starts like the opt-out marker
    /// but is malformed (longer token, or no reason).
    marker_malformed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Marker {
    Absent,
    WithReason,
    Malformed,
}

/// A string literal found by the lexer, with its cooked content (escape
/// sequences evaluated; raw strings taken as written) and the 1-based line it
/// opens on.
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

/// One scanned file: its unsuppressed hits and the names of the modules it
/// declares as `#[cfg(test)] mod name;` (whose files are test code too).
struct Scanned {
    hits: Vec<Hit>,
    test_mods: Vec<String>,
}

/// Marker state of one line comment's text: `Absent` unless it starts with
/// the marker prefix; `WithReason` only when the token is exactly the marker
/// followed by whitespace, a dash, and a nonempty reason; anything else that
/// starts like the marker is `Malformed`.
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

/// Evaluates the escape sequences of a non-raw string literal's body so the
/// lint classifies the value the program sees (`"task\u{3a}bogus"` is
/// `task:bogus` at runtime). Unknown or malformed escapes are kept as written;
/// rustc rejects those files anyway.
fn cook(body: &str) -> String {
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
/// blanked (newlines preserved) in `blanked`, every string literal's content
/// is collected with its line, and every `//` line comment is reported
/// (only those may carry the opt-out marker).
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
                text: cook(&chars[body_start..body_end].iter().collect::<String>()),
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
/// whitespace between tokens.
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

/// How the `#[cfg(test)]` item whose attribute starts at `j` is delimited.
struct CfgTestItem {
    /// Ends at a `;` at brace depth 0 rather than at the `}` closing a body.
    ends_at_semicolon: bool,
    /// `Some(name)` for a `mod name;` declaration (an out-of-line module
    /// whose file is test code).
    out_of_line_mod: Option<String>,
}

/// Looks past further attributes and qualifiers (`pub(crate)`, `unsafe`, …)
/// to the item keyword; an unrecognized item is treated as body-terminated.
fn cfg_test_item(chars: &[char], mut j: usize) -> CfgTestItem {
    let body = CfgTestItem {
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
                let (word, next) = word_at(chars, j).expect("identifier start");
                j = next;
                if word == "mod" {
                    let after = skip_whitespace(chars, j);
                    if let Some((name, end)) = word_at(chars, after) {
                        if chars.get(skip_whitespace(chars, end)) == Some(&';') {
                            return CfgTestItem {
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
                    return CfgTestItem {
                        ends_at_semicolon: !is_fn,
                        out_of_line_mod: None,
                    };
                }
                if SEMICOLON_ITEM_KEYWORDS.contains(&word.as_str()) {
                    return CfgTestItem {
                        ends_at_semicolon: true,
                        out_of_line_mod: None,
                    };
                }
            }
            _ => return body,
        }
    }
}

/// Per-char mask of every `#[cfg(test)]` attribute together with the whole
/// item that follows it, plus the names of the out-of-line test modules
/// declared that way. Runs on blanked text, so the attribute cannot hide
/// inside a string or comment.
fn cfg_test_mask(blanked: &str) -> (Vec<bool>, Vec<String>) {
    let chars: Vec<char> = blanked.chars().collect();
    let mut mask = vec![false; chars.len()];
    let mut test_mods = Vec::new();
    let mut i = 0;
    while i < chars.len() {
        if !starts_with_cfg_test(&chars, i) {
            i += 1;
            continue;
        }
        let item = cfg_test_item(&chars, i);
        test_mods.extend(item.out_of_line_mod);
        let mut depth = 0usize;
        while i < chars.len() {
            let c = chars[i];
            mask[i] = true;
            i += 1;
            match c {
                '{' => depth += 1,
                '}' => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 && !item.ends_at_semicolon {
                        break;
                    }
                }
                ';' if depth == 0 => break,
                _ => {}
            }
        }
    }
    (mask, test_mods)
}

/// Whether the whole literal matches `(note|task|workspace|agent):[A-Za-z:_-]+`.
fn looks_like_event_type(literal: &str) -> bool {
    let Some((namespace, rest)) = literal.split_once(':') else {
        return false;
    };
    NAMESPACES.contains(&namespace)
        && !rest.is_empty()
        && rest
            .chars()
            .all(|c| c.is_ascii_alphabetic() || matches!(c, ':' | '_' | '-'))
}

/// Whether an event-looking literal is in the catalog, or is a prefix of a
/// catalog type (a documented prefix such as `"agent:stream:"`).
fn is_catalogued(literal: &str) -> bool {
    ALL_EVENT_TYPES.contains(&literal)
        || (literal.ends_with(':') && ALL_EVENT_TYPES.iter().any(|t| t.starts_with(literal)))
}

/// Scans one Rust source file's text: every event-looking literal outside
/// test code that is not catalogued and not suppressed by a reasoned opt-out
/// marker, plus the file's `#[cfg(test)] mod name;` declarations.
fn scan_source(src: &str) -> Scanned {
    let lexed = lex(src);
    let markers = markers_by_line(src, &lexed.line_comments);
    let (mask, test_mods) = cfg_test_mask(&lexed.blanked);
    let hits = lexed
        .literals
        .into_iter()
        .filter(|lit| !mask.get(lit.offset).copied().unwrap_or(false))
        .filter(|lit| looks_like_event_type(&lit.text) && !is_catalogued(&lit.text))
        .filter_map(|lit| {
            let marker = markers.get(lit.line - 1).copied().unwrap_or(Marker::Absent);
            match marker {
                Marker::WithReason => None,
                Marker::Malformed | Marker::Absent => Some(Hit {
                    line: lit.line,
                    literal: lit.text,
                    marker_malformed: marker == Marker::Malformed,
                }),
            }
        })
        .collect();
    Scanned { hits, test_mods }
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

/// The directory holding the out-of-line child modules of `file`:
/// `lib.rs` / `main.rs` / `mod.rs` own their directory, any other `foo.rs`
/// owns `foo/`.
fn child_module_dir(file: &Path) -> PathBuf {
    let parent = file.parent().unwrap_or(Path::new(""));
    match file.file_name().and_then(|n| n.to_str()) {
        Some("lib.rs" | "main.rs" | "mod.rs") => parent.to_path_buf(),
        _ => parent.join(file.file_stem().unwrap_or_default()),
    }
}

/// Paths (a `name.rs` file and a `name/` directory) that hold the module
/// `name` declared from `file`.
fn test_module_paths(file: &Path, name: &str) -> [PathBuf; 2] {
    let dir = child_module_dir(file);
    [dir.join(format!("{name}.rs")), dir.join(name)]
}

/// Whether `file` is (or lives under) one of the collected test-module paths.
fn is_test_module_file(file: &Path, test_paths: &BTreeSet<PathBuf>) -> bool {
    test_paths.iter().any(|p| file == p || file.starts_with(p))
}

#[test]
fn event_type_literals_are_in_the_catalog() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..").join("..");
    let exempt: PathBuf = EXEMPT_FILE.iter().collect();
    assert!(
        root.join(&exempt).is_file(),
        "{} moved; update EXEMPT_FILE so the exemption keeps pointing at the catalog",
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

    let mut scanned = Vec::new();
    let mut test_paths = BTreeSet::new();
    for file in &files {
        let rel = file
            .strip_prefix(&root)
            .expect("source path under the workspace root");
        if is_exempt(rel) {
            continue;
        }
        let src =
            fs::read_to_string(file).unwrap_or_else(|e| panic!("read {}: {e}", file.display()));
        let result = scan_source(&src);
        for name in &result.test_mods {
            test_paths.extend(test_module_paths(file, name));
        }
        scanned.push((file, result.hits));
    }

    let mut report = Vec::new();
    for (file, hits) in scanned {
        if is_test_module_file(file, &test_paths) {
            continue;
        }
        let rel = file.strip_prefix(&root).expect("under root");
        for hit in hits {
            let note = if hit.marker_malformed {
                "  (opt-out marker is malformed: expected `// event-type-lint: allow — <reason>`)"
            } else {
                ""
            };
            report.push(format!(
                "{}:{}: \"{}\"{note}",
                display_rel(rel),
                hit.line,
                hit.literal
            ));
        }
    }

    assert!(
        report.is_empty(),
        "event-type string literals outside intent_core::events::ALL_EVENT_TYPES:\n\n{}\n\n\
         A real emitter must add its type to `ALL_EVENT_TYPES` in \
         crates/intent-core/src/events.rs (then regenerate the golden with \
         `INTENTD_UPDATE_GOLDENS=1 cargo test -p intent-core --test events`) and use the \
         constant. A string that is not an emitted event type may opt out with \
         `// event-type-lint: allow — <reason>` on the line immediately above; the reason is \
         required.",
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
    scan_source(src).hits.into_iter().map(|h| h.line).collect()
}

#[test]
fn flags_an_uncatalogued_literal_naming_its_line() {
    let src = r#"
fn emit(bus: &Bus) {
    bus.publish(NewEvent {
        event_type: "task:bogus".to_string(),
        ..Default::default()
    });
}
"#;
    let scanned = scan_source(src);
    assert_eq!(
        scanned.hits.iter().map(|h| h.line).collect::<Vec<_>>(),
        vec![line_of(src, "task:bogus")]
    );
    assert_eq!(scanned.hits[0].literal, "task:bogus");
    assert!(!scanned.hits[0].marker_malformed);
}

#[test]
fn catalogued_types_and_prefixes_pass() {
    let src = r#"
fn ok() -> Vec<&'static str> {
    vec!["task:status-changed", "note:created", "workspace:updated", "agent:idle", "agent:stream:"]
}
"#;
    assert_eq!(hit_lines(src), Vec::<usize>::new());
}

#[test]
fn non_event_shapes_are_never_considered() {
    let src = r#"
fn shapes(id: &str) -> Vec<String> {
    vec![
        "task:*".to_string(),
        "agent:".to_string(),
        format!("agent:{id}"),
        "expected task:status-changed here".to_string(),
        "file:changed".to_string(),
        "TASK:BOGUS".to_string(),
        "task:bogus/".to_string(),
    ]
}
"#;
    assert_eq!(hit_lines(src), Vec::<usize>::new());
}

#[test]
fn unknown_prefix_is_flagged() {
    let src = r#"
fn f() -> &'static str { "agent:bogus:" }
"#;
    assert_eq!(hit_lines(src), vec![2]);
}

#[test]
fn literals_in_comments_are_ignored() {
    let src = r#"
/// Emits `"task:bogus"` — see "workspace:nope".
fn f() -> &'static str {
    // "note:bogus"
    /* "agent:bogus" */
    "task:status-changed"
}
"#;
    assert_eq!(hit_lines(src), Vec::<usize>::new());
}

#[test]
fn raw_byte_and_c_strings_are_lexed() {
    let raw = "fn f() -> &'static str { r#\"task:bogus\"# }\n";
    assert_eq!(hit_lines(raw), vec![1]);
    let byte = "fn f() -> &'static [u8] { b\"task:bogus\" }\n";
    assert_eq!(hit_lines(byte), vec![1]);
    let quote_char = "fn f() -> &'static str { let _q = '\"'; \"task:bogus\" }\n";
    assert_eq!(hit_lines(quote_char), vec![1]);
    let raw_with_quote = "fn f() {\n    let _ = r#\"ends \"\"#;\n    let _ = \"task:bogus\";\n}\n";
    assert_eq!(hit_lines(raw_with_quote), vec![3]);
}

#[test]
fn escaped_literals_are_classified_on_their_cooked_value() {
    let unicode = "fn f() -> &'static str { \"task\\u{3a}bogus\" }\n";
    let scanned = scan_source(unicode);
    assert_eq!(
        scanned.hits.iter().map(|h| h.line).collect::<Vec<_>>(),
        vec![1]
    );
    assert_eq!(scanned.hits[0].literal, "task:bogus");
    let hex = "fn f() -> &'static str { \"\\x74ask:\\x62ogus\" }\n";
    assert_eq!(hit_lines(hex), vec![1]);
    let continuation = "fn f() -> &'static str { \"task:\\\n        bogus\" }\n";
    assert_eq!(hit_lines(continuation), vec![1]);
    let cooked_to_non_event = "fn f() -> &'static str { \"task:bo\\ngus\" }\n";
    assert_eq!(hit_lines(cooked_to_non_event), Vec::<usize>::new());
    let raw_is_not_cooked = "fn f() -> &'static str { r\"task\\u{3a}bogus\" }\n";
    assert_eq!(hit_lines(raw_is_not_cooked), Vec::<usize>::new());
}

#[test]
fn multi_line_literals_report_their_opening_line() {
    let src =
        "fn f() {\n    let _doc = \"line one\n    line two\";\n    let _ = \"task:bogus\";\n}\n";
    assert_eq!(hit_lines(src), vec![4]);
}

#[test]
fn cfg_test_items_are_skipped_but_code_after_them_is_not() {
    let src = r#"
#[cfg(test)]
mod tests {
    fn fixture() -> &'static str { "task:bogus" }
}

#[cfg(test)]
mod more_tests;

#[cfg(test)]
fn helper() -> &'static str { "note:bogus" }

#[cfg(test)]
const FIXTURE: &str = if cfg!(a) { "workspace:bogus" } else { "agent:bogus" };

#[cfg( test )]
pub(crate) fn spaced() -> &'static str { "task:also-bogus" }

#[cfg(not(test))]
fn real() -> &'static str { "task:bogus" }
"#;
    let scanned = scan_source(src);
    assert_eq!(
        scanned.hits.iter().map(|h| h.line).collect::<Vec<_>>(),
        vec![line_of(src, "fn real()")]
    );
    assert_eq!(scanned.test_mods, vec!["more_tests".to_string()]);
}

#[test]
fn cfg_test_mod_declarations_map_to_module_files() {
    let lib = Path::new("crates/x/src/lib.rs");
    assert_eq!(
        test_module_paths(lib, "v1_goldens"),
        [
            PathBuf::from("crates/x/src/v1_goldens.rs"),
            PathBuf::from("crates/x/src/v1_goldens"),
        ]
    );
    let events_mod = Path::new("crates/x/src/events/mod.rs");
    assert_eq!(
        test_module_paths(events_mod, "bus_tests")[0],
        PathBuf::from("crates/x/src/events/bus_tests.rs")
    );
    let plain = Path::new("crates/x/src/conflate.rs");
    assert_eq!(
        test_module_paths(plain, "fixtures"),
        [
            PathBuf::from("crates/x/src/conflate/fixtures.rs"),
            PathBuf::from("crates/x/src/conflate/fixtures"),
        ]
    );
    let test_paths: BTreeSet<PathBuf> = test_module_paths(lib, "v1_goldens").into_iter().collect();
    assert!(is_test_module_file(
        Path::new("crates/x/src/v1_goldens.rs"),
        &test_paths
    ));
    assert!(is_test_module_file(
        Path::new("crates/x/src/v1_goldens/nested.rs"),
        &test_paths
    ));
    assert!(!is_test_module_file(
        Path::new("crates/x/src/v1_goldens_extra.rs"),
        &test_paths
    ));
}

#[test]
fn opt_out_marker_with_a_reason_suppresses_the_literal() {
    let src = r#"
fn wake_metadata() -> serde_json::Value {
    json!({
        // event-type-lint: allow — message-metadata pseudo-type, not a bus event
        "eventTypes": ["agent:reportToParent"],
    })
}
"#;
    assert_eq!(hit_lines(src), Vec::<usize>::new());
}

#[test]
fn opt_out_marker_without_a_reason_still_fails() {
    for marker in [
        "// event-type-lint: allow",
        "// event-type-lint: allow —",
        "// event-type-lint: allow -",
        "// event-type-lint: allow reason without a dash",
        "// event-type-lint: allowance",
        "// event-type-lint: allow_me — reason",
    ] {
        let src = format!("fn f() -> &'static str {{\n    {marker}\n    \"task:bogus\"\n}}\n");
        let scanned = scan_source(&src);
        assert_eq!(scanned.hits.len(), 1, "{marker:?}: {:?}", scanned.hits);
        assert_eq!(scanned.hits[0].line, 3, "{marker:?}");
        assert!(scanned.hits[0].marker_malformed, "{marker:?}");
    }
}

#[test]
fn opt_out_marker_accepts_an_em_dash_or_a_hyphen() {
    for marker in [
        "// event-type-lint: allow — reason",
        "// event-type-lint: allow - reason",
        "// event-type-lint: allow   —   reason",
    ] {
        let src = format!("fn f() -> &'static str {{\n    {marker}\n    \"task:bogus\"\n}}\n");
        assert_eq!(hit_lines(&src), Vec::<usize>::new(), "{marker:?}");
    }
}

#[test]
fn opt_out_marker_must_sit_immediately_above_the_literal() {
    let src = "fn f() -> &'static str {\n    // event-type-lint: allow — reason\n\n    \"task:bogus\"\n}\n";
    let scanned = scan_source(src);
    assert_eq!(hit_lines(src), vec![4]);
    assert!(!scanned.hits[0].marker_malformed);
}

#[test]
fn opt_out_marker_counts_only_as_a_standalone_line_comment() {
    let in_block = "fn f() -> &'static str {\n    /*\n    // event-type-lint: allow — example */\n    \"task:bogus\"\n}\n";
    assert_eq!(hit_lines(in_block), vec![4]);
    let trailing = "fn f() -> &'static str {\n    let _n = 1; // event-type-lint: allow — example\n    \"task:bogus\"\n}\n";
    assert_eq!(hit_lines(trailing), vec![3]);
}

#[test]
fn only_the_catalog_is_exempt() {
    let exempt: PathBuf = EXEMPT_FILE.iter().collect();
    assert!(is_exempt(&exempt));
    let sibling: PathBuf = ["crates", "intent-core", "src", "model.rs"]
        .iter()
        .collect();
    assert!(!is_exempt(&sibling));
    assert_eq!(display_rel(&exempt), "crates/intent-core/src/events.rs");
}
