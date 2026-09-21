//! Agent-hidden-field egress lint.
//!
//! `intent_core::model::AGENT_HIDDEN_FIELDS` names session fields that must
//! never reach an agent's transcript, and the contract test in
//! `crates/intent-acp/src/tests_hidden_field_egress.rs` proves the scrub on
//! every egress its `EGRESS_REGISTRY` lists — but only on those. A new
//! `ws.<ns>.*` binding that serializes an `AgentLite` / `Event` row, or a new
//! services helper that copies `Event.data` into message metadata, would ship
//! unproven unless someone remembered the registry. This source-scanning test
//! fails, naming the file and the registry to edit, when:
//!
//! - a file under `crates/intent-acp/src/mcp_server/bindings/` reads session
//!   or event data (a `READER_TYPES` identifier, or a `READER_METHODS` call)
//!   and is neither a `SCRUBBED_BINDINGS` row nor a `HAND_PICKED_BINDINGS` row;
//! - a `SCRUBBED_BINDINGS` file does not call `strip_agent_hidden_fields`, or
//!   no `EGRESS_REGISTRY` entry starts with its registry-name prefix;
//! - a `fn` under `crates/intent-services/src/` copies a `.data` field
//!   wholesale (`e.data.clone()`, `"data": e.data,` … on an unborrowed
//!   receiver) into a `json!` value and is neither a `WAKE_METADATA_BUILDERS`
//!   row (registered, scrubbed) nor a `SAFE_DATA_COPIES` row (never reaches an
//!   agent; reason required), or a listed builder has no registry entry;
//! - an `EGRESS_REGISTRY` entry is claimed by no `SCRUBBED_BINDINGS` prefix
//!   and no `WAKE_METADATA_BUILDERS` name, or an allowlist row is stale (its
//!   file / fn is gone or no longer matches) — so the allowlists here and the
//!   registry cannot drift apart silently.
//!
//! The heuristic is deliberately small: comments and string literals are
//! blanked; items under a `#[cfg(…)]` naming `test` (not `not`) are skipped
//! from the attribute to the `}` closing the item, or to a `;` / `,` seen
//! first outside parens, brackets, and generics; modules declared
//! `#[cfg(test)] mod name;`, `tests.rs` / `*_tests.rs` files, and `tests/`
//! directories are skipped. Registry entry names are the first string literal
//! of each top-level `( … )` tuple in `EGRESS_REGISTRY`. The lint is
//! file-granular: it proves every binding file that serves session/event data
//! is scrubbed and has SOME registry coverage; the contract test remains the
//! per-entry proof, so a new `ws.<ns>.*` method in an already-scrubbed file
//! still needs its own registry entry. A binding that legitimately hand-picks
//! safe fields opts in with one `HAND_PICKED_BINDINGS` row carrying a reason;
//! a binding that serializes rows scrubs in `dispatch`, registers its
//! `ws.<ns>.*` egress entries, and adds a `SCRUBBED_BINDINGS` row.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

const REGISTRY_FILE: &str = "crates/intent-acp/src/tests_hidden_field_egress.rs";
const REGISTRY_CONST: &str = "EGRESS_REGISTRY";
const BINDINGS_DIR: &str = "crates/intent-acp/src/mcp_server/bindings";
const SERVICES_DIR: &str = "crates/intent-services/src";
const SCRUB_FN: &str = "strip_agent_hidden_fields";
const SELF_FILE: &str = "crates/intent-acp/tests/agent_hidden_field_egress_lint.rs";

/// Type identifiers whose presence means a binding handles session / event
/// rows.
const READER_TYPES: &[&str] = &["AgentLite", "AgentSession", "Event"];
/// `WorkspaceApi` methods (matched as `name(` calls) that serve session or
/// event data.
const READER_METHODS: &[&str] = &[
    "agent_list",
    "agent_list_active",
    "agent_list_including_retired",
    "agent_list_interrupted",
    "agent_list_retired_only",
    "agent_list_user_messages",
    "agent_get",
    "agent_get_conversation",
    "agent_get_message_block",
    "agent_get_queue",
    "agent_get_session",
    "agent_get_session_stats",
    "agent_diagnostics",
    "agent_snapshot",
    "agent_summary",
    "event_query",
    "event_agent_activity",
    "event_workspace_summary",
];

/// The allowlist, cross-checked against `EGRESS_REGISTRY` names.
struct Allowlist<'a> {
    /// Binding files whose `dispatch` scrubs every result: `(file, prefix)`
    /// where at least one registry entry name starts with `prefix`.
    scrubbed_bindings: &'a [(&'a str, &'a str)],
    /// Binding files that read session / event rows but hand-pick safe
    /// fields and never serialize a row: `(file, reason)`.
    hand_picked_bindings: &'a [(&'a str, &'a str)],
    /// `intent-services` fns that copy `Event.data` into message metadata:
    /// `(fn name, registry entry name)`.
    wake_metadata_builders: &'a [(&'a str, &'a str)],
    /// `intent-services` fns that copy a `.data` field into a `json!` value
    /// that never reaches an agent (persistence, FE-only shapes): `(fn name,
    /// reason)`.
    safe_data_copies: &'a [(&'a str, &'a str)],
}

const ALLOWLIST: Allowlist<'static> = Allowlist {
    scrubbed_bindings: &[
        ("crates/intent-acp/src/mcp_server/bindings/agent.rs", "ws.agent."),
        ("crates/intent-acp/src/mcp_server/bindings/event.rs", "ws.event."),
        (
            "crates/intent-acp/src/mcp_server/bindings/app/agents.rs",
            "ws.app.agents.",
        ),
    ],
    hand_picked_bindings: &[(
        "crates/intent-acp/src/mcp_server/bindings/workspace.rs",
        "archive() reads agent_list only to refuse while other agents run; the error names blockers by name/id and no row is returned",
    )],
    wake_metadata_builders: &[(
        "build_event_notification_metadata",
        "wake metadata (build_event_notification_metadata)",
    )],
    safe_data_copies: &[(
        "truncate_tool_call_for_persist",
        "caps an oversized agent:tool:call payload before the store write; the copy is persisted, not delivered",
    )],
};

// ---- lexing -----------------------------------------------------------------

fn push_blank(out: &mut String, c: char) {
    out.push(if c == '\n' { '\n' } else { ' ' });
}

/// `Some(hashes)` when a raw string literal (`r"`, `r#"`, `br"`, `cr#"`, …)
/// starts at `i`.
fn raw_string_hashes(chars: &[char], i: usize) -> Option<usize> {
    if i > 0 && (chars[i - 1].is_ascii_alphanumeric() || chars[i - 1] == '_') {
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

/// Replaces every comment, string literal, and char literal with spaces
/// (newlines preserved) so their contents never count as code.
fn blank_literals_and_comments(src: &str) -> String {
    let chars: Vec<char> = src.chars().collect();
    let mut out = String::with_capacity(src.len());
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        let next = chars.get(i + 1).copied();
        if c == '/' && next == Some('/') {
            while i < chars.len() && chars[i] != '\n' {
                out.push(' ');
                i += 1;
            }

            continue;
        }
        if c == '/' && next == Some('*') {
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
                    push_blank(&mut out, chars[i]);
                    i += 1;
                }
            }
            continue;
        }
        if let Some(hashes) = raw_string_hashes(&chars, i) {
            while chars[i] != '"' {
                out.push(' ');
                i += 1;
            }
            out.push(' ');
            i += 1;
            let closer: String = std::iter::once('"')
                .chain(std::iter::repeat_n('#', hashes))
                .collect();
            let closer: Vec<char> = closer.chars().collect();
            while i < chars.len() {
                if chars[i..].starts_with(&closer) {
                    for _ in 0..closer.len() {
                        out.push(' ');
                    }
                    i += closer.len();
                    break;
                }
                push_blank(&mut out, chars[i]);
                i += 1;
            }
            continue;
        }
        if c == '"' || (c == 'b' && next == Some('"')) {
            if c == 'b' {
                out.push(' ');
                i += 1;
            }
            out.push(' ');
            i += 1;
            while i < chars.len() && chars[i] != '"' {
                if chars[i] == '\\' {
                    out.push(' ');
                    i += 1;
                }
                if i < chars.len() {
                    push_blank(&mut out, chars[i]);
                    i += 1;
                }
            }
            if i < chars.len() {
                out.push(' ');
                i += 1;
            }
            continue;
        }
        if c == '\'' {
            // Char literal (`'a'`, `'\n'`, `'\u{1F600}'`) vs lifetime (`'a`).
            let close = if next == Some('\\') {
                chars[i + 2..]
                    .iter()
                    .position(|&ch| ch == '\'')
                    .map(|p| i + 2 + p)
            } else if chars.get(i + 2) == Some(&'\'') {
                Some(i + 2)
            } else {
                None
            };
            if let Some(close) = close {
                for _ in i..=close {
                    out.push(' ');
                }
                i = close + 1;
                continue;
            }
        }
        out.push(c);
        i += 1;
    }
    out
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
struct Token<'a> {
    text: &'a str,
    start: usize,
}

fn is_ident_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// Identifiers (with byte offsets) and single-char punctuation of blanked
/// source; whitespace is dropped.
fn tokenize(src: &str) -> Vec<Token<'_>> {
    let mut tokens = Vec::new();
    let mut iter = src.char_indices().peekable();
    while let Some((start, c)) = iter.next() {
        if c.is_whitespace() {
            continue;
        }
        if is_ident_char(c) {
            let mut end = start + c.len_utf8();
            while let Some(&(i, d)) = iter.peek() {
                if !is_ident_char(d) {
                    break;
                }
                end = i + d.len_utf8();
                iter.next();
            }
            tokens.push(Token {
                text: &src[start..end],
                start,
            });
        } else {
            tokens.push(Token {
                text: &src[start..start + c.len_utf8()],
                start,
            });
        }
    }
    tokens
}

/// Index of the token closing the brace/bracket/paren opened at `open`.
fn matching_close(tokens: &[Token<'_>], open: usize) -> Option<usize> {
    let (o, c) = match tokens[open].text {
        "{" => ("{", "}"),
        "[" => ("[", "]"),
        "(" => ("(", ")"),
        _ => return None,
    };
    let mut depth = 0usize;
    for (i, t) in tokens.iter().enumerate().skip(open) {
        if t.text == o {
            depth += 1;
        } else if t.text == c {
            depth -= 1;
            if depth == 0 {
                return Some(i);
            }
        }
    }
    None
}

/// Index just past a `#[cfg(…)]` attribute starting at `i` whose predicate
/// names `test` and not `not` (`#[cfg(test)]`, `#[cfg(all(test, unix))]`).
fn cfg_test_attr_end(tokens: &[Token<'_>], i: usize) -> Option<usize> {
    let head = ["#", "[", "cfg", "("];
    let matches = tokens[i..]
        .iter()
        .zip(head)
        .filter(|(t, want)| t.text == *want)
        .count();
    if matches != head.len() {
        return None;
    }
    let open = i + head.len() - 1;
    let close = matching_close(tokens, open)?;
    let pred: Vec<&str> = tokens[open + 1..close].iter().map(|t| t.text).collect();
    if !pred.contains(&"test") || pred.contains(&"not") {
        return None;
    }
    (tokens.get(close + 1)?.text == "]").then_some(close + 2)
}

/// Token ranges `[start, end)` covered by `#[cfg(test)]` items: the attribute
/// through the `}` closing the item's first brace, or through a `;` / `,`
/// seen first outside parens, brackets, and generics (a `mod x;` / `use`
/// declaration, a struct field, a struct-literal field, a match arm).
fn cfg_test_ranges(tokens: &[Token<'_>]) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    let mut i = 0;
    while i < tokens.len() {
        let Some(after_attr) = cfg_test_attr_end(tokens, i) else {
            i += 1;
            continue;
        };
        let mut j = after_attr;
        let mut end = tokens.len();
        let mut angle = 0usize;
        while j < tokens.len() {
            match tokens[j].text {
                ";" | "," if angle == 0 => {
                    end = j + 1;
                    break;
                }
                "{" => {
                    end = matching_close(tokens, j).map_or(tokens.len(), |c| c + 1);
                    break;
                }
                "(" | "[" => {
                    j = matching_close(tokens, j).map_or(tokens.len(), |c| c + 1);
                }
                "<" => {
                    angle += 1;
                    j += 1;
                }
                ">" if j > 0 && tokens[j - 1].text != "-" => {
                    angle = angle.saturating_sub(1);
                    j += 1;
                }
                _ => j += 1,
            }
        }
        ranges.push((i, end));
        i = end.max(i + 1);
    }
    ranges
}

/// Tokens outside every `#[cfg(test)]` range.
fn non_test_tokens<'a>(tokens: &[Token<'a>]) -> Vec<Token<'a>> {
    let ranges = cfg_test_ranges(tokens);
    tokens
        .iter()
        .enumerate()
        .filter(|(i, _)| !ranges.iter().any(|(s, e)| (*s..*e).contains(i)))
        .map(|(_, t)| *t)
        .collect()
}

/// Module names declared `#[cfg(test)] mod name;` (their files are test code).
fn cfg_test_module_decls(tokens: &[Token<'_>]) -> Vec<String> {
    cfg_test_ranges(tokens)
        .into_iter()
        .filter_map(|(s, e)| {
            let body = &tokens[cfg_test_attr_end(tokens, s)?..e];
            let mut it = body.iter().map(|t| t.text);
            // Skip `pub`, `pub(crate)`, ….
            let mut kw = it.next()?;
            while kw != "mod" {
                kw = it.next()?;
            }
            let name = it.next()?;
            (it.next()? == ";").then(|| name.to_string())
        })
        .collect()
}

fn line_of(src: &str, byte: usize) -> usize {
    src[..byte].matches('\n').count() + 1
}

// ---- readers ----------------------------------------------------------------

/// First `(line, what)` where non-test code names a `READER_TYPES` identifier
/// or calls a `READER_METHODS` method.
fn first_reader(src: &str, tokens: &[Token<'_>]) -> Option<(usize, String)> {
    tokens.iter().enumerate().find_map(|(i, t)| {
        if READER_TYPES.contains(&t.text) {
            return Some((line_of(src, t.start), format!("type `{}`", t.text)));
        }
        let is_call = tokens.get(i + 1).is_some_and(|n| n.text == "(");
        let is_method = i > 0 && tokens[i - 1].text == ".";
        (is_call && is_method && READER_METHODS.contains(&t.text))
            .then(|| (line_of(src, t.start), format!("call `.{}(`", t.text)))
    })
}

/// `(fn name, fn line, body tokens)` for every `fn` with a body.
fn functions<'a>(src: &str, tokens: &'a [Token<'a>]) -> Vec<(&'a str, usize, &'a [Token<'a>])> {
    let mut out = Vec::new();
    let mut i = 0;
    while i + 1 < tokens.len() {
        if tokens[i].text != "fn" || !tokens[i + 1].text.starts_with(is_ident_char) {
            i += 1;
            continue;
        }
        let name = tokens[i + 1].text;
        let line = line_of(src, tokens[i].start);
        let mut j = i + 2;
        let mut body = None;
        while j < tokens.len() {
            match tokens[j].text {
                ";" => break,
                "{" => {
                    let close = matching_close(tokens, j).unwrap_or(tokens.len() - 1);
                    body = Some(&tokens[j..=close]);
                    break;
                }
                // Parameter lists / array types may carry `;` (`[u8; 32]`).
                "(" | "[" => j = matching_close(tokens, j).map_or(tokens.len(), |c| c + 1),
                _ => j += 1,
            }
        }
        if let Some(body) = body {
            out.push((name, line, body));
        }
        i += 2;
    }
    out
}

/// Whether the receiver chain ending at the `.` token `dot` (`a.b.c` before
/// `.data`) is borrowed (`&a.b.c.data`, `&mut a.data`).
fn receiver_is_borrowed(body: &[Token<'_>], dot: usize) -> bool {
    let mut k = dot;
    while k >= 1 && body[k].text == "." && body[k - 1].text.starts_with(is_ident_char) {
        k -= 1;
        if k == 0 {
            return false;
        }
        if body[k - 1].text != "." {
            break;
        }
        k -= 1;
    }
    let mut prev = k.checked_sub(1);
    if prev.is_some_and(|p| body[p].text == "mut") {
        prev = prev.and_then(|p| p.checked_sub(1));
    }
    prev.is_some_and(|p| body[p].text == "&")
}

/// Byte offset of the first wholesale `.data` copy — `.data` followed by
/// `,` / `)` / `;` / `}`, or `.data.clone()`, on an unborrowed receiver — in
/// a fn body that also builds a `json!` value.
fn copies_data_into_json(body: &[Token<'_>]) -> Option<usize> {
    let has_json = body
        .windows(2)
        .any(|w| w[0].text == "json" && w[1].text == "!");
    if !has_json {
        return None;
    }
    body.windows(2).enumerate().find_map(|(i, w)| {
        if w[0].text != "." || w[1].text != "data" || receiver_is_borrowed(body, i) {
            return None;
        }
        let wholesale = match body.get(i + 2).map(|t| t.text) {
            Some("," | ")" | ";" | "}") => true,
            Some(".") => {
                body.get(i + 3).is_some_and(|t| t.text == "clone")
                    && body.get(i + 4).is_some_and(|t| t.text == "(")
            }
            _ => false,
        };
        wholesale.then_some(w[1].start)
    })
}

// ---- registry ---------------------------------------------------------------

/// Entry names of `const EGRESS_REGISTRY: &[(&str, Producer)] = &[ … ];` —
/// the first string literal of each top-level `( … )` tuple in the array.
/// Brackets are depth-tracked and `"…"` literals / `//` comments skipped, so
/// the JS snippets inside producers never count as structure.
fn registry_names(root: &Path) -> Result<Vec<String>, String> {
    let path = root.join(REGISTRY_FILE);
    let src = fs::read_to_string(&path).map_err(|e| format!("{}: {e}", path.display()))?;
    let decl = format!("const {REGISTRY_CONST}");
    let start = src
        .find(&decl)
        .ok_or_else(|| format!("{REGISTRY_FILE}: no `{decl}`"))?;
    let open = src[start..]
        .find("= &[")
        .map(|p| start + p + 3)
        .ok_or_else(|| format!("{REGISTRY_FILE}: `{REGISTRY_CONST}` is not a `&[…]`"))?;
    let chars: Vec<char> = src[open..].chars().collect();
    let mut names = Vec::new();
    let mut depth = 0usize;
    let mut want_name = false;
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            '/' if chars.get(i + 1) == Some(&'/') => {
                while i < chars.len() && chars[i] != '\n' {
                    i += 1;
                }
                continue;
            }
            '"' => {
                let mut j = i + 1;
                let mut lit = String::new();
                while j < chars.len() && chars[j] != '"' {
                    if chars[j] == '\\' {
                        j += 1;
                    }
                    if let Some(&ch) = chars.get(j) {
                        lit.push(ch);
                    }
                    j += 1;
                }
                if want_name {
                    names.push(lit);
                    want_name = false;
                }
                i = j + 1;
                continue;
            }
            '(' | '[' | '{' => {
                if depth == 1 && c == '(' {
                    want_name = true;
                }
                depth += 1;
            }
            ')' | ']' | '}' => {
                depth = depth.saturating_sub(1);
                if depth == 0 {
                    break;
                }
            }
            _ => {}
        }
        i += 1;
    }
    if depth != 0 {
        return Err(format!("{REGISTRY_FILE}: unterminated `{REGISTRY_CONST}`"));
    }
    if names.is_empty() {
        return Err(format!(
            "{REGISTRY_FILE}: `{REGISTRY_CONST}` has no entries"
        ));
    }
    Ok(names)
}

// ---- scan -------------------------------------------------------------------

fn rel(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn is_test_path(rel: &str) -> bool {
    rel.split('/').any(|seg| seg == "tests")
        || rel.ends_with("/tests.rs")
        || rel.ends_with("_tests.rs")
        || rel.ends_with("_test.rs")
}

fn rust_files(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        let Ok(entries) = fs::read_dir(&d) else {
            continue;
        };
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().is_some_and(|e| e == "rs") {
                out.push(p);
            }
        }
    }
    out.sort();
    out
}

struct SourceFile {
    rel: String,
    /// Blanked source, leaked so tokens can borrow it for the test's lifetime.
    src: &'static str,
    tokens: Vec<Token<'static>>,
}

/// Non-test `.rs` files under `dir`, each with its non-test tokens. Files
/// named by a sibling's `#[cfg(test)] mod name;` are dropped.
fn scan_dir(root: &Path, dir: &str) -> Vec<SourceFile> {
    let files = rust_files(&root.join(dir));
    let mut loaded: Vec<(PathBuf, String)> = files
        .into_iter()
        .filter_map(|p| fs::read_to_string(&p).ok().map(|s| (p, s)))
        .collect();
    let mut test_mods: BTreeSet<PathBuf> = BTreeSet::new();
    for (path, src) in &loaded {
        let blanked = blank_literals_and_comments(src);
        let tokens = tokenize(&blanked);
        let parent = path.parent().unwrap_or(Path::new(""));
        let stem = path.file_stem().map(|s| s.to_string_lossy().to_string());
        let mod_dir = match stem.as_deref() {
            Some("mod" | "lib" | "main") | None => parent.to_path_buf(),
            Some(stem) => parent.join(stem),
        };
        for name in cfg_test_module_decls(&tokens) {
            test_mods.insert(mod_dir.join(format!("{name}.rs")));
            test_mods.insert(mod_dir.join(&name).join("mod.rs"));
        }
    }
    loaded.retain(|(p, _)| !test_mods.contains(p) && !is_test_path(&rel(root, p)));
    loaded
        .into_iter()
        .map(|(path, src)| {
            let blanked: &'static str = Box::leak(blank_literals_and_comments(&src).into());
            let tokens = non_test_tokens(&tokenize(blanked));
            SourceFile {
                rel: rel(root, &path),
                src: blanked,
                tokens,
            }
        })
        .collect()
}

fn check(root: &Path, allow: &Allowlist<'_>) -> Vec<String> {
    let mut failures = Vec::new();
    let names = match registry_names(root) {
        Ok(n) => n,
        Err(e) => return vec![e],
    };

    for name in &names {
        let by_binding = allow
            .scrubbed_bindings
            .iter()
            .any(|(_, prefix)| name.starts_with(prefix));
        let by_builder = allow
            .wake_metadata_builders
            .iter()
            .any(|(_, entry)| entry == name);
        if !by_binding && !by_builder {
            failures.push(format!(
                "{REGISTRY_FILE}: `{REGISTRY_CONST}` entry `{name}` is claimed by no \
                 `SCRUBBED_BINDINGS` prefix and no `WAKE_METADATA_BUILDERS` row in \
                 {SELF_FILE} — add the row so the lint knows which file proves it"
            ));
        }
    }
    for (file, prefix) in allow.scrubbed_bindings {
        if !names.iter().any(|n| n.starts_with(prefix)) {
            failures.push(format!(
                "{file}: `SCRUBBED_BINDINGS` prefix `{prefix}` matches no `{REGISTRY_CONST}` \
                 entry in {REGISTRY_FILE} — register the binding's egress or drop the row"
            ));
        }
    }

    let bindings = scan_dir(root, BINDINGS_DIR);
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    for file in &bindings {
        let reader = first_reader(file.src, &file.tokens);
        let scrubbed = allow.scrubbed_bindings.iter().find(|(f, _)| *f == file.rel);
        let hand_picked = allow
            .hand_picked_bindings
            .iter()
            .find(|(f, _)| *f == file.rel);
        if scrubbed.is_some() || hand_picked.is_some() {
            seen.insert(file.rel.as_str());
        }
        match (reader, scrubbed, hand_picked) {
            (None, None, None) | (Some(_), None, Some(_)) => {}
            (None, Some(_), _) | (None, None, Some(_)) => failures.push(format!(
                "{}: allowlisted in {SELF_FILE} but no longer reads session/event data \
                 (no `READER_TYPES` identifier or `READER_METHODS` call) — drop the row",
                file.rel
            )),
            (Some((line, what)), None, None) => failures.push(format!(
                "{}:{line}: {what} reads session/event data but the file is not registered: \
                 scrub every result with `{SCRUB_FN}` in `dispatch`, add its `ws.<ns>.*` \
                 entries to `{REGISTRY_CONST}` in {REGISTRY_FILE}, and add a \
                 `SCRUBBED_BINDINGS` row in {SELF_FILE} (or a `HAND_PICKED_BINDINGS` row \
                 with a reason when no row is ever serialized)",
                file.rel
            )),
            (Some(_), Some(_), _) => {
                let scrubs = file
                    .tokens
                    .windows(2)
                    .any(|w| w[0].text == SCRUB_FN && w[1].text == "(");
                if !scrubs {
                    failures.push(format!(
                        "{}: `SCRUBBED_BINDINGS` row but the file never calls `{SCRUB_FN}(` \
                         — scrub in `dispatch` or move the row to `HAND_PICKED_BINDINGS`",
                        file.rel
                    ));
                }
            }
        }
    }
    for (file, _) in allow
        .scrubbed_bindings
        .iter()
        .chain(allow.hand_picked_bindings)
    {
        if !seen.contains(file) {
            failures.push(format!(
                "{file}: allowlisted in {SELF_FILE} but not found under {BINDINGS_DIR} — \
                 drop or rename the row"
            ));
        }
    }

    let services = scan_dir(root, SERVICES_DIR);
    let mut found_fns: BTreeSet<&str> = BTreeSet::new();
    for file in &services {
        for (name, line, body) in functions(file.src, &file.tokens) {
            let Some(copy_at) = copies_data_into_json(body) else {
                continue;
            };
            let copy_line = line_of(file.src, copy_at);
            let builder = allow
                .wake_metadata_builders
                .iter()
                .find(|(f, _)| *f == name);
            let safe = allow.safe_data_copies.iter().find(|(f, _)| *f == name);
            match builder.or(safe) {
                Some((f, _)) => {
                    found_fns.insert(f);
                }
                None => failures.push(format!(
                    "{}:{copy_line}: `fn {name}` (line {line}) copies a `.data` field \
                     wholesale into a `json!` value; if that data can carry session rows or \
                     event payloads reaching an agent, scrub it with `{SCRUB_FN}`, add an \
                     `{REGISTRY_CONST}` entry in {REGISTRY_FILE}, and add a \
                     `WAKE_METADATA_BUILDERS` row in {SELF_FILE}; if the copy never reaches \
                     an agent, add a `SAFE_DATA_COPIES` row with the reason",
                    file.rel
                )),
            }
        }
    }
    for (name, _) in allow
        .wake_metadata_builders
        .iter()
        .chain(allow.safe_data_copies)
    {
        if !found_fns.contains(name) {
            failures.push(format!(
                "{SERVICES_DIR}: `WAKE_METADATA_BUILDERS` / `SAFE_DATA_COPIES` row `{name}` \
                 matches no fn that copies `.data` into `json!` — drop or rename the row"
            ));
        }
    }
    for (name, entry) in allow.wake_metadata_builders {
        if !names.contains(&entry.to_string()) {
            failures.push(format!(
                "{REGISTRY_FILE}: `WAKE_METADATA_BUILDERS` row `{name}` names registry entry \
                 `{entry}` which `{REGISTRY_CONST}` does not contain"
            ));
        }
    }

    failures.sort();
    failures.dedup();
    failures
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/intent-acp has a workspace root")
        .to_path_buf()
}

#[test]
fn agent_hidden_field_egress_is_registered() {
    let failures = check(&workspace_root(), &ALLOWLIST);
    assert!(
        failures.is_empty(),
        "{} agent-hidden-field egress lint failure(s):\n\n{}\n\nSee the module doc of \
         {SELF_FILE} for the heuristic.",
        failures.len(),
        failures.join("\n\n")
    );
}

#[cfg(test)]
mod fixture {
    use super::*;

    const SCRATCH_BINDING: &str = "crates/intent-acp/src/mcp_server/bindings/scratch.rs";

    fn write(root: &Path, rel: &str, content: &str) {
        let path = root.join(rel);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, content).unwrap();
    }

    fn fixture() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(
            root,
            REGISTRY_FILE,
            r#"
            type Producer = fn(&Harness) -> Value;
            // ("ws.commented.out", |h| h.nothing()),
            const EGRESS_REGISTRY: &[(&str, Producer)] = &[
                ("ws.agent.list", |h| {
                    Box::pin(Harness::mcp(&h.srv, "ws.agent.list", "return await ws.agent.list([{(\"x\")}]);"))
                }),
                ("ws.agent.status", |h| Box::pin(async { json!({ "(": "[" }) })),
                ("wake metadata (build_meta)", |_h| Box::pin(async { build_meta() })),
            ];
            "#,
        );
        write(
            root,
            "crates/intent-acp/src/mcp_server/bindings/agent.rs",
            r"
            pub async fn dispatch(api: &Api) -> Value {
                let rows: Vec<AgentLite> = api.agent_list(ws).await?;
                strip_agent_hidden_fields(&mut out);
                out
            }
            #[cfg(test)]
            mod tests {
                fn unscrubbed() { api.agent_get(ws); }
            }
            ",
        );
        write(
            root,
            "crates/intent-acp/src/mcp_server/bindings/workspace.rs",
            "async fn archive(api: &Api) { let a = api.agent_list(ws).await; a.len() }\n",
        );
        write(
            root,
            "crates/intent-acp/src/mcp_server/bindings/mod.rs",
            "pub fn route(ns: &str) -> &str { \"agent\" }\n",
        );
        write(
            root,
            "crates/intent-services/src/lib.rs",
            r#"
            #[cfg(test)]
            mod scratch_tests;
            fn build_meta(e: &Event) -> Value {
                let data = e.data.clone();
                json!({ "data": data })
            }
            fn other(v: &Thing) -> Value { json!({ "x": v.x }) }
            fn persist(ev: &NewEvent, buf: [u8; 4]) -> NewEvent {
                let mut data = ev.data.clone();
                json!({ "truncated": true })
            }
            fn borrowed(e: &Event) -> Value {
                let report = completion_report(&e.data);
                let flag = has_flag(&mut e.data);
                json!({ "report": report, "flag": flag })
            }
            struct Ctx {
                #[cfg(test)]
                fault: Option<u32>,
                real: u32,
            }
            fn ctx() -> Ctx {
                Ctx {
                    #[cfg(test)]
                    fault: None,
                    real: 1,
                }
            }
            "#,
        );
        write(
            root,
            "crates/intent-services/src/scratch_tests.rs",
            "fn t(e: &Event) -> Value { json!({ \"data\": e.data }) }\n",
        );
        dir
    }

    const FIXTURE_ALLOW: Allowlist<'static> = Allowlist {
        scrubbed_bindings: &[(
            "crates/intent-acp/src/mcp_server/bindings/agent.rs",
            "ws.agent.",
        )],
        hand_picked_bindings: &[(
            "crates/intent-acp/src/mcp_server/bindings/workspace.rs",
            "counts rows only",
        )],
        wake_metadata_builders: &[("build_meta", "wake metadata (build_meta)")],
        safe_data_copies: &[("persist", "store-side cap, never delivered")],
    };

    #[test]
    fn baseline_fixture_passes() {
        let dir = fixture();
        assert_eq!(check(dir.path(), &FIXTURE_ALLOW), Vec::<String>::new());
    }

    #[test]
    fn unregistered_binding_reader_fails() {
        let dir = fixture();
        write(
            dir.path(),
            SCRATCH_BINDING,
            "async fn get(api: &Api) -> Value { json!(api.agent_get(ws).await?) }\n",
        );
        let failures = check(dir.path(), &FIXTURE_ALLOW);
        assert_eq!(failures.len(), 1, "{failures:?}");
        assert!(failures[0].starts_with(&format!("{SCRATCH_BINDING}:1: call `.agent_get(`")));
        assert!(failures[0].contains("SCRUBBED_BINDINGS"), "{}", failures[0]);
    }

    #[test]
    fn unregistered_reader_type_fails() {
        let dir = fixture();
        write(
            dir.path(),
            SCRATCH_BINDING,
            "fn project(rows: &[AgentLite]) -> Value { json!(rows) }\n",
        );
        let failures = check(dir.path(), &FIXTURE_ALLOW);
        assert_eq!(failures.len(), 1, "{failures:?}");
        assert!(failures[0].starts_with(&format!("{SCRATCH_BINDING}:1: type `AgentLite`")));
    }

    #[test]
    fn reader_only_in_comments_strings_or_cfg_test_passes() {
        let dir = fixture();
        write(
            dir.path(),
            SCRATCH_BINDING,
            r#"
            // api.agent_get(ws) is fine in a comment
            const HELP: &str = "AgentLite rows come from api.agent_list(";
            /* AgentSession */
            #[cfg(test)]
            mod tests { fn t() { let _: AgentLite = api.agent_get(ws); } }
            #[cfg(test)]
            fn helper() -> Vec<Event> { api.event_query(ws) }
            #[cfg(all(test, unix))]
            mod unix_tests { fn t(rows: &[AgentSession]) {} }
            #[cfg(not(test))]
            fn prod() -> u32 { 0 }
            "#,
        );
        assert_eq!(check(dir.path(), &FIXTURE_ALLOW), Vec::<String>::new());
    }

    #[test]
    fn scrubbed_binding_without_scrub_call_fails() {
        let dir = fixture();
        write(
            dir.path(),
            "crates/intent-acp/src/mcp_server/bindings/agent.rs",
            "pub async fn dispatch(api: &Api) -> Value { json!(api.agent_list(ws).await?) }\n",
        );
        let failures = check(dir.path(), &FIXTURE_ALLOW);
        assert_eq!(failures.len(), 1, "{failures:?}");
        assert!(failures[0].contains("never calls `strip_agent_hidden_fields(`"));
    }

    #[test]
    fn unclaimed_registry_entry_fails() {
        let dir = fixture();
        write(
            dir.path(),
            REGISTRY_FILE,
            r#"
            const EGRESS_REGISTRY: &[(&str, Producer)] = &[
                ("ws.agent.list", |h| Box::pin(h.list())),
                ("ws.scratch.get", |h| Box::pin(h.get())),
                ("wake metadata (build_meta)", |_h| Box::pin(async { build_meta() })),
            ];
            "#,
        );
        let failures = check(dir.path(), &FIXTURE_ALLOW);
        assert_eq!(failures.len(), 1, "{failures:?}");
        assert!(failures[0].contains("entry `ws.scratch.get` is claimed by no"));
    }

    #[test]
    fn stale_allowlist_rows_fail() {
        let dir = fixture();
        write(
            dir.path(),
            "crates/intent-acp/src/mcp_server/bindings/workspace.rs",
            "fn archive() -> u32 { 0 }\n",
        );
        fs::remove_file(
            dir.path()
                .join("crates/intent-acp/src/mcp_server/bindings/agent.rs"),
        )
        .unwrap();
        let failures = check(dir.path(), &FIXTURE_ALLOW);
        assert_eq!(failures.len(), 2, "{failures:?}");
        assert!(failures
            .iter()
            .any(|f| f.contains("agent.rs: allowlisted") && f.contains("not found")));
        assert!(failures
            .iter()
            .any(|f| f.contains("workspace.rs: allowlisted") && f.contains("no longer reads")));
    }

    #[test]
    fn unregistered_wake_metadata_builder_fails() {
        let dir = fixture();
        write(
            dir.path(),
            "crates/intent-services/src/wake.rs",
            r#"
            fn relay(e: &Event) -> Value {
                json!({ "type": e.event_type, "data": e.data, })
            }
            fn safe(e: &Event) -> Value {
                json!({ "type": e.event_type, "id": e.data.get("id") })
            }
            "#,
        );
        let failures = check(dir.path(), &FIXTURE_ALLOW);
        assert_eq!(failures.len(), 1, "{failures:?}");
        assert!(
            failures[0].starts_with("crates/intent-services/src/wake.rs:3: `fn relay` (line 2)"),
            "{}",
            failures[0]
        );
        assert!(failures[0].contains("WAKE_METADATA_BUILDERS"));
    }

    #[test]
    fn stale_or_unregistered_wake_metadata_row_fails() {
        let dir = fixture();
        let allow = Allowlist {
            wake_metadata_builders: &[
                ("build_meta", "wake metadata (build_meta)"),
                ("ghost", "wake metadata (ghost)"),
            ],
            ..FIXTURE_ALLOW
        };
        let failures = check(dir.path(), &allow);
        assert_eq!(failures.len(), 2, "{failures:?}");
        assert!(failures
            .iter()
            .any(|f| f.contains("row `ghost` matches no fn")));
        assert!(failures
            .iter()
            .any(|f| f.contains("row `ghost` names registry entry")));
    }

    #[test]
    fn stale_safe_data_copy_row_fails() {
        let dir = fixture();
        let allow = Allowlist {
            safe_data_copies: &[
                ("persist", "store-side cap, never delivered"),
                ("gone", "used to exist"),
            ],
            ..FIXTURE_ALLOW
        };
        let failures = check(dir.path(), &allow);
        assert_eq!(failures.len(), 1, "{failures:?}");
        assert!(
            failures[0].contains("row `gone` matches no fn"),
            "{}",
            failures[0]
        );
    }
}
