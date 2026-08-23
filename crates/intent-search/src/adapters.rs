//! Store-backed search adapters (`search.messages`/`events`/`notes`/
//! `codebase`, §5.15 / §14). This module owns the wire match shapes plus the
//! pure, transport-free matching helpers (case-insensitive substring matching,
//! preview windowing, and lightweight symbol heuristics). The store reads and
//! the `search:result`/`search:done` streaming live in the services layer.

use serde::Serialize;

/// `search.messages` hit: the owning agent + message ids, a preview snippet,
/// an optional relevance score, plus the context fields a global (cross-
/// workspace) result row needs to render — the owning workspace, the agent's
/// display name, the message role, and its timestamp. The trailing fields are
/// additive: existing callers reading `agentId`/`messageId`/`preview`/`score`
/// are unaffected.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageMatch {
    pub agent_id: String,
    pub message_id: String,
    pub preview: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<f64>,
    pub workspace_id: String,
    pub agent_name: String,
    pub role: String,
    pub timestamp: String,
}

/// `search.events` hit: the event id, a preview snippet, and an optional score.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EventMatch {
    pub event_id: String,
    pub preview: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<f64>,
}

/// `search.notes` hit: the note id, a preview snippet, and an optional score.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NoteMatch {
    pub note_id: String,
    pub preview: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<f64>,
}

/// `search.codebase` hit: the workspace-relative file, an optional detected
/// symbol, the 1-based line, a preview snippet, and a relevance score.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CodebaseMatch {
    pub file: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub line: Option<u64>,
    pub preview: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<f64>,
}

/// Case-insensitive substring test (the renderer's literal-search default).
#[must_use]
pub fn contains_ci(haystack: &str, query: &str) -> bool {
    if query.is_empty() {
        return false;
    }
    haystack.to_lowercase().contains(&query.to_lowercase())
}

/// Maximum preview length (chars) before trailing content is elided.
const PREVIEW_MAX: usize = 160;

/// Build a single-line preview snippet centered on the first case-insensitive
/// match of `query` in `text`. Newlines/tabs collapse to spaces; long previews
/// are windowed with leading/trailing ellipses.
#[must_use]
pub fn make_preview(text: &str, query: &str) -> String {
    let flat: String = text
        .chars()
        .map(|c| if c.is_whitespace() { ' ' } else { c })
        .collect();
    let collapsed = collapse_spaces(&flat);
    let chars: Vec<char> = collapsed.chars().collect();
    let lower = collapsed.to_lowercase();
    let start_char = match lower.find(&query.to_lowercase()) {
        Some(byte_idx) => lower[..byte_idx].chars().count(),
        None => 0,
    };
    let window_start = start_char.saturating_sub(20);
    let window_end = (window_start + PREVIEW_MAX).min(chars.len());
    let mut out = String::new();
    if window_start > 0 {
        out.push('…');
    }
    out.extend(&chars[window_start..window_end]);
    if window_end < chars.len() {
        out.push('…');
    }
    out.trim().to_string()
}

/// Build a safe FTS5 MATCH expression from a raw user-typed query, or `None`
/// when the query has no searchable tokens. User input is never passed to the
/// FTS5 query parser verbatim — operators, quotes, and punctuation would
/// surface as `fts5: syntax error` (§9 Internal) on every as-you-type
/// keystroke. Instead the query is reduced to its alphanumeric tokens (the
/// `unicode61` tokenizer's separator model), each emitted as a quoted phrase
/// token joined with `AND`. The final token — presumed mid-typing — also
/// matches as a prefix (`("tok" OR "tok"*)`): the plain branch is porter-
/// stemmed like the index, while the starred branch catches partial words the
/// stemmer would miss.
#[must_use]
pub fn fts_match_expr(raw: &str) -> Option<String> {
    let tokens: Vec<&str> = raw
        .split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .collect();
    let (last, init) = tokens.split_last()?;
    let mut parts: Vec<String> = init.iter().map(|t| format!("\"{t}\"")).collect();
    parts.push(format!("(\"{last}\" OR \"{last}\"*)"));
    Some(parts.join(" AND "))
}

/// Build a preview snippet for an FTS hit: window around the first raw-query
/// token that occurs literally (case-insensitively) in `text`, falling back to
/// the head of the text when no token appears verbatim (porter stemming can
/// match without a literal occurrence).
#[must_use]
pub fn fts_preview(text: &str, raw_query: &str) -> String {
    let term = raw_query
        .split(|c: char| !c.is_alphanumeric())
        .filter(|t| !t.is_empty())
        .find(|t| contains_ci(text, t))
        .unwrap_or("");
    make_preview(text, term)
}

/// Collapse runs of spaces into a single space.
fn collapse_spaces(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut prev_space = false;
    for c in s.chars() {
        if c == ' ' {
            if !prev_space {
                out.push(' ');
            }
            prev_space = true;
        } else {
            out.push(c);
            prev_space = false;
        }
    }
    out
}

/// Definition keywords whose following identifier is treated as a symbol name.
const SYMBOL_KEYWORDS: &[&str] = &[
    "fn",
    "struct",
    "enum",
    "trait",
    "impl",
    "type",
    "const",
    "static",
    "mod",
    "class",
    "def",
    "function",
    "interface",
    "module",
];

/// Lightweight symbol heuristic: if `line` looks like a definition (`fn foo`,
/// `struct Bar`, `class Baz`, `def qux`, …), return the declared identifier.
#[must_use]
pub fn extract_symbol(line: &str) -> Option<String> {
    let tokens: Vec<&str> = line
        .split(|c: char| c.is_whitespace() || c == '(')
        .collect();
    for (i, tok) in tokens.iter().enumerate() {
        if SYMBOL_KEYWORDS.contains(tok) {
            if let Some(next) = tokens.get(i + 1) {
                let name: String = next
                    .chars()
                    .take_while(|c| c.is_alphanumeric() || *c == '_')
                    .collect();
                if !name.is_empty() {
                    return Some(name);
                }
            }
        }
    }
    None
}
