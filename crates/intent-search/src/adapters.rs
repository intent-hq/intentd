//! Store-backed search adapters (`search.messages`/`events`/`memories`/`notes`/
//! `codebase`, §5.15 / §14). This module owns the wire match shapes plus the
//! pure, transport-free matching helpers (case-insensitive substring matching,
//! preview windowing, and lightweight symbol heuristics). The store reads and
//! the `search:result`/`search:done` streaming live in the services layer.

use serde::Serialize;

/// `search.messages` hit: the owning agent + message ids, a preview snippet, and
/// an optional relevance score.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageMatch {
    pub agent_id: String,
    pub message_id: String,
    pub preview: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub score: Option<f64>,
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

/// `search.memories` hit: the memory id, a preview snippet, and an optional
/// score, built over the BE memories store (§9.2, PROTOCOL §5.15).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MemoryMatch {
    pub memory_id: String,
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
