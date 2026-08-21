//! Pure content-edit helpers ported from the TS `ws.note.*` peers
//! (`src/features/mcp/main/mcp/ws-note-api.ts`). These mirror the byte-for-byte
//! string semantics the iOS app depends on: `add` positions, first exact-match
//! `edit`, 1-based inclusive `editLines`, the `setContent` cleaner, checkbox
//! task parsing, and asset-id parsing. User-facing failures surface as
//! [`Error::Internal`] so the router maps them to `-32603` with the original
//! message in `data`, matching the TS handler — except comment anchoring
//! failures, which are [`Error::InvalidParams`] (`-32602`) so clients see the
//! actionable message directly.

use intent_core::{Error, NoteTaskRow, Result};
use std::fmt::Write as _;

/// JS `\s` (ASCII subset): space, tab, the line terminators, FF and VT.
fn is_js_space_char(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\n' | '\r' | '\u{000b}' | '\u{000c}')
}

fn truncate_chars(s: &str, n: usize) -> String {
    s.chars().take(n).collect()
}

/// First `\n(#{1,6}\s)` after the cursor; returns the byte index of the `\n`.
fn find_next_heading(s: &str) -> Option<usize> {
    let bytes = s.as_bytes();
    for (i, &b) in bytes.iter().enumerate() {
        if b != b'\n' {
            continue;
        }
        let mut j = i + 1;
        let mut hashes = 0;
        while j < bytes.len() && bytes[j] == b'#' && hashes < 6 {
            hashes += 1;
            j += 1;
        }
        if hashes >= 1 && j < bytes.len() && is_js_space_char(bytes[j] as char) {
            return Some(i);
        }
    }
    None
}

/// `note.add` — returns `(new_content, position_info)`.
pub(crate) fn apply_add(
    old: &str,
    content: &str,
    heading: Option<&str>,
    position: Option<&str>,
) -> Result<(String, String)> {
    let add_section = match heading {
        Some(h) => format!("{h}\n\n{content}"),
        None => content.to_string(),
    };
    let pos = position.unwrap_or("");
    if pos.is_empty() || pos == "end" {
        Ok((format!("{old}\n\n{add_section}"), "at end".to_string()))
    } else if pos == "start" {
        Ok((format!("{add_section}\n\n{old}"), "at start".to_string()))
    } else if let Some(after_raw) = pos.strip_prefix("after:") {
        let after_heading = after_raw.trim();
        let heading_index = old.find(after_heading).ok_or_else(|| {
            Error::Internal(format!(
                "Heading not found: \"{after_heading}\". Use position=\"end\" or specify an existing heading."
            ))
        })?;
        let line_end = old[heading_index..].find('\n').map(|i| heading_index + i);
        let insert_point = line_end.unwrap_or(old.len());
        let insert_at = match find_next_heading(&old[insert_point..]) {
            Some(rel) => insert_point + rel,
            None => old.len(),
        };
        let new_content = format!(
            "{}\n\n{}{}",
            &old[..insert_at],
            add_section,
            &old[insert_at..]
        );
        Ok((new_content, format!("after \"{after_heading}\"")))
    } else {
        Err(Error::Internal(format!(
            "Invalid position: \"{pos}\". Use \"end\", \"start\", or \"after:HEADING\"."
        )))
    }
}

/// `note.edit` — first exact-match replacement. Returns
/// `(new_content, match_position, was_empty)`; `match_position` is a scalar
/// (char) offset, or `-1` when the note was empty.
pub(crate) fn apply_edit(old: &str, old_text: &str, new_text: &str) -> Result<(String, i64, bool)> {
    if old.is_empty() {
        return Ok((new_text.to_string(), -1, true));
    }
    match old.find(old_text) {
        None => Err(Error::Internal(format!(
            "Text not found in note. Make sure old matches exactly (including whitespace and line breaks).\n\nNote content length: {} chars.\n\nSearched for:\n{}{}",
            old.chars().count(),
            truncate_chars(old_text, 200),
            if old_text.chars().count() > 200 { "..." } else { "" }
        ))),
        Some(idx) => {
            let new_content = format!("{}{}{}", &old[..idx], new_text, &old[idx + old_text.len()..]);
            let match_position = i64::try_from(old[..idx].chars().count()).expect("value fits in i64");
            Ok((new_content, match_position, false))
        }
    }
}

/// `note.editLines` — 1-based inclusive replace/delete/insert.
pub(crate) fn apply_edit_lines(old: &str, start: i64, end: i64, content: &str) -> Result<String> {
    if start < 1 {
        return Err(Error::Internal(
            "start must be a positive integer".to_string(),
        ));
    }
    if end < 1 {
        return Err(Error::Internal(
            "end must be a positive integer".to_string(),
        ));
    }
    if start > end {
        return Err(Error::Internal(
            "start cannot be greater than end".to_string(),
        ));
    }
    if old.is_empty() {
        return Ok(content.to_string());
    }
    let lines: Vec<&str> = old.split('\n').collect();
    let total = i64::try_from(lines.len()).expect("value fits in i64");
    if start > total {
        return Err(Error::Internal(format!(
            "start ({start}) exceeds total lines in note ({total})"
        )));
    }
    if end > total {
        return Err(Error::Internal(format!(
            "end ({end}) exceeds total lines in note ({total})"
        )));
    }
    let mut result: Vec<&str> = Vec::new();
    result.extend_from_slice(&lines[..(usize::try_from(start).expect("value fits in usize") - 1)]);
    if !content.is_empty() {
        result.extend(content.split('\n'));
    }
    result.extend_from_slice(&lines[usize::try_from(end).expect("value fits in usize")..]);
    Ok(result.join("\n"))
}

fn remove_first_char(s: &str) -> String {
    let mut it = s.chars();
    it.next();
    it.as_str().to_string()
}

fn remove_last_char(s: &str) -> String {
    let mut it = s.chars();
    it.next_back();
    it.as_str().to_string()
}

/// Replicate `/:\s*"?(.+)"?$/` `match[1]` (single-line content only).
fn colon_extract(s: &str) -> Option<String> {
    let idx = s.find(':')?;
    let mut rest = &s[idx + 1..];
    rest = rest.trim_start_matches(is_js_space_char);
    if let Some(stripped) = rest.strip_prefix('"') {
        rest = stripped;
    }
    if rest.is_empty() {
        return None;
    }
    Some(rest.to_string())
}

/// `note.setContent` content cleaner (quote-strip, JSON-value extraction,
/// truncation and empty guards). The >50% reduction guard lives in the service.
pub(crate) fn clean_set_content(content: &str) -> Result<String> {
    let mut clean = content.to_string();
    if clean.starts_with('"') || clean.starts_with("\\\"") {
        clean = remove_first_char(&clean);
        if clean.ends_with('"') || clean.ends_with("\\\"") {
            clean = remove_last_char(&clean);
        }
    }
    if clean.contains("\": ") && !clean.contains('\n') {
        if let Some(extracted) = colon_extract(&clean) {
            clean = extracted;
        }
    }
    if clean.chars().count() < 50 && !clean.contains('\n') && clean.ends_with("...") {
        return Err(Error::Internal(
            "Content appears to be truncated. Please provide the complete content.".to_string(),
        ));
    }
    if clean.trim().is_empty() {
        return Err(Error::Internal("Content cannot be empty.".to_string()));
    }
    Ok(clean)
}

fn is_hex_or_dash(b: u8) -> bool {
    b.is_ascii_digit() || (b'a'..=b'f').contains(&b) || b == b'-'
}

/// First `[label](intent://local/task/<id>)` link; returns `(label, id)`.
fn find_task_link(s: &str) -> Option<(String, String)> {
    const PREFIX: &str = "](intent://local/task/";
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'[' {
            let label_start = i + 1;
            let mut j = label_start;
            while j < bytes.len() && bytes[j] != b']' {
                j += 1;
            }
            if j < bytes.len() && j > label_start && s[j..].starts_with(PREFIX) {
                let id_start = j + PREFIX.len();
                let mut k = id_start;
                while k < bytes.len() && is_hex_or_dash(bytes[k]) {
                    k += 1;
                }
                if k > id_start && k < bytes.len() && bytes[k] == b')' {
                    return Some((s[label_start..j].to_string(), s[id_start..k].to_string()));
                }
            }
        }
        i += 1;
    }
    None
}

/// Remove every `<!--agent:[^>]+-->` comment, mirroring the TS global replace.
fn strip_agent_comments(s: &str) -> String {
    const OPEN: &str = "<!--agent:";
    let mut out = String::new();
    let mut rest = s;
    while let Some(start) = rest.find(OPEN) {
        out.push_str(&rest[..start]);
        let after = &rest[start + OPEN.len()..];
        if let Some(gt) = after.find("-->") {
            let inner = &after[..gt];
            if !inner.is_empty() && !inner.contains('>') {
                rest = &after[gt + 3..];
                continue;
            }
        }
        out.push_str(OPEN);
        rest = after;
    }
    out.push_str(rest);
    out
}

/// `note.listTasks` — parse checkbox rows from note content.
pub(crate) fn parse_tasks(content: &str) -> Vec<NoteTaskRow> {
    content
        .split('\n')
        .enumerate()
        .filter_map(|(index, line)| {
            let (checkbox, task_text) = match_task_line(line)?;
            let (task_note_id, clean_text) = match find_task_link(&task_text) {
                Some((label, id)) => (Some(id), label),
                None => (None, strip_agent_comments(&task_text).trim().to_string()),
            };
            let status = match checkbox {
                'x' | 'X' => "done",
                '/' => "in-progress",
                _ => "todo",
            };
            Some(NoteTaskRow {
                line_number: index + 1,
                text: clean_text,
                status: status.to_string(),
                task_note_id: task_note_id.clone(),
                linked_task_note_id: task_note_id,
                depends_on: Vec::new(),
                conflicts_with: Vec::new(),
                unmet_depends_on: Vec::new(),
            })
        })
        .collect()
}

/// Replicate `/^(\s*[-*]\s*)\[([ xX\/])\]\s*(.+)$/`; returns `(checkbox, rest)`.
fn match_task_line(line: &str) -> Option<(char, String)> {
    let mut chars = line.chars().peekable();
    while matches!(chars.peek(), Some(c) if is_js_space_char(*c)) {
        chars.next();
    }
    match chars.next() {
        Some('-' | '*') => {}
        _ => return None,
    }
    while matches!(chars.peek(), Some(c) if is_js_space_char(*c)) {
        chars.next();
    }
    if chars.next() != Some('[') {
        return None;
    }
    let Some(checkbox @ (' ' | 'x' | 'X' | '/')) = chars.next() else {
        return None;
    };
    if chars.next() != Some(']') {
        return None;
    }
    while matches!(chars.peek(), Some(c) if is_js_space_char(*c)) {
        chars.next();
    }
    let rest: String = chars.collect();
    if rest.is_empty() {
        return None;
    }
    Some((checkbox, rest))
}

/// Parse an asset id from a raw id or a `workspace-asset://host/<id>` URL.
pub(crate) fn parse_asset_id(asset: &str) -> Result<String> {
    match asset.strip_prefix("workspace-asset://") {
        Some(after) => match after.find('/') {
            Some(slash) if slash > 0 && slash + 1 < after.len() => {
                Ok(after[slash + 1..].to_string())
            }
            _ => Err(Error::Internal(format!(
                "Invalid workspace-asset URL format: {asset}"
            ))),
        },
        None => Ok(asset.to_string()),
    }
}

// ---------------------------------------------------------------------------
// task.* content helpers (ported from `ws.task.*` in ws-note-api.ts).
// ---------------------------------------------------------------------------

/// Map a checkbox status word to its marker; `None` if unrecognized.
pub(crate) fn checkbox_for(word: &str) -> Option<&'static str> {
    match word {
        "todo" => Some("[ ]"),
        "in-progress" => Some("[/]"),
        "done" => Some("[x]"),
        _ => None,
    }
}

/// Map a checkbox character to its status word (TS `currentStatus` mapping).
fn status_word_from_cb(c: char) -> &'static str {
    match c {
        'x' => "done",
        '/' => "in-progress",
        _ => "todo",
    }
}

fn is_line_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\x0b' | b'\x0c')
}

/// Parse a `\s*-\s*\[cb\]` checkbox prefix on a single line. Returns
/// `(box_start_byte, after_bracket_byte, checkbox_char)` where `box_start` is
/// the index of `[` and `after_bracket` is just past `]`. Only `-` bullets and
/// the `[ x/]` checkbox class are accepted (matches the TS `task.*` regexes).
fn parse_dash_checkbox(line: &str) -> Option<(usize, usize, char)> {
    let b = line.as_bytes();
    let mut i = 0;
    while i < b.len() && is_line_space(b[i]) {
        i += 1;
    }
    if i >= b.len() || b[i] != b'-' {
        return None;
    }
    i += 1;
    while i < b.len() && is_line_space(b[i]) {
        i += 1;
    }
    if i >= b.len() || b[i] != b'[' {
        return None;
    }
    let box_start = i;
    i += 1;
    if i >= b.len() {
        return None;
    }
    let cb = b[i] as char;
    if cb != ' ' && cb != 'x' && cb != '/' {
        return None;
    }
    i += 1;
    if i >= b.len() || b[i] != b']' {
        return None;
    }
    i += 1;
    Some((box_start, i, cb))
}

/// `task.updateStatus` — set the checkbox of the line(s) whose task text equals
/// `task_text` exactly (primary), else the first checkbox line containing it
/// (fallback). `checkbox` is the bracket marker (e.g. `[x]`). Errors with the
/// TS "Task not found" message when nothing matches.
pub(crate) fn apply_task_status(content: &str, task_text: &str, checkbox: &str) -> Result<String> {
    let normalized = task_text.trim();
    let mut found = false;
    let mut out: Vec<String> = Vec::with_capacity(content.split('\n').count());
    for line in content.split('\n') {
        if let Some((box_start, after_idx, _cb)) = parse_dash_checkbox(line) {
            let after = &line[after_idx..];
            if after.trim() == normalized {
                found = true;
                out.push(format!("{}{}{}", &line[..box_start], checkbox, after));
                continue;
            }
        }
        out.push(line.to_string());
    }
    if found {
        return Ok(out.join("\n"));
    }
    // Fallback: first checkbox line that merely contains the task text.
    let mut fallback: Vec<String> = Vec::with_capacity(out.len());
    let mut done = false;
    for line in content.split('\n') {
        if !done {
            if let Some((box_start, after_idx, _cb)) = parse_dash_checkbox(line) {
                if line.contains(normalized) {
                    done = true;
                    fallback.push(format!(
                        "{}{}{}",
                        &line[..box_start],
                        checkbox,
                        &line[after_idx..]
                    ));
                    continue;
                }
            }
        }
        fallback.push(line.to_string());
    }
    if !done {
        return Err(Error::Internal(format!(
            "Task not found: \"{normalized}\". Make sure the task text matches exactly."
        )));
    }
    Ok(fallback.join("\n"))
}

/// Outcome of [`apply_task_line_update`].
#[derive(Debug)]
pub(crate) struct TaskLineUpdate {
    pub content: String,
    pub previous_text: String,
    pub new_text: String,
    pub status_word: String,
}

/// `task.update` — atomic single-line edit (1-based `line`) with an optional
/// `expected` conflict check. `status` is a validated word (`todo`/`in-progress`
/// /`done`) or `None` to keep the current status; `text` replaces the task text.
pub(crate) fn apply_task_line_update(
    content: &str,
    line: i64,
    text: Option<&str>,
    status: Option<&str>,
    expected: Option<&str>,
) -> Result<TaskLineUpdate> {
    let mut lines: Vec<String> = content.split('\n').map(str::to_string).collect();
    let total = i64::try_from(lines.len()).expect("value fits in i64");
    if line > total {
        return Err(Error::Internal(format!(
            "Line {line} does not exist. Note has {total} lines."
        )));
    }
    let current = lines[usize::try_from(line - 1).expect("value fits in usize")].clone();
    let parsed = parse_dash_checkbox(&current);
    let Some((box_start, after_idx, cb)) = parsed else {
        let trunc: String = current.chars().take(50).collect();
        let ellipsis = if current.chars().count() > 50 {
            "..."
        } else {
            ""
        };
        return Err(Error::Internal(format!(
            "Line {line} is not a task. Expected format: \"- [ ] task text\". Found: \"{trunc}{ellipsis}\""
        )));
    };
    let prefix = &current[..box_start];
    let current_task_text = current[after_idx..].trim_start();
    if let Some(exp) = expected {
        if exp.trim() != current_task_text.trim() {
            return Err(Error::Internal(format!(
                "Conflict detected: Task content has changed.\nExpected: \"{}\"\nActual: \"{}\"\nAnother agent may have modified this task. Please re-read the note and try again.",
                exp.trim(),
                current_task_text.trim()
            )));
        }
    }
    let current_status = status_word_from_cb(cb);
    let next_status = status.unwrap_or(current_status);
    let checkbox = checkbox_for(next_status).ok_or_else(|| {
        Error::Internal("Status must be 'todo', 'in-progress', or 'done'".to_string())
    })?;
    let final_text = match text {
        Some(t) => t.trim().to_string(),
        None => current_task_text.to_string(),
    };
    lines[usize::try_from(line - 1).expect("value fits in usize")] =
        format!("{prefix}{checkbox} {final_text}");
    Ok(TaskLineUpdate {
        content: lines.join("\n"),
        previous_text: current_task_text.to_string(),
        new_text: final_text,
        status_word: next_status.to_string(),
    })
}

// ---------------------------------------------------------------------------
// comment.* anchoring helpers (ported from findAndAnchorText in ws-note-api.ts).
// ---------------------------------------------------------------------------

/// All byte offsets where `search` occurs in `content` (non-overlapping).
fn find_all_occurrences(content: &str, search: &str) -> Vec<usize> {
    if search.is_empty() {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut from = 0;
    while let Some(rel) = content[from..].find(search) {
        let idx = from + rel;
        out.push(idx);
        from = idx + search.len();
    }
    out
}

fn count_occurrences(content: &str, search: &str) -> usize {
    find_all_occurrences(content, search).len()
}

/// 1-based line number of the byte offset `idx`.
fn line_of(content: &str, idx: usize) -> usize {
    content[..idx].bytes().filter(|b| *b == b'\n').count() + 1
}

/// `findAndAnchorText` — locate `comment_target` inside a unique `search_context`
/// occurrence. Returns `(from_byte, to_byte, line)`; failures are
/// [`Error::InvalidParams`] so the router surfaces the descriptive message as
/// `-32602` instead of an opaque `-32603 "Internal error"`.
///
/// When the exact substring search finds no occurrence, a plaintext-tolerant
/// fallback retries against a markdown-stripped projection of the note (see
/// [`plaintext_projection`]): editor clients derive anchors from the rendered
/// document's *plain text*, which drops markdown syntax and joins blocks with
/// no separator.
pub(crate) fn find_and_anchor_text(
    content: &str,
    search_context: &str,
    comment_target: &str,
) -> Result<(usize, usize, usize)> {
    let ctx = find_all_occurrences(content, search_context);
    if ctx.len() > 1 {
        return Err(Error::InvalidParams(
            "The search context appears multiple times in the document.".to_string(),
        ));
    }
    if ctx.is_empty() {
        let (from, to) = plaintext_fallback_anchor(content, search_context, comment_target)?;
        return Ok((from, to, line_of(content, from)));
    }
    let ctx_from = ctx[0];
    let Some(rel) = search_context.find(comment_target) else {
        return Err(Error::InvalidParams(
            "The comment target was not found within the search context.".to_string(),
        ));
    };
    if count_occurrences(search_context, comment_target) > 1 {
        return Err(Error::InvalidParams(
            "The comment target appears multiple times within the search context.".to_string(),
        ));
    }
    let from = ctx_from + rel;
    let to = from + comment_target.len();
    Ok((from, to, line_of(content, from)))
}

/// A plaintext rendering of a note's markdown plus a per-byte map back to the
/// source: `map[i]` is the starting byte offset in the markdown of the
/// character that produced byte `i` of `text` (multi-byte characters repeat
/// the same start; the end boundary is recomputed from the source on demand).
struct PlaintextProjection {
    text: String,
    map: Vec<usize>,
}

/// Characters dropped from BOTH the markdown projection and the search
/// needles: whitespace (block joins and newline/space differences become
/// flexible) plus inline emphasis/code delimiters, link brackets, and `@`.
/// Dropping them symmetrically keeps literal `*`/`_`/brackets in prose
/// matching. `@` is included because editor clients render bare filenames
/// (e.g. `KNOWN_ISSUES.md`) as mention chips whose canonical text carries a
/// leading `@` the markdown source never had; dropping it on both sides keeps
/// literal `@` in prose matching too.
fn is_normalized_away(c: char) -> bool {
    c.is_whitespace() || matches!(c, '*' | '_' | '`' | '~' | '[' | ']' | '@')
}

/// Normalize a client-supplied needle (searchContext / commentTarget) for the
/// plaintext fallback search.
fn normalize_needle(s: &str) -> String {
    s.chars().filter(|c| !is_normalized_away(*c)).collect()
}

/// Byte length of a leading list marker (`- `, `* `, `+ `, `1. `, `1) `) on a
/// line, if present.
fn list_marker_len(line: &str) -> Option<usize> {
    let b = line.as_bytes();
    match b.first()? {
        b'-' | b'*' | b'+' => (b.get(1) == Some(&b' ')).then_some(2),
        b'0'..=b'9' => {
            let digits = b.iter().take_while(|c| c.is_ascii_digit()).count();
            (matches!(b.get(digits), Some(b'.' | b')')) && b.get(digits + 1) == Some(&b' '))
                .then_some(digits + 2)
        }
        _ => None,
    }
}

/// Build the plaintext projection: strip HTML comments (including
/// `<!--anchor:…-->` markers from existing comments), leading heading /
/// list / blockquote markers, and every [`is_normalized_away`] character,
/// keeping link text while dropping the `(url)` part.
fn plaintext_projection(md: &str) -> PlaintextProjection {
    let mut text = String::with_capacity(md.len());
    let mut map = Vec::with_capacity(md.len());
    let mut i = 0;
    let mut at_line_start = true;
    while i < md.len() {
        let rest = &md[i..];
        if rest.starts_with("<!--") {
            if let Some(close) = rest.find("-->") {
                i += close + 3;
                continue;
            }
        }
        if at_line_start {
            at_line_start = false;
            let line_end = rest.find('\n').map_or(md.len(), |p| i + p);
            let line = &md[i..line_end];
            let indent = line.len() - line.trim_start_matches([' ', '\t']).len();
            let body = &line[indent..];
            let hashes = body.bytes().take_while(|b| *b == b'#').count();
            let skip = if (1..=6).contains(&hashes)
                && matches!(body.as_bytes().get(hashes), Some(b' ' | b'\t'))
            {
                hashes + 1
            } else if body.starts_with("> ") {
                2
            } else {
                list_marker_len(body).unwrap_or(0)
            };
            i += indent + skip;
            continue;
        }
        let ch = rest.chars().next().expect("in-bounds char");
        let clen = ch.len_utf8();
        if ch == '\n' {
            at_line_start = true;
            i += clen;
            continue;
        }
        if is_normalized_away(ch) {
            // Link target: drop the `(url)` immediately following a `]`.
            if ch == ']' {
                let after = &md[i + 1..];
                if after.starts_with('(') {
                    if let Some(close) = after.find(')') {
                        i += 1 + close + 1;
                        continue;
                    }
                }
            }
            i += clen;
            continue;
        }
        text.push(ch);
        for _ in 0..clen {
            map.push(i);
        }
        i += clen;
    }
    PlaintextProjection { text, map }
}

/// Minimum number of surviving context bytes (before-suffix + after-prefix
/// overlap around the target) required for the stale-context target rescue.
/// Guards against anchoring on an accidental short substring when the whole
/// context is gone.
const MIN_RESCUE_CONTEXT_OVERLAP: usize = 4;

/// Byte length of the longest suffix of `needle` that is also a suffix of
/// `hay`, clamped back to a char boundary of `needle` so a shared lead byte
/// of two different multi-byte characters never counts as surviving context.
fn suffix_overlap(needle: &str, hay: &str) -> usize {
    let mut n = needle
        .as_bytes()
        .iter()
        .rev()
        .zip(hay.as_bytes().iter().rev())
        .take_while(|(a, b)| a == b)
        .count();
    while !needle.is_char_boundary(needle.len() - n) {
        n -= 1;
    }
    n
}

/// Byte length of the longest prefix of `needle` that is also a prefix of
/// `hay`, clamped back to a char boundary of `needle` (see [`suffix_overlap`]).
fn prefix_overlap(needle: &str, hay: &str) -> usize {
    let mut n = needle
        .as_bytes()
        .iter()
        .zip(hay.as_bytes())
        .take_while(|(a, b)| a == b)
        .count();
    while !needle.is_char_boundary(n) {
        n -= 1;
    }
    n
}

/// Stale-context rescue: the search context no longer occurs in the document
/// (typically the editor doc lagged the server copy, so the ±50-char context
/// captured around the selection includes text from blocks that have since
/// changed). Locate the (normalized) comment target alone in the projection
/// and corroborate with whatever before/after context still matches adjacent
/// to it; requires at least [`MIN_RESCUE_CONTEXT_OVERLAP`] surviving bytes and
/// a strictly-best occurrence, so ambiguity is still an error and short
/// accidental matches are rejected. Returns `(projection_pos, needle_len)`.
fn rescue_target_position(
    proj_text: &str,
    needle_ctx: &str,
    needle_tgt: &str,
) -> Result<(usize, usize)> {
    let not_found =
        || Error::InvalidParams("Could not find the search context in the document.".to_string());
    // Same guard as the exact and projection paths: an ambiguous target
    // within the provided context would make the before/after split (and
    // therefore the scoring) anchor off the wrong instance.
    if count_occurrences(needle_ctx, needle_tgt) > 1 {
        return Err(Error::InvalidParams(
            "The comment target appears multiple times within the search context.".to_string(),
        ));
    }
    let Some(rel) = needle_ctx.find(needle_tgt) else {
        // Consistent with the exact/projection paths: a target that is not
        // inside the provided context is a target problem, not a missing
        // context.
        return Err(Error::InvalidParams(
            "The comment target was not found within the search context.".to_string(),
        ));
    };
    let (before, after) = (&needle_ctx[..rel], &needle_ctx[rel + needle_tgt.len()..]);
    // Stream over non-overlapping occurrences (no Vec of every position: a
    // short/common target in a large document would otherwise allocate and
    // score unboundedly).
    let mut best: Option<(usize, usize)> = None;
    let mut tied = false;
    let mut from = 0;
    while let Some(off) = proj_text[from..].find(needle_tgt) {
        let pos = from + off;
        from = pos + needle_tgt.len();
        let score = suffix_overlap(before, &proj_text[..pos])
            + prefix_overlap(after, &proj_text[pos + needle_tgt.len()..]);
        match best {
            Some((best_score, _)) if score < best_score => {}
            Some((best_score, _)) if score == best_score => tied = true,
            _ => {
                best = Some((score, pos));
                tied = false;
            }
        }
    }
    let Some((score, pos)) = best else {
        return Err(not_found());
    };
    if score < MIN_RESCUE_CONTEXT_OVERLAP {
        return Err(not_found());
    }
    if tied {
        return Err(Error::InvalidParams(
            "The comment target appears multiple times in the document.".to_string(),
        ));
    }
    Ok((pos, needle_tgt.len()))
}

/// Fallback anchor search over the plaintext projection. Applies the same
/// uniqueness rules as the exact path and maps the unique match back to
/// markdown byte offsets. When even the projected context is gone (stale
/// editor doc), falls back to [`rescue_target_position`].
fn plaintext_fallback_anchor(
    content: &str,
    search_context: &str,
    comment_target: &str,
) -> Result<(usize, usize)> {
    let not_found =
        || Error::InvalidParams("Could not find the search context in the document.".to_string());
    let needle_ctx = normalize_needle(search_context);
    if needle_ctx.is_empty() {
        return Err(not_found());
    }
    let proj = plaintext_projection(content);
    let ctx = find_all_occurrences(&proj.text, &needle_ctx);
    if ctx.len() > 1 {
        return Err(Error::InvalidParams(
            "The search context appears multiple times in the document.".to_string(),
        ));
    }
    let needle_tgt = normalize_needle(comment_target);
    let (pos, tgt_len) = if let Some(&ctx_from) = ctx.first() {
        if needle_tgt.is_empty() || !needle_ctx.contains(&needle_tgt) {
            return Err(Error::InvalidParams(
                "The comment target was not found within the search context.".to_string(),
            ));
        }
        if count_occurrences(&needle_ctx, &needle_tgt) > 1 {
            return Err(Error::InvalidParams(
                "The comment target appears multiple times within the search context.".to_string(),
            ));
        }
        let rel = needle_ctx.find(&needle_tgt).expect("checked above");
        (ctx_from + rel, needle_tgt.len())
    } else {
        if needle_tgt.is_empty() {
            // Consistent with the in-context path: an empty target is a
            // target problem, not a context-not-found.
            return Err(Error::InvalidParams(
                "The comment target was not found within the search context.".to_string(),
            ));
        }
        rescue_target_position(&proj.text, &needle_ctx, &needle_tgt)?
    };
    let from = proj.map[pos];
    let last_start = proj.map[pos + tgt_len - 1];
    let last_len = content[last_start..]
        .chars()
        .next()
        .expect("mapped offset is a char boundary")
        .len_utf8();
    let to = last_start + last_len;
    // A projected match may span existing `<!--anchor:…-->` markers (the
    // projection strips them). That is fine: overlapping comment ranges are
    // supported — the new pair interleaves with the existing pairs, and each
    // comment's own id still pins its markers. Callers sanitize the STORED
    // comment text separately (see [`strip_anchor_marker_text`]) so raw
    // marker text never leaks into comment rows.
    Ok((from, to))
}

// ---------------------------------------------------------------------------
// Comment anchor recovery (ported from
// `src/features/comments/markdown-anchor-recovery.ts`). `comment.add` embeds
// `<!--anchor:{id}:start-->…<!--anchor:{id}:end-->` markers into the note
// content; subsequent note edits may destroy one or both markers. Reference
// semantics: extract the surrounding CONTEXT_LENGTH-char context on add and,
// on later edits, try to relocate a partial anchor using that context;
// unresolvable anchors are marked orphaned and their stray markers removed.
// ---------------------------------------------------------------------------

/// Amount of surrounding text captured on add / used for recovery matching
/// (reference `CONTEXT_LENGTH = 50` in `markdown-anchor-recovery.ts`).
pub(crate) const ANCHOR_CONTEXT_LEN: usize = 50;

/// Health of a comment's anchor markers in a note's markdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AnchorState {
    /// Both markers present with a valid range.
    Healthy,
    /// Both markers absent — the anchored span was removed entirely.
    Missing,
    /// Only the `:start` marker survives.
    PartialStartOnly,
    /// Only the `:end` marker survives.
    PartialEndOnly,
    /// Both markers present but nothing (or only whitespace) between them.
    Degenerate,
}

/// Start/end byte offsets of a comment's `<!--anchor:{id}:…-->` markers, if
/// found. `start`/`end` point at the first byte of each marker.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct AnchorPositions {
    pub start: Option<usize>,
    pub end: Option<usize>,
}

fn start_marker(comment_id: &str) -> String {
    format!("<!--anchor:{comment_id}:start-->")
}

fn end_marker(comment_id: &str) -> String {
    format!("<!--anchor:{comment_id}:end-->")
}

/// Byte offsets of the `{id}:start` / `{id}:end` markers (reference
/// `findAnchorsInMarkdown`).
pub(crate) fn find_anchor_positions(markdown: &str, comment_id: &str) -> AnchorPositions {
    let start_pat = start_marker(comment_id);
    let end_pat = end_marker(comment_id);
    AnchorPositions {
        start: markdown.find(&start_pat),
        end: markdown.find(&end_pat),
    }
}

/// Classify a comment's anchor state in `markdown` (reference
/// `scanForProblematicAnchors` + healthy/missing cases).
pub(crate) fn classify_anchor_state(markdown: &str, comment_id: &str) -> AnchorState {
    let start_pat = start_marker(comment_id);
    let end_pat = end_marker(comment_id);
    let start = markdown.find(&start_pat);
    let end = markdown.find(&end_pat);
    match (start, end) {
        (None, None) => AnchorState::Missing,
        (Some(_), None) => AnchorState::PartialStartOnly,
        (None, Some(_)) => AnchorState::PartialEndOnly,
        (Some(s), Some(e)) => {
            let text_start = s + start_pat.len();
            if text_start > e {
                // Inverted order — treat as degenerate; both will be scrubbed.
                return AnchorState::Degenerate;
            }
            let between = &markdown[text_start..e];
            if between.trim().is_empty() {
                AnchorState::Degenerate
            } else {
                AnchorState::Healthy
            }
        }
    }
}

/// Up to [`ANCHOR_CONTEXT_LEN`] characters (UTF-8-safe) immediately preceding
/// `pos` in `content`. Reference `extractAnchoredText.contextBefore`.
pub(crate) fn context_before(content: &str, pos: usize) -> String {
    // Walk backwards up to CONTEXT_LENGTH chars from `pos`.
    let start = content[..pos]
        .char_indices()
        .rev()
        .take(ANCHOR_CONTEXT_LEN)
        .last()
        .map_or(pos, |(idx, _)| idx);
    content[start..pos].to_string()
}

/// Up to [`ANCHOR_CONTEXT_LEN`] characters (UTF-8-safe) immediately following
/// `pos` in `content`. Reference `extractAnchoredText.contextAfter`.
pub(crate) fn context_after(content: &str, pos: usize) -> String {
    let end = content[pos..]
        .char_indices()
        .take(ANCHOR_CONTEXT_LEN)
        .last()
        .map_or(pos, |(idx, ch)| pos + idx + ch.len_utf8());
    content[pos..end].to_string()
}

/// Remove every `{id}:start` / `{id}:end` marker occurrence from `markdown`
/// (reference `removeAnchors`; used to scrub broken/degenerate anchors).
pub(crate) fn remove_anchor_markers(markdown: &str, comment_id: &str) -> String {
    let start_pat = start_marker(comment_id);
    let end_pat = end_marker(comment_id);
    markdown.replace(&start_pat, "").replace(&end_pat, "")
}

const ANCHOR_MARKER_PREFIX: &str = "<!--anchor:";

/// True when `id` is a canonical hyphenated UUID (`8-4-4-4-12` hex digits).
/// Real anchor markers always carry a UUID id (`comment.add` mints v4 UUIDs
/// and validates client-supplied ids against this same canonical form);
/// anything else — e.g. the literal `{id}` in documentation examples — is
/// ordinary user content.
pub(crate) fn is_canonical_uuid(id: &str) -> bool {
    let b = id.as_bytes();
    if b.len() != 36 {
        return false;
    }
    b.iter().enumerate().all(|(i, &c)| match i {
        8 | 13 | 18 | 23 => c == b'-',
        _ => c.is_ascii_hexdigit(),
    })
}

/// Parse a real anchor marker at the start of `rest`
/// (`<!--anchor:<uuid>:start|end|point-->`). Returns `(uuid, marker_len)`;
/// `None` when the text merely resembles a marker (non-UUID id, unknown
/// role, unterminated comment).
fn parse_uuid_anchor_marker(rest: &str) -> Option<(&str, usize)> {
    let after = rest.strip_prefix(ANCHOR_MARKER_PREFIX)?;
    let colon = after.find(':')?;
    let id = &after[..colon];
    if !is_canonical_uuid(id) {
        return None;
    }
    let tail = &after[colon + 1..];
    ["start-->", "end-->", "point-->"]
        .iter()
        .find(|role| tail.starts_with(**role))
        .map(|role| (id, ANCHOR_MARKER_PREFIX.len() + colon + 1 + role.len()))
}

/// Remove every `<!--anchor:…-->` substring (any id, real or lookalike) from
/// `s`. Used to sanitize STORED comment text — `anchor_text`,
/// `anchor_before`, `anchor_after` — when the anchored span overlaps other
/// comments' markers: raw marker text must never leak into comment rows.
/// Runs to a fixpoint so removals that concatenate into new marker text are
/// also caught.
///
/// Removal is greedy from `<!--anchor:` to the NEXT `-->` anywhere in the
/// string — deliberately looser than [`parse_uuid_anchor_marker`]: an
/// unterminated lookalike followed by a real marker is one HTML comment in
/// rendered markdown, so the whole run (including text between them) is
/// dropped, matching what a renderer would hide.
pub(crate) fn strip_anchor_marker_text(s: &str) -> String {
    let mut cur = s.to_string();
    loop {
        let mut out = String::with_capacity(cur.len());
        let mut i = 0;
        let mut changed = false;
        while let Some(rel) = cur[i..].find(ANCHOR_MARKER_PREFIX) {
            let pos = i + rel;
            let Some(close) = cur[pos..].find("-->") else {
                break;
            };
            out.push_str(&cur[i..pos]);
            i = pos + close + "-->".len();
            changed = true;
        }
        out.push_str(&cur[i..]);
        if !changed {
            return out;
        }
        cur = out;
    }
}

/// Remove every UUID-format anchor marker whose uuid is not in `live_ids` —
/// phantom debris (an id with no comment row at all) and stale-orphan debris
/// (markers of rows already flagged `is_orphaned`) alike. Non-UUID
/// marker-lookalikes (documentation literals such as
/// `<!--anchor:{id}:start-->`) are user content and are never touched.
pub(crate) fn scrub_phantom_anchor_markers(
    content: &str,
    live_ids: &std::collections::HashSet<String>,
) -> String {
    let mut out = String::with_capacity(content.len());
    let mut i = 0;
    while let Some(rel) = content[i..].find(ANCHOR_MARKER_PREFIX) {
        let pos = i + rel;
        out.push_str(&content[i..pos]);
        match parse_uuid_anchor_marker(&content[pos..]) {
            Some((id, len)) if !live_ids.contains(id) => i = pos + len,
            Some((_, len)) => {
                out.push_str(&content[pos..pos + len]);
                i = pos + len;
            }
            None => {
                out.push_str(ANCHOR_MARKER_PREFIX);
                i = pos + ANCHOR_MARKER_PREFIX.len();
            }
        }
    }
    out.push_str(&content[i..]);
    out
}

/// Outcome of an anchor recovery attempt (reference `RecoveryResult`).
#[derive(Debug, Clone)]
pub(crate) enum RecoveryOutcome {
    /// Markers restored — caller should adopt the returned markdown.
    Recovered(String),
    /// Recovery failed — caller should scrub any stray markers and mark the
    /// comment orphaned. The reason mirrors the reference log messages and is
    /// logged by the re-anchor pass for diagnostics.
    Failed(&'static str),
}

/// Attempt to relocate a partially-anchored comment inside `markdown`, using
/// the stored surrounding context. Reference `recoverPartialAnchor` collapsed
/// to a single "anchor-neighbor" pass over the current content — the neighbor
/// word is derived from the stored `anchor_before` / `anchor_after` context
/// that `comment.add` captured, so no version history is required. The
/// original `anchor_text` is not used as an extra constraint here (it can
/// itself have been partly edited); the surviving marker + neighbor word are
/// what pin the recovered range, matching the reference behavior.
pub(crate) fn recover_partial_anchor(
    markdown: &str,
    comment_id: &str,
    anchor_before: Option<&str>,
    anchor_after: Option<&str>,
) -> RecoveryOutcome {
    let positions = find_anchor_positions(markdown, comment_id);
    match (positions.start, positions.end) {
        (Some(_), Some(_)) => RecoveryOutcome::Failed("both-anchors-present"),
        (None, None) => RecoveryOutcome::Failed("both-anchors-missing"),
        (Some(start_pos), None) => recover_missing_end(
            markdown,
            comment_id,
            start_pos,
            anchor_after.unwrap_or_default(),
        ),
        (None, Some(end_pos)) => recover_missing_start(
            markdown,
            comment_id,
            end_pos,
            anchor_before.unwrap_or_default(),
        ),
    }
}

/// Have `{id}:start`; need to restore `{id}:end` after the original anchored
/// text. Uses the first "neighbor" word from `context_after` (the word that
/// followed the end marker at add-time) as a landmark inside `markdown`. On
/// success the surviving start marker stays put and a fresh end marker is
/// inserted right before the neighbor, matching reference
/// `recoverMissingEndAnchor`'s trailing-whitespace walk so the anchored range
/// does not swallow trailing whitespace.
fn recover_missing_end(
    markdown: &str,
    comment_id: &str,
    start_pos: usize,
    context_after: &str,
) -> RecoveryOutcome {
    let start_pat = start_marker(comment_id);
    let end_pat = end_marker(comment_id);
    let text_start = start_pos + start_pat.len();
    if text_start > markdown.len() {
        return RecoveryOutcome::Failed("start-anchor-out-of-range");
    }
    let neighbor = leading_word(context_after);
    if neighbor.is_empty() {
        return RecoveryOutcome::Failed("neighbor-not-found");
    }
    let Some(rel) = markdown[text_start..].find(&neighbor) else {
        return RecoveryOutcome::Failed("neighbor-not-found");
    };
    let mut insertion = text_start + rel;
    // Walk backwards over whitespace so the freshly-placed end marker sits
    // immediately after the last non-whitespace char of the anchored text,
    // exactly like the reference does.
    while insertion > text_start {
        let prev = markdown[..insertion].chars().next_back();
        match prev {
            Some(c) if c.is_whitespace() => insertion -= c.len_utf8(),
            _ => break,
        }
    }
    let mut out = String::with_capacity(markdown.len() + end_pat.len());
    out.push_str(&markdown[..insertion]);
    out.push_str(&end_pat);
    out.push_str(&markdown[insertion..]);
    RecoveryOutcome::Recovered(out)
}

/// Have `{id}:end`; need to restore `{id}:start` before the original anchored
/// text. Symmetric counterpart to [`recover_missing_end`]; keeps the surviving
/// end marker and inserts a fresh start marker right after the trailing word
/// of `context_before`, walking forward over whitespace so the anchored range
/// starts at the first non-whitespace char after the neighbor.
fn recover_missing_start(
    markdown: &str,
    comment_id: &str,
    end_pos: usize,
    context_before: &str,
) -> RecoveryOutcome {
    let start_pat = start_marker(comment_id);
    let neighbor = trailing_word(context_before);
    if neighbor.is_empty() {
        return RecoveryOutcome::Failed("neighbor-not-found");
    }
    let Some(idx) = markdown[..end_pos].rfind(&neighbor) else {
        return RecoveryOutcome::Failed("neighbor-not-found");
    };
    let mut insertion = idx + neighbor.len();
    // Skip forward over whitespace so the fresh start marker sits right
    // before the first non-whitespace char of the anchored text.
    while insertion < end_pos {
        let next = markdown[insertion..].chars().next();
        match next {
            Some(c) if c.is_whitespace() => insertion += c.len_utf8(),
            _ => break,
        }
    }
    let mut out = String::with_capacity(markdown.len() + start_pat.len());
    out.push_str(&markdown[..insertion]);
    out.push_str(&start_pat);
    out.push_str(&markdown[insertion..]);
    RecoveryOutcome::Recovered(out)
}

fn leading_word(s: &str) -> String {
    s.trim_start()
        .chars()
        .take_while(|c| !c.is_whitespace())
        .collect()
}

fn trailing_word(s: &str) -> String {
    let trimmed = s.trim_end();
    let start = trimmed
        .char_indices()
        .rev()
        .take_while(|(_, c)| !c.is_whitespace())
        .last()
        .map_or(trimmed.len(), |(i, _)| i);
    trimmed[start..].to_string()
}

// ---------------------------------------------------------------------------
// @@@task block parsing (ported from notes/utils/task-block-parser.ts).
// ---------------------------------------------------------------------------

/// One task parsed from a `@@@task` block.
#[derive(Debug, Default)]
pub(crate) struct ParsedTaskBlock {
    pub title: String,
    pub content: String,
    /// Raw `key=` header attribute (local key, unresolved).
    pub key: Option<String>,
    /// Raw `dependsOn=` header attribute items (unresolved keys/titles/ids).
    pub depends_on: Vec<String>,
    /// Raw `conflictsWith=` header attribute items (unresolved).
    pub conflicts_with: Vec<String>,
    /// Raw `effort=` header attribute (unparsed estimate).
    pub effort: Option<String>,
    /// Parse-level problems on the fence header (unknown/duplicate/empty
    /// attributes). The block still parses; callers surface these as warnings.
    pub issues: Vec<String>,
}

/// Result of [`extract_task_blocks`].
pub(crate) struct TaskBlocksResult {
    pub tasks: Vec<ParsedTaskBlock>,
    pub content_without_blocks: String,
}

/// `^#\s+(.+)$` — single-`#` heading; returns the trimmed title if non-empty.
fn match_h1(line: &str) -> Option<String> {
    let rest = line.strip_prefix('#')?;
    if rest.starts_with('#') {
        return None;
    }
    let trimmed = rest.trim_start_matches(is_js_space_char);
    if trimmed.len() == rest.len() || trimmed.is_empty() {
        // No whitespace after '#', or nothing after it.
        return None;
    }
    let title = trimmed.trim();
    if title.is_empty() {
        None
    } else {
        Some(title.to_string())
    }
}

/// Parse a block body: first `# ` line is the title, the rest is the content.
fn parse_task_block_content(block: &str) -> Option<ParsedTaskBlock> {
    let normalized = block.replace("\r\n", "\n").replace('\r', "\n");
    let lines: Vec<&str> = normalized.split('\n').collect();
    let mut title: Option<String> = None;
    let mut start = 0;
    for (i, line) in lines.iter().enumerate() {
        if let Some(t) = match_h1(line) {
            title = Some(t);
            start = i + 1;
            break;
        }
    }
    let title = title?;
    let content = lines[start..].join("\n").trim().to_string();
    Some(ParsedTaskBlock {
        title,
        content,
        ..Default::default()
    })
}

/// True if the content contains at least one `@@@task`/`@@@tasks` fence-and-close
/// pair (valid title not required), matching the TS `hasTaskBlocks` regex test.
pub(crate) fn has_task_blocks(content: &str) -> bool {
    !scan_blocks(content).is_empty()
}

/// Header attributes parsed from a `@@@task` fence line.
#[derive(Debug, Default)]
struct TaskBlockHeader {
    key: Option<String>,
    depends_on: Vec<String>,
    conflicts_with: Vec<String>,
    effort: Option<String>,
    issues: Vec<String>,
}

/// One raw block found by [`scan_blocks`]: full range + fence header + body.
struct ScannedBlock {
    start: usize,
    end: usize,
    header: TaskBlockHeader,
    body: String,
}

/// Parse the fence-line text after `@@@task`/`@@@tasks` into header attributes.
///
/// Grammar: whitespace-separated `name=value` pairs; list values are
/// comma-separated bare tokens with optional whitespace around commas
/// (`dependsOn=a, b ,c`). Known names: `key`, `dependsOn`, `conflictsWith`,
/// `effort` (case-sensitive). Returns `None` when the text is not
/// attribute-shaped at all (free text, missing `=`, non-alphanumeric name) —
/// such lines are not fences, matching the pre-attribute behavior. Semantic
/// problems on attribute-shaped tokens (unknown name, duplicate, empty value)
/// keep the fence valid and are carried on `issues`.
fn parse_fence_header(header: &str) -> Option<TaskBlockHeader> {
    let mut h = TaskBlockHeader::default();
    let trimmed = header.trim();
    if trimmed.is_empty() {
        return Some(h);
    }
    let tokens: Vec<&str> = trimmed.split_whitespace().collect();
    let mut attrs: Vec<String> = Vec::new();
    let mut idx = 0;
    while idx < tokens.len() {
        let mut acc = tokens[idx].to_string();
        // Re-join list items split around commas: `a, b` / `a ,b` / `a , b`.
        while idx + 1 < tokens.len() && (acc.ends_with(',') || tokens[idx + 1].starts_with(',')) {
            idx += 1;
            acc.push_str(tokens[idx]);
        }
        attrs.push(acc);
        idx += 1;
    }
    let mut seen: Vec<String> = Vec::new();
    for attr in attrs {
        let (name, value) = attr.split_once('=')?;
        if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric()) {
            return None;
        }
        // Duplicates are detected by attribute-name occurrence (not by a
        // filled slot), so `key= key=second` is empty-value + duplicate and
        // the second value is never silently accepted.
        if seen.iter().any(|s| s == name) {
            h.issues.push(format!(
                "duplicate attribute `{name}` (first occurrence kept)"
            ));
            continue;
        }
        seen.push(name.to_string());
        match name {
            "key" => set_scalar_attr(&mut h.key, name, value, &mut h.issues),
            "effort" => set_scalar_attr(&mut h.effort, name, value, &mut h.issues),
            "dependsOn" => set_list_attr(&mut h.depends_on, name, value, &mut h.issues),
            "conflictsWith" => set_list_attr(&mut h.conflicts_with, name, value, &mut h.issues),
            other => h
                .issues
                .push(format!("unknown attribute `{other}` on task block header")),
        }
    }
    Some(h)
}

fn set_scalar_attr(slot: &mut Option<String>, name: &str, value: &str, issues: &mut Vec<String>) {
    if value.is_empty() {
        issues.push(format!("empty value for attribute `{name}`"));
    } else if value.contains(',') || value.contains('=') {
        // A comma or `=` inside a scalar value is almost certainly a stray
        // comma gluing attributes together (`key=a ,dependsOn=b`) or a list
        // value on a scalar attribute (`key=a,b`) — flag it, don't guess.
        issues.push(format!(
            "malformed value `{value}` for attribute `{name}` (expected a single bare token; attributes are separated by whitespace)"
        ));
    } else {
        *slot = Some(value.to_string());
    }
}

fn set_list_attr(slot: &mut Vec<String>, name: &str, value: &str, issues: &mut Vec<String>) {
    let mut items: Vec<String> = Vec::new();
    let mut any_item = false;
    for item in value.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        any_item = true;
        if item.contains('=') {
            // A stray comma before another attribute glues `name=value` into
            // this list (`dependsOn=a, key=b`) — flag and drop the item.
            issues.push(format!(
                "malformed item `{item}` in attribute `{name}` (attributes are separated by whitespace, not commas)"
            ));
        } else {
            items.push(item.to_string());
        }
    }
    if !any_item {
        issues.push(format!("empty value for attribute `{name}`"));
    }
    *slot = items;
}

/// Scan raw `@@@task` blocks. The fence line may carry optional header
/// attributes (see [`parse_fence_header`]); a bare fence behaves exactly as
/// before.
fn scan_blocks(content: &str) -> Vec<ScannedBlock> {
    let b = content.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while let Some(rel) = content[i..].find("@@@task") {
        let pos = i + rel;
        let mut j = pos + "@@@task".len();
        if j < b.len() && b[j] == b's' {
            j += 1;
        }
        // The keyword must be followed by whitespace or end-of-line.
        if j < b.len() && !matches!(b[j], b' ' | b'\t' | b'\r' | b'\n') {
            i = pos + "@@@task".len();
            continue;
        }
        let Some(line_end_rel) = content[j..].find('\n') else {
            // No newline after the fence keyword — not a block.
            i = pos + "@@@task".len();
            continue;
        };
        let line_end = j + line_end_rel;
        // Strip at most ONE trailing CR (CRLF line ending) — main's scanner
        // consumed a single optional `\r`, so `@@@task\r\r\n` stays a
        // non-fence via the interior-CR guard below.
        let raw_header = &content[j..line_end];
        let header_text = raw_header.strip_suffix('\r').unwrap_or(raw_header);
        let header = if header_text.contains('\r') {
            None
        } else {
            parse_fence_header(header_text)
        };
        let Some(header) = header else {
            i = pos + "@@@task".len();
            continue;
        };
        let body_start = line_end + 1;
        match content[body_start..].find("@@@") {
            Some(close_rel) => {
                let body_end = body_start + close_rel;
                let full_end = body_end + 3;
                out.push(ScannedBlock {
                    start: pos,
                    end: full_end,
                    header,
                    body: content[body_start..body_end].to_string(),
                });
                i = full_end;
            }
            None => break,
        }
    }
    out
}

/// `extractTasksBlocks` — parse all `@@@task` blocks and replace them with
/// indexed placeholders (valid) or a removed-marker (invalid).
pub(crate) fn extract_task_blocks(content: &str) -> TaskBlocksResult {
    let blocks = scan_blocks(content);
    let mut tasks = Vec::new();
    let mut out = String::new();
    let mut cursor = 0;
    let mut valid_index = 0;
    for block in blocks {
        out.push_str(&content[cursor..block.start]);
        match parse_task_block_content(&block.body) {
            Some(mut task) => {
                let _ = write!(out, "<!-- task-block-placeholder-{valid_index} -->");
                task.key = block.header.key;
                task.depends_on = block.header.depends_on;
                task.conflicts_with = block.header.conflicts_with;
                task.effort = block.header.effort;
                task.issues = block.header.issues;
                tasks.push(task);
                valid_index += 1;
            }
            None => out.push_str("<!-- invalid-task-block-removed -->"),
        }
        cursor = block.end;
    }
    out.push_str(&content[cursor..]);
    TaskBlocksResult {
        tasks,
        content_without_blocks: out,
    }
}

/// Strip common markdown formatting from a title (faithful port of the
/// non-lookbehind transforms in `stripMarkdownFormatting`). Single-character
/// italic markers are intentionally left intact (the TS version relies on
/// lookbehind, unavailable here); task titles are otherwise normalized.
pub(crate) fn strip_markdown_formatting(text: &str) -> String {
    if text.is_empty() {
        return String::new();
    }
    let mut r = strip_paired(text, "**");
    r = strip_paired(&r, "__");
    r = strip_paired(&r, "~~");
    r = strip_paired(&r, "`");
    r = strip_links(&r);
    r = strip_leading_headers(&r);
    r.trim().to_string()
}

/// Remove paired `delim`…`delim` markers, keeping the (non-empty) inner text.
fn strip_paired(s: &str, delim: &str) -> String {
    let mut out = String::new();
    let mut rest = s;
    loop {
        if let Some(open) = rest.find(delim) {
            let after = &rest[open + delim.len()..];
            match after.find(delim) {
                Some(close) if close > 0 => {
                    out.push_str(&rest[..open]);
                    out.push_str(&after[..close]);
                    rest = &after[close + delim.len()..];
                }
                _ => {
                    out.push_str(&rest[..open + delim.len()]);
                    rest = after;
                }
            }
        } else {
            out.push_str(rest);
            break;
        }
    }
    out
}

/// Replace `[label](url)` with `label`.
fn strip_links(s: &str) -> String {
    let mut out = String::new();
    let mut rest = s;
    while let Some(open) = rest.find('[') {
        let after = &rest[open + 1..];
        if let Some(close) = after.find(']') {
            let label = &after[..close];
            let tail = &after[close + 1..];
            if !label.contains('[') && tail.starts_with('(') {
                if let Some(paren) = tail[1..].find(')') {
                    out.push_str(&rest[..open]);
                    out.push_str(label);
                    rest = &tail[1 + paren + 1..];
                    continue;
                }
            }
        }
        out.push_str(&rest[..=open]);
        rest = &rest[open + 1..];
    }
    out.push_str(rest);
    out
}

/// Strip a leading `#{1,6}\s+` from each line.
fn strip_leading_headers(s: &str) -> String {
    s.split('\n')
        .map(|line| {
            let hashes = line.bytes().take_while(|b| *b == b'#').count();
            if (1..=6).contains(&hashes) {
                let after = &line[hashes..];
                let trimmed = after.trim_start_matches(is_js_space_char);
                if trimmed.len() != after.len() {
                    return trimmed.to_string();
                }
            }
            line.to_string()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// File extension (with leading dot) for a mime type (default `.png`), the
/// inverse of [`mime_from_extension`], per the TS `getExtensionFromMimeType`.
pub(crate) fn extension_from_mime(mime_type: &str) -> &'static str {
    match mime_type {
        "image/jpeg" | "image/jpg" => ".jpg",
        "image/gif" => ".gif",
        "image/webp" => ".webp",
        "image/svg+xml" => ".svg",
        "image/bmp" => ".bmp",
        "image/tiff" => ".tiff",
        _ => ".png",
    }
}

/// Strip an optional `data:<mime>;base64,` URL prefix from an asset payload,
/// mirroring the TS `data.replace(/^data:[^;]+;base64,/, '')`.
pub(crate) fn strip_data_url_prefix(data: &str) -> &str {
    if let Some(rest) = data.strip_prefix("data:") {
        if let Some((mime, payload)) = rest.split_once(";base64,") {
            if !mime.contains(';') {
                return payload;
            }
        }
    }
    data
}

/// Mint a unique asset id `<timestamp36>-<hash8><ext>`, the TS peer's
/// `${Date.now().toString(36)}-${contentHash}${extension}` shape (the 8-char
/// content-hash fragment is an opaque uniqueness hint, not a stable digest).
pub(crate) fn new_asset_id(base64_data: &str, mime_type: &str) -> String {
    use std::hash::{Hash, Hasher};
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis());
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    base64_data.hash(&mut hasher);
    let hash8 = format!("{:016x}", hasher.finish())[..8].to_string();
    format!(
        "{}-{}{}",
        to_base36(millis),
        hash8,
        extension_from_mime(mime_type)
    )
}

/// Lowercase base-36 rendering of `n` (JS `Number.prototype.toString(36)`).
fn to_base36(mut n: u128) -> String {
    const DIGITS: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyz";
    if n == 0 {
        return "0".to_string();
    }
    let mut out = Vec::new();
    while n > 0 {
        out.push(DIGITS[(n % 36) as usize]);
        n /= 36;
    }
    out.reverse();
    String::from_utf8(out).unwrap_or_default()
}

/// Mime type from an asset's extension (default `image/png`), per the TS map.
pub(crate) fn mime_from_extension(asset_id: &str) -> String {
    let ext = asset_id
        .rsplit_once('.')
        .map(|(_, e)| e.to_ascii_lowercase())
        .unwrap_or_default();
    match ext.as_str() {
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "svg" => "image/svg+xml",
        "bmp" => "image/bmp",
        "tiff" => "image/tiff",
        _ => "image/png",
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_end_start_and_after_heading() {
        let (end, info) = apply_add("# A\nbody", "more", None, Some("end")).unwrap();
        assert_eq!(end, "# A\nbody\n\nmore");
        assert_eq!(info, "at end");

        let (start, info) = apply_add("# A", "intro", None, Some("start")).unwrap();
        assert_eq!(start, "intro\n\n# A");
        assert_eq!(info, "at start");

        // Default (no position) appends at end.
        let (def, info) = apply_add("x", "y", None, None).unwrap();
        assert_eq!(def, "x\n\ny");
        assert_eq!(info, "at end");

        // Heading + content compose the section.
        let (h, _) = apply_add("", "body", Some("## H"), Some("end")).unwrap();
        assert_eq!(h, "\n\n## H\n\nbody");
    }

    #[test]
    fn add_after_heading_inserts_before_next_heading() {
        let old = "## One\nalpha\n## Two\nbeta";
        let (new, info) = apply_add(old, "inserted", None, Some("after:## One")).unwrap();
        assert_eq!(new, "## One\nalpha\n\ninserted\n## Two\nbeta");
        assert_eq!(info, "after \"## One\"");

        // Last section: appends at end.
        let (new2, _) = apply_add(old, "tail", None, Some("after:## Two")).unwrap();
        assert_eq!(new2, "## One\nalpha\n## Two\nbeta\n\ntail");
    }

    #[test]
    fn add_after_missing_heading_and_bad_position_error() {
        assert!(apply_add("text", "x", None, Some("after:## Nope")).is_err());
        assert!(apply_add("text", "x", None, Some("sideways")).is_err());
    }

    #[test]
    fn edit_replaces_first_match_only() {
        let (new, pos, empty) = apply_edit("aXbXc", "X", "-").unwrap();
        assert_eq!(new, "a-bXc");
        assert_eq!(pos, 1);
        assert!(!empty);
    }

    #[test]
    fn edit_on_empty_note_sets_content() {
        let (new, pos, empty) = apply_edit("", "ignored", "fresh").unwrap();
        assert_eq!(new, "fresh");
        assert_eq!(pos, -1);
        assert!(empty);
    }

    #[test]
    fn edit_no_match_errors() {
        assert!(apply_edit("hello world", "zzz", "x").is_err());
    }

    #[test]
    fn edit_lines_replace_delete_insert() {
        let old = "l1\nl2\nl3\nl4";
        // Replace lines 2..=3.
        assert_eq!(apply_edit_lines(old, 2, 3, "R").unwrap(), "l1\nR\nl4");
        // Delete lines 2..=3 (empty content).
        assert_eq!(apply_edit_lines(old, 2, 3, "").unwrap(), "l1\nl4");
        // Replace a single line with multiple lines (insert).
        assert_eq!(
            apply_edit_lines(old, 2, 2, "a\nb").unwrap(),
            "l1\na\nb\nl3\nl4"
        );
    }

    #[test]
    fn edit_lines_validates_bounds() {
        assert!(apply_edit_lines("a\nb", 0, 1, "x").is_err());
        assert!(apply_edit_lines("a\nb", 2, 1, "x").is_err());
        assert!(apply_edit_lines("a\nb", 1, 9, "x").is_err());
        // Empty note: returns content verbatim.
        assert_eq!(apply_edit_lines("", 1, 1, "seed").unwrap(), "seed");
    }

    #[test]
    fn clean_set_content_strips_quotes_and_guards() {
        assert_eq!(clean_set_content("\"hello\"").unwrap(), "hello");
        assert!(clean_set_content("   ").is_err());
        assert!(clean_set_content("short...").is_err());
        assert_eq!(
            clean_set_content("# normal\ncontent").unwrap(),
            "# normal\ncontent"
        );
    }

    #[test]
    fn parse_tasks_reads_checkboxes_and_links() {
        let content = concat!(
            "- [ ] todo item\n",
            "* [x] done item\n",
            "- [/] wip item\n",
            "  - [X] [Linked](intent://local/task/abc-123)\n",
            "not a task\n",
            "- [ ] with <!--agent:meta--> comment"
        );
        let tasks = parse_tasks(content);
        assert_eq!(tasks.len(), 5);
        assert_eq!(tasks[0].status, "todo");
        assert_eq!(tasks[0].text, "todo item");
        assert!(tasks[0].task_note_id.is_none());
        assert_eq!(tasks[1].status, "done");
        assert_eq!(tasks[2].status, "in-progress");
        assert_eq!(tasks[3].status, "done");
        assert_eq!(tasks[3].text, "Linked");
        assert_eq!(tasks[3].task_note_id.as_deref(), Some("abc-123"));
        assert_eq!(tasks[3].line_number, 4);
        assert_eq!(tasks[4].text, "with  comment");
    }

    #[test]
    fn task_status_primary_exact_match() {
        let content = "- [ ] alpha\n- [x] beta\n  - [/] gamma";
        // Primary: exact text match flips the checkbox, leaving others intact.
        let out = apply_task_status(content, "alpha", "[x]").unwrap();
        assert_eq!(out, "- [x] alpha\n- [x] beta\n  - [/] gamma");
        // The nested in-progress task can be set done by exact text.
        let out2 = apply_task_status(content, "gamma", "[x]").unwrap();
        assert_eq!(out2, "- [ ] alpha\n- [x] beta\n  - [x] gamma");
    }

    #[test]
    fn task_status_not_found_errors() {
        assert!(apply_task_status("- [ ] alpha", "missing", "[x]").is_err());
    }

    #[test]
    fn task_status_fallback_contains() {
        // No exact match, but the line contains the text → first match flips.
        let content = "- [ ] alpha beta gamma";
        let out = apply_task_status(content, "beta", "[x]").unwrap();
        assert_eq!(out, "- [x] alpha beta gamma");
    }

    #[test]
    fn task_line_update_status_text_and_conflict() {
        let content = "intro\n- [ ] do the thing\ntail";
        // Status-only update keeps the text.
        let u = apply_task_line_update(content, 2, None, Some("done"), None).unwrap();
        assert_eq!(u.content, "intro\n- [x] do the thing\ntail");
        assert_eq!(u.previous_text, "do the thing");
        assert_eq!(u.new_text, "do the thing");
        assert_eq!(u.status_word, "done");
        // Text update keeps the (todo) status.
        let u2 = apply_task_line_update(content, 2, Some("new text"), None, None).unwrap();
        assert_eq!(u2.content, "intro\n- [ ] new text\ntail");
        assert_eq!(u2.status_word, "todo");
        // Conflict: expected mismatches the current text.
        let err =
            apply_task_line_update(content, 2, None, Some("done"), Some("other")).unwrap_err();
        assert!(err.to_string().contains("Conflict detected"));
    }

    #[test]
    fn task_line_update_rejects_non_task_and_oob() {
        assert!(apply_task_line_update("plain line", 1, None, Some("done"), None).is_err());
        assert!(apply_task_line_update("- [ ] x", 5, None, Some("done"), None).is_err());
    }

    #[test]
    fn anchor_unique_match() {
        let content = "Hello world, this is a test.";
        let (from, to, line) = find_and_anchor_text(content, "this is a test", "test").unwrap();
        assert_eq!(&content[from..to], "test");
        assert_eq!(line, 1);
    }

    #[test]
    fn anchor_ambiguous_context_errors() {
        let content = "repeat repeat";
        let err = find_and_anchor_text(content, "repeat", "repeat").unwrap_err();
        assert!(err
            .to_string()
            .contains("appears multiple times in the document"));
    }

    #[test]
    fn anchor_context_not_found_and_target_errors() {
        let content = "the quick brown fox";
        assert!(find_and_anchor_text(content, "missing context", "x")
            .unwrap_err()
            .to_string()
            .contains("Could not find the search context"));
        assert!(find_and_anchor_text(content, "quick brown", "zzz")
            .unwrap_err()
            .to_string()
            .contains("was not found within the search context"));
        // Target appears twice within the (unique) context.
        let content2 = "x ab ab y unique-tail";
        assert!(find_and_anchor_text(content2, "ab ab y unique-tail", "ab")
            .unwrap_err()
            .to_string()
            .contains("appears multiple times within the search context"));
    }

    #[test]
    fn anchor_plaintext_fallback_strips_formatting() {
        // The FE derives the anchor from the tiptap document's *plain text*:
        // heading markers, bold delimiters, and link syntax are absent from
        // the needle even though the markdown source carries them.
        let content = "## Project Title\n\nThis has **bold text** and a [link label](https://example.com) inline.";
        let (from, to, line) = find_and_anchor_text(
            content,
            "This has bold text and a link label inline.",
            "bold text",
        )
        .unwrap();
        assert_eq!(&content[from..to], "bold text");
        assert_eq!(line, 3);
    }

    #[test]
    fn anchor_plaintext_fallback_crosses_block_boundary() {
        // tiptap `textBetween` joins blocks with no separator, so the plain
        // text has no `\n\n` where the markdown does.
        let content = "First paragraph ends here.\n\nSecond paragraph starts now.";
        let (from, to, _line) =
            find_and_anchor_text(content, "ends here.Second paragraph", "here.Second").unwrap();
        assert_eq!(&content[from..to], "here.\n\nSecond");
    }

    #[test]
    fn anchor_plaintext_fallback_heading_and_list_markers() {
        let content = "# Title\n\n- first item\n- second item\n";
        let (from, to, _line) =
            find_and_anchor_text(content, "Titlefirst itemsecond item", "second item").unwrap();
        assert_eq!(&content[from..to], "second item");
    }

    #[test]
    fn anchor_plaintext_fallback_ignores_anchor_markers() {
        // Existing comments embed `<!--anchor:…-->` markers into the note
        // markdown; the editor's plain text never contains them.
        let content = "alpha <!--anchor:c1:start-->beta<!--anchor:c1:end--> gamma delta";
        let (from, to, _line) = find_and_anchor_text(content, "beta gamma delta", "gamma").unwrap();
        assert_eq!(&content[from..to], "gamma");
    }

    #[test]
    fn anchor_plaintext_fallback_allows_span_over_existing_marker() {
        // Overlapping comment ranges are supported: a target whose mapped
        // source range swallows an existing anchor pair is anchored as-is,
        // markers included — the pairs interleave and each id still pins its
        // own markers.
        let content = "alpha <!--anchor:11111111-2222-3333-4444-555555555555:start-->beta\
                       <!--anchor:11111111-2222-3333-4444-555555555555:end--> gamma delta";
        let (from, to, _line) = find_and_anchor_text(content, "beta gamma delta", "beta gamma")
            .expect("span over an existing marker must anchor");
        assert!(content[from..to].starts_with("beta"));
        assert!(content[from..to].ends_with("gamma"));
        assert!(content[from..to].contains(":end-->"));
    }

    #[test]
    fn anchor_plaintext_fallback_allows_span_over_doc_literal_marker() {
        // A documentation-literal marker (non-UUID id) is ordinary user
        // content: a span containing it anchors fine and keeps the literal.
        let content = "alpha `<!--anchor:{id}:start-->` gamma delta";
        let (from, to, _line) = find_and_anchor_text(content, "alpha gamma delta", "alpha gamma")
            .expect("doc-literal marker must not trip the overlap guard");
        assert!(content[from..to].contains("<!--anchor:{id}:start-->"));
    }

    #[test]
    fn anchor_plaintext_fallback_multibyte_target_boundaries() {
        // Multi-byte characters at the end of the target: the mapped `to`
        // must land on the char's end boundary, not mid-codepoint.
        let content = "# Título\n\nUne **phrase où ça** finit là";
        let (from, to, _line) =
            find_and_anchor_text(content, "TítuloUne phrase où ça finit là", "où ça").unwrap();
        assert_eq!(&content[from..to], "où ça");
    }

    #[test]
    fn anchor_plaintext_fallback_preserves_ambiguity_rules() {
        let content = "**dup** text\n\n**dup** text";
        let err = find_and_anchor_text(content, "dup text", "dup").unwrap_err();
        assert!(
            matches!(err, Error::InvalidParams(ref m) if m.contains("appears multiple times in the document")),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn anchor_plaintext_fallback_mention_at_prefix() {
        // Real-world repro (2026-07-23, round 4): editor clients render bare
        // filenames as mention chips whose canonical text carries a leading
        // `@` the markdown source never had (`KNOWN_ISSUES.md` in the source,
        // `@KNOWN_ISSUES.md` in the extracted needle). `@` is normalized away
        // on both sides so the needle still matches.
        let content =
            "issue filed+closed; KNOWN_ISSUES.md was retired on main in favor of GitHub issues.";
        let (from, to, _line) = find_and_anchor_text(
            content,
            "issue filed+closed; @KNOWN_ISSUES.md was retired on main",
            "@KNOWN_ISSUES.md was retired",
        )
        .unwrap();
        assert_eq!(&content[from..to], "KNOWN_ISSUES.md was retired");
    }

    #[test]
    fn anchor_plaintext_fallback_at_ambiguity_fails_closed() {
        // With `@` normalized away, a fallback needle that used to match only
        // the literal `@alice` occurrence now also matches the bare `alice`
        // one — the ambiguity guard fails closed (error, never a wrong
        // anchor). Emphasis in the source keeps the exact-match path from
        // short-circuiting so the plaintext fallback is exercised.
        let content = "Ping **@alice** for details.\n\nPing **alice** for details.";
        let err = find_and_anchor_text(content, "Ping @alice for details", "@alice").unwrap_err();
        assert!(
            matches!(err, Error::InvalidParams(ref m) if m.contains("appears multiple times")),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn anchor_plaintext_fallback_literal_at_still_matches() {
        // A literal `@` present in BOTH the markdown and the needle keeps
        // matching after the symmetric drop.
        let content = "Contact user@example.com for access to the beta.";
        let (from, to, _line) = find_and_anchor_text(
            content,
            "Contact user@example.com for access",
            "user@example.com",
        )
        .unwrap();
        assert_eq!(&content[from..to], "user@example.com");
    }

    // Real-world dogfood repro (2026-07-22): the note gained a new paragraph
    // between `## Goal` and `## Diagnosis` on the server, but the editor doc
    // was still on the previous revision. The user selected across the
    // heading/paragraph boundary; the ±50-char context was built from the
    // *stale* plain text, so the full search context no longer exists in the
    // current markdown even though the selected text itself does.
    const STALE_CTX_MD: &str = "# Fix \"Failed to add comment\"\n\n## Goal\nAdding a comment succeeds instead of failing with an opaque \"Internal error\" toast.\n\n**Status: COMPLETE (2026-07-22).** All three tasks verified and merged — dogfood after restarting the app on the new build.\n\n## Diagnosis (confirmed in code)\n\n**Symptom:** Toast \"Failed to add comment / Internal error\" when adding a comment in the note editor. Nothing in the daemon log or FE console log.\n\n**Root cause — plaintext vs markdown mismatch.** The FE builds the anchor params from plain text.";

    const STALE_CTX_SELECTED: &str = "Diagnosis (confirmed in code)Symptom: Toast \"Failed to add comment / Internal error\" when adding a comment in the note editor. Nothing in the daemon log or FE console log.";
    // Tail of the *stale* revision's plain text (the Goal paragraph directly
    // preceded the Diagnosis heading before the Status paragraph landed).
    const STALE_CTX_BEFORE: &str = "of failing with an opaque \"Internal error\" toast.";
    const STALE_CTX_AFTER: &str = "Root cause — plaintext vs markdown mismatch. The F";

    #[test]
    fn anchor_target_rescue_stale_context_zero_join() {
        // tiptap `doc.textBetween` with no block separator: blocks join with
        // nothing. The stale before-context is not adjacent to the selection
        // in the current markdown, so the full-context search fails; the
        // target itself is unique and must still anchor.
        let ctx = format!("{STALE_CTX_BEFORE}{STALE_CTX_SELECTED}{STALE_CTX_AFTER}");
        let (from, to, line) =
            find_and_anchor_text(STALE_CTX_MD, &ctx, STALE_CTX_SELECTED).unwrap();
        assert!(STALE_CTX_MD[from..].starts_with("Diagnosis (confirmed in code)"));
        assert!(STALE_CTX_MD[..to].ends_with("FE console log."));
        assert_eq!(line, 8);
    }

    #[test]
    fn anchor_target_rescue_stale_context_space_join() {
        // Same case with single-space block joins (textBetween with a ' '
        // block separator).
        let selected = STALE_CTX_SELECTED.replace(")Symptom", ") Symptom");
        let ctx = format!("{STALE_CTX_BEFORE} {selected} {STALE_CTX_AFTER}");
        let (from, to, _line) = find_and_anchor_text(STALE_CTX_MD, &ctx, &selected).unwrap();
        assert!(STALE_CTX_MD[from..].starts_with("Diagnosis (confirmed in code)"));
        assert!(STALE_CTX_MD[..to].ends_with("FE console log."));
    }

    #[test]
    fn anchor_target_rescue_disambiguates_by_partial_context() {
        // Target appears twice; the stale context is gone but its after side
        // still matches only the second occurrence.
        let content = "alpha token beta\n\nnew paragraph\n\ngamma token delta";
        let (from, to, _line) =
            find_and_anchor_text(content, "vanished context token delta", "token").unwrap();
        assert_eq!(from, content.rfind("token").unwrap());
        assert_eq!(&content[from..to], "token");
    }

    #[test]
    fn anchor_target_rescue_ambiguous_target_errors() {
        // Target appears twice and the surviving context matches neither
        // occurrence better — must stay an error, not guess.
        let content = "alpha token beta\n\ngamma token beta";
        let err = find_and_anchor_text(content, "vanished token beta", "token").unwrap_err();
        assert!(
            matches!(err, Error::InvalidParams(ref m) if m.contains("appears multiple times")),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn anchor_target_rescue_missing_target_still_not_found() {
        // Target is inside the (stale) context but absent from the document:
        // nothing to rescue, so the context-not-found error stands.
        let err = find_and_anchor_text("some document text", "stale missing here", "missing")
            .unwrap_err();
        assert!(
            matches!(err, Error::InvalidParams(ref m) if m.contains("Could not find the search context")),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn anchor_rescue_target_outside_context_reports_target_error() {
        // Context missing AND target not contained in it: the error names the
        // target relationship, consistent with the in-context path.
        let err =
            find_and_anchor_text("alpha token beta", "vanished context here", "token").unwrap_err();
        assert!(
            matches!(err, Error::InvalidParams(ref m) if m.contains("comment target was not found")),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn anchor_rescue_empty_target_reports_target_error() {
        // Context missing AND target empty: the error names the target
        // relationship, consistent with the in-context path.
        let err = find_and_anchor_text("some document text", "stale context here", "").unwrap_err();
        assert!(
            matches!(err, Error::InvalidParams(ref m) if m.contains("comment target was not found")),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn anchor_target_rescue_requires_min_context_overlap() {
        // Target exists exactly once, but only 3 bytes of the stale context
        // survive around it (< MIN_RESCUE_CONTEXT_OVERLAP) — must stay
        // not-found rather than anchor on a near-bare target match.
        let content = "xyz token pqr";
        let err = find_and_anchor_text(content, "abc token dqr", "token").unwrap_err();
        assert!(
            matches!(err, Error::InvalidParams(ref m) if m.contains("Could not find the search context")),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn anchor_target_rescue_ambiguous_target_in_context_errors() {
        // The target appears twice within the provided (stale) context: the
        // rescue must reject like the exact/projection paths instead of
        // splitting the context at the first occurrence.
        let content = "alpha token beta\n\nsomething else entirely";
        let err = find_and_anchor_text(content, "token stale token beta", "token").unwrap_err();
        assert!(
            matches!(err, Error::InvalidParams(ref m) if m.contains("appears multiple times within the search context")),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn anchor_target_rescue_allows_span_over_existing_marker() {
        // Rescue output may also span an existing pair; overlap is supported.
        let content = "gone before <!--anchor:11111111-2222-3333-4444-555555555555:start-->beta\
                       <!--anchor:11111111-2222-3333-4444-555555555555:end--> gamma tail";
        let (from, to, _line) =
            find_and_anchor_text(content, "vanished before beta gamma tail", "beta gamma")
                .expect("rescued span over an existing marker must anchor");
        assert!(content[from..to].starts_with("beta"));
        assert!(content[from..to].ends_with("gamma"));
    }

    #[test]
    fn anchor_target_rescue_multibyte_target_boundaries() {
        let content = "# Título\n\nUne **phrase où ça** finit là aujourd'hui.";
        let (from, to, _line) =
            find_and_anchor_text(content, "vanished stale phrase où ça finit", "où ça").unwrap();
        assert_eq!(&content[from..to], "où ça");
    }

    #[test]
    fn overlap_helpers_clamp_to_char_boundaries() {
        // 'é' (C3 A9) vs 'è' (C3 A8): the shared lead byte must not count.
        assert_eq!(prefix_overlap("éx", "èx"), 0);
        assert_eq!(suffix_overlap("xé", "xè"), 0);
        assert_eq!(prefix_overlap("éx", "éy"), 'é'.len_utf8());
        assert_eq!(suffix_overlap("xé", "yé"), 'é'.len_utf8());
    }

    #[test]
    fn anchor_failures_are_invalid_params() {
        assert!(matches!(
            find_and_anchor_text("abc", "zzz", "z").unwrap_err(),
            Error::InvalidParams(_)
        ));
        assert!(matches!(
            find_and_anchor_text("repeat repeat", "repeat", "repeat").unwrap_err(),
            Error::InvalidParams(_)
        ));
        assert!(matches!(
            find_and_anchor_text("quick brown", "quick brown", "zzz").unwrap_err(),
            Error::InvalidParams(_)
        ));
        assert!(matches!(
            find_and_anchor_text("x ab ab y tail", "ab ab y tail", "ab").unwrap_err(),
            Error::InvalidParams(_)
        ));
    }

    #[test]
    fn extract_task_blocks_parses_and_placeholders() {
        let content =
            "intro\n@@@task\n# First\nbody one\n@@@\nmid\n@@@task\nno title here\n@@@\ntail";
        let result = extract_task_blocks(content);
        assert_eq!(result.tasks.len(), 1);
        assert_eq!(result.tasks[0].title, "First");
        assert_eq!(result.tasks[0].content, "body one");
        assert!(result
            .content_without_blocks
            .contains("<!-- task-block-placeholder-0 -->"));
        assert!(result
            .content_without_blocks
            .contains("<!-- invalid-task-block-removed -->"));
        assert!(has_task_blocks(content));
        assert!(!has_task_blocks("no blocks here"));
    }

    #[test]
    fn bare_header_has_no_attributes_or_issues() {
        let result = extract_task_blocks("@@@task\n# T\nbody\n@@@");
        assert_eq!(result.tasks.len(), 1);
        let t = &result.tasks[0];
        assert_eq!(t.key, None);
        assert!(t.depends_on.is_empty());
        assert!(t.conflicts_with.is_empty());
        assert_eq!(t.effort, None);
        assert!(t.issues.is_empty());
    }

    #[test]
    fn header_attr_key_alone() {
        let result = extract_task_blocks("@@@task key=t1\n# T\nbody\n@@@");
        assert_eq!(result.tasks.len(), 1);
        let t = &result.tasks[0];
        assert_eq!(t.key.as_deref(), Some("t1"));
        assert!(t.depends_on.is_empty());
        assert!(t.conflicts_with.is_empty());
        assert_eq!(t.effort, None);
        assert!(t.issues.is_empty());
        assert_eq!(t.title, "T");
        assert_eq!(t.content, "body");
        assert!(result
            .content_without_blocks
            .contains("<!-- task-block-placeholder-0 -->"));
    }

    #[test]
    fn header_attr_depends_on_list() {
        let result = extract_task_blocks("@@@task dependsOn=t1,t2,t3\n# T\n@@@");
        assert_eq!(result.tasks[0].depends_on, vec!["t1", "t2", "t3"]);
        assert!(result.tasks[0].issues.is_empty());
    }

    #[test]
    fn header_attr_conflicts_with_list() {
        let result = extract_task_blocks("@@@task conflictsWith=a,b\n# T\n@@@");
        assert_eq!(result.tasks[0].conflicts_with, vec!["a", "b"]);
        assert!(result.tasks[0].issues.is_empty());
    }

    #[test]
    fn header_attr_effort_alone() {
        let result = extract_task_blocks("@@@task effort=2h\n# T\n@@@");
        assert_eq!(result.tasks[0].effort.as_deref(), Some("2h"));
        assert!(result.tasks[0].issues.is_empty());
    }

    #[test]
    fn header_attrs_combined() {
        let content = "@@@task key=t3 dependsOn=t1,t2 conflictsWith=t4 effort=30m\n# T\nbody\n@@@";
        let result = extract_task_blocks(content);
        let t = &result.tasks[0];
        assert_eq!(t.key.as_deref(), Some("t3"));
        assert_eq!(t.depends_on, vec!["t1", "t2"]);
        assert_eq!(t.conflicts_with, vec!["t4"]);
        assert_eq!(t.effort.as_deref(), Some("30m"));
        assert!(t.issues.is_empty());
        assert!(has_task_blocks(content));
    }

    #[test]
    fn header_attrs_whitespace_tolerant() {
        let content = "@@@task \t key=t1 \t dependsOn=a, b ,c , d\teffort=1d \n# T\n@@@";
        let result = extract_task_blocks(content);
        let t = &result.tasks[0];
        assert_eq!(t.key.as_deref(), Some("t1"));
        assert_eq!(t.depends_on, vec!["a", "b", "c", "d"]);
        assert_eq!(t.effort.as_deref(), Some("1d"));
        assert!(t.issues.is_empty());
    }

    #[test]
    fn header_attrs_on_tasks_variant_and_crlf() {
        let result = extract_task_blocks("@@@tasks key=t1 dependsOn=a,b\r\n# T\r\nbody\r\n@@@");
        assert_eq!(result.tasks.len(), 1);
        let t = &result.tasks[0];
        assert_eq!(t.key.as_deref(), Some("t1"));
        assert_eq!(t.depends_on, vec!["a", "b"]);
        assert!(t.issues.is_empty());
    }

    #[test]
    fn header_unknown_attribute_is_issue_not_rejection() {
        let result = extract_task_blocks("@@@task key=t1 priority=high\n# T\n@@@");
        assert_eq!(result.tasks.len(), 1);
        let t = &result.tasks[0];
        assert_eq!(t.key.as_deref(), Some("t1"));
        assert_eq!(t.issues.len(), 1);
        assert!(t.issues[0].contains("unknown attribute `priority`"));
    }

    #[test]
    fn header_attribute_names_are_case_sensitive() {
        let result = extract_task_blocks("@@@task dependson=t1\n# T\n@@@");
        assert_eq!(result.tasks.len(), 1);
        let t = &result.tasks[0];
        assert!(t.depends_on.is_empty());
        assert_eq!(t.issues.len(), 1);
        assert!(t.issues[0].contains("unknown attribute `dependson`"));
    }

    #[test]
    fn header_empty_and_duplicate_values_are_issues() {
        let result = extract_task_blocks("@@@task key= effort=1h effort=2h dependsOn=,\n# T\n@@@");
        assert_eq!(result.tasks.len(), 1);
        let t = &result.tasks[0];
        assert_eq!(t.key, None);
        assert_eq!(t.effort.as_deref(), Some("1h"));
        assert!(t.depends_on.is_empty());
        assert_eq!(t.issues.len(), 3);
        assert!(t.issues[0].contains("empty value for attribute `key`"));
        assert!(t.issues[1].contains("duplicate attribute `effort`"));
        assert!(t.issues[2].contains("empty value for attribute `dependsOn`"));
    }

    #[test]
    fn header_duplicate_after_empty_first_value_is_not_accepted() {
        // Duplicates are keyed on attribute-name occurrence: a later value
        // never silently fills a slot the first (empty) occurrence left unset.
        let result = extract_task_blocks("@@@task key= key=second\n# T\n@@@");
        assert_eq!(result.tasks.len(), 1);
        let t = &result.tasks[0];
        assert_eq!(t.key, None);
        assert_eq!(t.issues.len(), 2);
        assert!(t.issues[0].contains("empty value for attribute `key`"));
        assert!(t.issues[1].contains("duplicate attribute `key`"));

        let result = extract_task_blocks("@@@task dependsOn= dependsOn=a\n# T\n@@@");
        assert_eq!(result.tasks.len(), 1);
        let t = &result.tasks[0];
        assert!(t.depends_on.is_empty());
        assert_eq!(t.issues.len(), 2);
        assert!(t.issues[0].contains("empty value for attribute `dependsOn`"));
        assert!(t.issues[1].contains("duplicate attribute `dependsOn`"));
    }

    #[test]
    fn header_multiple_trailing_crs_stay_non_fence() {
        // Main consumed at most one `\r` before requiring `\n`; extra CRs
        // must keep disqualifying the fence.
        assert!(!has_task_blocks("@@@task\r\r\n# T\n@@@"));
        assert!(!has_task_blocks("@@@task \r\r\n# T\n@@@"));
        assert!(!has_task_blocks("@@@task key=a\r\r\n# T\n@@@"));
        assert!(has_task_blocks("@@@task\r\n# T\n@@@"));
    }

    #[test]
    fn header_stray_comma_gluing_attributes_is_flagged() {
        // A comma between attributes glues the next `name=value` into the
        // previous value; that must surface as an issue, never silently.
        let result = extract_task_blocks("@@@task dependsOn=a, key=b\n# T\n@@@");
        assert_eq!(result.tasks.len(), 1);
        let t = &result.tasks[0];
        assert_eq!(t.depends_on, vec!["a"]);
        assert_eq!(t.key, None);
        assert_eq!(t.issues.len(), 1);
        assert!(t.issues[0].contains("malformed item `key=b` in attribute `dependsOn`"));

        let result = extract_task_blocks("@@@task key=a ,dependsOn=b\n# T\n@@@");
        assert_eq!(result.tasks.len(), 1);
        let t = &result.tasks[0];
        assert_eq!(t.key, None);
        assert!(t.depends_on.is_empty());
        assert_eq!(t.issues.len(), 1);
        assert!(t.issues[0].contains("malformed value `a,dependsOn=b` for attribute `key`"));
    }

    #[test]
    fn header_scalar_values_reject_commas_and_equals() {
        let result = extract_task_blocks("@@@task key=a,b effort=1h,2h\n# T\n@@@");
        assert_eq!(result.tasks.len(), 1);
        let t = &result.tasks[0];
        assert_eq!(t.key, None);
        assert_eq!(t.effort, None);
        assert_eq!(t.issues.len(), 2);
        assert!(t.issues[0].contains("malformed value `a,b` for attribute `key`"));
        assert!(t.issues[1].contains("malformed value `1h,2h` for attribute `effort`"));
    }

    #[test]
    fn header_free_text_is_not_a_fence() {
        // Non-attribute-shaped trailing text keeps the pre-attribute behavior:
        // the line is not a fence at all.
        assert!(!has_task_blocks("@@@task something\n# T\n@@@"));
        assert!(!has_task_blocks("@@@task =x\n# T\n@@@"));
        assert!(!has_task_blocks("@@@task key=a extra words\n# T\n@@@"));
        assert!(!has_task_blocks("@@@taskkey=a\n# T\n@@@"));
        let result = extract_task_blocks("@@@task something\n# T\n@@@");
        assert!(result.tasks.is_empty());
        assert_eq!(result.content_without_blocks, "@@@task something\n# T\n@@@");
    }

    #[test]
    fn strip_markdown_basic() {
        assert_eq!(strip_markdown_formatting("**Bold** title"), "Bold title");
        assert_eq!(strip_markdown_formatting("# Heading"), "Heading");
        assert_eq!(strip_markdown_formatting("[link](http://x)"), "link");
        assert_eq!(strip_markdown_formatting("plain"), "plain");
    }

    #[test]
    fn asset_id_parsing_and_mime() {
        assert_eq!(parse_asset_id("abc.png").unwrap(), "abc.png");
        assert_eq!(
            parse_asset_id("workspace-asset://ws-1/img.jpg").unwrap(),
            "img.jpg"
        );
        assert!(parse_asset_id("workspace-asset://ws-1/").is_err());
        assert_eq!(mime_from_extension("a.PNG"), "image/png");
        assert_eq!(mime_from_extension("a.jpeg"), "image/jpeg");
        assert_eq!(mime_from_extension("noext"), "image/png");
    }

    #[test]
    #[allow(clippy::case_sensitive_file_extension_comparisons)] // extensions generated by our own code with fixed case
    fn save_asset_helpers() {
        assert_eq!(extension_from_mime("image/jpeg"), ".jpg");
        assert_eq!(extension_from_mime("image/webp"), ".webp");
        assert_eq!(extension_from_mime("application/pdf"), ".png");

        assert_eq!(strip_data_url_prefix("data:image/png;base64,AAAA"), "AAAA");
        assert_eq!(strip_data_url_prefix("AAAA"), "AAAA");

        let id = new_asset_id("AAAA", "image/png");
        assert!(id.ends_with(".png"), "unexpected id: {id}");
        let stem = id.strip_suffix(".png").unwrap();
        let (ts, hash) = stem.split_once('-').expect("timestamp-hash shape");
        assert!(!ts.is_empty() && ts.chars().all(|c| c.is_ascii_alphanumeric()));
        assert_eq!(hash.len(), 8);
        // Round-trips through the read-side mime mapping.
        assert_eq!(mime_from_extension(&id), "image/png");
    }

    // -----------------------------------------------------------------------
    // Comment anchor context + recovery.
    // -----------------------------------------------------------------------

    fn wrap(id: &str, before: &str, anchored: &str, after: &str) -> String {
        format!("{before}<!--anchor:{id}:start-->{anchored}<!--anchor:{id}:end-->{after}")
    }

    #[test]
    fn context_before_after_are_char_bounded() {
        let content = "hello world foo bar baz";
        let anchor_start = content.find("foo").unwrap();
        let anchor_end = anchor_start + "foo".len();
        assert_eq!(context_before(content, anchor_start), "hello world ");
        assert_eq!(context_after(content, anchor_end), " bar baz");
    }

    #[test]
    fn context_before_after_are_utf8_safe() {
        // Mix of multi-byte glyphs (é = 2 bytes, 你 = 3 bytes, 😀 = 4 bytes)
        // exercises the char-boundary logic in context_before / context_after
        // — a byte-indexed slice here would panic or split a codepoint.
        let content = "café 你好 😀 target 世界 après 🎉";
        let anchor_start = content.find("target").unwrap();
        let anchor_end = anchor_start + "target".len();
        let before = context_before(content, anchor_start);
        let after = context_after(content, anchor_end);
        assert!(
            content.starts_with(&before),
            "before must be a suffix-prefix of content"
        );
        assert!(
            content.ends_with(&after),
            "after must be a prefix-suffix of content"
        );
        assert_eq!(before, "café 你好 😀 ");
        assert_eq!(after, " 世界 après 🎉");
        // And with a run longer than CONTEXT_LENGTH chars the slice is bounded
        // by char count (not byte count) and still ends on a char boundary.
        let long = "🎉".repeat(80) + "X" + &"🎉".repeat(80);
        let pos = long.find('X').unwrap();
        let before_long = context_before(&long, pos);
        let after_long = context_after(&long, pos + 1);
        assert_eq!(before_long.chars().count(), ANCHOR_CONTEXT_LEN);
        assert_eq!(after_long.chars().count(), ANCHOR_CONTEXT_LEN);
    }

    #[test]
    fn classify_healthy_partial_missing_degenerate() {
        let healthy = wrap("c1", "pre ", "target", " post");
        assert_eq!(classify_anchor_state(&healthy, "c1"), AnchorState::Healthy);
        let missing = "pre target post";
        assert_eq!(classify_anchor_state(missing, "c1"), AnchorState::Missing);
        let partial_start = "pre <!--anchor:c1:start-->target post";
        assert_eq!(
            classify_anchor_state(partial_start, "c1"),
            AnchorState::PartialStartOnly
        );
        let partial_end = "pre target<!--anchor:c1:end--> post";
        assert_eq!(
            classify_anchor_state(partial_end, "c1"),
            AnchorState::PartialEndOnly
        );
        let degenerate = "pre <!--anchor:c1:start--><!--anchor:c1:end--> post";
        assert_eq!(
            classify_anchor_state(degenerate, "c1"),
            AnchorState::Degenerate
        );
    }

    #[test]
    fn recover_missing_end_uses_context_after_neighbor() {
        // Start marker survives, end marker was deleted; the anchored word
        // ("target") is still followed by the original neighbor ("post").
        let markdown = "pre <!--anchor:c1:start-->target post";
        let out = recover_partial_anchor(markdown, "c1", Some("pre "), Some(" post"));
        let recovered = match out {
            RecoveryOutcome::Recovered(m) => m,
            other @ RecoveryOutcome::Failed(_) => panic!("expected Recovered, got {other:?}"),
        };
        assert!(
            recovered.contains("<!--anchor:c1:start-->target<!--anchor:c1:end--> post"),
            "unexpected recovery: {recovered}"
        );
        assert_eq!(
            classify_anchor_state(&recovered, "c1"),
            AnchorState::Healthy
        );
    }

    #[test]
    fn recover_missing_start_uses_context_before_neighbor() {
        // End marker survives, start marker was deleted; the anchored word
        // ("target") is still preceded by the original neighbor ("pre").
        let markdown = "pre target<!--anchor:c1:end--> post";
        let out = recover_partial_anchor(markdown, "c1", Some("pre "), Some(" post"));
        let recovered = match out {
            RecoveryOutcome::Recovered(m) => m,
            other @ RecoveryOutcome::Failed(_) => panic!("expected Recovered, got {other:?}"),
        };
        assert!(
            recovered.contains("pre <!--anchor:c1:start-->target<!--anchor:c1:end--> post"),
            "unexpected recovery: {recovered}"
        );
        assert_eq!(
            classify_anchor_state(&recovered, "c1"),
            AnchorState::Healthy
        );
    }

    #[test]
    fn recover_both_present_or_missing_is_not_attempted() {
        let both_present = wrap("c1", "pre ", "target", " post");
        assert!(matches!(
            recover_partial_anchor(&both_present, "c1", Some("pre "), Some(" post")),
            RecoveryOutcome::Failed(_)
        ));
        let both_missing = "pre target post";
        assert!(matches!(
            recover_partial_anchor(both_missing, "c1", Some("pre "), Some(" post")),
            RecoveryOutcome::Failed(_)
        ));
    }

    #[test]
    fn recover_partial_without_neighbor_context_fails() {
        // Partial anchor but no stored neighbor to search for.
        let markdown = "pre <!--anchor:c1:start-->target post";
        assert!(matches!(
            recover_partial_anchor(markdown, "c1", Some(""), Some("")),
            RecoveryOutcome::Failed(_)
        ));
    }

    #[test]
    fn remove_anchor_markers_strips_both_ends() {
        let markdown = wrap("c1", "pre ", "target", " post");
        let stripped = remove_anchor_markers(&markdown, "c1");
        assert_eq!(stripped, "pre target post");
        // No-op when nothing to strip.
        assert_eq!(remove_anchor_markers("plain", "c1"), "plain");
    }

    // -----------------------------------------------------------------------
    // Phantom-marker scrub.
    // -----------------------------------------------------------------------

    const LIVE_ID: &str = "11111111-2222-3333-4444-555555555555";
    const PHANTOM_ID: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";

    fn live_set(ids: &[&str]) -> std::collections::HashSet<String> {
        ids.iter().map(std::string::ToString::to_string).collect()
    }

    #[test]
    fn scrub_removes_phantom_markers_keeps_live() {
        let content = format!(
            "a <!--anchor:{LIVE_ID}:start-->b<!--anchor:{LIVE_ID}:end--> \
             c <!--anchor:{PHANTOM_ID}:start-->d<!--anchor:{PHANTOM_ID}:end--> \
             e <!--anchor:{PHANTOM_ID}:point--> f"
        );
        let out = scrub_phantom_anchor_markers(&content, &live_set(&[LIVE_ID]));
        assert_eq!(
            out,
            format!("a <!--anchor:{LIVE_ID}:start-->b<!--anchor:{LIVE_ID}:end--> c d e  f")
        );
    }

    #[test]
    fn scrub_never_touches_non_uuid_marker_lookalikes() {
        // Documentation literals (`{id}`, short ids, unknown roles) and
        // unterminated comments are user content — always preserved.
        let content = "see `<!--anchor:{id}:start-->` and <!--anchor:c1:end--> \
                       and <!--anchor:11111111-2222-3333-4444-555555555555:middle--> \
                       and <!--anchor:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:start";
        let out = scrub_phantom_anchor_markers(content, &live_set(&[]));
        assert_eq!(out, content);
    }

    #[test]
    fn scrub_empty_live_set_removes_all_uuid_markers() {
        let content = format!("x<!--anchor:{PHANTOM_ID}:start-->y<!--anchor:{PHANTOM_ID}:end-->z");
        let out = scrub_phantom_anchor_markers(&content, &live_set(&[]));
        assert_eq!(out, "xyz");
    }

    #[test]
    fn scrub_is_noop_without_markers() {
        let content = "plain text, no markers";
        assert_eq!(
            scrub_phantom_anchor_markers(content, &live_set(&[LIVE_ID])),
            content
        );
    }

    // -----------------------------------------------------------------------
    // Stored-text marker stripping.
    // -----------------------------------------------------------------------

    #[test]
    fn strip_anchor_marker_text_removes_all_markers() {
        let s = format!(
            "beta<!--anchor:{LIVE_ID}:end--> gamma <!--anchor:{{id}}:start--> delta \
             <!--anchor:{PHANTOM_ID}:point-->"
        );
        assert_eq!(strip_anchor_marker_text(&s), "beta gamma  delta ");
    }

    #[test]
    fn strip_anchor_marker_text_reaches_fixpoint() {
        // Removing the inner marker concatenates the halves of an outer one;
        // the fixpoint loop must catch the newly-formed marker too.
        let s = format!("a<!--anchor<!--anchor:{LIVE_ID}:end-->:x-->b");
        assert_eq!(strip_anchor_marker_text(&s), "ab");
    }

    #[test]
    fn strip_anchor_marker_text_keeps_unterminated_and_plain() {
        assert_eq!(strip_anchor_marker_text("plain"), "plain");
        let unterminated = "text <!--anchor:tail";
        assert_eq!(strip_anchor_marker_text(unterminated), unterminated);
    }
}
