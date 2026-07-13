//! Pure content-edit helpers ported from the TS `ws.note.*` peers
//! (`src/features/mcp/main/mcp/ws-note-api.ts`). These mirror the byte-for-byte
//! string semantics the iOS app depends on: `add` positions, first exact-match
//! `edit`, 1-based inclusive `editLines`, the `setContent` cleaner, checkbox
//! task parsing, and asset-id parsing. User-facing failures surface as
//! [`Error::Internal`] so the router maps them to `-32603` with the original
//! message in `data`, matching the TS handler.

use intent_core::{Error, NoteTaskRow, Result};

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
pub fn apply_add(
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
pub fn apply_edit(old: &str, old_text: &str, new_text: &str) -> Result<(String, i64, bool)> {
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
            let match_position = old[..idx].chars().count() as i64;
            Ok((new_content, match_position, false))
        }
    }
}

/// `note.editLines` — 1-based inclusive replace/delete/insert.
pub fn apply_edit_lines(old: &str, start: i64, end: i64, content: &str) -> Result<String> {
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
    let total = lines.len() as i64;
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
    result.extend_from_slice(&lines[..(start as usize - 1)]);
    if !content.is_empty() {
        result.extend(content.split('\n'));
    }
    result.extend_from_slice(&lines[end as usize..]);
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
pub fn clean_set_content(content: &str) -> Result<String> {
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
pub fn parse_tasks(content: &str) -> Vec<NoteTaskRow> {
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
        Some('-') | Some('*') => {}
        _ => return None,
    }
    while matches!(chars.peek(), Some(c) if is_js_space_char(*c)) {
        chars.next();
    }
    if chars.next() != Some('[') {
        return None;
    }
    let checkbox = match chars.next() {
        Some(c @ (' ' | 'x' | 'X' | '/')) => c,
        _ => return None,
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
pub fn parse_asset_id(asset: &str) -> Result<String> {
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
pub fn checkbox_for(word: &str) -> Option<&'static str> {
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
pub fn apply_task_status(content: &str, task_text: &str, checkbox: &str) -> Result<String> {
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
pub struct TaskLineUpdate {
    pub content: String,
    pub previous_text: String,
    pub new_text: String,
    pub status_word: String,
}

/// `task.update` — atomic single-line edit (1-based `line`) with an optional
/// `expected` conflict check. `status` is a validated word (`todo`/`in-progress`
/// /`done`) or `None` to keep the current status; `text` replaces the task text.
pub fn apply_task_line_update(
    content: &str,
    line: i64,
    text: Option<&str>,
    status: Option<&str>,
    expected: Option<&str>,
) -> Result<TaskLineUpdate> {
    let mut lines: Vec<String> = content.split('\n').map(str::to_string).collect();
    let total = lines.len() as i64;
    if line > total {
        return Err(Error::Internal(format!(
            "Line {line} does not exist. Note has {total} lines."
        )));
    }
    let current = lines[(line - 1) as usize].clone();
    let parsed = parse_dash_checkbox(&current);
    let (box_start, after_idx, cb) = match parsed {
        Some(v) => v,
        None => {
            let trunc: String = current.chars().take(50).collect();
            let ellipsis = if current.chars().count() > 50 {
                "..."
            } else {
                ""
            };
            return Err(Error::Internal(format!(
                "Line {line} is not a task. Expected format: \"- [ ] task text\". Found: \"{trunc}{ellipsis}\""
            )));
        }
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
    lines[(line - 1) as usize] = format!("{prefix}{checkbox} {final_text}");
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
/// occurrence. Returns `(from_byte, to_byte, line)`; errors carry the same
/// user-facing messages the TS handler throws (surfaced as `-32603` `data`).
pub fn find_and_anchor_text(
    content: &str,
    search_context: &str,
    comment_target: &str,
) -> Result<(usize, usize, usize)> {
    let ctx = find_all_occurrences(content, search_context);
    if ctx.is_empty() {
        return Err(Error::Internal(
            "Could not find the search context in the document.".to_string(),
        ));
    }
    if ctx.len() > 1 {
        return Err(Error::Internal(
            "The search context appears multiple times in the document.".to_string(),
        ));
    }
    let ctx_from = ctx[0];
    let rel = match search_context.find(comment_target) {
        Some(r) => r,
        None => {
            return Err(Error::Internal(
                "The comment target was not found within the search context.".to_string(),
            ))
        }
    };
    if count_occurrences(search_context, comment_target) > 1 {
        return Err(Error::Internal(
            "The comment target appears multiple times within the search context.".to_string(),
        ));
    }
    let from = ctx_from + rel;
    let to = from + comment_target.len();
    Ok((from, to, line_of(content, from)))
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
pub const ANCHOR_CONTEXT_LEN: usize = 50;

/// Health of a comment's anchor markers in a note's markdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AnchorState {
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
pub struct AnchorPositions {
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
pub fn find_anchor_positions(markdown: &str, comment_id: &str) -> AnchorPositions {
    let start_pat = start_marker(comment_id);
    let end_pat = end_marker(comment_id);
    AnchorPositions {
        start: markdown.find(&start_pat),
        end: markdown.find(&end_pat),
    }
}

/// Classify a comment's anchor state in `markdown` (reference
/// `scanForProblematicAnchors` + healthy/missing cases).
pub fn classify_anchor_state(markdown: &str, comment_id: &str) -> AnchorState {
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
pub fn context_before(content: &str, pos: usize) -> String {
    // Walk backwards up to CONTEXT_LENGTH chars from `pos`.
    let start = content[..pos]
        .char_indices()
        .rev()
        .take(ANCHOR_CONTEXT_LEN)
        .last()
        .map(|(idx, _)| idx)
        .unwrap_or(pos);
    content[start..pos].to_string()
}

/// Up to [`ANCHOR_CONTEXT_LEN`] characters (UTF-8-safe) immediately following
/// `pos` in `content`. Reference `extractAnchoredText.contextAfter`.
pub fn context_after(content: &str, pos: usize) -> String {
    let end = content[pos..]
        .char_indices()
        .take(ANCHOR_CONTEXT_LEN)
        .last()
        .map(|(idx, ch)| pos + idx + ch.len_utf8())
        .unwrap_or(pos);
    content[pos..end].to_string()
}

/// Remove every `{id}:start` / `{id}:end` marker occurrence from `markdown`
/// (reference `removeAnchors`; used to scrub broken/degenerate anchors).
pub fn remove_anchor_markers(markdown: &str, comment_id: &str) -> String {
    let start_pat = start_marker(comment_id);
    let end_pat = end_marker(comment_id);
    markdown.replace(&start_pat, "").replace(&end_pat, "")
}

/// Outcome of an anchor recovery attempt (reference `RecoveryResult`).
#[derive(Debug, Clone)]
pub enum RecoveryOutcome {
    /// Markers restored — caller should adopt the returned markdown.
    Recovered(String),
    /// Recovery failed — caller should scrub any stray markers and mark the
    /// comment orphaned. The reason mirrors the reference log messages and is
    /// carried for diagnostics / test assertions.
    Failed(#[allow(dead_code)] &'static str),
}

/// Attempt to relocate a partially-anchored comment inside `markdown`, using
/// the stored surrounding context. Reference `recoverPartialAnchor` collapsed
/// to a single "anchor-neighbor" pass over the current content — the neighbor
/// word is derived from the stored `anchor_before` / `anchor_after` context
/// that `comment.add` captured, so no version history is required. The
/// original `anchor_text` is not used as an extra constraint here (it can
/// itself have been partly edited); the surviving marker + neighbor word are
/// what pin the recovered range, matching the reference behavior.
pub fn recover_partial_anchor(
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
        .map(|(i, _)| i)
        .unwrap_or(trimmed.len());
    trimmed[start..].to_string()
}

// ---------------------------------------------------------------------------
// @@@task block parsing (ported from notes/utils/task-block-parser.ts).
// ---------------------------------------------------------------------------

/// One task parsed from a `@@@task` block.
pub struct ParsedTaskBlock {
    pub title: String,
    pub content: String,
}

/// Result of [`extract_task_blocks`].
pub struct TaskBlocksResult {
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
    Some(ParsedTaskBlock { title, content })
}

/// True if the content contains at least one `@@@task`/`@@@tasks` fence-and-close
/// pair (valid title not required), matching the TS `hasTaskBlocks` regex test.
pub fn has_task_blocks(content: &str) -> bool {
    !scan_blocks(content).is_empty()
}

/// Scan raw `@@@task` blocks → `(full_range_start, full_range_end, body)`.
fn scan_blocks(content: &str) -> Vec<(usize, usize, String)> {
    let b = content.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while let Some(rel) = content[i..].find("@@@task") {
        let pos = i + rel;
        let mut j = pos + "@@@task".len();
        if j < b.len() && b[j] == b's' {
            j += 1;
        }
        while j < b.len() && (b[j] == b' ' || b[j] == b'\t') {
            j += 1;
        }
        if j < b.len() && b[j] == b'\r' {
            j += 1;
        }
        if j >= b.len() || b[j] != b'\n' {
            i = pos + "@@@task".len();
            continue;
        }
        j += 1;
        let body_start = j;
        match content[j..].find("@@@") {
            Some(close_rel) => {
                let body_end = j + close_rel;
                let full_end = body_end + 3;
                out.push((pos, full_end, content[body_start..body_end].to_string()));
                i = full_end;
            }
            None => break,
        }
    }
    out
}

/// `extractTasksBlocks` — parse all `@@@task` blocks and replace them with
/// indexed placeholders (valid) or a removed-marker (invalid).
pub fn extract_task_blocks(content: &str) -> TaskBlocksResult {
    let blocks = scan_blocks(content);
    let mut tasks = Vec::new();
    let mut out = String::new();
    let mut cursor = 0;
    let mut valid_index = 0;
    for (start, end, body) in &blocks {
        out.push_str(&content[cursor..*start]);
        match parse_task_block_content(body) {
            Some(task) => {
                out.push_str(&format!("<!-- task-block-placeholder-{valid_index} -->"));
                tasks.push(task);
                valid_index += 1;
            }
            None => out.push_str("<!-- invalid-task-block-removed -->"),
        }
        cursor = *end;
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
pub fn strip_markdown_formatting(text: &str) -> String {
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
        match rest.find(delim) {
            Some(open) => {
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
            }
            None => {
                out.push_str(rest);
                break;
            }
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
        out.push_str(&rest[..open + 1]);
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
pub fn extension_from_mime(mime_type: &str) -> &'static str {
    match mime_type {
        "image/png" => ".png",
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
pub fn strip_data_url_prefix(data: &str) -> &str {
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
pub fn new_asset_id(base64_data: &str, mime_type: &str) -> String {
    use std::hash::{Hash, Hasher};
    let millis = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
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
pub fn mime_from_extension(asset_id: &str) -> String {
    let ext = asset_id
        .rsplit_once('.')
        .map(|(_, e)| e.to_ascii_lowercase())
        .unwrap_or_default();
    match ext.as_str() {
        "png" => "image/png",
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
            other => panic!("expected Recovered, got {other:?}"),
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
            other => panic!("expected Recovered, got {other:?}"),
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
}
