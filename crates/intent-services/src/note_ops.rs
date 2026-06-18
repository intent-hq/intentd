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
}
