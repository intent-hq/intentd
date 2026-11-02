//! Size bounds for note and comment reads (the 500 KiB read payload budget).
//!
//! Every read the app or an agent makes on the main connection must answer
//! in one frame under the transport's read payload budget (512,000 bytes).
//! The helpers here cut a note body into a line window that fits a byte
//! budget, so a read of a multi-megabyte note serves a bounded page plus a
//! cursor (`nextStartLine`) instead of the whole body.
//!
//! Sizes are counted as the JSON-escaped bytes the value occupies on the
//! wire, so escape-heavy content (`"`, `\`, control characters) counts at
//! its real cost.

use serde::Serialize;

/// Bytes of payload a bounded note or comment read may carry: the read
/// payload budget (512,000 bytes) less room for the JSON-RPC envelope and
/// the fixed fields around the bounded body.
pub const NOTE_READ_BUDGET_BYTES: usize = 480_000;

struct CountingSink(usize);

impl std::io::Write for CountingSink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0 += buf.len();
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// Compact JSON size of `value` in bytes, counted without allocating the
/// serialized text. A value that fails to serialize counts as 0.
#[must_use]
pub fn json_len<T: Serialize + ?Sized>(value: &T) -> usize {
    let mut sink = CountingSink(0);
    serde_json::to_writer(&mut sink, value).map_or(0, |()| sink.0)
}

/// JSON-escaped bytes of `s` without the surrounding quotes.
#[must_use]
pub fn escaped_str_len(s: &str) -> usize {
    json_len(s).saturating_sub(2)
}

/// A requested line window: 1-based `start`, at most `max` lines.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LineWindow {
    /// First line to serve (1-based). `None` = line 1.
    pub start_line: Option<usize>,
    /// Most lines to serve. `None` = as many as fit the byte budget.
    pub max_lines: Option<usize>,
    /// Byte offset into the start line to resume a line an earlier page
    /// cut (its `nextStartOffset`). `None` = the start of the line.
    pub start_offset: Option<usize>,
}

impl LineWindow {
    /// Whether the caller asked for a window at all.
    #[must_use]
    pub fn is_requested(&self) -> bool {
        self.start_line.is_some() || self.max_lines.is_some() || self.start_offset.is_some()
    }
}

/// One served page of a note body for `crossWorkspace.readNote`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NoteLinePage {
    /// The served lines joined with `\n` (the raw body when it all fits).
    pub content: String,
    /// The served lines as `   N | text`, joined with `\n`.
    pub numbered_content: String,
    /// First served line (1-based).
    pub start_line: usize,
    /// Byte offset into the first served line where `content` begins
    /// (non-zero only when the page resumes a cut line).
    pub start_offset: usize,
    /// Last served line (1-based); `start_line - 1` when no line was served.
    pub end_line: usize,
    /// Total lines in the note (`content.split('\n').count()`).
    pub total_lines: usize,
    /// The last served line was cut short to fit the budget.
    pub line_cut: bool,
    /// When `line_cut`: the byte offset into `end_line` where the next page
    /// resumes (on a UTF-8 character boundary).
    pub cut_offset: Option<usize>,
}

impl NoteLinePage {
    /// Whether the page is less than the whole note.
    #[must_use]
    pub fn truncated(&self) -> bool {
        self.line_cut
            || self.start_line > 1
            || self.start_offset > 0
            || self.end_line < self.total_lines
    }

    /// The line a caller passes as `startLine` to read on, or `None` at the
    /// end of the note. After a cut line this is the same line, resumed at
    /// [`Self::next_start_offset`], so following the cursor loses nothing.
    #[must_use]
    pub fn next_start_line(&self) -> Option<usize> {
        if self.cut_offset.is_some() {
            return Some(self.end_line);
        }
        (self.end_line < self.total_lines).then_some(self.end_line + 1)
    }

    /// The byte offset a caller passes as `startOffset` with
    /// [`Self::next_start_line`]; `None` unless the last line was cut.
    #[must_use]
    pub fn next_start_offset(&self) -> Option<usize> {
        self.cut_offset
    }
}

/// A line window that does not address the note.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NoteWindowError {
    /// `startLine` is past the last line.
    StartLinePastEnd {
        start_line: usize,
        total_lines: usize,
    },
    /// `startOffset` is not inside the start line or not on a character
    /// boundary.
    InvalidStartOffset {
        start_offset: usize,
        line_len: usize,
    },
}

impl std::fmt::Display for NoteWindowError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StartLinePastEnd {
                start_line,
                total_lines,
            } => write!(
                f,
                "startLine ({start_line}) exceeds total lines in note ({total_lines})"
            ),
            Self::InvalidStartOffset {
                start_offset,
                line_len,
            } => write!(
                f,
                "startOffset ({start_offset}) is not a character boundary inside the start line ({line_len} bytes)"
            ),
        }
    }
}

/// Longest prefix of `s` whose JSON-escaped size is at most `budget`,
/// ending on a character boundary.
fn escaped_prefix(s: &str, budget: usize) -> &str {
    let mut used = 0usize;
    for (idx, ch) in s.char_indices() {
        let mut buf = [0u8; 4];
        let cost = escaped_str_len(ch.encode_utf8(&mut buf));
        if used + cost > budget {
            return &s[..idx];
        }
        used += cost;
    }
    s
}

/// Cut `content` into the [`NoteLinePage`] that `window` asks for, keeping
/// the escaped size of `content` + `numbered_content` within `budget`.
///
/// Lines are split on `\n` and numbered from 1, the numbering of
/// `ws.note.read` and `note.editLines`. At least one line is always served
/// when the window starts inside the note: a line larger than the budget is
/// served as a prefix with `line_cut` set, and the next page resumes it at
/// `cut_offset` (`window.start_offset`). Following the cursor rebuilds the
/// note byte for byte: a page with a non-zero `start_offset` continues the
/// previous page's last line; any other page follows it after a `\n`.
///
/// # Errors
/// [`NoteWindowError`] when `startLine` is past the last line, or
/// `startOffset` is not a character boundary strictly inside the start line.
pub fn note_line_page(
    content: &str,
    window: LineWindow,
    budget: usize,
) -> Result<NoteLinePage, NoteWindowError> {
    let total_lines = content.split('\n').count();
    let start_line = window.start_line.unwrap_or(1).max(1);
    if start_line > total_lines {
        return Err(NoteWindowError::StartLinePastEnd {
            start_line,
            total_lines,
        });
    }
    let start_offset = window.start_offset.unwrap_or(0);
    if start_offset > 0 {
        let line = content.split('\n').nth(start_line - 1).unwrap_or("");
        if start_offset >= line.len() || !line.is_char_boundary(start_offset) {
            return Err(NoteWindowError::InvalidStartOffset {
                start_offset,
                line_len: line.len(),
            });
        }
    }
    let max_lines = window.max_lines.unwrap_or(usize::MAX).max(1);
    let mut page = NoteLinePage {
        content: String::new(),
        numbered_content: String::new(),
        start_line,
        start_offset,
        end_line: start_line - 1,
        total_lines,
        line_cut: false,
        cut_offset: None,
    };
    let mut used = 0usize;
    for (idx, line) in content.split('\n').enumerate().skip(start_line - 1) {
        let number = idx + 1;
        if number - start_line >= max_lines {
            break;
        }
        let line_offset = if number == start_line {
            start_offset
        } else {
            0
        };
        let line = &line[line_offset..];
        let prefix = format!("{number:>4} | ");
        let sep = usize::from(number > start_line);
        // A `\n` separator is 2 bytes once JSON-escaped, in both strings.
        let sep_cost = 2 * sep;
        let escaped = escaped_str_len(line);
        let cost = 2 * (escaped + sep_cost) + prefix.len();
        let served = if used + cost <= budget {
            line
        } else if number == start_line {
            let room = budget.saturating_sub(prefix.len()) / 2;
            let mut cut = escaped_prefix(line, room);
            if cut.is_empty() {
                // Always make progress: serve at least one character.
                let first = line.chars().next().map_or(0, char::len_utf8);
                cut = &line[..first];
            }
            if cut.len() < line.len() {
                page.line_cut = true;
                page.cut_offset = Some(line_offset + cut.len());
            }
            cut
        } else {
            break;
        };
        if sep == 1 {
            page.content.push('\n');
            page.numbered_content.push('\n');
        }
        page.content.push_str(served);
        page.numbered_content.push_str(&prefix);
        page.numbered_content.push_str(served);
        used += 2 * (escaped_str_len(served) + sep_cost) + prefix.len();
        page.end_line = number;
        if page.line_cut {
            break;
        }
    }
    Ok(page)
}

/// Where a bounded `note.lineAttribution.load` page stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AttributionPage {
    /// Whether the page carries less than every attributed line.
    pub truncated: bool,
    /// The next attributed line to pass as `startLine`, or `None` at the end.
    pub next_start_line: Option<usize>,
}

/// Keep only the attributed lines `window` asks for, in line order, while
/// the serialized `attributions` map stays within `budget` bytes. At least
/// one entry is kept when any line is in the window (a single entry is
/// small: a timestamp and an author).
///
/// Keys are 1-based line numbers as strings; a key that is not a number is
/// dropped from windowed reads.
#[must_use]
pub fn line_attribution_page(
    data: &mut crate::model::LineAttributionData,
    window: LineWindow,
    budget: usize,
) -> AttributionPage {
    let start = window.start_line.unwrap_or(1).max(1);
    let max = window.max_lines.unwrap_or(usize::MAX).max(1);
    let total = data.attributions.len();
    let mut entries: Vec<(usize, String, crate::model::LineAttributionInfo)> =
        std::mem::take(&mut data.attributions)
            .into_iter()
            .filter_map(|(key, info)| key.parse::<usize>().ok().map(|line| (line, key, info)))
            .collect();
    entries.sort_by_key(|(line, _, _)| *line);
    let mut used = 2usize;
    let mut next_start_line = None;
    let mut kept = 0usize;
    for (line, key, info) in entries {
        if line < start {
            continue;
        }
        let cost = json_len(&key) + 1 + json_len(&info) + 1;
        if kept >= max || (kept > 0 && used + cost > budget) {
            next_start_line = Some(line);
            break;
        }
        used += cost;
        kept += 1;
        data.attributions.insert(key, info);
    }
    AttributionPage {
        truncated: kept < total,
        next_start_line,
    }
}

/// `note.list` page size: default and hard maximum of `limit`.
pub const NOTE_LIST_DEFAULT_LIMIT: usize = 200;
pub const NOTE_LIST_MAX_LIMIT: usize = 500;
/// `comment.list` page size (threads): default and hard maximum of `limit`.
pub const COMMENT_LIST_DEFAULT_LIMIT: usize = 100;
pub const COMMENT_LIST_MAX_LIMIT: usize = 500;
/// `note.listTasks` page size (checkbox rows): default and hard maximum.
pub const NOTE_TASKS_DEFAULT_LIMIT: usize = 1_000;
pub const NOTE_TASKS_MAX_LIMIT: usize = 2_000;

/// The requested page of a bounded list read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageRequest {
    /// Rows to serve at most (already clamped to the method's maximum).
    pub limit: usize,
    /// The `nextToken` of the previous page, if any.
    pub next_token: Option<String>,
}

impl PageRequest {
    /// Clamp a caller `limit` (`None` = `default`) into `1..=max`.
    #[must_use]
    pub fn new(
        limit: Option<usize>,
        default: usize,
        max: usize,
        next_token: Option<String>,
    ) -> Self {
        Self {
            limit: limit.unwrap_or(default).clamp(1, max),
            next_token,
        }
    }
}

#[derive(Serialize, serde::Deserialize)]
struct CursorWire {
    v: u8,
    scope: String,
    key: String,
    id: String,
}

/// Opaque `nextToken` for a keyset cursor: the sort `key` and `id` of the
/// last row served, bound to `scope` (method + resource) so a token from
/// one list cannot be replayed against another.
#[must_use]
pub fn page_token(scope: &str, key: &str, id: &str) -> String {
    serde_json::to_string(&CursorWire {
        v: 1,
        scope: scope.to_string(),
        key: key.to_string(),
        id: id.to_string(),
    })
    .unwrap_or_default()
}

/// Decode a [`page_token`] minted for `scope` into `(key, id)`.
///
/// # Errors
/// `"invalid nextToken"` for a malformed token or one minted for another
/// list.
pub fn parse_page_token(token: &str, scope: &str) -> Result<(String, String), &'static str> {
    let cursor: CursorWire = serde_json::from_str(token).map_err(|_| "invalid nextToken")?;
    if cursor.v != 1 || cursor.scope != scope || cursor.id.is_empty() {
        return Err("invalid nextToken");
    }
    Ok((cursor.key, cursor.id))
}

/// Serve rows from `items` (already in page order and past the cursor)
/// until `limit` rows or `budget` serialized bytes. `render` builds a row;
/// `shrink(item, room)` builds a smaller row for a first row that alone is
/// over `room` bytes (a cut note body, a capped comment list). At least one
/// row is served when `items` is not empty.
///
/// Returns the rows and, when rows remain, the index of the last served
/// item (the caller mints the `nextToken` from it).
pub fn take_page<T>(
    items: &[T],
    limit: usize,
    budget: usize,
    mut render: impl FnMut(&T) -> serde_json::Value,
    mut shrink: impl FnMut(&T, usize) -> serde_json::Value,
) -> (Vec<serde_json::Value>, Option<usize>) {
    let mut rows = Vec::new();
    let mut used = 2usize;
    for (idx, item) in items.iter().enumerate() {
        if rows.len() >= limit {
            return (rows, Some(idx - 1));
        }
        let mut row = render(item);
        let mut cost = json_len(&row) + 1;
        if used + cost > budget {
            if !rows.is_empty() {
                return (rows, Some(idx - 1));
            }
            row = shrink(item, budget.saturating_sub(used + 1));
            cost = json_len(&row) + 1;
        }
        used += cost;
        rows.push(row);
    }
    (rows, None)
}

/// A full `Note` row whose serialized size is at most `room` bytes, made
/// by cutting `content` with [`crate::model::note_bounded_row`] (which adds
/// `contentTruncated` / `contentBytes`).
#[must_use]
pub fn note_row_within(note: &crate::model::Note, room: usize) -> serde_json::Value {
    let mut cap = room;
    loop {
        let row = crate::model::note_bounded_row(note.clone(), cap);
        if cap == 0 || json_len(&row) <= room {
            return row;
        }
        let over = json_len(&row) - room;
        cap = cap.saturating_sub(over.max(1));
    }
}

/// Whether a default read (`note.get`, a `note.list` / `note.subscribe` full
/// row) cuts this note's `content` to fit the read budget. Such a note's
/// whole-content writes must carry `baseContentBytes`, so a client that
/// only ever saw the cut prefix cannot replace the full text with it.
#[must_use]
pub fn note_default_read_is_cut(note: &crate::model::Note) -> bool {
    // Room for the page / envelope around a single row.
    json_len(note) + 16 > NOTE_READ_BUDGET_BYTES
}

/// The `nextToken` scope of a workspace's note list. `note.list` and the
/// `note.subscribe` seq-0 snapshot share it, so a snapshot's token pages
/// on through `note.list`.
#[must_use]
pub fn note_list_scope(workspace_id: &str) -> String {
    format!("note.list:{workspace_id}")
}

/// One bounded page of a workspace's notes for `note.list` and the
/// `note.subscribe` snapshot: notes in `(createdAt, id)` order after the
/// `page` cursor, at most `page.limit` rows and `budget` bytes. Rows use the
/// `projection` (`Some(Slim)` = slim rows) and, for full rows,
/// `max_content_bytes` like `note.get`; a full first row over the budget is
/// cut with `contentTruncated`. Returns the rows and the `nextToken` (`None`
/// on the last page).
///
/// # Errors
/// `"invalid nextToken"` for a token not minted for this workspace's list.
pub fn note_list_page(
    mut notes: Vec<crate::model::Note>,
    workspace_id: &str,
    projection: Option<crate::model::NoteListProjection>,
    max_content_bytes: Option<usize>,
    page: &PageRequest,
    budget: usize,
) -> Result<(Vec<serde_json::Value>, Option<String>), &'static str> {
    let scope = note_list_scope(workspace_id);
    notes.sort_by(|a, b| (&a.created_at, &a.id.0).cmp(&(&b.created_at, &b.id.0)));
    if let Some(token) = page.next_token.as_deref() {
        let (key, id) = parse_page_token(token, &scope)?;
        notes.retain(|n| (&n.created_at, &n.id.0) > (&key, &id));
    }
    let render = |note: &crate::model::Note| match projection {
        Some(crate::model::NoteListProjection::Slim) => {
            crate::model::note_list_slim_row(note.clone())
        }
        None => match max_content_bytes {
            Some(limit) => crate::model::note_bounded_row(note.clone(), limit),
            None => serde_json::to_value(note).unwrap_or(serde_json::Value::Null),
        },
    };
    let shrink = |note: &crate::model::Note, room: usize| match projection {
        Some(crate::model::NoteListProjection::Slim) => {
            crate::model::note_list_slim_row(note.clone())
        }
        None => note_row_within(note, room),
    };
    let (rows, last) = take_page(&notes, page.limit, budget, render, shrink);
    let next = last.map(|idx| page_token(&scope, &notes[idx].created_at, &notes[idx].id.0));
    Ok((rows, next))
}

/// The `nextToken` scope of one note's comment threads, shared by
/// `comment.list` and the `comment.subscribe` snapshot.
#[must_use]
pub fn comment_list_scope(workspace_id: &str, note_id: &str) -> String {
    format!("comment.list:{workspace_id}:{note_id}")
}

/// One bounded page of a note's comment threads for `comment.list` and the
/// `comment.subscribe` snapshot: threads in `comment.list` order (latest
/// activity first, then `threadId`) after the `page` cursor, at most
/// `page.limit` threads and `budget` bytes. A first thread whose `comments`
/// alone overflow the budget keeps the oldest comments that fit and adds
/// `commentsTruncated: true` (read the whole thread with
/// `comment.getThread`). Returns the rows and the `nextToken`.
///
/// # Errors
/// `"invalid nextToken"` for a token not minted for this note's threads.
pub fn comment_list_page(
    mut threads: Vec<crate::model::CommentThreadSummary>,
    workspace_id: &str,
    note_id: &str,
    page: &PageRequest,
    budget: usize,
) -> Result<(Vec<serde_json::Value>, Option<String>), &'static str> {
    let scope = comment_list_scope(workspace_id, note_id);
    threads.sort_by(|a, b| {
        b.last_activity
            .cmp(&a.last_activity)
            .then_with(|| a.thread_id.cmp(&b.thread_id))
    });
    if let Some(token) = page.next_token.as_deref() {
        let (key, id) = parse_page_token(token, &scope)?;
        threads.retain(|t| t.last_activity < key || (t.last_activity == key && t.thread_id > id));
    }
    let render =
        |t: &crate::model::CommentThreadSummary| serde_json::to_value(t).unwrap_or_default();
    let shrink = |t: &crate::model::CommentThreadSummary, room: usize| {
        let mut thread = t.clone();
        let comments = thread.comments.take().unwrap_or_default();
        let mut row = serde_json::to_value(&thread).unwrap_or_default();
        if t.comments.is_some() {
            let mut used =
                json_len(&row) + json_len("comments") + 3 + json_len("commentsTruncated") + 8;
            let mut kept = Vec::new();
            for comment in comments {
                let value = serde_json::to_value(&comment).unwrap_or_default();
                let cost = json_len(&value) + 1;
                if used + cost > room {
                    break;
                }
                used += cost;
                kept.push(value);
            }
            if let Some(obj) = row.as_object_mut() {
                obj.insert("comments".into(), serde_json::Value::Array(kept));
                obj.insert("commentsTruncated".into(), serde_json::Value::Bool(true));
            }
        }
        row
    };
    let (rows, last) = take_page(&threads, page.limit, budget, render, shrink);
    let next =
        last.map(|idx| page_token(&scope, &threads[idx].last_activity, &threads[idx].thread_id));
    Ok((rows, next))
}

/// One bounded page of a note's checkbox rows for `note.listTasks`: rows
/// in line order after the `page` cursor, at most `page.limit` rows and
/// `budget` bytes. A first row whose `text` alone overflows the budget is
/// served with its `text` cut and `textTruncated: true`. Returns the rows
/// and the `nextToken`.
///
/// # Errors
/// `"invalid nextToken"` for a token not minted for this note's rows.
pub fn note_tasks_page(
    rows: &[crate::model::NoteTaskRow],
    workspace_id: &str,
    note_id: &str,
    page: &PageRequest,
    budget: usize,
) -> Result<(Vec<serde_json::Value>, Option<String>), &'static str> {
    let scope = format!("note.listTasks:{workspace_id}:{note_id}");
    let start = match page.next_token.as_deref() {
        Some(token) => {
            let (key, _) = parse_page_token(token, &scope)?;
            let after: usize = key.parse().map_err(|_| "invalid nextToken")?;
            rows.partition_point(|r| r.line_number <= after)
        }
        None => 0,
    };
    let rows = &rows[start..];
    let render = |r: &crate::model::NoteTaskRow| serde_json::to_value(r).unwrap_or_default();
    let shrink = |r: &crate::model::NoteTaskRow, room: usize| {
        let mut row = r.clone();
        row.text.clear();
        let overhead = json_len(&row) + json_len("textTruncated") + 6;
        row.text = escaped_prefix(&r.text, room.saturating_sub(overhead)).to_string();
        let mut value = serde_json::to_value(&row).unwrap_or_default();
        if let Some(obj) = value.as_object_mut() {
            obj.insert("textTruncated".into(), serde_json::Value::Bool(true));
        }
        value
    };
    let (out, last) = take_page(rows, page.limit, budget, render, shrink);
    let next = last.map(|idx| page_token(&scope, &rows[idx].line_number.to_string(), "line"));
    Ok((out, next))
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) fn test_note(id: &str, created_at: &str, content: &str) -> crate::model::Note {
        use crate::model::{ContentType, Note, NoteMetadata, NoteVisibility};
        Note {
            id: crate::NoteId::from(id),
            workspace_id: crate::WorkspaceId::from("ws-1"),
            title: format!("Note {id}"),
            content: content.to_string(),
            content_type: ContentType::Markdown,
            tags: Vec::new(),
            is_pinned: false,
            is_archived: false,
            is_default: false,
            parent_id: None,
            visibility: NoteVisibility::Workspace,
            metadata: NoteMetadata::default(),
            created_at: created_at.to_string(),
            rev: 1,
            updated_at: created_at.to_string(),
        }
    }

    fn whole(content: &str) -> NoteLinePage {
        note_line_page(content, LineWindow::default(), NOTE_READ_BUDGET_BYTES).unwrap()
    }

    #[test]
    fn small_note_is_served_whole() {
        let page = whole("a\nb\nc");
        assert_eq!(page.content, "a\nb\nc");
        assert_eq!(page.numbered_content, "   1 | a\n   2 | b\n   3 | c");
        assert_eq!(
            (page.start_line, page.end_line, page.total_lines),
            (1, 3, 3)
        );
        assert!(!page.truncated());
        assert_eq!(page.next_start_line(), None);
    }

    #[test]
    fn empty_note_is_one_empty_line() {
        let page = whole("");
        assert_eq!(page.content, "");
        assert_eq!(page.numbered_content, "   1 | ");
        assert_eq!((page.end_line, page.total_lines), (1, 1));
        assert!(!page.truncated());
    }

    #[test]
    fn window_serves_requested_lines() {
        let page = note_line_page(
            "a\nb\nc\nd",
            LineWindow {
                start_line: Some(2),
                max_lines: Some(2),
                start_offset: None,
            },
            NOTE_READ_BUDGET_BYTES,
        )
        .unwrap();
        assert_eq!(page.content, "b\nc");
        assert_eq!(page.numbered_content, "   2 | b\n   3 | c");
        assert!(page.truncated());
        assert_eq!(page.next_start_line(), Some(4));
    }

    #[test]
    fn start_past_end_is_an_error() {
        let err = note_line_page(
            "a\nb",
            LineWindow {
                start_line: Some(3),
                max_lines: None,
                start_offset: None,
            },
            NOTE_READ_BUDGET_BYTES,
        )
        .unwrap_err();
        assert_eq!(
            err.to_string(),
            "startLine (3) exceeds total lines in note (2)"
        );
    }

    #[test]
    fn budget_cuts_on_a_line_boundary_and_pages_cover_the_note() {
        let content: String = (0..5_000)
            .map(|i| format!("line {i} {}", "x".repeat(200)))
            .collect::<Vec<_>>()
            .join("\n");
        let budget = 50_000;
        let mut start = 1;
        let mut lines = Vec::new();
        loop {
            let page = note_line_page(
                &content,
                LineWindow {
                    start_line: Some(start),
                    max_lines: None,
                    start_offset: None,
                },
                budget,
            )
            .unwrap();
            assert!(json_len(&page.content) + json_len(&page.numbered_content) <= budget + 4);
            assert!(!page.line_cut);
            lines.push(page.content.clone());
            match page.next_start_line() {
                Some(next) => start = next,
                None => break,
            }
        }
        assert!(lines.len() > 1);
        assert_eq!(lines.join("\n"), content);
    }

    /// Follow the cursor from line 1 and rebuild the note from its pages.
    fn rebuild(content: &str, budget: usize) -> (String, usize) {
        let mut out = String::new();
        let mut window = LineWindow::default();
        let mut pages = 0;
        loop {
            let page = note_line_page(content, window, budget).unwrap();
            assert!(json_len(&page.content) + json_len(&page.numbered_content) <= budget + 4);
            if pages > 0 && page.start_offset == 0 {
                out.push('\n');
            }
            out.push_str(&page.content);
            pages += 1;
            assert!(pages < 100_000, "cursor must make progress");
            match page.next_start_line() {
                Some(line) => {
                    window = LineWindow {
                        start_line: Some(line),
                        max_lines: None,
                        start_offset: page.next_start_offset(),
                    };
                }
                None => break,
            }
        }
        (out, pages)
    }

    #[test]
    fn a_line_larger_than_the_budget_resumes_at_a_byte_offset() {
        let content = format!("{}\nnext", "é\"".repeat(10_000));
        let page = note_line_page(&content, LineWindow::default(), 1_000).unwrap();
        assert!(page.line_cut);
        assert!(page.truncated());
        assert_eq!(page.end_line, 1);
        assert_eq!(page.next_start_line(), Some(1));
        let offset = page.next_start_offset().unwrap();
        assert_eq!(offset, page.content.len());
        assert!(content.is_char_boundary(offset));
        assert!(json_len(&page.content) + json_len(&page.numbered_content) <= 1_000 + 4);

        let (rebuilt, pages) = rebuild(&content, 1_000);
        assert!(pages > 2);
        assert_eq!(rebuilt, content);
    }

    #[test]
    fn multibyte_characters_at_every_cut_point_rebuild_byte_for_byte() {
        // Mixed 1-, 2-, 3- and 4-byte characters plus escapes, on oversized
        // lines between ordinary ones, at budgets that land cuts everywhere.
        let big = "aé€😀\"\\\t".repeat(3_000);
        let content = format!("first\n{big}\nmiddle\n\n{big}{big}\nlast");
        for budget in [997, 1_000, 1_001, 1_003, 4_096] {
            let (rebuilt, _) = rebuild(&content, budget);
            assert_eq!(rebuilt, content, "budget {budget}");
        }
    }

    #[test]
    fn a_resumed_page_serves_the_rest_of_the_line_then_following_lines() {
        let content = "abcdef\nghi";
        let page = note_line_page(
            content,
            LineWindow {
                start_line: Some(1),
                max_lines: None,
                start_offset: Some(3),
            },
            NOTE_READ_BUDGET_BYTES,
        )
        .unwrap();
        assert_eq!(page.content, "def\nghi");
        assert_eq!(page.numbered_content, "   1 | def\n   2 | ghi");
        assert_eq!(page.start_offset, 3);
        assert!(page.truncated());
        assert_eq!(page.next_start_line(), None);
    }

    #[test]
    fn a_start_offset_off_a_boundary_or_outside_the_line_is_an_error() {
        for offset in [1, 3, 4] {
            let err = note_line_page(
                "éa\nb",
                LineWindow {
                    start_line: Some(1),
                    max_lines: None,
                    start_offset: Some(offset),
                },
                NOTE_READ_BUDGET_BYTES,
            )
            .unwrap_err();
            assert!(
                matches!(err, NoteWindowError::InvalidStartOffset { .. }),
                "{offset}"
            );
        }
    }

    fn attribution(lines: usize) -> crate::model::LineAttributionData {
        crate::model::LineAttributionData {
            note_id: crate::NoteId::from_string("n"),
            workspace_id: crate::WorkspaceId::from("w"),
            computed_at: "t".into(),
            attributions: (1..=lines)
                .map(|line| {
                    (
                        line.to_string(),
                        crate::model::LineAttributionInfo {
                            timestamp: 1,
                            author: Some(crate::model::LineAttributionAuthor {
                                id: "agent-1".into(),
                                name: "Agent".into(),
                                author_type: "agent".into(),
                                turn_number: Some(3),
                            }),
                        },
                    )
                })
                .collect(),
        }
    }

    #[test]
    fn attribution_pages_in_line_order_and_cover_every_line() {
        let full = attribution(2_000);
        let mut seen = Vec::new();
        let mut start = 1;
        loop {
            let mut page = full.clone();
            let cut = line_attribution_page(
                &mut page,
                LineWindow {
                    start_line: Some(start),
                    max_lines: None,
                    start_offset: None,
                },
                10_000,
            );
            assert!(json_len(&page.attributions) <= 10_000);
            let mut lines: Vec<usize> = page
                .attributions
                .keys()
                .map(|k| k.parse().unwrap())
                .collect();
            lines.sort_unstable();
            assert_eq!(lines.first(), Some(&start));
            seen.extend(lines);
            assert!(cut.truncated);
            match cut.next_start_line {
                Some(next) => start = next,
                None => break,
            }
        }
        assert_eq!(seen, (1..=2_000).collect::<Vec<_>>());
    }

    #[test]
    fn attribution_window_that_fits_is_whole() {
        let mut page = attribution(3);
        let cut = line_attribution_page(&mut page, LineWindow::default(), NOTE_READ_BUDGET_BYTES);
        assert_eq!(page, attribution(3));
        assert!(!cut.truncated);
        assert_eq!(cut.next_start_line, None);
        let mut page = attribution(10);
        let cut = line_attribution_page(
            &mut page,
            LineWindow {
                start_line: Some(4),
                max_lines: Some(2),
                start_offset: None,
            },
            NOTE_READ_BUDGET_BYTES,
        );
        assert_eq!(page.attributions.keys().collect::<Vec<_>>(), ["4", "5"]);
        assert!(cut.truncated);
        assert_eq!(cut.next_start_line, Some(6));
    }

    #[test]
    fn page_tokens_round_trip_and_are_scoped() {
        let token = page_token("note.list:w", "2026-01-01", "n1");
        assert_eq!(
            parse_page_token(&token, "note.list:w"),
            Ok(("2026-01-01".to_string(), "n1".to_string()))
        );
        assert_eq!(
            parse_page_token(&token, "note.list:x"),
            Err("invalid nextToken")
        );
        assert_eq!(
            parse_page_token("garbage", "note.list:w"),
            Err("invalid nextToken")
        );
    }

    #[test]
    fn page_request_clamps() {
        assert_eq!(PageRequest::new(None, 200, 500, None).limit, 200);
        assert_eq!(PageRequest::new(Some(10_000), 200, 500, None).limit, 500);
        assert_eq!(PageRequest::new(Some(0), 200, 500, None).limit, 1);
    }

    #[test]
    fn take_page_stops_at_limit_or_budget() {
        let items: Vec<String> = (0..10).map(|i| format!("{i}{}", "x".repeat(98))).collect();
        let render = |s: &String| serde_json::json!(s);
        let shrink = |s: &String, _room: usize| serde_json::json!(s);
        let (rows, next) = take_page(&items, 3, usize::MAX, render, shrink);
        assert_eq!((rows.len(), next), (3, Some(2)));
        let (rows, next) = take_page(&items, 100, 450, render, shrink);
        assert_eq!((rows.len(), next), (4, Some(3)));
        assert!(json_len(&rows) <= 450);
        let (rows, next) = take_page(&items, 100, usize::MAX, render, shrink);
        assert_eq!((rows.len(), next), (10, None));
        let (rows, next) = take_page(&items[..0], 100, 10, render, shrink);
        assert_eq!((rows.len(), next), (0, None));
    }

    #[test]
    fn take_page_shrinks_an_oversized_first_row() {
        let items = vec!["y".repeat(1_000), "z".to_string()];
        let (rows, next) = take_page(
            &items,
            10,
            200,
            |s: &String| serde_json::json!(s),
            |s: &String, room| serde_json::json!(&s[..room - 2]),
        );
        assert_eq!(next, Some(0));
        assert_eq!(rows.len(), 1);
        assert!(json_len(&rows) <= 200);
    }

    #[test]
    fn note_row_within_fits_the_room() {
        let note = test_note("n1", "t0", &"\"line\"\n".repeat(20_000));
        let row = note_row_within(&note, 10_000);
        assert!(json_len(&row) <= 10_000);
        assert_eq!(row["contentTruncated"], true);
        assert_eq!(row["contentBytes"], note.content.len());
    }

    #[test]
    fn note_list_pages_cover_every_note_within_budget() {
        let notes: Vec<_> = (0..300)
            .map(|i| {
                test_note(
                    &format!("n{i:03}"),
                    &format!("t{:03}", i / 2),
                    &"b".repeat(3_000),
                )
            })
            .collect();
        for projection in [None, Some(crate::model::NoteListProjection::Slim)] {
            let mut seen = Vec::new();
            let mut token = None;
            loop {
                let page = PageRequest::new(
                    Some(120),
                    NOTE_LIST_DEFAULT_LIMIT,
                    NOTE_LIST_MAX_LIMIT,
                    token,
                );
                let (rows, next) =
                    note_list_page(notes.clone(), "ws-1", projection, None, &page, 100_000)
                        .unwrap();
                assert!(json_len(&rows) <= 100_000);
                assert!(!rows.is_empty() && rows.len() <= 120);
                seen.extend(rows.iter().map(|r| r["id"].as_str().unwrap().to_string()));
                match next {
                    Some(t) => token = Some(t),
                    None => break,
                }
            }
            let mut expected: Vec<String> = notes.iter().map(|n| n.id.0.clone()).collect();
            expected.sort();
            assert_eq!(seen, expected, "{projection:?}");
        }
    }

    #[test]
    fn note_list_page_cuts_a_huge_first_note_and_rejects_foreign_tokens() {
        let notes = vec![
            test_note("big", "t0", &"x".repeat(2_000_000)),
            test_note("small", "t1", "s"),
        ];
        let page = PageRequest::new(None, NOTE_LIST_DEFAULT_LIMIT, NOTE_LIST_MAX_LIMIT, None);
        let (rows, next) = note_list_page(
            notes.clone(),
            "ws-1",
            None,
            None,
            &page,
            NOTE_READ_BUDGET_BYTES,
        )
        .unwrap();
        assert_eq!(rows[0]["id"], "big");
        assert_eq!(rows[0]["contentTruncated"], true);
        assert!(json_len(&rows) <= NOTE_READ_BUDGET_BYTES);
        assert!(
            json_len(&rows[0]) > NOTE_READ_BUDGET_BYTES - 1_000,
            "the cut uses the room"
        );
        assert_eq!(rows.len(), 1);
        assert!(next.is_some());
        let page = PageRequest::new(None, NOTE_LIST_DEFAULT_LIMIT, NOTE_LIST_MAX_LIMIT, next);
        let err =
            note_list_page(notes, "ws-2", None, None, &page, NOTE_READ_BUDGET_BYTES).unwrap_err();
        assert_eq!(err, "invalid nextToken");
    }

    fn thread(
        id: &str,
        last_activity: &str,
        comments: usize,
    ) -> crate::model::CommentThreadSummary {
        use crate::model::{AuthorType, CommentStatus, CommentType, CommentWire};
        crate::model::CommentThreadSummary {
            thread_id: id.to_string(),
            note_id: crate::NoteId::from("n1"),
            targeted_text: Some("text".into()),
            anchor_id: None,
            status: "open".into(),
            created_at: "t0".into(),
            last_activity: last_activity.to_string(),
            latest_comment_author: "me".into(),
            latest_comment_author_type: AuthorType::User,
            latest_comment_author_principal_id: None,
            latest_comment_author_identity: None,
            latest_comment_at: last_activity.to_string(),
            comment_count: comments,
            comments: Some(
                (0..comments)
                    .map(|i| CommentWire {
                        id: format!("{id}-{i}"),
                        thread_id: id.to_string(),
                        note_id: None,
                        kind: CommentType::Comment,
                        content: "c".repeat(1_000),
                        author: "me".into(),
                        author_type: AuthorType::User,
                        author_principal_id: None,
                        author_identity: None,
                        status: CommentStatus::Open,
                        parent_id: None,
                        anchor: None,
                        anchor_text: None,
                        anchor_context: None,
                        suggestion_diff: None,
                        agent_id: None,
                        is_orphaned: None,
                        created_at: "t0".into(),
                        updated_at: "t0".into(),
                    })
                    .collect(),
            ),
        }
    }

    #[test]
    fn comment_pages_follow_list_order_and_cover_every_thread() {
        let threads: Vec<_> = (0..250)
            .map(|i| thread(&format!("th{i:03}"), &format!("t{:03}", i / 3), 3))
            .collect();
        let mut seen = Vec::new();
        let mut token = None;
        loop {
            let page = PageRequest::new(
                None,
                COMMENT_LIST_DEFAULT_LIMIT,
                COMMENT_LIST_MAX_LIMIT,
                token,
            );
            let (rows, next) =
                comment_list_page(threads.clone(), "w", "n1", &page, 120_000).unwrap();
            assert!(json_len(&rows) <= 120_000);
            seen.extend(
                rows.iter()
                    .map(|r| r["threadId"].as_str().unwrap().to_string()),
            );
            match next {
                Some(t) => token = Some(t),
                None => break,
            }
        }
        let mut expected = threads.clone();
        expected.sort_by(|a, b| {
            b.last_activity
                .cmp(&a.last_activity)
                .then_with(|| a.thread_id.cmp(&b.thread_id))
        });
        assert_eq!(
            seen,
            expected
                .iter()
                .map(|t| t.thread_id.clone())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn a_huge_thread_keeps_the_comments_that_fit() {
        let threads = vec![thread("big", "t1", 2_000), thread("small", "t0", 1)];
        let page = PageRequest::new(
            None,
            COMMENT_LIST_DEFAULT_LIMIT,
            COMMENT_LIST_MAX_LIMIT,
            None,
        );
        let (rows, next) =
            comment_list_page(threads, "w", "n1", &page, NOTE_READ_BUDGET_BYTES).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["commentsTruncated"], true);
        assert!(!rows[0]["comments"].as_array().unwrap().is_empty());
        assert!(json_len(&rows) <= NOTE_READ_BUDGET_BYTES);
        assert!(next.is_some());
    }

    #[test]
    fn task_pages_cover_every_row_in_line_order() {
        let rows: Vec<crate::model::NoteTaskRow> = (0..3_000)
            .map(|i| crate::model::NoteTaskRow {
                line_number: i * 2 + 1,
                text: format!("task {i} {}", "t".repeat(150)),
                status: "todo".into(),
                task_note_id: None,
                linked_task_note_id: None,
                depends_on: Vec::new(),
                conflicts_with: Vec::new(),
                unmet_depends_on: Vec::new(),
            })
            .collect();
        let mut seen = Vec::new();
        let mut token = None;
        loop {
            let page =
                PageRequest::new(None, NOTE_TASKS_DEFAULT_LIMIT, NOTE_TASKS_MAX_LIMIT, token);
            let (out, next) =
                note_tasks_page(&rows, "w", "n", &page, NOTE_READ_BUDGET_BYTES).unwrap();
            assert!(json_len(&out) <= NOTE_READ_BUDGET_BYTES);
            assert!(out.len() <= NOTE_TASKS_DEFAULT_LIMIT);
            seen.extend(out.iter().map(|r| r["lineNumber"].as_u64().unwrap()));
            match next {
                Some(t) => token = Some(t),
                None => break,
            }
        }
        assert_eq!(
            seen,
            rows.iter()
                .map(|r| r.line_number as u64)
                .collect::<Vec<_>>()
        );
        let huge = vec![crate::model::NoteTaskRow {
            text: "\"".repeat(400_000),
            ..rows[0].clone()
        }];
        let page = PageRequest::new(None, NOTE_TASKS_DEFAULT_LIMIT, NOTE_TASKS_MAX_LIMIT, None);
        let (out, next) = note_tasks_page(&huge, "w", "n", &page, NOTE_READ_BUDGET_BYTES).unwrap();
        assert_eq!(out[0]["textTruncated"], true);
        assert!(json_len(&out) <= NOTE_READ_BUDGET_BYTES);
        assert_eq!(next, None);
    }

    #[test]
    fn escaped_size_counts_escapes() {
        assert_eq!(escaped_str_len("ab"), 2);
        assert_eq!(escaped_str_len("\"\\"), 4);
        assert_eq!(escaped_str_len("\u{1}"), 6);
        assert_eq!(json_len(&serde_json::json!({ "a": 1 })), 7);
    }
}
