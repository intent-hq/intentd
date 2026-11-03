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

/// A `contentOffset` that does not address the note's content.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidContentOffset {
    pub offset: usize,
    pub content_bytes: usize,
}

impl std::fmt::Display for InvalidContentOffset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "contentOffset ({}) is not a character boundary inside the note content ({} bytes)",
            self.offset, self.content_bytes
        )
    }
}

/// The `note.get` row: the note from byte `content_offset` of its content,
/// cut by `max_content_bytes` (when given) and to fit `budget` bytes.
///
/// A row that carries the whole note serializes exactly as before. Any
/// other row adds `contentTruncated: true`, `contentBytes` (the full
/// content's byte length), `contentOffset` (where `content` starts) and
/// `nextContentOffset` (the `contentOffset` that reads on, on a UTF-8
/// boundary; `null` once `content` reaches the end). Following the cursor
/// and concatenating `content` rebuilds the note byte for byte.
///
/// # Errors
/// [`InvalidContentOffset`] when a non-zero `content_offset` is not a
/// character boundary strictly inside the content.
pub fn note_get_row(
    note: &crate::model::Note,
    max_content_bytes: Option<usize>,
    content_offset: usize,
    budget: usize,
) -> Result<serde_json::Value, InvalidContentOffset> {
    let full = note.content.len();
    if content_offset > 0
        && (content_offset >= full || !note.content.is_char_boundary(content_offset))
    {
        return Err(InvalidContentOffset {
            offset: content_offset,
            content_bytes: full,
        });
    }
    let mut rest = note.clone();
    rest.content = note.content[content_offset..].to_string();
    let first = match max_content_bytes {
        Some(limit) => crate::model::note_bounded_row(rest.clone(), limit),
        None => serde_json::to_value(&rest).unwrap_or_default(),
    };
    let mut row = if json_len(&first) <= budget {
        first
    } else {
        note_row_within(&rest, budget)
    };
    let mut served = row["content"].as_str().map_or(0, str::len);
    if served == 0 && !rest.content.is_empty() {
        // Always make progress: serve at least one character.
        served = rest.content.chars().next().map_or(0, char::len_utf8);
        if let Some(obj) = row.as_object_mut() {
            obj.insert(
                "content".into(),
                serde_json::Value::String(rest.content[..served].to_string()),
            );
        }
    }
    let whole = content_offset == 0 && served == full;
    if let Some(obj) = row.as_object_mut() {
        if whole {
            obj.remove("contentTruncated");
            obj.remove("contentBytes");
        } else {
            let end = content_offset + served;
            obj.insert("contentTruncated".into(), serde_json::Value::Bool(true));
            obj.insert("contentBytes".into(), serde_json::json!(full));
            obj.insert("contentOffset".into(), serde_json::json!(content_offset));
            obj.insert(
                "nextContentOffset".into(),
                serde_json::json!((end < full).then_some(end)),
            );
        }
    }
    Ok(row)
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
/// `lastActivity` changes when a thread gets a reply, so a plain keyset on
/// it would skip a thread replied to mid-walk (it jumps ahead of the
/// cursor). The walk therefore runs in rounds: a round serves the threads
/// whose activity falls in `(lo, hi]`, where `hi` is the newest activity
/// when the round starts. Threads that gain activity during the round move
/// above `hi` and are served by the next round, so no thread is skipped; a
/// thread can appear twice in one walk, and the later row is the newer one.
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
        activity_key(&b.last_activity)
            .cmp(&activity_key(&a.last_activity))
            .then_with(|| a.thread_id.cmp(&b.thread_id))
    });
    let newest = |threads: &[crate::model::CommentThreadSummary]| {
        threads.first().map(|t| activity_key(&t.last_activity))
    };
    let mut cursor = match page.next_token.as_deref() {
        None => CommentCursor {
            lo: None,
            hi: newest(&threads).unwrap_or_default(),
            at: None,
        },
        Some(token) => {
            let (key, id) = parse_page_token(token, &scope)?;
            let key: CommentCursorKey =
                serde_json::from_str(&key).map_err(|_| "invalid nextToken")?;
            CommentCursor {
                lo: key.lo,
                hi: key.hi,
                at: key.at.map(|at| (at, id)),
            }
        }
    };
    // Advance to the next round while this one has nothing left, so a
    // page is empty only when the walk is over.
    let window = loop {
        let window: Vec<_> = threads
            .iter()
            .filter(|t| cursor.contains(t))
            .cloned()
            .collect();
        let newer = newest(&threads).filter(|n| *n > cursor.hi);
        match newer {
            Some(newer) if window.is_empty() => {
                cursor = CommentCursor {
                    lo: Some(cursor.hi),
                    hi: newer,
                    at: None,
                };
            }
            _ => break window,
        }
    };
    let all_threads = threads;
    let threads = window;
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
    let next = match last {
        Some(idx) => Some(CommentCursor {
            lo: cursor.lo.clone(),
            hi: cursor.hi.clone(),
            at: Some((
                activity_key(&threads[idx].last_activity),
                threads[idx].thread_id.clone(),
            )),
        }),
        // Round done: start another for threads that gained activity.
        None => newest(&all_threads)
            .filter(|n| *n > cursor.hi)
            .map(|newer| CommentCursor {
                lo: Some(cursor.hi.clone()),
                hi: newer,
                at: None,
            }),
    };
    Ok((rows, next.map(|c| c.token(&scope))))
}

/// Fixed-width sort key for a thread's `lastActivity`. RFC 3339 strings do
/// not compare as text once the fraction's trailing zeros are trimmed
/// (`…:00.1Z` sorts after `…:00.12Z`), so a parsed time becomes its
/// zero-padded nanoseconds since the epoch; an unparseable value sorts after
/// every parsed one, by its text.
fn activity_key(at: &str) -> String {
    match crate::clock::parse_iso(at) {
        Some(t) => format!("{:039}", t.unix_timestamp_nanos() + 10_i128.pow(37)),
        None => format!("~{at}"),
    }
}

/// Position of a `comment.list` walk (see [`comment_list_page`]): the
/// activity window `(lo, hi]` of the current round and the last thread
/// served in it (`at` = its `lastActivity` and `threadId`).
struct CommentCursor {
    lo: Option<String>,
    hi: String,
    at: Option<(String, String)>,
}

#[derive(Serialize, serde::Deserialize)]
struct CommentCursorKey {
    lo: Option<String>,
    hi: String,
    at: Option<String>,
}

impl CommentCursor {
    fn contains(&self, t: &crate::model::CommentThreadSummary) -> bool {
        let la = &activity_key(&t.last_activity);
        if *la > self.hi || self.lo.as_ref().is_some_and(|lo| la <= lo) {
            return false;
        }
        match &self.at {
            None => true,
            Some((at, id)) => la < at || (la == at && t.thread_id > *id),
        }
    }

    fn token(&self, scope: &str) -> String {
        let key = CommentCursorKey {
            lo: self.lo.clone(),
            hi: self.hi.clone(),
            at: self.at.as_ref().map(|(at, _)| at.clone()),
        };
        let id = self.at.as_ref().map_or("*", |(_, id)| id.as_str());
        page_token(scope, &serde_json::to_string(&key).unwrap_or_default(), id)
    }
}

/// One bounded page of a note's checkbox rows for `note.listTasks`: rows
/// in line order after the `page` cursor, at most `page.limit` rows and
/// `budget` bytes. Returns the rows and the `nextToken`.
///
/// Nothing is dropped. A first row whose `text` alone overflows the budget
/// is served with a prefix of its `text` and `textTruncated: true`, and the
/// `nextToken` resumes inside that text: the next page starts with the same
/// row (same `lineNumber`) carrying the rest from byte `textOffset` (a UTF-8
/// boundary). Concatenating the `text` of consecutive rows that share a
/// `lineNumber` rebuilds the full text.
///
/// # Errors
/// `"invalid nextToken"` for a token not minted for this note's rows, or one
/// whose text offset is no longer a character boundary inside its row.
pub fn note_tasks_page(
    rows: &[crate::model::NoteTaskRow],
    workspace_id: &str,
    note_id: &str,
    page: &PageRequest,
    budget: usize,
) -> Result<(Vec<serde_json::Value>, Option<String>), &'static str> {
    let scope = format!("note.listTasks:{workspace_id}:{note_id}");
    let (start, mut text_offset) = match page.next_token.as_deref() {
        Some(token) => {
            let (key, _) = parse_page_token(token, &scope)?;
            if let Some((line, offset)) = key.split_once('@') {
                let line: usize = line.parse().map_err(|_| "invalid nextToken")?;
                let offset: usize = offset.parse().map_err(|_| "invalid nextToken")?;
                let idx = rows
                    .iter()
                    .position(|r| r.line_number == line)
                    .ok_or("invalid nextToken")?;
                let text = &rows[idx].text;
                if offset == 0 || offset >= text.len() || !text.is_char_boundary(offset) {
                    return Err("invalid nextToken");
                }
                (idx, offset)
            } else {
                let after: usize = key.parse().map_err(|_| "invalid nextToken")?;
                (rows.partition_point(|r| r.line_number <= after), 0)
            }
        }
        None => (0, 0),
    };
    let mut out = Vec::new();
    let mut used = 2usize;
    for (idx, r) in rows.iter().enumerate().skip(start) {
        if out.len() >= page.limit {
            let prev = &rows[idx - 1];
            return Ok((
                out,
                Some(page_token(&scope, &prev.line_number.to_string(), "line")),
            ));
        }
        let mut row = r.clone();
        row.text = r.text[text_offset..].to_string();
        let offset = text_offset;
        text_offset = 0;
        let mut value = serde_json::to_value(&row).unwrap_or_default();
        if offset > 0 {
            if let Some(obj) = value.as_object_mut() {
                obj.insert("textOffset".into(), serde_json::json!(offset));
            }
        }
        let cost = json_len(&value) + 1;
        if used + cost <= budget {
            used += cost;
            out.push(value);
            continue;
        }
        if !out.is_empty() {
            let prev = &rows[idx - 1];
            return Ok((
                out,
                Some(page_token(&scope, &prev.line_number.to_string(), "line")),
            ));
        }
        // The first row alone overflows: serve a prefix of its text and
        // resume inside it on the next page.
        let rest = row.text.clone();
        row.text.clear();
        let mut shell = serde_json::to_value(&row).unwrap_or_default();
        if let Some(obj) = shell.as_object_mut() {
            obj.insert("textTruncated".into(), serde_json::Value::Bool(true));
            if offset > 0 {
                obj.insert("textOffset".into(), serde_json::json!(offset));
            }
        }
        let room = budget.saturating_sub(used + 1 + json_len(&shell));
        let mut cut = escaped_prefix(&rest, room);
        if cut.is_empty() {
            let first = rest.chars().next().map_or(0, char::len_utf8);
            cut = &rest[..first];
        }
        if let Some(obj) = shell.as_object_mut() {
            obj.insert("text".into(), serde_json::Value::String(cut.to_string()));
            if cut.len() == rest.len() {
                obj.remove("textTruncated");
            }
        }
        out.push(shell);
        if cut.len() == rest.len() {
            // Rounding let the whole remainder fit after all.
            let more = idx + 1 < rows.len();
            return Ok((
                out,
                more.then(|| page_token(&scope, &r.line_number.to_string(), "line")),
            ));
        }
        let key = format!("{}@{}", r.line_number, offset + cut.len());
        return Ok((out, Some(page_token(&scope, &key, "line"))));
    }
    Ok((out, None))
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

    /// RFC 3339 times trim trailing zeros, so `…:00.52Z` sorts before
    /// `…:00.5Z` as text although it is later. A thread replied to in the
    /// same second as the round's newest activity must still be served again.
    #[test]
    fn same_second_replies_compare_as_times_not_text() {
        let at = |frac: &str| format!("2026-11-03T12:00:00{frac}Z");
        let mut threads = vec![
            thread("a", &at(".5"), 1),
            thread("b", &at(".4"), 1),
            thread("c", &at(".3"), 1),
        ];
        let mut seen: Vec<(String, String)> = Vec::new();
        let mut token = None;
        for page_no in 0..10 {
            let page = PageRequest::new(Some(1), 1, 1, token.take());
            let (rows, next) =
                comment_list_page(threads.clone(), "w", "n1", &page, NOTE_READ_BUDGET_BYTES)
                    .unwrap();
            seen.extend(rows.iter().map(|r| {
                (
                    r["threadId"].as_str().unwrap().to_string(),
                    r["lastActivity"].as_str().unwrap().to_string(),
                )
            }));
            let Some(next) = next else { break };
            token = Some(next);
            if page_no == 1 {
                threads[0].last_activity = at(".52");
            }
        }
        let last_a = seen.iter().rev().find(|(id, _)| id == "a").unwrap();
        assert_eq!(last_a.1, at(".52"), "the reply's row is served: {seen:?}");
        for id in ["b", "c"] {
            assert!(seen.iter().any(|(t, _)| t == id), "{id} served: {seen:?}");
        }
        assert!(activity_key(&at("")) < activity_key(&at(".05")));
        assert!(activity_key(&at(".5")) < activity_key(&at(".52")));
        assert_eq!(activity_key(&at(".5")), activity_key(&at(".500")));
    }

    /// A reply bumps `lastActivity`, which moved an unserved thread ahead
    /// of a plain activity keyset so the walk skipped it. Every thread
    /// replied to mid-walk must still be served, with its newest row last.
    #[test]
    fn threads_replied_to_mid_walk_are_not_skipped() {
        let mut threads: Vec<_> = (0..30)
            .map(|i| thread(&format!("th{i:02}"), &format!("t{i:03}"), 1))
            .collect();
        let mut seen: Vec<(String, String)> = Vec::new();
        let mut token = None;
        let mut clock = 100;
        for _ in 0..50 {
            let page = PageRequest::new(Some(4), 4, 4, token.take());
            let (rows, next) =
                comment_list_page(threads.clone(), "w", "n1", &page, NOTE_READ_BUDGET_BYTES)
                    .unwrap();
            seen.extend(rows.iter().map(|r| {
                (
                    r["threadId"].as_str().unwrap().to_string(),
                    r["lastActivity"].as_str().unwrap().to_string(),
                )
            }));
            let Some(next) = next else { break };
            token = Some(next);
            // Reply to the oldest thread not served yet, and to one that was.
            for id in ["th00", "th29"] {
                if clock < 106 {
                    clock += 1;
                    let t = threads.iter_mut().find(|t| t.thread_id == id).unwrap();
                    t.last_activity = format!("t{clock:03}");
                }
            }
        }
        assert!(seen.len() > 30, "replied threads were served again");
        for t in &threads {
            let last = seen
                .iter()
                .rev()
                .find(|(id, _)| *id == t.thread_id)
                .unwrap_or_else(|| panic!("{} was skipped", t.thread_id));
            assert_eq!(
                last.1, t.last_activity,
                "{} ends on its newest row",
                t.thread_id
            );
        }
        assert_eq!(
            comment_list_page(
                threads,
                "w",
                "n1",
                &PageRequest::new(
                    None,
                    4,
                    4,
                    Some(page_token("comment.list:w:n1", "t001", "x"))
                ),
                NOTE_READ_BUDGET_BYTES,
            )
            .unwrap_err(),
            "invalid nextToken"
        );
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
    }

    /// Follow `note.get`'s content cursor and rebuild the content.
    fn rebuild_note_get(note: &crate::model::Note, max: Option<usize>, budget: usize) -> String {
        let mut out = String::new();
        let mut offset = 0;
        for _ in 0..100_000 {
            let row = note_get_row(note, max, offset, budget).unwrap();
            assert!(json_len(&row) <= budget, "row over budget");
            out.push_str(row["content"].as_str().unwrap());
            if row.get("contentTruncated").is_none() {
                assert_eq!(offset, 0);
                return out;
            }
            assert_eq!(
                usize::try_from(row["contentBytes"].as_u64().unwrap()).unwrap(),
                note.content.len()
            );
            assert_eq!(
                usize::try_from(row["contentOffset"].as_u64().unwrap()).unwrap(),
                offset
            );
            match row["nextContentOffset"].as_u64() {
                Some(next) => {
                    let next = usize::try_from(next).unwrap();
                    assert!(next > offset && note.content.is_char_boundary(next));
                    offset = next;
                }
                None => return out,
            }
        }
        panic!("cursor must make progress");
    }

    #[test]
    fn note_get_cursor_rebuilds_an_oversized_note_byte_for_byte() {
        let big = "aé€😀\"\\\t\r\n".repeat(120_000);
        let one_line = "é€😀\"x".repeat(150_000);
        for content in [big.as_str(), one_line.as_str(), "small"] {
            let note = test_note("n", "t", content);
            for (max, budget) in [
                (None, NOTE_READ_BUDGET_BYTES),
                (Some(500_000), NOTE_READ_BUDGET_BYTES),
                (Some(4_099), NOTE_READ_BUDGET_BYTES),
                (None, 50_003),
            ] {
                assert_eq!(
                    rebuild_note_get(&note, max, budget),
                    content,
                    "{max:?} {budget}"
                );
            }
        }
        let small = test_note("n", "t", "small");
        let row = note_get_row(&small, None, 0, NOTE_READ_BUDGET_BYTES).unwrap();
        assert_eq!(row, serde_json::to_value(&small).unwrap());
    }

    #[test]
    fn note_get_cursor_rejects_offsets_off_a_boundary_or_outside_the_content() {
        let note = test_note("n", "t", "éab");
        for offset in [1, 4, 5] {
            let err = note_get_row(&note, None, offset, NOTE_READ_BUDGET_BYTES).unwrap_err();
            assert_eq!(err.offset, offset);
        }
        let row = note_get_row(&note, None, 2, NOTE_READ_BUDGET_BYTES).unwrap();
        assert_eq!(row["content"], "ab");
        assert_eq!(row["contentOffset"], 2);
        assert_eq!(row["nextContentOffset"], serde_json::Value::Null);
        assert_eq!(row["contentTruncated"], true);
    }

    fn task_row(line: usize, text: String) -> crate::model::NoteTaskRow {
        crate::model::NoteTaskRow {
            line_number: line,
            text,
            status: "todo".into(),
            task_note_id: None,
            linked_task_note_id: None,
            depends_on: Vec::new(),
            conflicts_with: Vec::new(),
            unmet_depends_on: Vec::new(),
        }
    }

    /// Page through `rows` and rebuild each row's text by concatenating the
    /// fragments that share its `lineNumber`.
    fn rebuild_tasks(rows: &[crate::model::NoteTaskRow], budget: usize) -> Vec<(u64, String)> {
        let mut rebuilt: Vec<(u64, String)> = Vec::new();
        let mut token = None;
        for _ in 0..100_000 {
            let page =
                PageRequest::new(None, NOTE_TASKS_DEFAULT_LIMIT, NOTE_TASKS_MAX_LIMIT, token);
            let (out, next) = note_tasks_page(rows, "w", "n", &page, budget).unwrap();
            assert!(!out.is_empty());
            assert!(json_len(&out) <= budget, "page over budget");
            for row in &out {
                let line = row["lineNumber"].as_u64().unwrap();
                let text = row["text"].as_str().unwrap();
                match rebuilt.last_mut() {
                    Some((last, acc)) if *last == line => {
                        assert_eq!(
                            usize::try_from(row["textOffset"].as_u64().unwrap()).unwrap(),
                            acc.len()
                        );
                        acc.push_str(text);
                    }
                    _ => {
                        assert!(row.get("textOffset").is_none());
                        rebuilt.push((line, text.to_string()));
                    }
                }
            }
            match next {
                Some(t) => token = Some(t),
                None => return rebuilt,
            }
        }
        panic!("cursor must make progress");
    }

    #[test]
    fn an_oversized_task_text_resumes_on_the_next_page_and_rebuilds() {
        let huge = "aé€😀\"\\\t".repeat(60_000);
        let rows = vec![
            task_row(1, "small".into()),
            task_row(3, huge.clone()),
            task_row(5, "middle".into()),
            task_row(7, format!("{huge}{huge}")),
            task_row(9, "last".into()),
        ];
        for budget in [NOTE_READ_BUDGET_BYTES, 100_003, 4_097] {
            let rebuilt = rebuild_tasks(&rows, budget);
            let expected: Vec<(u64, String)> = rows
                .iter()
                .map(|r| (r.line_number as u64, r.text.clone()))
                .collect();
            assert_eq!(rebuilt, expected, "budget {budget}");
        }
        // The first page cuts the oversized row and marks it.
        let page = PageRequest::new(None, NOTE_TASKS_DEFAULT_LIMIT, NOTE_TASKS_MAX_LIMIT, None);
        let (out, next) = note_tasks_page(&rows[1..2], "w", "n", &page, 4_097).unwrap();
        assert_eq!(out[0]["textTruncated"], true);
        assert!(next.is_some());
    }

    #[test]
    fn a_task_text_cursor_off_a_boundary_or_outside_the_row_is_rejected() {
        let rows = vec![task_row(1, "éabc".into()), task_row(3, "x".into())];
        let scope = "note.listTasks:w:n";
        for key in ["1@1", "1@5", "1@0", "2@1", "1@x", "x@1"] {
            let page = PageRequest::new(
                None,
                NOTE_TASKS_DEFAULT_LIMIT,
                NOTE_TASKS_MAX_LIMIT,
                Some(page_token(scope, key, "line")),
            );
            assert_eq!(
                note_tasks_page(&rows, "w", "n", &page, NOTE_READ_BUDGET_BYTES).unwrap_err(),
                "invalid nextToken",
                "{key}"
            );
        }
        let page = PageRequest::new(
            None,
            NOTE_TASKS_DEFAULT_LIMIT,
            NOTE_TASKS_MAX_LIMIT,
            Some(page_token(scope, "1@2", "line")),
        );
        let (out, _) = note_tasks_page(&rows, "w", "n", &page, NOTE_READ_BUDGET_BYTES).unwrap();
        assert_eq!(out[0]["text"], "abc");
        assert_eq!(out[0]["textOffset"], 2);
        assert_eq!(out[1]["text"], "x");
    }

    #[test]
    fn escaped_size_counts_escapes() {
        assert_eq!(escaped_str_len("ab"), 2);
        assert_eq!(escaped_str_len("\"\\"), 4);
        assert_eq!(escaped_str_len("\u{1}"), 6);
        assert_eq!(json_len(&serde_json::json!({ "a": 1 })), 7);
    }
}
