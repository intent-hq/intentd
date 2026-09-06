//! Reusable pagination contract (PROTOCOL §5.5 / R2-10).
//!
//! A single helper backing the paginated reads (`agent.getConversation`,
//! `event.query`, `file-tracking.loadCommits`, terminal/script historical
//! scrollback). The contract is: reverse-chronological backward paging
//! (newest page first, follow the token to older pages), a server-clamped
//! `limit` in `[1, 200]` defaulting to 50, and an **opaque** base64
//! continuation token (never a raw numeric offset on the wire).
//!
//! ## Append-stability
//! The token encodes the *oldest-indexed* boundary of the next (older) page
//! (`{"b": <index-from-oldest>}`). Because new items are always appended at the
//! newest end, an item's index counted from the oldest end never shifts under
//! appends, so following a token returns exactly the same older items
//! regardless of how many items were appended in the meantime (Q13). This holds
//! as long as items are not pruned from the oldest end between calls.
//!
//! The token's internal encoding is a private implementation detail: callers
//! and clients MUST treat it as opaque.

use base64::Engine as _;
use serde_json::{json, Value};

/// Default page size when the client omits `limit`.
pub(crate) const DEFAULT_PAGE_LIMIT: usize = 50;
/// Hard server-side cap on page size.
pub(crate) const MAX_PAGE_LIMIT: usize = 200;

/// Clamp a client-supplied `limit` into the contract range. `None` yields the
/// default (50); zero/negative clamp up to 1; values over the cap clamp down to
/// 200.
#[must_use]
pub fn clamp_limit(limit: Option<i64>) -> usize {
    match limit {
        None => DEFAULT_PAGE_LIMIT,
        Some(l) => usize::try_from(l.clamp(1, i64::try_from(MAX_PAGE_LIMIT).unwrap_or(i64::MAX)))
            .unwrap_or(MAX_PAGE_LIMIT),
    }
}

/// Encode an opaque continuation token from its internal JSON cursor.
fn encode_token(cursor: &Value) -> String {
    base64::engine::general_purpose::STANDARD_NO_PAD
        .encode(serde_json::to_vec(cursor).expect("cursor json is always serializable"))
}

/// Decode an opaque continuation token back into its JSON cursor. A malformed
/// token decodes to `None`, which callers treat as "start from the newest page".
fn decode_token(token: &str) -> Option<Value> {
    let bytes = base64::engine::general_purpose::STANDARD_NO_PAD
        .decode(token)
        .ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// Decode the oldest-indexed exclusive end carried by a backward page token.
/// Malformed and non-backward tokens return `None`, matching [`page_window`]'s
/// behavior of restarting from the newest page.
pub(crate) fn backward_page_boundary(token: &str) -> Option<usize> {
    usize::try_from(decode_token(token)?.get("b").and_then(Value::as_u64)?).ok()
}

/// The half-open `start..end` slice (in the source's oldest→newest order) that
/// makes up the requested page, plus the token for the next (older) page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageWindow {
    pub start: usize,
    pub end: usize,
    pub next_token: Option<String>,
}

/// Resolve the page window over a chronological (oldest→newest) collection of
/// `len` items. The first page (no token) is the newest `limit` items; each
/// `next_token` walks one `limit`-sized page older. Returns `next_token` only
/// while older items remain.
pub fn page_window(len: usize, limit: Option<i64>, token: Option<&str>) -> PageWindow {
    let limit = clamp_limit(limit);
    // `end` is the exclusive upper bound (oldest-indexed) of this page; absent a
    // token we start at the newest end. Clamp to `len` so a stale token against
    // a shrunk collection degrades gracefully rather than panicking.
    let end = token
        .and_then(backward_page_boundary)
        .map_or(len, |boundary| boundary.min(len));
    let start = end.saturating_sub(limit);
    let next_token = if start > 0 {
        Some(encode_token(&json!({ "b": start })))
    } else {
        None
    };
    PageWindow {
        start,
        end,
        next_token,
    }
}

/// A seek page window (`agent.getConversation` `aroundMessageId` and its
/// forward continuations): the `start..end` slice plus BOTH continuation
/// tokens — `next_token` walks older history using the standard backward
/// cursor (`{"b": ..}`), so the older chain resolves through [`page_window`]
/// exactly like ordinary paging, while `prev_token` walks newer toward the
/// live tail using a forward cursor (`{"f": <start-from-oldest>}`). Both
/// cursors index from the oldest end, so both are append-stable (Q13), and
/// both are opaque base64 on the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SeekPageWindow {
    pub start: usize,
    pub end: usize,
    pub next_token: Option<String>,
    pub prev_token: Option<String>,
}

/// Build a [`SeekPageWindow`] over `start..end`, minting the older token while
/// rows remain below `start` and the newer token while rows remain at/after
/// `end`.
fn seek_window(len: usize, start: usize, end: usize) -> SeekPageWindow {
    SeekPageWindow {
        start,
        end,
        next_token: (start > 0).then(|| encode_token(&json!({ "b": start }))),
        prev_token: (end < len).then(|| encode_token(&json!({ "f": end }))),
    }
}

/// Resolve the page window centered on `index` (0-based, oldest→newest) — the
/// `aroundMessageId` seek. Half the page budget goes to rows older than the
/// target and the rest to the target and newer rows, clamped at either edge so
/// the page stays full whenever `len >= limit`; the target index is always
/// inside `start..end` (given `index < len`).
pub(crate) fn page_window_around(len: usize, limit: Option<i64>, index: usize) -> SeekPageWindow {
    let limit = clamp_limit(limit);
    let start = index.min(len).saturating_sub(limit / 2);
    let end = (start + limit).min(len);
    let start = end.saturating_sub(limit);
    seek_window(len, start, end)
}

/// Try to resolve `token` as a forward continuation cursor (`{"f": ..}`)
/// minted by a seek page's `prev_token`. Returns `None` for backward or
/// malformed tokens, which callers fall through to the [`page_window`]
/// contract — so pre-existing backward tokens keep byte-identical behavior.
pub(crate) fn forward_page_window(
    len: usize,
    limit: Option<i64>,
    token: &str,
) -> Option<SeekPageWindow> {
    let f = usize::try_from(decode_token(token)?.get("f").and_then(Value::as_u64)?)
        .expect("value fits in usize");
    let limit = clamp_limit(limit);
    let start = f.min(len);
    let end = (start + limit).min(len);
    Some(seek_window(len, start, end))
}

/// Admission anchor for [`budget_page`] — which end of a page the byte
/// budget grows from, mirroring the direction its consumer walks:
///
/// - `Newest`: legacy backward pages (client walks newest→oldest), so the
///   newest suffix is kept and the oldest rows are dropped.
/// - `Oldest`: forward (`prevToken`-minted) continuations (client walks
///   oldest→newest toward the live tail), so the oldest prefix is kept.
/// - `Target(pos)`: seek pages (`aroundMessageId` / `aroundIndex`) — `pos`
///   is the target's position WITHIN the page slice; the target is always
///   kept (even alone over budget) and the page grows outward from it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BudgetAnchor {
    Newest,
    Oldest,
    Target(usize),
}

/// Trim a page to a byte budget (the §5.5 slim page budget): given each
/// row's serialized size, return the kept contiguous `lo..hi` subrange of
/// the page slice. The anchor row is always admitted — a single row over
/// budget still serves alone (never an empty page, no infinite loop) — and
/// further rows are admitted only while the cumulative size stays within
/// `budget`, stopping at the first row that would exceed it (pages stay
/// contiguous; rows are never skipped over). `Target` grows outward
/// alternating older-first (mirroring the unbudgeted half-older split);
/// each direction stops independently at its first non-fitting row.
#[must_use]
pub fn budget_page(sizes: &[usize], anchor: BudgetAnchor, budget: usize) -> (usize, usize) {
    let n = sizes.len();
    if n == 0 {
        return (0, 0);
    }
    match anchor {
        BudgetAnchor::Newest => {
            let mut lo = n;
            let mut total = 0usize;
            while lo > 0 {
                let s = sizes[lo - 1];
                if lo != n && total.saturating_add(s) > budget {
                    break;
                }
                total = total.saturating_add(s);
                lo -= 1;
            }
            (lo, n)
        }
        BudgetAnchor::Oldest => {
            let mut hi = 0;
            let mut total = 0usize;
            while hi < n {
                let s = sizes[hi];
                if hi != 0 && total.saturating_add(s) > budget {
                    break;
                }
                total = total.saturating_add(s);
                hi += 1;
            }
            (0, hi)
        }
        BudgetAnchor::Target(pos) => {
            let pos = pos.min(n - 1);
            let mut lo = pos;
            let mut hi = pos + 1;
            let mut total = sizes[pos];
            let mut can_older = lo > 0;
            let mut can_newer = hi < n;
            while can_older || can_newer {
                if can_older {
                    let s = sizes[lo - 1];
                    if total.saturating_add(s) <= budget {
                        total = total.saturating_add(s);
                        lo -= 1;
                        can_older = lo > 0;
                    } else {
                        can_older = false;
                    }
                }
                if can_newer {
                    let s = sizes[hi];
                    if total.saturating_add(s) <= budget {
                        total = total.saturating_add(s);
                        hi += 1;
                        can_newer = hi < n;
                    } else {
                        can_newer = false;
                    }
                }
            }
            (lo, hi)
        }
    }
}

/// Re-mint the backward continuation token after a budget trim moved a
/// page's effective start: `{"b": start}` while older rows remain. Same
/// cursor shape as [`page_window`], so the resumed page picks up exactly at
/// the first excluded (older) row.
#[must_use]
pub fn remint_backward_token(start: usize) -> Option<String> {
    (start > 0).then(|| encode_token(&json!({ "b": start })))
}

/// Re-mint the forward continuation token after a budget trim moved a
/// page's effective end: `{"f": end}` while newer rows remain. Same cursor
/// shape as [`seek_window`], so the resumed page picks up exactly at the
/// first excluded (newer) row.
pub(crate) fn remint_forward_token(end: usize, len: usize) -> Option<String> {
    (end < len).then(|| encode_token(&json!({ "f": end })))
}

/// Serialized JSON byte size of one served row, for the §5.5 slim page
/// budget. Counted through a discarding writer so no page-sized string is
/// allocated just to be measured; a row that fails to serialize measures 0
/// and is admitted (fail-open, matching [`intent_core::slim_body_size`]).
/// Shared by the paginated read (`agent_ops`) and the seq-0 chat snapshot's
/// live-turn merge (`intent-transport`), so both sides of the budget agree
/// on what a row weighs.
pub fn serialized_size<T: serde::Serialize>(row: &T) -> usize {
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
    let mut sink = CountingSink(0);
    serde_json::to_writer(&mut sink, row).map_or(0, |()| sink.0)
}

/// A page of items plus the opaque token for the next (older) page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Page<T> {
    pub items: Vec<T>,
    pub next_token: Option<String>,
}

/// Paginate a chronological (oldest→newest) slice into a newest→oldest page.
/// Items within the returned page are ordered newest-first; `next_token` follows
/// the contract in [`page_window`].
pub(crate) fn paginate_slice<T: Clone>(
    source: &[T],
    limit: Option<i64>,
    token: Option<&str>,
) -> Page<T> {
    let win = page_window(source.len(), limit, token);
    let items = source[win.start..win.end].iter().rev().cloned().collect();
    Page {
        items,
        next_token: win.next_token,
    }
}

/// Paginate a plaintext scrollback buffer into a newest→oldest page of lines,
/// returning the `{ items, nextToken }` envelope used by the terminal/script
/// historical-output reads. Trailing blank lines are trimmed (mirroring the
/// legacy formatted reads) before paging; the per-page size follows the standard
/// clamp (default 50, max 200) and the token is append-stable per [`page_window`].
pub(crate) fn paginate_text_lines(text: &str, limit: Option<i64>, token: Option<&str>) -> Value {
    let mut lines: Vec<&str> = text.split('\n').collect();
    while lines.last().is_some_and(|l| l.trim().is_empty()) {
        lines.pop();
    }
    let page = paginate_slice(&lines, limit, token);
    json!({
        "items": page.items,
        "nextToken": page.next_token,
    })
}

/// Encode an opaque continuation token for offset/skip-style backward paging
/// (used by the store-backed `event.query` and git `file-tracking.loadCommits`
/// reads, whose sources are already newest-first and expose a native
/// `LIMIT`/`OFFSET`). The wire form is opaque base64; the internal
/// `{ "o": <offset> }` is a private detail clients MUST NOT depend on.
///
/// Unlike [`page_window`], offset paging anchors on the *newest* end, so it is
/// not append-stable against inserts at the newest end — appropriate for
/// time-windowed event queries and immutable commit history, where the live
/// tail is read separately (Q13).
pub(crate) fn offset_token(next_offset: usize) -> String {
    encode_token(&json!({ "o": next_offset }))
}

/// Decode an offset-style continuation token into its offset. A missing or
/// malformed token starts from offset 0 (the newest page).
pub(crate) fn parse_offset(token: Option<&str>) -> usize {
    token
        .and_then(decode_token)
        .and_then(|v| v.get("o").and_then(Value::as_u64))
        .map_or(0, |n| usize::try_from(n).expect("value fits in usize"))
}

#[cfg(test)]
mod tests;
