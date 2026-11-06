//! Byte-budgeted pages for the forge proxy reads (`github.*` lists, Linear
//! issue lists — PROTOCOL §5.27 / §5.28).
//!
//! The upstream decides how large one page is: GitHub and Linear bodies run to
//! tens of KiB each, so a full page of 100 items can weigh megabytes. These
//! helpers keep one reply under the read payload budget without dropping or
//! cutting data that has no other read path:
//!
//! - **Page size.** `limit` defaults to 50 and is clamped to `[1, 100]`.
//! - **Byte budget.** Items are served in upstream order until the next one
//!   would push the page past [`PAGE_ITEMS_BUDGET_BYTES`]. When the page is
//!   cut short, `nextToken` resumes inside the same upstream page: it carries
//!   the upstream cursor of that page, the number of items already served
//!   from it, and the page size it was fetched with (a resume token's page
//!   size overrides the caller's `limit`, so the offset stays aligned).
//! - **Oversized item.** The first item of a page is always served. If it
//!   alone exceeds the budget, its longest strings are cut to a common
//!   length (on a UTF-8 boundary) and the item gains `contentTruncated: true`.
//!
//! Tokens are opaque base64 JSON (`{"c": cursor}`; a resume adds `"s"` and
//! `"n"`), the same encoding [`crate::github_ops::encode_next_token`] uses, so
//! a token from an older daemon still decodes.

use base64::Engine as _;
use serde_json::{json, Value};

/// Default page size of the forge proxy list reads.
pub(crate) const DEFAULT_PAGE_LIMIT: i64 = 50;
/// Hard maximum page size of the forge proxy list reads (GitHub REST's own
/// `per_page` maximum; Linear allows 250).
pub(crate) const MAX_PAGE_LIMIT: i64 = 100;
/// Bytes of serialized result one page may take (the shared wave-wide page
/// budget), so the JSON-RPC frame stays under the 512,000-byte read budget.
pub(crate) const PAGE_RESULT_BUDGET_BYTES: usize = 480_000;
/// Bytes of compact JSON one page's items may take: the result budget less
/// room for the envelope (`nextToken`, `headSha`, `truncated`).
pub(crate) const PAGE_ITEMS_BUDGET_BYTES: usize = PAGE_RESULT_BUDGET_BYTES - 2_048;
/// Marker set on an item whose long strings were cut to fit the budget.
pub(crate) const CONTENT_TRUNCATED_KEY: &str = "contentTruncated";

/// A decoded `nextToken`: the upstream cursor of the page to fetch, how many
/// of its items were already served, and the page size it was fetched with.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct PageToken {
    pub cursor: Option<String>,
    pub skip: usize,
    pub limit: Option<i64>,
}

/// One page request: the decoded token and the effective page size.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PageRequest {
    pub token: PageToken,
    pub limit: u8,
}

impl PageRequest {
    /// Decode `next_token` and clamp `limit` into `[1, 100]` (default 50). A
    /// resume token's page size wins over `limit`.
    pub(crate) fn new(limit: Option<i64>, next_token: Option<&str>) -> Self {
        let token = decode_page_token(next_token);
        let limit = clamp_page_limit(token.limit.or(limit));
        Self { token, limit }
    }

    /// The upstream cursor to fetch.
    pub(crate) fn cursor(&self) -> Option<String> {
        self.token.cursor.clone()
    }

    /// Fit the fetched upstream page into the byte budget: returns the items
    /// to serve and the wire `nextToken`.
    pub(crate) fn finish(
        &self,
        items: Vec<Value>,
        upstream_next: Option<&str>,
    ) -> (Vec<Value>, Value) {
        budget_page(items, self, upstream_next, PAGE_ITEMS_BUDGET_BYTES)
    }
}

/// Render upstream items to JSON values for [`PageRequest::finish`].
pub(crate) fn to_items<T: serde::Serialize>(items: &[T]) -> intent_core::Result<Vec<Value>> {
    items
        .iter()
        .map(serde_json::to_value)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| intent_core::Error::Internal(format!("serialize result failed: {e}")))
}

/// Clamp a wire `limit` into `[1, MAX_PAGE_LIMIT]`, defaulting to
/// [`DEFAULT_PAGE_LIMIT`].
pub(crate) fn clamp_page_limit(limit: Option<i64>) -> u8 {
    u8::try_from(limit.unwrap_or(DEFAULT_PAGE_LIMIT).clamp(1, MAX_PAGE_LIMIT))
        .expect("clamped to [1, 100]")
}

/// Decode an opaque `nextToken`. Absent or malformed → the first page.
pub(crate) fn decode_page_token(token: Option<&str>) -> PageToken {
    let Some(v) = token
        .and_then(|t| {
            base64::engine::general_purpose::STANDARD_NO_PAD
                .decode(t)
                .ok()
        })
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
    else {
        return PageToken::default();
    };
    PageToken {
        cursor: v.get("c").and_then(Value::as_str).map(String::from),
        skip: v
            .get("s")
            .and_then(Value::as_u64)
            .and_then(|s| usize::try_from(s).ok())
            .unwrap_or(0),
        limit: v.get("n").and_then(Value::as_i64),
    }
}

/// Encode a token that resumes `cursor`'s page after `skip` items.
fn encode_resume_token(cursor: Option<&str>, skip: usize, limit: u8) -> String {
    base64::engine::general_purpose::STANDARD_NO_PAD.encode(
        serde_json::to_vec(&json!({ "c": cursor, "s": skip, "n": limit }))
            .expect("token json is serializable"),
    )
}

fn json_len(v: &Value) -> usize {
    serde_json::to_vec(v).map_or(usize::MAX, |b| b.len())
}

/// Serve `items` (one upstream page) from the request's offset until the next
/// item would exceed `budget` bytes. See the module docs.
pub(crate) fn budget_page(
    items: Vec<Value>,
    request: &PageRequest,
    upstream_next: Option<&str>,
    budget: usize,
) -> (Vec<Value>, Value) {
    let skip = request.token.skip;
    let mut served = Vec::new();
    // `[` + `]`, plus one comma per item after the first.
    let mut used = 2usize;
    for (index, mut item) in items.into_iter().enumerate().skip(skip) {
        let mut size = json_len(&item) + usize::from(!served.is_empty());
        if served.is_empty() && used + size > budget {
            shrink_to_fit(&mut item, budget.saturating_sub(used));
            size = json_len(&item);
        } else if used + size > budget {
            let token = encode_resume_token(request.token.cursor.as_deref(), index, request.limit);
            return (served, Value::String(token));
        }
        used += size;
        served.push(item);
    }
    (served, crate::github_ops::next_token_value(upstream_next))
}

/// Cut every string in `item` longer than a common length so the item fits in
/// `budget` bytes, and mark it with [`CONTENT_TRUNCATED_KEY`].
pub(crate) fn shrink_to_fit(item: &mut Value, budget: usize) {
    if let Value::Object(map) = item {
        map.insert(CONTENT_TRUNCATED_KEY.into(), Value::Bool(true));
    }
    if json_len(item) <= budget {
        return;
    }
    let original = item.clone();
    let (mut lo, mut hi) = (0usize, longest_string(&original));
    // Largest cap that fits; 0 when even empty strings do not.
    while lo < hi {
        let mid = lo + (hi - lo).div_ceil(2);
        let mut candidate = original.clone();
        cap_strings(&mut candidate, mid);
        if json_len(&candidate) <= budget {
            lo = mid;
        } else {
            hi = mid - 1;
        }
    }
    *item = original;
    cap_strings(item, lo);
}

fn longest_string(v: &Value) -> usize {
    match v {
        Value::String(s) => s.len(),
        Value::Array(a) => a.iter().map(longest_string).max().unwrap_or(0),
        Value::Object(m) => m.values().map(longest_string).max().unwrap_or(0),
        _ => 0,
    }
}

fn cap_strings(v: &mut Value, cap: usize) {
    match v {
        Value::String(s) if s.len() > cap => {
            let mut end = cap;
            while !s.is_char_boundary(end) {
                end -= 1;
            }
            s.truncate(end);
        }
        Value::Array(a) => a.iter_mut().for_each(|x| cap_strings(x, cap)),
        Value::Object(m) => m.values_mut().for_each(|x| cap_strings(x, cap)),
        _ => {}
    }
}

#[cfg(test)]
mod tests;
