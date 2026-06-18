//! Timestamp helper (§9.6).
//!
//! All timestamps are RFC-3339 / ISO-8601 UTC strings to match the existing
//! wire format (TS `Date.toISOString()`). Always format timestamps through
//! [`now_iso`]; never format them ad hoc.

use time::format_description::well_known::Rfc3339;
use time::{Duration, OffsetDateTime};

/// Current UTC time as an RFC-3339 string.
pub fn now_iso() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_default()
}

/// Parse an RFC-3339 / ISO-8601 timestamp, returning `None` when malformed.
/// Used by `comment.list` `since` filtering (the TS `new Date(since)` guard).
pub fn parse_iso(s: &str) -> Option<OffsetDateTime> {
    OffsetDateTime::parse(s, &Rfc3339).ok()
}

/// UTC timestamp `minutes` minutes in the past, as an RFC-3339 string. Backs the
/// `event.*` time-window queries (`inLast` / `minutesAgo`), mirroring the TS
/// `new Date(Date.now() - minutesAgo * 60 * 1000).toISOString()`.
pub fn iso_minutes_ago(minutes: i64) -> String {
    (OffsetDateTime::now_utc() - Duration::minutes(minutes))
        .format(&Rfc3339)
        .unwrap_or_default()
}
