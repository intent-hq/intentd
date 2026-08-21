//! Timestamp helper (§9.6).
//!
//! All timestamps are RFC-3339 / ISO-8601 UTC strings to match the existing
//! wire format (TS `Date.toISOString()`). Always format timestamps through
//! [`now_iso`]; never format them ad hoc.

use time::format_description::well_known::Rfc3339;
use time::{Duration, OffsetDateTime};

/// Current UTC time as an RFC-3339 string.
#[must_use]
pub fn now_iso() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_default()
}

/// Current UTC time as whole milliseconds since the Unix epoch. Backs the
/// `SetupScript.updatedAt` epoch-ms field (§5.25), mirroring TS `Date.now()`.
#[must_use]
pub fn now_epoch_ms() -> u64 {
    let ns = OffsetDateTime::now_utc().unix_timestamp_nanos();
    if ns <= 0 {
        0
    } else {
        (ns / 1_000_000) as u64
    }
}

/// Parse an RFC-3339 / ISO-8601 timestamp, returning `None` when malformed.
/// Used by `comment.list` `since` filtering (the TS `new Date(since)` guard).
#[must_use]
pub fn parse_iso(s: &str) -> Option<OffsetDateTime> {
    OffsetDateTime::parse(s, &Rfc3339).ok()
}

/// Format a Unix timestamp (whole seconds since the epoch) as an RFC-3339 UTC
/// string. Used to render git commit times (`git log %aI`) for the
/// `file-tracking.loadCommits` wire shape. Returns an empty string when the
/// value is out of range.
#[must_use]
pub fn iso_from_unix_secs(secs: i64) -> String {
    OffsetDateTime::from_unix_timestamp(secs)
        .ok()
        .and_then(|dt| dt.format(&Rfc3339).ok())
        .unwrap_or_default()
}

/// UTC timestamp `minutes` minutes in the past, as an RFC-3339 string. Backs the
/// `event.*` time-window queries (`inLast` / `minutesAgo`), mirroring the TS
/// `new Date(Date.now() - minutesAgo * 60 * 1000).toISOString()`.
#[must_use]
pub fn iso_minutes_ago(minutes: i64) -> String {
    (OffsetDateTime::now_utc() - Duration::minutes(minutes))
        .format(&Rfc3339)
        .unwrap_or_default()
}

/// UTC timestamp `ms` milliseconds in the future, as an RFC-3339 string. Backs
/// the delete grace window's `deleteAt` deadline (`now + undoDelayMs`, §5.1).
#[must_use]
pub fn iso_ms_from_now(ms: u64) -> String {
    (OffsetDateTime::now_utc() + Duration::milliseconds(ms.cast_signed()))
        .format(&Rfc3339)
        .unwrap_or_default()
}
