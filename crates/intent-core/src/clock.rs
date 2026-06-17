//! Timestamp helper (§9.6).
//!
//! All timestamps are RFC-3339 / ISO-8601 UTC strings to match the existing
//! wire format (TS `Date.toISOString()`). Always format timestamps through
//! [`now_iso`]; never format them ad hoc.

use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

/// Current UTC time as an RFC-3339 string.
pub fn now_iso() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_default()
}
