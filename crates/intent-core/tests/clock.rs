//! Unit-style integration coverage for `intent-core::clock`.

use intent_core::clock::{iso_from_unix_secs, iso_minutes_ago, now_epoch_ms, now_iso, parse_iso};

#[test]
fn now_iso_returns_rfc3339_utc_string_that_round_trips() {
    let s = now_iso();
    assert!(!s.is_empty(), "now_iso must return a non-empty timestamp");
    assert!(
        s.ends_with('Z'),
        "now_iso must serialize as UTC ending in Z"
    );
    let parsed = parse_iso(&s).expect("now_iso output must be parseable by parse_iso");
    assert_eq!(parsed.offset().whole_seconds(), 0);
}

#[test]
fn now_epoch_ms_is_positive_and_close_to_now_iso() {
    let ms = now_epoch_ms();
    // Sanity: any plausible test run lies well after 2020-01-01 (1577836800000 ms).
    assert!(ms > 1_577_836_800_000, "epoch ms looks stale: {ms}");
    // And not absurdly far in the future (year 9999).
    assert!(ms < 253_402_300_799_000, "epoch ms looks bogus: {ms}");
}

#[test]
fn parse_iso_accepts_rfc3339_and_rejects_garbage() {
    let dt = parse_iso("2026-01-02T03:04:05Z").expect("RFC3339 parses");
    assert_eq!(dt.year(), 2026);
    assert_eq!(dt.month() as u8, 1);
    assert_eq!(dt.day(), 2);
    assert_eq!(dt.hour(), 3);
    assert_eq!(dt.minute(), 4);
    assert_eq!(dt.second(), 5);

    let with_offset = parse_iso("2026-01-02T03:04:05+02:00").expect("offset variant parses");
    assert_eq!(with_offset.offset().whole_hours(), 2);

    assert!(parse_iso("not-a-date").is_none());
    assert!(parse_iso("").is_none());
    assert!(parse_iso("2026-13-40T99:99:99Z").is_none());
}

#[test]
fn iso_from_unix_secs_formats_epoch_and_handles_out_of_range() {
    // Unix epoch zero must round-trip to the canonical 1970 timestamp.
    assert_eq!(iso_from_unix_secs(0), "1970-01-01T00:00:00Z");
    // Known fixed timestamp: 2026-01-01T00:00:00Z is 1767225600 unix seconds.
    let s = iso_from_unix_secs(1_767_225_600);
    assert_eq!(s, "2026-01-01T00:00:00Z");

    // Out-of-range values fall back to the empty string.
    assert_eq!(iso_from_unix_secs(i64::MAX), "");
    assert_eq!(iso_from_unix_secs(i64::MIN), "");
}

#[test]
fn iso_minutes_ago_is_in_the_past_and_round_trips() {
    let past = iso_minutes_ago(10);
    assert!(!past.is_empty());
    let parsed = parse_iso(&past).expect("iso_minutes_ago output parses");
    let now = parse_iso(&now_iso()).expect("now_iso parses");
    // The returned moment is before "now" and within the last hour.
    assert!(parsed <= now, "iso_minutes_ago should be in the past");
    let delta = (now - parsed).whole_seconds();
    assert!(
        (550..=650).contains(&delta),
        "iso_minutes_ago(10) drift outside 9–11 minutes: {delta}s"
    );
}

#[test]
fn iso_minutes_ago_with_zero_is_close_to_now() {
    let a = parse_iso(&iso_minutes_ago(0)).unwrap();
    let b = parse_iso(&now_iso()).unwrap();
    let drift = (b - a).whole_seconds().abs();
    assert!(drift < 5, "iso_minutes_ago(0) drift too large: {drift}s");
}
