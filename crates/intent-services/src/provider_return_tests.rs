//! Pure parts of returning to the home provider: reset-time parsing, the
//! backoff schedule and the state transitions.

use chrono::{DateTime, Duration, Local, Timelike, Utc};

use super::*;

fn utc(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
}

const NOW: &str = "2026-10-22T10:00:00Z";

#[test]
fn parses_iso_timestamps_with_and_without_offsets() {
    let now = utc(NOW);
    assert_eq!(
        parse_reset_time("rate_limit_error: resets at 2026-10-22T15:30:00Z", now),
        Some(utc("2026-10-22T15:30:00Z"))
    );
    assert_eq!(
        parse_reset_time("limit resets 2026-10-22T17:30:00+02:00", now),
        Some(utc("2026-10-22T15:30:00Z"))
    );
    assert_eq!(
        parse_reset_time("{\"resetsAt\":\"2026-10-22 12:00\"}", now),
        Some(utc("2026-10-22T12:00:00Z")),
        "no offset reads as UTC"
    );
    assert_eq!(
        parse_reset_time("resets at 2026-10-21T09:00:00Z", now),
        None,
        "a reset already past is not usable"
    );
}

#[test]
fn parses_a_unix_epoch_after_a_bar() {
    let now = utc(NOW);
    let reset = utc("2026-10-22T14:00:00Z");
    let text = format!("Claude AI usage limit reached|{}", reset.timestamp());
    assert_eq!(parse_reset_time(&text, now), Some(reset));
}

#[test]
fn parses_relative_phrasings() {
    let now = utc(NOW);
    for (text, expected) in [
        (
            "Rate limited. Try again in 3 hours.",
            now + Duration::hours(3),
        ),
        ("please retry after 90 seconds", now + Duration::seconds(90)),
        ("quota resets in 45 minutes", now + Duration::minutes(45)),
        (
            "usage limit; try again in 1.5h",
            now + Duration::minutes(90),
        ),
        ("Try again in 2 days", now + Duration::days(2)),
    ] {
        assert_eq!(parse_reset_time(text, now), Some(expected), "{text}");
    }
}

#[test]
fn parses_clock_times_in_utc() {
    let now = utc(NOW);
    assert_eq!(
        parse_reset_time("5-hour limit reached, resets at 17:00 UTC", now),
        Some(utc("2026-10-22T17:00:00Z"))
    );
    assert_eq!(
        parse_reset_time("limit resets at 9:30 UTC", now),
        Some(utc("2026-10-23T09:30:00Z")),
        "a clock time already past today means tomorrow"
    );
}

#[test]
fn parses_clock_times_in_local_time_without_a_zone() {
    let now = utc(NOW);
    for text in [
        "5-hour limit reached ∙ resets 5pm",
        "limit resets at 5:00 PM",
    ] {
        let parsed = parse_reset_time(text, now).expect(text);
        let local = parsed.with_timezone(&Local);
        assert_eq!((local.hour(), local.minute()), (17, 0), "{text}");
        assert!(
            parsed > now && parsed <= now + Duration::days(1),
            "{text}: {parsed}"
        );
    }
}

#[test]
fn anything_unparseable_is_none() {
    let now = utc(NOW);
    for text in [
        "429 Too Many Requests",
        "rate_limit_error: usage limit reached",
        "resets 3 times a day",
        "try again later",
        "",
    ] {
        assert_eq!(parse_reset_time(text, now), None, "{text:?}");
    }
}

#[test]
fn backoff_doubles_from_one_hour_and_caps_at_a_day() {
    let hours: Vec<i64> = (1..=8).map(|s| backoff_delay(s).num_hours()).collect();
    assert_eq!(hours, vec![1, 2, 4, 8, 16, 24, 24, 24]);
    assert_eq!(backoff_delay(1_000).num_hours(), 24);
}

#[test]
fn leaving_home_records_home_and_the_reported_or_fallback_time() {
    let now = utc(NOW);
    let reported = utc("2026-10-22T15:00:00Z");
    let state = on_switch(None, "claude-code", "claude-code@pb1", Some(reported), now);
    assert_eq!(
        state,
        ProviderReturnState {
            home: "claude-code".into(),
            return_after: "2026-10-22T15:00:00Z".into(),
            backoff_step: 0,
            away_on: "claude-code@pb1".into(),
            returning: None,
        }
    );
    let fallback = on_switch(None, "claude-code", "claude-code@pb1", None, now);
    assert_eq!(fallback.return_after, "2026-10-22T15:00:00Z", "now + 5h");
    let absurd = on_switch(
        None,
        "claude-code",
        "claude-code@pb1",
        Some(now + Duration::days(30)),
        now,
    );
    assert_eq!(
        absurd.return_after, "2026-10-22T15:00:00Z",
        "an untrusted time falls back"
    );
}

#[test]
fn a_failed_return_pushes_the_timer_out_with_backoff() {
    let now = utc(NOW);
    let away = on_switch(None, "claude-code", "claude-code@pb1", None, now);
    let mut state = away;
    let mut afters = Vec::new();
    for _ in 0..3 {
        state.returning = Some(ReturnMode::Timer);
        state = on_switch(Some(state), "claude-code", "claude-code@pb1", None, now);
        afters.push((state.backoff_step, state.return_after.clone()));
        assert_eq!(state.returning, None);
    }
    assert_eq!(
        afters,
        vec![
            (1, "2026-10-22T11:00:00Z".to_owned()),
            (2, "2026-10-22T12:00:00Z".to_owned()),
            (3, "2026-10-22T14:00:00Z".to_owned()),
        ]
    );
    let reported = on_switch(
        Some(state),
        "claude-code",
        "claude-code@pb2",
        Some(utc("2026-10-22T10:30:00Z")),
        now,
    );
    assert_eq!(
        reported.return_after, "2026-10-22T10:30:00Z",
        "a reported reset wins"
    );
    assert_eq!(reported.away_on, "claude-code@pb2");
}

#[test]
fn hops_between_backups_keep_the_timer_and_the_rotation_reaching_home_is_a_return() {
    let now = utc(NOW);
    let away = on_switch(None, "claude-code", "claude-code@pb1", None, now);
    let hop = on_switch(
        Some(away.clone()),
        "claude-code@pb1",
        "claude-code@pb2",
        None,
        now,
    );
    assert_eq!(hop.return_after, away.return_after);
    assert_eq!(hop.backoff_step, 0);
    assert_eq!(hop.away_on, "claude-code@pb2");
    let back = on_switch(Some(hop), "claude-code@pb2", "claude-code", None, now);
    assert_eq!(back.returning, Some(ReturnMode::Rotation));
    assert_eq!(back.home, "claude-code");
    assert_eq!(back.away_on, "claude-code@pb2");
}

#[test]
fn a_return_is_due_once_its_time_passed() {
    let now = utc(NOW);
    let state = on_switch(None, "claude-code", "claude-code@pb1", None, now);
    assert!(!return_due(&state, now));
    assert!(return_due(&state, now + Duration::hours(5)));
    let broken = ProviderReturnState {
        return_after: "not a time".into(),
        ..state
    };
    assert!(
        return_due(&broken, now),
        "an unreadable time never strands it"
    );
}

#[test]
fn the_state_round_trips_through_session_metadata() {
    let state = on_switch(None, "claude-code", "claude-code@pb1", None, utc(NOW));
    assert_eq!(read_state_from(None), None);
    let stored = json!({ PROVIDER_RETURN_METADATA_KEY: state.clone(), "other": 1 });
    assert_eq!(read_state_from(Some(&stored)), Some(state));
    let cleared = json!({ PROVIDER_RETURN_METADATA_KEY: null });
    assert_eq!(read_state_from(Some(&cleared)), None, "null is on home");
}
