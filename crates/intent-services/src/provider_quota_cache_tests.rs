//! Daemon-wide quota health (PROTOCOL §5.54 Backup failover, "Shared quota
//! state"): marks, expiry, and the started-before guard on clearing.

use chrono::{DateTime, Duration, Utc};

use super::*;

fn utc(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
}

#[test]
fn an_unknown_configuration_is_neither_exhausted_nor_working() {
    let cache = ProviderQuotaCache::default();
    let now = utc("2026-10-23T10:00:00Z");
    assert!(!cache.is_exhausted("claude-code", now));
    assert!(!cache.is_known_working("claude-code"));
    assert!(cache.exhausted(now).is_empty());
}

#[test]
fn a_quota_failure_marks_the_configuration_for_five_hours() {
    let cache = ProviderQuotaCache::default();
    let now = utc("2026-10-23T10:00:00Z");
    cache.mark_exhausted("claude-code", None, now, || true);
    assert_eq!(
        cache.exhausted_until("claude-code", now),
        Some(now + Duration::hours(5))
    );
    assert!(cache.is_exhausted("claude-code", now + Duration::hours(4)));
    assert_eq!(cache.exhausted(now), vec!["claude-code".to_owned()]);
    assert!(!cache.is_exhausted("claude-code", now + Duration::hours(5)));
    assert!(cache.exhausted(now + Duration::hours(5)).is_empty());
    assert!(!cache.is_known_working("claude-code"));
}

#[test]
fn a_reported_reset_time_bounds_the_mark() {
    let cache = ProviderQuotaCache::default();
    let now = utc("2026-10-23T10:00:00Z");
    cache.mark_exhausted("codex", Some(now + Duration::minutes(45)), now, || true);
    assert!(cache.is_exhausted("codex", now + Duration::minutes(44)));
    assert!(!cache.is_exhausted("codex", now + Duration::minutes(45)));
}

#[test]
fn an_unusable_reset_time_falls_back_to_five_hours() {
    let cache = ProviderQuotaCache::default();
    let now = utc("2026-10-23T10:00:00Z");
    for reported in [now + Duration::days(30), now - Duration::minutes(1)] {
        cache.mark_exhausted("codex", Some(reported), now, || true);
        assert_eq!(
            cache.exhausted_until("codex", now),
            Some(now + Duration::hours(5)),
            "{reported}"
        );
    }
}

#[test]
fn profiles_and_the_default_configuration_are_tracked_separately() {
    let cache = ProviderQuotaCache::default();
    let now = utc("2026-10-23T10:00:00Z");
    cache.mark_exhausted("claude-code@work", None, now, || true);
    assert!(cache.is_exhausted("claude-code@work", now));
    assert!(!cache.is_exhausted("claude-code", now));
    assert!(!cache.is_exhausted("claude-code@personal", now));
}

#[test]
fn output_after_the_mark_clears_it_for_everyone() {
    let cache = ProviderQuotaCache::default();
    let marked = utc("2026-10-23T10:00:00Z");
    cache.mark_exhausted("claude-code", None, marked, || true);
    let started = marked + Duration::minutes(1);
    cache.mark_working(
        "claude-code",
        started,
        started + Duration::seconds(3),
        || true,
    );
    assert!(!cache.is_exhausted("claude-code", started));
    assert!(cache.is_known_working("claude-code"));
}

#[test]
fn an_attempt_started_before_the_mark_does_not_clear_it() {
    let cache = ProviderQuotaCache::default();
    let started = utc("2026-10-23T10:00:00Z");
    let marked = started + Duration::seconds(30);
    cache.mark_exhausted("claude-code", None, marked, || true);
    cache.mark_working(
        "claude-code",
        started,
        marked + Duration::seconds(5),
        || true,
    );
    assert!(cache.is_exhausted("claude-code", marked + Duration::seconds(5)));
    assert!(!cache.is_known_working("claude-code"));
}

#[test]
fn an_expired_mark_is_replaced_by_any_working_attempt() {
    let cache = ProviderQuotaCache::default();
    let marked = utc("2026-10-23T10:00:00Z");
    cache.mark_exhausted("claude-code", None, marked, || true);
    let later = marked + Duration::hours(6);
    cache.mark_working("claude-code", marked - Duration::minutes(1), later, || true);
    assert!(cache.is_known_working("claude-code"));
}

#[test]
fn a_newer_failure_replaces_a_working_mark() {
    let cache = ProviderQuotaCache::default();
    let now = utc("2026-10-23T10:00:00Z");
    cache.mark_working("claude-code", now, now, || true);
    assert!(cache.is_known_working("claude-code"));
    cache.mark_exhausted("claude-code", None, now + Duration::minutes(1), || true);
    assert!(!cache.is_known_working("claude-code"));
    assert!(cache.is_exhausted("claude-code", now + Duration::minutes(2)));
}

#[test]
fn clones_share_the_same_state() {
    let cache = ProviderQuotaCache::default();
    let clone = cache.clone();
    let now = utc("2026-10-23T10:00:00Z");
    cache.mark_exhausted("codex", None, now, || true);
    assert!(clone.is_exhausted("codex", now));
}

#[test]
fn forgetting_a_configuration_clears_any_state_for_everyone() {
    let cache = ProviderQuotaCache::default();
    let clone = cache.clone();
    let now = utc("2026-10-23T10:00:00Z");
    cache.mark_exhausted("codex", None, now, || true);
    cache.mark_exhausted("codex@work", None, now, || true);
    cache.mark_working("claude-code", now, now, || true);

    cache.forget("codex");
    cache.forget("claude-code");
    assert!(!clone.is_exhausted("codex", now));
    assert!(!clone.is_known_working("claude-code"));
    assert!(clone.is_exhausted("codex@work", now));
    assert_eq!(clone.exhausted(now), vec!["codex@work".to_owned()]);
    cache.forget("never-marked");
}

#[test]
fn an_observation_from_outdated_settings_changes_nothing() {
    let cache = ProviderQuotaCache::default();
    let now = utc("2026-10-23T10:00:00Z");
    cache.mark_exhausted("codex", None, now, || false);
    assert!(cache.exhausted(now).is_empty());

    cache.mark_exhausted("codex", None, now, || true);
    let later = now + Duration::minutes(1);
    cache.mark_working("codex", later, later, || false);
    assert!(cache.is_exhausted("codex", later));
    assert!(!cache.is_known_working("codex"));
}
