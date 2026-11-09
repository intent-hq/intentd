//! Daemon-wide quota health (PROTOCOL §5.54 Backup failover, "Shared quota
//! state"): marks, expiry, and the started-before guard on clearing.

use chrono::{DateTime, Duration, Utc};

use super::*;

fn cache() -> ProviderQuotaCache {
    ProviderQuotaCache::new(Arc::default())
}

/// A read by a process this daemon did not see start.
fn read(selection: &str) -> SettingsRead {
    SettingsRead::elsewhere(selection)
}

fn utc(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
}

#[test]
fn an_unknown_configuration_is_neither_exhausted_nor_working() {
    let cache = cache();
    let now = utc("2026-10-23T10:00:00Z");
    assert!(!cache.is_exhausted("claude-code", now));
    assert!(!cache.is_known_working("claude-code"));
    assert!(cache.exhausted(now).is_empty());
}

#[test]
fn a_quota_failure_marks_the_configuration_for_five_hours() {
    let cache = cache();
    let now = utc("2026-10-23T10:00:00Z");
    cache.mark_exhausted(&read("claude-code"), None, now);
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
    let cache = cache();
    let now = utc("2026-10-23T10:00:00Z");
    cache.mark_exhausted(&read("codex"), Some(now + Duration::minutes(45)), now);
    assert!(cache.is_exhausted("codex", now + Duration::minutes(44)));
    assert!(!cache.is_exhausted("codex", now + Duration::minutes(45)));
}

#[test]
fn an_unusable_reset_time_falls_back_to_five_hours() {
    let cache = cache();
    let now = utc("2026-10-23T10:00:00Z");
    for reported in [now + Duration::days(30), now - Duration::minutes(1)] {
        cache.mark_exhausted(&read("codex"), Some(reported), now);
        assert_eq!(
            cache.exhausted_until("codex", now),
            Some(now + Duration::hours(5)),
            "{reported}"
        );
    }
}

#[test]
fn profiles_and_the_default_configuration_are_tracked_separately() {
    let cache = cache();
    let now = utc("2026-10-23T10:00:00Z");
    cache.mark_exhausted(&read("claude-code@work"), None, now);
    assert!(cache.is_exhausted("claude-code@work", now));
    assert!(!cache.is_exhausted("claude-code", now));
    assert!(!cache.is_exhausted("claude-code@personal", now));
}

#[test]
fn output_after_the_mark_clears_it_for_everyone() {
    let cache = cache();
    let marked = utc("2026-10-23T10:00:00Z");
    cache.mark_exhausted(&read("claude-code"), None, marked);
    let started = marked + Duration::minutes(1);
    cache.mark_working(
        &read("claude-code"),
        started,
        started + Duration::seconds(3),
    );
    assert!(!cache.is_exhausted("claude-code", started));
    assert!(cache.is_known_working("claude-code"));
}

#[test]
fn an_attempt_started_before_the_mark_does_not_clear_it() {
    let cache = cache();
    let started = utc("2026-10-23T10:00:00Z");
    let marked = started + Duration::seconds(30);
    cache.mark_exhausted(&read("claude-code"), None, marked);
    cache.mark_working(&read("claude-code"), started, marked + Duration::seconds(5));
    assert!(cache.is_exhausted("claude-code", marked + Duration::seconds(5)));
    assert!(!cache.is_known_working("claude-code"));
}

#[test]
fn an_expired_mark_is_replaced_by_any_working_attempt() {
    let cache = cache();
    let marked = utc("2026-10-23T10:00:00Z");
    cache.mark_exhausted(&read("claude-code"), None, marked);
    let later = marked + Duration::hours(6);
    cache.mark_working(&read("claude-code"), marked - Duration::minutes(1), later);
    assert!(cache.is_known_working("claude-code"));
}

#[test]
fn a_newer_failure_replaces_a_working_mark() {
    let cache = cache();
    let now = utc("2026-10-23T10:00:00Z");
    cache.mark_working(&read("claude-code"), now, now);
    assert!(cache.is_known_working("claude-code"));
    cache.mark_exhausted(&read("claude-code"), None, now + Duration::minutes(1));
    assert!(!cache.is_known_working("claude-code"));
    assert!(cache.is_exhausted("claude-code", now + Duration::minutes(2)));
}

#[test]
fn clones_share_the_same_state() {
    let cache = cache();
    let clone = cache.clone();
    let now = utc("2026-10-23T10:00:00Z");
    cache.mark_exhausted(&read("codex"), None, now);
    assert!(clone.is_exhausted("codex", now));
}

#[test]
fn forgetting_a_configuration_clears_any_state_for_everyone() {
    let cache = cache();
    let clone = cache.clone();
    let now = utc("2026-10-23T10:00:00Z");
    cache.mark_exhausted(&read("codex"), None, now);
    cache.mark_exhausted(&read("codex@work"), None, now);
    cache.mark_working(&read("claude-code"), now, now);

    cache.forget("codex");
    cache.forget("claude-code");
    assert!(!clone.is_exhausted("codex", now));
    assert!(!clone.is_known_working("claude-code"));
    assert!(clone.is_exhausted("codex@work", now));
    assert_eq!(clone.exhausted(now), vec!["codex@work".to_owned()]);
    cache.forget("never-marked");
}

#[test]
fn clearing_working_drops_only_a_working_observation() {
    let cache = cache();
    let now = utc("2026-10-23T10:00:00Z");
    cache.mark_working(&read("claude-code"), now, now);
    cache.mark_exhausted(&read("codex"), None, now);

    cache.clear_working("claude-code");
    cache.clear_working("codex");
    assert!(!cache.is_known_working("claude-code"));
    assert!(
        cache.is_exhausted("codex", now),
        "an exhausted mark is kept"
    );
}

#[test]
fn an_observation_from_outdated_settings_changes_nothing() {
    let activation = Arc::new(ProfileActivation::default());
    let cache = ProviderQuotaCache::new(activation.clone());
    let now = utc("2026-10-23T10:00:00Z");
    let earlier = SettingsRead::fingerprinted("codex@pwork", "f1");
    activation.saved_changed("pwork", Some("f2"));
    cache.mark_exhausted(&earlier, None, now);
    assert!(cache.exhausted(now).is_empty());

    cache.mark_exhausted(&SettingsRead::fingerprinted("codex@pwork", "f2"), None, now);
    let later = now + Duration::minutes(1);
    cache.mark_working(&earlier, later, later);
    assert!(cache.is_exhausted("codex@pwork", later));
    assert!(!cache.is_known_working("codex@pwork"));
}

/// A Default configuration with no overrides reads as `""`: once overrides
/// are saved, an observation on the bare installation is dropped
/// (intentd#3180 review).
#[test]
fn an_observation_on_a_default_without_overrides_is_dropped_once_overrides_are_saved() {
    let activation = Arc::new(ProfileActivation::default());
    let cache = ProviderQuotaCache::new(activation.clone());
    let now = utc("2026-10-23T10:00:00Z");
    let bare = SettingsRead::resolved("codex", None);
    activation.saved_changed(
        &provider_profiles::default_activation_key("codex"),
        Some("f1"),
    );
    cache.mark_exhausted(&bare, None, now);
    assert!(!cache.is_exhausted("codex", now));
}

/// Settings read before a profile was deleted never mark it, whatever the
/// read (intentd#3180 review).
#[test]
fn an_observation_on_a_deleted_profile_is_dropped() {
    let activation = Arc::new(ProfileActivation::default());
    let cache = ProviderQuotaCache::new(activation.clone());
    let now = utc("2026-10-23T10:00:00Z");
    let read = SettingsRead::fingerprinted("codex@pwork", "f1");
    activation.saved_changed("pwork", None);
    cache.mark_exhausted(&read, None, now);
    cache.mark_exhausted(&SettingsRead::elsewhere("codex@pwork"), None, now);
    cache.mark_working(&read, now, now);
    assert!(!cache.is_exhausted("codex@pwork", now));
    assert!(!cache.is_known_working("codex@pwork"));
}
