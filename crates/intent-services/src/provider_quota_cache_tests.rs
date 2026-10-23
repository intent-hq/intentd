//! Daemon-wide quota health (PROTOCOL §5.54 Backup failover, "Shared quota
//! state"): marks, expiry, the started-before guard on clearing, and the
//! manual-choice exemption.

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
    cache.mark_exhausted("claude-code", None, now);
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
    cache.mark_exhausted("codex", Some(now + Duration::minutes(45)), now);
    assert!(cache.is_exhausted("codex", now + Duration::minutes(44)));
    assert!(!cache.is_exhausted("codex", now + Duration::minutes(45)));
}

#[test]
fn an_unusable_reset_time_falls_back_to_five_hours() {
    let cache = ProviderQuotaCache::default();
    let now = utc("2026-10-23T10:00:00Z");
    for reported in [now + Duration::days(30), now - Duration::minutes(1)] {
        cache.mark_exhausted("codex", Some(reported), now);
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
    cache.mark_exhausted("claude-code@work", None, now);
    assert!(cache.is_exhausted("claude-code@work", now));
    assert!(!cache.is_exhausted("claude-code", now));
    assert!(!cache.is_exhausted("claude-code@personal", now));
}

#[test]
fn output_after_the_mark_clears_it_for_everyone() {
    let cache = ProviderQuotaCache::default();
    let marked = utc("2026-10-23T10:00:00Z");
    cache.mark_exhausted("claude-code", None, marked);
    let started = marked + Duration::minutes(1);
    cache.mark_working("claude-code", started, started + Duration::seconds(3));
    assert!(!cache.is_exhausted("claude-code", started));
    assert!(cache.is_known_working("claude-code"));
}

#[test]
fn an_attempt_started_before_the_mark_does_not_clear_it() {
    let cache = ProviderQuotaCache::default();
    let started = utc("2026-10-23T10:00:00Z");
    let marked = started + Duration::seconds(30);
    cache.mark_exhausted("claude-code", None, marked);
    cache.mark_working("claude-code", started, marked + Duration::seconds(5));
    assert!(cache.is_exhausted("claude-code", marked + Duration::seconds(5)));
    assert!(!cache.is_known_working("claude-code"));
}

#[test]
fn an_expired_mark_is_replaced_by_any_working_attempt() {
    let cache = ProviderQuotaCache::default();
    let marked = utc("2026-10-23T10:00:00Z");
    cache.mark_exhausted("claude-code", None, marked);
    let later = marked + Duration::hours(6);
    cache.mark_working("claude-code", marked - Duration::minutes(1), later);
    assert!(cache.is_known_working("claude-code"));
}

#[test]
fn a_newer_failure_replaces_a_working_mark() {
    let cache = ProviderQuotaCache::default();
    let now = utc("2026-10-23T10:00:00Z");
    cache.mark_working("claude-code", now, now);
    assert!(cache.is_known_working("claude-code"));
    cache.mark_exhausted("claude-code", None, now + Duration::minutes(1));
    assert!(!cache.is_known_working("claude-code"));
    assert!(cache.is_exhausted("claude-code", now + Duration::minutes(2)));
}

#[test]
fn clones_share_the_same_state() {
    let cache = ProviderQuotaCache::default();
    let clone = cache.clone();
    let now = utc("2026-10-23T10:00:00Z");
    cache.mark_exhausted("codex", None, now);
    assert!(clone.is_exhausted("codex", now));
}

const MODEL: Option<&str> = Some("opus");

/// A turn start: stamp, then check what the session read returned.
fn take(cache: &ProviderQuotaCache, agent: &AgentId, current: &str) -> bool {
    let read = cache.manual_choice_read();
    cache.take_manual_choice(agent, current, MODEL, read)
}

#[test]
fn a_manual_choice_is_consumed_once_by_a_turn_on_that_choice() {
    let cache = ProviderQuotaCache::default();
    let agent = AgentId::from("agent-manual");
    assert!(!take(&cache, &agent, "claude-code@work"));
    cache
        .begin_manual_choice(&agent, "claude-code@work", MODEL)
        .commit();
    assert!(cache.clone().is_pending_or_committed(&agent));
    assert!(take(&cache, &agent, "claude-code@work"));
    assert!(!take(&cache, &agent, "claude-code@work"));
    assert!(!cache.is_pending_or_committed(&agent));
}

#[test]
fn a_choice_landing_after_the_read_is_kept_for_the_next_turn() {
    let cache = ProviderQuotaCache::default();
    let agent = AgentId::from("agent-manual");
    // In flight while a turn reads the old configuration.
    let choice = cache.begin_manual_choice(&agent, "claude-code@work", MODEL);
    assert!(!take(&cache, &agent, "claude-code"));
    choice.commit();
    // Persisted after a turn's read began, which then returned the old one.
    let read = cache.manual_choice_read();
    cache
        .begin_manual_choice(&agent, "claude-code@personal", MODEL)
        .commit();
    assert!(!cache.take_manual_choice(&agent, "claude-code", MODEL, read));
    assert!(take(&cache, &agent, "claude-code@personal"));
    // `@work` landed before that read, so a later write replaced it.
    assert!(!take(&cache, &agent, "claude-code@work"));
}

#[test]
fn a_turn_on_an_in_flight_choice_uses_it_up() {
    let cache = ProviderQuotaCache::default();
    let agent = AgentId::from("agent-manual");
    let choice = cache.begin_manual_choice(&agent, "claude-code@work", MODEL);
    assert!(take(&cache, &agent, "claude-code@work"));
    choice.commit();
    assert!(!take(&cache, &agent, "claude-code@work"));
}

#[test]
fn a_failed_persist_discards_only_its_own_record() {
    let cache = ProviderQuotaCache::default();
    let agent = AgentId::from("agent-manual");
    // A failed first choice leaves nothing behind.
    drop(cache.begin_manual_choice(&agent, "claude-code@work", MODEL));
    assert!(!take(&cache, &agent, "claude-code@work"));
    assert!(!cache.is_pending_or_committed(&agent));
    // A successful choice of B survives a later failed change on B.
    cache
        .begin_manual_choice(&agent, "claude-code@work", MODEL)
        .commit();
    drop(cache.begin_manual_choice(&agent, "claude-code@work", MODEL));
    assert!(take(&cache, &agent, "claude-code@work"));
    // A failed change to C ends before a later successful one to C.
    let failed = cache.begin_manual_choice(&agent, "claude-code@work", MODEL);
    cache
        .begin_manual_choice(&agent, "claude-code@personal", MODEL)
        .commit();
    drop(failed);
    assert!(take(&cache, &agent, "claude-code@personal"));
}

/// Review of intentd#2624: B committed and untried, then two overlapping
/// changes to C that both fail, in either order. The session still carries
/// B, and B keeps its exemption.
#[test]
fn overlapping_failed_changes_keep_the_earlier_successful_choice() {
    for second_fails_first in [false, true] {
        let cache = ProviderQuotaCache::default();
        let agent = AgentId::from("agent-manual");
        cache
            .begin_manual_choice(&agent, "claude-code@work", MODEL)
            .commit();
        let t2 = cache.begin_manual_choice(&agent, "claude-code@personal", MODEL);
        let t3 = cache.begin_manual_choice(&agent, "claude-code@personal", MODEL);
        if second_fails_first {
            drop(t3);
            drop(t2);
        } else {
            drop(t2);
            drop(t3);
        }
        // The turn start reads the session, which still carries B.
        assert!(take(&cache, &agent, "claude-code@work"));
        assert!(!cache.is_pending_or_committed(&agent));
    }
}

/// Review of intentd#2624: a turn that read the old model on the same
/// configuration does not use up a model change still saving.
#[test]
fn a_turn_on_the_old_model_keeps_a_same_configuration_model_change() {
    let cache = ProviderQuotaCache::default();
    let agent = AgentId::from("agent-manual");
    let choice = cache.begin_manual_choice(&agent, "claude-code@work", Some("sonnet"));
    let read = cache.manual_choice_read();
    assert!(!cache.take_manual_choice(&agent, "claude-code@work", Some("opus"), read));
    choice.commit();
    let read = cache.manual_choice_read();
    assert!(cache.take_manual_choice(&agent, "claude-code@work", Some("sonnet"), read));
    assert!(!cache.is_pending_or_committed(&agent));
}
