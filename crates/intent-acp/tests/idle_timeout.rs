//! Unit tests for `ActivityTracker` (STAB-33).
//!
//! These tests verify the timestamp-tracking mechanism that supports the
//! activity-based idle timeout. Full integration test coverage (asserting that
//! `session::prompt` actually times out after the idle window) is deferred.

use std::time::Duration;
use tokio::time::sleep;

use intent_acp::session::ActivityTracker;

/// Unit test: `ActivityTracker` correctly tracks idle time.
#[tokio::test]
async fn activity_tracker_idle_measurement() {
    let tracker = ActivityTracker::new();

    // Should start with low idle
    assert!(tracker.idle_ms() < 100);

    // Wait 100ms
    sleep(Duration::from_millis(100)).await;
    let idle1 = tracker.idle_ms();
    assert!(idle1 >= 100, "idle after 100ms: {idle1}");

    // Touch and reset
    tracker.touch();
    assert!(tracker.idle_ms() < 100);

    // Wait another 100ms
    sleep(Duration::from_millis(100)).await;
    let idle2 = tracker.idle_ms();
    assert!(idle2 >= 100, "idle after touch+100ms: {idle2}");
}

/// Idle time resets on activity, not measured from start.
#[tokio::test]
async fn idle_resets_on_touch() {
    let tracker = ActivityTracker::new();

    // Wait 100ms
    sleep(Duration::from_millis(100)).await;
    assert!(tracker.idle_ms() >= 100);

    // Touch (reset)
    tracker.touch();

    // Idle should be low again
    assert!(tracker.idle_ms() < 100);

    // Wait another 100ms - idle is from last touch, not from start
    sleep(Duration::from_millis(100)).await;
    let idle = tracker.idle_ms();
    assert!(idle >= 100, "idle after touch: {idle}");
}

/// Simulates a turn with periodic activity that would exceed the old 1h limit.
#[tokio::test]
async fn periodic_activity_keeps_idle_low() {
    let tracker = ActivityTracker::new();

    // Simulate periodic updates over a window that would exceed a fixed timeout
    // Keep touching every 50ms for 500ms total
    for _ in 0..10 {
        sleep(Duration::from_millis(50)).await;
        tracker.touch();
        // After each touch, idle should be low
        assert!(tracker.idle_ms() < 100);
    }

    // Final idle should still be low (last touch was recent)
    assert!(tracker.idle_ms() < 100);
}
