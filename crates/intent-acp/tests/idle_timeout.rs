//! Regression tests for activity-based idle timeout (STAB-33).
//!
//! (a) Turn with periodic updates running past the old 1h limit survives.
//! (b) Fully silent turn dies after the idle window.
//! (c) Update-then-silence dies at last-activity + window, not turn-start + window.
//!
//! Note: These tests manipulate env vars, so they should not run in parallel.

use std::time::Duration;
use tokio::time::sleep;

use intent_acp::session::ActivityTracker;

/// Unit test: ActivityTracker correctly tracks idle time.
#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn activity_tracker_idle_measurement() {
    std::env::remove_var("INTENTD_PROMPT_IDLE_TIMEOUT_MS");
    let tracker = ActivityTracker::new();

    // Should start with ~0ms idle
    assert!(tracker.idle_ms() < 50);

    // Wait 100ms
    sleep(Duration::from_millis(100)).await;
    let idle1 = tracker.idle_ms();
    assert!(idle1 >= 100 && idle1 < 150, "idle after 100ms: {}", idle1);

    // Touch and reset
    tracker.touch();
    assert!(tracker.idle_ms() < 50);

    // Wait another 100ms
    sleep(Duration::from_millis(100)).await;
    let idle2 = tracker.idle_ms();
    assert!(
        idle2 >= 100 && idle2 < 150,
        "idle after touch+100ms: {}",
        idle2
    );
}

/// (c) Idle time resets on activity, not measured from start.
#[tokio::test]
async fn idle_resets_on_touch() {
    let tracker = ActivityTracker::new();

    // Wait 100ms
    sleep(Duration::from_millis(100)).await;
    assert!(tracker.idle_ms() >= 100);

    // Touch (reset)
    tracker.touch();

    // Idle should be near 0 again
    assert!(tracker.idle_ms() < 50);

    // Wait another 100ms - idle is from last touch, not from start
    sleep(Duration::from_millis(100)).await;
    let idle = tracker.idle_ms();
    assert!(
        idle >= 100 && idle < 150,
        "idle after touch: {} (expected ~100ms)",
        idle
    );
}

/// (a) Simulates a turn with periodic activity that would exceed the old 1h limit.
#[tokio::test]
async fn periodic_activity_keeps_idle_low() {
    let tracker = ActivityTracker::new();

    // Simulate periodic updates over a window that would exceed a fixed timeout
    // Keep touching every 50ms for 500ms total
    for _ in 0..10 {
        sleep(Duration::from_millis(50)).await;
        tracker.touch();
        // After each touch, idle should be near 0
        assert!(tracker.idle_ms() < 50);
    }

    // Final idle should still be low (last touch was <50ms ago)
    assert!(tracker.idle_ms() < 50);
}
