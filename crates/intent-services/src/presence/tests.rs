use std::time::{Duration, Instant};

use serde_json::{json, Value};

use super::{parse_cursor, parse_update, CursorThrottle, Offer, CURSOR_MIN_INTERVAL};

fn cursor(head: u64) -> Value {
    json!({ "rev": 1, "anchor": head, "head": head })
}

/// Drive a burst of `offers` carets `step` apart through the throttle,
/// honouring every `Defer` as the async driver would (a flush at the
/// deferred instant, folded into the timeline). Returns the published
/// carets with their publish instants.
fn drive(offers: usize, step: Duration) -> Vec<(Instant, Value)> {
    let start = Instant::now();
    let mut throttle = CursorThrottle::default();
    let mut published = Vec::new();
    let mut flush_at: Option<Instant> = None;
    for i in 0..offers {
        let now = start + step * u32::try_from(i).expect("small");
        if let Some(at) = flush_at.filter(|at| *at <= now) {
            if let Some(c) = throttle.flush(at) {
                published.push((at, c));
            }
            flush_at = None;
        }
        match throttle.offer(cursor(i as u64), now) {
            Offer::Publish => published.push((now, cursor(i as u64))),
            Offer::Defer(delay) => flush_at = Some(now + delay),
            Offer::Absorbed => assert!(flush_at.is_some(), "absorbed without a pending flush"),
        }
    }
    if let Some(at) = flush_at {
        if let Some(c) = throttle.flush(at) {
            published.push((at, c));
        }
    }
    published
}

/// Definition of done: a 1 s burst at 100 carets/s yields ≤10 deliveries per
/// second, consecutive deliveries are never closer than the floor, and the
/// last delivery is the last offered position.
#[test]
fn coalescer_caps_at_ten_per_second_and_ends_on_last_position() {
    let published = drive(100, Duration::from_millis(10));
    let first = published.first().expect("something published").0;
    let last = published.last().expect("something published");
    let span = last.0.duration_since(first);
    let seconds = (span.as_secs_f64()).max(1.0);
    let count = u32::try_from(published.len()).expect("small count");
    assert!(
        f64::from(count) <= 10.0 * seconds + 1.0,
        "{} deliveries over {span:?}",
        published.len()
    );
    for pair in published.windows(2) {
        assert!(
            pair[1].0.duration_since(pair[0].0) >= CURSOR_MIN_INTERVAL,
            "deliveries closer than the floor: {:?} then {:?}",
            pair[0].0,
            pair[1].0
        );
    }
    assert_eq!(
        last.1,
        cursor(99),
        "the trailing flush carries the last caret"
    );
    assert!(
        published.len() >= 10,
        "burst was over-throttled: {} deliveries",
        published.len()
    );
}

/// A lone caret publishes immediately (leading edge) and a second one inside
/// the floor is deferred by exactly the remainder.
#[test]
fn coalescer_leading_edge_then_defers_remainder() {
    let start = Instant::now();
    let mut throttle = CursorThrottle::default();
    assert_eq!(throttle.offer(cursor(1), start), Offer::Publish);
    let later = start + Duration::from_millis(30);
    assert_eq!(
        throttle.offer(cursor(2), later),
        Offer::Defer(CURSOR_MIN_INTERVAL.saturating_sub(Duration::from_millis(30)))
    );
    assert_eq!(throttle.offer(cursor(3), later), Offer::Absorbed);
    let at = start + CURSOR_MIN_INTERVAL;
    assert_eq!(throttle.flush(at), Some(cursor(3)), "last writer wins");
    assert_eq!(throttle.flush(at), None, "flush is one-shot");
    assert_eq!(
        throttle.offer(cursor(4), at + CURSOR_MIN_INTERVAL),
        Offer::Publish,
        "the floor restarts from the flush"
    );
}

#[test]
fn parse_update_accepts_focus_set_and_optional_typing() {
    let (focus, typing) = parse_update(&json!({
        "focus": [
            { "workspaceId": "ws-1", "agentId": "agent-1" },
            { "workspaceId": "ws-1", "noteId": "spec" },
            { "workspaceId": "ws-1", "noteId": "spec" },
            { "workspaceId": "ws-2" }
        ],
        "typing": { "agentId": "agent-1" }
    }))
    .expect("valid");
    assert_eq!(focus.len(), 3, "duplicate focus items collapse");
    assert_eq!(typing.as_deref(), Some("agent-1"));
    let (focus, typing) = parse_update(&json!({ "focus": [], "typing": null })).expect("valid");
    assert!(focus.is_empty());
    assert!(typing.is_none());
}

#[test]
fn parse_update_rejects_malformed_params() {
    for bad in [
        json!({}),
        json!({ "focus": "ws-1" }),
        json!({ "focus": [{ "agentId": "a" }] }),
        json!({ "focus": [{ "workspaceId": "" }] }),
        json!({ "focus": [{ "workspaceId": "ws", "noteId": 3 }] }),
        json!({ "focus": [], "typing": {} }),
        json!({ "focus": [], "typing": "agent-1" }),
    ] {
        assert!(
            matches!(
                parse_update(&bad),
                Err(intent_core::Error::InvalidParams(_))
            ),
            "{bad}"
        );
    }
}

#[test]
fn parse_cursor_requires_three_non_negative_integers() {
    assert_eq!(
        parse_cursor(&json!({ "rev": 3, "anchor": 10, "head": 12, "extra": true })).expect("ok"),
        json!({ "rev": 3, "anchor": 10, "head": 12 })
    );
    for bad in [
        json!({ "rev": 3, "anchor": 10 }),
        json!({ "rev": -1, "anchor": 10, "head": 12 }),
        json!({ "rev": "3", "anchor": 10, "head": 12 }),
    ] {
        assert!(
            matches!(
                parse_cursor(&bad),
                Err(intent_core::Error::InvalidParams(_))
            ),
            "{bad}"
        );
    }
}
