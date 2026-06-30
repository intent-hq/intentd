//! Unit-style integration coverage for `intent-core::events`.

use intent_core::events::{is_known_event_type, ALL_EVENT_TYPES};
use intent_core::is_known_event_type as re_exported_is_known_event_type;

#[test]
fn every_canonical_type_is_recognized() {
    for ty in ALL_EVENT_TYPES {
        assert!(
            is_known_event_type(ty),
            "ALL_EVENT_TYPES contains `{ty}` but is_known_event_type rejected it"
        );
    }
}

#[test]
fn unknown_event_types_are_rejected() {
    for bogus in [
        "",
        "agent",
        "agent:",
        "agent:bogus",
        "FILE_CHANGED",
        "file:rename",
        "totally-made-up",
    ] {
        assert!(
            !is_known_event_type(bogus),
            "`{bogus}` should not be recognized"
        );
    }
}

#[test]
fn taxonomy_has_no_duplicate_strings() {
    let mut sorted: Vec<&&str> = ALL_EVENT_TYPES.iter().collect();
    sorted.sort();
    let len_before = sorted.len();
    sorted.dedup();
    assert_eq!(
        sorted.len(),
        len_before,
        "ALL_EVENT_TYPES must contain no duplicates"
    );
    assert!(
        len_before > 50,
        "expected a rich taxonomy, got {len_before}"
    );
}

#[test]
fn re_export_matches_module_function() {
    // `intent_core::is_known_event_type` is re-exported from `events`; both
    // entry points must agree.
    for ty in ALL_EVENT_TYPES {
        assert_eq!(is_known_event_type(ty), re_exported_is_known_event_type(ty));
    }
    assert_eq!(
        is_known_event_type("nope"),
        re_exported_is_known_event_type("nope")
    );
}
