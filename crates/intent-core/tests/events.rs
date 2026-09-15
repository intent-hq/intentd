//! Unit-style integration coverage for `intent-core::events`, plus the
//! checked-in event-catalog golden (`tests/goldens/event_types.json`).

use std::collections::BTreeSet;
use std::path::PathBuf;

use intent_core::events::{
    is_collaborator_event_type, is_known_event_type, DiscriminatorKind, EventDiscriminator,
    ALL_EVENT_TYPES, COLLABORATOR_EVENT_TYPES, EVENT_DISCRIMINATORS,
};
use intent_core::is_known_event_type as re_exported_is_known_event_type;
use serde_json::{json, Value};

const GOLDEN_REL: &str = "tests/goldens/event_types.json";
const UPDATE_ENV: &str = "INTENTD_UPDATE_GOLDENS";
const GOLDEN_VERSION: u64 = 1;

fn golden_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(GOLDEN_REL)
}

fn regenerate_hint() -> String {
    format!("regenerate with `{UPDATE_ENV}=1 cargo test -p intent-core --test events`")
}

/// The golden as derived from the Rust catalog: `types` is `ALL_EVENT_TYPES`
/// sorted; `discriminators` maps each event type to
/// `{ path, kind, values, absent? }` from `EVENT_DISCRIMINATORS`.
fn expected_golden() -> Value {
    let types: BTreeSet<&str> = ALL_EVENT_TYPES.iter().copied().collect();
    let discriminators: serde_json::Map<String, Value> = EVENT_DISCRIMINATORS
        .iter()
        .map(|d| {
            let mut entry = json!({
                "path": d.path,
                "kind": d.kind.as_str(),
                "values": d.values,
            });
            if let Some(absent) = d.absent {
                entry["absent"] = json!(absent);
            }
            (d.event_type.to_string(), entry)
        })
        .collect();
    json!({
        "version": GOLDEN_VERSION,
        "types": types,
        "discriminators": discriminators,
    })
}

fn string_set(value: &Value) -> BTreeSet<String> {
    value
        .as_array()
        .map(|items| {
            items
                .iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

fn set_diff(label: &str, expected: &BTreeSet<String>, actual: &BTreeSet<String>) -> Vec<String> {
    let mut out = Vec::new();
    for missing in expected.difference(actual) {
        out.push(format!(
            "{label}: `{missing}` is in the Rust catalog but not in the golden"
        ));
    }
    for extra in actual.difference(expected) {
        out.push(format!(
            "{label}: `{extra}` is in the golden but not in the Rust catalog"
        ));
    }
    out
}

#[test]
fn golden_event_catalog_matches_rust_catalog() {
    let path = golden_path();
    let expected = expected_golden();
    if std::env::var_os(UPDATE_ENV).is_some_and(|v| v == "1") {
        let mut text = serde_json::to_string_pretty(&expected).expect("serialize golden");
        text.push('\n');
        std::fs::write(&path, text).unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
        return;
    }
    let text = std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("read {}: {e}\n{}", path.display(), regenerate_hint()));
    let actual: Value = serde_json::from_str(&text)
        .unwrap_or_else(|e| panic!("{} is not valid JSON: {e}", path.display()));

    let mut problems = set_diff(
        "types",
        &string_set(&expected["types"]),
        &string_set(&actual["types"]),
    );
    let expected_disc = expected["discriminators"]
        .as_object()
        .expect("expected discriminators");
    let empty = serde_json::Map::new();
    let actual_disc = actual["discriminators"].as_object().unwrap_or(&empty);
    let expected_keys: BTreeSet<String> = expected_disc.keys().cloned().collect();
    let actual_keys: BTreeSet<String> = actual_disc.keys().cloned().collect();
    problems.extend(set_diff("discriminators", &expected_keys, &actual_keys));
    for (event_type, want) in expected_disc {
        let Some(have) = actual_disc.get(event_type) else {
            continue;
        };
        problems.extend(set_diff(
            &format!("discriminators[{event_type}].values"),
            &string_set(&want["values"]),
            &string_set(&have["values"]),
        ));
        for field in ["path", "kind", "absent"] {
            if want.get(field) != have.get(field) {
                problems.push(format!(
                    "discriminators[{event_type}].{field}: Rust catalog has {}, golden has {}",
                    want.get(field).unwrap_or(&Value::Null),
                    have.get(field).unwrap_or(&Value::Null)
                ));
            }
        }
    }
    if problems.is_empty() && actual != expected {
        problems.push(format!(
            "golden differs from the Rust catalog outside the named tables (version {} expected, got {})",
            expected["version"], actual["version"]
        ));
    }
    assert!(
        problems.is_empty(),
        "{GOLDEN_REL} is out of date with intent_core::events:\n  {}\n{}",
        problems.join("\n  "),
        regenerate_hint()
    );
}

#[test]
fn golden_file_is_canonically_formatted() {
    if std::env::var_os(UPDATE_ENV).is_some_and(|v| v == "1") {
        return;
    }
    let path = golden_path();
    let text =
        std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()));
    let expected = expected_golden();
    // Content drift is reported (with names) by
    // `golden_event_catalog_matches_rust_catalog`; this test only checks the
    // byte form of a content-identical golden.
    if serde_json::from_str::<Value>(&text).ok().as_ref() != Some(&expected) {
        return;
    }
    let mut canonical = serde_json::to_string_pretty(&expected).expect("serialize golden");
    canonical.push('\n');
    assert!(
        text == canonical,
        "{GOLDEN_REL} is not in canonical pretty-printed form; {}",
        regenerate_hint()
    );
}

#[test]
fn discriminator_table_is_well_formed() {
    let mut seen = BTreeSet::new();
    for EventDiscriminator {
        event_type,
        path,
        kind,
        values,
        absent,
    } in EVENT_DISCRIMINATORS
    {
        assert!(
            is_known_event_type(event_type),
            "EVENT_DISCRIMINATORS names `{event_type}`, which is not in ALL_EVENT_TYPES"
        );
        assert!(
            seen.insert(*event_type),
            "`{event_type}` listed twice in EVENT_DISCRIMINATORS"
        );
        assert!(
            path.starts_with("data."),
            "`{event_type}` path `{path}` must start with `data.`"
        );
        let sorted: BTreeSet<&str> = values.iter().copied().collect();
        assert_eq!(
            sorted.iter().copied().collect::<Vec<_>>(),
            values.to_vec(),
            "`{event_type}` values must be sorted and deduplicated"
        );
        assert!(!values.is_empty(), "`{event_type}` has no values");
        if *kind == DiscriminatorKind::Keys {
            assert!(
                absent.is_none(),
                "`{event_type}` object-key discriminators are always present"
            );
        }
    }
    assert!(seen.contains("task:ready-tasks-changed"));
    assert!(seen.contains("workspace:updated"));
}

/// Multiplayer w3 golden: the event types a non-administrator principal never
/// receives — the refused remainder of [`ALL_EVENT_TYPES`] after
/// [`COLLABORATOR_EVENT_TYPES`]. Sorted; every taxonomy entry must appear in
/// exactly one of the two lists, so adding an event type forces an explicit
/// classification (a new owner-only type is added here, a guest-visible one
/// gets a vetting note in `COLLABORATOR_EVENT_TYPES`).
const COLLABORATOR_REFUSED_EVENT_TYPES: &[&str] = &[
    "agent:delivery-confirmed",
    "agent:event-delivery-failed",
    "agent:event-delivery-timeout",
    "agent:message:delivery-failed",
    "agent:message:received",
    "agent:message:sent",
    "agent:permission:request",
    "agent:permission:resolved",
    "agent:subscribed",
    "agent:subscriptions-restored",
    "agent:unsubscribed",
    "agent:woken-by-subscription",
    "app:ui-highlight",
    "app:ui-navigate",
    "app:workspace-open",
    "browser:tab-closed",
    "browser:tab-opened",
    "browser:tab-updated",
    "build:completed",
    "build:started",
    "client:connected",
    "client:disconnected",
    "git:clone:done",
    "git:clone:progress",
    "gitRoot:registered",
    "gitRoot:unregistered",
    "gitRoot:updated",
    "github:auth-changed",
    "hook:run-completed",
    "hook:run-started",
    "host:exec:exit",
    "host:exec:stderr",
    "host:exec:stdout",
    "mcp.servers:status-changed",
    "mcp:notification",
    "script:changed",
    "script:output",
    "script:state",
    "settings:changed",
    "terminal:command",
    "terminal:cwd",
    "terminal:data",
    "terminal:exit",
    "terminal:title",
    "test:completed",
    "test:started",
    "workspace:transfer:failed",
    "workspace:transfer:progress",
    "workspace:transfer:ready",
];

#[test]
fn collaborator_event_types_are_sorted_unique_vetted_and_canonical() {
    let types: Vec<&str> = COLLABORATOR_EVENT_TYPES.iter().map(|(t, _)| *t).collect();
    let mut sorted = types.clone();
    sorted.sort_unstable();
    assert_eq!(types, sorted, "COLLABORATOR_EVENT_TYPES must be sorted");
    let unique: BTreeSet<&str> = types.iter().copied().collect();
    assert_eq!(
        unique.len(),
        types.len(),
        "COLLABORATOR_EVENT_TYPES must contain no duplicates"
    );
    for (ty, note) in COLLABORATOR_EVENT_TYPES {
        assert!(
            !note.trim().is_empty(),
            "collaborator event type `{ty}` needs a vetting note"
        );
        assert!(
            is_known_event_type(ty),
            "collaborator event type `{ty}` is not in ALL_EVENT_TYPES"
        );
        assert!(is_collaborator_event_type(ty));
    }
}

#[test]
fn every_canonical_type_is_classified_for_collaborators_exactly_once() {
    let allowed: BTreeSet<&str> = COLLABORATOR_EVENT_TYPES.iter().map(|(t, _)| *t).collect();
    let refused: BTreeSet<&str> = COLLABORATOR_REFUSED_EVENT_TYPES.iter().copied().collect();

    let mut sorted_refused = COLLABORATOR_REFUSED_EVENT_TYPES.to_vec();
    sorted_refused.sort_unstable();
    assert_eq!(
        COLLABORATOR_REFUSED_EVENT_TYPES,
        sorted_refused.as_slice(),
        "COLLABORATOR_REFUSED_EVENT_TYPES must be sorted"
    );
    assert_eq!(
        refused.len(),
        COLLABORATOR_REFUSED_EVENT_TYPES.len(),
        "COLLABORATOR_REFUSED_EVENT_TYPES must contain no duplicates"
    );

    let both: Vec<&&str> = allowed.intersection(&refused).collect();
    assert!(
        both.is_empty(),
        "classified as both allowed and refused: {both:?}"
    );

    let unclassified: Vec<&&str> = ALL_EVENT_TYPES
        .iter()
        .filter(|t| !allowed.contains(*t) && !refused.contains(*t))
        .collect();
    assert!(
        unclassified.is_empty(),
        "event types in ALL_EVENT_TYPES classified neither allowed nor refused for collaborators: {unclassified:?}"
    );

    let all: BTreeSet<&str> = ALL_EVENT_TYPES.iter().copied().collect();
    let stale: Vec<&&str> = refused.iter().filter(|t| !all.contains(*t)).collect();
    assert!(
        stale.is_empty(),
        "refused golden names types missing from ALL_EVENT_TYPES: {stale:?}"
    );

    for ty in &refused {
        assert!(
            !is_collaborator_event_type(ty),
            "`{ty}` is refused for collaborators but the predicate allowed it"
        );
    }
}

#[test]
fn collaborator_predicate_is_default_deny() {
    // Owner-only families and unknown / non-canonical spellings never pass:
    // the predicate is membership in the vetted list, not a prefix rule.
    for bogus in [
        "",
        "*",
        "note:*",
        "terminal:data",
        "host:exec:stdout",
        "client:connected",
        "sandbox:cow:created",
        "NOTE:UPDATED",
    ] {
        assert!(
            !is_collaborator_event_type(bogus),
            "`{bogus}` must not pass the collaborator allowlist"
        );
    }
    assert!(is_collaborator_event_type("note:updated"));
}

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
