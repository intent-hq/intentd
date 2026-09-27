//! Deterministic eligibility policy tests, independent of storage and callers.

#[expect(dead_code, reason = "this target tests the generic policy subset")]
#[path = "../src/observation_policy.rs"]
mod observation_policy;

use observation_policy::{Coverage, Ineligible, ObservationScope, ObservationSlot};
use serde_json::{json, Value};

// Core DTO fixture v2 (repository_context.rs at 55c5f9cf): one execution
// scope contains multiple per-target connections. Use its serialized
// values until the separately owned core export is integrated; no second
// DTO or parser is defined here. Generation counters are decimal strings.
fn canonical_fixture() -> (Value, Value) {
    (
        json!({
            "execution": {"daemonId":"daemon-A", "authorityScopeId":"caller-workspace-1", "authorityGeneration":"9007199254740995"},
            "connection": {"connectionId":"gitlab-connection", "accountId":"account-A", "connectionGeneration":"18446744073709551615"},
        }),
        json!({
            "repository": {"provider":"gitlab", "instanceBaseUrl":"https://git.example:8443/gitlab", "projectPath":"team/sub/app"},
            "kind":"merge-request", "number":42,
        }),
    )
}

#[test]
fn canonical_scope_and_target_dimensions_have_independent_keys() {
    let (scope, resource) = canonical_fixture();
    let original = ObservationScope::new(scope.clone());
    let original_slot = ObservationSlot::new(&original, resource.clone());
    let mut keys = std::collections::HashSet::from([original_slot.key().clone()]);
    for (part, field, value) in [
        ("execution", "daemonId", json!("daemon-B")),
        ("execution", "authorityScopeId", json!("another-authority")),
        (
            "execution",
            "authorityGeneration",
            json!("9007199254740994"),
        ),
        ("connection", "connectionId", json!("other-connection")),
        ("connection", "accountId", json!("account-B")),
        (
            "connection",
            "connectionGeneration",
            json!("18446744073709551614"),
        ),
    ] {
        let mut changed = scope.clone();
        changed[part][field] = value;
        let changed = ObservationScope::new(changed);
        assert!(keys.insert(
            ObservationSlot::new(&changed, resource.clone())
                .key()
                .clone()
        ));
    }
    for (field, value) in [
        ("provider", "github"),
        ("instanceBaseUrl", "https://other.example:8443/gitlab"),
        ("instanceBaseUrl", "https://git.example:9443/gitlab"),
        (
            "instanceBaseUrl",
            "https://git.example:8443/another-installation",
        ),
        ("projectPath", "team/other/app"),
    ] {
        let mut changed = resource.clone();
        changed["repository"][field] = json!(value);
        assert!(keys.insert(ObservationSlot::new(&original, changed).key().clone()));
    }
    for kind in ["pull-request", "issue"] {
        let mut changed = resource.clone();
        changed["kind"] = json!(kind);
        assert!(keys.insert(ObservationSlot::new(&original, changed).key().clone()));
    }
    let mut changed = resource;
    changed["number"] = json!(43);
    assert!(keys.insert(ObservationSlot::new(&original, changed).key().clone()));
    assert_eq!(keys.len(), 15);
}

#[test]
fn a_resource_denial_does_not_invalidate_another_authorized_resource() {
    let (scope, resource) = canonical_fixture();
    let scope = ObservationScope::new(scope);
    let mut issue = resource.clone();
    issue["kind"] = json!("issue");
    let mut denied = ObservationSlot::new(&scope, resource);
    let mut unaffected = ObservationSlot::new(&scope, issue);
    let read = unaffected.begin(Coverage::Summary).unwrap();
    let warm = unaffected.success(read).unwrap();
    let request = denied.begin(Coverage::Detail).unwrap();
    denied.deny(request).unwrap();
    assert!(unaffected.can_serve(&warm, &scope));
}

#[test]
fn denial_invalidates_warm_detail_and_summary_and_requires_fresh_recovery() {
    let scope = ObservationScope::new("admitted-scope");
    let mut slot = ObservationSlot::new(&scope, "resource");
    let detail_read = slot.begin(Coverage::Detail).unwrap();
    let detail = slot.success(detail_read).unwrap();
    let summary_read = slot.begin(Coverage::Summary).unwrap();
    let summary = slot.success(summary_read).unwrap();
    let old_detail = slot.begin(Coverage::Detail).unwrap();
    let old_summary = slot.begin(Coverage::Summary).unwrap();
    assert!(slot.can_serve(&detail, &scope));
    assert!(slot.can_serve(&summary, &scope));

    let poll = slot.begin(Coverage::Detail).unwrap();
    slot.deny(poll).unwrap();
    assert!(!slot.can_serve(&detail, &scope));
    assert!(!slot.can_serve(&summary, &scope));
    assert_eq!(
        slot.success(old_detail).unwrap_err(),
        Ineligible::DeniedSinceRequest
    );

    let fresh_read = slot.begin(Coverage::Summary).unwrap();
    let fresh_summary = slot.success(fresh_read).unwrap();
    assert!(slot.can_serve(&fresh_summary, &scope));
    assert!(!slot.can_serve(&detail, &scope));
    assert!(!slot.can_serve(&summary, &scope));
    assert_eq!(
        slot.success(old_summary).unwrap_err(),
        Ineligible::DeniedSinceRequest
    );

    let fresh_read = slot.begin(Coverage::Detail).unwrap();
    let fresh_detail = slot.success(fresh_read).unwrap();
    assert!(slot.can_serve(&fresh_detail, &scope));
    assert!(slot.can_serve(&fresh_summary, &scope));
}

#[test]
fn retired_scope_excludes_hits_and_late_success_without_retiring_another_connection() {
    let old = ObservationScope::new("old-account");
    let mut slot = ObservationSlot::new(&old, "resource");
    let read = slot.begin(Coverage::Detail).unwrap();
    let warm = slot.success(read).unwrap();
    let in_flight = slot.begin(Coverage::Detail).unwrap();
    let other = ObservationScope::new("github-connection");
    let mut other_slot = ObservationSlot::new(&other, "resource");
    let read = other_slot.begin(Coverage::Detail).unwrap();
    let other_warm = other_slot.success(read).unwrap();

    old.clone().retire();
    assert!(!slot.can_serve(&warm, &old));
    assert_eq!(
        slot.success(in_flight).unwrap_err(),
        Ineligible::RetiredScope
    );
    assert_eq!(
        slot.begin(Coverage::Detail).unwrap_err(),
        Ineligible::RetiredScope
    );
    assert!(other_slot.can_serve(&other_warm, &other));
}

#[test]
fn replacement_scope_cannot_use_the_previous_scopes_receipts_even_with_same_key() {
    let original = ObservationScope::new("scope");
    let mut slot = ObservationSlot::new(&original, "resource");
    let read = slot.begin(Coverage::Detail).unwrap();
    let warm = slot.success(read).unwrap();
    let replacement = ObservationScope::new("scope");
    assert!(!slot.can_serve(&warm, &replacement));
    let mut new_slot = ObservationSlot::new(&replacement, "resource");
    let late = slot.begin(Coverage::Detail).unwrap();
    assert_eq!(
        new_slot.success(late).unwrap_err(),
        Ineligible::DifferentSlot
    );
}

#[test]
fn old_success_cannot_repopulate_an_evicted_slot_or_another_resource() {
    let scope = ObservationScope::new("scope");
    let mut slot = ObservationSlot::new(&scope, "resource");
    let late = slot.begin(Coverage::Detail).unwrap();
    drop(slot);
    let mut reinserted = ObservationSlot::new(&scope, "resource");
    assert_eq!(
        reinserted.success(late).unwrap_err(),
        Ineligible::DifferentSlot
    );
    let wrong_target = reinserted.begin(Coverage::Detail).unwrap();
    let mut other = ObservationSlot::new(&scope, "other-resource");
    assert_ne!(reinserted.key(), other.key());
    assert_eq!(
        other.deny(wrong_target).unwrap_err(),
        Ineligible::DifferentSlot
    );
}

#[test]
fn newer_observations_win_but_summary_does_not_replace_detail() {
    let scope = ObservationScope::new("scope");
    let mut slot = ObservationSlot::new(&scope, "resource");
    let older = slot.begin(Coverage::Detail).unwrap();
    let newer = slot.begin(Coverage::Detail).unwrap();
    let detail = slot.success(newer).unwrap();
    assert_eq!(
        slot.success(older).unwrap_err(),
        Ineligible::OlderObservation
    );
    let summary_read = slot.begin(Coverage::Summary).unwrap();
    let summary = slot.success(summary_read).unwrap();
    assert!(slot.can_serve(&detail, &scope));
    assert!(slot.can_serve(&summary, &scope));
}

#[test]
fn a_late_primary_denial_still_invalidates_a_newer_success() {
    let scope = ObservationScope::new("scope");
    let mut slot = ObservationSlot::new(&scope, "resource");
    let old_poll = slot.begin(Coverage::Detail).unwrap();
    let newer = slot.begin(Coverage::Detail).unwrap();
    let detail = slot.success(newer).unwrap();
    slot.deny(old_poll).unwrap();
    assert!(!slot.can_serve(&detail, &scope));
}

#[test]
fn authority_or_connection_denial_invalidates_all_its_resources_and_allows_fresh_recovery() {
    let scope = ObservationScope::new("same-connected-account");
    let mut mr = ObservationSlot::new(&scope, "merge-request");
    let mut issue = ObservationSlot::new(&scope, "issue");
    let read = mr.begin(Coverage::Detail).unwrap();
    let warm_mr = mr.success(read).unwrap();
    let read = issue.begin(Coverage::Summary).unwrap();
    let warm_issue = issue.success(read).unwrap();
    let old = mr.begin(Coverage::Detail).unwrap();
    scope.deny();
    assert!(!mr.can_serve(&warm_mr, &scope));
    assert!(!issue.can_serve(&warm_issue, &scope));
    assert_eq!(mr.success(old).unwrap_err(), Ineligible::DeniedSinceRequest);
    let read = mr.begin(Coverage::Detail).unwrap();
    let recovered = mr.success(read).unwrap();
    assert!(mr.can_serve(&recovered, &scope));
    assert!(!issue.can_serve(&warm_issue, &scope));
}

#[test]
fn injected_classification_controls_denial_and_preserves_distinct_errors() {
    // Opaque policy inputs test the injected predicate, not provider enums
    // or HTTP classification. Mapping the canonical provider errors to
    // resource-local or connection-wide denial requires separate integration.
    for (category, denies) in [
        ("primary-denial", true),
        ("optional-policy-restriction", false),
        ("optional-unavailable", false),
        ("transient", false),
        ("rate-limited", false),
        ("unknown", false),
    ] {
        let scope = ObservationScope::new("scope");
        let mut slot = ObservationSlot::new(&scope, "resource");
        let read = slot.begin(Coverage::Detail).unwrap();
        let detail = slot.success(read).unwrap();
        let read = slot.begin(Coverage::Summary).unwrap();
        let summary = slot.success(read).unwrap();
        let failure = (category, "opaque-error-evidence");
        let read = slot.begin(Coverage::Detail).unwrap();
        let returned = slot
            .failure(read, failure, |(kind, _)| *kind == "primary-denial")
            .unwrap();
        assert_eq!(returned, failure, "preserve the original error unchanged");
        assert_eq!(slot.can_serve(&detail, &scope), !denies, "{category}");
        assert_eq!(slot.can_serve(&summary, &scope), !denies, "{category}");
    }
}

#[test]
fn optional_or_transient_failures_cannot_recover_access_after_a_primary_denial() {
    let scope = ObservationScope::new("scope");
    let mut slot = ObservationSlot::new(&scope, "resource");
    let read = slot.begin(Coverage::Detail).unwrap();
    let warm = slot.success(read).unwrap();
    let read = slot.begin(Coverage::Detail).unwrap();
    slot.deny(read).unwrap();
    for error in [
        "OptionalRestricted",
        "Transient",
        "RateLimited",
        "Unknown",
        "Pending",
    ] {
        let read = slot.begin(Coverage::Detail).unwrap();
        assert_eq!(slot.failure(read, error, |_| false).unwrap(), error);
        assert!(!slot.can_serve(&warm, &scope));
    }
}
